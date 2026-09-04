//! The on-wire reconcile backend: a framed sync session over any byte stream.
//!
//! [`run_client`] and [`run_server`] perform the same bidirectional reconcile as
//! [`SyncNode::reconcile`], but over an `AsyncRead + AsyncWrite` stream instead of
//! a shared reference. The daemon runs [`run_server`] on an accepted `fabric/sync`
//! iroh bi-stream and [`run_client`] on an outbound one; the tests run both ends
//! over an in-memory [`tokio::io::duplex`], which is the "loopback backend". A
//! unit test asserts the wire session reaches the exact same state as the pure
//! reference reconcile — that is what makes the transport swappable behind one
//! conformance contract.
//!
//! Protocol (3 messages, content pushed to whoever needs it):
//! 1. client → server `Hello`: client manifest + hashes client lacks content for.
//! 2. server → client `Reply`: server's *pre-adopt* manifest + hashes server
//!    lacks + content bundle for everything the client must adopt or repair.
//! 3. client → server `Push`: content bundle for everything the server must adopt
//!    or repair.
//!
//! The server snapshots its manifest *before* adopting the client's entries so
//! the client still computes (and pushes) the content the server now needs.
//! Content is framed as raw length-prefixed bytes — never JSON-encoded — so file
//! payloads do not pay base64/array bloat.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::Mutex;

use super::manifest::{ContentHash, Manifest};
use super::node::{Reconciled, SyncNode, content_hash};

/// Largest JSON control frame accepted (manifests are metadata-only, so this is
/// generous headroom, not a content limit).
const MAX_JSON_FRAME: usize = 64 * 1024 * 1024;
/// Largest single content blob accepted (per file).
pub(crate) const MAX_BLOB: usize = 512 * 1024 * 1024;
/// Largest blob count in one bundle.
const MAX_BLOB_COUNT: u32 = 1_000_000;

#[derive(Debug, Serialize, Deserialize)]
struct HelloHeader {
    name: String,
    /// The sender's state to share: its WHOLE manifest, or a delta when
    /// `is_delta` is set. Named `manifest` because an older build parses this
    /// field by name and always reads it as a whole manifest.
    manifest: Manifest,
    wanted: Vec<ContentHash>,
    /// The sender's lattice-point digest BEFORE this exchange.
    ///
    /// Empty from a build that predates deltas, and that emptiness is the
    /// capability signal: a peer that cannot report a digest is never sent a
    /// delta, because it would read one as a whole manifest.
    #[serde(default)]
    digest: String,
    /// True when `manifest` carries only what changed.
    ///
    /// Never set for a peer that did not report a digest. An older build ignores
    /// this field and would treat a delta as the sender's entire state, which
    /// would make it push content for every path outside the delta.
    #[serde(default)]
    is_delta: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplyHeader {
    manifest: Manifest,
    wanted: Vec<ContentHash>,
    /// A configuration or data error the initiator must act on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// The responder's digest AFTER adopting what the initiator sent.
    ///
    /// This is the acknowledgement. The join is commutative, so once both sides
    /// have exchanged complete payloads they are at the SAME lattice point. If
    /// the initiator's own post-merge digest does not equal this, a payload was
    /// incomplete and the cursor that produced it cannot be trusted.
    #[serde(default)]
    digest: String,
    #[serde(default)]
    is_delta: bool,
}

// ---- framing primitives ----

async fn write_u32<W: AsyncWrite + Unpin>(w: &mut W, v: u32) -> Result<()> {
    w.write_all(&v.to_be_bytes()).await?;
    Ok(())
}

async fn read_u32<R: AsyncRead + Unpin>(r: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).await?;
    Ok(u32::from_be_bytes(buf))
}

async fn write_len_bytes<W: AsyncWrite + Unpin>(w: &mut W, bytes: &[u8]) -> Result<()> {
    write_u32(w, bytes.len() as u32).await?;
    w.write_all(bytes).await?;
    Ok(())
}

async fn read_len_bytes<R: AsyncRead + Unpin>(r: &mut R, max: usize) -> Result<Vec<u8>> {
    let len = read_u32(r).await? as usize;
    if len > max {
        bail!("sync frame of {len} bytes exceeds limit {max}");
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_blobs<W: AsyncWrite + Unpin>(
    w: &mut W,
    blobs: &[(ContentHash, Vec<u8>)],
) -> Result<()> {
    validate_blobs(blobs)?;
    write_u32(w, blobs.len() as u32).await?;
    for (hash, bytes) in blobs {
        w.write_all(&hash.0).await?;
        write_len_bytes(w, bytes).await?;
    }
    Ok(())
}

fn validate_blobs(blobs: &[(ContentHash, Vec<u8>)]) -> Result<()> {
    if blobs.len() > MAX_BLOB_COUNT as usize {
        bail!(
            "sync bundle of {} blobs exceeds limit {MAX_BLOB_COUNT}",
            blobs.len()
        );
    }
    if let Some((_, bytes)) = blobs.iter().find(|(_, bytes)| bytes.len() > MAX_BLOB) {
        bail!(
            "sync content blob of {} bytes exceeds limit {MAX_BLOB}",
            bytes.len()
        );
    }
    Ok(())
}

async fn write_error_reply<W: AsyncWrite + Unpin>(w: &mut W, message: &str) -> Result<()> {
    let reply = ReplyHeader {
        manifest: Manifest::new(),
        wanted: Vec::new(),
        error: Some(message.to_string()),
        digest: String::new(),
        is_delta: false,
    };
    write_len_bytes(w, &serde_json::to_vec(&reply)?).await?;
    // An older client ignores `error` and still expects the bundle count.
    write_u32(w, 0).await?;
    w.flush().await?;
    Ok(())
}

/// A content bundle read from the wire: how many blobs stored and their bytes.
#[derive(Debug, Clone, Copy, Default)]
struct Received {
    blobs: usize,
    bytes: usize,
}

/// Read a content bundle and store each blob in `node` if its bytes hash to the
/// advertised hash (content-addressed: corrupt bytes are dropped, never written).
async fn read_blobs_into<R: AsyncRead + Unpin>(
    r: &mut R,
    node: &Arc<Mutex<SyncNode>>,
) -> Result<Received> {
    let count = read_u32(r).await?;
    if count > MAX_BLOB_COUNT {
        bail!("sync bundle of {count} blobs exceeds limit {MAX_BLOB_COUNT}");
    }
    let mut received = Received::default();
    for _ in 0..count {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash).await?;
        let bytes = read_len_bytes(r, MAX_BLOB).await?;
        if content_hash(&bytes) != ContentHash(hash) {
            // Advertised hash did not match the bytes; skip rather than store
            // content the manifest cannot reference.
            continue;
        }
        received.blobs += 1;
        received.bytes += bytes.len();
        node.lock().await.put_content(bytes);
    }
    Ok(received)
}

/// What a peer said in its Hello, for the resolver to route and decide on.
pub struct HelloInfo {
    pub name: String,
    /// The peer's payload: its whole manifest, or a delta when `is_delta`.
    pub manifest: Arc<Manifest>,
    pub is_delta: bool,
    /// The peer's digest before this exchange. Empty from an older build.
    pub digest: String,
}

// ---- sessions ----

/// Run the initiating side of a reconcile for sync `name` against a peer stream.
pub async fn run_client<S>(
    mut stream: S,
    node: Arc<Mutex<SyncNode>>,
    name: &str,
    peer: &str,
) -> Result<Reconciled>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // 1. Snapshot local state and send Hello.
    //
    // A peer we hold a cursor for gets only what changed since that cursor. A
    // peer we do not gets everything, which covers first contact, a peer we
    // reset after a mismatch, and every peer after a restart.
    let (payload, is_delta, wanted, head_at_hello) = {
        let node = node.lock().await;
        let wanted = node.missing_content_hashes();
        let head_at_hello = node.changes().head();
        match node.changes().cursor_for(peer) {
            Some(cursor) => {
                let changed = node.changes().since(cursor);
                (node.manifest().subset(changed), true, wanted, head_at_hello)
            }
            None => (node.manifest().clone(), false, wanted, head_at_hello),
        }
    };
    node.lock().await.note_payload_sent(&payload);
    let hello = HelloHeader {
        name: name.to_string(),
        manifest: payload.clone(),
        wanted,
        digest: { node.lock().await.manifest().digest() },
        is_delta,
    };
    let hello_frame = serde_json::to_vec(&hello)?;
    // THE WHOLE MANIFEST, ON EVERY PASS, CHANGED OR NOT. This is the term that
    // makes a reconcile expensive and the term delta replication exists to
    // remove, so a measurement that leaves it out measures the wrong thing.
    let mut wire_bytes = hello_frame.len();
    write_len_bytes(&mut stream, &hello_frame).await?;
    stream.flush().await?;

    // 2. Read the server's reply header, then its content bundle into our store.
    let reply_frame = read_len_bytes(&mut stream, MAX_JSON_FRAME)
        .await
        .context("reading sync reply header")?;
    // The peer's whole manifest comes back the same way.
    wire_bytes += reply_frame.len();
    let reply: ReplyHeader = serde_json::from_slice(&reply_frame)?;
    if let Some(error) = &reply.error {
        bail!("{error}");
    }
    let received = read_blobs_into(&mut stream, &node).await?;
    wire_bytes += received.bytes;

    // 3. Adopt the server's winning entries and bundle what the server needs.
    let (pulled, blobs_for_server, fallback, landing) = {
        let mut node = node.lock().await;
        let pulled = node.adopt_from_peer(&reply.manifest);
        // What content to push. `hashes_peer_needs` infers what the peer lacks
        // by diffing against what it sent, which is only sound when it sent
        // EVERYTHING. Against a delta, every path outside the delta looks
        // missing and we would push the whole tree. See
        // `hashes_peer_needs_is_unsound_for_a_delta`.
        let mut wanted = if !reply.is_delta {
            node.hashes_peer_needs(&reply.manifest)
        } else if is_delta {
            node.content_for(&payload)
        } else {
            // We sent our whole manifest and the peer sent a delta. Inferring
            // from a delta is unsound, and pushing content for our entire
            // manifest would cost more than the thing this removes. The peer's
            // `wanted` list covers what it knows it lacks.
            Vec::new()
        };
        for hash in reply.wanted {
            if !wanted.contains(&hash) {
                wanted.push(hash);
            }
        }
        let blobs = node.gather_content(&wanted);

        // The acknowledgement. Both sides have now exchanged their payloads, and
        // the join is commutative, so complete payloads leave us at the SAME
        // lattice point. Equal digests therefore prove the peer holds everything
        // we had at `head_at_send`.
        //
        // Unequal digests prove a payload was incomplete, whatever the reason:
        // a cursor that outlived the state it described, a peer that lost its
        // manifest, a bug in this path. Forgetting the cursor costs one full
        // exchange and repairs all of them. This is the self-healing trigger,
        // and a rising count of it is a bug report.
        let final_digest = node.manifest().digest();
        // Did anything OTHER than this exchange move us?
        //
        // `adopt` records exactly one path per entry it takes, so a head that
        // advanced by exactly `pulled` means nothing else touched this node
        // between the Hello and now. Anything more is a concurrent local write
        // or an inbound reconcile from another peer, and on a three node line
        // the middle peer is almost always mid-flight with somebody.
        //
        // When that happens the digests below are comparing different moments
        // and disagreeing proves nothing, so no verdict is reached. Not
        // advancing the cursor costs a re-send, which is free; resetting it on a
        // false alarm would cost a full exchange.
        //
        // BUT READ THIS BEFORE TRUSTING IT. I traded a false-positive fallback
        // for a silent stall and did not notice I had moved the failure rather
        // than removed it. A node changed on every exchange never reaches a
        // verdict here, so its cursor never advances and the delta computed from
        // it grows until it is the whole manifest again. `delta_fallbacks` does
        // not see that, because nothing was found incomplete and nothing was
        // reset. See
        // `a_stalled_cursor_grows_the_payload_and_the_fallback_counter_stays_zero`,
        // and `full_payload_sends`, which counts the outcome whatever the cause.
        //
        // A fix that relocates a failure looks exactly like a fix that removes
        // one. The only difference is whether somebody wrote down which it was.
        //
        // AND THE RULE THAT CAME OUT OF DOING IT TWICE:
        //
        //   A guard that changes what one side does must SAY SO to the other,
        //   or the other side infers something false.
        //
        // This guard was added without one. The initiator knew it had moved
        // mid-exchange and reached no verdict; the responder could only see a
        // landing digest that did not match, which is indistinguishable from an
        // incomplete payload. So it forgot a good cursor and sent a whole
        // manifest, three times in twenty minutes between two live machines,
        // while every counter read healthy.
        //
        // The empty landing digest below is that sentence, spoken. Before adding
        // a guard here, ask what the peer will conclude from the behaviour it
        // can observe, and whether that conclusion is true.
        let only_this_exchange =
            node.changes().head() == head_at_hello.saturating_add(pulled as u64);
        let fallback = if reply.digest.is_empty() {
            // A peer that reports no digest predates deltas. It must never be
            // sent one, and no cursor may be held for it.
            node.changes_mut().reset_peer(peer);
            false
        } else if !only_this_exchange {
            false
        } else if final_digest == reply.digest {
            // Acknowledge our head AFTER adopting, not `head_at_send`.
            //
            // Equal digests mean the peer holds the same manifest we do, so it
            // holds EVERY path in our buffer, including the ones we just adopted
            // from it in this very pass. Acknowledging the older head would send
            // those paths straight back to the peer they came from on the next
            // pass, which is an echo that costs content bytes and never ends.
            let head = node.changes().head();
            node.changes_mut().acknowledge(peer, head);
            false
        } else {
            node.changes_mut().reset_peer(peer);
            true
        };
        // What we tell the responder we landed on. EMPTY when we cannot vouch
        // for it, which is the other half of the guard above.
        //
        // We know we moved mid-exchange; the responder cannot. Sending a digest
        // we know describes a different moment makes it reset a perfectly good
        // cursor and send us a whole manifest next pass. Saying nothing means
        // "no verdict", and it leaves its cursor alone.
        let landing = if only_this_exchange {
            final_digest.clone()
        } else {
            String::new()
        };
        (pulled, blobs, fallback, landing)
    };
    let for_server = (pulled, blobs_for_server);
    write_blobs(&mut stream, &for_server.1).await?;
    // Tell the server where we landed, so it can advance its cursor too.
    //
    // Without this the serving side can only ever acknowledge during a pass
    // where the digests ALREADY matched on arrival. Such a pass does not happen:
    // fabric runs no pass at all when nothing has changed, and when something
    // has changed the digests differ by definition. The server would therefore
    // never hold a cursor and would send its whole manifest forever.
    //
    // Sent only to a peer that reported a digest of its own. An older build does
    // not read this frame and must not be given it.
    if !reply.digest.is_empty() {
        write_len_bytes(&mut stream, landing.as_bytes()).await?;
    }
    stream.flush().await?;

    // Wait for the server to acknowledge it has read and stored the push before
    // returning (the caller closes the connection on return; without this ack the
    // close would race the server's read of the final content bundle).
    let _ack = read_u32(&mut stream).await?;

    let sent: usize = for_server.1.iter().map(|(_, b)| b.len()).sum();
    wire_bytes += sent;
    Ok(Reconciled {
        pulled: for_server.0,
        pushed: for_server.1.len(),
        bytes: sent + received.bytes,
        wire_bytes,
        fallbacks: usize::from(fallback),
    })
}

/// Run the accepting side of a reconcile against a peer stream. Returns the sync
/// `name` the peer asked for (so the daemon can route to the right entry), the
/// reconcile stats, and the resolver-provided context kept alive for the full
/// session.
pub async fn run_server<S, F, Fut, C>(
    mut stream: S,
    peer: &str,
    resolve: F,
) -> Result<(String, Reconciled, C)>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(HelloInfo) -> Fut,
    Fut: std::future::Future<Output = Result<Option<(Arc<Mutex<SyncNode>>, C)>>>,
{
    // 1. Read Hello.
    let HelloHeader {
        name,
        manifest,
        wanted,
        digest: peer_digest,
        is_delta,
    } = serde_json::from_slice(
        &read_len_bytes(&mut stream, MAX_JSON_FRAME)
            .await
            .context("reading sync hello header")?,
    )?;
    let manifest = Arc::new(manifest);

    let Some((node, context)) = resolve(HelloInfo {
        name: name.clone(),
        manifest: manifest.clone(),
        is_delta,
        digest: peer_digest.clone(),
    })
    .await?
    else {
        let message = format!("no local sync entry named {name:?}");
        write_error_reply(&mut stream, &message).await?;
        bail!(message);
    };

    // 2. Snapshot BEFORE adopting so the client still pushes the content we need,
    // then adopt the client's winning entries (content arrives in the Push).
    let (reply, blobs_for_client, pushed, head_at_reply) = {
        let mut node = node.lock().await;
        let before_digest = node.manifest().digest();

        // Take the acknowledgement FIRST, because it describes the state before
        // this exchange and therefore decides what we may send in it.
        //
        // A peer that reported no digest predates deltas and must never be sent
        // one. A peer standing exactly where we stand already holds everything
        // we hold, so our cursor for it may advance to our head.
        let acknowledged = if peer_digest.is_empty() {
            node.changes_mut().reset_peer(peer);
            false
        } else if peer_digest == before_digest {
            let head = node.changes().head();
            node.changes_mut().acknowledge(peer, head);
            true
        } else {
            false
        };

        // Our side of the exchange, chosen the way the client chose its own.
        let (server_payload, reply_is_delta) = match node.changes().cursor_for(peer) {
            Some(cursor) => {
                let changed = node.changes().since(cursor);
                (node.manifest().subset(changed), true)
            }
            None => (node.manifest().clone(), false),
        };

        // What content to push.
        //
        // `hashes_peer_needs` infers what the peer lacks by diffing against what
        // it sent, which is only sound when it sent EVERYTHING. Against a delta
        // every path outside the delta looks missing. See
        // `hashes_peer_needs_is_unsound_for_a_delta`.
        let mut client_needs = if !is_delta {
            node.hashes_peer_needs(&manifest)
        } else if reply_is_delta {
            node.content_for(&server_payload)
        } else {
            // The peer sent a delta but we hold no cursor for it, so we must
            // send our whole manifest. We cannot infer what it lacks from a
            // delta, and pushing content for our entire manifest would cost far
            // more than the thing this change removes. Its `wanted` list below
            // covers what it knows it lacks, and the next pass covers the rest.
            Vec::new()
        };
        for hash in &wanted {
            if !client_needs.contains(hash) {
                client_needs.push(*hash);
            }
        }
        let blobs = node.gather_content(&client_needs);
        let pushed = node.adopt_from_peer(&manifest);

        node.note_payload_sent(&server_payload);

        // The reply advertises what WE are still missing so the client repairs
        // us, and carries our POST-adopt digest as the acknowledgement.
        let reply = ReplyHeader {
            manifest: server_payload,
            wanted: node.missing_content_hashes(),
            error: None,
            digest: node.manifest().digest(),
            is_delta: reply_is_delta,
        };
        let _ = acknowledged;
        // The head AS OF THIS REPLY. Anything recorded after it belongs to a
        // change this exchange did not carry, and acknowledging it would claim
        // the peer holds something it was never sent.
        let head_at_reply = node.changes().head();
        (reply, blobs, pushed, head_at_reply)
    };

    // 3. Send reply header + our content bundle, then read the client's push.
    if let Err(error) = validate_blobs(&blobs_for_client) {
        let message = format!("{error:#}");
        write_error_reply(&mut stream, &message).await?;
        return Err(error);
    }
    write_len_bytes(&mut stream, &serde_json::to_vec(&reply)?).await?;
    write_blobs(&mut stream, &blobs_for_client).await?;
    stream.flush().await?;

    let received = read_blobs_into(&mut stream, &node).await?;

    // Where the client landed, against WHAT WE SENT rather than where we are now.
    //
    // The two are not the same thing. This node may serve one peer while another
    // exchange is changing it, and on a three node line the middle peer is
    // almost always mid-flight with somebody. Comparing against the current
    // digest reports "a payload was incomplete" for a state that simply moved
    // on, which forgets a perfectly good cursor and costs a full exchange. CI
    // caught exactly that: the relay case fell back on Linux while passing on a
    // quieter machine.
    //
    // `reply.digest` is the counterpart of the client's landing digest: both are
    // the join of the two sides as this exchange saw them, so they are equal if
    // and only if both payloads were complete. Anything that changed here
    // afterwards has a sequence above `head_at_reply` and goes out next pass.
    let mut fallback = false;
    if !peer_digest.is_empty() {
        let frame = read_len_bytes(&mut stream, MAX_JSON_FRAME)
            .await
            .context("reading the client's landing digest")?;
        let client_landed = String::from_utf8_lossy(&frame).to_string();
        let mut node = node.lock().await;
        if client_landed.is_empty() {
            // The initiator says it cannot vouch for where it landed, because
            // something else moved it mid-exchange. It knows that and we cannot
            // see it. NO VERDICT: keep the cursor, send a delta next time.
            //
            // Treating this as a mismatch is what made a busy pair exchange
            // whole manifests for no reason, three times in twenty minutes,
            // while every counter read healthy.
        } else if client_landed == reply.digest {
            node.changes_mut().acknowledge(peer, head_at_reply);
        } else {
            node.changes_mut().reset_peer(peer);
            fallback = true;
        }
    }

    // Acknowledge the push so the client can safely close the connection.
    write_u32(&mut stream, 1).await?;
    stream.flush().await?;

    let sent: usize = blobs_for_client.iter().map(|(_, b)| b.len()).sum();
    Ok((
        name,
        Reconciled {
            // Measured on the CLIENT side, which is where a pass originates.
            wire_bytes: 0,
            pulled: received.blobs,
            pushed,
            bytes: sent + received.bytes,
            fallbacks: usize::from(fallback),
        },
        context,
    ))
}

struct IdleTimeoutStream<S> {
    inner: S,
    peer: String,
    idle_timeout: Duration,
    read_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    write_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<S> IdleTimeoutStream<S> {
    fn new(inner: S, peer: &str, idle_timeout: Duration) -> Self {
        Self {
            inner,
            peer: peer.to_string(),
            idle_timeout,
            read_deadline: None,
            write_deadline: None,
        }
    }

    fn timeout_error(&self) -> io::Error {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "inbound sync session deadline elapsed for peer {}: no I/O progress for {} ms",
                self.peer,
                self.idle_timeout.as_millis()
            ),
        )
    }
}

fn deadline_elapsed(
    deadline: &mut Option<Pin<Box<tokio::time::Sleep>>>,
    idle_timeout: Duration,
    cx: &mut TaskContext<'_>,
) -> bool {
    deadline
        .get_or_insert_with(|| Box::pin(tokio::time::sleep(idle_timeout)))
        .as_mut()
        .poll(cx)
        .is_ready()
}

impl<S: AsyncRead + Unpin> AsyncRead for IdleTimeoutStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let filled_before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                if buf.filled().len() > filled_before {
                    this.read_deadline = None;
                }
                Poll::Ready(result)
            }
            Poll::Pending => {
                if deadline_elapsed(&mut this.read_deadline, this.idle_timeout, cx) {
                    this.read_deadline = None;
                    Poll::Ready(Err(this.timeout_error()))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for IdleTimeoutStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(result) => {
                if matches!(&result, Ok(written) if *written > 0) {
                    this.write_deadline = None;
                }
                Poll::Ready(result)
            }
            Poll::Pending => {
                if deadline_elapsed(&mut this.write_deadline, this.idle_timeout, cx) {
                    this.write_deadline = None;
                    Poll::Ready(Err(this.timeout_error()))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Ready(result) => {
                this.write_deadline = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                if deadline_elapsed(&mut this.write_deadline, this.idle_timeout, cx) {
                    this.write_deadline = None;
                    Poll::Ready(Err(this.timeout_error()))
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_shutdown(cx) {
            Poll::Ready(result) => {
                this.write_deadline = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                if deadline_elapsed(&mut this.write_deadline, this.idle_timeout, cx) {
                    this.write_deadline = None;
                    Poll::Ready(Err(this.timeout_error()))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

/// Run an inbound wire session with an I/O progress deadline. The resolver
/// context stays alive for the session, so timeout releases its operation guard.
pub(crate) async fn run_server_with_idle_timeout<S, F, Fut, C>(
    stream: S,
    peer: &str,
    resolve: F,
    idle_timeout: Duration,
) -> Result<(String, Reconciled, C)>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(HelloInfo) -> Fut,
    Fut: std::future::Future<Output = Result<Option<(Arc<Mutex<SyncNode>>, C)>>>,
{
    run_server(
        IdleTimeoutStream::new(stream, peer, idle_timeout),
        peer,
        resolve,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn author(n: u8) -> super::super::manifest::Author {
        super::super::manifest::Author([n; 32])
    }

    #[tokio::test]
    async fn an_inbound_deadline_drops_the_resolver_guard() {
        let node = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let guard_lock = Arc::new(Mutex::new(()));
        let resolver_lock = guard_lock.clone();
        let (guarded_tx, guarded_rx) = tokio::sync::oneshot::channel();
        let (mut client_end, server_end) = tokio::io::duplex(1 << 20);
        let mut server = tokio::spawn(async move {
            run_server_with_idle_timeout(
                server_end,
                "peer-a",
                move |_| async move {
                    let guard = resolver_lock.lock_owned().await;
                    let _ = guarded_tx.send(());
                    Ok(Some((node, guard)))
                },
                Duration::from_millis(250),
            )
            .await
        });

        let hello = HelloHeader {
            name: "catalog".to_string(),
            manifest: Manifest::new(),
            wanted: Vec::new(),
            digest: String::new(),
            is_delta: false,
        };
        write_len_bytes(&mut client_end, &serde_json::to_vec(&hello).unwrap())
            .await
            .unwrap();
        client_end.flush().await.unwrap();
        guarded_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), guard_lock.lock())
                .await
                .is_err(),
            "the resolver context did not hold its guard during the session"
        );

        let result = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
        if result.is_err() {
            server.abort();
        }
        let error = result
            .expect("the inbound session had no deadline")
            .expect("the inbound server task panicked")
            .expect_err("the stalled inbound session succeeded");
        assert!(
            format!("{error:#}").contains("inbound sync session deadline"),
            "the timeout error did not name the deadline: {error:#}"
        );
        assert!(
            format!("{error:#}").contains("peer-a"),
            "the timeout error did not name the peer: {error:#}"
        );
        let _guard = tokio::time::timeout(Duration::from_millis(100), guard_lock.lock())
            .await
            .expect("the inbound deadline did not release the resolver guard");
    }

    #[tokio::test]
    async fn a_progressing_inbound_session_can_outlive_the_idle_deadline() {
        let node = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let (guarded_tx, guarded_rx) = tokio::sync::oneshot::channel();
        let (mut client_end, server_end) = tokio::io::duplex(1 << 20);
        let server = tokio::spawn(async move {
            run_server_with_idle_timeout(
                server_end,
                "slow-peer",
                move |_| async move {
                    let _ = guarded_tx.send(());
                    Ok(Some((node, ())))
                },
                Duration::from_millis(200),
            )
            .await
        });

        let hello = HelloHeader {
            name: "catalog".to_string(),
            manifest: Manifest::new(),
            wanted: Vec::new(),
            digest: String::new(),
            is_delta: false,
        };
        write_len_bytes(&mut client_end, &serde_json::to_vec(&hello).unwrap())
            .await
            .unwrap();
        client_end.flush().await.unwrap();
        guarded_rx.await.unwrap();

        // The push count takes 320 ms to arrive, but every byte arrives within
        // the 200 ms idle bound. Progress must keep the session alive.
        for byte in 0u32.to_be_bytes() {
            tokio::time::sleep(Duration::from_millis(80)).await;
            client_end.write_all(&[byte]).await.unwrap();
            client_end.flush().await.unwrap();
        }

        let _ = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("the progressing inbound session did not finish")
            .expect("the inbound server task panicked")
            .expect("the progressing inbound session exceeded its idle deadline");
    }

    #[tokio::test]
    async fn a_missing_remote_entry_reaches_the_initiator_as_a_configuration_error() {
        let node = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let (client_end, server_end) = tokio::io::duplex(1 << 20);
        let server = tokio::spawn(async move {
            run_server(server_end, "peer-a", move |_| async move {
                Ok::<_, anyhow::Error>(None::<(Arc<Mutex<SyncNode>>, ())>)
            })
            .await
        });

        let error = run_client(client_end, node, "catalog", "peer-b")
            .await
            .expect_err("a missing remote entry was accepted");
        assert!(
            format!("{error:#}").contains("no local sync entry named \"catalog\""),
            "the initiator lost the remote configuration error: {error:#}"
        );
        assert!(server.await.unwrap().is_err());
    }

    /// WHAT `delta_fallbacks` DOES NOT COUNT.
    ///
    /// An initiator that keeps being changed mid-exchange never reaches a
    /// verdict, so it never acknowledges, so its cursor never moves. The delta
    /// it sends is computed correctly from a cursor that is simply old, and it
    /// grows every round until it is the whole manifest.
    ///
    /// Nothing about that is a fallback. No payload was found incomplete and no
    /// cursor was reset, so the counter stays at zero while the wire cost climbs
    /// back to where it started. **A full-manifest-sized exchange therefore
    /// happens without incrementing `delta_fallbacks`.**
    ///
    /// This test exists to state that plainly rather than to defend it. It
    /// asserts the CURRENT behaviour, including the zero, so that whatever is
    /// done about it has to come here and say so.
    #[tokio::test]
    async fn a_stalled_cursor_grows_the_payload_and_the_fallback_counter_stays_zero() {
        let node = Arc::new(Mutex::new(SyncNode::new(author(1))));
        for i in 0..200 {
            node.lock()
                .await
                .local_write(&format!("f{i:03}.md"), format!("body {i}").as_bytes(), 0, 0);
        }

        // One clean exchange first, so a cursor exists to stall.
        let mut sizes = Vec::new();
        let mut fallbacks = 0usize;
        for round in 0..4 {
            let (client_end, mut server_end) = tokio::io::duplex(1 << 22);
            let for_client = node.clone();
            let client = tokio::spawn(async move {
                run_client(client_end, for_client, "cat", "peer-under-test").await
            });

            let hello_frame = read_len_bytes(&mut server_end, MAX_JSON_FRAME).await.unwrap();
            let hello: HelloHeader = serde_json::from_slice(&hello_frame).unwrap();
            sizes.push(hello.manifest.len());

            // From round 1 on, change the initiator while it waits for us. This
            // is an inbound reconcile from a third peer, or a local write, on a
            // node busy enough for it to happen every time.
            if round > 0 {
                node.lock().await.local_write(
                    &format!("busy{round}.md"),
                    b"a third peer, or an agent, writing",
                    0,
                    0,
                );
            }

            // Answer as a converged peer: nothing to send, and the digest the
            // exchange should have landed on.
            let reply = ReplyHeader {
                manifest: Manifest::new(),
                wanted: Vec::new(),
                error: None,
                digest: hello.digest.clone(),
                is_delta: true,
            };
            write_len_bytes(&mut server_end, &serde_json::to_vec(&reply).unwrap())
                .await
                .unwrap();
            write_u32(&mut server_end, 0).await.unwrap();
            server_end.flush().await.unwrap();

            let pushed = read_u32(&mut server_end).await.unwrap();
            for _ in 0..pushed {
                let mut hash = [0u8; 32];
                server_end.read_exact(&mut hash).await.unwrap();
                let _ = read_len_bytes(&mut server_end, MAX_BLOB).await.unwrap();
            }
            let _landed = read_len_bytes(&mut server_end, MAX_JSON_FRAME).await.unwrap();
            write_u32(&mut server_end, 1).await.unwrap();
            server_end.flush().await.unwrap();

            fallbacks += client.await.unwrap().unwrap().fallbacks;
        }

        // Round 0 establishes the cursor from a full send. Round 1 is small
        // because the cursor was acknowledged. From then on the initiator is
        // changed every round, never acknowledges, and the payload climbs.
        // Measured: [200, 0, 1, 2]. Round 0 is first contact and sends
        // everything. Round 1 sends NOTHING, because round 0's exchange was
        // clean and the cursor advanced. From round 2 the initiator is being
        // changed every round, never acknowledges, and the payload climbs by one
        // entry each time it is not acknowledged.
        assert_eq!(sizes[0], 200, "round 0 should be first contact: {sizes:?}");
        assert_eq!(
            sizes[1], 0,
            "round 1 should carry nothing, the cursor having just advanced: {sizes:?}"
        );
        assert!(
            sizes[3] > sizes[2] && sizes[2] > sizes[1],
            "the payload did not climb, so the cursor is not stalling: {sizes:?}"
        );
        assert_eq!(
            fallbacks, 0,
            "the payload grew without a single fallback being counted, which is \
             the point of this test: {sizes:?}"
        );

        // And the counter that DOES see it. Round 0 sent the whole manifest at
        // first contact. The stall has not yet grown back to the whole manifest
        // in four rounds, so this is still 1: the point is that it counts the
        // outcome, and it will count the stall the moment the delta reaches full
        // size, which `delta_fallbacks` never will.
        assert_eq!(
            node.lock().await.full_payload_sends(),
            1,
            "full_payload_sends missed the first-contact send"
        );
    }

    /// `full_payload_sends` counts a stalled cursor once its delta has grown
    /// back to the whole manifest, which is the case `delta_fallbacks` cannot
    /// see. It counts the OUTCOME, so the flag the payload travels under does
    /// not matter.
    #[tokio::test]
    async fn full_payload_sends_counts_a_delta_that_grew_to_the_whole_manifest() {
        let mut node = SyncNode::new(author(1));
        node.local_write("a.md", b"one", 0, 0);
        node.local_write("b.md", b"two", 0, 0);
        assert_eq!(node.full_payload_sends(), 0);

        // A real delta: one path of two. Not counted.
        let small = node.manifest().subset(["a.md"]);
        node.note_payload_sent(&small);
        assert_eq!(
            node.full_payload_sends(),
            0,
            "a genuine delta must not be counted"
        );

        // A delta that has grown to cover every path IS the manifest, whatever
        // flag it travels under.
        let grown = node.manifest().subset(["a.md", "b.md"]);
        node.note_payload_sent(&grown);
        assert_eq!(
            node.full_payload_sends(),
            1,
            "a delta covering every path is a full payload and must be counted"
        );

        // An empty manifest is not a full payload, or a node with nothing to
        // sync would count every converged pass.
        let mut empty = SyncNode::new(author(2));
        empty.note_payload_sent(&Manifest::new());
        assert_eq!(empty.full_payload_sends(), 0, "nothing is not everything");
    }

    /// A RESPONDER THAT CHANGES MID-EXCHANGE MUST NOT CALL THE EXCHANGE
    /// INCOMPLETE.
    ///
    /// This is the defect CI found and the three-daemon relay test could not
    /// reproduce. That test depends on two exchanges overlapping by luck of
    /// timing, and on a fast machine they do not, so it passed with the fix
    /// reverted. A silent failure guarded by a test that cannot fail is not
    /// guarded at all.
    ///
    /// So the overlap is FORCED here rather than hoped for. The responder blocks
    /// reading the initiator's content bundle, and that window belongs entirely
    /// to a hand-driven client: it reads the reply, changes the responder's node
    /// as another peer would, and only then sends its push and its landing
    /// digest.
    ///
    /// The initiator lands exactly where the reply said the responder was, which
    /// is what a complete exchange looks like. Judging that against the
    /// responder's CURRENT state calls it incomplete and throws away a good
    /// cursor. Judging it against `reply.digest`, the true counterpart, does not.
    #[tokio::test]
    async fn a_responder_that_changes_mid_exchange_does_not_call_it_incomplete() {
        let server_node = Arc::new(Mutex::new(SyncNode::new(author(2))));
        server_node
            .lock()
            .await
            .local_write("shared.md", b"same on both", 0, 0);
        let converged = server_node.lock().await.manifest().clone();
        let client_digest = converged.digest();

        let (mut client_end, server_end) = tokio::io::duplex(1 << 20);
        let for_server = server_node.clone();
        let server = tokio::spawn(async move {
            run_server(server_end, "peer-under-test", move |_| async move {
                Ok(Some((for_server, ())))
            })
            .await
        });

        // Hello. The initiator stands exactly where the responder stands, so a
        // correct exchange has nothing to carry and nothing to repair.
        let hello = HelloHeader {
            name: "cat".to_string(),
            manifest: converged.clone(),
            wanted: Vec::new(),
            digest: client_digest.clone(),
            is_delta: false,
        };
        write_len_bytes(&mut client_end, &serde_json::to_vec(&hello).unwrap())
            .await
            .unwrap();
        client_end.flush().await.unwrap();

        let reply_frame = read_len_bytes(&mut client_end, MAX_JSON_FRAME).await.unwrap();
        let reply: ReplyHeader = serde_json::from_slice(&reply_frame).unwrap();
        assert!(!reply.digest.is_empty(), "the responder reported no digest");
        // Consume the responder's content bundle. Converged, so it is empty.
        let blobs = read_u32(&mut client_end).await.unwrap();
        assert_eq!(blobs, 0, "the fixture was not converged");

        // THE WINDOW. The responder is now blocked reading our push, holding no
        // lock. Move it, exactly as an exchange with a third peer would.
        let moved_to = {
            let mut node = server_node.lock().await;
            node.local_write("arrived-meanwhile.md", b"from another peer", 0, 0);
            node.manifest().digest()
        };
        assert_ne!(
            moved_to, reply.digest,
            "the responder did not actually move, so this test proves nothing"
        );

        // Our push is empty, and we landed exactly where the reply said the
        // responder was.
        write_u32(&mut client_end, 0).await.unwrap();
        write_len_bytes(&mut client_end, reply.digest.as_bytes())
            .await
            .unwrap();
        client_end.flush().await.unwrap();
        let _ack = read_u32(&mut client_end).await.unwrap();

        let (_, stats, ()) = server.await.unwrap().unwrap();
        assert_eq!(
            stats.fallbacks, 0,
            "the responder called a complete exchange incomplete because its own \
             state moved while it was serving. That forgets a good cursor and \
             costs a full manifest on the next pass, on exactly the three node \
             shape this runs on"
        );
        assert_eq!(
            server_node
                .lock()
                .await
                .changes()
                .cursor_for("peer-under-test"),
            Some(1),
            "the cursor was not advanced to the head as of the reply. Anything \
             recorded after it belongs to a change this exchange did not carry"
        );
    }

    /// AN INITIATOR THAT CHANGES MID-EXCHANGE MUST NOT CALL IT INCOMPLETE
    /// EITHER.
    ///
    /// The same defect from the other side. An inbound reconcile from a third
    /// peer can move the initiator between its Hello and its adopt, and then its
    /// own digest and the reply describe different moments. Disagreeing proves
    /// nothing, so no verdict may be reached.
    ///
    /// Forced the same way: a hand-driven responder reads the Hello, changes the
    /// initiator's node, and only then answers.
    #[tokio::test]
    async fn an_initiator_that_changes_mid_exchange_does_not_call_it_incomplete() {
        let client_node = Arc::new(Mutex::new(SyncNode::new(author(1))));
        client_node
            .lock()
            .await
            .local_write("shared.md", b"same on both", 0, 0);
        let converged = client_node.lock().await.manifest().clone();

        let (client_end, mut server_end) = tokio::io::duplex(1 << 20);
        let for_client = client_node.clone();
        let client = tokio::spawn(async move {
            run_client(client_end, for_client, "cat", "peer-under-test").await
        });

        // Read the Hello, then move the initiator before answering.
        let hello_frame = read_len_bytes(&mut server_end, MAX_JSON_FRAME).await.unwrap();
        let hello: HelloHeader = serde_json::from_slice(&hello_frame).unwrap();
        let moved_to = {
            let mut node = client_node.lock().await;
            node.local_write("arrived-meanwhile.md", b"from another peer", 0, 0);
            node.manifest().digest()
        };
        assert_ne!(
            moved_to, hello.digest,
            "the initiator did not actually move, so this test proves nothing"
        );

        // Answer as a converged peer would: nothing to send, and our digest is
        // where the exchange says both sides stood.
        let reply = ReplyHeader {
            manifest: Manifest::new(),
            wanted: Vec::new(),
            error: None,
            digest: converged.digest(),
            is_delta: true,
        };
        write_len_bytes(&mut server_end, &serde_json::to_vec(&reply).unwrap())
            .await
            .unwrap();
        write_u32(&mut server_end, 0).await.unwrap();
        server_end.flush().await.unwrap();

        let pushed = read_u32(&mut server_end).await.unwrap();
        for _ in 0..pushed {
            let mut hash = [0u8; 32];
            server_end.read_exact(&mut hash).await.unwrap();
            let _ = read_len_bytes(&mut server_end, MAX_BLOB).await.unwrap();
        }
        let _landed = read_len_bytes(&mut server_end, MAX_JSON_FRAME).await.unwrap();
        write_u32(&mut server_end, 1).await.unwrap();
        server_end.flush().await.unwrap();

        let stats = client.await.unwrap().unwrap();
        assert_eq!(
            stats.fallbacks, 0,
            "the initiator called a complete exchange incomplete because its own \
             state moved while it was mid-exchange. Its digest and the reply are \
             describing different moments, and disagreeing proves nothing"
        );
    }

    /// The wire session must reach the exact same folder state as the pure
    /// reference reconcile — the swappable-backend conformance guarantee.
    #[tokio::test]
    async fn wire_session_matches_pure_reconcile() {
        // Reference: two nodes reconciled purely in-process.
        let mut ref_a = SyncNode::new(author(1));
        let mut ref_b = SyncNode::new(author(2));
        ref_a.local_write("a.txt", b"alpha", 0, 0);
        ref_a.local_write("shared", b"from-a", 0, 0);
        ref_b.local_write("b.txt", b"beta", 0, 0);
        ref_b.local_write("shared", b"from-b", 0, 0);
        ref_a.reconcile(&mut ref_b);

        // Wire: same starting states, reconciled over an in-memory duplex.
        let a = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let b = Arc::new(Mutex::new(SyncNode::new(author(2))));
        {
            let mut a = a.lock().await;
            a.local_write("a.txt", b"alpha", 0, 0);
            a.local_write("shared", b"from-a", 0, 0);
        }
        {
            let mut b = b.lock().await;
            b.local_write("b.txt", b"beta", 0, 0);
            b.local_write("shared", b"from-b", 0, 0);
        }

        let (client_end, server_end) = tokio::io::duplex(1 << 20);
        let b_for_server = b.clone();
        let server = tokio::spawn(async move {
            run_server(server_end, "test-peer", move |hello| async move {
                assert_eq!(hello.name, "cat");
                Ok(Some((b_for_server, ())))
            })
            .await
        });
        let client = run_client(client_end, a.clone(), "cat", "test-server").await.unwrap();
        let (name, _server_stats, ()) = server.await.unwrap().unwrap();
        assert_eq!(name, "cat");
        assert!(!client.is_noop());

        // Both wire nodes match the pure reference exactly.
        assert_eq!(a.lock().await.folder_state(), ref_a.folder_state());
        assert_eq!(b.lock().await.folder_state(), ref_b.folder_state());
        assert_eq!(a.lock().await.folder_state(), b.lock().await.folder_state());
    }

    #[tokio::test]
    async fn converged_wire_session_is_a_noop() {
        let a = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let b = Arc::new(Mutex::new(SyncNode::new(author(2))));
        a.lock().await.local_write("x", b"same", 0, 0);
        b.lock().await.local_write("x", b"same", 0, 0);
        // Same content + same version(1)+... actually different authors: reconcile once.
        {
            let (c, s) = tokio::io::duplex(1 << 20);
            let b2 = b.clone();
            let srv = tokio::spawn(async move {
                run_server(s, "test-peer", move |_| async move { Ok(Some((b2, ()))) }).await
            });
            run_client(c, a.clone(), "cat", "test-server").await.unwrap();
            srv.await.unwrap().unwrap();
        }
        // Second session after convergence transfers no content.
        let (c, s) = tokio::io::duplex(1 << 20);
        let b2 = b.clone();
        let srv = tokio::spawn(async move {
            run_server(s, "test-peer", move |_| async move { Ok(Some((b2, ()))) }).await
        });
        let stats = run_client(c, a.clone(), "cat", "test-server").await.unwrap();
        srv.await.unwrap().unwrap();
        assert_eq!(
            stats.bytes, 0,
            "converged wire session moved bytes: {stats:?}"
        );
    }

    #[tokio::test]
    async fn wire_pushes_new_file_to_peer() {
        // The hetz-proof shape: a new file on the client lands on the server.
        let a = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let b = Arc::new(Mutex::new(SyncNode::new(author(2))));
        a.lock()
            .await
            .local_write("job-hetz.toml", b"host=hetz", 0, 0);

        let (c, s) = tokio::io::duplex(1 << 20);
        let b2 = b.clone();
        let srv = tokio::spawn(async move {
            run_server(s, "test-peer", move |_| async move { Ok(Some((b2, ()))) }).await
        });
        run_client(c, a.clone(), "cat", "test-server").await.unwrap();
        srv.await.unwrap().unwrap();

        let folder = b.lock().await.folder_state();
        assert_eq!(
            folder.get("job-hetz.toml").map(Vec::as_slice),
            Some(&b"host=hetz"[..])
        );
    }

    #[tokio::test]
    async fn wire_propagates_tombstone_once_then_replay_is_a_noop() {
        let a = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let b = Arc::new(Mutex::new(SyncNode::new(author(2))));
        a.lock().await.local_write("retired", b"old", 0, 0);

        let reconcile = |a: Arc<Mutex<SyncNode>>, b: Arc<Mutex<SyncNode>>| async move {
            let (client, server) = tokio::io::duplex(1 << 20);
            let task = tokio::spawn(async move {
                run_server(server, "test-peer", move |_| async move { Ok(Some((b, ()))) }).await
            });
            let client_stats = run_client(client, a, "bus", "test-server").await.unwrap();
            let (_, server_stats, ()) = task.await.unwrap().unwrap();
            (client_stats, server_stats)
        };

        reconcile(a.clone(), b.clone()).await;
        let bus = crate::sync::config::PolicyRules {
            propagate_deletes: true,
            sweep_tombstones: true,
        };
        assert!(a.lock().await.local_remove("retired", bus, 10));

        let (_, deleted) = reconcile(a.clone(), b.clone()).await;
        assert!(!deleted.is_noop());
        assert!(!a.lock().await.folder_state().contains_key("retired"));
        assert!(!b.lock().await.folder_state().contains_key("retired"));

        let (client_replay, server_replay) = reconcile(a, b).await;
        assert!(
            client_replay.is_noop() && server_replay.is_noop(),
            "a converged Tombstone replay moved state: client={client_replay:?} server={server_replay:?}"
        );
    }
}

#[cfg(test)]
mod wire_cost_tests {
    use super::*;

    fn author(n: u8) -> super::super::manifest::Author {
        super::super::manifest::Author([n; 32])
    }

    async fn reconcile_once(
        a: Arc<Mutex<SyncNode>>,
        b: Arc<Mutex<SyncNode>>,
    ) -> Reconciled {
        let (client_end, server_end) = tokio::io::duplex(1 << 22);
        let b_for_server = b.clone();
        let server = tokio::spawn(async move {
            run_server(server_end, "peer-a", move |_| async move {
                Ok(Some((b_for_server, ())))
            })
            .await
        });
        let stats = run_client(client_end, a, "cat", "peer-b").await.unwrap();
        let _ = server.await.unwrap().unwrap();
        stats
    }

    /// A CONVERGED PASS MUST SHIP NEITHER CONTENT NOR A MANIFEST.
    ///
    /// This assertion used to run the other way. Both sides sent their ENTIRE
    /// manifest in the handshake whether or not anything had changed, about
    /// 10 MB per pass per peer on the bus entry, and this test insisted on
    /// seeing that cost so a measurement could not report a converged pass as
    /// free. The delta path removed the cost, so the test now pins its absence.
    ///
    /// The bar is the fixture's OWN manifest size rather than a number picked to
    /// pass. A converged pass that costs a tenth of a manifest is still shipping
    /// something proportional to the tree, which is the thing being removed.
    #[tokio::test]
    async fn a_converged_pass_ships_neither_content_nor_a_manifest() {
        let a = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let b = Arc::new(Mutex::new(SyncNode::new(author(2))));
        // Enough entries that the manifest is unmistakably the dominant term.
        for i in 0..200 {
            let name = format!("file-{i:03}.md");
            a.lock().await.local_write(&name, b"x", 0, 0);
        }
        let manifest_bytes = serde_json::to_vec(a.lock().await.manifest())
            .unwrap()
            .len();
        assert!(
            manifest_bytes > 4_000,
            "the fixture manifest is only {manifest_bytes} bytes, too small to \
             tell a delta from a manifest"
        );

        // Converge them.
        let first = reconcile_once(a.clone(), b.clone()).await;
        assert!(
            first.wire_bytes > manifest_bytes,
            "the first pass must still carry the whole manifest, or the peers \
             never converged and the second pass proves nothing"
        );

        let second = reconcile_once(a.clone(), b.clone()).await;
        assert!(
            second.is_noop(),
            "the second pass was not converged, so this measures the wrong thing"
        );
        assert_eq!(
            second.bytes, 0,
            "a converged pass moved content, so the fixture is wrong"
        );
        assert_eq!(
            second.fallbacks, 0,
            "a converged pass fell back to full state, which means a cursor \
             described state the peer did not hold"
        );
        assert!(
            second.wire_bytes * 10 < manifest_bytes,
            "a converged pass shipped {} wire bytes against a {manifest_bytes} \
             byte manifest. It is still paying for the size of the tree",
            second.wire_bytes
        );
    }

    /// And content must still be counted, or the figure swaps one blind spot for
    /// another.
    #[tokio::test]
    async fn content_is_counted_as_well_as_the_manifest() {
        let a = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let b = Arc::new(Mutex::new(SyncNode::new(author(2))));
        let payload = vec![b'z'; 64 * 1024];
        a.lock().await.local_write("big.bin", &payload, 0, 0);

        let stats = reconcile_once(a, b).await;
        assert!(stats.bytes >= payload.len(), "the content was not transferred");
        assert!(
            stats.wire_bytes >= payload.len(),
            "wire bytes {} excludes the {} byte payload",
            stats.wire_bytes,
            payload.len()
        );
    }
}
