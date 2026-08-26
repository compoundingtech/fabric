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

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use super::manifest::{ContentHash, Manifest};
use super::node::{Reconciled, SyncNode, content_hash};

/// Largest JSON control frame accepted (manifests are metadata-only, so this is
/// generous headroom, not a content limit).
const MAX_JSON_FRAME: usize = 64 * 1024 * 1024;
/// Largest single content blob accepted (per file).
const MAX_BLOB: usize = 512 * 1024 * 1024;
/// Largest blob count in one bundle.
const MAX_BLOB_COUNT: u32 = 1_000_000;

#[derive(Debug, Serialize, Deserialize)]
struct HelloHeader {
    name: String,
    manifest: Manifest,
    wanted: Vec<ContentHash>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplyHeader {
    manifest: Manifest,
    wanted: Vec<ContentHash>,
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
    write_u32(w, blobs.len() as u32).await?;
    for (hash, bytes) in blobs {
        w.write_all(&hash.0).await?;
        write_len_bytes(w, bytes).await?;
    }
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

// ---- sessions ----

/// Run the initiating side of a reconcile for sync `name` against a peer stream.
pub async fn run_client<S>(
    mut stream: S,
    node: Arc<Mutex<SyncNode>>,
    name: &str,
) -> Result<Reconciled>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // 1. Snapshot local state and send Hello.
    let (local_manifest, wanted) = {
        let node = node.lock().await;
        (node.manifest().clone(), node.missing_content_hashes())
    };
    let hello = HelloHeader {
        name: name.to_string(),
        manifest: local_manifest.clone(),
        wanted,
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
    let received = read_blobs_into(&mut stream, &node).await?;
    wire_bytes += received.bytes;

    // 3. Adopt the server's winning entries and bundle what the server needs.
    let for_server = {
        let mut node = node.lock().await;
        let pulled = node.adopt(&reply.manifest);
        let mut wanted = node.hashes_peer_needs(&reply.manifest);
        for hash in reply.wanted {
            if !wanted.contains(&hash) {
                wanted.push(hash);
            }
        }
        let blobs = node.gather_content(&wanted);
        (pulled, blobs)
    };
    write_blobs(&mut stream, &for_server.1).await?;
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
    })
}

/// Run the accepting side of a reconcile against a peer stream. Returns the sync
/// `name` the peer asked for (so the daemon can route to the right entry), the
/// reconcile stats, and the resolver-provided context kept alive for the full
/// session.
pub async fn run_server<S, F, Fut, C>(mut stream: S, resolve: F) -> Result<(String, Reconciled, C)>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(String, Arc<Manifest>) -> Fut,
    Fut: std::future::Future<Output = Result<Option<(Arc<Mutex<SyncNode>>, C)>>>,
{
    // 1. Read Hello.
    let HelloHeader {
        name,
        manifest,
        wanted,
    } = serde_json::from_slice(
        &read_len_bytes(&mut stream, MAX_JSON_FRAME)
            .await
            .context("reading sync hello header")?,
    )?;
    let manifest = Arc::new(manifest);

    let Some((node, context)) = resolve(name.clone(), manifest.clone()).await? else {
        bail!("no local sync entry named {name:?}");
    };

    // 2. Snapshot BEFORE adopting so the client still pushes the content we need,
    // then adopt the client's winning entries (content arrives in the Push).
    let (reply, blobs_for_client, pushed) = {
        let mut node = node.lock().await;
        let server_manifest = node.manifest().clone();
        // Content the client should adopt from us (present entries where we win)
        // plus anything the client explicitly reported missing.
        let mut client_needs = node.hashes_peer_needs(&manifest);
        for hash in &wanted {
            if !client_needs.contains(hash) {
                client_needs.push(*hash);
            }
        }
        let blobs = node.gather_content(&client_needs);
        let pushed = node.adopt(&manifest);
        // The reply advertises what WE are still missing so the client repairs us.
        let reply = ReplyHeader {
            manifest: server_manifest,
            wanted: node.missing_content_hashes(),
        };
        (reply, blobs, pushed)
    };

    // 3. Send reply header + our content bundle, then read the client's push.
    write_len_bytes(&mut stream, &serde_json::to_vec(&reply)?).await?;
    write_blobs(&mut stream, &blobs_for_client).await?;
    stream.flush().await?;

    let received = read_blobs_into(&mut stream, &node).await?;

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
        },
        context,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn author(n: u8) -> super::super::manifest::Author {
        super::super::manifest::Author([n; 32])
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
            run_server(server_end, move |name, _| async move {
                assert_eq!(name, "cat");
                Ok(Some((b_for_server, ())))
            })
            .await
        });
        let client = run_client(client_end, a.clone(), "cat").await.unwrap();
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
                run_server(s, move |_, _| async move { Ok(Some((b2, ()))) }).await
            });
            run_client(c, a.clone(), "cat").await.unwrap();
            srv.await.unwrap().unwrap();
        }
        // Second session after convergence transfers no content.
        let (c, s) = tokio::io::duplex(1 << 20);
        let b2 = b.clone();
        let srv = tokio::spawn(async move {
            run_server(s, move |_, _| async move { Ok(Some((b2, ()))) }).await
        });
        let stats = run_client(c, a.clone(), "cat").await.unwrap();
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
            run_server(s, move |_, _| async move { Ok(Some((b2, ()))) }).await
        });
        run_client(c, a.clone(), "cat").await.unwrap();
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
                run_server(server, move |_, _| async move { Ok(Some((b, ()))) }).await
            });
            let client_stats = run_client(client, a, "bus").await.unwrap();
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
    use super::tests::*;
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
            run_server(server_end, move |_, _| async move { Ok(Some((b_for_server, ()))) }).await
        });
        let stats = run_client(client_end, a, "cat").await.unwrap();
        let _ = server.await.unwrap().unwrap();
        stats
    }

    /// A CONVERGED PASS SHIPS NOTHING AND STILL COSTS A MANIFEST.
    ///
    /// `bytes` counts content blobs, and a converged pass transfers no content,
    /// so by that measure a no-op reconcile is free. It is not: both sides send
    /// their entire manifest in the handshake whether or not anything changed.
    /// On the bus entry that is about 10 MB, every pass, per peer.
    ///
    /// A cost measurement that reports zero here would hide the exact term delta
    /// replication exists to remove, which is the only reason this counter was
    /// added.
    #[tokio::test]
    async fn a_converged_pass_transfers_no_content_and_still_ships_a_manifest() {
        let a = Arc::new(Mutex::new(SyncNode::new(author(1))));
        let b = Arc::new(Mutex::new(SyncNode::new(author(2))));
        // Enough entries that the manifest is unmistakably the dominant term.
        for i in 0..200 {
            let name = format!("file-{i:03}.md");
            a.lock().await.local_write(&name, b"x", 0, 0);
        }
        // Converge them.
        reconcile_once(a.clone(), b.clone()).await;
        let second = reconcile_once(a.clone(), b.clone()).await;

        assert!(
            second.is_noop(),
            "the second pass was not converged, so this measures the wrong thing"
        );
        assert_eq!(
            second.bytes, 0,
            "a converged pass moved content, so the fixture is wrong"
        );
        assert!(
            second.wire_bytes > 4_000,
            "a converged pass reported {} wire bytes; the manifests are not being \
             counted, and the manifest is the whole cost",
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
