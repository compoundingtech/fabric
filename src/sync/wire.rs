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
    write_len_bytes(&mut stream, &serde_json::to_vec(&hello)?).await?;
    stream.flush().await?;

    // 2. Read the server's reply header, then its content bundle into our store.
    let reply: ReplyHeader = serde_json::from_slice(
        &read_len_bytes(&mut stream, MAX_JSON_FRAME)
            .await
            .context("reading sync reply header")?,
    )?;
    let received = read_blobs_into(&mut stream, &node).await?;

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

    let sent: usize = for_server.1.iter().map(|(_, b)| b.len()).sum();
    Ok(Reconciled {
        pulled: for_server.0,
        pushed: for_server.1.len(),
        bytes: sent + received.bytes,
    })
}

/// Run the accepting side of a reconcile against a peer stream. Returns the sync
/// `name` the peer asked for (so the daemon can route to the right entry) and the
/// reconcile stats.
pub async fn run_server<S>(
    mut stream: S,
    resolve: impl FnOnce(&str) -> Option<Arc<Mutex<SyncNode>>>,
) -> Result<(String, Reconciled)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // 1. Read Hello.
    let hello: HelloHeader = serde_json::from_slice(
        &read_len_bytes(&mut stream, MAX_JSON_FRAME)
            .await
            .context("reading sync hello header")?,
    )?;

    let Some(node) = resolve(&hello.name) else {
        bail!("no local sync entry named {:?}", hello.name);
    };

    // 2. Snapshot BEFORE adopting so the client still pushes the content we need,
    // then adopt the client's winning entries (content arrives in the Push).
    let (reply, blobs_for_client, pushed) = {
        let mut node = node.lock().await;
        let server_manifest = node.manifest().clone();
        // Content the client should adopt from us (present entries where we win)
        // plus anything the client explicitly reported missing.
        let mut client_needs = node.hashes_peer_needs(&hello.manifest);
        for hash in &hello.wanted {
            if !client_needs.contains(hash) {
                client_needs.push(*hash);
            }
        }
        let blobs = node.gather_content(&client_needs);
        let pushed = node.adopt(&hello.manifest);
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

    let sent: usize = blobs_for_client.iter().map(|(_, b)| b.len()).sum();
    Ok((
        hello.name,
        Reconciled {
            pulled: received.blobs,
            pushed,
            bytes: sent + received.bytes,
        },
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
            run_server(server_end, move |name| {
                assert_eq!(name, "cat");
                Some(b_for_server)
            })
            .await
        });
        let client = run_client(client_end, a.clone(), "cat").await.unwrap();
        let (name, _server_stats) = server.await.unwrap().unwrap();
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
            let srv = tokio::spawn(async move { run_server(s, move |_| Some(b2)).await });
            run_client(c, a.clone(), "cat").await.unwrap();
            srv.await.unwrap().unwrap();
        }
        // Second session after convergence transfers no content.
        let (c, s) = tokio::io::duplex(1 << 20);
        let b2 = b.clone();
        let srv = tokio::spawn(async move { run_server(s, move |_| Some(b2)).await });
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
        let srv = tokio::spawn(async move { run_server(s, move |_| Some(b2)).await });
        run_client(c, a.clone(), "cat").await.unwrap();
        srv.await.unwrap().unwrap();

        let folder = b.lock().await.folder_state();
        assert_eq!(
            folder.get("job-hetz.toml").map(Vec::as_slice),
            Some(&b"host=hetz"[..])
        );
    }
}
