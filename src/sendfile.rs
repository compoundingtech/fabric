//! One-shot file transfer between two peers.
//!
//! scp-shaped: one file, one direction, no state kept afterwards and nothing
//! ever deleted. It carries none of folder sync's risk because it does none of
//! folder sync's work — there is no manifest, no reconciliation, and no way for
//! this to remove anything.
//!
//! # Where a file lands, and why the sender does not choose
//!
//! **This is a remote write primitive.** A sender that names its own destination
//! can write anywhere the receiving daemon can, and `../../.ssh/authorized_keys`
//! is the obvious end of that. So the RECEIVER decides: everything arrives under
//! a per-peer inbox in the fabric home, and the sender may only name a relative
//! path inside it.
//!
//! That is the same rule as everywhere else here — the side being acted on
//! decides what is allowed — and it means a person always knows where things
//! arrive without reading the sender's command line.
//!
//! The escape check runs on BOTH sides. The sender checks so the error arrives
//! in front of the person who typed it. The receiver checks because it must
//! never trust the sender, and that is the check that matters.

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::config::FabricHome;

pub const SEND_FILE_ALPN: &[u8] = b"fabric/send-file/0";

/// The name a permission is written against: `allow = ["send-file"]`.
pub const SERVICE: &str = "send-file";

/// The largest file this will move in one shot.
///
/// A limit rather than none, because the receiving side writes it to disk and a
/// peer should not be able to fill a disk by accident. The body streams in
/// bounded chunks, so this is not a memory allocation on either side. Large
/// enough for the things people actually send one at a time.
pub const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    /// A RELATIVE path inside the receiver's inbox. Never absolute, never with
    /// a parent component; both are refused on arrival.
    name: String,
    len: u64,
}

/// Where files from `peer` arrive.
pub fn inbox_for(home: &FabricHome, peer: &str) -> PathBuf {
    home.root().join("inbox").join(sanitize_peer(peer))
}

/// A peer's directory name, with anything path-shaped removed.
///
/// ANYTHING IN A MESSAGE THAT NAMES A LOCATION ON MY DISK IS HOSTILE INPUT
/// UNTIL PROVEN OTHERWISE. That is the rule, and this function exists because I
/// nearly broke it.
///
/// The obvious hostile field here is the file name, and it is checked. The peer
/// LABEL is the one that reads as internal and is not: it also comes off the
/// wire, it also names a directory, and a peer calling itself `/etc` would
/// otherwise place its inbox there. That is a remote arbitrary write reachable
/// by any trusted peer.
///
/// The daemon takes the peer id from the CONNECTION rather than from anything
/// the sender said, and this sanitisation is the second line behind that. When
/// adding a field to this protocol, ask whether it can name a path, and assume
/// it will.
fn sanitize_peer(peer: &str) -> String {
    let cleaned: String = peer
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    if cleaned.is_empty() {
        "unknown".to_string()
    } else {
        cleaned
    }
}

/// Is this a name a sender may write inside the inbox?
///
/// Refuses anything absolute, anything with a parent or root component, and
/// anything empty. A name that survives this can only land inside the inbox.
pub fn name_is_safe(name: &str) -> bool {
    let name = name.trim();
    if name.is_empty() {
        return false;
    }
    let path = Path::new(name);
    if path.is_absolute() {
        return false;
    }
    path.components().all(|component| {
        matches!(component, Component::Normal(part) if !part.is_empty())
    })
}

/// Resolve where a named file lands, refusing anything that escapes.
pub fn destination(home: &FabricHome, peer: &str, name: &str) -> Result<PathBuf> {
    if !name_is_safe(name) {
        bail!(
            "{name:?} is not a name this can write. A destination must be a \
             relative path with no parent components; everything arrives under \
             the inbox for the peer that sent it"
        );
    }
    Ok(inbox_for(home, peer).join(name))
}

/// Send one file from a path, streaming it. The initiating half.
///
/// Opens the file and streams its bytes to the peer in bounded chunks, so the
/// sending daemon never holds the whole file in memory. Previously the caller
/// read the file whole with `std::fs::read` and this held the whole slice, so a
/// 1.5 GiB transfer cost about 1.5 GiB on each side at once. Finding 7 of the
/// 2026-08-29 review.
pub async fn send_file<S>(stream: S, name: &str, path: &Path) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // The size comes from the file's metadata rather than from reading it, so
    // the sender never allocates against it either.
    let len = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("could not stat {}", path.display()))?
        .len();
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("could not open {}", path.display()))?;
    send_from_reader(stream, name, len, file).await
}

/// Send one file whose bytes are already in hand. A convenience over
/// [`send_from_reader`] for small payloads and the tests.
pub async fn send<S>(stream: S, name: &str, bytes: &[u8]) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send_from_reader(stream, name, bytes.len() as u64, bytes).await
}

/// Send exactly `len` bytes read from `reader`, streaming them to `stream`.
///
/// The whole file never lands in one buffer: `tokio::io::copy` moves it in
/// bounded chunks. A source that turns out shorter than `len` is caught, and
/// one that is longer is bounded by `take(len)`.
pub async fn send_from_reader<S, R>(mut stream: S, name: &str, len: u64, reader: R) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    // Checked here so the refusal reaches the person who typed it, and again on
    // arrival because the receiver cannot trust this one.
    if !name_is_safe(name) {
        bail!(
            "{name:?} is not a name fabric will write. Use a relative path with \
             no parent components"
        );
    }
    if len > MAX_FILE_BYTES {
        bail!("{len} bytes is larger than the {MAX_FILE_BYTES} byte limit for one transfer");
    }
    let header = serde_json::to_vec(&Header {
        name: name.to_string(),
        len,
    })?;
    stream.write_all(&(header.len() as u32).to_be_bytes()).await?;
    stream.write_all(&header).await?;
    // Exactly `len` bytes, streamed rather than buffered.
    let copied = tokio::io::copy(&mut reader.take(len), &mut stream).await?;
    if copied != len {
        bail!("read {copied} of the {len} bytes the file was said to hold");
    }
    stream.flush().await?;

    // Wait for the receiver to say it committed the file. Without this the
    // sender reports success for a transfer the far side may have refused.
    let mut ack = [0u8; 1];
    stream
        .read_exact(&mut ack)
        .await
        .context("the receiver closed before confirming the file")?;
    if ack[0] != 1 {
        bail!("the receiver refused the file");
    }
    Ok(())
}

/// Receive one file. The accepting half.
///
/// Returns where it landed.
pub async fn receive<S>(mut stream: S, home: &FabricHome, peer: &str) -> Result<PathBuf>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_HEADER_BYTES {
        bail!("send-file header of {len} bytes exceeds the limit");
    }
    let mut raw = vec![0u8; len];
    stream.read_exact(&mut raw).await?;
    let header: Header = serde_json::from_slice(&raw)?;

    if header.len > MAX_FILE_BYTES {
        bail!(
            "{} bytes is larger than the {MAX_FILE_BYTES} byte limit",
            header.len
        );
    }
    // THE CHECK THAT MATTERS. The sender ran one too, and that one is a
    // courtesy; this one is the boundary.
    let target = destination(home, peer, &header.name)?;

    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    // Temp then rename, so an interrupted transfer never appears at the final
    // path as a complete file. The body streams straight to the temp file in
    // bounded chunks, so the receiver never allocates against `header.len`.
    let temp = target.with_extension("fabric-partial");
    let copied = {
        let mut file = tokio::fs::File::create(&temp)
            .await
            .with_context(|| format!("writing {}", temp.display()))?;
        // Read EXACTLY header.len bytes. `take` stops the copy at the limit, so
        // it never waits for an EOF the sender does not send before its ack.
        let copied = tokio::io::copy(&mut (&mut stream).take(header.len), &mut file)
            .await
            .with_context(|| format!("receiving into {}", temp.display()))?;
        file.flush()
            .await
            .with_context(|| format!("flushing {}", temp.display()))?;
        copied
    };
    if copied != header.len {
        // The sender closed early or the transport dropped. Do not commit a
        // short file, and do not ack it.
        let _ = std::fs::remove_file(&temp);
        bail!("received {copied} of {} bytes before the stream ended", header.len);
    }
    std::fs::rename(&temp, &target)
        .with_context(|| format!("renaming into {}", target.display()))?;

    stream.write_all(&[1u8]).await?;
    stream.flush().await?;
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_that_escapes_the_inbox_is_refused() {
        assert!(name_is_safe("notes.md"));
        assert!(name_is_safe("sub/dir/notes.md"));

        // The whole point.
        assert!(!name_is_safe("../notes.md"));
        assert!(!name_is_safe("sub/../../notes.md"));
        assert!(!name_is_safe("/etc/passwd"));
        assert!(!name_is_safe("/home/myobie/.ssh/authorized_keys"));
        assert!(!name_is_safe(".."));
        assert!(!name_is_safe(""));
        assert!(!name_is_safe("   "));
    }

    /// The peer label arrives over the wire too, so it is not trusted either.
    #[test]
    fn a_peer_calling_itself_a_path_cannot_escape_the_inbox() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let inbox_root = home.root().join("inbox");
        for hostile in ["../..", "/etc", "..", "a/../../b"] {
            let path = inbox_for(&home, hostile);
            assert!(
                path.starts_with(&inbox_root),
                "a peer calling itself {hostile:?} placed its inbox at {} , \
                 outside {}",
                path.display(),
                inbox_root.display()
            );
        }
    }

    #[test]
    fn a_destination_always_lands_inside_the_peers_inbox() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let inbox = inbox_for(&home, "hetz");

        let ok = destination(&home, "hetz", "sub/notes.md").unwrap();
        assert!(ok.starts_with(&inbox));

        let refused = destination(&home, "hetz", "../../.ssh/authorized_keys");
        let error = refused.expect_err("an escaping destination was accepted");
        assert!(
            format!("{error:#}").contains("not a name this can write"),
            "the refusal did not say why: {error:#}"
        );
    }

    #[tokio::test]
    async fn a_file_arrives_intact_and_where_it_was_addressed() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();

        let (client, server) = tokio::io::duplex(1 << 20);
        let home_for_server = FabricHome::new(dir.path());
        let receiver =
            tokio::spawn(async move { receive(server, &home_for_server, "hetz").await });
        send(client, "sub/notes.bin", &payload).await.unwrap();
        let landed = receiver.await.unwrap().unwrap();

        assert_eq!(landed, inbox_for(&home, "hetz").join("sub/notes.bin"));
        assert_eq!(std::fs::read(&landed).unwrap(), payload);
        assert!(
            !landed.with_extension("fabric-partial").exists(),
            "the partial file was left behind"
        );
    }

    /// A file larger than any single buffer streams through send_from_reader and
    /// the streaming receiver intact. This exercises the finding-7 path: neither
    /// side allocates against the whole size, so many chunks cross the duplex.
    #[tokio::test]
    async fn a_large_file_streams_through_in_chunks() {
        let dir = tempfile::tempdir().unwrap();
        // A few MiB, well past the internal copy buffer, with a position-varying
        // pattern so a chunk written at the wrong offset would be caught.
        let payload: Vec<u8> = (0..(5 * 1024 * 1024u32)).map(|i| (i % 251) as u8).collect();

        let (client, server) = tokio::io::duplex(64 * 1024);
        let home_for_server = FabricHome::new(dir.path());
        let receiver =
            tokio::spawn(async move { receive(server, &home_for_server, "hetz").await });
        // send_from_reader with a reader (not a held slice) is the streaming API
        // the daemon uses for a file on disk.
        send_from_reader(client, "big.bin", payload.len() as u64, payload.as_slice())
            .await
            .unwrap();
        let landed = receiver.await.unwrap().unwrap();
        assert_eq!(std::fs::read(&landed).unwrap(), payload);
        assert!(!landed.with_extension("fabric-partial").exists());
    }

    /// A sender that promises more bytes than it delivers must not leave a
    /// committed file: the receiver reads exactly `len`, sees the stream end
    /// short, and refuses rather than writing a truncated file to the inbox.
    #[tokio::test]
    async fn a_short_stream_is_refused_and_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let (mut client, server) = tokio::io::duplex(1 << 16);
        let home_for_server = FabricHome::new(dir.path());
        let receiver =
            tokio::spawn(async move { receive(server, &home_for_server, "hetz").await });

        // Hand-write a header claiming 1000 bytes, then send 10 and close.
        let header = serde_json::to_vec(&Header {
            name: "short.bin".to_string(),
            len: 1000,
        })
        .unwrap();
        client
            .write_all(&(header.len() as u32).to_be_bytes())
            .await
            .unwrap();
        client.write_all(&header).await.unwrap();
        client.write_all(&[7u8; 10]).await.unwrap();
        client.shutdown().await.unwrap();
        drop(client);

        let result = receiver.await.unwrap();
        assert!(result.is_err(), "a short transfer must not be committed");
        let target = inbox_for(&home, "hetz").join("short.bin");
        assert!(!target.exists(), "a truncated file reached the inbox");
        assert!(!target.with_extension("fabric-partial").exists());
    }

    /// The receiver refuses an escaping name even when the sender does not check.
    ///
    /// The sender's check is a courtesy for whoever typed the command. This one
    /// is the boundary, and it is the only one that would still be there if the
    /// peer were hostile.
    #[tokio::test]
    async fn the_receiver_refuses_an_escape_the_sender_did_not_catch() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());

        let (mut client, server) = tokio::io::duplex(1 << 16);
        let home_for_server = FabricHome::new(dir.path());
        let receiver =
            tokio::spawn(async move { receive(server, &home_for_server, "hetz").await });

        // Hand-built, bypassing `send` entirely, the way a hostile peer would.
        let header = serde_json::to_vec(&Header {
            name: "../../escaped.txt".to_string(),
            len: 5,
        })
        .unwrap();
        client
            .write_all(&(header.len() as u32).to_be_bytes())
            .await
            .unwrap();
        client.write_all(&header).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        client.flush().await.unwrap();

        let result = receiver.await.unwrap();
        assert!(
            result.is_err(),
            "the receiver accepted a path that escapes its inbox"
        );
        assert!(
            !home.root().join("../../escaped.txt").exists(),
            "a file was written outside the inbox"
        );
    }
}
