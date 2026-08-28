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
/// A limit rather than none, because the receiving side allocates against it and
/// a peer should not be able to fill a disk by accident. Large enough for the
/// things people actually send one at a time.
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

/// Send one file. The initiating half.
pub async fn send<S>(mut stream: S, name: &str, bytes: &[u8]) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Checked here so the refusal reaches the person who typed it, and again on
    // arrival because the receiver cannot trust this one.
    if !name_is_safe(name) {
        bail!(
            "{name:?} is not a name fabric will write. Use a relative path with \
             no parent components"
        );
    }
    if bytes.len() as u64 > MAX_FILE_BYTES {
        bail!(
            "{} bytes is larger than the {MAX_FILE_BYTES} byte limit for one \
             transfer",
            bytes.len()
        );
    }
    let header = serde_json::to_vec(&Header {
        name: name.to_string(),
        len: bytes.len() as u64,
    })?;
    stream.write_all(&(header.len() as u32).to_be_bytes()).await?;
    stream.write_all(&header).await?;
    stream.write_all(bytes).await?;
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

    let mut body = vec![0u8; header.len as usize];
    stream.read_exact(&mut body).await?;

    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    // Temp then rename, so an interrupted transfer never appears at the final
    // path as a complete file.
    let temp = target.with_extension("fabric-partial");
    std::fs::write(&temp, &body).with_context(|| format!("writing {}", temp.display()))?;
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
