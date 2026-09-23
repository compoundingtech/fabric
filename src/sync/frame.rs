//! The `fabric/sync/1` framing the core needs without the engine: length
//! prefixed frames, an idle bound for a relayed stream, and the one reply a
//! daemon with no sync owner sends to a peer's hello.
//!
//! The engine crate builds its sessions on these same helpers, so the bytes a
//! refusal writes are exactly the bytes a session would.

use std::{
    io,
    pin::Pin,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

/// The largest JSON frame (a hello or a reply header) either side accepts.
pub const MAX_JSON_FRAME: usize = 64 * 1024 * 1024;

pub async fn write_u32<W: AsyncWrite + Unpin>(w: &mut W, v: u32) -> Result<()> {
    w.write_all(&v.to_be_bytes()).await?;
    Ok(())
}

pub async fn read_u32<R: AsyncRead + Unpin>(r: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).await?;
    Ok(u32::from_be_bytes(buf))
}

pub async fn write_len_bytes<W: AsyncWrite + Unpin>(w: &mut W, bytes: &[u8]) -> Result<()> {
    write_u32(w, bytes.len() as u32).await?;
    w.write_all(bytes).await?;
    Ok(())
}

pub async fn read_len_bytes<R: AsyncRead + Unpin>(r: &mut R, max: usize) -> Result<Vec<u8>> {
    let len = read_u32(r).await? as usize;
    if len > max {
        bail!("sync frame of {len} bytes exceeds limit {max}");
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// The phrase a daemon without a sync owner puts in its error reply. The
/// client side classifies it as `unavailable`: not a refusal a person must fix
/// in `peers.toml`, and not weather the engine should wait out.
pub const SYNC_UNAVAILABLE_MARKER: &str = "sync is unavailable on this peer";

/// An empty manifest, byte for byte what the engine's `Manifest::new()`
/// serializes to. The engine crate pins the two against each other.
#[derive(Debug, Serialize, Deserialize)]
pub struct EmptyManifest {
    pub entries: std::collections::BTreeMap<String, serde_json::Value>,
}

/// The reply header shape, with an empty manifest and the error set. An older
/// client ignores `error` and still expects the bundle count that follows.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorReplyHeader {
    pub manifest: EmptyManifest,
    pub wanted: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub digest: String,
    #[serde(default)]
    pub is_delta: bool,
}

impl ErrorReplyHeader {
    pub fn new(message: &str) -> Self {
        Self {
            manifest: EmptyManifest {
                entries: Default::default(),
            },
            wanted: Vec::new(),
            error: Some(message.to_string()),
            digest: String::new(),
            is_delta: false,
        }
    }
}

/// Write one error reply and the empty bundle count an older client expects.
pub async fn write_error_reply<W: AsyncWrite + Unpin>(w: &mut W, message: &str) -> Result<()> {
    let reply = ErrorReplyHeader::new(message);
    write_len_bytes(w, &serde_json::to_vec(&reply)?).await?;
    write_u32(w, 0).await?;
    w.flush().await?;
    Ok(())
}

/// Answer a peer's hello with one error reply and nothing else.
///
/// The hello is read and discarded first, because the client writes it before
/// it reads anything and a large manifest would otherwise block behind the
/// unread bytes. Nothing about the hello is acted on.
pub async fn refuse_hello<S>(mut stream: S, message: &str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _hello = read_len_bytes(&mut stream, MAX_JSON_FRAME)
        .await
        .context("reading the sync hello before refusing it")?;
    write_error_reply(&mut stream, message).await?;
    let _ = stream.shutdown().await;
    Ok(())
}

pub struct IdleTimeoutStream<S> {
    inner: S,
    peer: String,
    idle_timeout: Duration,
    read_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    write_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
}

/// Wrap a stream so that `idle_timeout` without progress ends it with an error.
pub fn idle_timeout_stream<S>(inner: S, peer: &str, idle_timeout: Duration) -> IdleTimeoutStream<S> {
    IdleTimeoutStream::new(inner, peer, idle_timeout)
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
