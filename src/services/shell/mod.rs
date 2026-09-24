//! `fabric shell`: a login shell on a peer's PTY, the terminal handling on the
//! local side, and the service that serves it.

use std::{
    io::{Read, Write},
    sync::mpsc as std_mpsc,
};

use anyhow::{Context, Result, bail};
use fabric_service_api::{BoxFuture, Bridge, Notice, PeerStream, Protocol, Service};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

/// Legacy one-shot shell framing. This ALPN is wire-compatible with every
/// released Fabric shell and must never carry generic tunnel frames.
pub const SHELL_ALPN: &[u8] = b"fabric/shell/0";
pub const SHELL_PROTOCOL: &str = "fabric/shell/0";
/// Resumable shell framing carried by the generic tunnel session protocol.
pub const RESUMABLE_SHELL_ALPN: &[u8] = b"fabric/shell/1";
/// The word a peer's allow array uses for this service. Both wire versions
/// answer to it: a permission is about the service, not about which version
/// negotiated it.
pub const SERVICE: &str = "shell";

pub mod client;
pub mod terminal;

const MAX_FRAME_LEN: usize = 1024 * 1024;
const CLIENT_STDIN: u8 = 1;
const CLIENT_RESIZE: u8 = 2;
const CLIENT_EOF: u8 = 3;
const SERVER_OUTPUT: u8 = 17;
const SERVER_EXIT: u8 = 18;
const SERVER_ERROR: u8 = 19;
const SERVER_STATUS: u8 = 20;
pub const EXIT_SHELL_DISABLED: i32 = fabric_service_api::REFUSED_EXIT_CODE;

#[derive(Debug)]
pub enum ClientFrame {
    Stdin(Vec<u8>),
    Resize { rows: u16, cols: u16 },
    Eof,
}

#[derive(Debug)]
pub enum ServerFrame {
    Output(Vec<u8>),
    Exit(i32),
    Error(String),
    Status(String),
}

pub async fn serve_shell_disabled<W>(send: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    serve_shell_failure(
        send,
        "refused service \"shell\": add shell to this peer's allow array in peers.toml",
        EXIT_SHELL_DISABLED,
    )
    .await
}

/// Send a complete failure response when a shell session cannot start.
pub async fn serve_shell_failure<W>(send: &mut W, message: &str, exit_code: i32) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_server_frame(send, ServerFrame::Error(message.to_string())).await?;
    write_server_frame(send, ServerFrame::Exit(exit_code)).await
}

const PROTOCOLS: &[Protocol] = &[
    Protocol {
        alpn: RESUMABLE_SHELL_ALPN,
        resumable: true,
        accept_event: "builtin_resumable_shell_accept",
    },
    Protocol {
        alpn: SHELL_ALPN,
        resumable: false,
        accept_event: "builtin_legacy_shell_accept",
    },
];

/// The shell service, as the base network sees it.
#[derive(Debug, Default)]
pub struct Shell;

impl Service for Shell {
    fn name(&self) -> &'static str {
        SERVICE
    }

    fn protocols(&self) -> &'static [Protocol] {
        PROTOCOLS
    }

    /// One PTY per stream. Inside a resumable session the stream is the
    /// session's, and `closed` ends the PTY when the session is reaped.
    fn serve(&self, stream: PeerStream) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let PeerStream {
                peer,
                mut read,
                mut write,
                closed,
                ..
            } = stream;
            serve_shell_session_until(&mut read, &mut write, &peer, closed).await?;
            write.shutdown().await?;
            Ok(())
        })
    }

    fn notice(&self, notice: &Notice<'_>) -> Option<Vec<u8>> {
        let encoded = match notice {
            Notice::Refused { error } => {
                let mut bytes =
                    encode_server_error(&format!("refused service \"shell\": {error}")).ok()?;
                bytes.extend(encode_frame(SERVER_EXIT, &EXIT_SHELL_DISABLED.to_be_bytes()).ok()?);
                return Some(bytes);
            }
            Notice::Unavailable { .. } => return None,
            Notice::FallingBack => {
                encode_server_status("peer does not support resumable shell; using legacy shell/0")
            }
            Notice::Probing { error, delay } => encode_server_status(&format!(
                "connection unavailable ({error}); probing remote shell protocol again in {:.1}s",
                delay.as_secs_f32()
            )),
            Notice::RetryingFallback { error, delay } => encode_server_status(&format!(
                "legacy shell unavailable ({error}); retrying before session start in {:.1}s",
                delay.as_secs_f32()
            )),
            Notice::Reconnecting {
                error,
                attempt,
                delay,
            } => encode_server_status(&format!(
                "connection lost ({error}); reconnecting attempt {attempt} in {:.1}s",
                delay.as_secs_f32()
            )),
            Notice::Resumed => {
                encode_server_status("connection restored; remote shell session resumed")
            }
            Notice::ResumeFailed { error } => {
                encode_server_error(&format!("remote shell could not resume: {error}"))
            }
        };
        encoded.ok()
    }

    /// A refusal arrives as Error and Exit on a stream whose request side the
    /// peer already closed.
    fn bridge(&self) -> Bridge {
        Bridge::WholeReply
    }
}

pub async fn serve_shell_session<R, W>(recv: &mut R, send: &mut W, peer: &str) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_shell_session_until(recv, send, peer, CancellationToken::new()).await
}

pub async fn serve_shell_session_until<R, W>(
    recv: &mut R,
    send: &mut W,
    peer: &str,
    cancel: CancellationToken,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let pty_system = native_pty_system();
    let pair = pty_system.openpty(PtySize::default())?;
    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string());
    let mut command = CommandBuilder::new(shell);
    // Markers so a user's shell rc can tell it's inside a fabric shell (which runs
    // in the daemon's session, not the caller's) and skip session-fragile startup.
    // FABRIC_PEER is the connecting peer's NodeID — who opened this shell.
    command.env("FABRIC_SHELL", "1");
    command.env("FABRIC_PEER", peer);
    let mut child = pair.slave.spawn_command(command)?;
    let mut child_killer = child.clone_killer();
    let mut reader = pair.master.try_clone_reader()?;
    let mut writer = pair.master.take_writer()?;
    let master = pair.master;
    drop(pair.slave);

    let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let reader_task = tokio::task::spawn_blocking(move || {
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if output_tx.send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let (input_tx, input_rx) = std_mpsc::channel::<Option<Vec<u8>>>();
    let writer_task = tokio::task::spawn_blocking(move || {
        for chunk in input_rx {
            let Some(chunk) = chunk else {
                break;
            };
            if writer.write_all(&chunk).is_err() {
                break;
            }
            let _ = writer.flush();
        }
    });

    let mut wait_task = tokio::task::spawn_blocking(move || child.wait());
    let mut stdin_done = false;
    let mut output_done = false;
    let mut exit_code = None;

    while !output_done || exit_code.is_none() {
        tokio::select! {
            frame = read_client_frame(recv), if !stdin_done => {
                match frame? {
                    Some(ClientFrame::Stdin(bytes)) => {
                        let _ = input_tx.send(Some(bytes));
                    }
                    Some(ClientFrame::Resize { rows, cols }) => {
                        master.resize(PtySize {
                            rows,
                            cols,
                            pixel_width: 0,
                            pixel_height: 0,
                        })?;
                    }
                    Some(ClientFrame::Eof) | None => {
                        let _ = input_tx.send(None);
                        stdin_done = true;
                    }
                }
            }
            output = output_rx.recv(), if !output_done => {
                match output {
                    Some(bytes) => write_server_frame(send, ServerFrame::Output(bytes)).await?,
                    None => output_done = true,
                }
            }
            status = &mut wait_task, if exit_code.is_none() => {
                let status = status.context("shell wait task failed")??;
                let code = status.exit_code().min(i32::MAX as u32) as i32;
                exit_code = Some(code);
                let _ = input_tx.send(None);
            }
            _ = cancel.cancelled() => {
                let _ = tokio::task::spawn_blocking(move || child_killer.kill()).await;
                let _ = input_tx.send(None);
                let _ = wait_task.await;
                let _ = reader_task.await;
                let _ = writer_task.await;
                return Ok(());
            }
        }
    }

    let _ = reader_task.await;
    let _ = writer_task.await;
    write_server_frame(send, ServerFrame::Exit(exit_code.unwrap_or(1))).await
}

pub async fn read_client_frame<R>(read: &mut R) -> Result<Option<ClientFrame>>
where
    R: AsyncRead + Unpin,
{
    let Some((kind, payload)) = read_frame(read).await? else {
        return Ok(None);
    };
    match kind {
        CLIENT_STDIN => Ok(Some(ClientFrame::Stdin(payload))),
        CLIENT_RESIZE => {
            if payload.len() != 4 {
                bail!("invalid resize frame length {}", payload.len());
            }
            Ok(Some(ClientFrame::Resize {
                rows: u16::from_be_bytes([payload[0], payload[1]]),
                cols: u16::from_be_bytes([payload[2], payload[3]]),
            }))
        }
        CLIENT_EOF => Ok(Some(ClientFrame::Eof)),
        _ => bail!("unknown shell client frame {kind}"),
    }
}

pub async fn write_client_stdin<W>(write: &mut W, bytes: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_frame(write, CLIENT_STDIN, bytes).await
}

pub async fn write_client_resize<W>(write: &mut W, rows: u16, cols: u16) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut payload = Vec::with_capacity(4);
    payload.extend_from_slice(&rows.to_be_bytes());
    payload.extend_from_slice(&cols.to_be_bytes());
    write_frame(write, CLIENT_RESIZE, &payload).await
}

pub async fn write_client_eof<W>(write: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_frame(write, CLIENT_EOF, &[]).await
}

pub async fn read_server_frame<R>(read: &mut R) -> Result<Option<ServerFrame>>
where
    R: AsyncRead + Unpin,
{
    let Some((kind, payload)) = read_frame(read).await? else {
        return Ok(None);
    };
    match kind {
        SERVER_OUTPUT => Ok(Some(ServerFrame::Output(payload))),
        SERVER_EXIT => {
            if payload.len() != 4 {
                bail!("invalid exit frame length {}", payload.len());
            }
            Ok(Some(ServerFrame::Exit(i32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]))))
        }
        SERVER_ERROR => Ok(Some(ServerFrame::Error(String::from_utf8(payload)?))),
        SERVER_STATUS => Ok(Some(ServerFrame::Status(String::from_utf8(payload)?))),
        _ => bail!("unknown shell server frame {kind}"),
    }
}

pub fn encode_server_status(message: &str) -> Result<Vec<u8>> {
    encode_frame(SERVER_STATUS, message.as_bytes())
}

pub fn encode_server_error(message: &str) -> Result<Vec<u8>> {
    encode_frame(SERVER_ERROR, message.as_bytes())
}

async fn write_server_frame<W>(write: &mut W, frame: ServerFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match frame {
        ServerFrame::Output(bytes) => write_frame(write, SERVER_OUTPUT, &bytes).await,
        ServerFrame::Exit(code) => write_frame(write, SERVER_EXIT, &code.to_be_bytes()).await,
        ServerFrame::Error(message) => write_frame(write, SERVER_ERROR, message.as_bytes()).await,
        ServerFrame::Status(message) => write_frame(write, SERVER_STATUS, message.as_bytes()).await,
    }
}

async fn read_frame<R>(read: &mut R) -> Result<Option<(u8, Vec<u8>)>>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; 5];
    if let Err(error) = read.read_exact(&mut header).await {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error.into());
    }

    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if len > MAX_FRAME_LEN {
        bail!("shell frame too large: {len} bytes");
    }

    let mut payload = vec![0; len];
    read.read_exact(&mut payload).await?;
    Ok(Some((header[0], payload)))
}

async fn write_frame<W>(write: &mut W, kind: u8, payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write.write_all(&encode_frame(kind, payload)?).await?;
    Ok(())
}

fn encode_frame(kind: u8, payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > MAX_FRAME_LEN {
        bail!("shell frame too large: {} bytes", payload.len());
    }

    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(kind);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::{Notice, ServerFrame, Service, Shell, encode_server_status, read_server_frame};
    use std::time::Duration;

    async fn frames(bytes: Vec<u8>) -> Vec<ServerFrame> {
        let mut read = bytes.as_slice();
        let mut frames = Vec::new();
        while let Some(frame) = read_server_frame(&mut read).await.unwrap() {
            frames.push(frame);
        }
        frames
    }

    async fn only_status(notice: Notice<'_>) -> String {
        match frames(Shell.notice(&notice).unwrap()).await.as_slice() {
            [ServerFrame::Status(message)] => message.clone(),
            other => panic!("expected one status frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_refusal_reaches_the_terminal_as_error_then_126() {
        let bytes = Shell.notice(&Notice::Refused { error: "no" }).unwrap();
        assert!(matches!(
            frames(bytes).await.as_slice(),
            [ServerFrame::Error(message), ServerFrame::Exit(126)]
                if message == "refused service \"shell\": no"
        ));
    }

    #[tokio::test]
    async fn waits_in_progress_read_as_they_always_have() {
        let delay = Duration::from_millis(2_500);
        assert_eq!(
            only_status(Notice::FallingBack).await,
            "peer does not support resumable shell; using legacy shell/0"
        );
        assert_eq!(
            only_status(Notice::Probing {
                error: "gone: reset",
                delay
            })
            .await,
            "connection unavailable (gone: reset); probing remote shell protocol again in 2.5s"
        );
        assert_eq!(
            only_status(Notice::RetryingFallback {
                error: "gone",
                delay
            })
            .await,
            "legacy shell unavailable (gone); retrying before session start in 2.5s"
        );
        assert_eq!(
            only_status(Notice::Reconnecting {
                error: "lost",
                attempt: 3,
                delay
            })
            .await,
            "connection lost (lost); reconnecting attempt 3 in 2.5s"
        );
        assert_eq!(
            only_status(Notice::Resumed).await,
            "connection restored; remote shell session resumed"
        );
    }

    #[tokio::test]
    async fn a_session_that_cannot_resume_is_an_error() {
        let bytes = Shell
            .notice(&Notice::ResumeFailed { error: "expired" })
            .unwrap();
        assert!(matches!(
            frames(bytes).await.as_slice(),
            [ServerFrame::Error(message)] if message == "remote shell could not resume: expired"
        ));
        assert!(Shell.notice(&Notice::Unavailable { error: "x" }).is_none());
    }

    #[tokio::test]
    async fn reconnect_status_frame_round_trips() {
        let bytes = encode_server_status("reconnected").unwrap();
        let mut read = &bytes[..];
        assert!(matches!(
            read_server_frame(&mut read).await.unwrap(),
            Some(ServerFrame::Status(message)) if message == "reconnected"
        ));
    }
}
