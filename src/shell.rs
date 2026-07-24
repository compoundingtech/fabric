use std::{
    io::{Read, Write},
    sync::mpsc as std_mpsc,
};

use anyhow::{Context, Result, bail};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};

pub const SHELL_ALPN: &[u8] = b"fabric/shell/0";
pub const SHELL_PROTOCOL: &str = "fabric/shell/0";

const MAX_FRAME_LEN: usize = 1024 * 1024;
const CLIENT_STDIN: u8 = 1;
const CLIENT_RESIZE: u8 = 2;
const CLIENT_EOF: u8 = 3;
const SERVER_OUTPUT: u8 = 17;
const SERVER_EXIT: u8 = 18;
const SERVER_ERROR: u8 = 19;

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
}

pub async fn serve_shell_disabled<W>(send: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_server_frame(
        send,
        ServerFrame::Error(
            "remote shell is disabled; start the server with `fabric up --allow-shell`".to_string(),
        ),
    )
    .await?;
    write_server_frame(send, ServerFrame::Exit(126)).await
}

pub async fn serve_shell_session<R, W>(recv: &mut R, send: &mut W, peer: &str) -> Result<()>
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
        _ => bail!("unknown shell server frame {kind}"),
    }
}

async fn write_server_frame<W>(write: &mut W, frame: ServerFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match frame {
        ServerFrame::Output(bytes) => write_frame(write, SERVER_OUTPUT, &bytes).await,
        ServerFrame::Exit(code) => write_frame(write, SERVER_EXIT, &code.to_be_bytes()).await,
        ServerFrame::Error(message) => write_frame(write, SERVER_ERROR, message.as_bytes()).await,
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
    if payload.len() > MAX_FRAME_LEN {
        bail!("shell frame too large: {} bytes", payload.len());
    }

    let mut header = [0u8; 5];
    header[0] = kind;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    write.write_all(&header).await?;
    write.write_all(payload).await?;
    Ok(())
}
