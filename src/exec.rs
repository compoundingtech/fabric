//! `fabric exec` — the scriptable, non-interactive counterpart to `shell`.
//!
//! Where `shell` runs a remote process on a PTY and pipes it interactively,
//! `exec` runs a command with no tty, captures its stdout and stderr as separate
//! streams, and propagates the remote process's exit code back as the local exit
//! code. That makes `fabric exec <peer> -- <cmd...>` safe to script over
//! (`out=$(fabric exec hetz -- cat /etc/hostname)`), with none of the
//! pipe-into-an-interactive-shell gymnastics.
//!
//! Security mirrors `shell`: this is arbitrary remote command execution, so it is
//! **default-deny per machine**. A daemon only runs an incoming exec if its own
//! config enables it (`allow_exec`, set via `--allow-exec`). Trust (`peers.toml`)
//! gates *who* may connect; `allow_exec` gates *whether* this node runs remote
//! commands at all. Both are required.

use anyhow::{Context, Result, bail};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::Command,
};

pub const EXEC_ALPN: &[u8] = b"fabric/exec/0";
pub const EXEC_PROTOCOL: &str = "fabric/exec/0";

const MAX_FRAME_LEN: usize = 1024 * 1024;
const CLIENT_ARGV: u8 = 1;
const SERVER_STDOUT: u8 = 17;
const SERVER_STDERR: u8 = 18;
const SERVER_EXIT: u8 = 19;
const SERVER_ERROR: u8 = 20;

/// Exit code sent when this node has `allow_exec` disabled (mirrors `shell`'s 126).
const EXIT_EXEC_DISABLED: i32 = 126;
/// Exit code sent when the requested command could not be spawned (mirrors sh 127).
const EXIT_SPAWN_FAILED: i32 = 127;

#[derive(Debug)]
pub enum ServerFrame {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
    Error(String),
}

/// Reply to an exec request when this node does not permit remote exec.
pub async fn serve_exec_disabled<W>(send: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_server_frame(
        send,
        ServerFrame::Error(
            "remote exec is disabled on this peer; enable it with `--allow-exec`".to_string(),
        ),
    )
    .await?;
    write_server_frame(send, ServerFrame::Exit(EXIT_EXEC_DISABLED)).await
}

/// Server side of an exec session: read the argv, spawn the command with no tty
/// and a null stdin, stream its stdout and stderr back as separate frames, then
/// send the process's exit code.
pub async fn serve_exec_session<R, W>(recv: &mut R, send: &mut W, peer: &str) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let argv = match read_argv(recv).await? {
        Some(argv) => argv,
        None => return Ok(()),
    };
    if argv.is_empty() {
        write_server_frame(send, ServerFrame::Error("empty command".to_string())).await?;
        return write_server_frame(send, ServerFrame::Exit(EXIT_SPAWN_FAILED)).await;
    }

    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        // Markers so the spawned command (and any shell it sources) can tell it is
        // running under fabric exec — in the daemon's session, not the caller's.
        // FABRIC_PEER is the connecting peer's NodeID — who ran this command.
        .env("FABRIC_EXEC", "1")
        .env("FABRIC_PEER", peer)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            write_server_frame(
                send,
                ServerFrame::Error(format!("failed to spawn {:?}: {error}", argv[0])),
            )
            .await?;
            return write_server_frame(send, ServerFrame::Exit(EXIT_SPAWN_FAILED)).await;
        }
    };

    let mut stdout = child.stdout.take().context("child stdout missing")?;
    let mut stderr = child.stderr.take().context("child stderr missing")?;
    let mut out_buf = [0u8; 8192];
    let mut err_buf = [0u8; 8192];
    let mut out_done = false;
    let mut err_done = false;

    // Drain both pipes concurrently so a chatty stderr can't deadlock stdout.
    while !out_done || !err_done {
        tokio::select! {
            result = stdout.read(&mut out_buf), if !out_done => match result? {
                0 => out_done = true,
                n => write_server_frame(send, ServerFrame::Stdout(out_buf[..n].to_vec())).await?,
            },
            result = stderr.read(&mut err_buf), if !err_done => match result? {
                0 => err_done = true,
                n => write_server_frame(send, ServerFrame::Stderr(err_buf[..n].to_vec())).await?,
            },
        }
    }

    let status = child.wait().await.context("exec wait failed")?;
    // `code()` is None when the child was killed by a signal; report 1 there.
    write_server_frame(send, ServerFrame::Exit(status.code().unwrap_or(1))).await
}

/// Client: send the command argv that the peer should run.
pub async fn write_client_argv<W>(write: &mut W, argv: &[String]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    // NUL-separated: argv members cannot contain a NUL byte, so this is lossless.
    let payload = argv.join("\0").into_bytes();
    write_frame(write, CLIENT_ARGV, &payload).await
}

/// Client: read the next server frame (stdout/stderr chunk, error, or exit).
pub async fn read_server_frame<R>(read: &mut R) -> Result<Option<ServerFrame>>
where
    R: AsyncRead + Unpin,
{
    let Some((kind, payload)) = read_frame(read).await? else {
        return Ok(None);
    };
    match kind {
        SERVER_STDOUT => Ok(Some(ServerFrame::Stdout(payload))),
        SERVER_STDERR => Ok(Some(ServerFrame::Stderr(payload))),
        SERVER_EXIT => {
            if payload.len() != 4 {
                bail!("invalid exit frame length {}", payload.len());
            }
            Ok(Some(ServerFrame::Exit(i32::from_be_bytes([
                payload[0], payload[1], payload[2], payload[3],
            ]))))
        }
        SERVER_ERROR => Ok(Some(ServerFrame::Error(String::from_utf8(payload)?))),
        _ => bail!("unknown exec server frame {kind}"),
    }
}

/// Server: read the argv frame the client sends first.
async fn read_argv<R>(read: &mut R) -> Result<Option<Vec<String>>>
where
    R: AsyncRead + Unpin,
{
    let Some((kind, payload)) = read_frame(read).await? else {
        return Ok(None);
    };
    if kind != CLIENT_ARGV {
        bail!("unexpected exec client frame {kind}");
    }
    if payload.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let text = String::from_utf8(payload).context("exec argv is not valid UTF-8")?;
    Ok(Some(text.split('\0').map(str::to_string).collect()))
}

async fn write_server_frame<W>(write: &mut W, frame: ServerFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    match frame {
        ServerFrame::Stdout(bytes) => write_frame(write, SERVER_STDOUT, &bytes).await,
        ServerFrame::Stderr(bytes) => write_frame(write, SERVER_STDERR, &bytes).await,
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
        bail!("exec frame too large: {len} bytes");
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
        bail!("exec frame too large: {} bytes", payload.len());
    }
    let mut header = [0u8; 5];
    header[0] = kind;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    write.write_all(&header).await?;
    write.write_all(payload).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // argv round-trips through the NUL-separated wire encoding, including args
    // that contain spaces and newlines (only NUL is disallowed).
    #[tokio::test]
    async fn argv_round_trips_through_the_wire() {
        let argv = vec![
            "sh".to_string(),
            "-c".to_string(),
            "echo hi there\nsecond line".to_string(),
        ];
        let mut buf = Vec::new();
        write_client_argv(&mut buf, &argv).await.unwrap();
        let decoded = read_argv(&mut buf.as_slice()).await.unwrap().unwrap();
        assert_eq!(decoded, argv);
    }

    #[tokio::test]
    async fn empty_argv_round_trips_as_empty() {
        let mut buf = Vec::new();
        write_client_argv(&mut buf, &[]).await.unwrap();
        let decoded = read_argv(&mut buf.as_slice()).await.unwrap().unwrap();
        assert!(decoded.is_empty());
    }

    // A real command streams stdout + stderr on their own frames and reports its
    // exit code — the core exec contract.
    #[tokio::test]
    async fn serve_exec_session_streams_streams_and_exit_code() {
        let argv = vec![
            "sh".to_string(),
            "-c".to_string(),
            "printf out; printf err 1>&2; exit 7".to_string(),
        ];
        let mut client_to_server = Vec::new();
        write_client_argv(&mut client_to_server, &argv).await.unwrap();

        let mut server_to_client = Vec::new();
        serve_exec_session(&mut client_to_server.as_slice(), &mut server_to_client, "test-peer")
            .await
            .unwrap();

        let mut reader = server_to_client.as_slice();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit = None;
        while let Some(frame) = read_server_frame(&mut reader).await.unwrap() {
            match frame {
                ServerFrame::Stdout(b) => stdout.extend_from_slice(&b),
                ServerFrame::Stderr(b) => stderr.extend_from_slice(&b),
                ServerFrame::Exit(code) => {
                    exit = Some(code);
                    break;
                }
                ServerFrame::Error(msg) => panic!("unexpected error frame: {msg}"),
            }
        }
        assert_eq!(stdout, b"out");
        assert_eq!(stderr, b"err");
        assert_eq!(exit, Some(7));
    }

    // The spawned command sees FABRIC_EXEC=1 and FABRIC_PEER=<connecting peer>, so
    // a script/rc can detect it is running under fabric exec and who invoked it.
    #[tokio::test]
    async fn serve_exec_session_sets_marker_env() {
        let argv = vec![
            "sh".to_string(),
            "-c".to_string(),
            "printf '%s:%s' \"$FABRIC_EXEC\" \"$FABRIC_PEER\"".to_string(),
        ];
        let mut client_to_server = Vec::new();
        write_client_argv(&mut client_to_server, &argv).await.unwrap();

        let mut server_to_client = Vec::new();
        serve_exec_session(&mut client_to_server.as_slice(), &mut server_to_client, "peer-abc123")
            .await
            .unwrap();

        let mut reader = server_to_client.as_slice();
        let mut stdout = Vec::new();
        while let Some(frame) = read_server_frame(&mut reader).await.unwrap() {
            match frame {
                ServerFrame::Stdout(b) => stdout.extend_from_slice(&b),
                ServerFrame::Exit(_) => break,
                ServerFrame::Error(msg) => panic!("unexpected error frame: {msg}"),
                ServerFrame::Stderr(_) => {}
            }
        }
        assert_eq!(stdout, b"1:peer-abc123");
    }

    // A missing binary is reported as an error frame + non-zero exit, not a hang.
    #[tokio::test]
    async fn serve_exec_session_reports_spawn_failure() {
        let argv = vec!["this-binary-does-not-exist-xyz".to_string()];
        let mut client_to_server = Vec::new();
        write_client_argv(&mut client_to_server, &argv).await.unwrap();

        let mut server_to_client = Vec::new();
        serve_exec_session(&mut client_to_server.as_slice(), &mut server_to_client, "test-peer")
            .await
            .unwrap();

        let mut reader = server_to_client.as_slice();
        let mut saw_error = false;
        let mut exit = None;
        while let Some(frame) = read_server_frame(&mut reader).await.unwrap() {
            match frame {
                ServerFrame::Error(_) => saw_error = true,
                ServerFrame::Exit(code) => {
                    exit = Some(code);
                    break;
                }
                _ => {}
            }
        }
        assert!(saw_error, "expected an error frame for a missing binary");
        assert_eq!(exit, Some(EXIT_SPAWN_FAILED));
    }

    #[tokio::test]
    async fn serve_exec_disabled_sends_error_then_126() {
        let mut buf = Vec::new();
        serve_exec_disabled(&mut buf).await.unwrap();
        let mut reader = buf.as_slice();
        assert!(matches!(
            read_server_frame(&mut reader).await.unwrap(),
            Some(ServerFrame::Error(_))
        ));
        assert!(matches!(
            read_server_frame(&mut reader).await.unwrap(),
            Some(ServerFrame::Exit(EXIT_EXEC_DISABLED))
        ));
    }
}
