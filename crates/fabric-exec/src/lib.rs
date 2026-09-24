//! `fabric exec` — the scriptable, non-interactive counterpart to `shell`.
//!
//! Where `shell` runs a remote process on a PTY and pipes it interactively,
//! `exec` runs a command with no tty, captures its stdout and stderr as separate
//! streams, and propagates the remote process's exit code back as the local exit
//! code. That makes `fabric exec <peer> -- <cmd...>` safe to script over
//! (`out=$(fabric exec hetz -- cat /etc/hostname)`), with none of the
//! pipe-into-an-interactive-shell gymnastics.
//!
//! Security mirrors `shell`: this is arbitrary remote command execution. A
//! daemon only runs an incoming exec when that peer's `allow` array contains
//! `exec`. An omitted grant denies the service.

use anyhow::{Context, Result, bail};
use fabric_service_api::{BoxFuture, Bridge, Notice, PeerStream, Protocol, Service};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    process::Command,
};

pub const EXEC_ALPN: &[u8] = b"fabric/exec/0";
pub const EXEC_PROTOCOL: &str = "fabric/exec/0";
/// The word a peer's allow array uses for this service.
pub const SERVICE: &str = "exec";

const MAX_FRAME_LEN: usize = 1024 * 1024;
const CLIENT_ARGV: u8 = 1;
const SERVER_STDOUT: u8 = 17;
const SERVER_STDERR: u8 = 18;
const SERVER_EXIT: u8 = 19;
const SERVER_ERROR: u8 = 20;

/// Exit code sent when policy refuses exec (mirrors `shell`'s 126).
pub const EXIT_EXEC_DISABLED: i32 = fabric_service_api::REFUSED_EXIT_CODE;
/// Exit code sent when the requested command could not be spawned (mirrors sh 127).
const EXIT_SPAWN_FAILED: i32 = 127;
/// How long to keep forwarding pipe output after the command has exited. The
/// command's own output is already in the pipe buffers by then, so this only
/// bounds how long a descendant holding the pipes can delay the exit frame.
const EXIT_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_millis(200);

#[derive(Debug)]
pub enum ServerFrame {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(i32),
    Error(String),
}

/// Reply to an exec request when policy does not permit remote exec.
pub async fn serve_exec_disabled<W>(send: &mut W) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    serve_exec_failure(
        send,
        "refused service \"exec\": add exec to this peer's allow array in peers.toml",
        EXIT_EXEC_DISABLED,
    )
    .await
}

/// Send a complete failure response when exec cannot start a remote command.
pub async fn serve_exec_failure<W>(send: &mut W, message: &str, exit_code: i32) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_server_frame(send, ServerFrame::Error(message.to_string())).await?;
    write_server_frame(send, ServerFrame::Exit(exit_code)).await
}

const PROTOCOLS: &[Protocol] = &[Protocol {
    alpn: EXEC_ALPN,
    resumable: false,
    accept_event: "builtin_exec_accept",
}];

/// The exec service, as the base network sees it.
#[derive(Debug, Default)]
pub struct Exec;

impl Service for Exec {
    fn name(&self) -> &'static str {
        SERVICE
    }

    fn protocols(&self) -> &'static [Protocol] {
        PROTOCOLS
    }

    fn serve(&self, stream: PeerStream) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let PeerStream {
                peer,
                mut read,
                mut write,
                ..
            } = stream;
            serve_exec_session(&mut read, &mut write, &peer).await?;
            write.shutdown().await?;
            Ok(())
        })
    }

    /// The local `fabric exec` otherwise sees only the stream end, and could
    /// not tell a refusal (126) from a command that failed to start (1).
    fn notice(&self, notice: &Notice<'_>) -> Option<Vec<u8>> {
        let (message, exit_code) = match notice {
            Notice::Refused { error } => (
                format!("refused service \"exec\": {error}"),
                EXIT_EXEC_DISABLED,
            ),
            Notice::Unavailable { error } => {
                (format!("failed to start service \"exec\": {error}"), 1)
            }
            _ => return None,
        };
        let mut bytes = encode_frame(SERVER_ERROR, message.as_bytes())?;
        bytes.extend(encode_frame(SERVER_EXIT, &exit_code.to_be_bytes())?);
        Some(bytes)
    }

    fn bridge(&self) -> Bridge {
        Bridge::WholeReply
    }
}

/// Drive the client side of a `fabric exec` session over the daemon-provided
/// socket: send the argv, forward the remote stdout/stderr to the local
/// stdout/stderr on their own streams, and return the remote command's exit code.
pub async fn run_client<S>(stream: S, peer: &str, cmd: &[String]) -> Result<i32>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut read, mut write) = tokio::io::split(stream);
    write_client_argv(&mut write, cmd).await?;

    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    let mut exit_code = None;

    while let Some(frame) = read_server_frame(&mut read).await? {
        match frame {
            ServerFrame::Stdout(bytes) => {
                stdout.write_all(&bytes).await?;
                stdout.flush().await?;
            }
            ServerFrame::Stderr(bytes) => {
                stderr.write_all(&bytes).await?;
                stderr.flush().await?;
            }
            ServerFrame::Error(message) => {
                stderr
                    .write_all(format!("fabric: peer {peer:?} ").as_bytes())
                    .await?;
                stderr.write_all(message.as_bytes()).await?;
                stderr.write_all(b"\n").await?;
                stderr.flush().await?;
            }
            ServerFrame::Exit(code) => {
                exit_code = Some(code.clamp(0, 255));
                break;
            }
        }
    }

    if exit_code.is_none() {
        stderr
            .write_all(
                format!(
                    "fabric: peer {peer:?} closed service \"exec\" before it returned an exit status\n"
                )
                .as_bytes(),
            )
            .await?;
    }

    stdout.flush().await?;
    stderr.flush().await?;
    Ok(exit_code.unwrap_or(1))
}

/// Server side of an exec session: read the argv, spawn the command with no tty
/// and a null stdin, stream its stdout and stderr back as separate frames, then
/// send the process's exit code.
/// `PATH` for a spawned command: fabric's own directory, then whatever the
/// daemon inherited.
///
/// Prepended rather than appended, so the fabric that answers is the one
/// actually running this daemon rather than an older copy earlier in the path.
/// Absent or unresolvable, the inherited value is returned unchanged: a spawned
/// command with a slightly short PATH is a much smaller problem than one with no
/// PATH at all.
pub fn exec_path_env() -> String {
    let inherited = std::env::var("PATH").unwrap_or_default();
    let Some(dir) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(std::path::Path::to_path_buf))
    else {
        return inherited;
    };
    let dir = dir.display().to_string();
    if inherited.is_empty() {
        return dir;
    }
    if inherited.split(':').any(|entry| entry == dir) {
        return inherited;
    }
    format!("{dir}:{inherited}")
}

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
        // Put fabric's own directory on PATH for the spawned command.
        //
        // The daemon inherits a minimal environment, so `fabric exec <peer> --
        // fabric ...` failed with "No such file or directory" even though fabric
        // was plainly installed. A login shell found it, because a profile adds
        // the directory; a bare exec did not.
        //
        // That is not a cosmetic gap. `fabric update` is deliberately local-only
        // BECAUSE a fleet sweep composes as `fabric exec <peer> -- fabric
        // update`. If that composition does not resolve, the argument for
        // leaving out a sweep subcommand is not true.
        //
        // The running binary's own directory, not a login shell: it is
        // deterministic, it runs no profile code we do not control, and it makes
        // `fabric exec <peer> -- fabric <anything>` work by construction.
        .env("PATH", exec_path_env())
        // Markers so the spawned command (and any shell it sources) can tell it is
        // running under fabric exec — in the daemon's session, not the caller's.
        // FABRIC_PEER is the connecting peer's NodeID — who ran this command.
        .env("FABRIC_EXEC", "1")
        .env("FABRIC_PEER", peer)
        .kill_on_drop(true)
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
    let mut client_buf = [0u8; 1];
    let mut out_done = false;
    let mut err_done = false;
    let mut status = None;

    // Drain both pipes concurrently so a chatty stderr can't deadlock stdout,
    // and watch the child at the same time. Keep reading the client side after
    // argv so a quiet child cannot hide a disconnected caller.
    //
    // The child's exit, not the pipes' end of file, is the end of the command.
    // A command that starts something in the background and exits leaves that
    // background process holding the inherited pipes, and waiting for them to
    // close made the caller live exactly as long as the stranger did: 5.08 s
    // for a `sleep 5 &` against 0.06 s for the same command without it,
    // measured live. What the command wrote before exiting is already in the
    // pipe buffers, so it is drained below; what a descendant writes later is
    // not the command's output.
    while !(out_done && err_done) && status.is_none() {
        tokio::select! {
            result = recv.read(&mut client_buf) => match result? {
                0 => return Ok(()),
                _ => bail!("unexpected exec client data after argv"),
            },
            result = stdout.read(&mut out_buf), if !out_done => match result? {
                0 => out_done = true,
                n => write_server_frame(send, ServerFrame::Stdout(out_buf[..n].to_vec())).await?,
            },
            result = stderr.read(&mut err_buf), if !err_done => match result? {
                0 => err_done = true,
                n => write_server_frame(send, ServerFrame::Stderr(err_buf[..n].to_vec())).await?,
            },
            result = child.wait() => status = Some(result.context("exec wait failed")?),
        }
    }

    let status = match status {
        Some(status) => status,
        None => tokio::select! {
            result = child.wait() => result.context("exec wait failed")?,
            result = recv.read(&mut client_buf) => match result? {
                0 => return Ok(()),
                _ => bail!("unexpected exec client data after argv"),
            },
        },
    };

    // The command has exited. Forward what it left in the pipes, then stop at
    // end of file or after a short silence, whichever comes first: a pipe that
    // stays open now belongs to a descendant that outlived the command.
    let drain_until = tokio::time::Instant::now() + EXIT_DRAIN_GRACE;
    while !(out_done && err_done) {
        tokio::select! {
            result = stdout.read(&mut out_buf), if !out_done => match result? {
                0 => out_done = true,
                n => write_server_frame(send, ServerFrame::Stdout(out_buf[..n].to_vec())).await?,
            },
            result = stderr.read(&mut err_buf), if !err_done => match result? {
                0 => err_done = true,
                n => write_server_frame(send, ServerFrame::Stderr(err_buf[..n].to_vec())).await?,
            },
            _ = tokio::time::sleep_until(drain_until) => break,
        }
    }

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

fn encode_frame(kind: u8, payload: &[u8]) -> Option<Vec<u8>> {
    if payload.len() > MAX_FRAME_LEN {
        return None;
    }
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(kind);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    Some(frame)
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
    use std::{fs, time::Duration};
    use tempfile::TempDir;

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
        let (mut client_to_server, mut server_recv) = tokio::io::duplex(4096);
        write_client_argv(&mut client_to_server, &argv)
            .await
            .unwrap();

        let mut server_to_client = Vec::new();
        serve_exec_session(&mut server_recv, &mut server_to_client, "test-peer")
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
        let (mut client_to_server, mut server_recv) = tokio::io::duplex(4096);
        write_client_argv(&mut client_to_server, &argv)
            .await
            .unwrap();

        let mut server_to_client = Vec::new();
        serve_exec_session(&mut server_recv, &mut server_to_client, "peer-abc123")
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
        write_client_argv(&mut client_to_server, &argv)
            .await
            .unwrap();

        let mut server_to_client = Vec::new();
        serve_exec_session(
            &mut client_to_server.as_slice(),
            &mut server_to_client,
            "test-peer",
        )
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
    async fn serve_exec_session_reaps_a_quiet_child_after_client_disconnect() {
        let dir = TempDir::new().unwrap();
        let pid_file = dir.path().join("child.pid");
        let argv = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "printf '%s' \"$$\" > \"$1\"; exec /bin/sleep 60".to_string(),
            "fabric-test".to_string(),
            pid_file.display().to_string(),
        ];
        let (mut client_send, mut server_recv) = tokio::io::duplex(4096);
        let (mut server_send, _client_recv) = tokio::io::duplex(4096);
        let mut server = tokio::spawn(async move {
            serve_exec_session(&mut server_recv, &mut server_send, "test-peer").await
        });

        write_client_argv(&mut client_send, &argv).await.unwrap();
        let pid = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(contents) = fs::read_to_string(&pid_file) {
                    break contents.parse::<i32>().unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the quiet exec child did not write its pid");

        drop(client_send);
        let server_result = tokio::time::timeout(Duration::from_secs(1), &mut server).await;
        let server_stopped = server_result.is_ok();
        let child_stopped = tokio::time::timeout(Duration::from_secs(1), async {
            while unsafe { libc::kill(pid, 0) == 0 } {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();

        if !child_stopped {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        if !server_stopped {
            server.abort();
        }
        assert!(server_stopped, "the exec session ignored client EOF");
        assert!(child_stopped, "the disconnected exec child stayed alive");
        server_result
            .unwrap()
            .expect("the exec server task panicked")
            .expect("the exec server rejected client EOF");
    }

    /// A command that starts a background child and exits is over when it
    /// exits. The child inherited the pipes and keeps them open; the exit frame
    /// must not wait for it.
    #[tokio::test]
    async fn serve_exec_session_reports_exit_while_a_background_child_holds_the_pipes() {
        let dir = TempDir::new().unwrap();
        let pid_file = dir.path().join("background.pid");
        let argv = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "printf before; /bin/sleep 30 & printf '%s' \"$!\" > \"$1\"; exit 5".to_string(),
            "fabric-test".to_string(),
            pid_file.display().to_string(),
        ];
        // A duplex, not a slice: a slice reads as end of file once the argv is
        // consumed, which the server rightly takes for a disconnected caller.
        let (mut client_send, mut server_recv) = tokio::io::duplex(4096);
        write_client_argv(&mut client_send, &argv).await.unwrap();

        let started = std::time::Instant::now();
        let mut server_to_client = Vec::new();
        let served = tokio::time::timeout(
            Duration::from_secs(5),
            serve_exec_session(&mut server_recv, &mut server_to_client, "test-peer"),
        )
        .await;
        drop(client_send);
        let elapsed = started.elapsed();
        // Reap the orphaned background child whatever the outcome.
        if let Ok(pid) = fs::read_to_string(&pid_file).map(|s| s.trim().parse::<i32>().unwrap_or(0))
            && pid > 0
        {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        served
            .expect("the exec session waited for a background child instead of the command")
            .expect("the exec session failed");

        let mut reader = server_to_client.as_slice();
        let mut stdout = Vec::new();
        let mut exit = None;
        while let Some(frame) = read_server_frame(&mut reader).await.unwrap() {
            match frame {
                ServerFrame::Stdout(b) => stdout.extend_from_slice(&b),
                ServerFrame::Exit(code) => {
                    exit = Some(code);
                    break;
                }
                ServerFrame::Error(msg) => panic!("unexpected error frame: {msg}"),
                ServerFrame::Stderr(_) => {}
            }
        }
        assert_eq!(stdout, b"before", "output written before the exit was lost");
        assert_eq!(exit, Some(5));
        assert!(
            elapsed < Duration::from_secs(2),
            "the exit frame took {elapsed:?}; it waited for the background child"
        );
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

#[cfg(test)]
mod notice_tests {
    use super::*;

    async fn frames(bytes: Vec<u8>) -> Vec<ServerFrame> {
        let mut read = bytes.as_slice();
        let mut frames = Vec::new();
        while let Some(frame) = read_server_frame(&mut read).await.unwrap() {
            frames.push(frame);
        }
        frames
    }

    #[tokio::test]
    async fn a_refusal_reaches_the_local_command_as_error_then_126() {
        let bytes = Exec
            .notice(&Notice::Refused {
                error: "not permitted",
            })
            .unwrap();
        assert!(matches!(
            frames(bytes).await.as_slice(),
            [ServerFrame::Error(message), ServerFrame::Exit(126)]
                if message == "refused service \"exec\": not permitted"
        ));
    }

    #[tokio::test]
    async fn an_unopened_stream_reaches_the_local_command_as_error_then_1() {
        let bytes = Exec
            .notice(&Notice::Unavailable { error: "timed out" })
            .unwrap();
        assert!(matches!(
            frames(bytes).await.as_slice(),
            [ServerFrame::Error(message), ServerFrame::Exit(1)]
                if message == "failed to start service \"exec\": timed out"
        ));
    }

    #[test]
    fn exec_has_nothing_to_say_about_a_resumable_session() {
        assert!(Exec.notice(&Notice::Resumed).is_none());
        assert!(Exec.notice(&Notice::FallingBack).is_none());
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    /// `fabric exec <peer> -- fabric ...` must resolve, because that composition
    /// is the reason `fabric update` has no fleet-sweep subcommand. If it does
    /// not resolve, the argument for the smaller scope is not true.
    #[test]
    fn a_spawned_command_can_find_the_fabric_that_spawned_it() {
        let path = exec_path_env();
        let dir = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .display()
            .to_string();
        assert!(
            path.split(':').any(|entry| entry == dir),
            "the running fabric's directory is not on the spawned PATH:\n{path}"
        );
    }

    /// Prepended, so the fabric that answers is the one running this daemon and
    /// not an older copy sitting earlier in the inherited path.
    #[test]
    fn fabric_comes_before_whatever_was_inherited() {
        let path = exec_path_env();
        let dir = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .display()
            .to_string();
        assert_eq!(
            path.split(':').next(),
            Some(dir.as_str()),
            "fabric's directory is not first:\n{path}"
        );
    }

    /// Whatever the daemon inherited has to survive. A command that gains fabric
    /// and loses `sh` is worse off than before.
    #[test]
    fn the_inherited_path_is_kept() {
        let inherited = std::env::var("PATH").unwrap_or_default();
        let path = exec_path_env();
        for entry in inherited.split(':').filter(|entry| !entry.is_empty()) {
            assert!(
                path.split(':').any(|kept| kept == entry),
                "the inherited entry {entry} was dropped:\n{path}"
            );
        }
    }
}
