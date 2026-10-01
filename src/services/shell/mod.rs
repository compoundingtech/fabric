//! `fabric shell`: a login shell on a peer's PTY, the terminal handling on the
//! local side, and the service that serves it.

use std::{
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc,
    },
    time::Duration,
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

/// The status that ends a wait inside a live session. The client stops treating
/// Ctrl-C as "stop waiting" when it sees this, so the key reaches the remote
/// program again.
pub(crate) const RESUMED_STATUS: &str = "connection restored; remote shell session resumed";

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
            Notice::Connecting { waited, delay } => encode_server_status(&format!(
                "waiting for a connection to the peer ({}s so far); trying again in {:.1}s; \
                 Ctrl-C stops",
                waited.as_secs(),
                delay.as_secs_f32()
            )),
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
            Notice::Resumed => encode_server_status(RESUMED_STATUS),
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

/// How long a hung-up shell gets to exit on its own before it is killed.
const SHELL_HANGUP_GRACE: Duration = Duration::from_secs(2);

/// Hangs the shell up however its session ends.
///
/// The cleanup at the end of a session runs only when the session future runs
/// to completion. A daemon that shuts down drops the future instead, and a
/// shell left running keeps the session's blocking PTY reader and child wait
/// alive. A tokio runtime does not finish dropping until every blocking task
/// has returned, so the daemon could not exit. A hang-up is what a shell
/// expects when its terminal goes away: SIGHUP, then SIGKILL if it is still
/// there after a grace period.
///
/// Nothing is sent once the child has been reaped, so a reused pid is never
/// signalled.
struct HangUpOnDrop {
    pid: Option<libc::pid_t>,
    reaped: Arc<AtomicBool>,
}

impl Drop for HangUpOnDrop {
    fn drop(&mut self) {
        let Some(pid) = self.pid.take() else {
            return;
        };
        if self.reaped.load(Ordering::SeqCst) {
            return;
        }
        unsafe { libc::kill(pid, libc::SIGHUP) };
        let reaped = self.reaped.clone();
        std::thread::spawn(move || {
            std::thread::sleep(SHELL_HANGUP_GRACE);
            if !reaped.load(Ordering::SeqCst) {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        });
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
    let reaped = Arc::new(AtomicBool::new(false));
    let hang_up = HangUpOnDrop {
        pid: child.process_id().map(|pid| pid as libc::pid_t),
        reaped: reaped.clone(),
    };
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

    let mut wait_task = tokio::task::spawn_blocking(move || {
        let status = child.wait();
        reaped.store(true, Ordering::SeqCst);
        status
    });
    let mut stdin_done = false;
    let mut output_done = false;
    let mut exit_code = None;
    let mut cancelled = false;

    let result: Result<()> = async {
        // MasterPty is Send, not Sync: this loop owns it across its awaits.
        let master = master;
        let incoming = next_client_frame(recv);
        tokio::pin!(incoming);
        while !output_done || exit_code.is_none() {
            tokio::select! {
                frame = &mut incoming, if !stdin_done => {
                    let (frame, recv) = frame;
                    incoming.set(next_client_frame(recv));
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
                    cancelled = true;
                    return Ok(());
                }
            }
        }
        Ok(())
    }
    .await;

    // A framing or output error owns the same cleanup as cancellation. Leaving
    // early via `?` otherwise detaches the blocking PTY reader and child wait.
    if exit_code.is_none() {
        drop(hang_up);
        let _ = input_tx.send(None);
        let _ = wait_task.await;
    }
    drop(input_tx);
    let _ = reader_task.await;
    let _ = writer_task.await;
    result?;
    if cancelled {
        Ok(())
    } else if let Some(code) = exit_code {
        write_server_frame(send, ServerFrame::Exit(code)).await
    } else {
        Ok(())
    }
}

// A partial frame must remain alive when output wins the select. Returning
// the reader lets the completed future be replaced without aliasing its borrow.
async fn next_client_frame<R>(read: &mut R) -> (Result<Option<ClientFrame>>, &mut R)
where
    R: AsyncRead + Unpin,
{
    let frame = read_client_frame(read).await;
    (frame, read)
}

pub(super) async fn next_server_frame<R>(read: &mut R) -> (Result<Option<ServerFrame>>, &mut R)
where
    R: AsyncRead + Unpin,
{
    let frame = read_server_frame(read).await;
    (frame, read)
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

    struct KillOnDrop(libc::pid_t);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            // The old implementation deliberately fails the assertion;
            // reap its leaked child rather than hanging the test runtime.
            unsafe { libc::kill(self.0, libc::SIGKILL) };
        }
    }

    /// A session dropped before its cleanup runs, as every session is when the
    /// daemon's runtime shuts down, still hangs its shell up. A shell left
    /// running keeps the blocking PTY reader and child wait alive, and a tokio
    /// runtime does not finish dropping until those return.
    ///
    /// The shell here has become a program that reads nothing, as an editor or
    /// a build would be: the end-of-file a dropped PTY writer sends ends a
    /// shell idle at its prompt, but not this.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dropped_shell_session_hangs_up_its_pty_child() -> anyhow::Result<()> {
        let (server, client) = tokio::io::duplex(8192);
        let (mut recv, mut send) = tokio::io::split(server);
        let task =
            tokio::spawn(
                async move { super::serve_shell_session(&mut recv, &mut send, "peer").await },
            );
        let (mut read, mut write) = tokio::io::split(client);
        super::write_client_stdin(
            &mut write,
            b"printf 'SHELLPID-%s-END\\n' $$; exec sleep 60\n",
        )
        .await?;
        let pid = tokio::time::timeout(Duration::from_secs(5), async {
            let mut seen = Vec::new();
            loop {
                if let Some(ServerFrame::Output(bytes)) = read_server_frame(&mut read).await? {
                    seen.extend(bytes);
                    for candidate in String::from_utf8_lossy(&seen).split("SHELLPID-") {
                        if let Some((digits, _)) = candidate.split_once("-END")
                            && let Ok(pid) = digits.parse::<libc::pid_t>()
                        {
                            return Ok::<_, anyhow::Error>(pid);
                        }
                    }
                }
            }
        })
        .await??;
        let cleanup = KillOnDrop(pid);

        // The client end stays open, so only the drop can end the shell.
        task.abort();
        let _ = task.await;
        let started = std::time::Instant::now();
        let mut alive = true;
        while started.elapsed() < Duration::from_secs(10) {
            if unsafe { libc::kill(pid, 0) } != 0 {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if alive {
            drop(cleanup);
        } else {
            // The service reaped the PID. Never signal a reused PID.
            std::mem::forget(cleanup);
        }
        assert!(!alive, "a dropped shell session left its PTY child alive");
        drop((read, write));
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_shell_stream_error_terminates_and_reaps_its_pty_child() -> anyhow::Result<()> {
        for output_error in [false, true] {
            let (server, client) = tokio::io::duplex(8192);
            let (mut recv, mut send) = tokio::io::split(server);
            let task = tokio::spawn(async move {
                super::serve_shell_session(&mut recv, &mut send, "peer").await
            });
            let (mut read, mut write) = tokio::io::split(client);
            let program: &[u8] = if output_error {
                b"stty raw -echo; printf 'SHELLPID-%s-END\\n' $$; while :; do printf .; sleep 0.05; done\n"
            } else {
                b"stty raw -echo; printf 'SHELLPID-%s-END\\n' $$; read ignored; exec sleep 60\n"
            };
            super::write_client_stdin(&mut write, program).await?;
            let pid = tokio::time::timeout(Duration::from_secs(5), async {
                let mut seen = Vec::new();
                loop {
                    if let Some(ServerFrame::Output(bytes)) = read_server_frame(&mut read).await? {
                        seen.extend(bytes);
                        for candidate in String::from_utf8_lossy(&seen).split("SHELLPID-") {
                            if let Some((digits, _)) = candidate.split_once("-END")
                                && let Ok(pid) = digits.parse::<libc::pid_t>()
                            {
                                return Ok::<_, anyhow::Error>(pid);
                            }
                        }
                    }
                }
            })
            .await??;
            let cleanup = KillOnDrop(pid);
            if output_error {
                drop(read);
                drop(write);
            } else {
                super::write_frame(&mut write, 255, &[]).await?;
            }
            let result = tokio::time::timeout(Duration::from_secs(5), task).await??;
            assert!(result.is_err(), "the stream failure was swallowed");
            let alive = unsafe { libc::kill(pid, 0) } == 0;
            if alive {
                drop(cleanup);
            } else {
                // The service reaped the PID. Never signal a reused PID.
                std::mem::forget(cleanup);
            }
            assert!(!alive, "shell stream error left its PTY child alive");
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_partial_shell_frame_survives_interleaved_pty_output() -> anyhow::Result<()> {
        use anyhow::Context as _;
        use std::{
            pin::Pin,
            sync::Arc,
            sync::atomic::{AtomicUsize, Ordering},
            task::{Context, Poll},
        };
        use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
        use tokio::sync::Notify;

        struct CountRead<R> {
            inner: R,
            bytes: Arc<AtomicUsize>,
            changed: Arc<Notify>,
        }
        impl<R: AsyncRead + Unpin> AsyncRead for CountRead<R> {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                let before = buf.filled().len();
                let result = Pin::new(&mut self.inner).poll_read(cx, buf);
                let consumed = buf.filled().len() - before;
                if consumed != 0 {
                    self.bytes.fetch_add(consumed, Ordering::SeqCst);
                    self.changed.notify_one();
                }
                result
            }
        }
        let gate_dir = tempfile::tempdir()?;
        let gate = gate_dir.path().join("ready");
        let program = format!(
            "stty raw -echo; printf 'SHELLPID-%s-END\\n' $$; while [ ! -f '{}' ]; do sleep 0.01; done; printf 'INTERLEAVED\\n'; read ignored; printf 'FRAGMENT_REACHED\\n'; read final\n",
            gate.display()
        );
        let (server, client) = tokio::io::duplex(8192);
        let (recv, mut send) = tokio::io::split(server);
        let bytes = Arc::new(AtomicUsize::new(0));
        let changed = Arc::new(Notify::new());
        let mut recv = CountRead {
            inner: recv,
            bytes: bytes.clone(),
            changed: changed.clone(),
        };
        let cancel = tokio_util::sync::CancellationToken::new();
        let server_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            super::serve_shell_session_until(&mut recv, &mut send, "peer", server_cancel).await
        });
        let (mut read, mut write) = tokio::io::split(client);
        super::write_client_stdin(&mut write, program.as_bytes()).await?;
        let mut seen = Vec::new();
        let pid = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(ServerFrame::Output(chunk)) = read_server_frame(&mut read).await? {
                    seen.extend(chunk);
                    for candidate in String::from_utf8_lossy(&seen).split("SHELLPID-") {
                        if let Some((digits, _)) = candidate.split_once("-END")
                            && let Ok(pid) = digits.parse::<libc::pid_t>()
                        {
                            return Ok::<_, anyhow::Error>(pid);
                        }
                    }
                }
            }
        })
        .await
        .context("waiting for shell PID")??;
        let cleanup = KillOnDrop(pid);
        // Deliver only the kind byte. Wait until the service consumed it,
        // then make its other select branch win before delivering the length.
        write.write_all(&[super::CLIENT_STDIN]).await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while bytes.load(Ordering::SeqCst) < 5 + program.len() + 1 {
                changed.notified().await;
            }
        })
        .await
        .context("waiting for partial header consumption")?;
        std::fs::write(gate, [])?;
        seen.clear();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !String::from_utf8_lossy(&seen).contains("INTERLEAVED\n") {
                if let Some(ServerFrame::Output(chunk)) = read_server_frame(&mut read).await? {
                    seen.extend(chunk);
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("waiting for interleaved output")??;
        write.write_all(&[0, 0, 0, 1, b'\n']).await?;
        seen.clear();
        let delivered = tokio::time::timeout(Duration::from_secs(5), async {
            while !String::from_utf8_lossy(&seen).contains("FRAGMENT_REACHED\n") {
                match read_server_frame(&mut read).await? {
                    Some(ServerFrame::Output(chunk)) => seen.extend(chunk),
                    None => anyhow::bail!("shell ended before fragmented input arrived"),
                    _ => {}
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        cancel.cancel();
        let stopped = tokio::time::timeout(Duration::from_secs(5), task).await;
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        if alive {
            drop(cleanup);
        } else {
            std::mem::forget(cleanup);
        }
        delivered.context("waiting for fragmented input effect")??;
        stopped.context("waiting for shell cancellation")???;
        assert!(!alive, "cancelled shell left its PTY child alive");
        Ok(())
    }

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

    /// A peer that is away is waited for, not reported as an error, and the
    /// wait says how to stop it.
    #[tokio::test]
    async fn waiting_for_an_away_peer_reads_as_a_wait() {
        let message = only_status(Notice::Connecting {
            waited: Duration::from_secs(12),
            delay: Duration::from_secs(5),
        })
        .await;
        assert_eq!(
            message,
            "waiting for a connection to the peer (12s so far); trying again in 5.0s; Ctrl-C stops"
        );
        assert!(!message.contains("offline") && !message.contains("unavailable"));
    }

    /// The client recognises the end of a wait by this exact status.
    #[tokio::test]
    async fn a_resumed_session_reports_the_status_the_client_waits_for() {
        assert_eq!(only_status(Notice::Resumed).await, super::RESUMED_STATUS);
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
