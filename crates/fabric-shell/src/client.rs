//! The interactive side of `fabric shell`: the local terminal, the stdin pump,
//! and the loop that replaces a session that ended without an exit status.
//!
//! It talks to the remote shell through a local socket the daemon bridges to
//! the peer, and asks for a new socket through `request_socket` when a session
//! has to be replaced. It knows nothing else about the daemon.

use std::{
    future::Future,
    io::IsTerminal,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    ServerFrame,
    terminal::{TerminalModeGuard, TerminalState},
};

/// How long `fabric shell` keeps trying to start a replacement shell after its
/// session ended without an exit status. The daemon's own pre-attach probing
/// has no bound, so this is the bound: a peer that stays away this long gets a
/// person's attention instead of a terminal pinned forever.
const SHELL_REPLACEMENT_DEADLINE: Duration = Duration::from_secs(5 * 60);
/// How often to ask the local daemon again while it is itself unavailable.
const SHELL_REPLACEMENT_LOCAL_RETRY: Duration = Duration::from_secs(1);

/// The stdin pump's view of the session it feeds.
///
/// A shell session that ends without an exit status is replaced by a new one
/// in the same terminal, so the pump has to be able to change where its bytes
/// go and to stop forwarding them while nothing is connected.
struct ShellInput {
    /// The current session's local socket, `None` between sessions.
    write: tokio::sync::Mutex<Option<tokio::net::unix::OwnedWriteHalf>>,
    /// False from the loss of a session until its replacement first answers.
    /// Bytes read while false are discarded, never replayed: the replacement is
    /// a fresh shell with a different working directory and history from the
    /// one the person was typing into, and a command meant for the old shell
    /// must not run in the new one.
    forwarding: AtomicBool,
    /// Bytes discarded while not forwarding, reported once forwarding resumes.
    discarded: AtomicUsize,
    /// Stdin reached end of file, so nobody is there to type into a replacement.
    eof: AtomicBool,
    /// Ctrl-C typed while not forwarding: the person wants out of the wait.
    interrupted: AtomicBool,
    interrupt: tokio::sync::Notify,
}

impl ShellInput {
    fn new() -> Self {
        Self {
            write: tokio::sync::Mutex::new(None),
            forwarding: AtomicBool::new(true),
            discarded: AtomicUsize::new(0),
            eof: AtomicBool::new(false),
            interrupted: AtomicBool::new(false),
            interrupt: tokio::sync::Notify::new(),
        }
    }

    /// Connect to a session socket, tell the remote PTY the window size, and
    /// make it the pump's destination.
    async fn attach(&self, socket: &Path) -> Result<tokio::net::unix::OwnedReadHalf> {
        let stream = tokio::net::UnixStream::connect(socket).await?;
        let (read, mut write) = stream.into_split();
        let (cols, rows) = terminal_size();
        crate::write_client_resize(&mut write, rows, cols).await?;
        *self.write.lock().await = Some(write);
        Ok(read)
    }

    /// The session is gone: stop forwarding until a replacement answers.
    async fn detach(&self) {
        *self.write.lock().await = None;
        self.forwarding.store(false, Ordering::SeqCst);
        self.discarded.store(0, Ordering::SeqCst);
        self.interrupted.store(false, Ordering::SeqCst);
    }

    /// The replacement answered. Returns how many bytes were discarded meanwhile.
    fn resume_forwarding(&self) -> usize {
        self.forwarding.store(true, Ordering::SeqCst);
        self.interrupted.store(false, Ordering::SeqCst);
        self.discarded.swap(0, Ordering::SeqCst)
    }

    fn is_forwarding(&self) -> bool {
        self.forwarding.load(Ordering::SeqCst)
    }

    fn stdin_closed(&self) -> bool {
        self.eof.load(Ordering::SeqCst)
    }

    fn take_interrupt(&self) -> bool {
        self.interrupted.swap(false, Ordering::SeqCst)
    }

    async fn resize(&self) {
        let (cols, rows) = terminal_size();
        if let Some(write) = self.write.lock().await.as_mut() {
            // A failed write is the session going away; the reader reports it.
            let _ = crate::write_client_resize(write, rows, cols).await;
        }
    }

    async fn pump_stdin(self: Arc<Self>) -> Result<()> {
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 8192];
        loop {
            let read = stdin.read(&mut buf).await?;
            if read == 0 {
                self.eof.store(true, Ordering::SeqCst);
                if let Some(write) = self.write.lock().await.as_mut() {
                    let _ = crate::write_client_eof(write).await;
                }
                return Ok(());
            }
            if self.is_forwarding() {
                if let Some(write) = self.write.lock().await.as_mut() {
                    // A failed write is the session going away; the reader
                    // reports the loss, and these bytes are part of it.
                    let _ = crate::write_client_stdin(write, &buf[..read]).await;
                }
                continue;
            }
            self.discarded.fetch_add(read, Ordering::SeqCst);
            if buf[..read].contains(&0x03) {
                self.interrupted.store(true, Ordering::SeqCst);
                self.interrupt.notify_one();
            }
        }
    }
}

/// Renders daemon status and client notices on stderr.
///
/// Status frames describe a wait in progress ("reconnecting attempt 3 in
/// 2.0s", "probing remote shell protocol again in 5.0s") and arrive once per
/// attempt. On a terminal they overwrite one line with a carriage return instead
/// of scrolling the session away; everything else ends that line first, so
/// shell output and notices start on a fresh one.
struct ShellNotices {
    stderr: tokio::io::Stderr,
    /// Raw mode leaves output post-processing off, so a newline alone would not
    /// return the carriage.
    line_end: &'static [u8],
    overwrite_status: bool,
    status_open: bool,
}

impl ShellNotices {
    fn new(raw: bool) -> Self {
        Self {
            stderr: tokio::io::stderr(),
            line_end: if raw { b"\r\n" } else { b"\n" },
            overwrite_status: raw && std::io::stderr().is_terminal(),
            status_open: false,
        }
    }

    async fn status(&mut self, message: &str) -> Result<()> {
        if self.overwrite_status {
            self.stderr.write_all(b"\r\x1b[K").await?;
            self.stderr.write_all(message.as_bytes()).await?;
            self.status_open = true;
        } else {
            self.stderr.write_all(message.as_bytes()).await?;
            self.stderr.write_all(self.line_end).await?;
        }
        self.stderr.flush().await?;
        Ok(())
    }

    async fn line(&mut self, message: &str) -> Result<()> {
        self.end_status().await?;
        self.stderr.write_all(message.as_bytes()).await?;
        self.stderr.write_all(self.line_end).await?;
        self.stderr.flush().await?;
        Ok(())
    }

    /// Terminate an overwriting status line so what follows starts fresh.
    async fn end_status(&mut self) -> Result<()> {
        if self.status_open {
            self.status_open = false;
            self.stderr.write_all(self.line_end).await?;
            self.stderr.flush().await?;
        }
        Ok(())
    }
}

/// The emulator state this session changes, tracked from the bytes it writes,
/// when stdout is a terminal. `None` when it is not: a pipe gets the session's
/// bytes and nothing of ours.
fn track_terminal_state() -> Option<TerminalState> {
    std::io::stdout().is_terminal().then(TerminalState::new)
}

/// Put the terminal back after a session: undo what it set and forget it. A
/// program inside the remote shell that died without cleaning up, or a session
/// that ended mid-screen, left these behind; the person is about to get their
/// terminal back, or a new shell is about to start on it.
async fn put_terminal_back(
    stdout: &mut tokio::io::Stdout,
    state: &mut Option<TerminalState>,
) -> Result<()> {
    if let Some(state) = state {
        let bytes = state.cleanup();
        state.clear();
        if !bytes.is_empty() {
            stdout.write_all(&bytes).await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}

/// The same, synchronously, for the path that re-raises a signal and never
/// returns to the runtime.
fn put_terminal_back_blocking(state: &mut Option<TerminalState>) {
    if let Some(state) = state {
        let bytes = state.cleanup();
        state.clear();
        if !bytes.is_empty() {
            let mut stdout = std::io::stdout().lock();
            let _ = std::io::Write::write_all(&mut stdout, &bytes);
            let _ = std::io::Write::flush(&mut stdout);
        }
    }
}

enum ShellSessionEnd {
    Exited(i32),
    /// The local socket closed without an exit status. `answered` says whether
    /// the remote shell ever produced output on this session.
    Lost {
        answered: bool,
    },
    /// Ctrl-C typed while waiting for a replacement shell to answer.
    Interrupted,
    /// The replacement shell did not answer within the deadline.
    DeadlinePassed,
}

enum ReplacementOutcome {
    Connected(tokio::net::unix::OwnedReadHalf),
    Interrupted,
    DeadlinePassed,
}

/// Drive one `fabric shell` command: a resumable session, and when that session
/// ends without an exit status, a replacement shell in the same terminal.
///
/// Same PTY when possible: a transport loss shorter than the server's detached
/// TTL resumes in place inside the daemon and never reaches this loop. What
/// reaches it is a session the server refused to resume (a remote daemon
/// restart, a sleep past the TTL) or a socket that simply closed. A session that
/// had answered is replaced, visibly; a session that never answered ended in a
/// refusal the daemon has already explained, and the command ends as before.
pub async fn run_client<R, F>(peer: &str, socket: PathBuf, mut request_socket: R) -> Result<i32>
where
    R: FnMut() -> F,
    F: Future<Output = Result<PathBuf>>,
{
    let mut signals = ShellSignals::new()?;
    let terminal = TerminalModeGuard::enable_if_terminal()?;
    let mut terminal_state = track_terminal_state();
    let input = Arc::new(ShellInput::new());
    let mut read = input.attach(&socket).await?;
    let stdin_task = tokio::spawn(input.clone().pump_stdin());

    let mut stdout = tokio::io::stdout();
    let mut notices = ShellNotices::new(terminal.is_enabled());
    let mut answer_deadline = None;

    let outcome: Result<i32> = async {
        loop {
            let end = run_shell_session(
                &mut read,
                &input,
                &mut signals,
                &terminal,
                &mut terminal_state,
                &mut notices,
                &mut stdout,
                peer,
                answer_deadline,
            )
            .await?;
            match end {
                ShellSessionEnd::Exited(code) => break Ok(code),
                ShellSessionEnd::Interrupted => {
                    notices
                        .line(&format!(
                            "fabric: interrupted while waiting for a new remote shell to {peer:?}"
                        ))
                        .await?;
                    break Ok(130);
                }
                ShellSessionEnd::DeadlinePassed => {
                    notices
                        .line(&format!(
                            "fabric: no new remote shell to {peer:?} answered within {}m; giving up",
                            SHELL_REPLACEMENT_DEADLINE.as_secs() / 60
                        ))
                        .await?;
                    break Ok(1);
                }
                ShellSessionEnd::Lost { answered } => {
                    if !answered || input.stdin_closed() {
                        notices
                            .line(&format!(
                                "fabric: peer {peer:?} closed service \"shell\" before it returned an exit status"
                            ))
                            .await?;
                        break Ok(1);
                    }
                    // The lost session's programs are gone with it; the new
                    // shell starts on a terminal put back first, and the
                    // loss is announced on that clean terminal.
                    put_terminal_back(&mut stdout, &mut terminal_state).await?;
                    notices
                        .line(&format!(
                            "fabric: remote shell to {peer:?} ended without an exit status; starting a new shell"
                        ))
                        .await?;
                    input.detach().await;
                    let deadline = tokio::time::Instant::now() + SHELL_REPLACEMENT_DEADLINE;
                    match wait_for_replacement_shell(&mut request_socket, &input, &mut notices, deadline)
                        .await?
                    {
                        ReplacementOutcome::Connected(replacement) => {
                            read = replacement;
                            answer_deadline = Some(deadline);
                        }
                        ReplacementOutcome::Interrupted => {
                            notices
                                .line(&format!(
                                    "fabric: interrupted while waiting for a new remote shell to {peer:?}"
                                ))
                                .await?;
                            break Ok(130);
                        }
                        ReplacementOutcome::DeadlinePassed => {
                            notices
                                .line(&format!(
                                    "fabric: no new remote shell to {peer:?} answered within {}m; giving up",
                                    SHELL_REPLACEMENT_DEADLINE.as_secs() / 60
                                ))
                                .await?;
                            break Ok(1);
                        }
                    }
                }
            }
        }
    }
    .await;

    stdin_task.abort();
    let _ = stdin_task.await;
    // Whatever ended the command, an exit, an error or a refusal, the session
    // may have left the terminal mid-screen. Put it back before termios.
    put_terminal_back(&mut stdout, &mut terminal_state).await?;
    terminal.restore()?;
    stdout.flush().await?;
    outcome
}

/// Pump one session until it exits, is lost, or the person gives up on it.
/// `answer_deadline` is set for a replacement session until it first answers.
#[allow(clippy::too_many_arguments)]
async fn run_shell_session(
    read: &mut tokio::net::unix::OwnedReadHalf,
    input: &ShellInput,
    signals: &mut ShellSignals,
    terminal: &TerminalModeGuard,
    terminal_state: &mut Option<TerminalState>,
    notices: &mut ShellNotices,
    stdout: &mut tokio::io::Stdout,
    peer: &str,
    mut answer_deadline: Option<tokio::time::Instant>,
) -> Result<ShellSessionEnd> {
    let mut answered = false;
    loop {
        tokio::select! {
            frame = crate::read_server_frame(read) => {
                let Some(frame) = frame? else {
                    return Ok(ShellSessionEnd::Lost { answered });
                };
                match frame {
                    ServerFrame::Output(bytes) => {
                        if !answered {
                            answered = true;
                            if answer_deadline.take().is_some() {
                                let discarded = input.resume_forwarding();
                                let mut ready =
                                    format!("fabric: new remote shell to {peer:?} is ready");
                                if discarded > 0 {
                                    ready.push_str(&format!(
                                        " ({discarded} bytes typed while disconnected were discarded)"
                                    ));
                                }
                                notices.line(&ready).await?;
                            }
                        }
                        if let Some(state) = terminal_state.as_mut() {
                            state.observe(&bytes);
                        }
                        notices.end_status().await?;
                        stdout.write_all(&bytes).await?;
                        stdout.flush().await?;
                    }
                    ServerFrame::Error(message) => {
                        notices.line(&format!("fabric: peer {peer:?} {message}")).await?;
                    }
                    ServerFrame::Status(message) => {
                        notices.status(&message).await?;
                    }
                    ServerFrame::Exit(code) => {
                        notices.end_status().await?;
                        return Ok(ShellSessionEnd::Exited(normalize_exit_code(code)));
                    }
                }
            }
            signal = signals.recv() => {
                match signal {
                    ShellSignal::Resize => input.resize().await,
                    ShellSignal::Suspend => {
                        terminal.restore()?;
                        suspend_current_process();
                        terminal.reenter_raw()?;
                        input.resize().await;
                    }
                    ShellSignal::Terminate(signal) => {
                        put_terminal_back_blocking(terminal_state);
                        terminal.restore()?;
                        terminate_with_signal(signal);
                    }
                }
            }
            _ = input.interrupt.notified(), if !input.is_forwarding() => {
                if input.take_interrupt() {
                    notices.end_status().await?;
                    return Ok(ShellSessionEnd::Interrupted);
                }
            }
            _ = tokio::time::sleep_until(answer_deadline.unwrap_or_else(tokio::time::Instant::now)),
                if answer_deadline.is_some() => {
                notices.end_status().await?;
                return Ok(ShellSessionEnd::DeadlinePassed);
            }
        }
    }
}

/// Obtain and connect a replacement shell socket, retrying while the local
/// daemon is itself unavailable (it may be restarting too), until `deadline`.
async fn wait_for_replacement_shell<R, F>(
    request_socket: &mut R,
    input: &ShellInput,
    notices: &mut ShellNotices,
    deadline: tokio::time::Instant,
) -> Result<ReplacementOutcome>
where
    R: FnMut() -> F,
    F: Future<Output = Result<PathBuf>>,
{
    let mut local_unavailable_reported = false;
    loop {
        match request_socket().await {
            Ok(socket) => match input.attach(&socket).await {
                Ok(read) => return Ok(ReplacementOutcome::Connected(read)),
                Err(error) => {
                    notices
                        .status(&format!(
                            "fabric: new shell socket is not ready ({error:#}); retrying"
                        ))
                        .await?;
                }
            },
            Err(error) => {
                if !local_unavailable_reported {
                    local_unavailable_reported = true;
                    notices
                        .line(&format!(
                            "fabric: local daemon unavailable ({error:#}); retrying every {}s for up to {}m",
                            SHELL_REPLACEMENT_LOCAL_RETRY.as_secs(),
                            SHELL_REPLACEMENT_DEADLINE.as_secs() / 60
                        ))
                        .await?;
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(ReplacementOutcome::DeadlinePassed);
        }
        tokio::select! {
            _ = tokio::time::sleep(SHELL_REPLACEMENT_LOCAL_RETRY) => {}
            _ = tokio::time::sleep_until(deadline) => return Ok(ReplacementOutcome::DeadlinePassed),
            _ = input.interrupt.notified() => {
                if input.take_interrupt() {
                    return Ok(ReplacementOutcome::Interrupted);
                }
            }
        }
    }
}


fn terminal_size() -> (u16, u16) {
    if std::io::stdout().is_terminal()
        && let Ok((cols, rows)) = crossterm::terminal::size()
    {
        return (cols, rows);
    }
    (80, 24)
}

fn normalize_exit_code(code: i32) -> i32 {
    code.clamp(0, 255)
}


enum ShellSignal {
    Resize,
    Suspend,
    Terminate(i32),
}

#[cfg(unix)]
struct ShellSignals {
    hangup: tokio::signal::unix::Signal,
    interrupt: tokio::signal::unix::Signal,
    quit: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    suspend: tokio::signal::unix::Signal,
    resize: tokio::signal::unix::Signal,
}

#[cfg(not(unix))]
struct ShellSignals;

#[cfg(unix)]
impl ShellSignals {
    fn new() -> Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            hangup: signal(SignalKind::hangup())?,
            interrupt: signal(SignalKind::interrupt())?,
            quit: signal(SignalKind::quit())?,
            terminate: signal(SignalKind::terminate())?,
            suspend: signal(SignalKind::from_raw(libc::SIGTSTP))?,
            resize: signal(SignalKind::window_change())?,
        })
    }

    async fn recv(&mut self) -> ShellSignal {
        tokio::select! {
            _ = self.hangup.recv() => ShellSignal::Terminate(libc::SIGHUP),
            _ = self.interrupt.recv() => ShellSignal::Terminate(libc::SIGINT),
            _ = self.quit.recv() => ShellSignal::Terminate(libc::SIGQUIT),
            _ = self.terminate.recv() => ShellSignal::Terminate(libc::SIGTERM),
            _ = self.suspend.recv() => ShellSignal::Suspend,
            _ = self.resize.recv() => ShellSignal::Resize,
        }
    }
}

#[cfg(not(unix))]
impl ShellSignals {
    fn new() -> Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) -> ShellSignal {
        std::future::pending().await
    }
}

#[cfg(unix)]
fn suspend_current_process() {
    // SIGTSTP is intercepted above so we can restore the terminal first. SIGSTOP
    // cannot be caught, which guarantees one real stop; execution resumes here
    // after the process receives SIGCONT.
    unsafe {
        libc::raise(libc::SIGSTOP);
    }
}

#[cfg(not(unix))]
fn suspend_current_process() {}

#[cfg(unix)]
fn terminate_with_signal(signal: i32) -> ! {
    // Tokio installed the process signal handler. Restore the default action
    // after restoring termios, then re-raise so parents observe a signal exit
    // instead of a fabricated numeric status.
    unsafe {
        libc::signal(signal, libc::SIG_DFL);
        libc::raise(signal);
        libc::_exit(128 + signal);
    }
}

#[cfg(not(unix))]
fn terminate_with_signal(_signal: i32) -> ! {
    std::process::exit(1)
}

