//! Read and write a child process's pipes on threads of their own, never on a
//! runtime worker.
//!
//! A pipe operation can block in the kernel even on a descriptor set to
//! non-blocking. `read` and `write` on a pipe take the pipe's lock first, and
//! `O_NONBLOCK` only decides what happens after that. A child whose own system
//! call sleeps while holding the lock therefore parks whoever touches the other
//! end, and the child does not have to be misbehaving: `std::io::copy` from an
//! idle socket into a pipe is a `splice(2)` on Linux, which sleeps with the
//! output pipe locked until the socket has data. A terminal service that copies
//! its PTY to stdout that way holds its stdout pipe locked for as long as the
//! terminal is quiet, and the daemon's next `read` of that pipe, made from inside
//! a task, parked one runtime worker for as long. Enough idle attaches, and the
//! runtime stops serving every peer on the host.
//!
//! So each end is owned by one plain OS thread that does the blocking call, and
//! the async side talks to it through a bounded channel. A pipe that is locked by
//! its peer costs one parked thread, which is cheap and which ends when the child
//! does. The channel is bounded so a slow consumer still pushes back on the child.

use std::{
    io::{self, Read, Write},
    os::fd::{AsRawFd, OwnedFd},
    pin::Pin,
    task::{Context, Poll, ready},
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    process::{ChildStderr, ChildStdin, ChildStdout},
    sync::mpsc,
};
use tokio_util::sync::PollSender;

/// How much one blocking read takes from the pipe.
const CHUNK: usize = 16 * 1024;
/// Chunks in flight between a reader thread and its consumer.
const CHANNEL_DEPTH: usize = 8;
/// A read that waited this long for the child is worth a line in the log.
const LONG_WAIT: Duration = Duration::from_secs(30);

/// The child's stdout (or stderr), read by a thread.
pub struct ThreadedReader {
    chunks: mpsc::Receiver<io::Result<Vec<u8>>>,
    pending: Vec<u8>,
    taken: usize,
}

/// The child's stdin, written by a thread.
pub struct ThreadedWriter {
    chunks: PollSender<Vec<u8>>,
}

pub fn stdout_reader(pipe: ChildStdout, label: &str) -> io::Result<ThreadedReader> {
    reader(pipe.into_owned_fd()?, label, "stdout")
}

pub fn stderr_reader(pipe: ChildStderr, label: &str) -> io::Result<ThreadedReader> {
    reader(pipe.into_owned_fd()?, label, "stderr")
}

pub fn stdin_writer(pipe: ChildStdin, label: &str) -> io::Result<ThreadedWriter> {
    writer(pipe.into_owned_fd()?, label)
}

/// How often a waiting reader thread looks up to see whether anyone still wants its data.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// `true` when the pipe has data or has reached its end, `false` after `interval` of silence.
fn wait_readable(file: &std::fs::File, interval: Duration) -> io::Result<bool> {
    let mut fds = [libc::pollfd {
        fd: file.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    }];
    loop {
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), 1, interval.as_millis() as libc::c_int) };
        if ready >= 0 {
            return Ok(ready > 0);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Back to blocking mode: the thread wants its `read` to wait, and `into_owned_fd`
/// leaves the descriptor as the runtime set it.
fn set_blocking(fd: &OwnedFd) -> io::Result<()> {
    let raw = fd.as_raw_fd();
    let flags = unsafe { libc::fcntl(raw, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(raw, libc::F_SETFL, flags & !libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn reader(fd: OwnedFd, label: &str, which: &'static str) -> io::Result<ThreadedReader> {
    set_blocking(&fd)?;
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    let label = label.to_string();
    std::thread::Builder::new()
        .name(format!("fabric-pipe-{which}"))
        .spawn(move || {
            let began = Instant::now();
            let mut file = std::fs::File::from(fd);
            let mut buf = vec![0u8; CHUNK];
            let (mut total, mut longest) = (0u64, Duration::ZERO);
            let end = loop {
                // Wait for the pipe with poll(2), which takes no pipe lock, so a
                // consumer that has gone away is noticed within one interval
                // instead of when the child next writes.
                if tx.is_closed() {
                    break "consumer-gone".to_string();
                }
                match wait_readable(&file, POLL_INTERVAL) {
                    Ok(false) => continue,
                    Ok(true) => {}
                    Err(error) => break format!("poll error: {error}"),
                }
                let waited = Instant::now();
                match file.read(&mut buf) {
                    Ok(0) => break "eof".to_string(),
                    Ok(n) => {
                        longest = longest.max(waited.elapsed());
                        total += n as u64;
                        if tx.blocking_send(Ok(buf[..n].to_vec())).is_err() {
                            break "consumer-gone".to_string();
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        let text = format!("error: {error}");
                        let _ = tx.blocking_send(Err(error));
                        break text;
                    }
                }
            };
            tracing::info!(
                target: "fabric::validation",
                event = "child_pipe_closed",
                child = %label,
                pipe = which,
                end = %end,
                bytes = total,
                open_ms = began.elapsed().as_millis() as u64,
                longest_gap_ms = longest.as_millis() as u64,
                "a child's pipe was read to its end"
            );
            if longest >= LONG_WAIT {
                tracing::info!(
                    target: "fabric::validation",
                    event = "child_pipe_long_wait",
                    child = %label,
                    pipe = which,
                    longest_gap_ms = longest.as_millis() as u64,
                    "a read of a child's pipe waited a long time for data"
                );
            }
        })?;
    Ok(ThreadedReader {
        chunks: rx,
        pending: Vec::new(),
        taken: 0,
    })
}

fn writer(fd: OwnedFd, label: &str) -> io::Result<ThreadedWriter> {
    set_blocking(&fd)?;
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(CHANNEL_DEPTH);
    let label = label.to_string();
    std::thread::Builder::new()
        .name("fabric-pipe-stdin".to_string())
        .spawn(move || {
            let began = Instant::now();
            let mut file = std::fs::File::from(fd);
            let mut total = 0u64;
            let mut end = "closed".to_string();
            while let Some(chunk) = rx.blocking_recv() {
                if let Err(error) = file.write_all(&chunk) {
                    end = format!("error: {error}");
                    break;
                }
                total += chunk.len() as u64;
            }
            // Dropping the file closes the pipe, which is what ends the child's input.
            drop(file);
            tracing::info!(
                target: "fabric::validation",
                event = "child_pipe_closed",
                child = %label,
                pipe = "stdin",
                end = %end,
                bytes = total,
                open_ms = began.elapsed().as_millis() as u64,
                "a child's stdin was written to its end"
            );
        })?;
    Ok(ThreadedWriter {
        chunks: PollSender::new(tx),
    })
}

impl AsyncRead for ThreadedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.taken < self.pending.len() {
                let available = &self.pending[self.taken..];
                let n = available.len().min(out.remaining());
                out.put_slice(&available[..n]);
                self.taken += n;
                return Poll::Ready(Ok(()));
            }
            match ready!(self.chunks.poll_recv(cx)) {
                Some(Ok(chunk)) => {
                    self.pending = chunk;
                    self.taken = 0;
                }
                Some(Err(error)) => return Poll::Ready(Err(error)),
                None => return Poll::Ready(Ok(())),
            }
        }
    }
}

impl AsyncWrite for ThreadedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match ready!(self.chunks.poll_reserve(cx)) {
            Ok(()) => {}
            Err(_) => return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
        }
        if self.chunks.send_item(data.to_vec()).is_err() {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.chunks.close();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        process::Command,
    };

    #[tokio::test]
    async fn cat_round_trips_through_the_threads() {
        let mut command = Command::new("cat");
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().expect("cat");
        let mut stdin = stdin_writer(child.stdin.take().expect("stdin"), "test cat").expect("w");
        let mut stdout =
            stdout_reader(child.stdout.take().expect("stdout"), "test cat").expect("r");
        let payload: Vec<u8> = (0..200_000u32).map(|n| (n % 251) as u8).collect();
        let sent = payload.clone();
        let feeder = tokio::spawn(async move {
            stdin.write_all(&sent).await.expect("write");
            stdin.shutdown().await.expect("shutdown");
        });
        let mut got = Vec::new();
        stdout.read_to_end(&mut got).await.expect("read");
        feeder.await.expect("feeder");
        assert_eq!(got, payload);
        child.wait().await.expect("wait");
    }

    #[tokio::test]
    async fn a_stdin_that_is_shut_down_ends_the_child() {
        let mut command = Command::new("cat");
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().expect("cat");
        let mut stdin = stdin_writer(child.stdin.take().expect("stdin"), "test eof").expect("w");
        stdin.write_all(b"x").await.expect("write");
        stdin.shutdown().await.expect("shutdown");
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("cat did not end after its stdin closed")
            .expect("wait");
        assert!(status.success());
    }

    #[tokio::test]
    async fn a_child_that_exits_reads_as_end_of_file() {
        let mut command = Command::new("true");
        command.stdout(Stdio::piped()).kill_on_drop(true);
        let mut child = command.spawn().expect("true");
        let mut stdout =
            stdout_reader(child.stdout.take().expect("stdout"), "test true").expect("r");
        let mut got = Vec::new();
        stdout.read_to_end(&mut got).await.expect("read");
        assert!(got.is_empty());
        child.wait().await.expect("wait");
    }

    /// The freeze, in one test: children that copy an idle socket into their
    /// stdout (a splice on Linux, which sleeps holding the pipe's lock) must not
    /// stop the runtime, however many of them there are. Run as a child of this
    /// test binary when the marker variable is set.
    #[cfg(target_os = "linux")]
    #[test]
    fn idle_splicing_children_do_not_stop_the_runtime() {
        use std::os::fd::AsFd;
        use std::os::unix::net::UnixStream;

        if std::env::var_os("FABRIC_TEST_IDLE_SPLICE").is_some() {
            // The child: one line, then copy an idle socket into a dup of stdout.
            let mut stdout =
                std::fs::File::from(std::io::stdout().as_fd().try_clone_to_owned().unwrap());
            writeln!(stdout, "ready").unwrap();
            let (mut idle, _peer) = UnixStream::pair().unwrap();
            let _ = std::io::copy(&mut idle, &mut stdout);
            return;
        }

        // A runtime that has stopped cannot time itself out, so an ordinary thread does.
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(60));
            eprintln!("idle splicing children stopped the runtime for 60 s");
            std::process::exit(3);
        });
        const WORKERS: usize = 2;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKERS)
            .enable_all()
            .build()
            .unwrap();
        let ticked = runtime.block_on(async {
            let exe = std::env::current_exe().unwrap();
            let mut kids = Vec::new();
            let mut readers = Vec::new();
            for _ in 0..(WORKERS * 3) {
                let mut command = Command::new(&exe);
                command
                    .args([
                        "--exact",
                        "child_pipes::tests::idle_splicing_children_do_not_stop_the_runtime",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("FABRIC_TEST_IDLE_SPLICE", "1")
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .kill_on_drop(true);
                let mut child = command.spawn().expect("spawn child");
                let pipe = child.stdout.take().unwrap();
                // FABRIC_TEST_RAW_PIPE reads the pipe on the runtime, as the daemon did;
                // the test is then expected to hang or fail. Used by hand to see the bug.
                let mut stdout: Box<dyn AsyncRead + Send + Unpin> =
                    if std::env::var_os("FABRIC_TEST_RAW_PIPE").is_some() {
                        Box::new(pipe)
                    } else {
                        Box::new(stdout_reader(pipe, "test splice").expect("stdout reader"))
                    };
                readers.push(tokio::spawn(async move {
                    let mut buf = [0u8; 256];
                    while let Ok(n) = stdout.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                }));
                kids.push(child);
            }
            // Give every child time to reach its idle copy, then require ticks.
            tokio::time::sleep(Duration::from_millis(800)).await;
            let ticked = tokio::time::timeout(Duration::from_secs(3), async {
                for _ in 0..20 {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .is_ok();
            for reader in &readers {
                reader.abort();
            }
            for child in &mut kids {
                let _ = child.start_kill();
            }
            ticked
        });
        runtime.shutdown_background();
        assert!(ticked, "idle splicing children stopped the runtime");
    }
}
