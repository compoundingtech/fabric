//! Start a child process without holding a runtime worker while the operating
//! system does it.
//!
//! `Command::spawn` is a blocking call dressed as an ordinary one: on Linux the
//! parent reads a pipe until the child has either executed or failed to, and a
//! parent that forks a large process can wait a long time for the kernel. Called
//! inline in async code it takes one of the runtime's few worker threads for that
//! whole time, and a daemon whose workers are all taken (or whose one worker is
//! taken by something that never returns) stops serving every peer. A stall seen
//! on 2026-10-09 and 2026-10-10 had exactly one worker parked in a pipe read
//! while the daemon's only child was a command it had just started.
//!
//! So the spawn runs on the blocking pool, which exists for this, and the caller
//! waits for it with a limit. A spawn that does not finish in time fails that one
//! request; it no longer fails the daemon. If it finishes after the limit, the
//! child it produced is killed instead of left behind.

use std::{
    collections::BTreeMap,
    io,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::process::{Child, Command};

/// Spawns that have started and not yet come back, by sequence number: what each
/// was for and when it began. A stall report reads this to say which request a
/// stuck spawn belongs to, which the child's own name cannot.
static IN_FLIGHT: Mutex<BTreeMap<u64, (String, Instant)>> = Mutex::new(BTreeMap::new());
static NEXT: AtomicU64 = AtomicU64::new(0);

/// A spawn that took longer than this is worth a log line even when it worked.
const SLOW_SPAWN: Duration = Duration::from_secs(1);

/// One line naming every spawn still in progress and how long it has been going,
/// oldest first.
pub fn in_flight() -> String {
    let now = Instant::now();
    let Ok(spawns) = IN_FLIGHT.lock() else {
        return "spawns-in-flight=unreadable".to_string();
    };
    let mut shown: Vec<(Duration, &str)> = spawns
        .values()
        .map(|(label, since)| (now.duration_since(*since), label.as_str()))
        .collect();
    shown.sort_by(|a, b| b.0.cmp(&a.0));
    let list = shown
        .iter()
        .take(8)
        .map(|(age, label)| format!("{label} {}s", age.as_secs()))
        .collect::<Vec<_>>()
        .join("; ");
    format!("spawns-in-flight={} [{list}]", shown.len())
}

struct InFlight(u64);

impl InFlight {
    fn begin(label: &str) -> Self {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut spawns) = IN_FLIGHT.lock() {
            spawns.insert(id, (label.to_string(), Instant::now()));
        }
        Self(id)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if let Ok(mut spawns) = IN_FLIGHT.lock() {
            spawns.remove(&self.0);
        }
    }
}

/// Longer than any healthy spawn, short enough that a request learns of the
/// failure before its caller gives up.
pub const SPAWN_LIMIT: Duration = Duration::from_secs(20);

/// `label` says what the spawn is for (service, program, who asked); it appears
/// in [`in_flight`] while the spawn runs and in the log if it was slow.
pub async fn spawn(label: &str, command: Command) -> io::Result<Child> {
    spawn_within(label, command, SPAWN_LIMIT).await
}

async fn spawn_within(label: &str, mut command: Command, limit: Duration) -> io::Result<Child> {
    let began = Instant::now();
    let in_flight = InFlight::begin(label);
    let mut start = tokio::task::spawn_blocking(move || command.spawn());
    let result = tokio::time::timeout(limit, &mut start).await;
    let took = began.elapsed();
    if took >= SLOW_SPAWN {
        tracing::warn!(
            target: "fabric::validation",
            event = "slow_child_spawn",
            label = %label,
            took_ms = took.as_millis() as u64,
            finished = result.is_ok(),
            "starting a child process took a long time"
        );
    }
    match result {
        Ok(Ok(result)) => result,
        Ok(Err(join)) => Err(io::Error::other(format!("spawn task failed: {join}"))),
        Err(_) => {
            // The blocking call is still running: keep it in the in-flight list
            // until it ends, and kill whatever it eventually produces.
            tokio::spawn(async move {
                let _in_flight = in_flight;
                if let Ok(Ok(mut late)) = start.await {
                    let _ = late.start_kill();
                    let _ = late.wait().await;
                }
            });
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("starting the process took longer than {}s", limit.as_secs()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn a_process_is_started_and_its_output_read() {
        let mut command = Command::new("echo");
        command.arg("hello").stdout(std::process::Stdio::piped());
        let mut child = spawn("test echo", command).await.expect("spawn echo");
        let mut out = String::new();
        child
            .stdout
            .take()
            .expect("stdout")
            .read_to_string(&mut out)
            .await
            .expect("read");
        assert_eq!(out.trim(), "hello");
        child.wait().await.expect("wait");
    }

    #[tokio::test]
    async fn a_program_that_does_not_exist_is_an_error_not_a_hang() {
        let error = spawn(
            "test missing",
            Command::new("/nonexistent/fabric-test-program"),
        )
        .await
        .expect_err("no such program");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    /// The runtime keeps serving while a spawn is outstanding: one worker, and a
    /// task that needs it, run to the end while the spawn call is still being
    /// awaited.
    #[tokio::test(flavor = "current_thread")]
    async fn waiting_for_a_spawn_does_not_hold_the_runtime() {
        let mut command = Command::new("sleep");
        command.arg("5").kill_on_drop(true);
        let pending = tokio::spawn(async move { spawn("test sleep", command).await });
        let ticked = tokio::time::timeout(Duration::from_secs(2), async {
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(ticked.is_ok(), "the runtime did not run while spawning");
        let mut child = pending.await.expect("join").expect("spawn sleep");
        child.start_kill().expect("kill");
        child.wait().await.expect("wait");
    }

    /// The parent waits in `spawn` until the child has executed, so a child that
    /// dawdles before exec is a slow spawn, which is what the limit is for.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_spawn_that_outlasts_its_limit_fails_the_request() {
        let mut command = Command::new("sleep");
        command.arg("30").kill_on_drop(true);
        unsafe {
            command.pre_exec(|| {
                std::thread::sleep(Duration::from_millis(400));
                Ok(())
            });
        }
        let error = spawn_within("test slow", command, Duration::from_millis(50))
            .await
            .expect_err("the spawn takes longer than the limit");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        // Give the late child time to appear and be killed; nothing may panic.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(!in_flight().contains("test slow"), "{}", in_flight());
    }

    /// A child that writes nothing and never exits must not hold a runtime
    /// worker: with every worker busy reading its silent pipe, the ticks below
    /// would stop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn silent_children_that_never_exit_do_not_park_the_runtime() {
        let mut readers = Vec::new();
        let mut children = Vec::new();
        for n in 0..6 {
            let mut command = Command::new("sleep");
            command
                .arg("30")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);
            let mut child = spawn(&format!("test silent {n}"), command)
                .await
                .expect("spawn sleep");
            let mut stdout = child.stdout.take().expect("stdout");
            let mut stderr = child.stderr.take().expect("stderr");
            readers.push(tokio::spawn(async move {
                let mut byte = [0u8; 1];
                let _ = stdout.read(&mut byte).await;
            }));
            readers.push(tokio::spawn(async move {
                let mut byte = [0u8; 1];
                let _ = stderr.read(&mut byte).await;
            }));
            children.push(child);
        }
        let ticked = tokio::time::timeout(Duration::from_secs(2), async {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        for reader in &readers {
            reader.abort();
        }
        for child in &mut children {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
        assert!(ticked.is_ok(), "twelve silent pipes stopped the runtime");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_spawn_in_progress_is_named_until_it_ends() {
        let mut command = Command::new("true");
        unsafe {
            command.pre_exec(|| {
                std::thread::sleep(Duration::from_millis(500));
                Ok(())
            });
        }
        let pending =
            tokio::spawn(async move { spawn("test named spawn for peer-x", command).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        let during = in_flight();
        assert!(during.contains("test named spawn for peer-x"), "{during}");
        let mut child = pending.await.expect("join").expect("spawn");
        child.wait().await.expect("wait");
        assert!(!in_flight().contains("test named spawn"), "{}", in_flight());
    }
}
