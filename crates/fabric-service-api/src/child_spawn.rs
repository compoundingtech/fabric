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

use std::{io, time::Duration};

use tokio::process::{Child, Command};

/// Longer than any healthy spawn, short enough that a request learns of the
/// failure before its caller gives up.
pub const SPAWN_LIMIT: Duration = Duration::from_secs(20);

pub async fn spawn(command: Command) -> io::Result<Child> {
    spawn_within(command, SPAWN_LIMIT).await
}

async fn spawn_within(mut command: Command, limit: Duration) -> io::Result<Child> {
    let mut start = tokio::task::spawn_blocking(move || command.spawn());
    match tokio::time::timeout(limit, &mut start).await {
        Ok(Ok(result)) => result,
        Ok(Err(join)) => Err(io::Error::other(format!("spawn task failed: {join}"))),
        Err(_) => {
            tokio::spawn(async move {
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
        let mut child = spawn(command).await.expect("spawn echo");
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
        let error = spawn(Command::new("/nonexistent/fabric-test-program"))
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
        let pending = tokio::spawn(spawn(command));
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
        let error = spawn_within(command, Duration::from_millis(50))
            .await
            .expect_err("the spawn takes longer than the limit");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        // Give the late child time to appear and be killed; nothing may panic.
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
}
