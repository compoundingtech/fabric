//! A process crash cannot flush its queue. A start already written by the
//! dedicated writer must nevertheless survive without an invented end event.
use std::{
    path::PathBuf,
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fabric::{
    audit::{self, Record},
    config::{FabricHome, PeerBook},
    control::{ControlRequest, ControlResponse},
    daemon::{FabricNode, send_control},
    services::{exec, shell},
};
use tokio::net::UnixStream;

struct CommandCleanup(PathBuf);
impl Drop for CommandCleanup {
    fn drop(&mut self) {
        // This pid belongs to the command launched by this test. Keep cleanup
        // armed on assertion failure as well as after the deliberate SIGKILL.
        if let Ok(text) = std::fs::read_to_string(&self.0)
            && let Ok(pid) = text.trim().parse::<i32>()
            && pid > 1
        {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_exec_has_a_start_trace_after_abrupt_daemon_death() -> Result<()> {
    abrupt_daemon_death(false, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_exec_child_cannot_outlive_an_abruptly_killed_daemon() -> Result<()> {
    abrupt_daemon_death(true, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shell_child_cannot_outlive_an_abruptly_killed_daemon() -> Result<()> {
    abrupt_daemon_death(true, true).await
}

fn server_command(home: &FabricHome) -> tokio::process::Command {
    let binary = std::env::var_os("FABRIC_CRASH_TEST_BIN")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_fabric").into());
    let mut command = tokio::process::Command::new(binary);
    command
        .arg("--home")
        .arg(home.root())
        .args(["daemon", "--allow-shell"])
        .env("SHELL", "/bin/sh")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

fn child_running(pid: i32) -> Result<bool> {
    #[cfg(target_os = "linux")]
    {
        let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => stat,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::ESRCH) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        let state = stat
            .rsplit_once(')')
            .context("invalid child kernel stat")?
            .1
            .trim_start();
        Ok(!state.starts_with('Z') && !state.starts_with('X'))
    }
    #[cfg(target_os = "macos")]
    {
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of_val(&info) as i32;
        let got = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size,
            )
        };
        if got == size {
            return Ok(info.pbi_status != 5);
        }
        let error = std::io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::ENOENT)) {
            Ok(false)
        } else {
            Err(error.into())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        if unsafe { libc::kill(pid, 0) } == 0 {
            return Ok(true);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error.into())
        }
    }
}

async fn abrupt_daemon_death(check_cleanup: bool, is_shell: bool) -> Result<()> {
    let temp = tempfile::tempdir()?;
    let server_home = FabricHome::new(temp.path().join("server"));
    let client_home = FabricHome::new(temp.path().join("client"));
    let client = FabricNode::start(client_home.clone()).await?;
    let mut book = PeerBook::load(&server_home)?;
    book.add_with_allow(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        Some(vec!["exec".into(), "shell".into()]),
    );
    book.save(&server_home)?;
    let mut server = server_command(&server_home).spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let addr = loop {
        if let Ok(ControlResponse::Status { endpoint_addr, .. }) =
            send_control(&server_home, ControlRequest::Status).await
        {
            break serde_json::from_value::<iroh::EndpointAddr>(endpoint_addr)?;
        }
        if Instant::now() >= deadline {
            bail!("server never became ready");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut book = PeerBook::load(&client_home)?;
    book.add_with_allow(
        addr.id,
        Some("server".into()),
        Some(addr),
        Some(vec!["echo".into()]),
    );
    book.save(&client_home)?;
    client.state().reload_peers().await?;
    let path = match send_control(
        &client_home,
        if is_shell {
            ControlRequest::Shell {
                peer: "server".into(),
            }
        } else {
            ControlRequest::Exec {
                peer: "server".into(),
            }
        },
    )
    .await?
    {
        ControlResponse::Exec { socket } | ControlResponse::Shell { socket } => socket,
        response => bail!("unexpected exec response: {response:?}"),
    };
    let mut stream = UnixStream::connect(path).await?;
    let pid_file = temp.path().join("command.pid");
    let cleanup = CommandCleanup(pid_file.clone());
    if is_shell {
        let quoted = format!("'{}'", pid_file.to_string_lossy().replace('\'', "'\\''"));
        let input = format!("trap '' HUP; printf '%s\\n' \"$$\" > {quoted}; exec sleep 30\n");
        shell::write_client_stdin(&mut stream, input.as_bytes()).await?;
    } else {
        // Pass the path as a positional argument, so no shell escaping is needed.
        exec::write_client_argv(
            &mut stream,
            &[
                "/bin/sh".into(),
                "-c".into(),
                "trap '' HUP; printf '%s\\n' \"$$\" > \"$1\"; exec sleep 30".into(),
                "audit-command".into(),
                pid_file.to_string_lossy().into_owned(),
            ],
        )
        .await?;
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let (session, pid) = loop {
        let records =
            audit::read(server_home.root(), None, Some(&client.id().to_string()))?.records;
        let pid = std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|text| {
                text.ends_with('\n')
                    .then(|| text.trim().parse::<i32>().ok())
                    .flatten()
            })
            .filter(|pid| *pid > 1);
        if let Some(pid) = pid
            && let Some(start) = records.iter().find_map(|record| match record {
                Record::ExecStart(call) | Record::ShellStart(call) => Some(call),
                _ => None,
            })
        {
            break (start.session_id.clone(), pid);
        }
        if Instant::now() >= deadline {
            bail!("running command did not publish its PID and audit start");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    anyhow::ensure!(
        child_running(pid)?,
        "owned child exited before daemon SIGKILL"
    );
    server
        .start_kill()
        .context("send SIGKILL to running daemon")?;
    server.wait().await.context("wait for killed daemon")?;
    if check_cleanup {
        #[cfg(target_os = "macos")]
        {
            server = server_command(&server_home).spawn()?;
            let deadline = Instant::now() + Duration::from_secs(10);
            while send_control(&server_home, ControlRequest::Status)
                .await
                .is_err()
            {
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "replacement daemon never became ready"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        let gone = loop {
            let running = child_running(pid).context("observe owned child after daemon death")?;
            if !running {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        anyhow::ensure!(
            gone,
            "owned child {pid} remained running after daemon crash cleanup"
        );
        #[cfg(target_os = "macos")]
        {
            server.start_kill()?;
            server.wait().await?;
        }
    }
    let records = audit::read(server_home.root(), None, Some(&client.id().to_string()))?.records;
    assert_eq!(
        records
            .iter()
            .filter(
                |record| matches!(record, Record::ExecStart(call) | Record::ShellStart(call) if call.session_id == session)
            )
            .count(),
        1
    );
    assert!(
        !records
            .iter()
            .any(|record| matches!(record, Record::ExecEnd(call) | Record::ShellEnd(call) if call.session_id == session))
    );
    drop(cleanup);
    client.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_child_storage_preserves_control_and_echo_but_never_executes_user_code() -> Result<()>
{
    let temp = tempfile::tempdir()?;
    let server_home = FabricHome::new(temp.path().join("server"));
    let client_home = FabricHome::new(temp.path().join("client"));
    std::fs::create_dir_all(server_home.root())?;
    std::fs::write(server_home.root().join("children"), b"occupied")?;
    let server = FabricNode::start(server_home.clone()).await?;
    let client = FabricNode::start(client_home.clone()).await?;
    let mut book = PeerBook::load(&server_home)?;
    book.add_with_allow(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        Some(vec!["exec".into(), "echo".into()]),
    );
    book.save(&server_home)?;
    server.state().reload_peers().await?;
    let mut book = PeerBook::load(&client_home)?;
    book.add(server.id(), Some("server".into()), Some(server.addr()));
    book.save(&client_home)?;
    client.state().reload_peers().await?;
    assert!(matches!(
        send_control(&server_home, ControlRequest::Status).await?,
        ControlResponse::Status { .. }
    ));
    client.ping("server").await?;
    let socket = client.dial("server", exec::EXEC_PROTOCOL).await?;
    let mut stream = UnixStream::connect(socket).await?;
    let marker = temp.path().join("must-not-run");
    exec::write_client_argv(
        &mut stream,
        &[
            "/bin/sh".into(),
            "-c".into(),
            "printf ran > \"$1\"".into(),
            "storage-fault".into(),
            marker.to_string_lossy().into_owned(),
        ],
    )
    .await?;
    let reply = tokio::time::timeout(Duration::from_secs(3), async {
        let mut refused = false;
        while let Some(frame) = exec::read_server_frame(&mut stream).await? {
            match frame {
                exec::ServerFrame::Error(message) => {
                    refused |= message.contains("fabric:spawn_failed")
                }
                exec::ServerFrame::Exit(code) => {
                    anyhow::ensure!(refused && code == 127);
                    return Ok::<_, anyhow::Error>(());
                }
                _ => {}
            }
        }
        bail!("spawn failure was not returned");
    })
    .await?;
    assert!(!marker.exists());
    client.shutdown().await?;
    server.shutdown().await?;
    reply
}
