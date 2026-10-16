//! A process crash cannot flush its queue. A start already written by the
//! dedicated writer must nevertheless survive without an invented end event.
use std::{
    path::PathBuf,
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use fabric::{
    audit::{self, Record},
    config::{FabricHome, PeerBook},
    control::{ControlRequest, ControlResponse},
    daemon::{FabricNode, send_control},
    services::exec,
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
    let temp = tempfile::tempdir()?;
    let server_home = FabricHome::new(temp.path().join("server"));
    let client_home = FabricHome::new(temp.path().join("client"));
    let client = FabricNode::start(client_home.clone()).await?;
    let mut book = PeerBook::load(&server_home)?;
    book.add_with_allow(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        Some(vec!["exec".into()]),
    );
    book.save(&server_home)?;
    let mut server = tokio::process::Command::new(env!("CARGO_BIN_EXE_fabric"))
        .arg("--home")
        .arg(server_home.root())
        .arg("daemon")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
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
        ControlRequest::Exec {
            peer: "server".into(),
        },
    )
    .await?
    {
        ControlResponse::Exec { socket } => socket,
        response => bail!("unexpected exec response: {response:?}"),
    };
    let mut stream = UnixStream::connect(path).await?;
    let pid_file = temp.path().join("command.pid");
    let cleanup = CommandCleanup(pid_file.clone());
    // Pass the path as a positional argument, so no shell escaping is needed.
    exec::write_client_argv(
        &mut stream,
        &[
            "/bin/sh".into(),
            "-c".into(),
            "printf '%s\\n' \"$$\" > \"$1\"; exec sleep 30".into(),
            "audit-command".into(),
            pid_file.to_string_lossy().into_owned(),
        ],
    )
    .await?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let session = loop {
        let records =
            audit::read(server_home.root(), None, Some(&client.id().to_string()))?.records;
        if pid_file.exists()
            && let Some(start) = records.iter().find_map(|record| match record {
                Record::ExecStart(call) => Some(call),
                _ => None,
            })
        {
            break start.session_id.clone();
        }
        if Instant::now() >= deadline {
            bail!("running command never wrote its audit start");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    server.start_kill()?;
    server.wait().await?;
    let records = audit::read(server_home.root(), None, Some(&client.id().to_string()))?.records;
    assert_eq!(
        records
            .iter()
            .filter(
                |record| matches!(record, Record::ExecStart(call) if call.session_id == session)
            )
            .count(),
        1
    );
    assert!(
        !records
            .iter()
            .any(|record| matches!(record, Record::ExecEnd(call) if call.session_id == session))
    );
    drop(cleanup);
    client.shutdown().await?;
    Ok(())
}
