//! A tunnel session that ends cleanly is the session ending, not a lost
//! transport.
//!
//! One request per connection through a dial listener produces a session that
//! finishes within a second, with both sides done. Whether the client's attach
//! sees the far stream close before or after its own writer notices the
//! session is complete depends on scheduling, and a short session loses that
//! race a fraction of a percent of the time. When it does, the retry loop used
//! to record a reconnect attempt, log "generic dial lost its transport" and
//! rewrite the telemetry file for a session that had nothing left to resume.
//! Measured on a live pair that was about thirty warnings a minute.
//!
//! The race needs two daemons in separate processes under real scheduling;
//! two nodes in one test runtime never lose it. So this test runs the real
//! binary twice and sends enough short sessions through it that losing the
//! race at least once is certain.

use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use fabric::{
    config::FabricHome,
    control::{ControlRequest, ControlResponse},
    daemon::send_control,
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const SESSIONS: usize = 2400;
const CONCURRENCY: usize = 8;

struct Daemon {
    child: Child,
    home: FabricHome,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn fabric(home: &FabricHome, args: &[&str]) -> Result<String> {
    let output = Command::new(env!("CARGO_BIN_EXE_fabric"))
        .arg("--home")
        .arg(home.root())
        .args(args)
        .output()
        .with_context(|| format!("failed to run fabric {args:?}"))?;
    if !output.status.success() {
        bail!(
            "fabric {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

async fn start_daemon(dir: &TempDir) -> Result<Daemon> {
    let home = FabricHome::new(dir.path());
    let key = dir.path().join("identity.toml");
    fabric(&home, &["key", "gen", "--out", key.to_str().unwrap()])?;
    let child = Command::new(env!("CARGO_BIN_EXE_fabric"))
        .arg("--home")
        .arg(home.root())
        .arg("daemon")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let daemon = Daemon { child, home };
    for _ in 0..100 {
        if send_control(&daemon.home, ControlRequest::Status)
            .await
            .is_ok()
        {
            return Ok(daemon);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("a daemon never became ready")
}

/// Reads one request, answers it and closes: the shape of a request/response
/// service whose clients make one connection per request.
async fn spawn_reply_then_close_service() -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = [0u8; 64];
                if let Ok(read) = stream.read(&mut request).await {
                    let _ = stream.write_all(&request[..read]).await;
                }
                let _ = stream.shutdown().await;
            });
        }
    });
    Ok((addr, task))
}

/// One request: write it, read the whole reply, close.
async fn one_request(addr: &str, i: usize) -> Result<()> {
    let payload = format!("request-{i}");
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(payload.as_bytes()).await?;
    let mut reply = vec![0u8; payload.len()];
    tokio::time::timeout(Duration::from_secs(30), stream.read_exact(&mut reply))
        .await
        .context("the reply timed out")??;
    anyhow::ensure!(reply == payload.as_bytes(), "wrong reply for {payload}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clean_one_request_sessions_are_not_counted_as_lost_transports() -> Result<()> {
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let server = start_daemon(&server_dir).await?;
    let client = start_daemon(&client_dir).await?;

    let server_id = fabric(&server.home, &["id"])?;
    let client_id = fabric(&client.home, &["id"])?;
    let server_addr = fabric(&server.home, &["addr"])?;
    let client_addr = fabric(&client.home, &["addr"])?;
    fabric(
        &server.home,
        &[
            "add",
            &client_id,
            "client",
            "--addr-json",
            &client_addr,
            "--allow",
            "short",
        ],
    )?;
    fabric(
        &client.home,
        &[
            "add",
            &server_id,
            "server",
            "--addr-json",
            &server_addr,
            "--allow",
            "short",
        ],
    )?;

    let (service_addr, service_task) = spawn_reply_then_close_service().await?;
    fabric(&server.home, &["expose", "short", "--tcp", &service_addr])?;
    let local_addr = fabric(
        &client.home,
        &["dial", "server", "short", "--tcp", "127.0.0.1:0"],
    )?;

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..SESSIONS {
        let local_addr = local_addr.clone();
        tasks.spawn(async move { one_request(&local_addr, i).await });
        if tasks.len() >= CONCURRENCY {
            tasks.join_next().await.context("no task")???;
        }
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let ControlResponse::ReachabilityStatus {
        connection_telemetry,
        ..
    } = send_control(&client.home, ControlRequest::ReachabilityStatus).await?
    else {
        bail!("unexpected status response");
    };
    let attempts: u64 = connection_telemetry
        .values()
        .map(|peer| peer.reconnect_attempts)
        .sum();
    let losses: u64 = connection_telemetry.values().map(|peer| peer.losses).sum();
    assert_eq!(
        (losses, attempts),
        (0, 0),
        "{SESSIONS} clean one-request sessions were counted as lost transports: \
         {connection_telemetry:?}"
    );

    service_task.abort();
    Ok(())
}
