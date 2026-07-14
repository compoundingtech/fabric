use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fabric::{
    config::{FabricHome, PeerBook},
    daemon::FabricNode,
};
use tempfile::TempDir;

fn fabric_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric")
}

fn run_shell(home: &FabricHome, peer: &str, input: &str) -> Result<Output> {
    let mut child = Command::new(fabric_bin())
        .arg("--home")
        .arg(home.root())
        .arg("shell")
        .arg(peer)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn fabric shell")?;

    child
        .stdin
        .as_mut()
        .context("fabric shell stdin missing")?
        .write_all(input.as_bytes())?;

    child.wait_with_output().context("fabric shell failed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_from_remote_shell_detaches_and_preserves_allow_shell() -> Result<()> {
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());
    let _node_a_guard = CliDaemonGuard::new(node_a_home.clone());

    let output = fabric_output(&node_a_home, &["up", "--allow-shell"])?;
    assert_success(&output, "fabric up --allow-shell");
    wait_for_cli_status(&node_a_home, true).await?;
    let node_a_id: iroh::EndpointId = fabric_stdout(&node_a_home, &["id"])?.trim().parse()?;
    let node_a_addr = cli_addr(&node_a_home)?;

    let node_b = FabricNode::start(node_b_home.clone()).await?;
    cli_add_peer(
        &node_a_home,
        node_b.id(),
        "node-b",
        serde_json::to_string(&node_b.addr())?,
    )?;
    trust_peer(
        &node_b_home,
        &node_b,
        node_a_id,
        Some("node-a"),
        Some(node_a_addr),
    )
    .await?;

    let before = run_shell(
        &node_b_home,
        "node-a",
        "printf 'before-restart\\n'; exit 0\n",
    )?;
    assert_success(&before, "pre-restart shell");
    assert!(
        String::from_utf8_lossy(&before.stdout).contains("before-restart"),
        "stdout was: {}",
        String::from_utf8_lossy(&before.stdout)
    );

    let restart_input = format!(
        "{} --home {} restart\nexit 0\n",
        sh_quote(fabric_bin()),
        sh_quote_path(node_a_home.root())
    );
    let restart = run_shell(&node_b_home, "node-a", &restart_input)?;
    assert_success(&restart, "remote fabric restart");
    assert!(
        String::from_utf8_lossy(&restart.stdout).contains("restart scheduled"),
        "stdout was: {}",
        String::from_utf8_lossy(&restart.stdout)
    );

    wait_for_restart_complete(&node_a_home).await?;
    let status = wait_for_cli_status(&node_a_home, true).await?;
    assert!(
        status.contains("shell\tallowed"),
        "status did not preserve allow_shell: {status}"
    );

    let restarted_addr = cli_addr(&node_a_home)?;
    trust_peer(
        &node_b_home,
        &node_b,
        node_a_id,
        Some("node-a"),
        Some(restarted_addr),
    )
    .await?;
    let node_b_status = fabric_stdout(&node_b_home, &["status"])?;
    assert!(
        node_b_status.contains("node-a") && node_b_status.contains("reachable"),
        "node B reachability did not recover: {node_b_status}"
    );

    let after = run_shell(
        &node_b_home,
        "node-a",
        "printf 'after-restart\\n'; exit 0\n",
    )?;
    assert_success(&after, "post-restart shell");
    assert!(
        String::from_utf8_lossy(&after.stdout).contains("after-restart"),
        "stdout was: {}",
        String::from_utf8_lossy(&after.stdout)
    );

    let restart_log = fs::read_to_string(node_a_home.restart_log_path())?;
    assert!(
        restart_log.contains("restart complete"),
        "restart log was: {restart_log}"
    );

    node_b.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trusted_peer_with_allow_shell_runs_remote_shell_and_propagates_exit() -> Result<()> {
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());

    let node_a = FabricNode::start_with_options(node_a_home.clone(), true).await?;
    let node_b = FabricNode::start(node_b_home.clone()).await?;
    trust_peer(
        &node_a_home,
        &node_a,
        node_b.id(),
        Some("node-b"),
        Some(node_b.addr()),
    )
    .await?;
    trust_peer(
        &node_b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    let output = run_shell(
        &node_b_home,
        "node-a",
        "printf 'fabric-shell-ok\\n'; exit 7\n",
    )?;
    assert_eq!(output.status.code(), Some(7));
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("fabric-shell-ok"),
        "stdout was: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trusted_peer_without_allow_shell_is_refused() -> Result<()> {
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());

    let node_a = FabricNode::start(node_a_home.clone()).await?;
    let node_b = FabricNode::start(node_b_home.clone()).await?;
    trust_peer(
        &node_a_home,
        &node_a,
        node_b.id(),
        Some("node-b"),
        Some(node_b.addr()),
    )
    .await?;
    trust_peer(
        &node_b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    let output = run_shell(&node_b_home, "node-a", "exit 0\n")?;
    assert_eq!(output.status.code(), Some(126));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("remote shell is disabled"),
        "stderr was: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn untrusted_peer_is_refused_even_when_shell_is_allowed() -> Result<()> {
    let node_a_dir = TempDir::new()?;
    let node_c_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_c_home = FabricHome::new(node_c_dir.path());

    let node_a = FabricNode::start_with_options(node_a_home.clone(), true).await?;
    let node_c = FabricNode::start(node_c_home.clone()).await?;
    trust_peer(
        &node_c_home,
        &node_c,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    let output = run_shell(&node_c_home, "node-a", "echo should-not-run\n")?;
    assert!(
        !output.status.success(),
        "untrusted shell unexpectedly succeeded"
    );
    assert!(
        !String::from_utf8_lossy(&output.stdout).contains("should-not-run"),
        "untrusted shell command ran: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    node_c.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

async fn trust_peer(
    home: &FabricHome,
    node: &FabricNode,
    id: iroh::EndpointId,
    name: Option<&str>,
    addr: Option<iroh::EndpointAddr>,
) -> Result<()> {
    let mut peers = PeerBook::load(home)?;
    peers.add(id, name.map(str::to_string), addr);
    peers.save(home)?;
    node.state().reload_peers().await?;
    Ok(())
}

struct CliDaemonGuard {
    home: FabricHome,
}

impl CliDaemonGuard {
    fn new(home: FabricHome) -> Self {
        Self { home }
    }
}

impl Drop for CliDaemonGuard {
    fn drop(&mut self) {
        let _ = Command::new(fabric_bin())
            .arg("--home")
            .arg(self.home.root())
            .arg("down")
            .output();
    }
}

fn fabric_output(home: &FabricHome, args: &[&str]) -> Result<Output> {
    Command::new(fabric_bin())
        .arg("--home")
        .arg(home.root())
        .args(args)
        .output()
        .context("failed to run fabric")
}

fn fabric_stdout(home: &FabricHome, args: &[&str]) -> Result<String> {
    let output = fabric_output(home, args)?;
    assert_success(&output, &format!("fabric {}", args.join(" ")));
    Ok(String::from_utf8(output.stdout)?)
}

fn assert_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed: status={:?}\nstdout={}\nstderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn cli_addr(home: &FabricHome) -> Result<iroh::EndpointAddr> {
    Ok(serde_json::from_str(
        fabric_stdout(home, &["addr"])?.trim(),
    )?)
}

fn cli_add_peer(
    home: &FabricHome,
    id: iroh::EndpointId,
    name: &str,
    addr_json: String,
) -> Result<()> {
    let output = Command::new(fabric_bin())
        .arg("--home")
        .arg(home.root())
        .arg("add")
        .arg(id.to_string())
        .arg(name)
        .arg("--addr-json")
        .arg(addr_json)
        .output()
        .context("failed to run fabric add")?;
    assert_success(&output, "fabric add");
    Ok(())
}

async fn wait_for_cli_status(home: &FabricHome, expected_allow_shell: bool) -> Result<String> {
    let started = Instant::now();
    loop {
        let output = fabric_output(home, &["status"])?;
        let current;
        if output.status.success() {
            let stdout = String::from_utf8(output.stdout)?;
            let expected = if expected_allow_shell {
                "shell\tallowed"
            } else {
                "shell\tdisabled"
            };
            if stdout.contains(expected) {
                return Ok(stdout);
            }
            current = stdout;
        } else {
            current = format!(
                "status={:?}\nstdout={}\nstderr={}",
                output.status.code(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        if started.elapsed() > Duration::from_secs(20) {
            bail!("timed out waiting for fabric status; last output: {current}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_restart_complete(home: &FabricHome) -> Result<()> {
    let started = Instant::now();
    loop {
        let current = match fs::read_to_string(home.restart_log_path()) {
            Ok(log) if log.contains("restart complete") => return Ok(()),
            Ok(log) => log,
            Err(error) => format!("{error:#}"),
        };
        if started.elapsed() > Duration::from_secs(20) {
            bail!("timed out waiting for restart completion; last log: {current}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn sh_quote_path(path: &Path) -> String {
    sh_quote(&path.display().to_string())
}
