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
    shell::{self, ServerFrame},
};
use iroh::{
    Endpoint,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
#[cfg(unix)]
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

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

#[derive(Debug, Clone)]
struct LegacyRawShell;

impl ProtocolHandler for LegacyRawShell {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id().to_string();
        let (mut send, mut recv) = connection.accept_bi().await?;
        shell::serve_shell_session(&mut recv, &mut send, &peer)
            .await
            .map_err(|error| AcceptError::from_boxed(error.into_boxed_dyn_error()))?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

/// The legacy ALPN is a wire contract with every released Fabric: it carries
/// one-shot shell framing and never generic tunnel frames. A build that routes
/// shell/0 through the tunnel session protocol makes an old peer reject the
/// first server frame with "unknown tunnel frame 17" and reconnect forever,
/// which is exactly what a mixed-version rollout hit in production. This pins
/// the separation so that regression fails here instead of on a real machine.
#[test]
fn legacy_and_resumable_shell_alpns_stay_separate() {
    assert_eq!(shell::SHELL_ALPN, b"fabric/shell/0");
    assert_eq!(shell::RESUMABLE_SHELL_ALPN, b"fabric/shell/1");
    assert_ne!(
        shell::SHELL_ALPN,
        shell::RESUMABLE_SHELL_ALPN,
        "the resumable tunnel framing must never reuse the legacy ALPN"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_client_falls_back_to_legacy_raw_shell_zero() -> Result<()> {
    let legacy_endpoint = Endpoint::bind(presets::N0).await?;
    let legacy = Router::builder(legacy_endpoint)
        .accept(shell::SHELL_ALPN, LegacyRawShell)
        .spawn();
    legacy.endpoint().online().await;

    let client_dir = TempDir::new()?;
    let client_home = FabricHome::new(client_dir.path());
    let client = FabricNode::start(client_home.clone()).await?;
    trust_peer(
        &client_home,
        &client,
        legacy.endpoint().id(),
        Some("legacy"),
        Some(legacy.endpoint().addr()),
    )
    .await?;

    let output = run_shell(
        &client_home,
        "legacy",
        "printf 'legacy-shell-zero-ok\\n'; exit 0\n",
    )?;
    assert_success(&output, "legacy shell/0 fallback");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("legacy-shell-zero-ok"),
        "stdout was: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("unknown tunnel frame"),
        "stderr was: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    client.shutdown().await?;
    legacy.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unavailable_old_peer_later_falls_back_to_raw_shell_zero() -> Result<()> {
    let legacy_secret = iroh::SecretKey::generate();
    let legacy_id = legacy_secret.public();
    let client_dir = TempDir::new()?;
    let client_home = FabricHome::new(client_dir.path());
    let client = FabricNode::start(client_home.clone()).await?;
    trust_peer(
        &client_home,
        &client,
        legacy_id,
        Some("legacy-later"),
        Some(iroh::EndpointAddr::new(legacy_id)),
    )
    .await?;

    let mut shell_child = tokio::process::Command::new(fabric_bin())
        .arg("--home")
        .arg(client_home.root())
        .arg("shell")
        .arg("legacy-later")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Reap the child on any abnormal exit. A panicking assertion unwinds
        // past the explicit kill, and a leaked `fabric shell` would keep a
        // tunnel session alive on the server for the rest of the run.
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn shell for unavailable legacy peer")?;
    let mut stdin = shell_child.stdin.take().context("shell stdin missing")?;
    let mut stdout = shell_child.stdout.take().context("shell stdout missing")?;
    let mut stderr = shell_child.stderr.take().context("shell stderr missing")?;
    stdin
        .write_all(b"printf '%s-%s\\n' legacy later; exit 0\n")
        .await?;

    // Do not start the old endpoint until the client has observed a real
    // transient pre-attach failure. The command is already buffered on the
    // local socket, proving protocol selection has not consumed or reframed it.
    let mut stderr_output =
        read_until_marker(&mut stderr, b"probing remote shell protocol again").await?;

    let legacy_endpoint = Endpoint::builder(presets::N0)
        .secret_key(legacy_secret)
        .bind()
        .await?;
    let legacy = Router::builder(legacy_endpoint)
        .accept(shell::SHELL_ALPN, LegacyRawShell)
        .spawn();
    legacy.endpoint().online().await;
    trust_peer(
        &client_home,
        &client,
        legacy_id,
        Some("legacy-later"),
        Some(legacy.endpoint().addr()),
    )
    .await?;

    let output = read_until_marker(&mut stdout, b"legacy-later").await?;
    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(30), shell_child.wait())
        .await
        .context("legacy fallback shell did not exit")??;
    stderr.read_to_end(&mut stderr_output).await?;

    assert_eq!(status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&output).contains("legacy-later"),
        "stdout was: {}",
        String::from_utf8_lossy(&output)
    );
    assert!(
        !String::from_utf8_lossy(&stderr_output).contains("unknown tunnel frame"),
        "stderr was: {}",
        String::from_utf8_lossy(&stderr_output)
    );

    client.shutdown().await?;
    legacy.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_raw_shell_zero_client_talks_to_new_server() -> Result<()> {
    let server_dir = TempDir::new()?;
    let server_home = FabricHome::new(server_dir.path());
    let server = FabricNode::start_with_options(server_home.clone(), true).await?;
    let legacy_client = Endpoint::bind(presets::N0).await?;
    trust_peer(
        &server_home,
        &server,
        legacy_client.id(),
        Some("legacy-client"),
        Some(legacy_client.addr()),
    )
    .await?;

    let connection = legacy_client
        .connect(server.addr(), shell::SHELL_ALPN)
        .await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    shell::write_client_stdin(&mut send, b"printf 'new-server-shell-zero-ok\\n'; exit 0\n").await?;
    shell::write_client_eof(&mut send).await?;

    let mut output = Vec::new();
    let mut exit = None;
    while let Some(frame) = shell::read_server_frame(&mut recv).await? {
        match frame {
            ServerFrame::Output(bytes) => output.extend_from_slice(&bytes),
            ServerFrame::Exit(code) => {
                exit = Some(code);
                break;
            }
            ServerFrame::Error(error) => bail!("legacy shell/0 server error: {error}"),
            ServerFrame::Status(status) => {
                bail!("legacy shell/0 unexpectedly emitted resumable status: {status}")
            }
        }
    }

    assert_eq!(exit, Some(0));
    assert!(
        String::from_utf8_lossy(&output).contains("new-server-shell-zero-ok"),
        "output was: {}",
        String::from_utf8_lossy(&output)
    );

    connection.close(0u32.into(), b"done");
    legacy_client.close().await;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumable_shell_one_survives_transport_drop() -> Result<()> {
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start_with_options(server_home.clone(), true).await?;
    let client = FabricNode::start(client_home.clone()).await?;
    trust_peer(
        &server_home,
        &server,
        client.id(),
        Some("client"),
        Some(client.addr()),
    )
    .await?;
    trust_peer(
        &client_home,
        &client,
        server.id(),
        Some("server"),
        Some(server.addr()),
    )
    .await?;

    let mut shell_child = tokio::process::Command::new(fabric_bin())
        .arg("--home")
        .arg(client_home.root())
        .arg("shell")
        .arg("server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Reap the child on any abnormal exit. A panicking assertion unwinds
        // past the explicit kill, and a leaked `fabric shell` would keep a
        // tunnel session alive on the server for the rest of the run.
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn resumable fabric shell")?;
    let mut stdin = shell_child.stdin.take().context("shell stdin missing")?;
    let mut stdout = shell_child.stdout.take().context("shell stdout missing")?;

    stdin.write_all(b"printf '%s-%s\\n' before drop\n").await?;
    read_until_marker(&mut stdout, b"before-drop").await?;

    let blocked = fabric_output(&server_home, &["debug", "block-tunnels"])?;
    assert_success(&blocked, "block shell tunnels");
    let dropped = fabric_output(&server_home, &["debug", "drop-tunnels"])?;
    assert_success(&dropped, "drop shell tunnel connection");
    stdin.write_all(b"printf '%s-%s\\n' during drop\n").await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let unblocked = fabric_output(&server_home, &["debug", "unblock-tunnels"])?;
    assert_success(&unblocked, "unblock shell tunnels");

    read_until_marker(&mut stdout, b"during-drop").await?;
    stdin
        .write_all(b"printf '%s-%s\\n' after drop; exit 0\n")
        .await?;
    read_until_marker(&mut stdout, b"after-drop").await?;
    drop(stdin);

    let status = tokio::time::timeout(Duration::from_secs(30), shell_child.wait())
        .await
        .context("resumable shell did not exit")??;
    assert_eq!(status.code(), Some(0));

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shell_past_detached_ttl_reports_the_session_is_gone() -> Result<()> {
    // Issue #21, the daily-use question: a MacBook sleeps longer than the 60s
    // detached TTL, now 15 minutes, the server reaps the PTY, and the client
    // wakes up. This test forces expiry rather than waiting for it, so the
    // retention value does not affect what it proves.
    //
    // Expiry has to be forced deterministically, or this test proves nothing,
    // and two obvious ways to force it do not work. Dropping the tunnel and
    // reaping shortly after does not: `debug block-tunnels` is only consulted
    // on the generic exposure accept path, not on the builtin resumable-shell
    // one, so the client reattaches inside the gap and the reap skips a session
    // that still has an attach. SIGSTOP on the `fabric shell` process does not
    // either: the tunnel client lives in the local daemon, not in the CLI, so
    // freezing the CLI leaves the daemon reconnecting and resuming on its own.
    //
    // Restarting the server daemon is deterministic and needs no timing at all.
    // The session store is in memory, so the restarted daemon cannot know any
    // session id, which is the same rejection an expired session produces and
    // the same thing a laptop finds after sleeping past the TTL.
    //
    // What this pins is that the client treats that rejection as terminal. It
    // does not silently attach to a fresh shell, which would let someone type
    // into a session that is not theirs, and it does not retry a session the
    // server has already refused, which would hang with no error and no exit.
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start_with_options(server_home.clone(), true).await?;
    let client = FabricNode::start(client_home.clone()).await?;
    trust_peer(
        &server_home,
        &server,
        client.id(),
        Some("client"),
        Some(client.addr()),
    )
    .await?;
    trust_peer(
        &client_home,
        &client,
        server.id(),
        Some("server"),
        Some(server.addr()),
    )
    .await?;

    let mut shell_child = tokio::process::Command::new(fabric_bin())
        .arg("--home")
        .arg(client_home.root())
        .arg("shell")
        .arg("server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Reap the child on any abnormal exit. A panicking assertion unwinds
        // past the explicit kill, and a leaked `fabric shell` would keep a
        // tunnel session alive on the server for the rest of the run.
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn resumable fabric shell")?;
    let mut stdin = shell_child.stdin.take().context("shell stdin missing")?;
    let mut stdout = shell_child.stdout.take().context("shell stdout missing")?;

    // Mark the session, then prove the marker is unique to this PTY.
    stdin
        .write_all(b"MARK=original; printf '%s-%s\n' before sleep\n")
        .await?;
    read_until_marker(&mut stdout, b"before-sleep").await?;

    // Lose the session for real: the restarted daemon keeps its identity and
    // its allow-list, and has no memory of any session.
    server.shutdown().await?;
    let server = FabricNode::start_with_options(server_home.clone(), true).await?;
    trust_peer(
        &client_home,
        &client,
        server.id(),
        Some("server"),
        Some(server.addr()),
    )
    .await?;

    let mut stderr = shell_child.stderr.take().context("shell stderr missing")?;
    let reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(30), stderr.read_to_end(&mut buf)).await;
        buf
    });
    let waited = tokio::time::timeout(Duration::from_secs(30), shell_child.wait()).await;
    let reported = String::from_utf8_lossy(&reader.await.unwrap_or_default()).into_owned();

    let Ok(status) = waited else {
        let _ = shell_child.kill().await;
        let _ = shell_child.wait().await;
        bail!("shell never exited after its session was lost; it reported:\n{reported}");
    };
    let status = status.context("failed to wait for shell")?;

    // A resume cannot legitimately succeed against a daemon that just lost its
    // session store, so seeing one means the test measured the wrong thing.
    assert!(
        !reported.contains("session resumed"),
        "session was not actually lost; the client resumed it:\n{reported}"
    );
    assert_ne!(
        status.code(),
        Some(0),
        "shell exited cleanly despite losing its remote session:\n{reported}"
    );
    // The message has to name the session and say it is not coming back.
    // "reconnecting" with no resolution is the failure this pins against.
    assert!(
        reported.contains("remote shell could not resume") && reported.contains("expired"),
        "shell exited without reporting that the remote session is gone:\n{reported}"
    );

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shell_sigterm_restores_exact_terminal_mode() -> Result<()> {
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start_with_options(server_home.clone(), true).await?;
    let client = FabricNode::start(client_home.clone()).await?;
    trust_peer(
        &server_home,
        &server,
        client.id(),
        Some("client"),
        Some(client.addr()),
    )
    .await?;
    trust_peer(
        &client_home,
        &client,
        server.id(),
        Some("server"),
        Some(server.addr()),
    )
    .await?;

    let pair = native_pty_system().openpty(PtySize::default())?;
    let terminal_fd = pair
        .master
        .as_raw_fd()
        .context("pseudo-terminal did not expose a raw fd")?;
    let before = terminal_snapshot(terminal_fd)?;
    let mut command = CommandBuilder::new(fabric_bin());
    command.arg("--home");
    command.arg(client_home.root());
    command.arg("shell");
    command.arg("server");
    let mut child = pair.slave.spawn_command(command)?;
    drop(pair.slave);

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if terminal_snapshot(terminal_fd)? != before {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("fabric shell never entered raw terminal mode")??;

    let pid = child
        .process_id()
        .context("shell child has no process id")?;
    let killed = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    if killed == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    tokio::time::timeout(
        Duration::from_secs(30),
        tokio::task::spawn_blocking(move || child.wait()),
    )
    .await
    .context("fabric shell did not terminate after SIGTERM")???;

    let after = terminal_snapshot(terminal_fd)?;
    assert_eq!(
        after, before,
        "fabric shell did not restore the exact pre-existing terminal mode"
    );

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
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
    wait_for_cli_status(&node_b_home, false).await?;

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
    wait_for_cli_status(&node_b_home, false).await?;

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
async fn remote_shell_exposes_fabric_marker_env() -> Result<()> {
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
    wait_for_cli_status(&node_b_home, false).await?;

    // node_b shells into node_a, so inside node_a's shell FABRIC_SHELL=1 and
    // FABRIC_PEER is node_b's NodeID (the connecting peer).
    let output = run_shell(
        &node_b_home,
        "node-a",
        "printf 'MARK=%s:%s\\n' \"$FABRIC_SHELL\" \"$FABRIC_PEER\"; exit 0\n",
    )?;
    assert_success(&output, "marker-env shell");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected = format!("MARK=1:{}", node_b.id());
    assert!(
        stdout.contains(&expected),
        "expected {expected}, stdout was: {stdout}"
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
    wait_for_cli_status(&node_b_home, false).await?;

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
    wait_for_cli_status(&node_c_home, false).await?;

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

async fn read_until_marker<R: AsyncRead + Unpin>(read: &mut R, marker: &[u8]) -> Result<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut output = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let count = read.read(&mut chunk).await?;
            if count == 0 {
                bail!(
                    "shell output closed before marker {:?}; output={}",
                    String::from_utf8_lossy(marker),
                    String::from_utf8_lossy(&output)
                );
            }
            output.extend_from_slice(&chunk[..count]);
            if output.windows(marker.len()).any(|window| window == marker) {
                return Ok(output);
            }
        }
    })
    .await
    .context("timed out waiting for shell output marker")?
}

#[cfg(unix)]
#[derive(Debug, PartialEq, Eq)]
struct TerminalSnapshot {
    input: libc::tcflag_t,
    output: libc::tcflag_t,
    control: libc::tcflag_t,
    local: libc::tcflag_t,
    characters: Vec<libc::cc_t>,
    input_speed: libc::speed_t,
    output_speed: libc::speed_t,
}

#[cfg(unix)]
fn terminal_snapshot(fd: std::os::fd::RawFd) -> Result<TerminalSnapshot> {
    let mut termios = std::mem::MaybeUninit::<libc::termios>::uninit();
    let result = unsafe { libc::tcgetattr(fd, termios.as_mut_ptr()) };
    if result == -1 {
        return Err(std::io::Error::last_os_error().into());
    }
    let termios = unsafe { termios.assume_init() };
    Ok(TerminalSnapshot {
        input: termios.c_iflag,
        output: termios.c_oflag,
        control: termios.c_cflag,
        local: termios.c_lflag,
        characters: termios.c_cc.to_vec(),
        input_speed: unsafe { libc::cfgetispeed(&termios) },
        output_speed: unsafe { libc::cfgetospeed(&termios) },
    })
}

fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn sh_quote_path(path: &Path) -> String {
    sh_quote(&path.display().to_string())
}
