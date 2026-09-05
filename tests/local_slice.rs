use std::{
    ffi::OsStr,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fabric::{
    config::{FabricHome, GitAccess, PeerBook, generate_identity_file},
    control::{ControlRequest, ControlResponse},
    daemon::{FabricNode, send_control},
};

#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
    task::JoinHandle,
};

// These tests start real iroh endpoints and daemon tasks; keep the default
// test runner from exercising that transport stack concurrently.
// Each test still creates its Tokio runtime before acquiring the guard, so keep
// worker counts low to avoid starving the one active transport test.
static LOCAL_SLICE_LOCKED: AtomicBool = AtomicBool::new(false);
const FABRIC_COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
const LOCAL_IO_TIMEOUT: Duration = Duration::from_secs(60);
const LARGE_PAYLOAD_TIMEOUT: Duration = Duration::from_secs(60);
const LOCAL_SLICE_SETTLE: Duration = Duration::from_millis(500);

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn git_clone_push_and_revocation_use_exact_repository_grants() -> Result<()> {
    let _guard = local_slice_guard().await;
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let source_dir = TempDir::new()?;
    let clone_root = TempDir::new()?;
    let helper_dir = TempDir::new()?;
    let remote = server_dir.path().join("mandat.git");
    let source = source_dir.path();
    let clone = clone_root.path().join("clone");

    git_ok(None, &["init", "--bare", remote.to_str().unwrap()])?;
    git_ok(Some(&remote), &["symbolic-ref", "HEAD", "refs/heads/main"])?;
    git_ok(Some(source), &["init"])?;
    git_ok(Some(source), &["config", "user.name", "Fabric Test"])?;
    git_ok(
        Some(source),
        &["config", "user.email", "fabric@example.invalid"],
    )?;
    let large = (0..(256 * 1024))
        .map(|offset| (offset % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(source.join("large.bin"), &large)?;
    fs::write(source.join("README.md"), b"first\n")?;
    git_ok(Some(source), &["add", "."])?;
    git_ok(Some(source), &["commit", "-m", "First"])?;
    git_ok(
        Some(source),
        &["remote", "add", "origin", remote.to_str().unwrap()],
    )?;
    git_ok(Some(source), &["push", "origin", "HEAD:refs/heads/main"])?;
    let initial = git_ok(Some(source), &["rev-parse", "HEAD"])?;

    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start(server_home.clone()).await?;
    let client = FabricNode::start(client_home.clone()).await?;

    let mut server_book = PeerBook::load(&server_home)?;
    server_book.add_with_allow(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        Some(vec!["echo".into()]),
    );
    server_book.share_git_remote("mandat", remote.clone())?;
    server_book.grant_git_remote("mandat", "client", GitAccess::Read)?;
    server_book.save(&server_home)?;
    server.state().reload_peers().await?;

    let mut client_book = PeerBook::load(&client_home)?;
    client_book.add_with_allow(
        server.id(),
        Some("server".into()),
        Some(server.addr()),
        Some(vec!["echo".into()]),
    );
    client_book.save(&client_home)?;
    client.state().reload_peers().await?;

    assert_eq!(client.ping("server").await?.bytes, 32);

    let helper = helper_dir.path().join("git-remote-fabric");
    symlink(fabric_bin(), &helper)?;
    let mut paths = vec![helper_dir.path().to_path_buf()];
    if let Some(inherited) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&inherited));
    }
    let path = std::env::join_paths(paths)?;
    let fabric_env = [
        ("FABRIC_HOME", client_home.root().as_os_str()),
        ("PATH", path.as_os_str()),
        ("GIT_TERMINAL_PROMPT", OsStr::new("0")),
    ];

    let cloned = run_git_process(
        None,
        &["clone", "fabric://server/mandat", clone.to_str().unwrap()],
        &fabric_env,
    )?;
    assert_process_ok("fabric clone", &cloned)?;
    assert_eq!(git_ok(Some(&clone), &["rev-parse", "HEAD"])?, initial);
    assert_eq!(fs::read(clone.join("large.bin"))?, large);
    assert_eq!(
        client.state().peer_connection_count().await,
        1,
        "echo and Git must share one generation-aware peer connection"
    );

    git_ok(Some(&clone), &["config", "user.name", "Fabric Test"])?;
    git_ok(
        Some(&clone),
        &["config", "user.email", "fabric@example.invalid"],
    )?;
    fs::write(clone.join("README.md"), b"second\n")?;
    git_ok(Some(&clone), &["add", "README.md"])?;
    git_ok(Some(&clone), &["commit", "-m", "Second"])?;
    let second = git_ok(Some(&clone), &["rev-parse", "HEAD"])?;
    let marker = remote.join("fabric-hook-ran");
    let hook = remote.join("hooks/pre-receive");
    fs::write(
        &hook,
        b"#!/bin/sh\ntest \"$FABRIC_GIT_REMOTE\" = mandat || exit 90\ntest \"$FABRIC_GIT_ACCESS\" = write || exit 91\ntest -n \"$FABRIC_PEER\" || exit 92\nprintf ran > \"$GIT_DIR/fabric-hook-ran\"\n",
    )?;
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755))?;

    let denied = run_git_process(
        Some(&clone),
        &["push", "origin", "HEAD:refs/heads/main"],
        &fabric_env,
    )?;
    assert!(!denied.status.success(), "a read grant allowed a push");
    let denial = String::from_utf8_lossy(&denied.stderr);
    assert!(
        denial.contains("did not grant write access"),
        "the denial was not actionable: {denial}"
    );
    assert_eq!(git_bare_head(&remote)?, initial);
    assert!(!marker.exists(), "a denied push started Git or its hook");

    let mut server_book = PeerBook::load(&server_home)?;
    server_book.grant_git_remote("mandat", "client", GitAccess::Write)?;
    server_book.save(&server_home)?;
    server.state().reload_peers().await?;
    let pushed = run_git_process(
        Some(&clone),
        &["push", "origin", "HEAD:refs/heads/main"],
        &fabric_env,
    )?;
    assert_process_ok("granted fabric push", &pushed)?;
    assert_eq!(git_bare_head(&remote)?, second);
    assert_eq!(fs::read(&marker)?, b"ran");

    let mut server_book = PeerBook::load(&server_home)?;
    server_book.revoke_git_remote("mandat", "client", GitAccess::Write)?;
    server_book.save(&server_home)?;
    server.state().reload_peers().await?;
    fs::write(clone.join("README.md"), b"third\n")?;
    git_ok(Some(&clone), &["add", "README.md"])?;
    git_ok(Some(&clone), &["commit", "-m", "Third"])?;
    let revoked = run_git_process(
        Some(&clone),
        &["push", "origin", "HEAD:refs/heads/main"],
        &fabric_env,
    )?;
    assert!(
        !revoked.status.success(),
        "a revoked write grant still pushed"
    );
    assert_eq!(git_bare_head(&remote)?, second);

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

struct LocalSliceGuard;

impl Drop for LocalSliceGuard {
    fn drop(&mut self) {
        LOCAL_SLICE_LOCKED.store(false, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_expose_dial_round_trips_and_acl_rejects_unknown_node() -> Result<()> {
    let _guard = local_slice_guard().await;
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_c_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());
    let node_c_home = FabricHome::new(node_c_dir.path());

    let node_a = FabricNode::start(node_a_home.clone()).await?;
    let node_b = FabricNode::start(node_b_home.clone()).await?;
    let node_c = FabricNode::start(node_c_home.clone()).await?;

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
    trust_peer(
        &node_c_home,
        &node_c,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    let echo_socket = node_a_dir.path().join("echo.sock");
    let echo_hits = Arc::new(AtomicUsize::new(0));
    let echo_task = spawn_echo_service(&echo_socket, echo_hits.clone()).await?;
    node_a.expose("pty-view", echo_socket).await?;

    let dial_socket = node_b.dial("node-a", "pty-view").await?;
    let payload = b"pty-view bytes through fabric";
    let response = unix_round_trip(&dial_socket, payload).await?;
    assert_eq!(response, payload);
    assert_eq!(echo_hits.load(Ordering::SeqCst), 1);

    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);

    let unauthorized_socket = node_c.dial("node-a", "pty-view").await?;
    let unauthorized = tokio::time::timeout(
        Duration::from_secs(5),
        unix_round_trip(&unauthorized_socket, b"not trusted"),
    )
    .await;
    assert!(
        !matches!(unauthorized, Ok(Ok(_))),
        "unauthorized node unexpectedly reached the exposed service"
    );
    assert_eq!(
        echo_hits.load(Ordering::SeqCst),
        1,
        "unauthorized node reached node A's local service"
    );

    let rejected_ping = node_c.ping("node-a").await;
    assert!(
        rejected_ping.is_err(),
        "unauthorized node unexpectedly reached the built-in echo"
    );

    trust_peer(
        &node_a_home,
        &node_a,
        node_c.id(),
        Some("node-c"),
        Some(node_c.addr()),
    )
    .await?;
    let trusted_later_ping = node_c.ping("node-a").await?;
    assert_eq!(trusted_later_ping.bytes, 32);

    echo_task.abort();
    node_c.shutdown().await?;
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_tunnel_survives_transport_reconnect_without_reopening_local_service() -> Result<()>
{
    let _guard = local_slice_guard().await;
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

    let echo_socket = node_a_dir.path().join("echo.sock");
    let echo_hits = Arc::new(AtomicUsize::new(0));
    let echo_task = spawn_echo_service(&echo_socket, echo_hits.clone()).await?;
    node_a.expose("pty-view", echo_socket).await?;

    let dial_socket = node_b.dial("node-a", "pty-view").await?;
    let mut stream = UnixStream::connect(&dial_socket).await?;

    stream_round_trip(&mut stream, b"before-drop").await?;

    run_fabric(&node_a_home, &["debug", "block-tunnels"])?;
    run_fabric(&node_a_home, &["debug", "drop-tunnels"])?;
    stream.write_all(b"during-drop").await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    run_fabric(&node_a_home, &["debug", "unblock-tunnels"])?;

    tokio::time::timeout(LOCAL_IO_TIMEOUT, read_expected(&mut stream, b"during-drop"))
        .await
        .context("generic reconnect payload timed out")??;
    stream_round_trip(&mut stream, b"after-drop").await?;
    assert_eq!(
        echo_hits.load(Ordering::SeqCst),
        1,
        "reconnect should keep the exposed Unix service connection alive"
    );

    echo_task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_tunnel_survives_client_endpoint_recycle_without_process_restart() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let node_b_id = node_b.id();
    let echo_socket = node_a_dir.path().join("echo.sock");
    let echo_hits = Arc::new(AtomicUsize::new(0));
    let echo_task = spawn_echo_service(&echo_socket, echo_hits.clone()).await?;
    node_a.expose("pty-view", echo_socket).await?;

    let dial_socket = node_b.dial("node-a", "pty-view").await?;
    let mut stream = UnixStream::connect(&dial_socket).await?;
    stream_round_trip(&mut stream, b"before-recycle").await?;

    send_control(&node_b_home, ControlRequest::RecycleEndpoint).await?;
    assert_eq!(
        node_b.id(),
        node_b_id,
        "endpoint recycle must preserve NodeID"
    );

    stream.write_all(b"during-recycle").await?;
    tokio::time::timeout(
        LOCAL_IO_TIMEOUT,
        read_expected(&mut stream, b"during-recycle"),
    )
    .await
    .context("endpoint recycle reconnect payload timed out")??;
    stream_round_trip(&mut stream, b"after-recycle").await?;
    assert_eq!(
        echo_hits.load(Ordering::SeqCst),
        1,
        "endpoint recycle should keep the exposed Unix service connection alive"
    );

    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);

    echo_task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_tunnel_reap_closes_existing_client_socket() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let echo_socket = node_a_dir.path().join("echo.sock");
    let echo_hits = Arc::new(AtomicUsize::new(0));
    let echo_task = spawn_echo_service(&echo_socket, echo_hits.clone()).await?;
    node_a.expose("pty-view", echo_socket).await?;

    let dial_socket = node_b.dial("node-a", "pty-view").await?;
    let mut stream = UnixStream::connect(&dial_socket).await?;
    stream_round_trip(&mut stream, b"before-reap").await?;

    run_fabric(&node_a_home, &["debug", "block-tunnels"])?;
    run_fabric(&node_a_home, &["debug", "drop-tunnels"])?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    run_fabric(&node_a_home, &["debug", "reap-tunnels", "--ttl-ms", "0"])?;
    run_fabric(&node_a_home, &["debug", "unblock-tunnels"])?;

    let mut buf = [0; 1];
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
        .await
        .context("reaped tunnel did not close client socket")??;
    assert_eq!(read, 0, "reaped tunnel should close the client socket");
    assert_eq!(
        echo_hits.load(Ordering::SeqCst),
        1,
        "expired reconnect should not open a replacement service connection"
    );

    echo_task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_expose_round_trips_stdio_handler() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    run_fabric(
        &node_a_home,
        &["expose", "stdio-cat", "--exec", "--", "/bin/cat"],
    )?;

    let dial_socket = node_b.dial("node-a", "stdio-cat").await?;
    let payload = b"stdio bytes through exec expose";
    let response = unix_round_trip(&dial_socket, payload).await?;
    assert_eq!(response, payload);

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_expose_reconnect_keeps_child_bound_to_tunnel_session() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let marker = node_a_dir.path().join("exec-spawns.txt");
    node_a
        .expose_exec(
            "stdio-cat",
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "printf spawn >> \"$1\"; exec /bin/cat".to_string(),
                "fabric-test".to_string(),
                marker.display().to_string(),
            ],
        )
        .await?;

    let dial_socket = node_b.dial("node-a", "stdio-cat").await?;
    let mut stream = UnixStream::connect(&dial_socket).await?;

    stream_round_trip(&mut stream, b"before-drop").await?;
    assert_eq!(fs::read_to_string(&marker)?, "spawn");

    run_fabric(&node_a_home, &["debug", "block-tunnels"])?;
    run_fabric(&node_a_home, &["debug", "drop-tunnels"])?;
    stream.write_all(b"during-drop").await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    run_fabric(&node_a_home, &["debug", "unblock-tunnels"])?;

    tokio::time::timeout(LOCAL_IO_TIMEOUT, read_expected(&mut stream, b"during-drop"))
        .await
        .context("exec reconnect payload timed out")??;
    stream_round_trip(&mut stream, b"after-drop").await?;
    assert_eq!(
        fs::read_to_string(&marker)?,
        "spawn",
        "reconnect should reuse the existing exec child"
    );

    drop(stream);
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_expose_child_exits_on_stdin_eof() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let pid_file = node_a_dir.path().join("exec-child.pid");
    node_a
        .expose_exec("stdio-cat", pid_cat_argv(&pid_file))
        .await?;

    let dial_socket = node_b.dial("node-a", "stdio-cat").await?;
    let mut stream = UnixStream::connect(&dial_socket).await?;
    stream_round_trip(&mut stream, b"before-eof").await?;
    let pid = read_pid(&pid_file)?;
    assert!(process_running(pid), "exec child should be running");

    drop(stream);
    wait_for_process_exit(pid).await?;

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_expose_reaps_child_on_session_ttl_expiry() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let pid_file = node_a_dir.path().join("exec-child.pid");
    node_a
        .expose_exec("stdio-cat", pid_cat_argv(&pid_file))
        .await?;

    let dial_socket = node_b.dial("node-a", "stdio-cat").await?;
    let mut stream = UnixStream::connect(&dial_socket).await?;
    stream_round_trip(&mut stream, b"before-drop").await?;
    let pid = read_pid(&pid_file)?;
    assert!(process_running(pid), "exec child should be running");

    run_fabric(&node_a_home, &["debug", "block-tunnels"])?;
    run_fabric(&node_a_home, &["debug", "drop-tunnels"])?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    run_fabric(&node_a_home, &["debug", "reap-tunnels", "--ttl-ms", "0"])?;
    wait_for_process_exit(pid).await?;

    drop(stream);
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_expose_enforces_per_exposure_child_limit() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let marker = node_a_dir.path().join("exec-spawns.txt");
    let ready = node_a_dir.path().join("exec-ready.txt");
    let release = node_a_dir.path().join("exec-release");
    node_a
        .expose_exec_with_limit(
            "limited-cat",
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "printf spawn >> \"$1\"; printf ready > \"$2\"; while [ ! -e \"$3\" ]; do sleep 0.1; done; exec /bin/cat".to_string(),
                "fabric-test".to_string(),
                marker.display().to_string(),
                ready.display().to_string(),
                release.display().to_string(),
            ],
            1,
        )
        .await?;

    let dial_socket = node_b.dial("node-a", "limited-cat").await?;
    let mut first = UnixStream::connect(&dial_socket).await?;
    wait_for_file_contents(&ready, "ready").await?;
    assert_eq!(fs::read_to_string(&marker)?, "spawn");

    assert_tunnel_rejects_quickly(&dial_socket, b"second-child").await?;
    assert_eq!(
        fs::read_to_string(&marker)?,
        "spawn",
        "limit rejection must not spawn a second child"
    );

    fs::write(&release, "go")?;
    stream_round_trip(&mut first, b"first-still-alive").await?;

    drop(first);
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_expose_spawn_failure_closes_local_stream_and_daemon_survives() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    node_a
        .expose_exec(
            "bad-exec",
            vec!["/definitely/not/a/fabric-test-command".to_string()],
        )
        .await?;

    let dial_socket = node_b.dial("node-a", "bad-exec").await?;
    assert_tunnel_rejects_quickly(&dial_socket, b"will-not-run").await?;

    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);

    fs::write(
        node_a_home.peers_path(),
        "[[peers]]\nid = \"not-a-node-id\"\n",
    )?;
    assert!(
        run_fabric(&node_a_home, &["reload-peers"]).is_err(),
        "invalid peers.toml unexpectedly reloaded"
    );
    let ping = node_b.ping("node-a").await?;
    assert_eq!(
        ping.bytes, 32,
        "failed reload should preserve the previously loaded allow-list"
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_expose_streams_payload_larger_than_tunnel_buffer() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    node_a
        .expose_exec("stdio-cat", vec!["/bin/cat".to_string()])
        .await?;

    let dial_socket = node_b.dial("node-a", "stdio-cat").await?;
    let stream = UnixStream::connect(&dial_socket).await?;
    let payload = vec![42; 5 * 1024 * 1024];
    let mut response = vec![0; payload.len()];
    let (mut read, mut write) = stream.into_split();
    let writer = async {
        write.write_all(&payload).await?;
        write.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    };
    let reader = async {
        read.read_exact(&mut response).await?;
        Ok::<(), anyhow::Error>(())
    };
    tokio::time::timeout(LARGE_PAYLOAD_TIMEOUT, async {
        tokio::try_join!(writer, reader)?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("large exec payload round trip timed out")??;
    assert_eq!(response, payload);

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_peer_and_daemon_configs_restore_on_restart() -> Result<()> {
    let _guard = local_slice_guard().await;
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());

    set_machine_services(&node_a_home, true, false)?;
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

    node_a
        .expose_exec("stdio-cat", vec!["/bin/cat".to_string()])
        .await?;
    assert_status_exposes(&node_a_home, "stdio-cat").await?;
    assert_status_shell_allowed(&node_a_home).await?;
    assert!(
        node_a_home.config_path().exists(),
        "daemon config should be persisted to config.toml"
    );
    let raw_config = fs::read_to_string(node_a_home.config_path())?;
    assert!(!raw_config.contains("allow_shell"));
    assert!(raw_config.contains("stdio-cat"));
    assert!(!raw_config.contains("node-b"));
    let raw_peers = fs::read_to_string(node_a_home.peers_path())?;
    assert!(raw_peers.contains("allow_shell = true"));
    assert!(raw_peers.contains("node-b"));

    node_a.shutdown().await?;
    let node_a = FabricNode::start(node_a_home.clone()).await?;
    trust_peer(
        &node_b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    assert_status_shell_allowed(&node_a_home).await?;
    assert_status_exposes(&node_a_home, "stdio-cat").await?;
    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);
    let dial_socket = node_b.dial("node-a", "stdio-cat").await?;
    let response = unix_round_trip(&dial_socket, b"after-restart").await?;
    assert_eq!(response, b"after-restart");

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unexpose_clears_persisted_config_and_restart_does_not_restore() -> Result<()> {
    let _guard = local_slice_guard().await;
    let node_a_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());

    let node_a = FabricNode::start(node_a_home.clone()).await?;
    node_a
        .expose_exec("stdio-cat", vec!["/bin/cat".to_string()])
        .await?;
    assert_status_exposes(&node_a_home, "stdio-cat").await?;
    assert!(fs::read_to_string(node_a_home.config_path())?.contains("stdio-cat"));

    run_fabric(&node_a_home, &["unexpose", "stdio-cat"])?;
    assert_status_does_not_expose(&node_a_home, "stdio-cat").await?;
    assert!(
        !fs::read_to_string(node_a_home.config_path())?.contains("stdio-cat"),
        "unexpose should remove the durable config entry"
    );

    node_a.shutdown().await?;
    let node_a = FabricNode::start(node_a_home.clone()).await?;
    assert_status_does_not_expose(&node_a_home, "stdio-cat").await?;

    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_expose_dial_listener_round_trips_and_reconnects() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let (tcp_echo_addr, echo_hits, echo_task) = spawn_tcp_echo_service().await?;
    run_fabric(
        &node_a_home,
        &["expose", "tcp-echo", "--tcp", tcp_echo_addr.as_str()],
    )?;
    let local_addr = run_fabric(
        &node_b_home,
        &["dial", "node-a", "tcp-echo", "--tcp", "127.0.0.1:0"],
    )?;
    let mut stream = TcpStream::connect(&local_addr).await?;

    tcp_stream_round_trip(&mut stream, b"before-drop").await?;

    run_fabric(&node_a_home, &["debug", "block-tunnels"])?;
    run_fabric(&node_a_home, &["debug", "drop-tunnels"])?;
    stream.write_all(b"during-drop").await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    run_fabric(&node_a_home, &["debug", "unblock-tunnels"])?;

    tokio::time::timeout(
        LOCAL_IO_TIMEOUT,
        read_expected_tcp(&mut stream, b"during-drop"),
    )
    .await
    .context("tcp reconnect payload timed out")??;
    tcp_stream_round_trip(&mut stream, b"after-drop").await?;
    assert_eq!(
        echo_hits.load(Ordering::SeqCst),
        1,
        "reconnect should keep the exposed TCP connection alive"
    );

    drop(stream);
    echo_task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_tcp_expose_survives_daemon_restart() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let (tcp_echo_addr, _echo_hits, echo_task) = spawn_tcp_echo_service().await?;
    node_a.expose_tcp("tcp-echo", tcp_echo_addr).await?;
    assert_status_exposes(&node_a_home, "tcp-echo").await?;

    node_a.shutdown().await?;
    let node_a = FabricNode::start(node_a_home.clone()).await?;
    trust_peer(
        &node_b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    assert_status_exposes(&node_a_home, "tcp-echo").await?;
    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);
    let local_addr = node_b
        .dial_tcp("node-a", "tcp-echo", "127.0.0.1:0".to_string())
        .await?;
    let response = tcp_round_trip(&local_addr, b"tcp-after-restart").await?;
    assert_eq!(response, b"tcp-after-restart");

    echo_task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_round_trips_builtin_echo() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let before = node_a.state().builtin_echo_hits();
    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);
    assert_eq!(node_a.state().builtin_echo_hits(), before + 1);

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_peer_traffic_converges_to_one_shared_connection() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let (from_a, from_b) = tokio::join!(node_a.ping("node-b"), node_b.ping("node-a"));
    assert_eq!(from_a?.bytes, 32);
    assert_eq!(from_b?.bytes, 32);
    assert_eq!(node_a.state().peer_connection_count().await, 1);
    assert_eq!(node_b.state().peer_connection_count().await, 1);

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_acl_rejects_untrusted_before_echo_handler() -> Result<()> {
    let _guard = local_slice_guard().await;
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_c_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());
    let node_c_home = FabricHome::new(node_c_dir.path());

    let node_a = FabricNode::start(node_a_home.clone()).await?;
    let node_b = FabricNode::start(node_b_home.clone()).await?;
    let node_c = FabricNode::start(node_c_home.clone()).await?;

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
    trust_peer(
        &node_c_home,
        &node_c,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    let trusted_ping = node_b.ping("node-a").await?;
    assert_eq!(trusted_ping.bytes, 32);
    let after_trusted = node_a.state().builtin_echo_hits();

    let rejected_ping = node_c.ping("node-a").await;
    assert!(
        rejected_ping.is_err(),
        "untrusted node unexpectedly reached built-in echo"
    );
    assert_eq!(
        node_a.state().builtin_echo_hits(),
        after_trusted,
        "untrusted ping reached node A's built-in echo handler"
    );

    node_c.shutdown().await?;
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reports_peer_reachability() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    let response = wait_for_reachability_status(&node_b_home).await?;
    let ControlResponse::ReachabilityStatus { version, peers, .. } = response else {
        panic!("unexpected response: {response:?}");
    };
    assert_eq!(version, fabric::version_string());
    let peer = peers
        .iter()
        .find(|peer| peer.name.as_deref() == Some("node-a"))
        .expect("node-a peer status missing");
    assert!(peer.reachable, "node-a should be reachable: {peer:?}");
    assert_eq!(peer.bytes, Some(32));
    assert!(peer.round_trip_micros.is_some());

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declarative_peer_config_is_loaded_on_start() -> Result<()> {
    let _guard = local_slice_guard().await;
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());

    let node_b_id = generate_identity_file(&node_b_home.identity_path())?;
    fs::write(
        node_a_home.peers_path(),
        format!("[[peers]]\nid = \"{node_b_id}\"\nname = \"node-b\"\nallow = [\"echo\"]\n"),
    )?;

    let node_a = FabricNode::start(node_a_home.clone()).await?;

    let mut node_b_peers = PeerBook::default();
    node_b_peers.add_with_allow(
        node_a.id(),
        Some("node-a".to_string()),
        Some(node_a.addr()),
        Some(vec!["echo".to_string()]),
    );
    node_b_peers.save(&node_b_home)?;

    let node_b = FabricNode::start(node_b_home).await?;
    assert_eq!(node_b.id(), node_b_id);

    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declarative_peer_config_can_be_reloaded_without_restart() -> Result<()> {
    let _guard = local_slice_guard().await;
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());

    let node_a = FabricNode::start(node_a_home.clone()).await?;
    let node_b = FabricNode::start(node_b_home.clone()).await?;
    trust_peer(
        &node_b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    assert!(
        node_b.ping("node-a").await.is_err(),
        "node A unexpectedly trusted node B before peers.toml reload"
    );
    fs::write(
        node_a_home.peers_path(),
        format!(
            "allow_shell = true\nallow_exec = false\n\n[[peers]]\nid = \"{}\"\nname = \"node-b\"\nallow = [\"echo\"]\n",
            node_b.id()
        ),
    )?;
    assert_eq!(run_fabric(&node_a_home, &["reload-peers"])?, "reloaded");

    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// An ACL refusal happens before the remote exec handler starts. The local
/// daemon must still send a framed failure to the CLI. Otherwise `fabric exec`
/// exits with no output and a person cannot tell a refusal from a broken dial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_names_the_peer_and_service_when_the_peer_acl_refuses_it() -> Result<()> {
    let _guard = local_slice_guard().await;
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start(server_home.clone()).await?;
    let client = FabricNode::start(client_home.clone()).await?;

    let mut server_book = PeerBook::load(&server_home)?;
    server_book.set_allow_exec(true);
    server_book.add_with_allow(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        Some(vec!["echo".into()]),
    );
    server_book.save(&server_home)?;
    server.state().reload_peers().await?;
    trust_peer(
        &client_home,
        &client,
        server.id(),
        Some("server"),
        Some(server.addr()),
    )
    .await?;
    assert_eq!(client.ping("server").await?.bytes, 32);

    let output = tokio::time::timeout(
        FABRIC_COMMAND_TIMEOUT,
        tokio::process::Command::new(fabric_bin())
            .arg("--home")
            .arg(client_home.root())
            .args(["exec", "server", "--", "fabric", "--version"])
            .output(),
    )
    .await
    .context("fabric exec timed out after an ACL refusal")??;
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(126), "stderr: {stderr}");
    assert!(
        stderr.contains("server"),
        "the refusal omitted the peer: {stderr:?}"
    );
    assert!(
        stderr.contains("exec"),
        "the refusal omitted the service: {stderr:?}"
    );

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_file_remains_authoritative_when_daemon_config_is_created() -> Result<()> {
    let _guard = local_slice_guard().await;
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());

    let node_b = FabricNode::start(node_b_home.clone()).await?;
    fs::write(
        node_a_home.peers_path(),
        format!(
            "allow_shell = true\n\n[[peers]]\nid = \"{}\"\nname = \"node-b\"\nallow = [\"echo\"]\n",
            node_b.id()
        ),
    )?;

    let node_a = FabricNode::start_with_options(node_a_home.clone(), true).await?;
    trust_peer(
        &node_b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    assert_status_shell_allowed(&node_a_home).await?;
    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);
    assert!(!node_a_home.config_path().exists());
    assert!(
        node_a_home.peers_path().exists(),
        "peers.toml should remain the authoritative allow-list"
    );
    assert!(fs::read_to_string(node_a_home.peers_path())?.contains("node-b"));

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// Finding 1 of the 2026-08-29 review. A dial to a peer that cannot be reached
/// held its permit for ever, and kept holding it after the consumer closed its
/// socket. With all 32 held, every `shell`, `exec` and new dial on the machine
/// waited with no error, while `status` and `ping` stayed green.
///
/// Bluey is the peer that makes this live: it is unreachable most of the time
/// by design, and anything that dials it and gives up leaves a permit behind.
///
/// CONTROL: the 32 consumers must be seen holding 32 permits before they close,
/// or "0 afterwards" proves nothing. And a fresh dial to a REAL peer must round
/// trip afterwards, because that is the symptom a person sees.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_abandoned_dial_to_an_unreachable_peer_releases_its_permit() -> Result<()> {
    let _guard = local_slice_guard().await;
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

    // A trusted peer with no address and nobody behind it: what a roaming
    // laptop looks like while it is asleep.
    let ghost = iroh::SecretKey::generate().public();
    trust_peer(&node_a_home, &node_a, ghost, Some("ghost"), None).await?;
    let ghost_socket = node_a.dial("ghost", "pty-view").await?;

    let echo_socket = node_b_dir.path().join("echo.sock");
    let echo_hits = Arc::new(AtomicUsize::new(0));
    let echo_task = spawn_echo_service(&echo_socket, echo_hits.clone()).await?;
    node_b.expose("pty-view", echo_socket).await?;
    let echo_dial = node_a.dial("node-b", "pty-view").await?;

    let state = node_a.state();
    let max = state.max_dial_handlers();
    assert_eq!(
        state.active_dial_handlers(),
        0,
        "permits in use before anything dialed"
    );

    let mut consumers = Vec::new();
    for _ in 0..max {
        consumers.push(UnixStream::connect(&ghost_socket).await?);
    }
    let held = wait_for_dial_handlers(&state, max).await;
    assert_eq!(
        held, max,
        "POSITIVE CONTROL FAILED: {max} consumers did not take {max} permits, so \
         their release below would prove nothing"
    );

    // Every consumer gives up. Nobody is waiting on any of these sessions now.
    drop(consumers);

    let released = wait_for_dial_handlers(&state, 0).await;
    assert_eq!(
        released, 0,
        "{released} dial permits are still held by sessions whose consumer closed. \
         Every shell, exec and dial on this machine now waits for a restart"
    );

    // The symptom a person sees, checked directly: a dial to a real peer works.
    let mut stream = tokio::time::timeout(LOCAL_IO_TIMEOUT, UnixStream::connect(&echo_dial))
        .await
        .context("connecting to the echo dial socket")??;
    tokio::time::timeout(
        LOCAL_IO_TIMEOUT,
        stream_round_trip(&mut stream, b"after-the-ghosts"),
    )
    .await
    .context("a dial to a reachable peer hung after abandoned dials to an unreachable one")??;

    echo_task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// Poll the permit count until it reads `want`, for up to 30 s. Returns the
/// last reading either way so the assertion can say what it saw.
async fn wait_for_dial_handlers(state: &fabric::daemon::DaemonState, want: usize) -> usize {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let now = state.active_dial_handlers();
        if now == want || Instant::now() >= deadline {
            return now;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn trust_peer(
    home: &FabricHome,
    node: &FabricNode,
    id: iroh::EndpointId,
    name: Option<&str>,
    addr: Option<iroh::EndpointAddr>,
) -> Result<()> {
    let mut peers = PeerBook::load(home)?;
    peers.add_with_allow(
        id,
        name.map(str::to_string),
        addr,
        Some(
            [
                "shell",
                "exec",
                "sync",
                "echo",
                "send-file",
                "pty-view",
                "stdio-cat",
                "limited-cat",
                "bad-exec",
                "tcp-echo",
                "web",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        ),
    );
    peers.save(home)?;
    node.state().reload_peers().await?;
    Ok(())
}

fn set_machine_services(home: &FabricHome, allow_shell: bool, allow_exec: bool) -> Result<()> {
    let mut peers = PeerBook::load(home)?;
    peers.set_allow_shell(allow_shell);
    peers.set_allow_exec(allow_exec);
    peers.save(home)
}

async fn exposed_protocols(home: &FabricHome) -> Result<Vec<String>> {
    let response = wait_for_status(home).await?;
    let ControlResponse::Status {
        exposed_protocols, ..
    } = response
    else {
        panic!("unexpected response: {response:?}");
    };
    Ok(exposed_protocols)
}

async fn wait_for_status(home: &FabricHome) -> Result<ControlResponse> {
    for _ in 0..50 {
        match send_control(home, ControlRequest::Status).await {
            Ok(response) => return Ok(response),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    send_control(home, ControlRequest::Status).await
}

async fn wait_for_reachability_status(home: &FabricHome) -> Result<ControlResponse> {
    for _ in 0..50 {
        match send_control(home, ControlRequest::ReachabilityStatus).await {
            Ok(response) => return Ok(response),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    send_control(home, ControlRequest::ReachabilityStatus).await
}

async fn assert_status_exposes(home: &FabricHome, protocol: &str) -> Result<()> {
    let exposed = exposed_protocols(home).await?;
    assert!(
        exposed.iter().any(|entry| entry == protocol),
        "{protocol:?} missing from exposed protocols: {exposed:?}"
    );
    Ok(())
}

async fn assert_status_does_not_expose(home: &FabricHome, protocol: &str) -> Result<()> {
    let exposed = exposed_protocols(home).await?;
    assert!(
        exposed.iter().all(|entry| entry != protocol),
        "{protocol:?} unexpectedly exposed: {exposed:?}"
    );
    Ok(())
}

async fn assert_status_shell_allowed(home: &FabricHome) -> Result<()> {
    let response = wait_for_status(home).await?;
    let ControlResponse::Status { allow_shell, .. } = response else {
        panic!("unexpected response: {response:?}");
    };
    assert!(allow_shell, "daemon should have shell allowed from config");
    Ok(())
}

async fn local_slice_guard() -> LocalSliceGuard {
    while LOCAL_SLICE_LOCKED
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(LOCAL_SLICE_SETTLE).await;
    LocalSliceGuard
}

fn fabric_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric")
}

#[cfg(unix)]
fn git_ok(cwd: Option<&Path>, args: &[&str]) -> Result<String> {
    let output = run_git_process(cwd, args, &[])?;
    assert_process_ok(&format!("git {args:?}"), &output)?;
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

#[cfg(unix)]
fn git_bare_head(repository: &Path) -> Result<String> {
    git_ok(
        None,
        &[
            "--git-dir",
            repository.to_str().unwrap(),
            "rev-parse",
            "refs/heads/main",
        ],
    )
}

#[cfg(unix)]
fn run_git_process(
    cwd: Option<&Path>,
    args: &[&str],
    env: &[(&str, &OsStr)],
) -> Result<std::process::Output> {
    let mut command = Command::new("git");
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    for (name, value) in env {
        command.env(name, value);
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn git {args:?}"))?;
    let started = Instant::now();
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait()? {
            break (status, false);
        }
        if started.elapsed() >= Duration::from_secs(60) {
            let _ = child.kill();
            break (child.wait()?, true);
        }
        thread::sleep(Duration::from_millis(20));
    };
    let mut stdout = Vec::new();
    child
        .stdout
        .take()
        .context("Git stdout was not piped")?
        .read_to_end(&mut stdout)?;
    let mut stderr = Vec::new();
    child
        .stderr
        .take()
        .context("Git stderr was not piped")?
        .read_to_end(&mut stderr)?;
    if timed_out {
        bail!(
            "git {args:?} timed out after 60 seconds\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
    }
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

#[cfg(unix)]
fn assert_process_ok(label: &str, output: &std::process::Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    bail!(
        "{label} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn run_fabric(home: &FabricHome, args: &[&str]) -> Result<String> {
    let mut child = Command::new(fabric_bin())
        .arg("--home")
        .arg(home.root())
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn fabric {args:?}"))?;
    let started = Instant::now();
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait()? {
            break (status, false);
        }
        if started.elapsed() >= FABRIC_COMMAND_TIMEOUT {
            let _ = child.kill();
            break (child.wait()?, true);
        }
        thread::sleep(Duration::from_millis(20));
    };

    let mut stdout = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_end(&mut stdout)?;
    }
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_end(&mut stderr)?;
    }

    if timed_out {
        bail!(
            "fabric {:?} timed out after {:?}\nstdout:\n{}\nstderr:\n{}",
            args,
            FABRIC_COMMAND_TIMEOUT,
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
    }

    if !status.success() {
        bail!(
            "fabric {:?} failed with status {}\nstdout:\n{}\nstderr:\n{}",
            args,
            status,
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
    }
    Ok(String::from_utf8(stdout)?.trim().to_string())
}

async fn spawn_echo_service(path: &Path, hits: Arc<AtomicUsize>) -> Result<JoinHandle<()>> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    Ok(tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            hits.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(echo_connection(stream));
        }
    }))
}

async fn spawn_tcp_echo_service() -> Result<(String, Arc<AtomicUsize>, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let hits = Arc::new(AtomicUsize::new(0));
    let task_hits = hits.clone();
    let task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            task_hits.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(tcp_echo_connection(stream));
        }
    });
    Ok((addr, hits, task))
}

async fn echo_connection(stream: UnixStream) {
    let (mut read, mut write) = stream.into_split();
    let _ = tokio::io::copy(&mut read, &mut write).await;
}

async fn tcp_echo_connection(stream: TcpStream) {
    let (mut read, mut write) = stream.into_split();
    let _ = tokio::io::copy(&mut read, &mut write).await;
}

async fn unix_round_trip(socket: &PathBuf, payload: &[u8]) -> Result<Vec<u8>> {
    let mut stream = UnixStream::connect(socket).await?;
    stream_round_trip(&mut stream, payload).await
}

async fn tcp_round_trip(addr: &str, payload: &[u8]) -> Result<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await?;
    tcp_stream_round_trip(&mut stream, payload).await
}

async fn assert_tunnel_rejects_quickly(socket: &PathBuf, payload: &[u8]) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = UnixStream::connect(socket).await?;
        let _ = stream.write_all(payload).await;
        let mut buf = [0; 1];
        let read = stream.read(&mut buf).await?;
        if read != 0 {
            bail!("rejected tunnel unexpectedly returned {read} bytes");
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("tunnel reject timed out")??;
    Ok(())
}

async fn stream_round_trip(stream: &mut UnixStream, payload: &[u8]) -> Result<Vec<u8>> {
    tokio::time::timeout(LOCAL_IO_TIMEOUT, async {
        stream.write_all(payload).await?;
        read_expected(stream, payload).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("unix round trip timed out")??;
    Ok(payload.to_vec())
}

async fn read_expected(stream: &mut UnixStream, expected: &[u8]) -> Result<()> {
    let mut response = vec![0; expected.len()];
    stream.read_exact(&mut response).await?;
    assert_eq!(response, expected);
    Ok(())
}

async fn tcp_stream_round_trip(stream: &mut TcpStream, payload: &[u8]) -> Result<Vec<u8>> {
    tokio::time::timeout(LOCAL_IO_TIMEOUT, async {
        stream.write_all(payload).await?;
        read_expected_tcp(stream, payload).await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("tcp round trip timed out")??;
    Ok(payload.to_vec())
}

async fn read_expected_tcp(stream: &mut TcpStream, expected: &[u8]) -> Result<()> {
    let mut response = vec![0; expected.len()];
    stream.read_exact(&mut response).await?;
    assert_eq!(response, expected);
    Ok(())
}

async fn wait_for_file_contents(path: &Path, expected: &str) -> Result<()> {
    tokio::time::timeout(LOCAL_IO_TIMEOUT, async {
        loop {
            match fs::read_to_string(path) {
                Ok(contents) if contents == expected => return Ok(()),
                Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .with_context(|| format!("{} did not contain {expected:?}", path.display()))?
}

fn pid_cat_argv(pid_file: &Path) -> Vec<String> {
    vec![
        "/bin/sh".to_string(),
        "-c".to_string(),
        "printf '%s' \"$$\" > \"$1\"; exec /bin/cat".to_string(),
        "fabric-test".to_string(),
        pid_file.display().to_string(),
    ]
}

fn read_pid(path: &Path) -> Result<i32> {
    fs::read_to_string(path)?
        .parse()
        .with_context(|| format!("failed to parse pid from {}", path.display()))
}

fn process_running(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

async fn wait_for_process_exit(pid: i32) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while process_running(pid) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .with_context(|| format!("process {pid} did not exit"))??;
    Ok(())
}

// ---------------------------------------------------------------------------
// NAMED PARTITION FAILURE MODES
//
// One test per way the network goes wrong, each measuring how long recovery
// took and asserting a stated bound. "It recovers" is not a property; "it
// carries bytes again within N seconds and nobody typed anything" is.
//
// EVERY TEST HERE IS A ROW IN docs/failure-modes.md, and every row there names
// the test that proves it. If you add a mode, add the row. If you change what a
// test proves, change the row. If you find a mode with no test, add the row
// anyway and write NOT PROVEN in it: a page listing only the failures we
// happened to test reads as a complete list of what can go wrong, and is not.
//
// That page is written for somebody who does not know fabric and is deciding
// whether to trust it on their own machines, so its numbers are the measured
// ones printed by these tests rather than adjectives.
// ---------------------------------------------------------------------------

/// How long a partition test will wait for the tunnel to carry bytes again.
/// Generous, because the assertion that matters is the MEASURED time printed
/// beside it, not this ceiling.
const RECOVERY_BUDGET: Duration = Duration::from_secs(30);

/// Wait until a NEW connection carries a payload again, and return how long.
///
/// This is the page-reload question: is the service reachable again. It is NOT
/// the recovery question, and on its own it guards nothing. A fresh dial builds
/// a fresh session and succeeds whether or not the retry loop exists at all,
/// which is exactly how the first version of these tests passed with recovery
/// removed. Every mode below therefore ALSO holds a live connection across the
/// partition, and that half is what fails when recovery is broken.
async fn time_until_tunnel_carries(local_addr: &str, payload: &[u8]) -> Result<Duration> {
    let started = std::time::Instant::now();
    loop {
        if started.elapsed() > RECOVERY_BUDGET {
            anyhow::bail!(
                "the tunnel never carried bytes again within {:?}, so service was \
                 NOT restored without intervention",
                RECOVERY_BUDGET
            );
        }
        if let Ok(mut stream) = TcpStream::connect(local_addr).await
            && tokio::time::timeout(
                Duration::from_secs(2),
                tcp_stream_round_trip(&mut stream, payload),
            )
            .await
            .is_ok()
        {
            return Ok(started.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Stand up the dev-server shape Nathan actually uses: a TCP service on one
/// machine, exposed, and dialed to a local port on the other.
async fn tcp_tunnel_pair() -> Result<(
    TempDir,
    TempDir,
    FabricHome,
    FabricHome,
    FabricNode,
    FabricNode,
    String,
    Arc<AtomicUsize>,
    JoinHandle<()>,
)> {
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());
    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(
        &a_home,
        &node_a,
        node_b.id(),
        Some("node-b"),
        Some(node_b.addr()),
    )
    .await?;
    trust_peer(
        &b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    let (echo_addr, hits, task) = spawn_tcp_echo_service().await?;
    run_fabric(&a_home, &["expose", "web", "--tcp", echo_addr.as_str()])?;
    let local_addr = run_fabric(&b_home, &["dial", "node-a", "web", "--tcp", "127.0.0.1:0"])?;
    // Wait for the tunnel to actually carry before handing it over. `dial`
    // binds the local port immediately, so a caller that connects straight away
    // is racing the first attach. Under a loaded machine that race is lost, and
    // the resulting timeout looks like a recovery failure in whichever test
    // happens to be running.
    time_until_tunnel_carries(&local_addr, b"ready").await?;
    Ok((
        a_dir, b_dir, a_home, b_home, node_a, node_b, local_addr, hits, task,
    ))
}

/// MODE 3: ASYMMETRIC. The exposing side stops accepting, while its own
/// outbound path stays up.
///
/// This is the one that usually breaks retry logic, because each side's view of
/// the other is different and only one of them knows anything is wrong.
///
/// The asymmetry is asserted rather than assumed: the blocked side must still
/// reach its peer while the peer cannot reach it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tunnel_recovers_from_an_asymmetric_partition() -> Result<()> {
    let _guard = local_slice_guard().await;
    let (_a_dir, _b_dir, a_home, _b_home, node_a, node_b, local_addr, _hits, task) =
        tcp_tunnel_pair().await?;

    // A LIVE connection, held across the partition. This is the live-reload
    // socket: the thing a browser leaves open and silently stops using.
    let mut live = TcpStream::connect(&local_addr).await?;
    tcp_stream_round_trip(&mut live, b"before").await?;

    // One direction only: A refuses new attaches and drops the live one.
    run_fabric(&a_home, &["debug", "block-tunnels"])?;
    run_fabric(&a_home, &["debug", "drop-tunnels"])?;
    // Written while the network is down. It must arrive later, unacknowledged
    // bytes being replayed by the session rather than lost.
    live.write_all(b"during-asymmetric").await?;

    // Prove the partition is genuinely one-way. A's own outbound still works,
    // or this is a full outage wearing an asymmetric name.
    let outbound = run_fabric(&a_home, &["ping", "node-b"]);
    assert!(
        outbound.is_ok(),
        "the blocked side lost its OUTBOUND path too, so this is not an \
         asymmetric partition and the test is measuring the wrong thing"
    );

    run_fabric(&a_home, &["debug", "unblock-tunnels"])?;

    // The held connection must resume on its own. THIS is the half that fails
    // when the retry loop is removed.
    let resumed = std::time::Instant::now();
    tokio::time::timeout(
        RECOVERY_BUDGET,
        read_expected_tcp(&mut live, b"during-asymmetric"),
    )
    .await
    .context("the LIVE connection never resumed after an asymmetric partition")??;
    let live_took = resumed.elapsed();
    tcp_stream_round_trip(&mut live, b"after-asymmetric").await?;

    let took = time_until_tunnel_carries(&local_addr, b"new-after-asymmetric").await?;
    println!(
        "MODE 3 asymmetric: live connection resumed in {live_took:?}, a new one \
         connected in {took:?}, nothing typed"
    );
    drop(live);

    task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// MODE 4: FLAPPING. Repeated brief drops, then one heal.
///
/// The question is not whether it recovers but whether BACKOFF makes recovery
/// take longer than the outage did. A retry schedule that widens on every
/// failure can leave a tunnel dark long after the network came back, and that
/// is indistinguishable from a hang to whoever is waiting.
///
/// So this measures recovery after five flaps and compares it against the
/// budget a single drop gets. It does not assert a specific backoff policy,
/// only that flapping does not turn a healed network into a long silence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flapping_does_not_make_recovery_slower_than_the_outage() -> Result<()> {
    let _guard = local_slice_guard().await;
    let (_a_dir, _b_dir, a_home, _b_home, node_a, node_b, local_addr, _hits, task) =
        tcp_tunnel_pair().await?;

    let mut live = TcpStream::connect(&local_addr).await?;
    tcp_stream_round_trip(&mut live, b"before").await?;

    for _ in 0..5 {
        run_fabric(&a_home, &["debug", "block-tunnels"])?;
        run_fabric(&a_home, &["debug", "drop-tunnels"])?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        run_fabric(&a_home, &["debug", "unblock-tunnels"])?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The held connection must still be usable after five flaps.
    //
    // RED measurement before the fix: a single partition resumed in about 20
    // ms. Five 200 ms flaps resumed in 1.2 to 1.9 seconds, and sometimes took
    // the full 15-second backoff step.
    //
    // The cause is not mysterious. `next_delay` walks
    // 100, 250, 500, 1000, 2000, 5000, 10000, 15000 ms and only resets once an
    // attach had been stable for two seconds. A tunnel that flapped faster than
    // that never reset, so brief independent outages became one long outage.
    //
    // A successful protocol attach proves that the peer is back. The next loss
    // must therefore restart at the 100 ms step. A 1.5 second budget allows the
    // complete first three-step sequence, including maximum jitter, without
    // permitting a retained 2, 5, 10, or 15 second delay.
    let resumed = std::time::Instant::now();
    tokio::time::timeout(Duration::from_millis(1500), async {
        live.write_all(b"after-flapping").await?;
        read_expected_tcp(&mut live, b"after-flapping").await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context(
        "the LIVE connection never resumed after flapping, even allowing for \
              a fresh three-step reconnect sequence",
    )??;
    let live_took = resumed.elapsed();

    let took = time_until_tunnel_carries(&local_addr, b"new-after-flapping").await?;
    println!(
        "MODE 4 flapping: live connection resumed in {live_took:?} after 5 flaps, \
         a new one connected in {took:?}, nothing typed"
    );
    drop(live);

    task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// Trust a peer and restrict it to a set of services.
async fn trust_peer_allowing(
    home: &FabricHome,
    node: &FabricNode,
    id: iroh::EndpointId,
    name: Option<&str>,
    addr: Option<iroh::EndpointAddr>,
    allow: &[&str],
) -> Result<()> {
    let mut peers = PeerBook::load(home)?;
    peers.add_with_allow(
        id,
        name.map(str::to_string),
        addr,
        Some(allow.iter().map(|s| s.to_string()).collect()),
    );
    peers.save(home)?;
    node.state().reload_peers().await?;
    Ok(())
}

/// A peer restricted to other services cannot reach this one, and a permitted
/// peer is unaffected.
///
/// This is the sharing feature: "Johannes may dial my web and nothing else" is
/// the same mechanism as "droppy may not". Both halves are asserted here,
/// because a permission system that denies everything is as useless as one that
/// permits everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_not_permitted_for_a_service_cannot_reach_it() -> Result<()> {
    let _guard = local_slice_guard().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());
    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;

    // A trusts B, but only for `echo`. `web` is exposed and NOT listed.
    trust_peer_allowing(
        &a_home,
        &node_a,
        node_b.id(),
        Some("node-b"),
        Some(node_b.addr()),
        &["echo"],
    )
    .await?;
    trust_peer(
        &b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    let (echo_addr, hits, task) = spawn_tcp_echo_service().await?;
    run_fabric(&a_home, &["expose", "web", "--tcp", echo_addr.as_str()])?;
    let local_addr = run_fabric(&b_home, &["dial", "node-a", "web", "--tcp", "127.0.0.1:0"])?;

    // The dial listener binds locally either way; the refusal happens when the
    // connection is actually made. So this must not carry bytes.
    let denied = async {
        let mut stream = TcpStream::connect(&local_addr).await?;
        tcp_stream_round_trip(&mut stream, b"should-not-arrive").await
    };
    assert!(
        tokio::time::timeout(Duration::from_secs(10), denied)
            .await
            .map(|r| r.is_err())
            .unwrap_or(true),
        "a peer with allow = [echo] reached `web`, so the gate is not gating"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "the exposed service was reached despite the peer not being permitted"
    );

    // Now permit it. The SAME dial must work, unchanged.
    trust_peer_allowing(
        &a_home,
        &node_a,
        node_b.id(),
        Some("node-b"),
        Some(node_b.addr()),
        &["echo", "web"],
    )
    .await?;
    let mut stream = TcpStream::connect(&local_addr).await?;
    tokio::time::timeout(
        Duration::from_secs(15),
        tcp_stream_round_trip(&mut stream, b"now-permitted"),
    )
    .await
    .context("a permitted peer could not reach the service it was just granted")??;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the permitted dial did not reach the exposed service"
    );

    drop(stream);
    task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// MODE 5: THE FAR PEER RESTARTS MID-SESSION. New process, same identity.
///
/// This is not an edge case, it is the daily workflow: a dev server is
/// restarted while a browser is connected to it through the tunnel. The
/// question a person actually has is "do I have to reload the page", and it has
/// two halves that can differ:
///
///   1. Does a NEW request work again, with nothing typed?
///   2. Does the connection that was already open survive?
///
/// The second is the live-reload socket. A page that looks fine and has quietly
/// stopped updating is worse than one that visibly died, so this test records
/// which of the two actually happens rather than asserting the comfortable one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_restarting_mid_session_restores_service_without_intervention() -> Result<()> {
    let _guard = local_slice_guard().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());
    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(
        &a_home,
        &node_a,
        node_b.id(),
        Some("node-b"),
        Some(node_b.addr()),
    )
    .await?;
    trust_peer(
        &b_home,
        &node_b,
        node_a.id(),
        Some("node-a"),
        Some(node_a.addr()),
    )
    .await?;

    let (echo_addr, hits, task) = spawn_tcp_echo_service().await?;
    run_fabric(&a_home, &["expose", "web", "--tcp", echo_addr.as_str()])?;
    let local_addr = run_fabric(&b_home, &["dial", "node-a", "web", "--tcp", "127.0.0.1:0"])?;
    time_until_tunnel_carries(&local_addr, b"ready").await?;

    let mut live = TcpStream::connect(&local_addr).await?;
    tcp_stream_round_trip(&mut live, b"before-restart").await?;

    // The dev server's machine restarts fabric. Same identity, new process.
    node_a.shutdown().await?;

    // WHOSE PROBLEM IS IT? A successful request after recovery cannot say.
    //
    // A live-reload socket dying when the server restarts is normal and is not
    // fabric's doing: run the same dev server locally with no fabric involved
    // and the socket dies too, because the process that owned it is gone. What
    // makes it a non-event locally is that the CLIENT RECONNECTS, which vite and
    // webpack both do within a second or two.
    //
    // So the question is what happens to that reconnect through the tunnel. It
    // has two parts a real client cares about:
    //
    //   1. How long until a reconnect succeeds after the peer returns.
    //   2. What a reconnect attempted DURING the outage does. A prompt failure
    //      lets a retrying client try again; a hang holds it until its own
    //      timeout, which for a browser can be tens of seconds.
    //
    // Keep the peer down while measuring the second part. The old test started
    // this measurement after recovery, so its name and its proof disagreed.
    let during = std::time::Instant::now();
    let attempted_during_outage = tokio::time::timeout(Duration::from_secs(5), async {
        let mut probe = TcpStream::connect(&local_addr).await?;
        tcp_stream_round_trip(&mut probe, b"reconnect-during-outage").await
    })
    .await;
    let during_took = during.elapsed();
    let outcome = match &attempted_during_outage {
        Ok(Ok(_)) => "succeeded",
        Ok(Err(_)) => "failed promptly, so a retrying client retries",
        Err(_) => "HUNG, which holds a retrying client until its own timeout",
    };

    // A reconnect must not hang. If it does, a client that retries is punished
    // for retrying and the failure IS fabric's.
    assert!(
        during_took < Duration::from_secs(5),
        "a reconnect attempted during the outage hung for {during_took:?}. A \
         client that retries would be held until its own timeout, and that makes \
         this fabric's problem rather than the application's"
    );
    assert!(
        matches!(attempted_during_outage, Ok(Err(_))),
        "a request succeeded while the peer was stopped: {attempted_during_outage:?}"
    );

    let node_a = FabricNode::start(a_home.clone()).await?;
    trust_peer(
        &a_home,
        &node_a,
        node_b.id(),
        Some("node-b"),
        Some(node_b.addr()),
    )
    .await?;

    // Half one: a NEW request, which is a person reloading the page.
    let fresh = time_until_tunnel_carries(&local_addr, b"after-restart").await?;

    // Half two: the connection that was already open. Recorded, not demanded,
    // because whichever way it goes belongs in the document as a fact.
    let live_survived = tokio::time::timeout(Duration::from_secs(20), async {
        live.write_all(b"live-after-restart").await?;
        read_expected_tcp(&mut live, b"live-after-restart").await?;
        Ok::<(), anyhow::Error>(())
    })
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false);

    println!(
        "MODE 5 peer restart: a reconnect attempted during the outage {outcome} \
         in {during_took:?}; a new request succeeded {fresh:?} after the restart; \
         the already-open connection survived: {live_survived}"
    );

    // The property that must hold either way: service is restored with nothing
    // typed. If a person must reload, the document has to say so, and that is
    // what `live_survived` records.
    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "the exposed service was never reached after the peer restarted"
    );

    drop(live);
    task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// MODE 2: A LONG DROP. Does anything time out permanently and never come back?
///
/// The backoff ceiling is 15 seconds, so a 90 second outage walks the whole
/// schedule several times over and sits at the top of it. That is the state
/// where a retry loop with a bad terminal condition gives up for good, and the
/// difference between "slow" and "never" is the only thing that matters here.
///
/// 90 seconds rather than the "minutes" in the brief: it clears the ceiling with
/// room to spare, and a test nobody will run because it takes five minutes
/// proves less than one that runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_outage_does_not_time_out_permanently() -> Result<()> {
    let _guard = local_slice_guard().await;
    let (_a_dir, _b_dir, a_home, _b_home, node_a, node_b, local_addr, _hits, task) =
        tcp_tunnel_pair().await?;

    let mut live = TcpStream::connect(&local_addr).await?;
    tcp_stream_round_trip(&mut live, b"before").await?;

    run_fabric(&a_home, &["debug", "block-tunnels"])?;
    run_fabric(&a_home, &["debug", "drop-tunnels"])?;
    live.write_all(b"during-long-outage").await?;

    // Ninety seconds of nothing working.
    tokio::time::sleep(Duration::from_secs(90)).await;
    run_fabric(&a_home, &["debug", "unblock-tunnels"])?;

    let resumed = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(45),
        read_expected_tcp(&mut live, b"during-long-outage"),
    )
    .await
    .context(
        "after a 90 second outage the live connection never came back, so \
         something timed out permanently",
    )??;
    let live_took = resumed.elapsed();
    let fresh = time_until_tunnel_carries(&local_addr, b"after-long-outage").await?;
    println!(
        "MODE 2 long outage (90 s): live connection resumed in {live_took:?}, a new \
         one connected in {fresh:?}, nothing typed"
    );

    drop(live);
    task.abort();
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[cfg(all(unix, debug_assertions))]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn sync_walks_do_not_delay_exec_pipe_delivery() -> Result<()> {
    let _guard = local_slice_guard().await;
    let target_dir = TempDir::new()?;
    let source_dir = TempDir::new()?;
    let helper_dir = TempDir::new()?;
    let target_home = FabricHome::new(target_dir.path());
    let source_home = FabricHome::new(source_dir.path());
    let target_folder = target_dir.path().join("shared");
    let source_folder = source_dir.path().join("shared");
    fs::create_dir_all(target_folder.join("data"))?;
    fs::create_dir_all(source_folder.join("data"))?;
    write_latency_sync(&target_home, &target_folder)?;
    write_latency_sync(&source_home, &source_folder)?;
    fs::write(target_folder.join(".fabric-test-walk-hold-ms"), b"500")?;

    let target = FabricNode::start(target_home.clone()).await?;
    let source = FabricNode::start(source_home.clone()).await?;
    trust_peer(
        &target_home,
        &target,
        source.id(),
        Some("source"),
        Some(source.addr()),
    )
    .await?;
    trust_peer(
        &source_home,
        &source,
        target.id(),
        Some("target"),
        Some(target.addr()),
    )
    .await?;

    let emitter = compile_pipe_tick(&helper_dir)?;
    target
        .expose_exec("stdio-cat", vec![emitter.display().to_string()])
        .await?;
    let scans_before = sync_full_scans(&target_home, "latency").await?;

    let keep_writing = Arc::new(AtomicBool::new(true));
    let writer_flag = keep_writing.clone();
    let changed_path = target_folder.join("data/changing.txt");
    let writer = thread::spawn(move || {
        let mut revision = 0_u64;
        while writer_flag.load(Ordering::Acquire) {
            fs::write(&changed_path, revision.to_string()).unwrap();
            revision += 1;
            thread::sleep(Duration::from_millis(50));
        }
    });

    let socket = source.dial("target", "stdio-cat").await?;
    let mut lines = BufReader::new(UnixStream::connect(socket).await?).lines();
    let mut first_source = None;
    let mut previous_source = None;
    let mut previous_delivery = None;
    let mut max_source_gap = Duration::ZERO;
    let mut max_delivery_gap = Duration::ZERO;
    let mut samples = 0_usize;

    loop {
        let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await
            .context("the exec pipe stopped delivering records")??
            .context("the exec child ended before the five-second window")?;
        let mut fields = line.split_whitespace();
        let _sequence: u64 = fields.next().context("record has no sequence")?.parse()?;
        let source_nanos: u64 = fields
            .next()
            .context("record has no source time")?
            .parse()?;
        let delivery = Instant::now();
        let source_time = Duration::from_nanos(source_nanos);
        let window_start = *first_source.get_or_insert(source_time);
        if let Some(previous) = previous_source {
            max_source_gap = max_source_gap.max(source_time.saturating_sub(previous));
        }
        if let Some(previous) = previous_delivery {
            max_delivery_gap = max_delivery_gap.max(delivery.saturating_duration_since(previous));
        }
        previous_source = Some(source_time);
        previous_delivery = Some(delivery);
        samples += 1;
        if source_time.saturating_sub(window_start) >= Duration::from_secs(5) {
            break;
        }
    }

    keep_writing.store(false, Ordering::Release);
    writer.join().expect("the filesystem writer panicked");
    let scans_after = sync_full_scans(&target_home, "latency").await?;
    source.shutdown().await?;
    target.shutdown().await?;

    println!(
        "five-second sync/exec pipe window: samples={samples} source_max={max_source_gap:?} delivery_max={max_delivery_gap:?} scans={}",
        scans_after.saturating_sub(scans_before)
    );
    assert!(
        scans_after >= scans_before + 2,
        "the five-second window ran no sustained sync scan load"
    );
    assert!(
        max_source_gap < Duration::from_millis(50),
        "the producer paused for {max_source_gap:?}; delivery timing cannot diagnose Fabric"
    );
    assert!(
        max_delivery_gap < Duration::from_millis(150),
        "sync activity delayed exec pipe delivery for {max_delivery_gap:?}; this is local scheduler starvation, not network weather"
    );
    Ok(())
}

#[cfg(all(unix, debug_assertions))]
fn write_latency_sync(home: &FabricHome, folder: &Path) -> Result<()> {
    let raw = format!(
        "[[sync]]\nname = \"latency\"\nfolder = {folder:?}\npeers = \"*\"\npolicy = \"bus\"\ninclude = [\"data/**\"]\n"
    );
    fs::write(home.syncs_path(), raw)?;
    Ok(())
}

#[cfg(all(unix, debug_assertions))]
fn compile_pipe_tick(directory: &TempDir) -> Result<PathBuf> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pipe_tick.rs");
    let output = directory.path().join("pipe-tick");
    let status = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
        .arg("--edition=2024")
        .arg(&source)
        .arg("-o")
        .arg(&output)
        .status()
        .with_context(|| format!("failed to compile {}", source.display()))?;
    if !status.success() {
        bail!("rustc failed to compile {}", source.display());
    }
    Ok(output)
}

#[cfg(all(unix, debug_assertions))]
async fn sync_full_scans(home: &FabricHome, name: &str) -> Result<u64> {
    for _ in 0..50 {
        match send_control(home, ControlRequest::SyncStatus).await {
            Ok(ControlResponse::SyncStatus { entries, .. }) => {
                return entries
                    .into_iter()
                    .find(|entry| entry.name == name)
                    .map(|entry| entry.full_scans)
                    .with_context(|| format!("sync entry {name:?} is absent"));
            }
            Ok(other) => bail!("unexpected sync status response: {other:?}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("the daemon did not return sync status")
}
