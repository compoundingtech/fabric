use std::{
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
    config::{FabricHome, PeerBook, generate_identity_file},
    control::{ControlRequest, ControlResponse},
    daemon::{FabricNode, send_control},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
    task::JoinHandle,
};

// These tests start real iroh endpoints and daemon tasks; keep the default
// test runner from exercising that transport stack concurrently.
static LOCAL_SLICE_LOCKED: AtomicBool = AtomicBool::new(false);
const FABRIC_COMMAND_TIMEOUT: Duration = Duration::from_secs(20);
const LOCAL_IO_TIMEOUT: Duration = Duration::from_secs(30);
const LOCAL_SLICE_SETTLE: Duration = Duration::from_millis(500);

struct LocalSliceGuard;

impl Drop for LocalSliceGuard {
    fn drop(&mut self) {
        LOCAL_SLICE_LOCKED.store(false, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    tokio::time::timeout(
        Duration::from_secs(10),
        read_expected(&mut stream, b"during-drop"),
    )
    .await??;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    tokio::time::timeout(
        Duration::from_secs(10),
        read_expected(&mut stream, b"during-drop"),
    )
    .await??;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    node_a
        .expose_exec_with_limit(
            "limited-cat",
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "printf spawn >> \"$1\"; exec /bin/cat".to_string(),
                "fabric-test".to_string(),
                marker.display().to_string(),
            ],
            1,
        )
        .await?;

    let dial_socket = node_b.dial("node-a", "limited-cat").await?;
    let mut first = UnixStream::connect(&dial_socket).await?;
    stream_round_trip(&mut first, b"first-child").await?;
    assert_eq!(fs::read_to_string(&marker)?, "spawn");

    assert_tunnel_rejects_quickly(&dial_socket, b"second-child").await?;
    assert_eq!(
        fs::read_to_string(&marker)?,
        "spawn",
        "limit rejection must not spawn a second child"
    );
    stream_round_trip(&mut first, b"first-still-alive").await?;

    drop(first);
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    tokio::time::timeout(Duration::from_secs(15), async {
        tokio::try_join!(writer, reader)?;
        Ok::<(), anyhow::Error>(())
    })
    .await??;
    assert_eq!(response, payload);

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unified_config_restores_shell_peers_and_exposes_on_restart() -> Result<()> {
    let _guard = local_slice_guard().await;
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
    assert!(raw_config.contains("allow_shell = true"));
    assert!(raw_config.contains("node-b"));
    assert!(raw_config.contains("stdio-cat"));

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        Duration::from_secs(10),
        read_expected_tcp(&mut stream, b"during-drop"),
    )
    .await??;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    let response = send_control(&node_b_home, ControlRequest::ReachabilityStatus).await?;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn declarative_peer_config_is_loaded_on_start() -> Result<()> {
    let _guard = local_slice_guard().await;
    let node_a_dir = TempDir::new()?;
    let node_b_dir = TempDir::new()?;
    let node_a_home = FabricHome::new(node_a_dir.path());
    let node_b_home = FabricHome::new(node_b_dir.path());

    let node_b_id = generate_identity_file(&node_b_home.identity_path())?;
    fs::write(
        node_a_home.peers_path(),
        format!("[[peers]]\nid = \"{node_b_id}\"\nname = \"node-b\"\n"),
    )?;

    let node_a = FabricNode::start(node_a_home.clone()).await?;

    let mut node_b_peers = PeerBook::default();
    node_b_peers.add(node_a.id(), Some("node-a".to_string()), Some(node_a.addr()));
    node_b_peers.save(&node_b_home)?;

    let node_b = FabricNode::start(node_b_home).await?;
    assert_eq!(node_b.id(), node_b_id);

    let ping = node_b.ping("node-a").await?;
    assert_eq!(ping.bytes, 32);

    node_b.shutdown().await?;
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
