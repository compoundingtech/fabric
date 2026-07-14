use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use fabric::{
    config::{FabricHome, PeerBook, generate_identity_file},
    control::{ControlRequest, ControlResponse},
    daemon::{FabricNode, send_control},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    task::JoinHandle,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_expose_dial_round_trips_and_acl_rejects_unknown_node() -> Result<()> {
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
async fn ping_round_trips_builtin_echo() -> Result<()> {
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
    let ControlResponse::ReachabilityStatus { peers, .. } = response else {
        panic!("unexpected response: {response:?}");
    };
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

async fn echo_connection(stream: UnixStream) {
    let (mut read, mut write) = stream.into_split();
    let _ = tokio::io::copy(&mut read, &mut write).await;
}

async fn unix_round_trip(socket: &PathBuf, payload: &[u8]) -> Result<Vec<u8>> {
    let mut stream = UnixStream::connect(socket).await?;
    stream.write_all(payload).await?;
    let mut response = vec![0; payload.len()];
    stream.read_exact(&mut response).await?;
    Ok(response)
}
