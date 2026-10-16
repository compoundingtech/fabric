use super::*;
use crate::session_paths::{SessionDirection, SessionPathStatus};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn echo(local: &mut tokio::net::TcpStream, bytes: &[u8]) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        local.write_all(bytes).await?;
        let mut got = vec![0; bytes.len()];
        local.read_exact(&mut got).await?;
        anyhow::ensure!(
            got == bytes,
            "session path observation changed payload bytes"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

async fn paths(state: &DaemonState, direction: SessionDirection) -> Result<SessionPathStatus> {
    let live = state.peer_connections.session_paths.snapshot();
    anyhow::ensure!(live.len() == 1, "expected one live session, got {live:?}");
    anyhow::ensure!(live[0].direction == direction);
    Ok(live[0].clone())
}

async fn replace_with_relay(node: &FabricNode, url: iroh::RelayUrl) -> Result<Endpoint> {
    let state = node.state();
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(load_or_create_identity(&state.home)?)
        .alpns(accepted_alpns(
            &state.services,
            &*state.exposures.read().await,
        ))
        .hooks(AllowListHook {
            policy: state.peer_book.clone(),
        })
        .relay_mode(iroh::RelayMode::custom([url]))
        .clear_ip_transports()
        .bind()
        .await?;
    tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await?;
    assert!(
        endpoint.bound_sockets().is_empty(),
        "relay-only fixture has an IP transport"
    );
    let generation = next_endpoint_generation(&state.home, state.endpoint_handle().generation)?;
    Ok(state
        .endpoint_tx
        .send_replace(CurrentEndpoint {
            generation,
            endpoint,
        })
        .endpoint)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PathScenario {
    Direct,
    Resume,
    RelayOnly,
    MigrateToRelay,
}

async fn session_path_fixture(scenario: PathScenario) -> Result<()> {
    let relay_only = scenario == PathScenario::RelayOnly;
    let resume = matches!(
        scenario,
        PathScenario::Resume | PathScenario::MigrateToRelay
    );
    let relay = if relay_only || scenario == PathScenario::MigrateToRelay {
        let mut config = iroh_relay::server::ServerConfig::default();
        config.relay = Some(iroh_relay::server::RelayConfig::new((
            std::net::Ipv4Addr::LOCALHOST,
            0,
        )));
        Some(iroh_relay::server::Server::spawn(config).await?)
    } else {
        None
    };
    let server_dir = tempfile::tempdir()?;
    let client_dir = tempfile::tempdir()?;
    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start(server_home.clone()).await?;
    let client = FabricNode::start(client_home.clone()).await?;
    if relay_only {
        let relay = relay.as_ref().unwrap();
        let url: iroh::RelayUrl = format!("http://{}", relay.http_addr().unwrap()).parse()?;
        for node in [&server, &client] {
            replace_with_relay(node, url.clone()).await?.close().await;
        }
    }
    let mut server_book = PeerBook::load(&server_home)?;
    server_book.add_inbound_only(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        Some(vec!["work/0".into()]),
    );
    server_book.save(&server_home)?;
    server.state().reload_peers().await?;
    let mut client_book = PeerBook::load(&client_home)?;
    client_book.add(server.id(), Some("server".into()), Some(server.addr()));
    client_book.save(&client_home)?;
    client.state().reload_peers().await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    server
        .expose_tcp("work/0", listener.local_addr()?.to_string())
        .await?;
    let copying = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let (mut read, mut write) = socket.split();
        tokio::io::copy(&mut read, &mut write).await
    });
    let addr = client
        .dial_tcp("server", "work/0", "127.0.0.1:0".into())
        .await?;
    let mut local = tokio::net::TcpStream::connect(addr).await?;
    echo(&mut local, b"path-before-reconnect")
        .await
        .context("initial session traffic")?;
    let initial = paths(&client.state(), SessionDirection::Outbound).await?;
    let incoming = paths(&server.state(), SessionDirection::Inbound).await?;
    assert_eq!(initial.session_id, incoming.session_id);
    assert_eq!(initial.peer_id, server.id().to_string());
    assert_eq!(incoming.peer_id, client.id().to_string());
    for status in [&initial, &incoming] {
        assert!(status.attached);
        let path = status
            .path_at_attach
            .as_ref()
            .context("attach path missing")?;
        assert_eq!(
            path.class.as_deref(),
            Some(if relay_only { "relay" } else { "direct" })
        );
        let remote = path
            .remote_addr
            .as_ref()
            .context("selected remote address missing")?;
        if relay_only {
            assert!(remote.starts_with("http://127.0.0.1:"));
        } else {
            assert!(
                remote.parse::<std::net::SocketAddr>().is_ok(),
                "invalid selected IP: {remote}"
            );
        }
        assert_eq!(path.remote_addrs.len(), 1);
    }
    assert_path_log_privacy(
        &client
            .state()
            .peer_connections
            .connection(server.id())
            .await
            .context("live connection missing")?,
    );
    if resume {
        if scenario == PathScenario::MigrateToRelay {
            let url: iroh::RelayUrl =
                format!("http://{}", relay.as_ref().unwrap().http_addr().unwrap()).parse()?;
            let old = replace_with_relay(&server, url.clone()).await?;
            // Publish the new relay address before taking away the direct path.
            client_book.add(server.id(), Some("server".into()), Some(server.addr()));
            client_book.save(&client_home)?;
            client.state().reload_peers().await?;
            let old_client = replace_with_relay(&client, url).await?;
            old.close().await;
            old_client.close().await;
        } else {
            client.state().drop_tunnel_connections();
        }
        // The same local stream must resume, keeping one session record.
        echo(&mut local, b"path-after-reconnect")
            .await
            .context("resumed session traffic")?;
        let resumed = paths(&client.state(), SessionDirection::Outbound).await?;
        let served = paths(&server.state(), SessionDirection::Inbound).await?;
        assert_eq!(resumed.session_id, initial.session_id);
        assert_eq!(served.session_id, initial.session_id);
        assert!(resumed.attach_count >= 2);
        assert!(served.attach_count >= 2);
        assert!(
            resumed.path_changes >= 1,
            "resumed session omitted its selected path change"
        );
        assert!(served.path_changes >= 1);
        if scenario == PathScenario::MigrateToRelay {
            assert_eq!(
                resumed.path_at_attach.as_ref().unwrap().class.as_deref(),
                Some("relay")
            );
            assert!(
                resumed
                    .path_at_attach
                    .as_ref()
                    .unwrap()
                    .remote_addr
                    .as_ref()
                    .unwrap()
                    .starts_with("http://127.0.0.1:")
            );
        }
        assert_ne!(
            resumed.path_at_attach.as_ref().unwrap().connection_id,
            initial.path_at_attach.as_ref().unwrap().connection_id
        );
        assert!(
            client
                .state()
                .peer_connections
                .presence
                .batch(None, 0)
                .events
                .iter()
                .all(|event| event.cause != "session_closed")
        );
    }
    local.shutdown().await?;
    let mut tail = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), local.read_to_end(&mut tail)).await??;
    assert!(tail.is_empty(), "observation injected trailing bytes");
    copying.await??;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .state()
                .peer_connections
                .session_paths
                .snapshot()
                .is_empty()
                && server
                    .state()
                    .peer_connections
                    .session_paths
                    .snapshot()
                    .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("session path records remained after normal completion")?;
    for state in [client.state(), server.state()] {
        let batch = state.peer_connections.presence.batch(None, 0);
        let attached: Vec<_> = batch
            .events
            .iter()
            .filter(|event| event.cause == "session_attached")
            .collect();
        let closed: Vec<_> = batch
            .events
            .iter()
            .filter(|event| event.cause == "session_closed")
            .collect();
        assert_eq!(closed.len(), 1);
        assert_eq!(attached.len(), if resume { 2 } else { 1 });
        assert!(
            attached.iter().chain(closed.iter()).all(|event| event
                .session
                .as_ref()
                .unwrap()
                .session_id
                == initial.session_id)
        );
        let close = closed[0]
            .session
            .as_ref()
            .unwrap()
            .path_at_close
            .as_ref()
            .context("close path missing")?;
        assert_eq!(
            close.class.as_deref(),
            Some(if relay_only || scenario == PathScenario::MigrateToRelay {
                "relay"
            } else {
                "direct"
            })
        );
        assert!(close.remote_addr.is_some());
        let json = serde_json::to_value(ControlResponse::PeerEvents { batch })?;
        assert!(
            json["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["cause"] == "session_closed"
                    && event["session"]["path_at_close"]["remote_addr"].is_string())
        );
    }
    client.shutdown().await?;
    server.shutdown().await?;
    if let Some(relay) = relay {
        relay.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn session_paths_report_real_attach_and_close_without_changing_payload() -> Result<()> {
    session_path_fixture(PathScenario::Direct).await
}
#[tokio::test]
async fn session_paths_preserve_identity_across_resume_and_emit_only_one_close() -> Result<()> {
    session_path_fixture(PathScenario::Resume).await
}

#[tokio::test]
async fn session_paths_report_relay_class_for_a_real_relay_only_tunnel() -> Result<()> {
    session_path_fixture(PathScenario::RelayOnly).await
}

#[tokio::test]
async fn session_paths_report_a_direct_to_relay_change_on_the_same_session() -> Result<()> {
    session_path_fixture(PathScenario::MigrateToRelay).await
}

fn assert_path_log_privacy(connection: &Connection) {
    #[derive(Clone, Default)]
    struct Buffer(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(buffer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        log_connection_paths("session_path_privacy", connection);
        log_sync_connection_paths_sample(connection, true);
        crate::pathwatch::log_paths(connection, "server", Duration::from_millis(1), &mut None);
    });
    let log = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    assert!(log.contains("session_path_privacy"));
    assert!(log.contains("sync_accept"));
    assert!(log.contains("pathwatch_snapshot"));
    for path in connection.paths().iter() {
        assert!(
            !log.contains(&path.remote_addr().to_string()),
            "remote path leaked to INFO: {log}"
        );
    }
    for field in [
        "remote_addr=",
        "local_addr=",
        "path_remote_addrs=",
        "path_local_addrs=",
    ] {
        assert!(!log.contains(field), "address field leaked to INFO: {log}");
    }
}
