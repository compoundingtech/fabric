//! Force the mux Ready / tunnel Hello boundary using existing entry points.
//! No sleeps or production injection hooks choose the replacement ordering.
use super::*;

#[tokio::test]
async fn preferred_duplicate_between_ready_and_hello_keeps_the_first_local_request() -> Result<()> {
    forced_ready_replacement(false, false).await
}

#[tokio::test]
async fn reverse_simultaneous_traffic_converges_after_ready_replacement() -> Result<()> {
    forced_ready_replacement(true, false).await
}

#[tokio::test]
async fn an_established_session_survives_a_preferred_duplicate_during_reconnect() -> Result<()> {
    forced_ready_replacement(false, true).await
}

fn start_client(
    local: UnixStream,
    state: &Arc<DaemonState>,
    home: FabricHome,
    initial: mux::MuxStream,
    cancel: CancellationToken,
) -> JoinHandle<Result<()>> {
    tokio::spawn(tunnel::run_client_connection_with_initial(
        local,
        state.endpoint_rx(),
        state.peer_connections.clone(),
        home,
        "server".into(),
        b"work/0".to_vec(),
        cancel,
        state.tunnel_drop_tx.subscribe(),
        None,
        initial,
    ))
}

async fn forced_ready_replacement(reverse_traffic: bool, interrupted_resume: bool) -> Result<()> {
    let first_dir = tempfile::tempdir()?;
    let second_dir = tempfile::tempdir()?;
    let first_home = FabricHome::new(first_dir.path());
    let second_home = FabricHome::new(second_dir.path());
    let first_id = load_or_create_identity(&first_home)?.public();
    let second_id = load_or_create_identity(&second_home)?.public();
    // The larger node's outgoing connection is nonpreferred at both ends.
    let (client_home, server_home) = if first_id > second_id {
        (first_home, second_home)
    } else {
        (second_home, first_home)
    };
    let server = FabricNode::start(server_home.clone()).await?;
    let client = FabricNode::start(client_home.clone()).await?;
    assert!(client.id() > server.id());
    let mut book = PeerBook::load(&server_home)?;
    book.add_inbound_only(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        Some(vec!["work/0".into()]),
    );
    book.save(&server_home)?;
    server.state().reload_peers().await?;
    let mut book = PeerBook::load(&client_home)?;
    if reverse_traffic {
        book.add_with_allow(
            server.id(),
            Some("server".into()),
            Some(server.addr()),
            Some(vec!["echo".into()]),
        );
    } else {
        book.add(server.id(), Some("server".into()), Some(server.addr()));
    }
    book.save(&client_home)?;
    client.state().reload_peers().await?;

    let target = TcpListener::bind("127.0.0.1:0").await?;
    let target_addr = target.local_addr()?;
    server.expose_tcp("work/0", target_addr.to_string()).await?;
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepted = accepts.clone();
    let copying = tokio::spawn(async move {
        loop {
            let (mut stream, _) = target.accept().await?;
            accepted.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let (mut read, mut write) = stream.split();
                tokio::io::copy(&mut read, &mut write).await
            });
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    });
    let client_state = client.state();
    let server_state = server.state();
    let endpoint = client_state.endpoint_handle();
    let started = Instant::now();
    let initial = client_state
        .peer_connections
        .open_stream(
            &endpoint.endpoint,
            endpoint.generation,
            &server.addr(),
            "work/0",
            mux::StreamActivity::Application,
        )
        .await?;
    assert_eq!(initial.connection.side(), Side::Client);
    let mut failed_connection = initial.connection.clone();
    let mut old_id = initial.connection.stable_id();
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        0,
        "mux Ready must precede opening the application target"
    );
    assert_eq!(server_state.tunnel_sessions.stats().await.total_sessions, 0);
    std::eprintln!(
        "phase=mux_ready elapsed={:?} old_connection={old_id} hello_started=false target_accepts=0",
        started.elapsed()
    );

    let (mut application, local) = UnixStream::pair()?;
    let mut local = Some(local);
    let mut initial = Some(initial);
    let cancel = CancellationToken::new();
    let mut attached = None;
    if interrupted_resume {
        attached = Some(start_client(
            local.take().unwrap(),
            &client_state,
            client_home.clone(),
            initial.take().unwrap(),
            cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            application.write_all(b"already-attached").await?;
            let mut got = [0; 16];
            application.read_exact(&mut got).await?;
            anyhow::ensure!(&got == b"already-attached");
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        server_state.tunnel_blocked.store(true, Ordering::SeqCst);
        client_state.drop_tunnel_connections();
        // Wait for a real reconnect, while the temporary admission block
        // prevents it completing. Then interrupt that connection as well.
        failed_connection = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(connection) =
                    client_state.peer_connections.connection(server.id()).await
                {
                    if connection.stable_id() != old_id
                        && server_state.tunnel_sessions.stats().await.active_attaches == 0
                    {
                        break connection;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("established session did not begin reconnecting")?;
        assert_eq!(failed_connection.side(), Side::Client);
        old_id = failed_connection.stable_id();
    }

    // This real reverse QUIC connection is preferred at both ends. Exchange
    // the existing generation frame, then register it with the same admission
    // entry point used by the daemon. No tunnel Hello has been sent yet.
    let reverse_endpoint = server_state.endpoint_handle();
    let duplicate = reverse_endpoint
        .endpoint
        .connect(client.addr(), mux::MUX_ALPN)
        .await?;
    assert_eq!(duplicate.side(), Side::Client);
    let (mut send, mut recv) = duplicate.open_bi().await?;
    send.write_u64(reverse_endpoint.generation).await?;
    send.finish()?;
    let client_generation = recv.read_u64().await?;
    server_state
        .peer_connections
        .register_incoming(&duplicate, reverse_endpoint.generation, client_generation)
        .await;
    let reverse_serving = tokio::spawn(handle_mux_connection(
        duplicate.clone(),
        server_state.clone(),
        false,
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let changed = client_state
                .peer_connections
                .connection(server.id())
                .await
                .is_some_and(|connection| connection.stable_id() != old_id);
            if changed && failed_connection.close_reason().is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("preferred duplicate admission did not finish")?;
    let canonical = client_state
        .peer_connections
        .connection(server.id())
        .await
        .context("canonical connection missing")?;
    assert_eq!(
        failed_connection.close_reason(),
        Some(iroh::endpoint::ConnectionError::LocallyClosed)
    );
    assert_ne!(canonical.stable_id(), old_id);
    assert_eq!(duplicate.close_reason(), None);
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        usize::from(interrupted_resume)
    );
    assert_eq!(
        server_state.tunnel_sessions.stats().await.total_sessions,
        usize::from(interrupted_resume)
    );
    std::eprintln!(
        "phase=preferred_duplicate_decided elapsed={:?} old_connection={old_id} canonical_connection={} old_close={:?} duplicate_close={:?} session_had_attached={interrupted_resume}",
        started.elapsed(),
        canonical.stable_id(),
        failed_connection.close_reason(),
        duplicate.close_reason()
    );

    server_state.tunnel_blocked.store(false, Ordering::SeqCst);
    let attached = attached.unwrap_or_else(|| {
        start_client(
            local.take().unwrap(),
            &client_state,
            client_home,
            initial.take().unwrap(),
            cancel.clone(),
        )
    });
    let payload = b"first-request-no-application-retry";
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let local_barrier = barrier.clone();
    let request = async {
        local_barrier.wait().await;
        tokio::time::timeout(Duration::from_secs(3), async {
            application.write_all(payload).await?;
            let mut got = vec![0; payload.len()];
            application.read_exact(&mut got).await?;
            anyhow::ensure!(got == payload, "payload changed or replayed");
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("first local request exceeded its bound")
        .and_then(|result| result)
    };
    let reverse = async {
        barrier.wait().await;
        if reverse_traffic {
            let endpoint = server_state.endpoint_handle();
            let mut echo = server_state
                .peer_connections
                .open_stream(
                    &endpoint.endpoint,
                    endpoint.generation,
                    &client.addr(),
                    std::str::from_utf8(BUILTIN_ECHO_ALPN)?,
                    mux::StreamActivity::Application,
                )
                .await?;
            let bytes = b"reverse-simultaneous";
            echo.send.write_all(bytes).await?;
            let mut got = vec![0; bytes.len()];
            echo.recv.read_exact(&mut got).await?;
            anyhow::ensure!(got == bytes, "reverse stream payload changed");
            echo.send.finish()?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let (request, reverse) = tokio::join!(
        request,
        tokio::time::timeout(Duration::from_secs(3), reverse)
    );
    let reverse = reverse
        .context("reverse request exceeded its bound")
        .and_then(|result| result);
    std::eprintln!(
        "phase=first_local_reply elapsed={:?} result={request:?} target_accepts={}",
        started.elapsed(),
        accepts.load(Ordering::SeqCst)
    );
    let converged = if reverse_traffic {
        let at_client = client_state.peer_connections.connection(server.id()).await;
        let at_server = server_state.peer_connections.connection(client.id()).await;
        at_client.is_some_and(|connection| {
            connection.side() == Side::Server && connection.stable_id() == canonical.stable_id()
        }) && at_server.is_some_and(|connection| {
            connection.side() == Side::Client && connection.stable_id() == duplicate.stable_id()
        }) && client_state.peer_connections.peer_count().await == 1
            && server_state.peer_connections.peer_count().await == 1
    } else {
        true
    };
    cancel.cancel();
    drop(application);
    let attach_result = tokio::time::timeout(Duration::from_secs(3), attached).await;
    std::eprintln!("attach_result={attach_result:?}");
    client.shutdown().await?;
    server.shutdown().await?;
    copying.abort();
    let _ = copying.await;
    tokio::time::timeout(Duration::from_secs(3), reverse_serving).await???;
    assert!(
        converged,
        "reverse traffic did not converge on the same preferred connection"
    );
    assert!(
        reverse.is_ok(),
        "reverse simultaneous request failed: {reverse:?}"
    );
    assert!(
        request.is_ok(),
        "a preferred duplicate after mux Ready ended the first local request: {request:?}; attach={attach_result:?}"
    );
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        1,
        "the request must start exactly one application connection"
    );
    Ok(())
}
