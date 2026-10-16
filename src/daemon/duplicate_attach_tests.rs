//! Force the mux Ready / tunnel Hello boundary using existing entry points.
//! No sleeps or production injection hooks choose the replacement ordering.
use super::*;

#[tokio::test]
async fn preferred_duplicate_between_ready_and_hello_keeps_the_first_local_request() -> Result<()> {
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
    book.add(server.id(), Some("server".into()), Some(server.addr()));
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
    let old_id = initial.connection.stable_id();
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

    // This real reverse QUIC connection is preferred at both ends. Exchange
    // the existing generation frame, then register it with the same admission
    // entry point used by the daemon. No tunnel Hello has been sent yet.
    let reverse_endpoint = server_state.endpoint_handle();
    let duplicate = reverse_endpoint
        .endpoint
        .connect(client.addr(), mux::MUX_ALPN)
        .await?;
    assert_eq!(duplicate.side(), Side::Client);
    let exchange: Result<u64> = async {
        let (mut send, mut recv) = duplicate.open_bi().await?;
        send.write_u64(reverse_endpoint.generation).await?;
        send.finish()?;
        Ok(recv.read_u64().await?)
    }
    .await;
    let reverse_serving = match exchange {
        Ok(client_generation) => {
            server_state
                .peer_connections
                .register_incoming(&duplicate, reverse_endpoint.generation, client_generation)
                .await;
            Some(tokio::spawn(handle_mux_connection(
                duplicate.clone(),
                server_state.clone(),
                false,
            )))
        }
        // A valid fix may retain the admitted connection and reject the new
        // duplicate. The request must work whichever connection is retained.
        Err(error) if duplicate.close_reason().is_some() => {
            std::eprintln!("reverse generation exchange ended: {error:#}");
            None
        }
        Err(error) => return Err(error),
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let changed = client_state
                .peer_connections
                .connection(server.id())
                .await
                .is_some_and(|connection| connection.stable_id() != old_id);
            if changed || duplicate.close_reason().is_some() {
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
    assert_eq!(accepts.load(Ordering::SeqCst), 0);
    assert_eq!(server_state.tunnel_sessions.stats().await.total_sessions, 0);
    std::eprintln!(
        "phase=preferred_duplicate_decided elapsed={:?} old_connection={old_id} canonical_connection={} old_close={:?} duplicate_close={:?} hello_started=false target_accepts=0",
        started.elapsed(),
        canonical.stable_id(),
        initial.connection.close_reason(),
        duplicate.close_reason()
    );

    let (mut application, local) = UnixStream::pair()?;
    let cancel = CancellationToken::new();
    let attached = tokio::spawn(tunnel::run_client_connection_with_initial(
        local,
        client_state.endpoint_rx(),
        client_state.peer_connections.clone(),
        client_home,
        "server".into(),
        b"work/0".to_vec(),
        cancel.clone(),
        client_state.tunnel_drop_tx.subscribe(),
        None,
        initial,
    ));
    let payload = b"first-request-no-application-retry";
    let request: Result<()> = tokio::time::timeout(Duration::from_secs(3), async {
        application.write_all(payload).await?;
        let mut got = vec![0; payload.len()];
        application.read_exact(&mut got).await?;
        anyhow::ensure!(got == payload, "payload changed or replayed");
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("first local request exceeded its bound")
    .and_then(|result| result);
    std::eprintln!(
        "phase=first_local_reply elapsed={:?} result={request:?} target_accepts={}",
        started.elapsed(),
        accepts.load(Ordering::SeqCst)
    );
    cancel.cancel();
    drop(application);
    let attach_result = tokio::time::timeout(Duration::from_secs(3), attached).await;
    std::eprintln!("attach_result={attach_result:?}");
    client.shutdown().await?;
    server.shutdown().await?;
    copying.abort();
    let _ = copying.await;
    if let Some(serving) = reverse_serving {
        tokio::time::timeout(Duration::from_secs(3), serving).await???;
    }
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
