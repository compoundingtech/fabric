//! Validates the path-quality instrument against a real iroh connection: two
//! endpoints on one machine, and `observe_paths` reports the connection's
//! multipath state (at least one path, a selected path, per-path RTT, and the
//! direct/relay class) — the per-path data the daemon's snapshot cannot see.

use std::time::Duration;

use anyhow::Result;
use fabric::pathwatch::observe_paths;
use iroh::{
    Endpoint,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};

const ALPN: &[u8] = b"fabric/pathwatch/test/0";

#[derive(Debug, Clone)]
struct Echo;

impl ProtocolHandler for Echo {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        tokio::io::copy(&mut recv, &mut send).await?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observe_paths_reports_selected_path_with_rtt() -> Result<()> {
    let server = Endpoint::bind(presets::N0).await?;
    let router = Router::builder(server).accept(ALPN, Echo).spawn();
    router.endpoint().online().await;
    let server_addr = router.endpoint().addr();

    let client = Endpoint::bind(presets::N0).await?;
    let conn = client.connect(server_addr, ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    // Exchange a few nonces so a path is established and its RTT is sampled.
    for n in 0..8u64 {
        send.write_all(&n.to_be_bytes()).await?;
        let mut buf = [0u8; 8];
        recv.read_exact(&mut buf).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let observed = observe_paths(&conn);
    assert!(
        !observed.paths.is_empty(),
        "expected at least one observed path"
    );
    assert!(
        observed.paths.iter().any(|p| p.selected),
        "expected a selected path, got {:?}",
        observed.paths
    );
    assert!(
        observed.selected.is_some(),
        "aggregate selected should be set"
    );
    assert!(
        observed.min_rtt.is_some(),
        "expected a measurable min RTT across paths"
    );
    // On one machine a direct (IP) path should be available.
    assert!(
        observed.paths.iter().any(|p| p.class == "direct"),
        "expected a direct path on loopback, got {:?}",
        observed.paths
    );

    conn.close(0u32.into(), b"done");
    router.shutdown().await?;
    client.close().await;
    Ok(())
}
