//! A closed peer connection is recorded, with its reason.
//!
//! Every "peer is offline" that follows a lost connection starts with the
//! connection closing, and a closure nobody recorded left an operator unable to
//! say whether the peer went away, the transport timed out, or this side
//! replaced the connection on purpose. This test holds the daemon to writing
//! one validation-log line per closed connection.
//!
//! It lives in its own test target on purpose. It reads the process-wide log,
//! and a subscriber installed for one test in a shared test binary sees events
//! from, and is missed by, the tests running beside it.

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result};
use fabric::{
    config::{FabricHome, PeerBook},
    daemon::FabricNode,
};
use tempfile::TempDir;

#[derive(Clone)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for Buffer {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
    type Writer = Buffer;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn trust(home: &FabricHome, node: &FabricNode, other: &FabricNode, name: &str) -> Result<()> {
    let mut peers = PeerBook::load(home)?;
    peers.add_with_allow(
        other.id(),
        Some(name.to_string()),
        Some(other.addr()),
        Some(vec!["echo".to_string()]),
    );
    peers.save(home)?;
    node.state().reload_peers().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closed_peer_connection_is_logged_with_its_reason() -> Result<()> {
    let buffer = Buffer(Arc::new(Mutex::new(Vec::new())));
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .with_writer(buffer.clone())
            .finish(),
    )
    .context("another subscriber is already installed in this test binary")?;

    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start(server_home.clone()).await?;
    let client = FabricNode::start(client_home.clone()).await?;
    trust(&server_home, &server, &client, "client").await?;
    trust(&client_home, &client, &server, "server").await?;

    // The client dials the server, which opens the connection.
    client.ping("server").await?;
    let server_id = server.id().to_string();

    // The server goes away. The client's connection to it closes.
    server.shutdown().await?;
    let line = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let text = String::from_utf8_lossy(&buffer.0.lock().unwrap()).to_string();
            if let Some(line) = text
                .lines()
                .find(|line| line.contains("peer_connection_closed") && line.contains(&server_id))
            {
                return line.to_string();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("the client never logged that its connection to the server closed")?;

    assert!(
        line.contains("we_dialled=true"),
        "the log does not say which side dialled: {line}"
    );
    assert!(
        line.contains("age_ms="),
        "the log does not say how long the connection lived: {line}"
    );
    let reason = line
        .split("reason=")
        .nth(1)
        .context("the log line has no reason")?;
    assert!(
        reason.starts_with("closed by peer"),
        "a peer that shut down should read as closed by the peer: {line}"
    );
    client.shutdown().await?;
    Ok(())
}
