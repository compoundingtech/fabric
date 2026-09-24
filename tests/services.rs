//! A service built on the base network through its public interface alone.
//!
//! The built-in services live in their own crates and reach the daemon only
//! through `fabric-service-api`. These tests hold the daemon to that: a service
//! it has never heard of, registered through the same interface, is served the
//! authenticated peer id, gated by the same grant, and told about a refusal in
//! its own words.

use std::time::Duration;

use anyhow::{Context, Result};
use fabric::{
    config::{FabricHome, PeerBook},
    daemon::{DaemonOptions, FabricNode},
    services::{self, BoxFuture, Notice, PeerStream, Protocol, Service, Services},
};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const WHOAMI_PROTOCOL: &str = "test/whoami/0";
const WHOAMI: &[Protocol] = &[Protocol {
    alpn: WHOAMI_PROTOCOL.as_bytes(),
    resumable: false,
    accept_event: "whoami_accept",
}];

/// Answers every stream with the peer id the base network handed it.
struct Whoami;

impl Service for Whoami {
    fn name(&self) -> &'static str {
        "whoami"
    }

    fn protocols(&self) -> &'static [Protocol] {
        WHOAMI
    }

    fn serve(&self, stream: PeerStream) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let PeerStream {
                peer, mut write, ..
            } = stream;
            write.write_all(peer.as_bytes()).await?;
            write.shutdown().await?;
            Ok(())
        })
    }

    fn notice(&self, notice: &Notice<'_>) -> Option<Vec<u8>> {
        match notice {
            Notice::Refused { error } => Some(format!("refused: {error}").into_bytes()),
            _ => None,
        }
    }
}

fn with_whoami(home: &FabricHome) -> Services {
    services::builtin(home).with(Whoami)
}

async fn start(dir: &TempDir, services: Services) -> Result<(FabricHome, FabricNode)> {
    let home = FabricHome::new(dir.path());
    let node =
        FabricNode::start_with_services(home.clone(), DaemonOptions::default(), services).await?;
    Ok((home, node))
}

async fn trust(
    home: &FabricHome,
    node: &FabricNode,
    other: &FabricNode,
    name: &str,
    allow: &[&str],
) -> Result<()> {
    let mut peers = PeerBook::load(home)?;
    peers.add_with_allow(
        other.id(),
        Some(name.to_string()),
        Some(other.addr()),
        Some(allow.iter().map(|service| (*service).to_string()).collect()),
    );
    peers.save(home)?;
    node.state().reload_peers().await?;
    Ok(())
}

/// Ask `server`'s whoami service, through `client`'s local socket.
async fn ask(client: &FabricNode) -> Result<String> {
    let socket = client.dial("server", WHOAMI_PROTOCOL).await?;
    let mut stream = tokio::net::UnixStream::connect(&socket).await?;
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), stream.read_to_end(&mut reply))
        .await
        .context("the whoami service did not answer within 20 seconds")??;
    Ok(String::from_utf8(reply)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_service_built_on_the_public_interface_hears_the_proven_peer() -> Result<()> {
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let (server_home, server) = start(
        &server_dir,
        with_whoami(&FabricHome::new(server_dir.path())),
    )
    .await?;
    let (client_home, client) = start(
        &client_dir,
        with_whoami(&FabricHome::new(client_dir.path())),
    )
    .await?;
    trust(&server_home, &server, &client, "client", &["whoami"]).await?;
    trust(&client_home, &client, &server, "server", &[]).await?;

    assert_eq!(ask(&client).await?, client.id().to_string());

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_base_network_refuses_a_service_the_peer_was_not_granted() -> Result<()> {
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let (server_home, server) = start(
        &server_dir,
        with_whoami(&FabricHome::new(server_dir.path())),
    )
    .await?;
    let (client_home, client) = start(
        &client_dir,
        with_whoami(&FabricHome::new(client_dir.path())),
    )
    .await?;
    trust(&server_home, &server, &client, "client", &["echo"]).await?;
    trust(&client_home, &client, &server, "server", &[]).await?;

    let reply = ask(&client).await?;
    assert!(
        reply.starts_with("refused: ") && reply.contains("whoami"),
        "the refusal did not reach the service's local command in its words: {reply:?}"
    );
    assert!(
        !reply.contains(&client.id().to_string()),
        "the service ran for a peer without its grant: {reply:?}"
    );

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemon_does_not_answer_a_service_it_was_not_given() -> Result<()> {
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let (server_home, server) = start(
        &server_dir,
        services::builtin(&FabricHome::new(server_dir.path())),
    )
    .await?;
    let (client_home, client) = start(
        &client_dir,
        with_whoami(&FabricHome::new(client_dir.path())),
    )
    .await?;
    trust(&server_home, &server, &client, "client", &["whoami"]).await?;
    trust(&client_home, &client, &server, "server", &[]).await?;

    let reply = ask(&client).await.unwrap_or_default();
    assert!(
        !reply.contains(&client.id().to_string()),
        "a daemon without the service answered it: {reply:?}"
    );

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}
