use anyhow::Result;
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};

pub mod config;
pub mod control;
pub mod daemon;
pub mod service;
pub mod shell;
mod tunnel;

const SPIKE_ALPN: &[u8] = b"fabric/spike/echo/0";

pub fn version_string() -> String {
    format!(
        "{}+{}",
        env!("CARGO_PKG_VERSION"),
        option_env!("FABRIC_BUILD_SHA").unwrap_or("unknown")
    )
}

pub async fn iroh_spike_round_trip(payload: &[u8]) -> Result<Vec<u8>> {
    let router = start_spike_accept_side().await?;
    router.endpoint().online().await;

    let response = spike_connect_side(router.endpoint().addr(), payload).await?;

    router.shutdown().await?;
    Ok(response)
}

async fn start_spike_accept_side() -> Result<Router> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    Ok(Router::builder(endpoint)
        .accept(SPIKE_ALPN, SpikeEcho)
        .spawn())
}

async fn spike_connect_side(addr: EndpointAddr, payload: &[u8]) -> Result<Vec<u8>> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    let conn = endpoint.connect(addr, SPIKE_ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    send.write_all(payload).await?;
    send.finish()?;

    let response = recv.read_to_end(payload.len() + 1024).await?;
    conn.close(0u32.into(), b"done");
    endpoint.close().await;
    Ok(response)
}

#[derive(Debug, Clone)]
struct SpikeEcho;

impl ProtocolHandler for SpikeEcho {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        tokio::io::copy(&mut recv, &mut send).await?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spike_round_trips_bytes_over_iroh() -> Result<()> {
        let payload = b"fabric proves iroh moves bytes";
        let response = iroh_spike_round_trip(payload).await?;
        assert_eq!(response, payload);
        Ok(())
    }
}
