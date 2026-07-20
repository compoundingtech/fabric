//! Connection multiplexing: exactly one QUIC connection per machine-pair, with
//! every logical socket carried as a QUIC bi-stream on it.
//!
//! Today fabric opens one iroh connection per tunnel (N per peer, each itself
//! multipath), so there are N independent path-states to health-check and N
//! connection handles to leak. This module consolidates to one persistent,
//! multipath connection per peer: a [`PeerConnections`] manager opens it on
//! demand, caches it, and hands out streams. Each stream begins with a
//! [`MuxStreamHeader`] naming the target protocol and the tunnel session id, so
//! the accepting side routes the stream to the right exposure — subsuming the old
//! per-ALPN dispatch into per-stream routing.
//!
//! The manager is keyed by peer id, so it works for an N-peer mesh, not just one
//! pair. The resumable offset+ACK tunnel framing rides each stream unchanged; a
//! connection drop re-opens the shared connection and re-attaches its streams,
//! which is rarer than per-tunnel drops because iroh multipath migrates paths
//! without dropping the connection.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use tokio::sync::Mutex;

/// The reserved ALPN for the multiplexed per-peer connection.
pub const MUX_ALPN: &[u8] = b"fabric/mux/1";

/// Largest protocol name accepted in a stream header (ALPN-scale).
const MAX_PROTOCOL_LEN: usize = 255;
/// Tunnel session id length, matching `tunnel::TunnelSessionId`.
pub const SESSION_ID_LEN: usize = 16;

/// The first bytes of every mux stream: which exposure it targets and the tunnel
/// session it belongs to (for resume). Wire format:
/// `[u16 BE protocol_len][protocol utf8][16 bytes session_id]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MuxStreamHeader {
    pub protocol: String,
    pub session_id: [u8; SESSION_ID_LEN],
}

impl MuxStreamHeader {
    pub fn new(protocol: impl Into<String>, session_id: [u8; SESSION_ID_LEN]) -> Self {
        Self {
            protocol: protocol.into(),
            session_id,
        }
    }

    /// Encode the header to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let proto = self.protocol.as_bytes();
        let mut out = Vec::with_capacity(2 + proto.len() + SESSION_ID_LEN);
        out.extend_from_slice(&(proto.len() as u16).to_be_bytes());
        out.extend_from_slice(proto);
        out.extend_from_slice(&self.session_id);
        out
    }

    /// Write the header to a QUIC send stream.
    pub async fn write(&self, send: &mut SendStream) -> Result<()> {
        if self.protocol.is_empty() {
            bail!("mux stream protocol cannot be empty");
        }
        if self.protocol.len() > MAX_PROTOCOL_LEN {
            bail!("mux stream protocol too long");
        }
        send.write_all(&self.encode())
            .await
            .context("write mux stream header")?;
        Ok(())
    }

    /// Read a header from a QUIC recv stream.
    pub async fn read(recv: &mut RecvStream) -> Result<Self> {
        let mut len_buf = [0u8; 2];
        recv.read_exact(&mut len_buf)
            .await
            .context("read mux header length")?;
        let len = u16::from_be_bytes(len_buf) as usize;
        if len == 0 {
            bail!("mux stream protocol cannot be empty");
        }
        if len > MAX_PROTOCOL_LEN {
            bail!("mux stream protocol length {len} exceeds {MAX_PROTOCOL_LEN}");
        }
        let mut proto = vec![0u8; len];
        recv.read_exact(&mut proto)
            .await
            .context("read mux header protocol")?;
        let protocol = String::from_utf8(proto).context("mux protocol is not utf8")?;
        let mut session_id = [0u8; SESSION_ID_LEN];
        recv.read_exact(&mut session_id)
            .await
            .context("read mux header session id")?;
        Ok(Self {
            protocol,
            session_id,
        })
    }
}

/// One peer's cached shared connection.
struct PeerConn {
    connection: Connection,
}

/// Manages exactly one multipath QUIC connection per peer, opening streams on it.
#[derive(Default)]
pub struct PeerConnections {
    conns: Mutex<HashMap<EndpointId, PeerConn>>,
}

impl PeerConnections {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a mux stream to `peer_addr`'s exposure `protocol` for `session_id`,
    /// reusing the peer's shared connection (opening it if needed, re-opening it
    /// once if the cached one has died). Returns the stream with its header
    /// already written, ready for the tunnel framing.
    pub async fn open_stream(
        &self,
        endpoint: &Endpoint,
        peer_addr: &EndpointAddr,
        protocol: &str,
        session_id: [u8; SESSION_ID_LEN],
    ) -> Result<(SendStream, RecvStream)> {
        let header = MuxStreamHeader::new(protocol.to_string(), session_id);

        // First attempt on the cached (or freshly opened) connection.
        let connection = self.get_or_open(endpoint, peer_addr).await?;
        match self.open_on(&connection, &header).await {
            Ok(streams) => Ok(streams),
            Err(_first) => {
                // The cached connection was likely dead; drop it, re-open once.
                self.forget(peer_addr.id).await;
                let connection = self.get_or_open(endpoint, peer_addr).await?;
                self.open_on(&connection, &header)
                    .await
                    .context("open mux stream after reconnect")
            }
        }
    }

    async fn open_on(
        &self,
        connection: &Connection,
        header: &MuxStreamHeader,
    ) -> Result<(SendStream, RecvStream)> {
        let (mut send, recv) = connection
            .open_bi()
            .await
            .context("open_bi on mux connection")?;
        header.write(&mut send).await?;
        Ok((send, recv))
    }

    /// Get the peer's cached connection, or open a fresh mux connection.
    async fn get_or_open(
        &self,
        endpoint: &Endpoint,
        peer_addr: &EndpointAddr,
    ) -> Result<Connection> {
        {
            let conns = self.conns.lock().await;
            if let Some(existing) = conns.get(&peer_addr.id)
                && existing.connection.close_reason().is_none()
            {
                return Ok(existing.connection.clone());
            }
        }
        let connection = endpoint
            .connect(peer_addr.clone(), MUX_ALPN)
            .await
            .context("connect mux connection")?;
        let mut conns = self.conns.lock().await;
        conns.insert(
            peer_addr.id,
            PeerConn {
                connection: connection.clone(),
            },
        );
        Ok(connection)
    }

    async fn forget(&self, peer: EndpointId) {
        self.conns.lock().await.remove(&peer);
    }

    /// Number of peers with a cached connection (diagnostics).
    pub async fn peer_count(&self) -> usize {
        self.conns.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{
        endpoint::presets,
        protocol::{AcceptError, ProtocolHandler, Router},
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn header_round_trips_through_bytes() {
        let header = MuxStreamHeader::new("pty-view", [7u8; SESSION_ID_LEN]);
        let bytes = header.encode();
        // len(8) + "pty-view"(8) + 16 = 26.
        assert_eq!(bytes.len(), 2 + 8 + SESSION_ID_LEN);
        assert_eq!(&bytes[0..2], &8u16.to_be_bytes());
    }

    /// A mux server that reads each stream's header and echoes the rest, counting
    /// how many distinct connections it accepted.
    #[derive(Debug, Clone)]
    struct MuxEcho {
        connections: Arc<AtomicUsize>,
        headers: Arc<Mutex<Vec<MuxStreamHeader>>>,
    }

    impl ProtocolHandler for MuxEcho {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            self.connections.fetch_add(1, Ordering::SeqCst);
            loop {
                let (mut send, mut recv) = match connection.accept_bi().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let headers = self.headers.clone();
                tokio::spawn(async move {
                    if let Ok(header) = MuxStreamHeader::read(&mut recv).await {
                        headers.lock().await.push(header);
                        let _ = tokio::io::copy(&mut recv, &mut send).await;
                        let _ = send.finish();
                    }
                });
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multiple_streams_ride_one_shared_connection() -> Result<()> {
        let connections = Arc::new(AtomicUsize::new(0));
        let headers = Arc::new(Mutex::new(Vec::new()));
        let server_ep = Endpoint::bind(presets::N0).await?;
        let router = Router::builder(server_ep)
            .accept(
                MUX_ALPN,
                MuxEcho {
                    connections: connections.clone(),
                    headers: headers.clone(),
                },
            )
            .spawn();
        router.endpoint().online().await;
        let server_addr = router.endpoint().addr();

        let client = Endpoint::bind(presets::N0).await?;
        let manager = PeerConnections::new();

        // Open two logical streams with different protocols + session ids.
        for (proto, sid) in [("pty-view", [1u8; 16]), ("demo-http", [2u8; 16])] {
            let (mut send, mut recv) = manager
                .open_stream(&client, &server_addr, proto, sid)
                .await?;
            send.write_all(b"ping").await?;
            send.finish()?;
            let mut buf = [0u8; 4];
            recv.read_exact(&mut buf).await?;
            assert_eq!(&buf, b"ping");
        }

        // Both streams rode ONE shared connection, and the manager cached one peer.
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "both streams must ride a single shared connection"
        );
        assert_eq!(manager.peer_count().await, 1);
        let seen = headers.lock().await;
        assert_eq!(seen.len(), 2);
        assert!(seen.iter().any(|h| h.protocol == "pty-view"));
        assert!(seen.iter().any(|h| h.protocol == "demo-http"));

        router.shutdown().await?;
        client.close().await;
        Ok(())
    }
}
