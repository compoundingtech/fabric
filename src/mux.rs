//! Connection multiplexing: exactly one QUIC connection per machine-pair, with
//! every logical socket carried as a QUIC bi-stream on it.
//!
//! Fabric previously opened one iroh connection per tunnel. That created many
//! independent path states and connection handles. This module keeps one
//! persistent multipath connection per peer. A [`PeerConnections`] manager
//! opens it on demand, caches it, and hands out streams. Each stream begins with a
//! [`MuxStreamHeader`] naming the target protocol, so the accepting side routes
//! the stream to the right exposure — subsuming the old per-ALPN dispatch into
//! per-stream routing. (The tunnel session id and resume offset ride in the
//! tunnel's own framing, so resume is unchanged.)
//!
//! The manager is keyed by peer id, so it works for an N-peer mesh, not just one
//! pair. The resumable offset+ACK tunnel framing rides each stream unchanged; a
//! connection drop re-opens the shared connection and re-attaches its streams,
//! which is rarer than per-tunnel drops because iroh multipath migrates paths
//! without dropping the connection.

use std::{
    collections::HashMap,
    fmt,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{Connection, RecvStream, SendStream, Side},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, mpsc},
};

/// The reserved ALPN for the multiplexed per-peer connection.
pub const MUX_ALPN: &[u8] = b"fabric/mux/1";
/// A diagnostic block that can clear without a config change.
pub(crate) const TEMPORARY_TUNNEL_BLOCK: &str = "fabric tunnel blocked";

/// Largest protocol name accepted in a stream header (ALPN-scale).
const MAX_PROTOCOL_LEN: usize = 255;
const MAX_RESPONSE_LEN: usize = 4096;
const OPEN_TIMEOUT: Duration = Duration::from_secs(3);

/// One admitted logical stream on the shared peer connection.
pub struct MuxStream {
    pub connection: Connection,
    pub send: SendStream,
    pub recv: RecvStream,
}

/// Whether opening this stream proves normal application activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamActivity {
    Application,
    Probe,
}

#[derive(Debug)]
struct StreamDenied(String);

impl fmt::Display for StreamDenied {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for StreamDenied {}

/// True when the peer admitted the connection but refused this logical stream.
pub(crate) fn is_stream_denied(error: &anyhow::Error) -> bool {
    error.downcast_ref::<StreamDenied>().is_some()
}

/// True when retrying cannot change this stream admission decision.
pub(crate) fn is_permanent_stream_denial(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<StreamDenied>()
        .is_some_and(|denied| denied.0 != TEMPORARY_TUNNEL_BLOCK)
}

/// The first bytes of every mux stream: which exposure it targets, replacing the
/// old per-ALPN dispatch with per-stream routing. Wire format:
/// `[u16 BE protocol_len][protocol utf8]`.
///
/// The header carries only the protocol; the tunnel session id (and resume
/// offset) already ride in the tunnel's own `Frame::Hello`, so the resumable
/// attach/resume framing sits on the stream unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MuxStreamHeader {
    pub protocol: String,
}

impl MuxStreamHeader {
    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
        }
    }

    /// Encode the header to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let proto = self.protocol.as_bytes();
        let mut out = Vec::with_capacity(2 + proto.len());
        out.extend_from_slice(&(proto.len() as u16).to_be_bytes());
        out.extend_from_slice(proto);
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
        Ok(Self { protocol })
    }
}

/// Admit a stream after its header passed the peer and service checks.
pub async fn write_ready(send: &mut SendStream) -> Result<()> {
    send.write_u8(0).await.context("write mux ready status")?;
    send.write_u16(0).await.context("write mux ready length")?;
    send.flush().await.context("flush mux ready status")?;
    Ok(())
}

/// Refuse only this stream. The shared connection stays usable.
pub async fn write_denied(send: &mut SendStream, reason: &str) -> Result<()> {
    let bytes = reason.as_bytes();
    let bytes = &bytes[..bytes.len().min(MAX_RESPONSE_LEN)];
    send.write_u8(1).await.context("write mux denial status")?;
    send.write_u16(bytes.len() as u16)
        .await
        .context("write mux denial length")?;
    send.write_all(bytes).await.context("write mux denial")?;
    send.finish().context("finish mux denial")?;
    Ok(())
}

async fn read_admission(recv: &mut RecvStream) -> Result<()> {
    let status = recv.read_u8().await.context("read mux admission status")?;
    let len = recv.read_u16().await.context("read mux admission length")? as usize;
    if len > MAX_RESPONSE_LEN {
        bail!("mux admission message too long: {len}");
    }
    let mut message = vec![0; len];
    recv.read_exact(&mut message)
        .await
        .context("read mux admission message")?;
    match status {
        0 if message.is_empty() => Ok(()),
        1 => Err(StreamDenied(String::from_utf8_lossy(&message).into_owned()).into()),
        _ => bail!("invalid mux admission status {status}"),
    }
}

/// One peer's cached shared connection.
#[derive(Debug)]
struct PeerConn {
    connection: Connection,
    generation: u64,
    last_application_activity: Option<Instant>,
}

/// Manages exactly one multipath QUIC connection per peer, opening streams on it.
#[derive(Debug)]
pub struct PeerConnections {
    local_id: EndpointId,
    conns: Mutex<HashMap<EndpointId, PeerConn>>,
    opened_tx: mpsc::UnboundedSender<Connection>,
}

impl PeerConnections {
    pub fn new(local_id: EndpointId, opened_tx: mpsc::UnboundedSender<Connection>) -> Self {
        Self {
            local_id,
            conns: Mutex::new(HashMap::new()),
            opened_tx,
        }
    }

    /// Open a mux stream to `peer_addr`'s exposure `protocol`, reusing the peer's
    /// shared connection (opening it if needed, re-opening it once if the cached
    /// one has died). Returns the stream with its header already written, ready
    /// for the tunnel framing.
    pub async fn open_stream(
        &self,
        endpoint: &Endpoint,
        generation: u64,
        peer_addr: &EndpointAddr,
        protocol: &str,
        activity: StreamActivity,
    ) -> Result<MuxStream> {
        let header = MuxStreamHeader::new(protocol.to_string());
        let mut last_error = None;
        for _ in 0..4 {
            let connection = self
                .get_or_open(endpoint, generation, peer_addr, activity)
                .await?;
            match self.open_on(&connection, &header).await {
                Ok(stream) => return Ok(stream),
                Err(error) => {
                    if is_stream_denied(&error) {
                        return Err(error);
                    }
                    self.forget_if(peer_addr.id, connection.stable_id()).await;
                    last_error = Some(error);
                    tokio::task::yield_now().await;
                }
            }
        }
        Err(last_error.expect("a mux stream attempt ran"))
            .context("open mux stream after reconnect")
    }

    async fn open_on(
        &self,
        connection: &Connection,
        header: &MuxStreamHeader,
    ) -> Result<MuxStream> {
        tokio::time::timeout(OPEN_TIMEOUT, async {
            let (mut send, mut recv) = connection
                .open_bi()
                .await
                .context("open_bi on mux connection")?;
            header.write(&mut send).await?;
            read_admission(&mut recv).await?;
            Ok(MuxStream {
                connection: connection.clone(),
                send,
                recv,
            })
        })
        .await
        .context("mux stream received no admission answer within 3 seconds")?
    }

    /// Get the peer's cached connection, or open a fresh mux connection.
    async fn get_or_open(
        &self,
        endpoint: &Endpoint,
        generation: u64,
        peer_addr: &EndpointAddr,
        activity: StreamActivity,
    ) -> Result<Connection> {
        let mut conns = self.conns.lock().await;
        if let Some(existing) = conns.get_mut(&peer_addr.id)
            && existing.generation == generation
            && existing.connection.close_reason().is_none()
        {
            if activity == StreamActivity::Application {
                existing.last_application_activity = Some(Instant::now());
            }
            return Ok(existing.connection.clone());
        }
        let connection = endpoint
            .connect(peer_addr.clone(), MUX_ALPN)
            .await
            .context("connect mux connection")?;
        conns.insert(
            peer_addr.id,
            PeerConn {
                connection: connection.clone(),
                generation,
                last_application_activity: (activity == StreamActivity::Application)
                    .then(Instant::now),
            },
        );
        let _ = self.opened_tx.send(connection.clone());
        Ok(connection)
    }

    async fn forget_if(&self, peer: EndpointId, stable_id: usize) {
        let mut connections = self.conns.lock().await;
        if connections
            .get(&peer)
            .is_some_and(|current| current.connection.stable_id() == stable_id)
        {
            connections.remove(&peer);
        }
    }

    /// Make one peer use a fresh multipath connection on its next stream.
    pub async fn redial(&self, peer: EndpointId, reason: &[u8]) -> bool {
        let removed = self.conns.lock().await.remove(&peer);
        if let Some(removed) = removed {
            removed.connection.close(0u32.into(), reason);
            return true;
        }
        false
    }

    /// Cache an accepted peer connection for traffic in the reverse direction.
    pub async fn register_incoming(&self, connection: &Connection, generation: u64) {
        let peer = connection.remote_id();
        let mut conns = self.conns.lock().await;
        let replace = match conns.get(&peer) {
            None => true,
            Some(current)
                if current.generation != generation
                    || current.connection.close_reason().is_some() =>
            {
                true
            }
            Some(current) => {
                current.connection.side() == Side::Server
                    || (self.local_id > peer && current.connection.side() == Side::Client)
            }
        };
        if replace {
            if let Some(old) = conns.remove(&peer) {
                old.connection
                    .close(0u32.into(), b"duplicate mux connection");
            }
            conns.insert(
                peer,
                PeerConn {
                    connection: connection.clone(),
                    generation,
                    last_application_activity: None,
                },
            );
        } else {
            connection.close(0u32.into(), b"duplicate mux connection");
        }
    }

    /// Record an admitted inbound application stream.
    pub async fn note_application_activity(&self, peer: EndpointId) {
        if let Some(connection) = self.conns.lock().await.get_mut(&peer) {
            connection.last_application_activity = Some(Instant::now());
        }
    }

    /// Recent application traffic makes a separate liveness probe redundant.
    pub async fn recently_active(&self, peer: EndpointId, within: Duration) -> bool {
        self.conns
            .lock()
            .await
            .get(&peer)
            .and_then(|connection| connection.last_application_activity)
            .is_some_and(|last| last.elapsed() <= within)
    }

    /// Return the live shared connection for path inspection.
    pub async fn connection(&self, peer: EndpointId) -> Option<Connection> {
        self.conns.lock().await.get(&peer).and_then(|current| {
            (current.connection.close_reason().is_none()).then(|| current.connection.clone())
        })
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
        let header = MuxStreamHeader::new("pty-view");
        let bytes = header.encode();
        // len(2) + "pty-view"(8) = 10.
        assert_eq!(bytes.len(), 2 + 8);
        assert_eq!(&bytes[0..2], &8u16.to_be_bytes());
    }

    #[test]
    fn only_the_debug_tunnel_block_is_a_retryable_admission_denial() {
        let temporary = anyhow::Error::new(StreamDenied(TEMPORARY_TUNNEL_BLOCK.to_string()));
        assert!(is_stream_denied(&temporary));
        assert!(!is_permanent_stream_denial(&temporary));

        let acl = anyhow::Error::new(StreamDenied(
            "peer not permitted for service \"web\"".to_string(),
        ));
        assert!(is_stream_denied(&acl));
        assert!(is_permanent_stream_denial(&acl));
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
                        let _ = write_ready(&mut send).await;
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
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = PeerConnections::new(client.id(), opened_tx);

        let probe = manager
            .open_stream(&client, 0, &server_addr, "probe", StreamActivity::Probe)
            .await?;
        drop(probe);
        assert!(
            !manager
                .recently_active(server_addr.id, Duration::from_secs(1))
                .await,
            "a health probe must not count as application traffic"
        );

        // Open two logical streams with different protocols on the same peer.
        for proto in ["pty-view", "demo-http"] {
            let stream = manager
                .open_stream(&client, 0, &server_addr, proto, StreamActivity::Application)
                .await?;
            let mut send = stream.send;
            let mut recv = stream.recv;
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
        assert_eq!(seen.len(), 3);
        assert!(seen.iter().any(|h| h.protocol == "pty-view"));
        assert!(seen.iter().any(|h| h.protocol == "demo-http"));
        drop(seen);

        assert!(
            manager
                .recently_active(server_addr.id, Duration::from_secs(1))
                .await
        );
        assert!(manager.redial(server_addr.id, b"test degradation").await);
        let stream = manager
            .open_stream(
                &client,
                0,
                &server_addr,
                "after-redial",
                StreamActivity::Application,
            )
            .await?;
        drop(stream);
        assert_eq!(connections.load(Ordering::SeqCst), 2);

        router.shutdown().await?;
        client.close().await;
        Ok(())
    }
}
