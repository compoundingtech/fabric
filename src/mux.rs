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
    collections::{HashMap, HashSet},
    fmt,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{
        ConnectError, ConnectingError, Connection, ConnectionError, ReadError, RecvStream,
        SendStream, Side, TransportErrorCode, WriteError,
    },
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
const OPEN_ATTEMPTS: usize = 4;
const DUPLICATE_OPEN_ATTEMPTS: usize = 8;
const DUPLICATE_RETRY_DELAY: Duration = Duration::from_millis(100);
const LEGACY_MUX_REPROBE_INTERVAL: Duration = Duration::from_secs(60);
const VALIDATION_LOG_TARGET: &str = "fabric::validation";
const NO_APPLICATION_PROTOCOL_ALERT: u8 = 0x78;
const DUPLICATE_CONNECTION_REASON: &[u8] = b"duplicate mux connection";
const STALE_GENERATION_REASON: &[u8] = b"endpoint generation changed";

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

/// True only when the peer explicitly rejects the offered mux ALPN.
fn is_mux_unsupported(error: &anyhow::Error) -> bool {
    let Some(error) = error.downcast_ref::<ConnectError>() else {
        return false;
    };
    matches!(
        error,
        ConnectError::Connecting {
            source:
                ConnectingError::ConnectionError {
                    source,
                    ..
                },
            ..
        } if is_no_application_protocol(source)
    )
}

fn is_no_application_protocol(error: &ConnectionError) -> bool {
    matches!(
        error,
        ConnectionError::ConnectionClosed(close)
            if close.error_code
                == TransportErrorCode::crypto(NO_APPLICATION_PROTOCOL_ALERT)
    )
}

fn is_duplicate_connection(error: &anyhow::Error) -> bool {
    fn is_duplicate_close(error: &ConnectionError) -> bool {
        matches!(
            error,
            ConnectionError::ApplicationClosed(close)
                if close.reason.as_ref() == DUPLICATE_CONNECTION_REASON
        )
    }

    error.chain().any(|source| {
        source
            .downcast_ref::<ConnectionError>()
            .is_some_and(is_duplicate_close)
            || source
                .downcast_ref::<ReadError>()
                .is_some_and(|error| {
                    matches!(error, ReadError::ConnectionLost(reason) if is_duplicate_close(reason))
                })
            || source
                .downcast_ref::<WriteError>()
                .is_some_and(|error| {
                    matches!(error, WriteError::ConnectionLost(reason) if is_duplicate_close(reason))
                })
    })
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

#[derive(Debug, Clone, Copy)]
struct LegacyFallback {
    retry_after: Instant,
    uses: u64,
}

/// Manages exactly one multipath QUIC connection per peer, opening streams on it.
#[derive(Debug)]
pub struct PeerConnections {
    local_id: EndpointId,
    conns: Mutex<HashMap<EndpointId, PeerConn>>,
    legacy_notices: Mutex<HashSet<(EndpointId, u64)>>,
    legacy_fallbacks: Mutex<HashMap<(EndpointId, u64), LegacyFallback>>,
    opened_tx: mpsc::UnboundedSender<Connection>,
    #[cfg(test)]
    mux_connect_attempts: std::sync::atomic::AtomicUsize,
}

impl PeerConnections {
    pub fn new(local_id: EndpointId, opened_tx: mpsc::UnboundedSender<Connection>) -> Self {
        Self {
            local_id,
            conns: Mutex::new(HashMap::new()),
            legacy_notices: Mutex::new(HashSet::new()),
            legacy_fallbacks: Mutex::new(HashMap::new()),
            opened_tx,
            #[cfg(test)]
            mux_connect_attempts: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Open a mux stream to `peer_addr`'s exposure `protocol`, reusing the peer's
    /// shared connection (opening it if needed and replacing a dead cached one).
    /// A duplicate close gets a short bounded convergence window because a peer
    /// can still hold the old canonical connection during an endpoint recycle.
    /// Returns the stream with its header already written for tunnel framing.
    pub async fn open_stream(
        &self,
        endpoint: &Endpoint,
        generation: u64,
        peer_addr: &EndpointAddr,
        protocol: &str,
        activity: StreamActivity,
    ) -> Result<MuxStream> {
        let header = MuxStreamHeader::new(protocol.to_string());
        let mut attempts = 0usize;
        let last_error = loop {
            attempts += 1;
            let connection = match self
                .get_or_open(endpoint, generation, peer_addr, protocol, activity)
                .await
            {
                Ok(Some(connection)) => connection,
                Ok(None) => {
                    self.note_legacy_use(peer_addr.id, generation, protocol)
                        .await;
                    return self.open_legacy(endpoint, peer_addr, protocol).await;
                }
                Err(error) => return Err(error),
            };
            self.clear_legacy_retry(peer_addr.id, generation).await;
            match self.open_on(&connection, &header).await {
                Ok(stream) => return Ok(stream),
                Err(error) => {
                    if is_stream_denied(&error) {
                        return Err(error);
                    }
                    self.forget_if(peer_addr.id, connection.stable_id()).await;
                    let duplicate = is_duplicate_connection(&error);
                    if duplicate && attempts < DUPLICATE_OPEN_ATTEMPTS {
                        tokio::time::sleep(DUPLICATE_RETRY_DELAY).await;
                        continue;
                    }
                    if attempts >= OPEN_ATTEMPTS {
                        break error;
                    }
                    tokio::task::yield_now().await;
                }
            }
        };
        Err(last_error).context("open mux stream after reconnect")
    }

    /// Open one direct legacy ALPN connection without caching a downgrade.
    async fn open_legacy(
        &self,
        endpoint: &Endpoint,
        peer_addr: &EndpointAddr,
        protocol: &str,
    ) -> Result<MuxStream> {
        let connection = endpoint
            .connect(peer_addr.clone(), protocol.as_bytes())
            .await
            .with_context(|| format!("connect legacy protocol {protocol}"))?;
        let (send, recv) = connection
            .open_bi()
            .await
            .with_context(|| format!("open_bi on legacy protocol {protocol}"))?;
        Ok(MuxStream {
            connection,
            send,
            recv,
        })
    }

    async fn note_legacy_fallback(
        &self,
        peer: EndpointId,
        generation: u64,
        protocol: &str,
        reason: &anyhow::Error,
    ) {
        let key = (peer, generation);
        let retry_after = Instant::now() + LEGACY_MUX_REPROBE_INTERVAL;
        let mut fallbacks = self.legacy_fallbacks.lock().await;
        fallbacks.retain(|(logged_peer, logged_generation), _| {
            *logged_peer != peer || *logged_generation == generation
        });
        fallbacks
            .entry(key)
            .and_modify(|fallback| fallback.retry_after = retry_after)
            .or_insert(LegacyFallback {
                retry_after,
                uses: 0,
            });
        drop(fallbacks);

        let first_for_generation = {
            let mut notices = self.legacy_notices.lock().await;
            notices.retain(|(logged_peer, logged_generation)| {
                *logged_peer != peer || *logged_generation == generation
            });
            notices.insert((peer, generation))
        };
        if first_for_generation {
            tracing::warn!(
                target: VALIDATION_LOG_TARGET,
                event = "mux_legacy_fallback",
                peer = %peer.fmt_short(),
                endpoint_generation = generation,
                protocol,
                reprobe_after_seconds = LEGACY_MUX_REPROBE_INTERVAL.as_secs(),
                reason = %format!("{reason:#}"),
                "peer rejected the mux ALPN; using an uncached direct ALPN connection"
            );
        }
    }

    async fn note_legacy_use(&self, peer: EndpointId, generation: u64, protocol: &str) {
        let uses = {
            let mut fallbacks = self.legacy_fallbacks.lock().await;
            let Some(fallback) = fallbacks.get_mut(&(peer, generation)) else {
                return;
            };
            fallback.uses = fallback.uses.saturating_add(1);
            fallback.uses
        };
        if uses > 1 && uses.is_power_of_two() {
            tracing::info!(
                target: VALIDATION_LOG_TARGET,
                event = "mux_legacy_fallback_uses",
                peer = %peer.fmt_short(),
                endpoint_generation = generation,
                protocol,
                fallback_uses = uses,
                "legacy fallback remains active"
            );
        }
    }

    async fn mux_probe_deferred(&self, peer: EndpointId, generation: u64) -> bool {
        self.legacy_fallbacks
            .lock()
            .await
            .get(&(peer, generation))
            .is_some_and(|fallback| fallback.retry_after > Instant::now())
    }

    async fn clear_legacy_retry(&self, peer: EndpointId, generation: u64) {
        self.legacy_fallbacks
            .lock()
            .await
            .remove(&(peer, generation));
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
        protocol: &str,
        activity: StreamActivity,
    ) -> Result<Option<Connection>> {
        let mut conns = self.conns.lock().await;
        if let Some(existing) = conns.get_mut(&peer_addr.id)
            && existing.generation == generation
            && existing.connection.close_reason().is_none()
        {
            if activity == StreamActivity::Application {
                existing.last_application_activity = Some(Instant::now());
            }
            return Ok(Some(existing.connection.clone()));
        }
        if let Some(existing) = conns.get(&peer_addr.id)
            && existing.generation > generation
        {
            bail!(
                "endpoint generation changed before mux open: requested {generation}, current {}",
                existing.generation
            );
        }
        if let Some(stale) = conns.remove(&peer_addr.id)
            && stale.generation != generation
            && stale.connection.close_reason().is_none()
        {
            // A generation change replaces the endpoint itself. Close its mux
            // connections before the new endpoint dials, so the peer does not
            // retain the old canonical connection and reject the replacement.
            stale.connection.close(0u32.into(), STALE_GENERATION_REASON);
        }
        if self.mux_probe_deferred(peer_addr.id, generation).await {
            return Ok(None);
        }
        #[cfg(test)]
        self.mux_connect_attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let connection = match endpoint.connect(peer_addr.clone(), MUX_ALPN).await {
            Ok(connection) => connection,
            Err(connect_error) => {
                let error = anyhow::Error::new(connect_error).context("connect mux connection");
                if is_mux_unsupported(&error) {
                    self.note_legacy_fallback(peer_addr.id, generation, protocol, &error)
                        .await;
                    return Ok(None);
                }
                return Err(error);
            }
        };
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
        Ok(Some(connection))
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
                    .close(0u32.into(), DUPLICATE_CONNECTION_REASON);
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
            connection.close(0u32.into(), DUPLICATE_CONNECTION_REASON);
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

    const LEGACY_ECHO_ALPN: &[u8] = b"fabric/test-legacy-echo/1";

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

    #[test]
    fn network_failures_do_not_mean_mux_is_unsupported() {
        for error in [ConnectionError::TimedOut, ConnectionError::Reset] {
            assert!(!is_no_application_protocol(&error));
        }
        assert!(!is_mux_unsupported(&anyhow::anyhow!(
            "peer route is unavailable"
        )));
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

    /// A peer can retain the old canonical connection briefly while the other
    /// endpoint changes generation. It rejects each new connection as a
    /// duplicate until the old close reaches it.
    #[derive(Debug, Clone)]
    struct TransientDuplicateMux {
        connections: Arc<AtomicUsize>,
        reject: usize,
    }

    impl ProtocolHandler for TransientDuplicateMux {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            let attempt = self.connections.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= self.reject {
                connection.close(0u32.into(), DUPLICATE_CONNECTION_REASON);
                return Ok(());
            }
            loop {
                let (mut send, mut recv) = match connection.accept_bi().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                tokio::spawn(async move {
                    if MuxStreamHeader::read(&mut recv).await.is_ok() {
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
    async fn a_transient_duplicate_connection_converges_without_a_daemon_restart() -> Result<()> {
        let connections = Arc::new(AtomicUsize::new(0));
        let server_ep = Endpoint::bind(presets::N0).await?;
        let router = Router::builder(server_ep)
            .accept(
                MUX_ALPN,
                TransientDuplicateMux {
                    connections: connections.clone(),
                    reject: 4,
                },
            )
            .spawn();
        router.endpoint().online().await;

        let client = Endpoint::bind(presets::N0).await?;
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = PeerConnections::new(client.id(), opened_tx);
        let stream = manager
            .open_stream(
                &client,
                1,
                &router.endpoint().addr(),
                "probe",
                StreamActivity::Probe,
            )
            .await?;
        assert_echo(stream, b"after-recycle").await?;
        assert_eq!(connections.load(Ordering::SeqCst), 5);

        router.shutdown().await?;
        client.close().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_new_endpoint_generation_closes_its_cached_mux_connection() -> Result<()> {
        let connections = Arc::new(AtomicUsize::new(0));
        let server_ep = Endpoint::bind(presets::N0).await?;
        let router = Router::builder(server_ep)
            .accept(
                MUX_ALPN,
                MuxEcho {
                    connections: connections.clone(),
                    headers: Arc::new(Mutex::new(Vec::new())),
                },
            )
            .spawn();
        router.endpoint().online().await;

        let client = Endpoint::bind(presets::N0).await?;
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = PeerConnections::new(client.id(), opened_tx);
        let first = manager
            .open_stream(
                &client,
                0,
                &router.endpoint().addr(),
                "probe",
                StreamActivity::Probe,
            )
            .await?;
        let old_connection = first.connection.clone();
        assert_echo(first, b"old-generation").await?;

        let second = manager
            .open_stream(
                &client,
                1,
                &router.endpoint().addr(),
                "probe",
                StreamActivity::Probe,
            )
            .await?;
        assert_echo(second, b"new-generation").await?;
        tokio::time::timeout(Duration::from_secs(1), old_connection.closed())
            .await
            .context("the old mux connection survived the endpoint generation change")?;
        assert_eq!(connections.load(Ordering::SeqCst), 2);

        router.shutdown().await?;
        client.close().await;
        Ok(())
    }

    /// An old-style server that accepts a service as its connection ALPN.
    #[derive(Debug, Clone)]
    struct LegacyEcho {
        connections: Arc<AtomicUsize>,
    }

    impl ProtocolHandler for LegacyEcho {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            self.connections.fetch_add(1, Ordering::SeqCst);
            loop {
                let (mut send, mut recv) = match connection.accept_bi().await {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut recv, &mut send).await;
                    let _ = send.finish();
                });
            }
            Ok(())
        }
    }

    async fn assert_echo(mut stream: MuxStream, message: &[u8]) -> Result<()> {
        stream.send.write_all(message).await?;
        stream.send.finish()?;
        let mut echoed = vec![0; message.len()];
        stream.recv.read_exact(&mut echoed).await?;
        assert_eq!(echoed, message);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mixed_versions_work_both_ways_then_converge_to_mux() -> Result<()> {
        let mux_connections = Arc::new(AtomicUsize::new(0));
        let legacy_connections = Arc::new(AtomicUsize::new(0));
        let headers = Arc::new(Mutex::new(Vec::new()));
        let server_ep = Endpoint::bind(presets::N0).await?;
        let router = Router::builder(server_ep)
            .accept(
                MUX_ALPN,
                MuxEcho {
                    connections: mux_connections.clone(),
                    headers,
                },
            )
            .accept(
                LEGACY_ECHO_ALPN,
                LegacyEcho {
                    connections: legacy_connections.clone(),
                },
            )
            .spawn();
        router.endpoint().online().await;
        let server_addr = router.endpoint().addr();

        // The same peer first behaves like an old build with no mux ALPN.
        router.endpoint().set_alpns(vec![LEGACY_ECHO_ALPN.to_vec()]);
        let new_client = Endpoint::bind(presets::N0).await?;
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = PeerConnections::new(new_client.id(), opened_tx);
        let legacy_protocol = str::from_utf8(LEGACY_ECHO_ALPN)?;
        let (first, second) = tokio::join!(
            manager.open_stream(
                &new_client,
                0,
                &server_addr,
                legacy_protocol,
                StreamActivity::Application,
            ),
            manager.open_stream(
                &new_client,
                0,
                &server_addr,
                legacy_protocol,
                StreamActivity::Application,
            ),
        );
        assert_echo(first?, b"new-to-old-1").await?;
        assert_echo(second?, b"new-to-old-2").await?;
        assert_eq!(legacy_connections.load(Ordering::SeqCst), 2);
        assert_eq!(mux_connections.load(Ordering::SeqCst), 0);
        assert_eq!(manager.peer_count().await, 0, "fallback must not be cached");
        assert_eq!(
            manager.mux_connect_attempts.load(Ordering::SeqCst),
            1,
            "the compatibility window must suppress repeated rejected mux handshakes"
        );
        assert_eq!(
            manager
                .legacy_fallbacks
                .lock()
                .await
                .get(&(server_addr.id, 0))
                .map(|fallback| fallback.uses),
            Some(2),
            "every direct fallback use must be countable"
        );
        assert_eq!(
            manager.legacy_notices.lock().await.len(),
            1,
            "repeated fallback must log once per peer and generation"
        );

        // An old-style client can still use a direct ALPN on a new server.
        let old_client = Endpoint::bind(presets::N0).await?;
        let old_connection = old_client
            .connect(server_addr.clone(), LEGACY_ECHO_ALPN)
            .await?;
        let (send, recv) = old_connection.open_bi().await?;
        assert_echo(
            MuxStream {
                connection: old_connection,
                send,
                recv,
            },
            b"old-to-new",
        )
        .await?;
        assert_eq!(legacy_connections.load(Ordering::SeqCst), 3);

        // Once the bounded re-probe is due, an upgraded peer returns to mux.
        router
            .endpoint()
            .set_alpns(vec![MUX_ALPN.to_vec(), LEGACY_ECHO_ALPN.to_vec()]);
        manager
            .legacy_fallbacks
            .lock()
            .await
            .get_mut(&(server_addr.id, 0))
            .expect("the old peer must have a fallback record")
            .retry_after = Instant::now();
        for message in [b"upgraded-1".as_slice(), b"upgraded-2".as_slice()] {
            let stream = manager
                .open_stream(
                    &new_client,
                    0,
                    &server_addr,
                    str::from_utf8(LEGACY_ECHO_ALPN)?,
                    StreamActivity::Application,
                )
                .await?;
            assert_echo(stream, message).await?;
        }
        assert_eq!(mux_connections.load(Ordering::SeqCst), 1);
        assert_eq!(manager.peer_count().await, 1);
        assert_eq!(manager.mux_connect_attempts.load(Ordering::SeqCst), 2);

        router.shutdown().await?;
        new_client.close().await;
        old_client.close().await;
        Ok(())
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
