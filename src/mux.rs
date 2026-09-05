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
//!
//! Mux version 2 exchanges each endpoint owner's generation before logical
//! stream admission. This lets a peer replace stale canonical state without
//! weakening the simultaneous-open tie-break for equal generations.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
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
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, mpsc},
};

/// The reserved ALPN for the multiplexed per-peer connection.
pub const MUX_ALPN: &[u8] = b"fabric/mux/2";
/// Mux version 2 exchanges endpoint generations before it admits streams.
pub const MUX_ENABLED: bool = true;
/// A diagnostic block that can clear without a config change.
pub(crate) const TEMPORARY_TUNNEL_BLOCK: &str = "fabric tunnel blocked";

/// Largest protocol name accepted in a stream header (ALPN-scale).
const MAX_PROTOCOL_LEN: usize = 255;
const MAX_RESPONSE_LEN: usize = 4096;
const OPEN_TIMEOUT: Duration = Duration::from_secs(3);
const GENERATION_PREFACE_TIMEOUT: Duration = Duration::from_secs(3);
const OPEN_ATTEMPTS: usize = 4;
// Keep one bounded attempt for a replacement connection.
const UNKNOWN_CONNECTION_FAILURES_BEFORE_REPLACE: usize = OPEN_ATTEMPTS - 1;
const DUPLICATE_OPEN_ATTEMPTS: usize = 8;
const DUPLICATE_RETRY_DELAY: Duration = Duration::from_millis(100);
const LEGACY_MUX_REPROBE_INTERVAL: Duration = Duration::from_secs(60);
const VALIDATION_LOG_TARGET: &str = "fabric::validation";
const NO_APPLICATION_PROTOCOL_ALERT: u8 = 0x78;
const DUPLICATE_CONNECTION_REASON: &[u8] = b"duplicate mux connection";
const STALE_GENERATION_REASON: &[u8] = b"endpoint generation changed";
const REPEATED_STREAM_FAILURE_REASON: &[u8] = b"repeated mux stream failures";
const REPEATED_ATTACH_FAILURE_REASON: &[u8] = b"repeated tunnel attach failures";
const ATTACH_FAILURES_BEFORE_REPLACE: u32 = 3;
const SUSTAINED_PROGRESS_DURATION: Duration = Duration::from_secs(5);
const SUSTAINED_PROGRESS_MAX_GAP: Duration = Duration::from_secs(2);

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

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

/// Health for the one shared connection that is current for a peer.
///
/// These values reset when that exact connection is replaced. They are not
/// durable session totals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentConnectionHealth {
    pub connection_id: u64,
    pub age_millis: u64,
    pub consecutive_attach_failures: u32,
    pub last_attach_failure_phase: Option<String>,
    pub last_attach_failure_duration_millis: Option<u64>,
    pub last_application_progress_millis_ago: Option<u64>,
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

fn is_connection_lost(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|source| source.downcast_ref::<ConnectionError>().is_some())
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
    /// The generation owned by the remote endpoint.
    remote_generation: u64,
    generation: u64,
    opened_at: Instant,
    health: ConnectionHealthState,
}

#[derive(Debug, Default)]
struct ConnectionHealthState {
    consecutive_attach_failures: u32,
    last_attach_failure_phase: Option<String>,
    last_attach_failure_duration: Option<Duration>,
    last_application_progress: Option<Instant>,
    sustained_progress_started: Option<Instant>,
}

impl ConnectionHealthState {
    fn note_application_progress(&mut self, now: Instant) {
        let continues = self
            .last_application_progress
            .is_some_and(|last| now.duration_since(last) <= SUSTAINED_PROGRESS_MAX_GAP);
        if self.sustained_progress_started.is_none() || !continues {
            self.sustained_progress_started = Some(now);
        }
        self.last_application_progress = Some(now);
        if self
            .sustained_progress_started
            .is_some_and(|started| now.duration_since(started) >= SUSTAINED_PROGRESS_DURATION)
        {
            self.consecutive_attach_failures = 0;
        }
    }

    fn note_attach_failure(&mut self, phase: &str, duration: Duration) -> u32 {
        self.consecutive_attach_failures = self.consecutive_attach_failures.saturating_add(1);
        self.last_attach_failure_phase = Some(phase.to_string());
        self.last_attach_failure_duration = Some(duration);
        self.sustained_progress_started = None;
        self.consecutive_attach_failures
    }
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
    /// Serializes connection replacement for one peer without making a slow
    /// dial block every other peer in the map. This keeps one small entry per
    /// peer used during the process lifetime; the fleet peer set is bounded.
    peer_gates: Mutex<HashMap<EndpointId, Arc<Mutex<()>>>>,
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
            peer_gates: Mutex::new(HashMap::new()),
            legacy_notices: Mutex::new(HashSet::new()),
            legacy_fallbacks: Mutex::new(HashMap::new()),
            opened_tx,
            #[cfg(test)]
            mux_connect_attempts: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    async fn peer_gate(&self, peer: EndpointId) -> Arc<Mutex<()>> {
        self.peer_gates
            .lock()
            .await
            .entry(peer)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
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
        if MUX_ENABLED {
            self.open_mux_stream(endpoint, generation, peer_addr, protocol, activity)
                .await
        } else {
            self.open_legacy(endpoint, peer_addr, protocol).await
        }
    }

    /// Open through the shared transport for focused tests.
    async fn open_mux_stream(
        &self,
        endpoint: &Endpoint,
        generation: u64,
        peer_addr: &EndpointAddr,
        protocol: &str,
        activity: StreamActivity,
    ) -> Result<MuxStream> {
        let header = MuxStreamHeader::new(protocol.to_string());
        let mut attempts = 0usize;
        let mut failed_connection = None;
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
                Err(error) => {
                    if is_duplicate_connection(&error) && attempts < DUPLICATE_OPEN_ATTEMPTS {
                        tokio::time::sleep(DUPLICATE_RETRY_DELAY).await;
                        continue;
                    }
                    return Err(error);
                }
            };
            self.clear_legacy_retry(peer_addr.id, generation).await;
            match self.open_on(&connection, &header).await {
                Ok(stream) => return Ok(stream),
                Err(error) => {
                    if is_stream_denied(&error) {
                        return Err(error);
                    }
                    let stable_id = connection.stable_id();
                    let failures = match failed_connection {
                        Some((failed_id, failures)) if failed_id == stable_id => failures + 1,
                        _ => 1,
                    };
                    failed_connection = Some((stable_id, failures));
                    // One logical stream can fail beside healthy siblings. A
                    // repeated unknown state must not preserve a dead handle.
                    if connection.close_reason().is_some()
                        || is_connection_lost(&error)
                        || failures >= UNKNOWN_CONNECTION_FAILURES_BEFORE_REPLACE
                    {
                        self.close_and_forget_if(peer_addr.id, stable_id).await;
                        failed_connection = None;
                    }
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

    async fn exchange_generations(connection: &Connection, local_generation: u64) -> Result<u64> {
        tokio::time::timeout(GENERATION_PREFACE_TIMEOUT, async {
            let (mut send, mut recv) = connection
                .open_bi()
                .await
                .context("open mux generation preface")?;
            send.write_u64(local_generation)
                .await
                .context("write local endpoint generation")?;
            send.flush()
                .await
                .context("flush local endpoint generation")?;
            let remote_generation = recv
                .read_u64()
                .await
                .context("read remote endpoint generation")?;
            send.finish().context("finish mux generation preface")?;
            Ok(remote_generation)
        })
        .await
        .context("mux generation exchange timed out")?
    }

    /// Read the dialler's endpoint generation and return this endpoint's value.
    pub async fn accept_generation(connection: &Connection, local_generation: u64) -> Result<u64> {
        tokio::time::timeout(GENERATION_PREFACE_TIMEOUT, async {
            let (mut send, mut recv) = connection
                .accept_bi()
                .await
                .context("accept mux generation preface")?;
            let remote_generation = recv
                .read_u64()
                .await
                .context("read remote endpoint generation")?;
            send.write_u64(local_generation)
                .await
                .context("write local endpoint generation")?;
            send.finish().context("finish mux generation reply")?;
            Ok(remote_generation)
        })
        .await
        .context("mux generation preface timed out")?
    }

    /// Get the peer's cached connection, or open a fresh mux connection.
    async fn get_or_open(
        &self,
        endpoint: &Endpoint,
        generation: u64,
        peer_addr: &EndpointAddr,
        protocol: &str,
        _activity: StreamActivity,
    ) -> Result<Option<Connection>> {
        {
            let conns = self.conns.lock().await;
            if let Some(existing) = conns.get(&peer_addr.id)
                && existing.generation == generation
                && existing.connection.close_reason().is_none()
            {
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
        }

        let peer_gate = self.peer_gate(peer_addr.id).await;
        let _peer_guard = peer_gate.lock().await;
        let mut conns = self.conns.lock().await;
        if let Some(existing) = conns.get(&peer_addr.id)
            && existing.generation == generation
            && existing.connection.close_reason().is_none()
        {
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
        drop(conns);
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
        let remote_generation = Self::exchange_generations(&connection, generation).await?;
        let mut conns = self.conns.lock().await;
        conns.insert(
            peer_addr.id,
            PeerConn {
                connection: connection.clone(),
                remote_generation,
                generation,
                opened_at: Instant::now(),
                health: ConnectionHealthState::default(),
            },
        );
        let _ = self.opened_tx.send(connection.clone());
        Ok(Some(connection))
    }

    async fn close_and_forget_if(&self, peer: EndpointId, stable_id: usize) {
        self.close_and_forget_if_with_reason(peer, stable_id, REPEATED_STREAM_FAILURE_REASON)
            .await;
    }

    async fn close_and_forget_if_with_reason(
        &self,
        peer: EndpointId,
        stable_id: usize,
        reason: &'static [u8],
    ) {
        let peer_gate = self.peer_gate(peer).await;
        let _peer_guard = peer_gate.lock().await;
        let mut connections = self.conns.lock().await;
        if connections
            .get(&peer)
            .is_some_and(|current| current.connection.stable_id() == stable_id)
        {
            let failed = connections
                .remove(&peer)
                .expect("the matched mux connection disappeared while locked");
            failed.connection.close(0u32.into(), reason);
        }
    }

    /// Make one peer use a fresh multipath connection on its next stream.
    pub async fn redial(&self, peer: EndpointId, reason: &[u8]) -> bool {
        self.redial_opened_before(peer, reason, Instant::now()).await
    }

    /// Close only a connection that existed before the failed probe started.
    pub async fn redial_opened_before(
        &self,
        peer: EndpointId,
        reason: &[u8],
        probe_started: Instant,
    ) -> bool {
        let mut connections = self.conns.lock().await;
        let Some(current) = connections.get(&peer) else {
            return false;
        };
        if current.opened_at > probe_started {
            return false;
        }
        let removed = connections
            .remove(&peer)
            .expect("the matched mux connection disappeared while locked");
        drop(connections);
        removed.connection.close(0u32.into(), reason);
        true
    }

    /// Cache an accepted peer connection for traffic in the reverse direction.
    pub async fn register_incoming(
        &self,
        connection: &Connection,
        generation: u64,
        remote_generation: u64,
    ) {
        let peer = connection.remote_id();
        let peer_gate = self.peer_gate(peer).await;
        let _peer_guard = peer_gate.lock().await;
        let mut conns = self.conns.lock().await;
        let replace = match conns.get(&peer) {
            None => true,
            Some(current)
                if current.generation != generation
                    || current.connection.close_reason().is_some() =>
            {
                true
            }
            Some(current) if remote_generation > current.remote_generation => true,
            Some(current) if remote_generation < current.remote_generation => false,
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
                    remote_generation,
                    generation,
                    opened_at: Instant::now(),
                    health: ConnectionHealthState::default(),
                },
            );
        } else {
            connection.close(0u32.into(), DUPLICATE_CONNECTION_REASON);
        }
    }

    /// Record application bytes or an acknowledgement on one exact connection.
    pub async fn note_application_progress(&self, peer: EndpointId, stable_id: usize) {
        if let Some(connection) = self.conns.lock().await.get_mut(&peer)
            && connection.connection.stable_id() == stable_id
        {
            connection.health.note_application_progress(Instant::now());
        }
    }

    /// Record a failed tunnel attach on one exact connection.
    ///
    /// Three consecutive failures replace that connection. Brief application
    /// progress does not clear the count.
    pub async fn note_attach_failure(
        &self,
        peer: EndpointId,
        stable_id: usize,
        phase: &str,
        duration: Duration,
    ) -> bool {
        let replace = {
            let mut connections = self.conns.lock().await;
            let Some(connection) = connections.get_mut(&peer) else {
                return false;
            };
            if connection.connection.stable_id() != stable_id {
                return false;
            }
            connection.health.note_attach_failure(phase, duration)
                >= ATTACH_FAILURES_BEFORE_REPLACE
        };
        if replace {
            self.close_and_forget_if_with_reason(peer, stable_id, REPEATED_ATTACH_FAILURE_REASON)
                .await;
        }
        replace
    }

    /// Recent application traffic makes a separate liveness probe redundant.
    pub async fn recently_active(&self, peer: EndpointId, within: Duration) -> bool {
        self.conns
            .lock()
            .await
            .get(&peer)
            .filter(|connection| connection.health.consecutive_attach_failures == 0)
            .and_then(|connection| connection.health.last_application_progress)
            .is_some_and(|last| last.elapsed() <= within)
    }

    /// Snapshot current connection health. Replacement starts a new window.
    pub async fn current_health(&self) -> HashMap<EndpointId, CurrentConnectionHealth> {
        let now = Instant::now();
        self.conns
            .lock()
            .await
            .iter()
            .filter(|(_, connection)| connection.connection.close_reason().is_none())
            .map(|(peer, connection)| {
                (
                    *peer,
                    CurrentConnectionHealth {
                        connection_id: connection.connection.stable_id() as u64,
                        age_millis: duration_millis(now.duration_since(connection.opened_at)),
                        consecutive_attach_failures: connection
                            .health
                            .consecutive_attach_failures,
                        last_attach_failure_phase: connection
                            .health
                            .last_attach_failure_phase
                            .clone(),
                        last_attach_failure_duration_millis: connection
                            .health
                            .last_attach_failure_duration
                            .map(duration_millis),
                        last_application_progress_millis_ago: connection
                            .health
                            .last_application_progress
                            .map(|last| duration_millis(now.duration_since(last))),
                    },
                )
            })
            .collect()
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    const LEGACY_ECHO_ALPN: &[u8] = b"fabric/test-legacy-echo/1";

    #[test]
    fn brief_progress_does_not_clear_consecutive_attach_failures() {
        let start = Instant::now();
        let mut health = ConnectionHealthState::default();
        assert_eq!(health.note_attach_failure("hello", Duration::from_millis(10)), 1);
        health.note_application_progress(start);
        health.note_application_progress(
            start + SUSTAINED_PROGRESS_DURATION - Duration::from_millis(1),
        );
        assert_eq!(
            health.note_attach_failure("attached", Duration::from_millis(20)),
            2,
            "a short successful attach must not reset the current connection"
        );
    }

    #[test]
    fn sustained_progress_clears_consecutive_attach_failures() {
        let start = Instant::now();
        let mut health = ConnectionHealthState::default();
        assert_eq!(health.note_attach_failure("hello", Duration::from_millis(10)), 1);
        health.note_application_progress(start);
        for second in 1..=SUSTAINED_PROGRESS_DURATION.as_secs() {
            health.note_application_progress(start + Duration::from_secs(second));
        }
        assert_eq!(
            health.note_attach_failure("attached", Duration::from_millis(20)),
            1,
            "sustained application progress must start a new failure sequence"
        );
    }

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
            PeerConnections::accept_generation(&connection, 0)
                .await
                .map_err(|error| {
                    AcceptError::from_err(std::io::Error::other(format!("{error:#}")))
                })?;
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

    #[derive(Debug, Clone)]
    struct ManagedMuxEcho {
        manager: Arc<PeerConnections>,
        generation: u64,
    }

    impl ProtocolHandler for ManagedMuxEcho {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            let remote_generation =
                PeerConnections::accept_generation(&connection, self.generation)
                    .await
                    .map_err(|error| {
                        AcceptError::from_err(std::io::Error::other(format!("{error:#}")))
                    })?;
            self.manager
                .register_incoming(&connection, self.generation, remote_generation)
                .await;
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

    #[derive(Debug, Clone)]
    struct BlockedGenerationMuxEcho {
        entered: Arc<tokio::sync::Barrier>,
        release: Arc<tokio::sync::Notify>,
    }

    impl ProtocolHandler for BlockedGenerationMuxEcho {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            let result: Result<()> = async {
                let (mut send, mut recv) = connection.accept_bi().await?;
                let _remote_generation = recv.read_u64().await?;
                self.entered.wait().await;
                self.release.notified().await;
                send.write_u64(0).await?;
                send.finish()?;

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
            .await;
            result
                .map_err(|error| AcceptError::from_err(std::io::Error::other(format!("{error:#}"))))
        }
    }

    async fn finish_streams_then_echo(connection: Connection, failures: usize) {
        let mut streams = 0usize;
        loop {
            let (mut send, mut recv) = match connection.accept_bi().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            streams += 1;
            tokio::spawn(async move {
                if MuxStreamHeader::read(&mut recv).await.is_err() {
                    return;
                }
                if streams <= failures {
                    let _ = send.finish();
                    return;
                }
                let _ = write_ready(&mut send).await;
                let _ = tokio::io::copy(&mut recv, &mut send).await;
                let _ = send.finish();
            });
        }
    }

    /// A slow connection to one peer must not delay an existing connection to
    /// another peer. Production showed a 2-second delay every 20 seconds when
    /// the health loop probed an absent peer beside a healthy peer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_slow_peer_open_does_not_block_another_peer() -> Result<()> {
        let client = Endpoint::builder(presets::N0).bind().await?;
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = Arc::new(PeerConnections::new(client.id(), opened_tx));

        let healthy = Router::builder(
            Endpoint::builder(presets::N0)
                .alpns(vec![MUX_ALPN.to_vec()])
                .bind()
                .await?,
        )
        .accept(
            MUX_ALPN,
            MuxEcho {
                connections: Arc::new(AtomicUsize::new(0)),
                headers: Arc::new(Mutex::new(Vec::new())),
            },
        )
        .spawn();
        let entered = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Notify::new());
        let slow = Router::builder(
            Endpoint::builder(presets::N0)
                .alpns(vec![MUX_ALPN.to_vec()])
                .bind()
                .await?,
        )
        .accept(
            MUX_ALPN,
            BlockedGenerationMuxEcho {
                entered: entered.clone(),
                release: release.clone(),
            },
        )
        .spawn();
        healthy.endpoint().online().await;
        slow.endpoint().online().await;

        let first = manager
            .open_mux_stream(
                &client,
                0,
                &healthy.endpoint().addr(),
                "first",
                StreamActivity::Probe,
            )
            .await?;
        drop(first);

        let slow_manager = manager.clone();
        let slow_client = client.clone();
        let slow_addr = slow.endpoint().addr();
        let slow_open = tokio::spawn(async move {
            slow_manager
                .open_mux_stream(&slow_client, 0, &slow_addr, "slow", StreamActivity::Probe)
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), entered.wait())
            .await
            .context("the slow peer did not reach generation exchange")?;

        let healthy_open = tokio::time::timeout(
            Duration::from_millis(250),
            manager.open_mux_stream(
                &client,
                0,
                &healthy.endpoint().addr(),
                "healthy",
                StreamActivity::Probe,
            ),
        )
        .await;
        release.notify_one();
        let slow_stream = slow_open.await??;
        drop(slow_stream);

        assert!(
            healthy_open.is_ok(),
            "a slow open to one peer blocked an existing connection to another peer"
        );
        healthy_open.expect("checked timeout")?;

        slow.shutdown().await?;
        healthy.shutdown().await?;
        client.close().await;
        Ok(())
    }

    /// Recovery must not close a connection that opened after the failed probe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_does_not_close_a_connection_opened_after_the_probe() -> Result<()> {
        let client = Endpoint::builder(presets::N0).bind().await?;
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = Arc::new(PeerConnections::new(client.id(), opened_tx));
        let entered = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Notify::new());
        let slow = Router::builder(
            Endpoint::builder(presets::N0)
                .alpns(vec![MUX_ALPN.to_vec()])
                .bind()
                .await?,
        )
        .accept(
            MUX_ALPN,
            BlockedGenerationMuxEcho {
                entered: entered.clone(),
                release: release.clone(),
            },
        )
        .spawn();
        slow.endpoint().online().await;

        let peer = slow.endpoint().id();
        let failed_probe_started = Instant::now();
        let slow_manager = manager.clone();
        let slow_client = client.clone();
        let slow_addr = slow.endpoint().addr();
        let slow_open = tokio::spawn(async move {
            slow_manager
                .open_mux_stream(&slow_client, 0, &slow_addr, "slow", StreamActivity::Probe)
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), entered.wait())
            .await
            .context("the slow peer did not reach generation exchange")?;

        release.notify_one();
        let stream = slow_open.await??;
        let stable_id = stream.connection.stable_id();
        drop(stream);
        let recovery = manager
            .redial_opened_before(peer, b"peer health recovery", failed_probe_started)
            .await;

        assert_eq!(
            recovery,
            false,
            "recovery closed a connection that opened after the failed probe started"
        );
        assert_eq!(
            manager.connection(peer).await.map(|conn| conn.stable_id()),
            Some(stable_id),
            "recovery closed the connection that completed after it started"
        );
        assert!(
            manager.redial(peer, b"current connection recovery").await,
            "recovery did not close the connection present when it started"
        );
        assert!(manager.connection(peer).await.is_none());

        slow.shutdown().await?;
        client.close().await;
        Ok(())
    }

    /// A peer that never answers must keep a fixed amount of manager state and
    /// must not delay a healthy peer while 32 repeated probes fail.
    ///
    /// The live control on hetz used 4.81 CPU-seconds over 93.339 seconds. A
    /// 342.087-second window with one offline peer used 17.09 CPU-seconds, or
    /// 4.996% of one core against the control's 5.153%. RSS followed the same
    /// bounded 128 MiB allocator sawtooth with and without the offline peer.
    /// This test pins the deterministic causes behind that result: each probe
    /// owns one bounded attempt, failed probes retain no connection, and a
    /// healthy peer never waits for the offline peer's gate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn offline_peer_cost_is_bounded_and_healthy_peer_stays_fast() -> Result<()> {
        const FAILED_PROBES: usize = 32;
        const FAILED_PROBE_WINDOW: Duration = Duration::from_millis(350);
        const HEALTHY_STREAM_BUDGET: Duration = Duration::from_millis(250);

        let client = Endpoint::builder(presets::N0).bind().await?;
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = Arc::new(PeerConnections::new(client.id(), opened_tx));
        let healthy = Router::builder(
            Endpoint::builder(presets::N0)
                .alpns(vec![MUX_ALPN.to_vec()])
                .bind()
                .await?,
        )
        .accept(
            MUX_ALPN,
            MuxEcho {
                connections: Arc::new(AtomicUsize::new(0)),
                headers: Arc::new(Mutex::new(Vec::new())),
            },
        )
        .spawn();
        healthy.endpoint().online().await;
        let healthy_addr = healthy.endpoint().addr();
        let offline_addr = EndpointAddr::new(iroh::SecretKey::generate().public());

        let mut first = manager
            .open_mux_stream(&client, 0, &healthy_addr, "control", StreamActivity::Probe)
            .await?;
        prove_measurement_stream(&mut first).await?;
        drop(first);
        let attempts_before = manager.mux_connect_attempts.load(Ordering::SeqCst);

        for probe in 0..FAILED_PROBES {
            let offline_manager = manager.clone();
            let offline_client = client.clone();
            let offline_addr = offline_addr.clone();
            let offline = tokio::spawn(async move {
                tokio::time::timeout(
                    FAILED_PROBE_WINDOW,
                    offline_manager.open_mux_stream(
                        &offline_client,
                        0,
                        &offline_addr,
                        "offline",
                        StreamActivity::Probe,
                    ),
                )
                .await
            });

            let expected_attempts = attempts_before + probe + 1;
            tokio::time::timeout(Duration::from_secs(1), async {
                while manager.mux_connect_attempts.load(Ordering::SeqCst) < expected_attempts {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .with_context(|| format!("offline probe {probe} never started"))?;

            let mut stream = tokio::time::timeout(
                HEALTHY_STREAM_BUDGET,
                manager.open_mux_stream(
                    &client,
                    0,
                    &healthy_addr,
                    "healthy",
                    StreamActivity::Probe,
                ),
            )
            .await
            .with_context(|| {
                format!("offline probe {probe} delayed the healthy peer past the budget")
            })??;
            prove_measurement_stream(&mut stream).await?;
            drop(stream);

            let offline = offline.await?;
            assert!(
                !matches!(offline, Ok(Ok(_))),
                "the offline peer unexpectedly accepted probe {probe}"
            );
        }

        assert_eq!(
            manager.mux_connect_attempts.load(Ordering::SeqCst) - attempts_before,
            FAILED_PROBES,
            "a probe must own one connection attempt and no hidden retry task"
        );
        assert_eq!(
            manager.conns.lock().await.len(),
            1,
            "failed probes must retain no connection beside the healthy one"
        );
        assert!(manager.legacy_notices.lock().await.is_empty());
        assert!(manager.legacy_fallbacks.lock().await.is_empty());

        healthy.shutdown().await?;
        client.close().await;
        Ok(())
    }

    async fn assert_stream_failure_recovery(
        failures: usize,
        expect_same_connection: bool,
    ) -> Result<()> {
        let first = iroh::SecretKey::generate();
        let second = iroh::SecretKey::generate();
        let (lower_key, higher_key) = if first.public() < second.public() {
            (first, second)
        } else {
            (second, first)
        };

        let lower_endpoint = Endpoint::builder(presets::N0)
            .secret_key(lower_key)
            .alpns(vec![MUX_ALPN.to_vec()])
            .bind()
            .await?;
        let (lower_opened_tx, mut lower_opened_rx) = mpsc::unbounded_channel();
        let lower_manager = Arc::new(PeerConnections::new(
            lower_endpoint.id(),
            lower_opened_tx,
        ));
        let lower_router = Router::builder(lower_endpoint)
            .accept(
                MUX_ALPN,
                ManagedMuxEcho {
                    manager: lower_manager.clone(),
                    generation: 5,
                },
            )
            .spawn();
        let lower_reverse_streams = tokio::spawn(async move {
            if let Some(connection) = lower_opened_rx.recv().await {
                finish_streams_then_echo(connection, failures).await;
            }
        });

        let higher_endpoint = Endpoint::builder(presets::N0)
            .secret_key(higher_key)
            .alpns(vec![MUX_ALPN.to_vec()])
            .bind()
            .await?;
        let (higher_opened_tx, _higher_opened_rx) = mpsc::unbounded_channel();
        let higher_manager = Arc::new(PeerConnections::new(
            higher_endpoint.id(),
            higher_opened_tx,
        ));
        let higher_router = Router::builder(higher_endpoint)
            .accept(
                MUX_ALPN,
                ManagedMuxEcho {
                    manager: higher_manager.clone(),
                    generation: 6,
                },
            )
            .spawn();
        lower_router.endpoint().online().await;
        higher_router.endpoint().online().await;

        let sibling = lower_manager
            .open_mux_stream(
                lower_router.endpoint(),
                5,
                &higher_router.endpoint().addr(),
                "probe",
                StreamActivity::Probe,
            )
            .await?;

        let canonical = higher_manager
            .connection(lower_router.endpoint().id())
            .await
            .context("the higher peer did not cache the canonical incoming connection")?;

        let replacement = higher_manager
            .open_mux_stream(
                higher_router.endpoint(),
                6,
                &lower_router.endpoint().addr(),
                "probe",
                StreamActivity::Probe,
            )
            .await
            .context("stream failures did not converge on a usable connection")?;
        assert_eq!(
            replacement.connection.stable_id() == canonical.stable_id(),
            expect_same_connection,
            "the recovery selected the wrong shared connection"
        );
        assert_echo(replacement, b"replacement").await?;
        if expect_same_connection {
            assert_echo(sibling, b"sibling-survived").await?;
        }

        lower_router.shutdown().await?;
        higher_router.shutdown().await?;
        lower_reverse_streams.await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_new_endpoint_generation_replaces_a_live_stale_canonical_connection() -> Result<()> {
        let first = iroh::SecretKey::generate();
        let second = iroh::SecretKey::generate();
        let (lower_key, higher_key) = if first.public() < second.public() {
            (first, second)
        } else {
            (second, first)
        };

        let lower_endpoint = Endpoint::builder(presets::N0)
            .secret_key(lower_key)
            .alpns(vec![MUX_ALPN.to_vec()])
            .bind()
            .await?;
        let (lower_opened_tx, _lower_opened_rx) = mpsc::unbounded_channel();
        let lower_manager = Arc::new(PeerConnections::new(
            lower_endpoint.id(),
            lower_opened_tx,
        ));
        let lower_router = Router::builder(lower_endpoint)
            .accept(
                MUX_ALPN,
                ManagedMuxEcho {
                    manager: lower_manager.clone(),
                    generation: 0,
                },
            )
            .spawn();
        lower_router.endpoint().online().await;

        let higher_old_endpoint = Endpoint::builder(presets::N0)
            .secret_key(higher_key.clone())
            .alpns(vec![MUX_ALPN.to_vec()])
            .bind()
            .await?;
        let higher_old_router = Router::builder(higher_old_endpoint)
            .accept(
                MUX_ALPN,
                MuxEcho {
                    connections: Arc::new(AtomicUsize::new(0)),
                    headers: Arc::new(Mutex::new(Vec::new())),
                },
            )
            .spawn();
        higher_old_router.endpoint().online().await;

        let first_stream = lower_manager
            .open_mux_stream(
                lower_router.endpoint(),
                0,
                &higher_old_router.endpoint().addr(),
                "probe",
                StreamActivity::Probe,
            )
            .await?;
        assert_eq!(first_stream.connection.side(), Side::Client);
        assert_echo(first_stream, b"old-generation").await?;

        let higher_new_endpoint = Endpoint::builder(presets::N0)
            .secret_key(higher_key)
            .alpns(vec![MUX_ALPN.to_vec()])
            .bind()
            .await?;
        higher_new_endpoint.online().await;
        let (higher_opened_tx, _higher_opened_rx) = mpsc::unbounded_channel();
        let higher_new_manager = PeerConnections::new(higher_new_endpoint.id(), higher_opened_tx);

        let replacement = higher_new_manager
            .open_mux_stream(
                &higher_new_endpoint,
                1,
                &lower_router.endpoint().addr(),
                "probe",
                StreamActivity::Probe,
            )
            .await
            .context("the peer retained generation 0 and refused generation 1 as a duplicate")?;
        assert_echo(replacement, b"new-generation").await?;

        higher_old_router.shutdown().await?;
        lower_router.shutdown().await?;
        higher_new_endpoint.close().await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_failed_mux_stream_keeps_its_siblings_and_canonical_connection() -> Result<()> {
        assert_stream_failure_recovery(1, true).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repeated_unknown_mux_failures_replace_the_connection() -> Result<()> {
        assert_stream_failure_recovery(usize::MAX, false).await
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
            PeerConnections::accept_generation(&connection, 0)
                .await
                .map_err(|error| {
                    AcceptError::from_err(std::io::Error::other(format!("{error:#}")))
                })?;
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
            .open_mux_stream(
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
            .open_mux_stream(
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
            .open_mux_stream(
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_streams_use_generation_aware_mux() -> Result<()> {
        let mux_connections = Arc::new(AtomicUsize::new(0));
        let legacy_connections = Arc::new(AtomicUsize::new(0));
        let server_ep = Endpoint::bind(presets::N0).await?;
        let router = Router::builder(server_ep)
            .accept(
                MUX_ALPN,
                MuxEcho {
                    connections: mux_connections.clone(),
                    headers: Arc::new(Mutex::new(Vec::new())),
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

        let client = Endpoint::bind(presets::N0).await?;
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = PeerConnections::new(client.id(), opened_tx);
        let stream = manager
            .open_stream(
                &client,
                0,
                &router.endpoint().addr(),
                str::from_utf8(LEGACY_ECHO_ALPN)?,
                StreamActivity::Application,
            )
            .await?;
        assert_echo(stream, b"stable-legacy").await?;

        assert_eq!(legacy_connections.load(Ordering::SeqCst), 0);
        assert_eq!(mux_connections.load(Ordering::SeqCst), 1);

        router.shutdown().await?;
        client.close().await;
        Ok(())
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
            manager.open_mux_stream(
                &new_client,
                0,
                &server_addr,
                legacy_protocol,
                StreamActivity::Application,
            ),
            manager.open_mux_stream(
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
                .open_mux_stream(
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
            .open_mux_stream(&client, 0, &server_addr, "probe", StreamActivity::Probe)
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
                .open_mux_stream(&client, 0, &server_addr, proto, StreamActivity::Application)
                .await?;
            let mut send = stream.send;
            let mut recv = stream.recv;
            send.write_all(b"ping").await?;
            send.finish()?;
            let mut buf = [0u8; 4];
            recv.read_exact(&mut buf).await?;
            assert_eq!(&buf, b"ping");
        }

        assert!(
            !manager
                .recently_active(server_addr.id, Duration::from_secs(1))
                .await,
            "mux admission and raw stream bytes do not prove tunnel progress"
        );
        let stable_id = manager
            .connection(server_addr.id)
            .await
            .expect("the shared connection is cached")
            .stable_id();
        manager
            .note_application_progress(server_addr.id, stable_id)
            .await;

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
        assert!(
            !manager
                .note_attach_failure(
                    server_addr.id,
                    stable_id,
                    "hello",
                    Duration::from_millis(10),
                )
                .await
        );
        assert!(
            !manager
                .recently_active(server_addr.id, Duration::from_secs(1))
                .await,
            "a current attach failure must not hide behind older progress"
        );
        assert!(
            !manager
                .note_attach_failure(
                    server_addr.id,
                    stable_id,
                    "attached",
                    Duration::from_millis(20),
                )
                .await
        );
        assert_eq!(
            manager.current_health().await[&server_addr.id]
                .consecutive_attach_failures,
            2
        );
        assert_eq!(
            manager.current_health().await[&server_addr.id]
                .last_attach_failure_phase
                .as_deref(),
            Some("attached")
        );
        assert!(
            manager
                .note_attach_failure(
                    server_addr.id,
                    stable_id,
                    "hello",
                    Duration::from_millis(30),
                )
                .await
        );
        assert_eq!(manager.peer_count().await, 0);
        let stream = manager
            .open_mux_stream(
                &client,
                0,
                &server_addr,
                "after-redial",
                StreamActivity::Application,
            )
            .await?;
        drop(stream);
        assert_eq!(connections.load(Ordering::SeqCst), 2);
        assert!(
            !manager
                .note_attach_failure(
                    server_addr.id,
                    stable_id,
                    "attached",
                    Duration::from_millis(40),
                )
                .await,
            "a late failure must not close the replacement connection"
        );
        assert_eq!(manager.peer_count().await, 1);

        router.shutdown().await?;
        client.close().await;
        Ok(())
    }

    #[derive(Debug, Clone, Copy)]
    struct ProcessSample {
        cpu_seconds: f64,
        rss_bytes: u64,
        package_idle_wakes: u64,
        interrupt_wakes: u64,
    }

    #[cfg(target_os = "macos")]
    fn process_sample() -> Result<ProcessSample> {
        let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::zeroed();
        // SAFETY: proc_pid_rusage writes one rusage_info_v4 into this aligned,
        // zeroed buffer. A zero return confirms that it initialized the buffer.
        let status = unsafe {
            libc::proc_pid_rusage(
                libc::getpid(),
                libc::RUSAGE_INFO_V4,
                info.as_mut_ptr().cast(),
            )
        };
        if status != 0 {
            return Err(std::io::Error::last_os_error()).context("read process resource use");
        }
        // SAFETY: the successful call initialized the complete version 4 value.
        let info = unsafe { info.assume_init() };
        Ok(ProcessSample {
            cpu_seconds: (info.ri_user_time + info.ri_system_time) as f64 / 1_000_000_000.0,
            rss_bytes: info.ri_resident_size,
            package_idle_wakes: info.ri_pkg_idle_wkups,
            interrupt_wakes: info.ri_interrupt_wkups,
        })
    }

    #[cfg(not(target_os = "macos"))]
    fn process_sample() -> Result<ProcessSample> {
        bail!("the wake-frequency measurement requires macOS process counters")
    }

    fn unique_connections(streams: &[MuxStream]) -> Vec<Connection> {
        let mut seen = HashSet::new();
        streams
            .iter()
            .filter_map(|stream| {
                seen.insert(stream.connection.stable_id())
                    .then(|| stream.connection.clone())
            })
            .collect()
    }

    fn network_totals(connections: &[Connection]) -> (u64, u64, u64) {
        connections.iter().fold((0, 0, 0), |totals, connection| {
            let stats = connection.stats();
            (
                totals.0 + stats.udp_tx.bytes + stats.udp_rx.bytes,
                totals.1 + stats.udp_tx.datagrams + stats.udp_rx.datagrams,
                totals.2 + stats.udp_tx.ios + stats.udp_rx.ios,
            )
        })
    }

    async fn open_measurement_stream(
        mode: &str,
        manager: &PeerConnections,
        client: &Endpoint,
        server: &EndpointAddr,
    ) -> Result<MuxStream> {
        if mode == "mux" {
            manager
                .open_mux_stream(client, 1, server, "measure", StreamActivity::Application)
                .await
        } else {
            manager.open_legacy(client, server, "measure").await
        }
    }

    async fn prove_measurement_stream(stream: &mut MuxStream) -> Result<()> {
        stream.send.write_all(b"x").await?;
        let mut reply = [0u8; 1];
        stream.recv.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"x");
        Ok(())
    }

    /// Compare the idle cost of 16 mux streams against 16 direct connections.
    ///
    /// Run each mode in a fresh process. The default window is 30 minutes:
    /// `FABRIC_MUX_MEASURE_MODE=mux cargo test --lib idle_mux_value_measurement -- --ignored --nocapture`
    /// Repeat with `FABRIC_MUX_MEASURE_MODE=direct`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "30-minute mux value measurement"]
    async fn idle_mux_value_measurement() -> Result<()> {
        let mode = std::env::var("FABRIC_MUX_MEASURE_MODE")
            .context("set FABRIC_MUX_MEASURE_MODE to mux or direct")?;
        if mode != "mux" && mode != "direct" {
            bail!("FABRIC_MUX_MEASURE_MODE must be mux or direct");
        }
        let window_seconds = std::env::var("FABRIC_MUX_MEASURE_SECONDS")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()?
            .unwrap_or(30 * 60);
        if window_seconds == 0 {
            bail!("FABRIC_MUX_MEASURE_SECONDS must be greater than zero");
        }

        let mux_connections = Arc::new(AtomicUsize::new(0));
        let legacy_connections = Arc::new(AtomicUsize::new(0));
        let server_endpoint = Endpoint::bind(presets::N0).await?;
        let router = Router::builder(server_endpoint)
            .accept(
                MUX_ALPN,
                MuxEcho {
                    connections: mux_connections,
                    headers: Arc::new(Mutex::new(Vec::new())),
                },
            )
            .accept(
                b"measure",
                LegacyEcho {
                    connections: legacy_connections,
                },
            )
            .spawn();
        router.endpoint().online().await;
        let server = router.endpoint().addr();
        let client = Endpoint::bind(presets::N0).await?;
        let (opened_tx, _opened_rx) = mpsc::unbounded_channel();
        let manager = Arc::new(PeerConnections::new(client.id(), opened_tx));

        let mut streams = Vec::with_capacity(16);
        for _ in 0..16 {
            let mut stream = open_measurement_stream(&mode, &manager, &client, &server).await?;
            prove_measurement_stream(&mut stream).await?;
            streams.push(stream);
        }
        let idle_connections = unique_connections(&streams);
        let network_start = network_totals(&idle_connections);
        let process_start = process_sample()?;
        println!(
            "measurement_start mode={mode} window_seconds={window_seconds} logical_sessions=16 connections={} pid={} cpu_seconds={:.6} rss_bytes={} package_idle_wakes={} interrupt_wakes={} network_bytes={} datagrams={} ios={}",
            idle_connections.len(),
            std::process::id(),
            process_start.cpu_seconds,
            process_start.rss_bytes,
            process_start.package_idle_wakes,
            process_start.interrupt_wakes,
            network_start.0,
            network_start.1,
            network_start.2,
        );

        tokio::time::sleep(Duration::from_secs(window_seconds)).await;

        let process_end = process_sample()?;
        let network_end = network_totals(&idle_connections);
        let cpu_seconds = process_end.cpu_seconds - process_start.cpu_seconds;
        let package_idle_wakes = process_end
            .package_idle_wakes
            .saturating_sub(process_start.package_idle_wakes);
        let interrupt_wakes = process_end
            .interrupt_wakes
            .saturating_sub(process_start.interrupt_wakes);
        let network_bytes = network_end.0.saturating_sub(network_start.0);
        println!(
            "measurement_idle mode={mode} window_seconds={window_seconds} connections={} cpu_seconds={cpu_seconds:.6} cpu_one_core_percent={:.6} rss_start_bytes={} rss_end_bytes={} package_idle_wakes={package_idle_wakes} package_idle_wakes_per_second={:.6} interrupt_wakes={interrupt_wakes} interrupt_wakes_per_second={:.6} network_bytes={network_bytes} network_bytes_per_second={:.3} datagrams={} ios={}",
            idle_connections.len(),
            cpu_seconds / window_seconds as f64 * 100.0,
            process_start.rss_bytes,
            process_end.rss_bytes,
            package_idle_wakes as f64 / window_seconds as f64,
            interrupt_wakes as f64 / window_seconds as f64,
            network_bytes as f64 / window_seconds as f64,
            network_end.1.saturating_sub(network_start.1),
            network_end.2.saturating_sub(network_start.2),
        );

        let mut recovery_micros = Vec::with_capacity(160);
        for _ in 0..10 {
            for connection in unique_connections(&streams) {
                connection.close(0u32.into(), b"mux value recovery sample");
            }
            let mut attempts = tokio::task::JoinSet::new();
            for _ in 0..16 {
                let mode = mode.clone();
                let manager = manager.clone();
                let client = client.clone();
                let server = server.clone();
                attempts.spawn(async move {
                    let started = Instant::now();
                    let stream = open_measurement_stream(&mode, &manager, &client, &server).await?;
                    Ok::<_, anyhow::Error>((started.elapsed(), stream))
                });
            }
            streams.clear();
            while let Some(result) = attempts.join_next().await {
                let (duration, mut stream) = result??;
                prove_measurement_stream(&mut stream).await?;
                recovery_micros.push(duration.as_micros() as u64);
                streams.push(stream);
            }
        }
        recovery_micros.sort_unstable();
        let p95 = recovery_micros[(recovery_micros.len() * 95).div_ceil(100) - 1];
        println!(
            "measurement_recovery mode={mode} samples={} p95_micros={p95} final_connections={}",
            recovery_micros.len(),
            unique_connections(&streams).len(),
        );

        router.shutdown().await?;
        client.close().await;
        Ok(())
    }
}
