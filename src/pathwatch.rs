//! Path-quality instrumentation: hold a long-lived connection to a peer and log
//! per-path state over time, so a degraded-but-connected direct path (the
//! documented 5s-RTT-for-30-min repro) is visible in the data instead of a blind
//! spot.
//!
//! iroh 1.0 connections are **multipath**: `Connection::paths()` yields several
//! concurrent paths (direct IPv4, direct IPv6, relay), one of which is
//! *selected* for application data. Each path exposes its own RTT, its
//! direct-vs-relay class, and its remote/local transport address (the UDP
//! 4-tuple). The daemon's periodic health poll only checks `Endpoint::online()`
//! (which stays true through a degradation) and the endpoint snapshot logs only
//! address *counts* — neither can show which path is hot or how slow it is.
//!
//! This probe closes that gap without acting on it (diagnosis, not a fix): it
//! keeps one echo connection alive per peer and, every interval, logs every
//! path's `{id, selected, ip/relay, remote_addr, local_addr, rtt}` plus an
//! aggregate, and flags when the *selected* path changes. Point it at a peer for
//! hours to capture the per-path RTT and selection behaviour across a
//! degradation — the data that shows whether iroh sticks to a degraded selected
//! path instead of re-selecting a healthy one.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointAddr, endpoint::Connection};
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Reconnect backoff after a probe connection drops.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);
/// Keepalive/RTT nonce size.
const NONCE_LEN: usize = 8;

/// The validation-log target, matching the daemon's other diagnostics.
const PATHWATCH_TARGET: &str = "fabric::validation";

/// A snapshot of one iroh path at one instant.
#[derive(Debug, Clone)]
pub struct PathObservation {
    pub id: String,
    pub selected: bool,
    /// "direct" (IP), "relay", or "other".
    pub class: &'static str,
    pub remote: String,
    pub local: String,
    pub rtt: Duration,
}

/// All paths of a connection at one instant, with derived aggregates.
#[derive(Debug, Clone, Default)]
pub struct PathObservations {
    pub paths: Vec<PathObservation>,
    /// "class:remote_addr" of the selected path, if any.
    pub selected: Option<String>,
    /// Minimum RTT across all paths.
    pub min_rtt: Option<Duration>,
}

/// Observe a connection's current multipath state. Pure read of the iroh path
/// API — the log/diff wrappers build on this, and tests assert on it directly.
pub fn observe_paths(connection: &Connection) -> PathObservations {
    let mut out = PathObservations::default();
    for path in connection.paths().iter() {
        let rtt = path.rtt();
        out.min_rtt = Some(out.min_rtt.map_or(rtt, |m| m.min(rtt)));
        let class = if path.is_ip() {
            "direct"
        } else if path.is_relay() {
            "relay"
        } else {
            "other"
        };
        if path.is_selected() {
            out.selected = Some(format!("{class}:{}", path.remote_addr()));
        }
        out.paths.push(PathObservation {
            id: format!("{:?}", path.id()),
            selected: path.is_selected(),
            class,
            remote: format!("{}", path.remote_addr()),
            local: format!("{:?}", path.local_addr()),
            rtt,
        });
    }
    out
}

/// Continuously probe one peer's path quality until `cancel` fires. Holds an
/// echo connection, sends a keepalive nonce each interval to keep paths warm and
/// measure application RTT, and logs per-path + aggregate state. Reconnects with
/// backoff on any connection error, re-reading the current endpoint each time so
/// it follows endpoint recycles.
pub async fn probe_peer_paths<F>(
    endpoint_of: F,
    peer_label: String,
    peer_addr: EndpointAddr,
    alpn: Vec<u8>,
    interval: Duration,
    cancel: CancellationToken,
) where
    F: Fn() -> Option<Endpoint>,
{
    loop {
        if cancel.is_cancelled() {
            return;
        }
        let Some(endpoint) = endpoint_of() else {
            if wait_or_cancelled(&cancel, RECONNECT_BACKOFF).await {
                return;
            }
            continue;
        };
        match probe_once(&endpoint, &peer_label, &peer_addr, &alpn, interval, &cancel).await {
            Ok(()) => {} // cancelled cleanly
            Err(error) => {
                info!(
                    target: PATHWATCH_TARGET,
                    event = "pathwatch_disconnected",
                    peer = %peer_label,
                    %error,
                    "path probe connection ended; will reconnect"
                );
            }
        }
        if wait_or_cancelled(&cancel, RECONNECT_BACKOFF).await {
            return;
        }
    }
}

/// One connect-and-observe cycle. Returns `Ok` when cancelled, `Err` on a
/// connection problem (so the caller reconnects).
async fn probe_once(
    endpoint: &Endpoint,
    peer_label: &str,
    peer_addr: &EndpointAddr,
    alpn: &[u8],
    interval: Duration,
    cancel: &CancellationToken,
) -> Result<()> {
    let connection = endpoint
        .connect(peer_addr.clone(), alpn)
        .await
        .context("connect for path probe")?;
    info!(
        target: PATHWATCH_TARGET,
        event = "pathwatch_connected",
        peer = %peer_label,
        remote_id = %connection.remote_id(),
        "path probe connected"
    );

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .context("open_bi for path probe")?;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_selected: Option<String> = None;
    let mut nonce: u64 = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                connection.close(0u32.into(), b"pathwatch done");
                return Ok(());
            }
            _ = ticker.tick() => {
                // Keepalive + application RTT: echo a nonce.
                nonce = nonce.wrapping_add(1);
                let app_rtt = echo_round_trip(&mut send, &mut recv, nonce).await?;
                log_paths(&connection, peer_label, app_rtt, &mut last_selected);
            }
        }
    }
}

/// Write an 8-byte nonce and read it back from the peer's echo, returning the
/// application-level round-trip time.
async fn echo_round_trip(
    send: &mut iroh::endpoint::SendStream,
    recv: &mut iroh::endpoint::RecvStream,
    nonce: u64,
) -> Result<Duration> {
    let started = Instant::now();
    send.write_all(&nonce.to_be_bytes())
        .await
        .context("pathwatch keepalive write")?;
    let mut buf = [0u8; NONCE_LEN];
    recv.read_exact(&mut buf)
        .await
        .context("pathwatch keepalive read")?;
    Ok(started.elapsed())
}

/// Log every path's state plus an aggregate, flagging a selected-path change.
fn log_paths(
    connection: &Connection,
    peer_label: &str,
    app_rtt: Duration,
    last_selected: &mut Option<String>,
) {
    let observed = observe_paths(connection);

    // Per-path lines: the granular record for correlation.
    for path in &observed.paths {
        info!(
            target: PATHWATCH_TARGET,
            event = "pathwatch_path",
            peer = %peer_label,
            path_id = %path.id,
            selected = path.selected,
            class = path.class,
            remote_addr = %path.remote,
            local_addr = %path.local,
            rtt_ms = path.rtt.as_secs_f64() * 1000.0,
        );
    }

    // Aggregate line: the at-a-glance signal, with a selected-path change flag
    // (a re-selection is exactly the recovery behaviour we are checking for).
    let selected_changed = observed.selected != *last_selected;
    info!(
        target: PATHWATCH_TARGET,
        event = "pathwatch_snapshot",
        peer = %peer_label,
        path_count = observed.paths.len(),
        selected = observed.selected.as_deref().unwrap_or("none"),
        selected_changed,
        min_path_rtt_ms = observed
            .min_rtt
            .map(|r| r.as_secs_f64() * 1000.0)
            .unwrap_or(f64::NAN),
        app_rtt_ms = app_rtt.as_secs_f64() * 1000.0,
    );
    *last_selected = observed.selected;
}

async fn wait_or_cancelled(cancel: &CancellationToken, dur: Duration) -> bool {
    tokio::select! {
        _ = cancel.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}
