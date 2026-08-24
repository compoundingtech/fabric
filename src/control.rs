use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::telemetry::PeerTelemetry;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Status,
    ReachabilityStatus,
    ReloadPeers,
    Expose {
        protocol: String,
        socket: PathBuf,
        #[serde(default = "default_persist")]
        persist: bool,
    },
    ExposeExec {
        protocol: String,
        argv: Vec<String>,
        max_children: usize,
        #[serde(default = "default_persist")]
        persist: bool,
    },
    ExposeTcp {
        protocol: String,
        addr: String,
        #[serde(default = "default_persist")]
        persist: bool,
    },
    Unexpose {
        protocol: String,
    },
    Dial {
        peer: String,
        protocol: String,
    },
    DialTcp {
        peer: String,
        protocol: String,
        bind: String,
    },
    Ping {
        peer: String,
    },
    /// One-shot service probe: a single ALPN connect against a peer, bounded by
    /// the caller's own deadline. Deliberately not a dial: it installs no
    /// listener, keeps no state, and never consults the shared dial backoff.
    Probe {
        peer: String,
        protocol: String,
        timeout_ms: u64,
    },
    Shell {
        peer: String,
    },
    Exec {
        peer: String,
    },
    DropTunnelConnections,
    SetTunnelBlocked {
        blocked: bool,
    },
    ReapTunnelSessions {
        ttl_millis: u64,
    },
    RecycleEndpoint,
    Restart {
        allow_shell: Option<bool>,
    },
    /// Re-read syncs.toml into the running daemon (mirrors ReloadPeers).
    SyncReload,
    /// Report the daemon's configured sync entries and their state.
    SyncStatus,
    Shutdown,
}

fn default_persist() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlResponse {
    Ok,
    Status {
        node_id: String,
        endpoint_addr: serde_json::Value,
        exposed_protocols: Vec<String>,
        dial_sockets: Vec<PathBuf>,
        allow_shell: bool,
        #[serde(default)]
        allow_exec: bool,
    },
    ReachabilityStatus {
        version: String,
        node_id: String,
        endpoint_addr: serde_json::Value,
        exposed_protocols: Vec<String>,
        dial_sockets: Vec<PathBuf>,
        allow_shell: bool,
        #[serde(default)]
        allow_exec: bool,
        peers: Vec<PeerReachability>,
        /// Durable loss/resume counters, keyed by peer label. Defaulted so an
        /// older client still decodes a newer daemon's reply.
        #[serde(default)]
        connection_telemetry: BTreeMap<String, PeerTelemetry>,
    },
    Restarting {
        log: PathBuf,
        allow_shell: bool,
    },
    Dial {
        socket: PathBuf,
    },
    DialTcp {
        addr: String,
    },
    Shell {
        socket: PathBuf,
    },
    Exec {
        socket: PathBuf,
    },
    Pong {
        peer: String,
        bytes: usize,
        round_trip_micros: u64,
        transport: Option<String>,
    },
    ProbeResult {
        peer: String,
        peer_id: String,
        protocol: String,
        /// supported | unsupported | unreachable | timeout
        outcome: String,
        round_trip_micros: Option<u64>,
        transport: Option<String>,
        error: Option<String>,
    },
    SyncStatus {
        entries: Vec<SyncEntryStatus>,
    },
    Error {
        message: String,
    },
}

/// One configured sync entry's status, for `fabric sync ls`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEntryStatus {
    pub name: String,
    pub folder: String,
    pub policy: String,
    pub peers: String,
    /// Legacy logical-Present count retained on the local control wire so an
    /// older client can still decode a newer daemon response.
    pub files: usize,
    #[serde(default)]
    pub present: usize,
    #[serde(default)]
    pub tombstones: usize,
    #[serde(default)]
    pub observed: usize,
    #[serde(default)]
    pub missing: usize,
    #[serde(default)]
    pub unexpected: usize,
    #[serde(default)]
    pub mismatched: usize,
    /// Monotonic full-folder scan attempts for this entry instance.
    #[serde(default)]
    pub full_scans: u64,
    /// Monotonic exact-manifest, complete-content inbound fast paths.
    #[serde(default)]
    pub inbound_noop_transactions: u64,
    /// Monotonic inbound transactions that selected guarded reconciliation.
    #[serde(default)]
    pub inbound_guarded_transactions: u64,
    /// Calls to `sync_once`. NOT `full_scans`, which counts two per call.
    #[serde(default)]
    pub sync_passes: u64,
    /// Cumulative microseconds inside each phase of `sync_once`. Two samples
    /// and a division describe the present; a total describes the past.
    #[serde(default)]
    pub scan_micros: u64,
    #[serde(default)]
    pub materialize_micros: u64,
    #[serde(default)]
    pub persist_micros: u64,
    #[serde(default)]
    pub reconcile_micros: u64,
    /// Why the tombstone sweep did or did not forget anything, as a short
    /// stable token. Empty from a daemon that predates the field, which is why
    /// it carries `#[serde(default)]` like the counters above.
    #[serde(default)]
    pub sweep: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerReachability {
    pub id: String,
    pub name: Option<String>,
    pub reachable: bool,
    pub bytes: Option<usize>,
    pub round_trip_micros: Option<u64>,
    pub transport: Option<String>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{LatencySummary, PeerTelemetry};

    fn populated_peer() -> PeerTelemetry {
        let mut reconnect = LatencySummary::default();
        reconnect.record(1_500_000);
        let mut probe = LatencySummary::default();
        probe.record(64_000);
        PeerTelemetry {
            losses: 1,
            resumes: 1,
            reconnect_attempts: 2,
            reconnect,
            probe_latency: BTreeMap::from([("relay".to_string(), probe)]),
            probes_reachable: 80,
            ..PeerTelemetry::default()
        }
    }

    /// `fabric status` must survive the control protocol with telemetry present.
    ///
    /// This is the test whose absence shipped a broken `fabric status`. The
    /// counters were fine as plain JSON, and a test proving that passed while
    /// the command was broken in production. `ControlResponse` is an
    /// INTERNALLY TAGGED enum, so serde routes it through its `Content` buffer,
    /// and that buffer has no `u128`. A `u128` field therefore failed with
    /// "u128 is not supported" only once a peer had been probed and the map was
    /// no longer empty.
    ///
    /// Two things make this catch what the earlier test missed: it serializes
    /// the real `ControlResponse`, not the inner struct, and it starts from a
    /// POPULATED map, because an empty one is exactly the case that always
    /// worked and that a fresh-node hand check exercises.
    #[test]
    fn a_reachability_status_carrying_telemetry_round_trips() {
        let response = ControlResponse::ReachabilityStatus {
            version: "0.2.0+test".to_string(),
            node_id: "node".to_string(),
            endpoint_addr: serde_json::json!({"id": "node"}),
            exposed_protocols: vec!["audit/echo".to_string()],
            dial_sockets: vec![PathBuf::from("/tmp/dial.sock")],
            allow_shell: true,
            allow_exec: false,
            peers: Vec::new(),
            connection_telemetry: BTreeMap::from([("droppy".to_string(), populated_peer())]),
        };

        let bytes = serde_json::to_vec(&response)
            .expect("the status response must serialize with telemetry present");
        let decoded: ControlResponse =
            serde_json::from_slice(&bytes).expect("the status response must decode");

        match decoded {
            ControlResponse::ReachabilityStatus {
                connection_telemetry,
                ..
            } => {
                let peer = &connection_telemetry["droppy"];
                assert_eq!(peer.losses, 1);
                assert_eq!(peer.probes_reachable, 80);
                assert!(
                    peer.reconnect.total_micros > 0,
                    "the measured total must cross the wire, not just its count"
                );
                assert_eq!(peer.probe_latency["relay"].samples, 1);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
