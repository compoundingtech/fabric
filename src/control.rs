use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::telemetry::{PeerTelemetry, TelemetryWindow};

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
    /// Open a reusable local socket for the Fabric Git smart protocol.
    Git {
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
    /// Send one local file to a peer's inbox.
    ///
    /// Carries the PATH rather than the bytes. The daemon runs as the same user
    /// and reads the file itself, so a large transfer never crosses the control
    /// socket.
    SendFile {
        peer: String,
        path: std::path::PathBuf,
        /// The relative name it should land under in the peer's inbox.
        name: String,
    },
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
        /// The time range and reset context for the cumulative counters.
        /// Defaulted so a new client still decodes an older daemon's reply.
        #[serde(default)]
        connection_telemetry_window: TelemetryWindow,
        /// Dial permits in use and the cap. Every shell, exec and dial holds
        /// one for the life of its session, and when all are held every new
        /// one waits with no error. Defaulted for an older daemon's reply.
        #[serde(default)]
        active_dial_handlers: usize,
        #[serde(default)]
        max_dial_handlers: usize,
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
    Git {
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
    SentFile {
        peer: String,
        name: String,
        bytes: u64,
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
    /// Existing paths the last scan could not read as syncable files.
    #[serde(default)]
    pub scan_issues: Vec<(String, String)>,
    /// Monotonic full-folder scan attempts for this entry instance.
    #[serde(default)]
    pub full_scans: u64,
    /// Monotonic exact-manifest, complete-content inbound fast paths.
    #[serde(default)]
    pub inbound_noop_transactions: u64,
    /// Monotonic inbound transactions that selected guarded reconciliation.
    #[serde(default)]
    pub inbound_guarded_transactions: u64,
    /// Calls to `sync_once`, and the only correct denominator for a per-pass
    /// cost. NOT `full_scans`: `scan_entry` also runs for inbound transactions,
    /// so no constant converts `full_scans` into a pass count.
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
    /// Every byte this entry put on or took off the wire, cumulative, INCLUDING
    /// the manifest shipped on every pass. Counted client-side, so summing
    /// across the fleet counts each transfer once.
    #[serde(default)]
    pub reconcile_wire_bytes: u64,
    /// Peer reconciles that returned an error, cumulative. A number that MOVES
    /// between two samples is a fault happening now; a large total on an old
    /// daemon may be history.
    #[serde(default)]
    pub reconcile_failures: u64,
    /// Why the tombstone sweep did or did not forget anything, as a short
    /// stable token. Empty from a daemon that predates the field, which is why
    /// it carries `#[serde(default)]` like the counters above.
    #[serde(default)]
    pub sweep: String,
    /// Peers this entry is NOT syncing with, and why, as `peer:reason`.
    ///
    /// Empty is healthy. `denied` means a person must edit `peers.toml`;
    /// `unreachable` means the network will fix itself. A reader must be able to
    /// tell a chore from weather without running a second command.
    #[serde(default)]
    pub stopped_peers: Vec<(String, String)>,
    /// Payloads this node SENT carrying its whole manifest, whatever the
    /// reason: first contact, a peer too old for deltas, a restart, or a cursor
    /// that stalled until its delta grew back to the whole manifest.
    ///
    /// Read it BESIDE `reconcile_wire_bytes`, which is counted on the initiator
    /// and includes the responder's reply. High bytes with a low count here
    /// means this machine is RECEIVING full payloads, not sending them.
    #[serde(default)]
    pub full_payload_sends: u64,
    /// Bytes of file content the daemon holds in memory for this entry. It was
    /// unbounded once, and this is the number that would have said so.
    #[serde(default)]
    pub content_bytes: u64,
    /// Reconciles that fell back to full state because a payload was
    /// incomplete. Zero is healthy. A number that RISES between two samples is a
    /// bug report: a cursor described state a peer did not hold.
    #[serde(default)]
    pub delta_fallbacks: u64,
    /// Lattice-point fingerprint of this entry's manifest. Empty from a daemon
    /// that predates the field. Compare it ACROSS peers: equal means converged,
    /// unequal means diverged. Counts cannot tell you this.
    #[serde(default)]
    pub digest: String,
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
            connection_telemetry_window: TelemetryWindow {
                started_unix_seconds: Some(1_788_369_000),
                reset_reason: None,
            },
            active_dial_handlers: 0,
            max_dial_handlers: 32,
        };

        let bytes = serde_json::to_vec(&response)
            .expect("the status response must serialize with telemetry present");
        let decoded: ControlResponse =
            serde_json::from_slice(&bytes).expect("the status response must decode");

        match decoded {
            ControlResponse::ReachabilityStatus {
                connection_telemetry,
                connection_telemetry_window,
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
                assert_eq!(
                    connection_telemetry_window.started_unix_seconds,
                    Some(1_788_369_000),
                    "the counter window must cross the real control wire"
                );
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
