use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
