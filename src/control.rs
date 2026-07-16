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
    Shell {
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
    },
    ReachabilityStatus {
        version: String,
        node_id: String,
        endpoint_addr: serde_json::Value,
        exposed_protocols: Vec<String>,
        dial_sockets: Vec<PathBuf>,
        allow_shell: bool,
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
    Pong {
        peer: String,
        bytes: usize,
        round_trip_micros: u64,
        transport: Option<String>,
    },
    Error {
        message: String,
    },
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
