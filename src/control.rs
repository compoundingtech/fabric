use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Status,
    ReachabilityStatus,
    ReloadPeers,
    Expose { protocol: String, socket: PathBuf },
    Dial { peer: String, protocol: String },
    Ping { peer: String },
    Shutdown,
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
    },
    ReachabilityStatus {
        node_id: String,
        endpoint_addr: serde_json::Value,
        exposed_protocols: Vec<String>,
        dial_sockets: Vec<PathBuf>,
        peers: Vec<PeerReachability>,
    },
    Dial {
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
