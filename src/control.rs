use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Status,
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
    Dial {
        socket: PathBuf,
    },
    Pong {
        peer: String,
        bytes: usize,
        round_trip_micros: u64,
    },
    Error {
        message: String,
    },
}
