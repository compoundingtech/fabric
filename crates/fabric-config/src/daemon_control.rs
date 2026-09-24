//! The messages the sync side sends on the daemon's control socket: the
//! companion's hello and compatibility check, and the `fabric sync` commands'
//! status, reload and publish.
//!
//! The daemon's control protocol has many more; they live with the daemon.
//! These are written here in the same JSON, so `fabric-sync` can speak them
//! without the daemon's crate, and a test on the daemon's side holds the two
//! definitions to one wire form.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

use crate::{
    FabricHome,
    sync::status::{SyncEntryStatus, SyncPublishFile, SyncPublishedFile, SyncRuntimeStatus},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Request {
    /// Re-read `syncs.toml` into the running daemon.
    SyncReload,
    /// Every configured entry's live state.
    SyncStatus,
    /// Which process owns sync, and whether this build may attach.
    SyncIpcCompatibility,
    /// One companion heartbeat. When the daemon delegates sync, the answer
    /// carries the session the companion needs to attach.
    SyncCompanionHello {
        version: String,
        sync_ipc_magic: String,
        sync_ipc_version: u16,
        /// Where the daemon can reach this companion's bridge listener.
        #[serde(default)]
        companion_socket: Option<PathBuf>,
    },
    /// Publish staged files into one sync entry as one set.
    SyncPublish {
        name: String,
        files: Vec<SyncPublishFile>,
        #[serde(default)]
        force: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Ok,
    SyncStatus {
        entries: Vec<SyncEntryStatus>,
        runtime: SyncRuntimeStatus,
    },
    SyncIpcCompatibility {
        version: String,
        sync_ipc_magic: String,
        sync_ipc_version: u16,
        /// `companion` when the daemon delegates sync to this process.
        owner: String,
        /// The daemon-instance nonce, for the companion only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nonce: Option<String>,
        /// The daemon's bridge socket for the companion's requests.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        daemon_socket: Option<PathBuf>,
        /// The daemon's public node id, the stable sync author.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
    },
    SyncPublished {
        files: Vec<SyncPublishedFile>,
    },
    Error {
        message: String,
    },
    /// Any other reply the daemon's protocol has.
    #[serde(other)]
    Other,
}

/// Send one request on the daemon's control socket and read its reply.
pub async fn send(home: &FabricHome, request: Request) -> Result<Response> {
    let mut stream = UnixStream::connect(home.control_socket_path())
        .await
        .with_context(|| "fabric daemon is not running; run `fabric up` first")?;
    let mut raw = serde_json::to_vec(&request)?;
    raw.push(b'\n');
    stream.write_all(&raw).await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let response: Response = serde_json::from_slice(&response)?;
    if let Response::Error { message } = response {
        bail!("{message}");
    }
    Ok(response)
}
