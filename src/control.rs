use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use fabric_config::sync::status::{
    SyncEntryStatus, SyncPublishFile, SyncPublishedFile, SyncRuntimeStatus,
};

use crate::{
    mux::CurrentConnectionHealth,
    telemetry::{PeerTelemetry, TelemetryWindow},
};

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
    /// Report whether this build can host the companion sync process.
    SyncIpcCompatibility,
    /// Register one heartbeat from the companion. When the daemon delegates
    /// sync, the answer carries the session the companion needs to attach:
    /// the daemon's bridge socket, the instance nonce, and the sync author.
    SyncCompanionHello {
        version: String,
        sync_ipc_magic: String,
        sync_ipc_version: u16,
        /// Where the daemon can reach this companion's bridge listener.
        #[serde(default)]
        companion_socket: Option<PathBuf>,
    },
    /// Report which process owns sync and whether its companion is present.
    SyncRuntimeStatus,
    /// Publish staged files into one sync entry under its operation guard, so
    /// the set becomes one scan, one persist, and one reconcile on each peer.
    ///
    /// Carries the bytes rather than a path: the daemon writes exactly what the
    /// caller reviewed, and a staged file is small by the nature of the thing
    /// being staged.
    SyncPublish {
        name: String,
        files: Vec<SyncPublishFile>,
        #[serde(default)]
        force: bool,
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
        /// Health for each current shared connection. Replacement resets it.
        #[serde(default)]
        current_connection_health: BTreeMap<String, CurrentConnectionHealth>,
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
        runtime: SyncRuntimeStatus,
    },
    SyncIpcCompatibility {
        version: String,
        sync_ipc_magic: String,
        sync_ipc_version: u16,
        /// `embedded` when this daemon runs its own engine, `companion` when
        /// it delegates sync to the companion process.
        owner: String,
        /// The daemon-instance nonce, only when `owner` is `companion` and the
        /// caller is the companion. A restart mints a new one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nonce: Option<String>,
        /// The daemon's bridge socket for the companion's requests.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        daemon_socket: Option<PathBuf>,
        /// This daemon's public node id, the stable sync author.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
    },
    SyncRuntimeStatus {
        runtime: SyncRuntimeStatus,
    },
    SyncPublished {
        files: Vec<SyncPublishedFile>,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerReachability {
    pub id: String,
    pub name: Option<String>,
    /// This peer is expected to disconnect and return.
    #[serde(default)]
    pub roaming: bool,
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
    use fabric_config::daemon_control::{
        Request as CompanionRequest, Response as CompanionResponse,
    };

    /// The companion writes its two control messages without this crate. Each
    /// must be the same JSON as the daemon's own definition, in both
    /// directions, or a companion and a daemon of one build stop talking.
    #[test]
    fn the_companions_control_messages_are_the_daemons() {
        let hello = CompanionRequest::SyncCompanionHello {
            version: "0.2.16+abc1234".into(),
            sync_ipc_magic: "fabric/sync-ipc".into(),
            sync_ipc_version: 1,
            companion_socket: Some(PathBuf::from("/home/run/sync-companion.sock")),
        };
        let daemon_hello = ControlRequest::SyncCompanionHello {
            version: "0.2.16+abc1234".into(),
            sync_ipc_magic: "fabric/sync-ipc".into(),
            sync_ipc_version: 1,
            companion_socket: Some(PathBuf::from("/home/run/sync-companion.sock")),
        };
        assert_eq!(
            serde_json::to_value(&hello).unwrap(),
            serde_json::to_value(&daemon_hello).unwrap()
        );
        let parsed: ControlRequest =
            serde_json::from_slice(&serde_json::to_vec(&hello).unwrap()).unwrap();
        assert!(matches!(parsed, ControlRequest::SyncCompanionHello { .. }));
        assert_eq!(
            serde_json::to_value(CompanionRequest::SyncIpcCompatibility).unwrap(),
            serde_json::to_value(ControlRequest::SyncIpcCompatibility).unwrap()
        );

        for (nonce, daemon_socket, node_id) in [
            (
                Some("ab".repeat(24)),
                Some(PathBuf::from("/home/run/sync-ipc.sock")),
                Some("cd".repeat(32)),
            ),
            (None, None, None),
        ] {
            let daemon = ControlResponse::SyncIpcCompatibility {
                version: "0.2.16+abc1234".into(),
                sync_ipc_magic: "fabric/sync-ipc".into(),
                sync_ipc_version: 1,
                owner: "companion".into(),
                nonce: nonce.clone(),
                daemon_socket: daemon_socket.clone(),
                node_id: node_id.clone(),
            };
            let companion = CompanionResponse::SyncIpcCompatibility {
                version: "0.2.16+abc1234".into(),
                sync_ipc_magic: "fabric/sync-ipc".into(),
                sync_ipc_version: 1,
                owner: "companion".into(),
                nonce,
                daemon_socket,
                node_id,
            };
            assert_eq!(
                serde_json::from_value::<CompanionResponse>(serde_json::to_value(&daemon).unwrap())
                    .unwrap(),
                companion
            );
            assert_eq!(
                serde_json::to_value(&companion).unwrap(),
                serde_json::to_value(&daemon).unwrap()
            );
        }
        let error = ControlResponse::Error {
            message: "fabric-sync 0.2.15 cannot attach to 0.2.16".into(),
        };
        assert_eq!(
            serde_json::from_value::<CompanionResponse>(serde_json::to_value(&error).unwrap())
                .unwrap(),
            CompanionResponse::Error {
                message: "fabric-sync 0.2.15 cannot attach to 0.2.16".into()
            }
        );
        assert_eq!(
            serde_json::from_value::<CompanionResponse>(
                serde_json::to_value(ControlResponse::Ok).unwrap()
            )
            .unwrap(),
            CompanionResponse::Ok
        );
        assert_eq!(
            serde_json::from_value::<CompanionResponse>(
                serde_json::to_value(ControlResponse::Restarting {
                    log: PathBuf::from("/home/logs/restart.log"),
                    allow_shell: false,
                })
                .unwrap()
            )
            .unwrap(),
            CompanionResponse::Other
        );

        // The `fabric sync` commands' three.
        for (companion, daemon) in [
            (CompanionRequest::SyncReload, ControlRequest::SyncReload),
            (CompanionRequest::SyncStatus, ControlRequest::SyncStatus),
            (
                CompanionRequest::SyncPublish {
                    name: "catalog".into(),
                    files: vec![SyncPublishFile {
                        rel: "a/b.md".into(),
                        bytes: b"hello".to_vec(),
                        executable: true,
                        base: Some("ef".repeat(32)),
                    }],
                    force: true,
                },
                ControlRequest::SyncPublish {
                    name: "catalog".into(),
                    files: vec![SyncPublishFile {
                        rel: "a/b.md".into(),
                        bytes: b"hello".to_vec(),
                        executable: true,
                        base: Some("ef".repeat(32)),
                    }],
                    force: true,
                },
            ),
        ] {
            assert_eq!(
                serde_json::to_value(&companion).unwrap(),
                serde_json::to_value(&daemon).unwrap()
            );
        }
        let entry = SyncEntryStatus {
            name: "catalog".into(),
            folder: "/catalog".into(),
            digest: "0123456789abcdef".into(),
            stopped_peers: vec![("hetz".into(), "denied".into())],
            sync_passes: 7,
            ..SyncEntryStatus::default()
        };
        let status = ControlResponse::SyncStatus {
            entries: vec![entry.clone()],
            runtime: SyncRuntimeStatus::new("companion", "active"),
        };
        assert_eq!(
            serde_json::from_value::<CompanionResponse>(serde_json::to_value(&status).unwrap())
                .unwrap(),
            CompanionResponse::SyncStatus {
                entries: vec![entry],
                runtime: SyncRuntimeStatus::new("companion", "active"),
            }
        );
        let published = ControlResponse::SyncPublished {
            files: vec![SyncPublishedFile {
                rel: "a/b.md".into(),
                version: 3,
                hash: "ab".repeat(32),
            }],
        };
        assert_eq!(
            serde_json::from_value::<CompanionResponse>(serde_json::to_value(&published).unwrap())
                .unwrap(),
            CompanionResponse::SyncPublished {
                files: vec![SyncPublishedFile {
                    rel: "a/b.md".into(),
                    version: 3,
                    hash: "ab".repeat(32),
                }],
            }
        );
    }

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
            current_connection_health: BTreeMap::from([(
                "droppy".to_string(),
                CurrentConnectionHealth {
                    connection_id: 7,
                    age_millis: 12_000,
                    consecutive_attach_failures: 2,
                    last_attach_failure_phase: Some("hello".to_string()),
                    last_attach_failure_duration_millis: Some(750),
                    last_application_progress_millis_ago: Some(500),
                },
            )]),
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
                current_connection_health,
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
                    current_connection_health["droppy"].consecutive_attach_failures,
                    2
                );
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
