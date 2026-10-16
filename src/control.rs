use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use fabric_config::sync::status::{
    SyncEntryStatus, SyncPublishFile, SyncPublishedFile, SyncRuntimeStatus,
};

use crate::{
    config::DialOptions,
    error::ErrorFields,
    mux::CurrentConnectionHealth,
    telemetry::{PeerTelemetry, TelemetryWindow},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Status,
    ReachabilityStatus,
    ReloadPeers,
    RuntimePeers {
        request: crate::runtime_peers::Request,
    },
    /// Wait for cached peer transitions without polling the network.
    PeerEvents {
        #[serde(default)]
        instance: Option<String>,
        #[serde(default)]
        after: u64,
        #[serde(default)]
        timeout_ms: u64,
    },
    Undial {
        peer: String,
        protocol: String,
    },
    Expose {
        protocol: String,
        socket: PathBuf,
        #[serde(default = "default_persist")]
        persist: bool,
        /// Send the connecting peer's identity as a first line to the socket.
        #[serde(default)]
        peer_header: bool,
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
        #[serde(default, skip_serializing_if = "DialOptions::is_default")]
        options: DialOptions,
    },
    DialTcp {
        peer: String,
        protocol: String,
        bind: String,
        #[serde(default, skip_serializing_if = "DialOptions::is_default")]
        options: DialOptions,
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

/// A configured listener skipped at daemon startup. The declaration remains
/// in config so it can be retried after its path or port is repaired.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DialRestoreFailure {
    pub peer: String,
    pub protocol: String,
    pub tcp: Option<String>,
    pub code: String,
    pub class: String,
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlResponse {
    RuntimePeers {
        reply: crate::runtime_peers::Reply,
    },
    PeerEvents {
        #[serde(flatten)]
        batch: crate::presence::PeerEvents,
    },

    Ok,
    Status {
        node_id: String,
        endpoint_addr: serde_json::Value,
        exposed_protocols: Vec<String>,
        dial_sockets: Vec<PathBuf>,
        allow_shell: bool,
        #[serde(default)]
        allow_exec: bool,
        #[serde(default)]
        audit: Option<crate::audit::AuditStatus>,
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
        #[serde(default)]
        audit: Option<crate::audit::AuditStatus>,
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
        #[serde(default)]
        dial_restore_failures: Vec<DialRestoreFailure>,
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
        /// The code of a failed probe, when the daemon knows it; absent from
        /// daemons before codes and for answers the command classifies itself.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<String>,
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
        /// The failure's code (docs/errors.md), absent from daemons before codes.
        #[serde(flatten)]
        fields: ErrorFields,
    },
}

impl ControlResponse {
    /// The reply for a request that failed, carrying the failure's code.
    pub fn error(error: &anyhow::Error) -> Self {
        Self::Error {
            message: format!("{error:#}"),
            fields: ErrorFields::of(error),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerReachability {
    pub id: String,
    pub name: Option<String>,
    /// This peer is expected to disconnect and return.
    #[serde(default)]
    pub roaming: bool,
    /// This peer only connects here and is never probed. `reachable` then says
    /// whether it is connected right now.
    #[serde(default)]
    pub inbound_only: bool,
    /// The peer is up and trusts this machine, but refused the echo stream
    /// because it has not granted `echo`. `reachable` is then true and there is
    /// no round trip to report. An older daemon never sets it.
    #[serde(default)]
    pub echo_denied: bool,
    pub reachable: bool,
    /// The service sampled for these latency/transport values. Null means no
    /// sample (inbound-only), or a reply from a daemon predating this field.
    #[serde(default)]
    pub measured: Option<String>,
    /// Set by a fresh exact-service status probe; cached echo health retains
    /// its existing reachable/echo_denied fields instead.
    #[serde(default)]
    pub probe_outcome: Option<String>,
    #[serde(default)]
    pub probe_code: Option<String>,
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
            companion_socket: Some(PathBuf::from("/home/example/run/sync-companion.sock")),
        };
        let daemon_hello = ControlRequest::SyncCompanionHello {
            version: "0.2.16+abc1234".into(),
            sync_ipc_magic: "fabric/sync-ipc".into(),
            sync_ipc_version: 1,
            companion_socket: Some(PathBuf::from("/home/example/run/sync-companion.sock")),
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
                Some(PathBuf::from("/home/example/run/sync-ipc.sock")),
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
            fields: Default::default(),
        };
        assert_eq!(
            serde_json::from_value::<CompanionResponse>(serde_json::to_value(&error).unwrap())
                .unwrap(),
            CompanionResponse::Error {
                message: "fabric-sync 0.2.15 cannot attach to 0.2.16".into(),
                fields: Default::default(),
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
                    log: PathBuf::from("/home/example/logs/restart.log"),
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
            stopped_peers: vec![("vps".into(), "denied".into())],
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
            audit: None,
            allow_exec: false,
            peers: Vec::new(),
            connection_telemetry: BTreeMap::from([("desktop".to_string(), populated_peer())]),
            connection_telemetry_window: TelemetryWindow {
                started_unix_seconds: Some(1_788_369_000),
                reset_reason: None,
            },
            current_connection_health: BTreeMap::from([(
                "desktop".to_string(),
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
            dial_restore_failures: Vec::new(),
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
                let peer = &connection_telemetry["desktop"];
                assert_eq!(peer.losses, 1);
                assert_eq!(peer.probes_reachable, 80);
                assert!(
                    peer.reconnect.total_micros > 0,
                    "the measured total must cross the wire, not just its count"
                );
                assert_eq!(peer.probe_latency["relay"].samples, 1);
                assert_eq!(
                    current_connection_health["desktop"].consecutive_attach_failures,
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

#[cfg(test)]
mod runtime_peer_schema_tests {
    use super::*;
    use crate::{
        error::{ErrorCode, FabricError},
        runtime_peers::*,
    };

    #[test]
    fn runtime_peer_control_golden_preserves_sources_receipts_and_error_classes() {
        let node_id = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let sources = vec![
            Source::Static {
                path: "/config/peers.toml".into(),
            },
            Source::Runtime {
                owner: "controller-b".into(),
            },
        ];
        let peer = EffectivePeer {
            node_id,
            name: Some("laptop".into()),
            direction: Direction::InboundOnly,
            trust_sources: sources.clone(),
            direction_sources: sources.clone(),
            services: BTreeMap::from([("work/0".into(), sources)]),
        };
        let request = ControlRequest::RuntimePeers {
            request: Request::Batch {
                transaction: Transaction {
                    owner: "controller-a".into(),
                    request_id: "replace-17".into(),
                    expected_revision: 6,
                    operations: vec![Operation::RemovePeer { node_id }],
                },
            },
        };
        let list = ControlResponse::RuntimePeers {
            reply: Reply {
                snapshot: Some(Snapshot {
                    instance: "instance-a".into(),
                    policy_revision: 9,
                    static_digest: "digest-a".into(),
                    peers: vec![peer.clone()],
                    next_cursor: None,
                    fault: None,
                }),
                mutation: None,
                affected: vec![],
                error: None,
            },
        };
        let mutation = ControlResponse::RuntimePeers {
            reply: Reply {
                snapshot: None,
                mutation: Some(MutationReply {
                    request_id: "replace-17".into(),
                    committed_revision: 7,
                    current_revision: 9,
                    affected: vec![peer.clone()],
                    removals: vec![Removal {
                        node_id,
                        owner_record_removed: true,
                        effective_trust_removed: false,
                    }],
                    cancellation: Cancellation::empty(),
                }),
                affected: vec![peer],
                error: None,
            },
        };
        let mut restarted = match &mutation {
            ControlResponse::RuntimePeers { reply } => reply.clone(),
            _ => unreachable!(),
        };
        restarted.mutation.as_mut().unwrap().cancellation = Cancellation {
            state: CancellationState::TimedOut,
            admissions: vec![17],
            remaining_admissions: vec![17],
            restart_evidence: Some(RestartEvidence {
                daemon_instance_changed: true,
                child_exit_observed: false,
            }),
            ..Cancellation::default()
        };
        restarted.error = Some(PolicyError::of(
            &FabricError::new(
                ErrorCode::CancellationTimeout,
                "daemon restarted; child exit was not observed",
            )
            .into(),
        ));
        let restarted = ControlResponse::RuntimePeers { reply: restarted };
        let errors: Vec<_> = [
            ErrorCode::RuntimePeersDisabled,
            ErrorCode::PeerManagedStatically,
            ErrorCode::PeerOwnedElsewhere,
            ErrorCode::PolicyRevisionConflict,
            ErrorCode::RuntimeStateUnavailable,
            ErrorCode::RuntimeCommitIndeterminate,
            ErrorCode::RuntimeReceiptCapacity,
            ErrorCode::RequestIdConflict,
            ErrorCode::CancellationTimeout,
        ]
        .into_iter()
        .map(|code| ControlResponse::RuntimePeers {
            reply: Reply {
                snapshot: None,
                mutation: None,
                affected: vec![],
                error: Some(PolicyError::of(
                    &FabricError::new(code, "example failure").into(),
                )),
            },
        })
        .collect();
        let value = serde_json::json!({ "request": request, "list_reply": list, "mutation_reply": mutation, "restart_reply": restarted, "coded_errors": errors });
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/json/runtime_peer_control.json");
        if std::env::var_os("UPDATE_GOLDEN").is_some() {
            std::fs::write(
                &path,
                format!("{}\n", serde_json::to_string_pretty(&value).unwrap()),
            )
            .unwrap();
        }
        let expected: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(value, expected);
        let _: ControlRequest = serde_json::from_value(value["request"].clone()).unwrap();
        for key in ["list_reply", "mutation_reply", "restart_reply"] {
            let _: ControlResponse = serde_json::from_value(value[key].clone()).unwrap();
        }
        for error in value["coded_errors"].as_array().unwrap() {
            let _: ControlResponse = serde_json::from_value(error.clone()).unwrap();
        }
    }
}
