//! Opt-in daemon-owned policy. Static inputs are never written by this module.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use iroh::{EndpointAddr, EndpointId};
use serde::{Deserialize, Serialize};

use crate::{
    config::{FabricHome, Peer, PeerBook},
    error::{ErrorCode, FabricError},
};

mod exchange;
pub mod pairing;

pub const MAX_OPERATIONS: usize = 128;
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;
pub const PAGE_SIZE: usize = 256;
const RECEIPT_SECONDS: u64 = 24 * 60 * 60;
const MAX_RECEIPTS: usize = 1024;
const WITHDRAWAL_RECEIPTS: usize = 64;
const MAX_RECORDS: usize = 1024;
pub const MAX_EXPIRY_SECONDS: u64 = 600;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    #[default]
    Normal,
    InboundOnly,
    DialOnly,
}
impl Direction {
    fn of(peer: &Peer) -> Self {
        if peer.inbound_only {
            Self::InboundOnly
        } else if peer.dial_only {
            Self::DialOnly
        } else {
            Self::Normal
        }
    }
    fn apply(self, peer: &mut Peer) {
        peer.inbound_only = self == Self::InboundOnly;
        peer.dial_only = self == Self::DialOnly;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Operation {
    AddPeer {
        node_id: EndpointId,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        direction: Option<Direction>,
        #[serde(default)]
        addr: Option<EndpointAddr>,
    },
    SetGrants {
        node_id: EndpointId,
        grants: Vec<String>,
    },
    SetExpiry {
        node_id: EndpointId,
        expires_at: i64,
    },
    RemovePeer {
        node_id: EndpointId,
    },
}
impl Operation {
    pub(crate) fn node_id(&self) -> EndpointId {
        match self {
            Self::AddPeer { node_id, .. }
            | Self::SetGrants { node_id, .. }
            | Self::SetExpiry { node_id, .. }
            | Self::RemovePeer { node_id } => *node_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    pub owner: String,
    pub request_id: String,
    pub expected_revision: u64,
    pub operations: Vec<Operation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    List {
        #[serde(default)]
        cursor: Option<Cursor>,
    },
    Batch {
        #[serde(flatten)]
        transaction: Transaction,
    },
    AddPeer {
        owner: String,
        request_id: String,
        expected_revision: u64,
        node_id: EndpointId,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        direction: Option<Direction>,
        #[serde(default)]
        addr: Option<EndpointAddr>,
    },
    SetGrants {
        owner: String,
        request_id: String,
        expected_revision: u64,
        node_id: EndpointId,
        grants: Vec<String>,
    },
    SetExpiry {
        owner: String,
        request_id: String,
        expected_revision: u64,
        node_id: EndpointId,
        expires_at: i64,
    },
    RemovePeer {
        owner: String,
        request_id: String,
        expected_revision: u64,
        node_id: EndpointId,
    },
    Cancellation {
        request_id: String,
    },
}
impl Request {
    pub fn into_transaction(self) -> Option<Transaction> {
        let (owner, request_id, expected_revision, operation) = match self {
            Self::Batch { transaction } => return Some(transaction),
            Self::AddPeer {
                owner,
                request_id,
                expected_revision,
                node_id,
                name,
                direction,
                addr,
            } => (
                owner,
                request_id,
                expected_revision,
                Operation::AddPeer {
                    node_id,
                    name,
                    direction,
                    addr,
                },
            ),
            Self::SetGrants {
                owner,
                request_id,
                expected_revision,
                node_id,
                grants,
            } => (
                owner,
                request_id,
                expected_revision,
                Operation::SetGrants { node_id, grants },
            ),
            Self::SetExpiry {
                owner,
                request_id,
                expected_revision,
                node_id,
                expires_at,
            } => (
                owner,
                request_id,
                expected_revision,
                Operation::SetExpiry { node_id, expires_at },
            ),
            Self::RemovePeer {
                owner,
                request_id,
                expected_revision,
                node_id,
            } => (
                owner,
                request_id,
                expected_revision,
                Operation::RemovePeer { node_id },
            ),
            _ => return None,
        };
        Some(Transaction {
            owner,
            request_id,
            expected_revision,
            operations: vec![operation],
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Source {
    Static { path: PathBuf },
    Runtime {
        owner: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expiry_request_id: Option<String>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectivePeer {
    pub node_id: EndpointId,
    pub name: Option<String>,
    pub direction: Direction,
    pub trust_sources: Vec<Source>,
    pub direction_sources: Vec<Source>,
    pub services: BTreeMap<String, Vec<Source>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    pub instance: String,
    pub revision: u64,
    pub after_node: EndpointId,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub instance: String,
    pub policy_revision: u64,
    pub static_digest: String,
    pub peers: Vec<EffectivePeer>,
    pub next_cursor: Option<Cursor>,
    pub fault: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationState {
    #[default]
    Pending,
    Complete,
    TimedOut,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RestartEvidence {
    pub daemon_instance_changed: bool,
    pub child_exit_observed: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Cancellation {
    pub state: CancellationState,
    pub admissions: Vec<u64>,
    pub sessions: Vec<String>,
    pub remaining_admissions: Vec<u64>,
    pub remaining_sessions: Vec<String>,
    #[serde(default)]
    pub restart_evidence: Option<RestartEvidence>,
}
impl Cancellation {
    pub fn empty() -> Self {
        Self {
            state: CancellationState::Complete,
            ..Self::default()
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Removal {
    pub node_id: EndpointId,
    pub owner_record_removed: bool,
    pub effective_trust_removed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationReply {
    pub request_id: String,
    pub committed_revision: u64,
    pub current_revision: u64,
    pub affected: Vec<EffectivePeer>,
    pub removals: Vec<Removal>,
    pub cancellation: Cancellation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyError {
    pub message: String,
    pub code: String,
    pub class: String,
}
impl PolicyError {
    pub fn of(error: &anyhow::Error) -> Self {
        let code = crate::error::code_of(error);
        Self {
            message: format!("{error:#}"),
            code: code.as_str().into(),
            class: code.class().as_str().into(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    pub snapshot: Option<Snapshot>,
    pub mutation: Option<MutationReply>,
    pub affected: Vec<EffectivePeer>,
    pub error: Option<PolicyError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    owner: String,
    peer: Peer,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expiry_request_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    request_id: String,
    digest: String,
    committed_revision: u64,
    created: u64,
    affected: Vec<EndpointId>,
    removals: Vec<Removal>,
    cancellation: Cancellation,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stored {
    format: u8,
    revision: u64,
    static_digest: String,
    records: Vec<Record>,
    receipts: Vec<Receipt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pairing: Vec<pairing::Attempt>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pairing_last_seen: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    expiry_last_seen: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone)]
pub(crate) struct StaticInput {
    pub book: PeerBook,
    path: PathBuf,
    digest: String,
}
impl StaticInput {
    pub fn load(home: &FabricHome) -> Result<Self> {
        let path = home.peers_path();
        let existing = match fs::symlink_metadata(&path) {
            Ok(_) => Some(path.clone()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                home.existing_peers_path()
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(existing) = existing {
            if existing != path {
                return Err(invalid(
                    "migrate the legacy peer path before enabling --runtime-peers",
                ));
            }
            let raw = fs::read(&path)
                .with_context(|| format!("read static policy {}", path.display()))?;
            let book = PeerBook::parse_read_only(std::str::from_utf8(&raw)?)?;
            Ok(Self {
                book,
                path,
                digest: blake3::hash(&raw).to_hex().to_string(),
            })
        } else {
            // Refuse embedded legacy policy rather than running write-on-load migration.
            if home.config_path().exists() {
                let raw = fs::read_to_string(home.config_path())?;
                let config: toml::Value = toml::from_str(&raw)?;
                if config
                    .get("peers")
                    .and_then(toml::Value::as_array)
                    .is_some_and(|peers| !peers.is_empty())
                {
                    return Err(invalid(
                        "migrate embedded peers before enabling --runtime-peers",
                    ));
                }
            }
            Ok(Self {
                book: PeerBook::default(),
                path,
                digest: blake3::hash(&[]).to_hex().to_string(),
            })
        }
    }
}

#[derive(Debug)]
pub(crate) struct RuntimePeers {
    path: PathBuf,
    instance: String,
    stored: Stored,
    static_input: StaticInput,
    fault: Option<String>,
    pairing_clock: (u64, std::time::Instant),
    expiry_seen: std::sync::Arc<std::sync::atomic::AtomicU64>,
    exchanges: BTreeMap<String, exchange::Exchange>,
    #[cfg(test)]
    fail_directory_sync: bool,
    #[cfg(test)]
    panic_confirmation_and_fault: (bool, bool),
}
pub(crate) struct Prepared {
    stored: Stored,
    pub book: PeerBook,
    pub replay: bool,
    pub no_op: Option<MutationReply>,
    expected_revision: u64,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn invalid(message: impl Into<String>) -> anyhow::Error {
    FabricError::new(ErrorCode::InvalidArgument, message).into()
}
fn unavailable(message: impl Into<String>) -> anyhow::Error {
    FabricError::new(ErrorCode::RuntimeStateUnavailable, message).into()
}
fn validate_token(value: &str, field: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(invalid(format!(
            "{field} must contain 1..128 bytes without control characters"
        )));
    }
    Ok(())
}

impl RuntimePeers {
    /// A bad runtime file keeps control available with all network policy closed.
    pub fn open(home: &FabricHome) -> (Self, PeerBook) {
        Self::open_with_ended_work(home, true, (now(), std::time::Instant::now()))
    }
    fn open_with_ended_work(
        home: &FabricHome,
        restarted: bool,
        pairing_clock: (u64, std::time::Instant),
    ) -> (Self, PeerBook) {
        let path = home.root().join("runtime-peers.toml");
        let placeholder = StaticInput {
            book: PeerBook::default(),
            path: home.peers_path(),
            digest: String::new(),
        };
        let mut store = Self {
            path,
            instance: format!("{:032x}", rand::random::<u128>()),
            stored: Stored {
                format: 1,
                revision: 0,
                static_digest: String::new(),
                records: vec![],
                receipts: vec![],
                pairing: vec![],
                expiry_last_seen: 0,
                pairing_last_seen: 0,
            },
            static_input: placeholder,
            fault: None,
            pairing_clock,
            expiry_seen: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(pairing_clock.0)),
            exchanges: BTreeMap::new(),
            #[cfg(test)]
            fail_directory_sync: false,
            #[cfg(test)]
            panic_confirmation_and_fault: (false, false),
        };
        let result = (|| -> Result<PeerBook> {
            store.static_input = StaticInput::load(home)?;
            // A dangling link is an unreadable existing state, not a new
            // daemon with no state. Never replace it with an empty policy.
            let exists = match fs::symlink_metadata(&store.path) {
                Ok(_) => true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            };
            if exists {
                use std::os::unix::fs::MetadataExt;
                let metadata = fs::metadata(&store.path)?;
                if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
                    return Err(unavailable(
                        "runtime policy must be owned by the daemon account and private (mode 0600)",
                    ));
                }
                store.stored = toml::from_str(&fs::read_to_string(&store.path)?)?;
                store.validate_stored()?;
            }
            let mut changed = !exists;
            if exists && store.stored.static_digest != store.static_input.digest {
                store.stored.revision = store
                    .stored
                    .revision
                    .checked_add(1)
                    .context("policy revision exhausted")?;
                changed = true;
            }
            store.stored.static_digest = store.static_input.digest.clone();
            for receipt in &mut store.stored.receipts {
                let cancellation = &mut receipt.cancellation;
                if cancellation.state == CancellationState::Complete {
                    continue;
                }
                if restarted {
                    if cancellation.state == CancellationState::Pending
                        || !cancellation.remaining_admissions.is_empty()
                        || !cancellation.remaining_sessions.is_empty()
                    {
                        cancellation.state = CancellationState::TimedOut;
                        cancellation.restart_evidence = Some(RestartEvidence {
                            daemon_instance_changed: true,
                            child_exit_observed: false,
                        });
                        changed = true;
                    }
                } else if cancellation.restart_evidence.is_none() {
                    // The live daemon proved its quarantined work ended. A
                    // prior crash's unknown children cannot use that proof.
                    if cancellation.state == CancellationState::Pending {
                        cancellation.state = CancellationState::Complete;
                    }
                    cancellation.remaining_admissions.clear();
                    cancellation.remaining_sessions.clear();
                    changed = true;
                }
            }
            let timestamp = store.pairing_timestamp();
            changed |= pairing::recover(&mut store.stored, timestamp, restarted);
            if now() < store.stored.expiry_last_seen {
                let deadline = store.stored.records.iter().filter_map(|record| record.expires_at)
                    .filter_map(|deadline| u64::try_from(deadline).ok()).max().unwrap_or(0);
                store.expiry_seen.fetch_max(deadline, std::sync::atomic::Ordering::Relaxed);
            }
            loop {
                let Some(transaction) = store.expired_transactions()?.into_iter().next() else { break; };
                let prepared = store.prepare(&transaction)?;
                store.commit(prepared, &transaction.request_id, Cancellation::empty())?;
            }
            let book = store.compose(&store.stored)?.0;
            if changed {
                store.persist(&store.stored)?;
            }
            Ok(book)
        })();
        match result {
            Ok(book) => (store, book),
            Err(error) => {
                store.fault = Some(format!("{error:#}"));
                (store, PeerBook::default())
            }
        }
    }

    fn validate_stored(&self) -> Result<()> {
        if self.stored.format != 1 {
            return Err(unavailable("unsupported runtime policy format"));
        }
        if self.stored.records.len() > MAX_RECORDS {
            return Err(unavailable("too many runtime owner/node records"));
        }
        let mut records = BTreeSet::new();
        for record in &self.stored.records {
            validate_token(&record.owner, "owner")?;
            if record.expires_at.is_some_and(|deadline| deadline <= 0) {
                // Reject impossible timestamps before converting them to a
                // duration or serializing them through TOML's signed integers.
                return Err(unavailable("invalid runtime record expires_at"));
            }
            if record.expires_at.is_some_and(|deadline| {
                u64::try_from(deadline).is_ok_and(|deadline| {
                    deadline.saturating_sub(self.stored.expiry_last_seen) > MAX_EXPIRY_SECONDS
                })
            }) {
                return Err(unavailable("runtime expiry exceeds its persisted 600-second bound"));
            }
            if record.expires_at.is_some() != record.expiry_request_id.is_some() {
                return Err(unavailable("runtime expiry requires its reserved cancellation receipt ID"));
            }
            if let Some(request_id) = &record.expiry_request_id {
                validate_token(request_id, "expiry_request_id")?;
                if self.stored.receipts.iter().any(|receipt| &receipt.request_id == request_id) {
                    return Err(unavailable("runtime expiry receipt ID is already committed"));
                }
            }
            if !records.insert((record.owner.clone(), record.peer.id)) {
                return Err(unavailable("duplicate owner/node runtime record"));
            }
        }
        let reserved = self.stored.records.iter().filter(|record| record.expires_at.is_some()).count();
        if self.stored.receipts.len().saturating_add(reserved) > MAX_RECEIPTS {
            return Err(unavailable("too many retained or reserved runtime receipts"));
        }
        let mut receipts = BTreeSet::new();
        for receipt in &self.stored.receipts {
            validate_token(&receipt.request_id, "request_id")?;
            if !receipts.insert(&receipt.request_id)
                || receipt.committed_revision > self.stored.revision
            {
                return Err(unavailable("invalid runtime receipt history"));
            }
        }
        pairing::validate(&self.stored.pairing)?;
        if self.stored.pairing.iter().any(|attempt| {
            attempt
                .status
                .completion
                .as_ref()
                .is_some_and(|receipt| receipt.policy_revision > self.stored.revision)
        }) {
            return Err(unavailable(
                "pairing completion revision exceeds current policy",
            ));
        }
        Ok(())
    }
    /// Caller must first drain quarantined work before reconciling an uncertain commit.
    pub fn reconcile(&mut self, home: &FabricHome) -> Result<PeerBook> {
        // Reconciliation is still the same daemon lifetime. Carry the original
        // monotonic anchor into recovery, before it decides whether permits expired.
        let (mut loaded, book) = Self::open_with_ended_work(home, false, self.pairing_clock);
        if let Some(error) = loaded.fault.take() {
            self.fault = Some(error.clone());
            return Err(unavailable(error));
        }
        loaded.instance = self.instance.clone();
        loaded.pairing_clock = self.pairing_clock;
        loaded.expiry_seen.fetch_max(self.expiry_timestamp(), std::sync::atomic::Ordering::Relaxed);
        loaded.exchanges = std::mem::take(&mut self.exchanges);
        loaded.prune_exchanges();
        *self = loaded;
        Ok(book)
    }
    pub fn access_for(&self, ids: &[EndpointId]) -> Vec<EffectivePeer> {
        if self.fault.is_some() {
            return vec![];
        }
        self.compose(&self.stored)
            .map(|(_, peers)| {
                peers
                    .into_iter()
                    .filter(|p| ids.contains(&p.node_id))
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn reload_candidate(&self, input: StaticInput) -> Result<PeerBook> {
        let preview = Self {
            path: self.path.clone(),
            instance: self.instance.clone(),
            stored: self.stored.clone(),
            static_input: input,
            fault: None,
            pairing_clock: self.pairing_clock,
            expiry_seen: self.expiry_seen.clone(),
            exchanges: BTreeMap::new(),
            #[cfg(test)]
            fail_directory_sync: false,
            #[cfg(test)]
            panic_confirmation_and_fault: (false, false),
        };
        preview.compose(&preview.stored).map(|result| result.0)
    }

    #[cfg(test)]
    pub(crate) fn fail_directory_sync_for_test(&mut self) {
        self.fail_directory_sync = true;
    }

    #[cfg(test)]
    pub(crate) fn panic_confirmation_and_fault_for_test(&mut self) {
        self.panic_confirmation_and_fault = (true, true);
    }

    pub fn fault(&self) -> Option<&str> {
        self.fault.as_deref()
    }
    pub fn set_fault(&mut self, error: &anyhow::Error) {
        #[cfg(test)]
        if std::mem::take(&mut self.panic_confirmation_and_fault.1) {
            panic!("injected fault-record worker failure");
        }
        self.fault = Some(format!("{error:#}"));
    }
    #[cfg(test)]
    pub fn revision(&self) -> u64 {
        self.stored.revision
    }
    fn ensure_available(&self) -> Result<()> {
        if let Some(error) = &self.fault {
            Err(unavailable(error.clone()))
        } else {
            Ok(())
        }
    }

    fn compose(&self, stored: &Stored) -> Result<(PeerBook, Vec<EffectivePeer>)> {
        let timestamp = self.expiry_timestamp();
        let (book, views) = self.compose_at(stored, timestamp)?;
        let deadlines: BTreeSet<_> = stored.records.iter()
            .filter_map(|record| record.expires_at.and_then(|value| u64::try_from(value).ok()))
            .collect();
        let generations = deadlines.into_iter().map(|deadline| {
            self.compose_at(stored, deadline)
                .map(|(book, _)| (deadline, book.into_effective_peers()))
        }).collect::<Result<Vec<_>>>()?;
        Ok((book.with_expiry_generations(self.pairing_clock, generations, self.expiry_seen.clone()), views))
    }

    fn compose_at(&self, stored: &Stored, timestamp: u64) -> Result<(PeerBook, Vec<EffectivePeer>)> {
        let mut nodes: BTreeMap<EndpointId, Vec<(&Peer, Source)>> = BTreeMap::new();
        for peer in self.static_input.book.peers() {
            nodes.entry(peer.id).or_default().push((
                peer,
                Source::Static {
                    path: self.static_input.path.clone(),
                },
            ));
        }
        for record in &stored.records {
            if record.expires_at.is_some_and(|deadline| {
                u64::try_from(deadline).is_ok_and(|deadline| deadline <= timestamp)
            }) {
                continue;
            }
            nodes.entry(record.peer.id).or_default().push((
                &record.peer,
                Source::Runtime {
                    owner: record.owner.clone(),
                    expires_at: record.expires_at,
                    expiry_request_id: record.expiry_request_id.clone(),
                },
            ));
        }
        let mut peers = Vec::new();
        let mut views = Vec::new();
        for (id, sources) in nodes {
            let mut merged = sources[0].0.clone();
            merged.allow.clear();
            let mut view = EffectivePeer {
                node_id: id,
                name: None,
                direction: Direction::of(&merged),
                trust_sources: vec![],
                direction_sources: vec![],
                services: BTreeMap::new(),
            };
            for (peer, source) in sources {
                if Direction::of(peer) != view.direction {
                    return Err(invalid(format!(
                        "direction conflict for {id}, source {source:?}"
                    )));
                }
                if let Some(name) = &peer.name {
                    if view.name.as_ref().is_some_and(|other| other != name) {
                        return Err(invalid(format!(
                            "label conflict for {id}, source {source:?}"
                        )));
                    }
                    view.name = Some(name.clone());
                }
                if merged.addr.is_none() {
                    merged.addr = peer.addr.clone();
                }
                view.trust_sources.push(source.clone());
                view.direction_sources.push(source.clone());
                for grant in &peer.allow {
                    validate_grant(grant)?;
                    if view.direction == Direction::DialOnly {
                        return Err(invalid(format!(
                            "dial-only node {id} cannot carry grants (source {source:?})"
                        )));
                    }
                    view.services
                        .entry(grant.clone())
                        .or_default()
                        .push(source.clone());
                }
            }
            merged.name = view.name.clone();
            merged.allow = view.services.keys().cloned().collect();
            peers.push(merged);
            views.push(view);
        }
        Ok((self.static_input.book.with_effective_peers(peers)?, views))
    }

    #[cfg(test)]
    pub(crate) fn advance_expiry_for_test(&self, timestamp: u64) {
        self.expiry_seen.fetch_max(timestamp, std::sync::atomic::Ordering::Relaxed);
    }

    fn expiry_timestamp(&self) -> u64 {
        let timestamp = now().max(self.stored.pairing_last_seen).max(self.stored.expiry_last_seen).max(
            self.pairing_clock.0.saturating_add(self.pairing_clock.1.elapsed().as_secs()),
        );
        self.expiry_seen.fetch_max(timestamp, std::sync::atomic::Ordering::Relaxed).max(timestamp)
    }

    /// Deterministic IDs let a caller find automatic revocation receipts.
    pub(crate) fn expiry_request_id(owner: &str, node_id: EndpointId, deadline: i64, request_id: &str) -> String {
        let mut digest = blake3::Hasher::new();
        for value in [owner, &node_id.to_string(), &deadline.to_string(), request_id] {
            digest.update(&(value.len() as u64).to_le_bytes());
            digest.update(value.as_bytes());
        }
        format!("expiry:{}", digest.finalize().to_hex())
    }

    pub(crate) fn expired_transactions(&self) -> Result<Vec<Transaction>> {
        self.ensure_available()?;
        let timestamp = self.expiry_timestamp();
        Ok(self.stored.records.iter().filter_map(|record| {
            let deadline = record.expires_at?;
            if u64::try_from(deadline).ok()? > timestamp {
                return None;
            }
            Some(Transaction {
                owner: record.owner.clone(),
                request_id: record.expiry_request_id.clone()?,
                expected_revision: self.stored.revision,
                operations: vec![Operation::RemovePeer { node_id: record.peer.id }],
            })
        }).collect())
    }
    pub fn prepare(&self, tx: &Transaction) -> Result<Prepared> {
        self.ensure_available()?;
        validate_token(&tx.owner, "owner")?;
        validate_token(&tx.request_id, "request_id")?;
        if let Some(record) = self.stored.records.iter().find(|record| {
            record.expiry_request_id.as_deref() == Some(tx.request_id.as_str())
        }) {
            let due = record.expires_at.and_then(|deadline| u64::try_from(deadline).ok())
                .is_some_and(|deadline| deadline <= self.expiry_timestamp());
            if !due || record.owner != tx.owner || !matches!(
                tx.operations.as_slice(),
                [Operation::RemovePeer { node_id }] if *node_id == record.peer.id
            ) {
                return Err(invalid("request_id is reserved for automatic expiry"));
            }
        }
        let encoded = serde_json::to_vec(tx)?;
        if encoded.len() > MAX_REQUEST_BYTES
            || tx.operations.is_empty()
            || tx.operations.len() > MAX_OPERATIONS
        {
            return Err(invalid(
                "runtime transaction requires 1..128 operations and at most 1 MiB",
            ));
        }
        let digest = blake3::hash(&encoded).to_hex().to_string();
        if let Some(receipt) = self
            .stored
            .receipts
            .iter()
            .find(|r| r.request_id == tx.request_id)
        {
            if receipt.digest != digest {
                return Err(FabricError::new(
                    ErrorCode::RequestIdConflict,
                    "request_id already names different contents",
                )
                .into());
            }
            return Ok(Prepared {
                stored: self.stored.clone(),
                book: self.compose(&self.stored)?.0,
                replay: true,
                no_op: None,
                expected_revision: self.stored.revision,
            });
        }
        if tx.expected_revision != self.stored.revision {
            return Err(FabricError::new(
                ErrorCode::PolicyRevisionConflict,
                format!(
                    "expected policy revision {}, current {}",
                    tx.expected_revision, self.stored.revision
                ),
            )
            .into());
        }
        let mut next = self.stored.clone();
        next.receipts
            .retain(|r| now().saturating_sub(r.created) < RECEIPT_SECONDS);
        let mut removals = Vec::new();
        for op in &tx.operations {
            let id = op.node_id();
            let index = next
                .records
                .iter()
                .position(|r| r.owner == tx.owner && r.peer.id == id);
            match op {
                Operation::AddPeer {
                    name,
                    direction,
                    addr,
                    ..
                } => {
                    if let Some(name) = name {
                        validate_token(name, "name")?;
                    }
                    if addr.as_ref().is_some_and(|addr| addr.id != id) {
                        return Err(invalid("address hint belongs to another NodeID"));
                    }
                    if let Some(index) = index {
                        let peer = &mut next.records[index].peer;
                        if name.is_some() {
                            peer.name = name.clone();
                        }
                        if let Some(direction) = direction {
                            if matches!(
                                (Direction::of(peer), direction),
                                (Direction::InboundOnly, Direction::DialOnly)
                                    | (Direction::DialOnly, Direction::InboundOnly)
                            ) {
                                return Err(invalid(
                                    "replace an opposite-direction record with remove_peer and add_peer in one batch",
                                ));
                            }
                            direction.apply(peer);
                        }
                        if addr.is_some() {
                            peer.addr = addr.clone();
                        }
                    } else {
                        let mut peer = Peer {
                            id,
                            name: name.clone(),
                            addr: addr.clone(),
                            roaming: false,
                            dial_only: false,
                            inbound_only: false,
                            allow: vec![],
                        };
                        direction.unwrap_or_default().apply(&mut peer);
                        next.records.push(Record {
                            owner: tx.owner.clone(),
                            peer,
                            expires_at: None,
                            expiry_request_id: None,
                        });
                    }
                }
                Operation::SetGrants { grants, .. } => {
                    let Some(index) = index else {
                        return Err(self.ownership_error(&next, id));
                    };
                    for grant in grants {
                        validate_grant(grant)?;
                    }
                    let mut grants = grants.clone();
                    grants.sort();
                    grants.dedup();
                    next.records[index].peer.allow = grants;
                }
                Operation::SetExpiry { expires_at, .. } => {
                    let Some(index) = index else {
                        return Err(self.ownership_error(&next, id));
                    };
                    let record = &mut next.records[index];
                    if let Some(existing) = record.expires_at {
                        if existing != *expires_at {
                            return Err(invalid("expires_at is immutable once set"));
                        }
                    } else {
                        let deadline = u64::try_from(*expires_at)
                            .map_err(|_| invalid("expires_at must be positive Unix seconds"))?;
                        let timestamp = self.expiry_timestamp();
                        if deadline <= timestamp
                            || deadline.saturating_sub(timestamp) > MAX_EXPIRY_SECONDS
                        {
                            return Err(invalid("expires_at must be within the next 600 seconds"));
                        }
                        record.expires_at = Some(*expires_at);
                        record.expiry_request_id = Some(Self::expiry_request_id(&tx.owner, id, *expires_at, &tx.request_id));
                    }
                }
                Operation::RemovePeer { .. } => {
                    if index.is_none()
                        && (self.static_input.book.peers().iter().any(|p| p.id == id)
                            || next.records.iter().any(|r| r.peer.id == id))
                    {
                        return Err(self.ownership_error(&next, id));
                    }
                    removals.push(Removal {
                        node_id: id,
                        owner_record_removed: index.is_some(),
                        effective_trust_removed: false,
                    });
                    if let Some(index) = index {
                        next.records.remove(index);
                    }
                }
            }
        }
        next.records
            .sort_by(|a, b| (&a.owner, a.peer.id).cmp(&(&b.owner, b.peer.id)));
        if next.records.len() > MAX_RECORDS {
            return Err(invalid(
                "runtime policy permits at most 1024 owner/node records",
            ));
        }
        let (book, views) = self.compose(&next)?;
        for removal in &mut removals {
            removal.effective_trust_removed = !views.iter().any(|p| p.node_id == removal.node_id);
        }
        let changed =
            serde_json::to_vec(&next.records)? != serde_json::to_vec(&self.stored.records)?;
        if changed {
            next.revision = next
                .revision
                .checked_add(1)
                .context("policy revision exhausted")?;
        }
        let affected: Vec<_> = tx
            .operations
            .iter()
            .map(Operation::node_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if !changed {
            return Ok(Prepared {
                stored: next,
                book,
                replay: false,
                no_op: Some(MutationReply {
                    request_id: tx.request_id.clone(),
                    committed_revision: self.stored.revision,
                    current_revision: self.stored.revision,
                    affected: views
                        .into_iter()
                        .filter(|p| affected.contains(&p.node_id))
                        .collect(),
                    removals,
                    cancellation: Cancellation::empty(),
                }),
                expected_revision: self.stored.revision,
            });
        }
        // Reserve the last slots for strict owner-scoped withdrawals. A batch
        // containing additions or replacements cannot consume this reserve.
        let withdrawal_only = tx.operations.iter().all(|op| match op {
            Operation::RemovePeer { .. } => true,
            Operation::SetExpiry { .. } => true,
            Operation::SetGrants { node_id, grants } => self
                .stored
                .records
                .iter()
                .find(|r| r.owner == tx.owner && r.peer.id == *node_id)
                .is_some_and(|record| grants.iter().all(|grant| record.peer.allow.contains(grant))),
            Operation::AddPeer { .. } => false,
        });
        let limit = if withdrawal_only {
            MAX_RECEIPTS
        } else {
            MAX_RECEIPTS - WITHDRAWAL_RECEIPTS
        };
        let reserved = next.records.iter().filter(|record| record.expires_at.is_some()).count();
        if next.receipts.len().saturating_add(reserved) >= limit {
            return Err(FabricError::new(
                ErrorCode::RuntimeReceiptCapacity,
                "unexpired runtime receipt capacity is full for this mutation",
            )
            .into());
        }
        next.receipts.push(Receipt {
            request_id: tx.request_id.clone(),
            digest,
            committed_revision: next.revision,
            created: now(),
            affected: affected.clone(),
            removals,
            cancellation: Cancellation::empty(),
        });
        Ok(Prepared {
            stored: next,
            book,
            replay: false,
            no_op: None,
            expected_revision: self.stored.revision,
        })
    }
    fn ownership_error(&self, stored: &Stored, id: EndpointId) -> anyhow::Error {
        let (code, message) = if stored.records.iter().any(|r| r.peer.id == id) {
            (
                ErrorCode::PeerOwnedElsewhere,
                "node has no record for this owner; other-owner access is preserved",
            )
        } else if self.static_input.book.peers().iter().any(|p| p.id == id) {
            (
                ErrorCode::PeerManagedStatically,
                "node is managed statically; static access is preserved",
            )
        } else {
            (
                ErrorCode::UnknownPeer,
                "add this owner/node record before setting grants",
            )
        };
        FabricError::new(code, message).peer(id.to_string()).into()
    }
    pub fn commit(
        &mut self,
        mut next: Prepared,
        request_id: &str,
        cancellation: Cancellation,
    ) -> Result<MutationReply> {
        self.ensure_available()?;
        if self.stored.revision != next.expected_revision {
            return Err(FabricError::new(
                ErrorCode::PolicyRevisionConflict,
                "policy revision changed during preparation",
            )
            .into());
        }
        if let Some(reply) = next.no_op {
            return Ok(reply);
        }
        if !next.replay {
            if next.stored.records.iter().any(|record| record.expires_at.is_some())
                || next.stored.expiry_last_seen != 0
            {
                next.stored.expiry_last_seen = self.expiry_timestamp();
            }
            // Completion records can advance while validation runs off-thread.
            // Preserve that metadata when committing a later policy candidate.
            for receipt in &mut next.stored.receipts {
                if let Some(current) = self
                    .stored
                    .receipts
                    .iter()
                    .find(|current| current.request_id == receipt.request_id)
                {
                    receipt.cancellation = current.cancellation.clone();
                }
            }
            next.stored
                .receipts
                .last_mut()
                .context("missing prepared receipt")?
                .cancellation = cancellation;
            if let Err(error) = self.persist(&next.stored) {
                if crate::error::code_of(&error) == ErrorCode::RuntimeCommitIndeterminate {
                    self.set_fault(&error);
                }
                return Err(error);
            }
            self.stored = next.stored;
        }
        self.receipt(request_id)
    }
    pub fn receipt(&self, request_id: &str) -> Result<MutationReply> {
        self.ensure_available()?;
        let receipt = self
            .stored
            .receipts
            .iter()
            .find(|r| r.request_id == request_id)
            .ok_or_else(|| invalid("unknown or expired runtime request_id"))?;
        let (_, views) = self.compose(&self.stored)?;
        Ok(MutationReply {
            request_id: receipt.request_id.clone(),
            committed_revision: receipt.committed_revision,
            current_revision: self.stored.revision,
            affected: views
                .into_iter()
                .filter(|p| receipt.affected.contains(&p.node_id))
                .collect(),
            removals: receipt.removals.clone(),
            cancellation: receipt.cancellation.clone(),
        })
    }
    pub fn update_cancellation(
        &mut self,
        request_id: &str,
        cancellation: Cancellation,
    ) -> Result<()> {
        self.ensure_available()?;
        let mut next = self.stored.clone();
        let receipt = next
            .receipts
            .iter_mut()
            .find(|r| r.request_id == request_id)
            .context("missing cancellation receipt")?;
        receipt.cancellation = cancellation;
        self.persist(&next)?;
        self.stored = next;
        Ok(())
    }
    pub fn reload_static(&mut self, input: StaticInput) -> Result<PeerBook> {
        self.ensure_available()?;
        let old = std::mem::replace(&mut self.static_input, input);
        let mut next = self.stored.clone();
        let result = (|| -> Result<PeerBook> {
            let (book, _) = self.compose(&next)?;
            if next.static_digest != self.static_input.digest {
                next.revision = next
                    .revision
                    .checked_add(1)
                    .context("policy revision exhausted")?;
                next.static_digest = self.static_input.digest.clone();
                self.persist(&next)?;
            }
            self.stored = next;
            Ok(book)
        })();
        if result.is_err() {
            self.static_input = old;
        }
        result
    }
    pub fn snapshot(&self, cursor: Option<&Cursor>) -> Result<Snapshot> {
        if let Some(cursor) = cursor {
            if cursor.instance != self.instance || cursor.revision != self.stored.revision {
                return Err(FabricError::new(
                    ErrorCode::PolicyRevisionConflict,
                    "list cursor belongs to another policy generation",
                )
                .into());
            }
        }
        let views = if self.fault.is_none() {
            self.compose(&self.stored)?.1
        } else {
            vec![]
        };
        let mut peers: Vec<_> = views
            .into_iter()
            .filter(|peer| cursor.is_none_or(|cursor| peer.node_id > cursor.after_node))
            .take(PAGE_SIZE + 1)
            .collect();
        let next_cursor = if peers.len() > PAGE_SIZE {
            peers.pop();
            peers.last().map(|p| Cursor {
                instance: self.instance.clone(),
                revision: self.stored.revision,
                after_node: p.node_id,
            })
        } else {
            None
        };
        Ok(Snapshot {
            instance: self.instance.clone(),
            policy_revision: self.stored.revision,
            static_digest: self.stored.static_digest.clone(),
            peers,
            next_cursor,
            fault: self.fault.clone(),
        })
    }
    fn persist(&self, next: &Stored) -> Result<()> {
        let parent = self.path.parent().context("runtime policy has no parent")?;
        let temp = parent.join(format!(
            ".runtime-peers-{:032x}.tmp",
            rand::random::<u128>()
        ));
        let bytes = toml::to_string(next)?;
        let mut renamed = false;
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temp)?;
            file.write_all(bytes.as_bytes())?;
            file.sync_all()?;
            fs::rename(&temp, &self.path)?;
            renamed = true;
            #[cfg(test)]
            if self.fail_directory_sync {
                anyhow::bail!("injected directory fsync failure after rename");
            }
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if !renamed {
            let _ = fs::remove_file(&temp);
        }
        result.map_err(|error| {
            FabricError::new(
                if renamed {
                    ErrorCode::RuntimeCommitIndeterminate
                } else {
                    ErrorCode::RuntimeStateUnavailable
                },
                format!("commit {}: {error:#}", self.path.display()),
            )
            .into()
        })
    }
}
fn validate_grant(grant: &str) -> Result<()> {
    if grant.is_empty()
        || grant.len() > 255
        || grant.chars().any(|c| {
            c.is_control() || c.is_whitespace() || matches!(c, '*' | '?' | '[' | ']' | ':')
        })
    {
        return Err(invalid(
            "grants must be exact service names, without wildcards, whitespace or port syntax",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    fn id(seed: u8) -> EndpointId {
        SecretKey::from_bytes(&[seed; 32]).public()
    }
    fn tx(owner: &str, request: &str, rev: u64, operations: Vec<Operation>) -> Transaction {
        Transaction {
            owner: owner.into(),
            request_id: request.into(),
            expected_revision: rev,
            operations,
        }
    }
    fn add(seed: u8, name: &str) -> Operation {
        Operation::AddPeer {
            node_id: id(seed),
            name: Some(name.into()),
            direction: None,
            addr: None,
        }
    }
    fn grants(seed: u8, services: &[&str]) -> Operation {
        Operation::SetGrants {
            node_id: id(seed),
            grants: services.iter().map(|s| s.to_string()).collect(),
        }
    }
    fn commit(store: &mut RuntimePeers, tx: &Transaction) -> Result<MutationReply> {
        let next = store.prepare(tx)?;
        store.commit(next, &tx.request_id, Cancellation::empty())
    }
    fn open(dir: &tempfile::TempDir) -> RuntimePeers {
        let (store, _) = RuntimePeers::open(&FabricHome::new(dir.path()));
        assert!(store.fault().is_none(), "{:?}", store.fault());
        store
    }

    #[test]
    fn exact_grants_rename_batch_and_restart_are_durable() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let result = commit(
            &mut store,
            &tx(
                "controller",
                "add",
                0,
                vec![add(1, "server"), grants(1, &["work/0"])],
            ),
        )?;
        assert_eq!(result.committed_revision, 1);
        let result = commit(
            &mut store,
            &tx("controller", "rename", 1, vec![add(1, "renamed")]),
        )?;
        assert_eq!(
            result.affected[0]
                .services
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            ["work/0"]
        );
        let mut store = open(&dir);
        let book = store.compose(&store.stored)?.0;
        assert!(book.may(&id(1), "work/0").is_ok());
        assert!(book.may(&id(1), "echo").is_err());
        assert!(book.may(&id(1), "work/1").is_err());
        let result = commit(
            &mut store,
            &tx(
                "controller",
                "replace",
                2,
                vec![
                    Operation::RemovePeer { node_id: id(1) },
                    add(2, "renamed"),
                    grants(2, &["work/0"]),
                ],
            ),
        )?;
        assert_eq!(result.committed_revision, 3);
        let book = open(&dir).compose(&store.stored)?.0;
        assert!(book.may(&id(1), "work/0").is_err());
        assert!(book.may(&id(2), "work/0").is_ok());
        Ok(())
    }
    #[test]
    fn retry_returns_receipt_and_current_access_without_regranting() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let request = tx(
            "a",
            "original",
            0,
            vec![add(1, "node"), grants(1, &["echo"])],
        );
        commit(&mut store, &request)?;
        commit(&mut store, &tx("a", "revoke", 1, vec![grants(1, &[])]))?;
        let result = commit(&mut store, &request)?;
        assert_eq!((result.committed_revision, result.current_revision), (1, 2));
        assert!(result.affected[0].services.is_empty());
        let mut changed = request.clone();
        changed.owner = "b".into();
        assert_eq!(
            crate::error::code_of(&store.prepare(&changed).err().unwrap()),
            ErrorCode::RequestIdConflict
        );
        let store = open(&dir);
        assert!(store.receipt("original")?.affected[0].services.is_empty());
        Ok(())
    }
    #[test]
    fn stale_revision_or_invalid_final_candidate_never_commits() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        commit(&mut store, &tx("a", "first", 0, vec![add(1, "node")]))?;
        let before = fs::read(&store.path)?;
        let error = store
            .prepare(&tx("a", "stale", 0, vec![add(2, "other")]))
            .err()
            .unwrap();
        assert_eq!(
            crate::error::code_of(&error),
            ErrorCode::PolicyRevisionConflict
        );
        assert!(
            store
                .prepare(&tx("a", "collision", 1, vec![add(2, "node")]))
                .is_err()
        );
        assert!(
            store
                .prepare(&tx("a", "wildcard", 1, vec![grants(1, &["*"])]))
                .is_err()
        );
        assert_eq!(before, fs::read(&store.path)?);
        assert_eq!(store.revision(), 1);
        Ok(())
    }
    #[test]
    fn static_symlink_and_other_owner_grants_remain_visible_and_untouched() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let target = dir.path().join("managed-policy");
        let raw = format!(
            "format = 2\n[[peers]]\nid = '{}'\nname = 'node'\nallow = ['echo']\n",
            id(1)
        );
        fs::write(&target, &raw)?;
        std::os::unix::fs::symlink(&target, home.peers_path())?;
        let mut permissions = fs::metadata(&target)?.permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&target, permissions)?;
        let mut store = open(&dir);
        assert_eq!(
            crate::error::code_of(
                &store
                    .prepare(&tx(
                        "a",
                        "static-remove",
                        0,
                        vec![Operation::RemovePeer { node_id: id(1) }]
                    ))
                    .err()
                    .unwrap()
            ),
            ErrorCode::PeerManagedStatically
        );
        commit(
            &mut store,
            &tx("a", "a", 0, vec![add(1, "node"), grants(1, &["exec"])]),
        )?;
        commit(
            &mut store,
            &tx("b", "b", 1, vec![add(1, "node"), grants(1, &["exec"])]),
        )?;
        let result = commit(
            &mut store,
            &tx(
                "a",
                "remove-a",
                2,
                vec![Operation::RemovePeer { node_id: id(1) }],
            ),
        )?;
        assert!(!result.removals[0].effective_trust_removed);
        assert_eq!(
            result.affected[0].services["echo"],
            vec![Source::Static {
                path: home.peers_path()
            }]
        );
        assert_eq!(
            result.affected[0].services["exec"],
            vec![Source::Runtime { owner: "b".into(), expires_at: None, expiry_request_id: None }]
        );
        assert_eq!(
            crate::error::code_of(
                &store
                    .prepare(&tx(
                        "a",
                        "remove-again",
                        3,
                        vec![Operation::RemovePeer { node_id: id(1) }]
                    ))
                    .err()
                    .unwrap()
            ),
            ErrorCode::PeerOwnedElsewhere
        );
        assert_eq!(fs::read_to_string(target)?, raw);
        assert!(home.peers_path().is_symlink());
        assert!(
            store
                .prepare(&tx("b", "rename-conflict", 3, vec![add(1, "wrong")]))
                .is_err()
        );
        Ok(())
    }
    #[test]
    fn legacy_or_corrupt_input_closes_policy_and_preserves_files() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        fs::write(home.peers_path(), "allow_exec = true\n")?;
        let (store, book) = RuntimePeers::open(&home);
        assert!(store.fault().is_some());
        assert!(book.peers().is_empty());
        assert_eq!(
            fs::read_to_string(home.peers_path())?,
            "allow_exec = true\n"
        );
        fs::remove_file(home.peers_path())?;
        let path = home.root().join("runtime-peers.toml");
        fs::write(&path, "broken = [")?;
        let (store, book) = RuntimePeers::open(&home);
        assert!(store.fault().is_some());
        assert!(book.peers().is_empty());
        assert_eq!(fs::read_to_string(path)?, "broken = [");
        assert!(store.snapshot(None)?.fault.is_some());
        Ok(())
    }
    #[test]
    fn broken_runtime_symlink_is_a_fault_and_is_never_replaced() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let path = dir.path().join("runtime-peers.toml");
        let missing = dir.path().join("missing-state.toml");
        std::os::unix::fs::symlink(&missing, &path)?;
        let (store, book) = RuntimePeers::open(&home);
        assert!(store.fault().is_some());
        assert!(book.peers().is_empty());
        assert_eq!(fs::read_link(&path)?, missing);
        assert!(!missing.exists());
        Ok(())
    }

    #[test]
    fn failed_pre_rename_write_keeps_policy_and_receipts_unchanged() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let request = tx("a", "failed", 0, vec![add(1, "node")]);
        let next = store.prepare(&request)?;
        let blocker = dir.path().join("directory");
        fs::create_dir(&blocker)?;
        store.path = blocker;
        assert_eq!(
            crate::error::code_of(
                &store
                    .commit(next, "failed", Cancellation::empty())
                    .unwrap_err()
            ),
            ErrorCode::RuntimeStateUnavailable
        );
        assert_eq!(store.revision(), 0);
        assert!(store.stored.records.is_empty());
        assert!(store.receipt("failed").is_err());
        assert!(store.fault().is_none());
        Ok(())
    }
    #[test]
    fn direction_conflicts_and_dial_only_grants_are_refused() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let inbound = Operation::AddPeer {
            node_id: id(1),
            name: Some("phone".into()),
            direction: Some(Direction::InboundOnly),
            addr: None,
        };
        commit(
            &mut store,
            &tx("a", "phone", 0, vec![inbound, grants(1, &["echo"])]),
        )?;
        assert_eq!(store.compose(&store.stored)?.0.dialable_peers().count(), 0);
        assert!(
            store
                .prepare(&tx("b", "direction-conflict", 1, vec![add(1, "phone")]))
                .is_err()
        );
        let dial = Operation::AddPeer {
            node_id: id(2),
            name: Some("runner".into()),
            direction: Some(Direction::DialOnly),
            addr: None,
        };
        assert!(
            store
                .prepare(&tx("a", "dial-grant", 1, vec![dial, grants(2, &["exec"])]))
                .is_err()
        );
        let opposite = Operation::AddPeer {
            node_id: id(1),
            name: Some("phone".into()),
            direction: Some(Direction::DialOnly),
            addr: None,
        };
        assert!(
            store
                .prepare(&tx("a", "flip", 1, vec![opposite.clone(), grants(1, &[])]))
                .is_err()
        );
        let replaced = commit(
            &mut store,
            &tx(
                "a",
                "replace-direction",
                1,
                vec![Operation::RemovePeer { node_id: id(1) }, opposite],
            ),
        )?;
        assert_eq!(replaced.committed_revision, 2);
        assert_eq!(replaced.affected[0].direction, Direction::DialOnly);
        assert!(replaced.affected[0].services.is_empty());
        Ok(())
    }
    #[test]
    fn restart_faults_unconfirmed_cancellation_and_preserves_observed_completion() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let request = tx("a", "pending", 0, vec![add(1, "node")]);
        let next = store.prepare(&request)?;
        store.commit(
            next,
            "pending",
            Cancellation {
                admissions: vec![7],
                remaining_admissions: vec![7],
                ..Cancellation::default()
            },
        )?;
        let mut store = open(&dir);
        let receipt = store.receipt("pending")?;
        assert_eq!(receipt.cancellation.state, CancellationState::TimedOut);
        assert_eq!(receipt.cancellation.remaining_admissions, [7]);
        let evidence = receipt.cancellation.restart_evidence.unwrap();
        assert!(evidence.daemon_instance_changed);
        assert!(!evidence.child_exit_observed);
        // Live reconciliation cannot establish exits from an older daemon.
        store.reconcile(&FabricHome::new(dir.path()))?;
        assert_eq!(
            store.receipt("pending")?.cancellation.remaining_admissions,
            [7]
        );
        // Actual completion, once observed, survives another restart.
        store.update_cancellation("pending", Cancellation::empty())?;
        let observed = open(&dir).receipt("pending")?.cancellation;
        assert_eq!(observed.state, CancellationState::Complete);
        assert!(observed.restart_evidence.is_none());
        // A missed deadline whose work later ended keeps that outcome too.
        store.update_cancellation(
            "pending",
            Cancellation {
                state: CancellationState::TimedOut,
                admissions: vec![8],
                ..Cancellation::default()
            },
        )?;
        let late = open(&dir).receipt("pending")?.cancellation;
        assert_eq!(late.state, CancellationState::TimedOut);
        assert!(late.remaining_admissions.is_empty());
        assert!(late.restart_evidence.is_none());
        Ok(())
    }
    #[test]
    fn runtime_batch_request_round_trips_through_json() -> Result<()> {
        let request = Request::Batch {
            transaction: tx("a", "batch", 0, vec![add(1, "node")]),
        };
        let encoded = serde_json::to_vec(&request)?;
        let decoded: Request = serde_json::from_slice(&encoded)?;
        assert_eq!(decoded.into_transaction().unwrap().operations.len(), 1);
        Ok(())
    }

    #[test]
    fn renamed_but_unconfirmed_commit_quarantines_until_disk_reconciliation() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let request = tx(
            "a",
            "uncertain",
            0,
            vec![add(1, "node"), grants(1, &["echo"])],
        );
        let prepared = store.prepare(&request)?;
        store.fail_directory_sync = true;
        let error = store
            .commit(prepared, "uncertain", Cancellation::empty())
            .unwrap_err();
        assert_eq!(
            crate::error::code_of(&error),
            ErrorCode::RuntimeCommitIndeterminate
        );
        assert!(store.fault().is_some());
        assert!(store.prepare(&request).is_err());
        // The durable ID resolves the disk outcome; no second grant mutation.
        store.fail_directory_sync = false;
        let book = store.reconcile(&FabricHome::new(dir.path()))?;
        assert!(book.may(&id(1), "echo").is_ok());
        assert_eq!(store.receipt("uncertain")?.committed_revision, 1);
        Ok(())
    }
    #[test]
    fn completion_updates_are_not_lost_when_another_candidate_is_prepared() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let first = tx("a", "first", 0, vec![add(1, "node")]);
        let prepared = store.prepare(&first)?;
        store.commit(
            prepared,
            "first",
            Cancellation {
                admissions: vec![1],
                remaining_admissions: vec![1],
                ..Cancellation::default()
            },
        )?;
        let second = tx("a", "second", 1, vec![grants(1, &["echo"])]);
        let prepared = store.prepare(&second)?;
        store.update_cancellation(
            "first",
            Cancellation {
                admissions: vec![1],
                ..Cancellation::empty()
            },
        )?;
        store.commit(prepared, "second", Cancellation::empty())?;
        assert_eq!(
            store.receipt("first")?.cancellation.state,
            CancellationState::Complete
        );
        assert_eq!(
            open(&dir).receipt("first")?.cancellation.state,
            CancellationState::Complete
        );
        Ok(())
    }
    #[test]
    fn broken_static_symlink_is_a_closed_policy_not_an_empty_input() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        std::os::unix::fs::symlink(dir.path().join("missing"), home.peers_path())?;
        let (store, book) = RuntimePeers::open(&home);
        assert!(store.fault().is_some());
        assert!(book.peers().is_empty());
        assert!(home.peers_path().is_symlink());
        Ok(())
    }

    #[test]
    fn list_pages_are_bound_to_revision_and_instance() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        for batch in 0..3 {
            let operations = (batch * 100..(batch + 1) * 100)
                .map(|index: u16| {
                    let mut bytes = [0; 32];
                    bytes[..2].copy_from_slice(&index.to_le_bytes());
                    Operation::AddPeer {
                        node_id: SecretKey::from_bytes(&bytes).public(),
                        name: Some(format!("node-{index}")),
                        direction: None,
                        addr: None,
                    }
                })
                .collect();
            commit(
                &mut store,
                &tx("a", &format!("batch-{batch}"), batch as u64, operations),
            )?;
        }
        let first = store.snapshot(None)?;
        assert_eq!(first.peers.len(), PAGE_SIZE);
        let cursor = first.next_cursor.unwrap();
        let second = store.snapshot(Some(&cursor))?;
        assert_eq!(second.peers.len(), 300 - PAGE_SIZE);
        assert!(second.next_cursor.is_none());
        assert!(second.peers.iter().all(|peer| {
            first
                .peers
                .iter()
                .all(|previous| previous.node_id != peer.node_id)
        }));
        assert!(open(&dir).snapshot(Some(&cursor)).is_err());
        commit(&mut store, &tx("a", "advance", 3, vec![add(42, "extra")]))?;
        assert!(store.snapshot(Some(&cursor)).is_err());
        Ok(())
    }
    #[test]
    fn receipt_capacity_refuses_before_commit_and_prunes_only_expired_slots() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        store.stored.receipts = (0..MAX_RECEIPTS - WITHDRAWAL_RECEIPTS)
            .map(|i| Receipt {
                request_id: format!("prior-{i}"),
                digest: "prior".into(),
                committed_revision: 0,
                created: now(),
                affected: vec![],
                removals: vec![],
                cancellation: Cancellation::empty(),
            })
            .collect();
        let request = tx("a", "next", 0, vec![add(1, "node")]);
        assert_eq!(
            crate::error::code_of(&store.prepare(&request).err().unwrap()),
            ErrorCode::RuntimeReceiptCapacity
        );
        store.stored.receipts[0].created = now() - RECEIPT_SECONDS - 1;
        assert!(store.prepare(&request).is_ok());
        assert_eq!(
            store.stored.receipts.len(),
            MAX_RECEIPTS - WITHDRAWAL_RECEIPTS
        );
        Ok(())
    }
    #[test]
    fn receipt_reserve_keeps_withdrawals_available_and_no_ops_use_no_slot() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        commit(
            &mut store,
            &tx(
                "a",
                "install",
                0,
                vec![add(1, "node"), grants(1, &["echo", "exec"])],
            ),
        )?;
        let template = store.stored.receipts[0].clone();
        store.stored.receipts = (0..MAX_RECEIPTS - WITHDRAWAL_RECEIPTS)
            .map(|index| Receipt {
                request_id: format!("retained-{index}"),
                ..template.clone()
            })
            .collect();
        store.persist(&store.stored)?;
        let mut store = open(&dir);
        let error = store
            .prepare(&tx("a", "add", 1, vec![add(2, "other")]))
            .err()
            .unwrap();
        assert_eq!(
            crate::error::code_of(&error),
            ErrorCode::RuntimeReceiptCapacity
        );
        let withdrawn = commit(
            &mut store,
            &tx("a", "narrow", 1, vec![grants(1, &["echo"])]),
        )?;
        assert_eq!(withdrawn.committed_revision, 2);
        assert!(store.compose(&store.stored)?.0.may(&id(1), "exec").is_err());
        commit(
            &mut store,
            &tx(
                "a",
                "remove",
                2,
                vec![Operation::RemovePeer { node_id: id(1) }],
            ),
        )?;
        assert!(store.compose(&store.stored)?.0.peers().is_empty());
        assert_eq!(
            store.stored.receipts.len(),
            MAX_RECEIPTS - WITHDRAWAL_RECEIPTS + 2
        );
        // Even a completely full pool cannot be enlarged with no-op receipts.
        while store.stored.receipts.len() < MAX_RECEIPTS {
            let index = store.stored.receipts.len();
            store.stored.receipts.push(Receipt {
                request_id: format!("retained-{index}"),
                ..template.clone()
            });
        }
        let before = fs::read(&store.path)?;
        for index in 0..128 {
            let reply = commit(
                &mut store,
                &tx(
                    "a",
                    &format!("no-op-{index}"),
                    3,
                    vec![Operation::RemovePeer { node_id: id(1) }],
                ),
            )?;
            assert_eq!(reply.committed_revision, 3);
            assert!(store.receipt(&format!("no-op-{index}")).is_err());
        }
        assert_eq!(store.stored.receipts.len(), MAX_RECEIPTS);
        assert_eq!(fs::read(&store.path)?, before);
        Ok(())
    }

    #[test]
    fn record_capacity_is_bounded_and_prepared_revision_is_rechecked() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let first = store.prepare(&tx("a", "first", 0, vec![add(1, "one")]))?;
        let stale = store.prepare(&tx("a", "stale", 0, vec![add(2, "two")]))?;
        store.commit(first, "first", Cancellation::empty())?;
        let error = store
            .commit(stale, "stale", Cancellation::empty())
            .unwrap_err();
        assert_eq!(
            crate::error::code_of(&error),
            ErrorCode::PolicyRevisionConflict
        );
        let template = store.stored.records[0].clone();
        store.stored.records = (0..MAX_RECORDS)
            .map(|index| Record {
                owner: format!("owner-{index}"),
                ..template.clone()
            })
            .collect();
        assert!(
            store
                .prepare(&tx("a", "overflow", 1, vec![add(2, "two")]))
                .is_err()
        );
        let removed = commit(
            &mut store,
            &tx(
                "owner-0",
                "remove",
                1,
                vec![Operation::RemovePeer { node_id: id(1) }],
            ),
        )?;
        assert_eq!(removed.committed_revision, 2);
        assert_eq!(store.stored.records.len(), MAX_RECORDS - 1);
        Ok(())
    }
    #[test]
    fn expiry_is_bounded_immutable_and_overlap_batch_replays_without_extension() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        commit(&mut store, &tx("owner", "old", 0, vec![add(1, "old"), grants(1, &["exec", "echo"])]))?;
        let deadline = i64::try_from(store.expiry_timestamp() + 600)?;
        let overlap = tx("owner", "overlap", 1, vec![
            add(2, "next"), grants(2, &["exec", "echo"]),
            Operation::SetExpiry { node_id: id(1), expires_at: deadline },
        ]);
        let installed = commit(&mut store, &overlap)?;
        assert_eq!(installed.committed_revision, 2);
        assert_eq!(store.stored.records.iter().find(|record| record.peer.id == id(1)).unwrap().expires_at, Some(deadline));
        assert_eq!(commit(&mut store, &overlap)?.committed_revision, 2);
        for value in [deadline - 1, deadline + 1, 0, -1, i64::MAX] {
            assert!(store.prepare(&tx("owner", &format!("change-{value}"), 2, vec![
                Operation::SetExpiry { node_id: id(1), expires_at: value },
            ])).is_err());
        }
        assert!(store.prepare(&tx("other", "wrong-owner", 2, vec![
            Operation::SetExpiry { node_id: id(1), expires_at: deadline },
        ])).is_err());
        assert!(store.prepare(&tx("owner", "too-far", 2, vec![
            Operation::SetExpiry { node_id: id(2), expires_at: deadline + 10 },
        ])).is_err());
        let unchanged = fs::read(&store.path)?;
        assert!(store.prepare(&tx("owner", "bad-atomic", 2, vec![
            add(3, "third"),
            Operation::SetExpiry { node_id: id(1), expires_at: deadline + 1 },
        ])).is_err());
        assert_eq!(fs::read(&store.path)?, unchanged);
        store.advance_expiry_for_test(u64::try_from(deadline)?);
        let expired = store.expired_transactions()?.remove(0);
        let removed = commit(&mut store, &expired)?;
        assert!(removed.removals[0].effective_trust_removed);
        assert_eq!(removed.committed_revision, 3);
        let replay = commit(&mut store, &overlap)?;
        assert_eq!(replay.committed_revision, 2);
        assert_eq!(replay.current_revision, 3);
        assert_eq!(replay.affected.len(), 1);
        assert_eq!(replay.affected[0].node_id, id(2));
        assert_eq!(store.stored.records.len(), 1);
        Ok(())
    }

    #[test]
    fn timed_policy_preserves_residual_sources_without_a_cleanup_commit() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        store.static_input.book.add_with_allow(
            id(1), Some("one".into()), None, Some(vec!["static/0".into()]),
        );
        commit(&mut store, &tx("expiring", "install", 0, vec![add(1, "one"), grants(1, &["exec"])]))?;
        commit(&mut store, &tx("residual", "residual", 1, vec![add(1, "one"), grants(1, &["echo"])]))?;
        let deadline = i64::try_from(store.expiry_timestamp() + 600)?;
        commit(&mut store, &tx("expiring", "deadline", 2, vec![
            Operation::SetExpiry { node_id: id(1), expires_at: deadline },
        ]))?;
        let book = store.compose(&store.stored)?.0;
        assert!(book.may(&id(1), "exec").is_ok());
        assert!(book.may(&id(1), "echo").is_ok());
        store.advance_expiry_for_test(u64::try_from(deadline)?);
        assert!(book.may(&id(1), "exec").is_err());
        assert!(book.may(&id(1), "static/0").is_ok());
        assert!(book.may(&id(1), "echo").is_ok());
        assert!(book.accepts_inbound(&id(1)));
        assert_eq!(store.revision(), 3);
        assert_eq!(store.stored.records.len(), 2);
        let transaction = store.expired_transactions()?.remove(0);
        let removed = commit(&mut store, &transaction)?;
        assert!(!removed.removals[0].effective_trust_removed);
        assert_eq!(removed.affected[0].services.len(), 2);
        Ok(())
    }

    #[test]
    fn restart_expires_elapsed_records_and_clock_rollback_before_admission() -> Result<()> {
        for rollback in [false, true] {
            let dir = tempfile::tempdir()?;
            let mut store = open(&dir);
            let deadline = i64::try_from(store.expiry_timestamp() + 600)?;
            commit(&mut store, &tx("owner", "install", 0, vec![
                add(1, "one"), grants(1, &["echo"]),
                Operation::SetExpiry { node_id: id(1), expires_at: deadline },
            ]))?;
            if rollback {
                store.stored.expiry_last_seen = u64::try_from(deadline)?;
            } else {
                store.stored.records[0].expires_at = Some(i64::try_from(now())? - 1);
            }
            let expiry_request_id = store.stored.records[0].expiry_request_id.clone().unwrap();
            store.persist(&store.stored)?;
            let (restarted, book) = RuntimePeers::open(&FabricHome::new(dir.path()));
            assert!(restarted.fault().is_none(), "{:?}", restarted.fault());
            assert!(book.peers().is_empty());
            assert_eq!(restarted.revision(), 2);
            let receipt = restarted.receipt(&expiry_request_id)?;
            assert_eq!(receipt.cancellation.state, CancellationState::Complete);
            assert!(receipt.removals[0].effective_trust_removed);
        }
        Ok(())
    }

    #[test]
    fn expiry_wire_rejects_overflow_and_reserves_its_receipt_identifier() -> Result<()> {
        let encoded = format!(
            r#"{{"operation":"set_expiry","owner":"owner","request_id":"set","expected_revision":0,"node_id":"{}","expires_at":9223372036854775808}}"#,
            id(1),
        );
        assert!(serde_json::from_str::<Request>(&encoded).is_err());
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let deadline = i64::try_from(store.expiry_timestamp() + 600)?;
        for value in [-1, 0, i64::try_from(store.expiry_timestamp())?, i64::MAX] {
            assert!(store.prepare(&tx("owner", &format!("invalid-{value}"), 0, vec![
                add(1, "one"), Operation::SetExpiry { node_id: id(1), expires_at: value },
            ])).is_err());
        }
        let receipt = commit(&mut store, &tx("owner", "install", 0, vec![
            add(1, "one"), Operation::SetExpiry { node_id: id(1), expires_at: deadline },
        ]))?;
        let reserved = store.stored.records[0].expiry_request_id.clone().unwrap();
        assert!(matches!(&receipt.affected[0].trust_sources[0],
            Source::Runtime { expires_at: Some(value), expiry_request_id: Some(request_id), .. }
                if *value == deadline && request_id == &reserved));
        assert!(store.prepare(&tx("owner", &reserved, 1, vec![grants(1, &[])])).is_err());
        let raw = fs::read_to_string(&store.path)?;
        assert!(raw.contains("expires_at"));
        let restarted = open(&dir);
        assert_eq!(restarted.stored.records[0].expires_at, Some(deadline));
        assert_eq!(restarted.stored.records[0].expiry_request_id.as_deref(), Some(reserved.as_str()));
        Ok(())
    }

    #[test]
    fn expiry_receipt_capacity_is_reserved_when_the_deadline_is_installed() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut store = open(&dir);
        let deadline = i64::try_from(store.expiry_timestamp() + 600)?;
        commit(&mut store, &tx("owner", "install", 0, vec![
            add(1, "one"),
            Operation::SetExpiry { node_id: id(1), expires_at: deadline },
        ]))?;
        let template = store.stored.receipts[0].clone();
        store.stored.receipts = (0..MAX_RECEIPTS - 1).map(|index| Receipt {
            request_id: format!("receipt-{index}"), ..template.clone()
        }).collect();
        assert!(store.prepare(&tx("owner", "ordinary", 1, vec![add(2, "two")])).is_err());
        store.advance_expiry_for_test(u64::try_from(deadline)?);
        let expired = store.expired_transactions()?.remove(0);
        commit(&mut store, &expired)?;
        assert_eq!(store.stored.receipts.len(), MAX_RECEIPTS);
        assert!(store.stored.records.is_empty());
        Ok(())
    }
}
