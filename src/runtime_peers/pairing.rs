//! Restricted pairing state: neither issuing nor presenting a permit grants access.
pub use super::exchange::{ExchangeReply, Message, MessageDirection, Payload};
use super::*;

pub const PAIR_ALPN: &[u8] = b"fabric/pair/1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum NetworkRequest {
    Present {
        attempt_id: String,
        secret: Secret,
    },
    Status {
        attempt_id: String,
    },
    Send {
        attempt_id: String,
        sequence: u64,
        digest: String,
        payload: Payload,
    },
    Receive {
        attempt_id: String,
        ack: Option<u64>,
    },
}

const MAX_PERMITS: usize = 64;
const MAX_ATTEMPTS: usize = 1024;
const MAX_LIFETIME: u64 = 600;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub grants: Vec<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub direction: Direction,
}
impl Policy {
    fn validate(&self) -> Result<()> {
        if self.grants.len() > 128 {
            return Err(invalid("a permit accepts at most 128 exact grants"));
        }
        for grant in &self.grants {
            validate_grant(grant)?;
        }
        if let Some(name) = &self.name {
            validate_token(name, "name")?;
        }
        if self.direction == Direction::DialOnly && !self.grants.is_empty() {
            return Err(invalid("dial-only permits cannot carry grants"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Issue {
        owner: String,
        creation_id: String,
        deadline: u64,
        policy: Policy,
    },
    Query {
        owner: String,
        creation_id: String,
    },
    Cancel {
        owner: String,
        creation_id: String,
    },
    PairSend {
        owner: String,
        attempt_id: String,
        sequence: u64,
        digest: String,
        payload: Payload,
    },
    PairReceive {
        owner: String,
        attempt_id: String,
        ack: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Issued,
    Pending,
    Expired,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub attempt_id: String,
    pub creation_id: String,
    pub state: State,
    pub attempt_revision: u64,
    pub deadline: u64,
    pub policy_digest: String,
    pub terminal_at: Option<u64>,
    #[serde(default)]
    pub bound_node_id: Option<EndpointId>,
    #[serde(default)]
    pub terminal_reason: Option<String>,
}

/// Plaintext is returned only at creation; Debug always redacts it.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);
impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}
impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    pub status: Option<Status>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<Secret>,
    pub error: Option<PolicyError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange: Option<ExchangeReply>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Attempt {
    pub(super) owner: String,
    policy: Policy,
    pub(super) status: Status,
    salt: Option<String>,
    secret_hash: Option<String>,
}
impl Attempt {
    fn expire(&mut self, timestamp: u64) {
        if matches!(self.status.state, State::Issued | State::Pending)
            && timestamp >= self.status.deadline
        {
            self.terminate(State::Expired, self.status.deadline);
        }
    }
    pub(super) fn terminate(&mut self, state: State, timestamp: u64) {
        self.status.state = state;
        self.status.attempt_revision += 1;
        self.status.terminal_at = Some(timestamp);
        self.status.terminal_reason = Some(
            match self.status.state {
                State::Expired => "expired",
                _ => "cancelled",
            }
            .into(),
        );
        self.salt = None;
        self.secret_hash = None;
    }
}

pub(super) fn validate(attempts: &[Attempt]) -> Result<()> {
    if attempts.len() > MAX_ATTEMPTS {
        return Err(unavailable("too many pairing attempts"));
    }
    let mut creations = BTreeSet::new();
    let mut ids = BTreeSet::new();
    let mut issued = 0;
    for attempt in attempts {
        validate_token(&attempt.owner, "owner")?;
        validate_token(&attempt.status.creation_id, "creation_id")?;
        attempt.policy.validate()?;
        let status = &attempt.status;
        validate_token(&status.attempt_id, "attempt_id")?;
        let active = matches!(status.state, State::Issued | State::Pending);
        let bound = status.bound_node_id.is_some();
        let revision = match status.state {
            State::Issued if !bound => 1,
            State::Pending if bound => 2,
            State::Expired | State::Rejected => {
                if bound {
                    3
                } else {
                    2
                }
            }
            _ => return Err(unavailable("invalid pairing attempt binding")),
        };
        issued += usize::from(active);
        if !creations.insert((&attempt.owner, &status.creation_id))
            || !ids.insert(&status.attempt_id)
            || status.policy_digest != digest(&attempt.policy)?
            || status.attempt_revision != revision
            || active != status.terminal_at.is_none()
            || active != attempt.salt.is_some()
            || active != attempt.secret_hash.is_some()
            || attempt.salt.as_ref().is_some_and(|salt| {
                blake3::Hash::from_hex(salt).is_err()
            })
            || attempt.secret_hash.as_ref().is_some_and(|hash| {
                blake3::Hash::from_hex(hash).is_err()
            })
            || (active && status.terminal_reason.is_some())
            // Earlier permit-only receipts had no reason or bound identity.
            || (!active && bound && status.terminal_reason.is_none())
            || status.terminal_reason.as_ref().is_some_and(|reason| {
                !matches!(
                    (&status.state, reason.as_str()),
                    (State::Expired, "expired")
                        | (State::Rejected, "cancelled")
                        | (State::Rejected, "exchange_lost")
                        | (State::Rejected, "exchange_limit")
                ) || (matches!(reason.as_str(), "exchange_lost" | "exchange_limit") && !bound)
            })
        {
            return Err(unavailable("invalid pairing attempt history"));
        }
    }
    if issued > MAX_PERMITS {
        return Err(unavailable("too many outstanding pairing permits"));
    }
    Ok(())
}
fn digest(policy: &Policy) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(policy)?)
        .to_hex()
        .to_string())
}
fn random_token() -> String {
    blake3::hash(&rand::random::<[u8; 32]>())
        .to_hex()
        .to_string()
}
fn secret_hash(salt: &str, secret: &str) -> blake3::Hash {
    let mut hash = blake3::Hasher::new();
    hash.update(salt.as_bytes());
    hash.update(secret.as_bytes());
    hash.finalize()
}

pub(super) fn recover(stored: &mut Stored, timestamp: u64, restarted: bool) -> bool {
    let backwards = timestamp < stored.pairing_last_seen;
    let mut changed = false;
    for attempt in &mut stored.pairing {
        if restarted && attempt.status.state == State::Pending {
            attempt.terminate(State::Rejected, timestamp);
            attempt.status.terminal_reason = Some("exchange_lost".into());
            changed = true;
        } else if matches!(attempt.status.state, State::Issued | State::Pending)
            && (backwards || timestamp >= attempt.status.deadline)
        {
            attempt.terminate(State::Expired, attempt.status.deadline);
            changed = true;
        }
    }
    changed
}

fn observe_clock(
    clock: &mut (u64, std::time::Instant),
    wall: u64,
    instant: std::time::Instant,
) -> u64 {
    let floor = clock
        .0
        .saturating_add(instant.duration_since(clock.1).as_secs());
    if wall > floor {
        *clock = (wall, instant);
        wall
    } else {
        floor
    }
}

impl RuntimePeers {
    /// Restricted to the authenticated transport identity; never returns a secret.
    pub fn pairing_network_request(
        &mut self,
        node: EndpointId,
        request: NetworkRequest,
    ) -> Result<Reply> {
        let timestamp = observe_clock(&mut self.pairing_clock, now(), std::time::Instant::now());
        self.pairing_network_request_at(node, request, timestamp)
    }

    fn pairing_network_request_at(
        &mut self,
        node: EndpointId,
        request: NetworkRequest,
        timestamp: u64,
    ) -> Result<Reply> {
        self.ensure_available()?;
        self.prepare_exchange_io(timestamp)?;
        let request = match request {
            NetworkRequest::Send {
                attempt_id,
                sequence,
                digest,
                payload,
            } => {
                return self.exchange_request(
                    None,
                    Some(node),
                    &attempt_id,
                    super::exchange::Operation::Send {
                        sequence,
                        digest,
                        payload,
                    },
                    timestamp,
                );
            }
            NetworkRequest::Receive { attempt_id, ack } => {
                return self.exchange_request(
                    None,
                    Some(node),
                    &attempt_id,
                    super::exchange::Operation::Receive { ack },
                    timestamp,
                );
            }
            other => other,
        };
        let refused = || invalid("pairing request refused");
        let (attempt_id, secret) = match &request {
            NetworkRequest::Present { attempt_id, secret } => (attempt_id, Some(secret)),
            NetworkRequest::Status { attempt_id } => (attempt_id, None),
            NetworkRequest::Send { .. } | NetworkRequest::Receive { .. } => unreachable!(),
        };
        let index = self
            .stored
            .pairing
            .iter()
            .position(|attempt| {
                &attempt.status.attempt_id == attempt_id
                    && attempt
                        .status
                        .terminal_at
                        .is_none_or(|ended| timestamp < ended.saturating_add(RECEIPT_SECONDS))
            })
            .ok_or_else(refused)?;
        let attempt = &self.stored.pairing[index];
        if attempt
            .status
            .bound_node_id
            .is_some_and(|bound| bound != node)
        {
            return Err(refused());
        }
        if let Some(secret) = secret {
            let (Some(salt), Some(expected)) = (&attempt.salt, &attempt.secret_hash) else {
                return Err(refused());
            };
            let actual = secret_hash(salt, &secret.0);
            // Hash equality is constant-time, unlike comparing serialized hex strings.
            if blake3::Hash::from_hex(expected).map_err(|_| refused())? != actual {
                return Err(refused());
            }
        } else if attempt.status.bound_node_id != Some(node) {
            return Err(refused());
        }

        let mut next = self.stored.clone();
        next.pairing_last_seen = timestamp.max(next.pairing_last_seen);
        let attempt = &mut next.pairing[index];
        attempt.expire(timestamp);
        let result = if secret.is_some() {
            match attempt.status.state {
                State::Issued => {
                    attempt.status.state = State::Pending;
                    attempt.status.bound_node_id = Some(node);
                    attempt.status.attempt_revision = 2;
                    Ok(attempt.status.clone())
                }
                State::Pending => Ok(attempt.status.clone()),
                State::Expired | State::Rejected => Err(refused()),
            }
        } else {
            Ok(attempt.status.clone())
        };
        self.commit_pairing(next)?;
        Ok(Reply {
            status: Some(result?),
            secret: None,
            error: None,
            exchange: None,
        })
    }

    pub(crate) fn pairing_request(&mut self, request: Request) -> Result<Reply> {
        let timestamp = observe_clock(&mut self.pairing_clock, now(), std::time::Instant::now());
        self.pairing_request_at(request, timestamp)
    }
    fn pairing_request_at(&mut self, request: Request, timestamp: u64) -> Result<Reply> {
        self.ensure_available()?;
        self.prepare_exchange_io(timestamp)?;
        let request = match request {
            Request::PairSend {
                owner,
                attempt_id,
                sequence,
                digest,
                payload,
            } => {
                return self.exchange_request(
                    Some(&owner),
                    None,
                    &attempt_id,
                    super::exchange::Operation::Send {
                        sequence,
                        digest,
                        payload,
                    },
                    timestamp,
                );
            }
            Request::PairReceive {
                owner,
                attempt_id,
                ack,
            } => {
                return self.exchange_request(
                    Some(&owner),
                    None,
                    &attempt_id,
                    super::exchange::Operation::Receive { ack },
                    timestamp,
                );
            }
            other => other,
        };
        let (owner, creation_id) = match &request {
            Request::Issue {
                owner, creation_id, ..
            }
            | Request::Query { owner, creation_id }
            | Request::Cancel { owner, creation_id } => (owner, creation_id),
            Request::PairSend { .. } | Request::PairReceive { .. } => unreachable!(),
        };
        validate_token(owner, "owner")?;
        validate_token(creation_id, "creation_id")?;
        let mut next = self.stored.clone();
        next.pairing_last_seen = timestamp.max(next.pairing_last_seen);
        for attempt in &mut next.pairing {
            attempt.expire(timestamp);
        }
        next.pairing.retain(|attempt| {
            attempt
                .status
                .terminal_at
                .is_none_or(|ended| timestamp < ended.saturating_add(RECEIPT_SECONDS))
        });
        let index = next
            .pairing
            .iter()
            .position(|a| &a.owner == owner && &a.status.creation_id == creation_id);
        let mut secret = None;
        let index = match request {
            Request::Issue {
                owner,
                creation_id,
                deadline,
                mut policy,
            } => {
                policy.validate()?;
                policy.grants.sort();
                policy.grants.dedup();
                if let Some(index) = index {
                    let prior = &next.pairing[index];
                    if prior.policy != policy || prior.status.deadline != deadline {
                        return Err(invalid(
                            "creation_id was already used for a different permit",
                        ));
                    }
                    index
                } else {
                    if deadline <= timestamp || deadline > timestamp.saturating_add(MAX_LIFETIME) {
                        return Err(invalid(
                            "permit deadline must be within the next 600 seconds",
                        ));
                    }
                    if next.pairing.len() >= MAX_ATTEMPTS
                        || next
                            .pairing
                            .iter()
                            .filter(|a| matches!(a.status.state, State::Issued | State::Pending))
                            .count()
                            >= MAX_PERMITS
                    {
                        return Err(invalid(
                            "pairing permit or reserved receipt capacity exhausted",
                        ));
                    }
                    let plaintext = random_token();
                    let salt = random_token();
                    let hash = secret_hash(&salt, &plaintext).to_hex().to_string();
                    let status = Status {
                        attempt_id: random_token(),
                        creation_id,
                        state: State::Issued,
                        attempt_revision: 1,
                        deadline,
                        policy_digest: digest(&policy)?,
                        terminal_at: None,
                        bound_node_id: None,
                        terminal_reason: None,
                    };
                    next.pairing.push(Attempt {
                        owner,
                        policy,
                        status,
                        salt: Some(salt),
                        secret_hash: Some(hash),
                    });
                    secret = Some(Secret(plaintext));
                    next.pairing.len() - 1
                }
            }
            Request::Query { .. } => {
                index.ok_or_else(|| invalid("unknown or expired creation_id"))?
            }
            Request::Cancel { .. } => {
                let index = index.ok_or_else(|| invalid("unknown or expired creation_id"))?;
                if matches!(
                    next.pairing[index].status.state,
                    State::Issued | State::Pending
                ) {
                    next.pairing[index].terminate(State::Rejected, timestamp);
                }
                index
            }
            Request::PairSend { .. } | Request::PairReceive { .. } => unreachable!(),
        };
        let status = next.pairing[index].status.clone();
        self.commit_pairing(next)?;
        Ok(Reply {
            status: Some(status),
            secret,
            error: None,
            exchange: None,
        })
    }

    /// Proactive cleanup under the same daemon commit barrier as pairing IO.
    /// An idle sweep does not advance durable highwater or write the state file.
    pub(crate) fn pairing_expire(&mut self) -> Result<()> {
        let timestamp = observe_clock(&mut self.pairing_clock, now(), std::time::Instant::now())
            .max(self.stored.pairing_last_seen);
        // Quarantine must not extend transcript lifetime. Keep only rejection
        // tombstones and budgets until a healthy commit can close the attempt.
        self.erase_expired_exchange_payloads(timestamp);
        self.ensure_available()?;
        let mut next = self.stored.clone();
        recover(&mut next, timestamp, false);
        next.pairing.retain(|attempt| {
            attempt
                .status
                .terminal_at
                .is_none_or(|ended| timestamp < ended.saturating_add(RECEIPT_SECONDS))
        });
        // Erase expired payloads even if persisting their terminal state fails.
        self.exchanges.retain(|id, _| {
            next.pairing.iter().any(|attempt| {
                &attempt.status.attempt_id == id && attempt.status.state == State::Pending
            })
        });
        if next.pairing != self.stored.pairing {
            next.pairing_last_seen = timestamp;
            self.commit_pairing(next)?;
        } else {
            self.prune_exchanges();
        }
        Ok(())
    }

    pub(super) fn commit_pairing(&mut self, next: Stored) -> Result<()> {
        // Same atomic file and sequencer as runtime grants; never compose pairing into policy.
        if next.pairing != self.stored.pairing
            || next.pairing_last_seen != self.stored.pairing_last_seen
        {
            if let Err(error) = self.persist(&next) {
                if crate::error::code_of(&error) == ErrorCode::RuntimeCommitIndeterminate {
                    self.set_fault(&error);
                }
                return Err(error);
            }
            self.stored = next;
        }
        self.prune_exchanges();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn issue(id: &str, deadline: u64) -> Request {
        Request::Issue {
            owner: "fleet".into(),
            creation_id: id.into(),
            deadline,
            policy: Policy {
                grants: vec!["st3-client-v0".into()],
                name: None,
                direction: Direction::Normal,
            },
        }
    }
    fn open(dir: &tempfile::TempDir) -> RuntimePeers {
        let (store, _) = RuntimePeers::open(&FabricHome::new(dir.path()));
        assert!(store.fault().is_none());
        store
    }
    fn query(id: &str) -> Request {
        Request::Query {
            owner: "fleet".into(),
            creation_id: id.into(),
        }
    }
    #[test]
    fn permit_is_private_durable_idempotent_and_never_grants_access() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let reply = store.pairing_request_at(issue("one", t + 600), t).unwrap();
        let secret = reply.secret.unwrap();
        assert!(!format!("{secret:?}").contains(&secret.0));
        let disk = fs::read_to_string(&store.path).unwrap();
        assert!(!disk.contains(&secret.0));
        assert!(store.snapshot(None).unwrap().peers.is_empty());
        assert_eq!(store.stored.revision, 0);
        let mut restarted = open(&dir);
        let replay = restarted
            .pairing_request_at(issue("one", t + 600), t)
            .unwrap();
        assert!(replay.secret.is_none());
        assert_eq!(
            replay.status.unwrap().attempt_id,
            reply.status.unwrap().attempt_id
        );
        assert!(
            restarted
                .pairing_request_at(issue("one", t + 601), t)
                .is_err()
        );
        assert!(
            restarted
                .pairing_request_at(
                    Request::Query {
                        owner: "other".into(),
                        creation_id: "one".into()
                    },
                    t
                )
                .is_err()
        );
        let cancelled = restarted
            .pairing_request_at(
                Request::Cancel {
                    owner: "fleet".into(),
                    creation_id: "one".into(),
                },
                t,
            )
            .unwrap();
        assert_eq!(cancelled.status.unwrap().state, State::Rejected);
        assert!(
            !fs::read_to_string(&store.path)
                .unwrap()
                .contains("secret_hash")
        );
        assert_eq!(
            open(&dir)
                .pairing_request_at(query("one"), t)
                .unwrap()
                .status
                .unwrap()
                .state,
            State::Rejected
        );
    }
    #[test]
    fn expiry_capacity_and_receipt_reservation_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        assert!(
            store
                .pairing_request_at(issue("invalid", t + 601), t)
                .is_err()
        );
        for i in 0..MAX_PERMITS {
            store
                .pairing_request_at(issue(&i.to_string(), t + 600), t)
                .unwrap();
        }
        assert!(store.pairing_request_at(issue("full", t + 600), t).is_err());
        assert_eq!(
            store
                .pairing_request_at(query("0"), t + 600)
                .unwrap()
                .status
                .unwrap()
                .state,
            State::Expired
        );
        store
            .pairing_request_at(issue("new", t + 1200), t + 600)
            .unwrap();
        assert!(
            store
                .pairing_request_at(query("0"), t + 600 + RECEIPT_SECONDS)
                .is_err()
        );
        assert!(store.snapshot(None).unwrap().peers.is_empty());
    }
    #[test]
    fn uncertain_commit_faults_and_restart_reconciles_without_secret_replay() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        store.fail_directory_sync = true;
        assert!(
            store
                .pairing_request_at(issue("crash", t + 600), t)
                .is_err()
        );
        assert!(store.fault().is_some());
        assert!(store.pairing_request_at(query("crash"), t).is_err());
        let reply = open(&dir).pairing_request_at(query("crash"), t).unwrap();
        assert!(reply.secret.is_none());
        assert_eq!(reply.status.unwrap().state, State::Issued);
    }
    #[test]
    fn clock_forward_then_backward_does_not_extend_live_permit() {
        let instant = std::time::Instant::now();
        let mut clock = (100, instant);
        assert_eq!(observe_clock(&mut clock, 400, instant), 400);
        assert_eq!(
            observe_clock(
                &mut clock,
                100,
                instant + std::time::Duration::from_secs(600)
            ),
            1000
        );
    }
    #[test]
    fn restart_expires_deadlines_and_fails_closed_on_clock_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        store.pairing_request_at(issue("one", 700), 100).unwrap();
        let mut expired = store.stored.clone();
        assert!(recover(&mut expired, 700, true));
        assert_eq!(expired.pairing[0].status.state, State::Expired);
        assert!(recover(&mut store.stored, 99, true));
        assert_eq!(store.stored.pairing[0].status.state, State::Expired);
        assert!(store.stored.pairing[0].secret_hash.is_none());
    }
    #[test]
    fn failed_pre_rename_commit_leaves_no_permit_and_can_retry() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let path = store.path.clone();
        store.path = dir.path().join("missing/runtime-peers.toml");
        assert!(
            store
                .pairing_request_at(issue("retry", t + 600), t)
                .is_err()
        );
        assert!(store.stored.pairing.is_empty());
        assert!(store.fault().is_none());
        store.path = path;
        assert!(
            store
                .pairing_request_at(issue("retry", t + 600), t)
                .unwrap()
                .secret
                .is_some()
        );
    }
    #[test]
    fn receipt_pool_exhaustion_cannot_prevent_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        store
            .pairing_request_at(issue("active", t + 600), t)
            .unwrap();
        let mut terminal = store.stored.pairing[0].clone();
        terminal.terminate(State::Rejected, t);
        for i in 1..MAX_ATTEMPTS {
            terminal.status.creation_id = format!("retained-{i}");
            terminal.status.attempt_id = format!("attempt-{i}");
            store.stored.pairing.push(terminal.clone());
        }
        assert!(store.pairing_request_at(issue("full", t + 600), t).is_err());
        let cancelled = store
            .pairing_request_at(
                Request::Cancel {
                    owner: "fleet".into(),
                    creation_id: "active".into(),
                },
                t,
            )
            .unwrap();
        assert_eq!(cancelled.status.unwrap().state, State::Rejected);
        assert_eq!(store.stored.pairing.len(), MAX_ATTEMPTS);
    }
    #[test]
    fn control_schema_round_trips_and_rejects_unknown_policy_fields() {
        use crate::control::{ControlRequest, ControlResponse};
        let request = ControlRequest::Pairing {
            request: issue("schema", 700),
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        let decoded: ControlRequest = serde_json::from_slice(&bytes).unwrap();
        assert!(matches!(decoded, ControlRequest::Pairing { .. }));
        let dir = tempfile::tempdir().unwrap();
        let reply = ControlResponse::Pairing {
            reply: open(&dir)
                .pairing_request_at(issue("schema", 700), 100)
                .unwrap(),
        };
        let bytes = serde_json::to_vec(&reply).unwrap();
        assert!(matches!(
            serde_json::from_slice::<ControlResponse>(&bytes).unwrap(),
            ControlResponse::Pairing { .. }
        ));
        let mut value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
        value["request"]["policy"]["wildcard"] = true.into();
        assert!(serde_json::from_value::<ControlRequest>(value).is_err());
    }
    fn node(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn presentation(status: &Status, secret: &Secret) -> NetworkRequest {
        NetworkRequest::Present {
            attempt_id: status.attempt_id.clone(),
            secret: secret.clone(),
        }
    }

    fn restricted_status(status: &Status) -> NetworkRequest {
        NetworkRequest::Status {
            attempt_id: status.attempt_id.clone(),
        }
    }

    fn assert_refused(result: Result<Reply>) {
        let error = result.unwrap_err();
        assert_eq!(crate::error::code_of(&error), ErrorCode::InvalidArgument);
        assert_eq!(error.to_string(), "pairing request refused");
    }

    #[test]
    fn presentation_binds_once_without_policy_or_secret_disclosure() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let issued = store
            .pairing_request_at(issue("binding", t + 600), t)
            .unwrap();
        let status = issued.status.unwrap();
        let secret = issued.secret.unwrap();
        for id in [node(1), node(2)] {
            assert_refused(store.pairing_network_request_at(id, restricted_status(&status), t));
            assert_refused(store.pairing_network_request_at(
                id,
                presentation(&status, &Secret::new("wrong")),
                t,
            ));
        }
        assert_eq!(store.stored.pairing[0].status, status);
        let reply = store
            .pairing_network_request_at(node(1), presentation(&status, &secret), t)
            .unwrap();
        assert!(reply.secret.is_none());
        let pending = reply.status.unwrap();
        assert_eq!(pending.state, State::Pending);
        assert_eq!(pending.attempt_revision, 2);
        assert_eq!(pending.bound_node_id, Some(node(1)));
        assert_eq!(pending.deadline, status.deadline);
        assert_eq!(pending.policy_digest, status.policy_digest);
        assert_eq!(
            store
                .pairing_network_request_at(node(1), presentation(&status, &secret), t,)
                .unwrap()
                .status
                .unwrap(),
            pending
        );
        assert_refused(store.pairing_network_request_at(
            node(2),
            presentation(&status, &secret),
            t,
        ));
        assert_refused(store.pairing_network_request_at(
            node(1),
            presentation(&status, &Secret::new("wrong")),
            t,
        ));
        assert_refused(store.pairing_network_request_at(node(2), restricted_status(&status), t));
        assert!(store.stored.pairing[0].secret_hash.is_some());
        validate(&store.stored.pairing).unwrap();
        assert!(store.snapshot(None).unwrap().peers.is_empty());
        assert!(store.access_for(&[node(1)]).is_empty());
        assert_eq!(store.stored.revision, 0);
        assert!(!fs::read_to_string(&store.path).unwrap().contains(&secret.0));
    }

    #[test]
    fn racing_identities_have_exactly_one_durable_winner() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let issued = store.pairing_request_at(issue("race", t + 600), t).unwrap();
        let status = issued.status.unwrap();
        let request = presentation(&status, &issued.secret.unwrap());
        let shared = std::sync::Arc::new(parking_lot::Mutex::new(store));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = [node(1), node(2)]
            .into_iter()
            .map(|id| {
                let shared = shared.clone();
                let barrier = barrier.clone();
                let request = request.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let result = shared.lock().pairing_network_request_at(id, request, t);
                    (id, result)
                })
            })
            .collect();
        let outcomes: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(outcomes.iter().filter(|(_, r)| r.is_ok()).count(), 1);
        let winner = outcomes.iter().find(|(_, r)| r.is_ok()).unwrap().0;
        let restarted = open(&dir);
        assert_eq!(
            restarted.stored.pairing[0].status.bound_node_id,
            Some(winner)
        );
        assert_eq!(restarted.stored.pairing[0].status.state, State::Rejected);
        assert!(restarted.snapshot(None).unwrap().peers.is_empty());
    }

    #[test]
    fn restart_rejects_pending_but_live_reconcile_preserves_lost_response() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let issued = store.pairing_request_at(issue("lost", t + 600), t).unwrap();
        let status = issued.status.unwrap();
        let request = presentation(&status, &issued.secret.unwrap());
        store.fail_directory_sync = true;
        let error = store
            .pairing_network_request_at(node(1), request.clone(), t)
            .unwrap_err();
        assert_eq!(
            crate::error::code_of(&error),
            ErrorCode::RuntimeCommitIndeterminate
        );
        assert!(store.fault().is_some());
        store.reconcile(&FabricHome::new(dir.path())).unwrap();
        assert_eq!(
            store
                .pairing_network_request_at(node(1), request, t)
                .unwrap()
                .status
                .unwrap()
                .state,
            State::Pending
        );
        let mut restarted = open(&dir);
        let terminal = restarted
            .pairing_network_request_at(node(1), restricted_status(&status), t)
            .unwrap()
            .status
            .unwrap();
        assert_eq!(terminal.state, State::Rejected);
        assert_eq!(terminal.attempt_revision, 3);
        assert_eq!(terminal.bound_node_id, Some(node(1)));
        assert_eq!(terminal.terminal_reason.as_deref(), Some("exchange_lost"));
        assert!(restarted.stored.pairing[0].secret_hash.is_none());
        assert!(restarted.stored.pairing[0].salt.is_none());
        assert_refused(restarted.pairing_network_request_at(
            node(2),
            restricted_status(&status),
            t,
        ));
        assert_eq!(open(&dir).stored.pairing[0].status, terminal);
    }

    #[test]
    fn pending_expiry_and_cancel_keep_only_restricted_terminal_identity() {
        for cancel in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let issued = store
                .pairing_request_at(issue("terminal", t + 600), t)
                .unwrap();
            let status = issued.status.unwrap();
            let request = presentation(&status, &issued.secret.unwrap());
            store
                .pairing_network_request_at(node(1), request.clone(), t)
                .unwrap();
            let timestamp = if cancel { t } else { t + 600 };
            if cancel {
                store
                    .pairing_request_at(
                        Request::Cancel {
                            owner: "fleet".into(),
                            creation_id: "terminal".into(),
                        },
                        timestamp,
                    )
                    .unwrap();
            }
            let terminal = store
                .pairing_network_request_at(node(1), restricted_status(&status), timestamp)
                .unwrap()
                .status
                .unwrap();
            assert_eq!(
                terminal.state,
                if cancel {
                    State::Rejected
                } else {
                    State::Expired
                }
            );
            assert_eq!(
                terminal.terminal_reason.as_deref(),
                Some(if cancel { "cancelled" } else { "expired" })
            );
            assert_eq!(terminal.attempt_revision, 3);
            assert_eq!(terminal.bound_node_id, Some(node(1)));
            assert!(store.stored.pairing[0].secret_hash.is_none());
            assert_refused(store.pairing_network_request_at(node(1), request, timestamp));
            assert_refused(store.pairing_network_request_at(
                node(2),
                restricted_status(&status),
                timestamp,
            ));
            assert_refused(store.pairing_network_request_at(
                node(1),
                restricted_status(&status),
                timestamp + RECEIPT_SECONDS,
            ));
            validate(&store.stored.pairing).unwrap();
            assert!(store.snapshot(None).unwrap().peers.is_empty());
            assert_eq!(store.stored.revision, 0);
            assert_eq!(open(&dir).stored.pairing[0].status, terminal);
        }
    }

    #[test]
    fn invalid_pending_and_terminal_histories_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let issued = store
            .pairing_request_at(issue("validation", t + 600), t)
            .unwrap();
        let status = issued.status.unwrap();
        store
            .pairing_network_request_at(node(1), presentation(&status, &issued.secret.unwrap()), t)
            .unwrap();
        let pending = store.stored.pairing.clone();
        for corrupt in 0..5 {
            let mut attempts = pending.clone();
            match corrupt {
                0 => attempts[0].status.bound_node_id = None,
                1 => attempts[0].status.attempt_revision = 1,
                2 => attempts[0].secret_hash = None,
                3 => attempts[0].status.terminal_at = Some(t),
                _ => attempts[0].status.terminal_reason = Some("exchange_lost".into()),
            }
            assert!(validate(&attempts).is_err());
        }
        let mut terminal = pending;
        terminal[0].terminate(State::Rejected, t);
        validate(&terminal).unwrap();
        terminal[0].status.attempt_revision = 2;
        assert!(validate(&terminal).is_err());
        terminal[0].status.attempt_revision = 3;
        terminal[0].secret_hash = Some("retained".into());
        assert!(validate(&terminal).is_err());
    }

    #[test]
    fn restricted_network_schema_is_strict_and_redacts_secrets() {
        let request = NetworkRequest::Present {
            attempt_id: "attempt".into(),
            secret: Secret::new("private"),
        };
        assert!(!format!("{request:?}").contains("private"));
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["operation"], "present");
        assert!(matches!(
            serde_json::from_value::<NetworkRequest>(value).unwrap(),
            NetworkRequest::Present { .. }
        ));
        assert!(
            serde_json::from_str::<NetworkRequest>(
                r#"{"operation":"status","attempt_id":"a","secret":"private"}"#,
            )
            .is_err()
        );
    }
    #[test]
    fn binding_pre_rename_failure_can_retry_and_expired_permit_cannot_bind() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let issued = store
            .pairing_request_at(issue("retry-binding", t + 600), t)
            .unwrap();
        let status = issued.status.unwrap();
        let request = presentation(&status, &issued.secret.unwrap());
        let path = store.path.clone();
        store.path = dir.path().join("missing/runtime-peers.toml");
        assert!(
            store
                .pairing_network_request_at(node(1), request.clone(), t)
                .is_err()
        );
        assert_eq!(store.stored.pairing[0].status.state, State::Issued);
        assert!(store.stored.pairing[0].status.bound_node_id.is_none());
        assert!(store.fault().is_none());
        store.path = path;
        let pending = store
            .pairing_network_request_at(node(2), request, t)
            .unwrap()
            .status
            .unwrap();
        assert_eq!(pending.bound_node_id, Some(node(2)));

        let issued = store
            .pairing_request_at(issue("expired-binding", t + 600), t)
            .unwrap();
        let status = issued.status.unwrap();
        assert_refused(store.pairing_network_request_at(
            node(1),
            presentation(&status, &issued.secret.unwrap()),
            t + 600,
        ));
        let expired = &store.stored.pairing[1];
        assert_eq!(expired.status.state, State::Expired);
        assert_eq!(expired.status.attempt_revision, 2);
        assert!(expired.status.bound_node_id.is_none());
        assert!(expired.secret_hash.is_none());
        assert_refused(store.pairing_network_request_at(
            node(1),
            restricted_status(&status),
            t + 600,
        ));
        assert!(store.snapshot(None).unwrap().peers.is_empty());
    }

    fn pending_exchange(store: &mut RuntimePeers, id: &str, t: u64) -> Status {
        let issued = store.pairing_request_at(issue(id, t + 600), t).unwrap();
        let status = issued.status.unwrap();
        store
            .pairing_network_request_at(node(1), presentation(&status, &issued.secret.unwrap()), t)
            .unwrap()
            .status
            .unwrap()
    }

    fn exchange_send(
        store: &mut RuntimePeers,
        status: &Status,
        owner: bool,
        sequence: u64,
        bytes: Vec<u8>,
        t: u64,
    ) -> Result<Reply> {
        let digest = blake3::hash(&bytes).to_hex().to_string();
        let payload = Payload::new(bytes);
        if owner {
            store.pairing_request_at(
                Request::PairSend {
                    owner: "fleet".into(),
                    attempt_id: status.attempt_id.clone(),
                    sequence,
                    digest,
                    payload,
                },
                t,
            )
        } else {
            store.pairing_network_request_at(
                node(1),
                NetworkRequest::Send {
                    attempt_id: status.attempt_id.clone(),
                    sequence,
                    digest,
                    payload,
                },
                t,
            )
        }
    }

    fn exchange_receive(
        store: &mut RuntimePeers,
        status: &Status,
        owner: bool,
        ack: Option<u64>,
        t: u64,
    ) -> Result<Reply> {
        if owner {
            store.pairing_request_at(
                Request::PairReceive {
                    owner: "fleet".into(),
                    attempt_id: status.attempt_id.clone(),
                    ack,
                },
                t,
            )
        } else {
            store.pairing_network_request_at(
                node(1),
                NetworkRequest::Receive {
                    attempt_id: status.attempt_id.clone(),
                    ack,
                },
                t,
            )
        }
    }

    #[test]
    fn opaque_exchange_is_bidirectional_replayed_acknowledged_and_memory_only() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "opaque", t);
        assert!(
            exchange_receive(&mut store, &status, true, None, t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .is_none()
        );
        let bytes = vec![0, 255, 42, 128, 0];
        let sent = exchange_send(&mut store, &status, false, 1, bytes.clone(), t)
            .unwrap()
            .exchange
            .unwrap();
        assert_eq!(
            (sent.sent_sequence, sent.total_bytes, sent.total_messages),
            (Some(1), 5, 1)
        );
        let first = exchange_receive(&mut store, &status, true, None, t)
            .unwrap()
            .exchange
            .unwrap()
            .message
            .unwrap();
        assert_eq!(first.direction, MessageDirection::NodeToOwner);
        assert_eq!(first.payload.as_bytes(), bytes);
        assert_eq!(first.digest, blake3::hash(&bytes).to_hex().to_string());
        assert_eq!(
            exchange_receive(&mut store, &status, true, None, t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .unwrap(),
            first
        );
        exchange_send(
            &mut store,
            &status,
            true,
            1,
            b"owner-opaque-private".to_vec(),
            t,
        )
        .unwrap();
        let reverse = exchange_receive(&mut store, &status, false, None, t)
            .unwrap()
            .exchange
            .unwrap()
            .message
            .unwrap();
        assert_eq!(reverse.direction, MessageDirection::OwnerToNode);
        assert_eq!(reverse.payload.as_bytes(), b"owner-opaque-private");
        assert!(
            exchange_receive(&mut store, &status, true, Some(1), t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .is_none()
        );
        assert!(
            exchange_receive(&mut store, &status, true, Some(1), t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .is_none()
        );
        let duplicate = exchange_send(&mut store, &status, false, 1, bytes, t)
            .unwrap()
            .exchange
            .unwrap();
        assert_eq!((duplicate.total_bytes, duplicate.total_messages), (25, 2));
        assert!(
            exchange_receive(&mut store, &status, true, None, t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .is_none()
        );
        let disk = fs::read_to_string(&store.path).unwrap();
        assert!(!disk.contains("owner-opaque-private"));
        assert!(!disk.contains(&reverse.digest));
        assert!(!disk.contains("total_bytes"));
        assert!(!format!("{:?}", reverse.payload).contains("owner-opaque-private"));
        let encoded = serde_json::to_value(&reverse.payload).unwrap();
        assert!(encoded.is_array());
        assert_eq!(
            serde_json::from_value::<Payload>(encoded).unwrap(),
            reverse.payload
        );
        assert!(store.snapshot(None).unwrap().peers.is_empty());
        assert!(store.access_for(&[node(1)]).is_empty());
        assert_eq!(store.stored.revision, 0);
    }

    #[test]
    fn exchange_rejects_digest_conflicts_gaps_and_unseen_acks_without_spending_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "ordering", t);
        for sequence in [0, 2, u64::MAX] {
            assert!(exchange_send(&mut store, &status, false, sequence, vec![1], t).is_err());
        }
        for digest in [
            "invalid".to_owned(),
            blake3::hash(&[2]).to_hex().to_string(),
        ] {
            assert!(
                store
                    .pairing_network_request_at(
                        node(1),
                        NetworkRequest::Send {
                            attempt_id: status.attempt_id.clone(),
                            sequence: 1,
                            digest,
                            payload: Payload::new(vec![1]),
                        },
                        t
                    )
                    .is_err()
            );
        }
        exchange_send(&mut store, &status, false, 1, vec![1], t).unwrap();
        exchange_send(&mut store, &status, false, 2, vec![2], t).unwrap();
        for ack in [0, 1, 2, u64::MAX] {
            assert!(exchange_receive(&mut store, &status, true, Some(ack), t).is_err());
        }
        assert!(exchange_send(&mut store, &status, false, 1, vec![9], t).is_err());
        assert!(exchange_send(&mut store, &status, false, 4, vec![4], t).is_err());
        let received = exchange_receive(&mut store, &status, true, None, t)
            .unwrap()
            .exchange
            .unwrap();
        assert_eq!((received.total_bytes, received.total_messages), (2, 2));
        assert_eq!(received.message.unwrap().sequence, 1);
        assert!(exchange_receive(&mut store, &status, true, Some(2), t).is_err());
        assert_eq!(
            exchange_receive(&mut store, &status, true, Some(1), t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .unwrap()
                .sequence,
            2
        );
        assert!(
            exchange_receive(&mut store, &status, true, Some(2), t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .is_none()
        );
        assert!(exchange_send(&mut store, &status, false, 1, vec![9], t).is_err());
        assert_eq!(
            exchange_send(&mut store, &status, false, 1, vec![1], t)
                .unwrap()
                .exchange
                .unwrap()
                .total_messages,
            2
        );
    }

    #[test]
    fn exchange_enforces_payload_shared_byte_and_direction_message_bounds_durably() {
        // Queued and acknowledged bytes both consume the lifetime budget.
        for acknowledge in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let status = pending_exchange(&mut store, "bytes", t);
            for sequence in 1..=8 {
                for owner in [false, true] {
                    exchange_send(&mut store, &status, owner, sequence, vec![42; 65536], t)
                        .unwrap();
                    if acknowledge {
                        exchange_receive(&mut store, &status, !owner, None, t).unwrap();
                        exchange_receive(&mut store, &status, !owner, Some(sequence), t).unwrap();
                    }
                }
            }
            let at_limit = exchange_send(&mut store, &status, false, 8, vec![42; 65536], t)
                .unwrap()
                .exchange
                .unwrap();
            assert_eq!(
                (at_limit.total_bytes, at_limit.total_messages),
                (1048576, 16)
            );
            let rejected = exchange_send(&mut store, &status, false, 9, vec![0], t).unwrap();
            assert_exchange_limit(&mut store, &status, rejected, &dir, t);
        }
        for owner in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let status = pending_exchange(&mut store, "count", t);
            for sequence in 1..=16 {
                exchange_send(&mut store, &status, owner, sequence, vec![], t).unwrap();
            }
            // The other direction can still use all sixteen messages.
            for sequence in 1..=16 {
                exchange_send(&mut store, &status, !owner, sequence, vec![], t).unwrap();
            }
            assert_eq!(
                exchange_send(&mut store, &status, owner, 16, vec![], t)
                    .unwrap()
                    .exchange
                    .unwrap()
                    .total_messages,
                32
            );
            let rejected = exchange_send(&mut store, &status, owner, 17, vec![], t).unwrap();
            assert_exchange_limit(&mut store, &status, rejected, &dir, t);
        }
        for owner in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let status = pending_exchange(&mut store, "oversize", t);
            exchange_send(&mut store, &status, !owner, 1, vec![1], t).unwrap();
            let rejected = exchange_send(&mut store, &status, owner, 1, vec![1; 65537], t).unwrap();
            assert_exchange_limit(&mut store, &status, rejected, &dir, t);
        }
    }

    fn assert_exchange_limit(
        store: &mut RuntimePeers,
        status: &Status,
        reply: Reply,
        dir: &tempfile::TempDir,
        t: u64,
    ) {
        let rejected = reply.status.unwrap();
        assert_eq!(rejected.state, State::Rejected);
        assert_eq!(rejected.terminal_reason.as_deref(), Some("exchange_limit"));
        assert!(reply.exchange.is_none());
        assert!(store.exchanges.is_empty());
        assert!(store.stored.pairing[0].secret_hash.is_none());
        assert!(store.stored.pairing[0].salt.is_none());
        assert!(
            exchange_receive(store, status, true, None, t)
                .unwrap()
                .exchange
                .is_none()
        );
        assert!(
            exchange_receive(store, status, false, None, t)
                .unwrap()
                .exchange
                .is_none()
        );
        assert!(store.snapshot(None).unwrap().peers.is_empty());
        assert_eq!(
            open(dir)
                .pairing_request_at(query(&status.creation_id), t)
                .unwrap()
                .status
                .unwrap()
                .terminal_reason
                .as_deref(),
            Some("exchange_limit")
        );
    }

    #[test]
    fn exchange_requires_creating_owner_bound_node_and_verified_pending_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let issued = store
            .pairing_request_at(issue("unbound", t + 600), t)
            .unwrap()
            .status
            .unwrap();
        assert!(
            exchange_send(&mut store, &issued, true, 1, vec![1], t)
                .unwrap()
                .exchange
                .is_none()
        );
        assert!(exchange_receive(&mut store, &issued, false, None, t).is_err());
        assert!(store.exchanges.is_empty());
        let status = pending_exchange(&mut store, "bound", t);
        let other = pending_exchange(&mut store, "other", t);
        exchange_send(&mut store, &status, false, 1, vec![42], t).unwrap();
        for attempt_id in [&status.attempt_id, &"unknown".to_owned()] {
            for request in [
                Request::PairReceive {
                    owner: "intruder".into(),
                    attempt_id: attempt_id.clone(),
                    ack: None,
                },
                Request::PairSend {
                    owner: "intruder".into(),
                    attempt_id: attempt_id.clone(),
                    sequence: 1,
                    digest: blake3::hash(&[1]).to_hex().to_string(),
                    payload: Payload::new(vec![1]),
                },
            ] {
                assert!(store.pairing_request_at(request, t).is_err());
            }
            for request in [
                NetworkRequest::Receive {
                    attempt_id: attempt_id.clone(),
                    ack: None,
                },
                NetworkRequest::Send {
                    attempt_id: attempt_id.clone(),
                    sequence: 1,
                    digest: blake3::hash(&[1]).to_hex().to_string(),
                    payload: Payload::new(vec![1]),
                },
            ] {
                assert!(
                    store
                        .pairing_network_request_at(node(2), request, t)
                        .is_err()
                );
            }
        }
        assert!(
            exchange_receive(&mut store, &other, true, None, t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .is_none()
        );
        assert_eq!(
            exchange_receive(&mut store, &status, true, None, t)
                .unwrap()
                .exchange
                .unwrap()
                .message
                .unwrap()
                .payload
                .as_bytes(),
            &[42]
        );
    }

    #[test]
    fn exchange_reconnect_and_live_reconcile_preserve_buffers_and_lifetime_budgets() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let issued = store
            .pairing_request_at(issue("resume", t + 600), t)
            .unwrap();
        let status = issued.status.unwrap();
        let present = presentation(&status, &issued.secret.unwrap());
        store
            .pairing_network_request_at(node(1), present.clone(), t)
            .unwrap();
        for sequence in 1..=16 {
            exchange_send(&mut store, &status, false, sequence, vec![7], t).unwrap();
        }
        exchange_receive(&mut store, &status, true, None, t).unwrap();
        exchange_receive(&mut store, &status, true, Some(1), t).unwrap();
        store
            .pairing_network_request_at(node(1), present, t)
            .unwrap();
        store.reconcile(&FabricHome::new(dir.path())).unwrap();
        let resumed = exchange_receive(&mut store, &status, true, None, t)
            .unwrap()
            .exchange
            .unwrap();
        assert_eq!(resumed.message.unwrap().sequence, 2);
        assert_eq!((resumed.total_bytes, resumed.total_messages), (16, 16));
        assert_eq!(
            exchange_send(&mut store, &status, false, 1, vec![7], t)
                .unwrap()
                .exchange
                .unwrap()
                .total_messages,
            16
        );
        let rejected = exchange_send(&mut store, &status, false, 17, vec![7], t).unwrap();
        assert_exchange_limit(&mut store, &status, rejected, &dir, t);
    }

    #[test]
    fn exchange_terminal_transitions_erase_traffic_and_restart_rejects_pending() {
        for cancel in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let status = pending_exchange(&mut store, "terminal", t);
            exchange_send(&mut store, &status, false, 1, vec![42], t).unwrap();
            exchange_send(&mut store, &status, true, 1, vec![43], t).unwrap();
            let time = if cancel {
                store
                    .pairing_request_at(
                        Request::Cancel {
                            owner: "fleet".into(),
                            creation_id: status.creation_id.clone(),
                        },
                        t,
                    )
                    .unwrap();
                t
            } else {
                t + 600
            };
            let reply = exchange_receive(&mut store, &status, true, None, time).unwrap();
            assert!(reply.exchange.is_none());
            assert!(store.exchanges.is_empty());
            assert!(
                exchange_receive(&mut store, &status, false, None, time)
                    .unwrap()
                    .exchange
                    .is_none()
            );
            assert!(
                exchange_send(&mut store, &status, true, 2, vec![44], time)
                    .unwrap()
                    .exchange
                    .is_none()
            );
        }
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "restart", t);
        exchange_send(&mut store, &status, false, 1, vec![42], t).unwrap();
        drop(store);
        let mut restarted = open(&dir);
        assert!(restarted.exchanges.is_empty());
        let reply = exchange_receive(&mut restarted, &status, false, None, t).unwrap();
        assert!(reply.exchange.is_none());
        assert_eq!(
            reply.status.unwrap().terminal_reason.as_deref(),
            Some("exchange_lost")
        );
        assert_eq!(
            open(&dir)
                .pairing_request_at(query("restart"), t)
                .unwrap()
                .status
                .unwrap()
                .terminal_reason
                .as_deref(),
            Some("exchange_lost")
        );
    }

    #[test]
    fn refused_exchange_io_also_prunes_expired_buffers() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "expiry", t);
        exchange_send(&mut store, &status, false, 1, vec![42], t).unwrap();
        assert!(
            store
                .pairing_network_request_at(
                    node(2),
                    NetworkRequest::Receive {
                        attempt_id: "unknown".into(),
                        ack: None,
                    },
                    t + 600
                )
                .is_err()
        );
        assert!(store.exchanges.is_empty());
        assert_eq!(store.stored.pairing[0].status.state, State::Expired);
    }

    #[test]
    fn exchange_direction_limit_applies_without_exhausting_other_direction() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "direction-count", t);
        for sequence in 1..=16 {
            exchange_send(&mut store, &status, false, sequence, vec![], t).unwrap();
        }
        let rejected = exchange_send(&mut store, &status, false, 17, vec![], t).unwrap();
        assert_exchange_limit(&mut store, &status, rejected, &dir, t);
    }

    #[test]
    fn exchange_uses_effective_monotonic_expiry_for_both_surfaces() {
        for owner in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let status = pending_exchange(&mut store, "clock", t);
            exchange_send(&mut store, &status, false, 1, vec![42], t).unwrap();
            store.pairing_clock = (t + 600, std::time::Instant::now());
            let reply = if owner {
                store.pairing_request(Request::PairReceive {
                    owner: "fleet".into(),
                    attempt_id: status.attempt_id,
                    ack: None,
                })
            } else {
                store.pairing_network_request(
                    node(1),
                    NetworkRequest::Receive {
                        attempt_id: status.attempt_id,
                        ack: None,
                    },
                )
            }
            .unwrap();
            assert!(reply.exchange.is_none());
            assert_eq!(reply.status.unwrap().state, State::Expired);
            assert!(store.exchanges.is_empty());
        }
    }

    #[test]
    fn uncertain_limit_rejection_cannot_reset_budget_after_live_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "limit-fault", t);
        exchange_send(&mut store, &status, false, 1, vec![42], t).unwrap();
        let path = store.path.clone();
        store.path = dir.path().join("missing/runtime-peers.toml");
        assert!(exchange_send(&mut store, &status, false, 2, vec![42; 65537], t).is_err());
        assert!(store.fault().is_some());
        store.path = path;
        store.reconcile(&FabricHome::new(dir.path())).unwrap();
        let rejected = exchange_receive(&mut store, &status, true, None, t).unwrap();
        assert_exchange_limit(&mut store, &status, rejected, &dir, t);
    }

    #[test]
    fn live_reconcile_prunes_buffers_when_pending_binding_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "changed-binding", t);
        exchange_send(&mut store, &status, false, 1, vec![42], t).unwrap();
        let mut changed = store.stored.clone();
        changed.pairing[0].status.bound_node_id = Some(node(2));
        store.persist(&changed).unwrap();
        store.reconcile(&FabricHome::new(dir.path())).unwrap();
        assert!(store.exchanges.is_empty());
        assert!(exchange_receive(&mut store, &status, false, None, t).is_err());
        let reply = store
            .pairing_network_request_at(
                node(2),
                NetworkRequest::Receive {
                    attempt_id: status.attempt_id,
                    ack: None,
                },
                t,
            )
            .unwrap();
        assert!(reply.exchange.unwrap().message.is_none());
    }

    #[test]
    fn proactive_pairing_expiry_erases_idle_queues_without_writing_idle_ticks() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "idle-expiry", t);
        exchange_send(&mut store, &status, false, 1, vec![42], t).unwrap();
        exchange_send(&mut store, &status, true, 1, vec![43], t).unwrap();
        let disk = fs::read_to_string(&store.path).unwrap();
        let highwater = store.stored.pairing_last_seen;
        // A needless persist would fail this otherwise idle tick.
        store.fail_directory_sync = true;
        store.pairing_expire().unwrap();
        assert!(store.fault().is_none());
        assert_eq!(store.stored.pairing_last_seen, highwater);
        assert_eq!(fs::read_to_string(&store.path).unwrap(), disk);
        assert_eq!(store.exchanges.len(), 1);
        store.fail_directory_sync = false;
        store.pairing_clock = (t + 600, std::time::Instant::now());
        store.pairing_expire().unwrap();
        assert!(store.exchanges.is_empty());
        assert_eq!(store.stored.pairing[0].status.state, State::Expired);
        assert!(store.stored.pairing[0].secret_hash.is_none());
        assert_eq!(store.stored.pairing_last_seen, t + 600);
        assert_eq!(open(&dir).stored.pairing[0].status.state, State::Expired);
        store.pairing_clock = (t + 600 + RECEIPT_SECONDS, std::time::Instant::now());
        store.pairing_expire().unwrap();
        assert!(store.stored.pairing.is_empty());
    }

    #[test]
    fn durable_exchange_limit_receipt_reopens_without_fault_and_requires_bound_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let status = pending_exchange(&mut store, "restart-limit", t);
        let rejected = exchange_send(&mut store, &status, false, 1, vec![42; 65537], t).unwrap();
        assert_eq!(
            rejected.status.unwrap().terminal_reason.as_deref(),
            Some("exchange_limit")
        );
        drop(store);
        let (mut restarted, _) = RuntimePeers::open(&FabricHome::new(dir.path()));
        assert!(restarted.fault().is_none());
        assert!(restarted.snapshot(None).unwrap().peers.is_empty());
        let reply = exchange_receive(&mut restarted, &status, false, None, t).unwrap();
        assert!(reply.exchange.is_none());
        assert_eq!(
            reply.status.unwrap().terminal_reason.as_deref(),
            Some("exchange_limit")
        );
        assert!(restarted.exchanges.is_empty());
        let mut history = restarted.stored.pairing.clone();
        validate(&history).unwrap();
        history[0].status.bound_node_id = None;
        history[0].status.attempt_revision = 2;
        assert!(validate(&history).is_err());
        history[0].status.bound_node_id = Some(node(1));
        history[0].status.attempt_revision = 3;
        history[0].status.state = State::Expired;
        assert!(validate(&history).is_err());
    }
}
