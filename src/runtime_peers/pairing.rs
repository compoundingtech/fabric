//! Permit control only: issuing a permit never creates a peer or grants access.
use super::*;

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Issued,
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
}

/// Serialized only in the first successful creation reply, never Debug output.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Attempt {
    owner: String,
    policy: Policy,
    status: Status,
    salt: Option<String>,
    secret_hash: Option<String>,
}
impl Attempt {
    fn expire(&mut self, timestamp: u64) {
        if self.status.state == State::Issued && timestamp >= self.status.deadline {
            self.terminate(State::Expired, self.status.deadline);
        }
    }
    fn terminate(&mut self, state: State, timestamp: u64) {
        self.status.state = state;
        self.status.attempt_revision += 1;
        self.status.terminal_at = Some(timestamp);
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
        let active = status.state == State::Issued;
        issued += usize::from(active);
        if !creations.insert((&attempt.owner, &status.creation_id))
            || !ids.insert(&status.attempt_id)
            || status.policy_digest != digest(&attempt.policy)?
            || status.attempt_revision != if active { 1 } else { 2 }
            || active != status.terminal_at.is_none()
            || active != attempt.salt.is_some()
            || active != attempt.secret_hash.is_some()
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
fn secret_hash(salt: &str, secret: &str) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(salt.as_bytes());
    hash.update(secret.as_bytes());
    hash.finalize().to_hex().to_string()
}

pub(super) fn recover(stored: &mut Stored, timestamp: u64) -> bool {
    let backwards = timestamp < stored.pairing_last_seen;
    let mut changed = false;
    for attempt in &mut stored.pairing {
        if attempt.status.state == State::Issued
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
    pub(crate) fn pairing_request(&mut self, request: Request) -> Result<Reply> {
        let timestamp = observe_clock(&mut self.pairing_clock, now(), std::time::Instant::now());
        self.pairing_request_at(request, timestamp)
    }
    fn pairing_request_at(&mut self, request: Request, timestamp: u64) -> Result<Reply> {
        self.ensure_available()?;
        let (owner, creation_id) = match &request {
            Request::Issue {
                owner, creation_id, ..
            }
            | Request::Query { owner, creation_id }
            | Request::Cancel { owner, creation_id } => (owner, creation_id),
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
                            .filter(|a| a.status.state == State::Issued)
                            .count()
                            >= MAX_PERMITS
                    {
                        return Err(invalid(
                            "pairing permit or reserved receipt capacity exhausted",
                        ));
                    }
                    let plaintext = random_token();
                    let salt = random_token();
                    let hash = secret_hash(&salt, &plaintext);
                    let status = Status {
                        attempt_id: random_token(),
                        creation_id,
                        state: State::Issued,
                        attempt_revision: 1,
                        deadline,
                        policy_digest: digest(&policy)?,
                        terminal_at: None,
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
                if next.pairing[index].status.state == State::Issued {
                    next.pairing[index].terminate(State::Rejected, timestamp);
                }
                index
            }
        };
        let status = next.pairing[index].status.clone();
        // Same atomic file and sequencer as runtime grants; permits are never composed into policy.
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
        Ok(Reply {
            status: Some(status),
            secret,
            error: None,
        })
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
        assert!(recover(&mut expired, 700));
        assert_eq!(expired.pairing[0].status.state, State::Expired);
        assert!(recover(&mut store.stored, 99));
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
}
