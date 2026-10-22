//! Permit control only: issuing a permit never creates a peer or grants access.
use super::*;

const MAX_PERMITS: usize = 64;
const MAX_ATTEMPTS: usize = 1024;
const MAX_OWNER_ATTEMPTS: usize = 128;
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Attempt {
    owner: String,
    issued_at: u64,
    policy: Policy,
    status: Status,
    salt: Option<String>,
    secret_hash: Option<String>,
}
impl std::fmt::Debug for Attempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attempt")
            .field("owner", &self.owner)
            .field("issued_at", &self.issued_at)
            .field("policy", &self.policy)
            .field("status", &self.status)
            .field("salt", &"[REDACTED]")
            .field("secret_hash", &"[REDACTED]")
            .finish()
    }
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
    let mut owners = BTreeMap::new();
    let mut issued = 0;
    for attempt in attempts {
        let status = &attempt.status;
        let check = (|| -> Result<()> {
            validate_token(&attempt.owner, "owner")?;
            validate_token(&status.creation_id, "creation_id")?;
            validate_token(&status.attempt_id, "attempt_id")?;
            attempt.policy.validate()?;
            let count = owners.entry(&attempt.owner).or_insert(0);
            *count += 1;
            if *count > MAX_OWNER_ATTEMPTS {
                return Err(unavailable("too many retained pairing attempts for owner"));
            }
            if status.deadline <= attempt.issued_at
                || status.deadline - attempt.issued_at > MAX_LIFETIME
            {
                return Err(unavailable("deadline must be within 600 seconds of issue time"));
            }
            let active = status.state == State::Issued;
            issued += usize::from(active);
            match (&status.state, status.terminal_at) {
                (State::Issued, None) => {}
                (State::Expired, Some(ended)) if ended == status.deadline => {}
                (State::Rejected, Some(ended))
                    if ended >= attempt.issued_at && ended < status.deadline => {}
                _ => return Err(unavailable("invalid terminal time for pairing state")),
            }
            if !creations.insert((&attempt.owner, &status.creation_id))
                || !ids.insert(&status.attempt_id)
                || status.policy_digest != digest(&attempt.policy)?
                || status.attempt_revision != if active { 1 } else { 2 }
                || active != attempt.salt.is_some()
                || active != attempt.secret_hash.is_some()
            {
                return Err(unavailable("invalid pairing attempt history"));
            }
            Ok(())
        })();
        check.with_context(|| format!("pairing attempt {:?}", status.attempt_id))?;
    }
    if issued > MAX_PERMITS {
        return Err(unavailable("too many outstanding pairing permits"));
    }
    Ok(())
}
fn digest(policy: &Policy) -> Result<String> {
    // Version the explicit wire fields, never the Rust struct's layout/order.
    let direction = match policy.direction {
        Direction::Normal => "normal",
        Direction::InboundOnly => "inbound_only",
        Direction::DialOnly => "dial_only",
    };
    let bytes = serde_json::to_vec(&(
        "fabric-pairing-policy-v1",
        &policy.grants,
        &policy.name,
        direction,
    ))?;
    Ok(format!("v1:{}", blake3::hash(&bytes).to_hex()))
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

pub(super) fn recover(stored: &mut Stored, timestamp: u64, restarted: bool) -> bool {
    // No durable wall-clock sample can prove elapsed downtime after rollback.
    // Outstanding credentials are therefore valid for this daemon lifetime only.
    let mut changed = false;
    for attempt in &mut stored.pairing {
        if attempt.status.state == State::Issued
            && (restarted || timestamp >= attempt.status.deadline)
        {
            attempt.terminate(State::Expired, attempt.status.deadline);
            changed = true;
        }
    }
    if changed {
        stored.pairing_last_seen = timestamp.max(stored.pairing_last_seen);
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
    fn pairing_timestamp(&mut self) -> u64 {
        observe_clock(&mut self.pairing_clock, now(), std::time::Instant::now())
    }

    /// Read-only projection: even logical expiry never writes durable state.
    pub(crate) fn pairing_query(&mut self, owner: &str, creation_id: &str) -> Result<Reply> {
        let timestamp = self.pairing_timestamp();
        self.pairing_query_at(owner, creation_id, timestamp)
    }

    fn pairing_query_at(&self, owner: &str, creation_id: &str, timestamp: u64) -> Result<Reply> {
        self.ensure_available()?;
        validate_token(owner, "owner")?;
        validate_token(creation_id, "creation_id")?;
        let attempt = self.stored.pairing.iter()
            .find(|attempt| attempt.owner == owner && attempt.status.creation_id == creation_id)
            .ok_or_else(|| invalid("unknown or expired creation_id"))?;
        let mut status = attempt.status.clone();
        if status.state == State::Issued && timestamp >= status.deadline {
            status.state = State::Expired;
            status.attempt_revision += 1;
            status.terminal_at = Some(status.deadline);
        }
        if status.terminal_at.is_some_and(|ended| timestamp >= ended.saturating_add(RECEIPT_SECONDS)) {
            return Err(invalid("unknown or expired creation_id"));
        }
        Ok(Reply { status: Some(status), secret: None, error: None })
    }

    pub(crate) fn pairing_request(&mut self, request: Request) -> Result<Reply> {
        if let Request::Query { owner, creation_id } = &request {
            return self.pairing_query(owner, creation_id);
        }
        let timestamp = self.pairing_timestamp();
        self.pairing_request_at(request, timestamp)
    }
    fn pairing_request_at(&mut self, request: Request, timestamp: u64) -> Result<Reply> {
        if let Request::Query { owner, creation_id } = &request {
            return self.pairing_query_at(owner, creation_id, timestamp);
        }
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
                        || next.pairing.iter().filter(|attempt| attempt.owner == owner).count()
                            >= MAX_OWNER_ATTEMPTS
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
                        issued_at: timestamp,
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
        if next.pairing != self.stored.pairing {
            next.pairing_last_seen = timestamp.max(next.pairing_last_seen);
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
        assert_eq!(cancelled.status.unwrap().state, State::Expired);
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
            State::Expired
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
        assert_eq!(reply.status.unwrap().state, State::Expired);
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
            terminal.owner = format!("owner-{}", i / MAX_OWNER_ATTEMPTS);
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
    fn queries_and_replays_never_persist_even_when_expired_or_sync_would_fail() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        store.pairing_request_at(issue("read", 700), 100).unwrap();
        let disk = fs::read(&store.path).unwrap();
        store.fail_directory_sync = true;
        for timestamp in [101, 690, 700, 701] {
            let reply = store.pairing_request_at(query("read"), timestamp).unwrap();
            assert_eq!(
                reply.status.unwrap().state,
                if timestamp < 700 { State::Issued } else { State::Expired }
            );
            assert_eq!(store.stored.pairing[0].status.state, State::Issued);
            assert_eq!(store.stored.pairing_last_seen, 100);
            assert_eq!(fs::read(&store.path).unwrap(), disk);
            assert!(store.fault().is_none());
        }
        store.pairing_request_at(issue("read", 700), 102).unwrap();
        assert_eq!(fs::read(&store.path).unwrap(), disk);
        assert!(store.pairing_request_at(query("unknown"), 700).is_err());
        assert!(store.pairing_request_at(query("read"), 700 + RECEIPT_SECONDS).is_err());
        assert_eq!(fs::read(&store.path).unwrap(), disk);
    }

    #[test]
    fn writer_persists_expiry_but_terminal_replays_do_not_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        store.pairing_request_at(issue("old", 700), 100).unwrap();
        store.pairing_request_at(issue("new", 1300), 700).unwrap();
        assert_eq!(store.stored.pairing[0].status.state, State::Expired);
        assert_eq!(store.stored.pairing_last_seen, 700);
        let disk = fs::read(&store.path).unwrap();
        store.fail_directory_sync = true;
        store.pairing_request_at(Request::Cancel {
            owner: "fleet".into(), creation_id: "old".into(),
        }, 701).unwrap();
        assert_eq!(store.stored.pairing_last_seen, 700);
        assert_eq!(fs::read(&store.path).unwrap(), disk);
    }

    #[test]
    fn restart_after_late_crash_and_small_clock_rollback_expires_outstanding_permit() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t0 = 100;
        store.pairing_request_at(issue("rollback-gap", t0 + 600), t0).unwrap();
        store.pairing_request_at(query("rollback-gap"), t0 + 590).unwrap();
        assert_eq!(store.stored.pairing_last_seen, t0);
        let mut recovered: Stored = toml::from_str(&fs::read_to_string(&store.path).unwrap()).unwrap();
        assert!(!recover(&mut recovered, t0 + 5, false));
        assert_eq!(recovered.pairing[0].status.state, State::Issued);
        assert!(recover(&mut recovered, t0 + 5, true));
        assert_eq!(recovered.pairing[0].status.state, State::Expired);
        assert!(recovered.pairing[0].salt.is_none());
        assert!(recovered.pairing[0].secret_hash.is_none());
        validate(&recovered.pairing).unwrap();
        assert!(!recover(&mut recovered, t0 + 6, true));
    }

    #[test]
    fn owner_retention_cap_leaves_capacity_for_other_owners_and_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        store.pairing_request_at(issue("active", 700), 100).unwrap();
        let mut terminal = store.stored.pairing[0].clone();
        terminal.terminate(State::Rejected, 100);
        for i in 1..MAX_OWNER_ATTEMPTS {
            terminal.status.creation_id = format!("retained-{i}");
            terminal.status.attempt_id = format!("attempt-{i}");
            store.stored.pairing.push(terminal.clone());
        }
        validate(&store.stored.pairing).unwrap();
        assert!(store.pairing_request_at(issue("blocked", 700), 100).is_err());
        let Request::Issue { creation_id, deadline, policy, .. } = issue("other", 700) else {
            unreachable!()
        };
        store.pairing_request_at(Request::Issue {
            owner: "other".into(), creation_id, deadline, policy,
        }, 100).unwrap();
        store.pairing_request_at(Request::Cancel {
            owner: "fleet".into(), creation_id: "active".into(),
        }, 101).unwrap();
        assert_eq!(store.stored.pairing[0].status.state, State::Rejected);
        terminal.status.creation_id = "over-cap".into();
        terminal.status.attempt_id = "over-cap".into();
        store.stored.pairing.push(terminal);
        assert!(format!("{:#}", validate(&store.stored.pairing).unwrap_err())
            .contains("too many retained pairing attempts for owner"));
    }

    #[test]
    fn validation_identifies_bad_deadline_and_terminal_time_without_leaking_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        store.pairing_request_at(issue("validate", 700), 100).unwrap();
        let attempt = store.stored.pairing[0].clone();
        let debug = format!("{attempt:?}");
        assert!(!debug.contains(attempt.salt.as_ref().unwrap()));
        assert!(!debug.contains(attempt.secret_hash.as_ref().unwrap()));
        validate(std::slice::from_ref(&attempt)).unwrap();
        for deadline in [0, 100, 701] {
            let mut invalid = attempt.clone();
            invalid.status.deadline = deadline;
            let error = format!("{:#}", validate(&[invalid]).unwrap_err());
            assert!(error.contains(&attempt.status.attempt_id));
            assert!(error.contains("deadline"));
        }
        for (state, ended) in [
            (State::Issued, Some(100)),
            (State::Expired, Some(699)),
            (State::Rejected, Some(99)),
            (State::Rejected, Some(700)),
            (State::Rejected, None),
        ] {
            let mut invalid = attempt.clone();
            invalid.status.state = state;
            invalid.status.terminal_at = ended;
            let error = format!("{:#}", validate(&[invalid]).unwrap_err());
            assert!(error.contains(&attempt.status.attempt_id));
            assert!(error.contains("terminal time"));
        }
    }

    #[test]
    fn policy_digest_uses_versioned_explicit_fields() {
        let policy = Policy {
            grants: vec!["st3-client-v0".into()], name: None, direction: Direction::Normal,
        };
        let bytes = br#"["fabric-pairing-policy-v1",["st3-client-v0"],null,"normal"]"#;
        assert_eq!(digest(&policy).unwrap(), format!("v1:{}", blake3::hash(bytes).to_hex()));
        for direction in [Direction::InboundOnly, Direction::DialOnly] {
            let changed = Policy { direction, ..policy.clone() };
            assert_ne!(digest(&policy).unwrap(), digest(&changed).unwrap());
        }
        let changed = Policy { name: Some("label".into()), ..policy.clone() };
        assert_ne!(digest(&policy).unwrap(), digest(&changed).unwrap());
        let changed = Policy { grants: vec![], ..policy.clone() };
        assert_ne!(digest(&policy).unwrap(), digest(&changed).unwrap());
    }

    #[test]
    fn unused_pairing_fields_are_omitted_for_rollback_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let disk = fs::read_to_string(&store.path).unwrap();
        assert!(!disk.contains("pairing"));
        store.pairing_request_at(issue("used", 700), 100).unwrap();
        let disk = fs::read_to_string(&store.path).unwrap();
        assert!(disk.contains("[[pairing]]"));
        assert!(disk.contains("pairing_last_seen = 100"));
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
