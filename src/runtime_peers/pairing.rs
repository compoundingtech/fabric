//! Restricted pairing state: neither issuing nor presenting a permit grants access.
use super::*;

pub const PAIR_ALPN: &[u8] = b"fabric/pair/1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum NetworkRequest {
    Present { attempt_id: String, secret: Secret },
    Status { attempt_id: String },
}

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
    fn project_status(&self, timestamp: u64) -> Status {
        let mut status = self.status.clone();
        if matches!(status.state, State::Issued | State::Pending)
            && timestamp >= status.deadline
        {
            status.state = State::Expired;
            status.attempt_revision += 1;
            status.terminal_at = Some(status.deadline);
            status.terminal_reason = Some("expired".into());
        }
        status
    }

    fn expire(&mut self, timestamp: u64) {
        if matches!(self.status.state, State::Issued | State::Pending)
            && timestamp >= self.status.deadline
        {
            self.terminate(State::Expired, self.status.deadline);
        }
    }
    fn terminate(&mut self, state: State, timestamp: u64) {
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
            let active = matches!(status.state, State::Issued | State::Pending);
            let bound = status.bound_node_id.is_some();
            let revision = match status.state {
                State::Issued if !bound => 1,
                State::Pending if bound => 2,
                State::Expired | State::Rejected => if bound { 3 } else { 2 },
                _ => return Err(unavailable("invalid pairing attempt binding")),
            };
            issued += usize::from(active);
            match (&status.state, status.terminal_at) {
                (State::Issued | State::Pending, None) => {}
                (State::Expired, Some(ended)) if ended == status.deadline => {}
                (State::Rejected, Some(ended))
                    if ended >= attempt.issued_at && ended < status.deadline => {}
                _ => return Err(unavailable("invalid terminal time for pairing state")),
            }
            if !creations.insert((&attempt.owner, &status.creation_id))
                || !ids.insert(&status.attempt_id)
                || status.policy_digest != digest(&attempt.policy)?
                || status.attempt_revision != revision
                || active != attempt.salt.is_some()
                || active != attempt.secret_hash.is_some()
                || attempt.salt.as_ref().is_some_and(|salt| blake3::Hash::from_hex(salt).is_err())
                || attempt.secret_hash.as_ref().is_some_and(|hash| blake3::Hash::from_hex(hash).is_err())
                || (active && status.terminal_reason.is_some())
                || (!active && bound && status.terminal_reason.is_none())
                || status.terminal_reason.as_ref().is_some_and(|reason| {
                    !matches!(
                        (&status.state, reason.as_str()),
                        (State::Expired, "expired")
                            | (State::Rejected, "cancelled")
                            | (State::Rejected, "exchange_lost")
                    ) || (reason == "exchange_lost" && !bound)
                })
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
fn secret_hash(salt: &str, secret: &str) -> blake3::Hash {
    let mut hash = blake3::Hasher::new();
    hash.update(salt.as_bytes());
    hash.update(secret.as_bytes());
    hash.finalize()
}

pub(super) fn recover(stored: &mut Stored, timestamp: u64, restarted: bool) -> bool {
    // Credentials cannot survive a restart: wall time cannot prove elapsed downtime.
    let backwards = timestamp < stored.pairing_last_seen;
    let mut changed = false;
    for attempt in &mut stored.pairing {
        if restarted && attempt.status.state == State::Pending
            && !backwards && timestamp < attempt.status.deadline
        {
            attempt.terminate(State::Rejected, timestamp);
            attempt.status.terminal_reason = Some("exchange_lost".into());
            changed = true;
        } else if matches!(attempt.status.state, State::Issued | State::Pending)
            && (restarted || backwards || timestamp >= attempt.status.deadline)
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

    /// Validate without persistence or the policy writer barrier. `None` means a
    /// real bind is needed; the writer must revalidate after acquiring its locks.
    pub(crate) fn pairing_network_read(
        &mut self,
        node: EndpointId,
        request: &NetworkRequest,
    ) -> Result<Option<Reply>> {
        let timestamp = self.pairing_timestamp();
        self.pairing_network_read_at(node, request, timestamp)
    }

    fn pairing_network_read_at(
        &self,
        node: EndpointId,
        request: &NetworkRequest,
        timestamp: u64,
    ) -> Result<Option<Reply>> {
        self.ensure_available()?;
        let refused = || invalid("pairing request refused");
        let (attempt_id, secret) = match request {
            NetworkRequest::Present { attempt_id, secret } => (attempt_id, Some(secret)),
            NetworkRequest::Status { attempt_id } => (attempt_id, None),
        };
        let attempt = self.stored.pairing.iter()
            .find(|attempt| &attempt.status.attempt_id == attempt_id)
            .ok_or_else(refused)?;
        let status = &attempt.status;
        let active = matches!(status.state, State::Issued | State::Pending);
        let expired = active && timestamp >= status.deadline;
        let terminal_at = if expired { Some(status.deadline) } else { status.terminal_at };
        if terminal_at.is_some_and(|ended| timestamp >= ended.saturating_add(RECEIPT_SECONDS))
            || status.bound_node_id.is_some_and(|bound| bound != node)
        {
            return Err(refused());
        }
        if let Some(secret) = secret {
            if !active || expired {
                return Err(refused());
            }
            let (Some(salt), Some(expected)) = (&attempt.salt, &attempt.secret_hash) else {
                return Err(refused());
            };
            // Hash equality is constant-time, unlike serialized hex equality.
            if blake3::Hash::from_hex(expected).map_err(|_| refused())?
                != secret_hash(salt, &secret.0)
            {
                return Err(refused());
            }
            if status.state == State::Issued {
                return Ok(None);
            }
        } else if status.bound_node_id != Some(node) {
            return Err(refused());
        }
        Ok(Some(Reply {
            status: Some(attempt.project_status(timestamp)),
            secret: None,
            error: None,
        }))
    }

    /// Restricted to the authenticated transport identity; never returns a secret.
    pub fn pairing_network_request(
        &mut self,
        node: EndpointId,
        request: NetworkRequest,
    ) -> Result<Reply> {
        let timestamp = self.pairing_timestamp();
        self.pairing_network_request_at(node, request, timestamp)
    }

    fn pairing_network_request_at(
        &mut self,
        node: EndpointId,
        request: NetworkRequest,
        timestamp: u64,
    ) -> Result<Reply> {
        if let Some(reply) = self.pairing_network_read_at(node, &request, timestamp)? {
            return Ok(reply);
        }
        let NetworkRequest::Present { attempt_id, .. } = request else {
            unreachable!("only an authenticated presentation needs a bind");
        };
        let mut next = self.stored.clone();
        let attempt = next.pairing.iter_mut()
            .find(|attempt| attempt.status.attempt_id == attempt_id)
            .expect("prevalidated attempt");
        attempt.status.state = State::Pending;
        attempt.status.bound_node_id = Some(node);
        attempt.status.attempt_revision = 2;
        let status = attempt.status.clone();
        self.commit_pairing(next, timestamp)?;
        Ok(Reply { status: Some(status), secret: None, error: None })
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
        let status = attempt.project_status(timestamp);
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
                if matches!(
                    next.pairing[index].status.state,
                    State::Issued | State::Pending
                ) {
                    next.pairing[index].terminate(State::Rejected, timestamp);
                }
                index
            }
        };
        let status = next.pairing[index].status.clone();
        self.commit_pairing(next, timestamp)?;
        Ok(Reply {
            status: Some(status),
            secret,
            error: None,
        })
    }

    fn commit_pairing(&mut self, mut next: Stored, timestamp: u64) -> Result<()> {
        // Same sequencer as runtime grants; reads never persist clock observations.
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
            assert_eq!(store.stored.pairing[0].secret_hash.is_none(), cancel);
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
            if cancel {
                assert_eq!(open(&dir).stored.pairing[0].status, terminal);
            } else {
                // A read projects expiry; a later writer makes it durable.
                store.pairing_request_at(issue("expiry-writer", timestamp + 600), timestamp).unwrap();
                assert_eq!(store.stored.pairing[0].status, terminal);
                assert!(store.stored.pairing[0].secret_hash.is_none());
            }
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
        assert_eq!(expired.status.state, State::Issued);
        assert_eq!(expired.status.attempt_revision, 1);
        assert!(expired.status.bound_node_id.is_none());
        assert!(expired.secret_hash.is_some());
        assert_refused(store.pairing_network_request_at(
            node(1),
            restricted_status(&status),
            t + 600,
        ));
        assert!(store.snapshot(None).unwrap().peers.is_empty());
    }

    #[test]
    fn network_validation_reads_and_replays_never_persist() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let issued = store.pairing_request_at(issue("read-network", 700), 100).unwrap();
        let status = issued.status.unwrap();
        let request = presentation(&status, &issued.secret.unwrap());
        let disk = fs::read(&store.path).unwrap();
        store.fail_directory_sync = true;
        assert!(store.pairing_network_read_at(node(1), &request, 101).unwrap().is_none());
        for request in [
            NetworkRequest::Status { attempt_id: "unknown".into() },
            NetworkRequest::Present { attempt_id: "unknown".into(), secret: Secret::new("wrong") },
            restricted_status(&status),
            presentation(&status, &Secret::new("wrong")),
        ] {
            assert_refused(store.pairing_network_request_at(node(1), request, 101));
        }
        assert_refused(store.pairing_network_request_at(node(1), request.clone(), 700));
        assert_eq!(fs::read(&store.path).unwrap(), disk);
        assert_eq!(store.stored.pairing_last_seen, 100);
        assert!(store.fault().is_none());

        store.fail_directory_sync = false;
        store.pairing_network_request_at(node(1), request.clone(), 101).unwrap();
        let disk = fs::read(&store.path).unwrap();
        store.fail_directory_sync = true;
        for timestamp in [102, 699] {
            assert!(store.pairing_network_read_at(node(1), &request, timestamp).unwrap().is_some());
            assert_eq!(store.pairing_network_request_at(node(1), request.clone(), timestamp)
                .unwrap().status.unwrap().state, State::Pending);
            assert_refused(store.pairing_network_request_at(node(2), request.clone(), timestamp));
            assert_refused(store.pairing_network_request_at(node(2), restricted_status(&status), timestamp));
        }
        for timestamp in [102, 700, 701] {
            let reply = store.pairing_network_request_at(node(1), restricted_status(&status), timestamp).unwrap();
            assert_eq!(reply.status.unwrap().state, if timestamp < 700 { State::Pending } else { State::Expired });
        }
        assert_refused(store.pairing_network_request_at(node(1), request, 700));
        assert_eq!(fs::read(&store.path).unwrap(), disk);
        assert_eq!(store.stored.pairing_last_seen, 101);
        assert_eq!(store.stored.pairing[0].status.state, State::Pending);
        assert!(store.fault().is_none());
    }

    #[test]
    fn prevalidated_binding_is_revalidated_after_another_writer() {
        for cancel in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let issued = store.pairing_request_at(issue("race", 700), 100).unwrap();
            let request = presentation(&issued.status.unwrap(), &issued.secret.unwrap());
            assert!(store.pairing_network_read_at(node(1), &request, 101).unwrap().is_none());
            if cancel {
                store.pairing_request_at(Request::Cancel {
                    owner: "fleet".into(), creation_id: "race".into(),
                }, 102).unwrap();
            } else {
                store.pairing_network_request_at(node(2), request.clone(), 102).unwrap();
            }
            let disk = fs::read(&store.path).unwrap();
            store.fail_directory_sync = true;
            assert_refused(store.pairing_network_request_at(node(1), request, 103));
            assert_eq!(fs::read(&store.path).unwrap(), disk);
            assert!(store.fault().is_none());
        }
    }
}
