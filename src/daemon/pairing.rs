//! Restricted authenticated pairing. No mux, exposure, or ordinary service dispatch.
use super::*;
use crate::runtime_peers::pairing::{NetworkRequest, Reply, State};
use parking_lot::Mutex as StdMutex;
use std::collections::{BTreeSet, HashMap};
use tokio::sync::Semaphore;

pub(super) const MAX_CONNECTIONS: usize = 32;
const MAX_CONNECTIONS_PER_NODE: usize = 2;
const MAX_PREAUTH_REQUEST: usize = 4096;
// JSON byte arrays require at most four encoded bytes per payload byte.
const MAX_REQUEST: usize = 4 * 64 * 1024 + 4096;
const BOUND_BURST: u32 = 8;
const BOUND_REFILL_SECONDS: u64 = 1;
const MAX_RATE_NODES: usize = 1024;
const IDLE: Duration = Duration::from_secs(10);
const FIRST_HEADER: Duration = Duration::from_secs(3);
const RECEIVE_WINDOW: u32 = 8192;
const BOUND_HINT_TTL: Duration = Duration::from_secs(600);

pub(super) fn restrict_transport<T: iroh::endpoint::ConnectionState>(connection: &Connection<T>) {
    // QUIC cannot retract credits already advertised before ALPN selection.
    // These setters restrict replenishment at the earliest public API point.
    connection.set_max_concurrent_bi_streams(VarInt::from_u32(1));
    connection.set_max_concurrent_uni_streams(VarInt::from_u32(0));
    connection.set_receive_window(VarInt::from_u32(RECEIVE_WINDOW));
}

#[derive(Debug)]
struct Bucket {
    tokens: u32,
    last: Instant,
}
impl Bucket {
    fn refill(&mut self, now: Instant, capacity: u32, period: u64) {
        let added = now.duration_since(self.last).as_secs() / period;
        self.tokens = self
            .tokens
            .saturating_add(added.min(u64::from(capacity)) as u32)
            .min(capacity);
        self.last += Duration::from_secs(added * period);
    }
}
#[derive(Debug)]
struct BoundHint {
    bucket: Bucket,
    expires: Instant,
}
#[derive(Debug)]
struct Rates {
    global: Bucket,
    nodes: HashMap<EndpointId, Bucket>,
    bound: HashMap<EndpointId, BoundHint>,
}
#[derive(Debug)]
pub(super) struct Gate {
    connections: Arc<Semaphore>,
    nodes: Arc<StdMutex<HashMap<EndpointId, usize>>>,
    attempts: StdMutex<BTreeSet<String>>,
    rates: StdMutex<Rates>,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            nodes: Arc::new(StdMutex::new(HashMap::new())),
            attempts: StdMutex::new(BTreeSet::new()),
            rates: StdMutex::new(Rates {
                global: Bucket {
                    tokens: 10,
                    last: Instant::now(),
                },
                nodes: HashMap::new(),
                bound: HashMap::new(),
            }),
        }
    }
}
struct ConnectionLease {
    nodes: Arc<StdMutex<HashMap<EndpointId, usize>>>,
    node: EndpointId,
}
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        let mut nodes = self.nodes.lock();
        if let Some(count) = nodes.get_mut(&self.node) {
            *count -= 1;
            if *count == 0 {
                nodes.remove(&self.node);
            }
        }
    }
}
struct AttemptLease<'a> {
    gate: &'a Gate,
    attempt: String,
}
impl Drop for AttemptLease<'_> {
    fn drop(&mut self) {
        self.gate.attempts.lock().remove(&self.attempt);
    }
}
impl Gate {
    fn connection_lease(&self, node: EndpointId) -> Option<ConnectionLease> {
        let mut nodes = self.nodes.lock();
        let count = nodes.entry(node).or_default();
        if *count >= MAX_CONNECTIONS_PER_NODE {
            return None;
        }
        *count += 1;
        Some(ConnectionLease {
            nodes: self.nodes.clone(),
            node,
        })
    }
    pub(super) fn admit(&self, connection: &Connection) -> bool {
        let Ok(slot) = self.connections.clone().try_acquire_owned() else {
            return false;
        };
        let Some(lease) = self.connection_lease(connection.remote_id()) else {
            return false;
        };
        let connection = connection.clone();
        tokio::spawn(async move {
            let _slot = slot;
            let _lease = lease;
            if tokio::time::timeout(Duration::from_secs(600), connection.closed())
                .await
                .is_err()
            {
                connection.close(VarInt::from_u32(0), b"pairing connection expired");
            }
        });
        true
    }
    fn rate(&self, node: EndpointId, now: Instant) -> bool {
        let mut rates = self.rates.lock();
        rates.global.refill(now, 10, 1);
        if rates.global.tokens == 0 {
            return false;
        }
        if !rates.nodes.contains_key(&node) && rates.nodes.len() >= MAX_RATE_NODES {
            rates
                .nodes
                .retain(|_, bucket| now.duration_since(bucket.last) < Duration::from_secs(60));
            if rates.nodes.len() >= MAX_RATE_NODES {
                return false;
            }
        }
        let bucket = rates.nodes.entry(node).or_insert(Bucket {
            tokens: 3,
            last: now,
        });
        bucket.refill(now, 3, 6);
        if bucket.tokens == 0 {
            return false;
        }
        bucket.tokens -= 1;
        rates.global.tokens -= 1;
        true
    }
    fn refund(&self) {
        let mut rates = self.rates.lock();
        rates.global.tokens = rates.global.tokens.saturating_add(1).min(10);
    }

    /// Only a scheduling hint, never proof of authorization. Every admitted
    /// request still undergoes the runtime's credential and identity checks.
    fn reserve_request(&self, node: EndpointId, now: Instant) -> Option<bool> {
        let mut rates = self.rates.lock();
        if rates.bound.get(&node).is_some_and(|hint| hint.expires <= now) {
            rates.bound.remove(&node);
        }
        if let Some(hint) = rates.bound.get_mut(&node) {
            hint.bucket.refill(now, BOUND_BURST, BOUND_REFILL_SECONDS);
            if hint.bucket.tokens == 0 {
                return None;
            }
            hint.bucket.tokens -= 1;
            return Some(false);
        }
        drop(rates);
        self.rate(node, now).then_some(true)
    }

    fn remember_bound(&self, node: EndpointId, now: Instant) {
        let mut rates = self.rates.lock();
        if let Some(hint) = rates.bound.get_mut(&node) {
            hint.expires = now + BOUND_HINT_TTL;
            return;
        }
        rates.bound.retain(|_, hint| hint.expires > now);
        if rates.bound.len() < MAX_RATE_NODES {
            rates.bound.insert(node, BoundHint {
                bucket: Bucket { tokens: BOUND_BURST, last: now },
                expires: now + BOUND_HINT_TTL,
            });
        }
    }

    #[cfg(test)]
    pub(super) fn reserve_connection_capacity_for_test(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.connections.clone().try_acquire_many_owned(MAX_CONNECTIONS as u32).unwrap()
    }
    fn lease(&self, attempt: &str) -> Option<AttemptLease<'_>> {
        if attempt.is_empty() || attempt.len() > 128 {
            return None;
        }
        let mut attempts = self.attempts.lock();
        if attempts.len() >= MAX_CONNECTIONS || !attempts.insert(attempt.to_owned()) {
            return None;
        }
        Some(AttemptLease {
            gate: self,
            attempt: attempt.to_owned(),
        })
    }
}

/// Decode the flat wire object directly. Deserializing the internally tagged
/// public enum would first buffer every payload byte as serde enum content.
fn decode_request(bytes: &[u8]) -> Result<NetworkRequest> {
    use crate::runtime_peers::pairing::{Payload, Secret};
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Operation {
        Present,
        Status,
        Send,
        Receive,
    }
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Frame {
        operation: Operation,
        attempt_id: String,
        secret: Option<Secret>,
        sequence: Option<u64>,
        digest: Option<String>,
        payload: Option<Payload>,
        ack: Option<u64>,
    }
    let frame: Frame = serde_json::from_slice(bytes).map_err(|_| refused())?;
    let Frame { operation, attempt_id, secret, sequence, digest, payload, ack } = frame;
    match operation {
        Operation::Present
            if sequence.is_none() && digest.is_none() && payload.is_none() && ack.is_none() =>
        {
            Ok(NetworkRequest::Present { attempt_id, secret: secret.ok_or_else(refused)? })
        }
        Operation::Status
            if secret.is_none() && sequence.is_none() && digest.is_none()
                && payload.is_none() && ack.is_none() =>
        {
            Ok(NetworkRequest::Status { attempt_id })
        }
        Operation::Send if secret.is_none() && ack.is_none() => Ok(NetworkRequest::Send {
            attempt_id,
            sequence: sequence.ok_or_else(refused)?,
            digest: digest.ok_or_else(refused)?,
            payload: payload.ok_or_else(refused)?,
        }),
        Operation::Receive
            if secret.is_none() && sequence.is_none() && digest.is_none() && payload.is_none() =>
        {
            Ok(NetworkRequest::Receive { attempt_id, ack })
        }
        _ => Err(refused()),
    }
}

async fn reject_frame(state: &Arc<DaemonState>, node: EndpointId, attempt: &str) {
    let state = state.clone();
    let attempt = attempt.to_owned();
    // A socket close or timeout cannot abandon durable rejection/quarantine.
    let rejection = tokio::spawn(async move {
        state.reject_pairing_frame(node, attempt).await
    });
    let _ = tokio::time::timeout(FIRST_HEADER, rejection).await;
}

fn refused() -> anyhow::Error {
    FabricError::new(ErrorCode::InvalidArgument, "pairing request refused").into()
}

pub(super) async fn serve(connection: Connection, state: Arc<DaemonState>) -> Result<()> {
    // Socket closure must not cancel a durable commit's quarantine/publication.
    // Framing waits and the connection itself are bounded separately. The handler
    // retains only its dedicated pairing-slot permit until this operation ends.
    let result = serve_requests(&connection, &state).await;
    // Never copy tokens, input JSON, or attempt IDs into close reasons or logs.
    connection.close(VarInt::from_u32(0), b"pairing connection ended");
    result
}
pub(super) async fn expire_loop(state: Arc<DaemonState>) {
    if state.runtime_peers.is_none() {
        state.cancel.cancelled().await;
        return;
    }
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticks.tick().await;
    let mut fault_reported = false;
    loop {
        tokio::select! {
            _ = state.cancel.cancelled() => return,
            _ = ticks.tick() => {}
        }
        // No clock observation or expiry worker is needed for retained receipts.
        let due = async {
            if !state.has_active_pairing_attempts().await? {
                return Ok::<_, anyhow::Error>(false);
            }
            state.pairing_expiry_due().await
        }
        .await;
        match due {
            Ok(false) => continue,
            Ok(true) => {}
            Err(_) => {
                if !fault_reported && !state.cancel.is_cancelled() {
                    warn!("pairing expiry inspection failed");
                }
                fault_reported = true;
                continue;
            }
        }
        // Once started, publication/quarantine completes even during shutdown.
        let reply = state
            .commit_pairing(|runtime| {
                runtime.pairing_expire()?;
                Ok(Reply {
                    status: None,
                    secret: None,
                    exchange: None,
                    error: None,
                })
            })
            .await;
        if let Some(error) = reply.error {
            if !fault_reported && !state.cancel.is_cancelled() {
                warn!(code = %error.code, "pairing expiry reconciliation failed");
            }
            fault_reported = true;
        } else {
            fault_reported = false;
        }
    }
}

async fn serve_requests(connection: &Connection, state: &Arc<DaemonState>) -> Result<()> {
    let first_header_deadline = tokio::time::Instant::now() + FIRST_HEADER;
    let (mut send, mut recv) =
        tokio::time::timeout_at(first_header_deadline, connection.accept_bi()).await??;
    let mut lease: Option<AttemptLease<'_>> = None;
    let mut deadline = first_header_deadline;
    loop {
        let mut length = [0u8; 4];
        tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + IDLE),
            recv.read_exact(&mut length),
        )
        .await??;
        // Reconnect authenticates a small frame before using the larger cap.
        let bound = lease.is_some();
        let length = u32::from_be_bytes(length) as usize;
        let cap = if bound { MAX_REQUEST } else { MAX_PREAUTH_REQUEST };
        if length == 0 || length > cap {
            if let Some(lease) = &lease {
                reject_frame(state, connection.remote_id(), &lease.attempt).await;
            }
            return Err(refused());
        }
        // Unknown identities need global credit before parsing or runtime work;
        // known bound identities reserve their independent token first.
        let charged = state.pairing_gate
            .reserve_request(connection.remote_id(), Instant::now())
            .ok_or_else(refused)?;
        let mut bytes = vec![0u8; length];
        tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + IDLE),
            recv.read_exact(&mut bytes[..length]),
        )
        .await??;
        let request = decode_request(&bytes)?;
        drop(bytes);
        if matches!(&request, NetworkRequest::Present { .. } | NetworkRequest::Status { .. })
            && length > MAX_PREAUTH_REQUEST
        {
            if let Some(lease) = &lease {
                reject_frame(state, connection.remote_id(), &lease.attempt).await;
            }
            return Err(refused());
        }
        let validated = state.pairing_network_read(connection.remote_id(), &request).await?;
        if charged {
            state.pairing_gate.refund();

        }
        let attempt = match &request {
            NetworkRequest::Present { attempt_id, .. }
            | NetworkRequest::Status { attempt_id }
            | NetworkRequest::Send { attempt_id, .. }
            | NetworkRequest::Receive { attempt_id, .. } => attempt_id,
        };
        if let Some(lease) = &lease {
            if &lease.attempt != attempt {
                return Err(refused());
            }
        } else {
            lease = Some(state.pairing_gate.lease(attempt).ok_or_else(refused)?);
        }
        let exchange = matches!(&request, NetworkRequest::Send { .. } | NetworkRequest::Receive { .. });
        let reply = if !exchange && validated.is_some() {
            validated.expect("checked read-only reply")
        } else {
            // A timeout or disconnect must not cancel durable publication and
            // quarantine after its off-thread worker has persisted a bind.
            let state = state.clone();
            let node = connection.remote_id();
            let commit = tokio::spawn(async move {
                state.pairing_network_request(node, request).await
            });
            tokio::time::timeout(FIRST_HEADER, commit).await??
        };
        // The restricted path never reports fault internals or any unbound attempt.
        if reply.error.is_some() {
            return Err(refused());
        }
        let status = reply.status.as_ref().ok_or_else(refused)?;
        if status.bound_node_id == Some(connection.remote_id()) {
            state.pairing_gate.remember_bound(connection.remote_id(), Instant::now());
        }
        let terminal = !matches!(status.state, State::Pending);
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        deadline = tokio::time::Instant::now()
            + Duration::from_secs(status.deadline.saturating_sub(wall).min(600));
        let bytes = serde_json::to_vec(&reply)?;
        tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + IDLE),
            async {
                send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
                send.write_all(&bytes).await
            },
        )
        .await??;
        if terminal {
            send.finish()?;
            // Keep the final status stream alive until acknowledged, not until a close races it.
            let _ = tokio::time::timeout(IDLE, send.stopped()).await;
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }
    #[test]
    fn pairing_limits_connections_attempts_and_rotating_id_floods() {
        let gate = Gate::default();
        let permits: Vec<_> = (0..MAX_CONNECTIONS)
            .map(|_| gate.connections.try_acquire().unwrap())
            .collect();
        assert!(gate.connections.try_acquire().is_err());
        drop(permits);
        assert!(gate.connections.try_acquire().is_ok());
        let first = gate.connection_lease(node(1)).unwrap();
        let second = gate.connection_lease(node(1)).unwrap();
        assert!(gate.connection_lease(node(1)).is_none());
        assert!(gate.connection_lease(node(2)).is_some());
        drop(first);
        assert!(gate.connection_lease(node(1)).is_some());
        drop(second);
        assert!(gate.nodes.lock().is_empty());
        let lease = gate.lease("one").unwrap();
        assert!(gate.lease("one").is_none());
        drop(lease);
        assert!(gate.lease("one").is_some());
        let now = Instant::now();
        for i in 0..10 {
            assert!(gate.rate(node(i), now));
        }
        assert!(!gate.rate(node(99), now));
        assert!(gate.rate(node(99), now + Duration::from_secs(1)));
    }
    #[test]
    fn per_node_limit_and_rate_book_are_bounded() {
        let gate = Gate::default();
        let now = Instant::now();
        for _ in 0..3 {
            assert!(gate.rate(node(1), now));
        }
        assert!(!gate.rate(node(1), now));
        assert!(gate.rate(node(1), now + Duration::from_secs(6)));
        let mut rates = gate.rates.lock();
        rates.nodes.clear();
        for i in 0..MAX_RATE_NODES {
            let mut seed = [0u8; 32];
            seed[..8].copy_from_slice(&(i as u64).to_be_bytes());
            rates.nodes.insert(
                iroh::SecretKey::from_bytes(&seed).public(),
                Bucket {
                    tokens: 3,
                    last: now,
                },
            );
        }
        drop(rates);
        assert!(!gate.rate(node(200), now + Duration::from_secs(7)));
        assert!(gate.rates.lock().nodes.len() <= MAX_RATE_NODES);
    }
    #[test]
    fn validated_requests_refund_global_tokens_and_bound_traffic_has_its_own_budget() {
        let gate = Gate::default();
        let now = Instant::now();
        assert!(gate.rate(node(1), now));
        gate.refund();
        gate.remember_bound(node(1), now);
        gate.remember_bound(node(2), now);
        assert_eq!(gate.rates.lock().global.tokens, 10);
        for seed in 10..20 {
            assert!(gate.rate(node(seed), now));
        }
        assert_eq!(gate.reserve_request(node(99), now), None);
        for seed in 100..200 {
            assert_eq!(gate.reserve_request(node(seed), now), None);
        }
        assert_eq!(gate.rates.lock().bound.len(), 2);
        for _ in 0..BOUND_BURST {
            assert_eq!(gate.reserve_request(node(1), now), Some(false));
        }
        assert_eq!(gate.reserve_request(node(1), now), None);
        assert_eq!(gate.reserve_request(node(2), now), Some(false));
        assert_eq!(gate.reserve_request(node(1), now + Duration::from_secs(BOUND_REFILL_SECONDS)), Some(false));
        assert_eq!(gate.rates.lock().global.tokens, 0);
        for _ in 0..20 {
            gate.refund();
        }
        assert_eq!(gate.rates.lock().global.tokens, 10);
    }
    #[test]
    fn bound_identity_hints_expire_and_have_a_fixed_capacity() {
        let gate = Gate::default();
        let now = Instant::now();
        for index in 0..=MAX_RATE_NODES {
            let mut seed = [0; 32];
            seed[..8].copy_from_slice(&(index as u64).to_be_bytes());
            gate.remember_bound(iroh::SecretKey::from_bytes(&seed).public(), now);
        }
        assert_eq!(gate.rates.lock().bound.len(), MAX_RATE_NODES);
        // Expired hints no longer grant the independent lane; after the global
        // refill this reservation must charge the global bucket instead.
        assert_eq!(gate.reserve_request(
            iroh::SecretKey::from_bytes(&[0; 32]).public(), now + BOUND_HINT_TTL,
        ), Some(true));
        gate.remember_bound(node(250), now + BOUND_HINT_TTL);
        assert_eq!(gate.rates.lock().bound.len(), 1);
    }

    #[tokio::test]
    async fn exhausted_global_lane_refuses_unknown_nodes_without_waiting_for_runtime() -> Result<()> {
        use crate::runtime_peers::pairing::PAIR_ALPN;
        let dir = tempfile::tempdir()?;
        let gateway = gateway(FabricHome::new(dir.path())).await?;
        let state = gateway.state();
        let runtime = state.runtime_peers.as_ref().unwrap().clone();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _runtime = runtime.lock().unwrap_or_else(|poison| poison.into_inner());
            let _ = entered.send(());
            let _ = blocked.recv();
        });
        ready.await?;
        let client = client().await?;
        let connection = client.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        let now = Instant::now();
        for seed in 10..20 {
            assert!(state.pairing_gate.rate(node(seed), now));
        }
        // Start a full refill interval at the barrier, independent of how long
        // endpoint startup took on the runner.
        state.pairing_gate.rates.lock().global.last = now;
        let response = rpc(&mut send, &mut recv, NetworkRequest::Status {
            attempt_id: "unknown".into(),
        });
        let refused = tokio::time::timeout(Duration::from_millis(500), response).await;
        release.send(())?;
        blocker.await?;
        assert!(matches!(refused, Ok(Err(_))),
            "unknown traffic reached runtime validation despite exhausted admission");
        client.close().await;
        gateway.shutdown().await?;
        Ok(())
    }


    #[tokio::test]
    async fn pairing_closes_connections_without_a_complete_first_header_in_three_seconds()
    -> Result<()> {
        use crate::runtime_peers::pairing::PAIR_ALPN;
        let dir = tempfile::tempdir()?;
        let gateway = gateway(FabricHome::new(dir.path())).await?;
        let client = client().await?;
        let connection = client.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut send, _recv) = connection.open_bi().await?;
        send.write_all(&[0]).await?;
        tokio::time::timeout(Duration::from_secs(4), connection.closed()).await?;
        assert!(gateway.state().pairing_gate.attempts.lock().is_empty());
        client.close().await;
        gateway.shutdown().await?;
        Ok(())
    }

    async fn rpc(
        send: &mut SendStream,
        recv: &mut RecvStream,
        request: NetworkRequest,
    ) -> Result<Reply> {
        let bytes = serde_json::to_vec(&request)?;
        send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        send.write_all(&bytes).await?;
        let mut length = [0; 4];
        tokio::time::timeout(Duration::from_secs(3), recv.read_exact(&mut length)).await??;
        let length = u32::from_be_bytes(length) as usize;
        anyhow::ensure!(length <= MAX_REQUEST, "oversized reply");
        let mut bytes = vec![0; length];
        recv.read_exact(&mut bytes).await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
    async fn gateway(home: FabricHome) -> Result<FabricNode> {
        FabricNode::start_with_daemon_options(
            home,
            DaemonOptions {
                runtime_peers: true,
                ..DaemonOptions::default()
            },
        )
        .await
    }
    async fn client() -> Result<Endpoint> {
        Ok(Endpoint::builder(presets::N0).bind().await?)
    }
    #[tokio::test]
    async fn restricted_pairing_binds_transport_identity_but_never_admits_services() -> Result<()> {
        use crate::runtime_peers::pairing::{PAIR_ALPN, Policy, Request};
        let dir = tempfile::tempdir()?;
        let gateway = gateway(FabricHome::new(dir.path())).await?;
        gateway
            .expose_exec("pair-test", vec!["/bin/cat".into()])
            .await?;
        let state = gateway.state();
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            + 600;
        let issued = state
            .pairing_request(Request::Issue {
                owner: "test".into(),
                creation_id: "network".into(),
                deadline,
                policy: Policy {
                    grants: vec!["pair-test".into()],
                    name: None,
                    direction: crate::runtime_peers::Direction::Normal,
                },
            })
            .await;
        assert!(issued.error.is_none());
        let attempt_id = issued.status.unwrap().attempt_id;
        let client = client().await?;
        let connection = client.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        let presented = rpc(
            &mut send,
            &mut recv,
            NetworkRequest::Present {
                attempt_id: attempt_id.clone(),
                secret: issued.secret.unwrap(),
            },
        )
        .await?;
        let status = presented.status.unwrap();
        assert_eq!(status.state, State::Pending);
        assert_eq!(status.bound_node_id, Some(client.id()));
        // Old transport credits can permit extra streams even after the
        // post-ALPN limits shrink. None may become a second pairing handler.
        if let Ok(Ok((mut extra_send, mut extra_recv))) = tokio::time::timeout(
            Duration::from_millis(50), connection.open_bi(),
        ).await {
            let request = serde_json::to_vec(&NetworkRequest::Status {
                attempt_id: attempt_id.clone(),
            })?;
            extra_send.write_all(&(request.len() as u32).to_be_bytes()).await?;
            extra_send.write_all(&request).await?;
            let mut header = [0; 4];
            assert!(tokio::time::timeout(
                Duration::from_millis(50), extra_recv.read_exact(&mut header),
            ).await.is_err(), "a second bidi stream was dispatched");
        }
        if let Ok(Ok(mut uni)) = tokio::time::timeout(
            Duration::from_millis(50), connection.open_uni(),
        ).await {
            uni.write_all(b"not a pairing request").await?;
            uni.finish()?;
        }
        // Rotating credential-less keys empty only the unvalidated lane.
        let now = Instant::now();
        for seed in 20..30 {
            assert!(state.pairing_gate.rate(node(seed), now));
        }
        assert!(!state.pairing_gate.rate(node(31), now));
        let same = rpc(
            &mut send,
            &mut recv,
            NetworkRequest::Status {
                attempt_id: attempt_id.clone(),
            },
        )
        .await?;
        assert_eq!(same.status.unwrap().bound_node_id, Some(client.id()));
        assert!(state.peer_book.read().await.peers().is_empty());
        for alpn in [BUILTIN_ECHO_ALPN, mux::MUX_ALPN, b"pair-test"] {
            match client.connect(gateway.addr(), alpn).await {
                Err(_) => {}
                Ok(connection) => {
                    tokio::time::timeout(Duration::from_secs(3), connection.closed()).await?;
                }
            }
        }
        connection.close(VarInt::from_u32(0), b"reconnect");
        tokio::time::timeout(Duration::from_secs(3), async {
            while !state.pairing_gate.attempts.lock().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        // Another authenticated node cannot enumerate even a known random attempt ID.
        let stranger = self::client().await?;
        let other = stranger.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut other_send, mut other_recv) = other.open_bi().await?;
        assert!(
            rpc(
                &mut other_send,
                &mut other_recv,
                NetworkRequest::Status { attempt_id }
            )
            .await
            .is_err()
        );
        stranger.close().await;
        client.close().await;
        gateway.shutdown().await?;
        Ok(())
    }
    #[tokio::test]
    async fn disabled_pairing_and_oversized_frames_create_no_pending_record() -> Result<()> {
        use crate::runtime_peers::pairing::PAIR_ALPN;
        let dir = tempfile::tempdir()?;
        let disabled = FabricNode::start(FabricHome::new(dir.path())).await?;
        let client = client().await?;
        assert!(client.connect(disabled.addr(), PAIR_ALPN).await.is_err());
        disabled.shutdown().await?;
        let enabled = gateway(FabricHome::new(dir.path())).await?;
        let connection = client.connect(enabled.addr(), PAIR_ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(&((MAX_PREAUTH_REQUEST + 1) as u32).to_be_bytes())
            .await?;
        let mut byte = [0; 1];
        let result = tokio::time::timeout(Duration::from_secs(3), recv.read(&mut byte)).await?;
        assert!(matches!(result, Ok(None) | Err(_)));
        assert!(enabled.state().peer_book.read().await.peers().is_empty());
        client.close().await;
        enabled.shutdown().await?;
        Ok(())
    }
    #[test]
    fn flat_decoder_preserves_network_shapes_and_rejects_invalid_fields() -> Result<()> {
        use crate::runtime_peers::pairing::{Payload, Secret};
        for request in [
            NetworkRequest::Present {
                attempt_id: "attempt".into(),
                secret: Secret::new("secret"),
            },
            NetworkRequest::Status { attempt_id: "attempt".into() },
            NetworkRequest::Send {
                attempt_id: "attempt".into(),
                sequence: 1,
                digest: "digest".into(),
                payload: Payload::new(vec![255; 64 * 1024]),
            },
            NetworkRequest::Receive { attempt_id: "attempt".into(), ack: Some(1) },
        ] {
            let bytes = serde_json::to_vec(&request)?;
            assert_eq!(serde_json::to_vec(&decode_request(&bytes)?)?, bytes);
        }
        for bytes in [
            br#"{"operation":"send","attempt_id":"a","sequence":1,"digest":"d"}"#.as_slice(),
            br#"{"operation":"status","attempt_id":"a","payload":[1]}"#,
            br#"{"operation":"status","attempt_id":"a","unknown":1}"#,
            br#"{"operation":"status","attempt_id":"a","attempt_id":"b"}"#,
        ] {
            assert!(decode_request(bytes).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn bound_oversize_header_rejects_only_associated_attempt_durably() -> Result<()> {
        use crate::runtime_peers::pairing::{PAIR_ALPN, Policy, Request};
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let gateway = gateway(home.clone()).await?;
        let state = gateway.state();
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?.as_secs() + 600;
        let mut permits = Vec::new();
        for creation_id in ["oversize", "unrelated"] {
            let reply = state.pairing_request(Request::Issue {
                owner: "test".into(),
                creation_id: creation_id.into(),
                deadline,
                policy: Policy {
                    grants: vec![],
                    name: None,
                    direction: crate::runtime_peers::Direction::Normal,
                },
            }).await;
            assert!(reply.error.is_none());
            permits.push(reply);
        }
        let client = client().await?;
        let connection = client.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        let first = permits.remove(0);
        let status = rpc(&mut send, &mut recv, NetworkRequest::Present {
            attempt_id: first.status.unwrap().attempt_id,
            secret: first.secret.unwrap(),
        }).await?.status.unwrap();
        assert_eq!(status.state, State::Pending);
        // The same node can hold another permit; a bad frame must not reject it.
        let other = client.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut other_send, mut other_recv) = other.open_bi().await?;
        let second = permits.remove(0);
        assert_eq!(rpc(&mut other_send, &mut other_recv, NetworkRequest::Present {
            attempt_id: second.status.unwrap().attempt_id,
            secret: second.secret.unwrap(),
        }).await?.status.unwrap().state, State::Pending);
        // Only the header is sent: rejection must precede body allocation/read.
        send.write_all(&((MAX_REQUEST + 1) as u32).to_be_bytes()).await?;
        tokio::time::timeout(Duration::from_secs(3), connection.closed()).await?;
        let unaffected = state.pairing_request(Request::Query {
            owner: "test".into(),
            creation_id: "unrelated".into(),
        }).await;
        assert_eq!(unaffected.status.unwrap().state, State::Pending);
        client.close().await;
        gateway.shutdown().await?;
        drop(state);
        let restarted = self::gateway(home).await?;
        for (creation_id, reason) in [
            ("oversize", "exchange_limit"),
            ("unrelated", "exchange_lost"),
        ] {
            let reply = restarted.state().pairing_request(Request::Query {
                owner: "test".into(),
                creation_id: creation_id.into(),
            }).await;
            assert!(reply.error.is_none());
            let status = reply.status.unwrap();
            assert_eq!(status.state, State::Rejected);
            assert_eq!(status.terminal_reason.as_deref(), Some(reason));
        }
        restarted.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn disconnected_presenter_cannot_abandon_indeterminate_commit_quarantine() -> Result<()> {
        use crate::runtime_peers::pairing::{PAIR_ALPN, Policy, Request};
        let dir = tempfile::tempdir()?;
        let gateway = gateway(FabricHome::new(dir.path())).await?;
        let state = gateway.state();
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            + 600;
        let issued = state
            .pairing_request(Request::Issue {
                owner: "test".into(),
                creation_id: "disconnect".into(),
                deadline,
                policy: Policy {
                    grants: vec![],
                    name: None,
                    direction: crate::runtime_peers::Direction::Normal,
                },
            })
            .await;
        assert!(issued.error.is_none());
        let client = client().await?;
        let connection = client.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut send, _recv) = connection.open_bi().await?;
        let runtime = state.runtime_peers.as_ref().unwrap().clone();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let mut runtime = runtime.lock().unwrap_or_else(|poison| poison.into_inner());
            runtime.fail_directory_sync_for_test();
            let _ = entered.send(());
            let _ = blocked.recv();
        });
        ready.await?;
        let runtime = state.runtime_peers.as_ref().unwrap();
        let owners_before_request = Arc::strong_count(runtime);
        let bytes = serde_json::to_vec(&NetworkRequest::Present {
            attempt_id: issued.status.unwrap().attempt_id,
            secret: issued.secret.unwrap(),
        })?;
        send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        send.write_all(&bytes).await?;
        // Read-only validation now precedes the admission barrier. Holding the
        // runtime mutex must not hold that barrier while validation waits.
        tokio::time::timeout(Duration::from_secs(3), async {
            // The blocked read worker owns a runtime handle only after the
            // complete request has reached read-only validation.
            while Arc::strong_count(runtime) <= owners_before_request {
                tokio::task::yield_now().await;
            }
        }).await?;
        assert!(state.peer_book.try_read().is_ok());
        connection.close(VarInt::from_u32(0), b"caller disconnected");
        // Give the old disconnect-select path a chance to abandon its commit future.
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        release.send(())?;
        blocker.await?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while state.policy_quarantine.lock().await.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(state.peer_book.read().await.peers().is_empty());
        client.close().await;
        gateway.shutdown().await?;
        Ok(())
    }
    async fn owner_request(
        home: &FabricHome,
        request: crate::runtime_peers::pairing::Request,
    ) -> Result<Reply> {
        match send_control(home, ControlRequest::Pairing { request }).await? {
            ControlResponse::Pairing { reply } => Ok(reply),
            other => bail!("unexpected owner reply: {other:?}"),
        }
    }
    #[tokio::test]
    async fn real_pairing_exchanges_opaque_bytes_with_owner_control_without_grants() -> Result<()> {
        use crate::runtime_peers::pairing::{PAIR_ALPN, Payload, Policy, Request};
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let gateway = gateway(home.clone()).await?;
        let state = gateway.state();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !home.control_socket_path().exists() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            + 600;
        let issued = owner_request(
            &home,
            Request::Issue {
                owner: "admission".into(),
                creation_id: "opaque".into(),
                deadline,
                policy: Policy {
                    grants: vec![],
                    name: None,
                    direction: crate::runtime_peers::Direction::InboundOnly,
                },
            },
        )
        .await?;
        assert!(issued.error.is_none());
        let attempt_id = issued.status.unwrap().attempt_id;
        let client = client().await?;
        let connection = client.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        assert_eq!(
            rpc(
                &mut send,
                &mut recv,
                NetworkRequest::Present {
                    attempt_id: attempt_id.clone(),
                    secret: issued.secret.unwrap(),
                }
            )
            .await?
            .status
            .unwrap()
            .state,
            State::Pending
        );
        let payload = vec![255u8; 64 * 1024];
        let digest = blake3::hash(&payload).to_hex().to_string();
        let sent = rpc(
            &mut send,
            &mut recv,
            NetworkRequest::Send {
                attempt_id: attempt_id.clone(),
                sequence: 1,
                digest: digest.clone(),
                payload: Payload::new(payload.clone()),
            },
        )
        .await?;
        assert!(sent.error.is_none());
        assert_eq!(sent.exchange.unwrap().total_bytes, payload.len() as u64);
        for _ in 0..2 {
            let received = owner_request(
                &home,
                Request::PairReceive {
                    owner: "admission".into(),
                    attempt_id: attempt_id.clone(),
                    ack: None,
                },
            )
            .await?;
            let message = received.exchange.unwrap().message.unwrap();
            assert_eq!(message.sequence, 1);
            assert_eq!(message.digest, digest);
            assert_eq!(message.payload.as_bytes(), payload);
        }
        let acked = owner_request(
            &home,
            Request::PairReceive {
                owner: "admission".into(),
                attempt_id: attempt_id.clone(),
                ack: Some(1),
            },
        )
        .await?;
        assert!(acked.exchange.unwrap().message.is_none());
        let response = b"opaque signed-admission-shaped response".to_vec();
        for _ in 0..2 {
            let reply = owner_request(
                &home,
                Request::PairSend {
                    owner: "admission".into(),
                    attempt_id: attempt_id.clone(),
                    sequence: 1,
                    digest: blake3::hash(&response).to_hex().to_string(),
                    payload: Payload::new(response.clone()),
                },
            )
            .await?;
            assert_eq!(reply.exchange.unwrap().total_messages, 2);
        }
        let received = rpc(
            &mut send,
            &mut recv,
            NetworkRequest::Receive {
                attempt_id,
                ack: None,
            },
        )
        .await?;
        let message = received.exchange.unwrap().message.unwrap();
        assert_eq!(message.payload.as_bytes(), response);
        assert!(state.peer_book.read().await.peers().is_empty());
        client.close().await;
        gateway.shutdown().await?;
        Ok(())
    }
}
