//! Restricted authenticated pairing. No mux, exposure, or ordinary service dispatch.
use super::*;
use crate::runtime_peers::pairing::{NetworkRequest, Reply, State};
use parking_lot::Mutex as StdMutex;
use std::collections::{BTreeSet, HashMap};
use tokio::sync::Semaphore;

const MAX_CONNECTIONS: usize = 32;
// JSON byte arrays require at most four encoded bytes per payload byte.
const MAX_REQUEST: usize = 4 * 64 * 1024 + 4096;
const MAX_RATE_NODES: usize = 1024;
const IDLE: Duration = Duration::from_secs(10);

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
struct Rates {
    global: Bucket,
    nodes: HashMap<EndpointId, Bucket>,
}
#[derive(Debug)]
pub(super) struct Gate {
    connections: Arc<Semaphore>,
    remote_connections: Semaphore,
    attempts: StdMutex<BTreeSet<String>>,
    rates: StdMutex<Rates>,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            remote_connections: Semaphore::new(MAX_CONNECTIONS),
            attempts: StdMutex::new(BTreeSet::new()),
            rates: StdMutex::new(Rates {
                global: Bucket {
                    tokens: 10,
                    last: Instant::now(),
                },
                nodes: HashMap::new(),
            }),
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
    pub(super) fn admit(&self, connection: &Connection) -> bool {
        let Ok(slot) = self.connections.clone().try_acquire_owned() else {
            return false;
        };
        let connection = connection.clone();
        tokio::spawn(async move {
            let _slot = slot;
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

fn refused() -> anyhow::Error {
    FabricError::new(ErrorCode::InvalidArgument, "pairing request refused").into()
}

pub(super) async fn serve(connection: Connection, state: Arc<DaemonState>) -> Result<()> {
    // Socket closure must not cancel a durable commit's quarantine/publication.
    // Framing waits and the connection itself are bounded separately. The incoming
    // handler retains its global incoming-slot permit until this operation ends.
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
        // Once started, publication/quarantine completes even during shutdown.
        let reply = state
            .commit_pairing(|runtime| {
                runtime.pairing_expire()?;
                Ok(Reply {
                    status: None,
                    secret: None,
                    exchange: None,
                    access: vec![],
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

/// One joining-side operation uses this daemon's existing authenticated identity.
pub(super) async fn remote(
    state: &DaemonState,
    gateway: EndpointId,
    addr: Option<EndpointAddr>,
    request: NetworkRequest,
) -> Reply {
    let result = async {
        if state.runtime_peers.is_none() {
            return Err(FabricError::new(
                ErrorCode::RuntimePeersDisabled,
                "pairing requires --runtime-peers",
            )
            .into());
        }
        if state.cancel.is_cancelled() {
            return Err(FabricError::new(ErrorCode::DaemonNotRunning, "daemon is stopping").into());
        }
        if addr.as_ref().is_some_and(|addr| addr.id != gateway) {
            return Err(FabricError::new(
                ErrorCode::InvalidArgument,
                "pairing address does not match pinned gateway NodeID",
            )
            .into());
        }
        let _slot = state
            .pairing_gate
            .remote_connections
            .try_acquire()
            .map_err(|_| {
                FabricError::new(
                    ErrorCode::ServiceUnavailable,
                    "outbound pairing connection limit reached",
                )
            })?;
        let attempt = match &request {
            NetworkRequest::Present { attempt_id, .. }
            | NetworkRequest::Status { attempt_id }
            | NetworkRequest::Send { attempt_id, .. }
            | NetworkRequest::Receive { attempt_id, .. } => attempt_id,
        };
        let bytes = serde_json::to_vec(&request)?;
        if bytes.len() > MAX_REQUEST {
            return Err(FabricError::new(
                ErrorCode::InvalidArgument,
                "encoded pairing request exceeds frame limit",
            )
            .into());
        }
        let endpoint = state.current_endpoint();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let connection = tokio::time::timeout_at(
            deadline,
            endpoint.connect(
                addr.unwrap_or_else(|| EndpointAddr::new(gateway)),
                crate::runtime_peers::pairing::PAIR_ALPN,
            ),
        )
        .await
        .map_err(|_| FabricError::new(ErrorCode::Timeout, "pairing connect timed out"))?
        .map_err(|_| {
            FabricError::new(
                ErrorCode::Unreachable,
                "pinned pairing gateway could not be reached",
            )
        })?;
        let operation = async {
            let (mut send, mut recv) = connection.open_bi().await?;
            send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
            send.write_all(&bytes).await?;
            // EOF lets the gateway release its attempt lease before closing.
            send.finish()?;
            let mut length = [0; 4];
            recv.read_exact(&mut length).await?;
            let length = u32::from_be_bytes(length) as usize;
            anyhow::ensure!(
                length > 0 && length <= crate::runtime_peers::MAX_REQUEST_BYTES,
                "invalid pairing reply frame"
            );
            let mut bytes = vec![0; length];
            recv.read_exact(&mut bytes).await?;
            let reply: Reply = serde_json::from_slice(&bytes)?;
            let status = reply.status.as_ref().context("missing pairing status")?;
            anyhow::ensure!(
                reply.secret.is_none()
                    && reply.error.is_none()
                    && status.attempt_id == *attempt
                    && status.bound_node_id == Some(endpoint.id()),
                "unbound pairing reply"
            );
            // The next local RPC may reconnect only after the old lease is gone.
            let _ = connection.closed().await;
            Ok::<_, anyhow::Error>(reply)
        };
        let result = tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| {
                FabricError::new(
                    ErrorCode::Timeout,
                    "pairing RPC timed out; reconcile attempt before retrying",
                )
            })
            .and_then(|result| {
                result.map_err(|_| {
                    FabricError::new(
                        ErrorCode::Unreachable,
                        "restricted pairing RPC failed; reconcile attempt before retrying",
                    )
                })
            });
        connection.close(VarInt::from_u32(0), b"pairing RPC ended");
        result.map_err(Into::into)
    }
    .await;
    result.unwrap_or_else(|error: anyhow::Error| Reply {
        status: None,
        secret: None,
        exchange: None,
        access: vec![],
        error: Some(crate::runtime_peers::PolicyError::of(&error)),
    })
}

async fn serve_requests(connection: &Connection, state: &DaemonState) -> Result<()> {
    let (mut send, mut recv) = tokio::time::timeout(IDLE, connection.accept_bi()).await??;
    let mut lease: Option<AttemptLease<'_>> = None;
    let mut deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    loop {
        let mut length = [0u8; 4];
        let wait = deadline.min(tokio::time::Instant::now() + IDLE);
        if tokio::time::timeout_at(wait, recv.read(&mut length[..1]))
            .await??
            .is_none()
        {
            send.finish()?;
            let _ = tokio::time::timeout(IDLE, send.stopped()).await;
            return Ok(());
        }
        tokio::time::timeout_at(wait, recv.read_exact(&mut length[1..])).await??;
        // Only a fixed header precedes rate permission; no attacker-sized allocation.
        if !state
            .pairing_gate
            .rate(connection.remote_id(), Instant::now())
        {
            return Err(refused());
        }
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 || length > MAX_REQUEST {
            return Err(refused());
        }
        let mut bytes = vec![0u8; length];
        tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + IDLE),
            recv.read_exact(&mut bytes[..length]),
        )
        .await??;
        let request: NetworkRequest =
            serde_json::from_slice(&bytes[..length]).map_err(|_| refused())?;
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
        let reply = state
            .pairing_network_request(connection.remote_id(), request)
            .await;
        // The restricted path never reports fault internals or any unbound attempt.
        if reply.error.is_some() {
            return Err(refused());
        }
        let status = reply.status.as_ref().ok_or_else(refused)?;
        let terminal = !matches!(status.state, State::Pending);
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        deadline = deadline.min(
            tokio::time::Instant::now() + Duration::from_secs(status.deadline.saturating_sub(wall)),
        );
        let bytes = serde_json::to_vec(&reply)?;
        send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        send.write_all(&bytes).await?;
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
        send.write_all(&((MAX_REQUEST + 1) as u32).to_be_bytes())
            .await?;
        let mut byte = [0; 1];
        let result = tokio::time::timeout(Duration::from_secs(3), recv.read(&mut byte)).await?;
        assert!(matches!(result, Ok(None) | Err(_)));
        assert!(enabled.state().peer_book.read().await.peers().is_empty());
        client.close().await;
        enabled.shutdown().await?;
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
        let bytes = serde_json::to_vec(&NetworkRequest::Present {
            attempt_id: issued.status.unwrap().attempt_id,
            secret: issued.secret.unwrap(),
        })?;
        send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        send.write_all(&bytes).await?;
        // Commit has acquired the admission barrier, but persistence is held off-thread.
        tokio::time::timeout(Duration::from_secs(3), async {
            while state.peer_book.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
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
    #[tokio::test]
    async fn atomic_confirmation_publishes_exact_grants_and_retry_never_regrants() -> Result<()> {
        use crate::runtime_peers::{
            Direction,
            pairing::{ConfirmRequest, PAIR_ALPN, Policy, Request},
        };
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let gateway = gateway(home.clone()).await?;
        gateway
            .expose_exec("not-granted", vec!["/bin/cat".into()])
            .await?;
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
                creation_id: "confirm".into(),
                deadline,
                policy: Policy {
                    grants: vec!["echo".into()],
                    name: Some("joining".into()),
                    direction: Direction::InboundOnly,
                },
            },
        )
        .await?;
        assert!(issued.error.is_none());
        let attempt_id = issued.status.unwrap().attempt_id;
        let client = client().await?;
        let pairing = client.connect(gateway.addr(), PAIR_ALPN).await?;
        let (mut send, mut recv) = pairing.open_bi().await?;
        let pending = rpc(
            &mut send,
            &mut recv,
            NetworkRequest::Present {
                attempt_id: attempt_id.clone(),
                secret: issued.secret.unwrap(),
            },
        )
        .await?
        .status
        .unwrap();
        assert_eq!(pending.state, State::Pending);
        match client.connect(gateway.addr(), BUILTIN_ECHO_ALPN).await {
            Err(_) => {}
            Ok(connection) => {
                tokio::time::timeout(Duration::from_secs(3), connection.closed()).await?;
            }
        }
        let revision = gateway
            .state()
            .runtime_peer_request(crate::runtime_peers::Request::List { cursor: None })
            .await
            .snapshot
            .unwrap()
            .policy_revision;
        let confirmation = ConfirmRequest {
            owner: "admission".into(),
            attempt_id: attempt_id.clone(),
            expected_node_id: client.id(),
            expected_attempt_revision: pending.attempt_revision,
            expected_policy_revision: revision,
            request_id: "confirm-once".into(),
        };
        let confirmed = owner_request(
            &home,
            Request::Confirm {
                confirmation: confirmation.clone(),
            },
        )
        .await?;
        assert!(confirmed.error.is_none(), "{:?}", confirmed.error);
        let receipt = confirmed.status.unwrap().completion.unwrap();
        assert_eq!(confirmed.access.len(), 1);
        assert!(confirmed.access[0].services.contains_key("echo"));
        let echo = client.connect(gateway.addr(), BUILTIN_ECHO_ALPN).await?;
        let (mut echo_send, mut echo_recv) = echo.open_bi().await?;
        echo_send.write_all(b"proof").await?;
        let mut bytes = [0; 5];
        tokio::time::timeout(Duration::from_secs(3), echo_recv.read_exact(&mut bytes)).await??;
        assert_eq!(&bytes, b"proof");
        let unrelated = client.connect(gateway.addr(), b"not-granted").await?;
        tokio::time::timeout(Duration::from_secs(3), unrelated.closed()).await?;
        let own_status = rpc(&mut send, &mut recv, NetworkRequest::Status { attempt_id }).await?;
        assert_eq!(own_status.status.unwrap().state, State::Confirmed);
        let removed = gateway
            .state()
            .runtime_peer_request(crate::runtime_peers::Request::RemovePeer {
                owner: "admission".into(),
                request_id: "revoke-confirmed".into(),
                expected_revision: receipt.policy_revision,
                node_id: client.id(),
            })
            .await;
        assert!(removed.error.is_none(), "{:?}", removed.error);
        let replay = owner_request(
            &home,
            Request::Confirm {
                confirmation: confirmation.clone(),
            },
        )
        .await?;
        assert!(replay.error.is_none(), "{:?}", replay.error);
        assert_eq!(
            replay.status.unwrap().completion.unwrap().request_id,
            receipt.request_id
        );
        assert!(replay.access.is_empty());
        assert!(gateway.state().peer_book.read().await.peers().is_empty());
        gateway.shutdown().await?;
        let restarted = self::gateway(home.clone()).await?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !home.control_socket_path().exists() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let replay = owner_request(&home, Request::Confirm { confirmation }).await?;
        assert!(replay.error.is_none(), "{:?}", replay.error);
        assert!(replay.access.is_empty());
        assert!(restarted.state().peer_book.read().await.peers().is_empty());
        match client.connect(restarted.addr(), BUILTIN_ECHO_ALPN).await {
            Err(_) => {}
            Ok(connection) => {
                tokio::time::timeout(Duration::from_secs(3), connection.closed()).await?;
            }
        }
        client.close().await;
        restarted.shutdown().await?;
        Ok(())
    }
    async fn remote_request(
        home: &FabricHome,
        target: &FabricNode,
        request: NetworkRequest,
    ) -> Result<Reply> {
        match send_control(
            home,
            ControlRequest::PairingRemote {
                gateway: target.id(),
                addr: Some(target.addr()),
                request,
            },
        )
        .await?
        {
            ControlResponse::Pairing { reply } => Ok(reply),
            other => bail!("unexpected joining reply: {other:?}"),
        }
    }
    async fn control_ready(home: &FabricHome) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !home.control_socket_path().exists() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }
    #[tokio::test]
    async fn joining_control_uses_existing_daemon_identity_without_temporary_trust() -> Result<()> {
        use crate::runtime_peers::{
            Direction,
            pairing::{ConfirmRequest, Payload, Policy, Request},
        };
        let owner_dir = tempfile::tempdir()?;
        let joining_dir = tempfile::tempdir()?;
        let owner_home = FabricHome::new(owner_dir.path());
        let joining_home = FabricHome::new(joining_dir.path());
        let owner = gateway(owner_home.clone()).await?;
        let joining = gateway(joining_home.clone()).await?;
        control_ready(&owner_home).await?;
        control_ready(&joining_home).await?;
        let original_id = joining.id();
        let deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            + 600;
        let issued = owner_request(
            &owner_home,
            Request::Issue {
                owner: "fleet".into(),
                creation_id: "daemon-join".into(),
                deadline,
                policy: Policy {
                    grants: vec!["echo".into()],
                    name: None,
                    direction: Direction::InboundOnly,
                },
            },
        )
        .await?;
        assert!(issued.error.is_none());
        let attempt_id = issued.status.unwrap().attempt_id;
        let pending = remote_request(
            &joining_home,
            &owner,
            NetworkRequest::Present {
                attempt_id: attempt_id.clone(),
                secret: issued.secret.unwrap(),
            },
        )
        .await?;
        assert!(pending.error.is_none(), "{:?}", pending.error);
        let pending = pending.status.unwrap();
        assert_eq!(pending.bound_node_id, Some(original_id));
        assert!(owner.state().peer_book.read().await.peers().is_empty());
        assert!(joining.state().peer_book.read().await.peers().is_empty());
        let proof = b"opaque member-key proof".to_vec();
        let sent = remote_request(
            &joining_home,
            &owner,
            NetworkRequest::Send {
                attempt_id: attempt_id.clone(),
                sequence: 1,
                digest: blake3::hash(&proof).to_hex().to_string(),
                payload: Payload::new(proof.clone()),
            },
        )
        .await?;
        assert!(sent.error.is_none(), "{:?}", sent.error);
        let received = owner_request(
            &owner_home,
            Request::PairReceive {
                owner: "fleet".into(),
                attempt_id: attempt_id.clone(),
                ack: None,
            },
        )
        .await?;
        assert_eq!(
            received
                .exchange
                .unwrap()
                .message
                .unwrap()
                .payload
                .as_bytes(),
            proof
        );
        let challenge = b"opaque challenge".to_vec();
        let sent = owner_request(
            &owner_home,
            Request::PairSend {
                owner: "fleet".into(),
                attempt_id: attempt_id.clone(),
                sequence: 1,
                digest: blake3::hash(&challenge).to_hex().to_string(),
                payload: Payload::new(challenge.clone()),
            },
        )
        .await?;
        assert!(sent.error.is_none());
        let received = remote_request(
            &joining_home,
            &owner,
            NetworkRequest::Receive {
                attempt_id: attempt_id.clone(),
                ack: None,
            },
        )
        .await?;
        assert!(received.error.is_none(), "{:?}", received.error);
        assert_eq!(
            received
                .exchange
                .unwrap()
                .message
                .unwrap()
                .payload
                .as_bytes(),
            challenge
        );
        let confirmed = owner_request(
            &owner_home,
            Request::Confirm {
                confirmation: ConfirmRequest {
                    owner: "fleet".into(),
                    attempt_id,
                    expected_node_id: original_id,
                    expected_attempt_revision: pending.attempt_revision,
                    expected_policy_revision: 0,
                    request_id: "admit-existing-identity".into(),
                },
            },
        )
        .await?;
        assert!(confirmed.error.is_none(), "{:?}", confirmed.error);
        assert!(joining.state().peer_book.read().await.peers().is_empty());
        assert_eq!(joining.id(), original_id);
        let echo = joining
            .state()
            .current_endpoint()
            .connect(owner.addr(), BUILTIN_ECHO_ALPN)
            .await?;
        let (mut send, mut recv) = echo.open_bi().await?;
        send.write_all(b"same-id").await?;
        let mut proof = [0; 7];
        tokio::time::timeout(Duration::from_secs(3), recv.read_exact(&mut proof)).await??;
        assert_eq!(&proof, b"same-id");
        joining.shutdown().await?;
        owner.shutdown().await?;
        Ok(())
    }
    #[tokio::test]
    async fn joining_control_requires_opt_in_and_authenticates_pinned_gateway() -> Result<()> {
        use crate::runtime_peers::pairing::Secret;
        let owner_dir = tempfile::tempdir()?;
        let joining_dir = tempfile::tempdir()?;
        let owner = gateway(FabricHome::new(owner_dir.path())).await?;
        let joining_home = FabricHome::new(joining_dir.path());
        let disabled = FabricNode::start(joining_home.clone()).await?;
        control_ready(&joining_home).await?;
        let request = NetworkRequest::Status {
            attempt_id: "unknown".into(),
        };
        let reply = remote_request(&joining_home, &owner, request).await?;
        assert_eq!(reply.error.unwrap().code, "runtime_peers_disabled");
        disabled.shutdown().await?;
        let joining = gateway(joining_home.clone()).await?;
        control_ready(&joining_home).await?;
        let request = ControlRequest::PairingRemote {
            gateway: joining.id(),
            addr: Some(owner.addr()),
            request: NetworkRequest::Present {
                attempt_id: "unknown".into(),
                secret: Secret::new("never-log-this"),
            },
        };
        assert!(!format!("{request:?}").contains("never-log-this"));
        let ControlResponse::Pairing { reply } = send_control(&joining_home, request).await? else {
            bail!("missing pairing reply");
        };
        assert_eq!(reply.error.unwrap().code, "invalid_argument");
        let mut spoofed = owner.addr();
        spoofed.id = node(99);
        let ControlResponse::Pairing { reply } = send_control(
            &joining_home,
            ControlRequest::PairingRemote {
                gateway: spoofed.id,
                addr: Some(spoofed),
                request: NetworkRequest::Status {
                    attempt_id: "unknown".into(),
                },
            },
        )
        .await?
        else {
            bail!("missing pairing reply");
        };
        let error = reply.error.unwrap();
        assert!(matches!(error.code.as_str(), "unreachable" | "timeout"));
        assert!(joining.state().peer_book.read().await.peers().is_empty());
        assert!(owner.state().peer_book.read().await.peers().is_empty());
        joining.shutdown().await?;
        owner.shutdown().await?;
        Ok(())
    }
    #[tokio::test]
    async fn joining_control_sanitizes_bound_gateway_error_replies() -> Result<()> {
        let joining_dir = tempfile::tempdir()?;
        let joining_home = FabricHome::new(joining_dir.path());
        let joining = gateway(joining_home.clone()).await?;
        control_ready(&joining_home).await?;
        let malicious = Endpoint::builder(presets::N0)
            .alpns(vec![crate::runtime_peers::pairing::PAIR_ALPN.to_vec()])
            .bind()
            .await?;
        let server = tokio::spawn({
            let malicious = malicious.clone();
            async move {
                let connection = malicious
                    .accept()
                    .await
                    .context("missing connection")?
                    .await?;
                let (mut send, mut recv) = connection.accept_bi().await?;
                let mut length = [0; 4];
                recv.read_exact(&mut length).await?;
                let length = u32::from_be_bytes(length) as usize;
                anyhow::ensure!(length <= MAX_REQUEST, "oversized request");
                let mut bytes = vec![0; length];
                recv.read_exact(&mut bytes).await?;
                let NetworkRequest::Status { attempt_id } = serde_json::from_slice(&bytes)? else {
                    bail!("expected status request");
                };
                let reply = Reply {
                    status: Some(Status {
                        attempt_id,
                        creation_id: "malicious".into(),
                        state: State::Pending,
                        attempt_revision: 2,
                        deadline: u64::MAX,
                        policy_digest: "unused".into(),
                        terminal_at: None,
                        bound_node_id: Some(connection.remote_id()),
                        terminal_reason: None,
                        completion: None,
                    }),
                    secret: None,
                    exchange: None,
                    access: vec![],
                    error: Some(crate::runtime_peers::PolicyError {
                        code: "injected-code".into(),
                        class: "injected-class".into(),
                        message: "injected-secret-message".into(),
                    }),
                };
                let bytes = serde_json::to_vec(&reply)?;
                send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
                send.write_all(&bytes).await?;
                send.finish()?;
                let _ = tokio::time::timeout(Duration::from_secs(3), send.stopped()).await;
                connection.close(VarInt::from_u32(0), b"fixture ended");
                Ok::<_, anyhow::Error>(())
            }
        });
        let ControlResponse::Pairing { reply } = send_control(
            &joining_home,
            ControlRequest::PairingRemote {
                gateway: malicious.id(),
                addr: Some(malicious.addr()),
                request: NetworkRequest::Status {
                    attempt_id: "bound-attempt".into(),
                },
            },
        )
        .await?
        else {
            bail!("missing pairing reply");
        };
        let encoded = serde_json::to_string(&reply)?;
        assert!(!encoded.contains("injected-"));
        assert!(reply.status.is_none());
        let error = reply.error.context("missing sanitized error")?;
        assert_eq!(error.code, "unreachable");
        assert_eq!(error.class, "fault");
        assert_eq!(
            error.message,
            "restricted pairing RPC failed; reconcile attempt before retrying"
        );
        tokio::time::timeout(Duration::from_secs(3), server).await???;
        malicious.close().await;
        joining.shutdown().await?;
        Ok(())
    }
}
