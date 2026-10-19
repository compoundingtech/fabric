//! Restricted authenticated pairing. No mux, exposure, or ordinary service dispatch.
use super::*;
use crate::runtime_peers::pairing::{NetworkRequest, Reply, State};
use parking_lot::Mutex as StdMutex;
use std::collections::{BTreeSet, HashMap};
use tokio::sync::Semaphore;

const MAX_CONNECTIONS: usize = 32;
const MAX_REQUEST: usize = 4096;
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
    attempts: StdMutex<BTreeSet<String>>,
    rates: StdMutex<Rates>,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
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
    let result = async {
        tokio::select! {
            _ = state.cancel.cancelled() => Ok(()),
            _ = connection.closed() => Ok(()),
            result = tokio::time::timeout(Duration::from_secs(600), serve_requests(&connection, &state)) => {
                result.map_err(|_| refused())?
            }
        }
    }.await;
    // Never copy tokens, input JSON, or attempt IDs into close reasons or logs.
    connection.close(VarInt::from_u32(0), b"pairing connection ended");
    result
}

async fn serve_requests(connection: &Connection, state: &DaemonState) -> Result<()> {
    let (mut send, mut recv) = tokio::time::timeout(IDLE, connection.accept_bi()).await??;
    let mut lease: Option<AttemptLease<'_>> = None;
    let mut deadline = tokio::time::Instant::now() + Duration::from_secs(600);
    loop {
        let mut length = [0u8; 4];
        tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + IDLE),
            recv.read_exact(&mut length),
        )
        .await??;
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
        let mut bytes = [0u8; MAX_REQUEST];
        tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + IDLE),
            recv.read_exact(&mut bytes[..length]),
        )
        .await??;
        let request: NetworkRequest =
            serde_json::from_slice(&bytes[..length]).map_err(|_| refused())?;
        let attempt = match &request {
            NetworkRequest::Present { attempt_id, .. } | NetworkRequest::Status { attempt_id } => {
                attempt_id
            }
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
}
