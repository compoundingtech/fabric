//! Memory-only opaque pairing traffic. No transcript is part of `Stored`.
use super::{pairing, *};
use std::collections::VecDeque;

const MAX_PAYLOAD: usize = 64 * 1024;
const MAX_BYTES: u64 = 1024 * 1024;
const MAX_DIRECTION_MESSAGES: usize = 16;
const MAX_MESSAGES: u64 = 32;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Payload(Vec<u8>);
impl Payload {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageDirection {
    NodeToOwner,
    OwnerToNode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub direction: MessageDirection,
    pub sequence: u64,
    pub digest: String,
    pub payload: Payload,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeReply {
    pub message: Option<Message>,
    pub sent_sequence: Option<u64>,
    pub total_bytes: u64,
    pub total_messages: u64,
}

#[derive(Default)]
struct Stream {
    // Digest history survives acknowledgement; payloads do not.
    digests: Vec<blake3::Hash>,
    queue: VecDeque<Message>,
    acknowledged: u64,
    delivered: u64,
}
pub(super) struct Exchange {
    owner: String,
    binding: pairing::Status,
    node_to_owner: Stream,
    owner_to_node: Stream,
    total_bytes: u64,
    total_messages: u64,
    limited: bool,
}
impl std::fmt::Debug for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED EXCHANGE]")
    }
}

pub(super) enum Operation {
    Send {
        sequence: u64,
        digest: String,
        payload: Payload,
    },
    Receive {
        ack: Option<u64>,
    },
}

enum ExchangeError {
    Refused,
    Limit,
}

pub(super) fn inactive() -> anyhow::Error {
    FabricError::new(ErrorCode::PairingExchangeInactive, "pairing exchange is not pending").into()
}
impl Exchange {
    fn new(owner: String, binding: pairing::Status) -> Self {
        Self {
            owner,
            binding,
            node_to_owner: Stream::default(),
            owner_to_node: Stream::default(),
            total_bytes: 0,
            total_messages: 0,
            limited: false,
        }
    }
    fn needs_terminal(&self, from_owner: bool, operation: &Operation) -> bool {
        if self.limited {
            return true;
        }
        let Operation::Send { sequence, digest, payload } = operation else {
            return false;
        };
        if payload.as_bytes().len() > MAX_PAYLOAD {
            return true;
        }
        let stream = if from_owner { &self.owner_to_node } else { &self.node_to_owner };
        if *sequence != stream.digests.len() as u64 + 1
            || blake3::Hash::from_hex(digest).ok() != Some(blake3::hash(payload.as_bytes()))
        {
            return false;
        }
        stream.digests.len() >= MAX_DIRECTION_MESSAGES
            || self.total_messages >= MAX_MESSAGES
            || self.total_bytes + payload.as_bytes().len() as u64 > MAX_BYTES
    }
    fn erase(&mut self) {
        self.node_to_owner = Stream::default();
        self.owner_to_node = Stream::default();
        self.limited = true;
    }
    fn apply(
        &mut self,
        from_owner: bool,
        operation: Operation,
    ) -> std::result::Result<ExchangeReply, ExchangeError> {
        if self.limited {
            return Err(ExchangeError::Limit);
        }
        let mut reply = ExchangeReply {
            message: None,
            sent_sequence: None,
            total_bytes: self.total_bytes,
            total_messages: self.total_messages,
        };
        match operation {
            Operation::Send {
                sequence,
                digest,
                payload,
            } => {
                // Resource violations terminate even if other message fields are invalid.
                if payload.as_bytes().len() > MAX_PAYLOAD {
                    return Err(ExchangeError::Limit);
                }
                let actual = blake3::hash(payload.as_bytes());
                if blake3::Hash::from_hex(&digest).ok() != Some(actual) {
                    return Err(ExchangeError::Refused);
                }
                let (stream, direction) = if from_owner {
                    (&mut self.owner_to_node, MessageDirection::OwnerToNode)
                } else {
                    (&mut self.node_to_owner, MessageDirection::NodeToOwner)
                };
                if sequence == 0 {
                    return Err(ExchangeError::Refused);
                }
                if sequence <= stream.digests.len() as u64 {
                    if stream.digests[(sequence - 1) as usize] != actual {
                        return Err(ExchangeError::Refused);
                    }
                    reply.sent_sequence = Some(sequence);
                    return Ok(reply);
                }
                if sequence != stream.digests.len() as u64 + 1 {
                    return Err(ExchangeError::Refused);
                }
                if stream.digests.len() >= MAX_DIRECTION_MESSAGES
                    || self.total_messages >= MAX_MESSAGES
                    || self.total_bytes + payload.as_bytes().len() as u64 > MAX_BYTES
                {
                    return Err(ExchangeError::Limit);
                }
                self.total_bytes += payload.as_bytes().len() as u64;
                self.total_messages += 1;
                stream.digests.push(actual);
                stream.queue.push_back(Message {
                    direction,
                    sequence,
                    digest: actual.to_hex().to_string(),
                    payload,
                });
                reply.sent_sequence = Some(sequence);
            }
            Operation::Receive { ack } => {
                let stream = if from_owner {
                    &mut self.node_to_owner
                } else {
                    &mut self.owner_to_node
                };
                if let Some(ack) = ack {
                    if ack == 0 || ack > stream.delivered {
                        return Err(ExchangeError::Refused);
                    }
                    if ack > stream.acknowledged {
                        // Only the oldest unacked message can have been delivered.
                        if stream
                            .queue
                            .front()
                            .is_none_or(|message| message.sequence != ack)
                        {
                            return Err(ExchangeError::Refused);
                        }
                        stream.queue.pop_front();
                        stream.acknowledged = ack;
                    }
                }
                if let Some(message) = stream.queue.front() {
                    stream.delivered = message.sequence;
                    reply.message = Some(message.clone());
                }
            }
        }
        reply.total_bytes = self.total_bytes;
        reply.total_messages = self.total_messages;
        Ok(reply)
    }
}

impl RuntimePeers {
    /// Execute transient traffic only; durable expiry/rejection needs the writer lane.
    fn exchange_read(
        &mut self,
        owner: Option<&str>,
        node: Option<EndpointId>,
        attempt_id: &str,
        operation: Operation,
    ) -> Result<Option<pairing::Reply>> {
        self.ensure_available()?;
        let timestamp = self.pairing_timestamp();
        let attempt = self.stored.pairing.iter().find(|attempt|
            attempt.status.attempt_id == attempt_id
                && match (owner, node) {
                    (Some(owner), None) => attempt.owner == owner,
                    (None, Some(node)) => attempt.status.bound_node_id == Some(node),
                    _ => false,
                }).ok_or_else(|| invalid("pairing exchange request refused"))?;
        if attempt.status.terminal_at.is_some_and(|ended|
            timestamp >= ended.saturating_add(RECEIPT_SECONDS))
        {
            return Err(invalid("pairing exchange request refused"));
        }
        if matches!(attempt.status.state, pairing::State::Issued | pairing::State::Pending)
            && timestamp >= attempt.status.deadline
        {
            self.erase_expired_exchange_payloads(timestamp);
            return Ok(None);
        }
        if attempt.status.state == pairing::State::Pending {
            let needs_terminal = match self.exchanges.get(attempt_id) {
                Some(exchange) => exchange.needs_terminal(owner.is_some(), &operation),
                None => matches!(&operation, Operation::Send { payload, .. }
                    if payload.as_bytes().len() > MAX_PAYLOAD),
            };
            if needs_terminal {
                return Ok(None);
            }
        }
        if self.stored.pairing.iter().any(|attempt|
            matches!(attempt.status.state, pairing::State::Issued | pairing::State::Pending)
                && timestamp >= attempt.status.deadline)
        {
            return Ok(None);
        }
        self.exchange_request(owner, node, attempt_id, operation, timestamp).map(Some)
    }

    pub(crate) fn pairing_exchange_network_read(
        &mut self, node: EndpointId, request: &pairing::NetworkRequest,
    ) -> Result<Option<pairing::Reply>> {
        let (attempt_id, operation) = match request {
            pairing::NetworkRequest::Send { attempt_id, sequence, digest, payload } =>
                (attempt_id, Operation::Send { sequence: *sequence, digest: digest.clone(), payload: payload.clone() }),
            pairing::NetworkRequest::Receive { attempt_id, ack } =>
                (attempt_id, Operation::Receive { ack: *ack }),
            _ => return Err(invalid("not an exchange operation")),
        };
        self.exchange_read(None, Some(node), attempt_id, operation)
    }

    pub(crate) fn pairing_exchange_owner_read(
        &mut self, request: &pairing::Request,
    ) -> Result<Option<pairing::Reply>> {
        let (owner, attempt_id, operation) = match request {
            pairing::Request::PairSend { owner, attempt_id, sequence, digest, payload } =>
                (owner, attempt_id, Operation::Send { sequence: *sequence, digest: digest.clone(), payload: payload.clone() }),
            pairing::Request::PairReceive { owner, attempt_id, ack } =>
                (owner, attempt_id, Operation::Receive { ack: *ack }),
            _ => return Err(invalid("not an exchange operation")),
        };
        self.exchange_read(Some(owner), None, attempt_id, operation)
    }

    pub(crate) fn reject_pairing_frame(
        &mut self, node: EndpointId, attempt_id: &str,
    ) -> Result<pairing::Reply> {
        self.ensure_available()?;
        let timestamp = self.pairing_timestamp();
        let index = self.stored.pairing.iter().position(|attempt|
            attempt.status.attempt_id == attempt_id && attempt.status.bound_node_id == Some(node))
            .ok_or_else(|| invalid("pairing exchange request refused"))?;
        if self.stored.pairing[index].status.state == pairing::State::Pending {
            if let Some(exchange) = self.exchanges.get_mut(attempt_id) {
                exchange.erase();
            }
            let mut next = self.stored.clone();
            let deadline = next.pairing[index].status.deadline;
            if timestamp >= deadline {
                next.pairing[index].terminate(pairing::State::Expired, deadline);
            } else {
                next.pairing[index].terminate(pairing::State::Rejected, timestamp);
                next.pairing[index].status.terminal_reason = Some("exchange_limit".into());
            }
            self.commit_pairing(next, timestamp)?;
        }
        Ok(pairing::Reply {
            status: Some(self.stored.pairing[index].status.clone()),
            secret: None, error: None, exchange: None,
        })
    }
}

impl RuntimePeers {
    pub(super) fn erase_expired_exchange_payloads(&mut self, timestamp: u64) {
        for exchange in self.exchanges.values_mut() {
            if timestamp >= exchange.binding.deadline {
                exchange.erase();
            }
        }
    }

    pub(super) fn prune_exchanges(&mut self) {
        let attempts = &self.stored.pairing;
        self.exchanges.retain(|id, exchange| {
            attempts.iter().any(|attempt| {
                attempt.status.state == pairing::State::Pending
                    && &attempt.status.attempt_id == id
                    && attempt.owner == exchange.owner
                    && attempt.status == exchange.binding
            })
        });
    }

    pub(super) fn prepare_exchange_io(&mut self, timestamp: u64) -> Result<()> {
        self.erase_expired_exchange_payloads(timestamp);
        if !self.stored.pairing.iter().any(|attempt|
            matches!(attempt.status.state, pairing::State::Issued | pairing::State::Pending)
                && timestamp >= attempt.status.deadline)
        {
            return Ok(());
        }
        let mut next = self.stored.clone();
        // Expire all queues, including when the requested operation is refused.
        pairing::recover(
            &mut next,
            timestamp.max(self.stored.pairing_last_seen),
            false,
        );
        self.exchanges.retain(|id, _| {
            next.pairing.iter().any(|attempt| {
                &attempt.status.attempt_id == id && attempt.status.state == pairing::State::Pending
            })
        });
        self.commit_pairing(next, timestamp)
    }

    pub(super) fn exchange_request(
        &mut self,
        owner: Option<&str>,
        node: Option<EndpointId>,
        attempt_id: &str,
        operation: Operation,
        timestamp: u64,
    ) -> Result<pairing::Reply> {
        self.ensure_available()?;
        self.prepare_exchange_io(timestamp)?;
        let refused = || invalid("pairing exchange request refused");
        let index = self
            .stored
            .pairing
            .iter()
            .position(|attempt| {
                attempt.status.attempt_id == attempt_id
                    && attempt
                        .status
                        .terminal_at
                        .is_none_or(|ended| timestamp < ended.saturating_add(RECEIPT_SECONDS))
            })
            .ok_or_else(refused)?;
        let attempt = &self.stored.pairing[index];
        if !match (owner, node) {
            (Some(owner), None) => attempt.owner == owner,
            (None, Some(node)) => attempt.status.bound_node_id == Some(node),
            _ => false,
        } {
            return Err(refused());
        }
        let status = attempt.status.clone();
        if status.state != pairing::State::Pending {
            if matches!(operation, Operation::Send { .. }) {
                return Err(inactive());
            }
            return Ok(pairing::Reply {
                status: Some(status),
                secret: None,
                error: None,
                exchange: None,
            });
        }
        let exchange = self
            .exchanges
            .entry(attempt_id.into())
            .or_insert_with(|| Exchange::new(attempt.owner.clone(), status.clone()));
        let result = exchange.apply(owner.is_some(), operation);
        match result {
            Ok(exchange) => Ok(pairing::Reply {
                status: Some(status),
                secret: None,
                error: None,
                exchange: Some(exchange),
            }),
            Err(ExchangeError::Refused) => Err(refused()),
            Err(ExchangeError::Limit) => {
                // Erase first: an uncertain durable commit must never leave usable traffic.
                if let Some(exchange) = self.exchanges.get_mut(attempt_id) {
                    exchange.erase();
                }
                let mut next = self.stored.clone();
                next.pairing[index].terminate(pairing::State::Rejected, timestamp);
                next.pairing[index].status.terminal_reason = Some("exchange_limit".into());
                let status = next.pairing[index].status.clone();
                if let Err(error) = self.commit_pairing(next, timestamp) {
                    self.set_fault(&error);
                    return Err(error);
                }
                Ok(pairing::Reply {
                    status: Some(status),
                    secret: None,
                    error: None,
                    exchange: None,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn faulted_sweep_erases_expired_traffic_without_persisting_or_resetting_history() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let (mut store, _) = RuntimePeers::open(&home);
        assert!(store.fault().is_none());
        let t = now();
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let mut statuses = Vec::new();
        for id in ["limit-fault", "other-traffic"] {
            let issued = store
                .pairing_request(pairing::Request::Issue {
                    owner: "fleet".into(),
                    creation_id: id.into(),
                    deadline: t + 600,
                    policy: pairing::Policy {
                        grants: vec![],
                        name: None,
                        direction: Direction::Normal,
                    },
                })
                .unwrap();
            let status = issued.status.unwrap();
            store
                .pairing_network_request(
                    node,
                    pairing::NetworkRequest::Present {
                        attempt_id: status.attempt_id.clone(),
                        secret: issued.secret.unwrap(),
                    },
                )
                .unwrap();
            for owner in [false, true] {
                store
                    .exchange_request(
                        owner.then_some("fleet"),
                        (!owner).then_some(node),
                        &status.attempt_id,
                        Operation::Send {
                            sequence: 1,
                            digest: blake3::hash(&[42]).to_hex().to_string(),
                            payload: Payload::new(vec![42]),
                        },
                        t,
                    )
                    .unwrap();
            }
            statuses.push(status);
        }
        let disk = fs::read_to_string(&store.path).unwrap();
        let path = store.path.clone();
        store.path = dir.path().join("missing/runtime-peers.toml");
        assert!(
            store
                .exchange_request(
                    None,
                    Some(node),
                    &statuses[0].attempt_id,
                    Operation::Send {
                        sequence: 2,
                        digest: blake3::hash(&vec![42; MAX_PAYLOAD + 1])
                            .to_hex()
                            .to_string(),
                        payload: Payload::new(vec![42; MAX_PAYLOAD + 1])
                    },
                    t
                )
                .is_err()
        );
        let fault = store.fault().unwrap().to_owned();
        let highwater = store.stored.pairing_last_seen;
        assert!(
            !store.exchanges[&statuses[1].attempt_id]
                .node_to_owner
                .queue
                .is_empty()
        );
        store.pairing_clock = (t + 601, std::time::Instant::now());
        assert!(store.pairing_expire().is_err());
        assert_eq!(store.fault(), Some(fault.as_str()));
        assert_eq!(store.stored.pairing_last_seen, highwater);
        assert!(
            store
                .stored
                .pairing
                .iter()
                .all(|attempt| attempt.status.state == pairing::State::Pending)
        );
        assert_eq!(store.exchanges.len(), 2);
        for exchange in store.exchanges.values() {
            assert!(exchange.limited);
            assert!(exchange.node_to_owner.queue.is_empty());
            assert!(exchange.node_to_owner.digests.is_empty());
            assert!(exchange.owner_to_node.queue.is_empty());
            assert!(exchange.owner_to_node.digests.is_empty());
            assert_eq!((exchange.total_bytes, exchange.total_messages), (2, 2));
            assert_eq!(exchange.binding.bound_node_id, Some(node));
            assert_eq!(exchange.owner, "fleet");
        }
        assert_eq!(fs::read_to_string(&path).unwrap(), disk);
        store.path = path;
        store.reconcile(&home).unwrap();
        // Reconciliation retains the monotonic floor, so due attempts and
        // their transient queues are already terminal before the next poll.
        assert!(store.exchanges.is_empty());
        for status in statuses {
            let reply = store
                .pairing_network_request(
                    node,
                    pairing::NetworkRequest::Receive {
                        attempt_id: status.attempt_id,
                        ack: None,
                    },
                )
                .unwrap();
            assert!(reply.exchange.is_none());
            assert_eq!(reply.status.unwrap().state, pairing::State::Expired);
        }
        assert!(store.exchanges.is_empty());
    }
}
