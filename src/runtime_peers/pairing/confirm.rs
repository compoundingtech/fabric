//! Pairing completion shares the runtime policy's atomic durable commit.
use super::*;

fn request_digest<T: Serialize>(operation: &str, request: &T) -> Result<String> {
    Ok(blake3::hash(&serde_json::to_vec(&(operation, request))?)
        .to_hex()
        .to_string())
}

pub(super) fn validate_completion(attempt: &Attempt) -> Result<()> {
    let status = &attempt.status;
    match (&status.completion, &attempt.request_digest) {
        (Some(receipt), Some(hash)) => {
            validate_token(&receipt.request_id, "request_id")?;
            if blake3::Hash::from_hex(hash).is_err()
                || receipt.attempt_id != status.attempt_id
                || Some(receipt.node_id) != status.bound_node_id
                || receipt.policy_digest != status.policy_digest
                || Some(receipt.committed_at) != status.terminal_at
                || receipt.outcome != status.state
                || !matches!(receipt.outcome, State::Confirmed | State::Rejected)
                || (receipt.outcome == State::Confirmed
                    && (receipt.committed_at >= status.deadline || receipt.policy_revision == 0))
            {
                return Err(unavailable("invalid pairing completion receipt"));
            }
        }
        (None, None)
            if status.state != State::Confirmed
                && status.terminal_reason.as_deref() != Some("rejected") => {}
        _ => {
            return Err(unavailable(
                "missing or inconsistent pairing completion identity",
            ));
        }
    }
    Ok(())
}

impl RuntimePeers {
    fn pairing_completion_index(
        &self,
        owner: &str,
        attempt_id: &str,
        node: EndpointId,
        request_id: &str,
        digest: &str,
        timestamp: u64,
    ) -> Result<(usize, bool)> {
        for (value, field) in [
            (owner, "owner"),
            (attempt_id, "attempt_id"),
            (request_id, "request_id"),
        ] {
            validate_token(value, field)?;
        }
        let retained = |attempt: &&Attempt| {
            attempt
                .status
                .terminal_at
                .is_none_or(|ended| timestamp < ended.saturating_add(RECEIPT_SECONDS))
        };
        if let Some(prior) = self.stored.pairing.iter().filter(retained).find(|attempt| {
            attempt.owner == owner
                && attempt.status.completion.as_ref()
                    .is_some_and(|r| r.request_id == request_id)
        }) {
            if prior.status.attempt_id != attempt_id
                || prior.status.bound_node_id != Some(node)
                || prior.request_digest.as_deref() != Some(digest)
            {
                return Err(FabricError::new(
                    ErrorCode::RequestIdConflict,
                    "pairing request_id already names different contents",
                )
                .into());
            }
        }
        let index = self
            .stored
            .pairing
            .iter()
            .enumerate()
            .find(|(_, attempt)| {
                retained(attempt)
                    && attempt.owner == owner
                    && attempt.status.attempt_id == attempt_id
            })
            .map(|(index, _)| index)
            .ok_or_else(|| invalid("unknown or expired pairing attempt"))?;
        let attempt = &self.stored.pairing[index];
        if attempt.status.bound_node_id != Some(node) {
            return Err(invalid("pairing confirmation binding mismatch"));
        }
        let replay = attempt
            .status
            .completion
            .as_ref()
            .is_some_and(|r| r.request_id == request_id)
            && attempt.request_digest.as_deref() == Some(digest);
        Ok((index, replay))
    }

    pub(crate) fn pairing_attempt_reply(&mut self, owner: &str, attempt_id: &str) -> Result<Reply> {
        let timestamp = self.pairing_timestamp();
        self.pairing_attempt_reply_at(owner, attempt_id, timestamp)
    }

    fn pairing_attempt_reply_at(
        &self,
        owner: &str,
        attempt_id: &str,
        timestamp: u64,
    ) -> Result<Reply> {
        self.ensure_available()?;
        validate_token(owner, "owner")?;
        validate_token(attempt_id, "attempt_id")?;
        let attempt = self
            .stored
            .pairing
            .iter()
            .find(|attempt| {
                attempt.owner == owner
                    && attempt.status.attempt_id == attempt_id
                    && attempt
                        .status
                        .terminal_at
                        .is_none_or(|ended| timestamp < ended.saturating_add(RECEIPT_SECONDS))
            })
            .ok_or_else(|| invalid("unknown or expired pairing attempt"))?;
        Ok(Reply {
            status: Some(attempt.status.clone()),
            secret: None,
            error: None,
            exchange: None,
            access: attempt
                .status
                .bound_node_id
                .map_or_else(Vec::new, |node| self.access_for(&[node])),
        })
    }


    pub(crate) fn prepare_pairing_confirmation(
        &mut self,
        request: &ConfirmRequest,
    ) -> Result<PreparedConfirmation> {
        let timestamp = self.pairing_timestamp();
        self.prepare_pairing_confirmation_at(request, timestamp)
    }

    fn prepare_pairing_confirmation_at(
        &self,
        request: &ConfirmRequest,
        timestamp: u64,
    ) -> Result<PreparedConfirmation> {
        self.ensure_available()?;
        let digest = request_digest("confirm", request)?;
        let (index, replay) = self.pairing_completion_index(
            &request.owner,
            &request.attempt_id,
            request.expected_node_id,
            &request.request_id,
            &digest,
            timestamp,
        )?;
        let mut next = self.stored.clone();
        if !replay {
            let attempt = &next.pairing[index];
            if attempt.status.state != State::Pending
                || timestamp >= attempt.status.deadline
                || attempt.status.attempt_revision != request.expected_attempt_revision
            {
                return Err(invalid("pairing attempt is not eligible for confirmation"));
            }
            if request.expected_policy_revision != next.revision {
                return Err(FabricError::new(
                    ErrorCode::PolicyRevisionConflict,
                    "pairing policy revision mismatch",
                )
                .into());
            }
            if next.records.iter().any(|record| record.peer.id == request.expected_node_id)
                || self.static_input.book.peers().iter()
                    .any(|peer| peer.id == request.expected_node_id)
            {
                return Err(invalid(
                    "pairing cannot confirm an already trusted NodeID",
                ));
            }
            if next.records.len() >= MAX_RECORDS {
                return Err(invalid(
                    "runtime policy permits at most 1024 owner/node records",
                ));
            }
            let mut peer = Peer {
                id: request.expected_node_id,
                name: attempt.policy.name.clone(),
                addr: None,
                roaming: false,
                dial_only: false,
                inbound_only: false,
                allow: attempt.policy.grants.clone(),
            };
            attempt.policy.direction.apply(&mut peer);
            next.records.push(Record {
                owner: request.owner.clone(),
                peer,
                expires_at: None,
                expiry_request_id: None,
            });
            next.records
                .sort_by(|a, b| (&a.owner, a.peer.id).cmp(&(&b.owner, b.peer.id)));
        }
        let book = self.compose(&next)?.0;
        Ok(PreparedConfirmation {
            book,
            replay,
            replay_reply: if replay {
                Some(self.pairing_attempt_reply_at(&request.owner, &request.attempt_id, timestamp)?)
            } else {
                None
            },
            records: next.records,
            request: request.clone(),
            revision: self.stored.revision,
        })
    }

    pub(crate) fn commit_pairing_confirmation(
        &mut self,
        prepared: PreparedConfirmation,
    ) -> Result<Reply> {
        let timestamp = self.pairing_timestamp();
        self.commit_pairing_confirmation_at(prepared, timestamp)
    }

    fn commit_pairing_confirmation_at(
        &mut self,
        prepared: PreparedConfirmation,
        timestamp: u64,
    ) -> Result<Reply> {
        self.ensure_available()?;
        let request = &prepared.request;
        let digest = request_digest("confirm", request)?;
        let (index, replay) = self.pairing_completion_index(
            &request.owner,
            &request.attempt_id,
            request.expected_node_id,
            &request.request_id,
            &digest,
            timestamp,
        )?;
        if replay {
            return self.pairing_attempt_reply_at(&request.owner, &request.attempt_id, timestamp);
        }
        if prepared.replay || self.stored.revision != prepared.revision {
            return Err(FabricError::new(
                ErrorCode::PolicyRevisionConflict,
                "policy changed during pairing preparation",
            )
            .into());
        }
        let attempt = &self.stored.pairing[index];
        if attempt.status.state != State::Pending
            || attempt.status.attempt_revision != request.expected_attempt_revision
        {
            return Err(invalid("pairing attempt changed during preparation"));
        }
        // This is the admission-barrier linearization point, before the potentially slow fsync.
        let mut next = self.stored.clone();
        next.pairing_last_seen = timestamp.max(next.pairing_last_seen);
        if timestamp >= attempt.status.deadline {
            next.pairing[index].expire(timestamp);
            self.commit_pairing(next, timestamp)?;
            return Err(invalid(
                "pairing attempt expired before confirmation commit",
            ));
        }
        next.records = prepared.records;
        next.revision = next
            .revision
            .checked_add(1)
            .context("policy revision exhausted")?;
        Self::complete_pairing(
            &mut next,
            index,
            State::Confirmed,
            &request.request_id,
            digest,
            timestamp,
        );
        if let Err(error) = self.persist(&next) {
            if crate::error::code_of(&error) == ErrorCode::RuntimeCommitIndeterminate {
                self.exchanges.remove(&request.attempt_id);
                self.set_fault(&error);
            }
            return Err(error);
        }
        #[cfg(test)]
        if std::mem::take(&mut self.panic_confirmation_and_fault.0) {
            panic!("injected confirmation worker failure after persist, before stored update");
        }
        self.stored = next;
        self.prune_exchanges();
        self.pairing_attempt_reply_at(&request.owner, &request.attempt_id, timestamp)
    }

    fn complete_pairing(
        next: &mut Stored,
        index: usize,
        outcome: State,
        request_id: &str,
        digest: String,
        timestamp: u64,
    ) {
        let attempt = &mut next.pairing[index];
        attempt.terminate(outcome.clone(), timestamp);
        if outcome == State::Rejected {
            attempt.status.terminal_reason = Some("rejected".into());
        }
        attempt.request_digest = Some(digest);
        attempt.status.completion = Some(Completion {
            request_id: request_id.into(),
            attempt_id: attempt.status.attempt_id.clone(),
            node_id: attempt
                .status
                .bound_node_id
                .expect("completion requires binding"),
            policy_digest: attempt.status.policy_digest.clone(),
            policy_revision: next.revision,
            committed_at: timestamp,
            outcome,
        });
        // Request IDs become reusable at the retention boundary. Remove the
        // obsolete identity in this same candidate, after the selected attempt
        // has been completed, so pruning cannot invalidate its vector index.
        next.pairing.retain(|attempt| {
            attempt
                .status
                .terminal_at
                .is_none_or(|ended| timestamp < ended.saturating_add(RECEIPT_SECONDS))
        });
    }

    pub(super) fn reject_pairing_at(
        &mut self,
        request: &RejectRequest,
        timestamp: u64,
    ) -> Result<Reply> {
        self.ensure_available()?;
        let digest = request_digest("reject", request)?;
        let (index, replay) = self.pairing_completion_index(
            &request.owner,
            &request.attempt_id,
            request.expected_node_id,
            &request.request_id,
            &digest,
            timestamp,
        )?;
        if !replay {
            let attempt = &self.stored.pairing[index];
            if attempt.status.state != State::Pending
                || timestamp >= attempt.status.deadline
                || attempt.status.attempt_revision != request.expected_attempt_revision
            {
                return Err(invalid("pairing attempt is not eligible for rejection"));
            }
            let mut next = self.stored.clone();
            next.pairing_last_seen = timestamp.max(next.pairing_last_seen);
            Self::complete_pairing(
                &mut next,
                index,
                State::Rejected,
                &request.request_id,
                digest,
                timestamp,
            );
            if let Err(error) = self.commit_pairing(next, timestamp) {
                if self.fault.is_some() {
                    self.exchanges.remove(&request.attempt_id);
                }
                return Err(error);
            }
        }
        self.pairing_attempt_reply_at(&request.owner, &request.attempt_id, timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn open(dir: &tempfile::TempDir) -> RuntimePeers {
        let (store, _) = RuntimePeers::open(&FabricHome::new(dir.path()));
        assert!(store.fault().is_none(), "{:?}", store.fault());
        store
    }

    fn pending(store: &mut RuntimePeers, id: &str, timestamp: u64) -> ConfirmRequest {
        pending_owned(store, id, timestamp, "fleet")
    }

    fn pending_owned(
        store: &mut RuntimePeers,
        id: &str,
        timestamp: u64,
        owner: &str,
    ) -> ConfirmRequest {
        let issued = store
            .pairing_request_at(
                Request::Issue {
                    owner: owner.into(),
                    creation_id: id.into(),
                    deadline: timestamp + 600,
                    policy: Policy {
                        grants: vec!["st3-client-v0".into(), "ssh".into()],
                        name: Some("paired".into()),
                        direction: Direction::InboundOnly,
                    },
                },
                timestamp,
            )
            .unwrap();
        let status = issued.status.unwrap();
        let status = store
            .pairing_network_request_at(
                node(1),
                NetworkRequest::Present {
                    attempt_id: status.attempt_id,
                    secret: issued.secret.unwrap(),
                },
                timestamp,
            )
            .unwrap()
            .status
            .unwrap();
        let bytes = b"private handshake".to_vec();
        store
            .pairing_request_at(
                Request::PairSend {
                    owner: owner.into(),
                    attempt_id: status.attempt_id.clone(),
                    sequence: 1,
                    digest: blake3::hash(&bytes).to_hex().to_string(),
                    payload: Payload::new(bytes),
                },
                timestamp,
            )
            .unwrap();
        assert!(!store.exchanges.is_empty());
        ConfirmRequest {
            owner: owner.into(),
            attempt_id: status.attempt_id,
            expected_node_id: node(1),
            expected_attempt_revision: 2,
            expected_policy_revision: store.stored.revision,
            request_id: format!("confirm-{id}"),
        }
    }

    fn confirm(store: &mut RuntimePeers, request: &ConfirmRequest, timestamp: u64) -> Reply {
        let prepared = store
            .prepare_pairing_confirmation_at(request, timestamp)
            .unwrap();
        store
            .commit_pairing_confirmation_at(prepared, timestamp)
            .unwrap()
    }

    fn remove(store: &mut RuntimePeers, request_id: &str) {
        let tx = Transaction {
            owner: "fleet".into(),
            request_id: request_id.into(),
            expected_revision: store.stored.revision,
            operations: vec![Operation::RemovePeer { node_id: node(1) }],
        };
        let prepared = store.prepare(&tx).unwrap();
        store
            .commit(prepared, request_id, Cancellation::empty())
            .unwrap();
    }

    #[test]
    fn atomic_confirmation_exact_policy_and_restart_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "atomic", t);
        assert!(store.snapshot(None).unwrap().peers.is_empty());
        let prepared = store.prepare_pairing_confirmation_at(&request, t).unwrap();
        assert!(store.snapshot(None).unwrap().peers.is_empty());
        let reply = store
            .commit_pairing_confirmation_at(prepared, t + 1)
            .unwrap();
        let status = reply.status.unwrap();
        let receipt = status.completion.clone().unwrap();
        assert_eq!(status.state, State::Confirmed);
        assert_eq!(status.attempt_revision, 3);
        assert_eq!(receipt.committed_at, t + 1);
        assert_eq!(receipt.policy_revision, 1);
        assert_eq!(receipt.policy_digest, status.policy_digest);
        assert_eq!(reply.access[0].direction, Direction::InboundOnly);
        assert_eq!(reply.access[0].name.as_deref(), Some("paired"));
        assert_eq!(
            reply.access[0].services.keys().cloned().collect::<Vec<_>>(),
            vec!["ssh".to_string(), "st3-client-v0".to_string()]
        );
        assert!(store.stored.receipts.is_empty());
        assert!(store.stored.pairing[0].salt.is_none());
        assert!(store.stored.pairing[0].secret_hash.is_none());
        assert!(store.exchanges.is_empty());
        store.validate_stored().unwrap();
        let mut restarted = open(&dir);
        assert_eq!(restarted.stored.revision, 1);
        let replay = restarted
            .prepare_pairing_confirmation_at(&request, t + 2)
            .unwrap();
        assert!(replay.replay);
        assert_eq!(
            restarted
                .commit_pairing_confirmation_at(replay, t + 2)
                .unwrap()
                .status
                .unwrap()
                .completion
                .unwrap(),
            receipt
        );
        assert_eq!(restarted.stored.revision, 1);
        assert_eq!(
            restarted.stored.pairing[0]
                .status
                .terminal_reason
                .as_deref(),
            Some("confirmed")
        );
    }

    #[test]
    fn confirmation_selectors_and_competing_preparations_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "selectors", t);
        let mut bad = request.clone();
        bad.owner = "other".into();
        assert!(store.prepare_pairing_confirmation_at(&bad, t).is_err());
        bad = request.clone();
        bad.expected_node_id = node(2);
        assert!(store.prepare_pairing_confirmation_at(&bad, t).is_err());
        bad = request.clone();
        bad.expected_attempt_revision = 1;
        assert!(store.prepare_pairing_confirmation_at(&bad, t).is_err());
        bad = request.clone();
        bad.expected_policy_revision += 1;
        assert!(store.prepare_pairing_confirmation_at(&bad, t).is_err());
        let first = store.prepare_pairing_confirmation_at(&request, t).unwrap();
        let mut competitor = request.clone();
        competitor.request_id = "competitor".into();
        let second = store
            .prepare_pairing_confirmation_at(&competitor, t)
            .unwrap();
        store.commit_pairing_confirmation_at(first, t).unwrap();
        assert!(store.commit_pairing_confirmation_at(second, t).is_err());
        assert_eq!(store.stored.records.len(), 1);
        assert_eq!(store.stored.revision, 1);
        let mut changed = request.clone();
        changed.expected_policy_revision = 1;
        assert_eq!(
            crate::error::code_of(
                &store
                    .prepare_pairing_confirmation_at(&changed, t)
                    .err()
                    .unwrap()
            ),
            ErrorCode::RequestIdConflict
        );
        bad = request.clone();
        bad.owner = "other".into();
        assert!(store.prepare_pairing_confirmation_at(&bad, t).is_err());
        let next = pending(&mut store, "reuse", t);
        let mut reused = next;
        reused.request_id = request.request_id;
        assert!(store.prepare_pairing_confirmation_at(&reused, t).is_err());
    }

    #[test]
    fn deadline_linearizes_at_commit_not_preparation_or_fsync_reply() {
        for offset in [600, 601] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let request = pending(&mut store, "expiry", t);
            let prepared = store
                .prepare_pairing_confirmation_at(&request, t + 599)
                .unwrap();
            assert!(
                store
                    .commit_pairing_confirmation_at(prepared, t + offset)
                    .is_err()
            );
            assert_eq!(store.stored.pairing[0].status.state, State::Expired);
            assert_eq!(store.stored.revision, 0);
            assert!(store.snapshot(None).unwrap().peers.is_empty());
            assert!(open(&dir).snapshot(None).unwrap().peers.is_empty());
        }
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "last-second", t);
        // Completion records the accepted instant, not a subsequent response/status instant.
        let receipt = confirm(&mut store, &request, t + 599)
            .status
            .unwrap()
            .completion
            .unwrap();
        store
            .pairing_request_at(
                Request::Query {
                    owner: "fleet".into(),
                    creation_id: "last-second".into(),
                },
                t + 601,
            )
            .unwrap();
        assert_eq!(
            store.stored.pairing[0].status.completion.as_ref(),
            Some(&receipt)
        );
        assert_eq!(receipt.committed_at, t + 599);
    }

    #[test]
    fn pre_rename_failure_keeps_pending_and_indeterminate_recovers_atomic_policy() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "crash", t);
        let prepared = store.prepare_pairing_confirmation_at(&request, t).unwrap();
        let original = store.path.clone();
        store.path = dir.path().join("missing-directory").join("state.toml");
        assert!(
            store
                .commit_pairing_confirmation_at(prepared, t + 1)
                .is_err()
        );
        store.path = original;
        assert!(store.fault().is_none());
        assert_eq!(store.stored.pairing[0].status.state, State::Pending);
        assert_eq!(store.stored.revision, 0);
        let disk: Stored = toml::from_str(&fs::read_to_string(&store.path).unwrap()).unwrap();
        assert_eq!(disk.pairing[0].status.state, State::Pending);
        assert!(disk.records.is_empty());
        let prepared = store
            .prepare_pairing_confirmation_at(&request, t + 2)
            .unwrap();
        store.fail_directory_sync = true;
        let error = store
            .commit_pairing_confirmation_at(prepared, t + 2)
            .unwrap_err();
        assert_eq!(
            crate::error::code_of(&error),
            ErrorCode::RuntimeCommitIndeterminate
        );
        assert!(store.fault().is_some());
        assert!(
            store
                .pairing_attempt_reply("fleet", &request.attempt_id)
                .is_err()
        );
        let mut restarted = open(&dir);
        assert_eq!(restarted.stored.pairing[0].status.state, State::Confirmed);
        assert_eq!(restarted.stored.revision, 1);
        assert_eq!(restarted.snapshot(None).unwrap().peers.len(), 1);
        assert!(
            restarted
                .prepare_pairing_confirmation_at(&request, t + 3)
                .unwrap()
                .replay
        );
        let replay = confirm(&mut restarted, &request, t + 3);
        assert_eq!(
            replay.status.unwrap().completion.unwrap().committed_at,
            t + 2
        );
    }

    #[test]
    fn rejection_receipt_is_exact_and_never_installs_policy() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let confirmation = pending(&mut store, "reject", t);
        let rejection = RejectRequest {
            owner: confirmation.owner.clone(),
            attempt_id: confirmation.attempt_id.clone(),
            expected_node_id: node(1),
            expected_attempt_revision: 2,
            request_id: "reject-one".into(),
        };
        let reply = store
            .pairing_request_at(
                Request::Reject {
                    rejection: rejection.clone(),
                },
                t,
            )
            .unwrap();
        let receipt = reply.status.unwrap().completion.unwrap();
        assert_eq!(receipt.outcome, State::Rejected);
        assert_eq!(receipt.policy_revision, 0);
        assert!(reply.access.is_empty());
        assert!(store.stored.pairing[0].secret_hash.is_none());
        assert!(store.exchanges.is_empty());
        assert_eq!(
            store
                .pairing_request_at(
                    Request::Reject {
                        rejection: rejection.clone()
                    },
                    t + 1
                )
                .unwrap()
                .status
                .unwrap()
                .completion
                .unwrap(),
            receipt
        );
        let mut bad = rejection.clone();
        bad.expected_attempt_revision = 3;
        assert!(
            store
                .pairing_request_at(Request::Reject { rejection: bad }, t + 1)
                .is_err()
        );
        let mut bad = rejection.clone();
        bad.request_id = "different".into();
        assert!(
            store
                .pairing_request_at(Request::Reject { rejection: bad }, t + 1)
                .is_err()
        );
        assert!(
            store
                .prepare_pairing_confirmation_at(&confirmation, t + 1)
                .is_err()
        );
        let mut restarted = open(&dir);
        assert_eq!(
            restarted
                .pairing_request_at(Request::Reject { rejection }, t + 1)
                .unwrap()
                .status
                .unwrap()
                .completion
                .unwrap(),
            receipt
        );
        assert!(restarted.snapshot(None).unwrap().peers.is_empty());
    }

    #[test]
    fn confirmation_replay_and_queries_after_removal_never_regrant_even_on_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "removed", t);
        let receipt = confirm(&mut store, &request, t)
            .status
            .unwrap()
            .completion
            .unwrap();
        remove(&mut store, "remove");
        for restart in [false, true] {
            if restart {
                store = open(&dir);
            }
            let replay = store
                .prepare_pairing_confirmation_at(&request, t + 1)
                .unwrap();
            assert!(replay.replay);
            assert!(replay.book.peers().is_empty());
            let reply = store.commit_pairing_confirmation_at(replay, t + 1).unwrap();
            assert!(reply.access.is_empty());
            assert_eq!(reply.status.unwrap().completion.as_ref(), Some(&receipt));
            let query = store
                .pairing_request_at(
                    Request::Query {
                        owner: "fleet".into(),
                        creation_id: "removed".into(),
                    },
                    t + 1,
                )
                .unwrap();
            assert!(query.access.is_empty());
            assert_eq!(query.status.unwrap().completion.as_ref(), Some(&receipt));
            let restricted = store
                .pairing_network_request_at(
                    node(1),
                    NetworkRequest::Status {
                        attempt_id: request.attempt_id.clone(),
                    },
                    t + 1,
                )
                .unwrap();
            assert!(restricted.access.is_empty());
            assert_eq!(
                restricted.status.unwrap().completion.as_ref(),
                Some(&receipt)
            );
            assert_eq!(store.stored.revision, 2);
        }
    }

    #[test]
    fn existing_owner_and_source_conflicts_are_refused() {
        for conflict in ["owner", "direction", "name", "already-trusted"] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let request = pending(&mut store, conflict, t);
            let tx = Transaction {
                owner: if conflict == "owner" {
                    "fleet"
                } else {
                    "other"
                }
                .into(),
                request_id: "existing".into(),
                expected_revision: 0,
                operations: vec![Operation::AddPeer {
                    node_id: node(1),
                    name: if conflict == "name" {
                        Some("conflicting".into())
                    } else {
                        None
                    },
                    direction: Some(if conflict == "direction" {
                        Direction::Normal
                    } else {
                        Direction::InboundOnly
                    }),
                    addr: None,
                }],
            };
            let prepared = store.prepare(&tx).unwrap();
            store
                .commit(prepared, "existing", Cancellation::empty())
                .unwrap();
            let mut request = request;
            request.expected_policy_revision = 1;
            assert!(store.prepare_pairing_confirmation_at(&request, t).is_err());
            assert_eq!(store.stored.pairing[0].status.state, State::Pending);
            assert_eq!(store.stored.records.len(), 1);
        }
    }

    #[test]
    fn static_label_direction_and_already_trusted_records_refuse_confirmation() {
        for fields in [
            "name = \"conflicting\"\ninbound_only = true\n",
            "name = \"paired\"\n",
            "name = \"paired\"\ninbound_only = true\n",
            "inbound_only = true\n",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let mut request = pending(&mut store, "static-conflict", t);
            fs::write(
                &store.static_input.path,
                format!("format = 2\n[[peers]]\nid = \"{}\"\n{fields}allow = [\"residual\"]\n", node(1)),
            ).unwrap();
            store.reload_static(StaticInput::load(&FabricHome::new(dir.path())).unwrap()).unwrap();
            request.expected_policy_revision = store.stored.revision;
            let disk = fs::read(&store.path).unwrap();
            let error = store.prepare_pairing_confirmation_at(&request, t).err().unwrap();
            assert!(error.to_string().contains("already trusted"));
            assert_eq!(fs::read(&store.path).unwrap(), disk);
            assert!(store.stored.records.is_empty());
            assert_eq!(store.stored.pairing[0].status.state, State::Pending);
            assert!(store.stored.pairing[0].status.completion.is_none());
        }
    }

    #[test]
    fn completion_request_ids_are_scoped_by_owner_and_retained_after_restart() {
        for first_confirm in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let mut first = pending(&mut store, "first-owner", t);
            first.request_id = "shared-request".into();
            let reject = |request: &ConfirmRequest| RejectRequest {
                owner: request.owner.clone(),
                attempt_id: request.attempt_id.clone(),
                expected_node_id: request.expected_node_id,
                expected_attempt_revision: request.expected_attempt_revision,
                request_id: request.request_id.clone(),
            };
            if first_confirm {
                confirm(&mut store, &first, t);
            } else {
                store.reject_pairing_at(&reject(&first), t).unwrap();
            }
            let mut second = pending_owned(&mut store, "second-owner", t, "other");
            second.request_id = first.request_id.clone();
            let second_reply = if first_confirm {
                store.reject_pairing_at(&reject(&second), t).unwrap()
            } else {
                confirm(&mut store, &second, t)
            };
            let second_receipt = second_reply.status.unwrap().completion.unwrap();
            store.validate_stored().unwrap();
            let disk = fs::read(&store.path).unwrap();
            store = open(&dir);
            let replay = if first_confirm {
                store.reject_pairing_at(&reject(&second), t + 1).unwrap()
            } else {
                let prepared = store.prepare_pairing_confirmation_at(&second, t + 1).unwrap();
                assert!(prepared.replay);
                assert_eq!(prepared.replay_reply.as_ref().unwrap().status.as_ref()
                    .unwrap().completion.as_ref(), Some(&second_receipt));
                store.commit_pairing_confirmation_at(prepared, t + 1).unwrap()
            };
            assert_eq!(replay.status.unwrap().completion.as_ref(), Some(&second_receipt));
            assert_eq!(fs::read(&store.path).unwrap(), disk);
            let first_replay = if first_confirm {
                confirm(&mut store, &first, t + 1)
            } else {
                store.reject_pairing_at(&reject(&first), t + 1).unwrap()
            };
            assert_eq!(first_replay.status.unwrap().completion.unwrap().attempt_id, first.attempt_id);
            let mut wrong_owner = first.clone();
            wrong_owner.owner = "unrelated".into();
            let error = store.prepare_pairing_confirmation_at(&wrong_owner, t + 1).err().unwrap();
            assert_ne!(crate::error::code_of(&error), ErrorCode::RequestIdConflict);
        }
    }

    #[test]
    fn independent_pairing_receipt_survives_full_runtime_receipt_pool() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "capacity", t);
        // Populate valid retained history without 1,024 unrelated fsyncs.
        for index in 0..MAX_RECEIPTS {
            store.stored.receipts.push(Receipt {
                request_id: format!("runtime-{index}"),
                digest: blake3::hash(b"history").to_hex().to_string(),
                committed_revision: 0,
                created: t,
                affected: vec![],
                removals: vec![],
                cancellation: Cancellation::empty(),
            });
        }
        store.validate_stored().unwrap();
        let reply = confirm(&mut store, &request, t);
        assert_eq!(reply.status.unwrap().state, State::Confirmed);
        assert_eq!(store.stored.receipts.len(), MAX_RECEIPTS);
        assert_eq!(open(&dir).stored.receipts.len(), MAX_RECEIPTS);
    }

    #[test]
    fn completion_validation_rejects_corrupt_or_duplicate_history() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "validation", t);
        confirm(&mut store, &request, t);
        let original = store.stored.pairing[0].clone();
        for mutation in 0..6 {
            let mut corrupt = original.clone();
            match mutation {
                0 => corrupt.request_digest = None,
                1 => corrupt.status.completion = None,
                2 => corrupt.status.completion.as_mut().unwrap().node_id = node(2),
                3 => corrupt.status.completion.as_mut().unwrap().committed_at += 1,
                4 => corrupt.status.completion.as_mut().unwrap().outcome = State::Rejected,
                _ => corrupt.status.completion.as_mut().unwrap().policy_digest = "different".into(),
            }
            assert!(validate(&[corrupt]).is_err());
        }
        let mut duplicate = original.clone();
        duplicate.status.attempt_id = "another".into();
        duplicate.status.creation_id = "another".into();
        duplicate.status.completion.as_mut().unwrap().attempt_id = "another".into();
        assert!(validate(&[original, duplicate]).is_err());
    }

    #[test]
    fn residual_static_access_and_provenance_survive_removal_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "static-residual", t);
        let receipt = confirm(&mut store, &request, t)
            .status
            .unwrap()
            .completion
            .unwrap();
        let static_text = format!(
            "format = 2\n[[peers]]\nid = \"{}\"\nname = \"paired\"\ninbound_only = true\nallow = [\"residual\"]\n",
            node(1)
        );
        fs::write(&store.static_input.path, &static_text).unwrap();
        let input = StaticInput::load(&FabricHome::new(dir.path())).unwrap();
        store.reload_static(input).unwrap();
        remove(&mut store, "remove-runtime");
        for restart in [false, true] {
            if restart {
                store = open(&dir);
            }
            let reply = confirm(&mut store, &request, t + 1);
            assert_eq!(reply.status.unwrap().completion.as_ref(), Some(&receipt));
            assert_eq!(reply.access.len(), 1);
            assert_eq!(
                reply.access[0].services.keys().cloned().collect::<Vec<_>>(),
                vec!["residual".to_string()]
            );
            assert!(
                reply.access[0]
                    .trust_sources
                    .iter()
                    .all(|source| matches!(source, Source::Static { .. }))
            );
            assert!(
                reply.access[0].services["residual"]
                    .iter()
                    .all(|source| matches!(source, Source::Static { .. }))
            );
            assert!(store.stored.records.is_empty());
        }
    }

    #[test]
    fn cancellation_metadata_updates_survive_prepared_confirmation() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "cancellation-metadata", t);
        store.stored.receipts.push(Receipt {
            request_id: "earlier".into(),
            digest: blake3::hash(b"earlier").to_hex().to_string(),
            committed_revision: 0,
            created: t,
            affected: vec![],
            removals: vec![],
            cancellation: Cancellation::default(),
        });
        let prepared = store.prepare_pairing_confirmation_at(&request, t).unwrap();
        store
            .update_cancellation("earlier", Cancellation::empty())
            .unwrap();
        store.commit_pairing_confirmation_at(prepared, t).unwrap();
        assert_eq!(
            store.receipt("earlier").unwrap().cancellation.state,
            CancellationState::Complete
        );
        assert_eq!(
            open(&dir).receipt("earlier").unwrap().cancellation.state,
            CancellationState::Complete
        );
    }

    #[test]
    fn strict_completion_wire_requests_refuse_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let request = pending(&mut store, "wire", now());
        let mut value = serde_json::to_value(&request).unwrap();
        value["extra"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ConfirmRequest>(value).is_err());
        let wire = Request::Confirm {
            confirmation: request,
        };
        let mut value = serde_json::to_value(wire).unwrap();
        assert!(serde_json::from_value::<Request>(value.clone()).is_ok());
        value["extra"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Request>(value).is_err());
    }

    #[test]
    fn rejected_after_preparation_cannot_commit_and_reject_selectors_are_strict() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "prepared-rejection", t);
        let prepared = store.prepare_pairing_confirmation_at(&request, t).unwrap();
        let rejection = RejectRequest {
            owner: request.owner.clone(),
            attempt_id: request.attempt_id.clone(),
            expected_node_id: node(1),
            expected_attempt_revision: 2,
            request_id: "reject-prepared".into(),
        };
        for selector in 0..3 {
            let mut bad = rejection.clone();
            match selector {
                0 => bad.owner = "other".into(),
                1 => bad.expected_node_id = node(2),
                _ => bad.expected_attempt_revision = 1,
            }
            assert!(
                store
                    .pairing_request_at(Request::Reject { rejection: bad }, t)
                    .is_err()
            );
            assert_eq!(store.stored.pairing[0].status.state, State::Pending);
            assert!(!store.exchanges.is_empty());
        }
        store
            .pairing_request_at(Request::Reject { rejection }, t)
            .unwrap();
        assert!(store.commit_pairing_confirmation_at(prepared, t).is_err());
        assert!(store.snapshot(None).unwrap().peers.is_empty());
        assert!(store.exchanges.is_empty());
    }

    #[test]
    fn record_capacity_refuses_confirmation_without_consuming_reserved_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "record-limit", t);
        for index in 0..MAX_RECORDS {
            store.stored.records.push(Record {
                owner: format!("owner-{index}"),
                expires_at: None,
                expiry_request_id: None,
                peer: Peer {
                    id: node(2),
                    name: None,
                    addr: None,
                    roaming: false,
                    dial_only: false,
                    inbound_only: false,
                    allow: vec![],
                },
            });
        }
        store.validate_stored().unwrap();
        assert!(store.prepare_pairing_confirmation_at(&request, t).is_err());
        assert_eq!(store.stored.pairing[0].status.state, State::Pending);
        assert!(store.stored.pairing[0].status.completion.is_none());
        assert_eq!(store.stored.revision, 0);
    }

    #[test]
    fn stored_completion_revision_bounds_are_enforced() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(&dir);
        let t = now();
        let request = pending(&mut store, "revision-bound", t);
        confirm(&mut store, &request, t);
        store.stored.pairing[0]
            .status
            .completion
            .as_mut()
            .unwrap()
            .policy_revision = 0;
        assert!(store.validate_stored().is_err());
        store.stored.pairing[0]
            .status
            .completion
            .as_mut()
            .unwrap()
            .policy_revision = 2;
        assert!(store.validate_stored().is_err());
    }

    #[test]
    fn completion_request_id_reuse_at_retention_boundary_prunes_atomically() {
        for reject in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = open(&dir);
            let t = now();
            let original = pending(&mut store, "old-receipt", t);
            let old_rejection = RejectRequest {
                owner: original.owner.clone(),
                attempt_id: original.attempt_id.clone(),
                expected_node_id: original.expected_node_id,
                expected_attempt_revision: original.expected_attempt_revision,
                request_id: "reusable".into(),
            };
            store.reject_pairing_at(&old_rejection, t).unwrap();
            let boundary = t + RECEIPT_SECONDS;
            let mut request = pending(&mut store, "new-receipt", boundary - 1);
            request.request_id = "reusable".into();
            assert_eq!(store.stored.pairing.len(), 2);
            assert!(
                store
                    .stored
                    .pairing
                    .iter()
                    .any(|attempt| { attempt.status.attempt_id == original.attempt_id })
            );
            let reply = if reject {
                store
                    .reject_pairing_at(
                        &RejectRequest {
                            owner: request.owner.clone(),
                            attempt_id: request.attempt_id.clone(),
                            expected_node_id: request.expected_node_id,
                            expected_attempt_revision: request.expected_attempt_revision,
                            request_id: request.request_id.clone(),
                        },
                        boundary,
                    )
                    .unwrap()
            } else {
                let prepared = store
                    .prepare_pairing_confirmation_at(&request, boundary)
                    .unwrap();
                store
                    .commit_pairing_confirmation_at(prepared, boundary)
                    .unwrap()
            };
            let receipt = reply.status.unwrap().completion.unwrap();
            assert_eq!(receipt.request_id, "reusable");
            assert_eq!(receipt.attempt_id, request.attempt_id);
            assert_eq!(store.stored.pairing.len(), 1);
            store.validate_stored().unwrap();
            let restarted = open(&dir);
            assert_eq!(restarted.stored.pairing.len(), 1);
            assert_eq!(
                restarted.stored.pairing[0].status.completion.as_ref(),
                Some(&receipt)
            );
            assert_eq!(
                restarted.snapshot(None).unwrap().peers.len(),
                usize::from(!reject)
            );
        }
    }
}
