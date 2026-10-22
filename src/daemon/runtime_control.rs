//! Policy commits run off the executor while the one policy generation is held.
use super::*;
use crate::runtime_peers::{self, Cancellation, CancellationState, PolicyError, Reply, Request};

async fn with_runtime<T: Send + 'static>(
    runtime: Arc<std::sync::Mutex<runtime_peers::RuntimePeers>>,
    access: impl FnOnce(&mut runtime_peers::RuntimePeers) -> Result<T> + Send + 'static,
) -> Result<T> {
    // Completion persistence uses this same lock. Even a read must acquire it
    // off-thread so a slow fsync cannot park an executor worker.
    tokio::task::spawn_blocking(move || {
        access(&mut runtime.lock().unwrap_or_else(|poison| poison.into_inner()))
    })
    .await?
}

#[derive(Debug, Clone)]
pub(super) struct Revocations {
    admissions: Vec<crate::admission::Revocation>,
    sessions: Vec<tunnel::SessionRevocation>,
}
impl Revocations {
    fn pending(&self) -> Cancellation {
        let admissions: Vec<_> = self.admissions.iter().map(|entry| entry.id).collect();
        let sessions: Vec<_> = self.sessions.iter().map(|entry| entry.id.clone()).collect();
        Cancellation {
            state: if admissions.is_empty() && sessions.is_empty() {
                CancellationState::Complete
            } else {
                CancellationState::Pending
            },
            remaining_admissions: admissions.clone(),
            remaining_sessions: sessions.clone(),
            admissions,
            sessions,
            restart_evidence: None,
        }
    }
    async fn status(&self, timed_out: bool) -> Cancellation {
        let mut status = self.pending();
        status.remaining_admissions.retain(|id| {
            self.admissions
                .iter()
                .any(|entry| entry.id == *id && !entry.complete())
        });
        status.remaining_sessions.clear();
        for session in &self.sessions {
            if !session.complete().await {
                status.remaining_sessions.push(session.id.clone());
            }
        }
        status.state =
            if status.remaining_admissions.is_empty() && status.remaining_sessions.is_empty() {
                CancellationState::Complete
            } else if timed_out {
                CancellationState::TimedOut
            } else {
                CancellationState::Pending
            };
        status
    }
}

pub(super) enum ConfigUpdate {
    UpsertExpose(PersistedExpose),
    RemoveExpose(String),
    UpsertDial(PersistedDial),
    RemoveDial { peer: String, protocol: String },
}
impl ConfigUpdate {
    pub fn apply(self, config: &mut FabricConfig) {
        match self {
            Self::UpsertExpose(expose) => config.upsert_expose(expose),
            Self::RemoveExpose(protocol) => {
                config.remove_expose(&protocol);
            }
            Self::UpsertDial(dial) => config.upsert_dial(dial),
            Self::RemoveDial { peer, protocol } => {
                config.remove_dial(&peer, &protocol);
            }
        }
    }
}

impl DaemonState {
    pub(super) async fn drain_policy_commits(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), self.wait_policy_commits())
            .await
            .map_err(|_| {
                anyhow::anyhow!("policy/config commits did not drain within three seconds")
            })?
    }

    async fn wait_policy_commits(&self) -> Result<()> {
        let _policy = self.policy_update.lock().await;
        let config = self.config_write.clone();
        let runtime = self.runtime_peers.clone();
        tokio::task::spawn_blocking(move || {
            drop(config.lock().unwrap_or_else(|poison| poison.into_inner()));
            if let Some(runtime) = runtime {
                drop(runtime.lock().unwrap_or_else(|poison| poison.into_inner()));
            }
        })
        .await?;
        Ok(())
    }

    async fn select_runtime_revocations(&self, book: &PeerBook) -> Revocations {
        Revocations {
            admissions: self.admissions.select_withdrawn(book),
            sessions: self.tunnel_sessions.select_withdrawn(book).await,
        }
    }
    async fn signal_runtime_revocations(&self, selected: &Revocations) {
        for admission in &selected.admissions {
            admission.cancel();
        }
        self.tunnel_sessions
            .revoke_selected(&selected.sessions)
            .await;
    }
    fn monitor_runtime_revocations(
        &self,
        runtime: Arc<std::sync::Mutex<runtime_peers::RuntimePeers>>,
        request_id: Option<String>,
        selected: Revocations,
    ) {
        if selected.admissions.is_empty() && selected.sessions.is_empty() {
            return;
        }
        // Retain timed-out detached work until it actually ends. It has been
        // removed from the session store, but reconciliation must still wait.
        let selected = Arc::new(selected);
        let pending = self.pending_policy_revocations.clone();
        pending.lock().unwrap().push(selected.clone());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut reported_timeout = false;
            loop {
                let mut status = selected
                    .status(tokio::time::Instant::now() >= deadline)
                    .await;
                let complete = status.state == CancellationState::Complete;
                let first_timeout =
                    status.state == CancellationState::TimedOut && !reported_timeout;
                if first_timeout {
                    reported_timeout = true;
                    warn!(event = "policy_cancellation_timeout", code = "cancellation_timeout", ?request_id, admissions = ?status.remaining_admissions, sessions = ?status.remaining_sessions, "revocation did not finish within two seconds");
                }
                // Keep the missed bound visible even after the remaining
                // work ends; empty remaining IDs then prove late completion.
                if complete && reported_timeout {
                    status.state = CancellationState::TimedOut;
                }
                if complete || first_timeout {
                    if let Some(request_id) = request_id.clone() {
                        let runtime = runtime.clone();
                        let cancel = cancel.clone();
                        let _ = tokio::task::spawn_blocking(move || {
                            let mut runtime = runtime.lock().unwrap_or_else(|poison| poison.into_inner());
                            if cancel.is_cancelled() { return; }
                            if let Err(error) = runtime.update_cancellation(&request_id, status) {
                                warn!(?request_id, error = %error, "could not persist cancellation completion; retained previous durable receipt");
                            }
                        }).await;
                    }
                }
                if complete {
                    pending
                        .lock()
                        .unwrap()
                        .retain(|entry| !Arc::ptr_eq(entry, &selected));
                    break;
                }
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(if reported_timeout { 100 } else { 10 })) => {}
                }
            }
        });
    }

    async fn quarantine_runtime_policy(
        &self,
        published: &mut PeerBook,
        error: &anyhow::Error,
        cancel_existing: bool,
    ) {
        *published = PeerBook::default();
        if let Some(runtime) = &self.runtime_peers {
            let fault = format!("{error:#}");
            let _ = with_runtime(runtime.clone(), move |runtime| {
                runtime.set_fault(&anyhow::anyhow!(fault));
                Ok(())
            })
            .await;
        }
        // A failed static edit closes new admissions without destroying work
        // admitted by the last accepted policy. Unknown committed state is the
        // only fault that must also cancel that work before reconciliation.
        let mut selected = if cancel_existing {
            self.select_runtime_revocations(published).await
        } else {
            Revocations {
                admissions: vec![],
                sessions: vec![],
            }
        };
        for previous in self.pending_policy_revocations.lock().unwrap().iter() {
            selected
                .admissions
                .extend(previous.admissions.iter().cloned());
            selected.sessions.extend(previous.sessions.iter().cloned());
        }
        selected.admissions.sort_by_key(|entry| entry.id);
        selected.admissions.dedup_by_key(|entry| entry.id);
        selected.sessions.sort_by(|a, b| a.id.cmp(&b.id));
        selected.sessions.dedup_by(|a, b| a.id == b.id);
        if cancel_existing {
            self.signal_runtime_revocations(&selected).await;
        }
        *self.policy_quarantine.lock().await = Some(selected);
    }

    pub(crate) async fn pairing_request(
        &self,
        request: runtime_peers::pairing::Request,
    ) -> runtime_peers::pairing::Reply {
        if matches!(&request, runtime_peers::pairing::Request::Query { .. }) {
            return self
                .read_pairing(move |runtime| runtime.pairing_request(request))
                .await;
        }
        self.commit_pairing(move |runtime| runtime.pairing_request(request))
            .await
    }

    pub(crate) async fn pairing_network_request(
        &self,
        node: EndpointId,
        request: runtime_peers::pairing::NetworkRequest,
    ) -> runtime_peers::pairing::Reply {
        let checked = self.pairing_network_read(node, &request).await;
        match checked {
            Ok(Some(reply)) => reply,
            Ok(None) => {
                self.commit_pairing(move |runtime| runtime.pairing_network_request(node, request))
                    .await
            }
            Err(error) => runtime_peers::pairing::Reply {
                status: None,
                secret: None,
                error: Some(PolicyError::of(&error)),
            },
        }
    }

    pub(super) async fn pairing_network_read(
        &self,
        node: EndpointId,
        request: &runtime_peers::pairing::NetworkRequest,
    ) -> Result<Option<runtime_peers::pairing::Reply>> {
        let request = request.clone();
        self.read_pairing_result(move |runtime| runtime.pairing_network_read(node, &request))
            .await
    }

    async fn read_pairing_result<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut runtime_peers::RuntimePeers) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let runtime = self.runtime_peers.as_ref().ok_or_else(|| {
            FabricError::new(ErrorCode::RuntimePeersDisabled, "pairing requires --runtime-peers")
        })?;
        with_runtime(runtime.clone(), operation).await
    }

    async fn read_pairing(
        &self,
        operation: impl FnOnce(&mut runtime_peers::RuntimePeers) -> Result<runtime_peers::pairing::Reply>
            + Send + 'static,
    ) -> runtime_peers::pairing::Reply {
        self.read_pairing_result(operation).await.unwrap_or_else(|error| {
            runtime_peers::pairing::Reply {
                status: None,
                secret: None,
                error: Some(PolicyError::of(&error)),
            }
        })
    }

    async fn commit_pairing(
        &self,
        operation: impl FnOnce(
            &mut runtime_peers::RuntimePeers,
        ) -> Result<runtime_peers::pairing::Reply>
        + Send
        + 'static,
    ) -> runtime_peers::pairing::Reply {
        use runtime_peers::pairing::Reply;
        let result = async {
            let runtime = self.runtime_peers.as_ref().ok_or_else(|| {
                FabricError::new(
                    ErrorCode::RuntimePeersDisabled,
                    "pairing requires --runtime-peers",
                )
            })?;
            let _serial = self.policy_update.lock().await;
            if self.cancel.is_cancelled() {
                return Err(
                    FabricError::new(ErrorCode::DaemonNotRunning, "daemon is stopping").into(),
                );
            }
            let runtime = runtime.clone();
            let result = tokio::task::spawn_blocking(move || {
                operation(&mut runtime.lock().unwrap_or_else(|poison| poison.into_inner()))
            })
            .await
            .unwrap_or_else(|error| {
                Err(FabricError::new(
                    ErrorCode::RuntimeCommitIndeterminate,
                    format!("pairing commit worker failed: {error}"),
                )
                .into())
            });
            if let Err(error) = &result {
                if crate::error::code_of(error) == ErrorCode::RuntimeCommitIndeterminate {
                    let mut published = self.peer_book.write().await;
                    self.quarantine_runtime_policy(&mut published, error, true)
                        .await;
                }
            }
            result
        }
        .await;
        result.unwrap_or_else(|error| Reply {
            status: None,
            secret: None,
            error: Some(PolicyError::of(&error)),
        })
    }

    pub(crate) async fn runtime_peer_request(&self, request: Request) -> Reply {
        let Some(runtime) = &self.runtime_peers else {
            return Reply {
                snapshot: None,
                mutation: None,
                affected: vec![],
                error: Some(PolicyError::of(
                    &FabricError::new(
                        ErrorCode::RuntimePeersDisabled,
                        "start the daemon with --runtime-peers to use runtime policy requests",
                    )
                    .into(),
                )),
            };
        };
        let _serial = self.policy_update.lock().await;
        if self.cancel.is_cancelled() {
            return Reply {
                snapshot: None,
                mutation: None,
                affected: vec![],
                error: Some(PolicyError::of(
                    &FabricError::new(ErrorCode::DaemonNotRunning, "daemon is stopping").into(),
                )),
            };
        }
        let affected = match &request {
            Request::Batch { transaction } => transaction
                .operations
                .iter()
                .map(|op| op.node_id())
                .collect::<Vec<_>>(),
            Request::AddPeer { node_id, .. }
            | Request::SetGrants { node_id, .. }
            | Request::RemovePeer { node_id, .. } => vec![*node_id],
            _ => vec![],
        };
        let result = match request {
            Request::List { cursor } => with_runtime(runtime.clone(), move |runtime| {
                runtime.snapshot(cursor.as_ref())
            })
            .await
            .map(|snapshot| Reply {
                snapshot: Some(snapshot),
                mutation: None,
                affected: vec![],
                error: None,
            }),
            Request::Cancellation { request_id } => {
                with_runtime(runtime.clone(), move |runtime| runtime.receipt(&request_id))
                    .await
                    .map(|mutation| Reply {
                        snapshot: None,
                        affected: mutation.affected.clone(),
                        mutation: Some(mutation),
                        error: None,
                    })
            }
            request => self
                .commit_runtime_transaction(
                    runtime.clone(),
                    request.into_transaction().expect("mutation request"),
                )
                .await
                .map(|mutation| Reply {
                    snapshot: None,
                    affected: mutation.affected.clone(),
                    mutation: Some(mutation),
                    error: None,
                }),
        };
        match result {
            Ok(mut reply) => {
                if let Some(receipt) = reply
                    .mutation
                    .as_ref()
                    .filter(|receipt| receipt.cancellation.state == CancellationState::TimedOut)
                {
                    let message = if receipt.cancellation.restart_evidence.is_some() {
                        "policy committed, but the daemon instance changed and child exit was not observed; inspect remaining IDs and restart_evidence"
                    } else {
                        "policy committed, but selected work did not finish within two seconds; inspect remaining IDs"
                    };
                    reply.error = Some(PolicyError::of(
                        &FabricError::new(ErrorCode::CancellationTimeout, message).into(),
                    ));
                }
                reply
            }
            Err(error) => {
                let (snapshot, affected) = with_runtime(runtime.clone(), move |runtime| {
                    Ok((runtime.snapshot(None).ok(), runtime.access_for(&affected)))
                })
                .await
                .unwrap_or_default();
                Reply {
                    snapshot,
                    mutation: None,
                    affected,
                    error: Some(PolicyError::of(&error)),
                }
            }
        }
    }
    async fn commit_runtime_transaction(
        &self,
        runtime: Arc<std::sync::Mutex<runtime_peers::RuntimePeers>>,
        transaction: runtime_peers::Transaction,
    ) -> Result<runtime_peers::MutationReply> {
        let prepared = {
            let runtime = runtime.clone();
            let transaction = transaction.clone();
            let home = self.home.clone();
            tokio::task::spawn_blocking(move || {
                let prepared = runtime
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .prepare(&transaction)?;
                if !prepared.replay && prepared.no_op.is_none() {
                    SyncBook::load(&home)?.validate_against(&prepared.book)?;
                }
                Ok::<_, anyhow::Error>(prepared)
            })
            .await??
        };
        if prepared.replay {
            return with_runtime(runtime, move |runtime| {
                runtime.receipt(&transaction.request_id)
            })
            .await;
        }
        if let Some(reply) = prepared.no_op.clone() {
            return Ok(reply);
        }
        // Preparation and sync-file I/O happen before this barrier. Policy
        // writers are serialized; commit also rechecks the prepared revision.
        // Admissions see one durable, published generation.
        let mut published = self.peer_book.write().await;
        let selected = self.select_runtime_revocations(&prepared.book).await;
        let cancellation = selected.pending();
        let book = prepared.book.clone();
        let receipt = {
            let runtime = runtime.clone();
            let request_id = transaction.request_id.clone();
            tokio::task::spawn_blocking(move || {
                runtime
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .commit(prepared, &request_id, cancellation)
            })
            .await
            .unwrap_or_else(|error| {
                Err(FabricError::new(
                    ErrorCode::RuntimeCommitIndeterminate,
                    format!("runtime commit worker failed: {error}"),
                )
                .into())
            })
        };
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(error) => {
                if crate::error::code_of(&error) == ErrorCode::RuntimeCommitIndeterminate {
                    self.quarantine_runtime_policy(&mut published, &error, true)
                        .await;
                }
                return Err(error);
            }
        };
        *published = book;
        self.signal_runtime_revocations(&selected).await;
        drop(published);
        self.monitor_runtime_revocations(runtime, Some(transaction.request_id), selected);
        self.peer_connections.presence.retry_now();
        Ok(receipt)
    }

    pub(super) async fn reload_runtime_peers(&self) -> Result<()> {
        let _serial = self.policy_update.lock().await;
        if self.cancel.is_cancelled() {
            return Err(FabricError::new(ErrorCode::DaemonNotRunning, "daemon is stopping").into());
        }
        let runtime = self.runtime_peers.as_ref().expect("runtime mode").clone();
        if with_runtime(runtime.clone(), |runtime| Ok(runtime.fault().is_some())).await? {
            if let Some(selected) = self.policy_quarantine.lock().await.as_ref() {
                if selected.status(false).await.state != CancellationState::Complete {
                    return Err(FabricError::new(ErrorCode::RuntimeStateUnavailable, "quarantined work has not finished; retry reconciliation after cancellation").into());
                }
            }
            let home = self.home.clone();
            let store = runtime.clone();
            let result = tokio::task::spawn_blocking(move || {
                store
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .reconcile(&home)
            })
            .await?;
            let book = result?;
            let mut published = self.peer_book.write().await;
            let selected = self.select_runtime_revocations(&book).await;
            *published = book;
            self.signal_runtime_revocations(&selected).await;
            *self.policy_quarantine.lock().await = None;
            drop(published);
            self.monitor_runtime_revocations(runtime, None, selected);
            self.peer_connections.presence.retry_now();
            return Ok(());
        }
        let home = self.home.clone();
        let input =
            match tokio::task::spawn_blocking(move || runtime_peers::StaticInput::load(&home))
                .await
                .unwrap_or_else(|error| Err(error.into()))
            {
                Ok(input) => input,
                Err(error) => {
                    let mut published = self.peer_book.write().await;
                    self.quarantine_runtime_policy(&mut published, &error, false)
                        .await;
                    return Err(error);
                }
            };
        let preview = input.clone();
        let candidate = match with_runtime(runtime.clone(), move |runtime| {
            runtime.reload_candidate(preview)
        })
        .await
        {
            Ok(candidate) => candidate,
            Err(error) => {
                let mut published = self.peer_book.write().await;
                self.quarantine_runtime_policy(&mut published, &error, false)
                    .await;
                return Err(error);
            }
        };
        let home = self.home.clone();
        let validation = candidate.clone();
        let validation = tokio::task::spawn_blocking(move || {
            SyncBook::load(&home)?.validate_against(&validation)
        })
        .await
        .unwrap_or_else(|error| Err(error.into()));
        if let Err(error) = validation {
            let mut published = self.peer_book.write().await;
            self.quarantine_runtime_policy(&mut published, &error, false)
                .await;
            return Err(error);
        }
        let mut published = self.peer_book.write().await;
        let selected = self.select_runtime_revocations(&candidate).await;
        let store = runtime.clone();
        let result = tokio::task::spawn_blocking(move || {
            store
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .reload_static(input)
        })
        .await
        .unwrap_or_else(|error| {
            Err(FabricError::new(
                ErrorCode::RuntimeCommitIndeterminate,
                format!("static policy commit worker failed: {error}"),
            )
            .into())
        });
        match result {
            Ok(book) => *published = book,
            Err(error) => {
                let indeterminate =
                    crate::error::code_of(&error) == ErrorCode::RuntimeCommitIndeterminate;
                self.quarantine_runtime_policy(&mut published, &error, indeterminate)
                    .await;
                if !indeterminate {
                    // Validation succeeded: honor exactly this candidate's
                    // withdrawals even though its durable revision failed.
                    self.signal_runtime_revocations(&selected).await;
                    let mut quarantine = self.policy_quarantine.lock().await;
                    if let Some(quarantine) = quarantine.as_mut() {
                        quarantine
                            .admissions
                            .extend(selected.admissions.iter().cloned());
                        quarantine
                            .sessions
                            .extend(selected.sessions.iter().cloned());
                    }
                    drop(quarantine);
                    self.monitor_runtime_revocations(runtime, None, selected);
                }
                return Err(error);
            }
        }
        self.signal_runtime_revocations(&selected).await;
        drop(published);
        self.monitor_runtime_revocations(runtime, None, selected);
        self.peer_connections.presence.retry_now();
        self.announce_to_peers().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        daemon::{FabricNode, send_control},
        runtime_peers::{Direction, Operation, Transaction},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    static SERIAL: Mutex<()> = Mutex::const_new(());
    fn options() -> DaemonOptions {
        DaemonOptions {
            runtime_peers: true,
            ..DaemonOptions::default()
        }
    }
    fn transaction(
        owner: &str,
        request_id: &str,
        expected_revision: u64,
        operations: Vec<Operation>,
    ) -> Request {
        Request::Batch {
            transaction: Transaction {
                owner: owner.into(),
                request_id: request_id.into(),
                expected_revision,
                operations,
            },
        }
    }
    fn add(node: &FabricNode, name: &str) -> Operation {
        Operation::AddPeer {
            node_id: node.id(),
            name: Some(name.into()),
            direction: Some(Direction::InboundOnly),
            addr: Some(node.addr()),
        }
    }
    fn grant(node: &FabricNode, services: &[&str]) -> Operation {
        Operation::SetGrants {
            node_id: node.id(),
            grants: services.iter().map(|s| s.to_string()).collect(),
        }
    }
    async fn request(home: &FabricHome, request: Request) -> Result<Reply> {
        // FabricNode starts its serving task asynchronously. Wait for the
        // fixture's control listener, without retrying a policy mutation.
        tokio::time::timeout(Duration::from_secs(3), async {
            while !home.control_socket_path().exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        match send_control(home, ControlRequest::RuntimePeers { request }).await? {
            ControlResponse::RuntimePeers { reply } => Ok(reply),
            other => bail!("unexpected policy reply {other:?}"),
        }
    }
    async fn mutate(
        home: &FabricHome,
        request_id: &str,
        revision: u64,
        operations: Vec<Operation>,
    ) -> Result<runtime_peers::MutationReply> {
        let reply = request(
            home,
            transaction("controller", request_id, revision, operations),
        )
        .await?;
        if let Some(error) = reply.error {
            bail!("{error:?}");
        }
        reply.mutation.context("missing mutation receipt")
    }
    async fn trust_server(
        client_home: &FabricHome,
        client: &FabricNode,
        server: &FabricNode,
    ) -> Result<()> {
        let mut peers = PeerBook::load(client_home)?;
        peers.add(server.id(), Some("server".into()), Some(server.addr()));
        peers.save(client_home)?;
        client.state().reload_peers().await
    }
    async fn echo_server() -> Result<(String, JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        Ok((
            addr,
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        break;
                    };
                    tokio::spawn(async move {
                        let (mut read, mut write) = stream.split();
                        let _ = tokio::io::copy(&mut read, &mut write).await;
                    });
                }
            }),
        ))
    }
    async fn round_trip(stream: &mut tokio::net::TcpStream, bytes: &[u8]) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), async {
            stream.write_all(bytes).await?;
            let mut reply = vec![0; bytes.len()];
            stream.read_exact(&mut reply).await?;
            assert_eq!(reply, bytes);
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }
    async fn cancellation(
        home: &FabricHome,
        request_id: &str,
    ) -> Result<runtime_peers::MutationReply> {
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let reply = request(
                    home,
                    Request::Cancellation {
                        request_id: request_id.into(),
                    },
                )
                .await?;
                let result = reply.mutation.context("missing cancellation receipt")?;
                if result.cancellation.state != CancellationState::Pending {
                    return Ok::<_, anyhow::Error>(result);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?
    }

    #[tokio::test]
    async fn pairing_network_reads_and_invalid_credentials_bypass_admission_barriers() -> Result<()> {
        use runtime_peers::pairing::{NetworkRequest, Policy, Request as PairRequest};
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let node = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let state = node.state();
        let created = state.pairing_request(PairRequest::Issue {
            owner: "controller".into(),
            creation_id: "read-barrier".into(),
            deadline: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?.as_secs() + 600,
            policy: Policy {
                grants: vec!["echo".into()],
                name: None,
                direction: Direction::Normal,
            },
        }).await;
        let attempt_id = created.status.context("issued status")?.attempt_id;
        let secret = created.secret.context("issued secret")?;
        let peer = iroh::SecretKey::from_bytes(&[79; 32]).public();
        let bound = state.pairing_network_request(peer, NetworkRequest::Present {
            attempt_id: attempt_id.clone(), secret,
        }).await;
        assert!(bound.error.is_none());
        let before = fs::read(home.root().join("runtime-peers.toml"))?;
        let _policy = state.policy_update.lock().await;
        let _publication = state.peer_book.write().await;
        for (id, expected_error) in [(attempt_id, false), ("unknown".into(), true)] {
            let reply = tokio::time::timeout(
                Duration::from_secs(1),
                state.pairing_network_request(peer, NetworkRequest::Status { attempt_id: id }),
            ).await?;
            assert_eq!(reply.error.is_some(), expected_error);
        }
        assert_eq!(before, fs::read(home.root().join("runtime-peers.toml"))?);
        assert!(state.policy_quarantine.lock().await.is_none());
        drop(_publication);
        drop(_policy);
        node.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn pairing_query_bypasses_policy_and_publication_barriers() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let node = FabricNode::start_with_daemon_options(home, options()).await?;
        let state = node.state();
        let _policy = state.policy_update.lock().await;
        let _publication = state.peer_book.write().await;
        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            state.pairing_request(runtime_peers::pairing::Request::Query {
                owner: "controller".into(),
                creation_id: "unknown".into(),
            }),
        )
        .await?;
        assert!(reply.error.is_none());
        assert!(reply.status.is_none());
        assert!(state.policy_quarantine.lock().await.is_none());
        drop(_publication);
        drop(_policy);
        node.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn runtime_grants_survive_restart_and_read_only_static_input_is_untouched() -> Result<()>
    {
        let _serial = SERIAL.lock().await;
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let target = server_dir.path().join("static-input");
        let raw = "format = 2\n";
        fs::write(&target, raw)?;
        std::os::unix::fs::symlink(&target, home.peers_path())?;
        let mut permissions = fs::metadata(&target)?.permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&target, permissions)?;
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_server(&client_home, &client, &server).await?;
        let (echo, task) = echo_server().await?;
        server.expose_tcp("work/0", echo.clone()).await?;
        server.expose_tcp("other/0", echo).await?;
        let original = transaction(
            "controller",
            "install",
            0,
            vec![add(&client, "client"), grant(&client, &["work/0"])],
        );
        let reply = request(&home, original.clone())
            .await?
            .mutation
            .context("missing install receipt")?;
        assert_eq!(reply.committed_revision, 1);
        assert!(server.state().may(&client.id(), "echo").await.is_err());
        assert!(server.state().may(&client.id(), "other/0").await.is_err());
        let port = client
            .dial_tcp("server", "work/0", "127.0.0.1:0".into())
            .await?;
        round_trip(&mut tokio::net::TcpStream::connect(&port).await?, b"before")
            .await
            .context("runtime grant: readiness read before restart")?;
        let refused_port = client
            .dial_tcp("server", "other/0", "127.0.0.1:0".into())
            .await?;
        let mut refused = tokio::net::TcpStream::connect(refused_port).await?;
        refused.write_all(b"not-granted").await?;
        let mut byte = [0];
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), refused.read(&mut byte)).await?,
            Ok(0) | Err(_)
        ));
        mutate(
            &home,
            "rename",
            1,
            vec![Operation::AddPeer {
                node_id: client.id(),
                name: Some("renamed".into()),
                direction: None,
                addr: None,
            }],
        )
        .await?;
        client.shutdown().await?;
        server.shutdown().await?;
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_server(&client_home, &client, &server).await?;
        assert!(server.state().may(&client.id(), "work/0").await.is_ok());
        assert!(server.state().may(&client.id(), "echo").await.is_err());
        let port = client
            .dial_tcp("server", "work/0", "127.0.0.1:0".into())
            .await?;
        let mut live = tokio::net::TcpStream::connect(&port).await?;
        round_trip(&mut live, b"after")
            .await
            .context("runtime grant: readiness read after restart")?;
        let installed = mutate(&home, "withdraw", 2, vec![grant(&client, &[])]).await?;
        assert!(server.state().may(&client.id(), "work/0").await.is_err());
        let done = cancellation(&home, "withdraw").await?;
        assert_eq!(done.cancellation.state, CancellationState::Complete);
        assert!(!installed.cancellation.sessions.is_empty());
        let old = request(&home, original)
            .await?
            .mutation
            .context("missing replay receipt")?;
        assert_eq!((old.committed_revision, old.current_revision), (1, 3));
        assert!(
            old.affected[0].services.is_empty(),
            "receipt replay regranted access"
        );
        assert_eq!(fs::read_to_string(&target)?, raw);
        assert!(home.peers_path().is_symlink());
        client.shutdown().await?;
        server.shutdown().await?;
        task.abort();
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let snapshot = request(&home, Request::List { cursor: None })
            .await?
            .snapshot
            .unwrap();
        assert!(snapshot.peers[0].services.is_empty());
        assert_eq!(snapshot.peers[0].name.as_deref(), Some("renamed"));
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn runtime_revision_cas_is_serial_and_rejections_do_not_persist() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let one = iroh::SecretKey::from_bytes(&[71; 32]).public();
        let two = iroh::SecretKey::from_bytes(&[72; 32]).public();
        let first = Request::AddPeer {
            owner: "a".into(),
            request_id: "one".into(),
            expected_revision: 0,
            node_id: one,
            name: Some("one".into()),
            direction: Some(Direction::InboundOnly),
            addr: None,
        };
        let second = Request::AddPeer {
            owner: "b".into(),
            request_id: "two".into(),
            expected_revision: 0,
            node_id: two,
            name: Some("two".into()),
            direction: Some(Direction::InboundOnly),
            addr: None,
        };
        let (first, second) = tokio::join!(request(&home, first), request(&home, second));
        let replies = [first?, second?];
        assert_eq!(
            replies
                .iter()
                .filter(|reply| reply.mutation.is_some())
                .count(),
            1
        );
        assert_eq!(
            replies
                .iter()
                .filter_map(|r| r.error.as_ref())
                .next()
                .unwrap()
                .code,
            "policy_revision_conflict"
        );
        let snapshot = request(&home, Request::List { cursor: None })
            .await?
            .snapshot
            .unwrap();
        assert_eq!(snapshot.policy_revision, 1);
        assert_eq!(snapshot.peers.len(), 1);
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn mode_is_explicit_and_corruption_keeps_control_available() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let server = FabricNode::start(home.clone()).await?;
        assert_eq!(
            request(&home, Request::List { cursor: None })
                .await?
                .error
                .unwrap()
                .code,
            "runtime_peers_disabled"
        );
        assert!(!home.root().join("runtime-peers.toml").exists());
        server.shutdown().await?;
        fs::write(home.root().join("runtime-peers.toml"), "malformed = [")?;
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let reply = request(&home, Request::List { cursor: None }).await?;
        assert!(reply.snapshot.unwrap().fault.is_some());
        assert!(server.state().peer_book.read().await.peers().is_empty());
        // Repair requires explicit reload; the daemon must not invent an older policy.
        fs::remove_file(home.root().join("runtime-peers.toml"))?;
        server.state().reload_peers().await?;
        assert!(
            request(&home, Request::List { cursor: None })
                .await?
                .snapshot
                .unwrap()
                .fault
                .is_none()
        );
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn restart_reports_unconfirmed_completion_as_a_fault_with_evidence() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let (mut store, _) = runtime_peers::RuntimePeers::open(&home);
        let transaction = Transaction {
            owner: "controller".into(),
            request_id: "before-restart".into(),
            expected_revision: 0,
            operations: vec![Operation::AddPeer {
                node_id: iroh::SecretKey::from_bytes(&[81; 32]).public(),
                name: Some("phone".into()),
                direction: Some(Direction::InboundOnly),
                addr: None,
            }],
        };
        let prepared = store.prepare(&transaction)?;
        store.commit(
            prepared,
            &transaction.request_id,
            Cancellation {
                admissions: vec![17],
                remaining_admissions: vec![17],
                sessions: vec!["old-session".into()],
                remaining_sessions: vec!["old-session".into()],
                ..Cancellation::default()
            },
        )?;
        drop(store);
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        for _ in 0..2 {
            let reply = request(
                &home,
                Request::Cancellation {
                    request_id: transaction.request_id.clone(),
                },
            )
            .await?;
            let error = reply.error.unwrap();
            assert_eq!(error.code, "cancellation_timeout");
            assert!(error.message.contains("child exit was not observed"));
            let cancellation = reply.mutation.unwrap().cancellation;
            assert_eq!(cancellation.state, CancellationState::TimedOut);
            assert_eq!(cancellation.remaining_admissions, [17]);
            assert_eq!(cancellation.remaining_sessions, ["old-session"]);
            let evidence = cancellation.restart_evidence.unwrap();
            assert!(evidence.daemon_instance_changed);
            assert!(!evidence.child_exit_observed);
            server.state().reload_peers().await?;
        }
        server.shutdown().await
    }

    #[tokio::test]
    async fn withdrawal_ends_detached_exec_and_its_child_but_preserves_other_work() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_server(&client_home, &client, &server).await?;
        let pid_path = dir.path().join("owned-child.pid");
        server
            .expose_exec(
                "long/0",
                vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "echo $$ > \"$1\"; trap '' TERM; exec sleep 600".into(),
                    "child".into(),
                    pid_path.to_string_lossy().into_owned(),
                ],
            )
            .await?;
        let (echo, task) = echo_server().await?;
        server.expose_tcp("short/0", echo).await?;
        mutate(
            &home,
            "install",
            0,
            vec![
                add(&client, "client"),
                grant(&client, &["long/0", "short/0"]),
            ],
        )
        .await?;
        let short_port = client
            .dial_tcp("server", "short/0", "127.0.0.1:0".into())
            .await?;
        let mut short = tokio::net::TcpStream::connect(&short_port).await?;
        round_trip(&mut short, b"keep").await?;
        let long_port = client
            .dial_tcp("server", "long/0", "127.0.0.1:0".into())
            .await?;
        let _long = tokio::net::TcpStream::connect(long_port).await?;
        let pid = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(raw) = fs::read_to_string(&pid_path) {
                    if let Ok(pid) = raw.trim().parse::<i32>() {
                        break pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        send_control(&home, ControlRequest::SetTunnelBlocked { blocked: true }).await?;
        send_control(&home, ControlRequest::DropTunnelConnections).await?;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if server
                    .state()
                    .tunnel_sessions
                    .stats()
                    .await
                    .detached_sessions
                    >= 2
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let receipt = mutate(&home, "remove-long", 1, vec![grant(&client, &["short/0"])]).await?;
        assert!(!receipt.cancellation.sessions.is_empty());
        assert!(server.state().may(&client.id(), "long/0").await.is_err());
        let done = cancellation(&home, "remove-long").await?;
        assert_eq!(done.cancellation.state, CancellationState::Complete);
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "owned exec child remains alive after completion receipt"
        );
        assert_eq!(
            server.state().tunnel_sessions.stats().await.total_sessions,
            1
        );
        send_control(&home, ControlRequest::SetTunnelBlocked { blocked: false }).await?;
        tokio::time::timeout(
            Duration::from_secs(6),
            round_trip(&mut short, b"still-permitted"),
        )
        .await??;
        client.shutdown().await?;
        server.shutdown().await?;
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn runtime_outbound_resolution_uses_the_effective_book_and_rechecks_direction()
    -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let remote_dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let remote_home = FabricHome::new(remote_dir.path());
        let raw = "format = 2\n";
        fs::write(home.peers_path(), raw)?;
        let client = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let server = FabricNode::start(remote_home.clone()).await?;
        let mut peers = PeerBook::load(&remote_home)?;
        peers.add_inbound_only(
            client.id(),
            Some("client".into()),
            Some(client.addr()),
            Some(vec!["work/0".into()]),
        );
        peers.save(&remote_home)?;
        server.state().reload_peers().await?;
        let (echo, task) = echo_server().await?;
        server.expose_tcp("work/0", echo).await?;
        mutate(
            &home,
            "outbound",
            0,
            vec![Operation::AddPeer {
                node_id: server.id(),
                name: Some("server".into()),
                direction: None,
                addr: Some(server.addr()),
            }],
        )
        .await?;
        let port = client
            .dial_tcp("server", "work/0", "127.0.0.1:0".into())
            .await?;
        round_trip(
            &mut tokio::net::TcpStream::connect(&port).await?,
            b"runtime-address",
        )
        .await?;
        mutate(
            &home,
            "inbound-only",
            1,
            vec![Operation::AddPeer {
                node_id: server.id(),
                name: None,
                direction: Some(Direction::InboundOnly),
                addr: None,
            }],
        )
        .await?;
        let connects = client.state().peer_connections.connect_attempts();
        assert!(
            client
                .dial_tcp("server", "work/0", "127.0.0.1:0".into())
                .await
                .is_err()
        );
        let mut rejected = tokio::net::TcpStream::connect(&port).await?;
        rejected.write_all(b"no-dial").await?;
        let mut byte = [0];
        let result = tokio::time::timeout(Duration::from_secs(2), rejected.read(&mut byte)).await?;
        assert!(matches!(result, Ok(0) | Err(_)));
        assert_eq!(client.state().peer_connections.connect_attempts(), connects);
        assert_eq!(fs::read_to_string(home.peers_path())?, raw);
        client.shutdown().await?;
        server.shutdown().await?;
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn uncertain_commit_closes_admissions_until_work_ends_and_disk_is_reconciled()
    -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_server(&client_home, &client, &server).await?;
        let (echo, task) = echo_server().await?;
        server.expose_tcp("work/0", echo).await?;
        mutate(
            &home,
            "install",
            0,
            vec![add(&client, "client"), grant(&client, &["work/0"])],
        )
        .await?;
        let port = client
            .dial_tcp("server", "work/0", "127.0.0.1:0".into())
            .await?;
        let mut live = tokio::net::TcpStream::connect(port).await?;
        round_trip(&mut live, b"attached").await?;
        server
            .state()
            .runtime_peers
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .fail_directory_sync_for_test();
        let trusted = server
            .state()
            .admissions
            .admit(client.id(), crate::admission::Grant::Trust);
        let withdrawal = transaction("controller", "uncertain", 1, vec![grant(&client, &[])]);
        let reply = request(&home, withdrawal.clone()).await?;
        assert_eq!(reply.error.unwrap().code, "runtime_commit_indeterminate");
        assert!(server.state().peer_book.read().await.peers().is_empty());
        assert!(
            trusted.revoked().is_cancelled(),
            "indeterminate state must cancel existing trust admissions too"
        );
        drop(trusted);
        assert!(reply.snapshot.unwrap().fault.is_some());
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let state = server.state();
                let guard = state.policy_quarantine.lock().await;
                if let Some(selected) = guard.as_ref() {
                    if selected.status(false).await.state == CancellationState::Complete {
                        break;
                    }
                }
                drop(guard);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        server.state().reload_peers().await?;
        assert!(server.state().may(&client.id(), "work/0").await.is_err());
        let reply = request(&home, withdrawal).await?;
        assert!(reply.error.is_none());
        let receipt = reply.mutation.unwrap();
        assert_eq!(receipt.committed_revision, 2);
        assert_eq!(receipt.cancellation.state, CancellationState::Complete);
        assert!(receipt.cancellation.restart_evidence.is_none());
        client.shutdown().await?;
        server.shutdown().await?;
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn withdrawal_ends_a_mux_admission_waiting_for_tunnel_hello() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_server(&client_home, &client, &server).await?;
        let (echo, task) = echo_server().await?;
        server.expose_tcp("work/0", echo).await?;
        mutate(
            &home,
            "install",
            0,
            vec![add(&client, "client"), grant(&client, &["work/0"])],
        )
        .await?;
        // Ready admits the logical stream; never send its tunnel Hello.
        // Keeping trust means revocation cannot rely on closing the whole mux.
        let mut stream = client.state().open_stream("server", b"work/0").await?;
        mutate(&home, "withdraw", 1, vec![grant(&client, &[])]).await?;
        let receipt = cancellation(&home, "withdraw").await?;
        assert_eq!(receipt.cancellation.state, CancellationState::Complete);
        assert!(!receipt.cancellation.admissions.is_empty());
        assert!(receipt.cancellation.sessions.is_empty());
        let mut byte = [0];
        let ended = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte)).await?;
        assert!(!matches!(ended, Ok(1..)));
        client.shutdown().await?;
        server.shutdown().await?;
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn audit_records_revocation_before_tunnel_hello_without_starting_a_command() -> Result<()>
    {
        let _serial = SERIAL.lock().await;
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_server(&client_home, &client, &server).await?;
        server
            .expose_exec_ephemeral(
                "audit/work",
                vec!["/bin/sh".into(), "-c".into(), "exit 0".into()],
                1,
            )
            .await?;
        mutate(
            &home,
            "install",
            0,
            vec![
                add(&client, "client"),
                grant(&client, &["echo", "audit/work"]),
            ],
        )
        .await?;
        // Mux Ready has admitted this service, but argv/Hello have not arrived.
        let mut stream = client.state().open_stream("server", b"audit/work").await?;
        mutate(&home, "withdraw", 1, vec![grant(&client, &["echo"])]).await?;
        let receipt = cancellation(&home, "withdraw").await?;
        assert_eq!(receipt.cancellation.state, CancellationState::Complete);
        let mut byte = [0];
        let ended = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte)).await?;
        assert!(!matches!(ended, Ok(1..)));
        client.ping("server").await?;
        assert!(server.state().audit.drain(Duration::from_secs(2)).await);
        let records =
            crate::audit::read(home.root(), None, Some(&client.id().to_string()))?.records;
        let selected: Vec<_> = records
            .iter()
            .filter(|record| {
                record
                    .call()
                    .is_some_and(|call| call.service.as_deref() == Some("audit/work"))
            })
            .collect();
        assert_eq!(selected.len(), 1);
        let crate::audit::Record::Refused(call) = selected[0] else {
            panic!("revoked call spawned a command");
        };
        assert!(call.argv.is_none());
        assert_eq!(
            call.code.as_deref(),
            Some("fabric:peer_not_permitted_for_service")
        );
        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn waiting_for_policy_storage_keeps_control_responsive() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        // Establish the listener before occupying the persistence lock.
        request(&home, Request::List { cursor: None }).await?;
        let state = server.state();
        let store = state.runtime_peers.as_ref().unwrap().clone();
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _guard = store.lock().unwrap();
            let _ = held_tx.send(());
            // A regression must fail, rather than deadlocking this test runtime.
            let _ = release_rx.recv_timeout(Duration::from_secs(2));
        });
        held_rx.recv_timeout(Duration::from_secs(1))?;
        let started = Instant::now();
        let listing_state = state.clone();
        let listing = tokio::spawn(async move {
            listing_state
                .runtime_peer_request(Request::List { cursor: None })
                .await
        });
        while state.policy_update.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
        let status = send_control(&home, ControlRequest::Status).await?;
        assert!(matches!(status, ControlResponse::Status { .. }));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "policy storage parked the control executor"
        );
        release_tx.send(())?;
        worker.join().unwrap();
        assert!(listing.await?.error.is_none());
        server.shutdown().await
    }

    #[tokio::test]
    async fn shutdown_drains_a_policy_commit_that_started_before_stop() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let state = server.state();
        let store = state.runtime_peers.as_ref().unwrap().clone();
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _guard = store.lock().unwrap();
            let _ = held_tx.send(());
            let _ = release_rx.recv();
        });
        held_rx.recv_timeout(Duration::from_secs(1))?;
        let request_home = home.clone();
        let mutation = tokio::spawn(async move {
            request(
                &request_home,
                Request::AddPeer {
                    owner: "a".into(),
                    request_id: "before-stop".into(),
                    expected_revision: 0,
                    node_id: iroh::SecretKey::from_bytes(&[94; 32]).public(),
                    name: Some("phone".into()),
                    direction: Some(Direction::InboundOnly),
                    addr: None,
                },
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
            while state.policy_update.try_lock().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let mut stopping = tokio::spawn(server.shutdown());
        tokio::time::timeout(Duration::from_secs(3), async {
            while !state.cancel.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut stopping)
                .await
                .is_err(),
            "shutdown returned before the in-flight commit ended"
        );
        release_tx.send(())?;
        worker.join().unwrap();
        let reply = mutation.await??;
        assert!(reply.error.is_none(), "{:?}", reply.error);
        tokio::time::timeout(Duration::from_secs(3), stopping).await???;
        let (disk, _) = runtime_peers::RuntimePeers::open(&home);
        assert_eq!(disk.receipt("before-stop")?.committed_revision, 1);
        Ok(())
    }

    // Exercise actual existing traffic, not only the quarantined snapshot.
    async fn failed_static_reload_handles_candidate_withdrawals(kind: &str) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let remote_dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let remote_home = FabricHome::new(remote_dir.path());
        let client = FabricNode::start(remote_home.clone()).await?;
        let mut static_book = PeerBook::default();
        static_book.add_with_allow(
            client.id(),
            Some("client".into()),
            Some(client.addr()),
            Some(vec!["work/0".into(), "other/0".into()]),
        );
        if kind == "git" {
            static_book.share_git_remote("repo", dir.path().join("repo.git"))?;
        }
        static_book.save(&home)?;
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        trust_server(&remote_home, &client, &server).await?;
        if kind == "git" {
            mutate(
                &home,
                "install-git",
                0,
                vec![
                    Operation::AddPeer {
                        node_id: client.id(),
                        name: Some("client".into()),
                        direction: None,
                        addr: Some(client.addr()),
                    },
                    grant(&client, &["git/repo/read"]),
                ],
            )
            .await?;
        }
        let (echo, task) = echo_server().await?;
        server.expose_tcp("work/0", echo.clone()).await?;
        server.expose_tcp("other/0", echo).await?;
        let port = client
            .dial_tcp("server", "work/0", "127.0.0.1:0".into())
            .await?;
        let mut live = tokio::net::TcpStream::connect(port).await?;
        round_trip(&mut live, b"before-bad-reload").await?;
        let other_port = client
            .dial_tcp("server", "other/0", "127.0.0.1:0".into())
            .await?;
        let mut other = tokio::net::TcpStream::connect(other_port).await?;
        round_trip(&mut other, b"unrelated-before-reload").await?;
        let original_static = fs::read(home.peers_path())?;
        let state = server.state();
        let admitted = state.admissions.admit(
            client.id(),
            crate::admission::Grant::Service("work/0".into()),
        );
        let withdrawn = admitted.revoked();
        let unrelated = state.admissions.admit(
            client.id(),
            crate::admission::Grant::Service("other/0".into()),
        );
        if kind == "malformed" {
            fs::write(home.peers_path(), "format = [")?;
        } else if kind == "git" {
            static_book.unshare_git_remote("repo")?;
            static_book.save(&home)?;
        } else {
            static_book.add_with_allow(
                client.id(),
                Some("client".into()),
                Some(client.addr()),
                Some(vec!["other/0".into()]),
            );
            static_book.save(&home)?;
        }
        let original_mode = fs::metadata(dir.path())?.permissions();
        if kind == "read-only" {
            fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500))?;
            assert!(
                fs::File::create(dir.path().join("write-probe")).is_err(),
                "test must run without permission bypass"
            );
        }
        let reload = state.reload_peers().await;
        // Restore directory permissions before any assertion/fixture cleanup.
        fs::set_permissions(dir.path(), original_mode)?;
        assert!(reload.is_err());
        if kind == "git" {
            assert!(format!("{:#}", reload.unwrap_err()).contains("unshared"));
        }
        assert!(state.peer_book.read().await.peers().is_empty());
        assert!(
            state
                .runtime_peers
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .fault()
                .is_some()
        );
        assert!(
            !unrelated.revoked().is_cancelled(),
            "failed persist cancelled unrelated work"
        );
        round_trip(&mut other, b"unrelated-after-fault").await?;
        if kind == "read-only" {
            assert!(
                withdrawn.is_cancelled(),
                "valid static withdrawal remained live after persist failed"
            );
            let mut byte = [0];
            let ended = tokio::time::timeout(Duration::from_secs(3), live.read(&mut byte)).await?;
            assert!(
                matches!(ended, Ok(0) | Err(_)),
                "withdrawn traffic remained live"
            );
        } else {
            assert!(
                !withdrawn.is_cancelled(),
                "invalid static edit cancelled accepted work"
            );
            round_trip(&mut live, b"still-running-after-fault").await?;
        }
        drop(admitted);
        // New logical streams are refused on the already existing connection.
        assert!(
            client
                .state()
                .open_stream("server", b"work/0")
                .await
                .is_err()
        );
        if kind == "malformed" {
            fs::write(home.peers_path(), original_static)?;
        } else if kind == "git" {
            static_book.share_git_remote("repo", dir.path().join("repo.git"))?;
            static_book.save(&home)?;
        }
        if kind == "read-only" {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if state
                        .policy_quarantine
                        .lock()
                        .await
                        .as_ref()
                        .unwrap()
                        .status(false)
                        .await
                        .state
                        == CancellationState::Complete
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await?;
        }
        state.reload_peers().await?;
        if kind == "read-only" {
            assert!(
                withdrawn.is_cancelled(),
                "successful repair must apply the withdrawal"
            );
        } else {
            assert!(!withdrawn.is_cancelled());
            round_trip(&mut live, b"running-after-repair").await?;
        }
        assert!(!unrelated.revoked().is_cancelled());
        round_trip(&mut other, b"unrelated-after-repair").await?;
        drop(unrelated);
        client.shutdown().await?;
        server.shutdown().await?;
        task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn static_reload_with_read_only_state_revokes_only_candidate_withdrawals() -> Result<()> {
        failed_static_reload_handles_candidate_withdrawals("read-only").await
    }
    #[tokio::test]
    async fn static_reload_with_runtime_git_conflict_fails_closed_and_preserves_work() -> Result<()>
    {
        failed_static_reload_handles_candidate_withdrawals("git").await
    }
    #[tokio::test]
    async fn malformed_static_reload_preserves_work_and_repairs_without_cancelling_it() -> Result<()>
    {
        failed_static_reload_handles_candidate_withdrawals("malformed").await
    }

    #[tokio::test]
    async fn preparing_a_transaction_does_not_hold_the_admission_barrier() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let state = server.state();
        let store = state.runtime_peers.as_ref().unwrap().clone();
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _guard = store.lock().unwrap();
            held_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(3));
        });
        held_rx.recv_timeout(Duration::from_secs(1))?;
        let mutation_state = state.clone();
        let mutation = tokio::spawn(async move {
            mutation_state
                .runtime_peer_request(transaction(
                    "a",
                    "prepare",
                    0,
                    vec![Operation::AddPeer {
                        node_id: iroh::SecretKey::from_bytes(&[95; 32]).public(),
                        name: Some("node".into()),
                        direction: None,
                        addr: None,
                    }],
                ))
                .await
        });
        while state.policy_update.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
        let available = state.peer_book.try_read().is_ok();
        release_tx.send(())?;
        worker.join().unwrap();
        assert!(mutation.await?.error.is_none());
        assert!(
            available,
            "preparation occupied the admission write barrier"
        );
        server.shutdown().await
    }

    #[tokio::test]
    async fn shutdown_still_closes_sessions_when_policy_drain_is_stalled() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let remote_dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let remote_home = FabricHome::new(remote_dir.path());
        let server = FabricNode::start_with_daemon_options(home.clone(), options()).await?;
        let client = FabricNode::start(remote_home.clone()).await?;
        trust_server(&remote_home, &client, &server).await?;
        let pid_path = dir.path().join("shutdown-child.pid");
        server
            .expose_exec(
                "work/0",
                vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    "echo $$ > \"$1\"; exec cat".into(),
                    "child".into(),
                    pid_path.to_string_lossy().into_owned(),
                ],
            )
            .await?;
        mutate(
            &home,
            "install",
            0,
            vec![add(&client, "client"), grant(&client, &["work/0"])],
        )
        .await?;
        let port = client
            .dial_tcp("server", "work/0", "127.0.0.1:0".into())
            .await?;
        let mut live = tokio::net::TcpStream::connect(port).await?;
        round_trip(&mut live, b"attached").await?;
        let pid: i32 = fs::read_to_string(pid_path)?.trim().parse()?;
        let state = server.state();
        let barrier = state.policy_update.lock().await;
        let stopped = tokio::time::timeout(Duration::from_secs(6), server.shutdown()).await;
        drop(barrier);
        let error = stopped?.unwrap_err();
        assert!(error.to_string().contains("commits did not drain"));
        assert_eq!(state.tunnel_sessions.stats().await.total_sessions, 0);
        assert!(!home.control_socket_path().exists());
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "shutdown skipped owned-child cleanup after the drain timeout"
        );
        // The serve error must come after bounded audit flushing, so the
        // completed call remains readable even on the failed-drain path.
        let records =
            crate::audit::read(home.root(), None, Some(&client.id().to_string()))?.records;
        let calls: Vec<_> = records
            .iter()
            .filter(|record| {
                record
                    .call()
                    .is_some_and(|call| call.service.as_deref() == Some("work/0"))
            })
            .collect();
        assert_eq!(calls.len(), 2);
        let crate::audit::Record::ExecStart(start) = calls[0] else {
            panic!("missing exec start");
        };
        let crate::audit::Record::ExecEnd(end) = calls[1] else {
            panic!("shutdown error skipped audit end flush");
        };
        assert_eq!(start.session_id, end.session_id);
        assert_eq!(end.bytes_in, 8);
        assert_eq!(end.bytes_out, 8);
        client.shutdown().await?;
        Ok(())
    }

    #[derive(Debug)]
    struct Uncooperative {
        started: Arc<AtomicBool>,
        finish: Arc<tokio::sync::Notify>,
    }
    const HANG_PROTOCOL: &[services::Protocol] = &[services::Protocol {
        alpn: b"runtime/hang/0",
        resumable: true,
        accept_event: "runtime_hang_accept",
    }];
    impl Service for Uncooperative {
        fn name(&self) -> &'static str {
            "hung"
        }
        fn protocols(&self) -> &'static [services::Protocol] {
            HANG_PROTOCOL
        }
        fn serve(&self, stream: PeerStream) -> services::BoxFuture<'static, Result<()>> {
            let started = self.started.clone();
            let finish = self.finish.clone();
            Box::pin(async move {
                let _stream = stream;
                started.store(true, Ordering::SeqCst);
                finish.notified().await;
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn cancellation_timeout_reports_remaining_work_without_undoing_policy() -> Result<()> {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let started = Arc::new(AtomicBool::new(false));
        let finish = Arc::new(tokio::sync::Notify::new());
        let server = FabricNode::start_with_services(
            home.clone(),
            options(),
            Services::new().with(Uncooperative {
                started: started.clone(),
                finish: finish.clone(),
            }),
        )
        .await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_server(&client_home, &client, &server).await?;
        mutate(
            &home,
            "install",
            0,
            vec![add(&client, "client"), grant(&client, &["hung"])],
        )
        .await?;
        let port = client
            .dial_tcp("server", "runtime/hang/0", "127.0.0.1:0".into())
            .await?;
        let _stream = tokio::net::TcpStream::connect(port).await?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while !started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        send_control(&home, ControlRequest::SetTunnelBlocked { blocked: true }).await?;
        send_control(&home, ControlRequest::DropTunnelConnections).await?;
        tokio::time::timeout(Duration::from_secs(3), async {
            while server
                .state()
                .tunnel_sessions
                .stats()
                .await
                .detached_sessions
                == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        mutate(&home, "withdraw", 1, vec![grant(&client, &[])]).await?;
        let receipt = cancellation(&home, "withdraw").await?;
        assert_eq!(receipt.cancellation.state, CancellationState::TimedOut);
        let fault = request(
            &home,
            Request::Cancellation {
                request_id: "withdraw".into(),
            },
        )
        .await?;
        assert_eq!(fault.error.unwrap().code, "cancellation_timeout");
        assert!(!receipt.cancellation.remaining_sessions.is_empty());
        assert!(server.state().may(&client.id(), "hung").await.is_err());
        let state = server.state();
        state
            .runtime_peers
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .fail_directory_sync_for_test();
        let fault = request(
            &home,
            transaction(
                "controller",
                "uncertain",
                2,
                vec![Operation::RemovePeer {
                    node_id: client.id(),
                }],
            ),
        )
        .await?;
        assert_eq!(fault.error.unwrap().code, "runtime_commit_indeterminate");
        // Current connection admissions have ended, but the detached handler
        // removed by the earlier withdrawal is still running.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let done = state
                    .policy_quarantine
                    .lock()
                    .await
                    .as_ref()
                    .unwrap()
                    .admissions
                    .iter()
                    .all(|entry| entry.complete());
                if done {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let error = state.reload_peers().await.unwrap_err();
        assert_eq!(
            crate::error::code_of(&error),
            ErrorCode::RuntimeStateUnavailable
        );
        finish.notify_one();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if state
                    .policy_quarantine
                    .lock()
                    .await
                    .as_ref()
                    .unwrap()
                    .status(false)
                    .await
                    .state
                    == CancellationState::Complete
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        state.reload_peers().await?;
        let late = cancellation(&home, "withdraw").await?.cancellation;
        assert_eq!(late.state, CancellationState::TimedOut);
        assert!(late.remaining_admissions.is_empty());
        assert!(late.remaining_sessions.is_empty());
        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }
}
