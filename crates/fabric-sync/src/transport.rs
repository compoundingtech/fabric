//! The engine's transport when it runs in the companion: every peer lookup and
//! every outbound reconcile goes to the daemon over the bridge.
//!
//! The session it talks to can change while the engine runs, because a daemon
//! restart mints a new nonce. Until the companion re-attaches, a lookup
//! resolves nothing and a reconcile fails with a local error, which the engine
//! reports as unavailable rather than as a network fault it must recover from.

use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result};
use tokio::{net::UnixStream, sync::Mutex};

use fabric_config::sync::{
    PeerRef, ResolvedPeers, SyncPeers,
    ipc::{IpcClient, IpcNonce, IpcStatus},
};

use crate::{
    engine::SyncTransport,
    node::{Reconciled, SyncNode},
};

#[derive(Clone, Default)]
pub struct IpcSyncTransport {
    session: Arc<RwLock<Option<IpcClient>>>,
}

impl IpcSyncTransport {
    /// A transport attached to one fixed daemon session.
    pub fn new(socket_path: impl Into<PathBuf>, nonce: IpcNonce) -> Self {
        let transport = Self::default();
        transport.attach(IpcClient::new(socket_path, nonce));
        transport
    }

    /// A transport with no daemon session yet.
    pub fn detached() -> Self {
        Self::default()
    }

    pub fn attach(&self, client: IpcClient) {
        *self.session.write().unwrap() = Some(client);
    }

    pub fn detach(&self) {
        *self.session.write().unwrap() = None;
    }

    pub fn session(&self) -> Option<IpcClient> {
        self.session.read().unwrap().clone()
    }

    pub fn socket_path(&self) -> Option<PathBuf> {
        self.session()
            .map(|client| client.socket_path().to_path_buf())
    }

    fn client(&self) -> Result<IpcClient> {
        self.session()
            .context("the sync companion is not attached to a fabric daemon")
    }

    pub async fn resolve_peers(&self, peers: &SyncPeers) -> Result<ResolvedPeers> {
        self.client()?.resolve_peers(peers).await
    }

    pub async fn open_outbound(&self, peer: PeerRef, sync_name: String) -> Result<UnixStream> {
        self.client()?.open_outbound(peer, sync_name).await
    }

    pub async fn status(&self) -> Result<IpcStatus> {
        self.client()?.status().await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.client()?.shutdown().await
    }
}

impl SyncTransport for IpcSyncTransport {
    async fn peers_for(&self, peers: &SyncPeers) -> ResolvedPeers {
        match self.resolve_peers(peers).await {
            Ok(resolved) => resolved,
            Err(_) => ResolvedPeers {
                peers: Vec::new(),
                unresolved: match peers {
                    SyncPeers::Wildcard(selector) => vec![selector.clone()],
                    SyncPeers::List(selectors) => selectors.clone(),
                },
            },
        }
    }

    async fn reconcile(
        &self,
        peer: PeerRef,
        name: String,
        node: Arc<Mutex<SyncNode>>,
    ) -> Result<Reconciled> {
        let stream = self.open_outbound(peer.clone(), name.clone()).await?;
        crate::wire::run_client(stream, node, &name, &peer.id).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use fabric_config::sync::ipc::{
        IpcError, IpcErrorKind, IpcListener, IpcNonce, IpcPeer, IpcRequestKind, IpcResponse,
        IpcRuntimeState, IpcStatus, write_response,
    };
    use tokio::sync::Mutex;

    use super::*;
    use crate::{manifest::Author, node::content_hash, wire};

    /// The daemon's side of the bridge, in miniature: resolves one peer and
    /// serves one sync entry from a node held in this test.
    async fn reference_server(
        listener: IpcListener,
        nonce: IpcNonce,
        remote: Arc<Mutex<SyncNode>>,
    ) -> Result<()> {
        loop {
            let Some((mut stream, request)) = listener.accept_request(&nonce).await? else {
                continue;
            };
            match request.kind {
                IpcRequestKind::ResolvePeers { .. } => {
                    write_response(
                        &mut stream,
                        &IpcResponse::peers(
                            request.request_id,
                            vec![IpcPeer {
                                key: "remote-key".into(),
                                id: "remote".into(),
                                roaming: false,
                            }],
                            Vec::new(),
                        ),
                    )
                    .await?;
                }
                IpcRequestKind::OpenOutbound { peer, sync_name } => {
                    if peer.key != "remote-key" || sync_name != "catalog" {
                        write_response(
                            &mut stream,
                            &IpcResponse::error(
                                request.request_id,
                                IpcError::new(IpcErrorKind::NotFound, "unknown sync target"),
                            ),
                        )
                        .await?;
                        continue;
                    }
                    write_response(&mut stream, &IpcResponse::ready(request.request_id)).await?;
                    let target = remote.clone();
                    wire::run_server(stream, "reference-client", move |hello| {
                        let target = target.clone();
                        async move { Ok((hello.name == "catalog").then_some((target, ()))) }
                    })
                    .await?;
                }
                IpcRequestKind::Status => {
                    write_response(
                        &mut stream,
                        &IpcResponse::status(
                            request.request_id,
                            IpcStatus {
                                state: IpcRuntimeState::Ready,
                                active_sessions: 0,
                                entries: Vec::new(),
                            },
                        ),
                    )
                    .await?;
                }
                IpcRequestKind::Shutdown => {
                    write_response(&mut stream, &IpcResponse::shutting_down(request.request_id))
                        .await?;
                    return Ok(());
                }
                _ => {
                    write_response(
                        &mut stream,
                        &IpcResponse::error(
                            request.request_id,
                            IpcError::new(IpcErrorKind::Unavailable, "not in this test"),
                        ),
                    )
                    .await?;
                }
            }
        }
    }

    #[tokio::test]
    async fn ipc_transport_passes_peer_and_reconcile_conformance() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("sync.sock");
        let nonce = IpcNonce::new("0123456789abcdef")?;
        let listener = IpcListener::bind(&socket)?;
        let remote = Arc::new(Mutex::new(SyncNode::new(Author([2; 32]))));
        remote
            .lock()
            .await
            .local_write("remote.md", b"over the bridge", 0, 0);
        let server = tokio::spawn(reference_server(listener, nonce.clone(), remote.clone()));
        let transport = IpcSyncTransport::new(socket, nonce);

        let peers = transport.peers_for(&SyncPeers::Wildcard("*".into())).await;
        assert_eq!(peers.unresolved, Vec::<String>::new());
        assert_eq!(peers.peers.len(), 1);
        assert_eq!(peers.peers[0].key, "remote-key");

        let local = Arc::new(Mutex::new(SyncNode::new(Author([1; 32]))));
        let stats = transport
            .reconcile(peers.peers[0].clone(), "catalog".into(), local.clone())
            .await?;
        assert!(!stats.is_noop());
        let local = local.lock().await;
        assert!(local.manifest().get("remote.md").is_some());
        assert!(local.has_content(&content_hash(b"over the bridge")));
        drop(local);

        assert_eq!(transport.status().await?.state, IpcRuntimeState::Ready);
        transport.shutdown().await?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn a_detached_transport_resolves_nothing_and_says_why() -> Result<()> {
        let transport = IpcSyncTransport::detached();
        let peers = transport.peers_for(&SyncPeers::List(vec!["a".into()])).await;
        assert_eq!(peers.unresolved, vec!["a".to_string()]);
        let local = Arc::new(Mutex::new(SyncNode::new(Author([1; 32]))));
        let error = transport
            .reconcile(
                PeerRef {
                    key: "k".into(),
                    id: "a".into(),
                    roaming: false,
                },
                "catalog".into(),
                local,
            )
            .await
            .expect_err("a detached transport reconciled");
        assert!(format!("{error:#}").contains("not attached to a fabric daemon"));
        Ok(())
    }
}
