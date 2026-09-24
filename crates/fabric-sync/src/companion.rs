//! The sync companion runtime: the engine, hosted outside the daemon.
//!
//! `fabric-sync` runs this under the OS service manager. Tests run it in
//! process beside a daemon. Either way it is the same code on the same two
//! sockets, so what a test proves is what production runs.
//!
//! The runtime has one loop that talks to the daemon and one listener that the
//! daemon talks to. Every ten seconds it sends a hello over the control socket.
//! The daemon's answer decides the phase: `embedded` means standby (no lease,
//! no watchers), `companion` means this process owns sync. The hello also
//! carries the session a companion needs to attach: the daemon's bridge socket,
//! the instance nonce, and the node id that is the stable sync author. A daemon
//! restart mints a new nonce, so the loop re-attaches within one interval and
//! the engine keeps running across it.

use std::{
    path::PathBuf,
    sync::{
        Arc, RwLock as StdRwLock,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use tokio::{
    net::UnixStream,
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{SyncEngine, SyncPaths, manifest::Author, transport::IpcSyncTransport};
use fabric_config::{
    FabricHome,
    daemon_control::{self, Request as ControlRequest, Response as ControlResponse},
    sync::ipc::{
        self, IpcClient, IpcError, IpcErrorKind, IpcListener, IpcNonce, IpcRequest,
        IpcRequestKind, IpcResponse, IpcRuntimeState, IpcStatus,
    },
};

/// How often an attached companion tells the daemon it is alive. The daemon
/// treats three missed heartbeats as absence.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// How quickly a companion without a daemon session tries again.
pub const REATTACH_INTERVAL: Duration = Duration::from_secs(1);
/// An inbound session that moves no bytes for this long is over.
pub const INBOUND_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Where the runtime stands with its daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompanionPhase {
    /// No daemon has answered yet, or the last heartbeat failed.
    Detached { reason: String },
    /// The daemon owns embedded sync; this process holds no lease.
    Standby,
    /// The daemon delegates sync; the engine runs here and holds the lease.
    Active,
    /// The daemon answered, but this process cannot serve it: a different
    /// build, a different bridge contract, or a busy state lease.
    Unavailable { reason: String },
}

impl CompanionPhase {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }

    /// The one-line form the supervised binary prints when the phase changes.
    pub fn describe(&self) -> String {
        match self {
            Self::Detached { reason } => format!("detached; {reason}"),
            Self::Standby => "standby; daemon owns embedded sync".to_string(),
            Self::Active => "active; this process owns sync".to_string(),
            Self::Unavailable { reason } => format!("unavailable; {reason}"),
        }
    }
}

struct RunningEngine {
    engine: Arc<SyncEngine<IpcSyncTransport>>,
    cancel: CancellationToken,
    author: Author,
}

struct Companion {
    home: FabricHome,
    paths: SyncPaths,
    cancel: CancellationToken,
    transport: Arc<IpcSyncTransport>,
    engine: Mutex<Option<RunningEngine>>,
    /// The nonce the daemon's requests must carry. `None` until attached.
    nonce: StdRwLock<Option<IpcNonce>>,
    phase: watch::Sender<CompanionPhase>,
    active_sessions: AtomicU32,
}

/// A running companion. Dropping it does not stop the runtime; call
/// [`CompanionHandle::shutdown`] so the lease is released before the caller
/// starts another owner.
pub struct CompanionHandle {
    inner: Arc<Companion>,
    task: JoinHandle<Result<()>>,
}

impl CompanionHandle {
    pub fn phase(&self) -> CompanionPhase {
        self.inner.phase.borrow().clone()
    }

    pub fn socket_path(&self) -> PathBuf {
        self.inner.home.sync_companion_socket_path()
    }

    /// Wait until the phase satisfies `accept`, or fail after `timeout` naming
    /// the phase it was in.
    pub async fn wait_for(
        &self,
        timeout: Duration,
        accept: impl Fn(&CompanionPhase) -> bool,
    ) -> Result<CompanionPhase> {
        let mut rx = self.inner.phase.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let current = rx.borrow_and_update().clone();
            if accept(&current) {
                return Ok(current);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                bail!("the sync companion is {}", current.describe());
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                let current = rx.borrow().clone();
                bail!("the sync companion is {}", current.describe());
            }
        }
    }

    pub async fn wait_until_active(&self, timeout: Duration) -> Result<()> {
        self.wait_for(timeout, CompanionPhase::is_active).await?;
        Ok(())
    }

    /// The engine, when this process owns sync. Tests read it to drive a pass
    /// on purpose; production reaches it only over the bridge.
    pub async fn engine(&self) -> Option<Arc<SyncEngine<IpcSyncTransport>>> {
        self.inner
            .engine
            .lock()
            .await
            .as_ref()
            .map(|running| running.engine.clone())
    }

    /// Stop the runtime: cancel the engine, release the lease, remove the
    /// socket, and wait for the tasks to end.
    pub async fn shutdown(self) -> Result<()> {
        self.inner.cancel.cancel();
        let result = self.task.await?;
        self.inner.stop_engine().await;
        result
    }
}

/// Start the companion runtime for `home`.
///
/// Binds the companion's bridge socket and begins the heartbeat. Returns as
/// soon as the socket is bound; use [`CompanionHandle::wait_until_active`] to
/// know when the daemon has delegated sync to it.
pub async fn start(home: FabricHome) -> Result<CompanionHandle> {
    std::fs::create_dir_all(home.root().join("run"))
        .with_context(|| format!("creating {}", home.root().join("run").display()))?;
    let socket_path = home.sync_companion_socket_path();
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)
            .with_context(|| format!("removing stale {}", socket_path.display()))?;
    }
    let listener = IpcListener::bind(&socket_path)?;
    let paths = SyncPaths::new(home.syncs_path(), home.root().join("sync"));
    let (phase, _) = watch::channel(CompanionPhase::Detached {
        reason: "no daemon has answered yet".to_string(),
    });
    let inner = Arc::new(Companion {
        home,
        paths,
        cancel: CancellationToken::new(),
        transport: Arc::new(IpcSyncTransport::detached()),
        engine: Mutex::new(None),
        nonce: StdRwLock::new(None),
        phase,
        active_sessions: AtomicU32::new(0),
    });
    let task = tokio::spawn(inner.clone().run(listener));
    Ok(CompanionHandle { inner, task })
}

impl Companion {
    async fn run(self: Arc<Self>, listener: IpcListener) -> Result<()> {
        let heartbeat = tokio::spawn(self.clone().heartbeat_loop());
        let served = tokio::spawn(self.clone().serve(listener));
        self.cancel.cancelled().await;
        heartbeat.abort();
        served.abort();
        let _ = heartbeat.await;
        let _ = served.await;
        Ok(())
    }

    async fn heartbeat_loop(self: Arc<Self>) {
        loop {
            let phase = self.heartbeat().await;
            let changed = *self.phase.borrow() != phase;
            if changed {
                tracing::info!(
                    target: crate::SYNC_LOG_TARGET,
                    event = "companion_phase",
                    phase = %phase.describe(),
                    "sync companion phase changed"
                );
            }
            self.phase.send_replace(phase.clone());
            let wait = match phase {
                CompanionPhase::Active | CompanionPhase::Standby => HEARTBEAT_INTERVAL,
                _ => REATTACH_INTERVAL,
            };
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    async fn heartbeat(&self) -> CompanionPhase {
        let request = ControlRequest::SyncCompanionHello {
            version: fabric_config::version_string(),
            sync_ipc_magic: ipc::IPC_MAGIC.to_string(),
            sync_ipc_version: ipc::IPC_VERSION,
            companion_socket: Some(self.home.sync_companion_socket_path()),
        };
        match daemon_control::send(&self.home, request).await {
            Ok(ControlResponse::SyncIpcCompatibility {
                owner,
                nonce,
                daemon_socket,
                node_id,
                ..
            }) => match owner.as_str() {
                "embedded" => {
                    self.stop_engine().await;
                    CompanionPhase::Standby
                }
                "companion" => self.attach(nonce, daemon_socket, node_id).await,
                other => CompanionPhase::Unavailable {
                    reason: format!("daemon granted unsupported owner {other}"),
                },
            },
            Ok(response) => CompanionPhase::Unavailable {
                reason: format!("unexpected daemon response {response:?}"),
            },
            // The daemon refuses a different build or bridge contract with a
            // message that names both sides; that is not a daemon that is away.
            Err(error) if format!("{error:#}").contains("fabric-sync") => {
                CompanionPhase::Unavailable {
                    reason: format!("{error:#}"),
                }
            }
            Err(error) => CompanionPhase::Detached {
                reason: format!("{error:#}"),
            },
        }
    }

    async fn attach(
        &self,
        nonce: Option<String>,
        daemon_socket: Option<PathBuf>,
        node_id: Option<String>,
    ) -> CompanionPhase {
        let (Some(nonce), Some(daemon_socket), Some(node_id)) = (nonce, daemon_socket, node_id)
        else {
            return CompanionPhase::Unavailable {
                reason: "the daemon delegated sync without a bridge session".to_string(),
            };
        };
        let nonce = match IpcNonce::new(nonce) {
            Ok(nonce) => nonce,
            Err(error) => {
                return CompanionPhase::Unavailable {
                    reason: format!("{error:#}"),
                };
            }
        };
        let author = match author_from_node_id(&node_id) {
            Ok(author) => author,
            Err(error) => {
                return CompanionPhase::Unavailable {
                    reason: format!("{error:#}"),
                };
            }
        };
        let same_session = self
            .transport
            .session()
            .is_some_and(|client| client.socket_path() == daemon_socket && client.nonce() == &nonce);
        if !same_session {
            self.transport
                .attach(IpcClient::new(daemon_socket, nonce.clone()));
            *self.nonce.write().unwrap() = Some(nonce);
        }
        match self.ensure_engine(author).await {
            Ok(()) => CompanionPhase::Active,
            Err(error) => CompanionPhase::Unavailable {
                reason: format!("{error:#}"),
            },
        }
    }

    async fn ensure_engine(&self, author: Author) -> Result<()> {
        let mut slot = self.engine.lock().await;
        if let Some(running) = slot.as_ref() {
            if running.author == author {
                return Ok(());
            }
            // A different daemon identity is a different sync author. Stop the
            // engine that was writing under the old one before starting anew.
            let running = slot.take().expect("checked above");
            running.cancel.cancel();
            drop(running);
        }
        let cancel = self.cancel.child_token();
        let engine = SyncEngine::new(
            self.paths.clone(),
            author.clone(),
            self.transport.clone(),
            cancel.clone(),
        )
        .await?;
        let runner = engine.clone();
        tokio::spawn(async move {
            if let Err(error) = runner.run().await {
                tracing::warn!(target: crate::SYNC_LOG_TARGET, %error, "sync engine stopped");
            }
        });
        *slot = Some(RunningEngine {
            engine,
            cancel,
            author,
        });
        Ok(())
    }

    /// Stop the engine and wait until it has let go of the state lease.
    ///
    /// The entry loops and any inbound session in flight hold clones of the
    /// engine; the lease is released when the last clone drops, which is when
    /// they observe the cancel. A caller that starts another owner right after
    /// this returns must find the lease free, so this waits for that moment
    /// rather than returning on the cancel alone. The wait is bounded: a loop
    /// wedged past it is logged and the lease goes with the last local clone.
    async fn stop_engine(&self) {
        let running = self.engine.lock().await.take();
        let Some(RunningEngine { engine, cancel, .. }) = running else {
            return;
        };
        cancel.cancel();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while Arc::strong_count(&engine) > 1 {
            if tokio::time::Instant::now() > deadline {
                tracing::warn!(
                    target: crate::SYNC_LOG_TARGET,
                    holders = Arc::strong_count(&engine) - 1,
                    "sync engine tasks did not stop within 10 s; releasing the lease anyway"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(engine);
    }

    async fn serve(self: Arc<Self>, listener: IpcListener) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                accepted = listener.accept_stream() => match accepted {
                    Ok(stream) => {
                        let companion = self.clone();
                        tokio::spawn(async move {
                            if let Err(error) = companion.handle(stream).await {
                                tracing::debug!(target: crate::SYNC_LOG_TARGET, %error, "sync bridge request failed");
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!(target: crate::SYNC_LOG_TARGET, %error, "sync bridge accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
        // Dropping the listener removes the socket.
        drop(listener);
    }

    async fn handle(&self, mut stream: UnixStream) -> Result<()> {
        let request: IpcRequest = ipc::read_request(&mut stream).await?;
        let request_id = request.request_id;
        let nonce = self.nonce.read().unwrap().clone();
        let Some(nonce) = nonce else {
            ipc::write_response(
                &mut stream,
                &IpcResponse::error(
                    request_id,
                    IpcError::new(
                        IpcErrorKind::Unavailable,
                        "the sync companion is not attached to a fabric daemon",
                    ),
                ),
            )
            .await?;
            return Ok(());
        };
        if let Err(error) = request.validate(&nonce) {
            ipc::write_response(&mut stream, &IpcResponse::error(request_id, error)).await?;
            return Ok(());
        }
        match request.kind {
            IpcRequestKind::OpenInbound {
                authenticated_peer_id,
                display_label,
            } => {
                let Some(engine) = self.engine().await else {
                    return refuse(&mut stream, request_id, "no sync engine is running here").await;
                };
                ipc::write_response(&mut stream, &IpcResponse::ready(request_id)).await?;
                let peer = display_label.unwrap_or(authenticated_peer_id);
                self.active_sessions.fetch_add(1, Ordering::Relaxed);
                engine
                    .serve_inbound(stream, &peer, INBOUND_IDLE_TIMEOUT)
                    .await;
                self.active_sessions.fetch_sub(1, Ordering::Relaxed);
                Ok(())
            }
            IpcRequestKind::Status => {
                let (state, entries) = match self.engine().await {
                    Some(engine) => (
                        IpcRuntimeState::Ready,
                        engine.status().await.into_iter().map(Into::into).collect(),
                    ),
                    None => (IpcRuntimeState::Unavailable, Vec::new()),
                };
                let status = IpcStatus {
                    state,
                    active_sessions: self.active_sessions.load(Ordering::Relaxed),
                    entries,
                };
                ipc::write_response(&mut stream, &IpcResponse::status(request_id, status)).await
            }
            IpcRequestKind::Reload => {
                let Some(engine) = self.engine().await else {
                    return refuse(&mut stream, request_id, "no sync engine is running here").await;
                };
                // One pass per entry BEFORE the answer, as the embedded owner
                // did: a caller that reloads and then reads status must see
                // the passes the reload caused, not a promise of them.
                match engine.reload().await {
                    Ok(()) => {
                        for name in engine.names().await {
                            let _ = engine.sync_once(&name).await;
                        }
                        ipc::write_response(&mut stream, &IpcResponse::ready(request_id)).await
                    }
                    Err(error) => {
                        ipc::write_response(
                            &mut stream,
                            &IpcResponse::error(
                                request_id,
                                IpcError::new(IpcErrorKind::Internal, format!("{error:#}")),
                            ),
                        )
                        .await
                    }
                }
            }
            IpcRequestKind::Publish { name, files, force } => {
                let Some(engine) = self.engine().await else {
                    return refuse(&mut stream, request_id, "no sync engine is running here").await;
                };
                let response = match publish_files(files)
                    .and_then(|files| Ok((files, ())))
                {
                    Ok((files, ())) => match engine.publish_staged(&name, files, force).await {
                        Ok(published) => IpcResponse::published(
                            request_id,
                            published
                                .into_iter()
                                .map(|file| fabric_config::sync::status::SyncPublishedFile {
                                    rel: file.rel,
                                    version: file.version,
                                    hash: file.hash.to_hex(),
                                })
                                .collect(),
                        ),
                        Err(error) => IpcResponse::error(
                            request_id,
                            IpcError::new(IpcErrorKind::Internal, format!("{error:#}")),
                        ),
                    },
                    Err(error) => IpcResponse::error(
                        request_id,
                        IpcError::new(IpcErrorKind::InvalidRequest, format!("{error:#}")),
                    ),
                };
                ipc::write_response(&mut stream, &response).await
            }
            IpcRequestKind::Shutdown => {
                ipc::write_response(&mut stream, &IpcResponse::shutting_down(request_id)).await?;
                self.cancel.cancel();
                Ok(())
            }
            IpcRequestKind::ResolvePeers { .. } | IpcRequestKind::OpenOutbound { .. } => {
                ipc::write_response(
                    &mut stream,
                    &IpcResponse::error(
                        request_id,
                        IpcError::new(
                            IpcErrorKind::InvalidRequest,
                            "the sync companion does not resolve peers or open outbound streams",
                        ),
                    ),
                )
                .await
            }
        }
    }

    async fn engine(&self) -> Option<Arc<SyncEngine<IpcSyncTransport>>> {
        self.engine
            .lock()
            .await
            .as_ref()
            .map(|running| running.engine.clone())
    }
}

async fn refuse(stream: &mut UnixStream, request_id: u64, message: &str) -> Result<()> {
    ipc::write_response(
        stream,
        &IpcResponse::error(
            request_id,
            IpcError::new(IpcErrorKind::Unavailable, message),
        ),
    )
    .await
}

fn publish_files(
    files: Vec<fabric_config::sync::status::SyncPublishFile>,
) -> Result<Vec<fabric_config::sync::staging::PublishFile>> {
    files
        .into_iter()
        .map(|file| {
            let base = match file.base {
                Some(hex) => Some(
                    fabric_config::sync::ContentHash::from_hex(&hex).ok_or_else(|| {
                        anyhow::anyhow!("{}: the base is not a content hash", file.rel)
                    })?,
                ),
                None => None,
            };
            Ok(fabric_config::sync::staging::PublishFile {
                rel: file.rel,
                bytes: file.bytes,
                executable: file.executable,
                base,
            })
        })
        .collect()
}

/// The sync author is the daemon's node id, byte for byte, so a companion
/// writes exactly what the embedded engine wrote for the same machine.
pub fn author_from_node_id(node_id: &str) -> Result<Author> {
    let id: iroh::EndpointId = node_id
        .parse()
        .with_context(|| format!("the daemon's node id {node_id:?} is not a node id"))?;
    Ok(Author(*id.as_bytes()))
}

/// Daily-rotated companion log beside the daemon's, same filter, same bound.
pub fn init_companion_tracing(home: &FabricHome) -> Result<()> {
    home.prepare()?;
    let retention = std::env::var("FABRIC_LOG_RETENTION_DAYS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(14);
    let mut builder = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix(home.sync_log_prefix());
    if retention > 0 {
        builder = builder.max_log_files(retention);
    }
    let appender = builder
        .build(home.validation_log_dir())
        .context("failed to build the sync companion log appender")?;
    let filter = std::env::var("FABRIC_LOG")
        .ok()
        .and_then(|raw| tracing_subscriber::EnvFilter::try_new(raw).ok())
        .unwrap_or_else(|| tracing_subscriber::EnvFilter::new("fabric=info"));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(appender)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
    Ok(())
}
