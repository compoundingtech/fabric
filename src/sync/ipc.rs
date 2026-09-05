//! The dormant local bridge between `fabric-sync` and `fabric`.

use std::{
    fmt,
    os::{
        fd::AsRawFd,
        unix::{fs::FileTypeExt, fs::MetadataExt, fs::PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::Mutex,
};

use super::{PeerRef, SyncNode, SyncPeers, SyncTransport, engine::ResolvedPeers};

pub const IPC_MAGIC: &str = "fabric/sync-ipc/1";
pub const IPC_VERSION: u16 = 1;
pub const MAX_CONTROL_FRAME: usize = 16 * 1024;
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// A daemon-instance value that prevents a stale local process from joining a
/// new daemon. The daemon creates and distributes the value outside this API.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IpcNonce(String);

impl IpcNonce {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if !(16..=128).contains(&value.len()) || !value.is_ascii() {
            bail!("a sync IPC nonce must contain 16 to 128 ASCII bytes");
        }
        Ok(Self(value))
    }
}

impl fmt::Debug for IpcNonce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IpcNonce([redacted])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind", content = "value")]
pub enum IpcPeerSelector {
    Wildcard(String),
    List(Vec<String>),
}

impl From<&SyncPeers> for IpcPeerSelector {
    fn from(value: &SyncPeers) -> Self {
        match value {
            SyncPeers::Wildcard(selector) => Self::Wildcard(selector.clone()),
            SyncPeers::List(selectors) => Self::List(selectors.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpcPeer {
    pub key: String,
    pub id: String,
    #[serde(default)]
    pub roaming: bool,
}

impl From<IpcPeer> for PeerRef {
    fn from(value: IpcPeer) -> Self {
        Self {
            key: value.key,
            id: value.id,
            roaming: value.roaming,
        }
    }
}

impl From<PeerRef> for IpcPeer {
    fn from(value: PeerRef) -> Self {
        Self {
            key: value.key,
            id: value.id,
            roaming: value.roaming,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum IpcRequestKind {
    ResolvePeers {
        selectors: IpcPeerSelector,
    },
    OpenOutbound {
        peer: IpcPeer,
        sync_name: String,
    },
    OpenInbound {
        authenticated_peer_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_label: Option<String>,
        sync_name: String,
    },
    Status,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpcRequest {
    pub magic: String,
    pub version: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_features: Vec<String>,
    pub nonce: IpcNonce,
    pub request_id: u64,
    pub kind: IpcRequestKind,
}

impl IpcRequest {
    pub fn new(nonce: IpcNonce, request_id: u64, kind: IpcRequestKind) -> Self {
        Self {
            magic: IPC_MAGIC.to_string(),
            version: IPC_VERSION,
            required_features: Vec::new(),
            nonce,
            request_id,
            kind,
        }
    }

    pub fn validate(&self, expected_nonce: &IpcNonce) -> std::result::Result<(), IpcError> {
        if self.magic != IPC_MAGIC {
            return Err(IpcError::new(
                IpcErrorKind::InvalidRequest,
                "the sync IPC magic value does not match",
            ));
        }
        if self.version != IPC_VERSION {
            return Err(IpcError::new(
                IpcErrorKind::Incompatible,
                format!(
                    "sync IPC version {} is incompatible with version {IPC_VERSION}",
                    self.version
                ),
            ));
        }
        if !self.required_features.is_empty() {
            return Err(IpcError::new(
                IpcErrorKind::Incompatible,
                format!(
                    "unsupported required sync IPC features: {}",
                    self.required_features.join(", ")
                ),
            ));
        }
        if &self.nonce != expected_nonce {
            return Err(IpcError::new(
                IpcErrorKind::Unauthorized,
                "the daemon-instance nonce does not match",
            ));
        }
        if self.request_id == 0 {
            return Err(IpcError::new(
                IpcErrorKind::InvalidRequest,
                "request ID zero is reserved",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IpcErrorKind {
    Incompatible,
    Unauthorized,
    InvalidRequest,
    Unavailable,
    NotFound,
    Busy,
    Internal,
}

impl IpcErrorKind {
    fn token(self) -> &'static str {
        match self {
            Self::Incompatible => "incompatible",
            Self::Unauthorized => "unauthorized",
            Self::InvalidRequest => "invalid-request",
            Self::Unavailable => "unavailable",
            Self::NotFound => "not-found",
            Self::Busy => "busy",
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpcError {
    pub kind: IpcErrorKind,
    pub message: String,
}

impl IpcError {
    pub fn new(kind: IpcErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.token(), self.message)
    }
}

impl std::error::Error for IpcError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IpcRuntimeState {
    Starting,
    Ready,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpcStatus {
    pub state: IpcRuntimeState,
    pub active_sessions: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum IpcResponseKind {
    Peers {
        peers: Vec<IpcPeer>,
        unresolved: Vec<String>,
    },
    Ready,
    Status {
        status: IpcStatus,
    },
    ShuttingDown,
    Error {
        error: IpcError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IpcResponse {
    pub magic: String,
    pub version: u16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_features: Vec<String>,
    pub request_id: u64,
    pub kind: IpcResponseKind,
}

impl IpcResponse {
    fn new(request_id: u64, kind: IpcResponseKind) -> Self {
        Self {
            magic: IPC_MAGIC.to_string(),
            version: IPC_VERSION,
            required_features: Vec::new(),
            request_id,
            kind,
        }
    }

    pub fn peers(request_id: u64, peers: Vec<IpcPeer>, unresolved: Vec<String>) -> Self {
        Self::new(request_id, IpcResponseKind::Peers { peers, unresolved })
    }

    pub fn ready(request_id: u64) -> Self {
        Self::new(request_id, IpcResponseKind::Ready)
    }

    pub fn status(request_id: u64, status: IpcStatus) -> Self {
        Self::new(request_id, IpcResponseKind::Status { status })
    }

    pub fn shutting_down(request_id: u64) -> Self {
        Self::new(request_id, IpcResponseKind::ShuttingDown)
    }

    pub fn error(request_id: u64, error: IpcError) -> Self {
        Self::new(request_id, IpcResponseKind::Error { error })
    }

    fn into_kind(self, request_id: u64) -> Result<IpcResponseKind> {
        if self.magic != IPC_MAGIC {
            bail!("the sync IPC response has the wrong magic value");
        }
        if self.version != IPC_VERSION {
            bail!(
                "the sync IPC response uses incompatible version {}",
                self.version
            );
        }
        if !self.required_features.is_empty() {
            bail!(
                "the sync IPC response requires unsupported features: {}",
                self.required_features.join(", ")
            );
        }
        if self.request_id != request_id {
            bail!(
                "the sync IPC response ID {} does not match request {request_id}",
                self.request_id
            );
        }
        match self.kind {
            IpcResponseKind::Error { error } => Err(error.into()),
            kind => Ok(kind),
        }
    }
}

pub async fn write_message<W, T>(writer: &mut W, message: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let encoded = serde_json::to_vec(message).context("encoding a sync IPC control frame")?;
    if encoded.len() > MAX_CONTROL_FRAME {
        bail!(
            "sync IPC control frame of {} bytes exceeds the {}-byte limit",
            encoded.len(),
            MAX_CONTROL_FRAME
        );
    }
    writer
        .write_all(&(encoded.len() as u32).to_be_bytes())
        .await?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_message<R, T>(reader: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let len = reader.read_u32().await? as usize;
    if len > MAX_CONTROL_FRAME {
        bail!("sync IPC control frame of {len} bytes exceeds the {MAX_CONTROL_FRAME}-byte limit");
    }
    let mut encoded = vec![0; len];
    reader.read_exact(&mut encoded).await?;
    serde_json::from_slice(&encoded).context("decoding a sync IPC control frame")
}

async fn read_request(stream: &mut UnixStream) -> Result<IpcRequest> {
    tokio::time::timeout(HANDSHAKE_TIMEOUT, read_message(stream))
        .await
        .context("the sync IPC request did not arrive within 5 seconds")?
}

pub async fn write_response(stream: &mut UnixStream, response: &IpcResponse) -> Result<()> {
    tokio::time::timeout(HANDSHAKE_TIMEOUT, write_message(stream, response))
        .await
        .context("the sync IPC response did not leave within 5 seconds")?
}

/// An owner-only Unix listener. Accepted clients must have the server's UID.
pub struct IpcListener {
    listener: UnixListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl IpcListener {
    pub fn bind(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let listener = UnixListener::bind(path)
            .with_context(|| format!("binding sync IPC socket {}", path.display()))?;
        let restricted = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("restricting sync IPC socket {}", path.display()))
            .and_then(|()| verify_owner_only_socket(path));
        if let Err(error) = restricted {
            drop(listener);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }
        let metadata = std::fs::metadata(path)?;
        Ok(Self {
            listener,
            path: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    async fn accept_stream(&self) -> Result<UnixStream> {
        let (stream, _) = self.listener.accept().await?;
        verify_same_user(&stream)?;
        Ok(stream)
    }

    /// Accept and validate one complete control request. Invalid protocol
    /// headers receive a structured refusal before this returns `None`.
    pub async fn accept_request(
        &self,
        expected_nonce: &IpcNonce,
    ) -> Result<Option<(UnixStream, IpcRequest)>> {
        let mut stream = self.accept_stream().await?;
        let request = read_request(&mut stream).await?;
        if let Err(error) = request.validate(expected_nonce) {
            write_response(&mut stream, &IpcResponse::error(request.request_id, error)).await?;
            return Ok(None);
        }
        Ok(Some((stream, request)))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for IpcListener {
    fn drop(&mut self) {
        let same_socket = std::fs::metadata(&self.path)
            .is_ok_and(|metadata| metadata.dev() == self.device && metadata.ino() == self.inode);
        if same_socket {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn verify_owner_only_socket(path: &Path) -> Result<()> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading sync IPC socket {}", path.display()))?;
    if !metadata.file_type().is_socket() {
        bail!("sync IPC path {} is not a Unix socket", path.display());
    }
    if metadata.uid() != current_uid() {
        bail!("sync IPC socket {} has a different owner", path.display());
    }
    if metadata.mode() & 0o777 != 0o600 {
        bail!("sync IPC socket {} must have mode 0600", path.display());
    }
    Ok(())
}

fn current_uid() -> libc::uid_t {
    // SAFETY: geteuid has no preconditions and changes no state.
    unsafe { libc::geteuid() }
}

#[cfg(target_os = "linux")]
fn verify_same_user(stream: &UnixStream) -> Result<()> {
    let mut credentials = std::mem::MaybeUninit::<libc::ucred>::uninit();
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: credentials points to enough aligned space, and len describes it.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            credentials.as_mut_ptr().cast(),
            &mut len,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("reading sync IPC peer credentials");
    }
    if len as usize != std::mem::size_of::<libc::ucred>() {
        bail!("the sync IPC peer credentials had an unexpected size");
    }
    // SAFETY: getsockopt succeeded and wrote the complete credential value.
    let credentials = unsafe { credentials.assume_init() };
    if credentials.uid != current_uid() {
        bail!("the sync IPC client has a different user");
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn verify_same_user(stream: &UnixStream) -> Result<()> {
    let mut uid = 0;
    let mut gid = 0;
    // SAFETY: uid and gid are valid output pointers for getpeereid.
    let result = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).context("reading sync IPC peer credentials");
    }
    if uid != current_uid() {
        bail!("the sync IPC client has a different user");
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn verify_same_user(_stream: &UnixStream) -> Result<()> {
    bail!("sync IPC peer credential checks are unsupported on this platform")
}

/// The client-side transport for the local bridge. No production path creates
/// this type until the companion process is ready for activation.
#[derive(Clone)]
pub struct IpcSyncTransport {
    socket_path: PathBuf,
    nonce: IpcNonce,
    next_request_id: Arc<AtomicU64>,
    timeout: Duration,
}

impl IpcSyncTransport {
    pub fn new(socket_path: impl Into<PathBuf>, nonce: IpcNonce) -> Self {
        Self {
            socket_path: socket_path.into(),
            nonce,
            next_request_id: Arc::new(AtomicU64::new(1)),
            timeout: HANDSHAKE_TIMEOUT,
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    async fn request(&self, kind: IpcRequestKind) -> Result<(UnixStream, IpcResponseKind)> {
        verify_owner_only_socket(&self.socket_path)?;
        let request_id = self
            .next_request_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| anyhow::anyhow!("the sync IPC request ID space was exhausted"))?;
        let request = IpcRequest::new(self.nonce.clone(), request_id, kind);
        let mut stream = tokio::time::timeout(self.timeout, UnixStream::connect(&self.socket_path))
            .await
            .context("the sync IPC socket did not accept within the handshake timeout")?
            .with_context(|| {
                format!(
                    "connecting to sync IPC socket {}",
                    self.socket_path.display()
                )
            })?;
        tokio::time::timeout(self.timeout, write_message(&mut stream, &request))
            .await
            .context("the sync IPC request did not leave within the handshake timeout")??;
        let response: IpcResponse = tokio::time::timeout(self.timeout, read_message(&mut stream))
            .await
            .context("the sync IPC response did not arrive within the handshake timeout")??;
        Ok((stream, response.into_kind(request_id)?))
    }

    pub async fn resolve_peers(&self, peers: &SyncPeers) -> Result<ResolvedPeers> {
        let (_, response) = self
            .request(IpcRequestKind::ResolvePeers {
                selectors: peers.into(),
            })
            .await?;
        let IpcResponseKind::Peers { peers, unresolved } = response else {
            bail!("the sync IPC peer request received the wrong response kind");
        };
        Ok(ResolvedPeers {
            peers: peers.into_iter().map(PeerRef::from).collect(),
            unresolved,
        })
    }

    pub async fn open_outbound(&self, peer: PeerRef, sync_name: String) -> Result<UnixStream> {
        let (stream, response) = self
            .request(IpcRequestKind::OpenOutbound {
                peer: peer.into(),
                sync_name,
            })
            .await?;
        if response != IpcResponseKind::Ready {
            bail!("the sync IPC open request received the wrong response kind");
        }
        Ok(stream)
    }

    pub async fn status(&self) -> Result<IpcStatus> {
        let (_, response) = self.request(IpcRequestKind::Status).await?;
        let IpcResponseKind::Status { status } = response else {
            bail!("the sync IPC status request received the wrong response kind");
        };
        Ok(status)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (_, response) = self.request(IpcRequestKind::Shutdown).await?;
        if response != IpcResponseKind::ShuttingDown {
            bail!("the sync IPC shutdown request received the wrong response kind");
        }
        Ok(())
    }

    #[cfg(test)]
    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
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
    ) -> Result<super::Reconciled> {
        let stream = self.open_outbound(peer.clone(), name.clone()).await?;
        super::wire::run_client(stream, node, &name, &peer.id).await
    }
}

/// Carry raw sync-wire bytes after the control handshake completes.
pub async fn relay_raw<A, B>(mut left: A, mut right: B) -> Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let copied = tokio::io::copy_bidirectional(&mut left, &mut right).await?;
    let _ = left.shutdown().await;
    let _ = right.shutdown().await;
    Ok(copied)
}

#[cfg(test)]
mod tests {
    use std::{
        os::unix::fs::PermissionsExt,
        sync::Arc,
        time::{Duration, Instant},
    };

    use anyhow::Result;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::Mutex,
    };

    use super::*;
    use crate::sync::{SyncNode, SyncPeers, SyncTransport, manifest::Author, node::content_hash};

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
                    crate::sync::wire::run_server(stream, "reference-client", move |hello| {
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
                IpcRequestKind::OpenInbound { .. } => {
                    write_response(
                        &mut stream,
                        &IpcResponse::error(
                            request.request_id,
                            IpcError::new(
                                IpcErrorKind::Unavailable,
                                "no inbound engine in this test",
                            ),
                        ),
                    )
                    .await?;
                }
            }
        }
    }

    async fn reference_transport() -> Result<(
        IpcSyncTransport,
        Arc<Mutex<SyncNode>>,
        tokio::task::JoinHandle<Result<()>>,
        tempfile::TempDir,
    )> {
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
        Ok((IpcSyncTransport::new(socket, nonce), remote, server, dir))
    }

    #[tokio::test]
    async fn ipc_transport_passes_peer_and_reconcile_conformance() -> Result<()> {
        let (transport, _remote, server, _dir) = reference_transport().await?;
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
    async fn a_wrong_instance_nonce_gets_a_structured_refusal() -> Result<()> {
        let (transport, _remote, server, _dir) = reference_transport().await?;
        let wrong = IpcSyncTransport::new(
            transport.socket_path().to_path_buf(),
            IpcNonce::new("fedcba9876543210")?,
        );
        let error = wrong
            .status()
            .await
            .expect_err("the wrong nonce was accepted");
        assert!(format!("{error:#}").contains("unauthorized"));

        transport.shutdown().await?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn an_incompatible_version_gets_a_structured_refusal() -> Result<()> {
        let (transport, _remote, server, _dir) = reference_transport().await?;
        let mut stream = UnixStream::connect(transport.socket_path()).await?;
        let mut request = IpcRequest::new(transport.nonce.clone(), 41, IpcRequestKind::Status);
        request.version = IPC_VERSION + 1;
        write_message(&mut stream, &request).await?;
        let response: IpcResponse = read_message(&mut stream).await?;
        assert!(matches!(
            response.kind,
            IpcResponseKind::Error {
                error: IpcError {
                    kind: IpcErrorKind::Incompatible,
                    ..
                }
            }
        ));

        transport.shutdown().await?;
        server.await??;
        Ok(())
    }

    #[test]
    fn an_unknown_required_feature_is_incompatible() -> Result<()> {
        let nonce = IpcNonce::new("0123456789abcdef")?;
        let mut request = IpcRequest::new(nonce.clone(), 7, IpcRequestKind::Status);
        request
            .required_features
            .push("future-critical-field".into());

        let error = request
            .validate(&nonce)
            .expect_err("an unknown required feature was ignored");
        assert_eq!(error.kind, IpcErrorKind::Incompatible);
        assert!(error.message.contains("future-critical-field"));
        Ok(())
    }

    #[tokio::test]
    async fn an_inbound_header_keeps_the_authenticated_peer_identity() -> Result<()> {
        let nonce = IpcNonce::new("0123456789abcdef")?;
        let sent = IpcRequest::new(
            nonce.clone(),
            19,
            IpcRequestKind::OpenInbound {
                authenticated_peer_id: "node-id-from-daemon".into(),
                display_label: Some("hetz".into()),
                sync_name: "catalog".into(),
            },
        );
        let (mut writer, mut reader) = tokio::io::duplex(MAX_CONTROL_FRAME);
        write_message(&mut writer, &sent).await?;
        let received: IpcRequest = read_message(&mut reader).await?;
        received.validate(&nonce)?;
        assert_eq!(received, sent);
        assert!(matches!(
            received.kind,
            IpcRequestKind::OpenInbound {
                authenticated_peer_id,
                display_label: Some(display_label),
                sync_name,
            } if authenticated_peer_id == "node-id-from-daemon"
                && display_label == "hetz"
                && sync_name == "catalog"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn control_frames_refuse_oversized_payloads_before_writing() {
        let (mut writer, mut reader) = tokio::io::duplex(MAX_CONTROL_FRAME * 2);
        let oversized = "x".repeat(MAX_CONTROL_FRAME + 1);
        let error = write_message(&mut writer, &oversized)
            .await
            .expect_err("an oversized control frame was accepted");
        assert!(format!("{error:#}").contains("exceeds"));

        let mut byte = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(20), reader.read_exact(&mut byte))
                .await
                .is_err(),
            "the rejected frame wrote bytes before it checked the limit"
        );
    }

    #[tokio::test]
    async fn declared_oversized_frames_fail_before_a_payload_arrives() -> Result<()> {
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer
            .write_all(&((MAX_CONTROL_FRAME + 1) as u32).to_be_bytes())
            .await?;
        let error = tokio::time::timeout(
            Duration::from_millis(20),
            read_message::<_, serde_json::Value>(&mut reader),
        )
        .await
        .expect("the reader waited for a rejected payload")
        .expect_err("an oversized declared frame was accepted");
        assert!(format!("{error:#}").contains("exceeds"));
        Ok(())
    }

    #[tokio::test]
    async fn the_bridge_socket_is_owner_only() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("sync.sock");
        let _listener = IpcListener::bind(&socket)?;
        let mode = std::fs::metadata(socket)?.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        Ok(())
    }

    #[tokio::test]
    async fn the_client_refuses_a_socket_that_stops_being_owner_only() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("sync.sock");
        let _listener = IpcListener::bind(&socket)?;
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o660))?;
        let transport = IpcSyncTransport::new(socket, IpcNonce::new("0123456789abcdef")?);

        let error = transport
            .status()
            .await
            .expect_err("the client accepted a group-writable bridge socket");
        assert!(format!("{error:#}").contains("mode 0600"));
        Ok(())
    }

    #[tokio::test]
    async fn raw_relay_preserves_both_directions_and_half_closes() -> Result<()> {
        let (mut left_client, left_relay) = UnixStream::pair()?;
        let (right_relay, mut right_client) = UnixStream::pair()?;
        let relay = tokio::spawn(relay_raw(left_relay, right_relay));

        left_client.write_all(b"left to right").await?;
        let mut from_left = vec![0; 13];
        right_client.read_exact(&mut from_left).await?;
        assert_eq!(from_left, b"left to right");

        right_client.write_all(b"right to left").await?;
        let mut from_right = vec![0; 13];
        left_client.read_exact(&mut from_right).await?;
        assert_eq!(from_right, b"right to left");

        left_client.shutdown().await?;
        right_client.shutdown().await?;
        let copied = tokio::time::timeout(Duration::from_secs(1), relay).await???;
        assert_eq!(copied, (13, 13));
        Ok(())
    }

    #[tokio::test]
    async fn a_stalled_handshake_has_a_deadline() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let socket = dir.path().join("sync.sock");
        let listener = IpcListener::bind(&socket)?;
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.listener.accept().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            Result::<()>::Ok(())
        });
        let transport = IpcSyncTransport::new(socket, IpcNonce::new("0123456789abcdef")?)
            .with_timeout(Duration::from_millis(20));

        let started = Instant::now();
        let error = transport
            .status()
            .await
            .expect_err("a stalled handshake had no deadline");
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(format!("{error:#}").contains("handshake timeout"));
        server.await??;
        Ok(())
    }
}
