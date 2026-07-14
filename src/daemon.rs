use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{
        AfterHandshakeOutcome, Connection, EndpointHooks, Incoming, RecvStream, SendStream, Side,
        VarInt, presets,
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{FabricHome, PeerBook, load_or_create_identity, validate_protocol},
    control::{ControlRequest, ControlResponse},
};

const BUILTIN_ECHO_ALPN: &[u8] = b"fabric/echo/0";

#[derive(Debug)]
struct AllowListHook {
    allowed: Arc<RwLock<HashSet<EndpointId>>>,
}

impl EndpointHooks for AllowListHook {
    async fn after_handshake(&self, conn: &Connection) -> AfterHandshakeOutcome {
        if conn.side() == Side::Client {
            return AfterHandshakeOutcome::Accept;
        }

        if self.allowed.read().await.contains(&conn.remote_id()) {
            AfterHandshakeOutcome::Accept
        } else {
            AfterHandshakeOutcome::Reject {
                error_code: VarInt::from_u32(403),
                reason: b"node is not in fabric allow-list".to_vec(),
            }
        }
    }
}

#[derive(Debug)]
pub struct DaemonState {
    home: FabricHome,
    endpoint: Endpoint,
    peer_book: RwLock<PeerBook>,
    allowed: Arc<RwLock<HashSet<EndpointId>>>,
    exposures: RwLock<HashMap<Vec<u8>, PathBuf>>,
    dial_sockets: Mutex<HashMap<(String, String), PathBuf>>,
    cancel: CancellationToken,
}

impl DaemonState {
    async fn new(home: FabricHome, cancel: CancellationToken) -> Result<Arc<Self>> {
        home.prepare()?;
        let secret_key = load_or_create_identity(&home)?;
        let peer_book = PeerBook::load(&home)?;
        let allowed = Arc::new(RwLock::new(peer_book.trusted_ids()));
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .alpns(vec![BUILTIN_ECHO_ALPN.to_vec()])
            .hooks(AllowListHook {
                allowed: allowed.clone(),
            })
            .bind()
            .await?;

        let _ = tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await;

        Ok(Arc::new(Self {
            home,
            endpoint,
            peer_book: RwLock::new(peer_book),
            allowed,
            exposures: RwLock::new(HashMap::new()),
            dial_sockets: Mutex::new(HashMap::new()),
            cancel,
        }))
    }

    pub fn id(&self) -> EndpointId {
        self.endpoint.id()
    }

    pub fn addr(&self) -> EndpointAddr {
        self.endpoint.addr()
    }

    pub async fn reload_peers(&self) -> Result<()> {
        let peer_book = PeerBook::load(&self.home)?;
        *self.allowed.write().await = peer_book.trusted_ids();
        *self.peer_book.write().await = peer_book;
        Ok(())
    }

    pub async fn expose(&self, protocol: &str, socket: PathBuf) -> Result<()> {
        let alpn = validate_protocol(protocol)?;
        if alpn == BUILTIN_ECHO_ALPN {
            bail!("{protocol:?} is reserved for fabric's built-in echo protocol");
        }
        if !socket.is_absolute() {
            bail!("expose socket must be an absolute path");
        }

        let mut exposures = self.exposures.write().await;
        exposures.insert(alpn, socket);
        self.endpoint.set_alpns(accepted_alpns(&exposures));
        Ok(())
    }

    pub async fn ping(&self, peer: &str) -> Result<PingOutcome> {
        let peer_addr = self.peer_book.read().await.resolve(peer)?;
        let nonce = rand::random::<[u8; 32]>();
        let started = std::time::Instant::now();
        let connection = self
            .endpoint
            .connect(peer_addr.clone(), BUILTIN_ECHO_ALPN)
            .await
            .with_context(|| format!("failed to connect to {peer:?} built-in echo"))?;
        let (mut send, mut recv) = connection.open_bi().await?;

        send.write_all(&nonce).await?;
        send.finish()?;

        let response = recv.read_to_end(nonce.len() + 1).await?;
        let round_trip = started.elapsed();
        if response != nonce {
            bail!(
                "ping nonce mismatch from {peer:?}: sent {} bytes, got {} bytes",
                nonce.len(),
                response.len()
            );
        }

        Ok(PingOutcome {
            peer: peer_addr.id.to_string(),
            bytes: response.len(),
            round_trip,
        })
    }

    pub async fn dial(&self, peer: &str, protocol: &str) -> Result<PathBuf> {
        let alpn = validate_protocol(protocol)?;
        let peer_addr = self.peer_book.read().await.resolve(peer)?;
        let key = (peer_addr.id.to_string(), protocol.to_string());

        let mut sockets = self.dial_sockets.lock().await;
        if let Some(existing) = sockets.get(&key)
            && existing.exists()
        {
            return Ok(existing.clone());
        }

        let socket_path = self.home.dial_socket_path(peer_addr.id, protocol);
        if socket_path.exists() {
            fs::remove_file(&socket_path)
                .with_context(|| format!("failed to remove stale {}", socket_path.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        sockets.insert(key, socket_path.clone());
        drop(sockets);

        tokio::spawn(run_dial_socket(
            listener,
            self.endpoint.clone(),
            peer_addr,
            alpn,
            self.cancel.clone(),
        ));

        Ok(socket_path)
    }

    async fn status_response(&self) -> Result<ControlResponse> {
        let exposed_protocols = self
            .exposures
            .read()
            .await
            .keys()
            .map(|alpn| String::from_utf8_lossy(alpn).to_string())
            .collect();
        let dial_sockets = self.dial_sockets.lock().await.values().cloned().collect();
        Ok(ControlResponse::Status {
            node_id: self.id().to_string(),
            endpoint_addr: serde_json::to_value(self.addr())?,
            exposed_protocols,
            dial_sockets,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PingOutcome {
    pub peer: String,
    pub bytes: usize,
    pub round_trip: Duration,
}

pub struct FabricNode {
    state: Arc<DaemonState>,
    task: JoinHandle<Result<()>>,
}

impl FabricNode {
    pub async fn start(home: FabricHome) -> Result<Self> {
        let cancel = CancellationToken::new();
        let state = DaemonState::new(home, cancel).await?;
        let task = tokio::spawn(serve(state.clone()));
        Ok(Self { state, task })
    }

    pub fn state(&self) -> Arc<DaemonState> {
        self.state.clone()
    }

    pub fn id(&self) -> EndpointId {
        self.state.id()
    }

    pub fn addr(&self) -> EndpointAddr {
        self.state.addr()
    }

    pub async fn expose(&self, protocol: &str, socket: PathBuf) -> Result<()> {
        self.state.expose(protocol, socket).await
    }

    pub async fn dial(&self, peer: &str, protocol: &str) -> Result<PathBuf> {
        self.state.dial(peer, protocol).await
    }

    pub async fn ping(&self, peer: &str) -> Result<PingOutcome> {
        self.state.ping(peer).await
    }

    pub async fn shutdown(self) -> Result<()> {
        self.state.cancel.cancel();
        self.task.await?
    }

    pub async fn wait(self) -> Result<()> {
        self.task.await?
    }
}

pub async fn run_daemon(home: FabricHome) -> Result<()> {
    FabricNode::start(home).await?.wait().await
}

pub async fn send_control(home: &FabricHome, request: ControlRequest) -> Result<ControlResponse> {
    let mut stream = UnixStream::connect(home.control_socket_path())
        .await
        .with_context(|| "fabric daemon is not running; run `fabric up` first")?;
    let mut raw = serde_json::to_vec(&request)?;
    raw.push(b'\n');
    stream.write_all(&raw).await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let response: ControlResponse = serde_json::from_slice(&response)?;
    if let ControlResponse::Error { message } = response {
        bail!("{message}");
    }
    Ok(response)
}

async fn serve(state: Arc<DaemonState>) -> Result<()> {
    let control_path = state.home.control_socket_path();
    if control_path.exists() {
        fs::remove_file(&control_path)
            .with_context(|| format!("failed to remove stale {}", control_path.display()))?;
    }
    let control_listener = UnixListener::bind(&control_path)
        .with_context(|| format!("failed to bind {}", control_path.display()))?;

    tokio::select! {
        result = run_control_socket(control_listener, state.clone()) => result?,
        result = run_iroh_accept_loop(state.clone()) => result?,
        _ = state.cancel.cancelled() => {}
    }

    state.cancel.cancel();
    state.endpoint.close().await;
    let _ = fs::remove_file(control_path);
    for path in state.dial_sockets.lock().await.values() {
        let _ = fs::remove_file(path);
    }
    Ok(())
}

async fn run_control_socket(listener: UnixListener, state: Arc<DaemonState>) -> Result<()> {
    loop {
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                tokio::spawn(handle_control_stream(stream, state.clone()));
            }
        }
    }
    Ok(())
}

async fn handle_control_stream(stream: UnixStream, state: Arc<DaemonState>) {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let response = match async {
        reader.read_line(&mut line).await?;
        let request: ControlRequest = serde_json::from_str(&line)?;
        process_control_request(request, state).await
    }
    .await
    {
        Ok(response) => response,
        Err(error) => ControlResponse::Error {
            message: format!("{error:#}"),
        },
    };

    let mut stream = reader.into_inner();
    if let Ok(mut raw) = serde_json::to_vec(&response) {
        raw.push(b'\n');
        let _ = stream.write_all(&raw).await;
        let _ = stream.shutdown().await;
    }
}

async fn process_control_request(
    request: ControlRequest,
    state: Arc<DaemonState>,
) -> Result<ControlResponse> {
    let response = match request {
        ControlRequest::Status => state.status_response().await?,
        ControlRequest::ReloadPeers => {
            state.reload_peers().await?;
            ControlResponse::Ok
        }
        ControlRequest::Expose { protocol, socket } => {
            state.expose(&protocol, socket).await?;
            ControlResponse::Ok
        }
        ControlRequest::Dial { peer, protocol } => {
            let socket = state.dial(&peer, &protocol).await?;
            ControlResponse::Dial { socket }
        }
        ControlRequest::Ping { peer } => {
            let pong = state.ping(&peer).await?;
            ControlResponse::Pong {
                peer: pong.peer,
                bytes: pong.bytes,
                round_trip_micros: pong.round_trip.as_micros().try_into().unwrap_or(u64::MAX),
            }
        }
        ControlRequest::Shutdown => {
            state.cancel.cancel();
            ControlResponse::Ok
        }
    };
    Ok(response)
}

async fn run_iroh_accept_loop(state: Arc<DaemonState>) -> Result<()> {
    loop {
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            incoming = state.endpoint.accept() => {
                let Some(incoming) = incoming else {
                    break;
                };
                tokio::spawn(handle_incoming_iroh(incoming, state.clone()));
            }
        }
    }
    Ok(())
}

async fn handle_incoming_iroh(incoming: Incoming, state: Arc<DaemonState>) {
    if let Err(error) = process_incoming_iroh(incoming, state).await {
        eprintln!("fabric: incoming iroh connection failed: {error:#}");
    }
}

async fn process_incoming_iroh(incoming: Incoming, state: Arc<DaemonState>) -> Result<()> {
    let mut accepting = incoming.accept()?;
    let alpn = accepting.alpn().await?;
    if alpn == BUILTIN_ECHO_ALPN {
        let connection = accepting.await?;
        handle_builtin_echo(connection).await?;
        return Ok(());
    }

    let socket = {
        let exposures = state.exposures.read().await;
        exposures.get(alpn.as_slice()).cloned()
    };
    let Some(socket) = socket else {
        return Ok(());
    };

    let connection = accepting.await?;
    let (send, recv) = connection.accept_bi().await?;
    let local = UnixStream::connect(&socket)
        .await
        .with_context(|| format!("failed to connect exposed socket {}", socket.display()))?;
    pipe_unix_iroh(local, send, recv).await?;
    Ok(())
}

async fn handle_builtin_echo(connection: Connection) -> Result<()> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    tokio::io::copy(&mut recv, &mut send).await?;
    send.finish()?;
    connection.closed().await;
    Ok(())
}

fn accepted_alpns(exposures: &HashMap<Vec<u8>, PathBuf>) -> Vec<Vec<u8>> {
    let mut alpns = Vec::with_capacity(exposures.len() + 1);
    alpns.push(BUILTIN_ECHO_ALPN.to_vec());
    alpns.extend(exposures.keys().cloned());
    alpns
}

async fn run_dial_socket(
    listener: UnixListener,
    endpoint: Endpoint,
    peer_addr: EndpointAddr,
    alpn: Vec<u8>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((local, _)) = accepted else {
                    break;
                };
                let endpoint = endpoint.clone();
                let peer_addr = peer_addr.clone();
                let alpn = alpn.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_dial_socket_connection(local, endpoint, peer_addr, alpn).await {
                        eprintln!("fabric: dial socket connection failed: {error:#}");
                    }
                });
            }
        }
    }
}

async fn handle_dial_socket_connection(
    local: UnixStream,
    endpoint: Endpoint,
    peer_addr: EndpointAddr,
    alpn: Vec<u8>,
) -> Result<()> {
    let connection = endpoint.connect(peer_addr, &alpn).await?;
    let (send, recv) = connection.open_bi().await?;
    pipe_unix_iroh(local, send, recv).await?;
    Ok(())
}

async fn pipe_unix_iroh(
    local: UnixStream,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (mut local_read, mut local_write) = local.into_split();
    let to_remote = async {
        tokio::io::copy(&mut local_read, &mut send).await?;
        send.finish()?;
        Ok::<(), anyhow::Error>(())
    };
    let to_local = async {
        tokio::io::copy(&mut recv, &mut local_write).await?;
        let _ = local_write.shutdown().await;
        Ok::<(), anyhow::Error>(())
    };
    tokio::try_join!(to_remote, to_local)?;
    Ok(())
}
