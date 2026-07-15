use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use anyhow::{Context, Result, bail};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{
        AfterHandshakeOutcome, Connection, EndpointHooks, Incoming, RecvStream, SendStream, Side,
        TransportAddrUsage, VarInt, presets,
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Mutex, RwLock, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{FabricHome, Peer, PeerBook, load_or_create_identity, validate_protocol},
    control::{ControlRequest, ControlResponse, PeerReachability},
    shell, tunnel,
};

const BUILTIN_ECHO_ALPN: &[u8] = b"fabric/echo/0";
const REACHABILITY_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_EXEC_MAX_CHILDREN: usize = 32;

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
    exposures: RwLock<HashMap<Vec<u8>, Exposure>>,
    dial_sockets: Mutex<HashMap<(String, String), DialSocket>>,
    tunnel_sessions: tunnel::ServerSessions,
    tunnel_drop_tx: watch::Sender<u64>,
    tunnel_blocked: AtomicBool,
    builtin_echo_hits: AtomicUsize,
    allow_shell: bool,
    cancel: CancellationToken,
}

#[derive(Debug, Clone)]
struct DialSocket {
    path: PathBuf,
    peer_addr: EndpointAddr,
}

#[derive(Debug, Clone)]
enum Exposure {
    Socket(PathBuf),
    Exec {
        argv: Vec<String>,
        limit: Arc<tunnel::ExecLimit>,
    },
}

impl Exposure {
    fn to_server_target(&self) -> tunnel::ServerTarget {
        match self {
            Self::Socket(path) => tunnel::ServerTarget::UnixSocket(path.clone()),
            Self::Exec { argv, limit } => tunnel::ServerTarget::Exec {
                argv: argv.clone(),
                limit: limit.clone(),
            },
        }
    }
}

#[derive(Debug)]
struct RestartPlan {
    log: PathBuf,
    allow_shell: bool,
}

impl DaemonState {
    async fn new(
        home: FabricHome,
        cancel: CancellationToken,
        allow_shell: bool,
    ) -> Result<Arc<Self>> {
        home.prepare()?;
        let secret_key = load_or_create_identity(&home)?;
        let peer_book = PeerBook::load(&home)?;
        let allowed = Arc::new(RwLock::new(peer_book.trusted_ids()));
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .alpns(accepted_alpns(&HashMap::new()))
            .hooks(AllowListHook {
                allowed: allowed.clone(),
            })
            .bind()
            .await?;

        let _ = tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await;
        let (tunnel_drop_tx, _) = watch::channel(0);

        Ok(Arc::new(Self {
            home,
            endpoint,
            peer_book: RwLock::new(peer_book),
            allowed,
            exposures: RwLock::new(HashMap::new()),
            dial_sockets: Mutex::new(HashMap::new()),
            tunnel_sessions: Arc::new(Mutex::new(HashMap::new())),
            tunnel_drop_tx,
            tunnel_blocked: AtomicBool::new(false),
            builtin_echo_hits: AtomicUsize::new(0),
            allow_shell,
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
        if matches_reserved_alpn(&alpn) {
            bail!("{protocol:?} is reserved for fabric's built-in protocols");
        }
        if !socket.is_absolute() {
            bail!("expose socket must be an absolute path");
        }

        let mut exposures = self.exposures.write().await;
        exposures.insert(alpn, Exposure::Socket(socket));
        self.endpoint.set_alpns(accepted_alpns(&exposures));
        Ok(())
    }

    pub async fn expose_exec(
        &self,
        protocol: &str,
        argv: Vec<String>,
        max_children: usize,
    ) -> Result<()> {
        let alpn = validate_protocol(protocol)?;
        if matches_reserved_alpn(&alpn) {
            bail!("{protocol:?} is reserved for fabric's built-in protocols");
        }
        if argv.is_empty() {
            bail!("exec exposure requires a command");
        }
        if max_children == 0 {
            bail!("exec exposure max children must be greater than zero");
        }

        let mut exposures = self.exposures.write().await;
        exposures.insert(
            alpn,
            Exposure::Exec {
                argv,
                limit: tunnel::ExecLimit::new(max_children),
            },
        );
        self.endpoint.set_alpns(accepted_alpns(&exposures));
        Ok(())
    }

    pub async fn reap_tunnel_sessions(&self, ttl: Duration) -> usize {
        tunnel::reap_server_sessions(self.tunnel_sessions.clone(), ttl).await
    }

    pub async fn ping(&self, peer: &str) -> Result<PingOutcome> {
        let peer_addr = self.peer_book.read().await.resolve(peer)?;
        self.ping_addr(peer, peer_addr).await
    }

    async fn ping_addr(&self, peer: &str, peer_addr: EndpointAddr) -> Result<PingOutcome> {
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
        let mut transport = classify_connection_transport(&connection);
        if transport.is_none()
            && let Some(info) = self.endpoint.remote_info(peer_addr.id).await
        {
            transport = classify_remote_transport(&info);
        }
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
            transport,
        })
    }

    pub async fn dial(&self, peer: &str, protocol: &str) -> Result<PathBuf> {
        let alpn = validate_protocol(protocol)?;
        self.dial_alpn(peer, protocol, alpn, true).await
    }

    async fn dial_alpn(
        &self,
        peer: &str,
        protocol: &str,
        alpn: Vec<u8>,
        reuse_existing: bool,
    ) -> Result<PathBuf> {
        let peer_addr = self.peer_book.read().await.resolve(peer)?;
        let key = (peer_addr.id.to_string(), protocol.to_string());

        let mut sockets = self.dial_sockets.lock().await;
        if let Some(existing) = sockets.get(&key)
            && reuse_existing
            && existing.path.exists()
            && existing.peer_addr == peer_addr
        {
            return Ok(existing.path.clone());
        }

        let socket_path = self.home.dial_socket_path(peer_addr.id, protocol);
        if let Some(existing) = sockets.remove(&key) {
            let _ = fs::remove_file(existing.path);
        }
        if socket_path.exists() {
            fs::remove_file(&socket_path)
                .with_context(|| format!("failed to remove stale {}", socket_path.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        sockets.insert(
            key,
            DialSocket {
                path: socket_path.clone(),
                peer_addr: peer_addr.clone(),
            },
        );
        drop(sockets);

        if alpn == shell::SHELL_ALPN {
            tokio::spawn(run_raw_dial_socket(
                listener,
                self.endpoint.clone(),
                peer_addr,
                alpn,
                self.cancel.clone(),
            ));
        } else {
            tokio::spawn(run_dial_socket(
                listener,
                self.endpoint.clone(),
                self.home.clone(),
                peer.to_string(),
                alpn,
                self.cancel.clone(),
                self.tunnel_drop_rx(),
            ));
        }

        Ok(socket_path)
    }

    async fn local_status_fields(
        &self,
    ) -> Result<(String, serde_json::Value, Vec<String>, Vec<PathBuf>)> {
        let exposed_protocols = self
            .exposures
            .read()
            .await
            .keys()
            .map(|alpn| String::from_utf8_lossy(alpn).to_string())
            .collect();
        let dial_sockets = self
            .dial_sockets
            .lock()
            .await
            .values()
            .map(|socket| socket.path.clone())
            .collect();
        Ok((
            self.id().to_string(),
            serde_json::to_value(self.addr())?,
            exposed_protocols,
            dial_sockets,
        ))
    }

    async fn status_response(&self) -> Result<ControlResponse> {
        let (node_id, endpoint_addr, exposed_protocols, dial_sockets) =
            self.local_status_fields().await?;
        Ok(ControlResponse::Status {
            node_id,
            endpoint_addr,
            exposed_protocols,
            dial_sockets,
            allow_shell: self.allow_shell,
        })
    }

    async fn reachability_status_response(&self) -> Result<ControlResponse> {
        let (node_id, endpoint_addr, exposed_protocols, dial_sockets) =
            self.local_status_fields().await?;
        let peers = self.peer_reachability().await;
        Ok(ControlResponse::ReachabilityStatus {
            version: crate::version_string(),
            node_id,
            endpoint_addr,
            exposed_protocols,
            dial_sockets,
            allow_shell: self.allow_shell,
            peers,
        })
    }

    fn schedule_restart(&self, allow_shell: Option<bool>) -> Result<RestartPlan> {
        let allow_shell = allow_shell.unwrap_or(self.allow_shell);
        self.home.prepare()?;
        let log_path = self.home.restart_log_path();
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("failed to open {}", log_path.display()))?;
        writeln!(
            log,
            "fabric restart requested: version={} allow_shell={allow_shell}",
            crate::version_string()
        )?;
        let err = log.try_clone()?;
        let exe = std::env::current_exe()?;
        let mut command = ProcessCommand::new(exe);
        command
            .arg("--home")
            .arg(self.home.root())
            .arg("restart-detacher");
        if allow_shell {
            command.arg("--allow-shell");
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(err));

        #[cfg(unix)]
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        command
            .spawn()
            .with_context(|| "failed to spawn restart detacher")?;

        Ok(RestartPlan {
            log: log_path,
            allow_shell,
        })
    }

    pub async fn peer_reachability(&self) -> Vec<PeerReachability> {
        let peers = self.peer_book.read().await.peers().to_vec();
        let mut statuses = Vec::with_capacity(peers.len());
        for peer in peers {
            statuses.push(self.check_peer_reachability(peer).await);
        }
        statuses
    }

    async fn check_peer_reachability(&self, peer: Peer) -> PeerReachability {
        let addr = peer
            .addr
            .clone()
            .unwrap_or_else(|| EndpointAddr::new(peer.id));
        let label = peer.name.clone().unwrap_or_else(|| peer.id.to_string());

        match tokio::time::timeout(REACHABILITY_TIMEOUT, self.ping_addr(&label, addr)).await {
            Ok(Ok(pong)) => PeerReachability {
                id: peer.id.to_string(),
                name: peer.name,
                reachable: true,
                bytes: Some(pong.bytes),
                round_trip_micros: Some(pong.round_trip.as_micros().try_into().unwrap_or(u64::MAX)),
                transport: pong.transport,
                error: None,
            },
            Ok(Err(error)) => PeerReachability {
                id: peer.id.to_string(),
                name: peer.name,
                reachable: false,
                bytes: None,
                round_trip_micros: None,
                transport: None,
                error: Some(format!("{error:#}")),
            },
            Err(_) => PeerReachability {
                id: peer.id.to_string(),
                name: peer.name,
                reachable: false,
                bytes: None,
                round_trip_micros: None,
                transport: None,
                error: Some(format!(
                    "timed out after {:.1}s",
                    REACHABILITY_TIMEOUT.as_secs_f32()
                )),
            },
        }
    }

    pub fn builtin_echo_hits(&self) -> usize {
        self.builtin_echo_hits.load(Ordering::SeqCst)
    }

    fn tunnel_drop_rx(&self) -> watch::Receiver<u64> {
        self.tunnel_drop_tx.subscribe()
    }

    fn drop_tunnel_connections(&self) {
        let current = *self.tunnel_drop_tx.borrow();
        let _ = self.tunnel_drop_tx.send(current.wrapping_add(1));
    }

    fn set_tunnel_blocked(&self, blocked: bool) {
        self.tunnel_blocked.store(blocked, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone)]
pub struct PingOutcome {
    pub peer: String,
    pub bytes: usize,
    pub round_trip: Duration,
    pub transport: Option<String>,
}

pub struct FabricNode {
    state: Arc<DaemonState>,
    task: JoinHandle<Result<()>>,
}

impl FabricNode {
    pub async fn start(home: FabricHome) -> Result<Self> {
        Self::start_with_options(home, false).await
    }

    pub async fn start_with_options(home: FabricHome, allow_shell: bool) -> Result<Self> {
        let cancel = CancellationToken::new();
        let state = DaemonState::new(home, cancel, allow_shell).await?;
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

    pub async fn expose_exec(&self, protocol: &str, argv: Vec<String>) -> Result<()> {
        self.state
            .expose_exec(protocol, argv, DEFAULT_EXEC_MAX_CHILDREN)
            .await
    }

    pub async fn expose_exec_with_limit(
        &self,
        protocol: &str,
        argv: Vec<String>,
        max_children: usize,
    ) -> Result<()> {
        self.state.expose_exec(protocol, argv, max_children).await
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

pub async fn run_daemon(home: FabricHome, allow_shell: bool) -> Result<()> {
    FabricNode::start_with_options(home, allow_shell)
        .await?
        .wait()
        .await
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
    for socket in state.dial_sockets.lock().await.values() {
        let _ = fs::remove_file(&socket.path);
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
        ControlRequest::ReachabilityStatus => state.reachability_status_response().await?,
        ControlRequest::ReloadPeers => {
            state.reload_peers().await?;
            ControlResponse::Ok
        }
        ControlRequest::Expose { protocol, socket } => {
            state.expose(&protocol, socket).await?;
            ControlResponse::Ok
        }
        ControlRequest::ExposeExec {
            protocol,
            argv,
            max_children,
        } => {
            state.expose_exec(&protocol, argv, max_children).await?;
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
                transport: pong.transport,
            }
        }
        ControlRequest::Shell { peer } => {
            let socket = state
                .dial_alpn(
                    &peer,
                    shell::SHELL_PROTOCOL,
                    shell::SHELL_ALPN.to_vec(),
                    false,
                )
                .await?;
            ControlResponse::Shell { socket }
        }
        ControlRequest::DropTunnelConnections => {
            state.drop_tunnel_connections();
            ControlResponse::Ok
        }
        ControlRequest::SetTunnelBlocked { blocked } => {
            state.set_tunnel_blocked(blocked);
            ControlResponse::Ok
        }
        ControlRequest::ReapTunnelSessions { ttl_millis } => {
            state
                .reap_tunnel_sessions(Duration::from_millis(ttl_millis))
                .await;
            ControlResponse::Ok
        }
        ControlRequest::Restart { allow_shell } => {
            let restart = state.schedule_restart(allow_shell)?;
            ControlResponse::Restarting {
                log: restart.log,
                allow_shell: restart.allow_shell,
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
        handle_builtin_echo(connection, state).await?;
        return Ok(());
    }
    if alpn == shell::SHELL_ALPN {
        let connection = accepting.await?;
        handle_builtin_shell(connection, state).await?;
        return Ok(());
    }

    let exposure = {
        let exposures = state.exposures.read().await;
        exposures.get(alpn.as_slice()).cloned()
    };
    let Some(exposure) = exposure else {
        return Ok(());
    };

    let connection = accepting.await?;
    if state.tunnel_blocked.load(Ordering::SeqCst) {
        connection.close(0u32.into(), b"fabric tunnel blocked");
        return Ok(());
    }
    let peer_id = connection.remote_id();
    let (send, recv) = connection.accept_bi().await?;
    tunnel::serve_connection(
        connection,
        send,
        recv,
        peer_id,
        exposure.to_server_target(),
        state.tunnel_sessions.clone(),
        state.tunnel_drop_rx(),
    )
    .await?;
    Ok(())
}

async fn handle_builtin_echo(connection: Connection, state: Arc<DaemonState>) -> Result<()> {
    state.builtin_echo_hits.fetch_add(1, Ordering::SeqCst);
    let (mut send, mut recv) = connection.accept_bi().await?;
    tokio::io::copy(&mut recv, &mut send).await?;
    send.finish()?;
    connection.closed().await;
    Ok(())
}

async fn handle_builtin_shell(connection: Connection, state: Arc<DaemonState>) -> Result<()> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    if state.allow_shell {
        shell::serve_shell_session(&mut recv, &mut send).await?;
    } else {
        shell::serve_shell_disabled(&mut send).await?;
    }
    send.finish()?;
    connection.closed().await;
    Ok(())
}

fn accepted_alpns(exposures: &HashMap<Vec<u8>, Exposure>) -> Vec<Vec<u8>> {
    let mut alpns = Vec::with_capacity(exposures.len() + 2);
    alpns.push(BUILTIN_ECHO_ALPN.to_vec());
    alpns.push(shell::SHELL_ALPN.to_vec());
    alpns.extend(exposures.keys().cloned());
    alpns
}

fn matches_reserved_alpn(alpn: &[u8]) -> bool {
    alpn == BUILTIN_ECHO_ALPN || alpn == shell::SHELL_ALPN
}

fn classify_connection_transport(connection: &Connection) -> Option<String> {
    let paths = connection.paths();
    let mut selected_ip = false;
    let mut selected_relay = false;
    let mut any_ip = false;
    let mut any_relay = false;

    for path in paths.iter() {
        let is_ip = path.is_ip();
        let is_relay = path.is_relay();
        any_ip |= is_ip;
        any_relay |= is_relay;
        if path.is_selected() {
            selected_ip |= is_ip;
            selected_relay |= is_relay;
        }
    }

    classify_transport(selected_ip, selected_relay)
        .or_else(|| classify_transport(any_ip, any_relay))
}

fn classify_remote_transport(info: &iroh::endpoint::RemoteInfo) -> Option<String> {
    let mut active_ip = false;
    let mut active_relay = false;

    for addr in info.addrs() {
        if !matches!(addr.usage(), TransportAddrUsage::Active) {
            continue;
        }
        active_ip |= addr.addr().is_ip();
        active_relay |= addr.addr().is_relay();
    }

    classify_transport(active_ip, active_relay)
}

fn classify_transport(has_ip: bool, has_relay: bool) -> Option<String> {
    match (has_ip, has_relay) {
        (true, true) => Some("mixed".to_string()),
        (true, false) => Some("direct".to_string()),
        (false, true) => Some("relay".to_string()),
        (false, false) => None,
    }
}

async fn run_dial_socket(
    listener: UnixListener,
    endpoint: Endpoint,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((local, _)) = accepted else {
                    break;
                };
                let endpoint = endpoint.clone();
                let home = home.clone();
                let peer = peer.clone();
                let alpn = alpn.clone();
                let cancel = cancel.clone();
                let drop_rx = drop_rx.clone();
                tokio::spawn(async move {
                    if let Err(error) =
                        tunnel::run_client_connection(local, endpoint, home, peer, alpn, cancel, drop_rx)
                            .await
                    {
                        eprintln!("fabric: dial socket connection failed: {error:#}");
                    }
                });
            }
        }
    }
}

async fn run_raw_dial_socket(
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
                    if let Err(error) = handle_raw_dial_socket_connection(local, endpoint, peer_addr, alpn).await {
                        eprintln!("fabric: dial socket connection failed: {error:#}");
                    }
                });
            }
        }
    }
}

async fn handle_raw_dial_socket_connection(
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
