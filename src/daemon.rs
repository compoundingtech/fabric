use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
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
    net::{TcpListener, UnixListener, UnixStream},
    sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{
        DEFAULT_EXEC_MAX_CHILDREN, FabricConfig, FabricHome, Peer, PeerBook, PersistedExpose,
        PersistedExposeTarget, load_or_create_identity, validate_protocol,
        validate_server_session_config, validate_tcp_addr,
    },
    control::{ControlRequest, ControlResponse, PeerReachability},
    shell, tunnel,
};

const BUILTIN_ECHO_ALPN: &[u8] = b"fabric/echo/0";
const REACHABILITY_TIMEOUT: Duration = Duration::from_secs(3);
const INCOMING_FAILURE_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const INCOMING_FAILURE_MAX_BACKOFF: Duration = Duration::from_secs(5);
const DIAL_FAILURE_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const DIAL_FAILURE_MAX_BACKOFF: Duration = Duration::from_secs(15);
const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(5);
const MAX_INCOMING_HANDLERS: usize = 32;
const MAX_DIAL_HANDLERS: usize = 32;

#[derive(Debug)]
struct FailureBackoff {
    state: Mutex<FailureBackoffState>,
    initial_delay: Duration,
    max_delay: Duration,
    log_interval: Duration,
}

#[derive(Debug)]
struct FailureBackoffState {
    consecutive_failures: usize,
    not_before: Instant,
    last_log: Option<Instant>,
    suppressed: usize,
}

impl FailureBackoff {
    fn new(initial_delay: Duration, max_delay: Duration, log_interval: Duration) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(FailureBackoffState {
                consecutive_failures: 0,
                not_before: Instant::now(),
                last_log: None,
                suppressed: 0,
            }),
            initial_delay,
            max_delay,
            log_interval,
        })
    }

    async fn wait(&self, cancel: &CancellationToken) -> bool {
        loop {
            let delay = {
                let state = self.state.lock().await;
                state.not_before.saturating_duration_since(Instant::now())
            };
            if delay.is_zero() {
                return true;
            }
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = cancel.cancelled() => return false,
            }
        }
    }

    async fn record_success(&self) {
        let mut state = self.state.lock().await;
        state.consecutive_failures = 0;
        state.not_before = Instant::now();
        state.suppressed = 0;
    }

    async fn record_failure(&self, label: &str, error: &(dyn fmt::Display + Sync)) {
        let (delay, consecutive_failures, suppressed, should_log) = {
            let now = Instant::now();
            let mut state = self.state.lock().await;
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            let delay = self.delay_for_step(state.consecutive_failures);
            state.not_before = now + delay;

            let should_log = state
                .last_log
                .is_none_or(|last_log| now.duration_since(last_log) >= self.log_interval);
            let suppressed = state.suppressed;
            if should_log {
                state.last_log = Some(now);
                state.suppressed = 0;
            } else {
                state.suppressed = state.suppressed.saturating_add(1);
            }

            (delay, state.consecutive_failures, suppressed, should_log)
        };

        if should_log {
            if suppressed > 0 {
                eprintln!(
                    "fabric: {label}: {error}; backing off for {delay:?} after {consecutive_failures} consecutive failures ({suppressed} similar failures suppressed)"
                );
            } else {
                eprintln!(
                    "fabric: {label}: {error}; backing off for {delay:?} after {consecutive_failures} consecutive failures"
                );
            }
        }
    }

    fn delay_for_step(&self, step: usize) -> Duration {
        let exponent = (step.saturating_sub(1)).min(8) as u32;
        let multiplier = 1u32 << exponent;
        self.initial_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }
}

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
    tcp_dials: Mutex<HashMap<(String, String, String), TcpDial>>,
    tunnel_sessions: tunnel::ServerSessionStore,
    tunnel_drop_tx: watch::Sender<u64>,
    tunnel_blocked: AtomicBool,
    builtin_echo_hits: AtomicUsize,
    allow_shell: bool,
    incoming_failures: Arc<FailureBackoff>,
    dial_failures: Arc<FailureBackoff>,
    incoming_slots: Arc<Semaphore>,
    dial_slots: Arc<Semaphore>,
    cancel: CancellationToken,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DaemonOptions {
    pub allow_shell: bool,
    pub server_session_max_total: Option<usize>,
    pub server_session_max_per_peer: Option<usize>,
    pub server_session_detached_ttl_secs: Option<u64>,
}

impl DaemonOptions {
    pub fn new(allow_shell: bool) -> Self {
        Self {
            allow_shell,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
struct DialSocket {
    path: PathBuf,
    peer_addr: EndpointAddr,
}

#[derive(Debug, Clone)]
struct TcpDial {
    addr: String,
    peer_addr: EndpointAddr,
}

#[derive(Debug, Clone)]
enum Exposure {
    Socket(PathBuf),
    Tcp {
        addr: String,
    },
    Exec {
        argv: Vec<String>,
        limit: Arc<tunnel::ExecLimit>,
    },
}

impl Exposure {
    fn to_server_target(&self) -> tunnel::ServerTarget {
        match self {
            Self::Socket(path) => tunnel::ServerTarget::UnixSocket(path.clone()),
            Self::Tcp { addr } => tunnel::ServerTarget::Tcp { addr: addr.clone() },
            Self::Exec { argv, limit } => tunnel::ServerTarget::Exec {
                argv: argv.clone(),
                limit: limit.clone(),
            },
        }
    }
}

fn load_persisted_exposures(home: &FabricHome) -> Result<HashMap<Vec<u8>, Exposure>> {
    let mut exposures = HashMap::new();
    for expose in FabricConfig::load(home)?.exposes() {
        let alpn = validate_protocol(&expose.protocol)?;
        if matches_reserved_alpn(&alpn) {
            bail!(
                "{:?} in {} is reserved for fabric's built-in protocols",
                expose.protocol,
                home.config_path().display()
            );
        }
        let exposure = match &expose.target {
            PersistedExposeTarget::Socket { socket } => {
                if !socket.is_absolute() {
                    bail!("expose socket must be an absolute path");
                }
                Exposure::Socket(socket.clone())
            }
            PersistedExposeTarget::Tcp { addr } => {
                validate_tcp_addr(addr)?;
                Exposure::Tcp { addr: addr.clone() }
            }
            PersistedExposeTarget::Exec { argv, max_children } => {
                if argv.is_empty() {
                    bail!("exec exposure requires a command");
                }
                if *max_children == 0 {
                    bail!("exec exposure max children must be greater than zero");
                }
                Exposure::Exec {
                    argv: argv.clone(),
                    limit: tunnel::ExecLimit::new(*max_children),
                }
            }
        };
        exposures.insert(alpn, exposure);
    }
    Ok(exposures)
}

fn set_config_allow_shell(home: &FabricHome, allow_shell: bool) -> Result<()> {
    let mut config = FabricConfig::load(home)?;
    config.set_allow_shell(allow_shell);
    config.save(home)
}

#[derive(Debug)]
struct RestartPlan {
    log: PathBuf,
    allow_shell: bool,
}

fn resolve_server_session_settings(
    config: &FabricConfig,
    options: DaemonOptions,
) -> Result<(tunnel::ServerSessionLimits, Duration)> {
    let server_sessions = config.server_sessions();
    let max_total = options
        .server_session_max_total
        .unwrap_or_else(|| server_sessions.max_total());
    let max_per_peer = options
        .server_session_max_per_peer
        .unwrap_or_else(|| server_sessions.max_per_peer());
    let detached_ttl_secs = options
        .server_session_detached_ttl_secs
        .unwrap_or_else(|| server_sessions.detached_ttl_secs());
    validate_server_session_config(max_total, max_per_peer, detached_ttl_secs)?;
    Ok((
        tunnel::ServerSessionLimits {
            max_total,
            max_per_peer,
        },
        Duration::from_secs(detached_ttl_secs),
    ))
}

impl DaemonState {
    async fn new(
        home: FabricHome,
        cancel: CancellationToken,
        options: DaemonOptions,
    ) -> Result<Arc<Self>> {
        home.prepare()?;
        let secret_key = load_or_create_identity(&home)?;
        if options.allow_shell {
            set_config_allow_shell(&home, true)?;
        }
        let config = FabricConfig::load(&home)?;
        let allow_shell = options.allow_shell || config.allow_shell().unwrap_or(false);
        let (tunnel_session_limits, tunnel_session_detached_ttl) =
            resolve_server_session_settings(&config, options)?;
        let peer_book = PeerBook::load(&home)?;
        let exposures = load_persisted_exposures(&home)?;
        let allowed = Arc::new(RwLock::new(peer_book.trusted_ids()));
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .alpns(accepted_alpns(&exposures))
            .hooks(AllowListHook {
                allowed: allowed.clone(),
            })
            .bind()
            .await?;

        let _ = tokio::time::timeout(Duration::from_secs(5), endpoint.online()).await;
        let (tunnel_drop_tx, _) = watch::channel(0);
        let tunnel_sessions =
            tunnel::ServerSessionStore::new(tunnel_session_limits, tunnel_session_detached_ttl);
        tunnel::spawn_server_session_reaper(tunnel_sessions.clone(), cancel.clone());

        Ok(Arc::new(Self {
            home,
            endpoint,
            peer_book: RwLock::new(peer_book),
            allowed,
            exposures: RwLock::new(exposures),
            dial_sockets: Mutex::new(HashMap::new()),
            tcp_dials: Mutex::new(HashMap::new()),
            tunnel_sessions,
            tunnel_drop_tx,
            tunnel_blocked: AtomicBool::new(false),
            builtin_echo_hits: AtomicUsize::new(0),
            allow_shell,
            incoming_failures: FailureBackoff::new(
                INCOMING_FAILURE_INITIAL_BACKOFF,
                INCOMING_FAILURE_MAX_BACKOFF,
                FAILURE_LOG_INTERVAL,
            ),
            dial_failures: FailureBackoff::new(
                DIAL_FAILURE_INITIAL_BACKOFF,
                DIAL_FAILURE_MAX_BACKOFF,
                FAILURE_LOG_INTERVAL,
            ),
            incoming_slots: Arc::new(Semaphore::new(MAX_INCOMING_HANDLERS)),
            dial_slots: Arc::new(Semaphore::new(MAX_DIAL_HANDLERS)),
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
        self.expose_socket(protocol, socket, true).await
    }

    async fn expose_socket(&self, protocol: &str, socket: PathBuf, persist: bool) -> Result<()> {
        let alpn = validate_protocol(protocol)?;
        if matches_reserved_alpn(&alpn) {
            bail!("{protocol:?} is reserved for fabric's built-in protocols");
        }
        if !socket.is_absolute() {
            bail!("expose socket must be an absolute path");
        }

        if persist {
            let mut config = FabricConfig::load(&self.home)?;
            config.upsert_expose(PersistedExpose::socket(
                protocol.to_string(),
                socket.clone(),
            ));
            config.save(&self.home)?;
        }

        let mut exposures = self.exposures.write().await;
        exposures.insert(alpn, Exposure::Socket(socket));
        self.endpoint.set_alpns(accepted_alpns(&exposures));
        Ok(())
    }

    pub async fn expose_tcp(&self, protocol: &str, addr: String) -> Result<()> {
        self.expose_tcp_with_persistence(protocol, addr, true).await
    }

    async fn expose_tcp_with_persistence(
        &self,
        protocol: &str,
        addr: String,
        persist: bool,
    ) -> Result<()> {
        let alpn = validate_protocol(protocol)?;
        if matches_reserved_alpn(&alpn) {
            bail!("{protocol:?} is reserved for fabric's built-in protocols");
        }
        validate_tcp_addr(&addr)?;

        if persist {
            let mut config = FabricConfig::load(&self.home)?;
            config.upsert_expose(PersistedExpose::tcp(protocol.to_string(), addr.clone()));
            config.save(&self.home)?;
        }

        let mut exposures = self.exposures.write().await;
        exposures.insert(alpn, Exposure::Tcp { addr });
        self.endpoint.set_alpns(accepted_alpns(&exposures));
        Ok(())
    }

    pub async fn expose_exec(
        &self,
        protocol: &str,
        argv: Vec<String>,
        max_children: usize,
    ) -> Result<()> {
        self.expose_exec_with_persistence(protocol, argv, max_children, true)
            .await
    }

    async fn expose_exec_with_persistence(
        &self,
        protocol: &str,
        argv: Vec<String>,
        max_children: usize,
        persist: bool,
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

        if persist {
            let mut config = FabricConfig::load(&self.home)?;
            config.upsert_expose(PersistedExpose::exec(
                protocol.to_string(),
                argv.clone(),
                max_children,
            ));
            config.save(&self.home)?;
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

    pub async fn expose_ephemeral(&self, protocol: &str, socket: PathBuf) -> Result<()> {
        self.expose_socket(protocol, socket, false).await
    }

    pub async fn expose_tcp_ephemeral(&self, protocol: &str, addr: String) -> Result<()> {
        self.expose_tcp_with_persistence(protocol, addr, false)
            .await
    }

    pub async fn expose_exec_ephemeral(
        &self,
        protocol: &str,
        argv: Vec<String>,
        max_children: usize,
    ) -> Result<()> {
        self.expose_exec_with_persistence(protocol, argv, max_children, false)
            .await
    }

    pub async fn unexpose(&self, protocol: &str) -> Result<()> {
        let alpn = validate_protocol(protocol)?;
        if matches_reserved_alpn(&alpn) {
            bail!("{protocol:?} is reserved for fabric's built-in protocols");
        }

        let mut config = FabricConfig::load(&self.home)?;
        config.remove_expose(protocol);
        config.save(&self.home)?;

        let mut exposures = self.exposures.write().await;
        exposures.remove(&alpn);
        self.endpoint.set_alpns(accepted_alpns(&exposures));
        Ok(())
    }

    pub async fn reap_tunnel_sessions(&self, ttl: Duration) -> usize {
        self.tunnel_sessions.reap_expired(ttl).await
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

    pub async fn dial_tcp(&self, peer: &str, protocol: &str, bind: String) -> Result<String> {
        validate_tcp_addr(&bind)?;
        let alpn = validate_protocol(protocol)?;
        let peer_addr = self.peer_book.read().await.resolve(peer)?;
        let key = (peer_addr.id.to_string(), protocol.to_string(), bind.clone());

        let mut tcp_dials = self.tcp_dials.lock().await;
        if let Some(existing) = tcp_dials.get_mut(&key) {
            existing.peer_addr = peer_addr;
            return Ok(existing.addr.clone());
        }
        let listener = TcpListener::bind(&bind)
            .await
            .with_context(|| format!("failed to bind tcp dial listener {bind}"))?;
        let addr = listener.local_addr()?.to_string();
        tcp_dials.insert(
            key,
            TcpDial {
                addr: addr.clone(),
                peer_addr: peer_addr.clone(),
            },
        );
        drop(tcp_dials);

        tokio::spawn(run_dial_tcp_listener(
            listener,
            self.endpoint.clone(),
            self.home.clone(),
            peer.to_string(),
            alpn,
            self.cancel.clone(),
            self.tunnel_drop_rx(),
            self.dial_failures.clone(),
            self.dial_slots.clone(),
        ));

        Ok(addr)
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
                self.dial_failures.clone(),
                self.dial_slots.clone(),
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
                self.dial_failures.clone(),
                self.dial_slots.clone(),
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

    fn schedule_restart(&self, requested_allow_shell: Option<bool>) -> Result<RestartPlan> {
        if let Some(allow_shell) = requested_allow_shell {
            set_config_allow_shell(&self.home, allow_shell)?;
        }
        let allow_shell = requested_allow_shell.unwrap_or(self.allow_shell);
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
        Self::start_with_daemon_options(home, DaemonOptions::new(allow_shell)).await
    }

    pub async fn start_with_daemon_options(
        home: FabricHome,
        options: DaemonOptions,
    ) -> Result<Self> {
        let cancel = CancellationToken::new();
        let state = DaemonState::new(home, cancel, options).await?;
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

    pub async fn expose_ephemeral(&self, protocol: &str, socket: PathBuf) -> Result<()> {
        self.state.expose_ephemeral(protocol, socket).await
    }

    pub async fn expose_tcp(&self, protocol: &str, addr: String) -> Result<()> {
        self.state.expose_tcp(protocol, addr).await
    }

    pub async fn expose_tcp_ephemeral(&self, protocol: &str, addr: String) -> Result<()> {
        self.state.expose_tcp_ephemeral(protocol, addr).await
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

    pub async fn expose_exec_ephemeral(
        &self,
        protocol: &str,
        argv: Vec<String>,
        max_children: usize,
    ) -> Result<()> {
        self.state
            .expose_exec_ephemeral(protocol, argv, max_children)
            .await
    }

    pub async fn unexpose(&self, protocol: &str) -> Result<()> {
        self.state.unexpose(protocol).await
    }

    pub async fn dial(&self, peer: &str, protocol: &str) -> Result<PathBuf> {
        self.state.dial(peer, protocol).await
    }

    pub async fn dial_tcp(&self, peer: &str, protocol: &str, bind: String) -> Result<String> {
        self.state.dial_tcp(peer, protocol, bind).await
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
    run_daemon_with_options(home, DaemonOptions::new(allow_shell)).await
}

pub async fn run_daemon_with_options(home: FabricHome, options: DaemonOptions) -> Result<()> {
    FabricNode::start_with_daemon_options(home, options)
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
        ControlRequest::Expose {
            protocol,
            socket,
            persist,
        } => {
            if persist {
                state.expose(&protocol, socket).await?;
            } else {
                state.expose_ephemeral(&protocol, socket).await?;
            }
            ControlResponse::Ok
        }
        ControlRequest::ExposeExec {
            protocol,
            argv,
            max_children,
            persist,
        } => {
            if persist {
                state.expose_exec(&protocol, argv, max_children).await?;
            } else {
                state
                    .expose_exec_ephemeral(&protocol, argv, max_children)
                    .await?;
            }
            ControlResponse::Ok
        }
        ControlRequest::ExposeTcp {
            protocol,
            addr,
            persist,
        } => {
            if persist {
                state.expose_tcp(&protocol, addr).await?;
            } else {
                state.expose_tcp_ephemeral(&protocol, addr).await?;
            }
            ControlResponse::Ok
        }
        ControlRequest::Unexpose { protocol } => {
            state.unexpose(&protocol).await?;
            ControlResponse::Ok
        }
        ControlRequest::Dial { peer, protocol } => {
            let socket = state.dial(&peer, &protocol).await?;
            ControlResponse::Dial { socket }
        }
        ControlRequest::DialTcp {
            peer,
            protocol,
            bind,
        } => {
            let addr = state.dial_tcp(&peer, &protocol, bind).await?;
            ControlResponse::DialTcp { addr }
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
        if !state.incoming_failures.wait(&state.cancel).await {
            break;
        }
        let permit = tokio::select! {
            _ = state.cancel.cancelled() => break,
            permit = state.incoming_slots.clone().acquire_owned() => {
                permit.context("incoming handler semaphore closed")?
            }
        };
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            incoming = state.endpoint.accept() => {
                let Some(incoming) = incoming else {
                    break;
                };
                tokio::spawn(handle_incoming_iroh(incoming, state.clone(), permit));
            }
        }
    }
    Ok(())
}

async fn handle_incoming_iroh(
    incoming: Incoming,
    state: Arc<DaemonState>,
    _permit: OwnedSemaphorePermit,
) {
    match process_incoming_iroh(incoming, state.clone()).await {
        Ok(()) => state.incoming_failures.record_success().await,
        Err(error) => {
            state
                .incoming_failures
                .record_failure("incoming iroh connection failed", &error)
                .await;
        }
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
    dial_failures: Arc<FailureBackoff>,
    dial_slots: Arc<Semaphore>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((local, _)) = accepted else {
                    break;
                };
                let permit = tokio::select! {
                    _ = cancel.cancelled() => break,
                    permit = dial_slots.clone().acquire_owned() => {
                        let Ok(permit) = permit else {
                            break;
                        };
                        permit
                    }
                };
                let endpoint = endpoint.clone();
                let home = home.clone();
                let peer = peer.clone();
                let alpn = alpn.clone();
                let cancel = cancel.clone();
                let drop_rx = drop_rx.clone();
                let dial_failures = dial_failures.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if !dial_failures.wait(&cancel).await {
                        return;
                    }
                    match
                        tunnel::run_client_connection(local, endpoint, home, peer, alpn, cancel, drop_rx)
                            .await
                    {
                        Ok(()) => dial_failures.record_success().await,
                        Err(error) => {
                            dial_failures
                                .record_failure("dial socket connection failed", &error)
                                .await;
                        }
                    }
                });
            }
        }
    }
}

async fn run_dial_tcp_listener(
    listener: TcpListener,
    endpoint: Endpoint,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
    dial_failures: Arc<FailureBackoff>,
    dial_slots: Arc<Semaphore>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((local, _)) = accepted else {
                    break;
                };
                let permit = tokio::select! {
                    _ = cancel.cancelled() => break,
                    permit = dial_slots.clone().acquire_owned() => {
                        let Ok(permit) = permit else {
                            break;
                        };
                        permit
                    }
                };
                let endpoint = endpoint.clone();
                let home = home.clone();
                let peer = peer.clone();
                let alpn = alpn.clone();
                let cancel = cancel.clone();
                let drop_rx = drop_rx.clone();
                let dial_failures = dial_failures.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if !dial_failures.wait(&cancel).await {
                        return;
                    }
                    match
                        tunnel::run_client_tcp_connection(local, endpoint, home, peer, alpn, cancel, drop_rx)
                            .await
                    {
                        Ok(()) => dial_failures.record_success().await,
                        Err(error) => {
                            dial_failures
                                .record_failure("dial tcp connection failed", &error)
                                .await;
                        }
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
    dial_failures: Arc<FailureBackoff>,
    dial_slots: Arc<Semaphore>,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((local, _)) = accepted else {
                    break;
                };
                let permit = tokio::select! {
                    _ = cancel.cancelled() => break,
                    permit = dial_slots.clone().acquire_owned() => {
                        let Ok(permit) = permit else {
                            break;
                        };
                        permit
                    }
                };
                let endpoint = endpoint.clone();
                let peer_addr = peer_addr.clone();
                let alpn = alpn.clone();
                let cancel = cancel.clone();
                let dial_failures = dial_failures.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if !dial_failures.wait(&cancel).await {
                        return;
                    }
                    match handle_raw_dial_socket_connection(local, endpoint, peer_addr, alpn).await {
                        Ok(()) => dial_failures.record_success().await,
                        Err(error) => {
                            dial_failures
                                .record_failure("dial socket connection failed", &error)
                                .await;
                        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_session_limit_options_override_config() {
        let config: FabricConfig = toml::from_str(
            r#"
            [server_sessions]
            max_total = 64
            max_per_peer = 16
            detached_ttl_secs = 60
            "#,
        )
        .unwrap();

        let (limits, detached_ttl) = resolve_server_session_settings(
            &config,
            DaemonOptions {
                server_session_max_total: Some(128),
                server_session_max_per_peer: Some(32),
                server_session_detached_ttl_secs: Some(45),
                ..DaemonOptions::default()
            },
        )
        .unwrap();

        assert_eq!(limits.max_total, 128);
        assert_eq!(limits.max_per_peer, 32);
        assert_eq!(detached_ttl, Duration::from_secs(45));
    }

    #[test]
    fn server_session_limit_options_validate_partial_overrides() {
        let config: FabricConfig = toml::from_str(
            r#"
            [server_sessions]
            max_total = 4
            max_per_peer = 2
            "#,
        )
        .unwrap();

        let error = resolve_server_session_settings(
            &config,
            DaemonOptions {
                server_session_max_per_peer: Some(8),
                ..DaemonOptions::default()
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("max_per_peer cannot exceed"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn server_session_limit_options_validate_ttl_override() {
        let config: FabricConfig = toml::from_str("").unwrap();

        let error = resolve_server_session_settings(
            &config,
            DaemonOptions {
                server_session_detached_ttl_secs: Some(0),
                ..DaemonOptions::default()
            },
        )
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("detached_ttl_secs must be greater than zero"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn failure_backoff_parks_after_failure_instead_of_tight_looping() {
        let backoff = FailureBackoff::new(
            Duration::from_millis(25),
            Duration::from_millis(100),
            Duration::from_secs(60),
        );
        let cancel = CancellationToken::new();

        backoff.record_failure("test failure", &"boom").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), backoff.wait(&cancel))
                .await
                .is_err(),
            "failed work should be parked instead of immediately retried"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(250), backoff.wait(&cancel))
                .await
                .expect("backoff did not clear")
        );
    }

    #[tokio::test]
    async fn failure_backoff_resets_after_success() {
        let backoff = FailureBackoff::new(
            Duration::from_millis(25),
            Duration::from_millis(100),
            Duration::from_secs(60),
        );
        let cancel = CancellationToken::new();

        backoff.record_failure("test failure", &"boom").await;
        backoff.record_success().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), backoff.wait(&cancel))
                .await
                .expect("success should clear backoff")
        );
    }

    #[tokio::test]
    async fn failure_backoff_can_be_cancelled_while_parked() {
        let backoff = FailureBackoff::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        let cancel = CancellationToken::new();

        backoff.record_failure("test failure", &"boom").await;
        cancel.cancel();
        assert!(!backoff.wait(&cancel).await);
    }
}
