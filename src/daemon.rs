use std::{
    collections::{BTreeSet, HashMap, HashSet},
    env, fmt,
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
use n0_watcher::Watcher as _;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, UnixListener, UnixStream},
    sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

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
const ENDPOINT_ONLINE_TIMEOUT: Duration = Duration::from_secs(5);
const ENDPOINT_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
const ENDPOINT_HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(30);
const ENDPOINT_HEALTH_POLL_FAILURES_BEFORE_RECYCLE: usize = 2;
const ENDPOINT_DIAGNOSTIC_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
const ENDPOINT_RECYCLE_MIN_INTERVAL: Duration = Duration::from_secs(60);
const ENDPOINT_RSS_RECYCLE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES: u64 = 300 * 1024 * 1024;
const ENDPOINT_RSS_RECYCLE_SETTLE_INTERVAL: Duration = Duration::from_secs(5);
const ENDPOINT_RSS_INEFFECTIVE_INITIAL_BACKOFF: Duration = Duration::from_secs(5 * 60);
const ENDPOINT_RSS_INEFFECTIVE_MAX_BACKOFF: Duration = Duration::from_secs(15 * 60);
const NETWORK_CHANGE_DEBOUNCE: Duration = Duration::from_millis(140);
const VALIDATION_LOG_TARGET: &str = "fabric::validation";

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
    endpoint_tx: watch::Sender<CurrentEndpoint>,
    endpoint_recycle: Mutex<()>,
    last_endpoint_recycle: Mutex<Option<Instant>>,
    peer_book: RwLock<PeerBook>,
    allowed: Arc<RwLock<HashSet<EndpointId>>>,
    exposures: RwLock<HashMap<Vec<u8>, Exposure>>,
    dial_sockets: Mutex<HashMap<(String, String), DialSocket>>,
    tcp_dials: Mutex<HashMap<(String, String, String), TcpDial>>,
    tunnel_sessions: tunnel::ServerSessionStore,
    tunnel_drop_tx: watch::Sender<u64>,
    tunnel_blocked: AtomicBool,
    network_usable: AtomicBool,
    builtin_echo_hits: AtomicUsize,
    allow_shell: bool,
    incoming_failures: Arc<FailureBackoff>,
    dial_failures: Arc<FailureBackoff>,
    incoming_slots: Arc<Semaphore>,
    dial_slots: Arc<Semaphore>,
    cancel: CancellationToken,
}

#[derive(Debug, Clone)]
pub(crate) struct CurrentEndpoint {
    pub(crate) generation: u64,
    pub(crate) endpoint: Endpoint,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointRecycleOutcome {
    Recycled,
    StaleGeneration,
    RateLimited { retry_after: Duration },
}

type RssSampler = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

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

async fn build_daemon_endpoint(
    home: &FabricHome,
    allowed: Arc<RwLock<HashSet<EndpointId>>>,
    exposures: &HashMap<Vec<u8>, Exposure>,
) -> Result<Endpoint> {
    let secret_key = load_or_create_identity(home)?;
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .alpns(accepted_alpns(exposures))
        .hooks(AllowListHook { allowed })
        .bind()
        .await?;
    let _ = tokio::time::timeout(ENDPOINT_ONLINE_TIMEOUT, endpoint.online()).await;
    Ok(endpoint)
}

pub fn init_daemon_tracing(home: &FabricHome) -> Result<()> {
    home.prepare()?;
    let appender =
        tracing_appender::rolling::daily(home.validation_log_dir(), home.validation_log_prefix());
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(validation_log_filter())
        .with_target(true)
        .with_writer(appender)
        .finish();

    if tracing::subscriber::set_global_default(subscriber).is_ok() {
        info!(
            target: VALIDATION_LOG_TARGET,
            event = "diagnostic_logging_init",
            iroh_path_trace = env::var_os("FABRIC_IROH_PATH_TRACE").is_some(),
            "fabric validation logging initialized"
        );
    }

    Ok(())
}

fn validation_log_filter() -> EnvFilter {
    if let Ok(filter) = env::var("FABRIC_LOG") {
        return EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("fabric=info"));
    }

    let filter = if env::var_os("FABRIC_IROH_PATH_TRACE").is_some() {
        concat!(
            "fabric=info,",
            "iroh=warn,",
            "noq=warn,",
            "iroh::socket=debug,",
            "iroh::socket::remote_map=debug,",
            "iroh::socket::remote_map::remote_state=debug,",
            "noq_proto::connection=debug"
        )
    } else {
        "fabric=info,iroh=warn,noq=warn,netwatch=warn"
    };
    EnvFilter::new(filter)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NetworkChangeEvent {
    reason: String,
    network_usable: bool,
    coalesced_events: usize,
}

#[derive(Debug)]
struct NetworkChangeDebouncer {
    quiet_window: Duration,
    pending: Option<NetworkChangeEvent>,
    due_at: Option<Instant>,
}

impl NetworkChangeDebouncer {
    fn new(quiet_window: Duration) -> Self {
        Self {
            quiet_window,
            pending: None,
            due_at: None,
        }
    }

    fn record(&mut self, reason: String, network_usable: bool, now: Instant) {
        let (coalesced_events, due_at) = match self.pending.as_ref() {
            Some(event) => (event.coalesced_events.saturating_add(1), self.due_at),
            None => (1, Some(now + self.quiet_window)),
        };
        self.pending = Some(NetworkChangeEvent {
            reason,
            network_usable,
            coalesced_events,
        });
        self.due_at = due_at;
    }

    fn due_at(&self) -> Option<Instant> {
        self.due_at
    }

    fn take_due(&mut self, now: Instant) -> Option<NetworkChangeEvent> {
        if self.due_at.is_some_and(|due_at| now >= due_at) {
            self.due_at = None;
            return self.pending.take();
        }
        None
    }

    fn pending_count(&self) -> usize {
        self.pending
            .as_ref()
            .map_or(0, |event| event.coalesced_events)
    }
}

#[derive(Debug)]
struct InterfaceSnapshot {
    interface_count: usize,
    up_interface_count: usize,
    default_route_interface: String,
    netwatch_regular_addr_count: usize,
    netwatch_loopback_addr_count: usize,
    up_interfaces: String,
    netwatch_regular_addrs: String,
}

fn interface_snapshot(state: &netwatch::interfaces::State) -> InterfaceSnapshot {
    let up_interfaces = state
        .interfaces
        .values()
        .filter(|iface| iface.is_up())
        .map(|iface| iface.name().to_string())
        .collect::<BTreeSet<_>>();
    let netwatch_regular_addrs = state
        .local_addresses
        .regular
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();

    InterfaceSnapshot {
        interface_count: state.interfaces.len(),
        up_interface_count: up_interfaces.len(),
        default_route_interface: state.default_route_interface.clone().unwrap_or_default(),
        netwatch_regular_addr_count: state.local_addresses.regular.len(),
        netwatch_loopback_addr_count: state.local_addresses.loopback.len(),
        up_interfaces: up_interfaces.into_iter().collect::<Vec<_>>().join(","),
        netwatch_regular_addrs: netwatch_regular_addrs
            .into_iter()
            .collect::<Vec<_>>()
            .join(","),
    }
}

impl DaemonState {
    async fn new(
        home: FabricHome,
        cancel: CancellationToken,
        options: DaemonOptions,
    ) -> Result<Arc<Self>> {
        home.prepare()?;
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
        let endpoint = build_daemon_endpoint(&home, allowed.clone(), &exposures).await?;
        let (endpoint_tx, _) = watch::channel(CurrentEndpoint {
            generation: 0,
            endpoint,
        });
        let (tunnel_drop_tx, _) = watch::channel(0);
        let tunnel_sessions =
            tunnel::ServerSessionStore::new(tunnel_session_limits, tunnel_session_detached_ttl);
        tunnel::spawn_server_session_reaper(tunnel_sessions.clone(), cancel.clone());

        Ok(Arc::new(Self {
            home,
            endpoint_tx,
            endpoint_recycle: Mutex::new(()),
            last_endpoint_recycle: Mutex::new(None),
            peer_book: RwLock::new(peer_book),
            allowed,
            exposures: RwLock::new(exposures),
            dial_sockets: Mutex::new(HashMap::new()),
            tcp_dials: Mutex::new(HashMap::new()),
            tunnel_sessions,
            tunnel_drop_tx,
            tunnel_blocked: AtomicBool::new(false),
            network_usable: AtomicBool::new(true),
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

    fn endpoint_handle(&self) -> CurrentEndpoint {
        self.endpoint_tx.borrow().clone()
    }

    fn current_endpoint(&self) -> Endpoint {
        self.endpoint_handle().endpoint
    }

    fn endpoint_rx(&self) -> watch::Receiver<CurrentEndpoint> {
        self.endpoint_tx.subscribe()
    }

    pub fn id(&self) -> EndpointId {
        self.current_endpoint().id()
    }

    pub fn addr(&self) -> EndpointAddr {
        self.current_endpoint().addr()
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
        self.current_endpoint()
            .set_alpns(accepted_alpns(&exposures));
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
        self.current_endpoint()
            .set_alpns(accepted_alpns(&exposures));
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
        self.current_endpoint()
            .set_alpns(accepted_alpns(&exposures));
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
        self.current_endpoint()
            .set_alpns(accepted_alpns(&exposures));
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
        let endpoint = self.current_endpoint();
        self.ping_addr_on_endpoint(endpoint, peer, peer_addr).await
    }

    async fn ping_addr_on_endpoint(
        &self,
        endpoint: Endpoint,
        peer: &str,
        peer_addr: EndpointAddr,
    ) -> Result<PingOutcome> {
        let nonce = rand::random::<[u8; 32]>();
        let started = std::time::Instant::now();
        let connection = endpoint
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
            && let Some(info) = endpoint.remote_info(peer_addr.id).await
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
            self.endpoint_rx(),
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
                self.endpoint_rx(),
                peer_addr,
                alpn,
                self.cancel.clone(),
                self.dial_failures.clone(),
                self.dial_slots.clone(),
            ));
        } else {
            tokio::spawn(run_dial_socket(
                listener,
                self.endpoint_rx(),
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

    async fn rehome_after_network_change(&self, reason: &str, network_usable: bool) {
        let endpoint = self.endpoint_handle();
        info!(
            target: VALIDATION_LOG_TARGET,
            event = "manual_network_change_fire",
            generation = endpoint.generation,
            network_usable,
            reason,
            "notifying iroh endpoint of debounced network change"
        );
        eprintln!(
            "fabric: network change detected ({reason}); notifying iroh endpoint generation {}",
            endpoint.generation
        );
        endpoint.endpoint.network_change().await;
        self.drop_tunnel_connections();

        if !network_usable {
            info!(
                target: VALIDATION_LOG_TARGET,
                event = "network_change_defer_health",
                generation = endpoint.generation,
                reason,
                "network has no usable default route"
            );
            eprintln!(
                "fabric: network has no usable default route yet; deferring endpoint health check"
            );
            return;
        }

        if self
            .endpoint_health_recovered(endpoint.clone(), "network change")
            .await
        {
            return;
        }

        if let Err(error) = self
            .recycle_endpoint_if_generation(endpoint.generation, "network health did not recover")
            .await
        {
            eprintln!("fabric: failed to recycle iroh endpoint after network change: {error:#}");
        }
    }

    async fn endpoint_health_recovered(&self, endpoint: CurrentEndpoint, context: &str) -> bool {
        if tokio::time::timeout(ENDPOINT_HEALTH_TIMEOUT, endpoint.endpoint.online())
            .await
            .is_ok()
        {
            info!(
                target: VALIDATION_LOG_TARGET,
                event = "endpoint_health",
                context,
                generation = endpoint.generation,
                online = true,
                peer_probe_attempted = false,
                peer_reachable = false,
                recovered = true,
                "endpoint online; peer echo probe skipped"
            );
            eprintln!(
                "fabric: iroh endpoint generation {} is online during {context}",
                endpoint.generation,
            );
            return true;
        }

        if self.endpoint_handle().generation != endpoint.generation {
            debug!(
                target: VALIDATION_LOG_TARGET,
                event = "endpoint_health",
                context,
                generation = endpoint.generation,
                stale_generation = true,
                peer_probe_attempted = false,
                "endpoint generation changed while health check was running"
            );
            return true;
        }

        let peer_reachable = tokio::time::timeout(
            ENDPOINT_HEALTH_TIMEOUT,
            self.any_peer_reachable_on_endpoint(endpoint.endpoint, context),
        )
        .await
        .unwrap_or(false);
        info!(
            target: VALIDATION_LOG_TARGET,
            event = "endpoint_health",
            context,
            generation = endpoint.generation,
            online = false,
            peer_probe_attempted = true,
            peer_reachable,
            recovered = peer_reachable,
            "endpoint health checked trusted peer echo"
        );
        peer_reachable
    }

    async fn any_peer_reachable_on_endpoint(&self, endpoint: Endpoint, context: &str) -> bool {
        let peers = self.peer_book.read().await.peers().to_vec();
        for peer in peers {
            let addr = peer
                .addr
                .clone()
                .unwrap_or_else(|| EndpointAddr::new(peer.id));
            let label = peer.name.clone().unwrap_or_else(|| peer.id.to_string());
            let result = tokio::time::timeout(
                REACHABILITY_TIMEOUT,
                self.ping_addr_on_endpoint(endpoint.clone(), &label, addr),
            )
            .await;
            if matches!(result, Ok(Ok(_))) {
                eprintln!("fabric: peer {label:?} reachable during {context}");
                return true;
            }
        }
        false
    }

    async fn log_endpoint_snapshot(&self) {
        let endpoint = self.endpoint_handle();
        let peers = self.peer_book.read().await.peers().to_vec();
        let network_state = netwatch::interfaces::State::new().await;
        let interfaces = interface_snapshot(&network_state);
        let mut relay_watcher = endpoint.endpoint.home_relay_status();
        let relays = relay_watcher.get();
        let home_relays = relays.len();
        let home_relays_connected = relays.iter().filter(|relay| relay.is_connected()).count();
        let home_relays_with_error = relays
            .iter()
            .filter(|relay| !relay.is_connected() && relay.last_error().is_some())
            .count();

        let mut remote_infos = 0usize;
        let mut remote_addrs_total = 0usize;
        let mut remote_addrs_active = 0usize;
        let mut remote_addrs_inactive = 0usize;
        let mut remote_addrs_ip = 0usize;
        let mut remote_addrs_relay = 0usize;

        for peer in &peers {
            let Some(info) = endpoint.endpoint.remote_info(peer.id).await else {
                continue;
            };
            remote_infos += 1;
            for addr in info.addrs() {
                remote_addrs_total += 1;
                match addr.usage() {
                    TransportAddrUsage::Active => remote_addrs_active += 1,
                    _ => remote_addrs_inactive += 1,
                }
                remote_addrs_ip += usize::from(addr.addr().is_ip());
                remote_addrs_relay += usize::from(addr.addr().is_relay());
            }
        }

        let rss_bytes = current_rss_bytes();
        let server_sessions = self.tunnel_sessions.stats().await;
        let active_incoming_handlers =
            MAX_INCOMING_HANDLERS.saturating_sub(self.incoming_slots.available_permits());
        let active_dial_handlers =
            MAX_DIAL_HANDLERS.saturating_sub(self.dial_slots.available_permits());
        info!(
            target: VALIDATION_LOG_TARGET,
            event = "endpoint_snapshot",
            generation = endpoint.generation,
            rss_known = rss_bytes.is_some(),
            rss_bytes = rss_bytes.unwrap_or(0),
            peer_count = peers.len(),
            remote_infos,
            remote_addrs_total,
            remote_addrs_active,
            remote_addrs_inactive,
            remote_addrs_ip,
            remote_addrs_relay,
            home_relays,
            home_relays_connected,
            home_relays_with_error,
            netwatch_interface_count = interfaces.interface_count,
            netwatch_up_interface_count = interfaces.up_interface_count,
            netwatch_default_route_interface = %interfaces.default_route_interface,
            netwatch_regular_addr_count = interfaces.netwatch_regular_addr_count,
            netwatch_loopback_addr_count = interfaces.netwatch_loopback_addr_count,
            netwatch_up_interfaces = %interfaces.up_interfaces,
            netwatch_regular_addrs = %interfaces.netwatch_regular_addrs,
            tunnel_server_sessions_total = server_sessions.total_sessions,
            tunnel_server_sessions_active = server_sessions.active_sessions,
            tunnel_server_sessions_detached = server_sessions.detached_sessions,
            tunnel_server_sessions_complete = server_sessions.complete_sessions,
            tunnel_server_sessions_done = server_sessions.done_sessions,
            tunnel_server_active_attaches = server_sessions.active_attaches,
            tunnel_server_buffered_bytes = server_sessions.buffered_bytes,
            tunnel_server_buffered_chunks = server_sessions.buffered_chunks,
            tunnel_server_sessions_with_buffered_data = server_sessions.sessions_with_buffered_data,
            tunnel_server_sessions_with_cleanup = server_sessions.sessions_with_cleanup,
            tunnel_server_sessions_with_reconnect_error = server_sessions.sessions_with_reconnect_error,
            tunnel_server_sessions_with_pending_remote_close = server_sessions.sessions_with_pending_remote_close,
            tunnel_server_reconnect_attempts_total = server_sessions.reconnect_attempts_total,
            active_incoming_handlers,
            active_dial_handlers,
            "endpoint diagnostic snapshot"
        );
    }

    async fn recycle_endpoint_if_generation(
        &self,
        expected_generation: u64,
        reason: &str,
    ) -> Result<EndpointRecycleOutcome> {
        let _guard = self.endpoint_recycle.lock().await;
        let old = self.endpoint_handle();
        if old.generation != expected_generation {
            debug!(
                target: VALIDATION_LOG_TARGET,
                event = "endpoint_recycle_skip",
                expected_generation,
                actual_generation = old.generation,
                reason,
                "endpoint recycle skipped because generation changed"
            );
            return Ok(EndpointRecycleOutcome::StaleGeneration);
        }

        if let Some(last_recycle) = *self.last_endpoint_recycle.lock().await {
            let since = last_recycle.elapsed();
            if since < ENDPOINT_RECYCLE_MIN_INTERVAL {
                let retry_after = ENDPOINT_RECYCLE_MIN_INTERVAL.saturating_sub(since);
                warn!(
                    target: VALIDATION_LOG_TARGET,
                    event = "endpoint_recycle_rate_limited",
                    generation = old.generation,
                    reason,
                    since_ms = since.as_millis() as u64,
                    min_interval_ms = ENDPOINT_RECYCLE_MIN_INTERVAL.as_millis() as u64,
                    retry_after_ms = retry_after.as_millis() as u64,
                    "endpoint recycle suppressed by rate limit"
                );
                return Ok(EndpointRecycleOutcome::RateLimited { retry_after });
            }
        }

        let started = Instant::now();
        let rss_before_bytes = current_rss_bytes();
        let exposures = self.exposures.read().await;
        let new_endpoint =
            build_daemon_endpoint(&self.home, self.allowed.clone(), &exposures).await?;
        drop(exposures);

        if new_endpoint.id() != old.endpoint.id() {
            bail!(
                "rebuilt endpoint id {} did not match previous id {}",
                new_endpoint.id(),
                old.endpoint.id()
            );
        }

        let new_generation = old.generation.wrapping_add(1);
        self.endpoint_tx.send_replace(CurrentEndpoint {
            generation: new_generation,
            endpoint: new_endpoint,
        });
        *self.last_endpoint_recycle.lock().await = Some(Instant::now());
        self.drop_tunnel_connections();
        old.endpoint.close().await;
        let duration = started.elapsed();
        let rss_after_bytes = current_rss_bytes();
        info!(
            target: VALIDATION_LOG_TARGET,
            event = "endpoint_recycle",
            reason,
            old_generation = old.generation,
            new_generation,
            duration_ms = duration.as_millis() as u64,
            rss_before_known = rss_before_bytes.is_some(),
            rss_before_bytes = rss_before_bytes.unwrap_or(0),
            rss_after_known = rss_after_bytes.is_some(),
            rss_after_bytes = rss_after_bytes.unwrap_or(0),
            "recycled iroh endpoint"
        );
        eprintln!(
            "fabric: recycled iroh endpoint generation {} -> {} ({reason})",
            old.generation, new_generation
        );
        Ok(EndpointRecycleOutcome::Recycled)
    }

    pub(crate) async fn force_endpoint_recycle(&self, reason: &str) -> Result<()> {
        let generation = self.endpoint_handle().generation;
        self.recycle_endpoint_if_generation(generation, reason)
            .await?;
        Ok(())
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
    init_daemon_tracing(&home)?;
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
        result = run_network_rehome_loop(state.clone()) => result?,
        result = run_endpoint_health_poll_loop(state.clone()) => result?,
        result = run_endpoint_rss_recycle_loop(state.clone()) => result?,
        result = run_endpoint_snapshot_loop(state.clone()) => result?,
        _ = state.cancel.cancelled() => {}
    }

    state.cancel.cancel();
    state.current_endpoint().close().await;
    let _ = fs::remove_file(control_path);
    for socket in state.dial_sockets.lock().await.values() {
        let _ = fs::remove_file(&socket.path);
    }
    Ok(())
}

async fn run_network_rehome_loop(state: Arc<DaemonState>) -> Result<()> {
    let monitor = match netwatch::netmon::Monitor::new().await {
        Ok(monitor) => monitor,
        Err(error) => {
            eprintln!("fabric: network monitor unavailable; roaming rehome disabled: {error:#}");
            state.cancel.cancelled().await;
            return Ok(());
        }
    };
    let mut interfaces = monitor.interface_state();
    let mut debouncer = NetworkChangeDebouncer::new(NETWORK_CHANGE_DEBOUNCE);

    loop {
        let due_at = debouncer.due_at();
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            _ = async {
                if let Some(due_at) = due_at {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(due_at)).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                if let Some(event) = debouncer.take_due(Instant::now()) {
                    info!(
                        target: VALIDATION_LOG_TARGET,
                        event = "netmon_debounce_fire",
                        coalesced_events = event.coalesced_events,
                        network_usable = event.network_usable,
                        reason = %event.reason,
                        debounce_ms = NETWORK_CHANGE_DEBOUNCE.as_millis() as u64,
                        "network-change debounce window elapsed"
                    );
                    state
                        .rehome_after_network_change(&event.reason, event.network_usable)
                        .await;
                }
            }
            update = interfaces.updated() => {
                let Ok(network_state) = update else {
                    eprintln!("fabric: network monitor stopped; roaming rehome disabled");
                    break;
                };
                let network_usable = network_state.default_route_interface.is_some()
                    && (network_state.have_v4 || network_state.have_v6);
                state.network_usable.store(network_usable, Ordering::SeqCst);
                let reason = format!(
                    "default_route={:?} have_v4={} have_v6={} unsuspend={}",
                    network_state.default_route_interface,
                    network_state.have_v4,
                    network_state.have_v6,
                    network_state.last_unsuspend.is_some()
                );
                let interfaces = interface_snapshot(&network_state);
                info!(
                    target: VALIDATION_LOG_TARGET,
                    event = "netmon_raw",
                    network_usable,
                    reason = %reason,
                    netwatch_interface_count = interfaces.interface_count,
                    netwatch_up_interface_count = interfaces.up_interface_count,
                    netwatch_default_route_interface = %interfaces.default_route_interface,
                    netwatch_regular_addr_count = interfaces.netwatch_regular_addr_count,
                    netwatch_loopback_addr_count = interfaces.netwatch_loopback_addr_count,
                    netwatch_up_interfaces = %interfaces.up_interfaces,
                    netwatch_regular_addrs = %interfaces.netwatch_regular_addrs,
                    "raw network monitor update"
                );
                debouncer.record(reason.clone(), network_usable, Instant::now());
                info!(
                    target: VALIDATION_LOG_TARGET,
                    event = "netmon_debounce_pending",
                    coalesced_events = debouncer.pending_count(),
                    network_usable,
                    reason = %reason,
                    debounce_ms = NETWORK_CHANGE_DEBOUNCE.as_millis() as u64,
                    "network-change update queued for debounce"
                );
            }
        }
    }

    Ok(())
}

async fn run_endpoint_snapshot_loop(state: Arc<DaemonState>) -> Result<()> {
    if !tracing::dispatcher::has_been_set() {
        state.cancel.cancelled().await;
        return Ok(());
    }

    let mut interval = tokio::time::interval(ENDPOINT_DIAGNOSTIC_SNAPSHOT_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;

    loop {
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            _ = interval.tick() => state.log_endpoint_snapshot().await,
        }
    }

    Ok(())
}

async fn run_endpoint_health_poll_loop(state: Arc<DaemonState>) -> Result<()> {
    let mut interval = tokio::time::interval(ENDPOINT_HEALTH_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    let mut consecutive_failures = 0usize;

    loop {
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            _ = interval.tick() => {
                if !state.network_usable.load(Ordering::SeqCst) {
                    consecutive_failures = 0;
                    continue;
                }

                let endpoint = state.endpoint_handle();
                if state
                    .endpoint_health_recovered(endpoint.clone(), "periodic health poll")
                    .await
                {
                    consecutive_failures = 0;
                    continue;
                }

                if state.endpoint_handle().generation != endpoint.generation {
                    consecutive_failures = 0;
                    continue;
                }

                consecutive_failures = consecutive_failures.saturating_add(1);
                warn!(
                    target: VALIDATION_LOG_TARGET,
                    event = "endpoint_health_poll_failed",
                    generation = endpoint.generation,
                    consecutive_failures,
                    recycle_after_failures = ENDPOINT_HEALTH_POLL_FAILURES_BEFORE_RECYCLE,
                    "endpoint health poll failed"
                );
                eprintln!(
                    "fabric: iroh endpoint generation {} failed health poll ({}/{})",
                    endpoint.generation,
                    consecutive_failures,
                    ENDPOINT_HEALTH_POLL_FAILURES_BEFORE_RECYCLE,
                );

                if consecutive_failures >= ENDPOINT_HEALTH_POLL_FAILURES_BEFORE_RECYCLE {
                    if let Err(error) = state
                        .recycle_endpoint_if_generation(
                            endpoint.generation,
                            "periodic health poll did not recover",
                        )
                        .await
                    {
                        eprintln!("fabric: failed to recycle iroh endpoint after health poll: {error:#}");
                    }
                    consecutive_failures = 0;
                }
            }
        }
    }

    Ok(())
}

async fn run_endpoint_rss_recycle_loop(state: Arc<DaemonState>) -> Result<()> {
    run_endpoint_rss_recycle_loop_with_sampler(
        state,
        ENDPOINT_RSS_RECYCLE_POLL_INTERVAL,
        ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES,
        ENDPOINT_RSS_RECYCLE_SETTLE_INTERVAL,
        ENDPOINT_RSS_INEFFECTIVE_INITIAL_BACKOFF,
        ENDPOINT_RSS_INEFFECTIVE_MAX_BACKOFF,
        Arc::new(current_rss_bytes),
    )
    .await
}

async fn run_endpoint_rss_recycle_loop_with_sampler(
    state: Arc<DaemonState>,
    poll_interval: Duration,
    threshold_bytes: u64,
    settle_interval: Duration,
    ineffective_initial_backoff: Duration,
    ineffective_max_backoff: Duration,
    sample_rss: RssSampler,
) -> Result<()> {
    let mut interval = tokio::time::interval(poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    let mut next_recycle_attempt = Instant::now();
    let mut ineffective_recycles = 0usize;

    loop {
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            _ = interval.tick() => {
                let Some(rss_bytes) = sample_rss() else {
                    debug!(
                        target: VALIDATION_LOG_TARGET,
                        event = "endpoint_rss_monitor",
                        rss_known = false,
                        threshold_bytes,
                        poll_interval_ms = poll_interval.as_millis() as u64,
                        "endpoint RSS monitor could not read current RSS"
                    );
                    continue;
                };

                if !rss_exceeds_recycle_threshold(Some(rss_bytes), threshold_bytes) {
                    next_recycle_attempt = Instant::now();
                    ineffective_recycles = 0;
                    continue;
                }

                let now = Instant::now();
                if now < next_recycle_attempt {
                    debug!(
                        target: VALIDATION_LOG_TARGET,
                        event = "endpoint_rss_recycle_deferred",
                        rss_bytes,
                        threshold_bytes,
                        ineffective_recycles,
                        retry_after_ms = next_recycle_attempt.saturating_duration_since(now).as_millis() as u64,
                        "endpoint RSS remains over threshold; waiting before next recycle attempt"
                    );
                    continue;
                }

                let endpoint = state.endpoint_handle();
                warn!(
                    target: VALIDATION_LOG_TARGET,
                    event = "endpoint_rss_recycle_trigger",
                    generation = endpoint.generation,
                    rss_bytes,
                    threshold_bytes,
                    poll_interval_ms = poll_interval.as_millis() as u64,
                    "endpoint RSS threshold exceeded; recycling endpoint"
                );
                eprintln!(
                    "fabric: iroh endpoint generation {} RSS {} MiB exceeded {} MiB; recycling",
                    endpoint.generation,
                    bytes_to_mib(rss_bytes),
                    bytes_to_mib(threshold_bytes),
                );

                let server_sessions_before = state.tunnel_sessions.stats().await;
                match state
                    .recycle_endpoint_if_generation(endpoint.generation, "rss threshold exceeded")
                    .await
                {
                    Ok(EndpointRecycleOutcome::Recycled) => {
                        let settled_rss = tokio::select! {
                            _ = state.cancel.cancelled() => break,
                            _ = tokio::time::sleep(settle_interval) => sample_rss(),
                        };

                        if rss_exceeds_recycle_threshold(settled_rss, threshold_bytes) {
                            let server_sessions_after = state.tunnel_sessions.stats().await;
                            ineffective_recycles = ineffective_recycles.saturating_add(1);
                            let backoff = endpoint_rss_ineffective_backoff(
                                ineffective_recycles,
                                ineffective_initial_backoff,
                                ineffective_max_backoff,
                            );
                            warn!(
                                target: VALIDATION_LOG_TARGET,
                                event = "rss_recycle_ineffective",
                                old_generation = endpoint.generation,
                                current_generation = state.endpoint_handle().generation,
                                rss_before_bytes = rss_bytes,
                                rss_after_settle_known = settled_rss.is_some(),
                                rss_after_settle_bytes = settled_rss.unwrap_or(0),
                                threshold_bytes,
                                settle_ms = settle_interval.as_millis() as u64,
                                ineffective_recycles,
                                backoff_ms = backoff.as_millis() as u64,
                                tunnel_server_sessions_total_before = server_sessions_before.total_sessions,
                                tunnel_server_sessions_total_after = server_sessions_after.total_sessions,
                                tunnel_server_sessions_active_before = server_sessions_before.active_sessions,
                                tunnel_server_sessions_active_after = server_sessions_after.active_sessions,
                                tunnel_server_sessions_detached_before = server_sessions_before.detached_sessions,
                                tunnel_server_sessions_detached_after = server_sessions_after.detached_sessions,
                                tunnel_server_sessions_complete_before = server_sessions_before.complete_sessions,
                                tunnel_server_sessions_complete_after = server_sessions_after.complete_sessions,
                                tunnel_server_sessions_done_before = server_sessions_before.done_sessions,
                                tunnel_server_sessions_done_after = server_sessions_after.done_sessions,
                                tunnel_server_active_attaches_before = server_sessions_before.active_attaches,
                                tunnel_server_active_attaches_after = server_sessions_after.active_attaches,
                                tunnel_server_buffered_bytes_before = server_sessions_before.buffered_bytes,
                                tunnel_server_buffered_bytes_after = server_sessions_after.buffered_bytes,
                                tunnel_server_buffered_chunks_before = server_sessions_before.buffered_chunks,
                                tunnel_server_buffered_chunks_after = server_sessions_after.buffered_chunks,
                                tunnel_server_sessions_with_buffered_data_before = server_sessions_before.sessions_with_buffered_data,
                                tunnel_server_sessions_with_buffered_data_after = server_sessions_after.sessions_with_buffered_data,
                                tunnel_server_sessions_with_cleanup_before = server_sessions_before.sessions_with_cleanup,
                                tunnel_server_sessions_with_cleanup_after = server_sessions_after.sessions_with_cleanup,
                                tunnel_server_sessions_with_reconnect_error_before = server_sessions_before.sessions_with_reconnect_error,
                                tunnel_server_sessions_with_reconnect_error_after = server_sessions_after.sessions_with_reconnect_error,
                                tunnel_server_sessions_with_pending_remote_close_before = server_sessions_before.sessions_with_pending_remote_close,
                                tunnel_server_sessions_with_pending_remote_close_after = server_sessions_after.sessions_with_pending_remote_close,
                                tunnel_server_reconnect_attempts_total_before = server_sessions_before.reconnect_attempts_total,
                                tunnel_server_reconnect_attempts_total_after = server_sessions_after.reconnect_attempts_total,
                                "RSS-triggered endpoint recycle did not reduce RSS below threshold; backing off"
                            );
                            eprintln!(
                                "fabric: RSS stayed over {} MiB after endpoint recycle ({} MiB); backing off RSS recycles for {}s",
                                bytes_to_mib(threshold_bytes),
                                bytes_to_mib(settled_rss.unwrap_or(0)),
                                backoff.as_secs(),
                            );
                            next_recycle_attempt = Instant::now() + backoff;
                        } else {
                            ineffective_recycles = 0;
                            next_recycle_attempt = Instant::now() + ENDPOINT_RECYCLE_MIN_INTERVAL;
                        }
                    }
                    Ok(EndpointRecycleOutcome::RateLimited { retry_after }) => {
                        next_recycle_attempt = Instant::now() + retry_after;
                    }
                    Ok(EndpointRecycleOutcome::StaleGeneration) => {
                        next_recycle_attempt = Instant::now();
                    }
                    Err(error) => {
                        eprintln!("fabric: failed to recycle iroh endpoint after RSS threshold: {error:#}");
                        next_recycle_attempt = Instant::now() + ENDPOINT_RSS_RECYCLE_POLL_INTERVAL;
                    }
                }
            }
        }
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
        ControlRequest::RecycleEndpoint => {
            state.force_endpoint_recycle("debug request").await?;
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
    let mut endpoint_rx = state.endpoint_rx();
    loop {
        if !state.incoming_failures.wait(&state.cancel).await {
            break;
        }
        let endpoint = endpoint_rx.borrow().clone();
        let permit = tokio::select! {
            _ = state.cancel.cancelled() => break,
            permit = state.incoming_slots.clone().acquire_owned() => {
                permit.context("incoming handler semaphore closed")?
            }
        };
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            changed = endpoint_rx.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            incoming = endpoint.endpoint.accept() => {
                let Some(incoming) = incoming else {
                    if endpoint_rx.has_changed().unwrap_or(false) {
                        let _ = endpoint_rx.changed().await;
                        continue;
                    }
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
        log_connection_paths("builtin_echo_accept", &connection);
        handle_builtin_echo(connection, state).await?;
        return Ok(());
    }
    if alpn == shell::SHELL_ALPN {
        let connection = accepting.await?;
        log_connection_paths("builtin_shell_accept", &connection);
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
    log_connection_paths("tunnel_accept", &connection);
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

fn current_rss_bytes() -> Option<u64> {
    current_rss_bytes_impl()
}

fn rss_exceeds_recycle_threshold(rss_bytes: Option<u64>, threshold_bytes: u64) -> bool {
    matches!(rss_bytes, Some(rss_bytes) if rss_bytes >= threshold_bytes)
}

fn bytes_to_mib(bytes: u64) -> u64 {
    bytes / (1024 * 1024)
}

fn endpoint_rss_ineffective_backoff(
    ineffective_recycles: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Duration {
    if ineffective_recycles == 0 {
        return Duration::ZERO;
    }
    let exponent = ineffective_recycles.saturating_sub(1).min(8) as u32;
    initial_backoff
        .saturating_mul(1u32 << exponent)
        .min(max_backoff)
}

#[cfg(target_os = "linux")]
fn current_rss_bytes_impl() -> Option<u64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    resident_pages.checked_mul(page_size as u64)
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn current_rss_bytes_impl() -> Option<u64> {
    use std::mem::{MaybeUninit, size_of};

    let mut info = MaybeUninit::<libc::mach_task_basic_info_data_t>::uninit();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    let result = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS {
        return None;
    }
    if count < (size_of::<libc::mach_task_basic_info_data_t>() / size_of::<libc::natural_t>()) as _
    {
        return None;
    }
    Some(unsafe { info.assume_init().resident_size as u64 })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn current_rss_bytes_impl() -> Option<u64> {
    None
}

fn connection_path_summary(
    connection: &Connection,
) -> (usize, usize, usize, usize, String, String) {
    let paths = connection.paths();
    let mut total = 0usize;
    let mut selected = 0usize;
    let mut ip = 0usize;
    let mut relay = 0usize;
    let mut local_addrs = BTreeSet::new();
    let mut remote_addrs = BTreeSet::new();

    for path in paths.iter() {
        total += 1;
        selected += usize::from(path.is_selected());
        ip += usize::from(path.is_ip());
        relay += usize::from(path.is_relay());
        local_addrs.insert(format!("{:?}", path.local_addr()));
        remote_addrs.insert(path.remote_addr().to_string());
    }

    (
        total,
        selected,
        ip,
        relay,
        local_addrs.into_iter().collect::<Vec<_>>().join(","),
        remote_addrs.into_iter().collect::<Vec<_>>().join(","),
    )
}

fn log_connection_paths(event: &'static str, connection: &Connection) {
    let (paths_total, paths_selected, paths_ip, paths_relay, path_local_addrs, path_remote_addrs) =
        connection_path_summary(connection);
    info!(
        target: VALIDATION_LOG_TARGET,
        event,
        remote = %connection.remote_id(),
        paths_total,
        paths_selected,
        paths_ip,
        paths_relay,
        path_local_addrs = %path_local_addrs,
        path_remote_addrs = %path_remote_addrs,
        "connection path snapshot"
    );
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
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
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
                let endpoint_rx = endpoint_rx.clone();
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
                        tunnel::run_client_connection(local, endpoint_rx, home, peer, alpn, cancel, drop_rx)
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
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
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
                let endpoint_rx = endpoint_rx.clone();
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
                        tunnel::run_client_tcp_connection(local, endpoint_rx, home, peer, alpn, cancel, drop_rx)
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
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
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
                let endpoint = endpoint_rx.borrow().endpoint.clone();
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

    #[test]
    fn network_change_debouncer_coalesces_burst_into_one_leading_edge_event() {
        let mut debouncer = NetworkChangeDebouncer::new(Duration::from_millis(140));
        let now = Instant::now();

        debouncer.record(
            "default_route=Some(en0) have_v4=true".to_string(),
            true,
            now,
        );
        debouncer.record(
            "default_route=Some(en0) have_v4=false".to_string(),
            false,
            now + Duration::from_millis(40),
        );
        debouncer.record(
            "default_route=Some(en0) have_v4=true have_v6=true".to_string(),
            true,
            now + Duration::from_millis(120),
        );

        assert_eq!(debouncer.pending_count(), 3);
        assert!(
            debouncer
                .take_due(now + Duration::from_millis(139))
                .is_none(),
            "leading-edge debounce should wait for the initial window"
        );

        let event = debouncer
            .take_due(now + Duration::from_millis(140))
            .expect("debounced event should fire once after the initial window");
        assert_eq!(event.coalesced_events, 3);
        assert!(event.network_usable);
        assert_eq!(
            event.reason,
            "default_route=Some(en0) have_v4=true have_v6=true"
        );
        assert!(debouncer.take_due(now + Duration::from_secs(3)).is_none());
    }

    #[test]
    fn rss_recycle_threshold_triggers_at_300_mib() {
        assert_eq!(ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES, 300 * 1024 * 1024);
        assert!(!rss_exceeds_recycle_threshold(
            None,
            ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES
        ));
        assert!(!rss_exceeds_recycle_threshold(
            Some(ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES - 1),
            ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES
        ));
        assert!(rss_exceeds_recycle_threshold(
            Some(ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES),
            ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES
        ));
        assert!(rss_exceeds_recycle_threshold(
            Some(550 * 1024 * 1024),
            ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES
        ));
    }

    #[test]
    fn ineffective_rss_recycle_backoff_scales_and_caps() {
        let initial = Duration::from_secs(5 * 60);
        let max = Duration::from_secs(15 * 60);

        assert_eq!(
            endpoint_rss_ineffective_backoff(0, initial, max),
            Duration::ZERO
        );
        assert_eq!(
            endpoint_rss_ineffective_backoff(1, initial, max),
            Duration::from_secs(5 * 60)
        );
        assert_eq!(
            endpoint_rss_ineffective_backoff(2, initial, max),
            Duration::from_secs(10 * 60)
        );
        assert_eq!(
            endpoint_rss_ineffective_backoff(3, initial, max),
            Duration::from_secs(15 * 60)
        );
        assert_eq!(
            endpoint_rss_ineffective_backoff(12, initial, max),
            Duration::from_secs(15 * 60)
        );
    }

    #[tokio::test]
    async fn rss_recycle_threshold_recycles_even_when_endpoint_online() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let node = FabricNode::start(FabricHome::new(temp.path())).await?;
        let state = node.state();
        let initial = state.endpoint_handle();
        tokio::time::timeout(ENDPOINT_HEALTH_TIMEOUT, initial.endpoint.online()).await?;

        let rss_monitor = tokio::spawn(run_endpoint_rss_recycle_loop_with_sampler(
            state.clone(),
            Duration::from_millis(10),
            ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES,
            Duration::ZERO,
            Duration::from_secs(60),
            Duration::from_secs(60),
            Arc::new(|| Some(ENDPOINT_RSS_RECYCLE_THRESHOLD_BYTES)),
        ));

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if state.endpoint_handle().generation > initial.generation {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;

        state.cancel.cancel();
        node.shutdown().await?;
        rss_monitor.await??;
        Ok(())
    }

    #[test]
    fn validation_logging_init_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let home = FabricHome::new(temp.path());

        init_daemon_tracing(&home).unwrap();
        init_daemon_tracing(&home).unwrap();
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
