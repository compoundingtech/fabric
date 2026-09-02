use std::{
    collections::{BTreeSet, HashMap, HashSet},
    env, fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    sync::{
        Arc, OnceLock, RwLock as StdRwLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::fd::AsRawFd;
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
    sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc, watch},
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
    control::{ControlRequest, ControlResponse, PeerReachability, SyncEntryStatus},
    exec, gitremote, mux, pathwatch, shell,
    sync::{
        self,
        config::SyncPeers,
        engine::{PeerRef, ResolvedPeers, SyncEngine, SyncTransport},
        manifest::Author as SyncAuthor,
        node::SyncNode,
    },
    telemetry::TelemetryStore,
    tunnel,
};

const BUILTIN_ECHO_ALPN: &[u8] = b"fabric/echo/0";
const SYNC_ALPN: &[u8] = b"fabric/sync/1";
const ECHO_SERVICE: &str = "echo";
const SHELL_SERVICE: &str = "shell";
const EXEC_SERVICE: &str = "exec";
const SYNC_SERVICE: &str = "sync";

/// Every built-in name accepted by a peer's explicit `allow` list.
///
/// A permission transcription uses this list plus the daemon's live exposure
/// names. Keep it tied to `service_name_for_alpn`, which enforces the gate.
pub const BUILTIN_SERVICE_NAMES: [&str; 5] = [
    SHELL_SERVICE,
    EXEC_SERVICE,
    SYNC_SERVICE,
    ECHO_SERVICE,
    crate::sendfile::SERVICE,
];
const REACHABILITY_TIMEOUT: Duration = Duration::from_secs(3);
const INCOMING_FAILURE_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const INCOMING_FAILURE_MAX_BACKOFF: Duration = Duration::from_secs(5);
const DIAL_FAILURE_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const DIAL_FAILURE_MAX_BACKOFF: Duration = Duration::from_secs(15);
const DIAL_LISTENER_STOP_TIMEOUT: Duration = Duration::from_secs(1);
const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(5);
const MAX_INCOMING_HANDLERS: usize = 32;
const MAX_DIAL_HANDLERS: usize = 32;
/// Sync connections can arrive in large coalesced bursts. Keep the default
/// validation log useful without making one path snapshot per connection a
/// second source of disk pressure. Full detail remains available at debug.
const SYNC_ACCEPT_INFO_SAMPLE_EVERY: usize = 128;
static SYNC_ACCEPT_LOG_SEQUENCE: AtomicUsize = AtomicUsize::new(0);
const ENDPOINT_ONLINE_TIMEOUT: Duration = Duration::from_secs(5);
const ENDPOINT_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);
/// iroh normally drains an endpoint within three seconds. A transport defect
/// must not make a recycle or daemon shutdown wait forever.
const ENDPOINT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const ENDPOINT_HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(30);
const ENDPOINT_HEALTH_POLL_FAILURES_BEFORE_RECYCLE: usize = 2;
const ENDPOINT_DIAGNOSTIC_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
const ENDPOINT_RECYCLE_MIN_INTERVAL: Duration = Duration::from_secs(60);
const ENDPOINT_RSS_OBSERVE_POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Report a new RSS peak only after it grows by a whole step, so the operator sees
/// growth without a per-sample stream. Reporting never interrupts the daemon.
const ENDPOINT_RSS_REPORT_STEP_BYTES: u64 = 128 * 1024 * 1024;
const NETWORK_CHANGE_DEBOUNCE: Duration = Duration::from_millis(140);
/// How often the daemon actively echo-probes each trusted peer, so a peer that has
/// roamed (changed network / public IP) is detected even when THIS machine saw no
/// local network change. `FABRIC_PEER_HEALTH_SECS` overrides; `0` disables.
const PEER_HEALTH_PROBE_INTERVAL: Duration = Duration::from_secs(20);
/// Consecutive failed peer probes before the daemon drives recovery for that peer.
const PEER_HEALTH_FAILURES_BEFORE_RECOVER: usize = 3;
/// Recovery attempts (cheap re-probe nudges) for a still-unreachable peer before
/// escalating to a full endpoint recycle — the heavy hammer a manual restart used
/// to require.
const PEER_HEALTH_ATTEMPTS_BEFORE_RECYCLE: usize = 3;
/// Escalating backoff between repeated recovery attempts for a still-unreachable
/// peer, so a genuinely-down peer does not cause recovery thrash.
const PEER_HEALTH_RECOVER_INITIAL_BACKOFF: Duration = Duration::from_secs(30);
const PEER_HEALTH_RECOVER_MAX_BACKOFF: Duration = Duration::from_secs(10 * 60);
const PATH_QUALITY_ABSOLUTE_FLOOR: Duration = Duration::from_secs(1);
const PATH_QUALITY_BASELINE_MULTIPLIER: u32 = 8;
const PATH_QUALITY_CONSECUTIVE_SAMPLES: usize = 3;
const PATH_QUALITY_WARMUP_SAMPLES: usize = 2;
const PATH_QUALITY_REDIAL_COOLDOWN: Duration = Duration::from_secs(60);
/// The validation log target. `pub(crate)` because `sync::engine` writes to the
/// same log and a second copy of the literal is a string that drifts.
pub(crate) const VALIDATION_LOG_TARGET: &str = "fabric::validation";

/// What a backoff record is about.
///
/// Failures have to be attributed, or they get charged to whoever dials next. A
/// dial keys on the peer and the ALPN, because "hetz is unreachable" says nothing
/// about droppy, and "droppy does not speak fabric/exec/0" says nothing about its
/// shell. The accept loop keys on itself: it throttles before any connection
/// exists, so there is no peer to attribute an accept failure to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BackoffKey {
    peer: String,
    alpn: String,
}

impl BackoffKey {
    fn dial(peer: &str, alpn: &[u8]) -> Self {
        Self {
            peer: peer.to_string(),
            alpn: String::from_utf8_lossy(alpn).to_string(),
        }
    }

    /// The single record for the inbound accept loop.
    fn accept_loop() -> Self {
        Self {
            peer: String::new(),
            alpn: String::new(),
        }
    }
}

#[derive(Debug)]
struct FailureBackoff {
    states: Mutex<HashMap<BackoffKey, FailureBackoffState>>,
    initial_delay: Duration,
    max_delay: Duration,
    log_interval: Duration,
}

#[derive(Debug)]
struct FailureBackoffState {
    consecutive_failures: usize,
    not_before: Instant,
    last_delay: Duration,
    last_log: Option<Instant>,
    suppressed: usize,
}

impl FailureBackoffState {
    fn new(now: Instant) -> Self {
        Self {
            consecutive_failures: 0,
            not_before: now,
            last_delay: Duration::ZERO,
            last_log: None,
            suppressed: 0,
        }
    }

    /// Nothing worth remembering.
    ///
    /// A streak is only meaningful while somebody is still retrying it. Once the
    /// backoff window has been served and a further window of the same length has
    /// passed with no new failure, whoever owned this key stopped dialling, and
    /// keeping the streak would charge a stale escalation to their next attempt.
    /// The grace period is derived from the delay this record itself produced, so
    /// there is no separate number to tune.
    fn is_idle(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.not_before) >= self.last_delay
    }
}

impl FailureBackoff {
    fn new(initial_delay: Duration, max_delay: Duration, log_interval: Duration) -> Arc<Self> {
        Arc::new(Self {
            states: Mutex::new(HashMap::new()),
            initial_delay,
            max_delay,
            log_interval,
        })
    }

    /// Drop records that carry no streak and no remaining delay. The live key
    /// space is bounded by trusted peers times ALPNs, both of which come from
    /// config, so this needs no cap of its own.
    fn prune(states: &mut HashMap<BackoffKey, FailureBackoffState>, now: Instant) {
        states.retain(|_, state| !state.is_idle(now));
    }

    async fn wait(&self, key: &BackoffKey, cancel: &CancellationToken) -> bool {
        loop {
            let delay = {
                let now = Instant::now();
                let mut states = self.states.lock().await;
                Self::prune(&mut states, now);
                states
                    .get(key)
                    .map(|state| state.not_before.saturating_duration_since(now))
                    .unwrap_or_default()
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

    async fn record_success(&self, key: &BackoffKey) {
        let now = Instant::now();
        let mut states = self.states.lock().await;
        states.remove(key);
        Self::prune(&mut states, now);
    }

    /// Record a failure that also delays this key's next attempt.
    async fn record_failure(
        &self,
        key: &BackoffKey,
        label: &str,
        error: &(dyn fmt::Display + Sync),
    ) {
        self.record_failure_inner(key, label, error, true).await
    }

    /// Record a failure for the rate-limited log only.
    ///
    /// Inbound connections use this: their outcome must not gate the next accept,
    /// so saying "backing off" would describe a delay that does not happen.
    async fn record_failure_for_diagnostics(
        &self,
        key: &BackoffKey,
        label: &str,
        error: &(dyn fmt::Display + Sync),
    ) {
        self.record_failure_inner(key, label, error, false).await
    }

    async fn record_failure_inner(
        &self,
        key: &BackoffKey,
        label: &str,
        error: &(dyn fmt::Display + Sync),
        delays_next_attempt: bool,
    ) {
        let (delay, consecutive_failures, suppressed, should_log) = {
            let now = Instant::now();
            let mut states = self.states.lock().await;
            let state = states
                .entry(key.clone())
                .or_insert_with(|| FailureBackoffState::new(now));
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            let delay = self.delay_for_step(state.consecutive_failures);
            state.not_before = now + delay;
            state.last_delay = delay;

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
            let consequence = if delays_next_attempt {
                format!("backing off for {delay:?}")
            } else {
                "not delaying anything else".to_string()
            };
            if suppressed > 0 {
                eprintln!(
                    "fabric: {label}: {error}; {consequence} after {consecutive_failures} consecutive failures ({suppressed} similar failures suppressed)"
                );
            } else {
                eprintln!(
                    "fabric: {label}: {error}; {consequence} after {consecutive_failures} consecutive failures"
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeOutcome {
    Supported,
    Unsupported,
    Unreachable,
    Timeout,
}

impl ProbeOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Unsupported => "unsupported",
            Self::Unreachable => "unreachable",
            Self::Timeout => "timeout",
        }
    }
}

impl fmt::Display for ProbeOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
struct ServiceProbe {
    peer: String,
    peer_id: String,
    outcome: ProbeOutcome,
    round_trip: Option<Duration>,
    transport: Option<String>,
    error: Option<String>,
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
    active_dial_listeners: Arc<AtomicUsize>,
    tcp_dials: Mutex<HashMap<(String, String, String), TcpDial>>,
    tunnel_sessions: tunnel::ServerSessionStore,
    /// Attached OUTBOUND sessions. `tunnel_sessions` only knows what we serve.
    client_attaches: Arc<tunnel::ClientAttachGauge>,
    tunnel_drop_tx: watch::Sender<u64>,
    tunnel_blocked: AtomicBool,
    network_usable: AtomicBool,
    builtin_echo_hits: AtomicUsize,
    allow_shell: bool,
    allow_exec: bool,
    incoming_failures: Arc<FailureBackoff>,
    dial_failures: Arc<FailureBackoff>,
    incoming_slots: Arc<Semaphore>,
    dial_slots: Arc<Semaphore>,
    mux_stream_slots: Arc<Semaphore>,
    peer_connections: Arc<mux::PeerConnections>,
    opened_mux_connections: Mutex<Option<mpsc::UnboundedReceiver<Connection>>>,
    git_sessions: gitremote::GitSessionLimits,
    cancel: CancellationToken,
    /// Durable loss/resume counters. Survives a restart, unlike the log lines
    /// that were previously the only record.
    telemetry: Arc<TelemetryStore>,
    /// The last path the liveness probe saw for each peer, keyed by peer label.
    ///
    /// A session-loss notice fires when the transport is already gone, so asking
    /// the endpoint for the path at that moment answers "none" and tells nobody
    /// which path just failed. The probe samples every peer on an interval, so
    /// its most recent answer is the honest one for "what were we on".
    ///
    /// A std lock, not the tokio one used elsewhere here, because the connection
    /// notices that read it are synchronous callbacks and cannot await. The
    /// critical section is one map lookup.
    last_probe_transport: Arc<StdRwLock<HashMap<String, String>>>,
    /// The file-sync engine, set once just after this state is constructed (it
    /// needs a handle back to the state to dial peers).
    sync_engine: OnceLock<Arc<SyncEngine<IrohSyncTransport>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct CurrentEndpoint {
    pub(crate) generation: u64,
    pub(crate) endpoint: Endpoint,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DaemonOptions {
    pub allow_shell: bool,
    pub allow_exec: bool,
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

#[derive(Debug)]
struct DialSocket {
    path: PathBuf,
    peer_addr: EndpointAddr,
    listener_cancel: CancellationToken,
    listener_task: Option<JoinHandle<()>>,
}

impl DialSocket {
    async fn stop(self) {
        self.stop_with_timeout(DIAL_LISTENER_STOP_TIMEOUT).await;
    }

    async fn stop_with_timeout(mut self, timeout: Duration) -> bool {
        self.listener_cancel.cancel();
        let stopped_gracefully = if let Some(mut task) = self.listener_task.take() {
            match tokio::time::timeout(timeout, &mut task).await {
                Ok(_) => true,
                Err(_) => {
                    warn!(
                        path = %self.path.display(),
                        ?timeout,
                        "dial listener did not stop after cancellation; aborting"
                    );
                    task.abort();
                    let _ = task.await;
                    false
                }
            }
        } else {
            true
        };
        let _ = fs::remove_file(&self.path);
        stopped_gracefully
    }
}

impl Drop for DialSocket {
    fn drop(&mut self) {
        self.listener_cancel.cancel();
        if let Some(task) = self.listener_task.take() {
            task.abort();
        }
        let _ = fs::remove_file(&self.path);
    }
}

struct DialListenerLease {
    active: Arc<AtomicUsize>,
}

impl DialListenerLease {
    fn new(active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::SeqCst);
        Self { active }
    }
}

impl Drop for DialListenerLease {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
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

fn set_config_allow_exec(home: &FabricHome, allow_exec: bool) -> Result<()> {
    let mut config = FabricConfig::load(home)?;
    config.set_allow_exec(allow_exec);
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
    RateLimited {
        retry_after: Duration,
    },
    /// Live shell or tunnel sessions are attached and this caller did not promise
    /// to preserve them, so the endpoint was left alone.
    SessionsAttached {
        active_sessions: usize,
    },
}

type RssSampler = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AllocatorTrimResult {
    attempted: bool,
    succeeded: bool,
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

/// How many daily validation logs to keep before the oldest is deleted.
///
/// Derived from the job the logs have to do, not rounded to look tidy. An
/// operator may be away from a machine for a month, so retention has to outlast
/// a trip: a fault in week one must still be readable on return. That sets a
/// floor of about 31 days. Two more weeks of margin covers the gap between
/// getting back and actually looking.
///
/// Measured cost at the observed rate of 8.8 to 10.3 MB per day: roughly 420 MB.
/// Before this bound existed nothing was ever deleted, and one machine had
/// accumulated 2.4 GB across 20 days.
///
/// This bounds the FILE COUNT, which is what stops indefinite growth. It does
/// not bound bytes: a single noisy day has reached 587 MB, so a bad run can
/// still cost far more than the daily average suggests. Capping that is a
/// question about log volume, not retention, and it is not what this solves.
pub const DEFAULT_LOG_RETENTION_DAYS: usize = 45;

/// Resolve the retention window, honouring `FABRIC_LOG_RETENTION_DAYS`.
///
/// `0` disables deletion and restores the old unbounded behaviour, for an
/// operator who would rather spend disk than lose history.
fn resolve_log_retention_days(raw: Option<&str>) -> Option<usize> {
    match raw.map(str::trim) {
        None | Some("") => Some(DEFAULT_LOG_RETENTION_DAYS),
        Some(value) => match value.parse::<usize>() {
            Ok(0) => None,
            Ok(days) => Some(days),
            // An unparseable override must not silently disable the bound; the
            // whole point is that nobody is watching this machine.
            Err(_) => Some(DEFAULT_LOG_RETENTION_DAYS),
        },
    }
}

pub fn init_daemon_tracing(home: &FabricHome) -> Result<()> {
    home.prepare()?;
    let retention =
        resolve_log_retention_days(env::var("FABRIC_LOG_RETENTION_DAYS").ok().as_deref());
    let mut builder = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix(home.validation_log_prefix());
    if let Some(days) = retention {
        builder = builder.max_log_files(days);
    }
    let appender = builder
        .build(home.validation_log_dir())
        .context("failed to build the validation log appender")?;
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
            log_retention_days = retention.unwrap_or(0),
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
        if options.allow_exec {
            set_config_allow_exec(&home, true)?;
        }
        let config = FabricConfig::load(&home)?;
        let allow_shell = options.allow_shell || config.allow_shell().unwrap_or(false);
        let allow_exec = options.allow_exec || config.allow_exec().unwrap_or(false);
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
        let telemetry = Arc::new(TelemetryStore::load(home.telemetry_path()));
        let local_id = endpoint_tx.borrow().endpoint.id();
        let (opened_mux_tx, opened_mux_rx) = mpsc::unbounded_channel();

        Ok(Arc::new(Self {
            home,
            endpoint_tx,
            endpoint_recycle: Mutex::new(()),
            last_endpoint_recycle: Mutex::new(None),
            peer_book: RwLock::new(peer_book),
            allowed,
            exposures: RwLock::new(exposures),
            dial_sockets: Mutex::new(HashMap::new()),
            active_dial_listeners: Arc::new(AtomicUsize::new(0)),
            tcp_dials: Mutex::new(HashMap::new()),
            tunnel_sessions,
            client_attaches: tunnel::ClientAttachGauge::new(),
            tunnel_drop_tx,
            tunnel_blocked: AtomicBool::new(false),
            network_usable: AtomicBool::new(true),
            builtin_echo_hits: AtomicUsize::new(0),
            allow_shell,
            allow_exec,
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
            mux_stream_slots: Arc::new(Semaphore::new(MAX_INCOMING_HANDLERS)),
            peer_connections: Arc::new(mux::PeerConnections::new(local_id, opened_mux_tx)),
            opened_mux_connections: Mutex::new(Some(opened_mux_rx)),
            git_sessions: gitremote::GitSessionLimits::default(),
            cancel,
            telemetry,
            last_probe_transport: Arc::new(StdRwLock::new(HashMap::new())),
            sync_engine: OnceLock::new(),
        }))
    }

    /// Dial permits in use right now, out of `MAX_DIAL_HANDLERS`.
    ///
    /// Every local connection to a dial socket, and every `shell` and `exec`,
    /// holds one for the life of its session. When all are held, every new one
    /// waits with no error and no log line, so this number is the instrument
    /// for "shell hangs while ping answers".
    pub fn active_dial_handlers(&self) -> usize {
        MAX_DIAL_HANDLERS.saturating_sub(self.dial_slots.available_permits())
    }

    pub fn max_dial_handlers(&self) -> usize {
        MAX_DIAL_HANDLERS
    }

    pub fn active_incoming_handlers(&self) -> usize {
        MAX_INCOMING_HANDLERS.saturating_sub(self.incoming_slots.available_permits())
    }

    pub async fn peer_connection_count(&self) -> usize {
        self.peer_connections.peer_count().await
    }

    pub(crate) fn telemetry(&self) -> Arc<TelemetryStore> {
        self.telemetry.clone()
    }

    fn connection_recorder(&self) -> ConnectionRecorder {
        ConnectionRecorder::new(self.telemetry.clone(), self.last_probe_transport.clone())
    }

    fn sync_engine(&self) -> Option<Arc<SyncEngine<IrohSyncTransport>>> {
        self.sync_engine.get().cloned()
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

    async fn open_peer_stream(
        &self,
        peer_addr: &EndpointAddr,
        protocol: &str,
        activity: mux::StreamActivity,
    ) -> Result<mux::MuxStream> {
        let endpoint = self.endpoint_handle();
        self.peer_connections
            .open_stream(
                &endpoint.endpoint,
                endpoint.generation,
                peer_addr,
                protocol,
                activity,
            )
            .await
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
        let endpoint = self.endpoint_handle();
        self.ping_addr_on_endpoint(endpoint, peer, peer_addr).await
    }

    async fn ping_addr_on_endpoint(
        &self,
        endpoint: CurrentEndpoint,
        peer: &str,
        peer_addr: EndpointAddr,
    ) -> Result<PingOutcome> {
        let nonce = rand::random::<[u8; 32]>();
        let started = std::time::Instant::now();
        let stream = self
            .peer_connections
            .open_stream(
                &endpoint.endpoint,
                endpoint.generation,
                &peer_addr,
                std::str::from_utf8(BUILTIN_ECHO_ALPN).expect("built-in ALPN is UTF-8"),
                mux::StreamActivity::Probe,
            )
            .await
            .with_context(|| format!("failed to connect to {peer:?} built-in echo"))?;
        let connection = stream.connection;
        let mut send = stream.send;
        let mut recv = stream.recv;

        send.write_all(&nonce).await?;
        send.finish()?;

        let response = recv.read_to_end(nonce.len() + 1).await?;
        let round_trip = started.elapsed();
        let mut transport = classify_connection_transport(&connection);
        if transport.is_none()
            && let Some(info) = endpoint.endpoint.remote_info(peer_addr.id).await
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
            self.peer_connections.clone(),
            self.client_attaches.clone(),
            self.connection_recorder(),
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
            && existing
                .listener_task
                .as_ref()
                .is_some_and(|task| !task.is_finished())
        {
            return Ok(existing.path.clone());
        }

        let socket_path = self.home.dial_socket_path(peer_addr.id, protocol);
        if let Some(existing) = sockets.remove(&key) {
            // Shell and exec deliberately replace their deterministic socket
            // path for each command. Stop and join the previous accept loop
            // before binding its replacement so its unlinked listener FD
            // cannot accumulate for the daemon's lifetime. Accepted sessions
            // have their own daemon-scoped lifetime and continue below.
            existing.stop().await;
        }
        if socket_path.exists() {
            fs::remove_file(&socket_path)
                .with_context(|| format!("failed to remove stale {}", socket_path.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        let listener_cancel = CancellationToken::new();
        let lease = DialListenerLease::new(self.active_dial_listeners.clone());

        // Built-in exec and legacy shell/0 remain one-shot raw framed streams.
        // Resumable shell/1 negotiates its own tunnel path and falls back to
        // shell/0 when the peer does not advertise the new ALPN.
        let listener_task = if alpn == exec::EXEC_ALPN
            || alpn == shell::SHELL_ALPN
            || alpn == gitremote::GIT_ALPN
        {
            tokio::spawn(run_raw_dial_socket(
                listener,
                self.endpoint_rx(),
                peer_addr.clone(),
                alpn,
                listener_cancel.clone(),
                self.cancel.clone(),
                self.dial_failures.clone(),
                self.dial_slots.clone(),
                self.peer_connections.clone(),
                lease,
            ))
        } else if alpn == shell::RESUMABLE_SHELL_ALPN {
            tokio::spawn(run_shell_dial_socket(
                listener,
                self.endpoint_rx(),
                self.home.clone(),
                peer.to_string(),
                peer_addr.clone(),
                listener_cancel.clone(),
                self.cancel.clone(),
                self.tunnel_drop_rx(),
                self.dial_failures.clone(),
                self.dial_slots.clone(),
                self.peer_connections.clone(),
                lease,
                self.client_attaches.clone(),
                self.connection_recorder(),
            ))
        } else {
            tokio::spawn(run_dial_socket(
                listener,
                self.endpoint_rx(),
                self.home.clone(),
                peer.to_string(),
                alpn,
                listener_cancel.clone(),
                self.cancel.clone(),
                self.tunnel_drop_rx(),
                self.dial_failures.clone(),
                self.dial_slots.clone(),
                self.peer_connections.clone(),
                lease,
                self.client_attaches.clone(),
                self.connection_recorder(),
            ))
        };
        sockets.insert(
            key,
            DialSocket {
                path: socket_path.clone(),
                peer_addr: peer_addr.clone(),
                listener_cancel,
                listener_task: Some(listener_task),
            },
        );
        drop(sockets);

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
            allow_exec: self.allow_exec,
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
            allow_exec: self.allow_exec,
            peers,
            connection_telemetry: self.telemetry.snapshot().peers,
            active_dial_handlers: self.active_dial_handlers(),
            max_dial_handlers: self.max_dial_handlers(),
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

    /// Open one logical stream against a peer's shared connection. This creates
    /// no listener or dial socket. It does not consult `dial_failures`, because
    /// shared dial backoff cannot answer whether a service is available now.
    /// The caller's deadline bounds the attempt.
    async fn probe_service(
        &self,
        peer: &str,
        protocol: &str,
        timeout: Duration,
    ) -> Result<ServiceProbe> {
        let addr = self.peer_book.read().await.resolve(peer)?;
        let peer_id = addr.id.to_string();
        let endpoint = self.endpoint_handle();

        let started = Instant::now();
        let attempt = tokio::time::timeout(
            timeout,
            self.peer_connections.open_stream(
                &endpoint.endpoint,
                endpoint.generation,
                &addr,
                protocol,
                mux::StreamActivity::Probe,
            ),
        )
        .await;
        let (outcome, round_trip, transport, error) = match attempt {
            Err(_elapsed) => (
                ProbeOutcome::Timeout,
                None,
                None,
                Some(format!("no answer within {:?}", timeout)),
            ),
            Ok(Ok(stream)) => {
                let elapsed = started.elapsed();
                let transport = classify_connection_transport(&stream.connection);
                (ProbeOutcome::Supported, Some(elapsed), transport, None)
            }
            Ok(Err(error)) => {
                if shell_resumable_alpn_unsupported(&error)
                    || error
                        .chain()
                        .any(|cause| cause.to_string().contains("is not exposed"))
                {
                    (ProbeOutcome::Unsupported, None, None, None)
                } else {
                    (
                        ProbeOutcome::Unreachable,
                        None,
                        None,
                        Some(format!("{error:#}")),
                    )
                }
            }
        };

        info!(
            target: VALIDATION_LOG_TARGET,
            event = "service_probe",
            peer,
            protocol,
            outcome = outcome.as_str(),
            timeout_ms = timeout.as_millis() as u64,
            "one-shot service probe"
        );

        Ok(ServiceProbe {
            peer: peer.to_string(),
            peer_id,
            outcome,
            round_trip,
            transport,
            error,
        })
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
        // Tell iroh to re-probe its paths, and leave working tunnels alone.
        //
        // This used to drop every tunnel connection unconditionally, on the
        // theory that a network change invalidates existing paths. In practice
        // the notice fires constantly on an idle healthy machine — 449 times in
        // one day on the author's laptop, every one of them reporting the exact
        // same unchanged state — and each drop tore down live sessions that
        // were working. A resumable shell reattached within about 76 seconds of
        // the previous one, forever. iroh already migrates or re-probes paths on
        // `network_change`, and a tunnel whose transport really is dead notices
        // and reconnects on its own, so the drop bought nothing and cost a
        // visible interruption every time.
        //
        // The recovery paths that genuinely need a teardown still do it
        // explicitly: `recover_unreachable_peer` when the endpoint itself is
        // suspect, `recycle_endpoint_if_generation` when the endpoint is
        // replaced, and the `debug drop-tunnels` control request.
        endpoint.endpoint.network_change().await;
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

    /// Drive recovery for a peer our active liveness probe found unreachable, even
    /// though no local network change fired (the roaming case). Cheap first: tell
    /// iroh to re-discover + re-probe all paths and drop stale tunnels so a roamed
    /// peer settles onto relay / its new direct address. Escalates to a full
    /// endpoint recycle only after repeated nudges have not brought it back — the
    /// same effect as the manual restart this replaces. `attempt` is the 1-based
    /// recovery attempt since the peer last answered.
    /// Recover one unreachable peer. `healthy_elsewhere` is how many OTHER peers
    /// answered in the same probe round: if any did, the endpoint is demonstrably
    /// working and this peer is simply away, so recovery stays cheap and local
    /// instead of recycling the endpoint out from under everyone else.
    async fn recover_unreachable_peer(
        &self,
        label: &str,
        attempt: usize,
        healthy_elsewhere: usize,
    ) {
        let endpoint = self.endpoint_handle();
        let attempts_exhausted = attempt >= PEER_HEALTH_ATTEMPTS_BEFORE_RECYCLE;
        let escalate_recycle = attempts_exhausted && healthy_elsewhere == 0;
        warn!(
            target: VALIDATION_LOG_TARGET,
            event = "peer_health_recover",
            peer = %label,
            generation = endpoint.generation,
            attempt,
            healthy_elsewhere,
            attempts_exhausted,
            escalate_recycle,
            "peer unreachable; re-probing paths"
        );
        eprintln!(
            "fabric: peer {label:?} unreachable (recovery attempt {attempt}, {healthy_elsewhere} other peers healthy); re-probing paths{}",
            if escalate_recycle {
                " + recycling endpoint"
            } else if attempts_exhausted {
                " (endpoint left alone: other peers are reachable)"
            } else {
                ""
            }
        );

        endpoint.endpoint.network_change().await;

        if escalate_recycle {
            // Only tear down live tunnels when the endpoint itself is suspect. One
            // roaming peer being away is no reason to drop everyone's sessions.
            self.drop_tunnel_connections();
            if let Err(error) = self
                .recycle_endpoint_if_generation(
                    endpoint.generation,
                    "peer unreachable after repeated re-probes",
                )
                .await
            {
                eprintln!(
                    "fabric: failed to recycle endpoint recovering peer {label:?}: {error:#}"
                );
            }
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
            self.any_peer_reachable_on_endpoint(endpoint.clone(), context),
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

    async fn any_peer_reachable_on_endpoint(
        &self,
        endpoint: CurrentEndpoint,
        context: &str,
    ) -> bool {
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
        let active_incoming_handlers = self.active_incoming_handlers();
        let active_dial_handlers = self.active_dial_handlers();
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

        // Never tear the transport out from under a user. Recycling drops every
        // attached shell and tunnel session, and a session that dies mid-command
        // is a worse outcome than whatever condition prompted the recycle. A
        // caller that genuinely preserves sessions uses the _preserving variant.
        let sessions = self.tunnel_sessions.stats().await;
        let outbound_attaches = self.client_attaches.attached();
        if let Some(attached) = recycle_blocked_by_sessions(&sessions, outbound_attaches) {
            warn!(
                target: VALIDATION_LOG_TARGET,
                event = "endpoint_recycle_skipped_sessions_attached",
                generation = self.endpoint_handle().generation,
                reason,
                active_sessions = sessions.active_sessions,
                active_attaches = sessions.active_attaches,
                outbound_attaches,
                "endpoint recycle skipped to preserve live sessions"
            );
            eprintln!(
                "fabric: not recycling endpoint ({reason}): {attached} live session(s) attached; leaving the transport up",
            );
            return Ok(EndpointRecycleOutcome::SessionsAttached {
                active_sessions: attached,
            });
        }

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
        close_endpoint_bounded(&old.endpoint, "endpoint recycle").await;
        let rss_after_close_bytes = current_rss_bytes();
        let trim_started = Instant::now();
        let allocator_trim = trim_process_allocator();
        let trim_duration = trim_started.elapsed();
        let rss_after_trim_bytes = current_rss_bytes();
        let duration = started.elapsed();
        info!(
            target: VALIDATION_LOG_TARGET,
            event = "endpoint_recycle",
            reason,
            old_generation = old.generation,
            new_generation,
            duration_ms = duration.as_millis() as u64,
            rss_before_known = rss_before_bytes.is_some(),
            rss_before_bytes = rss_before_bytes.unwrap_or(0),
            rss_after_close_known = rss_after_close_bytes.is_some(),
            rss_after_close_bytes = rss_after_close_bytes.unwrap_or(0),
            allocator_trim_attempted = allocator_trim.attempted,
            allocator_trim_succeeded = allocator_trim.succeeded,
            allocator_trim_duration_ms = trim_duration.as_millis() as u64,
            rss_after_trim_known = rss_after_trim_bytes.is_some(),
            rss_after_trim_bytes = rss_after_trim_bytes.unwrap_or(0),
            rss_after_known = rss_after_trim_bytes.is_some(),
            rss_after_bytes = rss_after_trim_bytes.unwrap_or(0),
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

        // Build the file-sync engine with a weak handle back to the state (so it
        // can dial peers) and start watching configured folders.
        let author = sync_author(state.id());
        let transport = IrohSyncTransport::new(Arc::downgrade(&state));
        let engine =
            SyncEngine::new(state.home.clone(), author, transport, state.cancel.clone()).await?;
        let _ = state.sync_engine.set(engine.clone());
        tokio::spawn(async move {
            if let Err(error) = engine.run().await {
                warn!(%error, "sync engine stopped");
            }
        });

        spawn_outgoing_mux_accepts(&state).await?;

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

async fn spawn_outgoing_mux_accepts(state: &Arc<DaemonState>) -> Result<()> {
    let mut receiver = state
        .opened_mux_connections
        .lock()
        .await
        .take()
        .context("outgoing mux accept loop already started")?;
    let state = state.clone();
    tokio::spawn(async move {
        loop {
            let connection = tokio::select! {
                _ = state.cancel.cancelled() => return,
                connection = receiver.recv() => connection,
            };
            let Some(connection) = connection else {
                return;
            };
            let state = state.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_mux_connection(connection, state, false).await {
                    debug!(%error, "outgoing mux accept loop failed");
                }
            });
        }
    });
    Ok(())
}

pub async fn run_daemon(home: FabricHome, allow_shell: bool) -> Result<()> {
    run_daemon_with_options(home, DaemonOptions::new(allow_shell)).await
}

pub async fn run_daemon_with_options(home: FabricHome, options: DaemonOptions) -> Result<()> {
    let _lease = DaemonLease::acquire(&home)?;
    init_daemon_tracing(&home)?;
    FabricNode::start_with_daemon_options(home, options)
        .await?
        .wait()
        .await
}

struct DaemonLease {
    _file: std::fs::File,
}

pub fn daemon_lock_available(home: &FabricHome) -> Result<bool> {
    let path = home.root().join("run/daemon.lock");
    home.prepare()?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    #[cfg(unix)]
    {
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn restart_down_decision(status_ok: bool, lease_available: bool) -> bool {
    !status_ok && lease_available
}

impl DaemonLease {
    fn acquire(home: &FabricHome) -> Result<Self> {
        home.prepare()?;
        let path = home.root().join("run/daemon.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        #[cfg(unix)]
        {
            // "Already held" can be TRANSIENTLY WRONG, so it is retried briefly
            // before being believed.
            //
            // A flock belongs to the open file description, and a forked child
            // shares its parent's descriptions until it execs. fabric spawns
            // subprocesses constantly — exec, shell, `security`, `systemctl` —
            // so a lease released a moment ago can still be pinned by a child
            // that has forked and not yet reached exec. The descriptor is
            // CLOEXEC, so the window is microseconds, and it closes on its own.
            //
            // Found as a test that failed three runs in five once something else
            // in the same binary started spawning subprocesses. The same race
            // reaches `fabric restart`, where it would read as "another daemon
            // is running" when none is.
            //
            // The retry is deliberately short, and the number is bounded from
            // BOTH sides rather than picked.
            //
            // Below: the window it rides out is a fork waiting to exec, which is
            // microseconds, so 200 ms is a thousandfold margin.
            //
            // Above: a REFUSAL has to stay prompt. A second daemon told the
            // lease is taken should say so and exit, and somebody waiting on
            // that answer should not sit through a long pause.
            // `second_daemon_for_same_home_is_refused_by_lease` allows 500 ms
            // for the whole refusal, and a first attempt at 500 ms here failed
            // it — the daemon was still retrying when the test looked. That test
            // is right: a refusal is an answer, and an answer should arrive.
            const LEASE_RETRY_FOR: Duration = Duration::from_millis(200);
            let deadline = Instant::now() + LEASE_RETRY_FOR;
            loop {
                let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if rc == 0 {
                    break;
                }
                if Instant::now() >= deadline {
                    bail!("fabric daemon lease is already held");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        Ok(Self { _file: file })
    }
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
        result = run_endpoint_rss_observe_loop(state.clone()) => result?,
        result = run_peer_health_loop(state.clone()) => result?,
        result = run_endpoint_snapshot_loop(state.clone()) => result?,
        _ = state.cancel.cancelled() => {}
    }

    state.cancel.cancel();
    close_endpoint_bounded(&state.current_endpoint(), "daemon shutdown").await;
    let _ = fs::remove_file(control_path);
    let dial_sockets: Vec<DialSocket> = state
        .dial_sockets
        .lock()
        .await
        .drain()
        .map(|(_, socket)| socket)
        .collect();
    for socket in dial_sockets {
        socket.stop().await;
    }
    Ok(())
}

async fn wait_for_close<F>(close: F, bound: Duration) -> bool
where
    F: std::future::Future<Output = ()>,
{
    tokio::time::timeout(bound, close).await.is_ok()
}

async fn close_endpoint_bounded(endpoint: &Endpoint, context: &str) {
    if !wait_for_close(endpoint.close(), ENDPOINT_CLOSE_TIMEOUT).await {
        warn!(
            target: VALIDATION_LOG_TARGET,
            event = "endpoint_close_timeout",
            context,
            timeout_ms = ENDPOINT_CLOSE_TIMEOUT.as_millis() as u64,
            "endpoint close exceeded its deadline"
        );
        eprintln!(
            "fabric: endpoint close exceeded {:?} during {context}; continuing shutdown",
            ENDPOINT_CLOSE_TIMEOUT
        );
    }
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
    run_rehome_updates(state, monitor.interface_state()).await
}

/// The source of interface-change updates.
///
/// Abstracted for one reason: so the loop below can be driven by a fake that
/// ends the stream on demand and the "monitor stopped" branch can be tested
/// without waiting for a real OS network monitor to die. Production is the
/// netwatch watcher.
trait InterfaceUpdates: Send {
    fn next_update(
        &mut self,
    ) -> impl std::future::Future<Output = Result<netwatch::interfaces::State>> + Send;
}

impl InterfaceUpdates for n0_watcher::Direct<netwatch::interfaces::State> {
    async fn next_update(&mut self) -> Result<netwatch::interfaces::State> {
        use n0_watcher::Watcher as _;
        self.updated()
            .await
            .map_err(|_| anyhow::anyhow!("network monitor watcher disconnected"))
    }
}

/// Drive roaming rehome from an interface-update stream until the daemon is
/// cancelled. Split from `run_network_rehome_loop` so the monitor-stopped path
/// is testable with an injected stream.
async fn run_rehome_updates(
    state: Arc<DaemonState>,
    mut interfaces: impl InterfaceUpdates,
) -> Result<()> {
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
            update = interfaces.next_update() => {
                let Ok(network_state) = update else {
                    eprintln!("fabric: network monitor stopped; roaming rehome disabled");
                    // PARK, do not exit. Returning Ok here would end serve()'s
                    // select! and shut the daemon down with exit code 0, which
                    // no supervisor restarts: the launchd plist sets
                    // KeepAlive.SuccessfulExit=false and the systemd unit sets
                    // Restart=on-failure. So the daemon would stay down until a
                    // person noticed, and the one log line would say roaming was
                    // disabled, not that the daemon exited. The daemon keeps
                    // serving shell, exec and sync; only roaming rehome is lost.
                    // Finding 9 of the 2026-08-29 review. This matches the
                    // monitor-unavailable-at-startup branch above, which already
                    // parks rather than returning.
                    state.cancel.cancelled().await;
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

/// Actively probe each trusted peer's liveness on an interval and, when a peer
/// stops answering (e.g. it roamed to a new network) with no local network change
/// to trigger the netmon rehome path, drive recovery for it. This closes the gap
/// where a roamed peer stayed unreachable both ways until a manual daemon restart.
/// It also emits per-probe latency + transport (direct/relay) telemetry.
///
/// `FABRIC_PEER_HEALTH_SECS` overrides the probe interval; `0` disables the loop.
async fn run_peer_health_loop(state: Arc<DaemonState>) -> Result<()> {
    let probe_interval = match std::env::var("FABRIC_PEER_HEALTH_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        Some(0) => {
            info!(
                target: VALIDATION_LOG_TARGET,
                event = "peer_health_disabled",
                "peer liveness probe disabled via FABRIC_PEER_HEALTH_SECS=0"
            );
            state.cancel.cancelled().await;
            return Ok(());
        }
        Some(secs) => Duration::from_secs(secs),
        None => PEER_HEALTH_PROBE_INTERVAL,
    };
    run_peer_health_loop_with(
        state,
        probe_interval,
        PEER_HEALTH_FAILURES_BEFORE_RECOVER,
        PEER_HEALTH_RECOVER_INITIAL_BACKOFF,
        PEER_HEALTH_RECOVER_MAX_BACKOFF,
    )
    .await
}

async fn run_peer_health_loop_with(
    state: Arc<DaemonState>,
    probe_interval: Duration,
    failures_before_recover: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Result<()> {
    let mut interval = tokio::time::interval(probe_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    let mut tracker = PeerHealthTracker::new(failures_before_recover, initial_backoff, max_backoff);
    let mut path_quality = pathwatch::PathQualityTracker::new(
        PATH_QUALITY_ABSOLUTE_FLOOR,
        PATH_QUALITY_BASELINE_MULTIPLIER,
        PATH_QUALITY_CONSECUTIVE_SAMPLES,
        PATH_QUALITY_WARMUP_SAMPLES,
        PATH_QUALITY_REDIAL_COOLDOWN,
    );
    let mut selected_paths = HashMap::<EndpointId, Option<String>>::new();

    loop {
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            _ = interval.tick() => {
                // A local network outage is not the peer's fault — the netmon rehome
                // and endpoint health poll own that case. Skip so we don't false-trigger
                // peer recovery for every peer whenever our own uplink blips.
                if !state.network_usable.load(Ordering::SeqCst) {
                    continue;
                }
                let peers = state.peer_book.read().await.peers().to_vec();
                // Probe the whole round before recovering anything. A single absent
                // roaming peer must not be able to order a global endpoint recycle
                // while other peers are answering fine, so the recovery decision
                // needs to know how the rest of the network looked this round.
                let mut round = Vec::with_capacity(peers.len());
                let mut reachable_peers = 0usize;
                for peer in peers {
                    let peer_id = peer.id;
                    let label = peer.name.clone().unwrap_or_else(|| peer_id.to_string());
                    if state
                        .peer_connections
                        .recently_active(peer_id, probe_interval)
                        .await
                    {
                        info!(
                            target: VALIDATION_LOG_TARGET,
                            event = "peer_health_probe_skipped",
                            peer = %label,
                            recent_application_traffic = true,
                            window_ms = probe_interval.as_millis() as u64,
                            "recent application traffic proved peer liveness"
                        );
                        reachable_peers += 1;
                        round.push((peer_id, label, true));
                        continue;
                    }
                    let health = state.check_peer_reachability(peer).await;
                    info!(
                        target: VALIDATION_LOG_TARGET,
                        event = "peer_health_probe",
                        peer = %label,
                        reachable = health.reachable,
                        rtt_us = health.round_trip_micros.unwrap_or(0),
                        transport = health.transport.as_deref().unwrap_or("none"),
                        "peer liveness probe"
                    );
                    // Keep what the probe measured. Before this it computed a
                    // round trip time and a path and discarded both, so the only
                    // way to compare direct against relay was to parse days of
                    // log text.
                    state.telemetry.record_probe(
                        &label,
                        health.reachable,
                        health.transport.as_deref(),
                        health.round_trip_micros.map(Duration::from_micros),
                    );
                    if let Some(round_trip_micros) = health.round_trip_micros
                        && let Some(connection) = state.peer_connections.connection(peer_id).await
                    {
                        pathwatch::log_paths(
                            &connection,
                            &label,
                            Duration::from_micros(round_trip_micros),
                            selected_paths.entry(peer_id).or_default(),
                        );
                    }
                    if let Some(transport) = health.transport.as_deref()
                        && let Ok(mut seen) = state.last_probe_transport.write()
                    {
                        seen.insert(label.clone(), transport.to_string());
                    }
                    if health.reachable
                        && let (Some(round_trip_micros), Some(transport)) =
                            (health.round_trip_micros, health.transport.as_deref())
                        && let pathwatch::PathQualityAction::Redial {
                            class,
                            baseline,
                            observed,
                        } = path_quality.on_sample(
                            peer_id,
                            state.endpoint_handle().generation,
                            transport,
                            Duration::from_micros(round_trip_micros),
                            Instant::now(),
                        )
                    {
                        let redialled = state
                            .peer_connections
                            .redial(peer_id, b"path quality degraded")
                            .await;
                        warn!(
                            target: VALIDATION_LOG_TARGET,
                            event = "path_quality_redial",
                            peer = %label,
                            class = %class,
                            baseline_ms = baseline.as_secs_f64() * 1000.0,
                            observed_ms = observed.as_secs_f64() * 1000.0,
                            redialled,
                            "persistent path degradation closed the shared peer connection"
                        );
                    }
                    if health.reachable {
                        reachable_peers += 1;
                    }
                    round.push((peer_id, label, health.reachable));
                }
                for (peer_id, label, reachable) in round {
                    if let PeerHealthAction::Recover { attempt } =
                        tracker.on_probe(peer_id, reachable, Instant::now())
                    {
                        let healthy_elsewhere = if reachable {
                            reachable_peers.saturating_sub(1)
                        } else {
                            reachable_peers
                        };
                        state
                            .recover_unreachable_peer(&label, attempt, healthy_elsewhere)
                            .await;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Recovery decision for a single peer probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerHealthAction {
    /// Peer is healthy or not yet past the failure threshold — do nothing.
    None,
    /// Drive recovery for this peer now. `attempt` is the 1-based recovery attempt
    /// since the peer last answered, driving escalating backoff + recycle escalation.
    Recover { attempt: usize },
}

/// Per-peer liveness bookkeeping feeding [`PeerHealthTracker`].
#[derive(Debug, Default, Clone)]
struct PeerHealthState {
    consecutive_failures: usize,
    recover_attempts: usize,
    next_recover_at: Option<Instant>,
}

/// Pure state machine: turns a stream of per-peer probe results into recovery
/// decisions. Kept clock-injected and free of any endpoint so the
/// failure→recover→backoff→reset logic is unit-testable in isolation.
struct PeerHealthTracker {
    failures_before_recover: usize,
    initial_backoff: Duration,
    max_backoff: Duration,
    peers: HashMap<EndpointId, PeerHealthState>,
}

impl PeerHealthTracker {
    fn new(
        failures_before_recover: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Self {
        Self {
            failures_before_recover: failures_before_recover.max(1),
            initial_backoff,
            max_backoff,
            peers: HashMap::new(),
        }
    }

    /// Record one probe result for `peer`. A reachable probe resets that peer's
    /// state; a failure counts toward the threshold and, once reached, fires
    /// `Recover` — then gates further fires behind an escalating backoff so a
    /// genuinely-down peer is retried periodically instead of on every probe.
    fn on_probe(&mut self, peer: EndpointId, reachable: bool, now: Instant) -> PeerHealthAction {
        let state = self.peers.entry(peer).or_default();
        if reachable {
            *state = PeerHealthState::default();
            return PeerHealthAction::None;
        }
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.consecutive_failures < self.failures_before_recover {
            return PeerHealthAction::None;
        }
        if let Some(next) = state.next_recover_at
            && now < next
        {
            return PeerHealthAction::None;
        }
        state.recover_attempts = state.recover_attempts.saturating_add(1);
        let backoff = peer_recovery_backoff(
            state.recover_attempts,
            self.initial_backoff,
            self.max_backoff,
        );
        state.next_recover_at = Some(now + backoff);
        PeerHealthAction::Recover {
            attempt: state.recover_attempts,
        }
    }
}

/// Escalating, capped backoff between repeated recovery attempts for a peer that
/// stays unreachable: `initial * 2^(attempt-1)`, capped at `max`; zero for attempt 0.
fn peer_recovery_backoff(attempt: usize, initial: Duration, max: Duration) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let exponent = attempt.saturating_sub(1).min(8) as u32;
    initial.saturating_mul(1u32 << exponent).min(max)
}

/// How many live sessions block an endpoint recycle, if any.
///
/// Both counters matter: `active_sessions` covers a session whose PTY is alive,
/// and `active_attaches` covers a client currently attached to one. Recycling
/// with either non-zero drops a user's shell mid-command.
fn recycle_blocked_by_sessions(
    stats: &tunnel::ServerSessionStats,
    outbound_attaches: usize,
) -> Option<usize> {
    let attached = stats.active_sessions + stats.active_attaches + outbound_attaches;
    (attached > 0).then_some(attached)
}

fn bytes_to_mib(bytes: u64) -> u64 {
    bytes / (1024 * 1024)
}

async fn run_endpoint_rss_observe_loop(state: Arc<DaemonState>) -> Result<()> {
    run_endpoint_rss_observe_loop_with_sampler(
        state,
        ENDPOINT_RSS_OBSERVE_POLL_INTERVAL,
        ENDPOINT_RSS_REPORT_STEP_BYTES,
        Arc::new(current_rss_bytes),
    )
    .await
}

/// Watch RSS and report it. Nothing here interrupts the daemon.
///
/// This loop used to recycle the iroh endpoint whenever RSS crossed a fixed
/// 300 MiB threshold. That was actively harmful: the memory does not live in the
/// endpoint, so recycling never reclaimed it — this daemon logged 599 recycles
/// that its own follow-up sample proved ineffective — while every recycle tore
/// down live shell and tunnel sessions and forced every peer to re-handshake.
/// A fixed limit also has no idea what a healthy working set is for a given
/// network size. Memory growth is now reported for an operator to judge, and only
/// an operator stops the daemon.
async fn run_endpoint_rss_observe_loop_with_sampler(
    state: Arc<DaemonState>,
    poll_interval: Duration,
    report_step_bytes: u64,
    sample_rss: RssSampler,
) -> Result<()> {
    let mut interval = tokio::time::interval(poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    let mut peak_reported_bytes = 0u64;

    loop {
        tokio::select! {
            _ = state.cancel.cancelled() => break,
            _ = interval.tick() => {
                let Some(rss_bytes) = sample_rss() else {
                    debug!(
                        target: VALIDATION_LOG_TARGET,
                        event = "endpoint_rss_monitor",
                        rss_known = false,
                        "RSS monitor could not read current RSS"
                    );
                    continue;
                };

                let generation = state.endpoint_handle().generation;
                debug!(
                    target: VALIDATION_LOG_TARGET,
                    event = "endpoint_rss_observed",
                    generation,
                    rss_bytes,
                    poll_interval_ms = poll_interval.as_millis() as u64,
                    "endpoint RSS sample"
                );

                // Report each new peak once it clears the previous report by a
                // whole step, so growth is visible without a per-sample stream.
                if rss_bytes >= peak_reported_bytes.saturating_add(report_step_bytes) {
                    peak_reported_bytes = rss_bytes;
                    warn!(
                        target: VALIDATION_LOG_TARGET,
                        event = "endpoint_rss_growth",
                        generation,
                        rss_bytes,
                        "daemon RSS reached a new reported peak"
                    );
                    eprintln!(
                        "fabric: memory in use {} MiB (new peak, endpoint generation {generation}); reporting only, no action taken",
                        bytes_to_mib(rss_bytes),
                    );
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
        ControlRequest::Probe {
            peer,
            protocol,
            timeout_ms,
        } => {
            let outcome = state
                .probe_service(&peer, &protocol, Duration::from_millis(timeout_ms))
                .await?;
            ControlResponse::ProbeResult {
                peer: outcome.peer,
                peer_id: outcome.peer_id,
                protocol,
                outcome: outcome.outcome.to_string(),
                round_trip_micros: outcome
                    .round_trip
                    .map(|rt| rt.as_micros().try_into().unwrap_or(u64::MAX)),
                transport: outcome.transport,
                error: outcome.error,
            }
        }
        ControlRequest::Shell { peer } => {
            let socket = state
                .dial_alpn(
                    &peer,
                    shell::SHELL_PROTOCOL,
                    shell::RESUMABLE_SHELL_ALPN.to_vec(),
                    false,
                )
                .await?;
            ControlResponse::Shell { socket }
        }
        ControlRequest::Exec { peer } => {
            let socket = state
                .dial_alpn(&peer, exec::EXEC_PROTOCOL, exec::EXEC_ALPN.to_vec(), false)
                .await?;
            ControlResponse::Exec { socket }
        }
        ControlRequest::Git { peer } => {
            let socket = state
                .dial_alpn(
                    &peer,
                    gitremote::GIT_PROTOCOL,
                    gitremote::GIT_ALPN.to_vec(),
                    true,
                )
                .await?;
            ControlResponse::Git { socket }
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
        ControlRequest::SyncReload => {
            if let Some(engine) = state.sync_engine() {
                engine.reload().await?;
                for name in engine.names().await {
                    let _ = engine.sync_once(&name).await;
                }
            }
            ControlResponse::Ok
        }
        ControlRequest::SendFile { peer, path, name } => {
            // Read the size, not the bytes: the file streams from disk during
            // the transfer, so the sending daemon never holds it whole.
            let bytes = match std::fs::metadata(&path) {
                Ok(meta) => meta.len(),
                Err(error) => {
                    return Ok(ControlResponse::Error {
                        message: format!("could not read {}: {error}", path.display()),
                    });
                }
            };
            match send_file_to_peer(&state, &peer, &name, &path).await {
                Ok(()) => ControlResponse::SentFile {
                    peer,
                    name,
                    bytes,
                },
                Err(error) => ControlResponse::Error {
                    message: format!("{error:#}"),
                },
            }
        }
        ControlRequest::SyncStatus => {
            let entries = match state.sync_engine() {
                Some(engine) => engine
                    .status()
                    .await
                    .into_iter()
                    .map(|status| SyncEntryStatus {
                        delta_fallbacks: status.delta_fallbacks,
                        full_payload_sends: status.full_payload_sends,
                        content_bytes: status.content_bytes,
                        stopped_peers: status.stopped_peers,
                        digest: status.digest,
                        name: status.name,
                        folder: status.folder.display().to_string(),
                        policy: status.policy.to_string(),
                        peers: peers_display(&status.peers),
                        files: status.present,
                        present: status.present,
                        tombstones: status.tombstones,
                        observed: status.observed,
                        missing: status.missing,
                        unexpected: status.unexpected,
                        mismatched: status.mismatched,
                        scan_issues: status.scan_issues,
                        full_scans: status.full_scans,
                        inbound_noop_transactions: status.inbound_noop_transactions,
                        inbound_guarded_transactions: status.inbound_guarded_transactions,
                        sync_passes: status.sync_passes,
                        scan_micros: status.scan_micros,
                        materialize_micros: status.materialize_micros,
                        persist_micros: status.persist_micros,
                        reconcile_micros: status.reconcile_micros,
                        reconcile_wire_bytes: status.reconcile_wire_bytes,
                        reconcile_failures: status.reconcile_failures,
                        sweep: status
                            .sweep
                            .as_ref()
                            .map(|state| state.token())
                            .unwrap_or_default(),
                    })
                    .collect(),
                None => Vec::new(),
            };
            ControlResponse::SyncStatus { entries }
        }
        ControlRequest::Shutdown => {
            state.cancel.cancel();
            ControlResponse::Ok
        }
    };
    Ok(response)
}

fn peers_display(peers: &SyncPeers) -> String {
    match peers {
        SyncPeers::Wildcard(_) => "*".to_string(),
        SyncPeers::List(list) => list.join(","),
    }
}

async fn run_iroh_accept_loop(state: Arc<DaemonState>) -> Result<()> {
    let mut endpoint_rx = state.endpoint_rx();
    loop {
        // Deliberately no failure gate here. One peer's rejected handshake is not
        // a reason to stop accepting from everyone else, and a shared gate made
        // that escalate: the more one peer failed, the longer every healthy peer
        // waited to be let in. Concurrency is bounded by the permit acquired just
        // below and held for the handler's life, so a flood of failing
        // connections is capped by handler slots rather than by a delay. The real
        // stop conditions are still below: a closed accept stream and
        // cancellation.
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
    let mut identity = IncomingIdentity::default();
    match process_incoming_iroh(incoming, state.clone(), &mut identity).await {
        Ok(()) => {
            state
                .incoming_failures
                .record_success(&identity.backoff_key())
                .await
        }
        Err(error) => {
            // Recorded for the rate-limited log only. Nothing waits on inbound
            // records, which is what keeps an unidentified connection from
            // throttling anything: there is no gate left for it to hold.
            state
                .incoming_failures
                .record_failure_for_diagnostics(
                    &identity.backoff_key(),
                    "incoming iroh connection failed",
                    &error,
                )
                .await;
        }
    }
}

/// What we know about an inbound connection, as we learn it.
///
/// A connection can fail before its ALPN is readable, and its peer id is only
/// known once the handshake completes, so diagnostics have to work with partial
/// identity rather than waiting for all of it.
#[derive(Debug, Default)]
struct IncomingIdentity {
    alpn: Option<String>,
    peer: Option<String>,
}

impl IncomingIdentity {
    fn backoff_key(&self) -> BackoffKey {
        BackoffKey {
            peer: self
                .peer
                .clone()
                .unwrap_or_else(|| "<unidentified>".to_string()),
            alpn: self
                .alpn
                .clone()
                .unwrap_or_else(|| "<unnegotiated>".to_string()),
        }
    }
}

/// Complete the handshake and record who it turned out to be.
///
/// The peer id only exists once the handshake completes, so this is the single
/// seam where an inbound connection stops being anonymous. Every branch below
/// goes through it, because a diagnostic record keyed on an unknown peer is
/// shared with every other unknown peer on the same ALPN — which means one
/// peer's success would clear another's failure record.
async fn handshake_and_identify(
    accepting: iroh::endpoint::Accepting,
    identity: &mut IncomingIdentity,
) -> Result<Connection> {
    let connection = accepting.await?;
    identity.peer = Some(connection.remote_id().to_string());
    Ok(connection)
}

impl DaemonState {
    /// May this peer reach this service? PER-PEER policy only.
    ///
    /// The daemon-wide `allow_shell` / `allow_exec` blanket is deliberately NOT
    /// applied here, and that is a correction rather than an omission. It is
    /// already enforced further in, by `serve_shell_disabled` and its exec
    /// twin, which refuse at the protocol level with a readable sentence and
    /// exit 126 — the conventional "found but not permitted to run".
    ///
    /// Checking it here as well replaced that with a closed connection and exit
    /// 1, which is a worse error for the same condition. A blanket that
    /// subtracts is right; subtracting it twice, in the place with the poorer
    /// message, is not.
    ///
    /// The ordering property still holds: no `allow` list can lift the blanket,
    /// because the blanket refuses after this check passes.
    pub async fn may(
        &self,
        peer: &iroh::EndpointId,
        service: &str,
    ) -> Result<(), crate::config::Denied> {
        self.peer_book.read().await.may(peer, service)
    }
}

/// The name a person would write in `allow` for this ALPN.
///
/// The ALPN is the protocol string verbatim, so an exposed service is simply
/// its own name. The built-ins get the short word someone would actually type,
/// and BOTH shell ALPNs answer to `shell`: a permission should be about the
/// service, not about which wire version negotiated it.
fn service_name_for_alpn(alpn: &[u8]) -> String {
    if alpn == BUILTIN_ECHO_ALPN {
        return ECHO_SERVICE.to_string();
    }
    if alpn == shell::SHELL_ALPN || alpn == shell::RESUMABLE_SHELL_ALPN {
        return SHELL_SERVICE.to_string();
    }
    if alpn == exec::EXEC_ALPN {
        return EXEC_SERVICE.to_string();
    }
    if alpn == SYNC_ALPN {
        return SYNC_SERVICE.to_string();
    }
    if alpn == crate::sendfile::SEND_FILE_ALPN {
        return crate::sendfile::SERVICE.to_string();
    }
    String::from_utf8_lossy(alpn).to_string()
}

/// Refuse a connection from a peer that is trusted but not permitted here.
///
/// Closed with the reason attached, so the dialling side can print WHY rather
/// than reporting a reset. A denial nobody can read is indistinguishable from a
/// network fault, and somebody who cannot tell them apart turns the feature off.
async fn deny_connection(connection: &Connection, service: &str, denied: &crate::config::Denied) {
    let reason = denied.to_string();
    tracing::info!(
        target: VALIDATION_LOG_TARGET,
        event = "permission_denied",
        peer = %connection.remote_id(),
        service = service,
        reason = %reason,
        "refused a service this peer is not permitted to reach"
    );
    connection.close(VarInt::from_u32(403), reason.as_bytes());
}

async fn process_incoming_iroh(
    incoming: Incoming,
    state: Arc<DaemonState>,
    identity: &mut IncomingIdentity,
) -> Result<()> {
    let mut accepting = incoming.accept()?;
    let alpn = accepting.alpn().await?;
    identity.alpn = Some(String::from_utf8_lossy(&alpn).to_string());

    // A generic exposure that does not exist is refused before the handshake,
    // exactly as before.
    let exposure = if matches_reserved_alpn(&alpn) {
        None
    } else {
        let found = {
            let exposures = state.exposures.read().await;
            exposures.get(alpn.as_slice()).cloned()
        };
        match found {
            Some(exposure) => Some(exposure),
            None => return Ok(()),
        }
    };

    let connection = handshake_and_identify(accepting, identity).await?;

    if alpn == mux::MUX_ALPN {
        log_connection_paths("mux_accept", &connection);
        handle_mux_connection(connection, state, true).await?;
        return Ok(());
    }

    // Git checks its qualified grant after it reads the requested remote and
    // operation. The handshake already proved that the peer is trusted.
    if alpn == gitremote::GIT_ALPN {
        log_connection_paths("builtin_git_accept", &connection);
        handle_git(connection, state).await?;
        return Ok(());
    }

    // ONE GATE for ordinary services: echo, both shells, exec, sync, and every
    // generic exposure. Git uses the exact repository grant above.
    //
    // Trusted and permitted are two different questions. `AllowListHook`
    // answered the first at handshake; this answers the second, and it needs
    // the ALPN, which the hook cannot see.
    let service = service_name_for_alpn(&alpn);
    if let Err(denied) = state.may(&connection.remote_id(), &service).await {
        deny_connection(&connection, &service, &denied).await;
        return Ok(());
    }

    if alpn == BUILTIN_ECHO_ALPN {
        log_connection_paths("builtin_echo_accept", &connection);
        handle_builtin_echo(connection, state).await?;
        return Ok(());
    }
    if alpn == shell::SHELL_ALPN {
        log_connection_paths("builtin_legacy_shell_accept", &connection);
        handle_builtin_legacy_shell(connection, state).await?;
        return Ok(());
    }
    if alpn == shell::RESUMABLE_SHELL_ALPN {
        log_connection_paths("builtin_resumable_shell_accept", &connection);
        handle_builtin_resumable_shell(connection, state).await?;
        return Ok(());
    }
    if alpn == exec::EXEC_ALPN {
        log_connection_paths("builtin_exec_accept", &connection);
        handle_builtin_exec(connection, state).await?;
        return Ok(());
    }
    if alpn == SYNC_ALPN {
        log_sync_connection_paths(&connection);
        handle_sync(connection, state).await?;
        return Ok(());
    }
    if alpn == crate::sendfile::SEND_FILE_ALPN {
        log_connection_paths("send_file_accept", &connection);
        handle_send_file(connection, state).await?;
        return Ok(());
    }

    let Some(exposure) = exposure else {
        return Ok(());
    };
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

/// Dial a peer and hand it one file.
async fn send_file_to_peer(
    state: &Arc<DaemonState>,
    peer: &str,
    name: &str,
    path: &std::path::Path,
) -> Result<()> {
    let addr = {
        let book = state.peer_book.read().await;
        let found = book
            .peers()
            .iter()
            .find(|candidate| {
                candidate.id.to_string() == peer || candidate.name.as_deref() == Some(peer)
            })
            .cloned();
        let found = found.with_context(|| format!("peer {peer:?} is not trusted"))?;
        found
            .addr
            .clone()
            .unwrap_or_else(|| EndpointAddr::new(found.id))
    };
    let stream = state
        .open_peer_stream(
            &addr,
            std::str::from_utf8(crate::sendfile::SEND_FILE_ALPN)
                .expect("send-file ALPN is UTF-8"),
            mux::StreamActivity::Application,
        )
        .await
        .with_context(|| format!("dialling {peer}"))?;
    let joined = tokio::io::join(stream.recv, stream.send);
    crate::sendfile::send_file(joined, name, path).await
}

/// Accept one file into this peer's inbox.
///
/// The peer's own id names the inbox, taken from the CONNECTION rather than
/// from anything the sender said, so a peer cannot choose where its files land.
async fn handle_send_file(connection: Connection, state: Arc<DaemonState>) -> Result<()> {
    let peer = connection.remote_id();
    let (send, recv) = connection.accept_bi().await?;
    receive_file_stream(peer, send, recv, state).await?;
    connection.closed().await;
    Ok(())
}

async fn receive_file_stream(
    peer: EndpointId,
    send: SendStream,
    recv: RecvStream,
    state: Arc<DaemonState>,
) -> Result<()> {
    let stream = tokio::io::join(recv, send);
    match crate::sendfile::receive(stream, &state.home, &peer.to_string()).await {
        Ok(path) => {
            info!(peer = %peer, path = %path.display(), "received a file");
        }
        Err(error) => {
            debug!(peer = %peer, %error, "refused or failed to receive a file");
        }
    }
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

async fn handle_builtin_legacy_shell(
    connection: Connection,
    state: Arc<DaemonState>,
) -> Result<()> {
    let peer = connection.remote_id().to_string();
    let (mut send, mut recv) = connection.accept_bi().await?;
    if state.allow_shell {
        shell::serve_shell_session(&mut recv, &mut send, &peer).await?;
    } else {
        shell::serve_shell_disabled(&mut send).await?;
    }
    send.finish()?;
    connection.closed().await;
    Ok(())
}

async fn handle_builtin_resumable_shell(
    connection: Connection,
    state: Arc<DaemonState>,
) -> Result<()> {
    let peer_id = connection.remote_id();
    let (send, recv) = connection.accept_bi().await?;
    tunnel::serve_connection(
        connection,
        send,
        recv,
        peer_id,
        tunnel::ServerTarget::Shell {
            allowed: state.allow_shell,
        },
        state.tunnel_sessions.clone(),
        state.tunnel_drop_rx(),
    )
    .await
}

async fn handle_builtin_exec(connection: Connection, state: Arc<DaemonState>) -> Result<()> {
    let peer = connection.remote_id().to_string();
    let (mut send, mut recv) = connection.accept_bi().await?;
    if state.allow_exec {
        exec::serve_exec_session(&mut recv, &mut send, &peer).await?;
    } else {
        exec::serve_exec_disabled(&mut send).await?;
    }
    send.finish()?;
    connection.closed().await;
    Ok(())
}

async fn handle_git(connection: Connection, state: Arc<DaemonState>) -> Result<()> {
    let peer = connection.remote_id();
    let (send, recv) = connection.accept_bi().await?;
    let book = state.peer_book.read().await.clone();
    gitremote::serve_session(recv, send, book, peer, state.git_sessions.clone()).await?;
    connection.closed().await;
    Ok(())
}

async fn handle_mux_connection(
    connection: Connection,
    state: Arc<DaemonState>,
    register_incoming: bool,
) -> Result<()> {
    if register_incoming {
        let generation = state.endpoint_handle().generation;
        state
            .peer_connections
            .register_incoming(&connection, generation)
            .await;
    }
    loop {
        let permit = tokio::select! {
            _ = state.cancel.cancelled() => return Ok(()),
            _ = connection.closed() => return Ok(()),
            permit = state.mux_stream_slots.clone().acquire_owned() => {
                permit.context("mux stream semaphore closed")?
            }
        };
        let accepted = tokio::select! {
            _ = state.cancel.cancelled() => return Ok(()),
            _ = connection.closed() => return Ok(()),
            accepted = connection.accept_bi() => accepted,
        };
        let (send, recv) = match accepted {
            Ok(streams) => streams,
            Err(_) => return Ok(()),
        };
        let connection = connection.clone();
        let state = state.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = handle_mux_stream(connection, send, recv, state).await {
                debug!(%error, "mux stream failed");
            }
        });
    }
}

async fn handle_mux_stream(
    connection: Connection,
    mut send: SendStream,
    mut recv: RecvStream,
    state: Arc<DaemonState>,
) -> Result<()> {
    let header = tokio::time::timeout(
        Duration::from_secs(10),
        mux::MuxStreamHeader::read(&mut recv),
    )
    .await
    .context("the peer did not send a mux header within 10 seconds")??;
    let alpn = header.protocol.as_bytes();
    if alpn == mux::MUX_ALPN {
        mux::write_denied(&mut send, "fabric/mux/1 cannot target itself").await?;
        return Ok(());
    }

    let exposure = if matches_reserved_alpn(alpn) {
        None
    } else {
        state.exposures.read().await.get(alpn).cloned()
    };
    if !matches_reserved_alpn(alpn) && exposure.is_none() {
        mux::write_denied(
            &mut send,
            &format!("service {:?} is not exposed", header.protocol),
        )
        .await?;
        return Ok(());
    }

    if alpn != gitremote::GIT_ALPN {
        let service = service_name_for_alpn(alpn);
        if let Err(denied) = state.may(&connection.remote_id(), &service).await {
            mux::write_denied(&mut send, &denied.to_string()).await?;
            return Ok(());
        }
    }
    if state.tunnel_blocked.load(Ordering::SeqCst) && exposure.is_some() {
        mux::write_denied(&mut send, "fabric tunnel blocked").await?;
        return Ok(());
    }

    if alpn != BUILTIN_ECHO_ALPN {
        state
            .peer_connections
            .note_application_activity(connection.remote_id())
            .await;
    }
    mux::write_ready(&mut send).await?;

    if alpn == BUILTIN_ECHO_ALPN {
        state.builtin_echo_hits.fetch_add(1, Ordering::SeqCst);
        tokio::io::copy(&mut recv, &mut send).await?;
        send.finish()?;
    } else if alpn == shell::SHELL_ALPN {
        let peer = connection.remote_id().to_string();
        if state.allow_shell {
            shell::serve_shell_session(&mut recv, &mut send, &peer).await?;
        } else {
            shell::serve_shell_disabled(&mut send).await?;
        }
        send.finish()?;
    } else if alpn == shell::RESUMABLE_SHELL_ALPN {
        let peer = connection.remote_id();
        tunnel::serve_connection(
            connection,
            send,
            recv,
            peer,
            tunnel::ServerTarget::Shell {
                allowed: state.allow_shell,
            },
            state.tunnel_sessions.clone(),
            state.tunnel_drop_rx(),
        )
        .await?;
    } else if alpn == exec::EXEC_ALPN {
        let peer = connection.remote_id().to_string();
        if state.allow_exec {
            exec::serve_exec_session(&mut recv, &mut send, &peer).await?;
        } else {
            exec::serve_exec_disabled(&mut send).await?;
        }
        send.finish()?;
    } else if alpn == gitremote::GIT_ALPN {
        let book = state.peer_book.read().await.clone();
        gitremote::serve_session(
            recv,
            send,
            book,
            connection.remote_id(),
            state.git_sessions.clone(),
        )
        .await?;
    } else if alpn == SYNC_ALPN {
        handle_sync_stream(connection.remote_id(), send, recv, state).await?;
    } else if alpn == crate::sendfile::SEND_FILE_ALPN {
        receive_file_stream(connection.remote_id(), send, recv, state).await?;
    } else if let Some(exposure) = exposure {
        let peer = connection.remote_id();
        tunnel::serve_connection(
            connection,
            send,
            recv,
            peer,
            exposure.to_server_target(),
            state.tunnel_sessions.clone(),
            state.tunnel_drop_rx(),
        )
        .await?;
    }
    Ok(())
}

/// Serve the accepting side of a `fabric/sync` reconcile: run the wire server
/// against the engine's node for the requested sync, then materialize what the
/// peer pushed us to disk.
async fn handle_sync(connection: Connection, state: Arc<DaemonState>) -> Result<()> {
    let peer = connection.remote_id();
    let (send, recv) = connection.accept_bi().await?;
    handle_sync_stream(peer, send, recv, state).await?;
    connection.closed().await;
    Ok(())
}

async fn handle_sync_stream(
    peer: EndpointId,
    send: SendStream,
    recv: RecvStream,
    state: Arc<DaemonState>,
) -> Result<()> {
    let Some(engine) = state.sync_engine() else {
        let mut send = send;
        send.reset(0u32.into())?;
        return Ok(());
    };
    let stream = tokio::io::join(recv, send);
    let resolver_engine = engine.clone();
    let peer = peer.to_string();
    let outcome = sync::wire::run_server(stream, &peer, move |hello| {
        let engine = resolver_engine.clone();
        async move {
            let prepared = engine.prepare_inbound_for_hello(&hello).await?;
            Ok(prepared.map(|prepared| (prepared.node(), prepared)))
        }
    })
    .await;
    match outcome {
        Ok((name, stats, prepared)) => {
            // The serving side's numbers used to stop here, which is how a
            // fallback taken while serving a peer stayed invisible.
            engine.record_inbound(&name, &stats).await;
            if !stats.is_noop() {
                debug!(sync = %name, ?stats, "served sync reconcile");
            }
            // Re-scan changes that landed during the session, then persist and
            // materialize while the inbound operation guard is still held.
            if let Err(error) = engine.complete_inbound(prepared).await {
                debug!(sync = %name, %error, "sync completion failed");
            }
            // AFTER completion, which ends by marking the generation durable.
            // What we just adopted still has to reach every peer that is not
            // the one we adopted it from.
            engine.note_inbound_adoption(&name, &stats).await;
        }
        Err(error) => debug!(%error, "sync serve failed"),
    }
    Ok(())
}

fn accepted_alpns(exposures: &HashMap<Vec<u8>, Exposure>) -> Vec<Vec<u8>> {
    let mut alpns = Vec::with_capacity(exposures.len() + 8);
    alpns.push(mux::MUX_ALPN.to_vec());
    alpns.push(BUILTIN_ECHO_ALPN.to_vec());
    alpns.push(shell::SHELL_ALPN.to_vec());
    alpns.push(shell::RESUMABLE_SHELL_ALPN.to_vec());
    alpns.push(exec::EXEC_ALPN.to_vec());
    alpns.push(SYNC_ALPN.to_vec());
    alpns.push(crate::sendfile::SEND_FILE_ALPN.to_vec());
    alpns.push(gitremote::GIT_ALPN.to_vec());
    alpns.extend(exposures.keys().cloned());
    alpns
}

fn matches_reserved_alpn(alpn: &[u8]) -> bool {
    alpn == mux::MUX_ALPN
        || alpn == crate::sendfile::SEND_FILE_ALPN
        || alpn == BUILTIN_ECHO_ALPN
        || alpn == shell::SHELL_ALPN
        || alpn == shell::RESUMABLE_SHELL_ALPN
        || alpn == exec::EXEC_ALPN
        || alpn == SYNC_ALPN
        || alpn == gitremote::GIT_ALPN
}

/// The iroh-backed sync transport: resolves peers from `peers.toml` and dials the
/// `fabric/sync` ALPN over the daemon's current endpoint. Holds a weak handle to
/// the daemon state to avoid a reference cycle (state -> engine -> transport).
pub struct IrohSyncTransport {
    state: Weak<DaemonState>,
}

impl IrohSyncTransport {
    fn new(state: Weak<DaemonState>) -> Arc<Self> {
        Arc::new(Self { state })
    }
}

impl SyncTransport for IrohSyncTransport {
    async fn peers_for(&self, peers: &SyncPeers) -> ResolvedPeers {
        let Some(state) = self.state.upgrade() else {
            return ResolvedPeers::default();
        };
        let book = state.peer_book.read().await;
        let mut resolved = ResolvedPeers::default();
        match peers {
            SyncPeers::Wildcard(selector) => {
                for peer in book.peers() {
                    resolved.peers.push(peer_ref(peer));
                }
                // "Every trusted peer" on a machine that trusts nobody is
                // nobody, and that is worth a line rather than a clean report.
                if resolved.peers.is_empty() {
                    resolved.unresolved.push(selector.clone());
                }
            }
            SyncPeers::List(selectors) => {
                for selector in selectors {
                    match book.peers().iter().find(|p| {
                        p.id.to_string() == *selector || p.name.as_deref() == Some(selector)
                    }) {
                        Some(peer) => resolved.peers.push(peer_ref(peer)),
                        None => resolved.unresolved.push(selector.clone()),
                    }
                }
            }
        }
        resolved
    }

    async fn reconcile(
        &self,
        peer: PeerRef,
        name: String,
        node: Arc<Mutex<SyncNode>>,
    ) -> Result<sync::Reconciled> {
        let Some(state) = self.state.upgrade() else {
            bail!("daemon is shutting down");
        };
        // OUR OWN policy applies to our own outbound sync, and that is not
        // symmetry for its own sake.
        //
        // Sync is bidirectional. Refusing a peer's incoming reconcile stops
        // nothing on its own, because we still dial THEM and pull the same
        // files. A one-way denial of a two-way protocol denies nothing, which
        // is what `a_peer_denied_sync_makes_the_entry_report_stopped_not_clean`
        // found: the file arrived anyway.
        //
        // So "this peer may not sync with me" also means "I will not sync with
        // it". The refusal carries the same wire phrase as a remote one, so it
        // is reported as `denied` rather than as a network fault.
        let Some(addr) = peer.addr.clone() else {
            bail!("sync peer {} has no address", peer.id);
        };
        // The IDENTITY, from the address, NOT `peer.id`.
        //
        // `PeerRef.id` is a display label: the peer's name when it has one and
        // the id only as a fallback. Keying policy on it silently skipped this
        // check for every named peer, which is exactly the mistake the header
        // in `peers.toml` warns about, made minutes after writing it. The test
        // caught it; reading the code did not.
        if let Err(denied) = state.may(&addr.id, "sync").await {
            bail!("{denied}");
        }
        let stream = state
            .open_peer_stream(
                &addr,
                std::str::from_utf8(SYNC_ALPN).expect("sync ALPN is UTF-8"),
                mux::StreamActivity::Application,
            )
            .await?;
        let joined = tokio::io::join(stream.recv, stream.send);
        sync::wire::run_client(joined, node, &name, &peer.id).await
    }
}

fn peer_ref(peer: &Peer) -> PeerRef {
    let label = peer.name.clone().unwrap_or_else(|| peer.id.to_string());
    let addr = peer
        .addr
        .clone()
        .unwrap_or_else(|| EndpointAddr::new(peer.id));
    PeerRef {
        id: label,
        addr: Some(addr),
    }
}

fn sync_author(id: EndpointId) -> SyncAuthor {
    SyncAuthor(*id.as_bytes())
}

fn current_rss_bytes() -> Option<u64> {
    current_rss_bytes_impl()
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim_process_allocator() -> AllocatorTrimResult {
    let succeeded = unsafe { libc::malloc_trim(0) != 0 };
    AllocatorTrimResult {
        attempted: true,
        succeeded,
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim_process_allocator() -> AllocatorTrimResult {
    AllocatorTrimResult {
        attempted: false,
        succeeded: false,
    }
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

fn sync_accept_is_info_sample(sequence: usize) -> bool {
    sequence.is_multiple_of(SYNC_ACCEPT_INFO_SAMPLE_EVERY)
}

fn log_sync_connection_paths(connection: &Connection) {
    let sequence = SYNC_ACCEPT_LOG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let info_sample = sync_accept_is_info_sample(sequence);
    if !info_sample && !tracing::enabled!(target: VALIDATION_LOG_TARGET, tracing::Level::DEBUG) {
        return;
    }
    let (paths_total, paths_selected, paths_ip, paths_relay, path_local_addrs, path_remote_addrs) =
        connection_path_summary(connection);
    if info_sample {
        info!(
            target: VALIDATION_LOG_TARGET,
            event = "sync_accept",
            remote = %connection.remote_id(),
            paths_total,
            paths_selected,
            paths_ip,
            paths_relay,
            path_local_addrs = %path_local_addrs,
            path_remote_addrs = %path_remote_addrs,
            sample_every = SYNC_ACCEPT_INFO_SAMPLE_EVERY,
            "connection path snapshot"
        );
    } else {
        debug!(
            target: VALIDATION_LOG_TARGET,
            event = "sync_accept",
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
    listener_cancel: CancellationToken,
    daemon_cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
    dial_failures: Arc<FailureBackoff>,
    dial_slots: Arc<Semaphore>,
    peer_connections: Arc<mux::PeerConnections>,
    _lease: DialListenerLease,
    gauge: Arc<tunnel::ClientAttachGauge>,
    recorder: ConnectionRecorder,
) {
    let backoff_key = BackoffKey::dial(&peer, &alpn);
    loop {
        tokio::select! {
            biased;
            _ = listener_cancel.cancelled() => break,
            _ = daemon_cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((local, _)) = accepted else {
                    break;
                };
                let permit = tokio::select! {
                    biased;
                    _ = listener_cancel.cancelled() => break,
                    _ = daemon_cancel.cancelled() => break,
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
                let cancel = daemon_cancel.clone();
                let drop_rx = drop_rx.clone();
                let dial_failures = dial_failures.clone();
                let backoff_key = backoff_key.clone();
                let peer_connections = peer_connections.clone();
                let notices = Some(generic_dial_notices(
                    peer.clone(),
                    String::from_utf8_lossy(&alpn).to_string(),
                    gauge.clone(),
                    recorder.clone(),
                ));
                tokio::spawn(async move {
                    let _permit = permit;
                    if !dial_failures.wait(&backoff_key, &cancel).await {
                        return;
                    }
                    match
                        tunnel::run_client_connection(
                            local,
                            endpoint_rx,
                            peer_connections,
                            home,
                            peer,
                            alpn,
                            cancel,
                            drop_rx,
                            notices,
                        )
                            .await
                    {
                        Ok(()) => dial_failures.record_success(&backoff_key).await,
                        Err(error) => {
                            dial_failures
                                .record_failure(&backoff_key, "dial socket connection failed", &error)
                                .await;
                        }
                    }
                });
            }
        }
    }
}

async fn run_shell_dial_socket(
    listener: UnixListener,
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
    home: FabricHome,
    peer: String,
    peer_addr: EndpointAddr,
    listener_cancel: CancellationToken,
    daemon_cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
    dial_failures: Arc<FailureBackoff>,
    dial_slots: Arc<Semaphore>,
    peer_connections: Arc<mux::PeerConnections>,
    _lease: DialListenerLease,
    gauge: Arc<tunnel::ClientAttachGauge>,
    recorder: ConnectionRecorder,
) {
    let backoff_key = BackoffKey::dial(&peer, shell::RESUMABLE_SHELL_ALPN);
    loop {
        tokio::select! {
            biased;
            _ = listener_cancel.cancelled() => break,
            _ = daemon_cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((local, _)) = accepted else {
                    break;
                };
                let permit = tokio::select! {
                    biased;
                    _ = listener_cancel.cancelled() => break,
                    _ = daemon_cancel.cancelled() => break,
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
                let peer_addr = peer_addr.clone();
                let cancel = daemon_cancel.clone();
                let drop_rx = drop_rx.clone();
                let dial_failures = dial_failures.clone();
                let backoff_key = backoff_key.clone();
                let peer_connections = peer_connections.clone();
                let gauge = gauge.clone();
                let recorder = recorder.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if !dial_failures.wait(&backoff_key, &cancel).await {
                        return;
                    }
                    match handle_shell_dial_socket_connection(
                        local,
                        endpoint_rx,
                        peer_connections,
                        home,
                        peer,
                        peer_addr,
                        cancel,
                        drop_rx,
                        gauge,
                        recorder,
                    )
                    .await
                    {
                        Ok(()) => dial_failures.record_success(&backoff_key).await,
                        Err(error) => {
                            dial_failures
                                .record_failure(&backoff_key, "shell dial socket connection failed", &error)
                                .await;
                        }
                    }
                });
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_shell_dial_socket_connection(
    mut local: UnixStream,
    mut endpoint_rx: watch::Receiver<CurrentEndpoint>,
    peer_connections: Arc<mux::PeerConnections>,
    home: FabricHome,
    peer: String,
    peer_addr: EndpointAddr,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
    gauge: Arc<tunnel::ClientAttachGauge>,
    recorder: ConnectionRecorder,
) -> Result<()> {
    let mut attempt = 0usize;
    loop {
        let current_peer_addr = PeerBook::load(&home)
            .and_then(|book| book.resolve(&peer))
            .unwrap_or_else(|_| peer_addr.clone());
        let endpoint = {
            let current = endpoint_rx.borrow();
            current.clone()
        };
        let generation = endpoint.generation;
        let connected = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            changed = endpoint_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                continue;
            }
            connected = peer_connections.open_stream(
                &endpoint.endpoint,
                endpoint.generation,
                &current_peer_addr,
                std::str::from_utf8(shell::RESUMABLE_SHELL_ALPN)
                    .expect("shell ALPN is UTF-8"),
                mux::StreamActivity::Application,
            ) => connected,
        };
        match connected {
            Ok(connection) => {
                // Protocol selection ends here. Once shell/1 has attached, its
                // reconnect loop must remain shell/1 so it resumes this exact
                // remote PTY rather than starting a legacy replacement.
                let notices =
                    shell_client_notices(peer.clone(), generation, gauge.clone(), recorder.clone());
                return tunnel::run_client_connection_with_initial(
                    local,
                    endpoint_rx,
                    peer_connections,
                    home,
                    peer,
                    shell::RESUMABLE_SHELL_ALPN.to_vec(),
                    cancel,
                    drop_rx,
                    Some(notices),
                    connection,
                )
                .await;
            }
            Err(error) => {
                if tunnel::is_permanent_failure(&error) {
                    return Err(error.context("peer refused resumable shell"));
                }
                if shell_resumable_alpn_unsupported(&error) {
                    write_shell_protocol_status(
                        &mut local,
                        "peer does not support resumable shell; using legacy shell/0",
                    )
                    .await?;
                    return run_legacy_shell_after_selection(
                        local,
                        endpoint_rx,
                        home,
                        peer,
                        current_peer_addr,
                        cancel,
                    )
                    .await;
                }
                attempt = attempt.saturating_add(1);
                let delay = shell_protocol_probe_delay(attempt);
                write_shell_protocol_status(
                    &mut local,
                    &format!(
                        "connection unavailable ({error:#}); probing remote shell protocol again in {:.1}s",
                        delay.as_secs_f32()
                    ),
                )
                .await?;
                if !wait_for_shell_protocol_retry(delay, &cancel, &mut endpoint_rx).await {
                    return Ok(());
                }
            }
        }
    }
}

async fn run_legacy_shell_after_selection(
    mut local: UnixStream,
    mut endpoint_rx: watch::Receiver<CurrentEndpoint>,
    home: FabricHome,
    peer: String,
    peer_addr: EndpointAddr,
    cancel: CancellationToken,
) -> Result<()> {
    let mut attempt = 0usize;
    loop {
        let current_peer_addr = PeerBook::load(&home)
            .and_then(|book| book.resolve(&peer))
            .unwrap_or_else(|_| peer_addr.clone());
        let endpoint = endpoint_rx.borrow().endpoint.clone();
        let connected = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            changed = endpoint_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                continue;
            }
            connected = endpoint.connect(current_peer_addr, shell::SHELL_ALPN) => connected,
        };
        match connected {
            Ok(connection) => {
                let (send, recv) = connection.open_bi().await?;
                return pipe_unix_iroh(local, send, recv).await;
            }
            Err(error) => {
                let error = anyhow::Error::new(error);
                if shell_resumable_alpn_unsupported(&error) {
                    return Err(error.context("peer supports neither shell/1 nor shell/0"));
                }
                attempt = attempt.saturating_add(1);
                let delay = shell_protocol_probe_delay(attempt);
                write_shell_protocol_status(
                    &mut local,
                    &format!(
                        "legacy shell unavailable ({error:#}); retrying before session start in {:.1}s",
                        delay.as_secs_f32()
                    ),
                )
                .await?;
                if !wait_for_shell_protocol_retry(delay, &cancel, &mut endpoint_rx).await {
                    return Ok(());
                }
            }
        }
    }
}

async fn write_shell_protocol_status(local: &mut UnixStream, message: &str) -> Result<()> {
    local
        .write_all(&shell::encode_server_status(message)?)
        .await?;
    Ok(())
}

fn shell_protocol_probe_delay(attempt: usize) -> Duration {
    const STEPS_MS: &[u64] = &[100, 250, 500, 1_000, 2_000, 5_000, 10_000, 15_000];
    Duration::from_millis(STEPS_MS[attempt.saturating_sub(1).min(STEPS_MS.len() - 1)])
}

async fn wait_for_shell_protocol_retry(
    delay: Duration,
    cancel: &CancellationToken,
    endpoint_rx: &mut watch::Receiver<CurrentEndpoint>,
) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        changed = endpoint_rx.changed() => changed.is_ok(),
        _ = tokio::time::sleep(delay) => true,
    }
}

fn shell_resumable_alpn_unsupported(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string().to_ascii_lowercase();
        message.contains("peer doesn't support any known protocol")
            || message.contains("no application protocol")
    })
}

/// Turns a connection notice into a durable count.
///
/// The notices are synchronous callbacks, so this holds only what it can read
/// without awaiting: the counter store and the last path the liveness probe
/// saw. Both are cheap locks over small maps.
#[derive(Clone)]
pub(crate) struct ConnectionRecorder {
    telemetry: Arc<TelemetryStore>,
    last_probe_transport: Arc<StdRwLock<HashMap<String, String>>>,
}

impl ConnectionRecorder {
    fn new(
        telemetry: Arc<TelemetryStore>,
        last_probe_transport: Arc<StdRwLock<HashMap<String, String>>>,
    ) -> Self {
        Self {
            telemetry,
            last_probe_transport,
        }
    }

    /// The path this peer was last seen on, or `None` when it has never been
    /// probed. `None` is recorded as unknown rather than guessed.
    fn path_for(&self, peer: &str) -> Option<String> {
        self.last_probe_transport
            .read()
            .ok()
            .and_then(|seen| seen.get(peer).cloned())
    }

    fn record(&self, peer: &str, event: &tunnel::ClientConnectionEvent) {
        let path = self.path_for(peer);
        match event {
            tunnel::ClientConnectionEvent::Reconnecting { attempt, .. } => {
                self.telemetry
                    .record_loss(peer, path.as_deref(), *attempt, Instant::now());
            }
            tunnel::ClientConnectionEvent::Resumed => {
                self.telemetry
                    .record_resume(peer, path.as_deref(), Instant::now());
            }
            tunnel::ClientConnectionEvent::Failed { .. } => {
                self.telemetry.record_resume_failure(peer);
            }
        }
    }
}

/// Shell connection notices, rendered twice on purpose.
///
/// The shell client sees a status frame in its terminal, and the daemon log gets
/// the same event in a line an operator can read. Before this, a dropped session
/// left one failure line in the service log and nothing about the attempt to get
/// it back, so "did my shell recover" was unanswerable from the log. Each line
/// names the peer and the endpoint generation, which is the route context that
/// distinguishes a peer roaming from this endpoint being rebuilt underneath it.
/// Connection notices for a generic dial: logged, never written to the stream.
///
/// A generic dial carries raw bytes for somebody else's protocol, so the status
/// frames the shell path renders into its terminal would corrupt it. The encoder
/// therefore returns None for every event and only the log records it. Without
/// this, a pty-remote or st sync tunnel lost and resumed its transport with no
/// trace anywhere, which made "did my tunnel drop" unanswerable for exactly the
/// consumers most likely to be long-lived.
fn generic_dial_notices(
    peer: String,
    protocol: String,
    gauge: Arc<tunnel::ClientAttachGauge>,
    recorder: ConnectionRecorder,
) -> tunnel::ClientConnectionNotices {
    tunnel::ClientConnectionNotices::new(move |event| {
        recorder.record(&peer, event);
        match event {
            tunnel::ClientConnectionEvent::Reconnecting {
                attempt,
                delay,
                error,
            } => warn!(
                target: VALIDATION_LOG_TARGET,
                event = "dial_session_reconnecting",
                peer = %peer,
                protocol = %protocol,
                attempt,
                delay_ms = delay.as_millis() as u64,
                error = %error,
                "generic dial lost its transport; reconnecting"
            ),
            tunnel::ClientConnectionEvent::Resumed => info!(
                target: VALIDATION_LOG_TARGET,
                event = "dial_session_resumed",
                peer = %peer,
                protocol = %protocol,
                "generic dial resumed after reconnect"
            ),
            tunnel::ClientConnectionEvent::Failed { error } => warn!(
                target: VALIDATION_LOG_TARGET,
                event = "dial_session_resume_failed",
                peer = %peer,
                protocol = %protocol,
                error = %error,
                "generic dial session ended and will not retry"
            ),
        }
        // Never any bytes: this stream belongs to another protocol.
        None
    })
    .with_gauge(gauge)
}

fn shell_client_notices(
    peer: String,
    generation: u64,
    gauge: Arc<tunnel::ClientAttachGauge>,
    recorder: ConnectionRecorder,
) -> tunnel::ClientConnectionNotices {
    tunnel::ClientConnectionNotices::new(move |event| {
        recorder.record(&peer, event);
        let encoded = match event {
            tunnel::ClientConnectionEvent::Reconnecting {
                attempt,
                delay,
                error,
            } => {
                warn!(
                    target: VALIDATION_LOG_TARGET,
                    event = "shell_session_reconnecting",
                    peer = %peer,
                    generation,
                    attempt,
                    delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "shell session lost its transport; reconnecting"
                );
                eprintln!(
                    "fabric: shell session to {peer:?} lost connection ({error}); reconnect attempt {attempt} in {:.1}s (endpoint generation {generation})",
                    delay.as_secs_f32()
                );
                shell::encode_server_status(&format!(
                    "connection lost ({error}); reconnecting attempt {attempt} in {:.1}s",
                    delay.as_secs_f32()
                ))
            }
            tunnel::ClientConnectionEvent::Resumed => {
                info!(
                    target: VALIDATION_LOG_TARGET,
                    event = "shell_session_resumed",
                    peer = %peer,
                    generation,
                    "shell session resumed after reconnect"
                );
                eprintln!(
                    "fabric: shell session to {peer:?} resumed (endpoint generation {generation})"
                );
                shell::encode_server_status("connection restored; remote shell session resumed")
            }
            tunnel::ClientConnectionEvent::Failed { error } => {
                warn!(
                    target: VALIDATION_LOG_TARGET,
                    event = "shell_session_resume_failed",
                    peer = %peer,
                    generation,
                    error = %error,
                    "shell session could not resume"
                );
                eprintln!(
                    "fabric: shell session to {peer:?} could NOT resume: {error} (endpoint generation {generation})"
                );
                shell::encode_server_error(&format!("remote shell could not resume: {error}"))
            }
        };
        encoded.ok()
    })
    .with_gauge(gauge)
}

#[allow(clippy::too_many_arguments)]
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
    peer_connections: Arc<mux::PeerConnections>,
    gauge: Arc<tunnel::ClientAttachGauge>,
    recorder: ConnectionRecorder,
) {
    let backoff_key = BackoffKey::dial(&peer, &alpn);
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
                let backoff_key = backoff_key.clone();
                let peer_connections = peer_connections.clone();
                let notices = Some(generic_dial_notices(
                    peer.clone(),
                    String::from_utf8_lossy(&alpn).to_string(),
                    gauge.clone(),
                    recorder.clone(),
                ));
                tokio::spawn(async move {
                    let _permit = permit;
                    if !dial_failures.wait(&backoff_key, &cancel).await {
                        return;
                    }
                    match
                        tunnel::run_client_tcp_connection(
                            local,
                            endpoint_rx,
                            peer_connections,
                            home,
                            peer,
                            alpn,
                            cancel,
                            drop_rx,
                            notices,
                        )
                            .await
                    {
                        Ok(()) => dial_failures.record_success(&backoff_key).await,
                        Err(error) => {
                            dial_failures
                                .record_failure(&backoff_key, "dial tcp connection failed", &error)
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
    listener_cancel: CancellationToken,
    daemon_cancel: CancellationToken,
    dial_failures: Arc<FailureBackoff>,
    dial_slots: Arc<Semaphore>,
    peer_connections: Arc<mux::PeerConnections>,
    _lease: DialListenerLease,
) {
    let backoff_key = BackoffKey::dial(&peer_addr.id.to_string(), &alpn);
    loop {
        tokio::select! {
            biased;
            _ = listener_cancel.cancelled() => break,
            _ = daemon_cancel.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((local, _)) = accepted else {
                    break;
                };
                let permit = tokio::select! {
                    biased;
                    _ = listener_cancel.cancelled() => break,
                    _ = daemon_cancel.cancelled() => break,
                    permit = dial_slots.clone().acquire_owned() => {
                        let Ok(permit) = permit else {
                            break;
                        };
                        permit
                    }
                };
                let endpoint = endpoint_rx.borrow().clone();
                let peer_addr = peer_addr.clone();
                let alpn = alpn.clone();
                let cancel = daemon_cancel.clone();
                let dial_failures = dial_failures.clone();
                let backoff_key = backoff_key.clone();
                let peer_connections = peer_connections.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if !dial_failures.wait(&backoff_key, &cancel).await {
                        return;
                    }
                    match handle_raw_dial_socket_connection(
                        local,
                        endpoint,
                        peer_addr,
                        alpn,
                        peer_connections,
                    )
                    .await
                    {
                        Ok(()) => dial_failures.record_success(&backoff_key).await,
                        Err(error) => {
                            dial_failures
                                .record_failure(&backoff_key, "dial socket connection failed", &error)
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
    endpoint: CurrentEndpoint,
    peer_addr: EndpointAddr,
    alpn: Vec<u8>,
    peer_connections: Arc<mux::PeerConnections>,
) -> Result<()> {
    let protocol = std::str::from_utf8(&alpn).context("protocol is not UTF-8")?;
    let stream = peer_connections
        .open_stream(
            &endpoint.endpoint,
            endpoint.generation,
            &peer_addr,
            protocol,
            mux::StreamActivity::Application,
        )
        .await?;
    pipe_unix_iroh(local, stream.send, stream.recv).await?;
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

    #[tokio::test]
    async fn endpoint_close_wait_has_a_hard_deadline() {
        let bound = Duration::from_millis(20);
        let started = Instant::now();
        let completed = wait_for_close(std::future::pending(), bound).await;

        assert!(!completed, "a stuck close must report its timeout");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a stuck close escaped its {bound:?} deadline"
        );
        assert!(
            wait_for_close(std::future::ready(()), bound).await,
            "a completed close must not report a timeout"
        );
    }

    /// The default validation filter must PASS the level our diagnostics use.
    ///
    /// This is the test I declined to write for #69, on the grounds that
    /// asserting a log line fires would mirror the change. That judgement was
    /// wrong and it cost a merge, a release and a deploy: the per-peer
    /// reconcile diagnostic went out at `debug!`, the default filter is
    /// `fabric=info`, and it emitted NOTHING on the live daemon. Three passes
    /// ran and produced zero lines.
    ///
    /// It is not a mirror. It pins the INTERACTION between two things written
    /// far apart: the level a diagnostic chooses, and the filter the daemon
    /// actually runs. Either can move without the other, and neither file
    /// mentions the other.
    #[test]
    fn the_default_validation_filter_passes_info_and_drops_debug() {
        use std::io;
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct Buffer(Arc<Mutex<Vec<u8>>>);
        impl io::Write for Buffer {
            fn write(&mut self, data: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
            type Writer = Buffer;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        // Assert the DEFAULT the daemon ships with, not whatever the shell set.
        let filter = EnvFilter::new("fabric=info,iroh=warn,noq=warn,netwatch=warn");
        let buffer = Buffer(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(buffer.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: VALIDATION_LOG_TARGET, event = "probe_info", "info probe");
            tracing::debug!(target: VALIDATION_LOG_TARGET, event = "probe_debug", "debug probe");
        });

        let written = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(
            written.contains("probe_info"),
            "an INFO diagnostic on the validation target must reach the log, got: {written}"
        );
        assert!(
            !written.contains("probe_debug"),
            "DEBUG is dropped by the default filter, so a debug! diagnostic is silent \
             rather than quiet. That is what shipped in #69. Got: {written}"
        );
    }

    #[test]
    fn sync_accept_info_logging_is_bounded() {
        let samples = (0..10_000)
            .filter(|sequence| sync_accept_is_info_sample(*sequence))
            .count();
        assert_eq!(samples, 79);
        assert!(sync_accept_is_info_sample(0));
        assert!(sync_accept_is_info_sample(128));
        assert!(!sync_accept_is_info_sample(127));
        assert!(!sync_accept_is_info_sample(129));
    }

    #[test]
    fn peer_recovery_backoff_scales_and_caps() {
        let initial = Duration::from_secs(30);
        let max = Duration::from_secs(600);
        assert_eq!(peer_recovery_backoff(0, initial, max), Duration::ZERO);
        assert_eq!(
            peer_recovery_backoff(1, initial, max),
            Duration::from_secs(30)
        );
        assert_eq!(
            peer_recovery_backoff(2, initial, max),
            Duration::from_secs(60)
        );
        assert_eq!(
            peer_recovery_backoff(3, initial, max),
            Duration::from_secs(120)
        );
        assert_eq!(
            peer_recovery_backoff(4, initial, max),
            Duration::from_secs(240)
        );
        assert_eq!(peer_recovery_backoff(10, initial, max), max); // saturated + capped
    }

    // Fault-injection: feed a scripted fail/ok probe sequence into the pure
    // recovery decision core and assert it fires at the failure threshold, backs
    // off between repeated attempts while the peer stays down, and fully resets
    // once the peer answers again.
    #[test]
    fn peer_health_tracker_fires_at_threshold_then_backs_off_then_resets() {
        use iroh::SecretKey;
        let peer = SecretKey::generate().public();
        let t0 = Instant::now();
        let mut tracker =
            PeerHealthTracker::new(3, Duration::from_secs(30), Duration::from_secs(600));

        // Below threshold: no recovery yet.
        assert_eq!(tracker.on_probe(peer, false, t0), PeerHealthAction::None);
        assert_eq!(tracker.on_probe(peer, false, t0), PeerHealthAction::None);
        // Threshold (3 consecutive failures) reached: fire attempt 1.
        assert_eq!(
            tracker.on_probe(peer, false, t0),
            PeerHealthAction::Recover { attempt: 1 }
        );
        // Still failing but inside the 30s backoff window: must NOT re-fire (no thrash).
        assert_eq!(
            tracker.on_probe(peer, false, t0 + Duration::from_secs(5)),
            PeerHealthAction::None
        );
        assert_eq!(
            tracker.on_probe(peer, false, t0 + Duration::from_secs(29)),
            PeerHealthAction::None
        );
        // Backoff elapsed, still failing: fire attempt 2.
        assert_eq!(
            tracker.on_probe(peer, false, t0 + Duration::from_secs(31)),
            PeerHealthAction::Recover { attempt: 2 }
        );
        // A reachable probe fully resets the peer.
        assert_eq!(
            tracker.on_probe(peer, true, t0 + Duration::from_secs(40)),
            PeerHealthAction::None
        );
        // Fresh failures must re-climb the threshold from zero (attempt back to 1).
        assert_eq!(
            tracker.on_probe(peer, false, t0 + Duration::from_secs(50)),
            PeerHealthAction::None
        );
        assert_eq!(
            tracker.on_probe(peer, false, t0 + Duration::from_secs(50)),
            PeerHealthAction::None
        );
        assert_eq!(
            tracker.on_probe(peer, false, t0 + Duration::from_secs(50)),
            PeerHealthAction::Recover { attempt: 1 }
        );
    }

    // A failing peer must never trip recovery for a different, healthy peer.
    #[test]
    fn peer_health_tracker_isolates_peers() {
        use iroh::SecretKey;
        let a = SecretKey::generate().public();
        let b = SecretKey::generate().public();
        let t0 = Instant::now();
        let mut tracker =
            PeerHealthTracker::new(2, Duration::from_secs(30), Duration::from_secs(600));

        assert_eq!(tracker.on_probe(a, false, t0), PeerHealthAction::None);
        assert_eq!(tracker.on_probe(b, true, t0), PeerHealthAction::None);
        // A reaches its threshold and recovers; B, interleaved, is unaffected.
        assert_eq!(
            tracker.on_probe(a, false, t0),
            PeerHealthAction::Recover { attempt: 1 }
        );
        assert_eq!(tracker.on_probe(b, true, t0), PeerHealthAction::None);
        assert_eq!(tracker.on_probe(b, false, t0), PeerHealthAction::None);
        assert_eq!(
            tracker.on_probe(b, false, t0),
            PeerHealthAction::Recover { attempt: 1 }
        );
    }

    #[test]
    fn server_session_limit_options_override_config() {
        let config: FabricConfig = toml::from_str(
            r#"
            [server_sessions]
            max_total = 64
            max_per_peer = 16
            detached_ttl_secs = 120
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

    /// One unreachable peer must not delay dials to a healthy one.
    ///
    /// The dial backoff used to be a single shared record for every peer and
    /// every ALPN. Measured on that version: after six failures dialling one
    /// peer, an unrelated healthy dial waited 3.2 seconds, on its way to the 15
    /// second ceiling. The coupling
    /// also ran the other way — one success on any peer wiped the absent peer's
    /// streak entirely, so the backoff stopped protecting anything.
    // Real time, deliberately: this backoff measures with std::time::Instant, so
    // a paused tokio clock would not move it and would only make the waits spin.
    #[tokio::test]
    async fn one_absent_peer_does_not_delay_or_reset_another() {
        let backoff = FailureBackoff::new(
            DIAL_FAILURE_INITIAL_BACKOFF,
            DIAL_FAILURE_MAX_BACKOFF,
            FAILURE_LOG_INTERVAL,
        );
        let cancel = CancellationToken::new();
        let absent = BackoffKey::dial("peer-a", shell::RESUMABLE_SHELL_ALPN);
        let healthy = BackoffKey::dial("peer-b", shell::RESUMABLE_SHELL_ALPN);

        for _ in 0..6 {
            backoff
                .record_failure(&absent, "dial to peer-a", &"peer-a is unreachable")
                .await;
        }

        // The healthy peer is not charged for it. Asserted on the record set
        // rather than on a stopwatch: under a paused clock a wait of zero still
        // reads as a few microseconds, and "no record" is the real claim.
        assert!(
            !backoff.states.lock().await.contains_key(&healthy),
            "a healthy peer must not acquire a backoff record from another peer's failures"
        );
        let started = Instant::now();
        assert!(backoff.wait(&healthy, &cancel).await);
        assert!(
            started.elapsed() < DIAL_FAILURE_INITIAL_BACKOFF,
            "a healthy peer must not wait behind another peer's failures"
        );

        // POSITIVE CONTROL: the absent peer really is still backed off, so the
        // check above is about attribution and not about a backoff that stopped
        // working altogether.
        assert_eq!(
            backoff
                .states
                .lock()
                .await
                .get(&absent)
                .map(|state| state.consecutive_failures),
            Some(6),
            "the absent peer must carry its own streak"
        );
        let started = Instant::now();
        assert!(backoff.wait(&absent, &cancel).await);
        assert!(
            started.elapsed() >= DIAL_FAILURE_INITIAL_BACKOFF,
            "the absent peer must still be serving its own backoff"
        );

        // And success on the healthy peer does not wipe the absent peer's streak.
        backoff.record_success(&healthy).await;
        backoff
            .record_failure(&absent, "dial to peer-a", &"peer-a is still unreachable")
            .await;
        let started = Instant::now();
        assert!(backoff.wait(&absent, &cancel).await);
        assert!(
            started.elapsed() > DIAL_FAILURE_INITIAL_BACKOFF,
            "an unrelated success must not reset another peer's escalation"
        );
    }

    /// The same ALPN distinction: a peer that does not offer one protocol must
    /// not make its other protocols look unreachable.
    // Real time, deliberately: this backoff measures with std::time::Instant, so
    // a paused tokio clock would not move it and would only make the waits spin.
    #[tokio::test]
    async fn one_refused_protocol_does_not_delay_another_on_the_same_peer() {
        let backoff = FailureBackoff::new(
            DIAL_FAILURE_INITIAL_BACKOFF,
            DIAL_FAILURE_MAX_BACKOFF,
            FAILURE_LOG_INTERVAL,
        );
        let cancel = CancellationToken::new();
        let exec = BackoffKey::dial("peer-a", exec::EXEC_ALPN);
        let shell = BackoffKey::dial("peer-a", shell::RESUMABLE_SHELL_ALPN);

        for _ in 0..4 {
            backoff
                .record_failure(&exec, "dial exec", &"exec is not allowed on peer-a")
                .await;
        }

        assert!(
            !backoff.states.lock().await.contains_key(&shell),
            "a refused exec must not create a backoff record for the shell"
        );
        let started = Instant::now();
        assert!(backoff.wait(&shell, &cancel).await);
        assert!(
            started.elapsed() < DIAL_FAILURE_INITIAL_BACKOFF,
            "a refused exec must not delay a shell to the same peer"
        );
    }

    /// Records must not accumulate for peers that are fine.
    // Real time, deliberately: this backoff measures with std::time::Instant, so
    // a paused tokio clock would not move it and would only make the waits spin.
    #[tokio::test]
    async fn recovered_and_idle_backoff_records_are_dropped() {
        let backoff = FailureBackoff::new(
            DIAL_FAILURE_INITIAL_BACKOFF,
            DIAL_FAILURE_MAX_BACKOFF,
            FAILURE_LOG_INTERVAL,
        );
        let cancel = CancellationToken::new();
        let key = BackoffKey::dial("peer-a", shell::RESUMABLE_SHELL_ALPN);

        backoff.record_failure(&key, "dial", &"boom").await;
        assert_eq!(backoff.states.lock().await.len(), 1);

        // A success drops its own record.
        backoff.record_success(&key).await;
        assert_eq!(backoff.states.lock().await.len(), 0);

        // A record whose owner served its window and then stopped dialling ages
        // out, so a stale streak cannot be charged to some later attempt.
        backoff.record_failure(&key, "dial", &"boom").await;
        assert!(backoff.wait(&key, &cancel).await);
        assert_eq!(
            backoff.states.lock().await.len(),
            1,
            "the record must survive its own backoff window"
        );
        tokio::time::sleep(DIAL_FAILURE_INITIAL_BACKOFF * 2).await;
        assert!(backoff.wait(&key, &cancel).await);
        assert_eq!(
            backoff.states.lock().await.len(),
            0,
            "an aged-out record must not be retained"
        );
    }

    #[test]
    fn live_sessions_block_an_endpoint_recycle() {
        let idle = tunnel::ServerSessionStats::default();
        assert_eq!(recycle_blocked_by_sessions(&idle, 0), None);

        let with_session = tunnel::ServerSessionStats {
            active_sessions: 1,
            ..Default::default()
        };
        assert_eq!(recycle_blocked_by_sessions(&with_session, 0), Some(1));

        let with_attach = tunnel::ServerSessionStats {
            active_attaches: 2,
            ..Default::default()
        };
        assert_eq!(recycle_blocked_by_sessions(&with_attach, 0), Some(2));

        let both = tunnel::ServerSessionStats {
            active_sessions: 1,
            active_attaches: 3,
            ..Default::default()
        };
        assert_eq!(recycle_blocked_by_sessions(&both, 0), Some(4));

        // A detached-but-resumable session is not an attached one; it must not
        // pin the endpoint forever.
        let detached_only = tunnel::ServerSessionStats {
            detached_sessions: 5,
            ..Default::default()
        };
        assert_eq!(recycle_blocked_by_sessions(&detached_only, 0), None);

        // An OUTBOUND attach counts too. Nothing we serve is attached here, which
        // is exactly the state that used to read as "idle" while a user held a
        // working shell out to a peer.
        assert_eq!(recycle_blocked_by_sessions(&idle, 1), Some(1));
        assert_eq!(recycle_blocked_by_sessions(&detached_only, 2), Some(2));
        assert_eq!(recycle_blocked_by_sessions(&both, 2), Some(6));
    }

    #[tokio::test]
    async fn one_absent_peer_does_not_recycle_while_others_are_healthy() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let node = FabricNode::start(FabricHome::new(temp.path())).await?;
        let state = node.state();
        let before = state.endpoint_handle().generation;

        // Attempts exhausted, but another peer answered this round: the endpoint is
        // demonstrably working, so the roaming peer must not take it down.
        state
            .recover_unreachable_peer("bluey", PEER_HEALTH_ATTEMPTS_BEFORE_RECYCLE, 1)
            .await;
        assert_eq!(
            state.endpoint_handle().generation,
            before,
            "a healthy peer elsewhere must protect the endpoint"
        );

        // Below the escalation threshold with nobody else healthy: still no recycle.
        state.recover_unreachable_peer("bluey", 1, 0).await;
        assert_eq!(state.endpoint_handle().generation, before);

        node.shutdown().await?;
        Ok(())
    }

    /// A probe answers a question about a peer. These pin the four answers and,
    /// just as importantly, that asking never changes local state.
    #[tokio::test]
    async fn probe_reports_supported_for_a_served_protocol() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (server, client, server_home, client_home) = probe_pair(temp.path()).await?;
        let _ = (&server_home, &client_home);

        let probe = client
            .state()
            .probe_service("server", "fabric/echo/0", Duration::from_secs(5))
            .await?;
        assert_eq!(probe.outcome, ProbeOutcome::Supported, "{probe:?}");
        assert!(probe.round_trip.is_some());
        assert!(probe.error.is_none());

        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn probe_reports_unsupported_for_a_protocol_the_peer_does_not_serve() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let (server, client, server_home, client_home) = probe_pair(temp.path()).await?;
        let _ = (&server_home, &client_home);

        // The literal PTY uses today, so this test also pins that exact string.
        let probe = client
            .state()
            .probe_service("server", "pty-remote", Duration::from_secs(5))
            .await?;
        assert_eq!(probe.outcome, ProbeOutcome::Unsupported, "{probe:?}");
        assert!(probe.round_trip.is_none());

        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn probe_reports_timeout_when_the_deadline_is_shorter_than_the_attempt() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let home = FabricHome::new(temp.path());
        let client = FabricNode::start(home.clone()).await?;
        let absent = unroutable_peer(&home, &client, "absent").await?;

        let probe = client
            .state()
            .probe_service(&absent, "fabric/echo/0", Duration::from_millis(50))
            .await?;
        assert_eq!(probe.outcome, ProbeOutcome::Timeout, "{probe:?}");
        assert!(probe.error.is_some());

        client.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn probe_creates_no_listener_and_never_touches_dial_backoff() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let home = FabricHome::new(temp.path());
        let client = FabricNode::start(home.clone()).await?;
        let state = client.state();
        let absent = unroutable_peer(&home, &client, "absent").await?;

        let listeners_before = state.active_dial_listeners.load(Ordering::SeqCst);
        let records_before = state.dial_failures.states.lock().await.len();

        // A failing probe is the interesting case: a dial here would have
        // recorded a failure and parked every other peer's next attempt.
        let probe = state
            .probe_service(&absent, "fabric/echo/0", Duration::from_millis(80))
            .await?;
        assert_ne!(probe.outcome, ProbeOutcome::Supported);

        assert_eq!(
            state.active_dial_listeners.load(Ordering::SeqCst),
            listeners_before,
            "a probe must not install a dial listener"
        );
        let records_after = state.dial_failures.states.lock().await.len();
        assert_eq!(
            records_after, records_before,
            "a probe must not record a dial failure for anyone"
        );
        assert_eq!(
            records_after, 0,
            "a probe must leave no backoff record at all, so it cannot park any dial"
        );

        client.shutdown().await?;
        Ok(())
    }

    #[test]
    fn no_fixed_rss_threshold_exists_in_the_daemon() {
        // Nathan's rule: Fabric must not enforce a fixed RSS recycle or kill limit
        // before healthy working sets are measured. This pins the absence of one:
        // the source may observe and report RSS, but must not act on a constant.
        // Needles are split so this assertion does not match itself.
        let source = include_str!("daemon.rs");
        for needle in [
            concat!("ENDPOINT_RSS_RECYCLE", "_THRESHOLD_BYTES"),
            concat!("rss threshold", " exceeded"),
            concat!("rss_exceeds", "_recycle_threshold"),
        ] {
            assert!(!source.contains(needle), "{needle} is back in the daemon");
        }
    }

    #[tokio::test]
    async fn high_rss_reports_growth_and_never_recycles_the_endpoint() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let node = FabricNode::start(FabricHome::new(temp.path())).await?;
        let state = node.state();
        let initial = state.endpoint_handle();
        tokio::time::timeout(ENDPOINT_HEALTH_TIMEOUT, initial.endpoint.online()).await?;

        // Ten gigabytes, far above any threshold this daemon used to enforce.
        let observer = tokio::spawn(run_endpoint_rss_observe_loop_with_sampler(
            state.clone(),
            Duration::from_millis(20),
            ENDPOINT_RSS_REPORT_STEP_BYTES,
            Arc::new(|| Some(10 * 1024 * 1024 * 1024)),
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(
            state.endpoint_handle().generation,
            initial.generation,
            "sustained high RSS must not recycle the endpoint"
        );
        state.cancel.cancel();
        observer.await??;
        node.shutdown().await?;
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

        let key = BackoffKey::dial("peer", b"fabric/echo/0");
        backoff.record_failure(&key, "test failure", &"boom").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), backoff.wait(&key, &cancel))
                .await
                .is_err(),
            "failed work should be parked instead of immediately retried"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(250), backoff.wait(&key, &cancel))
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

        let key = BackoffKey::dial("peer", b"fabric/echo/0");
        backoff.record_failure(&key, "test failure", &"boom").await;
        backoff.record_success(&key).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), backoff.wait(&key, &cancel))
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

        let key = BackoffKey::dial("peer", b"fabric/echo/0");
        backoff.record_failure(&key, "test failure", &"boom").await;
        cancel.cancel();
        assert!(!backoff.wait(&key, &cancel).await);
    }

    #[tokio::test]
    async fn dial_listener_stop_joins_cancellation_and_bounds_a_wedged_task() -> Result<()> {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let temp = tempfile::tempdir()?;
        let peer_addr = EndpointAddr::new(iroh::SecretKey::generate().public());

        let graceful_path = temp.path().join("graceful.sock");
        fs::write(&graceful_path, b"socket placeholder")?;
        let graceful_cancel = CancellationToken::new();
        let graceful_exited = Arc::new(AtomicBool::new(false));
        let graceful_task = {
            let cancel = graceful_cancel.clone();
            let exited = graceful_exited.clone();
            tokio::spawn(async move {
                cancel.cancelled().await;
                exited.store(true, Ordering::SeqCst);
            })
        };
        let graceful = DialSocket {
            path: graceful_path.clone(),
            peer_addr: peer_addr.clone(),
            listener_cancel: graceful_cancel,
            listener_task: Some(graceful_task),
        };

        assert!(
            graceful.stop_with_timeout(Duration::from_secs(1)).await,
            "normal listener cancellation should join without timing out"
        );
        assert!(
            graceful_exited.load(Ordering::SeqCst),
            "stop returned before the canceled listener task terminated"
        );
        assert!(!graceful_path.exists());

        let wedged_path = temp.path().join("wedged.sock");
        fs::write(&wedged_path, b"socket placeholder")?;
        let wedged_cancel = CancellationToken::new();
        let wedged_dropped = Arc::new(AtomicBool::new(false));
        let (wedged_started_tx, wedged_started_rx) = tokio::sync::oneshot::channel();
        let wedged_task = {
            let dropped = wedged_dropped.clone();
            tokio::spawn(async move {
                let _drop_flag = DropFlag(dropped);
                let _ = wedged_started_tx.send(());
                std::future::pending::<()>().await;
            })
        };
        wedged_started_rx
            .await
            .expect("wedged listener task did not start");
        let wedged = DialSocket {
            path: wedged_path.clone(),
            peer_addr,
            listener_cancel: wedged_cancel,
            listener_task: Some(wedged_task),
        };

        assert!(
            !wedged.stop_with_timeout(Duration::from_millis(10)).await,
            "wedged listener should take the timeout/abort path"
        );
        assert!(
            wedged_dropped.load(Ordering::SeqCst),
            "stop returned before the aborted listener task was joined"
        );
        assert!(!wedged_path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn repeated_exec_and_shell_dials_keep_one_listener_per_path() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let home = FabricHome::new(temp.path());
        let peer_a = iroh::SecretKey::generate().public();
        let peer_b = iroh::SecretKey::generate().public();
        let mut peers = PeerBook::default();
        peers.add(peer_a, Some("peer-a".to_string()), None);
        peers.add(peer_b, Some("peer-b".to_string()), None);
        peers.save(&home)?;

        let node = FabricNode::start(home.clone()).await?;
        let state = node.state();
        let cases = [
            ("peer-a", exec::EXEC_PROTOCOL, exec::EXEC_ALPN),
            ("peer-a", shell::SHELL_PROTOCOL, shell::RESUMABLE_SHELL_ALPN),
            ("peer-b", exec::EXEC_PROTOCOL, exec::EXEC_ALPN),
            ("peer-b", shell::SHELL_PROTOCOL, shell::RESUMABLE_SHELL_ALPN),
        ];

        // More replacements than the production macOS soft FD limit. The old
        // implementation left every unlinked listener task alive and exhausted
        // the process after this pattern repeated in long-running daemons.
        for _ in 0..300 {
            for (peer, protocol, alpn) in cases {
                state
                    .dial_alpn(peer, protocol, alpn.to_vec(), false)
                    .await?;
            }
            assert_eq!(
                state.active_dial_listeners.load(Ordering::SeqCst),
                cases.len(),
                "replacement leaked an accept loop"
            );
        }

        let generic_path = state.dial("peer-a", "test/reused/1").await?;
        let generic_key = (peer_a.to_string(), "test/reused/1".to_string());
        let first_task = state
            .dial_sockets
            .lock()
            .await
            .get(&generic_key)
            .and_then(|socket| socket.listener_task.as_ref())
            .map(JoinHandle::id)
            .expect("generic listener missing");
        assert_eq!(state.dial("peer-a", "test/reused/1").await?, generic_path);
        let second_task = state
            .dial_sockets
            .lock()
            .await
            .get(&generic_key)
            .and_then(|socket| socket.listener_task.as_ref())
            .map(JoinHandle::id)
            .expect("reused generic listener missing");
        assert_eq!(
            first_task, second_task,
            "reusable dial listener was replaced"
        );

        let stopped_task = state
            .dial_sockets
            .lock()
            .await
            .get_mut(&generic_key)
            .and_then(|socket| socket.listener_task.take())
            .expect("generic listener task missing before fault injection");
        stopped_task.abort();
        let _ = stopped_task.await;
        assert_eq!(
            state.active_dial_listeners.load(Ordering::SeqCst),
            cases.len()
        );
        assert_eq!(state.dial("peer-a", "test/reused/1").await?, generic_path);
        let replacement_task = state
            .dial_sockets
            .lock()
            .await
            .get(&generic_key)
            .and_then(|socket| socket.listener_task.as_ref())
            .map(JoinHandle::id)
            .expect("dead reusable listener was not replaced");
        assert_ne!(
            replacement_task, first_task,
            "finished reusable listener task was retained"
        );
        assert_eq!(
            state.active_dial_listeners.load(Ordering::SeqCst),
            cases.len() + 1
        );

        let paths: Vec<PathBuf> = state
            .dial_sockets
            .lock()
            .await
            .values()
            .map(|socket| socket.path.clone())
            .collect();
        assert_eq!(paths.len(), cases.len() + 1);
        assert!(paths.iter().all(|path| path.exists()));

        node.shutdown().await?;
        assert_eq!(state.active_dial_listeners.load(Ordering::SeqCst), 0);
        assert!(
            paths.iter().all(|path| !path.exists()),
            "daemon teardown left dial socket paths behind"
        );
        Ok(())
    }

    #[tokio::test]
    async fn replacing_exec_listener_preserves_accepted_session() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_daemon_options(
            server_home.clone(),
            DaemonOptions {
                allow_exec: true,
                ..DaemonOptions::default()
            },
        )
        .await?;
        let client = FabricNode::start(client_home.clone()).await?;

        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;

        let state = client.state();
        let socket = state
            .dial_alpn(
                "server",
                exec::EXEC_PROTOCOL,
                exec::EXEC_ALPN.to_vec(),
                false,
            )
            .await?;
        let mut first = UnixStream::connect(&socket).await?;
        let release = server_dir.path().join("release-exec");
        let argv = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "printf ready; while [ ! -e \"$1\" ]; do sleep 0.01; done; printf done".to_string(),
            "fabric-listener-test".to_string(),
            release.display().to_string(),
        ];
        exec::write_client_argv(&mut first, &argv).await?;

        let ready = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match exec::read_server_frame(&mut first).await? {
                    Some(exec::ServerFrame::Stdout(bytes)) => break Ok(bytes),
                    Some(exec::ServerFrame::Stderr(_)) => {}
                    Some(exec::ServerFrame::Error(error)) => bail!("{error}"),
                    Some(exec::ServerFrame::Exit(code)) => {
                        bail!("exec exited before listener replacement: {code}")
                    }
                    None => bail!("exec closed before listener replacement"),
                }
            }
        })
        .await??;
        assert_eq!(ready, b"ready");

        let replacement = state
            .dial_alpn(
                "server",
                exec::EXEC_PROTOCOL,
                exec::EXEC_ALPN.to_vec(),
                false,
            )
            .await?;
        assert_eq!(replacement, socket);
        assert_eq!(state.active_dial_listeners.load(Ordering::SeqCst), 1);

        fs::write(&release, b"go")?;
        let (stdout, exit) =
            tokio::time::timeout(Duration::from_secs(10), collect_exec(&mut first)).await??;
        assert_eq!(stdout, b"done");
        assert_eq!(
            exit, 0,
            "accepted exec did not survive listener replacement"
        );

        let mut second = UnixStream::connect(&replacement).await?;
        exec::write_client_argv(
            &mut second,
            &[
                "/bin/sh".to_string(),
                "-c".to_string(),
                "printf replacement".to_string(),
            ],
        )
        .await?;
        let (stdout, exit) =
            tokio::time::timeout(Duration::from_secs(10), collect_exec(&mut second)).await??;
        assert_eq!(stdout, b"replacement");
        assert_eq!(exit, 0);

        server.shutdown().await?;
        client.shutdown().await?;
        assert_eq!(state.active_dial_listeners.load(Ordering::SeqCst), 0);
        Ok(())
    }

    /// Two mutually trusting nodes, the shape every probe test needs.
    async fn probe_pair(
        root: &std::path::Path,
    ) -> Result<(FabricNode, FabricNode, FabricHome, FabricHome)> {
        let server_home = FabricHome::new(root.join("server"));
        let client_home = FabricHome::new(root.join("client"));
        let server = FabricNode::start(server_home.clone()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;
        Ok((server, client, server_home, client_home))
    }

    /// A trusted peer id that resolves but can never be reached, so a probe has
    /// to fall through to its deadline rather than answering quickly.
    async fn unroutable_peer(home: &FabricHome, node: &FabricNode, name: &str) -> Result<String> {
        let mut peers = PeerBook::load(home)?;
        peers.add_with_allow(
            iroh::SecretKey::generate().public(),
            Some(name.to_string()),
            None,
            Some(vec!["echo".to_string()]),
        );
        peers.save(home)?;
        node.state().reload_peers().await?;
        Ok(name.to_string())
    }

    async fn trust_test_peer(
        home: &FabricHome,
        node: &FabricNode,
        id: EndpointId,
        name: &str,
        addr: EndpointAddr,
    ) -> Result<()> {
        let mut peers = PeerBook::load(home)?;
        peers.add_with_allow(
            id,
            Some(name.to_string()),
            Some(addr),
            Some(
                [
                    "shell",
                    "exec",
                    "sync",
                    "echo",
                    "send-file",
                    "audit/echo",
                    "audit/sink",
                    "test/reused/1",
                ]
                .into_iter()
                .map(str::to_string)
                .collect(),
            ),
        );
        peers.save(home)?;
        node.state().reload_peers().await
    }

    async fn collect_exec(stream: &mut UnixStream) -> Result<(Vec<u8>, i32)> {
        let mut stdout = Vec::new();
        loop {
            match exec::read_server_frame(stream).await? {
                Some(exec::ServerFrame::Stdout(bytes)) => stdout.extend_from_slice(&bytes),
                Some(exec::ServerFrame::Stderr(_)) => {}
                Some(exec::ServerFrame::Error(error)) => bail!("{error}"),
                Some(exec::ServerFrame::Exit(code)) => return Ok((stdout, code)),
                None => bail!("exec closed without an exit frame"),
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn daemon_lease_rejects_concurrent_owner_and_allows_stale_file() {
        let temp = tempfile::tempdir().unwrap();
        let home = FabricHome::new(temp.path());
        home.prepare().unwrap();
        std::fs::write(home.root().join("run/daemon.lock"), b"stale-pid\n").unwrap();
        let stale = DaemonLease::acquire(&home).unwrap();
        drop(stale);
        let first = DaemonLease::acquire(&home).unwrap();
        assert!(!daemon_lock_available(&home).unwrap());
        assert!(DaemonLease::acquire(&home).is_err());
        drop(first);
        assert!(daemon_lock_available(&home).unwrap());
        assert!(DaemonLease::acquire(&home).is_ok());
    }

    #[test]
    fn restart_down_decision_requires_status_failure_and_free_lease() {
        assert!(!restart_down_decision(false, false));
        assert!(restart_down_decision(false, true));
        assert!(!restart_down_decision(true, true));
    }

    /// A network-change notice on a healthy machine must not disturb a working
    /// shell.
    ///
    /// The regression this pins was live and visible: the notice fires
    /// constantly on an idle laptop, 449 times in one day with an identical
    /// unchanged reason each time, and the handler used to drop every tunnel
    /// connection on every one of them. A held remote shell was therefore
    /// interrupted about every 76 seconds indefinitely, resuming each time and
    /// so looking like a flaky network rather than self-inflicted churn.
    #[tokio::test]
    async fn repeated_network_changes_do_not_disturb_a_live_resumable_shell() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_options(server_home.clone(), true).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;

        let state = client.state();
        let socket = state
            .dial_alpn(
                "server",
                shell::SHELL_PROTOCOL,
                shell::RESUMABLE_SHELL_ALPN.to_vec(),
                false,
            )
            .await?;
        let mut shell_stream = UnixStream::connect(&socket).await?;

        // Mark this PTY, so a silently replaced session is detectable.
        shell::write_client_stdin(
            &mut shell_stream,
            b"MARK=original; printf '%s-%s\n' before change\n",
        )
        .await?;
        read_shell_marker(&mut shell_stream, b"before-change").await?;

        let drops_before = *state.tunnel_drop_tx.borrow();
        for _ in 0..5 {
            state
                .rehome_after_network_change("test: healthy no-op notice", true)
                .await;
        }
        let drops_after = *state.tunnel_drop_tx.borrow();
        assert_eq!(
            drops_before, drops_after,
            "a healthy network-change notice must not close working tunnels"
        );

        // The same PTY is still there, still holding the shell variable it set
        // before the notices, so nothing was torn down and re-established.
        shell::write_client_stdin(&mut shell_stream, b"printf '%s-%s\n' $MARK survived\n").await?;
        read_shell_marker(&mut shell_stream, b"original-survived").await?;

        // Positive control. The check above is an equality, which would also
        // hold if the counter simply could not move here — a watch send with no
        // receivers is silently a no-op. Prove the probe works before trusting
        // what it did not observe.
        state.drop_tunnel_connections();
        assert!(
            *state.tunnel_drop_tx.borrow() > drops_after,
            "the tunnel-drop probe never moves, so the check above proved nothing"
        );

        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    /// The retention default is a decided value, so pin it.
    ///
    /// It is derived from the job: retention must outlast a month-long trip so a
    /// fault in week one is still readable on return, plus margin for the gap
    /// before anyone looks. Changing it should require changing this assertion
    /// and saying why.
    #[test]
    fn log_retention_default_outlasts_a_month_long_trip() {
        assert_eq!(DEFAULT_LOG_RETENTION_DAYS, 45);
        assert!(
            DEFAULT_LOG_RETENTION_DAYS > 31,
            "retention must exceed a month-long trip or an early fault is deleted \
             before anyone returns to the machine to investigate it"
        );
        assert_eq!(resolve_log_retention_days(None), Some(45));
    }

    /// An explicit 0 restores the old unbounded behaviour, on purpose.
    #[test]
    fn zero_days_disables_deletion() {
        assert_eq!(resolve_log_retention_days(Some("0")), None);
    }

    #[test]
    fn an_override_is_honoured() {
        assert_eq!(resolve_log_retention_days(Some("7")), Some(7));
        assert_eq!(resolve_log_retention_days(Some("  7  ")), Some(7));
    }

    /// A typo must not silently restore unbounded growth.
    ///
    /// Failing open here would be the worst outcome: nobody is at this machine's
    /// keyboard, so the defect would return unnoticed and look like the fix
    /// never worked.
    #[test]
    fn an_unparseable_override_falls_back_to_the_bound_not_to_unbounded() {
        for bad in ["", "  ", "lots", "-1", "9999999999999999999999", "7d"] {
            assert_eq!(
                resolve_log_retention_days(Some(bad)),
                Some(DEFAULT_LOG_RETENTION_DAYS),
                "{bad:?} must fall back to the bound, never to unbounded"
            );
        }
    }

    /// The counters must move because the real notice path ran.
    ///
    /// The store has its own unit tests, and they prove only that the store
    /// counts what it is told. They cannot catch the failure that matters here:
    /// a daemon that never calls it. So this drives a real shell over two real
    /// daemons, takes its transport away, and reads the counters back through
    /// the same control response an operator would use.
    #[tokio::test]
    async fn a_real_shell_loss_and_resume_moves_the_durable_counters() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_options(server_home.clone(), true).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;

        let state = client.state();
        let telemetry = state.telemetry();
        let socket = state
            .dial_alpn(
                "server",
                shell::SHELL_PROTOCOL,
                shell::RESUMABLE_SHELL_ALPN.to_vec(),
                false,
            )
            .await?;
        let mut shell_stream = UnixStream::connect(&socket).await?;
        shell::write_client_stdin(
            &mut shell_stream,
            b"MARK=original; printf '%s-%s\n' before drop\n",
        )
        .await?;
        read_shell_marker(&mut shell_stream, b"before-drop").await?;

        // The negative control, and it runs BEFORE the loss on purpose. A
        // counter that was already at 1 would make the assertion below pass
        // without the drop having done anything.
        assert!(
            telemetry
                .peer("server")
                .is_none_or(|stats| stats.losses == 0),
            "a healthy shell must record no loss; the later count would prove nothing"
        );

        state.drop_tunnel_connections();

        let deadline = Instant::now() + Duration::from_secs(30);
        let recorded = loop {
            if let Some(stats) = telemetry.peer("server")
                && stats.resumes >= 1
            {
                break stats;
            }
            if Instant::now() >= deadline {
                let seen = telemetry.peer("server");
                panic!("the shell never recorded a resume within 30s; counters: {seen:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        assert_eq!(
            recorded.losses, 1,
            "one dropped transport is one loss, however many retries it took"
        );
        assert_eq!(recorded.resumes, 1);
        assert!(
            recorded.reconnect_attempts >= 1,
            "a loss that resumed must have taken at least one attempt"
        );
        assert_eq!(
            recorded.reconnect.samples, 1,
            "the reconnect must be measured, not merely counted"
        );
        assert!(
            recorded.reconnect.max_micros > 0,
            "a measured reconnect cannot take zero time"
        );

        // The same PTY, so this counted a genuine resume of the original
        // session rather than a silent replacement that would also look like
        // a resume from the outside.
        shell::write_client_stdin(&mut shell_stream, b"printf '%s-%s\n' $MARK survived\n").await?;
        read_shell_marker(&mut shell_stream, b"original-survived").await?;

        // And the operator-facing read carries it, not just the in-process store.
        let response = state.reachability_status_response().await?;
        match response {
            ControlResponse::ReachabilityStatus {
                connection_telemetry,
                ..
            } => {
                let reported = connection_telemetry
                    .get("server")
                    .expect("the control response must carry the peer's counters");
                assert_eq!(reported.losses, 1);
                assert_eq!(reported.resumes, 1);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    /// The recovery path that genuinely needs a teardown must keep doing it.
    /// Not dropping tunnels on a noisy notice is only safe if a suspect endpoint
    /// still gets an explicit close.
    #[tokio::test]
    async fn endpoint_recovery_still_closes_tunnels_explicitly() -> Result<()> {
        let home_dir = tempfile::tempdir()?;
        let home = FabricHome::new(home_dir.path());
        let node = FabricNode::start(home.clone()).await?;
        let state = node.state();

        // Stand in for a live tunnel. The drop signal is a watch channel, and a
        // send with no receivers fails and leaves the value untouched, so
        // without this subscription the probe below could never move and the
        // test would fail for the wrong reason.
        let _tunnel = state.tunnel_drop_rx();

        // A peer that stayed unreachable with no other peer answering: the
        // endpoint itself is suspect, which is what escalation means.
        let drops_before = *state.tunnel_drop_tx.borrow();
        state
            .recover_unreachable_peer("absent-peer", PEER_HEALTH_ATTEMPTS_BEFORE_RECYCLE, 0)
            .await;
        let drops_after = *state.tunnel_drop_tx.borrow();
        assert!(
            drops_after > drops_before,
            "an escalated endpoint recovery must still close tunnels explicitly"
        );

        node.shutdown().await?;
        Ok(())
    }

    /// Read framed shell output until `marker` appears.
    async fn read_shell_marker<R>(stream: &mut R, marker: &[u8]) -> Result<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let mut seen = Vec::new();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match shell::read_server_frame(stream).await? {
                    Some(shell::ServerFrame::Output(bytes)) => {
                        seen.extend_from_slice(&bytes);
                        if seen.windows(marker.len()).any(|window| window == marker) {
                            return Ok(());
                        }
                    }
                    Some(shell::ServerFrame::Status(_)) => {}
                    Some(shell::ServerFrame::Error(error)) => bail!("shell error: {error}"),
                    Some(shell::ServerFrame::Exit(code)) => bail!("shell exited: {code}"),
                    None => bail!(
                        "shell stream closed before {:?}; saw {:?}",
                        String::from_utf8_lossy(marker),
                        String::from_utf8_lossy(&seen)
                    ),
                }
            }
        })
        .await
        .with_context(|| {
            format!(
                "timed out waiting for {:?}",
                String::from_utf8_lossy(marker)
            )
        })?
    }

    /// One peer's rejected handshakes must not delay accepting a healthy peer.
    ///
    /// The accept loop used to wait on a single shared failure record before it
    /// called accept at all, and every handler charged that same record. So an
    /// untrusted peer hammering the door escalated a delay that healthy peers
    /// then queued behind. Per-connection outcomes now bound nothing but their own
    /// diagnostics.
    #[tokio::test]
    async fn rejected_inbound_connections_do_not_delay_a_healthy_peer() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let good_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let good_home = FabricHome::new(good_dir.path());
        let server = FabricNode::start(server_home.clone()).await?;
        let good = FabricNode::start(good_home.clone()).await?;
        trust_test_peer(&server_home, &server, good.id(), "good", good.addr()).await?;
        trust_test_peer(&good_home, &good, server.id(), "server", server.addr()).await?;

        // Baseline the healthy path before adding pressure. The assertion below is
        // relative to this, because any gate on the accept loop must add at least
        // one whole backoff step on top of a normal accept.
        good.state().ping("server").await?;
        let started = Instant::now();
        good.state().ping("server").await?;
        let baseline = started.elapsed();

        // An untrusted endpoint: its handshakes are genuinely rejected by the
        // allow-list, so these are real inbound failures and not simulated ones.
        let intruder = Endpoint::bind(iroh::endpoint::presets::N0).await?;
        intruder.online().await;
        for _ in 0..6 {
            // Drive each rejection to completion, so any escalation the old code
            // would have recorded has definitely been recorded before we measure.
            let attempt = intruder.connect(server.addr(), BUILTIN_ECHO_ALPN).await;
            if let Ok(connection) = attempt {
                let _ = connection.open_bi().await;
                connection.close(0u32.into(), b"done");
            }
        }

        // The healthy peer is let in without queueing behind that.
        let started = Instant::now();
        good.state().ping("server").await?;
        let waited = started.elapsed();
        assert!(
            waited < baseline + INCOMING_FAILURE_INITIAL_BACKOFF,
            "a healthy peer waited {waited:?} behind another peer's rejected handshakes, \
             against a {baseline:?} baseline; a gate on the accept loop would add at \
             least one {INCOMING_FAILURE_INITIAL_BACKOFF:?} step"
        );

        intruder.close().await;
        good.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    /// A reconnecting session must not block the recycle that could restore it.
    ///
    /// This is the deadlock direction of the same guard: if a session whose
    /// transport is down still pinned the endpoint, then the endpoint could never
    /// be rebuilt, and rebuilding it may be exactly what lets that session
    /// reconnect. The guard is therefore held per-attach, not per-session.
    #[tokio::test]
    async fn a_reconnecting_session_does_not_block_the_recycle() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_options(server_home.clone(), true).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;

        let state = client.state();
        let socket = state
            .dial_alpn(
                "server",
                shell::SHELL_PROTOCOL,
                shell::RESUMABLE_SHELL_ALPN.to_vec(),
                false,
            )
            .await?;
        let mut shell_stream = UnixStream::connect(&socket).await?;
        shell::write_client_stdin(&mut shell_stream, b"printf '%s-%s\n' recon up\n").await?;
        read_shell_marker(&mut shell_stream, b"recon-up").await?;
        assert_eq!(state.client_attaches.attached(), 1);

        // Take the transport away and keep it away, so the session is genuinely
        // between attaches rather than momentarily so. Stopping the peer is the
        // deterministic way to do that: dropping the tunnel alone lets the client
        // reattach within about 100ms and the window would be a race.
        server.shutdown().await?;

        // While it is reconnecting, the guard must be down.
        let unheld = tokio::time::timeout(Duration::from_secs(20), async {
            while state.client_attaches.attached() > 0 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(
            unheld.is_ok(),
            "a session between attaches must not hold the recycle guard"
        );

        // And the recycle it might need is therefore allowed.
        let generation = state.endpoint_handle().generation;
        let outcome = state
            .recycle_endpoint_if_generation(generation, "reconnecting session")
            .await?;
        assert!(
            matches!(outcome, EndpointRecycleOutcome::Recycled),
            "a reconnecting session must not block the recycle, got {outcome:?}"
        );

        drop(shell_stream);
        client.shutdown().await?;
        Ok(())
    }

    /// A half-closed local input releases the recycle guard, and the session
    /// still delivers its remaining remote output across the recycle that
    /// releasing allowed.
    ///
    /// This is the correction to a wrong assumption of mine. I first released the
    /// guard when the whole attach returned, on the reasoning that a closed local
    /// socket meant no user was left to protect. Two things were wrong with that.
    /// The attach can stay up for a long time after the local side is done —
    /// measured at over 40 seconds on Linux, with the server still reporting the
    /// session attached, which pinned the endpoint-recycle guard for that whole
    /// window and made endpoint repair impossible. And a client that has finished
    /// SENDING may still be READING: a half-close is not a close, so the session
    /// must keep working after the guard is released.
    #[tokio::test]
    async fn half_closed_local_input_releases_guard_and_still_delivers_output() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_options(server_home.clone(), true).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;

        let state = client.state();
        let socket = state
            .dial_alpn(
                "server",
                shell::SHELL_PROTOCOL,
                shell::RESUMABLE_SHELL_ALPN.to_vec(),
                false,
            )
            .await?;
        let shell_stream = UnixStream::connect(&socket).await?;
        let (mut read_half, mut write_half) = shell_stream.into_split();

        shell::write_client_stdin(&mut write_half, b"printf '%s-%s\n' shell up\n").await?;
        read_shell_marker(&mut read_half, b"shell-up").await?;

        // PROOF 1: a live bidirectional local client blocks the recycle.
        assert_eq!(state.client_attaches.attached(), 1);
        let generation = state.endpoint_handle().generation;
        let outcome = state
            .recycle_endpoint_if_generation(generation, "half-close: still bidirectional")
            .await?;
        assert!(
            matches!(outcome, EndpointRecycleOutcome::SessionsAttached { .. }),
            "a live bidirectional local client must block the recycle, got {outcome:?}"
        );

        // Queue work whose output arrives AFTER the local input is finished, then
        // half-close: stop sending, keep reading.
        shell::write_client_stdin(
            &mut write_half,
            b"sleep 1; printf '%s-%s\n' delayed output\n",
        )
        .await?;
        write_half.shutdown().await?;
        drop(write_half);

        // PROOF 2: local input EOF releases the guard, without waiting for the
        // remote teardown that on Linux may not have happened at all.
        //
        // The bound is deliberately far below the remote's completion time. The
        // queued command sleeps for a second before it prints, so the session
        // cannot possibly have finished tearing down inside this window, and a
        // release observed here can only have come from local-input EOF. Without
        // that reasoning the check is not load-bearing at all: on macOS the remote
        // tears down in about 300ms, so a generous timeout passes either way, which
        // is exactly how I first failed to notice this assertion proved nothing.
        let release_bound = Duration::from_millis(500);
        let release_started = Instant::now();
        let released = tokio::time::timeout(release_bound, async {
            while state.client_attaches.attached() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "local input EOF must release the recycle guard within {release_bound:?} \
             regardless of remote teardown; still held after {:?}",
            release_started.elapsed()
        );

        // The recycle that releasing allowed now proceeds.
        let outcome = state
            .recycle_endpoint_if_generation(generation, "half-close: input finished")
            .await?;
        assert!(
            matches!(outcome, EndpointRecycleOutcome::Recycled),
            "with local input finished the recycle must proceed, got {outcome:?}"
        );

        // PROOF 3: the half-closed reader still receives the delayed output, and
        // receives it exactly once, across the recycle it just permitted.
        let mut seen = Vec::new();
        let read_result = tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                match shell::read_server_frame(&mut read_half).await {
                    Ok(Some(shell::ServerFrame::Output(bytes))) => seen.extend_from_slice(&bytes),
                    Ok(Some(shell::ServerFrame::Exit(_))) | Ok(None) => return Ok(()),
                    Ok(Some(shell::ServerFrame::Status(_))) => {}
                    Ok(Some(shell::ServerFrame::Error(error))) => {
                        return Err(anyhow::anyhow!("shell error: {error}"));
                    }
                    Err(error) => return Err(error),
                }
                if count_occurrences(&seen, b"delayed-output") > 0 {
                    // Keep draining briefly to catch a duplicate rather than
                    // returning at the first sighting.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        })
        .await;
        let text = String::from_utf8_lossy(&seen).into_owned();
        let occurrences = count_occurrences(&seen, b"delayed-output");
        assert_eq!(
            occurrences, 1,
            "delayed output must arrive exactly once across the recycle, saw {occurrences} in {text:?} (read result: {read_result:?})"
        );

        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    fn count_occurrences(haystack: &[u8], needle: &[u8]) -> usize {
        if needle.is_empty() || haystack.len() < needle.len() {
            return 0;
        }
        haystack
            .windows(needle.len())
            .filter(|w| *w == needle)
            .count()
    }

    /// One peer's success must not clear another peer's failure record on the
    /// same ALPN.
    ///
    /// record_success removes a key, so if inbound records are not keyed per peer
    /// then any peer completing a connection wipes every other peer's diagnostic
    /// history for that protocol. That is what happened before identity.peer was
    /// assigned: every record landed on <unidentified> and they all shared one
    /// entry.
    ///
    /// Both endpoints here are TRUSTED and use the same ALPN, because that is the
    /// only arrangement that exercises the clearing path. An earlier version of
    /// this test used a failing untrusted peer as the second party, which never
    /// calls record_success and so never tested the boundary it claimed to.
    #[tokio::test]
    async fn one_peers_success_does_not_clear_another_peers_inbound_record() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let server = FabricNode::start(server_home.clone()).await?;
        let state = server.state();

        let failing = Endpoint::bind(iroh::endpoint::presets::N0).await?;
        let succeeding = Endpoint::bind(iroh::endpoint::presets::N0).await?;
        failing.online().await;
        succeeding.online().await;
        trust_test_peer(
            &server_home,
            &server,
            failing.id(),
            "peer-failing",
            failing.addr(),
        )
        .await?;
        trust_test_peer(
            &server_home,
            &server,
            succeeding.id(),
            "peer-succeeding",
            succeeding.addr(),
        )
        .await?;

        let failing_key = BackoffKey {
            peer: failing.id().to_string(),
            alpn: String::from_utf8_lossy(BUILTIN_ECHO_ALPN).to_string(),
        };
        let succeeding_key = BackoffKey {
            peer: succeeding.id().to_string(),
            alpn: String::from_utf8_lossy(BUILTIN_ECHO_ALPN).to_string(),
        };

        // Peer A fails after a completed handshake. The echo handler propagates
        // its errors, so closing before opening a stream fails accept_bi.
        for _ in 0..3 {
            if let Ok(connection) = failing.connect(server.addr(), BUILTIN_ECHO_ALPN).await {
                connection.close(0u32.into(), b"no stream");
                connection.closed().await;
            }
        }

        // A's record exists and is keyed on A. This is also the check that fails
        // when identity.peer is not assigned, printing <unidentified>.
        let recorded = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let keys: Vec<BackoffKey> = state
                    .incoming_failures
                    .states
                    .lock()
                    .await
                    .keys()
                    .cloned()
                    .collect();
                if !keys.is_empty() {
                    return keys;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("no inbound failure was ever recorded")?;
        assert!(
            recorded.contains(&failing_key),
            "a failure after a completed handshake must be keyed on its peer; \
             recorded {recorded:?}, expected {failing_key:?}"
        );

        // Peer B now completes a full echo round trip, which is the success that
        // used to wipe A's record.
        let handled_before = state.builtin_echo_hits();
        let nonce = [7u8; 32];
        let connection = succeeding.connect(server.addr(), BUILTIN_ECHO_ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(&nonce).await?;
        send.finish()?;
        let echoed = recv.read_to_end(nonce.len() + 1).await?;
        assert_eq!(echoed, nonce, "peer B's echo must actually have succeeded");
        connection.close(0u32.into(), b"done");
        connection.closed().await;

        // Wait for the server to have actually run B's handler. A success leaves
        // no record of its own, so there is nothing in the record set to wait on:
        // an earlier draft waited for "B has no record", which was true before B
        // was ever processed and made this whole check vacuous. The echo counter
        // is the observable that B's handler ran.
        tokio::time::timeout(Duration::from_secs(20), async {
            while state.builtin_echo_hits() <= handled_before {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("peer B's echo was never handled by the server")?;
        // Then let the success be recorded, which happens after the handler
        // returns.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // THE BOUNDARY: B succeeded, and A's record is untouched.
        let after: Vec<BackoffKey> = state
            .incoming_failures
            .states
            .lock()
            .await
            .keys()
            .cloned()
            .collect();
        assert!(
            after.contains(&failing_key),
            "peer B's success cleared peer A's record: {after:?}"
        );
        assert!(
            !after.contains(&succeeding_key),
            "peer B succeeded, so it must hold no failure record: {after:?}"
        );

        succeeding.close().await;
        failing.close().await;
        server.shutdown().await?;
        Ok(())
    }

    /// Sustained inbound failure must stay bounded in both concurrency and log
    /// volume.
    ///
    /// Removing the accept gate must not trade a delay for an unbounded spin, so
    /// this is the control for that: handlers stay inside the semaphore's capacity
    /// and the failure log keeps suppressing instead of emitting a line each time.
    #[tokio::test]
    async fn sustained_inbound_failure_stays_bounded() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let server = FabricNode::start(server_home.clone()).await?;
        let state = server.state();

        let intruder = Endpoint::bind(iroh::endpoint::presets::N0).await?;
        intruder.online().await;

        let mut peak_in_flight = 0usize;
        for _ in 0..24 {
            let attempt = intruder.connect(server.addr(), BUILTIN_ECHO_ALPN).await;
            if let Ok(connection) = attempt {
                let _ = connection.open_bi().await;
                connection.close(0u32.into(), b"done");
            }
            peak_in_flight = peak_in_flight.max(
                MAX_INCOMING_HANDLERS.saturating_sub(state.incoming_slots.available_permits()),
            );
        }

        assert!(
            peak_in_flight <= MAX_INCOMING_HANDLERS,
            "in-flight handlers {peak_in_flight} exceeded the semaphore capacity"
        );

        // Log pressure: the suppression window is doing its job rather than
        // emitting one line per rejection.
        let records = state.incoming_failures.states.lock().await;
        let logged_and_suppressed: Vec<_> = records
            .values()
            .map(|record| (record.consecutive_failures, record.suppressed))
            .collect();
        drop(records);
        if let Some((failures, suppressed)) = logged_and_suppressed
            .iter()
            .copied()
            .max_by_key(|(failures, _)| *failures)
        {
            assert!(
                failures <= 1 || suppressed > 0,
                "repeated inbound failures must be rate-limited: {failures} failures, {suppressed} suppressed"
            );
        }

        intruder.close().await;
        server.shutdown().await?;
        Ok(())
    }

    /// The whole audit in one place: two peers, two concurrent sessions of
    /// different kinds, and every notice class fired at them.
    ///
    /// Each disruption was found and fixed separately, so this exists to catch a
    /// future one that only shows up with more than one peer or more than one
    /// session in flight — the shape none of the single-session tests can see.
    #[tokio::test]
    async fn concurrent_sessions_survive_every_healthy_notice() -> Result<()> {
        let client_dir = tempfile::tempdir()?;
        let shell_dir = tempfile::tempdir()?;
        let echo_dir = tempfile::tempdir()?;
        let client_home = FabricHome::new(client_dir.path());
        let shell_home = FabricHome::new(shell_dir.path());
        let echo_home = FabricHome::new(echo_dir.path());
        let client = FabricNode::start(client_home.clone()).await?;
        let shell_peer = FabricNode::start_with_options(shell_home.clone(), true).await?;
        let echo_peer = FabricNode::start(echo_home.clone()).await?;

        for (home, node, name) in [
            (&shell_home, &shell_peer, "shell-peer"),
            (&echo_home, &echo_peer, "echo-peer"),
        ] {
            trust_test_peer(home, node, client.id(), "client", client.addr()).await?;
            trust_test_peer(&client_home, &client, node.id(), name, node.addr()).await?;
        }

        // Session one: a resumable shell to the first peer.
        let state = client.state();
        let shell_socket = state
            .dial_alpn(
                "shell-peer",
                shell::SHELL_PROTOCOL,
                shell::RESUMABLE_SHELL_ALPN.to_vec(),
                false,
            )
            .await?;
        let mut shell_stream = UnixStream::connect(&shell_socket).await?;
        shell::write_client_stdin(
            &mut shell_stream,
            b"MARK=session-one; printf '%s-%s\n' shell up\n",
        )
        .await?;
        read_shell_marker(&mut shell_stream, b"shell-up").await?;

        // Session two: a generic dial to a different peer, carrying raw bytes.
        let echo_socket_path = echo_dir.path().join("echo.sock");
        let echo_listener = UnixListener::bind(&echo_socket_path)?;
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = echo_listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if stream.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        echo_peer.expose("audit/echo", echo_socket_path).await?;
        let dial_socket = state
            .dial_alpn("echo-peer", "audit/echo", b"audit/echo".to_vec(), false)
            .await?;
        let mut dial = UnixStream::connect(&dial_socket).await?;
        dial.write_all(b"dial-up-").await?;
        let mut echoed = [0u8; 8];
        tokio::time::timeout(Duration::from_secs(20), dial.read_exact(&mut echoed)).await??;
        assert_eq!(&echoed, b"dial-up-");

        assert_eq!(
            state.client_attaches.attached(),
            2,
            "both concurrent sessions must be counted"
        );
        let drops_before = *state.tunnel_drop_tx.borrow();
        let generation_before = state.endpoint_handle().generation;

        // Every notice class, in turn, none of which describes a broken endpoint.
        for _ in 0..3 {
            state
                .rehome_after_network_change("audit: healthy no-op notice", true)
                .await;
        }
        state
            .rehome_after_network_change("audit: route update", true)
            .await;
        // One peer away while the other answers: cheap recovery, no teardown.
        state
            .recover_unreachable_peer("absent", PEER_HEALTH_ATTEMPTS_BEFORE_RECYCLE, 1)
            .await;
        // And a recycle attempt, which is what a failed health poll ends in.
        let outcome = state
            .recycle_endpoint_if_generation(generation_before, "audit: health poll")
            .await?;
        assert!(
            matches!(outcome, EndpointRecycleOutcome::SessionsAttached { .. }),
            "live sessions must hold off the recycle, got {outcome:?}"
        );

        assert_eq!(
            *state.tunnel_drop_tx.borrow(),
            drops_before,
            "no healthy notice may close a tunnel"
        );
        assert_eq!(
            state.endpoint_handle().generation,
            generation_before,
            "no healthy notice may rebuild the endpoint"
        );

        // Both sessions are the same sessions, not fresh replacements.
        shell::write_client_stdin(&mut shell_stream, b"printf '%s-%s\n' $MARK alive\n").await?;
        read_shell_marker(&mut shell_stream, b"session-one-alive").await?;
        dial.write_all(b"still-ok").await?;
        tokio::time::timeout(Duration::from_secs(20), dial.read_exact(&mut echoed)).await??;
        assert_eq!(
            &echoed, b"still-ok",
            "the generic dial must still carry exact bytes"
        );

        // POSITIVE CONTROL: the drop probe used above can actually move.
        state.drop_tunnel_connections();
        assert!(
            *state.tunnel_drop_tx.borrow() > drops_before,
            "the tunnel-drop probe never moves, so the checks above proved nothing"
        );

        client.shutdown().await?;
        shell_peer.shutdown().await?;
        echo_peer.shutdown().await?;
        Ok(())
    }

    /// A generic dial's reconnect telemetry must reach the log and nothing else.
    ///
    /// The event names are the contract an operator greps for, so pin them here
    /// rather than discovering they changed while reading a live incident.
    #[test]
    fn generic_dial_notices_log_every_event_and_encode_nothing() {
        let notices = generic_dial_notices(
            "peer-a".to_string(),
            "pty-remote".to_string(),
            tunnel::ClientAttachGauge::new(),
            ConnectionRecorder::new(
                Arc::new(TelemetryStore::ephemeral()),
                Arc::new(StdRwLock::new(HashMap::new())),
            ),
        );
        for event in [
            tunnel::ClientConnectionEvent::Reconnecting {
                attempt: 3,
                delay: Duration::from_millis(250),
                error: "connection lost".to_string(),
            },
            tunnel::ClientConnectionEvent::Resumed,
            tunnel::ClientConnectionEvent::Failed {
                error: "session expired".to_string(),
            },
        ] {
            assert!(
                notices.encode_for_test(&event).is_none(),
                "a generic dial must never have bytes written into its stream"
            );
        }
    }

    /// A live GENERIC dial must block a recycle too, and must receive no
    /// injected bytes while doing so.
    ///
    /// The gauge rides the notices mechanism, which on the shell path also writes
    /// status frames into the local stream. A generic dial carries raw bytes for
    /// somebody else's protocol, so the same mechanism must stay silent here or
    /// it corrupts the stream it is trying to describe.
    #[tokio::test]
    async fn live_generic_dial_blocks_recycle_without_injecting_bytes() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start(server_home.clone()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;

        // An echo service behind a plain exposure: raw bytes, no framing.
        let echo_socket = server_dir.path().join("echo.sock");
        let echo_listener = UnixListener::bind(&echo_socket)?;
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = echo_listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if stream.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        server.expose("audit/echo", echo_socket.clone()).await?;

        let state = client.state();
        let socket = state
            .dial_alpn("server", "audit/echo", b"audit/echo".to_vec(), false)
            .await?;
        let mut dial = UnixStream::connect(&socket).await?;
        dial.write_all(b"ping-one").await?;
        let mut echoed = [0u8; 8];
        tokio::time::timeout(Duration::from_secs(20), dial.read_exact(&mut echoed)).await??;
        assert_eq!(
            &echoed, b"ping-one",
            "the generic dial must round-trip raw bytes untouched"
        );

        assert_eq!(
            state.client_attaches.attached(),
            1,
            "a live generic dial must register an attach"
        );
        let generation = state.endpoint_handle().generation;
        let outcome = state
            .recycle_endpoint_if_generation(generation, "audit: live generic dial")
            .await?;
        assert!(
            matches!(outcome, EndpointRecycleOutcome::SessionsAttached { .. }),
            "a live generic dial must block the recycle, got {outcome:?}"
        );

        // Still raw, still exact: the notices mechanism wrote nothing into it.
        dial.write_all(b"ping-two").await?;
        tokio::time::timeout(Duration::from_secs(20), dial.read_exact(&mut echoed)).await??;
        assert_eq!(
            &echoed, b"ping-two",
            "a generic dial must never receive injected status bytes"
        );

        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    /// An attached OUTBOUND session must block an endpoint recycle, and a
    /// session that is not attached must not.
    ///
    /// The guard reads the store of sessions we SERVE, so before the gauge it saw
    /// nothing at all while this machine held a working shell out to a peer:
    /// measured as served sessions=0, attaches=0, verdict None, and the recycle
    /// went ahead and bumped the generation from 0 to 1 underneath a live shell.
    /// That is the common case on a laptop, where every shell is outbound.
    ///
    /// The negative control matters as much as the positive one. A session stuck
    /// reconnecting because the local endpoint is broken must not hold off the
    /// recycle that would repair it, or a disruption bug becomes a deadlock.
    #[tokio::test]
    async fn attached_outbound_session_blocks_recycle_and_a_dead_one_does_not() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start_with_options(server_home.clone(), true).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;

        let state = client.state();
        let socket = state
            .dial_alpn(
                "server",
                shell::SHELL_PROTOCOL,
                shell::RESUMABLE_SHELL_ALPN.to_vec(),
                false,
            )
            .await?;
        let mut shell_stream = UnixStream::connect(&socket).await?;
        shell::write_client_stdin(&mut shell_stream, b"printf '%s-%s\n' out bound\n").await?;
        read_shell_marker(&mut shell_stream, b"out-bound").await?;

        // POSITIVE CONTROL. Nothing is served here, so this is entirely the
        // outbound gauge doing the work.
        let served = state.tunnel_sessions.stats().await;
        assert_eq!(
            served.active_sessions + served.active_attaches,
            0,
            "the client serves nothing; this test must exercise the outbound path"
        );
        assert_eq!(
            state.client_attaches.attached(),
            1,
            "a live outbound shell must register exactly one attach"
        );
        let generation = state.endpoint_handle().generation;
        let outcome = state
            .recycle_endpoint_if_generation(generation, "audit: attached outbound session")
            .await?;
        assert!(
            matches!(outcome, EndpointRecycleOutcome::SessionsAttached { .. }),
            "an attached outbound session must block the recycle, got {outcome:?}"
        );
        assert_eq!(
            state.endpoint_handle().generation,
            generation,
            "a blocked recycle must not bump the endpoint generation"
        );

        // The shell is untouched by the refused recycle.
        shell::write_client_stdin(&mut shell_stream, b"printf '%s-%s\n' still here\n").await?;
        read_shell_marker(&mut shell_stream, b"still-here").await?;

        // NEGATIVE CONTROL. Drop the local end so the outbound session goes away,
        // then prove the endpoint is recyclable again.
        drop(shell_stream);
        let released = tokio::time::timeout(Duration::from_secs(20), async {
            while state.client_attaches.attached() > 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(
            released.is_ok(),
            "the gauge must release when an outbound session ends, or a recycle can never run again"
        );
        let outcome = state
            .recycle_endpoint_if_generation(generation, "audit: no outbound session")
            .await?;
        assert!(
            matches!(outcome, EndpointRecycleOutcome::Recycled),
            "with nothing attached the recycle must proceed, got {outcome:?}"
        );

        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    /// The SERVER must stop counting a session as attached once the client's
    /// local end goes away.
    ///
    /// The test above proves the client releases its own gauge. It says nothing
    /// about the far end, and the far end is what pins the server's endpoint.
    /// This asserts the far end, on a quiet tunnel so no remote output can end
    /// the session by a second route.
    ///
    /// **This is NOT a regression guard for issue 32, and it was wrong to
    /// present it as one.** Issue 32 lives in the branch where the local read
    /// returns an ERROR, and dropping a `UnixStream` in-process closes it
    /// cleanly, so both platforms take the EOF branch here. I confirmed that on
    /// Linux CI twice: with the pre-fix teardown restored, this test passed,
    /// first with a shell and then with this quiet tunnel. Reaching the error
    /// branch needs an abrupt close such as a killed process or an RST, which is
    /// how the original trace produced it.
    ///
    /// The real guard for issue 32 is
    /// `tunnel::tests::an_abrupt_local_close_reports_the_end_just_like_a_clean_eof`,
    /// which injects the ending directly and therefore fails on every platform
    /// when the defect is present. What this test still earns is the healthy
    /// path: a clean local close must detach the session on the server promptly,
    /// and that must not regress.
    #[tokio::test]
    async fn a_clean_local_close_detaches_a_quiet_session_on_the_server_too() -> Result<()> {
        let server_dir = tempfile::tempdir()?;
        let client_dir = tempfile::tempdir()?;
        let server_home = FabricHome::new(server_dir.path());
        let client_home = FabricHome::new(client_dir.path());
        let server = FabricNode::start(server_home.clone()).await?;
        let client = FabricNode::start(client_home.clone()).await?;
        trust_test_peer(&server_home, &server, client.id(), "client", client.addr()).await?;
        trust_test_peer(&client_home, &client, server.id(), "server", server.addr()).await?;

        // A sink: it reads and never writes back. Anything it echoed would give
        // the teardown a second route and mask exactly what this test isolates.
        let sink_socket = server_dir.path().join("sink.sock");
        let sink_listener = UnixListener::bind(&sink_socket)?;
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = sink_listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    while let Ok(read) = stream.read(&mut buf).await {
                        if read == 0 {
                            break;
                        }
                    }
                });
            }
        });
        server.expose("audit/sink", sink_socket.clone()).await?;

        let client_state = client.state();
        let server_state = server.state();
        let socket = client_state
            .dial_alpn("server", "audit/sink", b"audit/sink".to_vec(), false)
            .await?;
        let mut dial = UnixStream::connect(&socket).await?;
        dial.write_all(b"open-the-session").await?;

        // POSITIVE CONTROL. Prove the server really is holding an attach before
        // asserting that it lets one go, or a server that never counted the
        // session would pass the real check for the wrong reason.
        let attached = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if server_state.tunnel_sessions.stats().await.active_attaches > 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(
            attached.is_ok(),
            "the server never counted the quiet session as attached, so this test \
             could not observe it detaching either"
        );

        drop(dial);

        // Bounded well under the 40s stall and well over the ~53ms healthy case,
        // so this fails on the defect and does not flake on a slow machine.
        let detached = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if server_state.tunnel_sessions.stats().await.active_attaches == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(
            detached.is_ok(),
            "the server still counts the session as attached 20s after the client \
             dropped its local end; a finished session pins the server's endpoint"
        );

        client.shutdown().await?;
        server.shutdown().await?;
        Ok(())
    }

    #[test]
    fn explicit_acl_names_match_every_builtin_gate_name() {
        let mapped = [
            service_name_for_alpn(shell::SHELL_ALPN),
            service_name_for_alpn(exec::EXEC_ALPN),
            service_name_for_alpn(SYNC_ALPN),
            service_name_for_alpn(BUILTIN_ECHO_ALPN),
            service_name_for_alpn(crate::sendfile::SEND_FILE_ALPN),
        ];
        assert_eq!(mapped, BUILTIN_SERVICE_NAMES.map(str::to_string));
        assert_eq!(
            service_name_for_alpn(shell::RESUMABLE_SHELL_ALPN),
            SHELL_SERVICE,
            "both shell wire versions must use one permission name"
        );
    }

    /// Finding 9 of the 2026-08-29 review. When the OS network monitor stops,
    /// the rehome loop must PARK, not return. `serve()` runs every background
    /// loop in one `select!` and shuts the daemon down when the first one
    /// returns, so a loop that returns `Ok` on "the monitor went away" exits the
    /// whole daemon with code 0 — which neither supervisor restarts.
    ///
    /// The test drives the loop with an update source that reports the monitor
    /// stopped, and asserts the loop does not return until the daemon is
    /// cancelled. Without the fix the loop returns `Ok` at once and this fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stopped_network_monitor_parks_the_loop_instead_of_ending_the_daemon()
    -> Result<()> {
        struct MonitorStopped;
        impl InterfaceUpdates for MonitorStopped {
            async fn next_update(&mut self) -> Result<netwatch::interfaces::State> {
                anyhow::bail!("the OS network monitor stopped")
            }
        }

        let dir = tempfile::tempdir()?;
        let cancel = CancellationToken::new();
        let state =
            DaemonState::new(FabricHome::new(dir.path()), cancel.clone(), DaemonOptions::default())
                .await?;

        let loop_task = tokio::spawn(run_rehome_updates(state.clone(), MonitorStopped));

        // Long enough for the loop to hit the monitor-stopped branch several
        // times over. With the bug it has already returned by now.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            !loop_task.is_finished(),
            "the rehome loop returned after the monitor stopped; serve() would \
             treat that as a clean shutdown and the daemon would exit with code \
             0, which no supervisor restarts"
        );

        // And it unwinds cleanly once the daemon really is cancelled, so the
        // fix does not turn a lost monitor into a loop that never stops.
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), loop_task)
            .await
            .expect("the loop did not return after the daemon was cancelled")??;
        Ok(())
    }
}
