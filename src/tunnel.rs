use std::{
    collections::{HashMap, VecDeque},
    fmt,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use iroh::{
    EndpointAddr, EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixStream},
    process::{ChildStderr, Command},
    sync::{Mutex, Notify, watch},
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{FabricHome, PeerBook},
    daemon::CurrentEndpoint,
    mux::{MuxStream, PeerConnections, StreamActivity},
    shell,
};

// Resumable byte tunnel used by generic `fabric dial` sockets. Each local Unix
// connection gets one session id; reconnecting iroh attaches replay unacked
// chunks and preserve the exposed Unix service connection on the accept side.
const MAX_FRAME_LEN: usize = 1024 * 1024;
const LOCAL_READ_BUF: usize = 8192;
const MAX_BUFFERED_BYTES: usize = 4 * 1024 * 1024;
const SERVER_SESSION_REAP_INTERVAL: Duration = Duration::from_secs(60);
/// A new local request must fail before its caller's ordinary five-second
/// timeout. Once a session attached, it retries without this deadline.
const INITIAL_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const HEALTH_PROGRESS_REPORT_INTERVAL: Duration = Duration::from_millis(250);
/// How often a session whose local input has ended asks the kernel whether the
/// consumer is still there. Only such sessions probe, so the cost is one
/// zero-length write per second per half-closed or abandoned session.
const LOCAL_ENDPOINT_PROBE_INTERVAL: Duration = Duration::from_secs(1);

const FRAME_HELLO: u8 = 1;
const FRAME_DATA: u8 = 2;
const FRAME_ACK: u8 = 3;
const FRAME_CLOSE: u8 = 4;
const FRAME_ERROR: u8 = 5;

pub type LocalRead = Box<dyn AsyncRead + Send + Unpin + 'static>;
pub type LocalWrite = Box<dyn AsyncWrite + Send + Unpin + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerSessionLimits {
    pub max_total: usize,
    pub max_per_peer: usize,
}

#[derive(Debug, Clone)]
pub enum ServerTarget {
    UnixSocket(PathBuf),
    Tcp {
        addr: String,
    },
    Exec {
        argv: Vec<String>,
        limit: Arc<ExecLimit>,
    },
    Shell {
        allowed: bool,
    },
}

#[derive(Debug)]
pub enum ClientConnectionEvent {
    Reconnecting {
        attempt: u64,
        delay: Duration,
        error: String,
    },
    Resumed,
    Failed {
        error: String,
    },
}

type NoticeEncoder = dyn Fn(&ClientConnectionEvent) -> Option<Vec<u8>> + Send + Sync + 'static;

/// How many outbound tunnel sessions are attached to a transport right now.
///
/// Sessions this daemon SERVES live in a store that can be counted directly.
/// Sessions it holds OUT do not: they live in the client attach loop and are
/// owned by whoever called it. Without this gauge the endpoint-recycle guard is
/// blind to exactly the sessions a user is most likely to notice, their own, and
/// a recycle tears down a working shell while the guard reports nothing attached.
///
/// This counts ATTACHED, not merely alive. A session stuck reconnecting because
/// the local endpoint is broken must NOT hold off the recycle that would fix it.
#[derive(Debug, Default)]
pub struct ClientAttachGauge {
    attached: AtomicUsize,
}

impl ClientAttachGauge {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn attached(&self) -> usize {
        self.attached.load(Ordering::SeqCst)
    }
}

/// Holds the gauge up while an attach is live AND its local input is still open.
///
/// Dropped by whichever comes first: the attach ending, or the local input
/// reaching EOF. The second case is the one that matters. A remote teardown can
/// stall — measured at over 40 seconds on Linux while the server still reported
/// the session attached — and the endpoint-recycle guard must not be pinned for
/// that whole time by a client that has stopped sending.
#[derive(Debug)]
struct ClientAttachGuard(Arc<ClientAttachGauge>);

impl ClientAttachGuard {
    fn new(gauge: Arc<ClientAttachGauge>) -> Self {
        gauge.attached.fetch_add(1, Ordering::SeqCst);
        Self(gauge)
    }
}

impl Drop for ClientAttachGuard {
    fn drop(&mut self) {
        self.0.attached.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct ClientConnectionNotices {
    encode: Arc<NoticeEncoder>,
    gauge: Option<Arc<ClientAttachGauge>>,
}

impl ClientConnectionNotices {
    pub fn new(
        encode: impl Fn(&ClientConnectionEvent) -> Option<Vec<u8>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            encode: Arc::new(encode),
            gauge: None,
        }
    }

    /// What this connection would write into the local stream for `event`, so a
    /// caller that must not inject bytes can prove it does not.
    pub fn encode_for_test(&self, event: &ClientConnectionEvent) -> Option<Vec<u8>> {
        (self.encode)(event)
    }

    /// Count this connection's attaches so the endpoint-recycle guard can see
    /// outbound sessions.
    pub fn with_gauge(mut self, gauge: Arc<ClientAttachGauge>) -> Self {
        self.gauge = Some(gauge);
        self
    }

    async fn emit(&self, session: &TunnelSession, event: ClientConnectionEvent) {
        if let Some(bytes) = (self.encode)(&event) {
            let _ = session.write_local_notice(&bytes).await;
        }
    }
}

#[derive(Debug)]
pub struct ExecLimit {
    max_children: usize,
    active_children: AtomicUsize,
}

impl ExecLimit {
    pub fn new(max_children: usize) -> Arc<Self> {
        Arc::new(Self {
            max_children,
            active_children: AtomicUsize::new(0),
        })
    }

    fn try_acquire(self: &Arc<Self>) -> Option<ExecPermit> {
        let mut active = self.active_children.load(Ordering::SeqCst);
        loop {
            if active >= self.max_children {
                return None;
            }
            match self.active_children.compare_exchange(
                active,
                active + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(ExecPermit {
                        limit: self.clone(),
                    });
                }
                Err(current) => active = current,
            }
        }
    }

    pub fn max_children(&self) -> usize {
        self.max_children
    }

    pub fn active_children(&self) -> usize {
        self.active_children.load(Ordering::SeqCst)
    }
}

struct ExecPermit {
    limit: Arc<ExecLimit>,
}

impl Drop for ExecPermit {
    fn drop(&mut self) {
        self.limit.active_children.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TunnelSessionId([u8; 16]);

impl TunnelSessionId {
    pub fn random() -> Self {
        Self(rand::random())
    }

    fn from_slice(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 16 {
            bail!("invalid tunnel session id length {}", bytes.len());
        }
        let mut id = [0; 16];
        id.copy_from_slice(bytes);
        Ok(Self(id))
    }
}

impl fmt::Display for TunnelSessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Frame {
    Hello {
        session_id: TunnelSessionId,
        recv_next: u64,
        resume: bool,
    },
    Data {
        offset: u64,
        bytes: Vec<u8>,
    },
    Ack {
        recv_next: u64,
    },
    Close {
        offset: u64,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone)]
struct BufferedChunk {
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct TunnelState {
    send_next: u64,
    send_acked: u64,
    recv_next: u64,
    send_buffer: VecDeque<BufferedChunk>,
    buffered_bytes: usize,
    send_closed: Option<u64>,
    remote_closed: bool,
    local_write_closed: bool,
    pending_remote_close: Option<u64>,
    active_attaches: usize,
    /// Live while this session's attach is up and its local input is still open.
    /// Released early on local-input EOF; see ClientAttachGuard.
    attach_gauge: Option<ClientAttachGuard>,
    last_detached: Option<Instant>,
    reconnect_attempts: u64,
    last_error: Option<String>,
    ever_attached: bool,
}

pub struct TunnelSession {
    id: TunnelSessionId,
    peer_id: EndpointId,
    local_write: Mutex<Option<LocalWrite>>,
    cleanup: Mutex<Option<SessionCleanup>>,
    state: Mutex<TunnelState>,
    notify: Notify,
    done: CancellationToken,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ServerSessionStats {
    pub total_sessions: usize,
    pub active_sessions: usize,
    pub detached_sessions: usize,
    pub complete_sessions: usize,
    pub done_sessions: usize,
    pub active_attaches: usize,
    pub buffered_bytes: usize,
    pub buffered_chunks: usize,
    pub sessions_with_buffered_data: usize,
    pub sessions_with_cleanup: usize,
    pub sessions_with_reconnect_error: usize,
    pub sessions_with_pending_remote_close: usize,
    pub reconnect_attempts_total: u64,
}

#[derive(Debug, Clone, Copy)]
struct TunnelSessionStats {
    active_attaches: usize,
    detached: bool,
    complete: bool,
    done: bool,
    buffered_bytes: usize,
    buffered_chunks: usize,
    has_cleanup: bool,
    reconnect_attempts: u64,
    has_reconnect_error: bool,
    has_pending_remote_close: bool,
}

#[derive(Debug)]
struct SessionCleanup {
    kill: CancellationToken,
}

#[derive(Debug)]
struct ExpiredResumeError {
    session_id: TunnelSessionId,
}

impl fmt::Display for ExpiredResumeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "server tunnel session {} expired", self.session_id)
    }
}

impl std::error::Error for ExpiredResumeError {}

impl fmt::Debug for TunnelSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunnelSession")
            .field("id", &self.id)
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

impl TunnelSession {
    pub fn new(
        id: TunnelSessionId,
        peer_id: EndpointId,
        local: UnixStream,
    ) -> (Arc<Self>, LocalRead) {
        let (read, write) = local.into_split();
        Self::new_parts(id, peer_id, Box::new(read), Box::new(write))
    }

    pub fn new_parts(
        id: TunnelSessionId,
        peer_id: EndpointId,
        read: LocalRead,
        write: LocalWrite,
    ) -> (Arc<Self>, LocalRead) {
        Self::new_parts_with_cleanup(id, peer_id, read, write, None)
    }

    fn new_parts_with_cleanup(
        id: TunnelSessionId,
        peer_id: EndpointId,
        read: LocalRead,
        write: LocalWrite,
        cleanup: Option<SessionCleanup>,
    ) -> (Arc<Self>, LocalRead) {
        let session = Arc::new(Self {
            id,
            peer_id,
            local_write: Mutex::new(Some(write)),
            cleanup: Mutex::new(cleanup),
            state: Mutex::new(TunnelState {
                send_next: 0,
                send_acked: 0,
                recv_next: 0,
                send_buffer: VecDeque::new(),
                buffered_bytes: 0,
                send_closed: None,
                remote_closed: false,
                local_write_closed: false,
                pending_remote_close: None,
                active_attaches: 0,
                attach_gauge: None,
                last_detached: None,
                reconnect_attempts: 0,
                last_error: None,
                ever_attached: false,
            }),
            notify: Notify::new(),
            done: CancellationToken::new(),
        });
        (session, read)
    }

    pub fn id(&self) -> TunnelSessionId {
        self.id
    }

    pub fn peer_id(&self) -> EndpointId {
        self.peer_id
    }

    pub async fn recv_next(&self) -> u64 {
        self.state.lock().await.recv_next
    }

    async fn has_attached(&self) -> bool {
        self.state.lock().await.ever_attached
    }

    pub async fn is_complete(&self) -> bool {
        let state = self.state.lock().await;
        state.send_closed.is_some()
            && state.remote_closed
            && state.send_buffer.is_empty()
            && state.send_acked >= state.send_next
    }

    async fn stats(&self) -> TunnelSessionStats {
        let state = self.state.lock().await;
        let complete = state.send_closed.is_some()
            && state.remote_closed
            && state.send_buffer.is_empty()
            && state.send_acked >= state.send_next;
        let active_attaches = state.active_attaches;
        let detached =
            active_attaches == 0 && state.last_detached.is_some() && !self.done.is_cancelled();
        let buffered_bytes = state.buffered_bytes;
        let buffered_chunks = state.send_buffer.len();
        let reconnect_attempts = state.reconnect_attempts;
        let has_reconnect_error = state.last_error.is_some();
        let has_pending_remote_close = state.pending_remote_close.is_some();
        drop(state);

        TunnelSessionStats {
            active_attaches,
            detached,
            complete,
            done: self.done.is_cancelled(),
            buffered_bytes,
            buffered_chunks,
            has_cleanup: self.cleanup.lock().await.is_some(),
            reconnect_attempts,
            has_reconnect_error,
            has_pending_remote_close,
        }
    }

    async fn detached_at(&self) -> Option<Instant> {
        let state = self.state.lock().await;
        if state.active_attaches > 0 || self.done.is_cancelled() {
            return None;
        }
        state.last_detached
    }

    pub async fn record_reconnect_attempt(&self, error: Option<String>) -> u64 {
        let mut state = self.state.lock().await;
        state.reconnect_attempts += 1;
        state.last_error = error;
        state.reconnect_attempts
    }

    pub async fn clear_reconnect_error(&self) {
        self.state.lock().await.last_error = None;
    }

    async fn write_local_notice(&self, bytes: &[u8]) -> Result<()> {
        let mut write = self.local_write.lock().await;
        let Some(write) = write.as_mut() else {
            bail!("tunnel {} local write is closed", self.id);
        };
        write.write_all(bytes).await?;
        write.flush().await?;
        Ok(())
    }

    async fn begin_attach(&self) -> Result<()> {
        if self.done.is_cancelled() {
            bail!("tunnel session {} is closed", self.id);
        }
        let mut state = self.state.lock().await;
        if self.done.is_cancelled() {
            bail!("tunnel session {} is closed", self.id);
        }
        state.active_attaches += 1;
        state.last_detached = None;
        state.ever_attached = true;
        Ok(())
    }

    async fn end_attach(&self) {
        let mut state = self.state.lock().await;
        state.active_attaches = state.active_attaches.saturating_sub(1);
        if state.active_attaches == 0 {
            state.last_detached = Some(Instant::now());
        }
        self.notify.notify_waiters();
    }

    pub async fn run_local_reader(self: Arc<Self>, mut read: LocalRead) -> Result<()> {
        let mut buf = [0; LOCAL_READ_BUF];
        loop {
            self.wait_for_buffer_space().await;
            let read = match read.read(&mut buf).await {
                Ok(read) => read,
                Err(error) => {
                    // An abrupt local close ends local input just as surely as a
                    // clean EOF does: no further bytes can arrive on this session.
                    // So end the send side the same way, which both releases the
                    // recycle guard and records the final offset.
                    //
                    // Recording the offset is the part that matters remotely. Only
                    // a recorded close makes the writer emit `Frame::Close`, and
                    // only that frame lets the server stop counting this session
                    // as attached. Without it the server waits for bytes that can
                    // never arrive.
                    //
                    // Dropping a local socket yields a clean zero-length read on
                    // macOS and an error on Linux, so this path used to run only
                    // on Linux. That is the whole of the platform difference: the
                    // 40-second stall in issue 32 was never a slow path, it was a
                    // message the server never received.
                    //
                    // Only the send side closes. This session may still be writing
                    // queued remote output, and a half-close is not a close.
                    self.mark_send_closed().await;
                    return Err(error.into());
                }
            };
            if read == 0 {
                self.mark_send_closed().await;
                return Ok(());
            }
            self.push_local_data(buf[..read].to_vec()).await;
        }
    }

    async fn wait_for_buffer_space(&self) {
        loop {
            // `notify_waiters` does not keep a permit. Create the waiter before
            // checking the state so an acknowledgement cannot drain the buffer
            // between the check and waiter creation, then leave this reader
            // asleep until some unrelated session change.
            let changed = self.notify.notified();
            {
                let state = self.state.lock().await;
                if state.buffered_bytes < MAX_BUFFERED_BYTES || state.send_closed.is_some() {
                    return;
                }
            }
            changed.await;
        }
    }

    async fn push_local_data(&self, bytes: Vec<u8>) {
        let mut state = self.state.lock().await;
        if state.send_closed.is_some() {
            return;
        }
        let offset = state.send_next;
        state.send_next += bytes.len() as u64;
        state.buffered_bytes += bytes.len();
        state.send_buffer.push_back(BufferedChunk { offset, bytes });
        drop(state);
        self.notify.notify_waiters();
    }

    async fn mark_send_closed(&self) {
        let mut state = self.state.lock().await;
        if state.send_closed.is_none() {
            state.send_closed = Some(state.send_next);
        }
        // The local side will send nothing further, so this session must stop
        // holding off an endpoint recycle. It may still be READING queued remote
        // output — a half-close is not a close — and that keeps working: only the
        // recycle guard is released here, the session and its replay are untouched.
        state.attach_gauge = None;
        drop(state);
        self.notify.notify_waiters();
    }

    /// Hold the recycle guard for this attach, unless the local input has already
    /// finished, in which case there is nothing left to protect from a recycle.
    async fn hold_attach_gauge(&self, gauge: Option<Arc<ClientAttachGauge>>) {
        let Some(gauge) = gauge else {
            return;
        };
        let mut state = self.state.lock().await;
        if state.send_closed.is_none() {
            state.attach_gauge = Some(ClientAttachGuard::new(gauge));
        }
    }

    async fn release_attach_gauge(&self) {
        let mut state = self.state.lock().await;
        state.attach_gauge = None;
    }

    async fn apply_peer_ack(&self, recv_next: u64) -> bool {
        let mut state = self.state.lock().await;
        let previous = state.send_acked;
        if recv_next > state.send_acked {
            state.send_acked = recv_next.min(state.send_next);
            drop_acked_chunks(&mut state);
        }
        let advanced = state.send_acked > previous;
        drop(state);
        self.notify.notify_waiters();
        advanced
    }

    async fn accept_data(&self, offset: u64, bytes: Vec<u8>) -> Result<bool> {
        let bytes = {
            let state = self.state.lock().await;
            if offset > state.recv_next {
                bail!(
                    "tunnel {} received out-of-order data at offset {offset}, expected {}",
                    self.id,
                    state.recv_next
                );
            }
            let already_have = (state.recv_next - offset) as usize;
            if already_have >= bytes.len() {
                drop(state);
                self.notify.notify_waiters();
                return Ok(false);
            }
            bytes[already_have..].to_vec()
        };

        let write_failed = {
            let mut write = self.local_write.lock().await;
            let Some(write) = write.as_mut() else {
                bail!("tunnel {} local write is closed", self.id);
            };
            match write.write_all(&bytes).await {
                Ok(()) => write.flush().await.err(),
                Err(error) => Some(error),
            }
        };
        if let Some(error) = write_failed {
            // Nobody is holding the other end of the local socket any more. End
            // the send side for the same reason an abrupt local read close does:
            // only a recorded close makes the writer emit `Frame::Close`, which
            // is what lets the server stop counting this session as attached.
            self.mark_send_closed().await;
            return Err(LocalEndpointGone {
                id: self.id,
                source: error,
            }
            .into());
        }

        let close_now = {
            let mut state = self.state.lock().await;
            state.recv_next += bytes.len() as u64;
            state
                .pending_remote_close
                .is_some_and(|offset| offset <= state.recv_next)
                && !state.local_write_closed
        };
        if close_now {
            self.shutdown_local_write().await?;
        }
        self.notify.notify_waiters();
        Ok(!bytes.is_empty())
    }

    async fn accept_remote_close(&self, offset: u64) -> Result<()> {
        let close_now = {
            let mut state = self.state.lock().await;
            state.remote_closed = true;
            if offset <= state.recv_next {
                !state.local_write_closed
            } else {
                state.pending_remote_close = Some(offset);
                false
            }
        };
        if close_now {
            self.shutdown_local_write().await?;
        }
        self.notify.notify_waiters();
        Ok(())
    }

    async fn shutdown_local_write(&self) -> Result<()> {
        {
            let mut state = self.state.lock().await;
            if state.local_write_closed {
                return Ok(());
            }
            state.local_write_closed = true;
        }
        let mut write = self.local_write.lock().await;
        if let Some(mut write) = write.take() {
            let _ = write.shutdown().await;
        }
        Ok(())
    }

    async fn local_input_ended(&self) -> bool {
        self.state.lock().await.send_closed.is_some()
    }

    /// Ask the kernel whether anybody still holds the other end of the local
    /// socket. `Ok` means somebody does, or that the question does not apply
    /// yet; `Err` is a typed `LocalEndpointGone`.
    ///
    /// Remote output used to be the only thing that could discover a consumer
    /// had gone: the write failed, and that failure ended the session (issue
    /// 51). A session that never reaches its peer never has output, so it
    /// never discovered anything. It retried for ever and held its dial permit
    /// the whole time, and thirty-two of them stopped every shell, exec and
    /// dial on the machine while status and ping stayed green.
    ///
    /// A zero-length write reaches the kernel and fails with `EPIPE` once the
    /// consumer has closed BOTH directions. A half-close, where the consumer
    /// sent everything and is waiting for output, leaves the socket writable,
    /// so it returns `Ok`. That is exactly the distinction needed: a consumer
    /// that is waiting is served, a consumer that has left is not retried for.
    ///
    /// Gated on the local input having ended, because until the reader sees
    /// EOF there is a consumer by definition, and the reader is the cheaper
    /// instrument.
    pub async fn probe_local_endpoint(&self) -> Result<()> {
        if !self.local_input_ended().await {
            return Ok(());
        }
        let failed = {
            let mut write = self.local_write.lock().await;
            let Some(write) = write.as_mut() else {
                return Ok(());
            };
            write.write(&[]).await.err()
        };
        let Some(source) = failed else {
            return Ok(());
        };
        self.mark_send_closed().await;
        Err(LocalEndpointGone {
            id: self.id,
            source,
        }
        .into())
    }

    /// Resolve only when the local consumer has gone. Meant as a `select!` arm
    /// beside whatever the session is otherwise waiting on.
    async fn watch_local_endpoint(&self) -> anyhow::Error {
        loop {
            tokio::time::sleep(LOCAL_ENDPOINT_PROBE_INTERVAL).await;
            if let Err(error) = self.probe_local_endpoint().await {
                return error;
            }
        }
    }

    pub async fn abort_local(&self) -> Result<()> {
        self.done.cancel();
        self.shutdown_local_write().await
    }

    async fn close(&self) {
        self.done.cancel();
        let _ = self.shutdown_local_write().await;
        if let Some(cleanup) = self.cleanup.lock().await.take() {
            cleanup.kill.cancel();
        }
    }

    pub async fn close_for_eviction(&self) {
        self.close().await;
    }

    pub async fn try_expire(&self, ttl: Duration) -> bool {
        if self.done.is_cancelled() || self.is_complete().await {
            self.close().await;
            return true;
        }
        {
            let state = self.state.lock().await;
            if state.active_attaches > 0 {
                return false;
            }
            let Some(detached) = state.last_detached else {
                return false;
            };
            if detached.elapsed() < ttl {
                return false;
            }
        }

        self.close().await;
        true
    }

    async fn run_attach(
        self: Arc<Self>,
        send: SendStream,
        recv: RecvStream,
        peer_recv_next: u64,
        health: Option<AttachConnectionHealth>,
    ) -> Result<()> {
        self.begin_attach().await?;
        self.apply_peer_ack(peer_recv_next).await;

        let result = async {
            let mut writer = tokio::spawn(write_attach_loop(self.clone(), send));
            let mut reader = tokio::spawn(read_attach_loop(self.clone(), recv, health));

            tokio::select! {
                result = &mut writer => {
                    reader.abort();
                    result?
                }
                result = &mut reader => {
                    writer.abort();
                    result?
                }
            }
        }
        .await;

        self.end_attach().await;
        result
    }
}

fn drop_acked_chunks(state: &mut TunnelState) {
    while let Some(front) = state.send_buffer.front_mut() {
        let end = front.offset + front.bytes.len() as u64;
        if end <= state.send_acked {
            let bytes = state.send_buffer.pop_front().expect("front checked").bytes;
            state.buffered_bytes = state.buffered_bytes.saturating_sub(bytes.len());
            continue;
        }
        if front.offset < state.send_acked {
            let delta = (state.send_acked - front.offset) as usize;
            front.bytes.drain(..delta);
            front.offset = state.send_acked;
            state.buffered_bytes = state.buffered_bytes.saturating_sub(delta);
        }
        break;
    }
}

async fn write_attach_loop(session: Arc<TunnelSession>, mut send: SendStream) -> Result<()> {
    let mut data_sent_until = {
        let state = session.state.lock().await;
        state.send_acked
    };
    let mut last_ack_sent = None;
    let mut close_sent = None;

    loop {
        // `notify_waiters` wakes futures created before the call, even when the
        // future has not been polled. Create this waiter before the state
        // snapshot. Otherwise local data can arrive after the snapshot and
        // before `select!` creates its waiter, leaving the writer asleep with
        // bytes ready to send.
        let changed = session.notify.notified();
        let (ack, data, close, complete) = {
            let state = session.state.lock().await;
            let ack = (last_ack_sent != Some(state.recv_next)).then_some(state.recv_next);
            let start = data_sent_until.max(state.send_acked);
            let data = chunks_from(&state.send_buffer, start);
            let new_data_sent_until = data
                .last()
                .map(|chunk| chunk.offset + chunk.bytes.len() as u64)
                .unwrap_or(start);
            data_sent_until = new_data_sent_until;
            let close = state
                .send_closed
                .filter(|offset| close_sent != Some(*offset) && data_sent_until >= *offset);
            let complete = state.send_closed.is_some()
                && state.remote_closed
                && state.send_buffer.is_empty()
                && state.send_acked >= state.send_next;
            (ack, data, close, complete)
        };

        if let Some(recv_next) = ack {
            write_frame(&mut send, Frame::Ack { recv_next }).await?;
            last_ack_sent = Some(recv_next);
        }
        for chunk in data {
            write_frame(
                &mut send,
                Frame::Data {
                    offset: chunk.offset,
                    bytes: chunk.bytes,
                },
            )
            .await?;
        }
        if let Some(offset) = close {
            write_frame(&mut send, Frame::Close { offset }).await?;
            close_sent = Some(offset);
        }
        if complete {
            let _ = send.finish();
            return Ok(());
        }

        tokio::select! {
            _ = changed => {}
            _ = session.done.cancelled() => return Ok(()),
            // A silent remote never writes, so a write failure can never tell
            // this loop the consumer left. Ask instead.
            error = session.watch_local_endpoint() => return Err(error),
        }
    }
}

fn chunks_from(buffer: &VecDeque<BufferedChunk>, start: u64) -> Vec<BufferedChunk> {
    let mut chunks = Vec::new();
    for chunk in buffer {
        let end = chunk.offset + chunk.bytes.len() as u64;
        if end <= start {
            continue;
        }
        if chunk.offset < start {
            let delta = (start - chunk.offset) as usize;
            chunks.push(BufferedChunk {
                offset: start,
                bytes: chunk.bytes[delta..].to_vec(),
            });
        } else {
            chunks.push(chunk.clone());
        }
    }
    chunks
}

async fn read_attach_loop(
    session: Arc<TunnelSession>,
    mut recv: RecvStream,
    mut health: Option<AttachConnectionHealth>,
) -> Result<()> {
    while let Some(frame) = read_frame(&mut recv).await? {
        match frame {
            Frame::Hello { .. } => bail!("unexpected tunnel hello after attach"),
            Frame::Data { offset, bytes } => {
                if session.accept_data(offset, bytes).await?
                    && let Some(health) = &mut health
                {
                    health.note_application_progress().await;
                }
            }
            Frame::Ack { recv_next } => {
                if session.apply_peer_ack(recv_next).await
                    && let Some(health) = &mut health
                {
                    health.note_application_progress().await;
                }
            }
            Frame::Close { offset } => session.accept_remote_close(offset).await?,
            Frame::Error { message } => bail!("tunnel peer error: {message}"),
        }
    }
    bail!("tunnel attach stream closed")
}

#[derive(Debug)]
struct Backoff {
    step: usize,
}

impl Backoff {
    fn new() -> Self {
        Self { step: 0 }
    }

    fn reset(&mut self) {
        self.step = 0;
    }

    fn next_delay(&mut self) -> Duration {
        const STEPS_MS: &[u64] = &[100, 250, 500, 1000, 2000, 5000, 10000, 15000];
        let base = STEPS_MS[self.step.min(STEPS_MS.len() - 1)];
        self.step = (self.step + 1).min(STEPS_MS.len() - 1);
        let jitter = 80 + (rand::random::<u64>() % 41);
        Duration::from_millis(base * jitter / 100)
    }
}

pub async fn run_client_connection(
    local: UnixStream,
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
    connections: Arc<PeerConnections>,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
    notices: Option<ClientConnectionNotices>,
) -> Result<()> {
    let (read, write) = local.into_split();
    run_client_connection_parts(
        Box::new(read),
        Box::new(write),
        endpoint_rx,
        connections,
        home,
        peer,
        alpn,
        cancel,
        drop_rx,
        notices,
        None,
    )
    .await
}

pub async fn run_client_connection_with_initial(
    local: UnixStream,
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
    connections: Arc<PeerConnections>,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
    notices: Option<ClientConnectionNotices>,
    initial_stream: MuxStream,
) -> Result<()> {
    let (read, write) = local.into_split();
    run_client_connection_parts(
        Box::new(read),
        Box::new(write),
        endpoint_rx,
        connections,
        home,
        peer,
        alpn,
        cancel,
        drop_rx,
        notices,
        Some(initial_stream),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_client_tcp_connection(
    local: TcpStream,
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
    connections: Arc<PeerConnections>,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
    notices: Option<ClientConnectionNotices>,
) -> Result<()> {
    let (read, write) = local.into_split();
    run_client_connection_parts(
        Box::new(read),
        Box::new(write),
        endpoint_rx,
        connections,
        home,
        peer,
        alpn,
        cancel,
        drop_rx,
        notices,
        None,
    )
    .await
}

async fn run_client_connection_parts(
    local_read: LocalRead,
    local_write: LocalWrite,
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
    connections: Arc<PeerConnections>,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
    notices: Option<ClientConnectionNotices>,
    initial_stream: Option<MuxStream>,
) -> Result<()> {
    let peer_id = PeerBook::load(&home)?.resolve(&peer)?.id;
    let session_id = TunnelSessionId::random();
    let (session, local_read) =
        TunnelSession::new_parts(session_id, peer_id, local_read, local_write);
    let reader = tokio::spawn(session.clone().run_local_reader(local_read));
    let result = run_client_attach_loop(
        session.clone(),
        endpoint_rx,
        connections,
        home,
        peer,
        alpn,
        cancel,
        drop_rx,
        notices,
        initial_stream,
    )
    .await;
    reader.abort();
    let _ = reader.await;
    result
}

async fn run_client_attach_loop(
    session: Arc<TunnelSession>,
    mut endpoint_rx: watch::Receiver<CurrentEndpoint>,
    connections: Arc<PeerConnections>,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    mut drop_rx: watch::Receiver<u64>,
    notices: Option<ClientConnectionNotices>,
    mut initial_stream: Option<MuxStream>,
) -> Result<()> {
    let mut backoff = Backoff::new();

    loop {
        if session.is_complete().await {
            return Ok(());
        }

        let result = if let Some(stream) = initial_stream.take() {
            attach_stream(
                session.clone(),
                stream,
                connections.clone(),
                drop_rx.clone(),
                notices.as_ref(),
                &mut backoff,
            )
            .await
        } else {
            let peer_addr = resolve_peer_for_attempt(&home, &peer, session.peer_id()).await;
            let endpoint = endpoint_rx.borrow().clone();
            connect_and_attach(
                session.clone(),
                endpoint,
                peer_addr,
                &alpn,
                connections.clone(),
                drop_rx.clone(),
                notices.as_ref(),
                &mut backoff,
            )
            .await
        };

        match result {
            Ok(()) if session.is_complete().await => return Ok(()),
            Ok(()) => {
                let attempt = session
                    .record_reconnect_attempt(Some("tunnel attach ended".to_string()))
                    .await;
                let delay = backoff.next_delay();
                if let Some(notices) = notices.as_ref() {
                    notices
                        .emit(
                            &session,
                            ClientConnectionEvent::Reconnecting {
                                attempt,
                                delay,
                                error: "transport attach ended".to_string(),
                            },
                        )
                        .await;
                }
                match wait_for_reconnect(delay, &cancel, &session, &mut drop_rx, &mut endpoint_rx)
                    .await
                {
                    ReconnectWait::Retry => continue,
                    ReconnectWait::Stop => return Ok(()),
                    ReconnectWait::LocalGone(error) => {
                        return fail_permanently(&session, notices.as_ref(), error).await;
                    }
                }
            }
            Err(error) => {
                let message = format!("{error:#}");
                if !session.has_attached().await || is_permanent_failure(&error) {
                    return fail_permanently(&session, notices.as_ref(), error).await;
                }
                let attempt = session
                    .record_reconnect_attempt(Some(message.clone()))
                    .await;
                let delay = backoff.next_delay();
                if let Some(notices) = notices.as_ref() {
                    notices
                        .emit(
                            &session,
                            ClientConnectionEvent::Reconnecting {
                                attempt,
                                delay,
                                error: message,
                            },
                        )
                        .await;
                }
                match wait_for_reconnect(delay, &cancel, &session, &mut drop_rx, &mut endpoint_rx)
                    .await
                {
                    ReconnectWait::Retry => continue,
                    ReconnectWait::Stop => return Ok(()),
                    ReconnectWait::LocalGone(error) => {
                        return fail_permanently(&session, notices.as_ref(), error).await;
                    }
                }
            }
        }
    }
}

/// End a session the retry loop must not retry: say so, close the local side,
/// and hand the typed error up so the caller can classify it too.
async fn fail_permanently(
    session: &TunnelSession,
    notices: Option<&ClientConnectionNotices>,
    error: anyhow::Error,
) -> Result<()> {
    if let Some(notices) = notices {
        notices
            .emit(
                session,
                ClientConnectionEvent::Failed {
                    error: format!("{error:#}"),
                },
            )
            .await;
    }
    session.abort_local().await?;
    Err(error)
}

enum ReconnectWait {
    Retry,
    Stop,
    /// The consumer closed its socket while this session waited to retry.
    /// Nothing a reconnect could do is for anybody now.
    LocalGone(anyhow::Error),
}

async fn wait_for_reconnect(
    delay: Duration,
    cancel: &CancellationToken,
    session: &TunnelSession,
    drop_rx: &mut watch::Receiver<u64>,
    endpoint_rx: &mut watch::Receiver<CurrentEndpoint>,
) -> ReconnectWait {
    tokio::select! {
        _ = tokio::time::sleep(delay) => ReconnectWait::Retry,
        _ = cancel.cancelled() => ReconnectWait::Stop,
        _ = session.done.cancelled() => ReconnectWait::Stop,
        changed = drop_rx.changed() => if changed.is_ok() { ReconnectWait::Retry } else { ReconnectWait::Stop },
        changed = endpoint_rx.changed() => if changed.is_ok() { ReconnectWait::Retry } else { ReconnectWait::Stop },
        error = session.watch_local_endpoint() => ReconnectWait::LocalGone(error),
    }
}

async fn resolve_peer_for_attempt(
    home: &FabricHome,
    peer: &str,
    fallback_id: EndpointId,
) -> EndpointAddr {
    PeerBook::load(home)
        .and_then(|book| book.resolve(peer))
        .unwrap_or_else(|_| EndpointAddr::new(fallback_id))
}

async fn connect_and_attach(
    session: Arc<TunnelSession>,
    endpoint: CurrentEndpoint,
    peer_addr: EndpointAddr,
    alpn: &[u8],
    connections: Arc<PeerConnections>,
    drop_rx: watch::Receiver<u64>,
    notices: Option<&ClientConnectionNotices>,
    backoff: &mut Backoff,
) -> Result<()> {
    // A connect to a peer that is away can take the whole handshake timeout
    // to fail. The consumer may leave during it, and then the rest of the
    // attempt is for nobody.
    let initial = !session.has_attached().await;
    let protocol = std::str::from_utf8(alpn).context("tunnel protocol is not UTF-8")?;
    let stream = tokio::select! {
        connected = connections.open_stream(
            &endpoint.endpoint,
            endpoint.generation,
            &peer_addr,
            protocol,
            StreamActivity::Application,
        ) => {
            connected.with_context(|| {
                if initial {
                    "failed to connect tunnel"
                } else {
                    "failed to reconnect tunnel"
                }
            })?
        }
        _ = tokio::time::sleep(INITIAL_CONNECT_TIMEOUT), if initial => {
            bail!(
                "peer did not accept the initial tunnel within {:?}",
                INITIAL_CONNECT_TIMEOUT
            )
        }
        error = session.watch_local_endpoint() => return Err(error),
    };
    attach_stream(session, stream, connections, drop_rx, notices, backoff).await
}

#[derive(Clone)]
struct AttachConnectionHealth {
    connections: Arc<PeerConnections>,
    peer: EndpointId,
    stable_id: usize,
    last_progress_report: Option<Instant>,
}

impl AttachConnectionHealth {
    fn new(connections: Arc<PeerConnections>, connection: &Connection) -> Self {
        Self {
            connections,
            peer: connection.remote_id(),
            stable_id: connection.stable_id(),
            last_progress_report: None,
        }
    }

    async fn note_application_progress(&mut self) {
        let now = Instant::now();
        if self
            .last_progress_report
            .is_some_and(|last| now.duration_since(last) < HEALTH_PROGRESS_REPORT_INTERVAL)
        {
            return;
        }
        self.last_progress_report = Some(now);
        self.connections
            .note_application_progress(self.peer, self.stable_id)
            .await;
    }

    async fn note_attach_failure(&self, phase: &str, duration: Duration) {
        self.connections
            .note_attach_failure(self.peer, self.stable_id, phase, duration)
            .await;
    }
}

async fn attach_stream(
    session: Arc<TunnelSession>,
    stream: MuxStream,
    connections: Arc<PeerConnections>,
    drop_rx: watch::Receiver<u64>,
    notices: Option<&ClientConnectionNotices>,
    backoff: &mut Backoff,
) -> Result<()> {
    let health = AttachConnectionHealth::new(connections, &stream.connection);
    let started = Instant::now();
    let mut phase = "hello";
    let result = attach_stream_inner(
        session.clone(),
        stream,
        drop_rx,
        notices,
        backoff,
        health.clone(),
        &mut phase,
    )
    .await;
    if !matches!(&result, Err(error) if is_permanent_failure(error))
        && (result.is_err() || !session.is_complete().await)
    {
        health.note_attach_failure(phase, started.elapsed()).await;
    }
    result
}

async fn attach_stream_inner(
    session: Arc<TunnelSession>,
    stream: MuxStream,
    drop_rx: watch::Receiver<u64>,
    notices: Option<&ClientConnectionNotices>,
    backoff: &mut Backoff,
    health: AttachConnectionHealth,
    phase: &mut &'static str,
) -> Result<()> {
    let MuxStream {
        connection,
        mut send,
        mut recv,
    } = stream;
    attach_drop_closer(&connection, drop_rx);

    let resume = session.has_attached().await;
    write_frame(
        &mut send,
        Frame::Hello {
            session_id: session.id(),
            recv_next: session.recv_next().await,
            resume,
        },
    )
    .await?;

    let (session_id, recv_next) = match read_frame(&mut recv).await? {
        Some(Frame::Hello {
            session_id,
            recv_next,
            ..
        }) => (session_id, recv_next),
        Some(Frame::Error { message }) => {
            return Err(ServerRejected(message).into());
        }
        Some(_) | None => bail!("tunnel server did not send hello"),
    };
    if session_id != session.id() {
        bail!("tunnel server replied with wrong session id {session_id}");
    }
    *phase = "attached";
    // A valid Hello proves that the peer accepted this exact durable session.
    // A later drop is a new outage and must not inherit an older outage's delay.
    backoff.reset();
    session.clear_reconnect_error().await;
    if resume && let Some(notices) = notices {
        notices.emit(&session, ClientConnectionEvent::Resumed).await;
    }

    // Held for the attached period, and dropped earlier if the local input
    // finishes first. Stored on the session so local-input EOF can release it
    // without waiting for this attach to return.
    session
        .hold_attach_gauge(notices.and_then(|notices| notices.gauge.clone()))
        .await;
    let result = session
        .clone()
        .run_attach(send, recv, recv_next, Some(health))
        .await;
    // Between attaches nothing is held: a reconnecting session must not block a
    // recycle, since a recycle may be exactly what lets it reconnect.
    session.release_attach_gauge().await;
    result
}

#[derive(Debug)]
struct ServerRejected(String);

impl fmt::Display for ServerRejected {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "tunnel server rejected session: {}", self.0)
    }
}

impl std::error::Error for ServerRejected {}

/// The local endpoint this tunnel exists to serve has gone away.
///
/// Raised when remote output cannot be written to the local socket, which is
/// what a caller abandoning its dial looks like from in here. It is a distinct
/// type rather than a message because the retry loop must be able to tell it
/// apart from a transport drop without matching on prose.
#[derive(Debug)]
struct LocalEndpointGone {
    id: TunnelSessionId,
    source: std::io::Error,
}

impl fmt::Display for LocalEndpointGone {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tunnel {} local endpoint is gone: {}",
            self.id, self.source
        )
    }
}

impl std::error::Error for LocalEndpointGone {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Whether reconnecting could possibly help, or the session is simply over.
///
/// Two shapes are permanent. An endpoint-level rejection is a trust decision the
/// peer will make identically every time, so retrying it forever makes a
/// default-deny shell or exec request hang instead of returning the refusal.
///
/// A dead LOCAL endpoint is permanent for a different and stronger reason: the
/// thing the tunnel exists to serve is gone. No remote peer can repair it, so
/// every further attempt reattaches, receives output nobody will read, fails on
/// the same dead socket, and sleeps. That is issue 51 — the loop cannot end,
/// because a pty never closes its side and `is_complete` waits for a remote
/// close that will never come.
pub(crate) fn is_permanent_failure(error: &anyhow::Error) -> bool {
    if error.downcast_ref::<ServerRejected>().is_some()
        || error.downcast_ref::<LocalEndpointGone>().is_some()
        || crate::mux::is_permanent_stream_denial(error)
    {
        return true;
    }
    let message = format!("{error:#}");
    message.contains("code 403") || message.contains("node is not in fabric allow-list")
}

#[derive(Debug, Clone)]
pub struct ServerSessionStore {
    inner: Arc<Mutex<HashMap<TunnelSessionId, Arc<TunnelSession>>>>,
    limits: ServerSessionLimits,
    detached_ttl: Duration,
}

impl ServerSessionStore {
    pub fn new(limits: ServerSessionLimits, detached_ttl: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            limits,
            detached_ttl,
        }
    }

    async fn get_or_create(
        &self,
        session_id: TunnelSessionId,
        peer_id: EndpointId,
        target: ServerTarget,
        resume: bool,
    ) -> Result<(Arc<TunnelSession>, bool)> {
        self.reap_expired(self.detached_ttl).await;
        if let Some(session) = self.get(session_id).await {
            return Ok((session, false));
        }

        if resume {
            return Err(ExpiredResumeError { session_id }.into());
        }
        self.evict_to_make_room(peer_id).await;
        self.ensure_room_for(peer_id).await?;

        let (session, local_read) = create_server_session(session_id, peer_id, target).await?;
        match self.insert_created(session.clone()).await {
            Ok(None) => {
                tokio::spawn(session.clone().run_local_reader(local_read));
                Ok((session, true))
            }
            Ok(Some(existing)) => {
                session.close_for_eviction().await;
                Ok((existing, false))
            }
            Err(error) => {
                session.close_for_eviction().await;
                Err(error)
            }
        }
    }

    async fn get(&self, session_id: TunnelSessionId) -> Option<Arc<TunnelSession>> {
        self.inner.lock().await.get(&session_id).cloned()
    }

    async fn insert_created(
        &self,
        session: Arc<TunnelSession>,
    ) -> Result<Option<Arc<TunnelSession>>> {
        let mut sessions = self.inner.lock().await;
        if let Some(existing) = sessions.get(&session.id()).cloned() {
            return Ok(Some(existing));
        }

        ensure_room_for_locked(&sessions, self.limits, session.peer_id())?;
        sessions.insert(session.id(), session);
        Ok(None)
    }

    async fn ensure_room_for(&self, peer_id: EndpointId) -> Result<()> {
        let sessions = self.inner.lock().await;
        ensure_room_for_locked(&sessions, self.limits, peer_id)
    }

    async fn evict_to_make_room(&self, peer_id: EndpointId) {
        loop {
            let sessions = self.inner.lock().await;
            let total_full = sessions.len() >= self.limits.max_total;
            let peer_full =
                count_peer_sessions_locked(&sessions, peer_id) >= self.limits.max_per_peer;
            if !total_full && !peer_full {
                return;
            }
            drop(sessions);

            let candidate = if peer_full {
                self.oldest_detached(Some(peer_id)).await
            } else {
                self.oldest_detached(None).await
            };
            let Some((session_id, session)) = candidate else {
                return;
            };

            session.close_for_eviction().await;
            let mut sessions = self.inner.lock().await;
            if sessions
                .get(&session_id)
                .is_some_and(|current| Arc::ptr_eq(current, &session))
            {
                sessions.remove(&session_id);
            }
        }
    }

    async fn remove_new_session(&self, session: &Arc<TunnelSession>) {
        session.close_for_eviction().await;
        let mut sessions = self.inner.lock().await;
        if sessions
            .get(&session.id())
            .is_some_and(|current| Arc::ptr_eq(current, session))
        {
            sessions.remove(&session.id());
        }
    }

    async fn oldest_detached(
        &self,
        peer_id: Option<EndpointId>,
    ) -> Option<(TunnelSessionId, Arc<TunnelSession>)> {
        let current: Vec<Arc<TunnelSession>> = self.inner.lock().await.values().cloned().collect();
        let mut oldest = None;
        for session in current {
            if peer_id.is_some_and(|peer_id| session.peer_id() != peer_id) {
                continue;
            }
            let Some(detached) = session.detached_at().await else {
                continue;
            };
            if oldest.as_ref().is_none_or(
                |(_, oldest_detached, _): &(TunnelSessionId, Instant, Arc<TunnelSession>)| {
                    detached < *oldest_detached
                },
            ) {
                oldest = Some((session.id(), detached, session));
            }
        }
        oldest.map(|(session_id, _, session)| (session_id, session))
    }

    pub async fn reap_expired(&self, ttl: Duration) -> usize {
        let current: Vec<Arc<TunnelSession>> = self.inner.lock().await.values().cloned().collect();
        let mut remove = Vec::new();
        let mut expired = 0;
        for session in current {
            if session.try_expire(ttl).await {
                expired += 1;
                remove.push((session.id(), session));
            }
        }

        if !remove.is_empty() {
            let mut sessions = self.inner.lock().await;
            for (id, session) in remove {
                if sessions
                    .get(&id)
                    .is_some_and(|current| Arc::ptr_eq(current, &session))
                {
                    sessions.remove(&id);
                }
            }
        }
        expired
    }

    pub async fn stats(&self) -> ServerSessionStats {
        let current: Vec<Arc<TunnelSession>> = self.inner.lock().await.values().cloned().collect();
        let mut stats = ServerSessionStats {
            total_sessions: current.len(),
            ..ServerSessionStats::default()
        };
        for session in current {
            let session_stats = session.stats().await;
            stats.active_attaches += session_stats.active_attaches;
            stats.active_sessions += usize::from(session_stats.active_attaches > 0);
            stats.detached_sessions += usize::from(session_stats.detached);
            stats.complete_sessions += usize::from(session_stats.complete);
            stats.done_sessions += usize::from(session_stats.done);
            stats.buffered_bytes += session_stats.buffered_bytes;
            stats.buffered_chunks += session_stats.buffered_chunks;
            stats.sessions_with_buffered_data += usize::from(session_stats.buffered_bytes > 0);
            stats.sessions_with_cleanup += usize::from(session_stats.has_cleanup);
            stats.sessions_with_reconnect_error += usize::from(session_stats.has_reconnect_error);
            stats.sessions_with_pending_remote_close +=
                usize::from(session_stats.has_pending_remote_close);
            stats.reconnect_attempts_total += session_stats.reconnect_attempts;
        }
        stats
    }

    #[cfg(test)]
    async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    #[cfg(test)]
    async fn contains(&self, session_id: TunnelSessionId) -> bool {
        self.inner.lock().await.contains_key(&session_id)
    }
}

fn ensure_room_for_locked(
    sessions: &HashMap<TunnelSessionId, Arc<TunnelSession>>,
    limits: ServerSessionLimits,
    peer_id: EndpointId,
) -> Result<()> {
    if sessions.len() >= limits.max_total {
        bail!(
            "server tunnel session limit reached ({}/{})",
            sessions.len(),
            limits.max_total
        );
    }
    let peer_sessions = count_peer_sessions_locked(sessions, peer_id);
    if peer_sessions >= limits.max_per_peer {
        bail!(
            "server tunnel session limit reached for peer {peer_id} ({}/{})",
            peer_sessions,
            limits.max_per_peer
        );
    }
    Ok(())
}

fn count_peer_sessions_locked(
    sessions: &HashMap<TunnelSessionId, Arc<TunnelSession>>,
    peer_id: EndpointId,
) -> usize {
    sessions
        .values()
        .filter(|session| session.peer_id() == peer_id)
        .count()
}

pub fn spawn_server_session_reaper(sessions: ServerSessionStore, cancel: CancellationToken) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(SERVER_SESSION_REAP_INTERVAL) => {
                    sessions.reap_expired(sessions.detached_ttl).await;
                }
            }
        }
    });
}

pub async fn serve_connection(
    connection: Connection,
    mut send: SendStream,
    mut recv: RecvStream,
    peer_id: EndpointId,
    target: ServerTarget,
    connections: Option<Arc<PeerConnections>>,
    sessions: ServerSessionStore,
    drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    attach_drop_closer(&connection, drop_rx);
    let health = connections.map(|connections| AttachConnectionHealth::new(connections, &connection));
    let Some(Frame::Hello {
        session_id,
        recv_next,
        resume,
    }) = read_frame(&mut recv).await?
    else {
        bail!("tunnel client did not send hello");
    };

    let (session, is_new_session) = match sessions
        .get_or_create(session_id, peer_id, target, resume)
        .await
    {
        Ok(admission) => admission,
        Err(error) => {
            let expired_resume = error.downcast_ref::<ExpiredResumeError>().is_some();
            send_tunnel_error(&connection, &mut send, format!("{error:#}")).await;
            if expired_resume {
                return Ok(());
            }
            return Err(error);
        }
    };
    if session.peer_id() != peer_id {
        if is_new_session {
            sessions.remove_new_session(&session).await;
        }
        bail!("tunnel session {session_id} belongs to a different peer");
    }

    if let Err(error) = write_frame(
        &mut send,
        Frame::Hello {
            session_id,
            recv_next: session.recv_next().await,
            resume: false,
        },
    )
    .await
    {
        if is_new_session {
            sessions.remove_new_session(&session).await;
        }
        return Err(error);
    }

    let attach_started = Instant::now();
    let result = session
        .clone()
        .run_attach(send, recv, recv_next, health.clone())
        .await;
    if matches!(&result, Err(error) if !is_permanent_failure(error))
        && let Some(health) = health
    {
        health
            .note_attach_failure("attached", attach_started.elapsed())
            .await;
    }
    sessions.reap_expired(sessions.detached_ttl).await;
    if let Err(error) = &result
        && is_expected_detach(error)
    {
        return Ok(());
    }
    result
}

fn is_expected_detach(error: &anyhow::Error) -> bool {
    let error = format!("{error:#}");
    error.contains("connection lost: closed")
        || error.contains("tunnel attach stream closed")
        || error.contains("closed: closed")
}

async fn send_tunnel_error(connection: &Connection, send: &mut SendStream, message: String) {
    if write_frame(send, Frame::Error { message }).await.is_err() {
        return;
    }
    let _ = send.finish();
    let stopped = send.stopped();
    tokio::select! {
        _ = stopped => {}
        _ = connection.closed() => {}
        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
}

async fn create_server_session(
    session_id: TunnelSessionId,
    peer_id: EndpointId,
    target: ServerTarget,
) -> Result<(Arc<TunnelSession>, LocalRead)> {
    match target {
        ServerTarget::UnixSocket(local_socket) => {
            let local = UnixStream::connect(&local_socket).await.with_context(|| {
                format!(
                    "failed to connect exposed socket {}",
                    local_socket.display()
                )
            })?;
            Ok(TunnelSession::new(session_id, peer_id, local))
        }
        ServerTarget::Tcp { addr } => {
            let local = TcpStream::connect(&addr)
                .await
                .with_context(|| format!("failed to connect exposed tcp {addr}"))?;
            let (read, write) = local.into_split();
            Ok(TunnelSession::new_parts(
                session_id,
                peer_id,
                Box::new(read),
                Box::new(write),
            ))
        }
        ServerTarget::Exec { argv, limit } => {
            spawn_exec_session(session_id, peer_id, argv, limit).await
        }
        ServerTarget::Shell { allowed } => spawn_shell_session(session_id, peer_id, allowed).await,
    }
}

async fn spawn_shell_session(
    session_id: TunnelSessionId,
    peer_id: EndpointId,
    allowed: bool,
) -> Result<(Arc<TunnelSession>, LocalRead)> {
    let (service, tunnel) = tokio::io::duplex(64 * 1024);
    let (service_read, service_write) = tokio::io::split(service);
    let (tunnel_read, tunnel_write) = tokio::io::split(tunnel);
    let kill = CancellationToken::new();
    let shell_kill = kill.clone();
    tokio::spawn(async move {
        let mut read = service_read;
        let mut write = service_write;
        let result = if allowed {
            shell::serve_shell_session_until(
                &mut read,
                &mut write,
                &peer_id.to_string(),
                shell_kill,
            )
            .await
        } else {
            shell::serve_shell_disabled(&mut write).await
        };
        if let Err(error) = result {
            eprintln!("fabric: shell session {session_id} failed: {error:#}");
        }
        let _ = write.shutdown().await;
    });

    Ok(TunnelSession::new_parts_with_cleanup(
        session_id,
        peer_id,
        Box::new(tunnel_read),
        Box::new(tunnel_write),
        Some(SessionCleanup { kill }),
    ))
}

async fn spawn_exec_session(
    session_id: TunnelSessionId,
    peer_id: EndpointId,
    argv: Vec<String>,
    limit: Arc<ExecLimit>,
) -> Result<(Arc<TunnelSession>, LocalRead)> {
    let Some(program) = argv.first() else {
        bail!("exposed exec command is empty");
    };
    let permit = limit.try_acquire().with_context(|| {
        format!(
            "exposed exec concurrency limit reached ({}/{})",
            limit.active_children(),
            limit.max_children()
        )
    })?;
    let label = argv.join(" ");
    let mut command = Command::new(program);
    command
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn exposed exec {program:?}"))?;
    let stdin = child
        .stdin
        .take()
        .context("exposed exec child stdin was not piped")?;
    let stdout = child
        .stdout
        .take()
        .context("exposed exec child stdout was not piped")?;
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(log_child_stderr(session_id, label.clone(), stderr));
    }
    let kill = CancellationToken::new();
    let kill_wait = kill.clone();
    tokio::spawn(async move {
        let result = tokio::select! {
            result = child.wait() => result,
            _ = kill_wait.cancelled() => {
                match child.kill().await {
                    Ok(()) => {
                        eprintln!("fabric: exec {label:?} session {session_id} killed after tunnel session expiry");
                        return;
                    }
                    Err(error) => Err(error),
                }
            }
        };
        drop(permit);
        match result {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!("fabric: exec {label:?} session {session_id} exited with {status}");
            }
            Err(error) => {
                eprintln!("fabric: exec {label:?} session {session_id} wait failed: {error:#}");
            }
        }
    });

    Ok(TunnelSession::new_parts_with_cleanup(
        session_id,
        peer_id,
        Box::new(stdout),
        Box::new(stdin),
        Some(SessionCleanup { kill }),
    ))
}

async fn log_child_stderr(session_id: TunnelSessionId, label: String, mut stderr: ChildStderr) {
    let mut lines = BufReader::new(&mut stderr).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => {
                eprintln!("fabric: exec {label:?} session {session_id} stderr: {line}");
            }
            Ok(None) => return,
            Err(error) => {
                eprintln!("fabric: exec {label:?} session {session_id} stderr failed: {error:#}");
                return;
            }
        }
    }
}

fn attach_drop_closer(connection: &Connection, mut drop_rx: watch::Receiver<u64>) {
    let weak_connection = connection.weak_handle();
    let closed = weak_connection.closed();
    tokio::spawn(async move {
        tokio::select! {
            _ = closed => {}
            changed = drop_rx.changed() => {
                if changed.is_ok()
                    && let Some(connection) = weak_connection.upgrade()
                {
                    connection.close(0u32.into(), b"fabric tunnel drop requested");
                }
            }
        }
    });
}

async fn write_frame<W>(write: &mut W, frame: Frame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let (kind, payload) = encode_frame(frame)?;
    if payload.len() > MAX_FRAME_LEN {
        bail!("tunnel frame too large: {} bytes", payload.len());
    }
    let mut header = [0; 5];
    header[0] = kind;
    header[1..].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    write.write_all(&header).await?;
    write.write_all(&payload).await?;
    write.flush().await?;
    Ok(())
}

async fn read_frame<R>(read: &mut R) -> Result<Option<Frame>>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0; 5];
    if let Err(error) = read.read_exact(&mut header).await {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error.into());
    }

    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    if len > MAX_FRAME_LEN {
        bail!("tunnel frame too large: {len} bytes");
    }
    let mut payload = vec![0; len];
    read.read_exact(&mut payload).await?;
    decode_frame(header[0], payload)
}

fn encode_frame(frame: Frame) -> Result<(u8, Vec<u8>)> {
    let mut payload = Vec::new();
    let kind = match frame {
        Frame::Hello {
            session_id,
            recv_next,
            resume,
        } => {
            payload.extend_from_slice(&session_id.0);
            payload.extend_from_slice(&recv_next.to_be_bytes());
            payload.push(u8::from(resume));
            FRAME_HELLO
        }
        Frame::Data { offset, bytes } => {
            payload.extend_from_slice(&offset.to_be_bytes());
            payload.extend_from_slice(&bytes);
            FRAME_DATA
        }
        Frame::Ack { recv_next } => {
            payload.extend_from_slice(&recv_next.to_be_bytes());
            FRAME_ACK
        }
        Frame::Close { offset } => {
            payload.extend_from_slice(&offset.to_be_bytes());
            FRAME_CLOSE
        }
        Frame::Error { message } => {
            payload.extend_from_slice(message.as_bytes());
            FRAME_ERROR
        }
    };
    Ok((kind, payload))
}

fn decode_frame(kind: u8, payload: Vec<u8>) -> Result<Option<Frame>> {
    let frame = match kind {
        FRAME_HELLO => {
            if payload.len() != 24 && payload.len() != 25 {
                bail!("invalid tunnel hello length {}", payload.len());
            }
            Frame::Hello {
                session_id: TunnelSessionId::from_slice(&payload[..16])?,
                recv_next: u64::from_be_bytes(payload[16..24].try_into()?),
                resume: payload.get(24).is_some_and(|value| *value != 0),
            }
        }
        FRAME_DATA => {
            if payload.len() < 8 {
                bail!("invalid tunnel data length {}", payload.len());
            }
            Frame::Data {
                offset: u64::from_be_bytes(payload[..8].try_into()?),
                bytes: payload[8..].to_vec(),
            }
        }
        FRAME_ACK => {
            if payload.len() != 8 {
                bail!("invalid tunnel ack length {}", payload.len());
            }
            Frame::Ack {
                recv_next: u64::from_be_bytes(payload[..8].try_into()?),
            }
        }
        FRAME_CLOSE => {
            if payload.len() != 8 {
                bail!("invalid tunnel close length {}", payload.len());
            }
            Frame::Close {
                offset: u64::from_be_bytes(payload[..8].try_into()?),
            }
        }
        FRAME_ERROR => Frame::Error {
            message: String::from_utf8_lossy(&payload).to_string(),
        },
        _ => bail!("unknown tunnel frame {kind}"),
    };
    Ok(Some(frame))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    use tokio::io::duplex;

    fn peer_id() -> EndpointId {
        SecretKey::generate().public()
    }

    fn session_id(byte: u8) -> TunnelSessionId {
        TunnelSessionId([byte; 16])
    }

    fn store(max_total: usize, max_per_peer: usize) -> ServerSessionStore {
        ServerSessionStore::new(
            ServerSessionLimits {
                max_total,
                max_per_peer,
            },
            Duration::from_secs(60),
        )
    }

    fn test_session(id: TunnelSessionId, peer: EndpointId) -> Arc<TunnelSession> {
        let (read, _read_peer) = duplex(64);
        let (_write_peer, write) = duplex(64);
        let (session, _local_read) =
            TunnelSession::new_parts(id, peer, Box::new(read), Box::new(write));
        session
    }

    fn test_session_with_cleanup(
        id: TunnelSessionId,
        peer: EndpointId,
        kill: CancellationToken,
    ) -> Arc<TunnelSession> {
        let (read, _read_peer) = duplex(64);
        let (_write_peer, write) = duplex(64);
        let (session, _local_read) = TunnelSession::new_parts_with_cleanup(
            id,
            peer,
            Box::new(read),
            Box::new(write),
            Some(SessionCleanup { kill }),
        );
        session
    }

    async fn mark_detached(session: &TunnelSession) {
        session.begin_attach().await.unwrap();
        session.end_attach().await;
    }

    #[tokio::test]
    async fn server_session_store_stats_counts_retained_session_state() {
        let store = store(4, 4);
        let peer = peer_id();

        let active = test_session(session_id(1), peer);
        active.begin_attach().await.unwrap();
        active.push_local_data(vec![1, 2, 3, 4]).await;
        active
            .record_reconnect_attempt(Some("attach failed".to_string()))
            .await;
        store.insert_created(active).await.unwrap();

        let kill = CancellationToken::new();
        let detached = test_session_with_cleanup(session_id(2), peer, kill);
        mark_detached(&detached).await;
        detached.push_local_data(vec![5, 6]).await;
        store.insert_created(detached).await.unwrap();

        let stats = store.stats().await;

        assert_eq!(stats.total_sessions, 2);
        assert_eq!(stats.active_sessions, 1);
        assert_eq!(stats.detached_sessions, 1);
        assert_eq!(stats.active_attaches, 1);
        assert_eq!(stats.buffered_bytes, 6);
        assert_eq!(stats.buffered_chunks, 2);
        assert_eq!(stats.sessions_with_buffered_data, 2);
        assert_eq!(stats.sessions_with_cleanup, 1);
        assert_eq!(stats.sessions_with_reconnect_error, 1);
        assert_eq!(stats.reconnect_attempts_total, 1);
    }

    #[tokio::test]
    async fn server_session_store_rejects_when_total_cap_has_no_detached_room() {
        let store = store(1, 1);
        let first = test_session(session_id(1), peer_id());
        first.begin_attach().await.unwrap();
        store.insert_created(first.clone()).await.unwrap();

        store.evict_to_make_room(peer_id()).await;

        let second = test_session(session_id(2), peer_id());
        let error = store.insert_created(second).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("server tunnel session limit reached"),
            "unexpected error: {error:#}"
        );
        assert_eq!(store.len().await, 1);
        assert!(store.contains(first.id()).await);
    }

    #[tokio::test]
    async fn server_session_store_evicts_oldest_detached_for_total_cap() {
        let store = store(2, 2);
        let first = test_session(session_id(1), peer_id());
        mark_detached(&first).await;
        store.insert_created(first.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;

        let second = test_session(session_id(2), peer_id());
        mark_detached(&second).await;
        store.insert_created(second.clone()).await.unwrap();

        store.evict_to_make_room(peer_id()).await;

        assert_eq!(store.len().await, 1);
        assert!(!store.contains(first.id()).await);
        assert!(store.contains(second.id()).await);
    }

    #[tokio::test]
    async fn server_session_store_evicts_same_peer_first_for_peer_cap() {
        let store = store(4, 1);
        let capped_peer = peer_id();
        let other_peer = peer_id();
        let capped = test_session(session_id(1), capped_peer);
        mark_detached(&capped).await;
        store.insert_created(capped.clone()).await.unwrap();
        let other = test_session(session_id(2), other_peer);
        mark_detached(&other).await;
        store.insert_created(other.clone()).await.unwrap();

        store.evict_to_make_room(capped_peer).await;

        assert_eq!(store.len().await, 1);
        assert!(!store.contains(capped.id()).await);
        assert!(store.contains(other.id()).await);
    }

    #[tokio::test]
    async fn server_session_store_does_not_evict_active_sessions() {
        let store = store(1, 1);
        let active = test_session(session_id(1), peer_id());
        active.begin_attach().await.unwrap();
        store.insert_created(active.clone()).await.unwrap();

        store.evict_to_make_room(peer_id()).await;

        assert_eq!(store.len().await, 1);
        assert!(store.contains(active.id()).await);
    }

    #[tokio::test]
    async fn server_session_reap_expires_detached_sessions() {
        let store = store(2, 2);
        let session = test_session(session_id(1), peer_id());
        mark_detached(&session).await;
        store.insert_created(session.clone()).await.unwrap();

        let expired = store.reap_expired(Duration::ZERO).await;

        assert_eq!(expired, 1);
        assert_eq!(store.len().await, 0);
        assert!(!store.contains(session.id()).await);
    }

    #[tokio::test]
    async fn server_session_store_rejects_resume_after_expiry() {
        let store = store(2, 2);
        let peer = peer_id();
        let session = test_session(session_id(1), peer);
        mark_detached(&session).await;
        store.insert_created(session.clone()).await.unwrap();

        assert_eq!(store.reap_expired(Duration::ZERO).await, 1);
        let error = store
            .get_or_create(
                session.id(),
                peer,
                ServerTarget::UnixSocket(PathBuf::from("/missing")),
                true,
            )
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("server tunnel session"),
            "unexpected error: {error:#}"
        );
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn server_session_eviction_cancels_cleanup() {
        let kill = CancellationToken::new();
        let session = test_session_with_cleanup(session_id(1), peer_id(), kill.clone());

        session.close_for_eviction().await;

        assert!(kill.is_cancelled());
    }

    /// A producer that never stops, standing in for a runaway remote process.
    struct EndlessProducer;

    impl AsyncRead for EndlessProducer {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let n = buf.remaining().min(8192);
            buf.put_slice(&vec![b'x'; n]);
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// A detached session's replay buffer is bounded, and the bound is the real
    /// backstop against a runaway remote process.
    ///
    /// The retention docs used to say this buffer had no cap of its own, that a
    /// runaway shell would reach roughly 17 MB across a 15 minute window, and
    /// that the detached TTL was therefore the only backstop. Measured, all
    /// three are wrong. The reader waits for buffer space, nothing ACKs a
    /// detached session, so it stops at exactly MAX_BUFFERED_BYTES and the
    /// remote process blocks on its own PTY write instead. Backpressure is the
    /// backstop; the TTL bounds how long a session lives, not how much it holds.
    ///
    /// This matters beyond tidiness: "no cap" was the stated reason not to raise
    /// the detached TTL further.
    #[tokio::test]
    async fn a_detached_replay_buffer_stops_at_the_cap_instead_of_growing() {
        let (_write_peer, write) = duplex(64);
        let (session, local_read) = TunnelSession::new_parts(
            session_id(77),
            peer_id(),
            Box::new(EndlessProducer),
            Box::new(write),
        );
        // Never attached, so nothing ever ACKs. That is the worst case for
        // retention and exactly the detached-session case being bounded here.
        tokio::spawn(session.clone().run_local_reader(local_read));

        let mut samples = Vec::new();
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            samples.push(session.state.lock().await.buffered_bytes);
        }

        // POSITIVE CONTROL. A producer that silently produced nothing would
        // satisfy every bound below while proving nothing at all.
        assert!(
            samples[0] > 0,
            "the producer never produced, so this test measured nothing"
        );
        for (index, bytes) in samples.iter().enumerate() {
            assert!(
                *bytes <= MAX_BUFFERED_BYTES,
                "sample {index} held {bytes} bytes, past the {MAX_BUFFERED_BYTES} byte cap"
            );
        }
        assert_eq!(
            samples.last(),
            samples.first(),
            "the buffer must settle at the cap rather than keep growing: {samples:?}"
        );
    }

    /// A local reader that hands over `bytes`, then ends the way the test asks.
    ///
    /// This is the whole platform difference in issue 32, made explicit. Dropping
    /// a local socket gives a clean zero-length read on macOS and an error on
    /// Linux. Injecting the ending directly turns a Linux-only, racy, 40-second
    /// CI failure into a decision this test makes on any operating system.
    struct EndingReader {
        bytes: Vec<u8>,
        fail_at_end: bool,
    }

    impl AsyncRead for EndingReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if !self.bytes.is_empty() {
                let take = self.bytes.len().min(buf.remaining());
                let chunk: Vec<u8> = self.bytes.drain(..take).collect();
                buf.put_slice(&chunk);
                return std::task::Poll::Ready(Ok(()));
            }
            if self.fail_at_end {
                // What Linux reports when the local peer drops the socket.
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection reset by peer",
                )));
            }
            // What macOS reports for the same event: a clean EOF.
            std::task::Poll::Ready(Ok(()))
        }
    }

    async fn session_after_local_input_ends(fail_at_end: bool) -> Arc<TunnelSession> {
        let (_write_peer, write) = duplex(64);
        let reader = EndingReader {
            bytes: b"hello".to_vec(),
            fail_at_end,
        };
        let (session, local_read) =
            TunnelSession::new_parts(session_id(9), peer_id(), Box::new(reader), Box::new(write));
        // Returns Ok on EOF and Err on an abrupt close. Either way the local
        // input is over, which is the only thing that matters here.
        let _ = session.clone().run_local_reader(local_read).await;
        session
    }

    /// Both endings must tell the remote that local input is over.
    ///
    /// Only a recorded `send_closed` makes the writer emit `Frame::Close`, and
    /// only that frame lets the server stop counting the session as attached.
    /// Without it the server waits for bytes that can never arrive, which is the
    /// 40-second Linux stall in issue 32: not a slow path, a missing message.
    #[tokio::test]
    async fn an_abrupt_local_close_reports_the_end_just_like_a_clean_eof() {
        let clean = session_after_local_input_ends(false).await;
        let abrupt = session_after_local_input_ends(true).await;

        let clean_closed = clean.state.lock().await.send_closed;
        let abrupt_closed = abrupt.state.lock().await.send_closed;

        assert_eq!(
            clean_closed,
            Some(5),
            "a clean EOF must close the send side after the 5 bytes it read"
        );
        assert_eq!(
            abrupt_closed, clean_closed,
            "an abrupt local close must end the send side exactly like a clean EOF; \
             leaving it open is what makes the server hold the session attached"
        );
    }

    /// Issue 51: an abandoned local dial must end the session, not retry forever.
    ///
    /// The caller exits, nobody holds the local socket, and the remote pty keeps
    /// producing output. Writing that output fails with a local `BrokenPipe`.
    /// Classified as a transport failure it reattaches, fails on the same dead
    /// socket, and sleeps, forever: `is_complete` waits for a remote close and a
    /// pty never closes its side, and every reconnect clears `last_detached` so
    /// the detached TTL cannot collect it either.
    #[tokio::test]
    async fn a_dead_local_endpoint_is_permanent_not_a_transport_drop() {
        let (write_peer, write) = duplex(64);
        let (session, _local_read) = TunnelSession::new_parts(
            session_id(51),
            peer_id(),
            Box::new(tokio::io::empty()),
            Box::new(write),
        );
        // What an abandoned dial looks like from inside the tunnel.
        drop(write_peer);

        let error = session
            .accept_data(0, b"remote pty output".to_vec())
            .await
            .expect_err("writing into a dropped local socket must fail");

        assert!(
            error.downcast_ref::<LocalEndpointGone>().is_some(),
            "a local io failure must be a type the retry loop can match on, \
             not prose it has to grep: {error:#}"
        );
        assert!(
            is_permanent_failure(&error),
            "no remote peer can repair a dead local socket, so this must not retry"
        );
        assert_eq!(
            session.state.lock().await.send_closed,
            Some(0),
            "the send side must close, or the server keeps counting this attached"
        );
    }

    /// The control for the case above. A transport drop must STAY retryable, or
    /// the fix above would turn every network blip into a killed session.
    #[test]
    fn a_transport_drop_is_still_retryable_and_a_refusal_is_still_permanent() {
        for transient in [
            "connection lost: timed out",
            "tunnel attach stream closed",
            "no route to host",
        ] {
            let error = anyhow::anyhow!("{transient}");
            assert!(
                !is_permanent_failure(&error),
                "{transient:?} must remain retryable"
            );
        }

        let refused = anyhow::Error::new(ServerRejected("denied".to_string()));
        assert!(
            is_permanent_failure(&refused),
            "a trust refusal is permanent"
        );
        let allow_list = anyhow::anyhow!("node is not in fabric allow-list");
        assert!(is_permanent_failure(&allow_list));

        // The typed error must survive being wrapped in context on the way up,
        // because that is how it actually reaches the retry loop.
        let wrapped = anyhow::Error::new(LocalEndpointGone {
            id: session_id(51),
            source: std::io::Error::from(std::io::ErrorKind::BrokenPipe),
        })
        .context("reading tunnel attach stream");
        assert!(
            is_permanent_failure(&wrapped),
            "context wrapping must not hide a dead local endpoint"
        );
    }

    /// The recycle guard must come off on both endings too.
    ///
    /// This part already worked. It is asserted beside the case above so a later
    /// change cannot fix one ending and quietly regress the other.
    #[tokio::test]
    async fn both_endings_release_the_recycle_guard() {
        for fail_at_end in [false, true] {
            let session = session_after_local_input_ends(fail_at_end).await;
            assert!(
                session.state.lock().await.attach_gauge.is_none(),
                "the recycle guard must be released when local input ends \
                 (fail_at_end={fail_at_end})"
            );
        }
    }

    /// Finding 1 of the 2026-08-29 review, at the socket.
    ///
    /// A session whose peer never answers has no remote output, so the only
    /// instrument issue 51 left it (a failed local write) never fires. It must
    /// be able to ask the kernel directly, and the answer must tell a consumer
    /// that has LEFT apart from one that has finished sending and is WAITING.
    /// Getting that wrong in one direction leaks permits; in the other it
    /// kills every request-then-half-close protocol while its peer is away.
    #[tokio::test]
    async fn a_consumer_that_closed_its_socket_is_detected_without_remote_output() {
        let (consumer, local) = tokio::net::UnixStream::pair().unwrap();
        let (session, read) = TunnelSession::new(session_id(1), peer_id(), local);
        let reader = tokio::spawn(session.clone().run_local_reader(read));

        // Alive and not even done sending: nothing to probe, nothing to say.
        session
            .probe_local_endpoint()
            .await
            .expect("a consumer that has not closed anything is present");

        // The consumer gives up entirely. The reader sees EOF, and nothing
        // else in the session can see anything.
        drop(consumer);
        let _ = reader.await;
        assert!(
            session.local_input_ended().await,
            "the reader must have recorded the EOF, or the probe is gated off"
        );

        let error = session
            .probe_local_endpoint()
            .await
            .expect_err("a zero-length write into a fully closed socket must fail");
        assert!(
            error.downcast_ref::<LocalEndpointGone>().is_some(),
            "the retry loop matches on the type, not the prose: {error:#}"
        );
        assert!(is_permanent_failure(&error));
    }

    /// The control. A consumer that half-closed is still there, and must still
    /// receive output that arrives later.
    #[tokio::test]
    async fn a_consumer_that_half_closed_is_still_served() {
        let (mut consumer, local) = tokio::net::UnixStream::pair().unwrap();
        let (session, read) = TunnelSession::new(session_id(2), peer_id(), local);
        let reader = tokio::spawn(session.clone().run_local_reader(read));

        // "I have sent my whole request; now I wait for the reply."
        consumer.shutdown().await.unwrap();
        let _ = reader.await;
        assert!(session.local_input_ended().await);

        session
            .probe_local_endpoint()
            .await
            .expect("a half-closed consumer is waiting, not gone");
        assert!(
            tokio::time::timeout(
                LOCAL_ENDPOINT_PROBE_INTERVAL * 3,
                session.watch_local_endpoint()
            )
            .await
            .is_err(),
            "the watcher must not resolve while the consumer is waiting"
        );

        // And the reply it is waiting for still reaches it.
        session
            .accept_data(0, b"the late reply".to_vec())
            .await
            .expect("output to a waiting consumer must be written");
        let mut got = vec![0; 32];
        let n = consumer.read(&mut got).await.unwrap();
        assert_eq!(&got[..n], b"the late reply");
    }

    /// The watcher is what the retry loop actually races against, so its
    /// resolution is asserted, not only the probe underneath it.
    #[tokio::test]
    async fn the_watcher_resolves_once_the_consumer_leaves() {
        let (consumer, local) = tokio::net::UnixStream::pair().unwrap();
        let (session, read) = TunnelSession::new(session_id(3), peer_id(), local);
        let reader = tokio::spawn(session.clone().run_local_reader(read));
        drop(consumer);
        let _ = reader.await;

        let error = tokio::time::timeout(
            LOCAL_ENDPOINT_PROBE_INTERVAL * 3,
            session.watch_local_endpoint(),
        )
        .await
        .expect("the watcher must resolve within a few probe intervals");
        assert!(error.downcast_ref::<LocalEndpointGone>().is_some());
    }
}
