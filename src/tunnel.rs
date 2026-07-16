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
    Endpoint, EndpointAddr, EndpointId,
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
};

// Resumable byte tunnel used by generic `fabric dial` sockets. Each local Unix
// connection gets one session id; reconnecting iroh attaches replay unacked
// chunks and preserve the exposed Unix service connection on the accept side.
const MAX_FRAME_LEN: usize = 1024 * 1024;
const LOCAL_READ_BUF: usize = 8192;
const MAX_BUFFERED_BYTES: usize = 4 * 1024 * 1024;
const SERVER_SESSION_REAP_INTERVAL: Duration = Duration::from_secs(60);
const ATTACH_STABLE_AFTER: Duration = Duration::from_secs(2);

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
            let read = read.read(&mut buf).await?;
            if read == 0 {
                self.mark_send_closed().await;
                return Ok(());
            }
            self.push_local_data(buf[..read].to_vec()).await;
        }
    }

    async fn wait_for_buffer_space(&self) {
        loop {
            {
                let state = self.state.lock().await;
                if state.buffered_bytes < MAX_BUFFERED_BYTES || state.send_closed.is_some() {
                    return;
                }
            }
            self.notify.notified().await;
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
        drop(state);
        self.notify.notify_waiters();
    }

    async fn apply_peer_ack(&self, recv_next: u64) {
        let mut state = self.state.lock().await;
        if recv_next > state.send_acked {
            state.send_acked = recv_next.min(state.send_next);
            drop_acked_chunks(&mut state);
        }
        drop(state);
        self.notify.notify_waiters();
    }

    async fn accept_data(&self, offset: u64, bytes: Vec<u8>) -> Result<()> {
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
                return Ok(());
            }
            bytes[already_have..].to_vec()
        };

        {
            let mut write = self.local_write.lock().await;
            let Some(write) = write.as_mut() else {
                bail!("tunnel {} local write is closed", self.id);
            };
            write.write_all(&bytes).await?;
            write.flush().await?;
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
        Ok(())
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

    pub async fn run_attach(
        self: Arc<Self>,
        send: SendStream,
        recv: RecvStream,
        peer_recv_next: u64,
    ) -> Result<()> {
        self.begin_attach().await?;
        self.apply_peer_ack(peer_recv_next).await;

        let result = async {
            let mut writer = tokio::spawn(write_attach_loop(self.clone(), send));
            let mut reader = tokio::spawn(read_attach_loop(self.clone(), recv));

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
            _ = session.notify.notified() => {}
            _ = session.done.cancelled() => return Ok(()),
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

async fn read_attach_loop(session: Arc<TunnelSession>, mut recv: RecvStream) -> Result<()> {
    while let Some(frame) = read_frame(&mut recv).await? {
        match frame {
            Frame::Hello { .. } => bail!("unexpected tunnel hello after attach"),
            Frame::Data { offset, bytes } => session.accept_data(offset, bytes).await?,
            Frame::Ack { recv_next } => session.apply_peer_ack(recv_next).await,
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
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    let (read, write) = local.into_split();
    run_client_connection_parts(
        Box::new(read),
        Box::new(write),
        endpoint_rx,
        home,
        peer,
        alpn,
        cancel,
        drop_rx,
    )
    .await
}

pub async fn run_client_tcp_connection(
    local: TcpStream,
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    let (read, write) = local.into_split();
    run_client_connection_parts(
        Box::new(read),
        Box::new(write),
        endpoint_rx,
        home,
        peer,
        alpn,
        cancel,
        drop_rx,
    )
    .await
}

async fn run_client_connection_parts(
    local_read: LocalRead,
    local_write: LocalWrite,
    endpoint_rx: watch::Receiver<CurrentEndpoint>,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    let peer_id = PeerBook::load(&home)?.resolve(&peer)?.id;
    let session_id = TunnelSessionId::random();
    let (session, local_read) =
        TunnelSession::new_parts(session_id, peer_id, local_read, local_write);
    let reader = tokio::spawn(session.clone().run_local_reader(local_read));
    let result = run_client_attach_loop(
        session.clone(),
        endpoint_rx,
        home,
        peer,
        alpn,
        cancel,
        drop_rx,
    )
    .await;
    reader.abort();
    let _ = reader.await;
    result
}

async fn run_client_attach_loop(
    session: Arc<TunnelSession>,
    mut endpoint_rx: watch::Receiver<CurrentEndpoint>,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    mut drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    let mut backoff = Backoff::new();

    loop {
        if session.is_complete().await {
            return Ok(());
        }

        let peer_addr = resolve_peer_for_attempt(&home, &peer, session.peer_id()).await;
        let endpoint = endpoint_rx.borrow().endpoint.clone();
        let attach_started = Instant::now();
        let result =
            connect_and_attach(session.clone(), endpoint, peer_addr, &alpn, drop_rx.clone()).await;

        match result {
            Ok(()) if session.is_complete().await => return Ok(()),
            Ok(()) => {
                session
                    .record_reconnect_attempt(Some("tunnel attach ended".to_string()))
                    .await;
            }
            Err(error) => {
                session
                    .record_reconnect_attempt(Some(format!("{error:#}")))
                    .await;
            }
        }

        if attach_started.elapsed() >= ATTACH_STABLE_AFTER {
            backoff.reset();
            session.clear_reconnect_error().await;
        }

        let delay = backoff.next_delay();
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = cancel.cancelled() => return Ok(()),
            _ = session.done.cancelled() => return Ok(()),
            changed = drop_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
            changed = endpoint_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
        }
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
    endpoint: Endpoint,
    peer_addr: EndpointAddr,
    alpn: &[u8],
    drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    let connection = endpoint
        .connect(peer_addr, alpn)
        .await
        .with_context(|| "failed to reconnect tunnel")?;
    attach_drop_closer(connection.clone(), drop_rx);
    let (mut send, mut recv) = connection.open_bi().await?;

    write_frame(
        &mut send,
        Frame::Hello {
            session_id: session.id(),
            recv_next: session.recv_next().await,
            resume: session.has_attached().await,
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
            session.abort_local().await?;
            bail!("tunnel server rejected session: {message}");
        }
        Some(_) | None => bail!("tunnel server did not send hello"),
    };
    if session_id != session.id() {
        bail!("tunnel server replied with wrong session id {session_id}");
    }

    session.run_attach(send, recv, recv_next).await
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
    sessions: ServerSessionStore,
    drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    attach_drop_closer(connection, drop_rx);
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
            let _ = write_frame(
                &mut send,
                Frame::Error {
                    message: format!("{error:#}"),
                },
            )
            .await;
            let _ = send.finish();
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

    let result = session.clone().run_attach(send, recv, recv_next).await;
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
    }
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

fn attach_drop_closer(connection: Connection, mut drop_rx: watch::Receiver<u64>) {
    tokio::spawn(async move {
        if drop_rx.changed().await.is_ok() {
            connection.close(0u32.into(), b"fabric tunnel drop requested");
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
}
