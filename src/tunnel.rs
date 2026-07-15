use std::{
    collections::{HashMap, VecDeque},
    fmt,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Mutex, Notify, watch},
};
use tokio_util::sync::CancellationToken;

use crate::config::{FabricHome, PeerBook};

// Resumable byte tunnel used by generic `fabric dial` sockets. Each local Unix
// connection gets one session id; reconnecting iroh attaches replay unacked
// chunks and preserve the exposed Unix service connection on the accept side.
const MAX_FRAME_LEN: usize = 1024 * 1024;
const LOCAL_READ_BUF: usize = 8192;
const MAX_BUFFERED_BYTES: usize = 4 * 1024 * 1024;
const SERVER_SESSION_TTL: Duration = Duration::from_secs(30 * 60);
const ATTACH_STABLE_AFTER: Duration = Duration::from_secs(2);

const FRAME_HELLO: u8 = 1;
const FRAME_DATA: u8 = 2;
const FRAME_ACK: u8 = 3;
const FRAME_CLOSE: u8 = 4;

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
}

#[derive(Debug)]
pub struct TunnelSession {
    id: TunnelSessionId,
    peer_id: EndpointId,
    local_write: Mutex<OwnedWriteHalf>,
    state: Mutex<TunnelState>,
    notify: Notify,
    done: CancellationToken,
}

impl TunnelSession {
    pub fn new(
        id: TunnelSessionId,
        peer_id: EndpointId,
        local: UnixStream,
    ) -> (Arc<Self>, OwnedReadHalf) {
        let (read, write) = local.into_split();
        let session = Arc::new(Self {
            id,
            peer_id,
            local_write: Mutex::new(write),
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

    pub async fn is_complete(&self) -> bool {
        let state = self.state.lock().await;
        state.send_closed.is_some()
            && state.remote_closed
            && state.send_buffer.is_empty()
            && state.send_acked >= state.send_next
    }

    pub async fn is_expired(&self, ttl: Duration) -> bool {
        let state = self.state.lock().await;
        if state.active_attaches > 0 || self.done.is_cancelled() {
            return false;
        }
        state
            .last_detached
            .is_some_and(|detached| detached.elapsed() >= ttl)
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

    async fn begin_attach(&self) {
        let mut state = self.state.lock().await;
        state.active_attaches += 1;
        state.last_detached = None;
    }

    async fn end_attach(&self) {
        let mut state = self.state.lock().await;
        state.active_attaches = state.active_attaches.saturating_sub(1);
        if state.active_attaches == 0 {
            state.last_detached = Some(Instant::now());
        }
        self.notify.notify_waiters();
    }

    pub async fn run_local_reader(self: Arc<Self>, mut read: OwnedReadHalf) -> Result<()> {
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
        let _ = write.shutdown().await;
        Ok(())
    }

    pub async fn run_attach(
        self: Arc<Self>,
        send: SendStream,
        recv: RecvStream,
        peer_recv_next: u64,
    ) -> Result<()> {
        self.begin_attach().await;
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
    endpoint: Endpoint,
    home: FabricHome,
    peer: String,
    alpn: Vec<u8>,
    cancel: CancellationToken,
    drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    let peer_id = PeerBook::load(&home)?.resolve(&peer)?.id;
    let session_id = TunnelSessionId::random();
    let (session, local_read) = TunnelSession::new(session_id, peer_id, local);
    let reader = tokio::spawn(session.clone().run_local_reader(local_read));
    let result =
        run_client_attach_loop(session.clone(), endpoint, home, peer, alpn, cancel, drop_rx).await;
    reader.abort();
    let _ = reader.await;
    result
}

async fn run_client_attach_loop(
    session: Arc<TunnelSession>,
    endpoint: Endpoint,
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
        let attach_started = Instant::now();
        let result = connect_and_attach(
            session.clone(),
            endpoint.clone(),
            peer_addr,
            &alpn,
            drop_rx.clone(),
        )
        .await;

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
        },
    )
    .await?;

    let Some(Frame::Hello {
        session_id,
        recv_next,
    }) = read_frame(&mut recv).await?
    else {
        bail!("tunnel server did not send hello");
    };
    if session_id != session.id() {
        bail!("tunnel server replied with wrong session id {session_id}");
    }

    session.run_attach(send, recv, recv_next).await
}

pub type ServerSessions = Arc<Mutex<HashMap<TunnelSessionId, Arc<TunnelSession>>>>;

pub async fn serve_connection(
    connection: Connection,
    mut send: SendStream,
    mut recv: RecvStream,
    peer_id: EndpointId,
    local_socket: PathBuf,
    sessions: ServerSessions,
    drop_rx: watch::Receiver<u64>,
) -> Result<()> {
    attach_drop_closer(connection, drop_rx);
    let Some(Frame::Hello {
        session_id,
        recv_next,
    }) = read_frame(&mut recv).await?
    else {
        bail!("tunnel client did not send hello");
    };

    let session =
        get_or_create_server_session(sessions.clone(), session_id, peer_id, local_socket).await?;
    if session.peer_id() != peer_id {
        bail!("tunnel session {session_id} belongs to a different peer");
    }

    write_frame(
        &mut send,
        Frame::Hello {
            session_id,
            recv_next: session.recv_next().await,
        },
    )
    .await?;

    let result = session.clone().run_attach(send, recv, recv_next).await;
    schedule_server_cleanup(sessions, session);
    if let Err(error) = &result
        && is_expected_detach(error)
    {
        return Ok(());
    }
    result
}

fn is_expected_detach(error: &anyhow::Error) -> bool {
    let error = format!("{error:#}");
    error.contains("connection lost: closed") || error.contains("tunnel attach stream closed")
}

async fn get_or_create_server_session(
    sessions: ServerSessions,
    session_id: TunnelSessionId,
    peer_id: EndpointId,
    local_socket: PathBuf,
) -> Result<Arc<TunnelSession>> {
    let mut sessions = sessions.lock().await;
    if let Some(session) = sessions.get(&session_id).cloned() {
        return Ok(session);
    }

    let local = UnixStream::connect(&local_socket).await.with_context(|| {
        format!(
            "failed to connect exposed socket {}",
            local_socket.display()
        )
    })?;
    let (session, local_read) = TunnelSession::new(session_id, peer_id, local);
    tokio::spawn(session.clone().run_local_reader(local_read));
    sessions.insert(session_id, session.clone());
    Ok(session)
}

fn schedule_server_cleanup(sessions: ServerSessions, session: Arc<TunnelSession>) {
    tokio::spawn(async move {
        tokio::time::sleep(SERVER_SESSION_TTL).await;
        if session.is_complete().await || session.is_expired(SERVER_SESSION_TTL).await {
            sessions.lock().await.remove(&session.id());
        }
    });
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
        } => {
            payload.extend_from_slice(&session_id.0);
            payload.extend_from_slice(&recv_next.to_be_bytes());
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
    };
    Ok((kind, payload))
}

fn decode_frame(kind: u8, payload: Vec<u8>) -> Result<Option<Frame>> {
    let frame = match kind {
        FRAME_HELLO => {
            if payload.len() != 24 {
                bail!("invalid tunnel hello length {}", payload.len());
            }
            Frame::Hello {
                session_id: TunnelSessionId::from_slice(&payload[..16])?,
                recv_next: u64::from_be_bytes(payload[16..24].try_into()?),
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
        _ => bail!("unknown tunnel frame {kind}"),
    };
    Ok(Some(frame))
}
