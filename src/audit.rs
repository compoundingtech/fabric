//! Best-effort process audit. Only the dedicated writer opens or writes files.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const MAX_QUEUED_RECORDS: usize = 4096;
const MAX_QUEUED_BYTES: usize = 16 * 1024 * 1024;
const MAX_ENCODED_RECORD_BYTES: usize = 8 * 1024 * 1024;
const AUDIT_FILE: &str = "audit.jsonl";

fn timestamp(time: SystemTime) -> String {
    OffsetDateTime::from(time)
        .format(&Rfc3339)
        .expect("audit time is representable")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok,
    Refused,
    SpawnFailed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRecord {
    pub time: String,
    pub peer: String,
    pub name: Option<String>,
    pub service: Option<String>,
    pub argv: Option<Vec<String>>,
    pub session_id: String,
    pub outcome: Option<Outcome>,
    pub code: Option<String>,
    pub exit_status: Option<i32>,
    pub duration_millis: Option<u64>,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Record {
    ExecStart(CallRecord),
    ExecEnd(CallRecord),
    ShellStart(CallRecord),
    ShellEnd(CallRecord),
    Refused(CallRecord),
    AuditLoss {
        time: String,
        count: u64,
        first_time: String,
        last_time: String,
    },
}

impl Record {
    pub fn call(&self) -> Option<&CallRecord> {
        match self {
            Self::ExecStart(record)
            | Self::ExecEnd(record)
            | Self::ShellStart(record)
            | Self::ShellEnd(record)
            | Self::Refused(record) => Some(record),
            Self::AuditLoss { .. } => None,
        }
    }

    fn memory_bytes(&self) -> usize {
        let Some(call) = self.call() else {
            return std::mem::size_of::<Self>() + 128;
        };
        std::mem::size_of::<Self>()
            + call.time.capacity()
            + call.peer.capacity()
            + call.name.as_ref().map_or(0, String::capacity)
            + call.service.as_ref().map_or(0, String::capacity)
            + call.session_id.capacity()
            + call.code.as_ref().map_or(0, String::capacity)
            + call.argv.as_ref().map_or(0, |argv| {
                argv.capacity() * std::mem::size_of::<String>()
                    + argv.iter().map(String::capacity).sum::<usize>()
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditStatus {
    pub dropped_records: u64,
    pub queued_records: usize,
    pub since: String,
}

#[derive(Debug, Clone)]
struct LossWindow {
    count: u64,
    first: SystemTime,
    last: SystemTime,
}

#[derive(Debug, Default)]
struct Losses {
    total: u64,
    pending: Option<LossWindow>,
}

#[derive(Debug, Default)]
struct Shared {
    losses: Mutex<Losses>,
    bytes: AtomicUsize,
    records: AtomicUsize,
    summarizing: AtomicBool,
    active_calls: AtomicUsize,
}

impl Shared {
    fn lose(&self) {
        let now = SystemTime::now();
        let mut losses = self
            .losses
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        losses.total += 1;
        match &mut losses.pending {
            Some(window) => {
                window.count += 1;
                window.last = now;
            }
            None => {
                losses.pending = Some(LossWindow {
                    count: 1,
                    first: now,
                    last: now,
                })
            }
        }
    }

    fn restore_loss(&self, older: LossWindow) {
        let mut losses = self
            .losses
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        losses.pending = Some(match losses.pending.take() {
            Some(newer) => LossWindow {
                count: older.count + newer.count,
                first: older.first,
                last: newer.last,
            },
            None => older,
        });
    }
}

#[derive(Debug, Clone)]
struct Options {
    max_file_bytes: u64,
    max_age: Duration,
    retained: usize,
    retention_age: Duration,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            max_file_bytes: 10 * 1024 * 1024,
            max_age: Duration::from_secs(24 * 3600),
            retained: 10,
            retention_age: Duration::from_secs(30 * 24 * 3600),
        }
    }
}

/// Clones share bounded admission and loss accounting. Drop never joins disk I/O.
#[derive(Debug, Clone)]
pub struct Audit {
    sender: mpsc::SyncSender<(Record, usize)>,
    shared: Arc<Shared>,
    since: String,
    max_bytes: usize,
}

impl Audit {
    pub fn start(root: &Path) -> Self {
        let mut files = AuditFiles::new(root.to_path_buf(), Options::default());
        Self::with_sink(MAX_QUEUED_RECORDS, MAX_QUEUED_BYTES, move |record| {
            files.append(record)
        })
    }

    pub(crate) fn with_sink(
        entries: usize,
        max_bytes: usize,
        mut sink: impl FnMut(&Record) -> Result<()> + Send + 'static,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<(Record, usize)>(entries);
        let shared = Arc::new(Shared::default());
        let worker = shared.clone();
        let spawned = std::thread::Builder::new()
            .name("fabric-audit".into())
            .spawn(move || {
                let mut warning: Option<Instant> = None;
                loop {
                    let mut consumed = None;
                    let disconnected = match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok((record, bytes)) => {
                            if let Err(error) = sink(&record) {
                                worker.lose();
                                audit_write_warning(&mut warning, &error);
                            }
                            consumed = Some(bytes);
                            false
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => true,
                        Err(mpsc::RecvTimeoutError::Timeout) => false,
                    };
                    worker.summarizing.store(true, Ordering::Release);
                    let loss = worker
                        .losses
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .pending
                        .take();
                    if let Some(loss) = loss {
                        let record = Record::AuditLoss {
                            time: timestamp(SystemTime::now()),
                            count: loss.count,
                            first_time: timestamp(loss.first),
                            last_time: timestamp(loss.last),
                        };
                        if let Err(error) = sink(&record) {
                            worker.restore_loss(loss);
                            audit_write_warning(&mut warning, &error);
                        }
                    }
                    worker.summarizing.store(false, Ordering::Release);
                    if let Some(bytes) = consumed {
                        worker.bytes.fetch_sub(bytes, Ordering::Release);
                        worker.records.fetch_sub(1, Ordering::Release);
                    }
                    if disconnected {
                        break;
                    }
                }
            });
        if let Err(error) = spawned {
            fabric_config::log_eprintln!(
                "fabric: audit writer could not start: {error}; audit loss remains visible in status"
            );
        }
        Self {
            sender,
            shared,
            since: timestamp(SystemTime::now()),
            max_bytes,
        }
    }

    pub fn record(&self, record: Record) {
        let bytes = record.memory_bytes();
        if self
            .shared
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.max_bytes)
            })
            .is_err()
        {
            self.shared.lose();
            return;
        }
        self.shared.records.fetch_add(1, Ordering::Release);
        if self.sender.try_send((record, bytes)).is_err() {
            self.shared.bytes.fetch_sub(bytes, Ordering::Release);
            self.shared.records.fetch_sub(1, Ordering::Release);
            self.shared.lose();
        }
    }

    pub fn status(&self) -> AuditStatus {
        AuditStatus {
            dropped_records: self
                .shared
                .losses
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .total,
            queued_records: self.shared.records.load(Ordering::Acquire),
            since: self.since.clone(),
        }
    }

    pub async fn drain(&self, bound: Duration) -> bool {
        tokio::time::timeout(bound, async {
            loop {
                if self.shared.active_calls.load(Ordering::Acquire) != 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                let pending_loss = self
                    .shared
                    .losses
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .pending
                    .is_some();
                if self.shared.records.load(Ordering::Acquire) == 0
                    && !pending_loss
                    && !self.shared.summarizing.load(Ordering::Acquire)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Kind {
    Exec,
    Shell,
}

pub(crate) use fabric_service_api::audit::Counts;

#[derive(Debug)]
struct CallState {
    record: CallRecord,
    kind: Kind,
    began: Instant,
    started: bool,
    finished: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct Call {
    audit: Audit,
    state: Arc<Mutex<CallState>>,
    counts: Arc<Counts>,
}

impl Audit {
    pub(crate) fn call(
        &self,
        peer: String,
        name: Option<String>,
        service: Option<String>,
        kind: Kind,
    ) -> Call {
        Call {
            audit: self.clone(),
            counts: Arc::new(Counts::default()),
            state: Arc::new(Mutex::new(CallState {
                record: CallRecord {
                    time: timestamp(SystemTime::now()),
                    peer,
                    name,
                    service,
                    argv: None,
                    session_id: format!("{:032x}", rand::random::<u128>()),
                    outcome: None,
                    code: None,
                    exit_status: None,
                    duration_millis: None,
                    bytes_in: 0,
                    bytes_out: 0,
                },
                kind,
                began: Instant::now(),
                started: false,
                finished: false,
            })),
        }
    }
}

impl Call {
    pub(crate) fn session_id(&self, id: impl ToString) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.started {
            state.record.session_id = id.to_string();
        }
    }

    pub(crate) fn refused(&self, code: fabric_config::error::ErrorCode) {
        self.refused_with_code(Some(code));
    }

    pub(crate) fn refused_uncoded(&self) {
        self.refused_with_code(None);
    }

    fn refused_with_code(&self, code: Option<fabric_config::error::ErrorCode>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.finished || state.started {
            return;
        }
        state.finished = true;
        let mut record = state.record.clone();
        record.time = timestamp(SystemTime::now());
        record.argv = None;
        record.outcome = Some(Outcome::Refused);
        record.code = code.map(|code| format!("fabric:{}", code.as_str()));
        drop(state);
        self.audit.record(Record::Refused(record));
    }

    fn start(&self, argv: Option<&[String]>) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.finished || state.started {
            return false;
        }
        state.started = true;
        state.began = Instant::now();
        state.record.argv = argv.map(<[String]>::to_vec);
        state.record.time = timestamp(SystemTime::now());
        let record = match state.kind {
            Kind::Exec => Record::ExecStart(state.record.clone()),
            Kind::Shell => Record::ShellStart(state.record.clone()),
        };
        drop(state);
        self.audit.record(record);
        true
    }

    fn finish(
        &self,
        outcome: Outcome,
        exit_status: Option<i32>,
        code: Option<fabric_config::error::ErrorCode>,
    ) {
        self.finish_coded(outcome, exit_status, code.map(|code| code.as_str()));
    }

    fn finish_coded(&self, outcome: Outcome, exit_status: Option<i32>, code: Option<&str>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.finished {
            return;
        }
        state.finished = true;
        let mut record = state.record.clone();
        record.time = timestamp(SystemTime::now());
        record.outcome = Some(outcome);
        record.exit_status = exit_status.or(record.exit_status);
        record.code = code.map(|code| format!("fabric:{code}"));
        record.duration_millis =
            Some(state.began.elapsed().as_millis().min(u64::MAX as u128) as u64);
        record.bytes_in = self.counts.input();
        record.bytes_out = self.counts.output();
        let record = match state.kind {
            Kind::Exec => Record::ExecEnd(record),
            Kind::Shell => Record::ShellEnd(record),
        };
        drop(state);
        self.audit.record(record);
    }
}

#[derive(Debug, Default)]
pub(crate) struct Drain {
    done: AtomicBool,
    notify: tokio::sync::Notify,
}
impl Drain {
    pub(crate) fn finish(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
    pub(crate) async fn wait(&self) {
        loop {
            let ready = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            ready.await;
        }
    }
}

pub(crate) struct CountRead<R> {
    pub(crate) inner: R,
    pub(crate) counts: Option<Arc<Counts>>,
    pub(crate) drain: Arc<Drain>,
}
impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CountRead<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        let result = std::pin::Pin::new(&mut this.inner).poll_read(cx, buffer);
        if let std::task::Poll::Ready(result) = &result {
            let bytes = buffer.filled().len() - before;
            if let Some(counts) = &this.counts {
                counts.add_out(bytes);
            }
            if result.is_err() || bytes == 0 {
                this.drain.finish();
            }
        }
        result
    }
}
impl<R> Drop for CountRead<R> {
    fn drop(&mut self) {
        self.drain.finish();
    }
}

pub(crate) struct CountAsyncWriter<W> {
    pub(crate) inner: W,
    pub(crate) counts: Option<Arc<Counts>>,
}
impl<W: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for CountAsyncWriter<W> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = std::pin::Pin::new(&mut this.inner).poll_write(cx, bytes);
        if let std::task::Poll::Ready(Ok(written)) = &result {
            if let Some(counts) = &this.counts {
                counts.add_in(*written);
            }
        }
        result
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// A future dropped by disconnect/revocation/shutdown still leaves an end fact.
#[derive(Debug)]
pub(crate) struct CallGuard(Call);
impl CallGuard {
    fn new(call: Call) -> Self {
        call.audit
            .shared
            .active_calls
            .fetch_add(1, Ordering::Release);
        Self(call)
    }

    pub(crate) fn observe_exit(&self, status: i32) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .record
            .exit_status = Some(status);
    }

    pub(crate) fn start_exec(&self, argv: &[String]) {
        self.0.start(Some(argv));
    }

    pub(crate) fn counts(&self) -> Arc<Counts> {
        self.0.counts.clone()
    }
    pub(crate) fn finish(
        self,
        outcome: Outcome,
        status: Option<i32>,
        code: Option<fabric_config::error::ErrorCode>,
    ) {
        self.0.finish(outcome, status, code);
    }
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        self.0.finish(Outcome::Cancelled, None, None);
        self.0
            .audit
            .shared
            .active_calls
            .fetch_sub(1, Ordering::Release);
    }
}

#[derive(Debug)]
struct ServiceObserver(Call);
impl fabric_service_api::audit::Observer for ServiceObserver {
    fn begin(&self) {
        self.0
            .audit
            .shared
            .active_calls
            .fetch_add(1, Ordering::Release);
    }
    fn end(&self) {
        self.0
            .audit
            .shared
            .active_calls
            .fetch_sub(1, Ordering::Release);
    }
    fn start(&self, argv: Option<&[String]>) -> bool {
        self.0.start(argv)
    }
    fn counts(&self) -> Arc<Counts> {
        self.0.counts.clone()
    }
    fn observe_exit(&self, status: i32) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .record
            .exit_status = Some(status);
    }
    fn finish(
        &self,
        outcome: fabric_service_api::audit::Outcome,
        status: Option<i32>,
        code: Option<&str>,
    ) {
        let outcome = match outcome {
            fabric_service_api::audit::Outcome::Ok => Outcome::Ok,
            fabric_service_api::audit::Outcome::SpawnFailed => Outcome::SpawnFailed,
            fabric_service_api::audit::Outcome::Cancelled => Outcome::Cancelled,
        };
        self.0.finish_coded(outcome, status, code);
    }
}

tokio::task_local! { static CURRENT: Option<Call>; }

pub(crate) fn current() -> Option<Call> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}

pub(crate) async fn scope<F: std::future::Future>(call: Option<Call>, future: F) -> F::Output {
    let observer = call.clone().map(|call| {
        Arc::new(ServiceObserver(call)) as Arc<dyn fabric_service_api::audit::Observer>
    });
    CURRENT
        .scope(call, fabric_service_api::audit::scope(observer, future))
        .await
}

pub(crate) fn exec_attempt() -> Option<CallGuard> {
    current().map(CallGuard::new)
}
pub(crate) use fabric_service_api::audit::exit_status;

fn audit_write_warning(last: &mut Option<Instant>, error: &anyhow::Error) {
    if last.is_none_or(|last| last.elapsed() >= Duration::from_secs(1)) {
        fabric_config::log_eprintln!(
            "fabric: audit write failed: {error:#}; failing open and counting lost records"
        );
        *last = Some(Instant::now());
    }
}

struct AuditFiles {
    root: PathBuf,
    options: Options,
    file: Option<File>,
    opened: SystemTime,
    bytes: u64,
    boundary: bool,
    sequence: u64,
    writer_id: u64,
}

impl AuditFiles {
    fn new(root: PathBuf, options: Options) -> Self {
        Self {
            root,
            options,
            file: None,
            opened: SystemTime::now(),
            bytes: 0,
            boundary: false,
            sequence: 0,
            writer_id: rand::random(),
        }
    }

    fn open(&mut self) -> Result<()> {
        if self.file.is_some() {
            return Ok(());
        }
        let path = self.root.join(AUDIT_FILE);
        let existed = path.try_exists()?;
        let mut options = OpenOptions::new();
        options.create(true).read(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        let metadata = file.metadata()?;
        anyhow::ensure!(
            metadata.is_file(),
            "audit destination must be a regular file"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        // A crash may leave a partial final JSON line. Keep that malformed
        // tail visible to the reader without joining the next valid event to it.
        if metadata.len() != 0 {
            file.seek(io::SeekFrom::End(-1))?;
            let mut last = [0];
            file.read_exact(&mut last)?;
            self.boundary |= last[0] != b'\n';
        }
        self.opened = metadata.created().unwrap_or_else(|_| SystemTime::now());
        self.bytes = metadata.len();
        self.file = Some(file);
        if !existed {
            self.file
                .as_ref()
                .expect("audit file opened")
                .sync_all()
                .context("sync new audit file")?;
            File::open(&self.root)?
                .sync_all()
                .context("sync audit directory")?;
        }
        self.prune()?;
        Ok(())
    }

    fn append(&mut self, record: &Record) -> Result<()> {
        let mut encoded = serde_json::to_vec(record)?;
        anyhow::ensure!(
            encoded.len() < MAX_ENCODED_RECORD_BYTES,
            "audit record exceeds encoded size bound"
        );
        encoded.push(b'\n');
        let result = self.append_bytes(&encoded);
        if result.is_err() {
            self.file = None;
            self.boundary = true;
        }
        result
    }

    fn append_bytes(&mut self, encoded: &[u8]) -> Result<()> {
        self.open()?;
        let old = SystemTime::now()
            .duration_since(self.opened)
            .unwrap_or_default()
            >= self.options.max_age;
        if self.bytes != 0
            && (old
                || self.bytes.saturating_add(encoded.len() as u64) > self.options.max_file_bytes)
        {
            self.file.take();
            self.sequence += 1;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let rotated = format!(
                "audit.{now:020}.{:016x}.{:020}.jsonl",
                self.writer_id, self.sequence
            );
            fs::rename(self.root.join(AUDIT_FILE), self.root.join(rotated))
                .context("rotate audit log")?;
            self.boundary = false;
            self.open()?;
            self.prune()?;
        }
        let file = self.file.as_mut().expect("audit file opened");
        if self.boundary {
            file.write_all(b"\n")?;
            self.bytes += 1;
            self.boundary = false;
        }
        file.write_all(encoded)?;
        // Completion means the append was synced, not merely buffered in the
        // process or kernel. Only this dedicated thread waits for storage.
        file.sync_all().context("sync audit record")?;
        self.bytes += encoded.len() as u64;
        Ok(())
    }

    fn prune(&self) -> Result<()> {
        let mut files = audit_paths(&self.root)?;
        files.retain(|path| path.file_name().is_some_and(|name| name != AUDIT_FILE));
        let remove_count = files.len().saturating_sub(self.options.retained);
        for (index, path) in files.into_iter().enumerate() {
            let aged = fs::metadata(&path)?
                .modified()
                .ok()
                .and_then(|modified| SystemTime::now().duration_since(modified).ok())
                .is_some_and(|age| age >= self.options.retention_age);
            if index < remove_count || aged {
                fs::remove_file(path).context("prune retained audit segment")?;
            }
        }
        Ok(())
    }
}

fn audit_paths(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let directory = match fs::read_dir(root) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(files),
        Err(error) => return Err(error).context("read audit directory"),
    };
    for entry in directory {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let rotated = name
            .strip_prefix("audit.")
            .and_then(|s| s.strip_suffix(".jsonl"))
            .is_some_and(|middle| {
                let fields: Vec<_> = middle.split('.').collect();
                fields.len() == 3
                    && fields.iter().all(|field| {
                        !field.is_empty() && field.bytes().all(|b| b.is_ascii_hexdigit())
                    })
            });
        if name == AUDIT_FILE || rotated {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

/// Hold file descriptors across a rotation and rescan once for a newly named
/// segment. Inode identity avoids reading the same file under both names.
fn readable_files(root: &Path) -> Result<Vec<File>> {
    let mut files = std::collections::BTreeMap::new();
    for _ in 0..2 {
        for path in audit_paths(root)? {
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => {
                    return Err(error).with_context(|| format!("read {}", path.display()));
                }
            };
            let metadata = file.metadata()?;
            #[cfg(unix)]
            let identity = {
                use std::os::unix::fs::MetadataExt;
                format!("{}:{}", metadata.dev(), metadata.ino())
            };
            #[cfg(not(unix))]
            let identity = path.to_string_lossy().into_owned();
            files
                .entry(identity)
                .or_insert((metadata.modified().unwrap_or(UNIX_EPOCH), file));
        }
    }
    let mut files: Vec<_> = files.into_values().collect();
    files.sort_by_key(|(modified, _)| *modified);
    Ok(files.into_iter().map(|(_, file)| file).collect())
}

#[derive(Debug, Serialize)]
pub struct AuditRead {
    pub records: Vec<Record>,
    pub malformed_records: u64,
}

pub fn read(root: &Path, since: Option<SystemTime>, peer: Option<&str>) -> Result<AuditRead> {
    let mut result = AuditRead {
        records: Vec::new(),
        malformed_records: 0,
    };
    for file in readable_files(root)? {
        let mut reader = BufReader::new(file);
        loop {
            let mut line = Vec::new();
            let bytes = reader
                .by_ref()
                .take((MAX_ENCODED_RECORD_BYTES + 1) as u64)
                .read_until(b'\n', &mut line)?;
            if bytes == 0 {
                break;
            }
            if bytes > MAX_ENCODED_RECORD_BYTES {
                result.malformed_records += 1;
                // Discard the remainder without allocating an unbounded line.
                while !line.ends_with(b"\n") {
                    line.clear();
                    if reader.by_ref().take(8192).read_until(b'\n', &mut line)? == 0 {
                        break;
                    }
                }
                continue;
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let record: Record = match serde_json::from_slice(&line) {
                Ok(record) => record,
                Err(_) => {
                    result.malformed_records += 1;
                    continue;
                }
            };
            let time = match &record {
                Record::AuditLoss { last_time, .. } => last_time,
                _ => &record.call().expect("call record").time,
            };
            let time = OffsetDateTime::parse(time, &Rfc3339)
                .ok()
                .map(SystemTime::from);
            if since.is_some_and(|since| time.is_none_or(|time| time < since)) {
                continue;
            }
            if let (Some(peer), Some(call)) = (peer, record.call())
                && call.peer != peer
                && call.name.as_deref() != Some(peer)
            {
                continue;
            }
            result.records.push(record);
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(peer: &str, name: &str, session: &str) -> Record {
        Record::ExecEnd(CallRecord {
            time: timestamp(SystemTime::now()),
            peer: peer.into(),
            name: Some(name.into()),
            service: Some("exec".into()),
            argv: Some(vec!["true".into()]),
            session_id: session.into(),
            outcome: Some(Outcome::Ok),
            code: None,
            exit_status: Some(0),
            duration_millis: Some(1),
            bytes_in: 0,
            bytes_out: 0,
        })
    }

    async fn run_audited_exec(audit: &Audit, argv: Vec<String>) -> Result<(Vec<u8>, i32)> {
        use tokio::io::{AsyncWriteExt, split};
        let call = audit.call(
            "peer-a".into(),
            Some("laptop".into()),
            Some("exec".into()),
            Kind::Exec,
        );
        let (mut client, server) = tokio::io::duplex(65536);
        let serving = tokio::spawn(async move {
            let (mut read, mut write) = split(server);
            scope(
                Some(call),
                crate::services::exec::serve_exec_session(&mut read, &mut write, "peer-a"),
            )
            .await
        });
        crate::services::exec::write_client_argv(&mut client, &argv).await?;
        let mut output = Vec::new();
        let code = loop {
            match crate::services::exec::read_server_frame(&mut client).await? {
                Some(
                    crate::services::exec::ServerFrame::Stdout(bytes)
                    | crate::services::exec::ServerFrame::Stderr(bytes),
                ) => output.extend(bytes),
                Some(crate::services::exec::ServerFrame::Exit(code)) => break code,
                Some(crate::services::exec::ServerFrame::Error(_)) => {}
                None => anyhow::bail!("audited exec closed before exit"),
            }
        };
        client.shutdown().await?;
        serving.await??;
        Ok((output, code))
    }

    #[tokio::test]
    async fn real_shell_start_and_end_do_not_retain_keystrokes_or_output() -> Result<()> {
        use tokio::io::{AsyncWriteExt, split};
        let root = tempfile::tempdir()?;
        let audit = Audit::start(root.path());
        let call = audit.call(
            "peer-a".into(),
            Some("laptop".into()),
            Some("shell".into()),
            Kind::Shell,
        );
        let (mut client, server) = tokio::io::duplex(65536);
        let serving = tokio::spawn(async move {
            let (mut read, mut write) = split(server);
            scope(
                Some(call),
                crate::services::shell::serve_shell_session(&mut read, &mut write, "peer-a"),
            )
            .await
        });
        let input = b"printf shell-stdin-sentinel-4421; exit 3\n";
        crate::services::shell::write_client_stdin(&mut client, input).await?;
        let mut output = Vec::new();
        let code = loop {
            match crate::services::shell::read_server_frame(&mut client).await? {
                Some(crate::services::shell::ServerFrame::Output(bytes)) => output.extend(bytes),
                Some(crate::services::shell::ServerFrame::Exit(code)) => break code,
                Some(crate::services::shell::ServerFrame::Status(_)) => {}
                Some(crate::services::shell::ServerFrame::Error(error)) => anyhow::bail!("{error}"),
                None => anyhow::bail!("audited shell closed before exit"),
            }
        };
        assert_eq!(code, 3);
        assert!(String::from_utf8_lossy(&output).contains("shell-stdin-sentinel-4421"));
        client.shutdown().await?;
        serving.await??;
        assert!(audit.drain(Duration::from_secs(2)).await);
        let records = read(root.path(), None, None)?.records;
        assert_eq!(records.len(), 2);
        assert!(matches!(records[0], Record::ShellStart(_)));
        assert!(matches!(records[1], Record::ShellEnd(_)));
        let end = records[1].call().unwrap();
        assert_eq!(records[0].call().unwrap().session_id, end.session_id);
        assert!(end.argv.is_none());
        assert_eq!(end.exit_status, Some(3));
        assert_eq!(end.bytes_in, input.len() as u64);
        assert_eq!(end.bytes_out, output.len() as u64);
        assert!(
            !fs::read_to_string(root.path().join(AUDIT_FILE))?
                .contains("shell-stdin-sentinel-4421")
        );
        Ok(())
    }

    #[tokio::test]
    async fn shell_signal_exit_does_not_invent_the_pty_fallback_as_a_real_exit_status() -> Result<()>
    {
        use tokio::io::split;
        let root = tempfile::tempdir()?;
        let audit = Audit::start(root.path());
        let call = audit.call("peer-a".into(), None, Some("shell".into()), Kind::Shell);
        let (mut client, server) = tokio::io::duplex(65536);
        let serving = tokio::spawn(async move {
            let (mut read, mut write) = split(server);
            scope(
                Some(call),
                crate::services::shell::serve_shell_session(&mut read, &mut write, "peer-a"),
            )
            .await
        });
        crate::services::shell::write_client_stdin(&mut client, b"kill -KILL $$\n").await?;
        let code = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match crate::services::shell::read_server_frame(&mut client).await? {
                    Some(crate::services::shell::ServerFrame::Exit(code)) => {
                        return Ok::<_, anyhow::Error>(code);
                    }
                    Some(_) => {}
                    None => anyhow::bail!("shell closed without its wire exit code"),
                }
            }
        })
        .await??;
        assert_eq!(code, 1, "retain the existing PTY wire fallback");
        serving.await??;
        assert!(audit.drain(Duration::from_secs(2)).await);
        let records = read(root.path(), None, None)?.records;
        assert_eq!(records.len(), 2);
        let Record::ShellEnd(end) = &records[1] else {
            panic!("missing shell end");
        };
        assert!(end.exit_status.is_none(), "a signal is not a real exit 1");
        assert_eq!(end.outcome, Some(Outcome::Ok));
        Ok(())
    }

    #[tokio::test]
    async fn real_exec_records_nonzero_exit_payload_counts_and_spawn_failure_without_output_content()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let script = root.path().join("emit.sh");
        fs::write(&script, b"AUDIT_PRIVATE_VALUE=environment-sentinel-4916; export AUDIT_PRIVATE_VALUE; printf '%s' \"$AUDIT_PRIVATE_VALUE\"; printf output-sentinel-6124 >&2; exit 7\n")?;
        let mut files = AuditFiles::new(
            root.path().into(),
            Options {
                max_file_bytes: 1,
                ..Options::default()
            },
        );
        let audit = Audit::with_sink(16, 65536, move |record| files.append(record));
        let argv = vec!["/bin/sh".into(), script.to_string_lossy().into_owned()];
        let (output, code) = run_audited_exec(&audit, argv.clone()).await?;
        assert_eq!(code, 7);
        assert_eq!(
            output.len(),
            b"environment-sentinel-4916output-sentinel-6124".len()
        );
        let (_, code) = run_audited_exec(&audit, vec!["/missing-audit-command".into()]).await?;
        assert_eq!(code, 127);
        assert!(audit.drain(Duration::from_secs(2)).await);
        let records = read(root.path(), None, None)?.records;
        assert_eq!(records.len(), 4);
        let end = records[1].call().unwrap();
        assert_eq!(end.argv.as_ref(), Some(&argv));
        assert_eq!(end.exit_status, Some(7));
        assert_eq!(end.outcome, Some(Outcome::Ok));
        assert_eq!(end.bytes_in, 0);
        assert_eq!(end.bytes_out, output.len() as u64);
        let failed = records[3].call().unwrap();
        assert_eq!(failed.outcome, Some(Outcome::SpawnFailed));
        assert!(
            failed.exit_status.is_none(),
            "a failed spawn has no child exit status"
        );
        assert_eq!(failed.code.as_deref(), Some("fabric:spawn_failed"));
        let paths = audit_paths(root.path())?;
        assert_eq!(paths.len(), 4, "each real event must exercise rotation");
        let text = paths
            .iter()
            .map(fs::read_to_string)
            .collect::<io::Result<Vec<_>>>()?
            .join("\n");
        for secret in [
            "environment-sentinel-4916",
            "output-sentinel-6124",
            "AUDIT_PRIVATE_VALUE",
        ] {
            assert!(!text.contains(secret), "audit retained process content");
        }
        Ok(())
    }

    #[tokio::test]
    async fn lifecycle_correlates_start_end_refusal_and_cancellation_without_duplicate_events() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let records = output.clone();
        let audit = Audit::with_sink(16, 16384, move |record| {
            records.lock().unwrap().push(record.clone());
            Ok(())
        });
        let call = audit.call(
            "peer-a".into(),
            Some("laptop".into()),
            Some("exec".into()),
            Kind::Exec,
        );
        call.session_id("durable-session");
        scope(Some(call.clone()), async {
            let guard = exec_attempt().unwrap();
            guard.start_exec(&["true".into()]);
            guard.counts().add_out(5);
            guard.finish(Outcome::Ok, Some(7), None);
        })
        .await;
        // Reattaching the transport without starting a process adds no event.
        scope(Some(call), async {}).await;
        let denied = audit.call("peer-a".into(), None, Some("exec".into()), Kind::Exec);
        denied.refused(fabric_config::error::ErrorCode::PeerNotPermittedForService);
        let shell = audit.call(
            "peer-b".into(),
            Some("server".into()),
            Some("shell".into()),
            Kind::Shell,
        );
        scope(Some(shell), async {
            drop(fabric_service_api::audit::shell_start().unwrap());
        })
        .await;
        assert!(audit.drain(Duration::from_secs(2)).await);
        let records = output.lock().unwrap();
        assert_eq!(records.len(), 5);
        assert!(matches!(records[0], Record::ExecStart(_)));
        let Record::ExecEnd(end) = &records[1] else {
            panic!("missing exec end");
        };
        assert_eq!(end.session_id, "durable-session");
        assert_eq!(end.exit_status, Some(7));
        assert_eq!(end.bytes_out, 5);
        let Record::Refused(denied) = &records[2] else {
            panic!("missing refusal");
        };
        assert!(denied.argv.is_none());
        assert_eq!(
            denied.code.as_deref(),
            Some("fabric:peer_not_permitted_for_service")
        );
        assert!(matches!(
            &records[4],
            Record::ShellEnd(CallRecord {
                outcome: Some(Outcome::Cancelled),
                exit_status: None,
                ..
            })
        ));
        assert_eq!(
            records[3].call().unwrap().session_id,
            records[4].call().unwrap().session_id
        );
    }

    #[tokio::test]
    async fn blocked_writer_bounds_entries_and_recovers_exact_loss_window() {
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let output = Arc::new(Mutex::new(Vec::new()));
        let records = output.clone();
        let mut first = true;
        let audit = Audit::with_sink(1, 4096, move |record| {
            if first {
                first = false;
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }
            records.lock().unwrap().push(record.clone());
            Ok(())
        });
        audit.record(call("peer-a", "laptop", "first"));
        tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
            .await
            .unwrap();
        audit.record(call("peer-a", "laptop", "second"));
        let before_loss = SystemTime::now();
        audit.record(call("peer-a", "laptop", "third"));
        audit.record(call("peer-a", "laptop", "fourth"));
        let after_loss = SystemTime::now();
        assert_eq!(audit.status().dropped_records, 2);
        assert_eq!(audit.status().queued_records, 2);
        assert!(!audit.drain(Duration::from_millis(20)).await);
        release_tx.send(()).unwrap();
        assert!(audit.drain(Duration::from_secs(2)).await);
        assert_eq!(audit.status().dropped_records, 2);
        assert_eq!(audit.status().queued_records, 0);
        assert_eq!(audit.shared.bytes.load(Ordering::Acquire), 0);
        let output = output.lock().unwrap();
        assert_eq!(output.len(), 3);
        let Record::AuditLoss {
            count,
            first_time,
            last_time,
            ..
        } = &output[1]
        else {
            panic!("missing loss record");
        };
        assert_eq!(*count, 2);
        let first = SystemTime::from(OffsetDateTime::parse(first_time, &Rfc3339).unwrap());
        let last = SystemTime::from(OffsetDateTime::parse(last_time, &Rfc3339).unwrap());
        assert!(first >= before_loss && last >= first && last <= after_loss);
    }

    #[tokio::test]
    async fn oversized_record_is_counted_and_reported_without_a_new_call() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let records = output.clone();
        let audit = Audit::with_sink(1, 1, move |record| {
            records.lock().unwrap().push(record.clone());
            Ok(())
        });
        audit.record(call("peer-a", "laptop", "oversized"));
        assert_eq!(audit.status().dropped_records, 1);
        assert_eq!(audit.status().queued_records, 0);
        assert!(audit.drain(Duration::from_secs(2)).await);
        let output = output.lock().unwrap();
        assert!(matches!(&output[..], [Record::AuditLoss { count: 1, .. }]));
    }

    #[tokio::test]
    async fn io_failure_fails_open_and_recovery_emits_loss() -> Result<()> {
        let root = tempfile::tempdir()?;
        fs::create_dir(root.path().join(AUDIT_FILE))?;
        let audit = Audit::start(root.path());
        audit.record(call("peer-a", "laptop", "lost"));
        tokio::time::timeout(Duration::from_secs(2), async {
            while audit.status().dropped_records == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        fs::remove_dir(root.path().join(AUDIT_FILE))?;
        assert!(audit.drain(Duration::from_secs(3)).await);
        let output = read(root.path(), None, None)?;
        assert!(matches!(
            &output.records[..],
            [Record::AuditLoss { count: 1, .. }]
        ));
        Ok(())
    }

    #[test]
    fn size_and_age_rotation_keep_bounded_private_segments() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut files = AuditFiles::new(
            root.path().into(),
            Options {
                max_file_bytes: 1,
                retained: 2,
                ..Options::default()
            },
        );
        for index in 0..5 {
            files.append(&call("peer-a", "laptop", &index.to_string()))?;
        }
        assert_eq!(audit_paths(root.path())?.len(), 3);
        assert_eq!(read(root.path(), None, None)?.records.len(), 3);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in audit_paths(root.path())? {
                assert_eq!(fs::metadata(path)?.permissions().mode() & 0o777, 0o600);
            }
        }
        let aged_root = tempfile::tempdir()?;
        let mut aged = AuditFiles::new(
            aged_root.path().into(),
            Options {
                max_age: Duration::ZERO,
                ..Options::default()
            },
        );
        aged.append(&call("peer-a", "laptop", "a"))?;
        aged.append(&call("peer-a", "laptop", "b"))?;
        assert_eq!(audit_paths(aged_root.path())?.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn startup_after_a_partial_append_preserves_the_next_event() -> Result<()> {
        let root = tempfile::tempdir()?;
        fs::write(root.path().join(AUDIT_FILE), b"{\"event\":\"exec_start\"")?;
        let audit = Audit::start(root.path());
        audit.record(call("peer-a", "laptop", "after-crash"));
        assert!(audit.drain(Duration::from_secs(2)).await);
        assert_eq!(audit.status().dropped_records, 0);
        let output = read(root.path(), None, None)?;
        assert_eq!(output.malformed_records, 1);
        assert_eq!(output.records.len(), 1);
        assert_eq!(output.records[0].call().unwrap().session_id, "after-crash");
        Ok(())
    }

    #[test]
    fn filters_include_global_loss_and_report_malformed_records() -> Result<()> {
        let root = tempfile::tempdir()?;
        let before = SystemTime::now() - Duration::from_secs(60);
        let mut earlier = call("peer-a", "laptop", "old");
        if let Record::ExecEnd(ref mut call) = earlier {
            call.time = timestamp(before);
        }
        let current = call("peer-b", "server", "new");
        let loss = Record::AuditLoss {
            time: timestamp(SystemTime::now()),
            count: 2,
            first_time: timestamp(before),
            last_time: timestamp(SystemTime::now()),
        };
        let mut file = File::create(root.path().join(AUDIT_FILE))?;
        for record in [earlier, current, loss] {
            serde_json::to_writer(&mut file, &record)?;
            file.write_all(b"\n")?;
        }
        file.write_all(b"invalid\n{\"event\":")?;
        fs::write(
            root.path().join("audit.notes.jsonl"),
            b"not an audit segment",
        )?;
        let output = read(
            root.path(),
            Some(SystemTime::now() - Duration::from_secs(30)),
            Some("server"),
        )?;
        assert_eq!(output.records.len(), 2);
        assert_eq!(output.malformed_records, 2);
        assert!(matches!(
            output.records[1],
            Record::AuditLoss { count: 2, .. }
        ));
        Ok(())
    }
}
