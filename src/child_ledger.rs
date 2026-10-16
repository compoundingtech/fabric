//! Best-effort durable ownership of service children. Storage is bounded so
//! remote commands remain available when an operator needs to repair the disk.
use fabric_service_api::child_tracking::{Owner, Record};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

const MAX_RECORDS: usize = 4096;
// Leave most of the existing 20-second spawn budget for the OS. A stalled
// ownership disk must not hold the only command that can repair it.
const STORAGE_LIMIT: Duration = Duration::from_millis(250);
type StorageJob = Box<dyn FnOnce() + Send>;
static STORAGE: OnceLock<Result<mpsc::SyncSender<StorageJob>, String>> = OnceLock::new();

fn storage<T: Send + 'static>(
    work: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let workers = STORAGE.get_or_init(|| {
        let (send, receive) = mpsc::sync_channel::<StorageJob>(64);
        let receive = Arc::new(Mutex::new(receive));
        for n in 0..4 {
            let receive = receive.clone();
            std::thread::Builder::new()
                .name(format!("fabric-child-storage-{n}"))
                .spawn(move || {
                    loop {
                        let Ok(work) = receive.lock().unwrap().recv() else {
                            break;
                        };
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
                    }
                })
                .map_err(|error| error.to_string())?;
        }
        Ok(send)
    });
    let (reply, result) = mpsc::sync_channel(1);
    let expired = Arc::new(AtomicBool::new(false));
    let skip = expired.clone();
    workers
        .as_ref()
        .map_err(|error| io::Error::other(error.clone()))?
        .try_send(Box::new(move || {
            if !skip.load(Ordering::Acquire) {
                let _ = reply.send(work());
            }
        }))
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "child ownership storage queue full",
            )
        })?;
    let result = result.recv_timeout(STORAGE_LIMIT).map_err(|error| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("child ownership storage exceeded 250 ms: {error}"),
        )
    });
    expired.store(true, Ordering::Release);
    result?
}

#[derive(Debug)]
struct Unrecorded;
impl Record for Unrecorded {}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    boot: String,
    start: u64,
    subsecond: u64,
}
#[derive(Debug)]
struct Process {
    identity: Identity,
    group: i32,
    parent: u32,
    running: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    pid: u32,
    identity: Identity,
    session_id: String,
}
impl Entry {
    fn filename(&self) -> String {
        let key = format!(
            "{}:{}:{}",
            self.identity.boot, self.identity.start, self.identity.subsecond
        );
        format!(
            "{}-{}.json",
            self.pid,
            blake3::hash(key.as_bytes()).to_hex()
        )
    }
    fn kill_if_matching(&self) -> io::Result<()> {
        if let Some(current) = process(self.pid)?
            && current.identity == self.identity
            && current.running
        {
            let target = if current.group == self.pid as i32 {
                -(self.pid as i32)
            } else {
                self.pid as i32
            };
            if unsafe { libc::kill(target, libc::SIGKILL) } != 0
                && io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
            {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
    fn still_running(&self) -> io::Result<bool> {
        Ok(process(self.pid)?.is_some_and(|p| p.identity == self.identity && p.running))
    }
}
#[derive(Debug)]
pub(crate) struct Ledger {
    inner: Arc<Inner>,
}
#[derive(Debug)]
struct Inner {
    dir: PathBuf,
    warning: Mutex<Option<Instant>>,
    swept: AtomicBool,
    write: Mutex<()>,
    cleanup: Option<mpsc::SyncSender<Entry>>,
}
#[derive(Debug)]
struct ChildRecord {
    ledger: Arc<Inner>,
    entry: Entry,
}
impl Record for ChildRecord {}
impl Drop for ChildRecord {
    fn drop(&mut self) {
        // Never wait for disk or child exit on a runtime worker. A full queue
        // leaves the synced record for the next startup's identity-safe sweep.
        if let Some(cleanup) = &self.ledger.cleanup {
            let _ = cleanup.try_send(self.entry.clone());
        }
    }
}
impl Ledger {
    pub(crate) fn start(root: &Path) -> Self {
        let dir = root.join("children");
        let (send, recv) = mpsc::sync_channel::<Entry>(1024);
        let path = dir.to_owned();
        let cleanup = std::thread::Builder::new()
            .name("fabric-child-cleanup".into())
            .spawn(move || {
                let mut pending: Vec<Entry> = Vec::new();
                loop {
                    match recv.recv_timeout(Duration::from_millis(100)) {
                        Ok(entry) => {
                            // Also covers a spawn/registration error after exec:
                            // releasing ownership always ends that exact child.
                            let _ = entry.kill_if_matching();
                            pending.push(entry);
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            if pending.is_empty() {
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(100));
                        }
                        Err(_) => {}
                    }
                    pending.retain(|entry| {
                        if entry.still_running().unwrap_or(true) {
                            return true;
                        }
                        match fs::remove_file(path.join(entry.filename())) {
                            Ok(()) => false,
                            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
                            Err(_) => false, // Retain the file, not an unbounded retry task.
                        }
                    });
                    // Live unreaped children retain their on-disk entry regardless
                    // of metadata cleanup pressure. The next startup handles it.
                    if pending.len() >= MAX_RECORDS {
                        pending.clear();
                    }
                }
            });
        let ledger = Self {
            inner: Arc::new(Inner {
                dir: dir.clone(),
                warning: Mutex::new(None),
                swept: AtomicBool::new(false),
                write: Mutex::new(()),
                cleanup: cleanup.as_ref().ok().map(|_| send),
            }),
        };
        if let Err(error) = cleanup {
            ledger.warn(&error);
        }
        let startup = ledger.inner.clone();
        if let Err(error) = storage(move || {
            // A slow startup sweep must not discover records from commands
            // admitted after its wait timed out. Commits share this barrier.
            let _write = startup.write.lock().unwrap();
            let result = (|| {
                fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(&dir)?;
                Self::sweep(&dir)
            })();
            startup.swept.store(true, Ordering::Release);
            if let Err(error) = &result {
                Ledger {
                    inner: startup.clone(),
                }
                .warn(error);
            }
            result
        }) {
            ledger.warn(&error);
        }
        ledger
    }
    fn warn(&self, error: &io::Error) {
        let mut last = self.inner.warning.lock().unwrap();
        if last.is_none_or(|at| at.elapsed() >= Duration::from_secs(60)) {
            *last = Some(Instant::now());
            tracing::warn!(path = %self.inner.dir.display(), %error,
                "child ownership storage unavailable; allowing commands without durable crash cleanup");
        }
    }
    fn sweep(dir: &Path) -> io::Result<()> {
        let mut selected = Vec::new();
        for (count, file) in fs::read_dir(dir)?.enumerate() {
            if count >= MAX_RECORDS {
                return Err(io::Error::other("child ownership record limit exceeded"));
            }
            let file = file?;
            let path = file.path();
            if !file.file_type()?.is_file() || path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let mut bytes = Vec::new();
            File::open(&path)?.take(4097).read_to_end(&mut bytes)?;
            let entry = if bytes.len() <= 4096 {
                serde_json::from_slice::<Entry>(&bytes).ok()
            } else {
                None
            };
            let Some(entry) = entry.filter(|e| {
                e.pid > 1
                    && e.pid <= i32::MAX as u32
                    && e.session_id.len() <= 128
                    && file.file_name() == e.filename().as_str()
            }) else {
                tracing::warn!(path = %path.display(), "invalid child ownership record; no process signalled");
                // An incomplete pre-exec commit never released its command.
                fs::remove_file(&path)?;
                continue;
            };
            // Identity checked against the kernel; a reused pid is skipped.
            entry.kill_if_matching()?;
            selected.push(entry);
        }
        let deadline = Instant::now() + Duration::from_secs(1);
        while !selected.is_empty() {
            let mut remaining = Vec::new();
            for entry in selected {
                if entry.still_running()? {
                    remaining.push(entry);
                } else {
                    match fs::remove_file(dir.join(entry.filename())) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                }
            }
            if remaining.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other(
                    "previous service children did not exit after SIGKILL",
                ));
            }
            std::thread::sleep(Duration::from_millis(5));
            selected = remaining;
        }
        Ok(())
    }
}
impl Owner for Ledger {
    fn before_exec(&self, pid: u32, session: &str) -> io::Result<Box<dyn Record>> {
        let inner = self.inner.clone();
        let session = session.to_owned();
        match storage(move || {
            let ledger = Ledger { inner };
            let result = ledger.commit(pid, &session);
            if let Err(error) = &result {
                ledger.warn(error);
            }
            result
        }) {
            Ok(entry) => Ok(Box::new(ChildRecord {
                ledger: self.inner.clone(),
                entry,
            })),
            Err(error) => {
                self.warn(&error);
                Ok(Box::new(Unrecorded))
            }
        }
    }
}
impl Ledger {
    fn commit(&self, pid: u32, session: &str) -> io::Result<Entry> {
        if session.is_empty() || session.len() > 128 {
            return Err(io::Error::other(
                "invalid child ownership session identifier",
            ));
        }
        // Capture identity before waiting for a storage lock or disk. A late
        // commit retains this identity, never the identity of a reused pid.
        let child = process(pid)?
            .ok_or_else(|| io::Error::other("child disappeared before ownership commit"))?;
        if child.parent != std::process::id() || child.group != pid as i32 || !child.running {
            return Err(io::Error::other(
                "child identity or process group changed before ownership commit",
            ));
        }
        let _write = self.inner.write.lock().unwrap();
        if !self.inner.swept.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "child ownership startup sweep has not completed",
            ));
        }
        if fs::read_dir(&self.inner.dir)?.take(MAX_RECORDS).count() >= MAX_RECORDS {
            return Err(io::Error::other("child ownership record limit reached"));
        }
        let entry = Entry {
            pid,
            identity: child.identity,
            session_id: session.into(),
        };
        let path = self.inner.dir.join(entry.filename());
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        let write = (|| {
            file.write_all(&serde_json::to_vec(&entry)?)?;
            file.sync_all()?;
            File::open(&self.inner.dir)?.sync_all()
        })();
        if let Err(error) = write {
            let _ = fs::remove_file(path);
            return Err(error);
        }
        // Return plain metadata: a late commit must never drop a ChildRecord
        // and kill a command that already continued after the storage bound.
        // Its durable entry can instead be checked by the next startup sweep.
        Ok(entry)
    }
}

#[cfg(target_os = "linux")]
fn process(pid: u32) -> io::Result<Option<Process>> {
    let path = PathBuf::from(format!("/proc/{pid}/stat"));
    let stat = match fs::read_to_string(&path) {
        Ok(stat) => stat,
        Err(e) if e.kind() == io::ErrorKind::NotFound || e.raw_os_error() == Some(libc::ESRCH) => {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let fields: Vec<_> = stat
        .rsplit_once(')')
        .ok_or_else(|| io::Error::other("invalid kernel process stat"))?
        .1
        .split_whitespace()
        .collect();
    let parse = |index: usize| {
        fields
            .get(index)
            .ok_or_else(|| io::Error::other("missing kernel process stat field"))?
            .parse::<u64>()
            .map_err(io::Error::other)
    };
    Ok(Some(Process {
        identity: Identity {
            boot: fs::read_to_string("/proc/sys/kernel/random/boot_id")?
                .trim()
                .into(),
            start: parse(19)?,
            subsecond: 0,
        },
        group: i32::try_from(parse(2)?).map_err(io::Error::other)?,
        parent: u32::try_from(parse(1)?).map_err(io::Error::other)?,
        running: fields.first().is_some_and(|s| !matches!(*s, "Z" | "X")),
    }))
}
#[cfg(target_os = "macos")]
fn process(pid: u32) -> io::Result<Option<Process>> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    let got = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    if got != size {
        let error = io::Error::last_os_error();
        if got == 0 && matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::ENOENT)) {
            return Ok(None);
        }
        return Err(error);
    }
    Ok(Some(Process {
        identity: Identity {
            boot: String::new(),
            start: info.pbi_start_tvsec,
            subsecond: info.pbi_start_tvusec,
        },
        group: info.pbi_pgid as i32,
        parent: info.pbi_ppid,
        running: info.pbi_status != 5, // SZOMB
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        os::unix::process::CommandExt,
        process::{Child, Command},
    };
    struct Cleanup(Child);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    fn child() -> Cleanup {
        Cleanup(
            Command::new("sleep")
                .arg("60")
                .process_group(0)
                .spawn()
                .unwrap(),
        )
    }
    #[test]
    fn startup_ends_a_matching_owned_child_and_removes_its_record() {
        let root = tempfile::tempdir().unwrap();
        let ledger = Ledger::start(root.path());
        let mut child = child();
        let record = ledger.before_exec(child.0.id(), "old-session").unwrap();
        assert_eq!(
            fs::read_dir(root.path().join("children")).unwrap().count(),
            1
        );
        Ledger::sweep(&root.path().join("children")).unwrap();
        assert!(!child.0.wait().unwrap().success());
        assert_eq!(
            fs::read_dir(root.path().join("children")).unwrap().count(),
            0
        );
        drop(record);
    }
    #[test]
    fn a_stale_pid_record_never_signals_a_different_process_identity() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("children");
        fs::create_dir(&dir).unwrap();
        let mut child = child();
        let mut identity = process(child.0.id()).unwrap().unwrap().identity;
        identity.start += 1;
        let entry = Entry {
            pid: child.0.id(),
            identity,
            session_id: "stale-session".into(),
        };
        fs::write(
            dir.join(entry.filename()),
            serde_json::to_vec(&entry).unwrap(),
        )
        .unwrap();
        Ledger::sweep(&dir).unwrap();
        assert!(child.0.try_wait().unwrap().is_none());
        assert_eq!(fs::read_dir(dir).unwrap().count(), 0);
    }
    #[tokio::test]
    async fn a_stalled_commit_allows_exec_and_a_late_write_does_not_kill_it() {
        let root = tempfile::tempdir().unwrap();
        let ledger = Arc::new(Ledger::start(root.path()));
        let lock = ledger.inner.write.lock().unwrap();
        let children = fabric_service_api::child_tracking::Children::default()
            .with_owner(ledger.clone(), "stalled-storage".into());
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .args(["-c", "printf running; exec sleep 60"])
            .stdout(std::process::Stdio::piped());
        let mut child = tokio::time::timeout(
            Duration::from_secs(3),
            children.scope(fabric_service_api::child_spawn::spawn(
                "stalled ownership storage",
                command,
            )),
        )
        .await
        .unwrap()
        .unwrap();
        use tokio::io::AsyncReadExt;
        let mut output = [0; 7];
        tokio::time::timeout(
            Duration::from_secs(1),
            child.stdout.as_mut().unwrap().read_exact(&mut output),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&output, b"running");
        assert!(!children.complete());
        drop(lock);
        // The in-flight write may finish after fail-open. It leaves metadata
        // for startup instead of dropping an ownership guard and killing exec.
        tokio::time::timeout(Duration::from_secs(3), async {
            while fs::read_dir(root.path().join("children")).unwrap().count() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(child.try_wait().unwrap().is_none());
        child.kill().await.unwrap();
    }
    #[test]
    fn a_sweep_error_does_not_disable_subsequent_records() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("children");
        fs::create_dir(&dir).unwrap();
        // Force the real sweep limit error without root-sensitive permissions.
        // Removing those files then permits records on this same ledger.
        for n in 0..=MAX_RECORDS {
            fs::write(dir.join(format!("ignored-{n}")), b"").unwrap();
        }
        let ledger = Ledger::start(root.path());
        for n in 0..=MAX_RECORDS {
            fs::remove_file(dir.join(format!("ignored-{n}"))).unwrap();
        }
        let child = child();
        let record = ledger
            .before_exec(child.0.id(), "after-sweep-error")
            .unwrap();
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        drop(record);
    }
    #[test]
    fn unavailable_ownership_storage_allows_spawns_without_failing_startup() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("children"), b"occupied").unwrap();
        let ledger = Ledger::start(root.path());
        let child = child();
        assert!(
            ledger
                .before_exec(child.0.id(), "unrecorded-session")
                .is_ok()
        );
    }
}
