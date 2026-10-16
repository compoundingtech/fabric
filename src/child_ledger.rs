//! Durable ownership of service children. User code is released only after its
//! pid, kernel start identity and session identifier are synced by the parent.
use fabric_service_api::child_tracking::{Owner, Record};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

const MAX_RECORDS: usize = 4096;
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
    fault: Option<String>,
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
        let opened = Self::open(&dir);
        match opened {
            Ok(ledger) => ledger,
            Err(error) => {
                tracing::warn!(path = %dir.display(), %error,
                    "child ownership storage unavailable; child commands will be refused");
                Self {
                    inner: Arc::new(Inner {
                        dir,
                        fault: Some(error.to_string()),
                        write: Mutex::new(()),
                        cleanup: None,
                    }),
                }
            }
        }
    }
    fn open(dir: &Path) -> io::Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        Self::sweep(dir)?;
        let (send, recv) = mpsc::sync_channel::<Entry>(1024);
        let path = dir.to_owned();
        std::thread::Builder::new()
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
            })?;
        Ok(Self {
            inner: Arc::new(Inner {
                dir: dir.into(),
                fault: None,
                write: Mutex::new(()),
                cleanup: Some(send),
            }),
        })
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
        if let Some(fault) = &self.inner.fault {
            return Err(io::Error::other(format!(
                "child ownership storage unavailable: {fault}"
            )));
        }
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
        Ok(Box::new(ChildRecord {
            ledger: self.inner.clone(),
            entry,
        }))
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
    #[test]
    fn unavailable_ownership_storage_refuses_spawns_without_failing_startup() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("children"), b"occupied").unwrap();
        let ledger = Ledger::start(root.path());
        let child = child();
        assert!(ledger.before_exec(child.0.id(), "blocked-session").is_err());
    }
}
