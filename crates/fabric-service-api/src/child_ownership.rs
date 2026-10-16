//! Process ownership below the service interface. No daemon paths or policy.
use crate::child_tracking::{Children, Record};
use std::{
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
    os::{fd::AsRawFd, unix::net::UnixStream},
    process::ExitStatus,
    sync::{Arc, Mutex, OnceLock, Weak, mpsc},
};
use tokio::process::{Child as TokioChild, Command};

/// A command and its process group belong to the caller until reaped/dropped.
/// The durable record, if supplied by the daemon, outlives the kill signal.
#[derive(Debug)]
pub struct Child {
    inner: TokioChild,
    pid: u32,
    _record: Option<Box<dyn Record>>,
    killed_group: bool,
}
impl Deref for Child {
    type Target = TokioChild;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for Child {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
impl Child {
    fn kill_group(&mut self) {
        if self.killed_group {
            return;
        }
        self.killed_group = true;
        // After reaping, a live leader with this pid may already be unrelated.
        if self.inner.id().is_none() && unsafe { libc::kill(self.pid as libc::pid_t, 0) } == 0 {
            return;
        }
        unsafe {
            libc::kill(-(self.pid as libc::pid_t), libc::SIGKILL);
        }
    }
    pub fn start_kill(&mut self) -> io::Result<()> {
        self.kill_group();
        self.inner.start_kill()
    }
    pub async fn kill(&mut self) -> io::Result<()> {
        self.start_kill()?;
        self.wait().await.map(|_| ())
    }
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        let status = self.inner.wait().await?;
        // Normal completion ends this command's ownership. Preserve existing
        // detached/background behavior; cancellation/drop owns group cleanup.
        self.killed_group = true;
        Ok(status)
    }
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        let status = self.inner.try_wait()?;
        if status.is_some() {
            self.killed_group = true;
        }
        Ok(status)
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        self.kill_group();
    }
}

static PRIVATE_FILES: Mutex<Vec<Weak<std::fs::File>>> = Mutex::new(Vec::new());
pub(crate) fn protect(file: Arc<std::fs::File>) {
    let mut files = PRIVATE_FILES.lock().unwrap();
    files.retain(|file| file.strong_count() != 0);
    files.push(Arc::downgrade(&file));
}

type Job = Box<dyn FnOnce() + Send>;
static WORKERS: OnceLock<Result<mpsc::SyncSender<Job>, String>> = OnceLock::new();
static RECORDERS: OnceLock<Result<mpsc::SyncSender<Job>, String>> = OnceLock::new();

// PDEATHSIG follows the thread which forked, not the process as a whole.
// Tokio blocking workers can retire; these native spawn workers live until
// process exit. A stuck spawn cannot create an unbounded queue or thread set.
pub(crate) fn submit(job: Job) -> io::Result<()> {
    submit_to(&WORKERS, "spawn", job)
}
fn submit_record(job: Job) -> io::Result<()> {
    submit_to(&RECORDERS, "child-record", job)
}
fn submit_to(
    pool: &OnceLock<Result<mpsc::SyncSender<Job>, String>>,
    name: &str,
    job: Job,
) -> io::Result<()> {
    let workers = pool.get_or_init(|| {
        let (send, recv) = mpsc::sync_channel::<Job>(64);
        let recv = Arc::new(Mutex::new(recv));
        for n in 0..4 {
            let recv = recv.clone();
            std::thread::Builder::new()
                .name(format!("fabric-{name}-{n}"))
                .spawn(move || {
                    loop {
                        let job = recv.lock().unwrap().recv();
                        let Ok(job) = job else { break };
                        // Keep the parent thread alive even if a caller's job panics.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    }
                })
                .map_err(|e| e.to_string())?;
        }
        Ok(send)
    });
    workers
        .as_ref()
        .map_err(|error| io::Error::other(error.clone()))?
        .try_send(job)
        .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "child spawn queue is full"))
}

pub(crate) fn deadline_after(limit: std::time::Duration) -> io::Result<libc::timespec> {
    let mut now: libc::timespec = unsafe { std::mem::zeroed() };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) } != 0 {
        return Err(io::Error::last_os_error());
    }
    now.tv_sec = now
        .tv_sec
        .checked_add(limit.as_secs().try_into().map_err(io::Error::other)?)
        .ok_or_else(|| io::Error::other("spawn deadline overflow"))?;
    now.tv_nsec += limit.subsec_nanos() as libc::c_long;
    if now.tv_nsec >= 1_000_000_000 {
        now.tv_nsec -= 1_000_000_000;
        now.tv_sec += 1;
    }
    Ok(now)
}

pub(crate) fn spawn(
    mut command: Command,
    children: Option<Children>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    deadline: libc::timespec,
) -> io::Result<Child> {
    let parent_pid = unsafe { libc::getpid() };
    // Strong references pin the exact descriptors through fork: none can be
    // closed/reused underneath this snapshot while another thread shuts down.
    let private: Vec<_> = PRIVATE_FILES
        .lock()
        .unwrap()
        .iter()
        .filter_map(Weak::upgrade)
        .collect();
    unsafe {
        command.pre_exec(move || {
            for file in &private {
                let fd = file.as_raw_fd();
                if fd > 2 {
                    libc::close(fd);
                }
            }
            // PTYs already made a new session/group. Other commands get their
            // own group without detaching them from the daemon's session.
            if libc::getpgrp() != libc::getpid() && libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            #[cfg(target_os = "linux")]
            {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent_pid {
                    return Err(io::Error::from_raw_os_error(libc::ECHILD));
                }
            }
            let _ = parent_pid;
            Ok(())
        });
    }
    let mut prepared = None;
    if children.as_ref().is_some_and(Children::has_owner) {
        let (parent, child) = UnixStream::pair()?;
        parent.set_read_timeout(Some(super::child_spawn::SPAWN_LIMIT))?;
        parent.set_write_timeout(Some(super::child_spawn::SPAWN_LIMIT))?;
        child.set_read_timeout(Some(super::child_spawn::SPAWN_LIMIT))?;
        child.set_write_timeout(Some(super::child_spawn::SPAWN_LIMIT))?;
        let parent = Arc::new(parent);
        let witness = parent.clone();
        let owner = children.clone().unwrap();
        let (ready, result) = mpsc::channel();
        submit_record(Box::new(move || {
            let mut pid = [0; 4];
            let result = (|| {
                (&*witness).read_exact(&mut pid)?;
                if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
                }
                check_deadline(&deadline)?;
                let pid = u32::from_ne_bytes(pid);
                // Track the fork before storage or runtime registration can
                // fail. Completion must still wait for that exact pid to end.
                owner.register(pid);
                owner.record(pid)
            })();
            let accepted = result.is_ok() && !cancelled.load(std::sync::atomic::Ordering::Acquire);
            if ready.send(result).is_ok() {
                let _ = (&*witness).write_all(&[u8::from(accepted)]);
            }
        }))?;
        // All operations after fork are async-signal-safe. The parent commits
        // identity on its native witness thread, then releases this gate. A
        // lost parent/failed commit cannot execute the user's command.
        unsafe {
            command.pre_exec(move || {
                libc::close(parent.as_raw_fd());
                let pid = (libc::getpid() as u32).to_ne_bytes();
                transfer(child.as_raw_fd(), &pid)?;
                let mut accepted = 0u8;
                loop {
                    let n = libc::read(child.as_raw_fd(), (&mut accepted as *mut u8).cast(), 1);
                    if n == 1 {
                        break;
                    }
                    if n < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(io::Error::from_raw_os_error(libc::EIO));
                }
                if accepted != 1 {
                    return Err(io::Error::from_raw_os_error(libc::EACCES));
                }
                Ok(())
            });
        }
        prepared = Some(result);
    }
    unsafe {
        command.pre_exec(move || {
            // Parent atomics become private copies after fork. An absolute
            // monotonic deadline remains valid in both processes instead.
            check_deadline(&deadline)?;
            Ok(())
        });
    }
    command.kill_on_drop(true);
    let spawned = command.spawn();
    // The witness sends its result before releasing exec. Failure before the
    // gate must not wait for the witness's bounded read timeout.
    let record = prepared
        .and_then(|result| result.try_recv().ok())
        .transpose()?;
    let inner = spawned?;
    let pid = inner
        .id()
        .ok_or_else(|| io::Error::other("spawned child has no pid"))?;
    if let Some(children) = children {
        children.register(pid);
    }
    Ok(Child {
        inner,
        pid,
        _record: record.flatten(),
        killed_group: false,
    })
}

// Only a kernel clock read, comparisons and an OS error value; safe after fork.
fn check_deadline(deadline: &libc::timespec) -> io::Result<()> {
    let mut now: libc::timespec = unsafe { std::mem::zeroed() };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if (now.tv_sec, now.tv_nsec) >= (deadline.tv_sec, deadline.tv_nsec) {
        return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
    }
    Ok(())
}

unsafe fn transfer(fd: libc::c_int, bytes: &[u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < bytes.len() {
        let n = unsafe { libc::write(fd, bytes[offset..].as_ptr().cast(), bytes.len() - offset) };
        if n > 0 {
            offset += n as usize;
        } else if n < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        } else {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Scratch(PathBuf);
    impl Scratch {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "fabric-child-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn command(&self) -> Command {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", "printf ran > \"$1\"", "gated-child"]);
            command.arg(self.0.join("ran"));
            command
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[derive(Debug)]
    struct EmptyRecord;
    impl Record for EmptyRecord {}
    #[derive(Debug)]
    struct Gate {
        started: Mutex<Option<tokio::sync::oneshot::Sender<u32>>>,
        permit: Mutex<mpsc::Receiver<bool>>,
    }
    impl crate::child_tracking::Owner for Gate {
        fn before_exec(&self, pid: u32, _: &str) -> io::Result<Box<dyn Record>> {
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(pid);
            }
            if self.permit.lock().unwrap().recv().unwrap_or(false) {
                Ok(Box::new(EmptyRecord))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "ownership commit refused",
                ))
            }
        }
    }
    fn gate() -> (
        Children,
        tokio::sync::oneshot::Receiver<u32>,
        mpsc::Sender<bool>,
    ) {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (permit, wait) = mpsc::channel();
        let owner = Arc::new(Gate {
            started: Mutex::new(Some(started)),
            permit: Mutex::new(wait),
        });
        (
            Children::default().with_owner(owner, "gated-session".into()),
            ready,
            permit,
        )
    }
    #[tokio::test]
    async fn a_command_cannot_execute_before_its_parent_commits_ownership() {
        let scratch = Scratch::new();
        let command = scratch.command();
        let (children, ready, permit) = gate();
        let pending = tokio::spawn(async move {
            children
                .scope(crate::child_spawn::spawn("commit gate", command))
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .unwrap()
            .unwrap();
        assert!(!scratch.0.join("ran").exists());
        permit.send(true).unwrap();
        let mut child = pending.await.unwrap().unwrap();
        assert!(child.wait().await.unwrap().success());
        assert!(scratch.0.join("ran").exists());
    }
    #[tokio::test]
    async fn a_failed_ownership_commit_never_executes_the_command() {
        let scratch = Scratch::new();
        let (children, _, permit) = gate();
        permit.send(false).unwrap();
        let error = children
            .scope(crate::child_spawn::spawn(
                "refused commit",
                scratch.command(),
            ))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(!scratch.0.join("ran").exists());
    }
    #[tokio::test]
    async fn cancelling_a_pending_commit_cannot_execute_the_command() {
        let scratch = Scratch::new();
        let command = scratch.command();
        let (children, ready, permit) = gate();
        let tracker = children.clone();
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let token = cancelled.clone();
        let runtime = tokio::runtime::Handle::current();
        let (send, result) = tokio::sync::oneshot::channel();
        let deadline = deadline_after(Duration::from_secs(20)).unwrap();
        submit(Box::new(move || {
            let _runtime = runtime.enter();
            let _ = send.send(spawn(command, Some(children), token, deadline));
        }))
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !tracker.complete(),
            "an unreaped fork was omitted from completion tracking"
        );
        // Force the caller's cancellation after fork, while storage still
        // holds the gate. Parent atomics in the child would be private copies.
        cancelled.store(true, Ordering::Release);
        permit.send(true).unwrap();
        let error = result.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            tracker.complete(),
            "the refused pre-exec child was not reaped"
        );
        assert!(!scratch.0.join("ran").exists());
    }
    #[derive(Debug)]
    struct CountingOwner(Arc<std::sync::atomic::AtomicUsize>);
    impl crate::child_tracking::Owner for CountingOwner {
        fn before_exec(&self, _: u32, _: &str) -> io::Result<Box<dyn Record>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(EmptyRecord))
        }
    }
    #[tokio::test]
    async fn an_expired_child_deadline_never_executes_user_code() {
        let scratch = Scratch::new();
        let command = scratch.command();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let children = Children::default().with_owner(
            Arc::new(CountingOwner(calls.clone())),
            "expired-session".into(),
        );
        let runtime = tokio::runtime::Handle::current();
        let (send, result) = tokio::sync::oneshot::channel();
        let deadline = deadline_after(Duration::ZERO).unwrap();
        submit(Box::new(move || {
            let _runtime = runtime.enter();
            let _ = send.send(spawn(
                command,
                Some(children),
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
                deadline,
            ));
        }))
        .unwrap();
        let error = result.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(!scratch.0.join("ran").exists());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "an expired queued request inspected a potentially reused pid"
        );
    }
    fn running(pid: i32) -> bool {
        #[cfg(target_os = "linux")]
        {
            std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .ok()
                .is_some_and(|s| !s.rsplit_once(')').unwrap().1.trim_start().starts_with('Z'))
        }
        #[cfg(target_os = "macos")]
        {
            let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
            let size = std::mem::size_of_val(&info) as i32;
            unsafe {
                libc::proc_pidinfo(
                    pid,
                    libc::PROC_PIDTBSDINFO,
                    0,
                    (&mut info as *mut libc::proc_bsdinfo).cast(),
                    size,
                ) == size
                    && info.pbi_status != 5
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            (unsafe { libc::kill(pid, 0) }) == 0
        }
    }
    struct KillPid(i32);
    impl Drop for KillPid {
        fn drop(&mut self) {
            unsafe {
                libc::kill(self.0, libc::SIGKILL);
            }
        }
    }
    #[tokio::test]
    async fn normal_completion_preserves_existing_background_command_behavior() {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", r#"sleep 60 & printf '%s\n' "$!"; exit 0"#])
            .stdout(std::process::Stdio::piped());
        let mut child = crate::child_spawn::spawn("normal background exit", command)
            .await
            .unwrap();
        let mut line = String::new();
        tokio::time::timeout(
            Duration::from_secs(2),
            BufReader::new(child.stdout.take().unwrap()).read_line(&mut line),
        )
        .await
        .unwrap()
        .unwrap();
        let pid: i32 = line.trim().parse().unwrap();
        let _cleanup = KillPid(pid);
        assert!(child.wait().await.unwrap().success());
        drop(child);
        assert!(
            running(pid),
            "normal command completion changed background behavior"
        );
    }
    #[tokio::test]
    async fn dropping_a_managed_child_ends_its_process_group() {
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "sleep 60 & printf '%s\\n' \"$!\"; wait"])
            .stdout(std::process::Stdio::piped());
        let mut child = crate::child_spawn::spawn("owned group", command)
            .await
            .unwrap();
        let mut line = String::new();
        tokio::time::timeout(
            Duration::from_secs(2),
            BufReader::new(child.stdout.take().unwrap()).read_line(&mut line),
        )
        .await
        .unwrap()
        .unwrap();
        let pid: i32 = line.trim().parse().unwrap();
        drop(child);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let alive = running(pid);
                if !alive {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("owned descendant survived group cleanup");
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn retiring_a_runtime_blocking_worker_does_not_kill_a_healthy_child() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_keep_alive(Duration::from_millis(10))
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::task::spawn_blocking(|| ()).await.unwrap();
            let mut command = Command::new("sleep");
            command.arg("60");
            let mut child = crate::child_spawn::spawn("parent lifetime", command)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                child.try_wait().unwrap().is_none(),
                "retiring a temporary worker killed the command"
            );
            child.kill().await.unwrap();
        });
    }
}

#[cfg(test)]
mod lease_tests {
    use super::*;
    use std::{
        fs::{File, OpenOptions},
        path::PathBuf,
        process::{Child as StdChild, Stdio},
        time::{Duration, Instant},
    };
    #[derive(Debug)]
    struct PendingOwner(PathBuf);
    impl crate::child_tracking::Owner for PendingOwner {
        fn before_exec(&self, pid: u32, _: &str) -> io::Result<Box<dyn Record>> {
            std::fs::write(self.0.join("pid"), pid.to_string())?;
            loop {
                std::thread::park();
            }
        }
    }
    #[test]
    fn ownership_gate_helper() {
        let Some(dir) = std::env::var_os("FABRIC_CHILD_GATE_HELPER_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        let lease = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(dir.join("lease"))
                .unwrap(),
        );
        assert_eq!(
            unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );
        crate::child_spawn::protect_from_children(lease.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let children = Children::default()
                .with_owner(Arc::new(PendingOwner(dir)), "pending-commit".into());
            let mut command = Command::new("sleep");
            command.arg("60");
            let _ = children
                .scope(crate::child_spawn::spawn("held child gate", command))
                .await;
        });
    }
    struct Cleanup {
        parent: StdChild,
        child: Option<i32>,
        dir: PathBuf,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.parent.kill();
            let _ = self.parent.wait();
            if let Some(pid) = self.child {
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    #[tokio::test]
    async fn a_child_waiting_for_commit_cannot_pin_its_dead_daemons_lease() {
        let dir = std::env::temp_dir().join(format!("fabric-lease-gate-{}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let lease = File::create(dir.join("lease")).unwrap();
        let parent = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "child_ownership::lease_tests::ownership_gate_helper",
                "--nocapture",
            ])
            .env("FABRIC_CHILD_GATE_HELPER_DIR", &dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut cleanup = Cleanup {
            parent,
            child: None,
            dir: dir.clone(),
        };
        let deadline = Instant::now() + Duration::from_secs(3);
        let pid = loop {
            if let Ok(text) = std::fs::read_to_string(dir.join("pid")) {
                break text.parse::<i32>().unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "child never reached the ownership gate"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        cleanup.child = Some(pid);
        #[cfg(target_os = "linux")]
        {
            let expected = dir.join("lease").canonicalize().unwrap();
            let inherited = std::fs::read_dir(format!("/proc/{pid}/fd"))
                .unwrap()
                .take(64)
                .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
                .any(|path| path == expected);
            assert!(
                !inherited,
                "the pre-exec child still holds the daemon lease descriptor"
            );
        }
        // Keep the child at the gate while its parent dies. On macOS there is
        // no PDEATHSIG; only closing the inherited lease makes it available.
        unsafe {
            libc::kill(pid, libc::SIGSTOP);
        }
        cleanup.parent.kill().unwrap();
        cleanup.parent.wait().unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "a pre-exec child retained its dead daemon's lease"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
