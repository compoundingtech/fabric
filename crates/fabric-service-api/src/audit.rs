//! Optional process observation installed by the base network for one call.
//!
//! Services report metadata through this interface without knowing the daemon,
//! its policy, its files or its writer. An observer must return promptly and
//! must not wait for storage. No observer means that reporting is a no-op.
use std::{
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Debug, Clone, Copy)]
pub enum Outcome {
    Ok,
    SpawnFailed,
    Cancelled,
}

#[derive(Debug, Default)]
pub struct Counts {
    input: AtomicU64,
    output: AtomicU64,
}
impl Counts {
    pub fn add_in(&self, bytes: usize) {
        self.input.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn add_out(&self, bytes: usize) {
        self.output.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn input(&self) -> u64 {
        self.input.load(Ordering::Acquire)
    }
    pub fn output(&self) -> u64 {
        self.output.load(Ordering::Acquire)
    }
}

/// Callbacks are nonblocking metadata admission, including on guard drop.
/// `code` is an existing stable error code without the `fabric:` prefix.
/// Implementations deduplicate terminal events for the call.
pub trait Observer: std::fmt::Debug + Send + Sync {
    fn begin(&self);
    fn end(&self);
    fn start(&self, argv: Option<&[String]>) -> bool;
    fn counts(&self) -> Arc<Counts>;
    fn observe_exit(&self, status: i32);
    fn finish(&self, outcome: Outcome, status: Option<i32>, code: Option<&str>);
}

tokio::task_local! { static CURRENT: Option<Arc<dyn Observer>>; }

pub async fn scope<F: std::future::Future>(
    observer: Option<Arc<dyn Observer>>,
    future: F,
) -> F::Output {
    CURRENT.scope(observer, future).await
}

#[derive(Debug)]
pub struct CallGuard {
    observer: Arc<dyn Observer>,
    finished: bool,
}
impl CallGuard {
    fn new(observer: Arc<dyn Observer>) -> Self {
        observer.begin();
        Self {
            observer,
            finished: false,
        }
    }
    pub fn start_exec(&self, argv: &[String]) {
        self.observer.start(Some(argv));
    }
    pub fn counts(&self) -> Arc<Counts> {
        self.observer.counts()
    }
    pub fn observe_exit(&self, status: i32) {
        self.observer.observe_exit(status);
    }
    pub fn finish(mut self, outcome: Outcome, status: Option<i32>, code: Option<&str>) {
        self.observer.finish(outcome, status, code);
        self.finished = true;
    }
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.observer.finish(Outcome::Cancelled, None, None);
        }
        self.observer.end();
    }
}

pub fn exec_attempt() -> Option<CallGuard> {
    CURRENT
        .try_with(Clone::clone)
        .ok()
        .flatten()
        .map(CallGuard::new)
}
pub fn shell_start() -> Option<CallGuard> {
    let observer = CURRENT.try_with(Clone::clone).ok().flatten()?;
    observer.start(None).then(|| CallGuard::new(observer))
}

pub struct CountWriter<W> {
    pub inner: W,
    pub counts: Option<Arc<Counts>>,
}
impl<W: Write> Write for CountWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        if let Some(counts) = &self.counts {
            counts.add_in(written);
        }
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub fn exit_status(status: std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status
            .code()
            .unwrap_or_else(|| 128 + status.signal().unwrap_or(0))
    }
    #[cfg(not(unix))]
    {
        status.code().unwrap_or(1)
    }
}
