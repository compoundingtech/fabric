//! Completion evidence for children owned by a policy admission.
//!
//! A dropped service future can signal kill-on-drop before the kernel has
//! actually ended its child. Revocation receipts wait for that separate event.
use std::{
    collections::BTreeSet,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug, Clone, Default)]
pub struct Children {
    tracked: Arc<Tracked>,
    owner: Option<Arc<dyn Owner>>,
    session: String,
}

/// Optional daemon-owned storage boundary. Called on a native thread while the
/// child is gated before exec. An error prevents the command from running.
pub trait Owner: std::fmt::Debug + Send + Sync {
    fn before_exec(&self, pid: u32, session: &str) -> std::io::Result<Box<dyn Record>>;
}

/// Keeps a durable ownership record until the managed child is released.
pub trait Record: std::fmt::Debug + Send {}
#[derive(Debug, Default)]
struct Tracked {
    spawning: AtomicUsize,
    pids: Mutex<BTreeSet<u32>>,
}
tokio::task_local! { static OWNER: Children; }
impl Children {
    pub fn with_owner(mut self, owner: Arc<dyn Owner>, session: String) -> Self {
        self.owner = Some(owner);
        self.session = session;
        self
    }
    pub fn with_session(&self, session: impl ToString) -> Self {
        Self {
            session: session.to_string(),
            ..self.clone()
        }
    }
    pub(crate) fn has_owner(&self) -> bool {
        self.owner.is_some()
    }
    pub(crate) fn record(&self, pid: u32) -> std::io::Result<Option<Box<dyn Record>>> {
        self.owner
            .as_ref()
            .map(|owner| owner.before_exec(pid, &self.session))
            .transpose()
    }
    pub fn current() -> Option<Self> {
        OWNER.try_with(Clone::clone).ok()
    }
    pub async fn scope<F: Future>(&self, work: F) -> F::Output {
        OWNER.scope(self.clone(), work).await
    }
    pub fn register(&self, pid: u32) {
        self.tracked.pids.lock().unwrap().insert(pid);
    }
    pub fn begin_spawn(&self) -> Spawn {
        self.tracked.spawning.fetch_add(1, Ordering::SeqCst);
        Spawn(self.clone())
    }
    /// PID reuse can conservatively delay completion; it cannot claim a live
    /// owned child has ended. Only ESRCH counts as proof that a PID is gone.
    pub fn complete(&self) -> bool {
        let mut pids = self.tracked.pids.lock().unwrap();
        pids.retain(|pid| {
            let result = unsafe { libc::kill(*pid as libc::pid_t, 0) };
            result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
        });
        pids.is_empty() && self.tracked.spawning.load(Ordering::SeqCst) == 0
    }
}
pub struct Spawn(Children);
impl Drop for Spawn {
    fn drop(&mut self) {
        self.0.tracked.spawning.fetch_sub(1, Ordering::SeqCst);
    }
}
/// Register a child created outside the asynchronous spawn helper (a PTY).
pub fn register(pid: u32) {
    if let Some(owner) = Children::current() {
        owner.register(pid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn dropping_work_is_not_completion_until_its_child_has_gone() {
        let children = Children::default();
        let child = children
            .scope(async {
                let mut command = tokio::process::Command::new("sh");
                command.args(["-c", "trap '' TERM; exec sleep 600"]);
                crate::child_spawn::spawn("completion test child", command)
                    .await
                    .unwrap()
            })
            .await;
        assert!(!children.complete());
        drop(child);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !children.complete() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("owned child was not reaped");
    }
}
