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
pub struct Children(Arc<Tracked>);
#[derive(Debug, Default)]
struct Tracked {
    spawning: AtomicUsize,
    pids: Mutex<BTreeSet<u32>>,
}
tokio::task_local! { static OWNER: Children; }
impl Children {
    pub fn current() -> Option<Self> {
        OWNER.try_with(Clone::clone).ok()
    }
    pub async fn scope<F: Future>(&self, work: F) -> F::Output {
        OWNER.scope(self.clone(), work).await
    }
    pub fn register(&self, pid: u32) {
        self.0.pids.lock().unwrap().insert(pid);
    }
    pub fn begin_spawn(&self) -> Spawn {
        self.0.spawning.fetch_add(1, Ordering::SeqCst);
        Spawn(self.clone())
    }
    /// PID reuse can conservatively delay completion; it cannot claim a live
    /// owned child has ended. Only ESRCH counts as proof that a PID is gone.
    pub fn complete(&self) -> bool {
        let mut pids = self.0.pids.lock().unwrap();
        pids.retain(|pid| {
            let result = unsafe { libc::kill(*pid as libc::pid_t, 0) };
            result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
        });
        pids.is_empty() && self.0.spawning.load(Ordering::SeqCst) == 0
    }
}
pub struct Spawn(Children);
impl Drop for Spawn {
    fn drop(&mut self) {
        self.0.0.spawning.fetch_sub(1, Ordering::SeqCst);
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
