//! Process-neutral paths and exclusive ownership for sync state.

use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

/// The result of a non-mutating sync-owner lease inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOwnerLeaseState {
    /// The state root or lock file does not exist.
    Absent,
    /// The lock file exists and no process owns it.
    Available,
    /// A process owns the lock file.
    Held,
}

/// The paths a sync engine needs from its host process.
///
/// The engine receives these paths directly. It does not need a daemon home,
/// identity file, peer book, control socket, or any other daemon state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPaths {
    config_path: PathBuf,
    state_root: PathBuf,
}

impl SyncPaths {
    pub fn new(config: impl Into<PathBuf>, state: impl Into<PathBuf>) -> Self {
        Self {
            config_path: config.into(),
            state_root: state.into(),
        }
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub fn state_root(&self) -> &Path {
        &self.state_root
    }

    pub fn owner_lease_path(&self) -> PathBuf {
        self.state_root.join("owner.lock")
    }
}

#[cfg(test)]
impl From<crate::config::FabricHome> for SyncPaths {
    fn from(home: crate::config::FabricHome) -> Self {
        Self::new(home.syncs_path(), home.root().join("sync"))
    }
}

/// The exclusive owner of one sync state root.
///
/// Keep this value for the complete engine lifetime. Dropping it releases the
/// operating-system lease. The lock file can remain after a clean stop.
#[derive(Debug)]
pub struct SyncOwnerLease {
    _file: File,
}

impl SyncOwnerLease {
    /// Inspect the lease without creating or changing any path.
    pub fn probe(paths: &SyncPaths) -> Result<SyncOwnerLeaseState> {
        let root = paths.state_root();
        if !root.exists() {
            return Ok(SyncOwnerLeaseState::Absent);
        }
        if !root.is_dir() {
            bail!("sync state root is not a directory: {}", root.display());
        }
        let path = paths.owner_lease_path();
        if !path.exists() {
            return Ok(SyncOwnerLeaseState::Absent);
        }
        if !path.is_file() {
            bail!("sync state owner lease is not a file: {}", path.display());
        }
        let file = OpenOptions::new().read(true).open(&path).with_context(|| {
            format!(
                "failed to inspect sync state owner lease {}",
                path.display()
            )
        })?;

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if rc == 0 {
                let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
                return Ok(SyncOwnerLeaseState::Available);
            }
            let error = std::io::Error::last_os_error();
            let code = error.raw_os_error();
            if code == Some(libc::EWOULDBLOCK) || code == Some(libc::EAGAIN) {
                return Ok(SyncOwnerLeaseState::Held);
            }
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect sync state owner lease {}",
                    path.display()
                )
            });
        }

        #[cfg(not(unix))]
        bail!("fabric sync state owner leases are not supported on this platform");
    }

    pub fn acquire(paths: &SyncPaths) -> Result<Self> {
        std::fs::create_dir_all(paths.state_root()).with_context(|| {
            format!(
                "failed to create sync state root {}",
                paths.state_root().display()
            )
        })?;
        let path = paths.owner_lease_path();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open sync state owner lease {}", path.display()))?;

        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            // A child can briefly inherit this descriptor between fork and
            // exec. CLOEXEC closes it at exec, so retry only that short window.
            const RETRY_FOR: Duration = Duration::from_millis(200);
            let deadline = Instant::now() + RETRY_FOR;
            loop {
                let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
                if rc == 0 {
                    break;
                }
                let error = std::io::Error::last_os_error();
                let code = error.raw_os_error();
                if code != Some(libc::EWOULDBLOCK) && code != Some(libc::EAGAIN) {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to acquire sync state owner lease {}",
                            path.display()
                        )
                    });
                }
                if Instant::now() >= deadline {
                    bail!(
                        "fabric sync state owner lease is already held at {}",
                        path.display()
                    );
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        #[cfg(not(unix))]
        bail!("fabric sync state owner leases are not supported on this platform");

        #[allow(unreachable_code)]
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probing_a_lease_never_creates_it() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SyncPaths::new(dir.path().join("syncs.toml"), dir.path().join("state"));
        assert_eq!(
            SyncOwnerLease::probe(&paths).unwrap(),
            SyncOwnerLeaseState::Absent
        );
        assert!(!paths.state_root().exists());
    }

    #[test]
    fn probing_distinguishes_an_available_lease_from_a_held_lease() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SyncPaths::new(dir.path().join("syncs.toml"), dir.path().join("state"));
        std::fs::create_dir_all(paths.state_root()).unwrap();
        std::fs::write(paths.owner_lease_path(), b"").unwrap();
        assert_eq!(
            SyncOwnerLease::probe(&paths).unwrap(),
            SyncOwnerLeaseState::Available
        );
        let lease = SyncOwnerLease::acquire(&paths).unwrap();
        assert_eq!(
            SyncOwnerLease::probe(&paths).unwrap(),
            SyncOwnerLeaseState::Held
        );
        drop(lease);
    }
}
