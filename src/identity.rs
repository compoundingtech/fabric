//! Local identity rotation. Only identity.toml is authoritative for the active key.
use std::{fs::{self, File, OpenOptions}, io::Write, os::{fd::AsRawFd, unix::fs::OpenOptionsExt}, path::{Path, PathBuf}, time::{SystemTime, UNIX_EPOCH}};
use anyhow::{Context, Result};
use iroh::EndpointId;
use serde::Serialize;
use crate::{config::{FabricHome, generate_identity_file, load_or_create_identity}, error::{ErrorCode, FabricError}};

const RETENTION_SECONDS: u64 = 600;

/// An advisory lock shared by every identity operation, including status.
pub struct IdentityLock(File);
impl IdentityLock {
    pub fn acquire(home: &FabricHome) -> Result<Self> {
        home.prepare()?;
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).mode(0o600).open(home.root().join("identity.lock"))?;
        // SAFETY: flock borrows a live descriptor and does not retain pointers.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Err(FabricError::new(ErrorCode::IdentityBusy, "another identity operation is in progress").into());
            }
            return Err(error.into());
        }
        Ok(Self(file))
    }
}
impl Drop for IdentityLock {
    fn drop(&mut self) {
        // SAFETY: the descriptor remains live until after this call.
        unsafe { libc::flock(self.0.as_raw_fd(), libc::LOCK_UN); }
    }
}

#[derive(Debug, Serialize)]
pub struct Status {
    pub current: EndpointId,
    pub next: Option<EndpointId>,
    pub state: &'static str,
}
#[derive(Debug, Serialize)]
pub struct Prepared {
    pub current: EndpointId,
    pub next: EndpointId,
}
#[derive(Debug, Serialize)]
pub struct Activated {
    pub previous: EndpointId,
    pub active: EndpointId,
    pub outcome: &'static str,
}
fn pending(home: &FabricHome) -> PathBuf { home.root().join("identity.next.toml") }
fn sync_home(home: &FabricHome) -> Result<()> { File::open(home.root())?.sync_all().context("sync identity directory") }
fn read_id(path: &Path) -> Result<EndpointId> {
    #[derive(serde::Deserialize)]
    struct Key { secret_key: iroh::SecretKey }
    let key: Key = toml::from_str(&fs::read_to_string(path)?)
        .map_err(|_| anyhow::anyhow!("invalid identity key file {}", path.display()))?;
    Ok(key.secret_key.public())
}
fn snapshot(home: &FabricHome) -> Result<Status> {
    let current = read_id(&home.identity_path())?;
    let next = match read_id(&pending(home)) {
        Ok(id) => Some(id),
        Err(error) if error.downcast_ref::<std::io::Error>().is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) => None,
        Err(error) => {
            tracing::warn!(%error, "ignoring unreadable pending identity; prepare replaces it");
            None
        }
    };
    Ok(Status { current, next, state: if next.is_some() { "prepared" } else { "active" } })
}
pub fn status(home: &FabricHome) -> Result<Status> {
    let _lock = IdentityLock::acquire(home)?;
    load_or_create_identity(home)?;
    if let Err(error) = cleanup_retired_locked(home, now()) {
        tracing::warn!(%error, "retired identity cleanup failed");
    }
    snapshot(home)
}
pub fn prepare(home: &FabricHome) -> Result<Prepared> {
    let _lock = IdentityLock::acquire(home)?;
    if let Err(error) = cleanup_retired_locked(home, now()) {
        tracing::warn!(%error, "retired identity cleanup failed");
    }
    load_or_create_identity(home)?;
    File::open(home.identity_path())?.sync_all()?;
    sync_home(home)?;
    let state = snapshot(home)?;
    if let Some(next) = state.next { return Ok(Prepared { current: state.current, next }); }
    let temporary = scratch_path(home, "identity.next");
    let next = generate_identity_file(&temporary)?;
    File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, pending(home))?;
    sync_home(home)?;
    Ok(Prepared { current: state.current, next })
}

/// The lock remains held across the caller's restart barrier. A retry after the
/// rename never writes a key: it only asks the caller to reconcile the daemon.
pub struct Activation {
    pub result: Activated,
    _lock: IdentityLock,
}
struct Swap {
    result: Activated,
    durability_error: Option<anyhow::Error>,
}
fn check_guards(home: &FabricHome, current: EndpointId, next: EndpointId) -> Result<bool> {
    let active = read_id(&home.identity_path())?;
    if active == next { return Ok(true); }
    if active != current || current == next || read_id(&pending(home)).ok() != Some(next) {
        return Err(FabricError::new(ErrorCode::IdentityMismatch, "identity does not match --expect-current and --expect-next; nothing changed").into());
    }
    Ok(false)
}
pub fn activate(home: &FabricHome, expected_current: EndpointId, expected_next: EndpointId) -> Result<Activation> {
    let lock = IdentityLock::acquire(home)?;
    let already = check_guards(home, expected_current, expected_next)?;
    let swap = activate_locked(home, expected_current, expected_next, already, &lock)?;
    Ok(Activation { result: swap.result, _lock: lock })
}
fn activate_locked(home: &FabricHome, expected_current: EndpointId, expected_next: EndpointId, already: bool, _lock: &IdentityLock) -> Result<Swap> {
    if already {
        return Ok(Swap { result: Activated { previous: expected_current, active: expected_next, outcome: "already_active" }, durability_error: sync_home(home).err() });
    }
    // Retain the old secret before the only authoritative swap. The retired
    // copy is never read as a serving key, even after a failed restart.
    let retired = home.root().join(format!("identity.retired.{}.{}.toml", now().saturating_add(RETENTION_SECONDS), expected_current));
    match OpenOptions::new().write(true).create_new(true).mode(0o600).open(&retired) {
        Ok(mut file) => {
            file.write_all(&fs::read(home.identity_path())?)?;
            file.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            anyhow::ensure!(read_id(&retired)? == expected_current, "retired identity does not match current key");
        }
        Err(error) => return Err(error.into()),
    }
    // This is intent, not a second identity authority: startup acts on it only
    // when identity.toml already contains expected_next.
    write_private_atomic(home, "identity.activation", expected_next.to_string().as_bytes())?;
    if check_guards(home, expected_current, expected_next)? {
        return Err(FabricError::new(ErrorCode::IdentityMismatch, "active identity changed during preparation; activation aborted before rename").into());
    }
    fs::rename(pending(home), home.identity_path())?;
    // Once rename succeeded, the caller must retire the old daemon even when
    // the directory durability barrier fails.
    let durability_error = sync_home(home).err();
    Ok(Swap { result: Activated { previous: expected_current, active: expected_next, outcome: "activated" }, durability_error })
}
fn now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() }
fn cleanup_retired_locked(home: &FabricHome, timestamp: u64) -> Result<Option<u64>> {
    let mut changed = false;
    let mut next_deadline: Option<u64> = None;
    for entry in fs::read_dir(home.root())? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => { tracing::warn!(%error, "cannot inspect retired identity entry"); continue; }
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(rest) = name.strip_prefix("identity.retired.") else { continue };
        let Some((expiry, suffix)) = rest.split_once('.') else { continue };
        let Some(id) = suffix.strip_suffix(".toml") else { continue };
        if id.parse::<EndpointId>().is_err() { continue }
        let Ok(deadline) = expiry.parse::<u64>() else { continue };
        if timestamp >= deadline {
            match fs::remove_file(entry.path()) {
                Ok(()) => changed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(%error, path = %entry.path().display(), "cannot remove retired identity"),
            }
        } else {
            next_deadline = Some(next_deadline.map_or(deadline, |next| next.min(deadline)));
        }
    }
    if changed { sync_home(home)?; }
    Ok(next_deadline)
}
pub fn cleanup_retired(home: &FabricHome) -> Result<()> {
    let _lock = IdentityLock::acquire(home)?;
    cleanup_retired_locked(home, now()).map(|_| ())
}

/// Run only during exclusive daemon startup, before restoring configuration.
/// A crash before the rename leaves the old key and does not retire its dials.
pub fn reconcile_startup(home: &FabricHome) -> Result<()> {
    let path = home.root().join("identity.activation");
    let expected = match fs::read_to_string(&path) {
        Ok(value) => value.parse::<EndpointId>()?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if read_id(&home.identity_path())? != expected { return Ok(()); }
    let mut config = crate::config::FabricConfig::load(home)?;
    let dials: Vec<_> = config.dials().iter().map(|dial| (dial.peer.clone(), dial.protocol.clone())).collect();
    for (peer, protocol) in dials { config.remove_dial(&peer, &protocol); }
    config.save(home)?;
    File::open(home.config_path())?.sync_all()?;
    sync_home(home)?;
    fs::remove_file(path)?;
    sync_home(home)
}

pub async fn activate_with_restart<P, PFut, G, F, Fut, R, RFut>(home: &FabricHome, current: EndpointId, next: EndpointId, preserve: P, restart: F, recover: R) -> Result<Activated>
where P: FnOnce() -> PFut, PFut: Future<Output = Result<G>>, F: FnOnce() -> Fut, Fut: Future<Output = Result<()>>,
      R: FnOnce(anyhow::Error) -> RFut, RFut: Future<Output = anyhow::Error> {
    let lock = IdentityLock::acquire(home)?;
    let already = check_guards(home, current, next)?;
    // A preserved daemon lease excludes concurrent startup through the rename.
    let startup_guard = if !already {
        match preserve().await {
            Ok(guard) => Some(guard),
            Err(error) => return Err(recover(error).await),
        }
    } else { None };
    let activation = activate_locked(home, current, next, already, &lock);
    drop(startup_guard);
    let activation = match activation {
        Ok(activation) => activation,
        Err(error) => return Err(recover(error).await),
    };
    restart().await.map_err(|error| FabricError::new(
        ErrorCode::RestartFailed,
        format!("identity changed; retry the same guarded activation to restart: {error:#}"),
    ).identity(current.to_string(), next.to_string()))?;
    if let Some(error) = activation.durability_error {
        return Err(FabricError::new(ErrorCode::RestartFailed, format!("identity renamed but directory durability could not be confirmed: {error:#}; retry the same activation"))
            .identity(current.to_string(), next.to_string()).into());
    }
    Ok(activation.result)
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RuntimeSnapshot {
    pub next: EndpointId,
    pub exposures: Vec<crate::config::PersistedExpose>,
}
pub fn write_runtime_snapshot(home: &FabricHome, next: EndpointId, exposures: Vec<crate::config::PersistedExpose>) -> Result<()> {
    let bytes = serde_json::to_vec(&RuntimeSnapshot { next, exposures })?;
    write_private_atomic(home, "identity.exposures", &bytes)
}
pub fn discard_runtime_snapshot(home: &FabricHome) -> Result<()> {
    match fs::remove_file(home.root().join("identity.exposures")) {
        Ok(()) => sync_home(home),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn scratch_path(home: &FabricHome, name: &str) -> PathBuf {
    home.root().join(format!(".{name}.{}.tmp", iroh::SecretKey::generate().public()))
}

fn write_private_atomic(home: &FabricHome, name: &str, bytes: &[u8]) -> Result<()> {
    let path = home.root().join(name);
    let temporary = scratch_path(home, name);
    let result = (|| {
        let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_home(home)
    })();
    if result.is_err() { let _ = fs::remove_file(&temporary); }
    result
}
pub fn inherited_exposures(home: &FabricHome) -> Result<Vec<crate::config::PersistedExpose>> {
    let path = home.root().join("identity.exposures");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let snapshot: RuntimeSnapshot = serde_json::from_slice(&bytes)?;
    if snapshot.next == read_id(&home.identity_path())? { Ok(snapshot.exposures) } else { Ok(Vec::new()) }
}
pub fn finish_startup_snapshot(home: &FabricHome) -> Result<()> {
    let bytes = match fs::read(home.root().join("identity.exposures")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let snapshot: RuntimeSnapshot = serde_json::from_slice(&bytes)?;
    if snapshot.next != read_id(&home.identity_path())? { return Ok(()); }
    match fs::remove_file(home.root().join("identity.exposures")) {
        Ok(()) => sync_home(home),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Persist the actual launch mode, not the mere presence of an old policy file.
pub fn record_runtime_mode(home: &FabricHome, enabled: bool) -> Result<()> {
    let path = home.root().join("identity.runtime-peers");
    if enabled {
        let file = OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
        file.sync_all()?;
    } else {
        match fs::remove_file(path) {
            Ok(()) => {},
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
            Err(error) => return Err(error.into()),
        }
    }
    sync_home(home)
}

pub async fn retired_key_cleanup(home: FabricHome, cancel: tokio_util::sync::CancellationToken) {
    loop {
        if cancel.is_cancelled() { return; }
        let root = home.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _lock = IdentityLock::acquire(&root)?;
            cleanup_retired_locked(&root, now())
        }).await;
        let delay = match result {
            // Every activation restarts the daemon, so a daemon with no retired
            // keys needs no polling task.
            Ok(Ok(None)) => return,
            Ok(Ok(Some(deadline))) => std::time::Duration::from_secs(deadline.saturating_sub(now()).max(1)),
            Ok(Err(error)) if crate::error::code_of(&error) == ErrorCode::IdentityBusy => std::time::Duration::from_millis(100),
            Ok(Err(error)) => {
                tracing::warn!(%error, "retired identity cleanup failed");
                std::time::Duration::from_secs(1)
            },
            Err(error) => {
                tracing::warn!(%error, "retired identity cleanup worker failed");
                std::time::Duration::from_secs(1)
            },
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(delay) => {},
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn pre_rename_recovery_retains_identity_lock_on_stop_and_write_failures() -> Result<()> {
        for fail_stop in [false, true] {
            let dir = tempfile::tempdir()?;
            let home = FabricHome::new(dir.path());
            let prepared = prepare(&home)?;
            fs::create_dir(home.root().join("identity.activation"))?;
            let recovered = std::cell::Cell::new(false);
            let result = activate_with_restart(&home, prepared.current, prepared.next,
                || async {
                    if fail_stop { anyhow::bail!("stop failed"); }
                    Ok(())
                },
                || async { panic!("pre-rename failure must not restart next") },
                |error| async {
                    let busy = IdentityLock::acquire(&home).err().context("recovery released identity lock").unwrap();
                    assert_eq!(crate::error::code_of(&busy), ErrorCode::IdentityBusy);
                    assert_eq!(read_id(&home.identity_path()).unwrap(), prepared.current);
                    recovered.set(true);
                    error
                },
            ).await;
            assert!(result.is_err());
            assert!(recovered.get());
        }
        Ok(())
    }
    use super::*;
    #[test]
    fn prepare_is_durable_private_and_idempotent() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let first = prepare(&home)?;
        let second = prepare(&home)?;
        assert_eq!(first.next, second.next);
        assert_eq!(first.current, second.current);
        assert_eq!(status(&home)?.state, "prepared");
        assert_eq!(fs::metadata(pending(&home))?.permissions().mode() & 0o777, 0o600);
        assert_eq!(load_or_create_identity(&home)?.public(), first.current);
        Ok(())
    }
    #[test]
    fn activation_swap_crash_recovery_and_lost_reply_do_not_reswap() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        let activation = activate(&home, prepared.current, prepared.next)?;
        assert_eq!(activation.result.outcome, "activated");
        drop(activation); // crash before restart
        assert_eq!(status(&home)?.current, prepared.next);
        assert_eq!(status(&home)?.state, "active");
        let before = fs::read(home.identity_path())?;
        let retry = activate(&home, prepared.current, prepared.next)?;
        assert_eq!(retry.result.outcome, "already_active");
        assert_eq!(fs::read(home.identity_path())?, before);
        Ok(())
    }
    #[test]
    fn mismatch_changes_nothing_and_operations_are_busy_while_locked() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        let before = fs::read(home.identity_path())?;
        let wrong = iroh::SecretKey::generate().public();
        assert_eq!(crate::error::code_of(&activate(&home, wrong, prepared.next).err().unwrap()), ErrorCode::IdentityMismatch);
        assert_eq!(fs::read(home.identity_path())?, before);
        assert_eq!(status(&home)?.next, Some(prepared.next));
        let _lock = IdentityLock::acquire(&home)?;
        assert_eq!(crate::error::code_of(&prepare(&home).err().unwrap()), ErrorCode::IdentityBusy);
        Ok(())
    }
    #[test]
    fn retired_secret_expires_without_touching_active_or_policy() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        let _activation = activate(&home, prepared.current, prepared.next)?;
        let active = fs::read(home.identity_path())?;
        let retired: Vec<_> = fs::read_dir(home.root())?.filter_map(|e| e.ok()).filter(|e| e.file_name().to_string_lossy().starts_with("identity.retired.")).collect();
        assert_eq!(retired.len(), 1);
        cleanup_retired_locked(&home, now().saturating_add(RETENTION_SECONDS))?;
        assert!(!retired[0].path().exists());
        assert_eq!(fs::read(home.identity_path())?, active);
        Ok(())
    }

    #[tokio::test]
    async fn restart_failure_has_both_ids_and_retry_only_restarts() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        let error = activate_with_restart(&home, prepared.current, prepared.next, || async { Ok(()) }, || async {
            anyhow::bail!("injected restart failure")
        }, |error| async { error }).await.unwrap_err();
        assert_eq!(crate::error::code_of(&error), ErrorCode::RestartFailed);
        let report = crate::error::render(&error, true);
        let json: serde_json::Value = serde_json::from_str(report.stdout.as_deref().unwrap())?;
        assert_eq!(json["error"]["previous"], prepared.current.to_string());
        assert_eq!(json["error"]["active"], prepared.next.to_string());
        assert_eq!(status(&home)?.current, prepared.next);
        let active = fs::read(home.identity_path())?;
        let result = activate_with_restart(&home, prepared.current, prepared.next, || async { Err::<(), _>(anyhow::anyhow!("retry must not preserve again")) }, || async {
            assert_eq!(load_or_create_identity(&home)?.public(), prepared.next);
            Ok(())
        }, |error| async { error }).await?;
        assert_eq!(result.outcome, "already_active");
        assert_eq!(fs::read(home.identity_path())?, active);
        Ok(())
    }

    #[test]
    fn startup_retires_dials_but_preserves_all_other_home_consumers() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        let peer = iroh::SecretKey::generate().public();
        let config: crate::config::FabricConfig = toml::from_str(&format!(
            "[[exposes]]\nprotocol = 'web'\ntype = 'socket'\nsocket = '/tmp/web.sock'\n[[dials]]\npeer = '{peer}'\nprotocol = 'web'\n"
        ))?;
        config.save(&home)?;
        let original: toml::Value = toml::from_str(&fs::read_to_string(home.config_path())?)?;
        let preserved = ["peers.toml", "runtime-peers.toml", "audit.jsonl", "syncs.toml"];
        for filename in preserved {
            fs::write(home.root().join(filename), format!("unchanged {filename}"))?;
        }
        let activation = activate(&home, prepared.current, prepared.next)?;
        reconcile_startup(&home)?;
        assert!(crate::config::FabricConfig::load(&home)?.dials().is_empty());
        let after: toml::Value = toml::from_str(&fs::read_to_string(home.config_path())?)?;
        let mut expected = original;
        expected["dials"] = toml::Value::Array(Vec::new());
        assert_eq!(after, expected);
        for filename in preserved {
            assert_eq!(fs::read_to_string(home.root().join(filename))?, format!("unchanged {filename}"));
        }
        assert!(!home.root().join("identity.activation").exists());
        drop(activation);
        Ok(())
    }

    #[tokio::test]
    async fn failed_pre_swap_stop_keeps_prepared_identity_and_never_restarts() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        let before = fs::read(home.identity_path())?;
        let error = activate_with_restart(&home, prepared.current, prepared.next,
            || async { Err::<(), _>(anyhow::anyhow!("old daemon stop barrier failed")) },
            || async { panic!("must not restart or rename after a failed stop barrier") },
            |error| async { error },
        ).await.unwrap_err();
        assert!(error.to_string().contains("stop barrier"));
        assert_eq!(fs::read(home.identity_path())?, before);
        assert_eq!(status(&home)?.state, "prepared");
        assert_eq!(status(&home)?.next, Some(prepared.next));
        Ok(())
    }

    #[tokio::test]
    async fn startup_lease_spans_rename_and_is_released_before_restart() -> Result<()> {
        struct Guard { lease: Option<File>, home: FabricHome, next: EndpointId }
        impl Drop for Guard {
            fn drop(&mut self) {
                assert_eq!(read_id(&self.home.identity_path()).unwrap(), self.next);
                assert!(crate::daemon::try_daemon_lease(&self.home).unwrap().is_none());
                self.lease.take();
            }
        }
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        activate_with_restart(&home, prepared.current, prepared.next,
            || async {
                Ok(Guard { lease: crate::daemon::try_daemon_lease(&home)?, home: home.clone(), next: prepared.next })
            },
            || async {
                assert!(crate::daemon::try_daemon_lease(&home)?.is_some());
                Ok(())
            },
            |error| async { error },
        ).await?;
        Ok(())
    }

    #[test]
    fn incomplete_pre_rename_intent_staging_does_not_block_prepared_startup() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        // A crash may interrupt a staging write, but never publishes its bytes.
        fs::write(home.root().join("identity.tmp"), b"partial")?;
        reconcile_startup(&home)?;
        assert_eq!(status(&home)?.current, prepared.current);
        assert_eq!(status(&home)?.next, Some(prepared.next));
        drop(activate(&home, prepared.current, prepared.next)?);
        assert_eq!(fs::read_to_string(home.root().join("identity.activation"))?, prepared.next.to_string());
        Ok(())
    }

    #[test]
    fn cleanup_errors_and_corrupt_pending_do_not_block_status_or_prepare() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let first = prepare(&home)?;
        fs::create_dir(home.root().join(format!("identity.retired.0.{}.toml", first.current)))?;
        fs::write(pending(&home), "not a private key")?;
        let state = status(&home)?;
        assert_eq!(state.current, first.current);
        assert_eq!(state.next, None);
        let replacement = prepare(&home)?;
        assert_eq!(replacement.current, first.current);
        assert_ne!(replacement.next, first.next);
        assert_eq!(prepare(&home)?.next, replacement.next);
        Ok(())
    }

    #[test]
    fn private_scratch_writes_do_not_follow_the_old_shared_symlink() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        let sentinel = home.root().join("sentinel");
        fs::write(&sentinel, b"unchanged")?;
        std::os::unix::fs::symlink(&sentinel, home.root().join("identity.tmp"))?;
        write_runtime_snapshot(&home, prepared.next, Vec::new())?;
        drop(activate(&home, prepared.current, prepared.next)?);
        assert_eq!(fs::read(&sentinel)?, b"unchanged");
        Ok(())
    }

    #[tokio::test]
    async fn activation_rechecks_pending_guard_after_preservation() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        let prepared = prepare(&home)?;
        let error = activate_with_restart(&home, prepared.current, prepared.next,
            || async {
                fs::remove_file(pending(&home))?;
                generate_identity_file(&pending(&home))?;
                Ok(())
            },
            || async { panic!("changed pending guard must not restart or rename") },
            |error| async { error },
        ).await.unwrap_err();
        assert_eq!(crate::error::code_of(&error), ErrorCode::IdentityMismatch);
        assert_eq!(status(&home)?.current, prepared.current);
        Ok(())
    }
    #[tokio::test]
    async fn failed_runtime_launch_does_not_enable_retained_runtime_policy() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        prepare(&home)?;
        record_runtime_mode(&home, false)?;
        fs::write(home.config_path(), "invalid = [")?;
        let result = crate::daemon::FabricNode::start_with_daemon_options(home.clone(),
            crate::daemon::DaemonOptions { runtime_peers: true, ..Default::default() },
        ).await;
        assert!(result.is_err());
        assert!(!home.root().join("identity.runtime-peers").exists());
        Ok(())
    }
}
