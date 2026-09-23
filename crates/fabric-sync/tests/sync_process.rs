//! The process boundary: sync running in the companion, reached by the daemon
//! over the local bridge in both directions.
//!
//! Every test here starts real daemons over real iroh on one machine, and a
//! companion runtime beside the daemons that delegate. The runtime is the same
//! code `fabric-sync` runs under the service manager, on the same two sockets,
//! so what passes here is what production runs. One test kills the real
//! `fabric-sync` binary mid-pass, because process exit is the one crash an
//! in-process runtime cannot stand in for.
//!
//! The mixed matrix is the plan's four cases: old to old, new companion to old
//! daemon, old daemon to new companion, new to new. "Old" is this build's
//! embedded owner, which is what every peer runs until it takes the paired
//! release; the wire between them is the unchanged `fabric/sync/1`.
//!
//! As in `folder_sync.rs`: assert the mechanism, not only the outcome. Every
//! convergence check also asserts that no delta fallback fired.

use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use fabric::{
    config::{FabricHome, PeerBook},
    control::{ControlRequest, ControlResponse, SyncEntryStatus, SyncRuntimeStatus},
    daemon::{FabricNode, send_control},
};
use fabric_sync::{SyncOwnerLease, SyncOwnerLeaseState, SyncPaths, companion};

mod support;
#[allow(unused_imports)]
use support::{fabric_bin as fabric_binary, old_fabric_bin};

use tempfile::TempDir;
use tokio::sync::Mutex;

/// Real daemons bind real sockets; keep the cases serialized.
static SYNC_PROCESS_LOCK: Mutex<()> = Mutex::const_new(());

const ENTRY: &str = "shared";

/// Which build a node runs. `New` is this build: a daemon in this process and a
/// companion in this process, through the same two sockets production uses.
/// `Old` is a deployed binary from before the process boundary, run as its own
/// process with its embedded engine: the peer a roaming laptop still is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    New,
    Old,
}

enum Daemon {
    New(FabricNode),
    Old {
        child: std::process::Child,
        bin: String,
        id: iroh::EndpointId,
        addr: iroh::EndpointAddr,
    },
}

struct Node {
    _dir: TempDir,
    home: FabricHome,
    folder: PathBuf,
    daemon: Daemon,
    companion: Option<companion::CompanionHandle>,
    side: Side,
}

impl Node {
    async fn start(side: Side, policy: &str) -> Result<Self> {
        let dir = TempDir::new()?;
        let home = FabricHome::new(dir.path());
        let folder = dir.path().join("folder");
        std::fs::create_dir_all(&folder)?;
        write_sync(&home, &folder, policy)?;
        let (daemon, companion) = match side {
            Side::New => {
                let node = FabricNode::start(home.clone()).await?;
                let handle = companion::start(home.clone()).await?;
                handle.wait_until_active(Duration::from_secs(20)).await?;
                (Daemon::New(node), Some(handle))
            }
            Side::Old => {
                let bin = old_fabric_bin().context("FABRIC_OLD_BIN names no deployed binary")?;
                home.prepare()?;
                fabric::config::generate_identity_file(&home.identity_path())?;
                let child = Command::new(&bin)
                    .arg("--home")
                    .arg(home.root())
                    .arg("daemon")
                    // A 0.2.14 binary can own sync either way; the ones before
                    // it ignore this and own it embedded. Both are old to us.
                    .env("FABRIC_SYNC_OWNER", "embedded")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?;
                for _ in 0..200 {
                    if send_control(&home, ControlRequest::Status).await.is_ok() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                let id: iroh::EndpointId = old_cli(&bin, &home, &["id"])?.trim().parse()?;
                let addr: iroh::EndpointAddr = serde_json::from_str(old_cli(&bin, &home, &["addr"])?.trim())?;
                (
                    Daemon::Old {
                        child,
                        bin,
                        id,
                        addr,
                    },
                    None,
                )
            }
        };
        Ok(Self {
            _dir: dir,
            home,
            folder,
            daemon,
            companion,
            side,
        })
    }

    fn id(&self) -> iroh::EndpointId {
        match &self.daemon {
            Daemon::New(node) => node.id(),
            Daemon::Old { id, .. } => *id,
        }
    }

    fn addr(&self) -> iroh::EndpointAddr {
        match &self.daemon {
            Daemon::New(node) => node.addr(),
            Daemon::Old { addr, .. } => addr.clone(),
        }
    }

    async fn reload_peers(&self) -> Result<()> {
        match &self.daemon {
            Daemon::New(node) => node.state().reload_peers().await,
            Daemon::Old { bin, .. } => old_cli(bin, &self.home, &["reload-peers"]).map(|_| ()),
        }
    }

    async fn stop(self) -> Result<()> {
        if let Some(companion) = self.companion {
            companion.shutdown().await?;
        }
        match self.daemon {
            Daemon::New(node) => node.shutdown().await,
            Daemon::Old { mut child, .. } => {
                unsafe {
                    libc::kill(child.id() as i32, libc::SIGTERM);
                }
                child.wait()?;
                Ok(())
            }
        }
    }
}

fn old_cli(bin: &str, home: &FabricHome, args: &[&str]) -> Result<String> {
    let output = Command::new(bin)
        .arg("--home")
        .arg(home.root())
        .args(args)
        .output()?;
    if !output.status.success() {
        bail!(
            "old fabric {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn write_sync(home: &FabricHome, folder: &Path, policy: &str) -> Result<()> {
    let toml =
        format!("[[sync]]\nname = {ENTRY:?}\nfolder = {folder:?}\npeers = \"*\"\npolicy = {policy:?}\n");
    std::fs::write(home.syncs_path(), toml)?;
    Ok(())
}

async fn trust(a: &Node, b: &Node, name: &str) -> Result<()> {
    let mut peers = PeerBook::load(&a.home)?;
    peers.add_with_allow(
        b.id(),
        Some(name.to_string()),
        Some(b.addr()),
        Some(vec!["sync".to_string()]),
    );
    peers.save(&a.home)?;
    a.reload_peers().await?;
    Ok(())
}

async fn pair(a: &Node, b: &Node) -> Result<()> {
    trust(a, b, "b").await?;
    trust(b, a, "a").await?;
    Ok(())
}

async fn sync_status(home: &FabricHome) -> Result<(Vec<SyncEntryStatus>, SyncRuntimeStatus)> {
    let mut last = None;
    for _ in 0..50 {
        match send_control(home, ControlRequest::SyncStatus).await {
            Ok(ControlResponse::SyncStatus { entries, runtime }) => return Ok((entries, runtime)),
            Ok(other) => bail!("expected SyncStatus, got {other:?}"),
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("no SyncStatus after 50 attempts")))
}

async fn entry_status(home: &FabricHome) -> Result<SyncEntryStatus> {
    let (entries, runtime) = sync_status(home).await?;
    entries
        .into_iter()
        .find(|entry| entry.name == ENTRY)
        .with_context(|| format!("the {ENTRY} entry is absent; runtime {runtime:?}"))
}

async fn wait_for_file(path: &Path, expected: &[u8]) -> bool {
    for _ in 0..100 {
        if std::fs::read(path).map(|bytes| bytes == expected).unwrap_or(false) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn wait_for_missing(path: &Path) -> bool {
    for _ in 0..100 {
        if !path.exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn wait_for_equal_digests(a: &Node, b: &Node) -> Result<String> {
    let mut last = (String::new(), String::new());
    for _ in 0..100 {
        let da = entry_status(&a.home).await?.digest;
        let db = entry_status(&b.home).await?.digest;
        if !da.is_empty() && da == db {
            return Ok(da);
        }
        last = (da, db);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bail!("digests never agreed: a={} b={}", last.0, last.1)
}

/// Two-way change, a delete, equal digests, and no fallback. The whole
/// contract for one pair, in one place, so each matrix case is one call.
async fn prove_pair(a: &Node, b: &Node) -> Result<()> {
    std::fs::write(a.folder.join("from-a.txt"), b"written on a")?;
    assert!(
        wait_for_file(&b.folder.join("from-a.txt"), b"written on a").await,
        "{:?} -> {:?}: a's file never reached b",
        a.side,
        b.side
    );
    std::fs::write(b.folder.join("from-b.txt"), b"written on b")?;
    assert!(
        wait_for_file(&a.folder.join("from-b.txt"), b"written on b").await,
        "{:?} -> {:?}: b's file never reached a",
        a.side,
        b.side
    );
    std::fs::remove_file(a.folder.join("from-a.txt"))?;
    assert!(
        wait_for_missing(&b.folder.join("from-a.txt")).await,
        "{:?} -> {:?}: a's delete never reached b",
        a.side,
        b.side
    );
    wait_for_equal_digests(a, b).await?;
    let (sa, sb) = (entry_status(&a.home).await?, entry_status(&b.home).await?);
    assert_eq!(sa.delta_fallbacks, 0, "a fell back to full state");
    assert_eq!(sb.delta_fallbacks, 0, "b fell back to full state");
    assert_eq!(sa.tombstones, 1, "a should hold one tombstone");
    assert_eq!(sb.tombstones, 1, "b should hold one tombstone");
    assert!(sa.stopped_peers.is_empty(), "a stopped: {:?}", sa.stopped_peers);
    assert!(sb.stopped_peers.is_empty(), "b stopped: {:?}", sb.stopped_peers);
    Ok(())
}

fn runtime_of(side: Side) -> &'static str {
    match side {
        Side::Old => "embedded",
        Side::New => "companion",
    }
}

/// One matrix case. A case with an old side needs `FABRIC_OLD_BIN`; without it
/// the case says so and proves nothing, which CI prevents by setting it.
async fn matrix_case(a_side: Side, b_side: Side) -> Result<()> {
    if (a_side == Side::Old || b_side == Side::Old) && old_fabric_bin().is_none() {
        println!("SKIPPED: set FABRIC_OLD_BIN to a deployed pre-boundary fabric binary");
        return Ok(());
    }
    let _guard = SYNC_PROCESS_LOCK.lock().await;
    let a = Node::start(a_side, "bus").await?;
    let b = Node::start(b_side, "bus").await?;
    pair(&a, &b).await?;
    assert_eq!(sync_status(&a.home).await?.1.owner, runtime_of(a_side));
    assert_eq!(sync_status(&b.home).await?.1.owner, runtime_of(b_side));
    prove_pair(&a, &b).await?;
    a.stop().await?;
    b.stop().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_fleet_old_daemon_to_old_daemon() -> Result<()> {
    matrix_case(Side::Old, Side::Old).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_fleet_new_companion_to_old_daemon() -> Result<()> {
    matrix_case(Side::New, Side::Old).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_fleet_old_daemon_to_new_companion() -> Result<()> {
    matrix_case(Side::Old, Side::New).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_fleet_new_companion_to_new_companion() -> Result<()> {
    matrix_case(Side::New, Side::New).await
}

/// A delegating daemon never takes the state lease; the companion holds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_companion_holds_the_lease_and_the_daemon_constructs_no_engine() -> Result<()> {
    let _guard = SYNC_PROCESS_LOCK.lock().await;
    let mut a = Node::start(Side::New, "catalog").await?;
    let paths = SyncPaths::new(a.home.syncs_path(), a.home.root().join("sync"));
    assert_eq!(SyncOwnerLease::probe(&paths)?, SyncOwnerLeaseState::Held);
    let (entries, runtime) = sync_status(&a.home).await?;
    assert_eq!(runtime, SyncRuntimeStatus::new("companion", "active"));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, ENTRY);

    let companion = a.companion.take().expect("started with a companion");
    companion.shutdown().await?;
    assert_eq!(
        SyncOwnerLease::probe(&paths)?,
        SyncOwnerLeaseState::Available,
        "the companion released the lease when it stopped"
    );
    a.stop().await
}

/// With the companion stopped, every other service runs, the daemon says so in
/// words, and a peer that syncs toward this machine is told `unavailable`
/// rather than left to think the network dropped it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_companion_leaves_the_daemon_healthy_and_is_reported_as_unavailable() -> Result<()> {
    let _guard = SYNC_PROCESS_LOCK.lock().await;
    let mut a = Node::start(Side::New, "bus").await?;
    let b = Node::start(Side::New, "bus").await?;
    pair(&a, &b).await?;

    a.companion.take().expect("started").shutdown().await?;
    // The daemon holds the last heartbeat for the presence window; the
    // companion's socket is gone now, so a status request fails fast and
    // honestly instead of waiting the window out.
    let (entries, runtime) = sync_status(&a.home).await?;
    assert_eq!(runtime.owner, "unavailable", "{runtime:?}");
    assert!(entries.is_empty(), "no engine answers for these entries: {entries:?}");
    let status = send_control(&a.home, ControlRequest::Status).await?;
    assert!(matches!(status, ControlResponse::Status { .. }), "the daemon itself is healthy");

    // b keeps syncing toward a and is told why nothing arrives.
    std::fs::write(b.folder.join("while-away.txt"), b"nobody home")?;
    let mut stopped = Vec::new();
    for _ in 0..100 {
        stopped = entry_status(&b.home).await?.stopped_peers;
        if stopped.iter().any(|(_, why)| why == "unavailable") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        stopped.iter().any(|(peer, why)| peer == "a" && why == "unavailable"),
        "b was not told that a has no sync owner: {stopped:?}"
    );

    // The companion returns and the folder converges; nothing needed a restart.
    let handle = companion::start(a.home.clone()).await?;
    handle.wait_until_active(Duration::from_secs(20)).await?;
    a.companion = Some(handle);
    assert!(
        wait_for_file(&a.folder.join("while-away.txt"), b"nobody home").await,
        "b's file never reached a after the companion returned"
    );
    wait_for_equal_digests(&a, &b).await?;
    a.stop().await?;
    b.stop().await
}

/// A companion of a different build is refused, holds no lease, and is named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_incompatible_companion_is_refused_and_named() -> Result<()> {
    let _guard = SYNC_PROCESS_LOCK.lock().await;
    let mut a = Node::start(Side::New, "bus").await?;
    a.companion.take().expect("started").shutdown().await?;
    let refused = send_control(
        &a.home,
        ControlRequest::SyncCompanionHello {
            version: "0.0.0+different".to_string(),
            sync_ipc_magic: fabric::sync::ipc::IPC_MAGIC.to_string(),
            sync_ipc_version: fabric::sync::ipc::IPC_VERSION,
            companion_socket: Some(a.home.sync_companion_socket_path()),
        },
    )
    .await
    .expect_err("a different build was accepted");
    assert!(format!("{refused:#}").contains("0.0.0+different"));
    let (_, runtime) = sync_status(&a.home).await?;
    assert_eq!(runtime, SyncRuntimeStatus::unavailable("incompatible"));
    let paths = SyncPaths::new(a.home.syncs_path(), a.home.root().join("sync"));
    assert_ne!(SyncOwnerLease::probe(&paths)?, SyncOwnerLeaseState::Held);
    a.stop().await
}

/// The daemon restarts under a running companion, as every update does. The
/// new daemon mints a new nonce; the companion re-attaches on its own and the
/// engine that kept running keeps its state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_companion_reattaches_after_the_daemon_restarts() -> Result<()> {
    let _guard = SYNC_PROCESS_LOCK.lock().await;
    let mut a = Node::start(Side::New, "bus").await?;
    let b = Node::start(Side::New, "bus").await?;
    pair(&a, &b).await?;
    std::fs::write(a.folder.join("before.txt"), b"before the restart")?;
    assert!(wait_for_file(&b.folder.join("before.txt"), b"before the restart").await);

    // The shape of every update: the daemon goes down and comes back while the
    // companion keeps running.
    // A placeholder node in its own live home fills the slot while a's daemon
    // is down; the directory must outlive the placeholder.
    let placeholder_dir = TempDir::new()?;
    let placeholder = FabricNode::start(FabricHome::new(placeholder_dir.path())).await?;
    let stopped = std::mem::replace(&mut a.daemon, Daemon::New(placeholder));
    match stopped {
        Daemon::New(node) => node.shutdown().await?,
        Daemon::Old { .. } => unreachable!("a is this build"),
    }
    let companion = a.companion.as_ref().expect("started");
    companion
        .wait_for(Duration::from_secs(30), |phase| !phase.is_active())
        .await
        .context("the companion never noticed the daemon leaving")?;
    let placeholder = std::mem::replace(
        &mut a.daemon,
        Daemon::New(FabricNode::start(a.home.clone()).await?),
    );
    match placeholder {
        Daemon::New(node) => node.shutdown().await?,
        Daemon::Old { .. } => unreachable!("the placeholder is this build"),
    }
    // b must re-trust a's new address; the identity is the same.
    trust(&b, &a, "a").await?;

    companion
        .wait_for(Duration::from_secs(30), |phase| phase.is_active())
        .await?;
    let mut runtime = SyncRuntimeStatus::default();
    for _ in 0..100 {
        runtime = sync_status(&a.home).await?.1;
        if runtime.owner == "companion" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(runtime, SyncRuntimeStatus::new("companion", "active"));
    let paths = SyncPaths::new(a.home.syncs_path(), a.home.root().join("sync"));
    assert_eq!(SyncOwnerLease::probe(&paths)?, SyncOwnerLeaseState::Held);

    std::fs::write(a.folder.join("after.txt"), b"after the restart")?;
    assert!(
        wait_for_file(&b.folder.join("after.txt"), b"after the restart").await,
        "the re-attached companion did not sync outbound"
    );
    std::fs::write(b.folder.join("inbound.txt"), b"inbound after restart")?;
    assert!(
        wait_for_file(&a.folder.join("inbound.txt"), b"inbound after restart").await,
        "the restarted daemon did not relay inbound to the companion"
    );
    wait_for_equal_digests(&a, &b).await?;
    a.stop().await?;
    b.stop().await
}

/// Editing `syncs.toml` and reloading reaches the companion's engine, and a
/// rejected reload is refused by the daemon before the companion hears of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reload_reaches_the_companion_and_a_bad_one_is_refused_first() -> Result<()> {
    let _guard = SYNC_PROCESS_LOCK.lock().await;
    let a = Node::start(Side::New, "bus").await?;
    let b = Node::start(Side::New, "bus").await?;
    pair(&a, &b).await?;

    let second = a.home.root().join("second");
    std::fs::create_dir_all(&second)?;
    let bad = format!(
        "[[sync]]\nname = {ENTRY:?}\nfolder = {:?}\npeers = \"*\"\npolicy = \"bus\"\n\n[[sync]]\nname = \"second\"\nfolder = {second:?}\npeers = [\"nobody\"]\npolicy = \"bus\"\n",
        a.folder
    );
    std::fs::write(a.home.syncs_path(), bad)?;
    let refused = send_control(&a.home, ControlRequest::SyncReload)
        .await
        .expect_err("an unknown selector was accepted");
    assert!(format!("{refused:#}").contains("nobody"), "{refused:#}");
    assert_eq!(sync_status(&a.home).await?.0.len(), 1, "the bad file did not load");

    let good = format!(
        "[[sync]]\nname = {ENTRY:?}\nfolder = {:?}\npeers = \"*\"\npolicy = \"bus\"\n\n[[sync]]\nname = \"second\"\nfolder = {second:?}\npeers = \"*\"\npolicy = \"bus\"\n",
        a.folder
    );
    std::fs::write(a.home.syncs_path(), good)?;
    send_control(&a.home, ControlRequest::SyncReload).await?;
    let mut names: Vec<String> = sync_status(&a.home)
        .await?
        .0
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["second".to_string(), ENTRY.to_string()]);
    a.stop().await?;
    b.stop().await
}

fn sync_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric-sync")
}

fn spawn_companion(home: &FabricHome) -> Result<std::process::Child> {
    Ok(Command::new(sync_bin())
        .args(["--home", home.root().to_str().context("non-UTF-8 home")?, "--standby"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?)
}

async fn wait_for_runtime(home: &FabricHome, owner: &str, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = SyncRuntimeStatus::default();
    while tokio::time::Instant::now() < deadline {
        last = sync_status(home).await?.1;
        if last.owner == owner {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bail!("the sync runtime never became {owner}: {last:?}")
}

fn count_files(folder: &Path) -> usize {
    std::fs::read_dir(folder)
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "bin"))
                .count()
        })
        .unwrap_or(0)
}

/// The real `fabric-sync` process, killed with SIGKILL in the middle of a
/// pass. The daemon stays up and never starts an engine of its own; the lease
/// is free; a restarted companion converges the folder from the durable state.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_companion_killed_mid_pass_recovers_on_restart_with_one_owner() -> Result<()> {
    let _guard = SYNC_PROCESS_LOCK.lock().await;
    let mut a = Node::start(Side::New, "bus").await?;
    a.companion.take().expect("started").shutdown().await?;
    let b = Node::start(Side::New, "bus").await?;
    pair(&a, &b).await?;
    // Slow every walk on a so the pass has a middle to be killed in.
    #[cfg(debug_assertions)]
    std::fs::write(a.folder.join(".fabric-test-walk-hold-ms"), b"300")?;

    const FILES: usize = 120;
    let payload = vec![7u8; 48 * 1024];
    for index in 0..FILES {
        std::fs::write(a.folder.join(format!("blob-{index:03}.bin")), &payload)?;
    }

    let mut child = spawn_companion(&a.home)?;
    wait_for_runtime(&a.home, "companion", Duration::from_secs(30)).await?;
    // Wait for the pass to be under way: something arrived, not everything.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let arrived = count_files(&b.folder);
        if arrived > 0 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            bail!("nothing reached b within a minute");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let arrived_at_kill = count_files(&b.folder);
    unsafe {
        libc::kill(child.id() as i32, libc::SIGKILL);
    }
    child.wait()?;
    println!("killed the companion with {arrived_at_kill} of {FILES} files delivered");

    let paths = SyncPaths::new(a.home.syncs_path(), a.home.root().join("sync"));
    assert_ne!(
        SyncOwnerLease::probe(&paths)?,
        SyncOwnerLeaseState::Held,
        "a killed companion must not leave the lease held"
    );
    // The daemon is healthy, and did not start an embedded engine to cover.
    let status = send_control(&a.home, ControlRequest::Status).await?;
    assert!(matches!(status, ControlResponse::Status { .. }));
    let (entries, runtime) = sync_status(&a.home).await?;
    assert_eq!(runtime.owner, "unavailable", "{runtime:?}");
    assert!(entries.is_empty());
    assert_ne!(SyncOwnerLease::probe(&paths)?, SyncOwnerLeaseState::Held);

    let mut child = spawn_companion(&a.home)?;
    wait_for_runtime(&a.home, "companion", Duration::from_secs(30)).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while count_files(&b.folder) < FILES {
        if tokio::time::Instant::now() > deadline {
            bail!("only {} of {FILES} files reached b after the restart", count_files(&b.folder));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    wait_for_equal_digests(&a, &b).await?;
    for index in 0..FILES {
        assert_eq!(
            std::fs::read(b.folder.join(format!("blob-{index:03}.bin")))?,
            payload,
            "blob {index} differs"
        );
    }
    assert_eq!(entry_status(&a.home).await?.stopped_peers, Vec::<(String, String)>::new());
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    child.wait()?;
    a.stop().await?;
    b.stop().await
}

// ---- the throughput comparison ----
//
// The plan's transport decision was taken without a measurement, because the
// bridge did not exist. This is the measurement: the same two-daemon workload
// over a fixed window, once with each daemon owning sync (the embedded path)
// and once with each daemon delegating to a real `fabric-sync` process (the
// bridge path). Real binaries, so each process's CPU and peak RSS is its own.
//
// `cargo test --test sync_process throughput -- --ignored --nocapture`
//
// Knobs: FABRIC_THROUGHPUT_WINDOW_SECS (600), FABRIC_THROUGHPUT_FILE_BYTES
// (262144), FABRIC_THROUGHPUT_WRITE_EVERY_MS (50), FABRIC_EMBEDDED_BIN (a
// deployed fabric binary to run the embedded path with; default this build
// with FABRIC_SYNC_OWNER=embedded), FABRIC_THROUGHPUT_ASSERT=1 to fail below
// the plan's 90 percent floor. Linux only: it reads procfs.

#[cfg(target_os = "linux")]
mod throughput {
    use super::*;
    use std::{
        io::Write,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        thread,
        time::Instant,
    };

    fn fabric_bin() -> String {
        fabric_binary().to_string()
    }

    fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
            .unwrap_or(default)
    }

    struct Proc {
        name: String,
        child: std::process::Child,
        cpu_start_ticks: u64,
        peak_rss_kib: u64,
    }

    impl Proc {
        fn pid(&self) -> u32 {
            self.child.id()
        }
    }

    fn cpu_ticks(pid: u32) -> Result<u64> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
        // Fields after the ")" are space separated; utime is field 14, stime 15.
        let after = stat.rsplit_once(')').context("stat has no comm")?.1;
        let fields: Vec<&str> = after.split_whitespace().collect();
        let utime: u64 = fields[11].parse()?;
        let stime: u64 = fields[12].parse()?;
        Ok(utime + stime)
    }

    fn rss_kib(pid: u32) -> Result<u64> {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                return Ok(rest.trim().trim_end_matches("kB").trim().parse()?);
            }
        }
        bail!("no VmRSS for {pid}")
    }

    fn clk_tck() -> f64 {
        // SAFETY: sysconf has no preconditions.
        unsafe { libc::sysconf(libc::_SC_CLK_TCK) as f64 }
    }

    fn fabric_output(home: &FabricHome, args: &[&str]) -> Result<String> {
        let output = Command::new(fabric_bin())
            .arg("--home")
            .arg(home.root())
            .args(args)
            .output()?;
        if !output.status.success() {
            bail!(
                "fabric {args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8(output.stdout)?)
    }

    fn spawn_daemon(home: &FabricHome, bin: &str, owner: &str) -> Result<Proc> {
        let child = Command::new(bin)
            .arg("--home")
            .arg(home.root())
            .arg("daemon")
            .env("FABRIC_SYNC_OWNER", owner)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let pid = child.id();
        Ok(Proc {
            name: format!("daemon@{}", home.root().display()),
            child,
            cpu_start_ticks: 0,
            peak_rss_kib: rss_kib(pid).unwrap_or(0),
        })
    }

    fn spawn_companion_proc(home: &FabricHome) -> Result<Proc> {
        let child = spawn_companion(home)?;
        let pid = child.id();
        Ok(Proc {
            name: format!("companion@{}", home.root().display()),
            child,
            cpu_start_ticks: 0,
            peak_rss_kib: rss_kib(pid).unwrap_or(0),
        })
    }

    async fn wait_for_control(home: &FabricHome) -> Result<()> {
        for _ in 0..200 {
            if send_control(home, ControlRequest::Status).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        bail!("the daemon at {} never answered", home.root().display())
    }

    fn trust_by_cli(home: &FabricHome, other: &FabricHome, name: &str) -> Result<()> {
        let addr = fabric_output(other, &["addr"])?;
        let addr: iroh::EndpointAddr = serde_json::from_str(addr.trim())?;
        let mut peers = PeerBook::load(home)?;
        peers.add_with_allow(
            addr.id,
            Some(name.to_string()),
            Some(addr),
            Some(vec!["sync".to_string()]),
        );
        peers.save(home)?;
        fabric_output(home, &["reload-peers"])?;
        Ok(())
    }

    #[derive(Debug)]
    struct Report {
        mode: &'static str,
        window_secs: f64,
        offered_bytes: u64,
        delivered_bytes: u64,
        wire_bytes: u64,
        processes: Vec<(String, f64, u64)>,
    }

    impl Report {
        fn print(&self) {
            println!(
                "THROUGHPUT mode={} window={:.1}s offered={} B delivered={} B ({:.2} MB/s) wire={} B",
                self.mode,
                self.window_secs,
                self.offered_bytes,
                self.delivered_bytes,
                self.delivered_bytes as f64 / self.window_secs / 1e6,
                self.wire_bytes
            );
            for (name, cpu_secs, peak_rss_kib) in &self.processes {
                println!(
                    "THROUGHPUT   {name}: cpu={cpu_secs:.2}s ({:.2}% of one core) peak_rss={} KiB",
                    cpu_secs / self.window_secs * 100.0,
                    peak_rss_kib
                );
            }
        }
    }

    /// One mode, one window. Returns the report; the caller compares.
    async fn measure(mode: &'static str, daemon_bin: &str, window: Duration) -> Result<Report> {
        let file_bytes: usize = env_or("FABRIC_THROUGHPUT_FILE_BYTES", 256 * 1024);
        let write_every = Duration::from_millis(env_or("FABRIC_THROUGHPUT_WRITE_EVERY_MS", 50));
        const RING: usize = 64;

        let dir_a = TempDir::new()?;
        let dir_b = TempDir::new()?;
        let home_a = FabricHome::new(dir_a.path());
        let home_b = FabricHome::new(dir_b.path());
        let folder_a = dir_a.path().join("folder");
        let folder_b = dir_b.path().join("folder");
        std::fs::create_dir_all(&folder_a)?;
        std::fs::create_dir_all(&folder_b)?;
        write_sync(&home_a, &folder_a, "bus")?;
        write_sync(&home_b, &folder_b, "bus")?;
        for home in [&home_a, &home_b] {
            home.prepare()?;
            fabric::config::generate_identity_file(&home.identity_path())?;
        }

        let owner = if mode == "bridge" { "companion" } else { "embedded" };
        let mut procs = vec![
            spawn_daemon(&home_a, daemon_bin, owner)?,
            spawn_daemon(&home_b, daemon_bin, owner)?,
        ];
        wait_for_control(&home_a).await?;
        wait_for_control(&home_b).await?;
        if mode == "bridge" {
            procs.push(spawn_companion_proc(&home_a)?);
            procs.push(spawn_companion_proc(&home_b)?);
            wait_for_runtime(&home_a, "companion", Duration::from_secs(30)).await?;
            wait_for_runtime(&home_b, "companion", Duration::from_secs(30)).await?;
        } else {
            wait_for_runtime(&home_a, "embedded", Duration::from_secs(30)).await?;
        }
        trust_by_cli(&home_a, &home_b, "b")?;
        trust_by_cli(&home_b, &home_a, "a")?;

        // Prove the pair before measuring, so a broken pairing is not read as
        // zero throughput.
        std::fs::write(folder_a.join("warmup.txt"), b"warm")?;
        assert!(
            wait_for_file(&folder_b.join("warmup.txt"), b"warm").await,
            "{mode}: the pair never synced"
        );
        let wire_before = entry_status(&home_a).await?.reconcile_wire_bytes
            + entry_status(&home_b).await?.reconcile_wire_bytes;
        for proc in &mut procs {
            proc.cpu_start_ticks = cpu_ticks(proc.pid())?;
        }

        // The writer: a ring of RING files, each write a new version whose first
        // 16 bytes are its sequence number, at a bounded offered rate.
        let stop = Arc::new(AtomicBool::new(false));
        let offered = Arc::new(AtomicU64::new(0));
        let writer = {
            let stop = stop.clone();
            let offered = offered.clone();
            let folder = folder_a.clone();
            thread::spawn(move || -> Result<()> {
                let mut sequence: u64 = 0;
                let body = vec![0xA5u8; file_bytes.saturating_sub(16)];
                while !stop.load(Ordering::Acquire) {
                    let name = folder.join(format!("ring-{:02}.bin", sequence as usize % RING));
                    let tmp = folder.join(format!(".ring-{}.tmp", sequence as usize % RING));
                    let mut file = std::fs::File::create(&tmp)?;
                    file.write_all(&format!("{sequence:016}").into_bytes())?;
                    file.write_all(&body)?;
                    drop(file);
                    std::fs::rename(&tmp, &name)?;
                    offered.fetch_add(file_bytes as u64, Ordering::Relaxed);
                    sequence += 1;
                    thread::sleep(write_every);
                }
                Ok(())
            })
        };

        // The reader on b: every 100 ms, read the sequence header of each ring
        // file and count each new version once.
        let started = Instant::now();
        let mut seen = vec![None::<u64>; RING];
        let mut delivered_versions: u64 = 0;
        let mut next_sample = started;
        while started.elapsed() < window {
            for (slot, last) in seen.iter_mut().enumerate() {
                let path = folder_b.join(format!("ring-{slot:02}.bin"));
                let Ok(mut file) = std::fs::File::open(&path) else { continue };
                let mut header = [0u8; 16];
                if std::io::Read::read_exact(&mut file, &mut header).is_err() {
                    continue;
                }
                let Ok(sequence) = std::str::from_utf8(&header)
                    .ok()
                    .and_then(|text| text.parse::<u64>().ok())
                    .context("header")
                else {
                    continue;
                };
                if *last != Some(sequence) {
                    *last = Some(sequence);
                    delivered_versions += 1;
                }
            }
            if Instant::now() >= next_sample {
                for proc in &mut procs {
                    if let Ok(rss) = rss_kib(proc.pid()) {
                        proc.peak_rss_kib = proc.peak_rss_kib.max(rss);
                    }
                }
                next_sample += Duration::from_secs(1);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let window_secs = started.elapsed().as_secs_f64();
        stop.store(true, Ordering::Release);
        writer.join().expect("writer panicked")?;

        let processes = procs
            .iter()
            .map(|proc| {
                let ticks = cpu_ticks(proc.pid()).unwrap_or(proc.cpu_start_ticks);
                (
                    proc.name.clone(),
                    (ticks - proc.cpu_start_ticks) as f64 / clk_tck(),
                    proc.peak_rss_kib,
                )
            })
            .collect();
        let wire_after = entry_status(&home_a).await?.reconcile_wire_bytes
            + entry_status(&home_b).await?.reconcile_wire_bytes;
        let report = Report {
            mode,
            window_secs,
            offered_bytes: offered.load(Ordering::Relaxed),
            delivered_bytes: delivered_versions * file_bytes as u64,
            wire_bytes: wire_after.saturating_sub(wire_before),
            processes,
        };

        for proc in procs.iter_mut().rev() {
            unsafe {
                libc::kill(proc.pid() as i32, libc::SIGTERM);
            }
            let _ = proc.child.wait();
        }
        Ok(report)
    }

    #[ignore = "a 2 x 10-minute measurement with real processes; run on purpose"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sync_throughput_comparison_measurement() -> Result<()> {
        let _guard = SYNC_PROCESS_LOCK.lock().await;
        let window = Duration::from_secs(env_or("FABRIC_THROUGHPUT_WINDOW_SECS", 600));
        let embedded_bin = std::env::var("FABRIC_EMBEDDED_BIN").unwrap_or_else(|_| fabric_bin());
        println!(
            "THROUGHPUT embedded binary {} ({}); bridge binary {} ({})",
            embedded_bin,
            String::from_utf8_lossy(&Command::new(&embedded_bin).arg("--version").output()?.stdout).trim(),
            fabric_bin(),
            fabric::version_string()
        );
        let embedded = measure("embedded", &embedded_bin, window).await?;
        embedded.print();
        let bridge = measure("bridge", &fabric_bin(), window).await?;
        bridge.print();
        let ratio = bridge.delivered_bytes as f64 / embedded.delivered_bytes.max(1) as f64;
        println!(
            "THROUGHPUT bridge/embedded content throughput = {:.3} (floor 0.900)",
            ratio
        );
        if env_or::<u8>("FABRIC_THROUGHPUT_ASSERT", 0) == 1 {
            assert!(
                ratio >= 0.90,
                "the bridge delivered {:.1}% of the embedded path's content throughput; the plan stops activation below 90%",
                ratio * 100.0
            );
        }
        Ok(())
    }
}
