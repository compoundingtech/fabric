//! The permanent latency test: sync activity must not delay exec pipe
//! delivery. The command, workload, window, and bounds did not change while
//! sync was extracted; what changed is where the walk runs. Since activation
//! the hold runs in the companion runtime and the measured pipe stays in the
//! daemon, and since the engine left the core there is no other place it can
//! run.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use fabric::{
    config::{FabricHome, PeerBook},
    control::{ControlRequest, ControlResponse},
    daemon::{FabricNode, send_control},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::UnixStream,
    sync::Mutex,
};

static LATENCY_LOCK: Mutex<()> = Mutex::const_new(());

async fn trust_peer(
    home: &FabricHome,
    node: &FabricNode,
    id: iroh::EndpointId,
    name: Option<&str>,
    addr: Option<iroh::EndpointAddr>,
) -> Result<()> {
    let mut peers = PeerBook::load(home)?;
    peers.add_with_allow(
        id,
        name.map(str::to_string),
        addr,
        Some(
            ["shell", "exec", "sync", "echo", "stdio-cat"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        ),
    );
    peers.save(home)?;
    node.state().reload_peers().await?;
    Ok(())
}

/// The permanent latency test. The command, workload, window, and bounds did
/// not change while sync was extracted; what changed is where the walk runs.
/// Since activation the hold runs in the companion runtime and the measured
/// pipe stays in the daemon.
#[cfg(all(unix, debug_assertions))]
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn sync_walks_do_not_delay_exec_pipe_delivery() -> Result<()> {
    let _guard = LATENCY_LOCK.lock().await;
    let target_dir = TempDir::new()?;
    let source_dir = TempDir::new()?;
    let helper_dir = TempDir::new()?;
    let target_home = FabricHome::new(target_dir.path());
    let source_home = FabricHome::new(source_dir.path());
    let target_folder = target_dir.path().join("shared");
    let source_folder = source_dir.path().join("shared");
    fs::create_dir_all(target_folder.join("data"))?;
    fs::create_dir_all(source_folder.join("data"))?;
    write_latency_sync(&target_home, &target_folder)?;
    write_latency_sync(&source_home, &source_folder)?;
    fs::write(target_folder.join(".fabric-test-walk-hold-ms"), b"500")?;

    let target = FabricNode::start(target_home.clone()).await?;
    let source = FabricNode::start(source_home.clone()).await?;
    let mut companions = Vec::new();
    for home in [&target_home, &source_home] {
        let companion = fabric_sync::companion::start(home.clone()).await?;
        companion.wait_until_active(Duration::from_secs(20)).await?;
        companions.push(companion);
    }
    trust_peer(
        &target_home,
        &target,
        source.id(),
        Some("source"),
        Some(source.addr()),
    )
    .await?;
    trust_peer(
        &source_home,
        &source,
        target.id(),
        Some("target"),
        Some(target.addr()),
    )
    .await?;

    let emitter = compile_pipe_tick(&helper_dir)?;
    target
        .expose_exec("stdio-cat", vec![emitter.display().to_string()])
        .await?;
    let scans_before = sync_full_scans(&target_home, "latency").await?;

    let keep_writing = Arc::new(AtomicBool::new(true));
    let writer_flag = keep_writing.clone();
    let changed_path = target_folder.join("data/changing.txt");
    let writer = thread::spawn(move || {
        let mut revision = 0_u64;
        while writer_flag.load(Ordering::Acquire) {
            fs::write(&changed_path, revision.to_string()).unwrap();
            revision += 1;
            thread::sleep(Duration::from_millis(50));
        }
    });

    let socket = source.dial("target", "stdio-cat").await?;
    let mut lines = BufReader::new(UnixStream::connect(socket).await?).lines();
    let mut first_source = None;
    let mut previous_source = None;
    let mut previous_delivery = None;
    let mut max_source_gap = Duration::ZERO;
    let mut max_delivery_gap = Duration::ZERO;
    let mut samples = 0_usize;

    loop {
        let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await
            .context("the exec pipe stopped delivering records")??
            .context("the exec child ended before the five-second window")?;
        let mut fields = line.split_whitespace();
        let _sequence: u64 = fields.next().context("record has no sequence")?.parse()?;
        let source_nanos: u64 = fields
            .next()
            .context("record has no source time")?
            .parse()?;
        let delivery = Instant::now();
        let source_time = Duration::from_nanos(source_nanos);
        let window_start = *first_source.get_or_insert(source_time);
        if let Some(previous) = previous_source {
            max_source_gap = max_source_gap.max(source_time.saturating_sub(previous));
        }
        if let Some(previous) = previous_delivery {
            max_delivery_gap = max_delivery_gap.max(delivery.saturating_duration_since(previous));
        }
        previous_source = Some(source_time);
        previous_delivery = Some(delivery);
        samples += 1;
        if source_time.saturating_sub(window_start) >= Duration::from_secs(5) {
            break;
        }
    }

    keep_writing.store(false, Ordering::Release);
    writer.join().expect("the filesystem writer panicked");
    let scans_after = sync_full_scans(&target_home, "latency").await?;
    for companion in companions {
        companion.shutdown().await?;
    }
    source.shutdown().await?;
    target.shutdown().await?;

    println!(
        "five-second sync/exec pipe window: samples={samples} source_max={max_source_gap:?} delivery_max={max_delivery_gap:?} scans={}",
        scans_after.saturating_sub(scans_before)
    );
    assert!(
        scans_after >= scans_before + 2,
        "the five-second window ran no sustained sync scan load"
    );
    assert!(
        max_source_gap < Duration::from_millis(50),
        "the producer paused for {max_source_gap:?}; delivery timing cannot diagnose Fabric"
    );
    assert!(
        max_delivery_gap < Duration::from_millis(150),
        "sync activity delayed exec pipe delivery for {max_delivery_gap:?}; this is local scheduler starvation, not network weather"
    );
    Ok(())
}

#[cfg(all(unix, debug_assertions))]
fn write_latency_sync(home: &FabricHome, folder: &Path) -> Result<()> {
    let raw = format!(
        "[[sync]]\nname = \"latency\"\nfolder = {folder:?}\npeers = \"*\"\npolicy = \"bus\"\ninclude = [\"data/**\"]\n"
    );
    fs::write(home.syncs_path(), raw)?;
    Ok(())
}

#[cfg(all(unix, debug_assertions))]
fn compile_pipe_tick(directory: &TempDir) -> Result<PathBuf> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pipe_tick.rs");
    let output = directory.path().join("pipe-tick");
    let status = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
        .arg("--edition=2024")
        .arg(&source)
        .arg("-o")
        .arg(&output)
        .status()
        .with_context(|| format!("failed to compile {}", source.display()))?;
    if !status.success() {
        bail!("rustc failed to compile {}", source.display());
    }
    Ok(output)
}

#[cfg(all(unix, debug_assertions))]
async fn sync_full_scans(home: &FabricHome, name: &str) -> Result<u64> {
    for _ in 0..50 {
        match send_control(home, ControlRequest::SyncStatus).await {
            Ok(ControlResponse::SyncStatus { entries, .. }) => {
                return entries
                    .into_iter()
                    .find(|entry| entry.name == name)
                    .map(|entry| entry.full_scans)
                    .with_context(|| format!("sync entry {name:?} is absent"));
            }
            Ok(other) => bail!("unexpected sync status response: {other:?}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    bail!("the daemon did not return sync status")
}
