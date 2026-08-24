//! End-to-end `fabric sync` over real iroh on one machine — the local stand-in
//! for the Mac -> Hetzner catalog proof. Two daemons, each with a catalog sync
//! entry, mutually trusted: a file dropped on A propagates to B's folder over the
//! `fabric/sync` ALPN, and a delete on B propagates to A and stays deleted.

use std::{path::Path, time::Duration};

use anyhow::Result;
use fabric::{
    config::{FabricHome, PeerBook},
    control::{ControlRequest, ControlResponse, SyncEntryStatus},
    daemon::{FabricNode, send_control},
};
use tempfile::TempDir;
use tokio::sync::Mutex;

static SYNC_SLICE_LOCK: Mutex<()> = Mutex::const_new(());

async fn trust_peer(
    home: &FabricHome,
    node: &FabricNode,
    id: iroh::EndpointId,
    name: &str,
    addr: iroh::EndpointAddr,
) -> Result<()> {
    let mut peers = PeerBook::load(home)?;
    peers.add(id, Some(name.to_string()), Some(addr));
    peers.save(home)?;
    node.state().reload_peers().await?;
    Ok(())
}

fn write_sync(home_dir: &Path, folder: &Path, policy: &str) {
    let toml = format!(
        "[[sync]]\nname = \"shared\"\nfolder = {folder:?}\npeers = \"*\"\npolicy = {policy:?}\n"
    );
    std::fs::write(home_dir.join("syncs.toml"), toml).unwrap();
}

async fn wait_for_file(path: &Path, expected: &[u8]) -> bool {
    for _ in 0..50 {
        if std::fs::read(path).map(|c| c == expected).unwrap_or(false) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn reload_sync(home: &FabricHome) -> Result<()> {
    for _ in 0..50 {
        if send_control(home, ControlRequest::SyncReload).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    send_control(home, ControlRequest::SyncReload).await?;
    Ok(())
}

async fn sync_status(home: &FabricHome, name: &str) -> Result<SyncEntryStatus> {
    let ControlResponse::SyncStatus { entries } =
        send_control(home, ControlRequest::SyncStatus).await?
    else {
        anyhow::bail!("unexpected sync status response");
    };
    entries
        .into_iter()
        .find(|entry| entry.name == name)
        .ok_or_else(|| anyhow::anyhow!("sync status did not include {name:?}"))
}

async fn wait_for_missing(path: &Path) -> bool {
    for _ in 0..50 {
        if !path.exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn assert_stays_missing(path: &Path) {
    for _ in 0..10 {
        assert!(!path.exists(), "{} unexpectedly reappeared", path.display());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_sync_propagates_new_file_and_a_delete_sticks() -> Result<()> {
    let _guard = SYNC_SLICE_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());

    let a_catalog = a_dir.path().join("catalog");
    let b_catalog = b_dir.path().join("catalog");
    std::fs::create_dir_all(&a_catalog)?;
    std::fs::create_dir_all(&b_catalog)?;
    write_sync(a_dir.path(), &a_catalog, "catalog");
    write_sync(b_dir.path(), &b_catalog, "catalog");

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;

    // Mutual trust with address hints for deterministic same-machine dialing.
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    // Drop a host=hetz job into A's catalog and drive a sync (mirrors the CLI's
    // reload after `fabric sync add`).
    std::fs::write(a_catalog.join("job-hetz.toml"), b"host=hetz")?;
    reload_sync(&a_home).await?;

    // B's daemon should watch + receive it fast.
    let b_job = b_catalog.join("job-hetz.toml");
    assert!(
        wait_for_file(&b_job, b"host=hetz").await,
        "job file did not propagate from A to B"
    );

    // A delete on B must stick, on B and on A. Deleting a file is a normal
    // thing a person does and a folder sync that undoes it is broken as a
    // folder sync, whatever the policy once specified.
    std::fs::remove_file(&b_job)?;
    reload_sync(&b_home).await?;
    assert!(
        wait_for_missing(&b_job).await,
        "catalog delete on B was undone on B"
    );
    assert!(
        wait_for_missing(&a_catalog.join("job-hetz.toml")).await,
        "catalog delete on B never reached A"
    );
    assert_stays_missing(&b_job).await;
    assert_stays_missing(&a_catalog.join("job-hetz.toml")).await;

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bus_update_beats_equal_version_delete_then_archive_survives_restart() -> Result<()> {
    let _guard = SYNC_SLICE_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());
    let a_bus = a_dir.path().join("bus");
    let b_bus = b_dir.path().join("bus");
    std::fs::create_dir_all(a_bus.join("inbox"))?;
    std::fs::create_dir_all(b_bus.join("inbox"))?;
    write_sync(a_dir.path(), &a_bus, "bus");
    write_sync(b_dir.path(), &b_bus, "bus");

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    let a_inbox = a_bus.join("inbox/job.md");
    let b_inbox = b_bus.join("inbox/job.md");
    std::fs::write(&a_inbox, b"seed")?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_file(&b_inbox, b"seed").await,
        "initial bus file did not converge"
    );

    // Stop both daemons so an update and delete are independently created from
    // the same v1 baseline. Present must win their equal-version v2 conflict.
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    std::fs::write(&a_inbox, b"concurrent update")?;
    std::fs::remove_file(&b_inbox)?;

    let node_a = FabricNode::start(a_home.clone()).await?;
    // Force A to persist its local v2 Present while B is offline.
    reload_sync(&a_home).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;
    reload_sync(&b_home).await?;
    assert!(
        wait_for_file(&b_inbox, b"concurrent update").await,
        "equal-version update did not beat the concurrent delete"
    );
    assert_eq!(std::fs::read(&a_inbox)?, b"concurrent update");

    // Once B has observed the winner, archiving it is a later local operation:
    // the inbox removal advances to v3 and must propagate along with the new
    // archive path.
    let a_archived = a_bus.join("archive/job.md");
    let b_archived = b_bus.join("archive/job.md");
    std::fs::create_dir_all(b_bus.join("archive"))?;
    std::fs::rename(&b_inbox, &b_archived)?;
    reload_sync(&b_home).await?;
    assert!(
        wait_for_file(&a_archived, b"concurrent update").await,
        "archive path did not propagate"
    );
    assert!(
        wait_for_missing(&a_inbox).await,
        "later higher-version inbox delete did not propagate"
    );

    // Restart both real daemon instances from their persisted state. The
    // tombstone must remain authoritative and the archived bytes must remain.
    node_b.shutdown().await?;
    node_a.shutdown().await?;
    let node_a = FabricNode::start(a_home.clone()).await?;
    reload_sync(&a_home).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;
    reload_sync(&b_home).await?;
    assert!(
        wait_for_file(&a_archived, b"concurrent update").await
            && wait_for_file(&b_archived, b"concurrent update").await,
        "archived bytes did not survive daemon restart"
    );
    assert_stays_missing(&a_inbox).await;
    assert_stays_missing(&b_inbox).await;

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_status_exposes_exact_inbound_scan_ledger() -> Result<()> {
    let _guard = SYNC_SLICE_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());
    let a_bus = a_dir.path().join("bus");
    let b_bus = b_dir.path().join("bus");
    std::fs::create_dir_all(&a_bus)?;
    std::fs::create_dir_all(&b_bus)?;
    write_sync(a_dir.path(), &a_bus, "bus");
    write_sync(b_dir.path(), &b_bus, "bus");

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    let a_file = a_bus.join("ledger.txt");
    let b_file = b_bus.join("ledger.txt");
    std::fs::write(&a_file, b"seed")?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_file(&b_file, b"seed").await,
        "ledger seed did not converge"
    );
    reload_sync(&b_home).await?;
    let baseline = sync_status(&b_home, "shared").await?;

    reload_sync(&a_home).await?;
    reload_sync(&a_home).await?;
    let converged = sync_status(&b_home, "shared").await?;
    assert_eq!(converged.full_scans, baseline.full_scans);
    assert_eq!(
        converged.inbound_noop_transactions,
        baseline.inbound_noop_transactions + 2
    );
    assert_eq!(
        converged.inbound_guarded_transactions,
        baseline.inbound_guarded_transactions
    );

    std::fs::write(&a_file, b"remote mutation")?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_file(&b_file, b"remote mutation").await,
        "ledger mutation did not converge"
    );
    let mutated = sync_status(&b_home, "shared").await?;
    assert_eq!(mutated.full_scans, converged.full_scans + 2);
    assert_eq!(
        mutated.inbound_guarded_transactions,
        converged.inbound_guarded_transactions + 1
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}
