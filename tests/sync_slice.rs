//! End-to-end `fabric sync` over real iroh on one machine — the local stand-in
//! for the Mac -> Hetzner catalog proof. Two daemons, each with a catalog sync
//! entry, mutually trusted: a file dropped on A propagates to B's folder over the
//! `fabric/sync` ALPN, and a catalog delete on B is restored (never propagates a
//! deletion).

use std::{path::Path, time::Duration};

use anyhow::Result;
use fabric::{
    config::{FabricHome, PeerBook},
    control::ControlRequest,
    daemon::{FabricNode, send_control},
};
use tempfile::TempDir;

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

fn write_catalog_sync(home_dir: &Path, folder: &Path) {
    let toml = format!(
        "[[sync]]\nname = \"catalog\"\nfolder = {folder:?}\npeers = \"*\"\npolicy = \"catalog\"\n"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_sync_propagates_new_file_and_restores_catalog_delete() -> Result<()> {
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());

    let a_catalog = a_dir.path().join("catalog");
    let b_catalog = b_dir.path().join("catalog");
    std::fs::create_dir_all(&a_catalog)?;
    std::fs::create_dir_all(&b_catalog)?;
    write_catalog_sync(a_dir.path(), &a_catalog);
    write_catalog_sync(b_dir.path(), &b_catalog);

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;

    // Mutual trust with address hints for deterministic same-machine dialing.
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    // Drop a host=hetz job into A's catalog and drive a sync (mirrors the CLI's
    // reload after `fabric sync add`).
    std::fs::write(a_catalog.join("job-hetz.toml"), b"host=hetz")?;
    send_control(&a_home, ControlRequest::SyncReload).await?;

    // B's daemon should watch + receive it fast.
    let b_job = b_catalog.join("job-hetz.toml");
    assert!(
        wait_for_file(&b_job, b"host=hetz").await,
        "job file did not propagate from A to B"
    );

    // A catalog delete on B must be restored, not propagated back to A.
    std::fs::remove_file(&b_job)?;
    send_control(&b_home, ControlRequest::SyncReload).await?;
    assert!(
        wait_for_file(&b_job, b"host=hetz").await,
        "catalog delete on B was not restored"
    );
    assert!(
        a_catalog.join("job-hetz.toml").exists(),
        "catalog delete on B must not remove the file on A"
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}
