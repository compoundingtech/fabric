//! The adversarial folder-sync matrix: one test per way a folder sync can go
//! wrong.
//!
//! A sync with no adversarial suite is a sync that works until the day it does
//! not. Each test here names a single failure and reproduces it. Where a case is
//! a known failure it says so in its own message, so nothing is silently
//! untested.
//!
//! These run real daemons over real iroh on one machine, because the failures
//! that matter are the ones a simulated transport cannot show: a peer that goes
//! away and comes back holding a stale copy.

use std::{path::Path, time::Duration};

use anyhow::Result;
use fabric::{
    config::{FabricHome, PeerBook},
    control::ControlRequest,
    daemon::{FabricNode, send_control},
};
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Real daemons bind real sockets; keep the cases serialized.
static FOLDER_SYNC_LOCK: Mutex<()> = Mutex::const_new(());

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

async fn wait_for_missing(path: &Path) -> bool {
    for _ in 0..50 {
        if !path.exists() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

/// A delete that "stuck" for one pass and came back on the next is the exact
/// shape of tonight's bug, so absence has to be watched, not sampled once.
async fn assert_stays_missing(path: &Path, label: &str) {
    for _ in 0..20 {
        assert!(
            !path.exists(),
            "{label}: {} came back from the dead",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
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

/// Delete on A. It must go on B, and it must not come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bus_delete_propagates_and_stays_deleted() -> Result<()> {
    let _guard = FOLDER_SYNC_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());

    let a_folder = a_dir.path().join("shared");
    let b_folder = b_dir.path().join("shared");
    std::fs::create_dir_all(&a_folder)?;
    std::fs::create_dir_all(&b_folder)?;
    write_sync(a_dir.path(), &a_folder, "bus");
    write_sync(b_dir.path(), &b_folder, "bus");

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    std::fs::write(a_folder.join("doomed.md"), b"delete me")?;
    reload_sync(&a_home).await?;
    let b_file = b_folder.join("doomed.md");
    assert!(
        wait_for_file(&b_file, b"delete me").await,
        "the file never reached B, so the delete case cannot be tested"
    );

    std::fs::remove_file(a_folder.join("doomed.md"))?;
    reload_sync(&a_home).await?;

    assert!(
        wait_for_missing(&b_file).await,
        "bus delete on A never reached B"
    );
    assert_stays_missing(&b_file, "delete propagated then reverted").await;
    assert_stays_missing(&a_folder.join("doomed.md"), "delete undone on the deleter").await;

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// THE CASE FROM TONIGHT. Delete while a peer is away. The peer returns holding
/// a stale present copy. The file must not come back.
///
/// This is the obvious way a delete breaks and bluey is away most of the time by
/// design, so it is not a corner case here, it is the normal case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bus_delete_while_peer_away_does_not_resurrect_on_return() -> Result<()> {
    let _guard = FOLDER_SYNC_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());

    let a_folder = a_dir.path().join("shared");
    let b_folder = b_dir.path().join("shared");
    std::fs::create_dir_all(&a_folder)?;
    std::fs::create_dir_all(&b_folder)?;
    write_sync(a_dir.path(), &a_folder, "bus");
    write_sync(b_dir.path(), &b_folder, "bus");

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    std::fs::write(a_folder.join("doomed.md"), b"delete me")?;
    reload_sync(&a_home).await?;
    let b_file = b_folder.join("doomed.md");
    assert!(
        wait_for_file(&b_file, b"delete me").await,
        "the file never reached B, so the away case cannot be tested"
    );

    // B goes away still believing the file is present.
    node_b.shutdown().await?;

    std::fs::remove_file(a_folder.join("doomed.md"))?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_missing(&a_folder.join("doomed.md")).await,
        "the file is not gone on A, so nothing else in this test means anything"
    );

    // B comes back holding its stale present copy.
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;
    reload_sync(&b_home).await?;

    assert!(
        wait_for_missing(&b_file).await,
        "the returning peer kept its stale copy instead of adopting the delete"
    );
    assert_stays_missing(
        &a_folder.join("doomed.md"),
        "the returning peer resurrected the file on the machine that deleted it",
    )
    .await;

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

fn write_sync_with_include(home_dir: &Path, folder: &Path, policy: &str, include: &str) {
    let toml = format!(
        "[[sync]]\nname = \"shared\"\nfolder = {folder:?}\npeers = \"*\"\npolicy = {policy:?}\ninclude = [{include:?}]\n"
    );
    std::fs::write(home_dir.join("syncs.toml"), toml).unwrap();
}
/// An excluded path is not fabric's business, so it must never reach a peer.
///
/// This began as a migration question: whether moving a path out of an entry's
/// `include` was enough to stop it syncing. The migration was superseded by
/// making a delete stick everywhere, but the invariant underneath it still
/// holds and is worth pinning, because `adopt` and `materialize_tracked` walk
/// the manifest and consult no globs at all. Only `scan_folder` filters.
///
/// CONTROL: an INCLUDED file must arrive on B. Without that the fixture cannot
/// tell "correctly excluded" apart from "sync is simply not working", and a
/// test that cannot tell those apart proves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_excluded_path_never_reaches_a_peer() -> Result<()> {
    let _guard = FOLDER_SYNC_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());

    let a_folder = a_dir.path().join("shared");
    let b_folder = b_dir.path().join("shared");
    std::fs::create_dir_all(a_folder.join("plans"))?;
    std::fs::create_dir_all(a_folder.join("notes"))?;
    std::fs::create_dir_all(&b_folder)?;
    write_sync_with_include(a_dir.path(), &a_folder, "catalog", "plans/**");
    write_sync_with_include(b_dir.path(), &b_folder, "catalog", "plans/**");

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    std::fs::write(a_folder.join("plans/included.md"), b"in the entry")?;
    std::fs::write(a_folder.join("notes/excluded.md"), b"not in the entry")?;
    reload_sync(&a_home).await?;

    // CONTROL. The included file crosses, so the fixture demonstrably syncs.
    assert!(
        wait_for_file(&b_folder.join("plans/included.md"), b"in the entry").await,
        "POSITIVE CONTROL FAILED: the included file never reached B, so this \
         fixture cannot tell exclusion from a broken sync"
    );

    // TEST. The excluded file sat beside it the whole time and must not cross.
    assert_stays_missing(
        &b_folder.join("notes/excluded.md"),
        "a path outside the entry's include globs crossed the wire",
    )
    .await;
    // And it must be left alone where it lives, not deleted as an unexpected file.
    assert_eq!(
        std::fs::read(a_folder.join("notes/excluded.md"))?,
        b"not in the entry",
        "fabric touched a file that is not in the entry"
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// THE CASE THAT DECIDES IT, under CATALOG policy.
///
/// Catalog is the policy the real catalog runs, and it is where the reported
/// bug happened. It used to refuse to originate a delete AND advance a
/// tombstoned path back to a higher Present whenever this node still held the
/// bytes. That second half is what let a single returning machine undo a delete
/// for everybody.
///
/// A returning peer is not a corner case. bluey is away most of the time by
/// design, and a machine that was asleep for a fortnight comes back holding
/// exactly this stale state.
///
/// The reported reproduction had one more detail worth naming: the file came
/// back carrying its ORIGINAL mtime, so by `stat` it was indistinguishable from
/// a file that was never deleted. That is why this went unnoticed, and it is why
/// this test checks for the path's absence rather than for a fresh timestamp.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_delete_while_peer_away_does_not_resurrect_on_return() -> Result<()> {
    let _guard = FOLDER_SYNC_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());

    let a_folder = a_dir.path().join("shared");
    let b_folder = b_dir.path().join("shared");
    std::fs::create_dir_all(&a_folder)?;
    std::fs::create_dir_all(&b_folder)?;
    write_sync(a_dir.path(), &a_folder, "catalog");
    write_sync(b_dir.path(), &b_folder, "catalog");

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    std::fs::write(a_folder.join("retired.md"), b"decommission me")?;
    reload_sync(&a_home).await?;
    let b_file = b_folder.join("retired.md");
    assert!(
        wait_for_file(&b_file, b"decommission me").await,
        "the file never reached B, so the away case cannot be tested"
    );

    // B goes away holding the file, exactly as a travelling machine does.
    node_b.shutdown().await?;

    std::fs::remove_file(a_folder.join("retired.md"))?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_missing(&a_folder.join("retired.md")).await,
        "the catalog delete did not even hold on the machine that made it"
    );

    // B returns still holding the bytes. This is where it used to resurrect.
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;
    reload_sync(&b_home).await?;

    assert!(
        wait_for_missing(&b_file).await,
        "the returning peer kept its stale copy instead of adopting the delete"
    );
    assert_stays_missing(
        &a_folder.join("retired.md"),
        "the returning peer resurrected the file on the machine that deleted it",
    )
    .await;

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}
