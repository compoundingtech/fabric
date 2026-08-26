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

/// Read both manifest digests for the `shared` entry.
async fn digest_of(home: &FabricHome) -> Result<String> {
    match send_control(home, ControlRequest::SyncStatus).await? {
        fabric::control::ControlResponse::SyncStatus { entries } => {
            let entry = entries
                .into_iter()
                .find(|e| e.name == "shared")
                .expect("the shared entry must exist");
            Ok(entry.digest)
        }
        other => panic!("expected SyncStatus, got {other:?}"),
    }
}

/// Two peers that hold the same files must report the same digest.
///
/// This is the contract the divergence instrument rests on. If two correctly
/// converged peers can report different digests, the instrument reports a
/// divergence that is not there, and a false alarm every pass is worse than no
/// instrument at all.
///
/// It covers a plain file, an executable file and a tombstone, because the
/// digest has to agree about all three kinds of entry and not only the easy one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn converged_peers_report_the_same_digest() -> Result<()> {
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

    // Positive control. The digest has to RESPOND to state before agreement
    // between two peers means anything: a constant would match every peer every
    // time and detect nothing. Capture the empty value now and compare later.
    let empty = digest_of(&a_home).await?;

    // A plain file, an executable file, and a delete, so the digest has to
    // agree about all three kinds of entry rather than only the easy one.
    std::fs::write(a_folder.join("plain.md"), b"hello")?;
    std::fs::write(a_folder.join("run.sh"), b"#!/bin/sh\necho hi\n")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            a_folder.join("run.sh"),
            std::fs::Permissions::from_mode(0o755),
        )?;
    }
    std::fs::write(a_folder.join("doomed.md"), b"delete me")?;
    reload_sync(&a_home).await?;

    assert!(
        wait_for_file(&b_folder.join("plain.md"), b"hello").await,
        "the file never reached B, so there is nothing to compare"
    );
    assert!(
        wait_for_file(&b_folder.join("run.sh"), b"#!/bin/sh\necho hi\n").await,
        "the executable never reached B"
    );

    std::fs::remove_file(a_folder.join("doomed.md"))?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_missing(&b_folder.join("doomed.md")).await,
        "the delete never reached B, so there is no tombstone to compare"
    );

    // Both peers hold the same three entries now. Let the passes settle.
    let mut last = (String::new(), String::new());
    for _ in 0..50 {
        let a_key = digest_of(&a_home).await?;
        let b_key = digest_of(&b_home).await?;
        last = (a_key.clone(), b_key.clone());
        if a_key == b_key && !a_key.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let (a_key, b_key) = last;

    assert!(!a_key.is_empty(), "A reported no digest at all");
    assert_ne!(
        a_key, empty,
        "the digest did not move when three entries arrived, so it is a \
         constant and it can detect nothing"
    );
    assert_eq!(
        a_key, b_key,
        "two converged peers disagree on the digest, so it cannot detect \
         divergence"
    );

    // The digest must also be STABLE. A value that changes every pass on a
    // quiet folder would make every cross-peer sample a coin toss.
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            digest_of(&a_home).await?,
            a_key,
            "the digest moved while nothing changed"
        );
    }
    node_b.shutdown().await?;
    node_a.shutdown().await?;
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

/// THE INCIDENT OF 2026-08-25, AS A TEST.
///
/// Removing a path from an entry's `include` leaves whatever the manifest
/// already recorded for it. Under a policy that does not propagate deletes that
/// is inert. Under one that does, the entry sees a path that is Present in its
/// manifest and absent from its scan, which is exactly what a local delete looks
/// like, so it tombstones the path and DELETES THE REAL FILE.
///
/// That is not hypothetical. It removed thirteen live plan files from three
/// machines. Every one was recoverable from git, which is the only reason it was
/// a bad night rather than a disaster.
///
/// A path that leaves an entry's include set must be FORGOTTEN, not deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_path_dropped_from_include_is_forgotten_not_deleted() -> Result<()> {
    let _guard = FOLDER_SYNC_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let a_folder = a_dir.path().join("shared");
    std::fs::create_dir_all(a_folder.join("plans"))?;
    std::fs::create_dir_all(a_folder.join("keep"))?;
    write_sync_with_include(a_dir.path(), &a_folder, "catalog", "**");

    let node_a = FabricNode::start(a_home.clone()).await?;

    let doomed = a_folder.join("plans/live-work.md");
    let kept = a_folder.join("keep/other.md");
    std::fs::write(&doomed, b"work someone is going to ship")?;
    std::fs::write(&kept, b"still included")?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_file(&doomed, b"work someone is going to ship").await,
        "POSITIVE CONTROL FAILED: the entry never recorded the file, so dropping \
         it from the include afterwards would prove nothing"
    );

    // Now narrow the include so plans/ is no longer this entry's business. The
    // file itself is untouched on disk and nobody asked for it to be deleted.
    write_sync_with_include(a_dir.path(), &a_folder, "catalog", "keep/**");
    reload_sync(&a_home).await?;

    assert_stays_missing(&a_folder.join("never-existed.md"), "sanity").await;
    node_a.shutdown().await?;

    assert!(
        doomed.exists(),
        "DROPPING A PATH FROM THE INCLUDE DELETED THE FILE. A path that leaves an \
         entry is not a file anybody deleted."
    );
    assert_eq!(
        std::fs::read(&doomed)?,
        b"work someone is going to ship",
        "the excluded file survived but its content changed"
    );
    assert!(kept.exists(), "an included file was lost too");
    Ok(())
}

/// The delta-replication goal, written as a property: **a small change must not
/// ship the whole manifest.**
///
/// This test is expected to FAIL until delta replication lands. It is ignored so
/// it does not block CI, and it must be run explicitly:
///
/// ```text
/// cargo test --test folder_sync a_small_change -- --ignored --nocapture
/// ```
///
/// It also measures a quiet window first, and that half already passes. Fabric
/// runs NO pass and ships NO bytes when nothing changes, so the idle case is
/// already free and needs no work. That measurement is kept here because it
/// rules out a whole class of fix: there are no idle passes to make cheaper, so
/// a cheap converged handshake would win nothing. The entire cost is in the
/// change case.
///
/// The positive control at the end is not optional. A quiet window that reports
/// zero proves nothing unless the same counters are shown to move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "expected to fail until delta replication lands; run explicitly"]
async fn a_small_change_must_not_ship_the_whole_manifest() -> Result<()> {
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

    for i in 0..300 {
        std::fs::write(a_folder.join(format!("f{i}.txt")), format!("content {i}\n"))?;
    }

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;
    reload_sync(&a_home).await?;

    assert!(
        wait_for_file(&b_folder.join("f299.txt"), b"content 299\n").await,
        "the peers never converged, so there is no quiet pass to measure"
    );
    // Let the tail of propagation finish before the window opens.
    tokio::time::sleep(Duration::from_secs(5)).await;

    async fn sample(home: &FabricHome) -> Result<(u64, u64, String)> {
        match send_control(home, ControlRequest::SyncStatus).await? {
            fabric::control::ControlResponse::SyncStatus { entries } => {
                let e = entries.into_iter().find(|e| e.name == "shared").unwrap();
                Ok((e.reconcile_wire_bytes, e.sync_passes, e.digest))
            }
            other => panic!("expected SyncStatus, got {other:?}"),
        }
    }

    let (b0, p0, d0) = sample(&a_home).await?;
    let window = 75;
    tokio::time::sleep(Duration::from_secs(window)).await;
    let (b1, p1, d1) = sample(&a_home).await?;

    let manifest = std::fs::metadata(
        a_dir
            .path()
            .join("sync")
            .join("shared")
            .join("manifest.json"),
    )
    .map(|m| m.len())
    .unwrap_or(0);

    println!("--- what a quiet pass ships ---");
    println!("window          {window} s, nothing changed");
    println!("digest before   {d0}");
    println!("digest after    {d1}");
    println!("digest moved    {}", d0 != d1);
    println!("manifest bytes  {manifest}");
    println!("passes          {}", p1 - p0);
    println!("wire bytes      {}", b1 - b0);
    if p1 > p0 {
        println!("per pass        {}", (b1 - b0) / (p1 - p0));
        println!(
            "manifests/pass  {:.2}",
            (b1 - b0) as f64 / (p1 - p0) as f64 / manifest.max(1) as f64
        );
    }

    // POSITIVE CONTROL. A zero above is only meaningful if these counters can
    // move at all. Change ONE file and prove they do. Without this, "0 passes"
    // is indistinguishable from a broken counter.
    std::fs::write(a_folder.join("f0.txt"), b"changed\n")?;
    let mut moved = (0u64, 0u64);
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let (b2, p2, d2) = sample(&a_home).await?;
        if p2 > p1 && b2 > b1 {
            moved = (b2 - b1, p2 - p1);
            println!("--- positive control: one file changed ---");
            println!("passes          {}", p2 - p1);
            println!("wire bytes      {}", b2 - b1);
            println!("per pass        {}", (b2 - b1) / (p2 - p1));
            println!(
                "manifests/pass  {:.2}",
                (b2 - b1) as f64 / (p2 - p1) as f64 / manifest.max(1) as f64
            );
            println!("digest moved    {}", d2 != d1);
            break;
        }
    }
    assert!(
        moved.1 > 0,
        "the counters never moved even after a real change, so the quiet-window \
         zero above proves nothing"
    );

    // The goal sentence of the delta plan: stop shipping the whole manifest.
    // One file changed by eight bytes. Anything at or above a whole manifest
    // per pass means the manifest is still the unit of transfer.
    //
    // One manifest per pass is the loosest bar that still means "not the whole
    // manifest". The real target is far lower and is not named here, because a
    // target chosen before the instrument runs everywhere is a target chosen to
    // be met.
    let per_pass = moved.0 / moved.1;
    assert!(
        per_pass < manifest,
        "a change of eight bytes shipped {per_pass} bytes, which is {:.2} whole \
         manifests of {manifest} bytes. The manifest is still the unit of \
         transfer",
        per_pass as f64 / manifest.max(1) as f64
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}
