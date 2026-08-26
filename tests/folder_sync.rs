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
//!
//! # READ THIS BEFORE WRITING THE NEXT TEST HERE
//!
//! **A test that runs with a fallback underneath it must assert the fallback did
//! not fire.** Otherwise it passes for the wrong reason and stays green while the
//! thing it guards is broken.
//!
//! This is not hypothetical. `a_delete_sticks_starting_from_either_peer` was
//! written without that assertion. Making `Manifest::subset` silently drop every
//! tombstone did NOT fail it: the incomplete delta produced a digest mismatch,
//! the cursor reset, and the next pass carried the delete inside a full
//! manifest. The delete arrived, the test passed, and a delta that never carried
//! a tombstone would have shipped while every delete in the fleet quietly cost a
//! whole manifest.
//!
//! Sync has two fallbacks that will do this to you. `delta_fallbacks` counts a
//! reconcile that found a payload incomplete and sent full state instead. The
//! periodic safety scan re-reads the tree when a watcher event is missed. Both
//! exist to make failures survivable, and both make a broken mechanism look
//! healthy from the outside.
//!
//! So assert the mechanism, not only the outcome. "The file is gone on B" is an
//! outcome. "The file is gone on B AND nothing fell back" is the mechanism.

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
/// Reconciles that found a payload incomplete and fell back to full state.
async fn fallbacks_of(home: &FabricHome) -> Result<u64> {
    match send_control(home, ControlRequest::SyncStatus).await? {
        fabric::control::ControlResponse::SyncStatus { entries } => Ok(entries
            .into_iter()
            .find(|e| e.name == "shared")
            .map(|e| e.delta_fallbacks)
            .unwrap_or(0)),
        other => panic!("expected SyncStatus, got {other:?}"),
    }
}

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

/// Several include globs, as a real list rather than one comma-joined glob.
fn write_sync_with_includes(home_dir: &Path, folder: &Path, policy: &str, include: &[&str]) {
    let globs = include
        .iter()
        .map(|g| format!("{g:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    let toml = format!(
        "[[sync]]\nname = \"shared\"\nfolder = {folder:?}\npeers = \"*\"\npolicy = {policy:?}\ninclude = [{globs}]\n"
    );
    std::fs::write(home_dir.join("syncs.toml"), toml).unwrap();
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

/// The assumption the merge-key digest rests on, made executable.
///
/// `Manifest::digest` covers `order_key` and deliberately omits `size`,
/// `executable`, `mtime_secs` and `mtime_nanos`. That is only sound because a
/// replica stores the origin's `FileMeta` VERBATIM: the scanner compares content
/// hashes rather than metadata, so a materialized copy never rewrites those
/// fields with its own.
///
/// If that ever stops being true, two correctly converged peers start holding
/// different metadata for the same path, and the digest cannot see it. Silent is
/// exactly the failure the whole instrument exists to prevent, so the assumption
/// is written here as a test rather than left in a design note.
///
/// It compares the manifests on disk field by field, not through the digest,
/// because a digest that ignores a field cannot notice that field changing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_stores_the_origin_metadata_verbatim() -> Result<()> {
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
        wait_for_file(&b_folder.join("run.sh"), b"#!/bin/sh\necho hi\n").await,
        "nothing reached B, so there is no replica to compare"
    );
    std::fs::remove_file(a_folder.join("doomed.md"))?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_missing(&b_folder.join("doomed.md")).await,
        "the delete never reached B, so there is no tombstone to compare"
    );

    // From `state.json`, which is the authoritative file. The `manifest.json`
    // projection is no longer written; see `write_state`.
    let read_manifest = |dir: &Path| -> Option<serde_json::Value> {
        let raw = std::fs::read(dir.join("sync").join("shared").join("state.json")).ok()?;
        let state: serde_json::Value = serde_json::from_slice(&raw).ok()?;
        state.get("manifest").cloned()
    };

    // Wait for both sides to hold all three entries before comparing, or the
    // comparison races propagation and fails for the wrong reason.
    let mut pair = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (Some(a), Some(b)) = (read_manifest(a_dir.path()), read_manifest(b_dir.path())) else {
            continue;
        };
        let count = |v: &serde_json::Value| {
            v.get("entries")
                .and_then(|e| e.as_object())
                .map(|o| o.len())
                .unwrap_or(0)
        };
        if count(&a) == 3 && count(&b) == 3 {
            pair = Some((a, b));
            break;
        }
    }
    let (a_manifest, b_manifest) = pair.expect("both peers never held all three entries");

    let a_entries = a_manifest["entries"].as_object().unwrap();
    let b_entries = b_manifest["entries"].as_object().unwrap();

    // Prove the comparison is not vacuous. If the on-disk shape ever stops
    // carrying these fields, an equality check over the entries would still
    // pass while guarding nothing at all.
    let present = a_entries
        .values()
        .find(|e| e.get("kind").and_then(|k| k.as_str()) == Some("present"))
        .expect("no present entry on disk to compare");
    for field in ["size", "executable", "mtime_secs", "mtime_nanos", "hash"] {
        assert!(
            present.get(field).is_some(),
            "the manifest on disk has no `{field}` field, so this test guards \
             nothing. The entry shape changed: {present}"
        );
    }

    for (path, a_entry) in a_entries {
        let b_entry = b_entries
            .get(path)
            .unwrap_or_else(|| panic!("B is missing {path}"));
        assert_eq!(
            a_entry, b_entry,
            "the replica rewrote metadata for {path}. The merge-key digest omits \
             size, executable and mtime, so it CANNOT see this difference. Either \
             restore verbatim storage or widen `Manifest::digest`"
        );
    }
    assert_eq!(
        a_entries.len(),
        b_entries.len(),
        "the two peers hold a different number of entries"
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// The delta-replication goal, written as a property: **a small change must not
/// ship the whole manifest.**
///
/// This failed when it was written, which was the point of writing it then. An
/// eight byte change shipped 218,565 bytes, or 2.00 whole manifests. It now
/// ships about 776 bytes against the same fixture.
///
/// It measures a quiet window first, and that half passed from the beginning.
/// Fabric runs NO pass and ships NO bytes when nothing changes, so the idle case
/// was already free. That measurement is kept because it rules out a whole class
/// of fix: there are no idle passes to make cheaper, so a cheap converged
/// handshake would have won nothing. It also explains why the serving side needs
/// the landing digest, since a pass where the digests already agree on arrival
/// never happens.
///
/// The positive control at the end is not optional. A quiet window that reports
/// zero proves nothing unless the same counters are shown to move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    let window = 45;
    tokio::time::sleep(Duration::from_secs(window)).await;
    let (b1, p1, d1) = sample(&a_home).await?;

    // The size of the MANIFEST, which is what a pass used to ship, taken from
    // inside the authoritative state file rather than from the projection that
    // is no longer written.
    let manifest = std::fs::read(a_dir.path().join("sync").join("shared").join("state.json"))
        .ok()
        .and_then(|raw| serde_json::from_slice::<serde_json::Value>(&raw).ok())
        .and_then(|state| state.get("manifest").cloned())
        .map(|m| serde_json::to_vec(&m).map(|v| v.len() as u64).unwrap_or(0))
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

/// A path that arrives OUTSIDE the receiver's include must not come back as a
/// delete.
///
/// This is the incident of 2026-08-25 in the other direction. `plans/**` was
/// taken out of an include, which left about twenty paths recorded but
/// unscannable, and the next pass read "in my records, absent from my scan" as a
/// local delete. Thirteen live files disappeared from three machines.
///
/// Widening an include across a fleet cannot be atomic: one machine gets it
/// first and ships entries the others cannot yet scan. This test models exactly
/// that. A sends a path B does not select, and the file must survive on A.
///
/// The guard that makes it survive is in `scan_into_node_observed`, which skips
/// the local-remove loop for any path outside the include.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_path_outside_the_receivers_include_is_not_deleted() -> Result<()> {
    let _guard = FOLDER_SYNC_LOCK.lock().await;
    let a_dir = TempDir::new()?;
    let b_dir = TempDir::new()?;
    let a_home = FabricHome::new(a_dir.path());
    let b_home = FabricHome::new(b_dir.path());
    let a_folder = a_dir.path().join("shared");
    let b_folder = b_dir.path().join("shared");
    std::fs::create_dir_all(a_folder.join("docs"))?;
    std::fs::create_dir_all(a_folder.join("keep"))?;
    std::fs::create_dir_all(b_folder.join("keep"))?;

    // A selects docs/. B does NOT, exactly like a fleet mid-rollout. Bus policy,
    // because that is the policy under which a delete actually propagates.
    write_sync_with_includes(a_dir.path(), &a_folder, "bus", &["docs/**", "keep/**"]);
    write_sync_with_includes(b_dir.path(), &b_folder, "bus", &["keep/**"]);

    let node_a = FabricNode::start(a_home.clone()).await?;
    let node_b = FabricNode::start(b_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;
    trust_peer(&b_home, &node_b, node_a.id(), "node-a", node_a.addr()).await?;

    let doc = a_folder.join("docs/pairing-api.md");
    std::fs::write(&doc, b"the shared document")?;
    // A path both sides select, to prove the pair really is syncing. Without it
    // a silent transport failure would look like a passing test.
    let shared = a_folder.join("keep/shared.md");
    std::fs::write(&shared, b"both sides want this")?;
    reload_sync(&a_home).await?;

    assert!(
        wait_for_file(&b_folder.join("keep/shared.md"), b"both sides want this").await,
        "the two peers never synced at all, so this proves nothing about includes"
    );

    // Now the real question. B holds an entry it cannot scan. Give it many
    // passes to do the wrong thing.
    // What B does with the file is recorded rather than demanded. `adopt` and
    // `materialize_tracked` walk the manifest and consult no globs at all; only
    // `scan_folder` filters. So B is expected to WRITE a path it does not
    // select, and simply never scan it.
    assert!(
        b_folder.join("docs/pairing-api.md").exists(),
        "B did not materialize a path outside its include. That is not a fault, \
         but it changes the answer to \"which machine may take a widened include \
         first\", so it must not change silently"
    );
    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            doc.exists(),
            "the document was deleted on A after B received a path it does not \
             select. This is the 2026-08-25 incident, reproduced"
        );
    }
    assert_eq!(
        std::fs::read(&doc)?,
        b"the shared document",
        "the document survived but its content changed"
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// A delete must stick in BOTH directions, not just outward from the machine
/// that happened to be tested.
///
/// The delta path is asymmetric: the initiating side sends first, and the
/// serving side answers with a payload chosen by a cursor it maintains
/// differently. A delete that propagates from the initiator therefore proves
/// nothing about one that starts on the responder. Both are exercised here in
/// one run, on the same pair, so neither direction can pass by accident of who
/// dialled whom.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_sticks_starting_from_either_peer() -> Result<()> {
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

    // A delete must travel IN the delta, not be rescued by the fallback.
    //
    // Without this the test cannot tell the two apart. A delta that silently
    // dropped tombstones would still pass: the digests would disagree, the
    // cursor would reset, and the next pass would carry the tombstone in a full
    // manifest. The delete sticks either way, and every delete would quietly
    // cost a whole manifest. Mutation-tested: dropping tombstones from
    // `Manifest::subset` passes without this and fails with it.
    let fallbacks_before = (fallbacks_of(&a_home).await?, fallbacks_of(&b_home).await?);

    // Direction one: born on A, deleted on A.
    std::fs::write(a_folder.join("from-a.md"), b"a")?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_file(&b_folder.join("from-a.md"), b"a").await,
        "A's file never reached B"
    );
    std::fs::remove_file(a_folder.join("from-a.md"))?;
    reload_sync(&a_home).await?;
    assert!(
        wait_for_missing(&b_folder.join("from-a.md")).await,
        "a delete on A never reached B"
    );

    // Direction two: born on B, deleted on B, over the same converged pair.
    std::fs::write(b_folder.join("from-b.md"), b"b")?;
    reload_sync(&b_home).await?;
    assert!(
        wait_for_file(&a_folder.join("from-b.md"), b"b").await,
        "B's file never reached A"
    );
    std::fs::remove_file(b_folder.join("from-b.md"))?;
    reload_sync(&b_home).await?;
    assert!(
        wait_for_missing(&a_folder.join("from-b.md")).await,
        "a delete on B never reached A. The delta path is asymmetric and this is \
         the direction that gets missed"
    );

    // Neither may come back. A delete that held for one pass and returned on the
    // next is the shape of the original bug.
    assert_stays_missing(&b_folder.join("from-a.md"), "A-born delete").await;
    assert_stays_missing(&a_folder.join("from-b.md"), "B-born delete").await;
    assert_stays_missing(&a_folder.join("from-a.md"), "delete undone on deleter").await;
    assert_stays_missing(&b_folder.join("from-b.md"), "delete undone on deleter").await;

    let fallbacks_after = (fallbacks_of(&a_home).await?, fallbacks_of(&b_home).await?);
    assert_eq!(
        fallbacks_after, fallbacks_before,
        "a delete forced a fallback to full state, so tombstones are not \
         travelling in the delta and every delete costs a whole manifest"
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// Three nodes in a LINE, so a change must travel THROUGH a peer that is neither
/// its origin nor its destination.
///
/// Two nodes cannot show this. On a pair, every change reaches its destination
/// directly, so a peer that adopts correctly but forwards nothing looks perfect.
/// The delta path makes that failure reachable: a node forwards by recording
/// what it adopted into its own buffer, and if it recorded nothing it would
/// converge with its source and silently starve everyone downstream.
///
/// A trusts only B. C trusts only B. A and C never speak, so B is the only route
/// and every assertion about C is an assertion about forwarding.
///
/// The delete travels in both directions across the line, because the delta path
/// is asymmetric and the middle peer is an initiator to one side and a responder
/// to the other in the same pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_change_and_a_delete_cross_a_peer_that_is_only_a_relay() -> Result<()> {
    let _guard = FOLDER_SYNC_LOCK.lock().await;
    let dirs = [TempDir::new()?, TempDir::new()?, TempDir::new()?];
    let homes: Vec<FabricHome> = dirs.iter().map(|d| FabricHome::new(d.path())).collect();
    let folders: Vec<_> = dirs.iter().map(|d| d.path().join("shared")).collect();
    for (dir, folder) in dirs.iter().zip(&folders) {
        std::fs::create_dir_all(folder)?;
        write_sync(dir.path(), folder, "bus");
    }

    let a = FabricNode::start(homes[0].clone()).await?;
    let b = FabricNode::start(homes[1].clone()).await?;
    let c = FabricNode::start(homes[2].clone()).await?;

    // A — B — C. A and C are never introduced.
    trust_peer(&homes[0], &a, b.id(), "b", b.addr()).await?;
    trust_peer(&homes[1], &b, a.id(), "a", a.addr()).await?;
    trust_peer(&homes[1], &b, c.id(), "c", c.addr()).await?;
    trust_peer(&homes[2], &c, b.id(), "b", b.addr()).await?;

    let fallbacks_before = (
        fallbacks_of(&homes[0]).await?,
        fallbacks_of(&homes[1]).await?,
        fallbacks_of(&homes[2]).await?,
    );

    // Outward: born on A, must reach C through B.
    std::fs::write(folders[0].join("relayed.md"), b"through the middle")?;
    reload_sync(&homes[0]).await?;
    assert!(
        wait_for_file(&folders[2].join("relayed.md"), b"through the middle").await,
        "A's file never reached C, so B adopted it and forwarded nothing"
    );

    // Backward: born on C, must reach A through B.
    std::fs::write(folders[2].join("returned.md"), b"back the other way")?;
    reload_sync(&homes[2]).await?;
    assert!(
        wait_for_file(&folders[0].join("returned.md"), b"back the other way").await,
        "C's file never reached A through B"
    );

    // A delete has to cross the relay too, in both directions.
    std::fs::remove_file(folders[0].join("relayed.md"))?;
    reload_sync(&homes[0]).await?;
    assert!(
        wait_for_missing(&folders[2].join("relayed.md")).await,
        "a delete on A never crossed B to reach C. A tombstone is a fragment like \
         any other and must forward like one"
    );
    std::fs::remove_file(folders[2].join("returned.md"))?;
    reload_sync(&homes[2]).await?;
    assert!(
        wait_for_missing(&folders[0].join("returned.md")).await,
        "a delete on C never crossed B to reach A"
    );

    // Neither may come back anywhere on the line, the relay included.
    for (i, folder) in folders.iter().enumerate() {
        assert_stays_missing(&folder.join("relayed.md"), &format!("node {i}")).await;
        assert!(
            !folder.join("returned.md").exists(),
            "returned.md came back on node {i}"
        );
    }

    // And none of it may have been rescued by a fallback. Without this the test
    // passes when forwarding is broken, because a digest mismatch resets the
    // cursor and the next full manifest carries everything anyway. See the note
    // at the top of this file.
    let fallbacks_after = (
        fallbacks_of(&homes[0]).await?,
        fallbacks_of(&homes[1]).await?,
        fallbacks_of(&homes[2]).await?,
    );
    assert_eq!(
        fallbacks_after, fallbacks_before,
        "relaying forced a fallback to full state, so changes are not forwarding \
         in the delta and the middle peer costs a whole manifest per change"
    );

    c.shutdown().await?;
    b.shutdown().await?;
    a.shutdown().await?;
    Ok(())
}

/// CASE THREE. A node that loses its unflushed log tail must converge, not sit
/// quietly behind.
///
/// The log is deliberately not fsynced per record, because it is an INDEX and an
/// index can be re-derived. The claim has two halves and this tests both on one
/// node: for a LOCAL change the filesystem holds the truth, and for an ADOPTED
/// change the peer does. So the whole log is taken away, which is worse than any
/// real crash, and both kinds of change must come back.
///
/// It does NOT delete the files themselves. An earlier version did, trying to
/// force the peer to be the only source, and it was measuring the wrong thing:
/// a file missing from disk is a user's delete, and fabric propagated it
/// correctly. The two peers agreed perfectly on the tombstones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_loses_its_log_converges_instead_of_staying_behind() -> Result<()> {
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

    // Converge on a body of files, so a snapshot really exists underneath.
    for i in 0..40 {
        std::fs::write(a_folder.join(format!("seed{i:02}.md")), format!("seed {i}"))?;
    }
    reload_sync(&a_home).await?;
    assert!(
        wait_for_file(&b_folder.join("seed39.md"), b"seed 39").await,
        "the peers never converged, so there is no 'behind' to come back from"
    );

    // One change of each kind, both landing in A's log rather than its snapshot.
    std::fs::write(a_folder.join("born-on-a.md"), b"local change")?;
    std::fs::write(b_folder.join("born-on-b.md"), b"adopted change")?;
    reload_sync(&a_home).await?;
    reload_sync(&b_home).await?;
    assert!(
        wait_for_file(&b_folder.join("born-on-a.md"), b"local change").await,
        "A's local change never reached B"
    );
    assert!(
        wait_for_file(&a_folder.join("born-on-b.md"), b"adopted change").await,
        "B's change never reached A"
    );

    // Kill A and take its WHOLE log, which is worse than losing a tail.
    node_a.shutdown().await?;
    let log = a_dir.path().join("sync").join("shared").join("log.jsonl");
    // Control. If there was no log, nothing was taken away and everything below
    // passes for the wrong reason.
    assert!(
        log.exists(),
        "A had no log to lose, so this test removes nothing and proves nothing"
    );
    let lost = std::fs::read(&log)?;
    assert!(!lost.is_empty(), "A's log was empty");
    std::fs::remove_file(&log)?;

    let node_a = FabricNode::start(a_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;

    // Neither change may be lost, and neither may be resurrected as a delete.
    assert!(
        wait_for_file(&a_folder.join("born-on-a.md"), b"local change").await,
        "A lost its own local change with the log. The filesystem was supposed \
         to be the durable record for that half"
    );
    assert!(
        wait_for_file(&a_folder.join("born-on-b.md"), b"adopted change").await,
        "A lost a change it had adopted. The peer was supposed to be the durable \
         record for that half"
    );
    assert!(
        wait_for_file(&b_folder.join("born-on-a.md"), b"local change").await,
        "A's lost log took the file off B as well, which is data loss, not a \
         catch-up"
    );

    // And they must actually agree, not merely both hold the files.
    let mut agreed = (String::new(), String::new());
    for _ in 0..60 {
        let a_digest = digest_of(&a_home).await?;
        let b_digest = digest_of(&b_home).await?;
        agreed = (a_digest.clone(), b_digest.clone());
        if a_digest == b_digest && !a_digest.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(
        agreed.0, agreed.1,
        "the two peers never reached the same lattice point after A lost its log"
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}

/// A REPAIR TAKES TWO PASSES AND ONLY ONE IS SCHEDULED.
///
/// This is the state that needs the second one. A is rolled back so it is
/// missing an entry AND the file, consistently, so its own scan sees nothing
/// wrong and records no delete. B still holds a cursor saying A has everything,
/// so B sends an empty delta. Nothing on either side has changed, so nothing
/// triggers another pass.
///
/// The first pass discovers the disagreement and both sides forget their
/// cursors, which is the right repair and useless until somebody reconciles
/// again. Without a follow-up, A sits quietly wrong until the five minute safety
/// net, and quiet is the failure this design exists to prevent.
///
/// The rollback is artificial. The state it produces is not: it is a crash
/// between adopting an entry and materialising its content.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_holding_a_stale_cursor_still_repairs_a_node_that_fell_behind() -> Result<()> {
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

    for i in 0..40 {
        std::fs::write(a_folder.join(format!("seed{i:02}.md")), format!("seed {i}"))?;
    }
    reload_sync(&a_home).await?;
    assert!(
        wait_for_file(&b_folder.join("seed39.md"), b"seed 39").await,
        "the peers never converged"
    );
    // Let A snapshot this state, which is the point it will be rolled back to.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let a_state = a_dir.path().join("sync").join("shared").join("state.json");
    let rollback = std::fs::read(&a_state)?;
    assert!(!rollback.is_empty(), "A never wrote a snapshot to roll back to");

    // Now something B owns, which A adopts AFTER that snapshot.
    std::fs::write(b_folder.join("only-on-b.md"), b"B owns this")?;
    reload_sync(&b_home).await?;
    assert!(
        wait_for_file(&a_folder.join("only-on-b.md"), b"B owns this").await,
        "B's file never reached A, so there is nothing for A to lose"
    );
    // B must have acknowledged A, or its cursor is not stale and the case does
    // not arise.
    let mut acknowledged = false;
    for _ in 0..40 {
        if digest_of(&a_home).await? == digest_of(&b_home).await? {
            acknowledged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(acknowledged, "the peers never agreed, so B's cursor is not stale");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Roll A back to before it ever saw the file, consistently: old snapshot, no
    // log, no file. A's own scan will find nothing missing and record no delete.
    node_a.shutdown().await?;
    std::fs::write(&a_state, &rollback)?;
    let log = a_dir.path().join("sync").join("shared").join("log.jsonl");
    let _ = std::fs::remove_file(&log);
    std::fs::remove_file(a_folder.join("only-on-b.md"))?;

    let node_a = FabricNode::start(a_home.clone()).await?;
    trust_peer(&a_home, &node_a, node_b.id(), "node-b", node_b.addr()).await?;

    assert!(
        wait_for_file(&a_folder.join("only-on-b.md"), b"B owns this").await,
        "A fell behind and B's stale cursor kept it there. The repair was applied \
         to the cursors and nothing ever used it"
    );
    assert!(
        b_folder.join("only-on-b.md").exists(),
        "B's own file went missing while repairing A"
    );

    node_b.shutdown().await?;
    node_a.shutdown().await?;
    Ok(())
}
