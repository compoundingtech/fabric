//! The `fabric sync` commands that belong to the companion: listing entries,
//! and staging and publishing changes to synced files.
//!
//! `fabric sync <command>` hands these to this binary with the same arguments,
//! and runs `add`, `rm` and `reload` itself; this binary hands those three
//! back the same way. Output is what `fabric sync` has always printed, because
//! scripts and gates read its columns.

use std::{collections::BTreeMap, path::PathBuf, process::Command};

use anyhow::{Context, Result, bail};
use fabric_config::{
    FabricHome,
    daemon_control::{self, Request as ControlRequest, Response as ControlResponse},
    sync::{
        SyncBook, SyncEntry, SyncPeers,
        cli::SyncCommands,
        staging,
        status::{SyncEntryStatus, SyncPublishFile, SyncRuntimeStatus},
    },
};

pub async fn run(home: &FabricHome, command: SyncCommands) -> Result<()> {
    match command {
        SyncCommands::Stage {
            target,
            from,
            entry,
        } => {
            let book = SyncBook::load(home)?;
            let target = absolutize(&target)?;
            let from = from.as_deref().map(absolutize).transpose()?;
            let staged = staging::stage(home, &book, &target, from.as_deref(), entry.as_deref())?;
            println!("staged\t{}", staged.rel);
            println!("entry\t{}", staged.entry);
            println!("edit\t{}", staged.staged_path.display());
            println!("target\t{}", staged.target_path.display());
            println!("base\t{}", staged.base.as_deref().unwrap_or("new"));
        }
        SyncCommands::Staged { entry, json } => {
            let book = SyncBook::load(home)?;
            let files = staging::list(home, &book, entry.as_deref())?;
            if json {
                println!("{}", serde_json::to_string_pretty(&files)?);
                return Ok(());
            }
            if files.is_empty() {
                println!("nothing staged");
                return Ok(());
            }
            for file in files {
                println!(
                    "{}\t{}\t{}\t{}B\thash={}\tbase={}\tedit={}\ttarget={}",
                    file.entry,
                    file.rel,
                    file.state(),
                    file.bytes,
                    &file.hash[..12],
                    file.base.as_deref().map(|hex| &hex[..12]).unwrap_or("new"),
                    file.staged_path.display(),
                    file.target_path.display()
                );
            }
        }
        SyncCommands::Publish {
            targets,
            all,
            entry,
            force,
        } => {
            let book = SyncBook::load(home)?;
            let groups = group_targets_by_entry(&book, &targets, all, entry.as_deref())?;
            for (name, rels) in groups {
                let (configured, files) = staging::read_for_publish(home, &book, &name, &rels)?;
                let rels: Vec<String> = files.iter().map(|file| file.rel.clone()).collect();
                let request = ControlRequest::SyncPublish {
                    name: name.clone(),
                    files: files
                        .iter()
                        .map(|file| SyncPublishFile {
                            rel: file.rel.clone(),
                            bytes: file.bytes.clone(),
                            executable: file.executable,
                            base: file.base.map(|hash| hash.to_hex()),
                        })
                        .collect(),
                    force,
                };
                match daemon_control::send(home, request).await {
                    Ok(ControlResponse::SyncPublished { files }) => {
                        for file in files {
                            println!(
                                "published\t{name}\t{}\tversion={}\thash={}",
                                file.rel,
                                file.version,
                                &file.hash[..12]
                            );
                        }
                    }
                    Ok(response) => bail!("unexpected daemon response: {response:?}"),
                    // Only when no daemon can take the request: it is down, or
                    // it predates SyncPublish. A refusal is a decision and is
                    // never retried around.
                    Err(error) if daemon_cannot_publish(&error) => {
                        let placed = staging::publish_locally(&configured, &files, force)?;
                        for (rel, hash) in placed {
                            println!(
                                "placed\t{name}\t{rel}\thash={}\tvia=folder",
                                &hash.to_hex()[..12]
                            );
                        }
                        println!(
                            "note\tno daemon took the publish ({}); the next scan records the \
                             files, and a set can cross to a peer in more than one reconcile",
                            first_line(&format!("{error:#}"))
                        );
                    }
                    Err(error) => return Err(error),
                }
                staging::forget(home, &name, &rels)?;
            }
        }
        SyncCommands::Discard { targets, entry } => {
            let book = SyncBook::load(home)?;
            let groups = group_targets_by_entry(&book, &targets, false, entry.as_deref())?;
            for (name, rels) in groups {
                staging::forget(home, &name, &rels)?;
                for rel in rels {
                    println!("discarded\t{name}\t{rel}");
                }
            }
        }
        SyncCommands::Ls { json } => {
            let (entries, runtime) =
                match daemon_control::send(home, ControlRequest::SyncStatus).await {
                    Ok(ControlResponse::SyncStatus { entries, runtime }) => (entries, runtime),
                    Ok(response) => bail!("unexpected daemon response: {response:?}"),
                    Err(_) => {
                        let book = SyncBook::load(home)?;
                        let entries = book.entries().iter().map(configured_sync_status).collect();
                        (
                            entries,
                            SyncRuntimeStatus {
                                owner: "unavailable".to_string(),
                                companion: "unknown".to_string(),
                            },
                        )
                    }
                };
            // Staged files are a local fact the daemon does not know: they live
            // in the fabric home, outside every folder. Count them here so a
            // staged change is never forgotten because nobody ran `staged`.
            let staged_counts = staged_counts(home);
            if json {
                let entries: Vec<_> = entries
                    .iter()
                    .map(|entry| {
                        SyncLsJsonEntry::from(entry)
                            .with_runtime(&runtime)
                            .with_staged(staged_counts.get(&entry.name).copied().unwrap_or(0))
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&entries)?);
                return Ok(());
            }
            println!(
                "runtime\towner={}\tcompanion={}",
                runtime.owner, runtime.companion
            );
            if entries.is_empty() {
                println!("no sync entries");
            }
            for entry in entries {
                let staged = staged_counts.get(&entry.name).copied().unwrap_or(0);
                if runtime.owner == "unavailable" {
                    println!(
                        "{}\t{}\t{}\tpeers={}\truntime=unavailable\tdrift=unknown\tstopped={}\tstaged={staged}",
                        entry.name,
                        entry.folder,
                        entry.policy,
                        entry.peers,
                        stopped_token(&entry),
                    );
                    continue;
                }
                let present = logical_present(&entry);
                if entry.missing == 0
                    && entry.unexpected == 0
                    && entry.mismatched == 0
                    && entry.scan_issues.is_empty()
                {
                    println!(
                        "{}\t{}\t{}\tpeers={}\tpresent={present}\ttombstones={}\tobserved={}\tdrift=clean\tscan_issues=none\tstopped={}\taway={}\tsync_passes={}\tfull_scans={}\tinbound_noop_transactions={}\tinbound_guarded_transactions={}\tscan_ms={}\tmaterialize_ms={}\tpersist_ms={}\treconcile_ms={}\treconcile_wire_bytes={}\treconcile_failures={}\tsweep={}\tdelta_fallbacks={}\tfull_payload_sends={}\tcontent_bytes={}\tdigest={}\tstaged={staged}",
                        entry.name,
                        entry.folder,
                        entry.policy,
                        entry.peers,
                        entry.tombstones,
                        entry.observed,
                        stopped_token(&entry),
                        away_token(&entry),
                        entry.sync_passes,
                        entry.full_scans,
                        entry.inbound_noop_transactions,
                        entry.inbound_guarded_transactions,
                        entry.scan_micros / 1000,
                        entry.materialize_micros / 1000,
                        entry.persist_micros / 1000,
                        entry.reconcile_micros / 1000,
                        entry.reconcile_wire_bytes,
                        entry.reconcile_failures,
                        sweep_token(&entry),
                        entry.delta_fallbacks,
                        entry.full_payload_sends,
                        entry.content_bytes,
                        short_digest(&entry.digest),
                    );
                } else {
                    println!(
                        "{}\t{}\t{}\tpeers={}\tpresent={present}\ttombstones={}\tobserved={}\tdrift=WARNING missing={} unexpected={} mismatched={}\tscan_issues={}\tstopped={}\taway={}\tsync_passes={}\tfull_scans={}\tinbound_noop_transactions={}\tinbound_guarded_transactions={}\tscan_ms={}\tmaterialize_ms={}\tpersist_ms={}\treconcile_ms={}\treconcile_wire_bytes={}\treconcile_failures={}\tsweep={}\tdelta_fallbacks={}\tfull_payload_sends={}\tcontent_bytes={}\tdigest={}\tstaged={staged}",
                        entry.name,
                        entry.folder,
                        entry.policy,
                        entry.peers,
                        entry.tombstones,
                        entry.observed,
                        entry.missing,
                        entry.unexpected,
                        entry.mismatched,
                        scan_issues_token(&entry),
                        stopped_token(&entry),
                        away_token(&entry),
                        entry.sync_passes,
                        entry.full_scans,
                        entry.inbound_noop_transactions,
                        entry.inbound_guarded_transactions,
                        entry.scan_micros / 1000,
                        entry.materialize_micros / 1000,
                        entry.persist_micros / 1000,
                        entry.reconcile_micros / 1000,
                        entry.reconcile_wire_bytes,
                        entry.reconcile_failures,
                        sweep_token(&entry),
                        entry.delta_fallbacks,
                        entry.full_payload_sends,
                        entry.content_bytes,
                        short_digest(&entry.digest),
                    );
                }
            }
        }
        SyncCommands::Add { .. } | SyncCommands::Rm { .. } | SyncCommands::Reload => {
            return hand_command_to_fabric();
        }
    }
    Ok(())
}

/// Replace this process with the `fabric` beside it, same arguments. `add`
/// checks selectors against the daemon's peer book, which this binary never
/// reads.
fn hand_command_to_fabric() -> Result<()> {
    let binary =
        std::env::current_exe().context("cannot resolve the running fabric-sync binary")?;
    let fabric = binary.with_file_name("fabric");
    if !fabric.exists() {
        bail!(
            "this sync command runs in fabric, which is not installed beside {}; \
             install fabric and fabric-sync from the same release",
            binary.display()
        );
    }
    let error = std::os::unix::process::CommandExt::exec(
        Command::new(&fabric).args(std::env::args_os().skip(1)),
    );
    Err(error).with_context(|| format!("failed to run {}", fabric.display()))
}

fn absolutize(folder: &str) -> Result<PathBuf> {
    let path = PathBuf::from(folder);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

#[derive(serde::Serialize)]
struct SyncLsJsonEntry<'a> {
    runtime_owner: String,
    companion: String,
    name: &'a str,
    folder: &'a str,
    policy: &'a str,
    peers: &'a str,
    present: usize,
    tombstones: usize,
    observed: usize,
    drift: Option<bool>,
    missing: usize,
    unexpected: usize,
    mismatched: usize,
    scan_issues: &'a [(String, String)],
    /// Calls to `sync_once`. NOT `full_scans`, which is two per call.
    sync_passes: u64,
    full_scans: u64,
    inbound_noop_transactions: u64,
    inbound_guarded_transactions: u64,
    /// Cumulative microseconds per phase of `sync_once`. Two samples and a
    /// division describe the present; a total on its own describes the past.
    scan_micros: u64,
    materialize_micros: u64,
    persist_micros: u64,
    reconcile_micros: u64,
    reconcile_wire_bytes: u64,
    reconcile_failures: u64,
    /// Why the tombstone sweep did or did not forget anything. `unknown` from a
    /// daemon that predates the field.
    sweep: &'a str,
    /// Peers this entry is NOT syncing with, and why. Empty is healthy.
    stopped_peers: Vec<String>,
    /// Roaming peers that this entry is waiting for. Empty is normal.
    away_peers: Vec<String>,
    /// Payloads this node SENT carrying its whole manifest, whatever the reason.
    /// High `reconcile_wire_bytes` with a low count here means this machine is
    /// RECEIVING full payloads rather than sending them.
    full_payload_sends: u64,
    /// Bytes of file content the daemon holds in memory for this entry.
    content_bytes: u64,
    /// Reconciles that fell back to full state. Zero is healthy; a RISING
    /// number means a cursor described state a peer did not hold.
    delta_fallbacks: u64,
    /// Lattice-point fingerprint of this entry's manifest. Compare it ACROSS
    /// peers: equal means converged, unequal means diverged. `present` and
    /// `tombstones` can match while the state differs, so they cannot answer
    /// this. Empty from a daemon that predates the field.
    digest: &'a str,
    /// Files staged for this entry under the fabric home and not yet
    /// published. Counted locally; the daemon does not know them.
    staged: usize,
}

impl<'a> From<&'a SyncEntryStatus> for SyncLsJsonEntry<'a> {
    fn from(entry: &'a SyncEntryStatus) -> Self {
        Self {
            runtime_owner: "unknown".to_string(),
            companion: "unknown".to_string(),
            name: &entry.name,
            folder: &entry.folder,
            policy: &entry.policy,
            peers: &entry.peers,
            present: logical_present(entry),
            tombstones: entry.tombstones,
            observed: entry.observed,
            drift: Some(
                entry.missing != 0
                    || entry.unexpected != 0
                    || entry.mismatched != 0
                    || !entry.scan_issues.is_empty(),
            ),
            missing: entry.missing,
            unexpected: entry.unexpected,
            mismatched: entry.mismatched,
            scan_issues: &entry.scan_issues,
            stopped_peers: entry
                .stopped_peers
                .iter()
                .filter(|(_, reason)| reason != "away")
                .map(|(peer, reason)| format!("{peer}:{reason}"))
                .collect(),
            away_peers: entry
                .stopped_peers
                .iter()
                .filter(|(_, reason)| reason == "away")
                .map(|(peer, _)| peer.clone())
                .collect(),
            full_payload_sends: entry.full_payload_sends,
            content_bytes: entry.content_bytes,
            delta_fallbacks: entry.delta_fallbacks,
            digest: &entry.digest,
            staged: 0,
            sync_passes: entry.sync_passes,
            full_scans: entry.full_scans,
            inbound_noop_transactions: entry.inbound_noop_transactions,
            inbound_guarded_transactions: entry.inbound_guarded_transactions,
            scan_micros: entry.scan_micros,
            materialize_micros: entry.materialize_micros,
            persist_micros: entry.persist_micros,
            reconcile_micros: entry.reconcile_micros,
            reconcile_wire_bytes: entry.reconcile_wire_bytes,
            reconcile_failures: entry.reconcile_failures,
            sweep: sweep_token(entry),
        }
    }
}

impl SyncLsJsonEntry<'_> {
    fn with_staged(mut self, staged: usize) -> Self {
        self.staged = staged;
        self
    }

    fn with_runtime(mut self, runtime: &SyncRuntimeStatus) -> Self {
        self.runtime_owner = runtime.owner.clone();
        self.companion = runtime.companion.clone();
        if runtime.owner == "unavailable" {
            self.drift = None;
        }
        self
    }
}

fn configured_sync_status(entry: &SyncEntry) -> SyncEntryStatus {
    SyncEntryStatus {
        name: entry.name.clone(),
        folder: entry.folder.display().to_string(),
        policy: entry.policy.as_str().to_string(),
        peers: match &entry.peers {
            SyncPeers::Wildcard(_) => "*".to_string(),
            SyncPeers::List(peers) => peers.join(","),
        },
        stopped_peers: vec![("runtime".to_string(), "unavailable".to_string())],
        ..SyncEntryStatus::default()
    }
}

fn short_digest(digest: &str) -> &str {
    if digest.is_empty() {
        return "unknown";
    }
    &digest[..digest.len().min(12)]
}

/// Which peers this entry has stopped syncing with, and why.
///
/// `none` when everything is healthy. A partial stop names the peer, because an
/// entry that converges with two machines and is cut off from a third is not a
/// healthy entry and an entry-wide flag would call it one.
fn stopped_token(entry: &SyncEntryStatus) -> String {
    let stopped = entry
        .stopped_peers
        .iter()
        .filter(|(_, reason)| reason != "away")
        .map(|(peer, reason)| format!("{peer}:{reason}"))
        .collect::<Vec<_>>();
    if stopped.is_empty() {
        "none".to_string()
    } else {
        stopped.join(",")
    }
}

fn away_token(entry: &SyncEntryStatus) -> String {
    let away = entry
        .stopped_peers
        .iter()
        .filter(|(_, reason)| reason == "away")
        .map(|(peer, _)| peer.as_str())
        .collect::<Vec<_>>();
    if away.is_empty() {
        "none".to_string()
    } else {
        away.join(",")
    }
}

fn scan_issues_token(entry: &SyncEntryStatus) -> String {
    if entry.scan_issues.is_empty() {
        return "none".to_string();
    }
    entry
        .scan_issues
        .iter()
        .map(|(path, reason)| format!("{path}:{reason}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// The sweep reason, or a placeholder when the daemon has not decided one yet.
///
/// An older daemon sends nothing here, so this must not render an empty string
/// as if it were a state.
fn sweep_token(entry: &SyncEntryStatus) -> &str {
    if entry.sweep.is_empty() {
        "unknown"
    } else {
        &entry.sweep
    }
}

fn logical_present(entry: &SyncEntryStatus) -> usize {
    if entry.present == 0 {
        entry.files
    } else {
        entry.present
    }
}

#[cfg(test)]
mod sync_ls_tests {
    use super::*;
    use SyncEntryStatus;

    fn status() -> SyncEntryStatus {
        SyncEntryStatus {
            delta_fallbacks: 0,
            full_payload_sends: 0,
            content_bytes: 0,
            stopped_peers: vec![
                ("hetz".into(), "denied".into()),
                ("bluey".into(), "away".into()),
            ],
            digest: "lattice-point-aaaa".into(),
            name: "catalog".to_string(),
            folder: "/catalog".to_string(),
            policy: "catalog".to_string(),
            peers: "*".to_string(),
            files: 40,
            present: 40,
            tombstones: 3,
            observed: 42,
            missing: 0,
            unexpected: 2,
            mismatched: 0,
            scan_issues: vec![("large.bin".into(), "too-large".into())],
            sync_passes: 9,
            full_scans: 17,
            inbound_noop_transactions: 11,
            inbound_guarded_transactions: 3,
            scan_micros: 1_500,
            materialize_micros: 2_500,
            persist_micros: 3_500,
            reconcile_micros: 4_500,
            reconcile_wire_bytes: 11_000_000,
            reconcile_failures: 3,
            sweep: "disabled".to_string(),
        }
    }

    #[test]
    fn sync_ls_json_schema_exposes_counts_and_drift() {
        let status = status();
        let json = serde_json::to_value(SyncLsJsonEntry::from(&status)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "runtime_owner": "unknown",
                "companion": "unknown",
                "name": "catalog",
                "folder": "/catalog",
                "policy": "catalog",
                "peers": "*",
                "present": 40,
                "tombstones": 3,
                "observed": 42,
                "drift": true,
                "missing": 0,
                "unexpected": 2,
                "mismatched": 0,
                "scan_issues": [["large.bin", "too-large"]],
                "full_scans": 17,
                "inbound_noop_transactions": 11,
                "inbound_guarded_transactions": 3,
                "sync_passes": 9,
                "scan_micros": 1500,
                "staged": 0,
                "materialize_micros": 2500,
                "persist_micros": 3500,
                "reconcile_micros": 4500,
                "reconcile_wire_bytes": 11000000,
                "reconcile_failures": 3,
                "sweep": "disabled",
                "delta_fallbacks": 0,
                "full_payload_sends": 0,
                "content_bytes": 0,
                "stopped_peers": ["hetz:denied"],
                "away_peers": ["bluey"],
                "digest": "lattice-point-aaaa"
            })
        );
        assert_eq!(stopped_token(&status), "hetz:denied");
        assert_eq!(away_token(&status), "bluey");
    }

    #[test]
    fn sync_ls_accepts_legacy_control_present_count() {
        let mut status = status();
        status.present = 0;
        assert_eq!(logical_present(&status), 40);
    }

    #[test]
    fn sync_ls_accepts_legacy_status_without_observability_counters() {
        let status: SyncEntryStatus = serde_json::from_value(serde_json::json!({
            "name": "catalog",
            "folder": "/catalog",
            "policy": "catalog",
            "peers": "*",
            "files": 40,
            "present": 40,
            "tombstones": 3,
            "observed": 40,
            "missing": 0,
            "unexpected": 0,
            "mismatched": 0
        }))
        .unwrap();
        assert_eq!(status.full_scans, 0);
        assert_eq!(status.inbound_noop_transactions, 0);
        assert_eq!(status.inbound_guarded_transactions, 0);
    }
}

/// Group target paths by the entry that would publish each, or take every
/// staged file of one entry for `--all`.
fn group_targets_by_entry(
    book: &SyncBook,
    targets: &[String],
    all: bool,
    entry: Option<&str>,
) -> Result<BTreeMap<String, Vec<String>>> {
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    if all {
        let Some(entry) = entry else {
            bail!("--all needs --entry <name>");
        };
        if book.get(entry).is_none() {
            bail!("no sync entry named {entry:?}");
        }
        groups.insert(entry.to_string(), Vec::new());
        return Ok(groups);
    }
    if targets.is_empty() {
        bail!("give one or more target paths, or --all --entry <name>");
    }
    for target in targets {
        let resolved = staging::resolve_target(book, &absolutize(target)?, entry)?;
        groups
            .entry(resolved.entry.name)
            .or_default()
            .push(resolved.rel);
    }
    Ok(groups)
}

/// True only when no daemon can take a request: it is not running, or it is
/// an older build that does not know the request type.
fn daemon_cannot_publish(error: &anyhow::Error) -> bool {
    let detail = format!("{error:#}");
    detail.contains("is not running") || detail.contains("unknown variant")
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or(text)
}

/// Staged files per entry, from the staging tree under the fabric home. An
/// unreadable tree prints one line and counts as nothing, so `sync ls` still
/// answers about the daemon.
fn staged_counts(home: &FabricHome) -> BTreeMap<String, usize> {
    let listed = SyncBook::load(home).and_then(|book| staging::list(home, &book, None));
    match listed {
        Ok(files) => {
            let mut counts = BTreeMap::new();
            for file in files {
                *counts.entry(file.entry).or_insert(0) += 1;
            }
            counts
        }
        Err(error) => {
            eprintln!("fabric: could not read the staging tree: {error:#}");
            BTreeMap::new()
        }
    }
}
