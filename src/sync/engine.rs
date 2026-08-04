//! The async sync engine: real folders on disk, kept converged with peers.
//!
//! The engine is the daemon-managed layer that turns the pure [`SyncNode`] and
//! the on-wire backend into a live feature. Per configured entry it:
//! - **scans** the folder into the node (each file → `local_write`, missing files
//!   → `local_remove` under policy),
//! - **materializes** the node's manifest back to disk (writes present content,
//!   restores catalog deletes, removes bus tombstones),
//! - **watches** the folder and re-syncs on change (near-instant, not a poll),
//! - **reconciles** with each target peer over a swappable [`SyncTransport`].
//!
//! The transport is the seam that makes the backend swappable: the daemon plugs
//! in an iroh transport (over the `fabric/sync` ALPN); the tests plug in an
//! in-process loopback transport and exercise the whole engine against real
//! temp folders with no network. Manifests are persisted per entry so logical
//! versions stay monotonic across daemon restarts.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use crate::config::FabricHome;

use super::config::{PolicyRules, SyncBook, SyncEntry, SyncPeers};
use super::manifest::{Author, ContentHash, Manifest};
use super::node::{Reconciled, SyncNode, content_hash};

/// How long to wait after a filesystem event settles before syncing, so a burst
/// of writes coalesces into one reconcile.
const WATCH_DEBOUNCE: Duration = Duration::from_millis(150);
/// A continuously mutating tree must still make progress, but it must not drive
/// the engine at the debounce frequency forever. This caps watcher-driven
/// reconciles at one per window while coalescing everything in between.
const WATCH_MAX_COALESCE: Duration = Duration::from_secs(2);
/// Safety-net periodic reconcile even without filesystem events (catches missed
/// events and newly trusted peers).
const PERIODIC_RESYNC: Duration = Duration::from_secs(30);
/// Bounded safety scan for watcher events missed across sleep/wake or a
/// transient watcher failure. Clean periodic ticks do not scan the tree.
const MISSED_EVENT_RESYNC: Duration = Duration::from_secs(5 * 60);
/// Watcher notifications can arrive after the materialization that caused
/// them. Remember only a bounded number of exact post-write identities so a
/// delayed daemon-owned event can be acknowledged without another tree scan.
const MAX_DAEMON_WRITE_FINGERPRINTS: usize = 4_096;

#[inline]
fn periodic_scan_due(dirty: bool, safety_due: bool) -> bool {
    dirty || safety_due
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileFingerprint {
    hash: ContentHash,
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    ctime_secs: i64,
    #[cfg(unix)]
    ctime_nanos: i64,
}

impl FileFingerprint {
    fn after_write(path: &Path, hash: ContentHash) -> std::io::Result<Self> {
        let metadata = std::fs::metadata(path)?;
        if !metadata.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "fingerprinted path is not a regular file",
            ));
        }
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            hash,
            len: metadata.len(),
            modified: metadata.modified()?,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            ctime_secs: metadata.ctime(),
            #[cfg(unix)]
            ctime_nanos: metadata.ctime_nsec(),
        })
    }

    fn read(path: &Path) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::after_write(path, content_hash(&bytes))
    }
}

#[derive(Debug)]
struct DaemonWriteFingerprint {
    fingerprint: FileFingerprint,
    generation: u64,
    sequence: u64,
    committed: bool,
}

#[derive(Debug, Default)]
struct DaemonWriteJournal {
    next_sequence: u64,
    entries: HashMap<PathBuf, DaemonWriteFingerprint>,
    order: VecDeque<(PathBuf, u64)>,
}

impl DaemonWriteJournal {
    fn record(&mut self, path: PathBuf, fingerprint: FileFingerprint, generation: u64) {
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let sequence = self.next_sequence;
        self.entries.insert(
            path.clone(),
            DaemonWriteFingerprint {
                fingerprint,
                generation,
                sequence,
                committed: false,
            },
        );
        self.order.push_back((path, sequence));
        while self.order.len() > MAX_DAEMON_WRITE_FINGERPRINTS {
            let Some((path, sequence)) = self.order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&path)
                .is_some_and(|entry| entry.sequence == sequence)
            {
                self.entries.remove(&path);
            }
        }
    }

    fn consume_batch(
        &mut self,
        paths: &[(PathBuf, FileFingerprint)],
        first_event_generation: u64,
    ) -> bool {
        let matches = !paths.is_empty()
            && paths.iter().all(|(path, fingerprint)| {
                self.entries.get(path).is_some_and(|entry| {
                    entry.committed
                        && entry.generation.checked_add(1) == Some(first_event_generation)
                        && entry.fingerprint == *fingerprint
                })
            });
        // Whether this was the expected event or an external mismatch, never
        // let an old identity suppress a later change to the same path.
        for (path, _) in paths {
            self.entries.remove(path);
        }
        matches
    }

    fn forget_paths<'a>(&mut self, paths: impl IntoIterator<Item = &'a PathBuf>) {
        for path in paths {
            self.entries.remove(path);
        }
    }

    fn commit_all(&mut self) {
        for entry in self.entries.values_mut() {
            entry.committed = true;
        }
    }
}

/// A dialable peer for a reconcile: a display id and, for the iroh transport, its
/// address. The loopback transport routes by `id` alone.
#[derive(Debug, Clone)]
pub struct PeerRef {
    pub id: String,
    pub addr: Option<EndpointAddr>,
}

/// The swappable transport that carries a client-side reconcile to a peer. The
/// daemon implements this over iroh; tests implement it in-process.
pub trait SyncTransport: Send + Sync + 'static {
    /// The peers an entry's selector resolves to right now (membership follows
    /// `peers.toml` for the `"*"` wildcard).
    fn peers_for(&self, peers: &SyncPeers) -> impl Future<Output = Vec<PeerRef>> + Send;

    /// Run a client reconcile for sync `name` against `peer`, mutating `node`.
    fn reconcile(
        &self,
        peer: PeerRef,
        name: String,
        node: Arc<Mutex<SyncNode>>,
    ) -> impl Future<Output = Result<Reconciled>> + Send;
}

/// Per-entry work bookkeeping shared with the filesystem-watcher callback.
///
/// The mutation generation makes queued inbound sessions reusable only after a
/// durable scan of the exact generation they can observe. The first inbound
/// session always scans; sessions already queued behind it can skip the
/// redundant pre-merge scan/persist when no mutating event occurred meanwhile.
#[derive(Debug)]
struct EntryWork {
    mutation_generation: AtomicU64,
    durable_generation: AtomicU64,
    inbound_waiters: AtomicUsize,
    daemon_writes: StdMutex<DaemonWriteJournal>,
    /// Monotonic while this name remains continuously configured in the same
    /// daemon. Exposed through `fabric sync ls` so production can prove whether
    /// an inbound transaction walked the tree.
    full_scans: AtomicU64,
    /// Exact-manifest, complete-content inbound transactions that bypassed the
    /// guarded scan/materialize path.
    inbound_noop_transactions: AtomicU64,
    /// Inbound transactions that selected the guarded scan/materialize path.
    inbound_guarded_transactions: AtomicU64,
    #[cfg(test)]
    persist_calls: AtomicUsize,
}

impl EntryWork {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            // Generation one forces the first inbound session to scan even
            // before the watcher observes its first event.
            mutation_generation: AtomicU64::new(1),
            durable_generation: AtomicU64::new(0),
            inbound_waiters: AtomicUsize::new(0),
            daemon_writes: StdMutex::new(DaemonWriteJournal::default()),
            full_scans: AtomicU64::new(0),
            inbound_noop_transactions: AtomicU64::new(0),
            inbound_guarded_transactions: AtomicU64::new(0),
            #[cfg(test)]
            persist_calls: AtomicUsize::new(0),
        })
    }

    fn record_mutation(&self) -> u64 {
        self.mutation_generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1)
    }

    fn record_daemon_write(&self, path: &Path, hash: ContentHash, generation: u64) {
        let Ok(fingerprint) = FileFingerprint::after_write(path, hash) else {
            return;
        };
        self.daemon_writes
            .lock()
            .unwrap()
            .record(path.to_path_buf(), fingerprint, generation);
    }

    fn commit_daemon_writes(&self) {
        self.daemon_writes.lock().unwrap().commit_all();
    }

    fn acknowledge_daemon_write_batch(&self, batch: &WatchEventBatch) -> bool {
        if !batch.daemon_write_candidate
            || !batch.contiguous
            || batch.paths.is_empty()
            || self.mutation_generation.load(Ordering::Acquire) != batch.last_generation
        {
            self.daemon_writes
                .lock()
                .unwrap()
                .forget_paths(&batch.paths);
            return false;
        }

        let mut current = Vec::with_capacity(batch.paths.len());
        for path in &batch.paths {
            let Ok(fingerprint) = FileFingerprint::read(path) else {
                self.daemon_writes
                    .lock()
                    .unwrap()
                    .forget_paths(&batch.paths);
                return false;
            };
            current.push((path.clone(), fingerprint));
        }
        let matches = self
            .daemon_writes
            .lock()
            .unwrap()
            .consume_batch(&current, batch.first_generation);
        if matches {
            // The callback already advanced the mutation generation before
            // queuing this batch. Exact daemon-owned bytes are already durable,
            // so acknowledge only the generations represented by this batch.
            self.mark_generation_durable(batch.last_generation);
        }
        matches
    }

    fn begin_inbound(self: &Arc<Self>) -> InboundWaiter {
        let queued = self.inbound_waiters.fetch_add(1, Ordering::AcqRel) > 0;
        InboundWaiter {
            work: self.clone(),
            queued,
        }
    }

    fn may_reuse_durable_scan(&self, queued: bool) -> bool {
        queued && self.is_clean()
    }

    fn is_clean(&self) -> bool {
        self.mutation_generation.load(Ordering::Acquire)
            == self.durable_generation.load(Ordering::Acquire)
    }

    fn mark_generation_durable(&self, generation: u64) {
        self.durable_generation
            .fetch_max(generation, Ordering::AcqRel);
    }
}

/// Cancellation-safe accounting for inbound sessions waiting on the entry
/// operation guard.
struct InboundWaiter {
    work: Arc<EntryWork>,
    queued: bool,
}

impl Drop for InboundWaiter {
    fn drop(&mut self) {
        self.work.inbound_waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One configured entry's live state.
struct EntryState {
    config: SyncEntry,
    policy: PolicyRules,
    node: Arc<Mutex<SyncNode>>,
    /// Serialize filesystem scan/materialize phases for this entry. An inbound
    /// transaction keeps an owned guard across the wire session; outbound
    /// sessions release it while dialing to avoid distributed lock inversion.
    operation: Arc<Mutex<()>>,
    /// Last state the engine actually observed or materialized on local disk.
    /// A Present held only in the node is not evidence that a missing path was
    /// locally deleted; this receipt is what distinguishes those cases.
    observed: Arc<StdMutex<HashMap<String, ContentHash>>>,
    work: Arc<EntryWork>,
}

/// Atomically persisted authoritative state for one sync entry. `manifest.json`
/// remains a compatibility/inspection projection, but restart recovery reads
/// this combined file so manifest transitions and their observed-disk receipt
/// cannot be torn apart by a crash.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedEntryState {
    manifest: Manifest,
    observed: HashMap<String, ContentHash>,
}

/// State retained across an inbound merge.
///
/// An exactly converged peer can use `Noop`: its manifest cannot change our
/// node, and a complete local content store means the session cannot repair
/// anything locally. Every other reconcile uses `Guarded`; its operation guard
/// keeps engine-driven scan/materialize work out of the middle of the wire
/// session, and `baseline` distinguishes local paths from remote-only paths at
/// completion.
pub(crate) struct PreparedInbound {
    entry: Arc<EntryState>,
    mode: PreparedInboundMode,
}

enum PreparedInboundMode {
    Noop,
    Guarded {
        baseline: HashMap<String, ContentHash>,
        manifest: Manifest,
        _waiter: InboundWaiter,
        _operation: OwnedMutexGuard<()>,
    },
}

impl PreparedInbound {
    pub(crate) fn node(&self) -> Arc<Mutex<SyncNode>> {
        self.entry.node.clone()
    }
}

impl<T: SyncTransport> std::fmt::Debug for SyncEngine<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncEngine").finish_non_exhaustive()
    }
}

/// The engine: owns every entry's node and drives scan/materialize/reconcile.
pub struct SyncEngine<T: SyncTransport> {
    home: FabricHome,
    author: Author,
    transport: Arc<T>,
    entries: RwLock<HashMap<String, Arc<EntryState>>>,
    /// Entry names that already have a watch loop, so a reload only spawns loops
    /// for newly added entries.
    watching: StdMutex<HashSet<String>>,
    cancel: CancellationToken,
}

impl<T: SyncTransport> SyncEngine<T> {
    /// Build an engine from the current `syncs.toml`, loading any persisted
    /// manifests. Does not start watching; call [`SyncEngine::run`] for that.
    pub async fn new(
        home: FabricHome,
        author: Author,
        transport: Arc<T>,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>> {
        let engine = Arc::new(Self {
            home,
            author,
            transport,
            entries: RwLock::new(HashMap::new()),
            watching: StdMutex::new(HashSet::new()),
            cancel,
        });
        engine.load_from_config().await?;
        Ok(engine)
    }

    /// (Re)load entries from `syncs.toml`, keeping existing nodes for entries
    /// that are unchanged and dropping entries no longer configured.
    pub async fn load_from_config(&self) -> Result<()> {
        let book = SyncBook::load(&self.home)?;
        let mut entries = self.entries.write().await;
        let mut next: HashMap<String, Arc<EntryState>> = HashMap::new();
        for cfg in book.entries() {
            let policy = cfg.policy.rules();
            // The existing watcher for a name survives reloads, so its shared
            // mutation generation must survive too even when another config
            // field changed and the node is rebuilt.
            let work = entries
                .get(&cfg.name)
                .map(|existing| existing.work.clone())
                .unwrap_or_else(EntryWork::new);
            // Reuse an existing node for an unchanged entry so in-memory content
            // survives a reload; otherwise start one from the persisted manifest.
            let (node, operation, observed) = match entries.get(&cfg.name) {
                Some(existing) if existing.config == *cfg => (
                    existing.node.clone(),
                    existing.operation.clone(),
                    existing.observed.clone(),
                ),
                _ => {
                    work.durable_generation.store(0, Ordering::Release);
                    work.record_mutation();
                    let (node, observed) = self.load_node_and_observed(cfg).await?;
                    (
                        Arc::new(Mutex::new(node)),
                        Arc::new(Mutex::new(())),
                        Arc::new(StdMutex::new(observed)),
                    )
                }
            };
            next.insert(
                cfg.name.clone(),
                Arc::new(EntryState {
                    config: cfg.clone(),
                    policy,
                    node,
                    operation,
                    observed,
                    work,
                }),
            );
        }
        *entries = next;
        Ok(())
    }

    async fn load_node_and_observed(
        &self,
        cfg: &SyncEntry,
    ) -> Result<(SyncNode, HashMap<String, ContentHash>)> {
        let mut node = SyncNode::new(self.author);
        if let Some(state) = self.read_state(&cfg.name)? {
            node.adopt(&state.manifest);
            return Ok((node, state.observed));
        }
        if let Some(manifest) = self.read_manifest(&cfg.name)? {
            node.adopt(&manifest);
        }
        let observed = observed_from_disk(node.manifest(), cfg)?;
        Ok((node, observed))
    }

    /// Resolve a sync name to its node (used by the daemon's inbound accept).
    pub async fn node_for(&self, name: &str) -> Option<Arc<Mutex<SyncNode>>> {
        self.entries
            .read()
            .await
            .get(name)
            .map(|entry| entry.node.clone())
    }

    /// Expose an entry's node to an inbound reconcile, bypassing folder scans
    /// only when the peer is exactly converged and our content store is
    /// complete.
    ///
    /// An unobserved local filesystem change is safe in this exact no-op case:
    /// the peer's manifest cannot win or cause local materialization, so the
    /// watcher can record the local intent normally. Any differing manifest or
    /// missing local content takes the guarded path below.
    pub(crate) async fn prepare_inbound_for_manifest(
        &self,
        name: &str,
        remote_manifest: &Manifest,
    ) -> Result<Option<PreparedInbound>> {
        let Some(entry) = self.entries.read().await.get(name).cloned() else {
            return Ok(None);
        };
        let is_complete_noop = {
            let node = entry.node.lock().await;
            node.manifest() == remote_manifest && node.missing_content_hashes().is_empty()
        };
        if is_complete_noop {
            entry
                .work
                .inbound_noop_transactions
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Some(PreparedInbound {
                entry,
                mode: PreparedInboundMode::Noop,
            }));
        }
        self.prepare_inbound_entry(entry).await.map(Some)
    }

    /// Scan and durably record local filesystem changes before exposing an
    /// entry's node to a potentially mutating inbound reconcile.
    ///
    /// This ordering is essential for delete-propagating policies: an atomic
    /// local rename/delete may already express user intent while its watcher
    /// event is still inside the debounce window. Letting a peer reconcile and
    /// materialize first could restore the stale Present entry and erase the
    /// only observable evidence of that local deletion. Scanning before merge
    /// also avoids treating paths that are genuinely new on the remote as local
    /// deletions, because they are not in the observed-disk receipt yet.
    #[cfg(test)]
    pub(crate) async fn prepare_inbound(&self, name: &str) -> Result<Option<PreparedInbound>> {
        let Some(entry) = self.entries.read().await.get(name).cloned() else {
            return Ok(None);
        };
        self.prepare_inbound_entry(entry).await.map(Some)
    }

    async fn prepare_inbound_entry(&self, entry: Arc<EntryState>) -> Result<PreparedInbound> {
        let waiter = entry.work.begin_inbound();
        let operation = entry.operation.clone().lock_owned().await;
        let queued = waiter.queued;

        if !entry.work.may_reuse_durable_scan(queued) {
            let generation = entry.work.mutation_generation.load(Ordering::Acquire);
            let before_manifest = entry.node.lock().await.manifest().clone();
            let before_observed = entry.observed.lock().unwrap().clone();
            self.scan_entry(&entry).await?;
            let final_manifest = entry.node.lock().await.manifest().clone();
            let final_observed = entry.observed.lock().unwrap().clone();
            if entry.work.durable_generation.load(Ordering::Acquire) != generation
                || final_manifest != before_manifest
                || final_observed != before_observed
                || !self.state_path(&entry.config.name).exists()
            {
                // A legacy entry may not have state.json yet, and a crash
                // during this first wire session must not lose newly observed
                // local intent. An already durable no-op scan needs no rewrite.
                self.persist_entry(&entry).await?;
            }
            entry.work.mark_generation_durable(generation);
        }
        entry
            .work
            .inbound_guarded_transactions
            .fetch_add(1, Ordering::Relaxed);
        let baseline = entry.observed.lock().unwrap().clone();
        let manifest = entry.node.lock().await.manifest().clone();
        Ok(PreparedInbound {
            entry,
            mode: PreparedInboundMode::Guarded {
                baseline,
                manifest,
                _waiter: waiter,
                _operation: operation,
            },
        })
    }

    /// Complete an inbound transaction while its entry operation guard is still
    /// held. Disk changes that landed during the wire session are compared to
    /// the pre-merge baseline: a vanished baseline Present is a local delete,
    /// while a remote-only Present is materialized instead of tombstoned.
    pub(crate) async fn complete_inbound(&self, prepared: PreparedInbound) -> Result<()> {
        let PreparedInbound { entry, mode } = prepared;
        let PreparedInboundMode::Guarded {
            baseline,
            manifest,
            _waiter,
            _operation,
        } = mode
        else {
            return Ok(());
        };
        let generation = entry.work.mutation_generation.load(Ordering::Acquire);
        // This scan is not optional: it catches disk changes that landed during
        // the wire session, whose watcher events may still be inside the
        // debounce window, so the mutation generation cannot stand in for it.
        // It is cheap now because scan_folder reuses recorded hashes for files
        // whose size and mtime are unchanged.
        self.scan_entry(&entry).await?;
        self.materialize_entry_state(&entry, &baseline).await?;
        let final_manifest = entry.node.lock().await.manifest().clone();
        let final_observed = entry.observed.lock().unwrap().clone();
        if final_manifest != manifest || final_observed != baseline {
            self.persist_entry(&entry).await?;
        }
        entry.work.mark_generation_durable(generation);
        Ok(())
    }

    /// The configured sync names.
    pub async fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.read().await.keys().cloned().collect();
        names.sort();
        names
    }

    /// A stable snapshot of logical manifest state and the materialized-disk
    /// receipt for every entry.
    pub async fn status(&self) -> Vec<SyncStatus> {
        let entries = self.entries.read().await;
        let mut out = Vec::new();
        for (name, entry) in entries.iter() {
            let _operation = entry.operation.lock().await;
            let node = entry.node.lock().await;
            let observed = entry.observed.lock().unwrap();
            let manifest = node.manifest();
            let present = manifest.present_paths().count();
            let tombstones = manifest.len() - present;
            let missing = manifest
                .present_paths()
                .filter(|(path, _)| !observed.contains_key(path.as_str()))
                .count();
            let unexpected = observed
                .keys()
                .filter(|path| !manifest.get(path).is_some_and(|item| item.is_present()))
                .count();
            let mismatched = manifest
                .present_paths()
                .filter(|(path, meta)| {
                    observed
                        .get(path.as_str())
                        .is_some_and(|hash| hash != &meta.hash)
                })
                .count();
            out.push(SyncStatus {
                name: name.clone(),
                folder: entry.config.folder.clone(),
                policy: entry.config.policy.as_str(),
                peers: entry.config.peers.clone(),
                present,
                tombstones,
                observed: observed.len(),
                missing,
                unexpected,
                mismatched,
                full_scans: entry.work.full_scans.load(Ordering::Relaxed),
                inbound_noop_transactions: entry
                    .work
                    .inbound_noop_transactions
                    .load(Ordering::Relaxed),
                inbound_guarded_transactions: entry
                    .work
                    .inbound_guarded_transactions
                    .load(Ordering::Relaxed),
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Scan the folder, materialize, reconcile with every target peer, then
    /// materialize again and persist the manifest. The full one-shot sync for an
    /// entry — safe to call from a watcher, a timer, or after an inbound session.
    pub async fn sync_once(&self, name: &str) -> Result<()> {
        let Some(entry) = self.entries.read().await.get(name).cloned() else {
            return Ok(());
        };
        // Never hold the local operation guard across a peer dial. If A and B
        // initiate together, retaining A while awaiting B's inbound guard (and
        // vice versa) is a distributed lock inversion. Carry a pre-merge
        // baseline across the unlocked network step instead.
        let (baseline, manifest) = {
            let _operation = entry.operation.lock().await;
            let protected = entry.observed.lock().unwrap().clone();
            let generation = entry.work.mutation_generation.load(Ordering::Acquire);
            self.scan_entry(&entry).await?;
            self.materialize_entry_state(&entry, &protected).await?;
            self.persist_entry(&entry).await?;
            entry.work.mark_generation_durable(generation);
            let baseline = entry.observed.lock().unwrap().clone();
            let manifest = entry.node.lock().await.manifest().clone();
            (baseline, manifest)
        };

        let peers = self.transport.peers_for(&entry.config.peers).await;
        for peer in peers {
            if self.cancel.is_cancelled() {
                break;
            }
            match self
                .transport
                .reconcile(peer.clone(), name.to_string(), entry.node.clone())
                .await
            {
                Ok(stats) => {
                    if !stats.is_noop() {
                        tracing::debug!(sync = name, peer = peer.id, ?stats, "sync reconciled");
                    }
                }
                Err(error) => {
                    tracing::debug!(sync = name, peer = peer.id, %error, "sync reconcile failed");
                }
            }
        }

        let _operation = entry.operation.lock().await;
        self.scan_entry(&entry).await?;
        self.materialize_entry_state(&entry, &baseline).await?;
        let final_manifest = entry.node.lock().await.manifest().clone();
        let final_observed = entry.observed.lock().unwrap().clone();
        if final_manifest != manifest || final_observed != baseline {
            self.persist_entry(&entry).await?;
        }
        Ok(())
    }

    /// Materialize just this entry to disk.
    pub async fn materialize_entry(&self, name: &str) -> Result<()> {
        let Some(entry) = self.entries.read().await.get(name).cloned() else {
            return Ok(());
        };
        let _operation = entry.operation.lock().await;
        let protected = entry.observed.lock().unwrap().clone();
        self.materialize_entry_state(&entry, &protected).await?;
        self.persist_entry(&entry).await
    }

    async fn scan_entry(&self, entry: &EntryState) -> Result<bool> {
        entry.work.full_scans.fetch_add(1, Ordering::Relaxed);
        let root = entry.config.folder.clone();
        let cfg = entry.config.clone();
        let policy = entry.policy;
        let mut node = entry.node.lock().await;
        let mut observed = entry.observed.lock().unwrap();
        scan_into_node_observed(&mut node, &root, &cfg, policy, &mut observed)
    }

    async fn materialize_entry_state(
        &self,
        entry: &EntryState,
        protected: &HashMap<String, ContentHash>,
    ) -> Result<()> {
        let root = entry.config.folder.clone();
        let policy = entry.policy;
        let generation = entry.work.mutation_generation.load(Ordering::Acquire);
        let mut node = entry.node.lock().await;
        let mut observed = entry.observed.lock().unwrap();
        materialize_tracked(
            &mut node,
            &root,
            policy,
            protected,
            &mut observed,
            Some((&entry.work, generation)),
        )
    }

    async fn persist_entry(&self, entry: &EntryState) -> Result<()> {
        #[cfg(test)]
        entry.work.persist_calls.fetch_add(1, Ordering::Relaxed);
        let manifest = entry.node.lock().await.manifest().clone();
        let observed = entry.observed.lock().unwrap().clone();
        self.write_state(
            &entry.config.name,
            &PersistedEntryState { manifest, observed },
        )?;
        entry.work.commit_daemon_writes();
        Ok(())
    }

    fn manifest_path(&self, name: &str) -> PathBuf {
        self.home
            .root()
            .join("sync")
            .join(sanitize_name(name))
            .join("manifest.json")
    }

    fn state_path(&self, name: &str) -> PathBuf {
        self.home
            .root()
            .join("sync")
            .join(sanitize_name(name))
            .join("state.json")
    }

    fn read_state(&self, name: &str) -> Result<Option<PersistedEntryState>> {
        let path = self.state_path(name);
        if !path.exists() {
            return Ok(None);
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let state: PersistedEntryState = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(Some(state))
    }

    fn read_manifest(&self, name: &str) -> Result<Option<Manifest>> {
        let path = self.manifest_path(name);
        if !path.exists() {
            return Ok(None);
        }
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let manifest: Manifest = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(Some(manifest))
    }

    fn write_manifest(&self, name: &str, manifest: &Manifest) -> Result<()> {
        let path = self.manifest_path(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let raw = serde_json::to_string_pretty(manifest)?;
        write_atomic(&path, raw.as_bytes())
    }

    fn write_state(&self, name: &str, state: &PersistedEntryState) -> Result<()> {
        let path = self.state_path(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let raw = serde_json::to_string_pretty(state)?;
        // The combined state is authoritative and lands atomically first.
        write_atomic(&path, raw.as_bytes())?;
        // Keep the established manifest path current for operators and older
        // Fabric binaries. A crash between these writes still recovers from the
        // already-committed combined state above.
        self.write_manifest(name, &state.manifest)
    }

    /// Start watching every configured entry's folder and syncing on change,
    /// then run until the cancellation token fires. Idempotent per entry.
    pub async fn run(self: &Arc<Self>) -> Result<()> {
        self.ensure_watching().await;
        self.cancel.cancelled().await;
        Ok(())
    }

    /// Re-read `syncs.toml` into the engine and start watching any newly added
    /// entries. Mirrors `reload-peers`: a running daemon picks up the new file
    /// without a restart. (Changing an existing entry's folder still needs a
    /// restart to re-point its watcher.)
    pub async fn reload(self: &Arc<Self>) -> Result<()> {
        self.load_from_config().await?;
        self.ensure_watching().await;
        Ok(())
    }

    /// Spawn a watch loop for every configured entry that does not already have
    /// one.
    async fn ensure_watching(self: &Arc<Self>) {
        let names = self.names().await;
        let mut watching = self.watching.lock().unwrap();
        for name in names {
            if watching.insert(name.clone()) {
                let engine = self.clone();
                tokio::spawn(async move {
                    engine.entry_loop(name).await;
                });
            }
        }
    }

    async fn entry_loop(self: Arc<Self>, name: String) {
        let entry = match self.entries.read().await.get(&name) {
            Some(entry) => entry.clone(),
            None => return,
        };
        let root = entry.config.folder.clone();

        // Best-effort initial sync.
        if let Err(error) = self.sync_once(&name).await {
            tracing::warn!(sync = %name, %error, "initial sync failed");
        }

        // The channel is only an edge trigger. One pending signal is enough;
        // keeping it bounded prevents an arbitrarily hot writer from building
        // an in-memory event backlog while the current sync is running.
        let (tx, mut rx) = mpsc::channel::<WatchEvent>(1);
        let _watcher = spawn_watcher(&root, tx, entry.work.clone());

        let mut ticker = tokio::time::interval(PERIODIC_RESYNC);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // consume the immediate first tick
        let mut last_safety_scan = tokio::time::Instant::now();

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let dirty = entry.work.mutation_generation.load(Ordering::Acquire)
                        != entry.work.durable_generation.load(Ordering::Acquire);
                    let safety_due = last_safety_scan.elapsed() >= MISSED_EVENT_RESYNC;
                    if periodic_scan_due(dirty, safety_due) {
                        if safety_due { last_safety_scan = tokio::time::Instant::now(); }
                        if let Err(error) = self.sync_once(&name).await {
                            tracing::debug!(sync = %name, %error, "periodic sync failed");
                        }
                    }
                }
                event = rx.recv() => {
                    let Some(event) = event else { break; };
                    // Wait for a quiet edge, but cap the window so a
                    // continuously mutating tree still makes bounded progress.
                    let Some(batch) = coalesce_watch_events(
                        event,
                        &mut rx,
                        WATCH_DEBOUNCE,
                        WATCH_MAX_COALESCE,
                    )
                    .await
                    else {
                        break;
                    };
                    // Every materialization and its state persist hold this
                    // guard. Do not acknowledge a delayed self-event before
                    // the bytes it identifies are durably committed.
                    let daemon_owned = {
                        let _operation = entry.operation.lock().await;
                        entry.work.acknowledge_daemon_write_batch(&batch)
                    };
                    if daemon_owned {
                        continue;
                    }
                    if let Err(error) = self.sync_once(&name).await {
                        tracing::debug!(sync = %name, %error, "watch sync failed");
                    }
                }
            }
        }
    }
}

/// Coalesce watcher events until the tree is quiet for [`WATCH_DEBOUNCE`], or
/// until [`WATCH_MAX_COALESCE`] bounds a continuous mutation stream.
///
/// Returns `None` only when the watcher channel has closed.
async fn coalesce_watch_events(
    first: WatchEvent,
    rx: &mut mpsc::Receiver<WatchEvent>,
    debounce: Duration,
    max_coalesce: Duration,
) -> Option<WatchEventBatch> {
    let mut batch = WatchEventBatch::new(first);
    let deadline = tokio::time::Instant::now() + max_coalesce;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            next = tokio::time::timeout(debounce, rx.recv()) => {
                match next {
                    Ok(Some(event)) => {
                        batch.push(event);
                        continue;
                    }
                    Ok(None) => return None,
                    Err(_) => break,
                }
            }
        }
    }
    while let Ok(event) = rx.try_recv() {
        batch.push(event);
    }
    Some(batch)
}

/// Logical manifest and materialized-disk receipt counts for `fabric sync ls`.
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub name: String,
    pub folder: PathBuf,
    pub policy: &'static str,
    pub peers: SyncPeers,
    pub present: usize,
    pub tombstones: usize,
    pub observed: usize,
    pub missing: usize,
    pub unexpected: usize,
    pub mismatched: usize,
    /// Completed or attempted full folder scans since this entry instance was
    /// loaded. Monotonic while the name remains continuously configured in the
    /// same daemon.
    pub full_scans: u64,
    /// Exact-manifest, complete-content inbound transactions that took the
    /// production no-scan fast path.
    pub inbound_noop_transactions: u64,
    /// Inbound transactions that selected the guarded scan/materialize path.
    pub inbound_guarded_transactions: u64,
}

// ---- filesystem scan / materialize (sync helpers, unit-testable) ----

struct ScannedFile {
    rel: String,
    path: PathBuf,
    /// Content, read only when the file is new or changed. An unchanged file
    /// keeps this `None` and reuses its recorded hash, which is the difference
    /// between hashing the whole tree every scan and hashing only what moved.
    bytes: Option<Vec<u8>>,
    hash: ContentHash,
    mtime_secs: i64,
    mtime_nanos: u32,
}

impl ScannedFile {
    /// Content for the rare paths that need bytes for a file we did not re-read:
    /// reviving an inherited tombstone, or backfilling content the node lost.
    fn read_bytes(&self) -> Result<Vec<u8>> {
        match &self.bytes {
            Some(bytes) => Ok(bytes.clone()),
            None => std::fs::read(&self.path)
                .with_context(|| format!("failed to read {}", self.path.display())),
        }
    }
}

/// Walk `root` recursively, returning in-scope regular files (symlinks skipped,
/// include globs applied, paths normalized).
fn scan_folder(root: &Path, entry: &SyncEntry, known: &Manifest) -> Result<Vec<ScannedFile>> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for child in
            std::fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))?
        {
            let child = child?;
            let file_type = child.file_type()?;
            let path = child.path();
            if file_type.is_symlink() {
                continue; // never follow symlinks out of the folder
            }
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Ok(rel) = path.strip_prefix(root) else {
                continue;
            };
            let rel = rel.to_string_lossy();
            let Some(norm) = Manifest::normalize_path(&rel) else {
                continue;
            };
            if !entry.includes(&norm) {
                continue;
            }
            let (mtime_secs, mtime_nanos) = mtime_of(&child);
            let size = child.metadata().map(|meta| meta.len()).unwrap_or(u64::MAX);
            // Reuse the recorded hash when size and both mtime components are
            // byte-identical to what the manifest already saw. Anything that
            // differs, or is unknown, is read and hashed as before.
            let known = known
                .get(&norm)
                .and_then(|entry| entry.meta())
                .filter(|meta| {
                    meta.size == size
                        && meta.mtime_secs == mtime_secs
                        && meta.mtime_nanos == mtime_nanos
                });
            let (bytes, hash) = match known {
                Some(meta) => (None, meta.hash),
                None => {
                    let bytes = std::fs::read(&path)
                        .with_context(|| format!("failed to read {}", path.display()))?;
                    let hash = content_hash(&bytes);
                    (Some(bytes), hash)
                }
            };
            out.push(ScannedFile {
                rel: norm,
                path,
                bytes,
                hash,
                mtime_secs,
                mtime_nanos,
            });
        }
    }
    Ok(out)
}

/// Scan `root` into `node`: record every file, and treat files that vanished
/// from disk per policy (catalog ignores; bus tombstones). Returns whether the
/// manifest changed.
#[cfg(test)]
fn scan_into_node(
    node: &mut SyncNode,
    root: &Path,
    entry: &SyncEntry,
    policy: PolicyRules,
) -> Result<bool> {
    let mut observed = observed_from_manifest(node.manifest());
    scan_into_node_observed(node, root, entry, policy, &mut observed)
}

#[cfg(test)]
fn observed_from_manifest(manifest: &Manifest) -> HashMap<String, ContentHash> {
    manifest
        .present_paths()
        .map(|(path, meta)| (path.clone(), meta.hash))
        .collect()
}

/// Build the initial receipt from bytes that are actually on disk and still
/// match the persisted manifest. A persisted remote Present with no local bytes
/// was never materialized, so it must not be treated as deletion evidence after
/// restart; a new or changed disk file is deliberately omitted so the first scan
/// records it as a local write.
fn observed_from_disk(
    manifest: &Manifest,
    entry: &SyncEntry,
) -> Result<HashMap<String, ContentHash>> {
    let mut observed = HashMap::new();
    for file in scan_folder(&entry.folder, entry, manifest)? {
        let hash = file.hash;
        if manifest
            .get(&file.rel)
            .and_then(|entry| entry.meta())
            .is_some_and(|meta| meta.hash == hash)
        {
            observed.insert(file.rel, hash);
        }
    }
    Ok(observed)
}

/// Scan against the last state actually observed on disk. Manifest-only Present
/// entries may have arrived from a concurrent reconcile and must not become
/// tombstones merely because they have not been materialized yet.
fn scan_into_node_observed(
    node: &mut SyncNode,
    root: &Path,
    entry: &SyncEntry,
    policy: PolicyRules,
    observed: &mut HashMap<String, ContentHash>,
) -> Result<bool> {
    let scanned = scan_folder(root, entry, node.manifest())?;
    let previous = observed.clone();
    let mut current = HashMap::new();
    let mut changed = false;
    for file in &scanned {
        let hash = file.hash;
        current.insert(file.rel.clone(), hash);
        if previous.get(&file.rel) == Some(&hash) {
            // Catalog is a union-of-presence policy. A Tombstone may still be
            // inherited from an older bus configuration or a peer running an
            // older policy. If unchanged physical bytes survive on any catalog
            // node, advance them to a higher Present so the manifest, wire
            // state, persisted state, and every materialized folder converge.
            // Bus deliberately keeps the Tombstone authoritative instead.
            if !policy.propagate_deletes
                && node
                    .manifest()
                    .get(&file.rel)
                    .is_some_and(|entry| !entry.is_present())
            {
                if node.local_write(
                    &file.rel,
                    &file.read_bytes()?,
                    file.mtime_secs,
                    file.mtime_nanos,
                ) {
                    changed = true;
                }
                continue;
            }
            // Refill content after restart only when the manifest still names
            // these exact bytes. If a peer changed the entry while its old disk
            // bytes remained, those bytes are stale rather than a local edit.
            //
            // "After restart" is the whole point: content the node already holds
            // must not be read and re-hashed again. Without the get_content
            // check this refill re-read every unchanged file on every scan,
            // which kept BLAKE3 at the top of the profile even once scan_folder
            // stopped hashing, because read_bytes here undid that saving.
            if node.get_content(&hash).is_none()
                && node
                    .manifest()
                    .get(&file.rel)
                    .and_then(|entry| entry.meta())
                    .is_some_and(|meta| meta.hash == hash)
            {
                node.put_content(file.read_bytes()?);
            }
        } else if node.local_write(
            &file.rel,
            &file.read_bytes()?,
            file.mtime_secs,
            file.mtime_nanos,
        ) {
            changed = true;
        }
    }

    let now = now_secs();
    for path in previous.keys() {
        if !current.contains_key(path) && node.local_remove(path, policy, now) {
            changed = true;
        }
    }
    *observed = current;
    Ok(changed)
}

/// Write `node`'s present entries to disk (only where content differs) and, under
/// a delete-propagating policy, remove tombstoned files.
#[cfg(test)]
fn materialize(node: &SyncNode, root: &Path, policy: PolicyRules) -> Result<()> {
    std::fs::create_dir_all(root)
        .with_context(|| format!("failed to create {}", root.display()))?;
    for (rel, meta) in node.manifest().present_paths() {
        let Some(bytes) = node.get_content(&meta.hash) else {
            continue; // content not held yet; a reconcile will fetch it
        };
        let path = root.join(rel);
        let needs_write = match std::fs::read(&path) {
            Ok(existing) => content_hash(&existing) != meta.hash,
            Err(_) => true,
        };
        if needs_write {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // Do NOT stamp the origin's mtime here.
            //
            // Stamping it was an optimisation so scan_folder's size-and-mtime cache
            // would hit on materialized files. It made mtime a value SHARED across
            // nodes instead of a local write timestamp, and that broke the cache's
            // core assumption. Two nodes contending on one key can produce entries
            // with identical size and identical mtime but different content, and the
            // scan then reuses the recorded hash and reports content the file does
            // not hold. The republish branch reads the real bytes, sees a different
            // hash, concludes the user edited the file, and republishes under local
            // authorship. Both sides do it and the versions leapfrog forever.
            //
            // Measured on Linux CI run 30814462024 attempt 4: twenty-four
            // consecutive republishes alternating between two nodes, every one with
            // prior manifest hash equal to the scanned hash yet a different hash
            // after, from v44 to v67 with no convergence.
            //
            // A materialized file now carries the local write time, so the cache
            // misses on it and the next scan reads and hashes the real bytes. The
            // cache still hits for genuinely untouched local files, which is where
            // its benefit actually was.
            write_atomic(&path, bytes)?;
        }
    }
    if policy.propagate_deletes {
        for (rel, entry) in node.manifest().entries() {
            if !entry.is_present() {
                let _ = std::fs::remove_file(root.join(rel));
            }
        }
    }
    Ok(())
}

/// Materialize a reconcile result with a final local-delete guard.
///
/// The post-merge scan closes the broad wire-session window. This final
/// existence check closes the narrower scan-to-write window: if a path that was
/// locally Present before the merge vanished after the scan, record its
/// tombstone instead of restoring it. A remote-only path is not in `protected`,
/// so an absent destination is still materialized normally.
fn materialize_tracked(
    node: &mut SyncNode,
    root: &Path,
    policy: PolicyRules,
    protected: &HashMap<String, ContentHash>,
    observed: &mut HashMap<String, ContentHash>,
    daemon_writes: Option<(&EntryWork, u64)>,
) -> Result<()> {
    std::fs::create_dir_all(root)
        .with_context(|| format!("failed to create {}", root.display()))?;
    let present: Vec<_> = node
        .manifest()
        .present_paths()
        .map(|(rel, meta)| (rel.clone(), *meta))
        .collect();
    let now = now_secs();
    for (rel, meta) in present {
        let path = root.join(&rel);
        let existing = std::fs::read(&path);
        let protected_local_path = protected.contains_key(&rel);
        if policy.propagate_deletes
            && protected_local_path
            && matches!(
                &existing,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound
            )
        {
            node.local_remove(&rel, policy, now);
            observed.remove(&rel);
            continue;
        }

        let needs_write = match existing {
            Ok(existing) => {
                let existing_hash = content_hash(&existing);
                if existing_hash == meta.hash {
                    observed.insert(rel.clone(), meta.hash);
                    false
                } else if protected.get(&rel) != Some(&existing_hash) {
                    // The bytes changed (or appeared) after the protected disk
                    // receipt was captured. This is a concurrent local edit,
                    // not stale content to overwrite with the remote Present.
                    node.local_write(&rel, &existing, 0, 0);
                    observed.insert(rel.clone(), existing_hash);
                    false
                } else {
                    true
                }
            }
            Err(_) => true,
        };
        if needs_write {
            let Some(bytes) = node.get_content(&meta.hash) else {
                continue; // content not held yet; a reconcile will fetch it
            };
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // Do NOT stamp the origin's mtime here.
            //
            // Stamping it was an optimisation so scan_folder's size-and-mtime cache
            // would hit on materialized files. It made mtime a value SHARED across
            // nodes instead of a local write timestamp, and that broke the cache's
            // core assumption. Two nodes contending on one key can produce entries
            // with identical size and identical mtime but different content, and the
            // scan then reuses the recorded hash and reports content the file does
            // not hold. The republish branch reads the real bytes, sees a different
            // hash, concludes the user edited the file, and republishes under local
            // authorship. Both sides do it and the versions leapfrog forever.
            //
            // Measured on Linux CI run 30814462024 attempt 4: twenty-four
            // consecutive republishes alternating between two nodes, every one with
            // prior manifest hash equal to the scanned hash yet a different hash
            // after, from v44 to v67 with no convergence.
            //
            // A materialized file now carries the local write time, so the cache
            // misses on it and the next scan reads and hashes the real bytes. The
            // cache still hits for genuinely untouched local files, which is where
            // its benefit actually was.
            write_atomic(&path, bytes)?;
            if let Some((work, generation)) = daemon_writes {
                work.record_daemon_write(&path, meta.hash, generation);
            }
            observed.insert(rel, meta.hash);
        }
    }
    if policy.propagate_deletes {
        for (rel, entry) in node.manifest().entries() {
            if !entry.is_present() {
                let _ = std::fs::remove_file(root.join(rel));
                observed.remove(rel);
            }
        }
    }
    Ok(())
}

/// Write bytes atomically: to a temp sibling, then rename over the target.
///
/// A materialized file keeps the receiver's own write time. It is NOT stamped
/// with the sender's mtime, and that is deliberate.
///
/// Stamping used to happen here, and it caused a permanent three-node
/// divergence. The scan cache treats a file as unchanged when its size and mtime
/// match the recorded metadata. Stamping made mtime a cross-node value, so two
/// contending entries of equal size could collide on size plus mtime, and the
/// cache then reported content the file did not actually hold. Versions
/// leapfrogged and never converged. Commit 62a30b8 removed the stamping; the
/// cost is one re-read per materialized file, which is the cheap side of that
/// trade.
///
/// The consequence is that fabric does not replicate an mtime at all today, so a
/// consumer must not read a replica's mtime as an activity signal. See issue 27:
/// st2 derived agent liveness that way and reported live remote agents as
/// unknown.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension(format!(
        "{}.fabric-tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("")
    ));
    std::fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to rename into {}", path.display()))?;
    Ok(())
}

/// Set a file's modification time, leaving its access time alone.
///
/// Test-only since 62a30b8 removed mtime stamping from materialization. The
/// production path deliberately never sets an mtime, so the only caller left is
/// the test that reconstructs the size-plus-mtime collision that stamping used
/// to manufacture.
#[cfg(test)]
fn set_file_mtime(path: &Path, secs: i64, nanos: u32) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("path is not a valid C string: {}", path.display()))?;
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: secs as libc::time_t,
            tv_nsec: nanos as libc::c_long,
        },
    ];
    // SAFETY: c_path is a valid NUL-terminated path and times is a two-element
    // timespec array, which is exactly what utimensat expects.
    let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn mtime_of(entry: &std::fs::DirEntry) -> (i64, u32) {
    let Ok(meta) = entry.metadata() else {
        return (0, 0);
    };
    let Ok(modified) = meta.modified() else {
        return (0, 0);
    };
    match modified.duration_since(UNIX_EPOCH) {
        Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos()),
        Err(err) => (-(err.duration().as_secs() as i64), 0),
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Make a sync name safe to use as a directory component for its manifest store.
fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Start a recursive filesystem watcher on `root`, forwarding a unit signal on
/// every event. The returned watcher must be kept alive for events to flow.
fn watcher_event_is_mutation(kind: &notify::EventKind) -> bool {
    matches!(
        kind,
        notify::EventKind::Create(_) | notify::EventKind::Modify(_) | notify::EventKind::Remove(_)
    )
}

fn watcher_event_can_match_daemon_write(kind: &notify::EventKind) -> bool {
    use notify::event::ModifyKind;

    matches!(
        kind,
        notify::EventKind::Create(_)
            | notify::EventKind::Modify(
                ModifyKind::Any | ModifyKind::Data(_) | ModifyKind::Metadata(_) | ModifyKind::Other
            )
    )
}

#[derive(Debug)]
struct WatchEvent {
    paths: Vec<PathBuf>,
    generation: u64,
    daemon_write_candidate: bool,
}

#[derive(Debug)]
struct WatchEventBatch {
    paths: HashSet<PathBuf>,
    first_generation: u64,
    last_generation: u64,
    contiguous: bool,
    daemon_write_candidate: bool,
}

impl WatchEventBatch {
    fn new(event: WatchEvent) -> Self {
        Self {
            paths: event.paths.into_iter().collect(),
            first_generation: event.generation,
            last_generation: event.generation,
            contiguous: true,
            daemon_write_candidate: event.daemon_write_candidate,
        }
    }

    fn push(&mut self, event: WatchEvent) {
        self.contiguous &= self.last_generation.checked_add(1) == Some(event.generation);
        self.last_generation = event.generation;
        self.daemon_write_candidate &= event.daemon_write_candidate;
        self.paths.extend(event.paths);
    }
}

fn spawn_watcher(
    root: &Path,
    tx: mpsc::Sender<WatchEvent>,
    work: Arc<EntryWork>,
) -> Option<notify::RecommendedWatcher> {
    use notify::{RecursiveMode, Watcher};

    let mut watcher =
        match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res
                && watcher_event_is_mutation(&event.kind)
            {
                let generation = work.record_mutation();
                let _ = tx.try_send(WatchEvent {
                    paths: event.paths,
                    generation,
                    daemon_write_candidate: watcher_event_can_match_daemon_write(&event.kind),
                });
            }
        }) {
            Ok(watcher) => watcher,
            Err(error) => {
                tracing::warn!(root = %root.display(), %error, "failed to create fs watcher");
                return None;
            }
        };
    // Create the folder first so watching it succeeds.
    let _ = std::fs::create_dir_all(root);
    if let Err(error) = watcher.watch(root, RecursiveMode::Recursive) {
        tracing::warn!(root = %root.display(), %error, "failed to watch folder");
        return None;
    }
    Some(watcher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::config::SyncPolicy;
    use crate::sync::manifest::Entry;
    use std::sync::{Mutex as StdMutex, Weak};

    #[test]
    fn materialized_file_is_re_read_once_on_the_next_scan() -> Result<()> {
        // The live miss the synthetic test could not see: a file received from a
        // peer carries the SENDER's mtime in the manifest, while the local
        // filesystem stamps its own at write time. Without restoring the
        // recorded mtime, every file this node ever received misses the scan
        // cache forever and is re-read and re-hashed on every scan.
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        let entry = entry_with_policy("bus", root, SyncPolicy::Bus);

        // A remote-authored Present whose mtime is deliberately not "now".
        let remote_mtime_secs = 1_600_000_000i64;
        let remote_mtime_nanos = 123_456_789u32;
        let mut node = SyncNode::new(Author([9u8; 32]));
        node.local_write(
            "from-peer.md",
            b"content that arrived over the wire",
            remote_mtime_secs,
            remote_mtime_nanos,
        );

        let mut observed = HashMap::new();
        materialize_tracked(
            &mut node,
            root,
            entry.policy.rules(),
            &HashMap::new(),
            &mut observed,
            None,
        )?;
        assert!(root.join("from-peer.md").exists(), "file was materialized");

        let scanned = scan_folder(root, &entry, node.manifest())?;
        let file = scanned
            .iter()
            .find(|f| f.rel == "from-peer.md")
            .expect("materialized file is in scope");
        // This test previously asserted the opposite, and that assertion was the
        // optimisation that caused a permanent three-node divergence. Stamping the
        // origin's mtime made the scan cache hit on materialized files, but it also
        // made mtime a value shared across nodes, so two contending entries of equal
        // size could collide on size and mtime with different content and the cache
        // would report bytes the file did not hold.
        //
        // The deliberate cost of correctness, and the performance evidence for it: a
        // materialized file carries the LOCAL write time, so it is read and hashed
        // once on the next scan. Genuinely untouched local files still hit the cache,
        // which is where its benefit actually was.
        assert_ne!(
            (file.mtime_secs, file.mtime_nanos),
            (remote_mtime_secs, remote_mtime_nanos),
            "materialize must NOT stamp the origin mtime"
        );
        assert!(
            file.bytes.is_some(),
            "a freshly materialized file is re-read once; that is the cost of not \
             trusting a shared mtime"
        );
        Ok(())
    }

    #[test]
    fn converged_rescan_reuses_recorded_hashes_instead_of_rereading() -> Result<()> {
        // The inbound completion scan is mandatory for correctness, so the cost
        // of a converged rescan is what matters. A profile of the live daemon
        // put BLAKE3 at the top of the on-CPU work, because every scan re-read
        // and re-hashed the whole tree. An unchanged file must now be recognised
        // by size and mtime alone and carry no bytes.
        let temp = tempfile::tempdir()?;
        let root = temp.path();
        std::fs::write(root.join("a.txt"), b"alpha")?;
        std::fs::write(root.join("b.txt"), b"beta")?;
        let entry = entry_with_policy("bus", root, SyncPolicy::Bus);

        let first = scan_folder(root, &entry, &Manifest::default())?;
        assert_eq!(first.len(), 2);
        assert!(
            first.iter().all(|file| file.bytes.is_some()),
            "an unknown file must be read and hashed"
        );

        // Record what that scan learned, the way a real reconcile would.
        let mut node = SyncNode::new(Author([7u8; 32]));
        for file in &first {
            node.local_write(
                &file.rel,
                file.bytes.as_ref().expect("first scan reads bytes"),
                file.mtime_secs,
                file.mtime_nanos,
            );
        }

        let second = scan_folder(root, &entry, node.manifest())?;
        assert_eq!(second.len(), 2);
        assert!(
            second.iter().all(|file| file.bytes.is_none()),
            "a converged rescan must not re-read unchanged files"
        );
        let before: HashMap<_, _> = first.iter().map(|f| (f.rel.clone(), f.hash)).collect();
        let after: HashMap<_, _> = second.iter().map(|f| (f.rel.clone(), f.hash)).collect();
        assert_eq!(before, after, "reused hashes must match what was recorded");

        // A changed file is read and hashed again.
        std::thread::sleep(Duration::from_millis(20));
        std::fs::write(root.join("a.txt"), b"different content")?;
        let third = scan_folder(root, &entry, node.manifest())?;
        let changed = third
            .iter()
            .find(|file| file.rel == "a.txt")
            .expect("a.txt is still in scope");
        assert!(
            changed.bytes.is_some(),
            "a changed file must be re-read and re-hashed"
        );
        assert_eq!(changed.hash, content_hash(b"different content"));
        let untouched = third
            .iter()
            .find(|file| file.rel == "b.txt")
            .expect("b.txt is still in scope");
        assert!(
            untouched.bytes.is_none(),
            "one changed file must not force its neighbours to be re-read"
        );
        Ok(())
    }

    #[test]
    fn periodic_scan_decision_covers_clean_dirty_and_safety_paths() {
        assert!(!periodic_scan_due(false, false));
        assert!(periodic_scan_due(true, false));
        assert!(periodic_scan_due(false, true));
    }

    fn entry_with_policy(name: &str, folder: &Path, policy: SyncPolicy) -> SyncEntry {
        SyncEntry {
            name: name.to_string(),
            folder: folder.to_path_buf(),
            peers: SyncPeers::Wildcard("*".into()),
            policy,
            include: None,
        }
    }

    fn catalog_entry(name: &str, folder: &Path) -> SyncEntry {
        entry_with_policy(name, folder, SyncPolicy::Catalog)
    }

    #[test]
    fn watcher_wakes_only_for_mutations() {
        use notify::event::{AccessKind, AccessMode, CreateKind, ModifyKind, RemoveKind};

        assert!(!watcher_event_is_mutation(&notify::EventKind::Access(
            AccessKind::Open(AccessMode::Read)
        )));
        assert!(!watcher_event_is_mutation(&notify::EventKind::Any));
        assert!(!watcher_event_is_mutation(&notify::EventKind::Other));
        assert!(watcher_event_is_mutation(&notify::EventKind::Create(
            CreateKind::Any
        )));
        assert!(watcher_event_is_mutation(&notify::EventKind::Modify(
            ModifyKind::Any
        )));
        assert!(watcher_event_is_mutation(&notify::EventKind::Remove(
            RemoveKind::Any
        )));
        assert!(watcher_event_can_match_daemon_write(
            &notify::EventKind::Modify(ModifyKind::Data(notify::event::DataChange::Any))
        ));
        assert!(!watcher_event_can_match_daemon_write(
            &notify::EventKind::Modify(ModifyKind::Name(notify::event::RenameMode::Any))
        ));
        assert!(!watcher_event_can_match_daemon_write(
            &notify::EventKind::Remove(RemoveKind::Any)
        ));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_file_reads_do_not_wake_watcher_but_writes_do() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watched.txt");
        std::fs::write(&path, b"seed").unwrap();
        let (tx, mut rx) = mpsc::channel::<WatchEvent>(1);
        let work = EntryWork::new();
        let _watcher = spawn_watcher(dir.path(), tx, work.clone()).unwrap();
        let generation = work.mutation_generation.load(Ordering::Acquire);

        assert_eq!(std::fs::read(&path).unwrap(), b"seed");
        assert!(
            tokio::time::timeout(Duration::from_millis(250), rx.recv())
                .await
                .is_err(),
            "Linux OPEN/read access must not wake the sync watcher"
        );
        assert_eq!(
            work.mutation_generation.load(Ordering::Acquire),
            generation,
            "access events must not advance the mutation generation"
        );

        std::fs::write(&path, b"changed").unwrap();
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("a real write must wake the sync watcher")
            .expect("watcher channel closed");
        assert_eq!(event.paths, vec![path]);
        assert!(
            work.mutation_generation.load(Ordering::Acquire) > generation,
            "a mutation must advance the generation"
        );
    }

    #[test]
    fn delayed_daemon_materialization_event_is_acknowledged_without_rescan() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let path = root.join("remote.md");
        let mut node = SyncNode::new(Author([1; 32]));
        node.local_write("remote.md", b"remote bytes", 0, 0);
        let mut observed = HashMap::new();
        let work = EntryWork::new();
        let generation = work.mutation_generation.load(Ordering::Acquire);

        materialize_tracked(
            &mut node,
            root,
            SyncPolicy::Bus.rules(),
            &HashMap::new(),
            &mut observed,
            Some((&work, generation)),
        )
        .unwrap();
        work.commit_daemon_writes();
        work.mark_generation_durable(generation);
        assert_eq!(std::fs::read(&path).unwrap(), b"remote bytes");

        // Model a notify event delivered after materialization and persistence.
        // The callback has already advanced the mutation generation.
        let first_event_generation = work.record_mutation();
        let mut batch = WatchEventBatch::new(WatchEvent {
            paths: vec![path.clone()],
            generation: first_event_generation,
            daemon_write_candidate: true,
        });
        let second_event_generation = work.record_mutation();
        batch.push(WatchEvent {
            paths: vec![path],
            generation: second_event_generation,
            daemon_write_candidate: true,
        });
        assert!(work.acknowledge_daemon_write_batch(&batch));
        assert_eq!(
            work.mutation_generation.load(Ordering::Acquire),
            work.durable_generation.load(Ordering::Acquire),
            "an exact delayed self-event must not leave periodic work dirty"
        );
        assert_eq!(
            work.full_scans.load(Ordering::Relaxed),
            0,
            "the delayed daemon-owned batch must not rescan the tree"
        );
    }

    #[tokio::test]
    async fn same_path_external_mutation_after_materialization_stays_dirty_and_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        let path = root.join("remote.md");
        write_bus_sync(dir.path(), &root);
        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        entry
            .node
            .lock()
            .await
            .local_write("remote.md", b"daemon bytes", 0, 0);
        let generation = entry.work.mutation_generation.load(Ordering::Acquire);
        engine
            .materialize_entry_state(&entry, &HashMap::new())
            .await
            .unwrap();
        engine.persist_entry(&entry).await.unwrap();
        entry.work.mark_generation_durable(generation);
        entry.work.full_scans.store(0, Ordering::Relaxed);

        // An external write lands before the delayed watcher event is handled.
        // Its current identity no longer matches the daemon's post-write record.
        std::fs::write(&path, b"external bytes").unwrap();
        let event_generation = entry.work.record_mutation();
        let batch = WatchEventBatch::new(WatchEvent {
            paths: vec![path],
            generation: event_generation,
            daemon_write_candidate: true,
        });
        assert!(!entry.work.acknowledge_daemon_write_batch(&batch));
        assert_ne!(
            entry.work.mutation_generation.load(Ordering::Acquire),
            entry.work.durable_generation.load(Ordering::Acquire),
            "an immediate external mutation must still schedule a scan"
        );
        assert!(periodic_scan_due(true, false));
        engine.sync_once("bus").await.unwrap();
        assert!(
            entry.work.full_scans.load(Ordering::Relaxed) >= 2,
            "the non-suppressed event must take the normal sync scan path"
        );
        let node = entry.node.lock().await;
        assert_eq!(
            node.manifest()
                .get("remote.md")
                .and_then(|entry| entry.meta())
                .map(|meta| meta.hash),
            Some(content_hash(b"external bytes"))
        );
    }

    #[test]
    fn dropped_watcher_generation_cannot_suppress_daemon_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("remote.md");
        std::fs::write(&path, b"daemon bytes").unwrap();
        let work = EntryWork::new();
        let generation = work.mutation_generation.load(Ordering::Acquire);
        work.record_daemon_write(&path, content_hash(b"daemon bytes"), generation);
        work.commit_daemon_writes();
        work.mark_generation_durable(generation);

        let delivered_generation = work.record_mutation();
        let batch = WatchEventBatch::new(WatchEvent {
            paths: vec![path],
            generation: delivered_generation,
            daemon_write_candidate: true,
        });
        let _dropped_generation = work.record_mutation();

        assert!(!work.acknowledge_daemon_write_batch(&batch));
        assert_ne!(
            work.mutation_generation.load(Ordering::Acquire),
            work.durable_generation.load(Ordering::Acquire),
            "a generation dropped by the bounded channel must remain dirty"
        );
    }

    #[test]
    fn daemon_write_journal_overflow_fails_open_to_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oldest.md");
        std::fs::write(&path, b"daemon bytes").unwrap();
        let work = EntryWork::new();
        let generation = work.mutation_generation.load(Ordering::Acquire);
        let fingerprint = FileFingerprint::read(&path).unwrap();
        {
            let mut journal = work.daemon_writes.lock().unwrap();
            journal.record(path.clone(), fingerprint.clone(), generation);
            for index in 0..MAX_DAEMON_WRITE_FINGERPRINTS {
                journal.record(
                    dir.path().join(format!("newer-{index}.md")),
                    fingerprint.clone(),
                    generation,
                );
            }
            assert_eq!(journal.order.len(), MAX_DAEMON_WRITE_FINGERPRINTS);
            assert_eq!(journal.entries.len(), MAX_DAEMON_WRITE_FINGERPRINTS);
            assert!(!journal.entries.contains_key(&path));
        }
        work.commit_daemon_writes();
        work.mark_generation_durable(generation);

        let event_generation = work.record_mutation();
        let batch = WatchEventBatch::new(WatchEvent {
            paths: vec![path],
            generation: event_generation,
            daemon_write_candidate: true,
        });
        assert!(!work.acknowledge_daemon_write_batch(&batch));
        assert_ne!(
            work.mutation_generation.load(Ordering::Acquire),
            work.durable_generation.load(Ordering::Acquire)
        );
    }

    #[test]
    fn rename_remove_and_stat_failure_events_stay_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("remote.md");
        std::fs::write(&path, b"daemon bytes").unwrap();
        let work = EntryWork::new();

        let unsafe_kinds = [
            notify::EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Any,
            )),
            notify::EventKind::Remove(notify::event::RemoveKind::Any),
        ];
        for kind in unsafe_kinds {
            let daemon_write_candidate = watcher_event_can_match_daemon_write(&kind);
            assert!(!daemon_write_candidate);
            let generation = work.mutation_generation.load(Ordering::Acquire);
            work.record_daemon_write(&path, content_hash(b"daemon bytes"), generation);
            work.commit_daemon_writes();
            work.mark_generation_durable(generation);
            let event_generation = work.record_mutation();
            let batch = WatchEventBatch::new(WatchEvent {
                paths: vec![path.clone()],
                generation: event_generation,
                daemon_write_candidate,
            });
            assert!(!work.acknowledge_daemon_write_batch(&batch));
            assert_ne!(
                work.mutation_generation.load(Ordering::Acquire),
                work.durable_generation.load(Ordering::Acquire)
            );
        }

        let generation = work.mutation_generation.load(Ordering::Acquire);
        work.record_daemon_write(&path, content_hash(b"daemon bytes"), generation);
        work.commit_daemon_writes();
        work.mark_generation_durable(generation);
        std::fs::remove_file(&path).unwrap();
        let event_generation = work.record_mutation();
        let batch = WatchEventBatch::new(WatchEvent {
            paths: vec![path],
            generation: event_generation,
            // Exercise the stat-failure branch independently of remove-kind
            // classification.
            daemon_write_candidate: true,
        });
        assert!(!work.acknowledge_daemon_write_batch(&batch));
        assert_ne!(
            work.mutation_generation.load(Ordering::Acquire),
            work.durable_generation.load(Ordering::Acquire)
        );
    }

    #[tokio::test]
    async fn continuous_events_are_bounded_by_max_coalesce_window() {
        let (tx, mut rx) = mpsc::channel(1);
        let sender = tokio::spawn(async move {
            for generation in 1..=40 {
                let _ = tx
                    .send(WatchEvent {
                        paths: Vec::new(),
                        generation,
                        daemon_write_candidate: false,
                    })
                    .await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let started = tokio::time::Instant::now();
        assert!(
            coalesce_watch_events(
                rx.recv().await.unwrap(),
                &mut rx,
                Duration::from_millis(20),
                Duration::from_millis(100),
            )
            .await
            .is_some()
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(80) && elapsed < Duration::from_millis(250),
            "continuous events escaped the bounded coalescer: {elapsed:?}"
        );
        sender.abort();
    }

    #[test]
    fn scan_then_materialize_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("a.toml"), b"aaa").unwrap();
        std::fs::create_dir_all(root.join("sub")).unwrap();
        std::fs::write(root.join("sub/b.toml"), b"bbb").unwrap();

        let entry = catalog_entry("cat", root);
        let mut node = SyncNode::new(Author([1; 32]));
        assert!(scan_into_node(&mut node, root, &entry, entry.policy.rules()).unwrap());
        assert_eq!(node.manifest().present_paths().count(), 2);

        // Materialize into a fresh folder yields identical files.
        let dir2 = tempfile::tempdir().unwrap();
        materialize(&node, dir2.path(), entry.policy.rules()).unwrap();
        assert_eq!(std::fs::read(dir2.path().join("a.toml")).unwrap(), b"aaa");
        assert_eq!(
            std::fs::read(dir2.path().join("sub/b.toml")).unwrap(),
            b"bbb"
        );
    }

    #[test]
    fn include_glob_filters_scan() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("agent.toml"), b"x").unwrap();
        std::fs::write(root.join("notes.md"), b"y").unwrap();
        let mut entry = catalog_entry("cat", root);
        entry.include = Some(vec!["*.toml".into()]);

        let mut node = SyncNode::new(Author([1; 32]));
        scan_into_node(&mut node, root, &entry, entry.policy.rules()).unwrap();
        let paths: Vec<_> = node
            .manifest()
            .present_paths()
            .map(|(p, _)| p.clone())
            .collect();
        assert_eq!(paths, vec!["agent.toml".to_string()]);
    }

    #[test]
    fn catalog_scan_ignores_local_delete_and_materialize_restores() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("keep.toml"), b"payload").unwrap();
        let entry = catalog_entry("cat", root);
        let policy = entry.policy.rules();

        let mut node = SyncNode::new(Author([1; 32]));
        scan_into_node(&mut node, root, &entry, policy).unwrap();

        // Delete on disk, rescan: catalog records no change.
        std::fs::remove_file(root.join("keep.toml")).unwrap();
        let changed = scan_into_node(&mut node, root, &entry, policy).unwrap();
        assert!(!changed, "catalog delete must not change the manifest");
        // Materialize restores the file from the retained content.
        materialize(&node, root, policy).unwrap();
        assert_eq!(std::fs::read(root.join("keep.toml")).unwrap(), b"payload");
    }

    #[test]
    fn authoritative_tombstone_with_stale_observed_file_respects_policy() {
        for policy in [SyncPolicy::Catalog, SyncPolicy::Bus] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let path = root.join("retired.toml");
            std::fs::write(&path, b"stale bytes").unwrap();
            let entry = entry_with_policy("sync", root, policy);
            let rules = policy.rules();

            // Model an authoritative Tombstone/v2 received while the exact
            // previously-observed bytes still physically exist.
            let mut node = SyncNode::new(Author([1; 32]));
            node.local_write("retired.toml", b"stale bytes", 0, 0);
            node.local_remove("retired.toml", SyncPolicy::Bus.rules(), 10);
            let stale_hash = content_hash(b"stale bytes");
            let mut observed = HashMap::from([("retired.toml".to_string(), stale_hash)]);
            let protected = observed.clone();

            let changed =
                scan_into_node_observed(&mut node, root, &entry, rules, &mut observed).unwrap();
            assert_eq!(changed, policy == SyncPolicy::Catalog);
            if policy == SyncPolicy::Catalog {
                assert!(matches!(
                    node.manifest().get("retired.toml"),
                    Some(Entry::Present(meta))
                        if meta.version == 3 && meta.hash == stale_hash
                ));
            } else {
                assert!(matches!(
                    node.manifest().get("retired.toml"),
                    Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
                ));
            }
            assert_eq!(observed.get("retired.toml"), Some(&stale_hash));

            materialize_tracked(&mut node, root, rules, &protected, &mut observed, None).unwrap();
            let file_survives = policy == SyncPolicy::Catalog;
            assert_eq!(path.exists(), file_survives, "policy {policy:?}");
            assert_eq!(
                observed.contains_key("retired.toml"),
                file_survives,
                "policy {policy:?}"
            );
        }
    }

    #[test]
    fn changed_physical_file_after_tombstone_becomes_higher_present() {
        for policy in [SyncPolicy::Catalog, SyncPolicy::Bus] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path();
            let path = root.join("retired.toml");
            std::fs::write(&path, b"old").unwrap();
            let entry = entry_with_policy("sync", root, policy);

            let mut node = SyncNode::new(Author([1; 32]));
            node.local_write("retired.toml", b"old", 0, 0);
            node.local_remove("retired.toml", SyncPolicy::Bus.rules(), 10);
            let mut observed = HashMap::from([("retired.toml".to_string(), content_hash(b"old"))]);

            // A byte change is a new local intent, unlike the unchanged stale
            // file above, and must advance causally beyond Tombstone/v2.
            std::fs::write(&path, b"resurrected").unwrap();
            assert!(
                scan_into_node_observed(&mut node, root, &entry, policy.rules(), &mut observed,)
                    .unwrap()
            );
            assert!(matches!(
                node.manifest().get("retired.toml"),
                Some(Entry::Present(meta))
                    if meta.version == 3 && meta.hash == content_hash(b"resurrected")
            ));
            assert_eq!(
                observed.get("retired.toml"),
                Some(&content_hash(b"resurrected"))
            );
        }
    }

    // A loopback transport: each peer is a (id, sync-name, node) captured
    // directly, so reconcile drives a real framed wire session over an in-memory
    // duplex — exactly the iroh transport's path, minus the network.
    struct LoopPeer {
        id: String,
        name: String,
        node: Arc<Mutex<SyncNode>>,
    }

    #[derive(Clone, Default)]
    struct LoopbackTransport {
        peers: Arc<StdMutex<Vec<LoopPeer>>>,
    }

    impl LoopbackTransport {
        fn add_peer(&self, id: &str, name: &str, node: Arc<Mutex<SyncNode>>) {
            self.peers.lock().unwrap().push(LoopPeer {
                id: id.to_string(),
                name: name.to_string(),
                node,
            });
        }
    }

    impl SyncTransport for LoopbackTransport {
        async fn peers_for(&self, _peers: &SyncPeers) -> Vec<PeerRef> {
            self.peers
                .lock()
                .unwrap()
                .iter()
                .map(|peer| PeerRef {
                    id: peer.id.clone(),
                    addr: None,
                })
                .collect()
        }

        async fn reconcile(
            &self,
            peer: PeerRef,
            name: String,
            node: Arc<Mutex<SyncNode>>,
        ) -> Result<Reconciled> {
            let target = {
                let peers = self.peers.lock().unwrap();
                peers
                    .iter()
                    .find(|p| p.id == peer.id && p.name == name)
                    .map(|p| p.node.clone())
            };
            let Some(target) = target else {
                return Ok(Reconciled::default());
            };
            let (client_end, server_end) = tokio::io::duplex(1 << 20);
            let server_name = name.clone();
            let server = tokio::spawn(async move {
                crate::sync::wire::run_server(server_end, move |n, _| async move {
                    Ok(if n == server_name {
                        Some((target, ()))
                    } else {
                        None
                    })
                })
                .await
            });
            let stats = crate::sync::wire::run_client(client_end, node, &name).await?;
            let _ = server.await;
            Ok(stats)
        }
    }

    struct EnginePeer {
        id: String,
        engine: Weak<SyncEngine<EngineLoopbackTransport>>,
    }

    /// Loopback transport that exercises the production inbound engine hooks,
    /// including the entry operation guard, rather than routing straight to a
    /// bare node like `LoopbackTransport`.
    #[derive(Default)]
    struct EngineLoopbackTransport {
        peers: StdMutex<Vec<EnginePeer>>,
        reconciles: AtomicUsize,
    }

    impl EngineLoopbackTransport {
        fn add_peer(&self, id: &str, engine: &Arc<SyncEngine<Self>>) {
            self.peers.lock().unwrap().push(EnginePeer {
                id: id.to_string(),
                engine: Arc::downgrade(engine),
            });
        }

        fn reset_reconciles(&self) {
            self.reconciles.store(0, Ordering::Relaxed);
        }

        fn reconcile_count(&self) -> usize {
            self.reconciles.load(Ordering::Relaxed)
        }
    }

    impl SyncTransport for EngineLoopbackTransport {
        async fn peers_for(&self, _peers: &SyncPeers) -> Vec<PeerRef> {
            self.peers
                .lock()
                .unwrap()
                .iter()
                .filter(|peer| peer.engine.strong_count() > 0)
                .map(|peer| PeerRef {
                    id: peer.id.clone(),
                    addr: None,
                })
                .collect()
        }

        async fn reconcile(
            &self,
            peer: PeerRef,
            name: String,
            node: Arc<Mutex<SyncNode>>,
        ) -> Result<Reconciled> {
            self.reconciles.fetch_add(1, Ordering::Relaxed);
            let target = {
                let peers = self.peers.lock().unwrap();
                peers
                    .iter()
                    .find(|candidate| candidate.id == peer.id)
                    .and_then(|candidate| candidate.engine.upgrade())
            };
            let Some(target) = target else {
                return Ok(Reconciled::default());
            };

            let (client_end, server_end) = tokio::io::duplex(1 << 20);
            let resolver_target = target.clone();
            let server = tokio::spawn(async move {
                let (_, _, prepared) =
                    crate::sync::wire::run_server(server_end, move |requested, remote_manifest| {
                        let engine = resolver_target.clone();
                        async move {
                            let prepared = engine
                                .prepare_inbound_for_manifest(&requested, &remote_manifest)
                                .await?;
                            Ok(prepared.map(|prepared| (prepared.node(), prepared)))
                        }
                    })
                    .await?;
                target.complete_inbound(prepared).await
            });
            let stats = crate::sync::wire::run_client(client_end, node, &name).await?;
            server.await??;
            Ok(stats)
        }
    }

    fn write_named_sync(home: &Path, name: &str, folder: &Path, policy: SyncPolicy) {
        let toml = format!(
            "[[sync]]\nname = {name:?}\nfolder = {folder:?}\npeers = \"*\"\npolicy = {:?}\n",
            policy.as_str()
        );
        std::fs::write(home.join("syncs.toml"), toml).unwrap();
    }

    fn write_syncs(home: &Path, folder: &Path) {
        write_named_sync(home, "catalog", folder, SyncPolicy::Catalog);
    }

    fn write_bus_sync(home: &Path, folder: &Path) {
        write_named_sync(home, "bus", folder, SyncPolicy::Bus);
    }

    async fn run_inbound_wire_reconcile(
        engine: Arc<SyncEngine<LoopbackTransport>>,
        remote: Arc<Mutex<SyncNode>>,
    ) -> Reconciled {
        let (client_end, server_end) = tokio::io::duplex(1 << 20);
        let resolver_engine = engine.clone();
        let server = tokio::spawn(async move {
            let (_, stats, prepared) =
                crate::sync::wire::run_server(server_end, move |name, remote_manifest| {
                    let engine = resolver_engine.clone();
                    async move {
                        let prepared = engine
                            .prepare_inbound_for_manifest(&name, &remote_manifest)
                            .await?;
                        Ok(prepared.map(|prepared| (prepared.node(), prepared)))
                    }
                })
                .await?;
            engine.complete_inbound(prepared).await?;
            Ok::<_, anyhow::Error>(stats)
        });
        crate::sync::wire::run_client(client_end, remote, "bus")
            .await
            .unwrap();
        server.await.unwrap().unwrap()
    }

    async fn archive_race_fixture() -> (
        tempfile::TempDir,
        PathBuf,
        Arc<SyncEngine<LoopbackTransport>>,
        Arc<Mutex<SyncNode>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(root.join("inbox")).unwrap();
        std::fs::write(root.join("inbox/archived.md"), b"archive me").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();

        // Start the remote from the exact same stale Present entry and content,
        // then add one path that has never existed locally.
        let local_node = engine.node_for("bus").await.unwrap();
        let manifest = local_node.lock().await.manifest().clone();
        let mut remote = SyncNode::new(Author([2; 32]));
        remote.put_content(b"archive me".to_vec());
        remote.adopt(&manifest);
        remote.local_write("inbox/remote-new.md", b"new from peer", 0, 0);

        (dir, root, engine, Arc::new(Mutex::new(remote)))
    }

    async fn assert_archive_outcome(engine: &SyncEngine<LoopbackTransport>, root: &Path) {
        let node = engine.node_for("bus").await.unwrap();
        let node = node.lock().await;
        assert!(
            matches!(
                node.manifest().get("inbox/archived.md"),
                Some(Entry::Tombstone(_))
            ),
            "the local inbox removal must become a tombstone before merge"
        );
        assert!(
            matches!(
                node.manifest().get("archive/archived.md"),
                Some(Entry::Present(_))
            ),
            "the archive side of the local rename must remain present"
        );
        assert!(
            matches!(
                node.manifest().get("inbox/remote-new.md"),
                Some(Entry::Present(_))
            ),
            "a genuinely new remote file must not be falsely tombstoned"
        );
        drop(node);

        assert!(!root.join("inbox/archived.md").exists());
        assert_eq!(
            std::fs::read(root.join("archive/archived.md")).unwrap(),
            b"archive me"
        );
        assert_eq!(
            std::fs::read(root.join("inbox/remote-new.md")).unwrap(),
            b"new from peer"
        );
    }

    #[tokio::test]
    async fn queued_inbound_noops_reuse_one_durable_prepare_scan() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("shared.md"), b"stable").unwrap();
        write_bus_sync(dir.path(), &root);
        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        entry.work.full_scans.store(0, Ordering::Relaxed);
        entry.work.persist_calls.store(0, Ordering::Relaxed);

        let first = engine.prepare_inbound("bus").await.unwrap().unwrap();
        let second_engine = engine.clone();
        let second =
            tokio::spawn(
                async move { second_engine.prepare_inbound("bus").await.unwrap().unwrap() },
            );
        let third_engine = engine.clone();
        let third =
            tokio::spawn(
                async move { third_engine.prepare_inbound("bus").await.unwrap().unwrap() },
            );

        tokio::time::timeout(Duration::from_secs(1), async {
            while entry.work.inbound_waiters.load(Ordering::Acquire) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("inbound sessions did not queue behind the first operation");

        engine.complete_inbound(first).await.unwrap();
        engine
            .complete_inbound(second.await.unwrap())
            .await
            .unwrap();
        engine.complete_inbound(third.await.unwrap()).await.unwrap();

        assert_eq!(
            entry.work.full_scans.load(Ordering::Relaxed),
            4,
            "first prepare + all three completion guards should scan; queued prepares should not"
        );
        assert_eq!(
            entry.work.persist_calls.load(Ordering::Relaxed),
            0,
            "an already durable no-op generation must not rewrite state"
        );
    }

    #[tokio::test]
    async fn serial_converged_inbound_noops_do_not_rescan_clean_tree() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("shared.md"), b"stable").unwrap();
        write_bus_sync(dir.path(), &root);
        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        let manifest = entry.node.lock().await.manifest().clone();
        let mut remote = SyncNode::new(Author([2; 32]));
        remote.put_content(b"stable".to_vec());
        remote.adopt(&manifest);
        let remote = Arc::new(Mutex::new(remote));
        entry.work.full_scans.store(0, Ordering::Relaxed);
        entry
            .work
            .inbound_noop_transactions
            .store(0, Ordering::Relaxed);
        entry
            .work
            .inbound_guarded_transactions
            .store(0, Ordering::Relaxed);
        entry.work.persist_calls.store(0, Ordering::Relaxed);

        for _ in 0..2 {
            let stats = run_inbound_wire_reconcile(engine.clone(), remote.clone()).await;
            assert!(stats.is_noop());
        }

        assert_eq!(
            entry.work.full_scans.load(Ordering::Relaxed),
            0,
            "serial converged inbound no-ops must not rescan an unchanged tree"
        );
        assert_eq!(
            entry.work.persist_calls.load(Ordering::Relaxed),
            0,
            "serial converged inbound no-ops must not rewrite durable state"
        );
        let status = engine.status().await;
        let status = status.iter().find(|status| status.name == "bus").unwrap();
        assert_eq!(status.full_scans, 0);
        assert_eq!(status.inbound_noop_transactions, 2);
        assert_eq!(status.inbound_guarded_transactions, 0);

        engine.load_from_config().await.unwrap();
        let status = engine.status().await;
        let status = status.iter().find(|status| status.name == "bus").unwrap();
        assert_eq!(status.full_scans, 0);
        assert_eq!(status.inbound_noop_transactions, 2);
        assert_eq!(status.inbound_guarded_transactions, 0);
    }

    #[tokio::test]
    async fn converged_inbound_noop_never_overwrites_unobserved_local_edit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("shared.md"), b"stable").unwrap();
        write_bus_sync(dir.path(), &root);
        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        let manifest = entry.node.lock().await.manifest().clone();
        let mut remote = SyncNode::new(Author([2; 32]));
        remote.put_content(b"stable".to_vec());
        remote.adopt(&manifest);

        // Model a write that has reached disk but whose watcher callback has
        // not run yet. An exact remote manifest cannot cause materialization,
        // so the fast path must leave these bytes alone for the normal watcher
        // scan to version afterward.
        std::fs::write(root.join("shared.md"), b"local edit").unwrap();
        entry.work.full_scans.store(0, Ordering::Relaxed);
        let stats = run_inbound_wire_reconcile(engine.clone(), Arc::new(Mutex::new(remote))).await;

        assert!(stats.is_noop());
        assert_eq!(entry.work.full_scans.load(Ordering::Relaxed), 0);
        assert_eq!(
            std::fs::read(root.join("shared.md")).unwrap(),
            b"local edit"
        );

        engine.sync_once("bus").await.unwrap();
        let node = entry.node.lock().await;
        let meta = node.manifest().get("shared.md").unwrap().meta().unwrap();
        assert_eq!(meta.hash, content_hash(b"local edit"));
        assert_eq!(meta.version, 2);
    }

    #[tokio::test]
    async fn changed_inbound_manifest_keeps_guarded_scans() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("shared.md"), b"stable").unwrap();
        write_bus_sync(dir.path(), &root);
        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        let manifest = entry.node.lock().await.manifest().clone();
        let mut remote = SyncNode::new(Author([2; 32]));
        remote.put_content(b"stable".to_vec());
        remote.adopt(&manifest);
        remote.local_write("shared.md", b"remote edit", 0, 0);
        entry.work.full_scans.store(0, Ordering::Relaxed);
        entry
            .work
            .inbound_noop_transactions
            .store(0, Ordering::Relaxed);
        entry
            .work
            .inbound_guarded_transactions
            .store(0, Ordering::Relaxed);

        let stats = run_inbound_wire_reconcile(engine.clone(), Arc::new(Mutex::new(remote))).await;

        assert!(!stats.is_noop());
        assert_eq!(
            entry.work.full_scans.load(Ordering::Relaxed),
            2,
            "a differing manifest must retain pre-merge and completion scans"
        );
        assert_eq!(
            std::fs::read(root.join("shared.md")).unwrap(),
            b"remote edit"
        );
        let status = engine.status().await;
        let status = status.iter().find(|status| status.name == "bus").unwrap();
        assert_eq!(status.full_scans, 2);
        assert_eq!(status.inbound_noop_transactions, 0);
        assert_eq!(status.inbound_guarded_transactions, 1);
    }

    #[tokio::test]
    async fn converged_manifest_with_missing_local_content_keeps_guarded_scans() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        write_bus_sync(dir.path(), &root);
        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        let mut remote = SyncNode::new(Author([2; 32]));
        remote.local_write("remote-only.md", b"repair me", 0, 0);
        entry.node.lock().await.adopt(remote.manifest());
        assert_eq!(entry.node.lock().await.missing_content_hashes().len(), 1);
        entry.work.full_scans.store(0, Ordering::Relaxed);

        let stats = run_inbound_wire_reconcile(engine.clone(), Arc::new(Mutex::new(remote))).await;

        assert!(!stats.is_noop());
        assert_eq!(
            entry.work.full_scans.load(Ordering::Relaxed),
            2,
            "missing local content must retain pre-merge and completion scans"
        );
        assert_eq!(
            std::fs::read(root.join("remote-only.md")).unwrap(),
            b"repair me"
        );
    }

    #[tokio::test]
    async fn two_engines_sync_real_folders_over_loopback() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        write_syncs(dir_a.path(), &dir_a.path().join("catalog"));
        write_syncs(dir_b.path(), &dir_b.path().join("catalog"));

        let cancel = CancellationToken::new();
        let ta = Arc::new(LoopbackTransport::default());
        let tb = Arc::new(LoopbackTransport::default());
        let a = SyncEngine::new(
            FabricHome::new(dir_a.path()),
            Author([1; 32]),
            ta.clone(),
            cancel.clone(),
        )
        .await
        .unwrap();
        let b = SyncEngine::new(
            FabricHome::new(dir_b.path()),
            Author([2; 32]),
            tb.clone(),
            cancel.clone(),
        )
        .await
        .unwrap();

        // Wire each engine as the other's peer (nodes captured directly).
        ta.add_peer("b", "catalog", b.node_for("catalog").await.unwrap());
        tb.add_peer("a", "catalog", a.node_for("catalog").await.unwrap());

        // A drops a file (the hetz-proof shape) and syncs; B then pulls.
        std::fs::create_dir_all(dir_a.path().join("catalog")).unwrap();
        std::fs::write(dir_a.path().join("catalog/job.toml"), b"host=hetz").unwrap();
        a.sync_once("catalog").await.unwrap();
        b.sync_once("catalog").await.unwrap();

        assert_eq!(
            std::fs::read(dir_b.path().join("catalog/job.toml")).unwrap(),
            b"host=hetz"
        );

        // Converge fully, then a catalog delete on B must be restored (never
        // propagates a deletion back to A).
        a.sync_once("catalog").await.unwrap();
        std::fs::remove_file(dir_b.path().join("catalog/job.toml")).unwrap();
        b.sync_once("catalog").await.unwrap();
        assert!(
            std::fs::read(dir_b.path().join("catalog/job.toml")).is_ok(),
            "catalog delete should be restored on B"
        );
        assert!(
            std::fs::read(dir_a.path().join("catalog/job.toml")).is_ok(),
            "catalog delete must not remove the file on A"
        );
    }

    #[tokio::test]
    async fn catalog_recovers_shared_tombstone_from_one_surviving_copy_over_wire_and_restart() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let root_a = dir_a.path().join("catalog");
        let root_b = dir_b.path().join("catalog");
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        std::fs::write(root_a.join("agent.kdl"), b"role \"worker\"\n").unwrap();
        write_syncs(dir_a.path(), &root_a);
        write_syncs(dir_b.path(), &root_b);

        // Seed the incident state on both nodes: one shared Tombstone/v2, but
        // only A retains the exact previously observed physical bytes.
        let mut tombstoned = SyncNode::new(Author([9; 32]));
        tombstoned.local_write("agent.kdl", b"role \"worker\"\n", 0, 0);
        tombstoned.local_remove("agent.kdl", SyncPolicy::Bus.rules(), 10);
        let manifest = tombstoned.manifest().clone();
        let stale_hash = content_hash(b"role \"worker\"\n");
        for (home, observed) in [
            (
                dir_a.path(),
                HashMap::from([("agent.kdl".to_string(), stale_hash)]),
            ),
            (dir_b.path(), HashMap::new()),
        ] {
            let seed = SyncEngine::new(
                FabricHome::new(home),
                Author([8; 32]),
                Arc::new(LoopbackTransport::default()),
                CancellationToken::new(),
            )
            .await
            .unwrap();
            seed.write_state(
                "catalog",
                &PersistedEntryState {
                    manifest: manifest.clone(),
                    observed,
                },
            )
            .unwrap();
        }

        let transport_a = Arc::new(LoopbackTransport::default());
        let transport_b = Arc::new(LoopbackTransport::default());
        let engine_a = SyncEngine::new(
            FabricHome::new(dir_a.path()),
            Author([1; 32]),
            transport_a.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let engine_b = SyncEngine::new(
            FabricHome::new(dir_b.path()),
            Author([2; 32]),
            transport_b.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        transport_a.add_peer("b", "catalog", engine_b.node_for("catalog").await.unwrap());
        transport_b.add_peer("a", "catalog", engine_a.node_for("catalog").await.unwrap());

        // A's catalog scan advances the surviving bytes to Present/v3, the wire
        // transfers that winner and its content, and B materializes it.
        engine_a.sync_once("catalog").await.unwrap();
        engine_b.sync_once("catalog").await.unwrap();
        engine_a.sync_once("catalog").await.unwrap();

        let recovered = b"role \"worker\"\n";
        assert_eq!(std::fs::read(root_a.join("agent.kdl")).unwrap(), recovered);
        assert_eq!(std::fs::read(root_b.join("agent.kdl")).unwrap(), recovered);
        for engine in [&engine_a, &engine_b] {
            assert!(matches!(
                engine
                    .node_for("catalog")
                    .await
                    .unwrap()
                    .lock()
                    .await
                    .manifest()
                    .get("agent.kdl"),
                Some(Entry::Present(meta))
                    if meta.version == 3 && meta.hash == stale_hash
            ));
        }

        // The same Present/v3 plus observed receipt is authoritative after a
        // restart, and replaying the scan does not create another version.
        drop(engine_a);
        drop(engine_b);
        for (home, root, author) in [
            (dir_a.path(), &root_a, Author([1; 32])),
            (dir_b.path(), &root_b, Author([2; 32])),
        ] {
            let restarted = SyncEngine::new(
                FabricHome::new(home),
                author,
                Arc::new(LoopbackTransport::default()),
                CancellationToken::new(),
            )
            .await
            .unwrap();
            restarted.sync_once("catalog").await.unwrap();
            let entry = restarted
                .entries
                .read()
                .await
                .get("catalog")
                .cloned()
                .unwrap();
            assert!(matches!(
                entry.node.lock().await.manifest().get("agent.kdl"),
                Some(Entry::Present(meta))
                    if meta.version == 3 && meta.hash == stale_hash
            ));
            assert_eq!(
                entry.observed.lock().unwrap().get("agent.kdl"),
                Some(&stale_hash)
            );
            assert_eq!(std::fs::read(root.join("agent.kdl")).unwrap(), recovered);
        }
    }

    #[tokio::test]
    async fn status_exposes_logical_observed_and_drift_counts() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("catalog");
        write_syncs(dir.path(), &root);
        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let entry = engine.entries.read().await.get("catalog").cloned().unwrap();
        let mut observed = HashMap::new();
        {
            let mut node = entry.node.lock().await;
            for index in 0..40 {
                let path = format!("agents/{index}/agent.kdl");
                let bytes = format!("worker {index}").into_bytes();
                node.local_write(&path, &bytes, 0, 0);
                observed.insert(path, content_hash(&bytes));
            }
            for index in 0..3 {
                let path = format!("retired/{index}.kdl");
                let bytes = format!("retired {index}").into_bytes();
                node.local_write(&path, &bytes, 0, 0);
                node.local_remove(&path, SyncPolicy::Bus.rules(), 10);
                if index < 2 {
                    observed.insert(path, content_hash(&bytes));
                }
            }
        }
        *entry.observed.lock().unwrap() = observed;

        let statuses = engine.status().await;
        let status = statuses.first().unwrap();
        assert_eq!(status.present, 40);
        assert_eq!(status.tombstones, 3);
        assert_eq!(status.observed, 42);
        assert_eq!(status.missing, 0);
        assert_eq!(status.unexpected, 2);
        assert_eq!(status.mismatched, 0);
    }

    #[tokio::test]
    async fn equal_version_update_beats_concurrent_delete_then_later_delete_wins_across_engines() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let root_a = dir_a.path().join("resources");
        let root_b = dir_b.path().join("resources");
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        write_bus_sync(dir_a.path(), &root_a);
        write_bus_sync(dir_b.path(), &root_b);

        let transport_a = Arc::new(EngineLoopbackTransport::default());
        let transport_b = Arc::new(EngineLoopbackTransport::default());
        // Give the deleting peer the higher author so this specifically proves
        // equal-version Present precedence happens before author tie-breaking.
        let engine_a = SyncEngine::new(
            FabricHome::new(dir_a.path()),
            Author([1; 32]),
            transport_a.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let engine_b = SyncEngine::new(
            FabricHome::new(dir_b.path()),
            Author([2; 32]),
            transport_b.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        transport_a.add_peer("b", &engine_b);
        transport_b.add_peer("a", &engine_a);

        let path_a = root_a.join("job.toml");
        let path_b = root_b.join("job.toml");
        std::fs::write(&path_a, b"seed").unwrap();
        engine_a.sync_once("bus").await.unwrap();
        engine_b.sync_once("bus").await.unwrap();
        assert_eq!(std::fs::read(&path_b).unwrap(), b"seed");

        // Make the two offline-style local intents from the same v1 baseline,
        // then scan and persist both before allowing either engine to reconcile.
        std::fs::write(&path_a, b"concurrent update").unwrap();
        std::fs::remove_file(&path_b).unwrap();
        for engine in [&engine_a, &engine_b] {
            let entry = engine.entries.read().await.get("bus").cloned().unwrap();
            let _operation = entry.operation.lock().await;
            engine.scan_entry(&entry).await.unwrap();
            engine.persist_entry(&entry).await.unwrap();
        }
        assert!(matches!(
            engine_a
                .node_for("bus")
                .await
                .unwrap()
                .lock()
                .await
                .manifest()
                .get("job.toml"),
            Some(Entry::Present(meta)) if meta.version == 2 && meta.author == Author([1; 32])
        ));
        assert!(matches!(
            engine_b
                .node_for("bus")
                .await
                .unwrap()
                .lock()
                .await
                .manifest()
                .get("job.toml"),
            Some(Entry::Tombstone(tombstone))
                if tombstone.version == 2 && tombstone.author == Author([2; 32])
        ));

        engine_a.sync_once("bus").await.unwrap();
        engine_b.sync_once("bus").await.unwrap();
        assert_eq!(std::fs::read(&path_a).unwrap(), b"concurrent update");
        assert_eq!(std::fs::read(&path_b).unwrap(), b"concurrent update");
        for engine in [&engine_a, &engine_b] {
            assert!(matches!(
                engine
                    .node_for("bus")
                    .await
                    .unwrap()
                    .lock()
                    .await
                    .manifest()
                    .get("job.toml"),
                Some(Entry::Present(meta)) if meta.version == 2
            ));
        }

        // B has now observed the winning update. Its later delete advances to
        // v3 and therefore beats the older Present everywhere.
        std::fs::remove_file(&path_b).unwrap();
        engine_b.sync_once("bus").await.unwrap();
        engine_a.sync_once("bus").await.unwrap();
        assert!(!path_a.exists());
        assert!(!path_b.exists());
        for engine in [&engine_a, &engine_b] {
            assert!(matches!(
                engine
                    .node_for("bus")
                    .await
                    .unwrap()
                    .lock()
                    .await
                    .manifest()
                    .get("job.toml"),
                Some(Entry::Tombstone(tombstone)) if tombstone.version == 3
            ));
        }
    }

    #[tokio::test]
    async fn simultaneous_three_peer_syncs_do_not_deadlock() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let dir_c = tempfile::tempdir().unwrap();
        let root_a = dir_a.path().join("resources");
        let root_b = dir_b.path().join("resources");
        let root_c = dir_c.path().join("resources");
        write_bus_sync(dir_a.path(), &root_a);
        write_bus_sync(dir_b.path(), &root_b);
        write_bus_sync(dir_c.path(), &root_c);
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        std::fs::create_dir_all(&root_c).unwrap();
        std::fs::write(root_a.join("a.md"), b"a").unwrap();
        std::fs::write(root_b.join("b.md"), b"b").unwrap();
        std::fs::write(root_c.join("c.md"), b"c").unwrap();

        let transport_a = Arc::new(EngineLoopbackTransport::default());
        let transport_b = Arc::new(EngineLoopbackTransport::default());
        let transport_c = Arc::new(EngineLoopbackTransport::default());
        let engine_a = SyncEngine::new(
            FabricHome::new(dir_a.path()),
            Author([1; 32]),
            transport_a.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let engine_b = SyncEngine::new(
            FabricHome::new(dir_b.path()),
            Author([2; 32]),
            transport_b.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let engine_c = SyncEngine::new(
            FabricHome::new(dir_c.path()),
            Author([3; 32]),
            transport_c.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        transport_a.add_peer("b", &engine_b);
        transport_a.add_peer("c", &engine_c);
        transport_b.add_peer("a", &engine_a);
        transport_b.add_peer("c", &engine_c);
        transport_c.add_peer("a", &engine_a);
        transport_c.add_peer("b", &engine_b);

        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::try_join!(
                    engine_a.sync_once("bus"),
                    engine_b.sync_once("bus"),
                    engine_c.sync_once("bus")
                )
            })
            .await
            .expect("simultaneous peer syncs deadlocked")
            .unwrap();
        }

        for root in [&root_a, &root_b, &root_c] {
            assert_eq!(std::fs::read(root.join("a.md")).unwrap(), b"a");
            assert_eq!(std::fs::read(root.join("b.md")).unwrap(), b"b");
            assert_eq!(std::fs::read(root.join("c.md")).unwrap(), b"c");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_node_2000_file_continuous_mutation_stays_bounded() {
        tokio::time::timeout(
            Duration::from_secs(60),
            run_three_node_2000_file_continuous_mutation_stress(),
        )
        .await
        .expect("3-node/2,000-file continuous-mutation stress exceeded 60 seconds");
    }

    /// Read a path's mtime as the manifest records it. `mtime_of` takes a DirEntry.
    fn mtime_of_path(path: &Path) -> (i64, u32) {
        let modified = std::fs::metadata(path).unwrap().modified().unwrap();
        match modified.duration_since(UNIX_EPOCH) {
            Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos()),
            Err(err) => (-(err.duration().as_secs() as i64), 0),
        }
    }

    /// Materialization must not stamp the origin's mtime, because doing so is what
    /// manufactured equal-size, equal-mtime collisions between two nodes' entries.
    /// Also documents, by assertion, the residual risk the fix does not close.
    ///
    /// This is the defect that made three nodes diverge permanently. The cache
    /// reuses a recorded hash when size and both mtime components match, and
    /// materialization used to stamp the ORIGIN's mtime onto the file so that cache
    /// would hit. That made mtime a value shared across nodes rather than a local
    /// write timestamp, so two contending entries of equal size could carry equal
    /// mtimes with different content. The scan then reported the wrong hash, the
    /// republish branch read the real bytes, saw a mismatch, and republished stale
    /// content under local authorship. Measured on Linux CI run 30814462024
    /// attempt 4: twenty-four alternating republishes from v44 to v67, never
    /// converging.
    ///
    /// Constructed deliberately rather than raced: same size, same recorded mtime,
    /// different bytes.
    #[test]
    fn materialization_does_not_manufacture_an_mtime_collision() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        let entry = entry_with_policy("bus", &root, SyncPolicy::Bus);

        // A manifest that records FIRST at a specific size and mtime.
        let first = b"c-167";
        let second = b"c-219";
        assert_eq!(first.len(), second.len(), "the collision needs equal size");
        let shared_secs = 1_700_000_000i64;
        let shared_nanos = 123_456_789u32;

        let mut node = SyncNode::new(Author([9u8; 32]));
        node.local_write("hot.txt", first, shared_secs, shared_nanos);
        let recorded = node
            .manifest()
            .get("hot.txt")
            .and_then(|e| e.meta())
            .copied()
            .unwrap();
        assert_eq!(recorded.hash, content_hash(first));

        // Put the OTHER content on disk wearing the recorded size and mtime. This is
        // exactly the state the old mtime stamping could produce.
        let path = root.join("hot.txt");
        std::fs::write(&path, second).unwrap();
        set_file_mtime(&path, shared_secs, shared_nanos).unwrap();
        let (disk_secs, disk_nanos) = mtime_of_path(&path);
        assert_eq!(
            (disk_secs, disk_nanos),
            (recorded.mtime_secs, recorded.mtime_nanos),
            "the test must actually reproduce the mtime collision"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            recorded.size,
            "the test must actually reproduce the size collision"
        );

        // With the collision present, the cache DOES still report the recorded
        // hash rather than the real bytes. That is deliberately asserted here
        // rather than hidden, because it is the residual risk the approved fix
        // does not close: the fix removes the mechanism that manufactured these
        // collisions, it does not make the cache robust to one that arrives some
        // other way, and closing that would need the content-identity mechanism
        // that was explicitly deferred.
        let scanned = scan_folder(&root, &entry, node.manifest()).unwrap();
        let file = scanned.iter().find(|f| f.rel == "hot.txt").unwrap();
        assert_eq!(
            file.hash,
            content_hash(first),
            "documented residual risk: the size-and-mtime cache trusts a collision"
        );

        // What the fix guarantees is that materialization no longer manufactures
        // one. A materialized file carries the LOCAL write time, so its mtime does
        // not equal the origin's recorded mtime and the cache misses on it.
        let mut peer = SyncNode::new(Author([3u8; 32]));
        peer.local_write("materialized.txt", second, shared_secs, shared_nanos);
        let mut observed = HashMap::new();
        materialize_tracked(
            &mut peer,
            &root,
            entry.policy.rules(),
            &HashMap::new(),
            &mut observed,
            None,
        )
        .unwrap();
        let (written_secs, written_nanos) = mtime_of_path(&root.join("materialized.txt"));
        assert_ne!(
            (written_secs, written_nanos),
            (shared_secs, shared_nanos),
            "materialization must not stamp the origin mtime; stamping it is what \
             made two nodes' entries collide on size and mtime"
        );
    }

    /// The cache must still hit for a genuinely untouched local file, which is the
    /// whole reason it exists.
    #[test]
    fn scan_still_reuses_the_recorded_hash_for_an_untouched_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        let entry = entry_with_policy("bus", &root, SyncPolicy::Bus);

        let bytes = b"untouched bytes";
        let path = root.join("still.txt");
        std::fs::write(&path, bytes).unwrap();
        let (secs, nanos) = mtime_of_path(&path);

        let mut node = SyncNode::new(Author([4u8; 32]));
        node.local_write("still.txt", bytes, secs, nanos);

        let scanned = scan_folder(&root, &entry, node.manifest()).unwrap();
        let file = scanned.iter().find(|f| f.rel == "still.txt").unwrap();
        assert_eq!(file.hash, content_hash(bytes));
        assert!(
            file.bytes.is_none(),
            "an untouched file must not be re-read; the cache is the point"
        );
    }

    /// And a real user edit must still be republished with local authorship and a
    /// higher version. Not suppressing stale content must not become suppressing
    /// genuine edits.
    #[test]
    fn a_real_user_edit_still_republishes_with_local_authorship() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        let entry = entry_with_policy("bus", &root, SyncPolicy::Bus);

        let path = root.join("edited.txt");
        std::fs::write(&path, b"original").unwrap();
        let (secs, nanos) = mtime_of_path(&path);
        let mine = Author([7u8; 32]);
        let mut node = SyncNode::new(mine);
        node.local_write("edited.txt", b"original", secs, nanos);
        let before = node.manifest().get("edited.txt").unwrap().version();

        // A genuine user edit.
        std::fs::write(&path, b"user edited this").unwrap();
        let changed = scan_into_node(&mut node, &root, &entry, SyncPolicy::Bus.rules()).unwrap();

        assert!(changed, "a real user edit must be recorded");
        let after = node
            .manifest()
            .get("edited.txt")
            .and_then(|e| e.meta())
            .copied()
            .unwrap();
        assert_eq!(after.hash, content_hash(b"user edited this"));
        assert_eq!(
            after.author, mine,
            "a user edit must carry local authorship"
        );
        assert!(
            after.version > before,
            "a user edit must outrank what it replaced"
        );
    }

    async fn run_three_node_2000_file_continuous_mutation_stress() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let dir_c = tempfile::tempdir().unwrap();
        let root_a = dir_a.path().join("resources");
        let root_b = dir_b.path().join("resources");
        let root_c = dir_c.path().join("resources");
        for (home, root) in [
            (dir_a.path(), &root_a),
            (dir_b.path(), &root_b),
            (dir_c.path(), &root_c),
        ] {
            write_bus_sync(home, root);
            std::fs::create_dir_all(root).unwrap();
        }
        for index in 0..1_999 {
            std::fs::write(root_a.join(format!("file-{index:04}.txt")), b"seed").unwrap();
        }
        std::fs::write(root_a.join("hot.txt"), b"seed").unwrap();

        let cancel = CancellationToken::new();
        let transport_a = Arc::new(EngineLoopbackTransport::default());
        let transport_b = Arc::new(EngineLoopbackTransport::default());
        let transport_c = Arc::new(EngineLoopbackTransport::default());
        let engine_a = SyncEngine::new(
            FabricHome::new(dir_a.path()),
            Author([1; 32]),
            transport_a.clone(),
            cancel.clone(),
        )
        .await
        .unwrap();
        let engine_b = SyncEngine::new(
            FabricHome::new(dir_b.path()),
            Author([2; 32]),
            transport_b.clone(),
            cancel.clone(),
        )
        .await
        .unwrap();
        let engine_c = SyncEngine::new(
            FabricHome::new(dir_c.path()),
            Author([3; 32]),
            transport_c.clone(),
            cancel.clone(),
        )
        .await
        .unwrap();
        transport_a.add_peer("b", &engine_b);
        transport_a.add_peer("c", &engine_c);
        transport_b.add_peer("a", &engine_a);
        transport_b.add_peer("c", &engine_c);
        transport_c.add_peer("a", &engine_a);
        transport_c.add_peer("b", &engine_b);

        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(15), async {
                tokio::try_join!(
                    engine_a.sync_once("bus"),
                    engine_b.sync_once("bus"),
                    engine_c.sync_once("bus")
                )
            })
            .await
            .expect("initial 2,000-file convergence timed out")
            .unwrap();
        }
        for engine in [&engine_a, &engine_b, &engine_c] {
            assert_eq!(
                engine
                    .node_for("bus")
                    .await
                    .unwrap()
                    .lock()
                    .await
                    .manifest()
                    .present_paths()
                    .count(),
                2_000
            );
        }
        for transport in [&transport_a, &transport_b, &transport_c] {
            transport.reset_reconciles();
        }
        for engine in [&engine_a, &engine_b, &engine_c] {
            engine.ensure_watching().await;
        }
        // Wait until every newly spawned loop has dialed both peers for its one
        // initial sync, then ensure each entry operation is idle before zeroing
        // the stress counters.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if [&transport_a, &transport_b, &transport_c]
                    .into_iter()
                    .all(|transport| transport.reconcile_count() >= 2)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("watch loops did not finish their initial peer dials");
        // Initial materialization may itself have queued one final mutation
        // burst. Let that bounded window flush, then require all inbound work
        // to drain before establishing the zero-work read baseline.
        tokio::time::sleep(WATCH_MAX_COALESCE + Duration::from_millis(250)).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut idle = true;
                for engine in [&engine_a, &engine_b, &engine_c] {
                    let entry = engine.entries.read().await.get("bus").cloned().unwrap();
                    idle &= entry.work.inbound_waiters.load(Ordering::Acquire) == 0;
                }
                if idle {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial inbound work did not drain");
        for engine in [&engine_a, &engine_b, &engine_c] {
            let entry = engine.entries.read().await.get("bus").cloned().unwrap();
            let _idle = entry.operation.lock().await;
        }

        for transport in [&transport_a, &transport_b, &transport_c] {
            transport.reset_reconciles();
        }
        let mut entries = Vec::new();
        for engine in [&engine_a, &engine_b, &engine_c] {
            let entry = engine.entries.read().await.get("bus").cloned().unwrap();
            entry.work.full_scans.store(0, Ordering::Relaxed);
            entry.work.persist_calls.store(0, Ordering::Relaxed);
            entries.push(entry);
        }

        // Model the exact Linux incident trigger: opening every watched file
        // must not schedule a reconcile or any additional scan/persist work.
        for root in [&root_a, &root_b, &root_c] {
            for index in 0..1_999 {
                assert_eq!(
                    std::fs::read(root.join(format!("file-{index:04}.txt"))).unwrap(),
                    b"seed"
                );
            }
            assert_eq!(std::fs::read(root.join("hot.txt")).unwrap(), b"seed");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            [&transport_a, &transport_b, &transport_c]
                .into_iter()
                .map(|transport| transport.reconcile_count())
                .sum::<usize>(),
            0,
            "read/access events retriggered reconciliation"
        );
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.work.full_scans.load(Ordering::Relaxed))
                .sum::<u64>(),
            0,
            "read/access events retriggered folder scans"
        );

        // Keep all three roots continuously mutating for longer than the
        // production coalescing cap. The cap must make progress without
        // returning to the old 150 ms reconcile loop.
        let started = tokio::time::Instant::now();
        let writers = [
            (root_a.clone(), "a"),
            (root_b.clone(), "b"),
            (root_c.clone(), "c"),
        ]
        .into_iter()
        .map(|(root, label)| {
            tokio::spawn(async move {
                for revision in 0..220 {
                    std::fs::write(
                        root.join("hot.txt"),
                        format!("{label}-{revision:03}").as_bytes(),
                    )
                    .unwrap();
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        });
        for writer in writers {
            writer.await.unwrap();
        }
        tokio::time::sleep(WATCH_MAX_COALESCE + Duration::from_millis(500)).await;
        let elapsed = started.elapsed();

        let reconciles = [&transport_a, &transport_b, &transport_c]
            .into_iter()
            .map(|transport| transport.reconcile_count())
            .sum::<usize>();
        let scans = entries
            .iter()
            .map(|entry| entry.work.full_scans.load(Ordering::Relaxed))
            .sum::<u64>();
        let persists = entries
            .iter()
            .map(|entry| entry.work.persist_calls.load(Ordering::Relaxed))
            .sum::<usize>();
        eprintln!(
            "bounded 3-node/2,000-file stress: {reconciles} reconciles, {scans} scans, \
             {persists} persists in {elapsed:?}"
        );
        // Bounded as RATES, not as fixed counts.
        //
        // The counts were duration-sensitive and that made this test flaky rather
        // than strict. The writers do a fixed NUMBER of revisions with a sleep
        // between them, so a loaded machine stretches the burst, elapses more
        // coalescing windows, and produces proportionally more legitimate work.
        // Measured: this passes locally at 30 to 34 reconciles, two below the old
        // cap of 36, and Linux CI produced 54 in 15.57s and 43 in 16.14s. Every one
        // of those runs, including both failures, stayed under 4 reconciles per
        // second. So the old caps failed runs whose actual amplification was fine.
        //
        // The ceilings below keep the original relationships exactly: the reconcile
        // rate stays at 4 per second, and the scan and persist ceilings are the old
        // caps expressed against that same rate, 112/36 and 76/36 of it, which is
        // the derivation the previous comment described. No new number is invented.
        let elapsed_millis = elapsed.as_millis().max(1);
        assert!(
            (reconciles as u128) * 1_000 <= elapsed_millis * 4,
            "reconcile rate exceeded: {reconciles} reconciles, {scans} scans, and {persists} persists in {elapsed:?}"
        );
        assert!(
            (scans as u128) * 1_000 * 36 <= elapsed_millis * 4 * 112,
            "full-folder scan rate exceeded: {scans} scans against {reconciles} reconciles in {elapsed:?}"
        );
        assert!(
            (persists as u128) * 1_000 * 36 <= elapsed_millis * 4 * 76,
            "state persist rate exceeded: {persists} persists against {reconciles} reconciles in {elapsed:?}"
        );

        // The bounded watcher work must still converge the latest value.
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(15), async {
                tokio::try_join!(
                    engine_a.sync_once("bus"),
                    engine_b.sync_once("bus"),
                    engine_c.sync_once("bus")
                )
            })
            .await
            .expect("post-stress convergence timed out")
            .unwrap();
        }
        let hot_a = std::fs::read(root_a.join("hot.txt")).unwrap();
        assert_eq!(std::fs::read(root_b.join("hot.txt")).unwrap(), hot_a);
        assert_eq!(std::fs::read(root_c.join("hot.txt")).unwrap(), hot_a);
        cancel.cancel();
    }

    #[tokio::test]
    async fn restart_keeps_unmaterialized_remote_present_until_content_arrives() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        write_bus_sync(dir.path(), &root);

        // Persist a remote Present without ever placing its bytes on disk. This
        // models a reconcile that learned metadata but could not fetch content
        // before the daemon restarted.
        let seed_engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let mut remote = SyncNode::new(Author([2; 32]));
        remote.local_write("inbox/remote-only.md", b"arrives later", 0, 0);
        let manifest = remote.manifest().clone();
        seed_engine.write_manifest("bus", &manifest).unwrap();
        drop(seed_engine);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();
        assert!(!root.join("inbox/remote-only.md").exists());
        assert!(matches!(
            engine
                .node_for("bus")
                .await
                .unwrap()
                .lock()
                .await
                .manifest()
                .get("inbox/remote-only.md"),
            Some(Entry::Present(_))
        ));

        // Once a peer supplies the referenced bytes, the same Present
        // materializes rather than being superseded by a false tombstone.
        let remote = Arc::new(Mutex::new(remote));
        let (client_end, server_end) = tokio::io::duplex(1 << 20);
        let resolver_engine = engine.clone();
        let server = tokio::spawn(async move {
            crate::sync::wire::run_server(server_end, move |name, _| {
                let engine = resolver_engine.clone();
                async move {
                    let prepared = engine.prepare_inbound(&name).await?;
                    Ok(prepared.map(|prepared| (prepared.node(), prepared)))
                }
            })
            .await
        });
        crate::sync::wire::run_client(client_end, remote, "bus")
            .await
            .unwrap();
        let (_, _, prepared) = server.await.unwrap().unwrap();
        engine.complete_inbound(prepared).await.unwrap();

        assert_eq!(
            std::fs::read(root.join("inbox/remote-only.md")).unwrap(),
            b"arrives later"
        );
        assert!(matches!(
            engine
                .node_for("bus")
                .await
                .unwrap()
                .lock()
                .await
                .manifest()
                .get("inbox/remote-only.md"),
            Some(Entry::Present(_))
        ));
    }

    #[tokio::test]
    async fn restart_prefers_atomic_state_pair_over_stale_projection_and_partial_temp() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("retired.md"), b"old").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();
        let present_manifest = engine
            .node_for("bus")
            .await
            .unwrap()
            .lock()
            .await
            .manifest()
            .clone();

        std::fs::remove_file(root.join("retired.md")).unwrap();
        engine.sync_once("bus").await.unwrap();
        let state_path = engine.state_path("bus");
        let manifest_path = engine.manifest_path("bus");
        let committed: PersistedEntryState =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert!(matches!(
            committed.manifest.get("retired.md"),
            Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
        ));
        assert!(!committed.observed.contains_key("retired.md"));

        // Model a crash after authoritative state.json committed but before its
        // compatibility projection: manifest.json is stale and an incomplete
        // state temp sibling remains. Restart must ignore both.
        engine.write_manifest("bus", &present_manifest).unwrap();
        let state_temp = state_path.with_extension("json.fabric-tmp");
        std::fs::write(&state_temp, b"{\"manifest\":").unwrap();
        drop(engine);

        let restarted = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let entry = restarted.entries.read().await.get("bus").cloned().unwrap();
        assert!(matches!(
            entry.node.lock().await.manifest().get("retired.md"),
            Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
        ));
        assert!(!entry.observed.lock().unwrap().contains_key("retired.md"));
        assert!(!root.join("retired.md").exists());

        restarted.sync_once("bus").await.unwrap();
        let replayed: PersistedEntryState =
            serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
        assert_eq!(replayed.manifest, committed.manifest);
        assert_eq!(replayed.observed, committed.observed);
        assert!(matches!(
            serde_json::from_slice::<Manifest>(&std::fs::read(&manifest_path).unwrap())
                .unwrap()
                .get("retired.md"),
            Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
        ));
    }

    #[tokio::test]
    async fn authoritative_state_survives_projection_write_failure() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("retired.md"), b"old").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();
        let node = engine.node_for("bus").await.unwrap();
        let tombstone_manifest = {
            let mut node = node.lock().await;
            assert!(node.local_remove("retired.md", SyncPolicy::Bus.rules(), 10));
            node.manifest().clone()
        };
        let committed = PersistedEntryState {
            manifest: tombstone_manifest,
            observed: HashMap::new(),
        };

        // Block only manifest.json's atomic temp path. write_state must report
        // the projection failure after the authoritative state pair committed.
        let manifest_path = engine.manifest_path("bus");
        std::fs::create_dir(manifest_path.with_extension("json.fabric-tmp")).unwrap();
        assert!(engine.write_state("bus", &committed).is_err());

        let on_disk: PersistedEntryState =
            serde_json::from_slice(&std::fs::read(engine.state_path("bus")).unwrap()).unwrap();
        assert_eq!(on_disk.manifest, committed.manifest);
        assert_eq!(on_disk.observed, committed.observed);
        assert!(matches!(
            serde_json::from_slice::<Manifest>(&std::fs::read(&manifest_path).unwrap())
                .unwrap()
                .get("retired.md"),
            Some(Entry::Present(meta)) if meta.version == 1
        ));
        drop(engine);

        let restarted = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let entry = restarted.entries.read().await.get("bus").cloned().unwrap();
        assert!(matches!(
            entry.node.lock().await.manifest().get("retired.md"),
            Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
        ));
        assert!(entry.observed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn restart_tombstone_stale_file_policy_and_observed_receipt_contract_matrix() {
        struct Case {
            label: &'static str,
            observed: Option<&'static [u8]>,
            disk: &'static [u8],
            expected_bus_present: bool,
        }

        // Catalog always recovers physical bytes into a higher Present because
        // union-of-presence cannot retain a Tombstone while any copy survives.
        // Bus keeps a matching unchanged receipt tombstoned; absent, mismatched,
        // or edited bytes are new local intent and advance to Present/v3.
        let cases = [
            Case {
                label: "matching receipt and unchanged bytes",
                observed: Some(b"old"),
                disk: b"old",
                expected_bus_present: false,
            },
            Case {
                label: "absent receipt and unchanged bytes",
                observed: None,
                disk: b"old",
                expected_bus_present: true,
            },
            Case {
                label: "mismatched receipt and unchanged bytes",
                observed: Some(b"different"),
                disk: b"old",
                expected_bus_present: true,
            },
            Case {
                label: "matching receipt and edited bytes",
                observed: Some(b"old"),
                disk: b"edited",
                expected_bus_present: true,
            },
        ];

        for final_policy in [SyncPolicy::Catalog, SyncPolicy::Bus] {
            for case in &cases {
                let dir = tempfile::tempdir().unwrap();
                let root = dir.path().join("resources");
                let path = root.join("retired.md");
                std::fs::create_dir_all(&root).unwrap();
                std::fs::write(&path, case.disk).unwrap();

                // Seed the exact incident state under the delete-propagating
                // policy: authoritative Tombstone/v2 plus the case's durable
                // observed receipt and physical bytes.
                write_named_sync(dir.path(), "contract", &root, SyncPolicy::Bus);
                let seed = SyncEngine::new(
                    FabricHome::new(dir.path()),
                    Author([1; 32]),
                    Arc::new(LoopbackTransport::default()),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
                let mut node = SyncNode::new(Author([1; 32]));
                node.local_write("retired.md", b"old", 0, 0);
                node.local_remove("retired.md", SyncPolicy::Bus.rules(), 10);
                let observed = case
                    .observed
                    .map(|bytes| HashMap::from([("retired.md".to_string(), content_hash(bytes))]))
                    .unwrap_or_default();
                seed.write_state(
                    "contract",
                    &PersistedEntryState {
                        manifest: node.manifest().clone(),
                        observed,
                    },
                )
                .unwrap();

                // Restart either after bus -> catalog or with bus unchanged.
                write_named_sync(dir.path(), "contract", &root, final_policy);
                drop(seed);
                let restarted = SyncEngine::new(
                    FabricHome::new(dir.path()),
                    Author([1; 32]),
                    Arc::new(LoopbackTransport::default()),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
                restarted.sync_once("contract").await.unwrap();

                let entry = restarted
                    .entries
                    .read()
                    .await
                    .get("contract")
                    .cloned()
                    .unwrap();
                let manifest = entry.node.lock().await.manifest().clone();
                let observed = entry.observed.lock().unwrap().clone();
                let context = format!("{} under {final_policy:?}", case.label);
                let expected_present =
                    final_policy == SyncPolicy::Catalog || case.expected_bus_present;
                if expected_present {
                    assert!(
                        matches!(
                            manifest.get("retired.md"),
                            Some(Entry::Present(meta))
                                if meta.version == 3 && meta.hash == content_hash(case.disk)
                        ),
                        "{context}"
                    );
                    assert_eq!(std::fs::read(&path).unwrap(), case.disk, "{context}");
                    assert_eq!(
                        observed.get("retired.md"),
                        Some(&content_hash(case.disk)),
                        "{context}"
                    );
                } else {
                    assert!(
                        matches!(
                            manifest.get("retired.md"),
                            Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
                        ),
                        "{context}"
                    );
                    let file_survives = final_policy == SyncPolicy::Catalog;
                    assert_eq!(path.exists(), file_survives, "{context}");
                    assert_eq!(
                        observed.contains_key("retired.md"),
                        file_survives,
                        "{context}"
                    );
                }

                // A second replay must be stable under either final policy.
                restarted.sync_once("contract").await.unwrap();
                let replayed = restarted
                    .entries
                    .read()
                    .await
                    .get("contract")
                    .cloned()
                    .unwrap();
                assert_eq!(
                    replayed.node.lock().await.manifest(),
                    &manifest,
                    "{context}"
                );
                assert_eq!(&*replayed.observed.lock().unwrap(), &observed, "{context}");
            }
        }
    }

    #[tokio::test]
    async fn inactive_entry_archive_uses_durable_observed_receipt_on_reload() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(root.join("inbox")).unwrap();
        std::fs::write(root.join("inbox/archived.md"), b"archive offline").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();

        // Disable the entry while its inbox Present is durably observed, then
        // perform the archive while no watcher/entry can see the rename.
        std::fs::write(dir.path().join("syncs.toml"), "").unwrap();
        engine.load_from_config().await.unwrap();
        assert!(engine.node_for("bus").await.is_none());
        std::fs::create_dir_all(root.join("archive")).unwrap();
        std::fs::rename(
            root.join("inbox/archived.md"),
            root.join("archive/archived.md"),
        )
        .unwrap();

        // Cross an actual daemon/engine restart boundary while the entry is
        // still disabled. Remove the compatibility projection as well, proving
        // that the authoritative combined state is the only recovery source.
        let state_path = engine.state_path("bus");
        let manifest_path = engine.manifest_path("bus");
        assert!(state_path.exists());
        drop(engine);
        std::fs::remove_file(&manifest_path).unwrap();

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(engine.node_for("bus").await.is_none());
        assert!(!manifest_path.exists());

        write_bus_sync(dir.path(), &root);
        engine.load_from_config().await.unwrap();
        engine.sync_once("bus").await.unwrap();

        let node = engine.node_for("bus").await.unwrap();
        let node = node.lock().await;
        assert!(matches!(
            node.manifest().get("inbox/archived.md"),
            Some(Entry::Tombstone(_))
        ));
        assert!(matches!(
            node.manifest().get("archive/archived.md"),
            Some(Entry::Present(_))
        ));
        drop(node);
        assert!(!root.join("inbox/archived.md").exists());
        assert_eq!(
            std::fs::read(root.join("archive/archived.md")).unwrap(),
            b"archive offline"
        );
    }

    #[tokio::test]
    async fn local_edit_after_post_merge_scan_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("shared.md"), b"original").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        engine.sync_once("bus").await.unwrap();

        let local_node = engine.node_for("bus").await.unwrap();
        let manifest = local_node.lock().await.manifest().clone();
        let mut remote = SyncNode::new(Author([2; 32]));
        remote.put_content(b"original".to_vec());
        remote.adopt(&manifest);
        remote.local_write("shared.md", b"remote edit", 0, 0);
        let remote = Arc::new(Mutex::new(remote));

        let prepared = engine.prepare_inbound("bus").await.unwrap().unwrap();
        let node = prepared.node();
        let (client_end, server_end) = tokio::io::duplex(1 << 20);
        let server = tokio::spawn(async move {
            crate::sync::wire::run_server(server_end, move |name, _| async move {
                assert_eq!(name, "bus");
                Ok(Some((node, prepared)))
            })
            .await
        });
        crate::sync::wire::run_client(client_end, remote, "bus")
            .await
            .unwrap();
        let (_, _, prepared) = server.await.unwrap().unwrap();

        // Pause completion after its post-merge scan, then edit the file. Final
        // materialization must version this local write above the remote edit.
        let PreparedInbound { entry, mode } = prepared;
        let PreparedInboundMode::Guarded {
            baseline,
            manifest: _,
            _waiter,
            _operation,
        } = mode
        else {
            panic!("a differing remote manifest must use guarded inbound");
        };
        engine.scan_entry(&entry).await.unwrap();
        std::fs::write(root.join("shared.md"), b"local after scan").unwrap();
        engine
            .materialize_entry_state(&entry, &baseline)
            .await
            .unwrap();
        engine.persist_entry(&entry).await.unwrap();

        assert_eq!(
            std::fs::read(root.join("shared.md")).unwrap(),
            b"local after scan"
        );
        let node = engine.node_for("bus").await.unwrap();
        let node = node.lock().await;
        let meta = node.manifest().get("shared.md").unwrap().meta().unwrap();
        assert_eq!(meta.hash, content_hash(b"local after scan"));
        assert_eq!(meta.version, 3);
    }

    #[tokio::test]
    async fn inbound_reconcile_preserves_local_archive_and_accepts_new_remote_file() {
        let (_dir, root, engine, remote) = archive_race_fixture().await;
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        entry.work.full_scans.store(0, Ordering::Relaxed);

        // st2 archive is one atomic rename inside the bus root. The watcher has
        // not scanned it yet when an inbound reconcile begins.
        std::fs::create_dir_all(root.join("archive")).unwrap();
        std::fs::rename(
            root.join("inbox/archived.md"),
            root.join("archive/archived.md"),
        )
        .unwrap();

        run_inbound_wire_reconcile(engine.clone(), remote.clone()).await;

        assert_archive_outcome(&engine, &root).await;
        assert_eq!(
            entry.work.full_scans.load(Ordering::Relaxed),
            2,
            "a remote manifest change must keep both archive-protecting scans"
        );
    }

    #[tokio::test]
    async fn archive_after_inbound_prepare_wins_over_stale_remote_present() {
        let (_dir, root, engine, remote) = archive_race_fixture().await;

        // Deterministically pause the inbound transaction after its first scan.
        // This is the post-scan/pre-merge window: the pre-merge baseline still
        // says inbox/archived.md is Present when the atomic archive lands.
        let remote_manifest = remote.lock().await.manifest().clone();
        let prepared = engine
            .prepare_inbound_for_manifest("bus", &remote_manifest)
            .await
            .unwrap()
            .unwrap();
        std::fs::create_dir_all(root.join("archive")).unwrap();
        std::fs::rename(
            root.join("inbox/archived.md"),
            root.join("archive/archived.md"),
        )
        .unwrap();

        let (client_end, server_end) = tokio::io::duplex(1 << 20);
        let node = prepared.node();
        let server = tokio::spawn(async move {
            crate::sync::wire::run_server(server_end, move |name, _| async move {
                assert_eq!(name, "bus");
                Ok(Some((node, prepared)))
            })
            .await
        });
        crate::sync::wire::run_client(client_end, remote, "bus")
            .await
            .unwrap();
        let (_, _, prepared) = server.await.unwrap().unwrap();
        engine.complete_inbound(prepared).await.unwrap();

        assert_archive_outcome(&engine, &root).await;
    }
}
