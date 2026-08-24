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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};

use crate::daemon::VALIDATION_LOG_TARGET;
use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use crate::config::FabricHome;

use super::config::{PolicyRules, SyncBook, SyncEntry, SyncPeers};
use super::manifest::{Author, ContentHash, FileMeta, Manifest};
use super::node::{Reconciled, SweepEvidence, SyncNode, content_hash};

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
    ///
    /// This counts EVERY `scan_entry` call, from all three of its callers, not
    /// the calls made by `sync_once` alone. Divide a cost by `sync_passes`.
    /// Never divide it by this, and never convert this into a pass count.
    full_scans: AtomicU64,
    /// Exact-manifest, complete-content inbound transactions that bypassed the
    /// guarded scan/materialize path.
    inbound_noop_transactions: AtomicU64,
    /// Inbound transactions that selected the guarded scan/materialize path.
    inbound_guarded_transactions: AtomicU64,
    /// Calls to `sync_once`, and the ONLY correct denominator for a per-pass
    /// cost.
    ///
    /// It is not `full_scans`, and NO CONSTANT converts one into the other.
    /// `scan_entry` has three callers. `sync_once` calls it twice, once each
    /// side of the peer step. `complete_inbound` calls it exactly once per
    /// guarded inbound transaction. `prepare_inbound_entry` calls it at most
    /// once more, and skips it when the durable scan is still good. So:
    ///
    /// ```text
    /// 2*sync_passes + guarded <= full_scans <= 2*sync_passes + 2*guarded
    /// ```
    ///
    /// where `guarded` is `inbound_guarded_transactions`. Inbound traffic is
    /// not a fixed multiple of local passes, so the ratio moves with what the
    /// peers are doing, and it is not knowable from this side.
    ///
    /// An earlier version of this comment asserted a flat two per call and
    /// named a "2x" correction. Measured on the live fleet in one 300 s window,
    /// the real ratio was 3.71 on a busy entry and 2.33 on a quiet one. A
    /// reader who applied the documented 2x to the busy entry would have
    /// overstated the rate by 85%, which is the same class of miscorrection
    /// this counter was added to prevent.
    sync_passes: AtomicU64,
    /// Cumulative time inside each phase of `sync_once`, in microseconds.
    /// Cumulative rather than per-pass so a reader takes two samples and
    /// divides, which is the only thing that describes the present.
    scan_micros: AtomicU64,
    materialize_micros: AtomicU64,
    persist_micros: AtomicU64,
    reconcile_micros: AtomicU64,
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
            sync_passes: AtomicU64::new(0),
            scan_micros: AtomicU64::new(0),
            materialize_micros: AtomicU64::new(0),
            persist_micros: AtomicU64::new(0),
            reconcile_micros: AtomicU64::new(0),
            #[cfg(test)]
            persist_calls: AtomicUsize::new(0),
        })
    }

    /// Add one phase's elapsed time to its running total.
    ///
    /// Saturating, because a counter that wraps is worse than one that stops:
    /// a wrapped delta reads as a huge negative and looks like a real event.
    fn add_phase(counter: &AtomicU64, started: Instant) {
        let micros = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        counter.fetch_add(micros, Ordering::Relaxed);
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
    /// Local hash cache, keyed on this machine's own disk facts. Never sent.
    scan_cache: Arc<StdMutex<HashMap<String, ScanCacheEntry>>>,
    /// Local wall-clock time of the last reconcile this node completed with
    /// each peer, keyed by peer id. Never sent; it is this node's own evidence
    /// of what a peer has been told, and the tombstone sweep will not forget a
    /// deletion until every configured peer has an ack newer than it.
    peer_acks: Arc<StdMutex<HashMap<String, i64>>>,
    /// Local time each expired tombstone was first seen expired HERE, for the
    /// tombstones still waiting on an ack. Deliberately not persisted: a
    /// tombstone whose stamp is lost is simply stamped again on the next pass
    /// and waits one more ack round, which is the safe direction.
    expired_since: Arc<StdMutex<HashMap<String, i64>>>,
    /// The last sweep state REPORTED for this entry. The sweep runs on every
    /// pass, so logging its reason every time would bury the validation log.
    /// This is what makes the log fire on a CHANGE and lets `sync ls` answer on
    /// demand.
    last_sweep: Arc<StdMutex<Option<SweepState>>>,
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
    /// Local hash cache. Absent in files written before it existed, in which
    /// case the first scan re-hashes once and warms it.
    #[serde(default)]
    scan_cache: HashMap<String, ScanCacheEntry>,
    /// Per-peer reconcile acks. Absent in files written before it existed, and
    /// an empty map is the safe value: with no ack for a peer the sweep
    /// refuses to forget anything until one is earned.
    #[serde(default)]
    peer_acks: HashMap<String, i64>,
}

/// What the LOCAL disk looked like when this path was last hashed.
///
/// Deliberately separate from `observed`, and the separation is the point.
/// `observed` decides whether a missing path becomes a tombstone, and a
/// tombstone crosses the wire; loading a performance concern onto it would put
/// a cache inside the structure that decides correctness.
///
/// It is equally deliberate that this never crosses the wire. The previous
/// version of this cache read its key from the REPLICATED manifest, so a local
/// caching decision was made from a value another machine chose. Two contending
/// entries of equal size could then collide on size plus mtime, the cache
/// reported content the file did not hold, and versions leapfrogged forever.
/// Keyed locally, that collision cannot be manufactured: these numbers describe
/// this machine's own disk and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ScanCacheEntry {
    size: u64,
    /// The mtime as READ BACK from disk, never the value that was requested.
    /// A filesystem with coarser precision truncates a stamp, so the requested
    /// value may never be observable again and the cache would miss forever.
    mtime_secs: i64,
    mtime_nanos: u32,
    hash: ContentHash,
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
            let (node, operation, observed, scan_cache, peer_acks, expired_since) =
                match entries.get(&cfg.name) {
                    Some(existing) if existing.config == *cfg => (
                        existing.node.clone(),
                        existing.operation.clone(),
                        existing.observed.clone(),
                        existing.scan_cache.clone(),
                        existing.peer_acks.clone(),
                        existing.expired_since.clone(),
                    ),
                    _ => {
                        work.durable_generation.store(0, Ordering::Release);
                        work.record_mutation();
                        let (node, observed, scan_cache, peer_acks) =
                            self.load_node_and_observed(cfg).await?;
                        (
                            Arc::new(Mutex::new(node)),
                            Arc::new(Mutex::new(())),
                            Arc::new(StdMutex::new(observed)),
                            Arc::new(StdMutex::new(scan_cache)),
                            Arc::new(StdMutex::new(peer_acks)),
                            Arc::new(StdMutex::new(HashMap::new())),
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
                    scan_cache,
                    peer_acks,
                    expired_since,
                    last_sweep: Arc::new(StdMutex::new(None)),
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
    ) -> Result<(
        SyncNode,
        HashMap<String, ContentHash>,
        HashMap<String, ScanCacheEntry>,
        HashMap<String, i64>,
    )> {
        let mut node = SyncNode::new(self.author);
        if let Some(state) = self.read_state(&cfg.name)? {
            node.adopt(&state.manifest);
            // The cache is absent in a file written before it existed. An empty
            // one is correct, not a fault: the next scan re-hashes once and
            // warms it.
            return Ok((node, state.observed, state.scan_cache, state.peer_acks));
        }
        if let Some(manifest) = self.read_manifest(&cfg.name)? {
            node.adopt(&manifest);
        }
        let mut scan_cache = HashMap::new();
        let observed = observed_from_disk(node.manifest(), cfg, &mut scan_cache)?;
        Ok((node, observed, scan_cache, HashMap::new()))
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
                sync_passes: entry.work.sync_passes.load(Ordering::Relaxed),
                scan_micros: entry.work.scan_micros.load(Ordering::Relaxed),
                materialize_micros: entry.work.materialize_micros.load(Ordering::Relaxed),
                persist_micros: entry.work.persist_micros.load(Ordering::Relaxed),
                reconcile_micros: entry.work.reconcile_micros.load(Ordering::Relaxed),
                sweep: entry.last_sweep.lock().unwrap().clone(),
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
        // Counted on entry, not on success, so it matches `full_scans`, which
        // counts attempts. Counting completions instead would let an early
        // error add phase time to the totals without adding a pass to divide
        // by, which inflates the per-pass cost exactly when something is wrong.
        entry.work.sync_passes.fetch_add(1, Ordering::Relaxed);
        // Never hold the local operation guard across a peer dial. If A and B
        // initiate together, retaining A while awaiting B's inbound guard (and
        // vice versa) is a distributed lock inversion. Carry a pre-merge
        // baseline across the unlocked network step instead.
        let (baseline, manifest) = {
            let _operation = entry.operation.lock().await;
            let protected = entry.observed.lock().unwrap().clone();
            let generation = entry.work.mutation_generation.load(Ordering::Acquire);
            let phase = Instant::now();
            let scan_changed = self.scan_entry(&entry).await?;
            EntryWork::add_phase(&entry.work.scan_micros, phase);
            let phase = Instant::now();
            self.materialize_entry_state(&entry, &protected).await?;
            EntryWork::add_phase(&entry.work.materialize_micros, phase);
            // A pass that changed nothing has nothing to record. This call was
            // unguarded while `prepare_inbound_entry` guards the identical one,
            // whose comment already states the rule: an already durable no-op
            // scan needs no rewrite. Applying it here, not inventing it.
            //
            // Each term earns its place:
            // - `scan_changed` is what the scan already returned and this call
            //   site used to discard.
            // - `observed` can move when the scan found nothing, because
            //   materialization restores a file deleted under catalog policy.
            // - the generation catches a watcher event not yet made durable.
            // - a legacy entry may have no state.json yet, so a first pass must
            //   write even when it changed nothing.
            let observed_changed = { *entry.observed.lock().unwrap() != protected };
            if scan_changed
                || observed_changed
                || entry.work.durable_generation.load(Ordering::Acquire) != generation
                || !self.state_path(&entry.config.name).exists()
            {
                let phase = Instant::now();
                self.persist_entry(&entry).await?;
                EntryWork::add_phase(&entry.work.persist_micros, phase);
            }
            entry.work.mark_generation_durable(generation);
            let baseline = entry.observed.lock().unwrap().clone();
            let manifest = entry.node.lock().await.manifest().clone();
            (baseline, manifest)
        };

        let peers = self.transport.peers_for(&entry.config.peers).await;
        // The whole peer step, every peer together. Timed as one phase because
        // that is the unit a reader can act on; a per-peer split is a later
        // change if this turns out to be where the time goes.
        let reconcile_phase = Instant::now();
        for peer in &peers {
            if self.cancel.is_cancelled() {
                break;
            }
            let peer_started = Instant::now();
            let outcome = self
                .transport
                .reconcile(peer.clone(), name.to_string(), entry.node.clone())
                .await;
            // Per peer, every pass, whether or not it changed anything. The
            // aggregate reconcile counter says the peer step is 91% of a pass;
            // it cannot say whether both peers cost the same. A relay-routed
            // peer and a direct one in the same window are the case that
            // matters, and the aggregate hides it.
            // INFO, not DEBUG. The daemon's default validation filter is
            // `fabric=info` (see `validation_log_filter`), so a `debug!` here is
            // dropped and the diagnostic is SILENT rather than quiet. It shipped
            // that way in #69 and emitted nothing at all on the live daemon.
            tracing::info!(
                target: VALIDATION_LOG_TARGET,
                event = "reconcile_peer",
                sync = name,
                peer = peer.id,
                micros = peer_started.elapsed().as_micros() as u64,
                failed = outcome.is_err(),
                "per-peer reconcile cost"
            );
            match outcome {
                Ok(stats) => {
                    if !stats.is_noop() {
                        tracing::debug!(sync = name, peer = peer.id, ?stats, "sync reconciled");
                    }
                    // A completed reconcile means this peer has merged the
                    // manifest we just sent, tombstones included. That is the
                    // only evidence the sweep is allowed to act on.
                    entry
                        .peer_acks
                        .lock()
                        .unwrap()
                        .insert(peer.id.clone(), now_secs());
                }
                Err(error) => {
                    tracing::debug!(sync = name, peer = peer.id, %error, "sync reconcile failed");
                }
            }
        }

        EntryWork::add_phase(&entry.work.reconcile_micros, reconcile_phase);

        let _operation = entry.operation.lock().await;
        let phase = Instant::now();
        self.scan_entry(&entry).await?;
        EntryWork::add_phase(&entry.work.scan_micros, phase);
        let phase = Instant::now();
        self.materialize_entry_state(&entry, &baseline).await?;
        EntryWork::add_phase(&entry.work.materialize_micros, phase);
        self.sweep_entry_tombstones(&entry, &peers).await;
        let final_manifest = entry.node.lock().await.manifest().clone();
        let final_observed = entry.observed.lock().unwrap().clone();
        if final_manifest != manifest || final_observed != baseline {
            let phase = Instant::now();
            self.persist_entry(&entry).await?;
            EntryWork::add_phase(&entry.work.persist_micros, phase);
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
        let mut cache = entry.scan_cache.lock().unwrap();
        scan_into_node_observed(&mut node, &root, &cfg, policy, &mut observed, &mut cache)
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
        // Same lock order as `scan_entry`: node, then observed, then cache.
        let cache = entry.scan_cache.lock().unwrap();
        materialize_tracked(
            &mut node,
            &root,
            policy,
            protected,
            &mut observed,
            &cache,
            Some((&entry.work, generation)),
        )
    }

    /// Forget tombstones this node can prove are dead and replicated.
    ///
    /// Called after the peer loop so the acks are as fresh as possible, and
    /// inside the caller's operation guard so a scan cannot interleave. It is a
    /// no-op unless the sweep is explicitly enabled, the policy sweeps, and
    /// every configured peer has acked while this node held the tombstone.
    async fn sweep_entry_tombstones(&self, entry: &EntryState, peers: &[PeerRef]) {
        let Some(ttl_secs) = tombstone_sweep_ttl_secs() else {
            self.report_sweep_state(entry, SweepState::Disabled);
            return;
        };
        if !entry.policy.sweep_tombstones {
            self.report_sweep_state(entry, SweepState::PolicyRetains);
            return;
        }
        let state = {
            let acks = entry.peer_acks.lock().unwrap();
            ack_gate(&entry.config.peers, peers, &acks)
        };
        self.report_sweep_state(entry, state.clone());
        let SweepState::Ready { acked_through } = state else {
            // Refusing is normal and usually correct. It is now also VISIBLE:
            // `report_sweep_state` has recorded the named reason.
            return;
        };
        let observed = entry.observed.lock().unwrap().clone();
        let evidence = SweepEvidence {
            now_secs: now_secs(),
            ttl_secs,
            acked_through: Some(acked_through),
        };
        let swept = {
            // Node first: a std guard held across the node's await point is not
            // Send, and this runs inside a spawned task.
            let mut node = entry.node.lock().await;
            let mut expired_since = entry.expired_since.lock().unwrap();
            node.sweep_tombstones(entry.policy, evidence, &observed, &mut expired_since)
        };
        if !swept.is_empty() {
            tracing::info!(
                sync = entry.config.name,
                swept = swept.len(),
                "swept expired tombstones"
            );
        }
    }

    /// Record the sweep state, and log it only when it CHANGES.
    ///
    /// The sweep runs on every pass. Logging the reason each time would add
    /// thousands of identical lines an hour and bury the signal it exists to
    /// provide, so the log fires on a transition and `fabric sync ls` answers
    /// the rest of the time.
    fn report_sweep_state(&self, entry: &EntryState, state: SweepState) {
        let changed = {
            let mut last = entry.last_sweep.lock().unwrap();
            if last.as_ref() == Some(&state) {
                false
            } else {
                *last = Some(state.clone());
                true
            }
        };
        if !changed {
            return;
        }
        match &state {
            SweepState::WaitingOnPeers(waiting) => tracing::info!(
                sync = entry.config.name,
                waiting_on = waiting.join(","),
                "tombstone sweep is waiting on a peer ack"
            ),
            SweepState::PeersAreWildcard => tracing::warn!(
                sync = entry.config.name,
                "tombstone sweep can NEVER run: a wildcard peer set cannot prove receipt"
            ),
            SweepState::PeersUnresolved {
                configured,
                resolved,
            } => tracing::info!(
                sync = entry.config.name,
                configured,
                resolved,
                "tombstone sweep is waiting: a configured peer is not in the peer book"
            ),
            SweepState::Ready { acked_through } => tracing::info!(
                sync = entry.config.name,
                acked_through,
                "tombstone sweep gate is open"
            ),
            SweepState::Disabled | SweepState::PolicyRetains => tracing::debug!(
                sync = entry.config.name,
                state = state.token(),
                "tombstone sweep is off"
            ),
        }
    }

    async fn persist_entry(&self, entry: &EntryState) -> Result<()> {
        #[cfg(test)]
        entry.work.persist_calls.fetch_add(1, Ordering::Relaxed);
        let manifest = entry.node.lock().await.manifest().clone();
        let observed = entry.observed.lock().unwrap().clone();
        let scan_cache = entry.scan_cache.lock().unwrap().clone();
        let peer_acks = entry.peer_acks.lock().unwrap().clone();
        self.write_state(
            &entry.config.name,
            &PersistedEntryState {
                manifest,
                observed,
                scan_cache,
                peer_acks,
            },
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
        // COMPACT, NOT PRETTY. This file is fabric's own index, not anyone's
        // data, and nobody reads 26 MB of JSON by eye. The indentation was 62%
        // of every byte written, and this file is rewritten WHOLE every time.
        let raw = serde_json::to_vec(manifest)?;
        write_atomic(&path, &raw)
    }

    fn write_state(&self, name: &str, state: &PersistedEntryState) -> Result<()> {
        let path = self.state_path(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Compact for the same reason as the manifest. Still JSON, so a build
        // that predates this reads it unchanged; only the whitespace is gone.
        let raw = serde_json::to_vec(state)?;
        // The combined state is authoritative and lands atomically first.
        write_atomic(&path, &raw)?;
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
        let _watcher = spawn_watcher(&root, tx, entry.work.clone(), entry.config.clone());

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
    /// Calls to `sync_once`. NOT `full_scans`, which is two per call.
    pub sync_passes: u64,
    /// Cumulative microseconds inside each phase of `sync_once`. Take two
    /// samples and divide; a total on its own describes the past.
    pub scan_micros: u64,
    pub materialize_micros: u64,
    pub persist_micros: u64,
    pub reconcile_micros: u64,
    /// Why the tombstone sweep did or did not forget anything, as last decided.
    /// `None` means no pass has reached the sweep yet for this entry instance.
    pub sweep: Option<SweepState>,
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
    size: u64,
    /// The executable bit as read from local disk, the one permission git
    /// tracks and therefore the one fabric replicates.
    executable: bool,
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
impl ScannedFile {
    fn cache_entry(&self) -> ScanCacheEntry {
        ScanCacheEntry {
            size: self.size,
            mtime_secs: self.mtime_secs,
            mtime_nanos: self.mtime_nanos,
            hash: self.hash,
        }
    }
}

/// Scan `root`, reusing a recorded hash when this machine's own disk facts are
/// unchanged.
///
/// The cache is keyed on `cache`, a LOCAL record, and never on the replicated
/// manifest. That distinction is the whole fix for the three-node divergence:
/// keying it on the manifest made a local caching decision from a value another
/// machine chose, so two contending entries of equal size could collide on size
/// plus mtime and the cache then reported content the file did not hold.
fn scan_folder(
    root: &Path,
    entry: &SyncEntry,
    cache: &HashMap<String, ScanCacheEntry>,
) -> Result<Vec<ScannedFile>> {
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
                // Git tracks a symlink as a first-class object; fabric does not
                // yet, because a symlink is a different KIND of manifest entry
                // rather than a file with a flag. Skipping in silence was the
                // problem: whoever hits it should see the gap named.
                eprintln!(
                    "fabric: skipping symlink {} — fabric does not sync symlinks, git does",
                    path.display()
                );
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
            // One stat per file, not two. `DirEntry::metadata` is a fresh
            // lstat on every call, and this loop used to call it twice: once
            // inside `mtime_of` and again on the next line. `mtime_of_metadata`
            // already takes the result, so the single-call shape was in the
            // file the whole time.
            let disk = child.metadata().ok();
            let (mtime_secs, mtime_nanos) = disk
                .as_ref()
                .map(mtime_of_metadata)
                .unwrap_or((0, 0));
            let size = disk.as_ref().map(|meta| meta.len()).unwrap_or(u64::MAX);
            let executable = disk.as_ref().is_some_and(is_executable);
            // Reuse the recorded hash when size and both mtime components are
            // byte-identical to what THIS MACHINE last observed. Anything that
            // differs, or is unknown, is read and hashed as before.
            let known = cache.get(&norm).filter(|seen| {
                seen.size == size
                    && seen.mtime_secs == mtime_secs
                    && seen.mtime_nanos == mtime_nanos
            });
            let (bytes, hash) = match known {
                Some(seen) => (None, seen.hash),
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
                size,
                executable,
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
    let mut cache = HashMap::new();
    scan_into_node_observed(node, root, entry, policy, &mut observed, &mut cache)
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
    cache: &mut HashMap<String, ScanCacheEntry>,
) -> Result<HashMap<String, ContentHash>> {
    let mut observed = HashMap::new();
    for file in scan_folder(&entry.folder, entry, cache)? {
        let hash = file.hash;
        cache.insert(file.rel.clone(), file.cache_entry());
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
    cache: &mut HashMap<String, ScanCacheEntry>,
) -> Result<bool> {
    // A ROOT THAT IS NOT THERE IS NOT A FOLDER SOMEBODY EMPTIED.
    //
    // `scan_folder` returns an empty result for a missing root. Every tracked
    // path would then be in the observed set and absent from the scan, which is
    // the same shape as a local delete, so a delete-propagating policy would
    // tombstone the entire entry and send that to every peer.
    //
    // A root vanishes for reasons that are not a deletion: an unmounted volume,
    // a directory renamed while a pass runs, a mount that is not ready at boot.
    // The blast radius is the whole entry rather than one path, so this is the
    // one place where doing nothing is clearly right. Wait until we can see it.
    //
    // Deleting the CONTENTS of a folder still propagates normally, because the
    // root survives that and the scan runs.
    if !root.exists() {
        return Ok(false);
    }
    let scanned = scan_folder(root, entry, cache)?;
    // Refresh the cache from what this scan actually saw, so the next scan of an
    // untouched file is free. Rebuilt rather than merged, so a vanished path
    // does not leak an entry forever.
    *cache = scanned
        .iter()
        .map(|file| (file.rel.clone(), file.cache_entry()))
        .collect();
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
                if node.local_write_with_mode(
                    &file.rel,
                    &file.read_bytes()?,
                    file.mtime_secs,
                    file.mtime_nanos,
                    file.executable,
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
        } else if node.local_write_with_mode(
            &file.rel,
            &file.read_bytes()?,
            file.mtime_secs,
            file.mtime_nanos,
            file.executable,
        ) {
            changed = true;
        }
    }

    let now = now_secs();
    for path in previous.keys() {
        if current.contains_key(path) {
            continue;
        }
        // A PATH THE ENTRY NO LONGER SELECTS HAS NOT BEEN DELETED. It has left
        // this entry's scope, and the two are indistinguishable from here: both
        // look like "in my records, absent from my scan".
        //
        // Treating the first as the second removed thirteen live files from
        // three machines on 2026-08-25. `plans/**` was taken out of an entry's
        // include, which left its paths recorded but unscannable, and the next
        // release turned delete propagation on. Every one of those files was
        // recoverable from git, which is the only reason it was recoverable.
        //
        // Narrowing an include is a scope change. Nobody deleted anything.
        if !entry.includes(path) {
            continue;
        }
        if node.local_remove(path, policy, now) {
            changed = true;
        }
    }
    // NOT DONE HERE: dropping the excluded paths from the manifest outright.
    // It is the tidier half of the rule and it needs its own change, because a
    // peer whose config still selects the path would send it back on every
    // reconcile and this side would drop it again, writing the whole index each
    // time. Fixing a delete by inventing a write loop is not a fix. Filtering on
    // adopt is the likelier answer and it changes what crosses the wire.
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
            // Stamp the origin's mtime. Safe now, and it was not before.
            //
            // Stamping once fed scan_folder's cache, whose key came from the
            // REPLICATED manifest. That made a local caching decision from a
            // value another machine chose: two nodes contending on one key could
            // produce entries with identical size and identical mtime but
            // different content, the scan reused the recorded hash and reported
            // content the file did not hold, and the versions leapfrogged
            // forever. Measured on Linux CI run 30814462024 attempt 4:
            // twenty-four consecutive republishes from v44 to v67, no
            // convergence.
            //
            // The cache is now keyed on this machine's own observed disk facts,
            // so a peer's mtime cannot collide into it. Stamping is once again
            // just metadata preservation, which is what FileMeta always claimed.
            write_atomic_with_mode(&path, bytes, meta.executable)?;
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
/// Whether the disk already holds the manifest's bytes, decided from a `stat`.
///
/// Materialization used to read and hash EVERY present file on EVERY pass. On a
/// converged tree that answer is always "unchanged", so the read and the hash
/// were pure waste. Measured on the live Silber daemon: one entry re-read and
/// re-hashed 70,157,702 bytes every 0.51 s, which is 136 MB/s, and
/// `blake3_hash_many_neon` was 14.22% of one core.
///
/// This asks the same question `scan_folder` asks, from the same evidence: size
/// and both mtime components byte-identical to what THIS MACHINE last observed.
/// Trusting that is not a new risk. `scan_folder` already runs first in every
/// pass and already trusts it, so re-deriving the opposite answer here from the
/// same disk was incoherent rather than safe.
///
/// A local cache entry is trusted by design; see
/// `materialization_does_not_manufacture_an_mtime_collision`, which pins that
/// only this machine can create one, so no peer can manufacture a hit.
///
/// Anything unknown, differing, or unreadable returns false and takes the full
/// read-and-hash path exactly as before.
fn already_materialized(
    path: &Path,
    rel: &str,
    meta: &FileMeta,
    cache: &HashMap<String, ScanCacheEntry>,
) -> bool {
    let Some(seen) = cache.get(rel) else {
        return false;
    };
    // The cache must agree with the manifest, or there is real work to do.
    if seen.hash != meta.hash {
        return false;
    }
    let Ok(disk) = std::fs::metadata(path) else {
        return false;
    };
    let (mtime_secs, mtime_nanos) = mtime_of_metadata(&disk);
    seen.size == disk.len() && seen.mtime_secs == mtime_secs && seen.mtime_nanos == mtime_nanos
}

fn materialize_tracked(
    node: &mut SyncNode,
    root: &Path,
    policy: PolicyRules,
    protected: &HashMap<String, ContentHash>,
    observed: &mut HashMap<String, ContentHash>,
    cache: &HashMap<String, ScanCacheEntry>,
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
        if already_materialized(&path, &rel, &meta, cache) {
            observed.insert(rel.clone(), meta.hash);
            continue;
        }
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
                    let executable = std::fs::metadata(&path)
                        .ok()
                        .is_some_and(|meta| is_executable(&meta));
                    node.local_write_with_mode(&rel, &existing, 0, 0, executable);
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
            // Stamp the origin's mtime. Safe now, and it was not before.
            //
            // Stamping once fed scan_folder's cache, whose key came from the
            // REPLICATED manifest. That made a local caching decision from a
            // value another machine chose: two nodes contending on one key could
            // produce entries with identical size and identical mtime but
            // different content, the scan reused the recorded hash and reported
            // content the file did not hold, and the versions leapfrogged
            // forever. Measured on Linux CI run 30814462024 attempt 4:
            // twenty-four consecutive republishes from v44 to v67, no
            // convergence.
            //
            // The cache is now keyed on this machine's own observed disk facts,
            // so a peer's mtime cannot collide into it. Stamping is once again
            // just metadata preservation, which is what FileMeta always claimed.
            write_atomic_with_mode(&path, bytes, meta.executable)?;
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

/// Fabric cannot propagate a metadata-only change. ONE CAUSE, TWO SYMPTOMS.
///
/// `SyncNode::local_write` returns early when the content hash is unchanged and
/// discards whatever metadata came with that write. That early return is what
/// makes applying a peer's content echo-free, so it is load-bearing rather than
/// an oversight. The consequence is that any change which alters no bytes never
/// advances a logical version, and a change that does not advance a version
/// never crosses the wire.
///
/// Both known symptoms are this one cause. Fix it and you fix both; fix either
/// symptom alone and you have not touched the cause:
///
/// 1. **A heartbeat is invisible.** Rewriting the same bytes with a new mtime
///    does not propagate, so a replica keeps the older timestamp. Issue 27: st2
///    derived agent liveness from a replica's mtime and reported live remote
///    agents as unknown.
/// 2. **A chmod is invisible.** `chmod +x` on an already-synced file changes no
///    bytes, so the new mode does not propagate. A NEW file carries its mode
///    correctly, which is the case that actually bites, but an existing one does
///    not change.
///
/// Symptom 2 is a DIVERGENCE FROM GIT, not merely a limitation, now that a
/// catalog is meant to be carriable by either transport. Git propagates a chmod:
/// a mode change rewrites the tree object, so it is a real commit with real
/// content. Fabric does not.
///
/// Closing this needs a local metadata-only change to advance a version while a
/// received one stays inert, which puts an asymmetry into the exact mechanism
/// that prevents infinite echo. That is a core engine change and it is not
/// authorized.
pub(crate) const METADATA_ONLY_CHANGES_DO_NOT_PROPAGATE: () = ();

/// Write bytes atomically, applying the executable bit fabric replicates.
///
/// A materialized file does NOT receive the origin's modification time. Fabric
/// syncs the attributes git syncs, and git deliberately does not track mtime.
/// `FileMeta` still carries one, but it is informational only: see the note on
/// that field, and on [`METADATA_ONLY_CHANGES_DO_NOT_PROPAGATE`].
fn write_atomic_with_mode(path: &Path, bytes: &[u8], executable: bool) -> Result<()> {
    write_atomic_inner(path, bytes, executable)
}

/// Write bytes atomically, for fabric's own state files, which are never
/// executable.
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    write_atomic_inner(path, bytes, false)
}

fn write_atomic_inner(path: &Path, bytes: &[u8], executable: bool) -> Result<()> {
    let tmp = path.with_extension(format!(
        "{}.fabric-tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("")
    ));
    std::fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    // Set the mode on the temp file, before the rename, so the file never
    // appears at its final path with the wrong permissions.
    if executable && let Err(error) = set_executable(&tmp) {
        // A failed chmod must not fail the write. The content is what the sync
        // is for, and a non-executable copy is recoverable by hand; a lost file
        // is not.
        eprintln!(
            "fabric: could not set the executable bit on {}: {error:#}",
            tmp.display()
        );
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to rename into {}", path.display()))?;
    Ok(())
}

/// Set a file's modification time, leaving its access time alone.
///
/// Mark a file executable, mirroring git's 755. Only the executable bits are
/// touched; fabric replicates no other permission bit, because git does not.
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?
        .permissions();
    let mode = perms.mode();
    perms.set_mode(mode | 0o111);
    std::fs::set_permissions(path, perms)
        .with_context(|| format!("failed to set the executable bit on {}", path.display()))?;
    Ok(())
}

/// Read whether a file is executable, the way git decides it.
fn is_executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o111 != 0
}

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

fn mtime_of_metadata(meta: &std::fs::Metadata) -> (i64, u32) {
    let Ok(modified) = meta.modified() else {
        return (0, 0);
    };
    match modified.duration_since(UNIX_EPOCH) {
        Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos()),
        Err(err) => (-(err.duration().as_secs() as i64), 0),
    }
}

/// Resolve the tombstone sweep window from `FABRIC_TOMBSTONE_SWEEP_DAYS`.
///
/// Unset means the sweep is OFF. That is deliberate: forgetting a tombstone is
/// the one sync operation that can bring a deleted file back across a fleet, so
/// it is opt-in per machine rather than a default that arrives with an upgrade.
/// A value that is absent, unparseable, or not positive disables it.
pub(crate) fn resolve_tombstone_sweep_days(raw: Option<&str>) -> Option<i64> {
    let days: i64 = raw?.trim().parse().ok()?;
    (days > 0).then_some(days)
}

fn tombstone_sweep_ttl_secs() -> Option<i64> {
    resolve_tombstone_sweep_days(std::env::var("FABRIC_TOMBSTONE_SWEEP_DAYS").ok().as_deref())
        .map(|days| days * 24 * 60 * 60)
}

/// The local time through which EVERY configured peer has completed a
/// reconcile, or `None` when that cannot be proven for all of them.
///
/// `resolved` is what `peers_for` returned. It silently drops a configured peer
/// that is not in the peer book, so a short list is itself a reason to refuse:
/// a peer we cannot even resolve is a peer we cannot claim has our tombstones.
/// A wildcard entry is refused outright, because its peer set is whatever the
/// book holds right now and a peer leaving the book must not be what makes a
/// deletion forgettable.
/// Why a tombstone sweep did or did not forget anything.
///
/// The sweep refusing is normal and usually correct. It refusing SILENTLY is
/// not: "waiting on a peer" and "nothing to sweep" looked identical from
/// outside, so an entry could wait on one roaming peer forever and report the
/// same thing as a healthy entry with no expired tombstones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SweepState {
    /// `FABRIC_TOMBSTONE_SWEEP_DAYS` is unset. Off is the default.
    Disabled,
    /// This policy retains tombstones by design. Catalog never sweeps.
    PolicyRetains,
    /// The peer set is a wildcard, so membership is whatever `peers.toml` holds
    /// at this moment. A peer leaving the book must never be what makes a
    /// deletion forgettable, so a wildcard entry can NEVER sweep.
    ///
    /// This is the trapdoor: `"*"` is the obvious way to add a peer, and doing
    /// so turns the sweep off rather than blocking it.
    PeersAreWildcard,
    /// A configured peer is absent from the local peer book, so it cannot be
    /// asked and its receipt cannot be proven.
    PeersUnresolved { configured: usize, resolved: usize },
    /// These peers have not acked while this node held the tombstones. Named,
    /// because "waiting" without a name is the thing that was missing.
    WaitingOnPeers(Vec<String>),
    /// The gate is open.
    Ready { acked_through: i64 },
}

impl SweepState {
    /// A short stable token for `fabric sync ls`.
    pub fn token(&self) -> String {
        match self {
            SweepState::Disabled => "disabled".to_string(),
            SweepState::PolicyRetains => "policy-retains".to_string(),
            SweepState::PeersAreWildcard => "never-sweeps-wildcard-peers".to_string(),
            SweepState::PeersUnresolved {
                configured,
                resolved,
            } => format!("peers-unresolved:{resolved}/{configured}"),
            SweepState::WaitingOnPeers(peers) => format!("waiting-on:{}", peers.join(",")),
            SweepState::Ready { .. } => "ready".to_string(),
        }
    }
}

/// Decide the gate AND say why, so the refusal can be reported.
fn ack_gate(
    configured: &SyncPeers,
    resolved: &[PeerRef],
    acks: &HashMap<String, i64>,
) -> SweepState {
    let SyncPeers::List(selectors) = configured else {
        return SweepState::PeersAreWildcard;
    };
    if selectors.is_empty() || resolved.len() != selectors.len() {
        return SweepState::PeersUnresolved {
            configured: selectors.len(),
            resolved: resolved.len(),
        };
    }
    let mut waiting: Vec<String> = resolved
        .iter()
        .filter(|peer| !acks.contains_key(&peer.id))
        .map(|peer| peer.id.clone())
        .collect();
    if !waiting.is_empty() {
        waiting.sort();
        return SweepState::WaitingOnPeers(waiting);
    }
    let mut earliest = i64::MAX;
    for peer in resolved {
        earliest = earliest.min(acks[&peer.id]);
    }
    if earliest == i64::MAX {
        return SweepState::PeersUnresolved {
            configured: selectors.len(),
            resolved: resolved.len(),
        };
    }
    SweepState::Ready {
        acked_through: earliest,
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

/// Whether a watched path can affect what this entry syncs.
///
/// Issue #57. An entry watches its whole root, but it syncs only what its
/// include globs select. On the live Silber daemon the declarations entry
/// selects 64 of 14,368 files, and st2 continuously writes files it does not
/// select: agent `status` files, and `pty/*.events.jsonl`. Every one of those
/// writes woke a full scan, so a tree whose selected files had not changed for
/// 101.6 hours was scanned about twice a second.
///
/// The default is to KEEP. An entry with no include globs selects everything,
/// so `includes` returns true and nothing is dropped. Anything this cannot
/// judge is kept too, because a missed real change is far worse than a wasted
/// scan: the cost of keeping is one scan, and the cost of dropping wrongly is a
/// declaration that does not propagate until the safety scan.
///
/// A directory is always kept. A directory event names the directory, not the
/// descendants the entry may sync under it.
/// Both spellings of the watched root: as configured, and as the OS resolves it.
///
/// A watcher reports the real path. Where the root reaches through a symlink,
/// which is the normal case for a macOS temp dir and can be the case for a
/// home, the two differ and only the resolved one strips cleanly.
fn watch_roots_for(root: &Path) -> Vec<PathBuf> {
    let mut roots = vec![root.to_path_buf()];
    if let Ok(resolved) = std::fs::canonicalize(root)
        && resolved != *root
    {
        roots.push(resolved);
    }
    roots
}

fn watch_path_is_relevant(roots: &[PathBuf], path: &Path, cfg: &SyncEntry) -> bool {
    let Some(rel) = roots.iter().find_map(|root| path.strip_prefix(root).ok()) else {
        // Outside every spelling of the root. Not ours to judge, so keep.
        return true;
    };
    let rel = rel.to_string_lossy();
    let Some(norm) = Manifest::normalize_path(&rel) else {
        // The root itself, or a path that does not normalize. Keep.
        return true;
    };
    if cfg.includes(&norm) {
        return true;
    }
    // A delete leaves nothing to stat, and `is_dir` is false for it. That is
    // the right way round: under catalog policy a delete does not propagate,
    // and the safety scan is the backstop for every other case.
    path.is_dir()
}

fn spawn_watcher(
    root: &Path,
    tx: mpsc::Sender<WatchEvent>,
    work: Arc<EntryWork>,
    cfg: SyncEntry,
) -> Option<notify::RecommendedWatcher> {
    use notify::{RecursiveMode, Watcher};

    // Create the folder BEFORE resolving the root, not just before watching it.
    // `canonicalize` fails on a path that does not exist, so a first run would
    // otherwise resolve nothing and carry only the configured spelling.
    let _ = std::fs::create_dir_all(root);

    // The watcher reports the REAL path. On macOS the temp and home roots run
    // through a symlink, so `/var/...` is reported as `/private/var/...` and a
    // strip against the configured root fails for every event. That failure is
    // safe, because an unjudgeable path is kept, but it would make this filter
    // silently inert on exactly the machine that needs it. Carry both spellings.
    let watch_roots = watch_roots_for(root);
    let mut watcher =
        match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res
                && watcher_event_is_mutation(&event.kind)
            {
                let paths: Vec<PathBuf> = event
                    .paths
                    .into_iter()
                    .filter(|path| watch_path_is_relevant(&watch_roots, path, &cfg))
                    .collect();
                // Nothing this entry syncs can have changed. Do NOT record a
                // mutation: the generation drives `dirty`, so bumping it here
                // would make the periodic tick scan anyway and the filter would
                // buy nothing.
                if paths.is_empty() {
                    return;
                }
                let generation = work.record_mutation();
                let _ = tx.try_send(WatchEvent {
                    paths,
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
    use crate::sync::manifest::{Author, Entry, FileMeta, Tombstone};
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
            &HashMap::new(),
            None,
        )?;
        assert!(root.join("from-peer.md").exists(), "file was materialized");

        let scanned = scan_folder(root, &entry, &HashMap::new())?;
        let file = scanned
            .iter()
            .find(|f| f.rel == "from-peer.md")
            .expect("materialized file is in scope");
        // The peer's mtime is NOT applied. Fabric syncs what git syncs, and git
        // does not track mtime, so a replica carries its own write time.
        assert_ne!(
            (file.mtime_secs, file.mtime_nanos),
            (remote_mtime_secs, remote_mtime_nanos),
            "materialization must not apply the origin mtime; git does not track it"
        );
        // It is still read once here, because this scan runs with a COLD cache.
        // The cache is only ever filled from real disk observations, never from
        // the value that was requested, so a filesystem that truncates a stamp
        // cannot make it miss forever.
        assert!(
            file.bytes.is_some(),
            "a cold cache must read the file rather than assume its hash"
        );

        // Warm the cache the way a real scan does, from what was OBSERVED, and
        // the next scan is free.
        let cache: HashMap<String, ScanCacheEntry> = scanned
            .iter()
            .map(|f| (f.rel.clone(), f.cache_entry()))
            .collect();
        let again = scan_folder(root, &entry, &cache)?;
        let file = again
            .iter()
            .find(|f| f.rel == "from-peer.md")
            .expect("still in scope");
        assert!(
            file.bytes.is_none(),
            "a materialized file must hit the cache on the next scan; before this \
             change it missed forever"
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

        let first = scan_folder(root, &entry, &HashMap::new())?;
        assert_eq!(first.len(), 2);
        assert!(
            first.iter().all(|file| file.bytes.is_some()),
            "an unknown file must be read and hashed"
        );

        // Record what that scan learned, the way a real reconcile would. The
        // cache is built from what was OBSERVED on disk, never from a manifest.
        let mut node = SyncNode::new(Author([7u8; 32]));
        for file in &first {
            node.local_write(
                &file.rel,
                file.bytes.as_ref().expect("first scan reads bytes"),
                file.mtime_secs,
                file.mtime_nanos,
            );
        }
        let cache: HashMap<String, ScanCacheEntry> = first
            .iter()
            .map(|f| (f.rel.clone(), f.cache_entry()))
            .collect();

        let second = scan_folder(root, &entry, &cache)?;
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
        let third = scan_folder(root, &entry, &cache)?;
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

    fn peer(id: &str) -> PeerRef {
        PeerRef {
            id: id.to_string(),
            addr: None,
        }
    }

    /// What the materialize re-read costs in RESIDENT MEMORY, measured.
    ///
    /// Ignored by default because it is a measurement, not a guard. Run it with
    /// `cargo test --release -- --ignored --nocapture materialize_resident_cost`.
    ///
    /// Sized to the live `st2-declarations-default` entry observed on
    /// 2026-08-19: three files of about 23 MB, 70.2 MB total, passed over once
    /// or twice a second. On 19 August the daemon's RSS sat at 2.52 GB with
    /// 2.2 GB resident and dirty in EMPTY large-allocation regions, while RSS
    /// was flat under 12.5 GB of churn per 90 s. That is retention, and this
    /// measures whether the read is what feeds it.
    #[test]
    #[ignore = "measurement, not a guard"]
    fn materialize_resident_cost_with_and_without_the_cache() {
        fn rss_kb() -> u64 {
            let out = std::process::Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()
                .expect("ps");
            String::from_utf8_lossy(&out.stdout)
                .trim()
                .parse()
                .unwrap_or(0)
        }

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        std::fs::create_dir_all(&root).unwrap();

        // Three files the size of the real build artifacts.
        let mut node = SyncNode::new(Author([3u8; 32]));
        let blob = vec![0xABu8; 23_346_256];
        for name in ["a.bin", "b.bin", "c.bin"] {
            node.local_write(name, &blob, 1_700_000_000, 0);
        }
        drop(blob);

        let mut observed = HashMap::new();
        materialize_tracked(
            &mut node,
            &root,
            SyncPolicy::Catalog.rules(),
            &HashMap::new(),
            &mut observed,
            &HashMap::new(),
            None,
        )
        .unwrap();

        // The cache a scan would have recorded for these three files.
        let mut cache = HashMap::new();
        let mut total = 0u64;
        for (rel, meta) in node
            .manifest()
            .present_paths()
            .map(|(r, m)| (r.clone(), *m))
            .collect::<Vec<_>>()
        {
            let path = root.join(&rel);
            let disk = std::fs::metadata(&path).unwrap();
            let (secs, nanos) = mtime_of_metadata(&disk);
            total += disk.len();
            cache.insert(
                rel,
                ScanCacheEntry {
                    size: disk.len(),
                    mtime_secs: secs,
                    mtime_nanos: nanos,
                    hash: meta.hash,
                },
            );
        }
        println!(
            "corpus: {} files, {:.1} MB",
            cache.len(),
            total as f64 / 1e6
        );

        const PASSES: usize = 200;

        let before = rss_kb();
        for _ in 0..PASSES {
            let mut obs = HashMap::new();
            materialize_tracked(
                &mut node,
                &root,
                SyncPolicy::Catalog.rules(),
                &HashMap::new(),
                &mut obs,
                &HashMap::new(),
                None,
            )
            .unwrap();
        }
        let after_reads = rss_kb();

        for _ in 0..PASSES {
            let mut obs = HashMap::new();
            materialize_tracked(
                &mut node,
                &root,
                SyncPolicy::Catalog.rules(),
                &HashMap::new(),
                &mut obs,
                &cache,
                None,
            )
            .unwrap();
        }
        let after_cached = rss_kb();

        println!(
            "RSS MB: start {:.0}, after {} re-reading passes {:.0}, after {} cached passes {:.0}",
            before as f64 / 1024.0,
            PASSES,
            after_reads as f64 / 1024.0,
            PASSES,
            after_cached as f64 / 1024.0
        );
        println!(
            "bytes read: re-reading path {:.1} GB, cached path 0.0 GB",
            (total as f64 * PASSES as f64) / 1e9
        );
    }

    /// What the sweep is worth, measured rather than asserted.
    ///
    /// Ignored by default because it is a measurement, not a guard. Run it with
    /// `cargo test --release -- --ignored --nocapture manifest_decode_cost`.
    /// The corpus is sized to the live `st2-bus-default` manifest observed on
    /// 2026-08-14: 11,636 present and 11,771 tombstones.
    #[test]
    #[ignore = "measurement, not a guard"]
    fn manifest_decode_cost_before_and_after_a_sweep() {
        const PRESENT: usize = 11_636;
        const TOMBSTONES: usize = 11_771;
        const ROUNDS: u32 = 5;

        let mut full = Manifest::new();
        for i in 0..PRESENT {
            full.insert(
                format!("agents/host{}/agent{i}/resources/item-{i}.json", i % 7),
                Entry::Present(FileMeta {
                    hash: ContentHash([(i % 251) as u8; 32]),
                    size: 1024 + i as u64,
                    executable: false,
                    mtime_secs: 1_786_700_000 + i as i64,
                    mtime_nanos: (i % 1_000_000_000) as u32,
                    version: 1 + (i % 5) as u64,
                    author: Author([(i % 13) as u8; 32]),
                }),
            );
        }
        let mut swept = full.clone();
        for i in 0..TOMBSTONES {
            full.insert(
                format!("agents/host{}/agent{i}/inbox/msg-{i}.md", i % 7),
                Entry::Tombstone(Tombstone {
                    version: 2,
                    author: Author([(i % 13) as u8; 32]),
                    deleted_secs: 1_786_000_000 + i as i64,
                }),
            );
        }

        let time = |manifest: &Manifest, label: &str| {
            let bytes = serde_json::to_vec(manifest).unwrap();
            let start = std::time::Instant::now();
            for _ in 0..ROUNDS {
                let decoded: Manifest = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(decoded.len(), manifest.len());
            }
            let per_decode = start.elapsed() / ROUNDS;
            println!(
                "{label}: {} entries, {:.1} MB, {:?} per decode",
                manifest.len(),
                bytes.len() as f64 / 1_048_576.0,
                per_decode
            );
            per_decode
        };

        // A real manifest beats a modelled one. Point FABRIC_MEASURE_MANIFEST at
        // a copy of a live manifest.json to measure the actual corpus; the
        // synthetic one above is the reproducible fallback.
        if let Ok(path) = std::env::var("FABRIC_MEASURE_MANIFEST") {
            let raw = std::fs::read(&path).expect("corpus file");
            let live: Manifest = serde_json::from_slice(&raw).expect("corpus parses as a manifest");
            let mut live_swept = Manifest::new();
            for (path, entry) in live.entries() {
                if entry.is_present() {
                    live_swept.insert(path.clone(), *entry);
                }
            }
            println!("corpus: {path}");
            let before = time(&live, "live before ");
            let after = time(&live_swept, "live after  ");
            println!(
                "decode cost removed by the sweep: {:.1}%",
                100.0 - (after.as_secs_f64() / before.as_secs_f64() * 100.0)
            );
            return;
        }

        // `swept` already holds only the present entries, which is exactly the
        // manifest a completed sweep leaves behind.
        let before = time(&full, "before sweep");
        let after = time(&mut swept, "after sweep ");
        println!(
            "decode cost removed by the sweep: {:.1}%",
            100.0 - (after.as_secs_f64() / before.as_secs_f64() * 100.0)
        );
    }

    #[test]
    fn the_tombstone_sweep_is_off_unless_explicitly_configured() {
        assert_eq!(resolve_tombstone_sweep_days(None), None, "unset means off");
        for raw in ["", "  ", "no", "0", "-1", "1.5", "many"] {
            assert_eq!(
                resolve_tombstone_sweep_days(Some(raw)),
                None,
                "{raw:?} must not enable the sweep"
            );
        }
        assert_eq!(resolve_tombstone_sweep_days(Some("7")), Some(7));
        assert_eq!(resolve_tombstone_sweep_days(Some(" 30 ")), Some(30));
    }

    /// Every reason the sweep can refuse must be NAMED, not merely absent.
    ///
    /// Nathan's rule on 2026-08-23: Bluey roams, and its reachability must
    /// never cause a concern. The sweep refusing is normal and usually
    /// correct. It refusing silently is the fault, because "waiting on a peer"
    /// and "nothing to sweep" looked identical from outside.
    #[test]
    fn every_sweep_refusal_says_why() {
        let both = SyncPeers::List(vec!["hetz".into(), "droppy".into()]);
        let resolved = vec![peer("hetz"), peer("droppy")];

        // 1. A peer that has never acked is NAMED.
        let acks = HashMap::from([("hetz".to_string(), 500)]);
        assert_eq!(
            ack_gate(&both, &resolved, &acks),
            SweepState::WaitingOnPeers(vec!["droppy".to_string()]),
            "the peer holding the sweep up must be named"
        );
        assert_eq!(
            ack_gate(&both, &resolved, &acks).token(),
            "waiting-on:droppy"
        );

        // 2. A configured peer absent from the peer book is reported as such,
        //    and NOT as a missing ack. They are different problems.
        assert_eq!(
            ack_gate(&both, &[peer("hetz")], &acks),
            SweepState::PeersUnresolved {
                configured: 2,
                resolved: 1
            }
        );

        // 3. The wildcard trapdoor. `"*"` is the obvious way to add a roaming
        //    peer, and it turns the sweep OFF rather than blocking it. It must
        //    say so rather than look like an ordinary empty sweep.
        let wildcard = SyncPeers::Wildcard("*".into());
        assert_eq!(
            ack_gate(&wildcard, &resolved, &acks),
            SweepState::PeersAreWildcard
        );
        assert_eq!(
            ack_gate(&wildcard, &resolved, &acks).token(),
            "never-sweeps-wildcard-peers",
            "the token must say it can never sweep, not merely that it did not"
        );

        // 4. The gate open, bounded by the SLOWEST peer.
        let acks = HashMap::from([("hetz".to_string(), 500), ("droppy".to_string(), 300)]);
        assert_eq!(
            ack_gate(&both, &resolved, &acks),
            SweepState::Ready { acked_through: 300 },
            "the slowest peer still bounds what we may forget"
        );
    }

    /// A roaming peer that is NOT configured on the entry cannot hold it up.
    ///
    /// This is the state on Silber on 2026-08-23: both entries read
    /// `peers=hetz,droppy`, and Bluey is only in the node peer book. The
    /// hazard is latent, and this pins that it stays latent.
    #[test]
    fn a_peer_outside_the_entry_cannot_block_its_sweep() {
        let configured = SyncPeers::List(vec!["hetz".into(), "droppy".into()]);
        let resolved = vec![peer("hetz"), peer("droppy")];
        let acks = HashMap::from([("hetz".to_string(), 500), ("droppy".to_string(), 500)]);

        assert_eq!(
            ack_gate(&configured, &resolved, &acks),
            SweepState::Ready { acked_through: 500 },
            "bluey is not configured on this entry, so it cannot gate it"
        );

        // Adding it to the ENTRY is what would gate it, and that must show as a
        // named wait rather than as silence.
        let with_bluey = SyncPeers::List(vec!["hetz".into(), "droppy".into(), "bluey".into()]);
        let resolved = vec![peer("hetz"), peer("droppy"), peer("bluey")];
        assert_eq!(
            ack_gate(&with_bluey, &resolved, &acks),
            SweepState::WaitingOnPeers(vec!["bluey".to_string()])
        );
    }

    #[test]
    fn acked_through_is_the_earliest_ack_and_only_when_every_peer_has_one() {
        let both = SyncPeers::List(vec!["a".into(), "b".into()]);
        let resolved = vec![peer("a"), peer("b")];
        let acks = HashMap::from([("a".to_string(), 500), ("b".to_string(), 300)]);
        assert_eq!(
            ack_gate(&both, &resolved, &acks),
            SweepState::Ready { acked_through: 300 },
            "the slowest peer bounds what we may forget"
        );

        // One peer has never acked: nothing is provable, so nothing is swept.
        let acks = HashMap::from([("a".to_string(), 500)]);
        assert_eq!(
            ack_gate(&both, &resolved, &acks),
            SweepState::WaitingOnPeers(vec!["b".to_string()])
        );
    }

    #[test]
    fn acked_through_refuses_a_wildcard_and_an_unresolvable_peer() {
        let acks = HashMap::from([("a".to_string(), 500), ("b".to_string(), 500)]);

        // A wildcard's peer set is whatever the book holds now. A peer leaving
        // the book must never be the thing that makes a deletion forgettable.
        let wildcard = SyncPeers::Wildcard("*".into());
        assert_eq!(
            ack_gate(&wildcard, &[peer("a"), peer("b")], &acks),
            SweepState::PeersAreWildcard
        );

        // peers_for silently drops a configured peer missing from the book, so
        // a short resolved list means we cannot speak for everyone.
        let three = SyncPeers::List(vec!["a".into(), "b".into(), "c".into()]);
        assert_eq!(
            ack_gate(&three, &[peer("a"), peer("b")], &acks),
            SweepState::PeersUnresolved {
                configured: 3,
                resolved: 2
            }
        );

        // An empty list would otherwise make the sweep vacuously safe.
        assert_eq!(
            ack_gate(&SyncPeers::List(vec![]), &[], &acks),
            SweepState::PeersUnresolved {
                configured: 0,
                resolved: 0
            }
        );
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

    /// The declarations entry as it is really configured: it selects agent
    /// declarations, templates and plans out of a catalog that also carries
    /// continuously written bus data.
    fn declarations_like_entry(root: &Path) -> SyncEntry {
        let mut cfg = entry_with_policy("declarations", root, SyncPolicy::Catalog);
        cfg.include = Some(vec![
            "_templates/**".to_string(),
            "**/agent.kdl".to_string(),
            "plans/**".to_string(),
        ]);
        cfg
    }

    /// Issue #57: a write the entry does not sync must not wake a scan.
    ///
    /// These are the paths st2 actually writes, taken from the live Silber
    /// catalog. Each one used to trigger a full scan of 14,368 files, which is
    /// why a tree whose selected files had not changed for 101.6 hours was
    /// scanned about twice a second.
    #[test]
    fn a_write_outside_the_glob_does_not_wake_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let cfg = declarations_like_entry(root);

        for noise in [
            "agents/Silber/cos/status",
            "agents/hetz/root/status",
            "pty/Silber.fabric.events.jsonl",
            "agents/Silber/cos/resources/archive/1785390445488-eavhqh.md",
        ] {
            let path = root.join(noise);
            assert!(
                !watch_path_is_relevant(&watch_roots_for(root), &path, &cfg),
                "{noise} is not selected by this entry, so it must not wake a scan"
            );
        }
    }

    /// THE REGRESSION THAT MATTERS, and the reason this filter is dangerous.
    ///
    /// If the filter over-matches, a real declaration change stops waking a
    /// scan and waits up to `MISSED_EVENT_RESYNC` instead of `WATCH_DEBOUNCE`.
    /// That is 300 s rather than 150 ms, and a slow declaration sync is how a
    /// fleet ends up running two versions of the truth. That is worse than the
    /// cost this filter removes.
    #[test]
    fn a_real_declaration_change_still_wakes_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let cfg = declarations_like_entry(root);

        for real in [
            "agents/Silber/fabric/agent.kdl",
            "agents/hetz/root/agent.kdl",
            "agents/.hetz-backup-1786658066/st2/agent.kdl",
            "_templates/Silber.root.AGENTS.md",
            "plans/artifacts/fabric/pr25/f26618d/RECEIPT.md",
        ] {
            let path = root.join(real);
            assert!(
                watch_path_is_relevant(&watch_roots_for(root), &path, &cfg),
                "{real} IS selected by this entry, so it must still wake a scan"
            );
        }
    }

    /// A directory event names the directory, not the descendants the entry
    /// syncs under it, so a directory is always kept.
    #[test]
    fn a_directory_event_is_kept_even_when_it_does_not_match() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let cfg = declarations_like_entry(root);

        let agent_dir = root.join("agents/Silber/fabric");
        std::fs::create_dir_all(&agent_dir).unwrap();
        assert!(
            !cfg.includes("agents/Silber/fabric"),
            "the directory itself does not match the glob"
        );
        assert!(
            watch_path_is_relevant(&watch_roots_for(root), &agent_dir, &cfg),
            "a directory can hold files the entry syncs, so it must be kept"
        );
    }

    /// An entry with no include globs selects everything, so the filter must be
    /// a no-op for it. This is what keeps the change safe for every other entry.
    #[test]
    fn an_entry_without_globs_keeps_every_event() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let cfg = entry_with_policy("bus", root, SyncPolicy::Bus);
        assert!(cfg.include.is_none(), "this entry selects everything");

        for any in ["a.md", "deep/nested/file.bin", "agents/x/status"] {
            assert!(
                watch_path_is_relevant(&watch_roots_for(root), &root.join(any), &cfg),
                "{any} must be kept when the entry has no globs"
            );
        }
    }

    /// A path the filter cannot judge is kept. Dropping wrongly costs a
    /// declaration that does not propagate; keeping wrongly costs one scan.
    #[test]
    fn a_path_outside_the_root_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let cfg = declarations_like_entry(root);
        assert!(
            watch_path_is_relevant(
                &watch_roots_for(root),
                Path::new("/somewhere/else/status"),
                &cfg
            ),
            "a path this cannot judge must be kept"
        );
        assert!(
            watch_path_is_relevant(&watch_roots_for(root), root, &cfg),
            "the root itself must be kept"
        );
    }

    /// The regression that a passing unit test could NOT have caught.
    ///
    /// The watcher reports the REAL path. A macOS temp dir, and a home, can
    /// reach through a symlink, so the watcher says `/private/var/...` while
    /// the entry says `/var/...`. A filter that strips only the configured root
    /// fails to strip, keeps the path, and is SILENTLY INERT on the machine
    /// that needs it. Every glob unit test still passes, because none of them
    /// goes through a watcher.
    ///
    /// This one does, so it fails if the roots stop being carried in both
    /// spellings.
    #[tokio::test]
    async fn the_filter_survives_a_root_that_reaches_through_a_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("agents/Silber/cos")).unwrap();
        std::fs::create_dir_all(root.join("agents/Silber/fabric")).unwrap();

        let (tx, mut rx) = mpsc::channel::<WatchEvent>(8);
        let work = EntryWork::new();
        let _watcher =
            spawn_watcher(root, tx, work.clone(), declarations_like_entry(root)).unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;

        // `EntryWork` starts at generation one on purpose, so compare against
        // what it was, not against zero.
        let quiet_generation = work.mutation_generation.load(Ordering::Acquire);

        // Noise the entry does not sync. No event may reach the loop.
        std::fs::write(root.join("agents/Silber/cos/status"), b"available").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), rx.recv())
                .await
                .is_err(),
            "a write outside the glob must not reach the sync loop; if the root \
             spelling stops matching, this filter goes silently inert"
        );
        assert_eq!(
            work.mutation_generation.load(Ordering::Acquire),
            quiet_generation,
            "and it must not bump the generation, or the periodic tick scans anyway"
        );

        // A real declaration change must still get through, promptly.
        std::fs::write(root.join("agents/Silber/fabric/agent.kdl"), b"agent {}").unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
        assert!(
            matches!(got, Ok(Some(_))),
            "a real declaration change must still wake the sync loop"
        );
    }

    /// The same inertness, reached a different way: a root that does not exist
    /// yet.
    ///
    /// `canonicalize` fails on a path that is not there, so resolving the root
    /// before creating it carries only the configured spelling, and the filter
    /// goes inert for the entry's whole first run.
    #[tokio::test]
    async fn the_filter_works_on_a_root_that_did_not_exist_yet() {
        let dir = tempfile::tempdir().unwrap();
        // Deliberately NOT created: spawn_watcher must create it before it
        // resolves it.
        let root = dir.path().join("fresh-entry");
        assert!(!root.exists(), "the root must not exist yet");

        let (tx, mut rx) = mpsc::channel::<WatchEvent>(8);
        let work = EntryWork::new();
        let _watcher =
            spawn_watcher(&root, tx, work.clone(), declarations_like_entry(&root)).unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        std::fs::create_dir_all(root.join("agents/Silber/cos")).unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        while tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_ok()
        {}

        std::fs::write(root.join("agents/Silber/cos/status"), b"available").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1500), rx.recv())
                .await
                .is_err(),
            "a write outside the glob must not reach the loop, even on the \
             entry's first run when the root had to be created first"
        );

        // POSITIVE CONTROL. The assertion above is satisfied by a watcher that
        // never started, so on its own it cannot fail. This proves the watcher
        // is alive and delivering on this very root, which is what makes the
        // silence above mean something.
        std::fs::create_dir_all(root.join("agents/Silber/fabric")).unwrap();
        std::fs::write(root.join("agents/Silber/fabric/agent.kdl"), b"agent {}").unwrap();
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(5), rx.recv()).await,
                Ok(Some(_))
            ),
            "the watcher must be alive on this root, or the silence above proves nothing"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_file_reads_do_not_wake_watcher_but_writes_do() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watched.txt");
        std::fs::write(&path, b"seed").unwrap();
        let (tx, mut rx) = mpsc::channel::<WatchEvent>(1);
        let work = EntryWork::new();
        let _watcher = spawn_watcher(
            dir.path(),
            tx,
            work.clone(),
            entry_with_policy("watch", dir.path(), SyncPolicy::Bus),
        )
        .unwrap();
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
            &HashMap::new(),
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

    /// A WATCHED FOLDER THAT IS NOT THERE IS NOT A FOLDER SOMEBODY EMPTIED.
    ///
    /// `scan_folder` returns an EMPTY result when the root does not exist. Every
    /// tracked path is then in the observed set and absent from the scan, which
    /// is the same shape as a local delete, so a delete-propagating policy
    /// tombstones the lot and sends that to every peer.
    ///
    /// The root can vanish for reasons that are not a deletion: an unmounted
    /// volume, a directory renamed while a pass runs, a mount that is not ready
    /// yet at boot. None of those mean the user deleted their files, and the
    /// blast radius is the whole entry rather than one path.
    ///
    /// Prefer the annoying failure. A folder we cannot see is a folder we cannot
    /// see, and the honest answer is to do nothing until we can.
    #[test]
    fn a_vanished_root_is_not_a_mass_delete() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("watched");
        std::fs::create_dir_all(&root).unwrap();
        for name in ["a.md", "b.md", "c.md"] {
            std::fs::write(root.join(name), b"payload").unwrap();
        }
        let entry = entry_with_policy("sync", &root, SyncPolicy::Bus);
        let rules = entry.policy.rules();
        assert!(
            rules.propagate_deletes,
            "this test is about a policy that propagates deletes"
        );

        let mut node = SyncNode::new(Author([1; 32]));
        let mut observed = HashMap::new();
        let mut cache = HashMap::new();
        scan_into_node_observed(&mut node, &root, &entry, rules, &mut observed, &mut cache)
            .unwrap();
        assert_eq!(
            node.manifest().present_paths().count(),
            3,
            "the fixture never recorded the files, so losing them below proves nothing"
        );

        // The volume goes away. Nobody deleted anything.
        std::fs::remove_dir_all(&root).unwrap();
        scan_into_node_observed(&mut node, &root, &entry, rules, &mut observed, &mut cache)
            .unwrap();

        assert_eq!(
            node.manifest().present_paths().count(),
            3,
            "a root that vanished was read as a delete of every file in the entry, \
             and that tombstone set goes to every peer"
        );
    }

    /// Turning delete propagation ON must not delete a file that is simply
    /// there. A delete pending for an unknown length of time is not a delete
    /// anybody asked for today.
    ///
    /// HONESTY ABOUT THIS TEST: it passes before the fix as well as after, and I
    /// am keeping it anyway. It was written to reproduce a proposed cause of the
    /// 2026-08-25 file loss, that enabling propagation replayed a backlog of old
    /// deletes. IT DOES NOT REPRODUCE, because that was not the cause. The cause
    /// was a path removed from an entry's include, which left it recorded but
    /// unscannable, and `a_path_dropped_from_include_is_forgotten_not_deleted`
    /// in tests/folder_sync.rs is the test that DOES reproduce it.
    ///
    /// A test that passes before and after is still worth having: it proves the
    /// switch itself is not the dangerous part, so nobody has to wonder again.
    #[test]
    fn enabling_delete_propagation_does_not_delete_what_is_still_there() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("a.md"), b"alpha").unwrap();
        std::fs::write(root.join("b.md"), b"beta").unwrap();
        let entry = entry_with_policy("sync", root, SyncPolicy::Catalog);
        let quiet = PolicyRules {
            propagate_deletes: false,
            sweep_tombstones: false,
        };
        let loud = PolicyRules {
            propagate_deletes: true,
            sweep_tombstones: false,
        };

        let mut node = SyncNode::new(Author([1; 32]));
        let mut observed = HashMap::new();
        let mut cache = HashMap::new();
        scan_into_node_observed(&mut node, root, &entry, quiet, &mut observed, &mut cache)
            .unwrap();
        assert_eq!(
            node.manifest().present_paths().count(),
            2,
            "the fixture never recorded both files, so the switch below proves nothing"
        );

        // The switch. Same disk, same records, nobody deleted anything.
        scan_into_node_observed(&mut node, root, &entry, loud, &mut observed, &mut cache)
            .unwrap();
        let protected = observed.clone();
        materialize_tracked(
            &mut node,
            root,
            loud,
            &protected,
            &mut observed,
            &cache,
            None,
        )
        .unwrap();

        assert!(root.join("a.md").exists(), "enabling propagation deleted a.md");
        assert!(root.join("b.md").exists(), "enabling propagation deleted b.md");
        assert_eq!(
            node.manifest().present_paths().count(),
            2,
            "enabling propagation tombstoned a file nobody deleted"
        );
    }

    /// The bookkeeping files are rewritten WHOLE every time they are written,
    /// and they are fabric's own index rather than anyone's data. Indenting
    /// them for a reader who does not exist cost 62% of every byte.
    ///
    /// Newlines are the property to pin, because `serde_json`'s compact form
    /// emits none and its pretty form emits one per field. This fails the moment
    /// somebody restores the indentation for readability.
    #[tokio::test]
    async fn persisted_bookkeeping_is_compact_json() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        // Enough records that pretty-printing is unmistakable rather than noise.
        for i in 0..25 {
            std::fs::write(root.join(format!("f{i}.md")), format!("body {i}")).unwrap();
        }
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

        for leaf in ["state.json", "manifest.json"] {
            let path = dir.path().join("sync").join("bus").join(leaf);
            let raw = std::fs::read(&path).unwrap_or_else(|e| panic!("{leaf}: {e}"));
            assert!(!raw.is_empty(), "{leaf} was not written");
            assert!(
                !raw.contains(&b'\n'),
                "{leaf} is pretty-printed ({} bytes); the compact form has no newlines",
                raw.len()
            );
        }
    }

    /// Catalog USED TO ignore a local delete here and materialize restored the
    /// file from retained content. That is the behaviour this test was written
    /// to protect and it is deliberately gone: a delete now changes the
    /// manifest and materialize leaves the path deleted.
    #[test]
    fn catalog_scan_records_local_delete_and_materialize_leaves_it_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("keep.toml"), b"payload").unwrap();
        let entry = catalog_entry("cat", root);
        let policy = entry.policy.rules();

        let mut node = SyncNode::new(Author([1; 32]));
        scan_into_node(&mut node, root, &entry, policy).unwrap();

        std::fs::remove_file(root.join("keep.toml")).unwrap();
        let changed = scan_into_node(&mut node, root, &entry, policy).unwrap();
        assert!(changed, "a catalog delete is a change to the manifest");
        materialize(&node, root, policy).unwrap();
        assert!(
            !root.join("keep.toml").exists(),
            "materialize brought a deleted catalog file back"
        );
    }

    #[test]
    fn authoritative_tombstone_is_never_overturned_by_a_surviving_stale_file() {
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

            let changed = scan_into_node_observed(
                &mut node,
                root,
                &entry,
                rules,
                &mut observed,
                &mut HashMap::new(),
            )
            .unwrap();
            // NO POLICY MAY RESURRECT. Catalog used to advance the surviving
            // bytes to Present/v3 here, which is exactly how one peer returning
            // from a long absence undid a delete for the whole fleet.
            assert!(!changed, "a surviving stale file must not revive a tombstone");
            assert!(
                matches!(
                    node.manifest().get("retired.toml"),
                    Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
                ),
                "{policy:?} let stale bytes on disk overturn an authoritative tombstone"
            );
            assert_eq!(observed.get("retired.toml"), Some(&stale_hash));

            materialize_tracked(
                &mut node,
                root,
                rules,
                &protected,
                &mut observed,
                &HashMap::new(),
                None,
            )
            .unwrap();
            // The tombstone is authoritative under EVERY policy now, so the
            // stale file is removed and its observed receipt goes with it.
            assert!(
                !path.exists(),
                "{policy:?} kept a file an authoritative tombstone had deleted"
            );
            assert!(
                !observed.contains_key("retired.toml"),
                "{policy:?} kept an observed receipt for a deleted path"
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
                scan_into_node_observed(
                    &mut node,
                    root,
                    &entry,
                    policy.rules(),
                    &mut observed,
                    &mut HashMap::new()
                )
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

    /// A pass that changes nothing must not rewrite state.
    ///
    /// Measured on the live fleet on 2026-08-25: `sync_once` rewrote a 45 MB
    /// state.json and a 25 MB manifest.json roughly every twenty seconds while
    /// `full_scans` rose and `present`, `tombstones` and
    /// `inbound_guarded_transactions` did not move at all. About 2 TB a day
    /// across four machines, all of it re-writing what was already there.
    ///
    /// The rule already existed on the inbound path, in
    /// `prepare_inbound_entry`, whose own comment says "an already durable
    /// no-op scan needs no rewrite". It was simply never applied here.
    #[tokio::test]
    async fn a_no_op_sync_pass_does_not_rewrite_state() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.md"), b"seed").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        // The first pass discovers the file. It MUST persist.
        engine.sync_once("bus").await.unwrap();
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        assert!(
            entry.work.persist_calls.load(Ordering::Relaxed) > 0,
            "the first pass discovers a file and must write it"
        );
        entry.work.persist_calls.store(0, Ordering::Relaxed);

        // Nothing changes on disk. These passes have nothing to record.
        engine.sync_once("bus").await.unwrap();
        engine.sync_once("bus").await.unwrap();
        engine.sync_once("bus").await.unwrap();

        assert_eq!(
            entry.work.persist_calls.load(Ordering::Relaxed),
            0,
            "a pass that changed nothing must not rewrite state"
        );
    }

    /// `full_scans` is TWO per `sync_once`, and `sync_passes` is one.
    ///
    /// This is not a decorative counter. Reading `full_scans` as a pass count
    /// overstates the rate by 2x, and dividing a per-pass cost by it halves the
    /// answer. That mistake was made against the live fleet and a measurement
    /// was reported and then withdrawn because of it.
    ///
    /// `sync_once` scans once before the peer step and again after, so the 2:1
    /// ratio is a property of the function rather than an accident of counting.
    /// If someone later removes the second scan, this test fails and tells them
    /// the counters now mean something different, which is the point.
    ///
    /// With NO inbound traffic, and only then, `sync_once` is the sole caller
    /// of `scan_entry` and the ratio is exactly two. The test below pins what
    /// production actually sees.
    #[tokio::test]
    async fn full_scans_counts_two_per_pass_and_sync_passes_counts_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.md"), b"seed").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let entry = {
            engine.sync_once("bus").await.unwrap();
            engine.entries.read().await.get("bus").cloned().unwrap()
        };
        entry.work.full_scans.store(0, Ordering::Relaxed);
        entry.work.sync_passes.store(0, Ordering::Relaxed);

        for _ in 0..5 {
            engine.sync_once("bus").await.unwrap();
        }

        assert_eq!(
            entry.work.sync_passes.load(Ordering::Relaxed),
            5,
            "sync_passes must count one per sync_once call"
        );
        assert_eq!(
            entry.work.full_scans.load(Ordering::Relaxed),
            10,
            "full_scans must count two per sync_once, once each side of the peer step"
        );
    }

    /// The 2:1 ratio above holds ONLY when no peer is talking to us, and
    /// production always has peers talking to us.
    ///
    /// `scan_entry` has three callers. `complete_inbound` runs it once per
    /// guarded inbound transaction and `prepare_inbound_entry` runs it at most
    /// once more, so inbound traffic lifts `full_scans` while leaving
    /// `sync_passes` alone. A reader who converts `full_scans` into a pass
    /// count with any constant is wrong by however much the peers happened to
    /// be doing, and that error is invisible from this side.
    ///
    /// This pins the bound instead, so the mistake is caught by a test rather
    /// than by a withdrawn number.
    #[tokio::test]
    async fn full_scans_counts_inbound_transactions_as_well_as_passes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.md"), b"seed").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        let entry = {
            engine.sync_once("bus").await.unwrap();
            engine.entries.read().await.get("bus").cloned().unwrap()
        };
        entry.work.full_scans.store(0, Ordering::Relaxed);
        entry.work.sync_passes.store(0, Ordering::Relaxed);
        entry
            .work
            .inbound_guarded_transactions
            .store(0, Ordering::Relaxed);

        for _ in 0..3 {
            engine.sync_once("bus").await.unwrap();
        }
        for _ in 0..2 {
            let prepared = engine.prepare_inbound("bus").await.unwrap().unwrap();
            engine.complete_inbound(prepared).await.unwrap();
        }

        let passes = entry.work.sync_passes.load(Ordering::Relaxed);
        let scans = entry.work.full_scans.load(Ordering::Relaxed);
        let guarded = entry
            .work
            .inbound_guarded_transactions
            .load(Ordering::Relaxed);

        assert_eq!(passes, 3, "only sync_once is a pass");
        assert_eq!(
            guarded, 2,
            "both inbound transactions took the guarded path"
        );
        assert!(
            scans > 2 * passes,
            "inbound transactions scan too, so full_scans must exceed two per \
             pass: scans={scans}, passes={passes}"
        );
        assert!(
            scans >= 2 * passes + guarded && scans <= 2 * passes + 2 * guarded,
            "full_scans must stay inside the bound the callers imply: \
             {} <= {scans} <= {}",
            2 * passes + guarded,
            2 * passes + 2 * guarded
        );
    }

    /// A pass must attribute its time to the phase that spent it.
    ///
    /// Not a counter mirror: it pins that the phases are wired to the work they
    /// name. `persist_micros` must stay at zero across passes that write
    /// nothing, while `scan_micros` rises, because every pass scans and only a
    /// changed pass writes. A phase timer attached to the wrong call site, or
    /// left outside the `if` that guards the write, fails here.
    #[tokio::test]
    async fn phase_timers_follow_the_work_and_not_the_pass() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.md"), b"seed").unwrap();
        write_bus_sync(dir.path(), &root);

        let engine = SyncEngine::new(
            FabricHome::new(dir.path()),
            Author([1; 32]),
            Arc::new(LoopbackTransport::default()),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        // The discovering pass writes, so persist time must be recorded.
        engine.sync_once("bus").await.unwrap();
        let entry = engine.entries.read().await.get("bus").cloned().unwrap();
        assert!(
            entry.work.persist_micros.load(Ordering::Relaxed) > 0,
            "a pass that wrote must record persist time"
        );

        let persist_after_write = entry.work.persist_micros.load(Ordering::Relaxed);
        let scan_after_write = entry.work.scan_micros.load(Ordering::Relaxed);

        // Nothing changes. These passes still scan, and must not write.
        for _ in 0..3 {
            engine.sync_once("bus").await.unwrap();
        }

        assert_eq!(
            entry.work.persist_micros.load(Ordering::Relaxed),
            persist_after_write,
            "a pass that wrote nothing must add no persist time"
        );
        assert!(
            entry.work.scan_micros.load(Ordering::Relaxed) > scan_after_write,
            "every pass scans, so scan time must keep rising"
        );
    }

    /// Skipping the write must not claim a durability we do not have.
    ///
    /// The dangerous version of this fix is one that stops writing AND still
    /// marks the generation durable, so a crash loses whatever the skipped
    /// write would have carried. This asserts the property that matters rather
    /// than the counter: after a no-op pass, what is ON DISK still equals what
    /// is in memory.
    #[tokio::test]
    async fn a_skipped_write_leaves_the_persisted_state_still_correct() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.md"), b"seed").unwrap();
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

        // A real local change, then several no-op passes on top of it.
        std::fs::write(root.join("b.md"), b"second").unwrap();
        engine.sync_once("bus").await.unwrap();
        engine.sync_once("bus").await.unwrap();
        engine.sync_once("bus").await.unwrap();

        let in_memory = engine
            .node_for("bus")
            .await
            .unwrap()
            .lock()
            .await
            .manifest()
            .clone();
        let raw = std::fs::read(engine.state_path("bus")).unwrap();
        let on_disk: PersistedEntryState = serde_json::from_slice(&raw).unwrap();

        assert_eq!(
            on_disk.manifest, in_memory,
            "after skipping writes, the persisted manifest must still equal the \
             live one, or a crash loses the change the skip decided not to record"
        );
        assert!(
            on_disk.manifest.get("b.md").is_some(),
            "the real change must be on disk, not merely in memory"
        );
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

        // Converge fully, then a delete on B must stick on B and reach A. This
        // used to assert the opposite, that the file was restored on B and kept
        // on A.
        a.sync_once("catalog").await.unwrap();
        std::fs::remove_file(dir_b.path().join("catalog/job.toml")).unwrap();
        b.sync_once("catalog").await.unwrap();
        a.sync_once("catalog").await.unwrap();
        b.sync_once("catalog").await.unwrap();
        assert!(
            !dir_b.path().join("catalog/job.toml").exists(),
            "the delete was undone on the machine that made it"
        );
        assert!(
            !dir_a.path().join("catalog/job.toml").exists(),
            "the delete never reached the peer"
        );
    }

    #[tokio::test]
    async fn a_shared_tombstone_is_not_undone_by_the_one_node_that_kept_the_bytes() {
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
                    scan_cache: HashMap::new(),
                    peer_acks: HashMap::new(),
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

        // A USED TO advance the surviving bytes to Present/v3 here, the wire
        // used to carry that winner to B, and B used to materialize it. That is
        // how one node holding an old copy undid a delete for everybody, and it
        // is the exact shape of a peer coming home after a fortnight away.
        engine_a.sync_once("catalog").await.unwrap();
        engine_b.sync_once("catalog").await.unwrap();
        engine_a.sync_once("catalog").await.unwrap();

        assert!(
            !root_a.join("agent.kdl").exists(),
            "the node that kept the bytes resurrected a deleted file"
        );
        assert!(
            !root_b.join("agent.kdl").exists(),
            "a resurrected file crossed the wire to a node that had let it go"
        );
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
                Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
            ));
        }

        // The tombstone is still authoritative after a restart, and replaying
        // the scan does not revive the path from the bytes A once held.
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
                Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
            ));
            assert_eq!(entry.observed.lock().unwrap().get("agent.kdl"), None);
            assert!(!root.join("agent.kdl").exists());
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

        // THE POINT OF THE FIX. The MANIFEST holds the colliding size and mtime,
        // and the manifest is replicated, so a peer chose those numbers. The scan
        // no longer consults it, so the collision cannot reach the cache and the
        // scan reports the REAL bytes on disk.
        //
        // This is what a peer could previously manufacture: stamping made the
        // origin's mtime a local fact, so two nodes contending on one key could
        // produce identical size and mtime with different content.
        let scanned = scan_folder(&root, &entry, &HashMap::new()).unwrap();
        let file = scanned.iter().find(|f| f.rel == "hot.txt").unwrap();
        assert_eq!(
            file.hash,
            content_hash(second),
            "the scan must report the bytes actually on disk; a replicated \
             manifest must never decide a local cache hit"
        );

        // The residual risk did not vanish, it moved out of reach of a peer. A
        // LOCAL cache entry claiming this size and mtime is still trusted, which
        // is what makes the cache a cache. The difference that matters is that
        // only this machine can create such an entry, by observing its own disk,
        // so no peer can manufacture one.
        let mut local_cache = HashMap::new();
        local_cache.insert(
            "hot.txt".to_string(),
            ScanCacheEntry {
                size: recorded.size,
                mtime_secs: shared_secs,
                mtime_nanos: shared_nanos,
                hash: content_hash(first),
            },
        );
        let scanned = scan_folder(&root, &entry, &local_cache).unwrap();
        let file = scanned.iter().find(|f| f.rel == "hot.txt").unwrap();
        assert_eq!(
            file.hash,
            content_hash(first),
            "a local cache entry is trusted by design; only its ORIGIN changed"
        );

        // And materialization still does NOT apply the origin mtime, because git
        // does not track one. What changed is only where the cache gets its key.
        let mut peer = SyncNode::new(Author([3u8; 32]));
        peer.local_write("materialized.txt", second, shared_secs, shared_nanos);
        let mut observed = HashMap::new();
        materialize_tracked(
            &mut peer,
            &root,
            entry.policy.rules(),
            &HashMap::new(),
            &mut observed,
            &HashMap::new(),
            None,
        )
        .unwrap();
        let (written_secs, written_nanos) = mtime_of_path(&root.join("materialized.txt"));
        assert_ne!(
            (written_secs, written_nanos),
            (shared_secs, shared_nanos),
            "materialization must not apply the origin mtime"
        );
    }

    /// Issue #56: materialization must not re-read a file the cache already
    /// settled.
    ///
    /// Materialization used to read and hash EVERY present file on EVERY pass.
    /// On the live daemon that was 70,157,702 bytes every 0.51 s, and
    /// `blake3_hash_many_neon` was 14.22% of one core.
    ///
    /// The read cannot be observed directly, so this test makes the read CHANGE
    /// THE ANSWER. The disk carries bytes that differ from the recorded hash
    /// while wearing the same size and mtime. A materialization that reads sees
    /// drift and republishes it. A materialization that trusts the cache leaves
    /// it alone.
    ///
    /// Trusting it is the existing contract, not a new one.
    /// `materialization_does_not_manufacture_an_mtime_collision` pins that a
    /// LOCAL cache entry is trusted by design, and `scan_folder` already runs
    /// first in every pass and already trusts it. Re-deriving the opposite
    /// answer here, from the same disk, was incoherent rather than safe.
    #[test]
    fn a_converged_file_is_not_re_read_on_materialization() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();

        let recorded = b"the bytes the manifest records";
        let mut node = SyncNode::new(Author([7u8; 32]));
        node.local_write("hot.txt", recorded, 1_700_000_000, 123_456_789);
        let meta = node
            .manifest()
            .get("hot.txt")
            .and_then(|e| e.meta())
            .copied()
            .unwrap();

        // Materialize once, with no cache, so the file lands on disk.
        let mut observed = HashMap::new();
        materialize_tracked(
            &mut node,
            &root,
            SyncPolicy::Bus.rules(),
            &HashMap::new(),
            &mut observed,
            &HashMap::new(),
            None,
        )
        .unwrap();
        let path = root.join("hot.txt");
        assert_eq!(std::fs::read(&path).unwrap(), recorded);

        // Record what THIS MACHINE observed, which is what a scan would record.
        let size = std::fs::metadata(&path).unwrap().len();
        let (secs, nanos) = mtime_of_path(&path);
        let mut cache = HashMap::new();
        cache.insert(
            "hot.txt".to_string(),
            ScanCacheEntry {
                size,
                mtime_secs: secs,
                mtime_nanos: nanos,
                hash: meta.hash,
            },
        );

        // Put DIFFERENT bytes of the SAME length on disk, wearing that mtime.
        let drifted = b"the bytes the manifest recordZ";
        assert_eq!(
            drifted.len(),
            recorded.len(),
            "the collision needs equal size"
        );
        std::fs::write(&path, drifted).unwrap();
        set_file_mtime(&path, secs, nanos).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            size,
            "the test must actually reproduce the size collision"
        );
        assert_eq!(
            mtime_of_path(&path),
            (secs, nanos),
            "the test must actually reproduce the mtime collision"
        );

        let before = node.manifest().clone();
        let mut observed_after = HashMap::new();
        materialize_tracked(
            &mut node,
            &root,
            SyncPolicy::Bus.rules(),
            &HashMap::new(),
            &mut observed_after,
            &cache,
            None,
        )
        .unwrap();

        assert_eq!(
            node.manifest(),
            &before,
            "a cache hit must not be re-read, so the drift must not republish"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            drifted,
            "and a cache hit must not rewrite the file either"
        );
        assert_eq!(
            observed_after.get("hot.txt"),
            Some(&meta.hash),
            "the receipt must still be recorded on the skip path"
        );
    }

    /// The control. A file the cache does NOT cover is still read and hashed,
    /// so the fast path cannot quietly become "never check anything".
    #[test]
    fn a_file_the_cache_does_not_cover_is_still_read() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();

        let recorded = b"the bytes the manifest records";
        let mut node = SyncNode::new(Author([7u8; 32]));
        node.local_write("hot.txt", recorded, 1_700_000_000, 123_456_789);

        let mut observed = HashMap::new();
        materialize_tracked(
            &mut node,
            &root,
            SyncPolicy::Bus.rules(),
            &HashMap::new(),
            &mut observed,
            &HashMap::new(),
            None,
        )
        .unwrap();
        let path = root.join("hot.txt");

        // Drift the file, and offer a cache that does not mention it.
        let drifted = b"the bytes the manifest recordZ";
        std::fs::write(&path, drifted).unwrap();

        let before = node.manifest().clone();
        let mut observed_after = HashMap::new();
        materialize_tracked(
            &mut node,
            &root,
            SyncPolicy::Bus.rules(),
            &HashMap::new(),
            &mut observed_after,
            &HashMap::new(),
            None,
        )
        .unwrap();
        assert_ne!(
            node.manifest(),
            &before,
            "an uncached file must still be read, so this drift must republish"
        );
    }

    /// The executable bit is replicated, because git tracks it.
    ///
    /// The live case that motivated this: the synced catalog holds fabric
    /// binaries, and before this they arrived without the bit and could not be
    /// run.
    #[test]
    fn a_materialized_file_carries_the_executable_bit() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        let entry = entry_with_policy("bus", &root, SyncPolicy::Bus);

        let mut peer = SyncNode::new(Author([2u8; 32]));
        peer.local_write_with_mode("tool", b"#!/bin/sh\necho hi\n", 0, 0, true);
        peer.local_write_with_mode("notes.md", b"just text\n", 0, 0, false);
        let mut observed = HashMap::new();
        materialize_tracked(
            &mut peer,
            &root,
            entry.policy.rules(),
            &HashMap::new(),
            &mut observed,
            &HashMap::new(),
            None,
        )
        .unwrap();

        let tool = std::fs::metadata(root.join("tool")).unwrap();
        assert!(
            tool.permissions().mode() & 0o111 != 0,
            "an executable file must arrive executable, or a synced binary \
             cannot be run without a manual chmod"
        );
        let notes = std::fs::metadata(root.join("notes.md")).unwrap();
        assert!(
            notes.permissions().mode() & 0o111 == 0,
            "a plain file must not be made executable"
        );
    }

    /// A scan records the executable bit from local disk.
    #[test]
    fn a_scan_records_the_executable_bit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        let entry = entry_with_policy("bus", &root, SyncPolicy::Bus);

        std::fs::write(root.join("plain.txt"), b"text").unwrap();
        std::fs::write(root.join("run.sh"), b"#!/bin/sh\n").unwrap();
        set_executable(&root.join("run.sh")).unwrap();

        let scanned = scan_folder(&root, &entry, &HashMap::new()).unwrap();
        let run = scanned.iter().find(|f| f.rel == "run.sh").unwrap();
        let plain = scanned.iter().find(|f| f.rel == "plain.txt").unwrap();
        assert!(run.executable, "an executable file must be recorded as one");
        assert!(!plain.executable, "a plain file must not be");
    }

    /// A chmod on an ALREADY SYNCED file does not propagate. DIVERGENCE FROM GIT.
    ///
    /// Git propagates a chmod: a mode change rewrites the tree object, so it is
    /// a real commit. Fabric does not, because a chmod alters no bytes and
    /// `local_write` returns early on unchanged content. That early return is
    /// what makes applying a peer's content echo-free.
    ///
    /// Pinned so nobody reads the executable-bit support above as complete. It
    /// is correct for a NEW file, which is the case that bites, and not for a
    /// mode change on an existing one. Same cause as the invisible heartbeat:
    /// see `METADATA_ONLY_CHANGES_DO_NOT_PROPAGATE`.
    #[test]
    fn a_chmod_on_an_already_synced_file_does_not_propagate() {
        let mut node = SyncNode::new(Author([6u8; 32]));
        assert!(node.local_write_with_mode("tool", b"#!/bin/sh\n", 0, 0, false));
        assert_eq!(node.manifest().get("tool").unwrap().version(), 1);

        // chmod +x: same bytes, new mode.
        assert!(
            !node.local_write_with_mode("tool", b"#!/bin/sh\n", 0, 0, true),
            "a chmod alters no bytes, so it is a no-op; that no-op is echo-freedom"
        );
        let meta = node.manifest().get("tool").and_then(|e| e.meta()).unwrap();
        assert_eq!(meta.version, 1, "no version advance means nothing to send");
        assert!(
            !meta.executable,
            "the manifest keeps the OLD mode, so a peer never learns of the chmod"
        );
    }

    /// Every component of the key must be compared, including nanoseconds.
    ///
    /// Two writes inside the same second are ordinary, so a key that ignored
    /// nanoseconds would report the old hash for genuinely new bytes. That is
    /// the corruption class this whole change exists to prevent, so weakening
    /// the key must fail loudly rather than silently.
    #[test]
    fn a_changed_file_sharing_size_and_whole_seconds_is_still_re_read() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        let entry = entry_with_policy("bus", &root, SyncPolicy::Bus);

        let before = b"aaaaa";
        let after = b"bbbbb";
        assert_eq!(before.len(), after.len(), "size must be identical");
        let path = root.join("same-second.txt");
        std::fs::write(&path, before).unwrap();
        let secs = 1_690_000_000i64;
        set_file_mtime(&path, secs, 111_000_000).unwrap();
        let cached = scan_folder(&root, &entry, &HashMap::new()).unwrap();
        let cache: HashMap<String, ScanCacheEntry> = cached
            .iter()
            .map(|f| (f.rel.clone(), f.cache_entry()))
            .collect();

        // Same size, same whole second, different nanoseconds, different bytes.
        std::fs::write(&path, after).unwrap();
        set_file_mtime(&path, secs, 222_000_000).unwrap();
        let (disk_secs, _) = mtime_of_path(&path);
        assert_eq!(disk_secs, secs, "the test needs the seconds to match");

        let scanned = scan_folder(&root, &entry, &cache).unwrap();
        let file = scanned.iter().find(|f| f.rel == "same-second.txt").unwrap();
        assert_eq!(
            file.hash,
            content_hash(after),
            "a sub-second change must not be masked by a coarse cache key"
        );
        assert!(file.bytes.is_some(), "changed bytes must be re-read");
    }

    /// A truncating filesystem must not make the cache miss forever.
    ///
    /// The cache is only ever filled from what was OBSERVED on disk, never from
    /// the value that was requested. So if a filesystem rounds a stamp away, the
    /// recorded key is the rounded value the next scan will also read, and the
    /// cache still hits. Storing the requested value instead would miss every
    /// time, on every file, permanently.
    #[test]
    fn the_cache_records_the_observed_mtime_not_the_requested_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("resources");
        std::fs::create_dir_all(&root).unwrap();
        let entry = entry_with_policy("bus", &root, SyncPolicy::Bus);

        // Nanoseconds a coarse filesystem would round. Whatever it does, the
        // scan reads back the truth and caches that.
        let requested_secs = 1_650_000_000i64;
        let requested_nanos = 999_999_999u32;
        let mut peer = SyncNode::new(Author([5u8; 32]));
        peer.local_write("stamped.txt", b"bytes", requested_secs, requested_nanos);
        let mut observed = HashMap::new();
        materialize_tracked(
            &mut peer,
            &root,
            entry.policy.rules(),
            &HashMap::new(),
            &mut observed,
            &HashMap::new(),
            None,
        )
        .unwrap();

        let first = scan_folder(&root, &entry, &HashMap::new()).unwrap();
        let cache: HashMap<String, ScanCacheEntry> = first
            .iter()
            .map(|f| (f.rel.clone(), f.cache_entry()))
            .collect();
        let cached = cache.get("stamped.txt").expect("cached");
        let (disk_secs, disk_nanos) = mtime_of_path(&root.join("stamped.txt"));
        assert_eq!(
            (cached.mtime_secs, cached.mtime_nanos),
            (disk_secs, disk_nanos),
            "the cache must hold what the disk reports, whatever the stamp did"
        );

        let second = scan_folder(&root, &entry, &cache).unwrap();
        let file = second.iter().find(|f| f.rel == "stamped.txt").unwrap();
        assert!(
            file.bytes.is_none(),
            "the cache must hit even if the filesystem rounded the stamp away"
        );
    }

    /// A state file written before the cache existed must load and warm.
    #[test]
    fn a_state_file_without_a_scan_cache_loads_and_warms() {
        let legacy = serde_json::json!({
            "manifest": { "entries": {} },
            "observed": {}
        });
        let state: PersistedEntryState =
            serde_json::from_value(legacy).expect("a pre-cache state file must still load");
        assert!(
            state.scan_cache.is_empty(),
            "an absent cache is empty, not a parse failure"
        );
    }

    /// A same-content rewrite still does NOT propagate, and that is not an
    /// oversight.
    ///
    /// `local_write` returns false and discards the new mtime when the content
    /// hash is unchanged, which is what makes applying a peer's content
    /// echo-free. So a metadata-only change never advances a version and never
    /// crosses the wire.
    ///
    /// This is pinned so nobody reads the mtime stamping above as having fixed
    /// presence. It did not. Issue 27's heartbeat is a same-byte rewrite, and a
    /// replica still keeps the older timestamp.
    #[test]
    fn a_same_content_mtime_change_still_does_not_propagate() {
        let mut node = SyncNode::new(Author([7u8; 32]));
        assert!(node.local_write("status", b"available\n", 100, 0));
        assert_eq!(node.manifest().get("status").unwrap().version(), 1);

        // The heartbeat: identical bytes, later clock.
        assert!(
            !node.local_write("status", b"available\n", 900, 0),
            "a same-byte rewrite must remain a no-op; that no-op is echo-freedom"
        );
        let meta = node
            .manifest()
            .get("status")
            .and_then(|e| e.meta())
            .unwrap();
        assert_eq!(meta.version, 1, "no version advance means nothing to send");
        assert_eq!(
            meta.mtime_secs, 100,
            "the manifest keeps the OLD mtime, so a peer cannot learn the new one"
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

        // A LOCAL receipt of what this machine observed, which is the only thing
        // that may decide a cache hit.
        let mut cache = HashMap::new();
        cache.insert(
            "still.txt".to_string(),
            ScanCacheEntry {
                size: bytes.len() as u64,
                mtime_secs: secs,
                mtime_nanos: nanos,
                hash: content_hash(bytes),
            },
        );

        let scanned = scan_folder(&root, &entry, &cache).unwrap();
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
        // The ceiling was 4 per second, derived when a persist wrote 72 MB of
        // pretty-printed JSON. Compact bookkeeping made a pass cheaper, so more
        // legitimate passes fit the same fixed burst, and the old ceiling stopped
        // bounding amplification and started bounding speed.
        //
        // MEASURED ON ONE MACHINE, FIVE RUNS EACH, THE SAME BURST:
        //   pretty   30 to 34 reconciles, 2.86 to 3.15 per second
        //   compact  38 reconciles every run, 3.52 to 3.60 per second
        //
        // That rise is the intended effect and not amplification. The writers
        // still perform a fixed number of revisions, and the run still has to
        // converge below. The ceiling is re-derived to keep roughly the headroom
        // it used to have above the observed rate, so a genuine feedback loop
        // still trips it. It is fractionally more headroom than before, which is
        // deliberate: this test has a history of failing on a loaded machine at
        // rates that were legitimate.
        //
        // The scan and persist ceilings stay expressed against this same rate,
        // 112/36 and 76/36 of it, so the original relationships are unchanged.
        const MAX_RECONCILES_PER_SEC: u128 = 5;
        assert!(
            (reconciles as u128) * 1_000 <= elapsed_millis * MAX_RECONCILES_PER_SEC,
            "reconcile rate exceeded: {reconciles} reconciles, {scans} scans, and {persists} persists in {elapsed:?}"
        );
        assert!(
            (scans as u128) * 1_000 * 36 <= elapsed_millis * MAX_RECONCILES_PER_SEC * 112,
            "full-folder scan rate exceeded: {scans} scans against {reconciles} reconciles in {elapsed:?}"
        );
        assert!(
            (persists as u128) * 1_000 * 36 <= elapsed_millis * MAX_RECONCILES_PER_SEC * 76,
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
            scan_cache: HashMap::new(),
            peer_acks: HashMap::new(),
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
            expected_present: bool,
        }

        // BOTH POLICIES BEHAVE THE SAME NOW. Catalog used to recover physical
        // bytes into a higher Present, because union-of-presence could not
        // retain a Tombstone while any copy survived. That is gone: a delete
        // must stick in every synced folder.
        //
        // What remains is the receipt rule, and it is the same under either
        // policy. A matching unchanged receipt stays tombstoned, because those
        // bytes are the ones the delete was about. Absent, mismatched, or edited
        // bytes are NEW local intent and still advance to Present/v3, so an edit
        // made while a delete was in flight is never silently thrown away.
        let cases = [
            Case {
                label: "matching receipt and unchanged bytes",
                observed: Some(b"old"),
                disk: b"old",
                expected_present: false,
            },
            Case {
                label: "absent receipt and unchanged bytes",
                observed: None,
                disk: b"old",
                expected_present: true,
            },
            Case {
                label: "mismatched receipt and unchanged bytes",
                observed: Some(b"different"),
                disk: b"old",
                expected_present: true,
            },
            Case {
                label: "matching receipt and edited bytes",
                observed: Some(b"old"),
                disk: b"edited",
                expected_present: true,
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
                        scan_cache: HashMap::new(),
                        peer_acks: HashMap::new(),
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
                let expected_present = case.expected_present;
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
                    // Tombstoned under either policy means the file is gone
                    // from disk and its observed receipt with it.
                    assert!(!path.exists(), "{context}");
                    assert!(!observed.contains_key("retired.md"), "{context}");
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
