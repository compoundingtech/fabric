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
    collections::{HashMap, HashSet},
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
    #[cfg(test)]
    scan_calls: AtomicUsize,
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
            #[cfg(test)]
            scan_calls: AtomicUsize::new(0),
            #[cfg(test)]
            persist_calls: AtomicUsize::new(0),
        })
    }

    fn record_mutation(&self) {
        self.mutation_generation.fetch_add(1, Ordering::AcqRel);
    }

    fn begin_inbound(self: &Arc<Self>) -> InboundWaiter {
        let queued = self.inbound_waiters.fetch_add(1, Ordering::AcqRel) > 0;
        InboundWaiter {
            work: self.clone(),
            queued,
        }
    }

    fn may_reuse_durable_scan(&self, queued: bool) -> bool {
        queued
            && self.mutation_generation.load(Ordering::Acquire)
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

/// State captured immediately before an inbound merge. The operation guard
/// keeps every engine-driven scan/materialize for this entry out of the middle
/// of the wire session; `baseline` is the pre-merge observed-disk receipt that
/// distinguishes local paths from genuinely new remote paths at completion.
pub(crate) struct PreparedInbound {
    entry: Arc<EntryState>,
    baseline: HashMap<String, ContentHash>,
    manifest: Manifest,
    _waiter: InboundWaiter,
    _operation: OwnedMutexGuard<()>,
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

    /// Scan and durably record local filesystem changes before exposing an
    /// entry's node to an inbound reconcile.
    ///
    /// This ordering is essential for delete-propagating policies: an atomic
    /// local rename/delete may already express user intent while its watcher
    /// event is still inside the debounce window. Letting a peer reconcile and
    /// materialize first could restore the stale Present entry and erase the
    /// only observable evidence of that local deletion. Scanning before merge
    /// also avoids treating paths that are genuinely new on the remote as local
    /// deletions, because they are not in the observed-disk receipt yet.
    pub(crate) async fn prepare_inbound(&self, name: &str) -> Result<Option<PreparedInbound>> {
        let Some(entry) = self.entries.read().await.get(name).cloned() else {
            return Ok(None);
        };
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
        let baseline = entry.observed.lock().unwrap().clone();
        let manifest = entry.node.lock().await.manifest().clone();
        Ok(Some(PreparedInbound {
            entry,
            baseline,
            manifest,
            _waiter: waiter,
            _operation: operation,
        }))
    }

    /// Complete an inbound transaction while its entry operation guard is still
    /// held. Disk changes that landed during the wire session are compared to
    /// the pre-merge baseline: a vanished baseline Present is a local delete,
    /// while a remote-only Present is materialized instead of tombstoned.
    pub(crate) async fn complete_inbound(&self, prepared: PreparedInbound) -> Result<()> {
        let PreparedInbound {
            entry,
            baseline,
            manifest,
            _waiter,
            _operation,
        } = prepared;
        let generation = entry.work.mutation_generation.load(Ordering::Acquire);
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

    /// A one-line status per entry: name, folder, peer count, file count.
    pub async fn status(&self) -> Vec<SyncStatus> {
        let entries = self.entries.read().await;
        let mut out = Vec::new();
        for (name, entry) in entries.iter() {
            let node = entry.node.lock().await;
            out.push(SyncStatus {
                name: name.clone(),
                folder: entry.config.folder.clone(),
                policy: entry.config.policy.as_str(),
                peers: entry.config.peers.clone(),
                files: node.manifest().present_paths().count(),
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
        #[cfg(test)]
        entry.work.scan_calls.fetch_add(1, Ordering::Relaxed);
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
        let mut node = entry.node.lock().await;
        let mut observed = entry.observed.lock().unwrap();
        materialize_tracked(&mut node, &root, policy, protected, &mut observed)
    }

    async fn persist_entry(&self, entry: &EntryState) -> Result<()> {
        #[cfg(test)]
        entry.work.persist_calls.fetch_add(1, Ordering::Relaxed);
        let manifest = entry.node.lock().await.manifest().clone();
        let observed = entry.observed.lock().unwrap().clone();
        self.write_state(
            &entry.config.name,
            &PersistedEntryState { manifest, observed },
        )
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
        let (tx, mut rx) = mpsc::channel(1);
        let _watcher = spawn_watcher(&root, tx, entry.work.clone());

        let mut ticker = tokio::time::interval(PERIODIC_RESYNC);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // consume the immediate first tick

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = ticker.tick() => {
                    if let Err(error) = self.sync_once(&name).await {
                        tracing::debug!(sync = %name, %error, "periodic sync failed");
                    }
                }
                event = rx.recv() => {
                    if event.is_none() { break; }
                    // Wait for a quiet edge, but cap the window so a
                    // continuously mutating tree still makes bounded progress.
                    if !coalesce_watch_events(
                        &mut rx,
                        WATCH_DEBOUNCE,
                        WATCH_MAX_COALESCE,
                    )
                    .await
                    {
                        break;
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
/// Returns false only when the watcher channel has closed.
async fn coalesce_watch_events(
    rx: &mut mpsc::Receiver<()>,
    debounce: Duration,
    max_coalesce: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + max_coalesce;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            next = tokio::time::timeout(debounce, rx.recv()) => {
                match next {
                    Ok(Some(())) => continue,
                    Ok(None) => return false,
                    Err(_) => break,
                }
            }
        }
    }
    while rx.try_recv().is_ok() {}
    true
}

/// A one-line status for `fabric sync ls`.
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub name: String,
    pub folder: PathBuf,
    pub policy: &'static str,
    pub peers: SyncPeers,
    pub files: usize,
}

// ---- filesystem scan / materialize (sync helpers, unit-testable) ----

struct ScannedFile {
    rel: String,
    bytes: Vec<u8>,
    mtime_secs: i64,
    mtime_nanos: u32,
}

/// Walk `root` recursively, returning in-scope regular files (symlinks skipped,
/// include globs applied, paths normalized).
fn scan_folder(root: &Path, entry: &SyncEntry) -> Result<Vec<ScannedFile>> {
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
            let bytes = std::fs::read(&path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let (mtime_secs, mtime_nanos) = mtime_of(&child);
            out.push(ScannedFile {
                rel: norm,
                bytes,
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
    for file in scan_folder(&entry.folder, entry)? {
        let hash = content_hash(&file.bytes);
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
    let scanned = scan_folder(root, entry)?;
    let previous = observed.clone();
    let mut current = HashMap::new();
    let mut changed = false;
    for file in &scanned {
        let hash = content_hash(&file.bytes);
        current.insert(file.rel.clone(), hash);
        if previous.get(&file.rel) == Some(&hash) {
            // Refill content after restart only when the manifest still names
            // these exact bytes. If a peer changed the entry while its old disk
            // bytes remained, those bytes are stale rather than a local edit.
            if node
                .manifest()
                .get(&file.rel)
                .and_then(|entry| entry.meta())
                .is_some_and(|meta| meta.hash == hash)
            {
                node.put_content(file.bytes.clone());
            }
        } else if node.local_write(&file.rel, &file.bytes, file.mtime_secs, file.mtime_nanos) {
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
            write_atomic(&path, bytes)?;
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
fn watcher_event_is_mutation(kind: notify::EventKind) -> bool {
    matches!(
        kind,
        notify::EventKind::Create(_) | notify::EventKind::Modify(_) | notify::EventKind::Remove(_)
    )
}

fn spawn_watcher(
    root: &Path,
    tx: mpsc::Sender<()>,
    work: Arc<EntryWork>,
) -> Option<notify::RecommendedWatcher> {
    use notify::{RecursiveMode, Watcher};

    let mut watcher =
        match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if res.is_ok_and(|event| watcher_event_is_mutation(event.kind)) {
                work.record_mutation();
                let _ = tx.try_send(());
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

        assert!(!watcher_event_is_mutation(notify::EventKind::Access(
            AccessKind::Open(AccessMode::Read)
        )));
        assert!(!watcher_event_is_mutation(notify::EventKind::Any));
        assert!(!watcher_event_is_mutation(notify::EventKind::Other));
        assert!(watcher_event_is_mutation(notify::EventKind::Create(
            CreateKind::Any
        )));
        assert!(watcher_event_is_mutation(notify::EventKind::Modify(
            ModifyKind::Any
        )));
        assert!(watcher_event_is_mutation(notify::EventKind::Remove(
            RemoveKind::Any
        )));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_file_reads_do_not_wake_watcher_but_writes_do() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watched.txt");
        std::fs::write(&path, b"seed").unwrap();
        let (tx, mut rx) = mpsc::channel(1);
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
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap(),
            Some(()),
            "a real write must wake the sync watcher"
        );
        assert!(
            work.mutation_generation.load(Ordering::Acquire) > generation,
            "a mutation must advance the generation"
        );
    }

    #[tokio::test]
    async fn continuous_events_are_bounded_by_max_coalesce_window() {
        let (tx, mut rx) = mpsc::channel(1);
        let sender = tokio::spawn(async move {
            for _ in 0..40 {
                let _ = tx.send(()).await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let started = tokio::time::Instant::now();
        assert!(
            coalesce_watch_events(
                &mut rx,
                Duration::from_millis(20),
                Duration::from_millis(100),
            )
            .await
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
        for (policy, file_survives) in [(SyncPolicy::Catalog, true), (SyncPolicy::Bus, false)] {
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

            assert!(
                !scan_into_node_observed(&mut node, root, &entry, rules, &mut observed).unwrap(),
                "unchanged observed bytes must not resurrect a Tombstone under {policy:?}"
            );
            assert!(matches!(
                node.manifest().get("retired.toml"),
                Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
            ));
            assert_eq!(observed.get("retired.toml"), Some(&stale_hash));

            materialize_tracked(&mut node, root, rules, &protected, &mut observed).unwrap();
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
                crate::sync::wire::run_server(server_end, move |n| async move {
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
                    crate::sync::wire::run_server(server_end, move |requested| {
                        let engine = resolver_target.clone();
                        async move {
                            let prepared = engine.prepare_inbound(&requested).await?;
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
        entry.work.scan_calls.store(0, Ordering::Relaxed);
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
            entry.work.scan_calls.load(Ordering::Relaxed),
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
            entry.work.scan_calls.store(0, Ordering::Relaxed);
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
                .map(|entry| entry.work.scan_calls.load(Ordering::Relaxed))
                .sum::<usize>(),
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
            .map(|entry| entry.work.scan_calls.load(Ordering::Relaxed))
            .sum::<usize>();
        let persists = entries
            .iter()
            .map(|entry| entry.work.persist_calls.load(Ordering::Relaxed))
            .sum::<usize>();
        eprintln!(
            "bounded 3-node/2,000-file stress: {reconciles} reconciles, {scans} scans, \
             {persists} persists in {elapsed:?}"
        );
        assert!(
            reconciles <= 36 && (reconciles as u128) * 1_000 <= (elapsed.as_millis().max(1)) * 4,
            "continuous mutations caused {reconciles} reconciles, {scans} scans, and {persists} persists in {elapsed:?}"
        );
        // At two peers per node, the reconcile cap also bounds the local
        // pre/post scans and the receiver-side prepare/complete scans.
        assert!(
            scans <= 112,
            "continuous mutations caused {scans} full-folder scans in {elapsed:?}"
        );
        assert!(
            persists <= 76,
            "continuous mutations caused {persists} state persists in {elapsed:?}"
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
            crate::sync::wire::run_server(server_end, move |name| {
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
            expected_present: bool,
        }

        // A matching receipt proves unchanged physical bytes are stale, so the
        // authoritative Tombstone stays authoritative. An absent or mismatched
        // receipt cannot distinguish those bytes from a new local write and
        // therefore characterizes the current contract as a v3 resurrection.
        // Actually edited bytes are likewise explicit new local intent.
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
                if case.expected_present {
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
            crate::sync::wire::run_server(server_end, move |name| async move {
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
        let PreparedInbound {
            entry,
            baseline,
            manifest: _,
            _waiter,
            _operation,
        } = prepared;
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

        // st2 archive is one atomic rename inside the bus root. The watcher has
        // not scanned it yet when an inbound reconcile begins.
        std::fs::create_dir_all(root.join("archive")).unwrap();
        std::fs::rename(
            root.join("inbox/archived.md"),
            root.join("archive/archived.md"),
        )
        .unwrap();

        let (client_end, server_end) = tokio::io::duplex(1 << 20);
        let resolver_engine = engine.clone();
        let server = tokio::spawn(async move {
            crate::sync::wire::run_server(server_end, move |name| {
                let engine = resolver_engine.clone();
                async move {
                    let prepared = engine.prepare_inbound(&name).await?;
                    Ok(prepared.map(|prepared| (prepared.node(), prepared)))
                }
            })
            .await
        });
        crate::sync::wire::run_client(client_end, remote.clone(), "bus")
            .await
            .unwrap();
        let (_, _, prepared) = server.await.unwrap().unwrap();
        engine.complete_inbound(prepared).await.unwrap();

        assert_archive_outcome(&engine, &root).await;
    }

    #[tokio::test]
    async fn archive_after_inbound_prepare_wins_over_stale_remote_present() {
        let (_dir, root, engine, remote) = archive_race_fixture().await;

        // Deterministically pause the inbound transaction after its first scan.
        // This is the post-scan/pre-merge window: the pre-merge baseline still
        // says inbox/archived.md is Present when the atomic archive lands.
        let prepared = engine.prepare_inbound("bus").await.unwrap().unwrap();
        std::fs::create_dir_all(root.join("archive")).unwrap();
        std::fs::rename(
            root.join("inbox/archived.md"),
            root.join("archive/archived.md"),
        )
        .unwrap();

        let (client_end, server_end) = tokio::io::duplex(1 << 20);
        let node = prepared.node();
        let server = tokio::spawn(async move {
            crate::sync::wire::run_server(server_end, move |name| async move {
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
