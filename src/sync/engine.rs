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
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use iroh::EndpointAddr;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_util::sync::CancellationToken;

use crate::config::FabricHome;

use super::config::{PolicyRules, SyncBook, SyncEntry, SyncPeers};
use super::manifest::{Author, Manifest};
use super::node::{Reconciled, SyncNode, content_hash};

/// How long to wait after a filesystem event settles before syncing, so a burst
/// of writes coalesces into one reconcile.
const WATCH_DEBOUNCE: Duration = Duration::from_millis(150);
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

/// One configured entry's live state.
struct EntryState {
    config: SyncEntry,
    policy: PolicyRules,
    node: Arc<Mutex<SyncNode>>,
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
            // Reuse an existing node for an unchanged entry so in-memory content
            // survives a reload; otherwise start one from the persisted manifest.
            let node = match entries.get(&cfg.name) {
                Some(existing) if existing.config == *cfg => existing.node.clone(),
                _ => Arc::new(Mutex::new(self.load_node(cfg).await?)),
            };
            next.insert(
                cfg.name.clone(),
                Arc::new(EntryState {
                    config: cfg.clone(),
                    policy,
                    node,
                }),
            );
        }
        *entries = next;
        Ok(())
    }

    async fn load_node(&self, cfg: &SyncEntry) -> Result<SyncNode> {
        let mut node = SyncNode::new(self.author);
        if let Some(manifest) = self.read_manifest(&cfg.name)? {
            node.adopt(&manifest);
        }
        Ok(node)
    }

    /// Resolve a sync name to its node (used by the daemon's inbound accept).
    pub async fn node_for(&self, name: &str) -> Option<Arc<Mutex<SyncNode>>> {
        self.entries
            .read()
            .await
            .get(name)
            .map(|entry| entry.node.clone())
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

        self.scan_entry(&entry).await?;
        self.materialize_entry_state(&entry).await?;

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

        self.materialize_entry_state(&entry).await?;
        self.persist_entry(&entry).await?;
        Ok(())
    }

    /// Materialize just this entry to disk (used by the daemon after an inbound
    /// reconcile writes into the node).
    pub async fn materialize_entry(&self, name: &str) -> Result<()> {
        let Some(entry) = self.entries.read().await.get(name).cloned() else {
            return Ok(());
        };
        self.materialize_entry_state(&entry).await?;
        self.persist_entry(&entry).await
    }

    async fn scan_entry(&self, entry: &EntryState) -> Result<bool> {
        let root = entry.config.folder.clone();
        let cfg = entry.config.clone();
        let policy = entry.policy;
        let mut node = entry.node.lock().await;
        scan_into_node(&mut node, &root, &cfg, policy)
    }

    async fn materialize_entry_state(&self, entry: &EntryState) -> Result<()> {
        let root = entry.config.folder.clone();
        let policy = entry.policy;
        let node = entry.node.lock().await;
        materialize(&node, &root, policy)
    }

    async fn persist_entry(&self, entry: &EntryState) -> Result<()> {
        let manifest = entry.node.lock().await.manifest().clone();
        self.write_manifest(&entry.config.name, &manifest)
    }

    fn manifest_path(&self, name: &str) -> PathBuf {
        self.home
            .root()
            .join("sync")
            .join(sanitize_name(name))
            .join("manifest.json")
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
        let root = match self.entries.read().await.get(&name) {
            Some(entry) => entry.config.folder.clone(),
            None => return,
        };

        // Best-effort initial sync.
        if let Err(error) = self.sync_once(&name).await {
            tracing::warn!(sync = %name, %error, "initial sync failed");
        }

        let (tx, mut rx) = mpsc::unbounded_channel();
        let _watcher = spawn_watcher(&root, tx);

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
                    // Debounce: drain the burst before syncing.
                    tokio::time::sleep(WATCH_DEBOUNCE).await;
                    while rx.try_recv().is_ok() {}
                    if let Err(error) = self.sync_once(&name).await {
                        tracing::debug!(sync = %name, %error, "watch sync failed");
                    }
                }
            }
        }
    }
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
fn scan_into_node(
    node: &mut SyncNode,
    root: &Path,
    entry: &SyncEntry,
    policy: PolicyRules,
) -> Result<bool> {
    let scanned = scan_folder(root, entry)?;
    let mut changed = false;
    let mut seen = HashSet::new();
    for file in &scanned {
        seen.insert(file.rel.clone());
        if node.local_write(&file.rel, &file.bytes, file.mtime_secs, file.mtime_nanos) {
            changed = true;
        }
    }
    let present: Vec<String> = node
        .manifest()
        .present_paths()
        .map(|(path, _)| path.clone())
        .collect();
    let now = now_secs();
    for path in present {
        if !seen.contains(&path) && node.local_remove(&path, policy, now) {
            changed = true;
        }
    }
    Ok(changed)
}

/// Write `node`'s present entries to disk (only where content differs) and, under
/// a delete-propagating policy, remove tombstoned files.
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
fn spawn_watcher(root: &Path, tx: mpsc::UnboundedSender<()>) -> Option<notify::RecommendedWatcher> {
    use notify::{RecursiveMode, Watcher};

    let mut watcher =
        match notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if res.is_ok() {
                let _ = tx.send(());
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
    use std::sync::Mutex as StdMutex;

    fn catalog_entry(name: &str, folder: &Path) -> SyncEntry {
        SyncEntry {
            name: name.to_string(),
            folder: folder.to_path_buf(),
            peers: SyncPeers::Wildcard("*".into()),
            policy: SyncPolicy::Catalog,
            include: None,
        }
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
                    if n == server_name { Some(target) } else { None }
                })
                .await
            });
            let stats = crate::sync::wire::run_client(client_end, node, &name).await?;
            let _ = server.await;
            Ok(stats)
        }
    }

    fn write_syncs(home: &Path, folder: &Path) {
        let toml = format!(
            "[[sync]]\nname = \"catalog\"\nfolder = {folder:?}\npeers = \"*\"\npolicy = \"catalog\"\n"
        );
        std::fs::write(home.join("syncs.toml"), toml).unwrap();
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
}
