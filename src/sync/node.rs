//! The reconcilable node: fabric's in-memory sync state for one entry, plus the
//! pure pairwise reconcile that the fabric-native backend performs.
//!
//! A [`SyncNode`] holds a [`Manifest`] (logical per-file state) and a
//! content-addressed store (`hash → bytes`). Its methods are synchronous and
//! deterministic — no I/O, no clock — so the whole sync *semantics* can be
//! exhaustively property-tested here, independent of any transport. The async
//! engine wraps a `SyncNode`: it scans a real folder into `local_write` calls,
//! materializes the manifest back to disk, and ships manifests + content over a
//! swappable transport. [`SyncNode::reconcile`] is the loopback (in-process)
//! backend and the reference the on-wire backend must match.
//!
//! Key invariants that give fabric's promised behaviour:
//! - **Echo-safe versioning**: `local_write` bumps a path's version *only when
//!   the content hash changes*, so re-observing an engine-authored write is a
//!   no-op and a value synced A→B never ping-pongs back.
//! - **Catalog never deletes**: under a non-delete-propagating policy,
//!   `local_remove` records nothing; the manifest stays present and the
//!   materialized folder restores the file.
//! - **Convergence**: reconcile drives both manifests to `merge`, so any
//!   interleaving of edits and pairwise reconciles converges (see the property
//!   tests below).

use std::collections::{BTreeMap, HashMap};

use super::config::PolicyRules;
use super::manifest::{Author, ContentHash, Entry, FileMeta, Manifest, Tombstone};

/// BLAKE3 content hash of `bytes` — the transfer identity for a file's content.
pub fn content_hash(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

/// One node's sync state for a single entry: its manifest plus the content it
/// holds. The content store is keyed by hash so identical content is stored and
/// transferred once regardless of how many paths reference it.
#[derive(Debug, Clone)]
pub struct SyncNode {
    author: Author,
    manifest: Manifest,
    content: HashMap<ContentHash, Vec<u8>>,
}

/// What a single [`SyncNode::reconcile`] moved. All-zero means the two nodes were
/// already converged — the structural signal that there is no echo to chase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reconciled {
    /// Manifest entries this node adopted from the peer.
    pub pulled: usize,
    /// Manifest entries the peer adopted from this node.
    pub pushed: usize,
    /// Content bytes transferred in either direction.
    pub bytes: usize,
}

impl Reconciled {
    pub fn is_noop(&self) -> bool {
        self.pulled == 0 && self.pushed == 0 && self.bytes == 0
    }
}

impl SyncNode {
    pub fn new(author: Author) -> Self {
        Self {
            author,
            manifest: Manifest::new(),
            content: HashMap::new(),
        }
    }

    pub fn author(&self) -> Author {
        self.author
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn has_content(&self, hash: &ContentHash) -> bool {
        self.content.contains_key(hash)
    }

    pub fn get_content(&self, hash: &ContentHash) -> Option<&[u8]> {
        self.content.get(hash).map(Vec::as_slice)
    }

    /// Insert content bytes into the store (used by the async engine when a peer
    /// streams content for an adopted entry).
    pub fn put_content(&mut self, bytes: Vec<u8>) -> ContentHash {
        let hash = content_hash(&bytes);
        self.content.insert(hash, bytes);
        hash
    }

    /// Record a local file write at `path`.
    ///
    /// Returns whether the manifest changed. The version bumps only when the
    /// content hash differs from what the manifest already records for `path`,
    /// which makes re-observing an engine-authored (or unchanged) file a no-op —
    /// the core of echo/loop prevention.
    pub fn local_write(
        &mut self,
        path: &str,
        bytes: &[u8],
        mtime_secs: i64,
        mtime_nanos: u32,
    ) -> bool {
        let hash = content_hash(bytes);
        if let Some(Entry::Present(meta)) = self.manifest.get(path)
            && meta.hash == hash
        {
            // Same content already recorded — nothing changed. This is what
            // makes applying a peer's content (or a re-scan) echo-free.
            self.content.entry(hash).or_insert_with(|| bytes.to_vec());
            return false;
        }
        let next_version = self.manifest.get(path).map(Entry::version).unwrap_or(0) + 1;
        self.content.insert(hash, bytes.to_vec());
        self.manifest.insert(
            path.to_string(),
            Entry::Present(FileMeta {
                hash,
                size: bytes.len() as u64,
                mtime_secs,
                mtime_nanos,
                version: next_version,
                author: self.author,
            }),
        );
        true
    }

    /// Record that a present `path` disappeared from disk, under `policy`.
    ///
    /// Under catalog policy (`propagate_deletes == false`) this is a deliberate
    /// no-op: the manifest stays present so the file is restored, never removed
    /// on a peer. Under bus policy it records a tombstone that supersedes the
    /// present entry and propagates the deletion. Returns whether the manifest
    /// changed.
    pub fn local_remove(&mut self, path: &str, policy: PolicyRules, deleted_secs: i64) -> bool {
        if !policy.propagate_deletes {
            return false;
        }
        let Some(entry) = self.manifest.get(path) else {
            return false;
        };
        if !entry.is_present() {
            return false;
        }
        let next_version = entry.version() + 1;
        self.manifest.insert(
            path.to_string(),
            Entry::Tombstone(Tombstone {
                version: next_version,
                author: self.author,
                deleted_secs,
            }),
        );
        true
    }

    /// The materialized folder: every present manifest entry whose content this
    /// node holds, as `path → bytes`. Tombstoned paths are absent. This is what
    /// the async engine writes to disk, and what the convergence tests compare.
    pub fn folder_state(&self) -> BTreeMap<String, Vec<u8>> {
        let mut out = BTreeMap::new();
        for (path, meta) in self.manifest.present_paths() {
            if let Some(bytes) = self.content.get(&meta.hash) {
                out.insert(path.clone(), bytes.clone());
            }
        }
        out
    }

    /// Present paths whose content this node is missing (needs to fetch from a
    /// peer before it can materialize them). Drives content repair after a
    /// restart that lost the in-memory store.
    pub fn missing_content_hashes(&self) -> Vec<ContentHash> {
        let mut wanted = Vec::new();
        for (_, meta) in self.manifest.present_paths() {
            if !self.content.contains_key(&meta.hash) && !wanted.contains(&meta.hash) {
                wanted.push(meta.hash);
            }
        }
        wanted
    }

    /// Reconcile with `other`: both nodes adopt the merged manifest and exchange
    /// any content the other is missing. This is the in-process (loopback)
    /// backend; the on-wire backend performs the same exchange over a transport.
    ///
    /// After this returns, `self.manifest()` and `other.manifest()` both equal
    /// `self.manifest().merge(other.manifest())` (their pre-call join), and each
    /// side holds content for every present entry the other could supply.
    pub fn reconcile(&mut self, other: &mut SyncNode) -> Reconciled {
        // Capture both diffs against the original manifests before mutating.
        let self_adopts = self.manifest.diff_from(&other.manifest);
        let other_adopts = other.manifest.diff_from(&self.manifest);

        let mut stats = Reconciled {
            pulled: self_adopts.len(),
            pushed: other_adopts.len(),
            bytes: 0,
        };

        for adopt in &self_adopts.adopt {
            if let Entry::Present(meta) = &adopt.entry
                && let Some(bytes) = other.content.get(&meta.hash)
            {
                if !self.content.contains_key(&meta.hash) {
                    stats.bytes += bytes.len();
                }
                self.content.insert(meta.hash, bytes.clone());
            }
            self.manifest.insert(adopt.path.clone(), adopt.entry);
        }

        for adopt in &other_adopts.adopt {
            if let Entry::Present(meta) = &adopt.entry
                && let Some(bytes) = self.content.get(&meta.hash)
            {
                if !other.content.contains_key(&meta.hash) {
                    stats.bytes += bytes.len();
                }
                other.content.insert(meta.hash, bytes.clone());
            }
            other.manifest.insert(adopt.path.clone(), adopt.entry);
        }

        // Content repair: fill any present entry whose bytes a side still lacks
        // (e.g. adopted a hash the supplier didn't hold, or lost its store).
        stats.bytes += repair_content(self, other);
        stats.bytes += repair_content(other, self);

        stats
    }
}

/// Copy into `node` any content it is missing for its present entries that
/// `peer` can supply. Returns the number of bytes copied.
fn repair_content(node: &mut SyncNode, peer: &SyncNode) -> usize {
    let mut copied = 0;
    for hash in node.missing_content_hashes() {
        if let Some(bytes) = peer.content.get(&hash) {
            copied += bytes.len();
            node.content.insert(hash, bytes.clone());
        }
    }
    copied
}

/// Reconcile every pair repeatedly until a full pass moves nothing. Used by the
/// conformance tests (and models a gossip network reaching quiescence).
#[cfg(test)]
fn reconcile_to_fixpoint(nodes: &mut [SyncNode]) {
    loop {
        let mut changed = false;
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                let (left, right) = nodes.split_at_mut(j);
                let stats = left[i].reconcile(&mut right[0]);
                if !stats.is_noop() {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const CATALOG: PolicyRules = PolicyRules {
        propagate_deletes: false,
        sweep_tombstones: false,
    };
    const BUS: PolicyRules = PolicyRules {
        propagate_deletes: true,
        sweep_tombstones: false,
    };

    fn node(n: u8) -> SyncNode {
        SyncNode::new(Author([n; 32]))
    }

    #[test]
    fn local_write_is_echo_safe_on_unchanged_content() {
        let mut a = node(1);
        assert!(a.local_write("x", b"hello", 0, 0));
        // Writing identical content again does not bump the version.
        assert!(!a.local_write("x", b"hello", 5, 5));
        assert_eq!(a.manifest().get("x").unwrap().version(), 1);
    }

    #[test]
    fn reconcile_converges_two_nodes() {
        let mut a = node(1);
        let mut b = node(2);
        a.local_write("a.txt", b"from-a", 0, 0);
        b.local_write("b.txt", b"from-b", 0, 0);

        a.reconcile(&mut b);

        assert_eq!(a.folder_state(), b.folder_state());
        assert_eq!(a.folder_state().len(), 2);
        assert_eq!(a.folder_state()["a.txt"], b"from-a");
        assert_eq!(a.folder_state()["b.txt"], b"from-b");
    }

    #[test]
    fn second_reconcile_is_a_noop_no_echo() {
        let mut a = node(1);
        let mut b = node(2);
        a.local_write("a.txt", b"hi", 0, 0);
        let first = a.reconcile(&mut b);
        assert!(!first.is_noop());
        let second = a.reconcile(&mut b);
        assert!(
            second.is_noop(),
            "converged reconcile must move nothing: {second:?}"
        );
    }

    #[test]
    fn newer_version_wins_conflict() {
        let mut a = node(1);
        let mut b = node(2);
        a.local_write("x", b"a1", 0, 0); // (v1, author a)
        b.local_write("x", b"b1", 0, 0); // (v1, author b)
        a.reconcile(&mut b);
        // v1 tie → higher author (b=2) wins on both.
        assert_eq!(a.folder_state()["x"], b"b1");
        assert_eq!(b.folder_state()["x"], b"b1");

        // Now a edits again → v2 beats v1 everywhere.
        a.local_write("x", b"a2", 0, 0);
        a.reconcile(&mut b);
        assert_eq!(a.folder_state()["x"], b"a2");
        assert_eq!(b.folder_state()["x"], b"a2");
    }

    #[test]
    fn catalog_local_delete_is_restored_never_propagates() {
        let mut a = node(1);
        let mut b = node(2);
        a.local_write("keep.txt", b"payload", 0, 0);
        a.reconcile(&mut b);
        assert_eq!(b.folder_state()["keep.txt"], b"payload");

        // User deletes on a under catalog policy: manifest unchanged.
        let changed = a.local_remove("keep.txt", CATALOG, 0);
        assert!(!changed, "catalog delete must not change the manifest");
        // The file is still present in a's materialized folder (restored).
        assert_eq!(a.folder_state()["keep.txt"], b"payload");
        // And a reconcile does not delete it on b either.
        a.reconcile(&mut b);
        assert_eq!(b.folder_state()["keep.txt"], b"payload");
    }

    #[test]
    fn bus_delete_propagates_via_tombstone() {
        let mut a = node(1);
        let mut b = node(2);
        a.local_write("gone.txt", b"payload", 0, 0);
        a.reconcile(&mut b);
        assert!(b.folder_state().contains_key("gone.txt"));

        // Under bus policy a delete supersedes the present entry and propagates.
        assert!(a.local_remove("gone.txt", BUS, 100));
        a.reconcile(&mut b);
        assert!(!a.folder_state().contains_key("gone.txt"));
        assert!(!b.folder_state().contains_key("gone.txt"));
    }

    #[test]
    fn content_repair_restores_after_lost_store() {
        let mut a = node(1);
        let mut b = node(2);
        a.local_write("f", b"bytes", 0, 0);
        a.reconcile(&mut b);
        // Simulate a losing its content store but keeping its persisted manifest.
        a.content.clear();
        assert_eq!(a.missing_content_hashes().len(), 1);
        // Reconcile repairs content from b.
        a.reconcile(&mut b);
        assert_eq!(a.folder_state()["f"], b"bytes");
    }

    // ---------------- property tests: convergence under any interleaving -------

    #[derive(Debug, Clone)]
    enum Op {
        Write { node: usize, path: u8, content: u8 },
        Remove { node: usize, path: u8 },
        Reconcile { a: usize, b: usize },
    }

    fn arb_ops(n_nodes: usize) -> impl Strategy<Value = Vec<Op>> {
        let node_idx = 0..n_nodes;
        let pair = (0..n_nodes, 0..n_nodes).prop_filter("distinct nodes", |(a, b)| a != b);
        let op = prop_oneof![
            (node_idx.clone(), 0u8..3, 0u8..5).prop_map(|(node, path, content)| Op::Write {
                node,
                path,
                content
            }),
            (node_idx, 0u8..3).prop_map(|(node, path)| Op::Remove { node, path }),
            pair.prop_map(|(a, b)| Op::Reconcile { a, b }),
        ];
        prop::collection::vec(op, 0..40)
    }

    fn apply_ops(nodes: &mut [SyncNode], ops: &[Op], policy: PolicyRules) {
        for op in ops {
            match *op {
                Op::Write {
                    node,
                    path,
                    content,
                } => {
                    let p = format!("f{path}");
                    let bytes = vec![content; (content as usize) + 1];
                    nodes[node].local_write(&p, &bytes, 0, 0);
                }
                Op::Remove { node, path } => {
                    let p = format!("f{path}");
                    nodes[node].local_remove(&p, policy, 1);
                }
                Op::Reconcile { a, b } => {
                    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
                    let (left, right) = nodes.split_at_mut(hi);
                    left[lo].reconcile(&mut right[0]);
                }
            }
        }
    }

    proptest! {
        #[test]
        fn catalog_converges_under_any_interleaving(ops in arb_ops(3)) {
            let mut nodes = vec![node(1), node(2), node(3)];
            apply_ops(&mut nodes, &ops, CATALOG);
            reconcile_to_fixpoint(&mut nodes);

            let first = nodes[0].folder_state();
            for other in &nodes[1..] {
                prop_assert_eq!(&first, &other.folder_state());
            }
        }

        #[test]
        fn bus_converges_under_any_interleaving(ops in arb_ops(3)) {
            let mut nodes = vec![node(1), node(2), node(3)];
            apply_ops(&mut nodes, &ops, BUS);
            reconcile_to_fixpoint(&mut nodes);

            let first = nodes[0].folder_state();
            for other in &nodes[1..] {
                prop_assert_eq!(&first, &other.folder_state());
            }
            // All manifests are identical after quiescence too.
            for other in &nodes[1..] {
                prop_assert_eq!(nodes[0].manifest(), other.manifest());
            }
        }

        #[test]
        fn catalog_never_loses_a_file_that_was_shared(ops in arb_ops(3)) {
            // Any file written by any node and shared at least once must survive
            // to every node after quiescence — catalog never deletes.
            let mut nodes = vec![node(1), node(2), node(3)];
            // Seed a known file everyone will hold.
            nodes[0].local_write("seed.txt", b"seed", 0, 0);
            reconcile_to_fixpoint(&mut nodes);
            prop_assert!(nodes.iter().all(|n| n.folder_state().contains_key("seed.txt")));

            apply_ops(&mut nodes, &ops, CATALOG);
            reconcile_to_fixpoint(&mut nodes);

            // Even after arbitrary removes (which catalog ignores), the seed and
            // its content survive on every node.
            for n in &nodes {
                let folder = n.folder_state();
                prop_assert_eq!(
                    folder.get("seed.txt").map(Vec::as_slice),
                    Some(&b"seed"[..])
                );
            }
        }

        #[test]
        fn fixpoint_reconcile_is_noop_no_echo(ops in arb_ops(3)) {
            let mut nodes = vec![node(1), node(2), node(3)];
            apply_ops(&mut nodes, &ops, CATALOG);
            reconcile_to_fixpoint(&mut nodes);

            // One more full pass after quiescence transfers nothing.
            for i in 0..nodes.len() {
                for j in (i + 1)..nodes.len() {
                    let (left, right) = nodes.split_at_mut(j);
                    let stats = left[i].reconcile(&mut right[0]);
                    prop_assert!(stats.is_noop(), "echo after convergence: {:?}", stats);
                }
            }
        }
    }
}
