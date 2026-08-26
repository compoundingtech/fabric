//! The pure reconciliation core: versioned per-file state and the merge/diff
//! algebra that makes any interleaving of edits converge.
//!
//! This module has **no I/O and no clock**. A [`Manifest`] is a snapshot of one
//! folder's logical state — a map from relative path to a versioned [`Entry`].
//! [`Manifest::merge`] is a **join on a semilattice**: it is commutative,
//! associative, and idempotent. Those three laws are what guarantee the
//! properties fabric promises:
//!
//! - **Convergence**: folding `merge` over a set of manifests in *any* order
//!   yields the same result — so any interleaving of edits across peers
//!   converges to one state.
//! - **Echo/loop freedom**: once two manifests are equal, `merge` produces no
//!   change, so a value synced A→B is never re-sent B→A as if new.
//! - **Newer-wins**: a higher Lamport-style logical `version` always wins
//!   (never a wall clock, which is unreliable across machines). At the same
//!   version, a present update wins over a tombstone so a concurrent edit is
//!   not silently lost; author and content hash provide deterministic
//!   tie-breaks within the same entry kind.
//!
//! Delete handling (tombstones) is modelled here so the wire format is stable,
//! but *policy* — whether deletes are created/applied/swept — is decided one
//! layer up (see [`crate::sync::config::PolicyRules`]). Catalog policy never
//! creates a tombstone; bus policy does.

use std::collections::{BTreeMap, btree_map};

use serde::{Deserialize, Serialize};

/// A content identity — the BLAKE3 hash of a file's bytes. Two files with the
/// same `ContentHash` have identical content (used for transfer dedup).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    pub fn to_hex(self) -> String {
        let mut s = String::with_capacity(64);
        for byte in self.0 {
            s.push_str(&format!("{byte:02x}"));
        }
        s
    }
}

/// A deterministic author identity used only to break version ties. In the
/// running daemon this is a peer's iroh NodeID bytes; in tests it is arbitrary.
/// The concrete bytes never matter — only that all peers agree on the same
/// total order over them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Author(pub [u8; 32]);

/// Metadata for one *present* file in a manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMeta {
    /// BLAKE3 hash of the file content — the transfer identity.
    pub hash: ContentHash,
    /// File size in bytes (informational; the hash is the identity).
    pub size: u64,
    /// Whether the file is executable, the way git tracks it. ONLY this bit
    /// replicates; no other permission bit crosses.
    ///
    /// The resulting MODE is not fixed, and an earlier version of this comment
    /// claimed it was. Materialization creates the file with `fs::write`, which
    /// yields `0666 & ~umask`, and then ORs in `0o111`. So the mode depends on
    /// the RECEIVING host's umask:
    ///
    /// | receiver umask | plain file | executable file |
    /// | --- | --- | --- |
    /// | 022 | 0644 | 0755 |
    /// | 002 | 0664 | 0775 |
    ///
    /// Observed on hetz and droppy on 2026-08-23: 0664 and 0775, both umask
    /// 002. Do NOT depend on an exact mode here; depend only on the executable
    /// bit. st2's render deliberately differs: it writes an exact mode that is
    /// immune to umask, so the two systems do not agree on the other bits and
    /// are not meant to.
    ///
    /// Defaulted so a peer that predates this field still parses. Note the
    /// asymmetry: adding a field with a default is safe, removing one is not.
    ///
    /// A chmod on an ALREADY SYNCED file does not propagate. See
    /// [`crate::sync::engine::METADATA_ONLY_CHANGES_DO_NOT_PROPAGATE`].
    #[serde(default)]
    pub executable: bool,
    /// The origin's modification time. **Informational only.**
    ///
    /// It is recorded, it crosses the wire, and it is NEVER applied to a
    /// materialized file. Fabric syncs the attributes git syncs, and git
    /// deliberately does not track mtime. A replica therefore carries its own
    /// local write time.
    ///
    /// So a consumer must NOT read a replica's mtime as an activity signal.
    /// Issue 27 is exactly that mistake. Not used for ordering either — see
    /// `version`.
    ///
    /// Retained rather than removed because removal is a breaking wire change:
    /// these fields have no default, so a peer on an older build fails to parse
    /// a manifest that omits them.
    pub mtime_secs: i64,
    pub mtime_nanos: u32,
    /// Lamport-style logical version. A local edit sets this to
    /// `max(all versions seen for this path) + 1`, so a later edit always
    /// outranks the versions it could have seen.
    pub version: u64,
    /// The node that produced this version; the deterministic tie-break when
    /// two peers reach the same `version` for a path.
    pub author: Author,
}

/// A record that a path was deleted. Produced under any delete-propagating
/// policy, which is now every policy: catalog creates these too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    pub version: u64,
    pub author: Author,
    /// Logical deletion time, for TTL-based sweeping under bus policy.
    pub deleted_secs: i64,
}

/// One path's state in a manifest: a present file, or a tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Entry {
    Present(FileMeta),
    Tombstone(Tombstone),
}

impl Entry {
    pub fn version(&self) -> u64 {
        match self {
            Entry::Present(meta) => meta.version,
            Entry::Tombstone(t) => t.version,
        }
    }

    pub fn author(&self) -> Author {
        match self {
            Entry::Present(meta) => meta.author,
            Entry::Tombstone(t) => t.author,
        }
    }

    pub fn is_present(&self) -> bool {
        matches!(self, Entry::Present(_))
    }

    pub fn meta(&self) -> Option<&FileMeta> {
        match self {
            Entry::Present(meta) => Some(meta),
            Entry::Tombstone(_) => None,
        }
    }

    /// Rank equal logical versions before author/hash tie-breaking. A present
    /// update outranks a tombstone at the same version so a concurrent
    /// update/delete keeps the update; a later higher-version delete still wins.
    fn kind_rank(&self) -> u8 {
        match self {
            Entry::Present(_) => 1,
            Entry::Tombstone(_) => 0,
        }
    }

    fn tiebreak_hash(&self) -> [u8; 32] {
        match self {
            Entry::Present(meta) => meta.hash.0,
            Entry::Tombstone(_) => [0u8; 32],
        }
    }

    /// The total-order key. Higher wins. Ordering by `version` first gives
    /// newer-wins; `kind_rank`, `author`, and `hash` then make it a *total*
    /// order so merge is a well-defined join and convergence is guaranteed.
    fn order_key(&self) -> (u64, u8, [u8; 32], [u8; 32]) {
        (
            self.version(),
            self.kind_rank(),
            self.author().0,
            self.tiebreak_hash(),
        )
    }

    /// True when `self` should win over `other` in a merge.
    pub fn wins_over(&self, other: &Entry) -> bool {
        self.order_key() > other.order_key()
    }
}

/// A snapshot of one folder's logical state: relative path → versioned entry.
///
/// Paths are normalized to forward-slash relative strings (see
/// [`Manifest::normalize_path`]); this is a portable key across macOS and Linux.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    entries: BTreeMap<String, Entry>,
}

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Normalize an arbitrary relative path into the canonical manifest key:
    /// forward-slash separated, no `.` or empty components. Returns `None` for a
    /// path that escapes the folder root (contains `..`) or is absolute — those
    /// must never enter a manifest.
    pub fn normalize_path(path: &str) -> Option<String> {
        if path.starts_with('/') || path.starts_with('\\') {
            return None;
        }
        let mut parts = Vec::new();
        for part in path.split(['/', '\\']) {
            match part {
                "" | "." => continue,
                ".." => return None,
                other => parts.push(other),
            }
        }
        if parts.is_empty() {
            return None;
        }
        Some(parts.join("/"))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// A fingerprint of the LATTICE POINT this manifest occupies.
    ///
    /// It covers `order_key` for every path, which is exactly what `merge`
    /// consults. Two peers that agree on every `order_key` make every merge in
    /// both directions a no-op, and that is what convergence means. So equal
    /// digests mean converged and unequal digests mean diverged.
    ///
    /// Counts cannot do this job: see `counts_are_blind_to_divergence`. This
    /// exists because silent divergence has no announcing moment, so the
    /// instrument has to exist before the thing it watches.
    ///
    /// Each path is length-prefixed so that "ab" + "c" cannot digest the same
    /// as "a" + "bc".
    ///
    /// It deliberately omits `size`, `executable` and `mtime_secs`. A
    /// whole-entry digest was built beside this one and then deleted, for two
    /// reasons. Those three fields are already determined by the merge key,
    /// because a replica stores the origin's `FileMeta` verbatim and the scanner
    /// compares content hashes rather than metadata. And a whole-entry digest
    /// must serialize every entry on every `sync ls`, which is real CPU spent to
    /// find a state that cannot arise. Do not add it back without a measurement
    /// that shows the state arising.
    pub fn digest(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        for (path, entry) in &self.entries {
            let (version, kind, author, tiebreak) = entry.order_key();
            hasher.update(&(path.len() as u64).to_le_bytes());
            hasher.update(path.as_bytes());
            hasher.update(&version.to_le_bytes());
            hasher.update(&[kind]);
            hasher.update(&author);
            hasher.update(&tiebreak);
        }
        hasher.finalize().to_hex().to_string()
    }

    pub fn get(&self, path: &str) -> Option<&Entry> {
        self.entries.get(path)
    }

    pub fn entries(&self) -> btree_map::Iter<'_, String, Entry> {
        self.entries.iter()
    }

    /// Paths whose latest entry is a present file (the files that should exist
    /// on disk). Catalog never creates tombstones, but may temporarily retain
    /// inherited ones until a surviving physical copy advances them.
    pub fn present_paths(&self) -> impl Iterator<Item = (&String, &FileMeta)> {
        self.entries.iter().filter_map(|(path, entry)| match entry {
            Entry::Present(meta) => Some((path, meta)),
            Entry::Tombstone(_) => None,
        })
    }

    /// Insert or replace the entry at `path`. The caller is responsible for
    /// version assignment; use [`Manifest::upsert_winning`] to preserve
    /// newer-wins when the intent is "record this only if it outranks".
    pub fn insert(&mut self, path: String, entry: Entry) {
        self.entries.insert(path, entry);
    }

    /// Insert `entry` at `path` only if it wins over any existing entry there.
    /// Returns whether the manifest changed. This is `merge` for a single path.
    pub fn upsert_winning(&mut self, path: String, entry: Entry) -> bool {
        match self.entries.get(&path) {
            Some(existing) if !entry.wins_over(existing) => false,
            _ => {
                self.entries.insert(path, entry);
                true
            }
        }
    }

    pub fn remove(&mut self, path: &str) -> Option<Entry> {
        self.entries.remove(path)
    }

    /// The join of two manifests: for every path, keep the winning entry.
    ///
    /// This is commutative, associative, and idempotent — the semilattice laws
    /// that give convergence and echo-freedom.
    pub fn merge(&self, other: &Manifest) -> Manifest {
        let mut out = self.clone();
        out.merge_in_place(other);
        out
    }

    /// `merge` into `self`, returning whether anything changed.
    pub fn merge_in_place(&mut self, other: &Manifest) -> bool {
        let mut changed = false;
        for (path, entry) in &other.entries {
            if self.upsert_winning(path.clone(), *entry) {
                changed = true;
            }
        }
        changed
    }

    /// What `self` must adopt from `remote` to reach `self.merge(remote)`:
    /// exactly the paths where the remote entry wins over ours. For each present
    /// adoption the engine fetches content (deduped by hash); for each tombstone
    /// adoption the engine deletes (only under a delete-applying policy).
    pub fn diff_from(&self, remote: &Manifest) -> ManifestDiff {
        let mut adopt = Vec::new();
        for (path, entry) in &remote.entries {
            let take = match self.entries.get(path) {
                Some(existing) => entry.wins_over(existing),
                None => true,
            };
            if take {
                adopt.push(AdoptEntry {
                    path: path.clone(),
                    entry: *entry,
                });
            }
        }
        ManifestDiff { adopt }
    }
}

/// The result of [`Manifest::diff_from`]: entries the local side should adopt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestDiff {
    pub adopt: Vec<AdoptEntry>,
}

impl ManifestDiff {
    pub fn is_empty(&self) -> bool {
        self.adopt.is_empty()
    }

    pub fn len(&self) -> usize {
        self.adopt.len()
    }

    /// The distinct content hashes that adopting present entries requires. The
    /// engine intersects these with content it already holds to avoid
    /// re-transferring bytes it can reconstruct locally.
    pub fn wanted_hashes(&self) -> Vec<ContentHash> {
        let mut hashes = Vec::new();
        for adopt in &self.adopt {
            if let Entry::Present(meta) = &adopt.entry
                && !hashes.contains(&meta.hash)
            {
                hashes.push(meta.hash);
            }
        }
        hashes
    }
}

/// One entry the local side should adopt from a remote manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptEntry {
    pub path: String,
    pub entry: Entry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn author(n: u8) -> Author {
        Author([n; 32])
    }

    fn hash(n: u8) -> ContentHash {
        ContentHash([n; 32])
    }

    fn present(version: u64, author_n: u8, hash_n: u8) -> Entry {
        Entry::Present(FileMeta {
            hash: hash(hash_n),
            executable: false,
            size: hash_n as u64,
            mtime_secs: 0,
            mtime_nanos: 0,
            version,
            author: author(author_n),
        })
    }

    fn tomb(version: u64, author_n: u8) -> Entry {
        Entry::Tombstone(Tombstone {
            version,
            author: author(author_n),
            deleted_secs: 0,
        })
    }

    /// `fabric sync ls` reports present and tombstone COUNTS. An operator reads
    /// two peers with equal counts as two peers that agree. Two peers can be
    /// equal by count and diverged in fact at the same moment. This test holds
    /// that blindness still so the digest has something to be measured against.
    #[test]
    fn counts_are_blind_to_divergence() {
        let mut a = Manifest::new();
        a.insert("notes.txt".into(), present(4, 1, 7));
        a.insert("gone.txt".into(), tomb(2, 1));

        let mut b = Manifest::new();
        // Same path, same version, DIFFERENT content. A real divergence.
        b.insert("notes.txt".into(), present(4, 1, 9));
        b.insert("gone.txt".into(), tomb(2, 1));

        assert_ne!(a, b, "the two manifests really do differ");

        let counts = |m: &Manifest| {
            let present = m.entries().filter(|(_, e)| e.is_present()).count();
            (present, m.len() - present, m.len())
        };
        assert_eq!(
            counts(&a),
            counts(&b),
            "counts agree while the state does not: this is the blindness"
        );
    }

    /// The digest must separate what the counts call equal. That is the whole
    /// reason it exists.
    #[test]
    fn digest_separates_manifests_that_counts_call_equal() {
        let mut a = Manifest::new();
        a.insert("notes.txt".into(), present(4, 1, 7));
        a.insert("gone.txt".into(), tomb(2, 1));

        let mut b = Manifest::new();
        b.insert("notes.txt".into(), present(4, 1, 9));
        b.insert("gone.txt".into(), tomb(2, 1));

        assert_ne!(
            a.digest(),
            b.digest(),
            "the digest must see what the counts cannot"
        );
    }

    /// Insertion order must not change the digest. Every peer learns entries in
    /// its own order, so an order-sensitive digest would report every peer as
    /// diverged from every other. That is the common case, not an edge case.
    #[test]
    fn digest_ignores_insertion_order() {
        let mut a = Manifest::new();
        a.insert("a.txt".into(), present(1, 1, 1));
        a.insert("b.txt".into(), present(2, 2, 2));
        a.insert("c.txt".into(), tomb(3, 3));

        let mut b = Manifest::new();
        b.insert("c.txt".into(), tomb(3, 3));
        b.insert("b.txt".into(), present(2, 2, 2));
        b.insert("a.txt".into(), present(1, 1, 1));

        assert_eq!(a.digest(), b.digest(), "same entries, same lattice point");
    }

    #[test]
    fn normalize_rejects_escapes_and_absolutes() {
        assert_eq!(
            Manifest::normalize_path("a/b.txt").as_deref(),
            Some("a/b.txt")
        );
        assert_eq!(Manifest::normalize_path("./a/./b").as_deref(), Some("a/b"));
        assert_eq!(Manifest::normalize_path("a//b").as_deref(), Some("a/b"));
        assert_eq!(Manifest::normalize_path("../secret"), None);
        assert_eq!(Manifest::normalize_path("a/../../secret"), None);
        assert_eq!(Manifest::normalize_path("/etc/passwd"), None);
        assert_eq!(Manifest::normalize_path(""), None);
        assert_eq!(Manifest::normalize_path("."), None);
    }

    #[test]
    fn higher_version_wins() {
        assert!(present(2, 0, 0).wins_over(&present(1, 9, 9)));
        assert!(!present(1, 9, 9).wins_over(&present(2, 0, 0)));
    }

    #[test]
    fn version_tie_breaks_by_author() {
        assert!(present(1, 5, 0).wins_over(&present(1, 4, 0)));
        assert!(!present(1, 4, 0).wins_over(&present(1, 5, 0)));
    }

    #[test]
    fn present_outranks_tombstone_at_equal_version_and_author() {
        assert!(present(3, 7, 1).wins_over(&tomb(3, 7)));
        assert!(!tomb(3, 7).wins_over(&present(3, 7, 1)));
    }

    #[test]
    fn newer_present_beats_older_tombstone() {
        // A re-create (edit) after a delete wins if its version is higher.
        assert!(present(4, 0, 1).wins_over(&tomb(3, 9)));
    }

    #[test]
    fn entries_follow_version_then_present_then_author_hash_order() {
        let cases = [
            (
                "newer present beats older tombstone",
                present(4, 0, 1),
                tomb(3, 9),
            ),
            (
                "newer tombstone beats older present",
                tomb(4, 0),
                present(3, 9, 1),
            ),
            (
                "present beats tombstone at equal version before author",
                present(4, 0, 1),
                tomb(4, 9),
            ),
            (
                "author breaks a same-version same-kind tie",
                present(4, 9, 1),
                present(4, 8, 9),
            ),
            (
                "hash breaks an exact version-author-kind tie",
                present(4, 9, 2),
                present(4, 9, 1),
            ),
        ];

        for (contract, winner, loser) in cases {
            assert!(winner.wins_over(&loser), "{contract}");
            assert!(!loser.wins_over(&winner), "{contract}");
        }
    }

    #[test]
    fn diff_is_empty_between_equal_manifests() {
        let mut a = Manifest::new();
        a.insert("x".into(), present(1, 0, 0));
        let b = a.clone();
        assert!(a.diff_from(&b).is_empty());
        // Echo-freedom: merging an equal manifest changes nothing.
        assert!(!a.clone().merge_in_place(&b));
    }

    #[test]
    fn diff_reports_only_winning_remote_entries() {
        let mut local = Manifest::new();
        local.insert("keep".into(), present(5, 0, 0));
        local.insert("older".into(), present(1, 0, 0));

        let mut remote = Manifest::new();
        remote.insert("keep".into(), present(2, 9, 9)); // loses to local v5
        remote.insert("older".into(), present(3, 0, 1)); // wins over local v1
        remote.insert("new".into(), present(1, 0, 2)); // local lacks it

        let diff = local.diff_from(&remote);
        let mut paths: Vec<_> = diff.adopt.iter().map(|a| a.path.clone()).collect();
        paths.sort();
        assert_eq!(paths, vec!["new", "older"]);
        assert_eq!(diff.wanted_hashes().len(), 2);
    }

    // ----- property tests: the semilattice laws that guarantee convergence -----

    fn arb_entry() -> impl Strategy<Value = Entry> {
        let version = 0u64..4;
        let author_n = 0u8..3;
        let hash_n = 0u8..3;
        prop_oneof![
            (version.clone(), author_n.clone(), hash_n).prop_map(|(v, a, h)| present(v, a, h)),
            (version, author_n).prop_map(|(v, a)| tomb(v, a)),
        ]
    }

    fn arb_manifest() -> impl Strategy<Value = Manifest> {
        // Small path domain forces frequent conflicts on the same key.
        prop::collection::vec(("[a-c]", arb_entry()), 0..6).prop_map(|pairs| {
            let mut m = Manifest::new();
            for (path, entry) in pairs {
                m.insert(path, entry);
            }
            m
        })
    }

    proptest! {
        #[test]
        fn merge_is_commutative(a in arb_manifest(), b in arb_manifest()) {
            prop_assert_eq!(a.merge(&b), b.merge(&a));
        }

        /// The instrument's contract, stated as a property. Two peers that have
        /// each merged the other MUST report the same digest. If this can fail,
        /// the digest cannot be used to detect divergence, because it would call
        /// converged peers diverged.
        #[test]
        fn merged_peers_agree_on_the_digest(a in arb_manifest(), b in arb_manifest()) {
            prop_assert_eq!(a.merge(&b).digest(), b.merge(&a).digest());
        }

        /// Equal digest means equal lattice point, and nothing weaker. This is
        /// the other half: the digest must not be so coarse that two genuinely
        /// different states share one.
        #[test]
        fn digest_equality_means_lattice_equality(a in arb_manifest(), b in arb_manifest()) {
            let keys = |m: &Manifest| {
                m.entries()
                    .map(|(p, e)| (p.clone(), e.order_key()))
                    .collect::<Vec<_>>()
            };
            prop_assert_eq!(a.digest() == b.digest(), keys(&a) == keys(&b));
        }

        #[test]
        fn merge_is_associative(
            a in arb_manifest(),
            b in arb_manifest(),
            c in arb_manifest(),
        ) {
            prop_assert_eq!(a.merge(&b).merge(&c), a.merge(&b.merge(&c)));
        }

        #[test]
        fn merge_is_idempotent(a in arb_manifest(), b in arb_manifest()) {
            let once = a.merge(&b);
            prop_assert_eq!(once.merge(&b), once.clone());
            prop_assert_eq!(a.merge(&a), a.clone());
        }

        #[test]
        fn convergence_is_order_independent(
            a in arb_manifest(),
            b in arb_manifest(),
            c in arb_manifest(),
        ) {
            // Any order of folding merge over {a,b,c} yields the same state.
            let abc = a.merge(&b).merge(&c);
            let cba = c.merge(&b).merge(&a);
            let bca = b.merge(&c).merge(&a);
            prop_assert_eq!(&abc, &cba);
            prop_assert_eq!(&abc, &bca);
        }

        #[test]
        fn merge_dominates_both_inputs(a in arb_manifest(), b in arb_manifest()) {
            // Every path's merged entry is >= the entry each input had there:
            // merge only ever moves a path forward in the order.
            let m = a.merge(&b);
            for input in [&a, &b] {
                for (path, entry) in input.entries() {
                    let merged = m.get(path).expect("merged keeps every path");
                    prop_assert!(
                        merged == entry || merged.wins_over(entry),
                        "merge regressed path {path}"
                    );
                }
            }
        }

        #[test]
        fn diff_then_adopt_reaches_merge(a in arb_manifest(), b in arb_manifest()) {
            // Applying exactly the diff adoptions turns `a` into `a.merge(b)`.
            let diff = a.diff_from(&b);
            let mut adopted = a.clone();
            for adopt in diff.adopt {
                adopted.insert(adopt.path, adopt.entry);
            }
            prop_assert_eq!(adopted, a.merge(&b));
        }

        #[test]
        fn empty_diff_iff_already_merged(a in arb_manifest(), b in arb_manifest()) {
            // No adoption needed exactly when a already dominates b everywhere.
            let diff = a.diff_from(&b);
            let already = a.merge(&b) == a;
            prop_assert_eq!(diff.is_empty(), already);
        }
    }
}
