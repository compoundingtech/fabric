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

use std::collections::{BTreeMap, HashMap, HashSet};

use super::config::PolicyRules;
use super::delta::ChangeBuffer;
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
    /// Which paths changed here, and which peer has seen them.
    changes: ChangeBuffer,
    /// Payloads this node has SENT that carried its entire manifest.
    ///
    /// Counts the OUTCOME, not the reason. First contact, a peer too old for
    /// deltas, a restart, and a cursor that stalled until its delta grew back to
    /// the whole manifest all land here, because from the wire's point of view
    /// they are the same event and the cost is identical.
    ///
    /// `delta_fallbacks` cannot answer this. It counts one cause, a payload
    /// found incomplete, and stays silent for the other three. This is the
    /// number that was described to a person as "how you would know the delta
    /// path is not working".
    ///
    /// Both sides count their own sends, so it also says WHICH end of a costly
    /// exchange decided to be costly. `reconcile_wire_bytes` cannot: it is
    /// counted on the initiator and includes the responder's reply, so a peer
    /// sending a whole manifest inflates the other machine's byte count and
    /// records nothing in its own.
    full_payload_sends: u64,
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
    /// Reconciles that found a payload was incomplete and fell back to full
    /// state. Zero is the healthy value. A number that RISES between two samples
    /// is a bug report: it means a cursor described state a peer did not hold.
    pub fallbacks: usize,
    /// EVERY byte this reconcile put on or took off the wire, not just content.
    ///
    /// `bytes` above counts content blobs only, and content is the SMALL part.
    /// A pass ships the entire manifest in its Hello frame whether or not
    /// anything changed — 10 MB of it on the bus entry — so a figure that
    /// excludes the manifest understates what a reconcile costs by orders of
    /// magnitude, and the manifest is precisely what delta replication exists to
    /// stop shipping.
    ///
    /// Measuring the thing we are about to remove is the point.
    pub wire_bytes: usize,
}

impl Reconciled {
    pub fn is_noop(&self) -> bool {
        self.pulled == 0 && self.pushed == 0 && self.bytes == 0
    }
}

/// What the caller knows when it asks [`SyncNode::sweep_tombstones`] to forget
/// a deletion. Every field is this machine's own clock; none of it is on the
/// wire, because a peer's word about time is not evidence about what this node
/// has replicated.
#[derive(Debug, Clone, Copy)]
pub struct SweepEvidence {
    /// Local time now.
    pub now_secs: i64,
    /// How long a tombstone must survive before it may be forgotten.
    pub ttl_secs: i64,
    /// The local time through which EVERY configured peer has completed a
    /// reconcile. `None` means the caller cannot prove that, and nothing is
    /// swept.
    pub acked_through: Option<i64>,
}

impl SyncNode {
    pub fn new(author: Author) -> Self {
        Self {
            author,
            manifest: Manifest::new(),
            content: HashMap::new(),
            changes: ChangeBuffer::new(),
            full_payload_sends: 0,
        }
    }

    pub fn author(&self) -> Author {
        self.author
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Payloads sent that carried this node's entire manifest.
    pub fn full_payload_sends(&self) -> u64 {
        self.full_payload_sends
    }

    /// Note that a payload about to go out carries the whole manifest.
    ///
    /// Takes the payload rather than a boolean so the caller cannot disagree
    /// with the definition. A payload holding every path IS the manifest,
    /// whatever flag it travels under, which is what makes a stalled cursor
    /// visible here even though it is sent as a delta.
    pub fn note_payload_sent(&mut self, payload: &Manifest) {
        if payload.len() == self.manifest.len() && !self.manifest.is_empty() {
            self.full_payload_sends += 1;
        }
    }

    /// The changed-path bookkeeping for this node.
    pub fn changes(&self) -> &ChangeBuffer {
        &self.changes
    }

    /// Mutable access, for the engine to seed on load and forget on acknowledge.
    pub fn changes_mut(&mut self) -> &mut ChangeBuffer {
        &mut self.changes
    }

    pub fn has_content(&self, hash: &ContentHash) -> bool {
        self.content.contains_key(hash)
    }

    /// Bytes of content held in memory for this entry, across every blob.
    pub fn content_bytes(&self) -> u64 {
        self.content.values().map(|bytes| bytes.len() as u64).sum()
    }

    pub fn content_blobs(&self) -> usize {
        self.content.len()
    }

    /// THE CONTENT STORE IS BOUNDED BY THE MANIFEST. A blob stays while some
    /// Present entry names its hash and goes when none does.
    ///
    /// It used to only grow. Every version of every file stayed resident until
    /// restart: one 5 MB file rewritten forty times cost 376 MB on the writer
    /// and 248 MB on its peer, and the bus entry rewrites status files all day.
    ///
    /// Dropping an unreferenced blob loses nothing a peer can ask for. A peer
    /// requests a hash only after adopting the entry that names it from THIS
    /// manifest, so every hash it can name is one this manifest still names.
    /// Received blobs are inserted by `put_content` without pruning, and the
    /// full prune runs only after adoption, so a blob that arrives ahead of its
    /// entry is still there when the entry lands.
    fn hash_is_referenced(&self, hash: &ContentHash) -> bool {
        self.manifest
            .present_paths()
            .any(|(_, meta)| meta.hash == *hash)
    }

    /// The per-write form: one candidate hash, one linear pass over the
    /// manifest. Cheaper than rebuilding the referenced set on every write.
    fn forget_if_unreferenced(&mut self, hash: ContentHash) {
        if !self.hash_is_referenced(&hash) {
            self.content.remove(&hash);
        }
    }

    /// The bulk form, after an adopt or a reconcile that may have superseded
    /// many entries at once.
    fn prune_unreferenced_content(&mut self) {
        let referenced: HashSet<ContentHash> = self
            .manifest
            .present_paths()
            .map(|(_, meta)| meta.hash)
            .collect();
        self.content.retain(|hash, _| referenced.contains(hash));
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
        self.local_write_with_mode(path, bytes, mtime_secs, mtime_nanos, false)
    }

    /// Record a local file write, carrying the executable bit git would track.
    ///
    /// The early return on unchanged content applies here too, so a chmod that
    /// alters no bytes does NOT advance a version and does not propagate. See
    /// `engine::METADATA_ONLY_CHANGES_DO_NOT_PROPAGATE`.
    pub fn local_write_with_mode(
        &mut self,
        path: &str,
        bytes: &[u8],
        mtime_secs: i64,
        mtime_nanos: u32,
        executable: bool,
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
        let previous = self.manifest.get(path);
        let next_version = previous.map(Entry::version).unwrap_or(0) + 1;
        let superseded = previous.and_then(Entry::meta).map(|meta| meta.hash);
        self.content.insert(hash, bytes.to_vec());
        self.changes.record(path);
        self.manifest.insert(
            path.to_string(),
            Entry::Present(FileMeta {
                hash,
                executable,
                size: bytes.len() as u64,
                mtime_secs,
                mtime_nanos,
                version: next_version,
                author: self.author,
            }),
        );
        if let Some(old) = superseded
            && old != hash
        {
            self.forget_if_unreferenced(old);
        }
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
        let superseded = entry.meta().map(|meta| meta.hash);
        self.changes.record(path);
        self.manifest.insert(
            path.to_string(),
            Entry::Tombstone(Tombstone {
                version: next_version,
                author: self.author,
                deleted_secs,
            }),
        );
        if let Some(old) = superseded {
            self.forget_if_unreferenced(old);
        }
        true
    }

    /// Forget tombstones that are provably dead and provably replicated.
    ///
    /// A tombstone is the only record that a path was deleted. Forgetting one
    /// is therefore **not** a local cleanup: `diff_from` adopts any entry a
    /// peer holds that we do not, so a node that forgets a tombstone while a
    /// peer still holds `Present` for that path adopts the file back. Sweeping
    /// too early resurrects deleted files across the fleet.
    ///
    /// So a path is swept only when every one of these holds:
    ///
    /// - the policy sweeps at all (bus does, catalog never does),
    /// - the entry is a tombstone,
    /// - nothing is on local disk for it (`observed` has no receipt), so we are
    ///   not discarding the record of a file that still exists,
    /// - the tombstone is older than `ttl_secs`, and
    /// - every peer acked *strictly after this node was seen holding the
    ///   tombstone*, which with whole-second stamps means on a later pass.
    ///
    /// That last rule is the subtle one. `deleted_secs` comes from the clock of
    /// whichever node performed the delete, so it says when the file died, NOT
    /// when this node started holding the record of it. A tombstone can reach us
    /// already older than the TTL — every tombstone on a bus that has never
    /// swept is — and then an ack collected before it arrived would appear to
    /// authorize forgetting it. `expired_since` closes that: it stamps the local
    /// time each tombstone was first seen expired here, and the ack must beat
    /// that stamp. See the sweep tests in this module.
    ///
    /// `evidence.acked_through` is the caller's proof of replication: the
    /// earliest local time at which *every* configured peer last completed a
    /// reconcile. The caller passes `None` when it cannot prove that for all
    /// peers, and then nothing is swept. Fail-closed is deliberate — the cost of
    /// not sweeping is CPU, and the cost of sweeping early is a deleted file
    /// coming back.
    ///
    /// `expired_since` is caller-owned scratch state, mutated here. It holds one
    /// entry per *expired, not yet swept* tombstone, so it stays small in steady
    /// state and empties as the sweep drains a backlog. Losing it (a restart)
    /// costs one more ack round, never correctness.
    ///
    /// Returns the swept paths. Content is intentionally left alone: it is
    /// shared by hash and may still back a present path.
    pub fn sweep_tombstones(
        &mut self,
        policy: PolicyRules,
        evidence: SweepEvidence,
        observed: &HashMap<String, ContentHash>,
        expired_since: &mut HashMap<String, i64>,
    ) -> Vec<String> {
        let SweepEvidence {
            now_secs,
            ttl_secs,
            acked_through,
        } = evidence;
        if !policy.sweep_tombstones || ttl_secs <= 0 {
            return Vec::new();
        }
        let mut swept = Vec::new();
        let mut waiting = HashMap::new();
        for (path, entry) in self.manifest.entries() {
            let Entry::Tombstone(tombstone) = entry else {
                continue;
            };
            if observed.contains_key(path) {
                continue;
            }
            let Some(expires_at) = tombstone.deleted_secs.checked_add(ttl_secs) else {
                continue;
            };
            if now_secs < expires_at {
                continue;
            }
            // First time we have seen this one expired. Stamp it now, so the
            // ack we demand is one this node earned while already holding it.
            //
            // STRICTLY LATER, not "at or after". Stamps are whole seconds, and
            // one pass can reconcile a peer, adopt an expired tombstone, and
            // sweep inside the same second. `acked >= held_since` read that as
            // proof and forgot the tombstone before the peer was ever sent it;
            // the peer still held the file and handed it back on the next pass,
            // which deleted it and swept it again, silently, for ever. An ack
            // from a strictly later second is from a reconcile that completed
            // after this pass, and only that reconcile can have carried the
            // tombstone. Finding 5 of the 2026-08-29 review.
            let held_since = expired_since.get(path).copied().unwrap_or(now_secs);
            if acked_through.is_some_and(|acked| acked > held_since) {
                swept.push(path.clone());
            } else {
                waiting.insert(path.clone(), held_since);
            }
        }
        for path in &swept {
            self.manifest.remove(path);
            // A swept path has no entry left to send. It only reaches a sweep
            // after every peer acknowledged the tombstone, so dropping its slot
            // strands nobody.
            self.changes.forget_path(path);
        }
        // Rebuilt rather than retained, so the map tracks exactly the tombstones
        // still waiting for an ack and never grows into a second manifest.
        *expired_since = waiting;
        swept
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

    /// Adopt every entry from `remote` that wins over ours, returning the number
    /// adopted. Content for any newly-present entry is fetched separately (over
    /// the wire) or is already held; an entry with no available content simply
    /// does not materialize until its bytes arrive.
    pub fn adopt(&mut self, remote: &Manifest) -> usize {
        let diff = self.manifest.diff_from(remote);
        let adopted = diff.adopt.len();
        for entry in diff.adopt {
            self.changes.record(&entry.path);
            self.manifest.insert(entry.path, entry.entry);
        }
        if adopted > 0 {
            self.prune_unreferenced_content();
        }
        adopted
    }

    /// Content hashes for the present entries of `delta` that this node holds.
    ///
    /// A DELTA PASS MUST USE THIS, never `hashes_peer_needs`. That function
    /// infers what a peer lacks by diffing against the manifest the peer sent,
    /// which is only sound when the peer sent its WHOLE manifest. Give it a
    /// delta and every path outside the delta looks like a path the peer is
    /// missing, so the pass pushes content for the entire tree. That turns the
    /// optimisation into a regression far worse than the thing it replaced.
    ///
    /// `hashes_peer_needs_is_unsound_for_a_delta` holds that trap still.
    pub fn content_for(&self, delta: &Manifest) -> Vec<ContentHash> {
        let mut out = Vec::new();
        for (_, meta) in delta.present_paths() {
            if self.content.contains_key(&meta.hash) && !out.contains(&meta.hash) {
                out.push(meta.hash);
            }
        }
        out
    }

    /// The content bytes this node holds for `hashes`, as `(hash, bytes)` pairs.
    /// Hashes it lacks are silently skipped. Used to bundle content for a peer.
    pub fn gather_content(&self, hashes: &[ContentHash]) -> Vec<(ContentHash, Vec<u8>)> {
        hashes
            .iter()
            .filter_map(|hash| self.content.get(hash).map(|bytes| (*hash, bytes.clone())))
            .collect()
    }

    /// The content hashes a peer will need if it adopts from us: the present
    /// entries where our manifest wins over `remote`.
    pub fn hashes_peer_needs(&self, remote: &Manifest) -> Vec<ContentHash> {
        remote.diff_from(&self.manifest).wanted_hashes()
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
            // The loopback path moves nothing over a wire.
            wire_bytes: 0,
            pulled: self_adopts.len(),
            pushed: other_adopts.len(),
            bytes: 0,
            // The loopback path merges both sides directly, so it cannot leave a
            // payload incomplete and has nothing to fall back from.
            fallbacks: 0,
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
            self.changes.record(&adopt.path);
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
            other.changes.record(&adopt.path);
            other.manifest.insert(adopt.path.clone(), adopt.entry);
        }

        // Content repair: fill any present entry whose bytes a side still lacks
        // (e.g. adopted a hash the supplier didn't hold, or lost its store).
        stats.bytes += repair_content(self, other);
        stats.bytes += repair_content(other, self);

        // Both sides may have superseded entries above; neither keeps the
        // bytes of an entry it no longer names.
        self.prune_unreferenced_content();
        other.prune_unreferenced_content();

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

    fn reconcile_pair(nodes: &mut [SyncNode], a: usize, b: usize) -> Reconciled {
        let (lo, hi) = if a < b { (a, b) } else { (b, a) };
        let (left, right) = nodes.split_at_mut(hi);
        left[lo].reconcile(&mut right[0])
    }

    /// The real bus preset, not a local fixture. The `BUS` const above predates
    /// the sweep and still says `sweep_tombstones: false`, so a sweep test
    /// written against it would pass while production behaved differently.
    fn bus() -> PolicyRules {
        crate::sync::config::SyncPolicy::Bus.rules()
    }

    /// One node holding a tombstone for `path`, deleted at `deleted_secs`, with
    /// nothing on disk for it.
    fn tombstoned(deleted_secs: i64) -> SyncNode {
        let mut a = node(1);
        a.local_write("gone.txt", b"bytes", 0, 0);
        assert!(a.local_remove("gone.txt", bus(), deleted_secs));
        a
    }

    const DAY: i64 = 24 * 60 * 60;

    /// One sweep with no memory of an earlier pass. A test that cares what a
    /// previous pass stamped calls `sweep_tombstones` directly with its own map.
    fn sweep_once(
        node: &mut SyncNode,
        policy: PolicyRules,
        now_secs: i64,
        acked_through: Option<i64>,
        observed: &HashMap<String, ContentHash>,
    ) -> Vec<String> {
        node.sweep_tombstones(
            policy,
            SweepEvidence {
                now_secs,
                ttl_secs: DAY,
                acked_through,
            },
            observed,
            &mut HashMap::new(),
        )
    }

    #[test]
    fn a_tombstone_is_swept_only_after_the_ttl_and_only_once_every_peer_acked() {
        // now, acked_through, expected-to-sweep. Expiry is deleted(100) + ttl.
        // A single pass with no memory of an earlier one can NEVER sweep: the
        // pass that first sees a tombstone expired stamps it, and the ack it
        // needs must postdate that stamp. So an ack equal to `now` refuses.
        let cases = [
            (100 + DAY, Some(100 + DAY), false, "exactly at expiry, first seen now"),
            (100 + 5 * DAY, Some(100 + 5 * DAY), false, "well past expiry, first seen now"),
            (100 + DAY - 1, Some(100 + 5 * DAY), false, "ttl not elapsed"),
            (100 + 5 * DAY, Some(100 + DAY - 1), false, "ack predates expiry"),
            (100 + 5 * DAY, None, false, "a peer never acked"),
        ];
        for (now, acked, expected, why) in cases {
            let mut a = tombstoned(100);
            let swept = sweep_once(&mut a, bus(), now, acked, &HashMap::new());
            assert_eq!(!swept.is_empty(), expected, "{why}");
            assert_eq!(
                a.manifest().get("gone.txt").is_none(),
                expected,
                "manifest disagrees with the returned sweep: {why}"
            );
        }

        // The positive case takes two passes: one to stamp, one with an ack
        // that postdates the stamp. Exactly at expiry and well past it.
        for first_seen in [100 + DAY, 100 + 5 * DAY] {
            let mut a = tombstoned(100);
            let mut expired_since = HashMap::new();
            let evidence = |now, acked| SweepEvidence {
                now_secs: now,
                ttl_secs: DAY,
                acked_through: Some(acked),
            };
            let swept = a.sweep_tombstones(
                bus(),
                evidence(first_seen, first_seen),
                &HashMap::new(),
                &mut expired_since,
            );
            assert!(swept.is_empty(), "first_seen={first_seen}: swept on first sight");
            let swept = a.sweep_tombstones(
                bus(),
                evidence(first_seen + 1, first_seen + 1),
                &HashMap::new(),
                &mut expired_since,
            );
            assert_eq!(swept, vec!["gone.txt".to_string()], "first_seen={first_seen}");
            assert!(a.manifest().get("gone.txt").is_none());
        }
    }

    #[test]
    fn catalog_policy_never_sweeps_however_old_the_tombstone_is() {
        let mut a = tombstoned(100);
        let swept = sweep_once(
            &mut a,
            CATALOG,
            100 + 999 * DAY,
            Some(i64::MAX),
            &HashMap::new(),
        );
        assert!(swept.is_empty());
        assert!(a.manifest().get("gone.txt").is_some());
    }

    #[test]
    fn a_tombstone_whose_file_is_still_on_disk_is_never_swept() {
        let mut a = tombstoned(100);
        // An observed receipt means the bytes are physically present. Forgetting
        // the tombstone here would republish the file as a fresh local write.
        let observed = HashMap::from([("gone.txt".to_string(), content_hash(b"bytes"))]);
        let swept = sweep_once(&mut a, bus(), 100 + 9 * DAY, Some(i64::MAX), &observed);
        assert!(swept.is_empty());
        assert!(a.manifest().get("gone.txt").is_some());
    }

    #[test]
    fn a_present_entry_is_never_swept() {
        let mut a = node(1);
        a.local_write("live.txt", b"bytes", 0, 0);
        let swept = sweep_once(&mut a, bus(), i64::MAX / 2, Some(i64::MAX), &HashMap::new());
        assert!(swept.is_empty());
        assert!(a.manifest().get("live.txt").unwrap().is_present());
    }

    /// The reason the ack gate exists, proved rather than asserted.
    ///
    /// A node that forgets a tombstone a peer never received adopts the peer's
    /// `Present` back, because `diff_from` takes any entry we do not hold. This
    /// test sweeps under both rules and shows the unguarded one resurrects a
    /// deleted file while the guarded one does not.
    #[test]
    fn sweeping_before_a_peer_acked_resurrects_the_deleted_file() {
        // b never learned about the delete: it still holds the file.
        let mut b = node(2);
        b.local_write("gone.txt", b"bytes", 0, 0);

        // Unguarded: sweep with an ack that satisfies the rule, which models
        // trusting a TTL alone. The file comes back on the next reconcile.
        let mut unguarded = tombstoned(100);
        let swept = sweep_once(
            &mut unguarded,
            bus(),
            100 + 9 * DAY,
            Some(i64::MAX),
            &HashMap::new(),
        );
        assert_eq!(swept, vec!["gone.txt".to_string()]);
        let mut peer = b.clone();
        unguarded.reconcile(&mut peer);
        assert!(
            unguarded.manifest().get("gone.txt").is_some_and(|e| e.is_present()),
            "sweeping early must resurrect the file, or this test proves nothing"
        );

        // Guarded: b has never acked, so acked_through is None and nothing is
        // swept. The tombstone survives the same reconcile and still wins.
        let mut guarded = tombstoned(100);
        let swept = sweep_once(&mut guarded, bus(), 100 + 9 * DAY, None, &HashMap::new());
        assert!(swept.is_empty());
        let mut peer = b.clone();
        guarded.reconcile(&mut peer);
        assert!(
            !guarded.manifest().get("gone.txt").unwrap().is_present(),
            "the tombstone must still beat the peer's stale Present"
        );
        assert!(!peer.manifest().get("gone.txt").unwrap().is_present());
    }

    /// An ack older than the tombstone itself proves nothing.
    ///
    /// `deleted_secs` is the ORIGINATING node's clock reading, so it says when
    /// the file died, not when THIS node started holding the record of it. A
    /// tombstone that reaches us already older than the TTL is therefore
    /// sweepable on its first pass, against acks collected before we had it.
    ///
    /// One pass reconciles peers in list order, so this is ordinary: `x` acks,
    /// then `h` hands us a tombstone `x` has never seen, then the sweep runs.
    /// Finding 5 of the 2026-08-29 review: the same shape as the test below,
    /// with the clock reading the SAME SECOND for the ack and the sweep.
    ///
    /// Stamps are whole seconds. A pass that reconciles x (ack at T), then
    /// adopts an already expired tombstone from h, then sweeps, all inside
    /// second T, used to see `acked >= held_since` as `T >= T` and forget the
    /// tombstone before x was ever sent it. x still held the file, so the next
    /// pass adopted it back, deleted it again, swept again, and nothing said so.
    ///
    /// The rule that closes it: the ack must come from a reconcile that
    /// completed AFTER the pass that first saw the tombstone expired. With
    /// whole seconds that is a strictly later stamp, which is always a later
    /// pass. The control at the end proves the gate still opens then.
    #[test]
    fn a_tombstone_first_seen_expired_this_pass_is_not_swept_this_pass() {
        let mut x = node(3);
        x.local_write("gone.txt", b"bytes", 0, 0);
        let mut h = tombstoned(100);
        let mut m = node(2);

        // Pass 1, all inside one second T.
        let t = 100 + 9 * DAY;
        m.reconcile(&mut x); // x acks at T; m does not hold the tombstone yet
        m.reconcile(&mut h); // now it does, already expired
        let mut expired_since = HashMap::new();
        let evidence = |now, acked| SweepEvidence {
            now_secs: now,
            ttl_secs: DAY,
            acked_through: Some(acked),
        };
        let swept = m.sweep_tombstones(bus(), evidence(t, t), &HashMap::new(), &mut expired_since);
        assert!(
            swept.is_empty(),
            "swept in the same second the tombstone arrived, before x was sent it"
        );
        assert!(
            m.manifest().get("gone.txt").is_some_and(|e| !e.is_present()),
            "m must still hold the tombstone"
        );

        // Pass 2, one second later: x is told, and its ack now postdates the
        // stamp. This is the control: the gate opens when it should.
        m.reconcile(&mut x);
        assert!(
            !x.manifest().get("gone.txt").unwrap().is_present(),
            "x never received the tombstone, so the sweep below would strand it"
        );
        let swept = m.sweep_tombstones(
            bus(),
            evidence(t + 1, t + 1),
            &HashMap::new(),
            &mut expired_since,
        );
        assert_eq!(swept, vec!["gone.txt".to_string()]);

        // And the file does not come back from x afterwards. x still holds
        // the tombstone and may hand it back; a tombstone is not the file.
        m.reconcile(&mut x);
        assert!(
            !m.manifest().get("gone.txt").is_some_and(|e| e.is_present()),
            "RESURRECTED after a correct sweep"
        );
    }

    /// Every existing tombstone on the live bus is older than any sane TTL, so
    /// the first pass after the sweep is enabled is exactly this shape.
    #[test]
    fn an_ack_collected_before_the_tombstone_arrived_does_not_authorize_a_sweep() {
        // x is reachable and acking, and it still holds the file.
        let mut x = node(3);
        x.local_write("gone.txt", b"bytes", 0, 0);
        // h deleted it long ago: older than the TTL by nine days.
        let mut h = tombstoned(100);

        let mut m = node(2);

        // The pass reconciles x first, so x's ack is stamped here, and at this
        // moment m does not hold the tombstone yet.
        m.reconcile(&mut x);
        let acked_through = 100 + 9 * DAY;

        // Then h hands m the tombstone. x has not been told.
        m.reconcile(&mut h);
        let now = acked_through + 1;

        let mut expired_since = HashMap::new();
        let evidence = |now, acked| SweepEvidence {
            now_secs: now,
            ttl_secs: DAY,
            acked_through: Some(acked),
        };
        let swept = m.sweep_tombstones(
            bus(),
            evidence(now, acked_through),
            &HashMap::new(),
            &mut expired_since,
        );
        assert!(
            swept.is_empty(),
            "x acked before m held this tombstone, so x cannot have received it"
        );
        assert_eq!(
            expired_since.get("gone.txt"),
            Some(&now),
            "the refusal must be recorded, or the next pass repeats it forever"
        );

        // The consequence, so the assertion above is about a real outcome.
        m.reconcile(&mut x);
        assert!(
            !m.manifest().get("gone.txt").unwrap().is_present(),
            "the deleted file must not come back from x"
        );

        // Second pass. That reconcile told x, and x acks after the stamp, so the
        // same tombstone is now provably replicated and the sweep proceeds.
        let later = now + 60;
        let swept = m.sweep_tombstones(
            bus(),
            evidence(later, later),
            &HashMap::new(),
            &mut expired_since,
        );
        assert_eq!(swept, vec!["gone.txt".to_string()], "waiting must end");
        assert!(
            expired_since.is_empty(),
            "a swept path must not keep a stamp; the map tracks only the backlog"
        );
    }

    #[test]
    fn a_local_write_records_exactly_that_path() {
        let mut node = node(1);
        node.local_write("a.txt", b"hello", 0, 0);
        assert_eq!(node.changes().since(0), vec!["a.txt"]);
    }

    /// The echo-safe early return must not record. A node that recorded on every
    /// re-scan would offer every peer every path forever and the delta would
    /// cost more than the manifest.
    #[test]
    fn rewriting_identical_content_records_nothing() {
        let mut node = node(1);
        node.local_write("a.txt", b"hello", 0, 0);
        let cursor = node.changes().head();
        assert!(!node.local_write("a.txt", b"hello", 0, 0), "echo-safe");
        assert!(
            node.changes().since(cursor).is_empty(),
            "an unchanged rewrite must not enter the buffer"
        );
    }

    #[test]
    fn a_local_delete_records_the_tombstoned_path() {
        let mut node = node(1);
        node.local_write("a.txt", b"hello", 0, 0);
        let cursor = node.changes().head();
        assert!(node.local_remove("a.txt", bus(), 100));
        assert_eq!(node.changes().since(cursor), vec!["a.txt"]);
    }

    #[test]
    fn adopting_from_a_peer_records_what_was_adopted() {
        let mut source = node(1);
        source.local_write("a.txt", b"hello", 0, 0);
        source.local_write("b.txt", b"world", 0, 0);

        let mut target = node(2);
        let cursor = target.changes().head();
        assert_eq!(target.adopt(source.manifest()), 2);
        let mut recorded = target.changes().since(cursor);
        recorded.sort_unstable();
        assert_eq!(recorded, vec!["a.txt", "b.txt"]);
    }

    /// A restart must re-send, never skip.
    ///
    /// The engine restores a node by adopting the manifest it read from disk,
    /// and `adopt` records. So the buffer comes back holding every path and a
    /// peer at cursor zero is offered all of it. If `adopt` ever stops
    /// recording, a restarted node offers a peer NOTHING and the peer waits
    /// forever for changes it already missed. That is silent divergence, and it
    /// is exactly the crash-shaped footgun the delta literature warns about.
    #[test]
    fn a_node_restored_from_disk_offers_every_path_to_a_peer_at_zero() {
        let mut before = node(1);
        before.local_write("x.txt", b"one", 0, 0);
        before.local_write("y.txt", b"two", 0, 0);
        before.local_remove("y.txt", bus(), 100);
        before.local_write("z.txt", b"three", 0, 0);

        // What the engine does on load: a fresh node adopts the persisted
        // manifest. Nothing else restores the buffer.
        let mut restored = node(1);
        restored.adopt(before.manifest());

        let mut offered = restored.changes().since(0);
        offered.sort_unstable();
        assert_eq!(
            offered,
            vec!["x.txt", "y.txt", "z.txt"],
            "a restarted node must offer every path, tombstones included"
        );
        assert_eq!(
            restored.manifest().digest(),
            before.manifest().digest(),
            "the restore itself lost state, so the buffer check proves nothing"
        );
    }

    /// The trap a delta pass must not fall into, written down so nobody has to
    /// rediscover it by shipping it.
    ///
    /// `hashes_peer_needs` answers "what must the peer adopt from us", by
    /// diffing against the manifest the peer sent. That is right for a full
    /// manifest and WRONG for a delta: paths outside the delta look absent, so
    /// the answer becomes the whole tree.
    #[test]
    fn hashes_peer_needs_is_unsound_for_a_delta() {
        let mut node = node(1);
        node.local_write("x.txt", b"one", 0, 0);
        node.local_write("y.txt", b"two", 0, 0);
        node.local_write("z.txt", b"three", 0, 0);

        // The peer holds everything, and only z.txt is actually in flight.
        let peer_full = node.manifest().clone();
        let delta = peer_full.subset(["z.txt"]);

        assert!(
            node.hashes_peer_needs(&peer_full).is_empty(),
            "against the peer's whole manifest there is nothing to push"
        );
        assert_eq!(
            node.hashes_peer_needs(&delta).len(),
            2,
            "against a delta it claims the peer needs every path OUTSIDE the \
             delta, which is the entire tree on a real folder"
        );
        assert_eq!(
            node.content_for(&delta).len(),
            1,
            "content_for ships exactly the delta's own content"
        );
    }

    #[test]
    fn content_for_skips_entries_whose_bytes_we_lack() {
        let mut holder = node(1);
        holder.local_write("x.txt", b"one", 0, 0);
        let delta = holder.manifest().clone();

        // A node that knows the entry but never received its bytes.
        let mut empty = node(2);
        empty.adopt(&delta);
        assert!(
            empty.content_for(&delta).is_empty(),
            "a node cannot offer bytes it does not hold"
        );
    }

    #[test]
    fn content_for_ignores_tombstones() {
        let mut node = node(1);
        node.local_write("x.txt", b"one", 0, 0);
        node.local_remove("x.txt", bus(), 100);
        let delta = node.manifest().clone();
        assert!(
            node.content_for(&delta).is_empty(),
            "a tombstone has no content to ship"
        );
    }

    /// THE EQUIVALENCE THE DELTA PATH RESTS ON. Sending only the changed paths
    /// must land a peer on the SAME lattice point as sending the whole manifest.
    ///
    /// The digest is the oracle. If these two ever differ, a delta pass and a
    /// full pass disagree about the state of the world, and the disagreement is
    /// silent because both sides report a healthy pass.
    #[test]
    fn shipping_only_the_changed_paths_reaches_the_same_digest_as_shipping_everything() {
        let mut source = node(1);
        source.local_write("x.txt", b"one", 0, 0);
        source.local_write("y.txt", b"two", 0, 0);
        source.local_write("z.txt", b"three", 0, 0);

        // A peer that has already caught up on all three.
        let mut caught_up = node(2);
        caught_up.adopt(source.manifest());
        let cursor = source.changes().head();

        // Now the source changes two of them and deletes the third.
        source.local_write("x.txt", b"one changed", 1, 0);
        source.local_write("y.txt", b"two changed", 1, 0);
        source.local_remove("z.txt", bus(), 100);

        let changed = source.changes().since(cursor);
        assert_eq!(changed.len(), 3, "three paths moved");

        // One peer takes only the changed paths. Another takes everything.
        let mut by_delta = caught_up.clone();
        by_delta.adopt(&source.manifest().subset(changed));

        let mut by_full = caught_up.clone();
        by_full.adopt(source.manifest());

        assert_eq!(
            by_delta.manifest().digest(),
            by_full.manifest().digest(),
            "a delta pass and a full pass reached different lattice points"
        );
        assert_eq!(
            by_delta.manifest().digest(),
            source.manifest().digest(),
            "neither peer actually caught up with the source"
        );
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
    fn higher_version_recreation_resurrects_a_tombstone_and_replay_is_noop() {
        let mut a = node(1);
        let mut b = node(2);
        a.local_write("job", b"v1", 0, 0);
        a.reconcile(&mut b);

        assert!(a.local_remove("job", BUS, 10));
        a.reconcile(&mut b);
        assert!(matches!(
            a.manifest().get("job"),
            Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
        ));

        // Re-creating the path after observing its deletion advances from the
        // tombstone's version, so the new Present wins everywhere.
        assert!(b.local_write("job", b"v3-resurrected", 20, 0));
        assert!(matches!(
            b.manifest().get("job"),
            Some(Entry::Present(meta)) if meta.version == 3
        ));
        let first = a.reconcile(&mut b);
        assert!(!first.is_noop());
        assert_eq!(a.folder_state()["job"], b"v3-resurrected");
        assert_eq!(b.folder_state()["job"], b"v3-resurrected");

        let replay = a.reconcile(&mut b);
        assert!(
            replay.is_noop(),
            "replaying a converged resurrection echoed state: {replay:?}"
        );
    }

    #[test]
    fn offline_peer_rejoins_delete_and_resurrection_in_different_orders() {
        let schedules: &[&[(usize, usize)]] = &[
            &[(0, 2), (1, 2)],
            &[(1, 2), (0, 2)],
            &[(0, 2), (0, 1), (1, 2)],
        ];

        for schedule in schedules {
            let mut deleted = vec![node(1), node(2), node(3)];
            deleted[0].local_write("shared", b"seed", 0, 0);
            reconcile_to_fixpoint(&mut deleted);

            // Peer 2 is offline while peers 0 and 1 adopt a Tombstone/v2.
            assert!(deleted[0].local_remove("shared", BUS, 10));
            reconcile_pair(&mut deleted, 0, 1);
            for &(a, b) in *schedule {
                reconcile_pair(&mut deleted, a, b);
            }
            reconcile_to_fixpoint(&mut deleted);
            for peer in &deleted {
                assert!(matches!(
                    peer.manifest().get("shared"),
                    Some(Entry::Tombstone(tombstone)) if tombstone.version == 2
                ));
                assert!(!peer.folder_state().contains_key("shared"));
            }
            for a in 0..deleted.len() {
                for b in (a + 1)..deleted.len() {
                    assert!(
                        reconcile_pair(&mut deleted, a, b).is_noop(),
                        "delete replay echoed after schedule {schedule:?}"
                    );
                }
            }

            let mut resurrected = vec![node(1), node(2), node(3)];
            resurrected[0].local_write("shared", b"seed", 0, 0);
            reconcile_to_fixpoint(&mut resurrected);
            resurrected[0].local_remove("shared", BUS, 10);
            reconcile_pair(&mut resurrected, 0, 1);

            // Peer 1 recreates the file after seeing Tombstone/v2. Peer 2 is
            // still offline with Present/v1; every rejoin order must choose v3.
            resurrected[1].local_write("shared", b"revived", 20, 0);
            for &(a, b) in *schedule {
                reconcile_pair(&mut resurrected, a, b);
            }
            reconcile_to_fixpoint(&mut resurrected);
            for peer in &resurrected {
                assert!(matches!(
                    peer.manifest().get("shared"),
                    Some(Entry::Present(meta)) if meta.version == 3
                ));
                assert_eq!(peer.folder_state()["shared"], b"revived");
            }
        }
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

    /// Finding 4 of the 2026-08-29 review. The content store only grew: every
    /// version of every file stayed in memory until restart. One 5 MB file
    /// rewritten forty times cost 376 MB of resident memory on the writer and
    /// 248 MB on its peer, for a live file of 5 MB.
    ///
    /// The bound is the manifest: a blob stays while some Present entry names
    /// it, and goes when none does. That is the SMALLEST store that can still
    /// materialize the tree and answer every peer, because a peer only ever
    /// asks for a hash it just adopted from this manifest.
    #[test]
    fn a_superseded_version_is_not_held() {
        let mut n = node(1);
        for version in 0..40u8 {
            n.local_write("hot.md", &vec![version; 5000], 0, 0);
        }
        assert_eq!(
            n.content_blobs(),
            1,
            "forty rewrites of one path must leave one blob, the live one"
        );
        assert_eq!(n.content_bytes(), 5000);
        assert!(n.has_content(&content_hash(&vec![39u8; 5000])));
    }

    /// The control that keeps the rule honest: content is shared by hash, so
    /// a blob two paths name survives one of them moving on, and goes only
    /// when the last reference goes, whether by rewrite or by tombstone.
    #[test]
    fn shared_content_survives_until_its_last_reference_goes() {
        let mut n = node(2);
        let same = content_hash(b"same bytes");
        n.local_write("a.md", b"same bytes", 0, 0);
        n.local_write("b.md", b"same bytes", 0, 0);
        n.local_write("a.md", b"a moved on", 0, 0);
        assert!(
            n.has_content(&same),
            "b.md still names these bytes; dropping them would leave b.md unmaterializable"
        );
        assert!(n.local_remove("b.md", bus(), 10));
        assert!(
            !n.has_content(&same),
            "nothing names these bytes now, and a tombstone needs no content"
        );
        assert_eq!(n.content_blobs(), 1);
    }

    /// Adoption is the other way a version is superseded. A peer's newer
    /// entry replaces ours, and our old bytes must go with the old entry.
    #[test]
    fn content_superseded_by_adoption_is_not_held() {
        let mut a = node(1);
        let mut b = node(2);
        a.local_write("f.md", b"first", 0, 0);
        a.reconcile(&mut b);
        assert!(b.has_content(&content_hash(b"first")));

        // b moves the file on; a adopts the newer version and its bytes.
        b.local_write("f.md", b"second", 0, 0);
        a.reconcile(&mut b);
        for (label, n) in [("a", &a), ("b", &b)] {
            assert!(
                !n.has_content(&content_hash(b"first")),
                "{label} still holds bytes no Present entry names"
            );
            assert!(n.has_content(&content_hash(b"second")));
            assert_eq!(n.content_blobs(), 1, "{label}");
        }
    }

    proptest! {
        /// The property, over arbitrary write and delete sequences on two
        /// reconciling nodes: after every step each node holds exactly the
        /// blobs its Present entries name, no more and no fewer.
        #[test]
        fn content_held_is_exactly_what_the_manifest_names(
            steps in proptest::collection::vec((0u8..2, 0u8..4, 0u8..6, any::<bool>()), 1..60)
        ) {
            let mut nodes = [node(1), node(2)];
            for (who, path, content, delete) in steps {
                let path = format!("p{path}");
                let n = &mut nodes[who as usize];
                if delete {
                    n.local_remove(&path, bus(), 1);
                } else {
                    n.local_write(&path, &[content; 16], 0, 0);
                }
                let (left, right) = nodes.split_at_mut(1);
                left[0].reconcile(&mut right[0]);
                for n in &nodes {
                    let named: std::collections::HashSet<ContentHash> =
                        n.manifest().present_paths().map(|(_, m)| m.hash).collect();
                    let held: std::collections::HashSet<ContentHash> =
                        n.content.keys().copied().collect();
                    prop_assert_eq!(&held, &named, "held must equal named");
                }
            }
        }
    }
}
