//! What changed here, and which peer has seen it.
//!
//! A pass ships the whole manifest today, so a change of eight bytes costs two
//! whole manifests on the wire. See
//! `a_small_change_must_not_ship_the_whole_manifest`. This module holds the
//! bookkeeping that lets a pass ship the changed paths instead.
//!
//! # The buffer holds PATHS, not entries
//!
//! A path that changes ten times occupies one slot, and the entry is looked up
//! from the manifest when it is sent. That is correct because the lattice is a
//! map of independent last-writer-wins registers: only the winning entry for a
//! path can matter, and an earlier one a peer never saw would lose to it anyway.
//!
//! It also bounds the buffer STRUCTURALLY rather than by policy. The buffer can
//! never hold more slots than the manifest has paths, however many writes went
//! through it, so there is no eviction threshold to pick and no runaway to
//! guard. A peer that has fallen far behind is not a growth problem here.
//!
//! # Why a local sequence rather than a version vector
//!
//! [`crate::sync::manifest::FileMeta::version`] is per-path Lamport, NOT a
//! per-author sequence, so a vector over authors cannot describe what a peer
//! holds. This sequence is local, dense and gap-free by construction, and it is
//! only ever compared against cursors this same node handed out.
//!
//! # The responder cannot acknowledge on its own
//!
//! This is a hole in the mechanism, not an implementation detail, and only
//! running it found it. It is recorded here because the shape is easy to
//! reinvent.
//!
//! A responder sees the initiator's digest when the Hello arrives, and that is
//! its only chance to notice the two sides already agree. Such a pass never
//! happens. Fabric runs NO pass at all when nothing has changed, and when
//! something has changed the digests differ by definition. So the responder
//! would hold a cursor for nobody and ship its whole manifest forever, while the
//! initiating side looked fixed.
//!
//! The initiator therefore reports the digest it LANDED on, after merging, in a
//! frame the responder reads before it answers. That frame is sent only to a
//! peer that reported a digest of its own, so an older build is never handed one
//! it does not read.
//!
//! # Re-sending is free, skipping is fatal
//!
//! The join is idempotent, so a peer that receives a change twice merges it
//! twice to the same result. A peer that never receives it diverges silently and
//! forever. Every choice here therefore errs toward sending again.
//!
//! That is why a node that restarts seeds the buffer with EVERY path
//! ([`ChangeBuffer::seed_from`]) and starts every peer at zero. The first pass
//! after a restart re-sends everything, which costs one full exchange and cannot
//! skip. Nothing about a cursor has to survive a crash, so no cursor can survive
//! it wrongly. That removes the footgun the delta-CRDT literature warns about:
//! a stale acknowledgement that makes a node skip changes made after recovery.

use std::collections::{BTreeMap, HashMap};

use crate::sync::manifest::Manifest;

/// A per-peer cursor into some node's [`ChangeBuffer`].
///
/// It is meaningful ONLY to the node that issued it. Zero means "has seen
/// nothing", which is the safe starting point and the value a restart restores.
pub type Cursor = u64;

/// The changed paths of one sync entry, and the sequence each changed at.
#[derive(Debug, Clone, Default)]
pub struct ChangeBuffer {
    next_seq: Cursor,
    by_seq: BTreeMap<Cursor, String>,
    by_path: HashMap<String, Cursor>,
    /// How much of this buffer each peer has confirmed taking.
    ///
    /// A peer ABSENT from this map has confirmed nothing, and that is the safe
    /// reading: it means send full state. A restart restores exactly that, for
    /// every peer, because nothing here is written to disk. A cursor that cannot
    /// be persisted cannot be restored wrongly.
    acked: HashMap<String, Cursor>,
}

impl ChangeBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// The cursor a peer that has seen everything would hold.
    pub fn head(&self) -> Cursor {
        self.next_seq
    }

    /// How many paths are waiting. Bounded by the manifest's path count.
    pub fn len(&self) -> usize {
        self.by_seq.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_seq.is_empty()
    }

    /// Note that `path` changed, and return its new sequence.
    ///
    /// A path already in the buffer MOVES to the new sequence rather than
    /// gaining a second slot. Only its latest state can ever be sent, so an
    /// older slot would cost memory and ship nothing extra.
    pub fn record(&mut self, path: &str) -> Cursor {
        let seq = self.next_seq;
        self.next_seq += 1;
        if let Some(previous) = self.by_path.insert(path.to_string(), seq) {
            self.by_seq.remove(&previous);
        }
        self.by_seq.insert(seq, path.to_string());
        seq
    }

    /// Start from a manifest with every path pending, as a restart does.
    ///
    /// A node that came back holding an empty buffer would send NOTHING to a
    /// peer sitting at cursor zero, which is silent divergence dressed as a
    /// cheap pass. Seeding makes the first pass a full re-send instead.
    pub fn seed_from(&mut self, manifest: &Manifest) {
        for (path, _) in manifest.entries() {
            self.record(path);
        }
    }

    /// Every path that changed after `cursor`, oldest first.
    ///
    /// A cursor from a different node, or one from before a restart, reads as
    /// far behind and yields more paths rather than fewer. Wrong in the safe
    /// direction is the whole design here.
    pub fn since(&self, cursor: Cursor) -> Vec<&str> {
        self.by_seq
            .range(cursor..)
            .map(|(_, path)| path.as_str())
            .collect()
    }

    /// How much of this buffer `peer` has confirmed taking.
    ///
    /// `None` means it has confirmed nothing and must be sent full state. That
    /// covers a peer met for the first time and every peer after a restart.
    pub fn cursor_for(&self, peer: &str) -> Option<Cursor> {
        self.acked.get(peer).copied()
    }

    /// Record that `peer` durably applied everything below `cursor`.
    ///
    /// Call this only AFTER the peer confirmed it. Recording an acknowledgement
    /// that has not happened is how a change gets skipped, and a skipped change
    /// is silent divergence.
    pub fn acknowledge(&mut self, peer: &str, cursor: Cursor) {
        self.acked.insert(peer.to_string(), cursor);
    }

    /// Forget what `peer` has taken, so the next pass sends it full state.
    ///
    /// This is the fallback trigger. It is always safe: the cost is one full
    /// exchange and the alternative is a peer that quietly stays behind.
    pub fn reset_peer(&mut self, peer: &str) {
        self.acked.remove(peer);
    }

    /// The cursor every one of `peers` has reached, which is ZERO when any of
    /// them has confirmed nothing.
    ///
    /// Eviction uses this. A peer that has confirmed nothing must hold eviction
    /// back completely, or the buffer drops a change that peer still needs and
    /// nothing ever tells it what it missed.
    pub fn acked_by_all(&self, peers: &[&str]) -> Cursor {
        let mut low = Cursor::MAX;
        for peer in peers {
            match self.acked.get(*peer) {
                Some(cursor) => low = low.min(*cursor),
                None => return 0,
            }
        }
        if peers.is_empty() { 0 } else { low }
    }

    /// Drop one path because it no longer exists in the manifest.
    ///
    /// A swept tombstone is the case: the entry is gone, so there is nothing to
    /// look up and nothing to send. Leaving the path here would offer a peer a
    /// slot that resolves to no entry. A sweep only runs after every peer
    /// acknowledged the tombstone, so dropping it strands nobody.
    pub fn forget_path(&mut self, path: &str) {
        if let Some(seq) = self.by_path.remove(path) {
            self.by_seq.remove(&seq);
        }
    }

    /// Drop paths every peer has already taken.
    ///
    /// `acked_by_all` must be the MINIMUM cursor across every peer this node
    /// syncs with, and zero when any peer's cursor is unknown. Passing anything
    /// larger drops a change a peer still needs, which is exactly the silent
    /// divergence this module exists to avoid.
    pub fn forget_through(&mut self, acked_by_all: Cursor) {
        let stale: Vec<Cursor> = self.by_seq.range(..acked_by_all).map(|(s, _)| *s).collect();
        for seq in stale {
            if let Some(path) = self.by_seq.remove(&seq) {
                // Only clear the reverse index when it still points at the slot
                // being dropped. A later `record` for the same path owns it now.
                if self.by_path.get(&path) == Some(&seq) {
                    self.by_path.remove(&path);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::manifest::{Author, ContentHash, Entry, FileMeta};
    use proptest::prelude::*;

    fn present(version: u64, hash_n: u8) -> Entry {
        Entry::Present(FileMeta {
            hash: ContentHash([hash_n; 32]),
            executable: false,
            size: hash_n as u64,
            mtime_secs: 0,
            mtime_nanos: 0,
            version,
            author: Author([1; 32]),
        })
    }

    #[test]
    fn a_fresh_buffer_offers_nothing_and_a_recorded_path_is_offered() {
        let mut buffer = ChangeBuffer::new();
        assert!(buffer.since(0).is_empty(), "nothing has changed yet");
        buffer.record("a.txt");
        assert_eq!(buffer.since(0), vec!["a.txt"]);
    }

    #[test]
    fn a_path_that_changes_twice_takes_one_slot_and_ships_once() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        buffer.record("a.txt");
        buffer.record("a.txt");
        assert_eq!(buffer.len(), 1, "three writes, one path, one slot");
        assert_eq!(buffer.since(0), vec!["a.txt"]);
    }

    #[test]
    fn a_caught_up_peer_is_offered_nothing() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        buffer.record("b.txt");
        assert!(
            buffer.since(buffer.head()).is_empty(),
            "a peer at the head has seen everything"
        );
    }

    #[test]
    fn a_peer_is_offered_only_what_it_has_not_seen() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        let cursor = buffer.head();
        buffer.record("b.txt");
        assert_eq!(
            buffer.since(cursor),
            vec!["b.txt"],
            "a.txt was already taken"
        );
    }

    /// A path that changes again AFTER a peer caught up must be offered to it
    /// again. This is the case a naive "already sent" flag gets wrong.
    #[test]
    fn a_path_changed_after_a_peer_caught_up_is_offered_again() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        let cursor = buffer.head();
        assert!(buffer.since(cursor).is_empty());
        buffer.record("a.txt");
        assert_eq!(buffer.since(cursor), vec!["a.txt"]);
    }

    /// A restart must re-send rather than skip. A buffer that came back empty
    /// would offer a peer at zero nothing at all.
    #[test]
    fn a_seeded_buffer_offers_every_path_to_a_peer_at_zero() {
        let mut manifest = Manifest::new();
        manifest.insert("a.txt".into(), present(1, 1));
        manifest.insert("b.txt".into(), present(1, 2));
        manifest.insert("c.txt".into(), present(1, 3));

        let mut buffer = ChangeBuffer::new();
        buffer.seed_from(&manifest);
        let mut offered = buffer.since(0);
        offered.sort_unstable();
        assert_eq!(offered, vec!["a.txt", "b.txt", "c.txt"]);
    }

    /// A swept tombstone has no entry left to send, so it must leave the buffer
    /// rather than sit there resolving to nothing.
    #[test]
    fn forgetting_one_path_removes_its_slot() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        buffer.record("b.txt");
        buffer.forget_path("a.txt");
        assert_eq!(buffer.since(0), vec!["b.txt"]);
        assert_eq!(buffer.len(), 1);
    }

    #[test]
    fn forgetting_an_unknown_path_changes_nothing() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        buffer.forget_path("never-seen.txt");
        assert_eq!(buffer.since(0), vec!["a.txt"]);
    }

    #[test]
    fn forgetting_drops_only_what_every_peer_took() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        let cursor = buffer.head();
        buffer.record("b.txt");
        buffer.forget_through(cursor);
        assert_eq!(buffer.since(0), vec!["b.txt"], "a.txt was taken by all");
    }

    /// Forgetting must not strand a path that changed AGAIN after the cursor.
    /// The reverse index makes this easy to get wrong.
    #[test]
    fn forgetting_keeps_a_path_that_changed_again_after_the_cursor() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        buffer.record("b.txt");
        let cursor = buffer.head();
        buffer.record("a.txt");
        buffer.forget_through(cursor);
        assert_eq!(
            buffer.since(0),
            vec!["a.txt"],
            "a.txt changed after the cursor and must survive"
        );
    }

    #[test]
    fn an_unknown_peer_has_no_cursor_and_must_get_full_state() {
        let buffer = ChangeBuffer::new();
        assert_eq!(buffer.cursor_for("hetz"), None);
    }

    #[test]
    fn acknowledging_then_resetting_returns_a_peer_to_full_state() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        buffer.acknowledge("hetz", buffer.head());
        assert_eq!(buffer.cursor_for("hetz"), Some(1));
        buffer.reset_peer("hetz");
        assert_eq!(
            buffer.cursor_for("hetz"),
            None,
            "a reset peer must be sent everything again"
        );
    }

    /// The eviction guard. One peer that has confirmed nothing must hold the
    /// whole buffer, or eviction drops a change that peer still needs and
    /// nothing will ever tell it what it missed.
    #[test]
    fn one_silent_peer_holds_eviction_back_completely() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        buffer.acknowledge("hetz", buffer.head());
        assert_eq!(
            buffer.acked_by_all(&["hetz", "droppy"]),
            0,
            "droppy has confirmed nothing, so nothing may be forgotten"
        );
        buffer.acknowledge("droppy", buffer.head());
        assert_eq!(buffer.acked_by_all(&["hetz", "droppy"]), 1);
    }

    #[test]
    fn the_slowest_peer_sets_the_eviction_point() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        let slow = buffer.head();
        buffer.record("b.txt");
        buffer.acknowledge("hetz", buffer.head());
        buffer.acknowledge("droppy", slow);
        assert_eq!(buffer.acked_by_all(&["hetz", "droppy"]), slow);
    }

    /// With no peers there is nothing to protect, but forgetting everything on
    /// that basis would strand the first peer that ever appears.
    #[test]
    fn no_peers_forgets_nothing() {
        let mut buffer = ChangeBuffer::new();
        buffer.record("a.txt");
        assert_eq!(buffer.acked_by_all(&[]), 0);
    }

    proptest! {
        /// THE CONTRACT. Whatever sequence of writes happened, a peer sitting at
        /// `cursor` must be offered every path that changed after it. Miss one
        /// and that peer diverges silently, which is the failure this module
        /// exists to prevent.
        #[test]
        fn every_path_changed_after_a_cursor_is_offered(
            writes in prop::collection::vec("[a-c]\\.txt", 0..30),
            split in 0usize..30,
        ) {
            let mut buffer = ChangeBuffer::new();
            let split = split.min(writes.len());
            for path in &writes[..split] {
                buffer.record(path);
            }
            let cursor = buffer.head();

            let mut expected = std::collections::BTreeSet::new();
            for path in &writes[split..] {
                buffer.record(path);
                expected.insert(path.clone());
            }

            let offered: std::collections::BTreeSet<String> =
                buffer.since(cursor).into_iter().map(String::from).collect();
            prop_assert_eq!(offered, expected);
        }

        /// The buffer can never outgrow the paths it has seen, however many
        /// writes went through it. This is the structural bound that replaces an
        /// eviction policy.
        #[test]
        fn the_buffer_never_outgrows_its_distinct_paths(
            writes in prop::collection::vec("[a-e]\\.txt", 0..60),
        ) {
            let mut buffer = ChangeBuffer::new();
            for path in &writes {
                buffer.record(path);
            }
            let distinct: std::collections::BTreeSet<&String> = writes.iter().collect();
            prop_assert_eq!(buffer.len(), distinct.len());
        }
    }
}
