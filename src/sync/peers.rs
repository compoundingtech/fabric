//! What the daemon tells the engine about peers, and nothing more.

/// One peer as the engine sees it. `key` is transport-owned and opaque to the
/// engine; `id` is the display name used in status, logs, and sync cursors.
/// No address or peer policy crosses this process-neutral boundary.
#[derive(Debug, Clone)]
pub struct PeerRef {
    pub key: String,
    pub id: String,
    pub roaming: bool,
}

/// What an entry's peer selector resolves to right now, INCLUDING what it did
/// not resolve to.
///
/// A selector that matches nothing used to be dropped here without a record.
/// The engine then looped over the peers that did resolve, recorded nothing for
/// the one that did not, and every status surface called the entry clean and
/// syncing with every peer. That is what a typo in `syncs.toml` looks like from
/// day one, and what renaming a peer with `fabric add` looks like the moment
/// after. Finding 3 of the 2026-08-29 review.
#[derive(Debug, Clone, Default)]
pub struct ResolvedPeers {
    pub peers: Vec<PeerRef>,
    /// Selectors from the entry's `peers` that name no peer in the book. For a
    /// wildcard that selects nobody at all, the single selector `"*"`.
    pub unresolved: Vec<String>,
}

impl ResolvedPeers {
    pub fn all(peers: Vec<PeerRef>) -> Self {
        Self {
            peers,
            unresolved: Vec::new(),
        }
    }
}
