//! What the daemon, its sync companion and the `fabric sync` commands report
//! about sync to each other: entry status, runtime ownership, and the files of
//! a publish. They travel on the local control socket and on the bridge.

use serde::{Deserialize, Serialize};

/// One file of a `SyncPublish` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPublishFile {
    /// The path inside the synced folder, in manifest form.
    pub rel: String,
    pub bytes: Vec<u8>,
    #[serde(default)]
    pub executable: bool,
    /// The hex content hash of the published file when this was staged, or
    /// `None` when there was no published file. The daemon refuses to publish
    /// over a file that moved since, unless forced.
    #[serde(default)]
    pub base: Option<String>,
}

/// One file of a `SyncPublished` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPublishedFile {
    pub rel: String,
    pub version: u64,
    pub hash: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRuntimeStatus {
    /// `embedded`, `companion`, or `unavailable`.
    pub owner: String,
    /// `standby`, `active`, `absent`, `incompatible`, `timed-out`, `lease-busy`,
    /// or `unknown`.
    pub companion: String,
}

impl SyncRuntimeStatus {
    pub fn new(owner: &str, companion: &str) -> Self {
        Self {
            owner: owner.to_string(),
            companion: companion.to_string(),
        }
    }

    pub fn unavailable(reason: &str) -> Self {
        Self::new("unavailable", reason)
    }
}

/// One configured sync entry's status, for `fabric sync ls`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncEntryStatus {
    pub name: String,
    pub folder: String,
    pub policy: String,
    pub peers: String,
    /// Legacy logical-Present count retained on the local control wire so an
    /// older client can still decode a newer daemon response.
    pub files: usize,
    #[serde(default)]
    pub present: usize,
    #[serde(default)]
    pub tombstones: usize,
    #[serde(default)]
    pub observed: usize,
    #[serde(default)]
    pub missing: usize,
    #[serde(default)]
    pub unexpected: usize,
    #[serde(default)]
    pub mismatched: usize,
    /// Existing paths the last scan could not read as syncable files.
    #[serde(default)]
    pub scan_issues: Vec<(String, String)>,
    /// Monotonic full-folder scan attempts for this entry instance.
    #[serde(default)]
    pub full_scans: u64,
    /// Monotonic exact-manifest, complete-content inbound fast paths.
    #[serde(default)]
    pub inbound_noop_transactions: u64,
    /// Monotonic inbound transactions that selected guarded reconciliation.
    #[serde(default)]
    pub inbound_guarded_transactions: u64,
    /// Calls to `sync_once`, and the only correct denominator for a per-pass
    /// cost. NOT `full_scans`: `scan_entry` also runs for inbound transactions,
    /// so no constant converts `full_scans` into a pass count.
    #[serde(default)]
    pub sync_passes: u64,
    /// Cumulative microseconds inside each phase of `sync_once`. Two samples
    /// and a division describe the present; a total describes the past.
    #[serde(default)]
    pub scan_micros: u64,
    #[serde(default)]
    pub materialize_micros: u64,
    #[serde(default)]
    pub persist_micros: u64,
    #[serde(default)]
    pub reconcile_micros: u64,
    /// Every byte this entry put on or took off the wire, cumulative, INCLUDING
    /// the manifest shipped on every pass. Counted client-side, so summing
    /// across the fleet counts each transfer once.
    #[serde(default)]
    pub reconcile_wire_bytes: u64,
    /// Peer reconciles that returned an error, cumulative. A number that MOVES
    /// between two samples is a fault happening now; a large total on an old
    /// daemon may be history.
    #[serde(default)]
    pub reconcile_failures: u64,
    /// Why the tombstone sweep did or did not forget anything, as a short
    /// stable token. Empty from a daemon that predates the field, which is why
    /// it carries `#[serde(default)]` like the counters above.
    #[serde(default)]
    pub sweep: String,
    /// Peers this entry is NOT syncing with, and why, as `peer:reason`.
    ///
    /// `away` is an expected roaming-peer absence. `denied` means a person must
    /// edit `peers.toml`; `unreachable` means the network will fix itself.
    #[serde(default)]
    pub stopped_peers: Vec<(String, String)>,
    /// Payloads this node SENT carrying its whole manifest, whatever the
    /// reason: first contact, a peer too old for deltas, a restart, or a cursor
    /// that stalled until its delta grew back to the whole manifest.
    ///
    /// Read it BESIDE `reconcile_wire_bytes`, which is counted on the initiator
    /// and includes the responder's reply. High bytes with a low count here
    /// means this machine is RECEIVING full payloads, not sending them.
    #[serde(default)]
    pub full_payload_sends: u64,
    /// Bytes of file content the daemon holds in memory for this entry. It was
    /// unbounded once, and this is the number that would have said so.
    #[serde(default)]
    pub content_bytes: u64,
    /// Reconciles that fell back to full state because a payload was
    /// incomplete. Zero is healthy. A number that RISES between two samples is a
    /// bug report: a cursor described state a peer did not hold.
    #[serde(default)]
    pub delta_fallbacks: u64,
    /// Lattice-point fingerprint of this entry's manifest. Empty from a daemon
    /// that predates the field. Compare it ACROSS peers: equal means converged,
    /// unequal means diverged. Counts cannot tell you this.
    #[serde(default)]
    pub digest: String,
}
