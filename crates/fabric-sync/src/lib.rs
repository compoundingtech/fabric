//! The fabric file-sync engine and its supervised companion process.
//!
//! The daemon in the `fabric` crate authorizes and forwards sync streams and
//! reports status; everything that decides what to sync lives here: the
//! manifest algebra, a node's durable state, the wire sessions, the engine
//! that watches folders and drives passes, and the companion runtime that
//! hosts the engine behind the local bridge.
//!
//! This crate does not depend on the daemon's. It reaches the base network
//! through `fabric-service-api` (the refusal it may be told) and the daemon's
//! files and sockets through `fabric-config`; only its tests start a daemon.
//!
//! Layers:
//! - [`manifest`]: the pure reconciliation core (versioned per-file state,
//!   merge, diff), deterministic and heavily property-tested, no I/O.
//! - [`delta`]: what changed here and which peer has seen it, so a pass can
//!   ship the changed paths instead of the whole manifest.
//! - [`node`]: one node's state for one entry: its manifest plus content.
//! - [`wire`]: the `fabric/sync/1` session over any byte stream.
//! - [`engine`]: folder watchers, scans, materialization, persistence, passes.
//! - [`paths`]: the state paths and the exclusive owner lease.
//! - [`transport`]: the engine's transport when it runs behind the bridge.
//! - [`companion`]: the runtime `fabric-sync` runs, and its in-process form.

pub mod companion;
pub mod delta;
pub mod engine;
pub mod manifest;
pub mod node;
pub mod paths;
pub mod transport;
pub mod wire;

pub use companion::{CompanionHandle, CompanionPhase};
pub use delta::{ChangeBuffer, Cursor};
pub use engine::{PeerSyncState, SYNC_LOG_TARGET, SyncEngine, SyncStatus, SyncTransport};
pub use fabric_config::sync::{
    ContentHash, PeerRef, PolicyRules, ResolvedPeers, SyncBook, SyncEntry, SyncPeers, SyncPolicy,
    content_hash,
};
pub use manifest::{Author, FileMeta, Manifest, ManifestDiff};
pub use node::{Reconciled, SyncNode};
pub use paths::{SyncOwnerLease, SyncOwnerLeaseState, SyncPaths};
pub use transport::IpcSyncTransport;

impl From<SyncStatus> for fabric_config::sync::status::SyncEntryStatus {
    fn from(status: SyncStatus) -> Self {
        let peers = match &status.peers {
            SyncPeers::Wildcard(_) => "*".to_string(),
            SyncPeers::List(list) => list.join(","),
        };
        fabric_config::sync::status::SyncEntryStatus {
            delta_fallbacks: status.delta_fallbacks,
            full_payload_sends: status.full_payload_sends,
            content_bytes: status.content_bytes,
            stopped_peers: status.stopped_peers,
            digest: status.digest,
            name: status.name,
            folder: status.folder.display().to_string(),
            policy: status.policy.to_string(),
            peers,
            files: status.present,
            present: status.present,
            tombstones: status.tombstones,
            observed: status.observed,
            missing: status.missing,
            unexpected: status.unexpected,
            mismatched: status.mismatched,
            scan_issues: status.scan_issues,
            full_scans: status.full_scans,
            inbound_noop_transactions: status.inbound_noop_transactions,
            inbound_guarded_transactions: status.inbound_guarded_transactions,
            sync_passes: status.sync_passes,
            scan_micros: status.scan_micros,
            materialize_micros: status.materialize_micros,
            persist_micros: status.persist_micros,
            reconcile_micros: status.reconcile_micros,
            reconcile_wire_bytes: status.reconcile_wire_bytes,
            reconcile_failures: status.reconcile_failures,
            sweep: status
                .sweep
                .as_ref()
                .map(|state| state.token())
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The core writes an unavailable reply with an empty manifest it cannot
    /// build; pin its bytes to what this crate's `Manifest::new()` serializes.
    #[test]
    fn the_core_empty_manifest_matches_the_engine_empty_manifest() {
        let core = serde_json::to_value(fabric_config::sync::frame::EmptyManifest {
            entries: Default::default(),
        })
        .unwrap();
        let engine = serde_json::to_value(Manifest::new()).unwrap();
        assert_eq!(core, engine);
    }
}
