//! `fabric sync` — a generic, reusable file-sync primitive.
//!
//! A config file (`syncs.toml`) lists sync *entries*; the running fabric daemon
//! reads it and continuously ensures each entry's `folder` stays converged with
//! its `peers` under its `policy`. The sync *semantics* — union merge,
//! newer-wins conflict resolution, per-policy delete handling, echo/loop
//! prevention, convergence — live here in fabric, above a swappable transport
//! backend, so the same backend-agnostic test suite pins behaviour regardless of
//! which backend moves the bytes.
//!
//! Layers:
//! - [`config`]: the declarative `syncs.toml` surface (what tools/humans edit).
//! - [`manifest`]: the pure reconciliation core (versioned per-file state, merge,
//!   diff) — deterministic and heavily property-tested, no I/O.
//! - [`delta`]: what changed here and which peer has seen it, so a pass can ship
//!   the changed paths instead of the whole manifest.

pub mod config;
pub mod delta;
pub mod engine;
pub mod glob;
pub mod ipc;
pub mod manifest;
pub mod node;
pub mod paths;
pub mod wire;

pub use config::{PolicyRules, SyncBook, SyncEntry, SyncPeers, SyncPolicy};
pub use delta::{ChangeBuffer, Cursor};
pub use engine::{PeerRef, SYNC_LOG_TARGET, SyncEngine, SyncStatus, SyncTransport};
pub use manifest::{FileMeta, Manifest, ManifestDiff};
pub use node::{Reconciled, SyncNode, content_hash};
pub use paths::{SyncOwnerLease, SyncOwnerLeaseState, SyncPaths};
