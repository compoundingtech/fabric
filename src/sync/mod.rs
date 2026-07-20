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

pub mod config;
pub mod engine;
pub mod glob;
pub mod manifest;
pub mod node;
pub mod wire;

pub use config::{PolicyRules, SyncBook, SyncEntry, SyncPeers, SyncPolicy};
pub use engine::{PeerRef, SyncEngine, SyncStatus, SyncTransport};
pub use manifest::{FileMeta, Manifest, ManifestDiff};
pub use node::{Reconciled, SyncNode, content_hash};
