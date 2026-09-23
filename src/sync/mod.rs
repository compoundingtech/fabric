//! What the core keeps of file sync: the declarative config, the staging
//! commands, the local bridge to the `fabric-sync` process, and the few
//! definitions the engine crate shares with it.
//!
//! The engine itself, the wire sessions, the manifest algebra, the durable
//! state and the companion runtime live in the `fabric-sync` crate. The daemon
//! authorizes and forwards sync streams and reports status; it never builds
//! the engine.
//!
//! Layers here:
//! - [`config`]: the declarative `syncs.toml` surface (what tools/humans edit).
//! - [`staging`]: stage a change beside a synced folder and publish on purpose.
//! - [`ipc`]: the `fabric/sync-ipc/1` bridge, both sockets.
//! - [`frame`]: the wire framing, the idle bound, and the unavailable reply.
//! - [`model`], [`peers`]: content identity, path form, atomic write, peer refs.

pub mod config;
pub mod frame;
pub mod glob;
pub mod ipc;
pub mod model;
pub mod peers;
pub mod staging;

pub use config::{PolicyRules, SyncBook, SyncEntry, SyncPeers, SyncPolicy};
pub use frame::SYNC_UNAVAILABLE_MARKER;
pub use model::{ContentHash, content_hash, normalize_path, sanitize_name, write_atomic_with_mode};
pub use peers::{PeerRef, ResolvedPeers};
pub use staging::{PublishFile, StagedFile};
