//! Stage a change beside a synced folder and publish it on purpose.
//!
//! In a synced folder the write is the publish: the moment bytes land on disk,
//! every peer receives them. There is no state in which a change exists, is
//! complete, and has not yet been distributed, so nothing can be reviewed
//! before it crosses. This module adds that state.
//!
//! A staged file lives under `<fabric home>/staging/<entry>/<rel>`, never
//! inside any synced folder. That location is the whole guarantee. A fabric
//! daemon decides what to publish from exactly two things: the folder it walks
//! and the include globs in its own `syncs.toml`. Nothing else, not a config
//! key, not engine state, not a control request, reaches an old binary's scan.
//! A path outside every folder is therefore never published by any build that
//! has shipped, which is what a fleet that rolls one machine at a time needs.
//!
//! Publishing writes the staged bytes to the target path and forgets the
//! staged copy. [`crate::sync::SyncEngine::publish_staged`] does that under the
//! entry's operation guard so a set of files is one scan, one persist, and one
//! reconcile on each peer. [`publish_locally`] is the fallback when no daemon
//! answers or the daemon predates the request.

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use serde::Serialize;

use crate::config::FabricHome;

use super::{
    config::{SyncBook, SyncEntry},
    manifest::ContentHash,
};

/// The directory under the fabric home that holds every staged file.
pub const STAGING_DIR: &str = "staging";

/// Where every staged file of every entry lives.
pub fn staging_root(home: &FabricHome) -> PathBuf {
    home.root().join(STAGING_DIR)
}

/// Where one entry's staged files live.
pub fn entry_staging_dir(home: &FabricHome, entry: &str) -> PathBuf {
    staging_root(home).join(super::engine::sanitize_name(entry))
}

/// A target path resolved to the one entry that would publish it.
#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub entry: SyncEntry,
    /// The path inside the folder, in manifest form.
    pub rel: String,
    /// The published path: the entry folder joined with `rel`.
    pub target: PathBuf,
}

/// One staged file as `fabric sync staged` reports it.
#[derive(Debug, Clone, Serialize)]
pub struct StagedFile {
    pub entry: String,
    pub rel: String,
    pub staged_path: PathBuf,
    pub target_path: PathBuf,
    pub bytes: u64,
    /// Hex content hash of the staged bytes.
    pub hash: String,
    pub executable: bool,
    /// Hex content hash of the published file when this was staged, or `None`
    /// when there was none.
    pub base: Option<String>,
    pub staged_at: i64,
    /// Hex content hash of the file at the target path now, or `None` when
    /// there is none.
    pub published_now: Option<String>,
}

impl StagedFile {
    /// True when the published file is not the one this was staged against.
    pub fn changed_since_staging(&self) -> bool {
        self.base != self.published_now
    }

    /// `new` for a path with no published file, `edit` for a staged change to
    /// a published file that has not moved, `stale` when it has.
    pub fn state(&self) -> &'static str {
        match (self.base.is_some(), self.changed_since_staging()) {
            (_, true) => "stale",
            (false, false) => "new",
            (true, false) => "edit",
        }
    }
}

/// The bytes and base of one staged file, ready to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishFile {
    pub rel: String,
    pub bytes: Vec<u8>,
    pub executable: bool,
    pub base: Option<ContentHash>,
}

/// Resolve `target` to exactly one entry, by folder and then by include.
pub fn resolve_target(
    book: &SyncBook,
    target: &Path,
    entry_hint: Option<&str>,
) -> Result<ResolvedTarget> {
    let _ = (book, target, entry_hint);
    bail!("sync staging is not implemented yet")
}

/// Refuse when the staging tree lies inside any synced folder.
pub fn ensure_staging_outside_every_folder(home: &FabricHome, book: &SyncBook) -> Result<()> {
    let _ = (home, book);
    bail!("sync staging is not implemented yet")
}

/// Stage a change to `target`: seed the staged copy from `from`, else from the
/// published file, else empty, and record the published hash as the base.
pub fn stage(
    home: &FabricHome,
    book: &SyncBook,
    target: &Path,
    from: Option<&Path>,
    entry_hint: Option<&str>,
) -> Result<StagedFile> {
    let _ = (home, book, target, from, entry_hint);
    bail!("sync staging is not implemented yet")
}

/// Every staged file, for one entry or for all.
pub fn list(home: &FabricHome, book: &SyncBook, entry: Option<&str>) -> Result<Vec<StagedFile>> {
    let _ = (home, book, entry);
    bail!("sync staging is not implemented yet")
}

/// Read the staged bytes of `rels` in `entry`, or of every staged file of the
/// entry when `rels` is empty.
pub fn read_for_publish(
    home: &FabricHome,
    book: &SyncBook,
    entry: &str,
    rels: &[String],
) -> Result<(SyncEntry, Vec<PublishFile>)> {
    let _ = (home, book, entry, rels);
    bail!("sync staging is not implemented yet")
}

/// Forget staged copies after they were published or discarded.
pub fn forget(home: &FabricHome, entry: &str, rels: &[String]) -> Result<()> {
    let _ = (home, entry, rels);
    bail!("sync staging is not implemented yet")
}

/// Publish without a daemon: write each file atomically into the folder. The
/// daemon's next scan records them.
pub fn publish_locally(
    entry: &SyncEntry,
    files: &[PublishFile],
    force: bool,
) -> Result<Vec<(String, ContentHash)>> {
    let _ = (entry, files, force);
    bail!("sync staging is not implemented yet")
}
