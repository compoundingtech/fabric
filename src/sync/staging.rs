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

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::config::FabricHome;

use super::{
    config::{SyncBook, SyncEntry},
    engine::{sanitize_name, write_atomic_with_mode},
    manifest::{ContentHash, Manifest},
    node::content_hash,
};

/// The directory under the fabric home that holds every staged file.
pub const STAGING_DIR: &str = "staging";
/// The per-entry record of what each staged file was staged against.
const SIDECAR_NAME: &str = "staged.json";
const TEMP_SUFFIX: &str = ".fabric-tmp";

/// Where every staged file of every entry lives.
pub fn staging_root(home: &FabricHome) -> PathBuf {
    home.root().join(STAGING_DIR)
}

/// Where one entry's staged files live. Two entry names that sanitize to the
/// same directory name would share a tree; `syncs.toml` names are chosen by a
/// person and the sanitizer keeps letters, digits, `-` and `_`, so that is a
/// naming collision to notice, not a case to handle here.
pub fn entry_staging_dir(home: &FabricHome, entry: &str) -> PathBuf {
    staging_root(home).join(sanitize_name(entry))
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct Sidecar {
    #[serde(default)]
    files: BTreeMap<String, SidecarRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SidecarRecord {
    base: Option<String>,
    staged_at: i64,
}

fn load_sidecar(dir: &Path) -> Result<Sidecar> {
    let path = dir.join(SIDECAR_NAME);
    if !path.exists() {
        return Ok(Sidecar::default());
    }
    let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

fn save_sidecar(dir: &Path, sidecar: &Sidecar) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let raw = serde_json::to_vec_pretty(sidecar)?;
    write_atomic_with_mode(&dir.join(SIDECAR_NAME), &raw, false)
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

fn is_executable(meta: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        false
    }
}

/// The bytes and executable bit of the regular file at `path`, `None` when
/// nothing is there, and an error for anything else there. Symlinks are not
/// followed: fabric does not sync them, so it does not stage them either.
fn read_regular(path: &Path) -> Result<Option<(Vec<u8>, bool)>> {
    let meta = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to stat {}", path.display()));
        }
    };
    if !meta.is_file() {
        bail!("{} is not a regular file", path.display());
    }
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(Some((bytes, is_executable(&meta))))
}

/// `target` relative to `folder` in manifest form, by the configured spelling
/// of the folder or its canonical one. `None` when `target` is not inside it,
/// or is the folder itself.
fn rel_inside(folder: &Path, target: &Path) -> Option<String> {
    let rel = target.strip_prefix(folder).ok().or_else(|| {
        let canonical = folder.canonicalize().ok()?;
        target.strip_prefix(canonical).ok()
    })?;
    Manifest::normalize_path(&rel.to_string_lossy())
}

/// Resolve `target` to exactly one entry, by folder and then by include.
pub fn resolve_target(
    book: &SyncBook,
    target: &Path,
    entry_hint: Option<&str>,
) -> Result<ResolvedTarget> {
    if !target.is_absolute() {
        bail!("the target must be an absolute path, got {}", target.display());
    }
    let inside: Vec<(&SyncEntry, String)> = book
        .entries()
        .iter()
        .filter_map(|entry| rel_inside(&entry.folder, target).map(|rel| (entry, rel)))
        .collect();
    if inside.is_empty() {
        let folders = book
            .entries()
            .iter()
            .map(|entry| format!("{} ({})", entry.folder.display(), entry.name))
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "{} is not inside any synced folder; configured folders: {}",
            target.display(),
            if folders.is_empty() {
                "none".to_string()
            } else {
                folders
            }
        );
    }
    let resolved = |entry: &SyncEntry, rel: &str| ResolvedTarget {
        entry: entry.clone(),
        rel: rel.to_string(),
        target: entry.folder.join(rel),
    };
    let not_included = |entry: &SyncEntry, rel: &str| {
        format!(
            "no include glob of sync {:?} matches {rel}; its include list is {:?}, so publishing \
             would replicate nothing",
            entry.name,
            entry.include.clone().unwrap_or_default()
        )
    };
    if let Some(hint) = entry_hint {
        let Some((entry, rel)) = inside.iter().find(|(entry, _)| entry.name == hint) else {
            bail!(
                "{} is not inside the folder of sync {hint:?}",
                target.display()
            );
        };
        if !entry.includes(rel) {
            bail!("{}", not_included(entry, rel));
        }
        return Ok(resolved(entry, rel));
    }
    let included: Vec<&(&SyncEntry, String)> = inside
        .iter()
        .filter(|(entry, rel)| entry.includes(rel))
        .collect();
    match included.as_slice() {
        [] => {
            let detail = inside
                .iter()
                .map(|(entry, rel)| not_included(entry, rel))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("{detail}")
        }
        [(entry, rel)] => Ok(resolved(entry, rel)),
        many => {
            let names = many
                .iter()
                .map(|(entry, _)| entry.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "{} belongs to more than one sync ({names}); pass --entry <name>",
                target.display()
            )
        }
    }
}

/// Refuse when the staging tree lies inside any synced folder. A file staged
/// there would publish, which is the one thing staging exists to prevent.
pub fn ensure_staging_outside_every_folder(home: &FabricHome, book: &SyncBook) -> Result<()> {
    let staging = staging_root(home);
    for entry in book.entries() {
        if staging == entry.folder || rel_inside(&entry.folder, &staging).is_some() {
            bail!(
                "the staging tree {} lies inside the synced folder {} of sync {:?}; a file staged \
                 there would publish, so nothing was staged",
                staging.display(),
                entry.folder.display(),
                entry.name
            );
        }
    }
    Ok(())
}

fn describe(
    home: &FabricHome,
    entry: &SyncEntry,
    rel: &str,
    record: Option<&SidecarRecord>,
) -> Result<StagedFile> {
    let staged_path = entry_staging_dir(home, &entry.name).join(rel);
    let Some((bytes, executable)) = read_regular(&staged_path)? else {
        bail!("{rel} is not staged for sync {:?}", entry.name);
    };
    let target_path = entry.folder.join(rel);
    let published_now =
        read_regular(&target_path)?.map(|(published, _)| content_hash(&published).to_hex());
    Ok(StagedFile {
        entry: entry.name.clone(),
        rel: rel.to_string(),
        staged_path,
        target_path,
        bytes: bytes.len() as u64,
        hash: content_hash(&bytes).to_hex(),
        executable,
        base: record.and_then(|record| record.base.clone()),
        staged_at: record.map(|record| record.staged_at).unwrap_or(0),
        published_now,
    })
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
    ensure_staging_outside_every_folder(home, book)?;
    let resolved = resolve_target(book, target, entry_hint)?;
    let published = read_regular(&resolved.target)?;
    let (bytes, executable) = match from {
        Some(from) => {
            let Some(source) = read_regular(from)? else {
                bail!("{} does not exist", from.display());
            };
            source
        }
        None => published.clone().unwrap_or_default(),
    };
    let base = published.as_ref().map(|(bytes, _)| content_hash(bytes));

    let dir = entry_staging_dir(home, &resolved.entry.name);
    let staged_path = dir.join(&resolved.rel);
    if let Some(parent) = staged_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    write_atomic_with_mode(&staged_path, &bytes, executable)?;
    let mut sidecar = load_sidecar(&dir)?;
    sidecar.files.insert(
        resolved.rel.clone(),
        SidecarRecord {
            base: base.map(ContentHash::to_hex),
            staged_at: now_secs(),
        },
    );
    save_sidecar(&dir, &sidecar)?;
    describe(
        home,
        &resolved.entry,
        &resolved.rel,
        sidecar.files.get(&resolved.rel),
    )
}

/// Every regular file under `dir`, in manifest form, minus the sidecar and any
/// temp file an interrupted write left behind.
fn staged_rels(dir: &Path) -> Result<BTreeSet<String>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) -> Result<()> {
        for child in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
            let child = child?;
            let path = child.path();
            let file_type = child.file_type()?;
            if file_type.is_dir() {
                walk(root, &path, out)?;
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Ok(rel) = path.strip_prefix(root) else {
                continue;
            };
            let Some(norm) = Manifest::normalize_path(&rel.to_string_lossy()) else {
                continue;
            };
            if norm == SIDECAR_NAME || norm.ends_with(TEMP_SUFFIX) {
                continue;
            }
            out.insert(norm);
        }
        Ok(())
    }
    let mut out = BTreeSet::new();
    if dir.is_dir() {
        walk(dir, dir, &mut out)?;
    }
    Ok(out)
}

/// Every staged file, for one entry or for all. A file dropped into the tree
/// by hand, with no record, is listed with no base, so it reads as stale
/// against any published file and publishes only when forced.
pub fn list(home: &FabricHome, book: &SyncBook, entry: Option<&str>) -> Result<Vec<StagedFile>> {
    let mut out = Vec::new();
    for configured in book.entries() {
        if entry.is_some_and(|wanted| wanted != configured.name) {
            continue;
        }
        let dir = entry_staging_dir(home, &configured.name);
        let sidecar = load_sidecar(&dir)?;
        for rel in staged_rels(&dir)? {
            out.push(describe(home, configured, &rel, sidecar.files.get(&rel))?);
        }
    }
    Ok(out)
}

/// Read the staged bytes of `rels` in `entry`, or of every staged file of the
/// entry when `rels` is empty. Refuses a path the include no longer matches
/// and a target that is now a directory, before anything is written.
pub fn read_for_publish(
    home: &FabricHome,
    book: &SyncBook,
    entry: &str,
    rels: &[String],
) -> Result<(SyncEntry, Vec<PublishFile>)> {
    let Some(configured) = book.get(entry) else {
        bail!("no sync entry named {entry:?}");
    };
    let dir = entry_staging_dir(home, entry);
    let sidecar = load_sidecar(&dir)?;
    let rels: Vec<String> = if rels.is_empty() {
        staged_rels(&dir)?.into_iter().collect()
    } else {
        rels.to_vec()
    };
    if rels.is_empty() {
        bail!("nothing is staged for sync {entry:?}");
    }
    let mut files = Vec::with_capacity(rels.len());
    for rel in rels {
        if !configured.includes(&rel) {
            bail!(
                "no include glob of sync {entry:?} matches {rel}; publishing would replicate nothing"
            );
        }
        let Some((bytes, executable)) = read_regular(&dir.join(&rel))? else {
            bail!("{rel} is not staged for sync {entry:?}");
        };
        let target = configured.folder.join(&rel);
        if target.is_dir() {
            bail!(
                "{} is a directory; a staged file cannot replace it",
                target.display()
            );
        }
        let base = match sidecar.files.get(&rel).and_then(|record| record.base.as_deref()) {
            Some(hex) => Some(
                ContentHash::from_hex(hex)
                    .with_context(|| format!("{rel}: the recorded base is not a content hash"))?,
            ),
            None => None,
        };
        files.push(PublishFile {
            rel,
            bytes,
            executable,
            base,
        });
    }
    Ok((configured.clone(), files))
}

/// Forget staged copies after they were published or discarded. Empty
/// directories go with them, and an empty entry tree goes entirely.
pub fn forget(home: &FabricHome, entry: &str, rels: &[String]) -> Result<()> {
    let dir = entry_staging_dir(home, entry);
    let mut sidecar = load_sidecar(&dir)?;
    for rel in rels {
        let path = dir.join(rel);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("failed to remove {}", path.display()));
            }
        }
        sidecar.files.remove(rel);
        let mut parent = path.parent();
        while let Some(current) = parent {
            if current == dir || fs::remove_dir(current).is_err() {
                break;
            }
            parent = current.parent();
        }
    }
    if staged_rels(&dir)?.is_empty() {
        let _ = fs::remove_dir_all(&dir);
        return Ok(());
    }
    save_sidecar(&dir, &sidecar)
}

fn hex_or_absent(hash: Option<ContentHash>) -> String {
    hash.map(ContentHash::to_hex)
        .unwrap_or_else(|| "no file".to_string())
}

/// The one line a refused publish prints for one file.
pub fn refusal_line(rel: &str, base: Option<ContentHash>, current: Option<ContentHash>) -> String {
    format!(
        "{rel}: the published file changed since it was staged (staged against {}, now {}); \
         re-stage it or pass --force",
        hex_or_absent(base),
        hex_or_absent(current)
    )
}

/// Publish without a daemon: check every base against the folder, then write
/// each file atomically into it. The daemon's next scan records them, one at a
/// time if its watcher splits them.
pub fn publish_locally(
    entry: &SyncEntry,
    files: &[PublishFile],
    force: bool,
) -> Result<Vec<(String, ContentHash)>> {
    let mut refusals = Vec::new();
    for file in files {
        let current =
            read_regular(&entry.folder.join(&file.rel))?.map(|(bytes, _)| content_hash(&bytes));
        if current != file.base && !force {
            refusals.push(refusal_line(&file.rel, file.base, current));
        }
    }
    if !refusals.is_empty() {
        bail!("publish refused:\n{}", refusals.join("\n"));
    }
    let mut out = Vec::with_capacity(files.len());
    for file in files {
        let target = entry.folder.join(&file.rel);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        write_atomic_with_mode(&target, &file.bytes, file.executable)?;
        out.push((file.rel.clone(), content_hash(&file.bytes)));
    }
    Ok(out)
}
