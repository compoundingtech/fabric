//! Declarative sync configuration: the authoritative `syncs.toml` file.
//!
//! `syncs.toml` is to file sync what `peers.toml` is to trust: a hand-editable,
//! provisionable, reload-able list of `[[sync]]` entries. Each entry declares a
//! local `folder` to keep converged with a set of `peers` under a named
//! `policy`. A tool or a human just adds an entry; the running fabric daemon
//! reads the file and continuously ensures each sync happens.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::config::FabricHome;

/// Named policy preset for a sync entry.
///
/// A preset expands (via [`SyncPolicy::rules`]) into the explicit behavioural
/// knobs the engine enforces, so custom policies can be added later without
/// changing the on-disk `policy = "..."` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    /// Union + newer-wins + NEVER delete on a peer + no sweep + no tombstones.
    ///
    /// Safe for a job catalog: a file present on any peer is present on all
    /// peers, and nothing ever removes a file. Decommissioning is expressed as
    /// an edit (e.g. `retired = true`), never a file deletion.
    Catalog,
    /// Union + newer-wins + no delete + tombstone sweep. Reserved for the
    /// smalltalk bus; not yet fully implemented (see [`PolicyRules`]).
    Bus,
}

/// The explicit behavioural rules a [`SyncPolicy`] preset expands to.
///
/// Merge is always union and conflict resolution is always newer-wins (by
/// logical version with a deterministic author tie-break); those are fixed
/// across current presets, so only the delete/sweep axes vary here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyRules {
    /// Whether a local deletion propagates to peers (catalog: `false`).
    pub propagate_deletes: bool,
    /// Whether tombstones are swept after their TTL (catalog: `false`).
    pub sweep_tombstones: bool,
}

impl SyncPolicy {
    pub fn rules(self) -> PolicyRules {
        match self {
            SyncPolicy::Catalog => PolicyRules {
                propagate_deletes: false,
                sweep_tombstones: false,
            },
            SyncPolicy::Bus => PolicyRules {
                propagate_deletes: true,
                sweep_tombstones: true,
            },
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SyncPolicy::Catalog => "catalog",
            SyncPolicy::Bus => "bus",
        }
    }
}

/// The peer set an entry syncs with.
///
/// `"*"` means every peer in the local `peers.toml` allow-list (membership
/// follows trust: a newly trusted machine is included automatically). A list
/// names specific peers by their `peers.toml` name or 64-hex NodeID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SyncPeers {
    /// The literal string `"*"`.
    Wildcard(String),
    /// An explicit list of peer names or NodeIDs.
    List(Vec<String>),
}

impl SyncPeers {
    /// True when this entry targets every trusted peer.
    pub fn is_wildcard(&self) -> bool {
        matches!(self, SyncPeers::Wildcard(_))
    }

    /// The explicit peer selectors, or an empty slice for the wildcard.
    pub fn selectors(&self) -> &[String] {
        match self {
            SyncPeers::Wildcard(_) => &[],
            SyncPeers::List(list) => list,
        }
    }
}

/// One declared sync: keep `folder` converged with `peers` under `policy`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncEntry {
    /// Shared logical key. Two machines are the *same* sync when they use the
    /// same `name`; their local `folder` paths may differ.
    pub name: String,
    /// Absolute path to the local folder to keep synced.
    pub folder: PathBuf,
    /// Which peers to sync with (`"*"` or a list).
    pub peers: SyncPeers,
    /// The merge/delete policy preset.
    pub policy: SyncPolicy,
    /// Optional include globs. When present, only files whose relative path
    /// matches at least one glob are synced; absent means sync every file. Globs
    /// use [`crate::sync::glob`] semantics (`*`, `**`, `?`). Example:
    /// `include = ["*.toml"]` to sync only TOML files in a flat catalog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
}

impl SyncEntry {
    /// Whether a file at this normalized relative path is in scope for this
    /// entry. With no `include` globs every file is in scope.
    pub fn includes(&self, rel_path: &str) -> bool {
        match &self.include {
            None => true,
            Some(globs) => super::glob::matches_any(globs, rel_path),
        }
    }
}

/// The parsed contents of `syncs.toml`: an ordered list of `[[sync]]` entries.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SyncBook {
    #[serde(default, rename = "sync", skip_serializing_if = "Vec::is_empty")]
    entries: Vec<SyncEntry>,
}

impl SyncBook {
    /// Load and validate `syncs.toml`. A missing file is an empty book.
    pub fn load(home: &FabricHome) -> Result<Self> {
        let path = home.syncs_path();
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let book: Self =
            toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
        book.validate()?;
        Ok(book)
    }

    /// Validate and write `syncs.toml`.
    pub fn save(&self, home: &FabricHome) -> Result<()> {
        self.validate()?;
        home.prepare()?;
        let path = home.syncs_path();
        let raw = toml::to_string_pretty(self)?;
        fs::write(&path, raw).with_context(|| format!("failed to write {}", path.display()))
    }

    pub fn entries(&self) -> &[SyncEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Find an entry by name.
    pub fn get(&self, name: &str) -> Option<&SyncEntry> {
        self.entries.iter().find(|entry| entry.name == name)
    }

    /// Insert or replace the entry with this name, keeping entries name-sorted.
    pub fn upsert(&mut self, entry: SyncEntry) {
        self.entries.retain(|existing| existing.name != entry.name);
        self.entries.push(entry);
        self.entries.sort_by(|a, b| a.name.cmp(&b.name));
    }

    /// Remove an entry by name or by exact folder path. Returns whether one was
    /// removed.
    pub fn remove(&mut self, name_or_folder: &str) -> bool {
        let before = self.entries.len();
        let as_path = Path::new(name_or_folder);
        self.entries
            .retain(|entry| entry.name != name_or_folder && entry.folder != as_path);
        self.entries.len() != before
    }

    /// Enforce the invariants a well-formed `syncs.toml` must hold.
    pub fn validate(&self) -> Result<()> {
        let mut names = HashSet::new();
        let mut folders = HashSet::new();
        for entry in &self.entries {
            if entry.name.trim().is_empty() {
                bail!("sync name cannot be empty");
            }
            if !names.insert(entry.name.as_str()) {
                bail!("duplicate sync name {:?}", entry.name);
            }
            if !entry.folder.is_absolute() {
                bail!(
                    "sync {:?} folder must be an absolute path, got {}",
                    entry.name,
                    entry.folder.display()
                );
            }
            if !folders.insert(entry.folder.clone()) {
                bail!(
                    "duplicate sync folder {} (used by more than one entry)",
                    entry.folder.display()
                );
            }
            validate_peers(&entry.name, &entry.peers)?;
            if let Some(globs) = &entry.include {
                if globs.is_empty() {
                    bail!(
                        "sync {:?} include list cannot be empty; omit it to sync all files",
                        entry.name
                    );
                }
                if globs.iter().any(|glob| glob.trim().is_empty()) {
                    bail!("sync {:?} has an empty include glob", entry.name);
                }
            }
        }
        Ok(())
    }
}

fn validate_peers(name: &str, peers: &SyncPeers) -> Result<()> {
    match peers {
        SyncPeers::Wildcard(literal) => {
            if literal != "*" {
                bail!(
                    "sync {name:?} peers must be \"*\" or a list of peers; \
                     a single peer is written as [\"{literal}\"]"
                );
            }
        }
        SyncPeers::List(list) => {
            if list.is_empty() {
                bail!("sync {name:?} peers list cannot be empty; use \"*\" for all peers");
            }
            let mut seen = HashSet::new();
            for peer in list {
                if peer.trim().is_empty() {
                    bail!("sync {name:?} has an empty peer selector");
                }
                if !seen.insert(peer.as_str()) {
                    bail!("sync {name:?} lists peer {peer:?} more than once");
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml: &str) -> Result<SyncBook> {
        let book: SyncBook = toml::from_str(toml)?;
        book.validate()?;
        Ok(book)
    }

    #[test]
    fn parses_catalog_entry_with_wildcard_peers() {
        let book = parse(
            r#"
            [[sync]]
            name   = "convoy-catalog"
            folder = "/srv/convoy/catalog"
            peers  = "*"
            policy = "catalog"
            "#,
        )
        .unwrap();

        assert_eq!(book.entries().len(), 1);
        let entry = &book.entries()[0];
        assert_eq!(entry.name, "convoy-catalog");
        assert_eq!(entry.folder, PathBuf::from("/srv/convoy/catalog"));
        assert!(entry.peers.is_wildcard());
        assert_eq!(entry.policy, SyncPolicy::Catalog);
        assert_eq!(
            entry.policy.rules(),
            PolicyRules {
                propagate_deletes: false,
                sweep_tombstones: false,
            }
        );
    }

    #[test]
    fn parses_explicit_peer_list() {
        let book = parse(
            r#"
            [[sync]]
            name   = "pair"
            folder = "/data/pair"
            peers  = ["workstation", "hetzner"]
            policy = "bus"
            "#,
        )
        .unwrap();

        let entry = &book.entries()[0];
        assert!(!entry.peers.is_wildcard());
        assert_eq!(entry.peers.selectors(), &["workstation", "hetzner"]);
        assert_eq!(entry.policy, SyncPolicy::Bus);
        assert!(entry.policy.rules().propagate_deletes);
        assert!(entry.policy.rules().sweep_tombstones);
    }

    #[test]
    fn missing_file_is_empty_book() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let book = SyncBook::load(&home).unwrap();
        assert!(book.is_empty());
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let mut book = SyncBook::default();
        book.upsert(SyncEntry {
            name: "catalog".into(),
            folder: PathBuf::from("/srv/catalog"),
            peers: SyncPeers::Wildcard("*".into()),
            policy: SyncPolicy::Catalog,
            include: None,
        });
        book.save(&home).unwrap();

        let reloaded = SyncBook::load(&home).unwrap();
        assert_eq!(reloaded.entries(), book.entries());
        assert_eq!(home.syncs_path(), dir.path().join("syncs.toml"));
    }

    #[test]
    fn upsert_replaces_same_name_and_sorts() {
        let mut book = SyncBook::default();
        book.upsert(SyncEntry {
            name: "b".into(),
            folder: "/b".into(),
            peers: SyncPeers::Wildcard("*".into()),
            policy: SyncPolicy::Catalog,
            include: None,
        });
        book.upsert(SyncEntry {
            name: "a".into(),
            folder: "/a".into(),
            peers: SyncPeers::Wildcard("*".into()),
            policy: SyncPolicy::Catalog,
            include: None,
        });
        book.upsert(SyncEntry {
            name: "b".into(),
            folder: "/b2".into(),
            peers: SyncPeers::Wildcard("*".into()),
            policy: SyncPolicy::Bus,
            include: None,
        });

        assert_eq!(book.entries().len(), 2);
        assert_eq!(book.entries()[0].name, "a");
        assert_eq!(book.entries()[1].name, "b");
        assert_eq!(book.entries()[1].folder, PathBuf::from("/b2"));
        assert_eq!(book.entries()[1].policy, SyncPolicy::Bus);
    }

    #[test]
    fn remove_by_name_or_folder() {
        let mut book = SyncBook::default();
        book.upsert(SyncEntry {
            name: "one".into(),
            folder: "/one".into(),
            peers: SyncPeers::Wildcard("*".into()),
            policy: SyncPolicy::Catalog,
            include: None,
        });
        assert!(book.remove("/one"));
        assert!(book.is_empty());
    }

    #[test]
    fn rejects_relative_folder() {
        let error = parse(
            r#"
            [[sync]]
            name   = "rel"
            folder = "relative/path"
            peers  = "*"
            policy = "catalog"
            "#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("must be an absolute path"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn rejects_duplicate_names() {
        let error = parse(
            r#"
            [[sync]]
            name = "dup"
            folder = "/a"
            peers = "*"
            policy = "catalog"

            [[sync]]
            name = "dup"
            folder = "/b"
            peers = "*"
            policy = "catalog"
            "#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("duplicate sync name"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn rejects_duplicate_folders() {
        let error = parse(
            r#"
            [[sync]]
            name = "one"
            folder = "/same"
            peers = "*"
            policy = "catalog"

            [[sync]]
            name = "two"
            folder = "/same"
            peers = "*"
            policy = "catalog"
            "#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("duplicate sync folder"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn rejects_bare_string_peer_that_is_not_wildcard() {
        let error = parse(
            r#"
            [[sync]]
            name = "oops"
            folder = "/a"
            peers = "workstation"
            policy = "catalog"
            "#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("peers must be"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn rejects_empty_peer_list() {
        let error = parse(
            r#"
            [[sync]]
            name = "empty"
            folder = "/a"
            peers = []
            policy = "catalog"
            "#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("peers list cannot be empty"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn rejects_duplicate_peer_in_list() {
        let error = parse(
            r#"
            [[sync]]
            name = "duppeer"
            folder = "/a"
            peers = ["x", "x"]
            policy = "catalog"
            "#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("more than once"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn parses_and_applies_include_globs() {
        let book = parse(
            r#"
            [[sync]]
            name    = "toml-only"
            folder  = "/srv/catalog"
            peers   = "*"
            policy  = "catalog"
            include = ["*.toml"]
            "#,
        )
        .unwrap();
        let entry = &book.entries()[0];
        assert_eq!(entry.include.as_deref(), Some(&["*.toml".to_string()][..]));
        assert!(entry.includes("agent.toml"));
        assert!(!entry.includes("notes.md"));
    }

    #[test]
    fn absent_include_syncs_everything() {
        let entry = SyncEntry {
            name: "all".into(),
            folder: "/srv/all".into(),
            peers: SyncPeers::Wildcard("*".into()),
            policy: SyncPolicy::Catalog,
            include: None,
        };
        assert!(entry.includes("anything.bin"));
        assert!(entry.includes("nested/thing.txt"));
    }

    #[test]
    fn rejects_empty_include_list() {
        let error = parse(
            r#"
            [[sync]]
            name    = "emptyinc"
            folder  = "/a"
            peers   = "*"
            policy  = "catalog"
            include = []
            "#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("include list cannot be empty"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn round_trips_include_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let mut book = SyncBook::default();
        book.upsert(SyncEntry {
            name: "toml-only".into(),
            folder: "/srv/catalog".into(),
            peers: SyncPeers::Wildcard("*".into()),
            policy: SyncPolicy::Catalog,
            include: Some(vec!["*.toml".into()]),
        });
        book.save(&home).unwrap();
        let reloaded = SyncBook::load(&home).unwrap();
        assert_eq!(reloaded.entries(), book.entries());
    }

    #[test]
    fn rejects_unknown_policy() {
        let error = parse(
            r#"
            [[sync]]
            name = "bad"
            folder = "/a"
            peers = "*"
            policy = "mirror"
            "#,
        )
        .unwrap_err();
        // serde rejects the unknown variant during parse.
        assert!(
            format!("{error:#}").to_lowercase().contains("policy")
                || format!("{error:#}").contains("unknown variant"),
            "unexpected error: {error:#}"
        );
    }
}
