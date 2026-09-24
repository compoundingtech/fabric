//! The `fabric sync` subcommands, defined once for both binaries.
//!
//! `fabric` parses them, so its help and its argument errors are what they
//! always were. It runs `add` and `rm` itself, because they check selectors
//! against the daemon's peer book, and hands every other one to `fabric-sync`,
//! which parses the same definition and runs it.

use clap::Subcommand;

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum SyncCommands {
    /// Add or update a sync entry in syncs.toml and reload the daemon.
    Add {
        /// Local folder to keep synced (absolute, or relative to the CWD).
        folder: String,
        /// Shared logical name — use the SAME name on every machine for this sync.
        #[arg(long)]
        name: String,
        /// Peers to sync with: "*" (all trusted) or comma-separated names/ids.
        #[arg(long, default_value = "*")]
        peers: String,
        /// Policy preset: catalog or bus.
        #[arg(long, default_value = "catalog")]
        policy: String,
        /// Optional comma-separated include globs (default: sync all files).
        #[arg(long)]
        include: Option<String>,
    },
    /// List configured sync entries and their live state.
    Ls {
        /// Emit a stable JSON array for scripts.
        #[arg(long)]
        json: bool,
    },
    /// Remove a sync entry by name or folder and reload the daemon.
    Rm { name_or_folder: String },
    /// Re-read syncs.toml into the running daemon (like reload-peers).
    Reload,
    /// Stage a change to a synced file without publishing it.
    ///
    /// The staged copy lives under the fabric home, outside every synced
    /// folder, so no daemon publishes it until `fabric sync publish`.
    Stage {
        /// The path inside a synced folder that the change is for.
        target: String,
        /// Seed the staged copy from this file instead of the published one.
        #[arg(long)]
        from: Option<String>,
        /// The sync entry, when the target lies inside more than one folder.
        #[arg(long)]
        entry: Option<String>,
    },
    /// List staged changes and whether their published file moved since.
    Staged {
        /// Only this sync entry.
        #[arg(long)]
        entry: Option<String>,
        /// Emit a JSON array for scripts.
        #[arg(long)]
        json: bool,
    },
    /// Publish staged changes into their synced folder.
    Publish {
        /// The paths inside synced folders to publish.
        targets: Vec<String>,
        /// Publish every staged file of --entry.
        #[arg(long)]
        all: bool,
        /// The sync entry, for --all or to break a tie between folders.
        #[arg(long)]
        entry: Option<String>,
        /// Publish even if the published file changed since it was staged.
        #[arg(long)]
        force: bool,
    },
    /// Remove staged changes without publishing them.
    Discard {
        /// The paths inside synced folders whose staged copies to remove.
        targets: Vec<String>,
        /// The sync entry, to break a tie between folders.
        #[arg(long)]
        entry: Option<String>,
    },
}
