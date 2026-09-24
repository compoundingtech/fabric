//! Where a fabric home keeps its files: identity, peers, syncs, sockets, logs.

use std::{
    collections::hash_map::DefaultHasher,
    env, fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct FabricHome {
    root: PathBuf,
    peer_config_path: PathBuf,
    legacy_peer_config_path: Option<PathBuf>,
}

impl FabricHome {
    pub fn resolve(home: Option<PathBuf>) -> Result<Self> {
        let explicit = home.or_else(|| env::var_os("FABRIC_HOME").map(PathBuf::from));
        let home_dir = env::var_os("HOME").map(PathBuf::from);
        let config_root = env::var_os("XDG_CONFIG_HOME").map(PathBuf::from);
        Self::resolve_from(explicit, home_dir.as_deref(), config_root)
    }

    /// Pure resolution (env already read) so it is unit-testable without
    /// mutating process env.
    ///
    /// An explicit root equal to the default state root
    /// (`<HOME>/.local/share/fabric`) resolves peers/config from the XDG config
    /// dir exactly like the no-argument default. This matters because the
    /// service always launches the daemon as `--home <default-root>`: without
    /// this, the daemon would read `peers.toml` from under its `--home` while
    /// the interactive CLI reads `~/.config/fabric/peers.toml`, so a `fabric add`
    /// (or a restart-triggered migration) could silently leave the daemon with
    /// zero peers — a lockout. A genuinely different `--home`/`FABRIC_HOME`
    /// keeps the isolated config-under-root layout.
    fn resolve_from(
        explicit: Option<PathBuf>,
        home_dir: Option<&Path>,
        config_root: Option<PathBuf>,
    ) -> Result<Self> {
        let default = home_dir.map(|home| Self::default_layout(home, config_root));
        match explicit {
            Some(root) => {
                if let Some(default) = default.as_ref()
                    && default.root == root
                {
                    return Ok(default.clone());
                }
                Ok(Self::new(root))
            }
            None => default.context("HOME is not set; pass --home or FABRIC_HOME"),
        }
    }

    fn default_layout(home: &Path, config_root: Option<PathBuf>) -> Self {
        let root = home.join(".local/share/fabric");
        let config_root = config_root.unwrap_or_else(|| home.join(".config"));
        Self {
            peer_config_path: config_root.join("fabric/peers.toml"),
            legacy_peer_config_path: Some(root.join("peers.toml")),
            root,
        }
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            peer_config_path: root.join("peers.toml"),
            legacy_peer_config_path: None,
            root,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The conventional default (prod) state root, `$HOME/.local/share/fabric`.
    /// `None` only if `HOME` is unset. Independent of `FABRIC_HOME` — this is the
    /// canonical prod location, not whatever a dev override points at.
    pub fn default_state_root() -> Option<PathBuf> {
        env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share/fabric"))
    }

    /// True if this home is the default prod state root (i.e. NOT a dev/custom
    /// home). The managed OS-service is prod-only, so `service install` and the
    /// mutating-op mismatch guard key off this.
    pub fn is_default_state_root(&self) -> bool {
        Self::default_state_root().is_some_and(|default| default == self.root)
    }

    pub fn prepare(&self) -> Result<()> {
        fs::create_dir_all(self.root.join("run"))?;
        fs::create_dir_all(self.root.join("dials"))?;
        fs::create_dir_all(self.root.join("logs"))?;
        if let Some(parent) = self.peer_config_path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    pub fn identity_path(&self) -> PathBuf {
        self.root.join("identity.toml")
    }

    pub fn peers_path(&self) -> PathBuf {
        self.peer_config_path.clone()
    }

    /// Authoritative sync-entry file, a sibling of `peers.toml` in the same
    /// config directory (`~/.config/fabric/syncs.toml` for the default home and
    /// for an explicit `--home` that points at the default state root;
    /// `<home>/syncs.toml` for a non-default `--home`/`FABRIC_HOME`).
    pub fn syncs_path(&self) -> PathBuf {
        self.peer_config_path.with_file_name("syncs.toml")
    }

    pub fn config_path(&self) -> PathBuf {
        self.root.join("config.toml")
    }

    /// Durable connection counters. State, not config, so this lives under the
    /// state root and never under the config dir that `peers.toml` uses.
    pub fn telemetry_path(&self) -> PathBuf {
        self.root.join("telemetry.json")
    }

    /// Monotonic endpoint generation owned by this node identity.
    pub fn endpoint_generation_path(&self) -> PathBuf {
        self.root.join("endpoint-generation")
    }

    /// The peers file that exists, current layout first, so the peer book can
    /// read a home it has not migrated yet.
    pub fn existing_peers_path(&self) -> Option<PathBuf> {
        if self.peer_config_path.exists() {
            return Some(self.peer_config_path.clone());
        }
        self.legacy_peer_config_path
            .as_ref()
            .filter(|path| path.exists())
            .cloned()
    }

    /// Remove the peers file of the old layout once the peer book has moved it.
    pub fn remove_legacy_peer_config(&self) -> Result<()> {
        if let Some(path) = &self.legacy_peer_config_path
            && path != &self.peer_config_path
            && path.exists()
        {
            fs::remove_file(path)
                .with_context(|| format!("failed to remove {}", path.display()))?;
        }
        Ok(())
    }

    pub fn control_socket_path(&self) -> PathBuf {
        self.root.join("run/control.sock")
    }

    /// The daemon's bridge socket, where the sync companion sends its requests.
    pub fn sync_ipc_socket_path(&self) -> PathBuf {
        self.root
            .join("run")
            .join(crate::sync::ipc::DAEMON_SOCKET_NAME)
    }

    /// The companion's bridge socket, where the daemon sends its requests.
    pub fn sync_companion_socket_path(&self) -> PathBuf {
        self.root
            .join("run")
            .join(crate::sync::ipc::COMPANION_SOCKET_NAME)
    }

    /// The companion's daily-rotated log, beside the daemon's validation log.
    pub fn sync_log_prefix(&self) -> &'static str {
        "fabric-sync.log"
    }

    pub fn log_path(&self) -> PathBuf {
        self.root.join("logs/daemon.log")
    }

    pub fn validation_log_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    pub fn validation_log_prefix(&self) -> &'static str {
        "validation.log"
    }

    pub fn restart_log_path(&self) -> PathBuf {
        self.root.join("logs/restart.log")
    }

    pub fn dial_socket_path(&self, peer: impl std::fmt::Display, protocol: &str) -> PathBuf {
        let peer = peer.to_string();
        let short_peer = &peer[..peer.len().min(8)];
        self.root
            .join("dials")
            .join(format!("{}-{:08x}.sock", short_peer, short_hash(protocol)))
    }
}

fn short_hash(input: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_default_home_puts_peers_in_xdg_config() {
        let home = PathBuf::from("/home/alice");
        let fh = FabricHome::resolve_from(None, Some(&home), None).unwrap();
        assert_eq!(fh.root, PathBuf::from("/home/alice/.local/share/fabric"));
        assert_eq!(
            fh.peers_path(),
            PathBuf::from("/home/alice/.config/fabric/peers.toml")
        );
        assert_eq!(
            fh.legacy_peer_config_path,
            Some(PathBuf::from("/home/alice/.local/share/fabric/peers.toml"))
        );
    }

    #[test]
    fn resolve_explicit_default_root_matches_default_layout() {
        // Regression: the service launches the daemon as `--home <default-root>`,
        // so it MUST resolve peers exactly like the no-argument CLI (XDG config),
        // not from `<home>/peers.toml`. Reading the wrong file left the daemon
        // with zero peers and took down the cross-machine bus.
        let home = PathBuf::from("/home/alice");
        let explicit = PathBuf::from("/home/alice/.local/share/fabric");
        let fh = FabricHome::resolve_from(Some(explicit), Some(&home), None).unwrap();
        assert_eq!(
            fh.peers_path(),
            PathBuf::from("/home/alice/.config/fabric/peers.toml"),
            "explicit --home at the default root must read XDG-config peers"
        );
        assert_eq!(
            fh.legacy_peer_config_path,
            Some(PathBuf::from("/home/alice/.local/share/fabric/peers.toml"))
        );
    }

    #[test]
    fn resolve_explicit_custom_root_stays_isolated() {
        let home = PathBuf::from("/home/alice");
        let explicit = PathBuf::from("/tmp/fabric-test-home");
        let fh = FabricHome::resolve_from(Some(explicit.clone()), Some(&home), None).unwrap();
        assert_eq!(fh.root, explicit);
        assert_eq!(fh.peers_path(), explicit.join("peers.toml"));
        assert_eq!(fh.legacy_peer_config_path, None);
    }

    #[test]
    fn resolve_explicit_root_without_home_env_is_isolated() {
        let explicit = PathBuf::from("/tmp/fabric-test-home");
        let fh = FabricHome::resolve_from(Some(explicit.clone()), None, None).unwrap();
        assert_eq!(fh.root, explicit);
        assert_eq!(fh.peers_path(), explicit.join("peers.toml"));
    }

    #[test]
    fn non_default_home_is_not_the_default_state_root() {
        // A dev/custom home must never register as the prod default root — that's
        // what makes `service install` refuse it and keeps dev off the prod service.
        let dev = FabricHome::new("/tmp/fabric-dev-xyz");
        assert!(!dev.is_default_state_root());
    }

    #[test]
    fn the_computed_default_root_is_the_default_state_root() {
        if let Some(default) = FabricHome::default_state_root() {
            assert!(FabricHome::new(default).is_default_state_root());
        }
    }

    #[test]
    fn resolve_without_home_env_errors() {
        let error = FabricHome::resolve_from(None, None, None).unwrap_err();
        assert!(
            format!("{error:#}").contains("HOME is not set"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn resolve_default_home_respects_xdg_config_home() {
        let home = PathBuf::from("/home/alice");
        let xdg = PathBuf::from("/xdg/conf");
        let fh = FabricHome::resolve_from(None, Some(&home), Some(xdg)).unwrap();
        assert_eq!(
            fh.peers_path(),
            PathBuf::from("/xdg/conf/fabric/peers.toml")
        );
    }
}
