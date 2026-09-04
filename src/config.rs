use std::{
    collections::{HashMap, HashSet, hash_map::DefaultHasher},
    env, fs,
    hash::{Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use iroh::{EndpointAddr, EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, Value};

pub const DEFAULT_EXEC_MAX_CHILDREN: usize = 32;
pub const DEFAULT_SERVER_SESSION_MAX_TOTAL: usize = 64;
pub const DEFAULT_SERVER_SESSION_MAX_PER_PEER: usize = 16;

fn is_false(value: &bool) -> bool {
    !*value
}

/// How long a detached shell or tunnel session is kept before its PTY is reaped.
///
/// Fifteen minutes, chosen from measurement rather than taste. The cost of a
/// longer window is bounded by what a detached session actually retains: an idle
/// shell buffers nothing at all, measured at 0 bytes across a full detached
/// window, so holding it costs a session struct and a PTY process and nothing
/// that grows with time.
///
/// The benefit is the case that actually happens: a closed laptop lid over lunch
/// keeps its shell. Sixty seconds did not survive a coffee break.
///
/// A session still producing output is the expensive case, and it is bounded
/// too. The tunnel replay buffer stops at its own cap, currently 4 MiB:
/// nothing ACKs a detached session, the reader waits for buffer space
/// that never frees, and the remote process then blocks on its own PTY write.
/// Measured directly, a runaway producer pins at exactly 4 MiB and stays there.
/// So retention is bounded per session no matter how long this window is, and in
/// aggregate by `DEFAULT_SERVER_SESSION_MAX_TOTAL`, which is 256 MiB at the
/// defaults.
///
/// This doc previously said the buffer had no cap, that a runaway shell would
/// reach roughly 17 MB across this window, and that this TTL was the only
/// backstop. All three were wrong, and the first was the stated reason not to
/// raise this value further. Backpressure is the backstop against a runaway
/// process; this TTL bounds how long a session lives, not how much it holds.
pub const DEFAULT_SERVER_SESSION_DETACHED_TTL_SECS: u64 = 15 * 60;

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

    fn existing_peers_path(&self) -> Option<PathBuf> {
        if self.peer_config_path.exists() {
            return Some(self.peer_config_path.clone());
        }
        self.legacy_peer_config_path
            .as_ref()
            .filter(|path| path.exists())
            .cloned()
    }

    fn remove_legacy_peer_config(&self) -> Result<()> {
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

    pub fn dial_socket_path(&self, peer: EndpointId, protocol: &str) -> PathBuf {
        let peer = peer.to_string();
        let short_peer = &peer[..peer.len().min(8)];
        self.root
            .join("dials")
            .join(format!("{}-{:08x}.sock", short_peer, short_hash(protocol)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdentityFile {
    secret_key: SecretKey,
}

pub fn load_or_create_identity(home: &FabricHome) -> Result<SecretKey> {
    home.prepare()?;
    let path = home.identity_path();
    if path.exists() {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let file: IdentityFile =
            toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
        return Ok(file.secret_key);
    }

    let file = IdentityFile::generate();
    let raw = toml::to_string_pretty(&file)?;
    write_secret_file(&path, raw.as_bytes())?;
    Ok(file.secret_key)
}

pub fn generate_identity_file(path: &Path) -> Result<EndpointId> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let file = IdentityFile::generate();
    let id = file.secret_key.public();
    let raw = toml::to_string_pretty(&file)?;
    write_secret_file(path, raw.as_bytes())?;
    Ok(id)
}

impl IdentityFile {
    fn generate() -> Self {
        Self {
            secret_key: SecretKey::generate(),
        }
    }
}

#[cfg(unix)]
fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(bytes)?;
    Ok(())
}

#[cfg(not(unix))]
fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    file.write_all(bytes)?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Peer {
    pub id: EndpointId,
    pub name: Option<String>,
    pub addr: Option<EndpointAddr>,
    /// This peer is expected to disconnect and return, such as a laptop.
    /// Its absence is normal and must not drive failure recovery.
    #[serde(default, skip_serializing_if = "is_false")]
    pub roaming: bool,
    /// Which services this peer may reach, by the NAME a person types:
    /// `shell`, `exec`, `sync`, `echo`, or any exposed protocol such as `web`.
    ///
    /// NAMES A SERVICE, NOT A PORT, and that is deliberate. Fabric publishes
    /// named services; the port is a detail of the exposing side that never
    /// crosses the wire. A permission naming a port would name something the
    /// peer cannot see and this machine can change without telling anyone.
    ///
    /// Fabric is an allow list. A service not in this list is refused,
    /// including one exposed later. An omitted field becomes an empty list and
    /// grants nothing.
    ///
    /// Policy keys on `id` and never on `name`. A name is a local label that
    /// can be changed at any time, and a permission that follows a rename is a
    /// permission granted to whoever inherits the label.
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitRemote {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitAccess {
    Read,
    Write,
}

impl GitAccess {
    pub fn permission(self, remote: &str) -> String {
        let operation = match self {
            Self::Read => "read",
            Self::Write => "write",
        };
        format!("git/{remote}/{operation}")
    }
}

/// Why an incoming connection was refused, in the words a person should read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Denied {
    /// The node is not in the allow-list at all.
    NotTrusted,
    /// The node is trusted, but it has no service grants.
    NoGrants { service: String },
    /// The node is trusted, but not for this service.
    NotPermitted { service: String },
}

impl Denied {
    /// The phrase a refusal carries across the wire.
    ///
    /// A WIRE CONTRACT, not a log string. The refusing side closes the
    /// connection with this text and the dialling side matches on it to tell a
    /// refusal apart from a peer that is merely away. Those need different
    /// reactions: one waits for the network, the other waits for a person.
    ///
    /// `a_refusal_is_recognisable_from_the_other_side` pins both ends.
    pub const WIRE_MARKER: &'static str = "not permitted for service";

    /// Did this error come from a peer refusing us, rather than from the
    /// network?
    pub fn is_refusal(error: &str) -> bool {
        error.contains(Self::WIRE_MARKER)
    }
}

impl std::fmt::Display for Denied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Two DIFFERENT sentences on purpose. A person who cannot tell
            // denial from a network fault, or one kind of denial from the
            // other, will turn the whole thing off.
            Denied::NotTrusted => write!(f, "peer is not trusted by this node"),
            Denied::NoGrants { service } => {
                write!(
                    f,
                    "peer has no grants; not permitted for service {service:?}"
                )
            }
            Denied::NotPermitted { service } => {
                write!(f, "peer not permitted for service {service:?}")
            }
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PeerBook {
    /// Does this machine serve remote shells at all?
    ///
    /// This is separate from each peer's grant. Both gates must permit the
    /// service. Absence defaults closed, like an omitted peer grant.
    #[serde(default)]
    allow_shell: bool,
    /// Does this machine serve remote command execution at all?
    #[serde(default)]
    allow_exec: bool,
    peers: Vec<Peer>,
    #[serde(default)]
    git_remotes: Vec<GitRemote>,
}

impl PeerBook {
    pub fn load(home: &FabricHome) -> Result<Self> {
        if let Some((path, book)) = Self::load_existing(home)? {
            if path != home.peers_path() {
                book.write_peer_file(home)?;
                home.remove_legacy_peer_config()?;
            }
            Self::remove_embedded_config_peers(home)?;
            return Ok(book);
        }

        let mut config = FabricConfig::load(home)?;
        if config.peers.is_empty() {
            return Ok(Self::default());
        }

        let book = Self {
            allow_shell: config.allow_shell().unwrap_or(false),
            allow_exec: config.allow_exec().unwrap_or(false),
            peers: std::mem::take(&mut config.peers),
            git_remotes: Vec::new(),
        };
        book.validate()?;
        book.write_peer_file(home)?;
        config.save(home)?;
        Ok(book)
    }

    fn load_existing(home: &FabricHome) -> Result<Option<(PathBuf, Self)>> {
        let Some(path) = home.existing_peers_path() else {
            return Ok(None);
        };
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let book: Self =
            toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
        book.validate()?;
        Ok(Some((path, book)))
    }

    pub fn save(&self, home: &FabricHome) -> Result<()> {
        self.validate()?;
        self.write_peer_file(home)?;
        home.remove_legacy_peer_config()?;
        Self::remove_embedded_config_peers(home)?;
        Ok(())
    }

    /// The header written above a new `peers.toml`.
    ///
    /// The rule about `name` lives in the FILE, not only in the documentation.
    /// A rule someone has to go and read is a rule that gets designed around by
    /// somebody who did read it.
    const PEER_FILE_HEADER: &'static str = "\
# fabric peers. Written by `fabric add` and `fabric remove`; safe to edit.
#
# allow_shell  whether this machine serves remote shells.
# allow_exec   whether this machine serves remote command execution.
#        Both settings default to false when absent. The command-line flags
#        with the same names are accepted for compatibility but do not change
#        these settings. Each peer also needs the matching service grant.
#
# id     the peer's identity. PERMISSIONS KEY ON THIS AND ONLY THIS.
# name   a local label for your convenience. You can rename it at any time,
#        and nothing about permissions follows the rename. Never write a rule
#        that depends on a name.
# roaming  whether this peer is expected to disconnect and return. Defaults
#        to false. An absent roaming peer is away, not failed.
# allow  which services this peer may reach, by the name a person types:
#        shell, exec, sync, echo, or any protocol you expose such as web.
#        Git grants are git/<remote>/read and git/<remote>/write.
#        Fabric is an allow list. Anything unlisted is refused, including a
#        service you expose later. Omit this field to grant no services.
# git_remotes  host-local repository names and absolute Git directory paths.
#        A declaration grants nothing until a peer allow list names it.
#
# A service is a NAME, not a port. The port belongs to whichever side runs
# `fabric expose`, and it never crosses the wire.

";

    fn write_peer_file(&self, home: &FabricHome) -> Result<()> {
        home.prepare()?;
        let path = home.peers_path();
        let serialized = toml::to_string_pretty(self)?;
        let raw = match home.existing_peers_path() {
            Some(existing_path) => {
                let existing = fs::read_to_string(&existing_path)
                    .with_context(|| format!("failed to read {}", existing_path.display()))?;
                self.upsert_peer_document(&existing, &serialized, &existing_path)?
            }
            None => format!("{}{}", Self::PEER_FILE_HEADER, serialized),
        };
        write_atomic(&path, raw.as_bytes())
    }

    fn upsert_peer_document(
        &self,
        existing: &str,
        serialized: &str,
        path: &Path,
    ) -> Result<String> {
        let before: Self = toml::from_str(existing)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        before.validate()?;
        let mut document: DocumentMut = existing
            .parse()
            .with_context(|| format!("failed to edit {}", path.display()))?;
        // New nested tables need positions between existing table headers.
        // Wide gaps preserve the parsed order and give each edit that space.
        spread_table_positions(document.as_table_mut());
        let desired: DocumentMut = serialized
            .parse()
            .context("failed to prepare the peers.toml update")?;
        let desired_root = desired.as_table();
        let root = document.as_table_mut();

        if before.allow_shell != self.allow_shell {
            upsert_table_field(root, desired_root, "allow_shell")?;
        }
        if before.allow_exec != self.allow_exec {
            upsert_table_field(root, desired_root, "allow_exec")?;
        }
        let appended_peers = upsert_peer_tables(root, desired_root, &before.peers, &self.peers)?;
        upsert_git_remote_tables(root, desired_root, &before.git_remotes, &self.git_remotes)?;

        let mut updated = document.to_string();
        // A peer address contains nested tables. Append each complete record so
        // its address stays directly after its own `[[peers]]` header.
        for peer in appended_peers {
            if !updated.ends_with('\n') {
                updated.push('\n');
            }
            if !updated.ends_with("\n\n") {
                updated.push('\n');
            }
            updated.push_str(&toml::to_string_pretty(&PeerEntries {
                peers: std::slice::from_ref(&peer),
            })?);
        }
        Ok(updated)
    }

    fn remove_embedded_config_peers(home: &FabricHome) -> Result<()> {
        if !home.config_path().exists() {
            return Ok(());
        }
        let mut config = FabricConfig::load(home)?;
        if config.peers.is_empty() {
            return Ok(());
        }
        config.peers.clear();
        config.save(home)
    }

    pub fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub fn allow_shell(&self) -> bool {
        self.allow_shell
    }

    pub fn allow_exec(&self) -> bool {
        self.allow_exec
    }

    pub fn set_allow_shell(&mut self, allow_shell: bool) {
        self.allow_shell = allow_shell;
    }

    pub fn set_allow_exec(&mut self, allow_exec: bool) {
        self.allow_exec = allow_exec;
    }

    pub fn git_remotes(&self) -> &[GitRemote] {
        &self.git_remotes
    }

    pub fn git_remote(&self, name: &str) -> Option<&GitRemote> {
        self.git_remotes.iter().find(|remote| remote.name == name)
    }

    pub fn share_git_remote(&mut self, name: &str, path: PathBuf) -> Result<()> {
        validate_git_remote_name(name)?;
        if !path.is_absolute() {
            bail!("Git remote path must be absolute: {}", path.display());
        }
        if self.git_remote(name).is_some() {
            bail!("Git remote {name:?} is already shared; unshare it before changing its path");
        }
        self.git_remotes.push(GitRemote {
            name: name.to_string(),
            path,
        });
        self.git_remotes
            .sort_by(|left, right| left.name.cmp(&right.name));
        Ok(())
    }

    pub fn unshare_git_remote(&mut self, name: &str) -> Result<()> {
        validate_git_remote_name(name)?;
        let before = self.git_remotes.len();
        self.git_remotes.retain(|remote| remote.name != name);
        if self.git_remotes.len() == before {
            bail!("Git remote {name:?} is not shared");
        }
        let prefix = format!("git/{name}/");
        for peer in &mut self.peers {
            peer.allow
                .retain(|permission| !permission.starts_with(&prefix));
        }
        Ok(())
    }

    pub fn grant_git_remote(
        &mut self,
        remote: &str,
        peer: &str,
        access: GitAccess,
    ) -> Result<bool> {
        self.require_git_remote(remote)?;
        let permission = access.permission(remote);
        let peer = self.peer_mut(peer)?;
        if peer.allow.contains(&permission) {
            return Ok(false);
        }
        peer.allow.push(permission);
        peer.allow.sort();
        peer.allow.dedup();
        Ok(true)
    }

    pub fn revoke_git_remote(
        &mut self,
        remote: &str,
        peer: &str,
        access: GitAccess,
    ) -> Result<bool> {
        self.require_git_remote(remote)?;
        let permission = access.permission(remote);
        let peer = self.peer_mut(peer)?;
        let before = peer.allow.len();
        peer.allow.retain(|allowed| allowed != &permission);
        Ok(peer.allow.len() != before)
    }

    fn require_git_remote(&self, name: &str) -> Result<&GitRemote> {
        validate_git_remote_name(name)?;
        self.git_remote(name)
            .with_context(|| format!("Git remote {name:?} is not shared"))
    }

    fn peer_mut(&mut self, peer: &str) -> Result<&mut Peer> {
        let id = EndpointId::from_str(peer).ok();
        self.peers
            .iter_mut()
            .find(|entry| id == Some(entry.id) || entry.name.as_deref() == Some(peer))
            .with_context(|| format!("unknown peer {peer:?}; add it before granting Git access"))
    }

    /// May `id` reach `service`? The ONLY place this question is answered.
    ///
    /// Trusted and permitted are two different answers. A peer absent from the
    /// book is `NotTrusted`; a peer present but restricted is `NotPermitted`.
    ///
    /// This checks the per-peer grant. The caller applies the machine-level
    /// `allow_shell` and `allow_exec` settings from this same file.
    pub fn may(&self, id: &EndpointId, service: &str) -> Result<(), Denied> {
        let Some(peer) = self.peers.iter().find(|peer| peer.id == *id) else {
            return Err(Denied::NotTrusted);
        };
        match &peer.allow {
            allowed if allowed.is_empty() => Err(Denied::NoGrants {
                service: service.to_string(),
            }),
            allowed if allowed.iter().any(|s| s == service) => Ok(()),
            _ => Err(Denied::NotPermitted {
                service: service.to_string(),
            }),
        }
    }

    pub fn trusted_ids(&self) -> HashSet<EndpointId> {
        self.peers.iter().map(|peer| peer.id).collect()
    }

    pub fn add(&mut self, id: EndpointId, name: Option<String>, addr: Option<EndpointAddr>) {
        self.add_with_allow(id, name, addr, None)
    }

    /// Add a peer, optionally restricting it to a set of services.
    ///
    /// An existing entry's `allow` is preserved when this is called without
    /// one, so re-adding a peer to update its address cannot silently widen its
    /// permissions.
    pub fn add_with_allow(
        &mut self,
        id: EndpointId,
        name: Option<String>,
        addr: Option<EndpointAddr>,
        allow: Option<Vec<String>>,
    ) {
        let roaming = self
            .peers
            .iter()
            .find(|peer| peer.id == id)
            .is_some_and(|peer| peer.roaming);
        let allow = allow
            .or_else(|| {
                self.peers
                    .iter()
                    .find(|peer| peer.id == id)
                    .map(|peer| peer.allow.clone())
            })
            .unwrap_or_default();
        self.peers.retain(|peer| peer.id != id);
        if let Some(name) = &name {
            self.peers
                .retain(|peer| peer.name.as_deref() != Some(name.as_str()));
        }
        self.peers.push(Peer {
            id,
            name,
            addr,
            roaming,
            allow,
        });
        self.peers
            .sort_by_key(|peer| (peer.name.clone().unwrap_or_default(), peer.id.to_string()));
    }

    pub fn remove(&mut self, peer: &str) -> bool {
        let before = self.peers.len();
        if let Ok(id) = EndpointId::from_str(peer) {
            self.peers.retain(|entry| entry.id != id);
        } else {
            self.peers
                .retain(|entry| entry.name.as_deref() != Some(peer));
        }
        self.peers.len() != before
    }

    pub fn resolve(&self, peer: &str) -> Result<EndpointAddr> {
        if let Ok(id) = EndpointId::from_str(peer) {
            return Ok(self.addr_for_id(id));
        }

        let matches: Vec<&Peer> = self
            .peers
            .iter()
            .filter(|entry| entry.name.as_deref() == Some(peer))
            .collect();
        match matches.as_slice() {
            [entry] => Ok(entry
                .addr
                .clone()
                .unwrap_or_else(|| EndpointAddr::new(entry.id))),
            [] => bail!(
                "unknown peer {peer:?}; add it to peers.toml or use `fabric add <nodeid> [name]`"
            ),
            _ => bail!("ambiguous peer name {peer:?}"),
        }
    }

    fn addr_for_id(&self, id: EndpointId) -> EndpointAddr {
        self.peers
            .iter()
            .find(|entry| entry.id == id)
            .and_then(|entry| entry.addr.clone())
            .unwrap_or_else(|| EndpointAddr::new(id))
    }

    fn validate(&self) -> Result<()> {
        let mut ids = HashSet::new();
        let mut names = HashMap::new();
        for peer in &self.peers {
            if !ids.insert(peer.id) {
                bail!("duplicate peer id {}", peer.id);
            }
            if let Some(name) = &peer.name {
                if name.trim().is_empty() {
                    bail!("peer name cannot be empty");
                }
                if names.insert(name, peer.id).is_some() {
                    bail!("duplicate peer name {name:?}");
                }
            }
            if let Some(addr) = &peer.addr
                && addr.id != peer.id
            {
                bail!("address hint for {} points at {}", peer.id, addr.id);
            }
        }
        let mut remote_names = HashSet::new();
        for remote in &self.git_remotes {
            validate_git_remote_name(&remote.name)?;
            if !remote.path.is_absolute() {
                bail!(
                    "Git remote {:?} path must be absolute: {}",
                    remote.name,
                    remote.path.display()
                );
            }
            if !remote_names.insert(remote.name.as_str()) {
                bail!("duplicate Git remote name {:?}", remote.name);
            }
        }
        for peer in &self.peers {
            for permission in &peer.allow {
                let Some(rest) = permission.strip_prefix("git/") else {
                    continue;
                };
                let Some((remote, operation)) = rest.rsplit_once('/') else {
                    bail!("invalid Git permission {permission:?}");
                };
                validate_git_remote_name(remote)?;
                if !matches!(operation, "read" | "write") {
                    bail!("invalid Git permission {permission:?}");
                }
                if !remote_names.contains(remote) {
                    bail!("Git permission {permission:?} names an unshared remote {remote:?}");
                }
            }
        }
        Ok(())
    }
}

fn upsert_peer_tables(
    root: &mut Table,
    desired_root: &Table,
    before: &[Peer],
    after: &[Peer],
) -> Result<Vec<Peer>> {
    if before == after {
        return Ok(Vec::new());
    }
    let Some(desired_tables) = desired_root.get("peers").and_then(Item::as_array_of_tables) else {
        upsert_table_field(root, desired_root, "peers")?;
        return Ok(Vec::new());
    };
    let Some(current_tables) = root.get_mut("peers").and_then(Item::as_array_of_tables_mut) else {
        let Some(first) = desired_tables.iter().next() else {
            upsert_table_field(root, desired_root, "peers")?;
            return Ok(Vec::new());
        };
        let mut first_table = ArrayOfTables::new();
        first_table.push(first.clone());
        let first_id = table_endpoint_id(first, "id")
            .context("the prepared peers.toml update has no first peer id")?;
        let mut first_item = Item::ArrayOfTables(first_table);
        let first_position = root.position().unwrap_or(0).saturating_add(1);
        if let Some(item) = root.get_mut("peers") {
            replace_item_preserving_decor(item, first_item, first_position);
        } else {
            let mut next_position = first_position;
            assign_item_positions(&mut first_item, &mut next_position);
            root.insert("peers", first_item);
        }
        return Ok(after
            .iter()
            .filter(|peer| peer.id != first_id)
            .cloned()
            .collect());
    };

    let desired_ids: HashSet<EndpointId> = after.iter().map(|peer| peer.id).collect();
    if before.iter().any(|peer| !desired_ids.contains(&peer.id)) {
        current_tables.retain(|table| {
            table_endpoint_id(table, "id").is_some_and(|id| desired_ids.contains(&id))
        });
    }

    for table in current_tables.iter_mut() {
        let id = table_endpoint_id(table, "id")
            .context("a validated peers.toml table lost its peer id")?;
        let old_peer = before
            .iter()
            .find(|peer| peer.id == id)
            .context("a validated peers.toml table lost its old peer")?;
        let new_peer = after
            .iter()
            .find(|peer| peer.id == id)
            .context("a retained peers.toml table lost its new peer")?;
        let desired_table = desired_tables
            .iter()
            .find(|candidate| table_endpoint_id(candidate, "id") == Some(id))
            .context("the prepared peers.toml update lost a peer table")?;

        if old_peer.name != new_peer.name {
            upsert_table_field(table, desired_table, "name")?;
        }
        if old_peer.addr != new_peer.addr {
            upsert_table_field(table, desired_table, "addr")?;
        }
        if old_peer.roaming != new_peer.roaming {
            upsert_table_field(table, desired_table, "roaming")?;
        }
        if old_peer.allow != new_peer.allow {
            upsert_string_array_field(table, desired_table, "allow")?;
        }
    }

    let present: HashSet<EndpointId> = current_tables
        .iter()
        .filter_map(|table| table_endpoint_id(table, "id"))
        .collect();
    Ok(after
        .iter()
        .filter(|peer| !present.contains(&peer.id))
        .cloned()
        .collect())
}

#[derive(Serialize)]
struct PeerEntries<'a> {
    peers: &'a [Peer],
}

fn upsert_git_remote_tables(
    root: &mut Table,
    desired_root: &Table,
    before: &[GitRemote],
    after: &[GitRemote],
) -> Result<()> {
    if before == after {
        return Ok(());
    }
    let Some(current_tables) = root
        .get_mut("git_remotes")
        .and_then(Item::as_array_of_tables_mut)
    else {
        return upsert_table_field(root, desired_root, "git_remotes");
    };
    let Some(desired_tables) = desired_root
        .get("git_remotes")
        .and_then(Item::as_array_of_tables)
    else {
        return upsert_table_field(root, desired_root, "git_remotes");
    };

    let desired_names: HashSet<&str> = after.iter().map(|remote| remote.name.as_str()).collect();
    current_tables.retain(|table| {
        table_string(table, "name").is_some_and(|name| desired_names.contains(name))
    });

    for table in current_tables.iter_mut() {
        let name = table_string(table, "name")
            .context("a validated peers.toml table lost its Git remote name")?;
        let old_remote = before
            .iter()
            .find(|remote| remote.name == name)
            .context("a validated peers.toml table lost its old Git remote")?;
        let new_remote = after
            .iter()
            .find(|remote| remote.name == name)
            .context("a retained peers.toml table lost its new Git remote")?;
        if old_remote.path != new_remote.path {
            let desired_table = desired_tables
                .iter()
                .find(|candidate| table_string(candidate, "name") == Some(name))
                .context("the prepared peers.toml update lost a Git remote table")?;
            upsert_table_field(table, desired_table, "path")?;
        }
    }

    let present: HashSet<String> = current_tables
        .iter()
        .filter_map(|table| table_string(table, "name"))
        .map(str::to_owned)
        .collect();
    let mut next_position = current_tables
        .iter()
        .filter_map(Table::position)
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    for desired_table in desired_tables.iter() {
        let name = table_string(desired_table, "name")
            .context("the prepared peers.toml update has no Git remote name")?;
        if !present.contains(name) {
            let mut desired_table = desired_table.clone();
            assign_table_positions(&mut desired_table, &mut next_position);
            current_tables.push(desired_table);
        }
    }
    Ok(())
}

fn table_endpoint_id(table: &Table, key: &str) -> Option<EndpointId> {
    EndpointId::from_str(table_string(table, key)?).ok()
}

fn table_string<'a>(table: &'a Table, key: &str) -> Option<&'a str> {
    table.get(key)?.as_str()
}

fn upsert_string_array_field(table: &mut Table, desired: &Table, key: &str) -> Result<()> {
    let desired_item = desired
        .get(key)
        .with_context(|| format!("the prepared peers.toml update has no {key} field"))?;
    let Some(current_array) = table.get_mut(key).and_then(Item::as_array_mut) else {
        return upsert_table_field(table, desired, key);
    };
    let Some(desired_array) = desired_item.as_array() else {
        return upsert_table_field(table, desired, key);
    };

    let mut old_values: Vec<Value> = current_array.iter().cloned().collect();
    let desired_values: Vec<Value> = desired_array.iter().cloned().collect();
    let decor = current_array.decor().clone();
    let trailing = current_array.trailing().clone();
    let trailing_comma = current_array.trailing_comma();
    current_array.clear();
    *current_array.decor_mut() = decor;
    current_array.set_trailing(trailing);
    current_array.set_trailing_comma(trailing_comma);

    for desired_value in desired_values {
        let matching = old_values
            .iter()
            .position(|old_value| old_value.as_str() == desired_value.as_str());
        let value = matching
            .map(|index| old_values.remove(index))
            .unwrap_or(desired_value);
        current_array.push_formatted(value);
    }
    Ok(())
}

fn upsert_table_field(table: &mut Table, desired: &Table, key: &str) -> Result<()> {
    let Some(desired_item) = desired.get(key).cloned() else {
        table.remove(key);
        return Ok(());
    };
    let child_position = table.position().unwrap_or(0).saturating_add(1);
    if let Some(current_item) = table.get_mut(key) {
        let current_is_table = matches!(current_item, Item::Table(_) | Item::ArrayOfTables(_));
        let desired_is_table = matches!(desired_item, Item::Table(_) | Item::ArrayOfTables(_));
        replace_item_preserving_decor(current_item, desired_item, child_position);
        if current_is_table != desired_is_table
            && let Some(mut current_key) = table.key_mut(key)
        {
            // A key-value line uses one space before `=`, while a table header
            // uses no space before `]`. Clear the old context's key decor when
            // a field changes between these two shapes.
            current_key.fmt();
        }
    } else {
        let mut desired_item = desired_item;
        let mut next_position = child_position;
        assign_item_positions(&mut desired_item, &mut next_position);
        table.insert(key, desired_item);
    }
    Ok(())
}

fn replace_item_preserving_decor(current: &mut Item, mut desired: Item, fallback_position: isize) {
    let mut next_position = match &*current {
        Item::Table(table) => table.position().unwrap_or(fallback_position),
        Item::ArrayOfTables(tables) => tables
            .iter()
            .filter_map(Table::position)
            .min()
            .unwrap_or(fallback_position),
        _ => fallback_position,
    };
    assign_item_positions(&mut desired, &mut next_position);
    match (&*current, &mut desired) {
        (Item::Value(old), Item::Value(new)) => *new.decor_mut() = old.decor().clone(),
        (Item::Table(old), Item::Table(new)) => *new.decor_mut() = old.decor().clone(),
        _ => {}
    }
    *current = desired;
}

const TABLE_POSITION_GAP: isize = 1_000_000;

fn spread_table_positions(table: &mut Table) {
    if let Some(position) = table.position() {
        table.set_position(Some(position.saturating_mul(TABLE_POSITION_GAP)));
    }
    for (_, item) in table.iter_mut() {
        match item {
            Item::Table(child) => spread_table_positions(child),
            Item::ArrayOfTables(children) => {
                for child in children.iter_mut() {
                    spread_table_positions(child);
                }
            }
            _ => {}
        }
    }
}

fn assign_item_positions(item: &mut Item, next_position: &mut isize) {
    match item {
        Item::Table(table) => assign_table_positions(table, next_position),
        Item::ArrayOfTables(tables) => {
            for table in tables.iter_mut() {
                assign_table_positions(table, next_position);
            }
        }
        _ => {}
    }
}

fn assign_table_positions(table: &mut Table, next_position: &mut isize) {
    table.set_position(Some(*next_position));
    *next_position = next_position.saturating_add(1);
    for (_, item) in table.iter_mut() {
        assign_item_positions(item, next_position);
    }
}

pub fn validate_git_remote_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("Git remote name cannot be empty");
    }
    if name.len() > 64 {
        bail!("Git remote name must be 64 bytes or less");
    }
    if matches!(name, "." | "..") {
        bail!("Git remote name cannot be a dot segment");
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("Git remote name may contain only ASCII letters, digits, dot, underscore, and dash");
    }
    Ok(())
}

pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let permissions = fs::metadata(path)
        .ok()
        .map(|metadata| metadata.permissions());
    let temp = path.with_extension(format!(
        "toml.fabric-tmp-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    let written = (|| -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .with_context(|| format!("failed to create {}", temp.display()))?;
        if let Some(permissions) = permissions {
            file.set_permissions(permissions)
                .with_context(|| format!("failed to preserve permissions on {}", temp.display()))?;
        }
        file.write_all(bytes)
            .with_context(|| format!("failed to write {}", temp.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to flush {}", temp.display()))?;
        fs::rename(&temp, path)
            .with_context(|| format!("failed to rename into {}", path.display()))?;
        if let Some(parent) = path.parent()
            && let Ok(directory) = fs::File::open(parent)
        {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if written.is_err() {
        let _ = fs::remove_file(&temp);
    }
    written
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedExpose {
    pub protocol: String,
    #[serde(flatten)]
    pub target: PersistedExposeTarget,
}

impl PersistedExpose {
    pub fn socket(protocol: String, socket: PathBuf) -> Self {
        Self {
            protocol,
            target: PersistedExposeTarget::Socket { socket },
        }
    }

    pub fn exec(protocol: String, argv: Vec<String>, max_children: usize) -> Self {
        Self {
            protocol,
            target: PersistedExposeTarget::Exec { argv, max_children },
        }
    }

    pub fn tcp(protocol: String, addr: String) -> Self {
        Self {
            protocol,
            target: PersistedExposeTarget::Tcp { addr },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PersistedExposeTarget {
    Socket {
        socket: PathBuf,
    },
    Tcp {
        addr: String,
    },
    Exec {
        argv: Vec<String>,
        #[serde(default = "default_exec_max_children")]
        max_children: usize,
    },
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FabricConfig {
    #[serde(default)]
    allow_shell: Option<bool>,
    #[serde(default)]
    allow_exec: Option<bool>,
    /// The memory ceiling the last install asked for, in MiB.
    ///
    /// Persisted for the same reason the two allow flags are: the rendered
    /// plist or unit is not a place to remember an operator's choice, because
    /// the next re-render starts from whatever the caller passed and silently
    /// drops what it does not mention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_max_mb: Option<u64>,
    #[serde(default)]
    server_sessions: ServerSessionConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    peers: Vec<Peer>,
    #[serde(default)]
    exposes: Vec<PersistedExpose>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerSessionConfig {
    #[serde(default = "default_server_session_max_total")]
    max_total: usize,
    #[serde(default = "default_server_session_max_per_peer")]
    max_per_peer: usize,
    #[serde(default = "default_server_session_detached_ttl_secs")]
    detached_ttl_secs: u64,
}

impl Default for ServerSessionConfig {
    fn default() -> Self {
        Self {
            max_total: DEFAULT_SERVER_SESSION_MAX_TOTAL,
            max_per_peer: DEFAULT_SERVER_SESSION_MAX_PER_PEER,
            detached_ttl_secs: DEFAULT_SERVER_SESSION_DETACHED_TTL_SECS,
        }
    }
}

impl ServerSessionConfig {
    pub fn max_total(&self) -> usize {
        self.max_total
    }

    pub fn max_per_peer(&self) -> usize {
        self.max_per_peer
    }

    pub fn detached_ttl_secs(&self) -> u64 {
        self.detached_ttl_secs
    }
}

impl FabricConfig {
    pub fn load(home: &FabricHome) -> Result<Self> {
        let path = home.config_path();
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

    pub fn save(&self, home: &FabricHome) -> Result<()> {
        home.prepare()?;
        self.validate()?;
        let raw = toml::to_string_pretty(self)?;
        fs::write(home.config_path(), raw)?;
        Ok(())
    }

    pub fn allow_shell(&self) -> Option<bool> {
        self.allow_shell
    }

    pub fn allow_exec(&self) -> Option<bool> {
        self.allow_exec
    }

    pub fn memory_max_mb(&self) -> Option<u64> {
        self.memory_max_mb
    }

    pub fn server_sessions(&self) -> &ServerSessionConfig {
        &self.server_sessions
    }

    pub fn set_allow_shell(&mut self, allow_shell: bool) {
        self.allow_shell = Some(allow_shell);
    }

    pub fn set_allow_exec(&mut self, allow_exec: bool) {
        self.allow_exec = Some(allow_exec);
    }

    /// `None` clears the ceiling, which is what `--no-memory-max-mb` asks for.
    pub fn set_memory_max_mb(&mut self, memory_max_mb: Option<u64>) {
        self.memory_max_mb = memory_max_mb;
    }

    pub fn exposes(&self) -> &[PersistedExpose] {
        &self.exposes
    }

    pub fn upsert_expose(&mut self, expose: PersistedExpose) {
        self.exposes
            .retain(|entry| entry.protocol != expose.protocol);
        self.exposes.push(expose);
        self.exposes
            .sort_by(|left, right| left.protocol.cmp(&right.protocol));
    }

    pub fn remove_expose(&mut self, protocol: &str) -> bool {
        let before = self.exposes.len();
        self.exposes.retain(|entry| entry.protocol != protocol);
        self.exposes.len() != before
    }

    fn validate(&self) -> Result<()> {
        PeerBook {
            allow_shell: false,
            allow_exec: false,
            peers: self.peers.clone(),
            git_remotes: Vec::new(),
        }
        .validate()?;

        validate_server_session_config(
            self.server_sessions.max_total,
            self.server_sessions.max_per_peer,
            self.server_sessions.detached_ttl_secs,
        )?;

        let mut protocols = HashSet::new();
        for expose in &self.exposes {
            validate_protocol(&expose.protocol)?;
            if !protocols.insert(expose.protocol.as_str()) {
                bail!("duplicate expose protocol {:?}", expose.protocol);
            }
            match &expose.target {
                PersistedExposeTarget::Socket { socket } => {
                    if !socket.is_absolute() {
                        bail!("expose {:?} socket path must be absolute", expose.protocol);
                    }
                }
                PersistedExposeTarget::Tcp { addr } => {
                    validate_tcp_addr(addr)?;
                }
                PersistedExposeTarget::Exec { argv, max_children } => {
                    if argv.is_empty() {
                        bail!("expose {:?} exec command cannot be empty", expose.protocol);
                    }
                    if *max_children == 0 {
                        bail!(
                            "expose {:?} max_children must be greater than zero",
                            expose.protocol
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

fn default_exec_max_children() -> usize {
    DEFAULT_EXEC_MAX_CHILDREN
}

fn default_server_session_max_total() -> usize {
    DEFAULT_SERVER_SESSION_MAX_TOTAL
}

fn default_server_session_max_per_peer() -> usize {
    DEFAULT_SERVER_SESSION_MAX_PER_PEER
}

fn default_server_session_detached_ttl_secs() -> u64 {
    DEFAULT_SERVER_SESSION_DETACHED_TTL_SECS
}

pub fn validate_server_session_caps(max_total: usize, max_per_peer: usize) -> Result<()> {
    if max_total == 0 {
        bail!("server_sessions.max_total must be greater than zero");
    }
    if max_per_peer == 0 {
        bail!("server_sessions.max_per_peer must be greater than zero");
    }
    if max_per_peer > max_total {
        bail!("server_sessions.max_per_peer cannot exceed server_sessions.max_total");
    }
    Ok(())
}

pub fn validate_server_session_config(
    max_total: usize,
    max_per_peer: usize,
    detached_ttl_secs: u64,
) -> Result<()> {
    validate_server_session_caps(max_total, max_per_peer)?;
    if detached_ttl_secs == 0 {
        bail!("server_sessions.detached_ttl_secs must be greater than zero");
    }
    Ok(())
}

pub fn validate_tcp_addr(addr: &str) -> Result<()> {
    if addr.trim().is_empty() {
        bail!("tcp address cannot be empty");
    }
    if addr.bytes().any(|byte| byte == 0 || byte == b'\n') {
        bail!("tcp address cannot contain NUL or newline bytes");
    }
    if !addr.contains(':') {
        bail!("tcp address must be HOST:PORT");
    }
    Ok(())
}

pub fn parse_node_id(node_id: &str) -> Result<EndpointId> {
    EndpointId::from_str(node_id).with_context(|| format!("invalid node id {node_id:?}"))
}

pub fn parse_addr_json(addr: Option<&str>, expected: EndpointId) -> Result<Option<EndpointAddr>> {
    let Some(addr) = addr else {
        return Ok(None);
    };
    let parsed: EndpointAddr =
        serde_json::from_str(addr).context("address hints must be EndpointAddr JSON")?;
    if parsed.id != expected {
        bail!(
            "address hint id {} does not match node id {}",
            parsed.id,
            expected
        );
    }
    Ok(Some(parsed))
}

pub fn validate_protocol(protocol: &str) -> Result<Vec<u8>> {
    if protocol.is_empty() {
        bail!("protocol cannot be empty");
    }
    if protocol.len() > 255 {
        bail!("protocol ALPN is too long; keep it at 255 bytes or less");
    }
    if protocol.bytes().any(|byte| byte == 0 || byte == b'\n') {
        bail!("protocol cannot contain NUL or newline bytes");
    }
    if protocol.starts_with("git/") {
        bail!("the git/ protocol namespace is reserved for Fabric Git remotes");
    }
    Ok(protocol.as_bytes().to_vec())
}

fn short_hash(input: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A distinct, valid identity per call. An `EndpointId` is an ed25519
    /// public key, so it cannot be conjured from an arbitrary byte pattern.
    fn an_id(_seed: u8) -> EndpointId {
        SecretKey::generate().public()
    }

    /// Fabric is an allow list. An omitted list grants nothing.
    #[test]
    fn a_peer_without_an_allow_list_may_reach_nothing() {
        let mut book = PeerBook::default();
        let hetz = an_id(1);
        book.add(hetz, Some("hetz".into()), None);
        for service in ["sync", "shell", "anything-exposed-later"] {
            let denied = book
                .may(&hetz, service)
                .expect_err("an omitted allow list granted a service");
            assert!(
                denied.to_string().contains("no grants"),
                "the refusal hid the empty allow list: {denied}"
            );
        }
    }

    #[test]
    fn omitted_and_explicit_empty_lists_have_the_same_policy() {
        let id = an_id(1);
        let omitted: PeerBook = toml::from_str(&format!("[[peers]]\nid = \"{id}\"\n"))
            .expect("an omitted allow field must parse");
        let explicit: PeerBook = toml::from_str(&format!("[[peers]]\nid = \"{id}\"\nallow = []\n"))
            .expect("an explicit empty allow field must parse");

        for service in ["sync", "shell", "anything-exposed-later"] {
            assert_eq!(omitted.may(&id, service), explicit.may(&id, service));
            assert!(omitted.may(&id, service).is_err());
        }

        let written = toml::to_string_pretty(&omitted).expect("the peer book must serialize");
        assert!(
            written.contains("allow = []"),
            "saving did not make the empty allow list explicit: {written}"
        );
    }

    #[test]
    fn git_remote_and_peer_grants_round_trip_in_one_file() {
        let id = an_id(2);
        let mut book = PeerBook::default();
        book.add(id, Some("friend".into()), None);
        book.share_git_remote("mandat", PathBuf::from("/srv/git/mandat.git"))
            .unwrap();
        book.grant_git_remote("mandat", "friend", GitAccess::Read)
            .unwrap();

        let raw = toml::to_string_pretty(&book).unwrap();
        let restored: PeerBook = toml::from_str(&raw).unwrap();
        assert_eq!(
            restored.git_remote("mandat").unwrap().path,
            PathBuf::from("/srv/git/mandat.git")
        );
        assert_eq!(restored.may(&id, "git/mandat/read"), Ok(()));
        assert!(restored.may(&id, "git/mandat/write").is_err());
    }

    #[test]
    fn ordinary_service_grants_do_not_grant_git_access() {
        let id = an_id(3);
        let mut book = PeerBook::default();
        book.add_with_allow(
            id,
            Some("friend".into()),
            None,
            Some(vec!["shell".into(), "exec".into(), "pty-view".into()]),
        );
        book.share_git_remote("mandat", PathBuf::from("/srv/git/mandat.git"))
            .unwrap();

        assert!(book.may(&id, "git/mandat/read").is_err());
        assert!(book.may(&id, "git/mandat/write").is_err());
    }

    #[test]
    fn unsharing_removes_only_that_remotes_grants() {
        let id = an_id(4);
        let mut book = PeerBook::default();
        book.add(id, Some("friend".into()), None);
        for remote in ["mandat", "other"] {
            book.share_git_remote(remote, PathBuf::from(format!("/srv/git/{remote}.git")))
                .unwrap();
            book.grant_git_remote(remote, "friend", GitAccess::Read)
                .unwrap();
        }

        book.unshare_git_remote("mandat").unwrap();
        assert!(book.git_remote("mandat").is_none());
        assert!(book.may(&id, "git/mandat/read").is_err());
        assert_eq!(book.may(&id, "git/other/read"), Ok(()));
    }

    #[test]
    fn git_remote_names_and_paths_are_strict() {
        let mut book = PeerBook::default();
        for name in ["", ".", "..", "has/slash", "percent%20name"] {
            assert!(
                book.share_git_remote(name, PathBuf::from("/srv/git/repo.git"))
                    .is_err(),
                "invalid remote name {name:?} was accepted"
            );
        }
        assert!(
            book.share_git_remote("mandat", PathBuf::from("relative/repo.git"))
                .is_err()
        );
        book.share_git_remote("mandat", PathBuf::from("/srv/git/mandat.git"))
            .unwrap();
        assert!(
            book.share_git_remote("mandat", PathBuf::from("/srv/git/other.git"))
                .is_err(),
            "a share was silently rebound"
        );
    }

    #[test]
    fn git_permissions_must_name_a_declared_remote_and_exact_operation() {
        let id = an_id(5);
        for permission in ["git/missing/read", "git/mandat/admin", "git/mandat"] {
            let mut book = PeerBook::default();
            book.share_git_remote("mandat", PathBuf::from("/srv/git/mandat.git"))
                .unwrap();
            book.add_with_allow(
                id,
                Some("friend".into()),
                None,
                Some(vec![permission.into()]),
            );
            assert!(
                book.validate().is_err(),
                "invalid Git permission {permission:?} passed validation"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn atomic_peer_save_preserves_existing_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let home = FabricHome::new(directory.path());
        let mut book = PeerBook::default();
        book.save(&home).unwrap();
        fs::set_permissions(home.peers_path(), fs::Permissions::from_mode(0o640)).unwrap();

        book.share_git_remote("mandat", PathBuf::from("/srv/git/mandat.git"))
            .unwrap();
        book.save(&home).unwrap();

        let mode = fs::metadata(home.peers_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o640);
    }

    #[test]
    fn first_peer_save_creates_the_document_header() {
        let directory = tempfile::tempdir().unwrap();
        let home = FabricHome::new(directory.path());
        assert!(!home.peers_path().exists());

        PeerBook::default().save(&home).unwrap();

        let raw = fs::read_to_string(home.peers_path()).unwrap();
        assert!(raw.starts_with("# fabric peers."));
        PeerBook::load(&home).unwrap();
    }

    #[test]
    fn peer_add_and_remove_preserve_human_formatting() {
        let directory = tempfile::tempdir().unwrap();
        let home = FabricHome::new(directory.path());
        home.prepare().unwrap();
        let kept = an_id(20);
        let removed = an_id(21);
        let added = an_id(22);
        let kept_block = format!(
            "# This comment belongs to the peer.\n\
             [[peers]]\n\
             id   = '{kept}' # Keep this identity note.\n\
             name = 'kept'\n\
             allow = [ 'shell' ] # Keep this grant note.\n"
        );
        let original = format!(
            "# My peer file. Keep this header.\n\
             allow_shell   = false # Keep this setting note.\n\
             allow_exec = false\n\
             future_setting = 'keep me' # Keep unknown data too.\n\
             \n\
             {kept_block}\n\
             # This peer will be removed.\n\
             [[peers]]\n\
             id = '{removed}'\n\
             name = 'removed'\n\
             allow = []\n"
        );
        fs::write(home.peers_path(), original).unwrap();

        let mut book = PeerBook::load(&home).unwrap();
        book.add(added, Some("added".into()), None);
        book.save(&home).unwrap();

        let after_add = fs::read_to_string(home.peers_path()).unwrap();
        assert!(after_add.contains("# My peer file. Keep this header."));
        assert!(after_add.contains(&kept_block));
        assert!(after_add.contains("future_setting = 'keep me' # Keep unknown data too."));
        assert!(!after_add.contains("# fabric peers."));

        let mut book = PeerBook::load(&home).unwrap();
        book.set_allow_shell(true);
        book.save(&home).unwrap();
        let after_policy = fs::read_to_string(home.peers_path()).unwrap();
        assert!(after_policy.contains("allow_shell   = true # Keep this setting note."));

        let mut book = PeerBook::load(&home).unwrap();
        assert!(book.remove("removed"));
        book.save(&home).unwrap();

        let after_remove = fs::read_to_string(home.peers_path()).unwrap();
        assert!(after_remove.contains("# My peer file. Keep this header."));
        assert!(after_remove.contains(&kept_block));
        assert!(after_remove.contains("future_setting = 'keep me' # Keep unknown data too."));
        assert!(!after_remove.contains("# fabric peers."));
        assert!(!after_remove.contains(&removed.to_string()));
    }

    #[test]
    fn adding_addressed_peers_keeps_a_valid_peer_file() {
        let directory = tempfile::tempdir().unwrap();
        let home = FabricHome::new(directory.path());
        let first = an_id(23);
        let second = an_id(24);
        let third = an_id(25);

        let mut book = PeerBook::load(&home).unwrap();
        book.add(
            first,
            Some("z-first".into()),
            Some(EndpointAddr::new(first).with_ip_addr("127.0.0.1:11204".parse().unwrap())),
        );
        book.save(&home).unwrap();
        let mut book = PeerBook::load(&home).unwrap();
        book.add(
            second,
            Some("a-second".into()),
            Some(EndpointAddr::new(second).with_ip_addr("127.0.0.1:11205".parse().unwrap())),
        );
        book.save(&home).unwrap();

        let raw = fs::read_to_string(home.peers_path()).unwrap();
        toml::from_str::<PeerBook>(&raw)
            .unwrap_or_else(|error| panic!("the updated peer file is invalid: {error}\n{raw}"));

        let mut book = PeerBook::load(&home).unwrap();
        book.add(
            first,
            Some("z-first".into()),
            Some(EndpointAddr::new(first).with_ip_addr("127.0.0.1:11206".parse().unwrap())),
        );
        book.save(&home).unwrap();
        PeerBook::load(&home).unwrap();

        let mut book = PeerBook::load(&home).unwrap();
        book.add(third, Some("third".into()), None);
        book.save(&home).unwrap();
        let mut book = PeerBook::load(&home).unwrap();
        book.add(
            third,
            Some("third".into()),
            Some(EndpointAddr::new(third).with_ip_addr("127.0.0.1:11207".parse().unwrap())),
        );
        book.save(&home).unwrap();
        PeerBook::load(&home).unwrap();

        let mut book = PeerBook::load(&home).unwrap();
        assert!(book.remove("z-first"));
        book.save(&home).unwrap();
        PeerBook::load(&home).unwrap();
    }

    #[test]
    fn generic_exposures_cannot_take_the_git_namespace() {
        let error = validate_protocol("git/mandat/read")
            .unwrap_err()
            .to_string();
        assert!(error.contains("reserved"), "wrong refusal: {error}");
    }

    /// A peer WITH a list is deny by default, including for services this
    /// machine exposes later. That is the case worth having: trusting somebody
    /// today must not hand them whatever you publish next month.
    #[test]
    fn a_peer_with_an_allow_list_is_denied_everything_else() {
        let mut book = PeerBook::default();
        let johannes = an_id(2);
        book.add_with_allow(
            johannes,
            Some("johannes".into()),
            None,
            Some(vec!["web".into()]),
        );
        assert_eq!(book.may(&johannes, "web"), Ok(()));
        assert_eq!(
            book.may(&johannes, "shell"),
            Err(Denied::NotPermitted {
                service: "shell".into()
            })
        );
        assert_eq!(
            book.may(&johannes, "exposed-tomorrow"),
            Err(Denied::NotPermitted {
                service: "exposed-tomorrow".into()
            })
        );
    }

    /// Not trusted and not permitted are DIFFERENT answers with different
    /// sentences. Somebody who cannot tell them apart cannot fix either.
    #[test]
    fn an_unknown_peer_is_not_trusted_rather_than_not_permitted() {
        let book = PeerBook::default();
        assert_eq!(book.may(&an_id(3), "web"), Err(Denied::NotTrusted));
        assert_eq!(
            Denied::NotTrusted.to_string(),
            "peer is not trusted by this node"
        );
        assert_eq!(
            Denied::NotPermitted {
                service: "web".into()
            }
            .to_string(),
            "peer not permitted for service \"web\""
        );
    }

    /// Re-adding a peer to update its address must not silently widen what it
    /// may reach. This is the shape of accident that grants access nobody meant
    /// to grant.
    #[test]
    fn re_adding_a_peer_keeps_its_permissions() {
        let mut book = PeerBook::default();
        let droppy = an_id(4);
        book.add_with_allow(
            droppy,
            Some("droppy".into()),
            None,
            Some(vec!["web".into()]),
        );
        book.peers[0].roaming = true;
        book.add(droppy, Some("droppy".into()), None);
        assert_eq!(
            book.may(&droppy, "shell"),
            Err(Denied::NotPermitted {
                service: "shell".into()
            }),
            "re-adding a peer widened its permissions"
        );
        assert!(
            book.peers()[0].roaming,
            "re-adding a peer erased its roaming setting"
        );
    }

    /// Policy keys on the id. Renaming a peer changes nothing about what it may
    /// reach, and the label is free to move.
    #[test]
    fn renaming_a_peer_does_not_change_what_it_may_reach() {
        let mut book = PeerBook::default();
        let peer = an_id(5);
        book.add_with_allow(peer, Some("old".into()), None, Some(vec!["web".into()]));
        book.add_with_allow(peer, Some("new".into()), None, Some(vec!["web".into()]));
        assert_eq!(book.may(&peer, "web"), Ok(()));
        assert_eq!(
            book.may(&peer, "shell"),
            Err(Denied::NotPermitted {
                service: "shell".into()
            })
        );
    }

    /// The refusing side's sentence and the dialling side's matcher have to
    /// agree, or a refusal reads as a network fault and a person waits forever
    /// for weather that is actually a chore.
    #[test]
    fn a_refusal_is_recognisable_from_the_other_side() {
        let denied = Denied::NotPermitted {
            service: "web".into(),
        };
        assert!(
            Denied::is_refusal(&denied.to_string()),
            "the sentence this node sends is not recognised by the matcher that \
             reads it"
        );
        // As it actually arrives, wrapped by the transport.
        assert!(Denied::is_refusal(
            "connection lost: closed by peer: peer not permitted for service \"web\" (code 403)"
        ));
        let no_grants = Denied::NoGrants {
            service: "web".into(),
        };
        assert!(
            Denied::is_refusal(&no_grants.to_string()),
            "the empty-grant sentence looks like a network fault: {no_grants}"
        );
        // Not everything is a refusal. A peer that is simply away must NOT be
        // reported as one, or a person goes looking for a permission problem
        // that does not exist.
        assert!(!Denied::is_refusal("connection timed out"));
        assert!(!Denied::is_refusal("no addresses for peer"));
        assert!(!Denied::is_refusal(&Denied::NotTrusted.to_string()));
    }

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

    #[test]
    fn peer_config_rejects_duplicate_node_ids() {
        let id = SecretKey::generate().public();
        let book: PeerBook = toml::from_str(&format!(
            "[[peers]]\nid = \"{id}\"\nname = \"first\"\n\n\
             [[peers]]\nid = \"{id}\"\nname = \"second\"\n"
        ))
        .unwrap();

        let error = book.validate().unwrap_err();

        assert!(
            format!("{error:#}").contains("duplicate peer id"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn peer_config_accepts_documented_address_hint_shape() {
        let id = SecretKey::generate().public();
        let book: PeerBook = toml::from_str(&format!(
            "[[peers]]\nid = \"{id}\"\nname = \"workstation\"\n\n\
             [peers.addr]\nid = \"{id}\"\n\n\
             [[peers.addr.addrs]]\nRelay = \"https://relay.example.com/\"\n\n\
             [[peers.addr.addrs]]\nIp = \"203.0.113.10:11204\"\n"
        ))
        .unwrap();

        book.validate().unwrap();
        assert_eq!(book.peers().len(), 1);
        assert_eq!(book.peers()[0].addr.as_ref().unwrap().id, id);
    }

    #[test]
    fn machine_service_settings_default_closed_and_round_trip() {
        let mut book = PeerBook::default();
        assert!(!book.allow_shell());
        assert!(!book.allow_exec());

        book.set_allow_shell(true);
        book.set_allow_exec(true);
        let raw = toml::to_string_pretty(&book).unwrap();
        assert!(raw.contains("allow_shell = true"));
        assert!(raw.contains("allow_exec = true"));

        let loaded: PeerBook = toml::from_str(&raw).unwrap();
        assert!(loaded.allow_shell());
        assert!(loaded.allow_exec());
    }

    #[test]
    fn roaming_peer_setting_survives_a_peer_file_round_trip() {
        let id = SecretKey::generate().public();
        let book: PeerBook = toml::from_str(&format!(
            "[[peers]]\nid = \"{id}\"\nname = \"laptop\"\nroaming = true\n"
        ))
        .unwrap();

        let raw = toml::to_string_pretty(&book).unwrap();

        assert!(
            raw.contains("roaming = true"),
            "saving peers.toml dropped the roaming contract: {raw}"
        );
    }

    #[test]
    fn server_session_config_uses_defaults_when_missing() {
        let config: FabricConfig = toml::from_str("").unwrap();

        config.validate().unwrap();

        assert_eq!(
            config.server_sessions().max_total(),
            DEFAULT_SERVER_SESSION_MAX_TOTAL
        );
        assert_eq!(
            config.server_sessions().max_per_peer(),
            DEFAULT_SERVER_SESSION_MAX_PER_PEER
        );
        assert_eq!(
            config.server_sessions().detached_ttl_secs(),
            DEFAULT_SERVER_SESSION_DETACHED_TTL_SECS
        );
    }

    /// The retention window is a product decision, so pin the number itself.
    ///
    /// A default that drifts silently is worse than one that is wrong on purpose:
    /// this is what a user's held shell survives, and it was chosen against
    /// measured cost. Changing it should require changing this assertion and
    /// saying why.
    #[test]
    fn detached_retention_default_is_fifteen_minutes() {
        assert_eq!(
            DEFAULT_SERVER_SESSION_DETACHED_TTL_SECS, 900,
            "detached-shell retention is a decided product value, not an incidental one"
        );
        assert_eq!(
            ServerSessionConfig::default().detached_ttl_secs(),
            DEFAULT_SERVER_SESSION_DETACHED_TTL_SECS,
            "the default config must carry the decided value"
        );
    }

    #[test]
    fn server_session_config_accepts_custom_caps() {
        let config: FabricConfig = toml::from_str(
            r#"
            [server_sessions]
            max_total = 10
            max_per_peer = 3
            detached_ttl_secs = 30
            "#,
        )
        .unwrap();

        config.validate().unwrap();

        assert_eq!(config.server_sessions().max_total(), 10);
        assert_eq!(config.server_sessions().max_per_peer(), 3);
        assert_eq!(config.server_sessions().detached_ttl_secs(), 30);
    }

    #[test]
    fn server_session_config_rejects_invalid_caps() {
        let config: FabricConfig = toml::from_str(
            r#"
            [server_sessions]
            max_total = 2
            max_per_peer = 3
            "#,
        )
        .unwrap();

        let error = config.validate().unwrap_err();

        assert!(
            format!("{error:#}").contains("max_per_peer cannot exceed"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn server_session_config_rejects_zero_detached_ttl() {
        let config: FabricConfig = toml::from_str(
            r#"
            [server_sessions]
            detached_ttl_secs = 0
            "#,
        )
        .unwrap();

        let error = config.validate().unwrap_err();

        assert!(
            format!("{error:#}").contains("detached_ttl_secs must be greater than zero"),
            "unexpected error: {error:#}"
        );
    }
}
