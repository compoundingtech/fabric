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

pub const DEFAULT_EXEC_MAX_CHILDREN: usize = 32;
pub const DEFAULT_SERVER_SESSION_MAX_TOTAL: usize = 64;
pub const DEFAULT_SERVER_SESSION_MAX_PER_PEER: usize = 16;
pub const DEFAULT_SERVER_SESSION_DETACHED_TTL_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct FabricHome {
    root: PathBuf,
    peer_config_path: PathBuf,
    legacy_peer_config_path: Option<PathBuf>,
}

impl FabricHome {
    pub fn resolve(home: Option<PathBuf>) -> Result<Self> {
        if let Some(root) = home {
            return Ok(Self::new(root));
        }
        if let Some(root) = env::var_os("FABRIC_HOME") {
            return Ok(Self::new(root));
        }
        let home = env::var_os("HOME").context("HOME is not set; pass --home or FABRIC_HOME")?;
        let home = PathBuf::from(home);
        let root = home.join(".local/share/fabric");
        let config_root = env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"));
        Ok(Self {
            peer_config_path: config_root.join("fabric/peers.toml"),
            legacy_peer_config_path: Some(root.join("peers.toml")),
            root,
        })
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

    pub fn config_path(&self) -> PathBuf {
        self.root.join("config.toml")
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

    fn remove_legacy_peer_configs(&self) -> Result<()> {
        let mut paths = vec![self.peer_config_path.clone()];
        if let Some(path) = &self.legacy_peer_config_path
            && path != &self.peer_config_path
        {
            paths.push(path.clone());
        }

        for path in paths {
            if path.exists() {
                fs::remove_file(&path)
                    .with_context(|| format!("failed to remove {}", path.display()))?;
            }
        }
        Ok(())
    }

    pub fn control_socket_path(&self) -> PathBuf {
        self.root.join("run/control.sock")
    }

    pub fn log_path(&self) -> PathBuf {
        self.root.join("logs/daemon.log")
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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PeerBook {
    peers: Vec<Peer>,
}

impl PeerBook {
    pub fn load(home: &FabricHome) -> Result<Self> {
        if home.config_path().exists() {
            let mut config = FabricConfig::load(home)?;
            if !config.peers.is_empty() {
                return Ok(Self {
                    peers: config.peers,
                });
            }

            let Some(book) = Self::load_existing(home)? else {
                return Ok(Self::default());
            };
            config.peers = book.peers.clone();
            config.save(home)?;
            home.remove_legacy_peer_configs()?;
            return Ok(Self { peers: book.peers });
        }

        Ok(Self::load_existing(home)?.unwrap_or_default())
    }

    fn load_existing(home: &FabricHome) -> Result<Option<Self>> {
        let Some(path) = home.existing_peers_path() else {
            return Ok(None);
        };
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let book: Self =
            toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
        book.validate()?;
        Ok(Some(book))
    }

    pub fn save(&self, home: &FabricHome) -> Result<()> {
        home.prepare()?;
        self.validate()?;
        let mut config = FabricConfig::load(home)?;
        config.peers = self.peers.clone();
        config.save(home)?;
        home.remove_legacy_peer_configs()?;
        Ok(())
    }

    pub fn peers(&self) -> &[Peer] {
        &self.peers
    }

    pub fn trusted_ids(&self) -> HashSet<EndpointId> {
        self.peers.iter().map(|peer| peer.id).collect()
    }

    pub fn add(&mut self, id: EndpointId, name: Option<String>, addr: Option<EndpointAddr>) {
        self.peers.retain(|peer| peer.id != id);
        if let Some(name) = &name {
            self.peers
                .retain(|peer| peer.name.as_deref() != Some(name.as_str()));
        }
        self.peers.push(Peer { id, name, addr });
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
            [] => bail!("unknown peer {peer:?}; add it with `fabric add <nodeid> [name]`"),
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
        let mut names = HashMap::new();
        for peer in &self.peers {
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
        Ok(())
    }
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
    server_sessions: ServerSessionConfig,
    #[serde(default)]
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

    pub fn server_sessions(&self) -> &ServerSessionConfig {
        &self.server_sessions
    }

    pub fn set_allow_shell(&mut self, allow_shell: bool) {
        self.allow_shell = Some(allow_shell);
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
            peers: self.peers.clone(),
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
