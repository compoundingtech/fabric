use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, bail};
use iroh::{EndpointAddr, EndpointId, SecretKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct FabricHome {
    root: PathBuf,
}

impl FabricHome {
    pub fn resolve(home: Option<PathBuf>) -> Result<Self> {
        if let Some(root) = home {
            return Ok(Self { root });
        }
        if let Some(root) = env::var_os("FABRIC_HOME") {
            return Ok(Self { root: root.into() });
        }
        let home = env::var_os("HOME").context("HOME is not set; pass --home or FABRIC_HOME")?;
        Ok(Self {
            root: PathBuf::from(home).join(".local/share/fabric"),
        })
    }

    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn prepare(&self) -> Result<()> {
        fs::create_dir_all(self.root.join("run"))?;
        fs::create_dir_all(self.root.join("dials"))?;
        fs::create_dir_all(self.root.join("logs"))?;
        Ok(())
    }

    pub fn identity_path(&self) -> PathBuf {
        self.root.join("identity.toml")
    }

    pub fn peers_path(&self) -> PathBuf {
        self.root.join("peers.toml")
    }

    pub fn control_socket_path(&self) -> PathBuf {
        self.root.join("run/control.sock")
    }

    pub fn log_path(&self) -> PathBuf {
        self.root.join("logs/daemon.log")
    }

    pub fn dial_socket_path(&self, peer: EndpointId, protocol: &str) -> PathBuf {
        let peer = peer.to_string();
        let short_peer = &peer[..peer.len().min(12)];
        self.root
            .join("dials")
            .join(format!("{}-{}.sock", short_peer, safe_component(protocol)))
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

    let file = IdentityFile {
        secret_key: SecretKey::generate(),
    };
    let raw = toml::to_string_pretty(&file)?;
    write_secret_file(&path, raw.as_bytes())?;
    Ok(file.secret_key)
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
        let path = home.peers_path();
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
        fs::write(home.peers_path(), raw)?;
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

fn safe_component(input: &str) -> String {
    let out: String = input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() {
        "protocol".to_string()
    } else {
        out
    }
}
