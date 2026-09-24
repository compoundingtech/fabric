//! The few sync definitions the core shares with the engine crate.
//!
//! Everything here is pure or a one-file write: a content identity, the path
//! form a manifest keys on, the name form a state directory uses, and an atomic
//! file write. The engine, the companion, and the staging commands agree on
//! these; nothing here knows about manifests, nodes, or peers.

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// A content identity: the BLAKE3 hash of a file's bytes. Two files with the
/// same `ContentHash` have identical content (used for transfer dedup).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    /// Parse the 64-character form that `to_hex` writes. `None` for any other
    /// length or a non-hex character.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 || !hex.is_ascii() {
            return None;
        }
        let mut out = [0u8; 32];
        for (index, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let pair = std::str::from_utf8(chunk).ok()?;
            out[index] = u8::from_str_radix(pair, 16).ok()?;
        }
        Some(Self(out))
    }

    pub fn to_hex(self) -> String {
        let mut s = String::with_capacity(64);
        for byte in self.0 {
            s.push_str(&format!("{byte:02x}"));
        }
        s
    }
}

/// The content identity of `bytes`.
pub fn content_hash(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

/// The portable, forward-slash, relative form a manifest keys a path on.
/// `None` for an absolute path, a path that climbs out, or an empty one.
pub fn normalize_path(path: &str) -> Option<String> {
    if path.starts_with('/') || path.starts_with('\\') {
        return None;
    }
    let mut parts = Vec::new();
    for part in path.split(['/', '\\']) {
        match part {
            "" | "." => continue,
            ".." => return None,
            other => parts.push(other),
        }
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join("/"))
}

/// An entry name as a directory name: alphanumerics, `-` and `_` kept,
/// everything else replaced.
pub fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Write bytes to `path` atomically, via a temporary file and a rename, with
/// the executable bit set first when asked so the file never appears at its
/// final path with the wrong mode.
pub fn write_atomic_with_mode(path: &Path, bytes: &[u8], executable: bool) -> Result<()> {
    let tmp = path.with_extension(format!(
        "{}.fabric-tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("")
    ));
    std::fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    // A failed chmod must not fail the write. The content is what the sync
    // is for, and a non-executable copy is recoverable by hand; a lost file
    // is not.
    if executable && let Err(error) = set_executable(&tmp) {
        eprintln!(
            "fabric: could not set the executable bit on {}: {error:#}",
            tmp.display()
        );
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to rename into {}", path.display()))?;
    Ok(())
}

/// Set the executable bits the way git tracks them: OR in `0o111`.
pub fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?
        .permissions();
    let mode = perms.mode();
    perms.set_mode(mode | 0o111);
    std::fs::set_permissions(path, perms)
        .with_context(|| format!("failed to set the executable bit on {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips_and_rejects_the_wrong_length() {
        let hash = content_hash(b"bytes");
        assert_eq!(ContentHash::from_hex(&hash.to_hex()), Some(hash));
        assert_eq!(ContentHash::from_hex("abc"), None);
    }

    #[test]
    fn paths_normalize_to_relative_forward_slash_form() {
        assert_eq!(normalize_path("a\\b/./c"), Some("a/b/c".into()));
        assert_eq!(normalize_path("/abs"), None);
        assert_eq!(normalize_path("a/../b"), None);
        assert_eq!(normalize_path(""), None);
    }
}
