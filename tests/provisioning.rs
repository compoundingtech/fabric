use std::{fs, process::Command};

use anyhow::{Context, Result, bail};
use tempfile::TempDir;

fn fabric_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric")
}

fn stdout(output: std::process::Output) -> Result<String> {
    if !output.status.success() {
        bail!(
            "command failed with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

#[test]
fn key_gen_writes_identity_consumed_by_id() -> Result<()> {
    let temp = TempDir::new()?;
    let key_path = temp.path().join("box-key.toml");

    let node_id = stdout(
        Command::new(fabric_bin())
            .args(["key", "gen", "--out"])
            .arg(&key_path)
            .output()
            .context("failed to run fabric key gen")?,
    )?;
    assert!(!node_id.is_empty());

    let home = temp.path().join("home");
    fs::create_dir_all(&home)?;
    fs::copy(&key_path, home.join("identity.toml"))?;

    let reported_id = stdout(
        Command::new(fabric_bin())
            .arg("--home")
            .arg(&home)
            .arg("id")
            .output()
            .context("failed to run fabric id")?,
    )?;
    assert_eq!(reported_id, node_id);
    Ok(())
}

#[test]
fn version_flag_prints_semver_and_build_sha() -> Result<()> {
    let version = stdout(
        Command::new(fabric_bin())
            .arg("--version")
            .output()
            .context("failed to run fabric --version")?,
    )?;
    let prefix = format!("{}+", env!("CARGO_PKG_VERSION"));
    assert!(
        version.starts_with(&prefix),
        "version {version:?} did not start with {prefix:?}"
    );
    assert!(version.len() > prefix.len());
    Ok(())
}

#[test]
fn service_help_lists_user_service_lifecycle_commands() -> Result<()> {
    let help = stdout(
        Command::new(fabric_bin())
            .args(["service", "--help"])
            .output()
            .context("failed to run fabric service --help")?,
    )?;
    assert!(help.contains("install"));
    assert!(help.contains("status"));
    assert!(help.contains("uninstall"));
    Ok(())
}

#[test]
fn top_level_help_lists_declarative_peer_reload() -> Result<()> {
    let help = stdout(
        Command::new(fabric_bin())
            .arg("--help")
            .output()
            .context("failed to run fabric --help")?,
    )?;
    assert!(help.contains("reload-peers"));
    Ok(())
}

#[test]
fn peers_lists_declarative_config_without_add() -> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    fs::create_dir_all(&home)?;

    let peer_key = temp.path().join("peer-key.toml");
    let peer_id = stdout(
        Command::new(fabric_bin())
            .args(["key", "gen", "--out"])
            .arg(&peer_key)
            .output()
            .context("failed to run fabric key gen")?,
    )?;
    fs::write(
        home.join("peers.toml"),
        format!("[[peers]]\nid = \"{peer_id}\"\nname = \"box-a\"\n"),
    )?;

    let peers = stdout(
        Command::new(fabric_bin())
            .arg("--home")
            .arg(&home)
            .arg("peers")
            .output()
            .context("failed to run fabric peers")?,
    )?;
    assert_eq!(
        peers,
        format!("machine\tshell=disabled\texec=disabled\n{peer_id}\tbox-a\tno services")
    );
    Ok(())
}

#[test]
fn git_shares_and_peer_grants_live_in_peers_toml() -> Result<()> {
    let temp = TempDir::new()?;
    let home = temp.path().join("home");
    let repository = temp.path().join("mandat.git");
    fs::create_dir_all(&home)?;
    stdout(
        Command::new("git")
            .args(["init", "--bare"])
            .arg(&repository)
            .output()
            .context("failed to create the bare Git repository")?,
    )?;

    let peer_key = temp.path().join("peer-key.toml");
    let peer_id = stdout(
        Command::new(fabric_bin())
            .args(["key", "gen", "--out"])
            .arg(&peer_key)
            .output()?,
    )?;
    stdout(
        Command::new(fabric_bin())
            .arg("--home")
            .arg(&home)
            .args(["add", &peer_id, "friend", "--allow", "shell"])
            .output()?,
    )?;

    let shared = stdout(
        Command::new(fabric_bin())
            .arg("--home")
            .arg(&home)
            .args(["git", "share", "mandat"])
            .arg(&repository)
            .output()?,
    )?;
    assert!(shared.contains("access\tno peers"));

    stdout(
        Command::new(fabric_bin())
            .arg("--home")
            .arg(&home)
            .args(["git", "grant", "mandat", "friend", "--read-write"])
            .output()?,
    )?;
    let listed = stdout(
        Command::new(fabric_bin())
            .arg("--home")
            .arg(&home)
            .args(["git", "ls"])
            .output()?,
    )?;
    assert!(listed.contains("mandat"));
    assert!(listed.contains("bare"));
    assert!(listed.contains("read=friend"));
    assert!(listed.contains("write=friend"));

    stdout(
        Command::new(fabric_bin())
            .arg("--home")
            .arg(&home)
            .args(["git", "revoke", "mandat", "friend", "--read"])
            .output()?,
    )?;
    let raw = fs::read_to_string(home.join("peers.toml"))?;
    assert!(
        raw.contains("[[git_remotes]]"),
        "the Git remote table header changed:\n{raw}"
    );
    assert!(raw.contains("git/mandat/write"));
    assert!(!raw.contains("git/mandat/read"));
    assert!(raw.contains("shell"));

    stdout(
        Command::new(fabric_bin())
            .arg("--home")
            .arg(&home)
            .args(["git", "unshare", "mandat"])
            .output()?,
    )?;
    let raw = fs::read_to_string(home.join("peers.toml"))?;
    assert!(!raw.contains("[[git_remotes]]"));
    assert!(!raw.contains("git/mandat/"));
    assert!(raw.contains("shell"));
    assert!(
        fs::read_dir(&home)?.all(|entry| {
            !entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("fabric-tmp")
        }),
        "an atomic peers.toml temporary file remained"
    );
    Ok(())
}

#[test]
fn default_home_reads_peers_from_config_dir() -> Result<()> {
    let temp = TempDir::new()?;
    let fake_home = temp.path().join("user-home");
    let config_dir = fake_home.join(".config/fabric");
    fs::create_dir_all(&config_dir)?;

    let peer_key = temp.path().join("peer-key.toml");
    let peer_id = stdout(
        Command::new(fabric_bin())
            .args(["key", "gen", "--out"])
            .arg(&peer_key)
            .output()
            .context("failed to run fabric key gen")?,
    )?;
    fs::write(
        config_dir.join("peers.toml"),
        format!("[[peers]]\nid = \"{peer_id}\"\nname = \"config-peer\"\n"),
    )?;

    let peers = stdout(
        Command::new(fabric_bin())
            .env("HOME", &fake_home)
            .env_remove("FABRIC_HOME")
            .env_remove("XDG_CONFIG_HOME")
            .arg("peers")
            .output()
            .context("failed to run fabric peers")?,
    )?;
    assert_eq!(
        peers,
        format!("machine\tshell=disabled\texec=disabled\n{peer_id}\tconfig-peer\tno services")
    );
    Ok(())
}

#[test]
fn default_home_moves_legacy_peer_file_to_config_dir() -> Result<()> {
    let temp = TempDir::new()?;
    let fake_home = temp.path().join("user-home");
    let fabric_home = fake_home.join(".local/share/fabric");
    fs::create_dir_all(&fabric_home)?;
    fs::write(fabric_home.join("config.toml"), "allow_shell = true\n")?;

    let peer_key = temp.path().join("peer-key.toml");
    let peer_id = stdout(
        Command::new(fabric_bin())
            .args(["key", "gen", "--out"])
            .arg(&peer_key)
            .output()
            .context("failed to run fabric key gen")?,
    )?;
    fs::write(
        fabric_home.join("peers.toml"),
        format!("[[peers]]\nid = \"{peer_id}\"\nname = \"legacy-peer\"\n"),
    )?;

    let peers = stdout(
        Command::new(fabric_bin())
            .env("HOME", &fake_home)
            .env_remove("FABRIC_HOME")
            .env_remove("XDG_CONFIG_HOME")
            .arg("peers")
            .output()
            .context("failed to run fabric peers")?,
    )?;
    assert_eq!(
        peers,
        format!("machine\tshell=disabled\texec=disabled\n{peer_id}\tlegacy-peer\tno services")
    );
    let migrated_config = fs::read_to_string(fabric_home.join("config.toml"))?;
    assert!(migrated_config.contains("allow_shell = true"));
    assert!(!migrated_config.contains("legacy-peer"));
    assert!(!fabric_home.join("peers.toml").exists());
    let migrated_peers = fs::read_to_string(fake_home.join(".config/fabric/peers.toml"))?;
    assert!(migrated_peers.contains("legacy-peer"));
    Ok(())
}

#[test]
fn default_home_moves_embedded_peers_to_authoritative_peer_file() -> Result<()> {
    let temp = TempDir::new()?;
    let fake_home = temp.path().join("user-home");
    let fabric_home = fake_home.join(".local/share/fabric");
    fs::create_dir_all(&fabric_home)?;

    let peer_key = temp.path().join("peer-key.toml");
    let peer_id = stdout(
        Command::new(fabric_bin())
            .args(["key", "gen", "--out"])
            .arg(&peer_key)
            .output()
            .context("failed to run fabric key gen")?,
    )?;
    fs::write(
        fabric_home.join("config.toml"),
        format!("allow_shell = true\n\n[[peers]]\nid = \"{peer_id}\"\nname = \"embedded-peer\"\n"),
    )?;

    let peers = stdout(
        Command::new(fabric_bin())
            .env("HOME", &fake_home)
            .env_remove("FABRIC_HOME")
            .env_remove("XDG_CONFIG_HOME")
            .arg("peers")
            .output()
            .context("failed to run fabric peers")?,
    )?;
    assert_eq!(
        peers,
        format!("machine\tshell=allowed\texec=disabled\n{peer_id}\tembedded-peer\tno services")
    );

    let migrated_config = fs::read_to_string(fabric_home.join("config.toml"))?;
    assert!(migrated_config.contains("allow_shell = true"));
    assert!(!migrated_config.contains("embedded-peer"));
    let migrated_peers = fs::read_to_string(fake_home.join(".config/fabric/peers.toml"))?;
    assert!(migrated_peers.contains("embedded-peer"));
    Ok(())
}

#[test]
fn default_home_peer_file_overrides_embedded_config_peers() -> Result<()> {
    let temp = TempDir::new()?;
    let fake_home = temp.path().join("user-home");
    let fabric_home = fake_home.join(".local/share/fabric");
    let config_dir = fake_home.join(".config/fabric");
    fs::create_dir_all(&fabric_home)?;
    fs::create_dir_all(&config_dir)?;

    let old_key = temp.path().join("old-key.toml");
    let old_id = stdout(
        Command::new(fabric_bin())
            .args(["key", "gen", "--out"])
            .arg(&old_key)
            .output()
            .context("failed to generate old peer key")?,
    )?;
    let new_key = temp.path().join("new-key.toml");
    let new_id = stdout(
        Command::new(fabric_bin())
            .args(["key", "gen", "--out"])
            .arg(&new_key)
            .output()
            .context("failed to generate new peer key")?,
    )?;
    fs::write(
        fabric_home.join("config.toml"),
        format!("[[peers]]\nid = \"{old_id}\"\nname = \"old-peer\"\n"),
    )?;
    fs::write(
        config_dir.join("peers.toml"),
        format!("[[peers]]\nid = \"{new_id}\"\nname = \"new-peer\"\n"),
    )?;

    let peers = stdout(
        Command::new(fabric_bin())
            .env("HOME", &fake_home)
            .env_remove("FABRIC_HOME")
            .env_remove("XDG_CONFIG_HOME")
            .arg("peers")
            .output()
            .context("failed to run fabric peers")?,
    )?;
    assert_eq!(
        peers,
        format!("machine\tshell=disabled\texec=disabled\n{new_id}\tnew-peer\tno services")
    );
    let migrated_config = fs::read_to_string(fabric_home.join("config.toml"))?;
    assert!(!migrated_config.contains("old-peer"));
    assert!(fs::read_to_string(config_dir.join("peers.toml"))?.contains("new-peer"));
    Ok(())
}

#[test]
fn readme_st2_sync_recipe_is_copy_pasteable_and_scoped() -> Result<()> {
    const RECIPE: &str = r#"ST2_CATALOG="${XDG_STATE_HOME:-$HOME/.local/state}/st2/default/catalog"

fabric sync add "$ST2_CATALOG" --name st2-declarations-default --peers "*" --policy catalog --include "_templates/**,agents/**/agent.kdl,plans/**"
fabric sync add "$ST2_CATALOG/agents" --name st2-bus-default --peers "*" --policy bus --include "**/resources/**,**/status""#;

    let readme = include_str!("../README.md");
    let (_, after_heading) = readme
        .split_once("### Sync an st2 catalog safely")
        .context("README is missing the st2 sync heading")?;
    let (section, _) = after_heading
        .split_once("\n## Declarative Peer Config")
        .context("README st2 sync section has no end boundary")?;

    assert!(
        section.contains(RECIPE),
        "README must contain the exact portable st2 recipe"
    );
    assert_eq!(
        section.matches("fabric sync add ").count(),
        2,
        "st2 must use exactly two positive-list sync entries"
    );
    for required in [
        "resources/inbox",
        "resources/archive",
        "resources/context",
        "resources/links",
        "MUST NEVER",
        "$ST2_CATALOG/pty",
        "sockets",
        "PIDs",
        "locks",
        "exec runtime state",
        "logs",
        "temporary, backup, and partial files",
        "Workspaces and hooks are provisioned separately",
        "Never add a hidden `_syncproof` fixture",
        "fabric sync reload",
        "fabric sync ls",
        "fabric status",
        "fabric ping <peer-name>",
        "st2 resource add",
        "st2 message archive",
    ] {
        assert!(
            section.contains(required),
            "README st2 section is missing safety/verification contract {required:?}"
        );
    }
    assert!(
        !section.contains("--include \"_syncproof"),
        "the retired hidden fixture must never be an include"
    );

    let temp = TempDir::new()?;
    let fake_home = temp.path().join("home");
    let xdg_state = temp.path().join("state");
    let xdg_config = temp.path().join("config");
    let catalog = xdg_state.join("st2/default/catalog");
    fs::create_dir_all(catalog.join("agents"))?;

    let bin_dir = std::path::Path::new(fabric_bin())
        .parent()
        .context("fabric test binary has no parent")?;
    let path = std::env::join_paths(std::iter::once(bin_dir.to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))?;
    stdout(
        Command::new("sh")
            .args(["-eu", "-c", RECIPE])
            .env("HOME", &fake_home)
            .env("XDG_STATE_HOME", &xdg_state)
            .env("XDG_CONFIG_HOME", &xdg_config)
            .env_remove("FABRIC_HOME")
            .env("PATH", path)
            .output()
            .context("failed to execute README st2 sync recipe")?,
    )?;

    let raw = fs::read_to_string(xdg_config.join("fabric/syncs.toml"))?;
    let parsed: toml::Value = toml::from_str(&raw)?;
    let entries = parsed
        .get("sync")
        .and_then(toml::Value::as_array)
        .context("generated syncs.toml has no sync entries")?;
    assert_eq!(entries.len(), 2);

    let declarations = entries
        .iter()
        .find(|entry| {
            entry.get("name").and_then(toml::Value::as_str) == Some("st2-declarations-default")
        })
        .context("missing declarations entry")?;
    assert_eq!(
        declarations.get("folder").and_then(toml::Value::as_str),
        catalog.to_str()
    );
    assert_eq!(
        declarations.get("peers").and_then(toml::Value::as_str),
        Some("*")
    );
    assert_eq!(
        declarations.get("policy").and_then(toml::Value::as_str),
        Some("catalog")
    );
    let declaration_includes = declarations
        .get("include")
        .and_then(toml::Value::as_array)
        .context("declarations entry has no includes")?
        .iter()
        .map(|value| {
            value
                .as_str()
                .context("declarations include is not a string")
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        declaration_includes,
        vec!["_templates/**", "agents/**/agent.kdl", "plans/**"]
    );

    let bus = entries
        .iter()
        .find(|entry| entry.get("name").and_then(toml::Value::as_str) == Some("st2-bus-default"))
        .context("missing bus entry")?;
    assert_eq!(
        bus.get("folder").and_then(toml::Value::as_str),
        catalog.join("agents").to_str()
    );
    assert_eq!(bus.get("peers").and_then(toml::Value::as_str), Some("*"));
    assert_eq!(bus.get("policy").and_then(toml::Value::as_str), Some("bus"));
    let bus_includes = bus
        .get("include")
        .and_then(toml::Value::as_array)
        .context("bus entry has no includes")?
        .iter()
        .map(|value| value.as_str().context("bus include is not a string"))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(bus_includes, vec!["**/resources/**", "**/status"]);
    Ok(())
}
