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
    assert_eq!(peers, format!("{peer_id}\tbox-a"));
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
    assert_eq!(peers, format!("{peer_id}\tconfig-peer"));
    Ok(())
}

#[test]
fn default_home_migrates_legacy_peers_when_config_exists() -> Result<()> {
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
    assert_eq!(peers, format!("{peer_id}\tlegacy-peer"));
    let migrated = fs::read_to_string(fabric_home.join("config.toml"))?;
    assert!(migrated.contains("allow_shell = true"));
    assert!(migrated.contains("legacy-peer"));
    assert!(!fabric_home.join("peers.toml").exists());
    Ok(())
}
