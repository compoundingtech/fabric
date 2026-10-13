//! The `fabric update` command's contract with whoever runs it.
//!
//! The module's own tests cover what it decides. These cover what a caller and a
//! script actually see: the refusals, and the exit codes a fleet sweep reads.

use std::process::Command;

use anyhow::{Context, Result};
use tempfile::TempDir;

fn fabric_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric")
}

#[test]
fn top_level_help_offers_update() -> Result<()> {
    let out = Command::new(fabric_bin())
        .arg("--help")
        .output()
        .context("failed to run fabric --help")?;
    let help = String::from_utf8(out.stdout)?;
    assert!(help.contains("update"), "update is missing from the help");
    Ok(())
}

/// The supervisor is scheduled by `update`, not run by hand, so it must not be
/// offered in the help. It still has to exist as a command.
#[test]
fn the_restart_supervisor_is_usable_but_not_advertised() -> Result<()> {
    let listed = String::from_utf8(
        Command::new(fabric_bin())
            .arg("--help")
            .output()
            .context("failed to run fabric --help")?
            .stdout,
    )?;
    assert!(
        !listed.contains("supervise-restart"),
        "an internal command is being advertised"
    );

    let own = Command::new(fabric_bin())
        .args(["supervise-restart", "--help"])
        .output()
        .context("failed to run fabric supervise-restart --help")?;
    assert!(own.status.success(), "the hidden command does not exist");
    Ok(())
}

/// The help has to state the exit contract, because the exit code is the whole
/// interface for anything scripting a fleet sweep.
#[test]
fn the_help_states_what_each_exit_code_means() -> Result<()> {
    let help = String::from_utf8(
        Command::new(fabric_bin())
            .args(["update", "--help"])
            .output()
            .context("failed to run fabric update --help")?
            .stdout,
    )?;
    for expected in ["0 up to date", "1 update available", "2 error"] {
        assert!(help.contains(expected), "the help never says {expected:?}");
    }
    Ok(())
}

/// The help must not let a hash read as more than it is. On the release paths
/// the checksum comes from the same server as the artifact.
#[test]
fn the_help_does_not_oversell_the_checksum() -> Result<()> {
    let help = String::from_utf8(
        Command::new(fabric_bin())
            .args(["update", "--help"])
            .output()
            .context("failed to run fabric update --help")?
            .stdout,
    )?;
    assert!(
        help.contains("not that they are trustworthy"),
        "the help implies the hash proves more than it does"
    );
    Ok(())
}

#[test]
fn the_help_names_the_explicit_downgrade_override() -> Result<()> {
    let help = String::from_utf8(
        Command::new(fabric_bin())
            .args(["update", "--help"])
            .output()
            .context("failed to run fabric update --help")?
            .stdout,
    )?;
    assert!(help.contains("--allow-downgrade"));
    assert!(help.contains("older than or diverges"));
    Ok(())
}

/// The refusal that matters most, seen the way a caller sees it.
#[test]
fn an_explicit_url_without_a_hash_is_refused_and_says_why() -> Result<()> {
    let temp = TempDir::new()?;
    let artifact = temp.path().join("fabric.tar.gz");
    std::fs::write(&artifact, b"not really an archive")?;

    let out = Command::new(fabric_bin())
        .args(["update", "--url"])
        .arg(format!("file://{}", artifact.display()))
        .output()
        .context("failed to run fabric update")?;

    assert!(!out.status.success(), "an unverified url was accepted");
    let stderr = String::from_utf8(out.stderr)?;
    assert!(
        stderr.contains("--sha256"),
        "the refusal does not name what is missing: {stderr}"
    );
    Ok(())
}

/// Two sources at once is a mistake worth naming rather than resolving by
/// precedence, because guessing which one the caller meant is worse than asking.
#[test]
fn naming_a_tag_and_a_url_together_is_refused() -> Result<()> {
    let out = Command::new(fabric_bin())
        .args([
            "update",
            "--tag",
            "v0.2.0+deadbee",
            "--url",
            "file:///tmp/nope.tar.gz",
            "--sha256",
            "e6aac12fcf8be256aa713a017cfcd8d4e258f5f9f42e5bf8911ff189b73a1214",
        ])
        .output()
        .context("failed to run fabric update")?;
    assert!(!out.status.success(), "two sources were accepted at once");
    Ok(())
}

/// `--rollback` names no source, so combining it with one is a contradiction the
/// parser should catch before anything runs.
#[test]
fn rollback_cannot_be_combined_with_a_source() -> Result<()> {
    let out = Command::new(fabric_bin())
        .args(["update", "--rollback", "--tag", "v0.2.0+deadbee"])
        .output()
        .context("failed to run fabric update")?;
    assert!(
        !out.status.success(),
        "--rollback and --tag were accepted together"
    );
    Ok(())
}

/// Everything an explicit-URL update decides, run through the real command
/// against a copy of the binary in a scratch directory. The service manager is
/// hidden (no `systemctl` on `PATH`, `HOME` in the scratch directory), so the
/// binary being replaced is the copy and never an installed one. `--dry-run`
/// stops after the checks, so nothing is replaced even when an update is allowed.
mod explicit_url {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{os::unix::fs::PermissionsExt, path::PathBuf};

    struct Sandbox {
        _temp: TempDir,
        bin_dir: PathBuf,
        empty_path: PathBuf,
        installed: PathBuf,
        installed_version: String,
        installed_bytes: Vec<u8>,
        archives: PathBuf,
    }

    impl Sandbox {
        fn new() -> Result<Self> {
            let temp = TempDir::new()?;
            let bin_dir = temp.path().join("bin");
            std::fs::create_dir_all(&bin_dir)?;
            let empty_path = temp.path().join("empty");
            std::fs::create_dir_all(&empty_path)?;
            let archives = temp.path().join("archives");
            std::fs::create_dir_all(&archives)?;
            let installed = bin_dir.join("fabric");
            std::fs::copy(fabric_bin(), &installed)?;
            let installed_version = fabric::version_string();
            let installed_bytes = std::fs::read(&installed)?;
            Ok(Self {
                _temp: temp,
                bin_dir,
                empty_path,
                installed,
                installed_version,
                installed_bytes,
                archives,
            })
        }

        /// A release archive whose `fabric` is a script that reports `version`.
        fn archive_reporting(&self, version: &str) -> Result<(String, String)> {
            let script = format!("#!/bin/sh\necho \"{version}\"\n");
            let mut tar_bytes = Vec::new();
            {
                let mut tar = tar::Builder::new(&mut tar_bytes);
                let mut header = tar::Header::new_gnu();
                header.set_size(script.len() as u64);
                header.set_mode(0o755);
                header.set_cksum();
                tar.append_data(&mut header, "fabric", script.as_bytes())?;
                tar.finish()?;
            }
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            std::io::Write::write_all(&mut gz, &tar_bytes)?;
            let archive = gz.finish()?;
            let path = self.archives.join(format!("{}.tar.gz", archive.len()));
            std::fs::write(&path, &archive)?;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))?;
            let hash = Sha256::digest(&archive)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            Ok((format!("file://{}", path.display()), hash))
        }

        fn update(&self, version: &str, extra: &[&str]) -> Result<std::process::Output> {
            let (url, hash) = self.archive_reporting(version)?;
            Command::new(&self.installed)
                .env("PATH", &self.empty_path)
                .env("HOME", self.bin_dir.parent().unwrap())
                .args(["--home"])
                .arg(self.bin_dir.parent().unwrap().join("home"))
                .args(["update", "--dry-run", "--url", &url, "--sha256", &hash])
                .args(extra)
                .output()
                .context("failed to run fabric update")
        }

        /// Nothing installed changed, and nothing was left beside it.
        fn assert_untouched(&self) -> Result<()> {
            assert_eq!(std::fs::read(&self.installed)?, self.installed_bytes);
            let names: Vec<_> = std::fs::read_dir(&self.bin_dir)?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<std::io::Result<_>>()?;
            assert_eq!(names, vec![std::ffi::OsString::from("fabric")]);
            Ok(())
        }
    }

    fn shown(out: &std::process::Output) -> String {
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    }

    #[test]
    fn a_newer_build_is_accepted() -> Result<()> {
        let sandbox = Sandbox::new()?;
        let out = sandbox.update("99.0.0+abcdef0", &[])?;
        assert!(out.status.success(), "{}", shown(&out));
        assert!(shown(&out).contains("DRY RUN"), "{}", shown(&out));
        sandbox.assert_untouched()
    }

    #[test]
    fn an_older_build_is_refused_and_leaves_the_install_alone() -> Result<()> {
        let sandbox = Sandbox::new()?;
        let out = sandbox.update("0.0.1+abcdef0", &[])?;
        assert!(!out.status.success(), "an older build was accepted");
        let said = shown(&out);
        assert!(said.contains("is older than"), "{said}");
        assert!(said.contains("--allow-downgrade"), "{said}");
        assert!(said.contains("nothing was installed"), "{said}");
        sandbox.assert_untouched()?;

        let out = sandbox.update("0.0.1+abcdef0", &["--allow-downgrade"])?;
        assert!(out.status.success(), "{}", shown(&out));
        assert!(shown(&out).contains("WARNING"), "{}", shown(&out));
        sandbox.assert_untouched()
    }

    #[test]
    fn the_same_build_is_refused_unless_it_is_allowed() -> Result<()> {
        let sandbox = Sandbox::new()?;
        let out = sandbox.update(&sandbox.installed_version.clone(), &[])?;
        assert!(!out.status.success(), "the installed build was reinstalled");
        assert!(shown(&out).contains("already installed"), "{}", shown(&out));
        sandbox.assert_untouched()?;

        let out = sandbox.update(&sandbox.installed_version.clone(), &["--allow-downgrade"])?;
        assert!(out.status.success(), "{}", shown(&out));
        sandbox.assert_untouched()
    }

    #[test]
    fn expect_version_must_match_the_artifact() -> Result<()> {
        let sandbox = Sandbox::new()?;
        let out = sandbox.update("99.0.0+abcdef0", &["--expect-version", "99.0.0+abcdef0"])?;
        assert!(out.status.success(), "{}", shown(&out));

        let out = sandbox.update("99.0.0+abcdef0", &["--expect-version", "99.0.0+1111111"])?;
        assert!(!out.status.success(), "a different build was accepted");
        let said = shown(&out);
        assert!(said.contains("--expect-version"), "{said}");
        assert!(
            said.contains("99.0.0+abcdef0") && said.contains("99.0.0+1111111"),
            "{said}"
        );
        sandbox.assert_untouched()?;

        // Allowing a downgrade never waives an exact match.
        let out = sandbox.update(
            "99.0.0+abcdef0",
            &["--expect-version", "99.0.0+1111111", "--allow-downgrade"],
        )?;
        assert!(!out.status.success());
        sandbox.assert_untouched()
    }
}
