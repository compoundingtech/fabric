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
