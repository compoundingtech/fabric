//! The error contract in docs/errors.md, as a program sees it: the exit
//! status, the one-line `--json` object on stdout and the token on stderr.

use std::process::{Command, Output};

use anyhow::{Context, Result};
use fabric::{config::FabricHome, daemon::FabricNode};
use serde_json::Value;
use tempfile::TempDir;

fn fabric(home: &TempDir, args: &[&str]) -> Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_fabric"))
        .arg("--home")
        .arg(home.path())
        .args(args)
        .output()
        .with_context(|| format!("failed to run fabric {args:?}"))
}

/// The error object a failing `--json` command printed, after checking it is
/// the only line on stdout and the stderr line carries the same code.
fn error_object(output: &Output) -> Result<Value> {
    let stdout = String::from_utf8(output.stdout.clone())?;
    let stderr = String::from_utf8(output.stderr.clone())?;
    assert_eq!(stdout.lines().count(), 1, "stdout: {stdout:?}");
    let object: Value = serde_json::from_str(&stdout)?;
    let error = &object["error"];
    for key in ["code", "class", "message", "peer", "service"] {
        assert!(error.get(key).is_some(), "no {key} in {object}");
    }
    let code = error["code"].as_str().context("code is not a string")?;
    assert!(
        stderr.contains(&format!("Error: fabric:{code} ")),
        "stderr does not carry {code}: {stderr:?}"
    );
    Ok(error.clone())
}

#[test]
fn a_command_without_a_daemon_reports_daemon_not_running() -> Result<()> {
    let home = TempDir::new()?;
    let output = fabric(&home, &["--json", "dial", "box", "web"])?;
    assert_eq!(output.status.code(), Some(69));
    let error = error_object(&output)?;
    assert_eq!(error["code"], "daemon_not_running");
    assert_eq!(error["class"], "fault");

    let output = fabric(&home, &["dial", "box", "web"])?;
    assert_eq!(output.status.code(), Some(69));
    assert!(output.stdout.is_empty(), "stdout without --json");
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("Error: fabric:daemon_not_running fabric daemon is not running"),
        "{stderr}"
    );
    Ok(())
}

#[test]
fn json_is_accepted_after_the_command_too() -> Result<()> {
    let home = TempDir::new()?;
    let output = fabric(&home, &["dial", "box", "web", "--json"])?;
    assert_eq!(output.status.code(), Some(69));
    assert_eq!(error_object(&output)?["code"], "daemon_not_running");
    Ok(())
}

#[test]
fn probe_keeps_its_exit_status_and_reports_the_code() -> Result<()> {
    let home = TempDir::new()?;
    let output = fabric(&home, &["--json", "probe", "box", "web"])?;
    assert_eq!(output.status.code(), Some(64));
    assert_eq!(error_object(&output)?["code"], "daemon_not_running");

    let output = fabric(&home, &["--json", "probe", "box", "web", "--timeout", "0"])?;
    assert_eq!(output.status.code(), Some(64));
    let error = error_object(&output)?;
    assert_eq!(error["code"], "invalid_argument");
    assert_eq!(error["class"], "usage");
    Ok(())
}

#[test]
fn arguments_fabric_rejects_are_usage_errors() -> Result<()> {
    let home = TempDir::new()?;
    for args in [
        &["--json", "add", "not-a-node-id"][..],
        &["--json", "expose", "web"],
        &["--json", "expose", "web", "--exec"],
    ] {
        let output = fabric(&home, args)?;
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        let error = error_object(&output)?;
        assert_eq!(error["code"], "invalid_argument", "{args:?}");
        assert_eq!(error["class"], "usage", "{args:?}");
    }
    Ok(())
}

#[test]
fn a_clap_usage_error_prints_no_json() -> Result<()> {
    let home = TempDir::new()?;
    let output = fabric(&home, &["--json", "dial", "box"])?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_daemons_code_and_peer_reach_the_command() -> Result<()> {
    let home = TempDir::new()?;
    let node = FabricNode::start(FabricHome::new(home.path())).await?;
    let path = home.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_fabric"))
            .arg("--home")
            .arg(path)
            .args(["--json", "dial", "ghost", "web"])
            .output()
    })
    .await??;
    node.shutdown().await?;
    assert_eq!(output.status.code(), Some(69));
    let error = error_object(&output)?;
    assert_eq!(error["code"], "unknown_peer");
    assert_eq!(error["peer"], "ghost");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("unknown peer \"ghost\""),
        "{error}"
    );
    Ok(())
}
