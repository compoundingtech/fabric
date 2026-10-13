//! The error contract in docs/errors.md, as a program sees it: the exit
//! status, the one-line `--json` object on stdout and the token on stderr.

use std::process::{Command, Output};

use anyhow::{Context, Result};
use fabric::{
    config::{FabricHome, PeerBook},
    daemon::FabricNode,
};
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

/// Trust `peer` in `home`, granting exactly `allow`, and reload.
async fn trust(
    home: &TempDir,
    node: &FabricNode,
    peer: &FabricNode,
    name: &str,
    allow: &[&str],
) -> Result<()> {
    let home = FabricHome::new(home.path());
    let mut peers = PeerBook::load(&home)?;
    peers.add_with_allow(
        peer.id(),
        Some(name.to_string()),
        Some(peer.addr()),
        Some(allow.iter().map(|service| (*service).to_string()).collect()),
    );
    peers.save(&home)?;
    node.state().reload_peers().await
}

/// A probe answer names the code the far daemon chose: a refusal for a
/// service it does not grant, `service_not_exposed` for one nobody exposes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_probe_answer_names_the_far_sides_code() -> Result<()> {
    let server_home = TempDir::new()?;
    let client_home = TempDir::new()?;
    let server = FabricNode::start(FabricHome::new(server_home.path())).await?;
    let client = FabricNode::start(FabricHome::new(client_home.path())).await?;
    let socket = server_home.path().join("web.sock");
    let _listener = tokio::net::UnixListener::bind(&socket)?;
    server.expose("web", socket).await?;
    trust(&server_home, &server, &client, "client", &["echo"]).await?;
    trust(&client_home, &client, &server, "server", &[]).await?;
    // A machine the server does not trust at all.
    let stranger_home = TempDir::new()?;
    let stranger = FabricNode::start(FabricHome::new(stranger_home.path())).await?;
    trust(&stranger_home, &stranger, &server, "server", &[]).await?;

    let mut answers = Vec::new();
    for (home, protocol) in [
        (&client_home, "web"),
        (&client_home, "nothing-here"),
        (&stranger_home, "web"),
    ] {
        let path = home.path().to_path_buf();
        let output = tokio::task::spawn_blocking(move || {
            Command::new(env!("CARGO_BIN_EXE_fabric"))
                .arg("--home")
                .arg(path)
                .args(["--json", "probe", "server", protocol, "--timeout", "10"])
                .output()
        })
        .await??;
        let answer: Value = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("probe {protocol}: {output:?}"))?;
        answers.push((output.status.code(), answer));
    }
    stranger.shutdown().await?;
    client.shutdown().await?;
    server.shutdown().await?;

    // A refusal has always been answered `unreachable`; its code is what
    // tells it from a network fault. The refusal crossed the shared daemon
    // connection as a stream denial.
    let (status, refused) = &answers[0];
    assert_eq!(*status, Some(2), "{refused}");
    assert_eq!(refused["outcome"], "unreachable", "{refused}");
    assert_eq!(
        refused["code"], "peer_not_permitted_for_service",
        "{refused}"
    );
    assert_eq!(refused["class"], "refusal", "{refused}");
    let (status, missing) = &answers[1];
    assert_eq!(*status, Some(1), "{missing}");
    assert_eq!(missing["code"], "service_not_exposed", "{missing}");
    assert_eq!(missing["class"], "fault", "{missing}");
    let (status, untrusted) = &answers[2];
    assert_eq!(*status, Some(2), "{untrusted}");
    assert_eq!(untrusted["code"], "peer_not_in_allow_list", "{untrusted}");
    assert_eq!(untrusted["class"], "refusal", "{untrusted}");
    Ok(())
}
