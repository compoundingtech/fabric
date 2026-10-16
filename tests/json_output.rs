//! Golden tests for `--json`: the exact shape of what each command prints.
//!
//! These objects are what other programs parse, so a key that moves or a type
//! that changes must fail here, in the pull request that changes it. Values
//! that differ from run to run (node ids, paths, ports, timings) are replaced
//! with a placeholder before the comparison; keys and types are never
//! normalised. To accept an intended change, run with `UPDATE_GOLDEN=1` and
//! review the diff in `tests/golden/json/`.

use std::{fs, path::PathBuf, process::Command, time::Duration};

use anyhow::{Context, Result, bail};
use fabric::{
    config::{FabricHome, PeerBook},
    daemon::FabricNode,
};
use serde_json::Value;
use tempfile::TempDir;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(20);

fn fabric_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric")
}

fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden/json")
}

/// A command's raw result.
struct Ran {
    stdout: String,
    stderr: String,
    code: Option<i32>,
}

async fn fabric(home: &FabricHome, args: &[&str]) -> Result<Ran> {
    let output = tokio::time::timeout(
        COMMAND_TIMEOUT,
        tokio::process::Command::new(fabric_bin())
            .arg("--home")
            .arg(home.root())
            .args(args)
            .output(),
    )
    .await
    .with_context(|| format!("fabric {args:?} timed out"))??;
    Ok(Ran {
        stdout: String::from_utf8(output.stdout)?,
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        code: output.status.code(),
    })
}

/// What differs from run to run, by name.
struct Normaliser {
    replacements: Vec<(String, &'static str)>,
}

impl Normaliser {
    fn walk(&self, key: Option<&str>, value: &mut Value) {
        match value {
            Value::Object(map) => {
                for (key, value) in map.iter_mut() {
                    self.walk(Some(key), value);
                }
            }
            Value::Array(items) => {
                for item in items {
                    self.walk(key, item);
                }
            }
            Value::String(text) => {
                match key {
                    // The only keys whose strings are not ours to compare.
                    Some("since") => *text = "<AUDIT_SINCE>".into(),
                    Some("transport") => *text = "<TRANSPORT>".into(),
                    Some("address") => *text = "<HOST:PORT>".into(),
                    // A dial socket is named after a hash of the peer and
                    // protocol; the directory it lives in is what is pinned.
                    Some("socket") => {
                        let dir = text.rsplit_once('/').map_or("", |(dir, _)| dir).to_string();
                        *text = format!("{dir}/<NAME>.sock");
                        for (from, to) in &self.replacements {
                            *text = text.replace(from.as_str(), to);
                        }
                    }
                    // Which sync runtime a node has depends on how it was started.
                    Some("owner" | "companion") => *text = "<SYNC>".into(),
                    _ => {
                        for (from, to) in &self.replacements {
                            *text = text.replace(from.as_str(), to);
                        }
                    }
                }
            }
            Value::Number(_) => {
                if matches!(key, Some("round_trip_micros")) {
                    *value = Value::String("<MICROS>".into());
                }
            }
            _ => {}
        }
    }

    /// Parse exactly one JSON object from `stdout`, normalise it, and render
    /// it the way the golden file is written.
    fn render(&self, stdout: &str) -> Result<String> {
        let mut lines = stdout.lines();
        let line = lines.next().context("a --json command printed nothing")?;
        if lines.next().is_some() {
            bail!("a --json command printed more than one line: {stdout:?}");
        }
        let mut value: Value =
            serde_json::from_str(line).with_context(|| format!("not JSON: {line:?}"))?;
        if !value.is_object() {
            bail!("not a JSON object: {line}");
        }
        self.walk(None, &mut value);
        Ok(serde_json::to_string_pretty(&value)? + "\n")
    }

    fn check(&self, name: &str, stdout: &str) -> Result<()> {
        let rendered = self.render(stdout)?;
        let path = golden_dir().join(format!("{name}.json"));
        if std::env::var_os("UPDATE_GOLDEN").is_some() {
            fs::create_dir_all(golden_dir())?;
            fs::write(&path, &rendered)?;
            return Ok(());
        }
        let expected = fs::read_to_string(&path).with_context(|| {
            format!(
                "no golden file {}; run with UPDATE_GOLDEN=1 to write it",
                path.display()
            )
        })?;
        assert_eq!(
            rendered, expected,
            "{name}: the --json shape changed. If that is intended, run with UPDATE_GOLDEN=1"
        );
        Ok(())
    }
}

fn ok(ran: &Ran) -> Result<&str> {
    if ran.code != Some(0) {
        bail!(
            "exit {:?}\nstdout: {}\nstderr: {}",
            ran.code,
            ran.stdout,
            ran.stderr
        );
    }
    Ok(&ran.stdout)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_json_command_prints_one_stable_object() -> Result<()> {
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let drop_dir = TempDir::new()?;
    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start(server_home.clone()).await?;
    let client = FabricNode::start(client_home.clone()).await?;

    // Before either trust install can announce to the other node and produce
    // a legitimate handshake refusal, the audit store is empty.
    let audit = fabric(&client_home, &["audit", "--json"]).await?;
    Normaliser { replacements: Vec::new() }.check("audit_empty", ok(&audit)?)?;

    let mut server_book = PeerBook::load(&server_home)?;
    server_book.add_with_allow(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        Some(vec!["echo".into(), "send-file".into()]),
    );
    server_book.save(&server_home)?;
    server.state().reload_peers().await?;

    let mut client_book = PeerBook::load(&client_home)?;
    client_book.add_with_allow(
        server.id(),
        Some("server".into()),
        Some(server.addr()),
        Some(vec!["echo".into(), "web".into(), "send-file".into()]),
    );
    client_book.save(&client_home)?;
    client.state().reload_peers().await?;

    let normaliser = Normaliser {
        replacements: vec![
            (client_dir.path().display().to_string(), "<CLIENT_HOME>"),
            (server_dir.path().display().to_string(), "<SERVER_HOME>"),
            (drop_dir.path().display().to_string(), "<DROP>"),
            (client.id().to_string(), "<CLIENT_ID>"),
            (server.id().to_string(), "<SERVER_ID>"),
            (fabric::version_string(), "<VERSION>"),
        ],
    };

    // id
    let id = fabric(&client_home, &["id", "--json"]).await?;
    normaliser.check("id", ok(&id)?)?;
    let plain = fabric(&client_home, &["id"]).await?;
    assert_eq!(
        ok(&plain)?.trim(),
        client.id().to_string(),
        "the human form of `id` changed"
    );

    // addr: JSON with or without the flag, and the same object
    let addr = fabric(&client_home, &["addr"]).await?;
    let addr_json = fabric(&client_home, &["addr", "--json"]).await?;
    assert_eq!(ok(&addr)?, ok(&addr_json)?);
    let parsed: Value = serde_json::from_str(ok(&addr_json)?.trim())?;
    assert_eq!(parsed["id"], client.id().to_string());

    // peers
    let peers = fabric(&client_home, &["peers", "--json"]).await?;
    normaliser.check("peers", ok(&peers)?)?;

    // probe: a service the peer serves
    let probe = fabric(
        &client_home,
        &["probe", "server", "fabric/echo/0", "--json"],
    )
    .await?;
    let probe_value: Value = serde_json::from_str(probe.stdout.trim())
        .with_context(|| format!("probe printed {:?}", probe.stdout))?;
    assert_eq!(probe_value["outcome"], "supported", "{probe_value}");
    assert_eq!(probe.code, Some(0));
    normaliser.check("probe", &probe.stdout)?;

    // status
    let status = fabric(&client_home, &["status", "--json"]).await?;
    let mut status_value: Value = serde_json::from_str(ok(&status)?.trim())?;
    assert_eq!(status_value["peers"][0]["reachable"], true);
    status_value["endpoint_addr"] = "<ENDPOINT_ADDR>".into();
    normaliser.check("status", &serde_json::to_string(&status_value)?)?;

    // expose / dial / unexpose
    let socket = drop_dir.path().join("web.sock");
    let expose = fabric(
        &client_home,
        &[
            "expose",
            "web",
            "--socket",
            socket.to_str().unwrap(),
            "--json",
        ],
    )
    .await?;
    normaliser.check("expose", ok(&expose)?)?;

    let dial = fabric(&client_home, &["dial", "server", "web", "--json"]).await?;
    normaliser.check("dial_socket", ok(&dial)?)?;
    let dial_tcp = fabric(
        &client_home,
        &["dial", "server", "web", "--tcp", "127.0.0.1:0", "--json"],
    )
    .await?;
    normaliser.check("dial_tcp", ok(&dial_tcp)?)?;

    let unexpose = fabric(&client_home, &["unexpose", "web", "--json"]).await?;
    normaliser.check("unexpose", ok(&unexpose)?)?;

    // send-file
    let file = drop_dir.path().join("note.txt");
    fs::write(&file, b"hello")?;
    let sent = fabric(
        &client_home,
        &["send-file", "server", file.to_str().unwrap(), "--json"],
    )
    .await?;
    normaliser.check("send_file", ok(&sent)?)?;

    // reload-peers
    let reload = fabric(&client_home, &["reload-peers", "--json"]).await?;
    normaliser.check("reload_peers", ok(&reload)?)?;

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

/// A peer that trusts this machine but has not granted `echo` is up. `status`
/// used to call it offline with a refusal as the reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_says_a_peer_that_has_not_granted_echo_is_up() -> Result<()> {
    let server_dir = TempDir::new()?;
    let client_dir = TempDir::new()?;
    let server_home = FabricHome::new(server_dir.path());
    let client_home = FabricHome::new(client_dir.path());
    let server = FabricNode::start(server_home.clone()).await?;
    let client = FabricNode::start(client_home.clone()).await?;

    // The server trusts the client and grants it nothing.
    let mut server_book = PeerBook::load(&server_home)?;
    server_book.add_with_allow(
        client.id(),
        Some("client".into()),
        Some(client.addr()),
        None,
    );
    server_book.save(&server_home)?;
    server.state().reload_peers().await?;
    let mut client_book = PeerBook::load(&client_home)?;
    client_book.add_with_allow(
        server.id(),
        Some("server".into()),
        Some(server.addr()),
        Some(vec!["echo".into()]),
    );
    client_book.save(&client_home)?;
    client.state().reload_peers().await?;

    let normaliser = Normaliser {
        replacements: vec![
            (client_dir.path().display().to_string(), "<CLIENT_HOME>"),
            (server_dir.path().display().to_string(), "<SERVER_HOME>"),
            (client.id().to_string(), "<CLIENT_ID>"),
            (server.id().to_string(), "<SERVER_ID>"),
            (fabric::version_string(), "<VERSION>"),
        ],
    };

    let status = fabric(&client_home, &["status", "--json"]).await?;
    let mut value: Value = serde_json::from_str(ok(&status)?.trim())?;
    assert_eq!(value["peers"][0]["reachable"], true, "{value}");
    assert_eq!(value["peers"][0]["echo_denied"], true, "{value}");
    assert_eq!(value["peers"][0]["error"], Value::Null, "{value}");
    value["endpoint_addr"] = "<ENDPOINT_ADDR>".into();
    normaliser.check("status_echo_denied", &serde_json::to_string(&value)?)?;

    // The human form says the same, and never "offline".
    let text = fabric(&client_home, &["status"]).await?;
    let line = ok(&text)?
        .lines()
        .find(|line| line.starts_with("  server\t"))
        .context("status listed no server line")?
        .to_string();
    assert!(line.ends_with("\treachable\techo not granted"), "{line:?}");

    // And ping names the real reason, with the refusal's exit status.
    let ping = fabric(&client_home, &["ping", "server"]).await?;
    assert_eq!(ping.code, Some(77), "{}", ping.stderr);
    assert!(
        ping.stderr
            .contains("is up but has not granted this machine echo"),
        "{}",
        ping.stderr
    );

    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_json_command_prints_only_an_error_object() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path());
    let node = FabricNode::start(home.clone()).await?;

    for args in [
        &["dial", "nobody", "web", "--json"][..],
        &["expose", "web", "--json"],
    ] {
        let ran = fabric(&home, args).await?;
        assert_ne!(ran.code, Some(0), "{args:?} succeeded");
        let line = ran.stdout.trim();
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("{args:?}: stdout was not one JSON object: {line:?}"))?;
        assert!(
            value["error"]["code"].is_string() && value["error"]["message"].is_string(),
            "{args:?}: {value}"
        );
        assert_eq!(ran.stdout.lines().count(), 1, "{args:?}: {}", ran.stdout);
    }

    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn audit_json_reads_offline_records_and_keeps_global_loss_visible_under_peer_filter() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path());
    home.prepare()?;
    let record = serde_json::json!({ "event": "exec_end", "time": "2026-10-16T00:00:00Z", "peer": "peer-a",
        "name": "laptop", "service": "exec", "argv": ["true"], "session_id": "session-1", "outcome": "ok",
        "code": null, "exit_status": 7, "duration_millis": 12, "bytes_in": 0, "bytes_out": 5 });
    let loss = serde_json::json!({ "event": "audit_loss", "time": "2026-10-16T00:00:02Z", "count": 2,
        "first_time": "2026-10-16T00:00:00Z", "last_time": "2026-10-16T00:00:01Z" });
    fs::write(home.root().join("audit.jsonl"), format!("{record}\n{loss}\n{{\"event\":"))?;
    let normaliser = Normaliser { replacements: Vec::new() };
    let audit = fabric(&home, &["audit", "--peer", "laptop", "--json"]).await?;
    normaliser.check("audit", ok(&audit)?)?;
    let other = fabric(&home, &["audit", "--peer", "peer-b", "--json"]).await?;
    let other: Value = serde_json::from_str(ok(&other)?)?;
    assert_eq!(other["records"].as_array().unwrap().len(), 1);
    assert_eq!(other["records"][0]["event"], "audit_loss");
    let recent = fabric(&home, &["audit", "--since", "0s", "--json"]).await?;
    let recent: Value = serde_json::from_str(ok(&recent)?)?;
    assert!(recent["records"].as_array().unwrap().is_empty());
    Ok(())
}
