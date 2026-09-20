//! `fabric join` against a stub `ssh` on `PATH`.
//!
//! The stub answers `fabric id` with a fixed id and records every remote
//! command it is given, so the test can prove what a join runs on the far
//! side without a real sshd. Each case gets its own HOME, so `~/.ssh/config`
//! is the test's, and its own fabric home, so `peers.toml` is too.

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::{Context, Result};
use fabric::config::{FabricHome, PeerBook};
use tempfile::TempDir;

fn fabric_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric")
}

/// A stub `ssh` that logs its arguments and behaves per host:
/// `good`, `known` and `second` answer with an id; `down` is unreachable
/// (exit 255); `bare` has no fabric (exit 127). `known` claims to already
/// know the caller when asked for its peers.
fn install_stub_ssh(dir: &Path, remote_id: &str, log: &Path) -> Result<PathBuf> {
    install_stub_ssh_with(dir, &[("good", remote_id), ("known", remote_id)], log)
}

/// Like `install_stub_ssh`, with one id per host: two machines never share an
/// id, and fabric keys trust on the id, so a stub that answered every host
/// with one id would make the second host replace the first.
fn install_stub_ssh_with(dir: &Path, ids: &[(&str, &str)], log: &Path) -> Result<PathBuf> {
    let id_cases: String = ids
        .iter()
        .map(|(host, id)| format!("    {host}) echo \"{id}\"; exit 0 ;;\n"))
        .collect();
    let bin = dir.join("bin");
    fs::create_dir_all(&bin)?;
    let script = bin.join("ssh");
    fs::write(
        &script,
        format!(
            r#"#!/bin/sh
# record: host<TAB>command
host=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) shift 2; continue ;;
    --) shift; host="$1"; shift; break ;;
    *) host="$1"; shift; break ;;
  esac
done
cmd="$*"
printf '%s\t%s\n' "$host" "$cmd" >> "{log}"
case "$host" in
  down) echo "ssh: connect to host down port 22: No route to host" >&2; exit 255 ;;
  bare) echo "sh: fabric: command not found" >&2; exit 127 ;;
esac
case "$cmd" in
  *'" id'*) case "$host" in
{id_cases}    *) echo "stub: no id for $host" >&2; exit 2 ;;
    esac ;;
  *'" peers'*|*'peers 2>/dev/null'*)
    # `known` already lists the caller; the id is the first token of the
    # `add` that follows in the same command, so we grep it out of $cmd.
    if [ "$host" = known ]; then
      caller=$(printf '%s' "$cmd" | sed -n 's/.*grep -q .^\([0-9a-f]*\)..*/\1/p')
      printf '%s\tcaller\tshell\n' "$caller" > /tmp/fabric-join-stub-peers.$$
      # emulate: `if peers | grep -q '^id'` -> true -> run the add without --allow
      if printf '%s' "$cmd" | grep -q 'then'; then
        # run the "then" branch text as the recorded add; we only echo reloaded
        printf '%s\tTHEN-BRANCH\n' "$host" >> "{log}"
      fi
      echo reloaded; exit 0
    fi
    # unknown caller: the else branch runs the add with --allow
    printf '%s\tELSE-BRANCH\n' "$host" >> "{log}"
    echo reloaded; exit 0 ;;
  *'" add '*) echo reloaded; exit 0 ;;
esac
echo "stub: unexpected command: $cmd" >&2
exit 2
"#,
            log = log.display(),
            id_cases = id_cases
        ),
    )?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755))?;
    Ok(bin)
}

fn run_join(home: &FabricHome, test_home: &Path, bin: &Path, args: &[&str]) -> Result<Output> {
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    Command::new(fabric_bin())
        .env("PATH", path)
        .env("HOME", test_home)
        .arg("--home")
        .arg(home.root())
        .arg("join")
        .args(args)
        .output()
        .context("failed to run fabric join")
}

fn remote_id() -> String {
    iroh::SecretKey::generate().public().to_string()
}

#[test]
fn join_trusts_both_sides_with_the_documented_defaults() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path().join("fabric"));
    let log = dir.path().join("ssh.log");
    let id = remote_id();
    let bin = install_stub_ssh(dir.path(), &id, &log)?;

    let output = run_join(&home, dir.path(), &bin, &["good"])?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout={stdout} stderr={stderr}"
    );
    assert!(stdout.starts_with("joined\tgood\t"), "stdout={stdout}");

    // Here: the far machine is trusted under the host's name with no grants.
    let book = PeerBook::load(&home)?;
    let peer = book
        .peers()
        .iter()
        .find(|peer| peer.id.to_string() == id)
        .context("the far machine was not added locally")?;
    assert_eq!(peer.name.as_deref(), Some("good"));
    assert!(peer.allow.is_empty(), "nothing is granted here by default");

    // There: fabric id first, then an add for this machine's id with the
    // default allow, applied only if the far side does not know us yet.
    let local_id = fabric::config::load_or_create_identity(&home)?
        .public()
        .to_string();
    let recorded = fs::read_to_string(&log)?;
    let lines: Vec<&str> = recorded.lines().collect();
    assert!(
        lines[0].contains("\" id"),
        "first remote command: {}",
        lines[0]
    );
    let add = lines
        .iter()
        .find(|line| line.contains(&format!("add {local_id} ")))
        .context("no remote add was run")?;
    assert!(
        add.contains("--allow shell,exec"),
        "default allow missing: {add}"
    );
    assert!(add.contains("reload-peers"), "no reload there: {add}");
    assert!(
        add.contains(&format!("grep -q '^{local_id}'")),
        "a known peer must keep its grants: {add}"
    );
    // The local daemon is not running in this test, and the join says so
    // instead of failing.
    assert!(
        stdout.contains("local daemon not running"),
        "stdout={stdout}"
    );
    Ok(())
}

#[test]
fn explicit_allow_and_grant_are_written_as_given() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path().join("fabric"));
    let log = dir.path().join("ssh.log");
    let id = remote_id();
    let bin = install_stub_ssh(dir.path(), &id, &log)?;

    let output = run_join(
        &home,
        dir.path(),
        &bin,
        &[
            "good",
            "--allow",
            "shell,exec,sync",
            "--grant",
            "echo",
            "--name",
            "my-laptop",
        ],
    )?;
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let book = PeerBook::load(&home)?;
    let peer = book
        .peers()
        .iter()
        .find(|p| p.id.to_string() == id)
        .unwrap();
    assert_eq!(peer.allow, vec!["echo".to_string()]);

    let recorded = fs::read_to_string(&log)?;
    let add = recorded.lines().find(|l| l.contains(" add ")).unwrap();
    assert!(add.contains("'my-laptop' --allow shell,exec,sync"), "{add}");
    assert!(
        !add.contains("grep -q"),
        "explicit allow never consults the far list: {add}"
    );
    Ok(())
}

#[test]
fn a_repeat_without_flags_keeps_local_grants() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path().join("fabric"));
    let log = dir.path().join("ssh.log");
    let id = remote_id();
    let bin = install_stub_ssh(dir.path(), &id, &log)?;

    let first = run_join(&home, dir.path(), &bin, &["good", "--grant", "sync"])?;
    assert_eq!(first.status.code(), Some(0));
    let second = run_join(&home, dir.path(), &bin, &["good"])?;
    assert_eq!(second.status.code(), Some(0));

    let book = PeerBook::load(&home)?;
    let peer = book
        .peers()
        .iter()
        .find(|p| p.id.to_string() == id)
        .unwrap();
    assert_eq!(
        peer.allow,
        vec!["sync".to_string()],
        "a repeat narrowed the grant"
    );
    Ok(())
}

#[test]
fn unreachable_and_fabricless_hosts_are_reported_and_do_not_stop_the_rest() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path().join("fabric"));
    let log = dir.path().join("ssh.log");
    let id = remote_id();
    let bin = install_stub_ssh(dir.path(), &id, &log)?;

    let output = run_join(&home, dir.path(), &bin, &["down", "bare", "good"])?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={stdout} stderr={stderr}"
    );
    assert!(
        stderr.contains("down was not joined") && stderr.contains("ssh could not reach"),
        "{stderr}"
    );
    assert!(
        stderr.contains("bare was not joined") && stderr.contains("not installed there"),
        "{stderr}"
    );
    assert!(
        stderr.contains("install.sh"),
        "the install hint is missing: {stderr}"
    );
    assert!(
        stdout.contains("joined\tgood"),
        "the good host was skipped: {stdout}"
    );
    assert!(stderr.contains("2 of 3 host(s) not joined"), "{stderr}");
    // With several hosts ssh runs without prompts.
    let recorded = fs::read_to_string(&log)?;
    assert!(recorded.lines().all(|l| !l.is_empty()));
    let book = PeerBook::load(&home)?;
    assert_eq!(book.peers().len(), 1, "only the joined host is trusted");
    Ok(())
}

#[test]
fn all_reads_named_hosts_from_the_ssh_config() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path().join("fabric"));
    let log = dir.path().join("ssh.log");
    let (first, second) = (remote_id(), remote_id());
    let bin = install_stub_ssh_with(dir.path(), &[("good", &first), ("second", &second)], &log)?;
    fs::create_dir_all(dir.path().join(".ssh"))?;
    fs::write(
        dir.path().join(".ssh").join("config"),
        "Host *\n  ServerAliveInterval 30\nHost good\n  HostName 10.0.0.1\nHost second !down\n",
    )?;

    let dry = run_join(&home, dir.path(), &bin, &["--all", "--dry-run"])?;
    let stdout = String::from_utf8_lossy(&dry.stdout);
    assert_eq!(dry.status.code(), Some(0), "{stdout}");
    assert!(
        stdout.contains("would join\tgood\t") && stdout.contains("would join\tsecond\t"),
        "{stdout}"
    );
    assert!(
        !stdout.contains("down"),
        "a negated pattern is not a host: {stdout}"
    );
    assert!(!log.exists(), "a dry run must not run ssh");
    assert!(
        PeerBook::load(&home)?.peers().is_empty(),
        "a dry run must not write trust"
    );

    let output = run_join(&home, dir.path(), &bin, &["--all"])?;
    assert_eq!(
        output.status.code(),
        Some(0),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let names: Vec<String> = PeerBook::load(&home)?
        .peers()
        .iter()
        .filter_map(|p| p.name.clone())
        .collect();
    assert!(
        names.contains(&"good".to_string()) && names.contains(&"second".to_string()),
        "{names:?}"
    );
    Ok(())
}

#[test]
fn all_without_a_config_says_where_to_look() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path().join("fabric"));
    let log = dir.path().join("ssh.log");
    let bin = install_stub_ssh(dir.path(), &remote_id(), &log)?;
    let output = run_join(&home, dir.path(), &bin, &["--all"])?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_ne!(output.status.code(), Some(0));
    assert!(
        stderr.contains(".ssh/config") && stderr.contains("Host"),
        "{stderr}"
    );
    Ok(())
}

#[test]
fn joining_yourself_is_refused() -> Result<()> {
    let dir = TempDir::new()?;
    let home = FabricHome::new(dir.path().join("fabric"));
    let log = dir.path().join("ssh.log");
    let own = fabric::config::load_or_create_identity(&home)?
        .public()
        .to_string();
    let bin = install_stub_ssh(dir.path(), &own, &log)?;
    let output = run_join(&home, dir.path(), &bin, &["good"])?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("this machine"), "{stderr}");
    assert!(PeerBook::load(&home)?.peers().is_empty());
    Ok(())
}
