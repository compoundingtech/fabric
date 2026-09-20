//! `fabric join`: pair with a machine you can already ssh to, in one command.
//!
//! Fabric trust is symmetric and keyed on public keys, so joining a machine
//! means learning its id, writing it here, and writing this machine's id there.
//! Today that is six commands, two of them on the far side, with an id pasted
//! twice. A person who can already ssh to the machine has everything those
//! commands need: an authenticated channel that can run `fabric id` and
//! `fabric add` on the far side. This module uses that channel and nothing
//! else. It never reads a key, never copies a file, and never reimplements any
//! part of ssh: the `ssh` on `PATH` runs with the person's own config, agent,
//! and prompts.
//!
//! What a join writes is exactly what the manual steps write. Revocation stays
//! where it was: `fabric remove` on each side, or editing `peers.toml`.

use std::{
    fmt,
    path::Path,
    process::{Command, Output},
};

use anyhow::{Context, Result, bail};
use iroh::EndpointId;

/// Services the far machine lets this one use when `--allow` is not given.
/// An ssh login is total, so `shell` and `exec` are what ssh access already
/// meant; `sync` and exposed services are a separate decision and stay explicit.
pub const DEFAULT_ALLOW: &[&str] = &["shell", "exec"];

/// Seconds ssh may spend connecting before a host is reported unreachable.
const CONNECT_TIMEOUT_SECS: u32 = 15;

/// The path a fabric installed by `install.sh` lives at when it is not on the
/// non-interactive PATH of a remote ssh command.
const REMOTE_FABRIC_FALLBACK: &str = "$HOME/.local/bin/fabric";

/// A remote shell snippet that runs one fabric subcommand, preferring the
/// installer's location and falling back to whatever `PATH` has.
fn remote_fabric(args: &str) -> String {
    format!("f=\"{REMOTE_FABRIC_FALLBACK}\"; [ -x \"$f\" ] || f=fabric; \"$f\" {args}")
}

/// The remote command that prints the far machine's fabric id.
pub fn remote_id_command() -> String {
    remote_fabric("id")
}

/// What the far side should allow this machine, and whether that was the
/// person's explicit choice or the default for a machine it does not know yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllowPolicy {
    /// `--allow` was given: write exactly this, even for a known peer.
    Explicit(Vec<String>),
    /// No `--allow`: a peer the far side already knows keeps its grants (a
    /// re-run must not narrow anything); a new peer gets these.
    DefaultIfNew(Vec<String>),
}

/// The remote command that trusts this machine on the far side and reloads
/// its daemon. `fabric add` without `--allow` preserves an existing entry's
/// grants, so the default-for-new case first asks `fabric peers` whether this
/// id is already there. `reload-peers` failing is not fatal to the join: the
/// exit code is the `add`'s.
pub fn remote_add_command(local_id: EndpointId, local_name: &str, allow: &AllowPolicy) -> String {
    let base = format!("add {local_id} {}", shell_quote(local_name));
    let with_allow = |services: &[String]| {
        if services.is_empty() {
            base.clone()
        } else {
            format!("{base} --allow {}", services.join(","))
        }
    };
    let add = match allow {
        AllowPolicy::Explicit(services) => remote_fabric(&with_allow(services)),
        AllowPolicy::DefaultIfNew(services) => format!(
            "if {} 2>/dev/null | grep -q '^{local_id}'; then {}; else {}; fi",
            remote_fabric("peers"),
            remote_fabric(&base),
            remote_fabric(&with_allow(services))
        ),
    };
    format!(
        "{add}; s=$?; [ $s -eq 0 ] && ({}); exit $s",
        remote_fabric("reload-peers")
    )
}

/// Single-quote a string for a POSIX shell.
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

/// Refuse a service or name that could not be a plain token. Names and
/// services travel inside a remote shell command; quoting handles the shell,
/// but a value with a newline or a NUL in it is a mistake, not a name.
pub fn validate_token(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{kind} cannot be empty");
    }
    if value.chars().any(|c| c.is_control()) {
        bail!("{kind} {value:?} contains a control character");
    }
    Ok(())
}

/// The fabric id the far side printed: the last non-empty line of stdout.
pub fn parse_remote_id(stdout: &str) -> Result<EndpointId> {
    let line = stdout
        .lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .context("the far side printed nothing where a fabric id was expected")?;
    line.parse::<EndpointId>()
        .with_context(|| format!("the far side printed {line:?}, which is not a fabric id"))
}

/// The `Host` aliases in an OpenSSH client config: the machines a person has
/// named for themselves. Patterns (`*`, `?`) and negations (`!`) are skipped,
/// because they match hosts rather than naming one. Order is kept and
/// duplicates dropped. `Match` blocks contain no `Host` lines, so they need no
/// special handling; `Include` files are not followed.
pub fn ssh_config_hosts(config: &str) -> Vec<String> {
    let mut hosts: Vec<String> = Vec::new();
    for line in config.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut words = line.split_whitespace();
        let Some(keyword) = words.next() else {
            continue;
        };
        // OpenSSH accepts `Host name` and `Host=name`.
        let (keyword, first) = match keyword.split_once('=') {
            Some((keyword, rest)) => (keyword, Some(rest)),
            None => (keyword, None),
        };
        if !keyword.eq_ignore_ascii_case("host") {
            continue;
        }
        for alias in first.into_iter().chain(words) {
            if alias.is_empty()
                || alias.starts_with('!')
                || alias.contains('*')
                || alias.contains('?')
            {
                continue;
            }
            if !hosts.iter().any(|known| known == alias) {
                hosts.push(alias.to_string());
            }
        }
    }
    hosts
}

/// Where `--all` gets its list: the person's ssh client config.
pub fn ssh_config_path(home_dir: &Path) -> std::path::PathBuf {
    home_dir.join(".ssh").join("config")
}

/// Why a remote step did not happen.
#[derive(Debug)]
pub enum RemoteFailure {
    /// ssh itself could not connect or authenticate (ssh exits 255).
    Unreachable(String),
    /// The command ran there but fabric was not found.
    FabricMissing(String),
    /// The command ran there and failed for another reason.
    Failed { code: Option<i32>, stderr: String },
}

impl fmt::Display for RemoteFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable(detail) => write!(f, "ssh could not reach it: {detail}"),
            Self::FabricMissing(detail) => write!(
                f,
                "fabric is not installed there ({detail}); install it with \
                 `curl -sSf https://raw.githubusercontent.com/compoundingtech/fabric/main/install.sh | sh` \
                 and `fabric service install`, then join again"
            ),
            Self::Failed { code, stderr } => {
                write!(f, "the command failed there (exit {code:?}): {stderr}")
            }
        }
    }
}

impl std::error::Error for RemoteFailure {}

/// Classify an ssh run. ssh reserves exit 255 for its own failures; anything
/// else is the remote command's status.
pub fn classify(output: &Output) -> Result<String, RemoteFailure> {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
    }
    match output.status.code() {
        Some(255) => Err(RemoteFailure::Unreachable(one_line(&stderr))),
        Some(127) => Err(RemoteFailure::FabricMissing(one_line(&stderr))),
        code => {
            if stderr.contains("not found") && stderr.contains("fabric") {
                Err(RemoteFailure::FabricMissing(one_line(&stderr)))
            } else {
                Err(RemoteFailure::Failed {
                    code,
                    stderr: one_line(&stderr),
                })
            }
        }
    }
}

fn one_line(text: &str) -> String {
    let line = text.lines().last().unwrap_or("").trim();
    if line.is_empty() {
        "no output".to_string()
    } else {
        line.to_string()
    }
}

/// Run one command on `host` through the person's own ssh.
///
/// `batch` makes ssh refuse to prompt (for `--all`, where a prompt for one
/// host would stall the rest); a single join keeps ssh's prompts so a new host
/// key or a passphrase can be answered.
pub fn ssh_run(host: &str, command: &str, batch: bool) -> Result<Output> {
    let mut ssh = Command::new("ssh");
    ssh.arg("-o")
        .arg(format!("ConnectTimeout={CONNECT_TIMEOUT_SECS}"));
    if batch {
        ssh.arg("-o").arg("BatchMode=yes");
    }
    ssh.arg("--").arg(host).arg(command);
    ssh.output()
        .with_context(|| format!("failed to run ssh for {host:?}; is ssh installed?"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[test]
    fn config_hosts_are_the_named_aliases_only() {
        let config = "\
# comment
Host alpha
  HostName 1.2.3.4
host laptop desktop
Host *.example.com !secret
Host=alias
Match host alpha
  User me
Host alpha
";
        assert_eq!(
            ssh_config_hosts(config),
            vec!["alpha", "laptop", "desktop", "alias"]
        );
    }

    #[test]
    fn an_empty_or_patterns_only_config_yields_no_hosts() {
        assert!(ssh_config_hosts("").is_empty());
        assert!(ssh_config_hosts("Host *\n  ServerAliveInterval 30\n").is_empty());
    }

    #[test]
    fn the_remote_id_is_the_last_non_empty_line() {
        let id = iroh::SecretKey::generate().public();
        let stdout = format!("motd banner\n\n{id}\n");
        assert_eq!(parse_remote_id(&stdout).unwrap(), id);
        assert!(parse_remote_id("").is_err());
        assert!(parse_remote_id("fabric: not a daemon\n").is_err());
    }

    #[test]
    fn remote_commands_prefer_the_installer_path_and_quote_names() {
        let id = iroh::SecretKey::generate().public();
        let services = vec!["shell".to_string(), "exec".to_string()];
        let explicit =
            remote_add_command(id, "my laptop's", &AllowPolicy::Explicit(services.clone()));
        assert!(explicit.contains(REMOTE_FABRIC_FALLBACK));
        assert!(explicit.contains(&format!("add {id} 'my laptop'\\''s' --allow shell,exec")));
        assert!(explicit.contains("reload-peers"));
        assert!(
            !explicit.contains("peers 2>/dev/null"),
            "explicit never consults the far list"
        );

        // The default only applies to a peer the far side does not know: the
        // known branch adds without --allow, which preserves what is there.
        let default = remote_add_command(id, "laptop", &AllowPolicy::DefaultIfNew(services));
        assert!(default.contains(&format!("peers 2>/dev/null | grep -q '^{id}'")));
        assert!(default.contains(&format!("then f=\"{REMOTE_FABRIC_FALLBACK}\"; [ -x \"$f\" ] || f=fabric; \"$f\" add {id} 'laptop'; else")));
        assert!(default.contains(&format!("add {id} 'laptop' --allow shell,exec; fi")));

        let bare = remote_add_command(id, "laptop", &AllowPolicy::Explicit(Vec::new()));
        assert!(!bare.contains("--allow"));
        assert!(remote_id_command().ends_with("\"$f\" id"));
    }

    #[test]
    fn tokens_with_control_characters_are_refused() {
        assert!(validate_token("name", "laptop").is_ok());
        assert!(validate_token("name", "").is_err());
        assert!(validate_token("service", "sh\nell").is_err());
    }

    #[test]
    fn ssh_exit_codes_classify_the_failure() {
        let output = |code: i32, stderr: &str| Output {
            status: std::process::ExitStatus::from_raw(code << 8),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        };
        assert!(matches!(
            classify(&output(
                255,
                "ssh: connect to host x port 22: No route to host"
            )),
            Err(RemoteFailure::Unreachable(_))
        ));
        assert!(matches!(
            classify(&output(127, "sh: fabric: command not found")),
            Err(RemoteFailure::FabricMissing(_))
        ));
        assert!(matches!(
            classify(&output(1, "bash: line 1: fabric: not found")),
            Err(RemoteFailure::FabricMissing(_))
        ));
        assert!(matches!(
            classify(&output(2, "unknown peer")),
            Err(RemoteFailure::Failed { code: Some(2), .. })
        ));
        let ok = Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: b"abc\n".to_vec(),
            stderr: Vec::new(),
        };
        assert_eq!(classify(&ok).unwrap(), "abc\n");
    }
}
