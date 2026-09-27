//! `install.sh` as `curl ... | sh` runs it: where it looks for the latest
//! release, whichever of curl or wget the machine has.
//!
//! `api.github.com` allows 60 unauthenticated requests an hour per network
//! address, shared by every machine behind it. An installer that asks it for
//! the latest release cannot install anything once a network has spent that
//! budget, so the installer must read the unmetered `releases/latest` redirect,
//! the same one `fabric update` reads.
//!
//! Each run gets a PATH holding only the tools the installer needs and a stub
//! for the downloader. The stub records every URL, answers the redirect, and
//! refuses every download, so the installer stops right after it has resolved
//! the release and nothing touches the network.

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use anyhow::{Context, Result, bail};
use tempfile::TempDir;

const RELEASE_PAGE: &str = "https://github.com/compoundingtech/fabric/releases/tag/v0.2.99";

const CURL_STUB: &str = r#"#!/bin/sh
for arg in "$@"; do
  case "$arg" in
    http*) echo "$arg" >> "$URL_LOG" ;;
  esac
done
case "$*" in
  *api.github.com*)
    echo "curl: (22) The requested URL returned error: 403" >&2
    exit 22
    ;;
  */releases/latest*)
    case "$*" in
      *redirect_url*) printf '%s' "$STUB_LOCATION" ;;
      *) echo "<html>a release page</html>" ;;
    esac
    ;;
  *)
    echo "curl: (22) The requested URL returned error: 404" >&2
    exit 22
    ;;
esac
"#;

const WGET_STUB: &str = r#"#!/bin/sh
for arg in "$@"; do
  case "$arg" in
    http*) echo "$arg" >> "$URL_LOG" ;;
  esac
done
case "$*" in
  *api.github.com*)
    echo "ERROR 403: rate limit exceeded." >&2
    exit 8
    ;;
  *-S*/releases/latest*)
    {
      echo "Spider mode enabled. Check if remote file exists."
      echo "HTTP request sent, awaiting response..."
      echo "  HTTP/1.1 302 Found"
      echo "  Location: $STUB_LOCATION"
      echo "Location: $STUB_LOCATION [following]"
      echo "  HTTP/1.1 200 OK"
      echo "Remote file exists and could contain further links,"
    } >&2
    ;;
  *)
    echo "ERROR 404: Not Found." >&2
    exit 8
    ;;
esac
"#;

/// The tools the installer runs before its first download. Nothing else is on
/// PATH, so the real curl and wget are out of reach.
const TOOLS: &[&str] = &[
    "basename", "cat", "dirname", "grep", "head", "mktemp", "rm", "sed", "tr", "uname",
];

fn install_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("install.sh")
}

fn find_tool(name: &str) -> Result<PathBuf> {
    for dir in ["/usr/bin", "/bin"] {
        let path = Path::new(dir).join(name);
        if path.exists() {
            return Ok(path);
        }
    }
    bail!("{name} is not in /usr/bin or /bin")
}

struct Run {
    output: Output,
    urls: Vec<String>,
}

impl Run {
    fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output.stdout).into_owned()
    }

    fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).into_owned()
    }
}

/// Run `sh -s -- <args> < install.sh` with `downloader` stubbed as `stub`.
fn run_installer(downloader: &str, stub: &str, location: &str, args: &[&str]) -> Result<Run> {
    let temp = TempDir::new()?;
    let bin = temp.path().join("bin");
    fs::create_dir(&bin)?;
    for tool in TOOLS {
        symlink(find_tool(tool)?, bin.join(tool))?;
    }
    let stub_path = bin.join(downloader);
    fs::write(&stub_path, stub)?;
    fs::set_permissions(&stub_path, fs::Permissions::from_mode(0o755))?;
    let url_log = temp.path().join("urls");
    fs::write(&url_log, "")?;

    let output = Command::new("/bin/sh")
        .args(["-s", "--"])
        .args(args)
        .stdin(Stdio::from(fs::File::open(install_script())?))
        .env_clear()
        .env("PATH", &bin)
        .env("HOME", temp.path())
        .env("FABRIC_BIN_DIR", temp.path().join("installed"))
        .env("URL_LOG", &url_log)
        .env("STUB_LOCATION", location)
        .output()
        .context("failed to run install.sh")?;
    let urls = fs::read_to_string(&url_log)?
        .lines()
        .map(str::to_string)
        .collect();
    Ok(Run { output, urls })
}

fn assert_resolves_latest_from_the_redirect(downloader: &str, stub: &str) -> Result<()> {
    let run = run_installer(downloader, stub, RELEASE_PAGE, &[])?;
    assert!(
        run.urls.iter().all(|url| !url.contains("api.github.com")),
        "the installer asked the rate-limited API with {downloader}: {:?}",
        run.urls
    );
    assert!(
        run.urls
            .iter()
            .any(|url| url == "https://github.com/compoundingtech/fabric/releases/latest"),
        "the installer never read the releases/latest redirect with {downloader}: {:?}",
        run.urls
    );
    assert!(
        run.stdout().contains("target release: v0.2.99"),
        "the installer did not resolve latest to the redirect's tag with {downloader}\n\
         stdout: {}\nstderr: {}",
        run.stdout(),
        run.stderr()
    );
    Ok(())
}

#[test]
fn latest_comes_from_the_release_redirect_with_curl() -> Result<()> {
    assert_resolves_latest_from_the_redirect("curl", CURL_STUB)
}

#[test]
fn latest_comes_from_the_release_redirect_with_wget() -> Result<()> {
    assert_resolves_latest_from_the_redirect("wget", WGET_STUB)
}

/// With no release published, GitHub sends `releases/latest` to the release
/// list instead of a release page. That is not a tag, and the installer must say
/// it could not resolve the release rather than install something else.
#[test]
fn a_redirect_that_names_no_release_is_refused() -> Result<()> {
    let run = run_installer(
        "curl",
        CURL_STUB,
        "https://github.com/compoundingtech/fabric/releases",
        &[],
    )?;
    assert!(!run.output.status.success());
    assert!(
        !run.stdout().contains("target release:"),
        "stdout: {}",
        run.stdout()
    );
    assert!(
        run.stderr()
            .contains("could not resolve requested fabric release: latest"),
        "stderr: {}",
        run.stderr()
    );
    Ok(())
}

/// A pinned version needs no lookup at all, which is why it is the install that
/// works on a network whose API budget is spent.
#[test]
fn a_pinned_version_asks_nothing_about_releases() -> Result<()> {
    let run = run_installer("curl", CURL_STUB, RELEASE_PAGE, &["--version", "0.2.99"])?;
    assert!(
        run.stdout().contains("target release: v0.2.99"),
        "stdout: {}",
        run.stdout()
    );
    for url in &run.urls {
        assert!(
            url.starts_with("https://github.com/compoundingtech/fabric/releases/download/v0.2.99/"),
            "a pinned install fetched something other than its own assets: {url}"
        );
    }
    Ok(())
}
