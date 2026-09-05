//! `fabric update` — replace this machine's fabric with a verified build.
//!
//! The recipe here is not new. It existed three times over, in `install.sh`, in
//! the README, and in a shell script that lived on one machine and was
//! base64-encoded across the wire to the others. Each copy knew a trap the
//! others did not. This is one copy, in the binary, with the traps as tests.
//!
//! WHAT THE CHECKSUM DOES AND DOES NOT DO. With `--url` and an explicit
//! `--sha256` it is a real check that the bytes are the ones the caller named.
//! On the release paths the sidecar is fetched from the SAME server as the
//! artifact, so it protects against corruption and truncation and NOT against a
//! compromised release. That is the ordinary trust model for a release install,
//! and it is written down here so nobody reads the word "verify" as more than it
//! is.

use anyhow::{Context, Result, bail};

/// `--check` answers three questions, not two. A sweep that cannot tell "the
/// release server is unreachable" from "an update is available" will act on the
/// wrong one.
pub const CHECK_EXIT_CURRENT: i32 = 0;
pub const CHECK_EXIT_AVAILABLE: i32 = 1;
pub const CHECK_EXIT_ERROR: i32 = 2;

const RELEASE_REPO: &str = "compoundingtech/fabric";

/// Where the artifact comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// A published release. `None` means whatever is latest.
    Release { tag: Option<String> },
    /// An artifact the caller named, with the hash they expect it to have.
    Explicit { url: String, sha256: String },
}

/// The release target triple for the machine this binary was built for.
///
/// Deliberately built from `cfg!` rather than read from the environment: the
/// asset that gets installed must match the binary doing the installing, and a
/// runtime lookup could disagree with the compile that produced it.
pub fn target_triple() -> Result<&'static str> {
    Ok(match (cfg!(target_os = "macos"), cfg!(target_arch = "aarch64")) {
        (true, true) => "aarch64-apple-darwin",
        (false, true) => "aarch64-unknown-linux-gnu",
        (false, false) => "x86_64-unknown-linux-gnu",
        (true, false) => bail!("fabric publishes no release for this platform"),
    })
}

pub fn asset_name(target: &str) -> String {
    format!("fabric-{target}.tar.gz")
}

pub fn release_asset_url(tag: &str, asset: &str) -> String {
    format!("https://github.com/{RELEASE_REPO}/releases/download/{tag}/{asset}")
}

pub fn latest_release_api_url() -> String {
    format!("https://api.github.com/repos/{RELEASE_REPO}/releases/latest")
}

/// Decide where the artifact comes from, refusing the combination that would
/// run unverified bytes.
///
/// An explicit URL with no hash is remote code execution with good manners, so
/// it is rejected rather than defaulted. There is nothing sensible to default
/// to: the whole point of `--url` is that fabric does not know what is there.
pub fn resolve_source(
    tag: Option<String>,
    url: Option<String>,
    sha256: Option<String>,
) -> Result<Source> {
    match (tag, url, sha256) {
        (Some(_), Some(_), _) => {
            bail!("--tag and --url name two different artifacts; pass one of them")
        }
        (_, None, Some(_)) => {
            bail!("--sha256 only means something with --url; a release carries its own checksum")
        }
        (_, Some(url), None) => bail!(
            "--url requires --sha256.\n\
             \n\
             fabric will not install bytes it cannot check against a hash you \
             named, and it has nothing to compare {url} against on its own."
        ),
        (_, Some(url), Some(sha256)) => {
            let sha256 = normalise_sha256(&sha256)?;
            Ok(Source::Explicit { url, sha256 })
        }
        (tag, None, None) => Ok(Source::Release { tag }),
    }
}

/// Accept a hash in the shape a person actually pastes, and reject anything that
/// is not one. Length and alphabet are the whole contract.
fn normalise_sha256(raw: &str) -> Result<String> {
    let hash = raw.trim().to_ascii_lowercase();
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!(
            "--sha256 must be 64 hex characters, got {} characters: {raw}",
            hash.len()
        );
    }
    Ok(hash)
}

/// Pull the hash out of a published `.sha256` sidecar.
///
/// The sidecar reads `<hash>  dist/fabric-<target>.tar.gz`, carrying the path it
/// had on the builder. That path does not exist here, so handing the file to
/// `shasum -c` fails on the path rather than on the bytes. Take field one.
pub fn parse_sha256_sidecar(text: &str) -> Result<String> {
    let field = text
        .split_whitespace()
        .next()
        .context("the checksum sidecar was empty")?;
    normalise_sha256(field)
}

/// Compare the bytes we hold against the hash we expected.
pub fn verify_sha256(bytes: &[u8], expected: &str) -> Result<()> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let actual = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if actual != expected {
        bail!(
            "checksum mismatch, nothing was installed\n  expected  {expected}\n  actual    {actual}"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseBinaries {
    pub fabric: Vec<u8>,
    pub fabric_sync: Vec<u8>,
}

/// Take the matched binary pair out of a release archive.
///
/// A release archive holds literal `fabric` and `fabric-sync` members. It holds
/// no dot-prefixed paths, directories, or extra files. Anything else is not a
/// thing we published, and unpacking it to find out would already write it.
pub fn extract_release_binaries(archive: &[u8]) -> Result<ReleaseBinaries> {
    use std::io::Read;
    let decoder = flate2::read::GzDecoder::new(archive);
    let mut tar = tar::Archive::new(decoder);
    let mut fabric: Option<Vec<u8>> = None;
    let mut fabric_sync: Option<Vec<u8>> = None;
    let mut names = Vec::new();
    for entry in tar.entries().context("the archive could not be read")? {
        let mut entry = entry.context("the archive holds an unreadable member")?;
        // Compare the RAW stored name. The parsed path agrees with it today —
        // the tar crate normalises `./fabric` on the way IN, not on the way out,
        // so both forms read back as `./fabric` and both would reject it. This
        // is belt and braces, not a fix: it cannot drift if the path parser ever
        // starts normalising, and the exact bytes are what we actually publish.
        //
        // I claimed the opposite here first, that the parsed form would accept
        // `./fabric`. Mutating the code back proved it would not. Left corrected
        // rather than left flattering.
        let raw = entry.path_bytes().into_owned();
        let name = String::from_utf8_lossy(&raw).into_owned();
        names.push(name.clone());
        if raw.as_slice() == b"fabric" || raw.as_slice() == b"fabric-sync" {
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .with_context(|| format!("the {name} member could not be read"))?;
            match raw.as_slice() {
                b"fabric" => fabric = Some(bytes),
                b"fabric-sync" => fabric_sync = Some(bytes),
                _ => unreachable!(),
            }
        }
    }
    if names.len() != 2 || fabric.is_none() || fabric_sync.is_none() {
        bail!(
            "the archive is not a fabric release: expected exactly two members named \
             `fabric` and `fabric-sync`, found {names:?}"
        );
    }
    Ok(ReleaseBinaries {
        fabric: fabric.expect("checked above"),
        fabric_sync: fabric_sync.expect("checked above"),
    })
}

/// The version a release tag promises. Tags are `v<version>`; the binary reports
/// `<version>`.
pub fn version_for_tag(tag: &str) -> &str {
    tag.strip_prefix('v').unwrap_or(tag)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReleaseDirection {
    Current,
    Upgrade,
    Downgrade,
    Diverged,
}

fn release_commit(version: &str) -> Result<&str> {
    let commit = version
        .rsplit_once('+')
        .map(|(_, commit)| commit)
        .context("the version has no build commit")?;
    if !(7..=40).contains(&commit.len()) || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("the version build commit is not a Git commit: {version}");
    }
    Ok(commit)
}

fn release_direction_from_compare(status: &str) -> Result<ReleaseDirection> {
    match status {
        "identical" => Ok(ReleaseDirection::Current),
        "ahead" => Ok(ReleaseDirection::Upgrade),
        "behind" => Ok(ReleaseDirection::Downgrade),
        "diverged" => Ok(ReleaseDirection::Diverged),
        other => bail!("the release API returned an unknown comparison status: {other}"),
    }
}

async fn release_direction(installed: &str, available: &str) -> Result<ReleaseDirection> {
    if installed == available {
        return Ok(ReleaseDirection::Current);
    }
    let installed_commit = release_commit(installed)?;
    let available_commit = release_commit(available)?;
    let url = format!(
        "https://api.github.com/repos/{RELEASE_REPO}/compare/{installed_commit}...{available_commit}"
    );
    let body = fetch(&url).await?;
    let json: serde_json::Value = serde_json::from_slice(&body)
        .context("the release comparison API returned something that is not JSON")?;
    let status = json
        .get("status")
        .and_then(|status| status.as_str())
        .context("the release comparison API returned no status")?;
    release_direction_from_compare(status)
}

fn enforce_release_direction(
    installed: &str,
    available: &str,
    direction: ReleaseDirection,
    allow_downgrade: bool,
) -> Result<()> {
    if matches!(
        direction,
        ReleaseDirection::Downgrade | ReleaseDirection::Diverged
    ) && !allow_downgrade
    {
        let relation = if direction == ReleaseDirection::Downgrade {
            "is older than"
        } else {
            "does not contain"
        };
        bail!(
            "refusing to replace {installed} with {available}: the release {relation} the installed build. \
             Pass --allow-downgrade to replace it explicitly; nothing was installed"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Everything below touches the network, the filesystem or the service manager.
// ---------------------------------------------------------------------------

use std::path::{Path, PathBuf};

/// reqwest is built here without a process-wide crypto provider, because iroh
/// supplies one per endpoint instead. `update` makes its own requests, so it has
/// to install one before the first, and only once per process.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Fetch bytes from `https://` or `file:///`.
///
/// `file:///` exists so a locally built artifact can be installed the same way a
/// release is, with the same hash check. reqwest does not handle that scheme, so
/// it is read directly rather than pretended to be a request.
pub async fn fetch(url: &str) -> Result<Vec<u8>> {
    if let Some(path) = url.strip_prefix("file://") {
        // `file:///tmp/x` leaves a leading slash, which is the absolute path.
        let path = if path.is_empty() { "/" } else { path };
        return tokio::fs::read(path)
            .await
            .with_context(|| format!("failed to read {path}"));
    }
    if !url.starts_with("https://") {
        bail!("refusing {url}: only https:// and file:/// are accepted");
    }
    ensure_crypto_provider();
    let response = reqwest::Client::builder()
        // GitHub rejects requests with no user agent.
        .user_agent(concat!("fabric/", env!("CARGO_PKG_VERSION")))
        .build()?
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to reach {url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("{url} returned {status}");
    }
    Ok(response
        .bytes()
        .await
        .with_context(|| format!("failed to read the body of {url}"))?
        .to_vec())
}

/// The tag GitHub currently marks as latest.
pub async fn latest_tag() -> Result<String> {
    let body = fetch(&latest_release_api_url()).await?;
    let json: serde_json::Value =
        serde_json::from_slice(&body).context("the release API returned something that is not JSON")?;
    json.get("tag_name")
        .and_then(|tag| tag.as_str())
        .map(str::to_string)
        .context("the release API returned no tag_name")
}

/// Where the binary that the SERVICE MANAGER runs actually lives.
///
/// Not `command -v fabric`, and not always `current_exe`. The interactive
/// binary on `$PATH` can differ from the one in the plist or unit, and
/// installing at the wrong one leaves the daemon running the old code while
/// `--version` cheerfully reports the new. That trap is inherited from the shell
/// script this replaces, where it is written in a comment nobody else could see.
pub fn managed_binary_path() -> Result<PathBuf> {
    if let Some(path) = managed_binary_path_from_service_manager() {
        return Ok(path);
    }
    std::env::current_exe().context("could not determine which binary is running")
}

pub fn companion_binary_path(fabric_path: &Path) -> Result<PathBuf> {
    let dir = fabric_path
        .parent()
        .context("the fabric install path has no directory")?;
    Ok(dir.join("fabric-sync"))
}

#[cfg(target_os = "macos")]
fn managed_binary_path_from_service_manager() -> Option<PathBuf> {
    let plist = crate::service::launch_agent_path().ok()?;
    if !plist.exists() {
        return None;
    }
    // `plutil` is part of macOS and `service.rs` already relies on it. Parsing
    // our own XML by hand would be one guess about a file a person may have
    // edited.
    let out = std::process::Command::new("plutil")
        .args(["-extract", "ProgramArguments.0", "raw", "-o", "-"])
        .arg(&plist)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let path = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

#[cfg(target_os = "linux")]
fn managed_binary_path_from_service_manager() -> Option<PathBuf> {
    let out = std::process::Command::new("systemctl")
        .args(["--user", "show", "-p", "ExecStart", "--value", "fabric.service"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?;
    // The value reads `... argv[]=/path/to/fabric --home ... ; ...`
    let path = text
        .split("argv[]=")
        .nth(1)?
        .split_whitespace()
        .next()?
        .trim_end_matches(';')
        .to_string();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

/// What a staged binary says it is. Running it is the only honest way to ask.
pub fn binary_version(path: &Path) -> Result<String> {
    let out = std::process::Command::new(path)
        .arg("--version")
        .output()
        .with_context(|| format!("failed to run {}", path.display()))?;
    if !out.status.success() {
        bail!("{} could not report its version", path.display());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Put `bytes` at `path`, keeping the file that was there as a rollback.
///
/// Both steps go through a same-directory temporary plus a rename. Copying over
/// a running binary is `ETXTBSY`; renaming past it leaves the running process on
/// its old inode and the next start on the new one. Same directory matters, or
/// the rename is a cross-filesystem copy and stops being atomic.
pub fn install_binary(path: &Path, bytes: &[u8], stamp: &str) -> Result<PathBuf> {
    let staged = stage_binary(path, bytes, stamp)?;
    commit_staged(&staged, path, stamp)
}

/// Write the incoming binary NEXT TO where it will live, executable, without
/// disturbing what is there.
///
/// Beside it rather than in a temp dir for two reasons: the rename that follows
/// is only atomic within one filesystem, and a staged binary has to be runnable
/// so its `--version` can be asked before anything is committed.
pub fn stage_binary(path: &Path, bytes: &[u8], stamp: &str) -> Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let dir = path
        .parent()
        .context("the install path has no directory to write beside")?;
    let name = path
        .file_name()
        .context("the install path has no file name")?
        .to_string_lossy();
    let staged = dir.join(format!(".{name}-incoming-{stamp}"));
    std::fs::write(&staged, bytes)
        .with_context(|| format!("failed to stage {}", staged.display()))?;
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
        .with_context(|| format!("failed to make {} executable", staged.display()))?;
    Ok(staged)
}

/// Move a staged binary into place, keeping whatever was there as a rollback.
///
/// Both moves are renames within one directory. Copying over a running binary is
/// `ETXTBSY`; renaming past it leaves the running process on its old inode and
/// the next start on the new one.
pub fn commit_staged(staged: &Path, path: &Path, stamp: &str) -> Result<PathBuf> {
    let rollback = prepare_rollback(path, stamp)?;
    commit_prepared(staged, path)?;
    Ok(rollback)
}

/// Copy the installed binary to its final rollback path without replacing it.
///
/// A detached supervisor must own the rollback before the first pair member is
/// renamed. Keeping preparation separate makes that ordering enforceable.
fn prepare_rollback(path: &Path, stamp: &str) -> Result<PathBuf> {
    let dir = path
        .parent()
        .context("the install path has no directory to write beside")?;
    let rollback = path.with_file_name(format!(
        "{}.rollback-{stamp}",
        path.file_name()
            .context("the install path has no file name")?
            .to_string_lossy()
    ));

    if path.exists() {
        let name = path
            .file_name()
            .context("the install path has no file name")?
            .to_string_lossy();
        let aside = dir.join(format!(".{name}-rollback-{stamp}"));
        std::fs::copy(path, &aside)
            .with_context(|| format!("failed to copy {} aside", path.display()))?;
        std::fs::rename(&aside, &rollback)
            .with_context(|| format!("failed to place {}", rollback.display()))?;
    }
    Ok(rollback)
}

fn commit_prepared(staged: &Path, path: &Path) -> Result<()> {
    std::fs::rename(staged, path)
        .with_context(|| format!("failed to install {}", path.display()))?;
    Ok(())
}

fn restore_after_pair_commit_failure(path: &Path, rollback: &Path) -> Result<()> {
    if rollback.exists() {
        let bytes = std::fs::read(rollback)
            .with_context(|| format!("failed to read {}", rollback.display()))?;
        let staged = stage_binary(path, &bytes, &format!("restore-{}", timestamp()))?;
        commit_prepared(&staged, path).with_context(|| {
            format!(
                "failed to restore {} after a partial pair install",
                path.display()
            )
        })?;
    } else if path.exists() {
        std::fs::remove_file(path).with_context(|| {
            format!(
                "failed to remove {} after a partial first install",
                path.display()
            )
        })?;
    }
    Ok(())
}

/// Preserve both old members, arm supervision, then replace the matched pair.
fn commit_staged_pair<F>(
    staged: &Path,
    path: &Path,
    staged_companion: &Path,
    companion_path: &Path,
    stamp: &str,
    before_commit: F,
) -> Result<(PathBuf, PathBuf)>
where
    F: FnOnce(&Path, &Path) -> Result<()>,
{
    let rollback = prepare_rollback(path, stamp)?;
    let companion_rollback = match prepare_rollback(companion_path, stamp) {
        Ok(rollback) => rollback,
        Err(error) => {
            return Err(error.context("fabric-sync rollback could not be prepared"));
        }
    };
    before_commit(&rollback, &companion_rollback)?;
    commit_prepared(staged, path)?;
    if let Err(error) = commit_prepared(staged_companion, companion_path) {
        restore_after_pair_commit_failure(path, &rollback)?;
        return Err(error.context("fabric-sync could not be installed; fabric was restored"));
    }
    Ok((rollback, companion_rollback))
}

/// A stamp for rollback names. Seconds are enough: two updates inside one second
/// on one machine is not a case worth a dependency.
pub fn timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    format!("{secs}")
}

/// The newest rollback binary sitting beside `path`, if there is one.
pub fn newest_rollback(path: &Path) -> Result<Option<PathBuf>> {
    let dir = path.parent().context("no directory to search")?;
    let prefix = format!(
        "{}.rollback-",
        path.file_name()
            .context("no file name")?
            .to_string_lossy()
    );
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in std::fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?
    {
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        if best.as_ref().is_none_or(|(seen, _)| modified > *seen) {
            best = Some((modified, entry.path()));
        }
    }
    Ok(best.map(|(_, path)| path))
}

fn companion_rollback_path(fabric_rollback: &Path) -> Result<PathBuf> {
    let name = fabric_rollback
        .file_name()
        .context("the rollback path has no file name")?
        .to_string_lossy();
    let stamp = name
        .strip_prefix("fabric.rollback-")
        .context("the fabric rollback name has no stamp")?;
    Ok(fabric_rollback.with_file_name(format!("fabric-sync.rollback-{stamp}")))
}

fn remove_with_rollback(path: &Path, stamp: &str) -> Result<PathBuf> {
    let rollback = path.with_file_name(format!(
        "{}.rollback-{stamp}",
        path.file_name()
            .context("the install path has no file name")?
            .to_string_lossy()
    ));
    if path.exists() {
        std::fs::rename(path, &rollback).with_context(|| {
            format!("failed to preserve {} before removing it", path.display())
        })?;
    }
    Ok(rollback)
}

/// How long the supervisor gives the restarted daemon to answer before it
/// decides the new binary does not work. Generous: a slow machine coming back
/// under load must not be mistaken for a broken build.
const SUPERVISE_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
const UPDATE_GENERATION_FILE: &str = "update-generation";

fn update_generation_path(home: &crate::config::FabricHome) -> PathBuf {
    home.root().join("run").join(UPDATE_GENERATION_FILE)
}

fn new_update_generation() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}

fn write_update_generation(home: &crate::config::FabricHome, generation: &str) -> Result<()> {
    let path = update_generation_path(home);
    let parent = path.parent().context("the generation file has no directory")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    let temporary = parent.join(format!(".{UPDATE_GENERATION_FILE}-{}", std::process::id()));
    std::fs::write(&temporary, format!("{generation}\n"))
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    std::fs::rename(&temporary, &path)
        .with_context(|| format!("failed to place {}", path.display()))?;
    Ok(())
}

fn generation_is_current(home: &crate::config::FabricHome, generation: &str) -> bool {
    std::fs::read_to_string(update_generation_path(home))
        .map(|recorded| recorded.trim() == generation)
        .unwrap_or(false)
}

/// Check that the daemon came back, and put the old binary back if it did not.
///
/// THIS EXISTS BECAUSE THE RECOVERY ACTOR MAY HAVE NO WORKING ROUTE. If a bad
/// binary takes the daemon down, `fabric exec` stops working, and the tool that
/// would repair the machine is the tool that just broke it. The route must be
/// tested before a deliberate outage. Do not infer that an open SSH port means
/// the recovery actor can log in.
///
/// It cannot run in the updating process either: on Linux that process lives
/// inside the service's own cgroup and dies with the restart. So it is scheduled
/// as a transient unit, and it runs the ROLLBACK binary rather than the new one,
/// because the rollback is the copy already proven to work on this machine.
pub async fn supervise_restart(
    home: &crate::config::FabricHome,
    rollback: &Path,
    generation: &str,
    expect: &str,
) -> Result<()> {
    match wait_for_daemon_version(home, generation, expect, SUPERVISE_READY_TIMEOUT).await {
        VersionWaitDecision::Ready => {
            println!("supervise\tthe daemon came back running {expect}");
            return Ok(());
        }
        VersionWaitDecision::Superseded => {
            println!("supervise\tstopped because its generation is stale or missing");
            return Ok(());
        }
        VersionWaitDecision::Rollback => {}
        VersionWaitDecision::Wait => unreachable!(),
    }

    // The record can change after the final wait decision. Check it again
    // immediately before the first rollback mutation. Uncertainty is safe only
    // when it leaves the installed machine unchanged.
    if !generation_is_current(home, generation) {
        println!("supervise\tstopped because its generation is stale or missing");
        return Ok(());
    }

    eprintln!(
        "supervise\tthe daemon is not running {expect} after {}s; restoring {}",
        SUPERVISE_READY_TIMEOUT.as_secs(),
        rollback.display()
    );

    let installed_path = managed_binary_path()?;
    restore_rollback_machine(
        &installed_path,
        rollback,
        crate::service::prepare_update_rollback,
        |installed, companion_exists| {
            crate::service::restore_after_update_rollback(home, installed, companion_exists)
        },
    )?;
    if let Err(error) = crate::gitremote::install_helper_for(&installed_path) {
        eprintln!("supervise\tGit helper repair failed: {error:#}");
    }
    if crate::service::wait_for_control_socket(home, SUPERVISE_READY_TIMEOUT) {
        eprintln!("supervise\trolled back and the daemon came back");
        return Ok(());
    }
    bail!(
        "rolled back to {} and the daemon still did not answer; this machine needs a person",
        rollback.display()
    );
}

fn restore_rollback_machine<P, R>(
    installed_path: &Path,
    rollback: &Path,
    prepare_service: P,
    restore_service: R,
) -> Result<bool>
where
    P: FnOnce(bool) -> Result<()>,
    R: FnOnce(&Path, bool) -> Result<()>,
{
    let companion_exists = restore_rollback_binaries(installed_path, rollback)?;
    prepare_service(companion_exists)?;
    restore_service(installed_path, companion_exists)?;
    Ok(companion_exists)
}

/// Restore a matched prior binary set. A missing companion rollback means the
/// prior release was fabric-only, so the newly installed companion is removed.
fn restore_rollback_binaries(installed_path: &Path, rollback: &Path) -> Result<bool> {
    let companion_path = companion_binary_path(&installed_path)?;
    let companion_rollback = companion_rollback_path(rollback)?;
    let bytes = std::fs::read(rollback)
        .with_context(|| format!("failed to read {}", rollback.display()))?;
    let stamp = timestamp();
    let staged = stage_binary(&installed_path, &bytes, &stamp)?;
    let staged_companion = if companion_rollback.exists() {
        let bytes = std::fs::read(&companion_rollback)
            .with_context(|| format!("failed to read {}", companion_rollback.display()))?;
        Some(stage_binary(&companion_path, &bytes, &stamp)?)
    } else {
        None
    };
    let previous_companion = match staged_companion {
        Some(staged) => {
            commit_staged(&staged, &companion_path, &stamp)?
        }
        None => {
            remove_with_rollback(&companion_path, &stamp)?
        }
    };
    if let Err(error) = commit_staged(&staged, &installed_path, &stamp) {
        restore_after_pair_commit_failure(&companion_path, &previous_companion)?;
        return Err(error.context("fabric rollback failed; fabric-sync was restored"));
    }
    Ok(companion_rollback.exists())
}

/// Wait until the RUNNING DAEMON reports `expect`, not merely until something
/// answers the control socket.
///
/// "A socket answers" is not verification. The supervisor is scheduled alongside
/// the restart, and systemd batches timers by up to `AccuracySec`, so the two can
/// fire together — at which point the OLD daemon is still up and still
/// answering. It observed exactly that on droppy: supervisor and restart both
/// ran at 10:58:39 and it reported success having possibly never seen the new
/// binary at all.
///
/// If the new binary were broken, that race would report a healthy machine while
/// the machine went down, which is the one outcome this whole mechanism exists to
/// prevent. So ask the daemon who it is.
async fn wait_for_daemon_version(
    home: &crate::config::FabricHome,
    generation: &str,
    expect: &str,
    timeout: std::time::Duration,
) -> VersionWaitDecision {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if !generation_is_current(home, generation) {
            return VersionWaitDecision::Superseded;
        }
        // ReachabilityStatus rather than Status, because only that reply
        // carries the daemon's version, which is the whole question here.
        let observed =
            if let Ok(crate::control::ControlResponse::ReachabilityStatus { version, .. }) =
                crate::daemon::send_control(
                    home,
                    crate::control::ControlRequest::ReachabilityStatus,
                )
                .await
            {
                Some(version)
            } else {
                None
            };
        match version_wait_decision(
            observed.as_deref(),
            expect,
            std::time::Instant::now() >= deadline,
        ) {
            VersionWaitDecision::Ready => return VersionWaitDecision::Ready,
            VersionWaitDecision::Rollback => return VersionWaitDecision::Rollback,
            VersionWaitDecision::Superseded => unreachable!(),
            VersionWaitDecision::Wait => {}
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VersionWaitDecision {
    Ready,
    Wait,
    Rollback,
    Superseded,
}

/// Check the observed daemon before the deadline.
///
/// A sleeping Mac can resume after the nominal deadline. If launchd completed
/// the restart while the updater slept, that healthy version must win.
fn version_wait_decision(
    observed: Option<&str>,
    expect: &str,
    deadline_reached: bool,
) -> VersionWaitDecision {
    if observed == Some(expect) {
        VersionWaitDecision::Ready
    } else if deadline_reached {
        VersionWaitDecision::Rollback
    } else {
        VersionWaitDecision::Wait
    }
}

/// Schedule the supervisor to run outside this process's cgroup.
///
/// The delay lets the restart that `service::install` scheduled actually happen
/// first: the supervisor is a verifier, not the thing that bounces the daemon.
/// The command that schedules the supervisor.
///
/// NOT cfg-gated, so it can be tested from a Mac. This is the shape whose
/// regression leaves a Linux machine down with no way in, which is exactly the
/// kind of thing that must not be verifiable only on the platform where it
/// hurts.
///
/// It runs the ROLLBACK binary, not the new one. The rollback is the copy
/// already proven to work on this machine; asking a binary that may be broken to
/// supervise its own installation is not supervision.
pub fn supervisor_argv(
    home: &Path,
    rollback: &Path,
    generation: &str,
    expect: &str,
) -> (&'static str, Vec<String>) {
    (
        "systemd-run",
        vec![
            "--user".into(),
            // WITHOUT THIS THE DELAY IS A SUGGESTION, and the finding is worth
            // more than the flag.
            //
            // systemd defaults to AccuracySec=1min and batches timers. On droppy
            // both this verifier and the +3s restart were scheduled at 10:58:16
            // and BOTH FIRED AT 10:58:39 — 23 seconds late, together. The two
            // delays encode an ORDER, restart then verify, and batching turned
            // that order into a coincidence.
            //
            // A future reader sees two sensible delays and has no way to know
            // they were not honoured.
            "--timer-property=AccuracySec=1s".into(),
            // After the +3s restart that `service::install` scheduled: this
            // verifies that restart, it does not perform it.
            "--on-active=12".into(),
            rollback.display().to_string(),
            "--home".into(),
            home.display().to_string(),
            "supervise-restart".into(),
            "--rollback".into(),
            rollback.display().to_string(),
            "--generation".into(),
            generation.into(),
            "--expect".into(),
            expect.into(),
        ],
    )
}

/// The launchd-owned wrapper waits until replacement starts, then asks the
/// known-good rollback binary to verify the daemon.
#[cfg(any(target_os = "macos", test))]
const MACOS_SUPERVISOR_SCRIPT: &str = r#"
target=$1
plist=$2
installed=$3
rollback=$4
expect=$5
home=$6
updater_pid=$7
launchctl=$8
generation_file=$9
generation=${10}

cleanup() {
    /bin/rm -f "$plist"
    "$launchctl" bootout "$target" >/dev/null 2>&1 &
}

generation_is_current() {
    test -r "$generation_file" && test "$(/bin/cat "$generation_file")" = "$generation"
}

# launchd owns this process before either installed path changes. A pause or
# sleep here is safe: the timeout in the rollback binary does not start until
# the first replacement is visible.
while /usr/bin/cmp -s "$installed" "$rollback"; do
    if ! generation_is_current; then
        cleanup
        exit 0
    fi
    if ! kill -0 "$updater_pid" 2>/dev/null; then
        cleanup
        exit 0
    fi
    /bin/sleep 1
done

# On macOS the updater survives its service restart. Do not spend the recovery
# timeout while that updater is asleep or still completing launchd work.
while kill -0 "$updater_pid" 2>/dev/null; do
    if ! generation_is_current; then
        cleanup
        exit 0
    fi
    /bin/sleep 1
done

if ! generation_is_current; then
    cleanup
    exit 0
fi

"$rollback" --home "$home" supervise-restart --rollback "$rollback" --generation "$generation" --expect "$expect"
status=$?

cleanup
exit "$status"
"#;

#[cfg(any(target_os = "macos", test))]
#[derive(Debug, Clone)]
struct MacosSupervisorSpec {
    label: String,
    target: String,
    plist: PathBuf,
    xml: String,
}

#[cfg(any(target_os = "macos", test))]
fn xml_escape_update(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(any(target_os = "macos", test))]
fn render_macos_supervisor(
    home: &Path,
    installed: &Path,
    rollback: &Path,
    generation: &str,
    expect: &str,
    uid: u32,
    updater_pid: u32,
    stamp: &str,
    launchctl: &Path,
) -> MacosSupervisorSpec {
    let label = format!("com.compoundingtech.fabric.update-supervisor.{stamp}.{updater_pid}");
    let domain = format!("gui/{uid}");
    let target = format!("{domain}/{label}");
    let plist = home
        .join("run")
        .join(format!("update-supervisor-{stamp}-{updater_pid}.plist"));
    let stdout = home.join("logs/update-supervisor.out.log");
    let stderr = home.join("logs/update-supervisor.err.log");
    let args = [
        "/bin/sh".to_string(),
        "-c".to_string(),
        MACOS_SUPERVISOR_SCRIPT.to_string(),
        "fabric-update-supervisor".to_string(),
        target.clone(),
        plist.display().to_string(),
        installed.display().to_string(),
        rollback.display().to_string(),
        expect.to_string(),
        home.display().to_string(),
        updater_pid.to_string(),
        launchctl.display().to_string(),
        home.join("run")
            .join(UPDATE_GENERATION_FILE)
            .display()
            .to_string(),
        generation.to_string(),
    ];
    let arguments = args
        .iter()
        .map(|arg| format!("        <string>{}</string>", xml_escape_update(arg)))
        .collect::<Vec<_>>()
        .join("\n");
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
<dict>\n\
    <key>Label</key>\n\
    <string>{}</string>\n\
    <key>ProgramArguments</key>\n\
    <array>\n\
{}\n\
    </array>\n\
    <key>RunAtLoad</key>\n\
    <true/>\n\
    <key>KeepAlive</key>\n\
    <false/>\n\
    <key>AbandonProcessGroup</key>\n\
    <true/>\n\
    <key>ProcessType</key>\n\
    <string>Background</string>\n\
    <key>StandardOutPath</key>\n\
    <string>{}</string>\n\
    <key>StandardErrorPath</key>\n\
    <string>{}</string>\n\
</dict>\n\
</plist>\n",
        xml_escape_update(&label),
        arguments,
        xml_escape_update(&stdout.display().to_string()),
        xml_escape_update(&stderr.display().to_string()),
    );
    MacosSupervisorSpec {
        label,
        target,
        plist,
        xml,
    }
}

#[cfg(any(target_os = "macos", test))]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let parent = path.parent().context("the private file has no directory")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&temporary, bytes)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to protect {}", temporary.display()))?;
    std::fs::rename(&temporary, path)
        .with_context(|| format!("failed to place {}", path.display()))?;
    Ok(())
}

#[cfg(any(target_os = "macos", test))]
fn register_macos_supervisor<F>(spec: &MacosSupervisorSpec, mut launchctl: F) -> Result<()>
where
    F: FnMut(&[&str]) -> Result<bool>,
{
    write_private_file(&spec.plist, spec.xml.as_bytes())?;
    let domain = spec
        .target
        .rsplit_once('/')
        .map(|(domain, _)| domain)
        .context("the launchd supervisor target has no domain")?;
    let plist = spec.plist.display().to_string();
    let bootstrap = launchctl(&["bootstrap", domain, &plist]);
    if !matches!(bootstrap, Ok(true)) {
        let _ = std::fs::remove_file(&spec.plist);
        return match bootstrap {
            Ok(false) => bail!("launchctl did not bootstrap the update supervisor"),
            Err(error) => Err(error.context("failed to bootstrap the update supervisor")),
            Ok(true) => unreachable!(),
        };
    }
    match launchctl(&["print", &spec.target]) {
        Ok(true) => Ok(()),
        result => {
            let _ = launchctl(&["bootout", &spec.target]);
            let _ = std::fs::remove_file(&spec.plist);
            match result {
                Ok(false) => bail!("launchd does not own the update supervisor"),
                Err(error) => Err(error.context("failed to verify the update supervisor")),
                Ok(true) => unreachable!(),
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn schedule_macos_supervisor(
    home: &crate::config::FabricHome,
    installed: &Path,
    rollback: &Path,
    generation: &str,
    expect: &str,
    stamp: &str,
) -> Result<()> {
    if !rollback.exists() {
        println!("supervise\tskipped, there is no previous binary to fall back to");
        return Ok(());
    }
    let spec = render_macos_supervisor(
        home.root(),
        installed,
        rollback,
        generation,
        expect,
        unsafe { libc::geteuid() },
        std::process::id(),
        stamp,
        Path::new("/bin/launchctl"),
    );
    register_macos_supervisor(&spec, |args| {
        let output = std::process::Command::new("/bin/launchctl")
            .args(args)
            .output()
            .context("failed to run launchctl")?;
        Ok(output.status.success())
    })?;
    println!(
        "supervise\tlaunchd owns {}; it is armed before replacement",
        spec.label
    );
    Ok(())
}

#[cfg(target_os = "macos")]
fn schedule_before_pair_commit(
    home: &crate::config::FabricHome,
    options: &UpdateOptions,
    installed: &Path,
    rollback: &Path,
    generation: &str,
    expect: &str,
    stamp: &str,
) -> Result<()> {
    if options.no_restart || !home.is_default_state_root() {
        return Ok(());
    }
    schedule_macos_supervisor(home, installed, rollback, generation, expect, stamp)
}

#[cfg(not(target_os = "macos"))]
fn schedule_before_pair_commit(
    _home: &crate::config::FabricHome,
    _options: &UpdateOptions,
    _installed: &Path,
    _rollback: &Path,
    _generation: &str,
    _expect: &str,
    _stamp: &str,
) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn schedule_supervisor(
    home: &crate::config::FabricHome,
    rollback: &Path,
    generation: &str,
    expect: &str,
) -> Result<()> {
    if !rollback.exists() {
        // A first install has no rollback, so there is nothing to heal back to
        // and nothing known-good to supervise with.
        println!("supervise\tskipped, there is no previous binary to fall back to");
        return Ok(());
    }
    let (program, args) = supervisor_argv(home.root(), rollback, generation, expect);
    let status = std::process::Command::new(program)
        .args(&args)
        .status()
        .context("failed to schedule the restart supervisor")?;
    if !status.success() {
        bail!("could not schedule the restart supervisor: {status}");
    }
    println!(
        "supervise\tscheduled, will restore {} if the daemon does not come back",
        rollback.display()
    );
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn schedule_supervisor(
    _home: &crate::config::FabricHome,
    _rollback: &Path,
    _generation: &str,
    _expect: &str,
) -> Result<()> {
    // macOS installs verify in place: `launchctl kickstart` does not tear down
    // the caller, so `service::install` already waits for the control socket and
    // fails loudly if it never answers.
    Ok(())
}

/// What the caller asked for.
#[derive(Debug, Clone, Default)]
pub struct UpdateOptions {
    pub tag: Option<String>,
    pub url: Option<String>,
    pub sha256: Option<String>,
    pub check: bool,
    pub dry_run: bool,
    pub no_restart: bool,
    pub rollback: bool,
    pub allow_downgrade: bool,
}

/// Run an update. Returns the process exit code, which only `--check` uses for
/// anything other than success.
pub async fn run(home: &crate::config::FabricHome, options: UpdateOptions) -> Result<i32> {
    let installed_path = managed_binary_path()?;
    let installed = binary_version(&installed_path).unwrap_or_else(|_| "unknown".into());
    let companion_path = companion_binary_path(&installed_path)?;
    println!("installed\t{installed}");
    println!("path\t{}", installed_path.display());

    if options.rollback {
        return roll_back(home, &installed_path, &options).await;
    }
    if companion_path.exists() {
        let companion = binary_version(&companion_path)?;
        if companion != installed {
            bail!(
                "the installed pair does not match: fabric is {installed}, but fabric-sync is {companion}"
            );
        }
    }

    let source = resolve_source(
        options.tag.clone(),
        options.url.clone(),
        options.sha256.clone(),
    )?;
    let (url, expected_hash, expected_version) = resolve_artifact(&source).await?;
    println!("source\t{url}");

    let release_direction = if let Some(available) = &expected_version {
        match release_direction(&installed, available).await {
            Ok(direction) => Some(direction),
            Err(error) if options.allow_downgrade => {
                eprintln!(
                    "WARNING: cannot compare {installed} with {available}: {error:#}. \
                     --allow-downgrade permits the replacement."
                );
                None
            }
            Err(error) => {
                return Err(error.context(format!(
                    "cannot prove that {available} is newer than {installed}. \
                     Pass --allow-downgrade to replace it explicitly"
                )));
            }
        }
    } else {
        None
    };

    if options.check {
        let Some(available) = expected_version else {
            bail!(
                "--check compares released versions, and --url names an artifact whose \
                 version is only knowable by downloading it. Run without --check."
            );
        };
        println!("available\t{available}");
        if release_direction == Some(ReleaseDirection::Current) {
            println!("up to date");
            return Ok(CHECK_EXIT_CURRENT);
        }
        if release_direction == Some(ReleaseDirection::Downgrade) {
            println!("installed build is ahead of the latest release");
            return Ok(CHECK_EXIT_CURRENT);
        }
        if release_direction == Some(ReleaseDirection::Diverged) {
            println!("latest release diverges from the installed build");
            return Ok(CHECK_EXIT_ERROR);
        }
        println!("update available");
        return Ok(CHECK_EXIT_AVAILABLE);
    }

    if release_direction == Some(ReleaseDirection::Current) {
        println!("up to date");
        return Ok(0);
    }
    if let (Some(available), Some(direction)) = (&expected_version, release_direction) {
        enforce_release_direction(&installed, available, direction, options.allow_downgrade)?;
    }
    if matches!(
        release_direction,
        Some(ReleaseDirection::Downgrade | ReleaseDirection::Diverged)
    ) {
        eprintln!(
            "WARNING: replacing {installed} with {} because --allow-downgrade was set",
            expected_version
                .as_deref()
                .expect("a release has a version")
        );
    }

    let archive = fetch(&url).await?;
    verify_sha256(&archive, &expected_hash)?;
    println!("checksum\tok");

    let binaries = extract_release_binaries(&archive)?;
    let stamp = timestamp();
    let staged = stage_binary(&installed_path, &binaries.fabric, &stamp)?;
    let staged_companion = match stage_binary(&companion_path, &binaries.fabric_sync, &stamp) {
        Ok(path) => path,
        Err(error) => {
            let _ = std::fs::remove_file(&staged);
            return Err(error);
        }
    };

    // Ask the binary what it is before trusting it with the name of the one
    // that is running. A staged file that cannot answer is not installed.
    let staged_version = match binary_version(&staged) {
        Ok(version) => version,
        Err(error) => {
            let _ = std::fs::remove_file(&staged);
            let _ = std::fs::remove_file(&staged_companion);
            return Err(error.context("the downloaded binary could not run, so nothing was installed"));
        }
    };
    let companion_version = match binary_version(&staged_companion) {
        Ok(version) => version,
        Err(error) => {
            let _ = std::fs::remove_file(&staged);
            let _ = std::fs::remove_file(&staged_companion);
            return Err(error.context("the downloaded companion could not run, so nothing was installed"));
        }
    };
    if companion_version != staged_version {
        let _ = std::fs::remove_file(&staged);
        let _ = std::fs::remove_file(&staged_companion);
        bail!(
            "fabric reports {staged_version}, but fabric-sync reports {companion_version}; nothing was installed"
        );
    }
    println!("downloaded\t{staged_version}");

    if let Some(expected) = &expected_version
        && &staged_version != expected
    {
        let _ = std::fs::remove_file(&staged);
        let _ = std::fs::remove_file(&staged_companion);
        bail!(
            "the release claims to be {expected} but the binary in it reports \
             {staged_version}; nothing was installed"
        );
    }

    if options.dry_run {
        let _ = std::fs::remove_file(&staged);
        let _ = std::fs::remove_file(&staged_companion);
        println!();
        println!("DRY RUN: verified and stopped. Nothing was changed.");
        return Ok(0);
    }

    if let Err(error) = crate::gitremote::validate_helper_install(&installed_path) {
        let _ = std::fs::remove_file(&staged);
        let _ = std::fs::remove_file(&staged_companion);
        return Err(error.context("the Git helper path is unsafe, so nothing was installed"));
    }

    // Advance the generation before any installed path changes. This invalidates
    // every older supervisor, including one whose job survived its own cleanup.
    let generation = new_update_generation();
    write_update_generation(home, &generation)?;

    let (rollback, companion_rollback) = match commit_staged_pair(
        &staged,
        &installed_path,
        &staged_companion,
        &companion_path,
        &stamp,
        |rollback, _companion_rollback| {
            schedule_before_pair_commit(
                home,
                &options,
                &installed_path,
                rollback,
                &generation,
                &staged_version,
                &stamp,
            )
        },
    ) {
        Ok(pair) => pair,
        Err(error) => {
            let _ = std::fs::remove_file(&staged);
            let _ = std::fs::remove_file(&staged_companion);
            return Err(error);
        }
    };
    let helper = crate::gitremote::install_helper_for(&installed_path)?;
    println!("installed\t{}", binary_version(&installed_path)?);
    println!("companion\t{}", binary_version(&companion_path)?);
    println!("helper\t{}", helper.display());
    if rollback.exists() {
        println!("rollback\t{}", rollback.display());
    }
    if companion_rollback.exists() {
        println!("rollback companion\t{}", companion_rollback.display());
    }

    let running = binary_version(&installed_path)?;
    finish(home, &options, &rollback, &generation, &running)?;
    Ok(0)
}

/// Work out where the bytes come from, what hash they must have, and what
/// version they claim, without downloading the artifact itself.
async fn resolve_artifact(source: &Source) -> Result<(String, String, Option<String>)> {
    match source {
        Source::Explicit { url, sha256 } => Ok((url.clone(), sha256.clone(), None)),
        Source::Release { tag } => {
            let tag = match tag {
                Some(tag) => tag.clone(),
                None => latest_tag().await?,
            };
            let asset = asset_name(target_triple()?);
            let url = release_asset_url(&tag, &asset);
            let sidecar = fetch(&format!("{url}.sha256")).await?;
            let hash = parse_sha256_sidecar(&String::from_utf8_lossy(&sidecar))?;
            Ok((url, hash, Some(version_for_tag(&tag).to_string())))
        }
    }
}

/// Put the most recent rollback binary back.
async fn roll_back(
    home: &crate::config::FabricHome,
    installed_path: &Path,
    options: &UpdateOptions,
) -> Result<i32> {
    let Some(rollback) = newest_rollback(installed_path)? else {
        bail!(
            "there is no rollback binary beside {}, so there is nothing to go back to",
            installed_path.display()
        );
    };
    let version = binary_version(&rollback)?;
    let companion_path = companion_binary_path(installed_path)?;
    let companion_rollback = companion_rollback_path(&rollback)?;
    if companion_rollback.exists() {
        let companion_version = binary_version(&companion_rollback)?;
        if companion_version != version {
            bail!(
                "the rollback pair does not match: fabric is {version}, but fabric-sync is {companion_version}"
            );
        }
    }
    let generation = new_update_generation();
    write_update_generation(home, &generation)?;
    println!("rolling back to\t{version}");
    println!("from\t{}", rollback.display());

    let bytes = std::fs::read(&rollback)
        .with_context(|| format!("failed to read {}", rollback.display()))?;
    let stamp = timestamp();
    let staged = stage_binary(installed_path, &bytes, &stamp)?;
    let staged_companion = if companion_rollback.exists() {
        let bytes = std::fs::read(&companion_rollback)
            .with_context(|| format!("failed to read {}", companion_rollback.display()))?;
        match stage_binary(&companion_path, &bytes, &stamp) {
            Ok(path) => Some(path),
            Err(error) => {
                let _ = std::fs::remove_file(&staged);
                return Err(error);
            }
        }
    } else {
        None
    };
    if let Err(error) = crate::gitremote::validate_helper_install(installed_path) {
        let _ = std::fs::remove_file(&staged);
        if let Some(path) = &staged_companion {
            let _ = std::fs::remove_file(path);
        }
        return Err(error.context("the Git helper path is unsafe, so nothing was installed"));
    }
    let previous_companion = match staged_companion {
        Some(staged) => commit_staged(&staged, &companion_path, &stamp)?,
        None => remove_with_rollback(&companion_path, &stamp)?,
    };
    let previous = match commit_staged(&staged, installed_path, &stamp) {
        Ok(path) => path,
        Err(error) => {
            restore_after_pair_commit_failure(&companion_path, &previous_companion)?;
            return Err(error.context("fabric could not be rolled back; fabric-sync was restored"));
        }
    };
    let helper = crate::gitremote::install_helper_for(installed_path)?;
    println!("installed\t{}", binary_version(installed_path)?);
    if companion_path.exists() {
        println!("companion\t{}", binary_version(&companion_path)?);
    } else {
        println!("companion\tabsent in rollback target");
    }
    println!("helper\t{}", helper.display());
    if previous.exists() {
        println!("rollback\t{}", previous.display());
    }
    if previous_companion.exists() {
        println!("rollback companion\t{}", previous_companion.display());
    }
    let running = binary_version(installed_path)?;
    finish(home, options, &previous, &generation, &running)?;
    Ok(0)
}

/// Re-render the unit and restart, unless told not to.
fn finish(
    home: &crate::config::FabricHome,
    options: &UpdateOptions,
    rollback: &Path,
    generation: &str,
    expect: &str,
) -> Result<()> {
    if options.no_restart {
        println!("restart\tskipped");
        println!();
        println!("The new binary is in place but the running daemon is still the old one.");
        return Ok(());
    }
    if !home.is_default_state_root() {
        // `service::install` refuses a non-default home on purpose, so say why
        // rather than letting it fail with a message about something else.
        println!("restart\tskipped");
        println!();
        println!(
            "This --home is not the managed one, so there is no service to re-render \
             or restart."
        );
        return Ok(());
    }

    // Re-render the unit and restart in one step. Passing every option as `None`
    // keeps allow-shell, allow-exec and any memory ceiling exactly as they were:
    // they round trip through config.toml rather than being re-derived here.
    // Re-render pointing at the MANAGED binary, not at whatever is running this
    // command. `service::install` would use `current_exe`, which during a manual
    // test is a `target/debug` build.
    let managed = managed_binary_path()?;
    crate::service::install_at(
        home,
        &managed,
        crate::service::ServiceInstallOptions {
            allow_shell: None,
            allow_exec: None,
            memory_max_mb: None,
        },
    )?;
    schedule_supervisor(home, rollback, generation, expect)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a tarball the way the release workflow does, so the archive checks
    /// are tested against real bytes rather than a mock of them.
    fn make_archive(members: &[(&str, &[u8])]) -> Vec<u8> {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        let mut builder = tar::Builder::new(encoder);
        for (name, bytes) in members {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            builder.append_data(&mut header, name, *bytes).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    const HASH: &str = "e6aac12fcf8be256aa713a017cfcd8d4e258f5f9f42e5bf8911ff189b73a1214";

    #[test]
    fn an_older_release_is_refused_with_both_versions_named() {
        let error = enforce_release_direction(
            "0.2.0+7f4da21",
            "0.2.0+593a3f7",
            ReleaseDirection::Downgrade,
            false,
        )
        .expect_err("a downgrade was accepted without an override");
        let message = error.to_string();
        assert!(message.contains("0.2.0+7f4da21"));
        assert!(message.contains("0.2.0+593a3f7"));
        assert!(message.contains("--allow-downgrade"));
    }

    #[test]
    fn an_explicit_downgrade_override_is_accepted() {
        enforce_release_direction(
            "0.2.0+7f4da21",
            "0.2.0+593a3f7",
            ReleaseDirection::Downgrade,
            true,
        )
        .expect("the explicit downgrade override was refused");
    }

    #[test]
    fn github_compare_status_has_one_unambiguous_direction() {
        assert_eq!(
            release_direction_from_compare("ahead").unwrap(),
            ReleaseDirection::Upgrade
        );
        assert_eq!(
            release_direction_from_compare("behind").unwrap(),
            ReleaseDirection::Downgrade
        );
        assert_eq!(
            release_direction_from_compare("identical").unwrap(),
            ReleaseDirection::Current
        );
        assert_eq!(
            release_direction_from_compare("diverged").unwrap(),
            ReleaseDirection::Diverged
        );
    }

    /// The one refusal that matters. `--url` with no hash means running bytes
    /// nobody checked, and there is nothing to default to: the whole point of
    /// `--url` is that fabric does not know what is there.
    #[test]
    fn an_explicit_url_without_a_hash_is_refused() {
        let error = resolve_source(None, Some("https://example.test/f.tar.gz".into()), None)
            .expect_err("an unverified url was accepted");
        let message = format!("{error}");
        assert!(
            message.contains("--sha256"),
            "the refusal must name what is missing: {message}"
        );
    }

    #[test]
    fn an_explicit_url_with_a_hash_is_accepted_and_the_hash_normalised() {
        let source = resolve_source(
            None,
            Some("file:///tmp/f.tar.gz".into()),
            // Pasted with stray case and whitespace, as a person would.
            Some(format!("  {}  ", HASH.to_ascii_uppercase())),
        )
        .expect("a hashed url was refused");
        assert_eq!(
            source,
            Source::Explicit {
                url: "file:///tmp/f.tar.gz".into(),
                sha256: HASH.into(),
            }
        );
    }

    #[test]
    fn a_hash_that_is_not_a_sha256_is_refused() {
        for bad in ["deadbeef", "", &"z".repeat(64)] {
            assert!(
                resolve_source(None, Some("file:///f".into()), Some(bad.into())).is_err(),
                "accepted {bad:?} as a sha256"
            );
        }
    }

    #[test]
    fn naming_two_sources_at_once_is_refused() {
        assert!(
            resolve_source(Some("v0.2.0".into()), Some("file:///f".into()), Some(HASH.into()))
                .is_err(),
            "--tag and --url name different artifacts and must not combine"
        );
    }

    #[test]
    fn a_hash_without_a_url_is_refused() {
        assert!(
            resolve_source(None, None, Some(HASH.into())).is_err(),
            "a release carries its own checksum, so --sha256 alone is a mistake worth naming"
        );
    }

    #[test]
    fn no_options_means_the_latest_release() {
        assert_eq!(
            resolve_source(None, None, None).unwrap(),
            Source::Release { tag: None }
        );
    }

    /// The sidecar carries the path it had on the builder, which does not exist
    /// here. Taking field one is the difference between checking the bytes and
    /// failing on a directory name.
    #[test]
    fn the_sidecar_builder_path_is_ignored() {
        let sidecar = format!("{HASH}  dist/fabric-aarch64-apple-darwin.tar.gz\n");
        assert_eq!(parse_sha256_sidecar(&sidecar).unwrap(), HASH);
    }

    #[test]
    fn a_checksum_mismatch_is_an_error_that_names_both_sides() {
        let error = verify_sha256(b"not the release", HASH).expect_err("a mismatch was accepted");
        let message = format!("{error}");
        assert!(message.contains(HASH), "the expected hash is missing: {message}");
        assert!(
            message.contains("nothing was installed"),
            "a mismatch must say that it changed nothing: {message}"
        );
    }

    #[test]
    fn a_matching_checksum_passes() {
        use sha2::{Digest, Sha256};
        let bytes = b"some release bytes";
        let hash = Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        verify_sha256(bytes, &hash).expect("a matching checksum was rejected");
    }

    #[test]
    fn a_release_archive_yields_the_matched_pair() {
        let archive = make_archive(&[
            ("fabric", b"daemon ELF-ish"),
            ("fabric-sync", b"companion ELF-ish"),
        ]);
        let binaries = extract_release_binaries(&archive).unwrap();
        assert_eq!(binaries.fabric, b"daemon ELF-ish");
        assert_eq!(binaries.fabric_sync, b"companion ELF-ish");
    }

    /// Write a tar header by hand so the stored name is EXACTLY what we say.
    ///
    /// `tar::Builder` normalises `./fabric` to `fabric` on the way in, so it
    /// cannot produce the archive shape this test is about. GNU tar does produce
    /// it — `tar -czf x.tgz .` stores the dot-slash — so the fixture has to be
    /// built by hand or the test would silently be checking `fabric`.
    fn make_archive_with_raw_name(name: &str, bytes: &[u8]) -> Vec<u8> {
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        header[100..107].copy_from_slice(b"0000755");
        header[108..115].copy_from_slice(b"0000000");
        header[116..123].copy_from_slice(b"0000000");
        let size = format!("{:011o}", bytes.len());
        header[124..135].copy_from_slice(size.as_bytes());
        header[136..147].copy_from_slice(b"00000000000");
        header[148..156].copy_from_slice(b"        ");
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum: u32 = header.iter().map(|byte| u32::from(*byte)).sum();
        let checksum = format!("{checksum:06o}\0 ");
        header[148..156].copy_from_slice(checksum.as_bytes());

        let mut tar = Vec::new();
        tar.extend_from_slice(&header);
        tar.extend_from_slice(bytes);
        tar.resize(tar.len().div_ceil(512) * 512, 0);
        tar.extend_from_slice(&[0u8; 1024]);

        use std::io::Write;
        let mut encoder =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&tar).unwrap();
        encoder.finish().unwrap()
    }

    /// `./fabric` is what GNU tar stores for `tar -czf x.tgz .`, and it is not
    /// what the release workflow emits. Accepting it would mean accepting
    /// archives we did not build.
    #[test]
    fn an_archive_whose_member_is_dot_slash_fabric_is_refused() {
        // The fixture really does carry the dot-slash, or this proves nothing.
        let archive = make_archive_with_raw_name("./fabric", b"ELF-ish");
        let decoder = flate2::read::GzDecoder::new(&archive[..]);
        let mut probe = tar::Archive::new(decoder);
        let stored = probe
            .entries()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path_bytes()
            .into_owned();
        assert_eq!(
            stored.as_slice(),
            b"./fabric",
            "the fixture was normalised, so the test below would prove nothing"
        );

        assert!(
            extract_release_binaries(&archive).is_err(),
            "./fabric is not the member name a fabric release has"
        );
    }

    #[test]
    fn an_archive_with_an_extra_member_is_refused() {
        let archive = make_archive(&[
            ("fabric", b"ELF-ish"),
            ("fabric-sync", b"also ELF-ish"),
            ("install.sh", b"rm -rf /"),
        ]);
        assert!(
            extract_release_binaries(&archive).is_err(),
            "a release holds two members; a third one is somebody else's archive"
        );
    }

    #[test]
    fn an_archive_without_fabric_is_refused() {
        let archive = make_archive(&[("fabric", b"ELF-ish"), ("something-else", b"nope")]);
        assert!(extract_release_binaries(&archive).is_err());
    }

    #[test]
    fn a_tag_names_the_version_the_binary_will_report() {
        assert_eq!(version_for_tag("v0.2.0+76376d4"), "0.2.0+76376d4");
        // Idempotent, because a caller may paste either form.
        assert_eq!(version_for_tag("0.2.0+76376d4"), "0.2.0+76376d4");
    }

    #[test]
    fn the_release_url_matches_what_the_workflow_publishes() {
        let asset = asset_name("aarch64-apple-darwin");
        assert_eq!(asset, "fabric-aarch64-apple-darwin.tar.gz");
        assert_eq!(
            release_asset_url("v0.2.0+76376d4", &asset),
            "https://github.com/compoundingtech/fabric/releases/download/v0.2.0+76376d4/fabric-aarch64-apple-darwin.tar.gz"
        );
    }

    #[test]
    fn the_release_a_workflow_packages_only_the_reader() {
        let workflow = include_str!("../.github/workflows/release.yml");
        assert!(workflow.contains("tar -czf \"$archive\" -C dist/package fabric\n"));
        assert!(workflow.contains("[[ \"$members\" != \"fabric\" ]]"));
        assert!(!workflow.contains("release/fabric-sync\" dist/package/fabric-sync"));
        assert!(!workflow.contains("-C dist/package fabric fabric-sync"));
    }
}

#[cfg(test)]
mod io_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// `file:///` is not a convenience. It is how a locally built artifact gets
    /// installed through exactly the same path a release does, hash check
    /// included, so the custom-build route is not a second untested code path.
    #[tokio::test]
    async fn a_file_url_is_read_and_hashed_like_any_other_source() {
        use sha2::{Digest, Sha256};
        let dir = tempfile::tempdir().unwrap();
        let artifact = dir.path().join("fabric.tar.gz");
        std::fs::write(&artifact, b"pretend archive").unwrap();

        let url = format!("file://{}", artifact.display());
        let bytes = fetch(&url).await.expect("a file url could not be read");
        assert_eq!(bytes, b"pretend archive");

        let hash = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        verify_sha256(&bytes, &hash).expect("the same bytes failed their own hash");
    }

    #[tokio::test]
    async fn a_scheme_we_do_not_understand_is_refused() {
        for url in ["http://example.test/f", "ftp://example.test/f", "/tmp/f"] {
            let error = fetch(url).await.expect_err("accepted {url}");
            assert!(
                format!("{error}").contains("only https:// and file:///"),
                "the refusal should say what is accepted: {error}"
            );
        }
    }

    /// The binary that was there must still be reachable after an install, or a
    /// bad update has nothing to fall back to.
    #[test]
    fn installing_keeps_the_previous_binary_as_a_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fabric");
        std::fs::write(&path, b"the old binary").unwrap();

        let rollback = install_binary(&path, b"the new binary", "stamp").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"the new binary");
        assert_eq!(
            std::fs::read(&rollback).unwrap(),
            b"the old binary",
            "the previous binary was not preserved, so there is nothing to roll back to"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o755,
            "an installed binary that is not executable is not installed"
        );
    }

    /// A first install has nothing to preserve and must not invent something.
    #[test]
    fn installing_where_nothing_was_leaves_no_empty_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fabric");
        let rollback = install_binary(&path, b"the new binary", "stamp").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"the new binary");
        assert!(
            !rollback.exists(),
            "a rollback was created for a binary that never existed"
        );
    }

    #[test]
    fn one_stamp_stages_and_preserves_a_matched_pair() {
        let dir = tempfile::tempdir().unwrap();
        let fabric = dir.path().join("fabric");
        let companion = dir.path().join("fabric-sync");
        std::fs::write(&fabric, b"old daemon").unwrap();
        std::fs::write(&companion, b"old companion").unwrap();

        let staged_fabric = stage_binary(&fabric, b"new daemon", "pair").unwrap();
        let staged_companion = stage_binary(&companion, b"new companion", "pair").unwrap();
        assert_ne!(staged_fabric, staged_companion);
        let rollback = commit_staged(&staged_fabric, &fabric, "pair").unwrap();
        let companion_rollback = commit_staged(&staged_companion, &companion, "pair").unwrap();

        assert_eq!(std::fs::read(&rollback).unwrap(), b"old daemon");
        assert_eq!(
            companion_rollback_path(&rollback).unwrap(),
            companion_rollback
        );
        assert_eq!(
            std::fs::read(&companion_rollback).unwrap(),
            b"old companion"
        );
    }

    #[test]
    fn supervision_is_armed_before_the_first_pair_rename() {
        let dir = tempfile::tempdir().unwrap();
        let fabric = dir.path().join("fabric");
        let companion = dir.path().join("fabric-sync");
        std::fs::write(&fabric, b"old daemon").unwrap();
        std::fs::write(&companion, b"old companion").unwrap();
        let staged = stage_binary(&fabric, b"new daemon", "ordered").unwrap();
        let staged_companion = stage_binary(&companion, b"new companion", "ordered").unwrap();

        let (rollback, companion_rollback) = commit_staged_pair(
            &staged,
            &fabric,
            &staged_companion,
            &companion,
            "ordered",
            |rollback, companion_rollback| {
                assert_eq!(std::fs::read(&fabric)?, b"old daemon");
                assert_eq!(std::fs::read(&companion)?, b"old companion");
                assert_eq!(std::fs::read(rollback)?, b"old daemon");
                assert_eq!(std::fs::read(companion_rollback)?, b"old companion");
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(std::fs::read(&fabric).unwrap(), b"new daemon");
        assert_eq!(std::fs::read(&companion).unwrap(), b"new companion");
        assert_eq!(std::fs::read(rollback).unwrap(), b"old daemon");
        assert_eq!(std::fs::read(companion_rollback).unwrap(), b"old companion");
    }

    /// Nothing partial may survive a run. A leftover `.fabric-incoming-*` beside
    /// the binary would be a half-written binary sitting in the install
    /// directory, which is the sort of thing somebody later runs by mistake.
    #[test]
    fn installing_leaves_no_temporary_files_behind() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fabric");
        std::fs::write(&path, b"old").unwrap();
        install_binary(&path, b"new", "stamp").unwrap();

        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".fabric-"))
            .collect();
        assert!(leftovers.is_empty(), "temporary files were left: {leftovers:?}");
    }

    #[test]
    fn the_newest_rollback_is_the_one_offered() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fabric");
        std::fs::write(&path, b"current").unwrap();

        assert!(
            newest_rollback(&path).unwrap().is_none(),
            "a rollback was offered when none had been made"
        );

        let first = install_binary(&path, b"second", "1000").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let second = install_binary(&path, b"third", "2000").unwrap();

        let newest = newest_rollback(&path).unwrap().expect("no rollback found");
        assert_eq!(
            newest, second,
            "rolling back would have gone to {first:?} rather than the most recent"
        );
        assert_eq!(std::fs::read(&newest).unwrap(), b"second");
    }
}

#[cfg(test)]
mod supervisor_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    const REAL_PAIR_WORKER: &str = "FABRIC_REAL_PAIR_ROLLBACK_WORKER";
    const REAL_PAIR_INSTALLED: &str = "FABRIC_REAL_PAIR_INSTALLED";
    const REAL_PAIR_ROLLBACK: &str = "FABRIC_REAL_PAIR_ROLLBACK";

    fn executable(path: &Path, body: &str) {
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn run_real_pair_worker() -> bool {
        if std::env::var_os(REAL_PAIR_WORKER).is_none() {
            return false;
        }
        let installed = PathBuf::from(std::env::var_os(REAL_PAIR_INSTALLED).unwrap());
        let rollback = PathBuf::from(std::env::var_os(REAL_PAIR_ROLLBACK).unwrap());
        let companion = companion_binary_path(&installed).unwrap();
        let companion_rollback = companion_rollback_path(&rollback).unwrap();

        assert_ne!(
            std::fs::read(&installed).unwrap(),
            std::fs::read(&rollback).unwrap()
        );
        assert_ne!(
            std::fs::read(&companion).unwrap(),
            std::fs::read(&companion_rollback).unwrap()
        );
        let companion_restored = restore_rollback_machine(
            &installed,
            &rollback,
            |exists| {
                assert!(exists, "the supervisor did not find the companion rollback");
                Ok(())
            },
            |path, exists| {
                assert_eq!(path, installed);
                assert!(exists, "the supervisor would restore only fabric");
                Ok(())
            },
        )
        .unwrap();

        assert!(companion_restored);
        assert_eq!(
            std::fs::read(&installed).unwrap(),
            std::fs::read(&rollback).unwrap()
        );
        assert_eq!(
            std::fs::read(&companion).unwrap(),
            std::fs::read(&companion_rollback).unwrap()
        );
        true
    }

    fn real_pair_fixture(
        dir: &Path,
        test_name: &str,
    ) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        let installed = dir.join("fabric");
        let companion = dir.join("fabric-sync");
        let rollback = dir.join("fabric.rollback-real");
        let companion_rollback = dir.join("fabric-sync.rollback-real");
        executable(
            &installed,
            "#!/bin/sh\n# broken candidate fabric\nexit 99\n",
        );
        executable(
            &companion,
            "#!/bin/sh\n# broken candidate fabric-sync\nexit 98\n",
        );
        executable(
            &companion_rollback,
            "#!/bin/sh\n# known-good fabric-sync\nexit 0\n",
        );
        let test_exe = std::env::current_exe().unwrap();
        executable(
            &rollback,
            &format!(
                "#!/bin/sh\n{}=1 \\\n{}='{}' \\\n{}='{}' \\\nexec '{}' --exact '{}' --ignored --nocapture\n",
                REAL_PAIR_WORKER,
                REAL_PAIR_INSTALLED,
                installed.display(),
                REAL_PAIR_ROLLBACK,
                rollback.display(),
                test_exe.display(),
                test_name,
            ),
        );
        (installed, companion, rollback, companion_rollback)
    }

    fn run_macos_wrapper(
        installed: &Path,
        rollback: &Path,
        plist: &Path,
        launchctl: &Path,
        generation_file: &Path,
        generation: &str,
    ) -> std::process::ExitStatus {
        std::process::Command::new("/bin/sh")
            .args(["-c", MACOS_SUPERVISOR_SCRIPT, "fabric-update-supervisor"])
            .arg("gui/501/com.compoundingtech.fabric.update-supervisor.test")
            .arg(plist)
            .arg(installed)
            .arg(rollback)
            .arg("fabric 0.2.1+new")
            .arg("/tmp/fabric-home")
            .arg(u32::MAX.to_string())
            .arg(launchctl)
            .arg(generation_file)
            .arg(generation)
            .status()
            .unwrap()
    }

    fn wait_for_call(path: &Path, expected: &str) -> String {
        for _ in 0..100 {
            if let Ok(calls) = std::fs::read_to_string(path)
                && calls.contains(expected)
            {
                return calls;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("launchctl did not receive {expected}")
    }

    /// The supervisor must run OUTSIDE the caller's cgroup, and it must run the
    /// binary already proven to work on this machine.
    ///
    /// Pinned as a shape rather than an effect, and deliberately not cfg-gated to
    /// Linux. The failure this guards against leaves a remote machine down with
    /// no way back in, so it cannot be a thing only testable on the platform
    /// where it does the damage.
    #[test]
    fn the_supervisor_runs_detached_and_uses_the_known_good_binary() {
        let rollback = Path::new("/home/n/.local/bin/fabric.rollback-1000");
        let (program, args) = supervisor_argv(
            Path::new("/home/n/.local/share/fabric"),
            rollback,
            "generation-1",
            "0.2.0+abc1234",
        );

        assert_eq!(
            program, "systemd-run",
            "the supervisor would die with the cgroup it is meant to outlive"
        );
        assert!(
            args.iter().any(|a| a.starts_with("--on-active=")),
            "the supervisor must be scheduled, not run inline: {args:?}"
        );

        // It runs the rollback binary. Asking a possibly-broken new binary to
        // supervise its own installation is not supervision.
        assert_eq!(
            args.iter().find(|a| a.contains("fabric.rollback-")),
            Some(&rollback.display().to_string()),
            "the supervisor is not the known-good binary: {args:?}"
        );
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert!(
            args.windows(2)
                .any(|w| w == ["--rollback", &rollback.display().to_string()]),
            "the supervisor was not told what to restore: {args:?}"
        );
        assert!(
            args.contains(&"supervise-restart"),
            "the supervisor does not invoke the supervising subcommand: {args:?}"
        );
        assert!(
            args.windows(2)
                .any(|window| window == ["--generation", "generation-1"]),
            "the supervisor cannot reject a stale update: {args:?}"
        );
    }

    /// The two delays encode an ORDER: restart at +3s, verify at +12s. systemd
    /// defaults to `AccuracySec=1min` and batches timers, which on droppy fired
    /// both at the same instant and collapsed that order.
    #[test]
    fn the_supervisor_timer_is_accurate_enough_for_its_own_delay() {
        let (_, args) = supervisor_argv(
            Path::new("/home/n/.local/share/fabric"),
            Path::new("/home/n/.local/bin/fabric.rollback-1000"),
            "generation-1",
            "0.2.0+abc1234",
        );
        assert!(
            args.iter().any(|a| a == "--timer-property=AccuracySec=1s"),
            "without this systemd may fire the verifier alongside the restart it \
             is meant to verify: {args:?}"
        );
    }

    /// The supervisor must be told WHICH version counts as a healthy restart.
    /// Without it the only question it can ask is whether something answers the
    /// socket, and the old daemon answers right up until it is torn down.
    #[test]
    fn the_supervisor_is_told_what_a_healthy_restart_looks_like() {
        let (_, args) = supervisor_argv(
            Path::new("/home/n/.local/share/fabric"),
            Path::new("/home/n/.local/bin/fabric.rollback-1000"),
            "generation-1",
            "0.2.0+abc1234",
        );
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert!(
            args.windows(2).any(|w| w == ["--expect", "0.2.0+abc1234"]),
            "the supervisor cannot tell the new daemon from the old one: {args:?}"
        );
    }

    #[test]
    fn a_wake_checks_the_daemon_before_an_expired_deadline() {
        assert_eq!(
            version_wait_decision(Some("new"), "new", true),
            VersionWaitDecision::Ready,
            "a healthy restart seen after wake was rolled back"
        );
        assert_eq!(
            version_wait_decision(Some("old"), "new", true),
            VersionWaitDecision::Rollback,
            "an unhealthy restart survived the same wake"
        );
    }

    #[test]
    fn an_older_fabric_only_supervisor_restores_a_broken_first_pair() {
        let dir = tempfile::tempdir().unwrap();
        let installed = dir.path().join("fabric");
        let companion = dir.path().join("fabric-sync");
        let rollback = dir.path().join("fabric.rollback-release-a");
        executable(&installed, "#!/bin/sh\n# broken paired fabric\nexit 99\n");
        executable(&companion, "#!/bin/sh\n# new companion\nexit 99\n");
        executable(&rollback, "#!/bin/sh\n# proven Release A fabric\nexit 0\n");
        let expected = std::fs::read(&rollback).unwrap();
        let home = crate::config::FabricHome::new(dir.path().join("home"));
        let main_definition = dir.path().join("fabric.service");
        let companion_definition = dir.path().join("fabric-sync.service");
        std::fs::write(&main_definition, b"new-only main arguments").unwrap();
        std::fs::write(&companion_definition, b"new companion service").unwrap();
        let actions = std::cell::RefCell::new(Vec::new());

        let companion_restored = restore_rollback_machine(
            &installed,
            &rollback,
            |companion_exists| {
                assert!(!companion_exists, "Release A unexpectedly had a companion");
                std::fs::remove_file(&companion_definition)?;
                actions.borrow_mut().push("companion stopped and removed");
                Ok(())
            },
            |restored, companion_exists| {
                let spec = crate::service::ServiceSpec::new(
                    restored,
                    home.root(),
                    false,
                    false,
                    None,
                )?;
                let systemd = crate::service::rollback_systemd_definitions(
                    &spec,
                    companion_exists,
                )?;
                let launchd = crate::service::rollback_launchd_definitions(
                    &home,
                    &spec,
                    companion_exists,
                )?;
                assert!(systemd.companion.is_none());
                assert!(launchd.companion.is_none());
                std::fs::write(&main_definition, systemd.main)?;
                actions.borrow_mut().push("old main service restored");
                Ok(())
            },
        )
        .unwrap();

        assert!(!companion_restored);
        assert_eq!(std::fs::read(&installed).unwrap(), expected);
        assert!(!companion.exists(), "the new companion binary survived rollback");
        assert!(
            !companion_definition.exists(),
            "the new companion service survived rollback"
        );
        assert!(
            std::fs::read_to_string(&main_definition)
                .unwrap()
                .contains(&installed.display().to_string()),
            "the old reader did not restore its main service definition"
        );
        assert_eq!(
            actions.into_inner(),
            ["companion stopped and removed", "old main service restored"]
        );
    }

    #[test]
    fn a_pair_rollback_restores_both_matching_members() {
        let dir = tempfile::tempdir().unwrap();
        let installed = dir.path().join("fabric");
        let companion = dir.path().join("fabric-sync");
        let rollback = dir.path().join("fabric.rollback-old-pair");
        let companion_rollback = dir.path().join("fabric-sync.rollback-old-pair");
        executable(&installed, "#!/bin/sh\n# broken main\nexit 99\n");
        executable(&companion, "#!/bin/sh\n# broken companion\nexit 99\n");
        executable(&rollback, "#!/bin/sh\n# old main\nexit 0\n");
        executable(&companion_rollback, "#!/bin/sh\n# old companion\nexit 0\n");

        let companion_restored = restore_rollback_machine(
            &installed,
            &rollback,
            |_| Ok(()),
            |_, _| Ok(()),
        )
        .unwrap();

        assert!(companion_restored);
        assert_eq!(
            std::fs::read(&installed).unwrap(),
            std::fs::read(&rollback).unwrap()
        );
        assert_eq!(
            std::fs::read(&companion).unwrap(),
            std::fs::read(&companion_rollback).unwrap()
        );
    }

    #[test]
    fn the_macos_job_is_launchd_owned_and_has_no_calendar_timer() {
        let dir = tempfile::tempdir().unwrap();
        let spec = render_macos_supervisor(
            dir.path(),
            Path::new("/Users/n/.local/bin/fabric"),
            Path::new("/Users/n/.local/bin/fabric.rollback-1000"),
            "generation-1",
            "fabric 0.2.1+new",
            501,
            42,
            "1000",
            Path::new("/bin/launchctl"),
        );
        assert!(spec.xml.contains("<key>RunAtLoad</key>\n<true/>"));
        assert!(spec.xml.contains("<key>KeepAlive</key>\n<false/>"));
        assert!(spec.xml.contains("<key>AbandonProcessGroup</key>"));
        assert!(!spec.xml.contains("StartInterval"));
        assert!(!spec.xml.contains("StartCalendarInterval"));

        let mut calls = Vec::new();
        register_macos_supervisor(&spec, |args| {
            calls.push(
                args.iter()
                    .map(|arg| (*arg).to_string())
                    .collect::<Vec<_>>(),
            );
            Ok(true)
        })
        .unwrap();
        assert_eq!(calls[0][0], "bootstrap");
        assert_eq!(calls[1], ["print", spec.target.as_str()]);
        assert_eq!(
            std::fs::metadata(&spec.plist).unwrap().permissions().mode() & 0o777,
            0o600
        );
        if let Ok(output) = std::process::Command::new("plutil")
            .args(["-lint", "--"])
            .arg(&spec.plist)
            .output()
        {
            assert!(
                output.status.success(),
                "plutil rejected the supervisor plist: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn the_macos_job_removes_itself_after_success() {
        let dir = tempfile::tempdir().unwrap();
        let installed = dir.path().join("fabric");
        let rollback = dir.path().join("fabric.rollback-test");
        let plist = dir.path().join("supervisor.plist");
        let launchctl = dir.path().join("launchctl");
        let generation_file = dir.path().join("update-generation");
        executable(&installed, "#!/bin/sh\n# candidate\nexit 0\n");
        executable(&rollback, "#!/bin/sh\nexit 0\n");
        std::fs::write(&plist, b"loaded").unwrap();
        executable(
            &launchctl,
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$0.calls\"\n",
        );
        std::fs::write(&generation_file, b"generation-1\n").unwrap();

        let status = run_macos_wrapper(
            &installed,
            &rollback,
            &plist,
            &launchctl,
            &generation_file,
            "generation-1",
        );
        assert!(status.success());
        assert!(
            !plist.exists(),
            "the successful transient job left its plist"
        );
        assert_eq!(
            std::fs::read_to_string(&generation_file).unwrap(),
            "generation-1\n",
            "the supervisor changed its durable generation record"
        );
        let expected = "bootout gui/501/com.compoundingtech.fabric.update-supervisor.test";
        let calls = wait_for_call(&launchctl.with_extension("calls"), expected);
        assert!(calls.contains(expected));
    }

    #[test]
    fn an_interruption_after_replacement_restores_the_binary_later() {
        let dir = tempfile::tempdir().unwrap();
        let installed = dir.path().join("fabric");
        let rollback = dir.path().join("fabric.rollback-test");
        let plist = dir.path().join("supervisor.plist");
        let launchctl = dir.path().join("launchctl");
        let generation_file = dir.path().join("update-generation");
        executable(&installed, "#!/bin/sh\nexit 99\n");
        executable(
            &rollback,
            "#!/bin/sh\ninstalled=${0%%.rollback-*}\n/bin/cp \"$0\" \"$installed\"\nexit 0\n",
        );
        std::fs::write(&plist, b"loaded").unwrap();
        executable(
            &launchctl,
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$0.calls\"\n",
        );
        std::fs::write(&generation_file, b"generation-1\n").unwrap();

        // The updater is gone. The launchd-owned job starts from installed
        // replacement bytes and performs the recovery without that process.
        let status = run_macos_wrapper(
            &installed,
            &rollback,
            &plist,
            &launchctl,
            &generation_file,
            "generation-1",
        );
        assert!(status.success());
        assert_eq!(
            std::fs::read(&installed).unwrap(),
            std::fs::read(&rollback).unwrap()
        );
        assert!(!plist.exists(), "the rollback left its transient plist");
        let expected = "bootout gui/501/com.compoundingtech.fabric.update-supervisor.test";
        let calls = wait_for_call(&launchctl.with_extension("calls"), expected);
        assert!(calls.contains(expected));
    }

    fn stale_macos_supervisor_leaves_healthy_update_unchanged(record: Option<&str>) {
        let dir = tempfile::tempdir().unwrap();
        let installed = dir.path().join("fabric");
        let rollback = dir.path().join("fabric.rollback-test");
        let plist = dir.path().join("supervisor.plist");
        let launchctl = dir.path().join("launchctl");
        let generation_file = dir.path().join("update-generation");
        executable(&installed, "#!/bin/sh\n# healthy later build\nexit 0\n");
        let healthy = std::fs::read(&installed).unwrap();
        executable(
            &rollback,
            "#!/bin/sh\ninstalled=${0%%.rollback-*}\nprintf broken > \"$installed\"\nexit 0\n",
        );
        std::fs::write(&plist, b"stale loaded job").unwrap();
        executable(
            &launchctl,
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$0.calls\"\n",
        );
        if let Some(record) = record {
            std::fs::write(&generation_file, format!("{record}\n")).unwrap();
        }

        let status = run_macos_wrapper(
            &installed,
            &rollback,
            &plist,
            &launchctl,
            &generation_file,
            "stale-generation",
        );

        assert!(status.success());
        assert_eq!(std::fs::read(&installed).unwrap(), healthy);
        assert!(!plist.exists(), "the stale supervisor did not clean itself");
        if let Some(record) = record {
            assert_eq!(
                std::fs::read_to_string(&generation_file).unwrap(),
                format!("{record}\n"),
                "the stale supervisor removed the later update generation"
            );
        }
    }

    #[test]
    fn a_stale_supervisor_does_not_revert_a_healthy_later_update() {
        stale_macos_supervisor_leaves_healthy_update_unchanged(Some("later-generation"));
    }

    #[test]
    fn a_supervisor_without_a_generation_record_does_nothing() {
        stale_macos_supervisor_leaves_healthy_update_unchanged(None);
    }

    /// This test changes the current user's launchd domain for a few seconds.
    /// Run it explicitly on a Mac before a release that changes the updater.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "uses the live user launchd domain"]
    fn a_real_launchd_supervisor_rolls_back_and_removes_its_job() {
        if run_real_pair_worker() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("logs")).unwrap();
        std::fs::create_dir_all(dir.path().join("run")).unwrap();
        std::fs::write(
            dir.path().join("run").join(UPDATE_GENERATION_FILE),
            b"generation-1\n",
        )
        .unwrap();
        let (installed, companion, rollback, companion_rollback) = real_pair_fixture(
            dir.path(),
            "update::supervisor_tests::a_real_launchd_supervisor_rolls_back_and_removes_its_job",
        );
        let stamp = format!("test-{}-{}", timestamp(), std::process::id());
        let spec = render_macos_supervisor(
            dir.path(),
            &installed,
            &rollback,
            "generation-1",
            "fabric test",
            unsafe { libc::geteuid() },
            u32::MAX,
            &stamp,
            Path::new("/bin/launchctl"),
        );
        register_macos_supervisor(&spec, |args| {
            Ok(std::process::Command::new("/bin/launchctl")
                .args(args)
                .output()?
                .status
                .success())
        })
        .unwrap();

        let mut removed = false;
        for _ in 0..100 {
            let loaded = std::process::Command::new("/bin/launchctl")
                .args(["print", &spec.target])
                .output()
                .is_ok_and(|output| output.status.success());
            if !loaded
                && !spec.plist.exists()
                && std::fs::read(&installed).unwrap() == std::fs::read(&rollback).unwrap()
                && std::fs::read(&companion).unwrap() == std::fs::read(&companion_rollback).unwrap()
            {
                removed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        if !removed {
            let _ = std::process::Command::new("/bin/launchctl")
                .args(["bootout", &spec.target])
                .output();
        }
        assert!(
            removed,
            "the real launchd job did not restore both members and remove itself"
        );
    }

    /// This test changes the current user's systemd manager for a few seconds.
    /// Run it explicitly on Linux before a release that changes the updater.
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "uses the live user systemd manager"]
    fn a_real_systemd_supervisor_rolls_back_and_removes_its_jobs() {
        if run_real_pair_worker() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let (installed, companion, rollback, companion_rollback) = real_pair_fixture(
            dir.path(),
            "update::supervisor_tests::a_real_systemd_supervisor_rolls_back_and_removes_its_jobs",
        );
        let unit = format!("fabric-update-supervisor-test-{}", std::process::id());
        let (_, mut args) = supervisor_argv(dir.path(), &rollback, "generation-1", "fabric test");
        let timer = args
            .iter_mut()
            .find(|arg| arg.starts_with("--on-active="))
            .expect("the supervisor has no timer");
        *timer = "--on-active=1".into();
        args.insert(1, format!("--unit={unit}"));
        let scheduled = std::process::Command::new("systemd-run")
            .args(&args)
            .output()
            .unwrap();
        assert!(
            scheduled.status.success(),
            "systemd-run failed: {}",
            String::from_utf8_lossy(&scheduled.stderr)
        );

        let mut removed = false;
        for _ in 0..200 {
            let service_loaded = std::process::Command::new("systemctl")
                .args([
                    "--user",
                    "show",
                    &format!("{unit}.service"),
                    "-p",
                    "LoadState",
                ])
                .output()
                .is_ok_and(|output| output.stdout != b"LoadState=not-found\n");
            let timer_loaded = std::process::Command::new("systemctl")
                .args([
                    "--user",
                    "show",
                    &format!("{unit}.timer"),
                    "-p",
                    "LoadState",
                ])
                .output()
                .is_ok_and(|output| output.stdout != b"LoadState=not-found\n");
            if !service_loaded
                && !timer_loaded
                && std::fs::read(&installed).unwrap() == std::fs::read(&rollback).unwrap()
                && std::fs::read(&companion).unwrap() == std::fs::read(&companion_rollback).unwrap()
            {
                removed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        if !removed {
            let _ = std::process::Command::new("systemctl")
                .args([
                    "--user",
                    "stop",
                    &format!("{unit}.timer"),
                    &format!("{unit}.service"),
                ])
                .output();
        }
        assert!(
            removed,
            "the real systemd jobs did not restore both members and remove themselves"
        );
    }
}

#[cfg(test)]
mod supervisor_failure_tests {
    use super::*;
    use std::time::Duration;

    fn arm(home: &crate::config::FabricHome, generation: &str) {
        write_update_generation(home, generation).unwrap();
    }

    /// Stand up something that answers the control socket and calls itself
    /// `version`, so the supervisor can be pointed at a daemon that is up but is
    /// the WRONG ONE.
    async fn fake_daemon(home: &crate::config::FabricHome, version: &str) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let path = home.control_socket_path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let version = version.to_string();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = vec![0u8; 4096];
                let _ = stream.read(&mut request).await;
                let reply = crate::control::ControlResponse::ReachabilityStatus {
                    version: version.clone(),
                    node_id: "fake".into(),
                    endpoint_addr: serde_json::Value::Null,
                    exposed_protocols: Vec::new(),
                    dial_sockets: Vec::new(),
                    allow_shell: false,
                    allow_exec: false,
                    peers: Vec::new(),
                    connection_telemetry: Default::default(),
                    connection_telemetry_window: Default::default(),
                    current_connection_health: Default::default(),
                    active_dial_handlers: 0,
                    max_dial_handlers: 32,
                };
                let _ = stream
                    .write_all(&serde_json::to_vec(&reply).unwrap())
                    .await;
                let _ = stream.shutdown().await;
            }
        });
    }

    /// THE FAULT THIS INSTRUMENT EXISTS TO DETECT, made to happen on purpose.
    ///
    /// A daemon is up and answering, and it is the OLD one. That is exactly the
    /// state a failed update leaves behind, and the state the supervisor
    /// previously blessed as healthy because something answered the socket.
    ///
    /// An instrument has to be able to fail before its success means anything.
    #[tokio::test]
    async fn a_daemon_answering_with_the_wrong_version_is_not_a_healthy_restart() {
        let dir = tempfile::tempdir().unwrap();
        let home = crate::config::FabricHome::new(dir.path());
        fake_daemon(&home, "0.2.0+theoldone").await;
        arm(&home, "generation-1");

        assert_eq!(
            wait_for_daemon_version(
                &home,
                "generation-1",
                "0.2.0+thenewone",
                Duration::from_millis(600),
            )
            .await,
            VersionWaitDecision::Rollback,
            "the supervisor accepted the OLD daemon as a healthy restart, which is \
             the failure it exists to catch"
        );
    }

    /// And it must accept the right one, or it would roll back every good update.
    #[tokio::test]
    async fn a_daemon_answering_with_the_expected_version_is_a_healthy_restart() {
        let dir = tempfile::tempdir().unwrap();
        let home = crate::config::FabricHome::new(dir.path());
        fake_daemon(&home, "0.2.0+thenewone").await;
        arm(&home, "generation-1");

        assert_eq!(
            wait_for_daemon_version(
                &home,
                "generation-1",
                "0.2.0+thenewone",
                Duration::from_secs(5),
            )
            .await,
            VersionWaitDecision::Ready,
            "the supervisor rejected the daemon it was told to expect"
        );
    }

    /// Nothing answering at all must also fail, and must give up rather than hang.
    #[tokio::test]
    async fn no_daemon_at_all_is_not_a_healthy_restart() {
        let dir = tempfile::tempdir().unwrap();
        let home = crate::config::FabricHome::new(dir.path());
        arm(&home, "generation-1");
        let started = std::time::Instant::now();
        assert_eq!(
            wait_for_daemon_version(
                &home,
                "generation-1",
                "0.2.0+anything",
                Duration::from_millis(400),
            )
            .await,
            VersionWaitDecision::Rollback
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the supervisor hung instead of giving up"
        );
    }

    #[tokio::test]
    async fn a_stale_generation_stops_before_daemon_checks() {
        let dir = tempfile::tempdir().unwrap();
        let home = crate::config::FabricHome::new(dir.path());
        arm(&home, "later-generation");
        assert_eq!(
            wait_for_daemon_version(
                &home,
                "stale-generation",
                "0.2.0+old-candidate",
                Duration::from_secs(5),
            )
            .await,
            VersionWaitDecision::Superseded
        );
    }

    #[tokio::test]
    async fn a_missing_generation_stops_before_daemon_checks() {
        let dir = tempfile::tempdir().unwrap();
        let home = crate::config::FabricHome::new(dir.path());
        assert_eq!(
            wait_for_daemon_version(
                &home,
                "missing-generation",
                "0.2.0+old-candidate",
                Duration::from_secs(5),
            )
            .await,
            VersionWaitDecision::Superseded
        );
    }
}
