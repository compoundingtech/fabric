//! The signed app that the macOS service runs.
//!
//! macOS records a privacy grant (TCC) against the code identity of the process
//! responsible for an access, and every command that `fabric exec` starts names
//! the daemon as that process. A release binary is ad hoc signed, and an ad hoc
//! identity is the hash of the code itself, so to TCC every build is a new
//! program. The first command after an update that reads Desktop, Documents,
//! Downloads or the Photos library asks the person at the screen again, once per
//! folder. tccd says so in its log: "Failed to match existing code requirement".
//!
//! A bundle signed with one persistent identity under one identifier has a
//! designated requirement of `identifier "..." and certificate leaf = H"..."`
//! (or an Apple anchor for an Apple-issued identity). Every later build signed
//! the same way satisfies it, so a grant outlives the build it was given to.
//!
//! The bundle is a signed MIRROR of the installed pair. The updater still stages,
//! commits and rolls back the plain binaries beside each other, and the launchd
//! definitions still name the plain binary as argv[0], so every reader of those
//! definitions, an older rollback binary included, finds the file it manages.
//! Only launchd's `Program` names the signed copy.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// The bundle identifier, and the identifier the main executable is signed with.
pub const BUNDLE_ID: &str = "com.compoundingtech.fabric";
/// The identifier the companion is signed with inside the same bundle.
pub const SYNC_IDENTIFIER: &str = "com.compoundingtech.fabric-sync";
const BUNDLE_NAME: &str = "Fabric.app";
/// Which installed binary the bundle mirrors. Recorded inside the signed
/// Info.plist, so it cannot drift from the bytes it describes.
const PAYLOAD_KEY: &str = "FabricPayload";
const DIGEST_KEY: &str = "FabricPayloadSHA256";
const IDENTITY_KEY: &str = "FabricSigningIdentity";

/// Read the text that fills each privacy prompt. The prompts appear only for a
/// program that has a usage string; a bundle without one is denied instead,
/// which would quietly break a command a paired machine runs here.
const ACCESS_PURPOSE: &str =
    "A command that one of your paired machines runs through Fabric needs this access.";
const LOCAL_NETWORK_PURPOSE: &str =
    "Fabric connects directly to your other machines when they share a local network.";
const USAGE_KEYS: [&str; 7] = [
    "NSDesktopFolderUsageDescription",
    "NSDocumentsFolderUsageDescription",
    "NSDownloadsFolderUsageDescription",
    "NSRemovableVolumesUsageDescription",
    "NSNetworkVolumesUsageDescription",
    "NSPhotoLibraryUsageDescription",
    "NSAppleMusicUsageDescription",
];

/// The certificate's SHA-1, as `security find-identity -v -p codesigning`
/// prints it.
///
/// Only a hash. A name can match more than one certificate in a keychain, and a
/// keychain can hold identities of more than one team, so the identity is never
/// chosen by name or by search.
pub fn normalise_signing_identity(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.len() != 40 || !trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!(
            "a macOS signing identity is the certificate's 40-character SHA-1 hash, as \
             `security find-identity -v -p codesigning` prints it; got {trimmed:?}"
        );
    }
    Ok(trimmed.to_ascii_uppercase())
}

/// Where the app lives: a fixed path, so its identity never depends on where
/// the plain binary was installed.
pub fn bundle_path(home_dir: &Path) -> PathBuf {
    home_dir.join("Applications").join(BUNDLE_NAME)
}

/// The directory that holds the bundle's executables.
pub fn executable_dir(bundle: &Path) -> PathBuf {
    bundle.join("Contents").join("MacOS")
}

/// True when `path` is an executable inside some `*.app/Contents/MacOS`.
pub fn is_app_executable(path: &Path) -> bool {
    let Some(macos) = path.parent() else {
        return false;
    };
    let Some(contents) = macos.parent() else {
        return false;
    };
    let Some(bundle) = contents.parent() else {
        return false;
    };
    macos.file_name().is_some_and(|name| name == "MacOS")
        && contents.file_name().is_some_and(|name| name == "Contents")
        && bundle
            .extension()
            .is_some_and(|extension| extension == "app")
}

/// A digest over the pair the bundle mirrors. The bundle is left alone while
/// this matches, so an install that changes nothing signs nothing.
pub fn payload_digest(fabric: &Path, companion: Option<&Path>) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for (name, path) in [("fabric", Some(fabric)), ("fabric-sync", companion)] {
        let Some(path) = path else {
            continue;
        };
        let bytes =
            std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// `CFBundleShortVersionString` takes numbers and dots only.
fn bundle_version(version: &str) -> &str {
    version.split(['+', '-', ' ']).next().unwrap_or(version)
}

pub fn render_info_plist(version: &str, payload: &Path, digest: &str, identity: &str) -> String {
    let mut entries = vec![
        ("CFBundleIdentifier", BUNDLE_ID.to_string()),
        ("CFBundleExecutable", "fabric".to_string()),
        ("CFBundleName", "Fabric".to_string()),
        ("CFBundleDisplayName", "Fabric".to_string()),
        ("CFBundlePackageType", "APPL".to_string()),
        ("CFBundleInfoDictionaryVersion", "6.0".to_string()),
        (
            "CFBundleShortVersionString",
            bundle_version(version).to_string(),
        ),
        ("CFBundleVersion", bundle_version(version).to_string()),
        (
            "NSLocalNetworkUsageDescription",
            LOCAL_NETWORK_PURPOSE.to_string(),
        ),
    ];
    entries.extend(
        USAGE_KEYS
            .iter()
            .map(|key| (*key, ACCESS_PURPOSE.to_string())),
    );
    entries.push((PAYLOAD_KEY, payload.display().to_string()));
    entries.push((DIGEST_KEY, digest.to_string()));
    entries.push((IDENTITY_KEY, identity.to_string()));
    let body = entries
        .iter()
        .map(|(key, value)| {
            format!(
                "    <key>{}</key>\n    <string>{}</string>\n",
                xml_escape(key),
                xml_escape(value)
            )
        })
        .collect::<String>();
    // A background daemon: no Dock icon, no menu bar, no window.
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
<dict>\n\
{body}    <key>LSBackgroundOnly</key>\n\
    <true/>\n\
</dict>\n\
</plist>\n"
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(target_os = "macos")]
pub use platform::{payload_for_app_executable, prepare, remove};

#[cfg(target_os = "macos")]
mod platform {
    use std::{
        path::{Path, PathBuf},
        process::{Command, Output, Stdio},
        time::Duration,
    };

    use anyhow::{Context, Result, bail};

    use super::{
        BUNDLE_ID, DIGEST_KEY, IDENTITY_KEY, PAYLOAD_KEY, SYNC_IDENTIFIER, executable_dir,
        is_app_executable, payload_digest, render_info_plist,
    };

    const CODESIGN: &str = "/usr/bin/codesign";
    const LSREGISTER: &str = "/System/Library/Frameworks/CoreServices.framework/Frameworks/\
                              LaunchServices.framework/Support/lsregister";
    /// codesign and lsregister never need this long. The bound exists because
    /// a locked keychain can make codesign wait on a dialog that nobody is
    /// there to answer, and an install must not hang behind that.
    const TOOL_TIMEOUT: Duration = Duration::from_secs(60);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Materialized {
        Unchanged,
        Signed,
    }

    /// Decide what the service runs, building or refreshing the signed app
    /// first, and return the app's executable directory when it should be used.
    ///
    /// Never fails. An app that cannot be signed leaves the service on the plain
    /// binary, which is what every earlier release ran: a person sees the
    /// privacy prompts again, but the machine stays reachable.
    pub fn prepare(
        bundle: &Path,
        payload: &Path,
        companion_exists: bool,
        identity: Option<&str>,
    ) -> Option<PathBuf> {
        let Some(identity) = identity else {
            remove(bundle);
            return None;
        };
        let companion = companion_exists.then(|| payload.with_file_name("fabric-sync"));
        match materialize(bundle, payload, companion.as_deref(), identity, None) {
            Ok(Materialized::Unchanged) => {
                println!("app\t{} (unchanged)", bundle.display());
                Some(executable_dir(bundle))
            }
            Ok(Materialized::Signed) => {
                println!("app\t{} (signed by {identity})", bundle.display());
                Some(executable_dir(bundle))
            }
            Err(error) => {
                eprintln!(
                    "WARNING: the signed app at {} could not be prepared, so the service runs \
                     {} directly and macOS may ask for its privacy permissions again: {error:#}",
                    bundle.display(),
                    payload.display()
                );
                None
            }
        }
    }

    /// Remove the app, but only one this module built. A person's own
    /// `Fabric.app` at that path is not ours to delete.
    pub fn remove(bundle: &Path) {
        if read_info(bundle)
            .and_then(|info| info.get(PAYLOAD_KEY).cloned())
            .is_none()
        {
            return;
        }
        let _ = Command::new(LSREGISTER)
            .arg("-u")
            .arg(bundle)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        match std::fs::remove_dir_all(bundle) {
            Ok(()) => println!("app\t{} removed", bundle.display()),
            Err(error) => eprintln!("WARNING: could not remove {}: {error}", bundle.display()),
        }
    }

    /// The installed binary that an app executable mirrors, read from the
    /// bundle's signed Info.plist. `None` when `exe` is not inside an app.
    pub fn payload_for_app_executable(exe: &Path) -> Option<Result<PathBuf>> {
        if !is_app_executable(exe) {
            return None;
        }
        let bundle = exe.parent()?.parent()?.parent()?;
        let payload = read_info(bundle).and_then(|info| {
            info.get(PAYLOAD_KEY)
                .and_then(|value| value.as_str().map(PathBuf::from))
        });
        Some(match payload {
            Some(payload) => {
                let name = exe.file_name().unwrap_or_default();
                Ok(payload.with_file_name(name))
            }
            None => Err(anyhow::anyhow!(
                "{} runs inside {}, which does not record the installed binary it mirrors",
                exe.display(),
                bundle.display()
            )),
        })
    }

    fn materialize(
        bundle: &Path,
        payload: &Path,
        companion: Option<&Path>,
        identity: &str,
        keychain: Option<&Path>,
    ) -> Result<Materialized> {
        if is_app_executable(payload) {
            bail!(
                "{} is itself inside an app; install from the fabric binary on your PATH",
                payload.display()
            );
        }
        let digest = payload_digest(payload, companion)?;
        if bundle_is_current(bundle, payload, &digest, identity) {
            return Ok(Materialized::Unchanged);
        }
        if bundle.exists()
            && read_info(bundle)
                .and_then(|info| info.get(PAYLOAD_KEY).cloned())
                .is_none()
        {
            bail!(
                "{} exists and fabric did not build it; move it away to let the service use \
                 that path",
                bundle.display()
            );
        }
        let parent = bundle
            .parent()
            .context("the app path has no parent directory")?;
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        // Built beside its final path so the swap is a rename on one
        // filesystem, inside a hidden directory so nothing half-built is ever
        // at a path that ends in `.app`.
        let staging = parent.join(format!(".fabric-app-staging-{}", std::process::id()));
        if staging.exists() {
            std::fs::remove_dir_all(&staging)
                .with_context(|| format!("failed to clear {}", staging.display()))?;
        }
        let result = build_and_swap(
            &staging, bundle, payload, companion, &digest, identity, keychain,
        );
        let _ = std::fs::remove_dir_all(&staging);
        result?;
        // Registering lets local network privacy and System Settings name the
        // app. A failure here costs a nicer name, not access, so it only warns.
        if let Err(error) =
            run_bounded(Command::new(LSREGISTER).arg("-f").arg(bundle), TOOL_TIMEOUT)
                .and_then(require_success)
        {
            eprintln!(
                "WARNING: could not register {} with Launch Services: {error:#}",
                bundle.display()
            );
        }
        Ok(Materialized::Signed)
    }

    fn build_and_swap(
        staging: &Path,
        bundle: &Path,
        payload: &Path,
        companion: Option<&Path>,
        digest: &str,
        identity: &str,
        keychain: Option<&Path>,
    ) -> Result<()> {
        let incoming = staging.join("Fabric.app");
        let macos = executable_dir(&incoming);
        std::fs::create_dir_all(&macos)
            .with_context(|| format!("failed to create {}", macos.display()))?;
        copy_executable(payload, &macos.join("fabric"))?;
        if let Some(companion) = companion {
            copy_executable(companion, &macos.join("fabric-sync"))?;
        }
        let version = crate::update::binary_version(payload)?;
        let info = render_info_plist(&version, payload, digest, identity);
        std::fs::write(incoming.join("Contents/Info.plist"), info)
            .context("failed to write the app's Info.plist")?;

        // Inside out: nested code first, then the bundle that seals it.
        if companion.is_some() {
            codesign(
                identity,
                keychain,
                SYNC_IDENTIFIER,
                &macos.join("fabric-sync"),
            )?;
        }
        codesign(identity, keychain, BUNDLE_ID, &incoming)?;
        let verify = run_bounded(
            Command::new(CODESIGN)
                .args(["--verify", "--strict", "--deep"])
                .arg(&incoming),
            TOOL_TIMEOUT,
        )?;
        require_success(verify).context("the signed app does not verify")?;
        let signed_version = crate::update::binary_version(&macos.join("fabric"))?;
        if signed_version != version {
            bail!(
                "the signed app reports {signed_version}, but the installed binary reports {version}"
            );
        }

        // Swap. The running daemon keeps its executable mapped from whichever
        // inode it started on, so replacing the directory under it is safe.
        let previous = staging.join("Fabric-previous.app");
        let had_previous = bundle.exists();
        if had_previous {
            std::fs::rename(bundle, &previous)
                .with_context(|| format!("failed to move {} aside", bundle.display()))?;
        }
        if let Err(error) = std::fs::rename(&incoming, bundle) {
            if had_previous {
                let _ = std::fs::rename(&previous, bundle);
            }
            return Err(error).with_context(|| format!("failed to place {}", bundle.display()));
        }
        Ok(())
    }

    fn copy_executable(from: &Path, to: &Path) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        std::fs::copy(from, to)
            .with_context(|| format!("failed to copy {} to {}", from.display(), to.display()))?;
        std::fs::set_permissions(to, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to make {} executable", to.display()))?;
        Ok(())
    }

    fn codesign(
        identity: &str,
        keychain: Option<&Path>,
        identifier: &str,
        path: &Path,
    ) -> Result<()> {
        let mut command = Command::new(CODESIGN);
        command.args([
            "--force",
            "--timestamp=none",
            "--sign",
            identity,
            "--identifier",
            identifier,
        ]);
        if let Some(keychain) = keychain {
            command.arg("--keychain").arg(keychain);
        }
        let output = run_bounded(command.arg(path), TOOL_TIMEOUT)?;
        require_success(output)
            .with_context(|| format!("codesign could not sign {}", path.display()))
    }

    fn bundle_is_current(bundle: &Path, payload: &Path, digest: &str, identity: &str) -> bool {
        let Some(info) = read_info(bundle) else {
            return false;
        };
        let recorded = |key: &str| info.get(key).and_then(|value| value.as_str());
        if recorded(PAYLOAD_KEY) != Some(payload.display().to_string().as_str())
            || recorded(DIGEST_KEY) != Some(digest)
            || recorded(IDENTITY_KEY) != Some(identity)
        {
            return false;
        }
        run_bounded(
            Command::new(CODESIGN)
                .args(["--verify", "--strict", "--deep"])
                .arg(bundle),
            TOOL_TIMEOUT,
        )
        .and_then(require_success)
        .is_ok()
    }

    fn read_info(bundle: &Path) -> Option<serde_json::Map<String, serde_json::Value>> {
        let info = bundle.join("Contents/Info.plist");
        if !info.is_file() {
            return None;
        }
        let output = Command::new("/usr/bin/plutil")
            .args(["-convert", "json", "-o", "-"])
            .arg(&info)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        match serde_json::from_slice(&output.stdout).ok()? {
            serde_json::Value::Object(map) => Some(map),
            _ => None,
        }
    }

    fn require_success(output: Output) -> Result<()> {
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{} {}", output.status, stderr.trim())
    }

    /// Run a command, killing it if it outlives `timeout`.
    fn run_bounded(command: &mut Command, timeout: Duration) -> Result<Output> {
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to run {:?}", command.get_program()))?;
        let pid = child.id();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = sender.send(child.wait_with_output());
        });
        match receiver.recv_timeout(timeout) {
            Ok(output) => Ok(output?),
            Err(_) => {
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGKILL);
                }
                bail!(
                    "{:?} did not finish within {timeout:?}",
                    command.get_program()
                )
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// A throwaway self-signed code-signing identity in its own keychain.
        /// The test needs no certificate from anyone.
        ///
        /// Older macOS (the CI runner's) finds a signing key only in a keychain
        /// on the user's search list, even with `codesign --keychain`, so the
        /// scratch keychain joins the list while the identity lives. One test
        /// at a time, and the exact previous list is put back on drop, a
        /// failing test included. Fields drop in order: list, then keychain.
        struct ScratchIdentity {
            _search_list: SearchList,
            _dir: tempfile::TempDir,
            keychain: PathBuf,
            sha1: String,
            _serial: std::sync::MutexGuard<'static, ()>,
        }

        static SIGNING: std::sync::Mutex<()> = std::sync::Mutex::new(());

        struct SearchList {
            before: Vec<String>,
        }

        impl SearchList {
            fn read() -> Vec<String> {
                run("/usr/bin/security", &["list-keychains", "-d", "user"])
                    .lines()
                    .map(|line| line.trim().trim_matches('"').to_string())
                    .filter(|line| !line.is_empty())
                    .collect()
            }

            fn set(keychains: &[String]) {
                let mut args = vec!["list-keychains", "-d", "user", "-s"];
                args.extend(keychains.iter().map(String::as_str));
                let _ = Command::new("/usr/bin/security").args(&args).status();
            }

            fn join(keychain: &Path) -> Self {
                let before = Self::read();
                let mut during = before.clone();
                during.push(keychain.display().to_string());
                Self::set(&during);
                Self { before }
            }
        }

        impl Drop for SearchList {
            fn drop(&mut self) {
                Self::set(&self.before);
            }
        }

        fn run(program: &str, args: &[&str]) -> String {
            let output = Command::new(program).args(args).output().unwrap();
            assert!(
                output.status.success(),
                "{program} {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout).into_owned()
        }

        fn scratch_identity() -> ScratchIdentity {
            let serial = SIGNING
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let dir = tempfile::tempdir().unwrap();
            let path = |name: &str| dir.path().join(name).display().to_string();
            std::fs::write(
                dir.path().join("cert.cnf"),
                "[req]\ndistinguished_name = dn\nx509_extensions = ext\nprompt = no\n\
                 [dn]\nCN = Fabric Test Signing\n\
                 [ext]\nbasicConstraints = critical,CA:false\n\
                 keyUsage = critical,digitalSignature\n\
                 extendedKeyUsage = critical,codeSigning\n",
            )
            .unwrap();
            run(
                "/usr/bin/openssl",
                &[
                    "req",
                    "-new",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "2",
                    "-sha256",
                    "-config",
                    &path("cert.cnf"),
                    "-keyout",
                    &path("key.pem"),
                    "-out",
                    &path("cert.pem"),
                ],
            );
            run(
                "/usr/bin/openssl",
                &[
                    "pkcs12",
                    "-export",
                    "-inkey",
                    &path("key.pem"),
                    "-in",
                    &path("cert.pem"),
                    "-passout",
                    "pass:test",
                    "-out",
                    &path("id.p12"),
                ],
            );
            let keychain = dir.path().join("test.keychain-db");
            let keychain_arg = keychain.display().to_string();
            run(
                "/usr/bin/security",
                &["create-keychain", "-p", "test", &keychain_arg],
            );
            run(
                "/usr/bin/security",
                &["unlock-keychain", "-p", "test", &keychain_arg],
            );
            run(
                "/usr/bin/security",
                &[
                    "import",
                    &path("id.p12"),
                    "-k",
                    &keychain_arg,
                    "-P",
                    "test",
                    "-T",
                    "/usr/bin/codesign",
                ],
            );
            run(
                "/usr/bin/security",
                &[
                    "set-key-partition-list",
                    "-S",
                    "apple-tool:,apple:,codesign:",
                    "-s",
                    "-k",
                    "test",
                    &keychain_arg,
                ],
            );
            let fingerprint = run(
                "/usr/bin/openssl",
                &[
                    "x509",
                    "-noout",
                    "-fingerprint",
                    "-sha1",
                    "-in",
                    &path("cert.pem"),
                ],
            );
            let sha1 = fingerprint
                .trim()
                .rsplit('=')
                .next()
                .unwrap()
                .replace(':', "");
            ScratchIdentity {
                _search_list: SearchList::join(&keychain),
                _dir: dir,
                keychain,
                sha1: crate::macos_app::normalise_signing_identity(&sha1).unwrap(),
                _serial: serial,
            }
        }

        /// The designated requirement, as TCC stores it with a grant. codesign
        /// prints an ad hoc one as a comment, because it is only implicit.
        fn designated_requirement(path: &Path) -> String {
            let output = Command::new(CODESIGN)
                .args(["-d", "-r-"])
                .arg(path)
                .output()
                .unwrap();
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            text.lines()
                .find_map(|line| {
                    line.strip_prefix("designated => ")
                        .or_else(|| line.strip_prefix("# designated => "))
                })
                .unwrap_or_else(|| panic!("no designated requirement in:\n{text}"))
                .to_string()
        }

        fn satisfies(path: &Path, requirement: &str) -> bool {
            Command::new(CODESIGN)
                .args(["--verify", "--strict"])
                .arg(format!("-R={requirement}"))
                .arg(path)
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success()
        }

        /// One build of an installed pair: programs that answer `--version`
        /// like the real ones, ad hoc signed as a release build is.
        fn write_payload(dir: &Path, build: &str) -> PathBuf {
            use std::os::unix::fs::PermissionsExt;
            for name in ["fabric", "fabric-sync"] {
                let path = dir.join(name);
                std::fs::write(&path, format!("#!/bin/sh\necho \"{name} {build}\"\n")).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
                run(
                    CODESIGN,
                    &["--force", "--sign", "-", &path.display().to_string()],
                );
            }
            dir.join("fabric")
        }

        fn sign(
            bundle: &Path,
            payload: &Path,
            identity: &ScratchIdentity,
            sha1: &str,
        ) -> Result<Materialized> {
            let companion = payload.with_file_name("fabric-sync");
            materialize(
                bundle,
                payload,
                Some(&companion),
                sha1,
                Some(&identity.keychain),
            )
        }

        /// THE PROPERTY A PRIVACY GRANT NEEDS. TCC stores the designated
        /// requirement of the program it granted. A plain ad hoc build's
        /// requirement is its own code hash, so the next build fails it and
        /// macOS asks again. The app's requirement names an identifier and a
        /// certificate, so the next build signed the same way passes it.
        #[test]
        fn a_rebuilt_payload_keeps_the_app_identity_that_privacy_grants_name() {
            let identity = scratch_identity();
            let first = tempfile::tempdir().unwrap();
            let second = tempfile::tempdir().unwrap();
            let apps = tempfile::tempdir().unwrap();
            let bundle = apps.path().join("Fabric.app");
            let old_payload = write_payload(first.path(), "0.0.1");
            let new_payload = write_payload(second.path(), "0.0.2");

            // The control, and the check tccd fails after every update today.
            let plain = designated_requirement(&old_payload);
            assert!(plain.starts_with("cdhash"), "{plain}");
            assert!(satisfies(&old_payload, &plain));
            assert!(!satisfies(&new_payload, &plain));

            assert_eq!(
                sign(&bundle, &old_payload, &identity, &identity.sha1).unwrap(),
                Materialized::Signed
            );
            let granted = designated_requirement(&bundle);
            assert!(
                granted.contains(&format!("identifier \"{BUNDLE_ID}\"")),
                "{granted}"
            );
            let companion_granted =
                designated_requirement(&executable_dir(&bundle).join("fabric-sync"));
            assert!(
                companion_granted.contains(&format!("identifier \"{SYNC_IDENTIFIER}\"")),
                "{companion_granted}"
            );

            assert_eq!(
                sign(&bundle, &new_payload, &identity, &identity.sha1).unwrap(),
                Materialized::Signed
            );
            assert!(
                satisfies(&bundle, &granted),
                "the rebuilt app fails {granted}"
            );
            assert!(
                satisfies(
                    &executable_dir(&bundle).join("fabric-sync"),
                    &companion_granted
                ),
                "the rebuilt companion fails {companion_granted}"
            );
            assert_eq!(
                crate::update::binary_version(&executable_dir(&bundle).join("fabric")).unwrap(),
                "fabric 0.0.2"
            );
        }

        /// An install that changes nothing must not touch the app. Signing is
        /// the one step here that could ever involve a person.
        #[test]
        fn an_unchanged_payload_leaves_the_app_alone() {
            let identity = scratch_identity();
            let installed = tempfile::tempdir().unwrap();
            let apps = tempfile::tempdir().unwrap();
            let bundle = apps.path().join("Fabric.app");
            let payload = write_payload(installed.path(), "0.0.3");

            assert_eq!(
                sign(&bundle, &payload, &identity, &identity.sha1).unwrap(),
                Materialized::Signed
            );
            let signed_at = || {
                std::fs::metadata(executable_dir(&bundle).join("fabric"))
                    .unwrap()
                    .modified()
                    .unwrap()
            };
            let before = signed_at();
            assert_eq!(
                sign(&bundle, &payload, &identity, &identity.sha1).unwrap(),
                Materialized::Unchanged
            );
            assert_eq!(before, signed_at());

            let recorded = payload_for_app_executable(&executable_dir(&bundle).join("fabric"))
                .unwrap()
                .unwrap();
            assert_eq!(recorded, payload);
            let recorded_companion =
                payload_for_app_executable(&executable_dir(&bundle).join("fabric-sync"))
                    .unwrap()
                    .unwrap();
            assert_eq!(recorded_companion, payload.with_file_name("fabric-sync"));
        }

        /// A build that cannot be signed must not replace a working app, and
        /// must not leave anything half-built behind.
        #[test]
        fn a_failed_signature_leaves_the_previous_app_in_place() {
            let identity = scratch_identity();
            let first = tempfile::tempdir().unwrap();
            let second = tempfile::tempdir().unwrap();
            let apps = tempfile::tempdir().unwrap();
            let bundle = apps.path().join("Fabric.app");
            let payload = write_payload(first.path(), "0.0.4");
            sign(&bundle, &payload, &identity, &identity.sha1).unwrap();
            let requirement = designated_requirement(&bundle);

            let newer = write_payload(second.path(), "0.0.5");
            let absent = "0000000000000000000000000000000000000000";
            assert!(sign(&bundle, &newer, &identity, absent).is_err());

            assert!(satisfies(&bundle, &requirement));
            assert_eq!(
                crate::update::binary_version(&executable_dir(&bundle).join("fabric")).unwrap(),
                "fabric 0.0.4"
            );
            let leftovers: Vec<_> = apps
                .path()
                .read_dir()
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .filter(|name| name != "Fabric.app")
                .collect();
            assert!(leftovers.is_empty(), "left behind: {leftovers:?}");
        }

        /// A person's own app at that path is never replaced or removed.
        #[test]
        fn an_app_fabric_did_not_build_is_left_alone() {
            let identity = scratch_identity();
            let installed = tempfile::tempdir().unwrap();
            let apps = tempfile::tempdir().unwrap();
            let bundle = apps.path().join("Fabric.app");
            std::fs::create_dir_all(bundle.join("Contents")).unwrap();
            std::fs::write(bundle.join("Contents/Info.plist"), render_foreign_info()).unwrap();
            let payload = write_payload(installed.path(), "0.0.7");

            let error = sign(&bundle, &payload, &identity, &identity.sha1).unwrap_err();
            assert!(format!("{error:#}").contains("did not build"), "{error:#}");
            remove(&bundle);
            assert_eq!(
                std::fs::read_to_string(bundle.join("Contents/Info.plist")).unwrap(),
                render_foreign_info()
            );
        }

        fn render_foreign_info() -> String {
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
             \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
             <plist version=\"1.0\"><dict><key>CFBundleIdentifier</key>\
             <string>com.example.fabric</string></dict></plist>\n"
                .to_string()
        }

        /// The app is never built from a copy that is itself inside an app,
        /// which would make the bundle record itself as what it mirrors.
        #[test]
        fn an_app_is_never_built_from_an_app() {
            let identity = scratch_identity();
            let installed = tempfile::tempdir().unwrap();
            let apps = tempfile::tempdir().unwrap();
            let bundle = apps.path().join("Fabric.app");
            let payload = write_payload(installed.path(), "0.0.6");
            sign(&bundle, &payload, &identity, &identity.sha1).unwrap();
            let inside = executable_dir(&bundle).join("fabric");
            let other = apps.path().join("Other.app");
            let error = sign(&other, &inside, &identity, &identity.sha1).unwrap_err();
            assert!(format!("{error:#}").contains("inside an app"), "{error:#}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_signing_identity_is_only_ever_a_certificate_hash() {
        assert_eq!(
            normalise_signing_identity(" 0123456789abcdef0123456789abcdef01234567\n").unwrap(),
            "0123456789ABCDEF0123456789ABCDEF01234567"
        );
        for raw in [
            "",
            "Developer ID Application: Alex Example (ABCDE12345)",
            "0123456789ABCDEF0123456789ABCDEF0123456",
            "0123456789ABCDEF0123456789ABCDEF01234567B",
            "0123456789ABCDEF0123456789ABCDEF012345ZZ",
        ] {
            assert!(normalise_signing_identity(raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn only_an_executable_inside_an_app_counts_as_one() {
        assert!(is_app_executable(Path::new(
            "/Users/alex/Applications/Fabric.app/Contents/MacOS/fabric"
        )));
        assert!(!is_app_executable(Path::new(
            "/Users/alex/.local/bin/fabric"
        )));
        assert!(!is_app_executable(Path::new(
            "/Users/alex/Applications/Fabric/Contents/MacOS/fabric"
        )));
        assert!(!is_app_executable(Path::new("fabric")));
    }

    #[test]
    fn the_info_plist_names_the_app_and_records_what_it_mirrors() {
        let plist = render_info_plist(
            "0.2.24+abc1234",
            Path::new("/Users/alex/.local/bin/fabric"),
            "deadbeef",
            "0123456789ABCDEF0123456789ABCDEF01234567",
        );
        let compact = plist.split_whitespace().collect::<Vec<_>>().join(" ");
        for entry in [
            "<key>CFBundleIdentifier</key> <string>com.compoundingtech.fabric</string>",
            "<key>CFBundleExecutable</key> <string>fabric</string>",
            "<key>CFBundleShortVersionString</key> <string>0.2.24</string>",
            "<key>FabricPayload</key> <string>/Users/alex/.local/bin/fabric</string>",
            "<key>LSBackgroundOnly</key> <true/>",
            "<key>NSLocalNetworkUsageDescription</key> <string>",
        ] {
            assert!(compact.contains(entry), "{entry} missing from:\n{plist}");
        }
        for key in USAGE_KEYS {
            assert!(
                compact.contains(&format!("<key>{key}</key> <string>")),
                "{key} missing"
            );
        }
    }

    #[test]
    fn the_payload_digest_changes_when_either_member_changes() {
        let dir = tempfile::tempdir().unwrap();
        let fabric = dir.path().join("fabric");
        let sync = dir.path().join("fabric-sync");
        std::fs::write(&fabric, b"one").unwrap();
        std::fs::write(&sync, b"two").unwrap();
        let pair = payload_digest(&fabric, Some(&sync)).unwrap();
        assert_eq!(pair, payload_digest(&fabric, Some(&sync)).unwrap());
        assert_ne!(pair, payload_digest(&fabric, None).unwrap());
        std::fs::write(&sync, b"three").unwrap();
        assert_ne!(pair, payload_digest(&fabric, Some(&sync)).unwrap());
    }
}
