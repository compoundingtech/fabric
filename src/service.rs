use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

use crate::config::{FabricConfig, FabricHome};

/// Not `cfg`-gated, so the restart argv below and its test build on every
/// platform. A Linux-only string cannot be tested from a Mac, and this is the
/// exact shape whose regression takes a remote machine down.
const SERVICE_NAME: &str = "fabric.service";
const LAUNCHD_LABEL: &str = "com.compoundingtech.fabric";
/// How long to wait for launchd to fully unload a booted-out service before
/// bootstrapping the same label again — bootout is async, and bootstrapping a
/// still-loaded label races into "Bootstrap failed: 5: Input/output error".
const LAUNCHD_UNLOAD_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll interval while waiting for unload / between bootstrap retries.
const LAUNCHD_RETRY_BACKOFF: Duration = Duration::from_millis(300);
/// How long install waits for the freshly started daemon to accept on its
/// control socket before reporting that it did not come up.
const CONTROL_READY_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_READY_POLL: Duration = Duration::from_millis(100);
/// Bootstrap attempts before giving up — a re-install must be safe to re-run.
const LAUNCHD_BOOTSTRAP_MAX_ATTEMPTS: usize = 5;

#[derive(Debug, Clone, Copy)]
pub struct ServiceInstallOptions {
    pub allow_shell: Option<bool>,
    pub allow_exec: Option<bool>,
    /// Operator-declared memory ceiling, tri-state because a ceiling is itself
    /// optional. `None` means the caller never mentioned it, so keep whatever is
    /// persisted. `Some(None)` clears it. `Some(Some(mb))` sets it.
    pub memory_max_mb: Option<Option<u64>>,
}

#[derive(Debug, Clone)]
pub struct ServiceSpec {
    exe: PathBuf,
    home: PathBuf,
    allow_shell: bool,
    allow_exec: bool,
    memory_max_mb: Option<u64>,
}

impl ServiceSpec {
    pub fn new(
        exe: impl Into<PathBuf>,
        home: impl Into<PathBuf>,
        allow_shell: bool,
        allow_exec: bool,
        memory_max_mb: Option<u64>,
    ) -> Result<Self> {
        if memory_max_mb == Some(0) {
            bail!("--memory-max-mb must be greater than zero");
        }
        Ok(Self {
            exe: exe.into(),
            home: home.into(),
            allow_shell,
            allow_exec,
            memory_max_mb,
        })
    }

    fn current(
        home: &FabricHome,
        allow_shell: bool,
        allow_exec: bool,
        memory_max_mb: Option<u64>,
    ) -> Result<Self> {
        let exe = env::current_exe().context("failed to resolve current fabric executable")?;
        Self::new(exe, home.root(), allow_shell, allow_exec, memory_max_mb)
    }

    fn program_arguments(&self) -> Vec<String> {
        let mut args = vec![
            self.exe.display().to_string(),
            "--home".to_string(),
            self.home.display().to_string(),
            "daemon".to_string(),
        ];
        if self.allow_shell {
            args.push("--allow-shell".to_string());
        }
        if self.allow_exec {
            args.push("--allow-exec".to_string());
        }
        args
    }
}

pub fn install(home: &FabricHome, options: ServiceInstallOptions) -> Result<()> {
    let exe = env::current_exe().context("failed to resolve current fabric executable")?;
    install_at(home, &exe, options)
}

/// Install the service so it runs `exe`, whatever binary is asking.
///
/// `fabric update` needs this. It resolves the binary the service manager
/// already runs and installs the new bytes THERE, then re-renders the unit — and
/// the unit must keep naming that path. Rendering from `current_exe` would point
/// the daemon at whichever binary happened to run the update, which during
/// testing is a `target/debug` build and in general is nobody's idea of the
/// installed fabric.
///
/// That is the same trap as installing at `command -v fabric`, entered from the
/// other side.
pub fn install_at(home: &FabricHome, exe: &Path, options: ServiceInstallOptions) -> Result<()> {
    // The managed OS-service is a PROD-only concept, under a single global label.
    // Installing it against a dev/custom home would register a SECOND service on
    // the same label that fights the prod daemon (the service-vs-manual race).
    // A dev instance runs manually via `fabric up` on its own --home instead.
    if !home.is_default_state_root() {
        let default = FabricHome::default_state_root()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "$HOME/.local/share/fabric".to_string());
        bail!(
            "refusing to install the managed fabric service for a non-default home ({}).\n\
             The managed service is prod-only and lives on the default home ({default}); a second \
             managed service would fight the prod daemon.\n\
             For a dev instance, run it manually instead: `fabric --home {0} up` \
             (or set FABRIC_HOME to that home).",
            home.root().display(),
        );
    }
    home.prepare()?;
    let allow_shell = resolve_allow_shell(home, options.allow_shell)?;
    let allow_exec = resolve_allow_exec(home, options.allow_exec)?;
    let memory_max_mb = resolve_memory_max_mb(home, options.memory_max_mb)?;
    let spec = ServiceSpec::new(exe, home.root(), allow_shell, allow_exec, memory_max_mb)?;
    match ServiceManager::current()? {
        #[cfg(target_os = "linux")]
        ServiceManager::SystemdUser => install_systemd_user(&spec)?,
        #[cfg(target_os = "macos")]
        ServiceManager::LaunchdUser => install_launchd_user(home, &spec)?,
    }

    // The service manager returns once it has STARTED the process, not once the
    // daemon can answer. Between those two moments `fabric status` gets
    // connection refused, which reads as a failed install and is really a race
    // against the daemon binding its control socket. Wait for the socket to
    // actually accept before claiming success, so a script can install and then
    // immediately use the thing it installed.
    let ready = wait_for_control_socket(home, CONTROL_READY_TIMEOUT);
    println!("installed");
    println!("home\t{}", home.root().display());
    println!("allow-shell\t{allow_shell}");
    println!("allow-exec\t{allow_exec}");
    // Report the RESOLVED ceiling, not what the caller passed. They differ
    // whenever the caller said nothing and a persisted ceiling was kept, which
    // is precisely the case this line exists to make visible.
    println!(
        "memory-max-mb\t{}",
        memory_max_mb
            .map(|mb| mb.to_string())
            .unwrap_or_else(|| "unset".to_string())
    );
    if !ready {
        bail!(
            "the service is registered and {} reports it started, but its control socket at {} \
             did not accept a connection within {:?}.\n\
             The install itself succeeded; the daemon is either still coming up or failing at \
             startup. Check `fabric service status` and the daemon log at {} before re-running \
             install, which would only restart it.",
            "the service manager",
            home.control_socket_path().display(),
            CONTROL_READY_TIMEOUT,
            home.root().join("logs/service.err.log").display(),
        );
    }
    println!("control-socket\tready");
    Ok(())
}

/// Poll the daemon's control socket until it accepts, or the deadline passes.
///
/// Connect-and-drop is the honest check: the socket file appears before the
/// daemon is listening on it, so existence proves nothing.
pub(crate) fn wait_for_control_socket(home: &FabricHome, timeout: Duration) -> bool {
    let path = home.control_socket_path();
    let deadline = Instant::now() + timeout;
    loop {
        if std::os::unix::net::UnixStream::connect(&path).is_ok() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(CONTROL_READY_POLL);
    }
}

pub fn status() -> Result<()> {
    match ServiceManager::current()? {
        #[cfg(target_os = "linux")]
        ServiceManager::SystemdUser => run_command(
            "systemctl",
            &["--user", "status", SERVICE_NAME, "--no-pager"],
        ),
        #[cfg(target_os = "macos")]
        ServiceManager::LaunchdUser => {
            let target = launchd_service_target();
            run_command("launchctl", &["print", &target])
        }
    }
}

pub fn uninstall() -> Result<()> {
    match ServiceManager::current()? {
        #[cfg(target_os = "linux")]
        ServiceManager::SystemdUser => uninstall_systemd_user()?,
        #[cfg(target_os = "macos")]
        ServiceManager::LaunchdUser => uninstall_launchd_user()?,
    }
    println!("uninstalled");
    Ok(())
}

fn resolve_allow_shell(home: &FabricHome, requested: Option<bool>) -> Result<bool> {
    let mut config = FabricConfig::load(home)?;
    if let Some(allow_shell) = requested {
        config.set_allow_shell(allow_shell);
        config.save(home)?;
        return Ok(allow_shell);
    }
    Ok(config.allow_shell().unwrap_or(false))
}

fn resolve_allow_exec(home: &FabricHome, requested: Option<bool>) -> Result<bool> {
    let mut config = FabricConfig::load(home)?;
    if let Some(allow_exec) = requested {
        config.set_allow_exec(allow_exec);
        config.save(home)?;
        return Ok(allow_exec);
    }
    Ok(config.allow_exec().unwrap_or(false))
}

/// The command that restarts the managed Linux service, handed to systemd to
/// run on its own rather than issued in place.
///
/// `fabric exec` runs inside `fabric.service`'s cgroup, so a direct
/// `systemctl --user restart` tears down the caller mid-command. Scheduling it
/// as a transient unit moves the restart out of that cgroup and lets the caller
/// return first.
///
/// No `--unit` name is passed on purpose. systemd names the transient unit
/// itself, so two updates close together cannot collide on a name that already
/// exists — which would fail the second one for a reason nobody would guess.
fn systemd_restart_argv() -> (&'static str, Vec<String>) {
    (
        "systemd-run",
        vec![
            "--user".into(),
            // Long enough that the caller returns before its cgroup goes away,
            // short enough that an operator is not left waiting on it.
            "--on-active=3".into(),
            "systemctl".into(),
            "--user".into(),
            "restart".into(),
            SERVICE_NAME.into(),
        ],
    )
}

/// Resolve the memory ceiling, in the same shape as the two allow flags above.
///
/// The tri-state matters. A caller that says nothing must KEEP the persisted
/// ceiling; only an explicit `--no-memory-max-mb` removes one. Before this
/// existed the ceiling lived solely in the rendered unit, so every re-render
/// that did not name it threw it away, silently.
fn resolve_memory_max_mb(home: &FabricHome, requested: Option<Option<u64>>) -> Result<Option<u64>> {
    let mut config = FabricConfig::load(home)?;
    if let Some(memory_max_mb) = requested {
        config.set_memory_max_mb(memory_max_mb);
        config.save(home)?;
        return Ok(memory_max_mb);
    }
    Ok(config.memory_max_mb())
}

enum ServiceManager {
    #[cfg(target_os = "linux")]
    SystemdUser,
    #[cfg(target_os = "macos")]
    LaunchdUser,
}

impl ServiceManager {
    fn current() -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            return Ok(Self::SystemdUser);
        }
        #[cfg(target_os = "macos")]
        {
            return Ok(Self::LaunchdUser);
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            bail!("fabric service is currently supported on Linux systemd-user and macOS launchd");
        }
    }
}

#[cfg(target_os = "linux")]
fn install_systemd_user(spec: &ServiceSpec) -> Result<()> {
    let unit_path = systemd_user_unit_path()?;
    if let Some(parent) = unit_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&unit_path, render_systemd_user_unit(spec))
        .with_context(|| format!("failed to write {}", unit_path.display()))?;

    run_command("systemctl", &["--user", "daemon-reload"])?;
    run_command("systemctl", &["--user", "enable", SERVICE_NAME])?;
    let (program, args) = systemd_restart_argv();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    run_command(program, &args)?;
    println!("unit\t{}", unit_path.display());
    // Say scheduled, because it is. Claiming a restart that has not happened yet
    // would make a failed start look like a successful install.
    println!("restart\tscheduled");
    Ok(())
}

#[cfg(target_os = "linux")]
fn uninstall_systemd_user() -> Result<()> {
    let _ = Command::new("systemctl")
        .args(["--user", "disable", "--now", SERVICE_NAME])
        .status();
    let unit_path = systemd_user_unit_path()?;
    if unit_path.exists() {
        fs::remove_file(&unit_path)
            .with_context(|| format!("failed to remove {}", unit_path.display()))?;
    }
    run_command("systemctl", &["--user", "daemon-reload"])?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn install_launchd_user(home: &FabricHome, spec: &ServiceSpec) -> Result<()> {
    let plist_path = launch_agent_path()?;
    let domain = launchd_domain();
    let target = launchd_service_target();

    // Check the domain BEFORE writing anything. A LaunchAgent lives in the gui
    // domain, which only exists for a uid with an active login session, so
    // installing over ssh cannot work and launchctl reports it as an opaque
    // "Bootstrap failed: 5: Input/output error". Say what is actually wrong, and
    // leave the filesystem untouched rather than depositing a plist for a
    // service that was never registered.
    if !launchd_domain_available(&domain) {
        bail!("{}", launchd_domain_unavailable_message(&domain));
    }

    if let Some(parent) = plist_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    // Anything that fails from here leaves the host as it was found: the plist
    // back to its previous bytes or absent, and the label back to whatever
    // enable/disable state launchd had for it. Installing should not be able to
    // half-change a machine.
    let previously_disabled = launchd_label_disabled(&domain);
    let previous = fs::read(&plist_path).ok();
    fs::write(&plist_path, render_launch_agent_plist(home, spec)?)
        .with_context(|| format!("failed to write {}", plist_path.display()))?;
    let result = bootstrap_and_start(&plist_path, &domain, &target);
    if result.is_err() {
        restore_plist(&plist_path, previous.as_deref());
        if previously_disabled == Some(true) {
            let _ = Command::new("launchctl")
                .args(["disable", &target])
                .status();
        }
        return result;
    }

    println!("plist\t{}", plist_path.display());
    Ok(())
}

/// Restore a plist to what it was before a failed install: its previous bytes,
/// or absent if there was no file to begin with.
fn restore_plist(path: &std::path::Path, previous: Option<&[u8]>) {
    let restored = match previous {
        Some(bytes) => fs::write(path, bytes),
        None => fs::remove_file(path).or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(error)
            }
        }),
    };
    if let Err(error) = restored {
        eprintln!(
            "fabric: install failed and {} could not be restored: {error}",
            path.display()
        );
    }
}

/// Whether launchd holds a persistent disable override for our label, or None
/// when that cannot be determined. `fabric service uninstall` sets this, and it
/// outlives the plist, so install both clears it and restores it on failure.
fn launchd_label_disabled(domain: &str) -> Option<bool> {
    let output = Command::new("launchctl")
        .args(["print-disabled", domain])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let listing = String::from_utf8_lossy(&output.stdout);
    let line = listing.lines().find(|line| line.contains(LAUNCHD_LABEL))?;
    Some(line.contains("true") || line.contains("disabled"))
}

/// True when launchd can address this domain from the current session.
fn launchd_domain_available(domain: &str) -> bool {
    Command::new("launchctl")
        .args(["print", domain])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn launchd_domain_unavailable_message(domain: &str) -> String {
    format!(
        "launchd domain {domain} is not available from this session, so the managed service \
         cannot be registered.\n\
         A LaunchAgent lives in the gui domain, which exists only while that user has an active \
         login session, so `fabric service install` cannot work over ssh or from a headless \
         context.\n\
         Either run it from a terminal in a graphical session on that machine, or run the daemon \
         unmanaged with `fabric up` if the host has no login session to attach to.\n\
         Nothing was written; the previous service state is unchanged."
    )
}

#[cfg(target_os = "macos")]
fn bootstrap_and_start(plist_path: &std::path::Path, domain: &str, target: &str) -> Result<()> {
    let plist = plist_path.display().to_string();
    launchd_register(&plist, domain, target, &mut real_launchctl)
}

/// What `launchctl` did, so a test can assert the order rather than the source.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LaunchctlStep {
    Bootout,
    Enable,
    Bootstrap,
    Kickstart,
}

fn real_launchctl(step: LaunchctlStep, args: &[&str]) -> Result<()> {
    match step {
        LaunchctlStep::Bootout => {
            // On a FRESH install there is nothing to unload, and launchctl exits
            // non-zero with "Boot-out failed: 3: No such process" — harmless and
            // confusing. Swallow that case; surface a REAL bootout failure.
            if let Ok(output) = Command::new("launchctl").args(args).output()
                && !output.status.success()
            {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !bootout_failure_is_ignorable(output.status.code(), &stderr) {
                    eprint!("{stderr}");
                }
            }
            Ok(())
        }
        LaunchctlStep::Bootstrap => {
            // args are ["bootstrap", domain, plist]; the loaded-check target is
            // domain/label, which the caller passes as the fourth element.
            bootstrap_launchd_with_retry(args[1], args[2], args[3])
        }
        _ => run_command("launchctl", args),
    }
}

/// Register the service with launchd, in the one order that works from any prior
/// state.
///
/// `fabric service uninstall` runs `launchctl disable`, and that override
/// persists in launchd's per-user database across reboots and plist rewrites. A
/// disabled label cannot be bootstrapped, so enabling AFTER bootstrap meant an
/// uninstall permanently poisoned every later install: bootstrap failed with an
/// opaque "Input/output error" and never reached the enable that would have
/// fixed it. Enable first and install is idempotent from any prior state.
fn launchd_register(
    plist: &str,
    domain: &str,
    target: &str,
    run: &mut dyn FnMut(LaunchctlStep, &[&str]) -> Result<()>,
) -> Result<()> {
    // Stop any existing instance and WAIT for launchd to fully unload it, so a
    // re-install over a running managed daemon does not race bootout->bootstrap.
    run(LaunchctlStep::Bootout, &["bootout", target])?;
    wait_for_launchd_unloaded(target, LAUNCHD_UNLOAD_TIMEOUT);
    run(LaunchctlStep::Enable, &["enable", target])?;
    run(
        LaunchctlStep::Bootstrap,
        &["bootstrap", domain, plist, target],
    )?;
    // `bootstrap` starts a RunAtLoad job. A plain kickstart covers a service that
    // had previously been disabled without killing a process still binding its
    // endpoint and control socket; `kickstart -k` would race readiness.
    run(LaunchctlStep::Kickstart, &["kickstart", target])?;
    Ok(())
}

/// launchctl `bootout` fails when there is nothing to unload — a fresh install or
/// an already-stopped service — with "No such process" (ESRCH, code 3) or
/// "Could not find service …". That is expected and harmless; every other failure
/// (e.g. an I/O error) is worth surfacing.
fn bootout_failure_is_ignorable(code: Option<i32>, stderr: &str) -> bool {
    code == Some(3) || stderr.contains("No such process") || stderr.contains("Could not find")
}

/// True if launchd currently has the service loaded in the domain.
fn launchd_service_loaded(target: &str) -> bool {
    Command::new("launchctl")
        .args(["print", target])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Block until the service is no longer loaded, or the timeout elapses. `bootout`
/// returns before launchd has finished unloading, so bootstrapping immediately
/// can hit the loaded/unloading label and fail with EIO.
fn wait_for_launchd_unloaded(target: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while launchd_service_loaded(target) && Instant::now() < deadline {
        thread::sleep(LAUNCHD_RETRY_BACKOFF);
    }
}

/// Bootstrap the service, retrying on transient failure. Treats "already loaded"
/// (e.g. a concurrent bootstrap won the race) as success, so a re-install is
/// idempotent and never leaves the daemon dead.
fn bootstrap_launchd_with_retry(domain: &str, plist: &str, target: &str) -> Result<()> {
    let mut last = String::new();
    for attempt in 1..=LAUNCHD_BOOTSTRAP_MAX_ATTEMPTS {
        let status = Command::new("launchctl")
            .args(["bootstrap", domain, plist])
            .status()
            .with_context(|| "failed to run launchctl bootstrap")?;
        if status.success() || launchd_service_loaded(target) {
            return Ok(());
        }
        last = status.to_string();
        if attempt < LAUNCHD_BOOTSTRAP_MAX_ATTEMPTS {
            thread::sleep(LAUNCHD_RETRY_BACKOFF);
            // A prior instance may still have been settling; re-wait before retry.
            wait_for_launchd_unloaded(target, LAUNCHD_UNLOAD_TIMEOUT);
        }
    }
    bail!(
        "launchctl bootstrap {plist} failed after {LAUNCHD_BOOTSTRAP_MAX_ATTEMPTS} attempts \
         (last {last}).\n\
         A persistent launchd override is the usual cause: check \
         `launchctl print-disabled {domain} | grep fabric`. A label left disabled by a previous \
         `fabric service uninstall` cannot be bootstrapped, and launchctl reports it as an \
         opaque I/O error. This install enables the label first, so if you still see this, \
         capture `launchctl print {target}` before re-running."
    )
}

#[cfg(target_os = "macos")]
fn uninstall_launchd_user() -> Result<()> {
    let plist_path = launch_agent_path()?;
    let target = launchd_service_target();
    let _ = Command::new("launchctl")
        .args(["bootout", &target])
        .status();
    let _ = Command::new("launchctl")
        .args(["disable", &target])
        .status();
    if plist_path.exists() {
        fs::remove_file(&plist_path)
            .with_context(|| format!("failed to remove {}", plist_path.display()))?;
    }
    Ok(())
}

fn run_command(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to run {program} {}", args.join(" ")))?;
    if !status.success() {
        bail!("{program} {} failed with status {status}", args.join(" "));
    }
    Ok(())
}

fn home_dir() -> Result<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}

#[cfg(target_os = "linux")]
fn systemd_user_unit_path() -> Result<PathBuf> {
    let base = match env::var_os("XDG_CONFIG_HOME") {
        Some(path) => PathBuf::from(path),
        None => home_dir()?.join(".config"),
    };
    Ok(base.join("systemd/user").join(SERVICE_NAME))
}

#[cfg(target_os = "macos")]
pub(crate) fn launch_agent_path() -> Result<PathBuf> {
    Ok(home_dir()?
        .join("Library/LaunchAgents")
        .join(format!("{LAUNCHD_LABEL}.plist")))
}

#[cfg(target_os = "macos")]
fn launchd_domain() -> String {
    format!("gui/{}", unsafe { libc::geteuid() })
}

#[cfg(target_os = "macos")]
fn launchd_service_target() -> String {
    format!("{}/{}", launchd_domain(), LAUNCHD_LABEL)
}

pub fn render_systemd_user_unit(spec: &ServiceSpec) -> String {
    let exec_start = spec
        .program_arguments()
        .iter()
        .map(|arg| systemd_quote_arg(arg))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "[Unit]\n\
Description=fabric iroh transport daemon\n\
\n\
[Service]\n\
Type=simple\n\
ExecStart={exec_start}\n\
Restart=on-failure\n\
RestartSec=5s\n\
LimitNOFILE=8192\n\
{memory_max}WorkingDirectory={}\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        systemd_quote_arg(&spec.home.display().to_string()),
        memory_max = spec
            .memory_max_mb
            .map(|mb| format!("MemoryMax={mb}M\n"))
            .unwrap_or_default()
    )
}

pub fn render_launch_agent_plist(home: &FabricHome, spec: &ServiceSpec) -> Result<String> {
    // A resident-set ceiling is written only when the operator asked for one.
    // launchd treats ResidentSetSize as a reclaim preference rather than a kill,
    // but it is still a fixed number, and shipping one by default declares a
    // healthy working set nobody has measured yet.
    // A DESCRIPTOR CEILING IS NOT OPTIONAL, unlike the resident-set one.
    //
    // A launchd agent inherits launchd's own default, and that default is small.
    // This daemon holds a QUIC endpoint, a connection per peer, a control
    // socket, a dial socket per tunnel and the files it is syncing, so the
    // default is not a ceiling anybody chose for it.
    //
    // It ran out: `service.err.log` on the Mac carries
    // `Error: Too many open files (os error 24)` and the daemon died there.
    //
    // 8192 is not a measurement, and saying so matters. It is simply far above
    // any working set this daemon has shown, which for the entry that syncs
    // 17,600 files sat at a few dozen descriptors. The point is to remove an
    // arbitrary small number, not to install a different arbitrary number close
    // enough to matter.
    let mut soft_limits = String::from(
        "        <key>NumberOfFiles</key>\n\
        <integer>8192</integer>\n",
    );
    let mut hard_limits = String::new();

    // The resident-set ceiling stays opt-in. launchd treats ResidentSetSize as a
    // reclaim preference rather than a kill, but it is still a fixed number, and
    // shipping one by default declares a healthy working set nobody has measured.
    if let Some(mb) = spec.memory_max_mb {
        let rss_bytes = mb
            .checked_mul(1024)
            .and_then(|value| value.checked_mul(1024))
            .context("--memory-max-mb is too large")?;
        soft_limits.push_str(&format!(
            "        <key>ResidentSetSize</key>\n\
        <integer>{rss_bytes}</integer>\n"
        ));
        hard_limits = format!(
            "    <key>HardResourceLimits</key>\n\
    <dict>\n\
        <key>ResidentSetSize</key>\n\
        <integer>{rss_bytes}</integer>\n\
    </dict>\n"
        );
    }
    let resource_limits = format!(
        "    <key>SoftResourceLimits</key>\n\
    <dict>\n\
{soft_limits}    </dict>\n\
{hard_limits}"
    );
    let stdout_path = home.root().join("logs/service.out.log");
    let stderr_path = home.root().join("logs/service.err.log");
    let args = spec
        .program_arguments()
        .iter()
        .map(|arg| format!("        <string>{}</string>", xml_escape(arg)))
        .collect::<Vec<_>>()
        .join("\n");
    Ok(format!(
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
    <dict>\n\
        <key>SuccessfulExit</key>\n\
        <false/>\n\
    </dict>\n\
    <key>WorkingDirectory</key>\n\
    <string>{}</string>\n\
    <key>StandardOutPath</key>\n\
    <string>{}</string>\n\
    <key>StandardErrorPath</key>\n\
    <string>{}</string>\n\
{resource_limits}\
</dict>\n\
</plist>\n",
        xml_escape(LAUNCHD_LABEL),
        args,
        xml_escape(&home.root().display().to_string()),
        xml_escape(&stdout_path.display().to_string()),
        xml_escape(&stderr_path.display().to_string())
    ))
}

fn systemd_quote_arg(arg: &str) -> String {
    if !arg.is_empty()
        && arg.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'/' | b'.' | b'_' | b':' | b'-' | b'+' | b'=')
        })
    {
        return arg.to_string();
    }

    let mut quoted = String::from("\"");
    for ch in arg.chars() {
        match ch {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '$' => quoted.push_str("$$"),
            '%' => quoted.push_str("%%"),
            _ => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

fn xml_escape(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The unit must name the binary it was GIVEN, not the one rendering it.
    ///
    /// `fabric update` installs new bytes at the path the service manager
    /// already runs, then re-renders. If rendering used `current_exe` the unit
    /// would point at whichever binary ran the update — a `target/debug` build
    /// while testing, and in general nobody's idea of the installed fabric. That
    /// is the "install at the wrong path" trap entered from the other side.
    #[test]
    fn the_unit_names_the_binary_it_was_given_not_the_one_rendering_it() -> Result<()> {
        let home = FabricHome::new(Path::new("/home/nathan/.local/share/fabric"));
        let spec = ServiceSpec::new("/usr/local/bin/fabric", home.root(), true, true, None)?;

        let unit = render_systemd_user_unit(&spec);
        assert!(
            unit.contains("ExecStart=/usr/local/bin/fabric"),
            "the unit does not run the binary it was given:\n{unit}"
        );
        let plist = render_launch_agent_plist(&home, &spec)?;
        assert!(
            plist.contains("<string>/usr/local/bin/fabric</string>"),
            "the plist does not run the binary it was given:\n{plist}"
        );

        // And specifically not this test binary, which is what `current_exe`
        // would have produced.
        let running = std::env::current_exe()?.display().to_string();
        assert!(
            !unit.contains(&running) && !plist.contains(&running),
            "the rendered unit picked up the running binary instead of the given one"
        );
        Ok(())
    }

    /// A restart issued from inside the service's own cgroup kills the process
    /// issuing it.
    ///
    /// `fabric exec` runs its session inside `fabric.service`, so
    /// `fabric exec hetz -- fabric service install` restarts the very cgroup the
    /// caller lives in and dies partway through. That is reachable today with a
    /// command any of us might run.
    ///
    /// The restart therefore has to be handed to systemd to run on its own,
    /// outside the caller's cgroup. This pins the shape rather than the effect,
    /// because the failure mode is that the CALLER dies — a test that waited for
    /// the effect would be the thing that got killed.
    #[test]
    fn the_linux_service_restart_is_detached_from_the_caller() {
        let (program, args) = systemd_restart_argv();
        assert_eq!(
            program, "systemd-run",
            "the restart is issued in place, so it kills its own caller"
        );
        assert!(
            args.iter().any(|arg| arg == "--user"),
            "a user service restart must stay in the user manager: {args:?}"
        );
        assert!(
            args.iter().any(|arg| arg.starts_with("--on-active=")),
            "the restart must be scheduled, so the caller can return first: {args:?}"
        );
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        assert!(
            args.windows(4)
                .any(|w| w == ["systemctl", "--user", "restart", SERVICE_NAME]),
            "whatever wrapping it gains, it must still restart {SERVICE_NAME}: {args:?}"
        );
    }

    /// A ceiling an operator set once must survive a re-install that does not
    /// mention it.
    ///
    /// `allow_shell` and `allow_exec` already survive, because they round trip
    /// through `config.toml`. `memory_max_mb` did not: it lived only in the
    /// rendered plist or unit, and `render_*` emits it only when `Some`. So any
    /// re-install without `--memory-max-mb` removed a ceiling somebody set
    /// earlier, silently.
    ///
    /// `fabric update` re-renders the unit on every run, so this would have
    /// fired constantly rather than rarely.
    #[test]
    fn a_memory_ceiling_survives_a_reinstall_that_does_not_mention_it() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let home = FabricHome::new(dir.path());
        home.prepare()?;

        // An operator sets a ceiling once.
        assert_eq!(
            resolve_memory_max_mb(&home, Some(Some(512)))?,
            Some(512),
            "the ceiling the operator asked for was not the one applied"
        );

        // A later install says nothing about memory. It must not remove it.
        assert_eq!(
            resolve_memory_max_mb(&home, None)?,
            Some(512),
            "a re-install that never mentioned the ceiling removed it"
        );
        Ok(())
    }


    #[test]
    fn bootout_no_such_process_is_ignorable_but_real_errors_surface() {
        // Fresh install / already-stopped service — nothing to unload — suppress.
        assert!(bootout_failure_is_ignorable(
            Some(3),
            "Boot-out failed: 3: No such process\n"
        ));
        assert!(bootout_failure_is_ignorable(
            None,
            "Could not find service \"com.compoundingtech.fabric\" in domain\n"
        ));
        // A real failure (e.g. the bootstrap-race I/O error) must still surface.
        assert!(!bootout_failure_is_ignorable(
            Some(5),
            "Boot-out failed: 5: Input/output error\n"
        ));
        assert!(!bootout_failure_is_ignorable(
            Some(1),
            "some other launchctl error\n"
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_kickstart_does_not_kill_the_bootstrapped_process() {
        // `kickstart -k` would terminate the PID bootstrap just created and race
        // readiness; the plain form only covers a previously disabled service.
        let mut kickstart_args = Vec::new();
        let _ = launchd_register(
            "/tmp/x.plist",
            "gui/501",
            "gui/501/com.compoundingtech.fabric",
            &mut |step, args| {
                if step == LaunchctlStep::Kickstart {
                    kickstart_args = args.iter().map(|a| a.to_string()).collect();
                }
                Ok(())
            },
        );
        assert_eq!(
            kickstart_args,
            ["kickstart", "gui/501/com.compoundingtech.fabric"]
        );
        assert!(!kickstart_args.iter().any(|a| a == "-k"));
    }

    #[test]
    fn control_socket_wait_returns_false_when_nothing_is_listening() {
        // A service manager returns once it has STARTED the daemon, not once the
        // daemon can answer, so install waits for a real accept. The socket file
        // appears before anything listens on it, which is why the check connects
        // rather than checking existence.
        let temp = tempfile::tempdir().expect("tempdir");
        let home = FabricHome::new(temp.path());
        std::fs::create_dir_all(temp.path().join("run")).expect("run dir");
        std::fs::write(home.control_socket_path(), b"not a socket").expect("decoy");
        assert!(home.control_socket_path().exists());

        let started = Instant::now();
        assert!(
            !wait_for_control_socket(&home, Duration::from_millis(250)),
            "a path that exists but does not accept must not count as ready"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(250),
            "must wait the full deadline"
        );
    }

    #[test]
    fn control_socket_wait_returns_true_once_something_accepts() {
        let temp = tempfile::tempdir().expect("tempdir");
        let home = FabricHome::new(temp.path());
        std::fs::create_dir_all(temp.path().join("run")).expect("run dir");
        let _listener = std::os::unix::net::UnixListener::bind(home.control_socket_path())
            .expect("bind control socket");
        assert!(wait_for_control_socket(&home, Duration::from_secs(2)));
    }

    #[test]
    fn install_enables_the_label_before_bootstrapping_it() {
        // The Bluey incident: `fabric service uninstall` runs `launchctl disable`,
        // that override persists, and a disabled label cannot be bootstrapped.
        // Enabling only after bootstrap made an uninstall poison every later
        // install with an opaque I/O error. This fake refuses to bootstrap while
        // the label is disabled, exactly as launchd does, so the test fails if the
        // order regresses.
        let mut disabled = true;
        let mut steps = Vec::new();
        let result = launchd_register(
            "/tmp/com.compoundingtech.fabric.plist",
            "gui/502",
            "gui/502/com.compoundingtech.fabric",
            &mut |step, args| {
                steps.push(step.clone());
                match step {
                    LaunchctlStep::Enable => {
                        assert_eq!(args[0], "enable");
                        disabled = false;
                        Ok(())
                    }
                    LaunchctlStep::Bootstrap if disabled => {
                        bail!("Bootstrap failed: 5: Input/output error")
                    }
                    _ => Ok(()),
                }
            },
        );

        assert!(result.is_ok(), "install must succeed from a disabled label");
        let enable = steps
            .iter()
            .position(|step| *step == LaunchctlStep::Enable)
            .expect("enable ran");
        let bootstrap = steps
            .iter()
            .position(|step| *step == LaunchctlStep::Bootstrap)
            .expect("bootstrap ran");
        assert!(
            enable < bootstrap,
            "enable must precede bootstrap, got {steps:?}"
        );
        assert_eq!(steps.first(), Some(&LaunchctlStep::Bootout));
        assert_eq!(steps.last(), Some(&LaunchctlStep::Kickstart));
    }

    #[test]
    fn a_failed_install_leaves_no_plist_behind() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("com.compoundingtech.fabric.plist");

        // Nothing there before: a failure must leave nothing there after.
        std::fs::write(&path, b"written by a failing install").expect("write");
        restore_plist(&path, None);
        assert!(
            !path.exists(),
            "a failed fresh install must remove its plist"
        );

        // Something there before: a failure must put the original bytes back.
        let original = b"<plist>previous</plist>";
        std::fs::write(&path, original).expect("write");
        std::fs::write(&path, b"half-written replacement").expect("overwrite");
        restore_plist(&path, Some(original));
        assert_eq!(
            std::fs::read(&path).expect("read"),
            original,
            "a failed re-install must restore the previous plist"
        );
    }

    #[test]
    fn service_declares_no_memory_ceiling_unless_the_operator_sets_one() -> Result<()> {
        // Nathan's rule: no fixed product memory policy while a healthy working
        // set is unmeasured. An install with no --memory-max-mb must emit neither
        // a systemd MemoryMax nor launchd resident-set limits.
        let home = FabricHome::new(std::path::Path::new("/home/nathan/.local/share/fabric"));
        let spec = ServiceSpec::new("/usr/local/bin/fabric", home.root(), true, true, None)?;

        let unit = render_systemd_user_unit(&spec);
        assert!(!unit.contains("MemoryMax"));
        assert!(unit.contains("Restart=on-failure"));
        // Same reasoning as the launchd plist: the descriptor ceiling is not
        // opt-in, because the inherited default is a number nobody chose.
        assert!(unit.contains("LimitNOFILE=8192"));

        let plist = render_launch_agent_plist(&home, &spec)?;
        // The resident-set ceiling stays opt-in, so none of it appears here.
        assert!(!plist.contains("ResidentSetSize"));
        assert!(!plist.contains("HardResourceLimits"));
        // The DESCRIPTOR ceiling is not opt-in. Without it the daemon inherits
        // launchd's own small default, and it has already died of that:
        // `Error: Too many open files (os error 24)`.
        assert!(plist.contains("<key>SoftResourceLimits</key>"));
        assert!(plist.contains("<key>NumberOfFiles</key>"));
        assert!(plist.contains("<integer>8192</integer>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
        Ok(())
    }

    #[test]
    fn systemd_unit_runs_foreground_daemon_with_restart_and_memory_limit() -> Result<()> {
        let spec = ServiceSpec::new(
            "/usr/local/bin/fabric",
            "/home/nathan/.local/share/fabric",
            true,
            true,
            Some(512),
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(unit.contains("ExecStart=/usr/local/bin/fabric --home /home/nathan/.local/share/fabric daemon --allow-shell --allow-exec"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("RestartSec=5s"));
        assert!(unit.contains("MemoryMax=512M"));
        assert!(unit.contains("WantedBy=default.target"));
        Ok(())
    }

    #[test]
    fn systemd_unit_quotes_paths_and_escapes_specifiers() -> Result<()> {
        let spec = ServiceSpec::new(
            "/Applications/Fabric Tools/fabric",
            "/Users/nathan/Fabric 100%",
            false,
            false,
            Some(256),
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(unit.contains("ExecStart=\"/Applications/Fabric Tools/fabric\" --home \"/Users/nathan/Fabric 100%%\" daemon"));
        assert!(unit.contains("WorkingDirectory=\"/Users/nathan/Fabric 100%%\""));
        Ok(())
    }

    #[test]
    fn default_launch_agent_uses_one_gib_resident_set_headroom() -> Result<()> {
        let home = FabricHome::new("/Users/nathan/.local/share/fabric");
        let spec = ServiceSpec::new(
            "/Users/nathan/.local/bin/fabric",
            home.root(),
            false,
            false,
            Some(1024),
        )?;

        let plist = render_launch_agent_plist(&home, &spec)?;

        assert!(plist.contains("<integer>1073741824</integer>"));
        Ok(())
    }

    #[test]
    fn launch_agent_runs_foreground_daemon_with_keepalive_and_memory_limit() -> Result<()> {
        let home = FabricHome::new("/Users/nathan/.local/share/fabric");
        let spec = ServiceSpec::new(
            "/Users/nathan/.local/bin/fabric",
            home.root(),
            true,
            true,
            Some(512),
        )?;

        let plist = render_launch_agent_plist(&home, &spec)?;

        assert!(plist.contains("<string>com.compoundingtech.fabric</string>"));
        assert!(plist.contains("<string>/Users/nathan/.local/bin/fabric</string>"));
        assert!(plist.contains("<string>--home</string>"));
        assert!(plist.contains("<string>/Users/nathan/.local/share/fabric</string>"));
        assert!(plist.contains("<string>daemon</string>"));
        assert!(plist.contains("<string>--allow-shell</string>"));
        assert!(plist.contains("<string>--allow-exec</string>"));
        assert!(plist.contains("<key>SuccessfulExit</key>"));
        assert!(plist.contains("<false/>"));
        assert!(plist.contains("<key>ResidentSetSize</key>"));
        assert!(plist.contains("<integer>536870912</integer>"));
        Ok(())
    }

    #[test]
    fn launch_agent_xml_escapes_paths() -> Result<()> {
        let home = FabricHome::new("/Users/nathan/Fabric & Test");
        let spec = ServiceSpec::new("/tmp/fabric<dev>", home.root(), false, false, Some(128))?;

        let plist = render_launch_agent_plist(&home, &spec)?;

        assert!(plist.contains("<string>/tmp/fabric&lt;dev&gt;</string>"));
        assert!(plist.contains("<string>/Users/nathan/Fabric &amp; Test</string>"));
        assert!(!plist.contains("<string>--allow-shell</string>"));
        Ok(())
    }
}

#[cfg(test)]
mod plist_validity {
    use super::*;

    /// A malformed plist does not fail a string assertion, it fails at install
    /// time on somebody's machine. Render both branches and hand them to the
    /// system parser.
    #[test]
    fn rendered_plists_are_valid_property_lists() -> Result<()> {
        let home = FabricHome::new(std::path::Path::new("/home/nathan/.local/share/fabric"));
        for memory in [None, Some(512u64)] {
            let spec = ServiceSpec::new("/usr/local/bin/fabric", home.root(), true, true, memory)?;
            let plist = render_launch_agent_plist(&home, &spec)?;
            let path = std::env::temp_dir().join(format!("fabric-plist-{:?}.plist", memory));
            std::fs::write(&path, &plist)?;
            let out = std::process::Command::new("plutil")
                .arg("-lint")
                .arg(&path)
                .output();
            let _ = std::fs::remove_file(&path);
            match out {
                Ok(out) => assert!(
                    out.status.success(),
                    "plutil rejected the plist for memory_max_mb={memory:?}: {}\n{plist}",
                    String::from_utf8_lossy(&out.stderr)
                ),
                // Not a Mac, so there is nothing to lint with. Say so rather
                // than passing silently as if it had been checked.
                Err(_) => eprintln!("plutil unavailable, plist not linted here"),
            }
        }
        Ok(())
    }
}
