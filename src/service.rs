use std::{
    env, fs,
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

use crate::config::{FabricConfig, FabricHome};

#[cfg(target_os = "linux")]
const SERVICE_NAME: &str = "fabric.service";
const LAUNCHD_LABEL: &str = "com.compoundingtech.fabric";
/// How long to wait for launchd to fully unload a booted-out service before
/// bootstrapping the same label again — bootout is async, and bootstrapping a
/// still-loaded label races into "Bootstrap failed: 5: Input/output error".
const LAUNCHD_UNLOAD_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll interval while waiting for unload / between bootstrap retries.
const LAUNCHD_RETRY_BACKOFF: Duration = Duration::from_millis(300);
/// Bootstrap attempts before giving up — a re-install must be safe to re-run.
const LAUNCHD_BOOTSTRAP_MAX_ATTEMPTS: usize = 5;
pub const DEFAULT_MEMORY_MAX_MB: u64 = 1024;

#[derive(Debug, Clone, Copy)]
pub struct ServiceInstallOptions {
    pub allow_shell: Option<bool>,
    pub allow_exec: Option<bool>,
    pub memory_max_mb: u64,
}

#[derive(Debug, Clone)]
pub struct ServiceSpec {
    exe: PathBuf,
    home: PathBuf,
    allow_shell: bool,
    allow_exec: bool,
    memory_max_mb: u64,
}

impl ServiceSpec {
    pub fn new(
        exe: impl Into<PathBuf>,
        home: impl Into<PathBuf>,
        allow_shell: bool,
        allow_exec: bool,
        memory_max_mb: u64,
    ) -> Result<Self> {
        if memory_max_mb == 0 {
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
        memory_max_mb: u64,
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
    let spec = ServiceSpec::current(home, allow_shell, allow_exec, options.memory_max_mb)?;
    match ServiceManager::current()? {
        #[cfg(target_os = "linux")]
        ServiceManager::SystemdUser => install_systemd_user(&spec)?,
        #[cfg(target_os = "macos")]
        ServiceManager::LaunchdUser => install_launchd_user(home, &spec)?,
    }
    println!("installed");
    println!("home\t{}", home.root().display());
    println!("allow-shell\t{allow_shell}");
    println!("allow-exec\t{allow_exec}");
    println!("memory-max-mb\t{}", options.memory_max_mb);
    Ok(())
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
    run_command("systemctl", &["--user", "restart", SERVICE_NAME])?;
    println!("unit\t{}", unit_path.display());
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
    if let Some(parent) = plist_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(&plist_path, render_launch_agent_plist(home, spec)?)
        .with_context(|| format!("failed to write {}", plist_path.display()))?;

    let plist = plist_path.display().to_string();
    let domain = launchd_domain();
    let target = launchd_service_target();
    // Stop any existing instance, WAIT for launchd to fully unload it, then
    // bootstrap with a bounded retry. Without the settle-and-retry, a re-install
    // over a running managed daemon races bootout->bootstrap into
    // "Bootstrap failed: 5: Input/output error" and leaves NO daemon running —
    // which, for cos's only path to hetz, is an outage. A re-install must be
    // idempotent and safe to re-run.
    // On a FRESH install there is nothing to unload, and launchctl exits non-zero
    // with "Boot-out failed: 3: No such process" — harmless and confusing. Capture
    // its output and swallow that case; only surface a REAL bootout failure.
    if let Ok(output) = Command::new("launchctl").args(["bootout", &target]).output()
        && !output.status.success()
    {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !bootout_failure_is_ignorable(output.status.code(), &stderr) {
            eprint!("{stderr}");
        }
    }
    wait_for_launchd_unloaded(&target, LAUNCHD_UNLOAD_TIMEOUT);
    bootstrap_launchd_with_retry(&domain, &plist, &target)?;
    run_command("launchctl", &["enable", &target])?;
    // `bootstrap` starts a RunAtLoad job. A plain kickstart covers a service
    // that had previously been disabled without killing a process that is
    // still binding its endpoint and control socket. `kickstart -k` here races
    // readiness by terminating the PID that bootstrap just created.
    run_command("launchctl", &launchd_kickstart_args(&target))?;
    println!("plist\t{}", plist_path.display());
    Ok(())
}

#[cfg(target_os = "macos")]
fn launchd_kickstart_args(target: &str) -> [&str; 2] {
    ["kickstart", target]
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
         (last {last}); the service may be in a stuck state — try `launchctl bootout {target}` \
         then re-run"
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
fn launch_agent_path() -> Result<PathBuf> {
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
MemoryMax={}M\n\
WorkingDirectory={}\n\
\n\
[Install]\n\
WantedBy=default.target\n",
        spec.memory_max_mb,
        systemd_quote_arg(&spec.home.display().to_string())
    )
}

pub fn render_launch_agent_plist(home: &FabricHome, spec: &ServiceSpec) -> Result<String> {
    let rss_bytes = spec
        .memory_max_mb
        .checked_mul(1024)
        .and_then(|value| value.checked_mul(1024))
        .context("--memory-max-mb is too large")?;
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
    <key>SoftResourceLimits</key>\n\
    <dict>\n\
        <key>ResidentSetSize</key>\n\
        <integer>{rss_bytes}</integer>\n\
    </dict>\n\
    <key>HardResourceLimits</key>\n\
    <dict>\n\
        <key>ResidentSetSize</key>\n\
        <integer>{rss_bytes}</integer>\n\
    </dict>\n\
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
        assert!(!bootout_failure_is_ignorable(Some(1), "some other launchctl error\n"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_kickstart_does_not_kill_the_bootstrapped_process() {
        let args = launchd_kickstart_args("gui/501/com.compoundingtech.fabric");
        assert_eq!(args, ["kickstart", "gui/501/com.compoundingtech.fabric"]);
        assert!(!args.contains(&"-k"));
    }

    #[test]
    fn default_systemd_unit_uses_one_gib_memory_headroom() -> Result<()> {
        let spec = ServiceSpec::new(
            "/usr/local/bin/fabric",
            "/home/nathan/.local/share/fabric",
            false,
            false,
            DEFAULT_MEMORY_MAX_MB,
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert_eq!(DEFAULT_MEMORY_MAX_MB, 1024);
        assert!(unit.contains("MemoryMax=1024M"));
        Ok(())
    }

    #[test]
    fn systemd_unit_runs_foreground_daemon_with_restart_and_memory_limit() -> Result<()> {
        let spec = ServiceSpec::new(
            "/usr/local/bin/fabric",
            "/home/nathan/.local/share/fabric",
            true,
            true,
            512,
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
            256,
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
            DEFAULT_MEMORY_MAX_MB,
        )?;

        let plist = render_launch_agent_plist(&home, &spec)?;

        assert_eq!(DEFAULT_MEMORY_MAX_MB, 1024);
        assert!(plist.contains("<integer>1073741824</integer>"));
        Ok(())
    }

    #[test]
    fn launch_agent_runs_foreground_daemon_with_keepalive_and_memory_limit() -> Result<()> {
        let home = FabricHome::new("/Users/nathan/.local/share/fabric");
        let spec =
            ServiceSpec::new("/Users/nathan/.local/bin/fabric", home.root(), true, true, 512)?;

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
        let spec = ServiceSpec::new("/tmp/fabric<dev>", home.root(), false, false, 128)?;

        let plist = render_launch_agent_plist(&home, &spec)?;

        assert!(plist.contains("<string>/tmp/fabric&lt;dev&gt;</string>"));
        assert!(plist.contains("<string>/Users/nathan/Fabric &amp; Test</string>"));
        assert!(!plist.contains("<string>--allow-shell</string>"));
        Ok(())
    }
}
