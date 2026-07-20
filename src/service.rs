use std::{env, fs, path::PathBuf, process::Command};

use anyhow::{Context, Result, bail};

use crate::config::{FabricConfig, FabricHome};

#[cfg(target_os = "linux")]
const SERVICE_NAME: &str = "fabric.service";
const LAUNCHD_LABEL: &str = "com.compoundingtech.fabric";
pub const DEFAULT_MEMORY_MAX_MB: u64 = 1024;

#[derive(Debug, Clone, Copy)]
pub struct ServiceInstallOptions {
    pub allow_shell: Option<bool>,
    pub memory_max_mb: u64,
}

#[derive(Debug, Clone)]
pub struct ServiceSpec {
    exe: PathBuf,
    home: PathBuf,
    allow_shell: bool,
    memory_max_mb: u64,
}

impl ServiceSpec {
    pub fn new(
        exe: impl Into<PathBuf>,
        home: impl Into<PathBuf>,
        allow_shell: bool,
        memory_max_mb: u64,
    ) -> Result<Self> {
        if memory_max_mb == 0 {
            bail!("--memory-max-mb must be greater than zero");
        }
        Ok(Self {
            exe: exe.into(),
            home: home.into(),
            allow_shell,
            memory_max_mb,
        })
    }

    fn current(home: &FabricHome, allow_shell: bool, memory_max_mb: u64) -> Result<Self> {
        let exe = env::current_exe().context("failed to resolve current fabric executable")?;
        Self::new(exe, home.root(), allow_shell, memory_max_mb)
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
        args
    }
}

pub fn install(home: &FabricHome, options: ServiceInstallOptions) -> Result<()> {
    home.prepare()?;
    let allow_shell = resolve_allow_shell(home, options.allow_shell)?;
    let spec = ServiceSpec::current(home, allow_shell, options.memory_max_mb)?;
    match ServiceManager::current()? {
        #[cfg(target_os = "linux")]
        ServiceManager::SystemdUser => install_systemd_user(&spec)?,
        #[cfg(target_os = "macos")]
        ServiceManager::LaunchdUser => install_launchd_user(home, &spec)?,
    }
    println!("installed");
    println!("home\t{}", home.root().display());
    println!("allow-shell\t{allow_shell}");
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
    let _ = Command::new("launchctl")
        .args(["bootout", &target])
        .status();
    run_command("launchctl", &["bootstrap", &domain, &plist])?;
    run_command("launchctl", &["enable", &target])?;
    run_command("launchctl", &["kickstart", "-k", &target])?;
    println!("plist\t{}", plist_path.display());
    Ok(())
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
    fn default_systemd_unit_uses_one_gib_memory_headroom() -> Result<()> {
        let spec = ServiceSpec::new(
            "/usr/local/bin/fabric",
            "/home/nathan/.local/share/fabric",
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
            512,
        )?;

        let unit = render_systemd_user_unit(&spec);

        assert!(unit.contains("ExecStart=/usr/local/bin/fabric --home /home/nathan/.local/share/fabric daemon --allow-shell"));
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
        let spec = ServiceSpec::new("/Users/nathan/.local/bin/fabric", home.root(), true, 512)?;

        let plist = render_launch_agent_plist(&home, &spec)?;

        assert!(plist.contains("<string>com.compoundingtech.fabric</string>"));
        assert!(plist.contains("<string>/Users/nathan/.local/bin/fabric</string>"));
        assert!(plist.contains("<string>--home</string>"));
        assert!(plist.contains("<string>/Users/nathan/.local/share/fabric</string>"));
        assert!(plist.contains("<string>daemon</string>"));
        assert!(plist.contains("<string>--allow-shell</string>"));
        assert!(plist.contains("<key>SuccessfulExit</key>"));
        assert!(plist.contains("<false/>"));
        assert!(plist.contains("<key>ResidentSetSize</key>"));
        assert!(plist.contains("<integer>536870912</integer>"));
        Ok(())
    }

    #[test]
    fn launch_agent_xml_escapes_paths() -> Result<()> {
        let home = FabricHome::new("/Users/nathan/Fabric & Test");
        let spec = ServiceSpec::new("/tmp/fabric<dev>", home.root(), false, 128)?;

        let plist = render_launch_agent_plist(&home, &spec)?;

        assert!(plist.contains("<string>/tmp/fabric&lt;dev&gt;</string>"));
        assert!(plist.contains("<string>/Users/nathan/Fabric &amp; Test</string>"));
        assert!(!plist.contains("<string>--allow-shell</string>"));
        Ok(())
    }
}
