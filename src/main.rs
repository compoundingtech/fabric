use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::IsTerminal,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::{CommandFactory, Parser, Subcommand};
use fabric::{
    config::{
        DEFAULT_EXEC_MAX_CHILDREN, FabricHome, GitAccess, PeerBook, generate_identity_file,
        load_or_create_identity, parse_addr_json, parse_node_id,
    },
    control::{ControlRequest, ControlResponse, PeerReachability},
    daemon::{
        DaemonOptions, FabricNode, init_daemon_tracing, run_daemon_with_options, send_control,
    },
    exec,
    service::{self, ServiceInstallOptions},
    shell::{self, ServerFrame},
    sync::config::{SyncBook, SyncEntry, SyncPeers, SyncPolicy},
    telemetry::{PeerTelemetry, TelemetryWindow},
    terminal::TerminalModeGuard,
    update,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Parser)]
#[command(name = "fabric")]
#[command(about = "Local socket facade for iroh-backed cross-machine transports")]
struct Cli {
    #[arg(long)]
    version: bool,

    #[arg(long, global = true)]
    home: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

/// The local certificate authority.
///
/// Separate verbs for install and uninstall, rather than a flag, because they
/// are different acts. A trust decision that cannot be undone in one command is
/// a trap rather than a decision.
#[derive(Debug, Subcommand)]
enum CaCommands {
    /// Create a certificate authority for this machine. Trusts nothing yet.
    Init,
    /// Make this machine trust certificates fabric signs.
    Install {
        /// Skip the prompt. For a script that has already shown it.
        #[arg(long)]
        yes: bool,
    },
    /// Stop trusting them. The reverse of install, and just as easy.
    Uninstall,
    /// Whether an authority exists, whether it is trusted, and its limits.
    Status,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Manage fabric identity key files.
    Key {
        #[command(subcommand)]
        command: KeyCommands,
    },
    /// Print this node's stable iroh NodeID.
    Id,
    /// Print the running daemon's current EndpointAddr as JSON.
    Addr,
    /// Show daemon state and echo-ping reachability for trusted peers.
    Status,
    /// List trusted peers and their service grants.
    Peers,
    /// Share Git repositories with exact per-peer read and write grants.
    Git {
        #[command(subcommand)]
        command: GitCommands,
    },
    /// Reload peers.toml into the running daemon.
    ReloadPeers,
    /// Trust a peer NodeID and optionally assign a local name.
    Add {
        nodeid: String,
        name: Option<String>,
        /// Optional EndpointAddr JSON hint for deterministic local/direct dialing.
        #[arg(long = "addr-json")]
        addr_json: Option<String>,
        /// Restrict this peer to these services, by the name a person types:
        /// shell, exec, sync, echo, or any protocol you expose such as web.
        ///
        /// Anything unlisted is refused, INCLUDING a service you expose later.
        /// Omit the flag to grant no services.
        ///
        /// A service is a name, not a port. The port belongs to whoever runs
        /// `fabric expose` and never crosses the wire.
        #[arg(long = "allow", value_delimiter = ',')]
        allow: Option<Vec<String>>,
    },
    /// Remove a trusted peer by NodeID or name.
    Remove { peer: String },
    /// Send one file to a peer's inbox. One shot, one direction, no deletes.
    SendFile {
        /// The peer's name or NodeID.
        peer: String,
        /// The local file to send.
        path: PathBuf,
        /// What to call it in the peer's inbox. Defaults to the file's own name.
        ///
        /// A relative path only. Everything lands under the receiving machine's
        /// inbox for this peer, so a sender cannot choose where files go.
        #[arg(long)]
        r#as: Option<String>,
    },
    /// Manage the local certificate authority for fabric names.
    Ca {
        #[command(subcommand)]
        command: CaCommands,
    },
    /// Issue a certificate for a fabric name, signed by the local authority.
    Cert {
        /// A name ending in .fabric, or localhost.
        name: String,
        /// Where to write the certificate. The key is written beside it.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Start the local fabric daemon.
    Up {
        /// Run in the foreground instead of spawning a background daemon.
        #[arg(long)]
        foreground: bool,
        /// Accepted for compatibility. peers.toml decides shell availability.
        #[arg(long)]
        allow_shell: bool,
        /// Accepted for compatibility. peers.toml decides exec availability.
        #[arg(long)]
        allow_exec: bool,
        /// Maximum total server-side tunnel sessions.
        #[arg(long)]
        server_session_max_total: Option<usize>,
        /// Maximum server-side tunnel sessions for one peer.
        #[arg(long)]
        server_session_max_per_peer: Option<usize>,
        /// Seconds to keep a detached server-side tunnel session for reconnect.
        #[arg(long)]
        server_session_detached_ttl_secs: Option<u64>,
    },
    /// Stop the local fabric daemon.
    Down,
    /// Restart the local fabric daemon through a detached helper.
    Restart {
        /// Accepted for compatibility. peers.toml decides shell availability.
        #[arg(long, conflicts_with = "no_allow_shell")]
        allow_shell: bool,
        /// Accepted for compatibility. peers.toml decides shell availability.
        #[arg(long)]
        no_allow_shell: bool,
    },
    /// Expose a local service to trusted peers under an ALPN protocol.
    Expose {
        protocol: String,
        /// Expose an existing local Unix socket service.
        #[arg(long, conflicts_with_all = ["exec", "tcp"])]
        socket: Option<PathBuf>,
        /// Expose an existing local TCP service.
        #[arg(long, conflicts_with_all = ["socket", "exec"])]
        tcp: Option<String>,
        /// Spawn a command per incoming fabric tunnel session and pipe stdio.
        #[arg(long, conflicts_with_all = ["socket", "tcp"])]
        exec: bool,
        /// Maximum active children for this exec exposure.
        #[arg(long)]
        max_children: Option<usize>,
        /// Do not write this exposure to config.toml.
        #[arg(long)]
        ephemeral: bool,
        /// Command argv for --exec. Use `--` before the command.
        #[arg(
            value_name = "CMD",
            trailing_var_arg = true,
            allow_hyphen_values = true
        )]
        command: Vec<String>,
    },
    /// Stop exposing a protocol and remove its persisted config entry.
    Unexpose { protocol: String },
    /// Create a local Unix socket that tunnels to a peer's exposed protocol.
    Dial {
        peer: String,
        protocol: String,
        /// Listen on a local TCP address instead of creating a Unix socket.
        #[arg(long)]
        tcp: Option<String>,
    },
    /// Say what is wrong with this machine, in words a stranger can act on.
    ///
    /// Reports each check as `ok`, `setup`, `problem`, or `unknown`. A check
    /// that could not establish an answer says `unknown` rather than `ok`, and
    /// that counts as needing attention: a doctor is read INSTEAD of
    /// investigating, so it must not guess in the reassuring direction.
    ///
    /// Exit code is the answer: 0 nothing to do, 1 something needs attention.
    /// It never changes anything.
    Doctor,
    /// Round-trip a random nonce through a peer's built-in echo protocol.
    Ping { peer: String },
    /// Test whether a peer serves one protocol, right now, with a single connect.
    ///
    /// Not a dial: it installs no listener, keeps no state, never retries, and
    /// never waits on the shared dial backoff. Exit code is the answer:
    /// 0 supported, 1 unsupported, 2 unreachable, 3 timeout.
    Probe {
        /// Peer name or node id.
        peer: String,
        /// Exact ALPN to test, for example fabric/shell/1 or pty-remote.
        protocol: String,
        /// Caller deadline in seconds.
        #[arg(long, default_value = "3")]
        timeout: f64,
        /// Emit one JSON object instead of a human line.
        #[arg(long)]
        json: bool,
    },
    /// Open an interactive remote shell on a trusted peer.
    Shell { peer: String },
    /// Run a command on a trusted peer non-interactively: stream its stdout and
    /// stderr back and exit with the remote command's exit code. The scriptable
    /// counterpart to `shell`, e.g. `fabric exec hetz -- ls -la`.
    Exec {
        /// The trusted peer to run the command on.
        peer: String,
        /// The command and its arguments (put `--` before it to end fabric's flags).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
        cmd: Vec<String>,
    },
    /// Manage declarative file-sync entries (syncs.toml).
    Sync {
        #[command(subcommand)]
        command: SyncCommands,
    },
    /// Install or remove fabric as a user-managed OS service.
    Service {
        #[command(subcommand)]
        command: ServiceCommands,
    },
    /// Verify a restart from outside the service's own cgroup, and put the
    /// previous binary back if the daemon does not come up. Scheduled by
    /// `fabric update`; not something to run by hand.
    #[command(hide = true)]
    SuperviseRestart {
        #[arg(long)]
        rollback: PathBuf,
        /// The version the daemon must report before this counts as a healthy
        /// restart. Checking that a socket answers is not enough: the old daemon
        /// is still answering until the moment it is torn down.
        #[arg(long)]
        expect: String,
    },
    /// Update this machine's fabric to a verified build, then restart it.
    ///
    /// Acts on this machine only. To sweep the fleet, compose it:
    /// `fabric exec <peer> -- fabric update`, one machine at a time.
    Update {
        /// Install a specific release tag instead of the latest.
        #[arg(long)]
        tag: Option<String>,
        /// Install an artifact from an explicit URL. `https://` or `file:///`,
        /// the latter for testing a build you made yourself. Requires --sha256.
        #[arg(long)]
        url: Option<String>,
        /// The SHA-256 the artifact at --url must have. Required with --url:
        /// fabric will not install bytes it cannot check against a hash you
        /// named. Note this proves the bytes are the ones you asked for, not
        /// that they are trustworthy.
        #[arg(long)]
        sha256: Option<String>,
        /// Report what is installed against what is available and change
        /// nothing. Exits 0 up to date, 1 update available, 2 error — an
        /// unreachable release server must not read as an available update.
        #[arg(long)]
        check: bool,
        /// Download, verify and stage, then stop without changing anything.
        #[arg(long)]
        dry_run: bool,
        /// Install without re-rendering the service or restarting it. The
        /// running daemon keeps using the old binary until it is restarted.
        #[arg(long)]
        no_restart: bool,
        /// Put the most recent rollback binary back and restart.
        #[arg(long, conflicts_with_all = ["tag", "url", "check", "dry_run", "allow_downgrade"])]
        rollback: bool,
        /// Permit a release that is older than or diverges from this build.
        #[arg(long)]
        allow_downgrade: bool,
    },
    /// Internal/debug commands for transport testing.
    #[command(hide = true)]
    Debug {
        #[command(subcommand)]
        command: DebugCommands,
    },
    /// Internal foreground daemon entrypoint.
    #[command(hide = true)]
    Daemon {
        #[arg(long)]
        allow_shell: bool,
        #[arg(long)]
        allow_exec: bool,
        #[arg(long)]
        server_session_max_total: Option<usize>,
        #[arg(long)]
        server_session_max_per_peer: Option<usize>,
        #[arg(long)]
        server_session_detached_ttl_secs: Option<u64>,
    },
    /// Internal restart detacher.
    #[command(hide = true)]
    RestartDetacher {
        #[arg(long)]
        allow_shell: bool,
    },
    /// Internal restart worker.
    #[command(hide = true)]
    RestartHelper {
        #[arg(long)]
        allow_shell: bool,
    },
}

#[derive(Debug, Subcommand)]
enum GitCommands {
    /// Install or repair the git-remote-fabric helper beside this binary.
    InstallHelper,
    /// Declare a local Git repository. This grants no peer access.
    Share { remote: String, repository: PathBuf },
    /// Remove a declaration and every peer grant for it.
    Unshare { remote: String },
    /// Add exact read or write access for one trusted peer.
    Grant {
        remote: String,
        peer: String,
        #[arg(long, conflicts_with_all = ["write", "read_write"])]
        read: bool,
        #[arg(long, conflicts_with_all = ["read", "read_write"])]
        write: bool,
        #[arg(long = "read-write", conflicts_with_all = ["read", "write"])]
        read_write: bool,
    },
    /// Remove exact read or write access from one trusted peer.
    Revoke {
        remote: String,
        peer: String,
        #[arg(long, conflicts_with_all = ["write", "all"])]
        read: bool,
        #[arg(long, conflicts_with_all = ["read", "all"])]
        write: bool,
        #[arg(long, conflicts_with_all = ["read", "write"])]
        all: bool,
    },
    /// List every local declaration and its effective grants.
    Ls,
    /// Check Git, the helper link, the daemon, and each declaration.
    Status,
}

#[derive(Debug, Subcommand)]
enum KeyCommands {
    /// Generate an identity file without starting a daemon.
    Gen {
        /// Path to write the identity file.
        #[arg(long)]
        out: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum ServiceCommands {
    /// Install and start a user service for the foreground daemon.
    Install {
        /// Accepted for compatibility. peers.toml decides shell availability.
        #[arg(long, conflicts_with = "no_allow_shell")]
        allow_shell: bool,
        /// Accepted for compatibility. peers.toml decides shell availability.
        #[arg(long)]
        no_allow_shell: bool,
        /// Accepted for compatibility. peers.toml decides exec availability.
        #[arg(long, conflicts_with = "no_allow_exec")]
        allow_exec: bool,
        /// Accepted for compatibility. peers.toml decides exec availability.
        #[arg(long)]
        no_allow_exec: bool,
        /// Memory ceiling applied by systemd/launchd, in MiB. Unset by default:
        /// a healthy working set depends on how much this node syncs, so Fabric
        /// declares no ceiling unless an operator measures one and asks for it.
        /// Once set it is remembered, so a later install that omits it keeps it.
        #[arg(long, conflicts_with = "no_memory_max_mb")]
        memory_max_mb: Option<u64>,
        /// Remove a previously persisted memory ceiling.
        #[arg(long)]
        no_memory_max_mb: bool,
    },
    /// Show native service-manager status.
    Status,
    /// Stop and remove only service-manager artifacts.
    Uninstall,
}

#[derive(Debug, Subcommand)]
enum SyncCommands {
    /// Add or update a sync entry in syncs.toml and reload the daemon.
    Add {
        /// Local folder to keep synced (absolute, or relative to the CWD).
        folder: String,
        /// Shared logical name — use the SAME name on every machine for this sync.
        #[arg(long)]
        name: String,
        /// Peers to sync with: "*" (all trusted) or comma-separated names/ids.
        #[arg(long, default_value = "*")]
        peers: String,
        /// Policy preset: catalog or bus.
        #[arg(long, default_value = "catalog")]
        policy: String,
        /// Optional comma-separated include globs (default: sync all files).
        #[arg(long)]
        include: Option<String>,
    },
    /// List configured sync entries and their live state.
    Ls {
        /// Emit a stable JSON array for scripts.
        #[arg(long)]
        json: bool,
    },
    /// Remove a sync entry by name or folder and reload the daemon.
    Rm { name_or_folder: String },
    /// Re-read syncs.toml into the running daemon (like reload-peers).
    Reload,
}

#[derive(Debug, Subcommand)]
enum DebugCommands {
    /// Close active generic tunnel iroh attaches without stopping the daemon.
    DropTunnels,
    /// Reject new generic tunnel attaches until unblocked.
    BlockTunnels,
    /// Allow new generic tunnel attaches again.
    UnblockTunnels,
    /// Reap complete or expired generic tunnel sessions.
    ReapTunnels {
        #[arg(long, default_value_t = 0)]
        ttl_ms: u64,
    },
    /// Rebuild the daemon's iroh endpoint in-process.
    RecycleEndpoint,
    /// Run a foreground Unix-socket echo service.
    Echo {
        #[arg(long)]
        socket: PathBuf,
    },
    /// Connect stdin/stdout to a Unix socket.
    UnixCat {
        #[arg(long)]
        socket: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    if fabric::gitremote::invoked_as_remote_helper() {
        let code = fabric::gitremote::run_remote_helper().await?;
        std::process::exit(code);
    }

    let cli = Cli::parse();
    if cli.version {
        println!("{}", fabric::version_string());
        return Ok(());
    }

    let Some(command) = cli.command else {
        Cli::command().print_help()?;
        println!();
        return Ok(());
    };

    match command {
        Commands::Key {
            command: KeyCommands::Gen { out },
        } => {
            let id = generate_identity_file(&out)?;
            println!("{id}");
        }
        command => {
            let home = FabricHome::resolve(cli.home)?;
            match command {
                Commands::Key { .. } => unreachable!("key commands are handled before home setup"),
                Commands::Id => {
                    let key = load_or_create_identity(&home)?;
                    println!("{}", key.public());
                }
                Commands::Addr => match send_control(&home, ControlRequest::Status).await? {
                    ControlResponse::Status { endpoint_addr, .. } => {
                        println!("{}", serde_json::to_string(&endpoint_addr)?);
                    }
                    response => bail!("unexpected daemon response: {response:?}"),
                },
                Commands::Status => {
                    match send_control(&home, ControlRequest::ReachabilityStatus).await? {
                        ControlResponse::ReachabilityStatus {
                            version,
                            node_id,
                            endpoint_addr,
                            exposed_protocols,
                            dial_sockets,
                            allow_shell,
                            allow_exec,
                            peers,
                            connection_telemetry,
                            connection_telemetry_window,
                            active_dial_handlers,
                            max_dial_handlers,
                        } => {
                            print_status(
                                &version,
                                &node_id,
                                &endpoint_addr,
                                &exposed_protocols,
                                &dial_sockets,
                                allow_shell,
                                allow_exec,
                                &peers,
                                &connection_telemetry,
                                &connection_telemetry_window,
                                (active_dial_handlers, max_dial_handlers),
                            )?;
                        }
                        response => bail!("unexpected daemon response: {response:?}"),
                    }
                }
                Commands::Peers => {
                    let book = PeerBook::load(&home)?;
                    println!(
                        "machine\tshell={}\texec={}",
                        if book.allow_shell() {
                            "allowed"
                        } else {
                            "disabled"
                        },
                        if book.allow_exec() {
                            "allowed"
                        } else {
                            "disabled"
                        }
                    );
                    for peer in book.peers() {
                        let name = peer.name.clone().unwrap_or_default();
                        let policy = if peer.allow.is_empty() {
                            "no services".to_string()
                        } else {
                            peer.allow.join(",")
                        };
                        println!("{}\t{}\t{}", peer.id, name, policy);
                    }
                }
                Commands::Git { command } => match command {
                    GitCommands::InstallHelper => {
                        let binary = std::env::current_exe()
                            .context("cannot resolve the running Fabric binary")?;
                        let helper = fabric::gitremote::install_helper_for(&binary)?;
                        println!("installed\t{}", helper.display());
                    }
                    GitCommands::Share { remote, repository } => {
                        let path = canonical_git_directory(&repository)?;
                        let mut book = PeerBook::load(&home)?;
                        book.share_git_remote(&remote, path.clone())?;
                        book.save(&home)?;
                        let _ = send_control(&home, ControlRequest::ReloadPeers).await;
                        println!("shared\t{remote}");
                        println!("path\t{}", path.display());
                        println!("access\tno peers");
                        println!("next\tfabric git grant {remote} <peer> --read");
                    }
                    GitCommands::Unshare { remote } => {
                        let mut book = PeerBook::load(&home)?;
                        book.unshare_git_remote(&remote)?;
                        book.save(&home)?;
                        let _ = send_control(&home, ControlRequest::ReloadPeers).await;
                        println!("unshared\t{remote}");
                        println!("grants removed\tall");
                    }
                    GitCommands::Grant {
                        remote,
                        peer,
                        read,
                        write,
                        read_write,
                    } => {
                        let accesses = git_accesses(read, write, read_write, false)?;
                        if accesses.contains(&GitAccess::Write) {
                            eprintln!(
                                "fabric: write access can update every ref that Git accepts and can run repository receive hooks"
                            );
                        }
                        let mut book = PeerBook::load(&home)?;
                        for access in &accesses {
                            book.grant_git_remote(&remote, &peer, *access)?;
                        }
                        book.save(&home)?;
                        let _ = send_control(&home, ControlRequest::ReloadPeers).await;
                        println!("granted\t{}", git_access_names(&accesses));
                        println!("remote\t{remote}");
                        println!("peer\t{peer}");
                    }
                    GitCommands::Revoke {
                        remote,
                        peer,
                        read,
                        write,
                        all,
                    } => {
                        let accesses = git_accesses(read, write, all, true)?;
                        let mut book = PeerBook::load(&home)?;
                        for access in &accesses {
                            book.revoke_git_remote(&remote, &peer, *access)?;
                        }
                        book.save(&home)?;
                        let _ = send_control(&home, ControlRequest::ReloadPeers).await;
                        println!("revoked\t{}", git_access_names(&accesses));
                        println!("remote\t{remote}");
                        println!("peer\t{peer}");
                    }
                    GitCommands::Ls => {
                        let book = PeerBook::load(&home)?;
                        print_git_remotes(&book);
                    }
                    GitCommands::Status => {
                        let book = PeerBook::load(&home)?;
                        print_git_status(&home, &book).await;
                    }
                },
                Commands::ReloadPeers => {
                    send_control(&home, ControlRequest::ReloadPeers).await?;
                    println!("reloaded");
                }
                Commands::Add {
                    nodeid,
                    name,
                    addr_json,
                    allow,
                } => {
                    let id = parse_node_id(&nodeid)?;
                    let addr = parse_addr_json(addr_json.as_deref(), id)?;
                    let mut book = PeerBook::load(&home)?;
                    warn_if_permissions_would_stop_a_sync(&home, &allow)?;
                    book.add_with_allow(id, name, addr, allow);
                    book.save(&home)?;
                    let _ = send_control(&home, ControlRequest::ReloadPeers).await;
                }
                Commands::SendFile { peer, path, r#as } => {
                    let name = match r#as {
                        Some(name) => name,
                        None => path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .map(str::to_string)
                            .context("that path has no file name; pass --as")?,
                    };
                    match send_control(&home, ControlRequest::SendFile { peer, path, name }).await?
                    {
                        ControlResponse::SentFile { peer, name, bytes } => {
                            println!("sent\t{bytes} bytes");
                            println!("to\t{peer}");
                            println!("as\t{name}");
                        }
                        other => bail!("unexpected response: {other:?}"),
                    }
                }
                Commands::Ca { command } => match command {
                    CaCommands::Init => {
                        if fabric::ca::ca_cert_path(&home).exists() {
                            bail!(
                                "an authority already exists at {}. Remove it by hand if you \
                                 mean to replace it; overwriting one silently would leave \
                                 every certificate it signed unverifiable",
                                fabric::ca::ca_cert_path(&home).display()
                            );
                        }
                        let authority = fabric::ca::generate(&fabric::ca::hostname())?;
                        fabric::ca::write(&home, &authority)?;
                        println!("authority\t{}", fabric::ca::ca_cert_path(&home).display());
                        println!("key\t{}", fabric::ca::ca_key_path(&home).display());
                        println!("limited to\t{} and 127.0.0.0/8", fabric::ca::NAME_SUFFIX);
                        println!("trusted\tno. run `fabric ca install` to trust it");
                    }
                    CaCommands::Install { yes } => {
                        print!("{}", fabric::ca::install_prompt(&home));
                        if !yes {
                            print!("\nInstall it? [y/N] ");
                            use std::io::Write as _;
                            std::io::stdout().flush()?;
                            let mut answer = String::new();
                            std::io::stdin().read_line(&mut answer)?;
                            if !matches!(answer.trim(), "y" | "Y" | "yes") {
                                println!("nothing was changed");
                                return Ok(());
                            }
                        }
                        fabric::ca::install(&home)?;
                        println!("trusted\tyes");
                        println!("undo\tfabric ca uninstall");
                    }
                    CaCommands::Uninstall => {
                        fabric::ca::uninstall(&home)?;
                        println!("trusted\tno");
                    }
                    CaCommands::Status => {
                        let cert = fabric::ca::ca_cert_path(&home);
                        if !cert.exists() {
                            println!("authority\tnone. run `fabric ca init`");
                            return Ok(());
                        }
                        println!("authority\t{}", cert.display());
                        println!("key\t{}", fabric::ca::ca_key_path(&home).display());
                        println!("limited to\t{} and 127.0.0.0/8", fabric::ca::NAME_SUFFIX);
                        match fabric::ca::is_installed(&home) {
                            Ok(true) => println!("trusted\tyes"),
                            Ok(false) => println!("trusted\tno"),
                            Err(error) => println!("trusted\tunknown: {error:#}"),
                        }
                    }
                },
                Commands::Cert { name, out } => {
                    let ca_cert = std::fs::read_to_string(fabric::ca::ca_cert_path(&home))
                        .with_context(|| {
                            format!(
                                "no authority at {}. Run `fabric ca init` first",
                                fabric::ca::ca_cert_path(&home).display()
                            )
                        })?;
                    let ca_key = std::fs::read_to_string(fabric::ca::ca_key_path(&home))?;
                    let leaf = fabric::ca::issue(&ca_cert, &ca_key, &name)?;
                    let cert_path = out.unwrap_or_else(|| PathBuf::from(format!("{name}.crt")));
                    let key_path = cert_path.with_extension("key");
                    std::fs::write(&cert_path, &leaf.cert_pem)?;
                    fabric::ca::write_private(&key_path, &leaf.key_pem)?;
                    println!("certificate\t{}", cert_path.display());
                    println!("key\t{}", key_path.display());
                }
                Commands::Remove { peer } => {
                    let mut book = PeerBook::load(&home)?;
                    if !book.remove(&peer) {
                        bail!("peer {peer:?} is not trusted");
                    }
                    book.save(&home)?;
                    let _ = send_control(&home, ControlRequest::ReloadPeers).await;
                }
                Commands::Up {
                    foreground,
                    allow_shell,
                    allow_exec,
                    server_session_max_total,
                    server_session_max_per_peer,
                    server_session_detached_ttl_secs,
                } => {
                    let options = daemon_options(
                        allow_shell,
                        allow_exec,
                        server_session_max_total,
                        server_session_max_per_peer,
                        server_session_detached_ttl_secs,
                    );
                    if foreground {
                        init_daemon_tracing(&home)?;
                        let node = FabricNode::start_with_daemon_options(home, options).await?;
                        let peers = node.state().peer_reachability().await;
                        print_startup_reachability(&peers);
                        node.wait().await?;
                    } else {
                        spawn_daemon(&home, options).await?;
                        print_daemon_reachability(&home).await?;
                    }
                }
                Commands::Down => {
                    if let Err(error) = send_control(&home, ControlRequest::Shutdown).await {
                        warn_home_daemon_mismatch(&home).await;
                        return Err(error);
                    }
                    println!("stopped");
                }
                Commands::Restart {
                    allow_shell,
                    no_allow_shell,
                } => {
                    let allow_shell = allow_override(allow_shell, no_allow_shell);
                    let response =
                        match send_control(&home, ControlRequest::Restart { allow_shell }).await {
                            Ok(response) => response,
                            Err(error) => {
                                warn_home_daemon_mismatch(&home).await;
                                return Err(error);
                            }
                        };
                    match response {
                        ControlResponse::Restarting { log, allow_shell } => {
                            println!("restart scheduled");
                            println!("log\t{}", log.display());
                            println!("allow-shell\t{allow_shell}");
                        }
                        response => bail!("unexpected daemon response: {response:?}"),
                    }
                }
                Commands::Expose {
                    protocol,
                    socket,
                    tcp,
                    exec,
                    max_children,
                    ephemeral,
                    command,
                } => {
                    let exposed_protocol = protocol.clone();
                    let request = expose_request(
                        protocol,
                        socket,
                        tcp,
                        exec,
                        max_children,
                        ephemeral,
                        command,
                    )?;
                    send_control(&home, request).await?;
                    if let Err(error) = warn_if_no_trusted_peer_can_reach(&home, &exposed_protocol)
                    {
                        eprintln!(
                            "fabric: exposure succeeded, but its peer permissions could not be checked: {error:#}"
                        );
                    }
                    println!("exposed");
                }
                Commands::Unexpose { protocol } => {
                    send_control(&home, ControlRequest::Unexpose { protocol }).await?;
                    println!("unexposed");
                }
                Commands::Dial {
                    peer,
                    protocol,
                    tcp,
                } => {
                    if let Some(bind) = tcp {
                        match send_control(
                            &home,
                            ControlRequest::DialTcp {
                                peer,
                                protocol,
                                bind,
                            },
                        )
                        .await?
                        {
                            ControlResponse::DialTcp { addr } => println!("{addr}"),
                            response => bail!("unexpected daemon response: {response:?}"),
                        }
                    } else {
                        match send_control(&home, ControlRequest::Dial { peer, protocol }).await? {
                            ControlResponse::Dial { socket } => println!("{}", socket.display()),
                            response => bail!("unexpected daemon response: {response:?}"),
                        }
                    }
                }
                Commands::Doctor => {
                    let facts = fabric::doctor::gather(&home, |request| {
                        let home = home.clone();
                        async move { send_control(&home, request).await }
                    })
                    .await;
                    let findings = fabric::doctor::diagnose(&facts);
                    std::process::exit(fabric::doctor::report(&facts, &findings));
                }
                Commands::Ping { peer } => {
                    match send_control(&home, ControlRequest::Ping { peer }).await? {
                        ControlResponse::Pong {
                            peer,
                            bytes,
                            round_trip_micros,
                            transport,
                        } => {
                            let millis = round_trip_micros as f64 / 1000.0;
                            match transport {
                                Some(transport) => {
                                    println!(
                                        "pong from {peer}: {bytes} bytes in {millis:.3} ms via {transport}"
                                    );
                                }
                                None => {
                                    println!("pong from {peer}: {bytes} bytes in {millis:.3} ms");
                                }
                            }
                        }
                        response => bail!("unexpected daemon response: {response:?}"),
                    }
                }
                Commands::Probe {
                    peer,
                    protocol,
                    timeout,
                    json,
                } => {
                    if !(timeout.is_finite() && timeout > 0.0) {
                        eprintln!("fabric: --timeout must be a positive number of seconds");
                        std::process::exit(PROBE_EXIT_UNANSWERABLE);
                    }
                    if protocol.is_empty() {
                        eprintln!("fabric: PROTOCOL must be a non-empty ALPN string");
                        std::process::exit(PROBE_EXIT_UNANSWERABLE);
                    }
                    let timeout_ms = ((timeout * 1000.0).round() as u64).max(1);
                    // Exit 1 means "the peer does not serve this protocol". A local
                    // failure -- no daemon, a daemon too old to know `probe`, an
                    // unknown peer name -- must never land on that code, or a
                    // caller cannot tell a real answer from a broken question.
                    let response = match send_control(
                        &home,
                        ControlRequest::Probe {
                            peer,
                            protocol,
                            timeout_ms,
                        },
                    )
                    .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            eprintln!("fabric: probe could not be answered: {error:#}");
                            std::process::exit(PROBE_EXIT_UNANSWERABLE);
                        }
                    };
                    match response {
                        ControlResponse::ProbeResult {
                            peer,
                            peer_id,
                            protocol,
                            outcome,
                            round_trip_micros,
                            transport,
                            error,
                        } => {
                            if json {
                                println!(
                                    "{}",
                                    serde_json::json!({
                                        "peer": peer,
                                        "peer_id": peer_id,
                                        "protocol": protocol,
                                        "outcome": outcome,
                                        "round_trip_micros": round_trip_micros,
                                        "transport": transport,
                                        "error": error,
                                    })
                                );
                            } else {
                                print_probe_line(
                                    &peer,
                                    &protocol,
                                    &outcome,
                                    round_trip_micros,
                                    transport.as_deref(),
                                    error.as_deref(),
                                );
                            }
                            // The exit code is the machine-readable answer, so a
                            // caller can branch without parsing anything.
                            std::process::exit(match outcome.as_str() {
                                "supported" => PROBE_EXIT_SUPPORTED,
                                "unsupported" => PROBE_EXIT_UNSUPPORTED,
                                "unreachable" => PROBE_EXIT_UNREACHABLE,
                                "timeout" => PROBE_EXIT_TIMEOUT,
                                _ => PROBE_EXIT_UNANSWERABLE,
                            });
                        }
                        response => {
                            eprintln!("fabric: unexpected daemon response: {response:?}");
                            std::process::exit(PROBE_EXIT_UNANSWERABLE);
                        }
                    }
                }
                Commands::Shell { peer } => {
                    let socket = match send_control(&home, ControlRequest::Shell { peer }).await? {
                        ControlResponse::Shell { socket } => socket,
                        response => bail!("unexpected daemon response: {response:?}"),
                    };
                    let code = run_shell_client(&socket).await?;
                    std::process::exit(code);
                }
                Commands::Exec { peer, cmd } => {
                    let socket = match send_control(&home, ControlRequest::Exec { peer }).await? {
                        ControlResponse::Exec { socket } => socket,
                        response => bail!("unexpected daemon response: {response:?}"),
                    };
                    let code = run_exec_client(&socket, &cmd).await?;
                    std::process::exit(code);
                }
                Commands::Sync { command } => run_sync(&home, command).await?,
                Commands::Service { command } => match command {
                    ServiceCommands::Install {
                        allow_shell,
                        no_allow_shell,
                        allow_exec,
                        no_allow_exec,
                        memory_max_mb,
                        no_memory_max_mb,
                    } => {
                        service::install(
                            &home,
                            ServiceInstallOptions {
                                allow_shell: allow_override(allow_shell, no_allow_shell),
                                allow_exec: allow_override(allow_exec, no_allow_exec),
                                memory_max_mb: memory_override(memory_max_mb, no_memory_max_mb),
                            },
                        )?;
                    }
                    ServiceCommands::Status => {
                        service::status()?;
                    }
                    ServiceCommands::Uninstall => {
                        service::uninstall()?;
                    }
                },
                Commands::SuperviseRestart { rollback, expect } => {
                    update::supervise_restart(&home, &rollback, &expect).await?;
                }
                Commands::Update {
                    tag,
                    url,
                    sha256,
                    check,
                    dry_run,
                    no_restart,
                    rollback,
                    allow_downgrade,
                } => {
                    let result = update::run(
                        &home,
                        update::UpdateOptions {
                            tag,
                            url,
                            sha256,
                            check,
                            dry_run,
                            no_restart,
                            rollback,
                            allow_downgrade,
                        },
                    )
                    .await;
                    match result {
                        Ok(0) => {}
                        Ok(code) => std::process::exit(code),
                        // A FAILURE WHILE CHECKING IS NOT AN AVAILABLE UPDATE.
                        // Letting the error propagate would exit 1, which is the
                        // code that means "there is a new version", so a fleet
                        // sweep would read an unreachable release server as
                        // work to do. It exits 2 instead.
                        Err(error) if check => {
                            eprintln!("Error: {error:?}");
                            std::process::exit(update::CHECK_EXIT_ERROR);
                        }
                        Err(error) => return Err(error),
                    }
                }
                Commands::Debug { command } => match command {
                    DebugCommands::DropTunnels => {
                        send_control(&home, ControlRequest::DropTunnelConnections).await?;
                        println!("dropped tunnel connections");
                    }
                    DebugCommands::BlockTunnels => {
                        send_control(&home, ControlRequest::SetTunnelBlocked { blocked: true })
                            .await?;
                        println!("blocked tunnel attaches");
                    }
                    DebugCommands::UnblockTunnels => {
                        send_control(&home, ControlRequest::SetTunnelBlocked { blocked: false })
                            .await?;
                        println!("unblocked tunnel attaches");
                    }
                    DebugCommands::ReapTunnels { ttl_ms } => {
                        send_control(
                            &home,
                            ControlRequest::ReapTunnelSessions { ttl_millis: ttl_ms },
                        )
                        .await?;
                        println!("reaped tunnel sessions");
                    }
                    DebugCommands::RecycleEndpoint => {
                        send_control(&home, ControlRequest::RecycleEndpoint).await?;
                        println!("recycled endpoint");
                    }
                    DebugCommands::Echo { socket } => {
                        run_debug_echo(socket).await?;
                    }
                    DebugCommands::UnixCat { socket } => {
                        run_debug_unix_cat(socket).await?;
                    }
                },
                Commands::Daemon {
                    allow_shell,
                    allow_exec,
                    server_session_max_total,
                    server_session_max_per_peer,
                    server_session_detached_ttl_secs,
                } => {
                    run_daemon_with_options(
                        home,
                        daemon_options(
                            allow_shell,
                            allow_exec,
                            server_session_max_total,
                            server_session_max_per_peer,
                            server_session_detached_ttl_secs,
                        ),
                    )
                    .await?;
                }
                Commands::RestartDetacher { allow_shell } => {
                    run_restart_detacher(&home, allow_shell)?;
                }
                Commands::RestartHelper { allow_shell } => {
                    run_restart_helper(&home, allow_shell).await?;
                }
            }
        }
    }

    Ok(())
}

fn expose_request(
    protocol: String,
    socket: Option<PathBuf>,
    tcp: Option<String>,
    exec: bool,
    max_children: Option<usize>,
    ephemeral: bool,
    command: Vec<String>,
) -> Result<ControlRequest> {
    let persist = !ephemeral;
    if exec {
        if command.is_empty() {
            bail!("--exec requires a command: fabric expose {protocol} --exec -- <cmd> [args...]");
        }
        let max_children = max_children.unwrap_or(DEFAULT_EXEC_MAX_CHILDREN);
        if max_children == 0 {
            bail!("--max-children must be greater than zero");
        }
        return Ok(ControlRequest::ExposeExec {
            protocol,
            argv: command,
            max_children,
            persist,
        });
    }

    if max_children.is_some() {
        bail!("--max-children requires --exec");
    }

    if !command.is_empty() {
        bail!("command arguments require --exec");
    }

    if let Some(addr) = tcp {
        return Ok(ControlRequest::ExposeTcp {
            protocol,
            addr,
            persist,
        });
    }

    let Some(socket) = socket else {
        bail!("expose requires --socket <path>, --tcp <host:port>, or --exec -- <cmd> [args...]");
    };
    Ok(ControlRequest::Expose {
        protocol,
        socket,
        persist,
    })
}

async fn run_sync(home: &FabricHome, command: SyncCommands) -> Result<()> {
    match command {
        SyncCommands::Add {
            folder,
            name,
            peers,
            policy,
            include,
        } => {
            let folder = absolutize(&folder)?;
            let entry = SyncEntry {
                name: name.clone(),
                folder,
                peers: parse_sync_peers(&peers),
                policy: parse_sync_policy(&policy)?,
                include: parse_include(include.as_deref()),
            };
            let mut book = SyncBook::load(home)?;
            book.upsert(entry);
            book.save(home)?;
            // Apply live if the daemon is running; harmless if it is not.
            let _ = send_control(home, ControlRequest::SyncReload).await;
            println!("sync {name:?} written to {}", home.syncs_path().display());
        }
        SyncCommands::Ls { json } => match send_control(home, ControlRequest::SyncStatus).await? {
            ControlResponse::SyncStatus { entries } => {
                if json {
                    let entries: Vec<_> = entries.iter().map(SyncLsJsonEntry::from).collect();
                    println!("{}", serde_json::to_string_pretty(&entries)?);
                    return Ok(());
                }
                if entries.is_empty() {
                    println!("no sync entries");
                }
                for entry in entries {
                    let present = logical_present(&entry);
                    if entry.missing == 0
                        && entry.unexpected == 0
                        && entry.mismatched == 0
                        && entry.scan_issues.is_empty()
                    {
                        println!(
                            "{}\t{}\t{}\tpeers={}\tpresent={present}\ttombstones={}\tobserved={}\tdrift=clean\tscan_issues=none\tstopped={}\tsync_passes={}\tfull_scans={}\tinbound_noop_transactions={}\tinbound_guarded_transactions={}\tscan_ms={}\tmaterialize_ms={}\tpersist_ms={}\treconcile_ms={}\treconcile_wire_bytes={}\treconcile_failures={}\tsweep={}\tdelta_fallbacks={}\tfull_payload_sends={}\tcontent_bytes={}\tdigest={}",
                            entry.name,
                            entry.folder,
                            entry.policy,
                            entry.peers,
                            entry.tombstones,
                            entry.observed,
                            stopped_token(&entry),
                            entry.sync_passes,
                            entry.full_scans,
                            entry.inbound_noop_transactions,
                            entry.inbound_guarded_transactions,
                            entry.scan_micros / 1000,
                            entry.materialize_micros / 1000,
                            entry.persist_micros / 1000,
                            entry.reconcile_micros / 1000,
                            entry.reconcile_wire_bytes,
                            entry.reconcile_failures,
                            sweep_token(&entry),
                            entry.delta_fallbacks,
                            entry.full_payload_sends,
                            entry.content_bytes,
                            short_digest(&entry.digest),
                        );
                    } else {
                        println!(
                            "{}\t{}\t{}\tpeers={}\tpresent={present}\ttombstones={}\tobserved={}\tdrift=WARNING missing={} unexpected={} mismatched={}\tscan_issues={}\tstopped={}\tsync_passes={}\tfull_scans={}\tinbound_noop_transactions={}\tinbound_guarded_transactions={}\tscan_ms={}\tmaterialize_ms={}\tpersist_ms={}\treconcile_ms={}\treconcile_wire_bytes={}\treconcile_failures={}\tsweep={}\tdelta_fallbacks={}\tfull_payload_sends={}\tcontent_bytes={}\tdigest={}",
                            entry.name,
                            entry.folder,
                            entry.policy,
                            entry.peers,
                            entry.tombstones,
                            entry.observed,
                            entry.missing,
                            entry.unexpected,
                            entry.mismatched,
                            scan_issues_token(&entry),
                            stopped_token(&entry),
                            entry.sync_passes,
                            entry.full_scans,
                            entry.inbound_noop_transactions,
                            entry.inbound_guarded_transactions,
                            entry.scan_micros / 1000,
                            entry.materialize_micros / 1000,
                            entry.persist_micros / 1000,
                            entry.reconcile_micros / 1000,
                            entry.reconcile_wire_bytes,
                            entry.reconcile_failures,
                            sweep_token(&entry),
                            entry.delta_fallbacks,
                            entry.full_payload_sends,
                            entry.content_bytes,
                            short_digest(&entry.digest),
                        );
                    }
                }
            }
            response => bail!("unexpected daemon response: {response:?}"),
        },
        SyncCommands::Rm { name_or_folder } => {
            let mut book = SyncBook::load(home)?;
            if !book.remove(&name_or_folder) {
                bail!("no sync entry named or foldered {name_or_folder:?}");
            }
            book.save(home)?;
            let _ = send_control(home, ControlRequest::SyncReload).await;
            println!("removed sync {name_or_folder:?}");
        }
        SyncCommands::Reload => {
            send_control(home, ControlRequest::SyncReload).await?;
            println!("reloaded");
        }
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct SyncLsJsonEntry<'a> {
    name: &'a str,
    folder: &'a str,
    policy: &'a str,
    peers: &'a str,
    present: usize,
    tombstones: usize,
    observed: usize,
    drift: bool,
    missing: usize,
    unexpected: usize,
    mismatched: usize,
    scan_issues: &'a [(String, String)],
    /// Calls to `sync_once`. NOT `full_scans`, which is two per call.
    sync_passes: u64,
    full_scans: u64,
    inbound_noop_transactions: u64,
    inbound_guarded_transactions: u64,
    /// Cumulative microseconds per phase of `sync_once`. Two samples and a
    /// division describe the present; a total on its own describes the past.
    scan_micros: u64,
    materialize_micros: u64,
    persist_micros: u64,
    reconcile_micros: u64,
    reconcile_wire_bytes: u64,
    reconcile_failures: u64,
    /// Why the tombstone sweep did or did not forget anything. `unknown` from a
    /// daemon that predates the field.
    sweep: &'a str,
    /// Peers this entry is NOT syncing with, and why. Empty is healthy.
    stopped_peers: Vec<String>,
    /// Payloads this node SENT carrying its whole manifest, whatever the reason.
    /// High `reconcile_wire_bytes` with a low count here means this machine is
    /// RECEIVING full payloads rather than sending them.
    full_payload_sends: u64,
    /// Bytes of file content the daemon holds in memory for this entry.
    content_bytes: u64,
    /// Reconciles that fell back to full state. Zero is healthy; a RISING
    /// number means a cursor described state a peer did not hold.
    delta_fallbacks: u64,
    /// Lattice-point fingerprint of this entry's manifest. Compare it ACROSS
    /// peers: equal means converged, unequal means diverged. `present` and
    /// `tombstones` can match while the state differs, so they cannot answer
    /// this. Empty from a daemon that predates the field.
    digest: &'a str,
}

impl<'a> From<&'a fabric::control::SyncEntryStatus> for SyncLsJsonEntry<'a> {
    fn from(entry: &'a fabric::control::SyncEntryStatus) -> Self {
        Self {
            name: &entry.name,
            folder: &entry.folder,
            policy: &entry.policy,
            peers: &entry.peers,
            present: logical_present(entry),
            tombstones: entry.tombstones,
            observed: entry.observed,
            drift: entry.missing != 0
                || entry.unexpected != 0
                || entry.mismatched != 0
                || !entry.scan_issues.is_empty(),
            missing: entry.missing,
            unexpected: entry.unexpected,
            mismatched: entry.mismatched,
            scan_issues: &entry.scan_issues,
            stopped_peers: entry
                .stopped_peers
                .iter()
                .map(|(peer, reason)| format!("{peer}:{reason}"))
                .collect(),
            full_payload_sends: entry.full_payload_sends,
            content_bytes: entry.content_bytes,
            delta_fallbacks: entry.delta_fallbacks,
            digest: &entry.digest,
            sync_passes: entry.sync_passes,
            full_scans: entry.full_scans,
            inbound_noop_transactions: entry.inbound_noop_transactions,
            inbound_guarded_transactions: entry.inbound_guarded_transactions,
            scan_micros: entry.scan_micros,
            materialize_micros: entry.materialize_micros,
            persist_micros: entry.persist_micros,
            reconcile_micros: entry.reconcile_micros,
            reconcile_wire_bytes: entry.reconcile_wire_bytes,
            reconcile_failures: entry.reconcile_failures,
            sweep: sweep_token(entry),
        }
    }
}

fn canonical_git_directory(repository: &PathBuf) -> Result<PathBuf> {
    let output = ProcessCommand::new("git")
        .arg("-C")
        .arg(repository)
        .args(["rev-parse", "--absolute-git-dir"])
        .output()
        .context("failed to run Git; install Git before sharing a repository")?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{} is not a Git repository: {}",
            repository.display(),
            detail.trim()
        );
    }
    let raw = String::from_utf8(output.stdout).context("Git returned a non-UTF-8 directory")?;
    let path = PathBuf::from(raw.trim());
    fs::canonicalize(&path)
        .with_context(|| format!("failed to resolve Git directory {}", path.display()))
}

fn git_accesses(read: bool, write: bool, both: bool, revoke: bool) -> Result<Vec<GitAccess>> {
    match (read, write, both) {
        (true, false, false) => Ok(vec![GitAccess::Read]),
        (false, true, false) => Ok(vec![GitAccess::Write]),
        (false, false, true) => Ok(vec![GitAccess::Read, GitAccess::Write]),
        _ if revoke => bail!("choose exactly one of --read, --write, or --all"),
        _ => bail!("choose exactly one of --read, --write, or --read-write"),
    }
}

fn git_access_names(accesses: &[GitAccess]) -> &'static str {
    match accesses {
        [GitAccess::Read] => "read",
        [GitAccess::Write] => "write",
        _ => "read,write",
    }
}

fn git_grant_labels(book: &PeerBook, remote: &str, access: GitAccess) -> Vec<String> {
    let permission = access.permission(remote);
    let mut labels = book
        .peers()
        .iter()
        .filter(|peer| peer.allow.contains(&permission))
        .map(|peer| peer.name.clone().unwrap_or_else(|| peer.id.to_string()))
        .collect::<Vec<_>>();
    labels.sort();
    labels
}

fn git_repository_kind(path: &PathBuf) -> &'static str {
    let output = ProcessCommand::new("git")
        .arg("--git-dir")
        .arg(path)
        .args(["rev-parse", "--is-bare-repository"])
        .output();
    match output {
        Ok(output) if output.status.success() && output.stdout.starts_with(b"true") => "bare",
        Ok(output) if output.status.success() => "worktree",
        _ => "unavailable",
    }
}

fn comma_list(values: Vec<String>) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values.join(",")
    }
}

fn print_git_remotes(book: &PeerBook) {
    for remote in book.git_remotes() {
        println!(
            "{}\t{}\t{}\tread={}\twrite={}",
            remote.name,
            remote.path.display(),
            git_repository_kind(&remote.path),
            comma_list(git_grant_labels(book, &remote.name, GitAccess::Read)),
            comma_list(git_grant_labels(book, &remote.name, GitAccess::Write)),
        );
    }
}

async fn print_git_status(home: &FabricHome, book: &PeerBook) {
    match ProcessCommand::new("git").arg("--version").output() {
        Ok(output) if output.status.success() => {
            println!(
                "git\tok\t{}",
                String::from_utf8_lossy(&output.stdout).trim()
            )
        }
        _ => println!("git\tproblem\tGit is not available"),
    }

    match std::env::current_exe() {
        Ok(binary) => {
            let helper = fabric::gitremote::helper_path_for(&binary);
            match (helper, fabric::gitremote::helper_is_installed_for(&binary)) {
                (Ok(path), Ok(true)) => println!("helper\tok\t{}", path.display()),
                (Ok(path), Ok(false)) => println!(
                    "helper\tproblem\tmissing or unrelated {}; run fabric git install-helper",
                    path.display()
                ),
                (_, Err(error)) => println!("helper\tproblem\t{error:#}"),
                (Err(error), _) => println!("helper\tunknown\t{error:#}"),
            }
        }
        Err(error) => println!("helper\tunknown\tcannot resolve the Fabric binary: {error}"),
    }

    match send_control(home, ControlRequest::Status).await {
        Ok(_) => println!("daemon\tok\trunning"),
        Err(error) => println!("daemon\tproblem\t{error:#}"),
    }

    println!(
        "configuration\tok\t{} Git remotes",
        book.git_remotes().len()
    );
    for remote in book.git_remotes() {
        let kind = git_repository_kind(&remote.path);
        let verdict = if kind == "unavailable" {
            "problem"
        } else {
            "ok"
        };
        println!(
            "remote {}\t{}\t{} ({kind})",
            remote.name,
            verdict,
            remote.path.display()
        );
    }
}

/// Say so BEFORE writing a permission that would stop a sync entry.
///
/// Every other signal about a denied sync arrives after the mistake and only
/// for somebody looking: a counter someone reads, a line in `sync ls` someone
/// runs. None of them wake anyone. The moment a person's hands are on the
/// keyboard is the only moment this information is free.
fn warn_if_permissions_would_stop_a_sync(
    home: &fabric::config::FabricHome,
    allow: &Option<Vec<String>>,
) -> Result<()> {
    let allow = allow.as_deref().unwrap_or_default();
    if allow.iter().any(|service| service == "sync") {
        return Ok(());
    }
    let configured = fabric::sync::SyncBook::load(home)
        .map(|book| book.entries().len())
        .unwrap_or(0);
    if configured == 0 {
        return Ok(());
    }
    eprintln!(
        "fabric: this peer will NOT be permitted to sync, and {configured} sync \
         entr{} configured on this machine.",
        if configured == 1 { "y is" } else { "ies are" }
    );
    eprintln!(
        "fabric: a denied sync does not fail loudly. The two machines simply \
         stop converging."
    );
    eprintln!("fabric: add `sync` to --allow if that is not what you meant.");
    Ok(())
}

/// Return every peer that needs `service` added when nobody can reach it.
fn peers_needing_new_service(book: &PeerBook, service: &str) -> Option<Vec<String>> {
    if book.peers().is_empty() {
        return None;
    }
    let mut denied = book
        .peers()
        .iter()
        .filter(|peer| book.may(&peer.id, service).is_err())
        .map(|peer| peer.name.clone().unwrap_or_else(|| peer.id.to_string()))
        .collect::<Vec<_>>();
    if denied.len() != book.peers().len() {
        return None;
    }
    denied.sort();
    Some(denied)
}

/// Warn after an exposure succeeds when its ACL makes it unreachable.
fn warn_if_no_trusted_peer_can_reach(home: &FabricHome, service: &str) -> Result<()> {
    let book = PeerBook::load(home)?;
    let Some(peers) = peers_needing_new_service(&book, service) else {
        return Ok(());
    };
    eprintln!(
        "fabric: no trusted peer may reach {service:?}; add it to allow for: {}",
        peers.join(", ")
    );
    Ok(())
}

#[cfg(test)]
mod permission_helpers_tests {
    use super::*;

    fn peer_book(first_allow: &[&str]) -> PeerBook {
        let mut book = PeerBook::default();
        let first = iroh::SecretKey::generate().public();
        let second = iroh::SecretKey::generate().public();
        book.add_with_allow(
            first,
            Some("droppy".into()),
            None,
            Some(
                first_allow
                    .iter()
                    .map(|service| (*service).into())
                    .collect(),
            ),
        );
        book.add_with_allow(second, Some("hetz".into()), None, Some(vec!["sync".into()]));
        book
    }

    #[test]
    fn a_new_exposure_warns_only_when_every_peer_is_denied() {
        assert_eq!(
            peers_needing_new_service(&peer_book(&[]), "web"),
            Some(vec!["droppy".into(), "hetz".into()])
        );
        assert_eq!(
            peers_needing_new_service(&peer_book(&["web"]), "web"),
            None,
            "one peer can reach the exposure, so the warning would be false"
        );
    }
}

/// The first 12 characters of the lattice-point digest, which is enough to
/// compare two machines by eye. Scripts should read the full value from
/// `sync ls --json` rather than this.
fn short_digest(digest: &str) -> &str {
    if digest.is_empty() {
        return "unknown";
    }
    &digest[..digest.len().min(12)]
}

/// Which peers this entry has stopped syncing with, and why.
///
/// `none` when everything is healthy. A partial stop names the peer, because an
/// entry that converges with two machines and is cut off from a third is not a
/// healthy entry and an entry-wide flag would call it one.
fn stopped_token(entry: &fabric::control::SyncEntryStatus) -> String {
    if entry.stopped_peers.is_empty() {
        return "none".to_string();
    }
    entry
        .stopped_peers
        .iter()
        .map(|(peer, reason)| format!("{peer}:{reason}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn scan_issues_token(entry: &fabric::control::SyncEntryStatus) -> String {
    if entry.scan_issues.is_empty() {
        return "none".to_string();
    }
    entry
        .scan_issues
        .iter()
        .map(|(path, reason)| format!("{path}:{reason}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// The sweep reason, or a placeholder when the daemon has not decided one yet.
///
/// An older daemon sends nothing here, so this must not render an empty string
/// as if it were a state.
fn sweep_token(entry: &fabric::control::SyncEntryStatus) -> &str {
    if entry.sweep.is_empty() {
        "unknown"
    } else {
        &entry.sweep
    }
}

fn logical_present(entry: &fabric::control::SyncEntryStatus) -> usize {
    if entry.present == 0 {
        entry.files
    } else {
        entry.present
    }
}

#[cfg(test)]
mod connection_telemetry_tests {
    use super::*;
    use fabric::telemetry::LatencySummary;

    #[test]
    fn an_absent_roaming_peer_is_away_not_unreachable() {
        let peer = PeerReachability {
            id: "node-id".to_string(),
            name: Some("bluey".to_string()),
            roaming: true,
            reachable: false,
            bytes: None,
            round_trip_micros: None,
            transport: None,
            error: Some("timed out".to_string()),
        };

        assert_eq!(
            format_peer_reachability(&peer),
            "bluey\tnode-id\taway\troaming peer"
        );
    }

    #[test]
    fn an_old_status_without_roaming_keeps_unreachable_behavior() {
        let peer: PeerReachability = serde_json::from_value(serde_json::json!({
            "id": "node-id",
            "name": "server",
            "reachable": false,
            "bytes": null,
            "round_trip_micros": null,
            "transport": null,
            "error": "timed out"
        }))
        .unwrap();

        assert_eq!(
            format_peer_reachability(&peer),
            "server\tnode-id\tunreachable\ttimed out"
        );
    }

    fn current(peers: &[&str]) -> BTreeSet<String> {
        peers.iter().map(|peer| (*peer).to_string()).collect()
    }

    fn test_window() -> TelemetryWindow {
        TelemetryWindow {
            started_unix_seconds: Some(0),
            reset_reason: None,
        }
    }

    fn peer_with_losses() -> PeerTelemetry {
        let mut reconnect = LatencySummary::default();
        reconnect.record(1_500_000);
        reconnect.record(1_900_000);
        reconnect.record(4_500_000);
        reconnect.record(1_800_000);
        PeerTelemetry {
            losses: 4,
            resumes: 4,
            resume_failures: 0,
            reconnect_attempts: 7,
            reconnect,
            losses_by_path: BTreeMap::from([("direct".to_string(), 3), ("relay".to_string(), 1)]),
            resumes_by_path: BTreeMap::from([("direct".to_string(), 2), ("relay".to_string(), 2)]),
            ..PeerTelemetry::default()
        }
    }

    fn probed_peer(direct: &[u64], relay: &[u64]) -> PeerTelemetry {
        let mut probe_latency = BTreeMap::new();
        for (path, samples) in [("direct", direct), ("relay", relay)] {
            if samples.is_empty() {
                continue;
            }
            let mut latency = LatencySummary::default();
            for micros in samples {
                latency.record(*micros);
            }
            probe_latency.insert(path.to_string(), latency);
        }
        PeerTelemetry {
            probes_reachable: (direct.len() + relay.len()) as u64,
            probe_latency,
            ..PeerTelemetry::default()
        }
    }

    /// A healthy peer must show its paths. This is the whole point.
    ///
    /// The sessions block keys off losses, so a peer that has never dropped
    /// prints nothing there. Healthy is the NORMAL state, so keying this block
    /// the same way would blank exactly the peers an operator looks at most, and
    /// hide the path evidence on every one of them.
    #[test]
    fn a_peer_with_probes_and_no_losses_still_shows_its_paths() {
        let peer = probed_peer(&[80_000, 90_000], &[60_000, 64_000, 66_000]);
        assert_eq!(peer.losses, 0, "this fixture must be the healthy case");
        let lines = path_latency_lines(
            &BTreeMap::from([("droppy".to_string(), peer)]),
            &current(&["droppy"]),
        );

        assert_eq!(lines[0], "paths");
        assert!(
            lines.iter().any(|line| line.contains("droppy")),
            "a peer with no losses must not vanish: {lines:?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("direct")),
            "its direct path must be reported: {lines:?}"
        );
        assert!(
            lines.iter().any(|line| line.contains("relay")),
            "its relay path must be reported: {lines:?}"
        );
    }

    /// The busiest path comes first, because which path a peer actually spends
    /// its time on is the finding rather than a detail.
    #[test]
    fn the_busiest_path_is_listed_first() {
        let peer = probed_peer(&[80_000], &[60_000, 61_000, 62_000]);
        let lines = path_latency_lines(
            &BTreeMap::from([("droppy".to_string(), peer)]),
            &current(&["droppy"]),
        );
        let relay = lines.iter().position(|l| l.contains("relay")).unwrap();
        let direct = lines.iter().position(|l| l.contains("direct")).unwrap();
        assert!(relay < direct, "relay carried 3 of 4 probes: {lines:?}");
        assert!(lines[relay].contains("75%"), "{}", lines[relay]);
        assert!(lines[direct].contains("25%"), "{}", lines[direct]);
    }

    /// Mean and max are exact; bucketed percentiles are not.
    ///
    /// This shipped briefly reporting p50/p90 from the histogram. On live data
    /// direct and relay both printed `p50=100.0ms p90=200.0ms` while their means
    /// differed and their maxima differed by more than 2x, because the bucket
    /// bounds double and both distributions fell in the same bucket. The display
    /// hid the very difference it exists to show. Pin the exact values so nobody
    /// swaps them back for percentiles that look more precise.
    #[test]
    fn the_reported_latency_is_exact_not_bucketed() {
        // 40ms and 680ms sit in different buckets; their mean, 360ms, sits in
        // neither, so a bucketed statistic could not produce this number.
        let peer = probed_peer(&[40_000, 680_000], &[]);
        let lines = path_latency_lines(
            &BTreeMap::from([("droppy".to_string(), peer)]),
            &current(&["droppy"]),
        );
        let direct = lines.iter().find(|l| l.contains("direct")).unwrap();
        assert!(
            direct.contains("mean=360.0ms"),
            "mean must be the exact average, got {direct}"
        );
        assert!(
            direct.contains("max=680.0ms"),
            "max must be the exact largest sample, got {direct}"
        );
        assert!(
            !direct.contains("p50") && !direct.contains("p90"),
            "bucketed percentiles collapse distinct paths together: {direct}"
        );
    }

    /// A peer that never answered still has something worth reporting.
    #[test]
    fn an_unreachable_peer_reports_its_reachability_rather_than_vanishing() {
        let peer = PeerTelemetry {
            probes_reachable: 9,
            probes_unreachable: 243,
            ..PeerTelemetry::default()
        };
        let lines = path_latency_lines(
            &BTreeMap::from([("bluey".to_string(), peer)]),
            &current(&["bluey"]),
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("bluey") && l.contains("reachable 9/252")),
            "an unreachable peer must still be listed: {lines:?}"
        );
    }

    #[test]
    fn no_probes_at_all_says_so_rather_than_printing_an_empty_heading() {
        assert_eq!(
            path_latency_lines(&BTreeMap::new(), &BTreeSet::new()),
            vec!["paths\tno probes recorded".to_string()]
        );
    }

    /// The README shows this shape and tells an operator how to read it, so a
    /// silent rename here would make the documentation wrong.
    #[test]
    fn the_rendered_shape_matches_the_documented_one() {
        let telemetry = BTreeMap::from([("hetz".to_string(), peer_with_losses())]);
        let lines = connection_telemetry_lines(&telemetry, &test_window(), &current(&["hetz"]));
        assert_eq!(lines[0], "sessions\tsince 1970-01-01T00:00:00Z");
        assert_eq!(
            lines[1],
            // p50 is a bucket bound, because a histogram cannot report better
            // than its bucket. The max is the exact largest sample seen.
            "  hetz\tlost=4 resumed=4 failed=0 attempts=7 reconnect_p50=2.0s reconnect_max=4.5s"
        );
        assert_eq!(
            lines[2],
            "    lost_on=direct=3,relay=1 resumed_on=direct=2,relay=2"
        );
    }

    /// A peer that never dropped must not appear, or a healthy mesh reads as a
    /// wall of zeroes and the peers that did drop stop standing out.
    #[test]
    fn a_peer_with_no_loss_is_omitted() {
        let telemetry = BTreeMap::from([
            ("quiet".to_string(), PeerTelemetry::default()),
            ("hetz".to_string(), peer_with_losses()),
        ]);
        let lines = connection_telemetry_lines(&telemetry, &test_window(), &current(&["hetz"]));
        assert!(lines.iter().all(|line| !line.contains("quiet")));
        assert!(lines.iter().any(|line| line.contains("hetz")));
    }

    #[test]
    fn no_losses_at_all_says_so_rather_than_printing_an_empty_heading() {
        let lines = connection_telemetry_lines(&BTreeMap::new(), &test_window(), &BTreeSet::new());
        assert_eq!(
            lines,
            vec![
                "sessions\tsince 1970-01-01T00:00:00Z".to_string(),
                "  no losses recorded".to_string(),
            ]
        );
    }

    /// A loss that never came back has no duration, and a dash is honest where
    /// `0.0s` would read as an instant recovery.
    #[test]
    fn an_unfinished_reconnect_reports_a_dash_not_zero() {
        let telemetry = BTreeMap::from([(
            "bluey".to_string(),
            PeerTelemetry {
                losses: 1,
                resume_failures: 1,
                reconnect_attempts: 3,
                ..PeerTelemetry::default()
            },
        )]);
        let lines = connection_telemetry_lines(&telemetry, &test_window(), &current(&["bluey"]));
        assert!(
            lines[1].contains("reconnect_p50=- reconnect_max=-"),
            "unexpected line: {}",
            lines[1]
        );
    }

    #[test]
    fn a_removed_peer_is_marked_in_every_durable_telemetry_block() {
        let mut peer = peer_with_losses();
        let mut latency = LatencySummary::default();
        latency.record(64_000);
        peer.probes_reachable = 1;
        peer.probe_latency.insert("relay".to_string(), latency);
        let telemetry = BTreeMap::from([("droppy".to_string(), peer)]);
        let current = current(&["hetz"]);

        let sessions = connection_telemetry_lines(&telemetry, &test_window(), &current);
        assert!(
            sessions
                .iter()
                .any(|line| line.contains("droppy [not in peers.toml]")),
            "a durable session total must not look current: {sessions:?}"
        );

        let paths = path_latency_lines(&telemetry, &current);
        assert!(
            paths
                .iter()
                .any(|line| line.contains("droppy [not in peers.toml]")),
            "durable path totals need the same roster context: {paths:?}"
        );
    }

    #[test]
    fn an_unknown_legacy_window_says_why_it_is_unknown() {
        let line = telemetry_window_line(&TelemetryWindow {
            started_unix_seconds: None,
            reset_reason: Some(
                "window start unknown: snapshot predates window tracking".to_string(),
            ),
        });
        assert_eq!(
            line,
            "sessions\twindow start unknown: snapshot predates window tracking"
        );
    }
}

#[cfg(test)]
mod sync_ls_tests {
    use super::*;
    use fabric::control::SyncEntryStatus;

    fn status() -> SyncEntryStatus {
        SyncEntryStatus {
            delta_fallbacks: 0,
            full_payload_sends: 0,
            content_bytes: 0,
            stopped_peers: vec![("hetz".into(), "denied".into())],
            digest: "lattice-point-aaaa".into(),
            name: "catalog".to_string(),
            folder: "/catalog".to_string(),
            policy: "catalog".to_string(),
            peers: "*".to_string(),
            files: 40,
            present: 40,
            tombstones: 3,
            observed: 42,
            missing: 0,
            unexpected: 2,
            mismatched: 0,
            scan_issues: vec![("large.bin".into(), "too-large".into())],
            sync_passes: 9,
            full_scans: 17,
            inbound_noop_transactions: 11,
            inbound_guarded_transactions: 3,
            scan_micros: 1_500,
            materialize_micros: 2_500,
            persist_micros: 3_500,
            reconcile_micros: 4_500,
            reconcile_wire_bytes: 11_000_000,
            reconcile_failures: 3,
            sweep: "disabled".to_string(),
        }
    }

    #[test]
    fn sync_ls_json_schema_exposes_counts_and_drift() {
        let status = status();
        let json = serde_json::to_value(SyncLsJsonEntry::from(&status)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "name": "catalog",
                "folder": "/catalog",
                "policy": "catalog",
                "peers": "*",
                "present": 40,
                "tombstones": 3,
                "observed": 42,
                "drift": true,
                "missing": 0,
                "unexpected": 2,
                "mismatched": 0,
                "scan_issues": [["large.bin", "too-large"]],
                "full_scans": 17,
                "inbound_noop_transactions": 11,
                "inbound_guarded_transactions": 3,
                "sync_passes": 9,
                "scan_micros": 1500,
                "materialize_micros": 2500,
                "persist_micros": 3500,
                "reconcile_micros": 4500,
                "reconcile_wire_bytes": 11000000,
                "reconcile_failures": 3,
                "sweep": "disabled",
                "delta_fallbacks": 0,
                "full_payload_sends": 0,
                "content_bytes": 0,
                "stopped_peers": ["hetz:denied"],
                "digest": "lattice-point-aaaa"
            })
        );
    }

    #[test]
    fn sync_ls_accepts_legacy_control_present_count() {
        let mut status = status();
        status.present = 0;
        assert_eq!(logical_present(&status), 40);
    }

    #[test]
    fn sync_ls_accepts_legacy_status_without_observability_counters() {
        let status: SyncEntryStatus = serde_json::from_value(serde_json::json!({
            "name": "catalog",
            "folder": "/catalog",
            "policy": "catalog",
            "peers": "*",
            "files": 40,
            "present": 40,
            "tombstones": 3,
            "observed": 40,
            "missing": 0,
            "unexpected": 0,
            "mismatched": 0
        }))
        .unwrap();
        assert_eq!(status.full_scans, 0);
        assert_eq!(status.inbound_noop_transactions, 0);
        assert_eq!(status.inbound_guarded_transactions, 0);
    }
}

fn absolutize(folder: &str) -> Result<PathBuf> {
    let path = PathBuf::from(folder);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn parse_sync_peers(value: &str) -> SyncPeers {
    if value.trim() == "*" {
        SyncPeers::Wildcard("*".to_string())
    } else {
        SyncPeers::List(
            value
                .split(',')
                .map(|part| part.trim().to_string())
                .filter(|part| !part.is_empty())
                .collect(),
        )
    }
}

fn parse_sync_policy(value: &str) -> Result<SyncPolicy> {
    match value {
        "catalog" => Ok(SyncPolicy::Catalog),
        "bus" => Ok(SyncPolicy::Bus),
        other => bail!("unknown sync policy {other:?}; use catalog or bus"),
    }
}

fn parse_include(value: Option<&str>) -> Option<Vec<String>> {
    let value = value?;
    let globs: Vec<String> = value
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect();
    if globs.is_empty() { None } else { Some(globs) }
}

#[allow(clippy::too_many_arguments)]
fn print_status(
    version: &str,
    node_id: &str,
    endpoint_addr: &serde_json::Value,
    exposed_protocols: &[String],
    dial_sockets: &[PathBuf],
    allow_shell: bool,
    allow_exec: bool,
    peers: &[PeerReachability],
    connection_telemetry: &BTreeMap<String, PeerTelemetry>,
    connection_telemetry_window: &TelemetryWindow,
    dial_handlers: (usize, usize),
) -> Result<()> {
    println!("version\t{version}");
    println!("node\t{node_id}");
    println!("addr\t{}", serde_json::to_string(endpoint_addr)?);
    println!("exposed\t{}", joined_or_dash(exposed_protocols));
    let dials: Vec<String> = dial_sockets
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    println!("dials\t{}", joined_or_dash(&dials));
    println!(
        "shell\t{}",
        if allow_shell { "allowed" } else { "disabled" }
    );
    println!("exec\t{}", if allow_exec { "allowed" } else { "disabled" });
    // Shell, exec and every dial share these. When all are held, a new one
    // waits with no error, which reads as "hangs while ping answers".
    let (active, max) = dial_handlers;
    println!("dial handlers\t{active}/{max} in use");
    print_peer_reachability(peers);
    let current_peers = peers
        .iter()
        .map(|peer| peer.name.clone().unwrap_or_else(|| peer.id.clone()))
        .collect::<BTreeSet<_>>();
    print_connection_telemetry(
        connection_telemetry,
        connection_telemetry_window,
        &current_peers,
    );
    print_path_latency(connection_telemetry, &current_peers);
    Ok(())
}

/// Report what the counters know about losing and regaining a transport.
///
/// The point of the line is the pair: a resume count on its own cannot say
/// whether resumption works, because 9 resumes out of 10 losses and 9 out of 90
/// are very different systems. The path breakdown answers "came back how", and
/// the measured median answers "came back how fast".
fn print_connection_telemetry(
    telemetry: &BTreeMap<String, PeerTelemetry>,
    window: &TelemetryWindow,
    current_peers: &BTreeSet<String>,
) {
    for line in connection_telemetry_lines(telemetry, window, current_peers) {
        println!("{line}");
    }
}

/// Report the accumulated probe latency for each peer, split by path.
///
/// The peer table above shows one instantaneous ping. That single sample cannot
/// answer the question that matters for a machine that moves networks: is the
/// direct path to this peer actually better than the relay, and which one is it
/// spending its time on? The daemon has measured that on every probe in the
/// durable window, and the only way to read it was to parse `telemetry.json`
/// by hand — the exact grepping these counters exist to end.
///
/// This reports facts and reaches no verdict. It does not label a path degraded
/// and it changes no routing.
fn print_path_latency(
    telemetry: &BTreeMap<String, PeerTelemetry>,
    current_peers: &BTreeSet<String>,
) {
    for line in path_latency_lines(telemetry, current_peers) {
        println!("{line}");
    }
}

fn path_latency_lines(
    telemetry: &BTreeMap<String, PeerTelemetry>,
    current_peers: &BTreeSet<String>,
) -> Vec<String> {
    // A peer is included on probe evidence alone. Keying this off losses, the
    // way the sessions block does, would blank the healthy peer — and healthy is
    // the normal state, so it is the one that must never be empty.
    let measured: Vec<_> = telemetry
        .iter()
        .filter(|(_, stats)| stats.probes_reachable > 0 || stats.probes_unreachable > 0)
        .collect();
    if measured.is_empty() {
        return vec!["paths\tno probes recorded".to_string()];
    }

    let mut lines = vec!["paths".to_string()];
    for (peer, stats) in measured {
        let peer = telemetry_peer_label(peer, current_peers);
        let total = stats.probes_reachable + stats.probes_unreachable;
        lines.push(format!(
            "  {peer}\treachable {}/{}",
            stats.probes_reachable, total
        ));

        // Busiest path first: which path a peer actually spends its time on is
        // the finding, not an afterthought.
        let mut paths: Vec<_> = stats
            .probe_latency
            .iter()
            .filter(|(_, latency)| latency.samples > 0)
            .collect();
        paths.sort_by(|a, b| b.1.samples.cmp(&a.1.samples).then(a.0.cmp(b.0)));

        let answered: u64 = paths.iter().map(|(_, latency)| latency.samples).sum();
        for (path, latency) in paths {
            let share = if answered > 0 {
                format!("{:.0}%", 100.0 * latency.samples as f64 / answered as f64)
            } else {
                "-".to_string()
            };
            // Mean and max, not percentiles, and that is deliberate. Latency is
            // stored in buckets whose bounds double, so around 50–200ms two
            // paths that genuinely differ land in the same bucket and print
            // identical percentiles. Live data showed exactly that: direct and
            // relay both reported p50 100.0ms and p90 200.0ms while their means
            // differed and their maxima differed by more than 2x. A number that
            // hides the difference it exists to show is worse than none.
            //
            // Mean and max are both stored exactly, so they are reported exactly.
            lines.push(format!(
                "    {path}\t{share}\tn={}\tmean={}\tmax={}",
                latency.samples,
                format_micros(latency.mean_micros()),
                format_micros(Some(latency.max_micros)),
            ));
        }
    }
    lines
}

fn format_micros(micros: Option<u64>) -> String {
    match micros {
        Some(micros) => format!("{:.1}ms", micros as f64 / 1000.0),
        None => "-".to_string(),
    }
}

fn connection_telemetry_lines(
    telemetry: &BTreeMap<String, PeerTelemetry>,
    window: &TelemetryWindow,
    current_peers: &BTreeSet<String>,
) -> Vec<String> {
    let recorded: Vec<_> = telemetry
        .iter()
        .filter(|(_, stats)| stats.losses > 0 || stats.resumes > 0 || stats.resume_failures > 0)
        .collect();
    let mut lines = vec![telemetry_window_line(window)];
    if recorded.is_empty() {
        lines.push("  no losses recorded".to_string());
        return lines;
    }

    for (peer, stats) in recorded {
        let peer = telemetry_peer_label(peer, current_peers);
        let median = stats
            .reconnect
            .quantile_micros(0.5)
            .map(format_seconds)
            .unwrap_or_else(|| "-".to_string());
        let worst = if stats.reconnect.samples > 0 {
            format_seconds(stats.reconnect.max_micros)
        } else {
            "-".to_string()
        };
        lines.push(format!(
            "  {peer}\tlost={} resumed={} failed={} attempts={} reconnect_p50={median} reconnect_max={worst}",
            stats.losses, stats.resumes, stats.resume_failures, stats.reconnect_attempts
        ));
        if !stats.losses_by_path.is_empty() || !stats.resumes_by_path.is_empty() {
            lines.push(format!(
                "    lost_on={} resumed_on={}",
                format_path_counts(&stats.losses_by_path),
                format_path_counts(&stats.resumes_by_path)
            ));
        }
    }
    lines
}

fn telemetry_window_line(window: &TelemetryWindow) -> String {
    let started = window.started_unix_seconds.and_then(|seconds| {
        i64::try_from(seconds)
            .ok()
            .and_then(|seconds| OffsetDateTime::from_unix_timestamp(seconds).ok())
            .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
    });
    match (started, window.reset_reason.as_deref()) {
        (Some(started), Some(reason)) => format!("sessions\tsince {started}; {reason}"),
        (Some(started), None) => format!("sessions\tsince {started}"),
        (None, Some(reason)) => format!("sessions\t{reason}"),
        (None, None) => "sessions\twindow unknown: daemon did not report it".to_string(),
    }
}

fn telemetry_peer_label(peer: &str, current_peers: &BTreeSet<String>) -> String {
    if current_peers.contains(peer) {
        peer.to_string()
    } else {
        format!("{peer} [not in peers.toml]")
    }
}

fn format_seconds(micros: u64) -> String {
    format!("{:.1}s", micros as f64 / 1_000_000.0)
}

fn format_path_counts(counts: &BTreeMap<String, u64>) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }
    counts
        .iter()
        .map(|(path, count)| format!("{path}={count}"))
        .collect::<Vec<_>>()
        .join(",")
}

async fn print_daemon_reachability(home: &FabricHome) -> Result<()> {
    match send_control(home, ControlRequest::ReachabilityStatus).await? {
        ControlResponse::ReachabilityStatus { peers, .. } => {
            print_startup_reachability(&peers);
            Ok(())
        }
        response => bail!("unexpected daemon response: {response:?}"),
    }
}

fn print_startup_reachability(peers: &[PeerReachability]) {
    if peers.is_empty() {
        println!("reachability: no trusted peers");
        return;
    }

    for peer in peers {
        println!("reachability: {}", format_peer_reachability(peer));
    }
}

fn print_peer_reachability(peers: &[PeerReachability]) {
    if peers.is_empty() {
        println!("peers\t-");
        return;
    }

    println!("peers");
    for peer in peers {
        println!("  {}", format_peer_reachability(peer));
    }
}

fn format_peer_reachability(peer: &PeerReachability) -> String {
    let label = peer.name.as_deref().unwrap_or(&peer.id);
    if peer.reachable {
        let millis = peer.round_trip_micros.unwrap_or_default() as f64 / 1000.0;
        let transport = peer.transport.as_deref().unwrap_or("unknown");
        format!(
            "{label}\t{}\treachable\t{} bytes\t{millis:.3} ms\t{transport}",
            peer.id,
            peer.bytes.unwrap_or_default()
        )
    } else if peer.roaming {
        format!("{label}\t{}\taway\troaming peer", peer.id)
    } else {
        let error = peer.error.as_deref().unwrap_or("unreachable");
        format!("{label}\t{}\tunreachable\t{error}", peer.id)
    }
}

fn joined_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(",")
    }
}

/// Resolve an enable/disable flag pair into a tri-state override: `Some(true)` to
/// enable, `Some(false)` to explicitly disable, `None` to leave the persisted
/// value untouched. Shared by the shell and exec allow flags.
/// The same tri-state as `allow_override`, for a value that is itself optional.
/// Nothing said keeps the persisted ceiling; `--no-memory-max-mb` clears it.
fn memory_override(value: Option<u64>, clear: bool) -> Option<Option<u64>> {
    if clear {
        return Some(None);
    }
    value.map(Some)
}

fn allow_override(enable: bool, disable: bool) -> Option<bool> {
    if enable {
        Some(true)
    } else if disable {
        Some(false)
    } else {
        None
    }
}

/// Probe exit codes are the machine-readable answer. 0 through 3 are answers
/// about the PEER; anything else means the question could not be asked, which a
/// caller must not confuse with "unsupported".
const PROBE_EXIT_SUPPORTED: i32 = 0;
const PROBE_EXIT_UNSUPPORTED: i32 = 1;
const PROBE_EXIT_UNREACHABLE: i32 = 2;
const PROBE_EXIT_TIMEOUT: i32 = 3;
const PROBE_EXIT_UNANSWERABLE: i32 = 64;

/// One human line per probe outcome. Machine callers use --json or the exit code.
fn print_probe_line(
    peer: &str,
    protocol: &str,
    outcome: &str,
    round_trip_micros: Option<u64>,
    transport: Option<&str>,
    error: Option<&str>,
) {
    match outcome {
        "supported" => {
            let millis = round_trip_micros.unwrap_or(0) as f64 / 1000.0;
            match transport {
                Some(transport) => {
                    println!("{peer} supports {protocol} ({millis:.3} ms via {transport})")
                }
                None => println!("{peer} supports {protocol} ({millis:.3} ms)"),
            }
        }
        "unsupported" => println!("{peer} does not support {protocol}"),
        "timeout" => println!("{peer} did not answer for {protocol} before the deadline"),
        _ => match error {
            Some(error) => println!("{peer} is unreachable for {protocol}: {error}"),
            None => println!("{peer} is unreachable for {protocol}"),
        },
    }
}

fn daemon_options(
    allow_shell: bool,
    allow_exec: bool,
    server_session_max_total: Option<usize>,
    server_session_max_per_peer: Option<usize>,
    server_session_detached_ttl_secs: Option<u64>,
) -> DaemonOptions {
    DaemonOptions {
        allow_shell,
        allow_exec,
        server_session_max_total,
        server_session_max_per_peer,
        server_session_detached_ttl_secs,
    }
}

fn run_restart_detacher(home: &FabricHome, allow_shell: bool) -> Result<()> {
    println!(
        "restart detacher started: version={} allow_shell={allow_shell}",
        fabric::version_string()
    );
    let exe = std::env::current_exe()?;
    let mut command = ProcessCommand::new(exe);
    command.arg("--home").arg(home.root()).arg("restart-helper");
    if allow_shell {
        command.arg("--allow-shell");
    }
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    println!("restart helper spawned: pid={}", child.id());
    Ok(())
}

async fn run_restart_helper(home: &FabricHome, allow_shell: bool) -> Result<()> {
    println!(
        "restart helper started: version={} allow_shell={allow_shell}",
        fabric::version_string()
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    match send_control(home, ControlRequest::Shutdown).await {
        Ok(_) => println!("shutdown requested"),
        Err(error) => println!("shutdown request failed; continuing: {error:#}"),
    }

    if let Err(error) = wait_for_daemon_down(home, Duration::from_secs(10)).await {
        println!("daemon did not report down before restart; continuing: {error:#}");
    }

    let start_result = spawn_daemon(home, DaemonOptions::new(allow_shell)).await;
    if let Err(error) = &start_result {
        println!("daemon start failed; checking final state: {error:#}");
    }

    match wait_for_daemon_ready(home, allow_shell, Duration::from_secs(10)).await {
        Ok(_) => {
            println!("restart complete");
            Ok(())
        }
        Err(ready_error) => {
            if let Err(start_error) = start_result {
                bail!("restart failed: {start_error:#}; final status: {ready_error:#}");
            }
            Err(ready_error)
        }
    }
}

async fn wait_for_daemon_down(home: &FabricHome, timeout: Duration) -> Result<()> {
    let started = Instant::now();
    loop {
        let status_ok = send_control(home, ControlRequest::Status).await.is_ok();
        if fabric::daemon::restart_down_decision(
            status_ok,
            fabric::daemon::daemon_lock_available(home)?,
        ) {
            return Ok(());
        }
        if started.elapsed() > timeout {
            bail!("daemon still answered after {:.1}s", timeout.as_secs_f32());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_daemon_ready(
    home: &FabricHome,
    expected_allow_shell: bool,
    timeout: Duration,
) -> Result<()> {
    let started = Instant::now();
    loop {
        match send_control(home, ControlRequest::Status).await {
            Ok(ControlResponse::Status { allow_shell, .. }) => {
                if allow_shell != expected_allow_shell {
                    bail!(
                        "daemon is running with allow_shell={allow_shell}, expected {expected_allow_shell}"
                    );
                }
                return Ok(());
            }
            Ok(response) => bail!("unexpected daemon response: {response:?}"),
            Err(error) => {
                if started.elapsed() > timeout {
                    bail!(
                        "daemon did not become ready after {:.1}s: {error:#}",
                        timeout.as_secs_f32()
                    );
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run_shell_client(socket: &PathBuf) -> Result<i32> {
    let stream = tokio::net::UnixStream::connect(socket).await?;
    let (mut read, write) = stream.into_split();
    let mut signals = ShellSignals::new()?;
    let terminal = TerminalModeGuard::enable_if_terminal()?;
    let (cols, rows) = terminal_size();
    let write = Arc::new(tokio::sync::Mutex::new(write));
    shell::write_client_resize(&mut *write.lock().await, rows, cols).await?;

    let stdin_write = write.clone();
    let stdin_task = tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 8192];
        loop {
            let read = stdin.read(&mut buf).await?;
            if read == 0 {
                shell::write_client_eof(&mut *stdin_write.lock().await).await?;
                return Ok::<(), anyhow::Error>(());
            }
            shell::write_client_stdin(&mut *stdin_write.lock().await, &buf[..read]).await?;
        }
    });

    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    let mut exit_code = 1;

    loop {
        tokio::select! {
            frame = shell::read_server_frame(&mut read) => {
                let Some(frame) = frame? else {
                    break;
                };
                match frame {
                    ServerFrame::Output(bytes) => {
                        stdout.write_all(&bytes).await?;
                        stdout.flush().await?;
                    }
                    ServerFrame::Error(message) => {
                        stderr.write_all(message.as_bytes()).await?;
                        stderr.write_all(b"\n").await?;
                        stderr.flush().await?;
                    }
                    ServerFrame::Status(message) => {
                        stderr.write_all(message.as_bytes()).await?;
                        stderr.write_all(b"\n").await?;
                        stderr.flush().await?;
                    }
                    ServerFrame::Exit(code) => {
                        exit_code = normalize_exit_code(code);
                        break;
                    }
                }
            }
            signal = signals.recv() => {
                match signal {
                    ShellSignal::Resize => {
                        let (cols, rows) = terminal_size();
                        shell::write_client_resize(&mut *write.lock().await, rows, cols).await?;
                    }
                    ShellSignal::Suspend => {
                        terminal.restore()?;
                        suspend_current_process();
                        terminal.reenter_raw()?;
                        let (cols, rows) = terminal_size();
                        shell::write_client_resize(&mut *write.lock().await, rows, cols).await?;
                    }
                    ShellSignal::Terminate(signal) => {
                        terminal.restore()?;
                        terminate_with_signal(signal);
                    }
                }
            }
        }
    }

    stdin_task.abort();
    let _ = stdin_task.await;
    terminal.restore()?;
    stdout.flush().await?;
    stderr.flush().await?;
    Ok(exit_code)
}

/// Drive the client side of a `fabric exec` session over the daemon-provided
/// socket: send the argv, forward the remote stdout/stderr to the local
/// stdout/stderr on their own streams, and return the remote command's exit code.
async fn run_exec_client(socket: &PathBuf, cmd: &[String]) -> Result<i32> {
    let stream = tokio::net::UnixStream::connect(socket).await?;
    let (mut read, mut write) = stream.into_split();
    exec::write_client_argv(&mut write, cmd).await?;

    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    let mut exit_code = 1;

    while let Some(frame) = exec::read_server_frame(&mut read).await? {
        match frame {
            exec::ServerFrame::Stdout(bytes) => {
                stdout.write_all(&bytes).await?;
                stdout.flush().await?;
            }
            exec::ServerFrame::Stderr(bytes) => {
                stderr.write_all(&bytes).await?;
                stderr.flush().await?;
            }
            exec::ServerFrame::Error(message) => {
                stderr.write_all(message.as_bytes()).await?;
                stderr.write_all(b"\n").await?;
                stderr.flush().await?;
            }
            exec::ServerFrame::Exit(code) => {
                exit_code = normalize_exit_code(code);
                break;
            }
        }
    }

    stdout.flush().await?;
    stderr.flush().await?;
    Ok(exit_code)
}

/// When a mutating command (down/restart) can't reach a daemon at the target
/// home, warn if a daemon IS running on the DEFAULT (prod) home — the common dev
/// footgun of forgetting --home/FABRIC_HOME (or the dev daemon being down). The
/// command still fails on its own "not running" error; this just adds the hint.
async fn warn_home_daemon_mismatch(target: &FabricHome) {
    if target.is_default_state_root() {
        return;
    }
    let Some(default_root) = FabricHome::default_state_root() else {
        return;
    };
    if target.root() == default_root.as_path() {
        return;
    }
    let default_sock = default_root.join("run/control.sock");
    if tokio::net::UnixStream::connect(&default_sock).await.is_ok() {
        eprintln!(
            "fabric: no daemon at --home {}, but a fabric daemon IS running on the default home \
             {} — did you forget --home/FABRIC_HOME (dev commands must target your dev home), or \
             is your dev daemon down?",
            target.root().display(),
            default_root.display(),
        );
    }
}

async fn run_debug_echo(socket: PathBuf) -> Result<()> {
    if socket.exists() {
        fs::remove_file(&socket)?;
    }
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let _cleanup = SocketFileGuard(socket.clone());
    println!("echo listening\t{}", socket.display());

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                tokio::spawn(async move {
                    let (mut read, mut write) = stream.into_split();
                    if let Err(error) = tokio::io::copy(&mut read, &mut write).await {
                        eprintln!("fabric debug echo: connection failed: {error}");
                    }
                });
            }
            result = tokio::signal::ctrl_c() => {
                result?;
                break;
            }
        }
    }

    Ok(())
}

async fn run_debug_unix_cat(socket: PathBuf) -> Result<()> {
    let stream = tokio::net::UnixStream::connect(&socket).await?;
    let (mut read, mut write) = stream.into_split();

    let to_socket = async {
        let mut stdin = tokio::io::stdin();
        tokio::io::copy(&mut stdin, &mut write).await?;
        write.shutdown().await?;
        Ok::<(), anyhow::Error>(())
    };
    let to_stdout = async {
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut read, &mut stdout).await?;
        stdout.flush().await?;
        Ok::<(), anyhow::Error>(())
    };
    tokio::try_join!(to_socket, to_stdout)?;
    Ok(())
}

fn terminal_size() -> (u16, u16) {
    if std::io::stdout().is_terminal()
        && let Ok((cols, rows)) = crossterm::terminal::size()
    {
        return (cols, rows);
    }
    (80, 24)
}

fn normalize_exit_code(code: i32) -> i32 {
    code.clamp(0, 255)
}

struct SocketFileGuard(PathBuf);

impl Drop for SocketFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

enum ShellSignal {
    Resize,
    Suspend,
    Terminate(i32),
}

#[cfg(unix)]
struct ShellSignals {
    hangup: tokio::signal::unix::Signal,
    interrupt: tokio::signal::unix::Signal,
    quit: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    suspend: tokio::signal::unix::Signal,
    resize: tokio::signal::unix::Signal,
}

#[cfg(not(unix))]
struct ShellSignals;

#[cfg(unix)]
impl ShellSignals {
    fn new() -> Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            hangup: signal(SignalKind::hangup())?,
            interrupt: signal(SignalKind::interrupt())?,
            quit: signal(SignalKind::quit())?,
            terminate: signal(SignalKind::terminate())?,
            suspend: signal(SignalKind::from_raw(libc::SIGTSTP))?,
            resize: signal(SignalKind::window_change())?,
        })
    }

    async fn recv(&mut self) -> ShellSignal {
        tokio::select! {
            _ = self.hangup.recv() => ShellSignal::Terminate(libc::SIGHUP),
            _ = self.interrupt.recv() => ShellSignal::Terminate(libc::SIGINT),
            _ = self.quit.recv() => ShellSignal::Terminate(libc::SIGQUIT),
            _ = self.terminate.recv() => ShellSignal::Terminate(libc::SIGTERM),
            _ = self.suspend.recv() => ShellSignal::Suspend,
            _ = self.resize.recv() => ShellSignal::Resize,
        }
    }
}

#[cfg(not(unix))]
impl ShellSignals {
    fn new() -> Result<Self> {
        Ok(Self)
    }

    async fn recv(&mut self) -> ShellSignal {
        std::future::pending().await
    }
}

#[cfg(unix)]
fn suspend_current_process() {
    // SIGTSTP is intercepted above so we can restore the terminal first. SIGSTOP
    // cannot be caught, which guarantees one real stop; execution resumes here
    // after the process receives SIGCONT.
    unsafe {
        libc::raise(libc::SIGSTOP);
    }
}

#[cfg(not(unix))]
fn suspend_current_process() {}

#[cfg(unix)]
fn terminate_with_signal(signal: i32) -> ! {
    // Tokio installed the process signal handler. Restore the default action
    // after restoring termios, then re-raise so parents observe a signal exit
    // instead of a fabricated numeric status.
    unsafe {
        libc::signal(signal, libc::SIG_DFL);
        libc::raise(signal);
        libc::_exit(128 + signal);
    }
}

#[cfg(not(unix))]
fn terminate_with_signal(_signal: i32) -> ! {
    std::process::exit(1)
}

async fn spawn_daemon(home: &FabricHome, options: DaemonOptions) -> Result<()> {
    if send_control(home, ControlRequest::Status).await.is_ok() {
        println!("already running");
        return Ok(());
    }

    home.prepare()?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(home.log_path())?;
    let err = log.try_clone()?;
    let exe = std::env::current_exe()?;
    let mut command = ProcessCommand::new(exe);
    command.arg("--home").arg(home.root()).arg("daemon");
    if options.allow_shell {
        command.arg("--allow-shell");
    }
    if options.allow_exec {
        command.arg("--allow-exec");
    }
    if let Some(max_total) = options.server_session_max_total {
        command
            .arg("--server-session-max-total")
            .arg(max_total.to_string());
    }
    if let Some(max_per_peer) = options.server_session_max_per_peer {
        command
            .arg("--server-session-max-per-peer")
            .arg(max_per_peer.to_string());
    }
    if let Some(detached_ttl_secs) = options.server_session_detached_ttl_secs {
        command
            .arg("--server-session-detached-ttl-secs")
            .arg(detached_ttl_secs.to_string());
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()?;

    let started = Instant::now();
    loop {
        if send_control(home, ControlRequest::Status).await.is_ok() {
            println!("started");
            return Ok(());
        }
        if started.elapsed() > Duration::from_secs(10) {
            bail!(
                "daemon did not become ready; see {}",
                home.log_path().display()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
