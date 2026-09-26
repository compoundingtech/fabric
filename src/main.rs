use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::{CommandFactory, Parser, Subcommand};
use fabric::{
    config::{
        DEFAULT_EXEC_MAX_CHILDREN, FabricHome, GitAccess, PeerBook, generate_identity_file,
        load_or_create_identity, parse_addr_json, parse_node_id,
    },
    control::{ControlRequest, ControlResponse, PeerReachability, SyncRuntimeStatus},
    daemon::{
        DaemonOptions, FabricNode, init_daemon_tracing, run_daemon_with_options, send_control,
    },
    service::{self, ServiceInstallOptions},
    sync::cli::SyncCommands,
    sync::config::{SyncBook, SyncEntry, SyncPeers, SyncPolicy},
    telemetry::{PeerTelemetry, TelemetryWindow},
    update,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::AsyncWriteExt;

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
    /// Pair with a machine you can already ssh to: one command, both directions.
    ///
    /// Runs `fabric id` there over your own ssh (config, agent and prompts
    /// included), trusts that id here under the host's name, then runs
    /// `fabric add` and `fabric reload-peers` there for this machine. Nothing is
    /// copied but two public keys. What it writes is exactly what the manual
    /// `fabric add` steps write; `fabric remove` on each side undoes it.
    Join {
        /// ssh destinations: aliases from ~/.ssh/config or user@host.
        hosts: Vec<String>,
        /// Join every named Host in ~/.ssh/config (patterns like `*` are skipped).
        /// ssh runs without prompts in this mode, so a host that would ask for a
        /// passphrase or a new host key is reported, not joined.
        #[arg(long)]
        all: bool,
        /// Services that machine lets THIS machine use. Default: shell,exec, which
        /// is what an ssh login already amounts to. Add sync or an exposed name
        /// deliberately.
        #[arg(long = "allow", value_delimiter = ',')]
        allow: Option<Vec<String>>,
        /// Services THIS machine lets that machine use. Default: none, because
        /// being able to ssh somewhere never let it reach you.
        #[arg(long = "grant", value_delimiter = ',')]
        grant: Option<Vec<String>>,
        /// The name that machine records for this one. Default: this host's name.
        #[arg(long)]
        name: Option<String>,
        /// Only write trust here; leave the far side's peers.toml alone.
        #[arg(long)]
        local_only: bool,
        /// Print what would be joined and change nothing.
        #[arg(long)]
        dry_run: bool,
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
        #[arg(long, hide = true)]
        allow_shell: bool,
        /// Accepted for compatibility. peers.toml decides exec availability.
        #[arg(long, hide = true)]
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
        #[arg(long, hide = true, conflicts_with = "no_allow_shell")]
        allow_shell: bool,
        /// Accepted for compatibility. peers.toml decides shell availability.
        #[arg(long, hide = true)]
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
    /// Reports each check as `ok`, `info`, `setup`, `problem`, or `unknown`. A check
    /// that could not establish an answer says `unknown` rather than `ok`, and
    /// that counts as needing attention: a doctor is read INSTEAD of
    /// investigating, so it must not guess in the reassuring direction.
    ///
    /// Exit code is the answer: 0 no attention needed, 1 problem or setup,
    /// 2 command-line usage error, 3 unknown because a check could not answer.
    /// The command never changes anything.
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
        /// The update generation this supervisor owns. A stale or missing
        /// generation record makes the supervisor stop without changing bytes.
        #[arg(long)]
        generation: String,
        /// The version the daemon must report before this counts as a healthy
        /// restart. Checking that a socket answers is not enough: the old daemon
        /// is still answering until the moment it is torn down.
        #[arg(long)]
        expect: String,
        /// Restore the named rollback immediately. This is the manual recovery
        /// path printed after a scheduled update.
        #[arg(long)]
        restore_now: bool,
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
        #[arg(long, hide = true, conflicts_with = "no_allow_shell")]
        allow_shell: bool,
        /// Accepted for compatibility. peers.toml decides shell availability.
        #[arg(long, hide = true)]
        no_allow_shell: bool,
        /// Accepted for compatibility. peers.toml decides exec availability.
        #[arg(long, hide = true, conflicts_with = "no_allow_exec")]
        allow_exec: bool,
        /// Accepted for compatibility. peers.toml decides exec availability.
        #[arg(long, hide = true)]
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
    if fabric::services::git::invoked_as_remote_helper() {
        let code = fabric::services::git::run_remote_helper(|| {
            let home = FabricHome::resolve(None)?;
            Ok(move |peer: String| async move {
                let response = send_control(&home, ControlRequest::Git { peer })
                    .await
                    .with_context(|| "the running Fabric daemon could not open a Git transport")?;
                let socket = match response {
                    ControlResponse::Git { socket } => socket,
                    other => {
                        bail!("the running Fabric daemon returned an unexpected reply: {other:?}")
                    }
                };
                tokio::net::UnixStream::connect(&socket)
                    .await
                    .with_context(|| format!("failed to connect to Fabric at {}", socket.display()))
            })
        })
        .await?;
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
                            current_connection_health,
                            active_dial_handlers,
                            max_dial_handlers,
                        } => {
                            let sync_runtime = match send_control(
                                &home,
                                ControlRequest::SyncRuntimeStatus,
                            )
                            .await
                            {
                                Ok(ControlResponse::SyncRuntimeStatus { runtime }) => runtime,
                                _ => SyncRuntimeStatus {
                                    owner: "unknown".to_string(),
                                    companion: "unknown".to_string(),
                                },
                            };
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
                                &current_connection_health,
                                (active_dial_handlers, max_dial_handlers),
                                &sync_runtime,
                            )?;
                        }
                        response => bail!("unexpected daemon response: {response:?}"),
                    }
                }
                Commands::Peers => {
                    let book = PeerBook::load(&home)?;
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
                        let helper = fabric::services::git::install_helper_for(&binary)?;
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
                    SyncBook::load(&home)?.validate_against(&book)?;
                    book.save(&home)?;
                    let _ = send_control(&home, ControlRequest::ReloadPeers).await;
                }
                Commands::Join {
                    hosts,
                    all,
                    allow,
                    grant,
                    name,
                    local_only,
                    dry_run,
                } => {
                    let code = run_join(&home, hosts, all, allow, grant, name, local_only, dry_run)
                        .await?;
                    std::process::exit(code);
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
                    SyncBook::load(&home)?.validate_against(&book)?;
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
                            let _ = allow_shell;
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
                    let socket = request_shell_socket(&home, &peer).await?;
                    let (home_ref, peer_ref) = (&home, peer.as_str());
                    let code = fabric::services::shell::client::run_client(&peer, socket, move || {
                        request_shell_socket(home_ref, peer_ref)
                    })
                    .await?;
                    std::process::exit(code);
                }
                Commands::Exec { peer, cmd } => {
                    let socket =
                        match send_control(&home, ControlRequest::Exec { peer: peer.clone() })
                            .await?
                        {
                            ControlResponse::Exec { socket } => socket,
                            response => bail!("unexpected daemon response: {response:?}"),
                        };
                    let stream = tokio::net::UnixStream::connect(&socket).await?;
                    let code = fabric::services::exec::run_client(stream, &peer, &cmd).await?;
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
                Commands::SuperviseRestart {
                    rollback,
                    generation,
                    expect,
                    restore_now,
                } => {
                    update::supervise_restart(
                        &home,
                        &rollback,
                        &generation,
                        &expect,
                        restore_now,
                    )
                    .await?;
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
                    if let Err(error) = run_daemon_with_options(
                        home,
                        daemon_options(
                            allow_shell,
                            allow_exec,
                            server_session_max_total,
                            server_session_max_per_peer,
                            server_session_detached_ttl_secs,
                        ),
                    )
                    .await {
                        fabric_config::log::stderr(&format!("Error: {error:?}"));
                        std::process::exit(1);
                    }
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

/// `fabric sync add`, `rm` and `reload` change what the daemon syncs, and
/// `add` checks selectors against the daemon's peer book, so they run here.
/// Every other sync command belongs to the companion: this process becomes
/// the `fabric-sync` installed beside it, with the same arguments.
async fn run_sync(home: &FabricHome, command: SyncCommands) -> Result<()> {
    match command {
        SyncCommands::Ls { .. }
        | SyncCommands::Stage { .. }
        | SyncCommands::Staged { .. }
        | SyncCommands::Publish { .. }
        | SyncCommands::Discard { .. } => return hand_sync_command_to_companion(),
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
            let peers = PeerBook::load(home)?;
            book.validate_against(&peers)?;
            book.save(home)?;
            // Apply live if the daemon is running; harmless if it is not.
            let _ = send_control(home, ControlRequest::SyncReload).await;
            println!("sync {name:?} written to {}", home.syncs_path().display());
        }
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

/// Replace this process with the `fabric-sync` beside it, same arguments.
fn hand_sync_command_to_companion() -> Result<()> {
    let binary = std::env::current_exe().context("cannot resolve the running Fabric binary")?;
    let companion = binary.with_file_name("fabric-sync");
    if !companion.exists() {
        bail!(
            "this sync command runs in fabric-sync, which is not installed beside {}; \
             install fabric and fabric-sync from the same release",
            binary.display()
        );
    }
    let error = std::os::unix::process::CommandExt::exec(
        ProcessCommand::new(&companion).args(std::env::args_os().skip(1)),
    );
    Err(error).with_context(|| format!("failed to run {}", companion.display()))
}

#[cfg(test)]
mod sync_command_tests {
    use super::*;

    #[tokio::test]
    async fn sync_add_rejects_an_unknown_explicit_selector_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let folder = dir.path().join("catalog");

        let error = run_sync(
            &home,
            SyncCommands::Add {
                folder: folder.display().to_string(),
                name: "catalog".into(),
                peers: "mac".into(),
                policy: "catalog".into(),
                include: None,
            },
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("unknown peer selector \"mac\""));
        assert!(!home.syncs_path().exists());
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
            let helper = fabric::services::git::helper_path_for(&binary);
            match (helper, fabric::services::git::helper_is_installed_for(&binary)) {
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
        assert_eq!(
            lines[0],
            "session history (lifetime totals)\tsince 1970-01-01T00:00:00Z"
        );
        assert_eq!(
            lines[1],
            // p50 is a bucket bound, because a histogram cannot report better
            // than its bucket. The max is the exact largest sample seen.
            "  hetz\tlifetime_lost=4 lifetime_resumed=4 lifetime_failed=0 lifetime_attempts=7 reconnect_p50=2.0s reconnect_max=4.5s"
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
                "session history (lifetime totals)\tsince 1970-01-01T00:00:00Z"
                    .to_string(),
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
            "session history (lifetime totals)\twindow start unknown: snapshot predates window tracking"
        );
    }

    #[test]
    fn current_connection_health_names_its_replacement_scope() {
        let lines = current_connection_health_lines(&BTreeMap::from([(
            "bluey".to_string(),
            fabric::mux::CurrentConnectionHealth {
                connection_id: 19,
                age_millis: 12_500,
                consecutive_attach_failures: 2,
                last_attach_failure_phase: Some("hello".to_string()),
                last_attach_failure_duration_millis: Some(750),
                last_application_progress_millis_ago: Some(450),
            },
        )]));
        assert_eq!(lines[0], "current connections");
        assert_eq!(
            lines[1],
            "  bluey\tid=19 age=12.5s attach_failures=2 last_failure=hello/750ms last_application_progress=450ms ago"
        );
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
    _allow_shell: bool,
    _allow_exec: bool,
    peers: &[PeerReachability],
    connection_telemetry: &BTreeMap<String, PeerTelemetry>,
    connection_telemetry_window: &TelemetryWindow,
    current_connection_health: &BTreeMap<String, fabric::mux::CurrentConnectionHealth>,
    dial_handlers: (usize, usize),
    sync_runtime: &SyncRuntimeStatus,
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
    // Shell, exec and every dial share these. When all are held, a new one
    // waits with no error, which reads as "hangs while ping answers".
    let (active, max) = dial_handlers;
    println!("dial handlers\t{active}/{max} in use");
    println!(
        "sync runtime\towner={}\tcompanion={}",
        sync_runtime.owner, sync_runtime.companion
    );
    print_peer_reachability(peers);
    print_current_connection_health(current_connection_health);
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

fn print_current_connection_health(
    health: &BTreeMap<String, fabric::mux::CurrentConnectionHealth>,
) {
    for line in current_connection_health_lines(health) {
        println!("{line}");
    }
}

fn current_connection_health_lines(
    health: &BTreeMap<String, fabric::mux::CurrentConnectionHealth>,
) -> Vec<String> {
    let mut lines = vec!["current connections".to_string()];
    if health.is_empty() {
        lines.push("  none".to_string());
        return lines;
    }
    for (peer, connection) in health {
        let progress = connection
            .last_application_progress_millis_ago
            .map(|millis| format!("{} ago", format_millis(millis)))
            .unwrap_or_else(|| "none".to_string());
        let failure = match (
            connection.last_attach_failure_phase.as_deref(),
            connection.last_attach_failure_duration_millis,
        ) {
            (Some(phase), Some(millis)) => format!("{phase}/{}", format_millis(millis)),
            _ => "none".to_string(),
        };
        lines.push(format!(
            "  {peer}\tid={} age={} attach_failures={} last_failure={failure} last_application_progress={progress}",
            connection.connection_id,
            format_millis(connection.age_millis),
            connection.consecutive_attach_failures,
        ));
    }
    lines
}

fn format_millis(millis: u64) -> String {
    if millis < 1_000 {
        format!("{millis}ms")
    } else {
        format!("{:.1}s", millis as f64 / 1_000.0)
    }
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
            "  {peer}\tlifetime_lost={} lifetime_resumed={} lifetime_failed={} lifetime_attempts={} reconnect_p50={median} reconnect_max={worst}",
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
        (Some(started), Some(reason)) => {
            format!("session history (lifetime totals)\tsince {started}; {reason}")
        }
        (Some(started), None) => {
            format!("session history (lifetime totals)\tsince {started}")
        }
        (None, Some(reason)) => format!("session history (lifetime totals)\t{reason}"),
        (None, None) => {
            "session history (lifetime totals)\twindow unknown: daemon did not report it"
                .to_string()
        }
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

fn run_restart_detacher(home: &FabricHome, _allow_shell: bool) -> Result<()> {
    println!(
        "restart detacher started: version={}",
        fabric::version_string()
    );
    let exe = std::env::current_exe()?;
    let mut command = ProcessCommand::new(exe);
    command.arg("--home").arg(home.root()).arg("restart-helper");
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    println!("restart helper spawned: pid={}", child.id());
    Ok(())
}

async fn run_restart_helper(home: &FabricHome, _allow_shell: bool) -> Result<()> {
    println!(
        "restart helper started: version={}",
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

    let start_result = spawn_daemon(home, DaemonOptions::default()).await;
    if let Err(error) = &start_result {
        println!("daemon start failed; checking final state: {error:#}");
    }

    match wait_for_daemon_ready(home, Duration::from_secs(10)).await {
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

async fn wait_for_daemon_ready(home: &FabricHome, timeout: Duration) -> Result<()> {
    let started = Instant::now();
    loop {
        match send_control(home, ControlRequest::Status).await {
            Ok(ControlResponse::Status { .. }) => return Ok(()),
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

/// One joined host, or why it was not.
struct JoinOutcome {
    host: String,
    result: Result<String>,
}

/// `fabric join`: see the command's help. Returns the process exit code: 0 when
/// every host joined, 1 when any did not.
#[allow(clippy::too_many_arguments)]
async fn run_join(
    home: &FabricHome,
    hosts: Vec<String>,
    all: bool,
    allow: Option<Vec<String>>,
    grant: Option<Vec<String>>,
    name: Option<String>,
    local_only: bool,
    dry_run: bool,
) -> Result<i32> {
    use fabric::join;

    let allow = match allow {
        Some(services) => join::AllowPolicy::Explicit(services),
        None => join::AllowPolicy::DefaultIfNew(
            join::DEFAULT_ALLOW
                .iter()
                .map(|service| service.to_string())
                .collect(),
        ),
    };
    let allow_services = match &allow {
        join::AllowPolicy::Explicit(services) | join::AllowPolicy::DefaultIfNew(services) => {
            services.clone()
        }
    };
    for service in allow_services.iter().chain(grant.iter().flatten()) {
        join::validate_token("service", service)?;
    }
    let local_name = match name {
        Some(name) => name,
        // The short name: `hostname` may answer with a domain suffix the far
        // side has no use for.
        None => fabric::ca::hostname()
            .split('.')
            .next()
            .unwrap_or("this machine")
            .to_string(),
    };
    join::validate_token("name", &local_name)?;

    let mut targets = hosts;
    if all {
        let home_dir = std::env::var_os("HOME")
            .map(PathBuf::from)
            .context("HOME is not set; pass hosts explicitly instead of --all")?;
        let path = join::ssh_config_path(&home_dir);
        let config = match fs::read_to_string(&path) {
            Ok(config) => config,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to read {}", path.display()));
            }
        };
        let found = join::ssh_config_hosts(&config);
        if found.is_empty() && targets.is_empty() {
            bail!(
                "no named Host entries in {}; add one per machine there, or pass hosts on the \
                 command line",
                path.display()
            );
        }
        for host in found {
            if !targets.contains(&host) {
                targets.push(host);
            }
        }
    }
    if targets.is_empty() {
        bail!("nothing to join: pass one or more ssh hosts, or --all");
    }
    for host in &targets {
        join::validate_token("host", host)?;
    }

    let local_id = load_or_create_identity(home)?.public();
    let grant_text = match &grant {
        Some(services) if !services.is_empty() => services.join(","),
        Some(_) => "-".to_string(),
        None => "- (a known peer keeps its grants)".to_string(),
    };
    let allow_text = match &allow {
        join::AllowPolicy::Explicit(services) if services.is_empty() => "-".to_string(),
        join::AllowPolicy::Explicit(services) => services.join(","),
        join::AllowPolicy::DefaultIfNew(services) => {
            format!("{} (a known peer keeps its grants)", services.join(","))
        }
    };
    if dry_run {
        for host in &targets {
            println!("would join\t{host}\tallow there={allow_text}\tgrant here={grant_text}\tas={local_name}");
        }
        return Ok(0);
    }

    let batch = all || targets.len() > 1;
    let mut outcomes = Vec::new();
    for host in &targets {
        let result = join_one(
            home,
            host,
            local_id,
            &local_name,
            &allow,
            grant.as_deref(),
            local_only,
            batch,
        )
        .await;
        outcomes.push(JoinOutcome {
            host: host.clone(),
            result,
        });
    }

    let mut failed = 0;
    for outcome in &outcomes {
        match &outcome.result {
            Ok(summary) => println!("joined\t{}\t{summary}", outcome.host),
            Err(error) => {
                failed += 1;
                eprintln!("fabric: {} was not joined: {error:#}", outcome.host);
            }
        }
    }
    if failed == 0 {
        Ok(0)
    } else {
        eprintln!(
            "fabric: {failed} of {} host(s) not joined; the ones that were are trusted on both sides",
            outcomes.len()
        );
        Ok(1)
    }
}

/// Join a single host. Trust is written here first, so a far side that then
/// fails leaves this machine able to see the peer once someone adds it there.
#[allow(clippy::too_many_arguments)]
async fn join_one(
    home: &FabricHome,
    host: &str,
    local_id: iroh::EndpointId,
    local_name: &str,
    allow: &fabric::join::AllowPolicy,
    grant: Option<&[String]>,
    local_only: bool,
    batch: bool,
) -> Result<String> {
    use fabric::join;

    let output = join::ssh_run(host, &join::remote_id_command(), batch)?;
    let stdout = join::classify(&output).map_err(anyhow::Error::from)?;
    let remote_id = join::parse_remote_id(&stdout)?;
    if remote_id == local_id {
        bail!("that is this machine (same fabric id); nothing to join");
    }

    let mut book = PeerBook::load(home)?;
    let previous = book
        .peers()
        .iter()
        .find(|peer| peer.id == remote_id)
        .map(|peer| peer.allow.clone());
    // No --grant: a peer already here keeps what it has (add_with_allow
    // preserves an existing entry's allow when given None); a new one gets
    // nothing, which is what an omitted --allow means everywhere in fabric.
    let effective_grant = grant
        .map(<[String]>::to_vec)
        .or_else(|| previous.clone())
        .unwrap_or_default();
    warn_if_permissions_would_stop_a_sync(home, &Some(effective_grant.clone()))?;
    book.add_with_allow(
        remote_id,
        Some(host.to_string()),
        None,
        grant.map(<[String]>::to_vec),
    );
    SyncBook::load(home)?.validate_against(&book)?;
    book.save(home)?;
    let local_daemon = send_control(home, ControlRequest::ReloadPeers).await.is_ok();

    let mut summary = format!(
        "id={remote_id}\tgrant here={}",
        if effective_grant.is_empty() {
            "-".to_string()
        } else {
            effective_grant.join(",")
        }
    );
    if let Some(previous) = previous
        && previous != effective_grant
    {
        summary.push_str(&format!(
            " (was {})",
            if previous.is_empty() {
                "-".to_string()
            } else {
                previous.join(",")
            }
        ));
    }
    if !local_daemon {
        summary.push_str("\tlocal daemon not running; trust applies when it starts");
    }

    if local_only {
        summary.push_str("\tfar side untouched (--local-only)");
        return Ok(summary);
    }

    let output = join::ssh_run(
        host,
        &join::remote_add_command(local_id, local_name, allow),
        batch,
    )?;
    let far = join::classify(&output).map_err(anyhow::Error::from)?;
    summary.push_str(&format!(
        "\tallow there={}\tas={local_name}",
        match allow {
            join::AllowPolicy::Explicit(services) if services.is_empty() => "-".to_string(),
            join::AllowPolicy::Explicit(services) => services.join(","),
            join::AllowPolicy::DefaultIfNew(services) => {
                format!("{} if new, kept if known", services.join(","))
            }
        }
    ));
    if !far.contains("reloaded") {
        summary.push_str("\tits daemon did not reload; trust applies when it starts");
    }
    Ok(summary)
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

struct SocketFileGuard(PathBuf);

impl Drop for SocketFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

async fn request_shell_socket(home: &FabricHome, peer: &str) -> Result<PathBuf> {
    match send_control(
        home,
        ControlRequest::Shell {
            peer: peer.to_string(),
        },
    )
    .await?
    {
        ControlResponse::Shell { socket } => Ok(socket),
        response => bail!("unexpected daemon response: {response:?}"),
    }
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
