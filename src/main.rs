use std::{
    fs::{self, OpenOptions},
    io::IsTerminal,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use clap::{CommandFactory, Parser, Subcommand};
use fabric::{
    config::{
        DEFAULT_EXEC_MAX_CHILDREN, FabricHome, PeerBook, generate_identity_file,
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
    terminal::TerminalModeGuard,
};
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
    /// List trusted peers.
    Peers,
    /// Reload peers.toml into the running daemon.
    ReloadPeers,
    /// Trust a peer NodeID and optionally assign a local name.
    Add {
        nodeid: String,
        name: Option<String>,
        /// Optional EndpointAddr JSON hint for deterministic local/direct dialing.
        #[arg(long = "addr-json")]
        addr_json: Option<String>,
    },
    /// Remove a trusted peer by NodeID or name.
    Remove { peer: String },
    /// Start the local fabric daemon.
    Up {
        /// Run in the foreground instead of spawning a background daemon.
        #[arg(long)]
        foreground: bool,
        /// Serve remote shells to trusted peers.
        #[arg(long)]
        allow_shell: bool,
        /// Serve non-interactive remote command execution (`fabric exec`) to
        /// trusted peers. Default-deny — arbitrary remote code, opt-in only.
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
        /// Force the restarted daemon to serve remote shells.
        #[arg(long, conflicts_with = "no_allow_shell")]
        allow_shell: bool,
        /// Force the restarted daemon to reject remote shells.
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
        /// Start the managed daemon with remote shell serving enabled.
        #[arg(long, conflicts_with = "no_allow_shell")]
        allow_shell: bool,
        /// Persist remote shell serving as disabled for the managed daemon.
        #[arg(long)]
        no_allow_shell: bool,
        /// Start the managed daemon with non-interactive remote exec enabled
        /// (`fabric exec`). Default-deny — opt-in only.
        #[arg(long, conflicts_with = "no_allow_exec")]
        allow_exec: bool,
        /// Persist remote exec serving as disabled for the managed daemon.
        #[arg(long)]
        no_allow_exec: bool,
        /// Memory ceiling applied by systemd/launchd, in MiB. Unset by default:
        /// a healthy working set depends on how much this node syncs, so Fabric
        /// declares no ceiling unless an operator measures one and asks for it.
        #[arg(long)]
        memory_max_mb: Option<u64>,
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
                            )?;
                        }
                        response => bail!("unexpected daemon response: {response:?}"),
                    }
                }
                Commands::Peers => {
                    let book = PeerBook::load(&home)?;
                    for peer in book.peers() {
                        match &peer.name {
                            Some(name) => println!("{}\t{}", peer.id, name),
                            None => println!("{}", peer.id),
                        }
                    }
                }
                Commands::ReloadPeers => {
                    send_control(&home, ControlRequest::ReloadPeers).await?;
                    println!("reloaded");
                }
                Commands::Add {
                    nodeid,
                    name,
                    addr_json,
                } => {
                    let id = parse_node_id(&nodeid)?;
                    let addr = parse_addr_json(addr_json.as_deref(), id)?;
                    let mut book = PeerBook::load(&home)?;
                    book.add(id, name, addr);
                    book.save(&home)?;
                    let _ = send_control(&home, ControlRequest::ReloadPeers).await;
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
                    } => {
                        service::install(
                            &home,
                            ServiceInstallOptions {
                                allow_shell: allow_override(allow_shell, no_allow_shell),
                                allow_exec: allow_override(allow_exec, no_allow_exec),
                                memory_max_mb,
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
                    if entry.missing == 0 && entry.unexpected == 0 && entry.mismatched == 0 {
                        println!(
                            "{}\t{}\t{}\tpeers={}\tpresent={present}\ttombstones={}\tobserved={}\tdrift=clean",
                            entry.name,
                            entry.folder,
                            entry.policy,
                            entry.peers,
                            entry.tombstones,
                            entry.observed,
                        );
                    } else {
                        println!(
                            "{}\t{}\t{}\tpeers={}\tpresent={present}\ttombstones={}\tobserved={}\tdrift=WARNING missing={} unexpected={} mismatched={}",
                            entry.name,
                            entry.folder,
                            entry.policy,
                            entry.peers,
                            entry.tombstones,
                            entry.observed,
                            entry.missing,
                            entry.unexpected,
                            entry.mismatched,
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
            drift: entry.missing != 0 || entry.unexpected != 0 || entry.mismatched != 0,
            missing: entry.missing,
            unexpected: entry.unexpected,
            mismatched: entry.mismatched,
        }
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
mod sync_ls_tests {
    use super::*;
    use fabric::control::SyncEntryStatus;

    fn status() -> SyncEntryStatus {
        SyncEntryStatus {
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
                "mismatched": 0
            })
        );
    }

    #[test]
    fn sync_ls_accepts_legacy_control_present_count() {
        let mut status = status();
        status.present = 0;
        assert_eq!(logical_present(&status), 40);
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
    print_peer_reachability(peers);
    Ok(())
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
