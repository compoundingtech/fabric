use std::{
    fs::OpenOptions,
    io::IsTerminal,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use clap::{CommandFactory, Parser, Subcommand};
use fabric::{
    config::{
        FabricHome, PeerBook, generate_identity_file, load_or_create_identity, parse_addr_json,
        parse_node_id,
    },
    control::{ControlRequest, ControlResponse, PeerReachability},
    daemon::{FabricNode, run_daemon, send_control},
    shell::{self, ServerFrame},
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
    /// Expose a local Unix socket service to trusted peers under an ALPN protocol.
    Expose {
        protocol: String,
        #[arg(long)]
        socket: PathBuf,
    },
    /// Create a local Unix socket that tunnels to a peer's exposed protocol.
    Dial { peer: String, protocol: String },
    /// Round-trip a random nonce through a peer's built-in echo protocol.
    Ping { peer: String },
    /// Open an interactive remote shell on a trusted peer.
    Shell { peer: String },
    /// Internal foreground daemon entrypoint.
    #[command(hide = true)]
    Daemon {
        #[arg(long)]
        allow_shell: bool,
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
                            peers,
                        } => {
                            print_status(
                                &version,
                                &node_id,
                                &endpoint_addr,
                                &exposed_protocols,
                                &dial_sockets,
                                allow_shell,
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
                } => {
                    if foreground {
                        let node = FabricNode::start_with_options(home, allow_shell).await?;
                        let peers = node.state().peer_reachability().await;
                        print_startup_reachability(&peers);
                        node.wait().await?;
                    } else {
                        spawn_daemon(&home, allow_shell).await?;
                        print_daemon_reachability(&home).await?;
                    }
                }
                Commands::Down => {
                    send_control(&home, ControlRequest::Shutdown).await?;
                    println!("stopped");
                }
                Commands::Restart {
                    allow_shell,
                    no_allow_shell,
                } => {
                    let allow_shell = allow_shell_override(allow_shell, no_allow_shell);
                    match send_control(&home, ControlRequest::Restart { allow_shell }).await? {
                        ControlResponse::Restarting { log, allow_shell } => {
                            println!("restart scheduled");
                            println!("log\t{}", log.display());
                            println!("allow-shell\t{allow_shell}");
                        }
                        response => bail!("unexpected daemon response: {response:?}"),
                    }
                }
                Commands::Expose { protocol, socket } => {
                    send_control(&home, ControlRequest::Expose { protocol, socket }).await?;
                    println!("exposed");
                }
                Commands::Dial { peer, protocol } => {
                    match send_control(&home, ControlRequest::Dial { peer, protocol }).await? {
                        ControlResponse::Dial { socket } => println!("{}", socket.display()),
                        response => bail!("unexpected daemon response: {response:?}"),
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
                Commands::Shell { peer } => {
                    let socket = match send_control(&home, ControlRequest::Shell { peer }).await? {
                        ControlResponse::Shell { socket } => socket,
                        response => bail!("unexpected daemon response: {response:?}"),
                    };
                    let code = run_shell_client(&socket).await?;
                    std::process::exit(code);
                }
                Commands::Daemon { allow_shell } => {
                    run_daemon(home, allow_shell).await?;
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

fn print_status(
    version: &str,
    node_id: &str,
    endpoint_addr: &serde_json::Value,
    exposed_protocols: &[String],
    dial_sockets: &[PathBuf],
    allow_shell: bool,
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

fn allow_shell_override(allow_shell: bool, no_allow_shell: bool) -> Option<bool> {
    if allow_shell {
        Some(true)
    } else if no_allow_shell {
        Some(false)
    } else {
        None
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

    let start_result = spawn_daemon(home, allow_shell).await;
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
        if send_control(home, ControlRequest::Status).await.is_err() {
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
    let (mut read, mut write) = stream.into_split();
    let _raw_mode = RawModeGuard::enable_if_terminal()?;
    let (cols, rows) = terminal_size();
    shell::write_client_resize(&mut write, rows, cols).await?;

    let stdin_task = tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 8192];
        loop {
            let read = stdin.read(&mut buf).await?;
            if read == 0 {
                shell::write_client_eof(&mut write).await?;
                return Ok::<(), anyhow::Error>(());
            }
            shell::write_client_stdin(&mut write, &buf[..read]).await?;
        }
    });

    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    let mut exit_code = 1;

    while let Some(frame) = shell::read_server_frame(&mut read).await? {
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
            ServerFrame::Exit(code) => {
                exit_code = normalize_exit_code(code);
                break;
            }
        }
    }

    stdin_task.abort();
    let _ = stdin_task.await;
    stdout.flush().await?;
    stderr.flush().await?;
    Ok(exit_code)
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

struct RawModeGuard {
    enabled: bool,
}

impl RawModeGuard {
    fn enable_if_terminal() -> Result<Self> {
        if std::io::stdin().is_terminal() {
            crossterm::terminal::enable_raw_mode()?;
            Ok(Self { enabled: true })
        } else {
            Ok(Self { enabled: false })
        }
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if self.enabled {
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
}

async fn spawn_daemon(home: &FabricHome, allow_shell: bool) -> Result<()> {
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
    if allow_shell {
        command.arg("--allow-shell");
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
