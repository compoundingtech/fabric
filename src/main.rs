use std::{
    fs::OpenOptions,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use fabric::{
    config::{
        FabricHome, PeerBook, generate_identity_file, load_or_create_identity, parse_addr_json,
        parse_node_id,
    },
    control::{ControlRequest, ControlResponse, PeerReachability},
    daemon::{FabricNode, run_daemon, send_control},
};

#[derive(Debug, Parser)]
#[command(name = "fabric")]
#[command(about = "Local socket facade for iroh-backed cross-machine transports")]
struct Cli {
    #[arg(long, global = true)]
    home: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
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
    },
    /// Stop the local fabric daemon.
    Down,
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
    /// Internal foreground daemon entrypoint.
    #[command(hide = true)]
    Daemon,
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

    match cli.command {
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
                            node_id,
                            endpoint_addr,
                            exposed_protocols,
                            dial_sockets,
                            peers,
                        } => {
                            print_status(
                                &node_id,
                                &endpoint_addr,
                                &exposed_protocols,
                                &dial_sockets,
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
                Commands::Up { foreground } => {
                    if foreground {
                        let node = FabricNode::start(home).await?;
                        let peers = node.state().peer_reachability().await;
                        print_startup_reachability(&peers);
                        node.wait().await?;
                    } else {
                        spawn_daemon(&home).await?;
                        print_daemon_reachability(&home).await?;
                    }
                }
                Commands::Down => {
                    send_control(&home, ControlRequest::Shutdown).await?;
                    println!("stopped");
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
                Commands::Daemon => {
                    run_daemon(home).await?;
                }
            }
        }
    }

    Ok(())
}

fn print_status(
    node_id: &str,
    endpoint_addr: &serde_json::Value,
    exposed_protocols: &[String],
    dial_sockets: &[PathBuf],
    peers: &[PeerReachability],
) -> Result<()> {
    println!("node\t{node_id}");
    println!("addr\t{}", serde_json::to_string(endpoint_addr)?);
    println!("exposed\t{}", joined_or_dash(exposed_protocols));
    let dials: Vec<String> = dial_sockets
        .iter()
        .map(|path| path.display().to_string())
        .collect();
    println!("dials\t{}", joined_or_dash(&dials));
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

async fn spawn_daemon(home: &FabricHome) -> Result<()> {
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
    ProcessCommand::new(exe)
        .arg("--home")
        .arg(home.root())
        .arg("daemon")
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
