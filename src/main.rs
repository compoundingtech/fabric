use std::{
    fs::OpenOptions,
    path::PathBuf,
    process::{Command as ProcessCommand, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use fabric::{
    config::{FabricHome, PeerBook, load_or_create_identity, parse_addr_json, parse_node_id},
    control::{ControlRequest, ControlResponse},
    daemon::{run_daemon, send_control},
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
    /// Print this node's stable iroh NodeID.
    Id,
    /// Print the running daemon's current EndpointAddr as JSON.
    Addr,
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
    /// Internal foreground daemon entrypoint.
    #[command(hide = true)]
    Daemon,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let home = FabricHome::resolve(cli.home)?;

    match cli.command {
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
                run_daemon(home).await?;
            } else {
                spawn_daemon(&home).await?;
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
        Commands::Daemon => {
            run_daemon(home).await?;
        }
    }

    Ok(())
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
