use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{ArgGroup, Args, Parser, Subcommand};
use fabric_config::{
    FabricHome,
    daemon_control::{self, Request as ControlRequest, Response as ControlResponse},
    sync::{SyncBook, cli::SyncCommands, ipc},
};
use fabric_sync::{SyncOwnerLease, SyncOwnerLeaseState, SyncPaths, companion};

mod commands;

/// The process modes. Flattened into both parsers below.
#[derive(Debug, Args)]
struct Modes {
    /// Print the build version.
    #[arg(long)]
    version: bool,

    /// Validate sync config, state ownership, and daemon compatibility.
    #[arg(long)]
    check: bool,

    /// Run under the service manager. Standby while the daemon owns embedded
    /// sync; own sync the moment the daemon delegates it.
    #[arg(long, hide = true)]
    standby: bool,

    /// Use an isolated fabric state root.
    #[arg(long, global = true)]
    home: Option<PathBuf>,
}

/// What the service manager runs, and everything but the sync commands.
///
/// It does not know the `sync` subcommand on purpose. Building that command
/// tree at startup kept about 180 KiB more resident in a companion that runs
/// all day and never uses it.
#[derive(Debug, Parser)]
#[command(name = "fabric-sync")]
#[command(about = "The fabric file-sync companion process")]
#[command(
    after_help = "fabric-sync sync <COMMAND> runs the `fabric sync` commands that list \
                        entries and stage and publish changes."
)]
#[command(group(ArgGroup::new("action").required(true).multiple(false).args(["version", "check", "standby"])))]
struct Cli {
    #[command(flatten)]
    modes: Modes,
}

/// The same, with the sync commands, parsed only when `sync` is asked for.
#[derive(Debug, Parser)]
#[command(name = "fabric-sync")]
#[command(about = "The fabric file-sync companion process")]
#[command(group(ArgGroup::new("action").required(true).multiple(false).args(["version", "check", "standby"])))]
#[command(subcommand_negates_reqs = true)]
struct CliWithSync {
    #[command(flatten)]
    modes: Modes,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List sync entries, and stage and publish changes to synced files; the
    /// same commands as `fabric sync`, which hands them here.
    Sync {
        #[command(subcommand)]
        command: SyncCommands,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let modes = if std::env::args_os().skip(1).any(|arg| arg == "sync") {
        let cli = CliWithSync::parse();
        if let Some(Command::Sync { command }) = cli.command {
            let home = FabricHome::resolve(cli.modes.home)?;
            return commands::run(&home, command).await;
        }
        cli.modes
    } else {
        Cli::parse().modes
    };
    if modes.version {
        println!("{}", fabric_config::version_string());
        return Ok(());
    }
    let home = FabricHome::resolve(modes.home)?;
    if modes.check {
        return check(home).await;
    }
    serve(home).await
}

/// The supervised mode. One line to stdout per phase change, so a service log
/// says what the process was doing without being verbose while it does it.
async fn serve(home: FabricHome) -> Result<()> {
    companion::init_companion_tracing(&home)?;
    let handle = companion::start(home).await?;
    let mut previous = String::new();
    let mut ticks = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = shutdown_signal() => break,
            _ = ticks.tick() => {
                let state = handle.phase().describe();
                if state != previous {
                    println!("runtime\t{state}");
                    previous = state;
                }
            }
        }
    }
    handle.shutdown().await
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn check(home: FabricHome) -> Result<()> {
    let paths = SyncPaths::new(home.syncs_path(), home.root().join("sync"));
    SyncBook::load_path(paths.config_path())?;
    println!("config\tok\t{}", paths.config_path().display());

    let lease = SyncOwnerLease::probe(&paths)?;
    println!("state\tok\t{}", paths.state_root().display());
    println!("owner\t{}", lease_name(lease));

    let response = daemon_control::send(&home, ControlRequest::SyncIpcCompatibility).await?;
    let ControlResponse::SyncIpcCompatibility {
        version,
        sync_ipc_magic,
        sync_ipc_version,
        owner,
        ..
    } = response
    else {
        bail!("the daemon returned the wrong sync compatibility response");
    };
    let local = fabric_config::version_string();
    if version != local {
        bail!("fabric-sync is {local}, but the daemon is {version}");
    }
    if sync_ipc_magic != ipc::IPC_MAGIC || sync_ipc_version != ipc::IPC_VERSION {
        bail!(
            "the daemon sync IPC is {sync_ipc_magic} version {sync_ipc_version}; expected {} version {}",
            ipc::IPC_MAGIC,
            ipc::IPC_VERSION
        );
    }
    if owner == "embedded" && lease != SyncOwnerLeaseState::Held {
        bail!("the daemon reports embedded sync ownership, but the owner lease is not held");
    }
    println!("daemon\tok\t{version}");
    println!("ipc\tok\t{sync_ipc_magic}\t{sync_ipc_version}");
    println!("daemon owner\t{owner}");
    Ok(())
}

fn lease_name(state: SyncOwnerLeaseState) -> &'static str {
    match state {
        SyncOwnerLeaseState::Absent => "absent",
        SyncOwnerLeaseState::Available => "available",
        SyncOwnerLeaseState::Held => "held",
    }
}
