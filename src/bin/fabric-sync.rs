use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{ArgGroup, Parser};
use fabric::{
    config::FabricHome,
    control::{ControlRequest, ControlResponse},
    daemon::send_control,
    sync::{SyncBook, SyncOwnerLease, SyncOwnerLeaseState, SyncPaths, ipc},
};

#[derive(Debug, Parser)]
#[command(name = "fabric-sync")]
#[command(about = "Diagnostic companion for fabric file sync")]
#[command(group(ArgGroup::new("action").required(true).multiple(false).args(["version", "check"])))]
struct Cli {
    /// Print the build version.
    #[arg(long)]
    version: bool,

    /// Validate sync config, state ownership, and daemon compatibility.
    #[arg(long)]
    check: bool,

    /// Use an isolated fabric state root.
    #[arg(long, global = true)]
    home: Option<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.version {
        println!("{}", fabric::version_string());
        return Ok(());
    }
    check(FabricHome::resolve(cli.home)?).await
}

async fn check(home: FabricHome) -> Result<()> {
    let paths = SyncPaths::new(home.syncs_path(), home.root().join("sync"));
    SyncBook::load_path(paths.config_path())?;
    println!("config\tok\t{}", paths.config_path().display());

    let lease = SyncOwnerLease::probe(&paths)?;
    println!("state\tok\t{}", paths.state_root().display());
    println!("owner\t{}", lease_name(lease));

    let response = send_control(&home, ControlRequest::SyncIpcCompatibility).await?;
    let ControlResponse::SyncIpcCompatibility {
        version,
        sync_ipc_magic,
        sync_ipc_version,
        owner,
    } = response
    else {
        bail!("the daemon returned the wrong sync compatibility response");
    };
    let local = fabric::version_string();
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
    Ok(())
}

fn lease_name(state: SyncOwnerLeaseState) -> &'static str {
    match state {
        SyncOwnerLeaseState::Absent => "absent",
        SyncOwnerLeaseState::Available => "available",
        SyncOwnerLeaseState::Held => "held",
    }
}
