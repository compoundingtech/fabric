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
#[command(group(ArgGroup::new("action").required(true).multiple(false).args(["version", "check", "standby"])))]
struct Cli {
    /// Print the build version.
    #[arg(long)]
    version: bool,

    /// Validate sync config, state ownership, and daemon compatibility.
    #[arg(long)]
    check: bool,

    /// Run the supervised compatibility standby.
    #[arg(long, hide = true)]
    standby: bool,

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
    let home = FabricHome::resolve(cli.home)?;
    if cli.check {
        return check(home).await;
    }
    standby(home).await
}

async fn standby(home: FabricHome) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
    let mut previous = String::new();
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = interval.tick() => {
                let state = standby_heartbeat(&home).await;
                if state != previous {
                    println!("runtime\t{state}");
                    previous = state;
                }
            }
        }
    }
}

async fn standby_heartbeat(home: &FabricHome) -> String {
    let request = ControlRequest::SyncCompanionHello {
        version: fabric::version_string(),
        sync_ipc_magic: ipc::IPC_MAGIC.to_string(),
        sync_ipc_version: ipc::IPC_VERSION,
    };
    match send_control(home, request).await {
        Ok(ControlResponse::SyncIpcCompatibility { owner, .. }) if owner == "embedded" => {
            "standby; daemon owns embedded sync".to_string()
        }
        Ok(ControlResponse::SyncIpcCompatibility { owner, .. }) => {
            format!("unavailable; daemon granted unsupported owner {owner}")
        }
        Ok(response) => format!("unavailable; unexpected daemon response {response:?}"),
        Err(error) => format!("unavailable; {error:#}"),
    }
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
