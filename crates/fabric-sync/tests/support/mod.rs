//! What the tests need from the daemon's side, which the companion itself
//! never links: the `fabric` binary, and a daemon started in process beside a
//! companion.
//!
//! The binary belongs to the core package, so `CARGO_BIN_EXE_fabric` is not
//! set for this crate's tests; build it on first use with the same cargo and
//! target directory this test run uses, and find it beside the test binary's
//! own directory.

#![allow(dead_code)]

use std::{
    path::PathBuf,
    process::Command,
    sync::OnceLock,
};

pub fn fabric_bin() -> &'static str {
    static PATH: OnceLock<String> = OnceLock::new();
    PATH.get_or_init(|| {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let status = Command::new(cargo)
            .args(["build", "-p", "fabric", "--bin", "fabric"])
            .status()
            .expect("cargo builds the fabric binary");
        assert!(status.success(), "building the fabric binary failed");
        let exe = std::env::current_exe().expect("the test binary has a path");
        // <target>/<profile>/deps/<test>-<hash> -> <target>/<profile>/fabric
        let profile_dir = exe
            .parent()
            .and_then(|deps| deps.parent())
            .expect("the test binary lives under a profile directory");
        let path: PathBuf = profile_dir.join("fabric");
        assert!(path.exists(), "no fabric binary at {}", path.display());
        path.display().to_string()
    })
}

/// A deployed fabric binary from before the process boundary, for the mixed
/// matrix: `FABRIC_OLD_BIN` names it. `None` skips those cases loudly.
pub fn old_fabric_bin() -> Option<String> {
    std::env::var("FABRIC_OLD_BIN").ok().filter(|path| !path.is_empty())
}

/// A daemon and its companion, started together in one process.
///
/// The harness shape for every test that needs sync: the daemon delegates, the
/// companion in this process owns it, and both go through the same two
/// sockets production uses. Derefs to the node so a test reads ids, addresses
/// and state as before; `shutdown` stops the companion first so the lease is
/// free before the daemon goes.
pub struct HostedNode {
    node: fabric::daemon::FabricNode,
    companion: Option<fabric_sync::CompanionHandle>,
}

impl HostedNode {
    pub async fn start(home: fabric_config::FabricHome) -> anyhow::Result<Self> {
        Self::start_with_options(home, fabric::daemon::DaemonOptions::default()).await
    }

    pub async fn start_with_options(
        home: fabric_config::FabricHome,
        options: fabric::daemon::DaemonOptions,
    ) -> anyhow::Result<Self> {
        let node = fabric::daemon::FabricNode::start_with_daemon_options(home.clone(), options).await?;
        let handle = fabric_sync::companion::start(home).await?;
        handle
            .wait_until_active(std::time::Duration::from_secs(30))
            .await?;
        Ok(Self {
            node,
            companion: Some(handle),
        })
    }

    pub fn node(&self) -> &fabric::daemon::FabricNode {
        &self.node
    }

    pub fn companion(&self) -> Option<&fabric_sync::CompanionHandle> {
        self.companion.as_ref()
    }

    /// The engine, wherever it runs, for a test that drives a pass on purpose.
    pub async fn engine(
        &self,
    ) -> Option<std::sync::Arc<fabric_sync::SyncEngine<fabric_sync::IpcSyncTransport>>> {
        match &self.companion {
            Some(companion) => companion.engine().await,
            None => None,
        }
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        if let Some(companion) = self.companion {
            companion.shutdown().await?;
        }
        self.node.shutdown().await
    }
}

impl std::ops::Deref for HostedNode {
    type Target = fabric::daemon::FabricNode;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}
