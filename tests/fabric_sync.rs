//! The dormant companion's diagnostic contract.

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    os::unix::{fs::PermissionsExt, net::UnixListener},
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use fabric::{
    config::FabricHome,
    control::{ControlRequest, ControlResponse},
    sync::{SyncOwnerLease, SyncPaths, ipc},
};

fn fabric_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric")
}

fn sync_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric-sync")
}

#[derive(Debug, PartialEq, Eq)]
struct FileSnapshot {
    mode: u32,
    bytes: Option<Vec<u8>>,
}

fn snapshot(root: &Path) -> Result<BTreeMap<PathBuf, FileSnapshot>> {
    fn walk(root: &Path, path: &Path, out: &mut BTreeMap<PathBuf, FileSnapshot>) -> Result<()> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            let relative = path.strip_prefix(root)?.to_path_buf();
            let bytes = metadata
                .is_file()
                .then(|| std::fs::read(&path))
                .transpose()?;
            out.insert(
                relative,
                FileSnapshot {
                    mode: metadata.permissions().mode(),
                    bytes,
                },
            );
            if metadata.is_dir() {
                walk(root, &path, out)?;
            }
        }
        Ok(())
    }

    let mut files = BTreeMap::new();
    walk(root, root, &mut files)?;
    Ok(files)
}

#[test]
fn both_release_binaries_report_the_same_version() -> Result<()> {
    let fabric = Command::new(fabric_bin()).arg("--version").output()?;
    let sync = Command::new(sync_bin()).arg("--version").output()?;
    assert!(fabric.status.success());
    assert!(sync.status.success());
    assert_eq!(fabric.stdout, sync.stdout);
    Ok(())
}

#[test]
fn check_validates_the_running_embedded_owner_without_mutating_state() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let home = FabricHome::new(temp.path());
    let folder = temp.path().join("shared");
    std::fs::create_dir_all(&folder)?;
    std::fs::write(
        home.syncs_path(),
        format!(
            "[[sync]]\nname = \"shared\"\nfolder = {:?}\npeers = \"*\"\npolicy = \"catalog\"\n",
            folder
        ),
    )?;
    let paths = SyncPaths::new(home.syncs_path(), home.root().join("sync"));
    let lease = SyncOwnerLease::acquire(&paths)?;
    std::fs::create_dir_all(home.root().join("run"))?;
    let listener = UnixListener::bind(home.control_socket_path())?;
    let server = std::thread::spawn(move || -> Result<()> {
        let (stream, _) = listener.accept()?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let request: ControlRequest = serde_json::from_str(&line)?;
        assert!(matches!(request, ControlRequest::SyncIpcCompatibility));
        let response = ControlResponse::SyncIpcCompatibility {
            version: fabric::version_string(),
            sync_ipc_magic: ipc::IPC_MAGIC.to_string(),
            sync_ipc_version: ipc::IPC_VERSION,
            owner: "embedded".to_string(),
        };
        let stream = reader.get_mut();
        serde_json::to_writer(&mut *stream, &response)?;
        stream.write_all(b"\n")?;
        Ok(())
    });
    let before = snapshot(temp.path())?;

    let output = Command::new(sync_bin())
        .args([
            "--home",
            temp.path().to_str().context("non-UTF-8 temp path")?,
            "--check",
        ])
        .output()?;

    server.join().expect("compatibility server panicked")?;
    assert!(
        output.status.success(),
        "check failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("owner\theld"));
    assert!(stdout.contains("daemon\tok"));
    assert_eq!(
        snapshot(temp.path())?,
        before,
        "--check changed the fabric home"
    );
    drop(lease);
    Ok(())
}
