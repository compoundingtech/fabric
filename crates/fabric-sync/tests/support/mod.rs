//! Where the tests find the `fabric` binary. It belongs to the core package,
//! so `CARGO_BIN_EXE_fabric` is not set for this crate's tests; build it on
//! first use with the same cargo and target directory this test run uses, and
//! find it beside the test binary's own directory.

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
