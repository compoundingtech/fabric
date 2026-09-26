//! The files and local contracts the fabric daemon and its sync companion
//! share: where a fabric home keeps things, what `syncs.toml` says, the bridge
//! between the two processes, and the version both must be built at.
//!
//! The daemon depends on this crate, and so does `fabric-sync`, which depends
//! on nothing else of the daemon's. Which peer may use which service stays the
//! daemon's decision; nothing here reads a grant.

pub mod daemon_control;
mod home;
pub mod log;
pub mod sync;

pub use home::FabricHome;

/// The build both processes report, `<version>+<commit>`. A companion refuses
/// to attach to a daemon built from anything else.
pub fn version_string() -> String {
    format!(
        "{}+{}",
        env!("CARGO_PKG_VERSION"),
        option_env!("FABRIC_BUILD_SHA").unwrap_or("unknown")
    )
}
