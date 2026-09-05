//! The `fabric doctor` exit contract for people and fleet scripts.

use std::process::Command;

use anyhow::{Context, Result};
use fabric::doctor::{Finding, Verdict, exit_code};

fn fabric_bin() -> &'static str {
    env!("CARGO_BIN_EXE_fabric")
}

#[test]
fn an_unknown_answer_does_not_share_claps_usage_error_code() -> Result<()> {
    let unknown = exit_code(&[Finding {
        check: "versions".to_string(),
        verdict: Verdict::Unknown,
        detail: "could not ask the peer".to_string(),
        action: None,
    }]);
    let usage = Command::new(fabric_bin())
        .args(["doctor", "unexpected"])
        .output()
        .context("failed to run fabric with an invalid doctor argument")?;

    assert_eq!(unknown, 3);
    assert_eq!(usage.status.code(), Some(2));
    assert_ne!(Some(unknown), usage.status.code());
    Ok(())
}

#[test]
fn doctor_help_documents_every_exit_code() -> Result<()> {
    let output = Command::new(fabric_bin())
        .args(["doctor", "--help"])
        .output()
        .context("failed to run fabric doctor --help")?;
    let help = String::from_utf8(output.stdout)?;

    for expected in [
        "0 no attention needed",
        "1 problem or setup",
        "2 command-line usage error",
        "3 unknown",
    ] {
        assert!(help.contains(expected), "the help never says {expected:?}");
    }
    Ok(())
}
