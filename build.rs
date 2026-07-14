use std::{env, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-changed=.git/HEAD");

    let sha = env::var("GITHUB_SHA")
        .ok()
        .and_then(|sha| short_sha(&sha))
        .or_else(git_sha)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=FABRIC_BUILD_SHA={sha}");
}

fn git_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?;
    short_sha(sha.trim())
}

fn short_sha(sha: &str) -> Option<String> {
    let sha = sha.trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.chars().take(7).collect())
    }
}
