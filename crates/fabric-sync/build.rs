use std::{env, fs, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    emit_git_rerun_files();

    let sha = env::var("GITHUB_SHA")
        .ok()
        .and_then(|sha| short_sha(&sha))
        .or_else(git_sha)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=FABRIC_BUILD_SHA={sha}");
}

fn emit_git_rerun_files() {
    let Some(head_path) = git_path("HEAD") else {
        println!("cargo:rerun-if-changed=.git/HEAD");
        return;
    };
    println!("cargo:rerun-if-changed={head_path}");

    let Ok(head) = fs::read_to_string(&head_path) else {
        return;
    };
    let Some(reference) = head.strip_prefix("ref:").map(str::trim) else {
        return;
    };
    if let Some(ref_path) = git_path(reference) {
        println!("cargo:rerun-if-changed={ref_path}");
    }
    if let Some(packed_refs) = git_path("packed-refs") {
        println!("cargo:rerun-if-changed={packed_refs}");
    }
}

fn git_path(path: &str) -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-path", path])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8(output.stdout).ok()?;
    let path = path.trim();
    (!path.is_empty()).then(|| path.to_string())
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
