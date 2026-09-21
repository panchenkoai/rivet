//! Stamps the commit into `--version` (`rivet 0.27.0 (abc1234)`): a partner's
//! bug report names a version, and one version ships from many pre-release
//! builds. Falls back to `RIVET_GIT_SHA` (a Docker build has no `.git`) and then
//! to `unknown`, never failing the build.

use std::process::Command;

fn main() {
    let sha = std::env::var("RIVET_GIT_SHA")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "--short=9", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=RIVET_GIT_SHA={sha}");
    println!("cargo:rerun-if-env-changed=RIVET_GIT_SHA");
    // `--git-path` resolves inside a worktree too, where `.git` is a file.
    for name in ["HEAD", "index"] {
        if let Some(p) = Command::new("git")
            .args(["rev-parse", "--git-path", name])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|p| std::path::Path::new(p).exists())
        {
            println!("cargo:rerun-if-changed={p}");
        }
    }
}
