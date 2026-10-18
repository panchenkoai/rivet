//! Guard: the crate manifest must stay parseable by `cargo-chef`, whose TOML
//! parser (`cargo-manifest`) is spec-strict and rejects **multi-line inline
//! tables** that `cargo build` happily tolerates. A `dep = {` opened on one
//! line and closed on the next breaks only the release Docker image build
//! (`cargo chef prepare --recipe-path recipe.json`) — every other build stays
//! green, so it sails through local checks and CI and only surfaces at release.
//!
//! This is exactly what bit 0.16.0: both `linux/amd64` and `linux/arm64` Docker
//! builds failed with `invalid inline table` at the `postgres = {` line, *after*
//! crates.io, the binaries, and the GitHub release had already published — an
//! un-re-runnable, half-published release. The fix is trivial (one line each);
//! the cost was a whole patch release. This test makes the regression loud and
//! local instead.

use std::path::PathBuf;

/// Every TOML inline table (`key = { … }`) must open and close on the same
/// line. A trailing `= {` with nothing after it begins a multi-line inline
/// table, which the TOML spec forbids and `cargo-chef` rejects.
#[test]
fn cargo_toml_has_no_multiline_inline_tables() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("read Cargo.toml");

    let offenders: Vec<String> = text
        .lines()
        .enumerate()
        .filter(|(_, line)| {
            let t = line.trim_start();
            if t.starts_with('#') {
                return false; // a commented-out example is not a real table
            }
            // `<key> = {` followed only by whitespace to end-of-line: an inline
            // table opened but not closed here. A closed single-line table
            // (`= { version = "1" }`) leaves non-whitespace after the `= {`.
            matches!(t.split_once("= {"), Some((_, rest)) if rest.trim().is_empty())
        })
        .map(|(i, line)| format!("  line {}: {}", i + 1, line.trim()))
        .collect();

    assert!(
        offenders.is_empty(),
        "Cargo.toml has multi-line inline table(s) — collapse each onto one line.\n\
         `cargo build` tolerates them, but `cargo chef prepare` in the release Docker \
         build rejects them as `invalid inline table` (this is what broke the 0.16.0 \
         image build):\n{}",
        offenders.join("\n")
    );
}

/// A link target that is absolute, or whose `..` climbs above the repository root.
fn link_leaves_the_repo(link: &std::path::Path, target: &std::path::Path) -> bool {
    use std::path::Component;
    if target.is_absolute() {
        return true;
    }
    let resolved = link
        .parent()
        .unwrap_or(std::path::Path::new(""))
        .join(target);
    let mut depth: i64 = 0;
    for c in resolved.components() {
        match c {
            Component::ParentDir => depth -= 1,
            Component::Normal(_) => depth += 1,
            _ => {}
        }
        if depth < 0 {
            return true;
        }
    }
    false
}

/// Every symlink git tracks points inside the repository by a relative path. `cargo publish`
/// archives what git lists, so a tracked link to an absolute path packs on the machine that has
/// the target and fails on a clean runner: 0.32.0's first tag stopped at `failed to open for
/// archiving: .gate-baseline`, a link to one machine's gate baseline.
#[test]
fn every_tracked_symlink_is_relative_and_stays_in_the_repo() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out = std::process::Command::new("git")
        .args(["ls-files", "-s"])
        .current_dir(&root)
        .output()
        .expect("run git ls-files");
    let listing = String::from_utf8_lossy(&out.stdout);
    let mut offenders = Vec::new();
    for line in listing.lines().filter(|l| l.starts_with("120000 ")) {
        let Some((_, path)) = line.split_once('\t') else {
            continue;
        };
        let link = std::path::Path::new(path);
        let target = match std::fs::read_link(root.join(link)) {
            Ok(t) => t,
            Err(e) => {
                offenders.push(format!(
                    "  {path}: tracked as a link but unreadable here ({e})"
                ));
                continue;
            }
        };
        if link_leaves_the_repo(link, &target) {
            offenders.push(format!("  {path} -> {}", target.display()));
        }
    }
    assert_eq!(
        offenders,
        Vec::<String>::new(),
        "tracked symlinks must be relative and stay inside the repository (cargo publish \
         archives them on a clean runner)",
    );
}

#[test]
fn a_link_leaves_the_repo_when_absolute_or_climbing_past_the_root() {
    use std::path::Path;
    assert!(link_leaves_the_repo(
        Path::new(".gate-baseline"),
        Path::new("/Users/x/rivet/.gate-baseline")
    ));
    assert!(link_leaves_the_repo(
        Path::new("dev/a"),
        Path::new("../../outside")
    ));
    assert!(!link_leaves_the_repo(
        Path::new("dev/matrices/surface/cli"),
        Path::new("../../cli_matrix")
    ));
    assert!(!link_leaves_the_repo(Path::new("dev/a"), Path::new("b")));
}
