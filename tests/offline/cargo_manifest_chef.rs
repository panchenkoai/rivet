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
