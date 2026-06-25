//! Driving the `rivet` / `rivet-mcp` binaries from integration tests, plus
//! light helpers for writing the YAML config and discovering produced files.

#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Command, Output};

/// Absolute path to the `rivet` binary built for this integration test.
pub const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");

/// Absolute path to the `rivet-mcp` binary built for this integration test.
pub const RIVET_MCP_BIN: &str = env!("CARGO_BIN_EXE_rivet-mcp");

/// Write `yaml` to `<tmpdir>/rivet.yaml` and return the path.  The tempdir is
/// returned too so the caller can keep it alive for the duration of the run.
pub fn write_config(tmpdir: &tempfile::TempDir, yaml: &str) -> PathBuf {
    let path = tmpdir.path().join("rivet.yaml");
    std::fs::write(&path, yaml).expect("write rivet config");
    path
}

/// Run `rivet <args...>` and capture stdout/stderr.  Panics if the process
/// cannot be spawned (which indicates a build-time problem, not a test
/// failure).
pub fn run_rivet(args: &[&str]) -> Output {
    Command::new(RIVET_BIN)
        .args(args)
        .output()
        .expect("spawn rivet binary")
}

/// Like `run_rivet` but sets `RUST_LOG=warn` so that `log::warn!` output is
/// visible in stderr.  Use this when a test needs to assert on warning messages
/// emitted via the log crate (plan validation warnings, quality warnings, etc.).
pub fn run_rivet_with_warn_log(args: &[&str]) -> Output {
    Command::new(RIVET_BIN)
        .args(args)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn rivet binary")
}

/// Convenience: `rivet run --config <path> --export <name>` and return the
/// captured output.  Caller is responsible for asserting exit code / contents.
pub fn run_rivet_export(config_path: &std::path::Path, export_name: &str) -> Output {
    run_rivet(&[
        "run",
        "--config",
        config_path.to_str().unwrap(),
        "--export",
        export_name,
    ])
}

/// Like [`run_rivet_export`] but with extra environment variables set on the
/// rivet subprocess — used by the value-checksum guard to flip
/// `RIVET_VALUE_CHECKSUM` on for a single export.
pub fn run_rivet_export_with_env(
    config_path: &std::path::Path,
    export_name: &str,
    env: &[(&str, &str)],
) -> Output {
    let mut cmd = Command::new(RIVET_BIN);
    cmd.args([
        "run",
        "--config",
        config_path.to_str().unwrap(),
        "--export",
        export_name,
    ]);
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.output().expect("spawn rivet binary")
}

/// Collect every file with the given extension under `dir` (non-recursive).
/// Useful for locating the timestamped output rivet just wrote.
pub fn files_with_extension(dir: &std::path::Path, ext: &str) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return vec![];
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|e| e == ext))
        .map(|e| e.path())
        .collect()
}
