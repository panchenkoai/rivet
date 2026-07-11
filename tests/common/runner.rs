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

/// `run_rivet` with extra environment variables (fault hooks, log levels).
pub fn run_rivet_env(args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(RIVET_BIN);
    cmd.args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.output().expect("spawn rivet binary")
}

/// Spawn `rivet run --config <cfg>` and wait up to `timeout` for it to exit on
/// its own. Returns the elapsed time if it terminated within the budget (and
/// asserts a clean exit), or `None` if it had to be killed. This is what makes a
/// `until_current`-terminates-under-load assertion possible without risking a
/// suite-wide hang: a drain loop that never reaches its stop condition fails the
/// test (returns `None`) instead of blocking `output()` forever.
pub fn run_rivet_bounded(
    cfg: &std::path::Path,
    timeout: std::time::Duration,
) -> Option<std::time::Duration> {
    let start = std::time::Instant::now();
    let mut child = Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .spawn()
        .expect("spawn rivet binary");
    loop {
        if let Some(status) = child.try_wait().expect("try_wait rivet") {
            assert!(status.success(), "bounded rivet run exited non-zero");
            return Some(start.elapsed());
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
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
