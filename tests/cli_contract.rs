//! CLI contract tests — `--help` output shape and exit codes.
//!
//! QA backlog Task 5.3.  The CLI is the product surface for operators and
//! automation (schedulers, CI pipelines).  Help output and exit codes must
//! change intentionally, not accidentally.
//!
//! ## Scope
//!
//! This file asserts *structural* contract properties — "the command exists",
//! "help mentions the subcommand name" — rather than full textual snapshots.
//! A byte-exact snapshot is too brittle (it flips on every doc tweak) and
//! too loose (it does not describe intent).  For key fields (`--config`,
//! `--export`, etc.) we assert presence explicitly.
//!
//! Live-source tests (auth failure, destination failure) are out of scope for
//! this suite because they require a real database.  The classifier-level
//! behaviour is already covered by `tests/retry_integration.rs`.
//!
//! ## How it works
//!
//! Cargo provides `env!("CARGO_BIN_EXE_rivet")` when compiling integration
//! tests for a crate that defines `[[bin]] name = "rivet"`.  The constant is
//! the absolute path to the freshly-built binary; we invoke it via
//! `std::process::Command`, capture stdout/stderr, and assert against the
//! captured output.

use std::process::Command;

/// Absolute path to the `rivet` binary built for this integration test.
const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");

/// Shorthand for running `rivet <args...>` and returning `(exit_code, stdout, stderr)`.
fn run(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(RIVET_BIN)
        .args(args)
        .output()
        .expect("failed to spawn rivet binary");
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    (code, stdout, stderr)
}

// ─── --help / --version shape ────────────────────────────────

#[test]
fn root_help_lists_every_top_level_subcommand() {
    let (code, stdout, _stderr) = run(&["--help"]);
    assert_eq!(code, 0, "`rivet --help` must exit 0");

    for sub in [
        "run",
        "check",
        "doctor",
        "state",
        "plan",
        "apply",
        "repair",
        "reconcile",
        "metrics",
        "init",
        "completions",
    ] {
        assert!(
            stdout.contains(sub),
            "root --help must mention subcommand '{sub}'; got:\n{stdout}"
        );
    }
}

#[test]
fn run_help_mentions_required_flags() {
    let (code, stdout, _stderr) = run(&["run", "--help"]);
    assert_eq!(code, 0);
    for flag in ["--config", "--export"] {
        assert!(
            stdout.contains(flag),
            "`rivet run --help` must mention '{flag}'; got:\n{stdout}"
        );
    }
}

#[test]
fn plan_help_exit_zero_and_mentions_command() {
    let (code, stdout, _stderr) = run(&["plan", "--help"]);
    assert_eq!(code, 0);
    assert!(
        stdout.to_lowercase().contains("plan"),
        "`rivet plan --help` output must mention 'plan'; got:\n{stdout}"
    );
}

#[test]
fn apply_help_exit_zero_and_mentions_command() {
    let (code, stdout, _stderr) = run(&["apply", "--help"]);
    assert_eq!(code, 0);
    assert!(
        stdout.to_lowercase().contains("apply"),
        "`rivet apply --help` output must mention 'apply'; got:\n{stdout}"
    );
}

#[test]
fn check_help_exit_zero_and_mentions_command() {
    let (code, stdout, _stderr) = run(&["check", "--help"]);
    assert_eq!(code, 0);
    assert!(
        stdout.to_lowercase().contains("check"),
        "`rivet check --help` output must mention 'check'; got:\n{stdout}"
    );
}

#[test]
fn doctor_help_exit_zero_and_mentions_command() {
    let (code, stdout, _stderr) = run(&["doctor", "--help"]);
    assert_eq!(code, 0);
    assert!(
        stdout.to_lowercase().contains("doctor"),
        "`rivet doctor --help` output must mention 'doctor'; got:\n{stdout}"
    );
}

#[test]
fn unknown_subcommand_exits_nonzero_and_suggests_help() {
    let (code, _stdout, stderr) = run(&["definitely-not-a-real-subcommand"]);
    assert_ne!(code, 0, "unknown subcommand must exit non-zero");
    assert!(
        stderr.to_lowercase().contains("help") || stderr.to_lowercase().contains("usage"),
        "unknown subcommand error must point operators at --help; got stderr:\n{stderr}"
    );
}

// ─── Exit-code contract ──────────────────────────────────────

#[test]
fn run_with_missing_config_file_exits_nonzero() {
    // Config file doesn't exist — the CLI must fail fast with a non-zero
    // exit code (so schedulers treat the run as failed) and leave a
    // recognizable error hint on stderr.
    let (code, _stdout, stderr) = run(&["run", "--config", "/nonexistent/rivet_qa_config.yaml"]);
    assert_ne!(code, 0, "missing config file must exit non-zero");
    assert!(
        !stderr.is_empty(),
        "missing config must produce a non-empty stderr diagnostic"
    );
}

#[test]
fn run_with_invalid_yaml_exits_nonzero_with_actionable_stderr() {
    // Hand a YAML the parser must reject (empty `exports:` list, see
    // QA backlog Task 5.1).  The CLI must surface the parser error so an
    // operator can fix it; the exit code must be non-zero.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.yaml");
    std::fs::write(
        &path,
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports: []
"#,
    )
    .unwrap();

    let (code, _stdout, stderr) = run(&["run", "--config", path.to_str().unwrap()]);
    assert_ne!(code, 0, "invalid config must exit non-zero");
    assert!(
        stderr.to_lowercase().contains("export") || stderr.to_lowercase().contains("config"),
        "stderr must point operator at the bad field; got:\n{stderr}"
    );
}

#[test]
fn check_help_and_run_help_advertise_config_flag_consistently() {
    // Both subcommands take the same `--config` flag.  A rename in one place
    // without the other is a common hazard for CLIs; this test flags it.
    let (_c1, h_run, _) = run(&["run", "--help"]);
    let (_c2, h_check, _) = run(&["check", "--help"]);
    assert!(h_run.contains("--config"));
    assert!(h_check.contains("--config"));
}
