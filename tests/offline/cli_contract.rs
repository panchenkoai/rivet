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

/// Every variant of `Commands`, lowercased — clap's default rename for a
/// subcommand name, and this enum declares no `command(name = ..)` override.
///
/// DERIVED, not typed. The list used to be eleven names written out by hand
/// while `Commands` had sixteen variants: `cdc`, `load`, `validate`, `schema`
/// and `journal` were missing, in a test called
/// `root_help_lists_every_top_level_subcommand` inside a file called
/// `cli_contract.rs`. A hand-written dimension cannot notice its own omission —
/// the defect this repo has now paid for in five separate places (audit
/// 2026-08-17).
fn declared_subcommands() -> Vec<String> {
    let src = include_str!("../../src/cli/args.rs");
    let start = src
        .find("pub enum Commands {")
        .expect("src/cli/args.rs must declare `pub enum Commands`");
    let body = &src[start..];
    let end = body.find("\n}").expect("the enum must close");
    let mut out = Vec::new();
    for line in body[..end].lines().skip(1) {
        let t = line.trim();
        // Variants sit at one indent level and start with an uppercase letter;
        // attributes, doc comments and nested field lines do not.
        if line.starts_with("    ")
            && !line.starts_with("     ")
            && t.chars().next().is_some_and(|c| c.is_ascii_uppercase())
        {
            let name: String = t
                .chars()
                .take_while(|c| c.is_alphanumeric())
                .collect::<String>()
                .to_lowercase();
            if !name.is_empty() {
                out.push(name);
            }
        }
    }
    assert!(
        out.len() >= 12,
        "parsed only {} subcommand(s) from `Commands` — the parser lost the enum, \
         and an empty dimension is exactly the failure this derivation replaces: {out:?}",
        out.len()
    );
    out
}

#[test]
fn root_help_lists_every_top_level_subcommand() {
    let (code, stdout, _stderr) = run(&["--help"]);
    assert_eq!(code, 0, "`rivet --help` must exit 0");

    // The `Commands:` block only — `stdout.contains("run")` also matches prose
    // ("Run export jobs…", "run `doctor` first"), so the old check passed for
    // names that were never listed.
    let block = stdout
        .split("Commands:")
        .nth(1)
        .and_then(|s| s.split("\nOptions:").next())
        .expect("`rivet --help` must have a Commands: block");
    let listed: Vec<&str> = block
        .lines()
        .filter_map(|l| l.strip_prefix("  "))
        .filter(|l| !l.starts_with(' '))
        .filter_map(|l| l.split_whitespace().next())
        .collect();

    for sub in declared_subcommands() {
        assert!(
            listed.contains(&sub.as_str()),
            "`Commands` declares `{sub}` but root --help does not LIST it \
             (listed: {listed:?})"
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

/// `--json-errors` — a GLOBAL flag with zero references anywhere in the tree,
/// found by deriving the flag list from `args.rs` rather than reading a
/// remembered one.
///
/// Its whole purpose is machine-readable orchestration: a wrapper parses stderr
/// to decide what failed. So the contract is not "an error is printed" but
/// "stderr PARSES as JSON carrying `error`" — and the pairing matters, because a
/// flag that silently did nothing still prints a perfectly good human message and
/// every exit code stays identical. The caller's parser is the only thing that
/// notices, in production.
///
/// A missing config is the failure used because it needs no database, so this
/// lives with the other offline CLI-contract checks.
#[test]
fn json_errors_makes_stderr_machine_readable_and_is_off_by_default() {
    let missing = "/nonexistent/rivet-json-errors-probe.yaml";

    let (code, _, plain) = run(&["check", "--config", missing]);
    assert_ne!(code, 0, "a missing config must fail");
    assert!(
        serde_json::from_str::<serde_json::Value>(plain.trim()).is_err(),
        "WITHOUT the flag stderr must stay the human message — emitting JSON by default \
         would break every operator reading a terminal; got:\n{plain}"
    );

    let (json_code, _, raw) = run(&["--json-errors", "check", "--config", missing]);
    assert_eq!(
        json_code, code,
        "--json-errors must change the RENDERING only: an exit code that moves with it \
         means the flag is on a different failure path than the one it formats"
    );
    let v: serde_json::Value = serde_json::from_str(raw.trim()).unwrap_or_else(|e| {
        panic!("--json-errors must make stderr parse as JSON ({e}); got:\n{raw}")
    });
    let msg = v
        .get("error")
        .and_then(|e| e.as_str())
        .unwrap_or_else(|| panic!("the JSON must carry a string `error` field; got:\n{raw}"));
    assert!(
        !msg.trim().is_empty(),
        "the `error` field must carry the reason, not an empty string; got:\n{raw}"
    );
}

// ─── state runs / finish-run: the frozen-prefix escape hatch ─────────────

/// The round-4 escape hatch end to end through the REAL binary: a stale
/// `running` row (hard crash, no successful successor — nothing else can ever
/// release it since supersession went success-only) is listed by
/// `state runs --running`, closed by `state finish-run`, and gone from the
/// next listing. A typo'd id exits non-zero — a stamp that touched nothing
/// must never read as success.
#[test]
fn state_runs_lists_a_frozen_row_and_finish_run_releases_it() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("r.yaml");
    std::fs::write(
        &cfg,
        "source:\n  type: postgres\n  url: postgres://x/x\ndestination:\n  type: local\n  path: ./out\nexports:\n  - name: orders\n    query: SELECT 1\n",
    )
    .unwrap();
    let cfg = cfg.to_str().unwrap();
    // Seed the stale row through the product's own store (begin with no finish
    // = the hard-crash shape).
    {
        let store = rivet::state::StateStore::open(cfg).unwrap();
        store
            .begin_run(
                "r-dead",
                "orders",
                "file:///tmp/out",
                "2026-08-27T00:00:00Z",
            )
            .unwrap();
    }

    let (code, out, err) = run(&["state", "runs", "-c", cfg, "--running"]);
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    assert!(
        out.contains("r-dead") && out.contains("running"),
        "the frozen row must be listed: {out}"
    );
    assert!(
        out.contains("finish-run"),
        "the listing must point at the escape hatch: {out}"
    );

    let (code, _out, err) = run(&["state", "finish-run", "-c", cfg, "--run-id", "nope"]);
    assert_ne!(
        code, 0,
        "a typo'd id must refuse loudly, not report success"
    );
    assert!(err.contains("nope"), "the refusal names the id: {err}");

    let (code, out, _err) = run(&["state", "finish-run", "-c", cfg, "--run-id", "r-dead"]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("interrupted"), "{out}");

    let (code, out, _err) = run(&["state", "runs", "-c", cfg, "--running"]);
    assert_eq!(code, 0);
    assert!(
        !out.contains("r-dead"),
        "the stamped row must stop freezing the prefix: {out}"
    );
}
