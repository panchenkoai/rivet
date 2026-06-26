//! AUDIT cluster `cli-dispatch` — proof tests for two dispatch-layer fixes.
//!
//! L12 (export typo): `rivet run/check/plan --export <typo>` used to error with
//! the bare "export '<typo>' not found in config" and NOT list the declared
//! export names (unlike the "Did you mean" field-typo lint). `dispatch.rs` now
//! validates the `--export` selection up front and enumerates the sorted names.
//!
//! L22 (check vs run footgun): `rivet check` on a stdout+chunked config exited 0
//! and printed "Strategy: chunked" with no warning, while `run`/`plan` reject it
//! via plan-validation ([stdout-no-chunked]). `dispatch_check` now also runs
//! `validate_plan` and surfaces Rejected/Warning lines, exiting non-zero on a
//! Rejected so `check` and `run` agree.
//!
//! These assert the CORRECT (fixed) behavior.

use crate::common::*;

/// Single-export Postgres config whose destination is **stdout** and whose mode
/// is **chunked** — the [stdout-no-chunked] combination that `run`/`plan` reject
/// but `check` used to pass silently.
fn stdout_chunked_config(table: &str) -> String {
    format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {table}
    query: "SELECT id, name FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: 1000
    format: parquet
    destination:
      type: stdout
"#
    )
}

/// Single-export Postgres config with a local destination — used to drive the
/// `--export <typo>` path (the config is otherwise valid; only the CLI flag is
/// wrong).
fn simple_local_config(table: &str, out_dir: &std::path::Path) -> String {
    format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {table}
    query: "SELECT id, name FROM {table}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

// L22 — `rivet check` on a stdout+chunked config must exit non-zero, agreeing
// with `run`/`plan` (which reject [stdout-no-chunked]) instead of silently
// passing at exit 0 with "Strategy: chunked".
#[test]
#[ignore = "live: postgres"]
fn check_on_stdout_chunked_exits_nonzero() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &stdout_chunked_config(table.name()));

    let result = run_rivet(&[
        "check",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);

    assert!(
        !result.status.success(),
        "check on stdout+chunked must exit non-zero (plan-validation Rejected \
         [stdout-no-chunked]), mirroring run/plan; got exit {:?}\nstdout:\n{}\nstderr:\n{}",
        result.status.code(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

// L22 — the check output must name the rejecting rule so the operator learns
// *why* the combination is invalid, not just that something failed.
#[test]
#[ignore = "live: postgres"]
fn check_on_stdout_chunked_names_the_rule() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &stdout_chunked_config(table.name()));

    let result = run_rivet(&[
        "check",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );

    assert!(
        combined.contains("stdout-no-chunked"),
        "check must surface the plan-validation rule [stdout-no-chunked] that run/plan \
         already reject; got:\n{combined}"
    );
}

// L12 — `rivet check --export <typo>` must enumerate the available export names
// (sorted), like the field-typo lint does, instead of the bare "not found".
#[test]
#[ignore = "live: postgres"]
fn check_unknown_export_lists_available_names() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_local_config(table.name(), out.path()));

    // A deliberate typo of the real export name (the table name).
    let typo = format!("{}_typo", table.name());
    let result = run_rivet(&[
        "check",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &typo,
    ]);

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );

    assert!(
        !result.status.success(),
        "check --export <typo> must exit non-zero; got exit {:?}\n{combined}",
        result.status.code(),
    );
    // The error must name the typo AND list the real export name so the operator
    // can copy/paste the correct choice.
    assert!(
        combined.contains(&typo) && combined.contains(table.name()),
        "check --export <typo> must name the typo '{typo}' and enumerate the known export \
         '{}' (sorted Known exports list); got:\n{combined}",
        table.name()
    );
    assert!(
        combined.contains("Known exports"),
        "the error must carry the enumerated 'Known exports:' list (parity with `rivet state \
         reset` and the field-typo lint); got:\n{combined}"
    );
}

// L12 — parity check: `rivet run --export <typo>` gets the same enumerated-names
// error (the helper is shared across the --export-taking dispatch arms).
#[test]
#[ignore = "live: postgres"]
fn run_unknown_export_lists_available_names() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &simple_local_config(table.name(), out.path()));

    let typo = format!("{}_typo", table.name());
    let result = run_rivet(&["run", "--config", cfg.to_str().unwrap(), "--export", &typo]);

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );

    assert!(
        !result.status.success(),
        "run --export <typo> must exit non-zero; got exit {:?}\n{combined}",
        result.status.code(),
    );
    assert!(
        combined.contains("Known exports") && combined.contains(table.name()),
        "run --export <typo> must enumerate the known export names just like check; got:\n{combined}"
    );
}
