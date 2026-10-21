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

/// Single-export Postgres rig whose destination is **stdout** and whose mode
/// is **chunked** — the [stdout-no-chunked] combination that `run`/`plan` reject
/// but `check` used to pass silently.
fn stdout_chunked_rig(table: &str) -> Rig {
    Rig::pg_batch(table)
        .query(&format!("SELECT id, name FROM {table}"))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000")
        .dest_stdout()
}

/// Single-export Postgres rig with a local destination — used to drive the
/// `--export <typo>` path (the config is otherwise valid; only the CLI flag is
/// wrong).
fn simple_local_rig(table: &str, out_dir: &std::path::Path) -> Rig {
    Rig::pg_batch(table)
        .query(&format!("SELECT id, name FROM {table}"))
        .dest_path(out_dir.to_path_buf())
}

// L22 — `rivet check` on a stdout+chunked config must exit non-zero, agreeing
// with `run`/`plan` (which reject [stdout-no-chunked]) instead of silently
// passing at exit 0 with "Strategy: chunked".
#[test]
#[ignore = "live: postgres"]
fn check_on_stdout_chunked_exits_nonzero() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let rig = stdout_chunked_rig(table.name());

    let result = rig.cli(&["check", "--export", table.name()]);

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
    let rig = stdout_chunked_rig(table.name());

    let result = rig.cli(&["check", "--export", table.name()]);

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
    let rig = simple_local_rig(table.name(), out.path());

    // A deliberate typo of the real export name (the table name).
    let typo = format!("{}_typo", table.name());
    let result = rig.cli(&["check", "--export", &typo]);

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
    let rig = simple_local_rig(table.name(), out.path());

    let typo = format!("{}_typo", table.name());
    let result = rig.run_args(&["--export", &typo]);

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

/// `apply --pool` on a stdout export writes the export alone on stdout.
#[test]
#[ignore = "live: postgres"]
fn apply_pool_writes_only_the_export_on_stdout() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .with_format("csv")
        .dest_stdout();
    let applied = rig.apply_env(&rig.config_path(), &["--pool", "2"], &[]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let stdout = String::from_utf8_lossy(&applied.stdout);
    let id_of = |line: &str| line.split(',').next().and_then(|c| c.parse::<i64>().ok());
    let mut delivered: Vec<i64> = stdout.lines().filter_map(id_of).collect();
    delivered.sort();
    let source: Vec<i64> = pg_connect()
        .query(&format!("SELECT id FROM {} ORDER BY id", table.name()), &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(delivered, source, "the export's rows are on stdout");
    let foreign = stdout
        .lines()
        .filter(|l| *l != "id,name" && id_of(l).is_none())
        .count();
    assert!(
        foreign == 0,
        "`apply --pool` wrote {foreign} line(s) of its own into a stdout export"
    );
}

/// A run of `lossy-x` beside a live run of `lossy x`, an export whose name differs only where a
/// file name cannot spell it, paused while it holds its run lease: the run delivers (through 0.32.1
/// both leases were one file, `.lease-chunk-run-lossy-x-…`, and it was refused
/// RIVET_STATE_RUN_IN_PROGRESS beside no live run of its own).
#[test]
#[ignore = "live: postgres"]
fn a_run_beside_a_live_export_whose_name_reads_alike_delivers() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5);
    let query = format!("SELECT id, name FROM {}", table.name());
    let rig = Rig::pg_batch(table.name())
        .query(&query)
        .export_named("lossy-x")
        .also_export("lossy x", &query);
    let scratch = tempfile::tempdir().unwrap();
    let paused = scratch.path().join("paused");
    let mut sibling = rig.spawn_args_env(
        &["--export", "lossy x"],
        &[
            ("RIVET_TEST_PAUSE_AT", "pg_after_snapshot_open:8000"),
            ("RIVET_TEST_PAUSE_MARKER", paused.to_str().unwrap()),
        ],
    );
    let waited = std::time::Instant::now();
    while !paused.exists() {
        assert!(
            waited.elapsed().as_secs() < 60,
            "the sibling run never reached its read"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let mine = rig.cli(&["run", "--export", "lossy-x"]);
    let said = String::from_utf8_lossy(&mine.stderr).into_owned();
    assert!(
        sibling.wait().unwrap().success(),
        "the paused sibling delivers"
    );
    assert!(
        mine.status.success(),
        "the run beside a sibling that reads alike:\n{said}"
    );
}
