//! AUDIT-RED — cluster `state-guardrails` (findings #21, #22, #23).
//!
//! These tests drive the real `rivet state …` subcommands against the live
//! Postgres stack and assert the CORRECT behavior. They are expected to FAIL
//! against current code and pass once the guardrails are added.
//!
//! * #21 — `state reset-chunks -e <typo>` silently succeeds (exit 0,
//!   "Removed 0 …") with no export-name guardrail, unlike `state reset` which
//!   validates the name and errors with a "Known exports" hint.
//! * #22 — `state reset` does NOT clear `export_progression`, so
//!   `state progression` still reports the old committed boundary after the
//!   cursor has been reset (`state show` is empty, progression is stale).
//! * #23 — read-only inspect (`state show`) never parses the config; a garbage
//!   / missing config path yields a false "No exports have been run yet" exit 0
//!   and litters a fresh `.rivet_state.db`.
//!
//! Run with: `cargo test --test audit_state -- --ignored`

use crate::common::*;

/// Chunked-checkpoint rig for `table` so a real chunk run is recorded in the
/// state DB (mirrors `state_reset_chunks_clears_checkpoint` in live_cli_flags).
fn chunked_checkpoint_rig(table: &str, out_dir: &std::path::Path) -> Rig {
    Rig::pg_batch(table)
        .query(&format!("SELECT id, name FROM {table}"))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 50")
        .export_line("chunk_checkpoint: true")
        .dest_path(out_dir.to_path_buf())
}

/// Incremental rig with `cursor_column: created_at` — an incremental run
/// records a committed boundary in `export_progression`.
fn incremental_rig(table: &str, out_dir: &std::path::Path) -> Rig {
    Rig::pg_batch(table)
        .query(&format!("SELECT id, name, created_at FROM {table}"))
        .mode("incremental")
        .export_line("cursor_column: created_at")
        .dest_path(out_dir.to_path_buf())
}

/// Run the rig's export and assert it succeeded.
fn run_export(rig: &Rig, export: &str) {
    let out = rig.run_args(&["--export", export]);
    assert!(
        out.status.success(),
        "setup: rivet run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// AUDIT-RED state-guardrails: `state reset-chunks -e <typo>` silently succeeds (exit 0). Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_reset_chunks_rejects_unknown_export() {
    require_alive(LiveService::Postgres);

    // Seed + run a chunked-checkpoint export so chunk_run rows exist for the
    // real export name; the typo'd name below is NOT in the config.
    let table = seed_pg_numeric_table(100);
    let out = tempfile::tempdir().unwrap();
    let rig = chunked_checkpoint_rig(table.name(), out.path());
    run_export(&rig, table.name());

    // `<name>x` is a typo: not declared in the config. Parity with
    // `state reset` requires this to be rejected, not silently "Removed 0".
    let typo = format!("{}x", table.name());
    let result = rig.cli(&["state", "reset-chunks", "--export", &typo]);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);

    assert!(
        !result.status.success(),
        "reset-chunks on an unknown export '{typo}' must exit NON-zero (parity with `state reset`); \
         got exit 0.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains(&typo),
        "reset-chunks must name the unknown export '{typo}' in its error; \
         got stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

// AUDIT-RED state-guardrails: `state reset` leaves export_progression stale — progression still reports the committed boundary. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_reset_clears_progression() {
    require_alive(LiveService::Postgres);

    // Incremental export advances the cursor AND records a committed boundary
    // in export_progression.
    let table = seed_pg_numeric_table(100);
    let out = tempfile::tempdir().unwrap();
    let rig = incremental_rig(table.name(), out.path());
    run_export(&rig, table.name());

    // Sanity: a committed boundary is recorded before the reset, otherwise the
    // post-reset assertion would pass vacuously.
    let before = rig.cli(&["state", "progression", "--export", table.name()]);
    assert!(
        before.status.success(),
        "setup: state progression must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&before.stderr)
    );
    let before_out = String::from_utf8_lossy(&before.stdout);
    assert!(
        before_out.contains(table.name()) && !before_out.contains("No progression boundaries"),
        "setup: progression must report a committed boundary for '{}' before reset; got:\n{before_out}",
        table.name()
    );

    // Reset the export's state.
    let reset = rig.cli(&["state", "reset", "--export", table.name()]);
    assert!(
        reset.status.success(),
        "state reset must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&reset.stderr)
    );

    // CORRECT behavior: after reset, progression must NOT still report a
    // committed boundary for the export. Currently the export_progression row
    // survives, so the old boundary is shown — stale.
    let after = rig.cli(&["state", "progression", "--export", table.name()]);
    assert!(
        after.status.success(),
        "state progression must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&after.stderr)
    );
    let after_out = String::from_utf8_lossy(&after.stdout);
    assert!(
        after_out.contains("No progression boundaries"),
        "after `state reset`, progression must report no committed boundary for '{}' \
         (it is stale — the export_progression row was not cleared); got:\n{after_out}",
        table.name()
    );
}

// state-guardrails: `state show -c <missing>.yaml` must refuse the path, exit non-zero and create no state DB beside it (fixed; it used to print "No state" and exit 0).
#[test]
#[ignore = "live: postgres"]
fn audit_state_show_refuses_a_missing_config_path() {
    require_alive(LiveService::Postgres);

    // Point at a config path that does not exist. A read-only inspect must
    // surface the bad path rather than silently opening a fresh state DB
    // beside it and printing "No exports have been run yet".
    let dir = tempfile::tempdir().unwrap();
    let missing_cfg = dir.path().join("does_not_exist.yaml");
    assert!(!missing_cfg.exists(), "precondition: config must be absent");

    // Raw invocation on purpose: the SUBJECT is a config path that does not
    // exist, which a rig (whose config always exists) cannot express — the
    // audit_observability precedent. Counted in the rig-adoption ratchet.
    let result = std::process::Command::new(RIVET_BIN)
        .args(["state", "show", "--config", missing_cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet state show");

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);

    assert!(
        !result.status.success(),
        "state show on a missing config '{}' must exit NON-zero, not print \
         \"No exports have been run yet\" exit 0.\nstdout:\n{stdout}\nstderr:\n{stderr}",
        missing_cfg.display()
    );

    // And it must NOT litter a fresh state DB next to the (missing) config.
    let leaked_db = dir.path().join(".rivet_state.db");
    assert!(
        !leaked_db.exists(),
        "state show on a missing config must not create {} (read-only inspect \
         should not materialize a state DB for a bad path)",
        leaked_db.display()
    );
}

/// P-19: `state reset --export X` under one config keeps the cursor a same-named export of another source stored.
#[test]
#[ignore = "live+gate-only: postgres; open defect P-19, acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_state_reset_keeps_the_cursor_of_another_source() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("audit_state");
    e.insert(&table, 1..=10, 180, Some(10));
    let other = pg_other_database_url();
    let _other_guard = pg_same_table_on(&other, &table, 7);
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let on = |rig: Rig, url: &str, out: &std::path::Path| {
        rig.source_url(url)
            .restage("incremental", &["cursor_column: id"])
            .dest_path(out.to_path_buf())
    };

    let rig = on(e.rig(&table), POSTGRES_URL, a.path());
    rig.run_ok();
    let rig = on(rig, &other, b.path());
    rig.run_ok();
    assert_eq!(read_ids(b.path()), (1..=7).collect::<Vec<_>>());

    let rig = on(rig, POSTGRES_URL, a.path());
    let reset = rig.cli(&["state", "reset", "--export", &table]);
    assert!(
        reset.status.success(),
        "{}",
        String::from_utf8_lossy(&reset.stderr)
    );

    let rig = on(rig, &other, b.path());
    rig.run_ok();
    let on_disk = read_ids(b.path()).len();
    assert!(
        on_disk == 7,
        "P-19: `state reset` under one config deleted the cursor of a same-named export of another source: its next run re-delivered the table ({on_disk} rows on disk for 7 source ids)"
    );
}

/// P-23: a `state reset` accepted while an incremental run reads must not leave that run's delta published as a full pass.
#[test]
#[ignore = "live+gate-only: postgres; open defect P-23, acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_state_reset_during_an_incremental_read_keeps_the_manifest_honest() {
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _guard) = e.table("audit_state");
    e.insert(&table, 1..=10, 180, Some(10));
    let out = tempfile::tempdir().unwrap();
    let rig = e
        .rig(&table)
        .restage("incremental", &["cursor_column: id"])
        .dest_path(out.path().to_path_buf());
    rig.run_ok();
    e.insert(&table, 11..=13, 170, Some(10));

    let scratch = tempfile::tempdir().unwrap();
    let paused = scratch.path().join("paused");
    let mut run = rig.spawn_args_env(
        &[],
        &[
            ("RIVET_TEST_PAUSE_AT", "pg_after_snapshot_open:4000"),
            ("RIVET_TEST_PAUSE_MARKER", paused.to_str().unwrap()),
        ],
    );
    let waited = std::time::Instant::now();
    while !paused.exists() {
        assert!(
            waited.elapsed().as_secs() < 60,
            "the run never reached its read"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let reset = rig.cli(&["state", "reset", "--export", &table]);
    let status = run.wait().unwrap();
    assert!(
        status.success(),
        "the incremental run failed beside a `state reset`"
    );
    if !reset.status.success() {
        return;
    }

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.path().join("manifest.json")).unwrap()).unwrap();
    let stage = tempfile::tempdir().unwrap();
    for part in manifest["parts"].as_array().expect("a parts list") {
        let name = part["path"].as_str().expect("a part path");
        std::fs::copy(out.path().join(name), stage.path().join(name)).unwrap();
    }
    let listed = ids_of(&read_all_parts(stage.path()));
    assert!(
        !manifest["source"]["extraction"]["cursor_low"].is_null()
            || listed == (1..=13).collect::<Vec<_>>(),
        "P-23: a `state reset` accepted during an incremental read left a manifest with no cursor_low over a delta: its parts hold ids {listed:?} of 1..=13"
    );
}
