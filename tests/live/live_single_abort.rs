//! The `files_committed_on_abort` boundary for the SINGLE runner — the cell the
//! runner-coverage matrix argued "by construction" until 2026-08-29: an error
//! RETURNED mid-commit-loop (not a crash) must leave `files_committed` = the
//! parts already recorded, because that count is the retry duplicate guard's
//! ONLY input (`decide_export_retry` — pipeline/single.rs). Template:
//! `parallel_keyset_worker_error_still_counts_the_durable_parts_postgres`.

use crate::common::*;

/// A multi-part single export (max_file_size + small batches force the roll —
/// two first cuts produced ONE part: a compressible payload, then one giant
/// batch that never hit maybe_split, which only runs between flushes) errors
/// at part 2
/// of ~3+: the run fails loud, no _SUCCESS, and the metrics row counts exactly
/// the durable parts — observed at the boundary from the REAL producer, not fed
/// to the decider by the test.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn single_commit_error_still_counts_the_durable_parts_postgres() {
    require_alive(LiveService::Postgres);
    let table = unique_name("single_abort");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
         INSERT INTO {table} SELECT g, repeat(md5(g::text), 32)
           FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    // maybe_split only rotates BETWEEN flushes: batch_size 100 + 100-row
    // groups make each ~100KB closed group exceed the 64KB cap (the working
    // recipe from audit_cloud_multipart — a single big batch never splits).
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .export_line("compression: none")
        .export_line("max_file_size: 64KB")
        .export_line("parquet:")
        .export_line("  row_group_strategy: fixed_rows")
        .export_line("  row_group_rows: 100")
        .export_line("tuning: {batch_size: 100}");
    let out = rig.run_with_env("RIVET_TEST_ERROR_AT", "single_part_commit:2");
    assert!(
        !out.status.success(),
        "an injected commit-loop error must fail the run"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("single_part_commit"),
        "the failure must be OUR injected error, not a fixture accident: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Fixture is not inert: the roll really produced parts before the fire.
    let on_disk = files_with_extension(&rig.out_dir(), "parquet").len();
    assert_eq!(
        on_disk, 2,
        "parts 0 and 1 must be durable before the injected error at part 2 \
         (max_file_size must have rolled at least 3 parts for this fixture)"
    );
    assert!(
        !rig.out_dir().join("_SUCCESS").is_file(),
        "no completion marker on a failed run"
    );

    // The boundary: the run's own metrics row counts what it left durable.
    let db = StateDb::next_to_config(&rig.config_path());
    let run_id = db.latest_run_id(rig.export_name());
    let m = db.metrics_row(&run_id);
    assert_eq!(
        m.files_committed,
        Some(on_disk as i64),
        "a failed single run must count the parts it left durable — the retry \
         duplicate guard reads this and nothing else"
    );

    let mut c2 = pg_connect();
    let _ = c2.batch_execute(&format!("DROP TABLE IF EXISTS {table};"));
}
