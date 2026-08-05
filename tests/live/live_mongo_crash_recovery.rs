//! Batch crash-recovery for the MongoDB keyset export, on the canonical [`Rig`]
//! — the Mongo twin of `live_mysql_crash_recovery.rs`. Requires the `mongo`
//! service.
//!
//! The crash windows are the engine-agnostic sink hooks in `pipeline::commit`
//! (`after_file_write`, `after_manifest_update`) — verified empirically to fire
//! on the Mongo keyset path. Contract on recovery: **no data loss** — every source
//! `_id` is present after the re-run — at **at-least-once** (a keyset full export
//! keeps no mid-run checkpoint, so the re-run rescans and the orphaned crash
//! page's rows survive as duplicates, deduped downstream by `_id`).

use crate::common::*;

const PORT: u16 = 27017;

/// Seed → crash at `hook` (mid-export) → re-run → assert completeness (every
/// `_id`) and at-least-once (rows ≥ source). Shared by both crash-window tests.
fn crash_then_recover_is_lossless(hook: &str, tag: &str) {
    require_alive(LiveService::Mongo);
    require_alive(LiveService::DuckDb);
    let db = unique_name(tag);
    MongoTest::connect(PORT, &db).seed_int_id("t", 5000); // 5 keyset pages

    let rig = Rig::mongo_batch("t")
        .source_url(&MongoTest::url(PORT, &db))
        .mongo("page_size: 1000")
        .duckdb_oracle();

    // Crash mid-export: a file (and, for after_manifest_update, a manifest row)
    // exists, but the run aborts before finalising.
    let crashed = rig.run_with_env("RIVET_TEST_PANIC_AT", hook);
    assert!(
        !crashed.status.success(),
        "RIVET_TEST_PANIC_AT={hook} must crash the export (non-zero exit)"
    );

    // Recover: a clean re-run (same rig → same config + destination) must
    // complete and surface EVERY source row.
    rig.run_ok();
    // Read by DuckDB, which does not share rivet's parquet codec. AT-LEAST-ONCE,
    // not exact: the orphaned crash page may legitimately duplicate on recovery,
    // so distinct must be exact while the total may exceed it.
    duckdb_assert_at_least_once(
        rig.oracle_dir(),
        "_id",
        5000,
        "recovery after a mid-export crash",
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_crash_after_file_write_recovers_without_loss() {
    crash_then_recover_is_lossless("after_file_write", "crash_afw");
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_crash_after_manifest_update_recovers_without_loss() {
    crash_then_recover_is_lossless("after_manifest_update", "crash_amu");
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_two_rapid_runs_into_same_prefix_do_not_clobber() {
    // Run-uniqueness for the Mongo batch path (mongo_parallel.rs millisecond part
    // stamp). Two rapid full runs into one prefix — no sleep — must EACH
    // materialise their files; the later must not silently overwrite the earlier
    // (LocalDestination idempotent_overwrite). The batch-mode twin of the SQL
    // full_mode_repeated_run tests and the CDC roast_second_run clobber test.
    require_alive(LiveService::Mongo);
    require_alive(LiveService::DuckDb);
    let db = unique_name("mongo_clobber");
    MongoTest::connect(PORT, &db).seed_int_id("t", 100);
    let rig = Rig::mongo_batch("t")
        .source_url(&MongoTest::url(PORT, &db))
        .mongo("page_size: 50")
        .duckdb_oracle();

    rig.run_ok();
    rig.run_ok(); // rapid second run into the SAME prefix, no sleep

    // Two full snapshots of 100 rows → 200 physical rows if neither run clobbered
    // the other, and every source _id is still present.
    // DuckDB, not the parquet crate rivet writes with. Both numbers stated:
    // 200 rows over 100 distinct is CORRECT here — two full
    // snapshots into one prefix — so neither "complete" nor "at least once" fits.
    duckdb_assert_rows_and_distinct(
        rig.oracle_dir(),
        "_id",
        200,
        100,
        "two rapid full runs must each materialise all 100 rows — no clobber",
    );
    // The parquet re-read above is a proxy; assert the dest manifest COPIES
    // (`manifest-<run>.json`, what reconcile / a warehouse autoloader read) also
    // span both runs. Summed row_count (not a file count) is robust to
    // parts-per-run (page_size), and RED pre-fix (one clobbered manifest.json).
    assert_eq!(
        dir_manifest_copy_total_rows(&rig.out_dir()),
        200,
        "run-unique manifest copies must sum both rapid runs' rows; a clobbered manifest is silent to the parquet re-read"
    );
}

/// Run-uniqueness for the PARALLEL Mongo runner (mongo_parallel.rs worker stamp:
/// `%3f` + `w{worker}` + page). Two rapid `parallel: 4` runs into ONE prefix must
/// each materialise their files — the later must not overwrite the earlier. The
/// parallel twin of the test above (which runs the single keyset path, so it does
/// not exercise the per-worker naming). (runner-coverage-matrix
/// run_unique_part_naming: mongo_parallel cell.)
#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_parallel_two_rapid_runs_into_same_prefix_do_not_clobber() {
    require_alive(LiveService::Mongo);
    require_alive(LiveService::DuckDb);
    let db = unique_name("mongo_par_clobber");
    MongoTest::connect(PORT, &db).seed_int_id("t", 100);
    let rig = Rig::mongo_batch("t")
        .source_url(&MongoTest::url(PORT, &db))
        .mongo("page_size: 30")
        .export_line("parallel: 4")
        .duckdb_oracle();

    rig.run_ok();
    rig.run_ok(); // rapid second parallel run into the SAME prefix, no sleep

    // DuckDB, not the parquet crate rivet writes with. Both numbers stated:
    // 200 rows over 100 distinct is CORRECT here — two full
    // snapshots into one prefix — so neither "complete" nor "at least once" fits.
    duckdb_assert_rows_and_distinct(
        rig.oracle_dir(),
        "_id",
        200,
        100,
        "two rapid parallel:4 runs must each materialise all 100 rows — no worker-part clobber",
    );
    assert_eq!(
        dir_manifest_copy_total_rows(&rig.out_dir()),
        200,
        "run-unique manifest copies must sum both parallel runs' rows"
    );
}
