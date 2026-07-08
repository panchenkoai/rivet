//! Batch crash-recovery for the MongoDB keyset export — the Mongo twin of
//! `live_mysql_crash_recovery.rs`. Requires the `mongo` service.
//!
//! The crash windows are the engine-agnostic sink hooks in `pipeline::commit`
//! (`after_file_write`, `after_manifest_update`) — verified empirically to fire
//! on the Mongo keyset path (a full-mode keyset export routes through the same
//! commit seam). Contract on recovery: **no data loss** — every source `_id` is
//! present after the re-run — at **at-least-once** (a keyset full export keeps no
//! mid-run checkpoint, so the re-run rescans from the start; the orphaned
//! crash-page's rows survive as duplicates, deduped downstream by `_id`). This
//! mirrors the SQL engines' at-least-once crash contract and the CDC sink rule.

use crate::common::*;

const PORT: u16 = 27017;

/// Seed → crash at `hook` (mid-export) → re-run → assert completeness (every
/// `_id`) and at-least-once (rows ≥ source). Shared by both crash-window tests.
fn crash_then_recover_is_lossless(hook: &str, tag: &str) {
    require_alive(LiveService::Mongo);
    let db = unique_name(tag);
    let m = MongoTest::connect(PORT, &db);
    m.seed_int_id("t", 5000); // 5 keyset pages at page_size 1000

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let cfg = write_mongo_config(
        cfg_dir.path(),
        &MongoTest::url(PORT, &db),
        "t",
        out.path(),
        ", mongo: { page_size: 1000 }",
        "",
    );

    // Crash mid-export: a file (and, for after_manifest_update, a manifest row)
    // exists, but the run aborts before finalising.
    let crashed = run_rivet_env(
        &["run", "-c", cfg.to_str().unwrap()],
        &[("RIVET_TEST_PANIC_AT", hook)],
    );
    assert!(
        !crashed.status.success(),
        "RIVET_TEST_PANIC_AT={hook} must crash the export (non-zero exit)"
    );

    // Recover: a clean re-run must complete and surface EVERY source row.
    let rec = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        rec.status.success(),
        "recovery run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&rec.stderr)
    );
    assert_eq!(
        dir_parquet_distinct_strings(out.path(), "_id").len(),
        5000,
        "recovery must lose NOTHING — every source _id present after the crash"
    );
    assert!(
        total_parquet_rows(out.path()) >= 5000,
        "recovery is at-least-once — the orphaned crash page may duplicate, never lose"
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
