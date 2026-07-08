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
    let db = unique_name(tag);
    MongoTest::connect(PORT, &db).seed_int_id("t", 5000); // 5 keyset pages

    let rig = Rig::mongo_batch("t")
        .source_url(&MongoTest::url(PORT, &db))
        .mongo("page_size: 1000");

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
    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        5000,
        "recovery must lose NOTHING — every source _id present after the crash"
    );
    assert!(
        total_parquet_rows(&rig.out_dir()) >= 5000,
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
