//! Live CDC coverage for the MongoDB change-stream engine — the soak + crash
//! matrix the SQL engines have (`live_cdc.rs`, `live_crash_soak.rs`), now for
//! Mongo. Requires the single-node replica set (`docker compose up -d mongo-rs`).
//!
//! What is proven here (each an at-least-once / no-loss invariant an operator
//! relies on, checked by RE-READING the destination, never rivet's counters):
//!
//! - **crash before ack re-reads** — a crash after the flush but before the
//!   checkpoint advances (`RIVET_TEST_PANIC_AT=cdc_after_flush_before_ack`, the
//!   same engine-agnostic sink hook the SQL engines use) must, on resume, re-read
//!   the un-acked changes: the destination is a complete superset of the source
//!   changes, never a gap.
//! - **soak dedup == source** — a workload of upserts + transactions (some
//!   touching one `_id` twice in ONE transaction) deduped STRICTLY by
//!   `(__pos, __seq)` must reproduce the source's current state exactly. Mongo's
//!   ordering differs from SQL: each event carries a DISTINCT, order-preserving
//!   resume token (`__pos`), so `__seq` is always 0 and `__pos` alone is the
//!   total order.
//! - **until_current drains and exits** — a bounded run captures the whole
//!   backlog and terminates (the regression guard for the `next_if_any`
//!   premature-stop backlog-drop the version matrix caught on 4.4).
//! - **resume** — a second run captures only what changed since.

use crate::common::*;

const PORT: u16 = 27018; // mongo-rs

/// `rivet cdc` against a Mongo replica set, typed Parquet output, bounded by
/// `--until-current`. Returns the process `Output` so a test can assert on it.
fn cdc_run(
    url: &str,
    table: &str,
    ckpt: &std::path::Path,
    out: &std::path::Path,
) -> std::process::Output {
    run_rivet(&[
        "cdc",
        "--source",
        url,
        "--table",
        table,
        "--checkpoint",
        ckpt.to_str().unwrap(),
        "--output",
        out.to_str().unwrap(),
        "--format",
        "parquet",
        "--until-current",
    ])
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_capture_resume_and_until_current_drain() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_cap");
    let m = MongoTest::connect(PORT, &db);
    let url = MongoTest::url(PORT, &db);
    let tbl = "t";
    m.drop_collection(tbl);

    let ck = tempfile::tempdir().unwrap();
    let ckpt = ck.path().join("c.ckpt");
    let out = tempfile::tempdir().unwrap();

    // Pin the anchor over a QUIET collection (idle first run) — must exit, not
    // block, and leave a checkpoint so the next run resumes from here.
    let r0 = cdc_run(&url, tbl, &ckpt, out.path());
    assert!(r0.status.success(), "idle until-current run must exit");
    assert!(
        ckpt.exists(),
        "a fresh checkpointed open must pin its anchor"
    );

    // Backlog: 3 inserts while no reader is running.
    m.upsert_set(tbl, 1, "v", "a");
    m.upsert_set(tbl, 2, "v", "b");
    m.upsert_set(tbl, 3, "v", "c");

    // until_current must DRAIN the whole backlog and exit (the 4.4 race guard).
    let r1 = cdc_run(&url, tbl, &ckpt, out.path());
    assert!(r1.status.success(), "resume until-current run must exit");
    let ids = dir_parquet_distinct_strings(out.path(), "_id");
    assert!(
        ["1", "2", "3"].iter().all(|i| ids.contains(*i)),
        "until_current dropped part of the backlog: got {ids:?}"
    );

    // A further run with nothing new captures zero — resume advanced past them.
    let out2 = tempfile::tempdir().unwrap();
    let r2 = cdc_run(&url, tbl, &ckpt, out2.path());
    assert!(
        r2.status.success(),
        "empty resume run failed. stderr:\n{}",
        String::from_utf8_lossy(&r2.stderr)
    );
    assert_eq!(
        total_parquet_rows(out2.path()),
        0,
        "resume must capture only new changes"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_crash_after_flush_before_ack_re_reads_on_resume() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_crash");
    let m = MongoTest::connect(PORT, &db);
    let url = MongoTest::url(PORT, &db);
    let tbl = "t";
    m.drop_collection(tbl);

    let ck = tempfile::tempdir().unwrap();
    let ckpt = ck.path().join("c.ckpt");
    let out = tempfile::tempdir().unwrap();

    // Pin, then 5 changes form the backlog.
    assert!(cdc_run(&url, tbl, &ckpt, out.path()).status.success());
    for i in 1..=5 {
        m.upsert_set(tbl, i, "v", &format!("x{i}"));
    }

    // Crash AFTER the flush, BEFORE the checkpoint advances: the changes are at
    // the destination but the resume token still points before them.
    let crashed = run_rivet_env(
        &[
            "cdc",
            "--source",
            &url,
            "--table",
            tbl,
            "--checkpoint",
            ckpt.to_str().unwrap(),
            "--output",
            out.path().to_str().unwrap(),
            "--format",
            "parquet",
            "--until-current",
        ],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")],
    );
    assert!(
        !crashed.status.success(),
        "the fault hook must crash the run"
    );

    // Resume (no fault): the un-acked changes are RE-READ. The destination is a
    // complete superset of the 5 source changes — no silent loss, at-least-once
    // the only allowed surplus.
    assert!(cdc_run(&url, tbl, &ckpt, out.path()).status.success());
    let ids = dir_parquet_distinct_strings(out.path(), "_id");
    for i in 1..=5 {
        assert!(ids.contains(&i.to_string()), "id {i} lost across the crash");
    }
    assert!(
        total_parquet_rows(out.path()) >= 5,
        "crash + resume must be at-least-once (superset), not lose rows"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_soak_dedup_matches_source_current_state() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_soak");
    let m = MongoTest::connect(PORT, &db);
    let url = MongoTest::url(PORT, &db);
    let tbl = "s";
    m.drop_collection(tbl);

    let ck = tempfile::tempdir().unwrap();
    let ckpt = ck.path().join("c.ckpt");
    let out = tempfile::tempdir().unwrap();

    // Pin over the empty collection, then run a mixed workload: 40 `_id`s
    // revisited 200 times, every fifth revision a TRANSACTION that touches the
    // same `_id` TWICE (…a then …b) — b must win the dedup.
    assert!(cdc_run(&url, tbl, &ckpt, out.path()).status.success());
    for r in 0..200_i64 {
        let id = r % 40;
        m.upsert_set(tbl, id, "v", &format!("r{r}"));
        if r % 5 == 0 {
            m.txn_updates(tbl, &[(id, "v", "txn_a"), (id, "v", &format!("t{r}b"))]);
        }
    }

    // Capture the whole log (a few bounded passes drain any tail).
    for _ in 0..3 {
        assert!(cdc_run(&url, tbl, &ckpt, out.path()).status.success());
    }

    // The independent oracle: dedup the captured change log STRICTLY by
    // (__pos, __seq) and compare to the source's actual current state.
    let deduped = mongo_deduped_field(read_mongo_cdc_changes(out.path()), "v");
    let source = m.current_state_i64(tbl, "v");
    assert_eq!(
        deduped, source,
        "deduped CDC change log must reproduce the source current state exactly \
         (intra-transaction ordering + no loss)"
    );
    assert_eq!(source.len(), 40, "all 40 _ids present");
}
