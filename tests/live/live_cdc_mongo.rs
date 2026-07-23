//! Live CDC coverage for the MongoDB change-stream engine, on the canonical
//! [`Rig`] in **config mode** (`mode: cdc`) — the same path the SQL CDC suites
//! use, so all four engines' CDC runs go through one harness. Requires the
//! single-node replica set (`docker compose up -d mongo-rs`).
//!
//! What is proven here (each an at-least-once / no-loss invariant, checked by
//! RE-READING the destination, never rivet's counters):
//!
//! - **crash before ack re-reads** — a crash after the flush but before the
//!   checkpoint advances (`RIVET_TEST_PANIC_AT=cdc_after_flush_before_ack`, the
//!   engine-agnostic sink hook) must, on resume, re-read the un-acked changes.
//! - **soak dedup == source** — upserts + transactions (some touching one `_id`
//!   twice in ONE transaction) deduped STRICTLY by `(__pos, __seq)` reproduce the
//!   source's current state. Mongo gives every event a DISTINCT `__pos`, so
//!   `__seq` is always 0 and `__pos` alone is the total order.
//! - **until_current drains and exits**, **resume** captures only what changed,
//!   **idle-first-run** still pins the anchor, **update/delete carry the image**,
//!   **initial snapshot** covers pre-existing docs, **mixed transaction** routes
//!   only the captured collection.

use crate::common::*;

const PORT: u16 = 27018; // mongo-rs

/// A `mode: cdc` Rig (until_current + checkpoint) over `table` in a fresh db.
fn cdc(db: &str, table: &str) -> Rig {
    Rig::mongo_cdc(table).source_url(&MongoTest::url(PORT, db))
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_capture_resume_and_until_current_drain() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_cap");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    // Pin the anchor over a QUIET collection (idle first run) — must exit and
    // leave a checkpoint so the next run resumes from here.
    let rig = cdc(&db, "t");
    rig.run_ok();
    assert!(
        rig.checkpoint().exists(),
        "a fresh checkpointed open must pin its anchor"
    );

    // Backlog: 3 inserts while no reader is running.
    m.upsert_set("t", 1, "v", "a");
    m.upsert_set("t", 2, "v", "b");
    m.upsert_set("t", 3, "v", "c");

    // until_current must DRAIN the whole backlog and exit (the 4.4 race guard).
    rig.run_ok();
    let ids = dir_parquet_distinct_strings(&rig.out_dir(), "_id");
    assert!(
        ["1", "2", "3"].iter().all(|i| ids.contains(*i)),
        "until_current dropped part of the backlog: got {ids:?}"
    );

    // A further run into a FRESH destination sharing the checkpoint captures
    // zero — resume advanced past the backlog.
    let rig2 = cdc(&db, "t").checkpoint_path(rig.checkpoint());
    rig2.run_ok();
    assert_eq!(
        total_parquet_rows(&rig2.out_dir()),
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
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok(); // pin
    for i in 1..=5 {
        m.upsert_set("t", i, "v", &format!("x{i}"));
    }

    // Crash AFTER the flush, BEFORE the checkpoint advances: the changes are at
    // the destination but the resume token still points before them.
    let crashed = rig.run_with_env("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack");
    assert!(
        !crashed.status.success(),
        "the fault hook must crash the run"
    );

    // Resume: the un-acked changes are RE-READ. The destination is a complete
    // superset of the 5 source changes — no silent loss, at-least-once the only
    // allowed surplus.
    rig.run_ok();
    let ids = dir_parquet_distinct_strings(&rig.out_dir(), "_id");
    for i in 1..=5 {
        assert!(ids.contains(&i.to_string()), "id {i} lost across the crash");
    }
    assert!(
        total_parquet_rows(&rig.out_dir()) >= 5,
        "crash + resume must be at-least-once (superset), not lose rows"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_soak_dedup_matches_source_current_state() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_soak");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("s");

    // Pin over the empty collection, then a mixed workload: 40 `_id`s revisited
    // 200 times, every fifth revision a TRANSACTION touching the same `_id` TWICE
    // (…a then …b) — b must win the dedup.
    let rig = cdc(&db, "s");
    rig.run_ok();
    for r in 0..200_i64 {
        let id = r % 40;
        m.upsert_set("s", id, "v", &format!("r{r}"));
        if r % 5 == 0 {
            m.txn_updates("s", &[(id, "v", "txn_a"), (id, "v", &format!("t{r}b"))]);
        }
    }

    // Capture the whole log (a few bounded passes drain any tail).
    for _ in 0..3 {
        rig.run_ok();
    }

    // The independent oracle: dedup the captured change log STRICTLY by
    // (__pos, __seq) and compare to the source's actual current state.
    let deduped = mongo_deduped_field(read_mongo_cdc_changes(&rig.out_dir()), "v");
    let source = m.current_state_i64("s", "v");
    assert_eq!(
        deduped, source,
        "deduped CDC change log must reproduce the source current state exactly \
         (intra-transaction ordering + no loss)"
    );
    assert_eq!(source.len(), 40, "all 40 _ids present");
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_idle_first_run_then_change_is_captured() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_idle");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    // Enable CDC over a QUIET collection: the first run captures zero changes but
    // MUST pin its anchor (Mongo has no server-side anchor — the MySQL model), or
    // the next run would re-anchor at "current" and skip everything since.
    let rig = cdc(&db, "t");
    rig.run_ok();
    assert!(
        rig.checkpoint().exists(),
        "an idle first run must still pin the anchor"
    );

    // A single change lands during the quiet period, captured on resume.
    m.upsert_set("t", 42, "v", "after_quiet_enable");
    rig.run_ok();
    assert!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").contains("42"),
        "the change made during a quiet first run must be captured on resume"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_update_and_delete_carry_document() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_ud");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok(); // pin

    // Phase 1 — insert + update, captured WHILE the doc still exists, so the
    // update's UpdateLookup returns the post-image (a later delete would make it
    // current-state NULL — the documented UpdateLookup caveat, not a loss).
    m.upsert_set("t", 7, "v", "created");
    m.upsert_set("t", 7, "v", "updated");
    rig.run_ok();
    // Phase 2 — delete, captured after. `document` is NULL (no pre-image
    // configured) — the schema MUST allow it (regression: a non-nullable
    // `document` errored the whole run on this exact delete).
    m.delete_one("t", 7);
    rig.run_ok();

    let changes = read_mongo_cdc_changes(&rig.out_dir());
    let ops: Vec<&str> = changes.iter().map(|c| c.op.as_str()).collect();
    assert!(ops.contains(&"insert"), "insert op captured: {ops:?}");
    assert!(ops.contains(&"update"), "update op captured: {ops:?}");
    assert!(ops.contains(&"delete"), "delete op captured: {ops:?}");
    // The UPDATE (captured before the delete) carries the post-image document.
    let upd = changes.iter().find(|c| c.op == "update").unwrap();
    assert!(
        upd.document.contains("updated"),
        "update must carry the post-image document, got: {}",
        upd.document
    );
    let del = changes.iter().find(|c| c.op == "delete").unwrap();
    assert_eq!(del.id, "7", "delete must carry the _id");
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_initial_snapshot_covers_preexisting_rows() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_snap");
    let m = MongoTest::connect(PORT, &db);
    // Pre-existing rows the stream alone would NOT see (they predate the anchor);
    // `initial: snapshot` must copy them before the CDC drain.
    m.seed_int_id("t", 500);

    let rig = cdc(&db, "t").cdc("initial: snapshot").cdc("max_events: 1");
    // A change so the bounded CDC leg has something to drain and exit on.
    m.upsert_set("t", 1, "v", "touched");
    rig.run_ok();

    // The snapshot leg wrote the pre-existing rows under `<dest>/snapshot/`.
    let snap_ids = walkdir_parquet_ids(&rig.out_dir(), "snapshot");
    assert_eq!(
        snap_ids.len(),
        500,
        "initial snapshot must cover all pre-existing rows"
    );
}

/// Distinct `_id` values across `.parquet` files under any subdir of `root`
/// whose path contains `marker` (e.g. the `snapshot/` handoff dir).
fn walkdir_parquet_ids(root: &std::path::Path, marker: &str) -> std::collections::BTreeSet<String> {
    let mut ids = std::collections::BTreeSet::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "parquet")
                && p.to_string_lossy().contains(marker)
            {
                for id in dir_parquet_distinct_strings(p.parent().unwrap(), "_id") {
                    ids.insert(id);
                }
            }
        }
    }
    ids
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_mixed_transaction_ending_on_uncaptured_table() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_mix");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("orders");
    m.drop_collection("audit");

    // Watch is whole-database; the `table: orders` config filters routing to it.
    let rig = cdc(&db, "orders");
    rig.run_ok(); // pin

    // ONE transaction touching the CAPTURED collection then ending on an
    // UNCAPTURED one — the captured change must appear, the uncaptured must not
    // leak, and the run must not stall on the boundary.
    m.txn_two_collections("orders", 1, "audit", 99);

    rig.run_ok();
    let ids = dir_parquet_distinct_strings(&rig.out_dir(), "_id");
    assert!(
        ids.contains("1"),
        "the captured-collection change must appear"
    );
    assert!(
        !ids.contains("99"),
        "the uncaptured collection must NOT leak into the orders stream"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_corrupt_checkpoint_fails_loudly_not_silent_reanchor() {
    // A corrupt / unreadable checkpoint was swallowed (`.ok().flatten()`) and
    // treated as "no checkpoint" → re-anchor at now → silent gap. It must fail
    // loudly instead (bug-hunt find).
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_corrupt");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok(); // pin — writes the checkpoint
    assert!(rig.checkpoint().exists(), "run pins a checkpoint");

    // Corrupt it, then produce a change the re-anchor would skip.
    std::fs::write(rig.checkpoint(), b"{ not valid json at all").unwrap();
    m.upsert_set("t", 1, "v", "a");

    // The run must FAIL — never exit 0 having silently re-anchored past the change.
    let _stderr = rig.run_expect_fail();
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_pos_column_leads_with_data_for_downstream_sort() {
    // `__pos` must lead with `{"_data"` so a downstream MERGE that `ORDER BY
    // __pos` sorts in oplog order — `_data` is the order-preserving resume
    // keystring, whereas a `rt`-first `__pos` sorts by the full token (whose hex
    // is not length-stable) and mis-orders the dedup (bug-hunt find).
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_posdata");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok(); // pin
    for i in 1..=4 {
        m.upsert_set("t", i, "v", "x");
    }
    rig.run_ok();

    let changes = read_mongo_cdc_changes(&rig.out_dir());
    assert!(!changes.is_empty(), "changes captured");
    for c in &changes {
        assert!(
            c.pos.starts_with("{\"_data\""),
            "__pos must be _data-first for a correct downstream sort, got: {}",
            c.pos
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_until_current_terminates_under_sustained_writes_and_keeps_backlog() {
    // A bounded run must (1) TERMINATE at the open-time cluster-time bound even
    // while writes keep arriving — the drain loop used to check its stop
    // condition only on an empty poll, so continuous writes hung it forever
    // (bug-hunt H) — and (2) still capture the pre-open backlog (a naive bound
    // dropped it). Assert both.
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_hbound");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok(); // pin
    for i in 0..30 {
        m.upsert_set("t", i, "v", "backlog"); // pre-open backlog: _id 0..29
    }

    let bg_db = db.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_bg = stop.clone();
    let bg = std::thread::spawn(move || {
        let w = MongoTest::connect(PORT, &bg_db);
        let mut i = 10_000;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            w.upsert_set("t", i, "v", "live");
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    let start = std::time::Instant::now();
    rig.run_ok(); // must return at the time bound, not hang until the writer stops
    let elapsed = start.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = bg.join();

    assert!(
        elapsed < std::time::Duration::from_secs(6),
        "until_current must terminate under sustained writes, took {elapsed:?}"
    );
    // The whole pre-open backlog (0..29) must be present — termination must NOT
    // come from dropping the backlog.
    let ids = dir_parquet_distinct_strings(&rig.out_dir(), "_id");
    for i in 0..30 {
        assert!(
            ids.contains(&i.to_string()),
            "backlog _id {i} must be captured, got {} ids",
            ids.len()
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_mongo_cdc_until_current_open_bound_two_runs_lose_nothing() {
    // The matrix cell cdc_until_current_open_bound_two_runs was `na` on Mongo,
    // claiming joint coverage by the terminates-under-writes test (which has NO
    // run 2 and never asserts the live tail) and the resume-drain test (whose
    // run 2 drains a ZERO tail — no live writer). Neither asserts the DEFER-NOT-
    // DROP UNION the SQL peers do (roast_{mysql,pg}_until_current_open_bound_two_
    // runs_lose_nothing): run 1 stops at a PREFIX of a live-writer stream, run 2
    // drains the deferred tail, and the distinct-id union re-read from the parquet
    // equals the SOURCE id set. This test completes the per-engine union set the
    // CLAUDE.md until_current rule names.
    //
    // Two contracts here, with different weights (per that rule):
    //  - TERMINATION is LOAD-BEARING on Mongo: the open-time cluster-time bound
    //    clips a tailable stream that would otherwise never empty-poll under
    //    sustained writes. The `run 1 terminates` assert goes RED if the bound is
    //    disabled (run 1 hangs, killed at 30s) — the real RED lever here.
    //  - DEFER-NOT-DROP (the union) is a BELT-AND-SUSPENDERS confirmation, NOT a
    //    silent-loss guard: Mongo's checkpoint is the last EMITTED event's own
    //    resume token (sink-driven; the idle-anchor pin fires only at a fresh
    //    open, never over-advancing at close), and the deferred tail is strictly
    //    AFTER it, so run 2 always recovers it — structural immunity via per-event
    //    tokens, the same reason cdc_large_transaction_atomic_across_crash is `na`
    //    on Mongo. The union assert can't go RED against a one-line clip mutant;
    //    it guards a future refactor away from per-event tokens. Oracle: the
    //    source collection, never rivet's own counters.
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_mongoob");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok(); // pin over the quiet collection, before the backlog

    // Pre-open backlog: _id 0..29.
    for i in 0..30 {
        m.upsert_set("t", i, "v", "backlog");
    }

    // A writer floods _id 10000+ THROUGH run 1, so run 1's open-time cluster-time
    // bound falls mid-stream and it must terminate on a PREFIX, deferring the tail.
    let bg_db = db.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_bg = stop.clone();
    let bg = std::thread::spawn(move || {
        let w = MongoTest::connect(PORT, &bg_db);
        let mut i = 10_000i64;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            w.upsert_set("t", i, "v", "live");
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    let cfg = rig.config_path();
    // Run 1 must TERMINATE under sustained writes (the cluster-time bound clips
    // it); killed at 30s if it hangs.
    let elapsed = run_rivet_bounded(&cfg, std::time::Duration::from_secs(30));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = bg.join();
    assert!(
        elapsed.is_some(),
        "run 1 must terminate at the open-time cluster-time bound under sustained writes (killed at 30s)"
    );

    // Writer stopped ⇒ every committed change now predates run 2's own bound.
    let elapsed2 = run_rivet_bounded(&cfg, std::time::Duration::from_secs(60));
    assert!(
        elapsed2.is_some(),
        "run 2 (no writers) must drain the deferred tail and exit"
    );

    let got = dir_parquet_distinct_strings(&rig.out_dir(), "_id");
    let want: std::collections::BTreeSet<String> = m
        .current_state_i64("t", "v")
        .into_keys()
        .map(|k| k.to_string())
        .collect();
    assert_eq!(
        got, want,
        "run1 ∪ run2 must hold exactly the source's committed _ids — the open-time \
         bound defers the tail to run 2, never drops it"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_uncaptured_collection_drop_does_not_wedge_capture() {
    // A whole-db watch also sees DDL (`drop`/`rename`) for OTHER collections.
    // The op mapping used to `bail!` on any non-row op, so dropping an uncaptured
    // collection failed the whole run — and every resume re-hit it: a wedge
    // (bug-hunt G). DDL is now skipped; the captured collection keeps flowing.
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_ddl");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("orders");
    m.drop_collection("scratch");
    m.upsert_set("scratch", 1, "v", "x"); // exists so the drop is a real DDL event

    let rig = cdc(&db, "orders");
    rig.run_ok(); // pin

    // A change to the captured collection, then a DROP of the UNCAPTURED one.
    m.upsert_set("orders", 1, "v", "a");
    m.drop_collection("scratch"); // DDL event on the shared db-watch
    m.upsert_set("orders", 2, "v", "b");

    // Must NOT bail on the drop — both orders changes captured.
    rig.run_ok();
    let ids = dir_parquet_distinct_strings(&rig.out_dir(), "_id");
    assert!(
        ids.contains("1") && ids.contains("2"),
        "captured collection must keep flowing across an uncaptured drop, got: {ids:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn roast_checkpoint_advances_on_uncaptured_only_traffic() {
    // The whole-db watch sees commits for UNCAPTURED collections too. The commit
    // boundary advances `last_commit` before routing, but no captured buffer ever
    // rolled — so the checkpoint never moved and every cycle re-read the whole
    // uncaptured backlog until the oplog rolled past it (bug-hunt K). The final
    // roll now fires on an unacked commit even with empty buffers.
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_kstall");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("orders");
    m.drop_collection("audit");

    let rig = cdc(&db, "orders");
    rig.run_ok(); // pin — writes the checkpoint at the current position
    let ckpt_after_pin = std::fs::read(rig.checkpoint()).unwrap();

    // Traffic on the UNCAPTURED collection only.
    for i in 0..20 {
        m.upsert_set("audit", i, "v", "log");
    }

    // A bounded run captures nothing for `orders`, but MUST advance the checkpoint
    // past the audit commits — otherwise the next run re-reads them forever.
    rig.run_ok();
    let ckpt_after_run = std::fs::read(rig.checkpoint()).unwrap();

    assert_ne!(
        ckpt_after_pin, ckpt_after_run,
        "the checkpoint must advance past uncaptured-only traffic, not stall"
    );
}

/// Audit blind cell (Mongo CDC per-type value fidelity): the only prior CDC
/// update/delete test seeds plain STRINGS, so a change-stream relaxed-vs-canonical
/// extJSON drift, or a Decimal128 rounding, on the OP PATH — distinct from the
/// batch verbatim rendering that `mongo_batch_type_fidelity_document_is_verbatim_
/// extjson` pins — was un-oracled. Insert a tricky-typed doc WHILE CDC captures it,
/// then UPDATE it (the change-stream UpdateLookup post-image goes through the same
/// rendering), and assert every captured `document` carries the SAME verbatim
/// extJSON the batch oracle requires: a large Int64 > 2^53, a Decimal128, nested
/// unicode. This is the independent oracle Mongo has no Form A for (the document
/// is a verbatim blob).
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_change_stream_renders_tricky_bson_verbatim_like_batch() {
    require_alive(LiveService::MongoRs);
    use mongodb::bson::{Bson, doc};
    let db = unique_name("cdc_types");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok(); // anchor over the empty collection (idle first run)

    // Tricky-typed doc inserted AFTER the anchor, so the change stream carries it.
    m.insert_many(
        "t",
        vec![doc! {
            "_id": 1_i64,
            "i64_big": 9_007_199_254_740_993_i64, // 2^53 + 1 — an f64 parser would round it
            "dec": Bson::Decimal128("123456789.987654321012345".parse().unwrap()),
            "nested": doc! { "k": "v-\u{00e9}\u{4e2d}", "arr": [1_i32, 2_i32, 3_i32] },
        }],
    );
    // Update it too — the UpdateLookup post-image returns the WHOLE doc, so its
    // rendering of the tricky types is the specific op-path concern.
    m.upsert_set("t", 1, "note", "changed");
    rig.run_ok(); // capture insert + update

    let changes = read_mongo_cdc_changes(&rig.out_dir());
    let with_doc: Vec<&MongoCdcChange> = changes
        .iter()
        .filter(|c| c.document.contains("i64_big"))
        .collect();
    assert!(
        !with_doc.is_empty(),
        "the change stream must capture at least one post-image carrying the tricky \
         document; captured {} change(s)",
        changes.len()
    );
    // The independent oracle: the SAME relaxed extended JSON the test renders with
    // the bson library directly (NOT rivet's document_to_json). The CDC op path
    // must not DRIFT from this default relaxed rendering (the audit's exact
    // concern) — so the large Int64 stays a bare, verbatim number, never the
    // canonical `$numberLong` a drifted op path would emit.
    for ch in with_doc {
        assert!(
            ch.document.contains("9007199254740993") && !ch.document.contains("$numberLong"),
            "large Int64 must render VERBATIM as the default relaxed bare number in the CDC \
             document (op '{}') — a canonical/relaxed DRIFT on the op path (or f64 rounding) \
             corrupts it; got: {}",
            ch.op,
            ch.document
        );
        assert!(
            ch.document.contains("123456789.987654321012345")
                && ch.document.contains("$numberDecimal"),
            "Decimal128 must be VERBATIM + type-tagged (`$numberDecimal`) in the CDC document \
             (op '{}'); got: {}",
            ch.op,
            ch.document
        );
        assert!(
            ch.document.contains("v-\u{00e9}\u{4e2d}") && ch.document.contains("arr"),
            "nested unicode + array must be VERBATIM in the CDC document (op '{}'); got: {}",
            ch.op,
            ch.document
        );
    }
}
