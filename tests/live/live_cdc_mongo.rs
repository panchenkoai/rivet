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
    Rig::mongo_cdc(table)
        .source_url(&MongoTest::url(PORT, db))
        .duckdb_oracle()
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_capture_resume_and_until_current_drain() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
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
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    assert!(
        ["1", "2", "3"].iter().all(|i| ids.contains(*i)),
        "until_current dropped part of the backlog: got {ids:?}"
    );

    // A further run into a FRESH destination sharing the checkpoint captures
    // zero — resume advanced past the backlog.
    let rig2 = cdc(&db, "t").checkpoint_path(rig.checkpoint());
    rig2.run_ok();
    assert_eq!(
        duckdb_declared_rows(rig2.oracle_dir()),
        0,
        "resume must capture only new changes"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_crash_after_flush_before_ack_re_reads_on_resume() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_crash");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let mut rig = cdc(&db, "t");
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

    // Resume into its OWN destination. Sharing one with the crashed run made this
    // assertion satisfiable by that run's ORPHANS: parts are durable BEFORE the hook
    // fires (roll_all flushes, then panics, then saves and acks), so a resume that
    // re-read NOTHING still found five ids under a `**/*.parquet` glob. This was
    // Mongo's only at-least-once evidence, and it could not fail — the CDC audit's
    // first finding, and the reason `resume_into_fresh_dest` exists on the rig.
    //
    // Scoped this way the assertion means what it says: every id below was captured
    // by the RESUME. RED-proven by deleting the resume run — "id 1 lost across the
    // crash", where the shared-destination version stayed green.
    rig.resume_into_fresh_dest();
    rig.run_ok();
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    for i in 1..=5 {
        assert!(ids.contains(&i.to_string()), "id {i} lost across the crash");
    }
    assert!(
        duckdb_declared_rows(rig.oracle_dir()) >= 5,
        "crash + resume must be at-least-once (superset), not lose rows"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_soak_dedup_matches_source_current_state() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
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
    require_alive(LiveService::DuckDb);
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
        duckdb_declared_distinct_set(rig.oracle_dir(), "_id").contains("42"),
        "the change made during a quiet first run must be captured on resume"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_update_and_delete_carry_document() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
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
    require_alive(LiveService::DuckDb);
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
                for id in duckdb_dir_parquet_distinct_strings(p.parent().unwrap(), "_id") {
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
    require_alive(LiveService::DuckDb);
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
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
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
    require_alive(LiveService::DuckDb);
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
    require_alive(LiveService::DuckDb);
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
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_hbound");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let rig = cdc(&db, "t");
    rig.run_ok(); // pin
    for i in 0..30 {
        m.upsert_set("t", i, "v", "backlog"); // pre-open backlog: _id 0..29
    }

    let bg_db = db.clone();
    let mut bg = BgWriter::spawn(move |stop_bg| {
        let w = MongoTest::connect(PORT, &bg_db);
        let mut i = 10_000;
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            w.upsert_set("t", i, "v", "live");
            i += 1;
            std::thread::sleep(std::time::Duration::from_millis(15));
        }
    });

    // BOUNDED, like every peer of this test shape (pg/mysql/mssql sustained-
    // writes all use run_rivet_bounded): Mongo is the LOAD-BEARING engine for
    // the open-bound — its tailable stream never empty-polls under sustained
    // writes, so a regression here HANGS run_ok forever, fed by the 15ms
    // upserter below (r3 bughunt: the one unbounded drain among four peers).
    let elapsed = run_rivet_bounded(&rig.config_path(), std::time::Duration::from_secs(30));
    bg.stop();
    let elapsed = elapsed.unwrap_or_else(|| {
        panic!("until_current drain HUNG past the 30s watchdog under sustained writes")
    });

    assert!(
        elapsed < std::time::Duration::from_secs(6),
        "until_current must terminate under sustained writes, took {elapsed:?}"
    );
    // The whole pre-open backlog (0..29) must be present — termination must NOT
    // come from dropping the backlog.
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
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
    // the process rules until_current rule names.
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
    require_alive(LiveService::DuckDb);
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
    let mut bg = BgWriter::spawn(move |stop_bg| {
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
    bg.stop();
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

    let got = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
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
    require_alive(LiveService::DuckDb);
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
    let ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
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
    require_alive(LiveService::DuckDb);
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
    require_alive(LiveService::DuckDb);
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

/// Per-FIELD presence profile against MONGODB ITSELF — the schemaless analogue
/// of the per-column NULL profile the SQL engines carry.
///
/// Mongo's CDC image is a JSON BLOB (`_id` + `document`), so "a column silently
/// became NULL" has no direct shape here. The equivalent silent loss is a FIELD
/// vanishing from the rendered document while every count still balances: the
/// change count matches, `_id`s all present, and one key is simply gone from the
/// post-images. The existing oracles cannot see it — the soak test deduplicates
/// on ONE field (`v`) and the tricky-BSON test reads a SINGLE document, so a
/// fault that drops a different field, or drops it in only some documents, is
/// invisible to both.
///
/// The oracle is MongoDB's own per-field presence count, compared against
/// DuckDB's `json_extract` over the captured `document` column — two readers
/// that share no code with rivet's renderer.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_per_field_presence_matches_the_source() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    use mongodb::bson::doc;
    let db = unique_name("cdc_fields");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("f");

    let rig = cdc(&db, "f");
    rig.run_ok(); // anchor over the empty collection

    // A POPULATION, not one document: a fault that drops a field in only some
    // renderings needs more than one to be distinguishable from a fixture typo.
    // `opt` is deliberately present on only half the documents, so the profile
    // has to match a NON-TRIVIAL number on at least one field — a check where
    // every field is present everywhere passes just as well when the extraction
    // is broken and returns everything.
    const DOCS: i64 = 24;
    let docs: Vec<_> = (1..=DOCS)
        .map(|i| {
            let mut d = doc! { "_id": i, "always": format!("a{i}"), "nested": doc!{ "k": i } };
            if i % 2 == 0 {
                d.insert("opt", format!("o{i}"));
            }
            d
        })
        .collect();
    m.insert_many("f", docs);
    for _ in 0..3 {
        rig.run_ok();
    }

    let captured = read_mongo_cdc_changes(&rig.out_dir()).len() as i64;
    assert!(
        captured >= DOCS,
        "fixture inert: only {captured} change(s) captured for {DOCS} inserted documents"
    );

    // Every post-image must CARRY its document. A delete has none by design, so
    // the profile is taken over the non-delete ops only.
    let missing_doc = duckdb_dir_scalar(
        &rig.out_dir(),
        "count(*) - count(\"document\")",
        Some("__op <> 'delete'"),
    );
    assert_eq!(
        missing_doc, 0,
        "{missing_doc} captured post-image(s) carry no document at all — a blob that \
         degrades to NULL is this model's whole-row silent loss"
    );

    for (field, expected) in [("always", DOCS), ("opt", DOCS / 2), ("nested", DOCS)] {
        // MongoDB's own answer, not a number this test assumes.
        let src = m.count_field_present("f", field) as i64;
        assert_eq!(
            src, expected,
            "fixture drifted: MongoDB reports {src} document(s) with '{field}', the fixture \
             builds {expected} — the comparison below would then grade the wrong population"
        );
        let dst = duckdb_dir_scalar(
            &rig.out_dir(),
            &format!("count(json_extract(\"document\", '$.{field}'))"),
            Some("__op <> 'delete'"),
        );
        assert_eq!(
            src, dst,
            "field '{field}': presence parity against MongoDB itself — source {src}, \
             captured {dst}. A renderer that drops a field moves this and nothing else; \
             the change count, the `_id` set and a single-field dedup all still balance."
        );
    }
}

/// A DELETE carries the PRE-IMAGE when the collection has one — the half of Mongo's
/// delete semantics that nothing asked about.
///
/// `mongo_cdc_update_and_delete_carry_document` asserts the opposite case and says
/// so: with pre-images off, a delete's `document` is NULL and the schema must allow
/// it. That is the DEFAULT, and it left the whole `full_document_before_change`
/// request — which rivet issues on every stream — verified by nothing. A silent
/// no-op and a working pre-image look identical from the default collection.
///
/// The distinction matters to a consumer: with a pre-image the delete tombstone
/// carries what the row WAS, so a loader can reconcile it against the destination;
/// without one it carries only `_id`.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs (6.0+ for pre-images)"]
fn mongo_cdc_delete_carries_the_pre_image_when_the_collection_has_one() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_preimg");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");
    // The collection must EXIST before collMod can change it.
    m.upsert_set("t", 9, "v", "seed");
    if !m.enable_pre_images("t") {
        // Said out loud rather than passed quietly: on a pre-6.0 server there is no
        // pre-image to carry and this cell is genuinely not applicable.
        skip_live(&format!(
            "server major {} has no changeStreamPreAndPostImages",
            m.server_major()
        ));
        return;
    }

    let rig = cdc(&db, "t");
    rig.run_ok(); // pin the anchor past the seed

    m.upsert_set("t", 9, "v", "doomed");
    m.delete_one("t", 9);
    rig.run_ok();

    let changes = read_mongo_cdc_changes(&rig.out_dir());
    let del = changes
        .iter()
        .find(|c| c.op == "delete")
        .unwrap_or_else(|| {
            panic!(
                "no delete captured — the fixture is inert ({} changes)",
                changes.len()
            )
        });
    assert_eq!(del.id, "9", "the delete must carry its _id");

    // MEASURED 2026-08-25 (MongoDB 7.0.37): {"_id":9,"v":"doomed"} — the pre-image
    // is the document as it was at the instant of the delete, not the seed value it
    // held two writes earlier. Both halves are asserted, because a `document` that
    // merely parses proves nothing about WHICH image arrived.
    assert!(
        del.document.contains("\"v\":\"doomed\""),
        "the delete must carry the PRE-IMAGE — the value the document held when it \
         was deleted. Got: {}",
        del.document
    );
    assert!(
        !del.document.contains("seed"),
        "...and the pre-image is the state at DELETE time, not an earlier one. \
         Got: {}",
        del.document
    );
}

/// The OTHER side of the same window: a crash after the checkpoint is persisted.
///
/// Mongo's `ack` is the trait's default no-op — only PostgreSQL consumes on read —
/// so its resume position is the checkpoint FILE alone. That makes
/// `cdc_after_checkpoint_before_ack` and `cdc_after_ack` the same instant for this
/// engine, and what protects it is not the ack but the ORDER: parts flushed, then
/// manifested, then the checkpoint saved. A crash anywhere in that sequence either
/// leaves the checkpoint behind the data (re-read, a duplicate) or everything
/// durable together.
///
/// Two ordering mutants were applied and this stayed GREEN against both —
/// `p.save(ck)` moved above the flush, and the ack moved above the checkpoint —
/// for a structural reason worth writing down rather than hiding: the fault hook
/// sits AFTER every durability step, so anything the crash could have skipped was
/// already written before it fired. Mongo's own model closes the rest (see
/// `TxnFramer::single_event_commit`): the resume token is per-event and
/// consume-free, so a checkpoint landing mid-transaction re-reads the tail rather
/// than skipping it the way a PG slot or an MSSQL from-LSN would.
///
/// What the body DOES grade is its two preconditions, and they are not decoration:
/// `leg1 < 6` fails if the crash left nothing behind, and a resume that delivers
/// NOTHING fails outright — which is what a checkpoint jumping past unread changes
/// would produce.
///
/// The sibling above (`cdc_after_flush_before_ack`) proves the duplicate side; this
/// proves there is no gap side. Both resume into a FRESH destination, because a
/// shared one is satisfied by the crashed run's own durable parts — Mongo's
/// at-least-once evidence used to be exactly that shape.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_crash_after_the_checkpoint_still_delivers_every_change() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let db = unique_name("cdc_ckcrash");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    let mut rig = cdc(&db, "t").cdc("rollover: 2");
    rig.run_ok(); // pin the anchor
    for i in 1..=6 {
        m.upsert_set("t", i, "v", &format!("x{i}"));
    }

    let crashed = rig.run_with_env("RIVET_TEST_PANIC_AT", "cdc_after_checkpoint_before_ack");
    assert!(
        !crashed.status.success(),
        "the fault hook must crash the run — a fault that did not fire leaves this \
         asserting an ordinary two-run export"
    );
    // The crash must have left work behind, or the resume below has nothing to
    // prove: with rollover 2 and six changes, run 1 checkpoints after the first
    // part and dies with four changes unread.
    let leg1 = duckdb_declared_rows(rig.oracle_dir());
    assert!(
        leg1 < 6,
        "run 1 delivered {leg1} of 6 before the crash — nothing was left for the \
         resume, so the union below would pass over a resume that captured zero"
    );

    // Leg 1's ids, read BEFORE the destination moves: unlike the sibling above, the
    // resume here legitimately starts PAST what the checkpoint already covered, so
    // the oracle is the union — and the union is exactly what at-least-once promises.
    let leg1_ids = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");

    rig.resume_into_fresh_dest();
    rig.run_ok();
    let resumed = duckdb_declared_distinct_set(rig.oracle_dir(), "_id");
    assert!(
        !resumed.is_empty(),
        "the resume leg delivered NOTHING — with four changes left unread after the \
         crash, an empty leg means the checkpoint jumped past them"
    );
    let mut union = leg1_ids.clone();
    union.extend(resumed.iter().cloned());
    for i in 1..=6 {
        assert!(
            union.contains(&i.to_string()),
            "id {i} was never delivered by EITHER leg — the checkpoint advanced past a \
             change whose part was not durable. leg1={leg1_ids:?} resume={resumed:?}"
        );
    }
}

/// A configured collection that does not exist must SAY so — today it is silence.
///
/// MEASURED 2026-08-25: `table: no_such_collection` against a database that holds
/// `real` ran to `status: success, rows: 0` with no warning of any kind. A typo and
/// a genuinely quiet window are indistinguishable, and the first is the one an
/// operator needs to hear about before they conclude "CDC works, there is just no
/// traffic".
///
/// A WARNING and not a refusal, and the asymmetry is the point: on Mongo a
/// collection is created by its first write, so capturing one that does not exist
/// YET is a legitimate and common setup — start the stream, then let the app create
/// it. Refusing would break that. What is not legitimate is saying nothing.
///
/// Mongo's stream is scoped to ONE database (`database(&db).watch()`), so the
/// cross-schema ambiguity the SQL engines refuse cannot arise here — this engine's
/// share of the resolution contract is the zero-match arm alone.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_warns_when_a_configured_collection_does_not_exist() {
    require_alive(LiveService::MongoRs);
    let db = unique_name("cdc_ghost");
    let m = MongoTest::connect(PORT, &db);
    // A real collection beside it, so the warning can name what IS there — a
    // "collection not found" that lists nothing is a message an operator cannot act
    // on when the cause is a typo.
    m.upsert_set("real", 1, "v", "here");

    let rig = cdc(&db, "no_such_collection");
    let said = rig.run_ok_capture();
    assert!(
        said.contains("no_such_collection"),
        "the run must name the collection it could not find. Got:\n{said}"
    );
    assert!(
        said.contains("real"),
        "...and name what the database DOES hold, because the cause is almost always \
         a typo and the fix is the neighbouring name. Got:\n{said}"
    );

    // Not too WIDE: a collection that exists must stay silent, or the warning fires
    // on every correct config and stops being read.
    let quiet = cdc(&db, "real");
    let out = quiet.run_ok_capture();
    assert!(
        !out.contains("could not find"),
        "an existing collection is the ordinary case and must not warn. Got:\n{out}"
    );
    // The OUTCOME too, and it has to be a real one. The first version of this
    // assertion read the GHOST rig's destination and demanded a row — which is
    // wrong twice over: that export captures `no_such_collection`, so zero is the
    // CORRECT answer, and the `real` write happened before either rig anchored, so
    // nothing was in bound for the other either. It was written to satisfy the
    // conformance gate without checking what the fixture does, which is the exact
    // defect this suite keeps finding in other people's tests.
    //
    // The claim being proven is "a missing collection WARNS rather than refusing",
    // and what makes that claim bite is that capture still WORKS afterwards — so
    // write AFTER the anchor and read it back.
    m.upsert_set("real", 2, "v", "after-anchor");
    quiet.run_ok();
    let delivered = read_mongo_cdc_changes(&quiet.out_dir()).len();
    assert_eq!(
        delivered, 1,
        "the warning path must not wedge capture — a change written after the anchor \
         has to arrive, or `warning, not refusal` is an empty claim"
    );
}

/// `rivet cdc --output --format csv` on Mongo — the SUBCOMMAND's text writer over
/// a JSON document column.
///
/// The cdc-cli-surface ledger measured the subcommand's coverage as `mysql 11 of
/// 13 flags · mssql 4 · postgres 3 · mongo 0`, and this is the Mongo cell worth
/// closing first. Every other engine's CDC row is flat columns the CSV writer has
/// seen a thousand times; Mongo's payload is one `document` column holding the
/// whole BSON as JSON — braces, quotes, commas, and whatever the application put
/// in a string. That is the CSV writer's hostile input, and the two ways it fails
/// are the two this repo's csv-fidelity ledger already caught elsewhere: a value
/// silently truncated, or an un-escaped delimiter splitting one row into two.
///
/// The oracle is DuckDB's `read_csv_auto` plus hard-coded expected values —
/// deliberately NOT the `csv` crate, which is what rivet writes with (one library
/// round-tripping itself grades the pair's agreement, not the file's correctness),
/// and deliberately not rivet's own read-back.
///
/// FIVE flags are graded here, and each one BITES rather than merely appearing on
/// the command line — the distinction the ledger's `test` state is worth nothing
/// without:
///   `--source`      wrong/absent → no connection, no rows
///   `--table`       ignored → the neighbouring collection's write appears (5 rows)
///   `--checkpoint`  ignored → run 2 re-anchors to now and captures 0
///   `--output`      ignored → NDJSON on stdout, no file to read
///   `--format csv`  ignored → parquet, and the CSV oracle finds nothing to open
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_cli_writes_a_faithful_csv_of_the_document_column() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    use mongodb::bson::doc;
    let db = unique_name("cdc_cli_csv");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("docs");
    m.drop_collection("other");

    let d = tempfile::tempdir().expect("tempdir");
    let ckpt = d.path().join("cli.ckpt");
    let url = MongoTest::url(PORT, &db);
    // The destination is the SHARED workdir, not the tempdir: the DuckDB oracle
    // reads from inside a container, and a path it cannot see reads as zero rows —
    // the harness bug that looks exactly like data loss.
    let (host, container) = live_shared_workdir(&unique_name("mongo_cli_csv"));

    // Run 1 pins the resume token. Mongo without a checkpoint starts at the
    // source's CURRENT position, so a bounded run's open-time snapshot IS its
    // anchor and nothing written afterwards is in bound — without this leg the
    // test could only ever capture zero, whatever the writer did.
    let anchor = run_rivet_args_bounded_env(
        &[
            "cdc",
            "--source",
            &url,
            "--table",
            "docs",
            "--checkpoint",
            ckpt.to_str().unwrap(),
        ],
        &[],
        std::time::Duration::from_secs(60),
    );
    assert!(anchor.is_some(), "the anchoring run did not terminate");
    assert!(ckpt.is_file(), "run 1 wrote no checkpoint to resume from");

    // The hostile payloads, one per CSV failure mode. The newline is the one that
    // corrupts silently: un-escaped it splits a row in two, and every count still
    // looks plausible.
    m.insert_many(
        "docs",
        vec![
            doc! { "_id": 11_i64, "note": "has,comma" },
            doc! { "_id": 12_i64, "note": "has\"quote" },
            doc! { "_id": 13_i64, "note": "line\nbreak" },
            doc! { "_id": 14_i64, "nested": doc! { "a": 1_i64, "b": [1_i64, 2_i64] } },
        ],
    );
    // A write the config did NOT ask for, so `--table` has something to exclude.
    m.insert_many("other", vec![doc! { "_id": 99_i64, "note": "not mine" }]);

    let out = run_rivet_args_bounded_env(
        &[
            "cdc",
            "--source",
            &url,
            "--table",
            "docs",
            "--checkpoint",
            ckpt.to_str().unwrap(),
            "--output",
            host.to_str().unwrap(),
            "--format",
            "csv",
        ],
        &[],
        std::time::Duration::from_secs(90),
    );
    assert!(out.is_some(), "the capturing run did not terminate");
    assert!(
        !files_with_extension(&host, "csv").is_empty(),
        "no .csv here — an oracle that shrugged at an empty set would grade nothing. \
         This one assert catches TWO of the graded flags, MEASURED: `--format \
         parquet` leaves the directory holding parquet, and dropping `--checkpoint` \
         re-anchors run 2 at now, so it captures zero events and the sink writes no \
         file at all"
    );

    let v = duckdb_run_sql_json(&format!(
        "SELECT _id, document FROM read_csv_auto('{container}/**/*.csv', header=true) ORDER BY _id"
    ));
    let rows = v["rows"].as_array().cloned().unwrap_or_default();
    let got: std::collections::BTreeMap<i64, serde_json::Value> = rows
        .iter()
        .filter_map(|r| {
            let id = r.get(0)?.as_str()?.parse::<i64>().ok()?;
            let doc: serde_json::Value = serde_json::from_str(r.get(1)?.as_str()?).ok()?;
            Some((id, doc))
        })
        .collect();

    assert_eq!(
        got.keys().copied().collect::<Vec<_>>(),
        vec![11, 12, 13, 14],
        "expected exactly the four documents written to the CONFIGURED collection. \
         Id 99 is the neighbouring collection and means the routing filter let it \
         through (`--table` cannot simply be dropped here — the subcommand refuses \
         `--output` without exactly one, MEASURED, so the mutant that grades this \
         line is pointing it at `other`, which yields [99]); zero means the \
         checkpoint never resumed; a row lost or gained anywhere else means the CSV \
         split. Got: {got:#?}"
    );

    // Hard-coded expectations, not a re-render of what rivet produced.
    assert_eq!(got[&11]["note"], serde_json::json!("has,comma"));
    assert_eq!(got[&12]["note"], serde_json::json!("has\"quote"));
    assert_eq!(got[&13]["note"], serde_json::json!("line\nbreak"));
    assert_eq!(got[&14]["nested"], serde_json::json!({"a": 1, "b": [1, 2]}));

    m.drop_collection("docs");
    m.drop_collection("other");
}

/// A dotted collection name is one collection, not a schema qualifier — and the
/// sibling it used to also match must stay out.
///
/// Round-3B bughunt, two defects meeting in one fixture. The config layer REFUSED
/// every dotted Mongo collection under `mode: cdc`, on a reason that had stopped
/// being true: the refusal was written when `table_matches` split the name into a
/// bogus `schema.table` and routed zero events, the ROUTER was then fixed to try
/// the full name first, and the guard stayed — refusing a working capture and
/// telling the operator to rename a production collection.
///
/// Underneath it hid the real defect. With the split arm still live for Mongo, a
/// collection whose first segment equals the DATABASE name matched TWICE: `table:
/// <db>.orders` took both the collection literally named `<db>.orders` and the
/// sibling `orders`. Two collections interleaved into one destination, `status:
/// success`, and no count could show it — each collection's rows are individually
/// plausible.
///
/// Mongo has no schema to qualify with, so the split arm no longer runs there at
/// all. This test pins both halves at once: the dotted name captures, and only it.
#[test]
#[ignore = "live: requires docker compose up -d mongo-rs"]
fn mongo_cdc_captures_a_dotted_collection_without_swallowing_its_sibling() {
    require_alive(LiveService::MongoRs);
    use mongodb::bson::doc;
    // The database name is the FIRST SEGMENT of the dotted collection — the exact
    // shape that used to match twice. A neutral prefix would prove nothing.
    let db = unique_name("dotted").to_lowercase();
    let dotted = format!("{db}.orders");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection(&dotted);
    m.drop_collection("orders");

    // `initial: snapshot` is not incidental: the snapshot leg is the one that goes
    // through `finalize`, and its manifest is where the engine-blind name split
    // recorded a different collection's identity. Without it the manifest assertion
    // below grades nothing — PROVEN by mutating the fix back and watching this test
    // stay green until the leg was added.
    let rig = cdc(&db, &dotted).cdc_line("initial: snapshot");
    rig.run_ok(); // anchor
    m.insert_many(&dotted, vec![doc! { "_id": 1_i64, "who": "dotted" }]);
    // The sibling the split arm used to pull in.
    m.insert_many("orders", vec![doc! { "_id": 2_i64, "who": "sibling" }]);
    rig.run_ok();

    let who: std::collections::BTreeSet<String> = read_mongo_cdc_changes(&rig.out_dir())
        .iter()
        .filter_map(|c| {
            serde_json::from_str::<serde_json::Value>(&c.document)
                .ok()?
                .get("who")?
                .as_str()
                .map(|s| s.to_string())
        })
        .collect();
    assert_eq!(
        who,
        ["dotted"]
            .iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>(),
        "the dotted collection must be captured (a config that no longer even \
         LOADS would give an empty set) and the sibling `orders` must not be — \
         `sibling` appearing here is the interleave, which counts cannot see"
    );

    // Both LEGS must agree about the identity they recorded. `finalize` derived
    // `schema`/`table` by splitting on the first dot regardless of engine, so the
    // snapshot leg's manifest named `{schema: "<db>", table: "orders"}` — the
    // SIBLING collection, which exists and holds different rows — while the drain
    // recorded the whole string. `identity_source` re-joins with a dot, so both
    // render the same source id and the single-source guard sees nothing; only a
    // consumer reading the two fields separately meets the contradiction.
    {
        let p = rig.out_dir().join("snapshot").join("manifest.json");
        assert!(
            p.is_file(),
            "the snapshot leg must have produced a manifest — without one this \
             assertion is vacuous, which is exactly how the defect survived"
        );
        let doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&p).expect("read manifest"))
                .expect("parse manifest");
        assert_eq!(
            doc["source"]["schema"],
            serde_json::Value::Null,
            "a MongoDB manifest must carry NO schema — the database is not one, and \
             recording the first dot-segment as a schema names a different \
             collection. Got: {}",
            doc["source"]
        );
        assert_eq!(
            doc["source"]["table"].as_str(),
            Some(dotted.as_str()),
            "and the table must be the collection's WHOLE name: {}",
            doc["source"]
        );
    }

    m.drop_collection(&dotted);
    m.drop_collection("orders");
}
