//! Live coverage for the MongoDB BATCH read path (JSON-blob model), built on the
//! canonical [`Rig`]. Requires the standalone mongo (`docker compose up -d
//! mongo`). Every check re-reads the destination Parquet (distinct `_id` set /
//! verbatim document text), never rivet's own counters — the test-self-oracle rule.
//!
//! Invariants proven:
//! - **blob round-trip** — every document exports as `_id` + `document`, the
//!   distinct `_id` set equals the source.
//! - **keyset no loss/dup** — `page_size` paging over the whole collection loses
//!   and duplicates nothing across page boundaries.
//! - **parallel any-`_id`** — `parallel: N` `_id`-range fan-out reads the whole
//!   collection with no gap/overlap, for integer / String / ObjectId `_id`.
//! - **typed cursor** — keyset pages a STRING `_id` collection (impossible before
//!   the typed-cursor token; the display-column round-trip would mis-order it).
//! - **type fidelity** — a document of tricky BSON types exports with the exact
//!   extended-JSON TEXT (large `Int64 > 2^53`, `Decimal128`) verbatim, so a
//!   downstream `PARSE_JSON` (BigQuery / Snowflake) reconstructs it losslessly.
//! - **reconcile / resume / heterogeneous-`_id` guard** — the operational surface.

use crate::common::*;

const PORT: u16 = 27017; // standalone mongo

/// A batch Rig pointed at collection `table` in a fresh per-test db.
fn batch(db: &str, table: &str) -> Rig {
    Rig::mongo_batch(table).source_url(&MongoTest::url(PORT, db))
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_keyset_no_loss_or_dup() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mkeyset");
    MongoTest::connect(PORT, &db).seed_int_id("bench", 5000);

    // page_size 2000 → 3 pages: exercises page boundaries.
    let rig = batch(&db, "bench").mongo("page_size: 2000");
    rig.run_ok();

    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        5000,
        "distinct _id must equal source (no loss)"
    );
    // Distinct count alone hides duplicates (a doc emitted twice keeps the same
    // distinct set) — assert total == distinct so a dup fails loudly (bug-hunt:
    // the four 'no loss OR dup' tests only checked distinct).
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        5000,
        "total rows must equal distinct — no duplicate export"
    );
    assert!(
        dir_parquet_has_column(&rig.out_dir(), "document"),
        "blob document column present"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_parallel_integer_id_no_loss_or_dup() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mpar");
    MongoTest::connect(PORT, &db).seed_int_id("bench", 5000);

    // parallel: 4 on an INTEGER _id — impossible before Bson range bounds.
    let rig = batch(&db, "bench")
        .mongo("page_size: 1000")
        .export_line("parallel: 4");
    rig.run_ok();

    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        5000,
        "parallel _id-range union must be the whole collection"
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        5000,
        "parallel ranges must not double-count at a boundary (total == distinct)"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_typed_cursor_pages_string_id() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mstr");
    MongoTest::connect(PORT, &db).seed_string_id("bench", 4000);

    // Keyset a STRING _id — the typed cursor token round-trips it; a display-only
    // cursor would mis-order (was an actionable error before A').
    let rig = batch(&db, "bench").mongo("page_size: 1500");
    rig.run_ok();

    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        4000,
        "string-_id keyset must read the whole collection (no loss)"
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        4000,
        "no duplicate rows across string-_id page boundaries (total == distinct)"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_parallel_objectid_and_string_id() {
    require_alive(LiveService::Mongo);
    // ObjectId (the Mongo default) and String `_id` — the `$sample` `_id`-range
    // fan-out must tile either ordered type completely, not just integers.
    for (seed_objectid, tag) in [(true, "mpoid"), (false, "mpstr")] {
        let db = unique_name(tag);
        let m = MongoTest::connect(PORT, &db);
        if seed_objectid {
            m.seed_objectid("bench", 5000);
        } else {
            m.seed_string_id("bench", 5000);
        }
        let rig = batch(&db, "bench")
            .mongo("page_size: 1000")
            .export_line("parallel: 4");
        rig.run_ok();
        let kind = if seed_objectid { "ObjectId" } else { "String" };
        assert_eq!(
            dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
            5000,
            "parallel over {kind} _id must read the whole collection"
        );
        assert_eq!(
            total_parquet_rows(&rig.out_dir()),
            5000,
            "parallel over {kind} _id must not duplicate at a range boundary"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_type_fidelity_document_is_verbatim_extjson() {
    require_alive(LiveService::Mongo);
    use mongodb::bson::doc;
    let db = unique_name("mtypes");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");
    // A large Int64 (> 2^53) is the portability trap: a downstream f64 JSON
    // parser would round it. rivet stores the document TEXT verbatim, so the
    // exact digits must be present for a bigint-aware PARSE_JSON to recover.
    m.insert_many(
        "t",
        vec![doc! {
            "_id": 1_i64,
            "i64_big": 9_007_199_254_740_993_i64, // 2^53 + 1
            "dec": mongodb::bson::Bson::Decimal128("123456789.987654321012345".parse().unwrap()),
            // Nested document — the `type_json` cell's claim (content fidelity
            // of structured JSON through Parquet) needs its own evidence, not a
            // ride on the scalar asserts.
            "nested": doc! { "k": "v-\u{00e9}\u{4e2d}", "arr": [1_i32, 2_i32, 3_i32] },
        }],
    );

    let rig = batch(&db, "t");
    rig.run_ok();

    let docs = dir_parquet_distinct_strings(&rig.out_dir(), "document");
    let doc_text = docs.iter().next().expect("one document");
    assert!(
        doc_text.contains("9007199254740993"),
        "large Int64 must be verbatim in the document text, got: {doc_text}"
    );
    assert!(
        doc_text.contains("123456789.987654321012345"),
        "Decimal128 must be verbatim (type-tagged), got: {doc_text}"
    );
    // type_json: nested structure + non-ASCII content survive verbatim.
    assert!(
        doc_text.contains("v-\u{00e9}\u{4e2d}") && doc_text.contains("arr"),
        "nested JSON (unicode value + array) must be verbatim, got: {doc_text}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_run_reconcile_matches_source_count() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mrecon");
    MongoTest::connect(PORT, &db).seed_int_id("bench", 3000);

    let rig = batch(&db, "bench").mongo("page_size: 1000");
    // `rivet run --reconcile` is Mongo's whole-export count check — the source
    // `count_documents` vs the destination row count. (The partition-level
    // `rivet reconcile` / `rivet repair` commands are N/A for keyset: it has no
    // natural partitions.) The rig builds the config; the flag rides a bespoke run.
    let r = run_rivet(&[
        "run",
        "-c",
        rig.config_path().to_str().unwrap(),
        "--reconcile",
    ]);
    assert!(
        r.status.success(),
        "run --reconcile must exit 0 on a match; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );
    // The reconcile verdict rides the run summary on stderr. NB: a bare
    // `.contains("MATCH")` is vacuous — "MISMATCH" contains "MATCH" — so a real
    // mismatch would pass it too. Assert the positive verdict AND the absence of
    // "MISMATCH" (bug-hunt: the oracle was self-satisfying).
    let summary = String::from_utf8_lossy(&r.stderr);
    assert!(
        summary.contains("MATCH") && !summary.contains("MISMATCH"),
        "reconcile must report a source/dest MATCH (not MISMATCH), got:\n{summary}"
    );
    // Independent oracle: the verdict is rivet's own counter chain — also
    // physically re-read the destination (matrix audit: self-oracle class).
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        3000,
        "destination parquet must physically hold every source document, independent of the reconcile verdict"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_resume_reads_only_new_since_last_run() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mresume");
    let m = MongoTest::connect(PORT, &db);
    m.seed_int_id("t", 2000);

    // The SAME rig across both runs — the keyset checkpoint persists in the
    // export state keyed by its (stable) config + destination.
    let rig = batch(&db, "t").mongo("page_size: 500, resume: true");
    rig.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        2000
    );

    // 500 new docs arrive with higher _id.
    for i in 2001..=2500 {
        m.upsert_set("t", i, "v", "new");
    }

    // Run 2 (resume) must read ONLY the 500 new — not rescan the first 2000.
    rig.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        2500,
        "resume must union to the full set with no loss"
    );
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        2500,
        "resume must add only the new rows — 4500 would mean run 2 rescanned"
    );
    // Dest manifest copies (reconcile's artifact), not just the parquet re-read:
    // must span both runs (2000 + 500) — a resumed run clobbering a prior
    // manifest is silent to the re-read but breaks reconcile.
    assert_eq!(
        dir_manifest_copy_total_rows(&rig.out_dir()),
        2500,
        "run-unique manifest copies must sum run 1 (2000) + run 2 (500); a clobbered manifest is silent to the parquet re-read"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo-rs (snapshot needs a replica set)"]
fn mongo_batch_read_concern_snapshot_empty_first_run_then_populated() {
    // `read_concern: snapshot` is a point-in-time read (5.0+ replica set), so it
    // rides the RS, not the standalone. Cover the key AND an empty→second-run
    // interaction: an EMPTY first run under snapshot must succeed reading 0 (a
    // 0-doc collection must not wedge), and a second run after the collection is
    // populated must read every doc under snapshot.
    require_alive(LiveService::MongoRs);
    const RS_PORT: u16 = 27018;
    let db = unique_name("mrc_snap");
    let m = MongoTest::connect(RS_PORT, &db);
    // `read_concern: snapshot` for a find requires MongoDB 5.0+; on 4.4 it is
    // unsupported, so self-skip rather than fail the version matrix.
    if m.server_major() < 5 {
        eprintln!("skipping: read_concern: snapshot needs MongoDB 5.0+ (server is 4.x)");
        return;
    }
    m.drop_collection("t");

    let rig = Rig::mongo_batch("t")
        .source_url(&MongoTest::url(RS_PORT, &db))
        .mongo("read_concern: snapshot");

    // Empty first run: snapshot read of a 0-doc collection reads nothing, cleanly.
    rig.run_ok();
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        0,
        "empty snapshot run must read 0, not hang or phantom-read"
    );

    // Populate, then a second run: snapshot must read the full collection.
    m.seed_int_id("t", 1500);
    rig.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        1500,
        "snapshot read_concern must read the whole collection on the second run"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_no_cursor_timeout_false_empty_first_run_then_populated() {
    // `no_cursor_timeout: false` is the NON-default (default `true`): it flips the
    // scan cursor's `noCursorTimeout` OFF. Cover the key AND an empty→second-run
    // interaction: an empty first run reads 0, and a second run over a populated,
    // page_size-paged collection reads everything — the flag must not truncate the
    // scan.
    require_alive(LiveService::Mongo);
    let db = unique_name("mnct");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");

    // page_size forces a paged (multi-cursor) scan — where the cursor flag applies.
    let rig = batch(&db, "t").mongo("page_size: 400, no_cursor_timeout: false");

    rig.run_ok();
    assert_eq!(
        total_parquet_rows(&rig.out_dir()),
        0,
        "empty first run reads 0 with no_cursor_timeout: false"
    );

    m.seed_int_id("t", 1200);
    rig.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        1200,
        "no_cursor_timeout: false must not truncate the paged scan on the populated run"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_keyset_on_heterogeneous_id_errors_loudly_full_scan_still_works() {
    require_alive(LiveService::Mongo);
    use mongodb::bson::doc;
    let db = unique_name("mhetero");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");
    // int + string `_id` in one collection: `$gt` is type-bracketed, so a keyset
    // seek that reaches the last int silently returns nothing for the strings.
    let mut docs = Vec::new();
    for i in 1..=1000_i64 {
        docs.push(doc! { "_id": i, "v": "n" });
    }
    for i in 1..=1000_i64 {
        docs.push(doc! { "_id": format!("s-{i:04}"), "v": "s" });
    }
    m.insert_many("t", docs);

    // Keyset MUST refuse loudly — never silently export one bracket.
    let stderr = batch(&db, "t").mongo("page_size: 400").run_expect_fail();
    assert!(
        stderr.contains("heterogeneous"),
        "keyset on heterogeneous _id must fail naming the cause; stderr:\n{stderr}"
    );

    // Full scan (no page_size) — a single cursor crosses BSON brackets — is whole.
    let fs = batch(&db, "t");
    fs.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&fs.out_dir(), "_id").len(),
        2000,
        "full scan must read every _id across both types"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn roast_resume_must_not_bypass_heterogeneous_id_guard() {
    // The untested CROSS-PRODUCT of two green tests (resume × hetero-guard),
    // found by the multi-agent hunt: the guard is gated on `after_id.is_none()`,
    // and with `resume: true` every run after the first supplies a persisted
    // cursor — so the guard never runs again. A string `_id` inserted after
    // run 1 is then PERMANENTLY unexportable: the resumed filter
    // `{_id: {$gt: <int cursor>}}` is type-bracketed and matches none of them,
    // and every future run reports success with 0 rows — a forever-silent gap.
    require_alive(LiveService::Mongo);
    use mongodb::bson::doc;
    let db = unique_name("mresumeht");
    let m = MongoTest::connect(PORT, &db);
    m.seed_int_id("t", 100); // uniform int _id — run 1 passes the guard

    // SAME rig across runs: the keyset checkpoint persists in export_state.
    let rig = batch(&db, "t").mongo("page_size: 40, resume: true");
    rig.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
        100
    );

    // The collection turns heterogeneous AFTER the cursor was pinned.
    m.insert_many("t", vec![doc! { "_id": "zzz", "v": "s" }]);

    // The resumed run must fail LOUDLY (same contract as the first-run guard) —
    // never exit 0 having silently skipped the new bracket forever.
    let stderr = rig.run_expect_fail();
    assert!(
        stderr.contains("heterogeneous"),
        "a resumed keyset run over a now-heterogeneous _id collection must fail \
         naming the cause, not succeed with a silent permanent gap; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn roast_null_and_object_id_must_not_slip_past_the_bracket_guard() {
    // id_bracket's catch-all arm lumped Null, Object, Array, Regex … into ONE
    // bracket, so a collection whose min `_id` is null and max is an object
    // compared equal and slipped past ensure_uniform_id_type — while `$gt`
    // still cannot cross those BSON bands: the same silent loss the guard
    // exists to stop, in a new disguise (multi-agent hunt, confirmed live).
    require_alive(LiveService::Mongo);
    use mongodb::bson::{Bson, doc};
    let db = unique_name("mnullobj");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");
    m.insert_many(
        "t",
        vec![
            doc! { "_id": Bson::Null, "v": "null-id" },
            doc! { "_id": { "a": 1 }, "v": "obj1" },
            doc! { "_id": { "a": 2 }, "v": "obj2" },
            doc! { "_id": { "a": 3 }, "v": "obj3" },
        ],
    );

    // Keyset must refuse loudly — min/max sit in DIFFERENT BSON bands even
    // though both landed in the old catch-all bracket.
    let stderr = batch(&db, "t").mongo("page_size: 1").run_expect_fail();
    assert!(
        stderr.contains("heterogeneous"),
        "null+object _id must be refused as heterogeneous, not silently paged; \
         stderr:\n{stderr}"
    );

    // Full scan stays the remediation: all 4 docs, both bands.
    let fs = batch(&db, "t");
    fs.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&fs.out_dir(), "_id").len(),
        4,
        "full scan must read every doc across the null and object bands"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn roast_nan_id_refused_for_keyset_and_parallel_full_scan_complete() {
    // MongoDB comparison operators NEVER match NaN against a non-NaN operand,
    // and NaN sorts first within the numeric band. Found by the multi-agent
    // hunt, two silent-loss prongs (both bracket-0 numeric, so the heterogeneous
    // guard passes today):
    //  (a) keyset: a page boundary landing ON the NaN row makes the next filter
    //      `{_id: {$gt: NaN}}` match NOTHING → the whole remaining collection is
    //      silently dropped ("range exhausted");
    //  (b) parallel: every worker range `{$gte: lo, $lt: hi}` fails both bounds
    //      for NaN → the union of ranges is NOT the whole collection, the NaN
    //      doc silently vanishes while the run reports success.
    require_alive(LiveService::Mongo);
    use mongodb::bson::doc;
    let db = unique_name("mnan");
    let m = MongoTest::connect(PORT, &db);
    m.drop_collection("t");
    m.insert_many(
        "t",
        vec![
            doc! { "_id": f64::NAN, "v": "nan" },
            doc! { "_id": 1_i64, "v": "a" },
            doc! { "_id": 2_i64, "v": "b" },
            doc! { "_id": 3_i64, "v": "c" },
        ],
    );

    // (a) keyset with the page boundary right after the NaN row (NaN sorts
    // first, page_size 1 ⇒ cursor == NaN) must refuse loudly, not truncate.
    let stderr = batch(&db, "t").mongo("page_size: 1").run_expect_fail();
    assert!(
        stderr.contains("NaN"),
        "keyset over a NaN _id must fail naming NaN, not silently lose the tail; \
         stderr:\n{stderr}"
    );

    // (b) parallel range fan-out must refuse loudly too, not drop the NaN doc.
    let stderr = batch(&db, "t")
        .mongo("page_size: 2")
        .export_line("parallel: 2")
        .run_expect_fail();
    assert!(
        stderr.contains("NaN"),
        "parallel ranges over a NaN _id must fail naming NaN, not drop the doc; \
         stderr:\n{stderr}"
    );

    // Remediation path: the full scan (single cursor, no seek) reads all 4.
    let fs = batch(&db, "t");
    fs.run_ok();
    assert_eq!(
        dir_parquet_distinct_strings(&fs.out_dir(), "_id").len(),
        4,
        "full scan must read every doc including the NaN _id"
    );
}
