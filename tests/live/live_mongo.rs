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
        "distinct _id must equal source (no loss/dup)"
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
        "string-_id keyset must read the whole collection with no loss/dup"
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
        assert_eq!(
            dir_parquet_distinct_strings(&rig.out_dir(), "_id").len(),
            5000,
            "parallel over {} _id must read the whole collection",
            if seed_objectid { "ObjectId" } else { "String" }
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
    // The reconcile verdict rides the run summary on stderr.
    let summary = String::from_utf8_lossy(&r.stderr);
    assert!(
        summary.contains("MATCH"),
        "reconcile must report a source/dest MATCH, got:\n{summary}"
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
