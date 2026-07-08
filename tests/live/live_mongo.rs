//! Live coverage for the MongoDB BATCH read path (JSON-blob model). Requires the
//! standalone mongo (`docker compose up -d mongo`). Every check re-reads the
//! destination Parquet (distinct `_id` set / verbatim document text), never
//! rivet's own counters — the test-self-oracle rule.
//!
//! Invariants proven:
//! - **blob round-trip** — every document exports as `_id` + `document`, the
//!   distinct `_id` set equals the source.
//! - **keyset no loss/dup** — `page_size` paging over the whole collection loses
//!   and duplicates nothing across page boundaries.
//! - **parallel any-`_id`** — `parallel: N` `_id`-range fan-out reads the whole
//!   collection with no gap/overlap, for integer `_id` (not just ObjectId).
//! - **typed cursor** — keyset pages a STRING `_id` collection (impossible before
//!   the typed-cursor token; the display-column round-trip would mis-order it).
//! - **type fidelity** — a document of tricky BSON types exports with the exact
//!   extended-JSON TEXT (large `Int64 > 2^53`, `Decimal128`) verbatim, so a
//!   downstream `PARSE_JSON` (BigQuery / Snowflake) reconstructs it losslessly.

use crate::common::*;

const PORT: u16 = 27017; // standalone mongo

fn write_cfg(
    dir: &std::path::Path,
    db: &str,
    table: &str,
    out: &std::path::Path,
    extra_mongo: &str,
    extra_export: &str,
) -> std::path::PathBuf {
    let url = MongoTest::url(PORT, db);
    let cfg = format!(
        "source: {{ type: mongo, url: \"{url}\"{extra_mongo} }}\n\
         exports:\n  - {{ name: {table}, table: {table}, mode: full, format: parquet, \
         destination: {{ type: local, path: \"{}\" }}{extra_export} }}\n",
        out.display(),
    );
    let p = dir.join("cfg.yaml");
    std::fs::write(&p, cfg).unwrap();
    p
}

fn run_export(cfg: &std::path::Path) -> std::process::Output {
    run_rivet(&["run", "-c", cfg.to_str().unwrap()])
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_keyset_no_loss_or_dup() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mkeyset");
    let m = MongoTest::connect(PORT, &db);
    m.seed_int_id("bench", 5000);

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    // page_size 2000 → 3 pages: exercises page boundaries.
    let cfg = write_cfg(
        cfg_dir.path(),
        &db,
        "bench",
        out.path(),
        ", mongo: { page_size: 2000 }",
        "",
    );
    assert!(run_export(&cfg).status.success());

    let ids = dir_parquet_distinct_strings(out.path(), "_id");
    assert_eq!(
        ids.len(),
        5000,
        "distinct _id must equal source (no loss/dup)"
    );
    assert!(
        dir_parquet_has_column(out.path(), "document"),
        "blob document column present"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_parallel_integer_id_no_loss_or_dup() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mpar");
    let m = MongoTest::connect(PORT, &db);
    m.seed_int_id("bench", 5000);

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    // parallel: 4 on an INTEGER _id — impossible before Bson range bounds.
    let cfg = write_cfg(
        cfg_dir.path(),
        &db,
        "bench",
        out.path(),
        ", mongo: { page_size: 1000 }",
        ", parallel: 4",
    );
    assert!(run_export(&cfg).status.success());

    let ids = dir_parquet_distinct_strings(out.path(), "_id");
    assert_eq!(
        ids.len(),
        5000,
        "parallel _id-range union must be the whole collection"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_batch_typed_cursor_pages_string_id() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mstr");
    let m = MongoTest::connect(PORT, &db);
    m.seed_string_id("bench", 4000);

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    // Keyset a STRING _id — the typed cursor token round-trips it; a display-only
    // cursor would mis-order (was an actionable error before A').
    let cfg = write_cfg(
        cfg_dir.path(),
        &db,
        "bench",
        out.path(),
        ", mongo: { page_size: 1500 }",
        "",
    );
    assert!(run_export(&cfg).status.success());

    assert_eq!(
        dir_parquet_distinct_strings(out.path(), "_id").len(),
        4000,
        "string-_id keyset must read the whole collection with no loss/dup"
    );
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

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let cfg = write_cfg(cfg_dir.path(), &db, "t", out.path(), "", "");
    assert!(run_export(&cfg).status.success());

    let docs = dir_parquet_distinct_strings(out.path(), "document");
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
    let m = MongoTest::connect(PORT, &db);
    m.seed_int_id("bench", 3000);

    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let cfg = write_cfg(
        cfg_dir.path(),
        &db,
        "bench",
        out.path(),
        ", mongo: { page_size: 1000 }",
        "",
    );
    // `rivet run --reconcile` is Mongo's whole-export count check — the source
    // `count_documents` vs the destination row count. (The partition-level
    // `rivet reconcile` / `rivet repair` commands are N/A for keyset: it has no
    // natural partitions, and the CLI says so rather than guessing.)
    let r = run_rivet(&["run", "-c", cfg.to_str().unwrap(), "--reconcile"]);
    assert!(
        r.status.success(),
        "run --reconcile must exit 0 on a match; stderr:\n{}",
        String::from_utf8_lossy(&r.stderr)
    );
    // The reconcile verdict rides the run summary on stderr (like the per-export
    // progress line), not stdout.
    let summary = String::from_utf8_lossy(&r.stderr);
    assert!(
        summary.contains("MATCH"),
        "reconcile must report a source/dest MATCH, got:\n{summary}"
    );
}
