//! The `quality:` gate on EVERY runner (ADR-0028, amendment 2026-10-02).
//!
//! Each SQL case exports a 20-row table whose `v` is unique except that row 8
//! repeats row 13's value. The two copies sit in different MIDDLE chunks / pages
//! / ranges, so the run fails only if every sink's measurements reach the ledger
//! and the seam grades the merge: exit 3 with the shared failure text, exactly as
//! on `single`. Parallel-Mongo exports one `document` column, which is never
//! null or duplicated, so its case trips `row_count_min` instead.

use crate::common::*;

const DUPLICATE: &str = "column 'v': 1 duplicate values out of 20 rows";

/// Export a table with one duplicated `v` under `unique_columns: [v]`; return the run.
fn pg_duplicate_run(tag: &str, text_key: bool, mode: &str, lines: &[&str]) -> std::process::Output {
    require_alive(LiveService::Postgres);
    let table = unique_name(tag);
    let (key, key_val) = if text_key {
        ("k TEXT PRIMARY KEY", "'k' || lpad(g::text, 4, '0')")
    } else {
        ("k BIGINT PRIMARY KEY", "g")
    };
    // Row 8 (chunk 2 of 4, page 3 of 7) repeats row 13 (chunk 3, page 5).
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} ({key}, v INT NOT NULL);
         INSERT INTO {table} SELECT {key_val}, CASE WHEN g = 8 THEN 13 ELSE g END
         FROM generate_series(1, 20) g;"
    ))
    .unwrap();
    let _guard = PgTable::adopt(table.clone());

    let export = unique_name(&format!("{tag}_exp"));
    let out = tempfile::tempdir().unwrap();
    let mut rig = Rig::pg_batch(&table)
        .mode(mode)
        .export_named(&export)
        .export_line("quality:")
        .export_line("  unique_columns: [v]")
        .export_line("  unique_max_entries: 1000")
        .dest_path(out.path().to_path_buf());
    for l in lines {
        rig = rig.export_line(l);
    }
    rig.run_args(&["--export", &export])
}

fn assert_failed_as_single_does(runner: &str, code: Option<i32>, stderr: &[u8], detail: &str) {
    let stderr = String::from_utf8_lossy(stderr);
    assert_eq!(
        code,
        Some(3),
        "{runner}: a quality violation must exit 3 (data integrity); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("quality check(s) failed") && stderr.contains(detail),
        "{runner}: the failure must carry the shared contract and `{detail}`; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_on_the_single_runner() {
    let r = pg_duplicate_run("qg_single", false, "full", &[]);
    assert_failed_as_single_does("single", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_keyset_pages() {
    let lines = ["chunk_by_key: k", "chunk_size: 3"];
    let r = pg_duplicate_run("qg_keyset", true, "chunked", &lines);
    assert_failed_as_single_does("keyset", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_parallel_keyset_ranges() {
    let lines = ["chunk_by_key: k", "chunk_size: 3", "parallel: 4"];
    let r = pg_duplicate_run("qg_keyset_par", true, "chunked", &lines);
    assert_failed_as_single_does("keyset-parallel", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_chunks() {
    let lines = ["chunk_column: k", "chunk_size: 5"];
    let r = pg_duplicate_run("qg_chunked", false, "chunked", &lines);
    assert_failed_as_single_does("chunked", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_parallel_chunks() {
    let lines = ["chunk_column: k", "chunk_size: 5", "parallel: 2"];
    let r = pg_duplicate_run("qg_chunked_par", false, "chunked", &lines);
    assert_failed_as_single_does("chunked-parallel", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_checkpoint_chunks() {
    let lines = ["chunk_column: k", "chunk_size: 5", "chunk_checkpoint: true"];
    let r = pg_duplicate_run("qg_ckpt", false, "chunked", &lines);
    assert_failed_as_single_does("chunked-checkpoint", r.status.code(), &r.stderr, DUPLICATE);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn quality_gate_fails_a_duplicate_split_across_parallel_checkpoint_chunks() {
    let lines = [
        "chunk_column: k",
        "chunk_size: 5",
        "chunk_checkpoint: true",
        "parallel: 2",
    ];
    let r = pg_duplicate_run("qg_ckpt_par", false, "chunked", &lines);
    assert_failed_as_single_does(
        "chunked-checkpoint-parallel",
        r.status.code(),
        &r.stderr,
        DUPLICATE,
    );
}

/// 24 rows over `parallel: 4` keyset ranges of 6 rows, paged by 3: every range ends on an EMPTY page.
/// Row 8 (range 2) repeats row 13's `v` (range 3), so the run must fail as `single` does.
fn parallel_keyset_duplicate_run(engine: SqlEngine, tag: &str) -> std::process::Output {
    engine.alive();
    let (k, v) = (engine.col("k"), engine.col("v"));
    let i = engine.int64();
    let (table, _guard) = engine.create(tag, &format!("{k} {i} PRIMARY KEY, {v} {i} NOT NULL"));
    let rows: Vec<String> = (1..=24)
        .map(|g| format!("({g}, {})", if g == 8 { 13 } else { g }))
        .collect();
    engine.exec(&format!("INSERT INTO {table} VALUES {}", rows.join(", ")));
    let export = unique_name(&format!("{tag}_exp"));
    engine
        .rig(&table)
        .mode("chunked")
        .export_named(&export)
        .export_line("chunk_by_key: k")
        .export_line("chunk_size: 3")
        .export_line("parallel: 4")
        .export_line("quality:")
        .export_line("  unique_columns: [v]")
        .export_line("  unique_max_entries: 1000")
        .run_args(&["--export", &export])
}

const DUPLICATE_24: &str = "column 'v': 1 duplicate values out of 24 rows";

#[test]
#[ignore = "live: requires docker compose mysql"]
fn quality_gate_fails_a_duplicate_across_parallel_keyset_ranges_ending_empty_mysql() {
    let r = parallel_keyset_duplicate_run(SqlEngine::Mysql, "qg_kpar_my");
    assert_failed_as_single_does("keyset-parallel", r.status.code(), &r.stderr, DUPLICATE_24);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn quality_gate_fails_a_duplicate_across_parallel_keyset_ranges_ending_empty_mssql() {
    let r = parallel_keyset_duplicate_run(SqlEngine::Mssql, "qg_kpar_ms");
    assert_failed_as_single_does("keyset-parallel", r.status.code(), &r.stderr, DUPLICATE_24);
}

#[test]
#[ignore = "live: requires docker compose oracle"]
fn quality_gate_fails_a_duplicate_across_parallel_keyset_ranges_ending_empty_oracle() {
    let r = parallel_keyset_duplicate_run(SqlEngine::Oracle, "qg_kpar_ora");
    assert_failed_as_single_does("keyset-parallel", r.status.code(), &r.stderr, DUPLICATE_24);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn quality_gate_fails_a_short_mongo_parallel_export() {
    require_alive(LiveService::Mongo);
    let db = unique_name("mquality");
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id("bench", 4000);
    let rig = Rig::mongo_batch("bench")
        .source_url(&MongoTest::url(27017, &db))
        .mongo("page_size: 1000")
        .export_line("parallel: 4")
        .export_line("quality:")
        .export_line("  row_count_min: 5000");
    let r = rig.run_args(&[]);
    assert_failed_as_single_does(
        "mongo-parallel",
        r.status.code(),
        &r.stderr,
        "row_count 4000 below minimum 5000",
    );
    m.drop_database();
}
