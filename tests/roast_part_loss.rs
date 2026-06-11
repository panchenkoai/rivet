//! ROAST-RED live-part-loss: chunked/keyset runners drop rotated file parts.
//!
//! `ExportSink::maybe_split` (src/pipeline/sink/mod.rs) rotates the temp file
//! into `sink.completed_parts` whenever `max_file_size` is reached — but
//! `completed_parts` is drained only by the single-export path
//! (src/pipeline/single.rs).  The chunked runners (src/pipeline/chunked/exec.rs)
//! and the keyset runner (src/pipeline/keyset.rs) upload only `sink.tmp.path()`
//! — the LAST partial part — while recording `rows = sink.total_rows` in the
//! manifest.  Every rotated part is silently deleted when the sink drops,
//! so the destination loses data while the manifest looks healthy.
//!
//! These tests assert the CORRECT behavior (total rows across ALL part files
//! at the destination == seeded row count) and are expected to FAIL until the
//! chunked/keyset paths drain `completed_parts` like single.rs does.
//!
//! Run: `docker compose up -d postgres mysql && cargo test --test roast_part_loss -- --ignored`

mod common;
use common::*;

use mysql::prelude::Queryable;

/// Count data rows (lines minus the header) across every `.csv` part file in
/// `dir`.  CSV is used instead of parquet so `bytes_written` grows
/// deterministically with every batch (no row-group flush dependency) and a
/// row is exactly one line (payloads contain no newlines/quotes).
fn csv_data_rows(dir: &std::path::Path) -> (usize, Vec<(std::path::PathBuf, usize)>) {
    let mut per_file = Vec::new();
    let mut total = 0usize;
    for path in files_with_extension(dir, "csv") {
        let content = std::fs::read_to_string(&path).expect("read csv part file");
        // A header-only file (the bug's signature) has 1 line → 0 data rows.
        let rows = content.lines().count().saturating_sub(1);
        total += rows;
        per_file.push((path, rows));
    }
    (total, per_file)
}

// ─── chunked (sequential, exec.rs) — rotated parts must reach the destination ─

// ROAST-RED live-part-loss: chunked runner uploads only sink.tmp (last partial
// part); parts rotated into sink.completed_parts by maybe_split are deleted.
// Asserts CORRECT behavior; expected to FAIL until the fix lands.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn roast_chunked_split_parts_all_rows_reach_destination() {
    require_alive(LiveService::Postgres);

    // 2000 rows × ~1KB payload ≈ 2MB of CSV.  chunk_size 1000 → 2 range
    // chunks; batch_size 250 → each batch writes ~250KB, far above the 64KB
    // cap, so maybe_split rotates after every batch (~4 rotations per chunk).
    // The current code then uploads only the final (header-only) temp file of
    // each chunk while reporting all 1000 rows in the manifest.
    const ROWS: i64 = 2_000;
    let table = seed_pg_wide_table(ROWS, 1000);

    let export = unique_name("roast_chunk_split");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, payload FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: 1000
    format: csv
    compression: none
    max_file_size: 64KB
    destination: {{type: local, path: {dir}}}
    tuning: {{batch_size: 250}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = run_rivet_export(&cfg, &export);
    assert!(
        run.status.success(),
        "chunked export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let (total, per_file) = csv_data_rows(out.path());
    assert!(
        !per_file.is_empty(),
        "chunked export must produce at least one csv part file at the destination"
    );
    assert_eq!(
        total, ROWS as usize,
        "DATA LOSS: only {total} of {ROWS} seeded rows survive across the destination part \
         files {per_file:?} — maybe_split rotated parts into sink.completed_parts, but the \
         chunked runner uploaded only sink.tmp (the last partial part) and the rotated parts \
         were deleted with the sink"
    );
}

// ─── keyset (keyset.rs) — rotated parts must reach the destination ───────────

/// Drop the MySQL test table on exit even if an assertion fails (the shared
/// `MysqlTable` guard cannot be constructed outside `common::mysql`).
struct DropMysqlTable(String);
impl Drop for DropMysqlTable {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

// ROAST-RED live-part-loss: keyset runner uploads only sink.tmp per page;
// parts rotated into sink.completed_parts by maybe_split are deleted.
// Asserts CORRECT behavior; expected to FAIL until the fix lands.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn roast_keyset_split_parts_all_rows_reach_destination() {
    require_alive(LiveService::Mysql);

    // VARCHAR PK → chunked mode auto-selects keyset (no integer chunk column).
    // 1000 rows × ~1KB payload; chunk_size 500 → 2 keyset pages; batch_size
    // 250 → ~250KB per batch >> 64KB cap → maybe_split rotates after every
    // batch, leaving each page's final temp file header-only.
    const N: usize = 1_000;
    let table = unique_name("roast_keyset_split");
    let _guard = DropMysqlTable(table.clone());

    let mut conn = mysql_connect();
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {table} (uid VARCHAR(40) NOT NULL PRIMARY KEY, payload TEXT NOT NULL)"
    ))
    .unwrap();
    conn.query_drop("SET SESSION cte_max_recursion_depth = 20000")
        .unwrap();
    conn.query_drop(format!(
        "INSERT INTO {table} (uid, payload) \
         WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < {N}) \
         SELECT CONCAT('id-', LPAD(n, 6, '0')), REPEAT('x', 1000) FROM seq"
    ))
    .unwrap();

    let export = unique_name("roast_keyset_exp");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: mysql, url: "{MYSQL_URL}"}}
exports:
  - name: {export}
    table: {table}
    mode: chunked
    chunk_size: 500
    format: csv
    compression: none
    max_file_size: 64KB
    destination: {{type: local, path: {dir}}}
    tuning: {{batch_size: 250}}
"#,
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = run_rivet_export(&cfg, &export);
    assert!(
        run.status.success(),
        "keyset export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let (total, per_file) = csv_data_rows(out.path());
    assert!(
        !per_file.is_empty(),
        "keyset export must produce at least one csv page file at the destination"
    );
    assert_eq!(
        total, N,
        "DATA LOSS: only {total} of {N} seeded rows survive across the destination page \
         files {per_file:?} — maybe_split rotated parts into sink.completed_parts, but the \
         keyset runner uploaded only sink.tmp (the last partial part) and the rotated parts \
         were deleted with the sink"
    );
}
