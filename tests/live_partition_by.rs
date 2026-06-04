//! Live E2E for value-based output partitioning (`partition_by`).
//!
//! Guards the integration path the unit tests can't reach: the real source
//! round-trip (min/max + NULL probe), the per-bucket query expansion, the
//! Hive `col=value/` layout, the NULL bucket, half-open `[lo, hi)` boundary
//! semantics, row-count integrity (sum of partitions == source), and the
//! per-partition manifest + `_SUCCESS`.
//!
//! Live: requires `docker compose up -d` (Postgres). Each test is `#[ignore]`.

mod common;

use std::path::{Path, PathBuf};

use common::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use postgres::NoTls;

struct PgCleanup(String);

impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

/// Total Parquet rows under `dir`, recursively (partition output nests one
/// level deep in `col=value/`).
fn parquet_rows_recursive(dir: &Path) -> usize {
    parquet_files_recursive(dir)
        .iter()
        .map(|f| parquet_row_count(f))
        .sum()
}

fn parquet_files_recursive(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in rd.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            out.extend(parquet_files_recursive(&path));
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    out
}

fn parquet_row_count(path: &Path) -> usize {
    let bytes = std::fs::read(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    builder
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

/// Rows in a single partition directory (`<root>/<col=value>`).
fn partition_rows(root: &Path, segment: &str) -> usize {
    parquet_rows_recursive(&root.join(segment))
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_by_day_splits_into_hive_dirs_with_null_bucket() {
    require_alive(LiveService::Postgres);

    let table = unique_name("part_day");
    let mut c = pg_connect();
    // Deterministic fixture: 3 days + a row exactly on the day-2 midnight
    // boundary + NULLs. The midnight row pins the half-open `< hi` rule (it
    // must land in day 2, not day 1).
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, created_at TIMESTAMP);
         INSERT INTO {table} (id, created_at) VALUES
           (1, '2023-01-01 09:00:00'),
           (2, '2023-01-01 18:30:00'),
           (3, '2023-01-02 00:00:00'),   -- exact boundary → belongs to day 2
           (4, '2023-01-02 12:00:00'),
           (5, '2023-01-03 06:00:00'),
           (6, NULL),                    -- NULL bucket
           (7, NULL);",
    ))
    .unwrap();
    let _guard = PgCleanup(table.clone());

    let export = unique_name("ci_part");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export}
    query: "SELECT id, created_at FROM {table}"
    partition_by: created_at
    partition_granularity: day
    format: parquet
    destination:
      type: local
      path: {out}/{{partition}}
"#,
        out = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let result = run_rivet_export(&cfg_path, &export);
    assert!(
        result.status.success(),
        "partitioned run failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    let root = out_dir.path();

    // Hive-style per-day directories + the Hive NULL bucket exist.
    for seg in [
        "created_at=2023-01-01",
        "created_at=2023-01-02",
        "created_at=2023-01-03",
        "created_at=__HIVE_DEFAULT_PARTITION__",
    ] {
        assert!(
            root.join(seg).is_dir(),
            "missing partition directory: {seg}"
        );
    }

    // Per-partition row counts, including the half-open boundary row (id 3
    // → day 2) and the two NULLs.
    assert_eq!(partition_rows(root, "created_at=2023-01-01"), 2, "day 1");
    assert_eq!(
        partition_rows(root, "created_at=2023-01-02"),
        2,
        "day 2 (incl. the 00:00:00 boundary row)"
    );
    assert_eq!(partition_rows(root, "created_at=2023-01-03"), 1, "day 3");
    assert_eq!(
        partition_rows(root, "created_at=__HIVE_DEFAULT_PARTITION__"),
        2,
        "NULL bucket"
    );

    // Integrity: every source row lands in exactly one partition — no loss,
    // no duplication.
    assert_eq!(parquet_rows_recursive(root), 7, "sum of partitions == source");

    // Each partition is an independent, complete prefix.
    for seg in [
        "created_at=2023-01-01",
        "created_at=__HIVE_DEFAULT_PARTITION__",
    ] {
        let p = root.join(seg);
        assert!(p.join("manifest.json").is_file(), "{seg}: manifest.json");
        assert!(p.join("_SUCCESS").is_file(), "{seg}: _SUCCESS");
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_by_month_granularity_buckets_by_month() {
    require_alive(LiveService::Postgres);

    let table = unique_name("part_mon");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, d DATE);
         INSERT INTO {table} (id, d) VALUES
           (1, '2023-01-05'), (2, '2023-01-31'),
           (3, '2023-02-01'), (4, '2023-02-28'),
           (5, '2023-03-15');",
    ))
    .unwrap();
    let _guard = PgCleanup(table.clone());

    let export = unique_name("ci_mon");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export}
    query: "SELECT id, d FROM {table}"
    partition_by: d
    partition_granularity: month
    format: parquet
    destination:
      type: local
      path: {out}/{{partition}}
"#,
        out = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let result = run_rivet_export(&cfg_path, &export);
    assert!(
        result.status.success(),
        "month-partitioned run failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    let root = out_dir.path();
    assert_eq!(partition_rows(root, "d=2023-01"), 2, "Jan");
    assert_eq!(partition_rows(root, "d=2023-02"), 2, "Feb");
    assert_eq!(partition_rows(root, "d=2023-03"), 1, "Mar");
    assert_eq!(parquet_rows_recursive(root), 5, "sum == source");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_by_with_chunked_mode_preserves_row_counts() {
    // `partition_by` is orthogonal to `mode`: chunking runs inside each
    // partition. Chunk fan-out depends on the key range (see docs), but row
    // counts must stay exact — this guards that invariant.
    require_alive(LiveService::Postgres);

    let table = unique_name("part_chunk");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, created_at TIMESTAMP);
         INSERT INTO {table} (id, created_at) VALUES
           (1, '2023-01-01 01:00:00'), (2, '2023-01-01 02:00:00'), (3, '2023-01-01 03:00:00'),
           (4, '2023-01-02 01:00:00'), (5, '2023-01-02 02:00:00'), (6, '2023-01-02 03:00:00');",
    ))
    .unwrap();
    let _guard = PgCleanup(table.clone());

    let export = unique_name("ci_pc");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export}
    query: "SELECT id, created_at FROM {table}"
    partition_by: created_at
    partition_granularity: day
    mode: chunked
    chunk_column: id
    chunk_size: 2
    format: parquet
    destination:
      type: local
      path: {out}/{{partition}}
"#,
        out = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let result = run_rivet_export(&cfg_path, &export);
    assert!(
        result.status.success(),
        "chunked+partition run failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    let root = out_dir.path();
    assert_eq!(partition_rows(root, "created_at=2023-01-01"), 3, "day 1");
    assert_eq!(partition_rows(root, "created_at=2023-01-02"), 3, "day 2");
    assert_eq!(parquet_rows_recursive(root), 6, "chunked partitions: no loss/dup");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn partition_by_rejects_missing_token() {
    require_alive(LiveService::Postgres);

    let table = unique_name("part_notok");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, created_at TIMESTAMP);
         INSERT INTO {table} (id, created_at) VALUES (1, '2023-01-01 00:00:00');",
    ))
    .unwrap();
    let _guard = PgCleanup(table.clone());

    let export = unique_name("ci_notok");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    // No {partition} token in the destination — must be refused up front.
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export}
    query: "SELECT id, created_at FROM {table}"
    partition_by: created_at
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let result = run_rivet_export(&cfg_path, &export);
    assert!(
        !result.status.success(),
        "run without a {{partition}} token must fail"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("{partition}"),
        "error should mention the missing {{partition}} token, got: {stderr}"
    );
}
