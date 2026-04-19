//! Performance-safety smoke tests.
//!
//! QA backlog:
//!   * Task 9.1 — exercise multi-batch / multi-file paths under a non-trivial
//!     but CI-friendly dataset.
//!   * Task 9.2 — concurrency smoke for chunked export.
//!
//! These are *regression guards*, not benchmarks.  The wall-time budget is
//! generous (30s per test) so only egregious regressions (10x+ slowdown)
//! fail the suite.  Fine-grained performance work belongs in a separate
//! `criterion` harness.

mod common;

use std::time::{Duration, Instant};

use common::*;

/// Wall-time budget below which the test is considered healthy.  Above this,
/// the test *fails* — forcing an investigation instead of silently slowing
/// down CI over time.
const BUDGET: Duration = Duration::from_secs(30);

/// RAII cleanup guard for the wide test table.
struct WideTableCleanup(String);
impl Drop for WideTableCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

/// Seed a Postgres table with `row_count` rows of a wide-ish shape.
fn seed_wide_table(row_count: i64) -> (String, WideTableCleanup) {
    let name = unique_name("qa9");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            amount NUMERIC(12,2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            payload TEXT NOT NULL
        );"
    ))
    .unwrap();

    // Chunked inserts to avoid one gigantic SQL statement.  1_000 rows per
    // INSERT is a safe MTU for most Postgres configurations.
    let mut remaining = row_count;
    let mut base: i64 = 0;
    while remaining > 0 {
        let batch = remaining.min(1_000);
        let mut sql = format!("INSERT INTO {name} (id, name, amount, payload) VALUES ");
        for i in 0..batch {
            if i > 0 {
                sql.push_str(", ");
            }
            let id = base + i;
            sql.push_str(&format!(
                "({id}, 'name_{id}', {:.2}, repeat('x', 200))",
                (id as f64) * 0.1
            ));
        }
        c.batch_execute(&sql).unwrap();
        base += batch;
        remaining -= batch;
    }

    (name.clone(), WideTableCleanup(name))
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn task_9_1_moderate_dataset_exports_within_time_budget() {
    require_alive(LiveService::Postgres);

    // 5_000 rows × 200B payload ≈ 1 MiB of source data.  Large enough to
    // exercise multiple internal batches but small enough to stay under
    // the 30s CI budget with a wide safety margin.
    const ROWS: i64 = 5_000;
    let (table, _guard) = seed_wide_table(ROWS);

    let export_name = unique_name("qa91_moderate");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name, amount, created_at, payload FROM {table}"
    mode: full
    format: parquet
    compression: zstd
    destination: {{type: local, path: {dir}}}
    tuning: {{batch_size: 500}}
"#,
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let t0 = Instant::now();
    let out_run = run_rivet_export(&cfg, &export_name);
    let elapsed = t0.elapsed();

    assert!(
        out_run.status.success(),
        "moderate dataset export failed; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr)
    );
    assert!(
        elapsed < BUDGET,
        "moderate dataset ({ROWS} rows, batch_size=500) exceeded {BUDGET:?} budget: {elapsed:?}"
    );

    // Sanity: exactly one parquet was produced and its row count matches.
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let files = files_with_extension(out.path(), "parquet");
    assert_eq!(files.len(), 1);
    let bytes = std::fs::read(&files[0]).unwrap();
    let total: usize = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert_eq!(total, ROWS as usize);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn task_9_1_split_by_size_produces_multiple_files_and_no_row_loss() {
    // Forcing a small max_file_size splits the single dataset into several
    // parquet files.  Every chunk must go somewhere; the total row count
    // across all parts must equal the source.
    require_alive(LiveService::Postgres);

    const ROWS: i64 = 3_000;
    let (table, _guard) = seed_wide_table(ROWS);

    let export_name = unique_name("qa91_split");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name, amount, payload FROM {table}"
    mode: full
    format: parquet
    compression: none  # deterministic size → deterministic split count
    max_file_size: 64KB
    destination: {{type: local, path: {dir}}}
    tuning: {{batch_size: 250}}
"#,
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    assert!(run_rivet_export(&cfg, &export_name).status.success());

    let files = files_with_extension(out.path(), "parquet");
    assert!(
        files.len() >= 2,
        "64KB file cap + {ROWS} rows must split into multiple files; got {files:?}"
    );
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let total: usize = files
        .iter()
        .map(|p| {
            let bytes = std::fs::read(p).unwrap();
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                .unwrap()
                .build()
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(
        total, ROWS as usize,
        "split-by-size must never lose or duplicate rows"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn task_9_2_chunked_parallelism_exports_all_rows_without_duplicates() {
    // Parallel chunked export with 4 workers against an auto-split plan.
    // Assertions: total row count equals source; every source id appears
    // exactly once in the concatenation of output parquets.
    require_alive(LiveService::Postgres);

    const ROWS: i64 = 200;
    let (table, _guard) = seed_wide_table(ROWS);

    let export_name = unique_name("qa92_par");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: 30
    parallel: 4
    format: parquet
    compression: none
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let t0 = Instant::now();
    let out_run = run_rivet_export(&cfg, &export_name);
    let elapsed = t0.elapsed();

    assert!(
        out_run.status.success(),
        "parallel chunked export failed; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr)
    );
    assert!(elapsed < BUDGET);

    // Enumerate ids across all parts — must equal exactly 0..ROWS.
    use arrow::array::{Array, AsArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut ids = Vec::new();
    for p in files_with_extension(out.path(), "parquet") {
        let bytes = std::fs::read(&p).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let col = batch.column_by_name("id").expect("id column");
            if let Some(a) = col.as_primitive_opt::<arrow::datatypes::Int64Type>() {
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        ids.push(a.value(i));
                    }
                }
            }
        }
    }
    ids.sort();
    let expected: Vec<i64> = (0..ROWS).collect();
    assert_eq!(
        ids, expected,
        "parallel chunked must export each id exactly once"
    );
}
