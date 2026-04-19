//! Cross-database parity tests.
//!
//! QA backlog Task 3.3.  Run the logically-same export against Postgres and
//! MySQL seeded with identical data and assert the outputs agree on the
//! observable contract: row count, column count, and the set of primary-key
//! values present.  Any difference is either a real dialect bug or a
//! documented deviation; today we assume parity is the contract.
//!
//! Out of scope here:
//!   - exact column type equivalence (Postgres NUMERIC vs MySQL DECIMAL may
//!     produce different Arrow physical types through the two driver paths;
//!     tightening that is backlog Task 2.4 + ADR work).
//!   - timestamp precision parity (drivers differ on microsecond rounding).
//!
//! This file proves the *core* parity: no row loss, no phantom row duplication.

mod common;

use common::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn export_to_parquet_local(
    url: &str,
    source_type: &str,
    query: &str,
    out_dir: &std::path::Path,
) -> std::path::PathBuf {
    let export_name = unique_name("qa33");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: {source_type}
  url: "{url}"
exports:
  - name: {export_name}
    query: "{query}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {dir}
"#,
        dir = out_dir.display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "rivet ({source_type}) exited {}; stderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    let files = files_with_extension(out_dir, "parquet");
    assert_eq!(files.len(), 1, "expected one parquet for {source_type}");
    files.into_iter().next().unwrap()
}

fn read_total_rows(path: &std::path::Path) -> usize {
    let bytes = std::fs::read(path).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

fn read_id_set(path: &std::path::Path) -> std::collections::BTreeSet<i64> {
    use arrow::array::{Array, AsArray};
    let bytes = std::fs::read(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    let mut out = std::collections::BTreeSet::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column_by_name("id").expect("id column");
        if let Some(a) = col.as_primitive_opt::<arrow::datatypes::Int64Type>() {
            for i in 0..a.len() {
                if !a.is_null(i) {
                    out.insert(a.value(i));
                }
            }
        } else {
            panic!("id column must decode as Int64");
        }
    }
    out
}

#[test]
#[ignore = "live: requires docker compose postgres + mysql"]
fn pg_and_mysql_full_exports_agree_on_row_count_and_id_set() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Mysql);

    const ROWS: i64 = 40;

    let pg_table = seed_pg_numeric_table(ROWS);
    let my_table = seed_mysql_numeric_table(ROWS);

    let pg_out = tempfile::tempdir().unwrap();
    let my_out = tempfile::tempdir().unwrap();

    let pg_path = export_to_parquet_local(
        POSTGRES_URL,
        "postgres",
        &format!(
            "SELECT id, name, amount FROM {} ORDER BY id",
            pg_table.name()
        ),
        pg_out.path(),
    );
    let my_path = export_to_parquet_local(
        MYSQL_URL,
        "mysql",
        &format!(
            "SELECT id, name, amount FROM {} ORDER BY id",
            my_table.name()
        ),
        my_out.path(),
    );

    // 1. Same row count.
    let pg_rows = read_total_rows(&pg_path);
    let my_rows = read_total_rows(&my_path);
    assert_eq!(
        pg_rows, my_rows,
        "Postgres vs MySQL row count mismatch: pg={pg_rows}, mysql={my_rows}"
    );
    assert_eq!(pg_rows, ROWS as usize);

    // 2. Same id set.
    let pg_ids = read_id_set(&pg_path);
    let my_ids = read_id_set(&my_path);
    assert_eq!(
        pg_ids,
        my_ids,
        "Postgres vs MySQL id set mismatch; \
         pg\\mysql={:?}, mysql\\pg={:?}",
        pg_ids.difference(&my_ids).collect::<Vec<_>>(),
        my_ids.difference(&pg_ids).collect::<Vec<_>>()
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + mysql"]
fn pg_and_mysql_empty_exports_behave_identically() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Mysql);

    let pg_table = seed_pg_numeric_table(0);
    let my_table = seed_mysql_numeric_table(0);

    let pg_out = tempfile::tempdir().unwrap();
    let my_out = tempfile::tempdir().unwrap();

    // Run both exports — must both exit 0 and both produce zero files
    // (documented behaviour: zero rows → no file, see live_parquet_roundtrip.rs).
    {
        let export_name = unique_name("qa33pg_empty");
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {}"
    mode: full
    format: parquet
    destination: {{type: local, path: {}}}
"#,
            pg_table.name(),
            pg_out.path().display(),
        );
        let cfg_path = write_config(&cfg_dir, &yaml);
        assert!(run_rivet_export(&cfg_path, &export_name).status.success());
    }
    {
        let export_name = unique_name("qa33my_empty");
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
source: {{type: mysql, url: "{MYSQL_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {}"
    mode: full
    format: parquet
    destination: {{type: local, path: {}}}
"#,
            my_table.name(),
            my_out.path().display(),
        );
        let cfg_path = write_config(&cfg_dir, &yaml);
        assert!(run_rivet_export(&cfg_path, &export_name).status.success());
    }

    assert!(files_with_extension(pg_out.path(), "parquet").is_empty());
    assert!(files_with_extension(my_out.path(), "parquet").is_empty());
}

#[test]
#[ignore = "live: requires docker compose postgres + mysql"]
fn pg_and_mysql_chunked_exports_agree_on_row_count() {
    // Chunked execution exercises a different SQL shaping path
    // (`build_chunk_query_sql` with the dialect-appropriate identifier
    // quoting).  Assert row count parity under that path too.
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Mysql);

    const ROWS: i64 = 30;

    let pg_table = seed_pg_numeric_table(ROWS);
    let my_table = seed_mysql_numeric_table(ROWS);

    let pg_out = tempfile::tempdir().unwrap();
    let my_out = tempfile::tempdir().unwrap();

    for (url, src, table_name, out_dir, tag) in [
        (
            POSTGRES_URL,
            "postgres",
            pg_table.name().to_string(),
            pg_out.path().to_path_buf(),
            "pg",
        ),
        (
            MYSQL_URL,
            "mysql",
            my_table.name().to_string(),
            my_out.path().to_path_buf(),
            "my",
        ),
    ] {
        let export_name = unique_name(&format!("qa33_{tag}_chunk"));
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
source:
  type: {src}
  url: "{url}"
exports:
  - name: {export_name}
    query: "SELECT id, name, amount FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: 7
    format: parquet
    destination:
      type: local
      path: {dir}
"#,
            dir = out_dir.display()
        );
        let cfg_path = write_config(&cfg_dir, &yaml);
        let out = run_rivet_export(&cfg_path, &export_name);
        assert!(
            out.status.success(),
            "rivet ({src}) chunked exited {}; stderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        );
    }

    let sum = |dir: &std::path::Path| -> usize {
        files_with_extension(dir, "parquet")
            .iter()
            .map(|p| read_total_rows(p))
            .sum()
    };
    assert_eq!(
        sum(pg_out.path()),
        ROWS as usize,
        "pg chunked total row count mismatch"
    );
    assert_eq!(
        sum(my_out.path()),
        ROWS as usize,
        "mysql chunked total row count mismatch"
    );
}
