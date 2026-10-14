//! Cross-database parity tests.
//!
//! QA backlog Task 3.3.  Run the logically-same export against Postgres and
//! MySQL seeded with identical data and assert the outputs agree on the
//! observable contract: row count, column count, and the set of primary-key
//! values present.  Any difference is either a real dialect bug or a
//! documented deviation; today we assume parity is the contract.
//!
//! Out of scope here:
//!   - exact column type equivalence beyond decimals (one body below holds
//!     `decimal(p,s)` -> `Decimal128(p,s)` on every engine; the rest of the
//!     type families are per-engine goldens).
//!   - timestamp precision parity (drivers differ on microsecond rounding).
//!
//! This file proves the *core* parity: no row loss, no phantom row duplication.

use crate::common::*;
use arrow::array::types::Decimal128Type;
use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn export_to_parquet_local(
    url: &str,
    source_type: &str,
    query: &str,
    out_dir: &std::path::Path,
) -> std::path::PathBuf {
    let export_name = unique_name("qa33");
    let base = match source_type {
        "postgres" => Rig::pg_batch(&export_name),
        "mysql" => Rig::mysql_batch(&export_name),
        other => panic!("parity helper knows postgres|mysql, got {other}"),
    };
    let rig = base
        .source_url(url)
        .query(query)
        .mode("full")
        .export_line("columns:")
        .export_line("  amount: decimal(12,2)")
        .dest_path(out_dir.to_path_buf());
    let out = rig.run_args(&["--export", &export_name]);
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
        let rig = Rig::pg_batch(&export_name)
            .query(&format!("SELECT id FROM {}", pg_table.name()))
            .mode("full")
            .dest_path(pg_out.path().to_path_buf());
        assert!(rig.run_args(&["--export", &export_name]).status.success());
    }
    {
        let export_name = unique_name("qa33my_empty");
        let rig = Rig::mysql_batch(&export_name)
            .query(&format!("SELECT id FROM {}", my_table.name()))
            .mode("full")
            .dest_path(my_out.path().to_path_buf());
        assert!(rig.run_args(&["--export", &export_name]).status.success());
    }

    assert!(files_with_extension(pg_out.path(), "parquet").is_empty());
    assert!(files_with_extension(my_out.path(), "parquet").is_empty());
}

#[test]
#[ignore = "live: requires docker compose postgres + mysql"]
fn pg_and_mysql_chunked_exports_agree_on_row_count_and_id_set() {
    // Chunked execution exercises a different SQL shaping path
    // (`build_chunk_query_sql` with the dialect-appropriate identifier
    // quoting).  Assert row count parity under that path too — AND id-set
    // equality: the range/BETWEEN chunk path splits the id space at chunk
    // boundaries (here 6|7, 13|14, 20|21, 27|28 for contiguous ids 0..29 at
    // chunk_size 7), so a boundary off-by-one that DROPS chunk[i].end while
    // DUPLICATING chunk[i+1].start nets to the same physical count and slips
    // past a count-only assertion. count==ROWS catches a net-duplication;
    // the id-set == {0..ROWS} catches any dropped id; together they prove
    // exactly-once. (keyset already has this lock via read_uid_set; the
    // BETWEEN path did not — see the data-correctness audit.)
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
        let base = match src {
            "postgres" => Rig::pg_batch(&export_name),
            "mysql" => Rig::mysql_batch(&export_name),
            other => panic!("parity loop knows postgres|mysql, got {other}"),
        };
        let rig = base
            .source_url(url)
            .query(&format!("SELECT id, name, amount FROM {table_name}"))
            .mode("chunked")
            .export_line("chunk_column: id")
            .export_line("chunk_size: 7")
            .export_line("columns:")
            .export_line("  amount: decimal(12,2)")
            .dest_path(out_dir.clone());
        let out = rig.run_args(&["--export", &export_name]);
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
    // Union of per-file id sets — re-reads the destination Parquet, not rivet's
    // own counters. A dropped boundary row leaves a hole here even when the
    // physical count is restored by a duplicate elsewhere.
    let union_ids = |dir: &std::path::Path| -> std::collections::BTreeSet<i64> {
        files_with_extension(dir, "parquet")
            .iter()
            .flat_map(|p| read_id_set(p))
            .collect()
    };
    let expected: std::collections::BTreeSet<i64> = (0..ROWS).collect();

    assert_eq!(
        sum(pg_out.path()),
        ROWS as usize,
        "pg chunked total row count mismatch (net duplication or loss)"
    );
    assert_eq!(
        union_ids(pg_out.path()),
        expected,
        "pg chunked id-set must equal source 0..{ROWS} — a missing id is a boundary DROP"
    );
    assert_eq!(
        sum(my_out.path()),
        ROWS as usize,
        "mysql chunked total row count mismatch (net duplication or loss)"
    );
    assert_eq!(
        union_ids(my_out.path()),
        expected,
        "mysql chunked id-set must equal source 0..{ROWS} — a missing id is a boundary DROP"
    );
}

/// Head-to-head parity extended to SQL Server: PG and MSSQL, seeded identically
/// (`id 0..ROWS`, `amount = id*1.5`), must export the same row count and the same
/// `id`-set. Closes the MSSQL cell of `docs/cross-config-matrix.yaml::cross_db_parity`
/// — previously MSSQL only had its own independent differential, never a
/// cross-engine agreement check.
#[test]
#[ignore = "live: requires docker compose up -d postgres mssql"]
fn pg_and_mssql_full_exports_agree_on_row_count_and_id_set() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Mssql);
    const ROWS: i64 = 500;

    let pg_table = seed_pg_numeric_table(ROWS);
    let ms_table = seed_mssql_numeric_table(ROWS);

    let pg_out = tempfile::tempdir().unwrap();
    let pg_path = export_to_parquet_local(
        POSTGRES_URL,
        "postgres",
        &format!("SELECT id, name, amount FROM {}", pg_table.name()),
        pg_out.path(),
    );

    // MSSQL through its own constructor (mssql_batch already carries the tls
    // opt-in the old inline yaml spelled by hand).
    let ms_out = tempfile::tempdir().unwrap();
    let ms_path = {
        let export_name = unique_name("qa33ms");
        let rig = Rig::mssql_batch(&export_name)
            .query(&format!(
                "SELECT id, name, amount FROM dbo.{}",
                ms_table.name()
            ))
            .mode("full")
            .dest_path(ms_out.path().to_path_buf());
        let out = rig.run_args(&["--export", &export_name]);
        assert!(
            out.status.success(),
            "mssql export failed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        files_with_extension(ms_out.path(), "parquet")
            .into_iter()
            .next()
            .expect("one mssql parquet")
    };

    assert_eq!(
        read_total_rows(&pg_path),
        read_total_rows(&ms_path),
        "PG and MSSQL must export equal row counts"
    );
    assert_eq!(
        read_id_set(&pg_path),
        read_id_set(&ms_path),
        "PG and MSSQL must export equal id-sets"
    );
}

/// The scaled values of Decimal128 column `col` over `batches`, in row order; the column must be `Decimal128(p, s)`.
fn decimal_values(
    batches: &[arrow::record_batch::RecordBatch],
    col: &str,
    p: u8,
    s: i8,
) -> Vec<i128> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(col)
            .unwrap_or_else(|| panic!("missing column {col}"));
        assert_eq!(
            arr.data_type(),
            &DataType::Decimal128(p, s),
            "{col} must reach Parquet as Decimal128({p}, {s}), the type its catalog declares, never a float or text"
        );
        let prim = arr.as_primitive::<Decimal128Type>();
        out.extend((0..prim.len()).map(|i| prim.value(i)));
    }
    out
}

/// A `decimal(18,2)` and a `decimal(18,6)` column reach Parquet as `Decimal128` at the precision and scale the engine's catalog declares, with no override, and the values sum exactly.
fn a_decimal_column_keeps_its_catalog_precision_and_scale(e: SqlEngine) {
    e.alive();
    let (id, amount, fee) = (e.col("id"), e.col("amount"), e.col("fee"));
    let (table, _guard) = e.create(
        "golden_dec",
        &format!(
            "{id} {} PRIMARY KEY, {amount} {}, {fee} {}",
            e.int64(),
            e.decimal(18, 2),
            e.decimal(18, 6)
        ),
    );
    for row in [
        "1, 0.10, 0.000001",
        "2, 0.20, 0.000002",
        "3, 999999999999.99, 10.123456",
        "4, -100.05, -0.123456",
    ] {
        e.exec(&format!(
            "INSERT INTO {table} ({id}, {amount}, {fee}) VALUES ({row})"
        ));
    }
    let rig = e.rig("golden_dec").query(&format!(
        "SELECT {id}, {amount}, {fee} FROM {table} ORDER BY {id}"
    ));
    rig.run_ok();
    let batches = read_all_parts(&rig.out_dir());
    let amounts = decimal_values(&batches, "amount", 18, 2);
    let fees = decimal_values(&batches, "fee", 18, 6);
    assert_eq!(
        amounts,
        vec![10, 20, 99_999_999_999_999, -10_005],
        "amount x 10^2, row by row"
    );
    assert_eq!(
        fees,
        vec![1, 2, 10_123_456, -123_456],
        "fee x 10^6, row by row"
    );
    assert_eq!(
        amounts.iter().sum::<i128>(),
        99_999_999_990_024,
        "SUM(amount) x 10^2 must equal the source's 999999999900.24 (decoding stays exact)"
    );
    assert_eq!(
        fees.iter().sum::<i128>(),
        10_000_003,
        "SUM(fee) x 10^6 must equal the source's 10.000003"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_decimal_column_keeps_its_catalog_precision_and_scale_postgres() {
    a_decimal_column_keeps_its_catalog_precision_and_scale(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_decimal_column_keeps_its_catalog_precision_and_scale_mysql() {
    a_decimal_column_keeps_its_catalog_precision_and_scale(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_decimal_column_keeps_its_catalog_precision_and_scale_mssql() {
    a_decimal_column_keeps_its_catalog_precision_and_scale(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_decimal_column_keeps_its_catalog_precision_and_scale_oracle() {
    a_decimal_column_keeps_its_catalog_precision_and_scale(SqlEngine::Oracle);
}
