//! Shared Parquet assertions and DB setup for type-matrix live tests.

use std::path::Path;

use arrow::array::types::{Decimal128Type, TimestampMicrosecondType};
use arrow::array::{
    Array, AsArray, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use chrono::NaiveDateTime;
use mysql::prelude::Queryable;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use postgres::NoTls;

use crate::common::{
    MSSQL_URL, MYSQL_URL, POSTGRES_URL, mssql_drop_table, mssql_exec, run_rivet_export,
    unique_name, write_config,
};

/// RAII guard for the PostgreSQL matrix table + companion enum type. Drops
/// both on test exit so a failing assertion never poisons the next run.
pub struct PgCleanup {
    pub table: String,
    pub enum_type: String,
}

impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, NoTls) {
            let _ = c.execute(
                &format!(
                    "DROP TABLE IF EXISTS {} CASCADE; DROP TYPE IF EXISTS {} CASCADE",
                    self.table, self.enum_type
                ),
                &[],
            );
        }
    }
}

/// RAII guard for the MySQL matrix table.
pub struct MysqlCleanup(pub String);

impl Drop for MysqlCleanup {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

/// RAII guard for the SQL Server matrix table.
pub struct MssqlCleanup(pub String);

impl Drop for MssqlCleanup {
    fn drop(&mut self) {
        mssql_drop_table(&self.0);
    }
}

pub const PG_MATRIX_COLUMNS: &str = "\
    id, c_smallint, c_integer, c_bigint, amount, fee, price, c_real, c_double, \
    c_date, c_time, created_at, created_at_tz, label, c_varchar, c_bpchar, \
    raw_bytes, uid, attrs, attrs_json, c_bool, interval_col, enum_col, tags, nums, \
    large_text, note_nullable, note_all_null";

/// Same as [`PG_MATRIX_COLUMNS`] without the `List` columns (`tags`, `nums`):
/// CSV has no array cell, so a CSV export of those fails loudly by design.
/// Parquet keeps them.
pub const PG_MATRIX_COLUMNS_CSV: &str = "\
    id, c_smallint, c_integer, c_bigint, amount, fee, price, c_real, c_double, \
    c_date, c_time, created_at, created_at_tz, label, c_varchar, c_bpchar, \
    raw_bytes, uid, attrs, attrs_json, c_bool, interval_col, enum_col, \
    large_text, note_nullable, note_all_null";

pub const MYSQL_MATRIX_COLUMNS: &str = "\
    id, c_tinyint, c_tinyint_u, c_bool, c_boolean, c_smallint, c_smallint_u, c_int, c_int_u, \
    c_bigint, c_bigint_u, amount, fee, price, c_float, c_double, c_date, c_time, \
    created_at_dt, created_at_ts, label, c_char, c_text, c_varchar, long_text, medium_text, \
    raw_bytes, var_bytes, blob_bytes, uid, extras, enum_col, set_col, year_col, \
    c_bit1, c_bit8, note_nullable, note_all_null";

pub const MATRIX_COLUMN_OVERRIDES_YAML: &str = "    columns:\n      amount: decimal(18,2)\n      fee: decimal(20,6)\n      price: decimal(10,2)\n";

pub const MSSQL_MATRIX_COLUMNS: &str = "\
    id, c_smallint, c_int, c_bigint, c_tinyint, c_bit, amount, fee, price, \
    c_real, c_float, c_date, c_time, created_at, created_at_tz, label, c_varchar, c_char, \
    raw_bytes, uid, c_nvarchar, note_nullable, note_all_null";

pub fn read_parquet_batches(path: &Path) -> (SchemaRef, Vec<RecordBatch>) {
    let bytes = std::fs::read(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder.build().unwrap().map(|b| b.unwrap()).collect();
    (schema, batches)
}

pub fn sum_decimal128_scaled(batches: &[RecordBatch], col: &str) -> i128 {
    let mut sum = 0i128;
    for b in batches {
        let arr = b
            .column_by_name(col)
            .unwrap_or_else(|| panic!("missing column {col}"));
        assert!(
            matches!(arr.data_type(), DataType::Decimal128(..)),
            "contract: `{col}` must be Decimal128, got {:?}",
            arr.data_type()
        );
        let prim = arr.as_primitive::<Decimal128Type>();
        for i in 0..prim.len() {
            if prim.is_null(i) {
                continue;
            }
            sum = sum
                .checked_add(prim.value(i))
                .expect("sum overflow — unexpected test data scale");
        }
    }
    sum
}

pub fn matrix_naive_wall_micros(naive: &str) -> i64 {
    NaiveDateTime::parse_from_str(naive, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

fn assert_field_dtype(schema: &SchemaRef, name: &str, expected: DataType) {
    let field = schema
        .field_with_name(name)
        .unwrap_or_else(|_| panic!("missing field {name}"));
    assert_eq!(
        *field.data_type(),
        expected,
        "schema field `{name}` type mismatch"
    );
}

/// Create enum type + table + seed for PostgreSQL matrix. Returns enum type name for cleanup.
pub fn setup_pg_matrix_table(table_name: &str) -> String {
    let enum_type = format!("{table_name}_status");
    let mut c = crate::common::pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table_name} CASCADE; DROP TYPE IF EXISTS {enum_type} CASCADE;"
    ))
    .unwrap();
    c.batch_execute(&format!(
        "CREATE TYPE {enum_type} AS ENUM ('active', 'inactive', 'pending');"
    ))
    .unwrap();
    let schema = apply_table_name(&load_fixture("postgres_schema.sql"), table_name)
        .replace("{enum_type}", &enum_type);
    let seed = apply_table_name(&load_fixture("postgres_seed.sql"), table_name);
    c.batch_execute(&schema).unwrap();
    c.batch_execute(&seed).unwrap();
    enum_type
}

pub fn setup_mysql_matrix_table(table_name: &str) {
    let mut c = crate::common::mysql_connect();
    c.query_drop(format!("DROP TABLE IF EXISTS {table_name}"))
        .unwrap();
    c.query_drop(apply_table_name(
        &load_fixture("mysql_schema.sql"),
        table_name,
    ))
    .unwrap();
    for stmt in apply_table_name(&load_fixture("mysql_seed.sql"), table_name)
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        c.query_drop(stmt).unwrap();
    }
}

pub fn assert_pg_doc_parquet_schema(schema: &SchemaRef) {
    assert_field_dtype(schema, "c_smallint", DataType::Int16);
    assert_field_dtype(schema, "c_integer", DataType::Int32);
    assert_field_dtype(schema, "c_bigint", DataType::Int64);
    assert_field_dtype(schema, "amount", DataType::Decimal128(18, 2));
    assert_field_dtype(schema, "fee", DataType::Decimal128(20, 6));
    assert_field_dtype(schema, "price", DataType::Decimal128(10, 2));
    assert_field_dtype(schema, "c_real", DataType::Float32);
    assert_field_dtype(schema, "c_double", DataType::Float64);
    assert_field_dtype(schema, "c_date", DataType::Date32);
    assert_field_dtype(schema, "c_time", DataType::Time64(TimeUnit::Microsecond));
    assert_field_dtype(
        schema,
        "created_at",
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    assert_field_dtype(
        schema,
        "created_at_tz",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    );
    assert_field_dtype(schema, "label", DataType::Utf8);
    assert_field_dtype(schema, "c_varchar", DataType::Utf8);
    assert_field_dtype(schema, "c_bpchar", DataType::Utf8);
    assert_field_dtype(schema, "raw_bytes", DataType::Binary);
    // UUID columns now land as FixedSizeBinary(16) so the Parquet file
    // carries native `LogicalType::Uuid` — Arrow data type changed but the
    // value semantics are preserved (16 canonical bytes).
    assert_field_dtype(schema, "uid", DataType::FixedSizeBinary(16));
    // JSON columns stay as Utf8 on the Arrow side; the `arrow.json`
    // extension type drives the native `LogicalType::Json` in Parquet
    // metadata without changing the on-disk byte payload.
    assert_field_dtype(schema, "attrs", DataType::Utf8);
    assert_field_dtype(schema, "attrs_json", DataType::Utf8);
    assert_field_dtype(schema, "c_bool", DataType::Boolean);
    assert_field_dtype(schema, "interval_col", DataType::Utf8);
    assert_field_dtype(schema, "enum_col", DataType::Utf8);
    assert_field_dtype(schema, "large_text", DataType::Utf8);
    match schema.field_with_name("tags").unwrap().data_type() {
        DataType::List(f) => assert_eq!(f.data_type(), &DataType::Utf8),
        dt => panic!("tags expected List<Utf8>, got {dt:?}"),
    }
    match schema.field_with_name("nums").unwrap().data_type() {
        DataType::List(f) => assert_eq!(f.data_type(), &DataType::Int32),
        dt => panic!("nums expected List<Int32>, got {dt:?}"),
    }
    assert_eq!(
        schema
            .field_with_name("enum_col")
            .unwrap()
            .metadata()
            .get("rivet.logical_type")
            .map(String::as_str),
        Some("enum")
    );
}

pub fn assert_pg_doc_parquet_values(batches: &[RecordBatch]) {
    assert_eq!(
        sum_decimal128_scaled(batches, "amount"),
        99_999_999_990_024_i128
    );
    assert_eq!(sum_decimal128_scaled(batches, "fee"), 10_000_003_i128);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);

    let b = batches.first().unwrap();
    let r1 = matrix_naive_wall_micros("2035-08-07 09:08:07.987654");
    assert_eq!(
        b.column_by_name("c_bigint")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        9223372036854775807
    );
    assert_eq!(
        b.column_by_name("created_at")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        r1
    );
    assert_eq!(
        b.column_by_name("interval_col")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "P1Y2M3D"
    );
    assert_eq!(
        b.column_by_name("enum_col")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "active"
    );
    let large = b
        .column_by_name("large_text")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(large.value(0).len(), 5000);
    assert_eq!(large.value(0).chars().next(), Some('X'));

    // all-null column
    let all_null = b.column_by_name("note_all_null").unwrap();
    for i in 0..all_null.len() {
        assert!(all_null.is_null(i), "note_all_null row {i} must be null");
    }

    let tags = b
        .column_by_name("tags")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let tag_values = tags.value(0);
    let tag_strs = tag_values.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(tag_strs.value(0), "alpha");
}

pub fn assert_mysql_doc_parquet_schema(schema: &SchemaRef) {
    assert_field_dtype(schema, "c_tinyint", DataType::Int16);
    assert_field_dtype(schema, "c_tinyint_u", DataType::Int16);
    assert_field_dtype(schema, "c_bool", DataType::Boolean);
    assert_field_dtype(schema, "c_boolean", DataType::Boolean);
    assert_field_dtype(schema, "c_smallint", DataType::Int16);
    assert_field_dtype(schema, "c_smallint_u", DataType::Int32);
    assert_field_dtype(schema, "c_int", DataType::Int32);
    assert_field_dtype(schema, "c_int_u", DataType::Int64);
    assert_field_dtype(schema, "c_bigint", DataType::Int64);
    assert_field_dtype(schema, "c_bigint_u", DataType::UInt64);
    assert_field_dtype(schema, "amount", DataType::Decimal128(18, 2));
    assert_field_dtype(schema, "fee", DataType::Decimal128(20, 6));
    assert_field_dtype(schema, "price", DataType::Decimal128(10, 2));
    assert_field_dtype(schema, "c_float", DataType::Float32);
    assert_field_dtype(schema, "c_double", DataType::Float64);
    assert_field_dtype(schema, "c_date", DataType::Date32);
    assert_field_dtype(schema, "c_time", DataType::Time64(TimeUnit::Microsecond));
    assert_field_dtype(
        schema,
        "created_at_dt",
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
    assert_field_dtype(
        schema,
        "created_at_ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    );
    assert_field_dtype(schema, "label", DataType::Utf8);
    assert_field_dtype(schema, "c_char", DataType::Utf8);
    assert_field_dtype(schema, "c_text", DataType::Utf8);
    assert_field_dtype(schema, "c_varchar", DataType::Utf8);
    assert_field_dtype(schema, "long_text", DataType::Utf8);
    assert_field_dtype(schema, "medium_text", DataType::Utf8);
    assert_field_dtype(schema, "raw_bytes", DataType::Binary);
    assert_field_dtype(schema, "var_bytes", DataType::Binary);
    assert_field_dtype(schema, "blob_bytes", DataType::Binary);
    assert_field_dtype(schema, "extras", DataType::Utf8);
    assert_field_dtype(schema, "enum_col", DataType::Utf8);
    assert_field_dtype(schema, "set_col", DataType::Utf8);
    assert_field_dtype(schema, "year_col", DataType::Int16);
    assert_field_dtype(schema, "c_bit1", DataType::Boolean);
    assert_field_dtype(schema, "c_bit8", DataType::Int64);
}

pub fn assert_mysql_doc_parquet_values(batches: &[RecordBatch]) {
    assert_eq!(
        sum_decimal128_scaled(batches, "amount"),
        99_999_999_990_024_i128
    );
    assert_eq!(sum_decimal128_scaled(batches, "fee"), 10_000_003_i128);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);

    let b = batches.first().unwrap();
    let r1 = matrix_naive_wall_micros("2035-08-07 09:08:07.987654");
    assert_eq!(
        b.column_by_name("c_tinyint_u")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        255
    );
    assert_eq!(
        b.column_by_name("c_smallint_u")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        65535
    );
    assert_eq!(
        b.column_by_name("c_int_u")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4294967295
    );
    assert_eq!(
        b.column_by_name("c_bigint_u")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        18446744073709551615
    );
    assert_eq!(
        b.column_by_name("enum_col")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "b"
    );
    assert_eq!(
        b.column_by_name("year_col")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        2024
    );
    assert_eq!(
        b.column_by_name("long_text")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .len(),
        5000
    );
    assert_eq!(
        b.column_by_name("created_at_dt")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .value(0),
        r1
    );
    assert_eq!(
        b.column_by_name("c_bit8")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        255
    );
}

pub fn load_fixture(name: &str) -> String {
    let path = format!(
        "{}/tests/type_roundtrip/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read fixture {name}: {e}"))
}

pub fn apply_table_name(sql: &str, table_name: &str) -> String {
    sql.replace("{table_name}", table_name)
}

/// Run the canonical PG type-matrix export with an explicit Parquet
/// compression codec. `compression` must be one of the names accepted by
/// `CompressionType` (`zstd`, `snappy`, `gzip`, `lz4`, `none`).
pub fn run_pg_matrix_export_with_compression(table_name: &str, compression: &str, out_dir: &Path) {
    let export_name = unique_name("type_rt_pg_cmp");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: >-
      SELECT {PG_MATRIX_COLUMNS}
      FROM {table_name} ORDER BY id
    mode: full
    format: parquet
    compression: {compression}
{MATRIX_COLUMN_OVERRIDES_YAML}
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "postgres type-matrix parquet/{compression}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Run the canonical PG type-matrix export and write the resulting Parquet
/// (or CSV) into `out_dir`. Shared by the original Parquet/CSV roundtrips and
/// the DuckDB / ClickHouse validators (ADR-0014 §4).
pub fn run_pg_matrix_export(table_name: &str, format: &str, out_dir: &Path) {
    let export_name = unique_name("type_rt_pg");
    let cfg_dir = tempfile::tempdir().unwrap();
    // CSV has no array cell — drop the List columns (Parquet keeps them).
    let columns = if format == "csv" {
        PG_MATRIX_COLUMNS_CSV
    } else {
        PG_MATRIX_COLUMNS
    };
    // CSV has no compression encoder — rivet (correctly) refuses `csv + zstd`
    // rather than silently drop the codec, so the CSV variant must use `none`.
    let compression = if format == "csv" { "none" } else { "zstd" };
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: >-
      SELECT {columns}
      FROM {table_name} ORDER BY id
    mode: full
    format: {format}
    compression: {compression}
{MATRIX_COLUMN_OVERRIDES_YAML}
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "postgres type-matrix {format}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Run the canonical MySQL type-matrix export and write the resulting Parquet
/// (or CSV) into `out_dir`. See [`run_pg_matrix_export`].
pub fn run_mysql_matrix_export(table_name: &str, format: &str, out_dir: &Path) {
    let export_name = unique_name("type_rt_my");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: >-
      SELECT {MYSQL_MATRIX_COLUMNS}
      FROM {table_name} ORDER BY id
    mode: full
    format: {format}
    compression: zstd
{MATRIX_COLUMN_OVERRIDES_YAML}
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "mysql type-matrix {format}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Create + seed the SQL Server type-matrix table from the shared fixtures.
pub fn setup_mssql_matrix_table(table_name: &str) {
    mssql_drop_table(table_name);
    mssql_exec(&apply_table_name(
        &load_fixture("mssql_schema.sql"),
        table_name,
    ));
    mssql_exec(&apply_table_name(
        &load_fixture("mssql_seed.sql"),
        table_name,
    ));
}

/// Export the SQL Server type matrix to `out_dir`. Decimal precision is pinned
/// via the shared `columns:` overrides (same as PG/MySQL) so `amount` / `fee` /
/// `price` land at their declared precision rather than the driver's
/// max-precision default.
pub fn run_mssql_matrix_export(table_name: &str, format: &str, out_dir: &Path) {
    let export_name = unique_name("type_rt_ms");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
exports:
  - name: {export_name}
    query: >-
      SELECT {MSSQL_MATRIX_COLUMNS}
      FROM {table_name} ORDER BY id
    mode: full
    format: {format}
    compression: zstd
{MATRIX_COLUMN_OVERRIDES_YAML}
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "mssql type-matrix {format}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
