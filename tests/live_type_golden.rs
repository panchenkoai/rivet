//! Golden E2E tests: Trust & Reproducibility (roadmap Milestone §1).
//!
//! **Roadmap doc:** [docs/reference/testing.md](../../docs/reference/testing.md#trust-milestone-type-golden-round-trip)
//!
//! Mirrors the same contracts for Postgres and MySQL:
//!
//! - decimal exactness (`Decimal128`, fixed sums — no Utf8/FLOAT64 regressions),
//! - temporal semantics (`TIMESTAMP`/UTC vs naive wall-clock; PG `TIMESTAMP`/`TIMESTAMPTZ`,
//!   MySQL `DATETIME`/ `TIMESTAMP` with Rivet `SET time_zone = '+00:00'`),
//! - binary round-trip (`BYTEA` / `BLOB`; MySQL `BINARY(n)` + charset 63),
//! - canonical UUID-ish text (`Uuid` PG / fixed-width UTF-8 char on MySQL),
//! - combined demo table **`rivet_type_matrix`**: one Parquet covers decimals, µs timestamps,
//!   binary, Utf8 UUID + JSON/`attrs`.
//!
//! Live tests require `docker compose up -d` (Postgres + MySQL as applicable). Each test is `#[ignore]`.

mod common;

use std::path::Path;

use arrow::array::types::{Decimal128Type, TimestampMicrosecondType};
use arrow::array::{
    Array, AsArray, BinaryArray, BooleanArray, Int16Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use chrono::NaiveDateTime;
use common::*;
use mysql::prelude::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use postgres::NoTls;
use serde_json::json;

struct PgCleanup(String);

impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

struct MysqlCleanup(String);

impl Drop for MysqlCleanup {
    fn drop(&mut self) {
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

fn read_parquet_batches(path: &Path) -> (SchemaRef, Vec<RecordBatch>) {
    let bytes = std::fs::read(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder.build().unwrap().map(|b| b.unwrap()).collect();
    (schema, batches)
}

/// Sum Decimal128 scaled integers for column `col` across all batches (panic if type diverges).
fn sum_decimal128_scaled(batches: &[RecordBatch], col: &str) -> i128 {
    let mut sum = 0i128;
    for b in batches {
        let arr = b
            .column_by_name(col)
            .unwrap_or_else(|| panic!("missing column {col}"));
        assert!(
            matches!(arr.data_type(), DataType::Decimal128(..)),
            "golden contract: `{col}` must be Decimal128 in Parquet Arrow schema, got {:?}",
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

/// Matches `cargo run` TIMESTAMP / naive wall semantics in other goldens (`and_utc` on naive local).
fn matrix_naive_wall_micros(naive: &str) -> i64 {
    NaiveDateTime::parse_from_str(naive, "%Y-%m-%d %H:%M:%S%.f")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn golden_decimal_payments_exact_sums_and_decimal128_types() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("golden_pay");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            amount NUMERIC(18, 2),
            fee NUMERIC(18, 6)
        );

        INSERT INTO {table_name} (id, amount, fee) VALUES
          (1, 0.10, 0.000001),
          (2, 0.20, 0.000002),
          (3, 999999999999.99, 10.123456),
          (4, -100.05, -0.123456);",
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("golden_dec");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, amount, fee FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    columns:
      amount: decimal(18,2)
      fee: decimal(18,6)
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "golden decimal export failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1, "golden export must emit one parquet");

    let (schema, batches) = read_parquet_batches(&files[0]);
    let fields: Vec<_> = schema.fields().iter().collect();
    assert_eq!(fields.len(), 3);
    match fields[1].data_type() {
        DataType::Decimal128(p, s) => {
            assert_eq!((*p, *s), (18u8, 2i8), "amount must stay Decimal128(18,2)");
        }
        other => panic!(
            "amount column must not downgrade to {:?} (golden no-float/no-utf8 contract)",
            other
        ),
    }
    match fields[2].data_type() {
        DataType::Decimal128(p, s) => {
            assert_eq!((*p, *s), (18u8, 6i8), "fee must stay Decimal128(18,6)")
        }
        other => panic!("fee column golden type mismatch: {other:?}"),
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);

    // Roadmap sums: SUM(amount) = 999999999900.24 ; SUM(fee) = 10.000003
    let sum_amount_scaled = sum_decimal128_scaled(&batches, "amount");
    let sum_fee_scaled = sum_decimal128_scaled(&batches, "fee");

    assert_eq!(
        sum_amount_scaled, 99_999_999_990_024_i128,
        "SUM(amount)×10² must equal source golden (decoding must stay exact)"
    );
    assert_eq!(
        sum_fee_scaled, 10_000_003_i128,
        "SUM(fee)×10⁶ must equal source golden"
    );

    // Row-level anchors (scaled integers)
    let b0 = &batches[0];
    let amounts = b0
        .column_by_name("amount")
        .unwrap()
        .as_primitive::<Decimal128Type>();
    let fees = b0
        .column_by_name("fee")
        .unwrap()
        .as_primitive::<Decimal128Type>();
    assert_eq!(amounts.value(0), 10_i128); // 0.10 × 100
    assert_eq!(fees.value(0), 1_i128); // 0.000001 × 10⁶
    assert_eq!(amounts.value(3), -10_005);
    assert_eq!(fees.value(3), -123_456);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn golden_timestamps_naive_vs_timestamptz_parquet_microsecond_semantics() {
    require_alive(LiveService::Postgres);

    let naive1 =
        NaiveDateTime::parse_from_str("2035-08-07 09:08:07.987654", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap();
    let expected_row1_micros = naive1.and_utc().timestamp_micros();

    let naive2_wall_clock_as_utc =
        NaiveDateTime::parse_from_str("2019-02-03 03:07:06.554433", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap();
    let expected_row2_naive_micros = naive2_wall_clock_as_utc.and_utc().timestamp_micros();
    // Same absolute instant encoded as TIMESTAMPTZ with +05 offset (same as row2 naive when interpreted UTC).
    let expected_row2_tstz_micros = expected_row2_naive_micros;

    let table_name = unique_name("golden_evt");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        r#"CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            created_at_tz TIMESTAMPTZ NOT NULL
        );

        INSERT INTO {table_name} VALUES
          (1, TIMESTAMP '2035-08-07 09:08:07.987654',
              TIMESTAMPTZ '2035-08-07 09:08:07.987654Z'),
          (2, TIMESTAMP '2019-02-03 03:07:06.554433',
              TIMESTAMPTZ '2019-02-03 08:07:06.554433+05');"#,
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("golden_ts");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, created_at, created_at_tz FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "golden timestamp export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);
    match schema.field_with_name("created_at").unwrap().data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => {}
        dt => panic!("TIMESTAMP WITHOUT TIME ZONE must become tz=None Arrow timestamp, got {dt:?}"),
    }
    match schema.field_with_name("created_at_tz").unwrap().data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => assert_eq!(tz.as_ref(), "UTC"),
        dt => panic!("TIMESTAMPTZ must normalize to Timestamp(..., UTC), got {dt:?}"),
    }

    let b = batches.first().unwrap();
    let naive_phys = b
        .column_by_name("created_at")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    let tstz_phys = b
        .column_by_name("created_at_tz")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();

    assert_eq!(naive_phys.value(0), expected_row1_micros);
    assert_eq!(tstz_phys.value(0), expected_row1_micros);

    assert_eq!(naive_phys.value(1), expected_row2_naive_micros);
    assert_eq!(tstz_phys.value(1), expected_row2_tstz_micros);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn golden_bytea_exact_binary_roundtrip() {
    require_alive(LiveService::Postgres);

    let payload: Vec<u8> = vec![0x00, 0xff, 0x01, 0x23, 0x45];

    let table_name = unique_name("golden_bin");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, blob BYTEA NOT NULL);",
    ))
    .unwrap();
    c.execute(
        &format!("INSERT INTO {table_name} (id, blob) VALUES (1, $1)"),
        &[&payload],
    )
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("golden_blob");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT blob FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "golden bytea export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);

    let (schema, batches) = read_parquet_batches(&files[0]);
    assert!(matches!(
        schema.field_with_name("blob").unwrap().data_type(),
        DataType::Binary
    ));

    let col = batches[0]
        .column_by_name("blob")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("BINARY column");
    assert_eq!(col.value(0), payload.as_slice());
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn golden_uuid_utf8_roundtrip_expected_canonical_lower() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("golden_uid");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        r#"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, uid UUID NOT NULL);
           INSERT INTO {table_name} VALUES (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011'::uuid);"#,
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("golden_uuid");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT uid FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "golden uuid export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    let (_, batches) = read_parquet_batches(&files[0]);

    let col = batches[0]
        .column_by_name("uid")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("uuid maps to Utf8");
    assert!(matches!(
        batches[0]
            .schema()
            .field_with_name("uid")
            .unwrap()
            .data_type(),
        DataType::Utf8
    ));

    assert_eq!(
        col.value(0),
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011",
        "PostgreSQL canonical uuid text form must survive export"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_golden_decimal_payments_exact_sums_and_decimal128_types() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("golden_pay_my");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            amount DECIMAL(18, 2) NOT NULL,
            fee DECIMAL(18, 6) NOT NULL
        ) ENGINE=InnoDB;

        INSERT INTO {table_name} (id, amount, fee) VALUES
          (1, 0.10, 0.000001),
          (2, 0.20, 0.000002),
          (3, 999999999999.99, 10.123456),
          (4, -100.05, -0.123456);",
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("golden_my_dec");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, amount, fee FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    columns:
      amount: decimal(18,2)
      fee: decimal(18,6)
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "mysql golden decimal export failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1, "mysql golden export must emit one parquet");

    let (schema, batches) = read_parquet_batches(&files[0]);
    let fields: Vec<_> = schema.fields().iter().collect();
    assert_eq!(fields.len(), 3);
    match fields[1].data_type() {
        DataType::Decimal128(p, s) => {
            assert_eq!((*p, *s), (18u8, 2i8), "amount must stay Decimal128(18,2)");
        }
        other => panic!(
            "mysql amount column must not downgrade to {:?} (golden no-float/no-utf8 contract)",
            other
        ),
    }
    match fields[2].data_type() {
        DataType::Decimal128(p, s) => {
            assert_eq!((*p, *s), (18u8, 6i8), "fee must stay Decimal128(18,6)")
        }
        other => panic!("mysql fee column golden type mismatch: {other:?}"),
    }

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);

    let sum_amount_scaled = sum_decimal128_scaled(&batches, "amount");
    let sum_fee_scaled = sum_decimal128_scaled(&batches, "fee");

    assert_eq!(sum_amount_scaled, 99_999_999_990_024_i128);
    assert_eq!(sum_fee_scaled, 10_000_003_i128);

    let b0 = &batches[0];
    let amounts = b0
        .column_by_name("amount")
        .unwrap()
        .as_primitive::<Decimal128Type>();
    let fees = b0
        .column_by_name("fee")
        .unwrap()
        .as_primitive::<Decimal128Type>();
    assert_eq!(amounts.value(0), 10_i128);
    assert_eq!(fees.value(0), 1_i128);
    assert_eq!(amounts.value(3), -10_005);
    assert_eq!(fees.value(3), -123_456);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_golden_datetime_vs_timestamp_parquet_microsecond_semantics() {
    require_alive(LiveService::Mysql);

    let naive1 =
        NaiveDateTime::parse_from_str("2035-08-07 09:08:07.987654", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap();
    let expected_row1_micros = naive1.and_utc().timestamp_micros();

    let naive2_wall_clock_as_utc =
        NaiveDateTime::parse_from_str("2019-02-03 03:07:06.554433", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap();
    let expected_row2_naive_micros = naive2_wall_clock_as_utc.and_utc().timestamp_micros();
    let expected_row2_ts_micros = expected_row2_naive_micros;

    let table_name = unique_name("golden_evt_my");
    let mut c = mysql_connect();
    // DATETIME(6) = naive wall-clock; TIMESTAMP(6) with Rivet SET time_zone = '+00:00' stores UTC —
    // same literals as Postgres golden row-pair parquet microsecond expectations.
    c.query_drop(format!(
        r"CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            created_at_dt DATETIME(6) NOT NULL,
            created_at_ts TIMESTAMP(6) NOT NULL
        ) ENGINE=InnoDB;

        INSERT INTO {table_name} VALUES
          (1, '2035-08-07 09:08:07.987654', '2035-08-07 09:08:07.987654'),
          (2, '2019-02-03 03:07:06.554433', '2019-02-03 03:07:06.554433');",
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("golden_my_ts");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, created_at_dt AS created_at, created_at_ts AS created_at_tz FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "mysql golden timestamp export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);
    match schema.field_with_name("created_at").unwrap().data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => {}
        dt => panic!("MySQL DATETIME must become tz=None Arrow timestamp, got {dt:?}"),
    }
    match schema.field_with_name("created_at_tz").unwrap().data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => assert_eq!(tz.as_ref(), "UTC"),
        dt => panic!("MySQL TIMESTAMP must normalize to Timestamp(..., UTC), got {dt:?}"),
    }

    let b = batches.first().unwrap();
    let naive_phys = b
        .column_by_name("created_at")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    let ts_phys = b
        .column_by_name("created_at_tz")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();

    assert_eq!(naive_phys.value(0), expected_row1_micros);
    assert_eq!(ts_phys.value(0), expected_row1_micros);

    assert_eq!(naive_phys.value(1), expected_row2_naive_micros);
    assert_eq!(ts_phys.value(1), expected_row2_ts_micros);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_golden_blob_exact_binary_roundtrip() {
    require_alive(LiveService::Mysql);

    let payload: Vec<u8> = vec![0x00u8, 0xff, 0x01, 0x23, 0x45];
    let table_name = unique_name("golden_bin_my");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, data BLOB NOT NULL) ENGINE=InnoDB;
         INSERT INTO {table_name} VALUES (1, X'00ff012345');",
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("golden_my_blob");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: "SELECT data AS payload FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "mysql golden blob export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);

    let (schema, batches) = read_parquet_batches(&files[0]);
    assert!(matches!(
        schema.field_with_name("payload").unwrap().data_type(),
        DataType::Binary
    ));

    let col = batches[0]
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("BLOB/binary column");
    assert_eq!(col.value(0), payload.as_slice());
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_golden_fixed_binary_four_is_arrow_binary_not_utf8() {
    require_alive(LiveService::Mysql);

    // Matches `BINARY(4)` in `dev/mysql/init.sql`: protocol often reports STRING + charset 63.
    let expect: &[u8] = &[0x00, 0xff, 0x01, 0x23];
    let table_name = unique_name("golden_fixed4");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            raw_bytes BINARY(4) NOT NULL
        ) ENGINE=InnoDB;
        INSERT INTO {table_name} VALUES (1, UNHEX('00ff0123'));",
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("golden_my_fixed_binary");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: "SELECT raw_bytes FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "mysql golden BINARY(n) export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);
    assert!(
        matches!(
            schema.field_with_name("raw_bytes").unwrap().data_type(),
            DataType::Binary
        ),
        "BINARY(n) must be Arrow Binary, not Utf8 lossy STRING"
    );
    let col = batches[0]
        .column_by_name("raw_bytes")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("BINARY column");
    assert_eq!(col.value(0), expect);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_golden_uuid_varchar_utf8_expected_canonical_lower() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("golden_uid_my");
    let mut c = mysql_connect();
    c.query_drop(format!(
        r#"CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, uid VARCHAR(36) NOT NULL)
           ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
           INSERT INTO {table_name} VALUES (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011');"#,
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("golden_my_uuid");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: "SELECT uid FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "mysql golden uuid-ish char export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (_, batches) = read_parquet_batches(&files[0]);

    let col = batches[0]
        .column_by_name("uid")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("CHAR maps to Utf8");
    assert!(matches!(
        batches[0]
            .schema()
            .field_with_name("uid")
            .unwrap()
            .data_type(),
        DataType::Utf8
    ));

    assert_eq!(
        col.value(0),
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011",
        "canonical UUID text must survive mysql export unchanged"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn golden_type_matrix_full_export_parquet_roundtrip() {
    require_alive(LiveService::Postgres);

    let r1_naive_micros = matrix_naive_wall_micros("2035-08-07 09:08:07.987654");
    let r2_naive_wall = matrix_naive_wall_micros("2019-02-03 03:07:06.554433");
    let r3_micros = matrix_naive_wall_micros("2020-01-15 00:00:00.000001");
    let r4_micros = matrix_naive_wall_micros("2021-06-30 12:59:59.999999");

    let table_name = unique_name("golden_tmtx_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        r#"CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            label TEXT NOT NULL,
            amount NUMERIC(18, 2),
            fee NUMERIC(18, 6),
            created_at TIMESTAMP NOT NULL,
            created_at_tz TIMESTAMPTZ NOT NULL,
            raw_bytes BYTEA NOT NULL,
            uid UUID NOT NULL,
            attrs JSONB
        );

        INSERT INTO {table_name} (
            id, label, amount, fee, created_at, created_at_tz, raw_bytes, uid, attrs
        ) VALUES
          (1, 'payments-like', 0.10, 0.000001,
              TIMESTAMP '2035-08-07 09:08:07.987654',
              TIMESTAMPTZ '2035-08-07 09:08:07.987654Z',
              '\x00ff012345'::bytea,
              'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011'::uuid,
              '{{"tier":"gold","n":1}}'::jsonb),
          (2, 'payments-like', 0.20, 0.000002,
              TIMESTAMP '2019-02-03 03:07:06.554433',
              TIMESTAMPTZ '2019-02-03 08:07:06.554433+05',
              '\xdeadbeef'::bytea,
              'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380022'::uuid,
              '["a","b"]'::jsonb),
          (3, 'payments-like', 999999999999.99, 10.123456,
              TIMESTAMP '2020-01-15 00:00:00.000001',
              TIMESTAMPTZ '2020-01-15 00:00:00.000001+00',
              '\xcafe'::bytea,
              'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380033'::uuid,
              '{{"big":true}}'::jsonb),
          (4, 'payments-like', -100.05, -0.123456,
              TIMESTAMP '2021-06-30 12:59:59.999999',
              TIMESTAMPTZ '2021-06-30 12:59:59.999999+00',
              '\x00'::bytea,
              'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380044'::uuid,
              '{{}}'::jsonb);"#,
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    let export_name = unique_name("golden_tmtx_pg_run");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: >-
      SELECT id, label, amount, fee, created_at, created_at_tz,
             raw_bytes, uid, attrs
      FROM {table_name} ORDER BY id
    mode: full
    format: parquet
    compression: zstd
    columns:
      amount: decimal(18,2)
      fee: decimal(18,6)
    destination:
      type: local
      path: {out_dir}
"#,
        table_name = table_name,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "golden type-matrix pg export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1, "single parquet part");
    let (schema, batches) = read_parquet_batches(&files[0]);

    match schema.field_with_name("amount").unwrap().data_type() {
        DataType::Decimal128(18, 2) => {}
        dt => panic!("amount Decimal128 mismatch: {dt:?}"),
    }
    match schema.field_with_name("fee").unwrap().data_type() {
        DataType::Decimal128(18, 6) => {}
        dt => panic!("fee Decimal128 mismatch: {dt:?}"),
    }
    match schema.field_with_name("created_at").unwrap().data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => {}
        dt => panic!("created_at TIMESTAMP Arrow type: {dt:?}"),
    }
    match schema.field_with_name("created_at_tz").unwrap().data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => assert_eq!(tz.as_ref(), "UTC"),
        dt => panic!("created_at_tz: {dt:?}"),
    }
    match schema.field_with_name("raw_bytes").unwrap().data_type() {
        DataType::Binary => {}
        dt => panic!("raw_bytes BYTEA maps to Arrow Binary; got {dt:?}"),
    }
    match schema.field_with_name("attrs").unwrap().data_type() {
        DataType::Utf8 => {}
        dt => panic!("attrs: {dt:?}"),
    }

    assert_eq!(
        sum_decimal128_scaled(&batches, "amount"),
        99_999_999_990_024_i128,
    );
    assert_eq!(sum_decimal128_scaled(&batches, "fee"), 10_000_003_i128,);

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);

    let b = batches.first().unwrap();

    let created_at_naive = b
        .column_by_name("created_at")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    let tstz_phys = b
        .column_by_name("created_at_tz")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    assert_eq!(created_at_naive.value(0), r1_naive_micros);
    assert_eq!(tstz_phys.value(0), r1_naive_micros);
    assert_eq!(created_at_naive.value(1), r2_naive_wall);
    assert_eq!(tstz_phys.value(1), r2_naive_wall);
    assert_eq!(created_at_naive.value(2), r3_micros);
    assert_eq!(tstz_phys.value(2), r3_micros);
    assert_eq!(created_at_naive.value(3), r4_micros);
    assert_eq!(tstz_phys.value(3), r4_micros);

    let rb = b
        .column_by_name("raw_bytes")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(rb.value(0), &[0x00, 0xff, 0x01, 0x23, 0x45]);
    assert_eq!(rb.value(1), &[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(rb.value(2), &[0xca, 0xfe]);
    assert_eq!(rb.value(3), &[0x00]);

    let uid_arr = b
        .column_by_name("uid")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(uid_arr.value(0), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011");
    assert_eq!(uid_arr.value(3), "d0eebc99-9c0b-4ef8-bb6d-6bb9bd380044");

    let attrs = b
        .column_by_name("attrs")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for (row, expected) in [
        (0, json!({"n": 1, "tier": "gold"})),
        (1, json!(["a", "b"])),
        (2, json!({"big": true})),
        (3, json!({})),
    ] {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(attrs.value(row)).unwrap(),
            expected,
            "attrs row {}",
            row,
        );
    }
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_golden_type_matrix_full_export_parquet_roundtrip() {
    require_alive(LiveService::Mysql);

    let r1_naive_micros = matrix_naive_wall_micros("2035-08-07 09:08:07.987654");
    let r2_naive_wall = matrix_naive_wall_micros("2019-02-03 03:07:06.554433");
    let r3_micros = matrix_naive_wall_micros("2020-01-15 00:00:00.000001");
    let r4_micros = matrix_naive_wall_micros("2021-06-30 12:59:59.999999");

    let table_name = unique_name("golden_tmtx_my");
    let mut c = mysql_connect();
    c.query_drop(format!(
        r#"CREATE TABLE {table_name} (
            id BIGINT PRIMARY KEY,
            label VARCHAR(200) NOT NULL,
            amount DECIMAL(18, 2) NULL,
            fee DECIMAL(18, 6) NULL,
            created_at_dt DATETIME(6) NOT NULL,
            created_at_ts TIMESTAMP(6) NOT NULL,
            raw_bytes BINARY(4) NOT NULL,
            uid VARCHAR(36) NOT NULL,
            extras JSON NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

        INSERT INTO {table_name} (
            id, label, amount, fee, created_at_dt, created_at_ts, raw_bytes, uid, extras
        ) VALUES
          (1, 'payments-like', 0.10, 0.000001,
              '2035-08-07 09:08:07.987654',
              '2035-08-07 09:08:07.987654',
              UNHEX('00ff0123'),
              'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011',
              JSON_OBJECT('tier', 'gold', 'n', 1)),
          (2, 'payments-like', 0.20, 0.000002,
              '2019-02-03 03:07:06.554433',
              '2019-02-03 03:07:06.554433',
              UNHEX('deadbeef'),
              'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380022',
              JSON_ARRAY('a', 'b')),
          (3, 'payments-like', 999999999999.99, 10.123456,
              '2020-01-15 00:00:00.000001',
              '2020-01-15 00:00:00.000001',
              UNHEX('cafe'),
              'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380033',
              CAST('{{"big":true}}' AS JSON)),
          (4, 'payments-like', -100.05, -0.123456,
              '2021-06-30 12:59:59.999999',
              '2021-06-30 12:59:59.999999',
              UNHEX('00'),
              'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380044',
              CAST('{{}}' AS JSON));"#,
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("golden_tmtx_my_run");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: >-
      SELECT id, label, amount, fee,
             created_at_dt AS created_at,
             created_at_ts AS created_at_tz,
             raw_bytes, uid,
             extras AS attrs
      FROM {table_name} ORDER BY id
    mode: full
    format: parquet
    compression: zstd
    columns:
      amount: decimal(18,2)
      fee: decimal(18,6)
    destination:
      type: local
      path: {out_dir}
"#,
        table_name = table_name,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "golden type-matrix mysql export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);

    match schema.field_with_name("amount").unwrap().data_type() {
        DataType::Decimal128(18, 2) => {}
        dt => panic!("amount Decimal128 mismatch: {dt:?}"),
    }
    match schema.field_with_name("fee").unwrap().data_type() {
        DataType::Decimal128(18, 6) => {}
        dt => panic!("fee Decimal128 mismatch: {dt:?}"),
    }
    match schema.field_with_name("created_at").unwrap().data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => {}
        dt => panic!("created_at: {dt:?}"),
    }
    match schema.field_with_name("created_at_tz").unwrap().data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => assert_eq!(tz.as_ref(), "UTC"),
        dt => panic!("created_at_tz: {dt:?}"),
    }

    assert_eq!(
        sum_decimal128_scaled(&batches, "amount"),
        99_999_999_990_024_i128,
    );
    assert_eq!(sum_decimal128_scaled(&batches, "fee"), 10_000_003_i128,);

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);

    let b = batches.first().unwrap();

    let created_at_naive = b
        .column_by_name("created_at")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    let ts_phys = b
        .column_by_name("created_at_tz")
        .unwrap()
        .as_primitive::<TimestampMicrosecondType>();
    assert_eq!(created_at_naive.value(0), r1_naive_micros);
    assert_eq!(ts_phys.value(0), r1_naive_micros);
    assert_eq!(created_at_naive.value(1), r2_naive_wall);
    assert_eq!(ts_phys.value(1), r2_naive_wall);
    assert_eq!(created_at_naive.value(2), r3_micros);
    assert_eq!(ts_phys.value(2), r3_micros);
    assert_eq!(created_at_naive.value(3), r4_micros);
    assert_eq!(ts_phys.value(3), r4_micros);

    let rb = b
        .column_by_name("raw_bytes")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(rb.value(0), &[0x00, 0xff, 0x01, 0x23]);
    assert_eq!(rb.value(1), &[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(rb.value(2), &[0xca, 0xfe, 0x00, 0x00]);
    assert_eq!(rb.value(3), &[0x00, 0x00, 0x00, 0x00]);

    let attrs = b
        .column_by_name("attrs")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let expected_objs = vec![
        json!({"tier": "gold", "n": 1}),
        json!(["a", "b"]),
        json!({"big": true}),
        json!({}),
    ];
    for (row, exp) in expected_objs.into_iter().enumerate() {
        let got = serde_json::from_str::<serde_json::Value>(attrs.value(row)).unwrap();
        assert_eq!(got, exp, "attrs row {}", row);
    }
}

// ── Full type-matrix: broad schema-coverage goldens ──────────────────────────

/// Verifies that every Rivet-mapped Postgres type produces the correct Arrow
/// DataType in Parquet: BOOL, INT2, INT4, REAL, DATE, TIME, INTERVAL, ENUM,
/// TEXT[], INT[].  Row-level anchors confirm the boolean and int2 round-trips.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_golden_full_type_matrix_schema_coverage() {
    require_alive(LiveService::Postgres);

    let enum_type = unique_name("rivet_status");
    let table_name = unique_name("golden_full_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        r#"
        CREATE TYPE {enum_type} AS ENUM ('active', 'inactive', 'pending');
        CREATE TABLE {table_name} (
            id           BIGINT PRIMARY KEY,
            flag         BOOLEAN,
            int2_col     SMALLINT,
            int4_col     INTEGER,
            float4_col   REAL,
            date_col     DATE,
            time_col     TIME,
            interval_col INTERVAL,
            enum_col     {enum_type},
            tags         TEXT[],
            nums         INTEGER[]
        );
        INSERT INTO {table_name} VALUES
          (1, TRUE,  32767,       2147483647,  3.14::real, '2024-03-15', '14:30:00.123456',  INTERVAL '1 year 2 months 3 days', 'active',   ARRAY['alpha','beta'], ARRAY[1,2,3]),
          (2, FALSE, -32768,     -2147483648, -1.5::real,  '1970-01-01', '00:00:00',         INTERVAL '-1 year',                'inactive', ARRAY['gamma'],        ARRAY[42]),
          (3, NULL,  NULL,        0,           0.0::real,  '2000-02-29', '23:59:59.999999',  INTERVAL '0',                      NULL,       ARRAY[]::text[],       NULL);
        "#,
    ))
    .unwrap();

    struct Cleanup(String, String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {} CASCADE", self.0), &[]);
                let _ = c.execute(&format!("DROP TYPE IF EXISTS {} CASCADE", self.1), &[]);
            }
        }
    }
    let _guard = Cleanup(table_name.clone(), enum_type.clone());

    let export_name = unique_name("golden_full_pg_run");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT * FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "pg full type matrix export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);

    assert_eq!(
        *schema.field_with_name("id").unwrap().data_type(),
        DataType::Int64
    );
    assert_eq!(
        *schema.field_with_name("flag").unwrap().data_type(),
        DataType::Boolean
    );
    assert_eq!(
        *schema.field_with_name("int2_col").unwrap().data_type(),
        DataType::Int16
    );
    assert_eq!(
        *schema.field_with_name("int4_col").unwrap().data_type(),
        DataType::Int32
    );
    assert_eq!(
        *schema.field_with_name("float4_col").unwrap().data_type(),
        DataType::Float32
    );
    assert_eq!(
        *schema.field_with_name("date_col").unwrap().data_type(),
        DataType::Date32
    );
    assert_eq!(
        *schema.field_with_name("time_col").unwrap().data_type(),
        DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        *schema.field_with_name("interval_col").unwrap().data_type(),
        DataType::Utf8,
        "interval_col: Interval(MonthDayNano) is not Parquet-writable; must map to Utf8 ISO 8601"
    );
    assert_eq!(
        *schema.field_with_name("enum_col").unwrap().data_type(),
        DataType::Utf8
    );
    match schema.field_with_name("tags").unwrap().data_type() {
        DataType::List(f) => assert_eq!(f.data_type(), &DataType::Utf8, "tags inner type"),
        dt => panic!("tags: expected List<Utf8>, got {dt:?}"),
    }
    match schema.field_with_name("nums").unwrap().data_type() {
        DataType::List(f) => assert_eq!(f.data_type(), &DataType::Int32, "nums inner type"),
        dt => panic!("nums: expected List<Int32>, got {dt:?}"),
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected 3 rows");

    let b = batches.first().unwrap();

    let flags = b
        .column_by_name("flag")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(flags.value(0), "flag[0] must be true");
    assert!(!flags.value(1), "flag[1] must be false");
    assert!(flags.is_null(2), "flag[2] must be null");

    let int2s = b
        .column_by_name("int2_col")
        .unwrap()
        .as_any()
        .downcast_ref::<Int16Array>()
        .unwrap();
    assert_eq!(int2s.value(0), 32767_i16);
    assert_eq!(int2s.value(1), -32768_i16);
    assert!(int2s.is_null(2), "int2_col[2] must be null");

    // INTERVAL values must round-trip as ISO 8601 duration strings.
    let intervals = b
        .column_by_name("interval_col")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        intervals.value(0),
        "P1Y2M3D",
        "INTERVAL '1 year 2 months 3 days'"
    );
    assert_eq!(intervals.value(1), "P-1Y", "INTERVAL '-1 year'");
    assert_eq!(intervals.value(2), "PT0S", "INTERVAL '0'");
}

/// Verifies that every Rivet-mapped MySQL type produces the correct Arrow
/// DataType in Parquet: BOOLEAN/TINYINT(1), BIT(1), BIT(8), TINYINT,
/// DATE, TIME(6), YEAR, ENUM, VARBINARY, BLOB.  Row-level anchors confirm
/// the boolean and BIT byte-decoding round-trips.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_golden_full_type_matrix_schema_coverage() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("golden_full_my");
    let mut c = mysql_connect();
    c.query_drop(format!(
        r#"CREATE TABLE {table_name} (
            id            BIGINT PRIMARY KEY,
            flag          BOOLEAN,
            bit1_col      BIT(1),
            bit8_col      BIT(8),
            tiny_col      TINYINT,
            date_col      DATE,
            time_col      TIME(6),
            year_col      YEAR,
            enum_col      ENUM('a', 'b', 'c'),
            varbinary_col VARBINARY(4),
            blob_col      BLOB
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"#,
    ))
    .unwrap();
    c.query_drop(format!(
        r#"INSERT INTO {table_name}
            (id, flag, bit1_col, bit8_col, tiny_col, date_col, time_col, year_col, enum_col, varbinary_col, blob_col)
           VALUES
            (1, TRUE,  b'1', b'10101010',  127, '2024-03-15', '14:30:00.123456', 2024, 'b', 0xDEADBEEF, 0x0102030405),
            (2, FALSE, b'0', b'00000001', -128, '1970-01-01', '00:00:00.000000', 2000, 'a', 0x00000000, 0xCAFE),
            (3, NULL,  NULL, NULL,           0, '2000-02-29', '23:59:59.999999', NULL, NULL, NULL,       NULL)"#,
    ))
    .unwrap();
    let _guard = MysqlCleanup(table_name.clone());

    let export_name = unique_name("golden_full_my_run");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: "SELECT * FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "mysql full type matrix export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);

    assert_eq!(
        *schema.field_with_name("id").unwrap().data_type(),
        DataType::Int64
    );
    assert_eq!(
        *schema.field_with_name("flag").unwrap().data_type(),
        DataType::Boolean,
        "BOOLEAN/TINYINT(1) must map to Boolean"
    );
    assert_eq!(
        *schema.field_with_name("bit1_col").unwrap().data_type(),
        DataType::Boolean,
        "BIT(1) must map to Boolean"
    );
    assert_eq!(
        *schema.field_with_name("bit8_col").unwrap().data_type(),
        DataType::Int64,
        "BIT(8) must map to Int64"
    );
    assert_eq!(
        *schema.field_with_name("tiny_col").unwrap().data_type(),
        DataType::Int16,
        "TINYINT must map to Int16"
    );
    assert_eq!(
        *schema.field_with_name("date_col").unwrap().data_type(),
        DataType::Date32
    );
    assert_eq!(
        *schema.field_with_name("time_col").unwrap().data_type(),
        DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        *schema.field_with_name("year_col").unwrap().data_type(),
        DataType::Int16,
        "YEAR must map to Int16"
    );
    assert_eq!(
        *schema.field_with_name("enum_col").unwrap().data_type(),
        DataType::Utf8,
        "ENUM must map to Utf8"
    );
    assert_eq!(
        *schema.field_with_name("varbinary_col").unwrap().data_type(),
        DataType::Binary,
        "VARBINARY must map to Binary"
    );
    assert_eq!(
        *schema.field_with_name("blob_col").unwrap().data_type(),
        DataType::Binary,
        "BLOB must map to Binary"
    );

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected 3 rows");

    let b = batches.first().unwrap();

    let flags = b
        .column_by_name("flag")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(flags.value(0), "flag[0] must be true");
    assert!(!flags.value(1), "flag[1] must be false");
    assert!(flags.is_null(2), "flag[2] must be null");

    let bit1s = b
        .column_by_name("bit1_col")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(bit1s.value(0), "bit1_col[0]=b'1' must be true");
    assert!(!bit1s.value(1), "bit1_col[1]=b'0' must be false");

    let bit8s = b
        .column_by_name("bit8_col")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        bit8s.value(0),
        0b10101010_i64,
        "bit8_col[0]=b'10101010'=170"
    );
    assert_eq!(bit8s.value(1), 1_i64, "bit8_col[1]=b'00000001'=1");

    let tinys = b
        .column_by_name("tiny_col")
        .unwrap()
        .as_any()
        .downcast_ref::<Int16Array>()
        .unwrap();
    assert_eq!(tinys.value(0), 127_i16);
    assert_eq!(tinys.value(1), -128_i16);
}
