//! Regression: MySQL `BIGINT UNSIGNED` with a `decimal(p,s)` column override
//! must materialize the source values, not silently NULL them.
//!
//! Pre-fix, [src/source/mysql/arrow_convert.rs] had a `mysql_decimal_to_*`
//! helper that only handled `Value::Bytes` (text-protocol DECIMAL); when the
//! source column came back as `Value::UInt(u64)` the row fell into a default
//! `_ => append_null()` arm. The user-facing symptom: a YAML override like
//! `c_bigint_u: decimal(20,0)` produced an all-NULL Parquet column, with no
//! error or warning. This test pins the fix and is the canonical workaround
//! for Snowflake / BigQuery Parquet readers that reject raw UINT64 > 2^63-1
//! (see `docs/snowflake-load.md`).
//!
//! Discovered during the v0.7.8 type-roundtrip → Snowflake landing exercise.
//! Auto-memory: `rivet-mysql-uint64-decimal-override-bug`.

use crate::common::*;

use super::helpers::{MysqlCleanup, read_parquet_batches, setup_mysql_matrix_table};

use arrow::array::{Array, Decimal128Array};
use arrow::datatypes::DataType;

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_uint64_with_decimal_override_preserves_values() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("type_rt_my_u64dec");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("type_rt_my_u64dec");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: >-
      SELECT id, c_bigint_u FROM {table_name} ORDER BY id
    mode: full
    format: parquet
    compression: zstd
    columns:
      c_bigint_u: decimal(20,0)
    destination:
      type: local
      path: {path}
"#,
        path = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "rivet failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);
    assert_eq!(
        *schema.field_with_name("c_bigint_u").unwrap().data_type(),
        DataType::Decimal128(20, 0),
        "override must materialize as Decimal128(20,0)"
    );

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "expected canonical 4-row matrix");

    // Canonical seed values for `c_bigint_u` (from
    // `tests/type_roundtrip/fixtures/mysql_seed.sql`), in id order:
    //   1 -> 18446744073709551615  (u64::MAX)
    //   2 ->  9223372036854775808  (u64 = i64::MAX + 1; the regression case)
    //   3 ->                    1
    //   4 ->                    0
    let expected: [i128; 4] = [
        18_446_744_073_709_551_615_i128,
        9_223_372_036_854_775_808_i128,
        1,
        0,
    ];

    let mut got: Vec<i128> = Vec::with_capacity(4);
    for batch in &batches {
        let col = batch
            .column_by_name("c_bigint_u")
            .expect("c_bigint_u column");
        let arr = col
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");
        for i in 0..arr.len() {
            assert!(
                !arr.is_null(i),
                "row {i}: c_bigint_u must not be NULL — that was the bug"
            );
            got.push(arr.value(i));
        }
    }
    assert_eq!(got, expected.to_vec());
}
