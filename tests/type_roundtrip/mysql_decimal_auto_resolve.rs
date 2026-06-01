//! Regression: MySQL DECIMAL columns resolve their precision/scale from the
//! wire column definition — no `columns:` override needed — matching
//! PostgreSQL's catalog-hint path.
//!
//! Before, [src/source/mysql/arrow_convert.rs] mapped DECIMAL/NEWDECIMAL to
//! `Unsupported` (a stale comment claimed the mysql crate didn't expose
//! precision/scale on `Column`). In fact `column_length()` is the display
//! width and `decimals()` is the scale, so `derive_decimal_ps` recovers exact
//! `(p, s)`. Verified live against DECIMAL(18,2)/(20,6)/(10,2)/(8,0)/UNSIGNED.

use crate::common::*;

use super::helpers::{MysqlCleanup, read_parquet_batches, setup_mysql_matrix_table};

use arrow::datatypes::DataType;

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_decimal_resolves_precision_scale_without_override() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("type_rt_my_decauto");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("type_rt_my_decauto");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: >-
      SELECT id, amount, fee, price FROM {table_name} ORDER BY id
    mode: full
    format: parquet
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

    // Precision/scale recovered from the wire metadata, with no override.
    assert_eq!(
        *schema.field_with_name("amount").unwrap().data_type(),
        DataType::Decimal128(18, 2),
        "DECIMAL(18,2) must resolve without an override"
    );
    assert_eq!(
        *schema.field_with_name("fee").unwrap().data_type(),
        DataType::Decimal128(20, 6)
    );
    assert_eq!(
        *schema.field_with_name("price").unwrap().data_type(),
        DataType::Decimal128(10, 2)
    );

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "expected canonical 4-row matrix");
}
