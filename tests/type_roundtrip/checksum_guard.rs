//! Permanent regression guard for the value checksum (Form A).
//!
//! Each engine's FULL type matrix — exported through rivet with
//! `RIVET_VALUE_CHECKSUM=1` — must produce zero checksum mismatch. The matrix
//! carries the engine-specific types content_items lacks (unsigned widths, BIT,
//! ENUM/SET/YEAR, datetime2/datetimeoffset, uuid, json, decimals), so a side-A
//! dispatch arm that drifts from `build_array` (a missing sub-case, e.g. the PG
//! enum gap this test caught) surfaces here as a loud mismatch — instead of
//! silently shipping until a user enables the checksum on such a table.

use super::helpers::{
    MATRIX_COLUMN_OVERRIDES_YAML, MSSQL_MATRIX_COLUMNS, MYSQL_MATRIX_COLUMNS, MssqlCleanup,
    MysqlCleanup, PG_MATRIX_COLUMNS, PgCleanup, setup_mssql_matrix_table, setup_mysql_matrix_table,
    setup_pg_matrix_table,
};
use crate::common::{
    MSSQL_URL, MYSQL_URL, POSTGRES_URL, run_rivet_export_with_env, unique_name, write_config,
};

const CHECKSUM_ON: &[(&str, &str)] = &[("RIVET_VALUE_CHECKSUM", "1")];

fn assert_mismatch_free(out: std::process::Output, engine: &str) {
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "{engine} type-matrix export with RIVET_VALUE_CHECKSUM=1 failed \
         (a 'checksum mismatch' here = side A drifted from build_array):\n{err}"
    );
    assert!(
        !err.contains("checksum mismatch"),
        "{engine} type-matrix produced a value-checksum mismatch:\n{err}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_type_matrix_value_checksum_guard() {
    let table = unique_name("ck_guard_pg");
    let enum_type = setup_pg_matrix_table(&table);
    let _g = PgCleanup {
        table: table.clone(),
        enum_type,
    };
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: ck_guard
    query: >-
      SELECT {PG_MATRIX_COLUMNS} FROM {table} ORDER BY id
    mode: full
    format: parquet
    compression: zstd
{MATRIX_COLUMN_OVERRIDES_YAML}
    destination:
      type: local
      path: {path}
"#,
        path = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    assert_mismatch_free(
        run_rivet_export_with_env(&cfg, "ck_guard", CHECKSUM_ON),
        "postgres",
    );
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_type_matrix_value_checksum_guard() {
    let table = unique_name("ck_guard_my");
    setup_mysql_matrix_table(&table);
    let _g = MysqlCleanup(table.clone());
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: ck_guard
    query: >-
      SELECT {MYSQL_MATRIX_COLUMNS} FROM {table} ORDER BY id
    mode: full
    format: parquet
    compression: zstd
{MATRIX_COLUMN_OVERRIDES_YAML}
    destination:
      type: local
      path: {path}
"#,
        path = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    assert_mismatch_free(
        run_rivet_export_with_env(&cfg, "ck_guard", CHECKSUM_ON),
        "mysql",
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_type_matrix_value_checksum_guard() {
    let table = unique_name("ck_guard_ms");
    setup_mssql_matrix_table(&table);
    let _g = MssqlCleanup(table.clone());
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
exports:
  - name: ck_guard
    query: >-
      SELECT {MSSQL_MATRIX_COLUMNS} FROM {table} ORDER BY id
    mode: full
    format: parquet
    compression: zstd
{MATRIX_COLUMN_OVERRIDES_YAML}
    destination:
      type: local
      path: {path}
"#,
        path = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    assert_mismatch_free(
        run_rivet_export_with_env(&cfg, "ck_guard", CHECKSUM_ON),
        "mssql",
    );
}
