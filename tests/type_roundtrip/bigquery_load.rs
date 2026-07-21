//! Live Parquet validation via BigQuery (ADR-0014 §4 + §6 — BQ resolver row).
//!
//! Same shape as [`duckdb_load`] / [`clickhouse_load`] / [`pyarrow_load`] but
//! pushes the canonical PG / MySQL type matrices through a real cloud target.
//! The Parquet is the contract; BigQuery is the oracle.
//!
//! Catches what the local validators do not:
//!
//! * Warehouse-specific autoload behavior (`UINT64` overflow, `LogicalType::Json`
//!   *not* promoted to native JSON, `LogicalType::Uuid` landing as BYTES,
//!   List<X> shape under `--parquet_enable_list_inference`).
//! * That the fidelity table operators read in
//!   [docs/recipes/snowflake-load.md] still matches reality.
//! * The `c_bigint_u: decimal(20,0)` workaround keeps working — without it
//!   BigQuery's Parquet reader also rejects `UINT64 > 2^63-1`.
//!
//! Gating: requires `BIGQUERY_TEST_PROJECT` env + `bq` CLI on PATH. Tests
//! skip cleanly otherwise so CI without GCP credentials stays green.
//!
//! Configurable via env:
//!   BIGQUERY_TEST_PROJECT  — required, e.g. `rivet-data-tool`
//!   BIGQUERY_TEST_DATASET  — optional, default `rivet_type_lab`
//!   BIGQUERY_TEST_LOCATION — optional, default `EU`

use crate::common::*;

use super::helpers::{
    MYSQL_MATRIX_COLUMNS, MssqlCleanup, MysqlCleanup, PgCleanup, run_mssql_matrix_export,
    run_pg_matrix_export, setup_mssql_matrix_table, setup_mysql_matrix_table,
    setup_pg_matrix_table,
};

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

// ─── Gating + helpers ──────────────────────────────────────────────────────

struct BqConfig {
    project: String,
    dataset: String,
    location: String,
}

/// Read BigQuery configuration from env. Returns `None` if any required piece
/// is missing — tests skip in that case rather than fail.
fn bq_config() -> Option<BqConfig> {
    let project = std::env::var("BIGQUERY_TEST_PROJECT").ok()?;
    // `bq --version` is the cheapest reachable probe.
    if Command::new("bq").arg("--version").output().is_err() {
        eprintln!("bigquery_load: skipping — `bq` CLI not on PATH");
        return None;
    }
    Some(BqConfig {
        project,
        dataset: std::env::var("BIGQUERY_TEST_DATASET")
            .unwrap_or_else(|_| "rivet_type_lab".to_string()),
        location: std::env::var("BIGQUERY_TEST_LOCATION").unwrap_or_else(|_| "EU".to_string()),
    })
}

/// Drop the BQ table at end of test even on panic.
struct BqTableGuard<'a> {
    cfg: &'a BqConfig,
    table: String,
}

impl Drop for BqTableGuard<'_> {
    fn drop(&mut self) {
        let _ = Command::new("bq")
            .arg(format!("--project_id={}", self.cfg.project))
            .arg("rm")
            .arg("-f")
            .arg("-t")
            .arg(format!("{}.{}", self.cfg.dataset, self.table))
            .output();
    }
}

impl BqConfig {
    fn load_parquet(&self, table: &str, parquet_path: &Path) {
        let out = Command::new("bq")
            .arg(format!("--project_id={}", self.project))
            .arg("load")
            .arg(format!("--location={}", self.location))
            .arg("--replace")
            .arg("--source_format=PARQUET")
            .arg("--parquet_enable_list_inference")
            .arg(format!("{}.{}", self.dataset, table))
            .arg(parquet_path)
            .output()
            .expect("`bq load` must run");
        assert!(
            out.status.success(),
            "bq load failed:\nstderr: {}\nstdout: {}",
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout),
        );
    }

    /// Return `(column_name -> "TYPE"[, " REPEATED"])` for one BigQuery table.
    fn schema(&self, table: &str) -> HashMap<String, String> {
        let out = Command::new("bq")
            .arg(format!("--project_id={}", self.project))
            .arg("show")
            .arg("--schema")
            .arg("--format=prettyjson")
            .arg(format!("{}.{}", self.dataset, table))
            .output()
            .expect("`bq show --schema`");
        assert!(
            out.status.success(),
            "bq show failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let v: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("bq show schema is valid JSON");
        v.as_array()
            .expect("bq show returns array of fields")
            .iter()
            .map(|f| {
                let name = f["name"].as_str().unwrap().to_string();
                let ty = f["type"].as_str().unwrap().to_string();
                let mode = f.get("mode").and_then(|m| m.as_str()).unwrap_or("");
                let value = if mode == "REPEATED" {
                    format!("REPEATED {ty}")
                } else {
                    ty
                };
                (name, value)
            })
            .collect()
    }

    fn query_rows(&self, sql: &str) -> serde_json::Value {
        let out = Command::new("bq")
            .arg(format!("--project_id={}", self.project))
            .arg("query")
            .arg(format!("--location={}", self.location))
            .arg("--use_legacy_sql=false")
            .arg("--format=prettyjson")
            .arg(sql)
            .output()
            .expect("`bq query`");
        assert!(
            out.status.success(),
            "bq query failed:\nsql: {sql}\nstderr: {}\nstdout: {}",
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout),
        );
        serde_json::from_slice(&out.stdout).expect("bq query returns valid JSON")
    }
}

// ─── PostgreSQL matrix → Parquet → BigQuery ────────────────────────────────

#[test]
#[ignore = "live: requires BIGQUERY_TEST_PROJECT env + bq CLI"]
fn bigquery_validates_postgres_type_matrix_parquet() {
    require_alive(LiveService::Postgres);
    let Some(cfg) = bq_config() else {
        eprintln!("bigquery_load: skipping (BIGQUERY_TEST_PROJECT not set)");
        return;
    };

    let pg_table = unique_name("bq_pg");
    let enum_type = setup_pg_matrix_table(&pg_table);
    let _pg_guard = PgCleanup {
        table: pg_table.clone(),
        enum_type,
    };

    let out_dir = tempfile::tempdir().unwrap();
    run_pg_matrix_export(&pg_table, "parquet", out_dir.path());
    let parquet = files_with_extension(out_dir.path(), "parquet")
        .into_iter()
        .next()
        .expect("one parquet part");

    let bq_table = unique_name("bq_pg");
    let _table_guard = BqTableGuard {
        cfg: &cfg,
        table: bq_table.clone(),
    };
    cfg.load_parquet(&bq_table, &parquet);

    // 1) Autoload schema — the columns ADR-0014 cares about. JSON / UUID land
    //    as BYTES (not native JSON / UUID — Snowflake autoload makes the same
    //    choice); binary / time / timestamp survive natively. List<X> arrives
    //    as REPEATED with --parquet_enable_list_inference (the field is
    //    `REPEATED RECORD` with an inner `item` scalar; we assert the mode).
    let schema = cfg.schema(&bq_table);
    let expected: &[(&str, &str)] = &[
        ("id", "INTEGER"),
        ("c_smallint", "INTEGER"),
        ("c_integer", "INTEGER"),
        ("c_bigint", "INTEGER"),
        ("amount", "NUMERIC"),
        ("fee", "NUMERIC"),
        ("price", "NUMERIC"),
        ("c_real", "FLOAT"),
        ("c_double", "FLOAT"),
        ("c_date", "DATE"),
        ("c_time", "TIME"),
        ("created_at", "TIMESTAMP"),
        ("created_at_tz", "TIMESTAMP"),
        ("label", "STRING"),
        ("raw_bytes", "BYTES"),
        ("uid", "BYTES"),
        ("attrs", "BYTES"),
        ("attrs_json", "BYTES"),
        ("c_bool", "BOOLEAN"),
        ("interval_col", "STRING"),
        ("enum_col", "STRING"),
        ("large_text", "STRING"),
    ];
    for (col, want) in expected {
        let got = schema
            .get(*col)
            .unwrap_or_else(|| panic!("BigQuery did not see column `{col}`; saw: {schema:?}"));
        assert_eq!(
            got, want,
            "BigQuery autoload type for `{col}`: expected {want}, got {got}",
        );
    }
    // Lists must arrive as REPEATED — exact inner type is BigQuery-version-dependent
    // (`REPEATED RECORD` with `item` subfield is what `--parquet_enable_list_inference`
    // currently produces from Arrow's `list / item` Parquet structure).
    assert!(
        schema
            .get("tags")
            .is_some_and(|t| t.starts_with("REPEATED")),
        "tags must be REPEATED, got {:?}",
        schema.get("tags"),
    );
    assert!(
        schema
            .get("nums")
            .is_some_and(|t| t.starts_with("REPEATED")),
        "nums must be REPEATED, got {:?}",
        schema.get("nums"),
    );

    // 2) Values from row 1: microsecond timestamps, JSON byte content, UUID hex,
    //    binary hex. tz-aware and naive variants must both reflect the same
    //    UTC wall clock since the seed wrote them at offset 0.
    let row1 = cfg.query_rows(&format!(
        "SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', created_at)    AS naive,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', created_at_tz) AS utc,
                TO_HEX(raw_bytes)                                      AS raw_hex,
                TO_HEX(uid)                                            AS uid_hex,
                SAFE_CONVERT_BYTES_TO_STRING(attrs)                    AS attrs_str
         FROM `{}.{}.{}` WHERE id = 1",
        cfg.project, cfg.dataset, bq_table,
    ));
    let r = &row1[0];
    assert_eq!(r["naive"], "2035-08-07 09:08:07.987654");
    assert_eq!(r["utc"], "2035-08-07 09:08:07.987654");
    assert_eq!(r["raw_hex"], "00ff012345");
    assert_eq!(r["uid_hex"], "a0eebc999c0b4ef8bb6d6bb9bd380011");
    let attrs: serde_json::Value =
        serde_json::from_str(r["attrs_str"].as_str().unwrap()).expect("attrs is valid JSON");
    assert_eq!(attrs["tier"], "gold");
    assert_eq!(attrs["n"], 1);

    // 3) Decimal sums (same constants the in-process roundtrip uses).
    let sums = cfg.query_rows(&format!(
        "SELECT CAST(SUM(amount * 100) AS STRING) AS s_amount,
                CAST(SUM(fee   * 1000000) AS STRING) AS s_fee
         FROM `{}.{}.{}`",
        cfg.project, cfg.dataset, bq_table,
    ));
    assert_eq!(sums[0]["s_amount"], "99999999990024");
    assert_eq!(sums[0]["s_fee"], "10000003");

    // 4) Lists, JSON validity, null propagation across all rows.
    let basics = cfg.query_rows(&format!(
        "SELECT COUNT(*)                                              AS n,
                SUM(ARRAY_LENGTH(tags))                               AS tag_total,
                SUM(ARRAY_LENGTH(nums))                               AS num_total,
                COUNTIF(note_all_null IS NULL)                        AS null_count,
                COUNTIF(SAFE.PARSE_JSON(
                    SAFE_CONVERT_BYTES_TO_STRING(attrs)) IS NOT NULL) AS attrs_json_ok
         FROM `{}.{}.{}`",
        cfg.project, cfg.dataset, bq_table,
    ));
    let b = &basics[0];
    assert_eq!(b["n"], "4", "row count");
    assert_eq!(b["tag_total"], "4");
    assert_eq!(b["num_total"], "8");
    assert_eq!(b["null_count"], "4", "note_all_null IS NULL for every row");
    assert_eq!(
        b["attrs_json_ok"], "4",
        "every attrs payload must round-trip through PARSE_JSON"
    );
}

// ─── MySQL matrix → Parquet → BigQuery ─────────────────────────────────────

#[test]
#[ignore = "live: requires BIGQUERY_TEST_PROJECT env + bq CLI"]
fn bigquery_validates_mysql_type_matrix_parquet() {
    require_alive(LiveService::Mysql);
    let Some(cfg) = bq_config() else {
        eprintln!("bigquery_load: skipping (BIGQUERY_TEST_PROJECT not set)");
        return;
    };

    let my_table = unique_name("bq_my");
    setup_mysql_matrix_table(&my_table);
    let _my_guard = MysqlCleanup(my_table.clone());

    // Custom export with `c_bigint_u: decimal(20,0)` override on top of the
    // canonical amount/fee/price block. Without it BigQuery (and Snowflake)
    // reject the raw `UINT64 > 2^63-1` payload on row 1 with
    //   "Value 18446744073709551615 outside range for column".
    // This is the documented operator workaround — see
    // docs/recipes/snowflake-load.md.
    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("bq_my");
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
      FROM {my_table} ORDER BY id
    mode: full
    format: parquet
    compression: zstd
    columns:
      amount: decimal(18,2)
      fee: decimal(20,6)
      price: decimal(10,2)
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
        "rivet export failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let parquet = files_with_extension(out_dir.path(), "parquet")
        .into_iter()
        .next()
        .expect("one parquet part");

    let bq_table = unique_name("bq_my");
    let _table_guard = BqTableGuard {
        cfg: &cfg,
        table: bq_table.clone(),
    };
    cfg.load_parquet(&bq_table, &parquet);

    // 1) UNSIGNED widening + native time / timestamp / binary. `c_bigint_u`
    //    arrives as NUMERIC because we exported with `c_bigint_u: decimal(20,0)`
    //    (raw `UINT64 > i64::MAX` overflows BigQuery's Parquet reader otherwise).
    let schema = cfg.schema(&bq_table);
    let expected: &[(&str, &str)] = &[
        ("id", "INTEGER"),
        ("c_tinyint", "INTEGER"),
        ("c_tinyint_u", "INTEGER"),
        ("c_bool", "BOOLEAN"),
        ("c_boolean", "BOOLEAN"),
        ("c_smallint", "INTEGER"),
        ("c_smallint_u", "INTEGER"),
        ("c_int", "INTEGER"),
        ("c_int_u", "INTEGER"),
        ("c_bigint", "INTEGER"),
        ("c_bigint_u", "NUMERIC"),
        ("amount", "NUMERIC"),
        ("fee", "NUMERIC"),
        ("price", "NUMERIC"),
        ("c_float", "FLOAT"),
        ("c_double", "FLOAT"),
        ("c_date", "DATE"),
        ("c_time", "TIME"),
        ("created_at_dt", "TIMESTAMP"),
        ("created_at_ts", "TIMESTAMP"),
        ("label", "STRING"),
        ("raw_bytes", "BYTES"),
        ("var_bytes", "BYTES"),
        ("blob_bytes", "BYTES"),
        ("uid", "STRING"),
        ("extras", "BYTES"),
        ("enum_col", "STRING"),
        ("set_col", "STRING"),
        ("year_col", "INTEGER"),
        ("c_bit1", "BOOLEAN"),
        ("c_bit8", "INTEGER"),
    ];
    for (col, want) in expected {
        let got = schema
            .get(*col)
            .unwrap_or_else(|| panic!("BigQuery did not see column `{col}`; saw: {schema:?}"));
        assert_eq!(
            got, want,
            "BigQuery autoload type for `{col}`: expected {want}, got {got}",
        );
    }

    // 2) Multi-byte UTF-8 survives byte-for-byte. id=4 seed is
    //    `unicode: 日本語 🚀` which is the UTF-8 byte sequence below.
    let utf8 = cfg.query_rows(&format!(
        "SELECT TO_HEX(SAFE_CAST(note_nullable AS BYTES)) AS h
         FROM `{}.{}.{}` WHERE id = 4",
        cfg.project, cfg.dataset, bq_table,
    ));
    assert_eq!(
        utf8[0]["h"], "756e69636f64653a20e697a5e69cace8aa9e20f09f9a80",
        "Unicode 日本語 🚀 must round-trip as canonical UTF-8 bytes",
    );

    // 3) Microsecond precision on TIME and TIMESTAMP for the id=1 row.
    let r1 = cfg.query_rows(&format!(
        "SELECT FORMAT_TIME('%H:%M:%E6S', c_time)                       AS t,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', created_at_dt)  AS dt,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', created_at_ts)  AS ts,
                TO_HEX(raw_bytes)                                       AS raw_hex,
                TO_HEX(blob_bytes)                                      AS blob_hex,
                SAFE_CONVERT_BYTES_TO_STRING(extras)                    AS extras_str
         FROM `{}.{}.{}` WHERE id = 1",
        cfg.project, cfg.dataset, bq_table,
    ));
    let r = &r1[0];
    assert_eq!(r["t"], "09:08:07.987654");
    assert_eq!(r["dt"], "2035-08-07 09:08:07.987654");
    assert_eq!(r["ts"], "2035-08-07 09:08:07.987654");
    assert_eq!(r["raw_hex"], "00ff0123");
    assert_eq!(r["blob_hex"], "cafebabe");
    let extras: serde_json::Value =
        serde_json::from_str(r["extras_str"].as_str().unwrap()).expect("extras is JSON");
    assert_eq!(extras["tier"], "gold");
    assert_eq!(extras["n"], 1);

    // 4) Decimal sums.
    let sums = cfg.query_rows(&format!(
        "SELECT CAST(SUM(amount * 100)     AS STRING) AS s_amount,
                CAST(SUM(fee   * 1000000)  AS STRING) AS s_fee
         FROM `{}.{}.{}`",
        cfg.project, cfg.dataset, bq_table,
    ));
    assert_eq!(sums[0]["s_amount"], "99999999990024");
    assert_eq!(sums[0]["s_fee"], "10000003");
}

// ─── SQL Server matrix → Parquet → BigQuery ────────────────────────────────

#[test]
#[ignore = "live: requires BIGQUERY_TEST_PROJECT env + bq CLI"]
fn bigquery_validates_mssql_type_matrix_parquet() {
    require_alive(LiveService::Mssql);
    let Some(cfg) = bq_config() else {
        eprintln!("bigquery_load: skipping (BIGQUERY_TEST_PROJECT not set)");
        return;
    };

    let ms_table = unique_name("bq_ms");
    setup_mssql_matrix_table(&ms_table);
    let _ms_guard = MssqlCleanup(ms_table.clone());

    // `run_mssql_matrix_export` applies the canonical decimal overrides
    // (amount/fee/price) and `tls.accept_invalid_certs` for the dev container.
    let out_dir = tempfile::tempdir().unwrap();
    run_mssql_matrix_export(&ms_table, "parquet", out_dir.path());
    let parquet = files_with_extension(out_dir.path(), "parquet")
        .into_iter()
        .next()
        .expect("one parquet part");

    let bq_table = unique_name("bq_ms");
    let _table_guard = BqTableGuard {
        cfg: &cfg,
        table: bq_table.clone(),
    };
    cfg.load_parquet(&bq_table, &parquet);

    // 1) Autoload schema. int family widens to INTEGER, bit → BOOLEAN,
    //    decimal/money → NUMERIC, real/float → FLOAT, datetime2 → TIMESTAMP,
    //    uniqueidentifier (FixedSizeBinary(16) + LogicalType::Uuid) lands as
    //    BYTES (same as PG `uuid`; Snowflake autoload makes the same choice).
    let schema = cfg.schema(&bq_table);
    let expected: &[(&str, &str)] = &[
        ("id", "INTEGER"),
        ("c_smallint", "INTEGER"),
        ("c_int", "INTEGER"),
        ("c_bigint", "INTEGER"),
        ("c_tinyint", "INTEGER"),
        ("c_bit", "BOOLEAN"),
        ("amount", "NUMERIC"),
        ("fee", "NUMERIC"),
        ("price", "NUMERIC"),
        ("c_real", "FLOAT"),
        ("c_float", "FLOAT"),
        ("c_date", "DATE"),
        ("c_time", "TIME"),
        ("created_at", "TIMESTAMP"),
        // datetimeoffset → Timestamp(µs, UTC) → BigQuery TIMESTAMP (a UTC instant,
        // same as datetime2 here; BQ has no separate tz-aware type).
        ("created_at_tz", "TIMESTAMP"),
        ("label", "STRING"),
        ("c_varchar", "STRING"),
        ("c_char", "STRING"),
        ("raw_bytes", "BYTES"),
        ("uid", "BYTES"),
        ("c_nvarchar", "STRING"),
        ("note_nullable", "STRING"),
        ("note_all_null", "STRING"),
    ];
    for (col, want) in expected {
        let got = schema
            .get(*col)
            .unwrap_or_else(|| panic!("BigQuery did not see column `{col}`; saw: {schema:?}"));
        assert_eq!(
            got, want,
            "BigQuery autoload type for `{col}`: expected {want}, got {got}",
        );
    }

    // 2) Row 1 values: microsecond TIME / TIMESTAMP, varbinary + uniqueidentifier
    //    as canonical-order bytes, and Unicode nvarchar byte-exact.
    let r1 = cfg.query_rows(&format!(
        "SELECT FORMAT_TIME('%H:%M:%E6S', c_time)                      AS t,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', created_at)    AS ts,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', created_at_tz) AS ts_tz,
                TO_HEX(raw_bytes)                                      AS raw_hex,
                TO_HEX(uid)                                            AS uid_hex,
                c_nvarchar                                            AS nv
         FROM `{}.{}.{}` WHERE id = 1",
        cfg.project, cfg.dataset, bq_table,
    ));
    let r = &r1[0];
    assert_eq!(r["t"], "13:45:30.123456");
    assert_eq!(r["ts"], "2026-01-15 13:45:30.123456");
    // datetimeoffset id=1 carried +05:30; FORMAT_TIMESTAMP renders in UTC, so the
    // 13:45:30 +05:30 wall clock reads back as the 08:15:30 UTC instant.
    assert_eq!(r["ts_tz"], "2026-01-15 08:15:30.123456");
    assert_eq!(r["raw_hex"], "00112233445566ff");
    assert_eq!(r["uid_hex"], "6f9619ff8b86d011b42d00c04fc964ff");
    assert_eq!(r["nv"], "héllo wörld");

    // 3) Aggregates: 3 rows, signed bigint sums to zero (+9e9, -9e9, 0),
    //    decimal scale preserved (1234.56 - 0.01 + 0.00 = 1234.55), every
    //    note_all_null is NULL.
    let agg = cfg.query_rows(&format!(
        "SELECT COUNT(*)                            AS n,
                CAST(SUM(c_bigint) AS STRING)       AS s_bigint,
                CAST(SUM(amount * 100) AS STRING)   AS s_amount,
                COUNTIF(note_all_null IS NULL)      AS null_count
         FROM `{}.{}.{}`",
        cfg.project, cfg.dataset, bq_table,
    ));
    let a = &agg[0];
    assert_eq!(a["n"], "3");
    assert_eq!(a["s_bigint"], "0");
    assert_eq!(a["s_amount"], "123455");
    assert_eq!(a["null_count"], "3", "note_all_null IS NULL for every row");
}
