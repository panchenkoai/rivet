//! MySQL type edge cases — parity with `pg_edge_cases.rs`. Each block is a
//! self-contained MySQL table that exercises one boundary, verified through
//! DuckDB (and pyarrow for Decimal256).
//!
//! Categories:
//!   * Decimal: `DECIMAL(38,0)` (Decimal128 max), `DECIMAL(50,10)` (Decimal256,
//!     50 significant digits — past the i128 ceiling, exercising the i256
//!     parse) — resolved **without an override** (MySQL carries precision/scale
//!     on the wire, unlike PostgreSQL's catalog-only path; this is the parity
//!     test).
//!   * Datetime: pre-1970 + far-future `DATETIME` (naive), `TIMESTAMP` (UTC).
//!   * JSON: deep nesting, unicode keys, i64-edge numbers, mixed-type arrays.
//!   * String: empty, whitespace, 100 KB single cell.
//!   * MySQL natives: `BIT(8)`, `YEAR` bounds, `SET`, `TINYINT(1)`, all-NULL.

use mysql::prelude::Queryable;

use crate::common::*;

/// Bring up a temporary MySQL table from inline DDL, run a Rivet export
/// (parquet, zstd) into a shared DuckDB workdir, return the container-side glob
/// and the raw export `Output` (callers decide success vs expected failure).
fn mysql_edge_export(
    setup_sql: &str,
    query: &str,
    label: &str,
    overrides_yaml: &str,
) -> (String, std::process::Output) {
    let table = unique_name(label);
    let pool = mysql::Pool::new(MYSQL_URL).expect("mysql pool");
    let mut conn = pool.get_conn().expect("mysql conn");
    conn.query_drop(format!("DROP TABLE IF EXISTS {table}")).unwrap();
    for stmt in setup_sql.replace("{table_name}", &table).split(';') {
        let s = stmt.trim();
        if !s.is_empty() {
            conn.query_drop(s)
                .unwrap_or_else(|e| panic!("setup `{label}` failed on `{s}`: {e}"));
        }
    }

    let (host_dir, container_dir) = duckdb_shared_workdir(&format!("{label}_out"));
    let cfg_dir = tempfile::tempdir().unwrap();
    let export_name = format!("{label}_export");
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: "{}"
    mode: full
    format: parquet
    compression: zstd
{overrides_yaml}
    destination:
      type: local
      path: {out_dir}
"#,
        query.replace("{table_name}", &table),
        out_dir = host_dir.display(),
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg_path, &export_name);
    let _ = conn.query_drop(format!("DROP TABLE IF EXISTS {table}"));
    (format!("{container_dir}/*.parquet"), out)
}

fn mysql_edge_export_ok(setup_sql: &str, query: &str, label: &str, overrides_yaml: &str) -> String {
    let (glob, out) = mysql_edge_export(setup_sql, query, label, overrides_yaml);
    assert!(
        out.status.success(),
        "edge-case export `{label}`: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    glob
}

// ─── Decimal boundaries — resolved WITHOUT an override (the parity point) ────

#[test]
#[ignore = "live: requires docker compose mysql + duckdb"]
fn mysql_edge_decimal_boundaries_resolve_without_override() {
    let glob = mysql_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            d38_0 DECIMAL(38, 0),
            d50_10 DECIMAL(50, 10)
         );
         INSERT INTO {table_name} VALUES
            (1, 99999999999999999999999999999999999999,
                1234567890123456789012345678901234567890.0123456789)",
        "SELECT id, d38_0, d50_10 FROM {table_name} ORDER BY id",
        "my_edge_dec",
        // No `columns:` override — MySQL derives precision/scale from the wire.
        "",
    );

    let described = duckdb_run_sql_json(&format!("DESCRIBE SELECT * FROM read_parquet('{glob}')"));
    let actual = duckdb_parse_describe(&described);
    assert_eq!(actual["d38_0"], "DECIMAL(38,0)", "Decimal128 boundary");
    // DuckDB downgrades precision > 38 to DOUBLE (same as the PG edge test);
    // the Parquet still carries Decimal256, checked via pyarrow below.
    assert_eq!(actual["d50_10"], "DOUBLE", "DuckDB downgrades precision-50 to DOUBLE");

    let v = duckdb_run_sql_json(&format!(
        "SELECT d38_0::VARCHAR FROM read_parquet('{glob}') WHERE id = 1"
    ));
    assert_eq!(
        v["rows"][0][0].as_str().unwrap(),
        "99999999999999999999999999999999999999",
        "Decimal128 max value"
    );

    let stdout = duckdb_run_python(&format!(
        r#"
import pyarrow.parquet as pq, glob
paths = sorted(glob.glob('{glob}'))
t = pq.read_table(paths[0])
print(str(t.column('d50_10').to_pylist()[0]))
"#,
    ));
    assert_eq!(
        stdout.trim(),
        "1234567890123456789012345678901234567890.0123456789",
        "Decimal256 value must round-trip exactly through pyarrow (no override needed)"
    );
}

// ─── Datetime boundaries: pre-1970 + far-future DATETIME, UTC TIMESTAMP ──────

#[test]
#[ignore = "live: requires docker compose mysql + duckdb"]
fn mysql_edge_datetime_boundaries_round_trip() {
    // MySQL TIMESTAMP is limited to 1970–2038; pre-epoch / far-future must use
    // DATETIME (naive). TIMESTAMP carries the UTC instant (rivet pins the
    // session to +00:00).
    let glob = mysql_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            dt_pre DATETIME(6),
            dt_far DATETIME(6),
            ts_utc TIMESTAMP(6)
         );
         INSERT INTO {table_name} VALUES
            (1, '1700-01-01 12:00:00.000000',
                '9999-12-31 23:59:59.999999',
                '2024-06-15 07:00:00.000000')",
        "SELECT id, dt_pre, dt_far, ts_utc FROM {table_name} ORDER BY id",
        "my_edge_dt",
        "",
    );
    let row = duckdb_run_sql_json(&format!(
        "SELECT strftime(dt_pre, '%Y-%m-%d %H:%M:%S.%f') AS p,
                strftime(dt_far, '%Y-%m-%d %H:%M:%S.%f') AS f,
                strftime(ts_utc AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f') AS u
         FROM read_parquet('{glob}') WHERE id = 1"
    ));
    let r = row["rows"][0].as_array().unwrap();
    assert_eq!(r[0].as_str().unwrap(), "1700-01-01 12:00:00.000000", "pre-1970 DATETIME");
    assert_eq!(r[1].as_str().unwrap(), "9999-12-31 23:59:59.999999", "far-future DATETIME");
    assert_eq!(r[2].as_str().unwrap(), "2024-06-15 07:00:00.000000", "UTC TIMESTAMP instant");
}

// ─── JSON edge content ──────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql + duckdb"]
fn mysql_edge_json_content_round_trip() {
    let glob = mysql_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            j_deep JSON, j_unicode JSON, j_bignum JSON, j_mixed JSON
         );
         INSERT INTO {table_name} VALUES
            (1, '{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":\"deep\"}}}}}',
                '{\"日本\":\"value\",\"emoji\":\"🚀\"}',
                '{\"n\":9223372036854775807}',
                '[1,2,\"three\",true,null,{\"k\":\"v\"}]')",
        "SELECT id, j_deep, j_unicode, j_bignum, j_mixed FROM {table_name} ORDER BY id",
        "my_edge_json",
        "",
    );
    let r = duckdb_run_sql_json(&format!(
        "SELECT json_extract_string(j_deep::JSON, '$.a.b.c.d.e') AS deep,
                json_extract_string(j_unicode::JSON, '$.\"日本\"') AS uni,
                json_extract_string(j_bignum::JSON, '$.n') AS bignum,
                json_array_length(j_mixed::JSON) AS arrlen
         FROM read_parquet('{glob}') WHERE id = 1"
    ));
    let row = r["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "deep");
    assert_eq!(row[1].as_str().unwrap(), "value", "JSON unicode key");
    assert_eq!(row[2].as_str().unwrap(), "9223372036854775807", "i64 max in JSON");
    assert_eq!(row[3].as_str().unwrap(), "6", "mixed-type array length");
}

// ─── String extremes ────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql + duckdb"]
fn mysql_edge_string_extremes_round_trip() {
    let glob = mysql_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            s_empty TEXT, s_ws TEXT, s_huge LONGTEXT
         );
         INSERT INTO {table_name} VALUES
            (1, '', '   \t\n   ', REPEAT('X', 100000))",
        "SELECT id, s_empty, s_ws, s_huge FROM {table_name} ORDER BY id",
        "my_edge_str",
        "",
    );
    let r = duckdb_run_sql_json(&format!(
        "SELECT length(s_empty) AS le, length(s_ws) AS lw,
                length(s_huge) AS lh, substr(s_huge, 1, 1) AS fc
         FROM read_parquet('{glob}') WHERE id = 1"
    ));
    let row = r["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "0", "empty string");
    assert_eq!(row[1].as_str().unwrap(), "8", "whitespace string length (3+1+1+3)");
    assert_eq!(row[2].as_str().unwrap(), "100000", "100k-char string preserved");
    assert_eq!(row[3].as_str().unwrap(), "X");
}

// ─── MySQL-native types: BIT, YEAR bounds, SET, TINYINT(1), all-NULL ────────

#[test]
#[ignore = "live: requires docker compose mysql + duckdb"]
fn mysql_edge_native_types_round_trip() {
    let glob = mysql_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            c_bit8 BIT(8),
            year_min YEAR, year_max YEAR,
            set_col SET('x','y','z'),
            c_bool TINYINT(1),
            note_null VARCHAR(50)
         );
         INSERT INTO {table_name} VALUES
            (1, b'11111111', 1901, 2155, 'x,z', 1, NULL)",
        "SELECT id, c_bit8, year_min, year_max, set_col, c_bool, note_null FROM {table_name} ORDER BY id",
        "my_edge_native",
        "",
    );
    let r = duckdb_run_sql_json(&format!(
        "SELECT c_bit8::VARCHAR, year_min::VARCHAR, year_max::VARCHAR,
                set_col, c_bool::VARCHAR, CASE WHEN note_null IS NULL THEN 1 ELSE 0 END
         FROM read_parquet('{glob}') WHERE id = 1"
    ));
    let row = r["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "255", "BIT(8) all-ones = 255");
    assert_eq!(row[1].as_str().unwrap(), "1901", "YEAR min");
    assert_eq!(row[2].as_str().unwrap(), "2155", "YEAR max");
    assert_eq!(row[3].as_str().unwrap(), "x,z", "SET preserves multi-value text");
    assert_eq!(row[4].as_str().unwrap(), "true", "TINYINT(1) = bool true");
    assert_eq!(row[5].as_str().unwrap(), "1", "all-NULL column stays NULL");
}
