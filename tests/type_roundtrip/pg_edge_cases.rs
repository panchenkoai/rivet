//! PostgreSQL type edge cases — values that the canonical matrix avoids on
//! purpose because they would force every assertion in `helpers.rs` to
//! re-pin the expected sums. Each edge case here is a self-contained PG
//! table that exercises one boundary, then verifies through DuckDB.
//!
//! Categories covered (ADR-0014 §4 extended):
//!
//!   * Decimal: `numeric(38,0)` (Decimal128 max precision); `numeric(39,10)`
//!     (escalates to Decimal256); `numeric(5,-2)` (negative scale).
//!   * Timestamp: pre-epoch (1700), far-future (9999), non-UTC offset
//!     normalised to UTC at the wire.
//!   * JSON: deep nesting, unicode keys, i64-edge numbers, mixed-type arrays.
//!   * Array: empty, single, `NULL`-inside (PG arrays can hold NULL elements).
//!   * String: empty, all-whitespace, 10 KB single-cell, embedded NUL is
//!     skipped (PG rejects NUL in TEXT).
//!
//! Each block creates a small table with one or two rows. If a block fails
//! the panic message identifies the case immediately — no cross-row
//! interference between unrelated edges.

use std::process::Output;

use postgres::Client as PgClient;
use postgres::NoTls;

use crate::common::*;

/// Bring up a temporary PG table from inline DDL, then run a Rivet export
/// (parquet, zstd) into a shared duckdb workdir. Returns the container-side
/// glob path and the raw `Output` of the rivet child process — callers
/// decide whether the export was supposed to succeed (`out.status.success()`)
/// or to fail with a particular diagnostic (negative-scale decimal, etc.).
///
/// We do not assert success here so the "currently-fails" pins can reuse
/// the same setup/cleanup scaffolding instead of inlining a duplicate YAML.
fn pg_edge_export(
    setup_sql: &str,
    query: &str,
    label: &str,
    overrides_yaml: &str,
) -> (String, Output) {
    let table = unique_name(label);
    let mut c = PgClient::connect(POSTGRES_URL, NoTls).expect("connect");
    c.batch_execute(&format!("DROP TABLE IF EXISTS {table} CASCADE;"))
        .unwrap();
    c.batch_execute(&setup_sql.replace("{table_name}", &table))
        .unwrap();

    let (host_dir, container_dir) = duckdb_shared_workdir(&format!("{label}_out"));
    let cfg_dir = tempfile::tempdir().unwrap();
    let export_name = format!("{label}_export");
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
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

    // Best-effort drop — RAII isn't great here since each block has a different
    // schema and PgCleanup expects a fixed shape with an enum type.
    let _ = c.batch_execute(&format!("DROP TABLE IF EXISTS {table} CASCADE;"));

    (format!("{container_dir}/*.parquet"), out)
}

/// Caller helper: assert the export succeeded and return the glob to pass
/// to DuckDB. Every "success path" edge-case test uses this thin wrapper.
fn pg_edge_export_ok(setup_sql: &str, query: &str, label: &str, overrides_yaml: &str) -> String {
    let (glob, out) = pg_edge_export(setup_sql, query, label, overrides_yaml);
    assert!(
        out.status.success(),
        "edge-case export `{label}`: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    glob
}

// ─── Decimal boundaries ────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn pg_edge_decimal_boundaries_round_trip() {
    let glob = pg_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            d38_0 NUMERIC(38, 0),
            d39_10 NUMERIC(39, 10)
         );
         INSERT INTO {table_name} VALUES
            (1, 99999999999999999999999999999999999999, -- 38 nines, Decimal128 max
                12345678901234567890123456789.0123456789)",
        "SELECT id, d38_0, d39_10 FROM {table_name} ORDER BY id",
        "pg_edge_dec",
        // Ad-hoc SELECT queries do not let the driver reach numeric
        // precision/scale through the catalog; column overrides are the
        // documented escape hatch (roadmap §12).
        "    columns:\n      d38_0: decimal(38,0)\n      d39_10: decimal(39,10)\n",
    );

    // Schema-level: precision 38 stays Decimal128, precision 39 escalates.
    let described = duckdb_run_sql_json(&format!("DESCRIBE SELECT * FROM read_parquet('{glob}')"));
    let actual = duckdb_parse_describe(&described);
    assert_eq!(actual["d38_0"], "DECIMAL(38,0)", "Decimal128 boundary");
    // Known DuckDB limitation: DuckDB's `DECIMAL` only supports precision
    // 1..=38 (it's backed by HUGEINT — 128 bits). For Parquet columns with
    // precision > 38 DuckDB silently downgrades to DOUBLE and loses
    // precision past 2^53. Our Parquet file still carries the correct
    // `LogicalType::Decimal(39, 10)` — pyarrow reads it as Decimal256.
    // See `parquet_schema.rs` for the physical-type contract.
    assert_eq!(
        actual["d39_10"], "DOUBLE",
        "DuckDB downgrades precision-39 DECIMAL to DOUBLE; \
         if this ever becomes DECIMAL(...), DuckDB grew Decimal256 support — \
         update the test"
    );

    // Values: max-precision integer round-trips exactly through DuckDB
    // because 10^38 - 1 fits in DECIMAL(38,0) → HUGEINT.
    let v = duckdb_run_sql_json(&format!(
        "SELECT d38_0::VARCHAR FROM read_parquet('{glob}') WHERE id = 1"
    ));
    assert_eq!(
        v["rows"][0][0].as_str().unwrap(),
        "99999999999999999999999999999999999999",
        "Decimal128 max precision value"
    );

    // Cross-check d39_10 round-trip through pyarrow (the reference reader
    // that handles Decimal256 properly). The seed value rendered as a
    // string with 10 fractional digits is `12345678901234567890123456789.0123456789`.
    let stdout = duckdb_run_python(&format!(
        r#"
import pyarrow.parquet as pq, glob
paths = sorted(glob.glob('{glob}'))
t = pq.read_table(paths[0])
print(str(t.column('d39_10').to_pylist()[0]))
"#,
    ));
    assert_eq!(
        stdout.trim(),
        "12345678901234567890123456789.0123456789",
        "Decimal256 value must round-trip exactly through pyarrow"
    );
}

/// Known Parquet limitation: the `DECIMAL` logical type's scale must be
/// non-negative (Apache Parquet spec, `LogicalTypes.md`). PostgreSQL
/// `numeric(5, -2)` is otherwise representable in Rivet (`RivetType::Decimal
/// { precision: 5, scale: -2 }` maps to Arrow `Decimal128(5, -2)`) but the
/// Parquet writer rejects it at flush time.
///
/// This test pins the *current* behaviour — an export fails — so that any
/// future commit that adds a graceful fallback (auto-rescaling to
/// `decimal(p+|s|, 0)`, or fronting an explicit policy decision) must
/// update the test and write a doc entry.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_edge_decimal_negative_scale_currently_fails_at_parquet_write() {
    require_alive(LiveService::Postgres);

    let (_glob, out) = pg_edge_export(
        "CREATE TABLE {table_name} (id INT, d NUMERIC(5, -2));
         INSERT INTO {table_name} VALUES (1, 12345);",
        "SELECT id, d FROM {table_name} ORDER BY id",
        "pg_edge_negsc",
        "    columns:\n      d: decimal(5,-2)\n",
    );

    assert!(
        !out.status.success(),
        "negative-scale decimal export should fail (Parquet spec disallows it); \
         if it now succeeds, update this test and document the new policy"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("DECIMAL scale") || err.contains("scale: -2"),
        "expected an Invalid-DECIMAL-scale error in stderr, got:\n{err}"
    );
}

// ─── Timestamps: pre-epoch + far future + non-UTC normalisation ────────────

#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn pg_edge_timestamp_boundaries_round_trip() {
    let glob = pg_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            t_pre TIMESTAMPTZ,
            t_far TIMESTAMPTZ,
            t_offset TIMESTAMPTZ
         );
         INSERT INTO {table_name} VALUES
            (1, TIMESTAMPTZ '1700-01-01 12:00:00+00',
                TIMESTAMPTZ '9999-12-31 23:59:59.999999+00',
                TIMESTAMPTZ '2024-06-15 12:00:00+05')",
        "SELECT id, t_pre, t_far, t_offset FROM {table_name} ORDER BY id",
        "pg_edge_ts",
        "",
    );
    let row = duckdb_run_sql_json(&format!(
        "SELECT strftime(t_pre AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f') AS p,
                strftime(t_far AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f') AS f,
                strftime(t_offset AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f') AS o
         FROM read_parquet('{glob}') WHERE id = 1"
    ));
    let r = row["rows"][0].as_array().unwrap();
    assert_eq!(
        r[0].as_str().unwrap(),
        "1700-01-01 12:00:00.000000",
        "pre-epoch tz timestamp"
    );
    assert_eq!(
        r[1].as_str().unwrap(),
        "9999-12-31 23:59:59.999999",
        "far-future tz timestamp"
    );
    // +05 wall clock 12:00 normalises to 07:00 UTC.
    assert_eq!(
        r[2].as_str().unwrap(),
        "2024-06-15 07:00:00.000000",
        "non-UTC offset normalised to UTC"
    );
}

// ─── JSON edge content (deep nesting, unicode keys, i64 edge numbers) ──────

#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn pg_edge_json_content_round_trip() {
    let glob = pg_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            j_deep JSONB,
            j_unicode JSONB,
            j_bignum JSONB,
            j_mixed JSONB
         );
         INSERT INTO {table_name} VALUES
            (1, '{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":\"deep\"}}}}}'::jsonb,
                '{\"日本\":\"value\",\"emoji\":\"🚀\"}'::jsonb,
                '{\"n\":9223372036854775807}'::jsonb,
                '[1,2,\"three\",true,null,{\"k\":\"v\"}]'::jsonb)",
        "SELECT id, j_deep, j_unicode, j_bignum, j_mixed FROM {table_name} ORDER BY id",
        "pg_edge_json",
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
    assert_eq!(row[1].as_str().unwrap(), "value", "JSON with unicode key");
    assert_eq!(
        row[2].as_str().unwrap(),
        "9223372036854775807",
        "i64 max in JSON"
    );
    assert_eq!(row[3].as_str().unwrap(), "6", "mixed-type array length");
}

// ─── Arrays: empty, single, with NULL element ──────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn pg_edge_array_edge_shapes_round_trip() {
    let glob = pg_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            a_empty INT[],
            a_single INT[],
            a_with_null INT[]
         );
         INSERT INTO {table_name} VALUES
            (1, ARRAY[]::int[],
                ARRAY[42]::int[],
                ARRAY[1, NULL, 3]::int[])",
        "SELECT id, a_empty, a_single, a_with_null FROM {table_name} ORDER BY id",
        "pg_edge_arr",
        "",
    );
    let r = duckdb_run_sql_json(&format!(
        "SELECT len(a_empty) AS n_empty,
                len(a_single) AS n_single,
                len(a_with_null) AS n_wn,
                CASE WHEN list_contains(a_with_null, NULL) THEN 1 ELSE 0 END AS has_null
         FROM read_parquet('{glob}') WHERE id = 1"
    ));
    let row = r["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "0", "empty array preserved");
    assert_eq!(row[1].as_str().unwrap(), "1");
    assert_eq!(
        row[2].as_str().unwrap(),
        "3",
        "ARRAY[1,NULL,3] retains length 3"
    );
    // DuckDB's list_contains may not match NULL; a more portable check is
    // counting non-null elements: should be 2 (1 and 3).
    let r2 = duckdb_run_sql_json(&format!(
        "SELECT len(list_filter(a_with_null, x -> x IS NOT NULL)) AS non_nulls
         FROM read_parquet('{glob}') WHERE id = 1"
    ));
    assert_eq!(
        r2["rows"][0][0].as_str().unwrap(),
        "2",
        "ARRAY[1,NULL,3] keeps 2 non-NULL elements"
    );
}

// ─── String edge cases: empty, whitespace, large single cell ───────────────

#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn pg_edge_string_extremes_round_trip() {
    let glob = pg_edge_export_ok(
        "CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            s_empty TEXT,
            s_ws TEXT,
            s_huge TEXT
         );
         INSERT INTO {table_name} VALUES
            (1, '', '   \t\n   ', repeat('X', 100000))",
        "SELECT id, s_empty, s_ws, s_huge FROM {table_name} ORDER BY id",
        "pg_edge_str",
        "",
    );
    let r = duckdb_run_sql_json(&format!(
        "SELECT length(s_empty) AS le,
                length(s_ws) AS lw,
                length(s_huge) AS lh,
                substr(s_huge, 1, 1) AS first_char
         FROM read_parquet('{glob}') WHERE id = 1"
    ));
    let row = r["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "0", "empty string length");
    assert_eq!(
        row[1].as_str().unwrap(),
        "8",
        "whitespace string length (3+1+1+3)"
    );
    assert_eq!(
        row[2].as_str().unwrap(),
        "100000",
        "100k-char string preserved"
    );
    assert_eq!(row[3].as_str().unwrap(), "X");
}
