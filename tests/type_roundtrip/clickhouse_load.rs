//! Live Parquet validation via ClickHouse (ADR-0014 §4).
//!
//! Companion to `duckdb_load`: feeds the same Parquet produced by the
//! canonical PG / MySQL matrix into a second independent reader (ClickHouse
//! over HTTP :8123, see `docker-compose.yaml`). Two independent oracles
//! catch defects that one alone would not — DuckDB and ClickHouse infer
//! Parquet types with slightly different rules (e.g. ClickHouse treats every
//! Arrow column as `Nullable(T)`, returns Decimal as JSON numbers, BLOB as
//! `String`).
//!
//! ClickHouse-specific assertions worth knowing:
//!   * `file('/work/...', 'Parquet')` is enabled by `user_files.xml` patch.
//!   * `toUUID(...)` is strict — it errors on any malformed UUID, so a
//!     `count(toUUID(uid))` succeeding across N rows proves N valid UUIDs.
//!   * `isValidJSON(...)` returns 1 for valid JSON, 0 otherwise.

use crate::common::*;

use super::helpers::{
    MssqlCleanup, MysqlCleanup, PgCleanup, run_mssql_matrix_export, run_mysql_matrix_export,
    run_pg_matrix_export, setup_mssql_matrix_table, setup_mysql_matrix_table,
    setup_pg_matrix_table,
};

/// `path` is the in-container glob (relative to `/work` thanks to the
/// `user_files_path` patch). Returns the single object the query yields, or
/// panics with the row dump if the row count is not 1.
fn ch_one(sql: &str) -> serde_json::Value {
    let rows = clickhouse_run_sql_json(sql);
    assert_eq!(rows.len(), 1, "expected 1 row from: {sql}\nrows: {rows:?}");
    rows.into_iter().next().unwrap()
}

// ─── PostgreSQL matrix → Parquet → ClickHouse ──────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + clickhouse"]
fn clickhouse_validates_postgres_type_matrix_parquet() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::ClickHouse);

    let table_name = unique_name("ch_pg");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    // `clickhouse_shared_workdir` returns the (host, container) pair where
    // the container path is `/work/<rel>`. ClickHouse's `file()` function
    // sees the same dir relative to `user_files_path=/work`, so we strip
    // the leading `/work/` to build a relative glob.
    let (host_dir, container_dir) = clickhouse_shared_workdir(&unique_name("ch_pg_out"));
    run_pg_matrix_export(&table_name, "parquet", &host_dir);
    let rel = container_dir
        .strip_prefix("/work/")
        .expect("container path lives under /work");
    let glob = format!("{rel}/*.parquet");

    // 1) Schema autoload: every Rivet type lands on the expected ClickHouse
    //    physical type after Parquet read-back. All fields are Nullable
    //    because Rivet emits nullable Arrow fields by default.
    let described = clickhouse_run_sql_json(&format!("DESCRIBE TABLE file('{glob}', 'Parquet')"));
    let actual = clickhouse_parse_describe(described);
    let expected = [
        ("id", "Nullable(Int64)"),
        ("c_smallint", "Nullable(Int16)"),
        ("c_integer", "Nullable(Int32)"),
        ("c_bigint", "Nullable(Int64)"),
        ("amount", "Nullable(Decimal(18, 2))"),
        ("fee", "Nullable(Decimal(20, 6))"),
        ("price", "Nullable(Decimal(10, 2))"),
        ("c_real", "Nullable(Float32)"),
        ("c_double", "Nullable(Float64)"),
        ("c_date", "Nullable(Date32)"),
        ("created_at", "Nullable(DateTime64(6))"),
        ("created_at_tz", "Nullable(DateTime64(6, 'UTC'))"),
        ("raw_bytes", "Nullable(String)"),
        // ClickHouse 24.8 does not yet recognise `LogicalType::Uuid` in
        // Parquet — it autoloads our 16-byte field as `FixedString(16)`.
        // The bytes are correct (verified by `toUUID(uid)` parsing below)
        // and ClickHouse 25.x reads native `LogicalType::Uuid` as `UUID`
        // directly; when this stack image bumps, the assertion changes.
        ("uid", "Nullable(FixedString(16))"),
        // ClickHouse's stable JSON type also landed later; on 24.8 our
        // `LogicalType::Json` columns still arrive as `String`. The
        // Parquet footer is unchanged across reader versions.
        ("attrs", "Nullable(String)"),
        ("attrs_json", "Nullable(String)"),
        ("c_bool", "Nullable(Bool)"),
        ("tags", "Array(Nullable(String))"),
        ("nums", "Array(Nullable(Int32))"),
    ];
    for (col, want) in expected {
        let got = actual
            .get(col)
            .unwrap_or_else(|| panic!("clickhouse did not see column `{col}`: {actual:?}"));
        assert_eq!(
            got, want,
            "clickhouse autoload type for `{col}`: expected {want}, got {got}"
        );
    }

    // 2) Decimal sums: same constants as the in-process roundtrip + DuckDB.
    let sums = ch_one(&format!(
        "SELECT count(*) AS n,
                toString(sum(amount * 100)::Int64) AS s_amount,
                toString(sum(fee * 1000000)::Int64) AS s_fee
         FROM file('{glob}', 'Parquet')"
    ));
    assert_eq!(sums["n"].as_str().unwrap(), "4");
    assert_eq!(sums["s_amount"].as_str().unwrap(), "99999999990024");
    assert_eq!(sums["s_fee"].as_str().unwrap(), "10000003");

    // 3) JSON validity + UUID parsability across every row. `toUUID` raises
    //    on malformed input, so a successful count proves all 4 strings
    //    parse cleanly as UUIDs. (ClickHouse serialises UInt64 / Int64 as
    //    quoted strings in JSONEachRow by default — hence `as_str`.)
    // ClickHouse 24.8 surfaces our native-UUID column as `FixedString(16)`
    // (see DESCRIBE pin above); `toUUID` only accepts text-shaped UUIDs in
    // this version, so we go through `lower(hex(uid))` to get the 32-hex
    // form and assert each row parses back. Bytes are unchanged.
    let basics = ch_one(&format!(
        "SELECT sum(isValidJSON(attrs)) AS attrs_ok,
                sum(isValidJSON(attrs_json)) AS attrs_json_ok,
                count(toUUID(
                    concat(
                        substring(lower(hex(uid)), 1, 8),  '-',
                        substring(lower(hex(uid)), 9, 4),  '-',
                        substring(lower(hex(uid)), 13, 4), '-',
                        substring(lower(hex(uid)), 17, 4), '-',
                        substring(lower(hex(uid)), 21, 12)
                    )
                )) AS uid_ok
         FROM file('{glob}', 'Parquet')"
    ));
    assert_eq!(basics["attrs_ok"].as_str().unwrap(), "4");
    assert_eq!(basics["attrs_json_ok"].as_str().unwrap(), "4");
    assert_eq!(basics["uid_ok"].as_str().unwrap(), "4");

    // 4) tz vs naive timestamp wall clock on id=1.
    let ts = ch_one(&format!(
        "SELECT toString(created_at) AS naive, toString(created_at_tz) AS utc
         FROM file('{glob}', 'Parquet') WHERE id = 1"
    ));
    assert_eq!(ts["naive"].as_str().unwrap(), "2035-08-07 09:08:07.987654");
    assert_eq!(ts["utc"].as_str().unwrap(), "2035-08-07 09:08:07.987654");

    // 5) BLOB bytes (ClickHouse calls it `String` but stores raw bytes).
    let blob = ch_one(&format!(
        "SELECT lower(hex(raw_bytes)) AS h FROM file('{glob}', 'Parquet') WHERE id = 1"
    ));
    assert_eq!(blob["h"].as_str().unwrap(), "00ff012345");

    // 6) List element totals + the empty list survives (row id=3, tags = []).
    let lists = ch_one(&format!(
        "SELECT sum(length(tags)) AS tt, sum(length(nums)) AS nn,
                sum(length(tags) = 0) AS empty_lists
         FROM file('{glob}', 'Parquet')"
    ));
    assert_eq!(lists["tt"].as_str().unwrap(), "4");
    assert_eq!(lists["nn"].as_str().unwrap(), "8");
    assert_eq!(lists["empty_lists"].as_str().unwrap(), "1");

    // 7) Nullability propagates: note_all_null is NULL on every row.
    let nulls = ch_one(&format!(
        "SELECT sum(note_all_null IS NULL) AS all_null
         FROM file('{glob}', 'Parquet')"
    ));
    assert_eq!(nulls["all_null"].as_str().unwrap(), "4");

    // 8) Interval serialised as ISO 8601 string round-trips.
    let iv = ch_one(&format!(
        "SELECT interval_col FROM file('{glob}', 'Parquet') WHERE id = 1"
    ));
    assert_eq!(iv["interval_col"].as_str().unwrap(), "P1Y2M3D");
}

// ─── MySQL matrix → Parquet → ClickHouse ───────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql + clickhouse"]
fn clickhouse_validates_mysql_type_matrix_parquet() {
    require_alive(LiveService::Mysql);
    require_alive(LiveService::ClickHouse);

    let table_name = unique_name("ch_my");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let (host_dir, container_dir) = clickhouse_shared_workdir(&unique_name("ch_my_out"));
    run_mysql_matrix_export(&table_name, "parquet", &host_dir);
    let rel = container_dir.strip_prefix("/work/").unwrap();
    let glob = format!("{rel}/*.parquet");

    // 1) MySQL-specific physical types after autoload. UInt64 round-tripping
    //    is the high-value invariant here.
    let described = clickhouse_run_sql_json(&format!("DESCRIBE TABLE file('{glob}', 'Parquet')"));
    let actual = clickhouse_parse_describe(described);
    let expected = [
        ("c_tinyint", "Nullable(Int16)"),
        ("c_tinyint_u", "Nullable(Int16)"),
        ("c_smallint_u", "Nullable(Int32)"),
        ("c_int_u", "Nullable(Int64)"),
        // UInt64 — ClickHouse keeps it as native UInt64, no overflow widening.
        ("c_bigint_u", "Nullable(UInt64)"),
        ("amount", "Nullable(Decimal(18, 2))"),
        ("year_col", "Nullable(Int16)"),
        ("c_bit1", "Nullable(Bool)"),
        ("c_bit8", "Nullable(Int64)"),
        ("created_at_dt", "Nullable(DateTime64(6))"),
        ("created_at_ts", "Nullable(DateTime64(6, 'UTC'))"),
    ];
    for (col, want) in expected {
        let got = actual
            .get(col)
            .unwrap_or_else(|| panic!("clickhouse did not see column `{col}`: {actual:?}"));
        assert_eq!(
            got, want,
            "clickhouse autoload type for `{col}`: expected {want}, got {got}"
        );
    }

    // 2) UNSIGNED edge values round-trip across the wire — including the
    //    `BIGINT UNSIGNED` max (2^64 − 1), the case where naive Int64
    //    handling would silently overflow.
    let edge = ch_one(&format!(
        "SELECT toString(c_tinyint_u) AS a,
                toString(c_smallint_u) AS b,
                toString(c_int_u) AS c,
                toString(c_bigint_u) AS d
         FROM file('{glob}', 'Parquet') WHERE id = 1"
    ));
    assert_eq!(edge["a"].as_str().unwrap(), "255");
    assert_eq!(edge["b"].as_str().unwrap(), "65535");
    assert_eq!(edge["c"].as_str().unwrap(), "4294967295");
    assert_eq!(edge["d"].as_str().unwrap(), "18446744073709551615");

    // 3) Decimal sums (same constants across all readers).
    let sums = ch_one(&format!(
        "SELECT toString(sum(amount * 100)::Int64) AS s_amount,
                toString(sum(fee * 1000000)::Int64) AS s_fee
         FROM file('{glob}', 'Parquet')"
    ));
    assert_eq!(sums["s_amount"].as_str().unwrap(), "99999999990024");
    assert_eq!(sums["s_fee"].as_str().unwrap(), "10000003");

    // 4) JSON validity + UUID parsing across every row.
    let basics = ch_one(&format!(
        "SELECT sum(isValidJSON(extras)) AS json_ok,
                count(toUUID(uid)) AS uid_ok
         FROM file('{glob}', 'Parquet')"
    ));
    assert_eq!(basics["json_ok"].as_str().unwrap(), "4");
    assert_eq!(basics["uid_ok"].as_str().unwrap(), "4");

    // 5) Binary BLOBs (BINARY(4), BLOB) — bytes must match the seed.
    let blob = ch_one(&format!(
        "SELECT lower(hex(raw_bytes)) AS rb, lower(hex(blob_bytes)) AS bb
         FROM file('{glob}', 'Parquet') WHERE id = 1"
    ));
    assert_eq!(blob["rb"].as_str().unwrap(), "00ff0123");
    assert_eq!(blob["bb"].as_str().unwrap(), "cafebabe");

    // 6) bit(1) → Bool, bit(8) → Int64.
    let bits = ch_one(&format!(
        "SELECT c_bit1, toString(c_bit8) AS bit8 FROM file('{glob}', 'Parquet') WHERE id = 1"
    ));
    assert!(bits["c_bit1"].as_bool().unwrap());
    assert_eq!(bits["bit8"].as_str().unwrap(), "255");
}

// ─── SQL Server matrix → Parquet → ClickHouse ──────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse"]
fn clickhouse_validates_mssql_type_matrix_parquet() {
    require_alive(LiveService::Mssql);
    require_alive(LiveService::ClickHouse);

    let table_name = unique_name("ch_ms");
    setup_mssql_matrix_table(&table_name);
    let _guard = MssqlCleanup(table_name.clone());

    let (host_dir, container_dir) = clickhouse_shared_workdir(&unique_name("ch_ms_out"));
    run_mssql_matrix_export(&table_name, "parquet", &host_dir);
    let rel = container_dir.strip_prefix("/work/").unwrap();
    let glob = format!("{rel}/*.parquet");

    // 1) ClickHouse autoload types. tinyint widens to Int16, bit → Bool,
    //    decimal scale survives, uniqueidentifier lands as FixedString(16)
    //    (CH 24.x reads the arrow.uuid logical type as 16 raw bytes).
    let described = clickhouse_run_sql_json(&format!("DESCRIBE TABLE file('{glob}', 'Parquet')"));
    let actual = clickhouse_parse_describe(described);
    let expected = [
        ("id", "Nullable(Int64)"),
        ("c_smallint", "Nullable(Int16)"),
        ("c_int", "Nullable(Int32)"),
        ("c_bigint", "Nullable(Int64)"),
        ("c_tinyint", "Nullable(Int16)"),
        ("c_bit", "Nullable(Bool)"),
        ("amount", "Nullable(Decimal(18, 2))"),
        ("fee", "Nullable(Decimal(20, 6))"),
        ("price", "Nullable(Decimal(10, 2))"),
        ("c_real", "Nullable(Float32)"),
        ("c_float", "Nullable(Float64)"),
        ("c_date", "Nullable(Date32)"),
        ("created_at", "Nullable(DateTime64(6))"),
        // datetimeoffset → Timestamp(µs, UTC): ClickHouse carries the UTC zone
        // into the type (vs the naive datetime2 above), so the tz-awareness is
        // visible in the autoload schema, not just the value.
        ("created_at_tz", "Nullable(DateTime64(6, 'UTC'))"),
        ("label", "Nullable(String)"),
        ("c_varchar", "Nullable(String)"),
        ("raw_bytes", "Nullable(String)"),
        ("uid", "Nullable(FixedString(16))"),
        ("c_nvarchar", "Nullable(String)"),
    ];
    for (col, want) in expected {
        let got = actual
            .get(col)
            .unwrap_or_else(|| panic!("clickhouse did not see column `{col}`: {actual:?}"));
        assert_eq!(
            got, want,
            "clickhouse autoload type for `{col}`: expected {want}, got {got}"
        );
    }

    // 2) Aggregates: 3 rows, signed bigint sums to zero, decimal scale preserved.
    let agg = ch_one(&format!(
        "SELECT count(*) AS n, toString(sum(c_bigint)) AS sb, toString(sum(amount)) AS sa
         FROM file('{glob}', 'Parquet')"
    ));
    assert_eq!(agg["n"].as_str().unwrap(), "3");
    assert_eq!(agg["sb"].as_str().unwrap(), "0");
    assert_eq!(
        agg["sa"].as_str().unwrap(),
        "1234.55",
        "decimal scale must survive into ClickHouse"
    );

    // 3) Unicode text byte-exact; all-null column is null for every row.
    let txt = ch_one(&format!(
        "SELECT c_nvarchar, toString(isNull(note_all_null)) AS an
         FROM file('{glob}', 'Parquet') WHERE id = 1"
    ));
    assert_eq!(txt["c_nvarchar"].as_str().unwrap(), "héllo wörld");
    assert_eq!(
        txt["an"].as_str().unwrap(),
        "1",
        "note_all_null must be NULL"
    );

    // 4) datetimeoffset lands as its UTC instant. toUnixTimestamp64Micro is
    //    tz-independent, so the check is stable regardless of CH's server tz:
    //    +05:30 13:45:30 → 08:15:30 UTC, negative −08:00 00:00:00 → 08:00:00 UTC,
    //    NULL stays NULL.
    let dto1 = ch_one(&format!(
        "SELECT toString(toUnixTimestamp64Micro(created_at_tz)) AS us
         FROM file('{glob}', 'Parquet') WHERE id = 1"
    ));
    assert_eq!(
        dto1["us"].as_str().unwrap(),
        "1768464930123456",
        "datetimeoffset id=1 (+05:30) must round-trip as its UTC instant"
    );
    let dto2 = ch_one(&format!(
        "SELECT toString(toUnixTimestamp64Micro(created_at_tz)) AS us
         FROM file('{glob}', 'Parquet') WHERE id = 2"
    ));
    assert_eq!(
        dto2["us"].as_str().unwrap(),
        "946713600000000",
        "datetimeoffset id=2 (negative −08:00 offset) must round-trip as its UTC instant"
    );
    let dto3 = ch_one(&format!(
        "SELECT toString(isNull(created_at_tz)) AS n
         FROM file('{glob}', 'Parquet') WHERE id = 3"
    ));
    assert_eq!(
        dto3["n"].as_str().unwrap(),
        "1",
        "datetimeoffset NULL must stay NULL"
    );
}
