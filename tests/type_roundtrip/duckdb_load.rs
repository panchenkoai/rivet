//! Live Parquet validation via DuckDB (ADR-0014 §4).
//!
//! These tests re-export the canonical PostgreSQL / MySQL type matrices that
//! `*_parquet_roundtrip` already produce, but instead of asserting on the
//! Arrow batches read through the parquet crate, they push the file across a
//! second, independent reader — the `python:3.12-slim + duckdb` container
//! defined in `docker-compose.yaml`. The Parquet is the contract; DuckDB is
//! the oracle.
//!
//! What this catches that the Arrow-side roundtrip already does not:
//!
//! * Parquet logical types written but interpreted differently by a real
//!   downstream consumer (e.g. `DECIMAL(p,s)` precision mismatch, timestamp
//!   units, byte-array vs FIXED_LEN_BYTE_ARRAY for UUID/decimal).
//! * tz-aware vs naive timestamps actually round-tripping as
//!   `TIMESTAMP WITH TIME ZONE` / `TIMESTAMP` in DuckDB.
//! * JSON values being valid JSON when consumed by another engine
//!   (`json_valid(...)` returns true for every row).
//! * UUID strings being parseable as the native UUID type downstream.
//! * Binary values arriving as the same bytes (BLOB equality).
//! * List columns surviving the row-group reader, including the empty list.

use crate::common::*;

use super::helpers::{
    MssqlCleanup, MysqlCleanup, PgCleanup, run_mssql_matrix_export, run_mysql_matrix_export,
    run_pg_matrix_export, setup_mssql_matrix_table, setup_mysql_matrix_table,
    setup_pg_matrix_table,
};

// ─── PostgreSQL matrix → Parquet → DuckDB ──────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn duckdb_validates_postgres_type_matrix_parquet() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);

    let table_name = unique_name("dd_pg");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("dd_pg_out"));
    run_pg_matrix_export(&table_name, "parquet", &host_dir);

    let glob = format!("{container_dir}/*.parquet");
    let q = |sql: &str| duckdb_run_sql_json(&sql.replace("{f}", &glob));

    // 1) Physical autoload types DuckDB infers from the Parquet schema alone.
    // These are the columns that motivated ADR-0014 — JSON / UUID / enum fall
    // through to VARCHAR but everything else must land natively.
    let described = q("DESCRIBE SELECT * FROM read_parquet('{f}')");
    let actual = duckdb_parse_describe(&described);
    let expected = [
        ("id", "BIGINT"),
        ("c_smallint", "SMALLINT"),
        ("c_integer", "INTEGER"),
        ("c_bigint", "BIGINT"),
        ("amount", "DECIMAL(18,2)"),
        ("fee", "DECIMAL(20,6)"),
        ("price", "DECIMAL(10,2)"),
        ("c_real", "FLOAT"),
        ("c_double", "DOUBLE"),
        ("c_date", "DATE"),
        ("c_time", "TIME"),
        ("created_at", "TIMESTAMP"),
        ("created_at_tz", "TIMESTAMP WITH TIME ZONE"),
        ("label", "VARCHAR"),
        ("raw_bytes", "BLOB"),
        // UUID + JSON now autoload as DuckDB's native types because the
        // Parquet file carries `LogicalType::Uuid` / `LogicalType::Json`
        // (via the `arrow.uuid` / `arrow.json` extension wiring). Before
        // the switch these came back as `VARCHAR`.
        ("uid", "UUID"),
        ("attrs", "JSON"),
        ("attrs_json", "JSON"),
        ("c_bool", "BOOLEAN"),
        ("interval_col", "VARCHAR"),
        ("enum_col", "VARCHAR"),
        ("tags", "VARCHAR[]"),
        ("nums", "INTEGER[]"),
    ];
    for (col, want) in expected {
        let got = actual
            .get(col)
            .unwrap_or_else(|| panic!("duckdb did not see column `{col}`; saw: {actual:?}"));
        assert_eq!(
            got, want,
            "duckdb autoload type for `{col}`: expected {want}, got {got}"
        );
    }

    // 2) Decimals: sums computed in DuckDB must equal the sums computed by
    // the in-process Arrow reader (those constants come from `helpers.rs`).
    let sums =
        q("SELECT sum(amount * 100)::BIGINT, sum(fee * 1000000)::BIGINT FROM read_parquet('{f}')");
    let row = sums["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "99999999990024");
    assert_eq!(row[1].as_str().unwrap(), "10000003");

    // 3) Row count, JSON validity, UUID parsability — every row must pass.
    let basics = q("SELECT count(*) AS n,
                          sum(CASE WHEN json_valid(attrs) THEN 1 ELSE 0 END) AS attrs_ok,
                          sum(CASE WHEN json_valid(attrs_json) THEN 1 ELSE 0 END) AS attrs_json_ok,
                          sum(CASE WHEN TRY_CAST(uid AS UUID) IS NOT NULL THEN 1 ELSE 0 END) AS uid_ok
                   FROM read_parquet('{f}')");
    let row = basics["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "4", "row count");
    assert_eq!(
        row[1].as_str().unwrap(),
        "4",
        "all jsonb columns must be valid JSON"
    );
    assert_eq!(
        row[2].as_str().unwrap(),
        "4",
        "all json columns must be valid JSON"
    );
    assert_eq!(
        row[3].as_str().unwrap(),
        "4",
        "all uuid strings must cast to UUID"
    );

    // 4) tz-aware vs naive timestamp semantics. Both columns hold the same
    //    wall clock '2035-08-07 09:08:07.987654' on id=1, but the tz column
    //    must be UTC in DuckDB while the naive column has no timezone.
    let ts = q(
        "SELECT strftime(created_at, '%Y-%m-%d %H:%M:%S.%f') AS naive,
                       strftime(created_at_tz AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f') AS utc
                FROM read_parquet('{f}') WHERE id = 1",
    );
    let row = ts["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "2035-08-07 09:08:07.987654");
    assert_eq!(row[1].as_str().unwrap(), "2035-08-07 09:08:07.987654");

    // 5) Binary: the bytes recovered by DuckDB must match what we wrote in
    //    the seed (\x00ff012345 → hex 00FF012345).
    let blob = q("SELECT lower(to_hex(raw_bytes)) FROM read_parquet('{f}') WHERE id = 1");
    assert_eq!(blob["rows"][0][0].as_str().unwrap(), "00ff012345");

    // 6) Lists: total element counts across rows for tags + nums.
    let lists = q("SELECT sum(len(tags)) AS t, sum(len(nums)) AS n FROM read_parquet('{f}')");
    let row = lists["rows"][0].as_array().unwrap();
    assert_eq!(
        row[0].as_str().unwrap(),
        "4",
        "tags total = alpha,beta,gamma,only"
    );
    assert_eq!(row[1].as_str().unwrap(), "8", "nums total elements");

    // 7) Nullability propagates. `note_all_null` must be NULL for every row.
    let nulls =
        q("SELECT sum(CASE WHEN note_all_null IS NULL THEN 1 ELSE 0 END) FROM read_parquet('{f}')");
    assert_eq!(nulls["rows"][0][0].as_str().unwrap(), "4");

    // 8) Interval was serialised as ISO 8601 string.
    let iv = q("SELECT interval_col FROM read_parquet('{f}') WHERE id = 1");
    assert_eq!(iv["rows"][0][0].as_str().unwrap(), "P1Y2M3D");
}

// ─── MySQL matrix → Parquet → DuckDB ───────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql + duckdb"]
fn duckdb_validates_mysql_type_matrix_parquet() {
    require_alive(LiveService::Mysql);
    require_alive(LiveService::DuckDb);

    let table_name = unique_name("dd_my");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("dd_my_out"));
    run_mysql_matrix_export(&table_name, "parquet", &host_dir);

    let glob = format!("{container_dir}/*.parquet");
    let q = |sql: &str| duckdb_run_sql_json(&sql.replace("{f}", &glob));

    // 1) DuckDB-side autoload types. UNSIGNED integer widening is the
    //    interesting MySQL story (TINYINT UNSIGNED → SMALLINT in DuckDB,
    //    BIGINT UNSIGNED → UBIGINT — not silent truncation).
    let described = q("DESCRIBE SELECT * FROM read_parquet('{f}')");
    let actual = duckdb_parse_describe(&described);
    let expected = [
        ("c_tinyint", "SMALLINT"),
        ("c_tinyint_u", "SMALLINT"),
        ("c_smallint_u", "INTEGER"),
        ("c_int_u", "BIGINT"),
        ("c_bigint_u", "UBIGINT"),
        ("c_bool", "BOOLEAN"),
        ("amount", "DECIMAL(18,2)"),
        ("fee", "DECIMAL(20,6)"),
        ("price", "DECIMAL(10,2)"),
        ("created_at_dt", "TIMESTAMP"),
        ("created_at_ts", "TIMESTAMP WITH TIME ZONE"),
        ("raw_bytes", "BLOB"),
        ("var_bytes", "BLOB"),
        ("blob_bytes", "BLOB"),
        ("year_col", "SMALLINT"),
        ("c_bit1", "BOOLEAN"),
        ("c_bit8", "BIGINT"),
    ];
    for (col, want) in expected {
        let got = actual
            .get(col)
            .unwrap_or_else(|| panic!("duckdb did not see column `{col}`; saw: {actual:?}"));
        assert_eq!(
            got, want,
            "duckdb autoload type for `{col}`: expected {want}, got {got}"
        );
    }

    // 2) UNSIGNED edge values: 0xFF / 0xFFFF / 0xFFFFFFFF / 0xFFFFFFFFFFFFFFFF
    //    survive round-trip.  This is where naive int widening would lose data.
    let edge = q("SELECT c_tinyint_u, c_smallint_u, c_int_u, c_bigint_u
                  FROM read_parquet('{f}') WHERE id = 1");
    let row = edge["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "255");
    assert_eq!(row[1].as_str().unwrap(), "65535");
    assert_eq!(row[2].as_str().unwrap(), "4294967295");
    assert_eq!(
        row[3].as_str().unwrap(),
        "18446744073709551615",
        "BIGINT UNSIGNED max — must NOT round-trip as i64 overflow"
    );

    // 3) Decimal sums (same constants as the in-process roundtrip test).
    let sums =
        q("SELECT sum(amount * 100)::BIGINT, sum(fee * 1000000)::BIGINT FROM read_parquet('{f}')");
    let row = sums["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "99999999990024");
    assert_eq!(row[1].as_str().unwrap(), "10000003");

    // 4) JSON + UUID validity / parseability across every row.
    let basics = q("SELECT count(*) AS n,
                          sum(CASE WHEN json_valid(extras) THEN 1 ELSE 0 END) AS json_ok,
                          sum(CASE WHEN TRY_CAST(uid AS UUID) IS NOT NULL THEN 1 ELSE 0 END) AS uid_ok
                   FROM read_parquet('{f}')");
    let row = basics["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "4");
    assert_eq!(row[1].as_str().unwrap(), "4");
    assert_eq!(row[2].as_str().unwrap(), "4");

    // 5) Binary BLOB round-trips byte-for-byte. Seed for id=1 is BINARY(4)
    //    with 0x00FF0123 (4 bytes, no length-prefix).
    let blob = q("SELECT lower(to_hex(raw_bytes)),
                         lower(to_hex(blob_bytes))
                  FROM read_parquet('{f}') WHERE id = 1");
    let row = blob["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "00ff0123");
    assert_eq!(row[1].as_str().unwrap(), "cafebabe");

    // 6) bit(1) and bit(8) survived as boolean / bigint with the right values.
    let bits = q("SELECT c_bit1, c_bit8 FROM read_parquet('{f}') WHERE id = 1");
    let row = bits["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "True");
    assert_eq!(row[1].as_str().unwrap(), "255");

    // 7) tz-aware vs naive timestamp on id=1.
    let ts = q("SELECT strftime(created_at_dt, '%Y-%m-%d %H:%M:%S.%f'),
                       strftime(created_at_ts AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S.%f')
                FROM read_parquet('{f}') WHERE id = 1");
    let row = ts["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "2035-08-07 09:08:07.987654");
    assert_eq!(row[1].as_str().unwrap(), "2035-08-07 09:08:07.987654");
}

// ─── SQL Server matrix → Parquet → DuckDB ──────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql + duckdb"]
fn duckdb_validates_mssql_type_matrix_parquet() {
    require_alive(LiveService::Mssql);
    require_alive(LiveService::DuckDb);

    let table_name = unique_name("dd_ms");
    setup_mssql_matrix_table(&table_name);
    let _guard = MssqlCleanup(table_name.clone());

    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("dd_ms_out"));
    run_mssql_matrix_export(&table_name, "parquet", &host_dir);

    let glob = format!("{container_dir}/*.parquet");
    let q = |sql: &str| duckdb_run_sql_json(&sql.replace("{f}", &glob));

    // 1) DuckDB-side autoload types. The interesting MSSQL stories: tinyint
    //    (0-255) widens to SMALLINT, bit → BOOLEAN, uniqueidentifier lands as
    //    native UUID (FixedSizeBinary(16) + arrow.uuid), decimal scale survives.
    let described = q("DESCRIBE SELECT * FROM read_parquet('{f}')");
    let actual = duckdb_parse_describe(&described);
    let expected = [
        ("id", "BIGINT"),
        ("c_smallint", "SMALLINT"),
        ("c_int", "INTEGER"),
        ("c_bigint", "BIGINT"),
        ("c_tinyint", "SMALLINT"),
        ("c_bit", "BOOLEAN"),
        ("amount", "DECIMAL(18,2)"),
        ("fee", "DECIMAL(20,6)"),
        ("price", "DECIMAL(10,2)"),
        ("c_real", "FLOAT"),
        ("c_float", "DOUBLE"),
        ("c_date", "DATE"),
        ("c_time", "TIME"),
        ("created_at", "TIMESTAMP"),
        // datetimeoffset is tz-aware → Arrow Timestamp(µs, UTC) → DuckDB reads
        // the isAdjustedToUTC flag and lands it as a TIMESTAMPTZ (vs the naive
        // datetime2 above), proving the tz-awareness survived the Parquet.
        ("created_at_tz", "TIMESTAMP WITH TIME ZONE"),
        ("label", "VARCHAR"),
        ("c_varchar", "VARCHAR"),
        ("c_char", "VARCHAR"),
        ("raw_bytes", "BLOB"),
        ("uid", "UUID"),
        ("c_nvarchar", "VARCHAR"),
    ];
    for (col, want) in expected {
        let got = actual
            .get(col)
            .unwrap_or_else(|| panic!("duckdb did not see column `{col}`; saw: {actual:?}"));
        assert_eq!(
            got, want,
            "duckdb autoload type for `{col}`: expected {want}, got {got}"
        );
    }

    // 2) Aggregates survive: 3 rows, signed bigint sums to zero (+9e9, -9e9, 0),
    //    decimal scale preserved (1234.56 - 0.01 + 0.00).
    let agg = q("SELECT count(*), sum(c_bigint), sum(amount)
                 FROM read_parquet('{f}')");
    let row = agg["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "3");
    assert_eq!(row[1].as_str().unwrap(), "0");
    assert_eq!(
        row[2].as_str().unwrap(),
        "1234.55",
        "decimal scale must survive (1234.56 - 0.01 + 0.00)"
    );

    // 3) uniqueidentifier round-trips as a parseable native UUID, unicode text
    //    is byte-exact, and NULLs land NULL (note_all_null all-null).
    let r1 = q("SELECT uid::VARCHAR, c_nvarchar, note_all_null
                FROM read_parquet('{f}') WHERE id = 1");
    let row = r1["rows"][0].as_array().unwrap();
    assert_eq!(
        row[0].as_str().unwrap().to_lowercase(),
        "6f9619ff-8b86-d011-b42d-00c04fc964ff"
    );
    assert_eq!(row[1].as_str().unwrap(), "héllo wörld");
    assert!(row[2].is_null(), "note_all_null must be NULL");

    // 4) datetimeoffset lands as its UTC INSTANT (the offset is applied, not
    //    dropped). Asserted via epoch_us so the check is independent of DuckDB's
    //    session timezone (the rendered string would vary; the instant does not):
    //    +05:30 wall clock 13:45:30 → 08:15:30 UTC, negative −08:00 00:00:00 →
    //    08:00:00 UTC, and a NULL stays NULL.
    let dto = q("SELECT id, epoch_us(created_at_tz) AS us
                 FROM read_parquet('{f}') ORDER BY id");
    let rows = dto["rows"].as_array().unwrap();
    assert_eq!(
        rows[0].as_array().unwrap()[1].as_str().unwrap(),
        "1768464930123456",
        "datetimeoffset id=1 (+05:30) must round-trip as its UTC instant"
    );
    assert_eq!(
        rows[1].as_array().unwrap()[1].as_str().unwrap(),
        "946713600000000",
        "datetimeoffset id=2 (negative −08:00 offset) must round-trip as its UTC instant"
    );
    assert!(
        rows[2].as_array().unwrap()[1].is_null(),
        "datetimeoffset NULL must stay NULL"
    );
}
