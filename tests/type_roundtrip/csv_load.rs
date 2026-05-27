//! Live CSV validation via DuckDB `read_csv_auto` (ADR-0014 §4 extended).
//!
//! Parquet carries type schema; CSV does not. So this file's contract is
//! narrower than the Parquet validators: we only check that the serialised
//! text is *parseable by a downstream consumer with the right answer* —
//! decimals come back as the same number, JSON stays valid, UUIDs parse,
//! Unicode survives, embedded newlines / quotes / commas are escaped per
//! RFC 4180, nulls land as empty cells.
//!
//! Known CSV-serialisation gaps (pinned here so any change is deliberate):
//!
//!   * `TIMESTAMP WITH TIME ZONE` serialises without a `Z` / `+00:00`
//!     suffix — DuckDB autodetects `TIMESTAMP` (no tz). The Parquet
//!     contract still carries the tz; if CSV needs it, an explicit
//!     output format is required (future work).
//!   * Decimals come back as `DOUBLE` after autodetect — Rivet writes
//!     exact decimal text, but CSV has no type schema, so DuckDB's
//!     guesser picks DOUBLE for any numeric-looking column.
//!     Values still compare bit-exact below 2^53 magnitude; for full
//!     decimal fidelity the consumer should use a typed schema hint.

use crate::common::*;

use super::helpers::{PgCleanup, run_pg_matrix_export, setup_pg_matrix_table};

// ─── PostgreSQL matrix → CSV → DuckDB ──────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn duckdb_validates_postgres_csv_serialization() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);

    let table_name = unique_name("ddcsv_pg");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("ddcsv_pg_out"));
    run_pg_matrix_export(&table_name, "csv", &host_dir);
    let glob = format!("{container_dir}/*.csv");
    let q = |sql: &str| duckdb_run_sql_json(&sql.replace("{f}", &glob));

    // 1) Decimals: DuckDB autodetect returns DOUBLE for numeric-looking
    //    columns since CSV has no type schema. Values must still parse
    //    exactly for the magnitudes in our seed (≤ 2^53), so the sums
    //    used by the in-process Arrow test match here too.
    let sums = q(
        "SELECT (sum(amount * 100))::BIGINT, (sum(fee * 1000000))::BIGINT FROM read_csv_auto('{f}', header=True)",
    );
    let row = sums["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "99999999990024");
    assert_eq!(row[1].as_str().unwrap(), "10000003");

    // 2) JSON columns serialised as valid JSON. RFC-4180 quoting must
    //    survive the round-trip (double-quotes inside the field).
    let json_ok = q("SELECT
        sum(CASE WHEN json_valid(attrs) THEN 1 ELSE 0 END) AS attrs_ok,
        sum(CASE WHEN json_valid(attrs_json) THEN 1 ELSE 0 END) AS j_ok
        FROM read_csv_auto('{f}', header=True)");
    let row = json_ok["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "4", "attrs valid JSON after CSV");
    assert_eq!(
        row[1].as_str().unwrap(),
        "4",
        "attrs_json valid JSON after CSV"
    );

    // 3) UUID parseability across every row.
    let uuid_ok = q(
        "SELECT sum(CASE WHEN TRY_CAST(uid AS UUID) IS NOT NULL THEN 1 ELSE 0 END) FROM read_csv_auto('{f}', header=True)",
    );
    assert_eq!(uuid_ok["rows"][0][0].as_str().unwrap(), "4");

    // 4) Booleans + null handling. Plain `true` / `false` in the cell,
    //    empty cell for NULL.
    let nulls = q("SELECT
        sum(CASE WHEN c_bool THEN 1 ELSE 0 END) AS trues,
        sum(CASE WHEN note_all_null IS NULL THEN 1 ELSE 0 END) AS all_null,
        sum(CASE WHEN note_nullable IS NULL THEN 1 ELSE 0 END) AS some_null
        FROM read_csv_auto('{f}', header=True)");
    let row = nulls["rows"][0].as_array().unwrap();
    assert_eq!(row[0].as_str().unwrap(), "2", "two `true` rows in seed");
    assert_eq!(row[1].as_str().unwrap(), "4", "note_all_null all NULL");
    assert_eq!(row[2].as_str().unwrap(), "1", "note_nullable NULL on id=3");

    // 5) Embedded newline / quote / comma + unicode survive RFC-4180 quoting.
    let escapes = q("SELECT id, note_nullable FROM read_csv_auto('{f}', header=True) ORDER BY id");
    let rows = escapes["rows"].as_array().unwrap();
    // id=1 has embedded newline; id=2 has both quote and comma; id=4 unicode.
    let r1 = rows[0].as_array().unwrap();
    assert_eq!(r1[1].as_str().unwrap(), "line1\nline2");
    let r2 = rows[1].as_array().unwrap();
    assert_eq!(r2[1].as_str().unwrap(), "quote \"and\", comma");
    let r4 = rows[3].as_array().unwrap();
    assert_eq!(r4[1].as_str().unwrap(), "unicode: 日本語 🚀");

    // 6) Timestamp wall clock parses (DuckDB drops the tz on autodetect
    //    because the serialised value carries no offset — see header doc).
    let ts = q(
        "SELECT strftime(created_at_tz, '%Y-%m-%d %H:%M:%S.%f') FROM read_csv_auto('{f}', header=True) WHERE id = 1",
    );
    assert_eq!(
        ts["rows"][0][0].as_str().unwrap(),
        "2035-08-07 09:08:07.987654"
    );
}
