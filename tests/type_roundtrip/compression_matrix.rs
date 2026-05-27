//! Parquet compression-codec matrix (ADR-0014 §4 extended).
//!
//! For each supported codec — `zstd`, `snappy`, `gzip`, `none` — re-export
//! the PG matrix and replay it through DuckDB. Two reasons:
//!
//!   * **Decoder coverage** — a regression in arrow-rs `WriterProperties` or
//!     in the underlying compression crate could land a Parquet that loads
//!     fine in the writer's own reader but not in DuckDB / pyarrow.
//!   * **No silent value drift** — every codec must reach the same decimal
//!     sums, count, and row contents. Compression is supposed to be lossless;
//!     a value-level diff between codecs means an encoding bug.
//!
//! `lz4` is intentionally skipped — it is supported by Rivet but DuckDB's
//! Parquet reader rejects the legacy `LZ4_RAW` page format used by parquet-rs
//! by default. The other three codecs cover production reality.

use crate::common::*;

use super::helpers::{PgCleanup, run_pg_matrix_export_with_compression, setup_pg_matrix_table};

#[test]
#[ignore = "live: requires docker compose postgres + duckdb"]
fn duckdb_validates_postgres_parquet_compression_codecs() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);

    let table_name = unique_name("cmp_pg");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    // Expected values across every codec — these are the same constants used
    // by the zstd-only PG validator, recomputed independently per codec to
    // catch encoding/decoding drift.
    const EXPECTED_AMOUNT_SUM: &str = "99999999990024";
    const EXPECTED_FEE_SUM: &str = "10000003";
    const EXPECTED_COUNT: &str = "4";

    for codec in ["zstd", "snappy", "gzip", "none"] {
        let (host_dir, container_dir) =
            duckdb_shared_workdir(&format!("{}_{codec}", unique_name("cmp_pg_out")));
        run_pg_matrix_export_with_compression(&table_name, codec, &host_dir);
        let glob = format!("{container_dir}/*.parquet");
        let q = |sql: &str| duckdb_run_sql_json(&sql.replace("{f}", &glob));

        let res = q("SELECT count(*) AS n,
                            (sum(amount * 100))::BIGINT AS sa,
                            (sum(fee * 1000000))::BIGINT AS sf
                     FROM read_parquet('{f}')");
        let row = res["rows"][0].as_array().unwrap();
        assert_eq!(
            row[0].as_str().unwrap(),
            EXPECTED_COUNT,
            "row count mismatch under codec `{codec}`"
        );
        assert_eq!(
            row[1].as_str().unwrap(),
            EXPECTED_AMOUNT_SUM,
            "amount sum mismatch under codec `{codec}`"
        );
        assert_eq!(
            row[2].as_str().unwrap(),
            EXPECTED_FEE_SUM,
            "fee sum mismatch under codec `{codec}`"
        );

        // Sanity: a load-bearing row-1 lookup — confirms the data block
        // round-trips byte-for-byte, not just aggregates.
        let r1 = q("SELECT uid, lower(to_hex(raw_bytes)) AS rb, interval_col
                    FROM read_parquet('{f}') WHERE id = 1");
        let row1 = r1["rows"][0].as_array().unwrap();
        assert_eq!(
            row1[0].as_str().unwrap(),
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011",
            "uid mismatch under codec `{codec}`"
        );
        assert_eq!(
            row1[1].as_str().unwrap(),
            "00ff012345",
            "raw_bytes mismatch under codec `{codec}`"
        );
        assert_eq!(
            row1[2].as_str().unwrap(),
            "P1Y2M3D",
            "interval_col mismatch under codec `{codec}`"
        );
    }
}
