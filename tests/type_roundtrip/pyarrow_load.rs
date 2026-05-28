//! Live Parquet validation via pyarrow (ADR-0014 §4, third oracle).
//!
//! Companion to [`super::duckdb_load`] and [`super::clickhouse_load`]:
//! reads the same PG / MySQL matrix Parquet through the reference Arrow
//! implementation. Two things pyarrow catches that the other two cannot:
//!
//!   * **Field metadata**: pyarrow returns the Arrow schema with its
//!     `metadata` field populated *exactly* as Rivet wrote it
//!     (`b'rivet.native_type' → b'numeric'` etc.). DuckDB drops Arrow
//!     metadata on autoload; ClickHouse never reads it.
//!   * **Row-group statistics**: per-column `min`, `max`, `null_count`,
//!     `distinct_count` (if computed) from the Parquet footer. These are
//!     what query engines use for predicate pushdown — silently broken
//!     stats are a real performance regression that values-only tests
//!     never see.
//!
//! Lives next to the other validators so the docker-compose `rivet-duckdb`
//! container (which has both `duckdb` *and* `pyarrow` installed) does
//! double duty.

use crate::common::*;

use super::helpers::{
    MysqlCleanup, PgCleanup, run_mysql_matrix_export, run_pg_matrix_export,
    setup_mysql_matrix_table, setup_pg_matrix_table,
};

// ─── PostgreSQL matrix ─────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + duckdb (pyarrow)"]
fn pyarrow_validates_postgres_field_metadata_and_stats() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);

    let table_name = unique_name("pa_pg");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("pa_pg_out"));
    run_pg_matrix_export(&table_name, "parquet", &host_dir);
    let glob = format!("{container_dir}/*.parquet");

    let py = format!(
        r#"
import json, glob, sys
import pyarrow.parquet as pq

paths = sorted(glob.glob('{glob}'))
assert paths, "no parquet matched"
table = pq.read_table(paths[0])
schema = table.schema

# 1) every field's rivet.* metadata, as a dict {{column: {{key: value}}}}
fields = {{}}
for f in schema:
    md = {{}}
    if f.metadata:
        for k, v in f.metadata.items():
            ks = k.decode("utf-8")
            if ks.startswith("rivet."):
                md[ks] = v.decode("utf-8")
    fields[f.name] = {{"type": str(f.type), "nullable": f.nullable, "rivet_md": md}}

# 2) row-group statistics: min / max / null_count per column
pqf = pq.ParquetFile(paths[0])
stats = {{}}
for rg_idx in range(pqf.num_row_groups):
    rg = pqf.metadata.row_group(rg_idx)
    for i in range(rg.num_columns):
        col = rg.column(i)
        name = col.path_in_schema
        s = col.statistics
        if s is None:
            stats[name] = None
            continue
        # pyarrow refuses to materialise min/max for some logical types
        # (e.g. Decimal256, certain LIST inner cells). Capture what we can,
        # leave min/max as None on extraction failure — the test still asserts
        # null_count and has_min_max which always work.
        smin = smax = None
        if s.has_min_max:
            try:
                smin = str(s.min)
                smax = str(s.max)
            except Exception:
                pass
        stats[name] = {{
            "has_min_max": bool(s.has_min_max),
            "null_count": s.null_count,
            "min": smin,
            "max": smax,
        }}

print(json.dumps({{"fields": fields, "stats": stats}}))
"#,
    );
    let v = duckdb_run_python_json(&py);
    let fields = &v["fields"];
    let stats = &v["stats"];

    // ─── Field metadata: rivet.* keys reach the Parquet footer ────────────
    let assert_md = |col: &str, native: &str, fidelity: &str, logical: Option<&str>| {
        let md = &fields[col]["rivet_md"];
        assert_eq!(
            md["rivet.native_type"].as_str().unwrap_or("<missing>"),
            native,
            "{col} rivet.native_type"
        );
        assert_eq!(
            md["rivet.fidelity"].as_str().unwrap_or("<missing>"),
            fidelity,
            "{col} rivet.fidelity"
        );
        match logical {
            Some(want) => assert_eq!(
                md["rivet.logical_type"].as_str().unwrap_or("<missing>"),
                want,
                "{col} rivet.logical_type"
            ),
            None => assert!(
                md.get("rivet.logical_type").is_none(),
                "{col} must not carry rivet.logical_type"
            ),
        }
    };

    // Hit the load-bearing rows. The exhaustive metadata-pin lives in
    // `parquet_metadata.rs`; pyarrow's role here is *proof of path* — show
    // that the metadata reaches a fully external Arrow reader unmodified.
    assert_md("amount", "numeric", "exact", None);
    assert_md("uid", "uuid", "exact", Some("uuid"));
    assert_md("attrs", "jsonb", "logical_string", Some("json"));
    assert_md("attrs_json", "json", "logical_string", Some("json"));
    assert_md("interval_col", "interval", "compatible", Some("interval"));

    // ─── Row-group statistics sanity ──────────────────────────────────────
    // Every leaf column must have stats (Parquet writer default).
    for col in ["id", "amount", "fee", "price", "label", "uid", "c_bool"] {
        let s = &stats[col];
        assert!(
            !s.is_null(),
            "column `{col}` has no row-group statistics — Parquet pruning broken"
        );
    }

    // `note_all_null`: null_count must be exactly 4 (the row count).
    let nall = &stats["note_all_null"];
    assert_eq!(
        nall["null_count"].as_i64().unwrap_or(-1),
        4,
        "note_all_null null_count: {nall:?}"
    );
    // Min/max are unset for an all-null column — pyarrow has_min_max == false.
    assert!(
        !nall["has_min_max"].as_bool().unwrap_or(true),
        "note_all_null must report has_min_max=false"
    );

    // `id` stats: min=1, max=4 from the seed. Parquet writes them as the
    // physical INT64 value, which pyarrow surfaces as Python int → str.
    let id_stats = &stats["id"];
    assert_eq!(id_stats["min"].as_str().unwrap(), "1");
    assert_eq!(id_stats["max"].as_str().unwrap(), "4");
    assert_eq!(id_stats["null_count"].as_i64().unwrap(), 0);

    // `note_nullable`: exactly one row is NULL (id=3).
    assert_eq!(
        stats["note_nullable"]["null_count"].as_i64().unwrap(),
        1,
        "note_nullable null_count"
    );
}

// ─── MySQL matrix ──────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql + duckdb (pyarrow)"]
fn pyarrow_validates_mysql_field_metadata_and_stats() {
    require_alive(LiveService::Mysql);
    require_alive(LiveService::DuckDb);

    let table_name = unique_name("pa_my");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("pa_my_out"));
    run_mysql_matrix_export(&table_name, "parquet", &host_dir);
    let glob = format!("{container_dir}/*.parquet");

    let py = format!(
        r#"
import json, glob
import pyarrow.parquet as pq

paths = sorted(glob.glob('{glob}'))
table = pq.read_table(paths[0])
pqf = pq.ParquetFile(paths[0])

fields = {{}}
for f in table.schema:
    md = {{}}
    if f.metadata:
        for k, v in f.metadata.items():
            ks = k.decode("utf-8")
            if ks.startswith("rivet."):
                md[ks] = v.decode("utf-8")
    fields[f.name] = md

stats = {{}}
rg = pqf.metadata.row_group(0)
for i in range(rg.num_columns):
    col = rg.column(i)
    s = col.statistics
    smin = smax = None
    if s and s.has_min_max:
        try:
            smin = str(s.min)
            smax = str(s.max)
        except Exception:
            pass
    stats[col.path_in_schema] = {{
        "null_count": (s.null_count if s else None),
        "has_min_max": (bool(s.has_min_max) if s else False),
        "min": smin,
        "max": smax,
    }}
print(json.dumps({{"fields": fields, "stats": stats}}))
"#,
    );
    let v = duckdb_run_python_json(&py);
    let fields = &v["fields"];
    let stats = &v["stats"];

    // ENUM/SET — these are the columns the MySQL-driver fix turned from
    // String into Enum. Verify pyarrow sees the `rivet.logical_type=enum`
    // metadata on both, otherwise the fix did not actually persist.
    assert_eq!(
        fields["enum_col"]["rivet.logical_type"].as_str().unwrap(),
        "enum",
        "enum_col must carry rivet.logical_type=enum"
    );
    assert_eq!(
        fields["enum_col"]["rivet.native_type"].as_str().unwrap(),
        "enum"
    );
    assert_eq!(
        fields["set_col"]["rivet.logical_type"].as_str().unwrap(),
        "enum"
    );
    assert_eq!(
        fields["set_col"]["rivet.native_type"].as_str().unwrap(),
        "set"
    );

    // BIGINT UNSIGNED edge value reaches the Parquet column stats max as
    // 2^64-1. The numeric Parquet stats are written using the column's
    // physical type (INT64 with is_signed=false for UInt64), which pyarrow
    // surfaces as the unsigned value via `s.max`.
    let big = &stats["c_bigint_u"];
    assert_eq!(
        big["max"].as_str().unwrap(),
        "18446744073709551615",
        "c_bigint_u row-group max must be UINT64 max, not i64 overflow"
    );
    assert_eq!(big["null_count"].as_i64().unwrap(), 0);

    // Unsigned smaller widths: max is the all-ones boundary in the seed.
    assert_eq!(stats["c_tinyint_u"]["max"].as_str().unwrap(), "255");
    assert_eq!(stats["c_smallint_u"]["max"].as_str().unwrap(), "65535");
    assert_eq!(stats["c_int_u"]["max"].as_str().unwrap(), "4294967295");

    // BIT(1) and BIT(8) — provenance metadata distinguishes them now.
    assert_eq!(
        fields["c_bit1"]["rivet.native_type"].as_str().unwrap(),
        "bit(1)"
    );
    assert_eq!(
        fields["c_bit8"]["rivet.native_type"].as_str().unwrap(),
        "bit"
    );

    // BINARY vs VARBINARY distinguished after the driver fix.
    assert_eq!(
        fields["raw_bytes"]["rivet.native_type"].as_str().unwrap(),
        "binary"
    );
    assert_eq!(
        fields["var_bytes"]["rivet.native_type"].as_str().unwrap(),
        "varbinary"
    );
    assert_eq!(
        fields["blob_bytes"]["rivet.native_type"].as_str().unwrap(),
        "blob"
    );
}
