//! End-to-end check that Rivet's `rivet.*` field metadata reaches the
//! Parquet file footer (and is visible to any downstream Arrow reader).
//!
//! Three keys, defined in [`src/types/mapping.rs`]:
//!
//!   * `rivet.native_type`   — the source DB native type label
//!   * `rivet.fidelity`      — `exact` / `compatible` / `logical_string` / …
//!   * `rivet.logical_type`  — `json` / `uuid` / `enum` / `interval` (only on
//!     columns whose physical Arrow type is a generic container like Utf8)
//!
//! `parquet_schema.rs` pins the *Parquet-native* logical types. This file
//! pins the *Rivet-side* logical type and provenance metadata that travels
//! along the Arrow schema embedded in the Parquet `KeyValue` block. Together
//! they form the full type contract.
//!
//! T4 from ADR-0014: plain string columns must NOT carry
//! `rivet.logical_type` — otherwise a downstream consumer can't tell
//! semantic JSON/UUID/enum apart from plain Utf8. This test enforces that
//! invariant on every column.

use std::collections::HashMap;
use std::path::Path;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::common::*;

use super::helpers::{
    MysqlCleanup, PgCleanup, run_mysql_matrix_export, run_pg_matrix_export,
    setup_mysql_matrix_table, setup_pg_matrix_table,
};

const RIVET_NATIVE_TYPE: &str = "rivet.native_type";
const RIVET_LOGICAL_TYPE: &str = "rivet.logical_type";
const RIVET_FIDELITY: &str = "rivet.fidelity";

fn read_parquet_arrow_schema(path: &Path) -> SchemaRef {
    let bytes = std::fs::read(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
    builder.schema().clone()
}

fn field_metadata<'a>(schema: &'a SchemaRef, name: &str) -> &'a HashMap<String, String> {
    schema
        .field_with_name(name)
        .unwrap_or_else(|_| panic!("schema is missing field `{name}`"))
        .metadata()
}

/// Assert that a field has `rivet.native_type == native`, `rivet.fidelity ==
/// fidelity`, and either matches `logical` (when `Some`) or carries no
/// `rivet.logical_type` key at all (when `None`). The `None` branch is the
/// load-bearing assertion from ADR-0014 T4.
fn assert_field_md(
    schema: &SchemaRef,
    name: &str,
    native: &str,
    fidelity: &str,
    logical: Option<&str>,
) {
    let md = field_metadata(schema, name);
    assert_eq!(
        md.get(RIVET_NATIVE_TYPE).map(String::as_str),
        Some(native),
        "{name} rivet.native_type"
    );
    assert_eq!(
        md.get(RIVET_FIDELITY).map(String::as_str),
        Some(fidelity),
        "{name} rivet.fidelity"
    );
    match logical {
        Some(want) => assert_eq!(
            md.get(RIVET_LOGICAL_TYPE).map(String::as_str),
            Some(want),
            "{name} rivet.logical_type"
        ),
        None => assert!(
            !md.contains_key(RIVET_LOGICAL_TYPE),
            "{name} must NOT carry rivet.logical_type \
             (only json/uuid/enum/interval do); got {:?}",
            md.get(RIVET_LOGICAL_TYPE)
        ),
    }
}

// ─── PostgreSQL matrix ─────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parquet_metadata_pins_postgres_matrix_field_provenance() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("pmd_pg");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type: enum_type.clone(),
    };

    let out_dir = tempfile::tempdir().unwrap();
    run_pg_matrix_export(&table_name, "parquet", out_dir.path());
    let files = files_with_extension(out_dir.path(), "parquet");
    let schema = read_parquet_arrow_schema(&files[0]);

    // (column, native_type as PG reports it, fidelity, optional logical)
    let cases: &[(&str, &str, &str, Option<&str>)] = &[
        ("id", "int8", "exact", None),
        ("c_smallint", "int2", "exact", None),
        ("c_integer", "int4", "exact", None),
        ("c_bigint", "int8", "exact", None),
        ("amount", "numeric", "exact", None),
        ("fee", "numeric", "exact", None),
        ("price", "numeric", "exact", None),
        ("c_real", "float4", "exact", None),
        ("c_double", "float8", "exact", None),
        ("c_date", "date", "exact", None),
        ("c_time", "time", "exact", None),
        ("created_at", "timestamp", "exact", None),
        ("created_at_tz", "timestamptz", "exact", None),
        ("label", "text", "exact", None),
        ("c_varchar", "varchar", "exact", None),
        ("c_bpchar", "bpchar", "exact", None),
        ("raw_bytes", "bytea", "exact", None),
        // UUID fidelity bumped from `compatible` → `exact` after we
        // switched to FixedSizeBinary(16) + native Parquet `LogicalType::Uuid`.
        ("uid", "uuid", "exact", Some("uuid")),
        ("attrs", "jsonb", "logical_string", Some("json")),
        ("attrs_json", "json", "logical_string", Some("json")),
        ("c_bool", "bool", "exact", None),
        ("interval_col", "interval", "compatible", Some("interval")),
        // The enum column has the table-specific PG type name — assert just
        // the fidelity / logical bits here.
        ("large_text", "text", "exact", None),
        ("note_nullable", "text", "exact", None),
        ("note_all_null", "text", "exact", None),
    ];
    for (name, native, fidelity, logical) in cases {
        assert_field_md(&schema, name, native, fidelity, *logical);
    }

    // enum_col: native_type is the PG enum-type name (table-specific).
    let enum_md = field_metadata(&schema, "enum_col");
    assert_eq!(
        enum_md.get(RIVET_NATIVE_TYPE).map(String::as_str),
        Some(enum_type.as_str()),
        "enum_col native_type should be the PG enum type name"
    );
    assert_eq!(
        enum_md.get(RIVET_FIDELITY).map(String::as_str),
        Some("compatible")
    );
    assert_eq!(
        enum_md.get(RIVET_LOGICAL_TYPE).map(String::as_str),
        Some("enum")
    );

    // List columns: top-level field carries Rivet provenance metadata too.
    let tags_md = field_metadata(&schema, "tags");
    assert_eq!(
        tags_md.get(RIVET_FIDELITY).map(String::as_str),
        Some("compatible")
    );
    assert!(
        !tags_md.contains_key(RIVET_LOGICAL_TYPE),
        "list<text> must not get rivet.logical_type"
    );
    let nums_md = field_metadata(&schema, "nums");
    assert_eq!(
        nums_md.get(RIVET_FIDELITY).map(String::as_str),
        Some("compatible")
    );
}

// ─── MySQL matrix ──────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql"]
fn parquet_metadata_pins_mysql_matrix_field_provenance() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("pmd_my");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let out_dir = tempfile::tempdir().unwrap();
    run_mysql_matrix_export(&table_name, "parquet", out_dir.path());
    let files = files_with_extension(out_dir.path(), "parquet");
    let schema = read_parquet_arrow_schema(&files[0]);

    let cases: &[(&str, &str, &str, Option<&str>)] = &[
        ("c_tinyint", "tinyint", "exact", None),
        ("c_tinyint_u", "tinyint unsigned", "exact", None),
        ("c_bool", "tinyint(1)", "exact", None),
        ("c_boolean", "tinyint(1)", "exact", None),
        ("c_smallint", "smallint", "exact", None),
        ("c_smallint_u", "smallint unsigned", "exact", None),
        ("c_int", "int", "exact", None),
        ("c_int_u", "int unsigned", "exact", None),
        ("c_bigint", "bigint", "exact", None),
        ("c_bigint_u", "bigint unsigned", "exact", None),
        ("amount", "decimal", "exact", None),
        ("fee", "decimal", "exact", None),
        ("price", "decimal", "exact", None),
        ("c_float", "float", "exact", None),
        ("c_double", "double", "exact", None),
        ("c_date", "date", "exact", None),
        ("c_time", "time", "exact", None),
        ("created_at_dt", "datetime", "exact", None),
        ("created_at_ts", "timestamp", "exact", None),
        // MySQL string family: `mysql_native_type_name` distinguishes CHAR
        // vs VARCHAR via `column_type()` and BINARY vs VARBINARY via the
        // same OID + `character_set() == 63` (binary). TEXT variants
        // (TEXT/MEDIUMTEXT/LONGTEXT) still all arrive as MYSQL_TYPE_BLOB,
        // so they collapse to `blob` for now — distinguishing them would
        // require the text_blob_size flag which mysql crate does not expose.
        ("label", "varchar", "exact", None),
        ("c_char", "char", "exact", None),
        ("c_text", "blob", "exact", None),
        ("c_varchar", "varchar", "exact", None),
        ("long_text", "blob", "exact", None),
        ("medium_text", "blob", "exact", None),
        ("raw_bytes", "binary", "exact", None),
        ("var_bytes", "varbinary", "exact", None),
        ("blob_bytes", "blob", "exact", None),
        ("uid", "varchar", "exact", None),
        ("extras", "json", "logical_string", Some("json")),
        ("enum_col", "enum", "compatible", Some("enum")),
        ("set_col", "set", "compatible", Some("enum")),
        ("year_col", "year", "exact", None),
        ("c_bit1", "bit(1)", "exact", None),
        ("c_bit8", "bit", "exact", None),
        ("note_nullable", "varchar", "exact", None),
        ("note_all_null", "varchar", "exact", None),
    ];
    for (name, native, fidelity, logical) in cases {
        assert_field_md(&schema, name, native, fidelity, *logical);
    }
}
