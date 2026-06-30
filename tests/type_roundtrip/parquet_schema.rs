//! Parquet *schema-level* contracts (physical + logical types).
//!
//! `*_load.rs` tests verify what downstream engines see after autoload.
//! These tests verify what Rivet *writes* into the Parquet file header — the
//! `PrimitiveType` / `LogicalType` for every column. Two reasons that matters
//! independently of value-level round-trips:
//!
//!   * Engines that ignore Arrow metadata still pick up Parquet logical types
//!     (e.g. `DECIMAL`, `TIMESTAMP_MICROS isAdjustedToUTC`, `UUID`, `JSON`).
//!   * An accidental change in `arrow-rs` / `parquet-rs` versions could
//!     silently switch a column from `INT64 + DECIMAL` to `BYTE_ARRAY` and
//!     all our value-level assertions would still pass — the downstream
//!     engines would just decode bytes differently. This pin catches that.
//!
//! Current findings (v0.7.8, arrow/parquet 58):
//!
//!   * Decimals → `INT64`/`FIXED_LEN_BYTE_ARRAY` + `DECIMAL(p,s)` ✓
//!   * `timestamptz` → `INT64 + TIMESTAMP(isAdjustedToUTC=true, MICROS)` ✓
//!   * `timestamp` naive → `INT64 + TIMESTAMP(isAdjustedToUTC=false, MICROS)` ✓
//!   * `date` → `INT32 + DATE`; `time` → `INT64 + TIME_MICROS` ✓
//!   * binary → `BYTE_ARRAY` (no logical type) ✓
//!   * bool → `BOOLEAN` (no logical type) ✓
//!   * **uuid → `FIXED_LEN_BYTE_ARRAY(16) + Uuid`** — via the Arrow
//!     `arrow.uuid` canonical extension on a `FixedSizeBinary(16)` field,
//!     parquet-rs emits the native `LogicalType::Uuid`. Downstream
//!     autodetect (DuckDB / ClickHouse / BigQuery) recovers the UUID type
//!     without an explicit cast.
//!   * **json → `BYTE_ARRAY + Json`** — same wiring with the `arrow.json`
//!     canonical extension on `Utf8`. The on-disk bytes are unchanged
//!     versus the previous `String` representation; only the logical-type
//!     annotation in the Parquet footer differs.

use std::collections::HashMap;
use std::path::Path;

use parquet::basic::{LogicalType, Type as PhysicalType};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::common::*;

use super::helpers::{
    MysqlCleanup, PgCleanup, run_mysql_matrix_export, run_pg_matrix_export,
    setup_mysql_matrix_table, setup_pg_matrix_table,
};

/// Map column dot-path → (physical, logical) read out of the Parquet footer.
fn read_column_types(path: &Path) -> HashMap<String, (PhysicalType, Option<LogicalType>)> {
    let bytes = std::fs::read(path).expect("read parquet file");
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).expect("open parquet reader");
    let descr = reader.metadata().file_metadata().schema_descr();
    let mut out = HashMap::new();
    for i in 0..descr.num_columns() {
        let col = descr.column(i);
        out.insert(
            col.path().string(),
            (col.physical_type(), col.logical_type_ref().cloned()),
        );
    }
    out
}

fn assert_col(
    cols: &HashMap<String, (PhysicalType, Option<LogicalType>)>,
    path: &str,
    expected_phys: PhysicalType,
    expected_logical: Option<LogicalType>,
) {
    let (phys, logical) = cols
        .get(path)
        .unwrap_or_else(|| panic!("column `{path}` missing; saw: {:?}", cols.keys()));
    assert_eq!(
        *phys, expected_phys,
        "physical type for `{path}`: expected {expected_phys:?}, got {phys:?}"
    );
    assert_eq!(
        logical, &expected_logical,
        "logical type for `{path}`: expected {expected_logical:?}, got {logical:?}"
    );
}

// ─── PostgreSQL matrix ─────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn parquet_schema_pins_postgres_matrix_logical_types() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("psch_pg");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    let out_dir = tempfile::tempdir().unwrap();
    run_pg_matrix_export(&table_name, "parquet", out_dir.path());
    let files = files_with_extension(out_dir.path(), "parquet");
    let cols = read_column_types(&files[0]);

    use LogicalType::*;
    use PhysicalType::*;

    // Integers
    assert_col(&cols, "id", INT64, None);
    assert_col(
        &cols,
        "c_smallint",
        INT32,
        Some(LogicalType::integer(16, true)),
    );
    assert_col(&cols, "c_integer", INT32, None);
    assert_col(&cols, "c_bigint", INT64, None);

    // Decimals: small precision fits INT64; precision > 18 escalates to FLBA.
    assert_col(&cols, "amount", INT64, Some(LogicalType::decimal(2, 18)));
    assert_col(
        &cols,
        "fee",
        FIXED_LEN_BYTE_ARRAY,
        Some(LogicalType::decimal(6, 20)),
    );
    assert_col(&cols, "price", INT64, Some(LogicalType::decimal(2, 10)));

    // Floats
    assert_col(&cols, "c_real", FLOAT, None);
    assert_col(&cols, "c_double", DOUBLE, None);

    // Temporal
    assert_col(&cols, "c_date", INT32, Some(Date));
    assert_col(
        &cols,
        "c_time",
        INT64,
        Some(LogicalType::time(false, parquet::basic::TimeUnit::MICROS)),
    );
    assert_col(
        &cols,
        "created_at",
        INT64,
        Some(LogicalType::timestamp(
            false,
            parquet::basic::TimeUnit::MICROS,
        )),
    );
    assert_col(
        &cols,
        "created_at_tz",
        INT64,
        Some(LogicalType::timestamp(
            true,
            parquet::basic::TimeUnit::MICROS,
        )),
    );

    // Strings / text
    assert_col(&cols, "label", BYTE_ARRAY, Some(String));
    assert_col(&cols, "c_varchar", BYTE_ARRAY, Some(String));
    assert_col(&cols, "c_bpchar", BYTE_ARRAY, Some(String));
    assert_col(&cols, "large_text", BYTE_ARRAY, Some(String));
    assert_col(&cols, "note_nullable", BYTE_ARRAY, Some(String));
    assert_col(&cols, "note_all_null", BYTE_ARRAY, Some(String));

    // Binary — no logical type by design.
    assert_col(&cols, "raw_bytes", BYTE_ARRAY, None);

    // Boolean
    assert_col(&cols, "c_bool", BOOLEAN, None);

    // UUID + JSON now emit native Parquet logical types via the Arrow
    // `arrow.uuid` / `arrow.json` extension wiring in `build_arrow_field`.
    // Downstream readers (DuckDB, ClickHouse, pyarrow, BigQuery
    // autodetect) recognise these natively without a cast.
    assert_col(&cols, "uid", FIXED_LEN_BYTE_ARRAY, Some(Uuid));
    assert_col(&cols, "attrs", BYTE_ARRAY, Some(Json));
    assert_col(&cols, "attrs_json", BYTE_ARRAY, Some(Json));
    // `enum` / `interval` Parquet logical types exist but require richer
    // metadata than we currently track — leaving them as `String` until
    // there is operator demand. See `docs/type-mapping.md`.
    assert_col(&cols, "enum_col", BYTE_ARRAY, Some(String));
    assert_col(&cols, "interval_col", BYTE_ARRAY, Some(String));

    // Lists — inner column lives under `<col>.list.item` in Parquet (3-level
    // encoding). The List logical type sits on the *outer* group, which the
    // SchemaDescriptor flattens away, so we only check the inner leaves.
    assert_col(&cols, "tags.list.item", BYTE_ARRAY, Some(String));
    assert_col(&cols, "nums.list.item", INT32, None);
}

// ─── MySQL matrix ──────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql"]
fn parquet_schema_pins_mysql_matrix_logical_types() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("psch_my");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let out_dir = tempfile::tempdir().unwrap();
    run_mysql_matrix_export(&table_name, "parquet", out_dir.path());
    let files = files_with_extension(out_dir.path(), "parquet");
    let cols = read_column_types(&files[0]);

    use LogicalType::*;
    use PhysicalType::*;

    // Signed integers — int16 / int32 / int64 stay at their native width.
    assert_col(
        &cols,
        "c_tinyint",
        INT32,
        Some(LogicalType::integer(16, true)),
    );
    assert_col(&cols, "c_bigint", INT64, None);

    // UNSIGNED widening — TINYINT/SMALLINT/INT UNSIGNED widen one step;
    // BIGINT UNSIGNED stays UInt64 (uses `is_signed=false`).
    assert_col(
        &cols,
        "c_tinyint_u",
        INT32,
        Some(LogicalType::integer(16, true)),
    );
    assert_col(&cols, "c_smallint_u", INT32, None);
    assert_col(&cols, "c_int_u", INT64, None);
    assert_col(
        &cols,
        "c_bigint_u",
        INT64,
        Some(LogicalType::integer(64, false)),
    );

    // Decimals identical to PG matrix.
    assert_col(&cols, "amount", INT64, Some(LogicalType::decimal(2, 18)));
    assert_col(
        &cols,
        "fee",
        FIXED_LEN_BYTE_ARRAY,
        Some(LogicalType::decimal(6, 20)),
    );
    assert_col(&cols, "price", INT64, Some(LogicalType::decimal(2, 10)));

    // Temporal — both naive (DATETIME) and tz (TIMESTAMP) map.
    assert_col(
        &cols,
        "created_at_dt",
        INT64,
        Some(LogicalType::timestamp(
            false,
            parquet::basic::TimeUnit::MICROS,
        )),
    );
    assert_col(
        &cols,
        "created_at_ts",
        INT64,
        Some(LogicalType::timestamp(
            true,
            parquet::basic::TimeUnit::MICROS,
        )),
    );

    // Binary family — all three MySQL binary widths land as BYTE_ARRAY.
    assert_col(&cols, "raw_bytes", BYTE_ARRAY, None);
    assert_col(&cols, "var_bytes", BYTE_ARRAY, None);
    assert_col(&cols, "blob_bytes", BYTE_ARRAY, None);

    // BIT — bit(1) → BOOLEAN, bit(n>1) → INT64.
    assert_col(&cols, "c_bit1", BOOLEAN, None);
    assert_col(&cols, "c_bit8", INT64, None);

    // YEAR widens to int16.
    assert_col(
        &cols,
        "year_col",
        INT32,
        Some(LogicalType::integer(16, true)),
    );

    // `extras` is MySQL `JSON` — now emits native `LogicalType::Json`.
    // `uid` here is MySQL `VARCHAR(36)` without an override, so it stays
    // a generic `String`; only PG's native `uuid` (or a MySQL `columns:
    // uid: uuid` override) bumps to `LogicalType::Uuid`.
    assert_col(&cols, "extras", BYTE_ARRAY, Some(Json));
    assert_col(&cols, "uid", BYTE_ARRAY, Some(String));
    assert_col(&cols, "enum_col", BYTE_ARRAY, Some(String));
    assert_col(&cols, "set_col", BYTE_ARRAY, Some(String));
}
