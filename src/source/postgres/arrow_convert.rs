//! Postgres → Arrow conversion machinery.
//!
//! Everything that turns a `postgres::Row` (driver type) into an Arrow
//! `RecordBatch` lives here, including:
//!
//! - the `Type → RivetType → DataType` mapping pipeline
//!   (`pg_type_to_rivet`, `rivet_type_for_pg_column`, `pg_columns_to_schema`),
//! - per-cell decoders for INTERVAL (`PgInterval` + ISO 8601 serializer),
//!   UUID (`PgUuidDisplayed`), enum (`AnyAsString`), and NUMERIC (binary
//!   wire decoding via `pg_numeric_optional_*`),
//! - the row → array builders (`rows_to_record_batch_typed`, `build_array`,
//!   `build_pg_list_array`) and decimal helpers
//!   (`pg_numeric_to_decimal128`, `pg_numeric_to_decimal256`).
//!
//! Only three names cross the module boundary back into [`super`]:
//! [`pg_columns_to_schema`] and [`rivet_type_for_pg_column`] are called by
//! the `Source::export` / `Source::type_mappings` impls in `mod.rs`, and
//! [`rows_to_record_batch_typed`] is called by `pg_run_export`. Everything
//! else is private to this file.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, ListBuilder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::Timelike as _;
use postgres::Row;
use postgres::types::{FromSql as PgFromSql, Json, Kind, Type};
use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::source::pg_numeric_wire::{PgNumericWire, numeric_wire_normalized_plain};
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit as RivetTimeUnit, TypeMapping,
    build_arrow_field,
};

// ─── Wire-type adapters ──────────────────────────────────────────────────────

/// PostgreSQL `uuid` rows materialised as their canonical 16-byte form.
///
/// Targets Arrow `FixedSizeBinary(16)` per ADR-0014: with the `arrow.uuid`
/// extension type attached in [`crate::types::mapping::build_arrow_field`],
/// parquet-rs emits native `LogicalType::Uuid` and downstream engines
/// (DuckDB, ClickHouse, pyarrow, BigQuery autodetect) recover UUID
/// semantics without a cast.
///
/// Most servers transmit UUIDs as 16 raw bytes under the binary protocol;
/// the text branch covers the rare client/proxy that surfaces the
/// hyphenated form instead, so we never silently null an export.
#[derive(Clone)]
struct PgUuidBytes([u8; 16]);

impl<'a> PgFromSql<'a> for PgUuidBytes {
    fn accepts(ty: &Type) -> bool {
        ty == &Type::UUID
    }

    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() == 16 {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(raw);
            return Ok(Self(bytes));
        }
        let text = simdutf8::basic::from_utf8(raw)?.trim();
        Ok(Self(*uuid::Uuid::parse_str(text)?.as_bytes()))
    }
}

fn pg_numeric_optional_utf8_string(row: &Row, col_idx: usize) -> Result<Option<String>> {
    match row.try_get::<_, Option<PgNumericWire<'_>>>(col_idx)? {
        None => Ok(None),
        Some(w) => Ok(numeric_raw_to_optional_decimal_text(w.0)),
    }
}

fn numeric_raw_to_optional_decimal_text(raw: &[u8]) -> Option<String> {
    numeric_wire_normalized_plain(raw).or_else(|| {
        let text = simdutf8::basic::from_utf8(raw).ok()?.trim();
        (!text.is_empty()).then(|| text.to_owned())
    })
}

// ─── Type mapping ────────────────────────────────────────────────────────────

/// Map a PostgreSQL wire-protocol type to Rivet's canonical type.
///
/// This is the authoritative PostgreSQL → RivetType function. All other code
/// must go through here rather than constructing Arrow types directly.
///
/// Key decisions vs. the old `pg_type_to_arrow`:
/// - Unbounded server `NUMERIC` (OID only in row metadata) yields `Unsupported`
///   unless overwritten by YAML `columns:` or by `pg_fetch_numeric_catalog_hints`
///   for simple single-table selects (precision/scale from `information_schema`).
/// - `TIMESTAMPTZ` → `Timestamp { timezone: Some("UTC") }` instead of `None`
///   (roadmap §13: TIMESTAMPTZ must carry UTC semantics into Arrow/Parquet).
/// - `UUID` / `JSON` / `JSONB` → `Uuid` / `Json` variants, so `build_arrow_field`
///   attaches the `rivet.logical_type` metadata for downstream consumers.
fn pg_type_to_rivet(t: &Type) -> RivetType {
    match *t {
        Type::BOOL => RivetType::Bool,
        Type::INT2 => RivetType::Int16,
        Type::INT4 => RivetType::Int32,
        Type::INT8 => RivetType::Int64,
        // OID is u32; Int64 is a safe widening and avoids introducing a UInt32
        // variant that has no natural downstream type in most warehouses.
        Type::OID => RivetType::Int64,
        Type::FLOAT4 => RivetType::Float32,
        Type::FLOAT8 => RivetType::Float64,

        // The postgres wire protocol does NOT carry atttypmod (precision/scale)
        // in RowDescription for arbitrary queries — only the OID is available.
        // For unbounded server `NUMERIC`, see [`rivet_type_for_pg_column`] + catalog
        // hints; this arm is the final fallback when no declared precision exists.
        Type::NUMERIC => RivetType::Unsupported {
            native_type: "numeric".into(),
            reason: "precision/scale unavailable from query metadata and catalog lookup; \
                     use a column override (e.g. columns: amount: decimal(18,2)), \
                     or a single-table SELECT ... FROM schema.table \
                     when the DDL declares numeric precision."
                .into(),
        },

        Type::DATE => RivetType::Date,
        Type::TIME => RivetType::Time {
            unit: RivetTimeUnit::Microsecond,
        },
        Type::TIMESTAMP => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: None,
        },
        // Roadmap §13: TIMESTAMPTZ is always normalized to UTC.
        Type::TIMESTAMPTZ => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        },

        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => RivetType::String,
        Type::BYTEA => RivetType::Binary,

        // Roadmap §14: JSON/JSONB → Utf8 + rivet.logical_type=json metadata.
        Type::JSON | Type::JSONB => RivetType::Json,
        // Roadmap §14: UUID → Utf8 + rivet.logical_type=uuid metadata.
        Type::UUID => RivetType::Uuid,

        // Roadmap §13: interval → IntervalMonthDayNano.
        Type::INTERVAL => RivetType::Interval,

        _ => match t.kind() {
            // M6: PG enum → Utf8 + metadata logical=enum.
            Kind::Enum(_) => RivetType::Enum,
            // M6: 1-D arrays → List(inner). Nested arrays fall through to Unsupported.
            Kind::Array(elem_type) => RivetType::List {
                inner: Box::new(pg_type_to_rivet(elem_type)),
            },
            _ => RivetType::Unsupported {
                native_type: t.name().to_string(),
                reason: "no Rivet mapping for this PostgreSQL type".into(),
            },
        },
    }
}

/// Apply per-column overrides + numeric catalog hints on top of the wire-type
/// derived RivetType. The override path always wins; catalog hints are
/// consulted only when no override exists and the wire type is NUMERIC.
pub(super) fn rivet_type_for_pg_column(
    col: &postgres::Column,
    column_overrides: &ColumnOverrides,
    numeric_hints: Option<&HashMap<String, (u8, i8)>>,
) -> RivetType {
    if let Some(t) = column_overrides.get(col.name()) {
        return t.clone();
    }
    if *col.type_() == Type::NUMERIC
        && let Some(hints) = numeric_hints
        && let Some(&(p, s)) = hints.get(col.name())
    {
        return RivetType::Decimal {
            precision: p,
            scale: s,
        };
    }
    pg_type_to_rivet(col.type_())
}

/// Build an Arrow `Schema` from PostgreSQL `Column` descriptors by routing
/// each column through the `SourceColumn → RivetType → TypeMapping → Field`
/// pipeline.
///
/// `column_overrides` takes priority over autodetection: if the user declared
/// e.g. `amount: decimal(18,2)` in `rivet.yaml`, that `RivetType` replaces
/// the autodetected `Unsupported` for `NUMERIC` columns.
///
/// Returns `Err` for any column that has no safe Rivet mapping and no override,
/// rather than silently exporting wrong data as Utf8. When `numeric_catalog_hints`
/// is populated (simple single-table `SELECT … FROM`), declared `numeric(p,s)` from
/// the catalog is merged before falling back to `Unsupported`.
pub(super) fn pg_columns_to_schema(
    columns: &[postgres::Column],
    column_overrides: &ColumnOverrides,
    numeric_catalog_hints: Option<&HashMap<String, (u8, i8)>>,
) -> crate::error::Result<Schema> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();
    for col in columns {
        let rivet = rivet_type_for_pg_column(col, column_overrides, numeric_catalog_hints);
        let source = SourceColumn::simple(col.name(), col.type_().name(), true);
        let mapping = TypeMapping::from_source(&source, rivet);
        match build_arrow_field(&mapping) {
            Some(field) => fields.push(field),
            None => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.as_str(),
                    _ => "no Rivet mapping for this PostgreSQL type",
                };
                errors.push(format!(
                    "  • {} (PG type '{}'): {reason}",
                    col.name(),
                    col.type_().name()
                ));
            }
        }
    }
    if !errors.is_empty() {
        anyhow::bail!(
            "{} column(s) have no safe Rivet mapping — add column overrides in rivet.yaml:\n\
             columns:\n{}",
            errors.len(),
            errors.join("\n")
        );
    }
    Ok(Schema::new(fields))
}

// ─── INTERVAL decoder + ISO 8601 serializer ──────────────────────────────────

/// Reads a PostgreSQL `INTERVAL` value from its 16-byte binary wire format:
///   bytes 0–7  (i64 big-endian): microseconds within day
///   bytes 8–11 (i32 big-endian): days
///   bytes 12–15 (i32 big-endian): months
struct PgInterval {
    microseconds: i64,
    days: i32,
    months: i32,
}

impl<'a> postgres_types::FromSql<'a> for PgInterval {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err(format!("expected 16-byte interval, got {}", raw.len()).into());
        }
        let microseconds = i64::from_be_bytes(raw[0..8].try_into()?);
        let days = i32::from_be_bytes(raw[8..12].try_into()?);
        let months = i32::from_be_bytes(raw[12..16].try_into()?);
        Ok(Self {
            microseconds,
            days,
            months,
        })
    }
    fn accepts(ty: &postgres_types::Type) -> bool {
        *ty == postgres_types::Type::INTERVAL
    }
}

/// Serialise a PostgreSQL INTERVAL to an ISO 8601 duration string.
///
/// Arrow `Interval(MonthDayNano)` cannot be written to Parquet, so we emit
/// Utf8 instead.  The three components map as:
///   months → years + months  (e.g. 14 → "P1Y2M")
///   days   → days            (e.g. 3  → "3D")
///   µs     → T…H…M…S        (e.g. 90_061_000_000 → "T25H1M1S")
fn pg_interval_to_iso8601(months: i32, days: i32, microseconds: i64) -> String {
    use std::fmt::Write as _;
    let years = months / 12;
    let m = months % 12;
    let mut s = String::from("P");
    if years != 0 {
        write!(s, "{years}Y").ok();
    }
    if m != 0 {
        write!(s, "{m}M").ok();
    }
    if days != 0 {
        write!(s, "{days}D").ok();
    }
    if microseconds != 0 {
        let neg = microseconds < 0;
        let abs = microseconds.unsigned_abs();
        let h = abs / 3_600_000_000;
        let r = abs % 3_600_000_000;
        let mi = r / 60_000_000;
        let r2 = r % 60_000_000;
        let sec = r2 / 1_000_000;
        let us = r2 % 1_000_000;
        let sign = if neg { "-" } else { "" };
        s.push('T');
        if h != 0 {
            write!(s, "{sign}{h}H").ok();
        }
        if mi != 0 {
            write!(s, "{sign}{mi}M").ok();
        }
        if us != 0 {
            write!(s, "{sign}{sec}.{us:06}S").ok();
        } else if sec != 0 || (h == 0 && mi == 0) {
            write!(s, "{sign}{sec}S").ok();
        }
    }
    if s == "P" {
        s.push_str("T0S");
    }
    s
}

/// Generic wrapper that reads any Postgres binary value as a UTF-8 string.
/// Used for enum types whose OID is not a standard text OID.
struct AnyAsString(String);

impl<'a> postgres_types::FromSql<'a> for AnyAsString {
    fn from_sql(
        _ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(AnyAsString(simdutf8::basic::from_utf8(raw)?.to_string()))
    }
    fn accepts(_ty: &postgres_types::Type) -> bool {
        true
    }
}

// ─── Row → RecordBatch dispatcher ────────────────────────────────────────────

pub(super) fn rows_to_record_batch_typed(
    schema: &SchemaRef,
    columns: &[(String, Type)],
    rows: &[Row],
) -> Result<RecordBatch> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(columns.len());
    for (col_idx, (name, pg_type)) in columns.iter().enumerate() {
        let target_type = schema.field(col_idx).data_type();
        let arr = build_array(pg_type, target_type, col_idx, rows)?;
        // Guard (slice A): the value converter dispatches on the wire type, while
        // the schema field carries the resolved/overridden target type. If a
        // column override retyped the column to something the converter can't
        // produce (e.g. `uuid: string` — the wire UUID arm still builds
        // FixedSizeBinary(16) while the field is Utf8), the two disagree. Fail
        // loud with a column-named message instead of the opaque downstream
        // `RecordBatch::try_new` type-mismatch error.
        if arr.data_type() != target_type {
            anyhow::bail!(
                "column '{name}' (PG wire type {pg_type:?}): the value converter produced \
                 {:?} but the resolved column type is {target_type:?} — a column override \
                 retyped it to something the converter cannot build; remove the override \
                 or choose a compatible target type",
                arr.data_type(),
            );
        }
        arrays.push(arr);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(batch)
}

fn build_array(
    pg_type: &Type,
    target_type: &DataType,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    match *pg_type {
        Type::BOOL => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::INT2 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::INT4 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::INT8 | Type::OID => {
            let mut b = Int64Builder::with_capacity(rows.len());
            if *pg_type == Type::OID {
                for row in rows {
                    b.append_option(row.get::<_, Option<u32>>(col_idx).map(|v| v as i64));
                }
            } else {
                for row in rows {
                    b.append_option(row.get(col_idx));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::FLOAT4 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::FLOAT8 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                b.append_option(row.get(col_idx));
            }
            Ok(Arc::new(b.finish()))
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                let val: Option<String> = row.get(col_idx);
                b.append_option(val.as_deref());
            }
            Ok(Arc::new(b.finish()))
        }
        Type::BYTEA => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                match row.get::<_, Option<Vec<u8>>>(col_idx) {
                    Some(v) => b.append_value(&v),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::DATE => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::NaiveDate>>(col_idx) {
                    Some(d) => {
                        let epoch =
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
                        b.append_value((d - epoch).num_days() as i32);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::TIME => {
            let mut b = Time64MicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::NaiveTime>>(col_idx) {
                    Some(t) => {
                        let micros = t.num_seconds_from_midnight() as i64 * 1_000_000
                            + t.nanosecond() as i64 / 1_000;
                        b.append_value(micros);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        Type::TIMESTAMP => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::NaiveDateTime>>(col_idx) {
                    Some(ts) => b.append_value(ts.and_utc().timestamp_micros()),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // Roadmap §13: TIMESTAMPTZ is normalized to UTC. The Arrow array carries
        // the UTC timezone tag so Parquet writes TIMESTAMP_MICROS(isAdjustedToUTC=true).
        Type::TIMESTAMPTZ => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(col_idx) {
                    Some(ts) => b.append_value(ts.timestamp_micros()),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish().with_timezone("UTC")))
        }
        // UUID: 16-byte FixedSizeBinary so the `arrow.uuid` extension type
        // attached upstream lets parquet-rs emit native `LogicalType::Uuid`
        // (see ADR-0014 §4 and [`crate::types::mapping::build_arrow_field`]).
        // UUID honours the schema's resolved target (like the NUMERIC arm):
        // the default `FixedSizeBinary(16)`, or `Utf8` when an operator wrote
        // `columns: { uid: string }` to retype it to canonical text. The wire
        // read is the same; only the array we build follows `target_type`, so
        // the value dispatch can never disagree with the schema.
        Type::UUID => match target_type {
            DataType::Utf8 | DataType::LargeUtf8 => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 36);
                for row in rows {
                    match row.try_get::<_, Option<PgUuidBytes>>(col_idx)? {
                        None => b.append_null(),
                        Some(PgUuidBytes(bytes)) => {
                            b.append_value(uuid::Uuid::from_bytes(bytes).to_string())
                        }
                    }
                }
                Ok(Arc::new(b.finish()))
            }
            _ => {
                let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
                for row in rows {
                    match row.try_get::<_, Option<PgUuidBytes>>(col_idx)? {
                        None => b.append_null(),
                        Some(PgUuidBytes(bytes)) => b
                            .append_value(bytes)
                            .expect("16 bytes always matches FixedSizeBinary(16)"),
                    }
                }
                Ok(Arc::new(b.finish()))
            }
        },
        // JSON / JSONB: Utf8 preserving JSON semantics — `postgres` rejects `String` for these OIDs,
        // so deserialize via [`Json`] rather than emitting silent null arrays.
        Type::JSON | Type::JSONB => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.try_get::<_, Option<Json<JsonValue>>>(col_idx)? {
                    None => b.append_null(),
                    Some(Json(v)) => {
                        let s = serde_json::to_string(&v)?;
                        b.append_value(&s);
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // NUMERIC: exact Decimal128/256 path when the user declared precision/scale
        // via a column override; otherwise fall back to Utf8 (schema already carries
        // rivet.fidelity=unsupported metadata so downstream tooling can detect it).
        Type::NUMERIC => match target_type {
            DataType::Decimal128(p, s) => pg_numeric_to_decimal128(*p, *s, col_idx, rows),
            DataType::Decimal256(p, s) => pg_numeric_to_decimal256(*p, *s, col_idx, rows),
            _ => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                for row in rows {
                    let val = pg_numeric_optional_utf8_string(row, col_idx)?;
                    b.append_option(val.as_deref());
                }
                Ok(Arc::new(b.finish()))
            }
        },
        _ => {
            // INTERVAL → Utf8 ISO 8601 (Interval(MonthDayNano) is not Parquet-writable).
            if *pg_type == Type::INTERVAL {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 12);
                for row in rows {
                    match row.try_get::<_, Option<PgInterval>>(col_idx).ok().flatten() {
                        Some(iv) => b.append_value(pg_interval_to_iso8601(
                            iv.months,
                            iv.days,
                            iv.microseconds,
                        )),
                        None => b.append_null(),
                    }
                }
                return Ok(Arc::new(b.finish()));
            }

            let kind = pg_type.kind();
            // M6: 1-D arrays → Arrow List via Vec<T> deserialization
            if matches!(kind, Kind::Array(_)) {
                return build_pg_list_array(target_type, col_idx, rows);
            }

            // M6: Enum types → Utf8 (read binary enum label as UTF-8)
            if matches!(kind, Kind::Enum(_)) {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match row
                        .try_get::<_, Option<AnyAsString>>(col_idx)
                        .ok()
                        .flatten()
                    {
                        Some(s) => b.append_value(&s.0),
                        None => b.append_null(),
                    }
                }
                return Ok(Arc::new(b.finish()));
            }

            // Fail loud (slice A). `pg_columns_to_schema` already proved every
            // column maps to a supported type, so reaching here means a (wire
            // type, target type) pair with no value converter — almost always an
            // override that retyped a column to something this dispatch can't
            // read. The previous behaviour was `try_get::<String>` which silently
            // nulled every non-text value; that silent corruption is exactly what
            // CLAUDE.md "Remediation hints must recover from the degraded state"
            // forbids. Surface it instead of writing nulls.
            anyhow::bail!(
                "no value converter for PostgreSQL wire type {:?} → Arrow {:?} \
                 (column index {col_idx}); the column's source type and its \
                 resolved/overridden target type do not line up — adjust the \
                 column override so they match",
                pg_type,
                target_type,
            );
        }
    }
}

/// Build an Arrow `ListArray` from a PostgreSQL array column.
///
/// Dispatches to `Vec<T>` deserialization based on the Arrow element type.
/// Supports: bool, int16/32/64, float32/64, text. Other element types fall
/// back to a null list.
fn build_pg_list_array(
    target_type: &DataType,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    let inner_dt = if let DataType::List(field_ref) = target_type {
        field_ref.data_type()
    } else {
        anyhow::bail!("build_pg_list_array called with non-List target type");
    };

    // PG arrays can legally contain NULL elements (`ARRAY[1, NULL, 3]`).
    // The `postgres` crate's `Vec<T>` deserializer rejects such arrays with
    // an error; `try_get::<Vec<T>>` then returns `Err`, the `.ok().flatten()`
    // collapses that to `None`, and a *whole-row NULL* gets written — silent
    // data loss. The fix is to deserialize as `Vec<Option<T>>` so NULL inner
    // elements survive into the Arrow `ListBuilder` via `append_null()`.
    macro_rules! list_of {
        ($T:ty, $Builder:ty) => {{
            let mut lb = ListBuilder::new(<$Builder>::new());
            for row in rows {
                match row
                    .try_get::<_, Option<Vec<Option<$T>>>>(col_idx)
                    .ok()
                    .flatten()
                {
                    Some(v) => {
                        for x in &v {
                            match x {
                                Some(val) => lb.values().append_value(*val),
                                None => lb.values().append_null(),
                            }
                        }
                        lb.append(true);
                    }
                    None => lb.append(false),
                }
            }
            Ok(Arc::new(lb.finish()))
        }};
    }

    match inner_dt {
        DataType::Boolean => list_of!(bool, BooleanBuilder),
        DataType::Int16 => list_of!(i16, Int16Builder),
        DataType::Int32 => list_of!(i32, Int32Builder),
        DataType::Int64 => list_of!(i64, Int64Builder),
        DataType::Float32 => list_of!(f32, Float32Builder),
        DataType::Float64 => list_of!(f64, Float64Builder),
        DataType::Utf8 => {
            let mut lb = ListBuilder::new(StringBuilder::new());
            for row in rows {
                match row
                    .try_get::<_, Option<Vec<Option<String>>>>(col_idx)
                    .ok()
                    .flatten()
                {
                    Some(v) => {
                        for s in &v {
                            match s {
                                Some(val) => lb.values().append_value(val),
                                None => lb.values().append_null(),
                            }
                        }
                        lb.append(true);
                    }
                    None => lb.append(false),
                }
            }
            Ok(Arc::new(lb.finish()))
        }
        other => {
            log::warn!(
                "PG array: unsupported element type {:?}, writing null list",
                other
            );
            let mut lb = ListBuilder::new(StringBuilder::new());
            for _ in rows {
                lb.append(false);
            }
            Ok(Arc::new(lb.finish()))
        }
    }
}

// ─── NUMERIC → Decimal128 / Decimal256 ───────────────────────────────────────

/// Decode a single `NUMERIC` cell to a scaled `i128` for Arrow `Decimal128`.
///
/// The driver transmits `numeric` columns in Postgres wire binary (`numeric_recv`).
/// We decode exactly (via [`crate::source::pg_numeric_wire`]), then stringify for
/// [`crate::types::decimal::decimal_str_to_scaled_i128`] — never through `f64`.
/// A trivial `Utf8` fallback remains for unconventional cast-to-text callers.
fn pg_numeric_optional_scaled_i128(row: &Row, col_idx: usize, scale: i8) -> Result<Option<i128>> {
    use crate::types::decimal::decimal_str_to_scaled_i128;

    match row.try_get::<_, Option<PgNumericWire<'_>>>(col_idx) {
        Ok(Some(wire)) => match numeric_wire_normalized_plain(wire.0) {
            Some(plain) => {
                let t = plain.trim();
                if t.is_empty() {
                    return Ok(None);
                }
                decimal_str_to_scaled_i128(t, scale)
                    .map(Some)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "cannot parse DECIMAL {:?} as decimal(scale={scale}) after binary decode",
                            t
                        )
                    })
            }
            None => Err(anyhow::anyhow!(
                "PostgreSQL NUMERIC: unsupported NaN/infinity payload (column idx {col_idx})",
            )),
        },
        Ok(None) => Ok(None),
        Err(_) => {
            if let Ok(Some(s)) = row.try_get::<_, Option<String>>(col_idx) {
                let t = s.trim();
                if t.is_empty() {
                    return Ok(None);
                }
                return decimal_str_to_scaled_i128(t, scale)
                    .map(Some)
                    .ok_or_else(|| {
                        anyhow::anyhow!("cannot parse {:?} as decimal(scale={scale})", t)
                    });
            }
            Ok(None)
        }
    }
}

/// Build a `Decimal128Array` from a PostgreSQL `NUMERIC` column.
fn pg_numeric_to_decimal128(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    let mut b = Decimal128Builder::with_capacity(rows.len());
    for row in rows {
        match pg_numeric_optional_scaled_i128(row, col_idx, scale)? {
            Some(v) => b.append_value(v),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

/// Build a `Decimal256Array` for precision > 38 (roadmap §12).
fn pg_numeric_to_decimal256(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[Row],
) -> Result<Arc<dyn Array>> {
    use arrow::datatypes::i256;
    let mut b = Decimal256Builder::with_capacity(rows.len());
    for row in rows {
        match pg_numeric_optional_scaled_i128(row, col_idx, scale)? {
            Some(v) => b.append_value(i256::from_i128(v)),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}
