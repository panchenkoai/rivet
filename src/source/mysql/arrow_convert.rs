//! MySQL → Arrow conversion machinery — twin of `postgres/arrow_convert.rs`.
//!
//! Everything that turns a `mysql::Row` (driver type) into an Arrow
//! `RecordBatch` lives here:
//!
//! - the `mysql::Column → RivetType → DataType` mapping pipeline
//!   (`mysql_type_to_rivet`, `mysql_native_type_name`,
//!   `mysql_schema_and_arrow_types`),
//! - per-cell decoders for BIT (`bit_bytes_to_u64`), TIME
//!   (`parse_time_str_to_micros`), and DECIMAL (`mysql_decimal_to_*`),
//! - the row → array builders (`rows_to_record_batch_typed`, `build_array`).
//!
//! Five names cross the module boundary back into [`super`]: the schema +
//! types factory `mysql_schema_and_arrow_types`, the batch builder
//! `rows_to_record_batch_typed`, the type-name helper `mysql_native_type_name`
//! and `mysql_type_to_rivet` (both used by the `Source::type_mappings` impl
//! in `mod.rs`), and `bit_bytes_to_u64` (referenced by unit tests in
//! `mod.rs::tests`). Everything else is private to this file.

use std::sync::Arc;

use arrow::array::{
    Array, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use mysql::Value;
use mysql::consts::ColumnFlags;

use crate::error::Result;
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit as RivetTimeUnit, TypeMapping,
    build_arrow_field,
};

// ─── Native type names + Rivet type mapping ──────────────────────────────────

pub(super) fn mysql_native_type_name(col: &mysql::Column) -> String {
    use mysql::consts::ColumnType::*;
    let unsigned = col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    // Helper for integer types where we care about both the base name and an
    // optional ` unsigned` suffix. Round-trips through `expected_contracts.yaml`
    // which lists the canonical names `tinyint`, `tinyint unsigned`, …
    let int_name = |base: &str| -> String {
        if unsigned {
            format!("{base} unsigned")
        } else {
            base.into()
        }
    };
    match col.column_type() {
        // TINYINT(1) is the MySQL boolean convention — surface the display
        // width so downstream tooling can tell it apart from a plain TINYINT.
        MYSQL_TYPE_TINY if col.column_length() == 1 => "tinyint(1)".into(),
        MYSQL_TYPE_TINY => int_name("tinyint"),
        MYSQL_TYPE_SHORT => int_name("smallint"),
        MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => int_name("int"),
        MYSQL_TYPE_LONGLONG => int_name("bigint"),
        MYSQL_TYPE_FLOAT => "float".into(),
        MYSQL_TYPE_DOUBLE => "double".into(),
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => "decimal".into(),
        // ENUM and SET arrive on the wire as MYSQL_TYPE_STRING /
        // MYSQL_TYPE_VAR_STRING with the ENUM_FLAG / SET_FLAG set; the
        // dedicated MYSQL_TYPE_ENUM / MYSQL_TYPE_SET OIDs are rarely seen.
        // Check flags *before* falling through to the generic string family
        // so the native_type label reflects the actual semantic.
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING
            if col.flags().contains(ColumnFlags::ENUM_FLAG) =>
        {
            "enum".into()
        }
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING
            if col.flags().contains(ColumnFlags::SET_FLAG) =>
        {
            "set".into()
        }
        // Charset 63 = binary protocol; BINARY(n) uses MYSQL_TYPE_STRING and
        // VARBINARY(n) uses MYSQL_TYPE_VAR_STRING. Surface the distinction.
        MYSQL_TYPE_STRING if col.character_set() == 63 => "binary".into(),
        MYSQL_TYPE_VAR_STRING if col.character_set() == 63 => "varbinary".into(),
        MYSQL_TYPE_STRING => "char".into(),
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => "varchar".into(),
        MYSQL_TYPE_ENUM => "enum".into(),
        MYSQL_TYPE_SET => "set".into(),
        MYSQL_TYPE_JSON => "json".into(),
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
            "blob".into()
        }
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => "date".into(),
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => "time".into(),
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => "datetime".into(),
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => "timestamp".into(),
        // BIT(1) and BIT(n>1) map to different Rivet types; carry the bit-
        // width through native_type so the type-report reflects it.
        MYSQL_TYPE_BIT if col.column_length() == 1 => "bit(1)".into(),
        MYSQL_TYPE_BIT => "bit".into(),
        MYSQL_TYPE_YEAR => "year".into(),
        _ => "unknown".into(),
    }
}

/// Map a MySQL column descriptor to Rivet's canonical type.
///
/// Key decisions vs. the old `mysql_type_to_arrow`:
/// - `DECIMAL/NEWDECIMAL` → `Unsupported` (roadmap §12: no silent float fallback;
///   requires column override or `type_policy.decimal.unbounded`).
/// - `TIMESTAMP/TIMESTAMP2` → `Timestamp { timezone: Some("UTC") }` (roadmap §13:
///   MySQL TIMESTAMP is stored as UTC and session tz must be set to +00:00).
/// - `JSON` → `RivetType::Json` so `build_arrow_field` attaches both the
///   `rivet.logical_type=json` field metadata and the `arrow.json` extension
///   type (parquet-rs then emits native `LogicalType::Json`).
/// - `ENUM`/`SET` → `RivetType::Enum`. MySQL surfaces them as
///   `MYSQL_TYPE_STRING` / `MYSQL_TYPE_VAR_STRING` with the
///   `ENUM_FLAG` / `SET_FLAG` set (the dedicated `MYSQL_TYPE_ENUM` /
///   `MYSQL_TYPE_SET` OIDs are rare in the text protocol); we check the
///   flag *before* falling through to the generic string family so the
///   `rivet.logical_type=enum` metadata survives.
/// - `TINYINT(1)` / `BOOL` / `BOOLEAN` → `RivetType::Bool` (display-width 1 = MySQL boolean convention).
/// - `TINYINT` (other widths) → `RivetType::Int16`.
/// - `BIT(1)` → `RivetType::Bool`; `BIT(n>1)` → `RivetType::Int64` (avoids silent bit-truncation).
/// Derive DECIMAL `(precision, scale)` from a MySQL wire column definition.
/// `column_length` is the display width = precision + 1 (decimal point, when
/// `scale > 0`) + 1 (sign, when `signed`). Returns `None` when the arithmetic
/// can't yield a precision in MySQL's `1..=65` DECIMAL range, so the caller
/// keeps the column `Unsupported` rather than guess.
fn derive_decimal_ps(column_length: u32, scale: u8, signed: bool) -> Option<(u8, i8)> {
    let point = u32::from(scale > 0);
    let sign = u32::from(signed);
    let precision = column_length.checked_sub(point + sign)?;
    if !(1..=65).contains(&precision) {
        return None;
    }
    Some((u8::try_from(precision).ok()?, i8::try_from(scale).ok()?))
}

pub(super) fn mysql_type_to_rivet(col: &mysql::Column) -> RivetType {
    use mysql::consts::ColumnType::*;
    match col.column_type() {
        // BOOL / BOOLEAN in MySQL is TINYINT(1); display width == 1 is the canonical signal.
        // TINYINT(1) UNSIGNED is also treated as bool (same display-width convention).
        MYSQL_TYPE_TINY if col.column_length() == 1 => RivetType::Bool,
        MYSQL_TYPE_TINY => RivetType::Int16,
        MYSQL_TYPE_SHORT if col.flags().contains(ColumnFlags::UNSIGNED_FLAG) => RivetType::Int32,
        MYSQL_TYPE_SHORT => RivetType::Int16,
        MYSQL_TYPE_INT24 if col.flags().contains(ColumnFlags::UNSIGNED_FLAG) => RivetType::Int64,
        MYSQL_TYPE_LONG if col.flags().contains(ColumnFlags::UNSIGNED_FLAG) => RivetType::Int64,
        MYSQL_TYPE_INT24 => RivetType::Int32,
        MYSQL_TYPE_LONG => RivetType::Int32,
        MYSQL_TYPE_LONGLONG if col.flags().contains(ColumnFlags::UNSIGNED_FLAG) => {
            RivetType::UInt64
        }
        MYSQL_TYPE_LONGLONG => RivetType::Int64,
        MYSQL_TYPE_FLOAT => RivetType::Float32,
        MYSQL_TYPE_DOUBLE => RivetType::Float64,

        // MySQL DECIMAL precision/scale ARE recoverable from the wire column
        // definition: `decimals()` is the scale and `column_length()` is the
        // display width (precision + 1 for the point when scale>0 + 1 for the
        // sign when signed). Roadmap §12 forbids silent float conversion, so we
        // resolve exact p/s here — matching PostgreSQL's catalog-hint path —
        // and only fall back to Unsupported when the arithmetic can't yield a
        // sane precision.
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => {
            let signed = !col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
            match derive_decimal_ps(col.column_length(), col.decimals(), signed) {
                Some((precision, scale)) => RivetType::Decimal { precision, scale },
                None => RivetType::Unsupported {
                    native_type: "decimal".into(),
                    reason: "could not derive precision/scale from the MySQL column metadata; \
                             add a column override (columns: amount: decimal(18,2))"
                        .into(),
                },
            }
        }

        // ENUM and SET arrive on the wire as MYSQL_TYPE_STRING /
        // MYSQL_TYPE_VAR_STRING with the ENUM_FLAG / SET_FLAG set.
        // Without this check they would be misclassified as String and the
        // `rivet.logical_type=enum` Parquet metadata would be lost.
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING
            if col.flags().contains(ColumnFlags::ENUM_FLAG)
                || col.flags().contains(ColumnFlags::SET_FLAG) =>
        {
            RivetType::Enum
        }
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_STRING => {
            // Charset 63 = "binary"; `BINARY(n)` / `VARBINARY(n)` use STRING/VAR_STRING
            // metadata in the MySQL protocol, unlike `BLOB` OIDs — still binary bytes.
            if col.character_set() == 63 {
                RivetType::Binary
            } else {
                RivetType::String
            }
        }
        // Belt-and-suspenders for drivers/protocols that *do* surface the
        // dedicated MYSQL_TYPE_ENUM / MYSQL_TYPE_SET OIDs (rare in the
        // text protocol but seen with some configurations).
        MYSQL_TYPE_ENUM | MYSQL_TYPE_SET => RivetType::Enum,
        MYSQL_TYPE_JSON => RivetType::Json,

        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
            // charset 63 = binary; everything else is a text blob.
            if col.character_set() == 63 {
                RivetType::Binary
            } else {
                RivetType::Text
            }
        }

        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => RivetType::Date,

        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => RivetType::Time {
            unit: RivetTimeUnit::Microsecond,
        },

        // MySQL DATETIME has no timezone; stored as local/wall-clock time.
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: None,
        },
        // Roadmap §13: MySQL TIMESTAMP is always stored as UTC.
        // The driver must issue `SET time_zone = '+00:00'` before the query
        // (Chunk 4 / TypePolicy will enforce this; for now callers are responsible).
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => RivetType::Timestamp {
            unit: RivetTimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        },

        // BIT(1) is a single-bit boolean; BIT(n>1) is a multi-bit integer that must not be
        // truncated to a single boolean — map to Int64 (fits BIT(1)..BIT(63) losslessly).
        // BIT(64) technically needs u64 but Int64 is the widest signed Arrow integer type;
        // values using bit 63 as data rather than sign are rare and can use a column override.
        MYSQL_TYPE_BIT if col.column_length() == 1 => RivetType::Bool,
        MYSQL_TYPE_BIT => RivetType::Int64,
        MYSQL_TYPE_YEAR => RivetType::Int16,

        _ => RivetType::Unsupported {
            native_type: mysql_native_type_name(col),
            reason: "no Rivet mapping for this MySQL type".into(),
        },
    }
}

/// Build an Arrow `Schema` and a parallel `Vec<DataType>` from MySQL column
/// descriptors. Both are derived from the same `TypeMapping` slice so the
/// schema field type and the array type used in `build_array` are always
/// identical — mismatches would cause `RecordBatch::try_new` to panic.
///
/// `column_overrides` takes priority over autodetection.
pub(super) fn mysql_schema_and_arrow_types(
    columns: &[mysql::Column],
    column_overrides: &ColumnOverrides,
) -> crate::error::Result<(Schema, Vec<DataType>)> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrow_types: Vec<DataType> = Vec::with_capacity(columns.len());
    let mut errors: Vec<String> = Vec::new();

    for col in columns {
        let native = mysql_native_type_name(col);
        let rivet = crate::types::resolve_or(column_overrides, col.name_str().as_ref(), || {
            mysql_type_to_rivet(col)
        });
        let source = SourceColumn::simple(col.name_str().to_string(), native.clone(), true);
        let mapping = TypeMapping::from_source(&source, rivet);

        match (build_arrow_field(&mapping), mapping.arrow_type) {
            (Some(field), Some(dt)) => {
                fields.push(field);
                arrow_types.push(dt);
            }
            _ => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.as_str(),
                    _ => "no Rivet mapping for this MySQL type",
                };
                errors.push(format!(
                    "  • {} (MySQL type '{native}'): {reason}",
                    col.name_str()
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
    Ok((Schema::new(fields), arrow_types))
}

// ─── Row → RecordBatch dispatcher + per-type builders ────────────────────────

pub(super) fn rows_to_record_batch_typed(
    schema: &SchemaRef,
    arrow_types: &[DataType],
    rows: &[mysql::Row],
) -> Result<RecordBatch> {
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(arrow_types.len());
    for (col_idx, arrow_type) in arrow_types.iter().enumerate() {
        arrays.push(build_array(arrow_type, col_idx, rows)?);
    }
    Ok(RecordBatch::try_new(schema.clone(), arrays)?)
}

fn bytes_to_str(b: &[u8]) -> Option<&str> {
    simdutf8::basic::from_utf8(b).ok()
}

/// Narrow a value already widened to `i128` into a smaller signed integer,
/// erroring on overflow instead of silently wrapping the way `as iN` does.
/// MySQL column typing means valid rows always fit; a failure here is a genuine
/// type mismatch (e.g. an override that mis-declares the width, or a BIGINT
/// UNSIGNED value > i64::MAX routed through a signed builder) and must surface
/// loudly — never corrupt into a wrap. See CLAUDE.md "Remediation hints must
/// recover from the degraded state".
fn narrow<T>(v: i128, column_type: &str) -> Result<T>
where
    T: TryFrom<i128>,
{
    T::try_from(v).map_err(|_| anyhow::anyhow!("value {v} overflows {column_type} column"))
}

/// Interpret raw big-endian bytes from a MySQL BIT column as an unsigned integer.
/// MySQL sends BIT(n) values as ceil(n/8) big-endian bytes in the binary protocol.
pub(super) fn bit_bytes_to_u64(b: &[u8]) -> u64 {
    b.iter().fold(0u64, |acc, &byte| acc << 8 | u64::from(byte))
}

/// Parse MySQL text-protocol TIME string ("HH:MM:SS", "-HHH:MM:SS", "HH:MM:SS.uuuuuu")
/// into microseconds since midnight. Negative values are allowed.
fn parse_time_str_to_micros(s: &str) -> Option<i64> {
    let (neg, rest) = if let Some(r) = s.strip_prefix('-') {
        (true, r)
    } else {
        (false, s)
    };
    let (hms, us_part) = if let Some(pos) = rest.find('.') {
        let us_str = &rest[pos + 1..];
        let us_digits = us_str.len().min(6);
        let us = us_str[..us_digits].parse::<i64>().ok()?;
        let scale = 10i64.pow((6 - us_digits) as u32);
        (&rest[..pos], us * scale)
    } else {
        (rest, 0i64)
    };
    let mut parts = hms.splitn(3, ':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    let total = (h * 3_600 + m * 60 + s) * 1_000_000 + us_part;
    Some(if neg { -total } else { total })
}

// Structurally parallel to `postgres::arrow_convert::build_array` — both
// dispatch on the resolved target Arrow type (the schema's single decision) and
// read the wire value into the matching builder. The dispatch skeletons look
// like duplication, but the per-value read is irreducibly engine-specific
// (`mysql::Value` is a tagged enum coerced into the target; PostgreSQL reads via
// type-driven `FromSql`), so a shared generic would be a shallow seam — kept
// separate deliberately.
fn build_array(
    arrow_type: &DataType,
    col_idx: usize,
    rows: &[mysql::Row],
) -> Result<Arc<dyn Array>> {
    match arrow_type {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(*v != 0),
                    Some(Value::UInt(v)) => b.append_value(*v != 0),
                    // BIT(1) columns arrive as raw big-endian bytes, not decimal strings.
                    Some(Value::Bytes(bv)) => b.append_value(bit_bytes_to_u64(bv) != 0),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(narrow::<i16>(*v as i128, "smallint")?),
                    Some(Value::UInt(v)) => b.append_value(narrow::<i16>(*v as i128, "smallint")?),
                    // Parse to i128 first so an out-of-range numeric string errors
                    // (overflow) rather than nulling like a non-numeric one.
                    Some(Value::Bytes(bv)) => match atoi::atoi::<i128>(bv) {
                        Some(v) => b.append_value(narrow::<i16>(v, "smallint")?),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(narrow::<i32>(*v as i128, "int")?),
                    Some(Value::UInt(v)) => b.append_value(narrow::<i32>(*v as i128, "int")?),
                    Some(Value::Bytes(bv)) => match atoi::atoi::<i128>(bv) {
                        Some(v) => b.append_value(narrow::<i32>(v, "int")?),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt64 => {
            let mut b = UInt64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::UInt(v)) => b.append_value(*v),
                    Some(Value::Int(v)) if *v >= 0 => b.append_value(*v as u64),
                    Some(Value::Bytes(bv)) => {
                        if let Some(v) = atoi::atoi::<u64>(bv) {
                            b.append_value(v);
                        } else {
                            b.append_null();
                        }
                    }
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Int(v)) => b.append_value(*v),
                    // A BIGINT UNSIGNED value > i64::MAX cannot ride a signed
                    // builder — error loudly (the operator should map it to
                    // `decimal(20,0)`) rather than wrap into a negative.
                    Some(Value::UInt(v)) => b.append_value(narrow::<i64>(*v as i128, "bigint")?),
                    Some(Value::Bytes(bv)) => {
                        // BIT(n>1) columns arrive as raw big-endian bytes; TEXT columns as UTF-8.
                        // Try decimal parse first; fall back to big-endian uint interpretation.
                        let v =
                            atoi::atoi::<i64>(bv).unwrap_or_else(|| bit_bytes_to_u64(bv) as i64);
                        b.append_value(v);
                    }
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Float(v)) => b.append_value(*v),
                    Some(Value::Double(v)) => b.append_value(*v as f32),
                    Some(Value::Bytes(bv)) => match bytes_to_str(bv).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Float(v)) => b.append_value(*v as f64),
                    Some(Value::Double(v)) => b.append_value(*v),
                    Some(Value::Bytes(bv)) => match bytes_to_str(bv).and_then(|s| s.parse().ok()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Bytes(bv)) => b.append_value(String::from_utf8_lossy(bv).as_ref()),
                    Some(Value::Int(v)) => b.append_value(v.to_string()),
                    Some(Value::UInt(v)) => b.append_value(v.to_string()),
                    Some(Value::Float(v)) => b.append_value(v.to_string()),
                    Some(Value::Double(v)) => b.append_value(v.to_string()),
                    Some(Value::Date(y, m, d, h, mi, s, us)) => {
                        b.append_value(format!(
                            "{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"
                        ));
                    }
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 64);
            for row in rows {
                match row.as_ref(col_idx) {
                    Some(Value::Bytes(bv)) => b.append_value(bv),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // FixedSizeBinary(16) is the Arrow type for `RivetType::Uuid`. MySQL
        // has no native UUID OID, so the column has to arrive here via a
        // `columns: { uid: uuid }` override; the wire-side payload is a
        // canonical 36-char text UUID stored in CHAR/VARCHAR/BINARY.
        // Parsing the text into 16 bytes lets us hand parquet-rs a value
        // that pairs with the `arrow.uuid` extension and produces native
        // `LogicalType::Uuid` in the Parquet file.
        DataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                let bytes = match row.as_ref(col_idx) {
                    Some(Value::Bytes(bv)) if bv.len() == 16 => {
                        let mut a = [0u8; 16];
                        a.copy_from_slice(bv);
                        Some(a)
                    }
                    Some(Value::Bytes(bv)) => bytes_to_str(bv)
                        .and_then(|s| uuid::Uuid::parse_str(s.trim()).ok())
                        .map(|u| *u.as_bytes()),
                    _ => None,
                };
                match bytes {
                    Some(a) => b
                        .append_value(a)
                        .expect("16 bytes always fits FixedSizeBinary(16)"),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                match row.as_ref(col_idx) {
                    // MySQL wire protocol delivers TIME as Value::Time(neg, days, h, m, s, us)
                    Some(Value::Time(neg, days, h, m, s, us)) => {
                        let total_us = (*days as i64 * 86_400
                            + *h as i64 * 3_600
                            + *m as i64 * 60
                            + *s as i64)
                            * 1_000_000
                            + *us as i64;
                        b.append_value(if *neg { -total_us } else { total_us });
                    }
                    Some(Value::Bytes(bv)) => {
                        // text-protocol fallback: "HH:MM:SS" or "HHH:MM:SS.uuuuuu"
                        if let Some(us) = bytes_to_str(bv).and_then(parse_time_str_to_micros) {
                            b.append_value(us);
                        } else {
                            b.append_null();
                        }
                    }
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for row in rows {
                let d = match row.as_ref(col_idx) {
                    Some(Value::Date(y, m, d, _, _, _, _)) => {
                        chrono::NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
                    }
                    Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| {
                        chrono::NaiveDate::parse_from_str(
                            s.split(' ').next().unwrap_or(s),
                            "%Y-%m-%d",
                        )
                        .ok()
                    }),
                    _ => None,
                };
                match d {
                    Some(date) => {
                        let epoch =
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
                        b.append_value((date - epoch).num_days() as i32);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        // Both DATETIME (tz=None) and TIMESTAMP (tz=Some("UTC")) share the
        // same physical i64 microsecond values. The timezone tag on the array
        // type is what distinguishes them in the Arrow / Parquet schema.
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let tz_tag = tz.clone();
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let dt = match row.as_ref(col_idx) {
                    Some(Value::Date(y, mo, d, h, mi, s, us)) => chrono::NaiveDate::from_ymd_opt(
                        *y as i32, *mo as u32, *d as u32,
                    )
                    .and_then(|d| d.and_hms_micro_opt(*h as u32, *mi as u32, *s as u32, *us)),
                    Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()
                    }),
                    _ => None,
                };
                match dt {
                    Some(dt) => b.append_value(dt.and_utc().timestamp_micros()),
                    None => b.append_null(),
                }
            }
            let arr = b.finish();
            // Roadmap §13: attach the UTC timezone tag so Parquet writes
            // TIMESTAMP_MICROS(isAdjustedToUTC=true) for TIMESTAMP columns.
            match tz_tag {
                Some(tz_str) => Ok(Arc::new(arr.with_timezone(tz_str.as_ref()))),
                None => Ok(Arc::new(arr)),
            }
        }
        // Exact decimal path: column override declared decimal(p,s) for a MySQL DECIMAL column.
        DataType::Decimal128(p, s) => mysql_decimal_to_decimal128(*p, *s, col_idx, rows),
        DataType::Decimal256(p, s) => mysql_decimal_to_decimal256(*p, *s, col_idx, rows),
        // Fail loud (slice A), symmetric with the PostgreSQL path.
        // `mysql_schema_and_arrow_types` already proved every column resolves to
        // a supported Arrow type, so an unhandled type here is a should-never-
        // happen — writing a null array silently hid it. Surface it.
        _ => anyhow::bail!(
            "no value converter for MySQL column → Arrow {:?} (column index {col_idx}); \
             this Arrow type has no builder — report it as a type-support gap",
            arrow_type,
        ),
    }
}

// ─── DECIMAL → Decimal128 / Decimal256 ───────────────────────────────────────

/// Build a `Decimal128Array` from MySQL DECIMAL column bytes (text protocol),
/// or from a MySQL integer column when the operator declared a Decimal override.
/// Integer-source path: a column override (e.g. `c_bigint_u: decimal(20,0)`)
/// against `BIGINT UNSIGNED` lets unsigned values up to `u64::MAX` ride as
/// Decimal128 — Snowflake and BigQuery both reject Parquet UINT64 > 2^63-1
/// from the raw INT64 view, so this is the canonical workaround for that.
fn mysql_decimal_to_decimal128(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[mysql::Row],
) -> Result<Arc<dyn Array>> {
    use crate::types::decimal::decimal_str_to_scaled_i128;
    let mut b = Decimal128Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(col_idx) {
            Some(Value::Bytes(bv)) => {
                let s = bytes_to_str(bv).unwrap_or("");
                match decimal_str_to_scaled_i128(s, scale) {
                    Some(v) => b.append_value(v),
                    None => {
                        return Err(anyhow::anyhow!(
                            "cannot parse '{}' as decimal({},{})",
                            s,
                            precision,
                            scale
                        ));
                    }
                }
            }
            Some(Value::Int(v)) => match scale_int_to_i128(*v as i128, scale) {
                Some(scaled) => b.append_value(scaled),
                None => {
                    return Err(anyhow::anyhow!(
                        "decimal({},{}) overflow scaling integer {}",
                        precision,
                        scale,
                        v
                    ));
                }
            },
            Some(Value::UInt(v)) => match scale_int_to_i128(*v as i128, scale) {
                Some(scaled) => b.append_value(scaled),
                None => {
                    return Err(anyhow::anyhow!(
                        "decimal({},{}) overflow scaling unsigned integer {}",
                        precision,
                        scale,
                        v
                    ));
                }
            },
            _ => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

/// Scale an integer value (already widened to `i128`) by `10^scale` for
/// storage as a fixed-point Decimal. Returns `None` on overflow or negative
/// scale (Arrow's Parquet writer rejects negative scale at the time of writing,
/// so callers should not try to materialize one).
fn scale_int_to_i128(v: i128, scale: i8) -> Option<i128> {
    if scale < 0 {
        return None;
    }
    10i128
        .checked_pow(scale as u32)
        .and_then(|mult| v.checked_mul(mult))
}

/// Build a `Decimal256Array` for precision > 38. Same integer-override
/// handling as `mysql_decimal_to_decimal128`.
fn mysql_decimal_to_decimal256(
    precision: u8,
    scale: i8,
    col_idx: usize,
    rows: &[mysql::Row],
) -> Result<Arc<dyn Array>> {
    // Decimal256 (precision > 38): scale straight into i256 so values beyond
    // i128 are not truncated.
    use crate::types::decimal::{decimal_str_to_scaled_i256, scale_int_to_i256};
    let mut b = Decimal256Builder::with_capacity(rows.len());
    for row in rows {
        match row.as_ref(col_idx) {
            Some(Value::Bytes(bv)) => {
                let s = bytes_to_str(bv).unwrap_or("");
                match decimal_str_to_scaled_i256(s, scale) {
                    Some(v) => b.append_value(v),
                    None => {
                        return Err(anyhow::anyhow!(
                            "cannot parse '{}' as decimal({},{})",
                            s,
                            precision,
                            scale
                        ));
                    }
                }
            }
            Some(Value::Int(v)) => match scale_int_to_i256(*v as i128, scale) {
                Some(scaled) => b.append_value(scaled),
                None => {
                    return Err(anyhow::anyhow!(
                        "decimal({},{}) overflow scaling integer {}",
                        precision,
                        scale,
                        v
                    ));
                }
            },
            Some(Value::UInt(v)) => match scale_int_to_i256(*v as i128, scale) {
                Some(scaled) => b.append_value(scaled),
                None => {
                    return Err(anyhow::anyhow!(
                        "decimal({},{}) overflow scaling unsigned integer {}",
                        precision,
                        scale,
                        v
                    ));
                }
            },
            _ => b.append_null(),
        }
    }
    Ok(Arc::new(
        b.finish().with_precision_and_scale(precision, scale)?,
    ))
}

#[cfg(test)]
mod scale_int_overflow_tests {
    use super::{derive_decimal_ps, narrow, scale_int_to_i128};

    #[test]
    fn negative_scale_is_rejected() {
        // Parquet rejects negative-scale decimals; the scaler refuses too.
        assert_eq!(scale_int_to_i128(123, -1), None);
    }

    #[test]
    fn scale_zero_is_identity_and_u64_max_rides_losslessly() {
        assert_eq!(scale_int_to_i128(123, 0), Some(123));
        // The `bigint unsigned: decimal(20,0)` case: u64::MAX must survive whole.
        assert_eq!(
            scale_int_to_i128(u64::MAX as i128, 0),
            Some(u64::MAX as i128)
        );
    }

    #[test]
    fn normal_scaling() {
        assert_eq!(scale_int_to_i128(5, 2), Some(500));
        assert_eq!(scale_int_to_i128(-7, 3), Some(-7000));
    }

    #[test]
    fn overflow_returns_none_not_wrap() {
        // u64::MAX (~1.8e19) scaled by 10^20 (~1.8e39) exceeds i128::MAX → None.
        assert_eq!(scale_int_to_i128(u64::MAX as i128, 20), None);
        // 10^39 overflows i128 in checked_pow itself → None, not a panic.
        assert_eq!(scale_int_to_i128(1, 39), None);
        // Multiplication overflow on an already-max operand.
        assert_eq!(scale_int_to_i128(i128::MAX, 1), None);
    }

    // ── narrow(): the `as iN` truncation footgun, now checked ────────────────

    #[test]
    fn narrow_fits_in_range() {
        assert_eq!(narrow::<i16>(100, "smallint").unwrap(), 100i16);
        assert_eq!(narrow::<i32>(-5, "int").unwrap(), -5i32);
        assert_eq!(
            narrow::<i64>(u32::MAX as i128, "bigint").unwrap(),
            u32::MAX as i64
        );
    }

    #[test]
    fn narrow_overflow_errors_not_wraps() {
        assert!(narrow::<i16>(40_000, "smallint").is_err()); // > i16::MAX 32767
        assert!(narrow::<i16>(-40_000, "smallint").is_err());
        assert!(narrow::<i32>(5_000_000_000, "int").is_err()); // > i32::MAX
        // The real bug: a BIGINT UNSIGNED value > i64::MAX would wrap to a
        // negative with `as i64`; narrow turns it into a loud error instead.
        assert!(narrow::<i64>(u64::MAX as i128, "bigint").is_err());
    }

    // ── derive_decimal_ps: MySQL wire display-width → precision/scale ─────────

    #[test]
    fn derive_decimal_ps_signed_and_unsigned() {
        // DECIMAL(10,2) signed: "-99999999.99" = 12 chars (p + point + sign).
        assert_eq!(derive_decimal_ps(12, 2, true), Some((10, 2)));
        // DECIMAL(10,0) signed: "-9999999999" = 11 chars (no decimal point).
        assert_eq!(derive_decimal_ps(11, 0, true), Some((10, 0)));
        // DECIMAL(10,2) unsigned: "99999999.99" = 11 chars (no sign).
        assert_eq!(derive_decimal_ps(11, 2, false), Some((10, 2)));
        // Fixture columns: DECIMAL(18,2), DECIMAL(20,6) signed.
        assert_eq!(derive_decimal_ps(20, 2, true), Some((18, 2)));
        assert_eq!(derive_decimal_ps(22, 6, true), Some((20, 6)));
    }

    #[test]
    fn derive_decimal_ps_rejects_insane_widths() {
        assert_eq!(derive_decimal_ps(1, 2, true), None); // underflow → None, not panic
        assert_eq!(derive_decimal_ps(0, 0, false), None); // precision 0
        assert_eq!(derive_decimal_ps(100, 0, false), None); // > 65 MySQL max
    }
}
