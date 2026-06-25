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
use mysql::consts::{ColumnFlags, ColumnType};

use crate::error::Result;
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit as RivetTimeUnit, TypeMapping,
    build_arrow_field,
};

// ─── Pre-allocation per-value ceiling (security audit V22, CWE-770) ───────────

use crate::source::value_within_ceiling;

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
    max_value_bytes: Option<usize>,
) -> Result<RecordBatch> {
    // BIT columns put raw big-endian bytes on the wire, indistinguishable from
    // decimal text by inspecting the value alone (BIT(16) 0x3132 *is* the bytes
    // of "12"). `mysql::Row` carries the wire column metadata, so consult it
    // per column instead of guessing per value.
    let wire_columns = rows.first().map(|r| r.columns_ref());
    let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(arrow_types.len());
    for (col_idx, arrow_type) in arrow_types.iter().enumerate() {
        let is_bit = wire_columns.is_some_and(|cols| {
            cols.get(col_idx)
                .is_some_and(|c| matches!(c.column_type(), ColumnType::MYSQL_TYPE_BIT))
        });
        let column = schema.field(col_idx).name();
        arrays.push(build_array(
            arrow_type,
            col_idx,
            rows,
            is_bit,
            column,
            max_value_bytes,
        )?);
    }
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    if crate::source::value_checksum::enabled() {
        let a = mysql_rows_checksums(arrow_types, rows);
        let b = crate::source::value_checksum::arrow_batch_checksums(&batch);
        crate::source::value_checksum::verify(&a, &b, schema)?;
    }
    Ok(batch)
}

/// Source-side value checksum for MySQL (Form A, side A). Mirrors `build_array`'s
/// per-type decode from `mysql::Value`, independently of the Arrow build, so a
/// divergence means the value converter changed a value between read and Arrow
/// build. Dispatches on the resolved target Arrow type and covers exactly the
/// types [`crate::source::value_checksum::arrow_batch_checksums`] (side B) covers
/// (int / uint / float / bool / date / timestamp(µs) / utf8 / binary / decimal128);
/// everything else contributes 0 on both sides. Decimal128 uses an independent
/// `BigDecimal` decode (build_array goes through `decimal_str_to_scaled_i128` /
/// `scale_int_to_i128`) rounding toward zero to match.
fn mysql_rows_checksums(arrow_types: &[DataType], rows: &[mysql::Row]) -> Vec<u64> {
    use bigdecimal::{BigDecimal, RoundingMode, num_traits::ToPrimitive};
    let wire_columns = rows.first().map(|r| r.columns_ref());
    (0..arrow_types.len())
        .map(|col_idx| {
            let is_bit = wire_columns.is_some_and(|cols| {
                cols.get(col_idx)
                    .is_some_and(|c| matches!(c.column_type(), ColumnType::MYSQL_TYPE_BIT))
            });
            let mut acc: u64 = 0;
            // Hash the value's little-endian bytes (`add!`) or a raw byte slice
            // (`addb!`), XORed into the column accumulator — the SAME bytes side B
            // hashes for this target Arrow type. Widths must match (Int16 → i16,
            // Date32 → i32) or the matrix guard false-mismatches.
            macro_rules! add {
                ($e:expr) => {
                    acc ^= xxhash_rust::xxh3::xxh3_64(&($e).to_le_bytes())
                };
            }
            macro_rules! addb {
                ($b:expr) => {
                    acc ^= xxhash_rust::xxh3::xxh3_64($b)
                };
            }
            let bd_scaled = |bd: BigDecimal, scale: i8| -> Option<i128> {
                bd.with_scale_round(scale as i64, RoundingMode::Down)
                    .into_bigint_and_exponent()
                    .0
                    .to_i128()
            };
            match &arrow_types[col_idx] {
                DataType::Boolean => {
                    for row in rows {
                        let b = match row.as_ref(col_idx) {
                            Some(Value::Int(v)) => Some(*v != 0),
                            Some(Value::UInt(v)) => Some(*v != 0),
                            Some(Value::Bytes(bv)) => Some(bit_bytes_to_u64(bv) != 0),
                            _ => None,
                        };
                        if let Some(b) = b {
                            addb!(&[b as u8]);
                        }
                    }
                }
                DataType::Int16 => {
                    for row in rows {
                        match row.as_ref(col_idx) {
                            Some(Value::Int(v)) => add!(*v as i16),
                            Some(Value::UInt(v)) => add!(*v as i16),
                            Some(Value::Bytes(bv)) => {
                                if let Some(v) = atoi::atoi::<i128>(bv) {
                                    add!(v as i16);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                DataType::Int32 => {
                    for row in rows {
                        match row.as_ref(col_idx) {
                            Some(Value::Int(v)) => add!(*v as i32),
                            Some(Value::UInt(v)) => add!(*v as i32),
                            Some(Value::Bytes(bv)) => {
                                if let Some(v) = atoi::atoi::<i128>(bv) {
                                    add!(v as i32);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                DataType::UInt64 => {
                    for row in rows {
                        match row.as_ref(col_idx) {
                            Some(Value::UInt(v)) => add!(*v),
                            Some(Value::Int(v)) if *v >= 0 => add!(*v as u64),
                            Some(Value::Bytes(bv)) => {
                                if let Some(v) = atoi::atoi::<u64>(bv) {
                                    add!(v);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                DataType::Int64 => {
                    for row in rows {
                        match row.as_ref(col_idx) {
                            Some(Value::Int(v)) => add!(*v),
                            Some(Value::UInt(v)) => add!(*v),
                            Some(Value::Bytes(bv)) => {
                                if let Some(v) = int64_from_bytes(bv, is_bit).ok().flatten() {
                                    add!(v);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                DataType::Float32 => {
                    for row in rows {
                        let f: Option<f32> = match row.as_ref(col_idx) {
                            Some(Value::Float(v)) => Some(*v),
                            Some(Value::Double(v)) => Some(*v as f32),
                            Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| s.parse().ok()),
                            _ => None,
                        };
                        if let Some(f) = f {
                            add!(f.to_bits());
                        }
                    }
                }
                DataType::Float64 => {
                    for row in rows {
                        let f: Option<f64> = match row.as_ref(col_idx) {
                            Some(Value::Float(v)) => Some(*v as f64),
                            Some(Value::Double(v)) => Some(*v),
                            Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| s.parse().ok()),
                            _ => None,
                        };
                        if let Some(f) = f {
                            add!(f.to_bits());
                        }
                    }
                }
                DataType::Utf8 => {
                    for row in rows {
                        match row.as_ref(col_idx) {
                            Some(Value::Bytes(bv)) => match bytes_to_str(bv) {
                                Some(s) => addb!(s.as_bytes()),
                                None => addb!(String::from_utf8_lossy(bv).as_bytes()),
                            },
                            Some(Value::Int(v)) => addb!(v.to_string().as_bytes()),
                            Some(Value::UInt(v)) => addb!(v.to_string().as_bytes()),
                            Some(Value::Float(v)) => addb!(v.to_string().as_bytes()),
                            Some(Value::Double(v)) => addb!(v.to_string().as_bytes()),
                            Some(Value::Date(y, m, d, h, mi, sx, us)) => addb!(
                                format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{sx:02}.{us:06}")
                                    .as_bytes()
                            ),
                            _ => {}
                        }
                    }
                }
                DataType::Binary => {
                    for row in rows {
                        if let Some(Value::Bytes(bv)) = row.as_ref(col_idx) {
                            addb!(bv);
                        }
                    }
                }
                DataType::Date32 => {
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
                        if let Some(date) = d {
                            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                            add!((date - epoch).num_days() as i32);
                        }
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    for row in rows {
                        let dt = match row.as_ref(col_idx) {
                            Some(Value::Date(y, mo, d, h, mi, sx, us)) => {
                                chrono::NaiveDate::from_ymd_opt(*y as i32, *mo as u32, *d as u32)
                                    .and_then(|d| {
                                        d.and_hms_micro_opt(*h as u32, *mi as u32, *sx as u32, *us)
                                    })
                            }
                            Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| {
                                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()
                            }),
                            _ => None,
                        };
                        if let Some(dt) = dt {
                            add!(dt.and_utc().timestamp_micros());
                        }
                    }
                }
                DataType::Decimal128(_p, scale) => {
                    for row in rows {
                        let scaled = match row.as_ref(col_idx) {
                            Some(Value::Bytes(bv)) => bytes_to_str(bv)
                                .and_then(|s| s.parse::<BigDecimal>().ok())
                                .and_then(|bd| bd_scaled(bd, *scale)),
                            Some(Value::Int(v)) => bd_scaled(BigDecimal::from(*v), *scale),
                            Some(Value::UInt(v)) => bd_scaled(BigDecimal::from(*v), *scale),
                            _ => None,
                        };
                        if let Some(v) = scaled {
                            add!(v);
                        }
                    }
                }
                // FixedSizeBinary(UUID) / Time64 / Decimal256 / etc. — skipped on
                // both sides (see value_checksum coverage note).
                _ => {}
            }
            acc
        })
        .collect()
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

/// Decode an Int64 cell that arrived as `Value::Bytes`. BIT columns carry raw
/// big-endian bits; everything else (an integer in the text protocol, or a
/// TEXT column overridden to int64) carries UTF-8 decimal text. The two
/// encodings collide whenever the bit bytes happen to be ASCII digits, so the
/// column's BIT-ness — from the wire metadata, not the value's shape — picks
/// the decoder. Non-numeric text yields `None` (null, matching the Int16/Int32
/// arms); out-of-range values error loudly rather than wrap.
fn int64_from_bytes(bv: &[u8], is_bit: bool) -> Result<Option<i64>> {
    if is_bit {
        // BIT(64) with bit 63 set exceeds i64 — error (the operator can map it
        // to `decimal(20,0)`), never wrap into a negative.
        return Ok(Some(narrow::<i64>(bit_bytes_to_u64(bv) as i128, "bit")?));
    }
    match atoi::atoi::<i128>(bv) {
        Some(v) => Ok(Some(narrow::<i64>(v, "bigint")?)),
        None => Ok(None),
    }
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
    is_bit: bool,
    column: &str,
    max_value_bytes: Option<usize>,
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
                    Some(Value::Bytes(bv)) => match int64_from_bytes(bv, is_bit)? {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    },
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
                    // SIMD-validate the common valid-UTF-8 case (text columns
                    // are the highest-volume bytes on the decode path); fall
                    // back to a scalar lossy replacement only for the rare
                    // invalid value. Byte-identical to `from_utf8_lossy` but
                    // ~2.3x faster on wide text (bench `mysql_utf8_text_append`).
                    // Matches every other text path in this file + the PG decoder.
                    Some(Value::Bytes(bv)) => {
                        // Pre-allocation ceiling: the driver copy (`Value::Bytes`)
                        // is unavoidable, but bail before the append so the Arrow
                        // buffer never grows to hold the oversized cell.
                        value_within_ceiling(column, bv.len(), max_value_bytes)?;
                        match bytes_to_str(bv) {
                            Some(s) => b.append_value(s),
                            None => b.append_value(String::from_utf8_lossy(bv).as_ref()),
                        }
                    }
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
                    Some(Value::Bytes(bv)) => {
                        value_within_ceiling(column, bv.len(), max_value_bytes)?;
                        b.append_value(bv);
                    }
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

#[cfg(test)]
mod utf8_fast_path_tests {
    use super::bytes_to_str;

    /// The Utf8 text-column append swaps `String::from_utf8_lossy` for a
    /// simdutf8 fast path (`bytes_to_str`) with a lossy fallback on the rare
    /// invalid value. This must be byte-identical to pure lossy for *every*
    /// input — including invalid UTF-8, where the `None` branch falls back.
    fn append_as_done_in_decode(bv: &[u8]) -> String {
        match bytes_to_str(bv) {
            Some(s) => s.to_owned(),
            None => String::from_utf8_lossy(bv).into_owned(),
        }
    }

    #[test]
    fn fast_path_is_byte_identical_to_lossy() {
        let cases: &[&[u8]] = &[
            b"",                             // empty
            b"plain ascii",                  // valid ASCII
            "héllo wörld 日本語".as_bytes(), // valid multibyte UTF-8
            &[0xff, 0xfe, 0x00, 0x41],       // invalid bytes → lossy fallback path
            &[0x41, 0xc0, 0x42],             // lone continuation → replacement char
            &[0xe4, 0xb8],                   // truncated multibyte → replacement
        ];
        for &bv in cases {
            assert_eq!(
                append_as_done_in_decode(bv),
                String::from_utf8_lossy(bv),
                "fast path diverged from lossy for {bv:?}"
            );
        }
    }
}

#[cfg(test)]
mod int64_bytes_dispatch_tests {
    use super::int64_from_bytes;

    // Regression (mysql-bit): the Int64 arm used to atoi-first on raw bytes,
    // misdecoding any BIT value whose first byte is an ASCII digit (atoi also
    // ignores trailing non-digits). The column's BIT-ness — not the value's
    // shape — picks the decoder.

    #[test]
    fn bit_bytes_decode_big_endian_even_when_ascii_digits() {
        // BIT(8) 0x39 is also the text "9" — must decode as 57, not 9.
        assert_eq!(int64_from_bytes(&[0x39], true).unwrap(), Some(57));
        // BIT(16) 0x3132 is also the text "12" — the RED-test value, 12594.
        assert_eq!(int64_from_bytes(&[0x31, 0x32], true).unwrap(), Some(0x3132));
        // Digit first byte + non-digit tail: atoi would yield 1, truth is 12799.
        assert_eq!(int64_from_bytes(&[0x31, 0xFF], true).unwrap(), Some(12799));
        // Non-digit bytes were the only case the old fallback got right.
        assert_eq!(int64_from_bytes(&[0xAB, 0xCD], true).unwrap(), Some(0xABCD));
    }

    #[test]
    fn bit64_top_bit_set_errors_not_wraps() {
        // BIT(64) all-ones exceeds i64::MAX; wrapping to -1 would corrupt.
        assert!(int64_from_bytes(&[0xFF; 8], true).is_err());
        // The lossless neighbor: exactly i64::MAX still rides.
        assert_eq!(
            int64_from_bytes(&[0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], true).unwrap(),
            Some(i64::MAX)
        );
    }

    #[test]
    fn text_bytes_follow_the_int_arm_mismatch_policy() {
        assert_eq!(int64_from_bytes(b"12", false).unwrap(), Some(12));
        assert_eq!(int64_from_bytes(b"-42", false).unwrap(), Some(-42));
        // Non-numeric text overridden to int64 → null (matching the Int16/Int32
        // arms), never big-endian garbage from a bit fallback.
        assert_eq!(int64_from_bytes(b"hello", false).unwrap(), None);
        assert_eq!(int64_from_bytes(b"", false).unwrap(), None);
        // Out-of-range numeric text errors (overflow) rather than nulling.
        assert!(int64_from_bytes(b"9223372036854775808", false).is_err());
        assert_eq!(
            int64_from_bytes(b"9223372036854775807", false).unwrap(),
            Some(i64::MAX)
        );
    }
}

// ROAST-RED mysql-bit-decode: the Int64 builder arm in `build_array` tries an
// ASCII-decimal parse (`atoi`) on `Value::Bytes` BEFORE the big-endian BIT
// decode (`bit_bytes_to_u64`), so any BIT(n>1) value whose first byte happens
// to be an ASCII digit is silently misdecoded (atoi also ignores trailing
// non-digits). BIT(16) value 0x3132 arrives as bytes b"12" and decodes as 12
// instead of 12594.
// Asserts CORRECT behavior; expected to FAIL until the fix lands.
#[cfg(test)]
mod roast_mysql_bit_decode_tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType;
    use mysql::consts::ColumnType;
    use mysql::prelude::Queryable;
    use mysql::{Conn, OptsBuilder, Value};

    use super::{mysql_schema_and_arrow_types, rows_to_record_batch_typed};
    use crate::types::ColumnOverrides;

    // ── minimal fake MySQL server ─────────────────────────────────────────────
    //
    // `mysql::Row` has no public constructor reachable from this crate's
    // feature set (`new_row` lives in `mysql_common`, which is not a direct
    // dependency, and the `binlog` escape hatch is feature-gated off), so the
    // only way to obtain real driver rows deterministically is to speak just
    // enough of the wire protocol: handshake → auth OK → one text-protocol
    // resultset with a single BIT(16) column. This also carries the
    // `mysql::Column` metadata (MYSQL_TYPE_BIT) the decoder needs to consult.

    /// Frame a payload as a MySQL protocol packet: 3-byte LE length + seq id.
    fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
        let len = payload.len();
        let header = [len as u8, (len >> 8) as u8, (len >> 16) as u8, seq];
        stream
            .write_all(&header)
            .expect("fake MySQL server: write packet header");
        stream
            .write_all(payload)
            .expect("fake MySQL server: write packet payload");
    }

    /// Read one client packet (header + payload); contents are ignored.
    fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("fake MySQL server: read packet header");
        let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let mut payload = vec![0u8; len];
        stream
            .read_exact(&mut payload)
            .expect("fake MySQL server: read packet payload");
        payload
    }

    /// Length-encoded string (all our strings are < 251 bytes).
    fn lenc_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
        debug_assert!(bytes.len() < 251);
        out.push(bytes.len() as u8);
        out.extend_from_slice(bytes);
    }

    /// HandshakeV10 greeting advertising PROTOCOL_41 | SECURE_CONNECTION |
    /// PLUGIN_AUTH with `mysql_native_password`.
    fn handshake_greeting() -> Vec<u8> {
        let mut p = Vec::new();
        p.push(0x0a); // protocol version 10
        p.extend_from_slice(b"8.0.99\0"); // server version
        p.extend_from_slice(&1u32.to_le_bytes()); // connection id
        p.extend_from_slice(b"abcdefgh"); // auth plugin data part 1 (8 bytes)
        p.push(0x00); // filler
        // capabilities lower half:
        // CLIENT_LONG_PASSWORD (0x0001) | CLIENT_PROTOCOL_41 (0x0200)
        // | CLIENT_SECURE_CONNECTION (0x8000)
        p.extend_from_slice(&0x8201u16.to_le_bytes());
        p.push(0x21); // default collation: utf8_general_ci
        p.extend_from_slice(&0x0002u16.to_le_bytes()); // status: AUTOCOMMIT
        // capabilities upper half: CLIENT_PLUGIN_AUTH (0x0008_0000 >> 16)
        p.extend_from_slice(&0x0008u16.to_le_bytes());
        p.push(21); // auth plugin data total length (20 + NUL terminator)
        p.extend_from_slice(&[0u8; 10]); // reserved
        p.extend_from_slice(b"ijklmnopqrst\0"); // auth plugin data part 2 (13 bytes)
        p.extend_from_slice(b"mysql_native_password\0");
        p
    }

    /// ColumnDefinition41 for a `BIT(16)` column named `b`.
    fn bit16_column_definition() -> Vec<u8> {
        let mut p = Vec::new();
        lenc_bytes(&mut p, b"def"); // catalog (fixed value)
        lenc_bytes(&mut p, b""); // schema
        lenc_bytes(&mut p, b"t"); // table
        lenc_bytes(&mut p, b"t"); // org_table
        lenc_bytes(&mut p, b"b"); // name
        lenc_bytes(&mut p, b"b"); // org_name
        p.push(0x0c); // length of fixed-length fields
        p.extend_from_slice(&63u16.to_le_bytes()); // character set: binary
        p.extend_from_slice(&16u32.to_le_bytes()); // column_length: BIT(16)
        p.push(0x10); // column type: MYSQL_TYPE_BIT
        p.extend_from_slice(&0x0020u16.to_le_bytes()); // flags: UNSIGNED
        p.push(0); // decimals
        p.extend_from_slice(&[0, 0]); // filler
        p
    }

    /// Serve exactly one connection: handshake, auth OK, then answer the first
    /// COM_QUERY with one BIT(16) row whose raw wire bytes are [0x31, 0x32].
    fn serve_one_bit16_query(listener: TcpListener) {
        let (mut s, _) = listener.accept().expect("fake MySQL server: accept");
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(10))).unwrap();

        // ── connection phase (seq 0..=2) ──
        write_packet(&mut s, 0, &handshake_greeting());
        let _handshake_response = read_packet(&mut s);
        // OK: header 0x00, affected 0, last_insert_id 0, status AUTOCOMMIT, warnings 0.
        write_packet(&mut s, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

        // ── command phase: one COM_QUERY (client seq resets to 0) ──
        let _com_query = read_packet(&mut s);
        write_packet(&mut s, 1, &[0x01]); // column count = 1
        write_packet(&mut s, 2, &bit16_column_definition());
        write_packet(&mut s, 3, &[0xfe, 0x00, 0x00, 0x02, 0x00]); // EOF after metadata
        // Text-protocol row: one length-encoded cell, the raw BIT bytes 0x3132
        // — which are also the ASCII string "12". This is the trap.
        write_packet(&mut s, 4, &[0x02, 0x31, 0x32]);
        write_packet(&mut s, 5, &[0xfe, 0x00, 0x00, 0x02, 0x00]); // EOF after rows
        // Swallow the COM_QUIT the client sends on drop; errors are fine.
        let mut buf = [0u8; 64];
        let _ = s.read(&mut buf);
    }

    // ROAST-RED mysql-bit-decode: BIT(16) value 0x3132 must decode to 12594,
    // not to atoi(b"12") = 12.
    // Asserts CORRECT behavior; expected to FAIL until the fix lands.
    #[test]
    fn roast_mysql_bit16_decodes_as_big_endian_bits_not_ascii_digits() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake MySQL server");
        let port = listener.local_addr().expect("local_addr").port();
        let server = std::thread::spawn(move || serve_one_bit16_query(listener));

        let opts = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(port)
            .user(Some("root"))
            .pass(Some(""))
            .prefer_socket(false)
            .max_allowed_packet(Some(16 * 1024 * 1024))
            .tcp_connect_timeout(Some(Duration::from_secs(10)))
            .read_timeout(Some(Duration::from_secs(10)))
            .write_timeout(Some(Duration::from_secs(10)));
        let mut conn = Conn::new(opts).expect("connect to fake MySQL server");

        let rows: Vec<mysql::Row> = {
            let mut result = conn.query_iter("SELECT b FROM t").expect("COM_QUERY");
            let row_set = result.iter().expect("one result set");
            row_set.map(|r| r.expect("row decodes")).collect()
        };
        drop(conn);
        server.join().expect("fake MySQL server thread");

        // ── harness preconditions: the wire gave us exactly the production input ──
        assert_eq!(rows.len(), 1, "fake server sent exactly one row");
        let columns = rows[0].columns();
        assert!(
            matches!(columns[0].column_type(), ColumnType::MYSQL_TYPE_BIT),
            "column metadata is MYSQL_TYPE_BIT — the BIT-ness the decoder must consult"
        );
        assert_eq!(
            rows[0].as_ref(0),
            Some(&Value::Bytes(vec![0x31, 0x32])),
            "BIT(16) travels as raw big-endian bytes on the wire"
        );

        // ── production decode path: schema + arrow types from the same columns ──
        let (schema, arrow_types) = mysql_schema_and_arrow_types(&columns, &ColumnOverrides::new())
            .expect("schema for BIT(16) column");
        assert_eq!(
            arrow_types,
            vec![DataType::Int64],
            "BIT(n>1) maps to Int64 — the buggy builder arm"
        );

        let batch = rows_to_record_batch_typed(&Arc::new(schema), &arrow_types, &rows, None)
            .expect("record batch from BIT(16) row");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column");
        assert!(!arr.is_null(0), "BIT value must not be nulled");
        assert_eq!(
            arr.value(0),
            0x3132,
            "BIT(16) value 0x3132 must decode as big-endian bits (12594); the \
             atoi-first Int64 path reads bytes [0x31, 0x32] as ASCII \"12\" and \
             yields the wrong value 12"
        );
    }
}
