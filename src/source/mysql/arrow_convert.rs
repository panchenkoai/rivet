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

use std::borrow::Cow;
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
    if let Some(cols) = wire_columns {
        let expected: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let names: Vec<String> = cols.iter().map(|c| c.name_str().into_owned()).collect();
        let wire: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        crate::source::verify_wire_columns(&expected, &wire)?;
    }
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
    // Form A value-checksum (always-on): source-side pass (A) vs the built batch
    // (B) — fail loud if the value converter diverged between read and Arrow build.
    let a = crate::source::value_checksum::source_checksums(schema, &MysqlCellSource { rows });
    let b = crate::source::value_checksum::arrow_batch_checksums(&batch);
    crate::source::value_checksum::verify(&a, &b, schema)?;
    Ok(batch)
}

/// Side A of the Form A value-checksum for MySQL — an INDEPENDENT decode of the raw
/// `mysql::Value`s (mirroring `build_array`) so it equals side B on a correct build.
/// Drives the shared [`crate::source::value_checksum::source_checksums`] dispatch;
/// each accessor holds MySQL's per-type extraction (the Int/UInt/Bytes/Float/Double/
/// Date variants, unsigned widths, BIT via `is_bit`, decimal via `BigDecimal`). Bytes
/// must match `feed_cell` or the matrix guard false-mismatches.
struct MysqlCellSource<'a> {
    rows: &'a [mysql::Row],
}

impl MysqlCellSource<'_> {
    /// MySQL surfaces a `BIT` column's bytes big-endian; `int64_from_bytes` needs to
    /// know so it widens them correctly. Read from the first row's column metadata.
    fn is_bit(&self, col: usize) -> bool {
        self.rows.first().is_some_and(|r| {
            r.columns_ref()
                .get(col)
                .is_some_and(|c| matches!(c.column_type(), ColumnType::MYSQL_TYPE_BIT))
        })
    }
}

impl crate::source::value_checksum::CellSource for MysqlCellSource<'_> {
    fn num_rows(&self) -> usize {
        self.rows.len()
    }
    fn boolean(&self, col: usize, row: usize) -> Option<bool> {
        match self.rows[row].as_ref(col) {
            Some(Value::Int(v)) => Some(*v != 0),
            Some(Value::UInt(v)) => Some(*v != 0),
            Some(Value::Bytes(bv)) => Some(bit_bytes_to_u64(bv) != 0),
            _ => None,
        }
    }
    fn int16(&self, col: usize, row: usize) -> Option<i16> {
        match self.rows[row].as_ref(col) {
            Some(Value::Int(v)) => Some(*v as i16),
            Some(Value::UInt(v)) => Some(*v as i16),
            Some(Value::Bytes(bv)) => atoi::atoi::<i128>(bv).map(|v| v as i16),
            _ => None,
        }
    }
    fn int32(&self, col: usize, row: usize) -> Option<i32> {
        match self.rows[row].as_ref(col) {
            Some(Value::Int(v)) => Some(*v as i32),
            Some(Value::UInt(v)) => Some(*v as i32),
            Some(Value::Bytes(bv)) => atoi::atoi::<i128>(bv).map(|v| v as i32),
            _ => None,
        }
    }
    fn int64(&self, col: usize, row: usize) -> Option<i64> {
        match self.rows[row].as_ref(col) {
            Some(Value::Int(v)) => Some(*v),
            Some(Value::UInt(v)) => Some(*v as i64),
            Some(Value::Bytes(bv)) => int64_from_bytes(bv, self.is_bit(col)).ok().flatten(),
            _ => None,
        }
    }
    fn uint64(&self, col: usize, row: usize) -> Option<u64> {
        match self.rows[row].as_ref(col) {
            Some(Value::UInt(v)) => Some(*v),
            Some(Value::Int(v)) if *v >= 0 => Some(*v as u64),
            Some(Value::Bytes(bv)) => atoi::atoi::<u64>(bv),
            _ => None,
        }
    }
    fn float32(&self, col: usize, row: usize) -> Option<f32> {
        match self.rows[row].as_ref(col) {
            Some(Value::Float(v)) => Some(*v),
            Some(Value::Double(v)) => Some(*v as f32),
            Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| s.parse().ok()),
            _ => None,
        }
    }
    fn float64(&self, col: usize, row: usize) -> Option<f64> {
        match self.rows[row].as_ref(col) {
            Some(Value::Float(v)) => Some(*v as f64),
            Some(Value::Double(v)) => Some(*v),
            Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| s.parse().ok()),
            _ => None,
        }
    }
    fn decimal128(&self, col: usize, row: usize, scale: i8) -> Option<i128> {
        use bigdecimal::{BigDecimal, RoundingMode, num_traits::ToPrimitive};
        let bd: BigDecimal = match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) => bytes_to_str(bv)?.parse().ok()?,
            Some(Value::Int(v)) => BigDecimal::from(*v),
            Some(Value::UInt(v)) => BigDecimal::from(*v),
            _ => return None,
        };
        bd.with_scale_round(scale as i64, RoundingMode::Down)
            .into_bigint_and_exponent()
            .0
            .to_i128()
    }
    fn date32(&self, col: usize, row: usize) -> Option<i32> {
        let d = match self.rows[row].as_ref(col) {
            Some(Value::Date(y, m, d, _, _, _, _)) => {
                chrono::NaiveDate::from_ymd_opt(*y as i32, *m as u32, *d as u32)
            }
            Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(|s| {
                chrono::NaiveDate::parse_from_str(s.split(' ').next().unwrap_or(s), "%Y-%m-%d").ok()
            }),
            _ => None,
        }?;
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        Some((d - epoch).num_days() as i32)
    }
    fn ts_micros(&self, col: usize, row: usize) -> Option<i64> {
        let dt = match self.rows[row].as_ref(col) {
            Some(Value::Date(y, mo, d, h, mi, sx, us)) => {
                chrono::NaiveDate::from_ymd_opt(*y as i32, *mo as u32, *d as u32)
                    .and_then(|d| d.and_hms_micro_opt(*h as u32, *mi as u32, *sx as u32, *us))
            }
            Some(Value::Bytes(bv)) => bytes_to_str(bv)
                .and_then(|s| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()),
            _ => None,
        }?;
        Some(dt.and_utc().timestamp_micros())
    }
    fn binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) => Some(Cow::Borrowed(bv.as_slice())),
            _ => None,
        }
    }
    fn utf8(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) => Some(match bytes_to_str(bv) {
                Some(s) => Cow::Borrowed(s.as_bytes()),
                None => Cow::Owned(String::from_utf8_lossy(bv).into_owned().into_bytes()),
            }),
            Some(Value::Int(v)) => Some(Cow::Owned(v.to_string().into_bytes())),
            Some(Value::UInt(v)) => Some(Cow::Owned(v.to_string().into_bytes())),
            Some(Value::Float(v)) => Some(Cow::Owned(v.to_string().into_bytes())),
            Some(Value::Double(v)) => Some(Cow::Owned(v.to_string().into_bytes())),
            Some(Value::Date(y, m, d, hh, mi, sx, us)) => Some(Cow::Owned(
                format!("{y:04}-{m:02}-{d:02} {hh:02}:{mi:02}:{sx:02}.{us:06}").into_bytes(),
            )),
            _ => None,
        }
    }
    fn time64_micros(&self, col: usize, row: usize) -> Option<i64> {
        match self.rows[row].as_ref(col) {
            Some(Value::Time(neg, days, h, m, s, us)) => {
                let total =
                    (*days as i64 * 86_400 + *h as i64 * 3_600 + *m as i64 * 60 + *s as i64)
                        * 1_000_000
                        + *us as i64;
                Some(if *neg { -total } else { total })
            }
            Some(Value::Bytes(bv)) => bytes_to_str(bv).and_then(parse_time_str_to_micros),
            _ => None,
        }
    }
    fn fixed_binary(&self, col: usize, row: usize) -> Option<Cow<'_, [u8]>> {
        // Mirrors the FixedSizeBinary(16) build arm: 16 raw bytes verbatim, or
        // a 36-char text UUID parsed to its bytes.
        match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) if bv.len() == 16 => Some(Cow::Borrowed(bv.as_slice())),
            Some(Value::Bytes(bv)) => bytes_to_str(bv)
                .and_then(|s| uuid::Uuid::parse_str(s.trim()).ok())
                .map(|u| Cow::Owned(u.as_bytes().to_vec())),
            _ => None,
        }
    }
    fn decimal256(&self, col: usize, row: usize, scale: i8) -> Option<arrow::datatypes::i256> {
        use crate::types::decimal::decimal_str_to_scaled_i256;
        match self.rows[row].as_ref(col) {
            Some(Value::Bytes(bv)) => {
                bytes_to_str(bv).and_then(|s| decimal_str_to_scaled_i256(s.trim(), scale))
            }
            Some(Value::Int(v)) => decimal_str_to_scaled_i256(&v.to_string(), scale),
            Some(Value::UInt(v)) => decimal_str_to_scaled_i256(&v.to_string(), scale),
            _ => None,
        }
    }
    fn list(
        &self,
        _col: usize,
        _row: usize,
        _elem: &arrow::datatypes::DataType,
    ) -> Option<Vec<crate::source::value_checksum::ListElem>> {
        // MySQL has no array types; a List column never resolves here.
        None
    }
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
mod time_parse_tests {
    use super::parse_time_str_to_micros;

    /// `parse_time_str_to_micros` had NO direct test — every arithmetic operator
    /// in it was reachable only through a full MySQL export, so the mutation
    /// baseline carried EIGHTEEN uncaught operator mutants for this one
    /// function. Each value below is chosen so no two operators agree: h=1
    /// (1*3600 = 3600 vs 1+3600 = 3601), m=2 (2*60 = 120 vs 2+60 = 62), and a
    /// fraction whose digit count differs from 6 so the `6 - us_digits` exponent
    /// is observable at all.
    #[test]
    fn parse_time_str_to_micros_pins_every_arithmetic_step() {
        // (1*3600 + 2*60 + 3) * 1e6 + 456789
        assert_eq!(
            parse_time_str_to_micros("01:02:03.456789"),
            Some(3_723_456_789),
            "hours, minutes, seconds and a full 6-digit fraction"
        );
        // A SHORT fraction: 1 digit means scale 10^(6-1), so ".5" is 500_000 µs.
        // A `+` in that exponent would scale by 10^7 instead.
        assert_eq!(
            parse_time_str_to_micros("00:00:01.5"),
            Some(1_500_000),
            "a 1-digit fraction scales by 10^(6-1), not 10^(6+1)"
        );
        // No fraction: the `us_part` addend is 0, and the rest still holds.
        assert_eq!(parse_time_str_to_micros("02:00:00"), Some(7_200_000_000));
        // Negation applies to the WHOLE value, after the sum.
        assert_eq!(
            parse_time_str_to_micros("-01:02:03.456789"),
            Some(-3_723_456_789)
        );
        // Over-long fractions truncate at 6 digits, never round or error.
        assert_eq!(parse_time_str_to_micros("00:00:00.1234567"), Some(123_456));
        // Malformed input is None — never a panic, never a silent zero.
        assert_eq!(parse_time_str_to_micros("not a time"), None);
        assert_eq!(parse_time_str_to_micros("01:02"), None);
    }
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
    use super::{
        MysqlCellSource, TimeUnit, build_array, mysql_native_type_name, mysql_type_to_rivet,
    };
    use super::{RivetTimeUnit, RivetType};
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
    /// One `ColumnDefinition41` packet body, the wire shape the server sends to
    /// describe a result-set column. Every field the two mapping functions read
    /// — `character_set`, `column_length`, `column_type`, `flags`, `decimals` —
    /// is a parameter, so a test can hand the real driver any column MySQL can
    /// describe without needing a MySQL.
    fn column_definition(
        name: &str,
        character_set: u16,
        column_length: u32,
        column_type: u8,
        flags: u16,
        decimals: u8,
    ) -> Vec<u8> {
        let mut p = Vec::new();
        lenc_bytes(&mut p, b"def"); // catalog (fixed value)
        lenc_bytes(&mut p, b""); // schema
        lenc_bytes(&mut p, b"t"); // table
        lenc_bytes(&mut p, b"t"); // org_table
        lenc_bytes(&mut p, name.as_bytes()); // name
        lenc_bytes(&mut p, name.as_bytes()); // org_name
        p.push(0x0c); // length of fixed-length fields
        p.extend_from_slice(&character_set.to_le_bytes());
        p.extend_from_slice(&column_length.to_le_bytes());
        p.push(column_type);
        p.extend_from_slice(&flags.to_le_bytes());
        p.push(decimals);
        p.extend_from_slice(&[0, 0]); // filler
        p
    }

    fn bit16_column_definition() -> Vec<u8> {
        // charset 63 = binary, BIT(16), MYSQL_TYPE_BIT (0x10), UNSIGNED.
        column_definition("b", 63, 16, 0x10, 0x0020, 0)
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

    /// Serve one connection whose single result set has `defs.len()` columns and
    /// exactly one all-NULL row. The row exists only so the client hands back a
    /// `mysql::Row` carrying the column metadata — the metadata IS the subject.
    fn serve_one_metadata_query(listener: TcpListener, defs: Vec<Vec<u8>>) {
        let (mut s, _) = listener.accept().expect("fake MySQL server: accept");
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(10))).unwrap();

        write_packet(&mut s, 0, &handshake_greeting());
        let _handshake_response = read_packet(&mut s);
        write_packet(&mut s, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

        let _com_query = read_packet(&mut s);
        let n = u8::try_from(defs.len()).expect("fixture stays under the 251-column lenc boundary");
        let mut seq = 1u8;
        write_packet(&mut s, seq, &[n]); // column count
        for def in &defs {
            seq += 1;
            write_packet(&mut s, seq, def);
        }
        seq += 1;
        write_packet(&mut s, seq, &[0xfe, 0x00, 0x00, 0x02, 0x00]); // EOF after metadata
        seq += 1;
        write_packet(&mut s, seq, &vec![0xfb; defs.len()]); // one row, every cell NULL
        seq += 1;
        write_packet(&mut s, seq, &[0xfe, 0x00, 0x00, 0x02, 0x00]); // EOF after rows

        let mut buf = [0u8; 64];
        let _ = s.read(&mut buf);
    }

    /// One BINARY-protocol row packet: header, null bitmap, then the non-NULL
    /// values already encoded in their per-type binary form.
    ///
    /// The bitmap is the fiddly part of the format and the reason this is a
    /// function rather than a literal: it is `ceil((n + 7 + 2) / 8)` bytes and
    /// column `i` lives at bit `(i + 2) % 8` of byte `(i + 2) / 8` — the two-bit
    /// offset is reserved, and getting it wrong shifts every NULL by two columns
    /// instead of failing.
    fn binary_row_packet(cells: &[Option<Vec<u8>>]) -> Vec<u8> {
        let n = cells.len();
        let mut p = vec![0x00u8]; // packet header for a binary resultset row
        let mut bitmap = vec![0u8; (n + 9) / 8];
        for (i, c) in cells.iter().enumerate() {
            if c.is_none() {
                bitmap[(i + 2) / 8] |= 1 << ((i + 2) % 8);
            }
        }
        p.extend_from_slice(&bitmap);
        for c in cells.iter().flatten() {
            p.extend_from_slice(c);
        }
        p
    }

    /// Serve one connection speaking the BINARY (prepared-statement) protocol.
    ///
    /// This is the protocol that matters: rivet's export loop runs every query
    /// through `exec_iter` — its own comment says "query_iter returns a
    /// Text-protocol result, exec_iter Binary" — so `build_array` receives
    /// `Value::Int`/`Value::UInt` for integers, NOT the `Value::Bytes` a text
    /// resultset yields for everything. A fixture built on the text server would
    /// therefore exercise arms production never takes: correct assertions on an
    /// input the product does not produce.
    ///
    /// Flow: COM_STMT_PREPARE -> prepare-ok (+ column defs, since rivet binds no
    /// parameters, num_params is 0 and the param block is absent) -> COM_STMT_
    /// EXECUTE -> a normal resultset whose ROWS are in binary form.
    fn serve_one_binary_query(
        listener: TcpListener,
        defs: Vec<Vec<u8>>,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    ) {
        let (mut s, _) = listener.accept().expect("fake MySQL server: accept");
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(10))).unwrap();

        write_packet(&mut s, 0, &handshake_greeting());
        let _handshake_response = read_packet(&mut s);
        write_packet(&mut s, 2, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]);

        let ncol = u8::try_from(defs.len()).expect("fixture stays under 251 columns");
        let eof = [0xfe, 0x00, 0x00, 0x02, 0x00];

        loop {
            let cmd = read_packet(&mut s);
            match cmd.first() {
                // COM_STMT_PREPARE
                Some(0x16) => {
                    let mut ok = vec![0x00];
                    ok.extend_from_slice(&1u32.to_le_bytes()); // statement id
                    ok.extend_from_slice(&u16::from(ncol).to_le_bytes()); // num_columns
                    ok.extend_from_slice(&0u16.to_le_bytes()); // num_params
                    ok.push(0x00); // reserved
                    ok.extend_from_slice(&0u16.to_le_bytes()); // warnings
                    let mut seq = 1u8;
                    write_packet(&mut s, seq, &ok);
                    for def in &defs {
                        seq += 1;
                        write_packet(&mut s, seq, def);
                    }
                    if !defs.is_empty() {
                        seq += 1;
                        write_packet(&mut s, seq, &eof);
                    }
                }
                // COM_STMT_EXECUTE
                Some(0x17) => {
                    let mut seq = 1u8;
                    write_packet(&mut s, seq, &[ncol]);
                    for def in &defs {
                        seq += 1;
                        write_packet(&mut s, seq, def);
                    }
                    seq += 1;
                    write_packet(&mut s, seq, &eof);
                    for r in &rows {
                        assert_eq!(r.len(), defs.len(), "every fixture row must be full width");
                        seq += 1;
                        write_packet(&mut s, seq, &binary_row_packet(r));
                    }
                    seq += 1;
                    write_packet(&mut s, seq, &eof);
                }
                // COM_QUIT, or the connection went away
                Some(0x01) | None => break,
                // COM_STMT_CLOSE has no reply; anything else gets a bare OK.
                Some(0x19) => {}
                Some(_) => write_packet(&mut s, 1, &[0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00]),
            }
        }
    }

    /// Fetch real `mysql::Row`s over the BINARY protocol.
    ///
    /// The seam that makes `build_array` testable at all: it takes `&[mysql::Row]`
    /// and a `Row` has no public constructor — it can only be DECODED from the
    /// wire. Writing wire bytes and letting the driver decode them means the
    /// values under test arrive by exactly the path production values do.
    fn fetch_binary_rows(defs: Vec<Vec<u8>>, cells: Vec<Vec<Option<Vec<u8>>>>) -> Vec<mysql::Row> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake MySQL server");
        let port = listener.local_addr().expect("local_addr").port();
        let server = std::thread::spawn(move || serve_one_binary_query(listener, defs, cells));

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
        let rows: Vec<mysql::Row> = conn
            .exec("SELECT * FROM t", ())
            .expect("binary-protocol resultset decodes");
        drop(conn);
        server.join().expect("fake MySQL server thread");
        rows
    }

    /// Drive a set of column definitions through a real `mysql::Conn` and return
    /// the `(native_type_name, rivet_type)` pair rivet derives for each.
    fn map_columns(defs: Vec<Vec<u8>>) -> Vec<(String, RivetType)> {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake MySQL server");
        let port = listener.local_addr().expect("local_addr").port();
        let server = std::thread::spawn(move || serve_one_metadata_query(listener, defs));

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

        let mapped = {
            let mut result = conn.query_iter("SELECT * FROM t").expect("COM_QUERY");
            let row_set = result.iter().expect("one result set");
            let rows: Vec<mysql::Row> = row_set.map(|r| r.expect("row decodes")).collect();
            assert_eq!(rows.len(), 1, "fake server sent exactly one row");
            rows[0]
                .columns()
                .iter()
                .map(|c| (mysql_native_type_name(c), mysql_type_to_rivet(c)))
                .collect()
        };
        drop(conn);
        server.join().expect("fake MySQL server thread");
        mapped
    }

    /// Smoke test for the binary-protocol server: does the real driver accept the
    /// prepare/execute dialogue at all, and does an INT arrive as a TYPED value?
    ///
    /// `Value::Int` is the whole point. Over the text protocol the same column
    /// arrives as `Value::Bytes(b"42")`, so this assertion is what distinguishes
    /// the protocol the export loop uses from the one that is easy to fake.
    #[test]
    fn the_binary_protocol_server_yields_typed_values_not_text() {
        use mysql::consts::ColumnType::*;
        let defs = vec![column_definition("i", 33, 11, MYSQL_TYPE_LONG as u8, 0, 0)];
        let rows = fetch_binary_rows(defs, vec![vec![Some(42i32.to_le_bytes().to_vec())]]);
        assert_eq!(rows.len(), 1, "one row");
        assert_eq!(
            rows[0].as_ref(0),
            Some(&Value::Int(42)),
            "the binary protocol must yield a TYPED Int — text would give Bytes(b\"42\")"
        );
    }

    // ── binary-protocol cell producers ──────────────────────────────────────
    // One per `mysql::Value` variant `build_array` matches on. Which variant a
    // column yields is decided by its TYPE and flags, not by the bytes, so these
    // pair a column definition with the matching binary encoding.
    const F_UNSIGNED: u16 = 32;
    const CS_BIN: u16 = 63;
    const CS_UTF8: u16 = 33;

    fn lenc_of(v: &[u8]) -> Vec<u8> {
        let mut o = Vec::new();
        lenc_bytes(&mut o, v);
        o
    }

    /// `Value::Int` — a signed BIGINT.
    fn v_int(v: i64) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 20, MYSQL_TYPE_LONGLONG as u8, 0, 0),
            v.to_le_bytes().to_vec(),
        )
    }

    /// `Value::UInt` — and the value MUST exceed `i64::MAX` to actually get one.
    ///
    /// Measured, because the obvious assumption is wrong: the driver types a
    /// binary integer by MAGNITUDE, not by the column's UNSIGNED flag.
    /// `v_uint(200)` yields `Value::Int(200)`; only `v_uint(18e18)` yields
    /// `Value::UInt`. A fixture that sets the flag and passes a small number
    /// feeds the Int arm while reading as though it covered the UInt one — it
    /// passes, for the wrong reason. That is exactly how the first version of
    /// this grid left five `Value::UInt` arms unfed while looking complete;
    /// mutation testing named them, the green did not.
    fn v_uint(v: u64) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 20, MYSQL_TYPE_LONGLONG as u8, F_UNSIGNED, 0),
            v.to_le_bytes().to_vec(),
        )
    }

    /// `Value::Bytes` — the text-ish fallback every arm also accepts.
    fn v_bytes(v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 255, MYSQL_TYPE_VAR_STRING as u8, 0, 0),
            lenc_of(v),
        )
    }

    /// `Value::Bytes` from a BINARY column (charset 63) — same variant, but the
    /// bytes are not text.
    fn v_blob(v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_BIN, 65535, MYSQL_TYPE_BLOB as u8, 0, 0),
            lenc_of(v),
        )
    }

    fn v_float(v: f32) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 12, MYSQL_TYPE_FLOAT as u8, 0, 31),
            v.to_le_bytes().to_vec(),
        )
    }

    fn v_double(v: f64) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        (
            column_definition("c", CS_UTF8, 22, MYSQL_TYPE_DOUBLE as u8, 0, 31),
            v.to_le_bytes().to_vec(),
        )
    }

    /// `Value::Date` — the 11-byte DATETIME form (length, year u16, month, day,
    /// hour, minute, second, micros u32). The shorter 4- and 7-byte forms exist;
    /// the full one is used so every component is non-zero and therefore
    /// observable (a zero component cannot distinguish a dropped field).
    fn v_date(y: u16, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        let mut b = vec![11u8];
        b.extend_from_slice(&y.to_le_bytes());
        b.extend_from_slice(&[mo, d, h, mi, s]);
        b.extend_from_slice(&us.to_le_bytes());
        (
            column_definition("c", CS_BIN, 26, MYSQL_TYPE_DATETIME as u8, 0, 6),
            b,
        )
    }

    /// `Value::Time` — the 12-byte TIME form (length, sign, days u32, h, m, s,
    /// micros u32).
    fn v_time(neg: bool, days: u32, h: u8, m: u8, s: u8, us: u32) -> (Vec<u8>, Vec<u8>) {
        use mysql::consts::ColumnType::*;
        let mut b = vec![12u8, u8::from(neg)];
        b.extend_from_slice(&days.to_le_bytes());
        b.extend_from_slice(&[h, m, s]);
        b.extend_from_slice(&us.to_le_bytes());
        (
            column_definition("c", CS_BIN, 17, MYSQL_TYPE_TIME as u8, 0, 6),
            b,
        )
    }

    fn one_array(cell: (Vec<u8>, Vec<u8>), dt: &DataType, label: &str) -> Arc<dyn Array> {
        let (def, bytes) = cell;
        let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
        assert_eq!(rows.len(), 1, "{label}: one row");
        build_array(dt, 0, &rows, false, "c", None)
            .unwrap_or_else(|e| panic!("{label}: build_array errored: {e}"))
    }

    /// Every `CellSource` accessor, over the BINARY protocol.
    ///
    /// `MysqlCellSource` feeds value-checksum Form A — the number rivet publishes
    /// to attest that what it read is what it wrote. A wrong accessor does not
    /// corrupt the data; it corrupts the ATTESTATION, which is worse, because the
    /// artifact then carries a checksum that agrees with itself. All sixteen
    /// accessors take `&[mysql::Row]`, so none had a unit test until the binary
    /// harness existed — 65 standing baseline entries between them.
    ///
    /// Expectations are derived from what the quantity MEANS (an unscaled
    /// integer, days since the epoch, microseconds since it) and written by hand,
    /// never read back from a run.
    #[test]
    fn every_cell_source_accessor_reads_the_binary_value_it_should() {
        use crate::source::value_checksum::CellSource;

        fn src(cell: (Vec<u8>, Vec<u8>)) -> Vec<mysql::Row> {
            let (def, bytes) = cell;
            fetch_binary_rows(vec![def], vec![vec![Some(bytes)]])
        }

        let r = src(v_int(1));
        assert_eq!(MysqlCellSource { rows: &r }.num_rows(), 1);
        assert_eq!(MysqlCellSource { rows: &r }.boolean(0, 0), Some(true));
        let r = src(v_int(0));
        assert_eq!(MysqlCellSource { rows: &r }.boolean(0, 0), Some(false));

        let r = src(v_int(-12345));
        assert_eq!(MysqlCellSource { rows: &r }.int16(0, 0), Some(-12345));
        let r = src(v_int(-2_000_000_000));
        assert_eq!(
            MysqlCellSource { rows: &r }.int32(0, 0),
            Some(-2_000_000_000)
        );
        let r = src(v_int(-9_000_000_000_000_000_000));
        assert_eq!(
            MysqlCellSource { rows: &r }.int64(0, 0),
            Some(-9_000_000_000_000_000_000)
        );
        // Only a magnitude past i64::MAX actually yields Value::UInt — see v_uint.
        let r = src(v_uint(18_000_000_000_000_000_000));
        assert_eq!(
            MysqlCellSource { rows: &r }.uint64(0, 0),
            Some(18_000_000_000_000_000_000)
        );

        let r = src(v_float(0.5));
        assert_eq!(MysqlCellSource { rows: &r }.float32(0, 0), Some(0.5));
        let r = src(v_double(-1.25));
        assert_eq!(MysqlCellSource { rows: &r }.float64(0, 0), Some(-1.25));

        // decimal keeps the UNSCALED integer: 150.05 at scale 2 is 15005.
        let r = src(v_bytes(b"150.05"));
        assert_eq!(
            MysqlCellSource { rows: &r }.decimal128(0, 0, 2),
            Some(15005)
        );

        let r = src(v_bytes(b"h\xc3\xa9llo"));
        assert_eq!(
            MysqlCellSource { rows: &r }.utf8(0, 0).as_deref(),
            // The accessor hands back BYTES, not a validated str — the checksum
            // folds the exact wire bytes, so an invalid sequence must not be
            // silently replaced on the way in.
            Some("h\u{e9}llo".as_bytes())
        );
        let r = src(v_blob(&[0xde, 0xad]));
        assert_eq!(
            MysqlCellSource { rows: &r }.binary(0, 0).as_deref(),
            Some(&[0xde, 0xad][..])
        );

        // 2026-06-23 is 20627 days after 1970-01-01; the same instant at
        // 10:00:00.123456 is 1_782_208_800_123_456 microseconds after it.
        let r = src(v_date(2026, 6, 23, 10, 0, 0, 123_456));
        assert_eq!(MysqlCellSource { rows: &r }.date32(0, 0), Some(20627));
        assert_eq!(
            MysqlCellSource { rows: &r }.ts_micros(0, 0),
            Some(1_782_208_800_123_456)
        );

        // 13:45:30.123456 is 49_530_123_456 microseconds after midnight. No zero
        // components: at midnight the multiply, add and divide are indistinguishable.
        let r = src(v_time(false, 0, 13, 45, 30, 123_456));
        assert_eq!(
            MysqlCellSource { rows: &r }.time64_micros(0, 0),
            Some(49_530_123_456)
        );

        // A NULL must read as absent through EVERY accessor, not as a zero — a
        // checksum that folds NULL and 0 to one value attests they are the same.
        let r = fetch_binary_rows(vec![v_int(0).0], vec![vec![None]]);
        let s = MysqlCellSource { rows: &r };
        assert_eq!(s.int64(0, 0), None, "NULL is absent, never 0");
        assert_eq!(s.boolean(0, 0), None);
        assert_eq!(s.float64(0, 0), None);
        assert_eq!(s.utf8(0, 0), None);
    }

    /// The FULL `DataType` x `Value` dispatch grid of `build_array`, over the
    /// BINARY protocol.
    ///
    /// `build_array` dispatches TWICE: first on the target Arrow type, then, inside
    /// each arm, on which `mysql::Value` variant the wire produced. The first
    /// version of this fixture fed ONE value per Arrow arm and looked complete —
    /// it was a diagonal, not a grid. Mutation testing named the difference: 54
    /// missed mutants, of which 22 were unfed `Value::` sub-arms and 24 were
    /// operators sitting INSIDE those sub-arms, unreachable by construction.
    ///
    /// Which variant arrives is decided by the column's TYPE and FLAGS, so each
    /// row here pairs a producer with a target type. Only the binary protocol
    /// yields the typed variants at all: `exec_iter` is what rivet's export loop
    /// uses, and a text-protocol fixture would grade only the `Bytes` fallback.
    ///
    /// The oracle is arrow's own renderer against a hand-written literal — never a
    /// value recomputed with the conversion under test. Temporal cells are graded
    /// on their raw primitive instead, in
    /// `build_array_temporal_grid_converts_to_independently_computed_values`,
    /// because a render format is arrow's business but the NUMBER is rivet's.
    #[test]
    fn build_array_covers_every_value_variant_of_every_arrow_arm() {
        use arrow::util::display::array_value_to_string;

        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        /// (label, the (column-definition, binary-cell) pair, target type, expected render)
        type GridCase = (&'static str, (Vec<u8>, Vec<u8>), DataType, &'static str);
        let cases: Vec<GridCase> = vec![
            // Boolean: any non-zero is true, and the Bytes path reads BIT bytes
            // big-endian (b"1" is 0x31 = 49, not the digit one).
            ("bool/int", v_int(1), DataType::Boolean, "true"),
            ("bool/int0", v_int(0), DataType::Boolean, "false"),
            ("bool/uint", v_uint(u64::MAX), DataType::Boolean, "true"),
            ("bool/bytes", v_bytes(b"1"), DataType::Boolean, "true"),
            // Int16 / Int32 / Int64 / UInt64: three producers each.
            ("i16/int", v_int(-12345), DataType::Int16, "-12345"),
            ("i16/bytes", v_bytes(b"-42"), DataType::Int16, "-42"),
            (
                "i32/int",
                v_int(-2000000000),
                DataType::Int32,
                "-2000000000",
            ),
            (
                "i32/uint",
                v_uint(2000000000),
                DataType::Int32,
                "2000000000",
            ),
            ("i32/bytes", v_bytes(b"-42"), DataType::Int32, "-42"),
            (
                "i64/int",
                v_int(-9000000000000000000),
                DataType::Int64,
                "-9000000000000000000",
            ),
            ("i64/bytes", v_bytes(b"-42"), DataType::Int64, "-42"),
            // The value that MUST NOT ride a signed builder — why UInt64 is an arm.
            (
                "u64/uint",
                v_uint(18000000000000000000),
                DataType::UInt64,
                "18000000000000000000",
            ),
            ("u64/int", v_int(42), DataType::UInt64, "42"),
            (
                "utf8/uint_big",
                v_uint(u64::MAX),
                DataType::Utf8,
                "18446744073709551615",
            ),
            ("u64/bytes", v_bytes(b"123"), DataType::UInt64, "123"),
            // Float32 / Float64 accept BOTH float widths plus text.
            ("f32/float", v_float(0.5), DataType::Float32, "0.5"),
            ("f32/double", v_double(1.5), DataType::Float32, "1.5"),
            ("f32/bytes", v_bytes(b"2.5"), DataType::Float32, "2.5"),
            ("f64/double", v_double(-1.25), DataType::Float64, "-1.25"),
            ("f64/float", v_float(0.5), DataType::Float64, "0.5"),
            ("f64/bytes", v_bytes(b"2.5"), DataType::Float64, "2.5"),
            // Utf8 is the widest arm: it stringifies every variant.
            (
                "utf8/bytes",
                v_bytes("h\u{e9}llo".as_bytes()),
                DataType::Utf8,
                "h\u{e9}llo",
            ),
            ("utf8/int", v_int(42), DataType::Utf8, "42"),
            ("utf8/float", v_float(0.5), DataType::Utf8, "0.5"),
            ("utf8/double", v_double(-1.25), DataType::Utf8, "-1.25"),
            // A DATETIME asked for as text: the arm formats the components itself
            // rather than deferring to a Display impl, so it needs its own cell.
            (
                "utf8/date",
                v_date(2026, 6, 23, 10, 0, 0, 123_456),
                DataType::Utf8,
                "2026-06-23 10:00:00.123456",
            ),
            // Binary / FixedSizeBinary.
            (
                "binary/bytes",
                v_blob(&[0xde, 0xad]),
                DataType::Binary,
                "dead",
            ),
            (
                "fsb/bytes16",
                v_blob(&[
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                    0x0e, 0x0f, 0x10,
                ]),
                DataType::FixedSizeBinary(16),
                "0102030405060708090a0b0c0d0e0f10",
            ),
            // …and the 36-char TEXT rendering of a uuid, which must ALSO land as
            // the 16 canonical bytes. This is the exact shape that once nulled a
            // whole column on PostgreSQL CDC.
            (
                "fsb/uuid_text",
                v_bytes(b"01020304-0506-0708-090a-0b0c0d0e0f10"),
                DataType::FixedSizeBinary(16),
                "0102030405060708090a0b0c0d0e0f10",
            ),
            // Decimal keeps its exact text through to the sink.
            (
                "dec128/bytes",
                v_bytes(b"150.05"),
                DataType::Decimal128(10, 2),
                "150.05",
            ),
            (
                "dec256/bytes",
                v_bytes(b"150.05"),
                DataType::Decimal256(50, 2),
                "150.05",
            ),
            // The Bytes fallbacks of the temporal arms (their typed halves are
            // graded numerically in the sibling test).
            (
                "date32/bytes",
                v_bytes(b"2026-06-23"),
                DataType::Date32,
                "2026-06-23",
            ),
            (
                "ts/bytes",
                v_bytes(b"2026-06-23 10:00:00"),
                ts.clone(),
                "2026-06-23T10:00:00",
            ),
        ];

        let mut wrong = Vec::new();
        for (label, cell, dt, want) in cases {
            let arr = one_array(cell, &dt, label);
            let got = array_value_to_string(arr.as_ref(), 0).expect("renderable");
            if got != want {
                wrong.push(format!("{label}: want {want:?}, got {got:?}"));
            }
        }
        assert!(
            wrong.is_empty(),
            "build_array diverged from its documented conversion on the BINARY \
             protocol (the one `exec_iter` uses):\n  {}",
            wrong.join("\n  ")
        );
    }

    /// The grid cells whose correct answer is an ERROR or a NULL, not a value.
    ///
    /// These are only reachable through `Value::UInt`, and only with a magnitude
    /// past `i64::MAX` — the same measured quirk that hid them in the first place.
    /// A `BIGINT UNSIGNED` past the signed range must FAIL LOUD rather than wrap
    /// into a negative, and a negative signed value asked for an unsigned column
    /// must null rather than reinterpret its bits. Both are silent-corruption
    /// contracts, so a test that only checks the happy path is the wrong shape.
    #[test]
    fn build_array_fails_loud_instead_of_wrapping_an_out_of_range_unsigned() {
        for (label, dt) in [
            ("i16", DataType::Int16),
            ("i32", DataType::Int32),
            ("i64", DataType::Int64),
        ] {
            let (def, bytes) = v_uint(u64::MAX);
            let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
            let got = build_array(&dt, 0, &rows, false, "c", None);
            assert!(
                got.is_err(),
                "{label}: u64::MAX must ERROR on a signed builder, not wrap — got {:?}",
                got.map(|a| a.len())
            );
        }

        // The `*v >= 0` guard on UInt64's signed input: a negative can neither be
        // represented nor bit-reinterpreted, so it must fall through to NULL.
        let (def, bytes) = v_int(-1);
        let rows = fetch_binary_rows(vec![def], vec![vec![Some(bytes)]]);
        let arr = build_array(&DataType::UInt64, 0, &rows, false, "c", None)
            .expect("a negative into UInt64 is a NULL, not an error");
        assert!(
            arr.is_null(0),
            "a negative signed value must NULL on an unsigned builder, never \
             reinterpret its bits as a huge positive"
        );
    }

    /// The temporal half of the grid, graded on the RAW primitive.
    ///
    /// A rendered timestamp is arrow's formatting choice; the integer underneath is
    /// rivet's arithmetic, and that is what can silently drift. Each expectation
    /// below was computed independently of this codebase (days since epoch, micros
    /// since epoch, micros since midnight) rather than read back from a run.
    #[test]
    fn build_array_temporal_grid_converts_to_independently_computed_values() {
        use arrow::array::{Date32Array, Time64MicrosecondArray, TimestampMicrosecondArray};

        // 2026-06-23 is 20627 days after 1970-01-01.
        let a = one_array(
            v_date(2026, 6, 23, 10, 0, 0, 123_456),
            &DataType::Date32,
            "date32/date",
        );
        let d = a.as_any().downcast_ref::<Date32Array>().expect("Date32");
        assert_eq!(d.value(0), 20627, "Date32 must be days since the epoch");

        // 2026-06-23T10:00:00.123456Z is 1_782_208_800_123_456 microseconds after it.
        let a = one_array(
            v_date(2026, 6, 23, 10, 0, 0, 123_456),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            "ts/date",
        );
        let t = a
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Timestamp");
        assert_eq!(t.value(0), 1_782_208_800_123_456, "micros since the epoch");

        // 13:45:30.123456 is 49_530_123_456 microseconds after midnight. Every
        // component is non-zero on purpose: at 00:00:00 the multiply, the add and
        // the divide in that expression all yield 0 and cannot be told apart.
        let a = one_array(
            v_time(false, 0, 13, 45, 30, 123_456),
            &DataType::Time64(TimeUnit::Microsecond),
            "time64/time",
        );
        let t = a
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("Time64");
        assert_eq!(t.value(0), 49_530_123_456, "micros since midnight");

        // A negative TIME, and one carrying whole DAYS — MySQL's TIME is a signed
        // interval, not a clock reading, so both are representable and both were
        // unexercised.
        let a = one_array(
            v_time(true, 2, 1, 0, 0, 0),
            &DataType::Time64(TimeUnit::Microsecond),
            "time64/negative_days",
        );
        let t = a
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("Time64");
        assert_eq!(t.value(0), -(2 * 86_400 + 3_600) * 1_000_000);

        // The Bytes fallback of the time arm.
        let a = one_array(
            v_bytes(b"13:45:30.123456"),
            &DataType::Time64(TimeUnit::Microsecond),
            "time64/bytes",
        );
        let t = a
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("Time64");
        assert_eq!(t.value(0), 49_530_123_456);
    }

    /// The MySQL wire type map, asserted end-to-end against a REAL driver parse.
    ///
    /// Both `mysql_native_type_name` and `mysql_type_to_rivet` read a
    /// `mysql::Column` — a driver type with no public constructor — so for a long
    /// time neither had a unit test at all, and the whole map was covered only
    /// indirectly by live MySQL runs. That left the branch structure unguarded:
    /// stubbing either function's arms survived the entire offline suite.
    ///
    /// The oracle is INDEPENDENT of the code under test on both sides. The input
    /// is a wire packet built from the MySQL protocol spec (type bytes 0..=19 and
    /// 245..=254, `UNSIGNED_FLAG`=32, `ENUM_FLAG`=256, `SET_FLAG`=2048, charset
    /// 63 = binary) and parsed by the driver, not by rivet; the expected side is
    /// a hand-written literal per row. Nothing here recomputes an expectation
    /// with the mapping logic it is grading.
    ///
    /// Every row is a distinction rivet MAKES — a pair that shares a wire type
    /// and diverges on one metadata field is the point (signed vs unsigned,
    /// display width 1 vs not, charset 63 vs not, ENUM_FLAG vs SET_FLAG), because
    /// a fixture with only one side of each pair cannot see the guard collapse.
    #[test]
    fn the_mysql_wire_type_map_is_exhaustive_and_stable() {
        const UNSIGNED: u16 = 32;
        const ENUM_F: u16 = 256;
        const SET_F: u16 = 2048;
        const BIN: u16 = 63; // charset 63 = binary
        const UTF8: u16 = 33;
        let us = RivetTimeUnit::Microsecond;

        // (label, charset, column_length, type byte, flags, decimals, native, rivet)
        // Hand-aligned on purpose: this is a TABLE, and one row per line is what
        // makes a missing or duplicated wire type visible at a glance. rustfmt
        // explodes an 8-tuple into nine lines apiece and the table stops reading
        // as one.
        // The wire type is written as the DRIVER'S OWN CONSTANT, never a bare
        // byte. Two reasons, and the second is load-bearing: `247` tells a reader
        // nothing, and `the_fixture_exercises_every_wire_type_the_mapper_branches_on`
        // compares the `MYSQL_TYPE_*` tokens in this table against the ones the two
        // functions match on. That guard is only possible because the names appear
        // here literally — a table of bare bytes cannot be checked against the code.
        use mysql::consts::ColumnType::*;
        type WireCase = (&'static str, u16, u32, u8, u16, u8, &'static str, RivetType);
        #[rustfmt::skip]
        let cases: Vec<WireCase> = vec![
            // ── integers: the signed/unsigned widening ladder ──
            ("tiny1",   UTF8, 1,  MYSQL_TYPE_TINY as u8, 0, 0, "tinyint(1)", RivetType::Bool),
            ("tiny",    UTF8, 4,  MYSQL_TYPE_TINY as u8, 0, 0, "tinyint", RivetType::Int16),
            // TINYINT UNSIGNED still fits i16 — the name carries the sign, the type need not.
            ("tinyu",   UTF8, 3,  MYSQL_TYPE_TINY as u8, UNSIGNED, 0, "tinyint unsigned", RivetType::Int16),
            ("short",   UTF8, 6,  MYSQL_TYPE_SHORT as u8, 0, 0, "smallint", RivetType::Int16),
            ("shortu",  UTF8, 5,  MYSQL_TYPE_SHORT as u8, UNSIGNED, 0, "smallint unsigned", RivetType::Int32),
            ("int24",   UTF8, 9,  MYSQL_TYPE_INT24 as u8, 0, 0, "int", RivetType::Int32),
            ("int24u",  UTF8, 8,  MYSQL_TYPE_INT24 as u8, UNSIGNED, 0, "int unsigned", RivetType::Int64),
            ("long",    UTF8, 11, MYSQL_TYPE_LONG as u8, 0, 0, "int", RivetType::Int32),
            ("longu",   UTF8, 10, MYSQL_TYPE_LONG as u8, UNSIGNED, 0, "int unsigned", RivetType::Int64),
            ("big",     UTF8, 20, MYSQL_TYPE_LONGLONG as u8, 0, 0, "bigint", RivetType::Int64),
            // BIGINT UNSIGNED is the one integer that does NOT fit a signed Arrow type.
            ("bigu",    UTF8, 20, MYSQL_TYPE_LONGLONG as u8, UNSIGNED, 0, "bigint unsigned", RivetType::UInt64),
            ("year",    UTF8, 4,  MYSQL_TYPE_YEAR as u8, 0, 0, "year", RivetType::Int16),
            // ── floats ──
            ("f32",     UTF8, 12, MYSQL_TYPE_FLOAT as u8, 0, 31, "float", RivetType::Float32),
            ("f64",     UTF8, 22, MYSQL_TYPE_DOUBLE as u8, 0, 31, "double", RivetType::Float64),
            // ── decimal: precision is DERIVED, so vary sign and scale ──
            // signed, scale>0: length 12 = precision 10 + point + sign
            ("dec_s",   UTF8, 12, MYSQL_TYPE_NEWDECIMAL as u8, 0, 2, "decimal",
             RivetType::Decimal { precision: 10, scale: 2 }),
            // unsigned, scale>0: no sign byte, so the same precision needs one less
            ("dec_u",   UTF8, 11, MYSQL_TYPE_NEWDECIMAL as u8, UNSIGNED, 2, "decimal",
             RivetType::Decimal { precision: 10, scale: 2 }),
            // signed, scale 0: no point byte
            ("dec_i",   UTF8, 11, MYSQL_TYPE_NEWDECIMAL as u8, 0, 0, "decimal",
             RivetType::Decimal { precision: 10, scale: 0 }),
            // the pre-5.0 DECIMAL oid shares NEWDECIMAL's arm — exercised so the
            // alias cannot fall out of that arm unnoticed (see the guard).
            ("dec_old", UTF8, 11, MYSQL_TYPE_DECIMAL as u8, 0, 0, "decimal",
             RivetType::Decimal { precision: 10, scale: 0 }),
            // ── strings: charset 63 splits text from bytes on the SAME wire type ──
            ("varchar",   UTF8, 255, MYSQL_TYPE_VAR_STRING as u8, 0, 0, "varchar", RivetType::String),
            ("varbinary", BIN,  255, MYSQL_TYPE_VAR_STRING as u8, 0, 0, "varbinary", RivetType::Binary),
            ("char",      UTF8, 10,  MYSQL_TYPE_STRING as u8, 0, 0, "char", RivetType::String),
            ("binary",    BIN,  10,  MYSQL_TYPE_STRING as u8, 0, 0, "binary", RivetType::Binary),
            // MYSQL_TYPE_VARCHAR is a THIRD spelling sharing the same arm.
            ("varchar_oid", UTF8, 255, MYSQL_TYPE_VARCHAR as u8, 0, 0, "varchar", RivetType::String),
            // ── ENUM/SET ride the string wire types and are told apart by FLAG ──
            ("enum",    UTF8, 10, MYSQL_TYPE_STRING as u8, ENUM_F, 0, "enum", RivetType::Enum),
            ("set",     UTF8, 10, MYSQL_TYPE_STRING as u8, SET_F, 0, "set", RivetType::Enum),
            // …and BOTH functions also carry a belt-and-suspenders arm for the
            // DEDICATED oids, which some drivers/protocol configurations do
            // surface. Those arms are a SEPARATE branch from the flag path above:
            // covering only the common spelling left `delete match arm
            // MYSQL_TYPE_ENUM` / `…_SET` alive in both functions — the three
            // survivors of this table's first mutation run, now dead.
            ("enum_oid", UTF8, 10, MYSQL_TYPE_ENUM as u8, 0, 0, "enum", RivetType::Enum),
            ("set_oid",  UTF8, 10, MYSQL_TYPE_SET as u8, 0, 0, "set", RivetType::Enum),
            // ── blobs: same charset-63 split, and FOUR oids share one arm ──
            ("blob",     BIN,  65535, MYSQL_TYPE_BLOB as u8, 0, 0, "blob", RivetType::Binary),
            ("textblob", UTF8, 65535, MYSQL_TYPE_BLOB as u8, 0, 0, "blob", RivetType::Text),
            ("tinyblob", BIN,  255,   MYSQL_TYPE_TINY_BLOB as u8, 0, 0, "blob", RivetType::Binary),
            ("medblob",  BIN,  16777215, MYSQL_TYPE_MEDIUM_BLOB as u8, 0, 0, "blob", RivetType::Binary),
            ("longblob", UTF8, 4294967295, MYSQL_TYPE_LONG_BLOB as u8, 0, 0, "blob", RivetType::Text),
            ("json",     UTF8, 4294967295, MYSQL_TYPE_JSON as u8, 0, 0, "json", RivetType::Json),
            // ── temporal: DATETIME is naive, TIMESTAMP is UTC — the whole point ──
            // Each of these has a `*2` (or NEWDATE) alias sharing its arm; both
            // spellings are exercised so a future split of the arm cannot silently
            // leave one half untested.
            ("date",      BIN, 10, MYSQL_TYPE_DATE as u8, 0, 0, "date", RivetType::Date),
            // MYSQL_TYPE_NEWDATE (0x0e) is deliberately ABSENT and cannot be added:
            // the driver's `TryFrom<u8> for ColumnType` skips 0x0e entirely (its
            // list steps 0x0d -> 0x0f), so a column definition carrying it fails
            // the resultset parse with `Unknown column type 14` before rivet's
            // mapper is ever called — measured, not assumed. Both mappers keep a
            // NEWDATE arm; it is unreachable through the client protocol, and the
            // guard records it as such rather than demanding an impossible row.
            ("time",      BIN, 10, MYSQL_TYPE_TIME as u8, 0, 0, "time", RivetType::Time { unit: us }),
            ("time2",     BIN, 10, MYSQL_TYPE_TIME2 as u8, 0, 0, "time", RivetType::Time { unit: us }),
            ("datetime",  BIN, 19, MYSQL_TYPE_DATETIME as u8, 0, 0, "datetime",
             RivetType::Timestamp { unit: us, timezone: None }),
            ("datetime2", BIN, 19, MYSQL_TYPE_DATETIME2 as u8, 0, 0, "datetime",
             RivetType::Timestamp { unit: us, timezone: None }),
            ("ts",        BIN, 19, MYSQL_TYPE_TIMESTAMP as u8, 0, 0, "timestamp",
             RivetType::Timestamp { unit: us, timezone: Some("UTC".into()) }),
            ("ts2",       BIN, 19, MYSQL_TYPE_TIMESTAMP2 as u8, 0, 0, "timestamp",
             RivetType::Timestamp { unit: us, timezone: Some("UTC".into()) }),
            // ── BIT: width 1 is a boolean, wider is an integer that must not truncate ──
            ("bit1",  BIN, 1,  MYSQL_TYPE_BIT as u8, UNSIGNED, 0, "bit(1)", RivetType::Bool),
            ("bit16", BIN, 16, MYSQL_TYPE_BIT as u8, UNSIGNED, 0, "bit", RivetType::Int64),
        ];

        let defs = cases
            .iter()
            .map(|(name, cs, len, ty, flags, dec, _, _)| {
                column_definition(name, *cs, *len, *ty, *flags, *dec)
            })
            .collect();
        let got = map_columns(defs);
        assert_eq!(got.len(), cases.len(), "one mapping per column definition");

        let mut wrong = Vec::new();
        for ((label, .., want_name, want_type), (got_name, got_type)) in cases.iter().zip(&got) {
            if got_name != want_name || got_type != want_type {
                wrong.push(format!(
                    "{label}: want ({want_name}, {want_type:?}), got ({got_name}, {got_type:?})"
                ));
            }
        }
        assert!(
            wrong.is_empty(),
            "the MySQL wire type map diverged from its spec. Each line is a column \
             the driver parsed from a protocol-shaped packet, mapped by rivet, and \
             compared to a hand-written expectation:\n  {}",
            wrong.join("\n  ")
        );
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
