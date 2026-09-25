//! Oracle result metadata → Rivet/Arrow types, and fetched rows → Arrow batches.
//!
//! rivet builds Arrow itself: the driver's own `query_arrow` floats bare `NUMBER`
//! and drops a `TIMESTAMP WITH TIME ZONE`'s zone. The export query is first
//! re-projected server-side ([`super::projection_expr`]) so every column that
//! reaches this module is one of: NUMBER, BINARY_FLOAT/DOUBLE, BOOLEAN, DATE,
//! TIMESTAMP, a character type (incl. CLOB fetched inline), or a binary type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Decimal128Builder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Schema, TimeUnit as ArrowTimeUnit};
use arrow::record_batch::RecordBatch;
use oracledb::{Metadata, OracleIntervalDS, OracleIntervalYM, OracleNumber, OracleTimestamp, Row};

use super::Ora;
use crate::error::Result;
use crate::types::decimal::decimal_str_to_scaled_i128;
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit, TypeMapping, build_arrow_field,
};

/// Warning attached to a bare `NUMBER` (no declared precision) exported as exact text.
pub(super) const BARE_NUMBER_WARNING: &str = "NUMBER without declared precision → exact decimal text (Utf8): its range (1E-130..1E126) \
     fits no fixed-scale decimal. Declare the type to load it as a number, e.g. \
     `columns: {ID: \"decimal(38,0)\"}`.";

/// Warning attached to a `TIMESTAMP(7..9)` column truncated to microseconds.
pub(super) const TIMESTAMP_NS_WARNING: &str = "TIMESTAMP(7..9) → Timestamp(microsecond): the sub-microsecond digits are truncated; \
     TIMESTAMP(0..6) is exact.";

/// The Rivet type for one re-projected column; `native` is its DECLARED type
/// (re-projection turns a zoned timestamp into a UTC `TIMESTAMP`, JSON into text).
pub(super) fn oracle_type_to_rivet(
    meta: &Metadata,
    native: &str,
    overrides: &ColumnOverrides,
) -> RivetType {
    crate::types::resolve_or(overrides, meta.name(), || match meta.db_type().name() {
        _ if native.starts_with("timestamp_tz") || native.starts_with("timestamp_ltz") => {
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            }
        }
        _ if native.starts_with("interval") => RivetType::Interval,
        _ if native == "json" => RivetType::Json,
        "DB_TYPE_NUMBER" => number_type(meta.precision(), meta.scale()),
        "DB_TYPE_BINARY_FLOAT" => RivetType::Float32,
        "DB_TYPE_BINARY_DOUBLE" => RivetType::Float64,
        "DB_TYPE_BOOLEAN" => RivetType::Bool,
        "DB_TYPE_DATE" | "DB_TYPE_TIMESTAMP" => RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        },
        "DB_TYPE_VARCHAR"
        | "DB_TYPE_NVARCHAR"
        | "DB_TYPE_CHAR"
        | "DB_TYPE_NCHAR"
        | "DB_TYPE_LONG"
        | "DB_TYPE_LONG_NVARCHAR"
        | "DB_TYPE_CLOB"
        | "DB_TYPE_NCLOB" => RivetType::String,
        "DB_TYPE_RAW" | "DB_TYPE_LONG_RAW" | "DB_TYPE_BLOB" => RivetType::Binary,
        other => RivetType::Unsupported {
            native_type: native_label(other),
            reason: format!(
                "Oracle column type {} has no Rivet mapping; select it through a \
                     conversion in a `query:` (e.g. TO_CHAR) or drop it",
                native_label(other)
            ),
        },
    })
}

/// NUMBER(p,s): small integers, then `Decimal`; bare `NUMBER`/`FLOAT` as exact text.
fn number_type(precision: u8, scale: i8) -> RivetType {
    match (precision, scale) {
        // Bare NUMBER (p=0) and FLOAT(b) (scale -127): no fixed-scale decimal holds them.
        (0, _) | (_, -127) => RivetType::String,
        (1..=9, 0) => RivetType::Int32,
        (10..=18, 0) => RivetType::Int64,
        (p, s) => RivetType::Decimal {
            precision: p,
            scale: s,
        },
    }
}

/// The lowercase native type label (`number`, `timestamp`, …) for reports.
fn native_label(db_type: &str) -> String {
    db_type.trim_start_matches("DB_TYPE_").to_lowercase()
}

/// True when `meta` is a bare `NUMBER`/`FLOAT` (exported as exact text).
fn is_bare_number(meta: &Metadata) -> bool {
    meta.db_type().name() == "DB_TYPE_NUMBER" && (meta.precision() == 0 || meta.scale() == -127)
}

/// `TypeMapping` for every column, with the Oracle-specific warnings attached.
pub(super) fn oracle_type_mappings(
    metas: &[Metadata],
    native: &[String],
    overrides: &ColumnOverrides,
) -> Vec<TypeMapping> {
    metas
        .iter()
        .zip(native)
        .map(|(m, native)| {
            let rivet = oracle_type_to_rivet(m, native, overrides);
            let source = SourceColumn::simple(m.name(), native.clone(), m.nullable());
            let mapping = TypeMapping::from_source(&source, rivet);
            if overrides.contains_key(m.name()) {
                mapping
            } else if is_bare_number(m) {
                mapping.with_warning(BARE_NUMBER_WARNING)
            } else if native.starts_with("timestamp") && m.scale() > 6 {
                mapping.with_warning(TIMESTAMP_NS_WARNING)
            } else {
                mapping
            }
        })
        .collect()
}

/// The Arrow schema for a result set; an error names every column without a mapping.
pub(super) fn oracle_schema(
    metas: &[Metadata],
    native: &[String],
    overrides: &ColumnOverrides,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(metas.len());
    let mut errors = Vec::new();
    for mapping in oracle_type_mappings(metas, native, overrides) {
        match build_arrow_field(&mapping) {
            Some(f) => fields.push(f),
            None => {
                let reason = match &mapping.rivet_type {
                    RivetType::Unsupported { reason, .. } => reason.clone(),
                    other => format!("no Arrow type for {other:?}"),
                };
                errors.push(format!("  • {}: {reason}", mapping.column_name));
            }
        }
    }
    if !errors.is_empty() {
        anyhow::bail!(
            "Oracle export: {} column(s) have no safe type mapping — add column overrides in \
             rivet.yaml (`columns:`):\n{}",
            errors.len(),
            errors.join("\n")
        );
    }
    Ok(Schema::new(fields))
}

/// An Oracle interval cell as the ISO 8601 duration rivet writes for every engine.
fn interval_iso(row: &Row, idx: usize, db_type: &str) -> Result<Option<String>> {
    Ok(match db_type {
        "DB_TYPE_INTERVAL_DS" => row.get::<Option<OracleIntervalDS>>(idx).ora()?.map(|v| {
            let us = i64::from(v.hours()) * 3_600_000_000
                + i64::from(v.minutes()) * 60_000_000
                + i64::from(v.seconds()) * 1_000_000
                + i64::from(v.nanoseconds()) / 1_000;
            crate::source::postgres::pg_interval_to_iso8601(0, v.days(), us)
        }),
        _ => row.get::<Option<OracleIntervalYM>>(idx).ora()?.map(|v| {
            crate::source::postgres::pg_interval_to_iso8601(
                v.years() * 12 + i32::from(v.months()),
                0,
                0,
            )
        }),
    })
}

/// Microseconds since the Unix epoch for an Oracle timestamp's fields, read as UTC.
pub(super) fn timestamp_micros(t: &OracleTimestamp) -> Result<i64> {
    let date = chrono::NaiveDate::from_ymd_opt(t.year() as i32, t.month() as u32, t.day() as u32)
        .ok_or_else(|| anyhow::anyhow!("oracle: invalid date {t}"))?;
    let time = chrono::NaiveTime::from_hms_nano_opt(
        t.hour() as u32,
        t.minute() as u32,
        t.second() as u32,
        t.nanoseconds(),
    )
    .ok_or_else(|| anyhow::anyhow!("oracle: invalid time {t}"))?;
    Ok(date.and_time(time).and_utc().timestamp_micros())
}

/// True when `row`'s zero-length flag column (see `super::Projection`) is set.
fn flagged_empty(row: &Row, flag: Option<usize>) -> Result<bool> {
    match flag {
        Some(j) => Ok(row.get::<Option<OracleNumber>>(j).ora()?.is_some()),
        None => Ok(false),
    }
}

/// One Arrow column from the fetched rows, typed by `dt`.
fn build_column(
    rows: &[Row],
    idx: usize,
    name: &str,
    dt: &DataType,
    max_value_bytes: Option<usize>,
    empty_flag: Option<usize>,
) -> Result<ArrayRef> {
    let number = |r: &Row| {
        r.get::<Option<OracleNumber>>(idx)
            .ora()
            .map(|v| v.map(|n| n.to_string()))
    };
    Ok(match dt {
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(rows.len());
            for r in rows {
                b.append_option(number(r)?.map(|s| s.parse::<i32>()).transpose()?);
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for r in rows {
                b.append_option(number(r)?.map(|s| s.parse::<i64>()).transpose()?);
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(p, s) => {
            let mut b =
                Decimal128Builder::with_capacity(rows.len()).with_precision_and_scale(*p, *s)?;
            for r in rows {
                match number(r)? {
                    Some(text) => {
                        b.append_value(decimal_str_to_scaled_i128(&text, *s).ok_or_else(|| {
                            anyhow::anyhow!("oracle: {name} = {text} does not fit decimal({p},{s})")
                        })?)
                    }
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(rows.len());
            for r in rows {
                b.append_option(r.get::<Option<f32>>(idx).ora()?);
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for r in rows {
                b.append_option(r.get::<Option<f64>>(idx).ora()?);
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for r in rows {
                b.append_option(r.get::<Option<bool>>(idx).ora()?);
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(ArrowTimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for r in rows {
                match r.get::<Option<OracleTimestamp>>(idx).ora()? {
                    Some(t) => b.append_value(timestamp_micros(&t)?),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish().with_timezone_opt(tz.clone()))
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), 0);
            for r in rows {
                match r.get::<Option<Vec<u8>>>(idx).ora()? {
                    Some(v) => {
                        crate::source::value_within_ceiling(name, v.len(), max_value_bytes)?;
                        b.append_value(v);
                    }
                    None if flagged_empty(r, empty_flag)? => b.append_value(b""),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows.len(), 0);
            let db_type = rows
                .first()
                .and_then(|r| r.columns().get(idx))
                .map_or("", |m| m.db_type().name());
            for r in rows {
                let v = match db_type {
                    "DB_TYPE_NUMBER" => number(r)?,
                    "DB_TYPE_INTERVAL_DS" | "DB_TYPE_INTERVAL_YM" => interval_iso(r, idx, db_type)?,
                    _ => r.get::<Option<String>>(idx).ora()?,
                };
                match v {
                    Some(v) => {
                        crate::source::value_within_ceiling(name, v.len(), max_value_bytes)?;
                        b.append_value(v);
                    }
                    None if flagged_empty(r, empty_flag)? => b.append_value(""),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        other => anyhow::bail!("oracle: column {name}: no row decoder for Arrow {other:?}"),
    })
}

/// A record batch for `rows` against the already-built `schema`.
pub(super) fn rows_to_batch(
    rows: &[Row],
    schema: &Arc<Schema>,
    max_value_bytes: Option<usize>,
    empty_flags: &[Option<usize>],
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let flag = empty_flags.get(i).copied().flatten();
            build_column(rows, i, f.name(), f.data_type(), max_value_bytes, flag)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_precision_and_scale_pick_the_narrowest_lossless_type() {
        assert_eq!(number_type(9, 0), RivetType::Int32);
        assert_eq!(number_type(10, 0), RivetType::Int64);
        assert_eq!(number_type(18, 0), RivetType::Int64);
        assert_eq!(
            number_type(19, 0),
            RivetType::Decimal {
                precision: 19,
                scale: 0
            }
        );
        assert_eq!(
            number_type(5, 2),
            RivetType::Decimal {
                precision: 5,
                scale: 2
            }
        );
        assert_eq!(number_type(0, -127), RivetType::String, "bare NUMBER");
        assert_eq!(number_type(126, -127), RivetType::String, "FLOAT(126)");
    }

    #[test]
    fn timestamp_fields_become_utc_micros_across_the_whole_oracle_range() {
        let t = OracleTimestamp::new_timestamp(2024, 2, 29, 13, 14, 15, 123_456_789);
        assert_eq!(timestamp_micros(&t).unwrap(), 1_709_212_455_123_456);
        let bc = OracleTimestamp::new_date(-4712, 1, 1);
        assert!(timestamp_micros(&bc).unwrap() < 0);
        let max = OracleTimestamp::new_timestamp(9999, 12, 31, 23, 59, 59, 999_999_999);
        assert_eq!(timestamp_micros(&max).unwrap(), 253_402_300_799_999_999);
    }
}
