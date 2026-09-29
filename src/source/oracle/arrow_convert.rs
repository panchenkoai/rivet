//! Oracle result metadata → Rivet/Arrow types, and fetched rows → Arrow batches.
//!
//! rivet builds Arrow itself: the driver's own `query_arrow` floats bare `NUMBER`
//! and drops a `TIMESTAMP WITH TIME ZONE`'s zone. The export query is first
//! re-projected server-side ([`super::projection_expr`]) so every column that
//! reaches this module is one of: NUMBER, BINARY_FLOAT/DOUBLE, BOOLEAN, DATE,
//! TIMESTAMP, a character type (incl. CLOB fetched inline), or a binary type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Schema, TimeUnit as ArrowTimeUnit};
use arrow::record_batch::RecordBatch;
use oracledb::{Metadata, OracleIntervalDS, OracleIntervalYM, OracleNumber, OracleTimestamp, Row};

use super::Ora;
use super::kind::{OraKind, native_label};
use crate::error::Result;
use crate::types::decimal::decimal_str_to_scaled_i128;
use crate::types::{
    ColumnOverrides, RivetType, SourceColumn, TimeUnit, TypeMapping, build_arrow_field,
};

/// Warning attached to a bare `NUMBER` column `name` (no declared precision) exported as exact text.
pub(super) fn bare_number_warning(name: &str) -> String {
    format!(
        "NUMBER without declared precision → exact decimal text (Utf8): its range \
         (1E-130..1E126) fits no fixed-scale decimal. Declare the type to load it as a \
         number: `columns: {{{name}: \"decimal(38,0)\"}}` for whole numbers, or a scale \
         the values need, e.g. \"decimal(38,10)\""
    )
}

/// Warning attached to a `TIMESTAMP(7..9)` column truncated to microseconds.
pub(super) const TIMESTAMP_NS_WARNING: &str = "TIMESTAMP(7..9) / INTERVAL DAY TO SECOND(7..9) → microseconds: the sub-microsecond \
     digits are truncated; a fractional precision of 0..6 is exact.";

/// The Rivet type for one re-projected column; `native` is its DECLARED type
/// (re-projection turns a zoned timestamp into a UTC `TIMESTAMP`, JSON into text).
pub(super) fn oracle_type_to_rivet(
    meta: &Metadata,
    native: &str,
    overrides: &ColumnOverrides,
) -> RivetType {
    crate::types::resolve_or(overrides, meta.name(), || {
        autodetect(
            OraKind::of(meta),
            native,
            meta.precision(),
            meta.scale(),
            &native_label(meta),
        )
    })
}

/// The Rivet type of a re-projected column of `kind`, declared as `native`; `label` names it when unmapped.
fn autodetect(kind: OraKind, native: &str, precision: u8, scale: i8, label: &str) -> RivetType {
    match kind {
        _ if native.starts_with("timestamp_tz") || native.starts_with("timestamp_ltz") => {
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            }
        }
        _ if native.starts_with("interval") => RivetType::Interval,
        _ if native == "json" => RivetType::Json,
        OraKind::Number => number_type(precision, scale),
        OraKind::BinaryFloat => RivetType::Float32,
        OraKind::BinaryDouble => RivetType::Float64,
        OraKind::Boolean => RivetType::Bool,
        OraKind::Date | OraKind::Timestamp => RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        },
        OraKind::Text | OraKind::Clob => RivetType::String,
        OraKind::Raw | OraKind::Blob => RivetType::Binary,
        _ => RivetType::Unsupported {
            native_type: label.to_string(),
            reason: format!(
                "Oracle column type {label} has no Rivet mapping; select a convertible \
                 expression of it in a `query:`, or drop it"
            ),
        },
    }
}

/// NUMBER(p,s): small integers, then `Decimal`; bare `NUMBER`/`FLOAT` as exact text.
/// Oracle's `s > p` and negative `s` are widened to the lossless decimal Parquet accepts.
fn number_type(precision: u8, scale: i8) -> RivetType {
    let decimal = |precision: u8, scale: i8| RivetType::Decimal { precision, scale };
    match (precision, scale) {
        // Bare NUMBER (p=0) and FLOAT(b) (scale -127): no fixed-scale decimal holds them.
        (0, _) | (_, -127) => RivetType::String,
        (1..=9, 0) => RivetType::Int32,
        (10..=18, 0) => RivetType::Int64,
        (p, s) if s.is_negative() => match p.checked_add(s.unsigned_abs()).filter(|w| *w <= 38) {
            Some(w) => decimal(w, 0),
            None => RivetType::String,
        },
        // Past 38 digits no Decimal128 holds it: exact text, like a bare NUMBER.
        (_, s) if s > 38 => RivetType::String,
        (p, s) => decimal(p.max(s.unsigned_abs()), s),
    }
}

/// True for a bare `NUMBER`/`FLOAT` (exported as exact text).
fn is_bare_number(kind: OraKind, precision: u8, scale: i8) -> bool {
    kind == OraKind::Number && (precision == 0 || scale == -127)
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
            } else if is_bare_number(OraKind::of(m), m.precision(), m.scale()) {
                mapping.with_warning(bare_number_warning(m.name()))
            } else if sub_microsecond(native, m.scale()) {
                TypeMapping {
                    fidelity: crate::types::TypeFidelity::Lossy,
                    ..mapping
                }
                .with_warning(TIMESTAMP_NS_WARNING)
            } else {
                mapping
            }
        })
        .collect()
}

/// True for a TIMESTAMP / INTERVAL DAY TO SECOND whose fraction is finer than the µs rivet keeps.
pub(super) fn sub_microsecond(native: &str, scale: i8) -> bool {
    (native.starts_with("timestamp") || native.starts_with("interval_ds")) && scale > 6
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
            "Oracle export: {} column(s) have no safe type mapping:\n{}",
            errors.len(),
            errors.join("\n")
        );
    }
    Ok(Schema::new(fields))
}

/// An Oracle interval cell as the ISO 8601 duration rivet writes for every engine.
fn interval_iso(row: &Row, idx: usize, kind: OraKind) -> Result<Option<String>> {
    Ok(match kind {
        OraKind::IntervalDs => row.get::<Option<OracleIntervalDS>>(idx).ora()?.map(|v| {
            interval_ds_iso(
                v.days(),
                v.hours(),
                v.minutes(),
                v.seconds(),
                v.nanoseconds(),
            )
        }),
        _ => row
            .get::<Option<OracleIntervalYM>>(idx)
            .ora()?
            .map(|v| interval_ym_iso(v.years(), i32::from(v.months()))),
    })
}

/// ISO 8601 for a DAY TO SECOND interval's fields, truncated to microseconds.
fn interval_ds_iso(days: i32, hours: i8, minutes: i8, seconds: i8, nanos: i32) -> String {
    let us = i64::from(hours) * 3_600_000_000
        + i64::from(minutes) * 60_000_000
        + i64::from(seconds) * 1_000_000
        + i64::from(nanos) / 1_000;
    crate::source::postgres::pg_interval_to_iso8601(0, days, us)
}

/// ISO 8601 for a YEAR TO MONTH interval, straight from its fields (YEAR(9) overflows i32 months).
fn interval_ym_iso(years: i32, months: i32) -> String {
    match (years, months) {
        (0, 0) => "PT0S".to_string(),
        (0, m) => format!("P{m}M"),
        (y, 0) => format!("P{y}Y"),
        (y, m) => format!("P{y}Y{m}M"),
    }
}

/// Oracle's signed year as chrono's proleptic one: Oracle has no year 0, so -1 (1 BC) is chrono 0.
pub(super) fn chrono_year(oracle: i32) -> i32 {
    if oracle.is_negative() {
        oracle + 1
    } else {
        oracle
    }
}

/// An Oracle timestamp's fields as a proleptic-Gregorian date-time, read as UTC.
fn timestamp_datetime(t: &OracleTimestamp) -> Result<chrono::NaiveDateTime> {
    let year = chrono_year(t.year() as i32);
    let date = chrono::NaiveDate::from_ymd_opt(year, t.month() as u32, t.day() as u32)
        .ok_or_else(|| anyhow::anyhow!("oracle: invalid date {t}"))?;
    let time = chrono::NaiveTime::from_hms_nano_opt(
        t.hour() as u32,
        t.minute() as u32,
        t.second() as u32,
        t.nanoseconds(),
    )
    .ok_or_else(|| anyhow::anyhow!("oracle: invalid time {t}"))?;
    Ok(date.and_time(time))
}

/// Microseconds since the Unix epoch for an Oracle timestamp's fields, read as UTC.
pub(super) fn timestamp_micros(t: &OracleTimestamp) -> Result<i64> {
    Ok(timestamp_datetime(t)?.and_utc().timestamp_micros())
}

/// Nanoseconds since the Unix epoch, or an error naming `name` outside Arrow's 1677..2262 range.
fn timestamp_nanos(t: &OracleTimestamp, name: &str) -> Result<i64> {
    timestamp_datetime(t)?
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "oracle: column {name} = {t} is outside the nanosecond timestamp range \
                 (1677-09-21..2262-04-11); declare it `timestamp` (microseconds) instead"
            )
        })
}

/// Days since the Unix epoch, or an error naming `name` when the value has a time of day.
fn date_days(t: &OracleTimestamp, name: &str) -> Result<i32> {
    let dt = timestamp_datetime(t)?;
    anyhow::ensure!(
        dt.time() == chrono::NaiveTime::MIN,
        "oracle: column {name} = {t} has a time of day, which a `date` override would drop; \
         declare it `timestamp`, or select TRUNC({name}) in a `query:`"
    );
    i32::try_from(dt.and_utc().timestamp() / 86_400).map_err(Into::into)
}

/// Oracle's own `SYYYY-MM-DD"T"HH24:MI:SS.FF` text of a timestamp: 6 fractional digits, 9 when finer.
fn timestamp_text(t: &OracleTimestamp) -> String {
    let y = t.year() as i32;
    let year = if y < 0 {
        format!("-{:04}", -y)
    } else {
        format!("{y:04}")
    };
    let ns = t.nanoseconds();
    let frac = if ns.is_multiple_of(1_000) {
        format!("{:06}", ns / 1_000)
    } else {
        format!("{ns:09}")
    };
    format!(
        "{year}-{:02}-{:02}T{:02}:{:02}:{:02}.{frac}",
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

/// Upper-case hex, as Oracle's `RAWTOHEX` renders bytes.
fn upper_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
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
    let kind = rows
        .first()
        .and_then(|r| r.columns().get(idx))
        .map_or(OraKind::Other, OraKind::of);
    Ok(match dt {
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(rows.len());
            for r in rows {
                b.append_option(number(r)?.map(|s| s.parse::<i16>()).transpose()?);
            }
            Arc::new(b.finish())
        }
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
                b.append_option(match kind {
                    OraKind::BinaryFloat => r.get::<Option<f32>>(idx).ora()?.map(f64::from),
                    _ => r.get::<Option<f64>>(idx).ora()?,
                });
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
        DataType::Timestamp(ArrowTimeUnit::Nanosecond, tz) => {
            let mut b = TimestampNanosecondBuilder::with_capacity(rows.len());
            for r in rows {
                match r.get::<Option<OracleTimestamp>>(idx).ora()? {
                    Some(t) => b.append_value(timestamp_nanos(&t, name)?),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish().with_timezone_opt(tz.clone()))
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(rows.len());
            for r in rows {
                match r.get::<Option<OracleTimestamp>>(idx).ora()? {
                    Some(t) => b.append_value(date_days(&t, name)?),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::FixedSizeBinary(width) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), *width);
            for r in rows {
                match r.get::<Option<Vec<u8>>>(idx).ora()? {
                    Some(v) => b.append_value(&v).map_err(|_| {
                        anyhow::anyhow!(
                            "oracle: column {name} holds a {}-byte value; a `uuid` override needs \
                             exactly {width} bytes (RAW({width}))",
                            v.len()
                        )
                    })?,
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
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
            for r in rows {
                let v = match kind {
                    OraKind::Number => number(r)?,
                    OraKind::IntervalDs | OraKind::IntervalYm => interval_iso(r, idx, kind)?,
                    OraKind::Date | OraKind::Timestamp => r
                        .get::<Option<OracleTimestamp>>(idx)
                        .ora()?
                        .map(|t| timestamp_text(&t)),
                    OraKind::BinaryFloat => r.get::<Option<f32>>(idx).ora()?.map(|v| v.to_string()),
                    OraKind::BinaryDouble => {
                        r.get::<Option<f64>>(idx).ora()?.map(|v| v.to_string())
                    }
                    OraKind::Boolean => r.get::<Option<bool>>(idx).ora()?.map(|v| v.to_string()),
                    OraKind::Raw | OraKind::Blob => {
                        r.get::<Option<Vec<u8>>>(idx).ora()?.map(|v| upper_hex(&v))
                    }
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
    row_index: &[usize],
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let flag = empty_flags.get(i).copied().flatten();
            let at = row_index.get(i).copied().unwrap_or(i);
            build_column(rows, at, f.name(), f.data_type(), max_value_bytes, flag)
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
        let dec = |precision, scale| RivetType::Decimal { precision, scale };
        assert_eq!(number_type(3, 5), dec(5, 5), "s > p widens to (s,s)");
        assert_eq!(number_type(10, 60), RivetType::String, "s > 38: exact text");
        assert_eq!(
            number_type(10, 38),
            dec(38, 38),
            "s = 38 still fits Decimal128"
        );
        assert_eq!(
            number_type(5, -2),
            dec(7, 0),
            "negative scale widens to integers"
        );
        assert_eq!(
            number_type(38, -1),
            RivetType::String,
            "past 38 digits: exact text"
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
        // 1 BC = Oracle year -1; DuckDB's make_date(0, 6, 15) is the independent value.
        let one_bc = OracleTimestamp::new_date(-1, 6, 15);
        assert_eq!(timestamp_micros(&one_bc).unwrap(), -62_152_876_800_000_000);
        let max = OracleTimestamp::new_timestamp(9999, 12, 31, 23, 59, 59, 999_999_999);
        assert_eq!(timestamp_micros(&max).unwrap(), 253_402_300_799_999_999);
    }

    #[test]
    fn a_year_to_month_interval_renders_past_the_i32_month_range() {
        assert_eq!(interval_ym_iso(999_999_999, 11), "P999999999Y11M");
        assert_eq!(interval_ym_iso(-999_999_999, -11), "P-999999999Y-11M");
        assert_eq!(interval_ym_iso(0, -3), "P-3M");
        assert_eq!(interval_ym_iso(2, 0), "P2Y");
        assert_eq!(interval_ym_iso(0, 0), "PT0S");
    }

    #[test]
    fn the_bare_number_warning_names_its_own_column() {
        let w = bare_number_warning("AMT");
        assert!(
            w.contains("`columns: {AMT: \"decimal(38,0)\"}` for whole numbers"),
            "{w}"
        );
    }

    #[test]
    fn timestamp_text_is_oracles_signed_iso_rendering() {
        let t = OracleTimestamp::new_timestamp(2024, 2, 29, 13, 14, 15, 123_456_000);
        assert_eq!(timestamp_text(&t), "2024-02-29T13:14:15.123456");
        let ns = OracleTimestamp::new_timestamp(2024, 2, 29, 13, 14, 15, 1);
        assert_eq!(timestamp_text(&ns), "2024-02-29T13:14:15.000000001");
        let bc = OracleTimestamp::new_date(-1, 6, 15);
        assert_eq!(timestamp_text(&bc), "-0001-06-15T00:00:00.000000");
        let one = OracleTimestamp::new_date(1, 1, 1);
        assert_eq!(timestamp_text(&one), "0001-01-01T00:00:00.000000");
    }

    #[test]
    fn a_date_is_whole_days_and_a_time_of_day_is_refused_by_name() {
        assert_eq!(
            date_days(&OracleTimestamp::new_date(1970, 1, 2), "D").unwrap(),
            1
        );
        assert_eq!(
            date_days(&OracleTimestamp::new_date(1969, 12, 31), "D").unwrap(),
            -1
        );
        let noon = OracleTimestamp::new_timestamp(1970, 1, 2, 12, 0, 0, 0);
        let err = date_days(&noon, "DT").unwrap_err().to_string();
        assert!(
            err.contains("column DT = ") && err.contains("time of day"),
            "{err}"
        );
        let ns = OracleTimestamp::new_timestamp(1970, 1, 2, 0, 0, 0, 1);
        assert!(
            date_days(&ns, "DT").is_err(),
            "a nanosecond is a time of day"
        );
    }

    #[test]
    fn nanosecond_timestamps_keep_every_digit_and_refuse_past_arrows_range() {
        let t = OracleTimestamp::new_timestamp(1970, 1, 1, 0, 0, 1, 123_456_789);
        assert_eq!(timestamp_nanos(&t, "T").unwrap(), 1_123_456_789);
        let far = OracleTimestamp::new_date(9999, 12, 31);
        let err = timestamp_nanos(&far, "T").unwrap_err().to_string();
        assert!(err.contains("column T = "), "{err}");
    }

    #[test]
    fn every_kind_autodetects_to_its_rivet_type() {
        let ts = |tz: Option<&str>| RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: tz.map(Into::into),
        };
        let a = |k, native| autodetect(k, native, 10, 2, "lbl");
        assert_eq!(a(OraKind::Timestamp, "timestamp_tz(6)"), ts(Some("UTC")));
        assert_eq!(a(OraKind::Timestamp, "timestamp_ltz(6)"), ts(Some("UTC")));
        assert_eq!(a(OraKind::Timestamp, "timestamp(6)"), ts(None));
        assert_eq!(a(OraKind::Date, "date"), ts(None));
        assert_eq!(a(OraKind::Text, "interval_ds"), RivetType::Interval);
        assert_eq!(a(OraKind::Clob, "json"), RivetType::Json);
        // VECTOR, XMLTYPE and object types arrive re-projected to CLOB.
        for n in ["vector", "object", "xmltype"] {
            assert_eq!(a(OraKind::Clob, n), RivetType::String, "{n}");
        }
        assert_eq!(
            a(OraKind::Number, "number(10,2)"),
            RivetType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(a(OraKind::BinaryFloat, "binary_float"), RivetType::Float32);
        assert_eq!(
            a(OraKind::BinaryDouble, "binary_double"),
            RivetType::Float64
        );
        assert_eq!(a(OraKind::Boolean, "boolean"), RivetType::Bool);
        assert_eq!(a(OraKind::Text, "varchar"), RivetType::String);
        assert_eq!(a(OraKind::Clob, "clob"), RivetType::String);
        assert_eq!(a(OraKind::Raw, "raw"), RivetType::Binary);
        assert_eq!(a(OraKind::Blob, "blob"), RivetType::Binary);
        match a(OraKind::Other, "bfile") {
            RivetType::Unsupported {
                native_type,
                reason,
            } => {
                assert_eq!(native_type, "lbl");
                assert!(reason.contains("Oracle column type lbl has no Rivet mapping"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn only_an_undeclared_number_is_bare() {
        assert!(is_bare_number(OraKind::Number, 0, 0));
        assert!(is_bare_number(OraKind::Number, 126, -127));
        assert!(!is_bare_number(OraKind::Number, 10, 0));
        assert!(!is_bare_number(OraKind::Number, 1, -1));
        assert!(!is_bare_number(OraKind::BinaryDouble, 0, 0));
        assert!(!is_bare_number(OraKind::Text, 0, -127));
    }

    #[test]
    fn a_day_to_second_interval_renders_every_field() {
        assert_eq!(interval_ds_iso(1, 2, 3, 4, 5_000), "P1DT2H3M4.000005S");
        assert_eq!(interval_ds_iso(0, 0, 0, 0, 999), "PT0S", "sub-µs truncated");
        assert_eq!(interval_ds_iso(0, 0, 0, 1, 0), "PT1S");
        assert_eq!(interval_ds_iso(0, 0, 1, 0, 0), "PT1M");
        assert_eq!(interval_ds_iso(0, 1, 0, 0, 0), "PT1H");
    }

    #[test]
    fn bytes_render_as_upper_hex() {
        assert_eq!(upper_hex(&[0x00, 0xab, 0xff]), "00ABFF");
        assert_eq!(upper_hex(&[]), "");
    }

    #[test]
    fn only_a_sub_microsecond_fraction_is_lossy() {
        assert!(sub_microsecond("timestamp", 9));
        assert!(sub_microsecond("interval_ds", 7));
        assert!(!sub_microsecond("timestamp", 6));
        assert!(!sub_microsecond("number", 9));
    }
}
