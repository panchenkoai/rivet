//! `RivetValue` — a typed, owned cell value for the CDC path, replacing the lossy
//! `serde_json::Value` intermediate.
//!
//! The point is **structural** typing: temporals are extracted from the driver
//! value's own components (`mysql::Value::Date(y, m, d, …)`), never re-parsed from
//! a rendered string — so the naive-timestamp hazard CLAUDE.md forbids never
//! arises. Decimals are carried as their exact source bytes and converted to
//! `Decimal128` losslessly at build time.
//!
//! This is the shared CDC value vocabulary. Converging the batch `arrow_convert`
//! onto the same type (so there is literally one value→Arrow mapping) is a
//! follow-up that must be **benchmark-gated** — the batch path is the hot path.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
    Int64Builder, StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value as Json;

use crate::error::Result;

/// Days from the Unix epoch (1970-01-01) for `Date32`.
fn epoch_days(d: NaiveDate) -> i32 {
    (d - NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch")).num_days() as i32
}

/// A typed, owned CDC cell value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RivetValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    /// Naive date-time, extracted structurally (the source session is UTC in
    /// rivet's setups; see the Snowflake/MySQL session-TZ notes).
    DateTime(NaiveDateTime),
    /// Microseconds since midnight (`TIME`).
    TimeMicros(i64),
    /// Raw source bytes — `Utf8` text, `Binary`, or a `Decimal` string, decided by
    /// the target Arrow type at build time.
    Bytes(Vec<u8>),
}

impl RivetValue {
    /// Map a MySQL driver value to `RivetValue` — structurally, no string reparse.
    pub(crate) fn from_mysql(v: &mysql::Value) -> Self {
        use mysql::Value;
        match v {
            Value::NULL => RivetValue::Null,
            Value::Int(i) => RivetValue::Int(*i),
            Value::UInt(u) => RivetValue::UInt(*u),
            Value::Float(f) => RivetValue::Float(*f as f64),
            Value::Double(d) => RivetValue::Float(*d),
            Value::Date(y, mo, d, h, mi, s, us) => {
                NaiveDate::from_ymd_opt(*y as i32, *mo as u32, *d as u32)
                    .and_then(|date| date.and_hms_micro_opt(*h as u32, *mi as u32, *s as u32, *us))
                    .map_or(RivetValue::Null, RivetValue::DateTime)
            } // zero-date → null
            Value::Time(neg, days, h, mi, s, us) => {
                let micros =
                    ((*days as i64 * 86_400 + *h as i64 * 3_600 + *mi as i64 * 60 + *s as i64)
                        * 1_000_000)
                        + *us as i64;
                RivetValue::TimeMicros(if *neg { -micros } else { micros })
            }
            Value::Bytes(b) => RivetValue::Bytes(b.clone()),
        }
    }

    /// Render for NDJSON output. Lossy-by-design (JSON has no decimal/timestamp
    /// type) — the typed Arrow path is the lossless one.
    pub(crate) fn to_json(&self) -> Json {
        match self {
            RivetValue::Null => Json::Null,
            RivetValue::Bool(b) => (*b).into(),
            RivetValue::Int(i) => (*i).into(),
            RivetValue::UInt(u) => (*u).into(),
            RivetValue::Float(f) => Json::from(*f),
            RivetValue::DateTime(dt) => Json::String(dt.to_string()),
            RivetValue::TimeMicros(us) => Json::from(*us),
            RivetValue::Bytes(b) => Json::String(String::from_utf8_lossy(b).into_owned()),
        }
    }
}

/// The Arrow type this sink can build for a resolved column type — passthrough for
/// the types `build_column` handles, `Utf8` for the rest (stringified). Keeps the
/// schema and the built arrays consistent.
pub(crate) fn sink_type(dt: &DataType) -> DataType {
    match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Binary
        | DataType::LargeBinary => normalize(dt),
        DataType::Timestamp(_, _) => DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Time64(_) | DataType::Time32(_) => DataType::Time64(TimeUnit::Microsecond),
        DataType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
        _ => DataType::Utf8,
    }
}

fn normalize(dt: &DataType) -> DataType {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => DataType::Int64,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            DataType::UInt64
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::Binary | DataType::LargeBinary => DataType::Binary,
        other => other.clone(),
    }
}

/// Build one Arrow column from typed cells (one per row; `None` ⇒ null). `dt` is
/// the already-[`sink_type`]-normalized target type.
pub(crate) fn build_column(dt: &DataType, cells: &[Option<&RivetValue>]) -> Result<ArrayRef> {
    use RivetValue as V;
    Ok(match dt {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::Bool(x)) => b.append_value(*x),
                    Some(V::Int(i)) => b.append_value(*i != 0),
                    Some(V::UInt(u)) => b.append_value(*u != 0),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::Int(i)) => b.append_value(*i),
                    Some(V::UInt(u)) if *u <= i64::MAX as u64 => b.append_value(*u as i64),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::UInt64 => {
            let mut b = UInt64Builder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::UInt(u)) => b.append_value(*u),
                    Some(V::Int(i)) if *i >= 0 => b.append_value(*i as u64),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::Float(f)) => b.append_value(*f),
                    Some(V::Int(i)) => b.append_value(*i as f64),
                    Some(V::UInt(u)) => b.append_value(*u as f64),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::DateTime(dt)) => b.append_value(epoch_days(dt.date())),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::DateTime(dt)) => b.append_value(dt.and_utc().timestamp_micros()),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::TimeMicros(us)) => b.append_value(*us),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(p, s) => {
            let mut b = Decimal128Builder::with_capacity(cells.len())
                .with_data_type(DataType::Decimal128(*p, *s));
            for c in cells {
                match c.and_then(|v| decimal_to_i128(v, *s)) {
                    Some(i) => b.append_value(i),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(cells.len(), 0);
            for c in cells {
                match c {
                    Some(V::Bytes(by)) => b.append_value(by),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        // Utf8 fallback: render anything to a string.
        _ => {
            let mut b = StringBuilder::with_capacity(cells.len(), 0);
            for c in cells {
                match c {
                    Some(v) => b.append_value(render_str(v)),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    })
}

/// Parse a decimal carried as source bytes (e.g. `"150.00"`) into the scaled
/// `i128` a `Decimal128(_, s)` column stores — lossless, no float.
fn decimal_to_i128(v: &RivetValue, scale: i8) -> Option<i128> {
    let s = match v {
        RivetValue::Bytes(b) => std::str::from_utf8(b).ok()?.trim().to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        _ => return None,
    };
    let (neg, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.as_str()),
    };
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    let scale = scale.max(0) as usize;
    let mut digits = String::with_capacity(int_part.len() + scale);
    digits.push_str(int_part);
    let frac: String = frac_part.chars().take(scale).collect();
    digits.push_str(&frac);
    for _ in frac.len()..scale {
        digits.push('0');
    }
    let mag: i128 = digits.parse().ok()?;
    Some(if neg { -mag } else { mag })
}

fn render_str(v: &RivetValue) -> String {
    match v {
        RivetValue::Null => String::new(),
        RivetValue::Bool(b) => b.to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        RivetValue::Float(f) => f.to_string(),
        RivetValue::DateTime(dt) => dt.to_string(),
        RivetValue::TimeMicros(us) => us.to_string(),
        RivetValue::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_parse_is_lossless() {
        let v = RivetValue::Bytes(b"150.05".to_vec());
        assert_eq!(decimal_to_i128(&v, 2), Some(15005));
        assert_eq!(
            decimal_to_i128(&RivetValue::Bytes(b"-7.5".to_vec()), 3),
            Some(-7500)
        );
        assert_eq!(
            decimal_to_i128(&RivetValue::Bytes(b"42".to_vec()), 0),
            Some(42)
        );
    }

    #[test]
    fn temporal_is_structural_not_string() {
        let v = RivetValue::from_mysql(&mysql::Value::Date(2026, 6, 23, 11, 58, 1, 500_000));
        let expected = NaiveDate::from_ymd_opt(2026, 6, 23)
            .unwrap()
            .and_hms_micro_opt(11, 58, 1, 500_000)
            .unwrap();
        assert_eq!(v, RivetValue::DateTime(expected));
        // zero-date degrades to null, never a bogus epoch.
        assert_eq!(
            RivetValue::from_mysql(&mysql::Value::Date(0, 0, 0, 0, 0, 0, 0)),
            RivetValue::Null
        );
    }
}
