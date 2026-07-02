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
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt8Builder, UInt16Builder,
    UInt32Builder, UInt64Builder,
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

    /// Rough in-memory footprint of this cell — drives the sink's memory-budget
    /// rollover. Heap length for `Bytes`; the scalar width otherwise.
    pub(crate) fn estimated_bytes(&self) -> usize {
        match self {
            RivetValue::Null | RivetValue::Bool(_) => 1,
            RivetValue::Int(_)
            | RivetValue::UInt(_)
            | RivetValue::Float(_)
            | RivetValue::TimeMicros(_) => 8,
            RivetValue::DateTime(_) => 12,
            RivetValue::Bytes(b) => b.len(),
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

/// True when [`build_column`] can produce an array of *exactly* this Arrow type
/// from a `RivetValue`. Drives whether the sink keeps the source's resolved type
/// — carrying its logical-type metadata + extension through `build_arrow_field`,
/// so `json` / `uuid` / real int widths land identically to the batch export —
/// or coarsens the column to `Utf8`.
pub(crate) fn is_buildable(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            // Both naive (datetime/datetime2/PG timestamp) and tz-aware
            // (datetimeoffset/PG timestamptz) — the latter as the UTC instant + the
            // column's zone label, matching the batch export (full type parity).
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}

/// The storage type the sink actually writes for a resolved column: the source's
/// own Arrow type when [`build_column`] can produce it exactly, else `Utf8`
/// (stringified). Keeps the schema field and the built array in lockstep.
pub(crate) fn render_type(arrow_type: Option<&DataType>) -> DataType {
    match arrow_type {
        Some(dt) if is_buildable(dt) => dt.clone(),
        _ => DataType::Utf8,
    }
}

/// Build one Arrow column from typed cells (one per row; `None` ⇒ null). `dt` is
/// the [`render_type`] — i.e. exactly the array type the schema field declares.
pub(crate) fn build_column(dt: &DataType, cells: &[Option<&RivetValue>]) -> Result<ArrayRef> {
    use RivetValue as V;

    // Normalise the explicit NULL variant to a missing cell up front, for every
    // builder arm at once. Without this the text arms (`Utf8`/`LargeUtf8`),
    // which accept ANY value via `render_str`, rendered `RivetValue::Null` as
    // an EMPTY STRING — every text/enum/json/interval NULL silently became ""
    // (and "" is not even valid JSON for a json column).
    let normalized: Vec<Option<&RivetValue>> = cells
        .iter()
        .map(|c| match c {
            Some(V::Null) => None,
            other => *other,
        })
        .collect();
    let cells: &[Option<&RivetValue>] = &normalized;

    // Integers: the binlog/driver value is always the widest signed/unsigned, so
    // narrow to the column's declared width — `try_from` nulls on the (impossible
    // for a correctly-typed column) overflow rather than silently wrapping.
    macro_rules! int_col {
        ($builder:ty, $ty:ty) => {{
            let mut b = <$builder>::with_capacity(cells.len());
            for c in cells {
                let v = match c {
                    Some(V::Int(i)) => <$ty>::try_from(*i).ok(),
                    Some(V::UInt(u)) => <$ty>::try_from(*u).ok(),
                    Some(V::Bool(x)) => Some(*x as $ty),
                    _ => None,
                };
                match v {
                    Some(v) => b.append_value(v),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }};
    }

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
        DataType::Int8 => int_col!(Int8Builder, i8),
        DataType::Int16 => int_col!(Int16Builder, i16),
        DataType::Int32 => int_col!(Int32Builder, i32),
        DataType::Int64 => int_col!(Int64Builder, i64),
        DataType::UInt8 => int_col!(UInt8Builder, u8),
        DataType::UInt16 => int_col!(UInt16Builder, u16),
        DataType::UInt32 => int_col!(UInt32Builder, u32),
        DataType::UInt64 => int_col!(UInt64Builder, u64),
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::Float(f)) => b.append_value(*f as f32),
                    Some(V::Int(i)) => b.append_value(*i as f32),
                    Some(V::UInt(u)) => b.append_value(*u as f32),
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
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            // The `DateTime` is the UTC instant; a tz-aware column carries it with its
            // zone label so it lands identically to the batch export (parity), never a
            // naive cast that drops the zone.
            let mut b = TimestampMicrosecondBuilder::with_capacity(cells.len());
            for c in cells {
                match c {
                    Some(V::DateTime(dt)) => b.append_value(dt.and_utc().timestamp_micros()),
                    _ => b.append_null(),
                }
            }
            let arr = b.finish();
            match tz {
                Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                None => Arc::new(arr),
            }
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
        DataType::LargeBinary => {
            let mut b = LargeBinaryBuilder::with_capacity(cells.len(), 0);
            for c in cells {
                match c {
                    Some(V::Bytes(by)) => b.append_value(by),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::FixedSizeBinary(n) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(cells.len(), *n);
            for c in cells {
                match c {
                    // Only a value of exactly the declared width is valid (e.g. a
                    // 16-byte UUID); anything else degrades to null rather than
                    // failing the whole batch.
                    Some(V::Bytes(by)) if by.len() == *n as usize => {
                        b.append_value(by).map_err(|e| anyhow::anyhow!(e))?
                    }
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::LargeUtf8 => {
            let mut b = LargeStringBuilder::with_capacity(cells.len(), 0);
            for c in cells {
                match c {
                    Some(v) => b.append_value(render_str(v)),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        // Utf8 + the catch-all: render anything to a string. `json` / `enum` ride
        // here (physically `Utf8`); their logical-type marker lives on the field.
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

/// MySQL binlog cell quirks, keyed by the column's NATIVE type (the binlog
/// row image is type-blind at decode time): each variant is what the wire
/// value actually is, and `apply` converts it to what the typed column needs.
/// Found by the all-types matrix audit — every one of these was a silent
/// per-column loss (NULLed by a strict builder) or corruption (enum index as
/// text) that count/sum verification could not see.
#[derive(Debug)]
pub(crate) enum MysqlCellFix {
    /// TIMESTAMP arrives as `"epoch[.micros]"` TEXT — parse to the UTC instant.
    TimestampEpoch,
    /// BIT(1) arrives as one raw byte — any set bit ⇒ true.
    BitBool,
    /// BIT(n>1) arrives as big-endian raw bytes — widen to u64.
    BitUint,
    /// YEAR arrives as its text rendering ("2024").
    YearText,
    /// ENUM arrives as its 1-based INDEX — map to the label (from the native
    /// type's `enum('a','b',…)` declaration); index 0 is MySQL's invalid-value
    /// sentinel → empty string, matching the server's own rendering.
    EnumLabels(Vec<String>),
    /// BINARY(n): the driver right-trims trailing NULs — pad back to width n
    /// (the batch export carries the full padded value).
    BinaryPad(usize),
}

/// The fix (if any) for a column, from the engine label + native type.
pub(crate) fn mysql_cell_fix(engine: &str, native: &str) -> Option<MysqlCellFix> {
    if engine != "mysql" {
        return None;
    }
    let n = native.to_ascii_lowercase();
    if n.starts_with("timestamp") {
        return Some(MysqlCellFix::TimestampEpoch);
    }
    if n == "bit(1)" {
        return Some(MysqlCellFix::BitBool);
    }
    // "bit" (no width) is what the wire metadata reports before the
    // information_schema enrichment — the value conversion needs no width.
    if n.starts_with("bit(") || n == "bit" {
        return Some(MysqlCellFix::BitUint);
    }
    if n == "year" || n.starts_with("year(") {
        return Some(MysqlCellFix::YearText);
    }
    if n.starts_with("enum(") {
        return Some(MysqlCellFix::EnumLabels(parse_enum_labels(native)));
    }
    if let Some(width) = n
        .strip_prefix("binary(")
        .and_then(|r| r.strip_suffix(')'))
        .and_then(|w| w.parse::<usize>().ok())
    {
        return Some(MysqlCellFix::BinaryPad(width));
    }
    None
}

/// Labels from `enum('a','b','it''s')`, in declaration (index) order.
fn parse_enum_labels(native: &str) -> Vec<String> {
    let Some(start) = native.find('(') else {
        return Vec::new();
    };
    let inner = &native[start + 1..native.len().saturating_sub(1)];
    let mut labels = Vec::new();
    let b = inner.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\'' {
            i += 1;
            continue;
        }
        let mut label = String::new();
        i += 1;
        while i < b.len() {
            if b[i] == b'\'' {
                if b.get(i + 1) == Some(&b'\'') {
                    label.push('\'');
                    i += 2;
                } else {
                    i += 1;
                    break;
                }
            } else {
                label.push(b[i] as char);
                i += 1;
            }
        }
        labels.push(label);
    }
    labels
}

impl MysqlCellFix {
    pub(crate) fn apply(&self, v: &RivetValue) -> RivetValue {
        use RivetValue as V;
        match (self, v) {
            (_, V::Null) => V::Null,
            (MysqlCellFix::TimestampEpoch, V::Bytes(b)) => std::str::from_utf8(b)
                .ok()
                .and_then(|s| {
                    let (sec, frac) = match s.split_once('.') {
                        Some((s, f)) => (s, f),
                        None => (s, ""),
                    };
                    let secs: i64 = sec.parse().ok()?;
                    let micros: u32 = if frac.is_empty() {
                        0
                    } else {
                        format!("{frac:0<6}").get(..6)?.parse().ok()?
                    };
                    chrono::DateTime::from_timestamp(secs, micros * 1_000).map(|dt| dt.naive_utc())
                })
                .map_or(V::Null, V::DateTime),
            (MysqlCellFix::BitBool, V::Bytes(b)) => V::Bool(b.iter().any(|x| *x != 0)),
            (MysqlCellFix::BitBool, V::Int(i)) => V::Bool(*i != 0),
            (MysqlCellFix::BitBool, V::UInt(u)) => V::Bool(*u != 0),
            (MysqlCellFix::BitUint, V::Bytes(b)) if b.len() <= 8 => {
                V::UInt(b.iter().fold(0u64, |acc, x| (acc << 8) | *x as u64))
            }
            (MysqlCellFix::YearText, V::Bytes(b)) => std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .map_or(V::Null, V::Int),
            (MysqlCellFix::EnumLabels(labels), V::Int(i)) => enum_label(labels, *i),
            (MysqlCellFix::EnumLabels(labels), V::UInt(u)) => enum_label(labels, *u as i64),
            (MysqlCellFix::BinaryPad(w), V::Bytes(b)) if b.len() < *w => {
                let mut p = b.clone();
                p.resize(*w, 0);
                V::Bytes(p)
            }
            (_, other) => other.clone(),
        }
    }
}

fn enum_label(labels: &[String], idx: i64) -> RivetValue {
    if idx <= 0 {
        return RivetValue::Bytes(Vec::new()); // MySQL's invalid-value sentinel ''
    }
    labels
        .get(idx as usize - 1)
        .map(|l| RivetValue::Bytes(l.clone().into_bytes()))
        .unwrap_or(RivetValue::Null)
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

    // RED test for the finding (all-types matrix audit): a NULL cell of a
    // text-shaped column arrived as `Some(RivetValue::Null)` and the Utf8
    // builder rendered it via `render_str` — an EMPTY STRING, not a null.
    // Every text/enum/json/interval NULL silently became "" (and "" is not
    // even valid JSON for a json column). A NULL must build as a null in
    // every arm, for every engine.
    #[test]
    fn explicit_null_value_builds_as_null_not_empty_string() {
        use arrow::array::Array;
        let cells: Vec<Option<&RivetValue>> = vec![Some(&RivetValue::Null), None];
        for dt in [DataType::Utf8, DataType::LargeUtf8] {
            let arr = build_column(&dt, &cells).unwrap();
            assert!(
                arr.is_null(0),
                "{dt:?}: Some(Null) must append a NULL, not an empty string"
            );
            assert!(arr.is_null(1));
        }
    }

    // All-types matrix audit findings: what the MySQL binlog ACTUALLY delivers
    // per native type (probed live via the NDJSON path), and the conversion
    // each typed column needs. Every case below was a silent per-column loss
    // (strict builder → NULL) or a corruption (enum index rendered as text).
    #[test]
    fn mysql_cell_fixes_convert_the_wire_shapes_the_binlog_delivers() {
        use RivetValue as V;
        let fix = |native: &str| mysql_cell_fix("mysql", native).expect(native);

        // TIMESTAMP(6): "epoch.micros" text → the UTC instant.
        let ts = fix("timestamp(6)").apply(&V::Bytes(b"1893553445.678901".to_vec()));
        assert_eq!(
            ts,
            V::DateTime(
                chrono::DateTime::from_timestamp(1_893_553_445, 678_901_000)
                    .unwrap()
                    .naive_utc()
            )
        );

        // BIT(1): one raw byte → Bool; BIT(8): big-endian bytes → UInt.
        assert_eq!(fix("bit(1)").apply(&V::Bytes(vec![1])), V::Bool(true));
        assert_eq!(fix("bit(8)").apply(&V::Bytes(vec![0xAA])), V::UInt(170));

        // YEAR: text rendering → Int.
        assert_eq!(fix("year").apply(&V::Bytes(b"2030".to_vec())), V::Int(2030));

        // ENUM: 1-based index → label; 0 → '' (MySQL's invalid sentinel).
        let e = fix("enum('a','b','c')");
        assert_eq!(e.apply(&V::Int(2)), V::Bytes(b"b".to_vec()));
        assert_eq!(e.apply(&V::Int(0)), V::Bytes(Vec::new()));

        // BINARY(4): trailing NULs trimmed by the driver → pad back to width.
        assert_eq!(
            fix("binary(4)").apply(&V::Bytes(Vec::new())),
            V::Bytes(vec![0, 0, 0, 0])
        );

        // NULL always stays NULL; other engines get no fix at all.
        assert_eq!(fix("year").apply(&V::Null), V::Null);
        assert!(mysql_cell_fix("postgres", "bit(1)").is_none());
        // varbinary is NOT padded (only fixed-width binary is).
        assert!(mysql_cell_fix("mysql", "varbinary(4)").is_none());
    }

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

    #[test]
    fn build_column_narrows_int_to_declared_width() {
        use arrow::array::{Array, Int32Array};
        let (v7, vmax) = (RivetValue::Int(7), RivetValue::Int(i64::MAX));
        let cells = [Some(&v7), None, Some(&vmax)];
        let arr = build_column(&DataType::Int32, &cells).unwrap();
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.value(0), 7);
        assert!(a.is_null(1)); // null stays null
        assert!(a.is_null(2)); // overflow → null, never a silent wrap
    }

    #[test]
    fn render_type_keeps_buildable_else_utf8() {
        // real width kept (matches the batch export); json/uuid ride as their
        // physical Utf8/FixedSizeBinary; a type the sink can't build → Utf8.
        assert_eq!(render_type(Some(&DataType::Int32)), DataType::Int32);
        assert_eq!(render_type(Some(&DataType::Utf8)), DataType::Utf8);
        assert_eq!(render_type(Some(&DataType::Date64)), DataType::Utf8);
        assert_eq!(render_type(None), DataType::Utf8);
    }

    /// The invariant that keeps schema and data in lockstep: every type
    /// `is_buildable` accepts, `build_column` must produce an array of *exactly*
    /// that type. Adding a type to one but not the other (a field with no matching
    /// array builder) would otherwise panic in `RecordBatch::try_new` at runtime,
    /// on the data — this fails in CI instead.
    #[test]
    fn is_buildable_iff_build_column_produces_that_type() {
        use arrow::array::Array;
        let buildable = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Decimal128(10, 2),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(16),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ];
        for dt in &buildable {
            assert!(is_buildable(dt), "is_buildable must accept {dt:?}");
            let arr = build_column(dt, &[None]).unwrap();
            assert_eq!(
                arr.data_type(),
                dt,
                "build_column({dt:?}) produced a mismatched type"
            );
        }
        // A type the sink can't build is rejected and coarsened to Utf8 — never a
        // field type with no matching builder.
        for dt in [
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            assert!(!is_buildable(&dt), "is_buildable must reject {dt:?}");
            assert_eq!(render_type(Some(&dt)), DataType::Utf8);
        }
    }
}
