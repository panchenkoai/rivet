//! `RivetValue` — a typed, owned cell value for the CDC path, replacing the lossy
//! `serde_json::Value` intermediate.
//!
//! The point is **structural** typing: temporals are extracted from the driver
//! value's own components (`mysql::Value::Date(y, m, d, …)`), never re-parsed from
//! a rendered string — so the naive-timestamp hazard the process rules forbids never
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

use crate::source::value_checksum::ListElem;

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
    /// A one-dimensional array (PG `text[]`/`integer[]`/…) — elements are
    /// scalar `RivetValue`s (inner NULLs as [`RivetValue::Null`]). Built into a
    /// real Arrow `List` column, matching the batch export.
    Array(Vec<RivetValue>),
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
    /// DECODED payload size — the value's own bytes, no allocator overhead.
    /// See `ChangeEvent::payload_bytes` for why this is not `estimated_bytes`.
    pub(crate) fn payload_bytes(&self) -> usize {
        match self {
            RivetValue::Null | RivetValue::Bool(_) => 1,
            RivetValue::Int(_)
            | RivetValue::UInt(_)
            | RivetValue::Float(_)
            | RivetValue::TimeMicros(_) => 8,
            RivetValue::DateTime(_) => 12,
            RivetValue::Bytes(b) => b.len(),
            RivetValue::Array(v) => v.iter().map(RivetValue::payload_bytes).sum::<usize>(),
        }
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        match self {
            RivetValue::Null | RivetValue::Bool(_) => 1,
            RivetValue::Int(_)
            | RivetValue::UInt(_)
            | RivetValue::Float(_)
            | RivetValue::TimeMicros(_) => 8,
            RivetValue::DateTime(_) => 12,
            // CAPACITY, not len — the model's contract is RESIDENT cost, and the
            // one producer whose Bytes carry real slack is Mongo: its document
            // cell comes through `serde_json::to_string`, whose doubling growth
            // leaves capacity/len in (1, 2]. Charging len under-counted a
            // large-document stream up to 2x — outside the calibration contract
            // that already caught a 12.7x under and a 1.8x over. A no-op for the
            // exact-capacity engines (PG's to_vec, MySQL's clone).
            RivetValue::Bytes(b) => b.capacity(),
            // The Vec's SLOTS plus the elements — the same accounting
            // `ChangeEvent::estimated_bytes` gives the top-level image. A flat `+ 16`
            // charged a 1000-element `integer[]` 8 KB where its slots alone are 32 KB
            // (`size_of::<RivetValue>()` is 32), so an array-heavy PostgreSQL table
            // under-counted ~4x — and up to ~32x on an all-NULL array, where the
            // elements cost 1 byte each and the slots cost everything.
            RivetValue::Array(v) => {
                v.capacity() * std::mem::size_of::<RivetValue>()
                    + v.iter().map(RivetValue::estimated_bytes).sum::<usize>()
            }
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
            RivetValue::Bytes(b) => Json::String(bytes_to_recoverable_string(b)),
            RivetValue::Array(v) => Json::Array(v.iter().map(RivetValue::to_json).collect()),
        }
    }
}

/// True when [`build_column`] can produce an array of *exactly* this Arrow type
/// from a `RivetValue`. Drives whether the sink keeps the source's resolved type
/// — carrying its logical-type metadata + extension through `build_arrow_field`,
/// so `json` / `uuid` / real int widths land identically to the batch export —
/// or coarsens the column to `Utf8`.
pub(crate) fn is_buildable(dt: &DataType) -> bool {
    if let DataType::List(inner) = dt {
        // One-dimensional arrays with the element types the batch list builder
        // produces — full type parity for PG arrays.
        return matches!(
            inner.data_type(),
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
        );
    }
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
            // NUMERIC precision > 38 (PG numeric / MySQL decimal up to 65).
            | DataType::Decimal256(_, _)
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

/// Canonical bytes for a `FixedSizeBinary(n)` cell, or `None` when the value
/// genuinely cannot fill the width.
///
/// A width-`n` value passes through. At `n == 16` a value that is NOT 16 bytes
/// gets the batch reader's second chance: the canonical 36-char text form is
/// parsed to its 16 raw bytes (`src/source/mysql/arrow_convert.rs` does exactly
/// this). Without it, the only route to UUID semantics on MySQL — the documented
/// `columns: { uid: uuid }` override — nulled 100% of a `CHAR(36)`/`VARCHAR(36)`
/// column on CDC while the batch export of the same table was correct, because
/// the binlog delivers the cell as the 36-byte text.
///
/// Both the builder and `cells_checksum` MUST go through here. They previously
/// shared the bare `len() == n` test, which is why the two-ended value check was
/// blind to the loss: side A skipped the 36-byte cell (contributing 0) and side B
/// hashed a null (also 0), so the folds agreed and the mismatch bail never fired.
/// Fixing one side alone would invert that into a false failure on correct data.
fn fixed_binary_bytes(by: &[u8], n: usize) -> Option<Vec<u8>> {
    if by.len() == n {
        return Some(by.to_vec());
    }
    if n == 16
        && let Ok(s) = std::str::from_utf8(by)
        && let Ok(u) = uuid::Uuid::parse_str(s.trim())
    {
        return Some(u.as_bytes().to_vec());
    }
    None
}

/// A non-NULL cell its planned column type cannot hold: the row, the value, and why.
///
/// The builder knows no column name; the sink names the column once via [`CellRefusal::into_error`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CellRefusal {
    pub row: usize,
    pub value: RivetValue,
    pub reason: String,
    /// The engine's own code when the engine worded the refusal (its reason then carries the remedy).
    pub code: Option<crate::error::Code>,
}

/// Why an engine cell fix cannot read a wire value, optionally with the engine's own code.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FixRefusal {
    pub reason: String,
    pub code: Option<crate::error::Code>,
}

impl From<&str> for FixRefusal {
    /// A refusal worded by the fix, coded by the sink.
    fn from(reason: &str) -> Self {
        Self {
            reason: reason.into(),
            code: None,
        }
    }
}

impl From<String> for FixRefusal {
    /// A refusal worded by the fix, coded by the sink.
    fn from(reason: String) -> Self {
        Self { reason, code: None }
    }
}

/// Why a cell is refused when its variant has no reading as the column type at all.
const NO_READING: &str = "cannot be read as that type";

impl CellRefusal {
    /// A refusal of `value` at `row` for `reason`.
    fn new(row: usize, value: &RivetValue, reason: impl Into<String>) -> Self {
        Self {
            row,
            value: value.clone(),
            reason: reason.into(),
            code: None,
        }
    }

    /// The coded run error naming `column` of type `dt`; `overridden` picks the override code and remedy.
    pub(crate) fn into_error(self, column: &str, dt: &DataType, overridden: bool) -> anyhow::Error {
        let full = render_str(&self.value);
        let shown: String = full.chars().take(64).collect();
        let more = if shown.len() < full.len() { "…" } else { "" };
        let (code, remedy) = if let Some(code) = self.code {
            (code, String::new())
        } else if overridden {
            (
                crate::error::codes::SOURCE_OVERRIDE_WIRE_MISMATCH,
                format!(
                    "correct or drop the `columns:` override for '{column}', then re-snapshot \
                     the table"
                ),
            )
        } else {
            (
                crate::error::codes::SOURCE_CDC_CELL_UNSUPPORTED,
                "leave the column out of the capture, then re-snapshot the table".to_string(),
            )
        };
        anyhow::Error::new(crate::error::CodedError::new(
            code,
            format!(
                "cdc: column '{column}' is {dt} but the captured value {shown:?}{more} (row {}) \
                 {}; rivet refuses rather than writing NULL. {remedy}",
                self.row, self.reason
            )
            .trim_end()
            .to_string(),
        ))
    }
}

/// The value a Boolean column stores for `v`.
fn bool_value(v: &RivetValue) -> Result<bool, String> {
    match v {
        RivetValue::Bool(b) => Ok(*b),
        RivetValue::Int(i) => Ok(*i != 0),
        RivetValue::UInt(u) => Ok(*u != 0),
        _ => Err(NO_READING.into()),
    }
}

/// The value an integer column of width `T` stores for `v`: exact integer text parses, overflow refuses.
fn int_value<T: TryFrom<i128>>(v: &RivetValue) -> Result<T, String> {
    let wide: i128 = match v {
        RivetValue::Bool(b) => i128::from(*b),
        RivetValue::Int(i) => i128::from(*i),
        RivetValue::UInt(u) => i128::from(*u),
        RivetValue::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(crate::types::decimal::decimal_text_to_int)
            .ok_or("is not an exact integer")?,
        _ => return Err(NO_READING.into()),
    };
    T::try_from(wide).map_err(|_| {
        "overflows the column's integer width (a BIT(64) with bit 63 set, or a BIGINT \
         UNSIGNED past i64::MAX); map the column to decimal(20,0) or a wider type"
            .into()
    })
}

/// The value a Float64 column stores for `v`; decimal text parses through the shared parser.
fn f64_value(v: &RivetValue) -> Result<f64, String> {
    match v {
        RivetValue::Float(f) => Ok(*f),
        RivetValue::Int(i) => Ok(*i as f64),
        RivetValue::UInt(u) => Ok(*u as f64),
        RivetValue::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(crate::types::decimal::decimal_text_to_float)
            .ok_or_else(|| "is not a number".into()),
        _ => Err(NO_READING.into()),
    }
}

/// The value a Float32 column stores for `v`, cast directly (never through f64) so rounding matches batch.
fn f32_value(v: &RivetValue) -> Result<f32, String> {
    match v {
        RivetValue::Float(f) => Ok(*f as f32),
        RivetValue::Int(i) => Ok(*i as f32),
        RivetValue::UInt(u) => Ok(*u as f32),
        RivetValue::Bytes(b) => std::str::from_utf8(b)
            .ok()
            .and_then(crate::types::decimal::decimal_text_to_float)
            .ok_or_else(|| "is not a number".into()),
        _ => Err(NO_READING.into()),
    }
}

/// The Date32 a DateTime at midnight stores; a time of day is refused, never dropped.
fn date_value(v: &RivetValue) -> Result<i32, String> {
    match v {
        RivetValue::DateTime(d) if d.time() == chrono::NaiveTime::MIN => Ok(epoch_days(d.date())),
        RivetValue::DateTime(_) => Err("has a time of day, which a `date` override would drop; \
                                        declare it `timestamp`"
            .into()),
        _ => Err(NO_READING.into()),
    }
}

/// The Time64 microseconds a time of day stores; a duration outside one day is refused.
fn time_value(v: &RivetValue) -> Result<i64, String> {
    match v {
        RivetValue::TimeMicros(us) if crate::types::is_time_of_day(*us) => Ok(*us),
        RivetValue::TimeMicros(_) => {
            Err("is outside 00:00..24:00, which a Parquet TIME cannot hold".into())
        }
        _ => Err(NO_READING.into()),
    }
}

/// Why a decimal cell is refused.
const NOT_A_DECIMAL: &str = "is not representable in this decimal column (NaN/Infinity, more \
                             fraction digits than the column's scale, or a MONEY value past the \
                             f64-exact 2^53 range that tiberius already rounded)";

/// Build one Arrow column of exactly `dt` from typed cells (one per row; `None` ⇒ null).
///
/// `dt` must satisfy [`is_buildable`]; the sink asserts that when it builds the schema.
#[allow(clippy::redundant_closure_call)]
pub(crate) fn build_column(
    dt: &DataType,
    cells: &[Option<&RivetValue>],
) -> Result<ArrayRef, CellRefusal> {
    use RivetValue as V;

    // An explicit NULL is a missing cell in every arm, so no text arm renders it as "".
    let normalized: Vec<Option<&RivetValue>> = cells
        .iter()
        .map(|c| match c {
            Some(V::Null) => None,
            other => *other,
        })
        .collect();
    let cells: &[Option<&RivetValue>] = &normalized;

    macro_rules! typed_col {
        ($builder:expr, $value:expr) => {{
            let mut b = $builder;
            for (row, c) in cells.iter().enumerate() {
                match c {
                    None => b.append_null(),
                    Some(v) => match $value(*v) {
                        Ok(x) => b.append_value(x),
                        Err(reason) => return Err(CellRefusal::new(row, v, reason)),
                    },
                }
            }
            b.finish()
        }};
    }

    let n = cells.len();
    Ok(match dt {
        DataType::Boolean => Arc::new(typed_col!(BooleanBuilder::with_capacity(n), bool_value)),
        DataType::Int8 => Arc::new(typed_col!(Int8Builder::with_capacity(n), int_value::<i8>)),
        DataType::Int16 => Arc::new(typed_col!(Int16Builder::with_capacity(n), int_value::<i16>)),
        DataType::Int32 => Arc::new(typed_col!(Int32Builder::with_capacity(n), int_value::<i32>)),
        DataType::Int64 => Arc::new(typed_col!(Int64Builder::with_capacity(n), int_value::<i64>)),
        DataType::UInt8 => Arc::new(typed_col!(UInt8Builder::with_capacity(n), int_value::<u8>)),
        DataType::UInt16 => Arc::new(typed_col!(
            UInt16Builder::with_capacity(n),
            int_value::<u16>
        )),
        DataType::UInt32 => Arc::new(typed_col!(
            UInt32Builder::with_capacity(n),
            int_value::<u32>
        )),
        DataType::UInt64 => Arc::new(typed_col!(
            UInt64Builder::with_capacity(n),
            int_value::<u64>
        )),
        DataType::Float32 => Arc::new(typed_col!(Float32Builder::with_capacity(n), f32_value)),
        DataType::Float64 => Arc::new(typed_col!(Float64Builder::with_capacity(n), f64_value)),
        DataType::Date32 => Arc::new(typed_col!(Date32Builder::with_capacity(n), date_value)),
        // The `DateTime` is the UTC instant; a tz-aware column carries it with its zone label (batch parity).
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let arr = typed_col!(
                TimestampMicrosecondBuilder::with_capacity(n),
                |v: &RivetValue| match v {
                    V::DateTime(d) => Ok(d.and_utc().timestamp_micros()),
                    _ => Err(NO_READING.to_string()),
                }
            );
            match tz {
                Some(tz) => Arc::new(arr.with_timezone(tz.clone())),
                None => Arc::new(arr),
            }
        }
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(typed_col!(
            Time64MicrosecondBuilder::with_capacity(n),
            time_value
        )),
        DataType::Decimal128(p, s) => Arc::new(typed_col!(
            Decimal128Builder::with_capacity(n).with_data_type(DataType::Decimal128(*p, *s)),
            |v: &RivetValue| decimal_to_i128(v, *s).ok_or_else(|| NOT_A_DECIMAL.to_string())
        )),
        DataType::Decimal256(p, s) => Arc::new(typed_col!(
            arrow::array::Decimal256Builder::with_capacity(n)
                .with_data_type(DataType::Decimal256(*p, *s)),
            |v: &RivetValue| decimal_to_i256(v, *s).ok_or_else(|| NOT_A_DECIMAL.to_string())
        )),
        // One-dimensional arrays → a real List column, element field included (batch parity).
        DataType::List(field) => build_list_column(field, cells)?,
        DataType::Binary => Arc::new(typed_col!(
            BinaryBuilder::with_capacity(n, 0),
            |v: &RivetValue| match v {
                V::Bytes(by) => Ok(by.clone()),
                _ => Err(NO_READING.to_string()),
            }
        )),
        DataType::LargeBinary => Arc::new(typed_col!(
            LargeBinaryBuilder::with_capacity(n, 0),
            |v: &RivetValue| match v {
                V::Bytes(by) => Ok(by.clone()),
                _ => Err(NO_READING.to_string()),
            }
        )),
        DataType::FixedSizeBinary(w) => {
            // Width-`w` bytes, or (at w=16) the 36-char text UUID the MySQL binlog delivers.
            let mut b = FixedSizeBinaryBuilder::with_capacity(n, *w);
            for (row, c) in cells.iter().enumerate() {
                let bytes = match c {
                    None => {
                        b.append_null();
                        continue;
                    }
                    Some(v @ V::Bytes(by)) => fixed_binary_bytes(by, *w as usize)
                        .ok_or_else(|| CellRefusal::new(row, v, NO_READING))?,
                    Some(v) => return Err(CellRefusal::new(row, v, NO_READING)),
                };
                b.append_value(&bytes)
                    .expect("fixed_binary_bytes returns exactly the column width");
            }
            Arc::new(b.finish())
        }
        DataType::Utf8 => Arc::new(typed_col!(StringBuilder::with_capacity(n, 0), text_value)),
        DataType::LargeUtf8 => Arc::new(typed_col!(
            LargeStringBuilder::with_capacity(n, 0),
            text_value
        )),
        other => unreachable!("build_column({other}): the sink asserts is_buildable first"),
    })
}

/// The text a Utf8 column stores for `v`; an array has no text reading here.
fn text_value(v: &RivetValue) -> Result<String, String> {
    match v {
        RivetValue::Array(_) => Err("is an array, which a text column cannot hold".into()),
        other => Ok(render_str(other)),
    }
}

/// The CDC side-A fold: an independent per-column checksum of the typed cells.
///
/// The source-side twin of [`crate::source::value_checksum::arrow_batch_checksums`]
/// over the built array; it reads each cell through the SAME value function the
/// builder uses, so a mismatch means the builder changed a value after it was read.
pub(crate) fn cells_checksum(dt: &DataType, cells: &[Option<&RivetValue>]) -> u64 {
    use RivetValue as V;
    use xxhash_rust::xxh3::xxh3_64;

    use crate::source::value_checksum::{encode_list_cell, is_covered};

    if !is_covered(dt) {
        return 0;
    }
    let mut acc: u64 = 0;
    for c in cells {
        let c = match c {
            Some(V::Null) | None => continue,
            Some(v) => *v,
        };
        macro_rules! le {
            ($opt:expr) => {
                if let Ok(v) = $opt {
                    acc = acc.wrapping_add(xxh3_64(&v.to_le_bytes()));
                }
            };
        }
        match dt {
            DataType::Boolean => {
                if let Ok(b) = bool_value(c) {
                    acc = acc.wrapping_add(xxh3_64(&[b as u8]));
                }
            }
            DataType::Int16 => le!(int_value::<i16>(c)),
            DataType::Int32 => le!(int_value::<i32>(c)),
            DataType::Int64 => le!(int_value::<i64>(c)),
            DataType::UInt64 => le!(int_value::<u64>(c)),
            DataType::Float32 => le!(f32_value(c)),
            DataType::Float64 => le!(f64_value(c)),
            DataType::Date32 => le!(date_value(c)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                if let V::DateTime(dt) = c {
                    acc = acc.wrapping_add(xxh3_64(&dt.and_utc().timestamp_micros().to_le_bytes()));
                }
            }
            DataType::Time64(TimeUnit::Microsecond) => le!(time_value(c)),
            DataType::Decimal128(_, s) => le!(decimal_to_i128(c, *s).ok_or(())),
            DataType::Decimal256(_, s) => le!(decimal_to_i256(c, *s).ok_or(())),
            DataType::Utf8 => {
                if let Ok(s) = text_value(c) {
                    acc = acc.wrapping_add(xxh3_64(s.as_bytes()));
                }
            }
            DataType::Binary => {
                if let V::Bytes(by) = c {
                    acc = acc.wrapping_add(xxh3_64(by));
                }
            }
            DataType::FixedSizeBinary(n) => {
                if let V::Bytes(by) = c
                    && let Some(bytes) = fixed_binary_bytes(by, *n as usize)
                {
                    acc = acc.wrapping_add(xxh3_64(&bytes));
                }
            }
            DataType::List(f) => {
                if let V::Array(elems) = c {
                    let encoded: Option<Vec<ListElem>> = elems
                        .iter()
                        .map(|e| list_elem(f.data_type(), e).ok())
                        .collect();
                    if let Some(encoded) = encoded {
                        acc = acc.wrapping_add(xxh3_64(&encode_list_cell(&encoded)));
                    }
                }
            }
            _ => {}
        }
    }
    acc
}

/// One list element as the checksum canon, read exactly as the list builder reads it.
fn list_elem(elem: &DataType, e: &RivetValue) -> Result<ListElem, String> {
    use RivetValue as V;
    Ok(match (elem, e) {
        (_, V::Null) => ListElem::Null,
        (DataType::Boolean, V::Bool(b)) => ListElem::Bool(*b),
        (DataType::Int16, V::Int(_)) => ListElem::I16(int_value(e)?),
        (DataType::Int32, V::Int(_)) => ListElem::I32(int_value(e)?),
        (DataType::Int64, V::Int(i)) => ListElem::I64(*i),
        (DataType::Float32, V::Float(x)) => ListElem::F32(*x as f32),
        (DataType::Float64, V::Float(x)) => ListElem::F64(*x),
        (DataType::Float64, V::Int(i)) => ListElem::F64(*i as f64),
        (DataType::Utf8, other) => ListElem::Str(text_value(other)?.into_bytes()),
        _ => return Err(NO_READING.into()),
    })
}

/// Why a non-array cell cannot fill a one-dimensional list column.
const NOT_ONE_DIMENSIONAL: &str = "is a multi-dimensional or non-representable array, which a \
                                   one-dimensional list column cannot hold; cast the column to \
                                   text in the source (e.g. col::text). The batch export fails \
                                   identically";

/// Build a `List<element>` column from [`RivetValue::Array`] cells, keeping the list field (batch parity).
///
/// A NULL cell is a null list; an empty array is an empty list; inner NULLs survive;
/// an element the element type cannot hold refuses the whole cell.
fn build_list_column(
    field: &arrow::datatypes::FieldRef,
    cells: &[Option<&RivetValue>],
) -> Result<ArrayRef, CellRefusal> {
    use RivetValue as V;
    use arrow::array::ListBuilder;

    macro_rules! list_col {
        ($child:expr, $pat:pat => $val:expr) => {{
            let mut lb = ListBuilder::new($child).with_field(field.clone());
            for (row, c) in cells.iter().enumerate() {
                match c {
                    Some(v @ V::Array(elems)) => {
                        for e in elems {
                            match list_elem(field.data_type(), e) {
                                Ok(ListElem::Null) => lb.values().append_null(),
                                Ok($pat) => lb.values().append_value($val),
                                Ok(_) => {
                                    unreachable!("list_elem returns the element type's variant")
                                }
                                Err(reason) => {
                                    return Err(CellRefusal::new(
                                        row,
                                        v,
                                        format!("has an element {:?} that {reason}", render_str(e)),
                                    ));
                                }
                            }
                        }
                        lb.append(true);
                    }
                    None => lb.append(false),
                    Some(other) => return Err(CellRefusal::new(row, other, NOT_ONE_DIMENSIONAL)),
                }
            }
            Arc::new(lb.finish()) as ArrayRef
        }};
    }

    Ok(match field.data_type() {
        DataType::Boolean => list_col!(BooleanBuilder::new(), ListElem::Bool(x) => x),
        DataType::Int16 => list_col!(Int16Builder::new(), ListElem::I16(x) => x),
        DataType::Int32 => list_col!(Int32Builder::new(), ListElem::I32(x) => x),
        DataType::Int64 => list_col!(Int64Builder::new(), ListElem::I64(x) => x),
        DataType::Float32 => list_col!(Float32Builder::new(), ListElem::F32(x) => x),
        DataType::Float64 => list_col!(Float64Builder::new(), ListElem::F64(x) => x),
        DataType::Utf8 => list_col!(
            StringBuilder::new(),
            ListElem::Str(x) => String::from_utf8(x).expect("text_value is UTF-8")
        ),
        other => unreachable!("list of {other}: the sink asserts is_buildable first"),
    })
}

/// The scaled-integer magnitude past which an f64 can no longer hold a
/// fixed-point value exactly (2^53). A MONEY/SMALLMONEY value (delivered as f64
/// by tiberius) whose scaled magnitude reaches this was ALREADY rounded by the
/// driver, so materialising it as an "exact" Decimal would present rounding as
/// exact. The batch export (`mssql::arrow_convert::f64_to_scaled_i128`) fails
/// loud at exactly this bound; the CDC path must match it.
const F64_EXACT_SCALED_LIMIT: f64 = 9_007_199_254_740_992.0; // 2^53

/// A finite fixed-point f64 rendered at `scale` decimal places, or `None` when
/// its scaled magnitude is past the f64-exact range (already rounded → refuse,
/// so `build_column` fails loud instead of storing a falsely-exact decimal).
fn f64_fixed_point_str(f: f64, scale: i8) -> Option<String> {
    let prec = scale.max(0) as usize;
    if f.abs() * 10f64.powi(prec as i32) >= F64_EXACT_SCALED_LIMIT {
        return None;
    }
    Some(format!("{f:.prec$}"))
}

/// As [`decimal_to_i128`] but into `i256` for `Decimal256` columns.
fn decimal_to_i256(v: &RivetValue, scale: i8) -> Option<arrow::datatypes::i256> {
    let s = match v {
        RivetValue::Bytes(b) => std::str::from_utf8(b).ok()?.trim().to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        RivetValue::Float(f) if f.is_finite() => f64_fixed_point_str(*f, scale)?,
        _ => return None,
    };
    crate::types::decimal::decimal_str_to_scaled_i256(&s, scale)
}

/// Parse a decimal carried as source bytes (e.g. `"150.00"`) into the scaled
/// `i128` a `Decimal128(_, s)` column stores — lossless, no float.
fn decimal_to_i128(v: &RivetValue, scale: i8) -> Option<i128> {
    let s = match v {
        RivetValue::Bytes(b) => std::str::from_utf8(b).ok()?.trim().to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        // Fixed-point values some drivers deliver as floats (SQL Server MONEY
        // via tiberius): render at the column scale first, then the shared
        // digit-exact parse below. Past 2^53 the f64 was already rounded by the
        // driver — f64_fixed_point_str returns None so build_column fails loud,
        // exactly like the batch export, never a silently-rounded "exact" value.
        RivetValue::Float(f) if f.is_finite() => f64_fixed_point_str(*f, scale)?,
        _ => return None,
    };
    // ONE decimal-string canon for the whole codebase (types::decimal) — the
    // hand-rolled twin this replaced would drift (it rejected '+5.00' and an
    // empty integer part the shared parser accepts).
    crate::types::decimal::decimal_str_to_scaled_i128(&s, scale)
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
    /// Signed MEDIUMINT arrives WITHOUT 24-bit sign extension (the binlog
    /// stores 3 bytes; 0x800000 decodes as +8388608 instead of −8388608).
    MediumIntSign,
    /// ENUM arrives as its 1-based INDEX — map to the label (from the native
    /// type's `enum('a','b',…)` declaration); index 0 is MySQL's invalid-value
    /// sentinel → empty string, matching the server's own rendering.
    EnumLabels(Vec<String>),
    /// SET arrives as its BITMASK (little-endian raw bytes, one bit per member)
    /// — render the set members comma-joined in declaration order, exactly as
    /// the server's own text form ("x,z").
    SetLabels(Vec<String>),
    /// BINARY(n): the driver right-trims trailing NULs — pad back to width n
    /// (the batch export carries the full padded value).
    BinaryPad(usize),
    /// TIME is a duration up to 838:59:59; one outside a day is refused with the batch's MySQL wording.
    TimeOfDay,
}

/// The fix (if any) for a column, from the engine + native type.
pub(crate) fn mysql_cell_fix(
    engine: crate::source::cdc::CdcEngine,
    native: &str,
) -> Option<MysqlCellFix> {
    if engine != crate::source::cdc::CdcEngine::Mysql {
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
    // Signed only — `mediumint unsigned` needs no sign extension.
    if (n == "mediumint" || n.starts_with("mediumint(")) && !n.contains("unsigned") {
        return Some(MysqlCellFix::MediumIntSign);
    }
    if n == "time" || n.starts_with("time(") {
        return Some(MysqlCellFix::TimeOfDay);
    }
    if n.starts_with("enum(") {
        return Some(MysqlCellFix::EnumLabels(parse_enum_labels(native)));
    }
    if n.starts_with("set(") {
        return Some(MysqlCellFix::SetLabels(parse_enum_labels(native)));
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
/// TOTAL over arbitrary input (finding #39: the byte-sliced version panicked
/// on a trailing `(` and on multibyte boundaries), and CHAR-wise (#39b: the
/// `byte as char` loop mojibake'd every non-ASCII label — `ENUM('привет')`
/// would have written garbled labels into the capture, silently).
fn parse_enum_labels(native: &str) -> Vec<String> {
    let Some(start) = native.find('(') else {
        return Vec::new();
    };
    let end = native.rfind(')').unwrap_or(native.len());
    let Some(inner) = native.get(start + 1..end.max(start + 1)) else {
        return Vec::new();
    };
    let mut labels = Vec::new();
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut label = String::new();
        loop {
            match chars.next() {
                Some('\'') if chars.peek() == Some(&'\'') => {
                    chars.next();
                    label.push('\'');
                }
                Some('\'') | None => break,
                Some(other) => label.push(other),
            }
        }
        labels.push(label);
    }
    labels
}

impl MysqlCellFix {
    /// The value the typed column needs for wire value `v`, or why the wire value has no reading.
    pub(crate) fn apply(&self, v: &RivetValue) -> Result<RivetValue, FixRefusal> {
        use RivetValue as V;
        Ok(match (self, v) {
            (_, V::Null) => V::Null,
            (MysqlCellFix::TimestampEpoch, V::Bytes(b)) => match epoch_text(b) {
                // '0000-00-00 00:00:00' arrives as epoch 0, which no real TIMESTAMP holds: NULL, as batch.
                Some((0, 0)) => V::Null,
                Some((secs, micros)) => chrono::DateTime::from_timestamp(secs, micros * 1_000)
                    .map(|dt| V::DateTime(dt.naive_utc()))
                    .ok_or("is past the range of a timestamp")?,
                None => return Err("is not a binlog TIMESTAMP (epoch seconds text)".into()),
            },
            (MysqlCellFix::BitBool, V::Bytes(b)) => V::Bool(b.iter().any(|x| *x != 0)),
            (MysqlCellFix::BitBool, V::Int(i)) => V::Bool(*i != 0),
            (MysqlCellFix::BitBool, V::UInt(u)) => V::Bool(*u != 0),
            (MysqlCellFix::BitUint, V::Bytes(b)) if b.len() <= 8 => {
                V::UInt(b.iter().fold(0u64, |acc, x| (acc << 8) | *x as u64))
            }
            (MysqlCellFix::YearText, V::Bytes(b)) => {
                let y = std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or("is not a YEAR")?;
                V::Int(if y == 1900 { 0 } else { y })
            }
            (MysqlCellFix::MediumIntSign, V::Int(i)) if *i >= (1 << 23) => V::Int(*i - (1 << 24)),
            (MysqlCellFix::MediumIntSign, V::UInt(u)) => {
                let i = *u as i64;
                V::Int(if i >= (1 << 23) { i - (1 << 24) } else { i })
            }
            (MysqlCellFix::EnumLabels(labels), V::Int(i)) => enum_label(labels, *i)?,
            (MysqlCellFix::EnumLabels(labels), V::UInt(u)) => {
                enum_label(labels, i64::try_from(*u).unwrap_or(i64::MAX))?
            }
            (MysqlCellFix::SetLabels(labels), V::Bytes(b)) if b.len() <= 8 => {
                // Little-endian storage: byte 0 carries members 1..=8.
                let mask = b
                    .iter()
                    .enumerate()
                    .fold(0u64, |acc, (i, x)| acc | ((*x as u64) << (8 * i)));
                set_labels(labels, mask)
            }
            (MysqlCellFix::SetLabels(labels), V::Int(i)) => set_labels(labels, *i as u64),
            (MysqlCellFix::SetLabels(labels), V::UInt(u)) => set_labels(labels, *u),
            (MysqlCellFix::TimeOfDay, V::TimeMicros(us)) if !crate::types::is_time_of_day(*us) => {
                return Err(FixRefusal {
                    reason: crate::source::mysql::time_outside_day_refusal(*us),
                    code: Some(crate::error::codes::SOURCE_VALUE_UNREPRESENTABLE),
                });
            }
            (MysqlCellFix::BinaryPad(w), V::Bytes(b)) if b.len() < *w => {
                let mut p = b.clone();
                p.resize(*w, 0);
                V::Bytes(p)
            }
            (_, other) => other.clone(),
        })
    }
}

/// `"secs[.frac]"` binlog TIMESTAMP text as (seconds, microseconds).
fn epoch_text(b: &[u8]) -> Option<(i64, u32)> {
    let s = std::str::from_utf8(b).ok()?;
    let (sec, frac) = s.split_once('.').unwrap_or((s, ""));
    let micros = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}").get(..6)?.parse().ok()?
    };
    Some((sec.parse().ok()?, micros))
}

fn set_labels(labels: &[String], mask: u64) -> RivetValue {
    let joined = labels
        .iter()
        .enumerate()
        .filter(|(i, _)| mask & (1 << i) != 0)
        .map(|(_, l)| l.as_str())
        .collect::<Vec<_>>()
        .join(",");
    RivetValue::Bytes(joined.into_bytes())
}

/// The label of 1-based ENUM index `idx`; 0 is MySQL's invalid-value sentinel `''`, past the list is refused.
fn enum_label(labels: &[String], idx: i64) -> Result<RivetValue, String> {
    if idx <= 0 {
        return Ok(RivetValue::Bytes(Vec::new()));
    }
    labels
        .get(idx as usize - 1)
        .map(|l| RivetValue::Bytes(l.clone().into_bytes()))
        .ok_or_else(|| format!("is past the column's {} ENUM labels", labels.len()))
}

/// Render `Bytes` to a string LOSSLESSLY: verbatim when the bytes are valid UTF-8
/// (the common case — a text/JSON/decimal column, a Mongo id), else PG-style hex
/// (`\x…`), which the reader can decode back to the original bytes.
///
/// The alternative, `String::from_utf8_lossy`, replaces every non-UTF-8 byte with
/// U+FFFD — silent, UNRECOVERABLE corruption. That bit both the NDJSON path (fixed
/// earlier) and the typed Parquet/CSV sink via [`render_str`] (#1 bughunt: a MySQL
/// CDC `latin1` TEXT column's `é` byte 0xE9 became U+FFFD in every non-ASCII cell,
/// live-proven `caf‹0xE9›` → `caf‹EF BF BD›`). Hex is a RECOVERABLE interim — the
/// complete fix for a non-UTF-8 *text* column is charset-aware decoding (transcode
/// via the binlog table-map charset), which the MySQL CDC reader does not yet do.
fn bytes_to_recoverable_string(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => {
            use std::fmt::Write as _;
            let mut hex = String::with_capacity(2 + b.len() * 2);
            hex.push_str("\\x");
            for byte in b {
                let _ = write!(hex, "{byte:02x}");
            }
            hex
        }
    }
}

fn render_str(v: &RivetValue) -> String {
    match v {
        RivetValue::Null => String::new(),
        RivetValue::Bool(b) => b.to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        RivetValue::Float(f) => f.to_string(),
        RivetValue::DateTime(dt) => dt.to_string(),
        RivetValue::TimeMicros(us) => crate::types::time_beyond_day(*us),
        RivetValue::Bytes(b) => bytes_to_recoverable_string(b),
        RivetValue::Array(v) => {
            let inner: Vec<String> = v.iter().map(render_str).collect();
            format!("[{}]", inner.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `build_column` with the sink's refusal wording for a non-overridden column `col`.
    fn build(col: &str, dt: &DataType, cells: &[Option<&RivetValue>]) -> anyhow::Result<ArrayRef> {
        build_column(dt, cells).map_err(|r| r.into_error(col, dt, false))
    }

    #[test]
    fn to_json_bytes_are_lossless_utf8_verbatim_binary_hex() {
        // #dogfood MED: NDJSON `to_json` ran `from_utf8_lossy` on EVERY Bytes
        // value, so a genuinely-binary column (bytea, uuid/GUID bytes) had every
        // non-UTF-8 byte replaced with U+FFFD — silent, unrecoverable corruption.
        // Text-valued Bytes (string/JSON/decimal columns) stay verbatim.
        assert_eq!(
            RivetValue::Bytes(b"hello".to_vec()).to_json(),
            Json::String("hello".to_string())
        );
        // Non-UTF-8 binary → PG-style hex, losslessly recoverable (no U+FFFD).
        let raw = vec![0xDEu8, 0xAD, 0xBE, 0xEF, 0x00, 0xFF];
        let j = RivetValue::Bytes(raw).to_json();
        assert_eq!(j, Json::String("\\xdeadbeef00ff".to_string()));
        if let Json::String(s) = &j {
            assert!(
                !s.contains('\u{FFFD}'),
                "binary must not be lossy-replaced: {s}"
            );
        }
    }

    #[test]
    fn render_str_bytes_are_lossless_like_to_json() {
        // #1 bughunt: the U+FFFD fix was wired into to_json (NDJSON) only — the
        // TYPED Parquet/CSV sink's render_str still ran from_utf8_lossy, so a MySQL
        // CDC latin1 TEXT column's `é` (0xE9) became U+FFFD in the Parquet cell
        // (live-proven `caf‹E9›` → `caf‹EF BF BD›`). render_str must match to_json:
        // verbatim UTF-8, else recoverable hex, NEVER U+FFFD.
        assert_eq!(
            render_str(&RivetValue::Bytes(b"caf\xc3\xa9".to_vec())),
            "café"
        );
        // The exact live-repro bytes: latin1 "café" = caf + 0xE9. The 0xE9 makes
        // the WHOLE slice invalid UTF-8, so — matching to_json — it renders as
        // whole-value hex (`\x636166e9`), which is recoverable; never U+FFFD.
        let latin1 = vec![b'c', b'a', b'f', 0xE9];
        let s = render_str(&RivetValue::Bytes(latin1));
        assert_eq!(s, "\\x636166e9", "non-UTF-8 must render as recoverable hex");
        assert!(!s.contains('\u{FFFD}'), "must not be lossy-replaced: {s}");
    }

    // Finding #39/#39b regression pins: the enum-label parser must be total
    // (trailing '(' / multibyte boundaries panicked) and CHAR-correct —
    // non-ASCII labels were mojibake'd byte-by-byte, i.e. silent label
    // corruption for any unicode ENUM.
    #[test]
    fn enum_labels_unicode_and_escapes_parse_exactly() {
        assert_eq!(
            parse_enum_labels("enum('привет','мир')"),
            vec!["привет".to_string(), "мир".to_string()]
        );
        assert_eq!(
            parse_enum_labels("enum('it''s','a')"),
            vec!["it's".to_string(), "a".to_string()]
        );
        assert_eq!(parse_enum_labels("enum("), Vec::<String>::new());
        assert_eq!(parse_enum_labels("enum('ok'"), vec!["ok".to_string()]);
        assert_eq!(parse_enum_labels("garbage"), Vec::<String>::new());
    }

    // Negative family #2 at the MySQL BINARY level: every cell fix must be
    // TOTAL over arbitrary wire bytes and arbitrary native-type strings — a
    // corrupt binlog cell may carry anything, and the fix layer must degrade
    // (Null / passthrough), never panic and never bring the stream down.
    // (The PG text parsers got this net yesterday; this is the mysql floor.)
    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, ..Default::default()
        })]

        #[test]
        fn cell_fixes_are_total_over_arbitrary_wire_values(
            native in "[a-z0-9_() ',]{0,40}",
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
            ival in proptest::prelude::any::<i64>(),
            uval in proptest::prelude::any::<u64>(),
        ) {
            use crate::source::cdc::CdcEngine;
            if let Some(fix) = mysql_cell_fix(CdcEngine::Mysql, &native) {
                for v in [
                    RivetValue::Bytes(bytes.clone()),
                    RivetValue::Int(ival),
                    RivetValue::UInt(uval),
                    RivetValue::Float(f64::NAN),
                    RivetValue::Null,
                ] {
                    let _ = fix.apply(&v);
                }
            }
            // Label parsers over arbitrary native strings.
            let _ = parse_enum_labels(&native);
        }

        #[test]
        fn build_column_is_total_over_arbitrary_cells(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..48),
            ival in proptest::prelude::any::<i64>(),
        ) {
            use arrow::datatypes::{DataType, TimeUnit};
            let owned = [
                Some(RivetValue::Bytes(bytes.clone())),
                Some(RivetValue::Int(ival)),
                Some(RivetValue::Float(f64::INFINITY)),
                None,
            ];
            let cells: Vec<Option<&RivetValue>> = owned.iter().map(|c| c.as_ref()).collect();
            for dt in [
                DataType::Int32,
                DataType::UInt64,
                DataType::Float64,
                DataType::Utf8,
                DataType::Binary,
                DataType::FixedSizeBinary(16),
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Boolean,
            ] {
                // Result may be Ok or a LOUD Err (decimal refuses NaN-likes);
                // the property is: no panic, ever.
                let _ = build("c", &dt, &cells);
            }
        }
    }

    // The two-ended contract: the independent cell fold must equal the fold of
    // the BUILT array for EVERY covered type — including the hostile cells
    // (nulls, narrowing, arrays with inner nulls, wide decimals). If an arm of
    // cells_checksum ever drifts from build_column, this matrix catches it
    // offline before any live flush does.
    #[test]
    fn cells_checksum_matches_built_array_for_every_covered_type() {
        use arrow::datatypes::Field;

        use crate::source::value_checksum::array_checksum;
        use RivetValue as V;
        let list_utf8 = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let list_i32 = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let cases: Vec<(DataType, Vec<Option<RivetValue>>)> = vec![
            (
                DataType::Boolean,
                vec![Some(V::Bool(true)), Some(V::Int(0)), None, Some(V::Null)],
            ),
            (
                DataType::Int16,
                // In-range narrowing from the wide driver value (20000 fits i16);
                // an OVERFLOW is a loud error now, covered by the narrows test.
                vec![
                    Some(V::Int(-5)),
                    Some(V::Int(20_000)),
                    Some(V::UInt(7)),
                    None,
                ],
            ),
            (DataType::Int32, vec![Some(V::Int(-8_388_608)), None]),
            (
                DataType::Int64,
                // 9e9 fits i64 (narrowed from the u64 driver value); a u64 past
                // i64::MAX is a loud error (build_column_narrows_int test).
                vec![
                    Some(V::Int(i64::MIN)),
                    Some(V::UInt(9_000_000_000)),
                    Some(V::Bytes(b"-42.00".to_vec())),
                    None,
                ],
            ),
            (
                DataType::UInt64,
                vec![Some(V::UInt(u64::MAX)), Some(V::UInt(1)), None],
            ),
            (
                DataType::Float32,
                vec![
                    Some(V::Float(1.5)),
                    Some(V::Int(i64::MAX)),
                    Some(V::Bytes(b"0.1".to_vec())),
                    None,
                ],
            ),
            (
                DataType::Float64,
                vec![
                    Some(V::Float(f64::NAN)),
                    Some(V::UInt(3)),
                    Some(V::Bytes(b"12345.67".to_vec())),
                    None,
                ],
            ),
            (
                DataType::Date32,
                vec![
                    Some(V::DateTime(
                        chrono::NaiveDate::from_ymd_opt(2024, 3, 15)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap(),
                    )),
                    None,
                ],
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                vec![
                    Some(V::DateTime(
                        chrono::NaiveDate::from_ymd_opt(2035, 8, 7)
                            .unwrap()
                            .and_hms_micro_opt(9, 8, 7, 987_654)
                            .unwrap(),
                    )),
                    None,
                ],
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                vec![Some(V::TimeMicros(86_399_999_999)), None],
            ),
            (
                DataType::Decimal128(18, 2),
                vec![
                    Some(V::Bytes(b"999999999999.99".to_vec())),
                    Some(V::Int(-42)),
                    None,
                ],
            ),
            (
                DataType::Decimal256(50, 10),
                vec![
                    Some(V::Bytes(
                        b"1234567890123456789012345678901234567890.0123456789".to_vec(),
                    )),
                    None,
                ],
            ),
            (
                DataType::Utf8,
                vec![Some(V::Bytes("üñíçødé".as_bytes().to_vec())), None],
            ),
            (
                DataType::Binary,
                vec![Some(V::Bytes(vec![0x00, 0xff, 0x01])), None],
            ),
            (
                DataType::FixedSizeBinary(16),
                vec![Some(V::Bytes(vec![7u8; 16])), None],
            ),
            (
                list_utf8,
                vec![
                    Some(V::Array(vec![
                        V::Bytes(b"with,comma".to_vec()),
                        V::Null,
                        V::Bytes(b"".to_vec()),
                    ])),
                    Some(V::Array(vec![])),
                    None,
                ],
            ),
            (
                list_i32,
                vec![Some(V::Array(vec![V::Int(1), V::Null, V::Int(-3)])), None],
            ),
        ];
        for (dt, owned) in cases {
            let cells: Vec<Option<&RivetValue>> = owned.iter().map(|c| c.as_ref()).collect();
            let arr = build("c", &dt, &cells).unwrap();
            assert_eq!(
                cells_checksum(&dt, &cells),
                array_checksum(arr.as_ref()),
                "fold ≠ built array for {dt:?}"
            );
        }
    }

    /// A `uuid` override on a MySQL `CHAR(36)`/`VARCHAR(36)` column resolves to
    /// `FixedSizeBinary(16)`, but the binlog delivers the cell as the 36-BYTE
    /// canonical text. The arm used to accept only exactly-16-byte values, so
    /// 100% of the column became NULL on CDC while the batch export of the same
    /// table was correct (batch has always parsed the text form).
    ///
    /// Two assertions, and the second is the one with teeth. Agreement alone is
    /// worthless here: before the fix the two folds ALSO agreed — side A skipped
    /// the 36-byte cell and side B hashed a null, both contributing 0 — which is
    /// exactly why the sink's two-ended value check could not see the loss. So
    /// the test first pins the recovered VALUE against a hard-coded expected byte
    /// array (an oracle independent of this module), then pins that the folds
    /// still agree, which is what would break if only one side were taught.
    #[test]
    fn a_text_uuid_survives_cdc_under_a_fixed_size_binary_override() {
        use crate::source::value_checksum::array_checksum;
        use RivetValue as V;
        use arrow::array::{Array, FixedSizeBinaryArray};

        let dt = DataType::FixedSizeBinary(16);
        // Canonical text, as MySQL stores it in CHAR(36) and ships it on the wire.
        let text = V::Bytes(b"550e8400-e29b-41d4-a716-446655440000".to_vec());
        // Independently derived: the RFC-4122 hex of that string, by hand.
        let expected: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        // A value already in 16-byte form must keep passing through untouched.
        let raw = V::Bytes(expected.to_vec());
        let cells = [Some(&text), Some(&raw), None];
        let refs: Vec<Option<&RivetValue>> = cells.to_vec();

        let arr = build("c", &dt, &refs).unwrap();
        let fsb = arr
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinary(16) array");

        assert!(
            !fsb.is_null(0),
            "the 36-char text form must decode, not degrade to null — this is the \
             whole defect: every row of the column became NULL while counts passed"
        );
        assert_eq!(
            fsb.value(0),
            expected,
            "text form must decode to its raw bytes"
        );
        assert_eq!(fsb.value(1), expected, "raw 16-byte form must pass through");
        assert!(fsb.is_null(2), "a missing cell stays null");

        assert_eq!(
            cells_checksum(&dt, &refs),
            array_checksum(arr.as_ref()),
            "both ends must canonicalise identically — teaching only the builder \
             turns a CORRECT export into a checksum-mismatch failure at sink.rs"
        );
    }

    // Sensitivity: a corrupted cell must MOVE the fold, so the sink's compare
    // fires — not a constant that trivially agrees.
    #[test]
    fn cells_checksum_detects_a_changed_cell() {
        use RivetValue as V;
        let a = [Some(V::Int(1)), Some(V::Int(2))];
        let b = [Some(V::Int(1)), Some(V::Int(3))];
        let ra: Vec<Option<&RivetValue>> = a.iter().map(|c| c.as_ref()).collect();
        let rb: Vec<Option<&RivetValue>> = b.iter().map(|c| c.as_ref()).collect();
        assert_ne!(
            cells_checksum(&DataType::Int64, &ra),
            cells_checksum(&DataType::Int64, &rb)
        );
    }

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
            let arr = build("c", &dt, &cells).unwrap();
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
        let fix = |native: &str| {
            mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native)
        };

        // TIMESTAMP(6): "epoch.micros" text → the UTC instant.
        let ts = fix("timestamp(6)")
            .apply(&V::Bytes(b"1893553445.678901".to_vec()))
            .unwrap();
        assert_eq!(
            ts,
            V::DateTime(
                chrono::DateTime::from_timestamp(1_893_553_445, 678_901_000)
                    .unwrap()
                    .naive_utc()
            )
        );

        // TIMESTAMP zero-date: the binlog carries '0000-00-00 00:00:00' as epoch 0, which no real
        // TIMESTAMP can hold (the range starts at 1970-01-01 00:00:01 UTC) — NULL, as the batch path.
        assert_eq!(
            fix("timestamp").apply(&V::Bytes(b"0".to_vec())).unwrap(),
            V::Null
        );
        assert_eq!(
            fix("timestamp(6)")
                .apply(&V::Bytes(b"0.000000".to_vec()))
                .unwrap(),
            V::Null
        );
        assert_eq!(
            fix("timestamp").apply(&V::Bytes(b"1".to_vec())).unwrap(),
            V::DateTime(chrono::DateTime::from_timestamp(1, 0).unwrap().naive_utc()),
            "the first real TIMESTAMP second stays a value"
        );

        // YEAR: the binlog decoder adds 1900 to the stored byte, so YEAR 0000 arrives as "1900"
        // (never a legal YEAR) and must read back as 0, as the batch path does.
        assert_eq!(
            fix("year").apply(&V::Bytes(b"1900".to_vec())).unwrap(),
            V::Int(0)
        );
        assert_eq!(
            fix("year").apply(&V::Bytes(b"1901".to_vec())).unwrap(),
            V::Int(1901)
        );
        assert_eq!(
            fix("year").apply(&V::Bytes(b"2024".to_vec())).unwrap(),
            V::Int(2024)
        );

        // BIT(1): one raw byte → Bool; BIT(8): big-endian bytes → UInt.
        assert_eq!(
            fix("bit(1)").apply(&V::Bytes(vec![1])).unwrap(),
            V::Bool(true)
        );
        assert_eq!(
            fix("bit(8)").apply(&V::Bytes(vec![0xAA])).unwrap(),
            V::UInt(170)
        );

        // YEAR: text rendering → Int.
        assert_eq!(
            fix("year").apply(&V::Bytes(b"2030".to_vec())).unwrap(),
            V::Int(2030)
        );

        // ENUM: 1-based index → label; 0 → '' (MySQL's invalid sentinel).
        let e = fix("enum('a','b','c')");
        assert_eq!(e.apply(&V::Int(2)).unwrap(), V::Bytes(b"b".to_vec()));
        assert_eq!(e.apply(&V::Int(0)).unwrap(), V::Bytes(Vec::new()));

        // BINARY(4): trailing NULs trimmed by the driver → pad back to width.
        assert_eq!(
            fix("binary(4)").apply(&V::Bytes(Vec::new())).unwrap(),
            V::Bytes(vec![0, 0, 0, 0])
        );

        // MEDIUMINT: 24-bit sign extension (0x800000 → −8388608); positives and
        // the unsigned variant untouched.
        let mi = fix("mediumint");
        assert_eq!(mi.apply(&V::Int(8_388_608)).unwrap(), V::Int(-8_388_608));
        assert_eq!(mi.apply(&V::Int(8_388_607)).unwrap(), V::Int(8_388_607));
        assert!(
            mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, "mediumint unsigned").is_none()
        );

        // SET: bitmask (LE bytes) → comma-joined labels in declaration order,
        // the server's own rendering ('x,z' for bits 0+2 = 0x05).
        let st = fix("set('x','y','z')");
        assert_eq!(
            st.apply(&V::Bytes(vec![0x05])).unwrap(),
            V::Bytes(b"x,z".to_vec())
        );
        assert_eq!(st.apply(&V::UInt(0)).unwrap(), V::Bytes(Vec::new()));

        // NULL always stays NULL; other engines get no fix at all.
        assert_eq!(fix("year").apply(&V::Null).unwrap(), V::Null);
        assert!(mysql_cell_fix(crate::source::cdc::CdcEngine::Postgres, "bit(1)").is_none());
        // varbinary is NOT padded (only fixed-width binary is).
        assert!(mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, "varbinary(4)").is_none());
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

    // Finding #2: SQL Server MONEY/SMALLMONEY arrive as f64 (tiberius). The CDC
    // schema types them Decimal128(19,4), and the Float→decimal conversion had NO
    // 2^53 guard — so a MONEY value past the f64-exact range (already rounded by
    // the driver) was stored as a falsely-EXACT decimal, while the batch export
    // fails loud on the identical value. Parity: refuse it here too. RED against
    // the pre-fix `format!("{f:.prec$}")` with no guard.
    #[test]
    fn cdc_money_past_f64_exact_range_fails_loud_not_a_falsely_exact_decimal() {
        use arrow::datatypes::DataType;
        // 2^53 / 10^4 ≈ 9.007e11 — a value just under stays exact, 1e12 is past.
        assert_eq!(
            decimal_to_i128(&RivetValue::Float(9.0e11), 4),
            Some(900_000_000_000_i128 * 10_000)
        );
        assert_eq!(decimal_to_i128(&RivetValue::Float(1e12), 4), None);
        // Decimal256 (wide numeric) carries the same guard.
        assert!(decimal_to_i256(&RivetValue::Float(1e12), 4).is_none());
        // A small MONEY value still builds.
        let small = RivetValue::Float(12.34);
        let ok = build("c", &DataType::Decimal128(19, 4), &[Some(&small)])
            .expect("a small MONEY value builds");
        assert_eq!(ok.len(), 1);
        // A huge MONEY value in a Decimal column fails loud with the 2^53 message.
        let huge = RivetValue::Float(1e12);
        let err = build("c", &DataType::Decimal128(19, 4), &[Some(&huge)])
            .expect_err("a MONEY value past f64-exact range must fail loud, not round silently");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("2^53") && msg.contains("money"),
            "message must name the 2^53 range and MONEY: {msg}"
        );
    }

    /// The `Value::Time` arm's arithmetic, which nothing exercised: the Date arm
    /// had a test, the Time arm had none, and the mutation baseline carried
    /// SEVENTEEN operator survivors for this one conversion.
    ///
    /// Every component is non-zero and distinct so no two operators agree:
    /// days=2 (2*86400 = 172800 vs 2+86400 = 86402), h=3 (3*3600 = 10800 vs
    /// 3+3600 = 3603), mi=4 (4*60 = 240 vs 4+60 = 64). A fixture with zeros —
    /// the obvious "a time value" — makes `*`, `+` and `/` indistinguishable and
    /// is exactly why these survived.
    #[test]
    fn mysql_time_arm_pins_every_arithmetic_step() {
        // (2*86400 + 3*3600 + 4*60 + 5) * 1e6 + 678901
        let v = RivetValue::from_mysql(&mysql::Value::Time(false, 2, 3, 4, 5, 678_901));
        assert_eq!(
            v,
            RivetValue::TimeMicros(183_845_678_901),
            "days, hours, minutes, seconds and microseconds must each carry their \
             own weight"
        );
        // The sign applies to the WHOLE value, after the sum — not to a part.
        let neg = RivetValue::from_mysql(&mysql::Value::Time(true, 2, 3, 4, 5, 678_901));
        assert_eq!(neg, RivetValue::TimeMicros(-183_845_678_901));
        // A pure-microsecond value: the `+ us` addend must survive on its own.
        assert_eq!(
            RivetValue::from_mysql(&mysql::Value::Time(false, 0, 0, 0, 0, 1)),
            RivetValue::TimeMicros(1)
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

    // Finding #4: an integer that fits the declared width builds; a genuine NULL
    // stays null; but an OVERFLOW must fail LOUD (batch parity via `narrow`), never
    // the silent null it used to be — a BIT(64) with bit 63 set was dropped on CDC
    // while the batch export surfaced it.
    #[test]
    fn build_column_narrows_int_and_fails_loud_on_overflow() {
        use arrow::array::{Array, Int32Array};
        // In-range values + a genuine null build cleanly.
        let (v7, vnull_src) = (RivetValue::Int(7), RivetValue::Int(-5));
        let arr = build("c", &DataType::Int32, &[Some(&v7), None, Some(&vnull_src)]).unwrap();
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.value(0), 7);
        assert!(a.is_null(1)); // null cell stays null
        assert_eq!(a.value(2), -5);
        // Overflow → loud error, not a silent null wrap.
        let vmax = RivetValue::Int(i64::MAX);
        let err = build("c", &DataType::Int32, &[Some(&vmax)])
            .expect_err("an integer overflowing the declared width must fail loud");
        assert!(
            err.to_string().to_lowercase().contains("overflow"),
            "message names the overflow: {err}"
        );
        // The reported case: a BIT(64) with bit 63 set arrives as u64 > i64::MAX
        // and must fail loud in an Int64 column, exactly like the batch export.
        let bit64 = RivetValue::UInt(u64::MAX);
        assert!(
            build("c", &DataType::Int64, &[Some(&bit64)]).is_err(),
            "a BIT(64) value past i64::MAX must fail loud, never a silent CDC null"
        );
    }

    // Finding #6: a one-dimensional List column must never silently null a
    // non-array cell. parse_pg_array_literal preserves a multi-dimensional PG
    // literal as raw text bytes (never a flat Array of NULLs); build_list_column
    // then fails LOUD on that non-array cell — batch parity — instead of writing
    // a silent null list. RED against the pre-fix `_ => lb.append(false)`.
    #[test]
    fn list_column_fails_loud_on_a_non_array_cell_not_a_silent_null() {
        use arrow::datatypes::Field;
        let list_i32 = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        // A NULL cell is a valid null list — must still succeed.
        let none: Option<&RivetValue> = None;
        build("c", &list_i32, &[none]).expect("a null cell builds a null list");
        // The raw multi-dim literal (as text bytes) must fail loud.
        let raw = RivetValue::Bytes(b"{{1,2},{3,4}}".to_vec());
        let err = build("c", &list_i32, &[Some(&raw)])
            .expect_err("a non-array cell in a list column must fail loud");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("multi-dimensional") && msg.contains("::text"),
            "message must name the multi-dim cause and the ::text remediation: {msg}"
        );
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
            let arr = build("c", dt, &[None]).unwrap();
            assert_eq!(
                arr.data_type(),
                dt,
                "build({dt:?}) produced a mismatched type"
            );
        }
        // A type the sink can't build is rejected, so the resolver plans it as text.
        for dt in [
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            assert!(!is_buildable(&dt), "is_buildable must reject {dt:?}");
        }
    }

    /// Every typed arm refuses a mismatched non-NULL cell by name, and still nulls a genuine NULL.
    #[test]
    fn a_mismatched_cell_is_refused_by_column_name_and_a_null_stays_null() {
        use RivetValue as V;
        let dec = V::Bytes(b"1.5".to_vec());
        let cases: Vec<(DataType, V)> = vec![
            (DataType::Int8, dec.clone()),
            (DataType::Int16, dec.clone()),
            (DataType::Int32, dec.clone()),
            (DataType::Int64, dec.clone()),
            (DataType::UInt8, dec.clone()),
            (DataType::UInt16, dec.clone()),
            (DataType::UInt32, dec.clone()),
            (DataType::UInt64, dec.clone()),
            (DataType::Int32, V::Float(1.5)),
            (DataType::Boolean, V::Bytes(b"t".to_vec())),
            (DataType::Float32, V::Bytes(b"abc".to_vec())),
            (DataType::Float64, V::Bytes(b"1.5x".to_vec())),
            (DataType::Float64, V::Bool(true)),
            (
                DataType::Date32,
                V::DateTime(
                    NaiveDate::from_ymd_opt(2026, 1, 1)
                        .unwrap()
                        .and_hms_opt(13, 14, 0)
                        .unwrap(),
                ),
            ),
            (DataType::Utf8, V::Array(vec![V::Int(1)])),
            (DataType::Date32, V::Bytes(b"2026-01-01".to_vec())),
            (DataType::Date32, V::Int(3)),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                V::Bytes(b"2026-01-01 00:00:00".to_vec()),
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                V::Int(3),
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                V::Bytes(b"12:00:00".to_vec()),
            ),
            (DataType::Binary, V::Int(7)),
            (DataType::LargeBinary, V::Int(7)),
            (
                DataType::FixedSizeBinary(16),
                V::Bytes(b"not-a-uuid".to_vec()),
            ),
            (DataType::FixedSizeBinary(4), V::Int(7)),
        ];
        for (dt, v) in &cases {
            let err = build("amount", dt, &[Some(v)])
                .expect_err(&format!("{dt:?} must refuse {v:?}, not write NULL"));
            let msg = format!("{err:#}");
            assert!(
                msg.contains("column 'amount'") && msg.contains(&dt.to_string()),
                "the refusal must name the column and the type: {msg}"
            );
            assert!(
                msg.contains(&format!("{:?}", render_str(v))),
                "the refusal must show the value: {msg}"
            );
            let arr = build("amount", dt, &[Some(&V::Null), None]).unwrap();
            assert_eq!(arr.null_count(), 2, "{dt:?}: a genuine NULL stays NULL");
        }
    }

    /// A long offending value is shown truncated, not dumped whole into the message.
    #[test]
    fn a_mismatch_message_truncates_the_value() {
        let long = RivetValue::Bytes(vec![b'9'; 500]);
        let msg = format!(
            "{:#}",
            build("c", &DataType::Int64, &[Some(&long)]).unwrap_err()
        );
        assert!(msg.contains(&format!("\"{}\"…", "9".repeat(64))), "{msg}");
        assert!(!msg.contains(&"9".repeat(65)), "{msg}");
        let short = format!(
            "{:#}",
            build(
                "c",
                &DataType::Int64,
                &[Some(&RivetValue::Bytes(b"1.5".to_vec()))]
            )
            .unwrap_err()
        );
        assert!(
            short.contains("\"1.5\" (row 0)"),
            "no ellipsis on a short value: {short}"
        );
    }

    /// An unsigned cell in a Boolean column is true exactly when it is non-zero.
    #[test]
    fn an_unsigned_cell_in_a_boolean_column_is_true_when_non_zero() {
        let arr = build(
            "b",
            &DataType::Boolean,
            &[Some(&RivetValue::UInt(1)), Some(&RivetValue::UInt(0))],
        )
        .unwrap();
        let b = arr
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(b.value(0) && !b.value(1));
    }

    /// A MySQL TIME outside one day is refused naming its row and value; in-range times build.
    #[test]
    fn a_time_outside_one_day_is_refused_by_row_and_value() {
        let dt = DataType::Time64(TimeUnit::Microsecond);
        let ok = RivetValue::TimeMicros(0);
        for us in [
            (838 * 3600 + 59 * 60 + 59) * 1_000_000i64,
            -3_600_000_000,
            86_400_000_000,
        ] {
            let bad = RivetValue::TimeMicros(us);
            let r = build_column(&dt, &[Some(&ok), Some(&bad)]).unwrap_err();
            assert_eq!((r.row, &r.value), (1, &bad));
            assert!(r.reason.contains("outside 00:00..24:00"), "{}", r.reason);
        }
        let ok = [0i64, 86_399_999_999].map(RivetValue::TimeMicros);
        let arr = build_column(&dt, &[Some(&ok[0]), Some(&ok[1])]).unwrap();
        assert_eq!(arr.null_count(), 0);
    }

    /// A DATE override refuses a DateTime with a time of day (batch wording) and keeps a midnight one.
    #[test]
    fn a_date_column_refuses_a_time_of_day_instead_of_dropping_it() {
        use arrow::array::Date32Array;
        let day = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let midnight = RivetValue::DateTime(day.and_hms_opt(0, 0, 0).unwrap());
        let afternoon = RivetValue::DateTime(day.and_hms_opt(13, 14, 0).unwrap());
        let r = build_column(&DataType::Date32, &[Some(&midnight), Some(&afternoon)]).unwrap_err();
        assert_eq!((r.row, &r.value), (1, &afternoon));
        assert!(
            r.reason
                .contains("has a time of day, which a `date` override would drop")
                && r.reason.contains("declare it `timestamp`"),
            "{}",
            r.reason
        );
        let arr = build_column(&DataType::Date32, &[Some(&midnight)]).unwrap();
        let d = arr.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(d.value(0), 19_797, "2024-03-15 is day 19797 of the epoch");
    }

    /// Decimal text fills a float column by the shared parser; non-numeric text is refused.
    #[test]
    fn float_columns_parse_decimal_text_and_refuse_words() {
        use arrow::array::{Float32Array, Float64Array};
        let cells = [
            RivetValue::Bytes(b"1.5".to_vec()),
            RivetValue::Bytes(b"-12345.67".to_vec()),
            RivetValue::Bytes(b"0.10".to_vec()),
        ];
        let refs: Vec<Option<&RivetValue>> = cells.iter().map(Some).collect();
        let a64 = build_column(&DataType::Float64, &refs).unwrap();
        let a64 = a64.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(a64.values().to_vec(), vec![1.5, -12345.67, 0.1]);
        let a32 = build_column(&DataType::Float32, &refs).unwrap();
        let a32 = a32.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(a32.values().to_vec(), vec![1.5f32, -12345.67f32, 0.1f32]);
        for dt in [DataType::Float32, DataType::Float64] {
            let bad = RivetValue::Bytes(b"n/a".to_vec());
            let r = build_column(&dt, &[Some(&bad)]).unwrap_err();
            assert_eq!(
                (r.row, &r.value, r.reason.as_str()),
                (0, &bad, "is not a number")
            );
        }
    }

    /// Exact integer text fills an integer column; a fraction or overflow is refused.
    #[test]
    fn integer_columns_take_exact_integer_text_only() {
        use arrow::array::Int32Array;
        let cells = [
            RivetValue::Bytes(b"42".to_vec()),
            RivetValue::Bytes(b"-7.00".to_vec()),
        ];
        let refs: Vec<Option<&RivetValue>> = cells.iter().map(Some).collect();
        let arr = build_column(&DataType::Int32, &refs).unwrap();
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.values().to_vec(), vec![42, -7]);
        let frac = RivetValue::Bytes(b"1.5".to_vec());
        let r = build_column(&DataType::Int32, &[Some(&frac)]).unwrap_err();
        assert_eq!(r.reason, "is not an exact integer");
        let big = RivetValue::Bytes(b"3000000000".to_vec());
        let r = build_column(&DataType::Int32, &[Some(&big)]).unwrap_err();
        assert!(r.reason.starts_with("overflows"), "{}", r.reason);
    }

    /// A list element that overflows or has the wrong variant refuses the cell, never a null element.
    #[test]
    fn a_list_element_that_does_not_fit_is_refused_not_nulled() {
        use arrow::datatypes::Field;
        let list = |t| DataType::List(Arc::new(Field::new("item", t, true)));
        let cases = [
            (list(DataType::Int16), RivetValue::Int(70_000)),
            (list(DataType::Int32), RivetValue::Int(i64::MAX)),
            (list(DataType::Int64), RivetValue::Bytes(b"x".to_vec())),
            (list(DataType::Boolean), RivetValue::Int(1)),
            (list(DataType::Float32), RivetValue::Bytes(b"x".to_vec())),
            (list(DataType::Float64), RivetValue::Bool(true)),
            (list(DataType::Utf8), RivetValue::Array(vec![])),
        ];
        for (dt, elem) in cases {
            let cell = RivetValue::Array(vec![RivetValue::Null, elem.clone()]);
            let r = build_column(&dt, &[None, Some(&cell)])
                .expect_err(&format!("{dt}: {elem:?} must be refused"));
            assert_eq!((r.row, &r.value), (1, &cell), "{dt}");
            assert!(r.reason.starts_with("has an element"), "{}", r.reason);
        }
        let ok = RivetValue::Array(vec![RivetValue::Int(1), RivetValue::Null]);
        let arr = build_column(&list(DataType::Int16), &[Some(&ok)]).unwrap();
        assert_eq!(arr.len(), 1);
    }

    /// Every list element type builds from its driver variant, and the cell fold equals the built list's fold.
    #[test]
    fn every_list_element_type_builds_from_its_driver_variant() {
        use arrow::array::{Array, ListArray};
        use arrow::datatypes::Field;

        use crate::source::value_checksum::array_checksum;
        use RivetValue as V;
        let cases = [
            (DataType::Boolean, V::Bool(true), "true"),
            (DataType::Int64, V::Int(i64::MIN), "-9223372036854775808"),
            (DataType::Float32, V::Float(1.5), "1.5"),
            (DataType::Float64, V::Float(2.25), "2.25"),
            (DataType::Float64, V::Int(3), "3.0"),
        ];
        for (elem, v, shown) in cases {
            let dt = DataType::List(Arc::new(Field::new("item", elem.clone(), true)));
            let cell = V::Array(vec![v.clone(), V::Null]);
            let arr = build_column(&dt, &[Some(&cell)])
                .unwrap_or_else(|r| panic!("{elem}: {v:?} refused: {}", r.reason));
            let list = arr.as_any().downcast_ref::<ListArray>().unwrap().value(0);
            assert_eq!(list.len(), 2, "{elem}");
            assert!(list.is_null(1), "{elem}");
            let got = arrow::util::display::array_value_to_string(&list, 0).unwrap();
            assert_eq!(got, shown, "{elem}");
            assert_eq!(
                cells_checksum(&dt, &[Some(&cell)]),
                array_checksum(arr.as_ref()),
                "{elem}: cell fold drifted from the built list"
            );
        }
    }

    /// Every MySQL cell fix refuses a wire value it cannot read instead of returning NULL.
    #[test]
    fn mysql_cell_fixes_refuse_an_unreadable_wire_value() {
        use RivetValue as V;
        let fix = |native: &str| {
            mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native)
        };
        assert!(fix("timestamp").apply(&V::Bytes(b"soon".to_vec())).is_err());
        assert!(
            fix("timestamp")
                .apply(&V::Bytes(b"99999999999999999".to_vec()))
                .is_err()
        );
        assert!(fix("year").apply(&V::Bytes(b"MMXXIV".to_vec())).is_err());
        let e = fix("enum('a','b')");
        let err = e.apply(&V::Int(3)).unwrap_err();
        assert!(
            err.reason.contains("past the column's 2 ENUM labels"),
            "{err:?}"
        );
        assert!(e.apply(&V::UInt(u64::MAX)).is_err());
        let t = fix("time(6)");
        let err = t
            .apply(&V::TimeMicros((838 * 3600 + 59 * 60 + 59) * 1_000_000))
            .unwrap_err();
        assert_eq!(
            err.code.map(|c| c.id),
            Some("RIVET_SOURCE_VALUE_UNREPRESENTABLE")
        );
        assert!(
            err.reason
                .starts_with("mysql: TIME 838:59:59.000000 is outside 00:00..24:00")
                && err.reason.contains("CAST(col AS CHAR)"),
            "{err:?}"
        );
        assert_eq!(
            t.apply(&V::TimeMicros(1)).unwrap(),
            V::TimeMicros(1),
            "a time of day passes through"
        );
        assert_eq!(e.apply(&V::UInt(1)).unwrap(), V::Bytes(b"a".to_vec()));
    }

    /// A TIME rendered as text is `[-]hh:mm:ss.ffffff`, never raw microseconds.
    #[test]
    fn a_time_renders_as_time_beyond_day_text() {
        let us = -(838 * 3600 + 59 * 60 + 59) * 1_000_000i64;
        assert_eq!(render_str(&RivetValue::TimeMicros(us)), "-838:59:59.000000");
        let arr = build_column(&DataType::Utf8, &[Some(&RivetValue::TimeMicros(1))]).unwrap();
        let a = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(a.value(0), "00:00:00.000001");
    }
}
