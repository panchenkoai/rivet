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
    pub(crate) fn estimated_bytes(&self) -> usize {
        match self {
            RivetValue::Null | RivetValue::Bool(_) => 1,
            RivetValue::Int(_)
            | RivetValue::UInt(_)
            | RivetValue::Float(_)
            | RivetValue::TimeMicros(_) => 8,
            RivetValue::DateTime(_) => 12,
            RivetValue::Bytes(b) => b.len(),
            RivetValue::Array(v) => v.iter().map(RivetValue::estimated_bytes).sum::<usize>() + 16,
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
                match c {
                    None => b.append_null(),
                    Some(v) => match decimal_to_i128(v, *s) {
                        Some(i) => b.append_value(i),
                        // A non-null cell that doesn't parse (PG 'NaN'::numeric,
                        // ±Infinity, a non-finite money float) is unrepresentable
                        // in a Parquet decimal — fail LOUD, exactly like the
                        // batch export does, never a silent NULL.
                        None => anyhow::bail!(
                            "cdc: unsupported decimal payload {:?} (NaN/Infinity \
                             is not representable in a decimal column; batch \
                             fails identically)",
                            render_str(v)
                        ),
                    },
                }
            }
            Arc::new(b.finish())
        }
        // NUMERIC precision > 38 — same digit-exact parse, into i256.
        DataType::Decimal256(p, s) => {
            let mut b = arrow::array::Decimal256Builder::with_capacity(cells.len())
                .with_data_type(DataType::Decimal256(*p, *s));
            for c in cells {
                match c {
                    None => b.append_null(),
                    Some(v) => match decimal_to_i256(v, *s) {
                        Some(i) => b.append_value(i),
                        None => anyhow::bail!(
                            "cdc: unsupported decimal payload {:?} (NaN/Infinity \
                             is not representable in a decimal column; batch \
                             fails identically)",
                            render_str(v)
                        ),
                    },
                }
            }
            Arc::new(b.finish())
        }
        // One-dimensional arrays → a real List column, ELEMENT FIELD INCLUDED
        // (name/nullability must match the batch schema for ArrayData parity).
        DataType::List(field) => build_list_column(field, cells)?,
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

/// The CDC side-A fold: an INDEPENDENT per-column checksum computed from the
/// typed cells (`RivetValue`s) — the source-side twin of
/// [`crate::source::value_checksum::arrow_batch_checksums`] over the built
/// array. Extraction mirrors [`build_column`] arm-for-arm (same narrowing,
/// same decimal parse, same render), byte-encoded identically to the batch
/// checksum canon, XOR-combined. A type the shared rule skips contributes 0 —
/// the skip set cannot drift from the batch pass's. A mismatch against the
/// built array means the builder changed a value between decode and Arrow.
pub(crate) fn cells_checksum(dt: &DataType, cells: &[Option<&RivetValue>]) -> u64 {
    use RivetValue as V;
    use xxhash_rust::xxh3::xxh3_64;

    use crate::source::value_checksum::{ListElem, encode_list_cell, is_covered};

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
                if let Some(v) = $opt {
                    acc ^= xxh3_64(&v.to_le_bytes());
                }
            };
        }
        match dt {
            DataType::Boolean => {
                let b = match c {
                    V::Bool(x) => Some(*x),
                    V::Int(i) => Some(*i != 0),
                    V::UInt(u) => Some(*u != 0),
                    _ => None,
                };
                if let Some(b) = b {
                    acc ^= xxh3_64(&[b as u8]);
                }
            }
            DataType::Int16 => le!(int_of::<i16>(c)),
            DataType::Int32 => le!(int_of::<i32>(c)),
            DataType::Int64 => le!(int_of::<i64>(c)),
            DataType::UInt64 => le!(int_of::<u64>(c)),
            // NOT float_of(c) as f32: i64→f64→f32 double-rounds differently
            // than the builder's direct i64→f32 for large ints.
            DataType::Float32 => le!(match c {
                V::Float(f) => Some(*f as f32),
                V::Int(i) => Some(*i as f32),
                V::UInt(u) => Some(*u as f32),
                _ => None,
            }),
            DataType::Float64 => le!(float_of(c)),
            DataType::Date32 => le!(match c {
                V::DateTime(dt) => Some(epoch_days(dt.date())),
                _ => None,
            }),
            DataType::Timestamp(TimeUnit::Microsecond, _) => le!(match c {
                V::DateTime(dt) => Some(dt.and_utc().timestamp_micros()),
                _ => None,
            }),
            DataType::Time64(TimeUnit::Microsecond) => le!(match c {
                V::TimeMicros(us) => Some(*us),
                _ => None,
            }),
            DataType::Decimal128(_, s) => le!(decimal_to_i128(c, *s)),
            DataType::Decimal256(_, s) => {
                if let Some(v) = decimal_to_i256(c, *s) {
                    acc ^= xxh3_64(&v.to_le_bytes());
                }
            }
            DataType::Utf8 => acc ^= xxh3_64(render_str(c).as_bytes()),
            DataType::Binary => {
                if let V::Bytes(by) = c {
                    acc ^= xxh3_64(by);
                }
            }
            DataType::FixedSizeBinary(n) => {
                if let V::Bytes(by) = c
                    && by.len() == *n as usize
                {
                    acc ^= xxh3_64(by);
                }
            }
            DataType::List(f) => {
                if let V::Array(elems) = c {
                    let encoded: Vec<ListElem> = elems
                        .iter()
                        .map(|e| match (f.data_type(), e) {
                            (_, V::Null) => ListElem::Null,
                            (DataType::Boolean, V::Bool(b)) => ListElem::Bool(*b),
                            (DataType::Int16, V::Int(i)) => {
                                i16::try_from(*i).map_or(ListElem::Null, ListElem::I16)
                            }
                            (DataType::Int32, V::Int(i)) => {
                                i32::try_from(*i).map_or(ListElem::Null, ListElem::I32)
                            }
                            (DataType::Int64, V::Int(i)) => ListElem::I64(*i),
                            (DataType::Float32, V::Float(x)) => ListElem::F32(*x as f32),
                            (DataType::Float64, V::Float(x)) => ListElem::F64(*x),
                            (DataType::Float64, V::Int(i)) => ListElem::F64(*i as f64),
                            (DataType::Utf8, other) => {
                                ListElem::Str(render_str(other).into_bytes())
                            }
                            _ => ListElem::Null,
                        })
                        .collect();
                    acc ^= xxh3_64(&encode_list_cell(&encoded));
                }
            }
            _ => {}
        }
    }
    acc
}

/// The integer a [`build_column`] int arm would append for this cell — shared
/// by the fold so narrowing (try_from ⇒ null on overflow) cannot drift.
fn int_of<T: TryFrom<i64> + TryFrom<u64> + From<bool>>(v: &RivetValue) -> Option<T> {
    match v {
        RivetValue::Int(i) => T::try_from(*i).ok(),
        RivetValue::UInt(u) => T::try_from(*u).ok(),
        RivetValue::Bool(b) => Some(T::from(*b)),
        _ => None,
    }
}

fn float_of(v: &RivetValue) -> Option<f64> {
    match v {
        RivetValue::Float(f) => Some(*f),
        RivetValue::Int(i) => Some(*i as f64),
        RivetValue::UInt(u) => Some(*u as f64),
        _ => None,
    }
}

/// Build a `List<element>` column from [`RivetValue::Array`] cells. The child
/// builder is chosen by the element type; the LIST FIELD itself is preserved
/// (`with_field`) so the element name/nullability — and therefore the
/// `ArrayData` — matches the batch export exactly. A NULL cell is a null list;
/// an empty array is an empty (non-null) list; inner NULL elements survive.
fn build_list_column(
    field: &arrow::datatypes::FieldRef,
    cells: &[Option<&RivetValue>],
) -> Result<ArrayRef> {
    use RivetValue as V;
    use arrow::array::ListBuilder;

    macro_rules! list_col {
        ($child:expr, $append:expr) => {{
            let mut lb = ListBuilder::new($child).with_field(field.clone());
            for c in cells {
                match c {
                    Some(V::Array(elems)) => {
                        for e in elems {
                            #[allow(clippy::redundant_closure_call)]
                            $append(lb.values(), e);
                        }
                        lb.append(true);
                    }
                    // NULL cell / inner NULL → a null list (empty array stays a
                    // non-null empty list, handled by the Array arm above).
                    None | Some(V::Null) => lb.append(false),
                    // A non-null, non-array cell can only reach a one-dimensional
                    // List column as a MULTI-dimensional PG array literal
                    // (`{{1,2},{3,4}}`), which parse_pg_array_literal refuses to
                    // flatten and preserves as raw text. rivet's List is 1-D and
                    // cannot hold it — fail LOUD, exactly like the batch export
                    // (arrow_convert.rs), never a silent null list.
                    Some(other) => anyhow::bail!(
                        "cdc: multi-dimensional / non-representable array value {:?} \
                         cannot be stored in a one-dimensional list column; cast the \
                         column to text in the source export (e.g. col::text). The \
                         batch export fails identically.",
                        render_str(other)
                    ),
                }
            }
            Ok(Arc::new(lb.finish()) as ArrayRef)
        }};
    }

    match field.data_type() {
        DataType::Boolean => list_col!(
            BooleanBuilder::new(),
            |b: &mut BooleanBuilder, e: &RivetValue| {
                match e {
                    V::Bool(v) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
        ),
        DataType::Int16 => list_col!(
            Int16Builder::new(),
            |b: &mut Int16Builder, e: &RivetValue| {
                match e {
                    V::Int(i) => match i16::try_from(*i) {
                        Ok(v) => b.append_value(v),
                        Err(_) => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
        ),
        DataType::Int32 => list_col!(
            Int32Builder::new(),
            |b: &mut Int32Builder, e: &RivetValue| {
                match e {
                    V::Int(i) => match i32::try_from(*i) {
                        Ok(v) => b.append_value(v),
                        Err(_) => b.append_null(),
                    },
                    _ => b.append_null(),
                }
            }
        ),
        DataType::Int64 => list_col!(
            Int64Builder::new(),
            |b: &mut Int64Builder, e: &RivetValue| {
                match e {
                    V::Int(i) => b.append_value(*i),
                    _ => b.append_null(),
                }
            }
        ),
        DataType::Float32 => list_col!(
            Float32Builder::new(),
            |b: &mut Float32Builder, e: &RivetValue| {
                match e {
                    V::Float(f) => b.append_value(*f as f32),
                    _ => b.append_null(),
                }
            }
        ),
        DataType::Float64 => list_col!(
            Float64Builder::new(),
            |b: &mut Float64Builder, e: &RivetValue| {
                match e {
                    V::Float(f) => b.append_value(*f),
                    V::Int(i) => b.append_value(*i as f64),
                    _ => b.append_null(),
                }
            }
        ),
        DataType::Utf8 => list_col!(
            StringBuilder::new(),
            |b: &mut StringBuilder, e: &RivetValue| {
                match e {
                    V::Null => b.append_null(),
                    other => b.append_value(render_str(other)),
                }
            }
        ),
        other => anyhow::bail!("unsupported list element type {other:?}"),
    }
}

/// As [`decimal_to_i128`] but into `i256` for `Decimal256` columns.
fn decimal_to_i256(v: &RivetValue, scale: i8) -> Option<arrow::datatypes::i256> {
    let s = match v {
        RivetValue::Bytes(b) => std::str::from_utf8(b).ok()?.trim().to_string(),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        RivetValue::Float(f) if f.is_finite() => {
            format!("{f:.prec$}", prec = scale.max(0) as usize)
        }
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
        // digit-exact parse below.
        RivetValue::Float(f) if f.is_finite() => {
            format!("{f:.prec$}", prec = scale.max(0) as usize)
        }
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
            (MysqlCellFix::MediumIntSign, V::Int(i)) if *i >= (1 << 23) => V::Int(*i - (1 << 24)),
            (MysqlCellFix::MediumIntSign, V::UInt(u)) => {
                let i = *u as i64;
                V::Int(if i >= (1 << 23) { i - (1 << 24) } else { i })
            }
            (MysqlCellFix::EnumLabels(labels), V::Int(i)) => enum_label(labels, *i),
            (MysqlCellFix::EnumLabels(labels), V::UInt(u)) => enum_label(labels, *u as i64),
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
            (MysqlCellFix::BinaryPad(w), V::Bytes(b)) if b.len() < *w => {
                let mut p = b.clone();
                p.resize(*w, 0);
                V::Bytes(p)
            }
            (_, other) => other.clone(),
        }
    }
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
        RivetValue::Array(v) => {
            let inner: Vec<String> = v.iter().map(render_str).collect();
            format!("[{}]", inner.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                let _ = build_column(&dt, &cells);
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
                // 40000 overflows i16 → builder nulls; the fold must too.
                vec![
                    Some(V::Int(-5)),
                    Some(V::Int(40_000)),
                    Some(V::UInt(7)),
                    None,
                ],
            ),
            (DataType::Int32, vec![Some(V::Int(-8_388_608)), None]),
            (
                DataType::Int64,
                vec![Some(V::Int(i64::MIN)), Some(V::UInt(u64::MAX)), None],
            ),
            (
                DataType::UInt64,
                vec![Some(V::UInt(u64::MAX)), Some(V::Int(-1)), None],
            ),
            (
                DataType::Float32,
                vec![Some(V::Float(1.5)), Some(V::Int(i64::MAX)), None],
            ),
            (
                DataType::Float64,
                vec![Some(V::Float(f64::NAN)), Some(V::UInt(3)), None],
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
                // wrong width → builder nulls; the fold must too.
                vec![
                    Some(V::Bytes(vec![7u8; 16])),
                    Some(V::Bytes(vec![1, 2])),
                    None,
                ],
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
            let arr = build_column(&dt, &cells).unwrap();
            assert_eq!(
                cells_checksum(&dt, &cells),
                array_checksum(arr.as_ref()),
                "fold ≠ built array for {dt:?}"
            );
        }
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
        let fix = |native: &str| {
            mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, native).expect(native)
        };

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

        // MEDIUMINT: 24-bit sign extension (0x800000 → −8388608); positives and
        // the unsigned variant untouched.
        let mi = fix("mediumint");
        assert_eq!(mi.apply(&V::Int(8_388_608)), V::Int(-8_388_608));
        assert_eq!(mi.apply(&V::Int(8_388_607)), V::Int(8_388_607));
        assert!(
            mysql_cell_fix(crate::source::cdc::CdcEngine::Mysql, "mediumint unsigned").is_none()
        );

        // SET: bitmask (LE bytes) → comma-joined labels in declaration order,
        // the server's own rendering ('x,z' for bits 0+2 = 0x05).
        let st = fix("set('x','y','z')");
        assert_eq!(st.apply(&V::Bytes(vec![0x05])), V::Bytes(b"x,z".to_vec()));
        assert_eq!(st.apply(&V::UInt(0)), V::Bytes(Vec::new()));

        // NULL always stays NULL; other engines get no fix at all.
        assert_eq!(fix("year").apply(&V::Null), V::Null);
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
        build_column(&list_i32, &[none]).expect("a null cell builds a null list");
        // The raw multi-dim literal (as text bytes) must fail loud.
        let raw = RivetValue::Bytes(b"{{1,2},{3,4}}".to_vec());
        let err = build_column(&list_i32, &[Some(&raw)])
            .expect_err("a non-array cell in a list column must fail loud");
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("multi-dimensional") && msg.contains("::text"),
            "message must name the multi-dim cause and the ::text remediation: {msg}"
        );
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
