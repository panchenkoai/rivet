//! Under `verify: content`, whether a part a crashed run committed holds the rows its chunk read:
//! the row count the state logged, and every key inside the chunk's bounds.

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;

use super::resume_m8::Listed;
use crate::destination::Destination;
use crate::error::Result;
use crate::plan::{ExtractionStrategy, FormatType, ResolvedRunPlan};
use crate::state::StateStore;

/// What a part is read back by: the chunk column, whether its bounds count days, and the part format.
pub(crate) struct ContentCheck<'a> {
    dest: &'a dyn Destination,
    column: String,
    by_days: bool,
    format: FormatType,
}

/// What reading one part back found.
#[derive(Debug, PartialEq)]
pub(crate) enum PartVerdict {
    /// The row count and every key are the chunk's.
    Its,
    /// Another row count, a key outside the bounds, a null key, or bytes that do not decode.
    NotIts(String),
    /// The key column has a type the check cannot compare with the bounds.
    Unchecked(String),
}

impl<'a> ContentCheck<'a> {
    /// The check for a range-chunk export under `verify: content`; `None` otherwise.
    pub(crate) fn of(plan: &ResolvedRunPlan, dest: &'a dyn Destination) -> Option<Self> {
        let ExtractionStrategy::Chunked(cp) = &plan.strategy else {
            return None;
        };
        plan.verify.requires_content().then(|| Self {
            dest,
            column: cp.column.clone(),
            by_days: cp.by_days.is_some(),
            format: plan.format,
        })
    }

    /// `present` without the parts whose content is not their chunk's, so each such chunk is read again.
    pub(crate) fn keep_parts_of_their_chunk(
        &self,
        state: &StateStore,
        run_id: &str,
        present: &Listed,
    ) -> Result<Listed> {
        let tasks = state.list_chunk_tasks_for_run(run_id)?;
        let mut kept = present.clone();
        for f in state.list_files_for_run(run_id)? {
            let base = f.file_name.rsplit('/').next().unwrap_or(&f.file_name);
            if present.get(base).copied() != u64::try_from(f.bytes).ok() {
                continue;
            }
            let Some(task) = super::chunk_index_of(&f.file_name)
                .and_then(|i| i.parse::<i64>().ok())
                .and_then(|i| tasks.iter().find(|t| t.chunk_index == i))
            else {
                continue;
            };
            let (Ok(lo), Ok(hi)) = (task.start_key.parse::<i64>(), task.end_key.parse::<i64>())
            else {
                continue;
            };
            let verdict = match self.dest.read(&f.file_name) {
                Ok(bytes) => part_verdict(
                    bytes,
                    self.format,
                    &self.column,
                    self.by_days,
                    f.row_count,
                    lo,
                    hi,
                ),
                Err(e) => PartVerdict::NotIts(format!("cannot be read back: {e:#}")),
            };
            match verdict {
                PartVerdict::Its => {}
                PartVerdict::NotIts(why) => {
                    log::warn!(
                        "resume: committed part '{}' is not the rows of chunk {} ({why}) — \
                         its chunk is re-exported this run",
                        f.file_name,
                        task.chunk_index
                    );
                    kept.remove(base);
                }
                PartVerdict::Unchecked(why) => log::warn!(
                    "resume: `verify: content` cannot check committed part '{}' ({why}) — \
                     declared by its size only",
                    f.file_name
                ),
            }
        }
        Ok(kept)
    }
}

/// Reads one part back and compares it with its chunk: `rows` logged, keys in `lo..=hi`.
pub(crate) fn part_verdict(
    bytes: Vec<u8>,
    format: FormatType,
    column: &str,
    by_days: bool,
    rows: i64,
    lo: i64,
    hi: i64,
) -> PartVerdict {
    let keys = match format {
        FormatType::Parquet => parquet_keys(bytes, column, by_days),
        FormatType::Csv => csv_keys(&bytes, column, by_days),
    };
    let keys = match keys {
        Ok(Keys::Read(k)) => k,
        Ok(Keys::Unreadable(why)) => return PartVerdict::Unchecked(why),
        Err(e) => return PartVerdict::NotIts(format!("does not decode: {e:#}")),
    };
    if i64::try_from(keys.len()).ok() != Some(rows) {
        return PartVerdict::NotIts(format!("{} rows, the state logged {rows}", keys.len()));
    }
    let inside = |k: &Key| match *k {
        Key::Int(v) => (lo..=hi).contains(&v),
        Key::Float(v) => v >= lo as f64 && v <= hi as f64,
        Key::Null => false,
    };
    match keys.iter().find(|k| !inside(k)) {
        None => PartVerdict::Its,
        Some(k) => PartVerdict::NotIts(format!("key {k:?} outside {lo}..={hi}")),
    }
}

/// One key of a part, as the bounds compare it.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Key {
    Int(i64),
    Float(f64),
    Null,
}

/// The keys of a part, or why their type cannot be compared with the bounds.
enum Keys {
    Read(Vec<Key>),
    Unreadable(String),
}

/// The keys of `column` in a Parquet part; `by_days` reads a date or timestamp as days since 1970-01-01.
fn parquet_keys(bytes: Vec<u8>, column: &str, by_days: bool) -> Result<Keys> {
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )?;
    let schema = builder.schema().clone();
    let i = schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(column))
        .ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::INTERNAL_RUN_INVARIANT,
                format!("no column `{column}`"),
            )
        })?;
    let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), [i]);
    let mut keys = Vec::new();
    for batch in builder.with_projection(mask).build()? {
        let col = batch?.column(0).clone();
        match keys_of(&col, by_days)? {
            Keys::Read(k) => keys.extend(k),
            unreadable => return Ok(unreadable),
        }
    }
    Ok(Keys::Read(keys))
}

/// The keys of one Arrow column.
fn keys_of(col: &dyn Array, by_days: bool) -> Result<Keys> {
    let dt = col.data_type();
    let cast = |to: &DataType| arrow::compute::cast(col, to);
    let keys = match dt {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) if by_days => {
            date32_keys(&cast(&DataType::Date32)?)
        }
        _ if by_days => return Ok(Keys::Unreadable(format!("a day key of type {dt}"))),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let a = cast(&DataType::Int64)?;
            let a = a
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("cast to Int64 gives an Int64Array");
            a.iter().map(|v| v.map_or(Key::Null, Key::Int)).collect()
        }
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => {
            let a = cast(&DataType::Float64)?;
            let a = a
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("cast to Float64 gives a Float64Array");
            a.iter().map(|v| v.map_or(Key::Null, Key::Float)).collect()
        }
        _ => return Ok(Keys::Unreadable(format!("a key of type {dt}"))),
    };
    Ok(Keys::Read(keys))
}

/// The days of a `Date32` array.
fn date32_keys(a: &dyn Array) -> Vec<Key> {
    let a = a
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .expect("cast to Date32 gives a Date32Array");
    a.iter()
        .map(|v| v.map_or(Key::Null, |d| Key::Int(d.into())))
        .collect()
}

/// The keys of `column` in a CSV part with a header row; an empty field is a null key.
fn csv_keys(bytes: &[u8], column: &str, by_days: bool) -> Result<Keys> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(bytes);
    let i = reader
        .headers()?
        .iter()
        .position(|h| h.eq_ignore_ascii_case(column))
        .ok_or_else(|| {
            crate::error::coded(
                crate::error::codes::INTERNAL_RUN_INVARIANT,
                format!("no column `{column}`"),
            )
        })?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
    let mut keys = Vec::new();
    for record in reader.records() {
        let record = record?;
        let field = record.get(i).unwrap_or_default();
        let key = if field.is_empty() {
            Key::Null
        } else if by_days {
            let day = field.get(..10).unwrap_or(field);
            match chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d") {
                Ok(d) => Key::Int((d - epoch).num_days()),
                Err(_) => return Ok(Keys::Unreadable(format!("a day key `{field}`"))),
            }
        } else if let Ok(v) = field.parse::<i64>() {
            Key::Int(v)
        } else if let Ok(v) = field.parse::<f64>() {
            Key::Float(v)
        } else {
            return Ok(Keys::Unreadable(format!("a key `{field}`")));
        };
        keys.push(key);
    }
    Ok(Keys::Read(keys))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn parquet_of(col: Arc<dyn Array>) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ID", col.data_type().clone(), true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let notes: Arc<dyn Array> = Arc::new(StringArray::from(vec!["x"; col.len()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![col, notes]).unwrap();
        let mut out = Vec::new();
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut out, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        out
    }

    fn pq(bytes: &[u8], rows: i64, lo: i64, hi: i64) -> PartVerdict {
        part_verdict(
            bytes.to_vec(),
            FormatType::Parquet,
            "id",
            false,
            rows,
            lo,
            hi,
        )
    }

    /// A part is its chunk's only with the logged row count and every key inside the bounds; a sibling's part, a null key or garbage is not.
    #[test]
    fn a_part_is_its_chunks_only_by_count_and_bounds() {
        let own = parquet_of(Arc::new(Int64Array::from_iter_values(11..=20)));
        assert_eq!(pq(&own, 10, 11, 20), PartVerdict::Its);
        let sibling = parquet_of(Arc::new(Int64Array::from_iter_values(21..=30)));
        assert!(matches!(pq(&sibling, 10, 11, 20), PartVerdict::NotIts(_)));
        assert!(matches!(pq(&own, 9, 11, 20), PartVerdict::NotIts(_)));
        let null = parquet_of(Arc::new(Int64Array::from(vec![Some(11), None])));
        assert!(matches!(pq(&null, 2, 11, 20), PartVerdict::NotIts(_)));
        assert!(matches!(
            pq(b"not a parquet", 1, 0, 9),
            PartVerdict::NotIts(_)
        ));
        let float = parquet_of(Arc::new(Float64Array::from(vec![11.5, 20.0])));
        assert_eq!(pq(&float, 2, 11, 20), PartVerdict::Its);
        assert!(matches!(pq(&float, 2, 12, 20), PartVerdict::NotIts(_)));
        let text = parquet_of(Arc::new(StringArray::from(vec!["11"])));
        assert!(matches!(pq(&text, 1, 11, 20), PartVerdict::Unchecked(_)));
    }

    /// `by_days` reads a date key as days since 1970-01-01, in Parquet and in CSV.
    #[test]
    fn a_day_key_is_read_as_days_since_the_epoch() {
        let days = parquet_of(Arc::new(Date32Array::from(vec![20000, 20001])));
        let v = |b: &[u8], f, lo, hi| part_verdict(b.to_vec(), f, "id", true, 2, lo, hi);
        assert_eq!(
            v(&days, FormatType::Parquet, 20000, 20001),
            PartVerdict::Its
        );
        assert!(matches!(
            v(&days, FormatType::Parquet, 20001, 20002),
            PartVerdict::NotIts(_)
        ));
        let csv = b"id,note\n2024-10-04,a\n2024-10-05 10:00:00,b\n";
        assert_eq!(v(csv, FormatType::Csv, 20000, 20001), PartVerdict::Its);
        assert!(matches!(
            v(csv, FormatType::Csv, 20001, 20001),
            PartVerdict::NotIts(_)
        ));
    }

    /// `by_days` reads only a date or timestamp key, and a date key is not compared without it.
    #[test]
    fn a_day_key_needs_by_days_and_by_days_needs_a_day_key() {
        let days = parquet_of(Arc::new(Date32Array::from(vec![20000])));
        let ints = parquet_of(Arc::new(Int64Array::from(vec![5])));
        let v =
            |b: Vec<u8>, by_days| part_verdict(b, FormatType::Parquet, "id", by_days, 1, 0, 30000);
        assert!(matches!(v(days, false), PartVerdict::Unchecked(_)));
        assert!(matches!(v(ints, true), PartVerdict::Unchecked(_)));
    }

    /// A CSV part is read by its header; an empty key is null, and a key that is no number is not compared.
    #[test]
    fn a_csv_part_is_read_by_its_header() {
        let v = |b: &[u8], rows, lo, hi| {
            part_verdict(b.to_vec(), FormatType::Csv, "id", false, rows, lo, hi)
        };
        assert_eq!(
            v(b"note,ID\na,11\n\"b,\nc\",20\n", 2, 11, 20),
            PartVerdict::Its
        );
        assert!(matches!(
            v(b"note,ID\na,21\n", 1, 11, 20),
            PartVerdict::NotIts(_)
        ));
        assert!(matches!(
            v(b"note,ID\na,\n", 1, 11, 20),
            PartVerdict::NotIts(_)
        ));
        assert!(matches!(
            v(b"note,ID\na,x1\n", 1, 11, 20),
            PartVerdict::Unchecked(_)
        ));
        assert!(matches!(v(b"note\na\n", 1, 11, 20), PartVerdict::NotIts(_)));
    }
}
