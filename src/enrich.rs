use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::config::MetaColumns;
use crate::content_hash::{self, ContentHashConfig};
use crate::error::Result;

pub const COL_EXPORTED_AT: &str = "_rivet_exported_at";
pub const COL_ROW_HASH: &str = "_rivet_row_hash";

/// Extend an Arrow schema with the columns rivet adds.
///
/// `hash` comes first and the `_rivet_*` meta columns after, because the two
/// are different kinds of thing: `__content_hash` is part of the warehouse
/// table's contract (the audit reads it), while the meta columns are optional
/// provenance. [`enrich_batch`] builds its arrays in this same order — the two
/// must not be able to disagree, which is why both live here rather than being
/// appended by each caller.
///
/// Fallible only because of `hash`: the content hash refuses column types it
/// cannot render identically in SQL, and that refusal belongs at schema time,
/// before a single row is read.
pub fn enrich_schema(
    schema: &SchemaRef,
    meta: &MetaColumns,
    hash: Option<&ContentHashConfig>,
) -> Result<SchemaRef> {
    if !meta.exported_at && !meta.row_hash && hash.is_none() {
        return Ok(schema.clone());
    }
    let base = match hash {
        Some(h) => content_hash::hashed_schema(schema, h)?,
        None => schema.clone(),
    };
    if !meta.exported_at && !meta.row_hash {
        return Ok(base);
    }
    let mut fields: Vec<Arc<Field>> = base.fields().iter().cloned().collect();
    if meta.exported_at {
        fields.push(Arc::new(Field::new(
            COL_EXPORTED_AT,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )));
    }
    if meta.row_hash {
        fields.push(Arc::new(Field::new(COL_ROW_HASH, DataType::Int64, false)));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Add rivet's columns to a RecordBatch, in [`enrich_schema`]'s order.
/// `exported_at_us` is a single microsecond-precision UTC timestamp shared by all rows.
pub fn enrich_batch(
    batch: &RecordBatch,
    meta: &MetaColumns,
    enriched_schema: &SchemaRef,
    exported_at_us: i64,
    hash: Option<&ContentHashConfig>,
) -> Result<RecordBatch> {
    if !meta.exported_at && !meta.row_hash && hash.is_none() {
        return Ok(batch.clone());
    }

    let n = batch.num_rows();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    if let Some(h) = hash {
        columns.push(content_hash::hash_array(batch, h)?);
    }

    if meta.exported_at {
        let ts_array =
            TimestampMicrosecondArray::from(vec![Some(exported_at_us); n]).with_timezone("UTC");
        columns.push(Arc::new(ts_array));
    }

    if meta.row_hash {
        columns.push(Arc::new(hash_column(batch, n)));
    }

    Ok(RecordBatch::try_new(enriched_schema.clone(), columns)?)
}

/// Compute deterministic 64-bit hashes for all rows in a batch.
/// Creates one ArrayFormatter per column (avoids per-cell String allocations),
/// then reuses a single scratch buffer across all rows.
fn hash_column(batch: &RecordBatch, n: usize) -> Int64Array {
    use std::io::Write as IoWrite;
    use xxhash_rust::xxh3::xxh3_128;

    let options = arrow::util::display::FormatOptions::default();
    let formatters: Vec<Option<arrow::util::display::ArrayFormatter>> = (0..batch.num_columns())
        .map(|i| {
            arrow::util::display::ArrayFormatter::try_new(batch.column(i).as_ref(), &options).ok()
        })
        .collect();

    let mut buf = Vec::with_capacity(256);
    let mut hashes = Vec::with_capacity(n);
    for row in 0..n {
        buf.clear();
        for (col_idx, fmt_opt) in formatters.iter().enumerate() {
            let array = batch.column(col_idx);
            if array.is_null(row) {
                buf.extend_from_slice(b"\x00");
            } else if let Some(fmt) = fmt_opt {
                let _ = write!(buf, "{}", fmt.value(row));
            }
            buf.push(b'\x1f');
        }
        let h = xxh3_128(&buf);
        hashes.push(h as i64);
    }
    Int64Array::from(hashes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;

    fn sample_batch() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    None,
                    Some("charlie"),
                ])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn hash_cfg() -> ContentHashConfig {
        ContentHashConfig {
            pk: "id".into(),
            cols: vec!["name".into()],
        }
    }

    // `__content_hash` precedes the `_rivet_*` meta columns, and — the part that
    // actually matters — enrich_schema and enrich_batch agree on that order.
    // They are separate functions over the same list, so a divergence would show
    // up as an Arrow "column count/type mismatch" at write time, on a live run.
    #[test]
    fn content_hash_column_precedes_meta_columns() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: true,
        };
        let cfg = hash_cfg();
        let enriched = enrich_schema(&schema, &meta, Some(&cfg)).unwrap();
        assert_eq!(
            enriched
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec![
                "id",
                "name",
                content_hash::COL_CONTENT_HASH,
                COL_EXPORTED_AT,
                COL_ROW_HASH
            ]
        );
        // try_new is what enforces the agreement — this unwrap IS the assertion.
        let out = enrich_batch(&batch, &meta, &enriched, 0, Some(&cfg)).unwrap();
        let hashes = out
            .column_by_name(content_hash::COL_CONTENT_HASH)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(hashes.value(0), content_hash::hash_of("1|alice"));
        assert_eq!(
            hashes.value(1),
            content_hash::hash_of(&format!("2|{}", content_hash::NULL_SENTINEL))
        );
    }

    // The hash must be computed over SOURCE columns only. If it were computed
    // after enrichment, `_rivet_exported_at` would enter the text and the value
    // would change every run, so no audit could ever match it.
    #[test]
    fn content_hash_ignores_the_meta_columns_it_ships_beside() {
        let (schema, batch) = sample_batch();
        let cfg = hash_cfg();
        let bare = MetaColumns {
            exported_at: false,
            row_hash: false,
        };
        let with_meta = MetaColumns {
            exported_at: true,
            row_hash: true,
        };
        let read = |meta: &MetaColumns| {
            let sch = enrich_schema(&schema, meta, Some(&cfg)).unwrap();
            enrich_batch(&batch, meta, &sch, 999, Some(&cfg))
                .unwrap()
                .column_by_name(content_hash::COL_CONTENT_HASH)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(read(&bare), read(&with_meta));
    }

    #[test]
    fn enrich_disabled_is_noop() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: false,
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        assert_eq!(enriched_schema.fields().len(), 2);
        let result = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn enrich_exported_at_only() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: false,
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        assert_eq!(enriched_schema.fields().len(), 3);
        assert_eq!(enriched_schema.field(2).name(), COL_EXPORTED_AT);

        let ts = 1_711_612_800_000_000i64;
        let result = enrich_batch(&batch, &meta, &enriched_schema, ts, None).unwrap();
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 3);

        let ts_col = result
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), ts);
        assert_eq!(ts_col.value(2), ts);
    }

    #[test]
    fn enrich_row_hash_only() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: true,
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        assert_eq!(enriched_schema.field(2).name(), COL_ROW_HASH);
        assert_eq!(*enriched_schema.field(2).data_type(), DataType::Int64);

        let result = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();
        let hash_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Different rows produce different hashes
        assert_ne!(hash_col.value(0), hash_col.value(1));
        assert_ne!(hash_col.value(1), hash_col.value(2));
    }

    #[test]
    fn enrich_both_columns() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: true,
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        assert_eq!(enriched_schema.fields().len(), 4);
        assert_eq!(enriched_schema.field(2).name(), COL_EXPORTED_AT);
        assert_eq!(enriched_schema.field(3).name(), COL_ROW_HASH);

        let result = enrich_batch(&batch, &meta, &enriched_schema, 123456, None).unwrap();
        assert_eq!(result.num_columns(), 4);
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn hash_is_deterministic() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: true,
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();

        let r1 = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();
        let r2 = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();

        let h1 = r1.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let h2 = r2.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..3 {
            assert_eq!(
                h1.value(i),
                h2.value(i),
                "hash should be deterministic for row {i}"
            );
        }
    }

    #[test]
    fn hash_distinguishes_null_from_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![None, Some("")]))],
        )
        .unwrap();

        let meta = MetaColumns {
            exported_at: false,
            row_hash: true,
        };
        let enriched_schema = enrich_schema(&schema, &meta, None).unwrap();
        let result = enrich_batch(&batch, &meta, &enriched_schema, 0, None).unwrap();
        let hashes = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_ne!(
            hashes.value(0),
            hashes.value(1),
            "NULL and empty string should hash differently"
        );
    }
}
