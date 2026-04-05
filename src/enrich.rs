use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::config::MetaColumns;
use crate::error::Result;

pub const COL_EXPORTED_AT: &str = "_rivet_exported_at";
pub const COL_ROW_HASH: &str = "_rivet_row_hash";

/// Extend an Arrow schema with requested meta columns.
pub fn enrich_schema(schema: &SchemaRef, meta: &MetaColumns) -> SchemaRef {
    if !meta.exported_at && !meta.row_hash {
        return schema.clone();
    }
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
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
    Arc::new(Schema::new(fields))
}

/// Add meta columns to a RecordBatch.
/// `exported_at_us` is a single microsecond-precision UTC timestamp shared by all rows.
pub fn enrich_batch(
    batch: &RecordBatch,
    meta: &MetaColumns,
    enriched_schema: &SchemaRef,
    exported_at_us: i64,
) -> Result<RecordBatch> {
    if !meta.exported_at && !meta.row_hash {
        return Ok(batch.clone());
    }

    let n = batch.num_rows();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    if meta.exported_at {
        let ts_array =
            TimestampMicrosecondArray::from(vec![Some(exported_at_us); n]).with_timezone("UTC");
        columns.push(Arc::new(ts_array));
    }

    if meta.row_hash {
        let mut hashes = Vec::with_capacity(n);
        for row in 0..n {
            hashes.push(hash_row(batch, row));
        }
        columns.push(Arc::new(Int64Array::from(hashes)));
    }

    Ok(RecordBatch::try_new(enriched_schema.clone(), columns)?)
}

/// Compute a deterministic 64-bit hash of all column values in a row.
/// Uses the lower 64 bits of xxHash3-128 reinterpreted as signed i64.
fn hash_row(batch: &RecordBatch, row: usize) -> i64 {
    use xxhash_rust::xxh3::xxh3_128;

    let mut buf = Vec::with_capacity(256);
    for col_idx in 0..batch.num_columns() {
        let array = batch.column(col_idx);
        if array.is_null(row) {
            buf.extend_from_slice(b"\x00");
        } else {
            let s = arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
            buf.extend_from_slice(s.as_bytes());
        }
        buf.push(b'\x1f'); // unit separator between fields
    }
    let h = xxh3_128(&buf);
    h as i64 // lower 64 bits, reinterpreted as signed
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

    #[test]
    fn enrich_disabled_is_noop() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: false,
            row_hash: false,
        };
        let enriched_schema = enrich_schema(&schema, &meta);
        assert_eq!(enriched_schema.fields().len(), 2);
        let result = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();
        assert_eq!(result.num_columns(), 2);
    }

    #[test]
    fn enrich_exported_at_only() {
        let (schema, batch) = sample_batch();
        let meta = MetaColumns {
            exported_at: true,
            row_hash: false,
        };
        let enriched_schema = enrich_schema(&schema, &meta);
        assert_eq!(enriched_schema.fields().len(), 3);
        assert_eq!(enriched_schema.field(2).name(), COL_EXPORTED_AT);

        let ts = 1_711_612_800_000_000i64;
        let result = enrich_batch(&batch, &meta, &enriched_schema, ts).unwrap();
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
        let enriched_schema = enrich_schema(&schema, &meta);
        assert_eq!(enriched_schema.field(2).name(), COL_ROW_HASH);
        assert_eq!(*enriched_schema.field(2).data_type(), DataType::Int64);

        let result = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();
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
        let enriched_schema = enrich_schema(&schema, &meta);
        assert_eq!(enriched_schema.fields().len(), 4);
        assert_eq!(enriched_schema.field(2).name(), COL_EXPORTED_AT);
        assert_eq!(enriched_schema.field(3).name(), COL_ROW_HASH);

        let result = enrich_batch(&batch, &meta, &enriched_schema, 123456).unwrap();
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
        let enriched_schema = enrich_schema(&schema, &meta);

        let r1 = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();
        let r2 = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();

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
        let enriched_schema = enrich_schema(&schema, &meta);
        let result = enrich_batch(&batch, &meta, &enriched_schema, 0).unwrap();
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
