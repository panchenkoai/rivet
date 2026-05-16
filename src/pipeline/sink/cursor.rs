//! Cursor value extraction from Arrow batches.
//!
//! Pure helper used by the streaming sink to remember the high-water mark of
//! the cursor column after each batch. Lives in its own file because the type
//! dispatch is long and its unit tests are independent — they need no
//! `ExportSink`, no `ResolvedRunPlan`, no I/O, just an `arrow::RecordBatch`.

use arrow::array::Array;
use arrow::array::{
    Date32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

/// Return the last row's value of `cursor_column` as a normalized string.
///
/// Returns `None` if:
/// - the column does not exist in the schema,
/// - the batch is empty,
/// - the last value is NULL,
/// - the column's Arrow type is not one of the supported cursor types
///   (int16/32/64, float64, utf8, timestamp(µs), date32).
pub(crate) fn extract_last_cursor_value(
    batch: &RecordBatch,
    cursor_column: &str,
    schema: &SchemaRef,
) -> Option<String> {
    let col_idx = schema.index_of(cursor_column).ok()?;
    let array = batch.column(col_idx);
    let last_row = batch.num_rows().checked_sub(1)?;

    if array.is_null(last_row) {
        return None;
    }

    match array.data_type() {
        DataType::Int16 => Some(
            array
                .as_any()
                .downcast_ref::<Int16Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Int32 => Some(
            array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Int64 => Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Float64 => Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Utf8 => Some(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(last_row)
                .to_string(),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?
                .value(last_row);
            let secs = micros / 1_000_000;
            let nsecs = ((micros % 1_000_000) * 1_000) as u32;
            let dt = chrono::DateTime::from_timestamp(secs, nsecs)?;
            Some(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        }
        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()?
                .value(last_row);
            let date =
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1)? + chrono::Duration::days(days as i64);
            Some(date.to_string())
        }
        _ => {
            log::warn!("cannot extract cursor for type {:?}", array.data_type());
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn cursor_int64() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("30".into())
        );
    }

    #[test]
    fn cursor_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100, 200]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("200".into())
        );
    }

    #[test]
    fn cursor_int16() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int16, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int16Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "id", &schema),
            Some("3".into())
        );
    }

    #[test]
    fn cursor_float64() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![1.5, 2.7]))],
        )
        .unwrap();
        let val = extract_last_cursor_value(&batch, "score", &schema).unwrap();
        assert!(val.starts_with("2.7"), "got: {val}");
    }

    #[test]
    fn cursor_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["aaa", "zzz"]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "key", &schema),
            Some("zzz".into())
        );
    }

    #[test]
    fn cursor_timestamp_microsecond() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let micros = 1_700_000_000_000_000i64; // 2023-11-14T22:13:20
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![micros]))],
        )
        .unwrap();
        let val = extract_last_cursor_value(&batch, "ts", &schema).unwrap();
        assert!(
            val.starts_with("2023-11-14T22:13:20"),
            "unexpected ts: {val}"
        );
    }

    #[test]
    fn cursor_date32() {
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));
        let days = 19723i32; // 2024-01-01
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![days]))],
        )
        .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "d", &schema),
            Some("2024-01-01".into())
        );
    }

    #[test]
    fn cursor_null_last_row_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }

    #[test]
    fn cursor_missing_column_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        assert_eq!(
            extract_last_cursor_value(&batch, "nonexistent", &schema),
            None
        );
    }

    #[test]
    fn cursor_empty_batch_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "id", &schema), None);
    }

    #[test]
    fn cursor_unsupported_type_returns_none() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bin",
            DataType::Binary,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from(vec![b"hello".as_slice()]))],
        )
        .unwrap();
        assert_eq!(extract_last_cursor_value(&batch, "bin", &schema), None);
    }
}
