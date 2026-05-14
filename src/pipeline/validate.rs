//! **Layer: Execution**
//!
//! Post-write output validation.  Opens the written file and counts rows to
//! verify the expected count.  Triggered only when `plan.validate = true`.

use std::path::Path;

use crate::error::Result;
use crate::plan::FormatType;

pub fn validate_output(path: &Path, format: FormatType, expected_rows: usize) -> Result<()> {
    let actual = match format {
        FormatType::Parquet => {
            let file = std::fs::File::open(path)?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            let mut count = 0usize;
            for batch in reader {
                count += batch?.num_rows();
            }
            count
        }
        FormatType::Csv => {
            let content = std::fs::read_to_string(path)?;
            let lines = content.lines().count();
            lines.saturating_sub(1)
        }
    };

    if actual != expected_rows {
        anyhow::bail!(
            "validation failed: expected {} rows, got {} in {}",
            expected_rows,
            actual,
            path.display()
        );
    }

    log::info!("validation passed: {} rows verified", actual);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp(content: &[u8], suffix: &str) -> NamedTempFile {
        let mut f = tempfile::Builder::new()
            .suffix(suffix)
            .tempfile()
            .expect("tempfile");
        f.write_all(content).expect("write");
        f.flush().expect("flush");
        f
    }

    // ── CSV ─────────────────────────────────────────────────────────────────

    #[test]
    fn csv_exact_row_count_passes() {
        let f = write_temp(b"id,name\n1,alice\n2,bob\n3,carol\n", ".csv");
        validate_output(f.path(), FormatType::Csv, 3).unwrap();
    }

    #[test]
    fn csv_wrong_row_count_fails() {
        let f = write_temp(b"id,name\n1,alice\n2,bob\n", ".csv");
        let err = validate_output(f.path(), FormatType::Csv, 5).unwrap_err();
        assert!(err.to_string().contains("expected 5"), "{err}");
    }

    #[test]
    fn csv_empty_body_zero_rows_passes() {
        // Header only — zero data rows.
        let f = write_temp(b"id,name\n", ".csv");
        validate_output(f.path(), FormatType::Csv, 0).unwrap();
    }

    #[test]
    fn csv_empty_file_zero_rows_passes() {
        // Completely empty file: lines().count() == 0, saturating_sub(1) == 0.
        let f = write_temp(b"", ".csv");
        validate_output(f.path(), FormatType::Csv, 0).unwrap();
    }

    #[test]
    fn csv_trailing_newline_does_not_count_as_row() {
        // "header\nrow1\n" → 2 lines → 2 - 1 = 1 data row
        let f = write_temp(b"id\n42\n", ".csv");
        validate_output(f.path(), FormatType::Csv, 1).unwrap();
    }

    // ── Parquet ──────────────────────────────────────────────────────────────

    fn write_parquet_rows(n_rows: usize) -> NamedTempFile {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (0..n_rows as i64).collect();
        let names: Vec<&str> = (0..n_rows).map(|_| "test").collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("batch");

        let mut f = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .expect("tempfile");
        let mut writer = ArrowWriter::try_new(&mut f, schema, None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close");
        f
    }

    #[test]
    fn parquet_exact_row_count_passes() {
        let f = write_parquet_rows(7);
        validate_output(f.path(), FormatType::Parquet, 7).unwrap();
    }

    #[test]
    fn parquet_wrong_row_count_fails() {
        let f = write_parquet_rows(3);
        let err = validate_output(f.path(), FormatType::Parquet, 10).unwrap_err();
        assert!(err.to_string().contains("expected 10"), "{err}");
        assert!(err.to_string().contains("got 3"), "{err}");
    }

    #[test]
    fn parquet_zero_rows_passes() {
        let f = write_parquet_rows(0);
        validate_output(f.path(), FormatType::Parquet, 0).unwrap();
    }

    #[test]
    fn parquet_corrupt_file_returns_error() {
        let f = write_temp(b"not a parquet file at all", ".parquet");
        let err = validate_output(f.path(), FormatType::Parquet, 1).unwrap_err();
        // Should fail with a parquet parse error, not a panic.
        let msg = err.to_string();
        assert!(
            !msg.contains("expected"),
            "should be parse error, not row-count mismatch: {msg}"
        );
    }

    #[test]
    fn parquet_multi_batch_row_count_passes() {
        // Write two separate batches so the reader sees multiple batches.
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut f = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .expect("tempfile");
        let mut writer = ArrowWriter::try_new(&mut f, schema.clone(), None).expect("writer");
        for _ in 0..3 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1i64, 2, 3, 4]))],
            )
            .expect("batch");
            writer.write(&batch).expect("write");
        }
        writer.close().expect("close");
        validate_output(f.path(), FormatType::Parquet, 12).unwrap();
    }
}
