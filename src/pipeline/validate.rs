//! **Layer: Execution**
//!
//! Post-write output validation.  Opens the written file and counts rows to
//! verify the expected count.  Triggered only when `plan.validate = true`.

use std::io::BufRead;
use std::path::Path;

use crate::error::Result;
use crate::plan::FormatType;

/// Counts RFC-4180 records by streaming the file: a `\n` terminates a record
/// only outside double quotes, so quoted embedded newlines (which our own CSV
/// writer emits) stay inside one record. Doubled quotes within a quoted field
/// self-cancel, so toggling on every `"` tracks quoted state without parsing
/// fields. CRLF terminators work because the `\n` is the trigger; a final
/// record without a trailing newline is counted at EOF.
fn count_csv_records(path: &Path) -> Result<usize> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut records = 0usize;
    let mut in_quotes = false;
    let mut pending = false;
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            break;
        }
        let len = buf.len();
        for &byte in buf {
            match byte {
                b'"' => {
                    in_quotes = !in_quotes;
                    pending = true;
                }
                b'\n' if !in_quotes => {
                    records += 1;
                    pending = false;
                }
                _ => pending = true,
            }
        }
        reader.consume(len);
    }
    if pending {
        records += 1;
    }
    Ok(records)
}

pub fn validate_output(path: &Path, format: FormatType, expected_rows: usize) -> Result<()> {
    let actual = match format {
        FormatType::Parquet => {
            // The footer carries the authoritative row count — the ArrowWriter
            // wrote it from the same batches we just encoded. Read it instead
            // of decoding+decompressing every page a second time (the part was
            // already streamed once for its checksum). `try_new` parses and
            // validates the footer; a corrupt footer fails loudly here.
            let file = std::fs::File::open(path)?;
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
            builder.metadata().file_metadata().num_rows() as usize
        }
        FormatType::Csv => count_csv_records(path)?.saturating_sub(1),
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

    // ROAST-RED validate-csv-newline: validate_output counts physical lines,
    // not RFC-4180 records, so a correct CSV part with a quoted embedded "\n"
    // (exactly what src/format/csv.rs emits — see its test
    // `string_with_newline_is_quoted`) fails validation and aborts the export.
    // Asserts CORRECT behavior; expected to FAIL until the fix lands.
    #[test]
    fn roast_csv_quoted_embedded_newline_is_one_record() {
        // Header + exactly 2 RFC-4180 data records; record 1's second field
        // contains an embedded newline inside quotes (4 physical lines total).
        let f = write_temp(b"id,note\n1,\"line1\nline2\"\n2,plain\n", ".csv");
        let result = validate_output(f.path(), FormatType::Csv, 2);
        assert!(
            result.is_ok(),
            "a quoted embedded newline is part of one RFC-4180 record, not a \
             record boundary; line-count validation miscounts 2 records as 3 \
             rows: {}",
            result.unwrap_err()
        );
    }

    #[test]
    fn csv_crlf_terminators_count_records() {
        let f = write_temp(b"id,name\r\n1,alice\r\n2,bob\r\n", ".csv");
        validate_output(f.path(), FormatType::Csv, 2).unwrap();
    }

    #[test]
    fn csv_doubled_quotes_inside_quoted_field_with_newline() {
        // Escaped quotes ("") must not flip the in-quote state, so the
        // embedded newline after them is still inside the quoted field.
        let f = write_temp(b"id,note\n1,\"say \"\"hi\"\"\nbye\"\n2,plain\n", ".csv");
        validate_output(f.path(), FormatType::Csv, 2).unwrap();
    }

    #[test]
    fn csv_no_trailing_newline_counts_final_record() {
        let f = write_temp(b"id,name\n1,alice\n2,bob", ".csv");
        validate_output(f.path(), FormatType::Csv, 2).unwrap();
    }

    #[test]
    fn csv_quoted_field_at_eof_without_trailing_newline() {
        let f = write_temp(b"id,note\n1,\"line1\nline2\"", ".csv");
        validate_output(f.path(), FormatType::Csv, 1).unwrap();
    }

    #[test]
    fn csv_quoted_embedded_crlf_is_one_record() {
        let f = write_temp(b"id,note\r\n1,\"line1\r\nline2\"\r\n", ".csv");
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
