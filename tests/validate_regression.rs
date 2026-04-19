use std::io::Write;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rivet::config::{CompressionType, FormatType};
use rivet::format::Format;
use rivet::format::csv::CsvFormat;
use rivet::format::parquet::ParquetFormat;
use rivet::pipeline::validate_output;

fn write_parquet(batches: &[RecordBatch]) -> tempfile::NamedTempFile {
    let schema = batches[0].schema();
    let fmt = ParquetFormat::new(CompressionType::None, None);
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = fmt.create_writer(&schema, Box::new(file)).unwrap();
    for batch in batches {
        writer.write_batch(batch).unwrap();
    }
    writer.finish().unwrap();
    tmp
}

fn write_csv(batches: &[RecordBatch]) -> tempfile::NamedTempFile {
    let schema = batches[0].schema();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = CsvFormat.create_writer(&schema, Box::new(file)).unwrap();
    for batch in batches {
        writer.write_batch(batch).unwrap();
    }
    writer.finish().unwrap();
    tmp
}

fn sample_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let ids: Vec<i32> = (1..=n as i32).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
}

// ─── Parquet validation ──────────────────────────────────────

#[test]
fn validate_parquet_correct_count_passes() {
    let batch = sample_batch(50);
    let tmp = write_parquet(&[batch]);
    assert!(validate_output(tmp.path(), FormatType::Parquet, 50).is_ok());
}

#[test]
fn validate_parquet_wrong_count_fails() {
    let batch = sample_batch(50);
    let tmp = write_parquet(&[batch]);
    let err = validate_output(tmp.path(), FormatType::Parquet, 99).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("expected 99"), "got: {msg}");
    assert!(msg.contains("got 50"), "got: {msg}");
}

#[test]
fn validate_parquet_multi_batch() {
    let b1 = sample_batch(30);
    let b2 = sample_batch(20);
    let tmp = write_parquet(&[b1, b2]);
    assert!(validate_output(tmp.path(), FormatType::Parquet, 50).is_ok());
}

#[test]
fn validate_parquet_nonexistent_file_errors() {
    let path = std::path::Path::new("/tmp/rivet_test_nonexistent_39481.parquet");
    assert!(validate_output(path, FormatType::Parquet, 0).is_err());
}

#[test]
fn validate_parquet_corrupt_file_errors() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(b"this is not a parquet file").unwrap();
    tmp.flush().unwrap();
    assert!(validate_output(tmp.path(), FormatType::Parquet, 0).is_err());
}

// ─── CSV validation ──────────────────────────────────────────

#[test]
fn validate_csv_correct_count_passes() {
    let batch = sample_batch(10);
    let tmp = write_csv(&[batch]);
    assert!(validate_output(tmp.path(), FormatType::Csv, 10).is_ok());
}

#[test]
fn validate_csv_wrong_count_fails() {
    let batch = sample_batch(10);
    let tmp = write_csv(&[batch]);
    let err = validate_output(tmp.path(), FormatType::Csv, 99).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("expected 99"), "got: {msg}");
}

#[test]
fn validate_csv_empty_file_zero_rows() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    writeln!(tmp, "id,name").unwrap();
    tmp.flush().unwrap();
    assert!(validate_output(tmp.path(), FormatType::Csv, 0).is_ok());
}

#[test]
fn validate_csv_nonexistent_file_errors() {
    let path = std::path::Path::new("/tmp/rivet_test_nonexistent_39481.csv");
    assert!(validate_output(path, FormatType::Csv, 0).is_err());
}

#[test]
fn validate_csv_completely_empty_file_zero_rows() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    assert!(validate_output(tmp.path(), FormatType::Csv, 0).is_ok());
}

// ─── Task 2.1 — Empty/tiny dataset coverage ──────────────────

#[test]
fn validate_parquet_zero_row_file_succeeds_for_zero_and_fails_with_clear_message() {
    // A zero-row Parquet file must validate against an expected count of 0
    // and fail against any positive expected count with an actionable message.
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(Int32Array::from(Vec::<i32>::new()))],
    )
    .unwrap();
    let fmt = ParquetFormat::new(CompressionType::None, None);
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = fmt.create_writer(&schema, Box::new(file)).unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    assert!(validate_output(tmp.path(), FormatType::Parquet, 0).is_ok());
    let err = validate_output(tmp.path(), FormatType::Parquet, 1).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("expected 1"), "got: {msg}");
    assert!(msg.contains("got 0"), "got: {msg}");
}

// ─── Validation goldens (migrated from former tests/v2_golden.rs) ──

#[test]
fn validate_parquet_three_rows_matches_expected() {
    use std::sync::Arc;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = ParquetFormat::new(CompressionType::Zstd, None)
        .create_writer(&schema, Box::new(file))
        .unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    validate_output(tmp.path(), FormatType::Parquet, 3).unwrap();
}

#[test]
fn validate_csv_two_rows_matches_expected() {
    use std::sync::Arc;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["alice", "bob"]))],
    )
    .unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = CsvFormat.create_writer(&schema, Box::new(file)).unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    validate_output(tmp.path(), FormatType::Csv, 2).unwrap();
}

#[test]
fn validate_wrong_count_fails_with_expected_message() {
    use std::sync::Arc;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = ParquetFormat::new(CompressionType::Zstd, None)
        .create_writer(&schema, Box::new(file))
        .unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    let result = validate_output(tmp.path(), FormatType::Parquet, 999);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("validation failed")
    );
}
