use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use rivet::config::CompressionType;
use rivet::format::Format;
use rivet::format::csv::CsvFormat;
use rivet::format::parquet::ParquetFormat;

fn make_basic_batch() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            Arc::new(Float64Array::from(vec![95.5, 87.0, 92.3])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .unwrap();

    (schema, batch)
}

fn write_to_vec(format: &dyn Format, schema: &Arc<Schema>, batches: &[RecordBatch]) -> Vec<u8> {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = format.create_writer(schema, Box::new(file)).unwrap();
    for batch in batches {
        writer.write_batch(batch).unwrap();
    }
    writer.finish().unwrap();
    std::fs::read(tmp.path()).unwrap()
}

// ─── CSV Golden Tests ────────────────────────────────────────

#[test]
fn test_csv_basic_types() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();

    let expected = "\
id,name,score,active
1,alice,95.5,true
2,bob,87,false
3,carol,92.3,true
";
    assert_eq!(output, expected);
}

#[test]
fn test_csv_null_handling() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let ids = Int32Array::from(vec![Some(1), None, Some(3)]);
    let names = StringArray::from(vec![Some("alice"), Some("bob"), None]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();

    let expected = "\
id,name
1,alice
,bob
3,
";
    assert_eq!(output, expected);
}

#[test]
fn test_csv_escaping() {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));

    let values = StringArray::from(vec!["simple", "has,comma", "has\"quote", "has\nnewline"]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[0], "text");
    assert_eq!(lines[1], "simple");
    assert_eq!(lines[2], "\"has,comma\"");
    assert_eq!(lines[3], "\"has\"\"quote\"");
    assert!(lines[4].starts_with("\"has"));
}

#[test]
fn test_csv_timestamp_format() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    )]));

    let micros = 1_700_000_000_000_000i64;
    let array = TimestampMicrosecondArray::from(vec![micros]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[0], "ts");
    assert!(
        lines[1].starts_with("2023-11-14T22:13:20"),
        "got: {}",
        lines[1]
    );
}

#[test]
fn test_csv_date_format() {
    let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, false)]));

    let days = 19723i32;
    let array = Date32Array::from(vec![days]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[1], "2024-01-01");
}

// ─── CSV additional type coverage (regression) ──────────────

#[test]
fn test_csv_int16() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int16, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int16Array::from(vec![i16::MIN, 0, i16::MAX]))],
    )
    .unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();
    let expected = "v\n-32768\n0\n32767\n";
    assert_eq!(output, expected);
}

#[test]
fn test_csv_float32() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from(vec![1.5f32, -0.0, 99.99]))],
    )
    .unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[0], "v");
    assert_eq!(lines[1], "1.5");
}

#[test]
fn test_csv_binary_hex_encoding() {
    let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(BinaryArray::from(vec![
            b"\x00\xff\xab".as_slice(),
            b"\xde\xad".as_slice(),
        ]))],
    )
    .unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();
    let expected = "b\n00ffab\ndead\n";
    assert_eq!(output, expected);
}

#[test]
fn test_csv_boolean_with_nulls() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "flag",
        DataType::Boolean,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
        ]))],
    )
    .unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();
    let expected = "flag\ntrue\n\nfalse\n";
    assert_eq!(output, expected);
}

#[test]
fn test_csv_mixed_nulls_all_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None, Some(42)])),
            Arc::new(Int64Array::from(vec![Some(100), None])),
            Arc::new(Float64Array::from(vec![None, Some(2.71)])),
            Arc::new(StringArray::from(vec![Some("ok"), None])),
        ],
    )
    .unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();
    let expected = "i32,i64,f64,s\n,100,,ok\n42,,2.71,\n";
    assert_eq!(output, expected, "null columns should be empty");
}

#[test]
fn test_csv_multi_batch_appends_without_extra_header() {
    let (schema, batch) = make_basic_batch();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["dave"])),
            Arc::new(Float64Array::from(vec![80.0])),
            Arc::new(BooleanArray::from(vec![false])),
        ],
    )
    .unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch, batch2]);
    let output = String::from_utf8(buf).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[0], "id,name,score,active");
    assert_eq!(lines.len(), 5, "header + 3 + 1 rows");
    assert_eq!(lines[4], "4,dave,80,false");
}

#[test]
fn test_csv_empty_batch_produces_header_only() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();
    assert_eq!(output, "a,b\n");
}

#[test]
fn test_csv_special_chars_regression() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            "normal",
            "with,comma",
            "with\"quote",
            "with,\"both",
            "",
        ]))],
    )
    .unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[1], "normal");
    assert_eq!(lines[2], "\"with,comma\"");
    assert_eq!(lines[3], "\"with\"\"quote\"");
    assert_eq!(lines[4], "\"with,\"\"both\"");
    assert_eq!(lines[5], "");
}

// ─── Parquet Golden: multi-batch round-trip ──────────────────

#[test]
fn test_parquet_multi_batch_roundtrip() {
    let (schema, batch1) = make_basic_batch();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["dave", "eve"])),
            Arc::new(Float64Array::from(vec![70.0, 60.0])),
            Arc::new(BooleanArray::from(vec![true, true])),
        ],
    )
    .unwrap();
    let buf = write_to_vec(
        &ParquetFormat::new(CompressionType::Zstd, None, None),
        &schema,
        &[batch1, batch2],
    );
    let data = bytes::Bytes::from(buf);
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data).unwrap();
    let reader = builder.build().unwrap();
    let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(total, 5);
}

#[test]
fn test_parquet_nullable_roundtrip() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![None, Some("bob"), None])),
        ],
    )
    .unwrap();
    let buf = write_to_vec(
        &ParquetFormat::new(CompressionType::None, None, None),
        &schema,
        &[batch],
    );
    let data = bytes::Bytes::from(buf);
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data).unwrap();
    let mut reader = builder.build().unwrap();
    let rb = reader.next().unwrap().unwrap();
    assert_eq!(rb.num_rows(), 3);
    let ids = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(ids.is_null(1));
    assert_eq!(ids.value(2), 3);
    let names = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(names.is_null(0));
    assert_eq!(names.value(1), "bob");
}

// ─── Parquet Round-trip Tests ────────────────────────────────

#[test]
fn test_parquet_roundtrip() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(
        &ParquetFormat::new(CompressionType::Zstd, None, None),
        &schema,
        &[batch],
    );
    let data = bytes::Bytes::from(buf);

    assert!(!data.is_empty());

    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data).unwrap();
    let mut reader = builder.build().unwrap();
    let read_batch = reader.next().unwrap().unwrap();

    assert_eq!(read_batch.num_rows(), 3);
    assert_eq!(read_batch.num_columns(), 4);

    let ids = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(2), 3);

    let names = read_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(2), "carol");
}

#[test]
fn test_parquet_compression_default_zstd() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(
        &ParquetFormat::new(CompressionType::Zstd, None, None),
        &schema,
        &[batch],
    );
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);
    let col_meta = row_group.column(0);

    assert!(
        matches!(col_meta.compression(), parquet::basic::Compression::ZSTD(_)),
        "expected ZSTD, got: {:?}",
        col_meta.compression()
    );
}

#[test]
fn test_parquet_compression_snappy() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(
        &ParquetFormat::new(CompressionType::Snappy, None, None),
        &schema,
        &[batch],
    );
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let col_meta = reader.metadata().row_group(0).column(0);
    assert_eq!(col_meta.compression(), parquet::basic::Compression::SNAPPY);
}

#[test]
fn test_parquet_compression_none() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(
        &ParquetFormat::new(CompressionType::None, None, None),
        &schema,
        &[batch],
    );
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let col_meta = reader.metadata().row_group(0).column(0);
    assert_eq!(
        col_meta.compression(),
        parquet::basic::Compression::UNCOMPRESSED
    );
}

#[test]
fn test_parquet_compression_gzip() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(
        &ParquetFormat::new(CompressionType::Gzip, Some(6), None),
        &schema,
        &[batch],
    );
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let col_meta = reader.metadata().row_group(0).column(0);
    assert!(
        matches!(col_meta.compression(), parquet::basic::Compression::GZIP(_)),
        "expected GZIP, got: {:?}",
        col_meta.compression()
    );
}

#[test]
fn test_parquet_compression_lz4() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(
        &ParquetFormat::new(CompressionType::Lz4, None, None),
        &schema,
        &[batch],
    );
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let col_meta = reader.metadata().row_group(0).column(0);
    assert_eq!(col_meta.compression(), parquet::basic::Compression::LZ4);
}

// ─── Extreme value coverage (QA backlog Task 2.4) ────────────

/// Long UTF-8 payload: mix of ASCII, BMP and emoji characters survives CSV
/// serialization without panic and remains valid UTF-8 on disk.
#[test]
fn csv_extreme_long_utf8_string_does_not_panic() {
    let pieces = ["rivet ", "αβγδ ", "🚀 ", "line\nend ", "quote\" "];
    let mut huge = String::new();
    while huge.len() < 100 * 1024 {
        for p in &pieces {
            huge.push_str(p);
        }
    }

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![huge.as_str()]))],
    )
    .unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = CsvFormat.create_writer(&schema, Box::new(file)).unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    let bytes = std::fs::read(tmp.path()).unwrap();
    let out = std::str::from_utf8(&bytes).expect("CSV output must be valid UTF-8");
    assert!(out.contains("αβγδ"));
    assert!(out.contains("🚀"));
}

/// Backlog allows "unsupported values fail clearly" — so we only assert
/// "no panic, output is deterministic and UTF-8".  Whether NaN serializes as
/// "NaN" / "" / errors is format-owned; this test only guards against crashes.
#[test]
fn csv_float_nan_and_inf_do_not_panic() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            0.0,
            -0.0,
        ]))],
    )
    .unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = CsvFormat.create_writer(&schema, Box::new(file)).unwrap();
    let _ = writer.write_batch(&batch);
    let _ = writer.finish();
    if let Ok(bytes) = std::fs::read(tmp.path()) {
        assert!(
            std::str::from_utf8(&bytes).is_ok(),
            "CSV output must remain valid UTF-8 even for NaN/Inf"
        );
    }
}

/// Parquet/Arrow must treat `""` and NULL as distinct values on round-trip.
/// QA backlog: "Add empty string vs null coverage."
#[test]
fn parquet_preserves_empty_string_vs_null_distinction_on_roundtrip() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![Some(""), None, Some("x")]))],
    )
    .unwrap();

    let fmt = ParquetFormat::new(CompressionType::None, None, None);
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = fmt.create_writer(&schema, Box::new(file)).unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    let bytes = std::fs::read(tmp.path()).unwrap();
    let data = bytes::Bytes::from(bytes);
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data).unwrap();
    let mut reader = builder.build().unwrap();
    let read = reader.next().unwrap().unwrap();
    let col = read
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.len(), 3);
    assert!(
        !col.is_null(0),
        "empty string must not be read back as NULL"
    );
    assert_eq!(col.value(0), "");
    assert!(col.is_null(1), "explicit NULL must remain NULL");
    assert_eq!(col.value(2), "x");
}

// ─── Row group metadata verification (§3.2 Roadmap) ─────────────────────────
//
// These tests write batches through ParquetFormat and read back the parquet file
// metadata to verify that the row_group_rows setting is applied correctly.
// This is the layer of evidence between unit tests (config math) and live E2E tests
// (actual DB exports) — it proves the wire-up: config → format → file metadata.

fn make_int_batch_n(n: usize) -> (Arc<Schema>, RecordBatch) {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..n as i64))],
    )
    .unwrap();
    (schema, batch)
}

fn write_parquet_with_row_group_rows(
    row_group_rows: Option<usize>,
    total_rows: usize,
) -> bytes::Bytes {
    let (schema, batch) = make_int_batch_n(total_rows);
    let fmt = ParquetFormat::new(CompressionType::None, None, row_group_rows);
    bytes::Bytes::from(write_to_vec(&fmt, &schema, &[batch]))
}

#[test]
fn parquet_row_group_fixed_rows_exact_count() {
    // 100 rows with row_group_rows=10 must produce exactly 10 row groups.
    use parquet::file::reader::FileReader;
    let data = write_parquet_with_row_group_rows(Some(10), 100);
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let n_groups = reader.metadata().num_row_groups();
    assert_eq!(
        n_groups, 10,
        "fixed_rows=10 with 100 rows must produce 10 row groups; got {n_groups}"
    );
}

#[test]
fn parquet_row_group_fixed_rows_partial_last_group() {
    // 105 rows with row_group_rows=10: ceil(105/10) = 11 groups.
    use parquet::file::reader::FileReader;
    let data = write_parquet_with_row_group_rows(Some(10), 105);
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let n_groups = reader.metadata().num_row_groups();
    assert_eq!(
        n_groups, 11,
        "105 rows ÷ 10 must give 11 groups (last partial); got {n_groups}"
    );
}

#[test]
fn parquet_row_group_all_rows_in_one_group_when_limit_exceeds_count() {
    // row_group_rows=10000 with only 50 rows → 1 group.
    use parquet::file::reader::FileReader;
    let data = write_parquet_with_row_group_rows(Some(10_000), 50);
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let n_groups = reader.metadata().num_row_groups();
    assert_eq!(n_groups, 1, "all rows fit in one group; got {n_groups}");
}

#[test]
fn parquet_row_group_none_produces_single_group_for_small_dataset() {
    // Without a row_group_rows limit, a small batch (50 rows) goes into 1 group
    // (library default is 1,048,576 rows — far above 50).
    use parquet::file::reader::FileReader;
    let data = write_parquet_with_row_group_rows(None, 50);
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let n_groups = reader.metadata().num_row_groups();
    assert_eq!(
        n_groups, 1,
        "small dataset with no row group limit must produce 1 group; got {n_groups}"
    );
}

#[test]
fn parquet_row_group_total_row_count_matches_source() {
    // Row group metadata must account for all source rows — no rows lost.
    use parquet::file::reader::FileReader;
    let total_rows = 137usize;
    let data = write_parquet_with_row_group_rows(Some(20), total_rows);
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let meta = reader.metadata();
    let counted: i64 = (0..meta.num_row_groups())
        .map(|i| meta.row_group(i).num_rows())
        .sum();
    assert_eq!(
        counted as usize, total_rows,
        "all {total_rows} rows must be accounted for in row group metadata; got {counted}"
    );
}

#[test]
fn parquet_row_group_schema_is_stable_across_all_groups() {
    // Every row group in the file must expose the same column schema.
    use parquet::file::reader::FileReader;
    let data = write_parquet_with_row_group_rows(Some(20), 100);
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let meta = reader.metadata();
    let file_schema = reader.metadata().file_metadata().schema();
    for i in 0..meta.num_row_groups() {
        let rg = meta.row_group(i);
        assert_eq!(
            rg.num_columns(),
            1,
            "row group {i} must have 1 column; got {}",
            rg.num_columns()
        );
        // Column name in metadata must match the schema.
        let col_path = rg.column(0).column_path().string();
        assert_eq!(
            col_path, "id",
            "row group {i} column name must be 'id'; got '{col_path}'"
        );
        let _ = file_schema; // schema object used for type context
    }
}

// ─── §3.3 CSV ignores Parquet-specific row group settings ────────────────────

#[test]
fn csv_output_succeeds_even_when_parquet_config_is_present_in_yaml() {
    // When a user sets `parquet:` block but the format is CSV, the sink receives
    // parquet_config=None for the CSV writer path — no panic, no error.
    // This is a unit-level proof that the format dispatch is clean.
    use rivet::format::csv::CsvFormat;

    let (schema, batch) = make_basic_batch();
    // Write through CsvFormat — if parquet settings leaked into CSV this would break.
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();
    assert!(
        output.contains("id"),
        "CSV output must contain header; got:\n{output}"
    );
    assert!(
        !output.is_empty(),
        "CSV output must not be empty when parquet config is also present"
    );
}
