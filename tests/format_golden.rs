use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use rivet::format::csv::CsvFormat;
use rivet::config::CompressionType;
use rivet::format::parquet::ParquetFormat;
use rivet::format::Format;

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

fn write_to_vec(
    format: &dyn Format,
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Vec<u8> {
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, false),
    ]));

    let values = StringArray::from(vec![
        "simple",
        "has,comma",
        "has\"quote",
        "has\nnewline",
    ]);
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
    ]));

    let micros = 1_700_000_000_000_000i64;
    let array = TimestampMicrosecondArray::from(vec![micros]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[0], "ts");
    assert!(lines[1].starts_with("2023-11-14T22:13:20"), "got: {}", lines[1]);
}

#[test]
fn test_csv_date_format() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Date32, false),
    ]));

    let days = 19723i32;
    let array = Date32Array::from(vec![days]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let buf = write_to_vec(&CsvFormat, &schema, &[batch]);
    let output = String::from_utf8(buf).unwrap();

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines[1], "2024-01-01");
}

// ─── Parquet Round-trip Tests ────────────────────────────────

#[test]
fn test_parquet_roundtrip() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(&ParquetFormat::new(CompressionType::Zstd, None), &schema, &[batch]);
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
    let buf = write_to_vec(&ParquetFormat::new(CompressionType::Zstd, None), &schema, &[batch]);
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);
    let col_meta = row_group.column(0);

    assert!(
        matches!(col_meta.compression(), parquet::basic::Compression::ZSTD(_)),
        "expected ZSTD, got: {:?}", col_meta.compression()
    );
}

#[test]
fn test_parquet_compression_snappy() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(&ParquetFormat::new(CompressionType::Snappy, None), &schema, &[batch]);
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let col_meta = reader.metadata().row_group(0).column(0);
    assert_eq!(col_meta.compression(), parquet::basic::Compression::SNAPPY);
}

#[test]
fn test_parquet_compression_none() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(&ParquetFormat::new(CompressionType::None, None), &schema, &[batch]);
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let col_meta = reader.metadata().row_group(0).column(0);
    assert_eq!(col_meta.compression(), parquet::basic::Compression::UNCOMPRESSED);
}

#[test]
fn test_parquet_compression_gzip() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(&ParquetFormat::new(CompressionType::Gzip, Some(6)), &schema, &[batch]);
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let col_meta = reader.metadata().row_group(0).column(0);
    assert!(
        matches!(col_meta.compression(), parquet::basic::Compression::GZIP(_)),
        "expected GZIP, got: {:?}", col_meta.compression()
    );
}

#[test]
fn test_parquet_compression_lz4() {
    let (schema, batch) = make_basic_batch();
    let buf = write_to_vec(&ParquetFormat::new(CompressionType::Lz4, None), &schema, &[batch]);
    let data = bytes::Bytes::from(buf);

    use parquet::file::reader::FileReader;
    let reader = parquet::file::reader::SerializedFileReader::new(data).unwrap();
    let col_meta = reader.metadata().row_group(0).column(0);
    assert_eq!(col_meta.compression(), parquet::basic::Compression::LZ4);
}
