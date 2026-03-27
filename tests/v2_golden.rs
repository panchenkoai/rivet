use rivet::pipeline::{generate_chunks, build_time_window_query};
use rivet::config::{Config, ExportMode, TimeColumnType};

// ─── Chunk Generation Golden Tests ───────────────────────────

#[test]
fn test_chunks_basic_range() {
    let chunks = generate_chunks(1, 1000, 300);
    assert_eq!(chunks, vec![
        (1, 300), (301, 600), (601, 900), (901, 1000)
    ]);
}

#[test]
fn test_chunks_exact_division() {
    let chunks = generate_chunks(0, 999, 500);
    assert_eq!(chunks, vec![(0, 499), (500, 999)]);
}

#[test]
fn test_chunks_single_chunk() {
    let chunks = generate_chunks(1, 50, 100000);
    assert_eq!(chunks, vec![(1, 50)]);
}

#[test]
fn test_chunks_empty_when_max_less_than_min() {
    assert!(generate_chunks(100, 50, 10).is_empty());
}

#[test]
fn test_chunks_large_range() {
    let chunks = generate_chunks(0, 999_999, 100_000);
    assert_eq!(chunks.len(), 10);
    assert_eq!(chunks[0], (0, 99_999));
    assert_eq!(chunks[9], (900_000, 999_999));
}

// ─── Time Window Query Golden Tests ──────────────────────────

#[test]
fn test_time_window_timestamp_format() {
    let q = build_time_window_query(
        "SELECT * FROM events",
        "created_at",
        TimeColumnType::Timestamp,
        7,
    );
    assert!(q.contains("_rivet WHERE created_at >= '"), "got: {}", q);
    assert!(q.contains("-"), "should have date format, got: {}", q);
}

#[test]
fn test_time_window_unix_format() {
    let q = build_time_window_query(
        "SELECT * FROM events",
        "ts",
        TimeColumnType::Unix,
        30,
    );
    assert!(q.contains("_rivet WHERE ts >= "), "got: {}", q);
    let after_gte = q.split("ts >= ").nth(1).unwrap();
    let num: i64 = after_gte.trim().parse().expect("should be a number");
    assert!(num > 1_000_000_000, "unix timestamp should be large, got: {}", num);
}

#[test]
fn test_time_window_wraps_base_query() {
    let q = build_time_window_query(
        "SELECT id, name FROM users",
        "updated_at",
        TimeColumnType::Timestamp,
        1,
    );
    assert!(q.contains("FROM (SELECT id, name FROM users) AS _rivet"), "got: {}", q);
}

// ─── Config Parsing Golden Tests ─────────────────────────────

#[test]
fn test_chunked_config_full_parse() {
    let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    query: "SELECT * FROM orders"
    mode: chunked
    chunk_column: id
    chunk_size: 50000
    parallel: 4
    format: parquet
    destination:
      type: local
      path: ./out
"#).unwrap();
    let e = &cfg.exports[0];
    assert_eq!(e.mode, ExportMode::Chunked);
    assert_eq!(e.chunk_column.as_deref(), Some("id"));
    assert_eq!(e.chunk_size, 50000);
    assert_eq!(e.parallel, 4);
}

#[test]
fn test_time_window_config_full_parse() {
    let cfg = Config::from_yaml(r#"
source:
  type: mysql
  url: "mysql://localhost/test"
exports:
  - name: events
    query: "SELECT * FROM events"
    mode: time_window
    time_column: created_at
    time_column_type: unix
    days_window: 30
    format: csv
    destination:
      type: local
      path: ./out
"#).unwrap();
    let e = &cfg.exports[0];
    assert_eq!(e.mode, ExportMode::TimeWindow);
    assert_eq!(e.time_column.as_deref(), Some("created_at"));
    assert_eq!(e.time_column_type, TimeColumnType::Unix);
    assert_eq!(e.days_window, Some(30));
}

#[test]
fn test_query_file_config_parses() {
    let cfg = Config::from_yaml(r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: custom
    query_file: sql/custom.sql
    format: parquet
    destination:
      type: local
      path: ./out
"#).unwrap();
    assert!(cfg.exports[0].query.is_none());
    assert_eq!(cfg.exports[0].query_file.as_deref(), Some("sql/custom.sql"));
}

// ─── Validate Output Golden Tests ────────────────────────────

#[test]
fn test_validate_parquet_correct_count() {
    use std::sync::Arc;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rivet::format::parquet::ParquetFormat;
    use rivet::format::Format;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = ParquetFormat.create_writer(&schema, Box::new(file)).unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    rivet::pipeline::validate_output(
        tmp.path(),
        rivet::config::FormatType::Parquet,
        3,
    ).unwrap();
}

#[test]
fn test_validate_csv_correct_count() {
    use std::sync::Arc;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rivet::format::csv::CsvFormat;
    use rivet::format::Format;

    let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["alice", "bob"]))],
    ).unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = CsvFormat.create_writer(&schema, Box::new(file)).unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    rivet::pipeline::validate_output(
        tmp.path(),
        rivet::config::FormatType::Csv,
        2,
    ).unwrap();
}

#[test]
fn test_validate_wrong_count_fails() {
    use std::sync::Arc;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rivet::format::parquet::ParquetFormat;
    use rivet::format::Format;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    let mut writer = ParquetFormat.create_writer(&schema, Box::new(file)).unwrap();
    writer.write_batch(&batch).unwrap();
    writer.finish().unwrap();

    let result = rivet::pipeline::validate_output(
        tmp.path(),
        rivet::config::FormatType::Parquet,
        999,
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("validation failed"));
}

// ─── Resource Monitoring ─────────────────────────────────────

#[test]
fn test_resource_get_rss_returns_nonzero() {
    let rss = rivet::resource::get_rss_mb();
    assert!(rss > 0, "RSS should be > 0 on macOS/Linux, got: {}", rss);
}

#[test]
fn test_resource_check_memory_disabled() {
    assert!(rivet::resource::check_memory(0));
}

#[test]
fn test_resource_check_memory_high_threshold() {
    assert!(rivet::resource::check_memory(100_000));
}
