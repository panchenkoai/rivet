//! Fuzz / smoke tests for output-format serialization.
//!
//! QA backlog Task 4A.3.  Exercise the CSV and Parquet writers with a
//! deterministic set of adversarial `RecordBatch` inputs and assert that
//! *no input causes a panic*.  Whether a given value "serialises cleanly"
//! or "returns an Err" is format-owned and not re-asserted here — the
//! acceptance criterion for this task is "serialization code does not panic
//! on malformed or extreme inputs".
//!
//! A proper `cargo-fuzz` libfuzzer target (with corpus + minimized
//! reproducers) is a planned follow-up (QA backlog Task 4A.5).  This file
//! is the fast deterministic subset runnable inside `cargo test`.

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use rivet::config::CompressionType;
use rivet::format::Format;
use rivet::format::csv::CsvFormat;
use rivet::format::parquet::ParquetFormat;

/// Write a batch to a temp file through the given writer factory; return the
/// path for downstream assertions.  Any writer error is swallowed — the test
/// only cares that no panic escaped.
fn try_write_to_tempfile<F: Format>(
    format: &F,
    schema: &Arc<Schema>,
    batch: &RecordBatch,
) -> tempfile::NamedTempFile {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let file = tmp.as_file().try_clone().unwrap();
    if let Ok(mut writer) = format.create_writer(schema, Box::new(file)) {
        let _ = writer.write_batch(batch);
        let _ = writer.finish();
    }
    tmp
}

// ─── Adversarial Utf8 batches ───────────────────────────────

fn utf8_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]))
}

/// Representative set of "nasty" UTF-8 payloads a real source could emit.
fn adversarial_strings() -> Vec<Option<String>> {
    vec![
        Some(String::new()),                                // empty
        None,                                               // NULL
        Some(" ".into()),                                   // whitespace
        Some("\n".into()),                                  // bare newline
        Some("\r\n".into()),                                // CRLF
        Some("\"".into()),                                  // bare double-quote
        Some(",".into()),                                   // CSV separator
        Some("a,b,c\nd,e,f".into()),                        // CSV-shaped content
        Some("\"nested\"\"quotes\"".into()),                // pre-quoted CSV
        Some("\t".into()),                                  // tab
        Some("\u{0001}\u{0002}\u{0003}".into()),            // low control chars
        Some("🚀 привіт 正體中文".into()),                  // multi-script unicode
        Some("líne1\nlíne2\nlíne3".into()),                 // multiline accented
        Some("a".repeat(64 * 1024)),                        // 64 KiB single cell
        Some("\u{FEFF}BOM-led string".into()),              // UTF-8 BOM at start
        Some("trailing\0null".into()),                      // embedded NUL
        Some(String::from_utf8(vec![0xC2, 0xA0]).unwrap()), // NBSP
    ]
}

#[test]
fn csv_writer_does_not_panic_on_adversarial_utf8_cells() {
    let schema = utf8_schema();
    for v in adversarial_strings() {
        let col = StringArray::from(vec![v]);
        let Ok(batch) = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]) else {
            continue;
        };
        let _tmp = try_write_to_tempfile(&CsvFormat, &schema, &batch);
    }
}

#[test]
fn parquet_writer_does_not_panic_on_adversarial_utf8_cells() {
    let schema = utf8_schema();
    for v in adversarial_strings() {
        let col = StringArray::from(vec![v]);
        let Ok(batch) = RecordBatch::try_new(schema.clone(), vec![Arc::new(col)]) else {
            continue;
        };
        let parquet = ParquetFormat::new(CompressionType::None, None);
        let _tmp = try_write_to_tempfile(&parquet, &schema, &batch);
    }
}

// ─── Extreme numeric batches ─────────────────────────────────

#[test]
fn csv_writer_handles_numeric_extremes_without_panic() {
    // Union of int/float corner cases: min/max, zero, negative-zero, NaN, Inf.
    let schema = Arc::new(Schema::new(vec![
        Field::new("i8", DataType::Int8, false),
        Field::new("i64", DataType::Int64, false),
        Field::new("u64", DataType::UInt64, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int8Array::from(vec![i8::MIN, -1, 0, 1, i8::MAX])),
            Arc::new(Int64Array::from(vec![i64::MIN, -1, 0, 1, i64::MAX])),
            Arc::new(UInt64Array::from(vec![
                0u64,
                1,
                u64::MAX / 2,
                u64::MAX - 1,
                u64::MAX,
            ])),
            Arc::new(Float32Array::from(vec![
                f32::MIN,
                -0.0,
                0.0,
                f32::NAN,
                f32::MAX,
            ])),
            Arc::new(Float64Array::from(vec![
                f64::NEG_INFINITY,
                -0.0,
                0.0,
                f64::NAN,
                f64::INFINITY,
            ])),
        ],
    )
    .unwrap();

    let _tmp = try_write_to_tempfile(&CsvFormat, &schema, &batch);
}

#[test]
fn parquet_writer_handles_numeric_extremes_without_panic() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i64", DataType::Int64, true),
        Field::new("f64", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(0),
                None,
                Some(i64::MAX),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(f64::NEG_INFINITY),
                Some(f64::NAN),
                None,
                Some(-0.0),
                Some(f64::INFINITY),
            ])),
        ],
    )
    .unwrap();

    let parquet = ParquetFormat::new(CompressionType::Zstd, None);
    let _tmp = try_write_to_tempfile(&parquet, &schema, &batch);
}

// ─── Timestamp / date boundary batches ───────────────────────

#[test]
fn csv_writer_handles_timestamp_and_date_extremes_without_panic() {
    // Timestamps at/near chrono's representable range plus typical values.
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts_us",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("d", DataType::Date32, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(i64::MIN / 2), // very old
                Some(0),            // unix epoch
                None,
                Some(i64::MAX / 2), // very far future
            ])),
            Arc::new(Date32Array::from(vec![
                Some(i32::MIN / 4),
                Some(0), // 1970-01-01
                None,
                Some(i32::MAX / 4),
            ])),
        ],
    )
    .unwrap();

    let _tmp = try_write_to_tempfile(&CsvFormat, &schema, &batch);
}

// ─── Empty, all-null, and wide-schema batches ────────────────

#[test]
fn csv_and_parquet_writers_accept_zero_row_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
        ],
    )
    .unwrap();

    let _csv = try_write_to_tempfile(&CsvFormat, &schema, &batch);
    let _pq = try_write_to_tempfile(
        &ParquetFormat::new(CompressionType::Snappy, None),
        &schema,
        &batch,
    );
}

#[test]
fn csv_writer_handles_all_null_column_without_panic() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let values: Vec<Option<&str>> = vec![None; 100];
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))]).unwrap();
    let _tmp = try_write_to_tempfile(&CsvFormat, &schema, &batch);
}

#[test]
fn csv_writer_handles_very_wide_schema_without_panic() {
    // 256 columns — stresses any per-column buffer allocation path.
    let fields: Vec<Field> = (0..256)
        .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let columns: Vec<ArrayRef> = (0..256i32)
        .map(|i| Arc::new(Int32Array::from(vec![i])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let _tmp = try_write_to_tempfile(&CsvFormat, &schema, &batch);
}

// ─── Binary column smoke ─────────────────────────────────────

#[test]
fn csv_writer_handles_binary_cells_with_extreme_bytes_without_panic() {
    let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
    let rows: Vec<Option<Vec<u8>>> = vec![
        Some(vec![]),
        None,
        Some(vec![0x00, 0xFF, 0xAB]),
        Some((0u8..=255u8).collect()),
        Some(vec![0xFF; 16 * 1024]),
    ];
    let arr = BinaryArray::from_iter(rows.iter().map(|r| r.as_deref()));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
    let _tmp = try_write_to_tempfile(&CsvFormat, &schema, &batch);
}
