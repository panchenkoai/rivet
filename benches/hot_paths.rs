/// Performance benchmarks — before/after comparison for the key hot paths
/// optimised in the performance audit.
///
/// Run:
///   cargo bench                          # all groups
///   cargo bench -- csv_write_batch       # one group
///   cargo bench -- --save-baseline before   # save snapshot to compare later
///   cargo bench -- --baseline before        # compare against saved snapshot
///
/// HTML reports land in: target/criterion/
use std::io::Write as IoWrite;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use xxhash_rust::xxh3::xxh3_128;

// ── test fixture ─────────────────────────────────────────────────────────────

const N_ROWS: usize = 10_000;

fn make_batch() -> RecordBatch {
    // 8 columns: Int64, Float64, Utf8 (plain), Utf8 (with commas/quotes), all nullable
    let n = N_ROWS;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("quoted", DataType::Utf8, true),
        Field::new("id2", DataType::Int64, true),
        Field::new("score2", DataType::Float64, false),
        Field::new("label2", DataType::Utf8, true),
        Field::new("tag", DataType::Utf8, true),
    ]));

    let ids: Vec<i64> = (0..n as i64).collect();
    let scores: Vec<f64> = (0..n).map(|i| i as f64 * 1.23).collect();
    let labels: Vec<Option<&str>> = (0..n)
        .map(|i| if i % 15 == 0 { None } else { Some("hello") })
        .collect();
    let quoted: Vec<Option<&str>> = (0..n)
        .map(|i| {
            if i % 7 == 0 {
                Some("value,with,commas")
            } else if i % 13 == 0 {
                Some(r#"has "quotes" inside"#)
            } else {
                Some("plain")
            }
        })
        .collect();
    let ids2: Vec<Option<i64>> = (0..n as i64)
        .map(|i| if i % 20 == 0 { None } else { Some(i * 2) })
        .collect();
    let scores2: Vec<f64> = (0..n).map(|i| -(i as f64)).collect();
    let labels2: Vec<Option<&str>> = (0..n)
        .map(|i| if i % 11 == 0 { None } else { Some("world") })
        .collect();
    let tags: Vec<Option<&str>> = (0..n)
        .map(|i| if i % 5 == 0 { None } else { Some("tag") })
        .collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(scores)),
            Arc::new(StringArray::from(labels)),
            Arc::new(StringArray::from(quoted)),
            Arc::new(Int64Array::from(ids2)),
            Arc::new(Float64Array::from(scores2)),
            Arc::new(StringArray::from(labels2)),
            Arc::new(StringArray::from(tags)),
        ],
    )
    .unwrap()
}

// ── CSV: before vs after ─────────────────────────────────────────────────────

/// Old: Vec::new (reallocates) + write!(buf, ",") + writeln!(buf)
/// + String::replace for quoting.
fn csv_before(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let mut first = true;
        for col_idx in 0..batch.num_columns() {
            if !first {
                write!(buf, ",").unwrap();
            }
            first = false;
            csv_write_value_before(&mut buf, batch.column(col_idx), row_idx);
        }
        writeln!(buf).unwrap();
    }
    buf
}

fn csv_write_value_before(writer: &mut dyn IoWrite, array: &dyn Array, idx: usize) {
    if array.is_null(idx) {
        return;
    }
    match array.data_type() {
        DataType::Int64 => {
            let v = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(idx);
            write!(writer, "{}", v).unwrap();
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(idx);
            write!(writer, "{}", v).unwrap();
        }
        DataType::Utf8 => {
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(idx);
            if v.contains(',') || v.contains('"') || v.contains('\n') {
                // OLD: allocates a new String on every call that hits this branch
                write!(writer, "\"{}\"", v.replace('"', "\"\"")).unwrap();
            } else {
                write!(writer, "{}", v).unwrap();
            }
        }
        _ => {}
    }
}

/// New: Vec::with_capacity + push(b',') + push(b'\n') + inline escape (no alloc).
fn csv_after(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::with_capacity(batch.num_rows() * batch.num_columns() * 8);
    for row_idx in 0..batch.num_rows() {
        for col_idx in 0..batch.num_columns() {
            if col_idx > 0 {
                buf.push(b',');
            }
            csv_write_value_after(&mut buf, batch.column(col_idx), row_idx);
        }
        buf.push(b'\n');
    }
    buf
}

fn csv_write_value_after(writer: &mut Vec<u8>, array: &dyn Array, idx: usize) {
    if array.is_null(idx) {
        return;
    }
    match array.data_type() {
        DataType::Int64 => {
            let v = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(idx);
            write!(writer, "{}", v).unwrap();
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(idx);
            write!(writer, "{}", v).unwrap();
        }
        DataType::Utf8 => {
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(idx);
            if v.contains(',') || v.contains('"') || v.contains('\n') {
                // NEW: write directly into buf, no intermediate String
                writer.extend_from_slice(b"\"");
                let mut rest = v;
                while let Some(pos) = rest.find('"') {
                    writer.extend_from_slice(rest[..pos].as_bytes());
                    writer.extend_from_slice(b"\"\"");
                    rest = &rest[pos + 1..];
                }
                writer.extend_from_slice(rest.as_bytes());
                writer.extend_from_slice(b"\"");
            } else {
                writer.extend_from_slice(v.as_bytes());
            }
        }
        _ => {}
    }
}

fn bench_csv(c: &mut Criterion) {
    let batch = make_batch();
    let mut group = c.benchmark_group("csv_write_batch");
    group.throughput(Throughput::Elements(N_ROWS as u64));
    group.bench_function("before", |b| b.iter(|| csv_before(&batch)));
    group.bench_function("after", |b| b.iter(|| csv_after(&batch)));
    group.finish();
}

// ── hash_column: before vs after ─────────────────────────────────────────────

/// Old: fresh Vec per row + array_value_to_string (allocates String per cell).
fn hash_before(batch: &RecordBatch) -> Vec<i64> {
    let n = batch.num_rows();
    let mut hashes = Vec::with_capacity(n);
    for row in 0..n {
        let mut buf = Vec::with_capacity(256); // new allocation every row
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            if array.is_null(row) {
                buf.extend_from_slice(b"\x00");
            } else {
                // allocates a String per non-null cell
                let s = arrow::util::display::array_value_to_string(array, row).unwrap_or_default();
                buf.extend_from_slice(s.as_bytes());
            }
            buf.push(b'\x1f');
        }
        hashes.push(xxh3_128(&buf) as i64);
    }
    hashes
}

/// New: ArrayFormatter created once per column, single buffer reused across rows.
fn hash_after(batch: &RecordBatch) -> Vec<i64> {
    let options = arrow::util::display::FormatOptions::default();
    let formatters: Vec<Option<arrow::util::display::ArrayFormatter>> = (0..batch.num_columns())
        .map(|i| {
            arrow::util::display::ArrayFormatter::try_new(batch.column(i).as_ref(), &options).ok()
        })
        .collect();

    let mut buf = Vec::with_capacity(256); // reused
    let n = batch.num_rows();
    let mut hashes = Vec::with_capacity(n);
    for row in 0..n {
        buf.clear();
        for (col_idx, fmt_opt) in formatters.iter().enumerate() {
            let array = batch.column(col_idx);
            if array.is_null(row) {
                buf.extend_from_slice(b"\x00");
            } else if let Some(fmt) = fmt_opt {
                // writes directly into buf, no intermediate String
                let _ = write!(buf, "{}", fmt.value(row));
            }
            buf.push(b'\x1f');
        }
        hashes.push(xxh3_128(&buf) as i64);
    }
    hashes
}

fn bench_hash(c: &mut Criterion) {
    let batch = make_batch();
    let mut group = c.benchmark_group("hash_column");
    group.throughput(Throughput::Elements(N_ROWS as u64));
    group.bench_function("before", |b| b.iter(|| hash_before(&batch)));
    group.bench_function("after", |b| b.iter(|| hash_after(&batch)));
    group.finish();
}

// ── Parquet: flush per batch vs flush only on close ──────────────────────────

fn bench_parquet(c: &mut Criterion) {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let batch = make_batch();
    let mut group = c.benchmark_group("parquet_write_batch");
    group.throughput(Throughput::Elements(N_ROWS as u64));

    group.bench_function("with_flush_per_batch", |b| {
        b.iter(|| {
            let mut buf: Vec<u8> = Vec::new();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap(); // old behaviour
            writer.close().unwrap();
            buf
        })
    });

    group.bench_function("without_flush", |b| {
        b.iter(|| {
            let mut buf: Vec<u8> = Vec::new();
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap(); // flush happens here once
            buf
        })
    });

    group.finish();
}

// ── batch column scan: DataType dispatch ─────────────────────────────────────
// Simulates the per-column type dispatch inside track_shape / quality checks.
// Real cost is the downcast + iteration, not the match itself.

fn bench_column_dispatch(c: &mut Criterion) {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;

    const ROWS: usize = 10_000;
    let int_col = Arc::new(Int64Array::from_iter_values(0..ROWS as i64)) as Arc<dyn Array>;
    let str_col = Arc::new(StringArray::from(
        (0..ROWS)
            .map(|i| {
                if i % 20 == 0 {
                    None
                } else {
                    Some("hello_world")
                }
            })
            .collect::<Vec<_>>(),
    )) as Arc<dyn Array>;

    // Inline shape-tracking scan (mirrors ExportSink::track_shape)
    fn string_max_len(col: &Arc<dyn Array>) -> u64 {
        if col.data_type() == &DataType::Utf8 {
            col.as_any()
                .downcast_ref::<StringArray>()
                .and_then(|a| a.iter().flatten().map(|s| s.len() as u64).max())
                .unwrap_or(0)
        } else {
            0
        }
    }

    // Inline null-ratio scan (mirrors quality check)
    fn null_ratio(col: &Arc<dyn Array>) -> f64 {
        let n = col.len();
        if n == 0 {
            return 0.0;
        }
        col.null_count() as f64 / n as f64
    }

    let mut group = c.benchmark_group("column_scan");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("string_max_len", |b| {
        b.iter(|| std::hint::black_box(string_max_len(&str_col)))
    });

    group.bench_function("null_ratio_int64", |b| {
        b.iter(|| std::hint::black_box(null_ratio(&int_col)))
    });

    group.bench_function("null_ratio_string", |b| {
        b.iter(|| std::hint::black_box(null_ratio(&str_col)))
    });

    group.finish();
}

// ── shape tracking: per-batch max-byte-len scan ───────────────────────────────
// Simulates ExportSink::track_shape(), which runs on every batch in the hot path.

fn bench_shape_tracking(c: &mut Criterion) {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType;

    const ROWS: usize = 10_000;

    let narrow: Vec<Option<&str>> = (0..ROWS)
        .map(|i| if i % 20 == 0 { None } else { Some("abc123") })
        .collect();
    let payload = "x".repeat(200);
    let wide: Vec<Option<&str>> = (0..ROWS)
        .map(|i| {
            if i % 50 == 0 {
                None
            } else {
                Some(payload.as_str())
            }
        })
        .collect();

    let narrow_arr = Arc::new(StringArray::from(narrow)) as Arc<dyn Array>;
    let wide_arr = Arc::new(StringArray::from(wide)) as Arc<dyn Array>;

    fn scan_max(col: &Arc<dyn Array>) -> u64 {
        if col.data_type() == &DataType::Utf8 {
            col.as_any()
                .downcast_ref::<StringArray>()
                .and_then(|a| a.iter().flatten().map(|s| s.len() as u64).max())
                .unwrap_or(0)
        } else {
            0
        }
    }

    let mut group = c.benchmark_group("shape_tracking");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("narrow_strings_6b", |b| {
        b.iter(|| std::hint::black_box(scan_max(&narrow_arr)))
    });

    group.bench_function("wide_strings_200b", |b| {
        b.iter(|| std::hint::black_box(scan_max(&wide_arr)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_csv,
    bench_hash,
    bench_parquet,
    bench_column_dispatch,
    bench_shape_tracking
);
criterion_main!(benches);
