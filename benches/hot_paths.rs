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
use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

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
                    writer.extend_from_slice(&rest.as_bytes()[..pos]);
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

// ── MySQL TIME parsing: str::parse vs atoi ───────────────────────────────────
// Mirrors parse_time_str_to_micros in src/source/mysql.rs.
// Benchmark result: str::parse wins here (atoi is 45% slower on &str paths).
// atoi advantage requires working directly on &[u8] — see mysql_int_bytes below.

fn parse_time_before(s: &str) -> Option<i64> {
    let (neg, rest) = if let Some(r) = s.strip_prefix('-') {
        (true, r)
    } else {
        (false, s)
    };
    let (hms, us_part) = if let Some(pos) = rest.find('.') {
        let us_str = &rest[pos + 1..];
        let us_digits = us_str.len().min(6);
        let us = us_str[..us_digits].parse::<i64>().ok()?;
        let scale = 10i64.pow((6 - us_digits) as u32);
        (&rest[..pos], us * scale)
    } else {
        (rest, 0i64)
    };
    let mut parts = hms.splitn(3, ':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    let total = (h * 3_600 + m * 60 + s) * 1_000_000 + us_part;
    Some(if neg { -total } else { total })
}

fn parse_time_after(s: &str) -> Option<i64> {
    let (neg, rest) = if let Some(r) = s.strip_prefix('-') {
        (true, r)
    } else {
        (false, s)
    };
    let (hms, us_part) = if let Some(pos) = rest.find('.') {
        let us_str = &rest[pos + 1..];
        let us_digits = us_str.len().min(6);
        let us = atoi::atoi::<i64>(&us_str.as_bytes()[..us_digits])?;
        let scale = 10i64.pow((6 - us_digits) as u32);
        (&rest[..pos], us * scale)
    } else {
        (rest, 0i64)
    };
    let mut parts = hms.splitn(3, ':');
    let h: i64 = atoi::atoi(parts.next()?.as_bytes())?;
    let m: i64 = atoi::atoi(parts.next()?.as_bytes())?;
    let s: i64 = atoi::atoi(parts.next()?.as_bytes())?;
    let total = (h * 3_600 + m * 60 + s) * 1_000_000 + us_part;
    Some(if neg { -total } else { total })
}

fn bench_mysql_parse_time(c: &mut Criterion) {
    // Realistic TIME strings: plain, with microseconds, negative, large hours
    let cases: Vec<&str> = vec![
        "12:30:45",
        "00:00:00",
        "08:15:30.123456",
        "-838:59:59",
        "100:00:00.000001",
        "23:59:59.999999",
        "01:02:03",
        "10:00:00.500000",
    ];
    // Repeat to get N_ROWS total calls
    let inputs: Vec<&str> = cases.iter().cycle().take(N_ROWS).copied().collect();

    let mut group = c.benchmark_group("mysql_parse_time");
    group.throughput(Throughput::Elements(N_ROWS as u64));

    group.bench_function("str_parse", |b| {
        b.iter(|| {
            inputs
                .iter()
                .map(|s| parse_time_before(s))
                .sum::<Option<i64>>()
        })
    });

    group.bench_function("atoi", |b| {
        b.iter(|| {
            inputs
                .iter()
                .map(|s| parse_time_after(s))
                .sum::<Option<i64>>()
        })
    });

    group.finish();
}

// ── MySQL integer Bytes path: bytes_to_str+str::parse vs atoi ────────────────
// Mirrors build_array Int32/Int64 Value::Bytes branch in src/source/mysql.rs.
// Hit when MySQL sends integers as text (text protocol, CHAR/VARCHAR columns).

fn int_bytes_before(bv: &[u8]) -> Option<i32> {
    std::str::from_utf8(bv).ok()?.parse::<i32>().ok()
}

fn int_bytes_after(bv: &[u8]) -> Option<i32> {
    atoi::atoi::<i32>(bv)
}

fn bench_mysql_int_bytes(c: &mut Criterion) {
    // Realistic integer strings stored as MySQL Bytes: short positives, negatives, boundaries
    let cases: Vec<&[u8]> = vec![
        b"0",
        b"1",
        b"42",
        b"-1",
        b"100",
        b"9999",
        b"-32768",
        b"32767",
        b"2147483647",
        b"-2147483648",
        b"12345",
        b"-9876",
    ];
    let inputs: Vec<&[u8]> = cases.iter().cycle().take(N_ROWS).copied().collect();

    let mut group = c.benchmark_group("mysql_int_bytes");
    group.throughput(Throughput::Elements(N_ROWS as u64));

    group.bench_function("str_parse", |b| {
        b.iter(|| {
            inputs
                .iter()
                .map(|bv| int_bytes_before(bv).map(|v| v as i64))
                .sum::<Option<i64>>()
        })
    });

    group.bench_function("atoi", |b| {
        b.iter(|| {
            inputs
                .iter()
                .map(|bv| int_bytes_after(bv).map(|v| v as i64))
                .sum::<Option<i64>>()
        })
    });

    group.finish();
}

// ── quality uniqueness: string formatter (before) vs typed hash (after) ──────
// Mirrors check_uniqueness() in src/quality.rs.
// Before: HashSet<String> built via ArrayFormatter → one String alloc per row.
// After:  HashSet<u64>   built via xxh3_64 on raw typed bytes — zero allocs.

fn uniqueness_int64_before(batch: &RecordBatch, col_idx: usize) -> usize {
    use arrow::array::Array;
    let col = batch.column(col_idx);
    let options = arrow::util::display::FormatOptions::default();
    let fmt = arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &options).unwrap();
    let mut seen = std::collections::HashSet::new();
    let mut dupes = 0usize;
    for row in 0..col.len() {
        if !col.is_null(row) {
            let s = fmt.value(row).to_string();
            if !seen.insert(s) {
                dupes += 1;
            }
        }
    }
    dupes
}

fn uniqueness_int64_after(batch: &RecordBatch, col_idx: usize) -> usize {
    use arrow::array::{Array, Int64Array};
    let col = batch.column(col_idx);
    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut dupes = 0usize;
    for row in 0..arr.len() {
        if !arr.is_null(row) {
            let h = xxh3_64(&arr.value(row).to_le_bytes());
            if !seen.insert(h) {
                dupes += 1;
            }
        }
    }
    dupes
}

fn uniqueness_string_before(batch: &RecordBatch, col_idx: usize) -> usize {
    use arrow::array::Array;
    let col = batch.column(col_idx);
    let options = arrow::util::display::FormatOptions::default();
    let fmt = arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &options).unwrap();
    let mut seen = std::collections::HashSet::new();
    let mut dupes = 0usize;
    for row in 0..col.len() {
        if !col.is_null(row) {
            let s = fmt.value(row).to_string();
            if !seen.insert(s) {
                dupes += 1;
            }
        }
    }
    dupes
}

fn uniqueness_string_after(batch: &RecordBatch, col_idx: usize) -> usize {
    use arrow::array::{Array, StringArray};
    let col = batch.column(col_idx);
    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut dupes = 0usize;
    for row in 0..arr.len() {
        if !arr.is_null(row) {
            let h = xxh3_64(arr.value(row).as_bytes());
            if !seen.insert(h) {
                dupes += 1;
            }
        }
    }
    dupes
}

fn bench_uniqueness(c: &mut Criterion) {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    const ROWS: usize = N_ROWS;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("uuid", DataType::Utf8, true),
    ]));

    let ids: Vec<Option<i64>> = (0..ROWS as i64)
        .map(|i| if i % 100 == 0 { None } else { Some(i) })
        .collect();

    let uuids: Vec<Option<String>> = (0..ROWS)
        .map(|i| {
            if i % 100 == 0 {
                None
            } else {
                Some(format!("550e8400-e29b-41d4-a716-{:012}", i))
            }
        })
        .collect();
    let uuid_refs: Vec<Option<&str>> = uuids.iter().map(|s| s.as_deref()).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(uuid_refs)),
        ],
    )
    .unwrap();

    let mut group = c.benchmark_group("quality_uniqueness");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("int64_string_formatter", |b| {
        b.iter(|| std::hint::black_box(uniqueness_int64_before(&batch, 0)))
    });

    group.bench_function("int64_typed_hash", |b| {
        b.iter(|| std::hint::black_box(uniqueness_int64_after(&batch, 0)))
    });

    group.bench_function("utf8_string_formatter", |b| {
        b.iter(|| std::hint::black_box(uniqueness_string_before(&batch, 1)))
    });

    group.bench_function("utf8_typed_hash", |b| {
        b.iter(|| std::hint::black_box(uniqueness_string_after(&batch, 1)))
    });

    group.finish();
}

// ── MySQL Utf8 text-column append: std from_utf8_lossy vs simdutf8 fast path ──
//
// The MySQL row→Arrow decode appends text columns (TEXT/LONGTEXT/VARCHAR/JSON)
// into a `StringBuilder`. The validation of the raw bytes as UTF-8 is on the
// hot path once per value; on wide-text tables (multiple ~KB text columns ×
// hundreds of thousands of rows) the scan is a real chunk of decode CPU.
//
// `before` = `String::from_utf8_lossy(b).as_ref()` (std, scalar validation) —
// today's `arrow_convert.rs:492`. `after` = simdutf8 fast path (Ok → &str)
// with a lossy fallback only for the rare invalid-UTF-8 value — what every
// other text path in the same file (and all of the PG decoder) already does.
// Byte-identical output; the only difference is the validator.
fn bench_utf8_text_append(c: &mut Criterion) {
    // Representative wide-text payload: ~4 KB valid UTF-8 per value (the
    // longtext/body shape), 10 k rows.
    const ROWS: usize = 10_000;
    let payload: Vec<u8> = (0..4096).map(|i| b"abcdefghij"[i % 10]).collect();
    let vals: Vec<Vec<u8>> = (0..ROWS).map(|_| payload.clone()).collect();

    fn build_before(vals: &[Vec<u8>]) -> StringArray {
        let mut b = StringBuilder::with_capacity(vals.len(), vals.len() * 32);
        for v in vals {
            b.append_value(String::from_utf8_lossy(v).as_ref());
        }
        b.finish()
    }
    fn build_after(vals: &[Vec<u8>]) -> StringArray {
        let mut b = StringBuilder::with_capacity(vals.len(), vals.len() * 32);
        for v in vals {
            match simdutf8::basic::from_utf8(v) {
                Ok(s) => b.append_value(s),
                Err(_) => b.append_value(String::from_utf8_lossy(v).as_ref()),
            }
        }
        b.finish()
    }

    let mut group = c.benchmark_group("mysql_utf8_text_append");
    group.throughput(Throughput::Bytes((ROWS * payload.len()) as u64));
    group.bench_function("before_std_lossy", |b| {
        b.iter(|| std::hint::black_box(build_before(&vals)))
    });
    group.bench_function("after_simdutf8", |b| {
        b.iter(|| std::hint::black_box(build_after(&vals)))
    });
    group.finish();
}

// ── CSV binary hex: per-byte write!("{:02x}") vs table + chunked write_all ───
// Mirrors the Binary arm of write_csv_value in src/format/csv.rs. The old form
// drove core::fmt through the dyn-Write vtable once per byte; the new form is a
// table lookup batched through a 1 KiB stack buffer. Byte-identical output.
fn hex_before(writer: &mut dyn IoWrite, bytes: &[u8]) {
    for byte in bytes {
        write!(writer, "{:02x}", byte).unwrap();
    }
}

fn hex_after(writer: &mut dyn IoWrite, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut chunk = [0u8; 1024];
    for slab in bytes.chunks(chunk.len() / 2) {
        let mut n = 0;
        for &b in slab {
            chunk[n] = HEX[(b >> 4) as usize];
            chunk[n + 1] = HEX[(b & 0x0f) as usize];
            n += 2;
        }
        writer.write_all(&chunk[..n]).unwrap();
    }
}

fn bench_csv_binary_hex(c: &mut Criterion) {
    // 10 k rows × a 64-byte blob (e.g. a sha-256 pair / small bytea) — the
    // per-byte cost compounds with column width.
    const ROWS: usize = 10_000;
    let blob: Vec<u8> = (0..64u32).map(|i| (i * 7 % 256) as u8).collect();
    let mut group = c.benchmark_group("csv_binary_hex");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("before_per_byte_fmt", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(ROWS * blob.len() * 2);
            for _ in 0..ROWS {
                hex_before(&mut buf, &blob);
            }
            buf
        })
    });
    group.bench_function("after_table_chunked", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(ROWS * blob.len() * 2);
            for _ in 0..ROWS {
                hex_after(&mut buf, &blob);
            }
            buf
        })
    });
    group.finish();
}

// ── CSV timestamp: dt.format(strftime-str) vs manual integer write ───────────
// Mirrors the Timestamp(µs) arm of write_csv_value. The old form re-parses the
// "%Y-%m-%dT%H:%M:%S%.6f" strftime string into format items on every value; the
// new form decomposes the DateTime and writes integers directly. Identical
// output for years 0..=9999 (the realistic range).
fn ts_before(writer: &mut dyn IoWrite, micros: i64) {
    let secs = micros / 1_000_000;
    let nsecs = ((micros % 1_000_000) * 1_000) as u32;
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
        write!(writer, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.6f")).unwrap();
    }
}

fn ts_after(writer: &mut dyn IoWrite, micros: i64) {
    use chrono::{Datelike as _, Timelike as _};
    let secs = micros / 1_000_000;
    let nsecs = ((micros % 1_000_000) * 1_000) as u32;
    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
        let y = dt.year();
        if (0..=9999).contains(&y) {
            write!(
                writer,
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                y,
                dt.month(),
                dt.day(),
                dt.hour(),
                dt.minute(),
                dt.second(),
                dt.nanosecond() / 1_000
            )
            .unwrap();
        } else {
            write!(writer, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.6f")).unwrap();
        }
    }
}

fn bench_csv_timestamp(c: &mut Criterion) {
    const ROWS: usize = 10_000;
    let base = 1_700_000_000_000_000i64;
    let inputs: Vec<i64> = (0..ROWS as i64).map(|i| base + i * 1_234_567).collect();
    let mut group = c.benchmark_group("csv_timestamp");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("before_strftime_reparse", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(ROWS * 27);
            for &m in &inputs {
                ts_before(&mut buf, m);
            }
            buf
        })
    });
    group.bench_function("after_manual_int", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(ROWS * 27);
            for &m in &inputs {
                ts_after(&mut buf, m);
            }
            buf
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_csv,
    bench_hash,
    bench_parquet,
    bench_column_dispatch,
    bench_shape_tracking,
    bench_mysql_parse_time,
    bench_mysql_int_bytes,
    bench_uniqueness,
    bench_utf8_text_append,
    bench_csv_binary_hex,
    bench_csv_timestamp,
);
criterion_main!(benches);
