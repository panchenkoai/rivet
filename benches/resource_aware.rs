/// Resource-aware benchmark suite — Phase 2 stabilization.
///
/// Covers the four resource-control features added in v0.5.0:
///   1. `auto_shrink`     — recursive batch splitting throughput
///   2. compression       — write throughput per codec (none / snappy / zstd-3 / zstd-9)
///   3. row group sizing  — rows-per-group computation cost
///   4. quality cap       — hash tracking with and without `unique_max_entries`
///
/// Run all:         cargo bench --bench resource_aware
/// Run one group:   cargo bench --bench resource_aware -- auto_shrink
/// Save baseline:   cargo bench --bench resource_aware -- --save-baseline v0_5_0
/// Compare:         cargo bench --bench resource_aware -- --baseline v0_5_0
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

// ── fixtures ──────────────────────────────────────────────────────────────────

const N_ROWS: usize = 10_000;

/// Narrow numeric batch: 5 Int64 columns × N_ROWS rows.
fn narrow_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("cat", DataType::Int64, false),
        Field::new("flag", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let col = |off: i64| -> Arc<dyn Array> {
        Arc::new(Int64Array::from_iter_values(off..off + n as i64))
    };
    RecordBatch::try_new(
        schema,
        vec![col(0), col(1000), col(2000), col(3000), col(4000)],
    )
    .unwrap()
}

/// Wide text batch: 10 Utf8 columns, each value ~200 bytes, × N_ROWS rows.
fn wide_batch(n: usize) -> RecordBatch {
    let fields: Vec<Field> = (0..10)
        .map(|i| Field::new(format!("col_{i}"), DataType::Utf8, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let payload = "x".repeat(200);
    let col = || -> Arc<dyn Array> {
        Arc::new(StringArray::from(
            (0..n).map(|_| payload.as_str()).collect::<Vec<_>>(),
        ))
    };
    let cols: Vec<Arc<dyn Array>> = (0..10).map(|_| col()).collect();
    RecordBatch::try_new(schema, cols).unwrap()
}

/// High-cardinality uuid batch: 2 Utf8 UUID columns × N_ROWS rows, all distinct.
fn hc_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("uuid_col", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));
    let uuids: Vec<String> = (0..n)
        .map(|i| format!("550e8400-e29b-41d4-a716-{i:012}"))
        .collect();
    let emails: Vec<String> = (0..n)
        .map(|i| format!("user{i}@bench.example.com"))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                uuids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                emails.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

// ── §4.5 auto_shrink: recursive split throughput ──────────────────────────────

/// Returns the Arrow buffer footprint of a batch in bytes.
/// Mirrors `SourceTuning::batch_memory_bytes` (which is pub(crate)).
fn batch_bytes(batch: &RecordBatch) -> usize {
    use arrow::array::Array;
    batch
        .columns()
        .iter()
        .map(|col| col.get_array_memory_size())
        .sum()
}

/// Simulate the splitting loop: given a batch and a byte limit, count how many
/// sub-batches would result.  This mirrors `process_dest_batch`'s recursion
/// without needing a live sink (no I/O, pure CPU).
fn count_auto_shrink_splits(batch: &RecordBatch, limit_bytes: usize) -> usize {
    use arrow::array::Array;
    fn split(b: &RecordBatch, limit: usize, acc: &mut usize) {
        let bytes: usize = b.columns().iter().map(|c| c.get_array_memory_size()).sum();
        if bytes <= limit || b.num_rows() <= 1 {
            *acc += 1;
            return;
        }
        let mid = b.num_rows() / 2;
        let lo = b.slice(0, mid);
        let hi = b.slice(mid, b.num_rows() - mid);
        split(&lo, limit, acc);
        split(&hi, limit, acc);
    }
    let mut count = 0;
    split(batch, limit_bytes, &mut count);
    count
}

fn bench_auto_shrink(c: &mut Criterion) {
    let batch = wide_batch(N_ROWS);
    let actual = batch_bytes(&batch);

    let mut g = c.benchmark_group("auto_shrink");
    g.throughput(Throughput::Elements(N_ROWS as u64));

    // No split needed (limit >> batch size)
    g.bench_function("no_split_needed", |b| {
        b.iter(|| std::hint::black_box(count_auto_shrink_splits(&batch, actual * 2)))
    });

    // 50% cap → ~2 sub-batches
    g.bench_function("half_cap_2_splits", |b| {
        b.iter(|| std::hint::black_box(count_auto_shrink_splits(&batch, (actual / 2).max(1))))
    });

    // 12% cap → ~8 sub-batches
    g.bench_function("tight_cap_8_splits", |b| {
        b.iter(|| std::hint::black_box(count_auto_shrink_splits(&batch, (actual / 8).max(1))))
    });

    // 1-byte cap → splits all the way to single rows
    g.bench_function("extreme_cap_single_rows", |b| {
        let small = wide_batch(64); // fewer rows so this completes quickly
        b.iter(|| std::hint::black_box(count_auto_shrink_splits(&small, 1)))
    });

    g.finish();
}

// ── §4.3 compression: write throughput per codec ─────────────────────────────

fn write_parquet_with_codec(
    batch: &RecordBatch,
    compression: parquet::basic::Compression,
) -> Vec<u8> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    let mut buf = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
    buf
}

fn bench_compression(c: &mut Criterion) {
    use parquet::basic::{Compression, GzipLevel, ZstdLevel};

    let batch = narrow_batch(N_ROWS);
    let mut g = c.benchmark_group("compression_profiles");
    g.throughput(Throughput::Elements(N_ROWS as u64));

    g.bench_function("none", |b| {
        b.iter(|| std::hint::black_box(write_parquet_with_codec(&batch, Compression::UNCOMPRESSED)))
    });
    g.bench_function("fast_snappy", |b| {
        b.iter(|| std::hint::black_box(write_parquet_with_codec(&batch, Compression::SNAPPY)))
    });
    g.bench_function("balanced_zstd3", |b| {
        b.iter(|| {
            std::hint::black_box(write_parquet_with_codec(
                &batch,
                Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            ))
        })
    });
    g.bench_function("compact_zstd9", |b| {
        b.iter(|| {
            std::hint::black_box(write_parquet_with_codec(
                &batch,
                Compression::ZSTD(ZstdLevel::try_new(9).unwrap()),
            ))
        })
    });
    // Include gzip for comparison context (not a rivet profile, reference only)
    g.bench_function("gzip6_reference", |b| {
        b.iter(|| {
            std::hint::black_box(write_parquet_with_codec(
                &batch,
                Compression::GZIP(GzipLevel::try_new(6).unwrap()),
            ))
        })
    });

    g.finish();
}

// ── §4.4 row group sizing: computation cost ───────────────────────────────────

/// Simulate `ParquetConfig::effective_row_group_rows()` at different targets.
/// The real implementation is in src/config/models.rs; we reproduce the
/// computation here to avoid needing a live config object.
fn compute_rows_per_group(schema: &Schema, target_mb: u64) -> usize {
    let estimated_row_bytes: u64 = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Int64 | DataType::Float64 | DataType::Timestamp(_, _) => 8,
            DataType::Int32 | DataType::Float32 => 4,
            DataType::Int16 => 2,
            DataType::Boolean => 1,
            DataType::Utf8 | DataType::LargeUtf8 => 64,
            _ => 16,
        })
        .sum();
    if estimated_row_bytes == 0 {
        return 1000;
    }
    let target_bytes = target_mb * 1024 * 1024;
    ((target_bytes / estimated_row_bytes) as usize).max(1000)
}

fn bench_row_group(c: &mut Criterion) {
    let narrow_schema = narrow_batch(1).schema();
    let wide_schema = wide_batch(1).schema();

    let mut g = c.benchmark_group("row_group_computation");
    g.throughput(Throughput::Elements(1));

    for mb in [32u64, 64, 128, 256] {
        g.bench_function(format!("narrow_{mb}mb"), |b| {
            b.iter(|| std::hint::black_box(compute_rows_per_group(&narrow_schema, mb)))
        });
        g.bench_function(format!("wide_{mb}mb"), |b| {
            b.iter(|| std::hint::black_box(compute_rows_per_group(&wide_schema, mb)))
        });
    }

    g.finish();
}

// ── §4.6 quality uniqueness cap ───────────────────────────────────────────────

/// Track uniqueness for `col_idx` in `batch`, stopping at `cap` (0 = no cap).
fn track_unique_hashes(
    batch: &RecordBatch,
    col_idx: usize,
    cap: usize,
) -> (std::collections::HashSet<u64>, bool) {
    use std::io::Write as _;
    use xxhash_rust::xxh3::xxh3_64;

    let col = batch.column(col_idx);
    let fmt_opts = arrow::util::display::FormatOptions::default();
    let mut set = std::collections::HashSet::with_capacity(cap.min(batch.num_rows()));
    let mut capped = false;

    if let Ok(fmt) = arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &fmt_opts) {
        let mut scratch = Vec::with_capacity(64);
        for row in 0..col.len() {
            if cap > 0 && set.len() >= cap {
                capped = true;
                break;
            }
            scratch.clear();
            let _ = write!(scratch, "{}", fmt.value(row));
            set.insert(xxh3_64(&scratch));
        }
    }
    (set, capped)
}

fn bench_quality_cap(c: &mut Criterion) {
    let batch = hc_batch(N_ROWS);

    let mut g = c.benchmark_group("quality_uniqueness_cap");
    g.throughput(Throughput::Elements(N_ROWS as u64));

    // No cap — track all N_ROWS distinct values
    g.bench_function("no_cap_all_rows", |b| {
        b.iter(|| std::hint::black_box(track_unique_hashes(&batch, 0, 0)))
    });

    // Cap at 50% — stops halfway through
    g.bench_function("cap_50pct", |b| {
        b.iter(|| std::hint::black_box(track_unique_hashes(&batch, 0, N_ROWS / 2)))
    });

    // Cap at 10% — stops early
    g.bench_function("cap_10pct", |b| {
        b.iter(|| std::hint::black_box(track_unique_hashes(&batch, 0, N_ROWS / 10)))
    });

    // Cap at 1 — immediate stop (worst-case call overhead)
    g.bench_function("cap_1", |b| {
        b.iter(|| std::hint::black_box(track_unique_hashes(&batch, 0, 1)))
    });

    g.finish();
}

// ── registry ──────────────────────────────────────────────────────────────────

criterion_group!(
    resource_aware_benches,
    bench_auto_shrink,
    bench_compression,
    bench_row_group,
    bench_quality_cap,
);
criterion_main!(resource_aware_benches);
