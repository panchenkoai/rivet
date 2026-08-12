/// One-shot vs streaming upload: the CPU/RAM cost of the two branches the
/// per-destination `oneshot_budget_mb` toggle selects (cloud.rs `write`).
///
/// The budget does not change the cost of either branch — it changes WHICH
/// parts take WHICH branch.  Halving it (64 → 32) moves more parts onto the
/// streaming branch (flat RAM, multipart, no Content-MD5); doubling it (→ 128)
/// lets more parts one-shot (single PUT, Content-MD5, buffer = part size).
///
/// This bench measures each branch over OpenDAL's in-memory service (offline —
/// no network), so the numbers reflect the local CPU + copy cost only.  Real
/// network latency/throughput is not representable here; the decision matrix
/// (which parts one-shot at budget 32/64/128) is asserted deterministically by
/// `destination::cloud::tests::configured_budget_moves_the_oneshot_switch_point`.
///
/// Run:
///   cargo bench -- upload_budget
use std::io::Write as IoWrite;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use opendal::blocking;

fn temp_file_of(size_mb: usize) -> tempfile::NamedTempFile {
    // Repeat a small pattern so allocating the file is cheap regardless of size.
    let mut tmp = tempfile::NamedTempFile::new().expect("temp file");
    let chunk = b"0123456789abcdef";
    let chunk = chunk.repeat(64); // 1 KiB
    let mut remaining = size_mb * 1024 * 1024;
    while remaining >= chunk.len() {
        tmp.write_all(&chunk).expect("write chunk");
        remaining -= chunk.len();
    }
    if remaining > 0 {
        tmp.write_all(&chunk[..remaining]).expect("write tail");
    }
    tmp.flush().expect("flush");
    tmp
}

fn blocking_operator() -> (blocking::Operator, tokio::runtime::Runtime) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let _guard = runtime.enter();
    let svc = opendal::services::Memory::default();
    let async_op = opendal::Operator::new(svc)
        .expect("async operator")
        .finish();
    let op = blocking::Operator::new(async_op).expect("operator");
    (op, runtime)
}

/// The one-shot branch: read the whole part into RAM, single PUT.
fn bench_oneshot_write(c: &mut Criterion) {
    let (op, _rt) = blocking_operator();
    for size_mb in [8usize, 64, 256] {
        let tmp = temp_file_of(size_mb);
        let mut group = c.benchmark_group(format!("upload_oneshot_write_{size_mb}mb"));
        group.throughput(Throughput::Bytes((size_mb * 1024 * 1024) as u64));
        group.bench_function("read_all_then_put", |b| {
            b.iter(|| {
                let body = std::fs::read(tmp.path()).expect("read");
                op.write("bench/key.parquet", body).expect("put");
            })
        });
        group.finish();
    }
}

/// The streaming branch: bounded-buffer copy from the file into the writer.
fn bench_streaming_write(c: &mut Criterion) {
    let (op, _rt) = blocking_operator();
    for size_mb in [8usize, 64, 256] {
        let tmp = temp_file_of(size_mb);
        let mut group = c.benchmark_group(format!("upload_streaming_write_{size_mb}mb"));
        group.throughput(Throughput::Bytes((size_mb * 1024 * 1024) as u64));
        group.bench_function("copy_to_writer", |b| {
            b.iter(|| {
                let mut src = std::fs::File::open(tmp.path()).expect("open");
                let mut dst = op
                    .writer("bench/key.parquet")
                    .expect("writer")
                    .into_std_write();
                std::io::copy(&mut src, &mut dst).expect("copy");
                dst.close().expect("close");
            })
        });
        group.finish();
    }
}

criterion_group!(
    upload_budget_benches,
    bench_oneshot_write,
    bench_streaming_write,
);
criterion_main!(upload_budget_benches);
