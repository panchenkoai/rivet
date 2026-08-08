/// Sequential vs bounded-concurrent part upload — the wall-time the
/// `exports[].upload_parallelism` knob reclaims when the per-part network
/// round-trip dominates the cost.
///
/// The destination here is a latency-bound stub: each `write()` sleeps a fixed
/// `latency` (one bounded network round-trip) and touches the local file, so
/// the transfer itself is free. That models an SSH tunnel / cross-region VPN /
/// high-BDP WAN where the sequential loop pays the latency N times in series
/// and concurrent uploads overlap it. The sequential arm IS the pre-feature
/// code path; the parallel arm reproduces `pipeline::single`'s scope+Semaphore
/// loop shape (the real commit seam is what the unit tests drive).
///
/// Run:
///   cargo bench -- concurrent_upload
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use rivet::destination_for_tests::{Destination, WriteOutcome};
use rivet::error::Result;
use rivet::resource::Semaphore;

/// Latency-dominated destination: sleeps `latency_ms` per write (a bounded
/// network round-trip), then reads the file so the "transfer" does some work.
struct LatencyDest {
    latency_ms: u64,
    written: AtomicUsize,
}

impl Destination for LatencyDest {
    fn write(&self, local_path: &Path, _remote_key: &str) -> Result<WriteOutcome> {
        std::thread::sleep(Duration::from_millis(self.latency_ms));
        let _ = std::fs::metadata(local_path);
        self.written.fetch_add(1, Ordering::Relaxed);
        Ok(WriteOutcome::opaque())
    }
    fn capabilities(&self) -> rivet::destination_for_tests::DestinationCapabilities {
        rivet::destination_for_tests::DestinationCapabilities {
            commit_protocol: rivet::destination_for_tests::WriteCommitProtocol::FinalizeOnClose,
            idempotent_overwrite: true,
            retry_safe: true,
            partial_write_risk: false,
        }
    }
}

fn staged_parts(dir: &std::path::Path, n: usize) -> Vec<std::path::PathBuf> {
    (0..n)
        .map(|i| {
            let p = dir.join(format!("part{i}.parquet"));
            std::fs::write(&p, vec![0xAAu8; 1024]).expect("stage part");
            p
        })
        .collect()
}

/// The PRE-feature upload loop: strictly sequential, one network round-trip
/// per part. This is `upload_parallelism: 1`.
fn upload_sequential(dest: &dyn Destination, parts: &[std::path::PathBuf]) {
    for p in parts {
        dest.write(p, "key").expect("write");
    }
}

/// The feature loop: bounded concurrent uploads (scope + Semaphore), one worker
/// per part. This is `upload_parallelism: N`.
fn upload_parallel(dest: &dyn Destination, parts: &[std::path::PathBuf], parallelism: usize) {
    let semaphore = Semaphore::new(parallelism);
    std::thread::scope(|scope| {
        for p in parts {
            semaphore.acquire();
            let semaphore = &semaphore;
            scope.spawn(move || {
                let result = dest.write(p, "key");
                semaphore.release();
                result.expect("write");
            });
        }
    });
}

fn bench_concurrent_upload(c: &mut Criterion) {
    let dir = tempfile::tempdir().expect("temp dir");
    let parts = staged_parts(dir.path(), 8);
    for latency_ms in [5u64, 50, 200] {
        let sequential_dest = LatencyDest {
            latency_ms,
            written: AtomicUsize::new(0),
        };
        let parallel_dest = LatencyDest {
            latency_ms,
            written: AtomicUsize::new(0),
        };
        let mut group = c.benchmark_group(format!("concurrent_upload_latency_{latency_ms}ms"));
        group.bench_function("sequential_8_parts", |b| {
            b.iter(|| upload_sequential(&sequential_dest, &parts))
        });
        group.bench_function("parallel_8_parts", |b| {
            b.iter(|| upload_parallel(&parallel_dest, &parts, 8))
        });
        group.finish();
    }
}

criterion_group!(concurrent_upload_benches, bench_concurrent_upload);
criterion_main!(concurrent_upload_benches);
