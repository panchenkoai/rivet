//! Live coverage for the OPT-2 adaptive concurrency governor.
//!
//! `GovernorState`'s *decision* logic is unit-tested. This pins the
//! *deterministic* end-to-end wiring on a real chunked-parallel run:
//!
//! 1. `governor_activates_and_run_completes` — with `adaptive: true` + `parallel > 1`
//!    the governor thread spins up on its own monitoring connection, the run
//!    completes, and every row round-trips.
//! 2. `governor_does_not_deadlock_when_chunks_fail` — regression: a failing chunk
//!    must not strand the governor and deadlock `thread::scope` (it did before the
//!    `finished`-counter fix — the worker only bumped `completed` on success).
//!
//! Observing an actual *back-off* live is timing-dependent (it needs a run long
//! enough to sample twice under rising pressure). The deterministic home for that
//! assertion is a hermetic test of an extracted `Governor` loop driven by a fake
//! pressure source — a follow-up to the "extract Governor" architecture-review item.
//!
//! Run: `docker compose up -d postgres && cargo test --test live_governor -- --ignored`.

mod common;
use common::*;

use std::time::Duration;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn total_parquet_rows(dir: &std::path::Path) -> usize {
    let mut n = 0;
    for path in files_with_extension(dir, "parquet") {
        let bytes = std::fs::read(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        for b in reader {
            n += b.unwrap().num_rows();
        }
    }
    n
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn governor_activates_and_run_completes() {
    require_alive(LiveService::Postgres);

    const N: usize = 1000;
    let table = seed_pg_numeric_table(N as i64);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
  tuning:
    adaptive: true
    min_parallel: 2
exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 100
    parallel: 8
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .env("RUST_LOG", "info")
        .output()
        .expect("spawn rivet run");

    assert!(
        out.status.success(),
        "adaptive chunked-parallel run must complete; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("adaptive concurrency governor active"),
        "governor must activate for adaptive + parallel>1; stderr:\n{stderr}"
    );
    assert_eq!(
        total_parquet_rows(out_dir.path()),
        N,
        "every row must round-trip through the governed parallel run"
    );
}

/// Regression for the governor deadlock: the worker bumps `completed` only on
/// success, but the governor's exit condition was keyed on it — so a *failing*
/// chunk left the governor spinning forever and `thread::scope` could never
/// join. Here every chunk fails (a `numeric` column with no override has no
/// safe Rivet mapping) with adaptive on + parallel>1. The run MUST terminate
/// (with an error), not hang. Spawned under a watchdog so a regression trips
/// the timeout instead of hanging the whole test binary.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn governor_does_not_deadlock_when_chunks_fail() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(500);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
  tuning:
    adaptive: true
    min_parallel: 2
exports:
  - name: {name}
    query: "SELECT id, name, amount FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 100
    parallel: 8
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let mut child = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn rivet run");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match child.try_wait().expect("try_wait on rivet child") {
            Some(status) => {
                assert!(
                    !status.success(),
                    "run with all-failing chunks should exit non-zero"
                );
                return;
            }
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                panic!(
                    "governor deadlock regression: adaptive chunked-parallel run did not \
                     terminate within 30s when every chunk fails"
                );
            }
            None => std::thread::sleep(Duration::from_millis(200)),
        }
    }
}
