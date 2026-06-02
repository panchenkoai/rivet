//! Live coverage for the OPT-2 adaptive concurrency governor.
//!
//! `GovernorState`'s *decision* logic is unit-tested. This pins the
//! *deterministic* end-to-end wiring on a real chunked-parallel run:
//!
//! 1. `governor_activates_and_run_completes` — with `adaptive: true` + `parallel > 1`
//!    the governor thread spins up on its own monitoring connection, the run
//!    completes, and every row round-trips.
//! 2. `governor_backs_off_under_concurrent_write_pressure` — the real thing: a
//!    background writer hammers the exported table (concurrent INSERTs +
//!    periodic CHECKPOINT, which is what bumps `pg_stat_bgwriter.checkpoints_req`,
//!    the governor's PG pressure proxy). A wide payload + small batches make the
//!    run last long enough — and the governor backing off to `min_parallel`
//!    lengthens it further — so the sampler fires repeatedly under rising
//!    pressure and we observe a real back-off in the run journal / log.
//! 3. `governor_does_not_deadlock_when_chunks_fail` — regression: a failing chunk
//!    must not strand the governor and deadlock `thread::scope` (it did before the
//!    `finished`-counter fix — the worker only bumped `completed` on success).
//!
//! Note on the pressure signal: organic INSERT load only moves `checkpoints_req`
//! once WAL exceeds `max_wal_size` (1 GB by default) — impractical to churn in a
//! test without mutating shared server config. A periodic explicit `CHECKPOINT`
//! is the deterministic mover and faithfully models a checkpoint-heavy write
//! workload, so the writer thread does both real INSERTs and CHECKPOINTs.
//!
//! Run: `docker compose up -d postgres && cargo test --test live_governor -- --ignored`.

mod common;
use common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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

/// The value-proof: under a real concurrent write workload the governor must
/// actually shed workers. A background thread hammers the *very rows being
/// exported* with UPDATEs (real WAL + dirty pages + dead tuples) and a periodic
/// CHECKPOINT — the CHECKPOINT is what moves `pg_stat_bgwriter.checkpoints_req`
/// (the PG pressure proxy). The governor samples slower than the checkpoint
/// cadence, so pressure reads as monotonically rising sample-over-sample and
/// the governor steps parallelism down toward `min_parallel`. We pre-warm the
/// writer before launching the run so even the first sample-pair is rising. We
/// UPDATE rather than INSERT so the exported id range
/// `[1, ROWS]` (and thus the round-trip row count) stays deterministic.
///
/// Spawned under a 120 s watchdog so a regression (e.g. the governor deadlock)
/// trips a timeout instead of hanging the whole test binary.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn governor_backs_off_under_concurrent_write_pressure() {
    require_alive(LiveService::Postgres);

    // Wide payload + small batches + a deliberately large per-batch throttle
    // make the run last a hardware-independent ~2 s+ (the throttle is a fixed
    // sleep, so a fast disk/CPU can't shorten it below the governor's reaction
    // window), giving the sampler many ticks under rising pressure.
    const ROWS: i64 = 20_000;
    let table = seed_pg_wide_table(ROWS, 1024);
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
    batch_size: 250
    throttle_ms: 100
exports:
  - name: {name}
    query: "SELECT id, payload FROM {name}"
    mode: chunked
    chunk_column: id
    chunk_size: 1000
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

    // Background writer: concurrent UPDATEs on the rows being exported plus a
    // CHECKPOINT every ~70 ms. The governor samples every 200 ms (set below),
    // comfortably *slower* than the checkpoint cadence, so every sample gap
    // captures at least one new checkpoint and `checkpoints_req` reads as
    // strictly rising pair-over-pair — the condition `GovernorState::observe`
    // needs to shed a worker. (At the old 80 ms ≈ 70 ms the two cadences raced,
    // so adjacent samples were often flat and the back-off was a coin flip on
    // fast hardware.) The UPDATEs are real write contention; the CHECKPOINT is
    // the deterministic signal mover. Organic INSERT load alone only bumps
    // `checkpoints_req` once WAL exceeds `max_wal_size` (1 GB) — impractical
    // here, and mutating that shared server setting is off-limits — so an
    // explicit CHECKPOINT stands in for a checkpoint-heavy workload.
    let stop = Arc::new(AtomicBool::new(false));
    let table_name = table.name().to_string();
    let writer = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut c = pg_connect();
            let mut k: i64 = 1;
            while !stop.load(Ordering::Relaxed) {
                let _ = c.batch_execute(&format!(
                    "UPDATE {table_name} SET payload = repeat('z', 1024), updated_at = now() \
                     WHERE id BETWEEN {k} AND {k} + 999; CHECKPOINT;"
                ));
                k += 1000;
                if k > ROWS {
                    k = 1;
                }
                std::thread::sleep(Duration::from_millis(70));
            }
        })
    };

    // Pre-warm the pressure signal: let the writer connect and drive
    // `checkpoints_req` up before rivet launches, so the governor's first
    // sample-pair already sees rising pressure — no startup race where the
    // early samples land before the writer's first checkpoint completes.
    std::thread::sleep(Duration::from_millis(400));

    // Watchdog: redirect stderr to a file so we can both assert on it and avoid
    // a piped-buffer deadlock while polling for exit.
    let log_path = cfg_dir.path().join("rivet.stderr");
    let log_file = std::fs::File::create(&log_path).unwrap();
    let mut child = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
        ])
        .env("RUST_LOG", "info")
        .env("RIVET_GOVERNOR_INTERVAL_MS", "200")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(log_file))
        .spawn()
        .expect("spawn rivet run");

    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    let status = loop {
        match child.try_wait().expect("try_wait on rivet child") {
            Some(s) => break s,
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                stop.store(true, Ordering::Relaxed);
                let _ = writer.join();
                panic!("governed run under concurrent write pressure did not finish within 120s");
            }
            None => std::thread::sleep(Duration::from_millis(100)),
        }
    };

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread");

    let stderr = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(
        status.success(),
        "governed run under concurrent write pressure must still complete; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("adaptive concurrency governor active"),
        "governor must arm for adaptive + parallel>1; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("backed off"),
        "under rising checkpoint pressure the governor must shed at least one worker \
         (a 'backed off' parallelism adjustment); stderr:\n{stderr}"
    );
    assert_eq!(
        total_parquet_rows(out_dir.path()),
        ROWS as usize,
        "every exported row must round-trip despite concurrent writes to the same rows"
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
