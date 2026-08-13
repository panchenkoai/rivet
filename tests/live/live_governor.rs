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
//! Run: `docker compose up -d postgres && cargo test --test live_suite -- --ignored`.

use crate::common::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn duckdb_total_parquet_rows(dir: &std::path::Path) -> usize {
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
        duckdb_total_parquet_rows(out_dir.path()),
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
        duckdb_total_parquet_rows(out_dir.path()),
        ROWS as usize,
        "every exported row must round-trip despite concurrent writes to the same rows"
    );
}

/// Regression for the governor deadlock: the worker bumps `completed` only on
/// success, but the governor's exit condition was keyed on it — so a *failing*
/// chunk left the governor spinning forever and `thread::scope` could never
/// join. Here every chunk fails at the destination-write stage — the local
/// path sits *under a regular file*, so each `dest.write` hits ENOTDIR — with
/// adaptive on + parallel>1. The run MUST terminate (with an error), not hang.
/// Spawned under a watchdog so a regression trips the timeout instead of
/// hanging the whole test binary.
///
/// NB: the failure is forced at the write stage on purpose. The original
/// version relied on a `NUMERIC` column having no safe mapping, but chunked
/// `NUMERIC(p,s)` now resolves its precision from the base query (catalog-hint
/// fix) and succeeds — so a numeric column no longer fails any chunk.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn governor_does_not_deadlock_when_chunks_fail() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(500);
    let out_dir = tempfile::tempdir().unwrap();
    // Point the local destination *under a regular file* so every chunk's
    // `dest.write` fails with ENOTDIR — a genuine per-chunk write failure that
    // drives the all-chunks-fail path through the real parallel write stage.
    let blocker = out_dir.path().join("not_a_dir");
    std::fs::write(&blocker, b"x").expect("write blocker file");
    let dest_path = blocker.join("sub");
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
        out = dest_path.display(),
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

/// #152: the SAME back-off, on the KEYSET runner. The governor was chunked-only;
/// a `parallel: N` keyset export on a straining source adapted batches but never
/// shed workers. Identical pressure workload, `chunk_by_key` instead of
/// `chunk_column` — the run must arm the keyset governor and shed at least one
/// worker under rising checkpoint pressure. RED against a build with the keyset
/// governor wiring removed (batches still adapt, but no "backed off" line).
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn keyset_governor_backs_off_under_concurrent_write_pressure() {
    require_alive(LiveService::Postgres);

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
    table: {name}
    mode: chunked
    chunk_by_key: id
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
    std::thread::sleep(Duration::from_millis(400));

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
        match child.try_wait().expect("try_wait") {
            Some(s) => break s,
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                stop.store(true, Ordering::Relaxed);
                let _ = writer.join();
                panic!("governed keyset run under write pressure did not finish within 120s");
            }
            None => std::thread::sleep(Duration::from_millis(100)),
        }
    };
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread");

    let stderr = std::fs::read_to_string(&log_path).unwrap_or_default();
    assert!(
        status.success(),
        "governed keyset run must complete; stderr:\n{stderr}"
    );
    assert!(
        // The chunked and keyset runners now share one GovernorHarness seam, so both log the
        // SAME arm message (the old "on keyset" suffix is gone); this being a keyset run, its
        // presence proves the governor armed on the keyset path.
        stderr.contains("adaptive concurrency governor active"),
        "the KEYSET governor must arm for adaptive + parallel>1; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("backed off"),
        "#152: under rising pressure the keyset governor must shed at least one worker; stderr:\n{stderr}"
    );
    assert_eq!(
        duckdb_total_parquet_rows(out_dir.path()),
        ROWS as usize,
        "every row must round-trip through the governed keyset run"
    );
}

/// Field regression (2026-08-13, production pool run): the governor used to
/// sample the SAME counter as the adaptive batch loop — on MySQL the
/// own-read spill proxy (`Created_tmp_disk_tables` + buffer-pool waits). An
/// export whose own queries spill tmp tables to disk therefore fed the
/// governor a permanently-rising signal on an otherwise idle server: it shed
/// workers 4→3→2→1 ("source pressure rising") and never recovered, since its
/// own pages kept the counter climbing. Measured in the field as every
/// keyset export running 2–2.7× slower (+1h48m makespan) with zero foreign
/// load. The governor now listens to `Innodb_log_waits` (redo-WRITE pressure
/// a read-only export cannot move — `Source::sample_governor_pressure`).
///
/// Fixture: a chunked MySQL export over a `DISTINCT` derived query — DISTINCT
/// blocks derived-merge, so every chunk materializes the whole ~40 MB derived
/// table, overflowing the default 16 MB `tmp_table_size` to disk: the run
/// PROVABLY inflates the spill counter by itself (asserted below, so the
/// fixture cannot go inert). The mechanism is runner-agnostic — chunked and
/// keyset share one GovernorHarness — chunked is used because a `DISTINCT`
/// spiller is legal there without introspection.
///
/// RED against the old shared-signal bridge: re-point
/// `impl PressureSource for Box<dyn Source>` back at `sample_pressure` and
/// this fails on the `backed off` assertion.
#[test]
#[ignore = "live: requires docker-compose mysql"]
fn mysql_governor_ignores_the_exports_own_spill_exhaust() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    // Serialize against other MySQL tests: this test flips server-wide
    // tmp-table globals, which would slow unrelated tests' queries — and a
    // concurrently-finishing sibling instance restoring the globals mid-run
    // could deflate this test's spill delta below its activation guard.
    let _globals_lock = mysql_globals_guard();

    const ROWS: i64 = 20_000;
    // Wide payload so the DISTINCT materialization overflows tmp_table_size.
    let name = unique_name("rivet_qa_gov_spill");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB"
    ))
    .expect("create table");
    for start in (1..=ROWS).step_by(1000) {
        let values: Vec<String> = (start..start + 1000)
            .map(|i| format!("({i}, REPEAT('x', 2048))"))
            .collect();
        c.query_drop(format!(
            "INSERT INTO {name} (id, payload) VALUES {}",
            values.join(",")
        ))
        .expect("seed batch");
    }
    let table = MysqlTable::adopt(name.clone());

    // MySQL 8's TempTable engine keeps implicit tmp tables in a shared 1 GB
    // RAM pool (`temptable_max_ram`), so a 40 MB DISTINCT materialization
    // never reaches disk and the spill counter stays flat — the fixture goes
    // inert (the activation guard below caught exactly that on first run).
    // Force the legacy MEMORY engine with a tiny size ceiling for the test's
    // duration so every chunk's materialization provably spills; the guard
    // restores the prior globals on every exit path (same flip-and-reset
    // pattern as the session-state timezone tests).
    struct TmpTableGlobals {
        engine: String,
        tmp_size: u64,
        heap_size: u64,
    }
    // Globals need SYSTEM_VARIABLES_ADMIN — the app user deliberately lacks
    // it, so the flip runs as root (compose: MYSQL_ROOT_PASSWORD=rivet).
    const MYSQL_ROOT_URL: &str = "mysql://root:rivet@127.0.0.1:3306/rivet";
    impl Drop for TmpTableGlobals {
        fn drop(&mut self) {
            if let Ok(pool) = mysql::Pool::new(MYSQL_ROOT_URL)
                && let Ok(mut c) = pool.get_conn()
            {
                let _ = c.query_drop(format!(
                    "SET GLOBAL internal_tmp_mem_storage_engine = {}",
                    self.engine
                ));
                let _ = c.query_drop(format!("SET GLOBAL tmp_table_size = {}", self.tmp_size));
                let _ = c.query_drop(format!(
                    "SET GLOBAL max_heap_table_size = {}",
                    self.heap_size
                ));
            }
        }
    }
    let prior = {
        let mut c = mysql::Pool::new(MYSQL_ROOT_URL)
            .expect("root pool")
            .get_conn()
            .expect("root conn");
        let get = |c: &mut mysql::PooledConn, var: &str| -> String {
            let rows: Vec<(String, String)> = c
                .query(format!("SHOW GLOBAL VARIABLES LIKE '{var}'"))
                .expect("read global");
            rows.first().map(|(_, v)| v.clone()).unwrap_or_default()
        };
        let engine = get(&mut c, "internal_tmp_mem_storage_engine");
        let tmp_size: u64 = get(&mut c, "tmp_table_size").parse().unwrap_or(16777216);
        let heap_size: u64 = get(&mut c, "max_heap_table_size")
            .parse()
            .unwrap_or(16777216);
        c.query_drop("SET GLOBAL internal_tmp_mem_storage_engine = MEMORY")
            .expect("force MEMORY tmp engine");
        c.query_drop("SET GLOBAL tmp_table_size = 16384")
            .expect("shrink tmp_table_size");
        c.query_drop("SET GLOBAL max_heap_table_size = 16384")
            .expect("shrink max_heap_table_size");
        TmpTableGlobals {
            engine,
            tmp_size,
            heap_size,
        }
    };

    let spills = |c: &mut mysql::PooledConn| -> u64 {
        let rows: Vec<(String, u64)> = c
            .query("SHOW GLOBAL STATUS LIKE 'Created_tmp_disk_tables'")
            .expect("sample spills");
        rows.first().map(|(_, v)| *v).unwrap_or(0)
    };
    let spills_before = spills(&mut c);

    // Canonical Rig config + runner — no bespoke YAML/Command (the harness
    // rule this file predates; new tests go through the rig).
    let rig = Rig::mysql_batch(table.name())
        .query(&format!(
            "SELECT DISTINCT id, payload FROM {}",
            table.name()
        ))
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  min_parallel: 1")
        .source_line("  batch_size: 250")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000")
        .export_line("parallel: 4");
    let out = rig.run_with_envs(&[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")]);
    let out_dir = rig.out_dir();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "run must complete; stderr:\n{stderr}");
    assert!(
        stderr.contains("adaptive concurrency governor active"),
        "governor must arm (adaptive + parallel>1); stderr:\n{stderr}"
    );
    // Activation-threshold guard: the fixture must really have spilled — a
    // fixture that stops spilling (bigger tmp_table_size, narrower payload)
    // would make the no-shed assertion below vacuous. ≥10 ties the delta to
    // this run's ~20 chunk materializations, not to concurrent test noise on
    // the shared server.
    let spill_delta = spills(&mut c).saturating_sub(spills_before);
    assert!(
        spill_delta >= 10,
        "fixture went inert: expected the run's own chunks to spill tmp tables \
         (Created_tmp_disk_tables delta ≥ 10), got {spill_delta}"
    );
    // The point: with the export's OWN spills proven present and no foreign
    // write load, the governor must hold parallelism flat — its signal is now
    // `Innodb_log_waits`, which its own read-only pages cannot move.
    assert!(
        !stderr.contains("backed off"),
        "governor shed workers on the export's OWN spill exhaust; stderr:\n{stderr}"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&out_dir),
        ROWS as usize,
        "every row must round-trip"
    );
    drop(prior);
}

/// The CLASS guard behind the 2026-08-13 field regression: on an IDLE source
/// every adaptive feedback loop (governor, batch shrink, and whatever joins
/// them later) has nothing legitimate to react to — so `adaptive: true` must
/// cost approximately nothing versus `adaptive: false` on the same fixture.
/// The governor bug this distills (self-exhaust spiral to `min_parallel: 1`)
/// made the adaptive side 2.4× slower; any future controller that feeds on a
/// signal its own workload moves will fail this same ratio gate, whatever its
/// mechanism. Limits stated honestly: the stand cannot express slowdowns that
/// need multi-GB tables vs a small buffer pool, so this canary catches
/// signal-driven spirals expressible at stand scale, and the run-over-run
/// throughput report (`aggregate::warn_throughput_regressions`) is the
/// production-scale net behind it.
#[test]
#[ignore = "live: requires docker-compose mysql"]
fn mysql_adaptive_never_loses_to_its_own_baseline_on_an_idle_source() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    // The A/B wall-clock ratio is only meaningful without a heavy sibling
    // starting between the two runs — take the same cross-process lock the
    // globals-flipping test holds (bughunt 2026-08-13).
    let _globals_lock = mysql_globals_guard();

    const ROWS: i64 = 60_000;
    let name = unique_name("rivet_qa_ab_canary");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB"
    ))
    .expect("create table");
    for start in (1..=ROWS).step_by(2000) {
        let values: Vec<String> = (start..start + 2000)
            .map(|i| format!("({i}, REPEAT('x', 512))"))
            .collect();
        c.query_drop(format!(
            "INSERT INTO {name} (id, payload) VALUES {}",
            values.join(",")
        ))
        .expect("seed batch");
    }
    let table = MysqlTable::adopt(name.clone());

    // Keyset-parallel with ≥10 batches per page, so BOTH feedback loops run
    // at their real cadence (the governor per wall-interval, the batch
    // controller per ADAPTIVE_SAMPLE_INTERVAL batches).
    // Canonical Rig config + runner per leg — no bespoke YAML/Command.
    let run = |adaptive: bool| -> (f64, String) {
        let rig = Rig::mysql_batch(table.name())
            .mode("chunked")
            .source_line("tuning:")
            .source_line(&format!("  adaptive: {adaptive}"))
            .source_line("  min_parallel: 1")
            .source_line("  batch_size: 500")
            .export_line("chunk_by_key: id")
            .export_line("chunk_size: 10000")
            .export_line("parallel: 4");
        let started = std::time::Instant::now();
        let out = rig.run_with_envs(&[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")]);
        let wall = started.elapsed().as_secs_f64();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        assert!(
            out.status.success(),
            "adaptive={adaptive} run failed:\n{stderr}"
        );
        assert_eq!(
            duckdb_total_parquet_rows(&rig.out_dir()),
            ROWS as usize,
            "adaptive={adaptive}: every row must round-trip"
        );
        (wall, stderr)
    };

    // Baseline first so its page cache warm-up, if anything, favors the
    // adaptive side — a false PASS from ordering is not possible, only a
    // false margin against the assertion.
    let (wall_off, _) = run(false);
    let (wall_on, stderr_on) = run(true);

    assert!(
        !stderr_on.contains("backed off"),
        "idle source: the governor has nothing to shed for; stderr:\n{stderr_on}"
    );
    // Generous bound: 1.6× + 1s absolute slack absorbs stand noise while the
    // failure mode it exists for (the field spiral) was 2.4×.
    assert!(
        wall_on <= wall_off * 1.6 + 1.0,
        "adaptive: true must not lose to adaptive: false on an idle source \
         (self-feedback suspected): on={wall_on:.2}s vs off={wall_off:.2}s"
    );
}
