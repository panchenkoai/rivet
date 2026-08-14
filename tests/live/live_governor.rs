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

/// Prove the governor was ARMED and AWAKE on this leg before any `backed off`
/// assertion is allowed to mean anything.
///
/// `!stderr.contains("backed off")` is equally true of a healthy idle governor
/// and of one that is structurally deaf — its sampler returning `None` freezes
/// parallelism at the ceiling forever (`GovernorState::observe` returns `None`
/// on a `None` sample), which is exactly the state a renamed counter produces
/// (PG 17 moved `checkpoints_req` to `pg_stat_checkpointer.num_requested`; a
/// login losing `VIEW SERVER STATE` does it on MSSQL; a missing
/// `Innodb_log_waits` does it on MySQL). The product emits a distinct line per
/// state (`src/pipeline/governor.rs`), so the canaries assert on all three
/// rather than on the shed count alone (bughunt 2026-08-13, finding "all four
/// canaries pass identically when the governor is DEAF").
fn assert_governor_awake(stderr: &str, engine: &str) {
    assert!(
        stderr.contains("adaptive concurrency governor active"),
        "{engine}: the governor must ARM (adaptive + parallel>1) — without it the \
         no-shed assertion grades nothing; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("governor monitoring connection failed"),
        "{engine}: the governor could not open its monitoring connection, so \
         parallelism stayed static and no-shed is vacuous; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("provides no pressure signal"),
        "{engine}: the governor armed but is DEAF — its sampler yields nothing, so \
         parallelism is frozen at the ceiling and no-shed is vacuous; stderr:\n{stderr}"
    );
}

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
    // Quiet window: this test's CHECKPOINT spam is a sibling's false foreign
    // pressure, and its own rivet runs are a sibling canary's CPU noise.
    let _quiet = quiet_window_guard();

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
    // Quiet window: this test's CHECKPOINT spam is a sibling's false foreign
    // pressure, and its own rivet runs are a sibling canary's CPU noise.
    let _quiet = quiet_window_guard();

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
    // Quiet window FIRST, then the mysql-globals lock (this test flips
    // server-wide tmp-table globals; a sibling restoring them mid-run could
    // deflate the spill delta below the activation guard).
    let _quiet = quiet_window_guard();
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

    // Activation-threshold probe, measured on a SESSION-scoped counter.
    //
    // The guard's job is "this fixture still spills" — otherwise the no-shed
    // assertion below is vacuous. The first version read GLOBAL
    // `Created_tmp_disk_tables` around the run, which cannot do that job on
    // this stand: the tmp-table globals were just shrunk SERVER-WIDE and only
    // the two tests that take `mysql_globals_guard` are excluded, so every
    // other concurrent MySQL live test now spills at a 16 KB threshold into
    // the same counter — an inert fixture stays "proven" by sibling noise
    // (bughunt 2026-08-13).
    //
    // `SHOW SESSION STATUS` is per-connection, so it is noise-immune by
    // construction. The probe replays the EXACT SQL rivet's chunked runner
    // emits for a curated query (`build_chunk_query_sql`:
    // `SELECT * FROM (<query>) AS _rivet WHERE <col> BETWEEN a AND b`) on a
    // connection opened AFTER the flip (session tmp-table limits are copied
    // from the globals at connect). Two windows, not one: the property under
    // test is "EVERY chunk materializes", which one window cannot express —
    // and the counter accumulates, so a single probe cannot tell +1-once from
    // +1-per-chunk. Measured on the dev stand: +1 per window with the flip,
    // +0 without it (so the probe genuinely goes RED on an inert fixture),
    // and `SHOW SESSION STATUS` itself contributes 0.
    //
    // The probe's SQL is DERIVED from the same `base_query` the rig exports, so
    // a future edit to the fixture query cannot leave the probe validating a
    // query the run no longer issues.
    let base_query = format!("SELECT DISTINCT id, payload FROM {}", table.name());
    let session_spills = |c: &mut mysql::PooledConn| -> u64 {
        let rows: Vec<(String, u64)> = c
            .query("SHOW SESSION STATUS LIKE 'Created_tmp_disk_tables'")
            .expect("sample session spills");
        rows.first().map(|(_, v)| *v).unwrap_or(0)
    };
    let mut probe = mysql_connect();
    let probe_before = session_spills(&mut probe);
    for (lo, hi) in [(1, 1000), (1001, 2000)] {
        let n: Option<u64> = probe
            .query_first(format!(
                "SELECT count(*) FROM (SELECT * FROM ({base_query}) AS _rivet \
                 WHERE id BETWEEN {lo} AND {hi}) probe"
            ))
            .expect("probe chunk query");
        assert_eq!(n, Some(1000), "probe window {lo}..{hi} must read its rows");
    }
    let probe_delta = session_spills(&mut probe).saturating_sub(probe_before);
    assert!(
        probe_delta >= 2,
        "fixture went inert: each chunk-shaped query must spill its DISTINCT \
         materialization to disk (session Created_tmp_disk_tables delta ≥ 2 over two \
         windows), got {probe_delta} — the no-shed assertion below would grade nothing"
    );

    // Canonical Rig config + runner — no bespoke YAML/Command (the harness
    // rule this file predates; new tests go through the rig).
    let rig = Rig::mysql_batch(table.name())
        .query(&base_query)
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
    assert_governor_awake(&stderr, "mysql");
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

/// The COST half of the class guard behind the 2026-08-13 field regression: on
/// an IDLE source every adaptive feedback loop (governor, batch shrink, and
/// whatever joins them later) has nothing legitimate to react to — so
/// `adaptive: true` must arm, stay awake, shed nothing, and cost approximately
/// nothing versus `adaptive: false` on the same fixture.
///
/// WHAT THIS TEST CANNOT DO, stated plainly: it is NOT red against the field
/// regression itself. Its fixture is a plain keyset export read through the
/// primary key — no DISTINCT/GROUP BY/filesort, so it creates no implicit temp
/// table, and 60 000 × 512 B never exhausts the buffer pool. The pre-fix
/// governor (pointed at MySQL's own-extraction sum
/// `Created_tmp_disk_tables + Innodb_buffer_pool_wait_free`) therefore samples
/// a FLAT counter here, and `GovernorState::observe` sheds only on
/// `cur > prev` — so the buggy build passes every assertion below. Verified by
/// reasoning about the fixture, not assumed: an earlier version of this doc
/// claimed "any future controller that feeds on a signal its own workload
/// moves will fail this same ratio gate", which this fixture cannot deliver
/// (bughunt 2026-08-13).
///
/// The RED proof against the coupled-signal regression lives in
/// `mysql_governor_ignores_the_exports_own_spill_exhaust`, whose fixture
/// provably crosses the activation threshold (a session-scoped probe asserts
/// the spill). What THIS test pins, and what would go red here, is the cost
/// side that the spill test does not measure: a governor that arms, is awake,
/// and still makes an idle-source run materially slower than its own
/// non-adaptive baseline — the shape (2.4× in the field) of any
/// self-feedback spiral large enough to show at stand scale. Limits: the stand
/// cannot express slowdowns that need multi-GB tables vs a small buffer pool;
/// the run-over-run throughput report
/// (`aggregate::warn_throughput_regressions`) is the production-scale net.
#[test]
#[ignore = "live: requires docker-compose mysql"]
fn mysql_adaptive_never_loses_to_its_own_baseline_on_an_idle_source() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    // Quiet window FIRST (cross-engine CPU/pressure exclusion), then the
    // narrower mysql-globals lock — consistent order, no deadlock.
    let _quiet = quiet_window_guard();
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

    assert_governor_awake(&stderr_on, "mysql");
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

/// Per-engine completion of the adaptive canary: PostgreSQL and SQL Server's
/// governor signals (`checkpoints_req`, `Log Flush Waits _Total`) are
/// write-driven counters a read-only export cannot move — so on an idle
/// source `adaptive: true` must shed nothing and cost ~nothing. That holds
/// by construction TODAY; this pins it so a future signal swap (exactly how
/// the MySQL regression shipped: the counter under the governor was
/// re-pointed for another loop's benefit) goes RED here instead of in a
/// field pool run. Lighter than the MySQL canary on purpose: no spill
/// forcing needed — the pin is "no self-feedback", not "spills are ignored".
#[test]
#[ignore = "live: requires docker-compose postgres"]
fn pg_adaptive_never_loses_to_its_own_baseline_on_an_idle_source() {
    require_alive(LiveService::Postgres);
    // Quiet window: the no-shed and ratio gates need no sibling pressure/CPU.
    let _quiet = quiet_window_guard();
    const ROWS: i64 = 40_000;
    let table = seed_pg_wide_table(ROWS, 512);
    let run = |adaptive: bool| -> (f64, String) {
        let rig = Rig::pg_batch(table.name())
            .mode("chunked")
            .source_line("tuning:")
            .source_line(&format!("  adaptive: {adaptive}"))
            .source_line("  min_parallel: 1")
            .source_line("  batch_size: 500")
            .export_line("chunk_column: id")
            .export_line("chunk_size: 5000")
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
    let (wall_off, _) = run(false);
    let (wall_on, stderr_on) = run(true);
    assert_governor_awake(&stderr_on, "postgres");
    assert!(
        !stderr_on.contains("backed off"),
        "idle source: the PG governor has nothing to shed for; stderr:\n{stderr_on}"
    );
    assert!(
        wall_on <= wall_off * 1.6 + 1.0,
        "adaptive must not lose to its own baseline: on={wall_on:.2}s off={wall_off:.2}s"
    );
}

/// SQL Server variant — the first end-to-end governor test on MSSQL at all:
/// its shed side has never had a deterministic live driver (forcing real
/// `Log Flush Waits` needs contended log flushes), so the arm + no-self-shed
/// half is pinned here and the shed CONTRACT rides the shared
/// GovernorState/harness proven on PG; the per-engine difference is only the
/// counter SQL, verified empirically (`_Total` row semantics).
#[test]
#[ignore = "live: requires docker-compose mssql"]
fn mssql_adaptive_never_loses_to_its_own_baseline_on_an_idle_source() {
    require_alive(LiveService::Mssql);
    // Quiet window: the no-shed and ratio gates need no sibling pressure/CPU.
    let _quiet = quiet_window_guard();
    const ROWS: i64 = 40_000;
    let table = seed_mssql_numeric_table(ROWS);
    let run = |adaptive: bool| -> (f64, String) {
        let rig = Rig::mssql_batch(table.name())
            .mode("chunked")
            .source_line("tuning:")
            .source_line(&format!("  adaptive: {adaptive}"))
            .source_line("  min_parallel: 1")
            .source_line("  batch_size: 500")
            .export_line("chunk_column: id")
            .export_line("chunk_size: 5000")
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
    let (wall_off, _) = run(false);
    let (wall_on, stderr_on) = run(true);
    assert_governor_awake(&stderr_on, "mssql");
    assert!(
        !stderr_on.contains("backed off"),
        "idle source: the MSSQL governor has nothing to shed for; stderr:\n{stderr_on}"
    );
    assert!(
        wall_on <= wall_off * 1.6 + 1.0,
        "adaptive must not lose to its own baseline: on={wall_on:.2}s off={wall_off:.2}s"
    );
}

/// The governor's PG **17+** sampler arm, against a real modern catalog.
///
/// The main live stack is postgres:16, where `pg_stat_bgwriter.checkpoints_req`
/// still resolves — so every other governor test takes the LEGACY arm and the
/// modern one shipped with no live coverage at all (round-3 completeness
/// critic). That arm is not a cosmetic difference: on PG 17+ the old query
/// ERRORs, and the batch loop samples INSIDE the export's cursor transaction,
/// so the error aborts it and the next FETCH dies. Verified by hand on the
/// postgres:18 stand (2026-08-14):
///
/// ```text
/// ERROR:  column "checkpoints_req" does not exist
/// ERROR:  current transaction is aborted, commands ignored until end of transaction block
/// ```
///
/// i.e. a wrong arm is a hard export failure, never a quiet `None`. This test
/// runs a governed export against that stand and requires the governor to be
/// AWAKE — armed, connected, and reading a signal — which is exactly what a
/// sampler that ERRORs or returns `None` cannot be.
///
/// SKIPPED (not failed) when the optional `dev/stand` is down: it is opt-in,
/// unlike the main compose stack. Bring it up with
/// `docker compose --profile batch -f dev/stand/docker-compose.yaml up -d pg18-batch`.
#[test]
#[ignore = "live: requires the optional dev/stand pg18-batch (:5518)"]
fn pg18_governor_samples_the_modern_checkpointer_catalog() {
    if !pg_modern_alive() {
        eprintln!(
            "SKIP pg18_governor_samples_the_modern_checkpointer_catalog: the optional \
             dev/stand pg18-batch (:5518) is not up"
        );
        return;
    }
    let _quiet = quiet_window_guard();

    let table = format!("gov_pg18_{}", std::process::id());
    let modern = POSTGRES_MODERN_URL;
    pg_modern_exec(&format!(
        "DROP TABLE IF EXISTS {table}; \
         CREATE TABLE {table} (id bigint PRIMARY KEY, payload text); \
         INSERT INTO {table} SELECT g, repeat('x', 512) FROM generate_series(1, 60000) g;"
    ));

    let rig = Rig::pg_batch(&table)
        .source_url(modern)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 5000")
        .export_line("parallel: 4")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  batch_size: 2000");
    let out = rig.run_with_envs(&[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")]);
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        out.status.success(),
        "the governed export must succeed on a PG17+ catalog — a sampler naming the \
         removed column would abort the cursor transaction; stderr:\n{stderr}"
    );

    // The whole point: on a PG17+ catalog the governor must be AWAKE. A sampler
    // that named the removed column would have aborted the cursor transaction
    // and failed the run outright; one that quietly returned None would trip
    // the "provides no pressure signal" branch.
    assert_governor_awake(&stderr, "postgres-18");
    pg_modern_exec(&format!("DROP TABLE IF EXISTS {table};"));
}

/// Run SQL on the optional PG 17+ stand via `docker exec` — it is not part of
/// the main stack, so the test helpers' pooled clients do not know about it.
fn pg_modern_exec(sql: &str) {
    let out = std::process::Command::new("docker")
        .args([
            "exec",
            "-i",
            "stand-pg18-batch-1",
            "psql",
            "-U",
            "rivet",
            "-d",
            "rivet",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ])
        .output()
        .expect("docker exec psql on the pg18 stand");
    assert!(
        out.status.success(),
        "pg18 stand SQL failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
