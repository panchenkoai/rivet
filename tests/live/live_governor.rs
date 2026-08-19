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

/// RAII for a background pressure-writer thread: it sets the stop flag and
/// REAPS the thread on every exit path, including the ones a trailing
/// `stop.store(..)` + `join()` cannot reach.
///
/// Three such paths exist in every pressure test here and none of them is
/// exotic: an assertion that fails ABOVE the join, the watchdog arm that
/// `panic!`s on a timeout, and the four `.expect`s inside
/// `Rig::run_with_envs_bounded` (create stdout/stderr file, spawn, try_wait) —
/// a panic there unwinds straight past the test's own cleanup.
///
/// A leaked writer is not merely untidy. These loops hammer a SHARED server
/// (~2 MB of redo per statement on MySQL, ~200 KB per commit on MSSQL) for the
/// remaining lifetime of the test BINARY, i.e. they become exactly the foreign
/// pressure every later governor test's `quiet_window_guard` exists to exclude
/// — and on MySQL the loop would keep INSERTing into a table the scratch guard
/// has already dropped, spinning on errors.
///
/// Declaration order is load-bearing: declare the writer AFTER the scratch
/// table's guard so it drops FIRST (Rust drops in reverse declaration order).
/// The writer must be stopped before the `DROP TABLE` it is inserting into,
/// otherwise the drop blocks behind that statement's metadata lock.
struct PressureWriter {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl PressureWriter {
    /// Spawn `body`, handing it the stop flag it must poll.
    fn spawn<F>(body: F) -> Self
    where
        F: FnOnce(Arc<AtomicBool>) + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || body(stop))
        };
        PressureWriter {
            stop,
            handle: Some(handle),
        }
    }

    /// Stop the writer and reap the thread; `true` if it PANICKED (the
    /// attribution guard the pressure tests assert on before grading a shed).
    /// Idempotent: `Drop` is a no-op once this has run, so the explicit call
    /// never double-joins.
    fn reap(&mut self) -> bool {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().is_some_and(|h| h.join().is_err())
    }
}

impl Drop for PressureWriter {
    fn drop(&mut self) {
        let _ = self.reap();
    }
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn governor_activates_and_run_completes() {
    require_alive(LiveService::Postgres);

    const N: usize = 1000;
    let table = seed_pg_numeric_table(N as i64);
    // Config through the canonical rig (it owns the tempdir and the
    // destination); the governor knobs ride the `tuning:` block as source
    // lines and the chunked plan as export lines.
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  min_parallel: 2")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 100")
        .export_line("parallel: 8");

    // `RUST_LOG=info` is load-bearing: the arming line the assertion below
    // reads is emitted at info level.
    let out = rig.run_with_envs(&[("RUST_LOG", "info")]);

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
        duckdb_total_parquet_rows(&rig.out_dir()),
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
    // Config through the canonical rig (it owns the tempdir and the
    // destination); the governor knobs ride the `tuning:` block as source
    // lines and the chunked plan as export lines.
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, payload FROM {}", table.name()))
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  min_parallel: 2")
        .source_line("  batch_size: 250")
        .source_line("  throttle_ms: 100")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000")
        .export_line("parallel: 8");

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
    let table_name = table.name().to_string();
    let mut writer = PressureWriter::spawn(move |stop| {
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
    });

    // Pre-warm the pressure signal: let the writer connect and drive
    // `checkpoints_req` up before rivet launches, so the governor's first
    // sample-pair already sees rising pressure — no startup race where the
    // early samples land before the writer's first checkpoint completes.
    std::thread::sleep(Duration::from_millis(400));

    // Watchdog through the rig: `run_with_envs_bounded` polls `try_wait` under a
    // wall-clock ceiling with stdout/stderr on FILES (polling an undrained pipe
    // is its own deadlock), so a governor hang fails THIS test instead of
    // wedging the suite behind the quiet-window lock. `None` == had to be killed.
    let out = rig.run_with_envs_bounded(
        &[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")],
        Duration::from_secs(120),
    );

    // Stop the writer BEFORE any panic path — a timeout must not leave a thread
    // hammering the shared server (and CHECKPOINTing) for the rest of the run.
    // The explicit reap keeps the panicked-writer verdict; the guard's Drop
    // covers the paths that never reach this line.
    assert!(!writer.reap(), "the write-pressure writer thread PANICKED");
    let out = out.expect("governed run under concurrent write pressure did not finish within 120s");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
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
        duckdb_total_parquet_rows(&rig.out_dir()),
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
    // Point the local destination *under a regular file* so every chunk's
    // `dest.write` fails with ENOTDIR — a genuine per-chunk write failure that
    // drives the all-chunks-fail path through the real parallel write stage.
    // The blocker lives in its own tempdir (the rig's destination must NOT be
    // pre-created here — `unwritable_dest_path` is exactly that opt-out).
    let fixture = tempfile::tempdir().unwrap();
    let blocker = fixture.path().join("not_a_dir");
    std::fs::write(&blocker, b"x").expect("write blocker file");
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, name, amount FROM {}", table.name()))
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  min_parallel: 2")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 100")
        .export_line("parallel: 8")
        .unwritable_dest_path(blocker.join("sub"));

    // Watchdog through the rig: `None` means the child had to be killed, i.e.
    // the deadlock is back. Output goes to files, not pipes — polling an
    // undrained pipe is its own deadlock.
    let out = rig
        .run_with_envs_bounded(&[], Duration::from_secs(30))
        .unwrap_or_else(|| {
            panic!(
                "governor deadlock regression: adaptive chunked-parallel run did not \
                 terminate within 30s when every chunk fails"
            )
        });
    assert!(
        !out.status.success(),
        "run with all-failing chunks should exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
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
    // Same rig, DIFFERENT plan: `table:` (the rig's default, no `query:`) and
    // `chunk_by_key` — this is the KEYSET runner, not the range-chunked one.
    // The two tests deliberately keep their own config builders: a shared
    // helper would hide the one line that decides which runner is under test.
    let rig = Rig::pg_batch(table.name())
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  min_parallel: 2")
        .source_line("  batch_size: 250")
        .source_line("  throttle_ms: 100")
        .export_line("chunk_by_key: id")
        .export_line("chunk_size: 1000")
        .export_line("parallel: 8");

    let table_name = table.name().to_string();
    let mut writer = PressureWriter::spawn(move |stop| {
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
    });
    std::thread::sleep(Duration::from_millis(400));

    // Same bounded watchdog as the chunked twin (see there): stderr to a file,
    // `None` == the run had to be killed at the ceiling.
    let out = rig.run_with_envs_bounded(
        &[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")],
        Duration::from_secs(120),
    );
    assert!(!writer.reap(), "the write-pressure writer thread PANICKED");
    let out = out.expect("governed keyset run under write pressure did not finish within 120s");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
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
        duckdb_total_parquet_rows(&rig.out_dir()),
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
    // The GOVERNOR instance, not the shared `mssql`. "Idle" is the whole
    // assertion, and on :1433 it was never a property of this fixture — 72 of
    // the 74 SQL Server live tests do not take the quiet-window lock, and
    // `Log Flush Waits/sec` is read with the `_Total` instance, i.e. server-wide.
    // Reproduced 2026-08-16: alone it passes in 12.98s; under a concurrent
    // commit driver it fails with CI's exact message and shed pattern
    // (4 -> 3 -> 2 -> 1). The governor was RIGHT both times; the fixture was not.
    require_alive(LiveService::MssqlGovernor);
    // Quiet window: the no-shed and ratio gates need no sibling pressure/CPU.
    let _quiet = quiet_window_guard();
    const ROWS: i64 = 40_000;
    let table = seed_mssql_governor_numeric_table(ROWS);
    // ...and the OTHER half of "idle", which the dedicated instance did NOT fix:
    // this fixture's own seed: SQL Server's automatic CHECKPOINT can flush a 40k-row
    // seed well after the INSERTs return, landing inside the graded run. Forcing it
    // here and waiting for the counter to go flat means the run measures the run.
    //
    // Honest scope: this was NOT what turned CI red. The bracket below measured +4
    // flush waits on the 11:00 failure — genuinely idle — so the shed came from the
    // governor's documented `cur > prev` rule, not from the seed. The CHECKPOINT
    // stays because removing the fixture's own deferred work is right regardless;
    // the ASSERTION, not the fixture, was the actual defect.
    mssql_governor_exec("CHECKPOINT");
    wait_for_quiet_mssql_governor_log();
    // Declared once and rendered INTO the config, so the assertion below cannot
    // drift from the run it grades — the hand-typed-dimension trap, in miniature.
    const MIN_PARALLEL: usize = 1;
    const CEILING: usize = 4;
    let run = |adaptive: bool| -> (f64, String) {
        let rig = Rig::mssql_governor_batch(table.name())
            .mode("chunked")
            .source_line("tuning:")
            .source_line(&format!("  adaptive: {adaptive}"))
            .source_line(&format!("  min_parallel: {MIN_PARALLEL}"))
            .source_line("  batch_size: 500")
            .export_line("chunk_column: id")
            .export_line("chunk_size: 5000")
            .export_line(&format!("parallel: {CEILING}"));
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
    // Bracket the graded run with the EXACT counter the governor samples, so a
    // failure says which of the two things broke instead of costing another
    // session of inference. The 07:40 failure had no such evidence: "nothing to
    // shed for" is a product accusation, and the product was innocent.
    const FLUSH_WAITS: &str = "SELECT cntr_value FROM sys.dm_os_performance_counters \
                               WHERE counter_name LIKE 'Log Flush Waits%' \
                               AND instance_name = '_Total'";
    let waits_before = mssql_governor_query_i64(FLUSH_WAITS);
    let (wall_on, stderr_on) = run(true);
    let flush_waits = mssql_governor_query_i64(FLUSH_WAITS) - waits_before;
    assert_governor_awake(&stderr_on, "mssql");
    // What an idle source must NOT produce is a shed that STAYS — not a shed.
    //
    // This assertion was `!stderr.contains("backed off")` and failed twice on CI
    // (07:40, 11:00) against a genuinely quiet server: the bracket below measured
    // +4 log flush waits across the whole graded run, then one 4→3 dip and an
    // immediate 3→4 recovery. That is not a defect, it is the DOCUMENTED policy
    // (`GovernorState::observe`): `under_pressure` is `cur > prev`, so on a
    // fine-grained cumulative counter any incidental server movement sheds one
    // worker and the next flat sample puts it straight back. Those docs also
    // record why hysteresis was REJECTED — #229 was a shed that would not come
    // back (4→3→2→1 read off the governor's own extraction, 1h48m lost on a field
    // pool run), and a sticky policy makes every engine's false positive more
    // expensive to buy protection on one.
    //
    // So the strict form asserted against the design, and only MSSQL noticed:
    // its signal is the fine-grained one. PG's `checkpoints_req` genuinely should
    // not move on an idle box, which is why its twin keeps the stricter form.
    // What this test is FOR is the #229 shape — a descent that never recovers.
    let levels = governor_transitions(&stderr_on);
    let sheds = levels.iter().filter(|(f, t)| t < f).count();
    let recoveries = levels.iter().filter(|(f, t)| t > f).count();
    let lowest = levels.iter().map(|(_, t)| *t).min().unwrap_or(CEILING);
    // A quiet stand measures a delta of 1 across a read-only run (2026-08-14);
    // the sibling pressure test needs THOUSANDS to shed. Past this the source was
    // not idle at all, and the FIXTURE is what broke, not the product.
    const IDLE_CEILING: i64 = 50;
    assert!(
        flush_waits <= IDLE_CEILING,
        "FIXTURE, not product: the source was NOT idle during the graded run \
         (+{flush_waits} log flush waits, ceiling {IDLE_CEILING}) — the governor shed for \
         real pressure and was RIGHT. Something is still writing to :1435.\n{stderr_on}"
    );
    assert!(
        !shed_never_recovered(&levels, MIN_PARALLEL, CEILING),
        "idle source (+{flush_waits} log flush waits, genuinely quiet): the MSSQL governor \
         shed {sheds} time(s) against {recoveries} recovery/ies, reaching parallelism {lowest} \
         (ceiling {CEILING}, floor {MIN_PARALLEL}) — a shed that does not come back is #229, \
         the regression this governor was rewritten for. One transient dip is the documented \
         steady state and is allowed; this is not one.\nstderr:\n{stderr_on}"
    );
    assert!(
        wall_on <= wall_off * 1.6 + 1.0,
        "adaptive must not lose to its own baseline: on={wall_on:.2}s off={wall_off:.2}s"
    );
}

// ─── The idle-canary verdict, as pure functions with an independent oracle ────
//
// A live run on a quiet server can legitimately produce ZERO transitions, so the
// canary's own assertion is vacuous exactly when the source behaves. Splitting
// the parse and the verdict out means the grading logic is provable against
// hand-written logs — the #229 walk must be REFUSED and the documented dip
// ALLOWED — instead of resting on a live run that may never exercise either.
// These two `#[test]`s are deliberately NOT `#[ignore]`: `cargo test
// --all-targets` (ci.yml) runs them with no database in sight.

/// Parse `governor parallelism A → B` transitions out of a run's stderr.
fn governor_transitions(stderr: &str) -> Vec<(usize, usize)> {
    stderr
        .lines()
        .filter_map(|l| l.split_once("governor parallelism ")?.1.split_once(" → "))
        .filter_map(|(from, rest)| {
            let to = rest.split_whitespace().next()?;
            Some((from.trim().parse().ok()?, to.parse().ok()?))
        })
        .collect()
}

/// The #229 shape: sheds that outnumber recoveries by more than the one
/// transient dip the policy documents, or a descent that reaches the floor.
fn shed_never_recovered(levels: &[(usize, usize)], floor: usize, ceiling: usize) -> bool {
    let sheds = levels.iter().filter(|(f, t)| t < f).count();
    let recoveries = levels.iter().filter(|(f, t)| t > f).count();
    let lowest = levels.iter().map(|(_, t)| *t).min().unwrap_or(ceiling);
    sheds > recoveries + 1 || lowest <= floor
}

#[test]
fn the_idle_canary_refuses_the_229_walk_and_allows_the_documented_dip() {
    let line = |from: usize, to: usize, why: &str| {
        format!(
            "[2026-08-17T11:14:05Z WARN rivet::pipeline::governor] export 'e': governor parallelism {from} → {to} ({why})"
        )
    };
    // #229: the shed that would not come back — 4→3→2→1, no recovery.
    let walk = [
        line(4, 3, "source pressure rising: backed off"),
        line(3, 2, "source pressure rising: backed off"),
        line(2, 1, "source pressure rising: backed off"),
    ]
    .join("\n");
    let levels = governor_transitions(&walk);
    assert_eq!(
        levels,
        vec![(4, 3), (3, 2), (2, 1)],
        "the parse must not be inert"
    );
    assert!(
        shed_never_recovered(&levels, 1, 4),
        "a descent to the floor with no recovery is exactly the regression this canary is for"
    );

    // The documented steady state: one dip, immediately undone.
    let dip = [
        line(4, 3, "source pressure rising: backed off"),
        line(3, 4, "source pressure eased: recovered"),
    ]
    .join("\n");
    let levels = governor_transitions(&dip);
    assert_eq!(levels, vec![(4, 3), (3, 4)]);
    assert!(
        !shed_never_recovered(&levels, 1, 4),
        "`cur > prev` on a fine-grained counter DOCUMENTS this dip — failing it \
         asserts against the design, which is what turned CI red twice"
    );

    // And a quiet run: no transitions at all is healthy, not a failure.
    assert!(!shed_never_recovered(
        &governor_transitions("nothing here"),
        1,
        4
    ));
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

/// The governor SHEDS on MySQL, under real redo pressure it did not create.
///
/// The completeness critic of the round-3 bughunt named this gap: the
/// governor's ability to BACK OFF was proven on PostgreSQL only. MySQL's new
/// foreign signal (`Innodb_log_waits`) was covered by must-not-shed canaries
/// plus a readability assert — and a counter that is permanently ZERO satisfies
/// both. `Innodb_log_waits` ticks only when the redo log buffer fills and a
/// thread must wait for a flush, which never happens on a default 16 MB buffer
/// under test-sized load, so "no shed" proved nothing about the mechanism.
///
/// The fixture makes the signal move for real: `innodb_log_buffer_size` is
/// shrunk to its 256 KB minimum for the test's duration (restored by a Drop
/// guard on every exit path) and a background writer streams 256 KB blobs into
/// a SCRATCH table — never the exported one, so the export's own rows are not
/// the pressure. Measured on the dev stand before writing the test: the counter
/// went 12 -> 390 in 2.4 s under exactly this shape, and stayed flat without
/// the shrink.
///
/// Four assertions, in the order that makes each meaningful:
///  1. the governor ARMED (otherwise the rest grades nothing),
///  2. this test's OWN writer really committed (the attributable half of the
///     activation guard),
///  3. the counter really moved (the server-wide half),
///  4. it BACKED OFF at least once.
///
/// Attribution honesty — what each half of the activation guard can and cannot
/// prove. `Innodb_log_waits` is a GLOBAL counter with no session twin (InnoDB
/// status variables ignore `SHOW SESSION STATUS`), so unlike the spill test's
/// session probe the delta in (3) is NOT isolated to this test: it is server-
/// wide, and this test has just shrunk `innodb_log_buffer_size` server-wide, so
/// any concurrent write anywhere on the instance contributes to it. The two
/// cross-process locks do NOT close that: `quiet_window_guard` and
/// `mysql_globals_guard` are taken only by the tests in this file (verified —
/// no other live file references either), so they serialise the governor
/// tests against each other and exclude nothing else on the shared server.
/// Hence the split. (2) counts SUCCESSFUL statements from this test's own
/// writer — state only this fixture can produce, and the only assertion that
/// goes RED on the silent-return paths below (a bad root password, a revoked
/// grant, an exhausted `max_connections`). (3) is a floor calibrated against
/// measurement, not guessed: this fixture moves the counter by ~378, while an
/// ordinary sibling seed (4 000 × 2 KB rows with the buffer already shrunk)
/// moves it by 16 — measured on the dev stand 2026-08-14. So >= 100 excludes
/// the sibling shape that the old `>= 10` admitted. Neither half alone is
/// proof of attribution; (2) is the load-bearing one.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_governor_backs_off_under_real_redo_pressure() {
    use mysql::prelude::Queryable;

    require_alive(LiveService::Mysql);
    let _quiet = quiet_window_guard();
    let _globals = mysql_globals_guard();

    // Globals need SYSTEM_VARIABLES_ADMIN — the app user deliberately lacks it,
    // so the flip runs as root (compose: MYSQL_ROOT_PASSWORD=rivet).
    const MYSQL_ROOT_URL: &str = "mysql://root:rivet@127.0.0.1:3306/rivet";
    struct RedoBuffer(u64);
    impl Drop for RedoBuffer {
        fn drop(&mut self) {
            if let Ok(pool) = mysql::Pool::new(MYSQL_ROOT_URL)
                && let Ok(mut c) = pool.get_conn()
            {
                let _ = c.query_drop(format!("SET GLOBAL innodb_log_buffer_size = {}", self.0));
            }
        }
    }
    let mut root = mysql::Pool::new(MYSQL_ROOT_URL)
        .expect("root pool")
        .get_conn()
        .expect("root conn");
    let prior_buffer: u64 = {
        let rows: Vec<(String, String)> = root
            .query("SHOW GLOBAL VARIABLES LIKE 'innodb_log_buffer_size'")
            .expect("read innodb_log_buffer_size");
        rows.first()
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(16_777_216)
    };
    root.query_drop("SET GLOBAL innodb_log_buffer_size = 262144")
        .expect("shrink the redo log buffer");
    let _restore = RedoBuffer(prior_buffer);

    let log_waits = |c: &mut mysql::PooledConn| -> u64 {
        let rows: Vec<(String, u64)> = c
            .query("SHOW GLOBAL STATUS LIKE 'Innodb_log_waits'")
            .expect("sample Innodb_log_waits");
        rows.first().map(|(_, v)| *v).unwrap_or(0)
    };

    const ROWS: i64 = 20_000;
    let table = seed_mysql_numeric_table(ROWS);
    let scratch = format!("{}_redo", table.name());
    root.query_drop(format!(
        "CREATE TABLE {scratch} (id BIGINT AUTO_INCREMENT PRIMARY KEY, payload LONGBLOB) \
         ENGINE=InnoDB"
    ))
    .expect("create the redo-pressure scratch table");
    // RAII, not a trailing DROP — the same treatment the MSSQL twin already
    // carries, for the same reason: this table holds 256 KB blobs on a SHARED
    // server, and a trailing statement is skipped by every panic path (a failed
    // assertion, the watchdog arm below, an `.expect` inside the bounded
    // runner), leaving one `<table>_redo` behind per timeout. Position is
    // deliberate (drop runs in reverse declaration order): declared BEFORE the
    // writer guard, so the writer is reaped first and the DROP does not queue
    // behind its INSERT's metadata lock; declared AFTER `_restore` and the
    // seeded `table`, so the log-buffer restore is the last thing to run.
    let _scratch_guard = MysqlTable::adopt(scratch.clone());

    // Background writer: 8 x 256 KB blobs per statement is ~2 MB of redo
    // against a 256 KB buffer, so each statement forces several waits. It
    // writes to the SCRATCH table, never the exported one — the pressure must
    // be FOREIGN, which is the whole point of the signal split.
    //
    // The ATTRIBUTABLE half of the activation guard: statements this test's own
    // writer really committed. Every failure path in the thread below is silent
    // by design (a dead writer must not abort the run mid-assertion), so without
    // this counter the only detector of a writer that never connected is a
    // GLOBAL counter every concurrent test also moves.
    let writer_inserts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut writer = PressureWriter::spawn({
        let inserts = Arc::clone(&writer_inserts);
        let scratch = scratch.clone();
        move |stop| {
            let Ok(pool) = mysql::Pool::new(MYSQL_ROOT_URL) else {
                return;
            };
            let Ok(mut c) = pool.get_conn() else { return };
            let values = std::iter::repeat_n("(REPEAT(0x61, 262144))", 8)
                .collect::<Vec<_>>()
                .join(",");
            while !stop.load(Ordering::Relaxed) {
                // Count only the INSERT: it is the redo mover (2 MB against a
                // 256 KB buffer), the DELETE is just housekeeping.
                if c.query_drop(format!("INSERT INTO {scratch} (payload) VALUES {values}"))
                    .is_ok()
                {
                    inserts.fetch_add(1, Ordering::Relaxed);
                }
                // Keep the table from growing without bound over the run.
                let _ = c.query_drop(format!("DELETE FROM {scratch} ORDER BY id ASC LIMIT 8"));
            }
        }
    });

    // Let the writer drive the counter up before rivet starts, so the
    // governor's first sample PAIR already spans rising pressure.
    std::thread::sleep(Duration::from_millis(500));
    let waits_before = log_waits(&mut root);

    // Canonical Rig config + runner — no bespoke YAML/Command (this test was
    // written by copying a legacy hand-rolled sibling; the harness rule says
    // one seam). `run_with_envs_bounded` IS the watchdog: it polls the child
    // under a wall-clock ceiling with stdout/stderr to files, so a governor
    // hang fails THIS test instead of wedging the suite behind
    // `quiet_window_guard` + `mysql_globals_guard` with a writer thread still
    // hammering the shared server.
    let rig = Rig::mysql_batch(table.name())
        .query(&format!("SELECT id, name, amount FROM {}", table.name()))
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  min_parallel: 2")
        .source_line("  batch_size: 250")
        .source_line("  throttle_ms: 100")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000")
        .export_line("parallel: 8");
    let out_dir = rig.out_dir();
    let Some(out) = rig.run_with_envs_bounded(
        &[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")],
        Duration::from_secs(120),
    ) else {
        // No hand-rolled stop/join here: unwinding reaps the writer through
        // `PressureWriter`'s Drop and drops the scratch table through
        // `_scratch_guard` — the paths a trailing statement misses.
        panic!(
            "governed MySQL run under redo pressure did not finish within 120s \
             (governor deadlock regression?); the writer had committed {} inserts",
            writer_inserts.load(Ordering::Relaxed)
        );
    };

    let waits_after = log_waits(&mut root);
    let writer_panicked = writer.reap();
    let inserts = writer_inserts.load(Ordering::Relaxed);

    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        out.status.success(),
        "the governed run must still complete under redo pressure; stderr:\n{stderr}"
    );
    assert_governor_awake(&stderr, "mysql");
    // Activation guard BEFORE the shed assertion: without it, a fixture that
    // stopped moving the counter would make the shed assertion silently
    // untestable rather than red. Two halves — see the doc block: the writer's
    // own success count is attributable to THIS test, the counter delta is
    // server-wide.
    assert!(
        !writer_panicked,
        "the redo-pressure writer PANICKED, so the shed assertion below grades \
         something other than this fixture (it committed {inserts} inserts first)"
    );
    assert!(
        inserts >= 5,
        "fixture went inert: this test's OWN writer committed {inserts} redo-pressure \
         inserts (needs >= 5) — it never connected or every statement failed (bad root \
         password? revoked grant? max_connections exhausted?), so any counter movement \
         below belongs to a concurrent sibling, not to this fixture"
    );
    let delta = waits_after.saturating_sub(waits_before);
    assert!(
        delta >= 100,
        "fixture went inert: Innodb_log_waits moved by {delta} across the run (needs \
         >= 100; ~378 measured for this shape over 2.4 s), so the governor had no rising \
         foreign signal to react to and the shed assertion below would grade nothing (is \
         innodb_log_buffer_size still shrunk? {inserts} writer inserts landed)"
    );
    assert!(
        stderr.contains("backed off"),
        "under rising Innodb_log_waits the governor must shed at least one worker; \
         counter moved by {delta} over {inserts} writer inserts; stderr:\n{stderr}"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&out_dir),
        ROWS as usize,
        "every exported row must round-trip while the governor is shedding"
    );
}

/// The pressure fixtures' teardown must survive a PANIC — the exact path a
/// trailing `DROP TABLE` / trailing `stop.store(..) + join()` cannot reach.
///
/// Every pressure test above leaves two things on a SHARED server while it
/// runs: a scratch table full of multi-hundred-KB blobs, and a thread writing
/// into it as fast as the server accepts. Both used to be cleaned up by
/// statements at the END of the test, so a timeout (the watchdog arm), a failed
/// assertion above them, or any `.expect` inside `Rig::run_with_envs_bounded`
/// leaked BOTH — one `<table>_redo` table per timeout, plus a redo-hammering
/// loop that outlives the test and becomes foreign pressure inside every later
/// governor test's "quiet window". This pins the RAII contract that replaced
/// them, by unwinding through a real panic and then asking the SERVER.
///
/// Both halves are RED-provable against the pre-fix shape, and each needs the
/// other: (1) alone is satisfied by a leaked writer (once the table is gone its
/// INSERTs just fail), which is why the writer counts LOOP ITERATIONS, not
/// successful inserts — a leaked thread keeps spinning whether or not the table
/// still exists. `landed` is the not-inert guard: without it a writer that
/// never connected would freeze the iteration counter and pass (2) vacuously.
///
/// Scope honesty: this grades the CONTRACT (`MysqlTable::adopt` +
/// `PressureWriter` both reap on unwind) that
/// `mysql_governor_backs_off_under_real_redo_pressure` and its MSSQL twin now
/// depend on. It cannot observe that those tests still USE the guards — a
/// re-introduced trailing `DROP TABLE` there is caught by review, not by this
/// test. The `panicked at ... simulated` line in the output is this test's own
/// panic, deliberately raised.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn governor_pressure_fixtures_are_reaped_by_a_panicking_run() {
    use mysql::prelude::Queryable;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::AtomicU64;

    require_alive(LiveService::Mysql);

    let mut c = mysql_connect();
    let scratch = unique_name("rivet_qa_redo_guard");
    c.query_drop(format!(
        "CREATE TABLE {scratch} (id BIGINT AUTO_INCREMENT PRIMARY KEY, payload LONGBLOB) \
         ENGINE=InnoDB"
    ))
    .expect("create the guard-proof scratch table");

    let iterations = Arc::new(AtomicU64::new(0));
    let landed = Arc::new(AtomicU64::new(0));
    let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| {
        // Declaration order mirrors the redo-pressure test: the writer drops
        // FIRST (stopped and joined), the scratch table second.
        let _scratch_guard = MysqlTable::adopt(scratch.clone());
        let _writer = PressureWriter::spawn({
            let iterations = Arc::clone(&iterations);
            let landed = Arc::clone(&landed);
            let scratch = scratch.clone();
            move |stop| {
                let Ok(pool) = mysql::Pool::new(MYSQL_URL) else {
                    return;
                };
                let Ok(mut conn) = pool.get_conn() else {
                    return;
                };
                while !stop.load(Ordering::Relaxed) {
                    if conn
                        .query_drop(format!(
                            "INSERT INTO {scratch} (payload) VALUES (REPEAT(0x61, 1024))"
                        ))
                        .is_ok()
                    {
                        landed.fetch_add(1, Ordering::Relaxed);
                    }
                    // Counted unconditionally: this is the LIVENESS signal, and
                    // it must keep moving for a leaked writer even after the
                    // table it writes to has been dropped.
                    iterations.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        });
        // Let the writer really connect and land statements, so the reap below
        // grades a LIVE thread rather than one that never started.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while landed.load(Ordering::Relaxed) < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("simulated: the watchdog arm / a failed assertion / an `.expect` in the runner");
    }))
    .is_err();
    assert!(unwound, "the simulated panic must actually unwind");

    assert!(
        landed.load(Ordering::Relaxed) >= 2,
        "fixture went inert: the writer landed {} inserts (needs >= 2) — it never \
         connected, so the liveness assertion below would grade a thread that was \
         never running",
        landed.load(Ordering::Relaxed)
    );
    let tables: Vec<String> = c
        .query(format!("SHOW TABLES LIKE '{scratch}'"))
        .expect("list the scratch table");
    assert!(
        tables.is_empty(),
        "the scratch table {scratch} outlived a panicking run — a trailing DROP is \
         skipped by every panic path; it must be an RAII guard"
    );

    let before = iterations.load(Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(300));
    let after = iterations.load(Ordering::Relaxed);
    assert_eq!(
        before, after,
        "the pressure writer outlived a panicking run ({before} -> {after} loop \
         iterations after the unwind) — it must be stopped and joined by Drop, not \
         by a trailing stop.store()/join()"
    );
}

/// The governor SHEDS on SQL Server, under real log-flush pressure.
///
/// The last engine on the shed axis. MSSQL's foreign signal is
/// `Log Flush Waits/sec` (`_Total`) — a thread waiting for a redo flush to
/// complete, which is WRITE-driven and so cannot be moved by a read-only
/// export. Before this test the engine had only a must-not-shed canary plus a
/// readability assert, and both are satisfied by a counter that never moves —
/// the same hazard proven on MySQL, where a permanently-flat mutant left both
/// pre-existing canaries green.
///
/// The mover here is plain COMMIT rate: every committed transaction forces a
/// log flush, and a writer streaming ~200 KB rows one transaction at a time
/// produces roughly one wait per commit. Measured on the stand before writing
/// the test: `_Total` went 1273 -> 1681 across 400 commits in 1.8 s. Unlike
/// the MySQL twin this needs NO server-wide setting flip — commits alone do
/// it — so there is nothing to restore and no globals lock to take. The writer
/// targets a SCRATCH table, never the exported one: the pressure must be
/// FOREIGN, which is the point of the signal split.
///
/// Assertions in the order that makes each meaningful: the governor ARMED,
/// this test's OWN writer really committed, the counter really moved, it
/// BACKED OFF, and every row still round-tripped.
///
/// Attribution honesty, same shape as the MySQL twin but WEAKER on the counter
/// half, and the difference is measured rather than assumed. `Log Flush Waits`
/// is read for `instance_name = '_Total'`, i.e. EVERY database on the instance;
/// `quiet_window_guard` is taken only by the tests in this file (verified — no
/// other live file references it), so it serialises the governor tests against
/// each other and excludes nothing else on the shared server. A plain sibling
/// loop of 2 000 small committed INSERTs — the shape of any seeder in the live
/// suite — moves `_Total` by 2 004 (measured on the dev stand, 2026-08-14),
/// so NO threshold on this counter is sibling-proof: the old `>= 10` was
/// satisfied 200× over by that traffic, and even the `>= 100` below is not
/// immune to it. Raising it only excludes INCIDENTAL movement (a dead writer
/// under a quiet stand measured a delta of 1). The attributable half is
/// therefore the writer's own committed-batch count, which no sibling can
/// produce — that is the assertion the inert-fixture proof goes RED on.
///
/// The run goes through the rig's BOUNDED runner: a governor deadlock (the
/// regression `governor_does_not_deadlock_when_chunks_fail` exists for) must
/// fail this one test, not wedge the suite behind `quiet_window_guard` while
/// the writer hammers the shared server forever.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_governor_backs_off_under_real_log_flush_pressure() {
    // Same dedicated instance, opposite direction: this one needs its OWN
    // writer to be the thing moving the counter. On the shared server it failed
    // by going inert — "committed 3 40-transaction batches (needs >= 5)" — a
    // loaded runner starving the fixture rather than the product misbehaving.
    require_alive(LiveService::MssqlGovernor);
    let _quiet = quiet_window_guard();

    // The exact counter `MssqlSource::sample_governor_pressure` reads — the
    // `_Total` row, not a SUM over the per-database rows (a SUM double-counts,
    // and a dropped database SHRINKS it, reading as "pressure eased").
    const FLUSH_WAITS: &str = "SELECT cntr_value FROM sys.dm_os_performance_counters \
                               WHERE counter_name LIKE 'Log Flush Waits%' \
                               AND instance_name = '_Total'";

    const ROWS: i64 = 20_000;
    let table = seed_mssql_governor_numeric_table(ROWS);
    let scratch = format!("{}_flush", table.name());
    mssql_governor_exec(&format!(
        "IF OBJECT_ID('dbo.{scratch}') IS NOT NULL DROP TABLE dbo.{scratch}; \
         CREATE TABLE dbo.{scratch} (id INT IDENTITY PRIMARY KEY, payload VARBINARY(MAX))"
    ));
    // RAII, not a trailing DROP: the scratch table holds ~200 KB rows on a
    // SHARED server, and a trailing statement is skipped by every panic path
    // (a failed assertion, the watchdog below, a panicking writer join).
    let _scratch_guard = MssqlTable::adopt(format!("dbo.{scratch}"));

    // Background writer: one COMMIT per row, each carrying ~200 KB, so the log
    // manager flushes constantly and `Log Flush Waits` climbs pair-over-pair —
    // the condition `GovernorState::observe` needs to shed.
    //
    // It uses the ERROR-TOLERANT executor, not `mssql_exec`: this loop performs
    // hundreds of fresh TDS logins under load it creates itself, and every step
    // of the panicking executor is an `.expect` — one transient login failure or
    // deadlock-victim error would abort the thread, make the test's verdict
    // `writer thread`, and skip every real assertion. The returned bool keeps
    // the tolerance honest by feeding the attributable activation guard below.
    let writer_batches = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut writer = PressureWriter::spawn({
        let batches = Arc::clone(&writer_batches);
        let scratch = scratch.clone();
        move |stop| {
            while !stop.load(Ordering::Relaxed) {
                // 40 committed transactions per batch keeps the round-trip
                // count low while the commit RATE stays high.
                if mssql_governor_try_exec(&format!(
                    "SET NOCOUNT ON; \
                     DECLARE @i INT = 0; \
                     WHILE @i < 40 \
                     BEGIN \
                       BEGIN TRAN; \
                       INSERT INTO dbo.{scratch} (payload) VALUES \
                         (CONVERT(VARBINARY(MAX), REPLICATE(CAST('x' AS VARCHAR(MAX)), 200000))); \
                       COMMIT; \
                       SET @i = @i + 1; \
                     END; \
                     DELETE TOP (40) FROM dbo.{scratch}"
                )) {
                    batches.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });

    // Let the writer drive the counter before rivet starts, so the governor's
    // first sample PAIR already spans rising pressure.
    std::thread::sleep(Duration::from_millis(500));
    let waits_before = mssql_governor_query_i64(FLUSH_WAITS);

    let rig = Rig::mssql_governor_batch(table.name())
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  min_parallel: 2")
        .source_line("  batch_size: 250")
        .source_line("  throttle_ms: 100")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000")
        .export_line("parallel: 8");
    // Watchdog: same 120 s ceiling as the PG and MySQL twins. The writer is
    // reaped by `PressureWriter`'s Drop on every panic path (including the
    // `.expect`s inside the bounded runner), so a wedged governor cannot leave
    // a 200 KB-per-commit loop running against the shared server.
    let bounded = rig.run_with_envs_bounded(
        &[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")],
        Duration::from_secs(120),
    );

    let waits_after = mssql_governor_query_i64(FLUSH_WAITS);
    let writer_panicked = writer.reap();
    let batches = writer_batches.load(Ordering::Relaxed);

    let out = bounded.unwrap_or_else(|| {
        panic!(
            "the governed MSSQL run under log-flush pressure did not finish within 120s \
             (governor deadlock regression?); the writer had committed {batches} batches"
        )
    });
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    assert!(
        out.status.success(),
        "the governed run must still complete under log-flush pressure; stderr:\n{stderr}"
    );
    assert_governor_awake(&stderr, "mssql");
    // Activation guard BEFORE the shed assertion: an inert fixture must go RED
    // here rather than make the shed assertion untestable. Two halves — see the
    // doc block: the writer's own committed-batch count is attributable to THIS
    // test, the `_Total` delta is instance-wide.
    assert!(
        !writer_panicked,
        "the log-flush writer PANICKED, so the shed assertion below grades something \
         other than this fixture (it committed {batches} batches first)"
    );
    assert!(
        batches >= 5,
        "fixture went inert: this test's OWN writer committed {batches} 40-transaction \
         batches (needs >= 5) — it never connected or every batch failed, so any counter \
         movement below belongs to a concurrent sibling, not to this fixture"
    );
    let delta = waits_after.saturating_sub(waits_before);
    assert!(
        delta >= 100,
        "fixture went inert: Log Flush Waits (_Total) moved by {delta} across the run \
         (needs >= 100; ~408 measured for this shape over 400 commits), so the governor \
         had no rising foreign signal to react to and the shed assertion below would \
         grade nothing ({batches} writer batches landed)"
    );
    assert!(
        stderr.contains("backed off"),
        "under rising Log Flush Waits the governor must shed at least one worker; \
         counter moved by {delta} over {batches} writer batches; stderr:\n{stderr}"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        ROWS as usize,
        "every exported row must round-trip while the governor is shedding"
    );
}

/// The checkpoint twin of [`governor_backs_off_under_concurrent_write_pressure`]
/// — the LAST `gap:` cell in `docs/runner-coverage-matrix.yaml`, filled to its
/// own recipe.
///
/// `chunked_checkpoint`'s parallel half IS wired to the governor
/// (`GovernorHarness::arm`/`spawn_into`/`drain_into` in `parallel_checkpoint.rs`)
/// and had two OFFLINE proofs: a call-site pin over the source text, and the
/// pool shed mechanism against real permit guards. Neither can catch a run whose
/// SAMPLER never moves under real pressure — which is precisely what the
/// `chunked` cell's live test does for `exec.rs`. The end-to-end shed on this
/// runner had been observed only in an ad-hoc run that was never committed
/// (2026-08-14), so the matrix carried it as an admission rather than a cell.
///
/// The journal, not just stderr, is the oracle for the shed: `ParallelismAdjusted`
/// is drained BEFORE the runner's error/bail check precisely so the record
/// survives, and a log line proves the message was formatted while the journal
/// proves the event was RECORDED — the artifact an operator actually reads back.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn checkpoint_governor_backs_off_under_concurrent_write_pressure() {
    require_alive(LiveService::Postgres);
    let _quiet = quiet_window_guard();

    const ROWS: i64 = 20_000;
    let table = seed_pg_wide_table(ROWS, 1024);
    // Identical to the twin, plus the one line this cell exists for.
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, payload FROM {}", table.name()))
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line("  min_parallel: 2")
        .source_line("  batch_size: 250")
        .source_line("  throttle_ms: 100")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000")
        .export_line("parallel: 8")
        .export_line("chunk_checkpoint: true");

    let table_name = table.name().to_string();
    let mut writer = PressureWriter::spawn(move |stop| {
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
    });
    std::thread::sleep(Duration::from_millis(400)); // pre-warm the signal

    let out = rig.run_with_envs_bounded(
        &[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")],
        Duration::from_secs(120),
    );
    assert!(!writer.reap(), "the write-pressure writer thread PANICKED");
    let out = out.expect("governed checkpoint run under write pressure did not finish within 120s");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "governed checkpoint run must still complete; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("adaptive concurrency governor active"),
        "governor must arm on the PARALLEL checkpoint runner too; stderr:\n{stderr}"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        ROWS as usize,
        "every row must round-trip while the governor resizes the pool mid-run — a shed \
         that strands a chunk delivers a short export with a green exit"
    );

    // The RECORDED shed, read from the STORED journal rather than `rivet journal`.
    //
    // That command's human output renders a SUBSET — files, retries, quality
    // issues — and never prints a `ParallelismAdjusted` at all, so the first
    // version of this assertion read an empty result while stderr showed the shed
    // had happened, and would have blamed the runner for a rendering gap. The
    // stored `run_journal.journal_json` is what the run actually wrote, and it is
    // what any later reader (a support dump, a post-mortem) parses.
    let journal = StateDb::next_to_config(&rig.config_path()).latest_journal_json(table.name());
    let v: serde_json::Value = serde_json::from_str(&journal).expect("journal_json parses");
    let sheds = v["entries"]
        .as_array()
        .map(|evs| {
            evs.iter()
                .filter_map(|e| e.get("event")?.get("ParallelismAdjusted"))
                .filter(|a| a["to"].as_u64() < a["from"].as_u64())
                .count()
        })
        .unwrap_or(0);
    assert!(
        sheds > 0,
        "under rising checkpoint pressure the PARALLEL checkpoint runner must RECORD at \
         least one shed as a ParallelismAdjusted event with to < from. stderr showed a \
         back-off: {}. journal_json:\n{journal}",
        stderr.contains("backed off")
    );
}

/// `apply --pool` with the governor ARMED — the exact combination of the #229
/// field incident, never once run by a test.
///
/// The self-throttle fix is proven at the SINGLE-export level (per-engine
/// backs-off tests + idle canaries + the checkpoint runner's end-to-end shed),
/// and the pool's permit mechanics are unit-proven — but the 1h48m field loss
/// was a POOL run with adaptive on, and no live test combined the two: every
/// stand pool-split test runs with the governor disarmed. This is the pool-level
/// idle canary: two governed exports through one pool on an idle source must
/// arm, deliver everything, and never walk toward the floor (#229's shape),
/// judged by the same pure verdict the single-export canaries use.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pool_of_governed_exports_on_an_idle_source_delivers_all_and_never_walks_down() {
    require_alive(LiveService::Postgres);
    let _quiet = quiet_window_guard();

    const ROWS: i64 = 20_000;
    const MIN_PARALLEL: usize = 1;
    const CEILING: usize = 4;
    let table = seed_pg_wide_table(ROWS, 512);
    let sibling_q = format!("SELECT id FROM {} WHERE id <= 500", table.name());
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, payload FROM {}", table.name()))
        .mode("chunked")
        .source_line("tuning:")
        .source_line("  adaptive: true")
        .source_line(&format!("  min_parallel: {MIN_PARALLEL}"))
        .export_line("chunk_column: id")
        .export_line("chunk_size: 1000")
        .export_line(&format!("parallel: {CEILING}"))
        .export_line("parallel_safe: true")
        .also_export("pool_gov_sibling", &sibling_q)
        .also_export_line("parallel_safe: true");
    let cfg = rig.config_path();

    let out = run_rivet_env(
        &["apply", cfg.to_str().unwrap(), "--pool", "2"],
        &[("RUST_LOG", "info"), ("RIVET_GOVERNOR_INTERVAL_MS", "200")],
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "pooled governed apply must succeed; stderr:\n{stderr}"
    );

    // The governor must actually be ARMED inside the pool worker — a pool that
    // silently disarms adaptive would pass every other assertion here while
    // testing nothing about the incident's combination.
    assert!(
        stderr.contains("adaptive concurrency governor active"),
        "the governor must arm for the pooled chunked export; stderr:\n{stderr}"
    );

    // The #229 verdict, over the pool's merged stderr: transitions from both
    // exports interleave line-wise, and the pure verdict counts sheds against
    // recoveries — a descent that never recovers, or a floor touch, fails.
    let levels = governor_transitions(&stderr);
    assert!(
        !shed_never_recovered(&levels, MIN_PARALLEL, CEILING),
        "idle source under --pool: a shed that does not come back is #229 — the exact \
         field-incident combination (pool + adaptive). transitions: {levels:?}\nstderr:\n{stderr}"
    );

    // Delivery, by the independent oracle: every row of both exports.
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir()),
        ROWS as usize,
        "the governed pooled giant must deliver every row"
    );
    assert_eq!(
        duckdb_total_parquet_rows(&rig.out_dir_for("pool_gov_sibling")),
        500,
        "the sibling export must deliver every row too"
    );
}
