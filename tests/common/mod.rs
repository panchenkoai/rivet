//! Shared test helpers for live-infrastructure integration tests.
//!
//! By Rust convention this file lives at `tests/common/mod.rs` (not
//! `tests/common.rs`) so cargo does NOT compile it as its own test binary.
//! Each integration test file that needs these helpers opts in with
//! `mod common;` and then `use common::*;`.
//!
//! ## Module layout
//!
//! Helpers are split by *thing they talk to* so each integration test binary's
//! dependency on the live stack is obvious from its imports:
//!
//!   * `env`     — endpoints, `LiveService`, `require_alive` (the live gate)
//!   * `pg`      — Postgres connection + RAII table guard + seeders
//!   * `mysql`   — MySQL analogues of the above
//!   * `runner`  — driving the `rivet` / `rivet-mcp` binaries, output discovery
//!   * `toxi`    — Toxiproxy admin client + cross-binary `flock` guard
//!   * `storage` — MinIO / fake-gcs bucket provisioning
//!
//! Everything is re-exported here, so `use common::*;` still picks up the full
//! surface without callers needing to know the submodule layout.
//!
//! ## Why live tests are gated with `#[ignore]`
//!
//! Live tests require the docker-compose stack (see `docker-compose.yaml`) to
//! be running.  We do *not* silently skip them when infrastructure is
//! unreachable — that would let CI pass even when the live-test matrix is
//! actually broken.  Instead, live tests carry `#[ignore = "live: ..."]` so
//! the default `cargo test` run stays offline, and `cargo test -- --ignored`
//! (or `--include-ignored`) opts into live mode.
//!
//! When live tests run against a non-healthy stack they fail with an actionable
//! message (see `env::require_alive`) — not a panic from deep inside the
//! `postgres`/`mysql` driver.
//!
//! ## Isolation
//!
//! Every test must allocate its own unique resource names (table, export name,
//! destination prefix, S3 bucket path) so the suite can run with
//! `--test-threads=N` without false-sharing.  Use [`unique_name`] for that —
//! it combines PID and an atomic counter.

// Each integration-test binary uses only a subset of these helpers; the rest
// would otherwise trip `dead_code` (for the items) and `unused_imports` (for
// the glob re-exports).
#![allow(dead_code, unused_imports)]

use std::sync::atomic::{AtomicU64, Ordering};

// Submodules are private — only their public items are re-exported below.
// Keeping `mod mysql` private avoids shadowing the external `mysql` crate
// when downstream tests do `use common::*;`.  Same idea for `env` vs
// `std::env`.
mod bigquery;
mod canon;
mod clickhouse;
mod duckdb;
mod env;
mod mongo;
mod mssql;
mod mysql;
#[cfg(feature = "oracle")]
mod oracle;
mod parquet;
mod pg;
mod refusal;
mod registry;
mod rig;
mod runner;
mod sql_engine;
mod state;
mod storage;
mod toxi;
mod verify;

pub use bigquery::*;
pub use canon::*;
pub use clickhouse::*;
pub use duckdb::*;
pub use env::*;
pub use mongo::*;
pub use mssql::*;
pub use mysql::*;
#[cfg(feature = "oracle")]
pub use oracle::*;
pub use parquet::*;
pub use pg::*;
pub use refusal::{FAILED_RUN_LEAVES_ENV, Leftover};
pub use registry::*;
pub use rig::*;
pub use runner::*;
pub use sql_engine::*;
pub use state::*;
pub use storage::*;
pub use toxi::*;

// ─── Unique resource naming (races-free suite parallelism) ─────────────────

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Build a globally-unique identifier safe to use as a SQL table name or an
/// export name.  Combines process id and an atomic counter so parallel
/// `cargo test --test-threads=N` runs do not collide.
/// Announce that a live test did NOT run, in a form a release lane can COUNT.
///
/// `cargo test` prints `ok` for a test that returned early, so a skip and a pass
/// are indistinguishable in the output — the same shape that let 20 warehouse
/// cells report `20 passed` in 0.00s. The harness half of that was fixed by giving
/// its runners an explicit `Skipped` outcome; the live suite has no such channel,
/// so the marker IS the channel.
///
/// One token, `RIVET-SKIP`, and the test's own name, because the messages it
/// replaces said `SKIP`, `skip:`, and `skipping` in three different shapes and a
/// lane cannot grep for all of them. What a lane does with the count is its
/// business: assert a ceiling, diff against a previous run, or just print it —
/// none of which is possible while the skips are invisible.
///
/// Returns `()` so a call site reads `return skip_live("...")`.
pub fn skip_live(why: &str) {
    let who = std::thread::current()
        .name()
        .unwrap_or("<unnamed test>")
        .to_string();
    let line = format!("RIVET-SKIP {who} — {why}");
    eprintln!("{line}");
    // ...and to a FILE, because the eprintln alone is unreadable. libtest CAPTURES
    // and discards a PASSING test's stderr, and a skip is a pass — so without
    // `--nocapture`, which no workflow passes, the marker was as invisible as the
    // four spellings it replaced. MEASURED: 0 occurrences in a normal run, 1 with
    // --nocapture. A file survives capture, parallelism and the test binary exiting.
    //
    // Append-only and best-effort: a test must never fail because its skip could not
    // be recorded. `RIVET_SKIP_LOG` lets a lane put it somewhere it will look;
    // otherwise it lands beside the build output.
    let path = std::env::var("RIVET_SKIP_LOG").unwrap_or_else(|_| {
        format!(
            "{}/rivet-skips.log",
            std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into())
        )
    });
    if let Some(dir) = std::path::Path::new(&path).parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    use std::io::Write as _;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        // One write per record: `writeln!` issues two, and parallel tests interleaved them.
        let _ = f.write_all(format!("{line}\n").as_bytes());
    }
}

/// The Postgres state URL `RIVET_TEST_STATE_URL` names, or `None` after recording the skip.
pub fn pg_state_url() -> Option<String> {
    match std::env::var("RIVET_TEST_STATE_URL") {
        Ok(url) if url.starts_with("postgres") => Some(url),
        Ok(_) => {
            skip_live("RIVET_TEST_STATE_URL is not a postgres URL");
            None
        }
        Err(_) => {
            skip_live("RIVET_TEST_STATE_URL unset");
            None
        }
    }
}

/// The re-baseline remedy every CDC data-loss message prints, written out by hand (never read from `src/`).
pub const REBASELINE_REMEDY: &str = "Re-baseline the stream in one run: delete the checkpoint \
     file if there is one; move every file out of the export's destination (for a `tables:` \
     export, every table's directory under it): the parts there still hold rows the source may \
     no longer have, and each table's snapshot/_SUCCESS marker goes with them; delete the \
     export's `cdc_snapshot` rows (one per table) from the state DB; give the export \
     `cdc.initial: snapshot` if it has none; and if a warehouse load consumes this stream, \
     truncate its `<table>__changes` table before the next load. That run anchors FIRST and \
     re-reads every table after, so nothing falls between the two. A separate `mode: full` \
     export does not re-baseline the stream.";

/// Follow [`REBASELINE_REMEDY`] on a rig with a local destination, step by step as printed, then run once.
pub fn follow_rebaseline_remedy(rig: &mut Rig, has_baseline: bool) {
    apply_rebaseline_remedy(rig, has_baseline);
    rig.run_ok();
}

/// The steps of [`REBASELINE_REMEDY`] as printed, up to the run.
pub fn apply_rebaseline_remedy(rig: &mut Rig, has_baseline: bool) {
    let _ = std::fs::remove_file(rig.checkpoint());
    let out = rig.out_dir();
    std::fs::rename(&out, out.with_extension("pre-rebaseline"))
        .expect("move the destination's files aside");
    std::fs::create_dir_all(&out).expect("recreate the destination");
    let cleared = clear_cdc_snapshot(
        &rig.config_path(),
        rig.export_name(),
        &out.to_string_lossy(),
    );
    assert_eq!(
        cleared > 0,
        has_baseline,
        "fixture: {cleared} `cdc_snapshot` row(s) for an export whose baseline is {has_baseline}"
    );
    if !has_baseline {
        rig.amend_cdc_line("initial: snapshot");
    }
}

/// One source write on an `(id, v)` table, engine-neutral.
#[derive(Clone, Copy)]
pub enum Churn {
    Insert(i64),
    Update(i64),
    Delete(i64),
}

impl Churn {
    /// The statement for a SQL engine.
    pub fn sql(self, table: &str) -> String {
        match self {
            Churn::Insert(id) => format!("INSERT INTO {table} (id, v) VALUES ({id}, {id}0)"),
            Churn::Update(id) => format!("UPDATE {table} SET v = 99 WHERE id = {id}"),
            Churn::Delete(id) => format!("DELETE FROM {table} WHERE id = {id}"),
        }
    }
}

/// The text of a run's `RIVET_STATE_CDC_TABLE_REJOINED` refusal, from its code to the end.
pub fn rejoin_refusal(said: String) -> String {
    let at = said
        .find("[RIVET_STATE_CDC_TABLE_REJOINED]")
        .unwrap_or_else(|| panic!("the run names its refusal:\n{said}"));
    said[at..].to_string()
}

/// Every file under `dir`, recursively.
fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    for e in std::fs::read_dir(dir)
        .expect("read the directory")
        .flatten()
    {
        if e.path().is_dir() {
            found.extend(files_under(&e.path()));
        } else {
            found.push(e.path());
        }
    }
    found.sort();
    found
}

/// `tb` leaves `tables:`, changes, and comes back: refused twice with nothing written; the remedy as printed re-reads it and capture resumes (each green run is graded by the rig oracle).
pub fn a_table_put_back_is_refused_until_rebaselined(
    mut rig: Rig,
    [ta, tb, tc]: [&str; 3],
    exec: &mut dyn FnMut(&str, Churn),
) {
    for t in [ta, tb, tc] {
        exec(t, Churn::Insert(1));
        exec(t, Churn::Insert(3));
    }
    rig = rig.tables(&[ta, tb, tc]).cdc_line("initial: snapshot");
    rig.run_ok();
    rig = rig.tables(&[ta, tc]);
    exec(tb, Churn::Insert(2));
    exec(tb, Churn::Update(1));
    exec(tb, Churn::Delete(3));
    exec(ta, Churn::Insert(2));
    rig.run_ok();
    rig = rig.tables(&[ta, tb, tc]);
    let dir = rig.out_dir().join(tb);
    let before = files_under(&dir);
    let first = rejoin_refusal(rig.run_expect_fail());
    for said in [
        format!(
            "table `{tb}` is not among the tables this stream captured on its last run ({ta}, {tc})"
        ),
        format!("move every file out of `file://{}`", dir.display()),
        format!(
            "delete the `cdc_snapshot` row of export '{}', table `{tb}`",
            rig.export_name()
        ),
    ] {
        assert!(first.contains(&said), "the refusal says `{said}`:\n{first}");
    }
    assert_eq!(
        rejoin_refusal(rig.run_expect_fail()),
        first,
        "the second run refuses for the same reason"
    );
    assert_eq!(files_under(&dir), before, "a refused run writes nothing");
    std::fs::rename(&dir, dir.with_extension("before-rejoin")).expect("move the table's files out");
    let cleared = clear_cdc_snapshot(
        &rig.config_path(),
        rig.export_name(),
        &dir.to_string_lossy(),
    );
    assert_eq!(
        cleared, 1,
        "fixture: the one `cdc_snapshot` row of the table put back"
    );
    rig.run_ok();
    assert!(
        dir.join("snapshot").join("_SUCCESS").exists(),
        "the remedy's run took the table's baseline again"
    );
    exec(tb, Churn::Insert(4));
    rig.run_ok();
}

/// `table:` pointed at `tb` over `ta`'s baseline is refused twice; pointed back, the stream continues with the row written meanwhile.
pub fn a_table_switched_over_a_baseline_is_refused_and_switched_back_continues(
    mut rig: Rig,
    [ta, tb]: [&str; 2],
    repoint: &dyn Fn(Rig, &str) -> Rig,
    exec: &mut dyn FnMut(&str, Churn),
) {
    for t in [ta, tb] {
        exec(t, Churn::Insert(1));
    }
    rig = repoint(rig, ta).cdc_line("initial: snapshot");
    rig.run_ok();
    exec(ta, Churn::Insert(2));
    rig = repoint(rig, tb);
    let before = files_under(&rig.out_dir());
    let first = rejoin_refusal(rig.run_expect_fail());
    assert!(
        first.contains(&format!(
            "table `{tb}` is not among the tables this stream captured on its last run ({ta})"
        )),
        "the refusal names the table and the last capture:\n{first}"
    );
    assert_eq!(
        rejoin_refusal(rig.run_expect_fail()),
        first,
        "the second run refuses for the same reason"
    );
    assert_eq!(
        files_under(&rig.out_dir()),
        before,
        "a refused run writes nothing"
    );
    rig = repoint(rig, ta);
    rig.run_ok();
    assert!(
        files_under(&rig.out_dir()).len() > before.len(),
        "pointed back: the row written before the switch arrives as a change part"
    );
}

pub fn unique_name(prefix: &str) -> String {
    let c = NAME_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    format!("{prefix}_{pid}_{c}")
}

/// RAII cross-process lock for the suite's QUIET WINDOW: taken by every test
/// that measures a wall-clock ratio (the adaptive canaries), generates
/// deliberate source pressure (the governor backs-off drivers), OR flips a
/// GLOBAL on the shared :3306 batch server (binlog_transaction_compression,
/// sql_mode, tmp-storage) — one lock so all of them mutually exclude (r5
/// bughunt: per-variable locks let a :3306 global flip skew a concurrent
/// canary and contaminate its sessions). Cargo runs
/// integration binaries and threads in parallel; a heavy sibling starting
/// mid-A/B skews a timing bound, and one test's CHECKPOINT spam is another's
/// false foreign pressure — engine-specific locks cannot cover cross-engine
/// CPU noise (a PG driver flaked the MSSQL canary, 2026-08-13). Same
/// advisory `flock(2)` shape as `toxiproxy_guard`; take it FIRST, before any
/// narrower guard (consistent order, no deadlock).
pub struct QuietWindowGuard {
    _file: std::fs::File,
}

/// Cross-PROCESS serialization keyed by `name` — for shared-server GLOBAL
/// flips on a server that has NO timing canaries (currently only the MSSQL
/// CDC instance :1434, key "mssql_cdc"; the name is the SERVER, not the
/// variable). A `static Mutex` serializes only within one process, and the
/// canonical runner (nextest) puts every test in its OWN process, so a
/// per-process lock is a no-op exactly where it matters (r3 bughunt).
///
/// The KEY MUST IDENTIFY THE SERVER, not the global being flipped: two tests
/// flipping DIFFERENT globals on the SAME server still contaminate each
/// other's fresh sessions, so a per-variable name mutually excludes nothing
/// (r5 bughunt — binlog_compression, sql_mode and the governor tmp-storage
/// globals all live on :3306 and were under three disjoint locks). Every
/// :3306 GLOBAL flip therefore takes `quiet_window_guard` (the single shared
/// batch-server + timing lock) instead of a per-name key.
pub fn cross_process_serial(name: &str) -> QuietWindowGuard {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join(format!("rivet_qa_serial_{name}.lock"));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|e| panic!("open serial lock {}: {e}", path.display()));
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        panic!(
            "flock(LOCK_EX) on {} failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    QuietWindowGuard { _file: file }
}

/// The stand's pgBouncer for one cell alone, its one server connection made to forget every prepared statement.
///
/// pgBouncer keeps a prepared statement by its text and reuses it across clients, and rivet reads every table
/// through `FETCH n FROM _rivet`: a table exported earlier by another cell would describe this cell's cursor.
pub fn pgbouncer_alone() -> QuietWindowGuard {
    require_alive(LiveService::PgBouncer);
    let alone = cross_process_serial("pgbouncer");
    postgres::Client::connect(PGBOUNCER_URL, postgres::NoTls)
        .expect("pgbouncer")
        .batch_execute("DEALLOCATE ALL")
        .expect("DEALLOCATE ALL through pgbouncer");
    alone
}

/// RAII background-writer: a thread that loops until its stop flag flips, and
/// on Drop (INCLUDING a panic unwind) sets the flag and JOINS. The sustained-
/// writes CDC tests spawned a bare JoinHandle then called a panic-capable
/// `run_rivet_bounded` BEFORE their manual stop+join — a non-zero exit
/// unwound past the join, DETACHED the writer, and it kept INSERTing into a
/// table the slot/table guard was about to drop (metadata-lock race + foreign
/// write pressure that later CDC tests in the same binary measure). Declared
/// AFTER the table/slot guards so it drops (and stops the writer) FIRST.
/// r7 bughunt; same reap-on-Drop shape as governor PressureWriter.
pub struct BgWriter {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl BgWriter {
    pub fn spawn<F>(body: F) -> Self
    where
        F: FnOnce(&std::sync::atomic::AtomicBool) + Send + 'static,
    {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s2 = stop.clone();
        let handle = std::thread::spawn(move || body(&s2));
        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Stop + join now (idempotent) — for tests that must QUIESCE the writer
    /// before a post-drain assertion reads stable state. Drop then no-ops.
    pub fn stop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for BgWriter {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

pub fn quiet_window_guard() -> QuietWindowGuard {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("rivet_qa_quiet_window.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|e| panic!("open quiet-window lock {}: {e}", path.display()));
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        panic!(
            "flock(LOCK_EX) on {} failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    QuietWindowGuard { _file: file }
}

/// Peak resident memory of any child process this test binary has reaped, in bytes.
///
/// `getrusage(RUSAGE_CHILDREN)` reports the MAXIMUM over all reaped children rather
/// than the last one's, so it is monotonic — which is exactly the shape a ceiling
/// assertion wants: "no rivet this test ever spawned went past X". It cannot
/// attribute a peak to one child, and does not need to.
///
/// `ru_maxrss` is BYTES on macOS and KILOBYTES on Linux — one field with two
/// meanings, and reading it wrong is a 1024× error in whichever direction hides the
/// failure. Normalised here so no caller has to remember.
#[cfg(unix)]
pub fn peak_child_rss_bytes() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, &mut ru) } != 0 {
        return 0;
    }
    let raw = ru.ru_maxrss.max(0) as u64;
    if cfg!(target_os = "macos") {
        raw
    } else {
        raw * 1024
    }
}

#[cfg(not(unix))]
pub fn peak_child_rss_bytes() -> u64 {
    0
}
