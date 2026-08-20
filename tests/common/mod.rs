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
mod clickhouse;
mod duckdb;
mod env;
mod mongo;
mod mssql;
mod mysql;
mod parquet;
mod pg;
mod rig;
mod runner;
mod state;
mod storage;
mod toxi;

pub use clickhouse::*;
pub use duckdb::*;
pub use env::*;
pub use mongo::*;
pub use mssql::*;
pub use mysql::*;
pub use parquet::*;
pub use pg::*;
pub use rig::*;
pub use runner::*;
pub use state::*;
pub use storage::*;
pub use toxi::*;

// ─── Unique resource naming (races-free suite parallelism) ─────────────────

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Build a globally-unique identifier safe to use as a SQL table name or an
/// export name.  Combines process id and an atomic counter so parallel
/// `cargo test --test-threads=N` runs do not collide.
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
