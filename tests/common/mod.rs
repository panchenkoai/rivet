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
mod env;
mod mysql;
mod pg;
mod runner;
mod storage;
mod toxi;

pub use env::*;
pub use mysql::*;
pub use pg::*;
pub use runner::*;
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
