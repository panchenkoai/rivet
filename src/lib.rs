//! **Rivet** — CLI tool to export PostgreSQL and MySQL tables to Parquet/CSV files
//! (local, S3, GCS) with tuning profiles, preflight diagnostics, chunked parallelism,
//! retry logic, and SQLite-backed state tracking.
//!
//! # Not a stable public API
//!
//! This crate (`rivet-cli` on crates.io) is a **CLI product**, not an embeddable library.
//! The library target exists solely to support Rust's integration test harness (`tests/`
//! requires a library to link against). Internal modules may change at any patch release
//! without notice. See `docs/adr/0002-cli-product-vs-library.md` for the full decision.
//!
//! **Stable for integration tests**: `config`, `format`, `pipeline`, `resource`, `state`.
//! All other modules are `pub(crate)` and not reachable from external consumers.

// Public — accessed by integration tests in tests/*.rs
pub mod config;
pub mod error;
pub mod format;
pub mod journal;
pub mod manifest;
pub mod pipeline;
pub mod resource;
pub mod source;
pub mod state;
pub mod tuning;
pub mod types;

// Public for the `rivet-mcp` binary in src/bin/. Not part of any external
// API contract — same "internal, may change at any patch" disclaimer applies.
pub mod mcp;

// pub(crate) — internal implementation modules; not part of any external API contract
pub(crate) mod destination;

/// Test-only re-exports of the otherwise `pub(crate)` `destination` module.
///
/// Integration tests in `tests/` need to construct a `Box<dyn Destination>`
/// to drive `pipeline::write_manifest` end-to-end, but the trait and factory
/// stay `pub(crate)` to keep the destination surface internal.  This window
/// re-exports just the two items required (`Destination`, `create_destination`)
/// behind a clearly-marked module name so the public crate API doesn't grow.
///
/// Not part of any external API contract — same "internal, may change at any
/// patch" disclaimer as the other lib modules in this file.
#[doc(hidden)]
pub mod destination_for_tests {
    pub use crate::destination::{Destination, create_destination};
}
pub(crate) mod enrich;
pub(crate) mod notify;
pub(crate) mod plan;
// Test-only fault-injection hook used by `tests/live_crash_recovery.rs`.
// Activated by the `RIVET_TEST_PANIC_AT` env var; no-op otherwise.  See
// module docs for details.
pub(crate) mod test_hook;
// Preflight diagnostics. The `check` and `doctor` entry points are invoked
// from `src/cli/dispatch.rs` (binary-only) and the internal
// `get_export_diagnostic` is used by `pipeline::plan_cmd` (lib + binary).
//
// We expose this as `pub mod` rather than `pub(crate) mod` so dead-code
// analysis sees the entry points as part of the crate's public API. Without
// that, the lib compilation unit (which doesn't depend on `cli::dispatch`)
// would mark the entire transitive surface as dead and force a blanket
// `#[allow(dead_code)]` that silences genuine dead code inside the module.
// Same "no external API contract" disclaimer as the other lib modules.
pub mod preflight;
pub(crate) mod quality;
pub(crate) mod sql;
