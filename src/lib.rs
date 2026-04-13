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
pub mod format;
pub mod pipeline;
pub mod resource;
pub mod state;

// pub(crate) — internal implementation modules; not part of any external API contract
pub(crate) mod destination;
pub(crate) mod enrich;
pub(crate) mod error;
pub(crate) mod notify;
pub(crate) mod plan;
// preflight functions are invoked from main.rs (binary), not from the lib target;
// #[allow(dead_code)] suppresses false-positive lint in the lib compilation unit.
#[allow(dead_code)]
pub(crate) mod preflight;
pub(crate) mod quality;
pub(crate) mod source;
pub(crate) mod sql;
pub(crate) mod tuning;
pub(crate) mod types;
