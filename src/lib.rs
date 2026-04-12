//! **Rivet** — CLI tool to export PostgreSQL and MySQL tables to Parquet/CSV files
//! (local, S3, GCS) with tuning profiles, preflight diagnostics, chunked parallelism,
//! retry logic, and SQLite-backed state tracking.
//!
//! This crate exposes its internals as public modules so that integration tests and
//! downstream tooling can reuse the building blocks.  The primary entry point for
//! end users is the `rivet` binary (see `src/main.rs`).

pub mod config;
pub mod destination;
pub mod enrich;
pub mod error;
pub mod format;
pub mod notify;
pub mod pipeline;
pub mod plan;
pub mod preflight;
pub mod quality;
pub mod resource;
pub mod source;
pub mod state;
pub mod tuning;
pub mod types;
