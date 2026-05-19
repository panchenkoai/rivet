//! CLI surface for the `rivet` binary.
//!
//! `main.rs` only sees [`Cli`] (parser type) and [`dispatch`] (entry point).
//! Internally the module is split into four single-purpose siblings so the
//! 900-line monolith stays out of `main.rs`:
//!
//! - [`args`] — clap derive types (`Cli`, `Commands`, `StateAction`,
//!   `PlanFormat`, `ReconcileFormat`); pure grammar, no behavior.
//! - [`validate`] — cross-flag invariants that clap cannot express.
//! - [`params`] — `--param KEY=VALUE` parsing and `--source*` resolution.
//! - [`dispatch`] — the `match` that routes each parsed subcommand to its
//!   pipeline/init/preflight entry point.
//!
//! Each submodule keeps its own unit tests, so the suite is split along the
//! same seams as the production code.

mod args;
mod dispatch;
mod params;
mod validate;

pub use args::Cli;
pub use dispatch::dispatch;
