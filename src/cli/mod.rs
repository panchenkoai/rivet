//! CLI surface for the `rivet` binary.
//!
//! `main.rs` only calls [`run_binary`].
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

pub use args::parse_cli;
pub use dispatch::dispatch;

/// Where the process logger sends a line first: the in-process renderer's channel.
pub(crate) const LOG_ROUTE: crate::redact::LogRoute = crate::pipeline::ipc::route_log_line;

/// The `rivet` binary's entry point: parse, dispatch, report a failure, exit with its class.
pub fn run_binary() {
    crate::redact::install_logger(LOG_ROUTE);
    #[cfg(feature = "oracle")]
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cli = parse_cli();
    let json_errors = cli.json_errors;
    if let Err(e) = dispatch(cli) {
        let msg = crate::pipeline::parent_ui::sanitize_terminal(&crate::redact::redact_error(&e));
        let exit_class = crate::error::classify_exit(&e);
        let code = crate::error::error_code(&e);
        if json_errors {
            let mut obj = serde_json::json!({ "error": msg, "exit_class": exit_class });
            if let Some(c) = code {
                obj["code"] = serde_json::Value::String(c.to_string());
            }
            eprintln!("{obj}");
        } else if let Some(c) = code {
            eprintln!("Error: [{c}] {msg}");
        } else {
            eprintln!("Error: {msg}");
        }
        std::process::exit(exit_class);
    }
}
