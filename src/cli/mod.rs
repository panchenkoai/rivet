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

/// Exit status of a command whose reader closed its stdout: 128 + SIGPIPE, what a shell reports for `yes | head`.
const CLOSED_STDOUT_EXIT: i32 = 141;

/// Whether a panic message is std's `print!` failing on a closed pipe (EPIPE is 32 on Linux and macOS).
fn is_closed_stdout_panic(msg: &str) -> bool {
    msg.contains("failed printing to stdout") && msg.contains("os error 32")
}

/// End the command with 141 and no panic report when the reader of its stdout went away (`| head`, `q` in a pager).
fn quiet_a_closed_stdout() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let msg = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        if is_closed_stdout_panic(msg) {
            std::process::exit(CLOSED_STDOUT_EXIT);
        }
        default(info)
    }));
}

/// The `rivet` binary's entry point: parse, dispatch, report a failure, exit with its class.
pub fn run_binary() {
    quiet_a_closed_stdout();
    crate::redact::install_logger();
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
