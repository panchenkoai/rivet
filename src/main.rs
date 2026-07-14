#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod cli;
mod config;
mod destination;
mod enrich;
mod error;
mod format;
mod init;
mod journal;
// The load layer is a `pub` library API; the thin `rivet load` CLI path exercises
// only part of it, so the loader's builder methods / report fields read back from
// cost views are dead *in the binary* while live in the library. Allow it here so
// the public API stays intact without a binary-only dead-code warning.
#[allow(dead_code)]
mod load;
mod manifest;
mod notify;
mod pipeline;
mod plan;
mod preflight;
mod quality;
mod redact;
mod resource;
mod scalar;
mod source;
mod sql;
mod state;
mod test_hook;
mod tuning;
mod types;

fn main() {
    // F-NEW-F (0.7.5 audit): default log level was `error`, so every
    // `log::warn!(...)` in the codebase (unused --param, --force as
    // no-op, schema-drift advisories, redaction notices, plaintext
    // credentials in URL, ...) was silently dropped unless the
    // operator set RUST_LOG=warn.  Showing warns by default makes
    // these guardrails visible without changing anything for
    // operators that already override RUST_LOG.
    // Credential-redaction invariant (ADR-0014): the redact module names "logs"
    // in its scope, but the `log::*` macros do not pass through the artifact-path
    // redaction wired at the error/summary call sites — a `log::warn!("…{e}", e)`
    // that captured a `scheme://user:password@host` connect error would print the
    // password to stderr (and `main` defaults the filter to `warn`, so these
    // lines are shown). Route every formatted line through `redacted_log_line` so
    // the sink itself is the chokepoint: no call site has to remember.
    use std::io::Write;
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format(|buf, record| {
            let line = redact::redacted_log_line(
                &buf.timestamp().to_string(),
                record.level().as_str(),
                record.target(),
                &record.args().to_string(),
            );
            writeln!(buf, "{line}")
        })
        .init();
    let cli = cli::parse_cli();
    let json_errors = cli.json_errors;
    if let Err(e) = cli::dispatch(cli) {
        // redact strips credentials; sanitize_terminal strips ANSI/OSC control
        // bytes a malicious source DB can embed in an error string (V9/CWE-150)
        // so the top-level error line cannot rewrite/clear the operator terminal.
        let msg = crate::pipeline::parent_ui::sanitize_terminal(&redact::redact_error(&e));
        // Machine-actionable exit-code taxonomy (see `error::ExitClass`): a
        // scheduler branches on the code (2=retryable, 3=data-integrity,
        // 4=schema-drift, 1=generic) instead of grepping `msg`.
        let exit_class = crate::error::classify_exit(&e);
        // A config/source failure tagged with a stable `RIVET_*` code surfaces it
        // for greppable tooling: a `code` field in JSON, a `[CODE]` text prefix.
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
