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
mod manifest;
mod notify;
mod pipeline;
mod plan;
mod preflight;
mod quality;
mod redact;
mod resource;
mod source;
mod sql;
mod state;
mod test_hook;
mod tuning;
mod types;

use clap::Parser;

fn main() {
    // F-NEW-F (0.7.5 audit): default log level was `error`, so every
    // `log::warn!(...)` in the codebase (unused --param, --force as
    // no-op, schema-drift advisories, redaction notices, plaintext
    // credentials in URL, ...) was silently dropped unless the
    // operator set RUST_LOG=warn.  Showing warns by default makes
    // these guardrails visible without changing anything for
    // operators that already override RUST_LOG.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    let cli = cli::Cli::parse();
    let json_errors = cli.json_errors;
    if let Err(e) = cli::dispatch(cli) {
        let msg = redact::redact_error(&e);
        if json_errors {
            eprintln!("{}", serde_json::json!({ "error": msg }));
        } else {
            eprintln!("Error: {msg}");
        }
        std::process::exit(1);
    }
}
