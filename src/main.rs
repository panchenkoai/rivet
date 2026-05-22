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
    env_logger::init();
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
