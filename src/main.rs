mod config;
mod destination;
mod enrich;
mod error;
mod format;
mod notify;
mod pipeline;
mod preflight;
mod quality;
mod resource;
mod source;
mod state;
mod tuning;
mod types;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use error::Result;

#[derive(Parser)]
#[command(name = "rivet", version, about = "Export data from databases to files")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run export jobs defined in config
    Run {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Run only a specific export by name
        #[arg(short, long)]
        export: Option<String>,
        /// Validate output files after writing
        #[arg(long)]
        validate: bool,
        /// Resume a chunked export with `chunk_checkpoint: true` (same query/chunk_column/chunk_size)
        #[arg(long)]
        resume: bool,
        /// Run all exports from the config concurrently (ignored with `--export`; needs 2+ exports)
        #[arg(long)]
        parallel_exports: bool,
        /// Run each export as a separate `rivet` child process (parallel; true per-export peak RSS; more overhead than threads)
        #[arg(long)]
        parallel_export_processes: bool,
        /// Query parameter: key=value (repeatable, substitutes ${key} in queries)
        #[arg(short, long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
    },
    /// Preflight check: diagnose source health for each export
    Check {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Check only a specific export by name
        #[arg(short, long)]
        export: Option<String>,
        /// Query parameter: key=value (repeatable, substitutes ${key} in queries)
        #[arg(short, long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
    },
    /// Verify source and destination auth before running exports
    Doctor {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
    },
    /// Manage export state
    State {
        #[command(subcommand)]
        action: StateAction,
    },
    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Show export metrics history
    Metrics {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Show metrics for a specific export
        #[arg(short, long)]
        export: Option<String>,
        /// Number of recent runs to show
        #[arg(short, long, default_value = "20")]
        last: usize,
    },
}

#[derive(Subcommand)]
enum StateAction {
    /// Show current state for all exports
    Show {
        #[arg(short, long)]
        config: String,
    },
    /// Reset state for an export
    Reset {
        #[arg(short, long)]
        config: String,
        /// Export name to reset
        #[arg(short, long)]
        export: String,
    },
    /// Show file manifest (files produced by exports)
    Files {
        #[arg(short, long)]
        config: String,
        /// Show files for a specific export
        #[arg(short, long)]
        export: Option<String>,
        /// Number of recent files to show
        #[arg(short, long, default_value = "50")]
        last: usize,
    },
    /// Clear persisted chunk plans (SQLite) for an export
    ResetChunks {
        #[arg(short, long)]
        config: String,
        #[arg(short, long)]
        export: String,
    },
    /// Show chunk checkpoint status for an export
    Chunks {
        #[arg(short, long)]
        config: String,
        #[arg(short, long)]
        export: String,
    },
}

fn parse_params(raw: &[String]) -> std::collections::HashMap<String, String> {
    raw.iter()
        .filter_map(|s| s.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            config,
            export,
            validate,
            resume,
            parallel_exports,
            parallel_export_processes,
            params,
        } => {
            let p = parse_params(&params);
            let p = if p.is_empty() { None } else { Some(p) };
            pipeline::run(
                &config,
                export.as_deref(),
                validate,
                resume,
                p.as_ref(),
                parallel_exports,
                parallel_export_processes,
            )?;
        }
        Commands::Check {
            config,
            export,
            params,
        } => {
            let p = parse_params(&params);
            let p = if p.is_empty() { None } else { Some(p) };
            preflight::check(&config, export.as_deref(), p.as_ref())?;
        }
        Commands::Doctor { config } => {
            preflight::doctor(&config)?;
        }
        Commands::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "rivet", &mut std::io::stdout());
        }
        Commands::Metrics {
            config,
            export,
            last,
        } => {
            pipeline::show_metrics(&config, export.as_deref(), last)?;
        }
        Commands::State { action } => match action {
            StateAction::Show { config } => {
                pipeline::show_state(&config)?;
            }
            StateAction::Reset { config, export } => {
                pipeline::reset_state(&config, &export)?;
            }
            StateAction::Files {
                config,
                export,
                last,
            } => {
                pipeline::show_files(&config, export.as_deref(), last)?;
            }
            StateAction::ResetChunks { config, export } => {
                pipeline::reset_chunk_checkpoint(&config, &export)?;
            }
            StateAction::Chunks { config, export } => {
                pipeline::show_chunk_checkpoint(&config, &export)?;
            }
        },
    }

    Ok(())
}
