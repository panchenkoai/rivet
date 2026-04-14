#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod config;
mod destination;
mod enrich;
mod error;
mod format;
mod init;
mod notify;
mod pipeline;
mod plan;
mod preflight;
mod quality;
mod resource;
mod source;
mod sql;
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
        /// Reconcile: run COUNT(*) on source query and compare with exported rows
        #[arg(long)]
        reconcile: bool,
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
    /// Generate a config scaffold from a live database (connect + introspect)
    Init {
        /// Database URL (postgresql:// or mysql://)
        #[arg(long)]
        source: String,
        /// Single table, optionally schema-qualified (e.g. public.orders). Omit to emit all tables/views in a Postgres schema or MySQL database.
        #[arg(long)]
        table: Option<String>,
        /// PostgreSQL: schema to export (default public). MySQL: database name if missing from the URL, or override URL database.
        #[arg(long)]
        schema: Option<String>,
        /// Write output to this file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Generate an execution plan artifact (no data exported)
    Plan {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Plan only a specific export by name
        #[arg(short, long)]
        export: Option<String>,
        /// Query parameter: key=value (repeatable)
        #[arg(short, long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
        /// Write plan JSON to this file (default: print summary to stdout)
        #[arg(short, long)]
        output: Option<String>,
        /// Output format: "pretty" (human summary) or "json" (machine-readable)
        #[arg(long, default_value = "pretty")]
        format: PlanFormat,
    },
    /// Execute a previously-generated plan artifact
    Apply {
        /// Path to plan JSON file produced by `rivet plan`
        plan_file: String,
        /// Skip staleness check (allow plans older than 24 h)
        #[arg(long)]
        force: bool,
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

#[derive(clap::ValueEnum, Clone)]
enum PlanFormat {
    /// Human-readable summary printed to stdout
    Pretty,
    /// Pretty-printed JSON (written to --output file or stdout)
    Json,
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
            reconcile,
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
                reconcile,
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
        Commands::Init {
            source,
            table,
            schema,
            output,
        } => {
            init::init(
                &source,
                table.as_deref(),
                schema.as_deref(),
                output.as_deref(),
            )?;
        }
        Commands::Plan {
            config,
            export,
            params,
            output,
            format,
        } => {
            let p = parse_params(&params);
            let p = if p.is_empty() { None } else { Some(p) };
            let fmt = match format {
                PlanFormat::Pretty => pipeline::PlanOutputFormat::Pretty,
                PlanFormat::Json => pipeline::PlanOutputFormat::Json(output),
            };
            pipeline::run_plan_command(&config, export.as_deref(), p.as_ref(), fmt)?;
        }
        Commands::Apply { plan_file, force } => {
            pipeline::run_apply_command(&plan_file, force)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_params_basic() {
        let input = vec!["KEY=value".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.get("KEY").unwrap(), "value");
    }

    #[test]
    fn parse_params_equals_in_value() {
        let input = vec!["FILTER=x=1 AND y=2".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.get("FILTER").unwrap(), "x=1 AND y=2");
    }

    #[test]
    fn parse_params_multiple() {
        let input = vec!["A=1".to_string(), "B=2".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("A").unwrap(), "1");
        assert_eq!(result.get("B").unwrap(), "2");
    }

    #[test]
    fn parse_params_empty_input() {
        let result = parse_params(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_params_no_equals_skipped() {
        let input = vec!["NO_EQUALS_HERE".to_string()];
        let result = parse_params(&input);
        assert!(result.is_empty());
    }

    #[test]
    fn parse_params_empty_value() {
        let input = vec!["KEY=".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.get("KEY").unwrap(), "");
    }

    #[test]
    fn parse_params_duplicate_last_wins() {
        let input = vec!["K=first".to_string(), "K=second".to_string()];
        let result = parse_params(&input);
        assert_eq!(result.get("K").unwrap(), "second");
    }
}
