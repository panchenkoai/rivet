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
mod test_hook;
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
    #[command(group = clap::ArgGroup::new("source_spec").required(true).multiple(false))]
    Init {
        /// Database URL (postgresql:// or mysql://). Visible in shell history / `ps`;
        /// prefer `--source-env` or `--source-file` for anything other than local dev.
        #[arg(long, group = "source_spec")]
        source: Option<String>,
        /// Name of an environment variable holding the database URL (e.g. DATABASE_URL).
        /// The URL never touches the command line.
        #[arg(long, value_name = "ENV_VAR", group = "source_spec")]
        source_env: Option<String>,
        /// Path to a file containing just the database URL (one line).
        /// Credentials stay on disk instead of entering the process command line.
        #[arg(long, value_name = "PATH", group = "source_spec")]
        source_file: Option<String>,
        /// Single table, optionally schema-qualified (e.g. public.orders). Omit to emit all tables/views in a Postgres schema or MySQL database.
        #[arg(long)]
        table: Option<String>,
        /// PostgreSQL: schema to export (default public). MySQL: database name if missing from the URL, or override URL database.
        #[arg(long)]
        schema: Option<String>,
        /// Write output to this file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
        /// Emit a machine-readable JSON discovery artifact (Epic B) instead of a YAML scaffold.
        /// Includes row estimates, size bytes, ranked cursor candidates, chunk candidates, and advisory notes.
        #[arg(long)]
        discover: bool,
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
    /// Targeted repair of chunks flagged by reconcile: emit a repair plan, or re-export only mismatched ranges (Epic H).
    Repair {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Export name to repair (must be `mode: chunked`)
        #[arg(short, long)]
        export: String,
        /// Path to a reconcile JSON report produced by `rivet reconcile --format json`.
        /// Omit to run reconcile in-process against the latest chunk run.
        #[arg(long)]
        report: Option<String>,
        /// Actually re-export the affected chunks. Without this flag, the plan is printed and nothing is executed.
        #[arg(long)]
        execute: bool,
        /// Output format for plan / report
        #[arg(long, default_value = "pretty")]
        format: ReconcileFormat,
        /// Write plan / report JSON to this file (with `--format json`)
        #[arg(short, long)]
        output: Option<String>,
        /// Query parameter: key=value (repeatable)
        #[arg(short, long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
    },
    /// Partition/window reconciliation: re-count per-partition on source and report mismatches (Epic F).
    /// Requires a chunked export previously run with `chunk_checkpoint: true`.
    Reconcile {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Export name to reconcile (must be `mode: chunked`)
        #[arg(short, long)]
        export: String,
        /// Output format: "pretty" (human summary) or "json" (machine-readable report)
        #[arg(long, default_value = "pretty")]
        format: ReconcileFormat,
        /// Write report JSON to this file (only with `--format json`)
        #[arg(short, long)]
        output: Option<String>,
        /// Query parameter: key=value (repeatable)
        #[arg(short, long = "param", value_name = "KEY=VALUE")]
        params: Vec<String>,
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
    /// Show committed / verified export boundaries (Epic G / ADR-0008)
    Progression {
        #[arg(short, long)]
        config: String,
        /// Show progression for a specific export
        #[arg(short, long)]
        export: Option<String>,
    },
}

#[derive(clap::ValueEnum, Clone)]
enum PlanFormat {
    /// Human-readable summary printed to stdout
    Pretty,
    /// Pretty-printed JSON (written to --output file or stdout)
    Json,
}

#[derive(clap::ValueEnum, Clone)]
enum ReconcileFormat {
    Pretty,
    Json,
}

fn parse_params(raw: &[String]) -> std::collections::HashMap<String, String> {
    raw.iter()
        .filter_map(|s| s.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Resolve the DB URL for `rivet init` from exactly one of `--source`,
/// `--source-env`, or `--source-file`.
///
/// Clap's argument group already enforces mutual exclusion and
/// at-least-one-required. This function only performs the side-effect of
/// reading env / file so that credentials never have to appear on the command
/// line (which would leak into shell history, `ps`, and `/proc/<pid>/cmdline`).
fn resolve_init_source(
    source: Option<String>,
    source_env: Option<String>,
    source_file: Option<String>,
) -> Result<String> {
    if let Some(url) = source {
        return Ok(url);
    }
    if let Some(var) = source_env {
        return std::env::var(&var)
            .map_err(|_| anyhow::anyhow!("--source-env '{}' is not set in the environment", var));
    }
    if let Some(path) = source_file {
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("cannot read --source-file '{}': {}", path, e))?;
        return Ok(raw.trim().to_string());
    }
    // Unreachable: clap ArgGroup enforces `required = true`.
    anyhow::bail!("--source, --source-env, or --source-file is required")
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
            source_env,
            source_file,
            table,
            schema,
            output,
            discover,
        } => {
            let fmt = if discover {
                init::InitFormat::DiscoveryJson
            } else {
                init::InitFormat::Yaml
            };
            let source_url = resolve_init_source(source, source_env, source_file)?;
            init::init(
                &source_url,
                table.as_deref(),
                schema.as_deref(),
                output.as_deref(),
                fmt,
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
        Commands::Reconcile {
            config,
            export,
            format,
            output,
            params,
        } => {
            let p = parse_params(&params);
            let p = if p.is_empty() { None } else { Some(p) };
            let fmt = match format {
                ReconcileFormat::Pretty => pipeline::ReconcileOutputFormat::Pretty,
                ReconcileFormat::Json => pipeline::ReconcileOutputFormat::Json(output),
            };
            pipeline::run_reconcile_command(&config, &export, p.as_ref(), fmt)?;
        }
        Commands::Repair {
            config,
            export,
            report,
            execute,
            format,
            output,
            params,
        } => {
            let p = parse_params(&params);
            let p = if p.is_empty() { None } else { Some(p) };
            let source = match report {
                Some(path) => pipeline::RepairReportSource::File(path),
                None => pipeline::RepairReportSource::Auto,
            };
            let fmt = match format {
                ReconcileFormat::Pretty => pipeline::RepairOutputFormat::Pretty,
                ReconcileFormat::Json => pipeline::RepairOutputFormat::Json(output),
            };
            pipeline::run_repair_command(&config, &export, p.as_ref(), source, execute, fmt)?;
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
            StateAction::Progression { config, export } => {
                pipeline::show_progression(&config, export.as_deref())?;
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
