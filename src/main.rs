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
    /// Output errors as {"error":"..."} JSON to stderr; useful for machine-readable orchestration
    #[arg(long, global = true)]
    json_errors: bool,
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
        /// Write the run aggregate summary as JSON to this file (in addition to .rivet_state.db)
        #[arg(long, value_name = "PATH")]
        summary_output: Option<String>,
        /// Print the run aggregate summary as JSON to stdout at the end of the run
        #[arg(long)]
        json: bool,
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
        /// Show per-column type fidelity report (source type → Rivet type → Arrow type)
        #[arg(long)]
        type_report: bool,
        /// Fail with non-zero exit code if any column has an unsafe type mapping
        #[arg(long)]
        strict: bool,
        /// Output type report as JSON (implies --type-report)
        #[arg(long)]
        json: bool,
        /// Check compatibility against a target warehouse (e.g. bigquery)
        #[arg(long, value_name = "TARGET")]
        target: Option<String>,
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
        /// Mutually exclusive with the YAML-only `--gcs-bucket` / `--s3-bucket` flags.
        #[arg(
            long,
            conflicts_with_all = ["gcs_bucket", "gcs_credentials_file", "s3_bucket", "s3_region"]
        )]
        discover: bool,
        /// Scaffold `destination: type: gcs` with this bucket (each export gets `prefix: exports/<table>/`).
        /// Incompatible with `--s3-bucket` and `--discover`.
        #[arg(long = "gcs-bucket", value_name = "NAME", conflicts_with = "s3_bucket")]
        gcs_bucket: Option<String>,
        /// Optional path for `credentials_file:` on GCS scaffolds. Omit entirely to use ADC
        /// (`gcloud auth application-default login`) or `GOOGLE_APPLICATION_CREDENTIALS` — no key in YAML.
        #[arg(
            long = "gcs-credentials-file",
            value_name = "PATH",
            requires = "gcs_bucket"
        )]
        gcs_credentials_file: Option<String>,
        /// Scaffold `destination: type: s3` with this bucket (each export gets `prefix: exports/<table>/`).
        /// Incompatible with `--gcs-bucket` and `--discover`.
        #[arg(long = "s3-bucket", value_name = "NAME")]
        s3_bucket: Option<String>,
        /// Optional AWS region for S3 scaffolds (when using `--s3-bucket`).
        #[arg(long = "s3-region", value_name = "REGION", requires = "s3_bucket")]
        s3_region: Option<String>,
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
    /// Inspect structured run journal (events, files, retries, quality issues)
    Journal {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Export name to show journal for
        #[arg(short, long)]
        export: String,
        /// Number of recent runs to show (newest first)
        #[arg(short, long, default_value = "5")]
        last: usize,
        /// Show journal for a specific run_id instead of recent runs
        #[arg(long, value_name = "RUN_ID")]
        run_id: Option<String>,
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
    /// Clear persisted chunk checkpoint rows (`chunk_run` / `chunk_task`).
    ResetChunks {
        #[arg(short, long)]
        config: String,
        /// Export whose chunk checkpoints should be cleared (same as `chunk_checkpoint` runs).
        #[arg(
            short,
            long,
            conflicts_with = "stuck_checkpoints",
            required_unless_present = "stuck_checkpoints"
        )]
        export: Option<String>,
        /// Reset checkpoints for **every export named in this config** that currently has
        /// `chunk_run.status = 'in_progress'` (crash, SIGKILL, stale concurrent worker).
        ///
        /// Ignores exports whose latest chunk run already finished (`completed`). Runs listed in the
        /// database but removed from the YAML are skipped with a printed note.
        ///
        /// Alias `--failed` refers to “checkpoint state stuck”, not HTTP-style failures or metric rows.
        #[arg(
            long,
            visible_alias = "failed",
            conflicts_with = "export",
            required_unless_present = "export"
        )]
        stuck_checkpoints: bool,
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

fn validate_cli(cmd: &Commands) -> Result<()> {
    match cmd {
        Commands::Run {
            parallel_exports,
            parallel_export_processes,
            ..
        } => {
            if *parallel_exports && *parallel_export_processes {
                anyhow::bail!(
                    "--parallel-exports and --parallel-export-processes are mutually exclusive; \
                     choose one parallelism mode"
                );
            }
        }
        Commands::Plan {
            format: PlanFormat::Pretty,
            output: Some(_),
            ..
        } => {
            anyhow::bail!("--output requires --format json (output is ignored in pretty mode)");
        }
        Commands::Reconcile {
            format: ReconcileFormat::Pretty,
            output: Some(_),
            ..
        } => {
            anyhow::bail!("--output requires --format json (output is ignored in pretty mode)");
        }
        Commands::Repair {
            format: ReconcileFormat::Pretty,
            output: Some(_),
            ..
        } => {
            anyhow::bail!("--output requires --format json (output is ignored in pretty mode)");
        }
        _ => {}
    }
    Ok(())
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

fn main() {
    env_logger::init();
    let cli = Cli::parse();
    let json_errors = cli.json_errors;
    if let Err(e) = validate_cli(&cli.command).and_then(|_| run(cli.command)) {
        if json_errors {
            eprintln!("{}", serde_json::json!({"error": format!("{e:#}")}));
        } else {
            eprintln!("Error: {e:#}");
        }
        std::process::exit(1);
    }
}

fn run(command: Commands) -> Result<()> {
    match command {
        Commands::Run {
            config,
            export,
            validate,
            reconcile,
            resume,
            parallel_exports,
            parallel_export_processes,
            summary_output,
            json,
            params,
        } => {
            let p = parse_params(&params);
            let p = if p.is_empty() { None } else { Some(p) };
            let summary_output_path = summary_output.as_ref().map(std::path::PathBuf::from);
            pipeline::run(
                &config,
                export.as_deref(),
                validate,
                reconcile,
                resume,
                p.as_ref(),
                parallel_exports,
                parallel_export_processes,
                summary_output_path.as_deref(),
                json,
            )?;
        }
        Commands::Check {
            config,
            export,
            params,
            type_report,
            strict,
            json,
            target,
        } => {
            let p = parse_params(&params);
            let p = if p.is_empty() { None } else { Some(p) };
            let tgt = target
                .as_deref()
                .and_then(crate::types::target::ExportTarget::from_str);
            preflight::check(
                &config,
                export.as_deref(),
                p.as_ref(),
                type_report || json || tgt.is_some(),
                strict,
                json,
                tgt,
            )?;
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
            gcs_bucket,
            gcs_credentials_file,
            s3_bucket,
            s3_region,
        } => {
            let fmt = if discover {
                init::InitFormat::DiscoveryJson
            } else {
                init::InitFormat::Yaml
            };
            let source_url = resolve_init_source(source, source_env, source_file)?;
            let yaml_dest = init::InitYamlDestination {
                gcs_bucket,
                gcs_credentials_file,
                s3_bucket,
                s3_region,
            };
            init::init(
                &source_url,
                table.as_deref(),
                schema.as_deref(),
                output.as_deref(),
                fmt,
                yaml_dest,
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
        Commands::Journal {
            config,
            export,
            last,
            run_id,
        } => {
            pipeline::show_journal(&config, &export, last, run_id.as_deref())?;
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
            StateAction::ResetChunks {
                config,
                export,
                stuck_checkpoints,
            } => {
                if stuck_checkpoints {
                    pipeline::reset_chunk_checkpoints_stuck(&config)?;
                } else if let Some(name) = export {
                    pipeline::reset_chunk_checkpoint(&config, &name)?;
                }
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

    /// Helper: try to parse a `rivet init …` invocation through clap. The leading
    /// `["rivet", "init"]` is added so we exercise the real subcommand router.
    /// Returns the `clap::error::ErrorKind` so callers can assert without needing
    /// a `Debug` impl on `Cli`.
    fn init_parse_kind(extra_args: &[&str]) -> std::result::Result<(), clap::error::ErrorKind> {
        let mut argv = vec!["rivet", "init"];
        argv.extend(extra_args);
        match Cli::try_parse_from(argv) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.kind()),
        }
    }

    fn assert_init_err(extra_args: &[&str], expected: clap::error::ErrorKind, what: &str) {
        match init_parse_kind(extra_args) {
            Ok(()) => panic!("{what}: expected clap to reject, but parsing succeeded"),
            Err(kind) => assert_eq!(kind, expected, "{what}"),
        }
    }

    fn assert_init_ok(extra_args: &[&str], what: &str) {
        if let Err(kind) = init_parse_kind(extra_args) {
            panic!("{what}: expected clap to accept, got error kind {kind:?}");
        }
    }

    fn state_reset_chunks_parse_from(
        extra_after_config: &[&str],
    ) -> std::result::Result<(), clap::error::ErrorKind> {
        let mut argv = vec!["rivet", "state", "reset-chunks", "--config", "c.yaml"];
        argv.extend(extra_after_config.iter().copied());
        match Cli::try_parse_from(argv) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.kind()),
        }
    }

    #[test]
    fn state_reset_chunks_accepts_export_or_stuck_or_failed_alias() {
        assert!(state_reset_chunks_parse_from(&["--export", "x"]).is_ok());
        assert!(state_reset_chunks_parse_from(&["--stuck-checkpoints"]).is_ok());
        assert!(state_reset_chunks_parse_from(&["--failed"]).is_ok());
    }

    #[test]
    fn state_reset_chunks_rejects_export_with_stuck() {
        assert_eq!(
            state_reset_chunks_parse_from(&["--export", "x", "--stuck-checkpoints"]).unwrap_err(),
            clap::error::ErrorKind::ArgumentConflict,
        );
    }

    #[test]
    fn state_reset_chunks_requires_export_or_stuck() {
        assert_eq!(
            state_reset_chunks_parse_from(&[]).unwrap_err(),
            clap::error::ErrorKind::MissingRequiredArgument,
        );
    }

    #[test]
    fn init_clap_rejects_gcs_and_s3_bucket_together() {
        assert_init_err(
            &[
                "--source",
                "postgresql://localhost/db",
                "--gcs-bucket",
                "g",
                "--s3-bucket",
                "s",
            ],
            clap::error::ErrorKind::ArgumentConflict,
            "--gcs-bucket + --s3-bucket",
        );
    }

    #[test]
    fn init_clap_rejects_discover_with_gcs_bucket() {
        assert_init_err(
            &[
                "--source",
                "postgresql://localhost/db",
                "--discover",
                "--gcs-bucket",
                "g",
            ],
            clap::error::ErrorKind::ArgumentConflict,
            "--discover + --gcs-bucket",
        );
    }

    #[test]
    fn init_clap_rejects_discover_with_s3_bucket() {
        assert_init_err(
            &[
                "--source",
                "postgresql://localhost/db",
                "--discover",
                "--s3-bucket",
                "s",
            ],
            clap::error::ErrorKind::ArgumentConflict,
            "--discover + --s3-bucket",
        );
    }

    #[test]
    fn init_clap_rejects_discover_with_credentials_or_region() {
        // `--gcs-credentials-file` requires `--gcs-bucket`, and the bucket conflicts
        // with `--discover`. Provide the bucket so we cleanly hit the discover×cloud
        // conflict (not the missing-required-arg path).
        assert_init_err(
            &[
                "--source",
                "postgresql://localhost/db",
                "--discover",
                "--gcs-bucket",
                "g",
                "--gcs-credentials-file",
                "/k.json",
            ],
            clap::error::ErrorKind::ArgumentConflict,
            "--discover + --gcs-credentials-file (with bucket)",
        );
        assert_init_err(
            &[
                "--source",
                "postgresql://localhost/db",
                "--discover",
                "--s3-bucket",
                "b",
                "--s3-region",
                "eu-west-1",
            ],
            clap::error::ErrorKind::ArgumentConflict,
            "--discover + --s3-region (with bucket)",
        );
    }

    #[test]
    fn init_clap_rejects_gcs_credentials_without_bucket() {
        assert_init_err(
            &[
                "--source",
                "postgresql://localhost/db",
                "--gcs-credentials-file",
                "/k.json",
            ],
            clap::error::ErrorKind::MissingRequiredArgument,
            "--gcs-credentials-file requires --gcs-bucket",
        );
    }

    #[test]
    fn init_clap_rejects_s3_region_without_bucket() {
        assert_init_err(
            &[
                "--source",
                "postgresql://localhost/db",
                "--s3-region",
                "eu-west-1",
            ],
            clap::error::ErrorKind::MissingRequiredArgument,
            "--s3-region requires --s3-bucket",
        );
    }

    #[test]
    fn init_clap_accepts_well_formed_gcs_invocation() {
        assert_init_ok(
            &[
                "--source",
                "postgresql://localhost/db",
                "--gcs-bucket",
                "my-bucket",
                "--gcs-credentials-file",
                "/k.json",
            ],
            "well-formed --gcs-bucket invocation",
        );
    }

    #[test]
    fn init_clap_accepts_well_formed_s3_invocation() {
        assert_init_ok(
            &[
                "--source",
                "postgresql://localhost/db",
                "--s3-bucket",
                "b",
                "--s3-region",
                "eu-west-1",
            ],
            "well-formed --s3-bucket invocation",
        );
    }

    fn make_run(parallel_exports: bool, parallel_export_processes: bool) -> Commands {
        Commands::Run {
            config: "c.yaml".into(),
            export: None,
            validate: false,
            reconcile: false,
            resume: false,
            parallel_exports,
            parallel_export_processes,
            summary_output: None,
            json: false,
            params: vec![],
        }
    }

    #[test]
    fn validate_run_rejects_both_parallel_flags() {
        let cmd = make_run(true, true);
        assert!(validate_cli(&cmd).is_err());
    }

    #[test]
    fn validate_run_accepts_single_parallel_flag() {
        assert!(validate_cli(&make_run(true, false)).is_ok());
        assert!(validate_cli(&make_run(false, true)).is_ok());
        assert!(validate_cli(&make_run(false, false)).is_ok());
    }

    #[test]
    fn validate_plan_rejects_output_with_pretty() {
        let cmd = Commands::Plan {
            config: "c.yaml".into(),
            export: None,
            params: vec![],
            output: Some("out.json".into()),
            format: PlanFormat::Pretty,
        };
        assert!(validate_cli(&cmd).is_err());
    }

    #[test]
    fn validate_plan_accepts_output_with_json() {
        let cmd = Commands::Plan {
            config: "c.yaml".into(),
            export: None,
            params: vec![],
            output: Some("out.json".into()),
            format: PlanFormat::Json,
        };
        assert!(validate_cli(&cmd).is_ok());
    }

    #[test]
    fn validate_reconcile_rejects_output_with_pretty() {
        let cmd = Commands::Reconcile {
            config: "c.yaml".into(),
            export: "e".into(),
            format: ReconcileFormat::Pretty,
            output: Some("out.json".into()),
            params: vec![],
        };
        assert!(validate_cli(&cmd).is_err());
    }

    #[test]
    fn validate_reconcile_accepts_output_with_json() {
        let cmd = Commands::Reconcile {
            config: "c.yaml".into(),
            export: "e".into(),
            format: ReconcileFormat::Json,
            output: Some("out.json".into()),
            params: vec![],
        };
        assert!(validate_cli(&cmd).is_ok());
    }

    #[test]
    fn validate_repair_rejects_output_with_pretty() {
        let cmd = Commands::Repair {
            config: "c.yaml".into(),
            export: "e".into(),
            report: None,
            execute: false,
            format: ReconcileFormat::Pretty,
            output: Some("out.json".into()),
            params: vec![],
        };
        assert!(validate_cli(&cmd).is_err());
    }

    #[test]
    fn validate_repair_accepts_output_with_json() {
        let cmd = Commands::Repair {
            config: "c.yaml".into(),
            export: "e".into(),
            report: None,
            execute: false,
            format: ReconcileFormat::Json,
            output: Some("out.json".into()),
            params: vec![],
        };
        assert!(validate_cli(&cmd).is_ok());
    }

    #[test]
    fn json_errors_flag_parsed_before_subcommand() {
        let cli = Cli::try_parse_from(["rivet", "--json-errors", "run", "--config", "c.yaml"])
            .expect("should parse");
        assert!(cli.json_errors);
    }

    #[test]
    fn json_errors_flag_parsed_after_subcommand() {
        let cli = Cli::try_parse_from(["rivet", "run", "--config", "c.yaml", "--json-errors"])
            .expect("should parse with global flag after subcommand");
        assert!(cli.json_errors);
    }

    #[test]
    fn json_errors_off_by_default() {
        let cli =
            Cli::try_parse_from(["rivet", "run", "--config", "c.yaml"]).expect("should parse");
        assert!(!cli.json_errors);
    }

    #[test]
    fn json_error_output_is_valid_json() {
        let err = anyhow::anyhow!("connection refused");
        let output = serde_json::json!({"error": format!("{err:#}")}).to_string();
        let parsed: serde_json::Value = serde_json::from_str(&output).expect("must be valid JSON");
        assert_eq!(parsed["error"], "connection refused");
    }
}
