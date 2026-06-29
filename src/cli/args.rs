//! Pure clap surface: every subcommand and shared format enum lives here.
//!
//! This module is intentionally free of business logic and `crate::*` calls
//! beyond the bare minimum needed by clap derive. Behavior — validation,
//! dispatch, parameter parsing — lives in sibling modules so the public CLI
//! grammar can evolve independently of the runtime wiring.

use clap::{Parser, Subcommand};
use clap_complete::Shell;

// The graded verify depth (`--depth`) is the *same* enum that gates the checks
// in the pipeline's `verify_at_destination`, re-exported through `pipeline` so
// the flag parses straight into it.  Defining it in `cli` instead would invert
// the layering (the pipeline would depend on the CLI), so this one shared type
// is the deliberate exception to the "no crate:: types" guideline above — it
// keeps a single source of truth for the three depth levels.
pub use crate::pipeline::ValidateDepth;

#[derive(Parser)]
#[command(
    name = "rivet",
    version,
    about = "Export data from databases to files",
    after_help = "Getting started (the happy path):\n  \
        1. rivet init     scaffold a config from your database\n  \
        2. rivet doctor   test source + destination auth\n  \
        3. rivet check    column-type & schema report\n  \
        4. rivet run      export your data\n\n\
        Docs: https://github.com/panchenkoai/rivet/blob/main/docs/getting-started.md"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    /// Output errors as {"error":"..."} JSON to stderr; useful for machine-readable orchestration
    #[arg(long, global = true)]
    pub json_errors: bool,
}

/// Parse argv into [`Cli`]. When a config-taking subcommand (the documented
/// `doctor` Step 1, `check`, `run`, …) is invoked with NO `-c/--config`, append
/// a `rivet init` recovery hint after clap's own usage error. Every other clap
/// outcome — `--help` / `--version` (which exit 0) and all other parse errors —
/// is left byte-for-byte unchanged. We deliberately do NOT default `-c` to
/// `./rivet.yaml`: that would silently run against an unintended config from the
/// wrong directory, breaking rivet's no-accidental-action contract.
pub fn parse_cli() -> Cli {
    use clap::Parser;
    match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            let missing_config = e.kind() == clap::error::ErrorKind::MissingRequiredArgument
                && e.to_string().contains("--config");
            if missing_config {
                let _ = e.print();
                eprintln!(
                    "\nHint: run `rivet init -o rivet.yaml` to scaffold a config, then pass it with `-c rivet.yaml`."
                );
                std::process::exit(2);
            }
            // Help/version (exit 0) and every other parse error: clap's default.
            e.exit();
        }
    }
}

#[derive(Subcommand)]
pub enum Commands {
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
        /// Row-count audit: run COUNT(*) on the source and compare with the
        /// exported row count; a mismatch fails the run. Implies `--validate`
        /// (also verifies the output file manifest).
        #[arg(long)]
        reconcile: bool,
        /// Resume a chunked export with `chunk_checkpoint: true` (same query/chunk_column/chunk_size)
        #[arg(long)]
        resume: bool,
        /// Override safety gates that would otherwise refuse the run.
        ///
        /// Today: with `--resume`, allows starting against a destination prefix
        /// whose `_SUCCESS` marker is already present.  Without `--force`,
        /// resume against an already-complete run refuses, so an operator
        /// cannot accidentally re-export over a verified dataset.
        #[arg(long)]
        force: bool,
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
    /// Column-type & schema report for each export (needs a working connection;
    /// run `doctor` first if it can't connect)
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
    /// Verify source + destination auth/connectivity (run this first)
    Doctor {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
    },
    /// Stream change data capture (CDC) from a source's transaction log.
    ///
    /// Experimental — MySQL binlog only. Emits one JSON object per row change to
    /// stdout (NDJSON) and, with `--checkpoint`, persists a resume position.
    /// Requires the source at `binlog_format = ROW` with a `REPLICATION SLAVE`
    /// grant.
    #[command(group = clap::ArgGroup::new("cdc_source").required(true).multiple(false))]
    Cdc {
        /// Database URL (mysql:// only for now). Visible in `ps`; prefer
        /// `--source-env`/`--source-file` outside local dev.
        #[arg(long, group = "cdc_source")]
        source: Option<String>,
        /// Name of an environment variable holding the database URL.
        #[arg(long, value_name = "ENV_VAR", group = "cdc_source")]
        source_env: Option<String>,
        /// Path to a file containing just the database URL (one line).
        #[arg(long, value_name = "PATH", group = "cdc_source")]
        source_file: Option<String>,
        /// Replica server-id for the binlog connection (must be distinct from the
        /// source's and any other replica).
        #[arg(long, default_value_t = 4271)]
        server_id: u32,
        /// Persist/resume the binlog position to this file. Omit to tail from the
        /// current position without checkpointing.
        #[arg(long, value_name = "PATH")]
        checkpoint: Option<String>,
        /// Only emit changes for this table (repeatable; default: all tables).
        #[arg(long, value_name = "TABLE")]
        table: Vec<String>,
        /// Stop after N change events (default: stream until interrupted).
        #[arg(long, value_name = "N")]
        max_events: Option<usize>,
        /// Write typed Parquet/CSV files to this directory (the upsert/after-image
        /// shape) instead of NDJSON to stdout. Requires exactly one `--table` —
        /// its schema is resolved from the source.
        #[arg(long, value_name = "DIR")]
        output: Option<String>,
        /// Output file format when `--output` is set: `parquet` (default) or `csv`.
        #[arg(long, value_name = "FORMAT", default_value = "parquet")]
        format: String,
        /// Rows per output file (rollover) when `--output` is set.
        #[arg(long, value_name = "N", default_value_t = 10_000)]
        rollover: usize,
        /// PostgreSQL logical slot name (CDC; created if absent).
        #[arg(long, value_name = "NAME", default_value = "rivet_slot")]
        slot: String,
        /// SQL Server CDC capture instance, e.g. `dbo_orders` — required for
        /// `sqlserver://` sources.
        #[arg(long, value_name = "INSTANCE")]
        capture_instance: Option<String>,
        /// Catch up to the source's current end and exit, instead of streaming
        /// indefinitely — the bounded "read to now and stop" model, ideal for a
        /// scheduler. For MySQL this is a non-blocking binlog dump; PostgreSQL /
        /// SQL Server already drain their backlog and exit.
        #[arg(long)]
        until_current: bool,
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
        /// Database URL (postgresql://, mysql://, or sqlserver://). Visible in shell history / `ps`;
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
        /// Single table, optionally schema-qualified (e.g. public.orders, dbo.orders). Omit to emit all tables/views in a Postgres/SQL Server schema or MySQL database.
        #[arg(long)]
        table: Option<String>,
        /// PostgreSQL: schema to export (default public). SQL Server: schema (default dbo). MySQL: database name if missing from the URL, or override URL database.
        #[arg(long)]
        schema: Option<String>,
        /// Whole-schema only: keep only tables/views matching this glob (`*`/`?`). Repeatable; a table is kept if it matches any `--include`. No `--include` = keep all.
        #[arg(long, value_name = "GLOB")]
        include: Vec<String>,
        /// Whole-schema only: drop tables/views matching this glob (`*`/`?`). Repeatable; `--exclude` wins over `--include`.
        #[arg(long, value_name = "GLOB")]
        exclude: Vec<String>,
        /// Write output to this file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
        /// Emit a machine-readable JSON discovery artifact instead of a YAML scaffold.
        /// Includes row estimates, size bytes, ranked cursor candidates, chunk candidates, and advisory notes.
        /// Mutually exclusive with the YAML-only `--gcs-bucket` / `--s3-bucket` flags.
        #[arg(
            long,
            conflicts_with_all = ["gcs_bucket", "gcs_credentials_file", "s3_bucket", "s3_region"]
        )]
        discover: bool,
        /// Override the suggested extraction mode for every scaffolded export.
        /// `cdc` scaffolds a change-data-capture export (mode: cdc + a cdc: block
        /// with engine-specific stream params) instead of a batch query. Other
        /// values (full / incremental / chunked / time_window) just override the
        /// auto-suggested mode.
        #[arg(long, value_name = "MODE")]
        mode: Option<String>,
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
    /// Execute a sealed plan artifact, or run a config's exports wave-by-wave
    Apply {
        /// A plan JSON artifact from `rivet plan` (sealed single-export replay),
        /// OR a YAML config (`.yaml`/`.yml`) to run its exports wave-by-wave in
        /// ascending `wave:` order — the wave each export was assigned by
        /// `rivet plan`.
        plan_file: String,
        /// Run the cheap (low-cost) exports within each wave concurrently, as
        /// separate processes (same as `parallel_export_processes: true` in the
        /// config). Config-wave mode only; heavier exports — which already
        /// chunk-parallelize internally — still run one at a time.
        #[arg(long)]
        parallel_export_processes: bool,
        /// Config-wave mode: skip exports a prior run already completed
        /// (`_SUCCESS` present) and resume incomplete chunked exports from their
        /// checkpoints, so a re-run after a partial failure does not redo
        /// finished tables. Independent tables are never re-exported.
        #[arg(long)]
        resume: bool,
        /// Skip staleness check (allow plans older than 24 h)
        #[arg(long)]
        force: bool,
    },
    /// Targeted repair of chunks flagged by reconcile: emit a repair plan, or re-export only mismatched ranges.
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
    /// Re-run manifest-aware verification against an existing destination, no extraction.
    ///
    /// The same file-manifest checks `rivet run --validate` performs at
    /// end-of-run, exposed as a standalone command for between-run polling and
    /// triage.  Reads manifest.json + _SUCCESS at the destination, head-checks
    /// every committed part for presence and recorded size_bytes.  Source is
    /// not queried — use `rivet reconcile` for a source-vs-export row audit.
    ///
    /// By default `validate` resolves the destination prefix the same way
    /// `run` does — `{date}` becomes today's UTC date.  Use `--date`,
    /// `--run-id`, or `--prefix` to point at a prior run instead of today.
    Validate {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Validate only this export (default: every export in the config)
        #[arg(short, long)]
        export: Option<String>,
        /// Output format: "pretty" (human summary) or "json" (machine-readable)
        #[arg(long, default_value = "pretty")]
        format: ValidateFormat,
        /// How deep to verify: "light" (manifest + _SUCCESS only, no prefix
        /// listing), "sample" (light + part reconcile + untracked surplus), or
        /// "full" (sample + the value-checksum re-read of every part).
        ///
        /// `full` is the default and matches the pre-graded behaviour. Use
        /// `light` for a fast "is this a complete, marked run?" poll, or
        /// `sample` for full structural verification without downloading parts.
        #[arg(long, default_value = "full")]
        depth: ValidateDepth,
        /// Write JSON report to this file (only with `--format json`)
        #[arg(short, long)]
        output: Option<String>,
        /// Resolve `{date}` to this ISO-8601 day (e.g. `2026-05-21`) instead of today.
        ///
        /// Use when a run that landed on a prior day's prefix needs to be
        /// re-verified — without this flag `validate` looks at today's
        /// resolved prefix and reports "no manifest" for yesterday's data.
        #[arg(long, value_name = "YYYY-MM-DD", conflicts_with = "prefix")]
        date: Option<String>,
        /// Substitute `{run_id}` in the destination template with this value.
        ///
        /// Composes with `--date`.  Has no effect if the template does not
        /// contain `{run_id}`.
        #[arg(long, value_name = "RUN_ID", conflicts_with = "prefix")]
        run_id: Option<String>,
        /// Skip placeholder resolution entirely and verify exactly this prefix.
        ///
        /// Use when the resolved template no longer matches the physical
        /// layout (e.g. data was relocated, or the template changed since
        /// the run landed).  The destination *type* still comes from
        /// config (`local`, `s3`, `gcs`, `azure`); only the resolved
        /// `path`/`prefix` string is overridden.
        #[arg(long, value_name = "PREFIX")]
        prefix: Option<String>,
    },
    /// Partition/window reconciliation: re-count per-partition on source and report mismatches.
    /// Requires a chunked export previously run with `chunk_checkpoint: true`.
    /// Exits non-zero when a mismatch is detected, so CI / orchestrators can gate on it
    /// (an `unknown` partition warns but does not fail).
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
        /// Emit the metrics as a JSON array to stdout (for CI / dashboards)
        /// instead of the text table. Empty history prints `[]`.
        #[arg(long)]
        json: bool,
    },
    /// Emit machine-readable schemas for Rivet's data contracts.
    ///
    /// Today: `rivet schema config` prints the JSON Schema for the
    /// `rivet.yaml` config to stdout.  Operators pipe this into a file
    /// and reference it via a `# yaml-language-server: $schema=...`
    /// header so VS Code / Neovim's YAML language server highlights
    /// invalid keys, suggests enum values, and surfaces required
    /// fields as the YAML is edited.  See
    /// `docs/cloud-destinations.md` for the broader contract.
    Schema {
        #[command(subcommand)]
        what: SchemaKind,
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
pub enum SchemaKind {
    /// Print the JSON Schema describing `rivet.yaml` to stdout.
    ///
    /// The schema is generated from the running binary's Rust types,
    /// so it always matches the config grammar this version accepts.
    /// Pipe to a file and reference it via a
    /// `# yaml-language-server: $schema=…` header in your config:
    ///
    ///     rivet schema config > rivet.schema.json
    Config,
}

#[derive(Subcommand)]
pub enum StateAction {
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
        /// Emit the file list as a JSON array to stdout (CI completeness checks)
        /// instead of the text table. Empty → `[]`.
        #[arg(long)]
        json: bool,
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
        /// Alias `--failed` refers to "checkpoint state stuck", not HTTP-style failures or metric rows.
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
    /// Show committed / verified export boundaries (the last fully-exported cursor position)
    Progression {
        #[arg(short, long)]
        config: String,
        /// Show progression for a specific export
        #[arg(short, long)]
        export: Option<String>,
    },
}

#[derive(clap::ValueEnum, Clone)]
pub enum PlanFormat {
    /// Human-readable summary printed to stdout
    Pretty,
    /// Pretty-printed JSON (written to --output file or stdout)
    Json,
}

#[derive(clap::ValueEnum, Clone)]
pub enum ReconcileFormat {
    Pretty,
    Json,
}

#[derive(clap::ValueEnum, Clone)]
pub enum ValidateFormat {
    Pretty,
    Json,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

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

    #[test]
    fn init_clap_accepts_repeatable_include_and_exclude_globs() {
        // Both flags are repeatable and parse alongside a source spec.
        let cli = Cli::try_parse_from([
            "rivet",
            "init",
            "--source",
            "sqlserver://sa:p@host:1433/db",
            "--include",
            "orders",
            "--include",
            "users",
            "--exclude",
            "bench_*",
        ])
        .expect("repeatable --include/--exclude must parse");
        match cli.command {
            Commands::Init {
                include, exclude, ..
            } => {
                assert_eq!(include, vec!["orders", "users"]);
                assert_eq!(exclude, vec!["bench_*"]);
            }
            _ => panic!("expected Init command"),
        }
    }

    #[test]
    fn init_clap_include_exclude_default_empty() {
        let cli = Cli::try_parse_from(["rivet", "init", "--source", "postgresql://localhost/db"])
            .expect("init without globs must parse");
        match cli.command {
            Commands::Init {
                include, exclude, ..
            } => {
                assert!(include.is_empty(), "include defaults to empty");
                assert!(exclude.is_empty(), "exclude defaults to empty");
            }
            _ => panic!("expected Init command"),
        }
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

    // ── validate --depth grammar ─────────────────────────────────────────

    /// Pull the parsed `--depth` out of a `rivet validate …` invocation.
    fn validate_depth(extra: &[&str]) -> ValidateDepth {
        let mut argv = vec!["rivet", "validate", "--config", "c.yaml"];
        argv.extend(extra);
        let cli = match Cli::try_parse_from(argv) {
            Ok(cli) => cli,
            Err(e) => panic!("validate should parse, got: {e}"),
        };
        match cli.command {
            Commands::Validate { depth, .. } => depth,
            _ => panic!("expected the Validate subcommand"),
        }
    }

    #[test]
    fn validate_depth_defaults_to_full() {
        // No `--depth` → Full, i.e. the pre-graded behaviour, so existing
        // invocations are unchanged.
        assert_eq!(validate_depth(&[]), ValidateDepth::Full);
    }

    #[test]
    fn validate_depth_parses_each_level() {
        assert_eq!(validate_depth(&["--depth", "light"]), ValidateDepth::Light);
        assert_eq!(
            validate_depth(&["--depth", "sample"]),
            ValidateDepth::Sample
        );
        assert_eq!(validate_depth(&["--depth", "full"]), ValidateDepth::Full);
    }

    #[test]
    fn validate_depth_rejects_unknown_level() {
        let kind =
            Cli::try_parse_from(["rivet", "validate", "--config", "c.yaml", "--depth", "deep"])
                .err()
                .map(|e| e.kind());
        assert_eq!(
            kind,
            Some(clap::error::ErrorKind::InvalidValue),
            "an unknown depth must be rejected by clap"
        );
    }
}
