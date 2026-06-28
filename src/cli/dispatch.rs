//! Subcommand dispatch: route parsed [`Cli`] commands into pipeline / init /
//! preflight entry points.
//!
//! Every arm here is intentionally a thin adapter — convert clap field types
//! (`Vec<String>`, `Option<String>`, etc.) into the shapes pipeline modules
//! actually want, then call exactly one function. Validation lives in
//! `validate`, parameter parsing in `params`, and the clap grammar in `args`.

use clap::CommandFactory;

use super::args::{
    Cli, Commands, PlanFormat, ReconcileFormat, SchemaKind, StateAction, ValidateDepth,
    ValidateFormat,
};
use super::params::{parse_params, resolve_init_source};
use super::validate::validate_cli;
use crate::config::Config;
use crate::error::Result;
use crate::{init, pipeline, preflight};

/// Validate a `--export <name>` selection against the loaded config and, on a
/// miss, bail with the sorted list of declared export names — so a typo
/// (`--export oders` for `orders`) names the choices instead of the bare
/// "export 'oders' not found in config" the pipeline/preflight resolvers emit
/// downstream. Mirrors the enumerated-names hint `rivet state reset` already
/// gives (`pipeline/cli.rs`) and the "Did you mean" field-typo lint
/// (`config/lints.rs`). A `None` selection (all exports) is always Ok.
///
/// This runs *before* the subcommand's own config load; the extra read of a
/// small YAML is the same cost `reset_state`/`reset_chunk_checkpoint` already
/// pay to validate an export name up front, and it keeps the good error in one
/// place for every `--export`-taking subcommand.
fn check_export_selection(config: &Config, export: Option<&str>) -> Result<()> {
    let Some(name) = export else { return Ok(()) };
    if config.exports.iter().any(|e| e.name == name) {
        return Ok(());
    }
    let mut known: Vec<&str> = config.exports.iter().map(|e| e.name.as_str()).collect();
    known.sort_unstable();
    anyhow::bail!(
        "export '{}' not found in config.\n  Known exports: {}\n  Hint: check the spelling against the names above.",
        name,
        if known.is_empty() {
            "(none defined)".to_string()
        } else {
            known.join(", ")
        },
    );
}

/// Validate and execute the parsed CLI. Returns `Err` with a formatted message
/// on validation failure or any subcommand error; `main.rs` decides whether to
/// render it as plain text or JSON via the `--json-errors` global flag.
pub fn dispatch(cli: Cli) -> Result<()> {
    validate_cli(&cli.command)?;
    match cli.command {
        Commands::Run {
            config,
            export,
            validate,
            reconcile,
            resume,
            force,
            parallel_exports,
            parallel_export_processes,
            summary_output,
            json,
            params,
        } => dispatch_run(
            config,
            export,
            validate,
            reconcile,
            resume,
            force,
            parallel_exports,
            parallel_export_processes,
            summary_output,
            json,
            params,
        ),
        Commands::Check {
            config,
            export,
            params,
            type_report,
            strict,
            json,
            target,
        } => dispatch_check(config, export, params, type_report, strict, json, target),
        Commands::Doctor { config } => preflight::doctor(&config),
        Commands::Cdc {
            source,
            source_env,
            source_file,
            server_id,
            checkpoint,
            table,
            max_events,
            output,
            format,
            rollover,
            slot,
            capture_instance,
            until_current,
        } => dispatch_cdc(CdcArgs {
            source,
            source_env,
            source_file,
            server_id,
            checkpoint,
            table,
            max_events,
            output,
            format,
            rollover,
            slot,
            capture_instance,
            until_current,
        }),
        Commands::Init {
            source,
            source_env,
            source_file,
            table,
            schema,
            include,
            exclude,
            output,
            discover,
            mode,
            gcs_bucket,
            gcs_credentials_file,
            s3_bucket,
            s3_region,
        } => dispatch_init(
            source,
            source_env,
            source_file,
            table,
            schema,
            include,
            exclude,
            output,
            discover,
            mode,
            gcs_bucket,
            gcs_credentials_file,
            s3_bucket,
            s3_region,
        ),
        Commands::Plan {
            config,
            export,
            params,
            output,
            format,
        } => dispatch_plan(config, export, params, output, format),
        Commands::Apply {
            plan_file,
            parallel_export_processes,
            resume,
            force,
        } => pipeline::run_apply_command(&plan_file, force, parallel_export_processes, resume),
        Commands::Validate {
            config,
            export,
            format,
            depth,
            output,
            date,
            run_id,
            prefix,
        } => dispatch_validate(config, export, format, depth, output, date, run_id, prefix),
        Commands::Reconcile {
            config,
            export,
            format,
            output,
            params,
        } => dispatch_reconcile(config, export, format, output, params),
        Commands::Repair {
            config,
            export,
            report,
            execute,
            format,
            output,
            params,
        } => dispatch_repair(config, export, report, execute, format, output, params),
        Commands::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "rivet", &mut std::io::stdout());
            Ok(())
        }
        Commands::Metrics {
            config,
            export,
            last,
        } => pipeline::show_metrics(&config, export.as_deref(), last),
        Commands::Journal {
            config,
            export,
            last,
            run_id,
        } => pipeline::show_journal(&config, &export, last, run_id.as_deref()),
        Commands::Schema { what } => dispatch_schema(what),
        Commands::State { action } => dispatch_state(action),
    }
}

fn dispatch_schema(what: SchemaKind) -> Result<()> {
    match what {
        SchemaKind::Config => {
            let schema = crate::config::generate_config_schema_pretty()?;
            // `print!` (not `println!`) — the schema string already
            // terminates with a newline; doubling it would diff
            // against the in-tree artifact.
            print!("{schema}");
            Ok(())
        }
    }
}

/// Parsed `rivet cdc` arguments (clap field types).
struct CdcArgs {
    source: Option<String>,
    source_env: Option<String>,
    source_file: Option<String>,
    server_id: u32,
    checkpoint: Option<String>,
    table: Vec<String>,
    max_events: Option<usize>,
    output: Option<String>,
    format: String,
    rollover: usize,
    slot: String,
    capture_instance: Option<String>,
    until_current: bool,
}

/// `rivet cdc`: build the engine's change stream via `create_change_stream`
/// (dispatch by URL scheme), then either emit NDJSON (default) or, with
/// `--output`, write typed Parquet/CSV files through the commit seam. `--output`
/// resolves the table's column schema from the source via `type_mappings`.
fn dispatch_cdc(a: CdcArgs) -> Result<()> {
    let (url, _prov) = resolve_init_source(a.source, a.source_env, a.source_file)?;
    let ckpt = a.checkpoint.map(std::path::PathBuf::from);
    let cdc_cfg = crate::source::cdc::CdcConfig {
        url: url.clone(),
        server_id: a.server_id,
        slot: a.slot,
        capture_instance: a.capture_instance,
        checkpoint: ckpt.clone(),
        until_current: a.until_current,
        // The CLI carries no TlsConfig; `None` ⇒ the require_tls_or_loopback gate
        // refuses a remote host (config-driven `rivet run` supplies source.tls).
        tls: None,
    };
    let Some(dir) = a.output else {
        // NDJSON to stdout: no durable sink, so the slot is deliberately not
        // advanced (correct at-least-once — the consumer owns durability). Resume
        // for MySQL is the checkpoint file; PostgreSQL re-reads from the slot.
        let mut stream = crate::source::cdc::create_change_stream(&cdc_cfg)?;
        return crate::source::cdc::run(stream.as_mut(), ckpt, a.table, a.max_events);
    };

    // --output: the typed file sink, via the same `run_capture` assembler the
    // `mode: cdc` run uses. The CLI is the ad-hoc path — `manifest.json` + `_SUCCESS`
    // at the destination are its run record; `rivet run` is the path that also
    // writes the state-DB metric + journal.
    let tbl = match a.table.as_slice() {
        [t] => t.clone(),
        _ => anyhow::bail!(
            "rivet cdc --output requires exactly one --table (its schema is resolved from the source)"
        ),
    };
    let fmt = match a.format.as_str() {
        "parquet" => crate::config::FormatType::Parquet,
        "csv" => crate::config::FormatType::Csv,
        other => anyhow::bail!("--format must be 'parquet' or 'csv', got {other:?}"),
    };
    let dest = crate::destination::create_destination(&crate::config::DestinationConfig {
        destination_type: crate::config::DestinationType::Local,
        path: Some(dir.clone()),
        ..Default::default()
    })?;
    let now = chrono::Utc::now().to_rfc3339();
    crate::source::cdc::run_capture(crate::source::cdc::CdcCapture {
        cdc_cfg,
        table: tbl,
        dest: dest.as_ref(),
        dest_uri: dir,
        format: fmt,
        max_events: a.max_events,
        rollover: a.rollover,
        rollover_memory_bytes: None,
        run_id: now.clone(),
        started_at: now,
    })
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn dispatch_run(
    config: String,
    export: Option<String>,
    validate: bool,
    reconcile: bool,
    resume: bool,
    force: bool,
    parallel_exports: bool,
    parallel_export_processes: bool,
    summary_output: Option<String>,
    json: bool,
    params: Vec<String>,
) -> Result<()> {
    let p = parse_params(&params);
    let p = if p.is_empty() { None } else { Some(p) };
    if let Some(name) = export.as_deref() {
        check_export_selection(&Config::load_with_params(&config, p.as_ref())?, Some(name))?;
    }
    let summary_output_path = summary_output.as_ref().map(std::path::PathBuf::from);
    pipeline::run(
        &config,
        export.as_deref(),
        validate,
        reconcile,
        resume,
        force,
        p.as_ref(),
        parallel_exports,
        parallel_export_processes,
        summary_output_path.as_deref(),
        json,
    )
}

fn dispatch_check(
    config: String,
    export: Option<String>,
    params: Vec<String>,
    type_report: bool,
    strict: bool,
    json: bool,
    target: Option<String>,
) -> Result<()> {
    let p = parse_params(&params);
    let p = if p.is_empty() { None } else { Some(p) };
    // A declared `--target` that doesn't parse is a loud error — never silently
    // dropped to `None` (which would give false target-compat assurance). This
    // mirrors the config-level `target:` validation in `preflight/mod.rs`.
    let tgt = match target.as_deref() {
        Some(s) => Some(crate::types::target::ExportTarget::parse(s).ok_or_else(|| {
            anyhow::anyhow!("unknown target '{s}' (expected: bigquery, duckdb, snowflake)")
        })?),
        None => None,
    };
    if let Some(name) = export.as_deref() {
        check_export_selection(&Config::load_with_params(&config, p.as_ref())?, Some(name))?;
    }
    preflight::check(
        &config,
        export.as_deref(),
        p.as_ref(),
        type_report || json || strict || tgt.is_some(),
        strict,
        json,
        tgt,
    )?;
    // Surface plan-validation diagnostics so `check` agrees with `run`/`plan`:
    // a stdout+chunked config is Rejected by all three, not silently passed by
    // `check` alone. `preflight::check` probes source/destination/types; this
    // adds the mode×destination compatibility gate (`validate_plan`). Skipped
    // under `--json` so NDJSON type-report output stays one object per line.
    check_plan_compatibility(&config, export.as_deref(), p.as_ref(), json)
}

/// Build the resolved plan for each selected export and surface
/// [`validate_plan`](crate::plan::validate_plan) diagnostics the same way
/// `rivet plan` does: print every `[rule] message`, and return an error on the
/// first `Rejected` so `check` exits non-zero on an incompatible combination
/// (e.g. `[stdout-no-chunked]`). Warnings/Degraded notes print but do not fail.
fn check_plan_compatibility(
    config_path: &str,
    export_name: Option<&str>,
    params: Option<&std::collections::HashMap<String, String>>,
    json_output: bool,
) -> Result<()> {
    if json_output {
        return Ok(());
    }
    let config = Config::load_with_params(config_path, params)?;
    let config_dir = std::path::Path::new(config_path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let selected: Vec<&crate::config::ExportConfig> = match export_name {
        Some(name) => config.exports.iter().filter(|e| e.name == name).collect(),
        None => config.exports.iter().collect(),
    };
    let mut rejected: Option<String> = None;
    for export in selected {
        // `--validate`/`--reconcile`/`--resume` are run-only flags; `check`
        // builds the plan with them off, matching how `rivet plan` validates.
        //
        // A `build_plan` failure here is NOT promoted to a `check` error: the
        // source/destination/type probes in `preflight::check` already ran and
        // own those diagnostics, and `build_plan` can fail for unrelated reasons
        // (e.g. a `table:`-shortcut chunk-shape probe). We only want the
        // compatibility verdict — when the plan won't build, log and skip it so
        // `check` never regresses to a hard error it did not produce before.
        let plan =
            match crate::plan::build_plan(&config, export, config_dir, false, false, false, params)
            {
                Ok(plan) => plan,
                Err(e) => {
                    log::warn!(
                        "check '{}': plan-compatibility check skipped (plan did not build): {:#}",
                        export.name,
                        e
                    );
                    continue;
                }
            };
        for d in crate::plan::validate_plan(&plan) {
            let line = format!("[{}] {}", d.rule, d.message);
            match d.level {
                crate::plan::DiagnosticLevel::Rejected => {
                    println!("Rejected: {line}");
                    rejected.get_or_insert(line);
                }
                crate::plan::DiagnosticLevel::Warning => println!("Warning: {line}"),
                crate::plan::DiagnosticLevel::Degraded => println!("Degraded: {line}"),
            }
        }
    }
    if let Some(line) = rejected {
        anyhow::bail!("{line}");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn dispatch_init(
    source: Option<String>,
    source_env: Option<String>,
    source_file: Option<String>,
    table: Option<String>,
    schema: Option<String>,
    include: Vec<String>,
    exclude: Vec<String>,
    output: Option<String>,
    discover: bool,
    mode: Option<String>,
    gcs_bucket: Option<String>,
    gcs_credentials_file: Option<String>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
) -> Result<()> {
    if let Some(m) = mode.as_deref()
        && !matches!(
            m,
            "full" | "incremental" | "chunked" | "time_window" | "cdc"
        )
    {
        anyhow::bail!(
            "--mode must be one of: full, incremental, chunked, time_window, cdc (got {m:?})"
        );
    }
    let fmt = if discover {
        init::InitFormat::DiscoveryJson
    } else {
        init::InitFormat::Yaml
    };
    let (source_url, provenance) = resolve_init_source(source, source_env, source_file)?;
    let yaml_dest = init::InitYamlDestination {
        gcs_bucket,
        gcs_credentials_file,
        s3_bucket,
        s3_region,
    };
    let filter = init::TableFilter { include, exclude };
    init::init(
        &source_url,
        &provenance,
        table.as_deref(),
        schema.as_deref(),
        output.as_deref(),
        fmt,
        yaml_dest,
        &filter,
        mode.as_deref(),
    )
}

fn dispatch_plan(
    config: String,
    export: Option<String>,
    params: Vec<String>,
    output: Option<String>,
    format: PlanFormat,
) -> Result<()> {
    let p = parse_params(&params);
    let p = if p.is_empty() { None } else { Some(p) };
    if let Some(name) = export.as_deref() {
        check_export_selection(&Config::load_with_params(&config, p.as_ref())?, Some(name))?;
    }
    let fmt = match format {
        PlanFormat::Pretty => pipeline::PlanOutputFormat::Pretty,
        PlanFormat::Json => pipeline::PlanOutputFormat::Json(output),
    };
    pipeline::run_plan_command(&config, export.as_deref(), p.as_ref(), fmt)
}

#[allow(clippy::too_many_arguments)]
fn dispatch_validate(
    config: String,
    export: Option<String>,
    format: ValidateFormat,
    depth: ValidateDepth,
    output: Option<String>,
    date: Option<String>,
    run_id: Option<String>,
    prefix: Option<String>,
) -> Result<()> {
    if let Some(name) = export.as_deref() {
        check_export_selection(&Config::load(&config)?, Some(name))?;
    }
    let fmt = match format {
        ValidateFormat::Pretty => pipeline::ValidateOutputFormat::Pretty,
        ValidateFormat::Json => pipeline::ValidateOutputFormat::Json(output),
    };
    // Parse --date once here so a malformed value fails before we open a
    // destination — the pipeline layer never sees a half-validated date.
    let parsed_date = match date {
        Some(s) => Some(
            chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|e| {
                anyhow::anyhow!("invalid --date '{}': expected YYYY-MM-DD ({})", s, e)
            })?,
        ),
        None => None,
    };
    // `--depth` is already the pipeline `ValidateDepth` (re-exported through
    // `args`), so it threads straight onto the target with no CLI→pipeline
    // mapping.
    let target = pipeline::ValidateTarget {
        date: parsed_date,
        run_id,
        prefix_override: prefix,
        depth,
    };
    pipeline::run_validate_command(&config, export.as_deref(), fmt, target)
}

fn dispatch_reconcile(
    config: String,
    export: String,
    format: ReconcileFormat,
    output: Option<String>,
    params: Vec<String>,
) -> Result<()> {
    let p = parse_params(&params);
    let p = if p.is_empty() { None } else { Some(p) };
    check_export_selection(
        &Config::load_with_params(&config, p.as_ref())?,
        Some(&export),
    )?;
    let fmt = match format {
        ReconcileFormat::Pretty => pipeline::ReconcileOutputFormat::Pretty,
        ReconcileFormat::Json => pipeline::ReconcileOutputFormat::Json(output),
    };
    pipeline::run_reconcile_command(&config, &export, p.as_ref(), fmt)
}

fn dispatch_repair(
    config: String,
    export: String,
    report: Option<String>,
    execute: bool,
    format: ReconcileFormat,
    output: Option<String>,
    params: Vec<String>,
) -> Result<()> {
    let p = parse_params(&params);
    let p = if p.is_empty() { None } else { Some(p) };
    check_export_selection(
        &Config::load_with_params(&config, p.as_ref())?,
        Some(&export),
    )?;
    let source = match report {
        Some(path) => pipeline::RepairReportSource::File(path),
        None => pipeline::RepairReportSource::Auto,
    };
    let fmt = match format {
        ReconcileFormat::Pretty => pipeline::RepairOutputFormat::Pretty,
        ReconcileFormat::Json => pipeline::RepairOutputFormat::Json(output),
    };
    pipeline::run_repair_command(&config, &export, p.as_ref(), source, execute, fmt)
}

fn dispatch_state(action: StateAction) -> Result<()> {
    match action {
        StateAction::Show { config } => pipeline::show_state(&config),
        StateAction::Reset { config, export } => pipeline::reset_state(&config, &export),
        StateAction::Files {
            config,
            export,
            last,
        } => pipeline::show_files(&config, export.as_deref(), last),
        StateAction::ResetChunks {
            config,
            export,
            stuck_checkpoints,
        } => {
            if stuck_checkpoints {
                pipeline::reset_chunk_checkpoints_stuck(&config)
            } else if let Some(name) = export {
                pipeline::reset_chunk_checkpoint(&config, &name)
            } else {
                // Unreachable: clap enforces one of the two via `required_unless_present`.
                Ok(())
            }
        }
        StateAction::Chunks { config, export } => pipeline::show_chunk_checkpoint(&config, &export),
        StateAction::Progression { config, export } => {
            pipeline::show_progression(&config, export.as_deref())
        }
    }
}
