//! Subcommand dispatch: route parsed [`Cli`] commands into pipeline / init /
//! preflight entry points.
//!
//! Every arm here is intentionally a thin adapter — convert clap field types
//! (`Vec<String>`, `Option<String>`, etc.) into the shapes pipeline modules
//! actually want, then call exactly one function. Validation lives in
//! `validate`, parameter parsing in `params`, and the clap grammar in `args`.

use clap::CommandFactory;

use super::args::{Cli, Commands, PlanFormat, ReconcileFormat, StateAction, ValidateFormat};
use super::params::{parse_params, resolve_init_source};
use super::validate::validate_cli;
use crate::error::Result;
use crate::{init, pipeline, preflight};

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
        } => dispatch_init(
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
        ),
        Commands::Plan {
            config,
            export,
            params,
            output,
            format,
        } => dispatch_plan(config, export, params, output, format),
        Commands::Apply { plan_file, force } => pipeline::run_apply_command(&plan_file, force),
        Commands::Validate {
            config,
            export,
            format,
            output,
        } => dispatch_validate(config, export, format, output),
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
        Commands::State { action } => dispatch_state(action),
    }
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
    let tgt = target
        .as_deref()
        .and_then(crate::types::target::ExportTarget::parse);
    preflight::check(
        &config,
        export.as_deref(),
        p.as_ref(),
        type_report || json || strict || tgt.is_some(),
        strict,
        json,
        tgt,
    )
}

#[allow(clippy::too_many_arguments)]
fn dispatch_init(
    source: Option<String>,
    source_env: Option<String>,
    source_file: Option<String>,
    table: Option<String>,
    schema: Option<String>,
    output: Option<String>,
    discover: bool,
    gcs_bucket: Option<String>,
    gcs_credentials_file: Option<String>,
    s3_bucket: Option<String>,
    s3_region: Option<String>,
) -> Result<()> {
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
    let fmt = match format {
        PlanFormat::Pretty => pipeline::PlanOutputFormat::Pretty,
        PlanFormat::Json => pipeline::PlanOutputFormat::Json(output),
    };
    pipeline::run_plan_command(&config, export.as_deref(), p.as_ref(), fmt)
}

fn dispatch_validate(
    config: String,
    export: Option<String>,
    format: ValidateFormat,
    output: Option<String>,
) -> Result<()> {
    let fmt = match format {
        ValidateFormat::Pretty => pipeline::ValidateOutputFormat::Pretty,
        ValidateFormat::Json => pipeline::ValidateOutputFormat::Json(output),
    };
    pipeline::run_validate_command(&config, export.as_deref(), fmt)
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
