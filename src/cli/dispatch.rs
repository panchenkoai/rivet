//! Subcommand dispatch: route parsed [`Cli`] commands into pipeline / init /
//! preflight entry points.
//!
//! Every arm here is intentionally a thin adapter — convert clap field types
//! (`Vec<String>`, `Option<String>`, etc.) into the shapes pipeline modules
//! actually want, then call exactly one function. Validation lives in
//! `validate`, parameter parsing in `params`, and the clap grammar in `args`.

use clap::CommandFactory;

use super::args::{
    Cli, Commands, PlanFormat, ReconcileFormat, SchemaKind, StateAction, ValidateFormat,
};
use super::params::{parse_params, resolve_init_source};
use super::validate::validate_cli;
use crate::config::Config;
use crate::error::Result;
use crate::state::StateStore;
use crate::{init, load, pipeline, preflight};

/// Does a `rivet cdc` run stop at the OPEN-TIME snapshot, or tail forever?
///
/// `--stream` is the daemon flag, and `until_current` is its exact inverse — the
/// one polarity inversion in this router that no error message would reveal: a
/// bounded run that never terminates looks like a slow source, and a daemon that
/// clips at the open bound looks like a healthy cycle that quietly stopped
/// following the log. Pure, because `dispatch` is a live-only body excluded
/// wholesale in `.cargo/mutants.toml` — every operator left inline there is a
/// mutant nothing is asked about (`tests/offline/live_only_purity_gate.rs`).
fn cdc_until_current(stream: bool) -> bool {
    !stream
}

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
        Commands::Run(args) => dispatch_run(args),
        Commands::Check {
            config,
            export,
            params,
            type_report,
            strict,
            json,
            target,
        } => dispatch_check(config, export, params, type_report, strict, json, target),
        Commands::Doctor { config, json } => preflight::doctor(&config, json),
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
            stream,
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
            until_current: cdc_until_current(stream),
        }),
        Commands::Load { config, run_id } => {
            load::orchestrate::run_loads(load::orchestrate::LoadArgs { config, run_id })
        }
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
            tls,
            tls_ca,
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
            tls,
            tls_ca,
        ),
        Commands::Plan(args) => dispatch_plan(args),
        Commands::Apply {
            plan_file,
            parallel_export_processes,
            resume,
            force,
            pool,
            split,
        } => pipeline::run_apply_command(
            &plan_file,
            force,
            parallel_export_processes,
            resume,
            pool,
            split,
        ),
        Commands::Validate(args) => dispatch_validate(args),
        Commands::Reconcile {
            config,
            export,
            format,
            output,
            params,
        } => dispatch_reconcile(config, export, format, output, params),
        Commands::Repair(args) => dispatch_repair(args),
        Commands::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "rivet", &mut std::io::stdout());
            Ok(())
        }
        Commands::Metrics {
            config,
            export,
            last,
            json,
        } => pipeline::show_metrics(&config, export.as_deref(), last, json),
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
        SchemaKind::Cli => {
            // CLI reference straight from the clap `Cli` derive — the same
            // source as `--help`, so it cannot drift from the actual commands.
            print!("{}", clap_markdown::help_markdown::<Cli>());
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
    use crate::source::cdc::{CdcEngine, CdcEngineOpts, DrainMode};
    let cdc_cfg = crate::source::cdc::CdcConfig {
        url: url.clone(),
        checkpoint: ckpt.clone(),
        // The CLI has no config file, so the CWD is the honest anchor here: the
        // operator typed the command in a shell whose directory they chose. The
        // config-driven path anchors to the config's directory instead.
        config_dir: std::path::PathBuf::from("."),
        drain: DrainMode::from_until_current(a.until_current),
        // The CLI carries no TlsConfig; `None` ⇒ the require_tls_or_loopback gate
        // refuses a remote host (config-driven `rivet run` supplies source.tls).
        tls: None,
        // The engine's knobs come from the CLI flags for the URL's engine; the CLI
        // has no source config block, so Mongo defaults to relaxed JSON (canonical
        // rides the config-driven `rivet run` path via `source.mongo.json`).
        engine: match CdcEngine::from_url(&url)? {
            CdcEngine::Mysql => CdcEngineOpts::Mysql {
                server_id: a.server_id,
                // `--table` IS the routing filter on this path; empty means capture
                // whatever the stream emits, which makes every event ours.
                configured_tables: a.table.clone(),
            },
            CdcEngine::Postgres => CdcEngineOpts::Postgres {
                slot: a.slot,
                // `--table` IS the routing filter on this path too, so it is
                // exactly the set the guard must classify. Empty (`rivet cdc`
                // with no `--table`) keeps it off: nothing named ⇒ nothing to
                // cross-check — the same rule the SQL Server arm below states.
                configured_tables: a.table.clone(),
            },
            CdcEngine::Mssql => CdcEngineOpts::Mssql {
                // `--table` IS the routing filter on this path too (the NDJSON leg
                // passes it to `run(...)`, the `--output` leg requires exactly
                // one), so it is exactly the set the catalog-identity guard must
                // cross-check against — a capture instance whose NAME splits to a
                // different `schema.table` than the catalog spells would otherwise
                // route/drop every event silently. Empty (`rivet cdc` with no
                // `--table`) keeps the guard off: nothing named ⇒ nothing to
                // cross-check, capture whatever the instance emits.
                configured_tables: a.table.clone(),
                capture_instance: a.capture_instance,
            },
            // Same contract as the SQL arms above: a bare `rivet cdc` with no
            // `--table` names nothing, so there is nothing to cross-check.
            CdcEngine::Mongo => CdcEngineOpts::Mongo {
                canonical: false,
                configured_tables: a.table.clone(),
            },
        },
    };
    let Some(dir) = a.output else {
        // NDJSON to stdout: no durable sink, so the slot is deliberately not
        // advanced (correct at-least-once — the consumer owns durability). Resume
        // for MySQL is the checkpoint file; PostgreSQL re-reads from the slot.
        // No ack ⇒ it cannot page, so `Unbounded`: one peek drains the whole
        // backlog and the LSN-frontier check ends the stream.
        let mut stream = crate::source::cdc::create_change_stream(
            &cdc_cfg,
            crate::source::cdc::PeekBound::Unbounded,
        )?;
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
    // `run_capture` derives the peek bound from this export's `rollover` (below),
    // so the sink and the stream share one source of truth for the part size.
    let now = chrono::Utc::now().to_rfc3339();
    let __cdc_read_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    crate::source::cdc::run_capture(
        crate::source::cdc::CdcCapture {
            // The CLI subcommand has no exports[] block; the table IS the export
            // identity here, which keeps family == export_name == table — exactly
            // the shape the load guard folds to one family.
            export_name: a.table.join("+"),
            cdc_cfg,
            outputs: vec![crate::source::cdc::CaptureOutput {
                table: tbl,
                dest: dest.as_ref(),
                dest_uri: dir,
                // The ad-hoc CLI has no `columns:` surface; config-driven runs do.
                overrides: crate::types::ColumnOverrides::new(),
                // Likewise no `row_hash:` surface — a hash's covered column set is
                // a contract the warehouse table carries, which needs a config file
                // to declare.
                row_hash: crate::config::RowHash::All(false),
            }],
            format: fmt,
            max_events: a.max_events,
            rollover: a.rollover,
            // The SAME protective default the config path applies — absence must not
            // mean "no byte budget" on the ad-hoc CLI either: `--rollover` bounds ROWS,
            // and a wide-row table would buffer unbounded to the row cap. The named
            // runner-bypass shape, at an entry point instead of a runner.
            // Through the NAMED supplier, so this entry point and the config path
            // cannot drift into two defaults.
            rollover_memory_bytes: crate::pipeline::cdc_job::cdc_rollover_memory_bytes(None),
            run_id: now.clone(),
            started_at: now,
            // The ad-hoc `rivet cdc` subcommand runs without a config, and so
            // without a state store to record into. A `mode: cdc` export — the
            // supported path, and the one the sweep and the load read — passes its
            // store, so every part reaches the database as it becomes durable.
            state: None,
        },
        &__cdc_read_bytes,
    )
    // `.1` is the outcome; `.0` is what the drain made durable. The ad-hoc
    // subcommand has no state store and no summary to attribute parts to, so
    // there is nothing here to record them into — unlike `mode: cdc`, where
    // discarding them was the defect this tuple exists to fix.
    .1
}

#[allow(clippy::too_many_arguments)]
fn dispatch_run(args: crate::cli::args::RunArgs) -> Result<()> {
    let crate::cli::args::RunArgs {
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
    } = args;
    let p = parse_params(&params)?;
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
    let p = parse_params(&params)?;
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
    let type_clean = preflight::check(
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
    // This BAILS on a rejection, so the "Looks good" epilogue below only prints
    // when BOTH gates pass — never alongside a "Rejected: …" line (dogfood MED).
    check_plan_compatibility(&config, export.as_deref(), p.as_ref(), json)?;
    if type_clean && !json {
        println!(
            "Looks good. Next: rivet run -c {config} --validate   # export, then verify row counts"
        );
    }
    Ok(())
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
    // CDC exports are not plannable — plan/apply is the batch path; CDC runs via
    // `rivet run`. Skipping them here avoids a misleading "plan did not build"
    // WARN for a valid `mode: cdc` export (which has `tables:`/`table:`, no query).
    let selected: Vec<&crate::config::ExportConfig> = selected
        .into_iter()
        .filter(|e| e.mode != crate::config::ExportMode::Cdc)
        .collect();
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
    tls: Option<crate::config::TlsMode>,
    tls_ca: Option<String>,
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
    let tls_config = resolve_init_tls(tls, tls_ca)?;
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
        tls_config.as_ref(),
    )
    .map_err(|e| {
        // The shared TLS gate prescribes a `source.tls:` config block — a
        // remedy that does not EXIST at init time, because init is the command
        // that creates the config. Recognized by TYPE (never by matching the
        // message text) and re-prescribed as the flag (#146).
        if e.chain().any(|c| {
            c.downcast_ref::<crate::source::TlsRequiredError>()
                .is_some()
        }) {
            e.context(
                "rivet init has no config file to add a `tls:` block to (it generates one) — \
                 re-run with `--tls verify-full` (add `--tls-ca <pem>` for a private CA), or \
                 `--tls disable` if this network path is already trusted; the chosen posture \
                 is written into the scaffold's `source.tls:` block",
            )
        } else {
            e
        }
    })
}

/// `--tls` / `--tls-ca` → the [`TlsConfig`] init connects with and scaffolds.
///
/// PURE so both directions are offline-testable: a CA with a mode that cannot
/// use one (`disable` / `require`) is refused loudly — silently ignoring it
/// would let an operator believe their private CA is being verified while the
/// connection skips verification entirely (#146).
fn resolve_init_tls(
    mode: Option<crate::config::TlsMode>,
    ca: Option<String>,
) -> Result<Option<crate::config::TlsConfig>> {
    use crate::config::TlsMode;
    let Some(mode) = mode else {
        // clap enforces `--tls-ca requires --tls`, so ca is None here too.
        return Ok(None);
    };
    if ca.is_some() && matches!(mode, TlsMode::Disable | TlsMode::Require) {
        anyhow::bail!(
            "--tls-ca is meaningless with `--tls {}`: that mode never verifies the server \
             certificate, so the CA would be silently ignored. Use `--tls verify-ca` or \
             `--tls verify-full` (or drop --tls-ca).",
            mode
        );
    }
    Ok(Some(crate::config::TlsConfig {
        mode,
        ca_file: ca,
        ..Default::default()
    }))
}

fn dispatch_plan(args: crate::cli::args::PlanArgs) -> Result<()> {
    let crate::cli::args::PlanArgs {
        config,
        export,
        params,
        output,
        annotate_waves,
        format,
    } = args;
    let p = parse_params(&params)?;
    let p = if p.is_empty() { None } else { Some(p) };
    if let Some(name) = export.as_deref() {
        check_export_selection(&Config::load_with_params(&config, p.as_ref())?, Some(name))?;
    }
    let fmt = match format {
        PlanFormat::Pretty => pipeline::PlanOutputFormat::Pretty,
        PlanFormat::Json => pipeline::PlanOutputFormat::Json(output),
    };
    pipeline::run_plan_command(&config, export.as_deref(), p.as_ref(), fmt, annotate_waves)
}

#[allow(clippy::too_many_arguments)]
fn dispatch_validate(args: crate::cli::args::ValidateArgs) -> Result<()> {
    let crate::cli::args::ValidateArgs {
        config,
        export,
        format,
        depth,
        output,
        date,
        run_id,
        prefix,
    } = args;
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
    let p = parse_params(&params)?;
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

fn dispatch_repair(args: crate::cli::args::RepairArgs) -> Result<()> {
    let crate::cli::args::RepairArgs {
        config,
        export,
        report,
        execute,
        format,
        output,
        params,
    } = args;
    let p = parse_params(&params)?;
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
        StateAction::Show { config, json } => pipeline::show_state(&config, json),
        StateAction::Reset { config, export } => pipeline::reset_state(&config, &export),
        StateAction::Files {
            config,
            export,
            last,
            json,
        } => pipeline::show_files(&config, export.as_deref(), last, json),
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
        StateAction::Chunks {
            config,
            export,
            json,
        } => pipeline::show_chunk_checkpoint(&config, &export, json),
        StateAction::Progression { config, export } => {
            pipeline::show_progression(&config, export.as_deref())
        }
        StateAction::Runs {
            config,
            running,
            last,
            json,
        } => show_runs(&config, running, last, json),
        StateAction::FinishRun { config, run_id } => finish_run_cmd(&config, &run_id),
        StateAction::Loads {
            config,
            target,
            last,
        } => show_loads(&config, target.as_deref(), last),
    }
}

/// `rivet state runs`: the run-status ledger, newest first — the rows
/// gc_orphans / cleanup_source / the consumed-exclusion actually read.
/// The two run-status commands REFUSE a missing config file: `StateStore::open`
/// derives the DB path from the config's DIRECTORY and would silently create a
/// fresh empty DB — and these commands' empty output is a VERDICT ("no row is
/// freezing any prefix"), so a typo'd `-c` mid-incident reads as a false
/// all-clear (round-5 hostile-input probe, live-verified).
fn require_config_file(config: &str) -> Result<()> {
    if std::path::Path::new(config).is_file() {
        return Ok(());
    }
    anyhow::bail!(
        "config '{config}' is not a file — refusing to answer from the state DB its \
         directory would imply (a fresh empty DB reads as an all-clear verdict)"
    )
}

fn show_runs(config: &str, running_only: bool, last: usize, json: bool) -> Result<()> {
    require_config_file(config)?;
    let store = StateStore::open(config)?;
    let rows = store.recent_run_status(last, running_only)?;
    if json {
        // Machine leg (hostile reviewer: the footer drives a scripted flow) —
        // same shape discipline as the sibling `--json` listings.
        let arr: Vec<serde_json::Value> = rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "run_id": r.run_id,
                    "export_name": r.export_name,
                    "prefix": r.prefix,
                    "status": r.status,
                    "started_at": r.started_at,
                    "finished_at": if r.finished_at.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::Value::String(r.finished_at.clone())
                    },
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&arr)?);
        return Ok(());
    }
    if rows.is_empty() {
        // `--last 0` clips everything: an empty listing then says NOTHING about
        // the DB, and "no running rows" would be an actively false all-clear
        // (round-5 hostile-input probe).
        let msg = if last == 0 {
            "--last 0 shows no rows by construction — raise it to inspect the ledger"
        } else if running_only {
            "no running rows — no run-status row is freezing any prefix"
        } else {
            "no runs recorded in the state DB yet"
        };
        println!("{msg}");
        return Ok(());
    }
    println!(
        "{:<28} {:<20} {:<11} {:<25} {:<25} PREFIX",
        "RUN ID", "EXPORT", "STATUS", "STARTED", "FINISHED"
    );
    for r in &rows {
        let finished = if r.finished_at.is_empty() {
            "-"
        } else {
            r.finished_at.as_str()
        };
        println!(
            "{:<28} {:<20} {:<11} {:<25} {:<25} {}",
            r.run_id, r.export_name, r.status, r.started_at, finished, r.prefix
        );
    }
    if running_only {
        println!(
            "\nA `running` row with NO live extract is a crash remnant: it makes gc spare \
             the prefix and cleanup refuse, forever (supersession needs a newer SUCCESS). \
             Close one you KNOW is dead with `rivet state finish-run -c <config> --run-id <id>`."
        );
    }
    Ok(())
}

/// `rivet state finish-run`: stamp a dead run's row `interrupted`. Refuses
/// loudly on a typo'd id and no-ops honestly on an already-terminal row —
/// only a `running` row is ever touched.
fn finish_run_cmd(config: &str, run_id: &str) -> Result<()> {
    use crate::state::FinishOutcome;
    require_config_file(config)?;
    let store = StateStore::open(config)?;
    match store.finish_run_checked(run_id, &chrono::Utc::now().to_rfc3339())? {
        FinishOutcome::Stamped => {
            println!(
                "run '{run_id}' stamped `interrupted` — the ledger no longer counts it as \
                 writing. If the export writes to a CLOUD destination, its crash left a \
                 `running` marker in the bucket too; the next `rivet load` over this state \
                 DB retires that marker in its gc pass (a load from another host cannot, \
                 and keeps sparing conservatively). Only stamp runs you KNOW are dead: a \
                 LIVE co-located run stamped here loses its ledger-side gc protection."
            );
            Ok(())
        }
        FinishOutcome::AlreadyTerminal(status) => {
            println!(
                "run '{run_id}' is already terminal (`{status}`) — nothing to do; it does \
                 not freeze any prefix"
            );
            Ok(())
        }
        FinishOutcome::NotFound => anyhow::bail!(
            "no run-status row has run_id '{run_id}' — check `rivet state runs -c \
             <config>`; refusing to report success for a stamp that touched nothing"
        ),
    }
}

/// `rivet state loads`: print the load ledger — one row per recorded `rivet
/// load`, newest first.
/// The empty-`state loads` message, or `None` when nothing should print. A
/// zero-row request (`--last 0`) prints nothing (matching `--json []`); an
/// unmatched `--target` filter says so rather than claiming the ledger is empty;
/// an unfiltered empty result keeps the "no loads recorded" line (dogfood LOW —
/// the old code printed "empty state DB" even for a filter miss on a full ledger).
fn empty_loads_message(target: Option<&str>, last: usize) -> Option<String> {
    if last == 0 {
        return None;
    }
    Some(match target {
        Some(t) => format!("no loads match --target '{t}'"),
        None => "no loads recorded in the state DB yet".to_string(),
    })
}

fn show_loads(config: &str, target: Option<&str>, last: usize) -> Result<()> {
    let store = StateStore::open(config)?;
    let loads = store.recent_loads(target, last)?;
    if loads.is_empty() {
        if let Some(msg) = empty_loads_message(target, last) {
            println!("{msg}");
        }
        return Ok(());
    }
    println!(
        "{:<26} {:<10} {:<6} {:>10} {:<8} {:>4}  target",
        "finished_at", "warehouse", "mode", "rows", "status", "runs"
    );
    for l in &loads {
        println!(
            "{:<26} {:<10} {:<6} {:>10} {:<8} {:>4}  {}",
            l.finished_at,
            l.warehouse,
            l.mode,
            l.rows_loaded,
            l.status,
            l.source_run_ids.len(),
            l.target_table,
        );
    }
    Ok(())
}

#[cfg(test)]
mod loads_listing_tests {
    use super::empty_loads_message;

    #[test]
    fn empty_loads_message_distinguishes_zero_request_and_filter_miss() {
        // #dogfood LOW: `--last 0` and an unmatched `--target` were both reported
        // as "no loads recorded in the state DB yet" on a NON-empty ledger.
        assert_eq!(empty_loads_message(None, 0), None); // --last 0 → print nothing
        assert_eq!(empty_loads_message(Some("x"), 0), None);
        assert_eq!(
            empty_loads_message(Some("x"), 5).unwrap(),
            "no loads match --target 'x'"
        );
        assert!(
            empty_loads_message(None, 5)
                .unwrap()
                .contains("no loads recorded")
        );
    }
}

#[cfg(test)]
mod init_tls_tests {
    use super::resolve_init_tls;
    use crate::config::TlsMode;

    /// Both directions of the pure resolver — an over-strict and an
    /// under-strict `--tls-ca` handling are the same bug from opposite sides.
    #[test]
    fn tls_ca_pairs_only_with_verifying_modes() {
        for mode in [TlsMode::VerifyCa, TlsMode::VerifyFull] {
            let t = resolve_init_tls(Some(mode), Some("/ca.pem".into()))
                .expect("verifying mode + CA is the intended pairing")
                .expect("some config");
            assert_eq!(t.ca_file.as_deref(), Some("/ca.pem"));
        }
        for mode in [TlsMode::Disable, TlsMode::Require] {
            let err = resolve_init_tls(Some(mode), Some("/ca.pem".into()))
                .expect_err("a CA that would be silently ignored must be refused");
            assert!(err.to_string().contains("--tls-ca"), "{err}");
        }
        assert!(resolve_init_tls(None, None).unwrap().is_none());
        // A bare mode carries no CA and no other TlsConfig noise.
        let t = resolve_init_tls(Some(TlsMode::Disable), None)
            .unwrap()
            .unwrap();
        assert_eq!(t.mode, TlsMode::Disable);
        assert!(t.ca_file.is_none() && !t.accept_invalid_certs);
    }

    /// The TLS gate's remedy, as `rivet init` sees it: a config-block
    /// prescription is unreachable from the command that CREATES the config,
    /// so init's dispatch must re-prescribe the FLAG. Recognized by TYPE via
    /// the gate's marker error — this test goes through the real dispatch and
    /// the real gate (TEST-NET address: the refusal fires before any socket).
    #[test]
    fn init_against_a_remote_host_names_the_flag_not_the_config_block() {
        let err = super::dispatch_init(
            Some("mysql://u:p@203.0.113.9:3306/db".into()),
            None,
            None,
            None,
            None,
            vec![],
            vec![],
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .expect_err("remote + no --tls must refuse");
        let text = format!("{err:#}");
        assert!(text.contains("--tls verify-full"), "{text}");
        assert!(
            err.chain().any(|c| c
                .downcast_ref::<crate::source::TlsRequiredError>()
                .is_some()),
            "the refusal must carry the typed marker, not just prose"
        );
    }
}

#[cfg(test)]
mod cdc_stream_polarity_tests {
    use super::cdc_until_current;

    /// `--stream` and `until_current` are exact opposites. Both directions,
    /// because an inverted polarity is the same bug from either side: a bounded
    /// run that tails forever, or a daemon that clips at the open bound and
    /// silently stops following the log.
    #[test]
    fn stream_is_the_exact_inverse_of_the_open_time_bound() {
        assert!(
            cdc_until_current(false),
            "no --stream: a `rivet cdc` run drains to the OPEN-TIME snapshot and exits"
        );
        assert!(
            !cdc_until_current(true),
            "--stream is the daemon: it must NOT clip at the open bound"
        );
    }
}
