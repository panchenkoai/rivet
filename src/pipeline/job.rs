use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::{DataIntegrityError, Result};
use crate::plan::{
    DiagnosticLevel, ExtractionStrategy, ResolvedRunPlan, build_plan, validate_plan,
};
use crate::state::StateStore;

use super::RunOptions;
use super::chunked::{self, run_chunked_parallel_checkpoint};
use super::single::{commit_incremental_cursor, run_with_reconnect};
use super::summary::RunSummary;
use crate::journal::RunEvent;

/// Classify a raw error message into a STABLE `error_class` so failures group and
/// trend without `LIKE '%…%'` string-matching (the 0.21.2 field post-mortem grouped
/// 43 failures into 3 classes by hand). Ordered MOST-SPECIFIC first — the
/// parallel-checkpoint wrapper embeds the inner `ERROR 3024` text, so it must match
/// before `statement_timeout`. Returns `None` when nothing matches: an unclassified
/// error is honest, not force-fit into a bucket.
pub(crate) fn classify_error_message(msg: &str) -> Option<&'static str> {
    let m = msg.to_ascii_lowercase();
    if m.contains("keyset could not read") || m.contains("could not read the key value") {
        Some("keyset_unreadable_key")
    } else if m.contains("parallel checkpoint worker") {
        Some("parallel_checkpoint")
    } else if m.contains("deadlock") {
        Some("deadlock")
    } else if m.contains("lock wait timeout") || m.contains("could not obtain lock") {
        // A lock-wait embeds "timeout" — match it before statement_timeout.
        Some("lock_timeout")
    } else if m.contains("3024")
        || m.contains("maximum statement execution time")
        || m.contains("statement timeout")
    {
        Some("statement_timeout")
    } else if m.contains("schema drift")
        || m.contains("schema changed")
        || m.contains("on_schema_drift")
    {
        Some("schema_drift")
    } else if m.contains("doesn't exist")
        || m.contains("does not exist")
        || m.contains("no such table")
        || m.contains("invalid object name")
        || m.contains("unknown table")
    {
        Some("relation_not_found")
    } else if m.contains("permission denied")
        || m.contains("command denied")
        || m.contains("privilege")
        || m.contains("view server state")
    {
        Some("privilege")
    } else if m.contains("access denied")
        || m.contains("authentication failed")
        || m.contains("password authentication")
    {
        Some("source_auth")
    } else if m.contains("connection reset")
        || m.contains("connection refused")
        || m.contains("connection closed")
        || m.contains("broken pipe")
        || m.contains("server has gone away")
        || m.contains("connection timed out")
    {
        Some("connection")
    } else if m.contains("certificate") || m.contains("tls handshake") || m.contains("ssl error") {
        Some("tls")
    } else if m.contains("no space left") || m.contains("disk full") || m.contains("quota exceeded")
    {
        Some("disk_full")
    } else if m.contains("out of memory") || m.contains("cannot allocate memory") {
        Some("out_of_memory")
    } else {
        None
    }
}

/// The resolved strategy's `(label, key column)` — the column the run pages by.
/// `None` for full/snapshot (no key). Shared by `key_descriptor_json` and the open
/// probe, which resolves the key's native type against this name.
fn strategy_key_column(plan: &ResolvedRunPlan) -> Option<(&'static str, &str)> {
    match &plan.strategy {
        ExtractionStrategy::Keyset(k) => Some(("keyset", &k.key_column)),
        ExtractionStrategy::Chunked(c) => Some(("chunked", &c.column)),
        ExtractionStrategy::Incremental(i) => Some(("incremental", &i.primary_column)),
        _ => None,
    }
}

/// A compact JSON descriptor of the resolved strategy's KEY: `{strategy, key}` plus
/// `db_type` + an `unsigned` flag when the open probe resolved the key's SOURCE
/// native type (the "was it unsigned" answer inline, which the Arrow repr elides).
/// `is_primary_key` is implicit for keyset — the planner requires a unique index —
/// so it is not restated.
fn key_descriptor_json(plan: &ResolvedRunPlan, key_native_type: Option<&str>) -> Option<String> {
    let (strategy, key) = strategy_key_column(plan)?;
    let mut obj = serde_json::json!({ "strategy": strategy, "key": key });
    if let Some(t) = key_native_type {
        obj["db_type"] = serde_json::Value::String(t.to_string());
        if t.to_ascii_lowercase().contains("unsigned") {
            obj["unsigned"] = serde_json::Value::Bool(true);
        }
    }
    Some(obj.to_string())
}

/// Capture failure-forensics context at run OPEN, best-effort. `export_schema` is
/// otherwise SUCCESS-only, so a run that fails before finalize leaves no schema —
/// the exact case a post-mortem needs (the 0.21.2 field DB had no schema for its 43
/// failures). From one short-lived open probe this records:
///  * the source SCHEMA (via `type_mappings`, LIMIT-0 so even a page-0 failure
///    captures the columns/types) → `store_schema`, keyed by export like the
///    success path (which OVERWRITES it, so a successful run is unaffected), and
///  * the source SERVER context (version + limits) → `summary.server_context_json`.
///
/// Never fails the run: any probe error is logged at debug and dropped. One extra
/// lightweight connection per export — cheap relative to the run it forensicates.
fn capture_open_forensics(plan: &ResolvedRunPlan, state: &StateStore, summary: &mut RunSummary) {
    let mut src = match crate::source::create_source(&plan.source) {
        Ok(s) => s,
        Err(e) => {
            log::debug!(
                "open-forensics: source connect failed for '{}': {e}",
                plan.export_name
            );
            return;
        }
    };
    summary.server_context_json = src.server_context();
    match src.type_mappings(&plan.base_query, &plan.column_overrides) {
        Ok(mappings) => {
            // Arrow repr matches the success path's `arrow_schema_to_columns` format
            // (so a later successful overwrite is consistent), and still names
            // unsignedness (`UInt64`). Fall back to the source native type only when
            // Arrow is None (an Unsupported type).
            let cols: Vec<crate::state::SchemaColumn> = mappings
                .iter()
                .map(|m| crate::state::SchemaColumn {
                    name: m.column_name.clone(),
                    data_type: m
                        .arrow_type
                        .as_ref()
                        .map(|t| format!("{t:?}"))
                        .unwrap_or_else(|| m.source_native_type.clone()),
                })
                .collect();
            // Resolve the KEY column's source native type for key_descriptor_json —
            // it names signedness ("bigint unsigned") that the Arrow repr elides.
            if let Some((_, key)) = strategy_key_column(plan) {
                summary.key_native_type = mappings
                    .iter()
                    .find(|m| m.column_name == key)
                    .map(|m| m.source_native_type.clone());
            }
            // store_schema_if_absent, NOT store_schema: a stored baseline is the
            // drift detector's comparison anchor, read by `detect_schema_change`
            // DURING the export (after this open probe). Overwriting it here would
            // blind schema-drift detection — a changed column would compare equal to
            // the just-stored current schema. A first-run failure (no baseline) is
            // exactly what this forensics exists for and still captures its schema.
            if let Err(e) = state.store_schema_if_absent(&summary.export_name, &cols) {
                log::debug!(
                    "open-forensics: store_schema_if_absent failed for '{}': {e}",
                    summary.export_name
                );
            }
        }
        Err(e) => log::debug!(
            "open-forensics: type_mappings failed for '{}': {e}",
            plan.export_name
        ),
    }
}

/// Assemble the full `export_metrics` row (v9) from the finished run summary +
/// plan. One builder so the run and apply paths persist an identical shape
/// rather than each inlining the metric fields. The richer signals
/// (`pg_temp_bytes_delta`, `reconciled`/`source_count`, effective `batch_size`,
/// config dims, `rivet_version`) are what `record_metric`'s old 15-arg shim
/// dropped on the floor — they exist on the summary/plan here, so persist them.
fn build_metric_row(
    summary: &RunSummary,
    plan: &ResolvedRunPlan,
    tuning_class: &str,
) -> crate::state::MetricRow {
    let (chunk_size, parallel) = match &plan.strategy {
        crate::plan::ExtractionStrategy::Chunked(cp) => {
            (Some(cp.chunk_size as i64), Some(cp.parallel as i64))
        }
        // #151: keyset runs demonstrably fanned N workers while the metric
        // stayed NULL — the runner-bypass class, in a metrics field.
        crate::plan::ExtractionStrategy::Keyset(kp) => {
            (Some(kp.chunk_size as i64), Some(kp.parallel.max(1) as i64))
        }
        _ => (None, None),
    };
    crate::state::MetricRow {
        export_name: summary.export_name.clone(),
        run_id: summary.run_id.clone(),
        duration_ms: summary.duration_ms,
        total_rows: summary.total_rows,
        peak_rss_mb: Some(summary.peak_rss_mb),
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        tuning_profile: Some(tuning_class.to_string()),
        format: Some(summary.format.clone()),
        mode: Some(summary.mode.clone()),
        files_produced: summary.files_produced as i64,
        bytes_written: summary.bytes_written as i64,
        bytes_read: summary.bytes_read as i64,
        retries: summary.retries as i64,
        validated: summary.validated,
        schema_changed: summary.schema_changed,
        files_committed: summary.files_committed as i64,
        reconciled: summary.reconciled,
        source_count: summary.source_count,
        quality_passed: summary.quality_passed,
        pg_temp_bytes_delta: summary.pg_temp_bytes_delta,
        batch_size: summary.batch_size as i64,
        batch_size_memory_mb: summary.batch_size_memory_mb.map(|m| m as i64),
        skip_reason: summary.skip_reason.clone(),
        schema_fingerprint: summary.schema_fingerprint.clone(),
        chunk_size,
        parallel,
        source_type: Some(format!("{:?}", plan.source.source_type).to_lowercase()),
        destination_type: Some(plan.destination.destination_type.label().to_string()),
        rivet_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        longest_chunk_ms: summary.journal.longest_chunk_ms(),
        // v12 chunking diagnostics: which key was chunked. (The resolved strategy
        // is already `mode` = strategy.mode_label.) A sparse-key post-mortem is now
        // one SELECT: mode + chunk_key (+ chunk_task for span/windows).
        chunk_key: plan.strategy.chunk_key().map(str::to_string),
        // ── v18 failure forensics — write-point map ──
        // WIRED (data already on the summary/plan at this point):
        error_class: summary
            .error_message
            .as_deref()
            .and_then(classify_error_message)
            .map(str::to_string),
        cursor_min: summary.cursor_low.clone(),
        cursor_max: summary.cursor_high.clone(),
        key_descriptor_json: key_descriptor_json(plan, summary.key_native_type.as_deref()),
        // Producers now wired: offending_value is stamped at the keyset throw
        // (`pipeline/keyset.rs`); server_context_json is captured at open by
        // `capture_open_forensics` (source `server_context()`).
        offending_value: summary.offending_value.clone(),
        server_context_json: summary.server_context_json.clone(),
    }
}

fn run_chunked_quality_gate(
    result: Result<()>,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<()> {
    result?;

    // The MULTI-PART runners — chunked AND keyset (which also backs parallel-Mongo,
    // routed through the Keyset strategy) — own their own execution loop and never
    // reach single mode's per-part sink.run_quality_checks(). So the run-wide
    // row_count completeness gate (row_count_min, Severity::Fail → exit 3) must run
    // HERE for all of them. It was Chunked-only, so a truncated keyset/parallel
    // extract (the paths auto-selected for LARGE tables, where completeness matters
    // most) exited 0/success with the tripwire silently disarmed.
    if !matches!(
        plan.strategy,
        ExtractionStrategy::Chunked(_) | ExtractionStrategy::Keyset(_)
    ) {
        return Ok(());
    }
    let qc = match &plan.quality {
        Some(q) => q,
        None => return Ok(()),
    };

    let total = summary.total_rows as usize;
    let row_issues = crate::quality::check_row_count(total, qc);
    let has_unsupported = !qc.null_ratio_max.is_empty() || !qc.unique_columns.is_empty();

    if has_unsupported {
        log::warn!(
            "export '{}': quality checks null_ratio_max and unique_columns are not supported on the multi-part runners (chunked / keyset / parallel-Mongo) — each part processes independently; only row_count bounds are checked",
            plan.export_name
        );
    }

    if !row_issues.is_empty() {
        for issue in &row_issues {
            log::warn!("quality FAIL: {}", issue.message);
        }
        summary.quality_passed = Some(false);
        // Surface *which* checks failed via the shared failure contract — see
        // `crate::quality::failure_message`. (Chunked mode only aggregates
        // row_count; null/unique are per-chunk and warn-logged above.) Tagged as
        // a data-integrity failure (exit 3) so a scheduler stops rather than
        // retries; the message text is unchanged.
        let fails: Vec<&str> = row_issues.iter().map(|i| i.message.as_str()).collect();
        return Err(DataIntegrityError::new(crate::quality::failure_message(
            &plan.export_name,
            Some("multi-part aggregate"),
            &fails,
        ))
        .into());
    }

    summary.quality_passed = Some(true);
    Ok(())
}

/// Snapshot `pg_stat_database.temp_bytes` for the run's source DB.
///
/// `None` for non-Postgres sources (no equivalent counter), or when the
/// snapshot probe fails (URL unresolvable, connection refused, view filtered).
/// Failures are silent — this is an observability metric, not a correctness
/// signal, so a failed probe must never block the actual export.
fn pg_temp_bytes_snapshot(plan: &ResolvedRunPlan) -> Option<i64> {
    if !matches!(plan.source.source_type, crate::config::SourceType::Postgres) {
        return None;
    }
    let url = plan.source.resolve_url().ok()?;
    crate::source::postgres::sample_temp_bytes(&url, plan.source.tls.as_ref())
}

/// The temp-spill the run gets CREDITED with, from the two snapshots bracketing
/// its window.
///
/// Pure and split out from its two call sites — `run_export_job` and
/// `run_export_job_with_chunk_source` — because BOTH of those need a live
/// PostgreSQL source to reach, so the arithmetic that decides the stored
/// `pg_temp_bytes_delta` AND whether [`pg_temp_bytes_warning`] fires was graded
/// by nothing offline. The first non-vacuous mutation run (2026-08-16) scored
/// `-` → `+` and `-` → `/` as survivors at the apply call site, and the live
/// oracle cannot see them either — the ONE live assertion on this field,
/// `pg_chunked_run_persists_extended_metric_columns`
/// (`tests/live/live_metrics_persist.rs`), asserts
/// `pg_temp_bytes_delta.is_some()`, which is equally true of a SUM (and the
/// apply-path metric test asserts nothing about the field at all). A `+`
/// reports the absolute counter (a database that has ever
/// spilled 200 MB warns "+200 MB spill" on every export that spills nothing) —
/// the false-alarm harm class, not a crash.
///
/// Floored at 0 because `pg_stat_database.temp_bytes` restarts at `pg_stat_reset()`
/// and at a server restart: `after < before` means the counter reset mid-window,
/// not that the run RECLAIMED spill, and a negative "credit" would offset a
/// sibling's real spill in any consumer that sums.
fn pg_temp_bytes_delta(before: i64, after: i64) -> i64 {
    (after - before).max(0)
}

/// Volume above which a `temp_bytes` delta is worth a WARN (100 MB).
const PG_TEMP_BYTES_WARN_MIN: i64 = 100 * 1024 * 1024;

/// The per-export PG temp-spill warning, pure so its wording is unit-tested.
///
/// `pg_stat_database.temp_bytes` is DATABASE-wide, exactly like the tmp-disk
/// counter `run_diagnosis` flags: with concurrent sibling exports (--pool,
/// --parallel-exports, --parallel-export-processes, apply --parallel) the
/// window overlaps every sibling's work, so attributing the whole delta to THIS
/// export is a measurement lie. The warning stays LOUD either way — it IS real
/// source pressure — but under concurrency it says the attribution is unknown
/// instead of naming one export as the cause (the same hedge the spill flag
/// carries; leaving it off this sibling was the last unhedged copy).
///
/// `None` below [`PG_TEMP_BYTES_WARN_MIN`] — a small spill is noise.
fn pg_temp_bytes_warning(
    export_name: &str,
    delta: i64,
    concurrent_siblings: bool,
) -> Option<String> {
    if delta <= PG_TEMP_BYTES_WARN_MIN {
        return None;
    }
    let mb = delta as f64 / (1024.0 * 1024.0);
    let remedy = "Consider lowering `tuning.batch_size` or setting \
                  `tuning.batch_size_memory_mb` below PG's `work_mem`.";
    Some(if concurrent_siblings {
        format!(
            "export '{export_name}': PG temp_bytes spill +{mb:.1} MB during this export's \
             window — real source pressure, but `pg_stat_database.temp_bytes` is \
             database-wide and concurrent sibling exports share the window, so per-export \
             attribution is unknown. If it tracks this export: {remedy}"
        )
    } else {
        format!(
            "export '{export_name}': PG temp_bytes spill +{mb:.1} MB during run — \
             cursor / sort overflow. {remedy}"
        )
    })
}

/// Snapshot the broader source-harm counters for the run's source engine, as
/// `(metric, cumulative_value)` pairs, dispatched by source type to the
/// per-engine probe. `None` for any connect/query failure or a source whose
/// probe is unavailable (e.g. MSSQL without `VIEW SERVER STATE`) — harm metrics
/// are observability, never a gate, so a missing snapshot just yields no
/// `export_harm` rows.
pub(super) fn harm_snapshot(source: &crate::config::SourceConfig) -> Option<Vec<(String, i64)>> {
    // One dispatch for "which engine": `create_source`. The former per-engine
    // free-fn switch here duplicated it (walk find, 2026-08-13); the harm
    // probe is now the trait's third telemetry axis
    // (`Source::harm_counters`), so any caller with a Source instance — or a
    // test with a fake — reaches it without a URL round-trip.
    crate::source::create_source(source).ok()?.harm_counters()
}

/// Per-metric delta (`after - before`, floored at 0) for counters present in
/// both snapshots, matched by name. Floored because these are monotonic
/// cumulative counters within a run; a negative would only arise from a counter
/// reset (server restart mid-run) and is not meaningful harm.
pub(super) fn harm_deltas(before: &[(String, i64)], after: &[(String, i64)]) -> Vec<(String, i64)> {
    let bmap: std::collections::HashMap<&str, i64> =
        before.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    after
        .iter()
        .filter_map(|(k, after_v)| {
            bmap.get(k.as_str())
                .map(|b| (k.clone(), (after_v - b).max(0)))
        })
        .collect()
}

/// A one-line run-health DIAGNOSIS for the operator's log — the flaky-link /
/// messy-DB signals a field log must carry so a log the team sends back
/// self-diagnoses. Returns `Some(line)` only when there is something worth
/// flagging (a reconnect the run survived, a resume-hit meaning the prior run
/// crashed, or a source-side tmp-disk spill that `export_harm` records but never
/// LOGGED before); a clean run returns `None` — its stats already live in the run
/// card. Emitted at WARN so it is visible at the default log level (INFO is not).
/// Pure so the wording is unit-tested without a run. PG temp-byte spills are warned
/// separately (`pg_temp_bytes_delta`), so they are not duplicated here.
/// Spill count below which a run is not worth flagging — shared by the
/// per-export DIAGNOSIS and the pool-window harm verdict so the two rules
/// cannot drift (the pool copy used to inline its own `100`).
pub(super) const SPILL_FLAG_MIN: i64 = 100;

/// What a [`spill_total`] fold counted, so the message can name it truthfully.
pub(super) struct Spill {
    /// Summed delta across every spill counter the engine reported.
    pub total: i64,
    /// The NOUN for what `total` counts — the units differ in MEANING between
    /// engines (MySQL counts tmp-disk TABLES, PostgreSQL counts temp FILES), so
    /// the caller interpolates this rather than hard-coding one engine's word.
    pub unit: &'static str,
}

/// Sum the source-harm deltas that mean "the source had to spill to disk",
/// across EVERY engine that has such a counter — one fold, shared by the
/// per-export DIAGNOSIS (`run_diagnosis`) and the run-level verdict
/// (`run::run_harm_verdict`) for the same reason [`SPILL_FLAG_MIN`] is shared:
/// two copies of the filter drift, and the first drift already happened —
/// both sites matched `tmp_disk` only, so PostgreSQL's `pg_temp_files` (the
/// direct spill analogue, `src/source/postgres/mod.rs::harm_counters`) tripped
/// NEITHER rule and the spill signal was silently absent on PG.
///
/// Engines without a spill counter stay silent, and that is correct, not a gap:
/// MSSQL reports `mssql_lock_waits`/`mssql_lock_wait_ms` (contention, not
/// spill) and Mongo reports scan/cache counters.
///
/// PG's `temp_bytes` VOLUME is warned separately per export
/// (`pg_temp_bytes_warning`); `pg_temp_files` is the file COUNT, a different
/// counter, so flagging it here is not a duplicate of that warning.
pub(super) fn spill_total(deltas: &[(String, i64)]) -> Spill {
    let sum = |needle: &str| -> i64 {
        deltas
            .iter()
            .filter(|(k, _)| k.contains(needle))
            .map(|(_, v)| *v)
            .sum()
    };
    // MySQL: `Created_tmp_disk_tables` (raw, or `mysql_`-prefixed by the probe).
    let tmp_disk = sum("tmp_disk");
    // PostgreSQL: `pg_temp_files`.
    let temp_files = sum("temp_files");
    let unit = match (tmp_disk > 0, temp_files > 0) {
        // One source engine per run, so the mixed arm is defensive only; it must
        // still not claim either engine's unit for the other's count.
        (true, true) => "disk spills (tmp-disk tables + temp files)",
        (false, true) => "temp-file spills",
        _ => "tmp-disk spills",
    };
    Spill {
        total: tmp_disk + temp_files,
        unit,
    }
}

pub(super) fn run_diagnosis(
    summary: &RunSummary,
    harm_deltas: &[(String, i64)],
    concurrent_siblings: bool,
) -> Option<String> {
    let mut flags: Vec<String> = Vec::new();
    if summary.reconnects > 0 {
        flags.push(format!(
            "{} reconnect(s) survived (flaky link)",
            summary.reconnects
        ));
    }
    if summary.resumed {
        flags.push("resumed a prior CRASHED run".to_string());
    }
    let Spill {
        total: spills,
        unit: spill_unit,
    } = spill_total(harm_deltas);
    if spills >= SPILL_FLAG_MIN {
        // The remedy must not suggest what the export ALREADY runs (a keyset
        // export told to "try chunk_by_key" reads as a broken diagnostic and
        // hides the real lever): on chunked/keyset the paging is already on,
        // so the remaining levers are the page/batch sizes.
        let remedy = match summary.mode.as_str() {
            "chunked" | "keyset" => "lower `chunk_size` (smaller pages) or `tuning.batch_size`",
            _ => "try `mode: chunked`/`chunk_by_key` or a smaller `tuning.batch_size`",
        };
        // The spill counters come from `SHOW GLOBAL STATUS` / `pg_stat_database`
        // — SERVER-wide, not per-connection. Solo they are a fair attribution
        // (rivet is the only query stream it knows of); with concurrent sibling
        // exports (--pool / --parallel-exports / --parallel-export-processes)
        // the window overlaps every sibling's work, so blaming THIS export would
        // be a measurement lie (field find, 2026-08-13: a pool run attributed a
        // 5-slot window's spills to the one export whose card they printed
        // beside). The run-level line this points at is emitted by every
        // concurrent-sibling parent via `run::RunHarmBracket`, so the pointer is
        // true on the pool AND the parallel paths, not just the pool.
        if concurrent_siblings {
            flags.push(format!(
                "{spills} {spill_unit} server-wide during this export's window — real \
                 source pressure, but the counter is server-global and concurrent \
                 sibling exports share the window, so per-export attribution is \
                 unknown (the run-level harm line carries the whole-window total); \
                 if it tracks this export, {remedy}"
            ));
        } else {
            flags.push(format!(
                "{spills} {spill_unit} — the source spilled to disk; {remedy}"
            ));
        }
    }
    if flags.is_empty() {
        return None;
    }
    Some(format!(
        "export '{}': DIAGNOSIS — {} rows · peak RSS {} MB · {} ms [{}] · retries={} · {}",
        summary.export_name,
        summary.total_rows,
        summary.peak_rss_mb,
        summary.duration_ms,
        summary.status,
        summary.retries,
        flags.join("; "),
    ))
}

/// Run `SELECT COUNT(*) FROM ({query})` against the source and compare with exported rows.
/// Skips reconciliation for incremental exports that used a cursor (moving target).
/// Exit gate for `run --reconcile`: a row-count mismatch is a data-integrity
/// failure (exit 3), mirroring the `rivet reconcile` subcommand
/// (`enforce_reconcile_exit`) and `--validate`. Returns `Err(DataIntegrityError)`
/// when a reconcile pass ran and disagreed with the source, so
/// `rivet run --reconcile && <next>` does not proceed past a mismatch;
/// a match or a skipped reconcile (`reconciled == None`) returns `Ok`. The
/// exported data is already durable — only the gate fails.
/// Fold the run's outcome into the process result: an export/quality failure
/// wins; otherwise a `run --reconcile` mismatch (`reconcile_gate`) surfaces so
/// the run exits non-zero (exit 3). Extracted as a pure fn so the WIRING — not
/// just the gate logic — is unit-tested: without this seam, un-hooking the gate
/// from `final_result` would silently reopen the "run --reconcile exits 0" bug
/// (#102) with every existing test still green.
fn resolve_final_result(
    failed: bool,
    run_result: crate::error::Result<()>,
    reconcile_gate: crate::error::Result<()>,
    manifest_gap: Option<String>,
) -> crate::error::Result<()> {
    if failed {
        return run_result;
    }
    // The manifest gap outranks the reconcile verdict. Reconcile answers "is the
    // data right?"; this answers "is the data REACHABLE?" — and an unreachable
    // prefix makes the first question moot. Ordered before so the exit code names
    // the condition an operator must act on first.
    if let Some(why) = manifest_gap {
        return Err(anyhow::anyhow!(why));
    }
    reconcile_gate
}

fn reconcile_run_gate(
    summary: &RunSummary,
    could_not_verify: Option<&str>,
) -> crate::error::Result<()> {
    // VERIFIED-WRONG: the count ran and DISAGREED → data-integrity, exit 3.
    if summary.reconciled == Some(false) {
        return Err(crate::error::DataIntegrityError::new(format!(
            "reconcile MISMATCH for '{}': the exported dataset disagrees with the source \
             count {} — see the reconcile log above",
            summary.export_name,
            summary.source_count.unwrap_or(-1),
        ))
        .into());
    }
    // COULD-NOT-VERIFY: reconcile was requested but could not run (source
    // unreachable, count NULL / non-integer / query error). The export itself
    // succeeded and is durable, but the assurance the operator asked for was not
    // obtained — an OPERATIONAL failure (exit 1, retry), NOT verified-OK (exit 0)
    // and NOT corruption (exit 3). Diverged from the `reconcile` subcommand before,
    // which propagated the same connect error (#10 bughunt).
    if let Some(reason) = could_not_verify {
        anyhow::bail!(
            "reconcile could not be verified for '{}': {reason}. The export completed and is \
             durable; re-run to obtain the reconcile assurance, or drop `--reconcile`.",
            summary.export_name
        );
    }
    Ok(())
}

/// Run the reconcile COUNT(*) and record `source_count` / `reconciled` on the
/// summary. Returns `Some(reason)` when reconcile COULD NOT run (the source was
/// unreachable, the count query failed, or its result was NULL / non-integer) —
/// a COULD-NOT-VERIFY condition the caller turns into an operational exit 1,
/// distinct from a genuine MISMATCH (`reconciled == Some(false)` → exit 3) and
/// from a legitimate SUBSET-strategy skip (`None`, exit 0). Before this a
/// could-not-verify left `reconciled = None`, indistinguishable from the skip, so
/// `run --reconcile` exited 0 as if verified-OK (#10 bughunt).
fn reconcile_source_count(plan: &ResolvedRunPlan, summary: &mut RunSummary) -> Option<String> {
    // Skip the full-source COUNT(*) for any SUBSET/DELTA strategy — its exported
    // count legitimately differs from the table total, and the #102 exit gate
    // must not turn that STRUCTURAL mismatch into a false exit-3 (which would
    // break every healthy scheduled keyset_incremental / time_window / incremental
    // `run --reconcile`). Previously only the Incremental cursor was skipped. This
    // is a SKIP, not a could-not-verify — return None (exit 0).
    if let Some(reason) = plan.strategy.reconcile_subset_skip() {
        log::info!(
            "reconcile: skipping full-count for '{}' ({reason})",
            plan.export_name
        );
        return None;
    }

    let count_sql = format!(
        "SELECT COUNT(*) FROM ({}) AS _rivet_reconcile",
        plan.base_query
    );
    log::info!(
        "reconcile: running source count query for '{}'",
        plan.export_name
    );

    let mut src = match crate::source::create_source(&plan.source) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("reconcile: could not connect to source: {:#}", e);
            return Some(format!(
                "could not connect to the source to reconcile: {e:#}"
            ));
        }
    };

    match src.query_scalar(&count_sql) {
        Ok(Some(val)) => {
            let Ok(count) = val.parse::<i64>() else {
                // A non-integer COUNT is COULD-NOT-VERIFY (we cannot compare), NOT
                // a mismatch (#10): operational exit 1, not the exit-3 corruption class.
                log::warn!("reconcile: could not parse count result '{val}' as integer");
                return Some(format!("source reconcile count '{val}' is not an integer"));
            };
            summary.source_count = Some(count);
            // ADR-0012 manifest-aware reconcile: compare source COUNT(*)
            // against the manifest's *cumulative* row total (sum of
            // committed parts), not just this run's writes.  In a
            // resume scenario, `summary.total_rows` reflects only the
            // chunks that re-ran in this invocation (e.g. 500 for one
            // chunk), while the on-disk dataset is everything that
            // ever committed (e.g. 2500 across resume attempts).
            // Comparing total_rows would falsely report MISMATCH on
            // every resume.  The manifest_parts accumulator already
            // holds the cumulative count (Phase C-γ hydration); use
            // its sum for the comparison.
            let committed_rows: i64 = summary.manifest_parts.iter().map(|p| p.rows).sum();
            let exported_total = if committed_rows > 0 {
                committed_rows
            } else {
                summary.total_rows
            };
            summary.reconciled = Some(exported_total == count);
            if exported_total != count {
                log::warn!(
                    "reconcile MISMATCH for '{}': committed {} rows, source has {}",
                    plan.export_name,
                    exported_total,
                    count
                );
            } else {
                log::info!(
                    "reconcile MATCH for '{}': {}/{}",
                    plan.export_name,
                    exported_total,
                    count
                );
            }
            // Reconcile RAN — match or mismatch is read off summary.reconciled by
            // the gate; this is NOT could-not-verify.
            None
        }
        Ok(None) => {
            log::warn!(
                "reconcile: COUNT(*) returned NULL for '{}'",
                plan.export_name
            );
            Some("source reconcile COUNT(*) returned NULL".to_string())
        }
        Err(e) => {
            log::warn!(
                "reconcile: count query failed for '{}': {:#}",
                plan.export_name,
                e
            );
            Some(format!("source reconcile count query failed: {e:#}"))
        }
    }
}

/// Synthesize a stand-in `RunSummary` for failures that occur **before** a
/// real summary can be created (plan-build errors, plan-validation rejection).
/// Aggregation needs every export accounted for, even those that never reached
/// `RunSummary::new`.
pub(crate) fn synthetic_failed_summary(export_name: &str, err: &anyhow::Error) -> RunSummary {
    let run_id = format!(
        "{}_{}",
        export_name,
        chrono::Utc::now().format("%Y%m%dT%H%M%S%3f"),
    );
    let journal = crate::journal::RunJournal::new(&run_id, export_name);
    RunSummary {
        bytes_read: 0,
        ledger: Default::default(),
        cursor_column: None,
        cursor_low: None,
        cursor_high: None,
        offending_value: None,
        server_context_json: None,
        key_native_type: None,
        state_backed: false,
        run_id,
        export_name: export_name.to_string(),
        status: "failed".into(),
        total_rows: 0,
        files_produced: 0,
        bytes_written: 0,
        files_committed: 0,
        duration_ms: 0,
        peak_rss_mb: 0,
        retries: 0,
        reconnects: 0,
        resumed: false,
        chunks_precomputed: false,
        validated: None,
        schema_changed: None,
        quality_passed: None,
        error_message: Some(crate::redact::redact_error(err)),
        tuning_profile: "balanced (default)".into(),
        batch_size: 0,
        batch_size_memory_mb: None,
        format: String::new(),
        mode: String::new(),
        compression: String::new(),
        // Pre-plan failure: we don't know (and never wrote to) a destination.
        destination_uri: None,
        source_count: None,
        pg_temp_bytes_delta: None,
        skip_reason: None,
        reconciled: None,
        manifest_parts: Vec::new(),
        schema_fingerprint: None,
        manifest_verification: None,
        apply_context: None,
        column_checksums: Vec::new(),
        column_checksums_incomplete: false,
        column_checksums_short_cover: false,
        checksum_key_column: None,
        journal,
    }
}

/// Clear the keyset in-progress resume anchor after a SUCCESSFUL finalize, so the
/// next run isn't misread as a resume of this finished one.
///
/// This is the TWO-ADAPTER seam for the post-finalize clear: BOTH job wrappers
/// (`run_export_job` and `run_export_job_with_chunk_source`) must call it. An
/// INCREMENTAL keyset run no longer clears the anchor in `run_keyset` (that clear
/// must be post-finalize, or a crash between data-complete and the manifest write
/// orphans the pages), so the responsibility moved up to the wrappers — and a
/// wrapper that inlined its own copy and forgot it was the round-3 wrapper-bypass
/// regression. A shared seam makes the wiring structural, not a per-wrapper
/// checklist. No-op for a non-keyset strategy, and for a FAILED run (the anchor
/// must survive a failure so the next run rehydrates rather than orphans).
/// Did this run end in a state where its keyset resume anchor must be KEPT?
///
/// The anchor (`resume_run_id` + the per-range recovery rows) is what lets the
/// next run adopt pages this one already made durable. Clearing it on a run that
/// did not actually complete strands them: the parts are on the prefix, and the
/// only mechanism that would pick them up is gone.
///
/// `failed` alone is not that question. It is `result.is_err()`, computed before
/// the manifest is written — so a run whose PARTS landed but whose MANIFEST did
/// not has `failed == false` while being, by then, a failed run: the status is
/// corrected to "failed", the cursor is held back, and the process exits
/// non-zero. Clearing the anchor there discarded the resume path for pages that
/// no manifest names, which is the strand-the-durable-rows shape this repo has
/// already paid for once.
///
/// Deliberately NOT folded into `failed` itself: `resolve_final_result` returns
/// `run_result` when `failed`, so widening that variable would make a
/// manifest-gap run exit 0 with an Ok result. Two questions, two names.
pub(super) struct RunOutcome<'a> {
    /// `result.is_err()` — the export/quality verdict, computed BEFORE the
    /// manifest is written.
    pub failed: bool,
    /// `Some(why)` when `finalize_manifest` could not write the manifest.
    pub manifest_gap: &'a Option<String>,
}

pub(super) fn keyset_anchor_survives(o: RunOutcome<'_>) -> bool {
    o.failed || o.manifest_gap.is_some()
}

fn finalize_keyset_anchor(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    export_name: &str,
    failed: bool,
) {
    if !failed && matches!(plan.strategy, ExtractionStrategy::Keyset(_)) {
        let _ = state.clear_resume_run_id(export_name);
        // Parallel keyset persists its per-range recovery rows under the same
        // anchor; clear them too (a no-op for sequential keyset, which writes none).
        let _ = state.clear_keyset_ranges(export_name);
    }
}

/// Mark a run `running` at its START — in the central run-status ledger AND (for
/// a cloud destination) as a `running` marker manifest projected into the bucket.
/// The ledger is authoritative for a co-located / shared-Postgres load; the bucket
/// marker lets a cross-boundary reader (Airflow, a foreign-host `rivet load`) see
/// the live run too. The `prefix` recorded in the ledger is the run's write URI,
/// which `gc_orphans` matches at-or-under its load prefix. Best-effort: a miss
/// only makes gc over-defer cleanup, so it warns rather than failing the export.
fn ledger_begin_run(state: &StateStore, plan: &ResolvedRunPlan, export_family: &str, run_id: &str) {
    let prefix = super::finalize::destination_uri_for_manifest(&plan.destination);
    let started_at = chrono::Utc::now().to_rfc3339();
    if let Err(e) = state.begin_run(run_id, &plan.export_name, &prefix, &started_at) {
        log::warn!(
            "export '{}': run-status begin failed (gc may over-defer orphan cleanup): {e:#}",
            plan.export_name
        );
    }
    super::finalize::write_running_manifest(plan, export_family, run_id, &started_at);
}

/// Transition a run to its terminal status in the run-status ledger at finalize.
/// Best-effort — mirrors the manifest status written alongside.
/// Close the run-status row(s) this run owns — BOTH of them when a resume
/// adopted a different id than the one the ledger was opened under.
///
/// A chunk-checkpoint resume replaces `summary.run_id` with the crashed run's id
/// (`chunked::ensure_chunk_checkpoint_plan`), so the row opened at the start is
/// no longer named by the summary. Closing only the summary's id leaves the
/// opening row `running` forever, and `has_active_run_on_prefix` then reports a
/// live writer on that prefix for good — `gc_orphans` defers cleanup
/// indefinitely, with no later run to supersede it.
///
/// One function because the two job wrappers had drifted: `run_export_job`
/// closed both, `run_export_job_with_chunk_source` — which dispatches to the very
/// checkpoint runners that DO the adopting — closed one.
fn ledger_finish_owned_runs(
    state: &StateStore,
    export_name: &str,
    opened_as: &str,
    summary: &RunSummary,
) {
    ledger_finish_run(state, export_name, &summary.run_id, &summary.status);
    if opened_as != summary.run_id {
        ledger_finish_run(state, export_name, opened_as, &summary.status);
    }
}

fn ledger_finish_run(state: &StateStore, export_name: &str, run_id: &str, status: &str) {
    let finished_at = chrono::Utc::now().to_rfc3339();
    if let Err(e) = state.finish_run(run_id, status, &finished_at) {
        log::warn!("export '{export_name}': run-status finish failed: {e:#}");
    }
}

/// ADR-0028 follow-up (arch roast 2026-08-21, Strong #1): the policy divergences
/// between the TWO orchestrator entry points — `run_export_job` (config-driven
/// `rivet run`) and `run_export_job_with_chunk_source` (artifact-driven
/// `rivet apply`). Everything else about the post-plan execution script is ONE
/// sequence, and it used to be written twice (~250 lines each): this session
/// alone paid that two-sites tax twice (the finalize seam call, then its
/// records-on-failure fix — each wired at both bodies). The policy is data;
/// the script lives once in [`execute_resolved_plan`].
struct TailPolicy<'a> {
    /// "export" | "apply" — log prefixes, manifest kind, report kind.
    kind: &'static str,
    /// Ledger/manifest family: `export.family()` on the run path (the CDC
    /// snapshot leg differs from the export name), the export name on apply
    /// (a sealed artifact has no ExportConfig — correctly, see the manifest
    /// call's comment in git history).
    family: &'a str,
    /// Where the run report lands.
    config_path: &'a str,
    /// What the runners receive as their config-path argument: the real path
    /// on the run path (chunk-checkpoint resume reads it), "" on apply (which
    /// does not support checkpoint-parallel resume).
    runner_config_path: &'a str,
    chunk_source: chunked::ChunkSource,
    /// Apply-only provenance recorded on the summary.
    apply_context: Option<crate::pipeline::summary::ApplyContext>,
    /// The reconcile leg exists only on the run path; apply folds `Ok(())`.
    allow_reconcile: bool,
    /// Run-path notifications; apply sends none.
    notifications: Option<&'a crate::config::NotificationsConfig>,
    /// Plan (rule, message) warnings the run path journals as `PlanWarning`
    /// events; apply logs them at validate time and journals none (existing
    /// behavior, preserved).
    plan_warnings: Vec<(String, String)>,
}

/// Does this run's reconcile leg run? Pure, because the three-input condition
/// is the ONE piece of NEW logic the two-entry-point unification introduced
/// (`allow_reconcile` is the policy bit; the other two already gated the leg on
/// both sides), and a live-only script body cannot be graded by the in-diff
/// mutation gate — its `&&`s and its `!` all survived on the first run.
///
/// All three must hold: the entry point ALLOWS a reconcile at all (apply does
/// not — a sealed artifact has no reconcile flag), the plan ASKED for one, and
/// the export did not already fail (reconciling a failed export compares a
/// partial write against the source and reports a mismatch that is not one).
fn should_reconcile(allow_reconcile: bool, plan_reconcile: bool, failed: bool) -> bool {
    allow_reconcile && plan_reconcile && !failed
}

/// The plan-validation verdict, pure: `Some(err)` when the plan carries
/// REJECTED diagnostics, `None` when it may run. Extracted because the
/// `!rejected.is_empty()` gate sits in the live-only `run_export_job` body and
/// its `!` survived the in-diff mutation gate — dropping it inverts the verdict,
/// so a clean plan would be refused and a rejected one would RUN.
fn plan_rejection_error(export_name: &str, rejected: &[String]) -> Option<anyhow::Error> {
    if rejected.is_empty() {
        return None;
    }
    Some(anyhow::anyhow!(
        "export '{}': plan validation failed:\n  {}",
        export_name,
        rejected.join("\n  ")
    ))
}

/// The two `--resume` / `--force` gates, pure. Both live inside `run_export_job`
/// — a live-only body the in-diff mutation gate cannot reach, which is why its
/// `&&` and both its `!`s survived (2026-08-21 run: 6 of the 16 misses were
/// these three operators). The conditions are opposite halves of one policy, so
/// they are stated together and unit-tested as one truth table.
///
/// `resume` asks to continue into a prefix; `force` is the audited override.
/// A `--resume` into a COMPLETE prefix is refused unless forced (re-exporting a
/// verified dataset is almost never meant); a FRESH run into a complete prefix
/// is only warned about, because refusing or auto-deleting would destroy
/// operator data.
fn resume_success_gate_applies(resume: bool, force: bool) -> bool {
    resume && !force
}

/// Sibling of [`resume_success_gate_applies`] — the fresh-run rerun-accumulation
/// warning (audit findings #5/#19/#30).
fn rerun_warning_applies(resume: bool, force: bool) -> bool {
    !resume && !force
}

/// Does this export bypass the batch plan/strategy machinery for the dedicated
/// CDC runner? Pure so the dispatch condition is graded offline — the `==`
/// survived as a live-only mutant (flipping it to `!=` sends every BATCH export
/// through the CDC runner and every CDC export through the batch planner).
fn dispatches_to_cdc_runner(mode: crate::config::ExportMode) -> bool {
    mode == crate::config::ExportMode::Cdc
}

/// THE post-plan execution script, written once: rss/forensics/harm bracket →
/// dispatch → ADR-0028 seam → bracket close → quality gate → status → diagnosis
/// → [reconcile] → journal → ledger close → manifest → cursor → keyset anchor →
/// validate → metrics → report → [notify] → final fold. Ordering comments are
/// load-bearing and live HERE only. LIVE-ONLY BY CONSTRUCTION (real source +
/// destination + StateStore) — the pure decisions are extracted and unit-tested
/// next door ([`pg_temp_bytes_delta`], [`pg_temp_bytes_warning`],
/// `run_diagnosis`, `resolve_final_result`, `keyset_anchor_survives`); the live
/// oracles are the plan/apply round-trips and the whole live suite.
fn execute_resolved_plan(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    tail: TailPolicy<'_>,
) -> (Result<()>, RunSummary) {
    let start = std::time::Instant::now();
    let rss_before = crate::resource::get_rss_mb();
    let rss_sampler = crate::resource::RssPeakSampler::start(rss_before, 100);
    let mut summary = RunSummary::new(plan);
    summary.apply_context = tail.apply_context;
    // Record this run `running` BEFORE any part lands — in the ledger + a bucket
    // marker manifest — the authority `gc_orphans` reads to spare a live extract's
    // committed-but-not-yet-manifested parts. (finish_run transitions it below.)
    ledger_begin_run(state, plan, tail.family, &summary.run_id);
    // The id the LEDGER row was opened under. A chunk-checkpoint RESUME adopts the
    // prior run's id (chunked/mod.rs) — `summary.run_id` changes AFTER this point —
    // and `finish_run` is a bare UPDATE, so closing the run under the adopted id
    // matched no row and left this one `running` forever.
    //
    // That leak is not cosmetic: `has_active_run_on_prefix` keeps answering true,
    // so `gc_orphans` defers cleanup, and since the load now excludes active runs
    // from the consumed set (dispatch.rs), every later load on that prefix
    // re-appends instead of recording progress — until an unrelated newer run of
    // the same export happens to supersede it.
    let ledger_run_id = summary.run_id.clone();
    // Failure forensics at open: source schema + server limits, so a run that fails
    // before finalize still explains itself (export_schema is otherwise success-only).
    capture_open_forensics(plan, state, &mut summary);

    // PG cursor / sort spill probe — captured around the actual run window.
    // Cluster-level counter, so this is a noisy upper bound on a shared host
    // but accurate on the single-tenant test DBs pilots typically use.
    let pg_temp_bytes_before = pg_temp_bytes_snapshot(plan);
    // Tier 2: broader source-harm counters (locks, rows read, buffer misses,
    // temp files) bracketed around the same run window; the per-counter delta is
    // stored in export_harm. Best-effort — see `harm_snapshot`.
    let harm_before = harm_snapshot(&plan.source);

    // Record plan diagnostics the caller already logged at validate time.
    for (rule, message) in &tail.plan_warnings {
        summary.journal.record(RunEvent::PlanWarning {
            rule: rule.clone(),
            message: message.clone(),
        });
    }

    let result = if plan.strategy.requires_parallel_execution() {
        if plan.strategy.is_resumable() {
            run_chunked_parallel_checkpoint(
                tail.runner_config_path,
                state,
                plan,
                &mut summary,
                tail.chunk_source,
            )
        } else {
            chunked::run_chunked_parallel(state, plan, &mut summary, tail.chunk_source)
        }
    } else {
        run_with_reconnect(
            state,
            plan,
            &mut summary,
            tail.runner_config_path,
            tail.chunk_source,
        )
    };
    // ADR-0028: THE export tail — apply the ledger the runner fed exactly once,
    // here, before anything downstream reads the summary. On runner success the
    // full seam runs (records + gates; a drift `fail` folds into `result` and
    // flows the same failed-status path a runner error does). On runner FAILURE
    // the records half still applies — the Failed manifest must describe the
    // durable debris with the OBSERVED fingerprint + Form-B, never the stale
    // baseline (seam bughunt 2026-08-21).
    let result = match result {
        Ok(()) => super::finalize::finalize_export(plan, Some(state), &mut summary),
        Err(e) => {
            super::finalize::finalize_export_records(&mut summary);
            Err(e)
        }
    };

    let rss_peak = rss_sampler.stop();
    let rss_after = crate::resource::get_rss_mb();
    // Harvest the run's bytes-read counter ONCE — every sink (per chunk, per
    // worker) incremented plan.bytes_read, so this single read covers every
    // runner by construction (#175).
    summary.bytes_read = plan.bytes_read.load(std::sync::atomic::Ordering::Relaxed);
    summary.duration_ms = start.elapsed().as_millis() as i64;
    summary.peak_rss_mb = rss_peak.max(rss_after).max(rss_before) as i64;

    // Close the harm bracket on the SAME window the run occupied, before the
    // status resolution below — the deltas are what the DIAGNOSIS reads.
    // Compute the temp_bytes delta only when both snapshots succeeded — partial
    // failures (e.g. dropped connection between runs) leave the field None so
    // the summary card omits the line entirely.
    if let Some(before) = pg_temp_bytes_before
        && let Some(after) = pg_temp_bytes_snapshot(plan)
    {
        let delta = pg_temp_bytes_delta(before, after);
        summary.pg_temp_bytes_delta = Some(delta);
        if let Some(line) = pg_temp_bytes_warning(
            &plan.export_name,
            delta,
            super::run::multi_export_concurrent(),
        ) {
            log::warn!("{line}");
        }
    }

    // Tier 2: record the per-counter source-harm delta. A failed or absent probe
    // (e.g. missing VIEW SERVER STATE on MSSQL) leaves no rows — never fatal.
    let mut harm_delta_vec: Vec<(String, i64)> = Vec::new();
    if let Some(before) = &harm_before
        && let Some(after) = harm_snapshot(&plan.source)
    {
        harm_delta_vec = harm_deltas(before, &after);
        if let Err(e) = state.record_harm(&summary.run_id, &summary.export_name, &harm_delta_vec) {
            log::debug!(
                "{} '{}': harm metrics write failed (informational): {:#}",
                tail.kind,
                summary.export_name,
                e
            );
        }
    }
    let tuning_class = plan.tuning.profile_name().to_string();
    let result = run_chunked_quality_gate(result, plan, &mut summary);
    let failed = result.is_err();
    match &result {
        Ok(()) => {
            if summary.status == "running" {
                summary.status = "success".into();
            }
        }
        Err(e) => {
            summary.status = "failed".into();
            let redacted = crate::redact::redact_error(e);
            summary.error_message = Some(redacted.clone());
            log::error!("{} '{}' failed: {}", tail.kind, plan.export_name, redacted);
        }
    }

    // Self-diagnosing run-health line — makes a log the field team sends back
    // readable at a glance (reconnects survived, a resume-hit, a source spill).
    // Emitted AFTER the success/failed resolution above so the line reports the
    // real terminal status, not the transient "running" it was built with (#18
    // bughunt: it ran before the status was resolved, so it always said running).
    if let Some(line) = run_diagnosis(
        &summary,
        &harm_delta_vec,
        super::run::multi_export_concurrent(),
    ) {
        log::warn!("{line}");
    }

    let mut reconcile_gate: crate::error::Result<()> = Ok(());
    if should_reconcile(tail.allow_reconcile, plan.reconcile, failed) {
        let could_not_verify = reconcile_source_count(plan, &mut summary);
        if let (Some(source_count), Some(matched)) = (summary.source_count, summary.reconciled) {
            summary.journal.record(RunEvent::ReconciliationResult {
                source_count,
                exported_rows: summary.total_rows,
                matched,
            });
        }
        // The reconcile verdict drives the EXIT CODE (folded into `final_result`):
        // exit 3 on a mismatch, exit 1 on a could-not-verify. It does NOT flip
        // `summary.status` to "failed" — that would make `finalize_manifest` write
        // a Failed manifest with no _SUCCESS, so `rivet load` would REFUSE a
        // COMPLETE, durable export because a post-hoc COUNT(*) raced a concurrent
        // write on a live source (#2 bughunt). The export succeeded and stays
        // loadable; the mismatch surfaces via `summary.reconciled` (report +
        // metrics + the ReconciliationResult journal event) and the non-zero exit.
        reconcile_gate = reconcile_run_gate(&summary, could_not_verify.as_deref());
    }

    // Terminal journal entry BEFORE the print, so a later `finalize_manifest`
    // gap that flips the status is NOT in the journal on either entry point
    // (one behaviour, not two — the round-5 apply-journal-bypass fix).
    summary.journal.record(RunEvent::RunCompleted {
        status: summary.status.clone(),
        error_message: summary.error_message.clone(),
        duration_ms: summary.duration_ms,
    });

    if let Err(e) = state.store_journal(&summary.journal) {
        log::warn!(
            "{} '{}': journal persist failed (run history not stored): {:#}",
            tail.kind,
            summary.export_name,
            e
        );
    }

    summary.print();
    // Transition the run-status ledger to its terminal status — the manifest
    // written just below is a PROJECTION of this record, so both carry the same
    // status. A crash before here leaves the row `running`; supersession by a
    // later run reconciles it (no age timer).
    ledger_finish_owned_runs(state, &plan.export_name, &ledger_run_id, &summary);
    // Order matters: write the manifest first, then run the manifest-aware
    // `--validate` pass against the destination, then persist the metrics
    // row, then write the run report.  The report sees the verification
    // verdict only because we run it before `finalize_run_report`; the
    // metrics row must also wait for `finalize_validate_manifest`, which can
    // downgrade `summary.validated` — recording earlier left `rivet metrics`
    // permanently saying validated=pass for a run whose report says it
    // failed.  The notification fires last so it carries the most complete
    // summary.
    // A run whose manifest never landed is NOT a success, whatever its rows say.
    // The parts are durable and the counts are right — and no manifest names them,
    // so the loader will not read them. Reporting success there is a claim the
    // artifacts do not support.
    //
    // The status has already been printed and the ledger row already closed by
    // this point, so both are corrected: the metrics row is written below and
    // picks up the new status, and the ledger row is re-closed. The manifest
    // itself cannot be re-written to say `failed` — failing to write it is the
    // problem.
    let manifest_gap = finalize_manifest(plan, tail.family, state, &summary, tail.kind);
    if let Some(why) = &manifest_gap {
        summary.status = "failed".into();
        summary.error_message = Some(why.clone());
        ledger_finish_owned_runs(state, &plan.export_name, &ledger_run_id, &summary);
    }
    // Round-2 audit #12: advance the incremental cursor now that the destination
    // manifest is durable — never before. A failure here is at-least-once safe (the
    // data + manifest are durable; the next run re-exports from the prior cursor),
    // so log loudly rather than fail a run whose write cycle already succeeded.
    //
    // "now that the manifest is durable" was a PREMISE, not a check: the cursor
    // advanced even when the manifest write had just failed, so the next run
    // started past data nothing described. Guarded now.
    if manifest_gap.is_some() {
        log::error!(
            "{} '{}': incremental cursor NOT advanced — the manifest did not land, so the \
             next run must re-export this window rather than skip past it",
            tail.kind,
            summary.export_name,
        );
    } else if let Err(e) = commit_incremental_cursor(state, plan, &summary) {
        log::error!(
            "{} '{}': cursor advance failed AFTER the manifest was written — the next run \
             re-exports from the prior cursor (at-least-once, no loss): {:#}",
            tail.kind,
            summary.export_name,
            e
        );
    }
    // Round-5: a keyset checkpoint run has now finalized its COMPLETE destination
    // manifest — clear the in-progress run_id (persisted for crash rehydration) so a
    // later run isn't treated as a resume of this finished one. Clearing AFTER the
    // manifest write is the same ordering as the cursor advance: a crash before here
    // leaves resume_run_id set, so the next run rehydrates rather than orphans.
    // EVERY entry point must clear it — a wrapper that skips it strands
    // resume_run_id forever (round-3 wrapper-bypass regression), which is exactly
    // why this script now exists once.
    finalize_keyset_anchor(
        state,
        plan,
        &summary.export_name,
        keyset_anchor_survives(RunOutcome {
            failed,
            manifest_gap: &manifest_gap,
        }),
    );
    if plan.validate {
        finalize_validate_manifest(plan, &mut summary, tail.kind);
    }
    // After finalize_validate_manifest: it can downgrade summary.validated, and
    // the metrics row must carry the final verdict.
    if let Err(e) = state.record_metric_full(&build_metric_row(&summary, plan, &tuning_class)) {
        log::warn!(
            "{} '{}': metrics write failed (run outcome not stored): {:#}",
            tail.kind,
            summary.export_name,
            e
        );
    }
    finalize_run_report(tail.config_path, &summary, tail.kind);
    crate::notify::maybe_send(tail.notifications, &summary);

    // An export failure wins, otherwise an unwritten manifest fails the run;
    // the reconcile leg is `Ok(())` where the policy disables it.
    let final_result = resolve_final_result(failed, result, reconcile_gate, manifest_gap);
    (final_result, summary)
}

pub(super) fn run_export_job(
    config_path: &str,
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    opts: &RunOptions<'_>,
) -> (Result<()>, RunSummary) {
    // CDC exports read the transaction log, not a query — they bypass the batch
    // plan/strategy machinery entirely and run through the dedicated CDC runner,
    // which produces the same (Result, RunSummary) contract + metric row.
    if dispatches_to_cdc_runner(export.mode) {
        // `initial: snapshot`: anchor first, then each pending table's full
        // snapshot (a recursive `mode: full` run into `…/snapshot/`, with its
        // own metric + journal), then the drain below. A failed snapshot fails
        // the export — the anchor stays, so the retry resumes gap-free.
        let pending = match super::cdc_job::initial_snapshot_pending(config, export, state) {
            Ok(p) => p,
            Err(e) => {
                let summary = synthetic_failed_summary(&export.name, &e);
                return (Err(e), summary);
            }
        };
        for synth in &pending {
            let (res, summary) =
                run_export_job(config_path, config, synth, state, config_dir, opts);
            if res.is_err() {
                return (res, summary);
            }
            // Snapshot done → record it in the state DB, the cleanup-proof twin of
            // the GCS `snapshot/_SUCCESS` marker: once here, `cleanup_source`
            // wiping the bucket no longer re-snapshots. Best-effort — a state
            // write failure must not fail an otherwise-successful snapshot.
            //
            // Degradation on that rare failure: the durable signal is lost, so if
            // `cleanup_source` later wipes the GCS `snapshot/_SUCCESS` too, the
            // NEXT run finds no evidence and re-snapshots the whole table —
            // wasteful (a fresh full re-read + re-load), NOT data loss: the
            // checkpoint survived, so `snapshot_plan`'s `resume_expected` keeps the
            // anchor and no changes are skipped.
            // The LABEL, never the relation read. `snapshot_plan` asks this store
            // with the configured string, and on SQL Server `synth.table` is the
            // catalog's pair — so writing that made the key unable to match itself
            // and every cycle re-snapshotted the whole table under a green run
            // (round-4, DEMONSTRATED). Falls back to `table` for every engine where
            // the two are the same string anyway.
            if let Some(table) = synth.snapshot_label.as_deref().or(synth.table.as_deref())
                && let Err(e) =
                    state.mark_snapshot_done(&export.name, table, &summary.journal.run_id)
            {
                log::warn!(
                    "cdc: snapshot-completion persist failed for '{}' table '{}': {:#}",
                    export.name,
                    table,
                    e
                );
            }
        }
        return super::cdc_job::run_cdc_export(config_path, config, export, state);
    }
    let plan = match build_plan(
        config,
        export,
        config_dir,
        opts.validate,
        opts.reconcile,
        opts.resume,
        opts.params,
    ) {
        Ok(p) => p,
        Err(e) => {
            let summary = synthetic_failed_summary(&export.name, &e);
            return (Err(e), summary);
        }
    };

    let diags = validate_plan(&plan);
    let mut rejected: Vec<String> = Vec::new();
    for d in &diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                log::error!("[{}] plan validation rejected: {}", d.rule, d.message);
                rejected.push(d.message.clone());
            }
            DiagnosticLevel::Warning => {
                log::warn!("[{}] plan validation warning: {}", d.rule, d.message);
            }
            DiagnosticLevel::Degraded => {
                log::info!("[{}] plan validation degraded: {}", d.rule, d.message);
            }
        }
    }
    if let Some(err) = plan_rejection_error(&plan.export_name, &rejected) {
        let summary = synthetic_failed_summary(&export.name, &err);
        return (Err(err), summary);
    }

    // ADR-0012 M8 / ADR-0013: refuse `--resume` against a destination whose
    // `_SUCCESS` marker is already present unless the operator explicitly
    // overrode the gate with `--force`.  Re-exporting over a verified
    // dataset is almost never what the operator meant; the gate makes the
    // override an audited decision.
    if resume_success_gate_applies(opts.resume, opts.force)
        && let Err(e) = check_success_gate_for_resume(&plan)
    {
        let summary = synthetic_failed_summary(&export.name, &e);
        return (Err(e), summary);
    }

    // rerun-accumulation footgun (audit findings #5/#19/#30): a *fresh* run
    // (no `--resume`) into a prefix that already carries a completed export
    // does NOT overwrite — it appends a new timestamp-/nonce-named part set
    // alongside the old one and rewrites manifest.json to describe only this
    // run, so a glob reader over the prefix double-counts / sees orphaned
    // parts.  Refusing or auto-deleting would destroy operator data, so this
    // is a loud, non-fatal WARN instead (the `--resume` path above keeps its
    // refuse-without-`--force` gate).  `--force` is the explicit opt-out.
    if rerun_warning_applies(opts.resume, opts.force) {
        warn_if_prefix_has_completed_run(&plan);
    }

    log::info!(
        "starting export '{}' (effective tuning: {})",
        plan.export_name,
        plan.tuning
    );

    // The post-plan execution script lives ONCE in `execute_resolved_plan`;
    // this entry point only supplies the run-path policy.
    let plan_warnings: Vec<(String, String)> = diags
        .iter()
        .filter(|d| {
            matches!(
                d.level,
                DiagnosticLevel::Warning | DiagnosticLevel::Degraded
            )
        })
        .map(|d| (d.rule.to_string(), d.message.clone()))
        .collect();
    let family = export.family();
    execute_resolved_plan(
        &plan,
        state,
        TailPolicy {
            kind: "export",
            family: &family,
            config_path,
            runner_config_path: config_path,
            chunk_source: chunked::ChunkSource::Detect,
            apply_context: None,
            allow_reconcile: true,
            notifications: config.notifications.as_ref(),
            plan_warnings,
        },
    )
}

// `finalize_*` and the M8 success-gate live in `pipeline::finalize` so this
// file stays focused on orchestration (build plan → dispatch → record
// metric → call finalize hooks).  Imports below give us local names.
use super::finalize::{
    check_success_gate_for_resume, finalize_manifest, finalize_run_report,
    finalize_validate_manifest, warn_if_prefix_has_completed_run,
};

/// Execute a pre-resolved plan with a caller-supplied `ChunkSource`.
///
/// Used by `rivet apply`: the plan comes from a deserialized `PlanArtifact` so
/// `build_plan` is skipped. Everything after validation routes through the ONE
/// post-plan script (`execute_resolved_plan`) — open forensics, the source-harm
/// bracket, the finalize seam, metrics, state persistence — so this entry point
/// can no longer diverge from `run_export_job` by omission; its policy is the
/// `TailPolicy` it passes.
///
/// LIVE-ONLY BY CONSTRUCTION, and deliberately not unit-tested: it needs a real
/// source, a real destination and a `StateStore`, so a body stub
/// (`-> Ok(())` — a survivor of the 2026-08-16 mutation run, which ran the
/// OFFLINE suite only) is unkillable here. Its oracles are live and they do
/// bite: `plan_and_apply_full_export_round_trip` and
/// `plan_and_apply_chunked_export_round_trip_uses_precomputed_ranges`
/// (`tests/live/live_plan_apply.rs`) read the destination back through DuckDB
/// and assert the exact row count, which a no-op apply cannot produce, and
/// `pg_apply_persists_metric_row` (`tests/live/live_metrics_persist.rs`) asserts
/// the metric row this function writes. The pure DECISIONS it makes are
/// extracted and unit-tested next door — [`pg_temp_bytes_delta`],
/// [`pg_temp_bytes_warning`], `run_diagnosis`, `resolve_final_result`.
///
/// Returns the `RunSummary` alongside the result, exactly as [`run_export_job`]
/// does, because the ORCHESTRATOR owns the run's tail: `run_apply_command` needs
/// this run's rows/duration to route them through `run::self_check_throughput`.
/// Discarding the summary here is what made the plan-artifact path the fifth
/// orchestrator tail with no run-over-run self-check (round-7 bughunt).
pub(crate) fn run_export_job_with_chunk_source(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    chunk_source: chunked::ChunkSource,
    config_path: &str,
    apply_context: Option<crate::pipeline::summary::ApplyContext>,
) -> (Result<()>, RunSummary) {
    // Re-validate the plan from the artifact (fast, no DB queries).
    let diags = validate_plan(plan);
    for d in &diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                // A refusal BEFORE any work: the caller still gets a summary, so
                // the run has one shape whatever it did (the same contract
                // `run_export_job` keeps for its own early bails).
                let err = anyhow::anyhow!(
                    "export '{}': plan validation rejected: {}",
                    plan.export_name,
                    d.message
                );
                let summary = synthetic_failed_summary(&plan.export_name, &err);
                return (Err(err), summary);
            }
            DiagnosticLevel::Warning => {
                log::warn!("[{}] plan validation warning: {}", d.rule, d.message);
            }
            DiagnosticLevel::Degraded => {
                log::info!("[{}] plan validation degraded: {}", d.rule, d.message);
            }
        }
    }

    log::info!(
        "apply: starting export '{}' (tuning: {})",
        plan.export_name,
        plan.tuning
    );

    // Same script, apply-path policy: artifact family = export name, no
    // checkpoint-parallel resume path (runner_config_path ""), no reconcile
    // leg, no notifications, plan warnings logged above but not journaled
    // (existing behavior, preserved).
    execute_resolved_plan(
        plan,
        state,
        TailPolicy {
            kind: "apply",
            family: &plan.export_name,
            config_path,
            runner_config_path: "",
            chunk_source,
            apply_context,
            allow_reconcile: false,
            notifications: None,
            plan_warnings: Vec::new(),
        },
    )
}

#[cfg(test)]
mod tests {

    /// A resume that ADOPTS a prior run's id must still close the row the ledger
    /// was opened under — both wrappers, through one seam.
    ///
    /// The row is opened with the new run's id; a chunk-checkpoint resume then
    /// replaces `summary.run_id` with the crashed run's. Closing only the
    /// summary's id leaves the opening row `running` with nothing to supersede
    /// it, and `has_active_run_on_prefix` reports a live writer on that prefix
    /// forever — so `gc_orphans` defers cleanup indefinitely.
    ///
    /// Asserted on the STATE the run leaves behind, read back from the store,
    /// rather than on a count of calls.
    #[test]
    fn an_adopted_run_id_does_not_strand_the_row_the_ledger_opened() {
        let state = crate::state::StateStore::open_in_memory().expect("in-memory state");
        let export = "orders";
        let opened_as = "run_new";
        let adopted = "run_crashed";

        // Both rows exist and are `running`: the crashed one from its own earlier
        // run, the new one from this run's start. Two rows is the whole point —
        // with one there is nothing to strand.
        let prefix = "file:///out";
        for rid in [adopted, opened_as] {
            state
                .begin_run(rid, export, prefix, "2026-01-01T00:00:00Z")
                .expect("open a run-status row");
        }
        assert_eq!(
            state.active_run_ids_on_prefix(prefix).unwrap().len(),
            2,
            "fixture is inert — two open rows are the whole point"
        );

        let summary = crate::pipeline::summary::RunSummary {
            run_id: adopted.into(),
            export_name: export.into(),
            status: "success".into(),
            ..Default::default()
        };
        ledger_finish_owned_runs(&state, export, opened_as, &summary);

        let still_active = state.active_run_ids_on_prefix(prefix).unwrap();
        assert!(
            still_active.is_empty(),
            "these run(s) are still marked active after the run ended: {still_active:?} — a \
             permanently running row makes gc_orphans defer this prefix forever, with no later \
             run to supersede it"
        );
    }

    /// The keyset resume anchor must outlive a run that did not finish — and
    /// "did not finish" includes a run whose PARTS landed but whose MANIFEST did
    /// not, which `failed` (computed from `result.is_err()` before the manifest
    /// is written) reports as a success.
    ///
    /// Clearing the anchor there wipes `resume_run_id` and the per-range recovery
    /// rows, so the next run cannot adopt pages already on the prefix that no
    /// manifest names — they are unreachable from both ends. Found by an
    /// adversarial pass over this branch; the manifest-gap handling that made the
    /// status "failed" did not widen the flag this decision reads.
    #[test]
    fn a_run_whose_manifest_did_not_land_keeps_its_keyset_resume_anchor() {
        let gap = Some("the manifest write FAILED".to_string());

        assert!(
            keyset_anchor_survives(RunOutcome {
                failed: false,
                manifest_gap: &gap
            }),
            "parts are durable and nothing names them — the anchor is the only way back to them"
        );
        assert!(
            keyset_anchor_survives(RunOutcome {
                failed: true,
                manifest_gap: &None
            }),
            "an export failure keeps its anchor, as it always did"
        );
        assert!(
            keyset_anchor_survives(RunOutcome {
                failed: true,
                manifest_gap: &gap
            }),
            "both at once is still a run that did not finish"
        );
        assert!(
            !keyset_anchor_survives(RunOutcome {
                failed: false,
                manifest_gap: &None
            }),
            "a genuinely complete run MUST clear it, or the next run is misread as a resume of \
             this finished one and reuses its frozen run_id"
        );
    }

    use super::*;

    #[test]
    fn run_reconcile_gate_fails_on_mismatch_matches_subcommand() {
        // `run --reconcile` must exit non-zero (data-integrity, exit 3) on a
        // row-count mismatch — the same gate the `rivet reconcile` subcommand
        // enforces via `enforce_reconcile_exit`. Before this, the flag path
        // logged "reconcile MISMATCH" but returned Ok, so
        // `rivet run --reconcile && <next>` silently proceeded past a data-loss
        // mismatch (dogfood issue #102).
        let mut s = RunSummary {
            export_name: "orders".into(),
            source_count: Some(1033),
            reconciled: Some(false),
            ..Default::default()
        };
        let err = reconcile_run_gate(&s, None).unwrap_err();
        assert!(
            err.downcast_ref::<crate::error::DataIntegrityError>()
                .is_some(),
            "a reconcile mismatch must carry the data-integrity marker"
        );
        assert_eq!(
            crate::error::classify_exit(&err),
            3,
            "a reconcile mismatch must classify as exit 3"
        );

        // A match, and a skipped reconcile (reconciled == None, no could-not-verify
        // reason), must NOT gate.
        s.reconciled = Some(true);
        assert!(reconcile_run_gate(&s, None).is_ok());
        s.reconciled = None;
        assert!(reconcile_run_gate(&s, None).is_ok());

        // #10: a COULD-NOT-VERIFY reconcile (reconciled == None BUT a reason is
        // present) is OPERATIONAL — exit 1, NOT verified-OK (exit 0) and NOT the
        // data-integrity class (exit 3).
        let err = reconcile_run_gate(&s, Some("could not connect to the source"))
            .expect_err("a could-not-verify reconcile must gate non-zero");
        assert!(
            err.downcast_ref::<crate::error::DataIntegrityError>()
                .is_none(),
            "could-not-verify must NOT be data-integrity (exit 3)"
        );
        assert_eq!(
            crate::error::classify_exit(&err),
            1,
            "a could-not-verify reconcile must classify as operational exit 1"
        );
        // A genuine MISMATCH wins over a could-not-verify reason (data-integrity).
        s.reconciled = Some(false);
        let err = reconcile_run_gate(&s, Some("noise")).unwrap_err();
        assert_eq!(crate::error::classify_exit(&err), 3);
    }

    /// The reconcile gate's full truth table — three booleans, eight cases, so
    /// no `&&`→`||` and no dropped `!` survives (the in-diff mutation gate found
    /// all three of those alive in the live-only script body; extracting the
    /// decision is what makes them killable offline).
    /// The plan-validation verdict: a clean plan RUNS, a rejected one is
    /// refused with every reason named. Kills the `delete !` mutant, which
    /// inverts both halves — refusing valid plans and running rejected ones.
    #[test]
    fn plan_rejection_error_refuses_only_a_rejected_plan_and_names_every_reason() {
        use super::plan_rejection_error;
        assert!(
            plan_rejection_error("orders", &[]).is_none(),
            "a plan with no REJECTED diagnostics must run"
        );
        let err = plan_rejection_error(
            "orders",
            &[
                "chunk key is nullable".to_string(),
                "no primary key".to_string(),
            ],
        )
        .expect("a rejected plan must not run");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("export 'orders'"),
            "names the export; got {msg}"
        );
        assert!(
            msg.contains("chunk key is nullable") && msg.contains("no primary key"),
            "every rejection reason must reach the operator; got {msg}"
        );
    }

    /// The resume/force policy as ONE truth table: the refuse-gate and the
    /// warn-gate are opposite halves, and `--force` disables both. Kills the
    /// `&&`→`||` and both `delete !` mutants the in-diff gate found alive in
    /// `run_export_job`.
    #[test]
    fn resume_and_rerun_gates_are_opposite_halves_disabled_by_force() {
        use super::{rerun_warning_applies, resume_success_gate_applies};
        // --resume, no --force: refuse a complete prefix; do NOT warn (the
        // resume path owns this case).
        assert!(resume_success_gate_applies(true, false));
        assert!(!rerun_warning_applies(true, false));
        // fresh run, no --force: warn about accumulation; the refuse-gate is
        // not this path's.
        assert!(!resume_success_gate_applies(false, false));
        assert!(rerun_warning_applies(false, false));
        // --force is the audited override: BOTH gates go quiet, resumed or not.
        for resume in [true, false] {
            assert!(
                !resume_success_gate_applies(resume, true),
                "--force must disable the resume refusal (resume={resume})"
            );
            assert!(
                !rerun_warning_applies(resume, true),
                "--force must disable the rerun warning (resume={resume})"
            );
        }
    }

    /// The CDC dispatch fork: exactly one mode takes the CDC runner, every other
    /// mode takes the batch planner. Kills the `==`→`!=` mutant (which would
    /// invert the whole fork).
    #[test]
    fn only_cdc_mode_dispatches_to_the_cdc_runner() {
        use super::dispatches_to_cdc_runner;
        use crate::config::ExportMode;
        assert!(dispatches_to_cdc_runner(ExportMode::Cdc));
        for m in [ExportMode::Full, ExportMode::Incremental] {
            assert!(
                !dispatches_to_cdc_runner(m),
                "{m:?} is a BATCH mode and must not reach the CDC runner"
            );
        }
    }

    #[test]
    fn should_reconcile_requires_permission_request_and_success() {
        use super::should_reconcile;
        // The ONLY true case: allowed, asked for, and the export succeeded.
        assert!(should_reconcile(true, true, false));

        // Each input alone is decisive.
        assert!(
            !should_reconcile(false, true, false),
            "apply does not allow a reconcile leg — a sealed artifact has no flag"
        );
        assert!(
            !should_reconcile(true, false, false),
            "the plan did not ask for one"
        );
        assert!(
            !should_reconcile(true, true, true),
            "a FAILED export must not reconcile: comparing a partial write against \
             the source reports a mismatch that is not one"
        );

        // …and no combination of the remaining four is true.
        for (a, r, f) in [
            (false, false, false),
            (false, false, true),
            (false, true, true),
            (true, false, true),
        ] {
            assert!(
                !should_reconcile(a, r, f),
                "({a},{r},{f}) must not reconcile"
            );
        }
    }

    #[test]
    fn resolve_final_result_surfaces_reconcile_mismatch_when_export_succeeded() {
        use crate::error::DataIntegrityError;
        // The WIRING guard for #102: when the export itself succeeded
        // (failed=false), a reconcile-gate error MUST become the run's result
        // (exit 3). This is the half the gate-logic test cannot cover — that
        // `run --reconcile` actually RETURNS the gate rather than Ok. Un-hooking
        // the fold reopens the bug; this test then goes red.
        let gate: crate::error::Result<()> = Err(DataIntegrityError::new("mismatch").into());
        let out = resolve_final_result(false, Ok(()), gate, None);
        assert!(
            out.is_err(),
            "a reconcile mismatch on a successful export must surface as the run result"
        );
        assert_eq!(crate::error::classify_exit(&out.unwrap_err()), 3);

        // An export/quality failure takes precedence over the reconcile gate.
        let qfail: crate::error::Result<()> = Err(DataIntegrityError::new("quality").into());
        assert!(resolve_final_result(true, qfail, Ok(()), None).is_err());

        // Clean run: no export failure, no reconcile mismatch → Ok. (A --validate
        // verified-wrong verdict is NON-fatal by design — ADR-0001 §I7; a hard gate
        // is the standalone `rivet validate` command, not `run --validate`.)
        assert!(resolve_final_result(false, Ok(()), Ok(()), None).is_ok());
        // A run whose manifest never landed is not a success, even with the
        // export and the reconcile both green: the parts are durable and no
        // manifest names them, so the loader cannot reach them.
        let gap = resolve_final_result(false, Ok(()), Ok(()), Some("no manifest".into()));
        assert!(
            gap.is_err(),
            "an unwritten manifest must fail the run — reporting success there is a claim the \
             artifacts do not support"
        );
    }

    #[test]
    fn run_diagnosis_flags_flaky_link_and_spill_signals_only() {
        let base = || RunSummary {
            export_name: "orders".into(),
            total_rows: 1000,
            peak_rss_mb: 50,
            duration_ms: 2000,
            status: "success".into(),
            ..Default::default()
        };
        // A clean run has nothing to diagnose — its stats are in the run card.
        assert!(run_diagnosis(&base(), &[], false).is_none());
        // Reconnects survived (the flaky-link signal) → flagged, with the count.
        let mut s = base();
        s.reconnects = 2;
        s.retries = 3;
        let line = run_diagnosis(&s, &[], false).expect("reconnects must diagnose");
        assert!(line.contains("2 reconnect"), "got: {line}");
        assert!(line.contains("retries=3"), "got: {line}");
        // #18 bughunt: the line interpolates the run STATUS — the caller must emit
        // it AFTER the success/failed resolution so this reads `[success]`, not the
        // transient `[running]` it was previously built with.
        assert!(
            line.contains("[success]"),
            "status must be the resolved one: {line}"
        );
        // A resume-hit means the prior run crashed → flagged.
        let mut s = base();
        s.resumed = true;
        assert!(
            run_diagnosis(&s, &[], false)
                .unwrap()
                .contains("resumed a prior CRASHED")
        );
        // A source tmp-disk spill (recorded in export_harm but never LOGGED before)
        // → flagged with the escape hatch.
        let line = run_diagnosis(&base(), &[("Created_tmp_disk_tables".into(), 2782)], false)
            .expect("spills must diagnose");
        assert!(line.contains("2782 tmp-disk spills"), "got: {line}");
        // A negligible spill is noise, not a diagnosis on its own.
        assert!(run_diagnosis(&base(), &[("Created_tmp_disk_tables".into(), 5)], false).is_none());
    }

    #[test]
    fn run_diagnosis_spill_remedy_never_suggests_the_strategy_already_running() {
        // Field find (2026-08-13 pool dogfood): a keyset export with source
        // tmp-disk spills was told to "try `mode: chunked`/`chunk_by_key`" —
        // the strategy it was ALREADY running. The remedy must be picked from
        // the summary's mode: paged modes get the page/batch levers, only the
        // unpaged modes are told to switch strategy.
        let with_mode = |mode: &str| RunSummary {
            export_name: "items".into(),
            total_rows: 1000,
            peak_rss_mb: 50,
            duration_ms: 2000,
            status: "success".into(),
            mode: mode.into(),
            ..Default::default()
        };
        let spills = [("Created_tmp_disk_tables".to_string(), 240_i64)];
        for paged in ["keyset", "chunked"] {
            let line =
                run_diagnosis(&with_mode(paged), &spills, false).expect("spills must diagnose");
            assert!(
                !line.contains("mode: chunked") && !line.contains("chunk_by_key"),
                "{paged} export must not be told to switch to a paging it already runs: {line}"
            );
            assert!(
                line.contains("chunk_size") && line.contains("batch_size"),
                "{paged} remedy must name the page/batch levers: {line}"
            );
        }
        for unpaged in ["full", "incremental", "timewindow"] {
            let line =
                run_diagnosis(&with_mode(unpaged), &spills, false).expect("spills must diagnose");
            assert!(
                line.contains("chunk_by_key"),
                "{unpaged} remedy should offer the paging escape: {line}"
            );
        }
    }

    /// Field find (2026-08-13, --pool 5): the spill counter is `SHOW GLOBAL
    /// STATUS` — server-wide — and a pool window overlaps every concurrent
    /// sibling's work. Solo runs keep the confident attribution; concurrent
    /// runs must say the counter is server-global instead of blaming the one
    /// export the line prints beside.
    #[test]
    fn run_diagnosis_spill_attribution_hedges_under_concurrent_siblings() {
        let s = || RunSummary {
            export_name: "items".into(),
            total_rows: 1000,
            duration_ms: 2000,
            status: "success".into(),
            mode: "keyset".into(),
            ..Default::default()
        };
        let spills = [("Created_tmp_disk_tables".to_string(), 240_i64)];
        let solo = run_diagnosis(&s(), &spills, false).expect("spills must diagnose");
        assert!(
            solo.contains("the source spilled to disk"),
            "solo attribution stays direct: {solo}"
        );
        let pooled = run_diagnosis(&s(), &spills, true).expect("spills must diagnose");
        assert!(
            pooled.contains("server-global") && pooled.contains("run-level harm"),
            "concurrent attribution must name the counter scope and point at the \
             run-level total: {pooled}"
        );
        assert!(
            !pooled.contains("— the source spilled to disk;"),
            "concurrent line must not carry the solo blame phrasing: {pooled}"
        );
    }

    /// The spill fold must cover EVERY engine that has a spill counter, and
    /// name what it counted. The shipped filter matched `tmp_disk` only —
    /// MySQL's counter — so PostgreSQL's `pg_temp_files` (the direct spill
    /// analogue in `postgres::harm_counters`) summed to zero and the spill
    /// signal was silently absent on PG, on BOTH the per-export and the
    /// run-level surface. Engines with no spill counter (MSSQL lock waits,
    /// Mongo scan counters) must stay at zero — that silence is correct.
    /// Fixtures are ≥2 counters per engine because this is a FOLD: one element
    /// makes any sum look right.
    /// RED against restoring the `tmp_disk`-only filter (the PG case reads 0)
    /// and against a fold that hard-codes MySQL's noun for a temp_files count.
    #[test]
    fn spill_total_counts_every_engines_spill_counter_and_names_the_unit() {
        // MySQL — raw `SHOW GLOBAL STATUS` key and the `mysql_`-prefixed form.
        let mysql = [
            ("Created_tmp_disk_tables".to_string(), 40_i64),
            ("mysql_created_tmp_disk_tables".to_string(), 60_i64),
        ];
        let s = spill_total(&mysql);
        assert_eq!(s.total, 100, "both tmp-disk keys must fold in");
        assert_eq!(s.unit, "tmp-disk spills");

        // PostgreSQL — the whole harm set, only `pg_temp_files` is a spill.
        let pg = [
            ("pg_blks_read".to_string(), 900_000_i64),
            ("pg_blks_hit".to_string(), 5_000_000_i64),
            ("pg_tup_returned".to_string(), 9_000_000_i64),
            ("pg_tup_fetched".to_string(), 800_000_i64),
            ("pg_temp_files".to_string(), 137_i64),
            ("pg_deadlocks".to_string(), 3_i64),
        ];
        let s = spill_total(&pg);
        assert_eq!(s.total, 137, "PG's temp_files IS the spill counter");
        assert_eq!(
            s.unit, "temp-file spills",
            "PG counts FILES — printing MySQL's tmp-disk-table noun for it would \
             misdescribe what was measured"
        );

        // MSSQL and Mongo have no spill counter: silence, not a gap.
        let mssql = [
            ("mssql_lock_waits".to_string(), 4_000_i64),
            ("mssql_lock_wait_ms".to_string(), 900_000_i64),
        ];
        assert_eq!(spill_total(&mssql).total, 0);
        let mongo = [
            ("mongo_docs_scanned".to_string(), 10_000_000_i64),
            ("mongo_wt_cache_bytes_read".to_string(), 1_048_576_i64),
        ];
        assert_eq!(spill_total(&mongo).total, 0);
    }

    /// The per-export DIAGNOSIS is the first surface that must see the PG
    /// spill: with the `tmp_disk`-only filter a PG run could spill thousands of
    /// temp files and the line never appeared. RED against that filter
    /// (`expect` fails: no line at all).
    #[test]
    fn run_diagnosis_flags_a_postgres_temp_file_spill_in_its_own_words() {
        let s = RunSummary {
            export_name: "events".into(),
            total_rows: 1000,
            duration_ms: 2000,
            status: "success".into(),
            mode: "chunked".into(),
            ..Default::default()
        };
        let pg = [
            ("pg_temp_files".to_string(), 150_i64),
            ("pg_tup_returned".to_string(), 9_000_000_i64),
        ];
        let line = run_diagnosis(&s, &pg, false).expect("a PG temp-file spill must diagnose");
        assert!(
            line.contains("150 temp-file spills"),
            "PG spill must be counted and named as temp FILES: {line}"
        );
        assert!(
            !line.contains("tmp-disk"),
            "must not print MySQL's unit for a PG temp_files count: {line}"
        );
        // Below the shared threshold it stays quiet, same as MySQL's counter.
        let quiet = [("pg_temp_files".to_string(), 99_i64)];
        assert!(run_diagnosis(&s, &quiet, false).is_none());
    }

    /// `pg_stat_database.temp_bytes` is DATABASE-wide, so under any
    /// concurrent-sibling mode the whole delta cannot be blamed on the one
    /// export whose name the line carries — the same measurement lie the
    /// tmp-disk flag was fixed for, left on its PG sibling. The warning must
    /// stay LOUD (it is real source pressure) and hedge the attribution.
    /// RED against the unhedged single-format warning (the concurrent line
    /// then still says "during run" and never says the scope is database-wide).
    #[test]
    fn pg_temp_bytes_warning_hedges_attribution_under_concurrent_siblings() {
        let big = 250 * 1024 * 1024;
        let solo = pg_temp_bytes_warning("orders", big, false).expect("250 MB must warn");
        assert!(
            solo.contains("during run") && solo.contains("+250.0 MB"),
            "solo wording keeps the direct attribution: {solo}"
        );
        let concurrent = pg_temp_bytes_warning("orders", big, true).expect("250 MB must warn");
        assert!(
            concurrent.contains("+250.0 MB"),
            "the hedge must stay LOUD about the volume: {concurrent}"
        );
        assert!(
            concurrent.contains("database-wide") && concurrent.contains("attribution is unknown"),
            "concurrent wording must name the counter scope and refuse to blame one \
             export: {concurrent}"
        );
        assert!(
            !concurrent.contains("during run —"),
            "concurrent line must not carry the solo phrasing: {concurrent}"
        );
        // Threshold is exclusive at 100 MB in both modes — a small spill is noise.
        assert!(pg_temp_bytes_warning("orders", 100 * 1024 * 1024, false).is_none());
        assert!(pg_temp_bytes_warning("orders", 100 * 1024 * 1024, true).is_none());
    }

    /// The bracket arithmetic itself — what the run is CREDITED with spilling.
    ///
    /// Both call sites need a live PostgreSQL source, so until
    /// [`pg_temp_bytes_delta`] was split out nothing offline graded it and the
    /// live oracle could not either (it asserts `is_some()`, which a sum
    /// satisfies). The fixture is engineered so no operator agrees with the
    /// difference: over `3 MB → 7 MB` the difference is 4 MB, the sum 10 MB, the
    /// quotient 2 — three distinct values, all positive, so `.max(0)` cannot mask
    /// the disagreement.
    #[test]
    fn pg_temp_bytes_delta_is_the_windows_growth_and_never_a_counter_reset() {
        const MB: i64 = 1024 * 1024;
        assert_eq!(
            pg_temp_bytes_delta(3 * MB, 7 * MB),
            4 * MB,
            "the credited spill is what the window ADDED, not the counter's absolute value"
        );
        // The operator-facing consequence, at the boundary the warning reads: a
        // 30 MB window on a database that has already spilled 120 MB is noise —
        // reporting the absolute counter instead turns it into a false alarm.
        assert!(
            pg_temp_bytes_warning("orders", pg_temp_bytes_delta(120 * MB, 150 * MB), false)
                .is_none(),
            "a 30 MB window must stay below the warn floor"
        );
        assert!(
            pg_temp_bytes_warning("orders", pg_temp_bytes_delta(0, 150 * MB), false).is_some(),
            "activation guard: the same 150 MB counter DOES warn when the window really \
             produced it — otherwise the assertion above passes on an inert threshold"
        );
        // `pg_stat_reset()` / a server restart mid-window: the counter went
        // BACKWARDS. That is a lost measurement, never a reclaim — a negative
        // credit would offset a sibling's real spill wherever these are summed.
        assert_eq!(
            pg_temp_bytes_delta(9 * MB, MB),
            0,
            "a counter reset mid-run must credit nothing, not a negative spill"
        );
    }

    #[test]
    fn synthetic_failed_summary_fields() {
        let err = anyhow::anyhow!("connection refused");
        let summary = synthetic_failed_summary("my_export", &err);
        assert_eq!(summary.export_name, "my_export");
        assert_eq!(summary.status, "failed");
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
        assert_eq!(summary.bytes_written, 0);
        assert!(
            summary
                .error_message
                .as_ref()
                .unwrap()
                .contains("connection refused")
        );
    }

    #[test]
    fn synthetic_failed_summary_run_id_contains_export_name() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(
            summary.run_id.starts_with("orders_"),
            "run_id was: {}",
            summary.run_id
        );
    }

    #[test]
    fn synthetic_failed_summary_journal_is_empty() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(summary.journal.entries.is_empty());
    }

    #[test]
    fn synthetic_failed_summary_no_quality_or_reconcile_state() {
        let err = anyhow::anyhow!("boom");
        let summary = synthetic_failed_summary("orders", &err);
        assert!(summary.quality_passed.is_none());
        assert!(summary.reconciled.is_none());
        assert!(summary.validated.is_none());
    }

    // ── run_chunked_quality_gate ────────────────────────────────────────────
    //
    // The chunked quality gate is the post-aggregation row-count check that
    // fires AFTER every chunk has been written and the totals are known.
    // It is the only quality validation chunked mode supports — null_ratio
    // and unique_columns are explicitly out of scope because each chunk
    // processes independently. Tests pin the gate logic so changes to chunked
    // quality semantics surface as visible diffs, not silent behaviour drift.

    use crate::config::QualityConfig;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, MetaColumns, SourceConfig,
        SourceType,
    };
    use crate::plan::{ChunkedPlan, ExtractionStrategy, ResolvedRunPlan};
    use crate::tuning::SourceTuning;

    fn chunked_plan_with_quality(quality: Option<QualityConfig>) -> ResolvedRunPlan {
        ResolvedRunPlan {
            split_window: None,
            bytes_read: Default::default(),
            export_name: "orders".into(),
            source_table: None,
            base_query: "SELECT id FROM orders".into(),
            is_split_unit: false,
            strategy: ExtractionStrategy::Chunked(ChunkedPlan {
                column: "id".into(),
                chunk_size: 100,
                chunk_count: None,
                parallel: 1,
                dense: false,
                by_days: None,
                checkpoint: false,
                max_attempts: 3,
            }),
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("/tmp".into()),
                ..Default::default()
            },
            quality,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody@127.0.0.1:9999/x".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                environment: None,
                tuning: None,
                tls: None,
                mongo: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    fn fresh_summary(plan: &ResolvedRunPlan, total_rows: i64) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("r", plan.export_name.clone());
        s.total_rows = total_rows;
        s.batch_size = 10_000;
        s.mode = "chunked".into();
        s.compression = "none".into();
        s
    }

    #[test]
    fn chunked_quality_gate_passes_through_existing_error() {
        // If the chunked run already failed, the gate must NOT mask that with
        // a successful Ok — the original error wins.
        let plan = chunked_plan_with_quality(None);
        let mut summary = fresh_summary(&plan, 0);
        let result = run_chunked_quality_gate(
            Err(anyhow::anyhow!("chunk 3 failed to write")),
            &plan,
            &mut summary,
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("chunk 3 failed"),
            "must propagate original error: {err}"
        );
        // quality_passed must remain None — we never got to evaluate it.
        assert!(summary.quality_passed.is_none());
    }

    #[test]
    fn chunked_quality_gate_no_quality_config_marks_no_decision() {
        // Without quality config, gate is a no-op and quality_passed stays None.
        let plan = chunked_plan_with_quality(None);
        let mut summary = fresh_summary(&plan, 5_000);
        run_chunked_quality_gate(Ok(()), &plan, &mut summary).expect("must pass");
        assert!(summary.quality_passed.is_none());
    }

    #[test]
    fn chunked_quality_gate_row_count_within_bounds_passes() {
        let plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(100),
            row_count_max: Some(10_000),
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        let mut summary = fresh_summary(&plan, 5_000);
        run_chunked_quality_gate(Ok(()), &plan, &mut summary).expect("in bounds must pass");
        assert_eq!(summary.quality_passed, Some(true));
    }

    #[test]
    fn chunked_quality_gate_row_count_below_min_fails() {
        let plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(100),
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        let mut summary = fresh_summary(&plan, 42);
        let err =
            run_chunked_quality_gate(Ok(()), &plan, &mut summary).expect_err("below min must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("quality check(s) failed") && msg.contains("multi-part aggregate"),
            "error must name the failed quality gate: {err}"
        );
        assert!(
            msg.contains("  - "),
            "error must surface the specific failing check(s), not just a generic message: {err}"
        );
        // The chunked quality bail carries the DataIntegrityError marker → exit
        // class 3 (STOP). The operator message is unchanged (asserted above).
        assert!(
            err.downcast_ref::<DataIntegrityError>().is_some(),
            "chunked quality-gate failure must be a typed data-integrity error"
        );
        assert_eq!(crate::error::classify_exit(&err), 3);
        assert_eq!(summary.quality_passed, Some(false));
    }

    #[test]
    fn keyset_quality_gate_row_count_below_min_fails() {
        // RED before the guard broadened Chunked → Chunked|Keyset: keyset (and
        // parallel-Mongo, which is the Keyset strategy) early-returned Ok, so the
        // row_count_min tripwire was SILENTLY DISARMED on the large-table runners —
        // a truncated extract exited 0/success. The gate must fire (exit 3) here too.
        let mut plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(100),
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        plan.strategy = ExtractionStrategy::Keyset(crate::plan::KeysetPlan {
            key_column: "id".into(),
            chunk_size: 500,
            checkpoint: false,
            incremental: false,
            parallel: 1,
        });
        let mut summary = fresh_summary(&plan, 42);
        let err = run_chunked_quality_gate(Ok(()), &plan, &mut summary)
            .expect_err("keyset below-min must FAIL, not silently pass");
        assert_eq!(crate::error::classify_exit(&err), 3);
        assert_eq!(summary.quality_passed, Some(false));
    }

    #[test]
    fn chunked_quality_gate_row_count_above_max_fails() {
        let plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: None,
            row_count_max: Some(1_000),
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        let mut summary = fresh_summary(&plan, 50_000);
        let err =
            run_chunked_quality_gate(Ok(()), &plan, &mut summary).expect_err("above max must fail");
        assert!(err.to_string().contains("quality"), "error: {err}");
        assert_eq!(summary.quality_passed, Some(false));
    }

    #[test]
    fn chunked_quality_gate_skips_unsupported_checks_with_warning() {
        // null_ratio_max and unique_columns are explicitly out of scope for
        // chunked mode. They must NOT cause failure; row_count alone decides.
        let plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(10),
            row_count_max: None,
            null_ratio_max: [("name".into(), 0.1)].into_iter().collect(),
            unique_columns: vec!["id".into()],
            unique_max_entries: None,
        }));
        let mut summary = fresh_summary(&plan, 1_000);
        run_chunked_quality_gate(Ok(()), &plan, &mut summary)
            .expect("unsupported checks must not fail in chunked mode");
        assert_eq!(summary.quality_passed, Some(true));
    }

    #[test]
    fn chunked_quality_gate_inactive_on_non_chunked_strategy() {
        // The gate must early-return on Snapshot/Incremental etc. — those
        // strategies validate inline via the streaming sink.
        let mut plan = chunked_plan_with_quality(Some(QualityConfig {
            row_count_min: Some(99_999), // would fail if evaluated
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: Vec::new(),
            unique_max_entries: None,
        }));
        plan.strategy = ExtractionStrategy::Snapshot;
        let mut summary = fresh_summary(&plan, 10);
        // No-op for non-chunked: must not fail even though min would not be met.
        run_chunked_quality_gate(Ok(()), &plan, &mut summary)
            .expect("non-chunked strategy must skip the gate");
        assert!(summary.quality_passed.is_none());
    }

    // ── build_metric_row ────────────────────────────────────────────────────
    //
    // The builder is what actually decides *what* lands in every `export_metrics`
    // row, so a single field wired to the wrong summary/plan member silently
    // persists a wrong metric for the entire pilot. The metrics-store test
    // (`record_metric_full_persists_v9_columns_in_order`) only guards the
    // MetricRow→SQL column mapping; this pins the summary/plan→MetricRow mapping
    // upstream of it. Every look-alike pair (files_committed vs files_produced,
    // source_count vs total_rows, chunk_size vs parallel) gets a *distinct* value
    // so a field swap surfaces as a wrong-value read, not a passing tie.

    #[test]
    fn build_metric_row_maps_every_summary_and_plan_field() {
        let mut summary = RunSummary::stub_for_testing("run-bmr", "orders");
        summary.duration_ms = 1234;
        summary.total_rows = 50_000;
        summary.peak_rss_mb = 142;
        summary.status = "success".into();
        summary.error_message = Some("export 'x': keyset could not read the 'id' value".into());
        summary.cursor_low = Some("1".into());
        summary.cursor_high = Some("18446744073709551615".into()); // u64 past i64::MAX
        summary.offending_value = Some("9223372036854775800".into()); // last-good before overflow
        summary.server_context_json =
            Some(r#"{"engine":"mysql","max_execution_time_ms":"30000"}"#.into());
        summary.key_native_type = Some("bigint unsigned".into()); // folds into key_descriptor
        summary.format = "parquet".into();
        summary.mode = "chunked".into();
        summary.files_produced = 7;
        summary.bytes_written = 4096;
        summary.bytes_read = 65536;
        summary.retries = 2;
        summary.validated = Some(true);
        summary.schema_changed = Some(false);
        // v9 signals — each distinct from its look-alike sibling.
        summary.files_committed = 6; // ≠ files_produced (7)
        summary.reconciled = Some(true);
        summary.source_count = Some(49_999); // ≠ total_rows (50_000)
        summary.quality_passed = Some(true);
        summary.pg_temp_bytes_delta = Some(1_048_576);
        summary.batch_size = 32_000;
        summary.batch_size_memory_mb = Some(256);
        summary.skip_reason = Some("manual".into());
        summary.schema_fingerprint = Some("fp-abc".into());

        // v10: longest_chunk_ms is delegated to the journal. Inject one paired
        // 640ms chunk span (via the journal's own test helper, not a reach into
        // `entries`) so the field is a known Some — proving the builder reads
        // the journal rather than hardcoding None.
        summary.journal.push_test_chunk_span(0, 640);

        // Chunked plan with distinct chunk_size/parallel so a swap can't pass.
        let mut plan = chunked_plan_with_quality(None);
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 100_000,
            chunk_count: None,
            parallel: 4,
            dense: false,
            by_days: None,
            checkpoint: false,
            max_attempts: 3,
        });

        // Destructure WITHOUT `..` so a new `MetricRow` field is a COMPILE error
        // here until it's bound and asserted — "every field" becomes a
        // compiler-enforced invariant, not a hopeful test name. (The earlier
        // `row.field` form silently ignored any field added to the struct.)
        let crate::state::MetricRow {
            export_name,
            run_id,
            duration_ms,
            total_rows,
            peak_rss_mb,
            status,
            error_message,
            tuning_profile,
            format,
            mode,
            files_produced,
            bytes_written,
            bytes_read,
            retries,
            validated,
            schema_changed,
            files_committed,
            reconciled,
            source_count,
            quality_passed,
            pg_temp_bytes_delta,
            batch_size,
            batch_size_memory_mb,
            skip_reason,
            schema_fingerprint,
            chunk_size,
            parallel,
            source_type,
            destination_type,
            rivet_version,
            longest_chunk_ms,
            chunk_key,
            error_class,
            cursor_min,
            cursor_max,
            key_descriptor_json,
            offending_value,
            server_context_json,
        } = build_metric_row(&summary, &plan, "safe");

        // ── core (v1) ──
        assert_eq!(export_name, "orders");
        assert_eq!(run_id, "run-bmr");
        assert_eq!(duration_ms, 1234);
        assert_eq!(total_rows, 50_000);
        assert_eq!(peak_rss_mb, Some(142));
        assert_eq!(status, "success");
        assert_eq!(
            error_message.as_deref(),
            Some("export 'x': keyset could not read the 'id' value")
        );
        assert_eq!(tuning_profile.as_deref(), Some("safe")); // builder arg
        assert_eq!(format.as_deref(), Some("parquet"));
        assert_eq!(mode.as_deref(), Some("chunked"));
        assert_eq!(files_produced, 7);
        assert_eq!(bytes_written, 4096);
        assert_eq!(bytes_read, 65536);
        assert_eq!(retries, 2);
        assert_eq!(validated, Some(true));
        assert_eq!(schema_changed, Some(false));
        // ── v9 ──
        assert_eq!(files_committed, 6);
        assert_eq!(reconciled, Some(true));
        assert_eq!(source_count, Some(49_999));
        assert_eq!(quality_passed, Some(true));
        assert_eq!(pg_temp_bytes_delta, Some(1_048_576));
        assert_eq!(batch_size, 32_000);
        assert_eq!(batch_size_memory_mb, Some(256));
        assert_eq!(skip_reason.as_deref(), Some("manual"));
        assert_eq!(schema_fingerprint.as_deref(), Some("fp-abc"));
        // ── plan-derived ──
        assert_eq!(chunk_size, Some(100_000));
        assert_eq!(parallel, Some(4));
        assert_eq!(source_type.as_deref(), Some("postgres"));
        assert_eq!(destination_type.as_deref(), Some("local"));
        assert_eq!(rivet_version.as_deref(), Some(env!("CARGO_PKG_VERSION")));
        // ── v10: delegated to the journal (pinned to the injected span) ──
        assert_eq!(longest_chunk_ms, Some(640));
        assert_eq!(longest_chunk_ms, summary.journal.longest_chunk_ms());
        // ── v12: chunking diagnostics — the chunk key column. The plan is a range
        // Chunked on "id"; the resolved strategy is the `mode` column ("chunked").
        assert_eq!(chunk_key.as_deref(), Some("id"));
        // ── v18: failure forensics ──
        assert_eq!(
            error_class.as_deref(),
            Some("keyset_unreadable_key"),
            "error_class is DERIVED from error_message, not hardcoded"
        );
        assert_eq!(cursor_min.as_deref(), Some("1"));
        assert_eq!(cursor_max.as_deref(), Some("18446744073709551615"));
        let kd = key_descriptor_json.expect("chunked strategy carries a key descriptor");
        assert!(kd.contains("\"strategy\":\"chunked\""), "{kd}");
        assert!(kd.contains("\"key\":\"id\""), "{kd}");
        // enriched from summary.key_native_type — the "was it unsigned" answer inline
        assert!(kd.contains("\"db_type\":\"bigint unsigned\""), "{kd}");
        assert!(kd.contains("\"unsigned\":true"), "{kd}");
        // producers wired: both pass through from the summary
        assert_eq!(offending_value.as_deref(), Some("9223372036854775800"));
        assert!(
            server_context_json
                .as_deref()
                .is_some_and(|s| s.contains("max_execution_time_ms")),
            "server_context flows from the summary: {server_context_json:?}"
        );
    }

    #[test]
    fn build_metric_row_non_chunked_has_no_chunk_dims() {
        // The chunk-config dimensions only exist for the Chunked strategy; every
        // other strategy must leave chunk_size/parallel NULL (the `_ => (None,
        // None)` arm) rather than persist a stale or zero value.
        let mut plan = chunked_plan_with_quality(None);
        plan.strategy = ExtractionStrategy::Snapshot;
        let summary = RunSummary::stub_for_testing("run-snap", "orders");

        let row = build_metric_row(&summary, &plan, "balanced");

        assert!(row.chunk_size.is_none(), "snapshot has no chunk_size");
        assert!(row.parallel.is_none(), "snapshot has no parallel");
        // The non-chunk dimensions are still populated.
        assert_eq!(row.source_type.as_deref(), Some("postgres"));
        assert_eq!(row.destination_type.as_deref(), Some("local"));
    }

    #[test]
    fn classify_error_message_maps_the_field_run_failure_classes() {
        // The exact three classes the 0.21.2 field post-mortem grouped BY HAND.
        assert_eq!(
            classify_error_message(
                "export 'aa_import_advcake': keyset could not read the 'id' value from the last row of page 0"
            ),
            Some("keyset_unreadable_key")
        );
        assert_eq!(
            classify_error_message(
                "MySqlError { ERROR 3024 (HY000): maximum statement execution time exceeded }"
            ),
            Some("statement_timeout")
        );
        // The parallel-checkpoint wrapper EMBEDS the inner 3024 text — most-specific-
        // first ordering must bucket it as parallel_checkpoint, NOT statement_timeout.
        // (RED if the two arms are reordered.)
        assert_eq!(
            classify_error_message(
                "export 'aa_payouts_version': parallel checkpoint worker errors:\nchunk 3: MySqlError { ERROR 3024 (HY000): maximum statement execution time exceeded }"
            ),
            Some("parallel_checkpoint"),
            "the 3024-embedding wrapper must not be mis-bucketed as a bare statement_timeout"
        );
        // Extended taxonomy: the field's ERROR 1146 (was None before) + the classes
        // that group the common infra failures.
        assert_eq!(
            classify_error_message(
                "MySqlError { ERROR 1146 (42S02): Table 'rivet.ext_x' doesn't exist }"
            ),
            Some("relation_not_found")
        );
        assert_eq!(
            classify_error_message("Lock wait timeout exceeded; try restarting transaction"),
            Some("lock_timeout"),
            "a lock-wait embeds 'timeout' — must not fall through to statement_timeout"
        );
        assert_eq!(
            classify_error_message("Deadlock found when trying to get lock"),
            Some("deadlock")
        );
        assert_eq!(
            classify_error_message("Connection reset by peer"),
            Some("connection")
        );
        assert_eq!(
            classify_error_message("SELECT command denied to user 'rivet'@'%' for table 't'"),
            Some("privilege")
        );
        assert_eq!(
            classify_error_message("No space left on device"),
            Some("disk_full")
        );
        // An unrecognized error stays honest (None), not force-fit into a bucket.
        assert_eq!(
            classify_error_message("some entirely novel failure with no known signature"),
            None
        );
    }

    // ── harm_deltas ─────────────────────────────────────────────────────────
    //
    // The per-counter delta feeding `export_harm`. Three semantics to pin:
    // matched counters subtract, a counter reset floors at 0 (never a negative
    // "harm"), and the result is the *name intersection* of the two snapshots
    // (a counter present in only one snapshot is dropped, not treated as 0).

    #[test]
    fn harm_deltas_subtracts_matched_counters() {
        let before = vec![
            ("pg_tup_returned".to_string(), 100),
            ("pg_blks_read".to_string(), 5),
        ];
        let after = vec![
            ("pg_tup_returned".to_string(), 150),
            ("pg_blks_read".to_string(), 9),
        ];
        let mut got = harm_deltas(&before, &after);
        got.sort();
        assert_eq!(
            got,
            vec![
                ("pg_blks_read".to_string(), 4),
                ("pg_tup_returned".to_string(), 50)
            ]
        );
    }

    #[test]
    fn harm_deltas_floors_counter_reset_at_zero() {
        // A mid-run server restart resets the cumulative counter; after < before
        // must not surface as negative harm.
        let before = vec![("pg_tup_returned".to_string(), 1_000)];
        let after = vec![("pg_tup_returned".to_string(), 40)];
        assert_eq!(
            harm_deltas(&before, &after),
            vec![("pg_tup_returned".to_string(), 0)]
        );
    }

    #[test]
    fn harm_deltas_intersects_counter_names() {
        // Only counters present in BOTH snapshots are emitted: a metric in only
        // `before` (probe stopped exposing it) or only `after` (newly appeared)
        // has no honest delta and is dropped.
        let before = vec![("shared".to_string(), 10), ("only_before".to_string(), 1)];
        let after = vec![("shared".to_string(), 25), ("only_after".to_string(), 7)];
        assert_eq!(
            harm_deltas(&before, &after),
            vec![("shared".to_string(), 15)]
        );
    }
}
