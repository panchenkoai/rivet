//! **`rivet plan`** — build and display a `PlanArtifact` without executing.
//!
//! This module is the coordinator for the plan command. It:
//!
//! 1. Loads config and resolves the `ResolvedRunPlan` (same as `rivet run`).
//! 2. Runs preflight diagnostics via `preflight::get_export_diagnostic`.
//! 3. For `Chunked` exports: opens a source connection and pre-computes chunk
//!    boundaries using `detect_and_generate_chunks` — no data is exported.
//! 4. For `Incremental` exports: reads the current cursor from `StateStore`.
//! 5. Packages everything into a `PlanArtifact` and either writes it to a file
//!    or prints a human-readable summary to stdout.

use std::collections::HashMap;
use std::path::Path;

use crate::config::Config;
use crate::error::Result;
use crate::plan::{
    ComputedPlanData, ExportRecommendation, ExtractionStrategy, PlanArtifact, PlanDiagnostics,
    PlanPrioritizationSnapshot, PrioritizationInputs, build_plan,
    campaign::recommend_campaign,
    inputs::{PrioritizationHints, build_prioritization_inputs},
    recommend::recommend_export,
    validate::{DiagnosticLevel, validate_plan},
};
use crate::state::StateStore;
use crate::{preflight, source};

use super::chunked::{chunk_plan_fingerprint, detect_and_generate_chunks};

/// Output format for `rivet plan`.
pub enum PlanOutputFormat {
    /// Human-readable summary printed to stdout; no file written.
    Pretty,
    /// Pretty-printed JSON written to the given path (or stdout if `None`).
    Json(Option<String>),
}

/// `--annotate-waves` packs a schedule across the WHOLE config, so it is a
/// category error when scoped to a single `--export` (packing one export alone
/// rewrites its wave to 1 — bug hunt 2026-08-08). True ⇔ both are set: the
/// `&&` must not soften to `||` (which would refuse a plain `--export`) — pinned
/// by `refuse_annotate_scoped_to_export_only_when_both_set`.
fn refuse_annotate_scoped_to_export(annotate_waves: bool, has_export: bool) -> bool {
    annotate_waves && has_export
}

/// Entry point for `rivet plan`.
///
/// Iterates over all matching exports, builds a `PlanArtifact` for each, and
/// either prints or saves it according to `format`.
pub fn run_plan_command(
    config_path: &str,
    export_name: Option<&str>,
    params: Option<&HashMap<String, String>>,
    format: PlanOutputFormat,
    annotate_waves: bool,
) -> Result<()> {
    // --annotate-waves packs a SCHEDULE across the whole config: it assigns each
    // export a wave relative to all the others. Scoped to a single --export it
    // packs that one export alone → wave 1, silently inverting its place in the
    // schedule (bug hunt 2026-08-08). The two are a category error; refuse them
    // together rather than write an incoherent wave.
    if refuse_annotate_scoped_to_export(annotate_waves, export_name.is_some()) {
        anyhow::bail!(
            "--annotate-waves schedules the WHOLE config (each export's wave is relative to              the others) and cannot be scoped to --export: packing one export alone would              rewrite its wave to 1. Drop --export to annotate the whole config, or drop              --annotate-waves to plan just this export."
        );
    }
    let config = Config::load_with_params(config_path, params)?;
    let config_dir = Path::new(config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let exports: Vec<_> = if let Some(name) = export_name {
        let e = config
            .exports
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", name))?;
        vec![e]
    } else {
        config.exports.iter().collect()
    };

    let state_path = config_dir.join(".rivet_state.db");
    let state = StateStore::open(state_path.to_str().unwrap_or(".rivet_state.db"))?;

    let mut built: Vec<(PlanArtifact, PrioritizationInputs, ExportRecommendation)> = Vec::new();

    for export in exports {
        built.push(build_plan_artifact(
            &config,
            export,
            config_path,
            config_dir,
            params,
            &state,
        )?);
    }

    let campaign_opt = if built.len() > 1 {
        let pairs: Vec<_> = built
            .iter()
            .map(|(_, i, r)| (i.clone(), r.clone()))
            .collect();
        Some(recommend_campaign(pairs))
    } else {
        None
    };

    // When `--output FILE` is given but the config yields more than one export,
    // a single path cannot hold every artifact: `rivet apply` reads exactly one
    // `PlanArtifact` per file (`PlanArtifact::from_file`), and a JSON array is
    // not a valid artifact. Writing all of them to the same path silently kept
    // only the last (audit #4). Instead, derive a distinct per-export path so no
    // export is dropped and each file remains directly consumable by `apply`.
    let multi_export = built.len() > 1;

    let mut artifacts: Vec<PlanArtifact> = Vec::with_capacity(built.len());
    for (mut artifact, _inputs, standalone_rec) in built {
        let snap = if let Some(ref camp) = campaign_opt {
            let rec = camp
                .ordered_exports
                .iter()
                .find(|e| e.export_name == artifact.export_name)
                .cloned()
                .unwrap_or_else(|| standalone_rec.clone());
            PlanPrioritizationSnapshot {
                export_recommendation: rec,
                campaign: Some(camp.clone()),
            }
        } else {
            PlanPrioritizationSnapshot {
                export_recommendation: standalone_rec,
                campaign: None,
            }
        };
        artifact.prioritization = Some(snap);
        artifacts.push(artifact);
    }

    // Record the advisory wave + parallel-safety on each export in the config
    // (in place), so the operator sees / edits them and `rivet apply` can run
    // exports wave-by-wave, parallelizing only the cheap (low-cost) ones.
    // `parallel_safe = cost_class Low` (< 100K rows): a heavy table already
    // chunk-parallelizes internally, so two of them at once would overload the
    // source — only the small / cheap exports share a concurrent wave batch.
    let recs: Vec<(String, u32, bool)> = artifacts
        .iter()
        .filter_map(|a| {
            a.prioritization.as_ref().map(|p| {
                let rec = &p.export_recommendation;
                // ...and not flagged `isolate_on_source` by the campaign (a
                // shared, contended source group) — the campaign owns the "run
                // alone" call, so a cheap export there still runs on its own.
                let parallel_safe =
                    rec.cost_class == crate::plan::CostClass::Low && !rec.isolate_on_source;
                (a.export_name.clone(), rec.recommended_wave, parallel_safe)
            })
        })
        .collect();
    // The packer needs each export's cost class (it sets the wave width K);
    // carried beside `recs` rather than widening the tuple `fields_to_write`
    // consumes.
    let cost_of: std::collections::HashMap<String, crate::plan::CostClass> = artifacts
        .iter()
        .filter_map(|a| {
            a.prioritization
                .as_ref()
                .map(|p| (a.export_name.clone(), p.export_recommendation.cost_class))
        })
        .collect();
    // Exports the campaign flagged `isolate_on_source` (a contended source
    // group — must run ALONE regardless of cost). The packer sets
    // parallel_safe:true so a wave's members run concurrently; these must NOT,
    // or --annotate-waves would overload their source (bug-hunt round 2).
    let isolate_of: std::collections::HashSet<String> = artifacts
        .iter()
        .filter_map(|a| {
            a.prioritization.as_ref().and_then(|p| {
                p.export_recommendation
                    .isolate_on_source
                    .then(|| a.export_name.clone())
            })
        })
        .collect();
    // `--annotate-waves` packs from MEASURED history (#150): the last
    // successful run's duration + peak RSS per export, read from the state
    // store plan already holds open. Exports with no history keep the cost
    // model's estimate — and each line below SAYS which one it stands on.
    let recs = if annotate_waves {
        repack_from_history(recs, &cost_of, &isolate_of, &state)
    } else {
        recs
    };
    let (fields, preserved) = fields_to_write(&recs, &config, annotate_waves);
    let written = if fields.is_empty() {
        std::collections::HashSet::new()
    } else {
        write_plan_fields_to_config(config_path, &fields)?
    };
    // `warn`, not `info`: a command that MUTATES the operator's config file
    // must say so at a level the default log setup shows — the info-level
    // version of this line is how a hand-tuned 5-per-wave schedule silently
    // became one 76-export wave (#150). Report ACTUAL writes, not intent
    // (bug hunt 2026-08-08).
    if !written.is_empty() || preserved > 0 {
        log::warn!(
            "plan: annotated {} export(s) in {}; left {} untouched (wave/parallel_safe already \
             set — rerun with --annotate-waves to overwrite)",
            written.len(),
            config_path,
            preserved
        );
    }
    // A field we meant to write but no `- name:` line matched is a SILENT
    // no-write — the loaded export name and the YAML text disagree (templated
    // name, odd quoting). Surface it loudly instead of over-counting it above.
    let unmatched: Vec<&String> = fields.keys().filter(|n| !written.contains(*n)).collect();
    if !unmatched.is_empty() {
        log::warn!(
            "plan: {} export(s) had recommendations but NO matching `- name:` line in {} — \
             nothing was written for them: {:?}",
            unmatched.len(),
            config_path,
            unmatched
        );
    }

    // Pool-scheduler advisory (#166): show the makespan a bounded work-stealing
    // pool would achieve from measured durations, so the operator can weigh it
    // against the wave count. Printed only when we have real durations for more
    // than one export (a single export has nothing to schedule).
    // The same parallel_safe the annotations write is what the pool print
    // must model — the old preview hardcoded `true` "because no config is in
    // scope", while the flags were computed twenty lines up (walk find,
    // 2026-08-13): heavies then predicted M-way parallelism apply-time
    // serialization can never reach.
    // …and the RECOMMENDATION is not what apply reads: `fields_to_write`
    // deliberately preserves a hand-set `parallel_safe:`, so the preview must
    // model the value now ON DISK (bughunt 2026-08-14).
    let safe_of = effective_parallel_safe(&recs, &config, &fields, &written);
    if pool_estimate_is_printable(&format) {
        print_pool_estimate(&artifacts, &safe_of, &state);
    }

    emit_artifacts(&artifacts, &format, multi_export, config_path)?;

    Ok(())
}

/// Derive a distinct per-export output path from the `--output` value when more
/// than one export is being planned.
///
/// Inserts the export name into the file stem (`plan.json` → `plan.orders.json`)
/// so every artifact lands on its own path and `rivet apply <file>` consumes a
/// single artifact unchanged. The export name is sanitised to a safe filename
/// fragment (any non-alphanumeric/`-`/`_` byte becomes `_`) so an export named
/// with a slash or dot cannot escape the intended directory or mangle the
/// extension. The original extension is preserved so per-export files keep the
/// same suffix (`.json`) the operator asked for.
fn per_export_output_path(base: &str, export_name: &str) -> String {
    let sanitized: String = export_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let path = Path::new(base);
    let parent = path.parent();
    let stem = path.file_stem().and_then(|s| s.to_str());
    let ext = path.extension().and_then(|s| s.to_str());

    let file_name = match (stem, ext) {
        (Some(stem), Some(ext)) => format!("{stem}.{sanitized}.{ext}"),
        (Some(stem), None) => format!("{stem}.{sanitized}"),
        // No usable stem (e.g. path was empty or just an extension): fall back to
        // appending the sanitized name so the artifact still lands on a distinct
        // path rather than colliding.
        (None, _) => format!("{base}.{sanitized}"),
    };

    match parent {
        Some(dir) if !dir.as_os_str().is_empty() => {
            dir.join(file_name).to_string_lossy().into_owned()
        }
        _ => file_name,
    }
}

fn build_plan_artifact(
    config: &Config,
    export: &crate::config::ExportConfig,
    config_path: &str,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    state: &StateStore,
) -> Result<(PlanArtifact, PrioritizationInputs, ExportRecommendation)> {
    let plan = build_plan(config, export, config_dir, false, false, false, params)?;

    // Collect plan-level compatibility diagnostics and emit Rejected ones as errors.
    let validate_diags = validate_plan(&plan);
    let mut validate_warnings: Vec<String> = Vec::new();
    for d in &validate_diags {
        match d.level {
            DiagnosticLevel::Rejected => {
                anyhow::bail!("[{}] {}", d.rule, d.message);
            }
            DiagnosticLevel::Warning | DiagnosticLevel::Degraded => {
                validate_warnings.push(format!("[{}] {}", d.rule, d.message));
            }
        }
    }

    let (computed, plan_diagnostics, hints) = match preflight::get_export_diagnostic(config, export)
    {
        Ok(mut diag) => {
            // #149: measured beats declared — same overlay `check` applies, so
            // the two surfaces quote the same figure with the same label.
            crate::preflight::overlay_measured_rows(&mut diag, export, state);
            // The plan artifact's warnings stay flat strings (severity tags are a
            // `rivet check` surface); take each warning's message text.
            let mut warnings: Vec<String> =
                diag.warnings.iter().map(|w| w.message.clone()).collect();
            warnings.extend(validate_warnings);
            // F3 (0.7.5 audit): the JSON artifact's `diagnostics`
            // exposed a non-Efficient verdict with `warnings: []`, so a
            // machine consumer could not see *why* the plan was flagged.
            // If preflight emitted no specific warning, fall back to the
            // build_suggestion text (the same line shown in `rivet check`)
            // so the JSON always carries at least one human-readable
            // reason matching the verdict.
            if warnings.is_empty() && !matches!(diag.verdict, preflight::HealthVerdict::Efficient) {
                if let Some(s) = diag.suggestion.clone() {
                    warnings.push(s);
                } else {
                    warnings.push(format!(
                        "verdict {} but preflight collected no specific warnings — review `rivet check` output for context",
                        diag.verdict
                    ));
                }
            }
            // Explain *why* this strategy was chosen (mode + chunk geometry +
            // parallelism) and its risk profile. Built from the same
            // `ExportDiagnostic` + config the numbers above came from, so the
            // narrative can never contradict them.
            let strategy_rationale = crate::plan::explain_strategy(&diag, export);
            let plan_diagnostics = PlanDiagnostics {
                verdict: diag.verdict.to_string(),
                warnings,
                recommended_profile: diag.recommended_profile.to_string(),
                strategy_rationale,
            };
            let row_is_measured = diag
                .row_source
                .as_deref()
                .is_some_and(|s| s.starts_with("measured"));
            let computed = compute_plan_data(&plan, diag.row_estimate, row_is_measured, state)?;
            let hints = PrioritizationHints {
                incremental_uses_index: diag.uses_index,
                cursor_range_observed: diag.cursor_min.is_some() && diag.cursor_max.is_some(),
            };
            (computed, plan_diagnostics, hints)
        }
        Err(e) => {
            log::warn!(
                "plan '{}': preflight diagnostics failed (continuing without them): {:#}",
                export.name,
                e
            );
            let computed = compute_plan_data(&plan, None, false, state)?;
            let mut warnings = vec!["preflight diagnostics unavailable".into()];
            warnings.extend(validate_warnings);
            let plan_diagnostics = PlanDiagnostics {
                verdict: "unknown (preflight failed)".into(),
                warnings,
                recommended_profile: "balanced".into(),
                // No diagnostic to explain from — be honest rather than fabricate
                // a rationale from config alone (no row estimate / index facts).
                strategy_rationale: "Strategy rationale unavailable — preflight diagnostics could \
                     not be collected for this export."
                    .into(),
            };
            (computed, plan_diagnostics, PrioritizationHints::default())
        }
    };

    let fingerprint = match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => chunk_plan_fingerprint(
            &plan.base_query,
            &cp.column,
            cp.chunk_size,
            cp.chunk_count,
            cp.dense,
            cp.by_days,
        ),
        _ => String::new(),
    };

    // Epic I: fold recent-run history into prioritization (bounded contribution).
    let history = match state.get_metrics(Some(&export.name), 20) {
        Ok(metrics) => Some(crate::plan::HistorySnapshot::summarize(&metrics)),
        Err(e) => {
            log::warn!(
                "plan '{}': history lookup failed; proceeding without historical refinement: {:#}",
                export.name,
                e
            );
            None
        }
    };

    let inputs =
        build_prioritization_inputs(export, &plan, &computed, &plan_diagnostics, hints, history);
    let recommendation = recommend_export(&inputs, &plan_diagnostics);

    let mut artifact = PlanArtifact::new(
        plan.export_name.clone(),
        plan.strategy.mode_label().to_string(),
        fingerprint,
        plan,
        computed,
        plan_diagnostics,
    );

    // F13 (0.7.5 audit): record the absolute config path so `rivet
    // apply` can locate the matching `.rivet_state.db` (cursors,
    // manifest history) even when the plan JSON is stored in a
    // different directory.  `canonicalize` resolves symlinks and
    // produces an absolute path; we fall back to the original string
    // if the path no longer resolves (rare, e.g. config deleted
    // between plan and apply).
    artifact.config_path = Some(
        Path::new(config_path)
            .canonicalize()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| config_path.to_string()),
    );

    Ok((artifact, inputs, recommendation))
}

/// Below this span the catalog is not allowed to override it: the F4
/// fresh-ANALYZE case is a SMALL table whose garbage `reltuples` (1130 on a
/// 30-row table) would otherwise win over an exact tiny span. A genuinely
/// non-unique chunk key on a table worth chunking has a span far above this.
const CATALOG_OVERRIDE_MIN_SPAN: i64 = 10_000;

/// The row estimate a chunked plan reports, from the two available signals.
///
/// * A MEASURED whole-table actual (a prior run's row count) always wins —
///   `#149`: on a sparse key the span is the ID range, not the row count
///   (950M..1.29B over 520K rows → 342M, far worse than the measurement).
/// * Otherwise take the LARGER of the key span and the catalog estimate.
///   For an integer key `span ≥ distinct keys` always holds, so a catalog
///   estimate LARGER than the span is direct evidence the chunk key is
///   NON-UNIQUE (rows > distinct keys) and the span is only a lower bound —
///   field find (2026-08-13 pool dogfood): a versioned table keyed by a
///   non-unique ref column held ~2.5 rows per key, and the span under-reported
///   ~831M catalog rows as ~333M, feeding the pool's makespan prediction.
///   When the catalog is the smaller side (the sparse case, or F4's
///   fresh-ANALYZE garbage) the span still wins, exactly as before.
/// * `span_is_exact` short-circuits ALL of it. Under `chunk_dense: true` the
///   ranges are ORDINALS `1..row_count` built from a real `COUNT(*)` taken in
///   this very plan ([`super::chunked::detect`]) — the span IS the row count,
///   not a key range, so `rows > distinct keys` cannot happen and there is
///   nothing for the catalog to correct. Letting `max()` win there publishes a
///   STALE `reltuples` over a fresh exact count: a table that lost 90% of its
///   rows without `ANALYZE` would be scheduled at its old size (bughunt
///   2026-08-13). Same reason the measured-actual branch is skipped — a prior
///   run's actual is older than this run's COUNT.
fn chunked_row_estimate(
    key_span: Option<i64>,
    catalog: Option<i64>,
    measured: bool,
    span_is_exact: bool,
) -> Option<i64> {
    // An exact count needs no second opinion, in either direction.
    if span_is_exact && let Some(span) = key_span {
        return Some(span);
    }
    if measured {
        return catalog.or(key_span);
    }
    match (key_span, catalog) {
        (Some(span), Some(cat)) if span >= CATALOG_OVERRIDE_MIN_SPAN => Some(span.max(cat)),
        // Tiny span: keep the exact span — a larger catalog here is the
        // fresh-ANALYZE garbage shape, not evidence of a non-unique key
        // (bughunt 2026-08-13 push-back on the max() rule).
        (Some(span), Some(_)) => Some(span),
        (a, b) => a.or(b),
    }
}

/// Compute the `ComputedPlanData` portion of the artifact.
///
/// For `Chunked` exports this opens a source connection and calls
/// `detect_and_generate_chunks` to pre-compute chunk boundaries.  No rows are
/// exported — we only run the `SELECT min(col) / max(col)` boundary queries.
///
/// For `Incremental` exports we read the last cursor value from `StateStore`.
fn compute_plan_data(
    plan: &crate::plan::ResolvedRunPlan,
    row_estimate: Option<i64>,
    // #149 (roast 2026-08-09): true when `row_estimate` is a MEASURED actual
    // (from the state store's last whole-table run), so the chunked branch does
    // NOT override it with the key-SPAN — which on a sparse key is the span, not
    // the row count (950M..1.29B over 520K rows → 342M, worse than the measure).
    row_is_measured: bool,
    state: &StateStore,
) -> Result<ComputedPlanData> {
    match &plan.strategy {
        ExtractionStrategy::Chunked(cp) => {
            let mut src = source::create_source(&plan.source)?;
            let chunk_ranges = detect_and_generate_chunks(
                &mut *src,
                &plan.base_query,
                &cp.column,
                cp.chunk_size,
                cp.chunk_count,
                &plan.export_name,
                cp.dense,
                cp.by_days,
                plan.source.source_type,
            )?;
            let chunk_count = chunk_ranges.len();
            // F4 (0.7.5 audit): for chunked exports the artifact already
            // contains the exact key span (`chunk_ranges[0].0` ..
            // `chunk_ranges[-1].1`).  Using that as `row_estimate`
            // beats `pg_class.reltuples` (which is `1130` on a fresh
            // 30-row PG table because `ANALYZE` hasn't run).  This
            // makes PG and MySQL agree on the same artifact.
            let chunked_estimate = chunk_ranges
                .first()
                .zip(chunk_ranges.last())
                .map(|(first, last)| (last.1 - first.0 + 1).max(0));
            // The span is an exact ROW count only on the dense path, whose
            // ranges are ordinals `1..COUNT(*)`. `by_days` is checked FIRST in
            // `detect_and_generate_chunks`, so a config with both set produces
            // DAY ordinals — a day span is not a row count, hence the guard.
            let span_is_exact = cp.dense && cp.by_days.is_none();
            Ok(ComputedPlanData {
                chunk_ranges,
                chunk_count,
                cursor_snapshot: None,
                row_estimate: chunked_row_estimate(
                    chunked_estimate,
                    row_estimate,
                    row_is_measured,
                    span_is_exact,
                ),
            })
        }

        ExtractionStrategy::Incremental(_) => {
            let cursor_snapshot = state.get(&plan.export_name)?.last_cursor_value;
            Ok(ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot,
                row_estimate,
            })
        }

        // Keyset pages are computed dynamically at run time (seek pagination),
        // so there are no precomputed ranges to describe here — like a snapshot.
        ExtractionStrategy::Snapshot
        | ExtractionStrategy::TimeWindow { .. }
        | ExtractionStrategy::Keyset(_) => Ok(ComputedPlanData {
            chunk_ranges: vec![],
            chunk_count: 0,
            cursor_snapshot: None,
            row_estimate,
        }),
    }
}

/// Print the pool scheduler's predicted makespan (#166) beside the wave plan,
/// from the SAME predictor `apply --pool` schedules with
/// ([`super::pool::predict_items`]) — the preview and the schedule are one
/// number by construction. History-less exports are included at the
/// placeholder (the old preview excluded them, so its makespan covered less
/// work than the wave plan beside it); when any prediction rests on a
/// placeholder or a failed attempt, the print says LOWER BOUND, mirroring
/// `run_pool`'s accounting.
/// May the pool advisory be printed for this output format?
///
/// `--format json` with no `--output` makes STDOUT the machine document: a
/// human advisory printed beside it turns the stream into two documents, and
/// `serde_json`/`jq` reject the whole thing. This used to be latent — the
/// advisory required MEASURED durations, so a fresh config never reached it —
/// until the predictor gained placeholder estimates and started printing for
/// every multi-export plan (live-suite catch on this branch). The decision is
/// a pure fn so it is unit-testable; the print itself writes to stdout and
/// cannot be observed in-process.
fn pool_estimate_is_printable(format: &PlanOutputFormat) -> bool {
    !matches!(format, PlanOutputFormat::Json(None))
}

fn print_pool_estimate(
    artifacts: &[PlanArtifact],
    safe_of: &std::collections::HashMap<String, bool>,
    state: &StateStore,
) {
    if artifacts.len() < 2 {
        return; // a single export has nothing to schedule
    }
    let predicted = super::pool::predict_items(
        state,
        artifacts.iter().map(|a| {
            (
                a.export_name.as_str(),
                safe_of.get(&a.export_name).copied().unwrap_or(false),
            )
        }),
        &std::collections::HashMap::new(),
    );
    let items: Vec<super::pool::PoolItem> = predicted.iter().map(|(i, _)| i.clone()).collect();
    let measured = predicted
        .iter()
        .filter(|(_, f)| matches!(f, super::pool::PredictedFrom::Measured(_)))
        .count();
    let estimated = predicted.len() - measured;
    let total: f64 = items.iter().map(|i| i.predicted_secs).sum();
    println!(
        "\n  Pool scheduler ({measured} measured, {estimated} estimated of {} export(s)):",
        predicted.len(),
    );
    println!("    sequential: {:.0} min", total / 60.0);
    for m in [2usize, 4, 6] {
        let mk = super::pool::predicted_makespan_secs(&items, m);
        let floor = super::pool::makespan_floor_secs(&items, m);
        println!(
            "    pool M={m}: {:.0} min  (floor max(longest,total/M) = {:.0})",
            mk / 60.0,
            floor / 60.0
        );
    }
    if estimated > 0 {
        println!(
            "    prediction is a LOWER BOUND: {estimated} export(s) have no successful run to \
             measure from — it tightens as runs complete"
        );
    }
    println!(
        "    (a bounded work-stealing pool; --annotate-waves packs the wave \
         alternative — see #166)"
    );
}

fn emit_artifacts(
    artifacts: &[PlanArtifact],
    format: &PlanOutputFormat,
    multi_export: bool,
    config_path: &str,
) -> Result<()> {
    match format {
        PlanOutputFormat::Pretty => {
            if multi_export {
                // A schema scan can yield dozens of exports; the full per-export
                // block would be hundreds of lines. Show a compact, one-line-per-
                // export table sorted by wave, then the wave-execution hint. Use
                // `--export <name>` for a single export's full detail.
                print_compact_summary(artifacts, config_path);
            } else {
                for artifact in artifacts {
                    artifact.print_summary();
                }
            }
        }
        // Multi-export JSON to stdout: a single export prints its object
        // unchanged, but printing N objects back-to-back is invalid JSON (audit
        // #L10: `jq` fails with "Extra data"). Wrap them in a JSON array so the
        // stream parses as one document.
        PlanOutputFormat::Json(None) => {
            if artifacts.len() > 1 {
                // `&[PlanArtifact]` serializes as a JSON array — one parseable
                // document rather than N concatenated objects.
                println!("{}", serde_json::to_string_pretty(artifacts)?);
            } else if let Some(artifact) = artifacts.first() {
                println!("{}", artifact.to_json_pretty()?);
            }
        }
        PlanOutputFormat::Json(Some(path)) => {
            for artifact in artifacts {
                let json = artifact.to_json_pretty()?;
                // For a single export keep the operator's exact path so `rivet
                // apply <path>` works verbatim. For multiple exports a single
                // path would overwrite (audit #4): give each export its own file.
                let out_path = if multi_export {
                    per_export_output_path(path, &artifact.export_name)
                } else {
                    path.clone()
                };
                std::fs::write(&out_path, &json)
                    .map_err(|e| anyhow::anyhow!("cannot write plan file '{}': {}", out_path, e))?;
                println!("Plan written to: {}", out_path);
            }
        }
    }
    Ok(())
}

/// Compact one-line-per-export table for a multi-export plan, sorted by wave
/// (then by descending score, then name). The full per-export block
/// (`print_summary`) would be hundreds of lines for a schema scan, so it is
/// reserved for a single-export plan (`--export <name>`).
fn print_compact_summary(artifacts: &[PlanArtifact], config_path: &str) {
    let key = |a: &PlanArtifact| {
        a.prioritization
            .as_ref()
            .map(|p| {
                (
                    p.export_recommendation.recommended_wave,
                    p.export_recommendation.priority_score,
                )
            })
            .unwrap_or((u32::MAX, 0))
    };
    let mut order: Vec<&PlanArtifact> = artifacts.iter().collect();
    order.sort_by(|a, b| {
        let (wa, sa) = key(a);
        let (wb, sb) = key(b);
        wa.cmp(&wb)
            .then(sb.cmp(&sa))
            .then(a.export_name.cmp(&b.export_name))
    });
    let name_w = order
        .iter()
        .map(|a| a.export_name.chars().count())
        .max()
        .unwrap_or(6)
        .clamp(6, 32);

    println!();
    println!(
        "  Plan: {} exports — `rivet apply {}` runs them by wave (lowest first)",
        artifacts.len(),
        config_path
    );
    println!();
    println!("{}", PlanArtifact::summary_header(name_w));
    for a in &order {
        println!("{}", a.summary_line(name_w));
    }
    println!();
    println!("  Full detail for one export:  rivet plan -c <config> --export <name>");
}

/// Per-export YAML fields that `plan` records in place (`wave`, `parallel_safe`),
/// keyed by export name → ordered `(field, value)` pairs.
type ExportFields = HashMap<String, Vec<(&'static str, String)>>;

/// Replace the cost model's `recommended_wave` with waves PACKED from measured
/// run history (`--annotate-waves` only). Tier = the export's existing `wave:`
/// (the operator's priority) when set, else the cost model's recommendation;
/// duration = the last successful run's, else a rows-based estimate; every
/// export is `parallel_safe: true` afterwards because under packing the WAVE
/// is the concurrency cap (K by cost class, RSS-guarded).
fn repack_from_history(
    recs: Vec<(String, u32, bool)>,
    cost_of: &std::collections::HashMap<String, crate::plan::CostClass>,
    isolate_of: &std::collections::HashSet<String>,
    state: &crate::state::StateStore,
) -> Vec<(String, u32, bool)> {
    let mut measured_n = 0usize;
    let items: Vec<crate::plan::waves::PackItem> = recs
        .iter()
        .map(|(name, rec_wave, _)| {
            // Tier by the cost model's recommended_wave, NOT the config's
            // current wave: under --annotate-waves the planner OWNS the schedule
            // (the additive default path preserves hand-set waves instead).
            // Tiering by existing.wave made a second annotate read the first
            // run's machine-written waves as operator tiers and froze the
            // packing (bug hunt 2026-08-08); recommended_wave is deterministic
            // from the cost model, so re-annotation is idempotent.
            let tier = *rec_wave;
            // Last successful run: the best predictor of the next one.
            let last = state
                .get_metrics(Some(name), 25)
                .ok()
                .into_iter()
                .flatten()
                .find(|m| m.status == "success");
            let (secs, rss) = match &last {
                Some(m) => {
                    measured_n += 1;
                    (
                        (m.duration_ms as f64 / 1000.0).max(0.001),
                        m.peak_rss_mb.unwrap_or(150),
                    )
                }
                // No history: a flat placeholder. Deliberately coarse — its
                // only job is ORDERING within the tier until a real run
                // exists; the label below says "estimated" so nobody mistakes
                // it for knowledge (#148/#149 give it real numbers later).
                None => (5.0, 150),
            };
            crate::plan::waves::PackItem {
                name: name.clone(),
                tier,
                cost_class: cost_of
                    .get(name)
                    .copied()
                    .unwrap_or(crate::plan::CostClass::Medium),
                predicted_secs: secs,
                peak_rss_mb: rss,
            }
        })
        .collect();
    let waves = crate::plan::waves::pack(&items, 4096);
    log::warn!(
        "plan --annotate-waves: packed {} export(s) into {} wave(s) from run history \
         ({} measured, {} estimated — an estimate orders the first run only)",
        items.len(),
        waves.len(),
        measured_n,
        items.len() - measured_n
    );
    let mut wave_of: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    for w in &waves {
        for m in &w.members {
            wave_of.insert(m.clone(), w.number);
        }
    }
    recs.into_iter()
        .map(|(name, rec_wave, _)| {
            let w = wave_of.get(&name).copied().unwrap_or(rec_wave);
            // parallel_safe: true so a packed wave's members run concurrently
            // (the wave IS the cap) — EXCEPT an isolate_on_source export, which
            // must run alone even among wave-mates, so run_waves keeps it in
            // its own single-child batch (bug-hunt round 2: forcing true here
            // discarded isolation and could overload a contended source).
            let ps = !isolate_of.contains(&name);
            (name, w, ps)
        })
        .collect()
}

/// Which of the plan's recommendations may actually be WRITTEN into the
/// operator's config: absent fields always; present fields only under
/// `--annotate-waves`. Returns the fields plus how many exports were fully
/// preserved (had every recommended field already set).
///
/// PURE, and per-FIELD rather than per-export: an export with a hand-set
/// `wave:` but no `parallel_safe:` gets only the missing half. The operator's
/// schedule is a decision; plan's job is to fill blanks, not to have opinions
/// about decisions already made (#150 — the config-clobber class).
fn fields_to_write(
    recs: &[(String, u32, bool)],
    config: &Config,
    annotate_waves: bool,
) -> (ExportFields, usize) {
    let mut fields: ExportFields = HashMap::new();
    let mut preserved = 0usize;
    for (name, wave, parallel_safe) in recs {
        let existing = config.exports.iter().find(|e| &e.name == name);
        let mut items: Vec<(&'static str, String)> = Vec::new();
        if annotate_waves || existing.is_none_or(|e| e.wave.is_none()) {
            items.push(("wave", wave.to_string()));
        }
        if annotate_waves || existing.is_none_or(|e| e.parallel_safe.is_none()) {
            items.push(("parallel_safe", parallel_safe.to_string()));
        }
        if items.is_empty() {
            preserved += 1;
        } else {
            fields.insert(name.clone(), items);
        }
    }
    (fields, preserved)
}

/// What `apply --pool` will read for `parallel_safe` on each export AFTER this
/// plan run finished writing — the value the preview must model.
///
/// The recommendation is NOT that value. `fields_to_write` preserves a hand-set
/// `parallel_safe:` (the operator's decision, #150 config-clobber class), so a
/// config that says `true` on an export the cost model rates High keeps `true`
/// on disk while `recs` says `false`. Sourcing the preview from `recs` made the
/// preview apply the C3 heavy-serialization floor to an export apply runs
/// M-way (preview 90 min, real 30) — and inverted, advertised parallelism apply
/// serializes, the exact defect the C3 model fixed on the apply side
/// (bughunt 2026-08-14). Three inputs, one rule per export:
///
///  * this run WROTE `parallel_safe` for it (blank field, or `--annotate-waves`
///    overwriting, AND a `- name:` line actually matched) → the recommendation
///    is now on disk;
///  * otherwise the config's own value survives;
///  * otherwise the field is absent from the file and `apply` reads
///    `unwrap_or(false)` — including the case where the write was attempted but
///    matched no `- name:` line (the `unmatched` warning above).
fn effective_parallel_safe(
    recs: &[(String, u32, bool)],
    config: &Config,
    fields: &ExportFields,
    written: &std::collections::HashSet<String>,
) -> HashMap<String, bool> {
    recs.iter()
        .map(|(name, _, rec_safe)| {
            let wrote_safe = written.contains(name)
                && fields
                    .get(name)
                    .is_some_and(|items| items.iter().any(|(k, _)| *k == "parallel_safe"));
            let effective = if wrote_safe {
                *rec_safe
            } else {
                config
                    .exports
                    .iter()
                    .find(|e| &e.name == name)
                    .and_then(|e| e.parallel_safe)
                    .unwrap_or(false)
            };
            (name.clone(), effective)
        })
        .collect()
}

fn write_plan_fields_to_config(
    config_path: &str,
    fields: &ExportFields,
) -> Result<std::collections::HashSet<String>> {
    if fields.is_empty() {
        return Ok(std::collections::HashSet::new());
    }
    let original = std::fs::read_to_string(config_path).map_err(|e| {
        anyhow::anyhow!(
            "cannot read config '{}' to record plan fields: {}",
            config_path,
            e
        )
    })?;
    let (updated, written) = apply_field_annotations(&original, fields);
    if updated != original {
        std::fs::write(config_path, updated).map_err(|e| {
            anyhow::anyhow!(
                "cannot write plan fields to config '{}': {}",
                config_path,
                e
            )
        })?;
    }
    Ok(written)
}

/// Pure core of [`write_plan_fields_to_config`] (text in → text out, unit-tested).
/// For each named export, inserts its `<field>: <value>` lines right after
/// `- name:` and drops any stale line for those same fields. Returns the set of
/// export names it ACTUALLY matched in the YAML — a name in `fields` that never
/// appears as a `- name:` line writes nothing, and the caller must not report
/// it as annotated (bug hunt 2026-08-08: the warn counted intent, not writes).
fn apply_field_annotations(
    yaml: &str,
    fields: &ExportFields,
) -> (String, std::collections::HashSet<String>) {
    let mut out = String::with_capacity(yaml.len() + 64);
    let mut written: std::collections::HashSet<String> = std::collections::HashSet::new();
    // (export name, indent of the `-`, indent of the export's fields) while
    // inside an export block; `None` between / outside exports.
    let mut current: Option<(String, usize, usize)> = None;

    for line in yaml.split_inclusive('\n') {
        let content = line.strip_suffix('\n').unwrap_or(line);

        if let Some((dash_indent, name)) = parse_export_name(content) {
            out.push_str(line);
            let field_indent = dash_indent + 2;
            if let Some(items) = fields.get(&name) {
                for (key, value) in items {
                    out.push_str(&" ".repeat(field_indent));
                    out.push_str(&format!("{key}: {value}\n"));
                }
                written.insert(name.clone());
            }
            current = Some((name, dash_indent, field_indent));
            continue;
        }

        if let Some((name, dash_indent, field_indent)) = current.as_ref() {
            let trimmed = content.trim_start();
            let indent = content.len() - trimmed.len();
            let blank_or_comment = trimmed.is_empty() || trimmed.starts_with('#');
            if !blank_or_comment && indent <= *dash_indent {
                current = None; // dedented out of the export block
            } else if indent == *field_indent
                && let Some(items) = fields.get(name)
                && items
                    .iter()
                    .any(|(key, _)| trimmed.starts_with(&format!("{key}:")))
            {
                continue; // drop the stale field line — the fresh one is already in
            }
        }
        out.push_str(line);
    }
    (out, written)
}

/// Parse a `<indent>- name: <name>` export list item → `(indent_of_dash, name)`.
/// Handles quoted names; `None` for any other line.
fn parse_export_name(line: &str) -> Option<(usize, String)> {
    let trimmed = line.trim_start();
    let dash_indent = line.len() - trimmed.len();
    let rest = trimmed.strip_prefix("- ")?;
    let value = rest.trim_start().strip_prefix("name:")?;
    // Drop an inline ` # …` comment (a YAML comment is a space then a hash).
    let value = value.split(" #").next().unwrap_or(value);
    let name = value
        .trim()
        .trim_matches(|c: char| c == '"' || c == '\'')
        .to_string();
    (!name.is_empty()).then_some((dash_indent, name))
}

#[cfg(test)]
mod tests {
    use super::per_export_output_path;
    use std::path::Path;

    use super::{
        ExportFields, apply_field_annotations, effective_parallel_safe, fields_to_write,
        refuse_annotate_scoped_to_export, repack_from_history,
    };

    /// The refuse-guard fires ONLY when BOTH `--annotate-waves` and `--export`
    /// are set — the `&&` must not become `||` (which would reject a plain
    /// `--export`) nor invert. All four combinations, so the mutant that flips
    /// the operator goes RED on the two rows it disagrees about.
    #[test]
    fn refuse_annotate_scoped_to_export_only_when_both_set() {
        assert!(
            refuse_annotate_scoped_to_export(true, true),
            "both set → refuse"
        );
        assert!(
            !refuse_annotate_scoped_to_export(true, false),
            "annotate whole config → allow"
        );
        assert!(
            !refuse_annotate_scoped_to_export(false, true),
            "plain --export → allow (|| would wrongly refuse this)"
        );
        assert!(
            !refuse_annotate_scoped_to_export(false, false),
            "neither → allow"
        );
    }

    /// The packer's duration must come from the REAL producer — the state
    /// store's own recorder — not a value this test hand-feeds one layer down
    /// (the fabricated-input class). Two exports, histories recorded through
    /// `record_metric`, ONE tier: the measured-slower export must land in the
    /// earlier (heavier) wave. RED against a producer-side mutant that reads
    /// the estimate instead of the metric: with no history consulted, both
    /// exports carry the same flat placeholder and the ordering assert has
    /// nothing to stand on (fails on the pair split below).
    #[test]
    fn repack_reads_durations_from_the_state_stores_recorder() {
        let state = crate::state::StateStore::open_in_memory().expect("state");
        for (name, ms) in [("slowpoke", 90_000i64), ("quickie", 1_000i64)] {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: name.to_string(),
                    run_id: format!("{name}_run"),
                    duration_ms: ms,
                    total_rows: 10,
                    peak_rss_mb: Some(120),
                    status: "success".to_string(),
                    error_message: None,
                    tuning_profile: None,
                    format: None,
                    mode: None,
                    files_produced: 1,
                    bytes_written: 100,
                    retries: 0,
                    validated: None,
                    schema_changed: None,
                    ..Default::default()
                })
                .expect("record");
        }
        // …and a failed newer run for slowpoke, which must be IGNORED — only a
        // successful run predicts anything.
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: "slowpoke".to_string(),
                run_id: "slowpoke_failed".to_string(),
                duration_ms: 5,
                total_rows: 0,
                peak_rss_mb: Some(50),
                status: "failed".to_string(),
                error_message: Some("boom".to_string()),
                tuning_profile: None,
                format: None,
                mode: None,
                files_produced: 0,
                bytes_written: 0,
                retries: 0,
                validated: None,
                schema_changed: None,
                ..Default::default()
            })
            .expect("record failed");

        let recs = vec![
            ("slowpoke".to_string(), 2, false),
            ("quickie".to_string(), 2, false),
            ("newcomer".to_string(), 2, false),
        ];
        let cost_of = recs
            .iter()
            .map(|(n, _, _)| (n.clone(), crate::plan::CostClass::High))
            .collect();

        // No isolate_on_source exports in this fixture.
        let packed = repack_from_history(recs, &cost_of, &std::collections::HashSet::new(), &state);
        let wave = |n: &str| packed.iter().find(|(m, _, _)| m == n).unwrap().1;
        // K=2 (High): measured 90s pairs with measured 1s in wave 1; the
        // history-less newcomer (flat 5s estimate) lands after the measured
        // slowpoke, proving the metric — not the placeholder — ordered them.
        assert!(
            wave("slowpoke") < wave("newcomer") || wave("slowpoke") == wave("newcomer"),
            "measured 90s must not sort below a 5s placeholder"
        );
        assert_eq!(wave("slowpoke"), 1, "the measured-heaviest opens the tier");
        // every packed export is parallel_safe: the wave is the cap now
        assert!(packed.iter().all(|(_, _, ps)| *ps));
        // the failed 5ms run did NOT become slowpoke's prediction: had it,
        // slowpoke (0.005s) would sort below quickie (1s) and lose wave 1's
        // first slot to it.
        let w1: Vec<&str> = packed
            .iter()
            .filter(|(_, w, _): &&(String, u32, bool)| *w == 1)
            .map(|(n, _, _)| n.as_str())
            .collect();
        assert!(w1.contains(&"slowpoke"), "w1={w1:?}");
    }

    /// An isolate_on_source export must come out parallel_safe:FALSE even under
    /// packing (bug-hunt round 2): the packer sets true so wave-mates run
    /// concurrently, but a contended-source export must stay alone in its wave.
    #[test]
    fn repack_keeps_isolate_on_source_out_of_the_concurrent_batch() {
        let state = crate::state::StateStore::open_in_memory().expect("state");
        for n in ["lonely", "buddy"] {
            state
                .record_metric_full(&crate::state::MetricRow {
                    export_name: n.to_string(),
                    run_id: format!("{n}_r"),
                    duration_ms: 1000,
                    total_rows: 10,
                    peak_rss_mb: Some(100),
                    status: "success".to_string(),
                    error_message: None,
                    tuning_profile: None,
                    format: None,
                    mode: None,
                    files_produced: 1,
                    bytes_written: 100,
                    retries: 0,
                    validated: None,
                    schema_changed: None,
                    ..Default::default()
                })
                .expect("rec");
        }
        let recs = vec![
            ("lonely".to_string(), 4, false),
            ("buddy".to_string(), 4, true),
        ];
        let cost_of = recs
            .iter()
            .map(|(n, _, _)| (n.clone(), crate::plan::CostClass::Low))
            .collect();
        let mut isolate = std::collections::HashSet::new();
        isolate.insert("lonely".to_string());
        let packed = repack_from_history(recs, &cost_of, &isolate, &state);
        let ps = |n: &str| packed.iter().find(|(m, _, _)| m == n).unwrap().2;
        assert!(
            !ps("lonely"),
            "isolate_on_source export must be parallel_safe:false"
        );
        assert!(
            ps("buddy"),
            "a normal packed export stays parallel_safe:true"
        );
    }

    /// --annotate-waves + --export is a category error: packing one export
    /// alone rewrites its wave to 1, inverting the schedule (bug hunt
    /// 2026-08-08). The refusal fires before any config load, so a bogus path
    /// still errors on the combination, not on IO.
    #[test]
    fn annotate_waves_refuses_to_be_scoped_to_one_export() {
        let err = super::run_plan_command(
            "/nonexistent/rivet.yaml",
            Some("just_me"),
            None,
            super::PlanOutputFormat::Pretty,
            true, // annotate_waves
        )
        .expect_err("--annotate-waves + --export must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--annotate-waves") && msg.contains("--export"),
            "the refusal must name both flags: {msg}"
        );
    }

    /// The plan preview and apply --pool now share ONE predictor
    /// ([`crate::pipeline::pool::predict_items`]) — its decisions (success
    /// filter, ms→s, placeholder inclusion) are pinned in pool.rs beside the
    /// implementation. What remains plan-side is only the wiring pinned by
    /// `print_pool_estimate`'s callsite (real `parallel_safe` map + the
    /// artifacts iterator), which the drift this closed lived in.
    /// The additive contract (#150): plan fills BLANKS; a value the operator
    /// set is untouched unless `--annotate-waves` says overwrite. Per FIELD —
    /// a hand-set `wave:` with no `parallel_safe:` gets only the missing half.
    ///
    /// RED against the pre-fix behavior (every recommendation written
    /// unconditionally): the hand-tuned case below would come back with a
    /// `wave` entry and the test fails on the exact clobber the field hit —
    /// a 5-per-wave split becoming one 76-export wave.
    #[test]
    fn plan_fills_blanks_and_never_clobbers_a_hand_set_schedule() {
        let source = crate::config::SourceConfig {
            source_type: crate::config::SourceType::Postgres,
            url: Some("postgresql://localhost/test".into()),
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
        };
        let mut cfg = crate::config::Config {
            source,
            exports: vec![
                crate::config::sample_export("hand_tuned"),
                crate::config::sample_export("half_set"),
                crate::config::sample_export("blank"),
            ],
            notifications: None,
            parallel_exports: false,
            parallel_export_processes: false,
            load: None,
        };
        cfg.exports[0].wave = Some(7);
        cfg.exports[0].parallel_safe = Some(false);
        cfg.exports[1].wave = Some(7); // parallel_safe left blank
        let recs = vec![
            ("hand_tuned".to_string(), 2, true),
            ("half_set".to_string(), 2, true),
            ("blank".to_string(), 2, true),
        ];

        let (fields, preserved) = fields_to_write(&recs, &cfg, false);
        assert!(
            !fields.contains_key("hand_tuned"),
            "a fully hand-set export must not be touched: {fields:?}"
        );
        assert_eq!(preserved, 1, "exactly the fully-set export is preserved");
        let half: Vec<&str> = fields["half_set"].iter().map(|(k, _)| *k).collect();
        assert_eq!(
            half,
            vec!["parallel_safe"],
            "only the MISSING half is written"
        );
        let blank: Vec<&str> = fields["blank"].iter().map(|(k, _)| *k).collect();
        assert_eq!(blank, vec!["wave", "parallel_safe"]);

        // --annotate-waves is the explicit overwrite: everything is written.
        let (fields, preserved) = fields_to_write(&recs, &cfg, true);
        assert_eq!(preserved, 0);
        assert_eq!(fields.len(), 3);
        assert_eq!(
            fields["hand_tuned"]
                .iter()
                .map(|(k, _)| *k)
                .collect::<Vec<_>>(),
            vec!["wave", "parallel_safe"]
        );
    }

    /// The preview and the schedule must be ONE number: `plan`'s pool print
    /// models the `parallel_safe` that is on DISK after the annotations were
    /// written, not the recommendation — because `fields_to_write` deliberately
    /// preserves a hand-set flag (the test directly above pins that
    /// preservation, and that is exactly the state where the two disagreed).
    ///
    /// Fixture carries THREE exports, one per branch of the rule, because the
    /// subject is a per-export fold: a hand-set flag that must WIN over the
    /// rec, a blank one whose write LANDED (rec wins), and a blank one whose
    /// write matched no `- name:` line (nothing on disk → `apply` reads
    /// `unwrap_or(false)`).
    ///
    /// RED-proven against the pre-fix source — `recs.iter().map(|(n, _, s)| (n,
    /// *s))` — which reports `hand_tuned = true` and `unmatched = true` where
    /// apply reads `false` / `false`.
    #[test]
    fn pool_preview_models_the_parallel_safe_apply_will_read_not_the_recommendation() {
        let source = crate::config::SourceConfig {
            source_type: crate::config::SourceType::Postgres,
            url: Some("postgresql://localhost/test".into()),
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
        };
        let mut cfg = crate::config::Config {
            source,
            exports: vec![
                crate::config::sample_export("hand_tuned"),
                crate::config::sample_export("blank"),
                crate::config::sample_export("unmatched"),
            ],
            notifications: None,
            parallel_exports: false,
            parallel_export_processes: false,
            load: None,
        };
        // The operator's decision: independent rows, run me concurrently.
        cfg.exports[0].wave = Some(7);
        cfg.exports[0].parallel_safe = Some(false);
        // The recommendation disagrees with the config on every export.
        let recs = vec![
            ("hand_tuned".to_string(), 2, true),
            ("blank".to_string(), 2, true),
            ("unmatched".to_string(), 2, true),
        ];
        let (fields, _preserved) = fields_to_write(&recs, &cfg, false);
        assert!(
            fields.contains_key("unmatched"),
            "the fixture must ATTEMPT the write it then fails to match"
        );
        // `write_plan_fields_to_config` returns only the names whose `- name:`
        // line matched; a templated/oddly-quoted name writes nothing.
        let written: std::collections::HashSet<String> =
            ["blank".to_string()].into_iter().collect();

        let safe = effective_parallel_safe(&recs, &cfg, &fields, &written);
        assert_eq!(
            safe.get("hand_tuned"),
            Some(&false),
            "a hand-set flag is what apply reads — the rec was never written"
        );
        assert_eq!(
            safe.get("blank"),
            Some(&true),
            "a blank field the plan just filled reads back as the rec"
        );
        assert_eq!(
            safe.get("unmatched"),
            Some(&false),
            "nothing landed on disk, so apply's `unwrap_or(false)` is the truth"
        );
    }

    fn wave_fields(pairs: &[(&str, u32)]) -> ExportFields {
        pairs
            .iter()
            .map(|(n, w)| (n.to_string(), vec![("wave", w.to_string())]))
            .collect()
    }

    /// Inserts `wave:` right after each `- name:`, replaces a stale one, and
    /// leaves comments / other fields / unlisted exports untouched.
    #[test]
    fn wave_annotations_insert_replace_and_preserve() {
        let yaml = "exports:\n  - name: orders   # the orders table\n    mode: incremental\n    wave: 9\n    cursor_column: updated_at\n  - name: events\n    mode: full\ndestination:\n  type: local\n";
        let (out, written) =
            apply_field_annotations(yaml, &wave_fields(&[("orders", 2), ("events", 1)]));
        assert_eq!(written.len(), 2, "both named exports matched");
        // orders: stale wave 9 replaced with 2, placed after name (comment kept)
        assert!(
            out.contains("- name: orders   # the orders table\n    wave: 2\n"),
            "{out}"
        );
        assert!(
            !out.contains("wave: 9"),
            "stale wave must be dropped:\n{out}"
        );
        // events: fresh wave 1 inserted right after name
        assert!(out.contains("- name: events\n    wave: 1\n"), "{out}");
        // untouched: the cursor field and the trailing top-level key
        assert!(out.contains("cursor_column: updated_at"));
        assert!(out.contains("destination:\n  type: local"));
    }

    /// An export whose name isn't in the map is left byte-identical.
    #[test]
    fn wave_annotations_leave_unlisted_export_untouched() {
        let yaml = "exports:\n  - name: orders\n    mode: full\n";
        let (out, written) = apply_field_annotations(yaml, &wave_fields(&[("other", 1)]));
        // "other" matches no `- name:` line — nothing written, and the set says so.
        assert!(
            written.is_empty(),
            "a name absent from the YAML writes nothing"
        );
        assert_eq!(out, yaml);
    }

    /// Regression (audit #4): two distinct exports under one `--output FILE`
    /// must derive two distinct paths, so neither is silently overwritten.
    #[test]
    fn per_export_paths_are_distinct() {
        let a = per_export_output_path("/tmp/plan.json", "orders");
        let b = per_export_output_path("/tmp/plan.json", "users");
        assert_ne!(a, b, "distinct exports must not collide on one path");
        assert_eq!(a, "/tmp/plan.orders.json");
        assert_eq!(b, "/tmp/plan.users.json");
    }

    /// The original extension is preserved so `rivet apply` (and any `*.json`
    /// glob) still find the per-export files.
    #[test]
    fn per_export_path_keeps_json_extension() {
        let p = per_export_output_path("/tmp/plan.json", "orders");
        assert_eq!(
            Path::new(&p).extension().and_then(|e| e.to_str()),
            Some("json"),
            "per-export file must keep the .json extension"
        );
    }

    /// An export name with no extension on the base still gets a distinct,
    /// non-colliding suffix rather than overwriting the base path.
    #[test]
    fn per_export_path_without_extension_appends_name() {
        let p = per_export_output_path("/tmp/plan", "orders");
        assert_eq!(p, "/tmp/plan.orders");
    }

    /// A hostile export name (path separators, dots) must not escape the
    /// directory or mangle the extension — every unsafe byte is replaced.
    #[test]
    fn per_export_path_sanitizes_unsafe_export_name() {
        let p = per_export_output_path("/tmp/plan.json", "../../etc/passwd");
        // No remaining path separators or `..` traversal in the derived name.
        assert!(
            !p.contains(".."),
            "sanitized path must not contain a `..` traversal: {p}"
        );
        let file = Path::new(&p)
            .file_name()
            .and_then(|f| f.to_str())
            .expect("derived path has a file name");
        assert!(
            !file.contains('/') && !file.contains('\\'),
            "sanitized file name must not contain a path separator: {file}"
        );
        assert_eq!(
            Path::new(&p).parent().and_then(|d| d.to_str()),
            Some("/tmp"),
            "derived path must stay in the requested directory"
        );
        assert_eq!(
            Path::new(&p).extension().and_then(|e| e.to_str()),
            Some("json"),
            "extension must survive sanitization"
        );
    }

    /// The pool advisory must never share STDOUT with the machine document.
    ///
    /// `rivet plan --format json` (no `--output`) makes stdout ONE JSON
    /// document; the advisory printed before it produced "expected value at
    /// line 2 column 3" for every multi-export config once the predictor
    /// started estimating from placeholders instead of requiring measured
    /// history. RED against `!matches!(..)` -> `true` (the shipped state).
    #[test]
    fn the_pool_advisory_never_shares_stdout_with_the_json_document() {
        use super::{PlanOutputFormat, pool_estimate_is_printable};
        // stdout carries the document — nothing else may be written there.
        assert!(!pool_estimate_is_printable(&PlanOutputFormat::Json(None)));
        // A file target leaves stdout free for the human lines.
        assert!(pool_estimate_is_printable(&PlanOutputFormat::Json(Some(
            "plan.json".to_string()
        ))));
        assert!(pool_estimate_is_printable(&PlanOutputFormat::Pretty));
    }

    /// Field find (2026-08-13 pool dogfood): a chunked export on a NON-UNIQUE
    /// key (a versioned table, ~2.5 rows per key value) has `catalog > span`,
    /// and the old span-always-wins rule under-reported ~831M rows as the
    /// ~333M key span — which then fed the pool's makespan prediction.
    #[test]
    fn chunked_row_estimate_prefers_catalog_when_key_is_provably_non_unique() {
        use super::chunked_row_estimate;
        // catalog > span ⇒ rows > distinct keys ⇒ the span is only a floor.
        assert_eq!(
            chunked_row_estimate(Some(333_000_000), Some(831_000_000), false, false),
            Some(831_000_000),
        );
        // Sparse key (#149 shape): span dwarfs the catalog — span still wins,
        // exactly the pre-existing behavior (over-estimating is the safe
        // direction for scheduling; the catalog is not trusted to shrink it).
        assert_eq!(
            chunked_row_estimate(Some(342_000_000), Some(520_000), false, false),
            Some(342_000_000),
        );
        // A measured whole-table actual beats the span in either direction.
        assert_eq!(
            chunked_row_estimate(Some(342_000_000), Some(520_000), true, false),
            Some(520_000),
        );
        // F4 fresh-ANALYZE shape: tiny exact span, garbage catalog above it —
        // the span must survive (the catalog override is gated on a span big
        // enough to be worth chunking).
        assert_eq!(
            chunked_row_estimate(Some(30), Some(1130), false, false),
            Some(30)
        );
        // Missing signals degrade to whichever side exists.
        assert_eq!(chunked_row_estimate(Some(42), None, false, false), Some(42));
        assert_eq!(chunked_row_estimate(None, Some(42), false, false), Some(42));
        assert_eq!(chunked_row_estimate(None, None, false, false), None);
    }

    /// `chunk_dense: true` makes the span an EXACT `COUNT(*)` over ordinals
    /// `1..row_count`, so no catalog figure — and no older measured actual —
    /// may override it. Bughunt 2026-08-13: the `span.max(catalog)` rule was
    /// written for a KEY span (where `catalog > span` proves a non-unique key);
    /// applied to a dense span it republishes a stale `reltuples`, so a table
    /// that lost 90% of its rows without `ANALYZE` schedules at its old size.
    /// Both directions are asserted — a fold that only checked the larger side
    /// would pass on `max()` alone.
    #[test]
    fn a_dense_span_is_an_exact_count_and_no_catalog_may_override_it() {
        use super::chunked_row_estimate;
        // Stale catalog ABOVE the fresh exact count (the 90%-deleted table):
        // the count wins. RED against `span.max(cat)` reaching the dense path.
        assert_eq!(
            chunked_row_estimate(Some(120_000), Some(1_200_000), false, true),
            Some(120_000),
        );
        // Catalog BELOW it (the ordinary lagging-stats direction): unchanged.
        assert_eq!(
            chunked_row_estimate(Some(1_200_000), Some(120_000), false, true),
            Some(1_200_000),
        );
        // A prior run's MEASURED actual is older than this plan's COUNT(*),
        // so the exact count outranks it too (RED against an early
        // `if measured` return).
        assert_eq!(
            chunked_row_estimate(Some(120_000), Some(1_200_000), true, true),
            Some(120_000),
        );
        // Small dense tables take the same path — the tiny-span carve-out for
        // fresh-ANALYZE garbage is subsumed, not contradicted.
        assert_eq!(
            chunked_row_estimate(Some(30), Some(1130), false, true),
            Some(30)
        );
        // An empty dense table yields no ranges → no span; the catalog is all
        // that is left, exactly as on the non-dense path.
        assert_eq!(
            chunked_row_estimate(None, Some(1130), false, true),
            Some(1130)
        );
    }
}
