//! **Layer: Coordinator** (post-run finalization steps)
//!
//! End-of-run hooks invoked by `pipeline::job` *after* the export has
//! reached its terminal status.  Each finalize step is intentionally
//! best-effort: a failure here does not change the run's exit code,
//! because the data has already landed (or definitively not landed) at
//! the destination.  Failure-handling policy mirrors ADR-0001 §I7
//! (manifest failures are non-fatal); see [`crate::pipeline::job`] for
//! the call order.
//!
//! Why this is a separate module: prior to this split the same file held
//! `run_export_job`, the `finalize_*` hooks, the M8 gate, and the
//! `destination_uri_for_manifest` helper.  At ~1100 lines `job.rs` was
//! becoming a god-module — Phase C-γ would have grown it by another
//! ~200.  Splitting on the natural boundary (orchestration vs.
//! finalization) keeps each file under ~800 lines and lets each test
//! suite import only what it needs.
//!
//! Functions are `pub(super)` so the only legal caller is
//! `pipeline::job::{run_export_job, run_export_job_with_chunk_source}`.
//! There is intentionally no public re-export for these — they are
//! orchestration glue, not a pipeline API.

use crate::config::DestinationConfig;
use crate::error::Result;
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

use super::summary::RunSummary;

/// ADR-0028: THE export tail — the one place the per-export post-write features
/// are applied, called by the dispatcher (`job.rs`) on runner success, before
/// `finalize_manifest`. Runners FEED `summary.ledger` as they commit (what
/// schema they saw, which checksums their sinks computed); this seam APPLIES:
///
///   1. manifest schema-fingerprint pin (ADR-0012 M3),
///   2. the `on_schema_drift` gate (post-run, from the run's resolved schema —
///      chunked runs its gate pre-chunk via `check_from_type_mappings`
///      (ADR-0021) and feeds no drift schema here, by design),
///   3. Form-B value-checksum harvest into the summary/manifest,
///   4. the shape-drift advisory warn.
///
/// Ordering is load-bearing and encoded HERE, once: the gate + harvest run
/// before `finalize_manifest` (a drift `fail` must abort before a manifest
/// exists; the manifest must record the checksums). Before this seam, every
/// runner re-assembled this tail by hand and a feature wired into one runner
/// was silently absent on the others — the runner-bypass class this seam
/// retires (three documented bites: the keyset/mongo drift-gate miss, Form-B
/// computed-then-discarded on all three large-table runners, and the keyset
/// part-name divergence). The telltale invariants in
/// `check_post_run_invariants` stay as the backstop: a runner that feeds
/// nothing still fails the drift/Form-B telltales.
pub(super) fn finalize_export(
    plan: &ResolvedRunPlan,
    state: Option<&StateStore>,
    summary: &mut RunSummary,
) -> Result<()> {
    let ledger = std::mem::take(&mut summary.ledger);

    if let Some(schema) = &ledger.drift_schema {
        // Fingerprint pin is idempotent (first call wins) — uniform across
        // runners now, so keyset/mongo manifests carry the fingerprint single
        // mode always recorded.
        super::manifest_writer::record_run_schema_fingerprint(summary, schema);
        if let Some(st) = state {
            super::schema_drift::check_from_sink_schema(
                st,
                &plan.export_name,
                schema,
                plan.schema_drift_policy,
                summary,
            )?;
        }
    }

    // Form B: record the run-wide checksums so the manifest carries them.
    // `harvest_column_checksums` itself suppresses on
    // `column_checksums_incomplete` (checkpoint-resume hydration) — that
    // decision stays in the harvest, not here.
    super::commit::harvest_column_checksums(
        summary,
        ledger.column_checksums,
        ledger.checksum_key_column,
    );

    // Epic 8: data shape drift — warn when string/binary columns grow beyond
    // threshold. Applied wherever a runner fed shape bytes (today the single
    // sink tracks them; a runner that starts feeding them gets the warn for
    // free — born `na`, per ADR-0028).
    if plan.shape_drift_warn_factor > 0.0
        && !ledger.column_max_bytes.is_empty()
        && let Some(st) = state
    {
        match st.detect_shape_drift(
            &plan.export_name,
            &ledger.column_max_bytes,
            plan.shape_drift_warn_factor,
        ) {
            Ok(warnings) => {
                for w in &warnings {
                    log::warn!(
                        "export '{}': shape drift in column '{}' — \
                         max byte length grew {:.1}× ({} → {} bytes); \
                         set `shape_drift_warn_factor` to a higher value to suppress",
                        plan.export_name,
                        w.column,
                        w.growth_factor,
                        w.stored_max_bytes,
                        w.current_max_bytes,
                    );
                    summary.journal.record(crate::journal::RunEvent::Warning {
                        context: format!("shape_drift:{}", w.column),
                        message: format!(
                            "column '{}' max byte length grew {:.1}× ({} → {} bytes)",
                            w.column, w.growth_factor, w.stored_max_bytes, w.current_max_bytes
                        ),
                    });
                }
            }
            Err(e) => log::warn!(
                "export '{}': shape tracking error: {:#}",
                plan.export_name,
                e
            ),
        }
    }

    Ok(())
}

/// Write `.rivet/runs/<run_id>/{summary.md,summary.json}` and surface a
/// stderr hint pointing at the report (plus a resume command, when
/// applicable).
///
/// Failures to write are non-fatal: the run keeps its existing exit code,
/// the reason is logged, and the resume hint is still shown so the operator
/// can recover even if disk-full prevents the report itself from landing.
/// Does this run have something to resume?
///
/// A FAILED run that already committed parts: the data is on the prefix, so the
/// next step is `--resume` and not a fresh run. On a success there is nothing to
/// resume, and on a failure that committed nothing a resume would find nothing.
///
/// Extracted because it could not be tested where it was. Four mutations of the
/// condition survived the suite — flipping `==` to `!=`, `>` to `<`/`==`/`>=` —
/// so the advisory could have stopped firing, or started firing on every
/// successful run, and nothing would have gone red. It matters more since a
/// manifest that fails to land now FAILS the run with its parts durable: that is
/// exactly the state this line addresses.
pub(super) fn should_offer_resume(summary: &RunSummary) -> bool {
    summary.status == "failed" && summary.files_committed > 0
}

pub(super) fn finalize_run_report(config_path: &str, summary: &RunSummary, kind: &str) {
    use std::io::Write;

    let dir = crate::pipeline::report::report_dir(config_path, &summary.run_id);
    let written = match crate::pipeline::report::write_run_report(config_path, summary) {
        Ok(_) => true,
        Err(e) => {
            log::warn!(
                "{} '{}': run report write failed (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
            false
        }
    };

    if crate::pipeline::ipc::capturing_events() {
        // The parent UI owns the screen in capturing mode; an extra stderr
        // tail here would interleave with the rendered cards.  The JSON/MD
        // files are still on disk for whoever wants them.
        return;
    }

    let stderr = std::io::stderr();
    let mut h = stderr.lock();
    // Per-export `report:` lines double the output of a multi-export run (one
    // per export); the run aggregate at the end already points at `.rivet/runs/`.
    // Keep the line only for a single export, where it is the one place to look.
    if written && !crate::pipeline::multi_export_mode() {
        let _ = writeln!(h, "report:    {}", dir.join("summary.md").display());
    }
    if should_offer_resume(summary) {
        let _ = writeln!(
            h,
            "resume:    rivet run --config {} --resume",
            crate::pipeline::report::shell_quote(config_path)
        );
    }
    let _ = h.flush();
}

/// Build the cloud-output manifest from the run's accumulated parts and
/// write it (plus `_SUCCESS` for clean runs) to the destination.
///
/// ADR-0012 M1 / M2 / M7: parts are already committed, manifest is written
/// next, then `_SUCCESS` only when status == Success.
///
/// Returns the reason the prefix did NOT become consumable, or `None` when it
/// did. This used to be logged at `warn` and dropped, so a run whose manifest
/// never landed printed `status: success`, `rows: 4,000`, `files: 4` — with its
/// parts on the prefix and no manifest naming them. A manifest-authoritative
/// `rivet load` does not load those rows, so "success" was a claim the artifacts
/// did not support. Worse, the caller advanced the incremental cursor
/// immediately after, on the stated premise that the manifest was durable: the
/// next run then started PAST data no manifest described.
///
/// The parts are still durable, which is exactly why this must be loud. Nothing
/// about the failure is visible in the data; it is visible only here.
/// A run that SKIPPED and committed no parts has nothing to describe.
///
/// Split out of `finalize_manifest` so the decision is reachable without a
/// `ResolvedRunPlan` and a `StateStore` — the mutation gate found both halves
/// of this condition unguarded (`==`→`!=` and `&&`→`||` both survived), and
/// each inversion is a real regression: flipping the equality makes every
/// SUCCESSFUL run skip its manifest, and widening the `&&` to `||` throws away
/// the manifest of a skipped run that DID commit parts — the
/// `[RIVET_VERIFY_SUCCESS_STALE]` shape this guard was added to end.
pub(super) fn skipped_run_wrote_nothing(summary: &RunSummary) -> bool {
    summary.status == "skipped" && summary.manifest_parts.is_empty()
}

pub(super) fn finalize_manifest(
    plan: &ResolvedRunPlan,
    // The export FAMILY (`ExportConfig::family()`), passed at RUNTIME and
    // deliberately NOT a field of `ResolvedRunPlan`: that type is sealed into
    // plan.json and its integrity hash covers the whole serialized struct, so
    // any field added to it invalidates every plan artifact across a version
    // boundary — in BOTH directions, with an error that blames the user for
    // hand-editing. Measured: adding it there broke released-plan → branch-apply
    // and branch-plan → released-apply alike.
    export_family: &str,
    state: &StateStore,
    summary: &RunSummary,
    kind: &str,
) -> Option<String> {
    use crate::manifest::ManifestStatus;
    use crate::pipeline::manifest_writer::{
        ManifestBuilder, WriteOutcome, write_manifest, write_manifest_keep_canonical_no_success,
    };

    // Catch any future runner that drifts summary aggregates away from
    // manifest_parts (the bug parallel_checkpoint had before e9b0796), or that
    // owns its loop and skips a per-runner facade (the runner-bypass class).
    //
    // The CHECK now runs in every build; only the REACTION differs. It was
    // `cfg!(debug_assertions)`-gated, and `[profile.release]` sets no
    // debug-assertions — so in the binary users actually run it was compiled out
    // entirely and a violation produced NO signal at all: not a panic, not a log
    // line. A run that silently skipped the drift gate and harvested no column
    // checksums looked exactly like a clean one.
    //
    // Debug/CI keeps the panic (a test must fail loudly). Release warns and
    // carries on: the run's data is already committed and aborting here would
    // destroy nothing but the user's afternoon — but the operator now learns
    // that this run's integrity records are incomplete, which is the whole point
    // of the guard.
    if let Err(e) = summary.check_post_run_invariants(plan.resume) {
        if cfg!(debug_assertions) {
            panic!(
                "summary↔manifest coherence violated at finalize_manifest \
                 for {} '{}': {}",
                kind, summary.export_name, e
            );
        }
        log::warn!(
            "{} '{}': run-integrity invariant violated — {}. The run's data is \
             committed, but its integrity records are incomplete; treat this \
             run's manifest as unverified and report it.",
            kind,
            summary.export_name,
            e
        );
    }

    let snapshot = match summary.journal.plan_snapshot() {
        Some(s) => s,
        None => {
            // Synthetic-failure summaries never recorded a PlanResolved event.
            // There is no committed work to manifest; just log and return.
            // A synthetic-failure summary never recorded a PlanResolved event:
            // there is no committed work to describe, so an absent manifest is
            // not a gap. `None`, not an error.
            log::debug!(
                "{} '{}': no plan snapshot, manifest skipped",
                kind,
                summary.export_name
            );
            return None;
        }
    };

    // A HEALTHY no-op describes nothing, so it must not describe the prefix.
    //
    // `"skipped"` is a real production status — `single.rs` sets it when a run
    // reads 0 rows under `skip_empty: true`, the ordinary outcome of an
    // incremental export with nothing new past the cursor. It used to fall
    // through the `_` arm below to `Interrupted`, and `write_manifest` then
    // OVERWROTE the canonical manifest with a zero-part interrupted document
    // while leaving the previous good run's `_SUCCESS` in place, now stale.
    //
    // Measured: run 1 exported 10 rows (manifest success, 1 part, _SUCCESS); run
    // 2 found nothing new and left `manifest.json` saying `interrupted, 0 parts,
    // 0 rows` over a prefix still holding the parquet. `rivet validate` then
    // refused the export — `[RIVET_VERIFY_SUCCESS_STALE]` plus the 10 delivered
    // rows reported as an `untracked object` — after a run that did nothing
    // wrong.
    //
    // The same early return the no-plan-snapshot case above takes, for the same
    // reason: there is no committed work to describe. The run itself is recorded
    // where run history belongs — `export_metrics`, `file_log`, `run_status` —
    // and the prefix keeps describing the last run that actually delivered.
    if skipped_run_wrote_nothing(summary) {
        log::debug!(
            "{} '{}': skipped run wrote no parts — leaving the prefix's manifest as it stands",
            kind,
            summary.export_name
        );
        return None;
    }

    let status = match summary.status.as_str() {
        "success" => ManifestStatus::Success,
        "failed" => ManifestStatus::Failed,
        _ => ManifestStatus::Interrupted,
    };

    // ADR-0012 M3: prefer the fingerprint captured at the sink (single +
    // chunked + checkpoint paths all populate it).  Fall back to the
    // state-store lookup only for resume scenarios where the live summary
    // never saw a schema.  The placeholder is a last-resort signal to the
    // reader that schema evidence was unavailable for this run.
    let schema_fingerprint = summary
        .schema_fingerprint
        .clone()
        .or_else(|| {
            state
                .get_stored_schema(&summary.export_name)
                .ok()
                .flatten()
                .map(|cols| crate::state::schema_fingerprint(&cols))
        })
        .unwrap_or_else(|| crate::manifest::SCHEMA_FINGERPRINT_UNAVAILABLE.to_string());

    let source_engine = match plan.source.source_type {
        crate::config::SourceType::Postgres => "postgres",
        crate::config::SourceType::Mysql => "mysql",
        crate::config::SourceType::Mssql => "mssql",
        crate::config::SourceType::Mongo => "mongo",
    };

    // The DECLARED table first — a name is a label, the config is the catalog.
    //
    // Deriving this by splitting `export_name` on '.' made the two legs of one
    // `initial: snapshot` CDC export disagree about their own identity: the drain
    // records the capture output's table, while the snapshot leg's synthesized
    // name (`orders__snapshot_orders`) has no dot, so it recorded nothing.
    // Measured on a real run into one prefix — drain
    // `{engine: postgres, table: "idsrc_orders"}`, leg
    // `{engine: postgres, table: null}` — which `identity_source` reads as
    // `postgres:idsrc_orders` and `postgres`, two sources under one export name,
    // and `ensure_single_source` refuses the flow the docs describe.
    //
    // The name split stays as the FALLBACK: an export declared with `query:`
    // has no `table:`, and a `schema.table` export name is still the only place
    // its schema appears.
    let (source_schema, source_table) = match plan.source_table.as_deref() {
        Some(t) => match t.split_once('.') {
            Some((s, tbl)) if !s.is_empty() && !tbl.is_empty() => {
                (Some(s.to_string()), Some(tbl.to_string()))
            }
            _ => (None, Some(t.to_string())),
        },
        None => match summary.export_name.split_once('.') {
            Some((s, t)) if !s.is_empty() && !t.is_empty() => {
                (Some(s.to_string()), Some(t.to_string()))
            }
            _ => (None, None),
        },
    };

    let started_at = summary
        .journal
        .entries
        .first()
        .map(|e| e.recorded_at)
        .unwrap_or_else(chrono::Utc::now);

    let mut builder = ManifestBuilder::new(
        snapshot,
        export_family,
        &summary.run_id,
        started_at,
        schema_fingerprint,
        source_engine,
        source_schema,
        source_table,
        destination_uri_for_manifest(&plan.destination),
    );
    // Record this unit's split window (if any) so --split --resume can reconstruct the
    // exact original partition from the prior run's manifests instead of re-sampling.
    builder.set_split_window(plan.split_window.clone());
    for part in &summary.manifest_parts {
        builder.record_part(
            part.part_id,
            part.path.clone(),
            part.rows,
            part.size_bytes,
            part.content_fingerprint.clone(),
            part.content_md5.clone(),
        );
    }
    if !summary.column_checksums.is_empty() {
        builder.set_column_checksums(
            summary.column_checksums.clone(),
            summary.checksum_key_column.clone(),
        );
    }
    if summary.cursor_column.is_some() || summary.cursor_high.is_some() {
        builder.set_cursor_range(
            summary.cursor_column.clone(),
            None, // cursor_type: follow-up (needs source-type plumbing)
            summary.cursor_low.clone(),
            summary.cursor_high.clone(),
            None, // set below, for every strategy — not just cursored ones
        );
    }
    // The source COUNT(*) `--reconcile` already ran. Recording it is what makes
    // `load::reconcile`'s source→file leg executable: without it that check,
    // `LoadIntegrity.source_rows` and `--allow-source-drift` are unreachable,
    // and the only "did the extract drop rows" evidence a loader ever sees is
    // rivet's own part-row arithmetic compared against itself.
    //
    // UNCONDITIONAL by strategy, deliberately: the count belongs to the run,
    // not to the cursor. It stays `None` unless the run probed the source, so
    // this adds no query — it stops discarding one already paid for.
    builder.set_source_row_count(summary.source_count);
    let manifest = builder.finalize(status);

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            let why = format!("could not create the destination for the manifest write: {e:#}");
            log::error!("{} '{}': {}", kind, summary.export_name, why);
            return Some(why);
        }
    };

    // #167: a `--split` unit shares its destination prefix with its N-1 siblings,
    // so it must NOT write the prefix-level `_SUCCESS` — that would mark the WHOLE
    // giant complete the moment the FIRST unit finishes (mis-skipping the rest on
    // resume, and tripping the M8 resume guard on a sibling's marker). The unit
    // writes its manifest + run-unique copy (its per-unit completion signal); the
    // pool writes `_SUCCESS` ONCE, after every unit succeeds.
    let write_result = if plan.is_split_unit {
        write_manifest_keep_canonical_no_success(&*dest, &manifest)
    } else {
        write_manifest(&*dest, &manifest)
    };
    match write_result {
        Ok(WriteOutcome::Written { success_marker }) => {
            log::info!(
                "{} '{}': manifest.json written ({} parts, {} rows){}",
                kind,
                summary.export_name,
                manifest.part_count,
                manifest.row_count,
                if success_marker { " + _SUCCESS" } else { "" },
            );
            None
        }
        // A streaming destination (stdout) has no prefix to describe, so there is
        // no manifest to miss — the only legitimately absent one.
        Ok(WriteOutcome::SkippedStreaming) => {
            log::info!(
                "{} '{}': manifest skipped (streaming destination)",
                kind,
                summary.export_name,
            );
            None
        }
        Err(e) => {
            let why = format!(
                "the manifest write FAILED, so the prefix has {} durable part(s) that no manifest \
                 names — a manifest-authoritative `rivet load` will not read them: {e:#}",
                manifest.part_count
            );
            log::error!("{} '{}': {}", kind, summary.export_name, why);
            Some(why)
        }
    }
}

/// Run the manifest-aware `--validate` pass against the destination prefix
/// (ADR-0012 M5/M6, ADR-0013).  Populates `summary.manifest_verification`;
/// failures are logged and non-fatal — the existing per-file row check has
/// already set `summary.validated`, and the operator gets a richer report
/// regardless of whether destination I/O succeeded here.
///
/// Streaming destinations (stdout) have no prefix to verify; skipped silently
/// since `finalize_manifest` has already logged its own "skipped streaming"
/// note for that case.
/// Does the manifest-aware pass overturn a per-file "validated" verdict?
///
/// Three conditions, and each one is load-bearing:
///
///   * `!passed` — only a FATAL verdict downgrades. An advisory failure (an
///     untracked surplus part) is not a reason to fail a run.
///   * `manifest_found` — a legacy run (M6) has no manifest to judge by, so it
///     keeps the row-count verdict it earned rather than being failed for the
///     absence of a file its version never wrote.
///   * `current == Some(true)` — there is nothing to downgrade from otherwise,
///     and overwriting `None` would claim a verdict that was never reached.
///
/// Extracted because it could not be tested in place. Three mutations survived
/// the suite — `&&`→`||` twice and `delete !` — each of which makes the downgrade
/// stop happening or start happening to runs that earned their pass. The failure
/// is quiet and durable: `rivet metrics` would say `validated=pass` for a run
/// whose own report says the manifest pass failed.
pub(super) fn should_downgrade_validated(
    v: &crate::pipeline::validate_manifest::ManifestVerification,
    current: Option<bool>,
) -> bool {
    !v.passed && v.manifest_found && current == Some(true)
}

pub(super) fn finalize_validate_manifest(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    kind: &str,
) {
    use crate::pipeline::validate_manifest::{ValidateDepth, verify_at_destination};

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::warn!(
                "{} '{}': could not create destination for --validate manifest pass (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
            return;
        }
    };
    if !dest.capabilities().commit_protocol.leaves_objects_at_rest() {
        log::debug!(
            "{} '{}': streaming destination — skipping manifest-aware --validate",
            kind,
            summary.export_name
        );
        return;
    }

    // Run finalize always does the full manifest pass (the graded `--depth`
    // levels are a `rivet validate` operator affordance, not a run-time knob);
    // this preserves the pre-graded end-of-run behaviour exactly.
    match verify_at_destination(&*dest, "", ValidateDepth::Full) {
        Ok(mut v) => {
            // Apply the export's `verify` policy: `content` turns size-only
            // parts into a fatal failure (review D).
            v.enforce_content_policy(plan.verify.requires_content());
            // Compose the file-row check (already on summary.validated) with
            // the manifest-aware verdict.  Downgrade on a *fatal* verdict
            // (`!passed`) — advisory failures (untracked surplus) don't fail
            // the run; legacy runs (M6) keep their row-count verdict.
            if should_downgrade_validated(&v, summary.validated) {
                summary.validated = Some(false);
            }
            log::info!(
                "{} '{}': --validate manifest pass: {} parts verified, {} failed{}{}",
                kind,
                summary.export_name,
                v.parts_verified,
                v.parts_failed,
                if v.success_marker_consistent {
                    " (_SUCCESS consistent)"
                } else if v.manifest_found {
                    ""
                } else {
                    " (legacy_run: no manifest)"
                },
                if v.has_failures() {
                    format!(" — {} issue(s)", v.failures.len())
                } else {
                    String::new()
                },
            );
            summary.manifest_verification = Some(v);
        }
        Err(e) => {
            log::warn!(
                "{} '{}': --validate manifest pass failed (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
        }
    }
}

/// ADR-0012 M8 — refuse to start a `--resume` run against a destination
/// prefix whose `_SUCCESS` marker is already present, unless the operator
/// passed `--force`.  The marker is the unambiguous signal that the prefix
/// already holds a verified dataset; quietly overwriting it is the kind
/// of mistake that costs a re-extraction window's worth of source pressure.
///
/// Streaming destinations (stdout) have no prefix to gate on; permitted.
/// I/O failures probing `_SUCCESS` (e.g. permission denied on the bucket
/// we're about to write to) bubble up as `Err` so the operator sees the
/// real problem before the run starts spending source query time.
/// #167: write the ONE prefix-level `_SUCCESS` that a giant's `--split` units
/// deliberately suppressed — the pool calls this after EVERY unit of the giant
/// has succeeded. It fingerprints the canonical `manifest.json` already in the
/// prefix (the last unit's, last-writer-wins) so `validate`'s marker-vs-manifest
/// consistency check holds. `dest_config` must be the FAMILY-expanded prefix the
/// units wrote to. Streaming destinations have no prefix (no-op).
pub(crate) fn write_split_success_marker(
    dest_config: &crate::config::DestinationConfig,
) -> Result<()> {
    use crate::manifest::{MANIFEST_FILENAME, SUCCESS_FILENAME, success_marker_body};
    let dest = crate::destination::create_destination(dest_config)?;
    if !dest.capabilities().commit_protocol.leaves_objects_at_rest() {
        return Ok(());
    }
    let manifest_bytes = dest.read(MANIFEST_FILENAME)?;
    let body = success_marker_body(&manifest_bytes);
    let tmp = tempfile::NamedTempFile::new()?;
    std::fs::write(tmp.path(), body.as_bytes())?;
    dest.write(tmp.path(), SUCCESS_FILENAME)?;
    Ok(())
}

pub(super) fn check_success_gate_for_resume(plan: &ResolvedRunPlan) -> Result<()> {
    use crate::manifest::SUCCESS_FILENAME;

    // #167: a `--split` unit shares its prefix with siblings and never writes the
    // prefix `_SUCCESS` itself (the pool does, once all units finish). A `_SUCCESS`
    // present during a unit's resume was written by the POOL for a PRIOR fully-
    // complete split — but the pool already skips a completed giant's units before
    // they reach here, so a unit that DOES reach resume is part of an incomplete
    // giant. Gating it on the (sibling-or-pool) marker would refuse legitimate
    // per-unit resume, so the split unit is exempt — its own run-unique manifest
    // copy is its completion signal, and the pool's per-unit skip is the real gate.
    if plan.is_split_unit {
        return Ok(());
    }

    let dest = crate::destination::create_destination(&plan.destination)?;
    if !dest.capabilities().commit_protocol.leaves_objects_at_rest() {
        log::debug!(
            "resume: streaming destination for export '{}' has no prefix; gate skipped",
            plan.export_name
        );
        return Ok(());
    }
    match dest.head(SUCCESS_FILENAME)? {
        Some(_) => anyhow::bail!(
            "export '{}': --resume refused — destination prefix already has _SUCCESS \
             from a prior completed run.  Re-running would overwrite a verified dataset. \
             Pass --force to override, or use a different destination prefix.",
            plan.export_name
        ),
        None => Ok(()),
    }
}

/// Footgun guard for a *fresh* (non-`--resume`) run into a destination prefix
/// that already carries a completed export.
///
/// The audit (findings #5/#19/#30) showed that re-running `rivet run` into the
/// same stable local prefix without `--resume` writes a brand-new set of
/// timestamp-/nonce-named part files *alongside* the old ones — nothing is
/// overwritten, and `manifest.json` is rewritten to describe only the latest
/// run.  A glob reader over the prefix (`read_parquet('<prefix>/*.parquet')`)
/// then over-counts: a chunked re-run doubles the row total while the manifest
/// silently claims the smaller count.
///
/// Unlike [`check_success_gate_for_resume`] this is **non-destructive and
/// non-fatal**: we never auto-delete the operator's prior data, and we never
/// change the run's exit code.  But the drift must not be *silent* (CLAUDE.md:
/// degraded/lossy paths must be loud), so when the prefix already holds a
/// completed run we emit a prominent `WARN` naming the prefix and the exact
/// risk, and point at the safe recoveries (`--resume`, or clear the prefix).
///
/// Streaming destinations (stdout) have no prefix to accumulate into; skipped.
/// I/O failures probing the marker are swallowed to a debug log — this is a
/// safety hint emitted *before* extraction, not a correctness gate, so a
/// transient stat failure must never block an otherwise-valid run (the resume
/// gate, which *does* gate, surfaces such errors instead).
/// Can this destination even HAVE a prior run's output sitting under the prefix?
///
/// Only a destination that commits named objects can — a `Streaming` one (stdout)
/// has no prefix to collide on, so probing it is meaningless. Everything else
/// (`Atomic` local, `FinalizeOnClose` cloud) is exactly where a second export
/// writing under one prefix silently overwrites the first, which is the whole
/// reason the warning exists.
///
/// Extracted because the comparison was untestable in place and `==`→`!=`
/// survived the suite. That mutation inverts the guard: every real destination
/// returns early — the rerun warning goes SILENT on local and cloud both — while
/// stdout gets probed instead. Nothing fails, nothing is logged, and the operator
/// who pointed two exports at one prefix is told nothing.
pub(super) fn prefix_guard_applies(commit: crate::destination::WriteCommitProtocol) -> bool {
    commit.leaves_objects_at_rest()
}

pub(super) fn warn_if_prefix_has_completed_run(plan: &ResolvedRunPlan) {
    use crate::manifest::{MANIFEST_FILENAME, SUCCESS_FILENAME};

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::debug!(
                "rerun-guard: could not create destination for export '{}' (skipping pre-run check): {:#}",
                plan.export_name,
                e
            );
            return;
        }
    };
    if !prefix_guard_applies(dest.capabilities().commit_protocol) {
        return;
    }

    // `_SUCCESS` is the unambiguous "a prior run completed cleanly here" signal;
    // `manifest.json` catches a prior run that committed parts even if `_SUCCESS`
    // is absent.  Probe `_SUCCESS` first so the warning is precise about a
    // *completed* run when it can be.
    let marker = match dest.head(SUCCESS_FILENAME) {
        Ok(Some(_)) => Some(SUCCESS_FILENAME),
        Ok(None) => match dest.head(MANIFEST_FILENAME) {
            Ok(Some(_)) => Some(MANIFEST_FILENAME),
            Ok(None) => None,
            Err(e) => {
                log::debug!(
                    "rerun-guard: stat {} failed for export '{}' (skipping pre-run check): {:#}",
                    MANIFEST_FILENAME,
                    plan.export_name,
                    e
                );
                return;
            }
        },
        Err(e) => {
            log::debug!(
                "rerun-guard: stat {} failed for export '{}' (skipping pre-run check): {:#}",
                SUCCESS_FILENAME,
                plan.export_name,
                e
            );
            return;
        }
    };

    if let Some(marker) = marker {
        log::warn!(
            "export '{}': {}",
            plan.export_name,
            rerun_warning_message(&destination_uri_for_manifest(&plan.destination), marker),
        );
    }
}

/// Whether this destination already holds a completed export (`_SUCCESS`).
/// `rivet apply --resume` uses it to skip exports a prior run finished, so a
/// re-run after a partial failure does not redo work already done. Reuses the
/// same probe as [`warn_if_prefix_has_completed_run`]; a streaming destination
/// (stdout) or a probe error counts as "not complete" (re-run it).
pub(crate) fn destination_has_success(dest: &crate::config::DestinationConfig) -> bool {
    use crate::manifest::SUCCESS_FILENAME;
    let Ok(d) = crate::destination::create_destination(dest) else {
        return false;
    };
    if !d.capabilities().commit_protocol.leaves_objects_at_rest() {
        return false;
    }
    matches!(d.head(SUCCESS_FILENAME), Ok(Some(_)))
}

/// The operator-facing body of the rerun-accumulation warning.
///
/// Split out so a regression test can pin the exact wording — the live audit
/// (`tests/audit_rerun.rs`) only accepts this guard as "loud enough" when the
/// message carries phrases like `already has`, `prior completed run`,
/// `_SUCCESS` / `would overwrite`, or `orphan`.  Weakening the text below those
/// markers would silently fail the audit, so the test below guards it.
fn rerun_warning_message(uri: &str, marker: &str) -> String {
    format!(
        "destination prefix '{uri}' already has a prior completed run ({marker} present) — \
         re-running WITHOUT --resume appends fresh timestamp-named parts alongside the old ones \
         (nothing is overwritten) and rewrites manifest.json to describe only this run, so a glob \
         reader over the prefix will double-count / orphan the old parts. \
         Use --resume to continue the prior run, or clear the prefix first."
    )
}

/// Project the `run_status` ledger's `running` row into the bucket as a
/// schema-less MARKER manifest at run START. Written as the run-unique copy only
/// (`manifest-<run_id>.json`) — NOT the canonical `manifest.json`, so a prior
/// run's `_SUCCESS`/canonical pair never desyncs (the `SuccessMarkerStale` trap).
/// A cross-boundary reader (Airflow, a foreign-host `rivet load`) then sees a
/// LIVE run on the prefix via `fetch_manifests_keyed` and does NOT GC its
/// in-flight parts. Cloud-only — a local export has no cross-host reader, and the
/// state-store ledger already covers the co-located case. Best-effort: a marker
/// write failure must never fail the run (gc still has the ledger). The terminal
/// manifest at finalize OVERWRITES this same run-unique file.
pub(super) fn write_running_manifest(
    plan: &ResolvedRunPlan,
    // The export FAMILY, passed by the caller — NOT `plan.export_name`. The two
    // writers for one run must agree: the terminal manifest records
    // `export.family()` (job.rs), so a marker recording the NAME diverges for the
    // one export whose family differs from its name — the CDC snapshot leg,
    // named `{parent}__snapshot_{table}`. A hard-killed leg then leaves a marker
    // claiming a family no other manifest carries, `ensure_single_export` bails
    // ("manifests from 2 distinct exports"), and the marker is un-supersedable,
    // so the sweep never clears it: the prefix is bricked for loading.
    //
    // Recording the WRONG family is strictly worse than recording none — an
    // empty field falls back to the substring fold, which handles the leg
    // correctly (`resolved_family`, load/reconcile.rs).
    export_family: &str,
    run_id: &str,
    started_at: &str,
) {
    use crate::config::{DestinationType, SourceType};
    use crate::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestSource, ManifestStatus, RunManifest,
    };
    use crate::pipeline::manifest_writer::write_manifest_without_success_marker;

    let kind = match plan.destination.destination_type {
        DestinationType::Gcs => "gcs",
        DestinationType::S3 => "s3",
        DestinationType::Azure => "azure",
        // No cross-boundary bucket reader for these: the ledger covers the
        // co-located case, so skip the marker.
        DestinationType::Local | DestinationType::Stdout => return,
    };
    let engine = match plan.source.source_type {
        SourceType::Postgres => "postgres",
        SourceType::Mysql => "mysql",
        SourceType::Mssql => "mssql",
        SourceType::Mongo => "mongo",
    };
    let manifest = RunManifest {
        row_hash: None,
        split_window: None, // the running marker is overwritten by the terminal manifest
        manifest_version: MANIFEST_VERSION,
        run_id: run_id.to_string(),
        export_family: export_family.to_string(),
        export_name: plan.export_name.clone(),
        mode: "batch".to_string(),
        started_at: started_at.to_string(),
        finished_at: String::new(),
        status: ManifestStatus::Running,
        source: ManifestSource {
            engine: engine.to_string(),
            schema: None,
            table: None,
            extraction: None,
        },
        destination: ManifestDestination {
            kind: kind.to_string(),
            uri: destination_uri_for_manifest(&plan.destination),
        },
        format: String::new(),
        compression: String::new(),
        schema_fingerprint: String::new(),
        row_count: 0,
        part_count: 0,
        parts: Vec::new(),
        checksum_render: None,
        column_checksums: None,
        checksum_key_column: None,
    };
    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::debug!(
                "export '{}': running-manifest destination unavailable (not fatal): {e:#}",
                plan.export_name
            );
            return;
        }
    };
    if let Err(e) = write_manifest_without_success_marker(&*dest, &manifest) {
        log::debug!(
            "export '{}': running-manifest write failed (not fatal; gc uses the ledger): {e:#}",
            plan.export_name
        );
    }
}

/// Best-effort textual URI for the manifest's `destination.uri` field.
///
/// The manifest is a record of where data was written, so the URI must
/// reflect what an operator would type to find the prefix again.
pub(crate) fn destination_uri_for_manifest(cfg: &DestinationConfig) -> String {
    use crate::config::DestinationType;
    match cfg.destination_type {
        DestinationType::Local => cfg
            .path
            .clone()
            .or_else(|| cfg.prefix.clone())
            .map(|p| format!("file://{p}"))
            .unwrap_or_else(|| "file://.".to_string()),
        DestinationType::S3 => {
            let bucket = cfg.bucket.as_deref().unwrap_or("");
            let prefix = cfg.prefix.as_deref().unwrap_or("");
            if prefix.is_empty() {
                format!("s3://{bucket}/")
            } else {
                format!("s3://{bucket}/{prefix}")
            }
        }
        DestinationType::Gcs => {
            let bucket = cfg.bucket.as_deref().unwrap_or("");
            let prefix = cfg.prefix.as_deref().unwrap_or("");
            if prefix.is_empty() {
                format!("gs://{bucket}/")
            } else {
                format!("gs://{bucket}/{prefix}")
            }
        }
        DestinationType::Azure => {
            // `az://<container>/<prefix>` — same Hadoop/HDFS-style scheme that
            // azcopy and most Azure-native tools recognise.  Manifest URI is
            // operator-facing, not used for opendal addressing.
            let container = cfg.bucket.as_deref().unwrap_or("");
            let prefix = cfg.prefix.as_deref().unwrap_or("");
            if prefix.is_empty() {
                format!("az://{container}/")
            } else {
                format!("az://{container}/{prefix}")
            }
        }
        DestinationType::Stdout => "stdout".to_string(),
    }
}

#[cfg(test)]
mod tests {

    /// The rerun guard must apply to every destination that can actually hold a
    /// previous run's parts — and the protocols come from the REAL destinations,
    /// not a hand-built capability struct, because the bug this guards is a
    /// mismatch between the guard and what `create_destination` really reports.
    ///
    /// RED against `==`→`!=` on the guard's comparison, which silences the
    /// overwrite warning on local AND cloud at once while probing stdout instead.
    #[test]
    fn the_rerun_guard_applies_to_every_destination_that_can_hold_a_prior_run() {
        use crate::config::{DestinationConfig, DestinationType};

        let protocol_of = |t: DestinationType, path: Option<String>| {
            let cfg = DestinationConfig {
                destination_type: t,
                path,
                ..Default::default()
            };
            crate::destination::create_destination(&cfg)
                .expect("destination builds")
                .capabilities()
                .commit_protocol
        };

        let tmp = tempfile::tempdir().expect("tempdir");
        let local = protocol_of(
            DestinationType::Local,
            Some(tmp.path().to_string_lossy().into_owned()),
        );
        assert!(
            prefix_guard_applies(local),
            "a local prefix is precisely where a second export overwrites the first"
        );

        let stdout = protocol_of(DestinationType::Stdout, None);
        assert!(
            !prefix_guard_applies(stdout),
            "stdout has no prefix to collide on — probing it would stat nothing"
        );

        assert_ne!(
            local, stdout,
            "if both destinations reported one protocol the assertions above would be vacuous"
        );
    }

    /// The manifest pass overturns a per-file pass ONLY when all three
    /// conditions hold — and each row below kills a mutant that survived.
    ///
    /// The quiet failure this guards: without the downgrade, `rivet metrics`
    /// records `validated=pass` for a run whose own report says the manifest
    /// pass failed, and it stays that way. `&&`→`||` and `delete !` each break
    /// it in a different direction, so a single happy-path assertion would let
    /// two of the three through.
    #[test]
    fn the_manifest_pass_downgrades_only_a_fatal_verdict_on_a_run_that_had_passed() {
        use crate::pipeline::validate_manifest::ManifestVerification;
        let v = |passed: bool, manifest_found: bool| ManifestVerification {
            passed,
            manifest_found,
            legacy_run: !manifest_found,
            parts_verified: 0,
            parts_md5_verified: 0,
            parts_failed: 0,
            success_marker_consistent: true,
            manifest_self_consistent: true,
            failures: Vec::new(),
            depth_level: "full".into(),
        };

        assert!(
            should_downgrade_validated(&v(false, true), Some(true)),
            "a FATAL manifest verdict on a run that passed its file check is the whole point"
        );
        assert!(
            !should_downgrade_validated(&v(true, true), Some(true)),
            "a passing manifest pass must not fail a run — `delete !` makes it do exactly that"
        );
        assert!(
            !should_downgrade_validated(&v(false, false), Some(true)),
            "a LEGACY run has no manifest to be judged by and keeps the verdict it earned"
        );
        assert!(
            !should_downgrade_validated(&v(false, true), None),
            "there is nothing to downgrade from — writing Some(false) would claim a verdict \
             the run never reached"
        );
        assert!(
            !should_downgrade_validated(&v(false, true), Some(false)),
            "already failed; the downgrade is not a second opinion"
        );
    }

    /// The resume advisory fires exactly when there is something to resume.
    ///
    /// RED-proven against the four mutants that survived the suite on its
    /// condition: `==`→`!=` (silent on failures, loud on successes), `>`→`<` /
    /// `==` / `>=` (silent with parts, loud with none). Each row below kills at
    /// least one of them, which is why the table has all four corners rather
    /// than the one case a reader would think to write.
    #[test]
    fn the_resume_hint_fires_only_when_a_failed_run_left_durable_parts() {
        let case = |status: &str, files: usize| {
            let s = crate::pipeline::summary::RunSummary {
                status: status.into(),
                files_committed: files,
                ..Default::default()
            };
            should_offer_resume(&s)
        };
        assert!(
            case("failed", 3),
            "a failed run WITH committed parts is the one case worth a resume line — the data \
             is on the prefix and a fresh run would not pick it up"
        );
        assert!(
            !case("failed", 0),
            "a failure that committed nothing has nothing to resume; the line would send the \
             operator after data that is not there"
        );
        assert!(!case("success", 3), "a successful run is not resumed");
        assert!(!case("success", 0), "nor an empty successful one");
    }

    use super::*;
    use crate::config::DestinationType;

    fn cfg_local(path: Option<&str>, prefix: Option<&str>) -> DestinationConfig {
        DestinationConfig {
            destination_type: DestinationType::Local,
            prefix: prefix.map(str::to_string),
            path: path.map(str::to_string),
            ..Default::default()
        }
    }

    // ── mutation-pilot gap closures ──────────────────────────────────────────
    // finalize_manifest (18 missed) + the success gates (4) had NO driving
    // test: stubbing finalize_manifest to () — i.e. never writing the manifest
    // at end of run — survived the whole lib suite.

    /// ADR-0028: the seam applies the ledger — Form-B checksums land on the
    /// summary (keyed), and the ledger is TAKEN (emptied) so a hypothetical
    /// second application cannot double-record. State-free half only (the drift
    /// gate + shape warn need a StateStore; their end-to-end proof is the live
    /// drift suite + the RED-proven telltale backstop). RED against a seam that
    /// reads the ledger without applying (checksums stay empty) and against one
    /// that clones instead of takes (second call re-harvests).
    #[test]
    fn finalize_export_harvests_the_ledger_and_takes_it() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let mut summary = crate::pipeline::summary::RunSummary::default();
        summary
            .ledger
            .merge_checksums(&[("v".to_string(), 9u64)].into());
        summary.ledger.note_key_column(Some("id".into()));

        finalize_export(&plan, None, &mut summary).expect("state-free seam must succeed");

        assert_eq!(
            summary
                .column_checksums
                .iter()
                .map(|c| (c.name.as_str(), c.checksum.as_str()))
                .collect::<Vec<_>>(),
            vec![("v", "9")],
            "the seam must harvest the ledger's Form-B checksums into the summary"
        );
        assert_eq!(summary.checksum_key_column.as_deref(), Some("id"));
        assert!(
            summary.ledger.column_checksums.is_empty(),
            "the seam must TAKE the ledger — a second application must find nothing"
        );

        // Second call: no ledger left, the harvested record must survive untouched
        // (harvest_column_checksums early-returns on an empty accumulator).
        finalize_export(&plan, None, &mut summary).expect("idempotent on an empty ledger");
        assert_eq!(summary.column_checksums.len(), 1);
    }

    /// ADR-0028: the seam's shape-drift arm — the ONE guard deciding whether the
    /// advisory warn runs. Positive: factor set + shape fed + a stored smaller
    /// max → a `shape_drift:` Warning journal event. Negative: factor 0.0
    /// (disabled — the config default is opt-in) with the SAME grown shape must
    /// warn nothing. Together they pin every guard mutant: `>` → `<`/`==`
    /// (positive stops warning), `>` → `>=` and `&&` → `||` (negative starts
    /// warning while disabled), `delete !` (positive skips a non-empty ledger).
    #[test]
    fn finalize_export_shape_warn_fires_only_when_enabled_and_fed() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::state::StateStore::open_in_memory().unwrap();

        let shape_of = |bytes: u64| -> std::collections::HashMap<String, u64> {
            [("payload".to_string(), bytes)].into()
        };
        let warned = |summary: &crate::pipeline::summary::RunSummary| {
            summary.journal.warnings().iter().any(|e| {
                matches!(&e.event, crate::journal::RunEvent::Warning { context, .. }
                    if context.starts_with("shape_drift:"))
            })
        };

        // Seed the stored high-water mark (first run: silently accepted).
        state
            .detect_shape_drift("public.orders", &shape_of(10), 2.0)
            .unwrap();

        // Positive: factor 2.0, 10 → 100 bytes (10×) must warn via the seam.
        let mut plan = fin_plan(dir.path());
        plan.shape_drift_warn_factor = 2.0;
        let mut summary = crate::pipeline::summary::RunSummary::default();
        summary.ledger.merge_shape(&shape_of(100));
        finalize_export(&plan, Some(&state), &mut summary).unwrap();
        assert!(
            warned(&summary),
            "a 10× column growth with warn_factor 2.0 must journal a shape_drift warning"
        );

        // Negative: factor 0.0 (disabled) — the same growth must warn NOTHING,
        // and the guard must not even consult the state (a `>=`/`||` mutant
        // enters the arm and warns).
        let mut plan = fin_plan(dir.path());
        plan.shape_drift_warn_factor = 0.0;
        let mut summary = crate::pipeline::summary::RunSummary::default();
        summary.ledger.merge_shape(&shape_of(100_000));
        finalize_export(&plan, Some(&state), &mut summary).unwrap();
        assert!(
            !warned(&summary),
            "shape_drift_warn_factor 0.0 means DISABLED — the seam must not warn"
        );
    }

    fn fin_plan(dest: &std::path::Path) -> crate::plan::ResolvedRunPlan {
        use crate::config::{SourceConfig, SourceType};
        crate::plan::ResolvedRunPlan {
            split_window: None,
            bytes_read: Default::default(),
            export_name: "public.orders".into(),
            source_table: None,
            base_query: "SELECT 1".into(),
            is_split_unit: false,
            strategy: crate::plan::ExtractionStrategy::Snapshot,
            format: crate::config::FormatType::Parquet,
            compression: crate::config::CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: Default::default(),
            destination: cfg_local(Some(&dest.to_string_lossy()), None),
            quality: None,
            tuning: crate::tuning::SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody@127.0.0.1:9999/nonexistent".into()),
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

    /// The four corners of "is there anything to describe".
    ///
    /// Both halves of the condition were unguarded until the mutation gate said
    /// so — `==`→`!=` and `&&`→`||` each survived the whole suite. The table
    /// below fails against either: invert the equality and the SUCCESS row
    /// stops writing a manifest; widen the `&&` and the skipped-WITH-parts row
    /// throws away a manifest that describes real committed data.
    #[test]
    fn only_a_skipped_run_with_no_parts_has_nothing_to_describe() {
        let with_part = |status: &str| {
            let mut s = RunSummary::stub_for_testing("r", String::from("e"));
            s.status = status.into();
            s.manifest_parts.push(crate::manifest::ManifestPart {
                part_id: 1,
                path: "part-000001.parquet".into(),
                rows: 5,
                size_bytes: 7,
                content_fingerprint: "xxh3:1".into(),
                content_md5: String::new(),
                status: crate::manifest::PartStatus::Committed,
            });
            s
        };
        let bare = |status: &str| {
            let mut s = RunSummary::stub_for_testing("r", String::from("e"));
            s.status = status.into();
            s
        };

        assert!(
            skipped_run_wrote_nothing(&bare("skipped")),
            "a skipped run with no parts is the one case that writes no manifest"
        );
        assert!(
            !skipped_run_wrote_nothing(&with_part("skipped")),
            "a skipped run that DID commit parts must still describe them"
        );
        assert!(
            !skipped_run_wrote_nothing(&bare("success")),
            "a successful run always writes its manifest, parts or not"
        );
        assert!(
            !skipped_run_wrote_nothing(&with_part("failed")),
            "a failed run's committed parts must reach the manifest too"
        );
    }

    /// A summary that carries everything finalize_manifest is supposed to
    /// surface into the manifest: a plan snapshot, one committed part, Form B
    /// checksums, and a cursor range.
    fn fin_summary(plan: &crate::plan::ResolvedRunPlan, status: &str) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("finrun", plan.export_name.clone());
        s.status = status.into();
        s.schema_fingerprint = Some("xxh3:00000000feedface".into());
        s.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 5,
            size_bytes: 7,
            content_fingerprint: "xxh3:1".into(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });
        s.column_checksums = vec![crate::manifest::ColumnChecksum {
            name: "id".into(),
            checksum: "42".into(),
        }];
        s.checksum_key_column = Some("id".into());
        s.cursor_column = Some("updated_at".into());
        s.cursor_low = Some("2026-01-01".into());
        s.cursor_high = Some("2026-02-01".into());
        s.journal
            .record(crate::journal::RunEvent::PlanResolved(Box::new(
                crate::journal::PlanSnapshot {
                    row_hash: None,
                    export_name: plan.export_name.clone(),
                    base_query: plan.base_query.clone(),
                    strategy: "snapshot".into(),
                    format: "parquet".into(),
                    compression: "none".into(),
                    destination_type: "local".into(),
                    tuning_profile: "balanced".into(),
                    batch_size: 1000,
                    validate: false,
                    reconcile: false,
                    resume: false,
                    chunk_key: None,
                    resumable: false,
                },
            )));
        s
    }

    fn read_manifest(dir: &std::path::Path) -> crate::manifest::RunManifest {
        serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap()
    }

    #[test]
    fn finalize_manifest_success_writes_full_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let summary = fin_summary(&plan, "success");

        finalize_manifest(&plan, "e", &state, &summary, "export");

        let m = read_manifest(dir.path());
        assert_eq!(m.status, crate::manifest::ManifestStatus::Success);
        assert_eq!(m.run_id, "finrun");
        assert_eq!(m.parts.len(), 1);
        assert_eq!(m.row_count, 5);
        assert_eq!(m.schema_fingerprint, "xxh3:00000000feedface");
        assert_eq!(
            m.column_checksums.as_ref().map(|c| c.len()),
            Some(1),
            "Form B checksums must land in the manifest"
        );
        assert_eq!(m.checksum_key_column.as_deref(), Some("id"));
        let ex = m.source.extraction.as_ref().expect("extraction section");
        assert_eq!(ex.cursor_low.as_deref(), Some("2026-01-01"));
        assert_eq!(ex.cursor_high.as_deref(), Some("2026-02-01"));
        assert_eq!(m.source.engine, "postgres");
        assert_eq!(m.source.schema.as_deref(), Some("public"));
        assert_eq!(m.source.table.as_deref(), Some("orders"));
        assert!(
            dir.path().join("_SUCCESS").exists(),
            "success run gets _SUCCESS"
        );
        assert!(
            dir.path().join("manifest-finrun.json").exists(),
            "run-unique manifest copy beside the canonical"
        );
    }

    #[test]
    fn finalize_manifest_failed_run_records_failed_and_no_success_marker() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        let state = crate::state::StateStore::open_in_memory().unwrap();
        let summary = fin_summary(&plan, "failed");

        finalize_manifest(&plan, "e", &state, &summary, "export");

        let m = read_manifest(dir.path());
        assert_eq!(m.status, crate::manifest::ManifestStatus::Failed);
        assert!(
            !dir.path().join("_SUCCESS").exists(),
            "a failed run must never leave a _SUCCESS marker"
        );
    }

    #[test]
    fn finalize_manifest_splits_schema_table_only_on_a_real_dot() {
        // "orders" (no dot) and "public." (empty table) must both yield
        // None/None — a `guard -> true` or `&& -> ||` mutant fabricates
        // Some("")/Some(...) fields from free-form export names.
        for name in ["orders", "public.", ".orders"] {
            let dir = tempfile::tempdir().unwrap();
            let plan = fin_plan(dir.path());
            let state = crate::state::StateStore::open_in_memory().unwrap();
            let mut summary = fin_summary(&plan, "success");
            summary.export_name = name.into();

            finalize_manifest(&plan, "e", &state, &summary, "export");

            let m = read_manifest(dir.path());
            assert_eq!(m.source.schema, None, "export_name {name:?}");
            assert_eq!(m.source.table, None, "export_name {name:?}");
        }
    }

    #[test]
    fn success_gate_refuses_resume_over_a_completed_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let plan = fin_plan(dir.path());
        // Empty prefix: resume proceeds.
        check_success_gate_for_resume(&plan).expect("no _SUCCESS -> gate passes");
        // Completed prefix: refuse loudly.
        std::fs::write(dir.path().join("_SUCCESS"), b"xxh3:0\n").unwrap();
        let err = check_success_gate_for_resume(&plan)
            .expect_err("_SUCCESS present -> resume must be refused");
        assert!(
            err.to_string().contains("refused"),
            "the refusal must be operator-actionable, got: {err:#}"
        );
    }

    #[test]
    fn destination_has_success_probes_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_local(Some(&dir.path().to_string_lossy()), None);
        assert!(!destination_has_success(&cfg), "no marker -> false");
        std::fs::write(dir.path().join("_SUCCESS"), b"xxh3:0\n").unwrap();
        assert!(destination_has_success(&cfg), "marker present -> true");
        // An unopenable destination counts as "not complete" (re-run it).
        let bad = cfg_local(Some("/nonexistent/definitely/missing"), None);
        assert!(!destination_has_success(&bad));
    }

    fn cfg_s3(bucket: &str, prefix: Option<&str>) -> DestinationConfig {
        DestinationConfig {
            destination_type: DestinationType::S3,
            bucket: Some(bucket.into()),
            prefix: prefix.map(str::to_string),
            ..Default::default()
        }
    }

    fn cfg_gcs(bucket: &str, prefix: Option<&str>) -> DestinationConfig {
        let mut c = cfg_s3(bucket, prefix);
        c.destination_type = DestinationType::Gcs;
        c
    }

    fn cfg_azure(container: &str, prefix: Option<&str>) -> DestinationConfig {
        let mut c = cfg_s3(container, prefix);
        c.destination_type = DestinationType::Azure;
        c
    }

    #[test]
    fn destination_uri_local_uses_path() {
        assert_eq!(
            destination_uri_for_manifest(&cfg_local(Some("/tmp/out"), None)),
            "file:///tmp/out"
        );
    }

    #[test]
    fn destination_uri_local_falls_back_to_prefix_then_dot() {
        assert_eq!(
            destination_uri_for_manifest(&cfg_local(None, Some("/var/data"))),
            "file:///var/data"
        );
        assert_eq!(
            destination_uri_for_manifest(&cfg_local(None, None)),
            "file://."
        );
    }

    #[test]
    fn destination_uri_s3_with_and_without_prefix() {
        assert_eq!(destination_uri_for_manifest(&cfg_s3("b", None)), "s3://b/");
        assert_eq!(
            destination_uri_for_manifest(&cfg_s3("b", Some("k/"))),
            "s3://b/k/"
        );
    }

    #[test]
    fn destination_uri_gcs_with_and_without_prefix() {
        assert_eq!(destination_uri_for_manifest(&cfg_gcs("b", None)), "gs://b/");
        assert_eq!(
            destination_uri_for_manifest(&cfg_gcs("b", Some("k/"))),
            "gs://b/k/"
        );
    }

    #[test]
    fn destination_uri_azure_with_and_without_prefix() {
        assert_eq!(
            destination_uri_for_manifest(&cfg_azure("c", None)),
            "az://c/"
        );
        assert_eq!(
            destination_uri_for_manifest(&cfg_azure("c", Some("runs/2026/"))),
            "az://c/runs/2026/"
        );
    }

    #[test]
    fn destination_uri_stdout_is_stable() {
        let mut c = cfg_local(None, None);
        c.destination_type = DestinationType::Stdout;
        assert_eq!(destination_uri_for_manifest(&c), "stdout");
    }

    // ── rerun-accumulation guard wording (audit findings #5/#19/#30) ─────────
    //
    // `warn_if_prefix_has_completed_run` only counts as the "loud" fix shape in
    // `tests/audit_rerun.rs` when its message matches that test's deliberately
    // narrow `warned_about_existing_prefix` matcher.  Pin the wording here so a
    // future copy-edit can't quietly drop below that bar and re-open the silent
    // double-count footgun while the live audit isn't running.

    /// Mirrors `tests/audit_rerun.rs::warned_about_existing_prefix`.
    fn audit_matcher_accepts(s: &str) -> bool {
        let s = s.to_lowercase();
        s.contains("_success")
            || s.contains("already has")
            || s.contains("prior completed run")
            || s.contains("would overwrite")
            || s.contains("orphan")
    }

    #[test]
    fn rerun_warning_message_matches_live_audit_matcher_for_success_marker() {
        let msg = rerun_warning_message("file:///tmp/out", "_SUCCESS");
        assert!(
            audit_matcher_accepts(&msg),
            "rerun warning must trip the live audit matcher; message was: {msg}"
        );
        // Names the prefix and the safe recovery so the operator can act.
        assert!(
            msg.contains("file:///tmp/out"),
            "must name the prefix: {msg}"
        );
        assert!(
            msg.contains("--resume"),
            "must point at the safe recovery: {msg}"
        );
    }

    #[test]
    fn rerun_warning_message_matches_live_audit_matcher_for_manifest_marker() {
        // When only `manifest.json` is present (committed parts, no `_SUCCESS`),
        // the `_SUCCESS` substring is gone — the message must still trip the
        // matcher via `already has` / `prior completed run` / `orphan`.
        let msg = rerun_warning_message("file:///tmp/out", "manifest.json");
        assert!(
            audit_matcher_accepts(&msg),
            "manifest-only rerun warning must still trip the live audit matcher; message was: {msg}"
        );
    }
}
