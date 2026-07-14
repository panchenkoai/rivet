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

/// Write `.rivet/runs/<run_id>/{summary.md,summary.json}` and surface a
/// stderr hint pointing at the report (plus a resume command, when
/// applicable).
///
/// Failures to write are non-fatal: the run keeps its existing exit code,
/// the reason is logged, and the resume hint is still shown so the operator
/// can recover even if disk-full prevents the report itself from landing.
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
    if summary.status == "failed" && summary.files_committed > 0 {
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
/// next, then `_SUCCESS` only when status == Success.  Failures are
/// non-fatal — the run keeps its exit code and operators can investigate
/// via the local run report.
pub(super) fn finalize_manifest(
    plan: &ResolvedRunPlan,
    state: &StateStore,
    summary: &RunSummary,
    kind: &str,
) {
    use crate::manifest::ManifestStatus;
    use crate::pipeline::manifest_writer::{ManifestBuilder, WriteOutcome, write_manifest};

    // CI gate: catch any future runner that drifts summary aggregates away
    // from manifest_parts (the bug parallel_checkpoint had before e9b0796).
    // Debug-build only — compiled out in release.
    if cfg!(debug_assertions)
        && let Err(e) = summary.check_post_run_invariants()
    {
        panic!(
            "summary↔manifest coherence violated at finalize_manifest \
             for {} '{}': {}",
            kind, summary.export_name, e
        );
    }

    let snapshot = match summary.journal.plan_snapshot() {
        Some(s) => s,
        None => {
            // Synthetic-failure summaries never recorded a PlanResolved event.
            // There is no committed work to manifest; just log and return.
            log::debug!(
                "{} '{}': no plan snapshot, manifest skipped",
                kind,
                summary.export_name
            );
            return;
        }
    };

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
        .unwrap_or_else(|| "xxh3:0000000000000000".to_string());

    let source_engine = match plan.source.source_type {
        crate::config::SourceType::Postgres => "postgres",
        crate::config::SourceType::Mysql => "mysql",
        crate::config::SourceType::Mssql => "mssql",
        crate::config::SourceType::Mongo => "mongo",
    };

    // `export_name` is often `schema.table`; split for the manifest fields
    // without fabricating values for free-form queries.
    let (source_schema, source_table) = match summary.export_name.split_once('.') {
        Some((s, t)) if !s.is_empty() && !t.is_empty() => {
            (Some(s.to_string()), Some(t.to_string()))
        }
        _ => (None, None),
    };

    let started_at = summary
        .journal
        .entries
        .first()
        .map(|e| e.recorded_at)
        .unwrap_or_else(chrono::Utc::now);

    let mut builder = ManifestBuilder::new(
        snapshot,
        &summary.run_id,
        started_at,
        schema_fingerprint,
        source_engine,
        source_schema,
        source_table,
        destination_uri_for_manifest(&plan.destination),
    );
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
            None, // source_row_count: follow-up (needs a source COUNT)
        );
    }
    let manifest = builder.finalize(status);

    let dest = match crate::destination::create_destination(&plan.destination) {
        Ok(d) => d,
        Err(e) => {
            log::warn!(
                "{} '{}': could not create destination for manifest write (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
            return;
        }
    };

    match write_manifest(&*dest, &manifest) {
        Ok(WriteOutcome::Written { success_marker }) => {
            log::info!(
                "{} '{}': manifest.json written ({} parts, {} rows){}",
                kind,
                summary.export_name,
                manifest.part_count,
                manifest.row_count,
                if success_marker { " + _SUCCESS" } else { "" },
            );
        }
        Ok(WriteOutcome::SkippedStreaming) => {
            log::info!(
                "{} '{}': manifest skipped (streaming destination)",
                kind,
                summary.export_name,
            );
        }
        Err(e) => {
            log::warn!(
                "{} '{}': manifest write failed (not fatal): {:#}",
                kind,
                summary.export_name,
                e
            );
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
pub(super) fn finalize_validate_manifest(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    kind: &str,
) {
    use crate::destination::WriteCommitProtocol;
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
    if dest.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
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
            if !v.passed && v.manifest_found && summary.validated == Some(true) {
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
pub(super) fn check_success_gate_for_resume(plan: &ResolvedRunPlan) -> Result<()> {
    use crate::destination::WriteCommitProtocol;
    use crate::manifest::SUCCESS_FILENAME;

    let dest = crate::destination::create_destination(&plan.destination)?;
    if dest.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
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
pub(super) fn warn_if_prefix_has_completed_run(plan: &ResolvedRunPlan) {
    use crate::destination::WriteCommitProtocol;
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
    if dest.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
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
    use crate::destination::WriteCommitProtocol;
    use crate::manifest::SUCCESS_FILENAME;
    let Ok(d) = crate::destination::create_destination(dest) else {
        return false;
    };
    if d.capabilities().commit_protocol == WriteCommitProtocol::Streaming {
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

    fn fin_plan(dest: &std::path::Path) -> crate::plan::ResolvedRunPlan {
        use crate::config::{SourceConfig, SourceType};
        crate::plan::ResolvedRunPlan {
            export_name: "public.orders".into(),
            base_query: "SELECT 1".into(),
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
        s.journal.record(crate::journal::RunEvent::PlanResolved(
            crate::journal::PlanSnapshot {
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
            },
        ));
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

        finalize_manifest(&plan, &state, &summary, "export");

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

        finalize_manifest(&plan, &state, &summary, "export");

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

            finalize_manifest(&plan, &state, &summary, "export");

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
