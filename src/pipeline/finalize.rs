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
    if written {
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
    use crate::pipeline::validate_manifest::verify_at_destination;

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

    match verify_at_destination(&*dest, "") {
        Ok(v) => {
            // Compose the file-row check (already on summary.validated) with
            // the manifest-aware verdict.  Legacy runs (M6) keep their existing
            // row-count verdict — manifest verification only DOWNgrades when
            // it has explicit failures.
            if v.has_failures() && summary.validated == Some(true) {
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

/// Best-effort textual URI for the manifest's `destination.uri` field.
///
/// The manifest is a record of where data was written, so the URI must
/// reflect what an operator would type to find the prefix again.
fn destination_uri_for_manifest(cfg: &DestinationConfig) -> String {
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
}
