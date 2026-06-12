//! **Layer: Observability**
//!
//! Per-run report artifacts written to `.rivet/runs/<run_id>/`.
//!
//! Two files are produced after every run (success or failure):
//! - `summary.json` — machine-readable [`RunReport`] (stable contract)
//! - `summary.md`   — operator-friendly Markdown (PRs, support tickets, incident reviews)
//!
//! Failures to write the report are non-fatal: callers log and continue, matching
//! the same policy used for `file_log` writes (invariant I7 — observability must
//! not break pipelines).
//!
//! The report is built from [`RunSummary`] + its embedded [`RunJournal`].  The
//! report struct is decoupled from `RunSummary` so the on-disk JSON schema can
//! evolve independently from internal accumulator fields.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::journal::{RunEvent, RunJournal};
use crate::pipeline::summary::RunSummary;

/// Compute the on-disk report directory for a run.
///
/// Layout: `dirname(config_path)/.rivet/runs/<run_id>/`, placed beside
/// `.rivet_state.db` to mirror the existing state-file convention.
pub fn report_dir(config_path: &str, run_id: &str) -> PathBuf {
    let config_dir = Path::new(config_path).parent().unwrap_or(Path::new("."));
    config_dir.join(".rivet").join("runs").join(run_id)
}

/// Public, stable JSON schema for a single export run.
///
/// Field additions are backwards-compatible (consumers must ignore unknown
/// fields); field removals or type changes require a versioned bump and are
/// avoided where possible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub run_id: String,
    pub export_name: String,
    pub status: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub duration_ms: i64,

    pub source_engine: Option<String>,
    pub destination_kind: Option<String>,
    pub format: String,
    pub compression: String,
    pub tuning_profile: String,
    pub batch_size: usize,

    pub total_rows: i64,
    pub files_produced: usize,
    pub bytes_written: u64,
    pub peak_rss_mb: i64,
    pub retries: u32,

    /// Postgres temp-spill delta in bytes (None on non-PG or when probe failed).
    pub pg_temp_bytes_delta: Option<i64>,

    pub validation: Option<ValidationOutcome>,
    pub reconciliation: Option<ReconciliationOutcome>,
    pub schema_changed: Option<bool>,
    pub schema_changes: Vec<SchemaChangeEntry>,
    pub plan_warnings: Vec<PlanWarningEntry>,

    pub error_message: Option<String>,
    /// True when the run failed *and* at least one file was already committed,
    /// so resuming would skip work rather than start from zero.
    pub resumable: bool,
    /// Suggested invocation to continue an interrupted run, or `None` when the
    /// run completed successfully.
    pub resume_command: Option<String>,

    /// Apply-time context (plan_id, --force usage, bypassed checks).  `None`
    /// when this run came from `rivet run` rather than `rivet apply`.  Added
    /// in 0.7.5 to close the audit-trail gap surfaced by finding F5 (forced
    /// applies left no record).  Existing consumers that ignore unknown
    /// fields are unaffected.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub apply_context: Option<crate::pipeline::summary::ApplyContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationOutcome {
    /// Composite verdict: per-file row counts pass AND, when applicable,
    /// manifest-aware verification (M5) passes.  Stays `true` for the
    /// legacy-run path where only row counts ran.
    pub passed: bool,
    /// Embedded manifest-aware verdict (ADR-0012 M5/M6, ADR-0013 — same
    /// `--validate` flag, no new flag added).  `None` for runs that didn't
    /// reach the manifest-write step (e.g. failed-before-any-part) or that
    /// targeted streaming destinations where no manifest exists.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub manifest: Option<crate::pipeline::ManifestVerification>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationOutcome {
    pub source_count: Option<i64>,
    pub exported_rows: i64,
    pub matched: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangeEntry {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub type_changed: Vec<(String, String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanWarningEntry {
    pub rule: String,
    pub message: String,
}

impl RunReport {
    /// Project a [`RunSummary`] into the public report schema.
    pub fn from_summary(summary: &RunSummary, config_path: &str) -> Self {
        let (started_at, finished_at) = journal_time_bounds(summary);
        let (source_engine, destination_kind) = plan_origin(summary);
        let schema_changes = collect_schema_changes(summary);
        let plan_warnings = collect_plan_warnings(summary);

        let resumable = summary.status == "failed" && summary.files_committed > 0;
        let resume_command = if resumable {
            Some(format!(
                "rivet run --config {} --resume",
                shell_quote(config_path)
            ))
        } else {
            None
        };

        Self {
            run_id: summary.run_id.clone(),
            export_name: summary.export_name.clone(),
            status: summary.status.clone(),
            started_at,
            finished_at,
            duration_ms: summary.duration_ms,

            source_engine,
            destination_kind,
            format: summary.format.clone(),
            compression: summary.compression.clone(),
            tuning_profile: summary.tuning_profile.clone(),
            batch_size: summary.batch_size,

            total_rows: summary.total_rows,
            files_produced: summary.files_produced,
            bytes_written: summary.bytes_written,
            peak_rss_mb: summary.peak_rss_mb,
            retries: summary.retries,

            pg_temp_bytes_delta: summary.pg_temp_bytes_delta,

            validation: summary.validated.map(|passed| ValidationOutcome {
                passed,
                manifest: summary.manifest_verification.clone(),
            }),
            reconciliation: summary.reconciled.map(|matched| ReconciliationOutcome {
                source_count: summary.source_count,
                exported_rows: summary.total_rows,
                matched,
            }),
            schema_changed: summary.schema_changed,
            schema_changes,
            plan_warnings,

            error_message: summary.error_message.clone(),
            resumable,
            resume_command,

            apply_context: summary.apply_context.clone(),
        }
    }
}

/// Write `summary.json` and `summary.md` to `.rivet/runs/<run_id>/`.
///
/// Returns the directory path on success.  Failures (permission denied, disk
/// full, etc.) are returned as `Err` — callers should log-and-continue rather
/// than propagate (see invariant I7).
pub fn write_run_report(config_path: &str, summary: &RunSummary) -> Result<PathBuf> {
    let dir = report_dir(config_path, &summary.run_id);
    std::fs::create_dir_all(&dir)?;

    let report = RunReport::from_summary(summary, config_path);

    let json = serde_json::to_string_pretty(&report)?;
    std::fs::write(dir.join("summary.json"), json)?;
    std::fs::write(dir.join("summary.md"), render_markdown(&report))?;

    Ok(dir)
}

fn journal_time_bounds(summary: &RunSummary) -> (Option<String>, Option<String>) {
    let started = summary
        .journal
        .entries
        .first()
        .map(|e| e.recorded_at.to_rfc3339());
    let finished = summary
        .journal
        .entries
        .iter()
        .rev()
        .find(|e| matches!(e.event, RunEvent::RunCompleted { .. }))
        .or_else(|| summary.journal.entries.last())
        .map(|e| e.recorded_at.to_rfc3339());
    (started, finished)
}

fn plan_origin(summary: &RunSummary) -> (Option<String>, Option<String>) {
    summary
        .journal
        .plan_snapshot()
        .map(|p| {
            let engine = engine_from_base_query(&p.base_query);
            (engine, Some(p.destination_type.clone()))
        })
        .unwrap_or((None, None))
}

/// The plan snapshot doesn't carry the source-engine label explicitly; infer
/// it from the resolved `base_query` shape.  Returns `None` if neither hint is
/// present (callers display the field as missing rather than fabricating one).
fn engine_from_base_query(_q: &str) -> Option<String> {
    // The base_query is the rendered SQL, which doesn't reliably encode the
    // engine.  Real engine identification belongs in a future PlanSnapshot
    // field; until then we leave this absent rather than guess.
    None
}

fn collect_schema_changes(summary: &RunSummary) -> Vec<SchemaChangeEntry> {
    summary
        .journal
        .schema_changes()
        .into_iter()
        .filter_map(|e| match &e.event {
            RunEvent::SchemaChanged {
                added,
                removed,
                type_changed,
            } => Some(SchemaChangeEntry {
                added: added.clone(),
                removed: removed.clone(),
                type_changed: type_changed.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn collect_plan_warnings(summary: &RunSummary) -> Vec<PlanWarningEntry> {
    summary
        .journal
        .entries
        .iter()
        .filter_map(|e| match &e.event {
            RunEvent::PlanWarning { rule, message } => Some(PlanWarningEntry {
                rule: rule.clone(),
                message: message.clone(),
            }),
            _ => None,
        })
        .collect()
}

/// Render the operator-friendly Markdown form of a [`RunReport`].
pub fn render_markdown(r: &RunReport) -> String {
    let mut out = String::with_capacity(1024);

    out.push_str(&format!("# Rivet run: `{}`\n\n", r.run_id));
    out.push_str(&format!("- **Export**: `{}`\n", r.export_name));
    out.push_str(&format!("- **Status**: {}\n", verdict_badge(&r.status)));
    if let Some(s) = &r.started_at {
        out.push_str(&format!("- **Started**: {}\n", s));
    }
    if let Some(f) = &r.finished_at {
        out.push_str(&format!("- **Finished**: {}\n", f));
    }
    out.push_str(&format!(
        "- **Duration**: {} ms ({:.2} s)\n\n",
        r.duration_ms,
        r.duration_ms as f64 / 1000.0
    ));

    out.push_str("## Plan\n\n");
    if let Some(e) = &r.source_engine {
        out.push_str(&format!("- Source engine: {}\n", e));
    }
    if let Some(d) = &r.destination_kind {
        out.push_str(&format!("- Destination: {}\n", d));
    }
    out.push_str(&format!("- Format: {}\n", r.format));
    if !r.compression.is_empty() {
        out.push_str(&format!("- Compression: {}\n", r.compression));
    }
    out.push_str(&format!(
        "- Tuning: profile={}, batch_size={}\n\n",
        r.tuning_profile, r.batch_size
    ));

    out.push_str("## Throughput\n\n");
    out.push_str(&format!("- Rows: {}\n", r.total_rows));
    out.push_str(&format!("- Files: {}\n", r.files_produced));
    out.push_str(&format!("- Bytes: {}\n", r.bytes_written));
    if r.peak_rss_mb > 0 {
        out.push_str(&format!("- Peak RSS: {} MB\n", r.peak_rss_mb));
    }
    if r.retries > 0 {
        out.push_str(&format!("- Retries: {}\n", r.retries));
    }
    if let Some(t) = r.pg_temp_bytes_delta
        && t > 0
    {
        out.push_str(&format!(
            "- PG temp spill: {:.1} MB\n",
            t as f64 / (1024.0 * 1024.0)
        ));
    }
    out.push('\n');

    out.push_str("## Verdicts\n\n");
    out.push_str(&format!(
        "- Validation: {}\n",
        match &r.validation {
            Some(v) if v.passed => "PASSED",
            Some(_) => "FAILED",
            None => "not requested",
        }
    ));
    if let Some(v) = &r.validation
        && let Some(m) = &v.manifest
    {
        if m.legacy_run {
            out.push_str("  - Manifest: not present (legacy_run: true)\n");
        } else if m.manifest_found {
            out.push_str(&format!(
                "  - Manifest: {} parts verified",
                m.parts_verified
            ));
            // Spell out content coverage: how many were md5-checked vs size-only,
            // so "verified" doesn't imply content verification it didn't do.
            if m.parts_verified > 0 {
                let size_only = m.parts_verified.saturating_sub(m.parts_md5_verified);
                out.push_str(&format!(
                    " ({} md5, {} size-only)",
                    m.parts_md5_verified, size_only
                ));
            }
            if m.parts_failed > 0 {
                out.push_str(&format!(", {} failed", m.parts_failed));
            }
            out.push_str(&format!(
                ", _SUCCESS {}",
                if m.success_marker_consistent {
                    "consistent"
                } else {
                    "absent"
                }
            ));
            out.push('\n');
            for failure in &m.failures {
                // Failure has its own `Display` impl in `validate_manifest.rs`
                // — single source of truth for operator-facing failure lines.
                out.push_str(&format!("    - {}\n", failure));
            }
        }
    }
    let reconciliation_line = match &r.reconciliation {
        Some(rc) if rc.matched => "MATCHED".to_string(),
        Some(rc) => format!(
            "MISMATCH (exported {} vs source {})",
            rc.exported_rows,
            rc.source_count
                .map(|n| n.to_string())
                .unwrap_or_else(|| "?".to_string())
        ),
        None => "not requested".to_string(),
    };
    out.push_str(&format!("- Reconciliation: {}\n", reconciliation_line));
    if let Some(changed) = r.schema_changed {
        out.push_str(&format!(
            "- Schema: {}\n",
            if changed { "CHANGED" } else { "unchanged" }
        ));
    }
    out.push('\n');

    if !r.schema_changes.is_empty() {
        out.push_str("## Schema changes\n\n");
        for ch in &r.schema_changes {
            if !ch.added.is_empty() {
                out.push_str(&format!("- Added: {}\n", ch.added.join(", ")));
            }
            if !ch.removed.is_empty() {
                out.push_str(&format!("- Removed: {}\n", ch.removed.join(", ")));
            }
            for (name, old, new) in &ch.type_changed {
                out.push_str(&format!("- Type changed: `{}` {} → {}\n", name, old, new));
            }
        }
        out.push('\n');
    }

    if !r.plan_warnings.is_empty() {
        out.push_str("## Plan warnings\n\n");
        for w in &r.plan_warnings {
            out.push_str(&format!("- `{}`: {}\n", w.rule, w.message));
        }
        out.push('\n');
    }

    if let Some(err) = &r.error_message {
        out.push_str("## Error\n\n");
        out.push_str("```\n");
        out.push_str(err);
        if !err.ends_with('\n') {
            out.push('\n');
        }
        out.push_str("```\n\n");
    }

    if r.resumable {
        out.push_str("## Resume\n\n");
        out.push_str(
            "The run failed after committing one or more files. \
             Resume picks up from the last committed checkpoint:\n\n",
        );
        out.push_str("```sh\n");
        if let Some(cmd) = &r.resume_command {
            out.push_str(cmd);
        } else {
            out.push_str("rivet run --resume");
        }
        out.push_str("\n```\n");
    } else if r.status == "failed" {
        out.push_str("## Resume\n\n");
        out.push_str(
            "No files were committed before the failure; resume would re-run \
             from the start.  Inspect the error above before retrying.\n",
        );
    }

    out
}

/// Render run journals as a pretty-printed JSON array for `rivet journal --json`.
///
/// `RunJournal` (and its embedded `JournalEntry` / `RunEvent`) already derive
/// `Serialize`, so the wire form is the canonical journal — no lossy projection.
/// Each element carries the full typed event stream (`FileWritten`, retries,
/// schema changes, the terminal `RunCompleted`, …) that the human view
/// summarises; a machine consumer gets every event with its `recorded_at`
/// timestamp.  An empty slice renders as `[]` (a valid JSON array), never the
/// human "No journal entries …" text, so consumers always parse cleanly.  The
/// caller is responsible for the `--config` existence check (finding #9) before
/// reaching here — this function is pure formatting.
///
/// `dead_code`-allowed until the `rivet journal --json` flag dispatch (a later
/// wave, in the off-limits `cli.rs`/`args.rs`) calls it; exercised today by the
/// module's unit tests.
#[allow(dead_code)]
pub(super) fn journals_to_json(journals: &[RunJournal]) -> Result<String> {
    Ok(serde_json::to_string_pretty(journals)?)
}

fn verdict_badge(status: &str) -> &'static str {
    match status {
        "success" => "SUCCESS",
        "failed" => "FAILED",
        "running" => "INTERRUPTED",
        _ => "UNKNOWN",
    }
}

/// Quote a path for inclusion in a copy-pasteable shell command.  Defensive
/// against spaces or quotes in the config path.  Single-quote wrapping is
/// POSIX-portable; embedded single quotes are escaped with `'\''`.
pub(super) fn shell_quote(s: &str) -> String {
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
    {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::PlanSnapshot;

    fn fresh_summary(status: &str, files_committed: usize) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("test_run_001", "orders").with_plan_snapshot(
            PlanSnapshot {
                export_name: "orders".into(),
                base_query: "SELECT * FROM orders".into(),
                strategy: "snapshot".into(),
                format: "parquet".into(),
                compression: "zstd".into(),
                destination_type: "local".into(),
                tuning_profile: "balanced".into(),
                batch_size: 1000,
                validate: true,
                reconcile: false,
                resume: false,
            },
        );
        s.total_rows = 12_345;
        s.files_produced = 3;
        s.bytes_written = 4096;
        s.files_committed = files_committed;
        s.duration_ms = 100;
        s.peak_rss_mb = 50;
        s.validated = Some(true);
        s.schema_changed = Some(false);
        if status == "failed" {
            s = s.with_error("connection reset");
        }
        s.with_status(status)
    }

    #[test]
    fn report_dir_is_under_config_dirname() {
        let p = report_dir("/tmp/foo/rivet.yaml", "abc");
        assert_eq!(p, std::path::PathBuf::from("/tmp/foo/.rivet/runs/abc"));
    }

    #[test]
    fn report_dir_handles_bare_filename() {
        let p = report_dir("rivet.yaml", "abc");
        assert_eq!(p, std::path::PathBuf::from(".rivet/runs/abc"));
    }

    #[test]
    fn from_summary_success_path_has_no_resume_hint() {
        let s = fresh_summary("success", 0);
        let r = RunReport::from_summary(&s, "rivet.yaml");
        assert_eq!(r.status, "success");
        assert!(!r.resumable);
        assert!(r.resume_command.is_none());
        assert!(r.error_message.is_none());
    }

    #[test]
    fn from_summary_failed_with_commits_is_resumable() {
        let s = fresh_summary("failed", 2);
        let r = RunReport::from_summary(&s, "rivet.yaml");
        assert_eq!(r.status, "failed");
        assert!(r.resumable);
        let cmd = r.resume_command.as_deref().unwrap();
        assert!(cmd.starts_with("rivet run --config "));
        assert!(cmd.ends_with(" --resume"));
    }

    #[test]
    fn from_summary_failed_without_commits_is_not_resumable() {
        let s = fresh_summary("failed", 0);
        let r = RunReport::from_summary(&s, "rivet.yaml");
        assert!(!r.resumable);
        assert!(r.resume_command.is_none());
    }

    #[test]
    fn write_run_report_creates_both_files() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("rivet.yaml");
        std::fs::write(&cfg, "exports: []").unwrap();

        let s = fresh_summary("success", 0);
        let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();

        assert!(out.join("summary.json").exists());
        assert!(out.join("summary.md").exists());
        let md = std::fs::read_to_string(out.join("summary.md")).unwrap();
        assert!(md.contains("Rivet run"));
        assert!(md.contains("orders"));
        assert!(md.contains("SUCCESS"));
    }

    #[test]
    fn json_is_parseable_and_carries_core_fields() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("rivet.yaml");
        std::fs::write(&cfg, "exports: []").unwrap();
        let s = fresh_summary("failed", 1);
        let out = write_run_report(cfg.to_str().unwrap(), &s).unwrap();
        let json = std::fs::read_to_string(out.join("summary.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["run_id"], "test_run_001");
        assert_eq!(parsed["status"], "failed");
        assert_eq!(parsed["resumable"], true);
        assert!(
            parsed["resume_command"]
                .as_str()
                .unwrap()
                .contains("--resume")
        );
    }

    #[test]
    fn shell_quote_passthrough_for_simple_paths() {
        assert_eq!(shell_quote("rivet.yaml"), "rivet.yaml");
        assert_eq!(
            shell_quote("/etc/rivet/config.yaml"),
            "/etc/rivet/config.yaml"
        );
    }

    #[test]
    fn shell_quote_wraps_paths_with_spaces() {
        assert_eq!(shell_quote("my config.yaml"), "'my config.yaml'");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn markdown_marks_running_status_as_interrupted() {
        let s = fresh_summary("running", 0);
        let r = RunReport::from_summary(&s, "rivet.yaml");
        let md = render_markdown(&r);
        assert!(md.contains("INTERRUPTED"));
    }

    #[test]
    fn journals_to_json_empty_is_valid_array() {
        let json = journals_to_json(&[]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_array(), "empty journals must serialize as []");
        assert_eq!(parsed.as_array().unwrap().len(), 0);
        // Must NOT leak the human-view sentinel into the machine contract.
        assert!(!json.contains("No journal entries"));
    }

    #[test]
    fn journals_to_json_carries_events_and_file_names() {
        let mut j = RunJournal::new("run-42", "orders");
        j.record(RunEvent::FileWritten {
            file_name: "part-000.parquet".into(),
            rows: 100,
            bytes: 4096,
            part_index: 0,
        });
        j.record(RunEvent::RunCompleted {
            status: "success".into(),
            error_message: None,
            duration_ms: 1234,
        });

        let json = journals_to_json(&[j]).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 1);

        let run = &arr[0];
        assert_eq!(run["run_id"], "run-42");
        assert_eq!(run["export_name"], "orders");

        let entries = run["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        // Each entry carries a timestamp and a typed event (serde externally
        // tagged enum → the variant name is the key).
        assert!(entries[0]["recorded_at"].is_string());
        let file_event = &entries[0]["event"]["FileWritten"];
        assert_eq!(file_event["file_name"], "part-000.parquet");
        assert_eq!(file_event["rows"], 100);
        // The produced file name (finding #24's human-view gap) is present in
        // the machine contract too, so `--json` consumers can cross-check it.
        assert!(json.contains("part-000.parquet"));
        // Terminal outcome is the second entry.
        assert_eq!(entries[1]["event"]["RunCompleted"]["status"], "success");
    }
}
