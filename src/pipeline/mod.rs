//! **Layer: Coordinator** (planning → execution → persistence/observability)
//!
//! `pipeline/mod.rs` is the only module allowed to bridge all three layers.
//! It reads a resolved plan (planning), dispatches to execution modules, then
//! records metrics and sends notifications (persistence/observability).
//!
//! See `docs/adr/0003-layer-classification.md` for the full module taxonomy.

mod aggregate;
mod apply_cmd;
pub(crate) mod chunked;
mod cli;
mod commit;
mod finalize;
pub(crate) mod ipc;
mod job;
mod keyset;
mod manifest_reconcile;
mod manifest_writer;
mod parallel_children;
pub(crate) mod parent_ui;
mod partition_expand;
mod plan_cmd;
pub(crate) mod progress;
mod reconcile_cmd;
mod repair_cmd;
pub(crate) mod report;
mod resume_decisions;
// `pub(crate)` so `error::classify_exit` can reach `retry::classify_error`
// (transient → exit-code 2) without routing through the test-only re-export.
pub(crate) mod retry;
// The `rivet run` orchestrator (~290 LOC) lives next door so this facade
// stays a thin re-export layer.  Module name shadows `pub fn run` below;
// the duplicate is resolved by Rust's namespace rules (modules live in
// the type namespace, fns in the value namespace) and unambiguous at
// every call site (`pipeline::run(...)` is the function).
mod run;
mod run_store;
mod single;
mod sink;
mod summary;
mod validate;
mod validate_cmd;
mod validate_manifest;

// ── Public API surface (consumed by `src/cli/dispatch.rs` + binaries) ──────
//
// These items are the contract the binary depends on.  Adding to this list
// is an API change that requires a release-note entry; removing or
// renaming requires a deprecation cycle.

pub use apply_cmd::run_apply_command;
pub use cli::{
    reset_chunk_checkpoint, reset_chunk_checkpoints_stuck, reset_state, show_chunk_checkpoint,
    show_files, show_journal, show_metrics, show_progression, show_state,
};
pub use plan_cmd::{PlanOutputFormat, run_plan_command};
pub use reconcile_cmd::{ReconcileOutputFormat, run_reconcile_command};
pub use repair_cmd::{RepairOutputFormat, RepairReportSource, run_repair_command};
pub use validate_cmd::{ValidateOutputFormat, ValidateTarget, run_validate_command};

// `RunSummary` is consumed by `notify::*` (via the Coordinator path) plus
// integration-test fixtures.  It is the canonical observability struct so
// it stays in the regular public surface.
pub use summary::RunSummary;

// ── Crate-internal cross-module use ────────────────────────────────────────

pub(crate) use job::run_export_job_with_chunk_source;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use retry::is_transient;

// ── Test-only surface ──────────────────────────────────────────────────────
//
// The integration tests in `tests/*.rs` exercise the trust-contract writers,
// readers, and decision logic without spinning up a full pipeline.  These
// items are NOT part of the public CLI contract — operators get them only
// transitively (via `summary.json`, `manifest.json`, `--validate`, etc.).
//
// Hidden behind `#[doc(hidden)] pub mod for_tests` so they don't pollute
// the rendered crate docs and so a renaming refactor here is a clear
// "test-only" change rather than appearing as a public API break.
//
// Convention matches the existing `destination_for_tests` window in
// `lib.rs`: tests reach these via `rivet::pipeline::for_tests::*`.

#[doc(hidden)]
pub mod for_tests {
    pub use super::chunked::generate_chunks;
    pub use super::manifest_writer::{ManifestBuilder, WriteOutcome, write_manifest};
    pub use super::report::{RunReport, report_dir, write_run_report};
    pub use super::resume_decisions::{
        PartDecision, QuarantineReason, ResumeDecision, ResumePlan, UntrackedDecision,
        build_resume_plan,
    };
    pub use super::retry::{RetryClass, classify_error};
    pub use super::validate::validate_output;
    pub use super::validate_manifest::{
        Failure as ManifestVerificationFailure, ManifestVerification, verify_at_destination,
    };
    pub use crate::plan::build_time_window_query;
}

// Backwards-compat re-exports at the crate root so existing test files
// keep compiling without a sweeping import-site update.  Each is delegated
// to `for_tests::*`; new test code should import from `for_tests` directly.
//
// `#[allow(unused_imports)]` because the bin target's dead-code analysis
// doesn't see the integration tests that consume these — same situation
// as `RunSummary::stub_for_testing`.
#[doc(hidden)]
#[allow(unused_imports)]
pub use for_tests::{
    ManifestBuilder, ManifestVerification, ManifestVerificationFailure, PartDecision,
    QuarantineReason, ResumeDecision, ResumePlan, RetryClass, RunReport, UntrackedDecision,
    WriteOutcome, build_resume_plan, build_time_window_query, classify_error, generate_chunks,
    report_dir, validate_output, verify_at_destination, write_manifest, write_run_report,
};

// The orchestrator and its `RunOptions` live in `run.rs`.  Re-exported
// here so external call sites keep using `pipeline::run(...)` and
// `pipeline::RunOptions`.  Multi-export render-mode flags ride along
// because `RunSummary::print` and the in-place card renderer read them.
pub use run::{RunOptions, run};
#[allow(unused_imports)] // `multi_export_concurrent` is wired for future use
pub(crate) use run::{multi_export_concurrent, multi_export_mode};

pub(crate) fn format_bytes(b: u64) -> String {
    if b >= 1_073_741_824 {
        format!("{:.1} GB", b as f64 / 1_073_741_824.0)
    } else if b >= 1_048_576 {
        format!("{:.1} MB", b as f64 / 1_048_576.0)
    } else if b >= 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else {
        format!("{} B", b)
    }
}

/// Strip the trailing recovery-hint portion of a chunked-pipeline error
/// message produced by `pipeline::chunked`.  Returns the cause prefix and
/// whether a chunked-checkpoint hint was detected.
///
/// Hints emitted by `pipeline::chunked` always follow the pattern
/// `<cause>; <connector> \`rivet …\` …`, so we cut at the first `; ` whose
/// remainder contains a backtick-quoted `rivet` invocation.
///
/// Used by both the per-export card renderer (`parent_ui`) and the run
/// aggregator (`aggregate`) so the long inline command doesn't wrap, distort
/// the in-place card layout, and doesn't repeat the consolidated recovery
/// block printed by the aggregator.
pub(crate) fn strip_chunked_recovery_hint(msg: &str) -> (&str, bool) {
    let mut pos = 0;
    while let Some(off) = msg[pos..].find("; ") {
        let abs = pos + off;
        let tail = &msg[abs + 2..];
        if tail.contains("`rivet ") {
            return (&msg[..abs], true);
        }
        pos = abs + 2;
    }
    (msg, false)
}

/// Truncate `s` to at most `max_chars` Unicode characters, appending `…`
/// when truncated.  Returns `s` unchanged if already short enough.  Used by
/// the in-place card renderer to keep every line within the chosen
/// terminal width — line wrapping breaks the cursor-up redraw math and
/// causes cards to drift down the screen.
pub(crate) fn clamp_line(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let keep = max_chars.saturating_sub(1);
    let mut out: String = s.chars().take(keep).collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SourceConfig, SourceType};
    use crate::plan::{
        CompressionType, DestinationConfig, DestinationType, DiagnosticLevel, ExtractionStrategy,
        FormatType, MetaColumns, ResolvedRunPlan, validate_plan,
    };
    use crate::tuning::SourceTuning;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
        assert_eq!(format_bytes(2_684_354_560), "2.5 GB");
    }

    #[test]
    fn strip_chunked_recovery_hint_strips_use_form() {
        let m = "export 'users': chunk checkpoint run 'users_x' still in progress; \
                 use `rivet run --config foo.yaml --export users --resume` or \
                 `rivet state reset-chunks --config foo.yaml --export users`";
        let (cause, hinted) = strip_chunked_recovery_hint(m);
        assert!(hinted);
        assert_eq!(
            cause,
            "export 'users': chunk checkpoint run 'users_x' still in progress"
        );
    }

    #[test]
    fn strip_chunked_recovery_hint_strips_fix_errors_form() {
        let m = "export 'a': chunk checkpoint incomplete (3 tasks not completed); \
                 fix errors and `rivet run --config c.yaml --export a --resume` or \
                 `rivet state reset-chunks --config c.yaml --export a`";
        let (cause, hinted) = strip_chunked_recovery_hint(m);
        assert!(hinted);
        assert_eq!(
            cause,
            "export 'a': chunk checkpoint incomplete (3 tasks not completed)"
        );
    }

    #[test]
    fn strip_chunked_recovery_hint_passthrough_when_no_hint() {
        let m = "export 'q': source connection refused; retry exhausted";
        let (cause, hinted) = strip_chunked_recovery_hint(m);
        assert!(!hinted);
        assert_eq!(cause, m);
    }

    #[test]
    fn clamp_line_truncates_with_ellipsis() {
        assert_eq!(clamp_line("short", 80), "short");
        assert_eq!(clamp_line("hello world", 8), "hello w…");
        let s = "αβγδ".repeat(50);
        let out = clamp_line(&s, 10);
        assert_eq!(out.chars().count(), 10);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn format_bytes_boundary_values() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1025), "1.0 KB");
        assert_eq!(format_bytes(1_048_575), "1024.0 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_823), "1024.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }

    fn minimal_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "test_export".into(),
            base_query: "SELECT 1".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::default(),
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".into()),
                ..Default::default()
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced (default)".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
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
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 2.0,
            parquet: None,
        }
    }

    #[test]
    fn test_run_summary_fields() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);
        assert_eq!(summary.export_name, "test_export");
        assert_eq!(summary.status, "running");
        assert_eq!(summary.total_rows, 0);
        assert_eq!(summary.files_produced, 0);
        assert_eq!(summary.tuning_profile, "balanced (default)");
        assert_eq!(summary.batch_size, 10_000);
        assert_eq!(summary.format, "parquet");
        assert_eq!(summary.mode, "full");
        assert!(
            summary.run_id.starts_with("test_export_"),
            "run_id should start with export name, got: {}",
            summary.run_id
        );
    }

    // ─── RunSummary::new() journal invariants ────────────────────────────────

    /// `RunSummary::new()` must immediately record a `PlanResolved` event as the
    /// first journal entry.  This satisfies the "what was planned?" query from ADR-0001.
    #[test]
    fn run_summary_new_records_plan_resolved_as_first_event() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);

        assert!(
            !summary.journal.entries.is_empty(),
            "journal must have at least one entry after RunSummary::new()"
        );
        assert!(
            matches!(
                summary.journal.entries[0].event,
                crate::journal::RunEvent::PlanResolved(_)
            ),
            "first journal event must be PlanResolved, got: {:?}",
            summary.journal.entries[0].event
        );
    }

    /// The `PlanSnapshot` recorded inside `PlanResolved` must faithfully capture
    /// key fields from the `ResolvedRunPlan`.
    #[test]
    fn run_summary_plan_snapshot_matches_plan_fields() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);

        let snap = summary
            .journal
            .plan_snapshot()
            .expect("plan_snapshot() must be Some after RunSummary::new()");

        assert_eq!(snap.export_name, plan.export_name);
        assert_eq!(snap.validate, plan.validate);
        assert_eq!(snap.reconcile, plan.reconcile);
        assert_eq!(snap.resume, plan.resume);
        assert_eq!(snap.batch_size, plan.tuning.batch_size);
    }

    /// The journal's `run_id` must match the `RunSummary`'s `run_id`.
    #[test]
    fn run_summary_journal_run_id_matches_summary_run_id() {
        let plan = minimal_plan();
        let summary = RunSummary::new(&plan);
        assert_eq!(
            summary.journal.run_id, summary.run_id,
            "journal run_id must match summary run_id"
        );
    }

    // ─── Rejected plan gate ──────────────────────────────────────────────────

    /// Gap 7 — `run_export_job` bails before execution when `validate_plan` returns
    /// a `Rejected` diagnostic.  The wiring is in `run_export_job` (this file,
    /// ~line 210): if `rejected` is non-empty, the function returns `anyhow::bail!`.
    ///
    /// This test verifies the *condition* that triggers the bail: that `validate_plan`
    /// does in fact produce a `Rejected` diagnostic for the stdout+split combination.
    /// The gate itself (`run_export_job`) cannot be called directly in tests because
    /// it requires a live database connection and config; we test its precondition here.
    #[test]
    fn rejected_plan_produces_rejected_diagnostic_blocking_run_export_job() {
        let mut plan = minimal_plan();
        // stdout + max_file_size triggers check_stdout_split → Rejected.
        plan.destination.destination_type = DestinationType::Stdout;
        plan.max_file_size_bytes = Some(10 * 1024 * 1024);

        let diags = validate_plan(&plan);
        let rejected_count = diags
            .iter()
            .filter(|d| d.level == DiagnosticLevel::Rejected)
            .count();

        assert!(
            rejected_count > 0,
            "stdout + max_file_size must produce a Rejected diagnostic so that \
             run_export_job bails before calling run_with_reconnect; got: {:?}",
            diags
                .iter()
                .map(|d| (&d.rule, &d.level))
                .collect::<Vec<_>>()
        );
    }

    /// stdout + chunked strategy also triggers a Rejected diagnostic (check_stdout_chunked).
    #[test]
    fn rejected_plan_stdout_chunked_blocks_run_export_job() {
        use crate::plan::ChunkedPlan;
        let mut plan = minimal_plan();
        plan.destination.destination_type = DestinationType::Stdout;
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 1000,
            chunk_count: None,
            parallel: 1,
            dense: false,
            by_days: None,
            max_attempts: 3,
            checkpoint: false,
        });

        let diags = validate_plan(&plan);
        assert!(
            diags.iter().any(|d| d.level == DiagnosticLevel::Rejected),
            "stdout + chunked must produce a Rejected diagnostic"
        );
    }

    // ─── synthetic_failed_summary ────────────────────────────────────────────

    /// Pre-`RunSummary::new` failures (plan-build error, plan-validation
    /// rejection) still need to be aggregated.  `synthetic_failed_summary`
    /// produces a minimally-populated summary that aggregation can consume
    /// without panicking.
    #[test]
    fn synthetic_failed_summary_carries_error_and_status() {
        let err = anyhow::anyhow!("could not connect to source: timeout");
        let s = job::synthetic_failed_summary("orders", &err);
        assert_eq!(s.export_name, "orders");
        assert_eq!(s.status, "failed");
        assert_eq!(
            s.error_message.as_deref(),
            Some("could not connect to source: timeout")
        );
        assert!(
            s.run_id.starts_with("orders_"),
            "run_id must be derived from export name, got {}",
            s.run_id
        );
        // Aggregation reads these fields directly — they must default to zero.
        assert_eq!(s.total_rows, 0);
        assert_eq!(s.files_produced, 0);
        assert_eq!(s.bytes_written, 0);
        assert_eq!(s.duration_ms, 0);
    }

    /// `entry_from_summary` must faithfully copy fields the aggregate cares
    /// about.  This guards against silent drift if `RunAggregateEntry` or
    /// `RunSummary` gain new fields.
    #[test]
    fn aggregate_entry_from_summary_copies_observable_fields() {
        let plan = minimal_plan();
        let mut summary = RunSummary::new(&plan);
        summary.status = "success".into();
        summary.total_rows = 12_345;
        summary.files_produced = 3;
        summary.bytes_written = 9_876_543;
        summary.duration_ms = 5_000;

        let entry = aggregate::entry_from_summary(&summary);
        assert_eq!(entry.export_name, summary.export_name);
        assert_eq!(entry.status, "success");
        assert_eq!(entry.run_id, summary.run_id);
        assert_eq!(entry.rows, 12_345);
        assert_eq!(entry.files, 3);
        assert_eq!(entry.bytes, 9_876_543);
        assert_eq!(entry.duration_ms, 5_000);
        assert_eq!(entry.mode, summary.mode);
        assert_eq!(entry.error_message, None);
    }
}
