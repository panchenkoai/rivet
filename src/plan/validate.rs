use crate::config::DestinationType;
use crate::plan::{ExtractionStrategy, ResolvedRunPlan};

/// Severity of a compatibility diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiagnosticLevel {
    /// Invalid combination; execution must not proceed.
    Rejected,
    /// Valid but likely unintentional or semantically misleading.
    Warning,
    /// Will execute, but with reduced or degraded guarantees in this combination.
    Degraded,
}

/// A structured diagnostic produced by [`validate_plan`].
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub level: DiagnosticLevel,
    /// Short machine-readable rule identifier.
    pub rule: &'static str,
    pub message: String,
}

/// Validate a [`ResolvedRunPlan`] for known incompatible or degraded combinations.
///
/// Returns a list of diagnostics. The caller is responsible for treating
/// [`DiagnosticLevel::Rejected`] entries as hard errors.
pub fn validate_plan(plan: &ResolvedRunPlan) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    check_stdout_split(&mut diags, plan);
    check_stdout_chunked(&mut diags, plan);
    check_incremental_reconcile(&mut diags, plan);
    check_time_window_reconcile(&mut diags, plan);
    check_quality_chunked(&mut diags, plan);
    check_quality_unique_no_cap(&mut diags, plan);
    check_resume_without_checkpoint(&mut diags, plan);
    check_stdout_manifest(&mut diags, plan);

    diags
}

fn is_stdout(plan: &ResolvedRunPlan) -> bool {
    plan.destination.destination_type == DestinationType::Stdout
}

/// `stdout + max_file_size` — file splitting produces multiple named parts; stdout has no
/// file boundary and cannot represent them.
fn check_stdout_split(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if is_stdout(plan) && plan.max_file_size_bytes.is_some() {
        diags.push(Diagnostic {
            level: DiagnosticLevel::Rejected,
            rule: "stdout-no-split",
            message: format!(
                "export '{}': max_file_size cannot be used with stdout destination — \
                 file splitting requires named output files",
                plan.export_name
            ),
        });
    }
}

/// `stdout + chunked/keyset` — each chunk (or keyset page) produces a separate binary
/// stream; concatenating them to stdout produces unusable output for binary formats
/// like Parquet. Keyset matters here because the planner auto-selects it for tables
/// without a single-integer PK — the operator never opted into multi-part output.
fn check_stdout_chunked(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if !is_stdout(plan) {
        return;
    }
    match &plan.strategy {
        ExtractionStrategy::Chunked(_) => diags.push(Diagnostic {
            level: DiagnosticLevel::Rejected,
            rule: "stdout-no-chunked",
            message: format!(
                "export '{}': chunked mode cannot be used with stdout destination — \
                 each chunk produces a separate binary stream; concatenated output is unusable",
                plan.export_name
            ),
        }),
        ExtractionStrategy::Keyset(_) => diags.push(Diagnostic {
            level: DiagnosticLevel::Rejected,
            rule: "stdout-no-keyset",
            message: format!(
                "export '{}': keyset pagination cannot be used with stdout destination — \
                 each page produces a separate binary stream; concatenated output is unusable",
                plan.export_name
            ),
        }),
        _ => {}
    }
}

/// `incremental + reconcile` — reconcile runs COUNT(*) on the full base query but only
/// new rows (since the cursor) are exported; the count will always appear mismatched.
fn check_incremental_reconcile(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if plan.reconcile && matches!(&plan.strategy, ExtractionStrategy::Incremental(_)) {
        diags.push(Diagnostic {
            level: DiagnosticLevel::Warning,
            rule: "incremental-reconcile-mismatch",
            message: format!(
                "export '{}': reconcile runs COUNT(*) on the full base query but only \
                 rows newer than the cursor are exported; the count will always appear \
                 mismatched after the first run",
                plan.export_name
            ),
        });
    }
}

/// `quality + chunked/keyset` — quality checks run per part file; row-count and
/// uniqueness checks operate on each chunk (or keyset page) independently, not on
/// the full dataset.
fn check_quality_chunked(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if plan.quality.is_some()
        && matches!(
            &plan.strategy,
            ExtractionStrategy::Chunked(_) | ExtractionStrategy::Keyset(_)
        )
    {
        diags.push(Diagnostic {
            level: DiagnosticLevel::Warning,
            rule: "quality-chunked-partial",
            message: format!(
                "export '{}': quality checks run per part file; row_count and \
                 unique_columns checks apply to each chunk independently, not the \
                 full dataset — results may be misleading",
                plan.export_name
            ),
        });
    }
}

/// `--resume` without `chunk_checkpoint: true` — the flag is silently ignored unless
/// the strategy is chunked with checkpoint enabled.
fn check_resume_without_checkpoint(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if plan.resume {
        let is_checkpointed = matches!(
            &plan.strategy,
            ExtractionStrategy::Chunked(cp) if cp.checkpoint
        );
        if !is_checkpointed {
            diags.push(Diagnostic {
                level: DiagnosticLevel::Warning,
                rule: "resume-no-checkpoint",
                message: format!(
                    "export '{}': --resume has no effect unless strategy is chunked with \
                     chunk_checkpoint: true",
                    plan.export_name
                ),
            });
        }
    }
}

/// `time_window + reconcile` — reconcile runs COUNT(*) on the full base query but only
/// rows within the configured time window are exported; the count will always appear mismatched.
fn check_time_window_reconcile(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if plan.reconcile && matches!(&plan.strategy, ExtractionStrategy::TimeWindow { .. }) {
        diags.push(Diagnostic {
            level: DiagnosticLevel::Warning,
            rule: "time-window-reconcile-mismatch",
            message: format!(
                "export '{}': reconcile runs COUNT(*) on the full base query but only \
                 rows within the configured time window are exported; the count will always \
                 appear mismatched",
                plan.export_name
            ),
        });
    }
}

/// `unique_columns` without `unique_max_entries` — uniqueness tracking on high-cardinality
/// columns (id, uuid, email) can grow unbounded in memory.  A cap should be set.
fn check_quality_unique_no_cap(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if let Some(q) = &plan.quality
        && !q.unique_columns.is_empty()
        && q.unique_max_entries.is_none()
    {
        diags.push(Diagnostic {
            level: DiagnosticLevel::Warning,
            rule: "quality-unique-no-cap",
            message: format!(
                "export '{}': unique_columns is configured without unique_max_entries — \
                 uniqueness tracking can grow unbounded on high-cardinality columns. \
                 Set unique_max_entries to cap memory usage (e.g. unique_max_entries: 1000000).",
                plan.export_name
            ),
        });
    }
}

/// `stdout + state recording` — the manifest records a generated filename that does not
/// correspond to any real file path; `rivet state files` will list phantom entries.
fn check_stdout_manifest(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if is_stdout(plan) {
        diags.push(Diagnostic {
            level: DiagnosticLevel::Degraded,
            rule: "stdout-manifest-phantom",
            message: format!(
                "export '{}': manifest records a generated filename that does not correspond \
                 to a real file path when using stdout destination",
                plan.export_name
            ),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IncrementalCursorMode;
    use crate::config::{SourceConfig, SourceType};
    use crate::plan::{
        ChunkedPlan, CompressionType, DestinationConfig, DestinationType, ExtractionStrategy,
        FormatType, IncrementalCursorPlan, MetaColumns, QualityConfig, ResolvedRunPlan,
    };

    fn incremental_simple() -> ExtractionStrategy {
        ExtractionStrategy::Incremental(IncrementalCursorPlan {
            primary_column: "updated_at".into(),
            fallback_column: None,
            mode: IncrementalCursorMode::SingleColumn,
        })
    }
    use crate::tuning::SourceTuning;

    fn base_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "test".into(),
            base_query: "SELECT 1".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::Zstd,
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
                mongo: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 2.0,
            parquet: None,
        }
    }

    fn stdout_plan() -> ResolvedRunPlan {
        let mut p = base_plan();
        p.destination.destination_type = DestinationType::Stdout;
        p.destination.path = None;
        p
    }

    fn chunked_plan_strategy(checkpoint: bool) -> ExtractionStrategy {
        ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 10_000,
            chunk_count: None,
            parallel: 1,
            dense: false,
            by_days: None,
            checkpoint,
            max_attempts: 3,
        })
    }

    fn rules(diags: &[Diagnostic]) -> Vec<&'static str> {
        diags.iter().map(|d| d.rule).collect()
    }

    #[test]
    fn clean_plan_produces_no_diagnostics() {
        assert!(validate_plan(&base_plan()).is_empty());
    }

    // --- stdout-no-split ---

    #[test]
    fn stdout_with_max_file_size_is_rejected() {
        let mut p = stdout_plan();
        p.max_file_size_bytes = Some(100 * 1024 * 1024);
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-split" && d.level == DiagnosticLevel::Rejected),
            "expected stdout-no-split rejection, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn local_with_max_file_size_is_clean() {
        let mut p = base_plan();
        p.max_file_size_bytes = Some(100 * 1024 * 1024);
        let diags = validate_plan(&p);
        assert!(
            diags.iter().all(|d| d.rule != "stdout-no-split"),
            "unexpected stdout-no-split, got: {:?}",
            rules(&diags)
        );
    }

    // --- stdout-no-chunked ---

    #[test]
    fn stdout_with_chunked_is_rejected() {
        let mut p = stdout_plan();
        p.strategy = chunked_plan_strategy(false);
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-chunked" && d.level == DiagnosticLevel::Rejected),
            "expected stdout-no-chunked rejection, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn local_with_chunked_is_clean() {
        let mut p = base_plan();
        p.strategy = chunked_plan_strategy(false);
        let diags = validate_plan(&p);
        assert!(
            diags.iter().all(|d| d.rule != "stdout-no-chunked"),
            "unexpected stdout-no-chunked, got: {:?}",
            rules(&diags)
        );
    }

    // --- incremental-reconcile-mismatch ---

    #[test]
    fn incremental_with_reconcile_warns() {
        let mut p = base_plan();
        p.strategy = incremental_simple();
        p.reconcile = true;
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "incremental-reconcile-mismatch"
                    && d.level == DiagnosticLevel::Warning),
            "expected incremental-reconcile-mismatch warning, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn snapshot_with_reconcile_is_clean() {
        let mut p = base_plan();
        p.reconcile = true;
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .all(|d| d.rule != "incremental-reconcile-mismatch"),
            "unexpected incremental-reconcile-mismatch, got: {:?}",
            rules(&diags)
        );
    }

    // --- quality-chunked-partial ---

    #[test]
    fn quality_with_chunked_warns() {
        let mut p = base_plan();
        p.strategy = chunked_plan_strategy(false);
        p.quality = Some(QualityConfig {
            row_count_min: Some(1),
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: vec![],
            unique_max_entries: None,
        });
        let diags = validate_plan(&p);
        assert!(
            diags.iter().any(|d| d.rule == "quality-chunked-partial"
                && d.level == DiagnosticLevel::Warning),
            "expected quality-chunked-partial warning, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn quality_with_snapshot_is_clean() {
        let mut p = base_plan();
        p.quality = Some(QualityConfig {
            row_count_min: Some(1),
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: vec![],
            unique_max_entries: None,
        });
        let diags = validate_plan(&p);
        assert!(
            diags.iter().all(|d| d.rule != "quality-chunked-partial"),
            "unexpected quality-chunked-partial, got: {:?}",
            rules(&diags)
        );
    }

    // --- resume-no-checkpoint ---

    #[test]
    fn resume_without_checkpoint_warns() {
        let mut p = base_plan();
        p.resume = true;
        // snapshot strategy — checkpoint not applicable
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "resume-no-checkpoint" && d.level == DiagnosticLevel::Warning),
            "expected resume-no-checkpoint warning, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn resume_with_chunked_no_checkpoint_warns() {
        let mut p = base_plan();
        p.strategy = chunked_plan_strategy(false);
        p.resume = true;
        let diags = validate_plan(&p);
        assert!(
            diags.iter().any(|d| d.rule == "resume-no-checkpoint"),
            "expected resume-no-checkpoint warning, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn resume_with_chunked_checkpoint_is_clean() {
        let mut p = base_plan();
        p.strategy = chunked_plan_strategy(true);
        p.resume = true;
        let diags = validate_plan(&p);
        assert!(
            diags.iter().all(|d| d.rule != "resume-no-checkpoint"),
            "unexpected resume-no-checkpoint, got: {:?}",
            rules(&diags)
        );
    }

    // --- stdout-manifest-phantom ---

    #[test]
    fn stdout_produces_degraded_manifest_diagnostic() {
        let p = stdout_plan();
        let diags = validate_plan(&p);
        assert!(
            diags.iter().any(
                |d| d.rule == "stdout-manifest-phantom" && d.level == DiagnosticLevel::Degraded
            ),
            "expected stdout-manifest-phantom degraded, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn local_destination_has_no_manifest_diagnostic() {
        let p = base_plan();
        let diags = validate_plan(&p);
        assert!(
            diags.iter().all(|d| d.rule != "stdout-manifest-phantom"),
            "unexpected stdout-manifest-phantom, got: {:?}",
            rules(&diags)
        );
    }

    // --- time-window-reconcile-mismatch ---

    #[test]
    fn time_window_with_reconcile_warns() {
        let mut p = base_plan();
        p.strategy = ExtractionStrategy::TimeWindow {
            column: "created_at".into(),
            column_type: crate::config::TimeColumnType::Timestamp,
            days_window: 7,
        };
        p.reconcile = true;
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "time-window-reconcile-mismatch"
                    && d.level == DiagnosticLevel::Warning),
            "expected time-window-reconcile-mismatch warning, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn time_window_without_reconcile_is_clean() {
        let mut p = base_plan();
        p.strategy = ExtractionStrategy::TimeWindow {
            column: "created_at".into(),
            column_type: crate::config::TimeColumnType::Timestamp,
            days_window: 7,
        };
        p.reconcile = false;
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .all(|d| d.rule != "time-window-reconcile-mismatch"),
            "unexpected time-window-reconcile-mismatch, got: {:?}",
            rules(&diags)
        );
    }

    // ─── Compatibility Matrix ─────────────────────────────────────────────────────
    //
    // Each test encodes one row of the mode × destination × flag compatibility table.
    // Tests that duplicate existing coverage are noted with a cross-reference rather
    // than re-added. Add a new row whenever a new mode, destination type, or flag is
    // introduced. The matrix is the authoritative record of what combinations are
    // Accepted / Rejected / Warning-producing / Degraded.
    //
    //  ID  │ Mode        │ Destination │ Flags                   │ Expected outcome
    // ─────┼─────────────┼─────────────┼─────────────────────────┼─────────────────────────────────────────
    //  M1  │ Snapshot    │ Local       │ —                       │ Clean           (clean_plan_produces_no_diagnostics)
    //  M2  │ Snapshot    │ Local       │ reconcile               │ Clean           (snapshot_with_reconcile_is_clean)
    //  M3  │ Snapshot    │ Local       │ quality                 │ Clean           (quality_with_snapshot_is_clean)
    //  M4  │ Snapshot    │ Local       │ max_file_size           │ Clean           (local_with_max_file_size_is_clean)
    //  M5  │ Incremental │ Local       │ —                       │ Clean
    //  M6  │ Chunked     │ Local       │ checkpoint + resume     │ Clean           (resume_with_chunked_checkpoint_is_clean)
    //  M7  │ Chunked     │ Local       │ parallel=4              │ Clean
    //  M8  │ TimeWindow  │ Local       │ —                       │ Clean
    //  M9  │ Stdout      │ —           │ snapshot (no flags)     │ Degraded        (stdout_produces_degraded_manifest_diagnostic)
    //  M10 │ Stdout      │ —           │ incremental             │ Degraded        [stdout-manifest-phantom] only
    //  M11 │ Stdout      │ —           │ max_file_size           │ Rejected+Degraded [stdout-no-split, stdout-manifest-phantom]
    //  M12 │ Stdout      │ —           │ chunked                 │ Rejected+Degraded [stdout-no-chunked, stdout-manifest-phantom]
    //  M13 │ Stdout      │ —           │ chunked + max_file_size │ Rejected×2+Degraded (all three rules)
    //  M14 │ Incremental │ Local       │ reconcile               │ Warning         (incremental_with_reconcile_warns)
    //  M15 │ TimeWindow  │ Local       │ reconcile               │ Warning         [time-window-reconcile-mismatch]
    //  M16 │ Chunked     │ Local       │ quality (no checkpoint) │ Warning         (quality_with_chunked_warns)
    //  M17 │ Snapshot    │ Local       │ resume                  │ Warning         (resume_without_checkpoint_warns)
    //  M18 │ Incremental │ Local       │ resume                  │ Warning         [resume-no-checkpoint]
    //  M19 │ Chunked     │ Local       │ no-checkpoint + resume  │ Warning         (resume_with_chunked_no_checkpoint_warns)
    //  M20 │ Stdout      │ —           │ incremental + reconcile │ Warning+Degraded [incremental-reconcile-mismatch, stdout-manifest-phantom]
    //  M21 │ Keyset      │ Local       │ —                       │ Clean
    //  M22 │ Stdout      │ —           │ keyset                  │ Rejected+Degraded [stdout-no-keyset, stdout-manifest-phantom]
    //  M23 │ Keyset      │ Local       │ quality                 │ Warning         (roast_quality_with_keyset_warns)
    //  M24 │ Keyset      │ Local       │ resume                  │ Warning         [resume-no-checkpoint]
    //  M25 │ Keyset      │ Local       │ reconcile               │ Clean           (full-table export; COUNT(*) matches)
    //  M26 │ Stdout      │ —           │ keyset + max_file_size  │ Rejected×2+Degraded [stdout-no-split, stdout-no-keyset, stdout-manifest-phantom]

    // M5 — Incremental + Local + no flags → Clean
    #[test]
    fn matrix_m5_incremental_local_no_flags_is_clean() {
        let mut p = base_plan();
        p.strategy = incremental_simple();
        assert!(
            validate_plan(&p).is_empty(),
            "incremental + local must produce no diagnostics"
        );
    }

    // M7 — Chunked + Local + parallel=4 (no quality, no resume) → Clean
    #[test]
    fn matrix_m7_chunked_local_parallel_is_clean() {
        let mut p = base_plan();
        p.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 10_000,
            chunk_count: None,
            parallel: 4,
            dense: false,
            by_days: None,
            checkpoint: false,
            max_attempts: 3,
        });
        assert!(
            validate_plan(&p).is_empty(),
            "chunked parallel + local must produce no diagnostics"
        );
    }

    // M8 — TimeWindow + Local + no flags → Clean
    #[test]
    fn matrix_m8_time_window_local_no_flags_is_clean() {
        let mut p = base_plan();
        p.strategy = ExtractionStrategy::TimeWindow {
            column: "created_at".into(),
            column_type: crate::config::TimeColumnType::Timestamp,
            days_window: 7,
        };
        assert!(
            validate_plan(&p).is_empty(),
            "time_window + local + no flags must produce no diagnostics"
        );
    }

    // M10 — Stdout + Incremental → exactly one Degraded diagnostic (manifest phantom)
    //        no other rules should fire for this clean-flags combination
    #[test]
    fn matrix_m10_stdout_incremental_degraded_manifest_only() {
        let mut p = stdout_plan();
        p.strategy = incremental_simple();
        let diags = validate_plan(&p);
        assert!(
            diags.iter().any(|d| d.rule == "stdout-manifest-phantom"),
            "must fire stdout-manifest-phantom, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags.iter().all(|d| d.rule == "stdout-manifest-phantom"),
            "stdout + incremental (no flags) must fire ONLY stdout-manifest-phantom, got: {:?}",
            rules(&diags)
        );
    }

    // M11 — Stdout + max_file_size → both Rejected and Degraded diagnostics fire
    #[test]
    fn matrix_m11_stdout_max_file_size_fires_rejected_and_degraded() {
        let mut p = stdout_plan();
        p.max_file_size_bytes = Some(100 * 1024 * 1024);
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-split" && d.level == DiagnosticLevel::Rejected),
            "must fire stdout-no-split Rejected, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags.iter().any(
                |d| d.rule == "stdout-manifest-phantom" && d.level == DiagnosticLevel::Degraded
            ),
            "must also fire stdout-manifest-phantom Degraded, got: {:?}",
            rules(&diags)
        );
    }

    // M12 — Stdout + chunked → both Rejected and Degraded diagnostics fire
    #[test]
    fn matrix_m12_stdout_chunked_fires_rejected_and_degraded() {
        let mut p = stdout_plan();
        p.strategy = chunked_plan_strategy(false);
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-chunked" && d.level == DiagnosticLevel::Rejected),
            "must fire stdout-no-chunked Rejected, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags.iter().any(
                |d| d.rule == "stdout-manifest-phantom" && d.level == DiagnosticLevel::Degraded
            ),
            "must also fire stdout-manifest-phantom Degraded, got: {:?}",
            rules(&diags)
        );
    }

    // M13 — Stdout + chunked + max_file_size → two Rejected rules + one Degraded all fire
    #[test]
    fn matrix_m13_stdout_chunked_max_file_size_fires_all_three_rules() {
        let mut p = stdout_plan();
        p.strategy = chunked_plan_strategy(false);
        p.max_file_size_bytes = Some(100 * 1024 * 1024);
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-split" && d.level == DiagnosticLevel::Rejected),
            "must fire stdout-no-split, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-chunked" && d.level == DiagnosticLevel::Rejected),
            "must fire stdout-no-chunked, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags.iter().any(
                |d| d.rule == "stdout-manifest-phantom" && d.level == DiagnosticLevel::Degraded
            ),
            "must fire stdout-manifest-phantom, got: {:?}",
            rules(&diags)
        );
        let rejected: Vec<_> = diags
            .iter()
            .filter(|d| d.level == DiagnosticLevel::Rejected)
            .collect();
        assert_eq!(
            rejected.len(),
            2,
            "exactly 2 Rejected diagnostics expected, got: {:?}",
            rules(&diags)
        );
    }

    // M15 — TimeWindow + Local + reconcile → Warning [time-window-reconcile-mismatch]
    //        (covered by time_window_with_reconcile_warns above — cross-reference only)

    // M18 — Incremental + Local + resume → Warning [resume-no-checkpoint]
    #[test]
    fn matrix_m18_incremental_with_resume_warns() {
        let mut p = base_plan();
        p.strategy = incremental_simple();
        p.resume = true;
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "resume-no-checkpoint" && d.level == DiagnosticLevel::Warning),
            "incremental + resume must warn resume-no-checkpoint, got: {:?}",
            rules(&diags)
        );
    }

    // M20 — Stdout + Incremental + reconcile → Warning [incremental-reconcile-mismatch]
    //        AND Degraded [stdout-manifest-phantom] both fire
    #[test]
    fn matrix_m20_stdout_incremental_reconcile_fires_warning_and_degraded() {
        let mut p = stdout_plan();
        p.strategy = incremental_simple();
        p.reconcile = true;
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "incremental-reconcile-mismatch"
                    && d.level == DiagnosticLevel::Warning),
            "must fire incremental-reconcile-mismatch Warning, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags.iter().any(
                |d| d.rule == "stdout-manifest-phantom" && d.level == DiagnosticLevel::Degraded
            ),
            "must also fire stdout-manifest-phantom Degraded, got: {:?}",
            rules(&diags)
        );
        assert_eq!(
            diags.len(),
            2,
            "exactly 2 diagnostics expected for this combination, got: {:?}",
            rules(&diags)
        );
    }

    // ── quality-unique-no-cap ──────────────────────────────────────────────────

    #[test]
    fn unique_columns_without_cap_warns() {
        let mut p = base_plan();
        p.quality = Some(QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: vec!["id".into()],
            unique_max_entries: None,
        });
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "quality-unique-no-cap" && d.level == DiagnosticLevel::Warning),
            "expected quality-unique-no-cap warning, got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn unique_columns_with_cap_is_clean() {
        let mut p = base_plan();
        p.quality = Some(QualityConfig {
            row_count_min: None,
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: vec!["id".into()],
            unique_max_entries: Some(1_000_000),
        });
        let diags = validate_plan(&p);
        assert!(
            diags.iter().all(|d| d.rule != "quality-unique-no-cap"),
            "with cap set, no quality-unique-no-cap warning expected; got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn quality_no_unique_columns_does_not_warn_about_cap() {
        let mut p = base_plan();
        p.quality = Some(QualityConfig {
            row_count_min: Some(1),
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: vec![],
            unique_max_entries: None,
        });
        let diags = validate_plan(&p);
        assert!(
            diags.iter().all(|d| d.rule != "quality-unique-no-cap"),
            "empty unique_columns must not trigger no-cap warning; got: {:?}",
            rules(&diags)
        );
    }

    #[test]
    fn no_quality_config_does_not_warn_about_cap() {
        let p = base_plan();
        let diags = validate_plan(&p);
        assert!(
            diags.iter().all(|d| d.rule != "quality-unique-no-cap"),
            "absent quality config must not trigger no-cap warning; got: {:?}",
            rules(&diags)
        );
    }

    // ── Keyset blind spot (ROAST-RED) ──────────────────────────────────────────

    fn keyset_plan_strategy() -> ExtractionStrategy {
        ExtractionStrategy::Keyset(crate::plan::KeysetPlan {
            key_column: "uuid_pk".into(),
            chunk_size: 10_000,
        })
    }

    // ROAST-RED plan-keyset-rules: compatibility rules match only Chunked(_); Keyset
    // (one part PER PAGE — same multi-part binary shape, auto-selected by the planner
    // for MySQL tables without an int PK) slips past stdout-no-chunked unflagged.
    // Asserts CORRECT behavior; expected to FAIL until the fix lands.
    #[test]
    fn roast_stdout_with_keyset_is_rejected() {
        let mut p = stdout_plan();
        p.strategy = keyset_plan_strategy();
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.level == DiagnosticLevel::Rejected && d.rule.starts_with("stdout-no-")),
            "keyset emits one binary part per page — stdout must be Rejected like chunked \
             (stdout-no-* rule family), got: {:?}",
            rules(&diags)
        );
    }

    // ROAST-RED plan-keyset-rules: quality checks run per-part for Keyset exactly as
    // for Chunked, but check_quality_chunked matches only Chunked(_) — keyset + quality
    // produces no quality-chunked-partial warning.
    // Asserts CORRECT behavior; expected to FAIL until the fix lands.
    #[test]
    fn roast_quality_with_keyset_warns() {
        let mut p = base_plan();
        p.strategy = keyset_plan_strategy();
        p.quality = Some(QualityConfig {
            row_count_min: Some(1),
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: vec![],
            unique_max_entries: None,
        });
        let diags = validate_plan(&p);
        assert!(
            diags.iter().any(|d| d.rule == "quality-chunked-partial"
                && d.level == DiagnosticLevel::Warning),
            "keyset quality checks run per-page part — expected quality-chunked-partial \
             warning as for chunked, got: {:?}",
            rules(&diags)
        );
    }

    // M21 — Keyset + Local + no flags → Clean
    #[test]
    fn matrix_m21_keyset_local_no_flags_is_clean() {
        let mut p = base_plan();
        p.strategy = keyset_plan_strategy();
        assert!(
            validate_plan(&p).is_empty(),
            "keyset + local must produce no diagnostics"
        );
    }

    // M22 — Stdout + keyset → Rejected [stdout-no-keyset] and Degraded
    //        [stdout-manifest-phantom] both fire; the chunked-specific rule must not
    #[test]
    fn matrix_m22_stdout_keyset_fires_rejected_and_degraded() {
        let mut p = stdout_plan();
        p.strategy = keyset_plan_strategy();
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-keyset" && d.level == DiagnosticLevel::Rejected),
            "must fire stdout-no-keyset Rejected, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags.iter().any(
                |d| d.rule == "stdout-manifest-phantom" && d.level == DiagnosticLevel::Degraded
            ),
            "must also fire stdout-manifest-phantom Degraded, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags.iter().all(|d| d.rule != "stdout-no-chunked"),
            "keyset must fire its own rule, not stdout-no-chunked; got: {:?}",
            rules(&diags)
        );
    }

    // M23 — Keyset + Local + quality → Warning [quality-chunked-partial]
    //        (covered by roast_quality_with_keyset_warns above — cross-reference only)

    // M24 — Keyset + Local + resume → Warning [resume-no-checkpoint]
    //        keyset has no checkpoint support; --resume is silently ignored
    #[test]
    fn matrix_m24_keyset_with_resume_warns() {
        let mut p = base_plan();
        p.strategy = keyset_plan_strategy();
        p.resume = true;
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "resume-no-checkpoint" && d.level == DiagnosticLevel::Warning),
            "keyset + resume must warn resume-no-checkpoint, got: {:?}",
            rules(&diags)
        );
    }

    // M25 — Keyset + Local + reconcile → Clean
    //        keyset exports the full table, so reconcile's COUNT(*) is comparable —
    //        unlike incremental/time_window it must NOT inherit a mismatch warning
    #[test]
    fn matrix_m25_keyset_with_reconcile_is_clean() {
        let mut p = base_plan();
        p.strategy = keyset_plan_strategy();
        p.reconcile = true;
        assert!(
            validate_plan(&p).is_empty(),
            "keyset + reconcile must produce no diagnostics"
        );
    }

    // M26 — Stdout + keyset + max_file_size → two Rejected rules + one Degraded all
    //        fire (keyset analogue of M13: the keyset rule composes independently
    //        with stdout-no-split, neither masks the other)
    #[test]
    fn matrix_m26_stdout_keyset_max_file_size_fires_all_three_rules() {
        let mut p = stdout_plan();
        p.strategy = keyset_plan_strategy();
        p.max_file_size_bytes = Some(100 * 1024 * 1024);
        let diags = validate_plan(&p);
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-split" && d.level == DiagnosticLevel::Rejected),
            "must fire stdout-no-split, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags
                .iter()
                .any(|d| d.rule == "stdout-no-keyset" && d.level == DiagnosticLevel::Rejected),
            "must fire stdout-no-keyset, got: {:?}",
            rules(&diags)
        );
        assert!(
            diags.iter().any(
                |d| d.rule == "stdout-manifest-phantom" && d.level == DiagnosticLevel::Degraded
            ),
            "must fire stdout-manifest-phantom, got: {:?}",
            rules(&diags)
        );
        let rejected: Vec<_> = diags
            .iter()
            .filter(|d| d.level == DiagnosticLevel::Rejected)
            .collect();
        assert_eq!(
            rejected.len(),
            2,
            "exactly 2 Rejected diagnostics expected, got: {:?}",
            rules(&diags)
        );
    }
}
