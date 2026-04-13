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
    check_quality_chunked(&mut diags, plan);
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

/// `stdout + chunked` — each chunk produces a separate binary stream; concatenating them
/// to stdout produces unusable output for binary formats like Parquet.
fn check_stdout_chunked(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if is_stdout(plan) && matches!(&plan.strategy, ExtractionStrategy::Chunked(_)) {
        diags.push(Diagnostic {
            level: DiagnosticLevel::Rejected,
            rule: "stdout-no-chunked",
            message: format!(
                "export '{}': chunked mode cannot be used with stdout destination — \
                 each chunk produces a separate binary stream; concatenated output is unusable",
                plan.export_name
            ),
        });
    }
}

/// `incremental + reconcile` — reconcile runs COUNT(*) on the full base query but only
/// new rows (since the cursor) are exported; the count will always appear mismatched.
fn check_incremental_reconcile(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if plan.reconcile && matches!(&plan.strategy, ExtractionStrategy::Incremental { .. }) {
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

/// `quality + chunked` — quality checks run per-chunk file; row-count and uniqueness
/// checks operate on each chunk independently, not on the full dataset.
fn check_quality_chunked(diags: &mut Vec<Diagnostic>, plan: &ResolvedRunPlan) {
    if plan.quality.is_some() && matches!(&plan.strategy, ExtractionStrategy::Chunked(_)) {
        diags.push(Diagnostic {
            level: DiagnosticLevel::Warning,
            rule: "quality-chunked-partial",
            message: format!(
                "export '{}': quality checks run per-chunk file; row_count and \
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
    use crate::config::{SourceConfig, SourceType};
    use crate::plan::{
        ChunkedPlan, CompressionType, DestinationConfig, DestinationType, ExtractionStrategy,
        FormatType, MetaColumns, QualityConfig, ResolvedRunPlan,
    };
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
                bucket: None,
                prefix: None,
                path: Some("./out".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
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
                tuning: None,
            },
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
        p.strategy = ExtractionStrategy::Incremental {
            cursor_column: "updated_at".into(),
        };
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
}
