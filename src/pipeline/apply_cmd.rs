//! **`rivet apply`** — execute a previously-generated `PlanArtifact`.
//!
//! Responsibilities:
//!
//! 1. Deserialize `PlanArtifact` from the plan JSON file.
//! 2. Check staleness (warn > 1 h, error > 24 h unless `--force`).
//! 3. For `Incremental`: validate cursor has not drifted since plan time.
//! 4. Execute using `ChunkSource::Precomputed` so chunk boundaries from the
//!    artifact are replayed without re-running `SELECT min/max` queries.
//! 5. Persist outcomes (manifest, cursor, metrics) via `StateStore` as usual.

use chrono::Duration;
use std::path::Path;

use crate::error::Result;
use crate::plan::{PlanArtifact, StalenessCheck};
use crate::state::StateStore;

use super::chunked::ChunkSource;

/// Entry point for `rivet apply <plan-file> [--force]`.
pub fn run_apply_command(plan_file: &str, force: bool) -> Result<()> {
    // 1. Load artifact
    let artifact = PlanArtifact::from_file(plan_file)?;

    // 2. Staleness check
    let warn_threshold = Duration::hours(1);
    let error_threshold = Duration::hours(24);
    match artifact.staleness(warn_threshold, error_threshold) {
        StalenessCheck::Fresh => {}
        StalenessCheck::StaleWarn(age) => {
            log::warn!(
                "plan '{}' is {} minutes old — consider regenerating with `rivet plan`",
                artifact.export_name,
                age.num_minutes()
            );
        }
        StalenessCheck::StaleError(age) => {
            if !force {
                anyhow::bail!(
                    "plan '{}' is {} hours old (limit: 24 h). Regenerate with `rivet plan` or pass --force to override.",
                    artifact.export_name,
                    age.num_hours()
                );
            }
            log::warn!(
                "plan '{}': ignoring staleness ({} h) because --force was passed",
                artifact.export_name,
                age.num_hours()
            );
        }
    }

    // 3. Open StateStore from the plan file's directory (no config file needed)
    let plan_dir = Path::new(plan_file)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let state_path = plan_dir.join(".rivet_state.db");
    let state = StateStore::open(state_path.to_str().unwrap_or(".rivet_state.db"))?;

    // 4. Cursor drift check for Incremental exports
    if artifact.computed.cursor_snapshot.is_some() {
        let current = state.get(&artifact.export_name)?.last_cursor_value;
        if !artifact.cursor_matches(current.as_deref()) {
            anyhow::bail!(
                "plan '{}': cursor has drifted since plan was generated \
                 (plan snapshot: {:?}, current: {:?}). \
                 Regenerate with `rivet plan` or pass --force to skip this check.",
                artifact.export_name,
                artifact.computed.cursor_snapshot,
                current,
            );
        }
    }

    // 5. Build ChunkSource from the pre-computed ranges in the artifact
    let chunk_source = if artifact.computed.chunk_ranges.is_empty() {
        ChunkSource::Detect
    } else {
        ChunkSource::Precomputed(artifact.computed.chunk_ranges.clone())
    };

    // 6. Execute using the plan from the artifact
    let plan = artifact.resolved_plan.clone();
    super::run_export_job_with_chunk_source(&plan, &state, chunk_source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, MetaColumns, SourceConfig,
        SourceType,
    };
    use crate::plan::{
        ComputedPlanData, ExtractionStrategy, PlanArtifact, PlanDiagnostics, ResolvedRunPlan,
    };
    use crate::tuning::SourceTuning;
    use chrono::{Duration, Utc};

    fn unreachable_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "orders".into(),
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
                path: Some("/tmp/rivet_apply_test".into()),
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
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            // Deliberately unreachable — tests that fail early won't try to connect.
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://nobody:wrong@127.0.0.2:9999/nonexistent".into()),
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
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
        }
    }

    fn fresh_artifact() -> PlanArtifact {
        PlanArtifact::new(
            "orders".into(),
            "full".into(),
            String::new(),
            unreachable_plan(),
            ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot: None,
                row_estimate: None,
            },
            PlanDiagnostics {
                verdict: "Efficient".into(),
                warnings: vec![],
                recommended_profile: "balanced".into(),
            },
        )
    }

    fn write_artifact(dir: &tempfile::TempDir, artifact: &PlanArtifact) -> String {
        let path = dir.path().join("plan.json");
        let json = artifact.to_json_pretty().expect("serialize");
        std::fs::write(&path, json).expect("write plan.json");
        path.to_str().unwrap().to_string()
    }

    // ── staleness enforcement ────────────────────────────────────────────────

    #[test]
    fn stale_error_without_force_is_rejected() {
        let mut artifact = fresh_artifact();
        artifact.created_at = Utc::now() - Duration::hours(25);
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_artifact(&dir, &artifact);

        let err = run_apply_command(&path, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("hours old") || msg.contains("24 h"),
            "expected staleness error: {msg}"
        );
    }

    // ── cursor drift ─────────────────────────────────────────────────────────

    #[test]
    fn cursor_drift_detected_no_prior_state() {
        let mut artifact = fresh_artifact();
        // Plan recorded a cursor snapshot; state store starts empty → drift.
        artifact.computed.cursor_snapshot = Some("2025-06-01T00:00:00Z".into());
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_artifact(&dir, &artifact);

        let err = run_apply_command(&path, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("drifted") || msg.contains("cursor"),
            "expected cursor drift error: {msg}"
        );
    }

    // ── file I/O errors ──────────────────────────────────────────────────────

    #[test]
    fn missing_plan_file_returns_error() {
        let err = run_apply_command("/tmp/rivet_nonexistent_xyzxyz.json", false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("cannot read") || msg.contains("No such file"),
            "expected file-not-found: {msg}"
        );
    }

    #[test]
    fn corrupt_plan_file_returns_parse_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("plan.json");
        std::fs::write(&path, b"not valid json at all").unwrap();
        let err = run_apply_command(path.to_str().unwrap(), false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("invalid plan") || msg.contains("JSON") || msg.contains("expected"),
            "expected parse error: {msg}"
        );
    }
}
