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
use super::summary::ApplyContext;

/// Entry point for `rivet apply <plan-file> [--parallel-export-processes] [--resume] [--force]`.
pub fn run_apply_command(plan_file: &str, force: bool, parallel: bool, resume: bool) -> Result<()> {
    // A YAML config selects the wave-ordered multi-export path (plan→apply
    // cycle): run every export wave-by-wave in ascending `wave:` order. A JSON
    // plan artifact falls through to the sealed single-export replay below.
    if plan_file.ends_with(".yaml") || plan_file.ends_with(".yml") {
        return super::run::run_waves(plan_file, force, parallel, resume);
    }
    if parallel || resume {
        log::warn!(
            "--parallel-export-processes / --resume apply only to wave-ordered config execution; ignored for a sealed plan artifact"
        );
    }

    // 1. Load artifact
    let artifact = PlanArtifact::from_file(plan_file)?;

    // 1a. Tamper-evidence (ADR-0005 PA10, finding #16): reject a plan whose
    //     `resolved_plan` was edited after planning before we run any query.
    //     Unlike staleness/cursor-drift this is NOT bypassable by --force —
    //     a hand-edited execution contract is never something the operator
    //     can opt into; the only correct recovery is to re-run `rivet plan`.
    artifact.verify_integrity()?;

    // 1b. Un-appliable inline-url plan (finding #17): a plan generated from an
    //     inline `url:` config has its credentials redacted to `REDACTED@…`
    //     with no env/file reference to re-resolve them, so apply would die
    //     deep in the driver with an opaque auth/"password missing" error.
    //     Catch it here with the exact remedy.
    reject_unrecoverable_inline_url(&artifact)?;

    // Track which preflight checks --force actually overrode for this run.
    // Threaded into RunSummary.apply_context so the run report carries an
    // auditable record of bypassed gates (finding F5 of the 0.7.5 audit).
    let mut force_bypassed: Vec<String> = Vec::new();

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
            // F6 (0.7.5 audit): "56035 hours old" is mathematically
            // correct but unreadable.  For ages above 48 h switch to
            // days + the actual `created_at` date; below that keep
            // hours so the boundary right above 24 h reads cleanly.
            let age_phrase = if age.num_hours() >= 48 {
                format!(
                    "{} days old (created {})",
                    age.num_days(),
                    artifact.created_at.format("%Y-%m-%d")
                )
            } else {
                format!("{} hours old", age.num_hours())
            };
            if !force {
                anyhow::bail!(
                    "plan '{}' is {} (limit: 24 h). Regenerate with `rivet plan` or pass --force to override.",
                    artifact.export_name,
                    age_phrase,
                );
            }
            force_bypassed.push("staleness".into());
            log::warn!(
                "plan '{}': ignoring staleness ({}) because --force was passed",
                artifact.export_name,
                age_phrase,
            );
        }
    }

    // 3. Open StateStore — F13 (0.7.5 audit) resolution policy:
    //    a) If the artifact recorded the original config path AND that
    //       directory exists, open state next to it.  This is the only
    //       path that keeps `apply` consistent with `rivet run`'s state
    //       location, so cursors, manifests, and schema history line up.
    //    b) Otherwise fall back to the plan file's own directory — the
    //       pre-0.7.5 behaviour, which is the right answer when the
    //       config has been deleted or moved and the operator just
    //       wants to replay the artifact in isolation.
    //
    //    The fallback path also emits a WARN so the operator notices
    //    the divergence (e.g. plan files stored separately from
    //    configs without explicit intent).
    let plan_dir = Path::new(plan_file)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let state_dir = match artifact
        .config_path
        .as_deref()
        .map(Path::new)
        .and_then(Path::parent)
    {
        Some(dir) if dir.exists() => dir.to_path_buf(),
        Some(dir) => {
            log::warn!(
                "plan '{}': original config dir '{}' no longer exists; opening state next to plan file instead. \
                 Cursors and manifest history from the original run will not be visible.",
                artifact.export_name,
                dir.display(),
            );
            plan_dir.to_path_buf()
        }
        None => {
            log::warn!(
                "plan '{}': artifact has no recorded config path (pre-0.7.5 plan?). \
                 Opening state next to the plan file; this may diverge from the state \
                 used by `rivet run` for the same config.",
                artifact.export_name,
            );
            plan_dir.to_path_buf()
        }
    };
    let state_path = state_dir.join(".rivet_state.db");
    let state = StateStore::open(state_path.to_str().unwrap_or(".rivet_state.db"))?;

    // 4. Cursor drift check for Incremental exports
    //
    // Finding F1 of the 0.7.5 audit: the error message has always told the
    // user "pass --force to skip this check", but until 0.7.5 the code did
    // not actually honour `--force` here.  The flag now bypasses the bail
    // with a WARN log, matching the documented contract and the analogous
    // staleness path above.
    if artifact.computed.cursor_snapshot.is_some() {
        let current = state.get(&artifact.export_name)?.last_cursor_value;
        if !artifact.cursor_matches(current.as_deref()) {
            if !force {
                anyhow::bail!(
                    "plan '{}': cursor has drifted since plan was generated \
                     (plan snapshot: {:?}, current: {:?}). \
                     Regenerate with `rivet plan` or pass --force to skip this check.",
                    artifact.export_name,
                    artifact.computed.cursor_snapshot,
                    current,
                );
            }
            force_bypassed.push("cursor_drift".into());
            log::warn!(
                "plan '{}': cursor has drifted (plan snapshot: {:?}, current: {:?}) — \
                 proceeding because --force was passed",
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

    // 6. Execute using the plan from the artifact, carrying the apply audit
    // context into the run report (F5).
    let apply_context = ApplyContext {
        plan_id: artifact.plan_id.clone(),
        forced: force,
        force_bypassed,
    };
    let plan = artifact.resolved_plan.clone();
    super::run_export_job_with_chunk_source(
        &plan,
        &state,
        chunk_source,
        plan_file,
        Some(apply_context),
    )
}

/// Finding #17: reject — with an actionable remedy — a plan whose source
/// credentials were stripped at plan time (inline `url:`) and cannot be
/// re-resolved at apply time.
///
/// `PlanArtifact::new` redacts plaintext credentials (PA9): an inline
/// `url: "postgresql://user:pass@host/db"` becomes `postgresql://REDACTED@host/db`.
/// That is recoverable only if the source *also* carries a reference apply can
/// resolve (`url_env` / `url_file` / `password_env`, or structured `host`+`user`).
/// When none of those exist, connecting later fails deep in the driver with an
/// opaque auth error; surface the fix instead.
fn reject_unrecoverable_inline_url(artifact: &PlanArtifact) -> Result<()> {
    let source = &artifact.resolved_plan.source;

    let url_redacted = source
        .url
        .as_deref()
        .is_some_and(|u| u.contains("REDACTED@"));
    if !url_redacted {
        return Ok(());
    }

    // A redacted URL is still appliable if apply has another way to obtain
    // credentials: an env/file URL reference, an env password, or enough
    // structured fields to rebuild the connection.
    let has_recovery = source.url_env.is_some()
        || source.url_file.is_some()
        || source.password_env.is_some()
        || (source.host.is_some() && source.user.is_some());
    if has_recovery {
        return Ok(());
    }

    anyhow::bail!(
        "plan '{}': source credentials were stripped from this artifact and cannot be \
         recovered at apply time — the plan was created from an inline `url:` config, whose \
         password is never persisted.\n  \
         Fix: re-plan from a config that uses `url_env: <VAR>` (or `url_file:`) so `rivet apply` \
         can resolve credentials, e.g.\n    \
         source:\n      type: {}\n      url_env: DATABASE_URL\n  \
         then `export DATABASE_URL=...` before running apply.",
        artifact.export_name,
        match artifact.resolved_plan.source.source_type {
            crate::config::SourceType::Postgres => "postgres",
            crate::config::SourceType::Mysql => "mysql",
            crate::config::SourceType::Mssql => "mssql",
        },
    )
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
                path: Some("/tmp/rivet_apply_test".into()),
                ..Default::default()
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            // Deliberately unreachable — tests that fail early won't try to
            // connect. No userinfo in the URL: nothing to redact, so the
            // finding-#17 gate (`reject_unrecoverable_inline_url`) does not fire
            // and these fixtures still exercise the staleness / cursor / driver
            // paths they were written for. A credentialed URL here would be
            // rewritten to `REDACTED@…` by PA9 redaction and then rejected by
            // #17 before reaching those gates.
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://127.0.0.2:9999/nonexistent".into()),
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
                strategy_rationale: String::new(),
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

        let err = run_apply_command(&path, false, false, false).unwrap_err();
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

        let err = run_apply_command(&path, false, false, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("drifted") || msg.contains("cursor"),
            "expected cursor drift error: {msg}"
        );
    }

    // ── file I/O errors ──────────────────────────────────────────────────────

    #[test]
    fn missing_plan_file_returns_error() {
        let err = run_apply_command("/tmp/rivet_nonexistent_xyzxyz.json", false, false, false)
            .unwrap_err();
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
        let err = run_apply_command(path.to_str().unwrap(), false, false, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("invalid plan") || msg.contains("JSON") || msg.contains("expected"),
            "expected parse error: {msg}"
        );
    }

    // ── artifact integrity / tamper-evidence (ADR-0005 PA10, finding #16) ─────

    /// Offline mirror of the live RED test `audit_apply_rejects_tampered_plan`:
    /// hand-edit `base_query` in the written plan file (orders → users), then
    /// `apply` must REJECT it at the integrity gate — before any DB connection,
    /// so this test needs no live source. Not bypassable by `--force`.
    #[test]
    fn apply_rejects_tampered_base_query() {
        let artifact = fresh_artifact();
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_artifact(&dir, &artifact);

        // Tamper the serialized artifact the same way the live test does:
        // rewrite the embedded base_query string in place, leaving the seal
        // (and created_at) untouched so only the integrity check can catch it.
        let json = std::fs::read_to_string(&path).unwrap();
        assert!(
            json.contains("SELECT 1"),
            "fixture must embed the planned base_query"
        );
        let tampered = json.replace("SELECT 1", "SELECT * FROM secrets");
        std::fs::write(&path, &tampered).unwrap();

        let err = run_apply_command(&path, false, false, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("integrity check failed") && msg.contains("modified after planning"),
            "tampered plan must be rejected at the integrity gate, got: {msg}"
        );

        // And --force must NOT override it — a hand-edited contract is not opt-in.
        let err_forced = run_apply_command(&path, true, false, false).unwrap_err();
        let msg_forced = format!("{err_forced:#}");
        assert!(
            msg_forced.contains("integrity check failed"),
            "--force must not bypass the integrity gate, got: {msg_forced}"
        );
    }

    // The "untouched artifact is accepted" half is covered deterministically and
    // without any connection attempt by `artifact.rs::integrity_seal_accepts_
    // untouched_artifact`, and indirectly here by `stale_error_without_force_is_
    // rejected` / `cursor_drift_detected_no_prior_state`, which both pass the
    // integrity gate to reach the staleness / cursor gates they assert on.
}
