//! **`rivet apply`** — execute a previously-generated `PlanArtifact`.
//!
//! Responsibilities:
//!
//! 1. Deserialize `PlanArtifact` from the plan JSON file.
//! 2. Answer a replay of a plan that already applied to exit 0 with that run (unless `--force`).
//! 3. Check staleness (warn > 1 h, error > 24 h unless `--force`).
//! 4. For `Incremental`: validate cursor has not drifted since plan time.
//! 5. Execute using `ChunkSource::Precomputed` so chunk boundaries from the
//!    artifact are replayed without re-running `SELECT min/max` queries.
//! 6. Persist outcomes (manifest, cursor, metrics) via `StateStore` as usual, and the plan id on the run that applied it.

use chrono::Duration;
use std::path::Path;

use crate::error::Result;
use crate::plan::{PlanArtifact, StalenessCheck};
use crate::state::StateStore;

use super::chunked::ChunkSource;
use super::summary::ApplyContext;

/// Entry point for `rivet apply <plan-file> [--parallel-export-processes] [--resume] [--force]`.
pub fn run_apply_command(
    plan_file: &str,
    force: bool,
    parallel: bool,
    resume: bool,
    pool: Option<usize>,
    split: bool,
) -> Result<()> {
    // A YAML config selects the wave-ordered multi-export path (plan→apply
    // cycle): run every export wave-by-wave in ascending `wave:` order — or,
    // with `--pool N`, as one bounded work-stealing pool (#166). A JSON plan
    // artifact falls through to the sealed single-export replay below.
    if plan_file.ends_with(".yaml") || plan_file.ends_with(".yml") {
        if let Some(m) = pool {
            return super::run::run_pool(plan_file, force, resume, m, split);
        }
        if split {
            log::warn!(
                "--split applies only to `--pool` scheduling (it breaks the pool's single-export \
                 floor); ignored without --pool."
            );
        }
        return super::run::run_waves(plan_file, force, parallel, resume);
    }
    if pool.is_some() {
        crate::rivet_bail!(
            crate::error::codes::CLI_OPTION_UNSUPPORTED,
            "--pool schedules a whole CONFIG's exports (it needs the full set to order by \
             duration) and does not apply to a sealed single-export plan artifact. Pass the \
             YAML config instead: `rivet apply <config.yaml> --pool N`."
        );
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

    // 2. Open StateStore — F13 (0.7.5 audit) resolution policy:
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

    // 3. A replay of a point-in-time plan that already applied to exit 0 (a retry
    //    whose first try died after rivet finished) answers with that run and
    //    extracts nothing. A reusable plan continues the stored cursor instead.
    let reusable = artifact.is_reusable();
    if !reusable
        && let Some(run_id) =
            state.run_that_applied_plan(&artifact.export_name, &artifact.plan_id)?
    {
        if !force {
            eprintln!("{}", already_applied_report(&state, &artifact, &run_id));
            return Ok(());
        }
        force_bypassed.push("already_applied".into());
        log::warn!(
            "plan '{}': plan {} already applied by run {run_id} — extracting again because \
             --force was passed",
            artifact.export_name,
            artifact.plan_id,
        );
    }

    // 4. Staleness check, for a point-in-time plan only: a reusable plan has no age.
    let warn_threshold = Duration::hours(1);
    let error_threshold = Duration::hours(24);
    let staleness = if reusable {
        StalenessCheck::Fresh
    } else {
        artifact.staleness(warn_threshold, error_threshold)
    };
    match staleness {
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
                crate::rivet_bail!(
                    crate::error::codes::PLAN_ARTIFACT_STALE,
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

    // 5. Cursor drift check for Incremental exports
    //
    // Finding F1 of the 0.7.5 audit: the error message has always told the
    // user "pass --force to skip this check", but until 0.7.5 the code did
    // not actually honour `--force` here.  The flag now bypasses the bail
    // with a WARN log, matching the documented contract and the analogous
    // staleness path above.
    // A cursor the last successful run moved by applying this same plan is the
    // plan's own progress, not drift.
    if artifact.computed.cursor_snapshot.is_some() {
        let source = artifact.resolved_plan.source.state_key();
        let current = state.get(&artifact.export_name, &source)?.last_cursor_value;
        if !artifact.cursor_matches(current.as_deref())
            && !state.cursor_last_moved_by_plan(
                &artifact.export_name,
                &source,
                &artifact.plan_id,
            )?
        {
            if !force {
                crate::rivet_bail!(
                    crate::error::codes::PLAN_ARTIFACT_STALE,
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

    // 6. Build ChunkSource from the pre-computed ranges in the artifact
    let chunk_source = if artifact.computed.chunk_ranges.is_empty() {
        ChunkSource::Detect
    } else {
        ChunkSource::Precomputed(artifact.computed.chunk_ranges.clone())
    };

    // 7. Execute using the plan from the artifact, carrying the apply audit
    // context into the run report (F5).
    let apply_context = ApplyContext {
        plan_id: artifact.plan_id.clone(),
        forced: force,
        force_bypassed,
    };
    // ADR-0005 PA1, the one clock read of an apply: the sealed plan keeps `{date}`
    // as written and this apply resolves it to its own UTC day, as `rivet run`
    // does; a plan frozen with `rivet plan --date`, or sealed by an earlier
    // release, holds a resolved path and changes nothing here.
    let mut plan = artifact.resolved_plan.clone();
    plan.resolve_destination(
        &crate::destination::placeholder::PlaceholderContext::for_today(&plan.export_name),
    );
    // The config is read BEFORE the run so the open probe knows whether to read
    // the source primary key — the load spec recorded afterwards carries it.
    // Recording with the key never asked for cleared the key `rivet run` had
    // recorded (`record_load_spec` upserts; a NULL key wins over a run-origin one).
    let config = artifact
        .config_path
        .as_deref()
        .and_then(|p| crate::config::Config::load(p).ok());
    let record_load_spec = config.as_ref().is_some_and(|c| c.load.is_some());
    let (result, summary) = super::run_export_job_with_chunk_source(
        &plan,
        &state,
        chunk_source,
        plan_file,
        Some(apply_context),
        record_load_spec,
    );
    if result.is_ok()
        && let Some(warning) = record_applied_plan(&state, &artifact, &summary.run_id)
    {
        log::warn!("{warning}");
    }
    if result.is_ok()
        && let Some(cfg_path) = artifact.config_path.as_deref()
        && let Some(config) = config.as_ref()
        && let Some(export) = config
            .exports
            .iter()
            .find(|e| e.name == artifact.export_name)
    {
        let cfg_dir = Path::new(cfg_path)
            .parent()
            .unwrap_or_else(|| Path::new("."));
        super::load_spec::record_after_run(config, export, &state, cfg_dir, None, &summary);
    }

    // 8. The run's tail. This arm is a FULL orchestrator — it opens the state
    // store, drives one export to completion and writes its `export_metrics`
    // row — so it owes the same run-over-run self-check every other tail emits,
    // through the same seam (`run::self_check_throughput`: it holds the
    // exactly-once deferral for a re-exec'd child and the ONE call site of
    // `aggregate::warn_throughput_regressions`).
    //
    // Without it, `rivet plan -c cfg.yaml -e orders -o plan.json && rivet apply
    // plan.json` degraded 2.7× in silence — and the degraded row then became the
    // baseline, so the NEXT run could not flag it either — while the byte-
    // identical export under `rivet run` warned (round-7 bughunt, the fifth tail
    // of the runner-bypass class).
    //
    // No aggregate card and no `run_aggregate` row: one export is exactly the
    // count `run::reports_run_aggregate` refuses, and the per-export card above
    // already printed every number it would carry. The self-check is not gated
    // on that count — a sealed artifact IS the whole invocation.
    //
    // The mode is `run_mode_label(1, false)` — "sequential", asked of the same
    // producer every other runner asks, never spelled here: this replay runs one
    // export in this process, overlapping nothing, so the self-check must keep
    // its ACTIONABLE tail rather than excusing a regression as source sharing.
    let entries = vec![super::aggregate::entry_from_summary(&summary)];
    super::run::self_check_throughput(
        &state,
        &entries,
        &super::run::RunModes::uniform(super::run::run_mode_label(1, false)),
    );
    result
}

/// Record that `run_id` applied this plan, so a replay answers with it; returns the warning of a miss (the replay then extracts again).
fn record_applied_plan(
    state: &StateStore,
    artifact: &PlanArtifact,
    run_id: &str,
) -> Option<String> {
    let why = match state.record_applied_plan(run_id, &artifact.plan_id) {
        Ok(true) => return None,
        Ok(false) => "no run-status row".to_string(),
        Err(e) => format!("{e:#}"),
    };
    Some(format!(
        "plan '{}': could not record that run {run_id} applied plan {} ({why}); a replay of this \
         plan will extract again",
        artifact.export_name, artifact.plan_id,
    ))
}

/// The line `rivet apply` prints for a plan that run `run_id` already applied, with that run's outcome.
fn already_applied_report(state: &StateStore, artifact: &PlanArtifact, run_id: &str) -> String {
    let outcome = match state.get_metric_by_run_id(run_id) {
        Ok(Some(m)) => format!(
            "{}, {} rows, {} files, {} bytes, finished {}",
            m.status, m.total_rows, m.files_produced, m.bytes_written, m.run_at
        ),
        _ => "its metrics row is not readable".into(),
    };
    format!(
        "apply: plan {} of export '{}' was already applied by run {run_id} ({outcome}); nothing \
         was extracted again. To extract new data, make a new plan with `rivet plan`.",
        artifact.plan_id, artifact.export_name,
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
        .is_some_and(|u| u.contains("REDACTED@") || crate::redact::has_keyword_password(u));
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

    crate::rivet_bail!(
        crate::error::codes::PLAN_ARTIFACT_NO_CREDENTIALS,
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
            crate::config::SourceType::Oracle => "oracle",
            crate::config::SourceType::Mongo => "mongo",
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
            split_window: None,
            bytes_read: Default::default(),
            parts_landed: Default::default(),
            single_pass_of: None,
            export_name: "orders".into(),
            partition_rollover: None,
            source_table: None,
            base_query: "SELECT 1".into(),
            query_template: None,
            catalog_query: None,
            destination_written: None,
            is_split_unit: false,
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
                mongo: None,
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

    // ── mutation-W6 gap closure ──────────────────────────────────────────────
    // The unrecoverable-credentials gate (PA9) had no direct test: stubbing it
    // to Ok(()) let a credential-stripped plan sail into an opaque driver auth
    // error at apply time — the exact failure the gate exists to pre-empt.
    #[test]
    fn redacted_url_gate_requires_a_recovery_path() {
        let set = |url: Option<&str>,
                   url_env: Option<&str>,
                   password_env: Option<&str>,
                   host: Option<&str>,
                   user: Option<&str>| {
            let mut a = fresh_artifact();
            let s = &mut a.resolved_plan.source;
            s.url = url.map(str::to_string);
            s.url_env = url_env.map(str::to_string);
            s.url_file = None;
            s.password_env = password_env.map(str::to_string);
            s.host = host.map(str::to_string);
            s.user = user.map(str::to_string);
            a
        };
        let redacted = Some("postgresql://REDACTED@h/db");

        // Un-redacted URL passes untouched.
        reject_unrecoverable_inline_url(&set(
            Some("postgresql://u:p@h/db"),
            None,
            None,
            None,
            None,
        ))
        .expect("plain url is appliable");
        // Redacted with NO recovery path must refuse loudly.
        let err = reject_unrecoverable_inline_url(&set(redacted, None, None, None, None))
            .expect_err("stripped credentials with no recovery must refuse");
        assert!(err.to_string().contains("cannot be"), "actionable: {err:#}");
        // Each recovery path independently unblocks (`||` chain, not `&&`)…
        reject_unrecoverable_inline_url(&set(redacted, Some("DB_URL"), None, None, None))
            .expect("url_env recovers");
        reject_unrecoverable_inline_url(&set(redacted, None, Some("PGPASS"), None, None))
            .expect("password_env recovers");
        reject_unrecoverable_inline_url(&set(redacted, None, None, Some("h"), Some("u")))
            .expect("host+user recovers");
        // …but host WITHOUT user is not enough (`&&`, not `||`).
        reject_unrecoverable_inline_url(&set(redacted, None, None, Some("h"), None))
            .expect_err("host without user cannot rebuild the connection");
        // A keyword/value string whose password was masked is refused the same way…
        let masked = Some("host=h user=u password=*** dbname=d");
        let err = reject_unrecoverable_inline_url(&set(masked, None, None, None, None))
            .expect_err("a masked keyword/value password cannot authenticate");
        assert!(
            err.to_string().contains("url_env: <VAR>"),
            "names the remedy: {err:#}"
        );
        reject_unrecoverable_inline_url(&set(masked, Some("DB_URL"), None, None, None))
            .expect("url_env recovers");
        // …and one that never held a password is appliable as it is.
        reject_unrecoverable_inline_url(&set(
            Some("host=/var/run/postgresql user=u dbname=d"),
            None,
            None,
            None,
            None,
        ))
        .expect("no password was stripped");
    }

    // ── staleness enforcement ────────────────────────────────────────────────

    /// A plan past 24 h is refused STALE before it runs, its age in hours below 48 h and in days with its date from 48 h.
    #[test]
    fn stale_error_without_force_is_rejected() {
        for (hours, phrase) in [
            (25, "25 hours old".to_string()),
            (47, "47 hours old".to_string()),
            (
                48,
                format!(
                    "2 days old (created {})",
                    (Utc::now() - Duration::hours(48)).format("%Y-%m-%d")
                ),
            ),
            (
                73,
                format!(
                    "3 days old (created {})",
                    (Utc::now() - Duration::hours(73)).format("%Y-%m-%d")
                ),
            ),
        ] {
            let mut artifact = validation_refused_artifact();
            artifact.created_at = Utc::now() - Duration::hours(hours);
            let dir = tempfile::TempDir::new().unwrap();
            let path = write_artifact(&dir, &artifact);
            let err = run_apply_command(&path, false, false, false, None, false).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_PLAN_ARTIFACT_STALE", 5);
            let want = format!(
                "plan 'orders' is {phrase} (limit: 24 h). Regenerate with `rivet plan` or pass --force to override."
            );
            assert!(format!("{err:#}").contains(&want), "{hours} h: {err:#}");
        }
    }

    // ── cursor drift ─────────────────────────────────────────────────────────

    /// A plan refused by validation if it ran, so a mutant that excuses the drift fails here at once instead of connecting to an unreachable source.
    #[test]
    fn cursor_drift_detected_no_prior_state() {
        let mut artifact = validation_refused_artifact();
        // Plan recorded a cursor snapshot; state store starts empty → drift.
        artifact.computed.cursor_snapshot = Some("2025-06-01T00:00:00Z".into());
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_artifact(&dir, &artifact);

        let err = run_apply_command(&path, false, false, false, None, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("drifted") || msg.contains("cursor"),
            "expected cursor drift error: {msg}"
        );
        crate::error::assert_coded(&err, "RIVET_PLAN_ARTIFACT_STALE", 5);
    }

    // ── replay of an applied plan (retry matrix F3) ──────────────────────────

    /// The state the plan's own apply left: run `r1` finished `status`, and recorded as the plan's run when it exited 0.
    fn applied_state(dir: &tempfile::TempDir, artifact: &PlanArtifact, status: &str, exit0: bool) {
        let state = StateStore::open(dir.path().join(".rivet_state.db").to_str().unwrap()).unwrap();
        state
            .begin_run("r1", "orders", "file:///out", "2026-01-01T00:00:00Z")
            .unwrap();
        state
            .finish_run("r1", status, "2026-01-01T00:01:00Z")
            .unwrap();
        state
            .record_metric_full(&crate::state::MetricRow {
                export_name: "orders".into(),
                run_id: "r1".into(),
                total_rows: 1000,
                status: status.into(),
                files_produced: 2,
                bytes_written: 4096,
                ..Default::default()
            })
            .unwrap();
        if exit0 {
            assert!(state.record_applied_plan("r1", &artifact.plan_id).unwrap());
        }
        state.update_legacy("orders", "1010").unwrap();
    }

    /// `(run_status rows, export_metrics rows)` in the state next to the plan.
    fn state_rows(dir: &tempfile::TempDir) -> (usize, usize) {
        let state = StateStore::open(dir.path().join(".rivet_state.db").to_str().unwrap()).unwrap();
        (
            state.recent_run_status(100, false).unwrap().len(),
            state.get_metrics(None, 100).unwrap().len(),
        )
    }

    /// An incremental cursor plan over `updated_at`.
    fn incremental() -> ExtractionStrategy {
        ExtractionStrategy::Incremental(crate::plan::IncrementalCursorPlan {
            primary_column: "updated_at".into(),
            fallback_column: None,
            mode: crate::config::IncrementalCursorMode::SingleColumn,
            settle: None,
        })
    }

    /// A keyset plan over `id`, continuing its stored key on a clean run when `incremental`.
    fn keyset(incremental: bool) -> ExtractionStrategy {
        ExtractionStrategy::Keyset(crate::plan::KeysetPlan {
            key_column: "id".into(),
            chunk_size: 1000,
            checkpoint: true,
            incremental,
            parallel: 1,
        })
    }

    /// Replayed after its own apply exited 0, a point-in-time plan answers with that run on every cycle and touches nothing.
    #[test]
    fn a_plan_that_applied_answers_every_replay_with_its_run_and_extracts_nothing() {
        for strategy in [ExtractionStrategy::Snapshot, keyset(false)] {
            let artifact = validation_refused_artifact_of(strategy.clone());
            let dir = tempfile::TempDir::new().unwrap();
            let path = write_artifact(&dir, &artifact);
            applied_state(&dir, &artifact, "success", true);
            let before = state_rows(&dir);
            for cycle in 1..=2 {
                run_apply_command(&path, false, false, false, None, false).unwrap_or_else(|e| {
                    panic!("{strategy:?}, replay {cycle}: already applied, got {e:#}")
                });
                assert_eq!(state_rows(&dir), before, "replay {cycle} wrote no run");
            }
        }
    }

    /// Replayed after its own apply exited 0, a reusable plan runs again on every cycle from the stored cursor, even when that apply moved the cursor its snapshot pinned and the plan is days old.
    #[test]
    fn a_reusable_plan_extracts_on_every_replay_from_the_stored_cursor() {
        for (strategy, snapshot) in [
            (incremental(), None),
            (incremental(), Some("1000")),
            (keyset(true), None),
        ] {
            let mut artifact = validation_refused_artifact_of(strategy.clone());
            artifact.computed.cursor_snapshot = snapshot.map(String::from);
            artifact.created_at = Utc::now() - Duration::hours(73);
            assert!(artifact.is_reusable(), "{strategy:?}");
            let dir = tempfile::TempDir::new().unwrap();
            let path = write_artifact(&dir, &artifact);
            applied_state(&dir, &artifact, "success", true);
            for cycle in 1..=2 {
                let err = run_apply_command(&path, false, false, false, None, false).unwrap_err();
                assert!(
                    format!("{err:#}").contains("max_file_size cannot be used with stdout"),
                    "{strategy:?}, snapshot {snapshot:?}, replay {cycle}: the replay must run the \
                     plan, got {err:#}"
                );
            }
        }
    }

    /// A reusable plan's snapshot cursor moved by a run that did not apply it, after or without an apply of it, is refused STALE.
    #[test]
    fn a_reusable_plan_whose_cursor_another_run_moved_is_refused_stale() {
        let mut artifact = validation_refused_artifact_of(incremental());
        artifact.computed.cursor_snapshot = Some("1000".into());
        for applied_first in [false, true] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = write_artifact(&dir, &artifact);
            applied_state(&dir, &artifact, "success", applied_first);
            if applied_first {
                let state =
                    StateStore::open(dir.path().join(".rivet_state.db").to_str().unwrap()).unwrap();
                state
                    .begin_run("r2", "orders", "file:///out", "2026-01-02T00:00:00Z")
                    .unwrap();
                state
                    .finish_run("r2", "success", "2026-01-02T00:01:00Z")
                    .unwrap();
                state.update_legacy("orders", "1020").unwrap();
            }
            let err = run_apply_command(&path, false, false, false, None, false).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_PLAN_ARTIFACT_STALE", 5);
            assert!(
                format!("{err:#}").contains("cursor has drifted"),
                "applied first {applied_first}: {err:#}"
            );
        }
    }

    /// A plan whose run is refused by plan validation, before it connects: proof that the replay ran it.
    fn validation_refused_artifact() -> PlanArtifact {
        validation_refused_artifact_of(ExtractionStrategy::Snapshot)
    }

    /// [`validation_refused_artifact`] with `strategy`.
    fn validation_refused_artifact_of(strategy: ExtractionStrategy) -> PlanArtifact {
        let mut plan = unreachable_plan();
        plan.strategy = strategy;
        plan.destination = DestinationConfig {
            destination_type: DestinationType::Stdout,
            ..Default::default()
        };
        plan.max_file_size_bytes = Some(1);
        let a = fresh_artifact();
        PlanArtifact::new(
            a.export_name,
            a.strategy,
            a.plan_fingerprint,
            plan,
            a.computed,
            a.diagnostics,
        )
    }

    /// A plan whose run did not exit 0, another plan's run, and `--force` still run the plan.
    #[test]
    fn only_an_exit_zero_apply_of_the_same_plan_answers_a_replay() {
        let artifact = validation_refused_artifact();
        let ran = |err: anyhow::Error, what: &str| {
            assert!(
                format!("{err:#}").contains("max_file_size cannot be used with stdout"),
                "{what}: the replay must run the plan, got {err:#}"
            );
        };
        for (status, exit0, force) in [
            ("failed", false, false),
            ("interrupted", false, false),
            ("running", true, false),
            ("success", true, true),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = write_artifact(&dir, &artifact);
            applied_state(&dir, &artifact, status, exit0);
            let err = run_apply_command(&path, force, false, false, None, false).unwrap_err();
            ran(err, &format!("{status}, exit0 {exit0}, force {force}"));
        }
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_artifact(&dir, &artifact);
        applied_state(&dir, &validation_refused_artifact(), "success", true);
        let err = run_apply_command(&path, false, false, false, None, false).unwrap_err();
        ran(err, "another plan's run");
    }

    /// The answer names the plan, the run and that run's outcome as its metrics row holds it, and the remedy.
    #[test]
    fn the_already_applied_report_names_the_plan_the_run_its_rows_and_the_remedy() {
        let artifact = validation_refused_artifact();
        let dir = tempfile::TempDir::new().unwrap();
        applied_state(&dir, &artifact, "success", true);
        let state = StateStore::open(dir.path().join(".rivet_state.db").to_str().unwrap()).unwrap();
        let line = already_applied_report(&state, &artifact, "r1");
        let (head, tail) = line.split_once(", finished ").expect(&line);
        assert_eq!(
            head,
            format!(
                "apply: plan {} of export 'orders' was already applied by run r1 (success, 1000 \
                 rows, 2 files, 4096 bytes",
                artifact.plan_id
            )
        );
        assert!(
            tail.ends_with(
                "); nothing was extracted again. To extract new data, make a new plan with `rivet plan`."
            ),
            "{line}"
        );
        assert!(
            already_applied_report(&state, &artifact, "r9")
                .contains("(its metrics row is not readable)")
        );
    }

    /// The run that applied a plan is found by it afterwards; a run id with no row is a warning, never a silent miss.
    #[test]
    fn an_exit_zero_apply_records_its_plan_and_a_missing_run_row_warns() {
        let artifact = validation_refused_artifact();
        let dir = tempfile::TempDir::new().unwrap();
        applied_state(&dir, &artifact, "success", false);
        let state = StateStore::open(dir.path().join(".rivet_state.db").to_str().unwrap()).unwrap();
        assert_eq!(
            state
                .run_that_applied_plan("orders", &artifact.plan_id)
                .unwrap(),
            None
        );
        assert_eq!(record_applied_plan(&state, &artifact, "r1"), None);
        assert_eq!(
            state
                .run_that_applied_plan("orders", &artifact.plan_id)
                .unwrap()
                .as_deref(),
            Some("r1")
        );
        let warning = record_applied_plan(&state, &artifact, "r9").expect("no row for r9");
        assert!(
            warning.contains("(no run-status row); a replay of this plan will extract again"),
            "{warning}"
        );
    }

    // ── file I/O errors ──────────────────────────────────────────────────────

    #[test]
    fn missing_plan_file_returns_error() {
        let err = run_apply_command(
            "/tmp/rivet_nonexistent_xyzxyz.json",
            false,
            false,
            false,
            None,
            false,
        )
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
        let err = run_apply_command(path.to_str().unwrap(), false, false, false, None, false)
            .unwrap_err();
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

        let err = run_apply_command(&path, false, false, false, None, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("integrity check failed") && msg.contains("modified after planning"),
            "tampered plan must be rejected at the integrity gate, got: {msg}"
        );

        // And --force must NOT override it — a hand-edited contract is not opt-in.
        let err_forced = run_apply_command(&path, true, false, false, None, false).unwrap_err();
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

    #[test]
    fn a_credential_stripped_plan_and_a_pooled_artifact_are_refused_by_code() {
        let mut a = fresh_artifact();
        let s = &mut a.resolved_plan.source;
        s.url = Some("postgresql://REDACTED@h/db".into());
        s.url_env = None;
        s.url_file = None;
        s.password_env = None;
        s.host = None;
        s.user = None;
        let err = reject_unrecoverable_inline_url(&a).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_PLAN_ARTIFACT_NO_CREDENTIALS", 1);

        let dir = tempfile::TempDir::new().unwrap();
        let path = write_artifact(&dir, &fresh_artifact());
        let err = run_apply_command(&path, false, false, false, Some(2), false).unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CLI_OPTION_UNSUPPORTED", 1);
    }

    /// The apply-side resolver: a sealed `{date}` becomes the apply's day with the template kept as the progress identity; a resolved path (a `--date` plan, an earlier release's artifact) is unchanged.
    #[test]
    fn a_sealed_date_resolves_to_the_apply_day_and_a_resolved_path_is_unchanged() {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        let ctx = crate::destination::placeholder::PlaceholderContext::for_today("orders");
        let mut kept = unreachable_plan();
        kept.destination.path = Some("/out/{date}".into());
        kept.resolve_destination(&ctx);
        assert_eq!(
            kept.destination.path.as_deref(),
            Some(format!("/out/{today}").as_str())
        );
        assert_eq!(
            kept.destination_written.as_deref(),
            Some("file:///out/{date}")
        );
        assert_eq!(
            kept.progress_key().landing.destination,
            "file:///out/{date}"
        );

        let mut frozen = unreachable_plan();
        frozen.destination.path = Some("/out/2001-02-03".into());
        frozen.destination_written = Some("file:///out/{date}".into());
        frozen.resolve_destination(&ctx);
        assert_eq!(frozen.destination.path.as_deref(), Some("/out/2001-02-03"));
        assert_eq!(
            frozen.destination_written.as_deref(),
            Some("file:///out/{date}")
        );

        let mut legacy = unreachable_plan();
        legacy.resolve_destination(&ctx);
        assert_eq!(
            legacy.destination.uri(),
            unreachable_plan().destination.uri()
        );
        assert_eq!(legacy.destination_written, None);
    }
}
