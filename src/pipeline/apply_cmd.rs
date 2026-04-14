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
