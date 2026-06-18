//! Shared schema-drift detection + baseline persistence (ADR-0021).
//!
//! Single mode calls this **post-write** from the sink's resolved schema;
//! chunked mode calls it **pre-chunk** from a `type_mappings`-resolved schema so
//! that `on_schema_drift: fail` aborts before any chunk is written. Keeping the
//! policy in one place guarantees both paths behave identically.

use crate::config::SchemaDriftPolicy;
use crate::error::{Result, SchemaDriftError};
use crate::journal::RunEvent;
use crate::state::{SchemaColumn, StateStore};

use super::summary::RunSummary;

/// Detect drift of `columns` against the stored baseline for `export_name` and
/// act per `policy`.
///
/// - First run (no baseline): `detect_schema_change` establishes it and returns
///   "no change" — `schema_changed = Some(false)`.
/// - Drift under `Continue`/`Warn`: log (Warn only), update the stored baseline,
///   continue.
/// - Drift under `Fail`: log and return `Err(SchemaDriftError)` — the caller
///   treats this as an abort (in chunked mode this happens **before** any chunk
///   writes; see ADR-0021).
/// - Tracking error: logged at warn, non-fatal (drift is advisory infra).
pub(super) fn check_and_persist(
    state: &StateStore,
    export_name: &str,
    columns: &[SchemaColumn],
    policy: SchemaDriftPolicy,
    summary: &mut RunSummary,
) -> Result<()> {
    match state.detect_schema_change(export_name, columns) {
        Ok(Some(change)) => {
            summary.schema_changed = Some(true);
            summary.journal.record(RunEvent::SchemaChanged {
                added: change.added.clone(),
                removed: change.removed.clone(),
                type_changed: change.type_changed.clone(),
            });
            match policy {
                SchemaDriftPolicy::Continue => {
                    if let Err(e) = state.store_schema(export_name, columns) {
                        log::warn!("export '{export_name}': schema store update failed: {e:#}");
                    }
                }
                SchemaDriftPolicy::Warn => {
                    log::warn!("export '{export_name}': schema changed!");
                    if !change.added.is_empty() {
                        log::warn!("  added: {}", change.added.join(", "));
                    }
                    if !change.removed.is_empty() {
                        log::warn!("  removed: {}", change.removed.join(", "));
                    }
                    for (col, old, new) in &change.type_changed {
                        log::warn!("  type changed: {col} ({old} → {new})");
                    }
                    if let Err(e) = state.store_schema(export_name, columns) {
                        log::warn!("export '{export_name}': schema store update failed: {e:#}");
                    }
                }
                SchemaDriftPolicy::Fail => {
                    log::error!(
                        "export '{export_name}': schema drift detected — aborting (on_schema_drift: fail)"
                    );
                    if !change.added.is_empty() {
                        log::error!("  added: {}", change.added.join(", "));
                    }
                    if !change.removed.is_empty() {
                        log::error!("  removed: {}", change.removed.join(", "));
                    }
                    for (col, old, new) in &change.type_changed {
                        log::error!("  type changed: {col} ({old} → {new})");
                    }
                    return Err(SchemaDriftError::new(format!(
                        "schema drift detected for export '{export_name}': \
                         {} column(s) added, {} removed, {} retyped — \
                         set `on_schema_drift: warn` to accept, or fix the schema mismatch",
                        change.added.len(),
                        change.removed.len(),
                        change.type_changed.len()
                    ))
                    .into());
                }
            }
        }
        Ok(None) => summary.schema_changed = Some(false),
        Err(e) => log::warn!("schema tracking error: {e:#}"),
    }
    Ok(())
}
