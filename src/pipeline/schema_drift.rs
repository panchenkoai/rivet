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

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str) -> SchemaColumn {
        SchemaColumn {
            name: name.into(),
            data_type: ty.into(),
        }
    }
    fn summary() -> RunSummary {
        RunSummary::stub_for_testing("run-1", "orders")
    }

    #[test]
    fn first_run_establishes_baseline_no_drift() {
        let st = StateStore::open_in_memory().unwrap();
        let mut s = summary();
        let cols = vec![col("id", "Int64"), col("name", "Utf8")];
        // No baseline yet → detect_schema_change establishes it, reports no change.
        check_and_persist(&st, "orders", &cols, SchemaDriftPolicy::Fail, &mut s).unwrap();
        assert_eq!(s.schema_changed, Some(false));
    }

    #[test]
    fn drift_under_fail_returns_err_and_flags_change() {
        let st = StateStore::open_in_memory().unwrap();
        let v1 = vec![col("id", "Int64")];
        check_and_persist(&st, "orders", &v1, SchemaDriftPolicy::Fail, &mut summary()).unwrap();
        // A new column appears on the next run.
        let v2 = vec![col("id", "Int64"), col("email", "Utf8")];
        let mut s2 = summary();
        let err = check_and_persist(&st, "orders", &v2, SchemaDriftPolicy::Fail, &mut s2)
            .expect_err("fail policy must abort on drift");
        assert!(
            format!("{err:#}").contains("schema drift detected"),
            "{err:#}"
        );
        assert_eq!(s2.schema_changed, Some(true));
    }

    #[test]
    fn drift_under_warn_stores_new_baseline_and_continues() {
        let st = StateStore::open_in_memory().unwrap();
        let v1 = vec![col("id", "Int64")];
        check_and_persist(&st, "orders", &v1, SchemaDriftPolicy::Warn, &mut summary()).unwrap();
        let v2 = vec![col("id", "Int64"), col("email", "Utf8")];
        let mut s2 = summary();
        check_and_persist(&st, "orders", &v2, SchemaDriftPolicy::Warn, &mut s2).unwrap();
        assert_eq!(s2.schema_changed, Some(true));
        // Warn updates the baseline → re-running v2 is now drift-free.
        let mut s3 = summary();
        check_and_persist(&st, "orders", &v2, SchemaDriftPolicy::Warn, &mut s3).unwrap();
        assert_eq!(s3.schema_changed, Some(false));
    }
}
