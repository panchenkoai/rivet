//! Schema-drift detection + baseline persistence — the runner-write facade for
//! `on_schema_drift` (ADR-0021), the third alongside `commit::record_part` and
//! `run_store::RunStore` (ADR-0018; that ADR's claim that drift "does not
//! generalize across modes" is what this module disproves).
//!
//! One deep core ([`check_and_persist`]: detect → policy → store) behind two
//! column-source **adapters**:
//!   - [`check_from_sink_schema`] — single mode, post-write, from the sink's
//!     data-derived Arrow schema.
//!   - [`check_from_type_mappings`] — chunked mode, pre-chunk, from a scan-free
//!     `type_mappings` probe, so `on_schema_drift: fail` aborts before any chunk
//!     is written.
//!
//! Both produce the *same* canonical `SchemaColumn` shape (via
//! `arrow_schema_to_columns`), so a baseline is comparable across modes.

use crate::config::SchemaDriftPolicy;
use crate::error::{Result, SchemaDriftError};
use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::state::{SchemaColumn, StateStore};

use super::summary::RunSummary;

/// Adapter — single mode: columns from the sink's resolved (data-derived) schema.
pub(super) fn check_from_sink_schema(
    state: &StateStore,
    export_name: &str,
    sink_schema: &arrow::datatypes::Schema,
    policy: SchemaDriftPolicy,
    summary: &mut RunSummary,
) -> Result<()> {
    let columns = crate::state::arrow_schema_to_columns(sink_schema);
    // A ZERO-ROW run resolves an EMPTY schema — the sink says so itself
    // ("empty-schema fallbacks (zero-row runs)", sink/mod.rs::on_schema) — and an
    // empty column set is not drift, it is the absence of evidence. Without this
    // guard the run diffs [] against the baseline, reports EVERY column removed,
    // and under `Warn` STORES the empty set as the new baseline; the next real
    // run then reports every column ADDED. Under `Fail` it aborts an export that
    // simply had nothing to export.
    //
    // MEASURED 2026-09-21 on a config `rivet init` wrote: all five exports ended
    // with `export_schema.columns_json = '[]'`, each stamped at its first
    // zero-row cycle, and the next run with two real rows logged
    // "added: id, name, email, age, balance, is_active, bio, created_at,
    // updated_at".
    //
    // The sibling adapter `check_from_type_mappings` has carried this guard all
    // along; this one did not, and the asymmetry IS the defect.
    if columns.is_empty() {
        summary.schema_changed.get_or_insert(false);
        return Ok(());
    }
    check_and_persist(state, export_name, &columns, policy, summary)
}

/// Adapter — chunked mode: columns from a scan-free `type_mappings` probe, run
/// **pre-chunk** so `on_schema_drift: fail` aborts before any chunk is written
/// (ADR-0021). Schema-resolution failures are non-fatal (logged) — drift is
/// advisory infra and must not fail an otherwise-healthy run.
pub(super) fn check_from_type_mappings(
    src: &mut dyn crate::source::Source,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
) -> Result<()> {
    let mappings = match src.type_mappings(&plan.base_query, &plan.column_overrides) {
        Ok(m) => m,
        Err(e) => {
            log::warn!(
                "export '{}': could not resolve schema for drift check (skipping): {e:#}",
                plan.export_name
            );
            // Mark the facade as HAVING RUN (no drift detectable) so the run-level
            // bypass guard (check_post_run_invariants) distinguishes "gate ran, could
            // not resolve" from "gate never called" (schema_changed stays None).
            summary.schema_changed.get_or_insert(false);
            return Ok(());
        }
    };
    let fields: Vec<arrow::datatypes::Field> = mappings
        .iter()
        .filter_map(crate::types::build_arrow_field)
        .collect();
    if fields.is_empty() {
        summary.schema_changed.get_or_insert(false);
        return Ok(());
    }
    let columns = crate::state::arrow_schema_to_columns(&arrow::datatypes::Schema::new(fields));
    check_and_persist(
        state,
        &plan.export_name,
        &columns,
        plan.schema_drift_policy,
        summary,
    )
}

/// Adapter — CDC: one captured table's probed columns, judged before any change is read.
pub(super) fn check_from_cdc_mappings(
    state: &StateStore,
    key: &str,
    mappings: &[crate::types::TypeMapping],
    policy: SchemaDriftPolicy,
) -> Result<()> {
    let fields: Vec<arrow::datatypes::Field> = mappings
        .iter()
        .filter_map(crate::types::build_arrow_field)
        .collect();
    if fields.is_empty() {
        return Ok(());
    }
    match state.get_stored_schema(key) {
        Ok(Some(stored)) => {
            if let Some(migrated) = migrate_upgrade_labels(&stored, mappings)
                && let Err(e) = state.store_schema(key, &migrated)
            {
                log::warn!("schema drift: could not migrate the baseline of '{key}': {e:#}");
            }
        }
        Ok(None) => {}
        Err(e) => log::warn!("schema drift: could not read the baseline of '{key}': {e:#}"),
    }
    let columns = crate::state::arrow_schema_to_columns(&arrow::datatypes::Schema::new(fields));
    check_and_persist(state, key, &columns, policy, &mut RunSummary::default())
}

/// True when `new` differs from its baseline entry only because rivet now labels it server text.
///
/// Before ADR-0038 the CDC sink wrote such a column as unlabelled Utf8 and the
/// baseline recorded the planned type instead: nothing for an Unsupported type,
/// the unbuildable Arrow type (e.g. `List(Date32)`) otherwise.
pub(super) fn is_upgrade_label_migration(
    old: Option<&SchemaColumn>,
    new: &crate::types::TypeMapping,
) -> bool {
    use crate::types::{Delivery, TextForm, rivet_type_to_arrow};
    new.delivery == Delivery::Text(TextForm::ServerText)
        && old.map(|c| c.data_type.clone())
            == rivet_type_to_arrow(&new.rivet_type).map(|dt| format!("{dt:?}"))
}

/// The baseline with every upgrade-label migration applied (one info line each), or `None` when nothing moved.
fn migrate_upgrade_labels(
    stored: &[SchemaColumn],
    mappings: &[crate::types::TypeMapping],
) -> Option<Vec<SchemaColumn>> {
    let mut out = stored.to_vec();
    let mut moved = false;
    for m in mappings {
        let at = out.iter().position(|c| c.name == m.column_name);
        if !is_upgrade_label_migration(at.map(|i| &out[i]), m) {
            continue;
        }
        let Some(field) = crate::types::build_arrow_field(m) else {
            continue;
        };
        let col = SchemaColumn {
            name: m.column_name.clone(),
            data_type: format!("{:?}", field.data_type()),
        };
        log::info!(
            "schema drift: column '{}' ({}) is now delivered as server text; the baseline \
             from before ADR-0038 did not record it that way, so it is updated without \
             reporting drift",
            m.column_name,
            m.source_native_type
        );
        match at {
            Some(i) => out[i] = col,
            None => out.push(col),
        }
        moved = true;
    }
    moved.then_some(out)
}

/// Deep core (private): detect drift of `columns` against the stored baseline for
/// `export_name` and act per `policy`.
///
/// - First run (no baseline): `detect_schema_change` establishes it and returns
///   "no change" — `schema_changed = Some(false)`.
/// - Drift under `Continue`/`Warn`: log (Warn only), update the stored baseline,
///   continue.
/// - Drift under `Fail`: log and return `Err(SchemaDriftError)` — the caller
///   treats this as an abort (in chunked mode this happens **before** any chunk
///   writes; see ADR-0021).
/// - Tracking error: logged at warn, non-fatal (drift is advisory infra).
fn check_and_persist(
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
        Err(e) => {
            log::warn!("schema tracking error: {e:#}");
            // The gate RAN (tracking just failed) — record that so the run-level
            // bypass guard doesn't read a tracking error as "gate never called".
            summary.schema_changed.get_or_insert(false);
        }
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

    fn mapping(name: &str, native: &str, t: crate::types::RivetType) -> crate::types::TypeMapping {
        crate::types::TypeMapping::from_source(
            &crate::types::SourceColumn::simple(name, native, true),
            t,
        )
    }

    fn server_text(
        name: &str,
        native: &str,
        t: crate::types::RivetType,
    ) -> crate::types::TypeMapping {
        mapping(name, native, t).with_text(crate::types::TextForm::ServerText)
    }

    fn bare_numeric() -> crate::types::RivetType {
        crate::types::RivetType::Unsupported {
            native_type: "numeric".into(),
            reason: "no precision".into(),
        }
    }

    fn date_list() -> crate::types::RivetType {
        crate::types::RivetType::List {
            inner: Box::new(crate::types::RivetType::Date),
        }
    }

    /// The label migration covers exactly what 0.30.0 recorded for an unbuildable column, nothing else.
    #[test]
    fn upgrade_label_migration_matches_only_what_the_old_baseline_recorded() {
        let n = server_text("n", "numeric", bare_numeric());
        assert!(
            is_upgrade_label_migration(None, &n),
            "0.30.0 recorded nothing"
        );
        assert!(!is_upgrade_label_migration(Some(&col("n", "Utf8")), &n));
        assert!(!is_upgrade_label_migration(Some(&col("n", "Int64")), &n));

        let d = server_text("d", "date[]", date_list());
        let old_list = format!(
            "{:?}",
            crate::types::rivet_type_to_arrow(&date_list()).unwrap()
        );
        assert_eq!(
            old_list, "List(Field { data_type: Date32, nullable: true })",
            "what 0.30.0 stored, measured with its release binary"
        );
        assert!(is_upgrade_label_migration(Some(&col("d", &old_list)), &d));
        assert!(
            !is_upgrade_label_migration(None, &d),
            "a List column was recorded"
        );

        let native = mapping("v", "bigint", crate::types::RivetType::Int64);
        assert!(
            !is_upgrade_label_migration(None, &native),
            "a new native column drifts"
        );
        let text = mapping("s", "text", crate::types::RivetType::String);
        assert!(!is_upgrade_label_migration(None, &text));
    }

    /// Under `fail`, a 0.30.0 baseline takes the server-text column silently; a genuinely new column still fails.
    #[test]
    fn cdc_gate_migrates_an_old_baseline_silently_and_still_fails_a_new_column() {
        let st = StateStore::open_in_memory().unwrap();
        let old_list = format!(
            "{:?}",
            crate::types::rivet_type_to_arrow(&date_list()).unwrap()
        );
        st.store_schema("k", &[col("id", "Int64"), col("d", &old_list)])
            .unwrap();
        let mut cols = vec![
            mapping("id", "bigint", crate::types::RivetType::Int64),
            server_text("n", "numeric", bare_numeric()),
            server_text("d", "date[]", date_list()),
        ];
        check_from_cdc_mappings(&st, "k", &cols, SchemaDriftPolicy::Fail).unwrap();
        let stored = st.get_stored_schema("k").unwrap().unwrap();
        assert!(stored.contains(&col("n", "Utf8")), "{stored:?}");
        assert!(stored.contains(&col("d", "Utf8")), "{stored:?}");

        cols.push(mapping("extra", "bigint", crate::types::RivetType::Int64));
        let err = check_from_cdc_mappings(&st, "k", &cols, SchemaDriftPolicy::Fail)
            .expect_err("a genuinely new column still drifts");
        assert!(
            format!("{err:#}").contains("schema drift detected"),
            "{err:#}"
        );
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

    /// A zero-row run must not DESTROY the baseline. Measured before it was
    /// written: three cycles of a generated incremental config left every one of
    /// five exports at `columns_json = '[]'`, each stamped at that export's first
    /// zero-row cycle, and the next run carrying two real rows then logged
    /// "added: <every column>".
    ///
    /// Both halves, because either alone passes a broken build: the baseline must
    /// SURVIVE, and the next real run must see NO drift.
    /// RED against removing the `columns.is_empty()` guard in
    /// `check_from_sink_schema`.
    #[test]
    fn a_zero_row_run_neither_wipes_the_baseline_nor_invents_drift() {
        use arrow::datatypes::{DataType, Field, Schema};
        let st = StateStore::open_in_memory().unwrap();
        let real = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, true),
        ]);
        check_from_sink_schema(
            &st,
            "orders",
            &real,
            SchemaDriftPolicy::Warn,
            &mut summary(),
        )
        .unwrap();

        // The zero-row run: an empty resolved schema.
        let empty = Schema::empty();
        let mut s_empty = summary();
        check_from_sink_schema(&st, "orders", &empty, SchemaDriftPolicy::Warn, &mut s_empty)
            .unwrap();
        assert_eq!(
            s_empty.schema_changed,
            Some(false),
            "an empty schema is the absence of evidence, not drift"
        );
        assert_eq!(
            st.get_stored_schema("orders").unwrap().map(|c| c.len()),
            Some(2),
            "the zero-row run must not overwrite the baseline with []"
        );

        // The next REAL run sees the unchanged schema and reports no drift.
        let mut s_next = summary();
        check_from_sink_schema(&st, "orders", &real, SchemaDriftPolicy::Warn, &mut s_next).unwrap();
        assert_eq!(
            s_next.schema_changed,
            Some(false),
            "the baseline survived, so the next real run is drift-free"
        );
    }

    /// The same shape under `fail`: a scheduled run with nothing to export must
    /// not ABORT the export. RED against removing the guard — without it this
    /// returns `Err`, which is a legitimate no-op failing the run.
    #[test]
    fn a_zero_row_run_does_not_abort_under_on_schema_drift_fail() {
        use arrow::datatypes::{DataType, Field, Schema};
        let st = StateStore::open_in_memory().unwrap();
        let real = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        check_from_sink_schema(
            &st,
            "orders",
            &real,
            SchemaDriftPolicy::Fail,
            &mut summary(),
        )
        .unwrap();

        let empty = Schema::empty();
        let mut s = summary();
        check_from_sink_schema(&st, "orders", &empty, SchemaDriftPolicy::Fail, &mut s)
            .expect("a run with no rows is not schema drift and must not abort");
        assert_eq!(s.schema_changed, Some(false));
    }
}
