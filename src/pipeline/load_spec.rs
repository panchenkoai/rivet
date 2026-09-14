use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::pipeline::summary::RunSummary;
use crate::state::{LoadSpecColumn, StateStore};

/// Record what `rivet load` plans from after a successful run of an export in a
/// any run: the columns it resolved at open and the key it
/// read then; a run whose open probe failed (or a CDC multiplex export, which has
/// no single probe) is captured from the source. A failed capture warns and keeps
/// the previous record.
pub(super) fn record_after_run(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    summary: &RunSummary,
) {
    if export.snapshot_parent.is_some() {
        return;
    }
    let recorded = match &summary.open_mappings {
        Some(mappings) => record_from_run(
            export,
            state,
            mappings,
            summary.open_primary_key.as_deref(),
            &summary.run_id,
        ),
        // Capturing costs a second source connection, so pay it only for a configured load.
        None if config.load.is_none() => return,
        None => capture_and_record(config, export, state, config_dir, params, &summary.run_id),
    };
    if let Err(e) = recorded {
        log::warn!(
            "export '{}': could not record the load spec for `rivet load` ({e:#}); \
             the load plans from the spec an earlier run recorded, if any",
            export.name
        );
    }
}

/// The spec from what the run itself resolved — no second source connection.
fn record_from_run(
    export: &ExportConfig,
    state: &StateStore,
    mappings: &[crate::types::TypeMapping],
    primary_key: Option<&[String]>,
    run_id: &str,
) -> Result<()> {
    let columns: Vec<LoadSpecColumn> = mappings.iter().map(LoadSpecColumn::from_mapping).collect();
    state.record_load_spec(&export.name, None, &columns, primary_key, run_id)
}

fn capture_and_record(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    run_id: &str,
) -> Result<()> {
    let overrides = crate::plan::parse_column_overrides_pub(&export.columns, &export.name)?;
    let units = crate::preflight::type_report::capture_load_units(
        config, export, &overrides, config_dir, params,
    )?;
    for unit in units {
        let columns: Vec<LoadSpecColumn> = unit
            .mappings
            .iter()
            .map(LoadSpecColumn::from_mapping)
            .collect();
        state.record_load_spec(
            &export.name,
            unit.table.as_deref(),
            &columns,
            unit.primary_key.as_deref(),
            run_id,
        )?;
    }
    Ok(())
}
