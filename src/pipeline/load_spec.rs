use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, ExportConfig};
use crate::error::Result;
use crate::state::{LoadSpecColumn, StateStore};

/// Record what `rivet load` plans from after a successful run of an export in a
/// config with a `load:` block; a failed capture warns and keeps the previous record.
pub(super) fn record_after_run(
    config: &Config,
    export: &ExportConfig,
    state: &StateStore,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    run_id: &str,
) {
    if config.load.is_none() || export.snapshot_parent.is_some() {
        return;
    }
    if let Err(e) = capture_and_record(config, export, state, config_dir, params, run_id) {
        log::warn!(
            "export '{}': could not record the load spec for `rivet load` ({e:#}); \
             the load plans from the spec an earlier run recorded, if any",
            export.name
        );
    }
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
