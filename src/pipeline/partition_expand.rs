//! Value-based output partitioning expansion (`partition_by`).
//!
//! `run` is the only command that materialises partitions. Right after the
//! export selection, [`expand_partitioned_exports`] rewrites the borrowed
//! `&ExportConfig` list into an **owned** list where every export with
//! `partition_by` set has been replaced by one concrete child export per
//! bucket — each with the bucket predicate wrapped into its query and the
//! `{partition}` token resolved to a Hive `col=value` segment in its
//! destination. Everything downstream (`run`'s loop, parallelism, manifest,
//! `validate`) then sees ordinary exports and needs no partition awareness —
//! which is why each partition gets its own manifest + `_SUCCESS` for free.
//!
//! The bucketing math and SQL builders are pure (`crate::plan::partition`);
//! this module owns the source round-trip (min/max + NULL probe) and the
//! `ExportConfig` synthesis.

use std::collections::HashMap;
use std::path::Path;

use crate::config::{ExportConfig, ExportMode, SourceConfig};
use crate::destination::placeholder;
use crate::error::Result;
use crate::plan::partition::{self, HIVE_NULL_PARTITION};

/// Replace every `partition_by` export in `selected` with its per-bucket child
/// exports; pass non-partitioned exports through unchanged (cloned).
///
/// Connects to the source once per partitioned export to read the partition
/// column's `[min, max]` span and whether any NULLs exist. An empty source
/// (no value rows and no NULLs) yields zero children for that export and logs
/// a warning rather than failing the whole run.
pub fn expand_partitioned_exports(
    selected: &[&ExportConfig],
    source: &SourceConfig,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
) -> Result<Vec<ExportConfig>> {
    let mut out: Vec<ExportConfig> = Vec::with_capacity(selected.len());
    for export in selected {
        match export.partition_by.as_deref() {
            None => out.push((*export).clone()),
            Some(col) => expand_one(export, col, source, config_dir, params, &mut out)?,
        }
    }
    Ok(out)
}

/// True when any export in the slice requests partitioning — used by `run` to
/// disable process-mode parallelism (child processes re-load the config from
/// disk and would not see the synthesised child names).
pub fn any_partitioned(selected: &[&ExportConfig]) -> bool {
    selected.iter().any(|e| e.partition_by.is_some())
}

fn expand_one(
    export: &ExportConfig,
    col: &str,
    source: &SourceConfig,
    config_dir: &Path,
    params: Option<&HashMap<String, String>>,
    out: &mut Vec<ExportConfig>,
) -> Result<()> {
    validate_partitionable(export, col)?;

    let base_query = export.resolve_query(config_dir, params)?;
    let st = source.source_type;

    let mut src = crate::source::create_source(source)?;

    // Span of the value (non-NULL) rows. SQL aggregates skip NULLs, so this is
    // the value span; the NULL bucket is probed independently below.
    let min_raw = src.query_scalar(&partition::build_min_query(&base_query, col, st))?;
    let max_raw = src.query_scalar(&partition::build_max_query(&base_query, col, st))?;

    let mut child_count = 0usize;
    if let (Some(min_s), Some(max_s)) = (min_raw.as_deref(), max_raw.as_deref()) {
        let min_day = partition::parse_scalar_day(min_s).ok_or_else(|| {
            anyhow::anyhow!(
                "export '{}': could not parse partition min '{}' from column '{}' as a date",
                export.name,
                min_s,
                col
            )
        })?;
        let max_day = partition::parse_scalar_day(max_s).ok_or_else(|| {
            anyhow::anyhow!(
                "export '{}': could not parse partition max '{}' from column '{}' as a date",
                export.name,
                max_s,
                col
            )
        })?;

        let ranges = partition::generate_ranges(min_day, max_day, export.partition_granularity);
        for range in &ranges {
            let query = partition::build_range_query(&base_query, col, range, st);
            out.push(make_child(export, col, &range.label_value, &range.label_value, query));
            child_count += 1;
        }
    }

    // NULL bucket: a range predicate can never match NULL, so any NULL rows
    // would be lost without this. `__HIVE_DEFAULT_PARTITION__` keeps them
    // queryable by Hive/Spark/duckdb partition discovery.
    let null_count = src
        .query_scalar(&partition::build_null_count_query(&base_query, col, st))?
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    if null_count > 0 {
        let query = partition::build_null_query(&base_query, col, st);
        out.push(make_child(export, col, HIVE_NULL_PARTITION, "null", query));
        child_count += 1;
    }

    if child_count == 0 {
        log::warn!(
            "export '{}': partition_by '{}' found no rows (no value span, no NULLs) — nothing to export",
            export.name,
            col
        );
    } else {
        log::info!(
            "export '{}': partition_by '{}' expanded into {} partition(s)",
            export.name,
            col,
            child_count
        );
    }
    Ok(())
}

/// Build one concrete child export for a single bucket.
///
/// - `path_value` is the value side of the Hive segment (`2023-01-01` or
///   `__HIVE_DEFAULT_PARTITION__`); the segment is `col=path_value`.
/// - `name_value` is a filename-safe suffix for the child export name (used in
///   state keys and output filenames).
fn make_child(
    parent: &ExportConfig,
    col: &str,
    path_value: &str,
    name_value: &str,
    query: String,
) -> ExportConfig {
    let mut child = parent.clone();
    // Stop any recursion and detach from the base query forms.
    child.partition_by = None;
    child.name = format!("{}__{}", parent.name, name_value);
    child.query = Some(query);
    child.query_file = None;
    child.table = None;
    let segment = format!("{col}={path_value}");
    child.destination = placeholder::expand_destination_partition(parent.destination.clone(), &segment);
    child
}

/// Up-front rules that make partitioning safe; checked before any source I/O.
fn validate_partitionable(export: &ExportConfig, col: &str) -> Result<()> {
    if col.trim().is_empty() {
        anyhow::bail!("export '{}': partition_by must name a column", export.name);
    }
    if export.mode == ExportMode::TimeWindow {
        anyhow::bail!(
            "export '{}': partition_by is not compatible with mode: time_window \
             (time_window already filters by a rolling window; partition a full/chunked/incremental export instead)",
            export.name
        );
    }
    let has_token = export
        .destination
        .path
        .as_deref()
        .is_some_and(|s| s.contains("{partition}"))
        || export
            .destination
            .prefix
            .as_deref()
            .is_some_and(|s| s.contains("{partition}"));
    if !has_token {
        anyhow::bail!(
            "export '{}': partition_by requires a '{{partition}}' token in destination.path or \
             destination.prefix (otherwise every partition would overwrite the same prefix)",
            export.name
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType, PartitionGranularity};

    fn part_export(token_in_path: bool) -> ExportConfig {
        let mut e = ExportConfig {
            name: "events".into(),
            query: Some("SELECT * FROM events".into()),
            ..base_export()
        };
        e.partition_by = Some("created_at".into());
        e.partition_granularity = PartitionGranularity::Day;
        e.destination = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(if token_in_path {
                "./out/events/{partition}".into()
            } else {
                "./out/events".into()
            }),
            ..Default::default()
        };
        e
    }

    // Minimal export built via the YAML path so we don't duplicate the full
    // field list (see the `sample_export` consolidation note).
    fn base_export() -> ExportConfig {
        serde_yaml_ng::from_str(
            "name: x\nquery: \"SELECT 1\"\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n",
        )
        .expect("parse base ExportConfig")
    }

    #[test]
    fn rejects_missing_partition_token() {
        let e = part_export(false);
        let err = validate_partitionable(&e, "created_at").unwrap_err();
        assert!(err.to_string().contains("{partition}"), "got: {err}");
    }

    #[test]
    fn accepts_partition_token_in_path() {
        let e = part_export(true);
        assert!(validate_partitionable(&e, "created_at").is_ok());
    }

    #[test]
    fn rejects_time_window_mode() {
        let mut e = part_export(true);
        e.mode = ExportMode::TimeWindow;
        let err = validate_partitionable(&e, "created_at").unwrap_err();
        assert!(err.to_string().contains("time_window"), "got: {err}");
    }

    #[test]
    fn make_child_resolves_segment_and_detaches() {
        let parent = part_export(true);
        let child = make_child(
            &parent,
            "created_at",
            "2023-01-03",
            "2023-01-03",
            "SELECT * FROM (SELECT * FROM events) AS _rivet_part WHERE x".into(),
        );
        assert_eq!(child.name, "events__2023-01-03");
        assert!(child.partition_by.is_none());
        assert!(child.table.is_none());
        assert_eq!(
            child.destination.path.as_deref(),
            Some("./out/events/created_at=2023-01-03")
        );
    }

    #[test]
    fn make_child_null_bucket_uses_hive_default() {
        let parent = part_export(true);
        let child = make_child(&parent, "created_at", HIVE_NULL_PARTITION, "null", "Q".into());
        assert_eq!(child.name, "events__null");
        assert_eq!(
            child.destination.path.as_deref(),
            Some("./out/events/created_at=__HIVE_DEFAULT_PARTITION__")
        );
    }

    #[test]
    fn any_partitioned_detects() {
        let p = part_export(true);
        let np = base_export();
        assert!(any_partitioned(&[&p]));
        assert!(!any_partitioned(&[&np]));
    }
}
