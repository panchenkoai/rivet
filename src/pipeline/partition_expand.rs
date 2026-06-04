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

    // I/O: the value span (SQL aggregates skip NULLs) + whether any NULLs exist.
    // The pure child-building below is split out so it is unit-testable without
    // a live source.
    let bounds = fetch_value_span(src.as_mut(), export, col, &base_query, st)?;
    let has_nulls = src
        .query_scalar(&partition::build_null_count_query(&base_query, col, st))?
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
        > 0;

    let children = build_partition_children(export, col, &base_query, st, bounds, has_nulls);
    if children.is_empty() {
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
            children.len()
        );
        out.extend(children);
    }
    Ok(())
}

/// Fetch the `[min, max]` day span of the partition column over `base_query`,
/// or `None` when there are no non-NULL rows. The NULL bucket is probed
/// separately by the caller.
fn fetch_value_span(
    src: &mut dyn crate::source::Source,
    export: &ExportConfig,
    col: &str,
    base_query: &str,
    st: crate::config::SourceType,
) -> Result<Option<(chrono::NaiveDate, chrono::NaiveDate)>> {
    let min_raw = src.query_scalar(&crate::sql::aggregate_sql(st, "min", col, base_query))?;
    let max_raw = src.query_scalar(&crate::sql::aggregate_sql(st, "max", col, base_query))?;
    let (Some(min_s), Some(max_s)) = (min_raw.as_deref(), max_raw.as_deref()) else {
        return Ok(None);
    };
    let parse = |raw: &str, which: &str| {
        crate::sql::parse_date_flexible(raw).ok_or_else(|| {
            anyhow::anyhow!(
                "export '{}': could not parse partition {} '{}' from column '{}' as a date",
                export.name,
                which,
                raw,
                col
            )
        })
    };
    Ok(Some((parse(min_s, "min")?, parse(max_s, "max")?)))
}

/// Pure: build the concrete per-bucket child exports from an already-fetched
/// value span and NULL flag. No I/O — this is the unit-tested heart of the
/// expansion (range generation, child synthesis, the NULL bucket).
fn build_partition_children(
    parent: &ExportConfig,
    col: &str,
    base_query: &str,
    st: crate::config::SourceType,
    bounds: Option<(chrono::NaiveDate, chrono::NaiveDate)>,
    has_nulls: bool,
) -> Vec<ExportConfig> {
    let mut children = Vec::new();
    if let Some((min_day, max_day)) = bounds {
        for range in partition::generate_ranges(min_day, max_day, parent.partition_granularity) {
            let query = partition::build_range_query(base_query, col, &range, st);
            children.push(make_child(parent, col, &range.label_value, &range.label_value, query));
        }
    }
    // NULL bucket: a range predicate can never match NULL, so any NULL rows
    // would be lost without this. `__HIVE_DEFAULT_PARTITION__` keeps them
    // queryable by Hive/Spark/duckdb partition discovery.
    if has_nulls {
        let query = partition::build_null_query(base_query, col, st);
        children.push(make_child(parent, col, HIVE_NULL_PARTITION, "null", query));
    }
    children
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
    if export.chunk_by_key.is_some() {
        // Keyset needs the `table:` shortcut so the planner can verify the key
        // is index-backed; partitioning rewrites the export into a `query:`
        // subquery (the date predicate), which keyset refuses. Fail fast with a
        // clear message instead of a per-partition keyset error.
        anyhow::bail!(
            "export '{}': partition_by is not compatible with chunk_by_key — keyset requires the \
             `table:` shortcut to verify the index, but partitioning rewrites the query. Use a range \
             `chunk_column` (dense/correlated key), a smaller `partition_granularity`, or `mode: full`.",
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
    fn rejects_chunk_by_key() {
        // Keyset needs `table:` introspection; partitioning rewrites into a
        // `query:` subquery, so the two cannot compose — fail fast.
        let mut e = part_export(true);
        e.mode = ExportMode::Chunked;
        e.chunk_by_key = Some("id".into());
        let err = validate_partitionable(&e, "created_at").unwrap_err();
        assert!(err.to_string().contains("chunk_by_key"), "got: {err}");
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

    // ── build_partition_children (pure expansion core, offline) ──────────────

    fn day(s: &str) -> chrono::NaiveDate {
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn child_names(children: &[ExportConfig]) -> Vec<String> {
        children.iter().map(|c| c.name.clone()).collect()
    }

    #[test]
    fn children_one_per_day_plus_null_bucket() {
        let parent = part_export(true);
        let children = build_partition_children(
            &parent,
            "created_at",
            "SELECT * FROM events",
            crate::config::SourceType::Postgres,
            Some((day("2023-01-01"), day("2023-01-03"))),
            true,
        );
        assert_eq!(
            child_names(&children),
            [
                "events__2023-01-01",
                "events__2023-01-02",
                "events__2023-01-03",
                "events__null",
            ]
        );
        // last is the NULL bucket → Hive default partition path.
        assert_eq!(
            children.last().unwrap().destination.path.as_deref(),
            Some("./out/events/created_at=__HIVE_DEFAULT_PARTITION__")
        );
    }

    #[test]
    fn children_no_null_bucket_when_no_nulls() {
        let parent = part_export(true);
        let children = build_partition_children(
            &parent,
            "created_at",
            "SELECT * FROM events",
            crate::config::SourceType::Postgres,
            Some((day("2023-01-01"), day("2023-01-02"))),
            false,
        );
        assert_eq!(child_names(&children), ["events__2023-01-01", "events__2023-01-02"]);
    }

    #[test]
    fn children_only_null_bucket_when_no_value_span() {
        let parent = part_export(true);
        let children = build_partition_children(
            &parent,
            "created_at",
            "SELECT * FROM events",
            crate::config::SourceType::Postgres,
            None,
            true,
        );
        assert_eq!(child_names(&children), ["events__null"]);
    }

    #[test]
    fn children_empty_when_no_rows_at_all() {
        let parent = part_export(true);
        let children = build_partition_children(
            &parent,
            "created_at",
            "SELECT * FROM events",
            crate::config::SourceType::Postgres,
            None,
            false,
        );
        assert!(children.is_empty());
    }
}
