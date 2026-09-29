//! Value-based output partitioning expansion (`partition_by`).
//!
//! `run` is the only command that materialises partitions. Right after the
//! export selection, [`expand_partitioned_exports`] rewrites the borrowed
//! `&ExportConfig` list into an **owned** list where every export with
//! `partition_by` set has been replaced by one concrete child export per
//! bucket — each carrying its bucket as `partition_window` (the planner wraps
//! it around the base query, so `table:` survives) and the `{partition}` token
//! resolved to a Hive `col=value` segment in its destination. Everything
//! downstream (`run`'s loop, parallelism, manifest, `validate`) then sees
//! ordinary exports and needs no partition awareness —
//! which is why each partition gets its own manifest + `_SUCCESS` for free.
//!
//! The bucketing math and SQL builders are pure (`crate::plan::partition`);
//! this module owns the source round-trip (min/max + NULL probe) and the
//! `ExportConfig` synthesis.

use std::collections::HashMap;
use std::path::Path;

use crate::config::{ExportConfig, SourceConfig};
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

    let children = build_partition_children(export, col, bounds, has_nulls);
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
        crate::scalar::parse_date_flexible(raw).ok_or_else(|| {
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
    bounds: Option<(chrono::NaiveDate, chrono::NaiveDate)>,
    has_nulls: bool,
) -> Vec<ExportConfig> {
    let mut children = Vec::new();
    if let Some((min_day, max_day)) = bounds {
        for range in partition::generate_ranges(min_day, max_day, parent.partition_granularity) {
            children.push(make_child(
                parent,
                col,
                &range.label_value,
                &range.label_value,
                Some(range.bounds()),
            ));
        }
    }
    // NULL bucket: a range predicate can never match NULL, so any NULL rows
    // would be lost without this. `__HIVE_DEFAULT_PARTITION__` keeps them
    // queryable by Hive/Spark/duckdb partition discovery.
    if has_nulls {
        children.push(make_child(parent, col, HIVE_NULL_PARTITION, "null", None));
    }
    children
}

/// Build one concrete child export for a single bucket.
///
/// - `path_value` is the value side of the Hive segment (`2023-01-01` or
///   `__HIVE_DEFAULT_PARTITION__`); the segment is `col=path_value`.
/// - `name_value` is a filename-safe suffix for the child export name (used in
///   state keys and output filenames).
/// - `range` is the bucket's `[lo, hi)` day bounds; `None` is the NULL bucket.
fn make_child(
    parent: &ExportConfig,
    col: &str,
    path_value: &str,
    name_value: &str,
    range: Option<(chrono::NaiveDate, chrono::NaiveDate)>,
) -> ExportConfig {
    let mut child = parent.clone();
    child.partition_by = None;
    child.name = format!("{}__{}", parent.name, name_value);
    child.partition_window = Some(crate::config::PartitionSynth {
        column: col.to_string(),
        range,
    });
    let segment = format!("{col}={path_value}");
    child.destination =
        placeholder::expand_destination_partition(parent.destination.clone(), &segment);
    child
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
    fn make_child_resolves_segment_and_detaches() {
        let parent = part_export(true);
        let child = make_child(
            &parent,
            "created_at",
            "2023-01-03",
            "2023-01-03",
            Some((day("2023-01-03"), day("2023-01-04"))),
        );
        assert_eq!(child.name, "events__2023-01-03");
        assert!(child.partition_by.is_none());
        assert_eq!(child.query, parent.query, "the base query form is kept");
        assert_eq!(
            child.partition_window.as_ref().and_then(|w| w.range),
            Some((day("2023-01-03"), day("2023-01-04")))
        );
        assert_eq!(
            child.destination.path.as_deref(),
            Some("./out/events/created_at=2023-01-03")
        );
    }

    #[test]
    fn a_table_partition_child_plans_its_bucket_over_the_table() {
        let cfg = crate::config::Config::from_yaml(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: events
    table: events
    mode: full
    partition_by: created_at
    format: parquet
    columns:
      "events.amount": "decimal(38,2)"
    destination:
      type: local
      path: "./out/events/{partition}"
"#,
        )
        .expect("config");
        let parent = &cfg.exports[0];
        let children = build_partition_children(
            parent,
            "created_at",
            Some((day("2023-01-03"), day("2023-01-03"))),
            true,
        );
        let plan = |c: &ExportConfig| {
            crate::plan::build_plan(&cfg, c, Path::new("."), false, false, false, None)
                .expect("plan")
        };
        let day_plan = plan(&children[0]);
        assert_eq!(
            day_plan.base_query,
            "SELECT * FROM (SELECT * FROM events) AS _rivet_part \
             WHERE \"created_at\" >= '2023-01-03' AND \"created_at\" < '2023-01-04'"
        );
        assert_eq!(day_plan.source_table.as_deref(), Some("events"));
        assert!(
            day_plan.column_overrides.contains_key("amount"),
            "a qualified override narrows to the child's table: {:?}",
            day_plan.column_overrides
        );
        let null_plan = plan(&children[1]);
        assert!(
            null_plan
                .base_query
                .ends_with("WHERE \"created_at\" IS NULL"),
            "{}",
            null_plan.base_query
        );
    }

    #[test]
    fn make_child_null_bucket_uses_hive_default() {
        let parent = part_export(true);
        let child = make_child(&parent, "created_at", HIVE_NULL_PARTITION, "null", None);
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
            Some((day("2023-01-01"), day("2023-01-02"))),
            false,
        );
        assert_eq!(
            child_names(&children),
            ["events__2023-01-01", "events__2023-01-02"]
        );
    }

    #[test]
    fn children_only_null_bucket_when_no_value_span() {
        let parent = part_export(true);
        let children = build_partition_children(&parent, "created_at", None, true);
        assert_eq!(child_names(&children), ["events__null"]);
    }

    #[test]
    fn children_empty_when_no_rows_at_all() {
        let parent = part_export(true);
        let children = build_partition_children(&parent, "created_at", None, false);
        assert!(children.is_empty());
    }
}
