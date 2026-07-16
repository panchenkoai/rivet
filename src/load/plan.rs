//! Config-driven load planning — derive a BigQuery load (native schema, table,
//! partition, source URIs) from a rivet export config, so a client never
//! hand-types column types. The schema comes from rivet's own type resolver
//! via `rivet check --target bigquery --json` (the argv/process boundary,
//! ADR-0026); the table/partition/destination come from the parsed config.

use crate::types::target::{TargetColumnSpec, TargetStatus};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::process::Command;

/// The warehouse load target — the config's **top-level `load:` block**,
/// declared ONCE for all exports. OSS accepts and ignores this block (a
/// reserved passthrough); the loader reads it here. One config file drives
/// both the export and the load — no second file, no per-table repetition.
///
/// `cleanup_source`/`cluster_by` are target-agnostic; the warehouse and its
/// connection config live in [`LoadTarget`], keyed on the `target:`
/// discriminator — so a config that names `snowflake` cannot carry BigQuery
/// fields (invalid combos fail to deserialize; no runtime `validate()`).
#[derive(Debug, Clone, Deserialize)]
pub struct LoadSection {
    #[serde(flatten)]
    pub target: LoadTarget,
    #[serde(default)]
    pub cleanup_source: bool,
    /// Primary key column(s) for the incremental/CDC current-state dedup view —
    /// the view's PARTITION BY. Required for `mode: incremental` / `mode: cdc`;
    /// ignored for `full` (which overwrites, no view). Composite key = several
    /// columns, e.g. `pk: [tenant, id]`.
    #[serde(default)]
    pub pk: Vec<String>,
    /// Load even when a run manifest's source count disagrees with what it
    /// extracted (source→file drift): warn instead of blocking. The
    /// file→warehouse count gate and manifest gates still apply.
    #[serde(default)]
    pub allow_source_drift: bool,
    /// After a successful load, delete staged Parquet under the export prefix
    /// that no `Success` manifest references — crash leftovers from an
    /// interrupted extract. Keeps the current run's files, manifests, and
    /// `_SUCCESS`; strictly gentler than `cleanup_source`, which wipes the whole
    /// prefix. Off by default. ⚠️ Only enable when no extract writes this prefix
    /// concurrently — it can't tell a crash orphan from a live run's in-flight
    /// parts (see `reconcile::gc_orphans`); the normal load-after-extract flow is
    /// safe.
    #[serde(default)]
    pub gc_orphans: bool,
    /// Clustering key column(s) — BigQuery `CLUSTER BY` / Snowflake `CLUSTER BY`.
    /// Empty = none. Applies at table creation.
    #[serde(default)]
    pub cluster_by: Vec<String>,
}

/// Per-export overrides of the top-level [`LoadSection`] — every field optional,
/// `None` inherits the top-level value. `target` is present ONLY to reject it:
/// the warehouse is shared (`plan_loads` runs one `rivet check --target`), so it
/// stays top-level.
// `deny_unknown_fields` so a per-export `load:` typo (`gc_orphan`, `cleanupsrc`)
// fails loudly instead of silently deserializing to the default and dropping the
// override. (LoadOverride has no `#[serde(flatten)]`, so unlike LoadSection this
// works directly.)
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LoadOverride {
    #[serde(default)]
    pk: Option<Vec<String>>,
    #[serde(default)]
    cleanup_source: Option<bool>,
    #[serde(default)]
    gc_orphans: Option<bool>,
    #[serde(default)]
    cluster_by: Option<Vec<String>>,
    #[serde(default)]
    allow_source_drift: Option<bool>,
    /// Only to REJECT — a per-export `load:` cannot re-target the warehouse.
    #[serde(default)]
    target: Option<serde_json::Value>,
}

impl LoadSection {
    /// The effective load config for one export: this top-level section with the
    /// export's [`LoadOverride`] applied — each `Some` field replaces, each
    /// `None` inherits. `target` is never overridden.
    fn with_override(&self, o: &LoadOverride) -> LoadSection {
        let mut eff = self.clone();
        if let Some(pk) = &o.pk {
            eff.pk = pk.clone();
        }
        if let Some(c) = o.cleanup_source {
            eff.cleanup_source = c;
        }
        if let Some(g) = o.gc_orphans {
            eff.gc_orphans = g;
        }
        if let Some(cb) = &o.cluster_by {
            eff.cluster_by = cb.clone();
        }
        if let Some(d) = o.allow_source_drift {
            eff.allow_source_drift = d;
        }
        eff
    }
}

/// A warehouse and its connection config. `target:` is the serde discriminator.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "target", rename_all = "lowercase")]
pub enum LoadTarget {
    Bigquery {
        project: String,
        dataset: String,
    },
    Snowflake {
        connection: String,
        warehouse: String,
        database: String,
        schema: String,
        storage_integration: String,
    },
}

impl LoadTarget {
    /// The `--target` name to pass to `rivet check`.
    pub fn name(&self) -> &'static str {
        match self {
            LoadTarget::Bigquery { .. } => "bigquery",
            LoadTarget::Snowflake { .. } => "snowflake",
        }
    }
}

/// Which load strategy an export's `mode` maps to. Drives BOTH the ledger's
/// file selection and the warehouse write path:
/// - `Full` — the export is a complete snapshot; load the LATEST run only and
///   OVERWRITE (chunked is a parallel full snapshot, same handling).
/// - `Incremental` — the export is a delta since a cursor; APPEND it to
///   `<table>__changes` and dedup to current state ordered by the cursor.
/// - `Cdc` — a change stream; APPEND + dedup by `(__pos, __seq)` with tombstones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMode {
    Full,
    Incremental,
    Cdc,
}

impl LoadMode {
    /// The ledger's `mode` discriminator (the `load_run.mode` column) — the single
    /// source of truth for the string that names each strategy in the state DB, so
    /// no call site hand-writes a stringly-typed `"full"`/`"cdc"` that can drift.
    pub fn ledger_str(self) -> &'static str {
        match self {
            LoadMode::Full => "full",
            LoadMode::Incremental => "incremental",
            LoadMode::Cdc => "cdc",
        }
    }
}

/// What a rivet config resolves to for a BigQuery load.
#[derive(Debug, Clone)]
pub struct LoadPlan {
    pub table: String,
    pub partition_by: Option<String>,
    pub specs: Vec<TargetColumnSpec>,
    /// `gs://bucket/base/` — the destination prefix up to the `{partition}`
    /// token, i.e. the root to list source Parquet under.
    pub gcs_prefix: String,
    /// The export's GCS destination (bucket + auth) — the native opendal client
    /// the load layer lists / reads / deletes through.
    pub destination: crate::config::DestinationConfig,
    /// The `load:` target from the same config.
    pub load: LoadSection,
    /// The export's mode → the load strategy (see [`LoadMode`]).
    pub mode: LoadMode,
    /// The incremental cursor column (from `cursor_column:`) — the dedup view's
    /// latest-per-PK ordering key. `Some` only for [`LoadMode::Incremental`].
    pub cursor_column: Option<String>,
}

/// One export's slice of `rivet check --target X --json`. The tool emits one
/// such JSON document **per export** (concatenated), so a multi-table config
/// yields a stream of these — parsed with a streaming deserializer.
#[derive(Deserialize)]
struct ExportReport {
    export: String,
    columns: Vec<ColReport>,
}

#[derive(Deserialize)]
struct ColReport {
    column: String,
    target_type: String,
    target_status: String,
}

/// Resolve a rivet config into **one [`LoadPlan`] per export** — the shared
/// top-level `load:` target plus each export's own table / partition / GCS
/// destination / native schema. `rivet check --json` emits one JSON document
/// per export, so a multi-table config produces a plan per table, all pointed
/// at the same warehouse target.
/// Every key a `load:` block may carry — the [`LoadSection`] fields plus the
/// flattened [`LoadTarget`] variant fields. The top-level block can't use serde
/// `deny_unknown_fields` (incompatible with the flattened `target` enum), so we
/// check its keys by hand — else a typo (`gc_orphan`, `cleanupsource`) silently
/// deserializes to the default and the setting never applies.
const LOAD_KEYS: &[&str] = &[
    "target",
    "cleanup_source",
    "pk",
    "allow_source_drift",
    "gc_orphans",
    "cluster_by",
    // LoadTarget::{Bigquery, Snowflake} variant fields (flattened in).
    "project",
    "dataset",
    "connection",
    "warehouse",
    "database",
    "schema",
    "storage_integration",
];

/// Reject any key in a `load:` block that isn't in [`LOAD_KEYS`] — turns a
/// silently-ignored typo into a loud error naming the valid keys.
fn check_load_keys(value: &serde_json::Value, whose: &str) -> Result<()> {
    if let Some(obj) = value.as_object() {
        for k in obj.keys() {
            if !LOAD_KEYS.contains(&k.as_str()) {
                bail!(
                    "unknown key `{k}` in the {whose} `load:` block — valid keys are: {}",
                    LOAD_KEYS.join(", ")
                );
            }
        }
    }
    Ok(())
}

pub fn plan_loads(config_path: &str, rivet_bin: &str) -> Result<Vec<LoadPlan>> {
    let yaml = std::fs::read_to_string(config_path)
        .with_context(|| format!("reading config {config_path}"))?;
    let cfg = crate::config::Config::from_yaml(&yaml).context("parsing rivet config")?;
    if cfg.exports.is_empty() {
        bail!("config has no exports");
    }

    // The `load:` target from the same config (OSS accepts + ignores it),
    // shared by every export.
    let load_value = cfg.load.clone().context(
        "config has no top-level `load:` block — add `load: { target, ... }` to load into a warehouse",
    )?;
    check_load_keys(&load_value, "top-level")?;
    let load: LoadSection =
        serde_json::from_value(load_value).context("parsing the top-level `load:` block")?;

    // Native schema from rivet's own resolver, for the load target — no
    // hand-typing. One JSON document per export, so parse a stream.
    let out = Command::new(rivet_bin)
        .args([
            "check",
            "-c",
            config_path,
            "--target",
            load.target.name(),
            "--json",
        ])
        .output()
        .with_context(|| {
            format!("running `{rivet_bin} check` — is rivet on PATH? pass --rivet-bin")
        })?;
    if !out.status.success() {
        bail!(
            "rivet check failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let reports: Vec<ExportReport> = serde_json::Deserializer::from_slice(&out.stdout)
        .into_iter::<ExportReport>()
        .collect::<Result<_, _>>()
        .context("parsing `rivet check --json` (one document per export)")?;

    build_plans(&cfg, &load, reports)
}

/// The **pure core** of [`plan_loads`]: map the parsed `rivet check` reports onto
/// one [`LoadPlan`] per export, given the config and the shared `load:` section.
///
/// No I/O — the subprocess and filesystem work is done by [`plan_loads`], and
/// everything they produced arrives in the args. That makes the per-export
/// resolution unit-testable without a `rivet` binary: the export→report name
/// match, `target_status`→[`TargetStatus`] mapping, [`ExportMode`]→[`LoadMode`]
/// mapping, the `gs://` prefix, the per-export `load:` override, and the
/// duplicate-target guard.
fn build_plans(
    cfg: &crate::config::Config,
    load: &LoadSection,
    reports: Vec<ExportReport>,
) -> Result<Vec<LoadPlan>> {
    let mut plans = Vec::with_capacity(reports.len());
    for report in reports {
        let export = cfg
            .exports
            .iter()
            .find(|e| e.name == report.export)
            .with_context(|| {
                format!(
                    "rivet check reported export `{}` not found in config",
                    report.export
                )
            })?;
        let table = export.table.clone().unwrap_or_else(|| export.name.clone());

        let dest = &export.destination;
        let bucket = dest.bucket.as_deref().with_context(|| {
            format!(
                "export `{}` has no destination `bucket` — a GCS destination is required",
                export.name
            )
        })?;
        let prefix = dest.prefix.as_deref().unwrap_or("");
        let base = prefix.split("{partition}").next().unwrap_or(prefix);
        let gcs_prefix = format!("gs://{bucket}/{base}");

        let specs = report
            .columns
            .into_iter()
            .map(|c| TargetColumnSpec {
                column_name: c.column,
                target_type: c.target_type,
                autoload_type: String::new(),
                status: match c.target_status.as_str() {
                    "fail" => TargetStatus::Fail,
                    "warn" => TargetStatus::Warn,
                    _ => TargetStatus::Ok,
                },
                note: None,
                cast_sql: None,
            })
            .collect();

        // Complete-snapshot modes → overwrite the latest run; delta modes → their
        // own append path. Exhaustive (no `_`) on purpose: a future delta-style
        // ExportMode then fails to COMPILE here until someone picks its load
        // semantics, instead of silently defaulting to OVERWRITE (the
        // incremental-overwrite data-loss class).
        let mode = match export.mode {
            crate::config::ExportMode::Cdc => LoadMode::Cdc,
            crate::config::ExportMode::Incremental => LoadMode::Incremental,
            crate::config::ExportMode::Full => LoadMode::Full, // whole result set
            crate::config::ExportMode::Chunked => LoadMode::Full, // parallel full snapshot
            crate::config::ExportMode::TimeWindow => LoadMode::Full, // whole rolling window
        };
        // Effective load config: the shared top-level `load:`, with this export's
        // own `load:` block overriding the table-specific fields (pk, cleanup, …).
        // The warehouse `target` is shared and cannot be re-targeted per export.
        let eff_load = match &export.load {
            Some(v) => {
                let o: LoadOverride = serde_json::from_value(v.clone()).with_context(|| {
                    format!("parsing export `{}` `load:` override", export.name)
                })?;
                if o.target.is_some() {
                    bail!(
                        "export `{}`: a per-export `load:` cannot override `target:` — the \
                         warehouse is shared; set `target:` in the top-level `load:` block only",
                        export.name
                    );
                }
                load.with_override(&o)
            }
            None => load.clone(),
        };
        plans.push(LoadPlan {
            table,
            partition_by: export.partition_by.clone(),
            specs,
            gcs_prefix,
            destination: export.destination.clone(),
            load: eff_load,
            mode,
            cursor_column: export.cursor_column.clone(),
        });
    }
    reject_duplicate_target_tables(&plans.iter().map(|p| p.table.as_str()).collect::<Vec<_>>())?;
    Ok(plans)
}

/// Reject two exports that resolve to the SAME warehouse table. The `target:` is
/// shared, so two exports whose `table:` (or `name:`) resolves alike land on one
/// warehouse object — a full OVERWRITE would clobber what a cdc/incremental
/// export appends a `<table>__changes` view over, and they'd share one ledger
/// skip-set. Pure + unit-testable; caught here, not silently at load time.
fn reject_duplicate_target_tables(tables: &[&str]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for t in tables {
        if !seen.insert(*t) {
            bail!(
                "two exports resolve to the same load target table `{t}` — each would clobber \
                 the other (a full OVERWRITE vs a cdc/incremental append share the table and \
                 its ledger). Give each export its own `table:` or destination."
            );
        }
    }
    Ok(())
}

/// Resolve the config's source engine into the CDC [`SourceEngine`] the dedup
/// view's `__pos` parse is keyed on. One config has one source, so this is a
/// job-wide property. MongoDB is supported too: its change stream carries a
/// document `_id` (the dedup partition key) and an order-preserving `_data`
/// resume token in `__pos`, so the current-state view applies just as it does to
/// the relational engines.
pub fn source_engine(config_path: &str) -> Result<crate::load::cdc::SourceEngine> {
    use crate::config::SourceType;
    use crate::load::cdc::SourceEngine;

    let yaml = std::fs::read_to_string(config_path)
        .with_context(|| format!("reading config {config_path}"))?;
    let cfg = crate::config::Config::from_yaml(&yaml).context("parsing rivet config")?;
    match cfg.source.source_type {
        SourceType::Postgres => Ok(SourceEngine::Postgres),
        SourceType::Mysql => Ok(SourceEngine::MySql),
        SourceType::Mssql => Ok(SourceEngine::SqlServer),
        SourceType::Mongo => Ok(SourceEngine::Mongo),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_str_names_each_mode_stably() {
        // The state DB's `load_run.mode` discriminator — every mode must map to
        // its exact stable string, since retry/skip logic keys off it. A drifted
        // value would mislabel loads in the ledger.
        assert_eq!(LoadMode::Full.ledger_str(), "full");
        assert_eq!(LoadMode::Incremental.ledger_str(), "incremental");
        assert_eq!(LoadMode::Cdc.ledger_str(), "cdc");
    }

    /// A `ColReport` with an explicit `target_status`.
    fn col(name: &str, status: &str) -> ColReport {
        ColReport {
            column: name.into(),
            target_type: "STRING".into(),
            target_status: status.into(),
        }
    }

    /// Drive the PURE `build_plans` (no `rivet` subprocess) — the deepened core
    /// of `plan_loads`. Kills the mutation survivors that live in the per-export
    /// resolution: the export→report name match (`==`→`!=`) and the `fail`/`warn`
    /// `target_status` arms. Also pins mode mapping, the `gs://` prefix, table
    /// resolution, and the cursor column.
    #[test]
    fn build_plans_matches_by_name_maps_statuses_and_mode() {
        let cfg = crate::config::Config::from_yaml(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: alpha
    table: alpha_tbl
    mode: full
    format: parquet
    destination:
      type: gcs
      bucket: b1
      prefix: exports/alpha/
  - name: beta
    table: beta_tbl
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination:
      type: gcs
      bucket: b2
      prefix: exports/beta/
load:
  target: bigquery
  project: p
  dataset: d
"#,
        )
        .unwrap();
        let load: LoadSection = serde_json::from_value(cfg.load.clone().unwrap()).unwrap();

        // Reports arrive in the OPPOSITE order to the exports, so a plan only
        // lands on the right table if its export is found by NAME, not position.
        let reports = vec![
            ExportReport {
                export: "beta".into(),
                columns: vec![col("id", "ok"), col("f", "fail"), col("w", "warn")],
            },
            ExportReport {
                export: "alpha".into(),
                columns: vec![col("id", "ok")],
            },
        ];

        let plans = build_plans(&cfg, &load, reports).unwrap();
        assert_eq!(plans.len(), 2);

        // reports[0] = beta → matched by name to the 2nd export (kills `==`→`!=`,
        // which would resolve the first NON-matching export instead).
        assert_eq!(
            plans[0].table, "beta_tbl",
            "found beta by name, not position"
        );
        assert_eq!(plans[0].mode, LoadMode::Incremental);
        assert_eq!(plans[0].cursor_column.as_deref(), Some("updated_at"));
        assert_eq!(plans[0].gcs_prefix, "gs://b2/exports/beta/");
        // target_status → spec.status (kills the `fail`/`warn` arm deletions,
        // which would collapse those columns to Ok and load an unmappable column).
        let statuses: Vec<_> = plans[0].specs.iter().map(|s| s.status).collect();
        assert_eq!(
            statuses,
            vec![TargetStatus::Ok, TargetStatus::Fail, TargetStatus::Warn]
        );

        // reports[1] = alpha → the full-snapshot export.
        assert_eq!(plans[1].table, "alpha_tbl");
        assert_eq!(plans[1].mode, LoadMode::Full);
        assert_eq!(plans[1].gcs_prefix, "gs://b1/exports/alpha/");
    }

    #[test]
    fn build_plans_bails_on_a_report_for_an_unknown_export() {
        let cfg = crate::config::Config::from_yaml(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: a\n    query: \"SELECT 1\"\n    format: parquet\n    \
             destination:\n      type: gcs\n      bucket: b\n      prefix: p/\nload:\n  \
             target: bigquery\n  project: p\n  dataset: d\n",
        )
        .unwrap();
        let load: LoadSection = serde_json::from_value(cfg.load.clone().unwrap()).unwrap();
        let reports = vec![ExportReport {
            export: "ghost".into(),
            columns: vec![],
        }];
        let err = build_plans(&cfg, &load, reports).unwrap_err().to_string();
        assert!(err.contains("ghost") && err.contains("not found"), "{err}");
    }

    #[test]
    fn reject_duplicate_target_tables_catches_a_collision() {
        // Two exports resolving to the same warehouse table would clobber each
        // other — caught at plan time, not silently at load time.
        assert!(reject_duplicate_target_tables(&["orders", "events", "orders"]).is_err());
        assert!(reject_duplicate_target_tables(&["orders", "events"]).is_ok());
        assert!(reject_duplicate_target_tables(&[]).is_ok());
    }

    #[test]
    fn multi_export_check_json_parses_as_a_stream() {
        // `rivet check --json` emits ONE document per export (no wrapping array).
        // A single-object parse fails "trailing characters"; the stream parser
        // yields one ExportReport per table. Guards the multi-table regression.
        let raw = concat!(
            "{\"export\":\"orders\",\"columns\":[{\"column\":\"id\",\"target_type\":\"NUMBER\",\"target_status\":\"ok\"}]}\n",
            "{\"export\":\"customers\",\"columns\":[{\"column\":\"cid\",\"target_type\":\"NUMBER\",\"target_status\":\"ok\"}]}\n"
        );
        let reports: Vec<ExportReport> = serde_json::Deserializer::from_slice(raw.as_bytes())
            .into_iter::<ExportReport>()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].export, "orders");
        assert_eq!(reports[1].export, "customers");
        assert_eq!(reports[1].columns[0].column, "cid");
    }

    #[test]
    fn bigquery_load_section_deserializes_into_its_variant() {
        let value = serde_json::json!({
            "target": "bigquery", "project": "p", "dataset": "d",
            "cleanup_source": true, "cluster_by": ["customer"]
        });
        let load: LoadSection = serde_json::from_value(value).unwrap();
        assert_eq!(load.target.name(), "bigquery");
        assert!(load.cleanup_source);
        assert_eq!(load.cluster_by, vec!["customer"]);
        match load.target {
            LoadTarget::Bigquery { project, dataset } => {
                assert_eq!((project.as_str(), dataset.as_str()), ("p", "d"));
            }
            _ => panic!("expected Bigquery variant"),
        }
    }

    #[test]
    fn snowflake_missing_field_is_unrepresentable_no_runtime_validate() {
        let full = serde_json::json!({
            "target": "snowflake", "connection": "rivet", "warehouse": "wh",
            "database": "db", "schema": "sc", "storage_integration": "si"
        });
        let load: LoadSection = serde_json::from_value(full).unwrap();
        assert_eq!(load.target.name(), "snowflake");

        // A snowflake block missing storage_integration doesn't deserialize —
        // the type makes it unrepresentable, so there is no runtime validate().
        let partial = serde_json::json!({
            "target": "snowflake", "connection": "rivet", "warehouse": "wh",
            "database": "db", "schema": "sc"
        });
        let err = serde_json::from_value::<LoadSection>(partial).unwrap_err();
        assert!(
            err.to_string().contains("storage_integration"),
            "error should name the missing field: {err}"
        );
    }

    #[test]
    fn unknown_target_is_rejected_at_deserialize() {
        let value = serde_json::json!({ "target": "redshift", "project": "p" });
        assert!(serde_json::from_value::<LoadSection>(value).is_err());
    }

    fn top_level_load() -> LoadSection {
        serde_json::from_value(serde_json::json!({
            "target": "bigquery", "project": "p", "dataset": "d",
            "pk": ["top"], "cleanup_source": true, "gc_orphans": false,
            "cluster_by": ["c0"], "allow_source_drift": false,
        }))
        .unwrap()
    }

    #[test]
    fn with_override_replaces_some_fields_and_inherits_the_rest() {
        let top = top_level_load();
        // Override ONLY pk + gc_orphans; the rest must inherit the top-level.
        let o: LoadOverride =
            serde_json::from_value(serde_json::json!({ "pk": ["id"], "gc_orphans": true }))
                .unwrap();
        let eff = top.with_override(&o);
        assert_eq!(eff.pk, vec!["id"], "pk replaced");
        assert!(eff.gc_orphans, "gc_orphans replaced");
        assert!(
            eff.cleanup_source,
            "cleanup_source inherited (top-level true)"
        );
        assert_eq!(eff.cluster_by, vec!["c0"], "cluster_by inherited");
        assert!(!eff.allow_source_drift, "allow_source_drift inherited");
    }

    #[test]
    fn override_parsing_leaves_omitted_fields_none() {
        let o: LoadOverride = serde_json::from_value(serde_json::json!({ "pk": ["id"] })).unwrap();
        assert_eq!(o.pk.as_deref(), Some(&["id".to_string()][..]));
        assert!(o.cleanup_source.is_none());
        assert!(o.gc_orphans.is_none());
        assert!(o.cluster_by.is_none());
        assert!(o.allow_source_drift.is_none());
        assert!(o.target.is_none());
    }

    #[test]
    fn empty_override_is_distinct_from_inherit() {
        let top = top_level_load();
        // An EXPLICIT empty pk clears the inherited one; a missing pk keeps it.
        let cleared: LoadOverride =
            serde_json::from_value(serde_json::json!({ "pk": [] })).unwrap();
        assert!(
            top.with_override(&cleared).pk.is_empty(),
            "explicit [] clears"
        );
        let inherit: LoadOverride = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(
            top.with_override(&inherit).pk,
            vec!["top"],
            "omitted inherits"
        );
    }

    #[test]
    fn override_carrying_target_is_detected() {
        // The plan_loads guard rejects a per-export `load:` that re-targets the
        // warehouse; the override captures `target:` so the guard can see it.
        let o: LoadOverride =
            serde_json::from_value(serde_json::json!({ "target": "snowflake" })).unwrap();
        assert!(
            o.target.is_some(),
            "target captured for the plan_loads guard"
        );
    }

    #[test]
    fn unknown_top_level_load_key_is_rejected() {
        // A typo (`gc_orphan` for `gc_orphans`) must fail loudly, not silently
        // deserialize to the default so the setting never applies.
        let typo = serde_json::json!({
            "target": "bigquery", "project": "p", "dataset": "d", "gc_orphan": true
        });
        let err = check_load_keys(&typo, "top-level").unwrap_err().to_string();
        assert!(err.contains("gc_orphan"), "{err}");
        // Every valid LoadSection + LoadTarget key passes.
        let ok = serde_json::json!({
            "target": "bigquery", "project": "p", "dataset": "d",
            "gc_orphans": true, "cleanup_source": false, "pk": ["id"],
            "allow_source_drift": true, "cluster_by": ["a"]
        });
        assert!(check_load_keys(&ok, "top-level").is_ok());
    }

    #[test]
    fn unknown_per_export_override_key_is_rejected() {
        // `deny_unknown_fields` on LoadOverride catches per-export typos.
        let typo = serde_json::json!({ "pk": ["id"], "cluster_bye": ["x"] });
        assert!(
            serde_json::from_value::<LoadOverride>(typo).is_err(),
            "a typo'd override key must fail to parse"
        );
        let ok = serde_json::json!({ "pk": ["id"], "cleanup_source": true });
        assert!(serde_json::from_value::<LoadOverride>(ok).is_ok());
    }
}
