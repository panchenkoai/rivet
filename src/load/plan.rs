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
    /// prefix. Off by default.
    #[serde(default)]
    pub gc_orphans: bool,
    /// Clustering key column(s) — BigQuery `CLUSTER BY` / Snowflake `CLUSTER BY`.
    /// Empty = none. Applies at table creation.
    #[serde(default)]
    pub cluster_by: Vec<String>,
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

        // full + chunked are both complete snapshots → overwrite the latest run.
        let mode = match export.mode {
            crate::config::ExportMode::Cdc => LoadMode::Cdc,
            crate::config::ExportMode::Incremental => LoadMode::Incremental,
            _ => LoadMode::Full,
        };
        plans.push(LoadPlan {
            table,
            partition_by: export.partition_by.clone(),
            specs,
            gcs_prefix,
            destination: export.destination.clone(),
            load: load.clone(),
            mode,
            cursor_column: export.cursor_column.clone(),
        });
    }
    Ok(plans)
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

/// List the `*.parquet` object URIs under `gcs_prefix` (recursive), via the
/// native opendal client. Returns full `gs://bucket/<key>` URIs — the warehouse
/// (BigQuery `LOAD DATA` / Snowflake `COPY`) reads them, not opendal.
///
/// `pub` (public-API root, kept alive though only the binary-only
/// `cli::dispatch` calls it) with `#[allow(private_interfaces)]` for the
/// injected internal `GcsStore` — see the twin note on
/// [`reconcile::fetch_manifests`](crate::load::reconcile::fetch_manifests).
#[allow(private_interfaces)]
pub fn list_gcs_uris(
    store: &crate::destination::gcs::GcsStore,
    gcs_prefix: &str,
) -> Result<Vec<String>> {
    let (bucket, base) = crate::load::split_gs_uri(gcs_prefix)?;
    Ok(store
        .list_files(base)?
        .into_iter()
        .filter(|k| k.ends_with(".parquet"))
        .map(|k| format!("gs://{bucket}/{k}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn list_gcs_uris_keeps_only_parquet_and_reconstructs_gs_uris() {
        use crate::destination::gcs::GcsStore;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for rel in [
            "data/part-0.parquet",
            "data/part-1.parquet",
            "data/sub/part-2.parquet",
            "data/_SUCCESS",      // sentinel: not parquet
            "data/manifest.json", // manifest: not parquet
            "data/manifest-r1.json",
        ] {
            let p = root.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, b"x").unwrap();
        }
        let store = GcsStore::open_fs(root.to_str().unwrap()).unwrap();

        let mut uris = list_gcs_uris(&store, "gs://my-bucket/data").unwrap();
        uris.sort();
        assert_eq!(
            uris,
            vec![
                "gs://my-bucket/data/part-0.parquet".to_string(),
                "gs://my-bucket/data/part-1.parquet".to_string(),
                "gs://my-bucket/data/sub/part-2.parquet".to_string(),
            ],
            "only *.parquet objects, each reconstructed as gs://<bucket>/<key>"
        );
    }
}
