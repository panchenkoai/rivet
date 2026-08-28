//! Config-driven load planning — derive a BigQuery load (native schema, table,
//! partition, source URIs) from a rivet export config, so a client never
//! hand-types column types. The schema comes from rivet's own type resolver,
//! called IN PROCESS (`preflight::collect_type_reports`, the same function
//! `rivet check --target X --json` renders from); the table/partition/
//! destination come from the parsed config.
//!
//! Until 0.24.x this shelled out to `rivet check --json` and parsed its stdout.
//! A subprocess resolves types with whatever binary it names, so version skew
//! was a live failure mode — `--rivet-bin` existed only to mitigate it, and one
//! config was parsed TWICE (once here, once in the child) with different
//! `${VAR}` resolution. Both are gone: one parse, one resolver, no argv.

use crate::types::target::TargetColumnSpec;
use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// The warehouse load target — the config's **top-level `load:` block**,
/// declared ONCE for all exports. OSS accepts and ignores this block (a
/// reserved passthrough); the loader reads it here. One config file drives
/// both the export and the load — no second file, no per-table repetition.
///
/// `cleanup_source`/`cluster_by` are target-agnostic; the warehouse and its
/// connection config live in [`LoadTarget`], keyed on the `target:`
/// discriminator. NOTE: `#[serde(flatten)]` on `target` DISABLES serde's
/// `deny_unknown_fields`, so cross-warehouse fields do NOT fail to deserialize —
/// a `target: snowflake` block silently accepted BigQuery's `project:`/`dataset:`
/// (dogfood LOW). [`reject_foreign_target_fields`] is the runtime guard that
/// closes that gap; the "invalid combos fail to deserialize" belief was false.
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
/// the warehouse is shared (`plan_loads` resolves ONE target's types for every
/// export), so it stays top-level.
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
    /// The target name the type resolver is keyed on (`ExportTarget::parse`) —
    /// the same token `rivet check --target` takes.
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

/// The warehouse table name for a source table, with the schema qualifier folded
/// into it rather than left as a dot.
///
/// The loaders build a fully-qualified name as `{project}.{dataset}.{table}`, so
/// a schema-qualified source table produced FOUR segments and the warehouse read
/// the extra one as part of the dataset:
///
/// ```text
/// table: public.orders  ->  rivet-data-tool.rivet_e2e.public.orders
/// Not found: Dataset rivet-data-tool:rivet_e2e.public
/// ```
///
/// A hard failure with a message that blames a dataset the operator never named.
///
/// Folded to `public_orders`, not truncated to `orders`: two schemas that both
/// have `orders` are a normal arrangement, and collapsing them onto one warehouse
/// table would turn a loud failure into a silent overwrite — the trade this
/// codebase keeps refusing to make. Bare names are untouched, so nothing that
/// works today changes.
fn warehouse_table_name(table: &str, export_name: &str) -> String {
    if !table.contains('.') {
        return table.to_string();
    }
    let folded = table.replace('.', "_");
    log::info!(
        "export '{export_name}': source table `{table}` is schema-qualified; the warehouse table \
         is `{folded}` (a dot would be read as part of the dataset name)"
    );
    folded
}

/// What a rivet config resolves to for a BigQuery load.
#[derive(Debug, Clone)]
pub struct LoadPlan {
    /// The declared export NAME (config `name:`), distinct from the warehouse
    /// `table` — error messages address the export the operator wrote, not the
    /// table it resolves to (dogfood LOW: require_pk labelled the table as the
    /// export).
    pub export_name: String,
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

/// Resolve a rivet config into **one [`LoadPlan`] per export** — the shared
/// top-level `load:` target plus each export's own table / partition / GCS
/// destination / native schema. The type resolver returns one report per
/// export, so a multi-table config produces a plan per table, all pointed
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

/// Warehouse fields that belong to exactly ONE target.
const BIGQUERY_ONLY: &[&str] = &["project", "dataset"];
const SNOWFLAKE_ONLY: &[&str] = &[
    "connection",
    "warehouse",
    "database",
    "schema",
    "storage_integration",
];

/// Reject fields that belong to a DIFFERENT warehouse than the resolved
/// `target`. `#[serde(flatten)]` on the target enum can't `deny_unknown_fields`,
/// so `LOAD_KEYS` (the union of all warehouses' fields) let a `target: snowflake`
/// block silently carry — and ignore — BigQuery's `project:`/`dataset:` (and
/// vice versa) (dogfood LOW). Name the offending key and its warehouse.
fn reject_foreign_target_fields(
    value: &serde_json::Value,
    target: &str,
    whose: &str,
) -> Result<()> {
    let (foreign, other) = match target {
        "bigquery" => (SNOWFLAKE_ONLY, "snowflake"),
        "snowflake" => (BIGQUERY_ONLY, "bigquery"),
        _ => return Ok(()),
    };
    if let Some(obj) = value.as_object() {
        for k in obj.keys() {
            if foreign.contains(&k.as_str()) {
                bail!(
                    "the {whose} `load:` block targets `{target}` but carries `{k}`, a `{other}` \
                     field — remove it (it would be silently ignored, masking a mis-configured load)"
                );
            }
        }
    }
    Ok(())
}

/// Resolve a load's staging prefix from an export destination.
///
/// Expands the same `{date}`/`{export}`/`{table}` placeholders the export wrote
/// with (`PlaceholderContext::for_today`) so the load lists the ACTUAL prefix
/// (`exports/orders/`) rather than the literal config token (`exports/{export}/`)
/// — without this the load found no manifests under the unexpanded path and
/// reported "up to date" having loaded nothing (#100). `{partition}` is stripped
/// (its per-partition sub-prefixes live below).
///
/// A `{date}` in the load-listed BASE is refused up front: it expands to the
/// LOAD day, so a nightly export + an after-midnight load list DIFFERENT prefixes
/// and the load silently reports "up to date" (bughunt HIGH — the same #100
/// silent-no-load class the expansion above closes for the static tokens, left
/// open for the day-specific one). `{run_id}` (and any token still unresolved
/// after expansion) fails loud the same way.
fn resolve_load_prefix(
    dest: &crate::config::DestinationConfig,
    export_name: &str,
    bucket: &str,
) -> Result<String> {
    // Refuse a day-specific `{date}` in the load base BEFORE expansion — once
    // expanded to the load day, the `contains('{')` guard below can never see it,
    // so an export written on a different UTC day is silently missed.
    let raw_prefix = dest.prefix.as_deref().unwrap_or("");
    let raw_base = raw_prefix.split("{partition}").next().unwrap_or(raw_prefix);
    if raw_base.contains("{date}") {
        bail!(
            "export `{}`: destination.prefix `{}` puts a day-specific `{{date}}` in the load base. \
             `rivet load` lists the LOAD-day prefix, so an export written on a different day (a \
             nightly export + an after-midnight load) lands under a different, EMPTY prefix and is \
             silently reported 'up to date'. Remove `{{date}}` from the load base, or place it \
             BELOW `{{partition}}` so the load can list a stable prefix.",
            export_name,
            raw_base
        );
    }
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(export_name);
    let expanded = crate::destination::placeholder::expand_destination(dest.clone(), &ctx);
    let prefix = expanded.prefix.as_deref().unwrap_or("");
    let base = prefix.split("{partition}").next().unwrap_or(prefix);
    if base.contains('{') {
        bail!(
            "export `{}`: load prefix `{}` still has an unresolved placeholder after expansion — \
             `rivet load` cannot reconstruct which run's output to load (a `{{run_id}}` prefix is \
             run-specific). Drop the run-specific token from `destination.prefix`, or run \
             `rivet load` from the context that wrote the export.",
            export_name,
            base
        );
    }
    Ok(format!("gs://{bucket}/{base}"))
}

/// The load prefix of ONE table of a multiplex `tables:` CDC stream, given the
/// export's resolved base prefix.
///
/// The extract fans each captured table out under `<base>/<table>/` — one
/// `manifest.json` + `_SUCCESS` per table, with the initial snapshot nested a
/// level below as `<base>/<table>/snapshot/` — and `rivet validate` descends
/// exactly that. So the load must list exactly that too, and the sub-prefix is
/// produced by the WRITER's own function ([`crate::pipeline::cdc_job::dest_for_table`])
/// rather than a second concatenation rule here: a cloud prefix is a LITERAL key
/// prefix (the destination concatenates `prefix + key` with no separator), so a
/// sub-prefix that forgets to supply its own slashes lists a mangled flat key and
/// finds nothing — the silent "up to date, loaded nothing" shape.
///
/// Applied AFTER [`resolve_load_prefix`] rather than to the raw destination, so
/// the placeholder expansion and the `{partition}` strip both see the base the
/// export wrote, and the table segment can never land below a stripped token.
fn table_load_prefix(base_uri: &str, table: &str) -> Result<String> {
    let (bucket, base) = crate::load::split_gs_uri(base_uri)?;
    let sub = crate::pipeline::cdc_job::dest_for_table(
        &crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Gcs,
            prefix: Some(base.to_string()),
            ..Default::default()
        },
        table,
    );
    Ok(format!("gs://{bucket}/{}", sub.prefix.unwrap_or_default()))
}

pub fn plan_loads(config_path: &str) -> Result<Vec<LoadPlan>> {
    // `Config::load`, not a raw `from_yaml`: it resolves `${VAR}`/`--param`
    // placeholders exactly as the `rivet check` child did. The old code parsed
    // the file TWICE with two different resolutions — an unexpanded `${BUCKET}`
    // in the parent's copy pointed the load at a literal-token prefix while the
    // child (which resolved it) reported types for the real one.
    let cfg = crate::config::Config::load(config_path).context("parsing rivet config")?;
    if cfg.exports.is_empty() {
        bail!("config has no exports");
    }

    // The `load:` target from the same config (OSS accepts + ignores it),
    // shared by every export.
    let load_value = cfg.load.clone().context(
        "config has no top-level `load:` block — add `load: { target, ... }` to load into a warehouse",
    )?;
    check_load_keys(&load_value, "top-level")?;
    let load: LoadSection = serde_json::from_value(load_value.clone())
        .context("parsing the top-level `load:` block")?;
    reject_foreign_target_fields(&load_value, load.target.name(), "top-level")?;

    // Native schema from rivet's own resolver, for the load target — no
    // hand-typing, and no subprocess: `collect_type_reports` is the function
    // `rivet check --target X --json` renders from, called directly, so the
    // types this load declares are the ones THIS binary resolves.
    let target = crate::types::target::ExportTarget::parse(load.target.name())
        .with_context(|| format!("unknown load target `{}`", load.target.name()))?;
    let reports = crate::preflight::collect_type_reports(&cfg, config_path, target)?;

    build_plans(&cfg, &load, reports)
}

/// The **pure core** of [`plan_loads`]: map the resolver's type reports onto
/// one [`LoadPlan`] per export, given the config and the shared `load:` section.
///
/// No I/O — the source connection and filesystem work is done by [`plan_loads`],
/// and everything they produced arrives in the args. That makes the per-export
/// resolution unit-testable without a source: the export→report name match, the
/// report-row→[`TargetColumnSpec`] rebuild, [`ExportMode`]→[`LoadMode`] mapping,
/// the `gs://` prefix, the per-export `load:` override, and the duplicate-target
/// guard.
///
/// The reports arrive as the resolver's OWN struct — it used to be a narrower
/// `Deserialize` mirror of `rivet check --json`, and a mirror can only carry the
/// keys someone remembered to declare, which is how `note` / `cast_sql` /
/// `autoload_type` came to be dropped on the floor.
fn build_plans(
    cfg: &crate::config::Config,
    load: &LoadSection,
    reports: Vec<crate::preflight::type_report::ExportTypeReport>,
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
        // A multiplex `tables:` CDC export is N tables through ONE export, and the
        // resolver hands us one report per table (`report.table`). Each is its own
        // warehouse table under its own `<base>/<table>/` sub-prefix — which is why
        // the fan-out has to happen HERE and not be left to the export name: a
        // single plan per export would point every table at one warehouse table and
        // one BASE prefix, whose recursive manifest listing sweeps in every sibling
        // table's parts. That merges N source tables into one warehouse table with
        // every count agreeing (#252).
        let source_table = report
            .table
            .clone()
            .or_else(|| export.table.clone())
            .unwrap_or_else(|| export.name.clone());
        let table = warehouse_table_name(&source_table, &export.name);

        let dest = &export.destination;
        // Round-6 HIGH: the load layer builds a GCS client UNCONDITIONALLY, so a
        // `type: s3` destination with a `load:` block silently listed a
        // SAME-NAMED GCS bucket (stranger-claimable) — empty → permanently
        // "up to date", exit 0; cleanup/gc would target that foreign prefix.
        // Both warehouse loaders are GCS-only (Snowflake rewrites gs://→gcs://),
        // so refuse anything else loudly at plan time.
        if dest.destination_type != crate::config::DestinationType::Gcs {
            anyhow::bail!(
                "export `{}` has `load:` but its destination is `type: {:?}` — the load \
                 layer reads GCS only (Snowflake via storage integration, BigQuery via \
                 LOAD DATA). Stage the export to a gcs destination, or drop the load \
                 block.",
                export.name,
                dest.destination_type
            );
        }
        let bucket = dest.bucket.as_deref().with_context(|| {
            format!(
                "export `{}` has no destination `bucket` — a GCS destination is required",
                export.name
            )
        })?;
        let base_prefix = resolve_load_prefix(dest, &export.name, bucket)?;
        let gcs_prefix = match &report.table {
            Some(t) => table_load_prefix(&base_prefix, t)?,
            None => base_prefix,
        };

        // The report row IS the resolver's `TargetColumnSpec`, split across the
        // report's optional fields — so rebuild the whole spec, not the two
        // fields the old JSON mirror happened to declare. `autoload_type` is
        // carried only when it DIVERGES (`collect_report`'s rule), so an absent
        // one means "same as native".
        let mut specs: Vec<TargetColumnSpec> = report
            .columns
            .into_iter()
            .map(|c| {
                // Both are `Some` together for every column resolved against a
                // target, and the load always resolves WITH one. Named loudly
                // rather than defaulted: a defaulted `Ok` would walk an
                // unmappable column straight past `validate_specs`.
                let (Some(target_type), Some(status)) = (c.target_type, c.target_status) else {
                    bail!(
                        "export `{}` column `{}`: the type resolver returned no {} type — \
                         refusing to guess one",
                        export.name,
                        c.column,
                        load.target.name()
                    );
                };
                Ok(TargetColumnSpec {
                    column_name: c.column,
                    autoload_type: c.autoload_type.unwrap_or_else(|| target_type.clone()),
                    target_type,
                    status,
                    note: c.target_note,
                    cast_sql: c.cast_sql,
                })
            })
            .collect::<Result<_>>()?;

        // The meta columns rivet writes at EXTRACTION are in every Parquet part
        // but absent from the column report, which the type resolver builds from
        // the SOURCE catalog. Without a spec the created table simply lacks the
        // column and the very first load fails on a schema mismatch — after the
        // extract has already run. So EVERY enabled meta column needs one; they
        // are resolved together because a spec for one and not the other is the
        // same bug twice (`_rivet_row_hash` had it fixed while
        // `_rivet_exported_at`, written by the identical seam, did not).
        //
        // Types go through the same per-target resolver every other column does
        // rather than a hardcoded literal, so they cannot drift from the
        // warehouse's own types (the hash: BigQuery INT64, Snowflake
        // NUMBER(38,0), ClickHouse Int64).
        // Order mirrors `enrich_schema`'s (exported_at, then row_hash) so the spec
        // list matches the Parquet's column order.
        let mut meta_specs: Vec<(&str, crate::types::RivetType)> = Vec::new();
        if export.meta_columns.exported_at {
            meta_specs.push((
                crate::enrich::COL_EXPORTED_AT,
                crate::types::RivetType::Timestamp {
                    unit: crate::types::TimeUnit::Microsecond,
                    timezone: Some("UTC".into()),
                },
            ));
        }
        if export.meta_columns.row_hash.enabled() {
            meta_specs.push((crate::enrich::COL_ROW_HASH, crate::types::RivetType::Int64));
        }
        if !meta_specs.is_empty() {
            let target = crate::types::target::ExportTarget::parse(load.target.name())
                .with_context(|| format!("unknown load target `{}`", load.target.name()))?;
            for (name, rivet_type) in &meta_specs {
                specs.push(target.resolve_column(crate::types::target::TargetInput {
                    column_name: name,
                    rivet_type,
                    arrow_type: None,
                    fidelity: crate::types::TypeFidelity::Exact,
                }));
            }
        }

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
            crate::config::ExportMode::TimeWindow => {
                // Full OVERWRITE by design — and said out loud (round-6): each
                // load replaces the warehouse table with the CURRENT window, so
                // history past `days_window` is capped, not accumulated. An
                // accumulation-minded operator loses history silently otherwise.
                eprintln!(
                    "  note: export `{}` is mode: time_window — each load OVERWRITES the \
                     warehouse table with the current window; rows older than the window \
                     are dropped from the warehouse (append-history needs mode: \
                     incremental).",
                    export.name
                );
                LoadMode::Full
            }
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
            export_name: export.name.clone(),
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
    use crate::types::target::TargetStatus;

    /// A schema-qualified source table must not leak its dot into the warehouse
    /// address, and two schemas that share a table name must stay apart.
    ///
    /// The loaders build `{project}.{dataset}.{table}`, so `public.orders` made
    /// four segments and BigQuery read the extra one as part of the dataset:
    /// `Not found: Dataset rivet-data-tool:rivet_e2e.public` — a hard failure
    /// naming a dataset the operator never wrote.
    ///
    /// The second assertion is the one that matters more: folding to `orders`
    /// would have fixed the error and collapsed two schemas onto one warehouse
    /// table, trading a loud failure for a silent overwrite.
    #[test]
    fn a_schema_qualified_table_folds_instead_of_splitting_the_dataset() {
        assert_eq!(warehouse_table_name("public.orders", "e"), "public_orders");
        assert_ne!(
            warehouse_table_name("public.orders", "e"),
            warehouse_table_name("archive.orders", "e"),
            "two schemas with the same table name must stay DISTINCT in the warehouse — \
             collapsing them is a silent overwrite wearing a bugfix"
        );
        assert_eq!(
            warehouse_table_name("orders", "e"),
            "orders",
            "a bare name is untouched: nothing that works today may change"
        );
    }

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

    #[test]
    fn resolve_load_prefix_expands_deterministic_tokens_and_refuses_run_specific() {
        use crate::config::{DestinationConfig, DestinationType};
        let dest = |prefix: &str| DestinationConfig {
            destination_type: DestinationType::Gcs,
            bucket: Some("BKT".into()),
            prefix: Some(prefix.into()),
            ..Default::default()
        };

        // {export}/{table} are deterministic → the load must list the ACTUAL
        // prefix, not the literal token. (#100: the load listed `exports/{export}/`
        // verbatim, found no manifests, and reported "up to date" — loaded nothing.)
        assert_eq!(
            resolve_load_prefix(&dest("exports/{export}/"), "orders", "BKT").unwrap(),
            "gs://BKT/exports/orders/"
        );
        // {partition} is stripped; {table} is an alias for {export}.
        assert_eq!(
            resolve_load_prefix(&dest("e/{table}/{partition}/"), "orders", "BKT").unwrap(),
            "gs://BKT/e/orders/"
        );
        // {date} in the load base is DAY-specific → refused (bughunt HIGH: it
        // expanded to the LOAD day, so a cross-midnight load silently listed an
        // empty prefix). See resolve_load_prefix_refuses_day_specific_date_in_the_base.
        assert!(resolve_load_prefix(&dest("d/{date}/{export}/"), "orders", "BKT").is_err());

        // {run_id} is run-specific and unknowable here → refuse LOUD, never a
        // literal-token listing that silently loads nothing.
        let err = resolve_load_prefix(&dest("e/{run_id}/"), "orders", "BKT").unwrap_err();
        assert!(
            err.to_string().contains("unresolved placeholder"),
            "a run-specific token must be refused, not silently listed: {err}"
        );
    }

    use crate::preflight::type_report::{ExportTypeReport, TypeReportRow};
    use crate::types::TypeFidelity;
    use crate::types::target::{ExportTarget, TargetInput};

    /// A report row as `type_report::collect_report` builds one — from the
    /// resolver's OWN [`TargetColumnSpec`], split across the row's optional
    /// fields by that function's rule (`autoload_type` carried ONLY when it
    /// diverges from the native type). This is the shape `plan_loads` now
    /// receives in process, so the test feeds `build_plans` what the real
    /// producer produces rather than a hand-typed subset of it.
    fn row_from_spec(spec: &TargetColumnSpec) -> TypeReportRow {
        TypeReportRow {
            column: spec.column_name.clone(),
            source_type: "-".into(),
            rivet_type: "-".into(),
            arrow_type: "-".into(),
            fidelity: TypeFidelity::Exact,
            warnings: vec![],
            target_type: Some(spec.target_type.clone()),
            target_status: Some(spec.status),
            target_note: spec.note.clone(),
            autoload_type: (spec.autoload_type != spec.target_type)
                .then(|| spec.autoload_type.clone()),
            cast_sql: spec.cast_sql.clone(),
        }
    }

    /// A report row with an explicit `target_status` (type irrelevant).
    fn col(name: &str, status: TargetStatus) -> TypeReportRow {
        row_from_spec(&TargetColumnSpec {
            column_name: name.into(),
            target_type: "STRING".into(),
            autoload_type: "STRING".into(),
            status,
            note: None,
            cast_sql: None,
        })
    }

    /// One export's report, as the resolver returns it.
    fn report(export: &str, columns: Vec<TypeReportRow>) -> ExportTypeReport {
        ExportTypeReport {
            export: export.into(),
            table: None,
            columns,
            violations: vec![],
            target_failures: false,
            recovery_sql: None,
        }
    }

    /// One TABLE'S report of a multiplex `tables:` export, as the resolver
    /// returns one per captured table (`table: Some(..)`, `export` still the
    /// export's own name).
    fn table_report(export: &str, table: &str, columns: Vec<TypeReportRow>) -> ExportTypeReport {
        ExportTypeReport {
            table: Some(table.into()),
            ..report(export, columns)
        }
    }

    /// Drive the PURE `build_plans` (no `rivet` subprocess) — the deepened core
    /// of `plan_loads`. Kills the mutation survivors that live in the per-export
    /// resolution: the export→report name match (`==`→`!=`) and the `fail`/`warn`
    /// `target_status` arms. Also pins mode mapping, the `gs://` prefix, table
    /// resolution, and the cursor column.
    /// Round-6 HIGH: a non-GCS destination with a `load:` block used to build a
    /// GCS client anyway — a same-named FOREIGN GCS bucket got listed (empty →
    /// "up to date" forever, exit 0; cleanup/gc would target it). RED against
    /// removing the destination_type gate in build_plans.
    #[test]
    fn a_load_block_on_a_non_gcs_destination_is_refused() {
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
      type: s3
      bucket: b1
      prefix: exports/alpha/
load:
  target: bigquery
  project: p
  dataset: d
"#,
        )
        .unwrap();
        let load: LoadSection = serde_json::from_value(cfg.load.clone().unwrap()).unwrap();
        let reports = vec![report("alpha", vec![col("id", TargetStatus::Ok)])];
        let err = build_plans(&cfg, &load, reports)
            .expect_err("s3 + load: must refuse at plan time")
            .to_string();
        assert!(
            err.contains("S3") && err.contains("load"),
            "must name the mismatch and the block: {err}"
        );
    }

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
            report(
                "beta",
                vec![
                    col("id", TargetStatus::Ok),
                    col("f", TargetStatus::Fail),
                    col("w", TargetStatus::Warn),
                ],
            ),
            report("alpha", vec![col("id", TargetStatus::Ok)]),
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

    /// `_rivet_row_hash` is written by rivet at extraction, so it never appears
    /// in the source column report. Without a spec the warehouse table is
    /// created without the column and the FIRST load fails on a schema mismatch
    /// — after the extract has already been paid for.
    #[test]
    fn build_plans_appends_a_spec_for_the_extraction_hash() {
        let yaml = |hash_block: &str| {
            format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
                 exports:\n  - name: a\n    table: t\n    mode: full\n    format: parquet\n\
                 \x20   destination:\n      type: gcs\n      bucket: b\n      prefix: p/\n{hash_block}\
                 load:\n  target: bigquery\n  project: p\n  dataset: d\n"
            )
        };
        let reports = || {
            vec![report(
                "a",
                vec![col("id", TargetStatus::Ok), col("status", TargetStatus::Ok)],
            )]
        };

        let without = crate::config::Config::from_yaml(&yaml("")).unwrap();
        let load: LoadSection = serde_json::from_value(without.load.clone().unwrap()).unwrap();
        let plans = build_plans(&without, &load, reports()).unwrap();
        assert_eq!(
            plans[0]
                .specs
                .iter()
                .map(|s| s.column_name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "status"],
            "no row_hash configured ⇒ no extra column"
        );

        let with = crate::config::Config::from_yaml(&yaml(
            "    meta_columns:\n      row_hash: [id, status]\n",
        ))
        .unwrap();
        let load: LoadSection = serde_json::from_value(with.load.clone().unwrap()).unwrap();
        let plans = build_plans(&with, &load, reports()).unwrap();
        let last = plans[0].specs.last().unwrap();
        assert_eq!(last.column_name, crate::enrich::COL_ROW_HASH);
        // Resolved through the per-target resolver, not hardcoded — BigQuery's
        // 64-bit integer. A Snowflake target would resolve NUMBER(38,0) here.
        assert_eq!(last.target_type, "INT64");
        assert_eq!(last.status, TargetStatus::Ok);

        // …and the SIBLING meta column, written by the identical seam, needs a
        // spec for the identical reason. `_rivet_row_hash` got one and
        // `_rivet_exported_at` did not, which is the same bug twice: both are
        // produced at extraction, so neither can ever appear in the SOURCE column
        // report, and a missing spec fails the first load on a schema mismatch
        // after the extract was paid for. Asserted as the ORDERED tail so it also
        // pins the order against `enrich_schema`'s (exported_at, then row_hash) —
        // a spec list in the other order describes a different Parquet.
        let both = crate::config::Config::from_yaml(&yaml(
            "    meta_columns:\n      exported_at: true\n      row_hash: true\n",
        ))
        .unwrap();
        let load: LoadSection = serde_json::from_value(both.load.clone().unwrap()).unwrap();
        let plans = build_plans(&both, &load, reports()).unwrap();
        let names: Vec<&str> = plans[0]
            .specs
            .iter()
            .map(|s| s.column_name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "id",
                "status",
                crate::enrich::COL_EXPORTED_AT,
                crate::enrich::COL_ROW_HASH,
            ],
            "every extraction-written meta column needs a spec, in enrich_schema's order"
        );
        let ts = plans[0].specs.iter().rev().nth(1).unwrap();
        assert_eq!(
            ts.column_name,
            crate::enrich::COL_EXPORTED_AT,
            "the timestamp spec sits before the hash spec"
        );
        assert_eq!(
            ts.target_type, "TIMESTAMP",
            "resolved through the per-target resolver, not hardcoded — BigQuery's \
             instant type for a Timestamp(us, UTC)"
        );
    }

    /// A multiplex `tables:` CDC config — the shape `rivet init --mode cdc`
    /// emits for a whole schema (#252).
    fn multiplex_cfg() -> crate::config::Config {
        crate::config::Config::from_yaml(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: cdc
    tables: [orders, customers, line_items]
    mode: cdc
    format: parquet
    cdc:
      checkpoint: ./cdc.ckpt
      initial: snapshot
    destination:
      type: gcs
      bucket: b
      prefix: exports/{export}/
load:
  target: bigquery
  project: p
  dataset: d
  pk: [id]
"#,
        )
        .unwrap()
    }

    /// A multiplex `tables:` CDC export is N source tables through ONE export,
    /// and `rivet load` must build ONE PLAN PER TABLE (#252).
    ///
    /// The two halves this pins are the two ways the fan-out can be dropped, and
    /// each fails differently:
    ///
    /// - drop `report.table` from the warehouse-table resolution and all N
    ///   collapse onto the EXPORT's name — three source tables merged into one
    ///   BigQuery table (here caught by `reject_duplicate_target_tables`, but
    ///   only because the collision is exact);
    /// - drop the per-table sub-prefix and all N point at the export BASE, whose
    ///   manifest listing is RECURSIVE — so every plan sweeps in every sibling
    ///   table's parts and loads them all into its own table, with every count
    ///   agreeing on the way through. That one is silent, which is why the prefix
    ///   assertion is per-table and exact, not a `contains`.
    ///
    /// The `Some(table)` values are NOT hand-typed: they come from
    /// `ExportConfig::multiplex_tables()`, the one function that decides whether
    /// an export is one unit or N — so a mutant there (returning `None`, dropping
    /// the CDC-mode gate) turns this red at the producer, not just the consumer.
    #[test]
    fn build_plans_fans_a_multiplex_tables_export_out_to_one_plan_per_table() {
        let cfg = multiplex_cfg();
        let load: LoadSection = serde_json::from_value(cfg.load.clone().unwrap()).unwrap();
        let tables = cfg.exports[0]
            .multiplex_tables()
            .expect("a `mode: cdc` export with `tables:` IS a multiplex")
            .to_vec();
        assert_eq!(tables.len(), 3, "fixture must cross the fan-out threshold");

        let reports: Vec<_> = tables
            .iter()
            .map(|t| table_report("cdc", t, vec![col("id", TargetStatus::Ok)]))
            .collect();
        let plans = build_plans(&cfg, &load, reports).unwrap();

        assert_eq!(
            plans.len(),
            3,
            "one plan per captured table, not one per export"
        );
        assert_eq!(
            plans.iter().map(|p| p.table.as_str()).collect::<Vec<_>>(),
            vec!["orders", "customers", "line_items"],
            "the warehouse table is the SOURCE table, not the export name"
        );
        // Each table's own sub-prefix — the layout the extract wrote
        // (`cdc_job::dest_for_table`) and `rivet validate` descends. The `{export}`
        // token still expands first, so the base is the export's, not the literal.
        assert_eq!(
            plans
                .iter()
                .map(|p| p.gcs_prefix.as_str())
                .collect::<Vec<_>>(),
            vec![
                "gs://b/exports/cdc/orders/",
                "gs://b/exports/cdc/customers/",
                "gs://b/exports/cdc/line_items/",
            ],
            "each plan must list ONLY its own table's prefix — a shared base lists \
             every sibling's parts recursively and merges them into one table"
        );
        for p in &plans {
            assert_eq!(p.mode, LoadMode::Cdc);
            assert_eq!(
                p.export_name, "cdc",
                "the plans still address the EXPORT the operator wrote"
            );
            assert_eq!(p.load.pk, vec!["id"], "the shared `load:` applies to each");
        }
    }

    /// The multiplex sub-prefix rule itself: the table becomes ONE path segment
    /// under the resolved base, with both slashes supplied — a cloud prefix is a
    /// literal key prefix, so a missing separator lists a mangled flat key
    /// (`…/cdccdc-0.parquet`) and the load silently reports "up to date".
    #[test]
    fn table_load_prefix_appends_one_slash_delimited_segment() {
        assert_eq!(
            table_load_prefix("gs://b/exports/cdc/", "orders").unwrap(),
            "gs://b/exports/cdc/orders/"
        );
        // A base written without its trailing slash must not fuse the segment on.
        assert_eq!(
            table_load_prefix("gs://b/exports/cdc", "orders").unwrap(),
            "gs://b/exports/cdc/orders/"
        );
        // A schema-qualified table stays VERBATIM in the path — the dot is folded
        // only in the warehouse NAME (`warehouse_table_name`); the extract wrote
        // the raw name as its directory.
        assert_eq!(
            table_load_prefix("gs://b/e/", "public.orders").unwrap(),
            "gs://b/e/public.orders/"
        );
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
        let reports = vec![report("ghost", vec![])];
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

    #[test]
    fn resolve_load_prefix_refuses_day_specific_date_in_the_base() {
        // #bughunt HIGH: {date} expands to the LOAD day, so a nightly export + an
        // after-midnight load list DIFFERENT prefixes → silent "up to date". Refuse
        // {date} in the load base; a static base (or {date} below {partition}) is ok.
        let dest = |p: &str| crate::config::DestinationConfig {
            prefix: Some(p.to_string()),
            ..Default::default()
        };
        let err = resolve_load_prefix(&dest("exports/{date}/{export}/"), "orders", "bkt")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("{date}") && err.contains("load base"),
            "must refuse a day-specific date base: {err}"
        );
        assert!(resolve_load_prefix(&dest("exports/{export}/"), "orders", "bkt").is_ok());
        // {date} BELOW {partition} is not in the listed base — allowed.
        assert!(
            resolve_load_prefix(
                &dest("exports/{export}/{partition}/{date}/"),
                "orders",
                "bkt"
            )
            .is_ok()
        );
    }

    #[test]
    fn cross_warehouse_load_fields_are_rejected() {
        // #dogfood LOW: `#[serde(flatten)]` on the target enum disables
        // deny_unknown_fields, so a `target: snowflake` block silently accepted
        // (and ignored) BigQuery's `project:`/`dataset:`. Now a loud error.
        let snow_with_bq = serde_json::json!({
            "target": "snowflake", "connection": "c", "warehouse": "w",
            "database": "db", "schema": "s", "storage_integration": "si",
            "project": "STALE", "dataset": "STALE"
        });
        let err = reject_foreign_target_fields(&snow_with_bq, "snowflake", "top-level")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("project") && err.contains("bigquery"),
            "snowflake+project must name the foreign field and warehouse: {err}"
        );
        // The reverse: bigquery target carrying a snowflake-only field.
        let bq_with_snow = serde_json::json!({
            "target": "bigquery", "project": "p", "dataset": "d", "warehouse": "WH"
        });
        assert!(reject_foreign_target_fields(&bq_with_snow, "bigquery", "top-level").is_err());
        // A clean, target-matching block passes.
        let clean = serde_json::json!({ "target": "bigquery", "project": "p", "dataset": "d" });
        assert!(reject_foreign_target_fields(&clean, "bigquery", "top-level").is_ok());
    }

    /// The report row IS the resolver's spec, split across optional fields — so
    /// the plan must rebuild the WHOLE spec, not the two fields the old JSON
    /// mirror declared.
    ///
    /// `plan_loads` used to obtain this data by parsing `rivet check --json`
    /// through a four-field `ColReport`; everything the mirror did not name was
    /// dropped on the floor (`note: None`, `cast_sql: None`, `autoload_type:
    /// String::new()`) — an empty autoload type claims the warehouse autoloads
    /// the column as `""`, and the L5 recovery hint the resolver computed never
    /// reached the plan. In process there is no mirror to forget a field.
    ///
    /// The oracle is the RESOLVER's own output, not a hand-typed string: build
    /// the spec with `ExportTarget::resolve_column`, split it the way
    /// `collect_report` does, and assert the plan's spec is field-for-field the
    /// one we started from. The JSON column is chosen because BigQuery's
    /// autoload DIVERGES for it (JSON → BYTES, with a cast + note), so the
    /// fixture crosses the threshold where the dropped fields are observable —
    /// a column whose autoload matches its native type could not tell the two
    /// implementations apart.
    #[test]
    fn build_plans_carries_the_resolvers_whole_spec_not_a_two_field_subset() {
        let cfg = crate::config::Config::from_yaml(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: a\n    table: t\n    mode: full\n    format: parquet\n    \
             destination:\n      type: gcs\n      bucket: b\n      prefix: p/\nload:\n  \
             target: bigquery\n  project: p\n  dataset: d\n",
        )
        .unwrap();
        let load: LoadSection = serde_json::from_value(cfg.load.clone().unwrap()).unwrap();

        let resolved = ExportTarget::BigQuery.resolve_column(TargetInput {
            column_name: "payload",
            rivet_type: &crate::types::RivetType::Json,
            arrow_type: None,
            fidelity: TypeFidelity::Exact,
        });
        // Fixture non-vacuity: if BigQuery ever autoloaded JSON faithfully this
        // test would pass against BOTH implementations and prove nothing.
        assert_ne!(
            resolved.autoload_type, resolved.target_type,
            "fixture must be a column whose autoload DIVERGES"
        );
        assert!(
            resolved.cast_sql.is_some() && resolved.note.is_some(),
            "fixture must carry the recovery hint + note the old mirror dropped"
        );

        let plans = build_plans(
            &cfg,
            &load,
            vec![report("a", vec![row_from_spec(&resolved)])],
        )
        .unwrap();
        let got = &plans[0].specs[0];
        assert_eq!(got.column_name, resolved.column_name);
        assert_eq!(got.target_type, resolved.target_type);
        assert_eq!(
            got.autoload_type, resolved.autoload_type,
            "the autoload type must survive the report round-trip (it was `String::new()`)"
        );
        assert_eq!(got.status, resolved.status);
        assert_eq!(
            got.note, resolved.note,
            "the resolver's note must reach the plan (it was `None`)"
        );
        assert_eq!(
            got.cast_sql, resolved.cast_sql,
            "the L5 recovery hint must reach the plan (it was `None`)"
        );

        // A column whose autoload does NOT diverge: the report omits
        // `autoload_type` entirely, and "absent" must mean "same as native",
        // never the empty string.
        let plain = ExportTarget::BigQuery.resolve_column(TargetInput {
            column_name: "id",
            rivet_type: &crate::types::RivetType::Int64,
            arrow_type: None,
            fidelity: TypeFidelity::Exact,
        });
        assert_eq!(plain.autoload_type, plain.target_type, "fixture premise");
        let plans =
            build_plans(&cfg, &load, vec![report("a", vec![row_from_spec(&plain)])]).unwrap();
        assert_eq!(plans[0].specs[0].autoload_type, plain.target_type);
    }

    /// A column the resolver could not type must be NAMED, never defaulted.
    ///
    /// `target_type`/`target_status` are `Some` together for every column
    /// resolved against a target, and a load always resolves with one — but a
    /// `None` defaulted to `TargetStatus::Ok` would walk an unmappable column
    /// straight past `validate_specs` (whose whole job is to refuse a `Fail`),
    /// which is the silent-loss shape, not a tidier default.
    #[test]
    fn build_plans_refuses_a_column_the_resolver_did_not_type() {
        let cfg = crate::config::Config::from_yaml(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: a\n    table: t\n    mode: full\n    format: parquet\n    \
             destination:\n      type: gcs\n      bucket: b\n      prefix: p/\nload:\n  \
             target: bigquery\n  project: p\n  dataset: d\n",
        )
        .unwrap();
        let load: LoadSection = serde_json::from_value(cfg.load.clone().unwrap()).unwrap();
        let mut untyped = col("mystery", TargetStatus::Ok);
        untyped.target_type = None;
        untyped.target_status = None;
        let err = build_plans(&cfg, &load, vec![report("a", vec![untyped])])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("mystery") && err.contains("bigquery"),
            "must name the column and the target, not guess a status: {err}"
        );
    }

    /// Write `yaml` to a temp file and hand back the dir (kept alive) + path.
    fn cfg_file(yaml: &str) -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rivet.yaml");
        std::fs::write(&path, yaml).unwrap();
        let s = path.to_string_lossy().to_string();
        (dir, s)
    }

    /// A source URL nothing can be listening on — port 1 refuses instantly.
    const CLOSED_SOURCE: &str = "postgresql://u:p@127.0.0.1:1/db";

    /// The config-level `load:` gates run BEFORE any source I/O.
    ///
    /// The source here is a closed port, so if `plan_loads` reached the type
    /// resolver first the error would be a connection failure; that it names
    /// the `load:` problem instead is the ordering proof. This is also the first
    /// test of ANY kind to call `plan_loads` — while the type report came from a
    /// subprocess the function could not be entered without a `rivet` binary on
    /// disk and a live source.
    ///
    /// Honest about which half a mutant can move: the missing-block arm is
    /// ordered by construction (the target comes FROM the block, so nothing can
    /// resolve types before it parses), and only pins the message. The typo arm
    /// is the graded one — deleting `check_load_keys` from `plan_loads` takes
    /// the run past the gate and into the closed-port connect (RED-proven:
    /// "resolving column types for the bigquery load: Connection refused").
    #[test]
    fn plan_loads_gates_the_load_block_before_any_source_io() {
        let (_dir, path) = cfg_file(&format!(
            "source:\n  type: postgres\n  url: \"{CLOSED_SOURCE}\"\n\
             exports:\n  - name: a\n    table: t\n    mode: full\n    format: parquet\n    \
             destination:\n      type: gcs\n      bucket: b\n      prefix: p/\n"
        ));
        let err = plan_loads(&path).unwrap_err().to_string();
        assert!(
            err.contains("no top-level `load:` block"),
            "the config gate must answer first, before the source is dialled: {err}"
        );

        // …and a typo'd key in the block is refused the same way.
        let (_dir, path) = cfg_file(&format!(
            "source:\n  type: postgres\n  url: \"{CLOSED_SOURCE}\"\n\
             exports:\n  - name: a\n    table: t\n    mode: full\n    format: parquet\n    \
             destination:\n      type: gcs\n      bucket: b\n      prefix: p/\nload:\n  \
             target: bigquery\n  project: p\n  dataset: d\n  gc_orphan: true\n"
        ));
        let err = plan_loads(&path).unwrap_err().to_string();
        assert!(err.contains("gc_orphan"), "{err}");
    }

    /// The type report is resolved IN PROCESS — no `rivet check` subprocess.
    ///
    /// `plan_loads` used to spawn `rivet check --target X --json` and parse its
    /// stdout, so this step could not be exercised at all without a `rivet`
    /// binary (and every failure arrived wearing the child's clothes: "running
    /// `rivet check` — is rivet on PATH? pass --rivet-bin", or a JSON parse
    /// error over the child's stderr). Now the resolver runs here, so the step
    /// is reachable offline and its failure names the EXPORT and the target.
    ///
    /// The source is a closed port: the assertion is not about the connection
    /// error's wording (that is the driver's) but about which layer reports it.
    #[test]
    fn plan_loads_resolves_types_in_process_never_a_rivet_check_subprocess() {
        let (_dir, path) = cfg_file(&format!(
            "source:\n  type: postgres\n  url: \"{CLOSED_SOURCE}\"\n\
             exports:\n  - name: orders\n    table: t\n    mode: full\n    format: parquet\n    \
             destination:\n      type: gcs\n      bucket: b\n      prefix: p/\nload:\n  \
             target: bigquery\n  project: p\n  dataset: d\n"
        ));
        let err = plan_loads(&path).unwrap_err().to_string();
        assert!(
            err.contains("orders") && err.contains("resolving column types"),
            "the in-process resolver must report the failing export: {err}"
        );
        assert!(
            err.contains("bigquery"),
            "…and which target it was resolving for: {err}"
        );
        assert!(
            !err.contains("rivet check") && !err.contains("--rivet-bin"),
            "no subprocess is involved any more — nothing may blame one: {err}"
        );
    }
}
