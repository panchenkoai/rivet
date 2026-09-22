//! Config-driven load planning — derive a BigQuery load (native schema, table,
//! partition, source URIs) from a rivet export config, so a client never
//! hand-types column types. The schema comes from the load spec `rivet run`
//! recorded in the state DB (`preflight::load_type_reports`), so the load never
//! reads the source; the table/partition/destination come from the parsed config.
//!
//! Until 0.24.x this shelled out to `rivet check --json` and parsed its stdout.
//! A subprocess resolves types with whatever binary it names, so version skew
//! was a live failure mode — `--rivet-bin` existed only to mitigate it, and one
//! config was parsed TWICE (once here, once in the child) with different
//! `${VAR}` resolution. Both are gone: one parse, one resolver, no argv.

use crate::types::target::TargetColumnSpec;
use anyhow::{Context, Result, bail};

use crate::config::load::HOURLY_LIFETIME_DAYS;
use crate::config::load::MAX_TABLE_PARTITIONS;
pub use crate::config::load::{
    Granularity, KeyColumns, LoadSection, LoadTarget, PartitionForm, PartitionSpec,
};

/// A resolved `partition:`: what the table is partitioned on, the expression that
/// creates it, and its options.
#[derive(Debug, Clone, PartialEq)]
pub struct TablePartition {
    pub key: PartitionKey,
    /// The warehouse's `PARTITION BY` expression (BigQuery) or clustering expression (Snowflake).
    pub expr: String,
    pub expiration_days: Option<u32>,
    pub require_filter: bool,
}

/// What a table is partitioned on, as the warehouse records it.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionKey {
    /// A time column at a granularity; `column: None` is the load (ingestion) time.
    Time {
        column: Option<String>,
        granularity: Granularity,
    },
    Range {
        column: String,
        start: i64,
        end: i64,
        interval: i64,
    },
}

impl PartitionKey {
    /// The partition column, when the key is one.
    pub fn column(&self) -> Option<&str> {
        match self {
            PartitionKey::Time { column, .. } => column.as_deref(),
            PartitionKey::Range { column, .. } => Some(column),
        }
    }

    /// Whether the key is a load date: the load time, or rivet's extraction stamp
    /// `_rivet_exported_at`, which every row of a run shares.
    pub fn is_load_date(&self) -> bool {
        match self {
            PartitionKey::Time { column: None, .. } => true,
            PartitionKey::Time {
                column: Some(c), ..
            } => c == crate::enrich::COL_EXPORTED_AT,
            PartitionKey::Range { .. } => false,
        }
    }

    /// Whether partitions of this key can expire: BigQuery has no expiry for integer ranges.
    pub fn takes_expiry(&self) -> bool {
        !matches!(self, PartitionKey::Range { .. })
    }

    /// `` `ts` by day ``, `load time by hour`, `` `n` in steps of 10 from 0 to 1000 ``.
    pub fn describe(&self) -> String {
        match self {
            PartitionKey::Time {
                column: Some(c),
                granularity,
            } => format!("`{c}` by {}", granularity.as_str()),
            PartitionKey::Time {
                column: None,
                granularity,
            } => format!("load time by {}", granularity.as_str()),
            PartitionKey::Range {
                column,
                start,
                end,
                interval,
            } => format!("`{column}` in steps of {interval} from {start} to {end}"),
        }
    }

    /// The same column (case-insensitively) at the same granularity or range.
    pub fn same_as(&self, other: &PartitionKey) -> bool {
        let same_col = |a: &Option<String>, b: &Option<String>| match (a, b) {
            (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
            (None, None) => true,
            _ => false,
        };
        match (self, other) {
            (
                PartitionKey::Time {
                    column: a,
                    granularity: ga,
                },
                PartitionKey::Time {
                    column: b,
                    granularity: gb,
                },
            ) => same_col(a, b) && ga == gb,
            (
                PartitionKey::Range {
                    column: a,
                    start: sa,
                    end: ea,
                    interval: ia,
                },
                PartitionKey::Range {
                    column: b,
                    start: sb,
                    end: eb,
                    interval: ib,
                },
            ) => a.eq_ignore_ascii_case(b) && sa == sb && ea == eb && ia == ib,
            _ => false,
        }
    }
}

/// Source primary keys `rivet run` recorded, by `(export, unit)`.
pub type RecordedKeys = std::collections::HashMap<(String, Option<String>), Vec<String>>;

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

/// How a CDC export's tables are laid out in the warehouse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcLayout {
    /// Baseline and changes in one `<table>__changes` log; `<table>` is the dedup
    /// view over it (`initial: snapshot`, and every stream without a baseline).
    LogAndView,
    /// `<table>` is a physical base (source schema + `__is_deleted`) the baseline
    /// legs overwrite; `<table>__changes` is a per-cycle buffer `rivet compact`
    /// merges into the base and drops (`backfill:` streams).
    BaseAndBuffer,
}

impl CdcLayout {
    /// The `<table>__changes` log is a disposable per-cycle buffer: no partition
    /// declaration, no option settling, no `__rebuild` leftovers to look for —
    /// `compact` drops it whole.
    pub fn log_is_disposable(self) -> bool {
        matches!(self, CdcLayout::BaseAndBuffer)
    }

    /// `rivet compact` has something to merge for this layout: the base is a
    /// physical table the legs overwrote, the log a buffer of changes since the
    /// last merge. The changelog + view layout keeps its state in the log itself.
    pub fn compacts(self) -> bool {
        matches!(self, CdcLayout::BaseAndBuffer)
    }
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
    /// The captured source table for a multiplex `tables:` export, `None` for a
    /// single-relation one — the `unit` the state DB keys its load specs by.
    pub unit: Option<String>,
    pub table: String,
    /// The resolved `load.partition` of the table the load writes.
    pub partition: Option<TablePartition>,
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
    /// The resolved dedup key: the configured `pk`, or the recorded source key for `auto`.
    pub pk: Vec<String>,
    /// The clustering of the table the load writes.
    pub clustering: Clustering,
    /// The run this plan was typed from — `(run_id, finished_at)` — once the load
    /// pinned it; a run that finishes after it is refused for this cycle.
    pub pinned_run: Option<(String, String)>,
    /// Where a CDC table's baseline lives (see [`CdcLayout`]); `LogAndView` for
    /// every non-CDC mode.
    pub layout: CdcLayout,
    /// Whether the base carries `__is_deleted`. A stream expresses deletes, so it
    /// defaults ON there and OFF for a query-based export; `load.deleted_flag`
    /// decides when written.
    pub deleted_flag: bool,
    /// Columns whose Parquet name has Cyrillic look-alikes, as (file name, warehouse name).
    pub renames: Vec<Rename>,
    /// One warning per renamed column, naming the fix to run on the source.
    pub rename_warnings: Vec<String>,
}

impl LoadPlan {
    /// The spec's column names as the Parquet carries them, before any look-alike rename.
    pub fn file_column_names(&self) -> Vec<String> {
        self.specs
            .iter()
            .map(|s| file_name(&self.renames, &s.column_name).to_string())
            .collect()
    }
}

/// The clustering columns of the table a load writes, and where they came from: a
/// change log follows a `Written` clustering and keeps its own under `Auto`.
#[derive(Debug, Clone, PartialEq)]
pub enum Clustering {
    /// Resolved from `cluster_by: auto` — the recorded source key, or nothing.
    Auto(Vec<String>),
    /// Written in the config: a column list, or `none`.
    Written(Vec<String>),
}

impl Clustering {
    pub fn columns(&self) -> &[String] {
        match self {
            Clustering::Auto(c) | Clustering::Written(c) => c,
        }
    }

    /// Whether the config wrote the clustering rather than leaving it at `auto`.
    pub fn is_written(&self) -> bool {
        matches!(self, Clustering::Written(_))
    }
}

/// Resolve a rivet config into **one [`LoadPlan`] per export** — the shared
/// top-level `load:` target plus each export's own table / partition / GCS
/// destination / native schema. The type resolver returns one report per
/// export, so a multi-table config produces a plan per table, all pointed
/// at the same warehouse target.
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

    // The `load:` target from the same config, shared by every export.
    let load = cfg.load.clone().context(
        "config has no top-level `load:` block — add `load: { target, ... }` to load into a warehouse",
    )?;

    let target = crate::types::target::ExportTarget::parse(load.target.name())
        .with_context(|| format!("unknown load target `{}`", load.target.name()))?;
    let state = crate::state::StateStore::open(config_path)
        .context("opening the state DB, which holds the column types `rivet run` recorded")?;
    let (reports, keys) = crate::preflight::load_type_reports(&cfg, &state, target)?;

    // Deferred: this plan is typed from the BY-NAME spec, which the load then pins
    // to the run it consumes (`orchestrate::pin_plan_to_its_run`) — a same-named
    // export of another config may have written that row, and its columns are not
    // this table's. The fit is checked strictly after the pin, or by
    // `check_spec_fit` when no pin is possible.
    build_plans_keyed(&cfg, &load, reports, &keys, SpecFit::Deferred)
}

/// The warehouse layout of one export: a `backfill:` stream keeps a physical base
/// and a disposable change buffer; everything else is the changelog + view.
/// An export mode's load strategy. Exhaustive (no `_`) on purpose: a future
/// delta-style mode fails to COMPILE here until someone picks its load
/// semantics, instead of silently defaulting to OVERWRITE (the
/// incremental-overwrite data-loss class).
pub fn load_mode_of(mode: crate::config::ExportMode) -> LoadMode {
    match mode {
        crate::config::ExportMode::Cdc => LoadMode::Cdc,
        crate::config::ExportMode::Incremental => LoadMode::Incremental,
        crate::config::ExportMode::Full => LoadMode::Full, // whole result set
        crate::config::ExportMode::Chunked => LoadMode::Full, // parallel full snapshot
        crate::config::ExportMode::TimeWindow => LoadMode::Full, // the current window, whole
    }
}

/// One export's effective load settings: the shared section, the export's own `load:`
/// block layered over it, then — for a captured table of a multiplex stream — that
/// table's `load.tables.<name>` block over both.
///
/// THE overlay. It was written out four times (three `resolved_*` readers and the plan
/// builder), and only the builder's copy applied the third layer, so the load honoured a
/// per-table override the extract never saw.
pub(crate) fn overlay(
    section: &LoadSection,
    export: &crate::config::ExportConfig,
    table: Option<&str>,
) -> LoadSection {
    let mut eff = match &export.load {
        Some(o) => section.with_override(o),
        None => section.clone(),
    };
    if let (Some(o), Some(t)) = (&export.load, table)
        && let Some(per_table) = o.tables.get(t)
    {
        eff = eff.with_override(per_table);
    }
    eff
}

/// [`overlay`] against the config's shared `load:` block; `None` when there is none.
pub fn effective_load(
    config: &crate::config::Config,
    export: &crate::config::ExportConfig,
    table: Option<&str>,
) -> Option<LoadSection> {
    config.load.as_ref().map(|s| overlay(s, export, table))
}

/// Whether THIS export's base carries the delete flag, from the config alone — the extract
/// asks, because only the writer can put a constant column in the file.
///
/// `table` names the captured table when the caller has one (a multiplex stream stamps per
/// table); `None` answers for the export as a whole.
pub fn resolved_deleted_flag(
    config: &crate::config::Config,
    export: &crate::config::ExportConfig,
    table: Option<&str>,
) -> bool {
    effective_load(config, export, table)
        .and_then(|eff| eff.deleted_flag)
        .unwrap_or(matches!(load_mode_of(export.mode), LoadMode::Cdc))
}

/// Whether the BASE table this export lands carries `__is_deleted` as data.
///
/// Only a base-and-buffer layout has a base to carry it: under the log-and-view layout the
/// flag lives in the changelog, and a base that is really a view cannot hold a column at
/// all. Named and offline-graded because every caller — the CDC baseline leg, the
/// incremental whole pass and the compaction — is a live-only body, where an inline
/// `&&` is a decision the mutation corpus excludes with nothing asked in return. Two
/// of the three know the layout statically and pass `true`; the third computes it.
pub fn base_carries_delete_flag(base_and_buffer: bool, deleted_flag: bool) -> bool {
    base_and_buffer && deleted_flag
}

/// Whether a whole-table pass may be folded into the changelog instead of landing as the
/// base table.
///
/// Folding is the log-and-view layout's answer, where the name is a view and cannot be
/// overwritten. Under base-and-buffer the first pass IS the base, so it must land as a
/// table and never join the log.
///
/// Takes only the layout: whether a first pass EXISTS is carried by the `Option` the
/// caller filters, so asking for it again here would put the same decision in two places.
pub fn whole_table_pass_may_join_the_log(base_and_buffer: bool) -> bool {
    !base_and_buffer
}

/// This export's effective warehouse partition. The EXTRACT asks, because nothing splits
/// one Parquet file at load time: only the writer can keep a part inside a load job's
/// partition budget. `table` names the captured table when the caller has one.
pub fn resolved_partition(
    config: &crate::config::Config,
    export: &crate::config::ExportConfig,
    table: Option<&str>,
) -> Option<PartitionSpec> {
    effective_load(config, export, table).and_then(|eff| eff.partition)
}

/// Where THIS export's current state will live — the section's `layout:` with the export's
/// block, and a captured table's block, layered over it. The EXTRACT asks too: a
/// base-and-buffer table's rows carry the delete flag as data, and only the writer can put
/// it in the file.
pub fn resolved_layout(
    config: &crate::config::Config,
    export: &crate::config::ExportConfig,
    table: Option<&str>,
) -> CdcLayout {
    let eff = effective_load(config, export, table);
    let choice = eff.as_ref().and_then(|eff| eff.layout);
    let compacts = eff.as_ref().is_some_and(warehouse_compacts);
    cdc_layout(export, load_mode_of(export.mode), choice, compacts)
}

/// Whether the warehouse can merge a buffer into a base: `rivet compact` is
/// BigQuery-only, so only there does a base-and-buffer table ever complete a cycle.
pub(crate) fn warehouse_compacts(section: &LoadSection) -> bool {
    matches!(section.target, LoadTarget::Bigquery { .. })
}

fn cdc_layout(
    export: &crate::config::ExportConfig,
    mode: LoadMode,
    choice: Option<crate::config::load::LayoutChoice>,
    warehouse_compacts: bool,
) -> CdcLayout {
    use crate::config::load::LayoutChoice;
    match (mode, choice) {
        // A `full` load overwrites the whole table on every pass: there is no
        // accumulated current state to lay out, so the key means nothing here.
        (LoadMode::Full, _) => CdcLayout::LogAndView,
        // A base without a compaction is a snapshot frozen at the backfill and a buffer
        // that grows for ever (measured on Snowflake): only a compacting warehouse gets
        // one — written, or derived from a stream's `backfill:`.
        (_, Some(LayoutChoice::BaseBuffer)) if warehouse_compacts => CdcLayout::BaseAndBuffer,
        (LoadMode::Cdc, None) if warehouse_compacts => {
            match export.cdc.as_ref().and_then(|c| c.backfill.as_ref()) {
                Some(_) => CdcLayout::BaseAndBuffer,
                None => CdcLayout::LogAndView,
            }
        }
        // A written `log_view`, an unwritten incremental, or a warehouse that cannot
        // compact: the changelog and its view.
        _ => CdcLayout::LogAndView,
    }
}

/// Whether a plan's `pk` / `cluster_by` / `partition` must name columns of the
/// spec it is built from now, or may be checked later against the pinned run's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecFit {
    Strict,
    Deferred,
}

const NOT_A_COLUMN: &str = "is not a column of the export";

/// The strict fit check a deferred plan still owes: every key, clustering and
/// partition column is a column of the spec it is typed from.
pub fn check_spec_fit(plan: &LoadPlan) -> Result<()> {
    // A delta mode writes a changelog or a base+buffer table, and BOTH carry rivet's
    // reserved `__` columns. A SOURCE column of the same name is silently destructive
    // there and in two different ways: `__op`/`__pos`/`__seq` are filtered out of a
    // MERGE's carried set by `is_meta_column`, so their values vanish; `__is_deleted`
    // is worse, because `merge_inputs` reads that name being PRESENT as "this table
    // has soft-delete semantics" and turns the tombstone arm on for a column the
    // source owns. `DELETE_FLAG_COLUMN`'s own doc claimed the `__` namespace meant a
    // collision "can never" happen — this is what makes that true.
    //
    // `full` is exempt: it overwrites with source columns only and never builds the
    // vocabulary, so refusing there would break configs that work.
    if !matches!(plan.mode, LoadMode::Full)
        && let Some(s) = plan
            .specs
            .iter()
            .find(|s| crate::load::cdc::is_reserved_column(&s.column_name))
    {
        bail!(
            "export `{}`: the source has a column named `{}`, which is a name rivet OWNS in \
             a `{}` load's changelog and base tables. Its values would not survive the merge \
             (and `__is_deleted` would switch on soft-delete semantics for rows the source \
             controls). Rename or exclude the column in the export's query, or load this \
             table with `mode: full`, where rivet builds no such columns.",
            plan.export_name,
            s.column_name,
            plan.mode.ledger_str(),
        );
    }
    let has = |c: &str| plan.specs.iter().any(|s| s.column_name == c);
    if let Some(m) = plan.pk.iter().find(|c| !has(c)) {
        bail!(
            "export `{}`: primary-key column `{m}` {NOT_A_COLUMN} — the dedup view partitions \
             by it. fix `pk` in the export's `load:` block",
            plan.export_name
        );
    }
    if let Some(m) = plan.clustering.columns().iter().find(|c| !has(c)) {
        bail!(
            "export `{}`: `cluster_by` column `{m}` {NOT_A_COLUMN}",
            plan.export_name
        );
    }
    resolve_partition(
        &plan.export_name,
        &plan.load,
        plan.mode,
        &plan.specs,
        SpecFit::Strict,
    )?;
    Ok(())
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
fn build_plans_keyed(
    cfg: &crate::config::Config,
    load: &LoadSection,
    reports: Vec<crate::preflight::type_report::ExportTypeReport>,
    keys: &RecordedKeys,
    fit: SpecFit,
) -> Result<Vec<LoadPlan>> {
    let mut plans = Vec::with_capacity(reports.len());
    for report in reports {
        let unit = report.table.clone();
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
                "export `{}` has `load:` but its destination is `type: {}` — the load \
                 layer reads GCS only (Snowflake via storage integration, BigQuery via \
                 LOAD DATA). Stage the export to a gcs destination, or drop the load \
                 block.",
                export.name,
                dest.destination_type.label()
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

        let source_table = unit.as_deref().or(export.table.as_deref());
        let (renames, rename_warnings) =
            fold_lookalike_columns(&export.name, load, &mut specs, |file, latin| {
                source_rename_action(cfg.source.source_type, source_table, file, latin)
                    + &config_keys_note(export, file)
            })?;

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
        let mode = load_mode_of(export.mode);
        if matches!(export.mode, crate::config::ExportMode::TimeWindow) {
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
        }
        // The shared `load:`, this export's block, and — for a captured table of a
        // multiplex stream — that table's block. One overlay, shared with the readers the
        // extract calls, so both sides answer the same question the same way.
        let eff_load = overlay(load, export, unit.as_deref());
        let recorded_pk: Option<Vec<String>> = keys
            .get(&(export.name.clone(), unit))
            .map(|k| k.iter().map(|c| folded(c)).collect());
        let (pk, mut cluster_by) =
            resolve_keys(&export.name, &eff_load, recorded_pk.as_deref(), &specs, fit)?;
        let mut rename_warnings = rename_warnings;
        if matches!(eff_load.cluster_by, KeyColumns::Auto) {
            for col in unclustered_renames(&mut cluster_by, &renames) {
                rename_warnings.push(format!(
                    "  note: export `{}`: `{col}` is left out of the automatic clustering — \
                     a renamed column cannot shape the staging table the rename goes through",
                    export.name
                ));
            }
        }
        let partition = resolve_partition(&export.name, &eff_load, mode, &specs, fit)?;
        refuse_renamed_shape_column(&export.name, &renames, partition.as_ref(), &cluster_by)?;
        let clustering = match eff_load.cluster_by {
            KeyColumns::Auto => Clustering::Auto(cluster_by),
            _ => Clustering::Written(cluster_by),
        };
        plans.push(LoadPlan {
            export_name: export.name.clone(),
            unit: report.table.clone(),
            table,
            partition,
            specs,
            gcs_prefix,
            destination: export.destination.clone(),
            layout: cdc_layout(export, mode, eff_load.layout, warehouse_compacts(&eff_load)),
            // A CDC stream can express a delete, a query cannot — so the flag is a
            // column a batch base does not pay for unless its operator asks.
            deleted_flag: eff_load
                .deleted_flag
                .unwrap_or(matches!(mode, LoadMode::Cdc)),
            load: eff_load,
            mode,
            cursor_column: export.cursor_column.as_deref().map(folded),
            pk,
            clustering,
            pinned_run: None,
            renames,
            rename_warnings,
        });
    }
    reject_duplicate_target_tables(
        &plans
            .iter()
            .map(|p| (p.table.as_str(), p.mode))
            .collect::<Vec<_>>(),
    )?;
    Ok(plans)
}

/// A column's (Parquet name, warehouse name).
pub type Rename = (String, String);

/// The Parquet name of warehouse column `column`.
pub(crate) fn file_name<'a>(renames: &'a [Rename], column: &'a str) -> &'a str {
    renames
        .iter()
        .find(|(_, latin)| latin == column)
        .map_or(column, |(file, _)| file.as_str())
}

/// `name` with its Cyrillic look-alikes made Latin, or unchanged when no fold applies.
fn folded(name: &str) -> String {
    super::latin_fold(name).unwrap_or_else(|| name.to_string())
}

/// Columns an older run spelled differently from the pinned run but that load as one warehouse column, as (older, pinned).
pub fn lookalike_spelling_changes(pinned: &[&str], older: &[&str]) -> Vec<(String, String)> {
    older
        .iter()
        .filter(|o| !pinned.contains(o))
        .filter_map(|o| {
            let f = folded(o);
            pinned
                .iter()
                .find(|p| folded(p) == f)
                .map(|p| (o.to_string(), p.to_string()))
        })
        .collect()
}

/// Renames each column whose Cyrillic look-alikes fold to a plain identifier, warning once per column.
fn fold_lookalike_columns(
    export: &str,
    load: &LoadSection,
    specs: &mut [TargetColumnSpec],
    action: impl Fn(&str, &str) -> String,
) -> Result<(Vec<Rename>, Vec<String>)> {
    let mut renames = Vec::new();
    for spec in specs.iter_mut() {
        if let Some(latin) = super::latin_fold(&spec.column_name) {
            renames.push((
                std::mem::replace(&mut spec.column_name, latin.clone()),
                latin,
            ));
        }
    }
    if renames.is_empty() {
        return Ok((renames, Vec::new()));
    }
    if !matches!(load.target, LoadTarget::Bigquery { .. }) {
        bail!(
            "export `{export}`: column(s) {} have Cyrillic look-alike letters; only a BigQuery \
             load renames them — rename them in the source",
            renames
                .iter()
                .map(|(f, _)| format!("`{f}`"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    for (file, latin) in &renames {
        let clash = specs
            .iter()
            .filter(|s| s.column_name.eq_ignore_ascii_case(latin))
            .count();
        if clash > 1 {
            bail!(
                "export `{export}`: column `{file}` has Cyrillic look-alike letters and would load \
                 as `{latin}`, which another column already is — rename one of them in the source"
            );
        }
    }
    let warnings = renames
        .iter()
        .map(|(file, latin)| {
            format!(
                "  warning: export `{export}`: column `{file}` has Cyrillic look-alike letters — it \
                 loads as `{latin}`. Fix it at the source once every run already exported is loaded: {}",
                action(file, latin)
            )
        })
        .collect();
    Ok((renames, warnings))
}

/// The export's own config keys that name `file` and must be renamed with it; empty when none do.
fn config_keys_note(export: &crate::config::ExportConfig, file: &str) -> String {
    let mut keys: Vec<&str> = [
        ("cursor_column", export.cursor_column.as_deref()),
        ("chunk_column", export.chunk_column.as_deref()),
        ("chunk_by_key", export.chunk_by_key.as_deref()),
    ]
    .into_iter()
    .filter(|(_, v)| *v == Some(file))
    .map(|(k, _)| k)
    .collect();
    if export.columns.contains_key(file) {
        keys.push("columns");
    }
    if keys.is_empty() {
        return String::new();
    }
    format!(
        " — and rename it in the export's {} in the same edit, or the next `rivet run` fails",
        keys.iter()
            .map(|k| format!("`{k}:`"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// The statement that renames `file` to `latin` on the source, or the alias for a query export.
fn source_rename_action(
    source: crate::config::SourceType,
    table: Option<&str>,
    file: &str,
    latin: &str,
) -> String {
    use crate::config::SourceType;
    let quote = |id: &str| match source {
        SourceType::Mysql => format!("`{id}`"),
        SourceType::Mssql => format!("[{id}]"),
        SourceType::Postgres | SourceType::Mongo => format!("\"{id}\""),
    };
    let Some(table) = table else {
        return format!("alias it in the export's query: {} AS {latin}", quote(file));
    };
    let qualified = table.split('.').map(quote).collect::<Vec<_>>().join(".");
    match source {
        SourceType::Postgres => format!(
            "ALTER TABLE {qualified} RENAME COLUMN {} TO {};",
            quote(file),
            quote(latin)
        ),
        SourceType::Mysql => format!(
            "ALTER TABLE {qualified} RENAME COLUMN {f} TO {l}; (MySQL 8.0.3+; on 5.7: ALTER \
             TABLE {qualified} CHANGE {f} {l} <its full column definition>;)",
            f = quote(file),
            l = quote(latin)
        ),
        SourceType::Mssql => format!(
            "EXEC sp_rename N'{table}.{file}', N'{latin}', N'COLUMN'; (on a table enabled for \
             change data capture, disable its capture instance first and re-enable it after)"
        ),
        SourceType::Mongo => {
            format!("db.{table}.updateMany({{}}, {{$rename: {{\"{file}\": \"{latin}\"}}}})")
        }
    }
}

/// Drops renamed columns from an automatic clustering, returning the ones dropped.
fn unclustered_renames(cluster_by: &mut Vec<String>, renames: &[Rename]) -> Vec<String> {
    let (dropped, kept) = std::mem::take(cluster_by)
        .into_iter()
        .partition(|c| renames.iter().any(|(_, latin)| latin == c));
    *cluster_by = kept;
    dropped
}

/// Refuses a partition or cluster column that is itself renamed: the staging table the rename needs cannot be shaped on it.
fn refuse_renamed_shape_column(
    export: &str,
    renames: &[(String, String)],
    partition: Option<&TablePartition>,
    cluster_by: &[String],
) -> Result<()> {
    let shaped = partition
        .and_then(|p| p.key.column())
        .into_iter()
        .chain(cluster_by.iter().map(String::as_str));
    for col in shaped {
        if let Some((file, _)) = renames.iter().find(|(_, latin)| latin == col) {
            bail!(
                "export `{export}`: `{col}` partitions or clusters the table but its source \
                 column `{file}` has Cyrillic look-alike letters — rename it in the source, or, when it only clusters, set `cluster_by:` in the export's `load:` block to other columns or `none`"
            );
        }
    }
    Ok(())
}

/// `plan` rebuilt from `spec` — the columns and key ONE run recorded — instead of
/// the by-name spec [`plan_loads`] typed it from.
///
/// The by-name row (`export_load_spec`) is last-writer-wins: on a state DB shared
/// by two configs whose exports share a NAME, the other config's run can retype
/// this table between the run and its load — a `_id` key on a PostgreSQL table,
/// another engine's column types in the DDL. The load therefore pins each plan to
/// the spec of the run it is about to consume, which only that run wrote.
pub fn retype_plan(
    cfg: &crate::config::Config,
    plan: &LoadPlan,
    spec: &crate::state::LoadSpec,
    target: crate::types::target::ExportTarget,
) -> Result<LoadPlan> {
    let export = cfg
        .exports
        .iter()
        .find(|e| e.name == plan.export_name)
        .with_context(|| format!("export `{}` not found in config", plan.export_name))?;
    let load = cfg
        .load
        .clone()
        .context("config has no top-level `load:` block")?;
    let mappings = spec.columns.iter().map(|c| c.to_mapping()).collect();
    let report = crate::preflight::type_report::report_from_mappings(
        export,
        plan.unit.clone(),
        mappings,
        &crate::types::policy::TypePolicy::warn_only(),
        Some(target),
    );
    let mut keys = RecordedKeys::new();
    if let Some(pk) = &spec.primary_key {
        keys.insert((export.name.clone(), plan.unit.clone()), pk.clone());
    }
    build_plans_keyed(cfg, &load, vec![report], &keys, SpecFit::Strict)?
        .pop()
        .context("one report yields one plan")
}

/// [`build_plans_keyed`] with no recorded keys.
#[cfg(test)]
fn build_plans(
    cfg: &crate::config::Config,
    load: &LoadSection,
    reports: Vec<crate::preflight::type_report::ExportTypeReport>,
) -> Result<Vec<LoadPlan>> {
    build_plans_keyed(cfg, load, reports, &RecordedKeys::new(), SpecFit::Strict)
}

/// Resolve `pk` and `cluster_by` for one export from its `load:` block, the key
/// `rivet run` recorded, and the warehouse column types.
fn resolve_keys(
    export: &str,
    load: &LoadSection,
    recorded: Option<&[String]>,
    specs: &[TargetColumnSpec],
    fit: SpecFit,
) -> Result<(Vec<String>, Vec<String>)> {
    let pk = match &load.pk {
        KeyColumns::Columns(cols) => cols.iter().map(|c| folded(c)).collect(),
        KeyColumns::Auto => recorded.map(<[String]>::to_vec).unwrap_or_default(),
        KeyColumns::None => Vec::new(),
    };
    let bigquery = matches!(load.target, LoadTarget::Bigquery { .. });
    let max = crate::load::bigquery::MAX_CLUSTER_COLUMNS;
    let type_of = |c: &str| {
        specs
            .iter()
            .find(|s| s.column_name == c)
            .map(|s| s.target_type.as_str())
    };
    if fit == SpecFit::Strict
        && let Some(missing) = pk.iter().find(|c| type_of(c).is_none())
    {
        let fix = match &load.pk {
            KeyColumns::Auto => {
                "`rivet run` recorded it as the source table's key; name the export's own key \
                 columns under `pk:` in its `load:` block"
            }
            _ => "fix `pk` in the export's `load:` block",
        };
        bail!(
            "export `{export}`: primary-key column `{missing}` {NOT_A_COLUMN} — \
             the dedup view partitions by it. {fix}"
        );
    }
    let cluster_by = match &load.cluster_by {
        KeyColumns::None => Vec::new(),
        KeyColumns::Columns(cols) => {
            if bigquery && cols.len() > max {
                bail!(
                    "export `{export}`: `cluster_by` names {} columns; BigQuery clusters on at most {max}",
                    cols.len()
                );
            }
            for c in cols {
                match type_of(c) {
                    None if fit == SpecFit::Deferred => {}
                    None => bail!("export `{export}`: `cluster_by` column `{c}` {NOT_A_COLUMN}"),
                    Some(t) if bigquery && !super::bigquery::clusterable(t) => bail!(
                        "export `{export}`: BigQuery cannot cluster on `{c}` ({t}); clusterable \
                         types are INT64, NUMERIC, BIGNUMERIC, STRING, BOOL, DATE, DATETIME, \
                         TIMESTAMP, GEOGRAPHY and RANGE"
                    ),
                    Some(_) => {}
                }
            }
            cols.clone()
        }
        KeyColumns::Auto => {
            let mut cols = Vec::new();
            for c in &pk {
                match type_of(c) {
                    Some(t) if bigquery && !super::bigquery::clusterable(t) => eprintln!(
                        "  warning: export `{export}`: not clustering on key column `{c}` — \
                         BigQuery cannot cluster a {t} column"
                    ),
                    Some(_) => cols.push(c.clone()),
                    None => {}
                }
            }
            if bigquery && cols.len() > max {
                eprintln!(
                    "  note: export `{export}`: clustering on the first {max} of {} key columns",
                    cols.len()
                );
                cols.truncate(max);
            }
            cols
        }
    };
    Ok((pk, cluster_by))
}

/// Resolve `partition` for one export against the warehouse column types (ADR-0034 D3).
fn resolve_partition(
    export: &str,
    load: &LoadSection,
    mode: LoadMode,
    specs: &[TargetColumnSpec],
    fit: SpecFit,
) -> Result<Option<TablePartition>> {
    let Some(spec) = &load.partition else {
        return Ok(None);
    };
    // Deferred: a partition column the BY-NAME spec lacks is not yet a refusal —
    // the pinned run's spec decides, or `check_spec_fit` refuses.
    if fit == SpecFit::Deferred
        && let Some(col) = spec.form.column()
        && !specs.iter().any(|s| s.column_name == col)
    {
        return Ok(None);
    }
    let column_type = |c: &str| -> Result<String> {
        if !super::is_safe_load_ident(c) {
            bail!(
                "export `{export}`: partition column `{}` is not a plain SQL identifier \
                 ([A-Za-z_][A-Za-z0-9_]*)",
                c.escape_default()
            );
        }
        specs
            .iter()
            .find(|s| s.column_name == c)
            .map(|s| base_type(&s.target_type))
            .with_context(|| format!("export `{export}`: partition column `{c}` {NOT_A_COLUMN}"))
    };
    let (key, expr) = match &load.target {
        LoadTarget::Bigquery { .. } => {
            super::bigquery::partition_expr(export, &spec.form, &column_type)?
        }
        LoadTarget::Snowflake { .. } => {
            super::snowflake::partition_expr(export, spec, &column_type)?
        }
    };
    if hourly_partitions_outlive_the_table(&key, spec.expiration_days) {
        eprintln!(
            "  warning: export `{export}`: hourly partitions reach BigQuery's \
             {MAX_TABLE_PARTITIONS}-partition limit after {HOURLY_LIFETIME_DAYS} days — set \
             `expiration_days` to at most {HOURLY_LIFETIME_DAYS}, or use `granularity: day`"
        );
    }
    if mode != LoadMode::Full {
        if spec.require_filter {
            eprintln!(
                "  note: export `{export}`: `require_filter` shapes a full-load table; the change \
                 log `__changes` cannot require a partition filter, since the current-state view \
                 reads all of it"
            );
        }
        if key.is_load_date() && spec.expiration_days.is_some() {
            eprintln!(
                "  note: export `{export}`: load-date partitions of the change log `__changes` \
                 (the load time, or `_rivet_exported_at`) do not expire — expiring them would \
                 drop rows that never changed from the current-state view"
            );
        }
    }
    Ok(Some(TablePartition {
        key,
        expr,
        expiration_days: spec.expiration_days,
        require_filter: spec.require_filter,
    }))
}

/// Whether an hourly key with this expiry can outgrow BigQuery's per-table partition cap.
fn hourly_partitions_outlive_the_table(key: &PartitionKey, expiration_days: Option<u32>) -> bool {
    matches!(
        key,
        PartitionKey::Time {
            granularity: Granularity::Hour,
            ..
        }
    ) && expiration_days.is_none_or(|d| d > HOURLY_LIFETIME_DAYS)
}

/// `NUMERIC(12, 2)` → `NUMERIC`, `ARRAY<INT64>` → `ARRAY`.
pub(crate) fn base_type(target_type: &str) -> String {
    target_type
        .split(['(', '<'])
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
}

/// Reject two exports that touch the SAME warehouse object. The `target:` is
/// shared, so two exports whose `table:` (or `name:`) resolves alike land on one
/// object — a full OVERWRITE would clobber what a cdc/incremental export appends
/// a `<table>__changes` view over, and they'd share one ledger skip-set.
///
/// An append mode occupies TWO objects: `<table>` (the view) and
/// `<table>__changes` (the log). Comparing plan tables alone missed a full
/// export of a source table literally named `orders__changes` next to a CDC
/// export of `orders` — different strings, one warehouse object, and the full
/// load's OVERWRITE landed on the live change log. Pure + unit-testable.
fn reject_duplicate_target_tables(plans: &[(&str, LoadMode)]) -> Result<()> {
    let mut seen: std::collections::HashMap<String, &str> = std::collections::HashMap::new();
    for (t, mode) in plans {
        // Every warehouse name a load can CREATE belongs here, not just the target.
        // `__staging` is the third: a whole-table pass whose Parquet exceeds one
        // job's partition budget lands through `<t>__staging` — `DROP TABLE IF
        // EXISTS`, refill, CLONE onto the target, `DROP TABLE` — and none of it is
        // behind `ensure_own`, which gates the TARGET fqtn only. So a source table
        // literally named `orders__staging` beside `orders` was destroyed by
        // `orders`' own load, silently, both exports exiting 0. `__staging` is an
        // ordinary schema name (dbt's package prefixes produce it), so this is a
        // naming collision, not an exotic identifier.
        //
        // Reserved under EVERY mode, deliberately: `materialize` is reached from
        // `load_one` (full), `load_one_incremental` (the first pass) AND
        // `load_one_cdc_base` (the baseline leg), so the staging name is not
        // full-only. A config holding both names is already broken today — the
        // destruction happens whenever the sibling's load splits into batches — so
        // refusing it loudly costs nothing that was working.
        let mut objects = vec![t.to_string(), format!("{t}__staging")];
        if !matches!(mode, LoadMode::Full) {
            objects.push(format!("{t}__changes"));
            objects.push(format!("{t}__changes__staging"));
            objects.push(format!("{t}__changes__merging"));
        }
        for o in objects {
            if let Some(prior) = seen.insert(o.clone(), t) {
                bail!(
                    "two exports resolve to the same warehouse object `{o}` (load targets `{prior}` \
                     and `{t}`) — each would clobber the other (a full OVERWRITE vs a \
                     cdc/incremental append share the object and its ledger). Give each export \
                     its own `table:` or destination."
                );
            }
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

    /// The delete flag is DATA on a base table, so only a layout that HAS a physical base
    /// can carry it. Under log-and-view the name is a view, which holds no column of its
    /// own — declaring the flag there must not put one in the file.
    ///
    /// Extracted from two live-only bodies (`load_one_incremental`, the CDC job's snapshot
    /// synthesis) where the same `&&` sat inline and the mutation corpus excluded it.
    #[test]
    fn a_base_carries_the_delete_flag_only_when_there_is_a_base_and_it_was_declared() {
        assert!(
            base_carries_delete_flag(true, true),
            "base-and-buffer with the flag declared: the column must exist from the first \
             pass, or the buffer's tombstones have nothing to flip"
        );
        assert!(
            !base_carries_delete_flag(true, false),
            "a base whose export did not declare the flag pays no column for it"
        );
        assert!(
            !base_carries_delete_flag(false, true),
            "log-and-view has no physical base — the flag lives in the changelog, and a \
             view cannot hold a column of its own"
        );
        assert!(!base_carries_delete_flag(false, false));
    }

    /// Folding a whole-table pass into the changelog is the log-and-view answer, where the
    /// target name is a view and cannot be overwritten. Under base-and-buffer that same
    /// pass IS the base and must land as a table, so it may never join the log.
    ///
    /// Only the layout is graded here. Whether a first pass EXISTS is carried by the
    /// `Option` the caller filters, so it is the caller's `if let` — not this predicate —
    /// and duplicating it as a parameter would put one decision in two places.
    #[test]
    fn a_whole_table_pass_joins_the_log_only_under_the_view_layout() {
        assert!(
            whole_table_pass_may_join_the_log(false),
            "log-and-view: a whole-table pass folds into the changelog"
        );
        assert!(
            !whole_table_pass_may_join_the_log(true),
            "base-and-buffer: the first pass IS the base and must land as a table"
        );
    }

    #[test]
    fn ledger_str_names_each_mode_stably() {
        // The state DB's `load_run.mode` discriminator — every mode must map to
        // its exact stable string. NOT because anything branches on it: the column
        // is WRITE-ONLY today (one production caller, `orchestrate.rs`'s record
        // builder, and no query in `load_journal_store` filters or orders by it).
        // The skip set is keyed by `loaded_source_run`, not by mode. What a drifted
        // value breaks is the audit trail an operator reads back — `rivet state
        // loads` and anything downstream of it — which is why the string is pinned.
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

    /// A TIMESTAMP column — the shape a `partition.column` needs.
    fn ts_col(name: &str) -> TypeReportRow {
        row_from_spec(&TargetColumnSpec {
            column_name: name.into(),
            target_type: "TIMESTAMP".into(),
            autoload_type: "TIMESTAMP".into(),
            status: TargetStatus::Ok,
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
        let load = cfg.load.clone().unwrap();
        let reports = vec![report("alpha", vec![col("id", TargetStatus::Ok)])];
        let err = build_plans(&cfg, &load, reports)
            .expect_err("s3 + load: must refuse at plan time")
            .to_string();
        assert!(
            err.contains("type: s3") && err.contains("load"),
            "must name the mismatch and the block: {err}"
        );
    }

    /// The by-name spec may belong to ANOTHER config's same-named export (a shared
    /// state DB, last writer wins): `pk: [id]` against a spec holding `_id` is not
    /// a refusal at plan time — the pin retypes from the run's own spec — but the
    /// strict check still refuses the plan if no pin happens. Measured live: four
    /// `users` exports on one Postgres state, MySQL's load refused on Mongo's `_id`.
    #[test]
    fn a_by_name_plan_defers_its_key_fit_to_the_pin_and_still_owes_it() {
        let cfg = crate::config::Config::from_yaml(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: users
    table: users
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: { type: gcs, bucket: b, prefix: pa/ }
    load: { pk: [id], cluster_by: [id], partition: { column: created_at, granularity: day } }
load:
  target: bigquery
  project: p
  dataset: d
"#,
        )
        .unwrap();
        let load = cfg.load.clone().unwrap();
        // The by-name row another config wrote: `_id` and `v`, none of ours.
        let foreign = || {
            vec![report(
                "users",
                vec![col("_id", TargetStatus::Ok), col("v", TargetStatus::Ok)],
            )]
        };
        let strict = build_plans_keyed(
            &cfg,
            &load,
            foreign(),
            &RecordedKeys::new(),
            SpecFit::Strict,
        )
        .unwrap_err()
        .to_string();
        assert!(strict.contains("primary-key column `id`"), "{strict}");
        let deferred = build_plans_keyed(
            &cfg,
            &load,
            foreign(),
            &RecordedKeys::new(),
            SpecFit::Deferred,
        )
        .expect("deferred: the pin decides")
        .pop()
        .unwrap();
        assert_eq!(deferred.pk, vec!["id".to_string()]);
        assert_eq!(
            deferred.partition, None,
            "a partition column the spec lacks waits for the pin"
        );
        let owed = check_spec_fit(&deferred).unwrap_err().to_string();
        assert!(owed.contains("primary-key column `id`"), "{owed}");
        // The run's own spec fits: the strict rebuild (what `retype_plan` does) passes.
        let own = vec![report(
            "users",
            vec![col("id", TargetStatus::Ok), ts_col("created_at")],
        )];
        let fitted = build_plans_keyed(&cfg, &load, own, &RecordedKeys::new(), SpecFit::Strict)
            .expect("the run's own columns fit")
            .pop()
            .unwrap();
        check_spec_fit(&fitted).expect("nothing owed");

        // A SOURCE column in rivet's reserved `__` vocabulary is refused on a delta
        // mode — see `a_source_column_in_rivets_reserved_namespace_is_refused` for why
        // silence here is destructive. Reuses this plan rather than rebuilding one.
        for name in ["__op", "__pos", "__seq", "__is_deleted"] {
            let mut clashing = fitted.clone();
            clashing.mode = LoadMode::Cdc;
            clashing.specs.push(crate::load::cdc::flag_spec(
                crate::load::cdc::Warehouse::BigQuery,
            ));
            clashing.specs.last_mut().unwrap().column_name = name.to_string();
            let err = check_spec_fit(&clashing).unwrap_err().to_string();
            assert!(
                err.contains(name) && err.contains("rivet OWNS"),
                "`{name}` must be refused by name: {err}"
            );
            // `full` builds none of that vocabulary, so it must stay loadable.
            clashing.mode = LoadMode::Full;
            check_spec_fit(&clashing).unwrap_or_else(|e| {
                panic!("a full load overwrites with source columns only — `{name}` is just a column there: {e}")
            });
        }

        // NON-VACUITY: rivet's OWN enrich columns live in the `_rivet_` namespace, not
        // `__`, and must never trip this. Without this arm the guard could refuse every
        // delta plan rivet itself builds and the loop above would still pass.
        let mut enriched = fitted.clone();
        enriched.mode = LoadMode::Cdc;
        enriched.specs.push(crate::load::cdc::flag_spec(
            crate::load::cdc::Warehouse::BigQuery,
        ));
        enriched.specs.last_mut().unwrap().column_name = crate::enrich::COL_ROW_HASH.to_string();
        check_spec_fit(&enriched)
            .expect("`_rivet_row_hash` is rivet's own enrich column, a different namespace");
    }

    /// The reserved `__` vocabulary is one predicate, and it holds all four names.
    ///
    /// It used to live in two places — `is_meta_column`'s three and
    /// `DELETE_FLAG_COLUMN` — and neither was ever compared against the SOURCE's
    /// columns, while `DELETE_FLAG_COLUMN`'s own doc claimed a collision "can never"
    /// happen. A comment beside a thing is not a guard.
    ///
    /// The two harms differ, which is why all four are refused rather than just the
    /// flag: `__op`/`__pos`/`__seq` are filtered out of a MERGE's carried set by
    /// `is_meta_column`, so a source column of that name loses its VALUES silently;
    /// `__is_deleted` instead makes `merge_inputs` infer soft-delete SEMANTICS for a
    /// column the source controls.
    #[test]
    fn a_source_column_in_rivets_reserved_namespace_is_refused() {
        use crate::load::cdc::is_reserved_column;
        for owned in ["__op", "__pos", "__seq", "__is_deleted"] {
            assert!(is_reserved_column(owned), "{owned} is rivet's");
        }
        for theirs in [
            "id",
            "is_deleted",
            "op",
            "_op",
            "__opx",
            "_rivet_row_hash",
            "_rivet_exported_at",
        ] {
            assert!(
                !is_reserved_column(theirs),
                "{theirs} belongs to the source (or to the `_rivet_` enrich namespace) \
                 and must stay loadable"
            );
        }
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
        let load = cfg.load.clone().unwrap();

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

        // A plan retyped from ONE run's recorded spec carries that run's columns
        // and key — not the by-name spec it was planned from. This is the seam the
        // shared-state race crosses: another config's same-named export can retype
        // the by-name row between the run and its load; the per-run spec cannot.
        {
            use crate::state::{LoadSpec, LoadSpecColumn};
            use crate::types::{RivetType, TypeFidelity};
            let alpha = plans.iter().find(|p| p.table == "alpha_tbl").unwrap();
            assert_eq!(
                alpha.pk,
                Vec::<String>::new(),
                "planned with no recorded key"
            );
            let column = |name: &str| LoadSpecColumn {
                name: name.into(),
                source_type: "int8".into(),
                rivet_type: RivetType::Int64,
                fidelity: TypeFidelity::Exact,
                nullable: false,
                warnings: Vec::new(),
            };
            let spec = LoadSpec {
                export_name: "alpha".into(),
                unit: None,
                columns: vec![column("tenant"), column("id")],
                primary_key: Some(vec!["tenant".into(), "id".into()]),
                run_id: Some("alpha_run_7".into()),
                origin: "run".into(),
                captured_at: "2026-09-17T00:00:00Z".into(),
            };
            let target = crate::types::target::ExportTarget::parse("bigquery").unwrap();
            let retyped = retype_plan(&cfg, alpha, &spec, target).unwrap();
            assert_eq!(retyped.table, "alpha_tbl");
            assert_eq!(retyped.gcs_prefix, alpha.gcs_prefix);
            assert_eq!(
                retyped
                    .specs
                    .iter()
                    .map(|s| s.column_name.as_str())
                    .collect::<Vec<_>>(),
                vec!["tenant", "id"],
                "the run's columns, in the run's order"
            );
            assert_eq!(retyped.pk, vec!["tenant".to_string(), "id".to_string()]);
        }

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
        let load = without.load.clone().unwrap();
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
        let load = with.load.clone().unwrap();
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
        let load = both.load.clone().unwrap();
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
        let load = cfg.load.clone().unwrap();
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
            assert_eq!(p.pk, vec!["id"], "the shared `load:` applies to each");
        }
    }

    /// One `load:` on a six-table stream is one partition column, one key, for six
    /// tables that do not share a schema. The stream's `load:` therefore takes a
    /// `tables:` map — a per-TABLE override layered over the export's, layered
    /// over the top level — so `created_at` partitions the tables that have it
    /// and `none` clears it where they do not, without splitting the stream.
    #[test]
    fn a_multiplex_export_takes_per_table_load_overrides() {
        let cfg = crate::config::Config::from_yaml(
            r#"
source:
  type: mysql
  url: "mysql://localhost/test"
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
      prefix: cdc/
    load:
      partition: { column: created_at, granularity: day }   # the stream's default
      tables:
        customers: { partition: none }                       # has no created_at
        line_items: { pk: [id, line_no], cluster_by: none }  # a composite key
load:
  target: bigquery
  project: p
  dataset: d
  pk: [id]
"#,
        )
        .unwrap();
        let load = cfg.load.clone().unwrap();
        let reports = vec![
            table_report(
                "cdc",
                "orders",
                vec![col("id", TargetStatus::Ok), ts_col("created_at")],
            ),
            table_report("cdc", "customers", vec![col("id", TargetStatus::Ok)]),
            table_report(
                "cdc",
                "line_items",
                vec![
                    col("id", TargetStatus::Ok),
                    col("line_no", TargetStatus::Ok),
                    ts_col("created_at"),
                ],
            ),
        ];
        let plans = build_plans(&cfg, &load, reports).unwrap();
        let by_table = |t: &str| plans.iter().find(|p| p.table == t).expect(t);

        assert!(
            by_table("orders").partition.is_some(),
            "the stream's default applies"
        );
        assert_eq!(by_table("orders").pk, vec!["id"]);
        assert!(
            by_table("customers").partition.is_none(),
            "`partition: none` on the table clears the stream's default"
        );
        assert_eq!(
            by_table("customers").pk,
            vec!["id"],
            "the rest is inherited"
        );
        assert!(by_table("line_items").partition.is_some());
        assert_eq!(
            by_table("line_items").pk,
            vec!["id", "line_no"],
            "the table's own key over the top-level one"
        );
        assert!(
            matches!(&by_table("line_items").clustering, Clustering::Written(c) if c.is_empty()),
            "the table's `cluster_by: none` is a WRITTEN empty clustering: {:?}",
            by_table("line_items").clustering
        );
        assert!(
            matches!(&by_table("orders").clustering, Clustering::Auto(_)),
            "a table without its own block inherits the default `cluster_by: auto`"
        );
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
        let load = cfg.load.clone().unwrap();
        let reports = vec![report("ghost", vec![])];
        let err = build_plans(&cfg, &load, reports).unwrap_err().to_string();
        assert!(err.contains("ghost") && err.contains("not found"), "{err}");
    }

    #[test]
    fn reject_duplicate_target_tables_catches_a_collision() {
        // Two exports resolving to the same warehouse table would clobber each
        // other — caught at plan time, not silently at load time.
        use LoadMode::{Cdc, Full, Incremental};
        assert!(
            reject_duplicate_target_tables(&[("orders", Full), ("events", Full), ("orders", Full)])
                .is_err()
        );
        assert!(reject_duplicate_target_tables(&[("orders", Full), ("events", Full)]).is_ok());
        assert!(reject_duplicate_target_tables(&[]).is_ok());

        // An append mode also occupies `<table>__changes`: a full export of a
        // source table NAMED `orders__changes` would OVERWRITE the CDC export's
        // live change log — two different plan tables, one warehouse object.
        let err = reject_duplicate_target_tables(&[("orders", Cdc), ("orders__changes", Full)])
            .unwrap_err()
            .to_string();
        assert!(err.contains("orders__changes"), "{err}");
        assert!(
            reject_duplicate_target_tables(&[("orders__changes", Full), ("orders", Incremental)])
                .is_err(),
            "order-independent"
        );
        // Two append exports on different tables occupy four distinct objects.
        assert!(
            reject_duplicate_target_tables(&[("orders", Cdc), ("events", Incremental)]).is_ok()
        );
    }

    /// `<table>__staging` is a warehouse name a load CREATES, so it is reserved too.
    ///
    /// A whole-table pass whose Parquet exceeds one job's partition budget lands
    /// through `<t>__staging`: `DROP TABLE IF EXISTS`, refill, CLONE onto the target,
    /// `DROP TABLE`. None of that is behind `ensure_own` — that gates the TARGET fqtn
    /// — so a source table literally named `orders__staging` beside `orders` was
    /// destroyed by `orders`' own load, with both exports exiting 0. It is an ordinary
    /// schema name (dbt's package prefixes produce exactly this), not an exotic
    /// identifier.
    ///
    /// EVERY mode, deliberately. The hunt scoped this to `full`; reading the callers
    /// says otherwise — `materialize` is reached from `load_one` (full),
    /// `load_one_incremental` (the first pass) and `load_one_cdc_base` (the baseline
    /// leg) alike, so a full-only reservation would leave two of the three paths able
    /// to destroy the sibling.
    ///
    /// RED against dropping `format!("{t}__staging")` from the object list.
    #[test]
    fn reject_duplicate_target_tables_reserves_the_staging_name_in_every_mode() {
        use LoadMode::{Cdc, Full, Incremental};
        for mode in [Full, Incremental, Cdc] {
            let err =
                reject_duplicate_target_tables(&[("orders", mode), ("orders__staging", Full)])
                    .unwrap_err()
                    .to_string();
            assert!(
                err.contains("orders__staging"),
                "{mode:?}: the staging name a load creates must collide with an export \
                 that targets it: {err}"
            );
            // Order-independent, like its `__changes` sibling.
            assert!(
                reject_duplicate_target_tables(&[("orders__staging", Full), ("orders", mode)])
                    .is_err(),
                "{mode:?}: order-independent"
            );
        }
        // And it does not invent collisions between unrelated tables.
        assert!(
            reject_duplicate_target_tables(&[("orders", Full), ("events__staging", Full)]).is_ok()
        );
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
        let load = cfg.load.clone().unwrap();

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
        let load = cfg.load.clone().unwrap();
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

    /// The config-level `load:` gates run BEFORE the state DB is read.
    ///
    /// Nothing is recorded for the export, so if `plan_loads` reached the type
    /// lookup first the error would name the missing load spec; that it names
    /// the `load:` problem instead is the ordering proof.
    ///
    /// Honest about which half a mutant can move: the missing-block arm is
    /// ordered by construction (the target comes FROM the block, so nothing can
    /// resolve types before it parses), and only pins the message. The typo arm
    /// is the graded one — parsing the block permissively in `Config::from_yaml` takes
    /// the run past the gate and into the empty state DB.
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
        let err = format!("{:#}", plan_loads(&path).unwrap_err());
        assert!(err.contains("gc_orphan"), "{err}");
    }

    fn orders_on_a_closed_source() -> (tempfile::TempDir, String) {
        cfg_file(&format!(
            "source:\n  type: postgres\n  url: \"{CLOSED_SOURCE}\"\n\
             exports:\n  - name: orders\n    table: t\n    mode: full\n    format: parquet\n    \
             destination:\n      type: gcs\n      bucket: b\n      prefix: p/\nload:\n  \
             target: bigquery\n  project: p\n  dataset: d\n"
        ))
    }

    /// The load plans from the state DB a run recorded and never dials the source.
    #[test]
    fn plan_loads_types_columns_from_the_state_db_with_the_source_unreachable() {
        use crate::types::{RivetType, TypeFidelity};
        let (_dir, path) = orders_on_a_closed_source();
        let col = |name: &str, rivet_type| crate::state::LoadSpecColumn {
            name: name.into(),
            source_type: "native".into(),
            rivet_type,
            fidelity: TypeFidelity::Exact,
            nullable: false,
            warnings: Vec::new(),
        };
        crate::state::StateStore::open(&path)
            .unwrap()
            .record_load_spec(
                "orders",
                None,
                &[
                    col("id", RivetType::Int64),
                    col(
                        "amount",
                        RivetType::Decimal {
                            precision: 12,
                            scale: 2,
                        },
                    ),
                ],
                Some(&["id".to_string()]),
                "run_1",
            )
            .unwrap();

        let plans = plan_loads(&path).unwrap();
        assert_eq!(plans.len(), 1);
        let specs: Vec<(&str, &str)> = plans[0]
            .specs
            .iter()
            .map(|s| (s.column_name.as_str(), s.target_type.as_str()))
            .collect();
        assert_eq!(specs.len(), 2, "{specs:?}");
        assert_eq!(specs[0], ("id", "INT64"));
        assert_eq!(specs[1].0, "amount");
        assert!(specs[1].1.starts_with("NUMERIC"), "{specs:?}");
        assert_eq!(plans[0].pk, vec!["id"], "pk: auto is the recorded key");
        assert_eq!(
            plans[0].clustering,
            Clustering::Auto(vec!["id".into()]),
            "cluster_by: auto is the key, and stays auto"
        );
    }

    /// With nothing recorded the load refuses, naming the export and the command that records it.
    #[test]
    fn plan_loads_without_a_recorded_spec_names_the_run_that_records_it() {
        let (_dir, path) = orders_on_a_closed_source();
        let err = format!("{:#}", plan_loads(&path).unwrap_err());
        assert!(
            err.contains("orders") && err.contains("rivet run"),
            "the refusal must name the export and `rivet run`: {err}"
        );
        assert!(
            !err.contains("resolving column types"),
            "the source must not be dialled: {err}"
        );
    }

    fn typed(name: &str, ty: &str) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: name.into(),
            target_type: ty.into(),
            autoload_type: ty.into(),
            status: TargetStatus::Ok,
            note: None,
            cast_sql: None,
        }
    }

    fn lookalike_cfg(cluster: &str) -> crate::config::Config {
        crate::config::Config::from_yaml(&format!(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: purchases
    table: purchases
    mode: full
    format: parquet
    destination: {{ type: gcs, bucket: b, prefix: pa/ }}
load:
  target: bigquery
  project: p
  dataset: d
  cluster_by: {cluster}
"#
        ))
        .unwrap()
    }

    #[test]
    fn a_cyrillic_lookalike_column_and_key_plan_under_their_latin_names() {
        let reports = || {
            vec![report(
                "purchases",
                vec![
                    col("\u{456}d", TargetStatus::Ok),
                    col("\u{441}omment", TargetStatus::Ok),
                ],
            )]
        };
        let mut keys = RecordedKeys::new();
        keys.insert(("purchases".into(), None), vec!["\u{456}d".into()]);
        let auto = lookalike_cfg("auto");
        let auto_plan = build_plans_keyed(
            &auto,
            auto.load.as_ref().unwrap(),
            reports(),
            &keys,
            SpecFit::Strict,
        )
        .expect("an automatic clustering leaves the renamed key out instead of refusing")
        .pop()
        .unwrap();
        assert!(
            auto_plan.clustering.columns().is_empty(),
            "{:?}",
            auto_plan.clustering
        );
        assert!(
            auto_plan
                .rename_warnings
                .iter()
                .any(|w| w.contains("`id` is left out of the automatic clustering")),
            "{:?}",
            auto_plan.rename_warnings
        );
        let written = lookalike_cfg("[id]");
        let err = build_plans_keyed(
            &written,
            written.load.as_ref().unwrap(),
            reports(),
            &keys,
            SpecFit::Strict,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("`id` partitions or clusters"),
            "a WRITTEN clustering on the renamed key is still refused: {err}"
        );
        let cfg = lookalike_cfg("none");
        let plan = build_plans_keyed(
            &cfg,
            cfg.load.as_ref().unwrap(),
            reports(),
            &keys,
            SpecFit::Strict,
        )
        .unwrap()
        .pop()
        .unwrap();
        let names: Vec<&str> = plan.specs.iter().map(|s| s.column_name.as_str()).collect();
        assert!(names.starts_with(&["id", "comment"]), "{names:?}");
        assert!(
            plan.rename_warnings[1]
                .contains("loads as `comment`. Fix it at the source once every run already exported is loaded: ALTER TABLE"),
            "{:?}",
            plan.rename_warnings
        );
        assert_eq!(
            plan.file_column_names()[..2],
            ["\u{456}d".to_string(), "\u{441}omment".to_string()],
            "the drift check compares the Parquet's own names"
        );
        assert_eq!(
            plan.renames,
            vec![
                ("\u{456}d".to_string(), "id".to_string()),
                ("\u{441}omment".to_string(), "comment".to_string())
            ]
        );
        assert_eq!(
            plan.pk,
            vec!["id".to_string()],
            "the recorded key folds with its column"
        );
    }

    #[test]
    fn a_lookalike_fold_that_collides_or_targets_snowflake_is_refused() {
        let mut clash = vec![spec_named("comment"), spec_named("\u{441}omment")];
        let err = fold_lookalike_columns(
            "e",
            &load_with("bigquery", serde_json::json!({})),
            &mut clash,
            no_action,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("another column already is"), "{err}");

        let mut one = vec![spec_named("\u{441}omment")];
        let err = fold_lookalike_columns(
            "e",
            &load_with("snowflake", serde_json::json!({})),
            &mut one,
            no_action,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("only a BigQuery load renames them"), "{err}");

        let mut plain = vec![spec_named("comment")];
        assert!(
            fold_lookalike_columns(
                "e",
                &load_with("snowflake", serde_json::json!({})),
                &mut plain,
                no_action
            )
            .unwrap()
            .0
            .is_empty(),
            "a load with nothing to rename is untouched on every target"
        );
    }

    #[test]
    fn a_config_written_cursor_and_key_fold_with_their_columns() {
        let cfg = crate::config::Config::from_yaml(
            "source: { type: postgres, url: \"postgresql://localhost/test\" }\n\
             exports:\n\
             \x20 - name: purchases\n\
             \x20   query: \"SELECT 1\"\n\
             \x20   mode: incremental\n\
             \x20   cursor_column: \"upd\u{430}ted_at\"\n\
             \x20   format: parquet\n\
             \x20   destination: { type: gcs, bucket: b, prefix: pa/ }\n\
             \x20   load: { pk: [\"\u{456}d\"], cluster_by: none }\n\
             load: { target: bigquery, project: p, dataset: d }\n",
        )
        .unwrap();
        let reports = vec![report(
            "purchases",
            vec![
                col("\u{456}d", TargetStatus::Ok),
                ts_col("upd\u{430}ted_at"),
            ],
        )];
        let plan = build_plans(&cfg, cfg.load.as_ref().unwrap(), reports)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(plan.cursor_column.as_deref(), Some("updated_at"));
        assert_eq!(plan.pk, vec!["id".to_string()]);
        assert!(
            plan.rename_warnings.iter().any(|w| w.ends_with(
                "— and rename it in the export's `cursor_column:` in the same edit, or the next \
                 `rivet run` fails"
            )),
            "{:?}",
            plan.rename_warnings
        );
    }

    #[test]
    fn an_append_load_reserves_the_staging_and_merging_tables_it_creates() {
        for name in ["orders__changes__staging", "orders__changes__merging"] {
            let err = reject_duplicate_target_tables(&[
                ("orders", LoadMode::Incremental),
                (name, LoadMode::Full),
            ])
            .unwrap_err()
            .to_string();
            assert!(err.contains(name), "{err}");
        }
    }

    #[test]
    fn a_column_two_runs_spell_differently_is_named_in_either_direction() {
        let cyr = "\u{441}ity";
        assert_eq!(
            lookalike_spelling_changes(&["id", "city"], &["id", cyr]),
            vec![(cyr.to_string(), "city".to_string())],
            "renamed at the source after the older run"
        );
        assert_eq!(
            lookalike_spelling_changes(&["id", cyr], &["id", "city"]),
            vec![("city".to_string(), cyr.to_string())],
            "the other way round: the pinned run carries the look-alike"
        );
        assert!(lookalike_spelling_changes(&["id", "city"], &["id", "city"]).is_empty());
        assert!(
            lookalike_spelling_changes(&["id", "city", "added"], &["id", "dropped"]).is_empty(),
            "an added or dropped column is drift, not a respelling"
        );
    }

    fn no_action(_: &str, _: &str) -> String {
        String::new()
    }

    #[test]
    fn the_warning_names_the_rename_to_run_on_each_source() {
        use crate::config::SourceType;
        let act = |src, table| source_rename_action(src, table, "\u{441}omment", "comment");
        assert_eq!(
            act(SourceType::Mysql, Some("shop.purchases")),
            "ALTER TABLE `shop`.`purchases` RENAME COLUMN `\u{441}omment` TO `comment`; (MySQL \
             8.0.3+; on 5.7: ALTER TABLE `shop`.`purchases` CHANGE `\u{441}omment` `comment` \
             <its full column definition>;)"
        );
        assert_eq!(
            act(SourceType::Postgres, Some("purchases")),
            "ALTER TABLE \"purchases\" RENAME COLUMN \"\u{441}omment\" TO \"comment\";"
        );
        assert_eq!(
            act(SourceType::Mssql, Some("dbo.purchases")),
            "EXEC sp_rename N'dbo.purchases.\u{441}omment', N'comment', N'COLUMN'; (on a \
             table enabled for change data capture, disable its capture instance first and \
             re-enable it after)"
        );
        assert_eq!(
            act(SourceType::Mongo, Some("purchases")),
            "db.purchases.updateMany({}, {$rename: {\"\u{441}omment\": \"comment\"}})"
        );
        assert_eq!(
            act(SourceType::Mysql, None),
            "alias it in the export's query: `\u{441}omment` AS comment"
        );
    }

    #[test]
    fn a_renamed_column_may_not_partition_or_cluster_the_table() {
        let renames = vec![("\u{441}reated".to_string(), "created".to_string())];
        let err = refuse_renamed_shape_column("e", &renames, None, &cols(&["created"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("`created` partitions or clusters"), "{err}");
        assert!(refuse_renamed_shape_column("e", &renames, None, &cols(&["id"])).is_ok());
    }

    fn spec_named(name: &str) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: name.into(),
            target_type: "STRING".into(),
            autoload_type: "STRING".into(),
            status: TargetStatus::Ok,
            note: None,
            cast_sql: None,
        }
    }

    fn load_with(target: &str, extra: serde_json::Value) -> LoadSection {
        let mut v = match target {
            "snowflake" => serde_json::json!({
                "target": "snowflake", "connection": "c", "warehouse": "w",
                "database": "d", "schema": "s", "storage_integration": "i"
            }),
            _ => serde_json::json!({ "target": "bigquery", "project": "p", "dataset": "d" }),
        };
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(v).unwrap()
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn auto_keys_come_from_the_recorded_key_and_skip_unclusterable_columns() {
        let specs = [
            typed("tenant", "INT64"),
            typed("score", "FLOAT64"),
            typed("id", "STRING"),
        ];
        let recorded = cols(&["tenant", "score", "id"]);
        let (pk, cluster) = resolve_keys(
            "e",
            &load_with("bigquery", serde_json::json!({})),
            Some(&recorded),
            &specs,
            SpecFit::Strict,
        )
        .unwrap();
        assert_eq!(pk, recorded);
        assert_eq!(cluster, cols(&["tenant", "id"]));
    }

    #[test]
    fn auto_clustering_keeps_the_first_four_key_columns_only_on_bigquery() {
        let names = ["a", "b", "c", "d", "e"];
        let specs: Vec<_> = names.iter().map(|n| typed(n, "INT64")).collect();
        let resolve = |target| {
            resolve_keys(
                "e",
                &load_with(target, serde_json::json!({})),
                Some(&cols(&names)),
                &specs,
                SpecFit::Strict,
            )
            .unwrap()
            .1
        };
        assert_eq!(resolve("bigquery"), cols(&["a", "b", "c", "d"]));
        assert_eq!(resolve("snowflake"), cols(&names));
    }

    #[test]
    fn explicit_clustering_is_refused_when_bigquery_cannot_hold_it() {
        let specs = [typed("id", "INT64"), typed("score", "FLOAT64")];
        let err = |extra| {
            resolve_keys(
                "e",
                &load_with("bigquery", extra),
                None,
                &specs,
                SpecFit::Strict,
            )
            .unwrap_err()
            .to_string()
        };
        let e = err(serde_json::json!({ "cluster_by": ["score"] }));
        assert!(e.contains("cannot cluster on `score` (FLOAT64)"), "{e}");
        let e = err(serde_json::json!({ "cluster_by": ["nope"] }));
        assert!(e.contains("`nope` is not a column"), "{e}");
        let e = err(serde_json::json!({ "cluster_by": ["id", "id", "id", "id", "id"] }));
        assert!(e.contains("at most 4"), "{e}");
    }

    /// A key column the export does not project cannot partition the dedup view; the
    /// refusal comes at plan time, not after the append (which would then be retried).
    #[test]
    fn resolve_keys_refuses_a_pk_column_the_export_does_not_have() {
        let specs = [typed("id", "INT64"), typed("v", "STRING")];
        let e = resolve_keys(
            "e",
            &load_with("bigquery", serde_json::json!({ "pk": ["idd"] })),
            None,
            &specs,
            SpecFit::Strict,
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("primary-key column `idd` is not a column"),
            "{e}"
        );
        assert!(e.contains("fix `pk`"), "{e}");
        // The recorded source key of a relation the export no longer projects.
        let e = resolve_keys(
            "e",
            &load_with("bigquery", serde_json::json!({})),
            Some(&cols(&["id", "missing"])),
            &specs,
            SpecFit::Strict,
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("`missing` is not a column"), "{e}");
        assert!(e.contains("`rivet run` recorded it"), "{e}");
    }

    #[test]
    fn an_explicit_pk_wins_over_the_recorded_key_and_none_clusters_nothing() {
        let specs = [typed("id", "INT64"), typed("ext", "INT64")];
        let recorded = cols(&["id"]);
        let resolve = |extra| {
            resolve_keys(
                "e",
                &load_with("bigquery", extra),
                Some(&recorded),
                &specs,
                SpecFit::Strict,
            )
            .unwrap()
        };
        assert_eq!(
            resolve(serde_json::json!({ "pk": ["ext"] })),
            (cols(&["ext"]), cols(&["ext"]))
        );
        assert_eq!(
            resolve(serde_json::json!({ "pk": ["ext"], "cluster_by": "none" })),
            (cols(&["ext"]), vec![])
        );
    }

    #[test]
    fn auto_without_a_recorded_key_resolves_to_no_key() {
        let (pk, cluster) = resolve_keys(
            "e",
            &load_with("bigquery", serde_json::json!({})),
            None,
            &[typed("id", "INT64")],
            SpecFit::Strict,
        )
        .unwrap();
        assert!(pk.is_empty() && cluster.is_empty());
    }

    fn resolve_bq(
        block: serde_json::Value,
        specs: &[TargetColumnSpec],
    ) -> Result<Option<TablePartition>> {
        resolve_partition(
            "e",
            &load_with("bigquery", serde_json::json!({ "partition": block })),
            LoadMode::Full,
            specs,
            SpecFit::Strict,
        )
    }

    #[test]
    fn bigquery_partition_expressions_follow_the_column_type() {
        let specs = [
            typed("ts", "TIMESTAMP"),
            typed("dt", "DATETIME"),
            typed("d", "DATE"),
            typed("n", "INT64"),
            typed("v", "STRING"),
        ];
        let expr = |block: serde_json::Value| resolve_bq(block, &specs).unwrap().unwrap().expr;
        let col = |c: &str, g: &str| serde_json::json!({ "column": c, "granularity": g });
        assert_eq!(expr(col("ts", "hour")), "TIMESTAMP_TRUNC(ts, HOUR)");
        assert_eq!(expr(col("ts", "day")), "TIMESTAMP_TRUNC(ts, DAY)");
        assert_eq!(expr(col("ts", "month")), "TIMESTAMP_TRUNC(ts, MONTH)");
        assert_eq!(expr(col("ts", "year")), "TIMESTAMP_TRUNC(ts, YEAR)");
        assert_eq!(expr(col("dt", "hour")), "DATETIME_TRUNC(dt, HOUR)");
        assert_eq!(expr(col("dt", "day")), "DATETIME_TRUNC(dt, DAY)");
        assert_eq!(expr(col("dt", "month")), "DATETIME_TRUNC(dt, MONTH)");
        assert_eq!(expr(col("dt", "year")), "DATETIME_TRUNC(dt, YEAR)");
        assert_eq!(expr(col("d", "day")), "d");
        assert_eq!(expr(col("d", "month")), "DATE_TRUNC(d, MONTH)");
        assert_eq!(expr(col("d", "year")), "DATE_TRUNC(d, YEAR)");
        assert_eq!(
            expr(
                serde_json::json!({ "range": { "column": "n", "start": 0, "end": 100, "interval": 5 } })
            ),
            "RANGE_BUCKET(n, GENERATE_ARRAY(0, 100, 5))"
        );
        assert_eq!(
            expr(serde_json::json!({ "ingestion": "hour" })),
            "TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)"
        );
        assert_eq!(
            expr(serde_json::json!({ "ingestion": "day" })),
            "_PARTITIONDATE"
        );
        assert_eq!(
            expr(serde_json::json!({ "ingestion": "month" })),
            "TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH)"
        );
        assert_eq!(
            expr(serde_json::json!({ "ingestion": "year" })),
            "TIMESTAMP_TRUNC(_PARTITIONTIME, YEAR)"
        );

        let key = resolve_bq(col("ts", "day"), &specs).unwrap().unwrap().key;
        assert_eq!(
            key,
            PartitionKey::Time {
                column: Some("ts".into()),
                granularity: Granularity::Day
            }
        );
        assert_eq!(
            resolve_bq(serde_json::json!({ "ingestion": "day" }), &specs)
                .unwrap()
                .unwrap()
                .key,
            PartitionKey::Time {
                column: None,
                granularity: Granularity::Day
            }
        );
    }

    #[test]
    fn bigquery_partition_refuses_a_form_the_column_type_cannot_take() {
        let specs = [
            typed("ts", "TIMESTAMP"),
            typed("d", "DATE"),
            typed("v", "STRING"),
            typed("n", "INT64"),
        ];
        let err = |block: serde_json::Value| resolve_bq(block, &specs).unwrap_err().to_string();
        let e = err(serde_json::json!({ "column": "d", "granularity": "hour" }));
        assert!(e.contains("`d` is a DATE, which has no hours"), "{e}");
        let e = err(serde_json::json!({ "column": "v" }));
        assert!(e.contains("cannot partition on `v` (STRING)"), "{e}");
        let e = err(serde_json::json!({ "column": "n" }));
        assert!(e.contains("cannot partition on `n` (INT64)"), "{e}");
        let e = err(serde_json::json!({ "column": "nope" }));
        assert!(e.contains("`nope` is not a column of the export"), "{e}");
        let e = err(
            serde_json::json!({ "range": { "column": "ts", "start": 0, "end": 10, "interval": 1 } }),
        );
        assert!(
            e.contains("`range` partitions an INT64 column, and `ts` is TIMESTAMP"),
            "{e}"
        );
        let e = err(serde_json::json!({ "column": "d) FROM x; --" }));
        assert!(e.contains("not a plain SQL identifier"), "{e}");
    }

    #[test]
    fn snowflake_partition_is_a_leading_date_trunc_and_refuses_bigquery_only_forms() {
        let specs = [
            typed("ts", "TIMESTAMP_NTZ(6)"),
            typed("d", "DATE"),
            typed("n", "NUMBER(38,0)"),
        ];
        let resolve = |block: serde_json::Value| {
            resolve_partition(
                "e",
                &load_with("snowflake", serde_json::json!({ "partition": block })),
                LoadMode::Full,
                &specs,
                SpecFit::Strict,
            )
        };
        let p = resolve(serde_json::json!({ "column": "ts", "granularity": "month" }))
            .unwrap()
            .unwrap();
        assert_eq!(p.expr, "DATE_TRUNC('MONTH', ts)");
        assert_eq!(
            resolve(serde_json::json!({ "column": "d" }))
                .unwrap()
                .unwrap()
                .expr,
            "DATE_TRUNC('DAY', d)"
        );
        let err = |block| resolve(block).unwrap_err().to_string();
        let e = err(serde_json::json!({ "column": "ts", "expiration_days": 30 }));
        assert!(e.contains("Snowflake has no partition expiry"), "{e}");
        let e = err(serde_json::json!({ "column": "ts", "require_filter": true }));
        assert!(e.contains("Snowflake has no partition expiry"), "{e}");
        let e = err(serde_json::json!({ "ingestion": "day" }));
        assert!(e.contains("no `range` or `ingestion` partitions"), "{e}");
        let e = err(
            serde_json::json!({ "range": { "column": "n", "start": 0, "end": 10, "interval": 1 } }),
        );
        assert!(e.contains("no `range` or `ingestion` partitions"), "{e}");
        let e = err(serde_json::json!({ "column": "n" }));
        assert!(e.contains("cannot partition on `n` (NUMBER)"), "{e}");
    }

    #[test]
    fn hourly_partitions_outlive_the_table_without_a_short_expiry() {
        let hourly = |column: Option<&str>| PartitionKey::Time {
            column: column.map(String::from),
            granularity: Granularity::Hour,
        };
        assert!(hourly_partitions_outlive_the_table(
            &hourly(Some("ts")),
            None
        ));
        assert!(hourly_partitions_outlive_the_table(
            &hourly(None),
            Some(417)
        ));
        assert!(!hourly_partitions_outlive_the_table(
            &hourly(Some("ts")),
            Some(416)
        ));
        let daily = PartitionKey::Time {
            column: Some("ts".into()),
            granularity: Granularity::Day,
        };
        assert!(!hourly_partitions_outlive_the_table(&daily, None));
        assert_eq!(HOURLY_LIFETIME_DAYS, 416);
    }

    #[test]
    fn partition_keys_compare_by_column_and_spec() {
        let time = |c: Option<&str>, g| PartitionKey::Time {
            column: c.map(String::from),
            granularity: g,
        };
        assert!(time(Some("ts"), Granularity::Day).same_as(&time(Some("TS"), Granularity::Day)));
        assert!(!time(Some("ts"), Granularity::Day).same_as(&time(Some("ts"), Granularity::Month)));
        assert!(!time(Some("ts"), Granularity::Day).same_as(&time(None, Granularity::Day)));
        let range = |interval| PartitionKey::Range {
            column: "n".into(),
            start: 0,
            end: 100,
            interval,
        };
        assert!(range(5).same_as(&range(5)));
        assert!(!range(5).same_as(&range(10)));
        assert!(!range(5).same_as(&time(Some("n"), Granularity::Day)));
        assert_eq!(time(Some("ts"), Granularity::Day).describe(), "`ts` by day");
        assert_eq!(
            time(None, Granularity::Hour).describe(),
            "load time by hour"
        );
        assert_eq!(range(5).describe(), "`n` in steps of 5 from 0 to 100");
        assert_eq!(Granularity::Hour.coarser(), Some(Granularity::Day));
        assert_eq!(Granularity::Year.coarser(), None);
        assert_eq!(Granularity::parse_sql("MONTH"), Some(Granularity::Month));
        assert_eq!(Granularity::parse_sql("WEEK"), None);
    }

    /// Where the current state will live, resolved from the CONFIG alone: the
    /// section's key with the export's own block layered over it. The EXTRACT asks
    /// this too — a base-and-buffer table's rows carry the delete flag as data, and
    /// a wrong answer here lands a base whose flag is NULL on every row.
    #[test]
    fn the_resolved_layout_composes_the_section_and_the_export_override() {
        let cfg = |load: &str, export_extra: &str, mode: &str| {
            let yaml = format!(
                "source:\n  type: mysql\n  url_env: DB_URL\nexports:\n  - name: t\n    \
                 table: t\n    mode: {mode}\n    cursor_column: updated_at\n    \
                 format: parquet\n    destination: {{ type: local, path: /tmp/t }}\n{export_extra}\
                 load:\n  target: bigquery\n  project: p\n  dataset: d\n{load}"
            );
            serde_yaml_ng::from_str::<crate::config::Config>(&yaml).expect("a config")
        };

        let written = cfg("  layout: base_buffer\n", "", "incremental");
        assert_eq!(
            resolved_layout(&written, &written.exports[0], None),
            CdcLayout::BaseAndBuffer,
            "an ordinary incremental export asks for a base by name"
        );

        let unwritten = cfg("", "", "incremental");
        assert_eq!(
            resolved_layout(&unwritten, &unwritten.exports[0], None),
            CdcLayout::LogAndView,
            "unwritten keeps the changelog and its view"
        );

        let overridden = cfg(
            "  layout: base_buffer\n",
            "    load:\n      layout: log_view\n",
            "incremental",
        );
        assert_eq!(
            resolved_layout(&overridden, &overridden.exports[0], None),
            CdcLayout::LogAndView,
            "the export's own block layers over the section"
        );

        let full = cfg("  layout: base_buffer\n", "", "full");
        assert_eq!(
            resolved_layout(&full, &full.exports[0], None),
            CdcLayout::LogAndView,
            "a full load overwrites its table; the key means nothing there"
        );
    }

    /// A stream with a `backfill:` on a warehouse without `rivet compact` (Snowflake)
    /// keeps the changelog and its view: a base there would freeze at the backfill
    /// while its buffer grew for ever, with every load prescribing a command that can
    /// only fail. RED against deriving the layout from the export alone.
    #[test]
    fn a_warehouse_without_compact_keeps_the_changelog_and_its_view() {
        let cfg = serde_yaml_ng::from_str::<crate::config::Config>(
            "source:\n  type: postgres\n  url: postgresql://localhost/db\nexports:\n\
             \x20 - name: t\n    table: t\n    mode: cdc\n    format: parquet\n\
             \x20   cdc: { backfill: auto, checkpoint: ./t.ckpt }\n\
             \x20   destination: { type: gcs, bucket: b, prefix: t/ }\n\
             load:\n  target: snowflake\n  connection: c\n  warehouse: w\n  database: d\n\
             \x20 schema: s\n  storage_integration: i\n",
        )
        .expect("a config");
        assert_eq!(
            resolved_layout(&cfg, &cfg.exports[0], None),
            CdcLayout::LogAndView
        );
    }

    /// Whether the base carries `__is_deleted`. A stream expresses deletes, so it
    /// defaults ON there; a query-based export cannot, so it defaults OFF and does
    /// not pay for the column. Written, the key decides either way.
    #[test]
    fn the_delete_flag_defaults_on_for_a_stream_and_off_for_a_query() {
        let cfg = |load: &str, mode: &str| {
            let yaml = format!(
                "source:\n  type: mysql\n  url_env: DB_URL\nexports:\n  - name: t\n    \
                 table: t\n    mode: {mode}\n    cursor_column: updated_at\n    \
                 format: parquet\n    destination: {{ type: local, path: /tmp/t }}\n\
                 load:\n  target: bigquery\n  project: p\n  dataset: d\n{load}"
            );
            serde_yaml_ng::from_str::<crate::config::Config>(&yaml).expect("a config")
        };
        let q = cfg("", "incremental");
        assert!(
            !resolved_deleted_flag(&q, &q.exports[0], None),
            "a query cannot express a delete — no column by default"
        );
        let s = cfg("", "cdc");
        assert!(
            resolved_deleted_flag(&s, &s.exports[0], None),
            "a stream can, and its base keeps the flag"
        );
        let asked = cfg("  deleted_flag: true\n", "incremental");
        assert!(
            resolved_deleted_flag(&asked, &asked.exports[0], None),
            "written, the key decides"
        );
        let refused = cfg("  deleted_flag: false\n", "cdc");
        assert!(
            !resolved_deleted_flag(&refused, &refused.exports[0], None),
            "and it decides against a stream too"
        );
    }

    /// A multiplex stream's `load.tables.<name>` block must reach the EXTRACT, not only
    /// the load plan.
    ///
    /// The plan builder applied all three layers while the extract-side readers stopped at
    /// the export block, so the warehouse expected a per-table answer and the snapshot leg
    /// stamped one export-level value into every captured table's files. The leg has the
    /// table name in hand (`cdc_job.rs`, the `pending_idx` loop), so the fix is the
    /// argument, not a new mechanism.
    ///
    /// RED against passing the export level: `customers` reads `true` with the override
    /// ignored.
    #[test]
    fn a_per_table_override_reaches_the_extract_not_only_the_load_plan() {
        let cfg = crate::config::Config::from_yaml(
            r#"
source:
  type: mysql
  url: "mysql://localhost/test"
exports:
  - name: cdc
    tables: [orders, customers]
    mode: cdc
    format: parquet
    cdc:
      checkpoint: ./cdc.ckpt
      initial: snapshot
    destination:
      type: gcs
      bucket: b
      prefix: cdc/
    load:
      deleted_flag: true
      partition: { column: created_at, granularity: day }
      tables:
        customers: { deleted_flag: false, partition: none }
load:
  target: bigquery
  project: p
  dataset: d
"#,
        )
        .expect("a multiplex config");
        let export = &cfg.exports[0];

        assert!(
            resolved_deleted_flag(&cfg, export, Some("orders")),
            "a table with no block of its own keeps the export's answer"
        );
        assert!(
            !resolved_deleted_flag(&cfg, export, Some("customers")),
            "its own block decides — this is what the load plan already honoured"
        );
        assert!(
            resolved_deleted_flag(&cfg, export, None),
            "asked for the export as a whole, the export-level answer stands"
        );

        assert!(
            resolved_partition(&cfg, export, Some("orders")).is_some(),
            "the stream's default partition applies to a table without a block"
        );
        assert!(
            resolved_partition(&cfg, export, Some("customers")).is_none(),
            "`partition: none` clears the inherited one for that table only"
        );
    }

    /// The partition the WRITER budgets each part against, resolved from the config alone.
    /// Shipped on this branch with no test of its own while both its siblings had one.
    #[test]
    fn the_resolved_partition_composes_the_section_and_the_export_override() {
        let cfg = |load: &str, export_extra: &str| {
            let yaml = format!(
                "source:\n  type: mysql\n  url_env: DB_URL\nexports:\n  - name: t\n    \
                 table: t\n    mode: incremental\n    cursor_column: updated_at\n    \
                 format: parquet\n    destination: {{ type: local, path: /tmp/t }}\n{export_extra}\
                 load:\n  target: bigquery\n  project: p\n  dataset: d\n{load}"
            );
            serde_yaml_ng::from_str::<crate::config::Config>(&yaml).expect("a config")
        };

        let none = cfg("", "");
        assert!(
            resolved_partition(&none, &none.exports[0], None).is_none(),
            "no `partition:` anywhere leaves the part sizing to max_file_size alone"
        );

        let shared = cfg(
            "  partition: { column: created_at, granularity: day }\n",
            "",
        );
        let spec = resolved_partition(&shared, &shared.exports[0], None)
            .expect("the section's partition applies to every export");
        assert_eq!(spec.form.column(), Some("created_at"));

        let overridden = cfg(
            "  partition: { column: created_at, granularity: day }\n",
            "    load:\n      partition: { column: made_at, granularity: month }\n",
        );
        let spec = resolved_partition(&overridden, &overridden.exports[0], None)
            .expect("the export's own block layers over the section");
        assert_eq!(spec.form.column(), Some("made_at"));

        let cleared = cfg(
            "  partition: { column: created_at, granularity: day }\n",
            "    load:\n      partition: none\n",
        );
        assert!(
            resolved_partition(&cleared, &cleared.exports[0], None).is_none(),
            "`none` clears an inherited partition rather than inheriting it"
        );
    }

    /// Only a CDC load of a stream WITH `backfill:` takes the base-and-buffer
    /// layout; the same stream without a baseline, and any batch load, keep the
    /// changelog + view.
    #[test]
    fn a_cdc_export_with_a_backfill_lands_as_base_and_buffer_only() {
        let parse = |yaml: &str| {
            serde_yaml_ng::from_str::<crate::config::ExportConfig>(yaml).expect("an export")
        };
        let with = parse(
            "name: t\ntable: t\nmode: cdc\nformat: parquet\n\
             cdc: { backfill: auto, checkpoint: ./t.ckpt }\n\
             destination: { type: local, path: /tmp/t }\n",
        );
        let without = parse(
            "name: t\ntable: t\nmode: cdc\nformat: parquet\ncdc: { checkpoint: ./t.ckpt }\n\
             destination: { type: local, path: /tmp/t }\n",
        );
        assert_eq!(
            cdc_layout(&with, LoadMode::Cdc, None, true),
            CdcLayout::BaseAndBuffer
        );
        assert_eq!(
            cdc_layout(&without, LoadMode::Cdc, None, true),
            CdcLayout::LogAndView
        );
        assert_eq!(
            cdc_layout(&with, LoadMode::Full, None, true),
            CdcLayout::LogAndView,
            "a batch load of the same export is no CDC layout"
        );
        // A warehouse that cannot compact (Snowflake) never gets a base and a buffer:
        // the base would freeze at the backfill and the buffer grow for ever, with no
        // view either. RED against deriving the layout from the export alone.
        assert_eq!(
            cdc_layout(&with, LoadMode::Cdc, None, false),
            CdcLayout::LogAndView,
            "no compaction, no base: the changelog and its view"
        );
        // WRITTEN, the key decides — which is how an ordinary query-based
        // incremental export gets a physical base to compact into.
        use crate::config::load::LayoutChoice;
        assert_eq!(
            cdc_layout(
                &without,
                LoadMode::Incremental,
                Some(LayoutChoice::BaseBuffer),
                true
            ),
            CdcLayout::BaseAndBuffer,
            "an incremental export asks for a base by name"
        );
        assert_eq!(
            cdc_layout(&with, LoadMode::Cdc, Some(LayoutChoice::LogView), true),
            CdcLayout::LogAndView,
            "written wins over the backfill-derived default"
        );
        assert_eq!(
            cdc_layout(&with, LoadMode::Full, Some(LayoutChoice::BaseBuffer), true),
            CdcLayout::LogAndView,
            "a full load overwrites the whole table; the key means nothing there"
        );
        assert_eq!(
            cdc_layout(&without, LoadMode::Incremental, None, true),
            CdcLayout::LogAndView,
            "unwritten keeps what shipped"
        );
        // The two properties every site depends on, as a truth table.
        assert!(CdcLayout::BaseAndBuffer.log_is_disposable());
        assert!(CdcLayout::BaseAndBuffer.compacts());
        assert!(!CdcLayout::LogAndView.log_is_disposable());
        assert!(!CdcLayout::LogAndView.compacts());
    }
}
