//! CDC export config: the `cdc:` block's types, the baseline pairing, and every rule that validates a `mode: cdc` export.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    Config, DestinationType, ExportConfig, ExportMode, SourceType, overlapping_table_pair,
};

/// Why a `mode: cdc` export on Oracle is refused — shared by the config loader and `rivet init`.
pub const ORACLE_CDC_UNSUPPORTED: &str =
    "`mode: cdc` is not supported for Oracle yet — use `mode: full`, `chunked` or `incremental`";

/// `until_current` defaults to `true` — the OSS model is the BOUNDED, scheduler-
/// driven drain ("read to the log end and exit"). `until_current: false` is an
/// explicit opt-in to the continuous model; making it the default would silently
/// put a hand-written CDC config onto it.
///
/// What `false` actually means is ENGINE-SPECIFIC, and the earlier wording
/// ("the never-terminating streaming path") was only true for one of them:
///
/// * **MySQL** — genuinely long-lived: a continuous run omits
///   `BINLOG_DUMP_NON_BLOCK` (`source/mysql/cdc.rs`), so the dump BLOCKS on the
///   binlog and the process stays up with an idle source.
/// * **PostgreSQL / SQL Server** — POLL adapters. `false` removes the open-time
///   ceiling, nothing more: the run still exits on CATCH-UP, so any moment the
///   source falls quiet ends it. `DrainMode`'s own doc says these "exit on
///   catch-up and an outer loop re-wraps them" — that outer loop does not exist
///   (`run_capture` has exactly two call sites, neither looping), so the run is
///   one unbounded pass, not a daemon. Measured on the PG CDC stand: exit 0
///   within seconds, both idle and under a 200-row/0.3 s writer.
/// * **MongoDB** — a tailable change stream: it keeps returning events under
///   sustained writes and stops on an empty poll, so it behaves like MySQL while
///   traffic lasts and like the poll adapters when it stops.
///
/// No data is at risk either way — the checkpoint stops at the last committed
/// position and the next run resumes there (defer-not-drop). The hazard is
/// operational: on a poll engine, `false` needs a supervisor to re-run it, or the
/// stream simply stops. If you want a scheduler, use the default.
fn default_true() -> bool {
    true
}

/// Default PostgreSQL logical slot when `cdc.slot` is omitted — shared by the
/// runner ([`crate::pipeline`]'s cdc job) and config validation, so the
/// same-slot conflict check sees the value that will actually be used.
pub const DEFAULT_PG_SLOT: &str = "rivet_slot";
/// Default MySQL replica `server_id` when `cdc.server_id` is omitted (see
/// [`DEFAULT_PG_SLOT`] for why this is a shared const).
pub const DEFAULT_MYSQL_SERVER_ID: u32 = 4271;

/// What the FIRST CDC run does before draining changes.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CdcInitialMode {
    /// Anchor-then-snapshot: create the resume anchor (PostgreSQL slot /
    /// MySQL binlog checkpoint / SQL Server LSN checkpoint) FIRST, then run a
    /// full batch snapshot of each table into `<destination>[/<table>]/snapshot/`,
    /// then drain CDC. Because the anchor predates the snapshot read, anything
    /// changed during the snapshot also appears in the change stream — an
    /// overlap (dedupe by PK + `__op`), never a gap. The safe switch ordering,
    /// enforced by construction instead of operator discipline.
    Snapshot,
    // NOTE: `adopt` (anchor an already-loaded table, move no rows) is NOT an OSS
    // mode — it belongs to the paid tier, whose audit is what consumes the
    // anchor (the one-way extension seam, ADR-0026; docs may name the crate,
    // this tree may not). Adding a variant here would put it in the published
    // config schema, the generated reference and the OSS binary — irreversibly,
    // once the crate is published. `oss_rejects_the_pro_only_initial_adopt` is
    // the guard.
}

/// Per-export CDC settings, required when `mode: cdc`. The output `table`,
/// `destination`, and `format` come from the export itself; this carries only the
/// CDC-specific knobs (resume + per-engine stream params).
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct CdcExportConfig {
    /// First-run behaviour: `snapshot` = anchor → full snapshot → drain (see
    /// [`CdcInitialMode`]). Omitted ⇒ capture changes only, with no anchor step
    /// (the default; the operator owns the initial load).
    ///
    /// `snapshot` anchors, and on engines with no server-side anchor (MySQL, SQL
    /// Server) that makes `checkpoint:` mandatory — the checkpoint file IS the
    /// anchor there.
    ///
    /// This doc line is what the generated config reference renders, so every
    /// accepted value must be explained HERE: the reference lists the variants
    /// from the enum but describes only this sentence, so an explanation left on
    /// a variant alone documents a value the reader is told exists and never told
    /// the meaning of.
    #[serde(default)]
    pub initial: Option<CdcInitialMode>,
    /// Persist/resume the source log position to this file. Omit to tail from the
    /// current position without checkpointing.
    pub checkpoint: Option<String>,
    /// Catch up to the source's current end and exit (a bounded run), instead of
    /// streaming indefinitely — ideal for a scheduler. For MySQL this is a
    /// non-blocking binlog dump; PostgreSQL / SQL Server already drain-and-exit.
    /// **Defaults to `true`** (bounded): the OSS model is scheduler-driven, and
    /// omitting this must NOT silently start a never-terminating stream. Setting
    /// `false` opts into the continuous model, which is engine-specific: a true
    /// daemon on MySQL (blocking binlog dump) and MongoDB (the change stream
    /// blocks awaiting events; ends only if the stream is invalidated/closed);
    /// PostgreSQL / SQL Server still exit on catch-up — one unbounded pass, run
    /// it under a supervisor.
    #[serde(default = "default_true")]
    pub until_current: bool,
    /// Stop at the first COMMIT BOUNDARY once N change events have been
    /// captured (default: until end of stream / interrupted). A soft cap, like
    /// `rollover`: a transaction is never split, so the run may overshoot N by
    /// the remainder of the transaction the cap landed in — a hard per-event
    /// stop cut transactions mid-flight and left the stream unable to advance
    /// past them.
    pub max_events: Option<usize>,
    /// Rows per output part file (default 100000). A part also rolls at a
    /// transaction boundary, so it never splits a transaction. Larger ⇒ fewer,
    /// bigger files but more drain memory — the PostgreSQL peek reads a part's
    /// worth per batch, so drain RSS is O(rollover). Tune per workload: raise it
    /// to cut file count, lower it to cap memory on a small extractor.
    pub rollover: Option<usize>,
    /// Roll a part once its buffered changes reach this many MB, whichever comes
    /// first with `rollover`. Caps the in-memory buffer and the part file size by
    /// bytes instead of a fixed row count — predictable for tables with wide
    /// (large JSON / blob) rows, mirroring the batch path's `batch_size_memory_mb`.
    /// Defaults to 256 (MiB): the row count alone is a budget for one row width,
    /// and absence must not mean "no byte budget". The bytes are the buffered
    /// changes' RESIDENT cost (struct + commit position + values), not the part
    /// file's size on disk. It bounds the buffer, not the process: a roll encodes
    /// up to 16 tables' parts at once, each a columnar copy of its buffered rows,
    /// so peak RSS sits above the budget (measured +31% on a 60-table stream).
    pub rollover_memory_mb: Option<usize>,
    /// MySQL replica server-id for the binlog connection (default 4271; must be
    /// distinct from the source's and any other replica).
    pub server_id: Option<u32>,
    /// PostgreSQL logical replication slot name (default `rivet_slot`).
    pub slot: Option<String>,
    /// SQL Server CDC capture instance, e.g. `dbo_orders` — required for
    /// `sqlserver://` sources.
    pub capture_instance: Option<String>,
    /// Which EXPORTS supply the baseline read (see [`CdcBackfill`]). Absent ⇒ no
    /// baseline: the stream captures changes only, and the operator owns the
    /// initial load.
    #[serde(default, deserialize_with = "backfill_written_means_a_value")]
    pub backfill: Option<CdcBackfill>,
}

/// The exports whose read strategy the baseline legs borrow.
///
/// A multiplex stream carries tables that do not agree on how they are read: on
/// the dev stand `orders` and `users` resolve to keyset-parallel over `id` while
/// `ext_ref_id_history` must range-chunk `ref_id`, having no unique key. One
/// strategy stated inside the `cdc:` block cannot cover them, and a per-table map
/// there would be a second vocabulary for what an export block already says — two
/// ways to declare a key is two truths, the same argument that keeps the type
/// overrides in one place.
///
/// So the baseline is declared by REFERENCE: each table names the ordinary batch
/// export that already describes how to read it — mode, key, page size, workers,
/// `chunk_checkpoint`, `tuning`, `columns`. The leg still WRITES where the
/// snapshot leg always wrote (`<destination>/<table>/snapshot/`), so every load
/// invariant is untouched: one export owns the prefix, and the referenced export
/// contributes a recipe, not a second load target.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum CdcBackfill {
    /// `auto` — pair each captured table with the export whose `table:` names it.
    Auto(AutoWord),
    /// An explicit list of export names, for a config where the pairing is not
    /// one-to-one by name.
    Exports(Vec<String>),
}

/// `backfill:` written with no value (YAML null) is refused, not read as absent —
/// absent means "no baseline", and a key the operator typed must say which one.
fn backfill_written_means_a_value<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<CdcBackfill>, D::Error> {
    CdcBackfill::deserialize(d).map(Some).map_err(|_| {
        serde::de::Error::custom(
            "`cdc.backfill` must be `auto` or a list of export names (`[orders, customers]`); \
             leave the key out for a stream with no baseline",
        )
    })
}

/// The literal `auto`, as its own type so the untagged enum cannot swallow a
/// mistyped word: `backfill: atuo` must fail the parse, not fall through to the
/// list arm and then to a confusing "expected a sequence".
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AutoWord {
    Auto,
}

impl schemars::JsonSchema for CdcBackfill {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CdcBackfill".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "`auto` (pair each captured table with the export that names it), \
                            or a list of export names",
            "anyOf": [
                { "type": "string", "enum": ["auto"] },
                { "type": "array", "items": { "type": "string" } }
            ]
        })
    }
}

/// The tables one `mode: cdc` export captures, in configured order.
///
/// `tables:` is the multiplex form and `table:` the single one; every caller that
/// asks "which tables does this stream carry" must get the same answer, so the
/// two spellings are folded HERE rather than at each call site.
pub fn cdc_captured_tables(export: &ExportConfig) -> Vec<String> {
    match (&export.tables, &export.table) {
        (Some(ts), _) => ts.clone(),
        (None, Some(t)) => vec![t.clone()],
        (None, None) => Vec::new(),
    }
}

/// The bare relation name — the last dotted segment.
fn bare(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

/// Whether two configured relation names name ONE table.
///
/// Both QUALIFIED: they must agree in full — `sales.orders` is not `public.orders`,
/// and a MongoDB collection named `audit.events` is not `app.events` (a dot is a
/// legal character there, not a qualifier). A BARE side pairs only with the
/// DEFAULT schema's spelling: `orders` is `public.orders` (`dbo.orders`) to whoever
/// left the schema off, and is NOT `sales.orders` — pairing those made an unrelated
/// export the recipe and the run loop stopped running it.
/// ponytail: the default-schema list is `public`/`dbo`; MySQL's `db.table` needs
/// the qualified spelling on both sides.
fn same_relation(a: &str, b: &str) -> bool {
    match (a.contains('.'), b.contains('.')) {
        (true, true) => a == b,
        (false, false) => a == b,
        _ => {
            let (qualified, plain) = if a.contains('.') { (a, b) } else { (b, a) };
            let (schema, leaf) = qualified.rsplit_once('.').unwrap_or(("", qualified));
            matches!(schema, "public" | "dbo") && leaf == plain
        }
    }
}

impl CdcExportConfig {
    /// Whether the stream has a baseline leg (`initial: snapshot` or `backfill:`) and so writes the `snapshot/` child.
    pub fn has_baseline(&self) -> bool {
        self.initial == Some(CdcInitialMode::Snapshot) || self.backfill.is_some()
    }

    /// `until_current: false` — a continuous stream the operator stops, never a bounded drain.
    pub fn runs_until_stopped(&self) -> bool {
        !self.until_current
    }

    /// Neither `initial: snapshot` nor `backfill:` — no anchor step, no baseline legs.
    pub fn captures_changes_only(&self) -> bool {
        !self.has_baseline()
    }
}

/// Why a baseline on this engine needs `cdc.checkpoint:`, or `None` when it does not.
///
/// ONE rule for both ways to declare a baseline (`initial:` in any mode, so a future
/// mode inherits it; `backfill:`): every engine but PostgreSQL has no server-side
/// anchor, so the checkpoint file IS the anchor — the baseline is only safe because
/// the anchor precedes it, and without it each run re-anchors at the current log
/// position and silently skips every change since the last one.
pub fn baseline_checkpoint_refusal(
    export: &ExportConfig,
    source_type: crate::config::SourceType,
) -> Option<String> {
    let cdc = export.cdc.as_ref()?;
    let declared = if cdc.backfill.is_some() {
        "`cdc.backfill:`"
    } else if cdc.initial.is_some() {
        "`cdc.initial:`"
    } else {
        return None;
    };
    if source_type == crate::config::SourceType::Postgres || cdc.checkpoint.is_some() {
        return None;
    }
    Some(format!(
        "export '{}': {declared} on {source_type:?} requires `cdc.checkpoint:` — this engine has \
         no server-side anchor, so the checkpoint file is the anchor: the baseline is only safe \
         because the anchor precedes it, and without it each run re-anchors at the current log \
         position and silently skips every change since the last one",
        export.name
    ))
}

/// The `columns:` a `mode: cdc` export resolves types with: its own, plus every
/// backfill recipe's — each recipe's bare keys QUALIFIED to its table, so one
/// table's declaration cannot reach a same-named column elsewhere. The CDC
/// export's own keys win. One map for the baseline leg, the stream and the
/// recorded load spec: they write one `<table>__changes`, so a column typed on
/// the recipe alone (the placement the conflict refusal recommends) must reach
/// all three, not only the leg.
pub fn effective_columns(
    export: &ExportConfig,
    all: &[ExportConfig],
) -> std::collections::HashMap<String, String> {
    let mut merged = std::collections::HashMap::new();
    for (table, recipe) in resolve_backfill(export, all).unwrap_or_default() {
        // Bare keys first, qualified second: a recipe naming both `price` and
        // `orders.price` resolves to the qualified one, whatever the map order.
        for (k, v) in recipe.columns.iter().filter(|(k, _)| !k.contains('.')) {
            // BARE table: `overrides_for_unit` narrows a `table.column` key by its
            // first dot, so `dbo.orders.price` would never reach unit `dbo.orders`.
            merged.insert(format!("{}.{k}", bare(&table)), v.clone());
        }
        for (k, v) in recipe.columns.iter().filter(|(k, _)| k.contains('.')) {
            merged.insert(k.clone(), v.clone());
        }
    }
    merged.extend(export.columns.clone());
    merged
}

/// Every export some `mode: cdc` export claims as a baseline recipe.
///
/// The run loop asks this to avoid reading one table twice in a single
/// invocation: the CDC export pulls its recipes itself, after the anchor. Names
/// only — an unresolvable reference is config load's business, and this must stay
/// usable (and silent) on a config that would not validate.
pub fn backfill_recipe_names(exports: &[ExportConfig]) -> std::collections::HashSet<String> {
    exports
        .iter()
        .filter(|e| e.mode == ExportMode::Cdc)
        .flat_map(|cdc| resolve_backfill(cdc, exports).unwrap_or_default())
        .map(|(_, recipe)| recipe.name.clone())
        .collect()
}

/// Why a whole-config `rivet plan` skips `export`, or `None` when it plans it.
pub fn batch_plan_skip_reason(
    export: &ExportConfig,
    recipes: &std::collections::HashSet<String>,
) -> Option<&'static str> {
    if export.mode == ExportMode::Cdc {
        Some("a CDC export has no batch plan; it runs with `rivet run`")
    } else if recipes.contains(&export.name) {
        Some("it is the backfill recipe of a `mode: cdc` export, which runs it after the anchor")
    } else {
        None
    }
}

/// `exports` minus the ones named in `recipes` — the whole-config run loop's view.
pub fn without_backfill_recipes<'a>(
    exports: impl IntoIterator<Item = &'a ExportConfig>,
    recipes: &std::collections::HashSet<String>,
) -> Vec<&'a ExportConfig> {
    exports
        .into_iter()
        .filter(|e| !recipes.contains(&e.name))
        .collect()
}

/// Two captured tables that share a LEAF name (`sales.orders`, `archive.orders`)
/// while a recipe of either declares `columns:` — a `table.column` type key is
/// narrowed by leaf, so one recipe's types would reach BOTH tables.
pub fn same_leaf_typed_pair(pairs: &[(String, &ExportConfig)]) -> Option<(String, String)> {
    for (i, (a, ra)) in pairs.iter().enumerate() {
        for (b, rb) in pairs.iter().skip(i + 1) {
            if a != b && bare(a) == bare(b) && !(ra.columns.is_empty() && rb.columns.is_empty()) {
                return Some((a.clone(), b.clone()));
            }
        }
    }
    None
}

/// Refuse a column `table`'s recipe and the CDC export type DIFFERENTLY — parsed
/// types, so two spellings of one type (`decimal(11,4)` / `numeric(11,4)`) are
/// not a conflict. The two legs write into one warehouse table, so one column
/// cannot have two types; a pure config decision, so it is made at config load
/// (every pair, not only the tables still pending a baseline — a conflict added
/// after the baseline landed must refuse the next run too).
pub fn refuse_backfill_type_conflict(
    cdc_export: &ExportConfig,
    table: &str,
    recipe: &ExportConfig,
) -> anyhow::Result<()> {
    let parsed =
        |e: &ExportConfig| crate::plan::build::parse_column_overrides_pub(&e.columns, &e.name);
    let (recipe_types, cdc_types) = (parsed(recipe)?, parsed(cdc_export)?);
    // BOTH sides narrowed: a qualified recipe key (`orders.price`) against a bare
    // CDC key (`price`) is the same column, and compared raw it was never seen.
    let cdc_for_table = crate::types::overrides_for_unit(&cdc_types, Some(table));
    let recipe_for_table = crate::types::overrides_for_unit(&recipe_types, Some(table));
    for (col, mine) in &recipe_for_table {
        if let Some(theirs) = cdc_for_table.get(col)
            && theirs != mine
        {
            anyhow::bail!(
                "export '{}': column '{col}' of table '{table}' is declared `{mine:?}` by its \
                 backfill export '{}' and `{theirs:?}` by the CDC export — the baseline and the \
                 stream write into one `<table>__changes`, so one column cannot have two types. \
                 Keep the declaration on the batch export and drop the other.",
                cdc_export.name,
                recipe.name
            );
        }
    }
    Ok(())
}

/// The recipe [`resolve_backfill`] paired with `table`, if any.
pub fn backfill_recipe_for<'a>(
    pairs: &[(String, &'a ExportConfig)],
    table: &str,
) -> Option<&'a ExportConfig> {
    pairs.iter().find(|(t, _)| t == table).map(|(_, r)| *r)
}

/// Pair every captured table with the export that supplies its baseline read.
///
/// The ONE definition of the pairing, called by config validation (so a broken
/// reference fails at load, before an anchor exists) and by the CDC job (so the
/// legs it builds cannot disagree with what validation admitted).
///
/// Every refusal here is a case that would otherwise present as success: a table
/// with no recipe would simply not be backfilled, and a run that captures changes
/// over a table whose history was never loaded looks exactly like a healthy one
/// until someone counts rows in the warehouse.
pub fn resolve_backfill<'a>(
    cdc: &ExportConfig,
    all: &'a [ExportConfig],
) -> std::result::Result<Vec<(String, &'a ExportConfig)>, String> {
    let Some(spec) = cdc.cdc.as_ref().and_then(|c| c.backfill.as_ref()) else {
        return Ok(Vec::new());
    };
    let tables = cdc_captured_tables(cdc);
    if tables.is_empty() {
        return Err(format!(
            "export '{}': `cdc.backfill` needs `table:` or `tables:` — there is nothing to \
             pair a baseline export with",
            cdc.name
        ));
    }

    // A candidate is any export that reads ONE named relation: a `query:` export
    // describes rows, not a table, so it can never be the baseline of a captured
    // table, and another `mode: cdc` export reads a log rather than the table.
    let candidate = |e: &ExportConfig| -> bool {
        e.name != cdc.name && e.mode != ExportMode::Cdc && e.table.is_some()
    };

    let mut pairs: Vec<(String, &ExportConfig)> = Vec::with_capacity(tables.len());
    match spec {
        CdcBackfill::Auto(_) => {
            for t in &tables {
                let matches: Vec<&ExportConfig> = all
                    .iter()
                    .filter(|e| {
                        candidate(e) && e.table.as_deref().is_some_and(|et| same_relation(et, t))
                    })
                    .collect();
                match matches.as_slice() {
                    [one] => pairs.push((t.clone(), *one)),
                    [] => {
                        return Err(format!(
                            "export '{}': `cdc.backfill: auto` found no export reading table \
                             '{t}' — add one (the batch export that already describes how to \
                             read it), or name the exports explicitly with \
                             `backfill: [<name>, …]`.\n  Without a baseline this table would \
                             capture changes over history nobody loaded, and the run would \
                             still report success.",
                            cdc.name
                        ));
                    }
                    many => {
                        let names: Vec<&str> = many.iter().map(|e| e.name.as_str()).collect();
                        return Err(format!(
                            "export '{}': `cdc.backfill: auto` found {} exports reading table \
                             '{t}' ({}) — the pairing is ambiguous. Name the one you mean with \
                             `backfill: [<name>, …]`.",
                            cdc.name,
                            many.len(),
                            names.join(", ")
                        ));
                    }
                }
            }
        }
        CdcBackfill::Exports(names) => {
            for n in names {
                let Some(e) = all.iter().find(|e| e.name == *n) else {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names export '{n}', which this config does \
                         not define",
                        cdc.name
                    ));
                };
                if e.name == cdc.name {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names the CDC export itself — the baseline \
                         is read by a BATCH export, not by the stream",
                        cdc.name
                    ));
                }
                if e.mode == ExportMode::Cdc {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names export '{n}', which is `mode: cdc` — \
                         a baseline reads the TABLE, not another change stream",
                        cdc.name
                    ));
                }
                let Some(t) = e.table.as_deref() else {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names export '{n}', which has no `table:` \
                         (a `query:` export describes rows, not a relation, so it cannot be a \
                         captured table's baseline)",
                        cdc.name
                    ));
                };
                let Some(captured) = tables.iter().find(|c| same_relation(c, t)) else {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names export '{n}', which reads '{t}' — a \
                         table this stream does not capture. Capture it, or drop the reference.",
                        cdc.name
                    ));
                };
                if pairs.iter().any(|(c, _)| c == captured) {
                    return Err(format!(
                        "export '{}': `cdc.backfill` names two exports for table '{captured}' — \
                         one baseline per table",
                        cdc.name
                    ));
                }
                pairs.push((captured.clone(), e));
            }
            // Partial coverage is the silent-success shape: the covered tables get a
            // baseline, the rest capture changes over history nobody loaded.
            let missing: Vec<&str> = tables
                .iter()
                .filter(|t| !pairs.iter().any(|(c, _)| c == *t))
                .map(String::as_str)
                .collect();
            if !missing.is_empty() {
                return Err(format!(
                    "export '{}': `cdc.backfill` covers {} of {} captured tables — no baseline \
                     for {}. List every table's export, or remove the ones you do not want \
                     captured from `tables:`.",
                    cdc.name,
                    pairs.len(),
                    tables.len(),
                    missing.join(", ")
                ));
            }
        }
    }
    // A baseline must read the WHOLE table. An incremental / time-window recipe
    // reads a slice, and copying its mode without its cursor fails at plan build —
    // after the anchor already exists, on every run until the config changes.
    for (t, e) in &pairs {
        if !matches!(e.mode, ExportMode::Full | ExportMode::Chunked) {
            return Err(format!(
                "export '{}': backfill export '{}' for table '{t}' is `mode: {:?}` — a baseline \
                 must read the whole table, so only a `full` or `chunked` export can be one; an \
                 incremental read would leave every row outside its window with no baseline.",
                cdc.name, e.name, e.mode
            ));
        }
    }
    Ok(pairs)
}

// Hand-written so the Rust `Default` MATCHES the serde default: `until_current`
// must be `true` (bounded). The derived `Default` would use `bool::default()` =
// `false`, and serde's `default = "default_true"` only affects Deserialize — so
// `CdcExportConfig::default()` would silently mean `DrainMode::Continuous`. That
// default is reached on the drain path (`cdc_job.rs`: `export.cdc.clone()
// .unwrap_or_default()`) whenever a `mode: cdc` export omits the whole `cdc:`
// block (valid for PG/MySQL), turning a minimal bounded drain into a
// never-terminating daemon that persists no resume position — the exact footgun
// `default_true` was added to kill. Mirrors `MongoConfig`'s hand-written Default.
impl Default for CdcExportConfig {
    fn default() -> Self {
        Self {
            initial: None,
            checkpoint: None,
            until_current: true,
            max_events: None,
            rollover: None,
            rollover_memory_mb: None,
            server_id: None,
            slot: None,
            capture_instance: None,
            backfill: None,
        }
    }
}

impl Config {
    /// CDC stream resources are per-export and their **defaults collide**: two
    /// PostgreSQL cdc exports without an explicit `slot:` both resolve to
    /// `rivet_slot` — each export's ack advances `confirmed_flush_lsn` past
    /// changes the *other* never read (mutual, silent data loss). Two MySQL
    /// exports both default to `server_id: 4271` — the server kills the older
    /// replica connection. A shared `checkpoint:` path makes exports overwrite
    /// each other's resume position on any engine. All three are config bugs a
    /// naive multi-table CDC config hits by default, so reject them at load, on
    /// the RESOLVED values (defaults included). SQL Server `capture_instance`
    /// sharing is deliberately allowed: the change-table poll is read-only and
    /// resume state lives in the per-export checkpoint.
    pub(super) fn validate_cdc_resource_conflicts(&self) -> crate::error::Result<()> {
        use std::collections::HashMap;

        let mut slots: HashMap<String, &str> = HashMap::new();
        let mut server_ids: HashMap<u32, &str> = HashMap::new();
        // Keyed by the NORMALISED, case-folded path (see the insert below).
        let mut checkpoints: HashMap<String, &str> = HashMap::new();

        for e in self.exports.iter().filter(|e| e.mode == ExportMode::Cdc) {
            let cdc = e.cdc.as_ref();
            // ZERO is refused for both rollover knobs, the same guard chunk_size
            // has. `0` is not "disable": `buf >= 0` is always true, so a zero
            // budget rolls a part on EVERY committed transaction — a file
            // explosion (one parquet + one PUT per txn), silently, while the
            // operator who typed 0 meant "no cap". `None` is the documented way
            // to get row-count-only; absence gets the protective default.
            if let Some(c) = cdc {
                if c.rollover == Some(0) {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_ROLLOVER_INVALID,
                        "export '{}': cdc.rollover must be >= 1 (got 0). A zero \
                         rollover rolls a part on every committed transaction; \
                         omit the field for the default (100000).",
                        e.name
                    );
                }
                if c.rollover_memory_mb == Some(0) {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_ROLLOVER_INVALID,
                        "export '{}': cdc.rollover_memory_mb must be >= 1 (got 0). \
                         A zero byte budget rolls a part on every committed \
                         transaction; omit the field for the default (256 MiB).",
                        e.name
                    );
                }
                // Checked at VALIDATION, where the error can name the field: the
                // runtime multiply (`mb * 1024 * 1024`) wraps in release, and a
                // wrapped budget of 0 is the file explosion above wearing a
                // plausible-looking huge number.
                if let Some(mb) = c.rollover_memory_mb
                    && mb.checked_mul(1024 * 1024).is_none()
                {
                    crate::config_bail!(
                        crate::error::codes::CONFIG_CDC_ROLLOVER_INVALID,
                        "export '{}': cdc.rollover_memory_mb {} overflows a byte \
                         count on this platform.",
                        e.name,
                        mb
                    );
                }
            }
            match self.source.source_type {
                SourceType::Postgres => {
                    let slot = cdc
                        .and_then(|c| c.slot.clone())
                        .unwrap_or_else(|| DEFAULT_PG_SLOT.to_string());
                    if let Some(prev) = slots.insert(slot.clone(), &e.name) {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_CDC_RESOURCE_CONFLICT,
                            "exports '{prev}' and '{}': same PostgreSQL slot '{slot}' — a slot \
                             has ONE consumer; each export's ack would advance it past changes \
                             the other never read (silent data loss). Set a distinct `cdc.slot:` \
                             per export (the default is '{DEFAULT_PG_SLOT}').",
                            e.name
                        );
                    }
                }
                SourceType::Mysql => {
                    let sid = cdc
                        .and_then(|c| c.server_id)
                        .unwrap_or(DEFAULT_MYSQL_SERVER_ID);
                    if let Some(prev) = server_ids.insert(sid, &e.name) {
                        crate::config_bail!(
                            crate::error::codes::CONFIG_CDC_RESOURCE_CONFLICT,
                            "exports '{prev}' and '{}': same MySQL server_id {sid} — the server \
                             kills the older replica connection when a new one registers with \
                             the same id. Set a distinct `cdc.server_id:` per export (the \
                             default is {DEFAULT_MYSQL_SERVER_ID}).",
                            e.name
                        );
                    }
                }
                SourceType::Mssql => {}
                // MongoDB change streams watch the whole database — no per-export
                // slot or server_id to collide. Two Mongo CDC exports sharing a
                // `checkpoint:` path IS still a conflict, caught by the shared
                // checkpoint check below.
                SourceType::Mongo | SourceType::Oracle => {}
            }
            // RESOLVED, not the raw string — the function's own doc promises "on
            // the RESOLVED values", and the runtime maps every relative path
            // through the config dir (`resolve_checkpoint`), so `./cdc/x.ckpt`
            // (init's own scaffold form) and `cdc/x.ckpt` are ONE file the raw
            // comparison called two. With `parallel_exports` the miss became two
            // streams acking one resume file concurrently; the next run resumed
            // from whichever wrote last and silently skipped the other's span.
            // Validation has no config-dir, so it NORMALISES both spellings the
            // same way the resolver does (component-wise, `.` dropped) — equal
            // normalised relatives resolve equal absolutely, whatever the dir.
            if let Some(ckpt) = cdc.and_then(|c| c.checkpoint.as_deref())
                && let Some(prev) = checkpoints.insert(
                    // `components()` KEEPS a leading `./` (only interior dots are
                    // normalised — measured: the first cut of this fix compared
                    // `[CurDir, cdc, x]` with `[cdc, x]` and still called them
                    // two), so CurDir is filtered explicitly. The resolver never
                    // sees it either: `config_dir.join(p)` makes every dot
                    // interior before ITS components() pass.
                    // Case-folded as well: `Orders.ckpt` and `orders.ckpt` are ONE file
                    // on macOS and Windows, and a config written on Linux is run there.
                    std::path::Path::new(ckpt)
                        .components()
                        .filter(|c| !matches!(c, std::path::Component::CurDir))
                        .collect::<std::path::PathBuf>()
                        .to_string_lossy()
                        .to_lowercase(),
                    &e.name,
                )
            {
                crate::config_bail!(
                    crate::error::codes::CONFIG_CDC_RESOURCE_CONFLICT,
                    "exports '{prev}' and '{}': same checkpoint path '{ckpt}' (compared \
                     case-insensitively: on macOS and Windows two spellings are one file) — \
                     each export must own its resume position or they overwrite each \
                     other's. Set a distinct `cdc.checkpoint:` per export.",
                    e.name
                );
            }
        }
        Ok(())
    }

    /// A `mode: cdc` export captures `table:` or a well-formed `tables:`, never a query.
    pub(super) fn validate_export_cdc_mode(
        &self,
        export: &ExportConfig,
    ) -> crate::error::Result<()> {
        if self.source.source_type == SourceType::Oracle {
            anyhow::bail!("export '{}': {ORACLE_CDC_UNSUPPORTED}", export.name);
        }
        match (&export.table, &export.tables) {
            (None, None) => anyhow::bail!(
                "export '{}': cdc mode requires `table:` (or `tables:` for a \
                 multi-table stream)",
                export.name
            ),
            (None, Some(ts)) => {
                if ts.is_empty() {
                    anyhow::bail!(
                        "export '{}': `tables:` must list at least one table",
                        export.name
                    );
                }
                let mut seen = std::collections::HashSet::new();
                for t in ts {
                    if !seen.insert(t.as_str()) {
                        anyhow::bail!(
                            "export '{}': duplicate table '{}' in `tables:`",
                            export.name,
                            t
                        );
                    }
                }
                // Two DISTINCT strings can still name one relation, and the
                // sink routes each event to the FIRST that matches — so the
                // loser's prefix collects a `_SUCCESS` and a `row_count: 0`
                // manifest on every run, which a downstream loader reads as
                // a healthy, complete, empty export rather than as a config
                // error (round-3B bughunt, DEMONSTRATED live on PostgreSQL:
                // three inserts, `status: success, rows: 3`, all three under
                // `bh_orders/` while `public.bh_orders/` published emptiness).
                // The distinct-string check above cannot see it.
                if self.source.source_type != SourceType::Mongo
                    && let Some((a, b)) = overlapping_table_pair(ts)
                {
                    anyhow::bail!(
                        "export '{}': `tables:` lists both '{}' and '{}', which can name \
                         the SAME relation — a bare name matches any schema, so every \
                         event routes to whichever is listed first and the other \
                         publishes an empty, successful-looking export forever. Keep one \
                         spelling.",
                        export.name,
                        a,
                        b
                    );
                }
                // A local destination writes each table under `<path>/<table>/`,
                // and on a case-insensitive filesystem (macOS, Windows) `Orders/` and
                // `orders/` are ONE directory: same-named parts overwrite each other
                // and the checkpoint moves past the lost rows (bughunt 2026-09-25,
                // measured on MySQL with `lower_case_table_names=0`).
                if export.destination.destination_type == DestinationType::Local
                    && let Some((a, b)) = super::case_colliding_table_pair(ts)
                {
                    anyhow::bail!(
                        "export '{}': `tables:` lists '{}' and '{}', which differ only by \
                         letter case — on a local destination their directories are one \
                         directory on a case-insensitive filesystem (macOS, Windows), so \
                         their parts overwrite each other. Capture one of them in its own \
                         export with its own `destination.path`, or write to a cloud \
                         destination, whose object keys are case-sensitive.",
                        export.name,
                        a,
                        b
                    );
                }
                // A streaming destination has no per-table sub-prefix to
                // extend — `dest_for_table`'s `Stdout` arm is a no-op while
                // the local and cloud arms both append `<table>/`. So every
                // captured table shares ONE stream: round-10 measured two
                // tables emitting two different CSV headers into one output,
                // which any reader downstream mixes silently. No manifest is
                // written for a streaming destination either, so the stream
                // is the only artifact and nothing records the interleave.
                if export.destination.destination_type == DestinationType::Stdout {
                    anyhow::bail!(
                        "export '{}': `tables:` cannot write to `destination: \
                         {{ type: stdout }}` — a stream has no per-table \
                         sub-prefix, so all {} tables would interleave into one \
                         output with their headers and column sets mixed. Use a \
                         local or cloud destination, or capture one table per \
                         export.",
                        export.name,
                        ts.len()
                    );
                }
                if self.source.source_type == SourceType::Mssql {
                    anyhow::bail!(
                        "export '{}': `tables:` is not yet supported for SQL Server — \
                         its capture instances are per-table; use one cdc export per \
                         table (capture_instance each)",
                        export.name
                    );
                }
            }
            // `table:` + `tables:` is refused earlier, by `validate_export_source`.
            (Some(_), _) => {}
        }
        if export.query.is_some() || export.query_file.is_some() {
            anyhow::bail!(
                "export '{}': cdc mode reads the transaction log, not a query — \
                 remove query/query_file and use `table:`",
                export.name
            );
        }
        Ok(())
    }

    /// A `cdc:` block is refused outside `mode: cdc`.
    pub(super) fn validate_cdc_block_needs_cdc_mode(
        &self,
        export: &ExportConfig,
    ) -> crate::error::Result<()> {
        if export.cdc.is_some() && export.mode != ExportMode::Cdc {
            anyhow::bail!(
                "export '{}': a `cdc:` block is only valid with `mode: cdc`",
                export.name
            );
        }
        Ok(())
    }

    /// CDC baselines, checkpoints and resume anchors.
    pub(super) fn validate_export_cdc(&self, export: &ExportConfig) -> crate::error::Result<()> {
        // A baseline (`initial: snapshot` or `backfill:`) writes each table's
        // snapshot under the reserved sub-prefix `snapshot/` — a table actually
        // NAMED "snapshot" would share a prefix with another table's marker.
        if let Some(cdc) = &export.cdc
            && cdc.has_baseline()
        {
            let clashes = |t: &str| t.rsplit('.').next().unwrap_or(t) == "snapshot";
            if export.table.as_deref().is_some_and(clashes)
                || export.tables.iter().flatten().any(|t| clashes(t))
            {
                anyhow::bail!(
                    "export '{}': a table named 'snapshot' collides with the reserved \
                     `snapshot/` sub-prefix that a baseline (`cdc.initial: snapshot` or \
                     `cdc.backfill`) writes — rename the table or use a separate export \
                     without a baseline",
                    export.name
                );
            }
        }

        // `initial:` needs a durable anchor BEFORE anything reads. PostgreSQL
        // pins server-side (the slot); MySQL / SQL Server have no server-side
        // anchor, so the checkpoint file IS the anchor there. Stated against the
        // MODE rather than only `snapshot` by name, so a future `initial:` mode
        // inherits the requirement instead of silently deferring the failure to
        // `ensure_anchor` — which demands a checkpoint on these engines for ANY
        // mode, i.e. after the run has already started.
        // One predicate for both baselines (`initial:` and `backfill:`).
        if let Some(why) = baseline_checkpoint_refusal(export, self.source.source_type) {
            anyhow::bail!(why);
        }

        // `cdc.backfill` is the other way to get a baseline, and it is the same
        // step: anchor first, then read the table. Declaring both would run two
        // baselines over one anchor — the synthesized `mode: full` leg AND the
        // referenced export's — into one prefix, which is not a merge but a
        // duplicate nobody asked for.
        if let Some(cdc) = &export.cdc
            && cdc.backfill.is_some()
            && cdc.initial.is_some()
        {
            anyhow::bail!(
                "export '{}': `cdc.initial:` and `cdc.backfill:` both describe the FIRST run's \
                 baseline — keep one. `initial: snapshot` synthesizes a single-stream full scan; \
                 `backfill:` borrows the read strategy of the batch export that already describes \
                 the table (its key, workers, page size and resume).",
                export.name
            );
        }

        // The pairing itself, resolved by the ONE function the CDC job also calls,
        // so a reference the run would reject cannot pass validation.
        // (The checkpoint requirement is `baseline_checkpoint_refusal`, above.)
        if let Some(cdc) = &export.cdc
            && cdc.backfill.is_some()
        {
            let pairs =
                resolve_backfill(export, &self.exports).map_err(|why| anyhow::anyhow!(why))?;
            // A document store has no schema qualifier: `audit.events` is a
            // collection, not `events` in schema `audit`. The bare-name fold that
            // pairs `orders` with `public.orders` on SQL would pair two DIFFERENT
            // collections here — and turn the other one's export into a recipe the
            // run loop stops running.
            if !self.source.source_type.is_sql() {
                for (captured, recipe) in &pairs {
                    if recipe.table.as_deref() != Some(captured.as_str()) {
                        anyhow::bail!(
                            "export '{}': `cdc.backfill` paired collection '{captured}' with export \
                             '{}', which reads '{}' — a MongoDB collection name is literal (a dot \
                             is part of the name, not a schema), so the recipe must read exactly \
                             `table: {captured}`",
                            export.name,
                            recipe.name,
                            recipe.table.as_deref().unwrap_or("")
                        );
                    }
                }
            }
            // The recipe's READ is validated here, not at the leg: `plan`/`check`
            // skip a recipe, so its table shortcut and `columns:` were first parsed
            // by the leg — after the anchor had been taken.
            for (table, recipe) in &pairs {
                if self.source.source_type.is_sql()
                    && let Some(t) = recipe.table.as_deref()
                {
                    super::export::validate_table_shortcut_ident(&recipe.name, t)?;
                }
                crate::plan::build::parse_column_overrides_pub(&recipe.columns, &recipe.name)?;
                // One column, one type across the recipe and the stream — decided
                // here, for every pair, so a conflict added after the baseline
                // refuses the next run at config load, not after its anchor.
                refuse_backfill_type_conflict(export, table, recipe)?;
            }
            // A `table.column` type key is narrowed by LEAF, so two captured tables
            // with one leaf cannot be typed apart: a recipe's `columns:` on either
            // would type both. Refuse rather than let the later recipe win silently.
            if let Some((a, b)) = same_leaf_typed_pair(&pairs) {
                anyhow::bail!(
                    "export '{}': captured tables '{a}' and '{b}' share the leaf name and a \
                     recipe declares `columns:` — per-table column types are keyed \
                     `table.column` by the bare name, so one declaration would type both. \
                     Capture them in two `mode: cdc` exports, or drop the recipe's `columns:`.",
                    export.name
                );
            }
        }

        // MongoDB change streams and the MySQL binlog have NO server-side resume
        // anchor (unlike a PostgreSQL slot, or SQL Server's change table whose
        // min-LSN floors a missing from-LSN into an over-read): the checkpoint
        // file IS the anchor. Without it every run re-anchors at "now" and
        // silently loses every change since the last one — so `mode: cdc` on
        // these two requires `cdc.checkpoint:` ALWAYS, not only under
        // `initial: snapshot`.
        //
        // MySQL was left out when this rule was first added for mongo, and the
        // hole was total: `cdc.initial` absent skips the `initial.is_some()` rule
        // above, and the run-time backstop that would catch it —
        // `CdcEngine::ensure_anchor`, which demands a checkpoint for
        // Mysql|Mssql|Mongo — is unreachable, because its only production caller
        // sits inside `initial_snapshot_pending`, which returns early when
        // `cdc.initial.is_none()`. Measured on a live stand: two runs with three
        // changes between them captured ZERO events, both exiting 0. `rivet
        // doctor` already prints the exact diagnosis, but `run` never invokes it.
        if export.mode == ExportMode::Cdc
            && matches!(
                self.source.source_type,
                SourceType::Mongo | SourceType::Mysql
            )
            && export
                .cdc
                .as_ref()
                .and_then(|c| c.checkpoint.as_ref())
                .is_none()
        {
            anyhow::bail!(
                "export '{}': {:?} `mode: cdc` requires `cdc.checkpoint:` — this engine has \
                 no server-side resume anchor, so without the checkpoint file each run \
                 re-anchors at the current position and silently loses every change \
                 between runs.",
                export.name,
                self.source.source_type
            );
        }

        // (A `mode: cdc` refusal of DOTTED Mongo collection names lived here until
        // round-3B. It was written when a dotted name really was dropped — the
        // router split it into a bogus `schema.table` — and the ROUTER was fixed
        // while the guard that existed because of the bug stayed, refusing a
        // capture that works and telling operators to rename a production
        // collection. Mongo's routing now has no split arm at all, so the string
        // has one reading; see `a_mongo_dotted_name_never_splits_into_a_schema_
        // qualifier` and `mongo_cdc_accepts_a_dotted_collection_name_because_the_
        // router_addresses_it`.)
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::export::sample_export;

    // ── cdc.backfill: the pairing ────────────────────────────────────────────

    /// A batch export reading one table, named as a recipe candidate.
    fn recipe(name: &str, table: &str) -> ExportConfig {
        let mut e = sample_export(name);
        e.mode = ExportMode::Chunked;
        e.table = Some(table.to_string());
        e.tables = None;
        e.cdc = None;
        e
    }

    /// The stream: N tables, one binlog, a `backfill:` spec.
    fn stream(tables: &[&str], spec: CdcBackfill) -> ExportConfig {
        let mut e = sample_export("stand_cdc");
        e.mode = ExportMode::Cdc;
        e.table = None;
        e.tables = Some(tables.iter().map(|t| t.to_string()).collect());
        e.cdc = Some(CdcExportConfig {
            backfill: Some(spec),
            ..Default::default()
        });
        e
    }

    /// A candidate reads ONE table by `table:` and is not itself a stream: a second
    /// `mode: cdc` export over the same table (a per-table capture kept beside the
    /// multiplex) reads a log, so it must not make the pairing ambiguous. RED against
    /// `e.name != cdc.name || …`, under which the other stream pairs too.
    #[test]
    fn resolve_backfill_never_pairs_a_table_with_another_stream() {
        let orders = recipe("orders", "orders");
        let mut other_stream = recipe("orders_cdc", "orders");
        other_stream.mode = ExportMode::Cdc;
        let auto = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders, other_stream, auto.clone()];
        let pairs = resolve_backfill(&auto, &all).expect("the other stream is not a candidate");
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1.name, "orders");
    }

    /// The happy path both spellings must agree on: every captured table pairs
    /// with the export that reads it, and the recipes keep their own strategies —
    /// which is the whole point, since a multiplex stream's tables do not agree on
    /// how they are read.
    #[test]
    fn resolve_backfill_pairs_tables_with_their_recipe_exports() {
        let orders = recipe("orders", "orders");
        let mut history = recipe("ext_ref_id_history", "ext_ref_id_history");
        // Different strategy per table — the case a single `cdc:`-level block
        // could not express.
        history.chunk_by_key = None;
        history.chunk_column = Some("ref_id".into());

        let auto = stream(
            &["orders", "ext_ref_id_history"],
            CdcBackfill::Auto(AutoWord::Auto),
        );
        let all = vec![orders.clone(), history.clone(), auto.clone()];
        let pairs = resolve_backfill(&auto, &all).expect("every table has exactly one export");
        let named: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(t, e)| (t.as_str(), e.name.as_str()))
            .collect();
        assert_eq!(
            named,
            vec![
                ("orders", "orders"),
                ("ext_ref_id_history", "ext_ref_id_history")
            ]
        );

        // The explicit spelling resolves to the same pairs, in the captured order.
        let listed = stream(
            &["orders", "ext_ref_id_history"],
            CdcBackfill::Exports(vec!["orders".into(), "ext_ref_id_history".into()]),
        );
        let all = vec![orders.clone(), history.clone(), listed.clone()];
        let pairs = resolve_backfill(&listed, &all).expect("both named");
        assert_eq!(pairs.len(), 2);

        // A schema-qualified capture still names the same relation.
        let qualified = stream(&["public.orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders.clone(), qualified.clone()];
        assert_eq!(
            resolve_backfill(&qualified, &all).expect("bare and qualified are one table")[0]
                .1
                .name,
            "orders"
        );

        // No `backfill:` at all ⇒ no pairs, not an error: capture-only is a mode.
        let mut plain = auto.clone();
        plain.cdc = Some(CdcExportConfig::default());
        assert!(resolve_backfill(&plain, &all).unwrap().is_empty());
    }

    /// Every refusal here is a shape that would otherwise present as SUCCESS: the
    /// stream captures changes over a table whose history nobody loaded, and the
    /// run exits 0. RED against returning `Ok` for any of them.
    #[test]
    fn resolve_backfill_refuses_every_pairing_that_would_look_like_success() {
        let orders = recipe("orders", "orders");
        let err = |cdc: &ExportConfig, all: &[ExportConfig]| {
            resolve_backfill(cdc, all).expect_err("must refuse")
        };

        // A captured table with no export to read it.
        let auto = stream(&["orders", "payments"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders.clone(), auto.clone()];
        let e = err(&auto, &all);
        assert!(e.contains("payments"), "{e}");

        // Two exports read the same table: the pairing is a guess.
        let twin = recipe("orders_copy", "orders");
        let auto1 = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders.clone(), twin.clone(), auto1.clone()];
        let e = err(&auto1, &all);
        assert!(e.contains("ambiguous") && e.contains("orders_copy"), "{e}");

        // Named explicitly, two exports for ONE table: both would be claimed as
        // recipes (`orders_copy` silently stops running) and the leg takes the first.
        let listed_twice = stream(
            &["orders"],
            CdcBackfill::Exports(vec!["orders".into(), "orders_copy".into()]),
        );
        let all = vec![orders.clone(), twin.clone(), listed_twice.clone()];
        let e = err(&listed_twice, &all);
        assert!(e.contains("two exports for table 'orders'"), "{e}");

        // A `query:` export describes rows, not a relation.
        let mut q = recipe("q", "orders");
        q.table = None;
        q.query = Some("SELECT 1".into());
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["q".into()]));
        let all = vec![q.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(e.contains("no `table:`"), "{e}");

        // Another change stream is not a baseline.
        let other_cdc = {
            let mut c = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
            c.name = "other_cdc".into();
            c
        };
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["other_cdc".into()]));
        let all = vec![other_cdc.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(e.contains("mode: cdc"), "{e}");

        // An export reading a table this stream does not capture.
        let elsewhere = recipe("elsewhere", "payments");
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["elsewhere".into()]));
        let all = vec![orders.clone(), elsewhere.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(e.contains("does not capture"), "{e}");

        // Partial coverage: the uncovered table is the silent half.
        let history = recipe("hist", "ext_ref_id_history");
        let listed = stream(
            &["orders", "ext_ref_id_history"],
            CdcBackfill::Exports(vec!["orders".into()]),
        );
        let all = vec![orders.clone(), history.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(
            e.contains("ext_ref_id_history") && e.contains("1 of 2"),
            "{e}"
        );

        // A stream with nothing to capture cannot be paired with anything.
        let mut tableless = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        tableless.tables = None;
        tableless.table = None;
        let all = vec![orders.clone(), tableless.clone()];
        let e = err(&tableless, &all);
        assert!(e.contains("nothing to pair"), "{e}");

        // A recipe that reads a SLICE of the table is not a baseline — refused
        // here, before an anchor exists, not at plan build after it.
        let mut inc = recipe("orders_inc", "orders");
        inc.mode = ExportMode::Incremental;
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["orders_inc".into()]));
        let all = vec![inc.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(
            e.contains("whole table") && e.contains("Incremental"),
            "{e}"
        );
        // …and a time-window recipe is the same slice-read class.
        let mut win = recipe("orders_win", "orders");
        win.mode = ExportMode::TimeWindow;
        let listed = stream(&["orders"], CdcBackfill::Exports(vec!["orders_win".into()]));
        let all = vec![win.clone(), listed.clone()];
        let e = err(&listed, &all);
        assert!(e.contains("whole table") && e.contains("TimeWindow"), "{e}");

        // Two QUALIFIED names that differ are two relations, whatever their last
        // segment says: `sales.orders` is not `public.orders`. Pairing them bound
        // the wrong export's read strategy and types onto the captured table — and
        // then skipped that export from the run and the load, exit 0.
        let foreign = recipe("sales_orders", "sales.orders");
        let auto = stream(&["public.orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![foreign.clone(), auto.clone()];
        let e = err(&auto, &all);
        assert!(
            e.contains("public.orders") && e.contains("no export"),
            "{e}"
        );

        // A BARE name still pairs with a qualified one: `orders` is `public.orders`
        // to an operator who wrote one of them without the schema.
        let bare_recipe = recipe("orders", "orders");
        let all = vec![bare_recipe.clone(), auto.clone()];
        assert_eq!(
            resolve_backfill(&auto, &all)
                .expect("a bare name pairs with its qualified spelling")
                .len(),
            1
        );
        // …but only with the DEFAULT schema's: a bare `orders` reads `public.orders`,
        // so it is not the recipe of `sales.orders` — pairing them silently stopped
        // running the unrelated `orders` export.
        let sales = stream(&["sales.orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![bare_recipe.clone(), sales.clone()];
        let e = resolve_backfill(&sales, &all).expect_err("bare does not fold onto sales");
        assert!(e.contains("sales.orders") && e.contains("no export"), "{e}");
    }

    /// A schema-qualified capture (`dbo.orders` — what init writes on SQL Server
    /// and for a non-`public` PostgreSQL schema) must qualify the recipe's bare
    /// key by the BARE table name: the consumer, `types::overrides_for_unit`,
    /// narrows by `split_once('.')` on a `table.column` key, so `dbo.orders.price`
    /// splits as (`dbo`, `orders.price`) and never applies — the baseline leg
    /// (which gets the bare key) is typed by the recipe while the stream and the
    /// recorded spec are not: two types in one `__changes`. The oracle is the
    /// consumer's answer, not the map's key shape.
    #[test]
    fn effective_columns_reach_the_stream_for_a_schema_qualified_capture() {
        use std::collections::HashMap;
        let mut orders = recipe("orders", "dbo.orders");
        orders.columns = HashMap::from([("price".to_string(), "decimal(12,4)".to_string())]);
        let auto = stream(&["dbo.orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = vec![orders, auto.clone()];

        let merged = effective_columns(&auto, &all);
        let parsed = crate::plan::build::parse_column_overrides_pub(&merged, &auto.name).unwrap();
        let for_stream = crate::types::overrides_for_unit(&parsed, Some("dbo.orders"));
        assert!(
            for_stream.contains_key("price"),
            "the recipe's type must reach the stream's unit `dbo.orders`; merged keys: {:?}",
            merged.keys().collect::<Vec<_>>()
        );
    }

    /// One map for the leg, the stream and the spec: a recipe's bare keys arrive
    /// qualified to ITS table, the CDC export's own keys win, and a stream with no
    /// recipes is exactly its own `columns:`.
    #[test]
    fn effective_columns_qualifies_each_recipe_to_its_table_and_lets_the_stream_win() {
        use std::collections::HashMap;
        let mut orders = recipe("orders", "orders");
        orders.columns = HashMap::from([
            ("price".to_string(), "decimal(12,4)".to_string()),
            ("orders.note".to_string(), "string".to_string()),
        ]);
        let mut history = recipe("hist", "ext_ref_id_history");
        history.columns = HashMap::from([("price".to_string(), "decimal(8,2)".to_string())]);
        let mut auto = stream(
            &["orders", "ext_ref_id_history"],
            CdcBackfill::Auto(AutoWord::Auto),
        );
        auto.columns = HashMap::from([("flag".to_string(), "bool".to_string())]);
        let all = vec![orders.clone(), history.clone(), auto.clone()];

        let m = effective_columns(&auto, &all);
        assert_eq!(
            m.get("orders.price").map(String::as_str),
            Some("decimal(12,4)")
        );
        assert_eq!(
            m.get("ext_ref_id_history.price").map(String::as_str),
            Some("decimal(8,2)"),
            "each recipe's bare key is qualified to ITS table"
        );
        assert_eq!(
            m.get("orders.note").map(String::as_str),
            Some("string"),
            "an already-qualified recipe key is kept as written"
        );
        assert_eq!(m.get("flag").map(String::as_str), Some("bool"));
        assert!(
            !m.contains_key("price"),
            "no bare recipe key may bleed across tables: {m:?}"
        );

        // The stream's own declaration wins over a recipe's for the same column.
        auto.columns
            .insert("orders.price".to_string(), "decimal(14,6)".to_string());
        let all = vec![orders.clone(), history.clone(), auto.clone()];
        assert_eq!(
            effective_columns(&auto, &all)
                .get("orders.price")
                .map(String::as_str),
            Some("decimal(14,6)")
        );

        // No recipes: exactly the export's own map.
        let mut plain = auto.clone();
        plain.cdc = None;
        assert_eq!(effective_columns(&plain, &all), plain.columns);

        // A recipe naming BOTH `price` and `orders.price`: the qualified key wins
        // deterministically (a HashMap walk made it a coin toss per process).
        let mut twin = orders.clone();
        twin.columns = std::collections::HashMap::from([
            ("price".to_string(), "decimal(8,2)".to_string()),
            ("orders.price".to_string(), "decimal(12,4)".to_string()),
        ]);
        let mut auto_twin = auto.clone();
        auto_twin.columns.clear();
        let all = vec![twin, history.clone(), auto_twin.clone()];
        for _ in 0..20 {
            assert_eq!(
                effective_columns(&auto_twin, &all)
                    .get("orders.price")
                    .map(String::as_str),
                Some("decimal(12,4)")
            );
        }
    }

    /// One predicate for "a baseline on this engine needs a checkpoint": both ways to
    /// declare the baseline, every engine but PostgreSQL, and a present checkpoint
    /// satisfies it. It was two rules with two wordings, one per spelling.
    #[test]
    fn baseline_checkpoint_refusal_is_one_rule_for_both_baselines() {
        use crate::config::{CdcInitialMode, SourceType};
        let with = |initial: Option<CdcInitialMode>, backfill: Option<CdcBackfill>, ckpt: bool| {
            let mut e = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
            e.cdc = Some(CdcExportConfig {
                initial,
                backfill,
                checkpoint: ckpt.then(|| "/tmp/ck".to_string()),
                ..Default::default()
            });
            e
        };
        let snap = Some(CdcInitialMode::Snapshot);
        let auto = Some(CdcBackfill::Auto(AutoWord::Auto));

        for (e, engine) in [
            (with(snap, None, false), SourceType::Mysql),
            (with(None, auto.clone(), false), SourceType::Mysql),
            (with(snap, None, false), SourceType::Mssql),
            (with(None, auto.clone(), false), SourceType::Mongo),
        ] {
            let why = baseline_checkpoint_refusal(&e, engine).expect("must refuse");
            assert!(why.contains("cdc.checkpoint"), "{why}");
        }
        // PostgreSQL anchors in the slot; a present checkpoint satisfies any engine;
        // no baseline means nothing to anchor first.
        assert!(
            baseline_checkpoint_refusal(&with(snap, None, false), SourceType::Postgres).is_none()
        );
        assert!(
            baseline_checkpoint_refusal(&with(None, auto.clone(), false), SourceType::Postgres)
                .is_none()
        );
        assert!(baseline_checkpoint_refusal(&with(snap, None, true), SourceType::Mysql).is_none());
        assert!(baseline_checkpoint_refusal(&with(None, auto, true), SourceType::Mssql).is_none());
        assert!(baseline_checkpoint_refusal(&with(None, None, false), SourceType::Mysql).is_none());
    }

    /// The run loop asks this to avoid reading one table twice in one invocation.
    /// It must stay silent on a config that would NOT validate — it runs before
    /// validation has a chance to speak, and a panic here would replace a clear
    /// config error with a crash.
    #[test]
    fn backfill_recipe_names_lists_claimed_exports_and_tolerates_a_broken_config() {
        let orders = recipe("orders", "orders");
        let auto = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        let names = backfill_recipe_names(&[orders.clone(), auto.clone()]);
        assert_eq!(
            names.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["orders"]
        );

        // Unresolvable: no export for the captured table. Empty, not a panic.
        let broken = stream(&["payments"], CdcBackfill::Auto(AutoWord::Auto));
        assert!(backfill_recipe_names(&[orders, broken]).is_empty());
    }

    /// The whole-config loops decide through these two, not inline: `rivet run` /
    /// `apply` drop the recipes, `rivet plan` drops the recipes AND the streams.
    #[test]
    fn whole_config_loops_skip_recipes_and_plan_skips_streams_too() {
        let orders = recipe("orders", "orders");
        let plain = recipe("payments", "payments");
        let auto = stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto));
        let all = [orders.clone(), plain.clone(), auto.clone()];
        let recipes = backfill_recipe_names(&all);

        let kept: Vec<&str> = without_backfill_recipes(&all, &recipes)
            .iter()
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(
            kept,
            vec!["payments", "stand_cdc"],
            "run keeps the stream, drops its recipe"
        );

        assert!(batch_plan_skip_reason(&plain, &recipes).is_none());
        assert!(
            batch_plan_skip_reason(&orders, &recipes)
                .is_some_and(|w| w.contains("backfill recipe")),
            "the recipe is skipped for being a recipe"
        );
        assert!(
            batch_plan_skip_reason(&auto, &recipes).is_some_and(|w| w.contains("CDC export")),
            "the stream is skipped for being a stream"
        );
        // A CDC export that is (wrongly) also named as a recipe is still a stream first.
        let mut both = auto;
        both.name = "orders".into();
        assert!(batch_plan_skip_reason(&both, &recipes).is_some_and(|w| w.contains("CDC export")));
    }

    /// The three predicates the CDC job's live-only bodies decide through
    /// (extracted from inline operators the purity gate flagged, 2026-09-17).
    #[test]
    fn cdc_job_predicates_have_their_truth_tables() {
        let orders = recipe("orders", "orders");
        let all = [
            orders.clone(),
            stream(&["orders"], CdcBackfill::Auto(AutoWord::Auto)),
        ];
        let pairs = resolve_backfill(&all[1], &all).unwrap();
        assert_eq!(
            backfill_recipe_for(&pairs, "orders").map(|r| r.name.as_str()),
            Some("orders")
        );
        assert!(backfill_recipe_for(&pairs, "payments").is_none());
        assert!(backfill_recipe_for(&[], "orders").is_none());

        let mut cdc = CdcExportConfig::default();
        assert!(cdc.captures_changes_only(), "no initial, no backfill");
        cdc.initial = Some(CdcInitialMode::Snapshot);
        assert!(!cdc.captures_changes_only());
        cdc.initial = None;
        cdc.backfill = Some(CdcBackfill::Auto(AutoWord::Auto));
        assert!(!cdc.captures_changes_only());

        let mut cdc = CdcExportConfig::default();
        assert!(!cdc.runs_until_stopped(), "the default drain is bounded");
        cdc.until_current = false;
        assert!(cdc.runs_until_stopped());
    }
}
