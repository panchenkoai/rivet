//! One export's effective load settings from the config alone: mode, layout, partition, delete flag.

use crate::config::load::{LoadSection, PartitionSpec};

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

/// The warehouse layout of one export: a `backfill:` stream keeps a physical base
/// and a disposable change buffer; everything else is the changelog + view.
/// An export mode's load strategy. Exhaustive (no `_`) on purpose: a future
/// delta-style mode fails to COMPILE here until someone picks its load
/// semantics, instead of silently defaulting to OVERWRITE (the
/// incremental-overwrite data-loss class).
pub fn load_mode_of(
    config: &crate::config::Config,
    export: &crate::config::ExportConfig,
) -> LoadMode {
    use crate::config::ExportMode;
    match export.mode {
        ExportMode::Cdc => LoadMode::Cdc,
        ExportMode::Incremental => LoadMode::Incremental,
        // only the keys past the last run's: a delta, never the whole table
        ExportMode::Full | ExportMode::Chunked
            if crate::plan::build::continued_key(config, export).is_some() =>
        {
            LoadMode::Incremental
        }
        ExportMode::Full => LoadMode::Full,    // whole result set
        ExportMode::Chunked => LoadMode::Full, // parallel full snapshot
        ExportMode::TimeWindow => LoadMode::Full, // the current window, whole
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
        .unwrap_or(matches!(load_mode_of(config, export), LoadMode::Cdc))
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
    let compacts = eff.as_ref().is_some_and(|l| l.target.compacts());
    cdc_layout(export, load_mode_of(config, export), choice, compacts)
}

pub(crate) fn cdc_layout(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_str_names_each_mode_stably() {
        // The state DB's `load_run.mode` discriminator: rows written by every earlier
        // release are read back by `overwritten_delta_warning` (a drifted "full" would
        // silence the upgrade warning) and by `rivet state loads`.
        assert_eq!(LoadMode::Full.ledger_str(), "full");
        assert_eq!(LoadMode::Incremental.ledger_str(), "incremental");
        assert_eq!(LoadMode::Cdc.ledger_str(), "cdc");
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

    /// Every export mode, each with and without `keyset_incremental`: only a run that
    /// continues past its last key is a delta, and a delta never loads as an overwrite.
    #[test]
    fn load_mode_of_every_mode_with_and_without_keyset_incremental() {
        use crate::config::ExportMode;
        let modes = [
            ExportMode::Full,
            ExportMode::Incremental,
            ExportMode::Chunked,
            ExportMode::TimeWindow,
            ExportMode::Cdc,
        ];
        // Exhaustive on purpose: a new mode stops compiling here until it joins the table.
        let expected = |mode: ExportMode, keyset_incremental: bool| match (mode, keyset_incremental)
        {
            (ExportMode::Cdc, _) => LoadMode::Cdc,
            (ExportMode::Incremental, _) => LoadMode::Incremental,
            (ExportMode::Chunked, true) => LoadMode::Incremental,
            (ExportMode::Chunked, false) => LoadMode::Full,
            (ExportMode::Full | ExportMode::TimeWindow, _) => LoadMode::Full,
        };
        let cfg = crate::config::Config::from_yaml(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    table: t\n    format: parquet\n    destination:\n      type: local\n      path: ./o\n",
        )
        .unwrap();
        for mode in modes {
            for keyset_incremental in [false, true] {
                let mut e = crate::config::sample_export("t");
                e.mode = mode;
                e.keyset_incremental = keyset_incremental;
                e.chunk_by_key = Some("id".into());
                assert_eq!(
                    load_mode_of(&cfg, &e),
                    expected(mode, keyset_incremental),
                    "{mode:?} keyset_incremental={keyset_incremental}"
                );
            }
        }
    }
}
