//! Build a [`ResolvedRunPlan`](super::contract::ResolvedRunPlan) from config and CLI flags.

use std::collections::HashMap;
use std::path::Path;

use crate::config::{Config, ExportConfig, ExportMode};
use crate::error::Result;
use crate::tuning::{SourceTuning, TuningProfile, merge_tuning_config};

use super::contract::{
    ChunkedPlan, ExtractionStrategy, IncrementalCursorPlan, KeysetPlan, ResolvedRunPlan,
};

/// Build a [`ResolvedRunPlan`] from config and CLI flags.
///
/// This is the only place where raw `ExportConfig` fields and CLI flags
/// are read for execution decisions. After this call the pipeline operates
/// only on `ResolvedRunPlan`.
pub fn build_plan(
    config: &Config,
    export: &ExportConfig,
    config_dir: &Path,
    validate: bool,
    reconcile: bool,
    resume: bool,
    params: Option<&HashMap<String, String>>,
) -> Result<ResolvedRunPlan> {
    // CDC has no batch plan — bail BEFORE resolve_query. A `tables:`-style CDC
    // export carries no `query`, so resolve_query would fail first and hide this
    // friendly message behind a confusing "no query" error (bughunt MED: the
    // dogfood MED-5 message was unreachable for the tables: shape).
    if export.mode == ExportMode::Cdc {
        anyhow::bail!(
            "export '{}': cdc mode has no batch plan — CDC exports stream changes continuously and \
             are run with `rivet run` (or the `rivet cdc` subcommand), not `rivet plan` / `rivet \
             apply`. Plan the batch exports separately, or run this one with `rivet run`.",
            export.name
        );
    }
    let base_query = {
        let q = export.resolve_query(config_dir, params)?;
        // #167: a `--split` range sub-export restricts the whole export to its key
        // window `(lo, hi]`. Injected HERE (on the base query every runner wraps)
        // so the bound is runner-agnostic by construction — full/chunked/keyset
        // all page over the already-bounded set, the exact runner-bypass trap a
        // per-runner filter would reintroduce.
        match &export.split {
            Some(s) => crate::source::query::wrap_key_range(
                &q,
                &s.key_column,
                s.lo.as_deref(),
                s.hi.as_deref(),
                config.source.source_type,
            ),
            None => q,
        }
    };

    let merged = merge_tuning_config(config.source.tuning.as_ref(), export.tuning.as_ref());
    // When no `tuning.profile:` is set, fall back to the env-derived default —
    // `Local` shaves the throttle, `Production` keeps the safety contract.
    let fallback_profile = config
        .source
        .environment
        .map(|e| e.default_profile())
        .unwrap_or(TuningProfile::Balanced);
    let tuning = SourceTuning::from_config_with_default_profile(merged.as_ref(), fallback_profile);
    let profile_label = |p: TuningProfile| match p {
        TuningProfile::Fast => "fast",
        TuningProfile::Balanced => "balanced",
        TuningProfile::Safe => "safe",
    };
    let env_label = |e: crate::config::SourceEnvironment| match e {
        crate::config::SourceEnvironment::Local => "local",
        crate::config::SourceEnvironment::Replica => "replica",
        crate::config::SourceEnvironment::Production => "production",
    };
    let tuning_profile_label = match merged.as_ref().and_then(|t| t.profile) {
        Some(p) => profile_label(p).to_string(),
        None => match config.source.environment {
            Some(env) => format!(
                "{} (default for environment: {})",
                profile_label(fallback_profile),
                env_label(env)
            ),
            None => "balanced (default)".to_string(),
        },
    };

    let strategy = match export.mode {
        ExportMode::Full => full_strategy(config, export),
        ExportMode::Incremental => {
            let primary_column = export.cursor_column.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "export '{}': incremental mode requires 'cursor_column'",
                    export.name
                )
            })?;
            let fallback_column = export.cursor_fallback_column.clone();
            let mode = export.incremental_cursor_mode;
            ExtractionStrategy::Incremental(IncrementalCursorPlan {
                primary_column,
                fallback_column,
                mode,
            })
        }
        ExportMode::Chunked => resolve_chunked_strategy(config, export, &tuning)?,
        ExportMode::TimeWindow => {
            let column = export.time_column.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "export '{}': time_window mode requires 'time_column'",
                    export.name
                )
            })?;
            let days_window = export.days_window.ok_or_else(|| {
                anyhow::anyhow!(
                    "export '{}': time_window mode requires 'days_window'",
                    export.name
                )
            })?;
            ExtractionStrategy::TimeWindow {
                column,
                column_type: export.time_column_type,
                days_window,
            }
        }
        // Unreachable: cdc bails at build_plan entry, before resolve_query.
        ExportMode::Cdc => unreachable!("cdc mode is refused at build_plan entry"),
    };

    let (compression, compression_level) = export.effective_compression();
    Ok(ResolvedRunPlan {
        export_name: export.name.clone(),
        bytes_read: Default::default(),
        // The LABEL, where the two differ. `plan.source_table` has exactly one
        // consumer — the manifest's recorded identity in `finalize` — and the two
        // legs of one `initial: snapshot` export must record the SAME source or
        // `ensure_single_export` refuses the whole prefix at load time. Round-5
        // measured that refusal on SQL Server with a bare `table:`: the drain wrote
        // `mssql:orders` (the configured string) while the leg, whose `table` is now
        // the catalog read, wrote `mssql:dbo.orders`. Both identities contain a
        // colon, so the coarsening fold that rescues the batch leg deliberately does
        // not apply — and the break surfaces only at `rivet load`, on artifacts that
        // are already durable, blaming the operator's prefix layout for a naming
        // split inside rivet.
        source_table: export
            .snapshot_label
            .clone()
            .or_else(|| export.table.clone()),
        base_query,
        is_split_unit: export.split.is_some(),
        // Thread the split window into the plan so finalize records it in the manifest —
        // the durable anchor for exact-partition resume (SplitSynth → manifest::SplitWindow).
        split_window: export.split.as_ref().map(|s| crate::manifest::SplitWindow {
            key_column: s.key_column.clone(),
            lo: s.lo.clone(),
            hi: s.hi.clone(),
        }),
        strategy,
        format: export.format,
        compression,
        compression_level,
        max_file_size_bytes: export.max_file_size_bytes(),
        skip_empty: export.skip_empty,
        meta_columns: export.meta_columns.clone(),
        // #167: a `--split` sub-export named `daily#0` must resolve `{export}` /
        // `{table}` to its FAMILY (`daily`), not the unit name — so all N range
        // units write into ONE shared prefix and the load view recombines them as
        // one logical table (the merge-back). An ordinary export's family IS its
        // name, so this is a no-op off the split path.
        destination: expand_destination_templates(export.destination.clone(), &export.family()),
        quality: export.quality.clone(),
        tuning,
        tuning_profile_label,
        // ADR-0013: `--reconcile` *implies* `--validate` — it must produce a
        // verdict that subsumes everything `--validate` does (per-file row check
        // + manifest M5/M6) and adds the source comparison. The two flags are
        // gated independently downstream (`if plan.validate { … }` /
        // `if plan.reconcile { … }`), so the implication lives here at plan
        // build: a reconcile run is also a validate run.
        validate: validate || reconcile,
        reconcile,
        resume,
        verify: export.verify,
        source: config.source.clone(),
        // Batch exports have at most one table; narrow qualified override keys
        // ("table.column") to it — bare keys pass through unchanged. Query
        // exports carry no table (validation already rejects qualified keys).
        column_overrides: crate::types::overrides_for_unit(
            &parse_column_overrides(&export.columns, &export.name)?,
            export.table.as_deref(),
        ),
        schema_drift_policy: export.on_schema_drift,
        shape_drift_warn_factor: export.shape_drift_warn_factor.unwrap_or(2.0),
        parquet: export.parquet.clone(),
    })
}

/// The strategy for `mode: full`. Source-aware: a MongoDB source with
/// `source.mongo.page_size` reads by `_id`-keyset pages (bounded query time,
/// per-page parts, the base for parallel `_id`-range reads) instead of one
/// long-held cursor. Every SQL engine — and MongoDB without `page_size` — uses
/// the single-cursor snapshot.
fn full_strategy(config: &Config, export: &ExportConfig) -> ExtractionStrategy {
    if config.source.source_type == crate::config::SourceType::Mongo
        && let Some(mongo) = config.source.mongo.as_ref()
        && let Some(page) = mongo.page_size
    {
        return ExtractionStrategy::Keyset(KeysetPlan {
            key_column: "_id".to_string(),
            chunk_size: page.max(1),
            // Resume is opt-in: default keeps `mode: full` re-reading each run.
            checkpoint: mongo.resume,
            // Mongo `resume` has always meant "continue from the last _id each
            // run" (incremental-append on an append-only _id stream), so map it to
            // the incremental opt-in to preserve that behaviour under the
            // crash-recovery/incremental split.
            incremental: mongo.resume,
            // `parallel: N` fans N `_id`-range workers (Mongo reader only).
            parallel: export.parallel,
        });
    }
    // #dogfood LOW: `parallel: N` fans out ONLY the Mongo `_id`-range reader, which
    // needs `source.mongo.page_size` to page by `_id`. Reaching here on Mongo means
    // page_size is unset, so a mongo full export stays a single cursor and
    // `parallel: N` is silently dropped — warn instead of a silent no-op.
    if config.source.source_type == crate::config::SourceType::Mongo && export.parallel > 1 {
        log::warn!(
            "export '{}': parallel: {} has no effect on a Mongo full export without \
             `source.mongo.page_size` — parallelism fans out the `_id`-range reader, which pages \
             by `_id`. Set `source.mongo.page_size` to enable it, or remove `parallel`.",
            export.name,
            export.parallel
        );
    }
    ExtractionStrategy::Snapshot
}

/// Resolve the strategy for `mode: chunked`.
///
/// Encapsulates three plan-time decisions that all share the same Postgres
/// introspection round-trip:
///
/// 1. **chunk_column** — explicit `chunk_column:` wins; otherwise the `table:`
///    shortcut on a Postgres source triggers auto-detection of a single
///    integer-family PK via
///    [`crate::source::postgres::introspect_pg_table_for_chunking`].
/// 2. **chunk_size** — explicit `chunk_size:` wins; if `chunk_size_memory_mb:`
///    is set, derive the row count from `<budget_mb> / avg_row_bytes`,
///    clamped to `[10_000, 5_000_000]`. Falls back to the YAML-default
///    `chunk_size` (100k) when neither is helpful.
/// 3. **small-table escape** — if the planner's `reltuples` estimate is
///    below the resolved `chunk_size`, downgrade `Chunked → Snapshot` since
///    a single chunk would offer no benefit and adds ~10 ms of plumbing
///    overhead per export. The downgrade is informational (log::info!) and
///    safe (a `Snapshot` writes one file like `mode: full`). Skipped when an
///    explicit chunk-shape knob (`chunk_count`, `chunk_by_days`,
///    `chunk_checkpoint`, `chunk_by_key`) is set — explicit user intent wins
///    over the heuristic.
///
/// Steps 1–3 each only fire on PG with `table:` shortcut; everything else
/// falls back to the original "explicit column required" path.
/// Why a `mode: chunked` export with no `table:` cannot be planned — `None`
/// when it can.
///
/// PURE, and deliberately reachable from outside the planner. `rivet check`
/// resolved its own view of the config and printed `Verdict: ACCEPTABLE` for
/// two shapes `plan` refuses outright (`query:` + `chunk_size_memory_mb`,
/// `query:` + `chunk_by_key`) — a preflight blessing a config that cannot run
/// is the diagnostic-bypass class at its sharpest, because the operator reads
/// the verdict and stops looking. One function, both callers: the planner
/// bails on it, the preflight reports it (`src/preflight/mod.rs`).
///
/// Mirrors the fast path above: an explicit `chunk_column:` with no memory
/// budget plans fine without a relation, so that shape is `None` here too.
pub(crate) fn chunked_without_table_error(export: &ExportConfig) -> Option<String> {
    if export.table.is_some() {
        return None;
    }
    // The no-probe fast path: explicit column, no budget to divide.
    if export.chunk_column.is_some() && export.chunk_size_memory_mb.is_none() {
        return None;
    }
    if export.chunk_by_key.is_some() {
        return Some(format!(
            "export '{}': `chunk_by_key:` needs the `table:` shortcut so the planner can \
             verify the key is backed by a unique index (an unindexed ORDER BY key would \
             filesort the whole table). Set `table:` instead of `query:`.",
            export.name
        ));
    }
    if export.chunk_size_memory_mb.is_some() {
        return Some(format!(
            "export '{}': `chunk_size_memory_mb:` only applies with the `table:` shortcut — \
             set `table:` (preferred) or remove `chunk_size_memory_mb:` and keep an explicit `chunk_size:`",
            export.name
        ));
    }
    Some(format!(
        "export '{}': chunked mode requires 'chunk_column' \
         (auto-resolve from PK is only supported with the `table:` shortcut)",
        export.name
    ))
}

fn resolve_chunked_strategy(
    config: &Config,
    export: &ExportConfig,
    tuning: &SourceTuning,
) -> Result<ExtractionStrategy> {
    // chunk_count / chunk_dense / chunk_by_days mutual-exclusion is shared
    // between the introspected and non-introspected paths.
    if let Some(count) = export.chunk_count {
        if count == 0 {
            anyhow::bail!("export '{}': chunk_count must be >= 1 (got 0)", export.name);
        }
        if export.chunk_dense {
            anyhow::bail!(
                "export '{}': chunk_count and chunk_dense are mutually exclusive",
                export.name
            );
        }
        if export.chunk_by_days.is_some() {
            anyhow::bail!(
                "export '{}': chunk_count and chunk_by_days are mutually exclusive",
                export.name
            );
        }
    }

    // Keyset (OPT-4) is its own shape — incompatible with the range / dense /
    // date / count knobs. Validated up front so the conflict is reported
    // regardless of which downstream path runs.
    if export.chunk_by_key.is_some() {
        for (conflict, name) in [
            (export.chunk_column.is_some(), "chunk_column"),
            (export.chunk_dense, "chunk_dense"),
            (export.chunk_by_days.is_some(), "chunk_by_days"),
            (export.chunk_count.is_some(), "chunk_count"),
        ] {
            if conflict {
                anyhow::bail!(
                    "export '{}': chunk_by_key and {} are mutually exclusive",
                    export.name,
                    name
                );
            }
        }
    }

    let max_attempts = export
        .chunk_max_attempts
        .unwrap_or_else(|| tuning.max_retries.saturating_add(1).max(1));

    // Fast path: explicit column AND no memory-budget knob → no DB probe.
    // Preserves the no-network plan-build invariant for hand-tuned configs.
    //
    // Exception (#103): when the column is integer-`BETWEEN`-sliced (the default
    // range mode — NOT `chunk_dense`/ROW_NUMBER, NOT `chunk_by_days`/date
    // half-open, both type-safe) AND a `table:` is present to probe, fall through
    // to introspection so a NON-integer column is refused rather than silently
    // dropping every fractional row that falls between two window boundaries.
    // `query:`-only can't be probed (no relation), so it stays on the fast path
    // with a loud can't-verify warning.
    let range_sliced = !export.chunk_dense && export.chunk_by_days.is_none();
    let can_probe_type = range_sliced && export.table.is_some();
    if export.chunk_column.is_some() && export.chunk_size_memory_mb.is_none() && !can_probe_type {
        if range_sliced && export.table.is_none() {
            log::warn!(
                "export '{}': chunk_column '{}' is range-sliced by integer BETWEEN, but with \
                 `query:` (no `table:`) the planner cannot verify it is integer-family — a \
                 numeric/decimal/float key silently drops rows between windows. Use `table:` (so \
                 the type is checked), `chunk_by_key:` (keyset — any orderable indexed key), or \
                 ensure the column is integer.",
                export.name,
                export.chunk_column.as_deref().unwrap_or("")
            );
        }
        return Ok(ExtractionStrategy::Chunked(ChunkedPlan {
            column: export.chunk_column.clone().unwrap(),
            chunk_size: export.chunk_size,
            chunk_count: export.chunk_count,
            parallel: export.parallel,
            dense: export.chunk_dense,
            by_days: export.chunk_by_days,
            checkpoint: export.chunk_checkpoint,
            max_attempts,
        }));
    }

    // Anything beyond the fast path requires the `table:` shortcut so we have
    // a known relation to probe in either Postgres or MySQL. The rule lives in
    // `chunked_without_table_error` so the PREFLIGHT can ask the same question
    // — `rivet check` used to bless configs this bails on (see that function).
    let Some(tbl) = export.table.as_ref() else {
        anyhow::bail!(
            "{}",
            chunked_without_table_error(export)
                .expect("no `table:` in chunked mode past the fast path is always an error")
        );
    };

    let url = config.source.resolve_url().map_err(|e| {
        anyhow::anyhow!(
            "export '{}': chunked mode needs the source URL for the introspection probe: {e}",
            export.name
        )
    })?;
    let introspection_result = match config.source.source_type {
        crate::config::SourceType::Postgres => {
            crate::source::postgres::introspect_pg_table_for_chunking(
                &url,
                config.source.tls.as_ref(),
                tbl,
            )
        }
        crate::config::SourceType::Mysql => {
            crate::source::mysql::introspect_mysql_table_for_chunking(
                &url,
                config.source.tls.as_ref(),
                tbl,
            )
        }
        crate::config::SourceType::Mssql => {
            crate::source::mssql::introspect_mssql_table_for_chunking(
                &url,
                config.source.tls.as_ref(),
                tbl,
            )
        }
        crate::config::SourceType::Mongo => anyhow::bail!(
            "chunked mode is not supported for MongoDB — use `mode: full` (the whole \
             collection is read as `_id` + `document` JSON)"
        ),
    };
    let introspection = match introspection_result {
        Ok(i) => i,
        // #103-regression fix: the probe here exists only to TYPE-CHECK an explicit
        // `chunk_column` (and to size memory / auto-resolve the column). When the
        // column is explicit and no memory-sizing is requested, a probe FAILURE
        // (table in a non-default schema so `schema.table::regclass` throws, a
        // transient blip) must NOT newly break a config that worked before #103
        // routed it through introspection. Can't-verify → warn loudly and emit the
        // Chunked plan on the explicit column, exactly like the `query:`-only path.
        // Genuinely need the probe (auto-resolve column / `chunk_size_memory_mb:` /
        // `chunk_by_key:` index check) → the error stays fatal.
        Err(e)
            if export.chunk_column.is_some()
                && export.chunk_size_memory_mb.is_none()
                && export.chunk_by_key.is_none() =>
        {
            log::warn!(
                "export '{}': chunked-mode introspection probe failed ({e}) — cannot verify \
                 chunk_column '{}' is integer-family. Proceeding on the explicit column; if it is \
                 numeric/decimal/float, range chunking silently drops rows between integer windows. \
                 Qualify the table as `schema.table` so the probe can reach it, or use `chunk_by_key:`.",
                export.name,
                export.chunk_column.as_deref().unwrap_or("")
            );
            return Ok(ExtractionStrategy::Chunked(ChunkedPlan {
                column: export.chunk_column.clone().unwrap(),
                chunk_size: export.chunk_size,
                chunk_count: export.chunk_count,
                parallel: export.parallel,
                dense: export.chunk_dense,
                by_days: export.chunk_by_days,
                checkpoint: export.chunk_checkpoint,
                max_attempts,
            }));
        }
        Err(e) => {
            return Err(anyhow::anyhow!(
                "export '{}': chunked-mode introspection probe failed: {e}. \
                 Set `chunk_column:` (and `chunk_size:`) explicitly or check connectivity.",
                export.name
            ));
        }
    };

    chunked_strategy_from_introspection(
        config.source.source_type,
        export,
        tbl,
        max_attempts,
        &introspection,
    )
}

/// Decision half of [`resolve_chunked_strategy`], split at the introspection
/// seam so strategy selection is unit-testable with a synthetic
/// [`crate::source::TableIntrospection`]. The W5 mutation run found its guards
/// unguarded offline: the keyset-key refusal `!`, the ADR-0020 MySQL-only
/// auto-keyset gate, the small-table escape, and the memory->rows arithmetic.
/// Plan-time WIDTH warning: a fixed `chunk_size` on a wide table (`chunk_size ×
/// avg_row_bytes` ≥ 512 MB per chunk) risks a statement timeout (MySQL ERROR 3024)
/// or a memory spike — the field's wide `*_version` tables failed this way. Returns
/// the actionable warning line, or `None` when it does not apply: a memory budget
/// is set (already width-aware), no row-width estimate, or the chunk is not heavy.
/// Pure so the threshold + message are unit-tested without a live DB.
fn heavy_chunk_warning(
    name: &str,
    chunk_size: usize,
    avg_row_bytes: Option<i64>,
    memory_mb_set: bool,
) -> Option<String> {
    const HEAVY_CHUNK_BYTES: i64 = 512 * 1024 * 1024;
    if memory_mb_set {
        return None;
    }
    let row_bytes = avg_row_bytes.filter(|b| *b > 0)?;
    let bytes_per_chunk = (chunk_size as i64).saturating_mul(row_bytes);
    if bytes_per_chunk < HEAVY_CHUNK_BYTES {
        return None;
    }
    Some(format!(
        "export '{name}': chunk_size {chunk_size} × ~{row_bytes} B/row ≈ {} MB per chunk on a wide \
         table — a chunk this large can exceed the statement timeout (e.g. MySQL ERROR 3024) or \
         spike memory. Prefer `chunk_size_memory_mb:` (width-aware — each chunk stays bounded by \
         BYTES, not row count), or lower `chunk_size`.",
        bytes_per_chunk / (1024 * 1024),
    ))
}

/// Resolve a keyset plan's `(checkpoint, incremental)` recovery flags.
///
/// `chunk_checkpoint` = crash-recovery (a crashed run resumes from its last
/// committed key; a CLEAN re-run re-reads the whole range, never skipping).
/// `keyset_incremental` = append-only "continue from key on a clean re-run",
/// which needs the high-water key persisted — that IS the checkpoint machinery,
/// so it implies checkpoint (else it is a silent no-op: the cursor is never
/// stored and every clean re-run re-reads the whole table).
///
/// Parallel keyset (`parallel > 1`) supports BOTH since iteration 2/3: crash-
/// recovery (`chunk_checkpoint`, per-range from the persisted boundaries) and
/// incremental-by-key (`keyset_incremental` — a fresh run seeks past the persisted
/// anchor and advances it at success). Same formula as sequential — the runner
/// applies the incremental floor/ceiling across its N ranges.
fn keyset_recovery(export: &ExportConfig) -> (bool, bool) {
    (
        export.chunk_checkpoint || export.keyset_incremental,
        export.keyset_incremental,
    )
}

fn chunked_strategy_from_introspection(
    source_type: crate::config::SourceType,
    export: &ExportConfig,
    tbl: &str,
    max_attempts: u32,
    introspection: &crate::source::TableIntrospection,
) -> Result<ExtractionStrategy> {
    // (1) Resolve chunk_size — explicit overrides the budget; otherwise compute
    // from the memory budget and the planner's row-width estimate. Shared by the
    // range-chunked and keyset paths.
    let chunk_size = if let Some(mb) = export.chunk_size_memory_mb {
        let row_bytes = introspection.avg_row_bytes.filter(|b| *b > 0).unwrap_or_else(|| {
            // Empty / un-analyzed table — fall back to a defensive 512 B so the
            // computed chunk_size lands near the YAML default of 100k for a
            // typical narrow table. The user can still override.
            log::warn!(
                "export '{}': chunk_size_memory_mb set but {} has no pg_class stats yet (run ANALYZE?) — \
                 defaulting to 512 B/row for sizing",
                export.name,
                tbl
            );
            512
        });
        let computed = (mb as i64 * 1024 * 1024 / row_bytes).max(1);
        let clamped = computed.clamp(10_000, 5_000_000) as usize;
        log::info!(
            "export '{}': chunk_size_memory_mb={} MB ÷ {} B/row ≈ {} rows (clamped to {})",
            export.name,
            mb,
            row_bytes,
            computed,
            clamped
        );
        clamped
    } else {
        export.chunk_size
    };

    // (1b) Width warning: a FIXED `chunk_size` on a WIDE table makes each chunk
    // read `chunk_size × avg_row_bytes` of data — a heavy chunk that can exceed the
    // statement timeout (MySQL ERROR 3024 / PG statement_timeout) or spike memory.
    // The field's wide `*_version` tables failed exactly this way. Emit BEFORE the
    // run so the operator switches to `chunk_size_memory_mb:` rather than eating a
    // multi-minute timeout.
    if let Some(w) = heavy_chunk_warning(
        &export.name,
        chunk_size,
        introspection.avg_row_bytes,
        export.chunk_size_memory_mb.is_some(),
    ) {
        log::warn!("{w}");
    }

    // (2) Small-table escape: a single chunk/page has no benefit and adds
    // plumbing latency. Downgrade to Snapshot (bounded + simplest). Only fires
    // when reltuples is a meaningful positive number — un-analyzed tables
    // (reltuples=0) keep the chunked/keyset plan to preserve existing semantics.
    //
    // Explicit chunk knobs win over the heuristic: `chunk_count` recomputes
    // chunk_size at detect time from min/max (so comparing the estimate
    // against the static default chunk_size is meaningless), `chunk_by_days`
    // asks for date-ranged chunks, `chunk_checkpoint` asks for resumability a
    // Snapshot cannot provide, and `chunk_by_key` pins keyset pagination.
    // None of these may be silently collapsed to one snapshot file.
    let explicit_chunk_shape = export.chunk_count.is_some()
        || export.chunk_by_days.is_some()
        || export.chunk_checkpoint
        || export.chunk_by_key.is_some();
    if !explicit_chunk_shape
        && introspection.row_estimate > 0
        && (introspection.row_estimate as usize) <= chunk_size
    {
        log::info!(
            "export '{}': {} has ~{} rows ≤ chunk_size {}; downgrading chunked → snapshot",
            export.name,
            tbl,
            introspection.row_estimate,
            chunk_size,
        );
        return Ok(ExtractionStrategy::Snapshot);
    }

    // (3) Explicit keyset key (OPT-4): page by a single index-backed unique key.
    // Validate it is a usable keyset key (single-column, NOT NULL, UNIQUE/PK);
    // refuse otherwise rather than emit a full-scan + filesort `ORDER BY` query.
    if let Some(key) = export.chunk_by_key.as_deref() {
        if !introspection.is_usable_keyset_key(key) {
            anyhow::bail!(
                "export '{}': chunk_by_key '{}' is not a usable keyset key on {} — it must be a \
                 single-column, NOT NULL, UNIQUE or PRIMARY key WHOSE TYPE the keyset cursor can \
                 read (integer / float / string / timestamp / date / uuid). A `decimal`/`numeric` \
                 key is excluded: the cursor cannot advance past it (it would fail mid-run after a \
                 partial write). Without a usable key, `ORDER BY {} LIMIT n` would also full-scan + \
                 filesort. Add a unique index of a supported type, pick another key, use a range \
                 `chunk_column:` (integer), or `mode: full`.",
                export.name,
                key,
                tbl,
                key
            );
        }
        log::info!(
            "export '{}': keyset (seek) pagination on '{}' (chunk_size={})",
            export.name,
            key,
            chunk_size
        );
        let (checkpoint, incremental) = keyset_recovery(export);
        return Ok(ExtractionStrategy::Keyset(KeysetPlan {
            key_column: key.to_string(),
            chunk_size,
            checkpoint,
            incremental,
            // `parallel: N` fans N ROW-percentile-range keyset workers (feat/
            // parallel-keyset). The runner samples the boundaries at run open and
            // seeks each disjoint `(lo, hi]` range concurrently; iteration 1 has no
            // crash-recovery (a crashed parallel run re-reads from scratch), which
            // is why `keyset_recovery` forces checkpoint/incremental off above.
            parallel: export.parallel,
        }));
    }

    // (4) Resolve chunk_column for range chunking, with an auto-keyset fallback
    // on MySQL when there is no single-integer PK but a usable unique key exists.
    let column = if let Some(c) = export.chunk_column.clone() {
        // #103 (variant 1): an explicit chunk_column that is integer-`BETWEEN`-
        // sliced must be integer-family, or range chunking silently drops rows
        // between windows. `chunk_dense` (ROW_NUMBER) and `chunk_by_days` (date
        // half-open) are type-safe and took the fast path above (never reach here).
        let range_sliced = !export.chunk_dense && export.chunk_by_days.is_none();
        if range_sliced && !introspection.is_integer_column(&c) {
            anyhow::bail!(
                "export '{}': chunk_column '{}' on {} is not an integer-family column — range \
                 chunking derives integer min/max boundaries and slices with `BETWEEN`, so a \
                 numeric/decimal/float/text key silently drops every value between two window \
                 boundaries. Use `chunk_by_key: {}` (keyset — any orderable indexed key), an \
                 integer column, `chunk_dense: true`, or `mode: full`.",
                export.name,
                c,
                tbl,
                c
            );
        }
        c
    } else {
        match introspection.single_int_pk.clone() {
            Some(col) => {
                // `info!` was below the default `warn` log level — operators
                // never saw which column the planner had picked, so a config
                // that intended `mode: chunked` + `chunk_column: created_at`
                // but typo'd the column name silently fell back to PK with
                // no signal. Elevate to `warn!` so the implicit choice is
                // visible in every run; tell the operator how to silence it.
                log::warn!(
                    "export '{}': chunk_column not set — auto-resolved to '{}' \
                     from the single-integer primary key on {}. \
                     Set `chunk_column:` explicitly to pin the choice and silence this warning.",
                    export.name,
                    col,
                    tbl
                );
                col
            }
            None => {
                // MySQL has no server-side cursor, so a non-int-PK table has no
                // safe range-chunk shape. If a single-column unique key exists,
                // page it with keyset instead of refusing (OPT-4). PG keeps
                // refusing — its `DECLARE CURSOR` snapshot is already bounded, so
                // `mode: full` is the safe answer there.
                //
                // ADR-0020 (Layer 1, deferred) deliberately keeps this
                // MySQL-only: flipping the guard is a behaviour-changing default
                // for every existing PG/MSSQL UUID-PK config, and is to be
                // promoted "after a real operator request, not on the strength
                // of 'we technically can'." The escape hatch is explicit
                // `chunk_by_key:` (which works since Layer 2). Do not flip
                // without re-opening ADR-0020.
                if source_type == crate::config::SourceType::Mysql
                    && let Some(key) = introspection.auto_keyset_key()
                {
                    log::warn!(
                        "export '{}': {} has no single-integer PK — auto-selected keyset \
                         (seek) pagination on unique key '{}'. Set `chunk_by_key:` explicitly \
                         to pin the choice and silence this warning.",
                        export.name,
                        tbl,
                        key
                    );
                    let (checkpoint, incremental) = keyset_recovery(export);
                    return Ok(ExtractionStrategy::Keyset(KeysetPlan {
                        key_column: key.to_string(),
                        chunk_size,
                        checkpoint,
                        incremental,
                        parallel: export.parallel,
                    }));
                }
                anyhow::bail!(
                    "export '{}': chunked mode found no safe shape on {} — no single-integer PK \
                     to range-chunk and no single-column UNIQUE/PRIMARY key to keyset-page. \
                     Set `chunk_column:` (integer) or `chunk_by_key:` (unique key) explicitly, \
                     add a unique index, or use `mode: full` to accept one long snapshot query.",
                    export.name,
                    tbl
                );
            }
        }
    };

    Ok(ExtractionStrategy::Chunked(ChunkedPlan {
        column,
        chunk_size,
        chunk_count: export.chunk_count,
        parallel: export.parallel,
        dense: export.chunk_dense,
        by_days: export.chunk_by_days,
        checkpoint: export.chunk_checkpoint,
        max_attempts,
    }))
}

/// Public re-export for callers outside `plan` (e.g. `preflight::type_report`).
pub fn parse_column_overrides_pub(
    raw: &std::collections::HashMap<String, String>,
    export_name: &str,
) -> Result<crate::types::ColumnOverrides> {
    parse_column_overrides(raw, export_name)
}

/// Parse the raw `columns:` map from `ExportConfig` into typed [`ColumnOverrides`].
///
/// Fails early (at plan-build time) with an actionable error so the user
/// fixes their `rivet.yaml` before the export runs.
fn parse_column_overrides(
    raw: &std::collections::HashMap<String, String>,
    export_name: &str,
) -> Result<crate::types::ColumnOverrides> {
    raw.iter()
        .map(|(col, type_str)| {
            crate::types::parse_type_str(type_str)
                .map(|t| (col.clone(), t))
                .map_err(|e| {
                    anyhow::anyhow!(
                        "export '{}': column override for '{}': {}",
                        export_name,
                        col,
                        e
                    )
                })
        })
        .collect()
}

/// Substitute placeholders in `destination.path` and `destination.prefix`.
///
/// Thin wrapper around [`crate::destination::placeholder::expand_destination`]
/// anchored at today's UTC date.  Kept here so existing call sites
/// (`run`, the in-process plan builder) keep their stable signature; callers
/// that need to re-target an earlier run's prefix (e.g. `validate --date`,
/// `validate --run-id`) should construct a `PlaceholderContext` directly
/// and call [`crate::destination::placeholder::expand_destination`].
pub(crate) fn expand_destination_templates(
    dest: crate::config::DestinationConfig,
    export_name: &str,
) -> crate::config::DestinationConfig {
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(export_name);
    crate::destination::placeholder::expand_destination(dest, &ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, IncrementalCursorMode, SourceConfig,
        SourceType, TimeColumnType,
    };

    fn minimal_source_config() -> SourceConfig {
        SourceConfig {
            source_type: SourceType::Postgres,
            url: Some("postgresql://localhost/test".into()),
            url_env: None,
            url_file: None,
            host: None,
            port: None,
            user: None,
            password: None,
            password_env: None,
            database: None,
            environment: None,
            tuning: None,
            tls: None,
            mongo: None,
        }
    }

    fn minimal_config() -> Config {
        Config {
            source: minimal_source_config(),
            exports: vec![],
            notifications: None,
            parallel_exports: false,
            parallel_export_processes: false,
            load: None,
        }
    }

    fn minimal_export() -> ExportConfig {
        // Baseline from the canonical test fixture; override only what these
        // plan-build tests care about (zstd compression, ./out destination).
        ExportConfig {
            compression: CompressionType::Zstd,
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".into()),
                ..Default::default()
            },
            ..crate::config::sample_export("test_export")
        }
    }

    /// The rule `rivet check` and `rivet plan` now share, in every direction.
    ///
    /// RED-proven the expensive way: the live audit
    /// (`check_must_not_accept_a_config_the_planner_refuses`) failed in CI on
    /// two of these shapes while `check` still had its own opinion. The unit
    /// test exists so the next regression is caught before a live stack is
    /// needed — and so a mutant that flips one arm has something to bite.
    #[test]
    fn chunked_without_table_names_the_reason_and_spares_the_fast_path() {
        let base = || ExportConfig {
            mode: crate::config::ExportMode::Chunked,
            table: None,
            query: Some("SELECT * FROM t".into()),
            ..minimal_export()
        };

        // The fast path: an explicit column with no budget plans without a
        // relation, so the preflight must NOT refuse it.
        let mut ok = base();
        ok.chunk_column = Some("id".into());
        assert_eq!(chunked_without_table_error(&ok), None);

        // …and a `table:` is always fine, whatever else is set.
        let mut with_table = base();
        with_table.table = Some("public.t".into());
        with_table.query = None;
        with_table.chunk_by_key = Some("id".into());
        assert_eq!(chunked_without_table_error(&with_table), None);

        // The three refusals, each naming its own knob rather than a generic
        // "bad config" — the operator has to know WHICH line to change.
        let mut by_key = base();
        by_key.chunk_by_key = Some("id".into());
        let m = chunked_without_table_error(&by_key).expect("chunk_by_key needs a relation");
        assert!(m.contains("chunk_by_key"), "{m}");

        let mut budget = base();
        budget.chunk_column = Some("id".into());
        budget.chunk_size_memory_mb = Some(64);
        let m = chunked_without_table_error(&budget).expect("a budget needs a row width");
        assert!(m.contains("chunk_size_memory_mb"), "{m}");

        let bare = base();
        let m = chunked_without_table_error(&bare).expect("no column, no table, no plan");
        assert!(m.contains("chunk_column"), "{m}");
    }

    #[test]
    fn snapshot_plan_from_full_mode() {
        let export = minimal_export();
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        assert!(matches!(plan.strategy, ExtractionStrategy::Snapshot));
        assert_eq!(plan.strategy.mode_label(), "full");
        assert_eq!(plan.export_name, "test_export");
        assert_eq!(plan.base_query, "SELECT 1");
        assert!(!plan.validate);
        assert!(!plan.reconcile);
        assert!(!plan.resume);
    }

    /// #167 Slice C: a `--split` sub-export's plan restricts `base_query` to its
    /// key window `(lo, hi]`, injection-safe, on the base every runner wraps. RED
    /// against a build_plan that ignores `export.split` (base_query stays the raw
    /// query, so every range unit scans the WHOLE table — N× duplication, the
    /// split's whole point defeated).
    #[test]
    fn split_subexport_plan_bounds_the_base_query_to_its_key_window() {
        // Interior window (both bounds): WHERE id > 1000 AND id <= 2000.
        let mut mid = minimal_export();
        mid.name = "daily#1".into();
        mid.query = Some("SELECT * FROM t".into());
        mid.split = Some(crate::config::SplitSynth {
            parent: "daily".into(),
            key_column: "id".into(),
            lo: Some("1000".into()),
            hi: Some("2000".into()),
        });
        let plan = build_plan(
            &minimal_config(),
            &mid,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        assert!(
            plan.base_query.contains("SELECT * FROM t")
                && plan.base_query.contains("> E'1000'")
                && plan.base_query.contains("<= E'2000'"),
            "interior window must bound both sides (PG literal form): {}",
            plan.base_query
        );

        // First window (no floor): only the upper bound.
        let mut first = mid.clone();
        first.name = "daily#0".into();
        first.split = Some(crate::config::SplitSynth {
            parent: "daily".into(),
            key_column: "id".into(),
            lo: None,
            hi: Some("1000".into()),
        });
        let plan0 = build_plan(
            &minimal_config(),
            &first,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        assert!(
            plan0.base_query.contains("<= E'1000'") && !plan0.base_query.contains(" > "),
            "first window must have no floor: {}",
            plan0.base_query
        );
    }

    #[test]
    fn incremental_plan_resolves_cursor_column() {
        let mut export = minimal_export();
        export.mode = ExportMode::Incremental;
        export.cursor_column = Some("updated_at".into());
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::Incremental(p) => {
                assert_eq!(p.primary_column, "updated_at");
                assert_eq!(p.mode, IncrementalCursorMode::SingleColumn);
            }
            _ => panic!("expected Incremental"),
        }
        assert_eq!(plan.strategy.mode_label(), "incremental");
    }

    #[test]
    fn chunked_plan_resolves_all_fields() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        export.chunk_size = 50_000;
        export.parallel = 4;
        export.chunk_checkpoint = true;
        export.chunk_max_attempts = Some(5);
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            true,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => {
                assert_eq!(cp.column, "id");
                assert_eq!(cp.chunk_size, 50_000);
                assert_eq!(cp.parallel, 4);
                assert!(cp.checkpoint);
                assert_eq!(cp.max_attempts, 5);
            }
            _ => panic!("expected Chunked"),
        }
        assert_eq!(plan.strategy.mode_label(), "chunked");
        assert!(plan.validate);
    }

    #[test]
    fn chunked_max_attempts_defaults_from_tuning() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => assert_eq!(cp.max_attempts, 4),
            _ => panic!("expected Chunked"),
        }
    }

    // ── keyset (OPT-4) planner gates (no DB) ──────────────────────────────

    #[test]
    fn chunk_by_key_conflicts_with_chunk_column() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_by_key = Some("uuid".into());
        export.chunk_column = Some("id".into());
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(msg.contains("mutually exclusive"), "got: {msg}");
    }

    #[test]
    fn chunk_by_key_without_table_requires_table_shortcut() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_by_key = Some("uuid".into());
        export.table = None;
        export.chunk_column = None;
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("table:") && msg.contains("chunk_by_key"),
            "should require the table shortcut to verify the index, got: {msg}"
        );
    }

    #[test]
    fn cdc_export_plan_gives_a_friendly_message_not_an_internal_invariant() {
        // #dogfood MED: `rivet plan` on a cdc-mode export leaked "internal routing
        // error — dispatch should branch on mode first". The bail is now
        // user-facing (points at `rivet run`), reached before any DB probe.
        let mut export = minimal_export();
        export.mode = ExportMode::Cdc;
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("no batch plan") && msg.contains("rivet run"),
            "must point the user at `rivet run`, got: {msg}"
        );
        assert!(
            !msg.contains("internal routing error"),
            "must not leak the internal invariant, got: {msg}"
        );
    }

    // ── auto-resolve chunk_column friendly errors (no DB) ─────────────────

    #[test]
    fn chunked_without_column_or_table_returns_explicit_error() {
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = None;
        export.table = None; // query: form, no auto-resolve possible
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("chunked mode requires 'chunk_column'")
                && msg.contains("`table:` shortcut"),
            "expected actionable error about table: shortcut; got: {msg}"
        );
    }

    #[test]
    fn chunked_explicit_column_skips_auto_resolve_and_no_db_call() {
        // Sanity: an explicit chunk_column with `query:` (no `table:`) short-
        // circuits without opening a connection. (#103: `table:` + a range-sliced
        // column now DOES probe the type, so the no-network fast path is
        // query-only / dense / by_days from here on.)
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("explicit_pk".into());
        export.table = None;
        export.query = Some("SELECT explicit_pk, v FROM something".into());
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .expect("explicit chunk_column must short-circuit auto-resolve");
        match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => assert_eq!(cp.column, "explicit_pk"),
            _ => panic!("expected Chunked"),
        }
    }

    #[test]
    fn explicit_non_integer_chunk_column_is_refused_not_silently_range_sliced() {
        // #103 (variant 1): an explicit chunk_column that is range-BETWEEN-sliced
        // must be integer-family — a numeric/decimal/float key silently drops
        // rows between integer windows. The planner refuses it and points at
        // chunk_by_key (keyset works on any orderable indexed key).
        use crate::config::SourceType;
        // introspection: `id` is integer-family, `amount` is not.
        let i = intro(Some("id"), &["id"], 1_000_000, Some(100), &["id"]);

        let mut bad = chunked_export();
        bad.chunk_column = Some("amount".into());
        let err =
            chunked_strategy_from_introspection(SourceType::Postgres, &bad, "public.t", 3, &i)
                .unwrap_err();
        assert!(
            err.to_string().contains("not an integer-family column"),
            "a non-integer chunk_column must be refused, not silently range-sliced: {err}"
        );

        // An integer explicit chunk_column resolves to Chunked as before.
        let mut good = chunked_export();
        good.chunk_column = Some("id".into());
        assert!(
            matches!(
                chunked_strategy_from_introspection(SourceType::Postgres, &good, "public.t", 3, &i),
                Ok(ExtractionStrategy::Chunked(_))
            ),
            "an integer chunk_column must still resolve"
        );
    }

    #[test]
    fn chunked_size_memory_mb_without_table_errors_with_hint() {
        // `chunk_size_memory_mb:` needs `table:` for the row-size probe.
        // Without it we should bail with a message that tells the user how
        // to fix the config, instead of silently falling back to chunk_size.
        let mut export = minimal_export();
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        export.chunk_size_memory_mb = Some(256);
        // No table: shortcut.
        export.table = None;
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("chunk_size_memory_mb") && msg.contains("`table:` shortcut"),
            "expected actionable error mentioning chunk_size_memory_mb + table:; got: {msg}"
        );
    }

    #[test]
    fn time_window_plan_resolves_column_and_days() {
        let mut export = minimal_export();
        export.mode = ExportMode::TimeWindow;
        export.time_column = Some("created_at".into());
        export.time_column_type = TimeColumnType::Unix;
        export.days_window = Some(30);
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::TimeWindow {
                column,
                column_type,
                days_window,
            } => {
                assert_eq!(column, "created_at");
                assert_eq!(*column_type, TimeColumnType::Unix);
                assert_eq!(*days_window, 30);
            }
            _ => panic!("expected TimeWindow"),
        }
        assert_eq!(plan.strategy.mode_label(), "timewindow");
    }

    #[test]
    fn plan_carries_cli_flags() {
        let export = minimal_export();
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            true,
            true,
            true,
            None,
        )
        .unwrap();
        assert!(plan.validate);
        assert!(plan.reconcile);
        assert!(plan.resume);
    }

    #[test]
    fn plan_resolves_tuning_profile_label() {
        let export = minimal_export();
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        assert_eq!(plan.tuning_profile_label, "balanced (default)");
    }

    #[test]
    fn expand_destination_templates_substitutes_all_placeholders() {
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let dest = DestinationConfig {
            destination_type: DestinationType::Local,
            prefix: Some("exports/{date}/{export}/".into()),
            path: Some("/data/{table}/{date}".into()),
            ..Default::default()
        };
        let expanded = expand_destination_templates(dest, "orders");
        assert_eq!(
            expanded.path.as_deref(),
            Some(format!("/data/orders/{today}").as_str())
        );
        assert_eq!(
            expanded.prefix.as_deref(),
            Some(format!("exports/{today}/orders/").as_str())
        );
    }

    // ── chunk_count validation ────────────────────────────────────────────────

    fn chunked_export() -> ExportConfig {
        let mut e = minimal_export();
        e.mode = ExportMode::Chunked;
        e.chunk_column = Some("id".into());
        e
    }

    // ── mutation-W5 gap closure ──────────────────────────────────────────────
    // The strategy-selection guards had no offline test (the probe needed a
    // live DB until the introspection seam split). Each case pins a decision
    // whose mutant survived W5.
    fn intro(
        single_int_pk: Option<&str>,
        keyset_keys: &[&str],
        row_estimate: i64,
        avg_row_bytes: Option<i64>,
        int_columns: &[&str],
    ) -> crate::source::TableIntrospection {
        crate::source::TableIntrospection {
            single_int_pk: single_int_pk.map(str::to_string),
            keyset_keys: keyset_keys.iter().map(|s| s.to_string()).collect(),
            row_estimate,
            avg_row_bytes,
            int_columns: int_columns.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn chunked_strategy_decision_gates() {
        use crate::config::SourceType;
        let resolve =
            |source_type, export: &ExportConfig, i: &crate::source::TableIntrospection| {
                chunked_strategy_from_introspection(source_type, export, "public.t", 3, i)
            };
        let base = || {
            let mut e = chunked_export();
            e.chunk_column = None; // let the resolver decide
            e
        };

        // Small-table escape: rows <= chunk_size downgrades to Snapshot —
        // INCLUSIVE boundary (row_estimate == chunk_size still snapshots).
        let e = base();
        let i = intro(Some("id"), &["id"], e.chunk_size as i64, Some(100), &["id"]);
        assert!(matches!(
            resolve(SourceType::Postgres, &e, &i).unwrap(),
            ExtractionStrategy::Snapshot
        ));
        // …but any explicit chunk-shape knob must NOT be collapsed silently.
        let mut e = base();
        e.chunk_checkpoint = true;
        assert!(
            !matches!(
                resolve(SourceType::Postgres, &e, &i).unwrap(),
                ExtractionStrategy::Snapshot
            ),
            "chunk_checkpoint asks for resumability a snapshot cannot provide"
        );

        // Explicit chunk_by_key: a usable key keysets; an unknown key REFUSES
        // (deleting the `!` accepts a full-scan+filesort key).
        let mut e = base();
        e.chunk_by_key = Some("uid".into());
        let i = intro(None, &["uid"], 1_000_000, Some(100), &[]);
        match resolve(SourceType::Postgres, &e, &i).unwrap() {
            ExtractionStrategy::Keyset(k) => assert_eq!(k.key_column, "uid"),
            other => panic!("usable chunk_by_key must keyset, got {other:?}"),
        }
        let mut e = base();
        e.chunk_by_key = Some("nope".into());
        let err =
            resolve(SourceType::Postgres, &e, &i).expect_err("unusable chunk_by_key must refuse");
        assert!(err.to_string().contains("not a usable keyset key"));

        // Memory-budget chunk_size: 100 MB at 1024 B/row = 102_400 rows — the
        // exact value pins the *,/ arithmetic; a 0-byte estimate must fall back
        // to the defensive 512 B (100 MB / 512 = 204_800), never divide by 0.
        let mut e = base();
        e.chunk_size_memory_mb = Some(100);
        let i = intro(Some("id"), &["id"], 10_000_000, Some(1024), &["id"]);
        match resolve(SourceType::Postgres, &e, &i).unwrap() {
            ExtractionStrategy::Chunked(p) => assert_eq!(p.chunk_size, 102_400),
            other => panic!("expected chunked, got {other:?}"),
        }
        let i = intro(Some("id"), &["id"], 10_000_000, Some(0), &["id"]);
        match resolve(SourceType::Postgres, &e, &i).unwrap() {
            ExtractionStrategy::Chunked(p) => assert_eq!(p.chunk_size, 204_800),
            other => panic!("expected chunked, got {other:?}"),
        }

        // ADR-0020: auto-keyset on a no-int-PK table is MYSQL-ONLY. The same
        // introspection on Postgres must refuse with the no-safe-shape error —
        // an `==` -> `!=` mutant flips the gate for every engine.
        let e = base();
        let i = intro(None, &["uuid_col"], 1_000_000, Some(100), &[]);
        match resolve(SourceType::Mysql, &e, &i).unwrap() {
            ExtractionStrategy::Keyset(k) => assert_eq!(k.key_column, "uuid_col"),
            other => panic!("mysql auto-keyset expected, got {other:?}"),
        }
        let err =
            resolve(SourceType::Postgres, &e, &i).expect_err("PG must not auto-keyset (ADR-0020)");
        assert!(err.to_string().contains("no safe shape"));
    }

    #[test]
    fn keyset_checkpoint_and_incremental_are_independent_plan_flags() {
        // The crash-recovery ⇄ incremental split: `chunk_checkpoint` sets ONLY
        // KeysetPlan.checkpoint (safe crash-recovery); the append-only
        // continue-on-clean-re-run is the separate `keyset_incremental` →
        // KeysetPlan.incremental. Before the split, checkpoint implied incremental
        // and this test could not distinguish them.
        use crate::config::SourceType;
        let resolve = |export: &ExportConfig, i: &crate::source::TableIntrospection| {
            chunked_strategy_from_introspection(SourceType::Postgres, export, "public.t", 3, i)
        };
        let i = intro(Some("id"), &["uid"], 1_000_000, Some(100), &["id"]);

        // checkpoint on, incremental unset → crash-recovery only, NOT incremental.
        let mut e = chunked_export();
        e.chunk_column = None;
        e.chunk_by_key = Some("uid".into());
        e.chunk_checkpoint = true;
        e.keyset_incremental = false;
        match resolve(&e, &i).unwrap() {
            ExtractionStrategy::Keyset(k) => {
                assert!(k.checkpoint, "chunk_checkpoint must set checkpoint");
                assert!(
                    !k.incremental,
                    "chunk_checkpoint alone must NOT imply incremental (the safety fix)"
                );
            }
            other => panic!("expected keyset, got {other:?}"),
        }

        // incremental opt-in → both flags set.
        e.keyset_incremental = true;
        match resolve(&e, &i).unwrap() {
            ExtractionStrategy::Keyset(k) => {
                assert!(k.checkpoint && k.incremental, "opt-in must set both");
            }
            other => panic!("expected keyset, got {other:?}"),
        }

        // keyset_incremental WITHOUT chunk_checkpoint must still set checkpoint —
        // incremental needs the persisted cursor, so it implies checkpoint. Before
        // the fix this was KeysetPlan{checkpoint:false, incremental:true}, a silent
        // no-op that re-read the whole table every run.
        e.chunk_checkpoint = false;
        e.keyset_incremental = true;
        match resolve(&e, &i).unwrap() {
            ExtractionStrategy::Keyset(k) => {
                assert!(
                    k.checkpoint && k.incremental,
                    "keyset_incremental must imply checkpoint (else it is a silent no-op): {k:?}"
                );
            }
            other => panic!("expected keyset, got {other:?}"),
        }
    }

    #[test]
    fn chunk_count_zero_is_rejected() {
        let mut export = chunked_export();
        export.chunk_count = Some(0);
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("chunk_count") && err.to_string().contains("1"),
            "expected 'chunk_count must be >= 1', got: {err}"
        );
    }

    #[test]
    fn chunk_count_with_chunk_dense_is_rejected() {
        let mut export = chunked_export();
        export.chunk_count = Some(10);
        export.chunk_dense = true;
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("mutually exclusive"),
            "expected 'mutually exclusive', got: {err}"
        );
    }

    #[test]
    fn chunk_count_with_chunk_by_days_is_rejected() {
        let mut export = chunked_export();
        export.chunk_count = Some(10);
        export.chunk_by_days = Some(7);
        let err = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("mutually exclusive"),
            "expected 'mutually exclusive', got: {err}"
        );
    }

    #[test]
    fn chunk_count_valid_threads_through_to_plan() {
        let mut export = chunked_export();
        export.chunk_count = Some(5);
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => assert_eq!(cp.chunk_count, Some(5)),
            _ => panic!("expected Chunked"),
        }
    }

    #[test]
    fn chunk_count_none_is_accepted_with_dense() {
        let mut export = chunked_export();
        export.chunk_count = None;
        export.chunk_dense = true;
        let plan = build_plan(
            &minimal_config(),
            &export,
            Path::new("."),
            false,
            false,
            false,
            None,
        )
        .unwrap();
        match &plan.strategy {
            ExtractionStrategy::Chunked(cp) => {
                assert!(cp.dense);
                assert!(cp.chunk_count.is_none());
            }
            _ => panic!("expected Chunked"),
        }
    }

    #[test]
    fn heavy_chunk_warning_fires_only_on_a_wide_fixed_size_chunk() {
        // Narrow table (512 B/row × 100k = ~49 MB/chunk) → no warning.
        assert!(heavy_chunk_warning("t", 100_000, Some(512), false).is_none());
        // Wide table (8 KB/row × 100k = ~763 MB/chunk ≥ 512 MB) → warn, actionably.
        let w = heavy_chunk_warning("t", 100_000, Some(8192), false)
            .expect("a wide fixed-size chunk must warn");
        assert!(w.contains("chunk_size_memory_mb"), "must name the fix: {w}");
        assert!(
            w.contains("ERROR 3024"),
            "must name the field failure mode: {w}"
        );
        // A memory budget is already width-aware → never warn, even if wide.
        assert!(heavy_chunk_warning("t", 100_000, Some(8192), true).is_none());
        // No catalog width estimate → can't judge → no warning.
        assert!(heavy_chunk_warning("t", 100_000, None, false).is_none());
        assert!(heavy_chunk_warning("t", 100_000, Some(0), false).is_none());
        // A large chunk_size makes even a moderate row width heavy.
        assert!(heavy_chunk_warning("t", 2_000_000, Some(300), false).is_some());
    }

    #[test]
    fn full_strategy_mongo_parallel_without_page_size_is_snapshot_and_warns() {
        // #dogfood: `parallel: N` on a Mongo full export fans out the `_id`-range
        // reader, which needs `source.mongo.page_size`. WITHOUT it the strategy is
        // Snapshot (a single cursor) — parallel is a no-op — and rivet WARNS rather
        // than silently dropping the flag. WITH page_size it engages the Keyset
        // (`_id`-range parallel) runner instead.
        let base = "source: { type: mongo, url: \"mongodb://127.0.0.1:27017/db\"MONGO }\n\
                    exports:\n  - name: m\n    table: coll\n    mode: full\n    parallel: 4\n    \
                    format: parquet\n    destination: { type: local, path: ./o }\n";
        let cfg_no_page =
            crate::config::Config::from_yaml(&base.replace("MONGO", "")).expect("parses");
        assert!(
            matches!(
                full_strategy(&cfg_no_page, &cfg_no_page.exports[0]),
                ExtractionStrategy::Snapshot
            ),
            "mongo full + parallel WITHOUT page_size must be Snapshot (parallel is a no-op)"
        );
        let cfg_page =
            crate::config::Config::from_yaml(&base.replace("MONGO", ", mongo: { page_size: 500 }"))
                .expect("parses");
        assert!(
            matches!(
                full_strategy(&cfg_page, &cfg_page.exports[0]),
                ExtractionStrategy::Keyset(_)
            ),
            "mongo full + parallel WITH page_size must engage the _id-range Keyset runner"
        );
    }

    #[test]
    fn expand_destination_templates_no_placeholders_unchanged() {
        let dest = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("./out".into()),
            ..Default::default()
        };
        let expanded = expand_destination_templates(dest, "orders");
        assert_eq!(expanded.path.as_deref(), Some("./out"));
        assert!(expanded.prefix.is_none());
    }

    #[test]
    fn expand_destination_templates_is_idempotent_on_already_expanded_strings() {
        // Regression guard for the validate/doctor wiring (2026-05-21):
        // both commands now call `expand_destination_templates` on the same
        // config `run` already expanded for.  Calling twice must NOT mangle
        // a string that has no remaining placeholders.
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let once = expand_destination_templates(
            DestinationConfig {
                destination_type: DestinationType::Local,
                prefix: Some("runs/{date}/{export}/".into()),
                ..Default::default()
            },
            "orders",
        );
        let twice = expand_destination_templates(once.clone(), "orders");
        assert_eq!(once.prefix, twice.prefix);
        assert_eq!(
            once.prefix.as_deref(),
            Some(format!("runs/{today}/orders/").as_str())
        );
    }
}
