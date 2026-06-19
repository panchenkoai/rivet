use super::HealthVerdict;
use crate::config::{ExportConfig, ExportMode};

/// B1: Human-readable strategy name derived from mode + config.
pub(crate) fn derive_strategy(export: &ExportConfig) -> String {
    match export.mode {
        ExportMode::Full => {
            if export.parallel > 1 {
                format!("full-parallel({})", export.parallel)
            } else {
                "full-scan".to_string()
            }
        }
        ExportMode::Incremental => {
            let col = export.cursor_column.as_deref().unwrap_or("?");
            format!("incremental({})", col)
        }
        ExportMode::Chunked => {
            let col = export.chunk_column.as_deref().unwrap_or("?");
            if let Some(days) = export.chunk_by_days {
                if export.parallel > 1 {
                    format!(
                        "date-chunked-parallel({}, {}d, p={})",
                        col, days, export.parallel
                    )
                } else {
                    format!("date-chunked({}, {}d)", col, days)
                }
            } else if export.parallel > 1 {
                format!(
                    "chunked-parallel({}, size={}, p={})",
                    col, export.chunk_size, export.parallel
                )
            } else {
                format!("chunked({}, size={})", col, export.chunk_size)
            }
        }
        ExportMode::TimeWindow => {
            let col = export.time_column.as_deref().unwrap_or("?");
            let days = export.days_window.unwrap_or(0);
            format!("time-window({}, {}d)", col, days)
        }
    }
}

/// The operator-facing mode line shown in the `rivet check` diagnostic —
/// `"chunked (column: id, size: 100000)"`, `"incremental (cursor: updated_at)"`,
/// etc. Shared verbatim by all three engine `diagnose_*` paths, which differ
/// only in their probes, not in this label.
pub(crate) fn diagnose_mode_str(export: &ExportConfig) -> String {
    match export.mode {
        ExportMode::Full => "full".to_string(),
        ExportMode::Incremental => format!(
            "incremental (cursor: {})",
            export.cursor_column.as_deref().unwrap_or("?")
        ),
        ExportMode::Chunked => format!(
            "chunked (column: {}, size: {})",
            export.chunk_column.as_deref().unwrap_or("?"),
            export.chunk_size
        ),
        ExportMode::TimeWindow => format!(
            "time_window (column: {}, days: {})",
            export.time_column.as_deref().unwrap_or("?"),
            export.days_window.unwrap_or(0)
        ),
    }
}

/// Resolve the base query the runner will issue, for the preflight probes.
///
/// For the `table:` shortcut (no `query:`) this is the canonical
/// `SELECT * FROM <table>` (`ExportConfig::resolve_query`, which validates and
/// quotes the ident) — NOT a `SELECT 1` placeholder, or every probe (row
/// estimate, scan type, cursor range) would describe a 1-row dummy relation
/// instead of the real table. `config_dir`/`params` are unused on the
/// `table:`/inline branches; preflight is non-fatal, so a resolution failure
/// falls back to the inline/placeholder text and surfaces the cause at debug
/// rather than aborting the diagnostic. Shared verbatim by all three engines.
pub(crate) fn resolve_preflight_base_query(export: &ExportConfig) -> String {
    match export.resolve_query(std::path::Path::new(""), None) {
        Ok(q) => q,
        Err(e) => {
            log::debug!(
                "preflight: base-query resolution failed for export '{}': {e}",
                export.name
            );
            export
                .query
                .clone()
                .unwrap_or_else(|| "SELECT 1".to_string())
        }
    }
}

/// B2: Recommend tuning profile based on row estimate and index usage.
pub(crate) fn recommend_profile(
    row_estimate: Option<i64>,
    uses_index: bool,
    export: &ExportConfig,
) -> &'static str {
    let rows = row_estimate.unwrap_or(0);
    match (uses_index, rows) {
        (true, r) if r <= 1_000_000 => "fast",
        (true, r) if r <= 10_000_000 => "balanced",
        (true, _) => "safe",
        (false, r) if r <= 100_000 => {
            if export.parallel > 1 {
                "safe"
            } else {
                "balanced"
            }
        }
        (false, r) if r <= 1_000_000 => "balanced",
        (false, _) => "safe",
    }
}

/// Key-range sparsity for chunked `BETWEEN` windows.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChunkSparsityInfo {
    pub is_sparse: bool,
    pub density: f64,
    pub range_span: i64,
    pub logical_windows: i64,
    pub row_count: i64,
}

pub(crate) fn chunk_sparsity_from_counts(
    row_count: i64,
    min_i: i64,
    max_i: i64,
    chunk_size: usize,
) -> ChunkSparsityInfo {
    let range_span = (max_i - min_i).max(1);
    let density = row_count as f64 / range_span as f64;
    let logical_windows = if chunk_size == 0 {
        0
    } else {
        (range_span + chunk_size as i64 - 1) / chunk_size as i64
    };
    let is_sparse = row_count > 0 && density < 0.1 && logical_windows > 10;
    ChunkSparsityInfo {
        is_sparse,
        density,
        range_span,
        logical_windows,
        row_count,
    }
}

/// B3: Detect sparse range risk for chunked mode.
pub(crate) fn check_sparse_range(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    cursor_min: Option<&str>,
    cursor_max: Option<&str>,
) -> Option<String> {
    if export.mode != ExportMode::Chunked {
        return None;
    }
    if export.chunk_dense {
        return None;
    }
    if export.chunk_by_days.is_some() {
        // Date chunks iterate by calendar interval; sparsity concept does not apply.
        return None;
    }
    let rows = row_estimate.unwrap_or(0);
    if rows == 0 {
        return None;
    }

    let (min_val, max_val) = match (cursor_min, cursor_max) {
        (Some(a), Some(b)) => (a, b),
        _ => return None,
    };

    let min_i: i64 = min_val.parse().ok()?;
    let max_i: i64 = max_val.parse().ok()?;
    let info = chunk_sparsity_from_counts(rows, min_i, max_i, export.chunk_size);
    if !info.is_sparse {
        return None;
    }

    let empty_pct = ((1.0 - info.density).clamp(0.0, 1.0) * 100.0) as u32;
    Some(format!(
        "Sparse key range: ~{}% of chunk windows will be empty (range {}..{}, ~{} rows). \
         Consider chunking on a dense surrogate (ROW_NUMBER) or switching to incremental mode.",
        empty_pct, min_val, max_val, rows
    ))
}

/// A single chunk query scanning more than this holds one statement — and its
/// MVCC snapshot / locks — open long enough to be the source-harm lever the
/// 0.12 harm A/B measured (MSSQL longest request 1839 ms → 276 ms once chunks
/// shrank). Advisory only: it flags multi-second chunk statements on wide
/// tables, never blocks them.
const MAX_CHUNK_SCAN_BYTES: i64 = 256 * 1024 * 1024;

/// B3b (dual of [`check_sparse_range`]): warn when `chunk_size` makes a single
/// chunk query scan *too many* bytes — `rows_per_chunk × avg_row_bytes`.
///
/// `rows_per_chunk` is `chunk_size` exactly for a dense (ROW_NUMBER) chunk, and
/// `density × chunk_size` for a range chunk (`density = rows / key-span`, via
/// [`chunk_sparsity_from_counts`]). Date chunks (`chunk_by_days`) iterate by
/// calendar interval, so rows-per-chunk is data-shaped and not derivable here —
/// skipped. Needs both a row estimate and a row-width estimate; a `None` for
/// either (e.g. MySQL, which has no trustworthy scan-free estimate) skips it.
pub(crate) fn check_oversized_chunk(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    avg_row_bytes: Option<i64>,
    cursor_min: Option<&str>,
    cursor_max: Option<&str>,
) -> Option<String> {
    if export.mode != ExportMode::Chunked || export.chunk_by_days.is_some() {
        return None;
    }
    let rows = row_estimate?;
    let bytes_per_row = avg_row_bytes?;
    if rows <= 0 || bytes_per_row <= 0 || export.chunk_size == 0 {
        return None;
    }

    let rows_per_chunk: i64 = if export.chunk_dense {
        // Dense ordinal windows hold exactly chunk_size rows (bar the last).
        export.chunk_size as i64
    } else {
        let min_i: i64 = cursor_min?.parse().ok()?;
        let max_i: i64 = cursor_max?.parse().ok()?;
        let info = chunk_sparsity_from_counts(rows, min_i, max_i, export.chunk_size);
        ((info.density * export.chunk_size as f64).round() as i64).max(1)
    };

    let bytes_per_chunk = rows_per_chunk.saturating_mul(bytes_per_row);
    if bytes_per_chunk <= MAX_CHUNK_SCAN_BYTES {
        return None;
    }

    // Bytes scale linearly with chunk_size (dense: chunk_size×B; range:
    // density×chunk_size×B), so scaling chunk_size by budget/actual lands one
    // chunk under the budget regardless of mode.
    let suggested = (export.chunk_size as i64)
        .saturating_mul(MAX_CHUNK_SCAN_BYTES)
        .saturating_div(bytes_per_chunk)
        .max(1);
    let mb = |b: i64| b / (1024 * 1024);
    Some(format!(
        "Heavy chunk: chunk_size={} makes each chunk query scan ~{} MB (~{} rows × ~{} B/row), \
         holding one statement — and its snapshot/locks — open that long (the source-harm a \
         smaller chunk avoids). Consider chunk_size around {} to keep each chunk under ~{} MB.",
        export.chunk_size,
        mb(bytes_per_chunk),
        rows_per_chunk,
        bytes_per_row,
        suggested,
        mb(MAX_CHUNK_SCAN_BYTES),
    ))
}

/// B4: Warn about dense surrogate sort cost when query uses ROW_NUMBER.
pub(crate) fn check_dense_surrogate_cost(export: &ExportConfig) -> Option<String> {
    let query = export.query.as_deref().unwrap_or("");
    let q_upper = query.to_uppercase();
    if export.mode == ExportMode::Chunked && (q_upper.contains("ROW_NUMBER") || export.chunk_dense)
    {
        Some(
            "Dense surrogate (ROW_NUMBER) requires a global sort -- this adds CPU and I/O cost \
             proportional to the full result set. For very large or hot tables, consider \
             incremental mode on an indexed cursor column, or a precomputed dense key."
                .to_string(),
        )
    } else {
        None
    }
}

/// Connections reserved for non-worker use (monitoring, admin, failover).
const CONNECTION_HEADROOM: u32 = 3;

/// Warn when configured parallelism meets or exceeds the DB's max_connections limit.
pub(crate) fn check_connection_limit(
    parallel: usize,
    db_max_connections: Option<u32>,
) -> Option<String> {
    if parallel <= 1 {
        return None;
    }
    match db_max_connections {
        None => Some(
            "Could not fetch DB max_connections — connection limit check skipped. \
             Ask your DBA to verify your user has access to DB server variables, \
             then verify your parallel setting manually."
                .to_string(),
        ),
        Some(max_conn) if parallel as u32 >= max_conn => Some(format!(
            "parallel={parallel} meets or exceeds DB max_connections={max_conn} — \
             workers will compete for connections and some may fail. \
             Reduce parallel to at most {} (leave headroom for other connections).",
            max_conn.saturating_sub(CONNECTION_HEADROOM).max(1),
        )),
        _ => None,
    }
}

/// B5: Warn about parallel memory risk.
pub(crate) fn check_parallel_memory_risk(
    export: &ExportConfig,
    row_estimate: Option<i64>,
) -> Option<String> {
    if export.parallel <= 1 {
        return None;
    }
    let rows = row_estimate.unwrap_or(0);
    if rows > 5_000_000 {
        Some(format!(
            "Parallel={} on ~{}M rows: each worker buffers batch_size rows in memory. \
             With wide rows this can cause high RSS. Monitor with memory_threshold_mb \
             or reduce parallel/batch_size.",
            export.parallel,
            rows / 1_000_000,
        ))
    } else {
        None
    }
}

/// L6: Recommend parallelism level.
pub(crate) fn recommend_parallelism(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    uses_index: bool,
) -> (u32, &'static str) {
    if export.mode != ExportMode::Chunked {
        return (1, "only chunked mode benefits from parallelism");
    }

    let rows = row_estimate.unwrap_or(0);

    if rows < 50_000 {
        return (1, "dataset too small to benefit from parallelism");
    }

    if !uses_index && rows > 5_000_000 {
        return (1, "no index — parallel scans would multiply source load");
    }

    if !uses_index {
        return (
            2,
            "no index — conservative parallelism to limit source impact",
        );
    }

    match rows {
        r if r < 500_000 => (2, "moderate dataset — 2 workers sufficient"),
        r if r < 5_000_000 => (4, "large dataset with index support"),
        _ => (
            4,
            "very large dataset — cap at 4 to control memory; increase with memory_threshold_mb monitoring",
        ),
    }
}

/// Collect all warnings (B3-B6) for an export.
pub(super) fn collect_warnings(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    avg_row_bytes: Option<i64>,
    chunk_min: Option<&str>,
    chunk_max: Option<&str>,
    db_max_connections: Option<u32>,
) -> Vec<String> {
    // A flat manifest of the preflight checks, in display order: each returns
    // `Some(warning)` when it applies, `None` otherwise. Adding the next check is
    // a one-line array insert, not a new if-let rung — the same provider shape
    // `RunSummary::render` uses for its rows. The checks keep their own
    // (individually unit-tested) signatures; only the assembly is centralised here.
    [
        check_connection_limit(export.parallel, db_max_connections),
        check_sparse_range(export, row_estimate, chunk_min, chunk_max),
        check_oversized_chunk(export, row_estimate, avg_row_bytes, chunk_min, chunk_max),
        check_dense_surrogate_cost(export),
        check_parallel_memory_risk(export, row_estimate),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Tables at or below this row count are a one-shot full copy, not a workload
/// worth a "DEGRADED" warning. Mirrors `init::TableInfo::suggest_mode`, which
/// recommends plain `mode: full` (no index/cursor) up to the same threshold —
/// so the scaffold init writes must not then be scolded by `check`.
pub(crate) const SMALL_TABLE_ROW_THRESHOLD: i64 = 100_000;

pub(crate) fn compute_verdict(
    row_estimate: Option<i64>,
    uses_index: bool,
    has_cursor: bool,
) -> HealthVerdict {
    let rows = row_estimate.unwrap_or(0);

    match (uses_index, has_cursor, rows) {
        (true, true, r) if r <= 10_000_000 => HealthVerdict::Efficient,
        (true, true, _) => HealthVerdict::Acceptable,
        // Indexed + no cursor of any size stays Acceptable. The cursor
        // requirement is for incremental mode; chunked mode (the
        // canonical large-table path) *is* the cursor and doesn't need
        // one declared. Without this arm a 10_000_001-row indexed
        // chunked export falls through to `_ => Degraded`, which the
        // operator reads as "your config is wrong" — but it's fine.
        (true, false, _) => HealthVerdict::Acceptable,
        // A KNOWN-small table (real estimate below the threshold) full-scanning
        // is exactly what `rivet init` recommends — not a problem. Keyed on a
        // real estimate (`Some`), so an UNKNOWN size still falls through to
        // Degraded and gets the index/incremental nudge.
        (false, _, r) if row_estimate.is_some() && r < SMALL_TABLE_ROW_THRESHOLD => {
            HealthVerdict::Acceptable
        }
        (false, _, r) if r <= 1_000_000 => HealthVerdict::Degraded,
        (false, true, r) if r <= 50_000_000 => HealthVerdict::Degraded,
        (false, _, _) => HealthVerdict::Unsafe,
    }
}

pub(crate) fn build_suggestion(
    verdict: &HealthVerdict,
    row_estimate: Option<i64>,
    uses_index: bool,
    export: &ExportConfig,
) -> Option<String> {
    let rows = row_estimate.unwrap_or(0);

    match verdict {
        HealthVerdict::Efficient => None,
        HealthVerdict::Acceptable => {
            if rows > 10_000_000 {
                let mut msg = format!("Large dataset (~{}M rows).", rows / 1_000_000);
                match export.mode {
                    ExportMode::Full => {
                        msg.push_str(" Switch to incremental mode with an indexed cursor column to avoid re-reading unchanged rows.");
                    }
                    ExportMode::Chunked if export.parallel <= 1 => {
                        msg.push_str(" Add parallel > 1 to speed up chunked extraction.");
                    }
                    _ => {
                        msg.push_str(" Use 'safe' tuning profile to limit database impact.");
                    }
                }
                Some(msg)
            } else {
                None
            }
        }
        HealthVerdict::Degraded => {
            let mut parts = Vec::new();
            if !uses_index {
                parts.push("No index detected -- full table scan.".to_string());
            }
            match export.mode {
                ExportMode::Full if export.cursor_column.is_none() => {
                    parts.push(
                        "Add an indexed cursor column and switch to incremental mode.".to_string(),
                    );
                }
                // Only suggest creating an index when we believe there isn't
                // one — `uses_index` already accounts for both the EXPLAIN
                // hint and the catalog-side probe (preflight::{postgres,
                // mysql}::column_has_*_*), so saying "create an index" when
                // a btree already exists would be a false alarm.
                ExportMode::Chunked if !uses_index => {
                    let col = export.chunk_column.as_deref().unwrap_or("chunk_column");
                    parts.push(format!(
                        "Create an index on '{}' to speed up range scans.",
                        col
                    ));
                }
                ExportMode::TimeWindow if !uses_index => {
                    let col = export.time_column.as_deref().unwrap_or("time_column");
                    parts.push(format!(
                        "Create an index on '{}' for efficient time-window filtering.",
                        col
                    ));
                }
                _ => {
                    if export.cursor_column.is_none() && !matches!(export.mode, ExportMode::Chunked)
                    {
                        parts.push(
                            "Consider adding a cursor column for incremental mode.".to_string(),
                        );
                    }
                }
            }
            parts.push("Use 'safe' tuning profile to limit database impact.".to_string());
            Some(parts.join(" "))
        }
        HealthVerdict::Unsafe => {
            let mut parts = vec![format!(
                "~{}M row scan without index support.",
                rows / 1_000_000
            )];
            match export.mode {
                ExportMode::Full => {
                    parts.push("Add an indexed cursor column and use incremental mode to avoid full re-reads.".to_string());
                }
                ExportMode::Chunked => {
                    let col = export.chunk_column.as_deref().unwrap_or("chunk_column");
                    parts.push(format!(
                        "Create an index on '{}'. Consider reducing chunk_size or adding parallel workers.",
                        col
                    ));
                }
                ExportMode::TimeWindow => {
                    let col = export.time_column.as_deref().unwrap_or("time_column");
                    parts.push(format!(
                        "Create an index on '{}'. Reduce days_window if possible.",
                        col
                    ));
                }
                ExportMode::Incremental => {
                    let col = export.cursor_column.as_deref().unwrap_or("cursor_column");
                    parts.push(format!("Create an index on '{}'.", col));
                }
            }
            parts.push("Use 'safe' tuning profile. Extract during off-peak hours.".to_string());
            Some(parts.join(" "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ExportConfig;

    fn cfg(extra_yaml: &str) -> ExportConfig {
        let yaml = format!(
            "name: test\nformat: parquet\ndestination:\n  type: local\n  path: /tmp\n{extra_yaml}"
        );
        serde_yaml_ng::from_str(&yaml).expect("parse ExportConfig")
    }

    // ── derive_strategy ─────────────────────────────────────────────────────

    #[test]
    fn derive_strategy_full_scan() {
        assert_eq!(derive_strategy(&cfg("mode: full\n")), "full-scan");
    }

    #[test]
    fn derive_strategy_full_parallel() {
        let e = cfg("mode: full\nparallel: 4\n");
        assert_eq!(derive_strategy(&e), "full-parallel(4)");
    }

    #[test]
    fn derive_strategy_incremental() {
        let e = cfg("mode: incremental\ncursor_column: updated_at\n");
        assert_eq!(derive_strategy(&e), "incremental(updated_at)");
    }

    #[test]
    fn derive_strategy_chunked_by_size() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 50000\n");
        assert_eq!(derive_strategy(&e), "chunked(id, size=50000)");
    }

    #[test]
    fn derive_strategy_date_chunked() {
        let e = cfg("mode: chunked\nchunk_column: created_at\nchunk_by_days: 7\n");
        assert_eq!(derive_strategy(&e), "date-chunked(created_at, 7d)");
    }

    // ── diagnose_mode_str / resolve_preflight_base_query (hoisted from the
    //    three engine diagnose_* paths; pin the shared shape once) ────────────

    #[test]
    fn diagnose_mode_str_renders_each_mode() {
        assert_eq!(diagnose_mode_str(&cfg("mode: full\n")), "full");
        assert_eq!(
            diagnose_mode_str(&cfg("mode: incremental\ncursor_column: updated_at\n")),
            "incremental (cursor: updated_at)"
        );
        assert_eq!(
            diagnose_mode_str(&cfg(
                "mode: chunked\nchunk_column: id\nchunk_size: 100000\n"
            )),
            "chunked (column: id, size: 100000)"
        );
    }

    #[test]
    fn resolve_preflight_base_query_table_shortcut_and_inline() {
        // `table:` shortcut → canonical SELECT * FROM <table>, never a SELECT 1 stub.
        assert_eq!(
            resolve_preflight_base_query(&cfg("mode: full\ntable: orders\n")),
            "SELECT * FROM orders"
        );
        // Inline `query:` passes through verbatim.
        assert_eq!(
            resolve_preflight_base_query(&cfg("mode: full\nquery: \"SELECT id FROM t WHERE x\"\n")),
            "SELECT id FROM t WHERE x"
        );
    }

    // ── recommend_profile ───────────────────────────────────────────────────

    #[test]
    fn recommend_profile_indexed_small_is_fast() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(500_000), true, &e), "fast");
    }

    #[test]
    fn recommend_profile_indexed_medium_is_balanced() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(5_000_000), true, &e), "balanced");
    }

    #[test]
    fn recommend_profile_indexed_large_is_safe() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(15_000_000), true, &e), "safe");
    }

    #[test]
    fn recommend_profile_no_index_small_is_balanced() {
        let e = cfg(""); // parallel=1 by default
        assert_eq!(recommend_profile(Some(50_000), false, &e), "balanced");
    }

    #[test]
    fn recommend_profile_no_index_large_is_safe() {
        let e = cfg("");
        assert_eq!(recommend_profile(Some(5_000_000), false, &e), "safe");
    }

    // ── chunk_sparsity_from_counts ──────────────────────────────────────────

    #[test]
    fn chunk_sparsity_dense_range_not_sparse() {
        // 10_000 rows in a 10_001 span (density ≈ 1.0) → not sparse
        let info = chunk_sparsity_from_counts(10_000, 0, 10_000, 100);
        assert!(!info.is_sparse);
        assert!(info.density > 0.9);
    }

    #[test]
    fn chunk_sparsity_very_sparse_range_is_sparse() {
        // 100 rows in a 1_000_000 span with 100_000 chunk_size → >10 windows, density < 0.1
        let info = chunk_sparsity_from_counts(100, 0, 1_000_000, 50_000);
        assert!(
            info.is_sparse,
            "expected sparse: density={:.6}",
            info.density
        );
        assert!(info.density < 0.1);
        assert!(info.logical_windows > 10);
    }

    #[test]
    fn chunk_sparsity_zero_rows_never_sparse() {
        let info = chunk_sparsity_from_counts(0, 0, 1_000_000, 100);
        assert!(!info.is_sparse);
    }

    // ── check_sparse_range ──────────────────────────────────────────────────

    #[test]
    fn check_sparse_range_non_chunked_mode_returns_none() {
        let e = cfg("mode: full\n");
        assert!(check_sparse_range(&e, Some(100), Some("1"), Some("1000000")).is_none());
    }

    #[test]
    fn check_sparse_range_chunk_dense_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_dense: true\n");
        assert!(check_sparse_range(&e, Some(100), Some("1"), Some("1000000")).is_none());
    }

    #[test]
    fn check_sparse_range_chunk_by_days_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: created_at\nchunk_by_days: 7\n");
        assert!(check_sparse_range(&e, Some(100), Some("1"), Some("1000000")).is_none());
    }

    #[test]
    fn check_sparse_range_sparse_returns_warning() {
        // 100 rows in a 5_000_000 span with chunk_size=100_000 → 50 windows > 10, density≈2e-5 < 0.1
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 100000\n");
        let w = check_sparse_range(&e, Some(100), Some("1"), Some("5000000"));
        assert!(w.is_some(), "expected sparse warning");
        let msg = w.unwrap();
        assert!(msg.contains("Sparse"), "got: {msg}");
    }

    #[test]
    fn check_sparse_range_dense_range_returns_none() {
        // 10_000 rows in 10_001 span → not sparse
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 100\n");
        assert!(check_sparse_range(&e, Some(10_000), Some("0"), Some("10000")).is_none());
    }

    // ── check_oversized_chunk ───────────────────────────────────────────────

    #[test]
    fn check_oversized_chunk_non_chunked_returns_none() {
        let e = cfg("mode: full\n");
        assert!(
            check_oversized_chunk(&e, Some(10_000_000), Some(500), Some("0"), Some("9999999"))
                .is_none()
        );
    }

    #[test]
    fn check_oversized_chunk_by_days_returns_none() {
        // Date chunks iterate by calendar interval — rows/chunk not derivable.
        let e = cfg("mode: chunked\nchunk_column: created_at\nchunk_by_days: 7\n");
        assert!(
            check_oversized_chunk(&e, Some(10_000_000), Some(500), Some("0"), Some("9999999"))
                .is_none()
        );
    }

    #[test]
    fn check_oversized_chunk_no_row_estimate_returns_none() {
        // MySQL: no trustworthy scan-free estimate → row_estimate None → skip.
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 1000000\n");
        assert!(check_oversized_chunk(&e, None, Some(500), Some("0"), Some("9999999")).is_none());
    }

    #[test]
    fn check_oversized_chunk_no_width_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 1000000\n");
        assert!(
            check_oversized_chunk(&e, Some(10_000_000), None, Some("0"), Some("9999999")).is_none()
        );
    }

    #[test]
    fn check_oversized_chunk_small_chunk_returns_none() {
        // 100k rows/chunk × 500 B = ~48 MB < 256 MB budget.
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 100000\n");
        let w = check_oversized_chunk(&e, Some(10_000_000), Some(500), Some("0"), Some("10000000"));
        assert!(w.is_none(), "48 MB/chunk is under budget: {w:?}");
    }

    #[test]
    fn check_oversized_chunk_heavy_range_chunk_warns_and_suggests_smaller() {
        // density 1.0 → 1M rows/chunk × 500 B = ~476 MB > 256 MB.
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 1000000\n");
        let w = check_oversized_chunk(&e, Some(10_000_000), Some(500), Some("0"), Some("10000000"))
            .expect("heavy chunk must warn");
        assert!(w.contains("Heavy chunk"), "got: {w}");
        assert!(
            w.contains("chunk_size=1000000"),
            "names the current size: {w}"
        );
        assert!(
            w.contains("chunk_size around"),
            "advises a smaller size: {w}"
        );
    }

    #[test]
    fn check_oversized_chunk_dense_uses_chunk_size_directly() {
        // Dense ordinal windows hold exactly chunk_size rows — no min/max needed.
        // 2M rows/chunk × 200 B = ~381 MB > 256 MB.
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_dense: true\nchunk_size: 2000000\n");
        let w = check_oversized_chunk(&e, Some(50_000_000), Some(200), None, None)
            .expect("dense heavy chunk must warn even without a key range");
        assert!(w.contains("Heavy chunk"), "got: {w}");
    }

    // ── check_dense_surrogate_cost ──────────────────────────────────────────

    #[test]
    fn check_dense_surrogate_row_number_in_query_returns_warning() {
        let e = cfg(
            "mode: chunked\nchunk_column: id\nquery: \"SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t\"\n",
        );
        assert!(check_dense_surrogate_cost(&e).is_some());
    }

    #[test]
    fn check_dense_surrogate_chunk_dense_flag_returns_warning() {
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_dense: true\n");
        assert!(check_dense_surrogate_cost(&e).is_some());
    }

    #[test]
    fn check_dense_surrogate_normal_chunked_returns_none() {
        let e = cfg("mode: chunked\nchunk_column: id\n");
        assert!(check_dense_surrogate_cost(&e).is_none());
    }

    // ── check_connection_limit ──────────────────────────────────────────────

    #[test]
    fn check_connection_limit_parallel_one_returns_none() {
        assert!(check_connection_limit(1, Some(100)).is_none());
    }

    #[test]
    fn check_connection_limit_unknown_max_conn_returns_warning() {
        let w = check_connection_limit(4, None).unwrap();
        assert!(
            w.contains("max_connections") || w.contains("limit check"),
            "got: {w}"
        );
    }

    #[test]
    fn check_connection_limit_exceeds_max_conn_warns() {
        let w = check_connection_limit(10, Some(8)).unwrap();
        assert!(w.contains("10") && w.contains("8"), "got: {w}");
    }

    #[test]
    fn check_connection_limit_within_limit_returns_none() {
        assert!(check_connection_limit(4, Some(100)).is_none());
    }

    // ── check_parallel_memory_risk ──────────────────────────────────────────

    #[test]
    fn check_parallel_memory_risk_parallel_one_returns_none() {
        let e = cfg("parallel: 1\n");
        assert!(check_parallel_memory_risk(&e, Some(10_000_000)).is_none());
    }

    #[test]
    fn check_parallel_memory_risk_small_rows_returns_none() {
        let e = cfg("parallel: 4\n");
        assert!(check_parallel_memory_risk(&e, Some(100_000)).is_none());
    }

    #[test]
    fn check_parallel_memory_risk_large_rows_returns_warning() {
        let e = cfg("parallel: 4\n");
        let w = check_parallel_memory_risk(&e, Some(6_000_000)).unwrap();
        assert!(
            w.contains("Parallel=4") || w.contains("arallel"),
            "got: {w}"
        );
    }

    // ── collect_warnings (the manifest) ─────────────────────────────────────

    #[test]
    fn collect_warnings_assembles_applicable_checks_in_registry_order() {
        // The manifest rewrite must keep display order AND aggregate every
        // applicable check. This config trips three at once: connection-limit
        // (parallel >= max_connections), sparse-range (density < 0.1), and
        // parallel-memory (> 5M rows). oversized-chunk and dense-surrogate stay
        // None, so the survivors must be the other three, in array order.
        let e = cfg("mode: chunked\nchunk_column: id\nchunk_size: 100000\nparallel: 20\n");
        let warnings = collect_warnings(
            &e,
            Some(10_000_000), // row_estimate
            None,             // avg_row_bytes → oversized-chunk check is skipped
            Some("1"),
            Some("1000000000"), // wide span → sparse range
            Some(20),           // db max_connections → connection-limit trips
        );
        assert_eq!(warnings.len(), 3, "three checks apply: {warnings:?}");
        assert!(
            warnings[0].contains("max_connections"),
            "connection-limit is first in the registry: {warnings:?}"
        );
        assert!(
            warnings[1].contains("Sparse"),
            "sparse-range is second: {warnings:?}"
        );
        assert!(
            warnings[2].contains("memory"),
            "parallel-memory is last: {warnings:?}"
        );
    }

    // ── compute_verdict ─────────────────────────────────────────────────────

    #[test]
    fn compute_verdict_indexed_cursor_small_is_efficient() {
        assert!(matches!(
            compute_verdict(Some(1_000_000), true, true),
            HealthVerdict::Efficient
        ));
    }

    #[test]
    fn compute_verdict_indexed_no_cursor_small_is_acceptable() {
        assert!(matches!(
            compute_verdict(Some(1_000_000), true, false),
            HealthVerdict::Acceptable
        ));
    }

    #[test]
    fn compute_verdict_no_index_small_is_degraded() {
        assert!(matches!(
            compute_verdict(Some(500_000), false, true),
            HealthVerdict::Degraded
        ));
    }

    #[test]
    fn compute_verdict_no_index_very_large_is_unsafe() {
        // > 1_000_000 rows, no index, no cursor → Unsafe
        assert!(matches!(
            compute_verdict(Some(5_000_000), false, false),
            HealthVerdict::Unsafe
        ));
    }
}
