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
                    format!("date-chunked-parallel({}, {}d, p={})", col, days, export.parallel)
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
    chunk_min: Option<&str>,
    chunk_max: Option<&str>,
    db_max_connections: Option<u32>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(w) = check_connection_limit(export.parallel, db_max_connections) {
        warnings.push(w);
    }
    if let Some(w) = check_sparse_range(export, row_estimate, chunk_min, chunk_max) {
        warnings.push(w);
    }
    if let Some(w) = check_dense_surrogate_cost(export) {
        warnings.push(w);
    }
    if let Some(w) = check_parallel_memory_risk(export, row_estimate) {
        warnings.push(w);
    }
    warnings
}

pub(crate) fn compute_verdict(
    row_estimate: Option<i64>,
    uses_index: bool,
    has_cursor: bool,
) -> HealthVerdict {
    let rows = row_estimate.unwrap_or(0);

    match (uses_index, has_cursor, rows) {
        (true, true, r) if r <= 10_000_000 => HealthVerdict::Efficient,
        (true, true, _) => HealthVerdict::Acceptable,
        (true, false, r) if r <= 10_000_000 => HealthVerdict::Acceptable,
        (false, _, r) if r <= 1_000_000 => HealthVerdict::Degraded,
        (false, true, r) if r <= 50_000_000 => HealthVerdict::Degraded,
        (false, _, _) => HealthVerdict::Unsafe,
        _ => HealthVerdict::Degraded,
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
                ExportMode::Chunked => {
                    let col = export.chunk_column.as_deref().unwrap_or("chunk_column");
                    parts.push(format!(
                        "Create an index on '{}' to speed up range scans.",
                        col
                    ));
                }
                ExportMode::TimeWindow => {
                    let col = export.time_column.as_deref().unwrap_or("time_column");
                    parts.push(format!(
                        "Create an index on '{}' for efficient time-window filtering.",
                        col
                    ));
                }
                _ => {
                    if export.cursor_column.is_none() {
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
