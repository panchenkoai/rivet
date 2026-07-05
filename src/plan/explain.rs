//! Strategy explanation — the *why* behind a plan's mode, geometry, and
//! parallelism.
//!
//! `rivet plan` prints the computed numbers (mode, chunk count, row estimate,
//! verdict). This module turns those numbers into a short, honest narrative: why
//! *this* mode was chosen, why *this* chunk geometry, why *this* parallelism, and
//! what the risk profile is (resumable? memory bound?).
//!
//! It is a **pure** function over data already available in the lib layer — the
//! preflight [`ExportDiagnostic`] plus the [`ExportConfig`] — and the shared
//! memory model in [`crate::tuning::memory`]. It deliberately does **not** touch
//! `crate::init` (bin-only) even though `init::TableInfo::mode_rationale` covers
//! the same ground for `rivet init`: the plan layer is library code and cannot
//! reference the binary's modules.
//!
//! All numbers are data-driven: a missing `row_estimate` or `avg_row_bytes`
//! degrades to an explicit "unavailable" note rather than a fabricated figure.

use crate::config::ExportConfig;
use crate::preflight::{ExportDiagnostic, SMALL_TABLE_ROW_THRESHOLD};
use crate::tuning::memory::{DEFAULT_MEM_BUDGET_MB, estimate_peak_rss_mb};

/// Build a concise (1–3 sentence) narrative explaining why this export's
/// strategy — mode, chunk geometry, parallelism — was chosen, plus its risk
/// profile (resumable? memory bound?).
///
/// Reads only from the preflight [`ExportDiagnostic`] and the [`ExportConfig`]
/// (both lib-accessible) and the shared [`crate::tuning::memory`] model, so it
/// compiles in the library and never fabricates a number it does not have.
pub fn explain_strategy(diag: &ExportDiagnostic, export: &ExportConfig) -> String {
    let mut sentences: Vec<String> = Vec::new();

    sentences.push(explain_mode(diag, export));
    if let Some(geometry) = explain_geometry(diag, export) {
        sentences.push(geometry);
    }
    sentences.push(explain_parallelism(diag, export));
    sentences.push(explain_risk(diag, export));

    sentences.join(" ")
}

/// Why this mode — grounds the choice in the row estimate vs. the chunked
/// threshold and the presence (or absence) of an indexed chunk/cursor column.
fn explain_mode(diag: &ExportDiagnostic, export: &ExportConfig) -> String {
    let rows = fmt_rows(diag.row_estimate);
    match diag.mode.as_str() {
        "chunked" => {
            let col = export
                .chunk_column
                .as_deref()
                .or(diag.cursor_column.as_deref())
                .unwrap_or("key");
            let index_note = if diag.uses_index {
                format!("chunk column `{col}` is indexed")
            } else {
                format!("chunk column `{col}` is NOT indexed (each chunk is a full scan)")
            };
            match diag.row_estimate {
                Some(r) if r >= SMALL_TABLE_ROW_THRESHOLD => format!(
                    "Mode chunked: {rows} rows ≥ the {threshold} threshold and {index_note}, so the table is split into key-range windows.",
                    threshold = fmt_rows(Some(SMALL_TABLE_ROW_THRESHOLD)),
                ),
                _ => format!("Mode chunked: configured explicitly ({rows}); {index_note}."),
            }
        }
        "incremental" => {
            let col = diag
                .cursor_column
                .as_deref()
                .or(export.cursor_column.as_deref())
                .unwrap_or("cursor");
            let index_note = if diag.uses_index {
                "the cursor is indexed, so the watermark predicate is an index range scan"
            } else {
                "the cursor is NOT indexed, so the watermark predicate scans the table"
            };
            format!(
                "Mode incremental on `{col}`: resumes from the last stored watermark and pulls only newer rows ({index_note})."
            )
        }
        "full" => match diag.row_estimate {
            Some(r) if r < SMALL_TABLE_ROW_THRESHOLD => format!(
                "Mode full: {rows} rows, below the {threshold} chunked threshold, so a single-pass copy is cheapest.",
                threshold = fmt_rows(Some(SMALL_TABLE_ROW_THRESHOLD)),
            ),
            _ => format!("Mode full: a single-pass copy of the whole result set ({rows})."),
        },
        "timewindow" => {
            let col = export.time_column.as_deref().unwrap_or("time column");
            let days = export
                .days_window
                .map(|d| format!("{d}-day"))
                .unwrap_or_else(|| "rolling".to_string());
            format!("Mode time_window: re-reads a {days} window of `{col}` each run ({rows}).")
        }
        "keyset" => {
            let col = export.chunk_by_key.as_deref().unwrap_or("key");
            format!(
                "Mode keyset: seek-paginates by the indexed key `{col}` ({rows}), bounding peak memory and query hold-time without a single-integer PK."
            )
        }
        other => format!("Mode {other}: {rows}."),
    }
}

/// Why this chunk_size / file geometry — only meaningful for chunked exports;
/// relates the row estimate to the chunk size and the resulting part count.
fn explain_geometry(diag: &ExportDiagnostic, export: &ExportConfig) -> Option<String> {
    if diag.mode != "chunked" {
        return None;
    }

    // An explicit `chunk_count` fixes the number of windows directly; the size is
    // derived from min/max at detect time, so don't claim a row-derived count.
    if let Some(count) = export.chunk_count {
        return Some(format!(
            "Geometry: {count} equal key-range windows (chunk_count={count}); chunk_size is derived from the observed min/max."
        ));
    }

    let size = export.chunk_size;
    match diag.row_estimate {
        Some(r) if size > 0 => {
            let parts = (r as usize).div_ceil(size).max(1);
            Some(format!(
                "Geometry: chunk_size {chunk} over {rows} rows yields ~{parts} part file(s).",
                chunk = fmt_rows(Some(size as i64)),
                rows = fmt_rows(Some(r)),
            ))
        }
        _ => Some(format!(
            "Geometry: chunk_size {chunk}; part count unknown (row estimate unavailable).",
            chunk = fmt_rows(Some(size as i64)),
        )),
    }
}

/// Why this parallelism — ties the configured worker count to the shared RSS
/// budget via `tuning::memory::estimate_peak_rss_mb`, and surfaces the preflight
/// recommendation when it diverges from what is configured.
fn explain_parallelism(diag: &ExportDiagnostic, export: &ExportConfig) -> String {
    // The worker count that will actually run is the configured `parallel`
    // (chunked only); other modes are single-threaded at query time.
    let parallel = parallel_workers(diag, export);

    let (rec_level, rec_reason) = diag.recommended_parallel;

    let budget = DEFAULT_MEM_BUDGET_MB;
    let mem_note = match diag.avg_row_bytes {
        Some(width) => {
            let peak = estimate_peak_rss_mb(parallel, width);
            let fit = if peak <= budget { "fits" } else { "EXCEEDS" };
            format!("parallel={parallel} {fit} the {budget} MB budget at ~{peak} MB peak")
        }
        None => {
            format!(
                "parallel={parallel} (row width unavailable, so peak RSS cannot be bounded here)"
            )
        }
    };

    if parallel == 1 {
        format!("Parallelism: single worker — {rec_reason}; {mem_note}.")
    } else if rec_level as usize == parallel {
        format!("Parallelism: {mem_note}; this matches the recommendation ({rec_reason}).")
    } else {
        format!("Parallelism: {mem_note}; preflight recommends {rec_level} ({rec_reason}).")
    }
}

/// Risk profile — resumability and the memory bound.
fn explain_risk(diag: &ExportDiagnostic, export: &ExportConfig) -> String {
    let resumable = match diag.mode.as_str() {
        // Incremental resumes by its stored watermark every run.
        "incremental" => "resumable: yes — re-runs continue from the stored watermark".to_string(),
        // Chunked resumes only when the checkpoint is on. Without it a failed run
        // re-reads the whole table from the first window.
        "chunked" => "resumable: per-chunk when checkpointing is on; otherwise a failed/re-run \
             re-reads the whole table"
            .to_string(),
        // Keyset pages forward by key but does not persist progress across runs.
        "keyset" => {
            "resumable: pages forward by key within a run, but a re-run restarts from the first page"
                .to_string()
        }
        // Full and time_window restart from scratch.
        _ => "resumable: no — a failed run restarts from the beginning".to_string(),
    };

    let parallel = parallel_workers(diag, export);
    let mem = match diag.avg_row_bytes {
        Some(width) => format!(
            "memory: bounded to ~{} MB peak",
            estimate_peak_rss_mb(parallel, width)
        ),
        None => "memory: peak unavailable (no row-width estimate)".to_string(),
    };

    format!("Risk — {resumable}; {mem}.")
}

/// Worker count that will actually run: the configured `parallel` for chunked
/// exports (the only mode that spawns a worker pool), 1 for everything else.
fn parallel_workers(diag: &ExportDiagnostic, export: &ExportConfig) -> usize {
    if diag.mode == "chunked" {
        export.parallel.max(1)
    } else {
        1
    }
}

/// Format an optional row count for prose: thousands-separated with a `M`/`K`
/// shorthand for large values, or an explicit "row estimate unavailable" when
/// `None` so the narrative never invents a number.
fn fmt_rows(rows: Option<i64>) -> String {
    match rows {
        None => "row estimate unavailable".to_string(),
        Some(r) if r < 0 => "row estimate unavailable".to_string(),
        Some(r) if r >= 1_000_000 => {
            let m = r as f64 / 1_000_000.0;
            format!("~{m:.1}M")
        }
        Some(r) if r >= 10_000 => {
            let k = r as f64 / 1_000.0;
            format!("~{k:.0}K")
        }
        Some(r) => format!("~{r}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ExportMode, FormatType};
    use crate::preflight::{ExportDiagnostic, HealthVerdict};

    fn base_export(name: &str) -> ExportConfig {
        crate::config::sample_export(name)
    }

    fn base_diag(mode: &str) -> ExportDiagnostic {
        ExportDiagnostic {
            export_name: "t".into(),
            strategy: mode.into(),
            mode: mode.into(),
            row_estimate: None,
            avg_row_bytes: None,
            cursor_column: None,
            cursor_min: None,
            cursor_max: None,
            scan_type: None,
            uses_index: false,
            verdict: HealthVerdict::Acceptable,
            recommended_profile: "balanced",
            recommended_parallel: (1, "small dataset"),
            warnings: vec![],
            suggestion: None,
        }
    }

    #[test]
    fn chunked_indexed_mentions_mode_threshold_and_index() {
        let mut diag = base_diag("chunked");
        diag.row_estimate = Some(5_000_000);
        diag.avg_row_bytes = Some(96);
        diag.uses_index = true;
        diag.recommended_parallel = (4, "large dataset with index support");

        let mut export = base_export("page_views");
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        export.chunk_size = 250_000;
        export.parallel = 4;

        let s = explain_strategy(&diag, &export);
        assert!(s.contains("chunked"), "mentions mode: {s}");
        assert!(s.contains("`id`"), "names the chunk column: {s}");
        assert!(s.contains("indexed"), "calls out the index: {s}");
        assert!(s.contains("~5.0M"), "uses the row estimate: {s}");
        // Geometry: 5_000_000 / 250_000 = 20 parts.
        assert!(s.contains("~20 part"), "derives the part count: {s}");
        // Parallelism: estimate_peak_rss_mb(4, 96) and the 2048 budget.
        let peak = estimate_peak_rss_mb(4, 96);
        assert!(
            s.contains(&format!("~{peak} MB")),
            "cites the RSS peak: {s}"
        );
        assert!(s.contains("2048 MB budget"), "cites the budget: {s}");
        assert!(s.contains("fits"), "states it fits: {s}");
    }

    #[test]
    fn incremental_mentions_watermark_and_cursor() {
        let mut diag = base_diag("incremental");
        diag.cursor_column = Some("updated_at".into());
        diag.row_estimate = Some(1_234_567);
        diag.avg_row_bytes = Some(64);
        diag.uses_index = true;

        let mut export = base_export("orders");
        export.mode = ExportMode::Incremental;
        export.cursor_column = Some("updated_at".into());

        let s = explain_strategy(&diag, &export);
        assert!(s.contains("incremental"), "mentions mode: {s}");
        assert!(s.contains("`updated_at`"), "names the cursor: {s}");
        assert!(s.contains("watermark"), "explains the resume driver: {s}");
        assert!(
            s.contains("resumable: yes"),
            "incremental is resumable: {s}"
        );
        // No geometry sentence for non-chunked modes.
        assert!(!s.contains("part file"), "no chunk geometry leaks in: {s}");
    }

    #[test]
    fn full_small_mentions_below_threshold() {
        let mut diag = base_diag("full");
        diag.row_estimate = Some(40_000);
        diag.avg_row_bytes = Some(50);

        let mut export = base_export("lookup");
        export.mode = ExportMode::Full;
        export.format = FormatType::Csv;

        let s = explain_strategy(&diag, &export);
        assert!(s.contains("full"), "mentions mode: {s}");
        assert!(s.contains("below"), "explains it's below threshold: {s}");
        assert!(s.contains("~40K"), "uses the row estimate: {s}");
        assert!(
            s.contains("resumable: no"),
            "full restarts from scratch: {s}"
        );
    }

    #[test]
    fn unknown_rows_falls_back_gracefully() {
        // Chunked but no row estimate and no row width: every number must
        // degrade to an explicit "unavailable", never a fabricated figure.
        let mut diag = base_diag("chunked");
        diag.uses_index = true;
        // row_estimate = None, avg_row_bytes = None (from base_diag).

        let mut export = base_export("mystery");
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("pk".into());
        export.chunk_size = 100_000;
        export.parallel = 2;

        let s = explain_strategy(&diag, &export);
        assert!(s.contains("chunked"), "mentions mode: {s}");
        assert!(
            s.contains("row estimate unavailable"),
            "says estimate is unavailable: {s}"
        );
        assert!(
            s.contains("part count unknown"),
            "geometry degrades gracefully: {s}"
        );
        assert!(
            s.contains("row width unavailable") || s.contains("peak unavailable"),
            "memory bound degrades gracefully: {s}"
        );
        // Crucially: no invented number where data is missing.
        assert!(!s.contains("~0 "), "must not fabricate a zero figure: {s}");
    }

    #[test]
    fn parallelism_flags_divergence_from_recommendation() {
        // Configured parallel (8) diverges from the preflight recommendation (4):
        // the narrative must surface the recommendation rather than hide it.
        let mut diag = base_diag("chunked");
        diag.row_estimate = Some(20_000_000);
        diag.avg_row_bytes = Some(40);
        diag.uses_index = true;
        diag.recommended_parallel = (4, "very large dataset — cap at 4 to control memory");

        let mut export = base_export("events");
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        export.chunk_size = 1_000_000;
        export.parallel = 8;

        let s = explain_strategy(&diag, &export);
        assert!(
            s.contains("preflight recommends 4"),
            "surfaces the divergent recommendation: {s}"
        );
    }

    #[test]
    fn over_budget_parallelism_says_exceeds() {
        // Wide rows × high parallelism should blow the budget and say so.
        let mut diag = base_diag("chunked");
        diag.row_estimate = Some(50_000_000);
        diag.avg_row_bytes = Some(8_000); // wide rows
        diag.uses_index = true;
        diag.recommended_parallel = (4, "large indexed dataset");

        let mut export = base_export("wide");
        export.mode = ExportMode::Chunked;
        export.chunk_column = Some("id".into());
        export.chunk_size = 1_000_000;
        export.parallel = 64;

        let s = explain_strategy(&diag, &export);
        let peak = estimate_peak_rss_mb(64, 8_000);
        assert!(peak > DEFAULT_MEM_BUDGET_MB, "sanity: scenario over budget");
        assert!(s.contains("EXCEEDS"), "flags the over-budget peak: {s}");
    }
}
