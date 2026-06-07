//! Chunk boundary detection.
//!
//! Queries min/max (and optionally COUNT) from the source to compute chunk ranges,
//! logs sparsity diagnostics, and returns the final `Vec<(i64, i64)>` chunk list.

use super::math::{generate_chunks, parse_date_flexible, parse_scalar_i64, strip_select_star_from};
use crate::error::Result;
use crate::source::Source;

/// `SELECT COUNT(*) FROM (<base>) AS _rivet_rowcnt`, with a fast path when
/// `base_query` is the `SELECT * FROM <ident>` shape produced by the `table:`
/// shortcut. The wrapped form forces Postgres to materialise the inner result
/// (we measured ~3.2 GB of `temp_files` on a 8.6 GB table) — the fast path
/// avoids the wrap entirely so `COUNT(*)` becomes a plain heap-scan / index-only
/// scan with no spill.
fn query_wrapped_row_count(src: &mut dyn Source, base_query: &str) -> Result<i64> {
    let sql = match strip_select_star_from(base_query) {
        Some(table_ident) => format!("SELECT COUNT(*) FROM {table_ident}"),
        None => format!("SELECT COUNT(*) FROM ({base_query}) AS _rivet_rowcnt"),
    };
    let raw = src
        .query_scalar(&sql)?
        .ok_or_else(|| anyhow::anyhow!("COUNT(*) returned no row"))?;
    parse_scalar_i64(&raw)
}

/// Refuse to range/date-chunk on a column that has NULL values.
///
/// Range chunking filters with `WHERE col BETWEEN min AND max` and date chunking
/// with `>= start AND < end`; both forms exclude NULL, so a row whose
/// `chunk_column` is NULL falls into no chunk and would vanish from the export
/// with no error. The keyset path already refuses a nullable key up front
/// (`plan::build::resolve_chunked_strategy`, via `is_usable_keyset_key`); the
/// range path historically did not. We detect the condition on the live data
/// (one aggregate — a nullable column with zero actual NULLs is fine and must
/// not be rejected) and bail with the same posture as keyset rather than drop
/// rows silently. `chunk_dense` does NOT call this: its `ROW_NUMBER()` numbers
/// every row, NULL-keyed included, so no row is lost.
fn bail_if_null_keyed(
    src: &mut dyn Source,
    base_query: &str,
    chunk_column: &str,
    export_name: &str,
    source_type: crate::config::SourceType,
) -> Result<()> {
    let col = crate::sql::quote_ident(source_type, chunk_column);
    // `COUNT(*) - COUNT(col)` = number of NULL-keyed rows, dialect-agnostic and
    // reusing the same fast-path-vs-wrap shape as the row-count query.
    let sql = match strip_select_star_from(base_query) {
        Some(table_ident) => format!("SELECT COUNT(*) - COUNT({col}) FROM {table_ident}"),
        None => format!("SELECT COUNT(*) - COUNT({col}) FROM ({base_query}) AS _rivet_nullcnt"),
    };
    let null_keyed = src
        .query_scalar(&sql)?
        .as_deref()
        .map(parse_scalar_i64)
        .transpose()?
        .unwrap_or(0);
    if null_keyed > 0 {
        anyhow::bail!(
            "export '{}': {} row(s) have NULL in chunk_column '{}'. Range/date chunking filters \
             with `BETWEEN min AND max` (or `>= .. < ..`), which excludes NULL — those rows would \
             be silently dropped from the export. Fix one of: use a NOT NULL column for \
             chunk_column; add `WHERE {} IS NOT NULL` to the query to drop them explicitly; set \
             `chunk_dense: true` (ROW_NUMBER covers every row, NULL-keyed included); or use \
             `mode: full`.",
            export_name,
            null_keyed,
            chunk_column,
            chunk_column
        );
    }
    Ok(())
}

fn log_chunk_boundaries_list(export_name: &str, chunks: &[(i64, i64)]) {
    const HEAD: usize = 8;
    const TAIL: usize = 8;
    if chunks.is_empty() {
        log::info!(
            "export '{}': no BETWEEN windows (empty key range)",
            export_name
        );
        return;
    }
    if chunks.len() <= HEAD + TAIL {
        for (i, (a, b)) in chunks.iter().enumerate() {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
    } else {
        for (i, (a, b)) in chunks.iter().enumerate().take(HEAD) {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
        log::info!(
            "export '{}':   ... {} windows omitted ...",
            export_name,
            chunks.len() - HEAD - TAIL
        );
        for (i, (a, b)) in chunks.iter().enumerate().skip(chunks.len() - TAIL) {
            log::info!("export '{}':   [{:>5}] {} .. {}", export_name, i, a, b);
        }
    }
}

fn log_chunk_sparsity_at_run(
    export_name: &str,
    chunk_column: &str,
    chunk_size: usize,
    min_val: i64,
    max_val: i64,
    chunks: &[(i64, i64)],
    row_count: i64,
) {
    if row_count == 0 {
        log::info!(
            "export '{}': COUNT(*) = 0 — no rows in export query; {} BETWEEN window(s) from `{}` min..max (runs will skip empty chunks)",
            export_name,
            chunks.len(),
            chunk_column
        );
        if !chunks.is_empty() && chunks.len() <= 24 {
            log_chunk_boundaries_list(export_name, chunks);
        }
        return;
    }

    let info =
        crate::preflight::chunk_sparsity_from_counts(row_count, min_val, max_val, chunk_size);
    if info.is_sparse {
        let fill_pct = info.density * 100.0;
        let empty_hint = (1.0 - info.density).clamp(0.0, 1.0) * 100.0;
        log::info!(
            "export '{}': sparse `{}` range — ~{:.2}% of the min..max ID band contains rows (~{:.1}% of logical windows likely empty). \
             rows={}, span≈{}, chunk_size={}, ~{} logical windows, {} BETWEEN chunks. Computed boundaries:",
            export_name,
            chunk_column,
            fill_pct,
            empty_hint,
            info.row_count,
            info.range_span,
            chunk_size,
            info.logical_windows,
            chunks.len(),
        );
        log_chunk_boundaries_list(export_name, chunks);
    } else {
        log::info!(
            "export '{}': `{}` range looks dense enough for BETWEEN chunking (rows={}, span≈{}, density≈{:.6}, {} chunks); continuing",
            export_name,
            chunk_column,
            info.row_count,
            info.range_span,
            info.density,
            chunks.len(),
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn detect_and_generate_chunks(
    src: &mut dyn Source,
    base_query: &str,
    chunk_column: &str,
    chunk_size: usize,
    chunk_count: Option<usize>,
    export_name: &str,
    chunk_dense: bool,
    chunk_by_days: Option<u32>,
    source_type: crate::config::SourceType,
) -> Result<Vec<(i64, i64)>> {
    if let Some(days_per_chunk) = chunk_by_days {
        // NULL-keyed rows are excluded by the date predicate `>= start AND < end`
        // — refuse rather than drop them silently. (chunk_dense, below, is exempt.)
        bail_if_null_keyed(src, base_query, chunk_column, export_name, source_type)?;
        let min_sql = crate::sql::aggregate_sql(source_type, "min", chunk_column, base_query);
        let max_sql = crate::sql::aggregate_sql(source_type, "max", chunk_column, base_query);
        let min_str = src.query_scalar(&min_sql)?.ok_or_else(|| {
            anyhow::anyhow!(
                "export '{}': min({}) returned NULL — table may be empty",
                export_name,
                chunk_column
            )
        })?;
        let max_str = src.query_scalar(&max_sql)?.ok_or_else(|| {
            anyhow::anyhow!(
                "export '{}': max({}) returned NULL — table may be empty",
                export_name,
                chunk_column
            )
        })?;

        let min_date = parse_date_flexible(&min_str).ok_or_else(|| {
            anyhow::anyhow!(
                "export '{}': could not parse min({}) = {:?} as a date (expected YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
                export_name, chunk_column, min_str
            )
        })?;
        let max_date = parse_date_flexible(&max_str).ok_or_else(|| {
            anyhow::anyhow!(
                "export '{}': could not parse max({}) = {:?} as a date (expected YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)",
                export_name, chunk_column, max_str
            )
        })?;

        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
        let min_days = (min_date - epoch).num_days();
        let max_days = (max_date - epoch).num_days();
        let total_days = (max_days - min_days).max(0);
        let chunks = generate_chunks(min_days, max_days, days_per_chunk as i64);
        log::info!(
            "export '{}': date chunk_column `{}` range {} .. {} ({} days, {} days/chunk → {} chunk(s))",
            export_name,
            chunk_column,
            min_date,
            max_date,
            total_days,
            days_per_chunk,
            chunks.len()
        );
        return Ok(chunks);
    }

    if chunk_dense {
        let row_count = query_wrapped_row_count(src, base_query)?;
        log::info!(
            "export '{}': chunk_dense: ROW_NUMBER() OVER (ORDER BY `{}`) — {} row(s), chunk_size={}",
            export_name,
            chunk_column,
            row_count,
            chunk_size
        );
        let chunks = if row_count <= 0 {
            vec![]
        } else {
            generate_chunks(1, row_count, chunk_size as i64)
        };
        log::info!(
            "export '{}': dense chunk plan: {} window(s) on ordinal 1..{}",
            export_name,
            chunks.len(),
            row_count
        );
        return Ok(chunks);
    }

    // NULL-keyed rows are excluded by `WHERE col BETWEEN min AND max` — refuse
    // rather than drop them silently. (chunk_dense, above, is exempt.)
    bail_if_null_keyed(src, base_query, chunk_column, export_name, source_type)?;

    let min_sql = crate::sql::aggregate_sql(source_type, "min", chunk_column, base_query);
    let max_sql = crate::sql::aggregate_sql(source_type, "max", chunk_column, base_query);

    let min_val = src
        .query_scalar(&min_sql)?
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    let max_val = src
        .query_scalar(&max_sql)?
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);

    let effective_chunk_size = if let Some(count) = chunk_count {
        let span = (max_val - min_val).max(0) as usize + 1;
        span.div_ceil(count)
    } else {
        chunk_size
    };

    log::info!(
        "export '{}': chunk_column `{}` range {} .. {} ({})",
        export_name,
        chunk_column,
        min_val,
        max_val,
        if let Some(count) = chunk_count {
            format!("chunk_count={count}, effective chunk_size={effective_chunk_size}")
        } else {
            format!("chunk_size={effective_chunk_size}")
        }
    );

    let chunks = generate_chunks(min_val, max_val, effective_chunk_size as i64);

    match query_wrapped_row_count(src, base_query) {
        Ok(row_count) => {
            log_chunk_sparsity_at_run(
                export_name,
                chunk_column,
                chunk_size,
                min_val,
                max_val,
                &chunks,
                row_count,
            );
        }
        Err(e) => {
            log::warn!(
                "export '{}': could not run COUNT(*) for sparsity diagnostics: {:#}; proceeding with {} windows from min/max only",
                export_name,
                e,
                chunks.len()
            );
            if chunks.len() <= 24 {
                log_chunk_boundaries_list(export_name, &chunks);
            } else {
                log::info!(
                    "export '{}': use `RUST_LOG=info rivet run` after fixing COUNT if you need the full boundary list",
                    export_name
                );
            }
        }
    }

    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceType;
    use std::collections::VecDeque;

    // ── ScriptedSource ───────────────────────────────────────────────────────
    // Returns pre-queued replies for query_scalar; panics if queue is exhausted.
    // export / type_mappings are never called in detect tests.

    /// Sniffs every SQL string that gets queried — lets us assert on the
    /// rewrite, since that is the whole point of the fast-path patch.
    struct ScriptedSource {
        replies: VecDeque<Result<Option<String>>>,
        seen_sql: Vec<String>,
        /// Value returned for the NULL-key guard query (`COUNT(*) - COUNT(col)`).
        /// Defaults to 0 so existing min/max/count scripts stay focused and the
        /// guard is a transparent no-op; the bail tests set it explicitly.
        null_keys: i64,
    }

    impl ScriptedSource {
        fn new(items: impl IntoIterator<Item = Result<Option<String>>>) -> Self {
            Self {
                replies: items.into_iter().collect(),
                seen_sql: Vec::new(),
                null_keys: 0,
            }
        }

        fn with_null_keys(mut self, n: i64) -> Self {
            self.null_keys = n;
            self
        }
    }

    fn ok(s: &str) -> Result<Option<String>> {
        Ok(Some(s.to_string()))
    }
    fn null() -> Result<Option<String>> {
        Ok(None)
    }

    impl crate::source::Source for ScriptedSource {
        fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
            self.seen_sql.push(sql.to_string());
            // The NULL-key guard issues `COUNT(*) - COUNT(col)`; answer it from
            // `null_keys` (default 0) so it doesn't consume the min/max/count
            // script and existing tests stay unchanged.
            if sql.contains("- COUNT(") {
                return Ok(Some(self.null_keys.to_string()));
            }
            self.replies
                .pop_front()
                .expect("ScriptedSource: queue exhausted")
        }
        fn export(
            &mut self,
            _request: &crate::source::ExportRequest<'_>,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> Result<()> {
            unimplemented!()
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _column_overrides: &crate::types::ColumnOverrides,
        ) -> Result<Vec<crate::types::TypeMapping>> {
            unimplemented!()
        }
    }

    fn detect(
        src: &mut dyn crate::source::Source,
        chunk_size: usize,
        chunk_dense: bool,
        chunk_by_days: Option<u32>,
    ) -> Result<Vec<(i64, i64)>> {
        detect_and_generate_chunks(
            src,
            "SELECT id FROM orders",
            "id",
            chunk_size,
            None,
            "orders",
            chunk_dense,
            chunk_by_days,
            SourceType::Postgres,
        )
    }

    // ── integer range path ──────────────────────────────────────────────────

    #[test]
    fn integer_range_basic_chunks_computed_correctly() {
        // min=1, max=1000, COUNT=500 → 10 chunks of size 100
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
        let chunks = detect(&mut src, 100, false, None).unwrap();
        assert_eq!(chunks.len(), 10);
        assert_eq!(chunks[0], (1, 100));
        assert_eq!(chunks[9], (901, 1000));
    }

    #[test]
    fn integer_range_null_minmax_collapses_to_zero_zero() {
        // NULL min/max → unwrap_or(0) on both → single chunk (0,0)
        let mut src = ScriptedSource::new([null(), null(), ok("0")]);
        let chunks = detect(&mut src, 100, false, None).unwrap();
        assert_eq!(chunks, vec![(0, 0)]);
    }

    #[test]
    fn integer_range_count_failure_still_returns_chunks() {
        // COUNT returns unparseable string → logged as warning; chunks from min/max returned
        let mut src = ScriptedSource::new([ok("1"), ok("100"), ok("not-a-number")]);
        let chunks = detect(&mut src, 50, false, None).unwrap();
        assert_eq!(chunks, vec![(1, 50), (51, 100)]);
    }

    #[test]
    fn integer_range_single_chunk_when_range_smaller_than_size() {
        let mut src = ScriptedSource::new([ok("5"), ok("20"), ok("15")]);
        let chunks = detect(&mut src, 100, false, None).unwrap();
        assert_eq!(chunks, vec![(5, 20)]);
    }

    // ── NULL-key guard (silent-drop prevention) ─────────────────────────────

    #[test]
    fn null_keyed_rows_in_integer_range_bail_not_silently_dropped() {
        // 3 rows have NULL chunk_column → range chunking would exclude them via
        // BETWEEN. Refuse with a clear, counted error instead of dropping them.
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]).with_null_keys(3);
        let err = detect(&mut src, 100, false, None).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("NULL in chunk_column"), "got: {msg}");
        assert!(msg.contains("3 row(s)"), "should report the count: {msg}");
    }

    #[test]
    fn null_keyed_rows_in_date_range_bail() {
        let mut src = ScriptedSource::new([ok("2023-01-01"), ok("2023-01-31")]).with_null_keys(7);
        let err = detect(&mut src, 10_000, false, Some(7)).unwrap_err();
        assert!(format!("{err:#}").contains("NULL in chunk_column"));
    }

    #[test]
    fn chunk_dense_is_exempt_from_null_guard() {
        // ROW_NUMBER() numbers every row, NULL-keyed included, so dense must NOT
        // run the guard even when NULLs exist — no bail, normal ordinal chunks.
        let mut src = ScriptedSource::new([ok("250")]).with_null_keys(99);
        let chunks = detect(&mut src, 100, true, None).unwrap();
        assert_eq!(chunks, vec![(1, 100), (101, 200), (201, 250)]);
    }

    // ── chunk_dense path ────────────────────────────────────────────────────

    #[test]
    fn chunk_dense_nonzero_produces_ordinal_chunks() {
        // COUNT=250, chunk_size=100 → ordinal windows 1..250
        let mut src = ScriptedSource::new([ok("250")]);
        let chunks = detect(&mut src, 100, true, None).unwrap();
        assert_eq!(chunks, vec![(1, 100), (101, 200), (201, 250)]);
    }

    #[test]
    fn chunk_dense_zero_rows_returns_empty() {
        let mut src = ScriptedSource::new([ok("0")]);
        let chunks = detect(&mut src, 100, true, None).unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn chunk_dense_count_returned_no_row_propagates_error() {
        // COUNT(*) returns no row (None) → error propagated (not just warned)
        let mut src = ScriptedSource::new([null()]);
        let result = detect(&mut src, 100, true, None);
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("COUNT") || msg.contains("no row"),
            "got: {msg}"
        );
    }

    // ── chunk_by_days path ──────────────────────────────────────────────────

    #[test]
    fn chunk_by_days_basic_range_produces_windows() {
        // 2023-01-01 .. 2023-01-31 (31 days), 7 days/chunk → 5 windows
        let mut src = ScriptedSource::new([ok("2023-01-01"), ok("2023-01-31")]);
        let chunks = detect(&mut src, 10_000, false, Some(7)).unwrap();
        assert_eq!(
            chunks.len(),
            5,
            "expected 5 weekly windows, got: {chunks:?}"
        );
    }

    #[test]
    fn chunk_by_days_single_day_returns_one_chunk() {
        let mut src = ScriptedSource::new([ok("2024-03-15"), ok("2024-03-15")]);
        let chunks = detect(&mut src, 10_000, false, Some(7)).unwrap();
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn chunk_by_days_null_min_returns_error() {
        let mut src = ScriptedSource::new([null()]);
        let result = detect(&mut src, 10_000, false, Some(7));
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("NULL") || msg.contains("empty"), "got: {msg}");
    }

    #[test]
    fn chunk_by_days_invalid_date_string_returns_error() {
        let mut src = ScriptedSource::new([ok("not-a-date"), ok("2023-12-31")]);
        let result = detect(&mut src, 10_000, false, Some(7));
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("parse") || msg.contains("date"), "got: {msg}");
    }

    #[test]
    fn chunk_by_days_datetime_format_parsed_correctly() {
        // DB returns DATETIME strings instead of DATE — still parseable
        let mut src = ScriptedSource::new([ok("2023-06-01 00:00:00"), ok("2023-06-30 23:59:59")]);
        let chunks = detect(&mut src, 10_000, false, Some(7)).unwrap();
        // 30 days / 7 = 5 chunks (days 0..29, each 7 except last)
        assert_eq!(chunks.len(), 5, "expected 5 windows, got: {chunks:?}");
    }

    // ── chunk_count path ────────────────────────────────────────────────────

    fn detect_with_count(
        src: &mut dyn crate::source::Source,
        chunk_count: usize,
    ) -> Result<Vec<(i64, i64)>> {
        detect_and_generate_chunks(
            src,
            "SELECT id FROM orders",
            "id",
            100_000, // chunk_size ignored when chunk_count is Some
            Some(chunk_count),
            "orders",
            false,
            None,
            SourceType::Postgres,
        )
    }

    #[test]
    fn chunk_count_divides_range_into_exact_n_chunks() {
        // min=1, max=1000, COUNT=500 → requested 10 chunks of size 100 each
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
        let chunks = detect_with_count(&mut src, 10).unwrap();
        assert_eq!(chunks.len(), 10, "expected 10 chunks: {chunks:?}");
        assert_eq!(chunks[0].0, 1);
        assert_eq!(chunks[9].1, 1000);
    }

    #[test]
    fn chunk_count_ceiling_division_produces_at_most_n_chunks() {
        // min=1, max=100 (100 values), chunk_count=7 → ceil(100/7)=15 per chunk → 7 chunks
        let mut src = ScriptedSource::new([ok("1"), ok("100"), ok("70")]);
        let chunks = detect_with_count(&mut src, 7).unwrap();
        assert!(
            chunks.len() <= 7,
            "must produce at most 7 chunks, got {}: {chunks:?}",
            chunks.len()
        );
        assert_eq!(chunks.first().unwrap().0, 1);
        assert_eq!(chunks.last().unwrap().1, 100);
    }

    #[test]
    fn chunk_count_1_produces_single_full_range_chunk() {
        let mut src = ScriptedSource::new([ok("1"), ok("999"), ok("500")]);
        let chunks = detect_with_count(&mut src, 1).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], (1, 999));
    }

    // ── Fast-path: subquery wrap is skipped when base is SELECT * FROM <ident> ──

    fn detect_with_base(
        src: &mut ScriptedSource,
        base_query: &str,
        chunk_dense: bool,
        chunk_by_days: Option<u32>,
    ) -> Result<Vec<(i64, i64)>> {
        detect_and_generate_chunks(
            src,
            base_query,
            "id",
            100,
            None,
            "tbl",
            chunk_dense,
            chunk_by_days,
            SourceType::Postgres,
        )
    }

    #[test]
    fn fast_path_select_star_emits_min_max_count_without_subquery_wrap() {
        // base = simple SELECT * FROM public.users → no `FROM (...) AS _rivet`
        // anywhere. PG can satisfy these as index-only scans, no temp_files.
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
        let _ = detect_with_base(&mut src, "SELECT * FROM public.users", false, None).unwrap();
        for sql in &src.seen_sql {
            assert!(
                !sql.contains("_rivet") && !sql.contains("FROM ("),
                "fast path must not wrap; got: {sql}"
            );
        }
        // min/max are explicit, COUNT is plain.
        assert!(src.seen_sql.iter().any(|s| s.starts_with("SELECT min(")));
        assert!(src.seen_sql.iter().any(|s| s.starts_with("SELECT max(")));
        assert!(
            src.seen_sql
                .iter()
                .any(|s| s.starts_with("SELECT COUNT(*) FROM public.users"))
        );
    }

    #[test]
    fn fallback_keeps_subquery_wrap_for_curated_query() {
        // Anything that isn't `SELECT * FROM <ident>` → original wrap.
        let mut src = ScriptedSource::new([ok("1"), ok("1000"), ok("500")]);
        let _ = detect_with_base(
            &mut src,
            "SELECT id FROM public.users WHERE active",
            false,
            None,
        )
        .unwrap();
        assert!(src.seen_sql.iter().any(|s| s.contains("AS _rivet_rowcnt")));
        assert!(src.seen_sql.iter().any(|s| s.contains("AS _rivet")));
    }

    #[test]
    fn fast_path_chunk_by_days_also_unwraps() {
        // chunk_by_days path issues min + max; both should hit the fast path.
        let mut src = ScriptedSource::new([ok("2024-01-01"), ok("2024-12-31")]);
        let _ = detect_with_base(&mut src, "SELECT * FROM public.events", false, Some(7)).unwrap();
        for sql in &src.seen_sql {
            assert!(
                !sql.contains("_rivet") && !sql.contains("FROM ("),
                "chunk_by_days fast path must not wrap; got: {sql}"
            );
        }
    }

    #[test]
    fn fast_path_chunk_dense_count_unwraps() {
        let mut src = ScriptedSource::new([ok("500")]);
        let _ = detect_with_base(&mut src, "SELECT * FROM public.orders", true, None).unwrap();
        // Single SQL — the COUNT — must be unwrapped.
        assert_eq!(src.seen_sql.len(), 1);
        assert!(
            src.seen_sql[0].starts_with("SELECT COUNT(*) FROM public.orders"),
            "got: {}",
            src.seen_sql[0]
        );
    }

    #[test]
    fn chunk_count_larger_than_range_caps_at_range_size() {
        // min=1, max=5 (5 values), chunk_count=100 → chunk_size=1 → 5 chunks
        let mut src = ScriptedSource::new([ok("1"), ok("5"), ok("5")]);
        let chunks = detect_with_count(&mut src, 100).unwrap();
        assert_eq!(chunks.len(), 5, "got: {chunks:?}");
        assert_eq!(chunks[0], (1, 1));
        assert_eq!(chunks[4], (5, 5));
    }
}
