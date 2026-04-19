//! Pure chunk math — no I/O, no database access.
//!
//! Date parsing, range generation, SQL query building, plan fingerprinting.
//! All functions in this module are deterministic and side-effect-free.

/// Parse a date string from the DB into a NaiveDate.
/// Handles DATE ('2023-01-01'), DATETIME ('2023-01-01 00:00:00'), and ISO-8601 variants.
pub(crate) fn parse_date_flexible(s: &str) -> Option<chrono::NaiveDate> {
    use chrono::NaiveDate;
    let s = s.trim();
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|dt| dt.date())
        })
        .or_else(|| {
            // ISO 8601 with T, optional fractional seconds
            let base = s.split('.').next().unwrap_or(s);
            let base = base.split('+').next().unwrap_or(base);
            chrono::NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S")
                .ok()
                .map(|dt| dt.date())
        })
}

pub fn generate_chunks(min: i64, max: i64, chunk_size: i64) -> Vec<(i64, i64)> {
    if max < min || chunk_size <= 0 {
        return vec![];
    }
    let mut chunks = Vec::new();
    let mut start = min;
    loop {
        // Saturating arithmetic: when `start` is within `chunk_size` of i64::MAX
        // the naive `start + chunk_size - 1` would overflow and panic. We clamp
        // at i64::MAX and then `.min(max)` produces the correct inclusive end.
        let end = start.saturating_add(chunk_size - 1).min(max);
        chunks.push((start, end));
        // Two exit conditions: the chunk reached `max`, or `end` hit i64::MAX
        // (next start would overflow). Either way no more chunks remain.
        if end == max || end == i64::MAX {
            break;
        }
        start = end + 1;
    }
    chunks
}

/// Synthetic ordinal column for `chunk_dense`; stripped before writing files.
pub(crate) const RIVET_CHUNK_RN_COL: &str = "_rivet_chunk_rn";

pub(crate) fn build_chunk_query_sql(
    base_query: &str,
    order_column: &str,
    start: i64,
    end: i64,
    chunk_dense: bool,
    chunk_by_days: bool,
    source_type: crate::config::SourceType,
) -> String {
    let quoted_col = crate::sql::quote_ident(source_type, order_column);

    if chunk_dense {
        return format!(
            "SELECT * FROM (SELECT _rivet_i.*, ROW_NUMBER() OVER (ORDER BY _rivet_i.{oc}) AS {rn} FROM ({bq}) AS _rivet_i) AS _rivet_w WHERE _rivet_w.{rn} BETWEEN {s} AND {e}",
            bq = base_query,
            oc = quoted_col,
            rn = RIVET_CHUNK_RN_COL,
            s = start,
            e = end,
        );
    }

    if chunk_by_days {
        // start/end are days since epoch; end is inclusive so the exclusive upper bound is end+1.
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is valid");
        let start_date = epoch + chrono::Duration::days(start);
        let end_date = epoch + chrono::Duration::days(end + 1);
        return format!(
            "SELECT * FROM ({base}) AS _rivet WHERE {col} >= '{start}' AND {col} < '{end}'",
            base = base_query,
            col = quoted_col,
            start = start_date.format("%Y-%m-%d"),
            end = end_date.format("%Y-%m-%d"),
        );
    }

    format!(
        "SELECT * FROM ({base}) AS _rivet WHERE {col} BETWEEN {start} AND {end}",
        base = base_query,
        col = quoted_col,
        start = start,
        end = end,
    )
}

pub(crate) fn chunk_plan_fingerprint(
    base_query: &str,
    chunk_column: &str,
    chunk_size: usize,
    chunk_dense: bool,
    chunk_by_days: Option<u32>,
) -> String {
    use xxhash_rust::xxh3::xxh3_64;
    let mut buf = String::with_capacity(base_query.len() + chunk_column.len() + 32);
    buf.push_str(base_query);
    buf.push('\x1f');
    buf.push_str(chunk_column);
    buf.push('\x1f');
    buf.push_str(&chunk_size.to_string());
    buf.push('\x1f');
    match chunk_by_days {
        Some(d) => buf.push_str(&format!("date_{}d", d)),
        None if chunk_dense => buf.push_str("dense_rn"),
        None => buf.push_str("range"),
    }
    format!("{:016x}", xxh3_64(buf.as_bytes()))
}

/// Parse a raw scalar string from the database as i64, accepting float representations.
pub(super) fn parse_scalar_i64(raw: &str) -> crate::error::Result<i64> {
    let t = raw.trim();
    t.parse::<i64>()
        .or_else(|_| t.parse::<f64>().map(|x| x as i64))
        .map_err(|_| anyhow::anyhow!("invalid numeric scalar: {:?}", t))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_chunks() {
        let chunks = generate_chunks(1, 100, 30);
        assert_eq!(chunks, vec![(1, 30), (31, 60), (61, 90), (91, 100)]);
    }

    #[test]
    fn test_generate_chunks_exact() {
        let chunks = generate_chunks(0, 99, 50);
        assert_eq!(chunks, vec![(0, 49), (50, 99)]);
    }

    #[test]
    fn test_generate_chunks_single() {
        let chunks = generate_chunks(1, 10, 100);
        assert_eq!(chunks, vec![(1, 10)]);
    }

    #[test]
    fn test_generate_chunks_empty() {
        assert!(generate_chunks(10, 5, 100).is_empty());
    }

    #[test]
    fn test_build_chunk_query_range_mode() {
        let q = build_chunk_query_sql(
            "SELECT id FROM t",
            "id",
            1,
            100,
            false,
            false,
            crate::config::SourceType::Postgres,
        );
        // Column name is quoted; numeric bounds are not
        assert!(q.contains("WHERE \"id\" BETWEEN 1 AND 100"), "got: {}", q);
        assert!(!q.contains("ROW_NUMBER()"), "got: {}", q);
    }

    #[test]
    fn test_build_chunk_query_range_mode_mysql() {
        let q = build_chunk_query_sql(
            "SELECT id FROM t",
            "id",
            1,
            100,
            false,
            false,
            crate::config::SourceType::Mysql,
        );
        assert!(q.contains("WHERE `id` BETWEEN 1 AND 100"), "got: {}", q);
    }

    #[test]
    fn test_build_chunk_query_dense_mode() {
        let q = build_chunk_query_sql(
            "SELECT id FROM t",
            "id",
            1,
            5000,
            true,
            false,
            crate::config::SourceType::Postgres,
        );
        assert!(q.contains("ROW_NUMBER()"), "got: {}", q);
        assert!(q.contains(RIVET_CHUNK_RN_COL), "got: {}", q);
        assert!(q.contains("BETWEEN 1 AND 5000"), "got: {}", q);
    }

    #[test]
    fn test_parse_date_flexible_date_only() {
        let d = parse_date_flexible("2023-06-15").unwrap();
        assert_eq!(d.to_string(), "2023-06-15");
    }

    #[test]
    fn test_parse_date_flexible_datetime() {
        let d = parse_date_flexible("2023-06-15 14:32:00").unwrap();
        assert_eq!(d.to_string(), "2023-06-15");
    }

    #[test]
    fn test_parse_date_flexible_iso8601() {
        let d = parse_date_flexible("2023-06-15T14:32:00").unwrap();
        assert_eq!(d.to_string(), "2023-06-15");
    }

    #[test]
    fn test_parse_date_flexible_iso8601_micros() {
        let d = parse_date_flexible("2023-06-15T14:32:00.123456").unwrap();
        assert_eq!(d.to_string(), "2023-06-15");
    }

    #[test]
    fn test_parse_date_flexible_invalid() {
        assert!(parse_date_flexible("not-a-date").is_none());
        assert!(parse_date_flexible("").is_none());
        assert!(parse_date_flexible("12345").is_none());
    }

    #[test]
    fn test_build_chunk_query_date_mode() {
        // days since epoch: 2023-01-01 = 19358, 2023-01-07 = 19364
        // chunk start=19358, end=19364 → >= '2023-01-01' AND < '2023-01-08'
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let start = (chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap() - epoch).num_days();
        let end = (chrono::NaiveDate::from_ymd_opt(2023, 1, 7).unwrap() - epoch).num_days();
        let q = build_chunk_query_sql(
            "SELECT * FROM orders",
            "created_at",
            start,
            end,
            false,
            true,
            crate::config::SourceType::Postgres,
        );
        assert!(q.contains(">= '2023-01-01'"), "got: {q}");
        assert!(q.contains("< '2023-01-08'"), "got: {q}"); // end+1 day exclusive
        assert!(!q.contains("BETWEEN"), "should use >= AND <, got: {q}");
    }

    #[test]
    fn test_build_chunk_query_date_mode_single_day() {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let day = (chrono::NaiveDate::from_ymd_opt(2024, 3, 15).unwrap() - epoch).num_days();
        let q = build_chunk_query_sql(
            "SELECT * FROM t",
            "ts",
            day,
            day,
            false,
            true,
            crate::config::SourceType::Postgres,
        );
        assert!(q.contains(">= '2024-03-15'"), "got: {q}");
        assert!(q.contains("< '2024-03-16'"), "got: {q}");
    }

    // ─── Property-style invariants (QA backlog Task 3.2) ────────────────────
    //
    // Core invariants checked on every non-empty `generate_chunks` output.
    // Encoded as a helper + wide grid sweep so every boundary condition
    // the planner encounters in practice is exercised deterministically.

    fn assert_chunk_invariants(min: i64, max: i64, size: i64, chunks: &[(i64, i64)]) {
        if max < min || size <= 0 {
            assert!(chunks.is_empty(), "invalid input must yield []: {chunks:?}");
            return;
        }

        assert!(
            !chunks.is_empty(),
            "valid range must yield at least one chunk"
        );
        assert_eq!(chunks.first().unwrap().0, min, "first chunk starts at min");
        assert_eq!(chunks.last().unwrap().1, max, "last chunk ends at max");

        for (i, (s, e)) in chunks.iter().enumerate() {
            assert!(s <= e, "chunk {i} well-formed");
            if i > 0 {
                let (_, pe) = chunks[i - 1];
                assert_eq!(*s, pe + 1, "chunks are adjacent, no gap or overlap");
            }
        }

        for (i, (s, e)) in chunks.iter().enumerate() {
            let len = e - s + 1;
            if i + 1 < chunks.len() {
                assert_eq!(len, size, "non-final chunks have exactly size elements");
            } else {
                assert!(len >= 1 && len <= size, "final chunk is 1..=size");
            }
        }

        let total: i64 = chunks.iter().map(|(s, e)| e - s + 1).sum();
        assert_eq!(total, max - min + 1, "sum of chunks equals total range");
    }

    #[test]
    fn generate_chunks_invariants_over_grid() {
        let mins: [i64; 6] = [-50, -1, 0, 1, 100, 1_000_000];
        let sizes: [i64; 8] = [1, 2, 3, 7, 10, 33, 100, 10_000];
        for &min in &mins {
            for &size in &sizes {
                for delta in 0..=(4 * size) {
                    let max = min + delta - 1;
                    let chunks = generate_chunks(min, max, size);
                    assert_chunk_invariants(min, max, size, &chunks);
                }
            }
        }
    }

    #[test]
    fn generate_chunks_empty_on_invalid_inputs() {
        assert!(generate_chunks(10, 5, 10).is_empty());
        assert!(generate_chunks(1, 100, 0).is_empty());
        assert!(generate_chunks(1, 100, -5).is_empty());
    }

    #[test]
    fn generate_chunks_single_element_range() {
        assert_eq!(generate_chunks(42, 42, 100), vec![(42, 42)]);
        assert_eq!(generate_chunks(42, 42, 1), vec![(42, 42)]);
    }

    #[test]
    fn generate_chunks_chunk_size_larger_than_range() {
        let chunks = generate_chunks(10, 20, 10_000);
        assert_eq!(chunks, vec![(10, 20)]);
        assert_chunk_invariants(10, 20, 10_000, &chunks);
    }

    #[test]
    fn generate_chunks_boundary_exact_multiple() {
        let chunks = generate_chunks(1, 100, 25);
        assert_eq!(chunks, vec![(1, 25), (26, 50), (51, 75), (76, 100)]);
        assert_chunk_invariants(1, 100, 25, &chunks);
    }

    #[test]
    fn generate_chunks_boundary_off_by_one() {
        let chunks = generate_chunks(1, 101, 25);
        assert_eq!(chunks.last(), Some(&(101, 101)));
        assert_chunk_invariants(1, 101, 25, &chunks);
    }

    /// Regression for the i64::MAX overflow that crashed `start + chunk_size - 1`
    /// before the saturating fix. See QA backlog Task 3.2 / 4A.2.
    #[test]
    fn generate_chunks_does_not_overflow_on_near_i64_max() {
        let min = i64::MAX - 10;
        let max = i64::MAX;
        let chunks = generate_chunks(min, max, 3);
        assert_chunk_invariants(min, max, 3, &chunks);
    }

    /// Extreme boundary combinations: each triple must either succeed or
    /// terminate immediately (empty).  Never panic.  QA backlog Task 4A.2.
    #[test]
    fn generate_chunks_does_not_panic_on_extreme_boundaries() {
        // Every entry generates at most a small bounded number of chunks,
        // otherwise this test would hang.  "Wide range × tiny chunk_size" is
        // out of scope for a fuzz smoke — it needs a planner-level cap.
        let triples: &[(i64, i64, i64)] = &[
            (0, 0, 1),
            (i64::MIN, i64::MIN + 1, 1),
            (i64::MIN, i64::MIN, 1),
            (i64::MIN, i64::MAX, i64::MAX),
            (i64::MAX - 2, i64::MAX, 1),
            (i64::MAX - 2, i64::MAX, 10),
            (i64::MAX - 2, i64::MAX, i64::MAX),
            (-10, 10, 3),
            (0, 0, i64::MAX),
            (0, 0, i64::MIN),
            (5, 5, -1),
        ];
        for &(min, max, size) in triples {
            let _ = generate_chunks(min, max, size);
        }
    }
}
