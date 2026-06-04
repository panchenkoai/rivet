//! Pure date-bucket math for value-based output partitioning
//! ([`crate::config::ExportConfig::partition_by`]).
//!
//! Given the `[min, max]` day span of a partition column and a granularity,
//! this produces the ordered, gap-free list of `col=value` buckets and their
//! half-open `[lo, hi)` date bounds. The bounds become a
//! `WHERE col >= lo AND col < hi` predicate; the label becomes the
//! Hive-style `col=value` path segment.
//!
//! This module is deliberately I/O-free and total so it can be exhaustively
//! unit-tested. The source round-trip (min/max query, NULL probe) and the
//! `ExportConfig` synthesis live in `crate::pipeline::partition_expand`.

use chrono::{Datelike, Months, NaiveDate};

use crate::config::{PartitionGranularity, SourceType};

/// Hive's conventional partition label for rows whose partition key is NULL.
/// Spark, Hive, and duckdb's `hive_partitioning` all recognise this token, so
/// a NULL bucket stays queryable instead of silently dropping rows.
pub(crate) const HIVE_NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// One partition bucket: a Hive `col=value` value label and its half-open
/// `[lo, hi)` date bounds (inclusive `lo`, exclusive `hi`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionRange {
    /// The value side of the `col=value` path segment, formatted per
    /// granularity (`2023-01-01` / `2023-01` / `2023`).
    pub(crate) label_value: String,
    /// Inclusive lower bound (`col >= lo`).
    pub(crate) lo: NaiveDate,
    /// Exclusive upper bound (`col < hi`).
    pub(crate) hi: NaiveDate,
}

/// Generate the ordered, gap-free list of partition buckets covering
/// `[min_day, max_day]` at `granularity`.
///
/// Buckets are contiguous: a day/month/year with no rows still yields a
/// bucket. The expansion layer decides whether to skip empties. Returns an
/// empty vec when `max_day < min_day` (e.g. the source has zero non-NULL
/// partition values).
pub(crate) fn generate_ranges(
    min_day: NaiveDate,
    max_day: NaiveDate,
    granularity: PartitionGranularity,
) -> Vec<PartitionRange> {
    if max_day < min_day {
        return Vec::new();
    }
    match granularity {
        PartitionGranularity::Day => day_ranges(min_day, max_day),
        PartitionGranularity::Month => month_ranges(min_day, max_day),
        PartitionGranularity::Year => year_ranges(min_day, max_day),
    }
}

/// Wrap `base_query` so it yields only rows in the half-open `[lo, hi)` date
/// range of the partition column — the per-bucket extraction query.
///
/// Bounds are emitted as `YYYY-MM-DD` date literals; both PostgreSQL and MySQL
/// coerce them against a `DATE` / `TIMESTAMP` / `TIMESTAMPTZ` column. For a
/// timestamptz column the literal is interpreted at the session time zone —
/// callers that need a fixed zone must pin it (`SET time_zone='+00:00'`).
pub(crate) fn build_range_query(
    base_query: &str,
    col: &str,
    range: &PartitionRange,
    source_type: SourceType,
) -> String {
    let q = crate::sql::quote_ident(source_type, col);
    format!(
        "SELECT * FROM ({base}) AS _rivet_part WHERE {q} >= '{lo}' AND {q} < '{hi}'",
        base = base_query,
        lo = range.lo.format("%Y-%m-%d"),
        hi = range.hi.format("%Y-%m-%d"),
    )
}

/// Wrap `base_query` for the NULL bucket — rows whose partition column is NULL,
/// which a range predicate can never match. These land in the Hive default
/// partition so they are never silently dropped.
pub(crate) fn build_null_query(base_query: &str, col: &str, source_type: SourceType) -> String {
    let q = crate::sql::quote_ident(source_type, col);
    format!("SELECT * FROM ({base_query}) AS _rivet_part WHERE {q} IS NULL")
}

/// `SELECT count(*) … WHERE {col} IS NULL` — non-zero ⇒ emit a NULL bucket.
pub(crate) fn build_null_count_query(base_query: &str, col: &str, source_type: SourceType) -> String {
    let q = crate::sql::quote_ident(source_type, col);
    format!("SELECT count(*) FROM ({base_query}) AS _rivet_nc WHERE {q} IS NULL")
}

fn day_ranges(min_day: NaiveDate, max_day: NaiveDate) -> Vec<PartitionRange> {
    let mut out = Vec::new();
    let mut cur = min_day;
    while cur <= max_day {
        // succ_opt() returns None only at NaiveDate::MAX — fall back to `cur`
        // and break below to avoid an infinite loop on a pathological bound.
        let hi = cur.succ_opt().unwrap_or(cur);
        out.push(PartitionRange {
            label_value: cur.format("%Y-%m-%d").to_string(),
            lo: cur,
            hi,
        });
        if hi == cur {
            break;
        }
        cur = hi;
    }
    out
}

fn month_start(d: NaiveDate) -> NaiveDate {
    // day-1 of the month always exists for any in-range year/month.
    NaiveDate::from_ymd_opt(d.year(), d.month(), 1).expect("month-start is always valid")
}

fn month_ranges(min_day: NaiveDate, max_day: NaiveDate) -> Vec<PartitionRange> {
    let mut out = Vec::new();
    let mut cur = month_start(min_day);
    let last = month_start(max_day);
    loop {
        let hi = cur.checked_add_months(Months::new(1)).unwrap_or(cur);
        out.push(PartitionRange {
            label_value: cur.format("%Y-%m").to_string(),
            lo: cur,
            hi,
        });
        if cur >= last || hi == cur {
            break;
        }
        cur = hi;
    }
    out
}

fn year_start(d: NaiveDate) -> NaiveDate {
    NaiveDate::from_ymd_opt(d.year(), 1, 1).expect("year-start is always valid")
}

fn year_ranges(min_day: NaiveDate, max_day: NaiveDate) -> Vec<PartitionRange> {
    let mut out = Vec::new();
    let mut cur = year_start(min_day);
    let last = year_start(max_day);
    loop {
        let hi = NaiveDate::from_ymd_opt(cur.year() + 1, 1, 1).unwrap_or(cur);
        out.push(PartitionRange {
            label_value: cur.format("%Y").to_string(),
            lo: cur,
            hi,
        });
        if cur >= last || hi == cur {
            break;
        }
        cur = hi;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn labels(ranges: &[PartitionRange]) -> Vec<String> {
        ranges.iter().map(|r| r.label_value.clone()).collect()
    }

    // ── day granularity ────────────────────────────────────────────────────

    #[test]
    fn day_three_day_span() {
        let r = generate_ranges(d("2023-01-01"), d("2023-01-03"), PartitionGranularity::Day);
        assert_eq!(labels(&r), ["2023-01-01", "2023-01-02", "2023-01-03"]);
        assert_eq!(r[0].lo, d("2023-01-01"));
        assert_eq!(r[0].hi, d("2023-01-02"));
        // half-open: last bucket's hi is the day AFTER max.
        assert_eq!(r[2].hi, d("2023-01-04"));
    }

    #[test]
    fn day_single_day_when_min_equals_max() {
        let r = generate_ranges(d("2023-06-15"), d("2023-06-15"), PartitionGranularity::Day);
        assert_eq!(labels(&r), ["2023-06-15"]);
        assert_eq!(r[0].hi, d("2023-06-16"));
    }

    #[test]
    fn day_crosses_month_boundary() {
        let r = generate_ranges(d("2023-01-31"), d("2023-02-01"), PartitionGranularity::Day);
        assert_eq!(labels(&r), ["2023-01-31", "2023-02-01"]);
    }

    #[test]
    fn day_crosses_leap_day() {
        let r = generate_ranges(d("2024-02-28"), d("2024-03-01"), PartitionGranularity::Day);
        assert_eq!(labels(&r), ["2024-02-28", "2024-02-29", "2024-03-01"]);
    }

    // ── month granularity ──────────────────────────────────────────────────

    #[test]
    fn month_spans_three_months_snaps_to_month_edges() {
        let r = generate_ranges(d("2023-01-15"), d("2023-03-05"), PartitionGranularity::Month);
        assert_eq!(labels(&r), ["2023-01", "2023-02", "2023-03"]);
        // first lo snaps back to the 1st; last hi is the 1st of the next month.
        assert_eq!(r[0].lo, d("2023-01-01"));
        assert_eq!(r[2].hi, d("2023-04-01"));
    }

    #[test]
    fn month_single_when_within_one_month() {
        let r = generate_ranges(d("2023-06-10"), d("2023-06-20"), PartitionGranularity::Month);
        assert_eq!(labels(&r), ["2023-06"]);
        assert_eq!(r[0].lo, d("2023-06-01"));
        assert_eq!(r[0].hi, d("2023-07-01"));
    }

    #[test]
    fn month_crosses_year_boundary() {
        let r = generate_ranges(d("2023-12-15"), d("2024-01-10"), PartitionGranularity::Month);
        assert_eq!(labels(&r), ["2023-12", "2024-01"]);
        assert_eq!(r[1].hi, d("2024-02-01"));
    }

    // ── year granularity ───────────────────────────────────────────────────

    #[test]
    fn year_spans_three_years() {
        let r = generate_ranges(d("2022-05-01"), d("2024-02-01"), PartitionGranularity::Year);
        assert_eq!(labels(&r), ["2022", "2023", "2024"]);
        assert_eq!(r[0].lo, d("2022-01-01"));
        assert_eq!(r[2].hi, d("2025-01-01"));
    }

    #[test]
    fn year_single() {
        let r = generate_ranges(d("2023-03-03"), d("2023-11-11"), PartitionGranularity::Year);
        assert_eq!(labels(&r), ["2023"]);
    }

    // ── edge cases ─────────────────────────────────────────────────────────

    #[test]
    fn empty_when_max_before_min() {
        let r = generate_ranges(d("2023-02-01"), d("2023-01-01"), PartitionGranularity::Day);
        assert!(r.is_empty());
    }

    #[test]
    fn ranges_are_contiguous_and_gap_free_day() {
        let r = generate_ranges(d("2023-01-01"), d("2023-01-10"), PartitionGranularity::Day);
        for w in r.windows(2) {
            // each bucket's hi is exactly the next bucket's lo — no gaps, no overlap.
            assert_eq!(w[0].hi, w[1].lo);
        }
    }

    // ── SQL builders ───────────────────────────────────────────────────────

    fn one_day(day: &str) -> PartitionRange {
        generate_ranges(d(day), d(day), PartitionGranularity::Day)
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn range_query_pg_is_half_open_and_quotes_ident() {
        let q = build_range_query(
            "SELECT * FROM events",
            "created_at",
            &one_day("2023-01-03"),
            SourceType::Postgres,
        );
        assert_eq!(
            q,
            "SELECT * FROM (SELECT * FROM events) AS _rivet_part \
             WHERE \"created_at\" >= '2023-01-03' AND \"created_at\" < '2023-01-04'"
        );
    }

    #[test]
    fn range_query_mysql_backtick_quotes() {
        let q = build_range_query(
            "SELECT * FROM events",
            "created_at",
            &one_day("2023-01-03"),
            SourceType::Mysql,
        );
        assert!(q.contains("WHERE `created_at` >= '2023-01-03' AND `created_at` < '2023-01-04'"));
    }

    #[test]
    fn null_query_filters_is_null() {
        let q = build_null_query("SELECT * FROM events", "created_at", SourceType::Postgres);
        assert_eq!(
            q,
            "SELECT * FROM (SELECT * FROM events) AS _rivet_part WHERE \"created_at\" IS NULL"
        );
    }

    #[test]
    fn null_count_query_filters_is_null() {
        // min/max aggregate SQL is shared (`crate::sql::aggregate_sql`) and
        // tested there; the NULL-count probe is partition-specific.
        assert!(
            build_null_count_query("SELECT * FROM events", "created_at", SourceType::Postgres)
                .contains("WHERE \"created_at\" IS NULL")
        );
    }
}
