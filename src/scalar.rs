//! Parse DB scalar strings into typed chunk/partition bounds.
//!
//! `Source::query_scalar` returns a column's `min` / `max` as a `String` in the
//! source's text form. The chunk planner (`pipeline::chunked`) and value-based
//! partitioning (`pipeline::partition_expand`) both need that string as a typed
//! bound — an `i64` for range chunking, a `NaiveDate` for day chunking. Those
//! callers live in different submodules, so the parsers live here in a shared
//! leaf — the inbound mirror of `crate::sql`, which builds the outbound SQL the
//! same scalars are read back from — rather than being duplicated per caller.

/// Parse a `NaiveDate` out of a DB scalar (typically `min`/`max` of a
/// date/timestamp column returned by `query_scalar`).
///
/// Single source of truth for both the chunked date-range path
/// (`pipeline::chunked`) and value-based partitioning
/// (`pipeline::partition_expand`), which live in different submodules and so
/// share this leaf rather than duplicate it.
///
/// Accepts, in order: `DATE` (`2023-01-01`), `DATETIME`
/// (`2023-01-01 14:32:00`), ISO-8601 with `T` and optional fractional seconds,
/// and finally a lenient leading-`YYYY-MM-DD` fallback that covers the
/// PostgreSQL `timestamptz` text form (`2023-01-01 14:32:00.123456+00`).
/// Returns `None` for an empty / unparseable value.
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
            // ISO 8601 with T, optional fractional seconds / offset.
            let base = s.split('.').next().unwrap_or(s);
            let base = base.split('+').next().unwrap_or(base);
            chrono::NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M:%S")
                .ok()
                .map(|dt| dt.date())
        })
        .or_else(|| {
            // Lenient fallback: any value whose first 10 chars are `YYYY-MM-DD`
            // (space-separated fractional/offset timestamptz, etc.).
            s.get(..10)
                .and_then(|head| NaiveDate::parse_from_str(head, "%Y-%m-%d").ok())
        })
}

/// Parse a raw scalar string from the database as `i64`, accepting float
/// representations (some drivers stringify an integer `min`/`max` as `42.0`).
pub(crate) fn parse_scalar_i64(raw: &str) -> crate::error::Result<i64> {
    let t = raw.trim();
    t.parse::<i64>()
        .or_else(|_| t.parse::<f64>().map(|x| x as i64))
        .map_err(|_| anyhow::anyhow!("invalid numeric scalar: {:?}", t))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> chrono::NaiveDate {
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn parse_date_flexible_handles_db_scalar_forms() {
        assert_eq!(parse_date_flexible("2023-06-15"), Some(d("2023-06-15")));
        assert_eq!(
            parse_date_flexible("2023-06-15 14:32:00"),
            Some(d("2023-06-15"))
        );
        assert_eq!(
            parse_date_flexible("2023-06-15T14:32:00"),
            Some(d("2023-06-15"))
        );
        assert_eq!(
            parse_date_flexible("2023-06-15T14:32:00.123456"),
            Some(d("2023-06-15"))
        );
        // PostgreSQL timestamptz text form — covered by the lenient fallback.
        assert_eq!(
            parse_date_flexible("2024-12-30 00:00:00.123456+00"),
            Some(d("2024-12-30"))
        );
    }

    #[test]
    fn parse_date_flexible_rejects_non_dates() {
        assert!(parse_date_flexible("").is_none());
        assert!(parse_date_flexible("not-a-date").is_none());
        assert!(parse_date_flexible("12345").is_none());
    }

    #[test]
    fn parse_scalar_i64_accepts_int_and_float_forms() {
        assert_eq!(parse_scalar_i64("42").unwrap(), 42);
        assert_eq!(parse_scalar_i64("  -7 ").unwrap(), -7);
        // Some drivers stringify an integer min/max as a float.
        assert_eq!(parse_scalar_i64("42.0").unwrap(), 42);
        assert_eq!(parse_scalar_i64("42.9").unwrap(), 42); // truncates toward zero
    }

    #[test]
    fn parse_scalar_i64_rejects_non_numeric() {
        assert!(parse_scalar_i64("").is_err());
        assert!(parse_scalar_i64("not-a-number").is_err());
    }
}
