//! SQL identifier quoting helpers.
//!
//! Column and table names that come from user configuration must never be
//! interpolated raw into query strings — doing so opens a SQL injection vector
//! even when `start`/`end` bounds are typed integers.  Use `quote_ident` for
//! every identifier that is not a literal written by the developer.

use crate::config::SourceType;

/// Quote a SQL identifier (column or table name) for the target source dialect.
///
/// - **PostgreSQL** — double-quoted: `"column_name"`.  Internal `"` characters are
///   escaped by doubling (`"col""name"`).
/// - **MySQL** — backtick-quoted: `` `column_name` ``.  Internal backticks are
///   escaped by doubling (`` `col``name` ``).
///
/// # Example
/// ```
/// use rivet::config::SourceType;
/// // internal use only; not part of the public crate API
/// ```
pub(crate) fn quote_ident(source_type: SourceType, name: &str) -> String {
    match source_type {
        SourceType::Postgres => format!("\"{}\"", name.replace('"', "\"\"")),
        SourceType::Mysql => format!("`{}`", name.replace('`', "``")),
    }
}

/// Parse a `NaiveDate` out of a DB scalar (typically `min`/`max` of a
/// date/timestamp column returned by `query_scalar`).
///
/// Single source of truth for both the chunked date-range path
/// (`pipeline::chunked`) and value-based partitioning (`plan::partition`), which
/// live in different layers and so must share this through the `sql` leaf rather
/// than duplicate it.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_plain_identifier() {
        assert_eq!(quote_ident(SourceType::Postgres, "id"), "\"id\"");
        assert_eq!(
            quote_ident(SourceType::Postgres, "created_at"),
            "\"created_at\""
        );
    }

    #[test]
    fn postgres_escapes_internal_double_quotes() {
        assert_eq!(
            quote_ident(SourceType::Postgres, "col\"name"),
            "\"col\"\"name\""
        );
    }

    #[test]
    fn mysql_plain_identifier() {
        assert_eq!(quote_ident(SourceType::Mysql, "id"), "`id`");
        assert_eq!(quote_ident(SourceType::Mysql, "created_at"), "`created_at`");
    }

    #[test]
    fn mysql_escapes_internal_backticks() {
        assert_eq!(quote_ident(SourceType::Mysql, "col`name"), "`col``name`");
    }

    fn d(s: &str) -> chrono::NaiveDate {
        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn parse_date_flexible_handles_db_scalar_forms() {
        assert_eq!(parse_date_flexible("2023-06-15"), Some(d("2023-06-15")));
        assert_eq!(parse_date_flexible("2023-06-15 14:32:00"), Some(d("2023-06-15")));
        assert_eq!(parse_date_flexible("2023-06-15T14:32:00"), Some(d("2023-06-15")));
        assert_eq!(parse_date_flexible("2023-06-15T14:32:00.123456"), Some(d("2023-06-15")));
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
}
