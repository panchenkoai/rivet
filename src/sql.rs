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
}
