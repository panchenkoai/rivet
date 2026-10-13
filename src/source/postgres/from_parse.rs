//! The relation of a simple Postgres `SELECT … FROM <table>` query, as a `regclass` literal.
//!
//! Used by the catalog-hints path ([`super::pg_fetch_numeric_catalog_hints`])
//! to ask `pg_catalog` for the numeric `(precision, scale)` of each column.
//! The query text is read by [`crate::sql::single_relation`]; anything but one
//! `[schema.]table` gives `None`, and the caller falls back to the result set.

/// The `<schema>.<table>` (or `<table>`) literal to cast to `regclass`: a part the query quoted stays quoted. `None` for a join, a comma list, a subquery or a name of more than two parts.
pub(super) fn try_parse_pg_simple_from_regclass_literal(query: &str) -> Option<String> {
    let parts = crate::sql::single_relation(query, true)?;
    let segment = |p: &crate::sql::NamePart| match p.quoted {
        true => format!("\"{}\"", p.name.replace('"', "\"\"")),
        false => p.name.clone(),
    };
    (parts.len() <= 2).then(|| parts.iter().map(segment).collect::<Vec<_>>().join("."))
}

#[cfg(test)]
mod tests {
    use super::try_parse_pg_simple_from_regclass_literal;

    #[test]
    fn parse_simple_from_unqualified_table_alias_where() {
        let q = "SELECT id, amount\nFROM transactions t\nWHERE x = 1";
        assert_eq!(
            try_parse_pg_simple_from_regclass_literal(q).as_deref(),
            Some("transactions")
        );
    }

    #[test]
    fn parse_simple_from_qualified() {
        let q = "SELECT id FROM public.orders WHERE 1=1";
        assert_eq!(
            try_parse_pg_simple_from_regclass_literal(q).as_deref(),
            Some("public.orders")
        );
    }

    #[test]
    fn parse_only_prefix() {
        let q = "SELECT * FROM ONLY inventory.items";
        assert_eq!(
            try_parse_pg_simple_from_regclass_literal(q).as_deref(),
            Some("inventory.items")
        );
    }

    #[test]
    fn join_rejected() {
        assert!(
            try_parse_pg_simple_from_regclass_literal("SELECT * FROM a INNER JOIN b USING (id)")
                .is_none()
        );
    }

    #[test]
    fn subquery_from_rejected() {
        assert!(
            try_parse_pg_simple_from_regclass_literal("SELECT * FROM (SELECT * FROM foo) s")
                .is_none()
        );
    }

    /// A quoted part keeps its quotes for the `regclass` cast, a name of three parts is not one, and a comment is never the relation.
    #[test]
    fn the_regclass_literal_quotes_what_the_query_quoted_and_skips_comments() {
        let lit = |q: &str| try_parse_pg_simple_from_regclass_literal(q);
        assert_eq!(lit("SELECT a FROM \"My T\"").as_deref(), Some("\"My T\""));
        assert_eq!(
            lit("SELECT a FROM s.\"a\"\"b\" x").as_deref(),
            Some("s.\"a\"\"b\"")
        );
        assert_eq!(lit("SELECT a FROM db.s.t"), None);
        assert_eq!(lit("SELECT a FROM t;").as_deref(), Some("t"));
        for q in [
            "SELECT amount -- rows from the last week\nFROM t",
            "SELECT amount\n-- taken from legacy\nFROM t",
            "SELECT amount /* from b */ FROM t",
            "select amount from t -- from b",
            "SELECT amount FROM t /* from b */",
        ] {
            assert_eq!(lit(q).as_deref(), Some("t"), "{q:?}");
        }
    }
}
