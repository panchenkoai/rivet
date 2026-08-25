//! Resolved capture identity — ONE answer to "which relation does this
//! `table:` string mean", instead of six.
//!
//! ## Why this module exists
//!
//! A `mode: cdc` export's `table:` string was carried unchanged from config to six
//! destinations, each interpreting it under different identifier rules:
//!
//! | site | rule |
//! |---|---|
//! | `sink::table_matches` — the routing AUTHORITY | full string, else `split_once('.')`, byte-exact |
//! | pg `check_configured_tables_are_routable` | `to_regclass` — PostgreSQL's parser, FOLDS case |
//! | `CdcSchemaResolver::resolve` | `SELECT * FROM {table}` — FOLDS |
//! | pg `row_image` | `rsplit('.')` then match the bare name in ANY schema |
//! | mysql `check_configured_tables_are_routable` | `TABLE_SCHEMA = DATABASE()` — one database |
//! | `enrich_schema_and_table` | `split_once('.')`, else `SELECT DATABASE()` |
//!
//! `classify_routing` exists ONLY to notice that the first two disagree — its
//! longest arm is literally `if !table_matches(cfg, resolved_schema, resolved_table)`.
//! Six repairs landed in that one gap on 2026-08-25 alone, and the last of them
//! found that on MySQL no catalog check CAN close it: `information_schema` is
//! filtered by privilege while `REPLICATION SLAVE` hands over the whole server's
//! binlog, so rivet captured a database its own user could not see.
//!
//! Debezium does not have this class because it cannot be expressed there:
//! `table.include.list` is `database.table`, always, and on PostgreSQL the filter is
//! a server-side PUBLICATION. rivet accepts a bare name only because a MongoDB
//! collection has no schema qualifier and may itself contain dots — one compromise,
//! inherited by three engines that do not need it.
//!
//! ## What this changes
//!
//! Resolution happens ONCE, before the stream opens, and yields a relation or an
//! error. Routing then compares the resolved pair exactly. The fold mismatch, the
//! bare-name ambiguity and the quoted identifier stop being hazards to GUARD and
//! become resolution FAILURES — which is the difference between a guard someone has
//! to remember to write and an outcome the type makes unavoidable.
//!
//! `configured` is kept beside the resolved pair on purpose: it is the destination
//! sub-prefix (`cdc_job::dest_for_table`), so resolving must not move anybody's
//! output path.

use crate::error::Result;

/// One relation the catalog reports for a configured name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogMatch {
    pub schema: String,
    pub table: String,
}

/// A configured `table:` resolved to exactly one relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedTable {
    /// The string the operator wrote. Stays the destination sub-prefix so
    /// resolution never moves an existing output path.
    pub configured: String,
    /// The catalog's own spelling — what wire events will carry.
    pub schema: String,
    pub table: String,
}

/// Resolve one configured `table:` against what the catalog reported for it.
///
/// The pure half — the query that produces `matches` is per-engine and lives with
/// the adapter. Everything that DECIDES is here, so a mutant can grade it.
///
/// Precedence mirrors `sink::table_matches`, deliberately: an exact WHOLE-name
/// match wins over a `schema.table` split. That order is not a nicety — a MongoDB
/// collection may be named `events.v2`, and splitting it would look up a collection
/// that does not exist. It is also why a bare name is accepted at all, and therefore
/// why the ambiguity below has to be caught rather than defined away.
pub(crate) fn resolve_captured_table(
    configured: &str,
    matches: &[CatalogMatch],
) -> Result<CapturedTable> {
    // A QUALIFIED name selects its own schema and cannot be ambiguous — but only if
    // the whole string is not itself a relation name (the dotted-collection case).
    let whole: Vec<&CatalogMatch> = matches.iter().filter(|m| m.table == configured).collect();
    let qualified: Vec<&CatalogMatch> = match configured.split_once('.') {
        Some((sch, tbl)) => matches
            .iter()
            .filter(|m| m.schema == sch && m.table == tbl)
            .collect(),
        None => Vec::new(),
    };

    let chosen: Vec<&CatalogMatch> = if !whole.is_empty() {
        whole
    } else if !qualified.is_empty() {
        qualified
    } else {
        matches.iter().collect()
    };

    match chosen.as_slice() {
        [] => anyhow::bail!(
            "cdc: `{configured}` matches no relation the source reports. Capture would \
             open a stream and route nothing — every event dropped by the routing \
             filter, silently. Check the name, and qualify it (`schema.table`) if it \
             lives outside the connection's default schema."
        ),
        [one] => Ok(CapturedTable {
            configured: configured.to_string(),
            schema: one.schema.clone(),
            table: one.table.clone(),
        }),
        many => {
            let listing = many
                .iter()
                .map(|m| format!("{}.{}", m.schema, m.table))
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::bail!(
                "cdc: `{configured}` is unqualified and {} relations share that name \
                 ({listing}). Routing matches a bare name in ANY schema, so all of them \
                 land in this export — and because images are matched by column NAME, a \
                 foreign row is written under THIS table's names when the arity matches, \
                 or as an all-NULL row when it does not. Neither is recoverable from the \
                 output. Qualify it (`schema.{configured}`) to capture the one you mean.",
                many.len()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(schema: &str, table: &str) -> CatalogMatch {
        CatalogMatch {
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// The ordinary case, and the one that must not regress: a bare name over
    /// exactly one relation resolves to the CATALOG's spelling while keeping the
    /// operator's string as the destination prefix.
    #[test]
    fn a_bare_name_over_one_relation_resolves_to_the_catalog_spelling() {
        let got = resolve_captured_table("orders", &[m("public", "orders")])
            .expect("one match is the ordinary case");
        assert_eq!(got.schema, "public");
        assert_eq!(got.table, "orders");
        assert_eq!(
            got.configured, "orders",
            "the operator's string stays the destination sub-prefix — resolving must \
             not move an existing output path"
        );
    }

    /// The FOLD case, which `classify_routing` exists to detect today: the operator
    /// writes `mixedcase`, the catalog spells it `MixedCase`, and wire events carry
    /// the catalog's spelling. Resolution takes the catalog's answer, so routing
    /// stops needing a fold-mismatch guard at all.
    #[test]
    fn resolution_takes_the_catalogs_case_not_the_configured_one() {
        let got = resolve_captured_table("mixedcase", &[m("public", "MixedCase")])
            .expect("the catalog found it");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("public", "MixedCase"),
            "wire events carry the catalog's spelling; routing on the CONFIGURED one \
             is the mismatch classify_routing had to notice"
        );
        assert_eq!(got.configured, "mixedcase");
    }

    /// A QUALIFIED name names one relation whatever else exists — the ambiguity
    /// below cannot apply to it, and refusing it would break every correct config.
    #[test]
    fn a_qualified_name_is_never_ambiguous() {
        let got = resolve_captured_table(
            "archive.orders",
            &[m("archive", "orders"), m("public", "orders")],
        )
        .expect("a qualified name selects its own schema");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("archive", "orders")
        );
    }

    /// The defect this module removes, twice measured: a bare name matching more
    /// than one relation. On PostgreSQL the foreign row is written under THIS
    /// table's column names; on MySQL the same, from a database the connection
    /// cannot even see.
    #[test]
    fn a_bare_name_over_two_relations_is_a_resolution_failure() {
        let err =
            resolve_captured_table("orders", &[m("public", "orders"), m("archive", "orders")])
                .expect_err("two relations means the config does not name one");
        let msg = err.to_string();
        assert!(
            msg.contains("public.orders") && msg.contains("archive.orders"),
            "name BOTH — an operator qualifies the name only if they know which one \
             they meant: {msg}"
        );
        assert!(
            msg.contains("archive.orders") && msg.contains("Qualify"),
            "and hand over the fix in the form they must type: {msg}"
        );
    }

    /// Nothing matched. Today this surfaces as a routing filter that silently drops
    /// every event; as a resolution failure it is one loud line before the stream
    /// opens.
    #[test]
    fn a_name_the_catalog_does_not_know_is_a_resolution_failure() {
        let err = resolve_captured_table("ghost", &[]).expect_err("no relation, no capture");
        assert!(
            err.to_string().contains("ghost"),
            "name what could not be resolved: {err}"
        );
    }

    /// PRECEDENCE, where both readings have a catalog match. The test below feeds a
    /// dotted name that only ONE reading can satisfy, so it passes whichever order
    /// the resolver uses — MEASURED: swapping the precedence left all six tests
    /// green. A fixture that cannot express the ambiguity cannot grade the rule that
    /// resolves it.
    ///
    /// Here `events.v2` is BOTH a collection named `events.v2` in schema `app` AND a
    /// relation `v2` in a schema called `events`. Whole-name wins, because that is
    /// what `sink::table_matches` does and routing is the authority: resolving to
    /// `events.v2` while the router matches `app`.`events.v2` would send every event
    /// to a table nobody configured.
    #[test]
    fn a_whole_name_match_wins_over_a_schema_dot_table_split() {
        let got = resolve_captured_table("events.v2", &[m("app", "events.v2"), m("events", "v2")])
            .expect("both readings match, and precedence must pick one");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("app", "events.v2"),
            "the WHOLE name wins — `sink::table_matches` tries the full string first,              and a resolver that disagrees with the router sends events to a relation              nobody configured"
        );
    }

    /// A dotted MongoDB collection is not a qualified name — `a.b.c` is a legal
    /// collection name, and this is the whole reason a bare name is accepted at all.
    /// Resolution must take the catalog's word rather than splitting on the dot.
    #[test]
    fn a_dotted_collection_name_resolves_whole() {
        let got = resolve_captured_table("events.v2", &[m("app", "events.v2")])
            .expect("the catalog reported it as one collection");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("app", "events.v2"),
            "splitting `events.v2` into schema `events` would look up a collection \
             that does not exist — the dotted-name case is why bare names exist"
        );
    }
}
