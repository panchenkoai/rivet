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
    /// Can this relation actually CARRY a change event?
    ///
    /// The engine's query decides, because only it knows its own catalog: a
    /// PostgreSQL view, matview, foreign table, UNLOGGED or TEMP relation writes no
    /// logical-decoding record; a MySQL VIEW never appears in a binlog TABLE_MAP.
    ///
    /// This is the difference between an AMBIGUITY and a NOTE, and getting it wrong
    /// is an outage either way. MEASURED: counting non-capturable twins as candidates
    /// made an UNLOGGED table in another schema hard-fail a healthy config — and a
    /// STRANGER's `CREATE TEMP TABLE` of the same name did too, so capture flapped
    /// green/red on a config nobody had touched. Counting them as nothing lost the
    /// truthful message in the other direction: a VIEW sharing the name is very often
    /// what the operator MEANT, and silence sends them hunting.
    ///
    /// So: two or more CAPTURABLE relations is a refusal; a non-capturable twin rides
    /// the message as context.
    pub capturable: bool,
    /// What the catalog calls it — `BASE TABLE`, `VIEW`, a PostgreSQL `relkind`.
    /// Carried because the ALTERNATIVE is a second catalog read under a different
    /// predicate, and two reads of one fact can name two different relations:
    /// MEASURED on MySQL, where the identity query scanned every schema for a BASE
    /// TABLE while the kind query pinned `TABLE_SCHEMA = DATABASE()`, so a VIEW
    /// `rivet.vv` beside a table `other_db.vv` had resolution name one and
    /// classification refuse the other. Reporting the kind here also makes the
    /// ambiguity message TRUE — "could mean the view or the table" is what the
    /// operator needs, not a silent pick of whichever the filter left standing.
    pub kind: String,
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
    /// Set when the configured name ALSO matched relations that cannot carry an
    /// event. Resolution succeeded — only one candidate was capturable — but the
    /// operator may well have meant the view, and silence sends them hunting. The
    /// caller logs this at `warn`.
    pub note: Option<String>,
}

/// Resolve one configured `table:` against what the catalog reported for it.
///
/// The DIVISION OF KNOWLEDGE is the design: the per-engine catalog query answers
/// "which relations could this string mean under every rule live on this engine"
/// (PostgreSQL: what `to_regclass` folds to AND what the byte-exact router would
/// capture; Mongo: the collection literally named), and this pure function answers
/// only 0 / 1 / many. Engine differences live with engines; the decision has one
/// home a mutant can grade.
///
/// MANY is a refusal, always — there is no precedence. The first cut ranked a
/// whole-name match above a `schema.table` split, mirroring the router, and the
/// folded-twin fixture proved that wrong on its first live run: `table: MixedCase`
/// with both `mixedcase` and `"MixedCase"` present resolved "cleanly" to the
/// byte-exact one while PostgreSQL's own parser reads the same string as the OTHER
/// relation. Two systems disagreeing about one string is not a tie to break — it is
/// the ambiguity this seam exists to surface, and only the operator knows which
/// relation they meant.
pub(crate) fn resolve_captured_table(
    configured: &str,
    matches: &[CatalogMatch],
) -> Result<CapturedTable> {
    // One relation reported through several rules is one relation: the PG query
    // reaches a row via up to four OR-arms, and a set that counts it twice would
    // refuse a perfectly unambiguous config.
    let mut distinct: Vec<&CatalogMatch> = Vec::new();
    for m in matches {
        if !distinct.iter().any(|d| **d == *m) {
            distinct.push(m);
        }
    }

    // Only relations that can CARRY an event compete for the identity. A twin that
    // emits nothing cannot put its rows in this export, so it is context, not a
    // collision.
    let (live, inert): (Vec<&CatalogMatch>, Vec<&CatalogMatch>) =
        distinct.iter().partition(|m| m.capturable);
    let note = if inert.is_empty() {
        String::new()
    } else {
        format!(
            " (the name also matches {}, which cannot carry a change event — if that \
             is the one you meant, capture is not possible for it)",
            inert
                .iter()
                .map(|m| format!("{}.{} [{}]", m.schema, m.table, m.kind))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let distinct: Vec<&CatalogMatch> = if live.is_empty() { distinct } else { live };

    match distinct.as_slice() {
        [] => anyhow::bail!(
            "cdc: `{configured}` matches no relation the source reports. Capture would \
             open a stream and route nothing — every event dropped by the routing \
             filter, silently. Check the name, and qualify it (`schema.table`) if it \
             lives outside the connection's default schema.{note}"
        ),
        [one] => Ok(CapturedTable {
            configured: configured.to_string(),
            schema: one.schema.clone(),
            table: one.table.clone(),
            note: (!note.is_empty()).then(|| {
                format!(
                    "`{configured}` resolves to {}.{}, but{note}",
                    one.schema, one.table
                )
            }),
        }),
        many => {
            let listing = many
                .iter()
                .map(|m| {
                    if m.kind.is_empty() {
                        format!("{}.{}", m.schema, m.table)
                    } else {
                        format!("{}.{} [{}]", m.schema, m.table, m.kind)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::bail!(
                "cdc: `{configured}` could mean {} different relations ({listing}) — the \
                 engine's own parser and rivet's byte-exact routing do not agree on one, \
                 or more than one schema holds the name. All of them would land in this \
                 export, and because images are matched by column NAME, a foreign row is \
                 written under THIS table's names when the arity matches, or as an \
                 all-NULL row when it does not. Neither is recoverable from the output. \
                 Qualify it (`schema.{configured}`, quoted as the catalog spells it) to \
                 capture the one you mean.{note}",
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
            kind: "BASE TABLE".into(),
            capturable: true,
        }
    }

    fn k(schema: &str, table: &str, kind: &str) -> CatalogMatch {
        CatalogMatch {
            schema: schema.into(),
            table: table.into(),
            kind: kind.into(),
            capturable: kind == "BASE TABLE",
        }
    }

    /// A twin that cannot CARRY an event is context, not a collision — and both
    /// directions of that are outages if you get them wrong.
    ///
    /// Counting them as candidates made an UNLOGGED table in another schema hard-fail
    /// a healthy config, and a STRANGER's `CREATE TEMP TABLE` of the same name did
    /// too — capture flapping green/red on a config nobody had touched. Counting them
    /// as nothing loses the message: a VIEW sharing the name is very often what the
    /// operator meant, and silence sends them hunting.
    #[test]
    fn a_twin_that_cannot_carry_an_event_is_a_note_not_an_ambiguity() {
        let got = resolve_captured_table(
            "vv",
            &[k("rivet", "vv", "VIEW"), k("other_db", "vv", "BASE TABLE")],
        )
        .expect("only ONE candidate can carry an event, so there is no ambiguity");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("other_db", "vv"),
            "the capturable relation wins — the view emits nothing and cannot put \
             rows in this export"
        );
        let note = got.note.expect("the view must be reported, not swallowed");
        assert!(
            note.contains("rivet.vv [VIEW]") && note.contains("cannot carry a change event"),
            "name it and say why it was skipped, because it may be what they meant: \
             {note}"
        );

        // ...and two CAPTURABLE relations are still a refusal.
        let err = resolve_captured_table(
            "vv",
            &[k("a", "vv", "BASE TABLE"), k("b", "vv", "BASE TABLE")],
        )
        .expect_err("two relations that can each carry events IS an ambiguity");
        assert!(err.to_string().contains("could mean"));
    }

    /// No capturable candidate at all: the name matches only relations that emit
    /// nothing. Resolution must not invent one — the engine's own classifier owns
    /// that refusal and says what to do about a view.
    #[test]
    fn a_name_matching_only_inert_relations_still_resolves_for_the_classifier() {
        let got = resolve_captured_table("vv", &[k("rivet", "vv", "VIEW")])
            .expect("one candidate, inert — classification refuses it, not resolution");
        assert_eq!((got.schema.as_str(), got.table.as_str()), ("rivet", "vv"));
    }

    /// The ordinary case, and the one that must not regress: one relation resolves
    /// to the CATALOG's spelling while the operator's string stays the destination
    /// sub-prefix.
    #[test]
    fn one_match_resolves_to_the_catalog_spelling_and_keeps_the_configured_string() {
        let got = resolve_captured_table("orders", &[m("public", "orders")])
            .expect("one match is the ordinary case");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("public", "orders")
        );
        assert_eq!(
            got.configured, "orders",
            "the operator's string is the destination sub-prefix — resolving must not \
             move an existing output path"
        );
    }

    /// The FOLD case: the operator writes `mixedcase`, the catalog spells it
    /// `MixedCase`, and wire events carry the catalog's spelling. Resolution takes
    /// the catalog's answer, so routing stops needing a fold-mismatch guard.
    #[test]
    fn resolution_takes_the_catalogs_case_not_the_configured_one() {
        let got = resolve_captured_table("mixedcase", &[m("public", "MixedCase")])
            .expect("the catalog found it");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("public", "MixedCase")
        );
        assert_eq!(got.configured, "mixedcase");
    }

    /// ONE relation reported through SEVERAL rules is one relation. The PG query
    /// reaches a row via up to four OR-arms; a resolver that counted it twice would
    /// refuse a perfectly unambiguous config — the false-alarm direction, which for
    /// a refusal is an outage.
    #[test]
    fn duplicate_reports_of_the_same_relation_do_not_make_an_ambiguity() {
        let got = resolve_captured_table(
            "public.orders",
            &[m("public", "orders"), m("public", "orders")],
        )
        .expect("the same relation twice is not two relations");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("public", "orders")
        );
    }

    /// MANY is a refusal, always — no precedence. The first cut ranked a whole-name
    /// match above a split, mirroring the router, and the folded-twin live fixture
    /// proved that wrong on its first run: `MixedCase` resolved "cleanly" to the
    /// byte-exact relation while PostgreSQL's own parser reads the same string as
    /// the folded one. Two systems disagreeing about one string is the ambiguity
    /// this seam exists to surface, not a tie to break.
    #[test]
    fn two_distinct_relations_are_refused_naming_both_whatever_their_shapes() {
        for (cfg, pair) in [
            ("orders", [m("public", "orders"), m("archive", "orders")]),
            (
                "MixedCase",
                [m("public", "MixedCase"), m("public", "mixedcase")],
            ),
            ("events.v2", [m("app", "events.v2"), m("events", "v2")]),
        ] {
            let err = resolve_captured_table(cfg, &pair)
                .expect_err("two distinct relations means the config does not name one");
            let msg = err.to_string();
            let a = format!("{}.{}", pair[0].schema, pair[0].table);
            let b = format!("{}.{}", pair[1].schema, pair[1].table);
            assert!(
                msg.contains(&a) && msg.contains(&b),
                "`{cfg}`: name BOTH — an operator qualifies only if they know which \
                 one they meant: {msg}"
            );
            assert!(msg.contains("Qualify"), "hand over the fix: {msg}");
        }
    }

    /// Nothing matched. Today this surfaces as a routing filter silently dropping
    /// every event; as a resolution failure it is one loud line before the stream
    /// opens.
    #[test]
    fn a_name_the_catalog_does_not_know_is_a_resolution_failure() {
        let err = resolve_captured_table("ghost", &[]).expect_err("no relation, no capture");
        assert!(err.to_string().contains("ghost"));
    }

    /// A dotted MongoDB collection resolves WHOLE — `a.b` is a legal collection
    /// name, and this is the entire reason a bare name is accepted anywhere. The
    /// Mongo catalog query supplies only the literal collection, so the set has one
    /// member; on an engine where the same string could also mean `schema.table`,
    /// the QUERY supplies both and the many-arm refuses. Engine knowledge in the
    /// query, decision in here.
    #[test]
    fn a_dotted_collection_name_resolves_whole_when_it_is_the_only_reading() {
        let got = resolve_captured_table("events.v2", &[m("app", "events.v2")])
            .expect("one reading, one relation");
        assert_eq!(
            (got.schema.as_str(), got.table.as_str()),
            ("app", "events.v2")
        );
    }
}
