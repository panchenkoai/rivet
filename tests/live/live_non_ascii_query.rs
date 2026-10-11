//! `query:` text holding characters outside ASCII (a string literal, a comment, a CTE
//! name): the export delivers what the source returns for it, as it does for the same
//! query spelled in ASCII. Every cell runs both spellings and re-reads the destination:
//! the row count, the keys, the names and the literal itself.

use super::live_planner_zero_rows::{delivered_values, insert_rows, source_values, spelled};
use crate::common::*;
use std::collections::BTreeSet;

const ROWS: usize = 250;

/// The standalone MongoDB of the stand.
const MONGO_PORT: u16 = 27017;

/// The pieces a query is spelled with: a string literal, a comment and a CTE name.
#[derive(Clone, Copy)]
struct Spelling {
    label: &'static str,
    comment: &'static str,
    cte: &'static str,
}

const ASCII: Spelling = Spelling {
    label: "ete",
    comment: "/* abcena */",
    cte: "et",
};

/// `été`, a Cyrillic comment whose first letter straddles byte 6 of a query it opens, and a CTE named `ét`.
const NON_ASCII: Spelling = Spelling {
    label: "\u{e9}t\u{e9}",
    comment: "/* ab\u{446}\u{435}\u{43d}\u{430} */",
    cte: "\u{e9}t",
};

/// How `engine` opens a text literal that may hold any character (SQL Server needs its `N`): two bytes everywhere, so a shape's byte offsets are the same on every engine.
fn open_quote(engine: SqlEngine) -> &'static str {
    match engine {
        SqlEngine::Mssql => "N'",
        _ => " '",
    }
}

/// A query over `table` spelled with a [`Spelling`] and the engine's [`open_quote`]; it returns the table's `id` and `name` and the literal as `label`.
type Shape = fn(Spelling, &str, &str) -> String;

/// The literal sits in the projection, before the outer `FROM`.
fn literal_in_the_projection(s: Spelling, table: &str, quote: &str) -> String {
    format!("SELECT id, name,{quote}{}' AS label FROM {table}", s.label)
}

/// The comment opens the query, so `SELECT` is not its first word.
fn comment_first(s: Spelling, table: &str, quote: &str) -> String {
    format!(
        "{} SELECT id, name,{quote}{}' AS label FROM {table}",
        s.comment, s.label
    )
}

/// The literal follows `SELECT *` directly.
fn literal_after_the_star(s: Spelling, table: &str, quote: &str) -> String {
    format!("SELECT *,{quote}{}' AS label FROM {table}", s.label)
}

/// The query opens with a CTE the outer `FROM` reads.
fn cte_first(s: Spelling, table: &str, quote: &str) -> String {
    format!(
        "WITH {cte} AS (SELECT id, name FROM {table}) SELECT id, name,{quote}{}' AS label FROM {cte}",
        s.label,
        cte = s.cte
    )
}

/// A fresh `(id, name)` table of [`ROWS`] rows named `café <id>`, and its drop guard.
fn names_table(engine: SqlEngine) -> (String, Box<dyn std::any::Any>) {
    let text = match engine {
        SqlEngine::Mssql => "NVARCHAR(40)",
        _ => "VARCHAR(40)",
    };
    let (table, guard) = engine.create(
        "non_ascii_q",
        &format!("id {} PRIMARY KEY, name {text} NOT NULL", engine.int64()),
    );
    let quote = open_quote(engine);
    insert_rows(engine, &table, "id, name", 1..=ROWS as i64, |g| {
        format!("{g},{quote}caf\u{e9} {g}'")
    });
    (table, guard)
}

/// Each shape, in `mode` with `lines`, delivers the table's rows, keys and names and its own literal — spelled in ASCII and outside it.
fn delivers_both_spellings(engine: SqlEngine, mode: &str, lines: &[String], shapes: &[Shape]) {
    engine.alive();
    let (table, _guard) = names_table(engine);
    let (ids, names) = (
        source_values(engine, &table, "id"),
        source_values(engine, &table, "name"),
    );
    assert!(
        ids.len() == ROWS && names.len() == ROWS && names.iter().all(|n| !n.is_ascii()),
        "fixture: {ROWS} rows, every name outside ASCII: {names:?}"
    );
    for shape in shapes {
        for spelling in [ASCII, NON_ASCII] {
            let query = shape(spelling, &table, open_quote(engine));
            let rig = lines
                .iter()
                .fold(engine.rig(&table).query(&query).mode(mode), |rig, line| {
                    rig.export_line(line)
                });
            rig.run_ok();
            let out = rig.out_dir();
            let delivered = |col: &str| delivered_values(&out, &spelled(engine, col));
            assert_eq!(
                (
                    total_parquet_rows(&out),
                    delivered("id"),
                    delivered("name"),
                    delivered("label"),
                ),
                (
                    ROWS,
                    ids.clone(),
                    names.clone(),
                    BTreeSet::from([spelling.label.to_string()]),
                ),
                "mode {mode}: `{query}` returns the table's {ROWS} rows and its literal"
            );
        }
    }
}

/// Range-chunked on `id`: the planner reads the query's projection before it runs a window.
fn a_chunked_query_holding_non_ascii_text_delivers_its_rows(engine: SqlEngine) {
    let lines = [
        format!("chunk_column: {}", spelled(engine, "id")),
        "chunk_size: 100".to_string(),
    ];
    let shapes: [Shape; 2] = [literal_in_the_projection, comment_first];
    delivers_both_spellings(engine, "chunked", &lines, &shapes);
}

/// `mode: full`: the query's first bytes are read to find the table it names, whatever they are.
fn a_query_opening_with_non_ascii_text_delivers_its_rows(engine: SqlEngine, shapes: &[Shape]) {
    let straddles =
        |shape: Shape, at: usize| !shape(NON_ASCII, "t", open_quote(engine)).is_char_boundary(at);
    assert!(
        straddles(comment_first, 6)
            && straddles(cte_first, 6)
            && straddles(literal_after_the_star, 12),
        "fixture: a multi-byte character sits across the end of the keyword each shape is tried for"
    );
    delivers_both_spellings(engine, "full", &[], shapes);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_chunked_query_holding_non_ascii_text_delivers_its_rows_postgres() {
    a_chunked_query_holding_non_ascii_text_delivers_its_rows(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn a_chunked_query_holding_non_ascii_text_delivers_its_rows_mysql() {
    a_chunked_query_holding_non_ascii_text_delivers_its_rows(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn a_chunked_query_holding_non_ascii_text_delivers_its_rows_mssql() {
    a_chunked_query_holding_non_ascii_text_delivers_its_rows(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose up -d oracle"]
fn a_chunked_query_holding_non_ascii_text_delivers_its_rows_oracle() {
    a_chunked_query_holding_non_ascii_text_delivers_its_rows(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn a_query_opening_with_non_ascii_text_delivers_its_rows_postgres() {
    a_query_opening_with_non_ascii_text_delivers_its_rows(
        SqlEngine::Pg,
        &[comment_first, literal_after_the_star, cte_first],
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn a_query_opening_with_non_ascii_text_delivers_its_rows_mysql() {
    a_query_opening_with_non_ascii_text_delivers_its_rows(
        SqlEngine::Mysql,
        &[comment_first, literal_after_the_star, cte_first],
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mssql"]
fn a_query_opening_with_non_ascii_text_delivers_its_rows_mssql() {
    a_query_opening_with_non_ascii_text_delivers_its_rows(
        SqlEngine::Mssql,
        &[comment_first, literal_after_the_star],
    );
}

/// Oracle has no `SELECT *, <expression>`: the comment shape alone opens with non-ASCII text there.
#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose up -d oracle"]
fn a_query_opening_with_non_ascii_text_delivers_its_rows_oracle() {
    a_query_opening_with_non_ascii_text_delivers_its_rows(SqlEngine::Oracle, &[comment_first]);
}

/// MongoDB reads a collection, never a query: a `query:` is refused by name in either spelling, and nothing is written.
#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn a_query_opening_with_non_ascii_text_is_refused_like_its_ascii_spelling_mongo() {
    require_alive(LiveService::Mongo);
    let db = unique_name("non_ascii_q");
    for spelling in [&ASCII, &NON_ASCII] {
        let query = format!("{} SELECT * FROM orders", spelling.comment);
        let rig = Rig::mongo_batch("orders")
            .source_url(&MongoTest::url(MONGO_PORT, &db))
            .query(&query);
        let text = rig.run_expect_fail();
        assert!(
            text.contains(
                "MongoDB source supports only `table: <collection>` with `mode: full` — a \
                 hand-written `query:` or a filtered/wrapped form has no MongoDB equivalent."
            ) && !text.contains("panicked"),
            "`{query}` is refused as a query MongoDB cannot run:\n{text}"
        );
        assert_eq!(
            total_parquet_rows(&rig.out_dir()),
            0,
            "`{query}`: refused before any row is written"
        );
    }
}
