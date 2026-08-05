//! Generative completeness guard for the SQL Server CDC cell mapper's fixture.
//!
//! Sibling of `mysql_wire_type_fixture_guard`, same class, different driver.
//! `cell_from_data` and `cell_to_rivet` between them branch on every
//! `ColumnData` variant SQL Server can hand back; this checks that the unit
//! fixture feeds each one, or that the variant is declared ROW_BOUND with the
//! live test that covers it instead.
//!
//! Why the declaration matters here more than it did for MySQL. Six temporal
//! variants — four arms, since DateTime, DateTime2 and SmallDateTime share one —
//! cannot be unit-tested at all: tiberius decodes `date`, `time`,
//! `datetime*` and `datetimeoffset` through `Row::try_get`, not from the
//! `ColumnData` the mapper is handed, and `Row` has no public constructor. They
//! are covered by the live MSSQL CDC suite — a claim that is worth only what it
//! has been tested with, so each entry names the live test AND the failure text
//! observed when that arm was deleted. Deleting an arm turns its whole column
//! NULL, which every count- and sum-shaped check passes.
//!
//! What this cannot do, stated so nobody reads more into a green: it proves a
//! variant is FED to the mapper, not that the expected value is right. A row
//! naming the right variant with a wrong expectation passes here and dies to a
//! mutant. Mutation testing and this guard have different blind spots — the
//! mutant cannot see an unfed variant hiding inside a shared arm, and this
//! cannot see a wrong answer.

use std::collections::BTreeSet;
use std::path::PathBuf;

const SRC: &str = "src/source/mssql/cdc.rs";

/// Variants no unit test can reach, each with the live test that does.
///
/// Every one of these was RED-proven on 2026-08-02 by deleting the arm and
/// running the named test against a live SQL Server — not assumed from the fact
/// that a live suite exists.
const ROW_BOUND: &[(&str, &str)] = &[
    (
        "Date",
        "needs a Row (tiberius decodes it via try_get) — live: \
         mssql_cdc_full_type_matrix_matches_batch, RED \"column d: CDC differs from the \
         batch export\"",
    ),
    (
        "Time",
        "needs a Row — live: mssql_cdc_full_type_matrix_matches_batch, RED \"column t\"",
    ),
    (
        "DateTime",
        "needs a Row; shares an arm with DateTime2 and SmallDateTime, and the live \
         fixture carries all three as SEPARATE columns — live: \
         mssql_cdc_full_type_matrix_matches_batch, RED \"column dt2\"",
    ),
    (
        "DateTime2",
        "needs a Row; shares DateTime's arm, covered as its own column dt2 — live: \
         mssql_cdc_full_type_matrix_matches_batch",
    ),
    (
        "SmallDateTime",
        "needs a Row; shares DateTime's arm, covered as its own column sdt — live: \
         mssql_cdc_full_type_matrix_matches_batch",
    ),
    (
        "DateTimeOffset",
        "needs a Row, and the WRONG try_get type silently yields NULL — live: \
         mssql_cdc_datetimeoffset_value_is_preserved, RED got \"1970-01-01 00:00:00\"",
    ),
];

fn source() -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(SRC);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

fn variants(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while let Some(pos) = text[i..].find("ColumnData::") {
        let s = i + pos + "ColumnData::".len();
        let mut e = s;
        while e < bytes.len() && (bytes[e].is_ascii_alphanumeric() || bytes[e] == b'_') {
            e += 1;
        }
        if e > s {
            out.insert(text[s..e].to_string());
        }
        i = e.max(i + 1);
    }
    out
}

/// The variants the two mapper functions branch on.
fn branched_on(src: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for f in ["fn cell_from_data", "fn cell_to_rivet"] {
        let start = src.find(f).unwrap_or_else(|| {
            panic!("{SRC} no longer defines `{f}` — did it move or get renamed?")
        });
        let rest = &src[start..];
        let end = rest.find("\n}\n").unwrap_or(rest.len());
        out.extend(variants(&rest[..end]));
    }
    out
}

/// The variants the unit fixture actually feeds — its `cases` table only.
///
/// Deliberately NOT the whole test function: it also asserts that a NULL and a
/// temporal variant DEFER to the row, and a variant mentioned in a defer
/// assertion is the opposite of covered.
fn fed_by_fixture(src: &str) -> BTreeSet<String> {
    let start = src.find("let cases: Vec<(&str, ColumnData").expect(
        "the ColumnData fixture's `cases` table is gone — this guard reads it to learn \
         which variants are exercised",
    );
    let rest = &src[start..];
    let end = rest
        .find("\n        ];")
        .expect("the `cases` table is not terminated by `];` at the expected indent");
    variants(&rest[..end])
}

#[test]
fn the_fixture_feeds_every_column_data_variant_the_mapper_branches_on() {
    let src = source();
    let fed = fed_by_fixture(&src);
    let row_bound: BTreeSet<String> = ROW_BOUND.iter().map(|(v, _)| v.to_string()).collect();

    let missing: Vec<String> = branched_on(&src)
        .into_iter()
        .filter(|v| !fed.contains(v) && !row_bound.contains(v))
        .collect();

    assert!(
        missing.is_empty(),
        "the MSSQL CDC mapper branches on ColumnData variant(s) the fixture never feeds \
         it:\n  {}\n\nAdd a row to `every_data_only_column_data_arm_maps_to_its_documented_value`. \
         An unfed variant is NOT caught by mutation testing when it shares an arm with a fed \
         sibling — deleting the arm breaks the sibling, the mutant dies, and the gap stays \
         invisible. If the variant needs a `Row` and can only be covered live, add it to \
         ROW_BOUND naming the live test AND the failure it produces when the arm is deleted.",
        missing.join("\n  ")
    );
}

#[test]
fn a_row_bound_variant_the_mapper_dropped_must_leave_the_list() {
    let branched = branched_on(&source());
    let stale: Vec<&str> = ROW_BOUND
        .iter()
        .filter(|(v, _)| !branched.contains(*v))
        .map(|(v, _)| *v)
        .collect();
    assert!(
        stale.is_empty(),
        "ROW_BOUND names variant(s) the mapper no longer branches on: {stale:?}. Delete the \
         entry — a live-coverage claim for a branch that does not exist protects nothing."
    );
}

#[test]
fn every_row_bound_entry_names_the_live_test_that_covers_it() {
    let bad: Vec<&str> = ROW_BOUND
        .iter()
        .filter(|(_, why)| !why.contains("live:") || !why.contains("mssql_cdc_"))
        .map(|(v, _)| *v)
        .collect();
    assert!(
        bad.is_empty(),
        "ROW_BOUND entries that do not name a live `mssql_cdc_*` test: {bad:?}. \"covered by \
         the live suite\" without a test name is the claim this repo has already been burned \
         by — it was believed for months before anyone deleted an arm and ran it."
    );
}
