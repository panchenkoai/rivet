//! Generative completeness guard for the MySQL wire-type fixture.
//!
//! `mysql_native_type_name` and `mysql_type_to_rivet` branch on `MYSQL_TYPE_*`
//! column types. `the_mysql_wire_type_map_is_exhaustive_and_stable` drives a
//! table of column definitions through a real driver and checks what they map
//! to. This guard checks the one thing that test cannot check about itself:
//! that its table exercises EVERY type the two functions branch on.
//!
//! Why a separate instrument, when mutation testing already grades this file.
//! `cargo mutants` deletes a match ARM, and nine of these arms are SHARED by
//! several spellings — `MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2`,
//! `MYSQL_TYPE_TINY_BLOB | … | MYSQL_TYPE_BLOB`, and so on. Deleting such an arm
//! breaks whichever spelling the fixture DOES cover, so the mutant dies and the
//! uncovered siblings stay invisible: measured, the first version of that table
//! exercised 20 of the 29 branched-on types and still killed 144 of 152 mutants.
//! The nine it missed were exactly the aliases hiding inside shared arms. The
//! moment someone splits such an arm to give one spelling different handling,
//! the untested half becomes a real bug — and the mutant only reports it AFTER
//! the split, which is after the bug. This guard reports it before.
//!
//! Both sides are derived from the source, so neither can rot into a hand-typed
//! list: the required set comes from the product functions, the covered set from
//! the fixture's own rows. That is why the fixture writes the driver's constant
//! (`MYSQL_TYPE_TIMESTAMP2 as u8`) instead of a bare `17` — a table of bytes
//! could not be compared against the code at all.
//!
//! Scope, stated plainly: this proves a branch is EXERCISED, not that its
//! expectation is RIGHT. A row naming the right constant with a wrong expected
//! value passes here and dies to a mutant. The two instruments have different
//! blind spots and neither replaces the other.

use std::collections::BTreeSet;

const SRC: &str = "src/source/mysql/arrow_convert.rs";

/// Types the CLIENT PROTOCOL cannot deliver, so no fixture row can exist.
///
/// Not a waiver for "we did not get to it" — each entry is a measured
/// impossibility, and the guard fails if one becomes coverable (see
/// `an_unreachable_type_that_became_reachable_must_be_covered`).
const UNREACHABLE: &[(&str, &str)] = &[(
    "MYSQL_TYPE_NEWDATE",
    "0x0e is absent from the driver's `TryFrom<u8> for ColumnType` (its list steps \
     0x0d -> 0x0f), so a column definition carrying it fails the resultset parse \
     with `Unknown column type 14` before rivet's mapper is called",
)];

fn source() -> String {
    super::nonvacuity::subject_text(SRC)
}

/// Every `MYSQL_TYPE_*` token inside `fn <name>`, up to the next top-level `}`.
fn types_named_by_fn(src: &str, name: &str) -> BTreeSet<String> {
    let start = src.find(&format!("fn {name}")).unwrap_or_else(|| {
        panic!("{SRC} no longer defines `{name}` — did it move or get renamed?")
    });
    let rest = &src[start..];
    let end = rest.find("\n}\n").unwrap_or(rest.len());
    tokens(&rest[..end])
}

/// Every `MYSQL_TYPE_*` token inside the fixture's case table.
fn types_covered_by_fixture(src: &str) -> BTreeSet<String> {
    let start = src.find("let cases: Vec<WireCase>").expect(
        "the wire-type fixture's `let cases: Vec<WireCase>` table is gone — this guard \
         reads it to learn which types are exercised",
    );
    let rest = &src[start..];
    let end = rest.find("\n        ];").unwrap_or(rest.len());
    // Comments inside the table explain absences (NEWDATE); a mention in prose is
    // not coverage, so only lines that are actual rows count.
    let rows: String = rest[..end]
        .lines()
        .filter(|l| l.trim_start().starts_with('('))
        .collect::<Vec<_>>()
        .join("\n");
    tokens(&rows)
}

fn tokens(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while let Some(pos) = text[i..].find("MYSQL_TYPE_") {
        let s = i + pos;
        let mut e = s + "MYSQL_TYPE_".len();
        while e < bytes.len() && (bytes[e].is_ascii_alphanumeric() || bytes[e] == b'_') {
            e += 1;
        }
        out.insert(text[s..e].to_string());
        i = e;
    }
    out
}

fn required() -> BTreeSet<String> {
    let src = source();
    let mut r = types_named_by_fn(&src, "mysql_native_type_name");
    r.extend(types_named_by_fn(&src, "mysql_type_to_rivet"));
    // The REQUIRED side is where this guard can go blind: both functions are
    // sliced by `find("\n}\n")` off their `fn` header, so a formatting change
    // that moves the first top-level `}` earlier truncates the branch list —
    // and a truncated requirement is trivially satisfied by whatever the
    // fixture happens to cover. 29 types today.
    super::nonvacuity::require_enumerated(
        r.len(),
        20,
        &format!("MYSQL_TYPE_* branches parsed out of the two mappers in {SRC}"),
        "Fix the slice (the mappers are still there) rather than the floor — this set IS the \
         requirement the fixture is graded against.",
    );
    r
}

#[test]
fn the_fixture_exercises_every_wire_type_the_mapper_branches_on() {
    let covered = types_covered_by_fixture(&source());
    let unreachable: BTreeSet<String> = UNREACHABLE.iter().map(|(t, _)| t.to_string()).collect();

    let missing: Vec<String> = required()
        .into_iter()
        .filter(|t| !covered.contains(t) && !unreachable.contains(t))
        .collect();

    assert!(
        missing.is_empty(),
        "the MySQL mappers branch on wire type(s) the fixture never feeds them:\n  {}\n\n\
         Add a row per type to `the_mysql_wire_type_map_is_exhaustive_and_stable`. An \
         uncovered type is NOT caught by mutation testing when it shares a match arm with \
         a covered sibling — deleting the arm breaks the sibling, the mutant dies, and the \
         gap stays invisible until someone splits the arm. If the type genuinely cannot \
         reach the mapper, add it to UNREACHABLE with the measured reason.",
        missing.join("\n  ")
    );
}

#[test]
fn an_unreachable_type_that_became_reachable_must_be_covered() {
    // The escape hatch must not outlive its reason. If a declared-unreachable type
    // stops being named by the mappers at all, the entry is stale and should go —
    // otherwise it silently excuses a type nobody branches on any more.
    let req = required();
    let stale: Vec<&str> = UNREACHABLE
        .iter()
        .filter(|(t, _)| !req.contains(*t))
        .map(|(t, _)| *t)
        .collect();
    assert!(
        stale.is_empty(),
        "UNREACHABLE names type(s) the mappers no longer branch on: {stale:?}. Delete the \
         entry — a waiver for a branch that does not exist hides nothing and confuses the \
         next reader."
    );
}

#[test]
fn every_unreachable_entry_carries_a_reason() {
    let empty: Vec<&str> = UNREACHABLE
        .iter()
        .filter(|(_, why)| why.trim().len() < 40)
        .map(|(t, _)| *t)
        .collect();
    assert!(
        empty.is_empty(),
        "UNREACHABLE entries without a real measured reason: {empty:?}. A bare exclusion is \
         indistinguishable from a gap someone muted to get a build green."
    );
}
