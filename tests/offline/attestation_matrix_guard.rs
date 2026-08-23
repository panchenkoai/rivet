//! Drift guard for `docs/attestation-matrix.yaml` — the ledger of who checks
//! each number rivet publishes about its own work.
//!
//! The sibling ledgers grade coverage; this one grades honesty, so its failure
//! modes are different. A coverage cell that rots reads as a missing test. An
//! attestation row that rots reads as a GUARANTEE — someone sees
//! `independence: independent` and stops asking. These four guards pin the ways
//! that could happen:
//!
//! 1. every claim resolves: it names a verifier, or says `none` and explains;
//! 2. an `independent` claim names the evidence, because the word is the whole
//!    value of the row and unbacked it is just a nicer synonym for `self`;
//! 3. a `self` claim carries a `negative_proof` or a `note` saying what it
//!    cannot see — "rivet verifies it" otherwise means only that a code path
//!    runs;
//! 4. the unverified count is shrink-only. Adding a claim nobody checks is a
//!    deliberate act, not something that lands in a refactor.

use std::fs;

use serde_yaml_ng::Value;

const MATRIX: &str = "docs/attestation-matrix.yaml";

/// Claims with `verified_by: none`. LOWER when one gets a verifier; never raise
/// without saying why in the row itself.
/// History: 2 -> 1 (`part_rows`, footer vs manifest) -> 0 (`schema_fingerprint`,
/// sensitivity to real DDL). At zero, a NEW unverified claim is a deliberate act.
const UNVERIFIED_RATCHET: usize = 0;

fn claims() -> Vec<Value> {
    let s = super::nonvacuity::subject_text(MATRIX);
    let doc: Value = serde_yaml_ng::from_str(&s).unwrap_or_else(|e| panic!("parse {MATRIX}: {e}"));
    let out: Vec<Value> = doc
        .get("claims")
        .and_then(|c| c.as_sequence())
        .unwrap_or_else(|| panic!("{MATRIX} must have a `claims:` sequence"))
        .clone();
    // A PRESENT but empty (or truncated) `claims:` is the dangerous shape: all
    // five guards below iterate it, so zero rows means five green tests over a
    // ledger asserting nothing — and this is the ledger whose rows read as
    // GUARANTEES. 16 claims today.
    super::nonvacuity::require_enumerated(
        out.len(),
        10,
        &format!("`claims:` rows in {MATRIX}"),
        "Restore the rows or re-point this reader; an attestation ledger that parses to \
         nothing still renders as `independence: independent` to every reader of the file.",
    );
    out
}

fn field(c: &Value, k: &str) -> Option<String> {
    c.get(k).map(|v| match v {
        Value::String(s) => s.clone(),
        other => format!("{other:?}"),
    })
}

fn id(c: &Value) -> String {
    field(c, "id").unwrap_or_else(|| "<no id>".into())
}

#[test]
fn every_claim_names_a_verifier_or_admits_none() {
    let mut bad = Vec::new();
    for c in claims() {
        let v = field(&c, "verified_by");
        match v.as_deref() {
            None => bad.push(format!("{}: no `verified_by` at all", id(&c))),
            Some("none") | Some("None") => {
                if field(&c, "gap").is_none() {
                    bad.push(format!(
                        "{}: `verified_by: none` with no `gap:` explaining what that costs",
                        id(&c)
                    ));
                }
            }
            Some(_) => {}
        }
    }
    assert!(
        bad.is_empty(),
        "attestation claim(s) that do not resolve. Every number rivet publishes is either \
         checked by something nameable, or it is trusted on rivet's word — and the second \
         case must SAY so, because an unanswered row reads as the first:\n  {}",
        bad.join("\n  ")
    );
}

#[test]
fn an_independent_claim_names_its_evidence() {
    let mut bad = Vec::new();
    for c in claims() {
        if field(&c, "independence").as_deref() == Some("independent")
            && field(&c, "evidence").is_none()
        {
            bad.push(id(&c));
        }
    }
    assert!(
        bad.is_empty(),
        "claim(s) asserting INDEPENDENT verification with no evidence: {bad:?}. Independence \
         is the entire value of such a row — a reader stops asking when they see it. Name the \
         cell, the test, or the other party that does the checking, or downgrade the row."
    );
}

#[test]
fn a_self_claim_says_what_it_cannot_see() {
    let mut bad = Vec::new();
    for c in claims() {
        let ind = field(&c, "independence");
        if matches!(ind.as_deref(), Some("self") | Some("internal"))
            && field(&c, "negative_proof").is_none()
            && field(&c, "note").is_none()
        {
            bad.push(id(&c));
        }
    }
    assert!(
        bad.is_empty(),
        "claim(s) verified by rivet against its own output, with neither a `negative_proof` \
         nor a `note` on the blind spot: {bad:?}. A self-check detects DRIFT and is \
         structurally blind to a defect in the spec, because both sides share it — that is \
         how a non-injective row hash lived four months and how an annihilating checksum fold \
         let `validate` pass on corrupted values. Add the cell that corrupts the artifact and \
         requires the check to fail, or state the limit."
    );
}

#[test]
fn unverified_claims_do_not_grow() {
    let n = claims()
        .iter()
        .filter(|c| {
            matches!(
                field(c, "verified_by").as_deref(),
                Some("none") | Some("None")
            )
        })
        .count();
    // Expressed as "over the ceiling", not `n <= RATCHET`: at a ratchet of zero
    // the latter is trivially true for a `usize` and clippy rightly says so — the
    // guard would have gone quiet at exactly the moment it started mattering.
    if n > UNVERIFIED_RATCHET {
        panic!(
            "{n} claims have no verifier, ratchet is {UNVERIFIED_RATCHET}. Publishing a number \
             nobody checks is a deliberate act: give it a checker and LOWER the ratchet, or \
             raise it in the same commit that explains why the claim must exist unchecked."
        );
    }
}

/// Guard #5 — a value-consuming claim must say WHICH TYPES it saw.
///
/// The four guards above ask *who* verifies a number. This one asks *what they
/// looked at*, and it exists because the first question can be answered
/// perfectly while the second goes unasked.
///
/// `row_hash_column` carried `independence: independent`, named a real second
/// implementation (`dev/release_oracle/rowhash.py`, a Python rebuild of the
/// canonical image hashed with `xxhash`) and recorded `PASS on
/// postgres/mysql/mssql 2026-08-02`. Every word true. It still missed
/// `Timestamp(µs, Some("UTC"))` — 278 of 1184 columns in a real production
/// database — because the probe it ran against did not carry that type. The
/// failure cost 63 of 65 exports on 2026-08-05.
///
/// So: any claim about a number DERIVED FROM CELL VALUES must declare its type
/// coverage. The consumer list is derived from the ledger itself (claims whose
/// `what` names a per-value digest), not hand-written here, so a NEW value
/// consumer added to the matrix inherits the requirement without anyone
/// remembering to extend this test.
#[test]
fn a_value_derived_claim_declares_its_type_coverage() {
    // A claim is value-derived if its own text says it is computed per row or
    // per column from cell values. Derived from the row, not a literal list —
    // a hand-written list grades only what its author already knew, which is
    // the exact failure this guard was written for.
    let value_derived: Vec<Value> = claims()
        .into_iter()
        .filter(|c| {
            let what = field(c, "what").unwrap_or_default().to_lowercase();
            (what.contains("per-row")
                || what.contains("per-column")
                || what.contains("value digest"))
                && (what.contains("digest") || what.contains("hash") || what.contains("checksum"))
        })
        .collect();

    assert!(
        value_derived.len() >= 3,
        "expected at least the three known value consumers (row_hash, Form A, Form B); \
         found {} — has a `what:` been reworded so this guard no longer recognises it? \
         The filter is deliberately on the CLAIM's own description, so rewording it \
         silently drops the requirement.",
        value_derived.len()
    );

    for c in &value_derived {
        let cov = field(c, "type_coverage").unwrap_or_default();
        assert!(
            !cov.trim().is_empty(),
            "claim '{}' is derived from CELL VALUES but declares no `type_coverage:`. \
             Naming a verifier is not enough — `row_hash_column` named an INDEPENDENT one \
             and still missed a type that is 23% of a real database, because nothing said \
             which types the verifier had seen. Add `type_coverage:` naming the fixture and \
             the test, or state the gap.",
            id(c)
        );
        // A coverage claim that names no fixture is a sentence, not a check.
        assert!(
            cov.contains("rivet_type_matrix") || cov.to_lowercase().contains("gap"),
            "claim '{}' declares type_coverage but names no fixture: {cov:?}. \
             Point it at `rivet_type_matrix` (the golden fixture that spans the type \
             surface) and the test that drives it, or say `gap:` and why.",
            id(c)
        );
    }
}

/// Guard #6 — the fixture those claims point at must actually span the type
/// surface, and the test that drives it must exist.
///
/// Guard #5 makes a claim NAME its coverage; this one makes the name true. The
/// pair is deliberate: a declaration nobody checks is how
/// `docs/scenario-artifact-matrix.yaml` came to list five tables as having "no
/// export_name column" when three of them did.
#[test]
fn the_type_coverage_fixture_and_its_driver_exist() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

    // The driver: one export putting the fixture through every value consumer.
    let stand = fs::read_to_string(root.join("tests/live/chunking_stand.rs"))
        .expect("read tests/live/chunking_stand.rs");
    assert!(
        stand.contains("fn run_type_matrix_through_every_value_consumer"),
        "the claims point at `stand_type_matrix_every_consumer_*`, but its driver is gone \
         from tests/live/chunking_stand.rs — a renamed or deleted test leaves the ledger \
         asserting coverage that nothing produces."
    );
    for eng in ["postgres", "mysql", "mssql"] {
        assert!(
            stand.contains(&format!("stand_type_matrix_every_consumer_{eng}")),
            "engine `{eng}` has no value-consumer test. The production failure this guards \
             was on MySQL while the only such test ran on PostgreSQL — one engine standing \
             in for three is exactly how it was missed."
        );
    }

    // The fixture: the golden type matrix must carry a TIMEZONE-AWARE timestamp,
    // the shape that broke. Checked per engine because the seeds are separate
    // files and can drift apart (they did: `dbo.orders` had two owners).
    for (seed, needle) in [
        ("seeds/common/mysql.sql", "TIMESTAMP"),
        ("seeds/common/postgres.sql", "TIMESTAMPTZ"),
        ("seeds/common/mssql.sql", "DATETIMEOFFSET"),
    ] {
        let text =
            fs::read_to_string(root.join(seed)).unwrap_or_else(|e| panic!("read {seed}: {e}"));
        // Slice from the CREATE TABLE to its closing paren. Splitting on the
        // bare word picks up comments and INSERTs — the first draft did, and
        // reported PostgreSQL as missing a column it has carried all along.
        let upper = text.to_uppercase();
        let create = upper
            .find("CREATE TABLE")
            .and_then(|i| upper[i..].find("TYPE_MATRIX").map(|j| i + j))
            .unwrap_or_else(|| panic!("{seed} has no `CREATE TABLE ... type_matrix`"));
        let end = upper[create..]
            .find("\n);")
            .map(|e| create + e)
            .unwrap_or(upper.len());
        let matrix_block = &upper[create..end];
        assert!(
            matrix_block.contains(needle),
            "{seed}'s type_matrix carries no {needle} column. The type-coverage claims in \
             {MATRIX} rest on this fixture spanning the type surface; a timezone-aware \
             timestamp is the specific shape that cost 63 of 65 exports on 2026-08-05."
        );
    }
}
