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
const UNVERIFIED_RATCHET: usize = 2;

fn claims() -> Vec<Value> {
    let s = fs::read_to_string(MATRIX).unwrap_or_else(|e| panic!("read {MATRIX}: {e}"));
    let doc: Value = serde_yaml_ng::from_str(&s).unwrap_or_else(|e| panic!("parse {MATRIX}: {e}"));
    doc.get("claims")
        .and_then(|c| c.as_sequence())
        .unwrap_or_else(|| panic!("{MATRIX} must have a `claims:` sequence"))
        .clone()
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
    assert!(
        n <= UNVERIFIED_RATCHET,
        "{n} claims have no verifier, ratchet is {UNVERIFIED_RATCHET}. Publishing a number \
         nobody checks is a deliberate act: give it a checker and LOWER the ratchet, or raise \
         it in the same commit that explains why the claim must exist unchecked."
    );
}
