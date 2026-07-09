//! Integration regressions for v0.7.3 P1.2 (better config parse errors).
//!
//! Two invariants this file pins:
//!
//! 1. **`deny_unknown_fields` is wired** — silent-drop typos are now
//!    hard errors.  A user mistyping `acccess_key_env` discovers the
//!    issue at parse time, not at run time.
//! 2. **Did-you-mean suggestions** — when the typo is close to a real
//!    field name, the error gains a `Did you mean \`X\`?` follow-up
//!    line that names *one* specific suggestion (vs serde's full
//!    "expected one of" list).
//!
//! Both apply at every level the operator can mistype: top-level,
//! `source:`, `exports[]`, `destination:`, `tuning:`.

use rivet::config::Config;

fn parse_err(yaml: &str) -> String {
    let err = Config::from_yaml(yaml).expect_err("config must fail to parse");
    format!("{err:#}")
}

// ── source-level typos ────────────────────────────────────────────────────────

#[test]
fn typo_in_source_field_is_rejected_with_suggestion() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  database_name: orders
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = parse_err(yaml);
    assert!(
        msg.contains("`database_name`"),
        "error must name the unknown field: {msg}"
    );
    assert!(
        msg.contains("Did you mean `database`"),
        "error must suggest the closest real field (`database`): {msg}"
    );
}

// ── destination-level typos ───────────────────────────────────────────────────

#[test]
fn typo_in_destination_field_is_rejected_with_suggestion() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: s3
      bucket: mybucket
      acccess_key_env: AWS_ACCESS_KEY_ID
"#;
    let msg = parse_err(yaml);
    assert!(
        msg.contains("`acccess_key_env`"),
        "error must name the unknown field: {msg}"
    );
    assert!(
        msg.contains("Did you mean `access_key_env`"),
        "error must suggest `access_key_env`: {msg}"
    );
}

// ── tuning-level typos ───────────────────────────────────────────────────────

#[test]
fn typo_in_tuning_field_is_rejected_with_suggestion() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tuning:
    profil: balanced
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = parse_err(yaml);
    assert!(
        msg.contains("`profil`"),
        "error must name the unknown field: {msg}"
    );
    assert!(
        msg.contains("Did you mean `profile`"),
        "error must suggest `profile`: {msg}"
    );
}

// ── top-level typos ──────────────────────────────────────────────────────────

#[test]
fn typo_at_top_level_is_rejected_with_suggestion() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
expoorts:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = parse_err(yaml);
    assert!(
        msg.contains("`expoorts`"),
        "error must name the unknown field: {msg}"
    );
    assert!(
        msg.contains("Did you mean `exports`"),
        "error must suggest `exports`: {msg}"
    );
}

// ── still bails when nothing is close ────────────────────────────────────────

#[test]
fn unknown_field_with_no_close_match_still_errors_without_suggestion() {
    // `flux_capacitor` has no close field name; the error should still
    // surface but without an embarrassing wrong suggestion.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  flux_capacitor: 88_mph
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = parse_err(yaml);
    assert!(
        msg.contains("`flux_capacitor`"),
        "error must name the unknown field: {msg}"
    );
    assert!(
        !msg.contains("Did you mean"),
        "no close-enough match, so no suggestion line should appear: {msg}",
    );
}

// ── misplaced-tuning fields keep the dedicated message ───────────────────────

#[test]
fn misplaced_tuning_field_under_source_keeps_specific_hint() {
    // `batch_size` directly under `source:` is a long-standing trap
    // — the dedicated `check_misplaced_tuning_fields` pre-check
    // catches it before serde sees the YAML.  This test ensures the
    // new lint layer didn't accidentally short-circuit that path
    // with the more generic "Did you mean" hint.
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  batch_size: 1000
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: local
      path: ./out
"#;
    let msg = parse_err(yaml);
    assert!(
        msg.contains("source.tuning"),
        "misplaced-tuning hint must still fire: {msg}"
    );
    assert!(
        !msg.contains("Did you mean"),
        "should not fall through to the generic did-you-mean path: {msg}",
    );
}

// ── mongo semantic validation (bug-hunt find) ─────────────────────────────────

#[test]
fn roast_mongo_resume_with_parallel_is_rejected() {
    // `resume: true` + `parallel: N` parsed fine but the parallel _id-range
    // path keeps NO keyset checkpoint: resume was silently ignored — the whole
    // collection re-read every run, with no warning anywhere. An impossible
    // combination must be a config error, not a silent behavior downgrade.
    let yaml = r#"
source:
  type: mongo
  url: "mongodb://localhost:27017/db"
  mongo: { resume: true, page_size: 100 }
exports:
  - name: t
    table: t
    mode: full
    format: parquet
    parallel: 4
    destination: { type: local, path: "/tmp/x" }
"#;
    let err = parse_err(yaml);
    assert!(
        err.contains("resume") && err.contains("parallel"),
        "the error must name the conflicting pair; got: {err}"
    );
}
