//! Backward-compatibility tests for plan / summary artifacts.
//!
//! Two KINDS of test live here, and the difference is the whole point:
//!
//! 1. **Loader tests** pass a frozen fixture through rivet's CURRENT
//!    deserializer. These are the ones that fail when a field is removed,
//!    renamed, or made required — before users with committed artifacts hit a
//!    confusing error. `legacy_summary_still_deserializes_through_run_report`
//!    is one; the plan half lives beside the type it loads, in
//!    `src/plan/artifact.rs::legacy_wire_compat`, because `PlanArtifact` is
//!    `pub(crate)`.
//!
//! 2. **Shape tests** interrogate the fixture as a bare `serde_json::Value`.
//!    They document the wire format an on-disk artifact carries. They CANNOT
//!    fail against a production change — both sides of the comparison are the
//!    frozen file — and are named `..._documents_...` so nobody mistakes them
//!    for the first kind.
//!
//! That distinction is not pedantry. This module's doc used to claim the first
//! property for every test in it while containing only the second, and when
//! `verify` became a REQUIRED field on the plan artifact, every assertion here
//! stayed green for two months while `rivet apply` rejected every plan.json
//! users already had on disk.
//!
//! The fixtures live under `tests/fixtures/artifacts_legacy/`. Each was
//! captured from a real `rivet plan` / `rivet run` against the fixture
//! tables in docker-compose at the version named in the filename.
//!
//! Why JSON-shape, not the Rust type: `plan::artifact::PlanArtifact` is
//! `pub(crate)` and intentionally not part of any external API. We validate
//! the **wire format** an existing-on-disk plan.json must satisfy — that's
//! what end users actually depend on, and it's what `rivet apply` reads.
//!
//! When the schema MUST change incompatibly: document the migration in
//! CHANGELOG, update the fixture in the same PR, and ideally provide a
//! one-shot migration tool.

use serde_json::Value;

const LEGACY_FULL: &str = include_str!("fixtures/artifacts_legacy/v0_7_5_plan_full.json");
const LEGACY_INCR: &str = include_str!("fixtures/artifacts_legacy/v0_7_5_plan_incremental.json");
const LEGACY_CHUNKED: &str = include_str!("fixtures/artifacts_legacy/v0_7_5_plan_chunked.json");
const LEGACY_SUMMARY: &str = include_str!("fixtures/artifacts_legacy/v0_7_5_summary.json");

fn parse(label: &str, body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("{label} not valid JSON: {e}"))
}

fn require_field(label: &str, v: &Value, field: &str) {
    assert!(
        v.get(field).is_some(),
        "{label}: missing field '{field}'. \
         If this rename is intentional, ship a migration AND update the fixture."
    );
}

#[test]
fn v0_7_5_plan_full_top_level_fields_present() {
    let v = parse("v0.7.5 plan full", LEGACY_FULL);
    for field in [
        "rivet_version",
        "plan_id",
        "created_at",
        "expires_at",
        "export_name",
        "strategy",
        "plan_fingerprint",
        "resolved_plan",
    ] {
        require_field("v0.7.5 plan full", &v, field);
    }
    assert_eq!(v["rivet_version"].as_str(), Some("0.7.5"));
    assert_eq!(v["strategy"].as_str(), Some("full"));
    assert_eq!(v["export_name"].as_str(), Some("pa_audit"));
}

#[test]
fn v0_7_5_plan_resolved_plan_subfields_present() {
    let v = parse("v0.7.5 plan full", LEGACY_FULL);
    let rp = v
        .get("resolved_plan")
        .expect("resolved_plan must be present");
    for field in [
        "export_name",
        "base_query",
        "format",
        "compression",
        "destination",
        "tuning",
        "meta_columns",
    ] {
        require_field("v0.7.5 resolved_plan", rp, field);
    }
}

#[test]
fn v0_7_5_plan_destination_subfields_documents_the_wire_union() {
    let v = parse("v0.7.5 plan full", LEGACY_FULL);
    let dest = &v["resolved_plan"]["destination"];
    // Pin the union: every destination-related field that an apply may read
    // must be present and `null` if not used. A change that re-flattens the
    // destination (e.g. moves S3 fields under `destination.s3`) breaks here.
    for field in [
        "type",
        "bucket",
        "prefix",
        "path",
        "region",
        "endpoint",
        "credentials_file",
        "access_key_env",
        "secret_key_env",
        "session_token_env",
        "aws_profile",
        "account_name",
        "account_key_env",
        "sas_token_env",
        "allow_anonymous",
    ] {
        require_field("v0.7.5 destination", dest, field);
    }
}

#[test]
fn v0_7_5_plan_incremental_documents_its_strategy_label() {
    let v = parse("v0.7.5 plan incremental", LEGACY_INCR);
    assert_eq!(v["strategy"].as_str(), Some("incremental"));
    // Incremental-specific fields should be reachable somewhere on the
    // resolved plan. Be liberal — accept either nested or flat layouts.
    let rp = &v["resolved_plan"];
    let dump = serde_json::to_string(rp).unwrap();
    assert!(
        dump.contains("cursor_column") || dump.contains("Incremental"),
        "incremental plan must carry cursor info — got: {dump}"
    );
}

#[test]
fn v0_7_5_plan_chunked_strategy_label_matches() {
    let v = parse("v0.7.5 plan chunked", LEGACY_CHUNKED);
    assert_eq!(v["strategy"].as_str(), Some("chunked"));
    let rp = &v["resolved_plan"];
    let dump = serde_json::to_string(rp).unwrap();
    assert!(
        dump.contains("chunk_size") || dump.contains("Chunked"),
        "chunked plan must carry chunk info — got: {dump}"
    );
}

#[test]
fn v0_7_5_summary_top_level_fields_documents_the_wire_spine() {
    // A v0.7.5 summary.json is what `notifications.slack` and the trust
    // artifacts machinery read. The shape is operator-visible and must
    // stay stable across patches.
    let v = parse("v0.7.5 summary", LEGACY_SUMMARY);
    // Don't pin every field (summary has many optional metrics) — just
    // the spine that downstream consumers definitely rely on.
    for field in ["run_id", "export_name", "status"] {
        require_field("v0.7.5 summary", &v, field);
    }
}

// ── the LOADER half: the guard the shape tests above cannot be ───────────────

/// A `summary.json` written by an older rivet must still deserialize through the
/// CURRENT `RunReport` — the type every external consumer of that artifact uses.
///
/// This is the assertion the whole module claimed to make and did not. The shape
/// tests parse the fixture into a bare `serde_json::Value` and then ask it about
/// its own keys, so only editing the fixture can turn them red. Adding a REQUIRED
/// field to `RunReport` without `#[serde(default)]`, or renaming one of its ~27
/// fields, breaks every pre-existing summary.json on a user's disk while every
/// one of those tests stays green — the exact shape of the `verify` bite that
/// cost two months on the plan side.
///
/// The plan half of this guarantee lives beside its type
/// (`src/plan/artifact.rs::legacy_wire_compat`) because `PlanArtifact` is
/// `pub(crate)`. `RunReport` is public, so that reason never applied here — the
/// summary fixture was simply left behind when the plan half was fixed.
#[test]
fn legacy_summary_still_deserializes_through_run_report() {
    let report: rivet::pipeline::RunReport =
        serde_json::from_str(LEGACY_SUMMARY).unwrap_or_else(|e| {
            panic!(
                "a v0.7.5 summary.json no longer loads through the current RunReport: {e}\n\
                 If this change is intentional, it BREAKS every summary.json already on \
                 disk. Give the new field #[serde(default)], or ship a migration and \
                 update the fixture in the same PR."
            )
        });

    // Values must SURVIVE, not merely parse: a rename with #[serde(default)]
    // deserializes Ok while silently dropping the field, which is the quieter
    // half of the same defect. Expected values are read from the fixture ONCE,
    // here, as an oracle independent of the loader under test.
    let raw: Value = parse("v0.7.5 summary", LEGACY_SUMMARY);
    assert_eq!(
        report.run_id,
        raw["run_id"].as_str().unwrap(),
        "run_id must survive the round-trip, not just parse"
    );
    assert_eq!(
        report.export_name,
        raw["export_name"].as_str().unwrap(),
        "export_name must survive the round-trip"
    );
    assert_eq!(
        report.status,
        raw["status"].as_str().unwrap(),
        "status must survive the round-trip"
    );
    assert_eq!(
        report.total_rows,
        raw["total_rows"].as_i64().unwrap(),
        "total_rows must survive the round-trip — a silently-defaulted 0 here is \
         what makes a dropped field invisible"
    );
}
