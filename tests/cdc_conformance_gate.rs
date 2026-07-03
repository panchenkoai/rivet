//! CDC engine conformance gate — the hard, offline-enforced checklist.
//!
//! Every CDC engine must carry a live test for every conformance case (or an
//! explicit N/A with a reason below). The 2026-07 reliability campaign found
//! that per-engine coverage drifts silently: MySQL had the mixed-transaction
//! test, PostgreSQL had the qualified-name test — and each engine's missing
//! case was exactly where a real bug lived (ultrareview bug_002/bug_004).
//! This test scans the live-test SOURCES for a per-(engine × case) function
//! name and fails compilation-adjacent (plain `cargo test`) when a new engine
//! or a new case lands without its row. Adding a case: add it to CASES with
//! the three expectations; adding an engine: every case must decide.

use std::fs;

#[derive(Clone, Copy)]
enum Expect {
    /// A test fn whose name contains this substring must exist in the file.
    Test(&'static str),
    /// Deliberately not applicable — the reason is printed on failure so the
    /// exemption stays justified, never silent. HYGIENE (M6): an NA claiming
    /// "shared/engine-agnostic path" rots the day that path forks per engine —
    /// when touching engine dispatch in the sink/cdc_job, re-read every NA
    /// below and convert the invalidated ones into Test rows.
    NA(&'static str),
}
use Expect::{NA, Test};

/// (case, mysql, postgres, mssql). Test names live in tests/live/live_cdc.rs
/// (mysql + pg) and tests/live/live_cdc_mssql.rs.
const CASES: &[(&str, Expect, Expect, Expect)] = &[
    (
        "resume_two_run",
        Test("fn cdc_resume_captures_only_new_changes"),
        Test("fn pg_cdc_resume_captures_only_new_changes"),
        Test("fn mssql_cdc_resume_captures_only_new_changes"),
    ),
    (
        "idle_first_run",
        Test("fn cdc_idle_first_run_then_change_is_captured"),
        Test("fn pg_cdc_idle_first_run_then_change_is_captured"),
        Test("fn mssql_cdc_idle_first_run_then_change_is_captured"),
    ),
    (
        "crash_before_ack",
        Test("fn cdc_crash_after_flush_before_ack"),
        Test("fn pg_cdc_crash_after_flush_before_ack"),
        Test("fn mssql_cdc_crash_before_checkpoint"),
    ),
    (
        "full_type_matrix",
        Test("fn cdc_full_type_matrix_matches_batch"),
        Test("fn pg_cdc_full_type_matrix_matches_batch"),
        Test("fn mssql_cdc_full_type_matrix_matches_batch"),
    ),
    (
        "update_delete_typed",
        Test("fn cdc_update_and_delete_carry_full_types"),
        Test("fn pg_cdc_update_and_delete_carry_full_types"),
        Test("fn mssql_cdc_update_and_delete_carry_full_types"),
    ),
    (
        "initial_snapshot",
        Test("fn cdc_initial_snapshot_covers_preexisting_rows"),
        Test("fn pg_cdc_initial_snapshot_covers_preexisting_rows"),
        Test("fn mssql_cdc_initial_snapshot_covers_preexisting_rows"),
    ),
    (
        "vanished_anchor_loud",
        Test("fn cdc_resume_from_missing_binlog_fails_loudly"),
        Test("fn pg_cdc_vanished_slot_with_checkpoint_fails_loudly"),
        Test("fn mssql_cdc_resume_past_retention_errors"),
    ),
    (
        "mixed_transaction_boundary",
        Test("fn cdc_mixed_transaction_ending_on_uncaptured_table"),
        Test("fn pg_cdc_mixed_transaction_ending_on_uncaptured_table"),
        Test("fn mssql_cdc_mixed_transaction_and_qualified_table"),
    ),
    (
        "schema_qualified_table",
        Test("fn cdc_schema_qualified_table_config_captures_events"),
        Test("fn pg_cdc_schema_qualified_table_config_captures_events"),
        Test("fn mssql_cdc_mixed_transaction_and_qualified_table"),
    ),
    (
        "non_utc_session",
        Test("fn cdc_non_utc_server_timezone_matches_batch"),
        Test("fn pg_cdc_non_utc_database_timezone_matches_batch"),
        NA(
            "datetime is naive and datetimeoffset carries its offset explicitly — \
            no MSSQL rendering is shaped by session/server timezone",
        ),
    ),
    (
        "empty_table_initial_converges",
        Test("fn cdc_initial_snapshot_of_an_empty_table_converges"),
        NA(
            "marker + forced skip_empty=false live in engine-agnostic cdc_job \
            code, pinned once on MySQL",
        ),
        NA("same engine-agnostic path, pinned once on MySQL"),
    ),
    (
        "multi_table_one_stream",
        Test("fn cdc_multi_table_stream_one_binlog_connection"),
        Test("fn pg_cdc_multi_table_stream_uses_one_slot"),
        NA(
            "config validation rejects `tables:` for MSSQL (one capture \
            instance per stream)",
        ),
    ),
    (
        "gremlin_sigkill_mid_drain",
        Test("fn gremlin_cdc_sigkill_mid_drain_recovers_without_gap"),
        NA(
            "the kill window exercises the shared sink/commit seam; pinned once \
            on MySQL (the engine with the client-only anchor, the hardest case)",
        ),
        NA("same shared seam; MSSQL's poll model re-reads from LSN by design"),
    ),
    (
        "gremlin_network_cut_mid_stream",
        Test("fn gremlin_cdc_binlog_cut_mid_drain_fails_loud_then_recovers"),
        NA(
            "PG polls via SQL per cycle (no long-lived replication stream to \
            cut); connection loss = a failed poll, covered by chaos suite",
        ),
        NA("MSSQL polls change tables via SQL; same as PG"),
    ),
    (
        "gremlin_destination_outage_mid_drain",
        Test("fn gremlin_cdc_gcs_upload_cut_fails_loud_then_recovers_without_clobber"),
        NA(
            "destination seam is engine-agnostic (shared commit path); pinned \
            once via the MySQL stream",
        ),
        NA("same engine-agnostic destination seam"),
    ),
    (
        "gremlin_checkpoint_write_failure",
        Test("fn gremlin_cdc_checkpoint_write_failure_is_loud_and_lossless"),
        NA(
            "checkpoint save is the shared Position::save path; PG's primary \
            anchor is the slot (server-side)",
        ),
        NA("same shared Position::save path; pinned once on MySQL"),
    ),
    (
        "cross_oracle_full_surface",
        Test("fn cdc_full_surface_cross_oracle_matches_literals"),
        NA(
            "external-reader agreement is anchored once on the client engine; \
            PG/MSSQL parquet passes the same readers via the batch \
            type_roundtrip suites, and CDC==batch is their matrix contract",
        ),
        NA("same anchoring argument"),
    ),
    (
        "event_ordering_commit_order",
        Test("fn cdc_event_order_within_and_across_transactions_is_commit_order"),
        NA(
            "part/row ordering lives in the shared sink (pinned once on the \
            client engine); PG's commit-LSN framing is unit-tested in its \
            parser",
        ),
        NA("same shared sink ordering; MSSQL's per-row LSN framing is \
            unit-tested in its adapter"),
    ),
    (
        "golden_calculated_metrics",
        Test("fn cdc_golden_fixture_tables_calculated_metrics"),
        NA(
            "the arithmetic anchor is pinned once, on the client engine; \
            cross-engine value fidelity is carried by the matrices + the \
            cross-oracle sweeps",
        ),
        NA(
            "same — one arithmetic anchor suffices; MSSQL values ride the \
            matrix + oracle contracts",
        ),
    ),
    (
        "gremlin_capture_job_stall",
        NA("MySQL has no external capture job — the binlog IS the capture"),
        NA("PG has no external capture job — the slot decodes on read"),
        Test("fn gremlin_mssql_capture_job_stall_loses_nothing"),
    ),
];

#[test]
fn every_cdc_engine_covers_every_conformance_case() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut mysql_pg = fs::read_to_string(root.join("tests/live/live_cdc.rs")).unwrap();
    mysql_pg.push_str(&fs::read_to_string(root.join("tests/live/gremlin_cdc.rs")).unwrap());
    mysql_pg.push_str(&fs::read_to_string(root.join("tests/live/live_cdc_golden.rs")).unwrap());
    mysql_pg.push_str(&fs::read_to_string(root.join("tests/live/live_cdc_oracle.rs")).unwrap());
    let mssql = fs::read_to_string(root.join("tests/live/live_cdc_mssql.rs")).unwrap();

    let mut missing = Vec::new();
    for (case, my, pg, ms) in CASES {
        for (engine, expect, hay) in [
            ("mysql", my, &mysql_pg),
            ("postgres", pg, &mysql_pg),
            ("mssql", ms, &mssql),
        ] {
            match expect {
                Test(needle) => {
                    if !hay.contains(needle) {
                        missing.push(format!("{engine} × {case}: no test matching `{needle}`"));
                    } else {
                        // Strength floor (M4): existence is not coverage — a
                        // hollowed-out test (name kept, asserts removed) must
                        // not satisfy the gate. Count assert tokens in the fn
                        // body (up to the next fn at the same nesting).
                        let start = hay.find(needle).unwrap();
                        let body_end = hay[start + needle.len()..]
                            .find("\n#[test]")
                            .map(|o| start + needle.len() + o)
                            .unwrap_or(hay.len());
                        let body = &hay[start..body_end];
                        let asserts = body.matches("assert").count();
                        if asserts == 0 {
                            missing.push(format!(
                                "{engine} × {case}: `{needle}` exists but carries NO assert \
                                 tokens — hollowed out?"
                            ));
                        }
                    }
                }
                NA(reason) => {
                    // Exemptions are visible in the source above; nothing to
                    // scan, but keep the reason referenced so it can't be an
                    // empty excuse.
                    assert!(!reason.is_empty());
                }
            }
        }
    }
    assert!(
        missing.is_empty(),
        "CDC conformance gate: every engine needs every case (or an explicit \
         NA with a reason in tests/cdc_conformance_gate.rs):\n  {}",
        missing.join("\n  ")
    );
}

/// Blind-spot №3 from the campaign: "0 rows = valid success". Three real bugs
/// (qualified routing, instance-name heuristic, the stalled commit boundary)
/// manifested as 0-row successes that a bare `run_cdc(&cfg)` + no assertion
/// would wave through. Every live CDC test that runs a capture must assert an
/// outcome: manifest rows, a batch comparison, or an explicit failure check.
#[test]
fn every_live_cdc_test_asserts_an_outcome() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut naked = Vec::new();
    for file in ["tests/live/live_cdc.rs", "tests/live/live_cdc_mssql.rs"] {
        let src = fs::read_to_string(root.join(file)).unwrap();
        // Split on test attributes; each chunk is one test body (plus tail).
        for chunk in src.split("#[test]").skip(1) {
            let name = chunk
                .lines()
                .find_map(|l| l.trim().strip_prefix("fn "))
                .unwrap_or("?")
                .split('(')
                .next()
                .unwrap_or("?")
                .to_string();
            let runs_capture = chunk.contains("run_cdc(");
            // Outcome = the test READS BACK what the capture produced (files,
            // destination listing, or the state DB) — not merely that the
            // process exited 0.
            let asserts_outcome = chunk.contains("manifest_rows")
                || chunk.contains("assert_cdc_matches_batch")
                || chunk.contains("read_one_batch")
                || chunk.contains("parquet_")
                || chunk.contains("read_csv(")
                || chunk.contains("gcs list")
                || chunk.contains("query_row")
                || chunk.contains("status.success()");
            if runs_capture && !asserts_outcome {
                naked.push(format!("{file}::{name}"));
            }
        }
    }
    assert!(
        naked.is_empty(),
        "live CDC tests running a capture without asserting an outcome \
         (0-row success would pass silently):\n  {}",
        naked.join("\n  ")
    );
}
