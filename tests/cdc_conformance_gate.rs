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

/// (case, mysql, postgres, mssql, mongo). Test names live in
/// tests/live/live_cdc.rs (mysql + pg), tests/live/live_cdc_mssql.rs, and
/// tests/live/live_cdc_mongo.rs.
const CASES: &[(&str, Expect, Expect, Expect, Expect)] = &[
    (
        "intra_transaction_seq",
        Test("fn cdc_intra_transaction_updates_get_distinct_seq"),
        Test("fn pg_cdc_intra_transaction_updates_get_distinct_seq"),
        Test("fn mssql_cdc_intra_transaction_updates_get_distinct_seq"),
        NA(
            "a Mongo change stream gives EVERY event a distinct, order-preserving \
            resume token (__pos), even within one transaction, so __seq is always \
            0 and the total order is __pos alone — the SQL shared-__pos + __seq \
            tiebreak is unrepresentable (mongo_cdc_soak proves __pos-only dedup)",
        ),
    ),
    (
        "sum_reconciles_intra_txn",
        Test("fn cdc_sum_reconciles_across_intra_txn_updates"),
        Test("fn pg_cdc_sum_reconciles_across_intra_txn_updates"),
        Test("fn mssql_cdc_sum_reconciles_across_intra_txn_updates"),
        Test("fn mongo_cdc_soak_dedup_matches_source"),
    ),
    (
        "resume_two_run",
        Test("fn cdc_resume_captures_only_new_changes"),
        Test("fn pg_cdc_resume_captures_only_new_changes"),
        Test("fn mssql_cdc_resume_captures_only_new_changes"),
        Test("fn mongo_cdc_capture_resume"),
    ),
    (
        "idle_first_run",
        Test("fn cdc_idle_first_run_then_change_is_captured"),
        Test("fn pg_cdc_idle_first_run_then_change_is_captured"),
        Test("fn mssql_cdc_idle_first_run_then_change_is_captured"),
        Test("fn mongo_cdc_idle_first_run_then_change_is_captured"),
    ),
    (
        "crash_before_ack",
        Test("fn cdc_crash_after_flush_before_ack"),
        Test("fn pg_cdc_crash_after_flush_before_ack"),
        Test("fn mssql_cdc_crash_before_checkpoint"),
        Test("fn mongo_cdc_crash_after_flush_before_ack"),
    ),
    (
        "full_type_matrix",
        Test("fn cdc_full_type_matrix_matches_batch"),
        Test("fn pg_cdc_full_type_matrix_matches_batch"),
        Test("fn mssql_cdc_full_type_matrix_matches_batch"),
        NA(
            "CDC and batch render `document` through the SAME `document_to_json` \
            blob serializer — the batch mongo_batch_type_fidelity test pins the \
            type surface (large Int64 / Decimal128 verbatim); there is no per-op \
            typing that could diverge CDC from batch",
        ),
    ),
    (
        "update_delete_typed",
        Test("fn cdc_update_and_delete_carry_full_types"),
        Test("fn pg_cdc_update_and_delete_carry_full_types"),
        Test("fn mssql_cdc_update_and_delete_carry_full_types"),
        Test("fn mongo_cdc_update_and_delete_carry_document"),
    ),
    (
        "initial_snapshot",
        Test("fn cdc_initial_snapshot_covers_preexisting_rows"),
        Test("fn pg_cdc_initial_snapshot_covers_preexisting_rows"),
        Test("fn mssql_cdc_initial_snapshot_covers_preexisting_rows"),
        Test("fn mongo_cdc_initial_snapshot_covers_preexisting_rows"),
    ),
    (
        "vanished_anchor_loud",
        Test("fn cdc_resume_from_missing_binlog_fails_loudly"),
        Test("fn pg_cdc_vanished_slot_with_checkpoint_fails_loudly"),
        Test("fn mssql_cdc_resume_past_retention_errors"),
        NA("a resume token older than the oplog surfaces the server's \
            ChangeStreamHistoryLost error DIRECTLY (rivet does not swallow it, no \
            silent re-anchor); forcing an oplog rollover in the gate is \
            impractical — the loud failure is the driver's"),
    ),
    (
        "mixed_transaction_boundary",
        Test("fn cdc_mixed_transaction_ending_on_uncaptured_table"),
        Test("fn pg_cdc_mixed_transaction_ending_on_uncaptured_table"),
        Test("fn mssql_cdc_mixed_transaction_and_qualified_table"),
        Test("fn mongo_cdc_mixed_transaction_ending_on_uncaptured_table"),
    ),
    (
        "schema_qualified_table",
        Test("fn cdc_schema_qualified_table_config_captures_events"),
        Test("fn pg_cdc_schema_qualified_table_config_captures_events"),
        Test("fn mssql_cdc_mixed_transaction_and_qualified_table"),
        NA(
            "Mongo addresses a collection by name within the URL's database — no \
            schema.table qualifier to parse or route",
        ),
    ),
    (
        "non_utc_session",
        Test("fn cdc_non_utc_server_timezone_matches_batch"),
        Test("fn pg_cdc_non_utc_database_timezone_matches_batch"),
        NA(
            "datetime is naive and datetimeoffset carries its offset explicitly — \
            no MSSQL rendering is shaped by session/server timezone",
        ),
        NA(
            "BSON dates are UTC milliseconds rendered as ISO-8601 in extended \
            JSON — no session/server timezone shapes the document text (unlike \
            test_decoding's session-zone rendering)",
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
        NA("same engine-agnostic cdc_job marker path, pinned once on MySQL"),
    ),
    (
        "multi_table_one_stream",
        Test("fn cdc_multi_table_stream_one_binlog_connection"),
        Test("fn pg_cdc_multi_table_stream_uses_one_slot"),
        NA(
            "config validation rejects `tables:` for MSSQL (one capture \
            instance per stream)",
        ),
        NA(
            "db.watch() is a single WHOLE-DATABASE stream by construction — a \
            `tables:` config routes the one stream to N collections; the \
            one-stream property is structural, not per-engine connection plumbing",
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
        NA(
            "same shared sink/commit seam; Mongo is a client-anchor engine like \
            MySQL (crash_before_ack re-read is proven in mongo_cdc_crash)",
        ),
    ),
    (
        "gremlin_network_cut_mid_stream",
        Test("fn gremlin_cdc_binlog_cut_mid_drain_fails_loud_then_recovers"),
        NA(
            "PG polls via SQL per cycle (no long-lived replication stream to \
            cut); connection loss = a failed poll, covered by chaos suite",
        ),
        NA("MSSQL polls change tables via SQL; same as PG"),
        NA(
            "a cut on the tailable stream surfaces as a failed next_change, loud, \
            then resumes from the persisted token — the drain/recover seam is \
            shared, pinned once on MySQL",
        ),
    ),
    (
        "gremlin_destination_outage_mid_drain",
        Test("fn gremlin_cdc_gcs_upload_cut_fails_loud_then_recovers_without_clobber"),
        NA(
            "destination seam is engine-agnostic (shared commit path); pinned \
            once via the MySQL stream",
        ),
        NA("same engine-agnostic destination seam"),
        NA("same engine-agnostic destination seam (shared commit path)"),
    ),
    (
        "gremlin_checkpoint_write_failure",
        Test("fn gremlin_cdc_checkpoint_write_failure_is_loud_and_lossless"),
        NA(
            "checkpoint save is the shared Position::save path; PG's primary \
            anchor is the slot (server-side)",
        ),
        NA("same shared Position::save path; pinned once on MySQL"),
        NA("same shared Position::save path (Mongo persists the token there too)"),
    ),
    (
        "concurrent_writers_mbt",
        Test("fn cdc_concurrent_writers_capture_converges_to_source_state"),
        NA(
            "interleaving pressure is anchored on the client engine; the sink \
            merge logic under test is shared",
        ),
        NA("same shared merge; MSSQL's change-table ordering is server-side"),
        NA(
            "the shared merge; Mongo's convergence-to-source under mixed \
            transactional writes is proven in mongo_cdc_soak",
        ),
    ),
    (
        "fault_point_sweep",
        Test("fn cdc_fault_point_sweep_every_phase_boundary_recovers"),
        NA(
            "the phase boundaries live in the shared sink/run_capture; swept \
            once on the client engine",
        ),
        NA("same shared boundaries"),
        NA("same shared sink/run_capture boundaries"),
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
        NA(
            "same anchoring; Mongo's blob passes the same readers via the batch \
            type-fidelity test, and CDC shares the document_to_json renderer",
        ),
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
        NA(
            "Mongo's __pos (resume token) IS commit order by construction — \
            mongo_cdc_soak's __pos-ordered dedup matching source proves it",
        ),
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
        NA("same one-arithmetic-anchor argument; Mongo values ride the blob"),
    ),
    (
        "gremlin_capture_job_stall",
        NA("MySQL has no external capture job — the binlog IS the capture"),
        NA("PG has no external capture job — the slot decodes on read"),
        Test("fn gremlin_mssql_capture_job_stall_loses_nothing"),
        NA("Mongo has no external capture job — the oplog IS the capture, like MySQL"),
    ),
];

#[test]
fn every_cdc_engine_covers_every_conformance_case() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut mysql_pg = fs::read_to_string(root.join("tests/live/live_cdc.rs")).unwrap();
    mysql_pg.push_str(&fs::read_to_string(root.join("tests/live/gremlin_cdc.rs")).unwrap());
    mysql_pg.push_str(&fs::read_to_string(root.join("tests/live/live_cdc_golden.rs")).unwrap());
    mysql_pg.push_str(&fs::read_to_string(root.join("tests/live/live_cdc_oracle.rs")).unwrap());
    mysql_pg.push_str(&fs::read_to_string(root.join("tests/live/live_cdc_mbt.rs")).unwrap());
    let mssql = fs::read_to_string(root.join("tests/live/live_cdc_mssql.rs")).unwrap();
    let mongo = fs::read_to_string(root.join("tests/live/live_cdc_mongo.rs")).unwrap();

    let mut missing = Vec::new();
    for (case, my, pg, ms, mo) in CASES {
        for (engine, expect, hay) in [
            ("mysql", my, &mysql_pg),
            ("postgres", pg, &mysql_pg),
            ("mssql", ms, &mssql),
            ("mongo", mo, &mongo),
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
    // Every `tests/live/*cdc*.rs` file is scanned AUTOMATICALLY — a hardcoded
    // list silently exempted `live_cdc_replica.rs` (its capture ran outside the
    // gate for months; the matrix audit caught it). Files whose name does not
    // carry `cdc` but still run CDC captures are listed explicitly.
    let mut files: Vec<String> = fs::read_dir(root.join("tests/live"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".rs") && n.contains("cdc"))
        .map(|n| format!("tests/live/{n}"))
        .collect();
    files.push("tests/live/live_batch_switch_golden.rs".to_string());
    files.sort();
    assert!(
        files.len() >= 10,
        "glob found only {} cdc files — the discovery itself regressed",
        files.len()
    );
    for file in files {
        let src = fs::read_to_string(root.join(&file)).unwrap();
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
            // Capture spellings: the rig's runners plus the two legacy
            // forms (run_cdc helper, direct Command) bespoke sites keep.
            let runs_capture = chunk.contains("run_cdc(")
                || chunk.contains("Command::new(RIVET_BIN)")
                || chunk.contains("run_ok(")
                || chunk.contains("run_with_env(")
                || chunk.contains("run_expect_fail(")
                || chunk.contains("run_and_read(");
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
                // NOTE: `status.success()` is deliberately NOT a marker — it is
                // exactly the "process exited 0" non-outcome this gate exists to
                // reject (a 0-row capture exits 0). It sat in this dictionary for
                // months, hollowing the gate; the matrix audit removed it. A
                // must-fail test's NEGATIVE exit assert IS an outcome (below).
                || chunk.contains("!out.status.success()")
                || chunk.contains("!res.status.success()")
                || chunk.contains("!output.status.success()")
                || chunk.contains("read_cdc_rows(") // replica-suite replay oracle
                // `rivet validate` re-reads the destination (parts + manifest +
                // checksums) — an independent read-back, not the capture's exit.
                || chunk.contains("args([\"validate\"")
                // External-warehouse oracles (the strongest read-back in the
                // suite): the CDC parquet is loaded and queried by another engine.
                || chunk.contains("duckdb_run_sql_json(")
                || chunk.contains("clickhouse_run_sql_json(")
                // Read-back helpers the newer suites use. This dictionary
                // is INTENTIONALLY wide: the read-backs differ because the
                // oracles differ (manifest vs parquet vs csv vs gcs listing
                // vs doctor json) — measured during the rig standardization:
                // 16 of 17 markers are live. Do not collapse it into one
                // canonical reader; oracle diversity is the defense.
                || chunk.contains("distinct_ids(")
                || chunk.contains("distinct_int_ids(")
                || chunk.contains("read_mongo_cdc_changes(") // Mongo blob-CDC oracle
                || chunk.contains("dir_parquet_distinct_strings(")
                || chunk.contains("read_all(")
                || chunk.contains("read_all_parts(")
                || chunk.contains("stage_metrics(")
                || chunk.contains("collect(") && chunk.contains("Metrics")
                || chunk.contains("all_ok") // doctor --json contract
                || chunk.contains("storage/v1") // GCS listing read-back
                || chunk.contains("ParquetRecordBatchReaderBuilder") // inline part read-back
                // A must-fail capture asserts the NEGATIVE outcome: `run_expect_fail`
                // requires a non-zero exit, so a silent 0-row success CANNOT satisfy
                // it (the exact risk this gate guards). E.g. the corrupt-checkpoint
                // roast — the run must fail loudly, not re-anchor and exit 0.
                || chunk.contains("run_expect_fail(")
                // Reads the persisted CDC checkpoint/anchor bytes back
                // (`std::fs::read(rig.checkpoint())`) — the client-anchor engines'
                // state-DB oracle. A checkpoint-advance/pin assertion is a real
                // outcome (the committed log position), distinct from row read-back.
                || chunk.contains("checkpoint())");
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
