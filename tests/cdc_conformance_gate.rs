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
        // #99: distinct from vanished_anchor_loud — the checkpoint FILE itself is
        // corrupt/truncated (not valid JSON), not a valid checkpoint whose
        // server-side anchor vanished. `.ok().flatten()` swallowed it into "no
        // checkpoint" → re-anchor at 'current' → silent gap. Every engine loads
        // the checkpoint through the shared `Position::load` (cdc_job resume-plan)
        // before anchoring; PG/MSSQL also load it at their own from-position site.
        // Each per-engine test proves that engine's run reaches the loud guard.
        "corrupt_checkpoint_loud",
        Test("fn cdc_corrupt_checkpoint_fails_loud_not_silently_absent"),
        Test("fn pg_cdc_corrupt_checkpoint_fails_loud_not_silently_absent"),
        Test("fn mssql_cdc_corrupt_checkpoint_fails_loud_not_silently_absent"),
        Test("fn roast_corrupt_checkpoint_fails_loudly_not_silent_reanchor"),
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
/// Capture markers DERIVED from the harness sources: the name of every
/// `pub fn` starting with run/cli/spawn/drain in the rig and the runner
/// module, as a `name(` spelling. A new runner method is born graded — the
/// failure mode this replaces is the hand dictionary silently lagging the
/// rig's API (bughunt find #3: ~28 CDC tests ungraded).
/// The derived capture-marker SET is pinned exactly — the >=12 floor alone
/// left 9 markers of silent-shrink headroom (r2 bughunt: renaming the 7
/// runner.rs wrappers to a non-prefix spelling dropped 21 -> 14, the floor
/// stayed quiet, and 14 real CDC tests flipped to ungraded). A lost marker
/// SKIPS tests rather than failing them, so shrink must be loud here.
/// Renaming/removing a runner is fine — update this list in the same commit.
#[test]
fn derived_capture_marker_set_is_pinned() {
    let expected: Vec<&str> = vec![
        "cli(",
        "cli_env(",
        "drain_and_read(",
        "run(",
        "run_and_read(",
        "run_args(",
        "run_args_env(",
        "run_expect_fail(",
        "run_ok(",
        "run_ok_capture(",
        "run_rivet(",
        "run_rivet_args_bounded(",
        "run_rivet_args_bounded_env(",
        "run_rivet_bounded(",
        "run_rivet_env(",
        "run_rivet_export(",
        "run_rivet_ok(",
        "run_rivet_with_warn_log(",
        "run_with_env(",
        "run_with_envs(",
        "run_with_envs_bounded(",
        "spawn_args_env(",
    ];
    let derived = derived_capture_markers();
    let derived_refs: Vec<&str> = derived.iter().map(|s| s.as_str()).collect();
    assert_eq!(
        derived_refs, expected,
        "the derived capture-marker set changed — if a runner was renamed or \
         added on purpose, update this pin in the same commit; if not, the \
         derivation regex or the harness layout rotted"
    );
}

/// Clip a `#[test]`-split span to the FIRST function's body (attrs +
/// signature + brace-matched block). Everything after — helper fns, the
/// next test's doc comment — belongs to no test and must not lend markers
/// to this one.
fn clip_to_first_fn_body(raw: &str) -> &str {
    let Some(fn_pos) = raw.find("fn ") else {
        return raw;
    };
    let Some(open_rel) = raw[fn_pos..].find('{') else {
        return raw;
    };
    let start = fn_pos + open_rel;
    // STRING-AWARE brace counting: the r3 byte counter counted braces inside
    // literals, so any corrupt-checkpoint fixture (`b"{ truncated"`) left the
    // depth unbalanced and the clip silently no-opped on exactly those tests
    // (r4 bughunt: 6 of 153 chunks; the opposite polarity — an unmatched `}`
    // in a string — would TRUNCATE the chunk and silently ungrade the test).
    // Handles "…" and b"…" with \-escapes, char literals, r"…"/r#"…"#
    // raw strings, // line comments, and /* */ block comments.
    let bytes = raw.as_bytes();
    let mut depth = 0usize;
    let mut i = start;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return &raw[..i + 1];
                }
            }
            b'"' => {
                // plain (or byte) string: skip to the unescaped closing quote
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 1,
                        b'"' => break,
                        _ => {}
                    }
                    i += 1;
                }
            }
            b'r' | b'b'
                if i + 1 < bytes.len()
                    && (bytes[i + 1] == b'"' || bytes[i + 1] == b'#' || bytes[i + 1] == b'r') =>
            {
                // possible raw / byte / byte-raw string: br#"…"#, r#"…"#, r"…", b"…"
                let mut j = i + 1;
                if bytes[j] == b'r' {
                    j += 1; // br…
                }
                let mut hashes = 0;
                while j < bytes.len() && bytes[j] == b'#' {
                    hashes += 1;
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b'"' {
                    if bytes[i] == b'b' && bytes[i + 1] == b'"' {
                        // b"…" is handled by the '"' arm next iteration; the
                        // 'b' itself is inert
                    } else {
                        // raw string: scan for `"` followed by `hashes` #s
                        j += 1;
                        'raw: while j < bytes.len() {
                            if bytes[j] == b'"' {
                                let mut k = 0;
                                while k < hashes
                                    && j + 1 + k < bytes.len()
                                    && bytes[j + 1 + k] == b'#'
                                {
                                    k += 1;
                                }
                                if k == hashes {
                                    i = j + hashes;
                                    break 'raw;
                                }
                            }
                            j += 1;
                        }
                        if j >= bytes.len() {
                            return raw; // unterminated — bail to the old behavior
                        }
                    }
                }
            }
            b'\'' => {
                // char literal (or lifetime — a lifetime has no closing quote
                // within 3 bytes with an escape shape, so bound the scan)
                if i + 2 < bytes.len() && bytes[i + 1] == b'\\' {
                    i += 3; // '\x' escape form start; closing quote next
                    while i < bytes.len() && bytes[i] != b'\'' {
                        i += 1;
                    }
                } else if i + 2 < bytes.len() && bytes[i + 2] == b'\'' {
                    i += 2; // 'c'
                }
                // else: lifetime like 'a — leave as-is
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'/' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                let mut nest = 1;
                i += 2;
                while i + 1 < bytes.len() && nest > 0 {
                    if bytes[i] == b'/' && bytes[i + 1] == b'*' {
                        nest += 1;
                        i += 1;
                    } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        nest -= 1;
                        i += 1;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    raw
}

/// The clipper itself must not lend a neighbor's marker — RED against the
/// unclipped split (the helper text below would leak into the chunk).
#[test]
fn chunk_clipper_drops_trailing_helper_text() {
    let span = "\n#[ignore]\nfn my_test() {\n    rig.run_ok();\n}\n\nfn helper_parquet_ids(dir: &Path) -> Vec<i64> {\n    duckdb_dir_parquet_ids(dir)\n}\n";
    let clipped = clip_to_first_fn_body(span);
    assert!(clipped.contains("run_ok"), "the test body itself stays");
    assert!(
        !clipped.contains("parquet_ids"),
        "the trailing helper must be clipped, not credited to the test: {clipped}"
    );
}

/// Braces inside STRING literals must not unbalance the clip — the standard
/// corrupt-checkpoint fixture writes `b"{ truncated"`, which no-opped the r3
/// byte counter on exactly those tests (r4 bughunt; RED against it).
#[test]
fn chunk_clipper_ignores_braces_in_literals() {
    let span = concat!(
        "\nfn corrupt_test() {\n",
        "    std::fs::write(&ckpt, b\"{ truncated\").unwrap();\n",
        "    let y = r#\"source: { type: pg }\n  unbalanced { here\"#;\n",
        "    // a } in a comment\n",
        "    rig.run_ok();\n",
        "}\n\n",
        "fn helper_parquet_ids() {}\n"
    );
    let clipped = clip_to_first_fn_body(span);
    assert!(clipped.contains("run_ok"), "body stays: {clipped}");
    assert!(
        !clipped.contains("helper_parquet_ids"),
        "literal braces must not extend the chunk into the helper: {clipped}"
    );
}

fn derived_capture_markers() -> &'static Vec<String> {
    use std::sync::OnceLock;
    static MARKERS: OnceLock<Vec<String>> = OnceLock::new();
    MARKERS.get_or_init(|| {
        let mut out = Vec::new();
        // The rig is a directory module (role split: render/materialize/
        // invoke/oracle) — scan every .rs under it plus the runner module.
        // The hollowing floor below fired the moment rig.rs became rig/ —
        // exactly the failure mode it exists for.
        let mut sources: Vec<std::path::PathBuf> = std::fs::read_dir("tests/common/rig")
            .expect("derive capture markers: read tests/common/rig")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "rs"))
            .collect();
        sources.push("tests/common/runner.rs".into());
        for src in sources {
            let text = std::fs::read_to_string(&src)
                .unwrap_or_else(|e| panic!("derive capture markers: read {}: {e}", src.display()));
            for line in text.lines() {
                let l = line.trim_start();
                if let Some(rest) = l.strip_prefix("pub fn ")
                    && let Some(name) = rest.split('(').next()
                    && ["run", "cli", "spawn", "drain"]
                        .iter()
                        .any(|p| name.starts_with(p))
                {
                    out.push(format!("{name}("));
                }
            }
        }
        out.sort();
        out.dedup();
        // An empty or shrunken derivation would silently hollow the gate
        // (runs_capture=false SKIPS a test rather than failing it) — so the
        // floor is a hard panic, and the trickiest historical miss is pinned
        // by name: the trailing 's' that defeated the run_with_env( substring.
        assert!(
            out.len() >= 12,
            "capture-marker derivation collapsed to {} markers — the harness \
             sources moved or the regex rotted; the gate would silently skip \
             every CDC test: {out:?}",
            out.len()
        );
        assert!(
            out.iter().any(|m| m == "run_with_envs("),
            "derived markers must include run_with_envs( — the spelling whose \
             absence hid the crash-injection tests: {out:?}"
        );
        out
    })
}

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
    // CONTENT net on top of the name filter: a CDC test in a file named
    // without "cdc" was invisible to this gate (r4 bughunt —
    // live_source_parity_sweep shipped exactly that shape). Any live file
    // whose text constructs a CDC rig or declares a cdc-mode config gets
    // scanned too, whatever it is called.
    for e in fs::read_dir(root.join("tests/live"))
        .unwrap()
        .filter_map(|e| e.ok())
    {
        let n = e.file_name().to_string_lossy().into_owned();
        let path = format!("tests/live/{n}");
        if !n.ends_with(".rs") || files.contains(&path) {
            continue;
        }
        let text = fs::read_to_string(e.path()).unwrap_or_default();
        if [
            "mysql_cdc(",
            "pg_cdc(",
            "mssql_cdc(",
            "mongo_cdc(",
            "mode: cdc",
            "mode(\"cdc\")",
        ]
        .iter()
        .any(|m| text.contains(m))
        {
            files.push(path);
        }
    }
    files.sort();
    assert!(
        files.len() >= 10,
        "glob found only {} cdc files — the discovery itself regressed",
        files.len()
    );
    for file in files {
        let src = fs::read_to_string(root.join(&file)).unwrap();
        // Split on test attributes, then CLIP each chunk to the first fn's
        // body via brace counting. The raw span-to-next-#[test] merged any
        // helper fn (and the next test's doc comment) into the PRECEDING
        // test's chunk — a helper named `walkdir_parquet_ids` sat between two
        // tests and its `parquet_` substring credited the previous test with
        // an outcome it never asserted; a hollowed body (run_ok(); no
        // asserts) graded green off the neighbor's text (r3 bughunt,
        // reproduced against live_cdc_mongo).
        for raw in src.split("#[test]").skip(1) {
            let chunk = clip_to_first_fn_body(raw);
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
                // DERIVED, not hand-listed: every `pub fn run*/cli*/spawn*/
                // drain*` in the rig and the runner module is a capture
                // spelling by construction. The hand dictionary drifted twice
                // in a month (run_rivet_ok, then the whole run_args family —
                // ~28 tests silently ungraded); "derive the enumerated
                // dimension, never type it in".
                || derived_capture_markers()
                    .iter()
                    .any(|m| chunk.contains(m.as_str()));
            // Outcome = the test READS BACK what the capture produced (files,
            // destination listing, or the state DB) — not merely that the
            // process exited 0.
            let asserts_outcome = chunk.contains("manifest_rows")
                // `cdc_id_ops` (-> `read_cdc_changes`, arrow straight off the
                // parquet) is the STRONGEST marker in this list and was missing
                // from it: a test asserting only that — i.e. reading the
                // delivered rows and nothing else — was reported as asserting no
                // outcome, while `manifest_rows` alone (rivet's own summary of
                // its own writes) counted. Found when the first test to use the
                // independent reader ALONE was rejected here (2026-08-17).
                || chunk.contains("cdc_id_ops")
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
                // Registered when the capture-marker fill (bughunt find #3)
                // surfaced 11 tests whose ORACLES were real but unlisted —
                // each spelling verified by reading the test, not guessed:
                // the DuckDB re-read helper family (duckdb_dir_csv_id_set,
                // duckdb_dir_parquet_ids, duckdb_total_parquet_rows, ...) —
                // an independent reader, the strongest oracle class here;
                || chunk.contains("duckdb_")
                // reading the manifest SIDECAR back (source-identity /
                // snapshot-leg tests assert on its recorded fields);
                || chunk.contains("manifest.json")
                // the mssql change-row replay oracle (read_cdc_rows' sibling);
                || chunk.contains("read_cdc_changes(")
                // The read-back DERIVED value the scenario smokes assert on.
                // drain_and_read(/run_and_read( themselves are NOT outcome
                // markers: they sit in the derived CAPTURE set too, so a
                // `let _ = rig.run_and_read();` with the result discarded
                // would self-satisfy the gate (r2 bughunt find).
                || chunk.contains("num_rows()")
                // …or typed CELL reads off the returned batches (downcasting a
                // column and asserting on its values — the TOAST-recovery test).
                || chunk.contains("downcast_ref::<")
                // decoding the CLI's NDJSON event stream and asserting on the
                // decoded events (the cdc-cli termination/backlog tests);
                || chunk.contains("serde_json::from_str::<serde_json::Value>")
                // Parquet re-read helpers (tests/common/parquet.rs): the seq
                // helper reads __seq/__pos/counter columns back with DuckDB;
                // deduped_current_sum folds a re-read change log. Registered
                // when the rig migration made two mssql tests VISIBLE to this
                // gate for the first time — they ran captures via a helper
                // (`run_rivet_ok`) the capture list never matched, so the gate
                // had never graded them at all.
                || chunk.contains("assert_intra_transaction_seq(")
                || chunk.contains("deduped_current_sum(")
                // The SERVER's own slot position — the strongest oracle a
                // slot-release test can have (roast_pg_cdc_empty_transaction_
                // churn asserts confirmed_flush_lsn advanced past the churn,
                // asked of PostgreSQL, never of rivet's counters).
                || chunk.contains("confirmed_flush_lsn")
                // A REFUSAL test's outcome is the named diagnostic, not a
                // delivery: the compressed-binlog test asserts stderr names
                // the event AND the setting (actionable), which is exactly the
                // refusal contract. `status.success()` alone stays a
                // non-marker; a named-diagnostic assertion does not.
                || chunk.contains("the stream-time refusal must name")
                // `rivet validate` re-reads the destination (parts + manifest +
                // checksums) — an independent read-back, not the capture's exit.
                || chunk.contains("args([\"validate\"")
                // …and the same command driven through the rig, which supplies
                // the config flag itself.
                || chunk.contains("cli(&[\"validate\"")
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
                || chunk.contains("duckdb_dir_parquet_distinct_strings(")
                // DuckDB read-backs. The strongest oracle in the suite for a
                // COMPLETENESS claim: DuckDB does not share the parquet crate
                // rivet ENCODES with, so a fault in that shared encode/decode
                // path cannot cancel out the way it can for a re-read through
                // rivet's own reader. Registered here rather than routed around
                // the gate, per the rule these markers exist to enforce.
                || chunk.contains("duckdb_distinct_set(")
                || chunk.contains("duckdb_distinct_i64_set(")
                || chunk.contains("duckdb_assert_complete(")
                || chunk.contains("duckdb_assert_at_least_once(")
                || chunk.contains("duckdb_assert_rows_and_distinct(")
                // `Rig::assert_complete` is the same oracle behind a method.
                || chunk.contains("assert_complete(")
                || chunk.contains("read_all(")
                || chunk.contains("read_all_parts(")
                // run_and_read( is deliberately NOT here: it is a CAPTURE
                // marker (derived set), and a dual-role spelling lets
                // `let _ = rig.run_and_read();` self-satisfy the gate. The
                // outcome is the assertion on the returned batches —
                // num_rows()/assert markers above (r2 bughunt find).
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
                // Reads the persisted CDC checkpoint/anchor bytes back — the
                // client-anchor engines' state-DB oracle. The spelling is the
                // READ, not the accessor: the bare `checkpoint())` substring
                // also matched pure builder configuration
                // (`.checkpoint_path(rig.checkpoint())`, live_cdc_mongo.rs) and
                // a setup WRITE (`write_checkpoint(.., &rig.checkpoint())`),
                // so a test could satisfy the gate without reading anything
                // back (r2 bughunt find).
                || chunk.contains("fs::read(rig.checkpoint())")
                || chunk.contains("read_to_string(rig.checkpoint())")
                || chunk.contains("fs::read(&ckpt")
                || chunk.contains("read_to_string(&ckpt");
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

// ── the engine set is DERIVED, not typed in ───────────────────────────────────
// The per-case columns below are a hand-written 4-tuple. That is fine as a shape,
// but nothing tied it to reality: a fifth CDC engine could ship with ZERO
// conformance rows and this gate would stay green, because it only ever asked
// about the four engines its author already knew. The sibling
// `tests/offline/chunking_matrix_guard.rs` had already solved this — it derives
// its columns from `SourceType` itself — so the fix is to use the same seam here
// rather than invent another.
//
// Parsed from the source text (not `strum`) so the gate stays dependency-free and
// so it also fails if the enum is MOVED, which a derive-macro reflection would not
// notice.
fn enum_variants(rel: &str, enum_name: &str) -> std::collections::HashSet<String> {
    let repo_root = || std::path::Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
    let text = std::fs::read_to_string(repo_root().join(rel))
        .unwrap_or_else(|e| panic!("read {rel}: {e}"));
    let needle = format!("enum {enum_name} {{");
    let start = text
        .find(&needle)
        .unwrap_or_else(|| panic!("`{needle}` not found in {rel}"));
    let body = &text[start + needle.len()..];
    let mut out = std::collections::HashSet::new();
    let mut depth = 1usize; // already inside the enum's `{`
    for line in body.lines() {
        let t = line.trim_start();
        let at_variant_depth = depth == 1;
        // Update depth AFTER classifying this line (an opening `{` affects the NEXT
        // lines, not this variant's own line). Stop at the enum's closing brace.
        let opens = t.matches('{').count();
        let closes = t.matches('}').count();
        if at_variant_depth && !t.is_empty() && !t.starts_with("//") && !t.starts_with('#') {
            let ident: String = t
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            if ident.chars().next().is_some_and(|c| c.is_ascii_uppercase()) {
                out.insert(ident);
            }
        }
        depth = depth + opens - closes.min(depth);
        if depth == 0 {
            break; // enum's closing brace
        }
    }
    out
}

/// [`enum_variants`] lowercased — `SourceType::Postgres` → `"postgres"`,
/// `ExportTarget::DuckDb` → `"duckdb"`, matching the matrix column labels exactly.
fn enum_variants_lowercased(rel: &str, enum_name: &str) -> std::collections::HashSet<String> {
    enum_variants(rel, enum_name)
        .into_iter()
        .map(|v| v.to_ascii_lowercase())
        .collect()
}

/// A new `SourceType` variant must force a column into this gate.
///
/// Without this, adding an engine is silent here: `CASES` keeps its 4-tuples, the
/// loop below keeps its four `(engine, expect, hay)` entries, every existing row
/// still matches, and the engine ships with no conformance obligation at all —
/// the un-enumerated-sibling hole. The assertion is deliberately EQUALITY, not
/// containment: an engine dropped from the gate is as wrong as one never added.
#[test]
fn conformance_columns_cover_every_source_engine() {
    let declared: std::collections::HashSet<String> = ENGINE_COLUMNS
        .iter()
        .map(|e| e.to_ascii_lowercase())
        .collect();
    let variants = enum_variants_lowercased("src/config/source.rs", "SourceType");
    // Parse sanity: a silent parse failure would make this vacuously pass.
    assert!(
        variants.len() >= 4 && variants.contains("postgres") && variants.contains("mongo"),
        "SourceType parse looks wrong ({variants:?}) — the guard would under-require"
    );
    assert_eq!(
        declared, variants,
        "the CDC conformance gate's engine columns must equal the SourceType variants;          add the engine's column to CASES (and its rows) or remove the stale one"
    );
}

/// The engine names this gate grades, in the order the `CASES` tuples use them.
const ENGINE_COLUMNS: [&str; 4] = ["mysql", "postgres", "mssql", "mongo"];
