//! The BATCH side's oracle gate — the three CDC gates, generalised.
//!
//! Measured 2026-08-29 over 594 live capture tests: CDC graded 35% of its
//! claims with an independent oracle and batch 23%, and the batch number was
//! not the finding. THIS was: CDC carries three gates (every capture asserts an
//! outcome; a completeness-claiming NAME needs an independent oracle; a
//! two-sided census pin), and batch carried NONE. Its 390 capture tests were in
//! decent shape by the authors' discipline and by nothing else — a naked export
//! could land tomorrow with nothing to notice.
//!
//! The first survey of that same batch set claimed "43% assert no outcome at
//! all". That was the CDC dictionary's blindness, not a defect: `read_uid_set`,
//! `ParquetRecordBatchReaderBuilder`, `files_with_extension` and the batch
//! suites' own failure spellings are outcomes the CDC list never needed. Hand
//! verification put the true number of naked exports at ZERO. So this gate
//! DERIVES its markers rather than repeating that mistake in a second hand list
//! — the two hand dictionaries inside the CDC gate file drifted apart from each
//! other within one file, which is the whole argument.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Live files that are NOT the CDC set — the same split the census measurement
/// used, with the CDC gate's own content net so a batch-named file that runs a
/// CDC capture is not graded twice under different rules.
fn batch_files() -> Vec<PathBuf> {
    let mut out = Vec::new();
    for e in fs::read_dir(root().join("tests/live")).expect("read tests/live") {
        let p = e.expect("dir entry").path();
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        if !name.ends_with(".rs") || name.contains("cdc") {
            continue;
        }
        let src = fs::read_to_string(&p).unwrap_or_default();
        let cdc_shaped = [
            "mysql_cdc(",
            "pg_cdc(",
            "mssql_cdc(",
            "mongo_cdc(",
            "mode: cdc",
        ]
        .iter()
        .any(|m| src.contains(m));
        if !cdc_shaped {
            out.push(p);
        }
    }
    out.sort();
    assert!(
        out.len() >= 60,
        "found only {} batch live files — the discovery regressed and this gate \
         would grade almost nothing while looking green",
        out.len()
    );
    out
}

/// Capture spellings, DERIVED from the rig and runner exactly as the CDC gate
/// derives them — one source, so a new runner is born graded on both sides.
fn capture_markers() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut files: Vec<PathBuf> = fs::read_dir(root().join("tests/common/rig"))
        .expect("read tests/common/rig")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.push(root().join("tests/common/runner.rs"));
    for f in files {
        for line in fs::read_to_string(&f).unwrap_or_default().lines() {
            let t = line.trim_start();
            if let Some(rest) = t.strip_prefix("pub fn ")
                && let Some(name) = rest.split('(').next()
                && ["run", "cli", "spawn", "drain", "apply"]
                    .iter()
                    .any(|p| name.starts_with(p))
            {
                out.insert(format!("{name}("));
            }
        }
    }
    out.insert("Command::new(RIVET_BIN)".into());
    assert!(
        out.len() >= 12,
        "capture-marker derivation collapsed to {} — every check below would \
         skip rather than fail: {out:?}",
        out.len()
    );
    out
}

/// Read-back spellings, DERIVED from what the harness actually EXPORTS: every
/// `pub fn` in tests/common whose name says it reads data back. A hand list is
/// what produced the 43% phantom.
fn outcome_markers() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut stack = vec![root().join("tests/common")];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = fs::read_dir(&dir) else { continue };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|x| x.to_str()) != Some("rs") {
                continue;
            }
            for line in fs::read_to_string(&p).unwrap_or_default().lines() {
                let t = line.trim_start();
                if let Some(rest) = t.strip_prefix("pub fn ")
                    && let Some(name) = rest.split('(').next()
                    && [
                        "read_",
                        "duckdb_",
                        "row_census",
                        "assert_complete",
                        "manifest_rows",
                        "declared_",
                        "files_with_extension",
                        "parquet_",
                        "dir_",
                        "mc_",
                        "fake_gcs_",
                        "row_hash_matches",
                    ]
                    .iter()
                    .any(|p| name.starts_with(p))
                {
                    out.insert(format!("{name}("));
                }
            }
        }
    }
    // The LEDGER is an outcome too, and the batch suites read it several ways:
    // through StateDb, through a hand-opened `.rivet_state.db`, and (for the
    // state-layer tests) through the product's own StateStore against an
    // in-memory or Postgres backend. A test whose SUBJECT is what rivet
    // recorded is not required to also re-read parquet — demanding that of
    // `keyset_run_records_parallel_cursor_bounds_and_bytes_read` would be
    // asking the wrong question of a test that already asks a precise one.
    for extra in [
        "ParquetRecordBatchReaderBuilder",
        "read_uid_set",
        "StateDb::",
        "StateStore::",
        "metrics_row(",
        "shape_rows(",
        "downcast_ref::<",
        ".rivet_state.db",
        "state_ref(",
        // A message/diagnostic assertion: for a test whose subject IS the text
        // rivet emits (a classification, a drift report, a warning), the words
        // are the outcome — the same allowance CDC tier-1 makes.
        "contains(",
        // The PROCESS TABLE, for the one test whose subject is orphaned
        // children rather than delivered rows: `pgrep` is its read-back, and
        // demanding parquet of it would be asking the wrong question.
        "pgrep",
    ] {
        out.insert(extra.to_string());
    }
    assert!(
        out.len() >= 25,
        "outcome-marker derivation collapsed to {} — this gate would demand \
         nothing: {out:?}",
        out.len()
    );
    out
}

/// Bodies of the same-file helpers a test calls, one level deep.
///
/// One level is deliberate and stated: it covers the delegation shape that
/// actually occurs here (`fn test() { helper(arg) }`) without pretending to a
/// call-graph this gate does not have. A helper that hides its oracle two
/// levels down would read as ungraded — a false ALARM, which is the safe
/// direction for a gate.
fn helper_bodies(src: &str, test_body: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = src;
    while let Some(at) = rest.find("fn ") {
        let after = &rest[at + 3..];
        let name: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() && test_body.contains(&format!("{name}(")) {
            // The WHOLE body, by brace counting — a fixed character window is a
            // magic number that silently truncates: at 6000 it cut
            // `pool_split_cloud` (185 lines) before its oracle and reported a
            // DuckDB-graded test as ungraded.
            let mut depth = 0i32;
            let mut end = after.len();
            for (i, c) in after.char_indices() {
                match c {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            end = i + 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            out.push(after[..end].to_string());
        }
        rest = after;
    }
    out
}

/// Readers a live file defines for ITSELF: any `fn` in this file whose body
/// reads the delivered artifacts. Returned as call spellings (`name(`).
fn local_readers(src: &str) -> BTreeSet<String> {
    let reads = [
        "read_parquet",
        "ParquetRecordBatchReaderBuilder",
        "files_with_extension",
        "read_dir",
        "read_to_string",
        "duckdb_",
        "declared_",
    ];
    let mut out = BTreeSet::new();
    let mut rest = src;
    while let Some(at) = rest.find("fn ") {
        let after = &rest[at + 3..];
        let name: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        // The body window. Sliced on a CHAR boundary — the tree has box-drawing
        // characters in comments, and a byte slice split one in half.
        let end = after
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|i| *i <= 4000)
            .last()
            .unwrap_or(0);
        let window = &after[..end];
        if !name.is_empty() && reads.iter().any(|r| window.contains(r)) {
            out.insert(format!("{name}("));
        }
        rest = after;
    }
    out
}

/// A must-fail test's NEGATIVE exit is its outcome; a bare `success()` is not.
const REFUSAL: [&str; 5] = [
    "!out.status.success()",
    "!res.status.success()",
    "!output.status.success()",
    "!status.success()",
    "run_expect_fail(",
];

fn test_chunks(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for raw in src.split("#[test]").skip(1) {
        let Some(open) = raw.find('{') else { continue };
        let (mut depth, mut i) = (0i32, open);
        let bytes: Vec<char> = raw.chars().collect();
        while i < bytes.len() {
            match bytes[i] {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
        let body: String = bytes[..i.min(bytes.len())].iter().collect();
        let name = body
            .lines()
            .find_map(|l| l.trim().strip_prefix("fn "))
            .unwrap_or("?")
            .split('(')
            .next()
            .unwrap_or("?")
            .to_string();
        out.push((name, body));
    }
    out
}

/// TIER 1 for batch: a test that RUNS a capture must read something back —
/// files, rows, the ledger, or a named refusal. "The process exited 0" is not
/// an outcome; a zero-row export exits 0 too.
#[test]
fn every_batch_capture_test_asserts_an_outcome() {
    let cap = capture_markers();
    let outcome = outcome_markers();
    let mut naked = Vec::new();
    for f in batch_files() {
        let src = fs::read_to_string(&f).unwrap();
        let file = f.file_name().unwrap().to_string_lossy().into_owned();
        // A file's OWN readers count. `id_set_and_fanout`, `read_uid_set` and
        // their kin are defined next to the tests that use them and read the
        // delivered parquet exactly as a shared helper would — the first survey
        // called 43% of batch tests oracle-less and hand checking put the true
        // number at ZERO, entirely because of these. Derived per file, so a new
        // local reader is recognised the day it is written.
        let local = local_readers(&src);
        for (name, body) in test_chunks(&src) {
            if !cap.iter().any(|m| body.contains(m.as_str())) {
                continue;
            }
            let reads_back = outcome.iter().any(|m| body.contains(m.as_str()))
                || local.iter().any(|m| body.contains(m.as_str()));
            let refuses = REFUSAL.iter().any(|m| body.contains(m))
                || body.contains(".code()")
                || body.contains("stderr");
            if !reads_back && !refuses {
                naked.push(format!("{file}::{name}"));
            }
        }
    }
    assert!(
        naked.is_empty(),
        "these batch tests run a capture and assert NOTHING about what it \
         produced — not a file, not a row, not a ledger entry, not a named \
         refusal. A zero-row export exits 0, so this shape grades the process, \
         not the product:\n  {}",
        naked.join("\n  ")
    );
}

/// TIER 2 for batch: a test whose NAME claims completeness or integrity must
/// carry an INDEPENDENT oracle — DuckDB, the census, the store leg, the
/// spec-derived row hash, or a source re-query. rivet's own counters cannot
/// grade a claim about rivet's own completeness.
#[test]
fn every_completeness_named_batch_test_carries_an_independent_oracle() {
    let claim = [
        "loses_nothing",
        "lose_no",
        "loses_no",
        "without_gap",
        "no_gap",
        "captures_only",
        "is_complete",
        "complete_destination",
        "nothing_dropped",
        "all_rows",
        "every_row",
        "exact",
    ];
    let independent = [
        "duckdb_",
        "row_census(",
        "assert_complete(",
        "row_hash_matches_independent_spec(",
        "duckdb_store_census(",
        "query_one(",
        "pg_connect()",
    ];
    let mut weak = Vec::new();
    for f in batch_files() {
        let src = fs::read_to_string(&f).unwrap();
        let file = f.file_name().unwrap().to_string_lossy().into_owned();
        for (name, body) in test_chunks(&src) {
            if !claim.iter().any(|w| name.contains(w)) {
                continue;
            }
            // The body, PLUS any helper in this file it calls: a test that
            // delegates its whole shape to `run_soak_case(fault)` carries its
            // oracle there, and reading only the test body reports a
            // DuckDB-graded soak as ungraded. Tier 1 needed the same fix for
            // the same reason — a reader one call away is still a reader.
            let via_helper = helper_bodies(&src, &body)
                .iter()
                .any(|h| independent.iter().any(|m| h.contains(m)));
            if independent.iter().any(|m| body.contains(m)) || via_helper {
                continue;
            }
            weak.push(format!("{file}::{name}"));
        }
    }
    // ZERO, and it got there by UPGRADES, not by widening the words.
    //
    // The gate landed at 18 and each one was closed by giving the test an
    // independent reader — DuckDB over the declared parts, the store census
    // over a bucket, DuckDB's own CSV parser — every upgrade run live. Two of
    // the 18 were the gate's own blindness rather than weak tests (a test that
    // delegates to `run_soak_case` carries its oracle in the helper), and the
    // fix was to teach the gate to follow one call, not to excuse the test.
    //
    // At zero the contract changes shape: a new batch test that claims
    // completeness in its NAME arrives with an independent oracle or does not
    // arrive. Raising this number is a decision someone argues for in the
    // commit that raises it.
    const WEAK_RATCHET: usize = 0;
    assert_eq!(
        weak.len(),
        WEAK_RATCHET,
        "batch completeness-claims graded WITHOUT an independent oracle moved \
         from {WEAK_RATCHET} to {}. Up: a new test claims completeness in its \
         name and grades it with rivet's own records or its own codec — give it \
         row_census, duckdb_*, the store census, or the spec-derived row hash, \
         or rename the claim out of the name. Down: you upgraded one — lower \
         the ratchet in the same commit so the win is banked.\n  {}",
        weak.len(),
        weak.join("\n  ")
    );
}
