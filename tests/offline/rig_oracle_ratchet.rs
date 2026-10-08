//! Shrink-only ceilings for the rig's default oracle.
//!
//! Every successful `run|load|compact --config` and `apply <config.yaml>` started
//! through the `Rig` or a shared runner helper is graded by
//! `dev/release_oracle/rig_oracle.py` (tests/common/verify.rs, facts from the
//! config file), and so is a `Rig::spawn_args_env` child its caller reaps with
//! exit 0; a hand-built spawn is not. An invocation that does not exit 0 is graded
//! against its pre-run snapshot (tests/common/refusal.rs). These counts may only go down:
//!
//! * typed declarations of what a failed run may leave (`.a_failed_run_may_leave(`,
//!   `FAILED_RUN_LEAVES_ENV`) and the product defects every failed run may show
//!   (`KNOWN_PRODUCT_DEFECTS`) — each one is a leftover the refusal grade lets through;
//! * oracle opt-outs (`.no_oracle("<reason>")`, `run_rivet_ok_no_oracle`,
//!   `RIVET_TEST_NO_ORACLE`) — each one is a live run no independent reader checks;
//! * call sites of the Rust-side DuckDB helpers (`tests/common/duckdb.rs`, the
//!   `duckdb_*` readers in `tests/common/parquet.rs`, the rig's container
//!   census) — the one DuckDB session belongs to the Python oracle, and these
//!   are the calls still to migrate onto it;
//! * hand-built `Command::new` spawns of the rivet binary in tests/live — runs
//!   no oracle grades.
//!
//! A count below its ceiling fails too, so every migration lowers the ceiling
//! in the same diff and cannot be spent later as silent slack.

use std::path::{Path, PathBuf};

/// Oracle opt-outs in tests/live: `.no_oracle(`, `run_rivet_ok_no_oracle(`, the `RIVET_TEST_NO_ORACLE` env by literal or by its `NO_ORACLE_ENV` constant.
// 17 -> 21 (2026-10-02): live_cdc_source_connections counts source connections, which the oracle's own read would add to.
// 21 -> 22 (2026-10-03): the PG truncate refusal's resumed run keeps the pre-truncate rows the refusal says only a re-snapshot removes.
// 22 -> 23 (2026-10-03): pg_cdc_a_declared_key_absent_from_the_old_key_does_not_split merges by a declared `load.pk: [code]`; the oracle dedups by the source primary key `id`.
// Raised 23 -> 24 (2026-10-08): the MongoDB oplog-gone cells run on a replica set container they start themselves, which the reader inside rivet-duckdb cannot reach.
const NO_ORACLE_CEILING: usize = 24; // ratchet-pin: no-oracle-opt-outs

/// Typed declarations in tests/live of what a run that does not exit 0 may leave: `.a_failed_run_may_leave(` and a raw run's `FAILED_RUN_LEAVES_ENV`.
// 0 -> 19 (2026-10-07): the refusal grade's first pass; every site is a gate that fails a run after its parts are written (quality, schema drift, a manifest that did not land).
// 19 -> 34 (2026-10-07): the full-stand sweep: 9 CDC streams that commit what they read before the refusal, 3 runs cut mid-way that keep the parts they wrote, 2 ClickHouse loads whose answer is lost after the write, 1 --reconcile verdict exit.
// 34 -> 35 (2026-10-07): the refused-run-keeps-refusing cells (one shared rig): parts, their file_log rows, the observed schema and the kept anchor.
// 35 -> 36 (2026-10-07): a parallel keyset resume that refuses over a deleted page while another worker finishes its range (seen on CI only).
// 36 -> 39 (2026-10-08): `run --validate` over a failed verification now exits 3 and a failed cursor write exits 1, both after the export completed: the two operator-contract generators and roast_metric_validated_matches_final_summary_verdict declare the delivered run.
// 39 -> 40 (2026-10-08): the SQL Server ADD COLUMN remedy cell: the log-gap refusal after the re-enable has stored the widened schema.
// 40 -> 41 (2026-10-08): the sabotage cells that take a resource away from a live run (one shared declaration, `stopped_mid_run`): the parts, file_log rows and checkpoint rows written before the stop stay for the next run.
const FAILED_RUN_LEFTOVER_CEILING: usize = 41; // ratchet-pin: failed-run-leftover-declarations

/// Entries of `KNOWN_PRODUCT_DEFECTS` (tests/common/refusal.rs): product defects every failed run may show.
// 2 -> 1 (2026-10-08): a run that fails before its first write leaves the prefix alone, so a failed manifest before a write is a failure, no longer an excuse.
const KNOWN_PRODUCT_DEFECT_CEILING: usize = 1; // ratchet-pin: failed-run-known-product-defects

/// Rust DuckDB-helper call sites across tests/ (see [`duckdb_helper_names`]).
// 626 -> 630 (2026-10-01): #378 merged first and added 4 calls in its Mongo null-_id tests.
// 630 -> 632 (2026-10-03): the PG/MySQL crash-mid-spilled-tail cells; on PG the rig oracle grades only the keys a stream's first run touched and passed 5 of 12 rows under the loss mutant.
const DUCKDB_HELPER_CEILING: usize = 632; // ratchet-pin: duckdb-helper-sites

/// Owned by a concurrent branch and migrated after it lands; not counted.
const EXCLUDED: &[&str] = &["tests/live/live_cdc_type_parity.rs"];

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read tests/").flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// (repo-relative path, text) of every counted test source.
fn sources() -> Vec<(String, String)> {
    let root = super::nonvacuity::repo_root();
    let mut files = Vec::new();
    rust_files(&root.join("tests"), &mut files);
    files
        .into_iter()
        .map(|p| {
            let rel = p.strip_prefix(&root).unwrap().display().to_string();
            let text = std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {rel}: {e}"));
            (rel, text)
        })
        .filter(|(rel, _)| !EXCLUDED.contains(&rel.as_str()) && !rel.starts_with("tests/offline/"))
        .collect()
}

/// The Rust DuckDB helpers, derived from the source: every `pub fn` in tests/common/duckdb.rs, every `fn duckdb_*` anywhere under tests/, and the rig's container-census methods.
fn duckdb_helper_names(srcs: &[(String, String)]) -> Vec<String> {
    let mut names: Vec<String> = [
        "census_oracle",
        "row_census",
        "duckdb_oracle",
        "assert_complete",
        "oracle_dir",
        "oracle_container_out",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for (rel, text) in srcs {
        for line in text.lines() {
            let l = line.trim_start();
            let decl = l
                .strip_prefix("pub fn ")
                .filter(|_| rel == "tests/common/duckdb.rs")
                .or_else(|| l.strip_prefix("pub fn duckdb_").map(|_| &l[7..]))
                .or_else(|| l.strip_prefix("fn duckdb_").map(|_| &l[3..]));
            if let Some(d) = decl {
                let name: String = d
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() && !names.contains(&name) {
                    names.push(name);
                }
            }
        }
    }
    names
}

/// Occurrences of `name(` that are not the `fn name(` declaration.
fn call_sites(text: &str, name: &str) -> usize {
    let needle = format!("{name}(");
    text.match_indices(&needle)
        .filter(|(i, _)| {
            let before = &text[..*i];
            let prev = before.chars().next_back();
            !prev.is_some_and(|c| c.is_alphanumeric() || c == '_') && !before.ends_with("fn ")
        })
        .count()
}

#[test]
fn no_oracle_opt_outs_never_grow() {
    let srcs = sources();
    super::nonvacuity::require_enumerated(srcs.len(), 100, "test sources scanned", "tests/ moved");
    let n: usize = srcs
        .iter()
        .filter(|(rel, _)| rel.starts_with("tests/live/"))
        .map(|(_, t)| {
            t.matches(".no_oracle(").count()
                + t.matches("run_rivet_ok_no_oracle(").count()
                + t.matches("\"RIVET_TEST_NO_ORACLE\"").count()
                // The same env through its exported constant (tests/common/verify.rs::NO_ORACLE_ENV).
                + t.matches("NO_ORACLE_ENV").count()
        })
        .sum();
    assert_eq!(
        n, NO_ORACLE_CEILING,
        "oracle opt-outs (`.no_oracle(`, `run_rivet_ok_no_oracle(`, `RIVET_TEST_NO_ORACLE`, `NO_ORACLE_ENV`): {n}, ceiling {NO_ORACLE_CEILING}. A new opt-out needs a reviewed \
         reason and a raised ceiling; a removed one lowers the ceiling in the same diff."
    );
}

#[test]
fn failed_run_leftover_declarations_never_grow() {
    let n: usize = sources()
        .iter()
        .filter(|(rel, _)| rel.starts_with("tests/live/"))
        .map(|(_, t)| {
            t.matches(".a_failed_run_may_leave(").count()
                + t.matches("FAILED_RUN_LEAVES_ENV").count()
        })
        .sum();
    assert_eq!(
        n, FAILED_RUN_LEFTOVER_CEILING,
        "declarations of what a failed run may leave (`.a_failed_run_may_leave(`, `FAILED_RUN_LEAVES_ENV`): {n}, ceiling \
         {FAILED_RUN_LEFTOVER_CEILING}. A new one needs a reviewed reason and a raised ceiling; a removed one lowers the ceiling in the same diff."
    );
}

/// Refusals the live cells accept with no `RIVET_*` code (`Refused::uncoded_known_defect(`): each is a code the registry owes.
// 5 -> 4 (2026-10-08): `--resume` over a complete prefix is refused as RIVET_DEST_ALREADY_COMPLETE.
// -1 (2026-10-08): the bounded-drain refusal on a PostgreSQL standby carries RIVET_SOURCE_CDC_PREREQUISITE.
const UNCODED_REFUSAL_CEILING: usize = 4; // ratchet-pin: uncoded-refusals

#[test]
fn uncoded_refusals_the_cells_accept_never_grow() {
    let n: usize = sources()
        .iter()
        .filter(|(rel, _)| rel.starts_with("tests/live/"))
        .map(|(_, t)| t.matches("Refused::uncoded_known_defect(").count())
        .sum();
    assert_eq!(
        n, UNCODED_REFUSAL_CEILING,
        "refusals accepted with no RIVET_* code (`Refused::uncoded_known_defect(`): {n}, ceiling {UNCODED_REFUSAL_CEILING}. \
         A refusal is expected by code; a coded one lowers the ceiling here."
    );
}

#[test]
fn known_product_defects_of_a_failed_run_never_grow() {
    let srcs = sources();
    let (_, text) = srcs
        .iter()
        .find(|(rel, _)| rel == "tests/common/refusal.rs")
        .expect("tests/common/refusal.rs moved");
    let list = text
        .split_once("const KNOWN_PRODUCT_DEFECTS")
        .and_then(|(_, rest)| rest.split_once("];"))
        .expect("KNOWN_PRODUCT_DEFECTS moved")
        .0;
    let n = list.matches("Leftover::").count();
    assert_eq!(
        n, KNOWN_PRODUCT_DEFECT_CEILING,
        "KNOWN_PRODUCT_DEFECTS entries: {n}, ceiling {KNOWN_PRODUCT_DEFECT_CEILING}. A product defect every failed run may show is \
         added in a reviewed diff; a fixed one lowers the ceiling here."
    );
}

#[test]
fn rust_duckdb_helper_call_sites_never_grow() {
    let srcs = sources();
    let names = duckdb_helper_names(&srcs);
    super::nonvacuity::require_enumerated(
        names.len(),
        20,
        "Rust DuckDB helper names",
        "the helpers moved",
    );
    let n: usize = srcs
        .iter()
        .map(|(_, t)| names.iter().map(|name| call_sites(t, name)).sum::<usize>())
        .sum();
    assert_eq!(
        n, DUCKDB_HELPER_CEILING,
        "Rust DuckDB helper call sites: {n}, ceiling {DUCKDB_HELPER_CEILING}. New reads go through \
         dev/release_oracle (the one DuckDB session); a migrated call lowers the ceiling here."
    );
}

/// `(test fn, failure class, reason prefix)` of every `.oracle_known_defect(` site: a product defect the oracle must keep catching, excused only for its class. Removing one is allowed; adding one is a reviewed diff here.
const KNOWN_DEFECTS: &[(&str, &str, &str)] = &[
    // ratchet-pin: oracle-known-defects strings
    (
        "roast_pg_cdc_refuses_a_bare_table_name_that_matches_two_relations",
        "delivered-only rows",
        "known defect: a bare-name capture delivers the WAL rows",
    ),
    (
        "a_pg_failover_to_the_standby_without_a_checkpoint_loses_the_rows_written_during_the_switch",
        "undelivered rows",
        "known defect: a PostgreSQL CDC failover without `cdc.checkpoint` creates a new slot",
    ),
    (
        "pg_slot_created_warning_remedy_recovers_the_row_written_while_the_slot_was_gone",
        "undelivered rows",
        "known defect: a PostgreSQL slot dropped under a stream with no checkpoint or baseline",
    ),
    (
        "mysql_missing_checkpoint_warning_remedy_recovers_the_row_written_while_it_was_gone",
        "undelivered rows",
        "known defect: a lost MySQL checkpoint on a stream with no baseline",
    ),
    (
        "mongo_missing_checkpoint_warning_remedy_recovers_the_document_written_while_it_was_gone",
        "undelivered rows",
        "known defect: a lost MongoDB checkpoint on a stream with no baseline",
    ),
    (
        "pg_duplicate_run",
        "a failed run left: resume-point",
        "known defect: a checkpointed run its quality gate fails is recorded in export_progression",
    ),
    (
        "chunked_checkpoint_refuses_to_clobber_a_cdc_manifest",
        "a failed run left: chunk-checkpoint",
        "known defect: the refusal to overwrite a CDC manifest comes after the chunk plan is stored",
    ),
    (
        "roast_resume_must_not_bypass_heterogeneous_id_guard",
        "a failed run left: resume-point",
        "known defect: a resumed keyset run the heterogeneous-_id guard refuses has already written its claim (resume_run_id, resume_owner) on the cursor row; the guard must come before the claim",
    ),
    (
        "mongo_heterogeneous_resume_remedy",
        "a failed run left: resume-point",
        "known defect: a resumed keyset run the heterogeneous-_id guard refuses has already written its claim (resume_run_id, resume_owner) on the cursor row; the guard must come before the claim",
    ),
]; // ratchet-pin: end

/// `(enclosing fn, class, reason)` of every `.oracle_known_defect(` / `.run_ok_capture_known_defect(` call in `text`.
fn known_defect_sites(text: &str) -> Vec<(String, String, String)> {
    text.match_indices(".oracle_known_defect(")
        .chain(text.match_indices(".run_ok_capture_known_defect("))
        .map(|(i, _)| {
            let before = &text[..i];
            let f = before
                .rfind("\nfn ")
                .map(|j| {
                    before[j + 4..]
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .collect::<String>()
                })
                .unwrap_or_default();
            let mut strings = text[i..].split('"').skip(1).step_by(2);
            let class = strings.next().unwrap_or_default().to_string();
            let reason = strings.next().unwrap_or_default().to_string();
            (f, class, reason)
        })
        .collect()
}

#[test]
fn oracle_known_defects_are_a_named_set_that_only_shrinks() {
    let sites: Vec<(String, String, String)> = sources()
        .iter()
        .filter(|(rel, _)| rel != "tests/common/rig/verify.rs")
        .flat_map(|(_, t)| known_defect_sites(t))
        .collect();
    super::nonvacuity::require_enumerated(
        sites.len(),
        1,
        "oracle_known_defect sites",
        "the marker moved",
    );
    let unlisted: Vec<&(String, String, String)> = sites
        .iter()
        .filter(|(f, c, r)| {
            !KNOWN_DEFECTS
                .iter()
                .any(|(kf, kc, kp)| kf == f && kc == c && r.starts_with(kp))
        })
        .collect();
    assert!(
        unlisted.is_empty(),
        "new `.oracle_known_defect` site(s) {unlisted:?}: a known product defect is added to \
         KNOWN_DEFECTS in a reviewed diff, never silently"
    );
}

/// Hand-built spawns of the rivet binary in tests/live: runs the default oracle never sees (the `Rig` and the `run_rivet*` helpers are graded).
const RAW_RIVET_CEILING: usize = 16; // ratchet-pin: raw-rivet-invocations

/// `Command::new` of the rivet binary in `text`, in each spelling the suite uses.
fn raw_rivet_sites(text: &str) -> usize {
    [
        "Command::new(RIVET_BIN)",
        "Command::new(rivet_bin())",
        "Command::new(env!(\"CARGO_BIN_EXE_rivet\"))",
    ]
    .iter()
    .map(|n| text.matches(n).count())
    .sum()
}

#[test]
fn raw_rivet_invocations_in_live_tests_never_grow() {
    let live: Vec<(String, String)> = sources()
        .into_iter()
        .filter(|(rel, _)| rel.starts_with("tests/live/"))
        .collect();
    super::nonvacuity::require_enumerated(
        live.len(),
        30,
        "live test sources scanned",
        "tests/live moved",
    );
    let n: usize = live.iter().map(|(_, t)| raw_rivet_sites(t)).sum();
    assert_eq!(
        n, RAW_RIVET_CEILING,
        "raw rivet invocations in tests/live: {n}, ceiling {RAW_RIVET_CEILING}. These runs bypass \
         the default oracle; drive rivet through `Rig` or a `run_rivet*` helper, and a \
         migrated call lowers the ceiling here."
    );
}
