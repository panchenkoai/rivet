//! Shrink-only ceilings for the rig's default oracle.
//!
//! Every successful `run|load|compact --config` and `apply <config.yaml>` started
//! through the `Rig` or a shared runner helper is graded by
//! `dev/release_oracle/rig_oracle.py` (tests/common/verify.rs, facts from the
//! config file), and so is a `Rig::spawn_args_env` child its caller reaps with
//! exit 0; a hand-built spawn is not. Three counts may only go down:
//!
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
const NO_ORACLE_CEILING: usize = 22; // ratchet-pin: no-oracle-opt-outs

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
        "pg_cdc_pk_changing_update_captures_and_does_not_brick",
        "delivered-only rows",
        "known defect: a PK-changing UPDATE carries no delete",
    ),
    (
        "a_pg_failover_to_the_standby_without_a_checkpoint_loses_the_rows_written_during_the_switch",
        "undelivered rows",
        "known defect: a PostgreSQL CDC failover without `cdc.checkpoint` creates a new slot",
    ),
]; // ratchet-pin: end

/// `(enclosing fn, class, reason)` of every `.oracle_known_defect("<class>", "<reason>")` call in `text`.
fn known_defect_sites(text: &str) -> Vec<(String, String, String)> {
    text.match_indices(".oracle_known_defect(")
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
const RAW_RIVET_CEILING: usize = 17; // ratchet-pin: raw-rivet-invocations

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
