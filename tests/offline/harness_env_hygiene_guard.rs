//! ENVIRONMENT HYGIENE: the harness neither mutates its own process environment
//! nor lets the gate drop a variable the harness reads.
//!
//! Hunt H4 (2026-10-02) measured both halves. `pg_store()` in `live_pg_state.rs`
//! wrapped `StateStore::open` in a process-wide `set_var("RIVET_STATE_URL")`;
//! under libtest's threads a rivet spawned by ANOTHER test in that window
//! inherited the Postgres URL and failed on an empty SQLite file. And the gate
//! merged the whole shell into every cell, so a leftover `RIVET_TEST_PANIC_AT`
//! read as a product panic. The fix strips every inherited `RIVET_*` and keeps an
//! allow-list of the harness's own; this file keeps that list honest.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Every `.rs` file under `dir`, at any depth.
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = vec![];
    for e in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(rust_files(&p));
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out
}

/// Helpers allowed to set a process-wide variable. Empty: a test passes env to the CHILD.
const SET_VAR_ALLOWED: &[&str] = &[];

#[test]
fn no_live_or_common_test_sets_process_env() {
    let mut hits = vec![];
    for dir in ["tests/live", "tests/common"] {
        for f in rust_files(&root().join(dir)) {
            let rel = f.strip_prefix(root()).unwrap().display().to_string();
            if SET_VAR_ALLOWED.contains(&rel.as_str()) {
                continue;
            }
            for (i, line) in std::fs::read_to_string(&f).unwrap().lines().enumerate() {
                if line.contains("set_var(") && !line.trim_start().starts_with("//") {
                    hits.push(format!("{rel}:{}: {}", i + 1, line.trim()));
                }
            }
        }
    }
    assert!(
        hits.is_empty(),
        "a process-wide set_var leaks into every rivet another thread spawns; pass the value to \
         the child (`envs`) or open the store at a StateRef instead:\n{}",
        hits.join("\n")
    );
}

/// No lib test moves `StateStore::open` of every concurrent test onto Postgres by setting `RIVET_STATE_URL`.
#[test]
fn no_lib_test_sets_the_state_url_for_the_process() {
    let hits: Vec<String> = rust_files(&root().join("src"))
        .iter()
        .flat_map(|f| {
            let rel = f.strip_prefix(root()).unwrap().display().to_string();
            std::fs::read_to_string(f)
                .unwrap()
                .lines()
                .enumerate()
                .filter(|(_, l)| l.contains("set_var(\"RIVET_STATE_URL\""))
                .map(|(i, l)| format!("{rel}:{}: {}", i + 1, l.trim()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        hits.is_empty(),
        "open the store at StateRef::Postgres(url) instead; a process-wide RIVET_STATE_URL turned \
         a_garbage_state_db_reports_corruption_not_a_phantom_process red whenever RIVET_TEST_STATE_URL was set:\n{}",
        hits.join("\n")
    );
}

/// The `RIVET_*` names in `HARNESS_ENV` in core.py.
fn gate_allow_list(core: &str) -> BTreeSet<String> {
    let start = core
        .find("HARNESS_ENV: frozenset[str] = frozenset((")
        .expect("HARNESS_ENV in core.py");
    let body = &core[start..];
    let end = body.find("))").expect("HARNESS_ENV closes");
    regex::Regex::new(r#""(RIVET_[A-Z0-9_]+)""#)
        .unwrap()
        .captures_iter(&body[..end])
        .map(|c| c[1].to_string())
        .collect()
}

/// Every `RIVET_*` the gate, the Rust harness and the Makefile's GATE_ENV read from the shell.
fn harness_reads() -> BTreeSet<(String, String)> {
    let py =
        regex::Regex::new(r#"(?:environ(?:\.get)?\(?\[?|getenv\()"(RIVET_[A-Z0-9_]+)""#).unwrap();
    let rs = regex::Regex::new(r#"env::var(?:_os)?\("(RIVET_[A-Z0-9_]+)""#).unwrap();
    let mk = regex::Regex::new(r"(?m)^\s*(RIVET_[A-Z0-9_]+)='").unwrap();
    let mut reads = BTreeSet::new();
    let mut scan = |re: &regex::Regex, f: &Path| {
        let rel = f.strip_prefix(root()).unwrap().display().to_string();
        for c in re.captures_iter(&std::fs::read_to_string(f).unwrap()) {
            reads.insert((c[1].to_string(), rel.clone()));
        }
    };
    for e in std::fs::read_dir(root().join("dev/release_oracle")).unwrap() {
        let p = e.unwrap().path();
        if p.extension().is_some_and(|x| x == "py") {
            scan(&py, &p);
        }
    }
    for dir in ["tests/live", "tests/common"] {
        for f in rust_files(&root().join(dir)) {
            scan(&rs, &f);
        }
    }
    scan(&mk, &root().join("Makefile"));
    reads
}

#[test]
fn the_gate_keeps_every_variable_the_harness_reads() {
    let core = std::fs::read_to_string(root().join("dev/release_oracle/core.py")).unwrap();
    let allowed = gate_allow_list(&core);
    let family = regex::Regex::new(r"^RIVET_(CDC|ORACLE)_[A-Z0-9]+_URL$").unwrap();
    assert!(
        core.contains(r#"HARNESS_ENV_FAMILY = re.compile(r"^RIVET_(CDC|ORACLE)_[A-Z0-9]+_URL$")"#)
    );
    let reads = harness_reads();
    assert!(
        reads.len() > 40,
        "the scan found only {} reads; the patterns drifted",
        reads.len()
    );
    let missing: Vec<String> = reads
        .iter()
        // The gate sets RIVET_STATE_URL itself before any cell reads it; a shell copy is the leak.
        .filter(|(name, _)| name != "RIVET_STATE_URL")
        .filter(|(name, _)| !allowed.contains(name) && !family.is_match(name))
        .map(|(name, file)| format!("{name} (read in {file})"))
        .collect();
    assert!(
        missing.is_empty(),
        "scrub_inherited_rivet_env() would drop a variable the harness reads; add it to \
         HARNESS_ENV in dev/release_oracle/core.py:\n{}",
        missing.join("\n")
    );
}

/// The `fn maybe_*` hook points `src/test_hook.rs` defines.
fn hook_fns() -> Vec<String> {
    let src = std::fs::read_to_string(root().join("src/test_hook.rs")).unwrap();
    regex::Regex::new(r"(?m)^\s*pub(?:\(crate\))?\s+fn\s+(maybe_\w+)\s*\(")
        .unwrap()
        .captures_iter(&src)
        .map(|c| c[1].to_string())
        .collect()
}

/// `"file kind argument"` → `file:line` for every environment read and test-hook call under `src/`.
fn product_env_surface() -> BTreeMap<String, String> {
    let hooks = hook_fns();
    assert!(hooks.len() >= 5, "found only {hooks:?} in src/test_hook.rs");
    let env = regex::Regex::new(
        r#"\benv::(var|var_os|vars|vars_os)\s*\(([^)]*)\)|\buse\s+std::env::(\{[^}]*\}|var\w*)|\benv\s*=\s*"([^"]+)""#,
    )
    .unwrap();
    let hook = regex::Regex::new(&format!(r"\b({})\s*\(\s*([^,)]*)", hooks.join("|"))).unwrap();
    let mut out = BTreeMap::new();
    for f in rust_files(&root().join("src")) {
        let rel = f.strip_prefix(root()).unwrap().display().to_string();
        let text = std::fs::read_to_string(&f).unwrap();
        let line = |at: usize| text[..at].matches('\n').count() + 1;
        let squash = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
        for c in env.captures_iter(&text) {
            let what = match (c.get(1), c.get(3), c.get(4)) {
                (Some(_), ..) => format!("env::{} {}", &c[1], squash(&c[2])),
                (_, Some(u), _) => format!("use std::env::{}", squash(u.as_str())),
                (_, _, Some(a)) => format!("clap env {}", a.as_str()),
                _ => unreachable!(),
            };
            out.entry(format!("{rel} {what}"))
                .or_insert_with(|| format!("{rel}:{}", line(c.get(0).unwrap().start())));
        }
        if rel != "src/test_hook.rs" {
            for c in hook.captures_iter(&text) {
                out.entry(format!("{rel} {} {}", &c[1], squash(&c[2])))
                    .or_insert_with(|| format!("{rel}:{}", line(c.get(0).unwrap().start())));
            }
        }
    }
    out
}

/// Every environment read and test-hook call site under `src/`, keyed without line numbers so an edit does not move it.
const PRODUCT_ENV_SURFACE: &[&str] = &[
    // ratchet-pin: product-env-surface strings
    "src/bin/rivet-mcp.rs clap env DATABASE_URL",
    "src/bin/seed/main.rs env::var CONFIRM_ENV",
    "src/cli/args.rs clap env RIVET_RUN_ID",
    "src/cli/params.rs env::var &var",
    "src/config/resolve.rs env::var var_name",
    "src/config/source.rs env::var env",
    "src/destination/azure.rs env::var env_name",
    "src/destination/gcs_auth.rs env::var \"APPDATA\"",
    "src/destination/gcs_auth.rs env::var \"GOOGLE_APPLICATION_CREDENTIALS\"",
    "src/destination/gcs_auth.rs env::var \"HOME\"",
    "src/destination/gcs_auth.rs env::var \"XDG_CONFIG_HOME\"",
    "src/destination/local.rs maybe_block_at \"before_commit_rename\"",
    "src/destination/s3.rs env::var \"AWS_PROFILE\"",
    "src/destination/s3.rs env::var env_name",
    "src/load/bigquery/mod.rs maybe_panic_at \"compact_after_merge\"",
    "src/load/bigquery/mod.rs maybe_panic_at \"compact_before_merge\"",
    "src/load/bigquery/tests.rs env::var \"BIGQUERY_TEST_DATASET\"",
    "src/load/bigquery/tests.rs env::var \"BIGQUERY_TEST_LOCATION\"",
    "src/load/bigquery/tests.rs env::var \"BIGQUERY_TEST_PROJECT\"",
    "src/load/bigquery/tests.rs env::var \"RIVET_BQ_CDC_DATA_COLS\"",
    "src/load/bigquery/tests.rs env::var \"RIVET_BQ_CDC_EXPECTED_STATE\"",
    "src/load/bigquery/tests.rs env::var \"RIVET_BQ_CDC_PARQUET_URI\"",
    "src/load/bigquery/tests.rs env::var \"RIVET_BQ_CDC_PK\"",
    "src/load/bigquery/tests.rs env::var \"RIVET_BQ_TEST_DATASET\"",
    "src/load/bigquery/tests.rs env::var \"RIVET_BQ_TEST_PARQUET_URI\"",
    "src/load/bq_rest.rs env::var \"RIVET_BQ_ACCESS_TOKEN\"",
    "src/load/bq_rest.rs env::var \"RIVET_BQ_API_ENDPOINT\"",
    "src/load/bq_rest.rs env::var \"RIVET_BQ_LOCATION\"",
    "src/load/clickhouse.rs env::var &self.password_env",
    "src/load/clickhouse.rs maybe_panic_at \"clickhouse_full_after_swap_created\"",
    "src/load/clickhouse.rs maybe_panic_at \"clickhouse_full_before_swap_in\"",
    "src/load/clickhouse.rs maybe_panic_at point",
    "src/load/clickhouse.rs maybe_panic_at_chunk \"clickhouse_after_part\"",
    "src/load/mod.rs env::var \"RIVET_SNOWFLAKE_KEY\"",
    "src/load/mod.rs maybe_panic_at \"load_after_adopt\"",
    "src/load/mod.rs maybe_panic_at \"load_after_append\"",
    "src/load/snowflake.rs env::var \"RIVET_SNOWFLAKE_KEY\"",
    "src/load/snowflake.rs env::var \"SNOWFLAKE_TEST_CONNECTION\"",
    "src/load/snowflake.rs env::var k",
    "src/mcp.rs env::var \"PGBOUNCER_ADMIN_URL\"",
    "src/notify.rs env::var env",
    "src/pipeline/chunked/exec.rs maybe_error_at_index \"chunk_export\"",
    "src/pipeline/chunked/exec.rs maybe_transient_once \"chunk_write\"",
    "src/pipeline/chunked/parallel_checkpoint.rs maybe_error_at_index \"chunk_export\"",
    "src/pipeline/chunked/parallel_checkpoint.rs maybe_panic_at_chunk \"after_chunk_complete\"",
    "src/pipeline/chunked/parallel_checkpoint.rs maybe_panic_at_chunk \"after_chunk_file\"",
    "src/pipeline/chunked/sequential_checkpoint.rs maybe_error_at_index \"chunk_export\"",
    "src/pipeline/chunked/sequential_checkpoint.rs maybe_panic_at_chunk \"after_chunk_complete\"",
    "src/pipeline/chunked/sequential_checkpoint.rs maybe_panic_at_chunk \"after_chunk_file\"",
    "src/pipeline/chunked/sequential_checkpoint.rs maybe_transient_once \"after_resume_adopt\"",
    "src/pipeline/commit.rs maybe_error_at_index \"sink_part_write\"",
    "src/pipeline/commit.rs maybe_panic_at \"after_file_write\"",
    "src/pipeline/commit.rs maybe_panic_at \"after_manifest_update\"",
    "src/pipeline/ipc.rs env::var ENV_IPC_EVENTS",
    "src/pipeline/keyset.rs maybe_error_at_index \"keyset_parallel_worker\"",
    "src/pipeline/keyset.rs maybe_error_at_index \"keyset_parallel_worker_midrange\"",
    "src/pipeline/keyset.rs maybe_exit_at_index \"keyset_parallel_range_committed\"",
    "src/pipeline/keyset.rs maybe_panic_at \"keyset_after_data_complete\"",
    "src/pipeline/keyset.rs maybe_panic_at \"keyset_after_open_before_first_page\"",
    "src/pipeline/keyset.rs maybe_panic_at &format!(\"after_keyset_page:{pages}\"",
    "src/pipeline/mongo_parallel.rs maybe_error_at_index \"mongo_parallel_worker\"",
    "src/pipeline/run.rs env::var_os ENV_CONCURRENT_SIBLINGS",
    "src/pipeline/run.rs env::var_os ENV_PARENT_SELF_CHECK",
    "src/pipeline/run.rs env::var_os super::ENV_PARENT_SELF_CHECK",
    "src/pipeline/run_store.rs maybe_panic_at \"after_cursor_commit\"",
    "src/pipeline/single.rs maybe_block_at \"after_source_read\"",
    "src/pipeline/single.rs maybe_panic_at \"after_source_read\"",
    "src/pipeline/sink/pipelined.rs env::var \"RIVET_PIPELINE_WRITES\"",
    "src/preflight/cdc_health.rs env::var \"RIVET_TEST_SLOT_WAL_BAR\"",
    "src/source/cdc/mod.rs env::var \"RIVET_CDC_MAX_TX_BYTES\"",
    "src/source/cdc/mod.rs env::var \"RIVET_CDC_MAX_TX_ROWS\"",
    "src/source/cdc/mod.rs env::var \"RIVET_CDC_SPILL_DIR\"",
    "src/source/cdc/mod.rs env::var key",
    "src/source/cdc/mod.rs maybe_panic_at \"cdc_after_open\"",
    "src/source/cdc/mod.rs maybe_panic_at \"cdc_before_resolve\"",
    "src/source/cdc/sink.rs maybe_panic_at \"cdc_after_ack\"",
    "src/source/cdc/sink.rs maybe_panic_at \"cdc_after_checkpoint_before_ack\"",
    "src/source/cdc/sink.rs maybe_panic_at \"cdc_after_flush_before_ack\"",
    "src/source/cdc/sink.rs maybe_panic_at \"cdc_before_manifest\"",
    "src/source/cdc/spill.rs env::var \"RIVET_MEASURE\"",
    "src/source/cdc/spill.rs env::var \"RIVET_REGENERATE_FIXTURES\"",
    "src/source/mysql/cdc.rs maybe_fail_at \"mysql_gtid_subset_query\"",
    "src/source/mysql/cdc.rs maybe_fail_at \"mysql_identity_query\"",
    "src/source/postgres/mod.rs maybe_pause_at \"pg_after_snapshot_open\"",
    "src/state/load_lease.rs env::var \"RIVET_STATE_LEASE_TTL_S\"",
    "src/state/migrations.rs env::var \"RIVET_TEST_STATE_URL\"",
    "src/state/mod.rs env::var \"RIVET_STATE_URL\"",
    "src/state/row.rs env::var \"RIVET_TEST_STATE_URL\"",
    "src/state/run_status_store.rs env::var \"RIVET_TEST_STATE_URL\"",
    "src/test_hook.rs env::var \"CARGO_TARGET_DIR\"",
    "src/test_hook.rs env::var \"RIVET_SKIP_LOG\"",
    "src/test_hook.rs env::var \"RIVET_TEST_BLOCK_AT\"",
    "src/test_hook.rs env::var \"RIVET_TEST_BLOCK_MS\"",
    "src/test_hook.rs env::var \"RIVET_TEST_ERROR_AT\"",
    "src/test_hook.rs env::var \"RIVET_TEST_PANIC_AT\"",
    "src/test_hook.rs env::var \"RIVET_TEST_PAUSE_AT\"",
    "src/test_hook.rs env::var \"RIVET_TEST_PAUSE_MARKER\"",
    "src/test_hook.rs env::var \"RIVET_TEST_TRANSIENT_ONCE\"",
    "src/tuning/adaptive.rs env::var \"RIVET_GOVERNOR_INTERVAL_MS\"",
]; // ratchet-pin: end

/// No new environment knob in the product: the env reads and hook points under `src/` are exactly the pinned set.
#[test]
fn the_product_adds_no_environment_read_and_no_test_hook_point() {
    let found = product_env_surface();
    assert!(
        found.len() > 60,
        "the scan found only {} sites; the patterns drifted",
        found.len()
    );
    let pinned: BTreeSet<&str> = PRODUCT_ENV_SURFACE.iter().copied().collect();
    let new: Vec<String> = found
        .iter()
        .filter(|(k, _)| !pinned.contains(k.as_str()))
        .map(|(k, at)| format!("{at}: {k}"))
        .collect();
    let gone: Vec<&&str> = pinned.iter().filter(|k| !found.contains_key(**k)).collect();
    let listing = found
        .keys()
        .map(|k| format!("    {k:?},"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        new.is_empty(),
        "a new environment read or test-hook point under src/ (src/test_hook.rs ships in release \
         binaries; make the test deterministic from outside rivet instead). If it is unavoidable, \
         add it to PRODUCT_ENV_SURFACE and declare it under 'Ratchets raised' in the PR body:\n{}",
        new.join("\n")
    );
    assert!(
        gone.is_empty(),
        "these pinned sites are gone; remove them from PRODUCT_ENV_SURFACE to bank it: {gone:?}\n\
         the current set:\n{listing}"
    );
}
