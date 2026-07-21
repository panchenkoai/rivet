//! Crash-point recovery matrix.
//!
//! QA backlog Task 1.1.  For each of the four observable write-path
//! boundaries, inject a panic via the `RIVET_TEST_PANIC_AT` hook (see
//! `src/test_hook.rs`), observe the post-crash state, then re-run without
//! the injection and assert recovery produces the expected final state.
//!
//! Boundaries exercised:
//!
//! | Point                    | Triggered at                                 | Expected post-crash state         |
//! |--------------------------|----------------------------------------------|-----------------------------------|
//! | `after_source_read`      | source stream drained, writer not finalised  | no file, no manifest, no cursor   |
//! | `after_file_write`       | `dest.write` Ok, no manifest yet             | file on disk, no manifest entry   |
//! | `after_manifest_update`  | manifest row written, cursor not advanced    | file + manifest, no cursor        |
//! | `after_cursor_commit`    | cursor advanced, no final metric             | file + manifest + cursor, no metric |
//!
//! After each crash we run rivet again without the env var and assert the
//! final state matches the no-crash baseline for that export (full row
//! count, single manifest entry per unique file name, cursor at expected
//! value).

use crate::common::*;

/// Distinct `id` values and physical row count across every Parquet part at the
/// destination — re-reads the files, NOT rivet's state DB. Lets a recovery test
/// assert the no-row-loss superset invariant the state-DB assertions cannot see.
fn parquet_ids_and_rows(dir: &std::path::Path) -> (std::collections::BTreeSet<i64>, i64) {
    (dir_parquet_id_set(dir), total_parquet_rows(dir) as i64)
}

/// RAII guard — drops a Postgres table on scope exit.
struct PgCleanup(String);
impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

/// Run rivet with the given panic-point injected, expecting a non-zero exit.
fn run_rivet_crash(
    cfg_path: &std::path::Path,
    export_name: &str,
    crash_at: &str,
) -> std::process::Output {
    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg_path.to_str().unwrap(),
            "--export",
            export_name,
        ])
        .env("RIVET_TEST_PANIC_AT", crash_at)
        .output()
        .expect("spawn rivet");
    assert!(
        !out.status.success(),
        "run with RIVET_TEST_PANIC_AT='{crash_at}' must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Open the state DB that rivet wrote next to the given config file.
fn open_state_db(cfg: &std::path::Path) -> rusqlite::Connection {
    let db = cfg.parent().unwrap().join(".rivet_state.db");
    rusqlite::Connection::open(db).expect("open state db")
}

fn manifest_count(cfg: &std::path::Path, export: &str) -> i64 {
    open_state_db(cfg)
        .query_row(
            "SELECT COUNT(*) FROM file_log WHERE export_name = ?1",
            [export],
            |r| r.get(0),
        )
        .unwrap_or(0)
}

fn cursor_value(cfg: &std::path::Path, export: &str) -> Option<String> {
    open_state_db(cfg)
        .query_row(
            "SELECT last_cursor_value FROM export_state WHERE export_name = ?1",
            [export],
            |r| r.get::<_, Option<String>>(0),
        )
        .unwrap_or(None)
}

fn metric_count(cfg: &std::path::Path, export: &str) -> i64 {
    open_state_db(cfg)
        .query_row(
            "SELECT COUNT(*) FROM export_metrics WHERE export_name = ?1",
            [export],
            |r| r.get(0),
        )
        .unwrap_or(0)
}

/// Seed a table with an ordered `updated_at` cursor column.
fn seed_cursor_table(rows: i64) -> (String, PgCleanup) {
    let name = unique_name("qa11");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            updated_at TIMESTAMPTZ NOT NULL
        );
        INSERT INTO {name} (id, updated_at)
        SELECT g, now() - (interval '1 minute') * ({rows} - g)
        FROM generate_series(1, {rows}) g;"
    ))
    .unwrap();
    (name.clone(), PgCleanup(name))
}

fn write_cfg(
    out_dir: &std::path::Path,
    table_name: &str,
    export_name: &str,
    cfg_dir: &tempfile::TempDir,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, updated_at FROM {table_name}"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        dir = out_dir.display()
    );
    write_config(cfg_dir, &yaml)
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_source_read_leaves_state_completely_clean() {
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(10);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_src");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_source_read");

    // Contract: no file, no manifest entry, no cursor advance, no metric.
    assert!(
        files_with_extension(out.path(), "parquet").is_empty(),
        "after_source_read crash must not produce a file"
    );
    assert_eq!(manifest_count(&cfg, &export), 0);
    assert_eq!(cursor_value(&cfg, &export), None);
    // Metric may or may not be written depending on where in the outer loop
    // the panic unwinds — either way, the resume logic does not depend on it.

    // Recovery: re-run without the crash.  Full row count must surface.
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(rec.status.success(), "recovery run must succeed");
    assert_eq!(
        files_with_extension(out.path(), "parquet").len(),
        1,
        "recovery run must produce the single expected file"
    );
    assert!(cursor_value(&cfg, &export).is_some());
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_file_write_leaves_file_but_no_manifest_or_cursor() {
    // ADR-0001 I2→I3 crash window.  The operator-visible state must be:
    // file on disk, manifest empty, cursor absent.  Recovery: next run sees
    // no cursor and re-exports — at-least-once delivery for that file.
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(8);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_file");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_file_write");

    let files_after_crash = files_with_extension(out.path(), "parquet");
    assert_eq!(
        files_after_crash.len(),
        1,
        "after_file_write must have left exactly one file on disk; got: {files_after_crash:?}"
    );
    assert_eq!(
        manifest_count(&cfg, &export),
        0,
        "after_file_write must leave manifest empty"
    );
    assert_eq!(cursor_value(&cfg, &export), None);

    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(rec.status.success());

    // Post-recovery: manifest has one entry for the recovery run, cursor is
    // now populated.  The orphaned pre-crash file is still on disk — this
    // is the documented at-least-once-delivery corollary.
    assert_eq!(manifest_count(&cfg, &export), 1);
    assert!(cursor_value(&cfg, &export).is_some());
    let total = files_with_extension(out.path(), "parquet").len();
    assert!(
        total >= 2,
        "orphaned pre-crash file + recovery file: expected >=2, got {total}"
    );

    // Re-read the DESTINATION (not the state DB): the recovery re-export must be
    // a COMPLETE superset of the source — every seeded id present at least once
    // (no row LOST across the orphan+recovery split) — with the only surplus
    // being the documented at-least-once orphan (physical >= source). The
    // existing assertions check file COUNT + state DB + exit code, never the
    // rows actually on disk; a regression that dropped rows on the recovery
    // re-export would pass them all.
    let (ids, physical) = parquet_ids_and_rows(out.path());
    let expected: std::collections::BTreeSet<i64> = (1..=8).collect();
    assert_eq!(
        ids, expected,
        "recovery must leave every source id (1..=8) at the destination — a missing id is row LOSS"
    );
    assert!(
        physical >= 8,
        "at-least-once: physical destination rows ({physical}) must be >= source (8)"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_manifest_update_leaves_file_and_manifest_but_no_cursor() {
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(7);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_mani");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_manifest_update");

    assert_eq!(files_with_extension(out.path(), "parquet").len(), 1);
    assert_eq!(
        manifest_count(&cfg, &export),
        1,
        "after_manifest_update must leave the manifest row written"
    );
    assert_eq!(
        cursor_value(&cfg, &export),
        None,
        "after_manifest_update must leave the cursor unset"
    );

    // Recovery: re-run.  No cursor → full re-export; manifest grows by 1.
    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(rec.status.success());
    assert_eq!(manifest_count(&cfg, &export), 2);
    assert!(cursor_value(&cfg, &export).is_some());

    // Destination re-read: the recovery re-export is a complete superset of the
    // source (every seeded id 1..=7 present; physical >= source for at-least-once).
    let (ids, physical) = parquet_ids_and_rows(out.path());
    let expected: std::collections::BTreeSet<i64> = (1..=7).collect();
    assert_eq!(
        ids, expected,
        "recovery must leave every source id (1..=7) at the destination — a missing id is row LOSS"
    );
    assert!(
        physical >= 7,
        "at-least-once: physical destination rows ({physical}) must be >= source (7)"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_cursor_commit_is_recoverable_with_full_state() {
    // Crash between cursor commit and final run metric.  The write cycle
    // is structurally complete; only observability (the metric row) is
    // missing.  The next run must see the cursor and export zero new rows.
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(6);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_curs");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    run_rivet_crash(&cfg, &export, "after_cursor_commit");

    assert_eq!(files_with_extension(out.path(), "parquet").len(), 1);
    assert_eq!(manifest_count(&cfg, &export), 1);
    let cursor_after_crash = cursor_value(&cfg, &export);
    assert!(
        cursor_after_crash.is_some(),
        "after_cursor_commit must leave the cursor advanced"
    );
    // Metric may be absent (panic before record_metric) OR present (panic
    // after).  The test accepts either outcome — the key invariant is that
    // the write cycle is durable.
    let _metric_after_crash = metric_count(&cfg, &export);

    // Recovery: re-run must see the cursor and export zero additional rows.
    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(rec.status.success());

    assert_eq!(
        files_with_extension(out.path(), "parquet").len(),
        1,
        "second run on unchanged source must not produce a new file (cursor saw no new rows)"
    );
    assert_eq!(
        cursor_value(&cfg, &export),
        cursor_after_crash,
        "cursor value must stay at the post-crash position when no new data arrived"
    );
}

// Round-2 audit #12: the incremental cursor advanced BEFORE the destination
// manifest was written, so a crash at after_cursor_commit advanced the cursor past
// parts the manifest never recorded — the next cycle resumes past them, and the
// manifest-authoritative `rivet load` never sees them (silent export→load loss).
// The parquet-glob path masks it (the orphan file is physically durable); a
// MANIFEST-DRIVEN read (only parts a Success manifest declares, as the loader does)
// exposes it, so this goes RED without the cursor-after-manifest fix.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn crash_after_cursor_commit_is_recoverable_via_a_manifest_driven_read() {
    require_alive(LiveService::Postgres);
    let (table, _guard) = seed_cursor_table(6);
    let cfg_dir = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let export = unique_name("qa11_m12");
    let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

    // Cycle 1: crash right after the cursor advances. With the fix the manifest is
    // already durable (finalize_manifest ran first); without it, the 6 rows are
    // orphaned from any manifest while the cursor points past them.
    run_rivet_crash(&cfg, &export, "after_cursor_commit");
    assert!(
        cursor_value(&cfg, &export).is_some(),
        "after_cursor_commit must leave the cursor advanced"
    );

    // Add 3 rows PAST the advanced cursor, then a clean next cycle captures them.
    pg_connect()
        .batch_execute(&format!(
            "INSERT INTO {table} (id, updated_at)
             SELECT g, now() + (interval '1 hour') * g FROM generate_series(7, 9) g;"
        ))
        .unwrap();
    let rec = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .output()
        .expect("spawn rivet");
    assert!(
        rec.status.success(),
        "recovery run must succeed: {}",
        String::from_utf8_lossy(&rec.stderr)
    );

    // Manifest-driven read (union of Success manifests' parts) must see ALL 9 rows —
    // the 6 from the crashed cycle plus the 3 new ones.
    let ids = manifest_driven_bigint_ids(out.path());
    assert_eq!(
        ids.len(),
        9,
        "every row must be reachable via a Success manifest (round-2 #12), got {} ids: {:?}",
        ids.len(),
        {
            let mut v: Vec<_> = ids.iter().collect();
            v.sort();
            v
        }
    );
}

/// Distinct BIGINT `id`s reachable the way `rivet load` reads: ONLY parts declared
/// by a `Success` manifest (run-unique `manifest-<run_id>.json` copies included),
/// never an orphan parquet that no manifest references.
fn manifest_driven_bigint_ids(out: &std::path::Path) -> std::collections::HashSet<i64> {
    let mut declared: std::collections::HashSet<String> = std::collections::HashSet::new();
    for e in std::fs::read_dir(out).unwrap() {
        let p = e.unwrap().path();
        let name = p
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        if !(name.starts_with("manifest") && name.ends_with(".json")) {
            continue;
        }
        let m: serde_json::Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        if m.get("status").and_then(|s| s.as_str()) != Some("success") {
            continue;
        }
        for part in m
            .get("parts")
            .and_then(|x| x.as_array())
            .into_iter()
            .flatten()
        {
            if let Some(path) = part.get("path").and_then(|x| x.as_str()) {
                declared.insert(path.to_string());
            }
        }
    }
    let mut ids = std::collections::HashSet::new();
    for part_name in &declared {
        let p = out.join(part_name);
        if !p.exists() {
            continue;
        }
        let f = std::fs::File::open(&p).unwrap();
        let r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
        for b in r {
            let b = b.unwrap();
            let col = b
                .column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .clone();
            for r in 0..b.num_rows() {
                ids.insert(col.value(r));
            }
        }
    }
    ids
}

// ─── OPT-6 slice 3: the same write-cycle boundaries through the SUBPROCESS engine ─
//
// `after_source_read` is covered by `parallel_processes_hard_crash_writes_no_partial_file`
// (no partial at the destination). The remaining boundaries are boundary-agnostic
// for the parent — it only sees "child exited non-zero" — so one parametrised test
// proves the parent aggregates a child crash AND a clean subprocess rerun recovers
// the full source id set with no loss, symmetric with the in-process matrix above.
// (The child runs the in-process write path, already covered per-boundary above;
// this adds the parent-aggregation + recovery dimension for `parallel_export_processes`.)
#[test]
#[ignore = "live: requires docker compose postgres"]
fn parallel_processes_recovers_from_child_crash_at_each_boundary() {
    require_alive(LiveService::Postgres);
    for crash_at in [
        "after_file_write",
        "after_manifest_update",
        "after_cursor_commit",
    ] {
        let (table, _guard) = seed_cursor_table(40);
        let cfg_dir = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();
        let export = unique_name("qa11_sub");
        let cfg = write_cfg(out.path(), &table, &export, &cfg_dir);

        // Crash the child mid-write-cycle through the subprocess engine — the env
        // is inherited by the spawned child, which runs the in-process write path.
        let crashed = std::process::Command::new(RIVET_BIN)
            .args([
                "run",
                "--config",
                cfg.to_str().unwrap(),
                "--parallel-export-processes",
            ])
            .env("RIVET_TEST_PANIC_AT", crash_at)
            .output()
            .expect("spawn rivet --parallel-export-processes");
        assert!(
            !crashed.status.success(),
            "{crash_at}: the parent must report the child crash (non-zero); stderr:\n{}",
            String::from_utf8_lossy(&crashed.stderr)
        );

        // Recover via a clean subprocess rerun.
        let rec = std::process::Command::new(RIVET_BIN)
            .args([
                "run",
                "--config",
                cfg.to_str().unwrap(),
                "--parallel-export-processes",
            ])
            .output()
            .expect("spawn rivet recovery");
        assert!(
            rec.status.success(),
            "{crash_at}: subprocess rerun must recover; stderr:\n{}",
            String::from_utf8_lossy(&rec.stderr)
        );

        // No row loss: the destination holds the full source id set (duplicates
        // from an at-least-once rerun are allowed; a *lost* id is not).
        let (ids, _rows) = parquet_ids_and_rows(out.path());
        assert_eq!(
            ids.len(),
            40,
            "{crash_at}: recovery must export all 40 source ids; got {}",
            ids.len()
        );
    }
}
