//! AUDIT-RED rerun-accumulation: re-running an export to the same stable local
//! prefix (no `--resume`) silently accumulates orphaned, timestamp-named part
//! files alongside the old ones.  The manifest (`manifest.json`) is overwritten
//! to describe only the latest run, so manifest/disk drift, and a glob reader
//! over the whole prefix over-counts (chunked re-run → 2× rows).
//!
//! Findings #5, #19, #30 (HIGH/MEDIUM footgun).
//!
//! The `_SUCCESS` overwrite-refusal gate (`finalize::check_success_gate_for_resume`)
//! is wired only into the `--resume` path; a plain `rivet run` into a prefix that
//! already carries `_SUCCESS` is neither refused nor warned about.
//!
//! These tests assert the *safe* expected behavior and so are expected to FAIL
//! until the footgun is fixed.  The fix may take EITHER shape — a loud refuse /
//! warn on re-run into a `_SUCCESS` prefix, OR a clean single set of parts that
//! matches the manifest — so the assertions accept either and only fail on the
//! silent-doubling that the live audit observed.

use crate::common::*;

use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Full-mode parquet→local config for `table` at `out_dir`.
fn full_config(table: &str, out_dir: &Path) -> String {
    format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {table}
    query: "SELECT id, name FROM {table}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

/// Chunked parquet→local config for `table` at `out_dir`.
fn chunked_config(table: &str, out_dir: &Path, chunk_size: i64) -> String {
    format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {table}
    query: "SELECT id, name FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: {chunk_size}
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

/// Total Parquet rows across every `*.parquet` file directly under `dir`.
/// Mirrors what a glob reader (`read_parquet('<dir>/*.parquet')`) would count.
fn total_parquet_rows(dir: &Path) -> i64 {
    let mut total = 0i64;
    for path in files_with_extension(dir, "parquet") {
        let bytes =
            std::fs::read(&path).unwrap_or_else(|e| panic!("read parquet {}: {e}", path.display()));
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap_or_else(|e| panic!("open parquet {}: {e}", path.display()));
        for batch in builder.build().unwrap() {
            total += batch.unwrap().num_rows() as i64;
        }
    }
    total
}

/// `manifest.json`'s declared `part_count` at the prefix root, if present.
fn manifest_part_count(dir: &Path) -> Option<i64> {
    let raw = std::fs::read_to_string(dir.join("manifest.json")).ok()?;
    let json: serde_json::Value = serde_json::from_str(&raw).ok()?;
    json["part_count"].as_i64()
}

/// True if stderr looks like the run warned/refused about an already-populated
/// (`_SUCCESS`) prefix — accepted as the "safe loud" fix shape.
fn warned_about_existing_prefix(stderr: &str) -> bool {
    let s = stderr.to_lowercase();
    // Deliberately narrow: only phrases an intentional re-run guard would emit.
    // Generic words ("existing", "re-run") would let incidental log text mask
    // the silent-doubling this RED test is meant to catch.
    s.contains("_success")
        || s.contains("already has")
        || s.contains("prior completed run")
        || s.contains("would overwrite")
        || s.contains("orphan")
}

// ─── (a) full re-run must not orphan part files ───────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn audit_full_rerun_does_not_orphan_parts() {
    // AUDIT-RED rerun-accumulation: a full re-run to the same prefix (no --resume) orphans the prior timestamp-named part while manifest.json is overwritten to part_count=1. Asserts CORRECT behavior; expected to FAIL until fixed.
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(120);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(&cfg_dir, &full_config(table.name(), out.path()));

    // First run: lands one timestamp-named part + manifest.json + _SUCCESS.
    let first = run_rivet_with_warn_log(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);
    assert!(
        first.status.success(),
        "first full run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&first.stderr)
    );

    // No sleep: parts and run_ids are millisecond-stamped (`%3f`), so
    // back-to-back sub-second runs must not collide — sleeping here would
    // mask exactly that regression (matrix audit: sleep-masked class).

    // Second run to the SAME prefix, no --resume.
    let second = run_rivet_with_warn_log(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);
    let second_stderr = String::from_utf8_lossy(&second.stderr);

    // SAFE fix shape #1: the second run refuses (non-zero) to overwrite a
    // _SUCCESS prefix.
    if !second.status.success() {
        return;
    }
    // SAFE fix shape #2: the second run loudly warns about the populated prefix.
    if warned_about_existing_prefix(&second_stderr) {
        return;
    }

    // Otherwise the run claimed clean success silently — the manifest must then
    // actually describe everything on disk: disk part count == manifest part_count.
    let parquet = files_with_extension(out.path(), "parquet");
    let disk_parts = parquet.len() as i64;
    let declared = manifest_part_count(out.path())
        .expect("manifest.json with part_count must exist after a successful run");

    assert_eq!(
        disk_parts, declared,
        "silent re-run drift: {disk_parts} parquet file(s) on disk but manifest.json \
         declares part_count={declared} (orphaned prior-run part left behind, and the \
         second run neither refused nor warned). files: {parquet:?}\nstderr:\n{second_stderr}"
    );
}

// ─── (b) chunked re-run must not double the dataset ───────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn audit_chunked_rerun_does_not_double() {
    // AUDIT-RED rerun-accumulation: a chunked re-run to the same prefix (no --resume) accumulates a fresh nonce-named part set, so a glob over the prefix counts 2x source rows. Asserts CORRECT behavior; expected to FAIL until fixed.
    require_alive(LiveService::Postgres);

    const ROWS: i64 = 150;
    const CHUNK_SIZE: i64 = 50; // 3 chunks per run

    let table = seed_pg_numeric_table(ROWS);
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let cfg = write_config(
        &cfg_dir,
        &chunked_config(table.name(), out.path(), CHUNK_SIZE),
    );

    // First chunked run: 3 nonce-named part files.
    let first = run_rivet_with_warn_log(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);
    assert!(
        first.status.success(),
        "first chunked run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(
        total_parquet_rows(out.path()),
        ROWS,
        "sanity: after one chunked run the prefix must hold exactly {ROWS} rows"
    );

    // Second chunked run to the SAME prefix, no --resume.  Chunk filenames carry
    // a random nonce, so even within the same second they land ALONGSIDE the
    // first set rather than overwriting it.
    let second = run_rivet_with_warn_log(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
    ]);
    let second_stderr = String::from_utf8_lossy(&second.stderr);

    // SAFE fix shape #1: the second run refuses (non-zero) into a _SUCCESS prefix.
    if !second.status.success() {
        return;
    }
    // SAFE fix shape #2: the second run loudly warns about the populated prefix.
    if warned_about_existing_prefix(&second_stderr) {
        return;
    }

    // Otherwise: a glob reader over the prefix must see exactly the source row
    // count, not 2x.  Currently the orphaned first-run parts double it.
    let rows_on_disk = total_parquet_rows(out.path());
    assert_eq!(
        rows_on_disk, ROWS,
        "silent re-run doubling: a glob over the prefix counts {rows_on_disk} rows after two \
         chunked runs of a {ROWS}-row source — the first run's parts were orphaned, not replaced, \
         and the second run neither refused nor warned.\nstderr:\n{second_stderr}"
    );
}
