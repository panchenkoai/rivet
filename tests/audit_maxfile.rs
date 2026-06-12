//! AUDIT-RED maxfile-validate-require: `max_file_size` is a silent no-op for
//! parquet exports below one row group, and `rivet validate` treats a
//! never-written prefix as a legacy run (exit 0) — a misconfigured CI gate
//! passes against nothing.
//!
//! ## Finding #6/#29 — parquet `max_file_size` silently inert
//!
//! `ExportSink::maybe_split` (src/pipeline/sink/mod.rs) rotates on
//! `writer.bytes_written()`, which for parquet counts only *flushed* bytes.
//! The default row group is ~1,048,576 rows, so any table that fits inside one
//! row group never flushes until `finish()`, `bytes_written()` stays ~0, and
//! `maybe_split` never rotates. A declared `max_file_size: 64KB` therefore
//! produces a single oversized parquet file (the live audit observed 112KB+)
//! with NO warning that the cap was unenforceable for this config.
//!
//! Correct behavior: EITHER the cap is honored (multiple parts) OR rivet emits
//! a warning naming `max_file_size` as not enforceable for this parquet config
//! (so the operator knows the cap did nothing). The silent no-op must not
//! happen. This test asserts that and is expected to FAIL until the fix lands.
//!
//! ## Finding #20 — `validate --prefix <empty>` exits 0
//!
//! `verify_at_destination` (src/pipeline/validate_manifest.rs) maps an absent
//! manifest (`head` → `Ok(None)`) to `ManifestVerification::legacy()` with
//! `legacy_run: true`, and the exit gate (`verdict_fails_exit`) keeps legacy
//! runs at exit 0. So `rivet validate --prefix <never-written-dir>` exits 0,
//! indistinguishable from a real success — a CI gate `rivet validate && deploy`
//! sails past a prefix that was never written. There is no `--require-manifest`
//! (or louder distinction) flag today, so the SAFE correct behavior — a
//! validate against an empty prefix is distinguishable from success — cannot be
//! made true without a production change. The assertion below pins the correct
//! behavior (non-zero exit) and is expected to FAIL until that change lands.
//!
//! Run: `docker compose up -d postgres && cargo test --test audit_maxfile -- --ignored`

mod common;

use common::*;

/// Self-seed a wide content table (`id, title, body, raw_html, metadata jsonb`)
/// with `rows` high-entropy, NON-repetitive rows — distinct md5 hashes
/// concatenated, so zstd cannot crush them the way it would `repeat()`. `rows`
/// stays far below the ~1M-row default parquet row group, so nothing flushes
/// until `finish()` (the condition under which `max_file_size` is inert), yet the
/// single part comfortably exceeds the 64KB cap. Returns the table name and a
/// `PgTable` drop-guard. Self-contained so the test runs in any Postgres job.
fn seed_wide_content(rows: i64) -> (String, PgTable) {
    let name = unique_name("rivet_qa_maxfile");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, title TEXT NOT NULL, \
         body TEXT NOT NULL, raw_html TEXT NOT NULL, metadata JSONB NOT NULL)"
    ))
    .expect("create wide content table");
    // body/raw_html = many DISTINCT md5s concatenated (≈960 / ≈640 hex chars,
    // unique per row) — high-entropy so the column does not dictionary/zstd away.
    c.batch_execute(&format!(
        "INSERT INTO {name} (id, title, body, raw_html, metadata) \
         SELECT g, md5(g::text), \
                (SELECT string_agg(md5((g * 100000 + j)::text), '') \
                   FROM generate_series(1, 30) j), \
                (SELECT string_agg(md5((g * 200000 + j)::text), '') \
                   FROM generate_series(1, 20) j), \
                jsonb_build_object('k', md5((g * 7)::text), 'g', g) \
         FROM generate_series(1, {rows}) g"
    ))
    .expect("seed wide content rows");
    (name.clone(), PgTable::adopt(name))
}

// ─── Finding #6/#29 — parquet max_file_size is a silent no-op ────────────────

// AUDIT-RED maxfile-validate-require: parquet `max_file_size` below one row
// group is a silent no-op (bytes_written counts only flushed bytes), producing
// one oversized file with no warning.
// Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_maxfilesize_parquet_warns_when_inert() {
    require_alive(LiveService::Postgres);

    // Self-seed a wide content fixture (text + jsonb) so the test is independent
    // of the seed-binary `content_items` table — that one is NOT provisioned in
    // the e2e job (it's the heavy ~50K-row fixture deferred to nightly-live), so
    // relying on it made this test pass locally and panic in CI on the empty
    // table. The rows are high-entropy and NON-repetitive (distinct md5s
    // concatenated, not `repeat()` which zstd would crush), so the single-row-
    // group zstd parquet comfortably exceeds the 64KB cap.
    let (table, _guard) = seed_wide_content(3000);

    let export = unique_name("audit_maxfile_pq");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    // Snapshot parquet export with a tiny cap and NO parquet.row_group_rows, so
    // the default ~1M-row group means nothing is flushed until finish() and
    // `maybe_split` never sees `bytes_written() >= max`. Wide columns ensure the
    // single file comfortably exceeds the declared 64KB cap.
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, title, body, raw_html, metadata FROM {table} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    max_file_size: 64KB
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // RUST_LOG=warn so any log::warn! about the cap being unenforceable reaches
    // stderr (this repo's convention for asserting on warnings).
    let run = run_rivet_with_warn_log(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export,
    ]);
    assert!(
        run.status.success(),
        "setup: parquet export must succeed (exit {:?}); stderr:\n{}",
        run.status.code(),
        String::from_utf8_lossy(&run.stderr),
    );

    let parts = files_with_extension(out.path(), "parquet");
    assert!(
        !parts.is_empty(),
        "setup: parquet export must produce at least one .parquet file at {}",
        out.path().display(),
    );

    // Size of the largest single part — used only to make the failure message
    // concrete (it shows the cap was wildly exceeded by one file).
    let max_part_bytes = parts
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .max()
        .unwrap_or(0);

    let stderr = String::from_utf8_lossy(&run.stderr);
    // Accept either remediation: the cap was honored (the file was split into
    // multiple parts), OR rivet warned that `max_file_size` is not enforceable
    // for this parquet config. The warning must NAME the option so the operator
    // can act on it.
    let warned_about_cap = stderr.to_lowercase().contains("max_file_size");
    let split_into_parts = parts.len() > 1;

    assert!(
        split_into_parts || warned_about_cap,
        "SILENT NO-OP: declared `max_file_size: 64KB` but the parquet export produced \
         {} part file(s) (largest {} bytes, ~{:.0}x the 64KB cap) and emitted NO warning \
         naming `max_file_size`. Below one row group `bytes_written()` counts no flushed \
         bytes, so `maybe_split` never rotates and the cap is silently inert. Either honor \
         the cap (rotate into multiple parts) or warn that `max_file_size` is not enforceable \
         for this parquet config.\nstderr:\n{}",
        parts.len(),
        max_part_bytes,
        max_part_bytes as f64 / (64.0 * 1024.0),
        stderr,
    );
}

// ─── Finding #20 — validate against a never-written prefix exits 0 ───────────

// AUDIT-RED maxfile-validate-require: `rivet validate --prefix <empty-dir>`
// treats an absent manifest as a legacy run and exits 0, indistinguishable from
// a real success — a misconfigured CI gate silently passes against nothing.
// The SAFE correct behavior is a louder distinction (e.g. exit non-zero, or a
// `--require-manifest` flag). Asserts CORRECT behavior; expected to FAIL until
// fixed (no such seam exists today, so this exit stays 0).
#[test]
#[ignore = "live: postgres"]
fn audit_validate_absent_prefix_can_fail() {
    require_alive(LiveService::Postgres);

    // A minimal config with a local destination. We never run `rivet run`, so
    // no manifest is ever written at the prefix below.
    let export = unique_name("audit_validate_empty");
    let cfg_dir = tempfile::tempdir().unwrap();
    let dest_dir = tempfile::tempdir().unwrap();
    let empty_prefix = dest_dir.path().join("never_written");
    std::fs::create_dir_all(&empty_prefix).expect("create empty prefix dir");

    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT 1 AS id"
    mode: full
    format: parquet
    compression: zstd
    destination: {{type: local, path: {dir}}}
"#,
        dir = dest_dir.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // --prefix overrides the resolved destination path to a directory that was
    // never written. There is no manifest.json there, so the verifier returns
    // the legacy-run verdict (`legacy_run: true`), which the exit gate keeps at 0.
    let out = run_rivet(&[
        "validate",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        &export,
        "--prefix",
        empty_prefix.to_str().unwrap(),
    ]);

    assert!(
        !out.status.success(),
        "SILENT PASS: `rivet validate --prefix <never-written-dir>` exited {:?} (success), \
         indistinguishable from a real verified run. An absent manifest is mapped to \
         `legacy_run` and the exit gate keeps it at 0, so a CI gate \
         `rivet validate && deploy` sails past a destination that was never written. \
         Validating an empty prefix must be distinguishable from success (a louder \
         distinction or a `--require-manifest` flag).\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
