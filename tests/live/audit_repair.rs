//! AUDIT-RED (cluster repair-trust): the reconcile → repair → reconcile loop
//! does not close, and `repair --execute` leaves the dataset in a state that
//! `rivet validate` flags.
//!
//! Two findings, both observed live against the docker stack:
//!
//! #7  `repair --execute` re-exports a mismatched chunk and prints
//!     "executed 1 · failed 0" (exit 0), but it drives the *stateless*
//!     chunked path (`pipeline::chunked::exec::run_chunked_sequential`),
//!     which never touches `chunk_task`. The stored `rows_written` for the
//!     chunk is left exactly as it was, so the very next `reconcile` recounts
//!     the source, compares against the *same* stale `rows_written`, and
//!     reports the *same* mismatch (exit 1). The reconcile → repair →
//!     reconcile loop therefore never converges.
//!
//! #8  The repair re-export lands a new parquet file alongside the originals
//!     (collision-proof naming, ADR-0009 RR5) but `manifest.json` is never
//!     updated to record it. A subsequent `rivet validate` lists the prefix
//!     and flags the repair-written file as an `untracked_object`.
//!
//! Both tests assert the CORRECT behavior (loop converges; manifest tracks the
//! repair file) and are expected to FAIL until the repair path updates state
//! and the manifest.

use crate::common::*;

// ─── setup helper ─────────────────────────────────────────────────────────────

/// Seed a `row_count`-row table, write a chunked-checkpoint config, run the
/// export, and return the table guard plus the output dir, config dir, and
/// config path. The state DB lands at `<config_dir>/.rivet_state.db`.
fn seed_and_run_chunked(row_count: i64, chunk_size: u32) -> (PgTable, Rig) {
    let table = seed_pg_numeric_table(row_count);
    let rig = Rig::pg_batch(table.name())
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line(&format!("chunk_size: {chunk_size}"))
        .export_line("chunk_checkpoint: true");
    let run_out = rig.run_args(&["--export", table.name()]);
    assert!(
        run_out.status.success(),
        "setup export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run_out.stderr)
    );
    (table, rig)
}

/// Run a SQL statement against the on-disk SQLite state DB via the `sqlite3`
/// CLI. The audit forced a chunk mismatch exactly this way (a direct
/// `UPDATE chunk_task ...`) — it is the most faithful reproduction because it
/// guarantees the mismatch survives a re-export that never touches state.
fn sqlite3_exec(state_db: &std::path::Path, sql: &str) {
    let out = std::process::Command::new("sqlite3")
        .arg(state_db)
        .arg(sql)
        .output()
        .expect("spawn sqlite3 (state DB edit)");
    assert!(
        out.status.success(),
        "sqlite3 edit failed: {sql}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `rivet reconcile --format json` → (exit success, parsed report).
fn reconcile_json(rig: &Rig, export: &str) -> (bool, serde_json::Value) {
    let out = rig.cli(&["reconcile", "--export", export, "--format", "json"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "reconcile --format json must emit valid JSON (err: {e}); stdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        )
    });
    (out.status.success(), json)
}

// ─── Finding #7: reconcile → repair → reconcile must converge ─────────────────

// AUDIT-RED repair-trust: `repair --execute` re-exports a chunk but never
// updates chunk_task.rows_written, so the next reconcile reports the SAME
// mismatch — the trust loop never converges. Asserts CORRECT behavior;
// expected to FAIL until fixed.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn audit_repair_then_reconcile_converges() {
    require_alive(LiveService::Postgres);
    let (table, rig) = seed_and_run_chunked(100, 50);
    let state_db = rig.config_path().parent().unwrap().join(".rivet_state.db");

    // Force a chunk mismatch the way the audit did: corrupt the stored
    // exported count for chunk 0 directly in the state DB. The source still
    // holds 50 rows in [1..50], so reconcile will see source=50 vs exported=999.
    sqlite3_exec(
        &state_db,
        "UPDATE chunk_task SET rows_written = 999 WHERE chunk_index = 0;",
    );

    // Precondition: reconcile detects the mismatch and gates (exit non-zero).
    let (ok_before, before) = reconcile_json(&rig, table.name());
    assert!(
        !ok_before,
        "precondition: reconcile must exit non-zero on the forced mismatch; report:\n{before:#}"
    );
    assert_eq!(
        before["summary"]["mismatches"].as_u64(),
        Some(1),
        "precondition: exactly one chunk must be mismatched before repair; report:\n{before:#}"
    );

    // Repair the mismatch.
    let repair = rig.cli(&["repair", "--export", table.name(), "--execute"]);
    assert!(
        repair.status.success(),
        "repair --execute must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&repair.stderr)
    );

    // CORRECT behavior: having repaired the only mismatched chunk, a fresh
    // reconcile must now report a clean MATCH and exit 0 — the loop converges.
    let (ok_after, after) = reconcile_json(&rig, table.name());
    let mismatches = after["summary"]["mismatches"].as_u64().unwrap_or(u64::MAX);
    assert_eq!(
        mismatches, 0,
        "BROKEN TRUST LOOP: after `repair --execute` reconcile still reports {mismatches} \
         mismatch(es) (chunk_task.rows_written was never updated by the repair re-export); \
         report:\n{after:#}"
    );
    assert!(
        ok_after,
        "BROKEN TRUST LOOP: reconcile must exit 0 after a successful repair, but it still \
         gates non-zero — reconcile → repair → reconcile never converges; report:\n{after:#}"
    );
}

// ─── Finding #8: `repair --execute` must keep `rivet validate` clean ──────────

// AUDIT-RED repair-trust: the repair re-export lands a new file but never
// updates manifest.json, so `rivet validate` flags it as an untracked object.
// Asserts CORRECT behavior (manifest tracks the repair file); expected to FAIL
// until fixed.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn audit_repair_keeps_validate_clean() {
    require_alive(LiveService::Postgres);
    let (table, rig) = seed_and_run_chunked(100, 50);

    // After the clean run, manifest.json + _SUCCESS + 2 chunk parts are at the
    // prefix; validate should be clean.
    let count_parquet = |dir: &std::path::Path| files_with_extension(dir, "parquet").len();
    let parts_before = count_parquet(&rig.out_dir());
    assert_eq!(
        parts_before, 2,
        "100 rows / chunk_size 50 → 2 chunk parts after the initial export"
    );

    // Drift the source under chunk 0 [1..50]: delete 20 rows → a real
    // mismatch (source 30 vs exported 50) that repair will re-export.
    let mut c = pg_connect();
    c.batch_execute(&format!("DELETE FROM {} WHERE id <= 20", table.name()))
        .expect("drift source");

    // Repair: re-exports chunk 0 as a NEW file alongside the originals.
    let repair = rig.cli(&["repair", "--export", table.name(), "--execute"]);
    assert!(
        repair.status.success(),
        "repair --execute must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&repair.stderr)
    );

    // Sanity: the repair actually wrote a new parquet file at the prefix —
    // that file is the candidate the manifest must learn about.
    let parts_after = count_parquet(&rig.out_dir());
    assert!(
        parts_after > parts_before,
        "repair --execute must write a new chunk file at the prefix (before {parts_before}, \
         after {parts_after})"
    );

    // Validate the prefix as a machine-readable report.
    let validate = rig.cli(&["validate", "--export", table.name(), "--format", "json"]);
    let stdout = String::from_utf8_lossy(&validate.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "validate --format json must emit valid JSON (err: {e}); stdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&validate.stderr)
        )
    });

    // CORRECT behavior: every part under the prefix is tracked by the
    // manifest, so validate surfaces NO `untracked_object` failure. Today the
    // repair-written file is untracked because the manifest was never updated.
    let failures = json["exports"][0]["verification"]["failures"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let untracked: Vec<&serde_json::Value> = failures
        .iter()
        .filter(|f| f["kind"].as_str() == Some("untracked_object"))
        .collect();
    assert!(
        untracked.is_empty(),
        "UNTRACKED REPAIR FILE: `repair --execute` wrote a new part but did not update \
         manifest.json, so `rivet validate` flags it as an untracked object: {untracked:#?}\n\
         full verification:\n{}",
        json["exports"][0]["verification"]
    );
}

// ─── Gate finding: a repaired prefix must still pass `validate --depth full` ──

/// Seed `id BIGINT, v TEXT` over 1..=1000 minus 500..=549, export chunked, insert the gap, repair.
fn seed_gap_run_insert_repair() -> (PgTable, Rig) {
    let (table, rig) = seed_gap_run_insert();
    let repair = rig.cli(&["repair", "--export", table.name(), "--execute"]);
    assert!(
        repair.status.success(),
        "repair: {}",
        String::from_utf8_lossy(&repair.stderr)
    );
    let (ok, after) = reconcile_json(&rig, table.name());
    assert!(ok, "reconcile must converge after repair: {after:#}");
    (table, rig)
}

/// Seed 1..=1000 minus 500..=549, export chunked (chunk_size 250), insert the gap; reconcile sees it.
fn seed_gap_run_insert() -> (PgTable, Rig) {
    let name = unique_name("repair_formb");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (id BIGINT PRIMARY KEY, v TEXT NOT NULL); \
         INSERT INTO {name} SELECT g, 'v' || g FROM generate_series(1, 1000) g \
         WHERE g NOT BETWEEN 500 AND 549;"
    ))
    .expect("seed");
    let table = PgTable::adopt(name);
    let rig = Rig::pg_batch(table.name())
        .census_oracle()
        .query(&format!("SELECT id, v FROM {}", table.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 250")
        .export_line("chunk_checkpoint: true");
    let run = rig.run_args(&["--export", table.name()]);
    assert!(
        run.status.success(),
        "export: {}",
        String::from_utf8_lossy(&run.stderr)
    );
    c.batch_execute(&format!(
        "INSERT INTO {} SELECT g, 'v' || g FROM generate_series(500, 549) g",
        table.name()
    ))
    .expect("insert gap");
    let (ok, _) = reconcile_json(&rig, table.name());
    assert!(!ok, "precondition: reconcile must see the inserted gap");
    (table, rig)
}

/// The canonical manifest at `out` as JSON.
fn manifest_json(out: &std::path::Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap()
}

/// Paths of the manifest parts with `status`.
fn parts_with_status(m: &serde_json::Value, status: &str) -> Vec<String> {
    m["parts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|p| p["status"] == status)
        .map(|p| p["path"].as_str().unwrap().to_string())
        .collect()
}

/// DuckDB `[count(*), count(DISTINCT id)]` over `names` under the oracle's view of `out`.
fn duck_count_distinct(rig: &Rig, names: &[String]) -> serde_json::Value {
    let container = rig.oracle_container_out();
    let files: Vec<String> = names.iter().map(|n| format!("'{container}/{n}'")).collect();
    duckdb_run_sql_json(&format!(
        "SELECT count(*), count(DISTINCT id) FROM read_parquet([{}])",
        files.join(", ")
    ))["rows"][0]
        .clone()
}

/// `validate --depth full` → (exit code, stdout+stderr).
fn validate_full(rig: &Rig, export: &str) -> (Option<i32>, String) {
    let v = rig.cli(&["validate", "--export", export, "--depth", "full"]);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&v.stdout),
        String::from_utf8_lossy(&v.stderr)
    );
    (v.status.code(), text)
}

#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn repaired_prefix_passes_validate_full_value_checksum() {
    require_alive(LiveService::Postgres);
    let (table, rig) = seed_gap_run_insert_repair();

    // Independent oracle: the DISTINCT (id, v) set of the declared parts equals the source, both ways.
    let container = rig.oracle_container_out();
    let files: Vec<String> = declared_parquet_parts(&rig.out_dir())
        .iter()
        .map(|p| format!("'{container}/{}'", p.file_name().unwrap().to_string_lossy()))
        .collect();
    assert_eq!(
        files.len(),
        4,
        "4 chunks, each declared once (the repaired ones by their repair part): {files:?}"
    );
    let e = OracleEngine::Postgres;
    let (attach, from) = e.source_sql("rivet", table.name());
    let v = duckdb_run_sql_json(&format!(
        "{} {attach} WITH s AS (SELECT DISTINCT CAST(id AS BIGINT) id, CAST(v AS VARCHAR) v \
         FROM {from}), d AS (SELECT DISTINCT CAST(id AS BIGINT) id, CAST(v AS VARCHAR) v \
         FROM read_parquet([{}])) \
         SELECT (SELECT count(*) FROM (SELECT * FROM s EXCEPT SELECT * FROM d)), \
                (SELECT count(*) FROM (SELECT * FROM d EXCEPT SELECT * FROM s)), \
                (SELECT count(*) FROM d)",
        e.load_sql(),
        files.join(", ")
    ));
    assert_eq!(
        v["rows"][0],
        serde_json::json!(["0", "0", "1000"]),
        "DuckDB: the declared parts must equal the source"
    );

    let names: Vec<String> = declared_parquet_parts(&rig.out_dir())
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        duck_count_distinct(&rig, &names),
        serde_json::json!(["1000", "1000"]),
        "DuckDB over the DECLARED parts: every source row exactly once, no duplicate from a \
         superseded original: {names:?}"
    );
    let m = manifest_json(&rig.out_dir());
    let superseded = parts_with_status(&m, "superseded");
    assert!(
        !superseded.is_empty(),
        "the repaired chunks' originals must be superseded: {m:#}"
    );
    for s in &superseded {
        assert!(
            rig.out_dir().join(s).is_file(),
            "RR5: the superseded file '{s}' stays on disk"
        );
    }

    let (code, text) = validate_full(&rig, table.name());
    assert_eq!(
        code,
        Some(0),
        "validate --depth full must PASS on a correctly repaired prefix (the manifest's value \
         checksums must include the repair parts); output:\n{text}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn the_load_resolver_reads_a_repaired_prefix_without_duplicates() {
    require_alive(LiveService::Postgres);
    let (_table, rig) = seed_gap_run_insert_repair();
    let out = rig.out_dir();
    let m: rivet::manifest::RunManifest =
        serde_json::from_str(&std::fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap();
    let all: Vec<String> = files_with_extension(&out, "parquet")
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    let selected =
        rivet::load::reconcile::select_load_keys(&[("manifest.json".to_string(), m)], &all);
    assert!(
        selected.len() < all.len(),
        "fixture inert: the prefix holds no superseded file: {all:?}"
    );
    assert_eq!(
        duck_count_distinct(&rig, &selected),
        serde_json::json!(["1000", "1000"]),
        "the parts `rivet load` would read hold every row exactly once: {selected:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn repair_stays_additive_when_an_original_part_has_no_chunk_index() {
    require_alive(LiveService::Postgres);
    let (table, rig) = seed_gap_run_insert();
    let out = rig.out_dir();

    // Rename one original part to a name that carries no chunk index.
    let mut m = manifest_json(&out);
    let old = m["parts"][0]["path"].as_str().unwrap().to_string();
    std::fs::rename(out.join(&old), out.join("legacy_part.parquet")).unwrap();
    m["parts"][0]["path"] = serde_json::json!("legacy_part.parquet");
    let bytes = serde_json::to_string_pretty(&m).unwrap().into_bytes();
    for f in std::fs::read_dir(&out).unwrap().map(|e| e.unwrap().path()) {
        let n = f.file_name().unwrap().to_string_lossy().into_owned();
        if n.starts_with("manifest") && n.ends_with(".json") {
            std::fs::write(&f, &bytes).unwrap();
        }
    }
    std::fs::write(
        out.join("_SUCCESS"),
        rivet::manifest::success_marker_body(&bytes),
    )
    .unwrap();

    let repair = rig.cli(&["repair", "--export", table.name(), "--execute"]);
    let stderr = String::from_utf8_lossy(&repair.stderr);
    assert!(repair.status.success(), "repair: {stderr}");
    assert!(
        stderr.contains("carries no chunk index") && stderr.contains("(additive)"),
        "the fallback must say why it stayed additive: {stderr}"
    );
    let m = manifest_json(&out);
    assert!(
        parts_with_status(&m, "superseded").is_empty(),
        "never guess: {m:#}"
    );
    let committed = parts_with_status(&m, "committed");
    assert_eq!(
        committed.len(),
        6,
        "4 originals + 2 repair parts stay declared: {m:#}"
    );
    assert_eq!(
        duck_count_distinct(&rig, &committed)[1],
        "1000",
        "no row lost by the fallback"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres duckdb"]
fn repaired_prefix_with_a_corrupted_part_still_fails_validate_full() {
    use arrow::array::{RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    require_alive(LiveService::Postgres);
    let (table, rig) = seed_gap_run_insert_repair();
    let out = rig.out_dir();
    let m: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap();
    assert!(
        m["column_checksums"]
            .as_array()
            .is_some_and(|a| !a.is_empty()),
        "Form B must survive repair or this test grades nothing: {m}"
    );

    // Tamper one `v` cell in the newest part (the repair part): same schema, same row count.
    let part = files_with_extension(&out, "parquet")
        .into_iter()
        .max_by_key(|p| std::fs::metadata(p).unwrap().modified().unwrap())
        .unwrap();
    let batches: Vec<RecordBatch> =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&part).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .map(|b| b.unwrap())
            .collect();
    let schema = batches[0].schema();
    let idx = schema.index_of("v").unwrap();
    {
        let f = std::fs::File::create(&part).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        for (bi, b) in batches.iter().enumerate() {
            let mut cols = b.columns().to_vec();
            if bi == 0 {
                let col = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let mut vals: Vec<String> =
                    col.iter().map(|x| x.unwrap_or("").to_string()).collect();
                vals[0].push('x');
                cols[idx] = std::sync::Arc::new(StringArray::from(vals));
            }
            w.write(&RecordBatch::try_new(b.schema(), cols).unwrap())
                .unwrap();
        }
        w.close().unwrap();
    }
    // Neutralise the size gate and re-stamp `_SUCCESS`, so only the value leg can fire.
    let part_name = part.file_name().unwrap().to_string_lossy().into_owned();
    let new_size = std::fs::metadata(&part).unwrap().len();
    for mf in std::fs::read_dir(&out).unwrap().map(|e| e.unwrap().path()) {
        let n = mf.file_name().unwrap().to_string_lossy().into_owned();
        if !(n.starts_with("manifest") && n.ends_with(".json")) {
            continue;
        }
        let mut j: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&mf).unwrap()).unwrap();
        for p in j["parts"].as_array_mut().unwrap() {
            if p["path"].as_str().is_some_and(|x| x.ends_with(&part_name)) {
                p["size_bytes"] = serde_json::json!(new_size);
            }
        }
        let bytes = serde_json::to_string_pretty(&j).unwrap().into_bytes();
        std::fs::write(&mf, &bytes).unwrap();
        if n == "manifest.json" {
            std::fs::write(
                out.join("_SUCCESS"),
                rivet::manifest::success_marker_body(&bytes),
            )
            .unwrap();
        }
    }

    let (code, text) = validate_full(&rig, table.name());
    assert_eq!(
        code,
        Some(3),
        "a corrupted repaired prefix must fail validate; output:\n{text}"
    );
    assert!(
        text.contains("[RIVET_VERIFY_VALUE_CHECKSUM]") && text.contains("column 'v'"),
        "the failure must be the value checksum on `v`, not another gate; output:\n{text}"
    );
}
