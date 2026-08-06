//! AUDIT-RED tests for the `plan-apply` cluster.
//!
//! These are RED tests: they assert the CORRECT behaviour and are expected to
//! FAIL against current code until the findings below are fixed. Production
//! code must NOT be touched in this phase.
//!
//! Findings under test (live, Postgres):
//!
//! * #4 — `rivet plan --format json --output FILE` with >1 export writes every
//!   artifact to the SAME path, so only the LAST export survives (no warning,
//!   no per-export distinction). apply then silently runs one export. CORRECT:
//!   both export names must be recoverable from the emitted plan output.
//! * #16 — `rivet apply` does not verify artifact integrity. Editing
//!   `resolved_plan.base_query` (e.g. orders → users) makes apply run the wrong
//!   table cleanly (exit 0). CORRECT: apply must reject a tampered plan rather
//!   than export the wrong table under the planned export name.
//!
//! Harness mirrors `tests/live_plan_apply.rs` exactly: `mod common; use
//! common::*;`, `#[ignore = "live: postgres"]`, drive the real binary via
//! `run_rivet` / `run_rivet_env`, assert on exit code + output files.

use crate::common::*;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// ─── helpers ────────────────────────────────────────────────────────────────

/// Read the total number of rows across every record batch in a Parquet file.
/// Used to identify which seeded table was actually exported: `orders` has
/// 2500 rows, `users` has 500 — a row count alone disambiguates the two.
fn parquet_total_rows(path: &std::path::Path) -> usize {
    let bytes = std::fs::read(path).expect("read parquet file");
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .expect("open parquet reader")
        .build()
        .expect("build parquet record batch reader")
        .map(|b| b.expect("decode record batch").num_rows())
        .sum()
}

/// Collect the set of `export_name` values recoverable from whatever the plan
/// command wrote into `dir`. Handles both the single-file shape (one JSON
/// object) and a hypothetical one-file-per-export shape (the collision finding
/// could be fixed either way) so the test asserts the *outcome* — both names
/// are recoverable — not the file layout.
fn recoverable_export_names(dir: &std::path::Path) -> std::collections::BTreeSet<String> {
    let mut names = std::collections::BTreeSet::new();
    for path in files_with_extension(dir, "json") {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        // A plan file may be a single JSON object or (if a fix emits a stream)
        // multiple. Try a single value first; fall back to line-delimited.
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
            collect_export_names(&v, &mut names);
        } else {
            for line in text.lines().filter(|l| !l.trim().is_empty()) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                    collect_export_names(&v, &mut names);
                }
            }
        }
    }
    names
}

/// Pull every `export_name` string out of a parsed plan JSON value, whether the
/// top level is a single artifact object or an array of artifacts.
fn collect_export_names(v: &serde_json::Value, out: &mut std::collections::BTreeSet<String>) {
    match v {
        serde_json::Value::Array(items) => {
            for item in items {
                collect_export_names(item, out);
            }
        }
        serde_json::Value::Object(_) => {
            if let Some(name) = v.get("export_name").and_then(|n| n.as_str()) {
                out.insert(name.to_string());
            }
        }
        _ => {}
    }
}

/// Replace the value of a single top-level-ish JSON string field
/// `"<key>": "<value>"` in pretty-printed plan JSON. Used to tamper with
/// `resolved_plan.base_query` while leaving the rest of the artifact (including
/// `created_at`, so the plan stays fresh) untouched. Panics if the field is
/// absent so a structural change in the artifact does not silently no-op.
fn replace_json_string_field(json: &str, key: &str, new_value: &str) -> String {
    let needle = format!("\"{key}\": \"");
    let start = json
        .find(&needle)
        .unwrap_or_else(|| panic!("could not find field '{key}' in plan JSON"));
    let value_start = start + needle.len();
    let end_offset = json[value_start..]
        .find('"')
        .expect("unterminated JSON string value");
    let value_end = value_start + end_offset;
    let mut out = json.to_string();
    out.replace_range(value_start..value_end, new_value);
    out
}

// ─── #4: plan --output with multiple exports must preserve ALL exports ────────

/// AUDIT-RED plan-apply: `plan --format json --output FILE` with 2 exports
/// overwrites the same path, keeping only the last. Asserts CORRECT behaviour
/// (both export names recoverable); expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_plan_output_preserves_all_exports() {
    require_alive(LiveService::Postgres);

    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let plan_dir = tempfile::tempdir().unwrap();

    // Two distinct exports against two distinct seeded tables. Unique names so
    // a parallel run cannot collide, but both backed by real tables so plan's
    // preflight succeeds for each.
    let first = unique_name("orders_exp");
    let second = unique_name("users_exp");

    let yaml = format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {first}
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
  - name: {second}
    query: "SELECT id FROM users"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = plan_dir.path().join("plan.json");

    // No --export flag → plan iterates ALL exports in the config.
    let plan_out = run_rivet_env(
        &[
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

    assert!(
        plan_out.status.success(),
        "rivet plan (2 exports) must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );

    let names = recoverable_export_names(plan_dir.path());
    assert!(
        names.contains(&first) && names.contains(&second),
        "plan --output with 2 exports must keep BOTH exports recoverable, \
         but only these export_name(s) survived: {names:?} \
         (expected both '{first}' and '{second}'). \
         Today only the last export survives because every artifact is written \
         to the same --output path."
    );
}

// ─── #16: apply must reject a plan whose base_query was tampered ──────────────

/// AUDIT-RED plan-apply: `apply` does not verify artifact integrity — editing
/// `resolved_plan.base_query` from orders to users runs the wrong table cleanly
/// (exit 0). Asserts CORRECT behaviour (apply rejects the tampered plan rather
/// than exporting users under the orders export name); expected to FAIL until
/// fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_apply_rejects_tampered_plan() {
    require_alive(LiveService::Postgres);

    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    // Plan an `orders` export (2500 rows). Single-column `SELECT id` so the
    // tampered query is valid against both tables and would run cleanly today.
    let export = unique_name("orders_exp");
    let yaml = format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {export}
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    let plan_out = run_rivet_env(
        &[
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );
    assert!(
        plan_out.status.success(),
        "rivet plan must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );

    // Tamper: rewrite the embedded base_query orders → users. `created_at` is
    // left untouched so the plan is still fresh and the staleness gate cannot
    // be what rejects it — only an integrity check can.
    let plan_json = std::fs::read_to_string(&plan_path).expect("read plan.json");
    assert!(
        plan_json.contains("SELECT id FROM orders"),
        "sanity: plan must embed the planned base_query; got:\n{}",
        &plan_json[..plan_json.len().min(800)]
    );
    let tampered = replace_json_string_field(&plan_json, "base_query", "SELECT id FROM users");
    assert!(
        tampered.contains("SELECT id FROM users"),
        "sanity: tamper must have rewritten base_query"
    );
    let tampered_path = cfg_dir.path().join("tampered_plan.json");
    std::fs::write(&tampered_path, &tampered).expect("write tampered plan");

    let apply_out = run_rivet_env(
        &["apply", tampered_path.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );

    // If apply DID run (exit 0), surface which table it exported so the failure
    // message names the wrong value: a users export under the orders name is
    // 500 rows, the planned orders export is 2500 rows.
    let exported_rows: usize = files_with_extension(out_dir.path(), "parquet")
        .iter()
        .map(|p| parquet_total_rows(p))
        .sum();

    assert!(
        !apply_out.status.success(),
        "rivet apply must REJECT a plan whose base_query was tampered \
         (orders → users), but it exited 0 and exported {exported_rows} rows \
         (users=500 vs planned orders=2500). \
         There is no artifact-integrity check, so the wrong table ran cleanly \
         under the planned export name. stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&apply_out.stdout),
        String::from_utf8_lossy(&apply_out.stderr)
    );
}

// ─── the apply wrapper must obey the manifest gap too ────────────────────────

/// `rivet apply` reaches the SAME single-export runner `rivet run` does, so it
/// sets `cursor_high` and can advance the incremental cursor. The guard that
/// refuses to advance it when the manifest did not land was bound only in
/// `run_export_job` — the apply wrapper discarded `finalize_manifest`'s verdict,
/// reported success, and moved the cursor past a window no manifest names. The
/// next run then starts after data nothing describes: a silent skip.
///
/// The failure is injected without touching the product: `manifest.json` is
/// pre-created as a DIRECTORY, so the manifest write fails while every parquet
/// part still lands — exactly the shape the guard exists for (parts durable, the
/// thing that names them absent).
///
/// The oracle is the PERSISTED CURSOR, not rivet's exit code or summary: those
/// describe the run that just ended, while the cursor decides where the next one
/// starts, which is where the loss would actually happen.
#[test]
#[ignore = "live: postgres"]
fn apply_must_not_advance_the_cursor_when_the_manifest_did_not_land() {
    require_alive(LiveService::Postgres);

    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let export = unique_name("orders_incr");
    let yaml = format!(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL

exports:
  - name: {export}
    query: "SELECT id, updated_at FROM orders"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    let plan_out = run_rivet_env(
        &[
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
            "--format",
            "json",
            "--output",
            plan_path.to_str().unwrap(),
        ],
        &[("DATABASE_URL", POSTGRES_URL)],
    );
    assert!(
        plan_out.status.success(),
        "rivet plan must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );

    // Block the manifest write: a directory cannot be replaced by a file rename.
    std::fs::create_dir_all(out_dir.path().join("manifest.json")).expect("block manifest.json");

    let apply_out = run_rivet_env(
        &["apply", plan_path.to_str().unwrap()],
        &[("DATABASE_URL", POSTGRES_URL)],
    );
    let stderr = String::from_utf8_lossy(&apply_out.stderr).into_owned();

    // The fixture must not be inert: if no part landed, the run failed for an
    // unrelated reason and the cursor assertion below would pass vacuously.
    let parts = files_with_extension(out_dir.path(), "parquet");
    assert!(
        !parts.is_empty(),
        "fixture is inert — no parquet part was written, so nothing was at risk; stderr:\n{stderr}"
    );

    let cursor = StateDb::next_to_config(&cfg).cursor_value(&export);
    assert_eq!(
        cursor,
        None,
        "the cursor advanced to {cursor:?} even though the manifest never landed — the next run \
         starts past {} durable part(s) nothing describes; stderr:\n{stderr}",
        parts.len()
    );

    assert!(
        !apply_out.status.success(),
        "a run whose manifest did not land must not report success; stderr:\n{stderr}"
    );
}

// ─── the drift gate must survive the plan → apply handoff ────────────────────

/// `on_schema_drift: fail` must mean the same thing whether the export is run
/// directly or applied from a plan artifact.
///
/// The gate lives inside `prepare_chunk_plan` (src/pipeline/chunked/mod.rs:186,
/// the sole caller of `check_from_type_mappings`), which only the
/// `ChunkSource::Detect` arm reaches. `rivet apply plan.json` supplies
/// `ChunkSource::Precomputed` for any artifact carrying chunk ranges, and all
/// four chunked runners then set `summary.chunks_precomputed = true` with the
/// comment `// drift gate skipped by contract`.
///
/// The contract they cite is "drift was evaluated on the original planning run
/// that produced the ranges". That is not what happens: `src/plan/` contains no
/// drift evaluation at all — `plan/build.rs:153` only COPIES the policy
/// (`schema_drift_policy: export.on_schema_drift`) into the artifact. And even a
/// planning-time evaluation would not help, because the drift this guards
/// against happens BETWEEN plan and apply. That window is the entire reason the
/// plan → review → apply workflow exists.
///
/// Measured on this stand — one config, one source state, `on_schema_drift:
/// fail`, a column dropped after the plan was sealed:
///
///   rivet run    ERROR schema drift detected — aborting (on_schema_drift: fail)
///                      removed: dropme
///   rivet apply  status: success, 500 rows
///
/// The `run` arm is the CONTROL and it is what makes this a contradiction rather
/// than an observation: the same policy, the same drift, refused one way and
/// honoured the other. The non-chunked half of `apply` also applies the gate
/// (single.rs:481), so the split is not even "apply skips it" — it is "apply
/// skips it for chunked exports", which are the ones moving the most data.
// AUDIT-RED plan-apply-drift: `apply` of a chunked plan skips `on_schema_drift`, exporting against a changed source and exiting 0, while `run` with the same config refuses. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn audit_apply_honours_on_schema_drift_for_a_chunked_plan() {
    require_alive(LiveService::Postgres);

    let table = unique_name("audit_drift");
    let mut c = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, keepme text, dropme text);
         INSERT INTO {table} SELECT g, 'k'||g, 'd'||g FROM generate_series(1,500) g;"
    ))
    .expect("seed");
    let _guard = PgTable::adopt(table.clone());

    let out = tempfile::tempdir().unwrap();
    let cfgdir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url_env: DATABASE_URL}}
exports:
  - name: {table}
    table: "public.{table}"
    mode: chunked
    chunk_column: id
    chunk_size: 100
    on_schema_drift: fail
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let cfg = write_config(&cfgdir, &yaml);
    let env = [("DATABASE_URL", POSTGRES_URL)];

    // 1. A first run records the schema the drift gate will compare against.
    let first = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &env);
    assert!(
        first.status.success(),
        "first run must succeed; stderr:\n{}",
        String::from_utf8_lossy(&first.stderr)
    );

    // 2. Seal a plan against that schema.
    let plan = cfgdir.path().join("plan.json");
    let planned = run_rivet_env(
        &[
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--output",
            plan.to_str().unwrap(),
            "--format",
            "json",
        ],
        &env,
    );
    assert!(
        planned.status.success() && plan.is_file(),
        "plan must be written; stderr:\n{}",
        String::from_utf8_lossy(&planned.stderr)
    );

    // 3. The source changes between review and execution — the window the
    //    plan/apply workflow is FOR.
    c.batch_execute(&format!("ALTER TABLE {table} DROP COLUMN dropme"))
        .expect("drop column");

    // 4. CONTROL: the same config, run directly, must refuse. If this passes,
    //    the fixture never produced drift and the apply arm below proves nothing.
    let control = run_rivet_env(&["run", "--config", cfg.to_str().unwrap()], &env);
    let control_text = String::from_utf8_lossy(&control.stderr).into_owned();
    assert!(
        !control.status.success() && control_text.contains("schema drift"),
        "fixture is inert: `rivet run` did NOT refuse after the column was dropped, so there is \
         no drift for `apply` to skip.\nstderr:\n{control_text}"
    );

    // 5. The same policy, the same drift, through the plan artifact.
    let applied = run_rivet_env(&["apply", plan.to_str().unwrap()], &env);
    let applied_text = format!(
        "{}{}",
        String::from_utf8_lossy(&applied.stdout),
        String::from_utf8_lossy(&applied.stderr)
    );
    assert!(
        !applied.status.success(),
        "`rivet apply` of a CHUNKED plan exported against a source whose schema had changed and \
         exited 0, while `rivet run` with the SAME config and the SAME `on_schema_drift: fail` \
         refused the identical drift (control arm above). The gate lives in `prepare_chunk_plan`, \
         which only the `Detect` arm reaches; a precomputed artifact sets \
         `chunks_precomputed = true` and skips it \"by contract\". The cited contract — that \
         drift was evaluated on the planning run — does not hold: `src/plan/` evaluates no \
         drift, it only copies the policy, and the drift here happened AFTER the plan was \
         sealed.\n\napply said:\n{applied_text}"
    );
}
