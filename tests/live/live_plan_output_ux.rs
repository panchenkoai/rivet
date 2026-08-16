//! Live UX tests for `rivet plan` output framing (config/flow audit L9, L10).
//!
//! | ID  | Scenario | Contract |
//! |-----|----------|----------|
//! | L10 | multi-export `plan --format json` (no `--output`) emits ONE valid JSON document (an array) on stdout, not N concatenated objects | stdout parses with serde_json and lists every export name |
//! | L9  | `plan --output FILE` in pretty mode is not a silent no-op | either the file is written, or the command fails with a message naming `--output`/`json` |

use crate::common::*;

/// One Postgres export block (full mode, parquet to `out_dir`).
fn pg_export_block(table: &str, out_dir: &std::path::Path) -> String {
    format!(
        "  - name: {table}\n    query: \"SELECT id, name FROM {table}\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: {out}\n",
        out = out_dir.display()
    )
}

/// Postgres config with one export block per table (`--export` omitted plans all).
fn pg_config(tables: &[&str], out_dir: &std::path::Path) -> String {
    let mut yaml =
        String::from("\nsource:\n  type: postgres\n  url_env: DATABASE_URL\n\nexports:\n");
    for t in tables {
        yaml.push_str(&pg_export_block(t, out_dir));
    }
    yaml
}

// ─── L10: multi-export JSON to stdout must be one valid JSON document ─────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_multi_export_json_stdout_is_valid_json_array() {
    require_alive(LiveService::Postgres);

    let table_a = seed_pg_numeric_table(10);
    let table_b = seed_pg_numeric_table(10);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = pg_config(&[table_a.name(), table_b.name()], out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);

    // No `--export`, no `--output`: plan ALL exports, JSON to stdout.
    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--format",
            "json",
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan --format json");

    assert!(
        plan_out.status.success(),
        "rivet plan --format json (multi-export) must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&plan_out.stderr)
    );

    let stdout = String::from_utf8_lossy(&plan_out.stdout);

    // The whole stdout must parse as ONE JSON document. Before the fix this was
    // two pretty-printed objects back-to-back, which `serde_json` (like `jq`)
    // rejects with a trailing-data error.
    let parsed: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("multi-export plan stdout must be a single valid JSON document; parse error: {e}\nstdout:\n{stdout}")
    });

    // It must be an array carrying one artifact per export.
    let arr = parsed
        .as_array()
        .unwrap_or_else(|| panic!("multi-export plan stdout must be a JSON array; got:\n{stdout}"));
    assert_eq!(
        arr.len(),
        2,
        "two exports must yield two artifacts in the array; got {}",
        arr.len()
    );

    let names: Vec<&str> = arr
        .iter()
        .filter_map(|a| a["export_name"].as_str())
        .collect();
    assert!(
        names.contains(&table_a.name()) && names.contains(&table_b.name()),
        "array must contain both export names {:?} and {:?}; got {names:?}",
        table_a.name(),
        table_b.name()
    );
}

// ─── L9: `--output` in pretty mode must not silently no-op ───────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_pretty_with_output_is_not_a_silent_noop() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = pg_config(&[table.name()], out_dir.path());
    let cfg = write_config(&cfg_dir, &yaml);
    let plan_path = cfg_dir.path().join("plan.json");

    let plan_out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "pretty",
            "--output",
            plan_path.to_str().unwrap(),
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan --format pretty --output");

    let stderr = String::from_utf8_lossy(&plan_out.stderr);
    let stdout = String::from_utf8_lossy(&plan_out.stdout);

    if plan_path.exists() {
        // Acceptable: `--output` was honored by writing the artifact.
        let written = std::fs::read_to_string(&plan_path).expect("read written plan");
        let _: serde_json::Value =
            serde_json::from_str(&written).expect("a written plan file must be valid JSON");
    } else {
        // Otherwise the command must NOT silently succeed: it must surface a
        // clear message that `--output` needs `--format json` (so nothing is
        // silently dropped). This is the current contract (CLI cross-flag
        // validation).
        assert!(
            !plan_out.status.success(),
            "pretty + --output must not silently succeed without writing a file; \
             stdout:\n{stdout}\nstderr:\n{stderr}"
        );
        let combined = format!("{stdout}{stderr}");
        assert!(
            combined.contains("--output") && combined.contains("json"),
            "rejection message must explain that --output needs --format json; \
             stdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }
}

// ─── the artifact's row_estimate: the number nothing read ────────────────────

/// `plan.json`'s `row_estimate` must describe the real table.
///
/// The 2026-08-16 mutation run left `compute_plan_data -> Ok(Default::default())`
/// alive, and triage found why: the estimate had NO oracle at any level. The
/// arithmetic is unreachable offline (the function opens a real source before
/// computing anything), and nothing live ever read the value — `grep -rn
/// row_estimate tests/` found only the PREFLIGHT estimate, which is a different
/// code path (`src/preflight/analysis.rs`), and three frozen legacy fixtures
/// that are compared against themselves. So a planner that reported a garbage
/// row count — or none at all — shipped green through every gate.
///
/// The fixture makes the expected value EXACT rather than a tolerance: the seed
/// helper writes dense ids `0..N-1`, and a chunked plan's estimate is the key
/// SPAN (`last_hi - first_lo + 1`), which for a gapless key is the row count
/// itself. A tolerance would have let `Default::default()` (None) and an
/// off-by-one both pass.
///
/// Kills: `replace compute_plan_data -> Result<ComputedPlanData> with
/// Ok(Default::default())`, and any arithmetic slip in the span it computes.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn plan_artifact_row_estimate_matches_the_real_table() {
    require_alive(LiveService::Postgres);

    const ROWS: i64 = 5_000;
    let table = seed_pg_numeric_table(ROWS);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    // `table:` (not `query:`) so the planner can verify the key is integer-family
    // and take the range-chunk path this estimate belongs to.
    let yaml = format!(
        "\nsource:\n  type: postgres\n  url_env: DATABASE_URL\n\nexports:\n\
         \x20 - name: {name}\n    table: {name}\n    mode: chunked\n    chunk_column: id\n\
         \x20   chunk_size: 1000\n    format: parquet\n    destination:\n      type: local\n\
         \x20     path: {out}\n",
        name = table.name(),
        out = out_dir.path().display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "plan",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            table.name(),
            "--format",
            "json",
        ])
        .env("DATABASE_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet plan --format json");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        out.status.success(),
        "rivet plan must exit 0; stderr:\n{stderr}"
    );

    let doc: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "plan --format json must emit one JSON document; parse error: {e}\nstdout:\n{}",
            String::from_utf8_lossy(&out.stdout)
        )
    });

    // Fixture liveness: a plan that computed nothing at all would also have no
    // chunks, and then the estimate assertion below would be grading absence
    // against absence.
    let chunks = doc["computed"]["chunk_count"]
        .as_u64()
        .or_else(|| {
            doc["computed"]["chunk_ranges"]
                .as_array()
                .map(|c| c.len() as u64)
        })
        .unwrap_or(0) as usize;
    assert!(
        chunks >= 4,
        "fixture went inert: a {ROWS}-row table at chunk_size 1000 must plan several \
         chunks, got {chunks} — the estimate below would grade nothing:\n{doc:#}"
    );

    let est = doc["computed"]["row_estimate"].as_i64().unwrap_or_else(|| {
        panic!(
            "the plan artifact carries NO row_estimate — a planner that computes \
             nothing reports nothing, and every downstream consumer (wave packing, \
             the pool's makespan, the operator's `Row est.` line) reads it:\n{doc:#}"
        )
    });
    assert_eq!(
        est,
        ROWS,
        "the artifact's row_estimate must describe the real table: the seed writes \
         dense ids 0..{last}, so the chunked key span IS the row count. Got {est} \
         for {ROWS} rows",
        last = ROWS - 1,
    );
}
