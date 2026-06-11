//! Live UX tests for `rivet plan` output framing (config/flow audit L9, L10).
//!
//! | ID  | Scenario | Contract |
//! |-----|----------|----------|
//! | L10 | multi-export `plan --format json` (no `--output`) emits ONE valid JSON document (an array) on stdout, not N concatenated objects | stdout parses with serde_json and lists every export name |
//! | L9  | `plan --output FILE` in pretty mode is not a silent no-op | either the file is written, or the command fails with a message naming `--output`/`json` |

mod common;

use common::*;

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
