//! Live E2E tests for `rivet init` against real databases.
//!
//! ## Test coverage
//!
//! | ID | Scenario | Key invariant |
//! |---|---|---|
//! | I1 | Schema-wide Postgres init | Seeded table name appears in YAML; `source.type = postgres` |
//! | I2 | Single-table Postgres init + `rivet check` round-trip | One export; `rivet check` exits 0 with emitted YAML |
//! | I3 | `--out` flag writes file, not stdout | File written; stderr says "Config written to"; stdout empty |
//! | I4 | Schema-wide MySQL init | Seeded table name appears in YAML; `source.type = mysql` |
//! | I5 | Unreachable DB URL | Non-zero exit; stderr contains actionable message |

use crate::common::*;

// ─── I1: schema-wide Postgres → seeded table name appears in YAML ─────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_pg_schema_wide_discovers_seeded_table() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);

    let out = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", POSTGRES_URL])
        .output()
        .expect("spawn rivet init");

    assert!(
        out.status.success(),
        "rivet init must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let yaml = String::from_utf8_lossy(&out.stdout);
    assert!(
        yaml.contains("type: postgres"),
        "emitted YAML must contain 'type: postgres'; got:\n{yaml}"
    );
    assert!(
        yaml.contains("exports:"),
        "emitted YAML must contain 'exports:' section"
    );
    assert!(
        yaml.contains(table.name()),
        "seeded table '{}' must appear in emitted YAML; got:\n{yaml}",
        table.name()
    );
    // Stronger than a bare substring check: every export block starts with
    // `  - name: <export_name>` and `rivet init` defaults the export name to
    // the table name. If the line is present the seeded table was discovered
    // *and* turned into its own export — not just mentioned in passing.
    let expected_export_header = format!("  - name: {}", table.name());
    assert!(
        yaml.contains(&expected_export_header),
        "seeded table '{}' must own a dedicated export block; expected line `{expected_export_header}`; got:\n{yaml}",
        table.name()
    );
    // 10 rows → `full` is the only mode the planner can pick for a table this
    // small. Since 0.6.0 (`feat(config): table: shortcut`) full-mode PG
    // exports on simple identifiers use the `table:` shortcut instead of an
    // explicit `SELECT col1, col2, ... FROM <table>` block, so we no longer
    // assert on `id` / `name` column substrings — see CHANGELOG § 0.6.0.
    assert!(
        yaml.contains("mode: full"),
        "schema-wide init must emit `mode: full` for the tiny seeded table; got:\n{yaml}"
    );
    // Scaffolded YAML must be structurally complete — every export gets a
    // `format:` and a `destination:` block. If either is missing the
    // emitted YAML wouldn't survive `rivet check`.
    assert!(
        yaml.contains("    format: parquet") && yaml.contains("    destination:"),
        "scaffolded YAML must include `format:` + `destination:` for each export; got:\n{yaml}"
    );
}

// ─── I2: single-table Postgres init + rivet check round-trip ──────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_pg_single_table_emits_valid_config_that_passes_check() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(20);
    let cfg_dir = tempfile::tempdir().unwrap();

    // ── Step 1: rivet init --table <seeded_table> ──────────────────────────
    let out = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", POSTGRES_URL, "--table", table.name()])
        .output()
        .expect("spawn rivet init");

    assert!(
        out.status.success(),
        "rivet init --table must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let yaml = String::from_utf8_lossy(&out.stdout);
    // Must reference the seeded table in the query.
    assert!(
        yaml.contains(table.name()),
        "emitted YAML must reference table '{}'; got:\n{yaml}",
        table.name()
    );
    // Must have exactly one export (single-table init).
    let export_count = yaml.matches("  - name:").count();
    assert_eq!(
        export_count, 1,
        "single-table init must emit exactly 1 export; got {export_count}"
    );

    // ── Step 2: replace url_env with url so rivet check can connect ────────
    // The emitted YAML uses `url_env: DATABASE_URL` by default. Swap in the
    // literal URL so the check can run without the env var. Replace only the
    // key=value; the trailing `# export ...` stays a valid YAML comment.
    let yaml_with_url = yaml.replace("url_env: DATABASE_URL", &format!("url: \"{POSTGRES_URL}\""));

    let cfg_path = cfg_dir.path().join("rivet.yaml");
    std::fs::write(&cfg_path, &yaml_with_url).expect("write patched config");

    // ── Step 3: rivet check against the emitted YAML ───────────────────────
    let check = std::process::Command::new(RIVET_BIN)
        .args(["check", "--config", cfg_path.to_str().unwrap()])
        .output()
        .expect("spawn rivet check");

    assert!(
        check.status.success(),
        "rivet check on init-emitted YAML must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&check.stderr),
        String::from_utf8_lossy(&check.stdout)
    );
}

// ─── I3: --out flag writes to file, not stdout ────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_pg_out_flag_writes_file_and_nothing_to_stdout() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let out_dir = tempfile::tempdir().unwrap();
    let yaml_path = out_dir.path().join("scaffold.yaml");

    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "init",
            "--source",
            POSTGRES_URL,
            "--table",
            table.name(),
            "--output",
            yaml_path.to_str().unwrap(),
        ])
        .output()
        .expect("spawn rivet init --out");

    assert!(
        out.status.success(),
        "rivet init --out must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // stdout must be empty when writing to a file.
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.is_empty(),
        "stdout must be empty when --out is used; got:\n{stdout}"
    );

    // stderr must mention the output path.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("Config written to"),
        "stderr must say 'Config written to'; got:\n{stderr}"
    );

    // File must exist and contain valid YAML.
    assert!(
        yaml_path.exists(),
        "output file '{}' must exist after --out",
        yaml_path.display()
    );
    let content = std::fs::read_to_string(&yaml_path).expect("read yaml file");
    assert!(
        content.contains(table.name()),
        "written YAML must contain seeded table name '{}'; got:\n{content}",
        table.name()
    );
}

// ─── I4: schema-wide MySQL init → seeded table appears in YAML ────────────────

#[test]
#[ignore = "live: requires docker compose mysql"]
fn init_mysql_schema_wide_discovers_seeded_table() {
    require_alive(LiveService::Mysql);

    let table = seed_mysql_numeric_table(10);

    let out = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", MYSQL_URL])
        .output()
        .expect("spawn rivet init mysql");

    assert!(
        out.status.success(),
        "rivet init (mysql) must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let yaml = String::from_utf8_lossy(&out.stdout);
    assert!(
        yaml.contains("type: mysql"),
        "emitted YAML must contain 'type: mysql'; got:\n{yaml}"
    );
    assert!(
        yaml.contains(table.name()),
        "seeded table '{}' must appear in emitted YAML; got:\n{yaml}",
        table.name()
    );
}

// ─── I5: unreachable DB URL → non-zero exit with actionable message ───────────

#[test]
#[ignore = "live: requires docker compose (any)"]
fn init_unreachable_url_exits_nonzero_with_actionable_message() {
    // We only need *any* live service to be up so the test runner has confirmed
    // the docker-compose stack is alive — the bad URL deliberately points
    // nowhere to test the error path.
    require_alive(LiveService::Postgres);

    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "init",
            "--source",
            "postgresql://bad:bad@127.0.0.1:19999/bad",
        ])
        .output()
        .expect("spawn rivet init with bad url");

    assert!(
        !out.status.success(),
        "rivet init with unreachable URL must exit non-zero; stdout:\n{}",
        String::from_utf8_lossy(&out.stdout)
    );

    let stderr = String::from_utf8_lossy(&out.stderr);
    // Must emit an error message, not silently fail.
    assert!(
        !stderr.is_empty(),
        "stderr must not be empty on connection failure"
    );
    // The error must be actionable: either mentions the URL, connection refused,
    // or a rivet-level "Error:" prefix.
    let has_actionable = stderr.contains("Error")
        || stderr.contains("error")
        || stderr.contains("connect")
        || stderr.contains("refused")
        || stderr.contains("19999");
    assert!(
        has_actionable,
        "stderr must contain an actionable message; got:\n{stderr}"
    );
}
