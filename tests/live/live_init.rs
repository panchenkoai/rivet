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

    let out = run_rivet(&["init", "--source", POSTGRES_URL]);

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
    let out = run_rivet(&["init", "--source", POSTGRES_URL, "--table", table.name()]);

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
    let check = run_rivet(&["check", "--config", cfg_path.to_str().unwrap()]);

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

    let out = run_rivet(&[
        "init",
        "--source",
        POSTGRES_URL,
        "--table",
        table.name(),
        "--output",
        yaml_path.to_str().unwrap(),
    ]);

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

    let out = run_rivet(&["init", "--source", MYSQL_URL]);

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

    let out = run_rivet(&[
        "init",
        "--source",
        "postgresql://bad:bad@127.0.0.1:19999/bad",
    ]);

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

// ─── I8: a broken view must not abort the whole schema-wide MySQL init ────────

/// The MySQL arm of the schema scan was the ONE of three that aborted on a
/// table it could list but not introspect — PG and MSSQL already skipped those
/// ("dropped between list and introspect"). Two real inputs hit the MySQL gap:
/// the suite's own parallelism (a sibling's RAII drop mid-scan — how
/// `init_mysql_schema_wide_discovers_seeded_table` flaked on 2026-08-18), and
/// this test's deterministic shape: `list_tables` includes VIEWs, and a view
/// whose base table is gone stays listed in `information_schema.tables` with
/// ZERO columns. One stale view therefore failed every schema-wide `rivet init`
/// against that database — no race required.
///
/// RED against the pre-fix arm: exit non-zero with "not found or has no
/// columns" before the seeded table is ever reached.
#[test]
#[ignore = "live: requires docker compose mysql"]
fn init_mysql_schema_wide_survives_a_broken_view() {
    require_alive(LiveService::Mysql);

    let table = seed_mysql_numeric_table(10);
    // A view over a base table that then disappears — listed, zero columns.
    let base = unique_name("rivet_qa_vbase");
    let view = unique_name("rivet_qa_vbroken");
    {
        use mysql::prelude::Queryable as _;
        let mut c = mysql_connect();
        for stmt in [
            format!("DROP VIEW IF EXISTS {view}"),
            format!("DROP TABLE IF EXISTS {base}"),
            format!("CREATE TABLE {base} (id INT)"),
            format!("CREATE VIEW {view} AS SELECT * FROM {base}"),
            format!("DROP TABLE {base}"),
        ] {
            c.query_drop(stmt).expect("seed broken view");
        }
    }
    let _cleanup = MysqlViewGuard(view.clone());

    let out = run_rivet(&["init", "--source", MYSQL_URL]);
    assert!(
        out.status.success(),
        "schema-wide init must SKIP the broken view, not abort the scan; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let yaml = String::from_utf8_lossy(&out.stdout);
    assert!(
        yaml.contains(table.name()),
        "the seeded table must still be discovered around the broken view; got:\n{yaml}"
    );
    assert!(
        !yaml.contains(&view),
        "the zero-column view must be skipped, not scaffolded as an export; got:\n{yaml}"
    );
}

/// RAII for the broken view — a bare trailing DROP is skipped by every panic
/// path, and a stale broken view then fails EVERY later schema-wide init test
/// on the shared server (exactly the failure this test exists to prevent).
struct MysqlViewGuard(String);
impl Drop for MysqlViewGuard {
    fn drop(&mut self) {
        use mysql::prelude::Queryable as _;
        if let Ok(mut c) = std::panic::catch_unwind(mysql_connect) {
            let _ = c.query_drop(format!("DROP VIEW IF EXISTS {}", self.0));
        }
    }
}

// ─── I9: schema-wide MSSQL init — the arm no test entered ─────────────────────

/// Schema-wide `rivet init` against SQL Server had NO test at any level: the
/// only MSSQL init test is single-table (`--table dbo.…`, audit_init_deferred),
/// which never enters `introspect_all`'s engine dispatch. Found by the mutation
/// gate on PR #245 and then PROVEN empirically — with the whole `"mssql"` match
/// arm DELETED, all 15 init live tests stayed green. Its PG twin
/// (`init_schema_flag_filters_to_schema`) goes RED on the same mutation of its
/// own arm, which is the difference between a live-guarded arm and an unguarded
/// one.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn init_mssql_schema_wide_discovers_seeded_table() {
    require_alive(LiveService::Mssql);

    let table = seed_mssql_numeric_table(10);

    let out = run_rivet(&["init", "--source", MSSQL_URL]);
    assert!(
        out.status.success(),
        "schema-wide rivet init (mssql) must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let yaml = String::from_utf8_lossy(&out.stdout);
    assert!(
        yaml.contains("type: mssql"),
        "emitted YAML must declare the mssql source; got:\n{yaml}"
    );
    assert!(
        yaml.contains(table.name()),
        "seeded table '{}' must be discovered by the schema-wide scan; got:\n{yaml}",
        table.name()
    );
}
