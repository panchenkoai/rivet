//! Live E2E for the two deferred `rivet init` findings (FIX KEY: init).
//!
//! | ID  | Finding | What the test proves |
//! |-----|---------|----------------------|
//! | L0  | init could not scaffold SQL Server | `init --source sqlserver://… --table dbo.orders` exits 0 and the emitted YAML passes `rivet check` (a runnable scaffold). |
//! | L1a | whole-schema dumped everything | `init … --exclude 'bench_*'` omits every `bench_*` object from the scaffold. |
//! | L1b | (same) | `init … --include 'orders'` yields exactly the `orders` export. |
//!
//! Harness mirrors `tests/live_init.rs`: gate on `require_alive`, drive the
//! `RIVET_BIN` built for this test, swap `url_env:` for a literal `url:` so
//! `rivet check` can connect without an env var.

mod common;

use common::*;

// ─── L0: SQL Server single-table init → runnable scaffold (passes check) ───────

#[test]
#[ignore = "live: mssql"]
fn init_mssql_single_table_emits_valid_config_that_passes_check() {
    require_alive(LiveService::Mssql);

    let cfg_dir = tempfile::tempdir().unwrap();

    // ── init --table dbo.orders ────────────────────────────────────────────
    let out = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", MSSQL_URL, "--table", "dbo.orders"])
        .output()
        .expect("spawn rivet init (mssql)");

    assert!(
        out.status.success(),
        "rivet init --table dbo.orders must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let yaml = String::from_utf8_lossy(&out.stdout);
    // SQL Server scaffold: `type: mssql`, schema-qualified `FROM dbo.orders`.
    assert!(
        yaml.contains("type: mssql"),
        "emitted YAML must declare `type: mssql`; got:\n{yaml}"
    );
    assert!(
        yaml.contains("  - name: orders"),
        "dbo.orders must own a dedicated export block; got:\n{yaml}"
    );
    assert!(
        yaml.contains("dbo.orders"),
        "emitted YAML must reference the schema-qualified `dbo.orders`; got:\n{yaml}"
    );
    // Exactly one export (single-table init).
    let export_count = yaml.matches("  - name:").count();
    assert_eq!(
        export_count, 1,
        "single-table init must emit exactly 1 export; got {export_count}:\n{yaml}"
    );
    // The `price DECIMAL(10,2)` column must ride through with declared
    // precision/scale (catalog hint), so no unbounded-decimal REVIEW marker.
    assert!(
        yaml.contains("price: decimal(10,2)"),
        "DECIMAL(10,2) must scaffold with its catalog precision/scale; got:\n{yaml}"
    );

    // ── swap url_env → literal url so `rivet check` can connect ─────────────
    let yaml_with_url = yaml.replace(
        "url_env: DATABASE_URL  # export DATABASE_URL='<your-url>'",
        &format!("url: \"{MSSQL_URL}\""),
    );
    let cfg_path = cfg_dir.path().join("rivet.yaml");
    std::fs::write(&cfg_path, &yaml_with_url).expect("write patched config");

    // ── rivet check on the init-emitted YAML must pass ─────────────────────
    let check = std::process::Command::new(RIVET_BIN)
        .args(["check", "--config", cfg_path.to_str().unwrap()])
        .output()
        .expect("spawn rivet check");

    assert!(
        check.status.success(),
        "rivet check on init-emitted mssql YAML must exit 0; stderr:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&check.stderr),
        String::from_utf8_lossy(&check.stdout)
    );
}

// ─── L1b: --include 'orders' yields ONLY the orders export ────────────────────

#[test]
#[ignore = "live: postgres"]
fn init_pg_include_glob_keeps_only_matching_table() {
    require_alive(LiveService::Postgres);

    // The seeded `public.orders` table is always present (docker-compose seed).
    let out = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", POSTGRES_URL, "--include", "orders"])
        .output()
        .expect("spawn rivet init --include");

    assert!(
        out.status.success(),
        "rivet init --include 'orders' must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let yaml = String::from_utf8_lossy(&out.stdout);
    // The glob `orders` is an exact (anchored) match, so sibling tables like
    // `orders_sparse` / `orders_coalesce` are excluded — exactly one export.
    let export_count = yaml.matches("  - name:").count();
    assert_eq!(
        export_count, 1,
        "`--include 'orders'` must yield exactly 1 export (anchored glob); got {export_count}:\n{yaml}"
    );
    assert!(
        yaml.contains("  - name: orders\n"),
        "the single export must be `orders`; got:\n{yaml}"
    );
}

// ─── L1a: --exclude 'bench_*' omits every bench object ────────────────────────

#[test]
#[ignore = "live: postgres"]
fn init_pg_exclude_glob_drops_bench_tables() {
    require_alive(LiveService::Postgres);

    let out = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", POSTGRES_URL, "--exclude", "bench_*"])
        .output()
        .expect("spawn rivet init --exclude");

    assert!(
        out.status.success(),
        "rivet init --exclude 'bench_*' must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let yaml = String::from_utf8_lossy(&out.stdout);
    // No `bench_*` export survives the exclude glob. Export headers are the
    // authoritative signal (`  - name: bench_…`); a bare `bench_` substring
    // could otherwise appear in an unrelated comment.
    assert!(
        !yaml.contains("  - name: bench_"),
        "`--exclude 'bench_*'` must omit every bench object; got:\n{yaml}"
    );
    // Sanity: the exclude did not nuke the whole schema — a non-bench table
    // (the seeded `orders`) still owns an export.
    assert!(
        yaml.contains("  - name: orders\n"),
        "non-bench tables must still be scaffolded; expected an `orders` export; got:\n{yaml}"
    );
}
