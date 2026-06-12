//! AUDIT-RED — cluster `column-applicability` (findings #11, #31, #32, #33).
//!
//! Type / quality references to columns are validated too late or not at all.
//! A live config/flow audit (real binary against the docker stack) confirmed:
//!
//! - #11: `columns: {quantity: string}` on an int4 column is accepted by parse,
//!   plan, AND `check --type-report` (which reports 'exact'!). Only `run` fails
//!   mid-extraction. `check` should reject it pre-flight.
//! - #31: `cursor_column: does_not_exist` — `check` exits 0; only `run` hits
//!   "column does not exist". `check` should flag the missing cursor column.
//! - #32: `columns: {price: decimal(20,0)}` on numeric(10,2) — `check
//!   --type-report` labels the column 'exact', but `run` drops the `.99`
//!   (lossy scale reduction). It must be flagged lossy, not 'exact'.
//! - #33: quality `unique_columns: [nonexistent_col]` is a silent no-op
//!   (run exits 0, quality: pass) — the gate silently vanishes
//!   (CLAUDE.md "never a silent no-op").
//!
//! These tests assert the CORRECT behavior and are expected to FAIL against
//! current code until the validation gap is closed.
//!
//! All tests run against the live, seeded `orders` table
//! (id int, user_id int, product varchar, quantity int, price numeric(10,2),
//! status varchar, notes text, ordered_at/updated_at timestamp) — read-only,
//! so no table is created or dropped.
//!
//! Run with: `cargo test --test audit_column_validation -- --ignored`

mod common;
use common::*;

/// Write `yaml` into a fresh tempdir and return both so the dir stays alive.
fn cfg(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = write_config(&d, yaml);
    (d, p)
}

// ─── #11: inapplicable type override must be rejected by `check` ──────────────

#[test]
#[ignore = "live: postgres"]
// AUDIT-RED column-applicability: `columns:{quantity:string}` on an int4 column is accepted by `check --type-report` (reports 'exact'); only `run` fails. Asserts CORRECT behavior; expected to FAIL until fixed.
fn audit_check_rejects_inapplicable_type_override() {
    require_alive(LiveService::Postgres);

    let export_name = unique_name("audit_inapplicable_override");
    let out = tempfile::tempdir().unwrap();
    // quantity is int4 in the seeded `orders` table; declaring it `string`
    // is an inapplicable override that the run rejects mid-extraction.
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, quantity, price, status FROM orders"
    mode: full
    format: parquet
    columns:
      quantity: "string"
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfgpath.to_str().unwrap(),
            "--export",
            &export_name,
            "--type-report",
        ])
        .output()
        .expect("spawn rivet check --type-report");

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    let combined = format!("{stdout}{stderr}");

    // CORRECT behavior: check must surface the int4→string conflict pre-flight,
    // either by exiting non-zero OR by naming the conflict in its output.
    // Today check exits 0 and the quantity column is reported with fidelity
    // 'exact' — the failure is deferred to `run`.
    let flagged = !result.status.success()
        || combined.contains("quantity")
            && (combined.contains("conflict")
                || combined.contains("incompatible")
                || combined.contains("inapplicable")
                || combined.contains("cannot")
                || combined.contains("mismatch"));
    assert!(
        flagged,
        "check must reject the int4->string override on `quantity` pre-flight \
         (non-zero exit or a named conflict), not defer it to run. \
         exit={:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        result.status.code()
    );
}

// ─── #31: missing cursor column must be flagged by `check` ────────────────────

#[test]
#[ignore = "live: postgres"]
// AUDIT-RED column-applicability: `cursor_column: does_not_exist` — `check` exits 0 (DEGRADED), only `run` hits "column does not exist". Asserts CORRECT behavior; expected to FAIL until fixed.
fn audit_check_flags_missing_cursor_column() {
    require_alive(LiveService::Postgres);

    let export_name = unique_name("audit_missing_cursor");
    let out = tempfile::tempdir().unwrap();
    // does_not_exist is not a column of `orders`; the incremental cursor
    // references a phantom column.
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, quantity, price FROM orders"
    mode: incremental
    cursor_column: does_not_exist
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfgpath.to_str().unwrap(),
            "--export",
            &export_name,
        ])
        .output()
        .expect("spawn rivet check (missing cursor column)");

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    let combined = format!("{stdout}{stderr}");

    // CORRECT behavior: check is a pre-flight gate — a cursor column that does
    // not exist must be flagged here (non-zero exit or a named missing-column
    // error), not silently pass and surface only at `run` time.
    let flagged = !result.status.success()
        || (combined.contains("does_not_exist")
            && (combined.contains("does not exist")
                || combined.contains("not found")
                || combined.contains("missing")
                || combined.contains("unknown")));
    assert!(
        flagged,
        "check must flag the missing cursor column `does_not_exist` \
         (non-zero exit or a missing-column error), not exit 0. \
         exit={:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        result.status.code()
    );
}

// ─── #33: quality gate on a nonexistent column must not silently pass ─────────

#[test]
#[ignore = "live: postgres"]
// AUDIT-RED column-applicability: quality `unique_columns:[nonexistent_col]` is a silent no-op (run exits 0, quality: pass) — the gate vanishes. Asserts CORRECT behavior; expected to FAIL until fixed.
fn audit_run_rejects_quality_on_nonexistent_column() {
    require_alive(LiveService::Postgres);

    let export_name = unique_name("audit_quality_ghost_col");
    let out = tempfile::tempdir().unwrap();
    // nonexistent_col is not produced by the query; a uniqueness gate that
    // references it can never evaluate, so the gate silently disappears.
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, quantity, price, status FROM orders"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    quality:
      unique_columns: [nonexistent_col]
      unique_max_entries: 100000
"#,
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    let combined = format!("{stdout}{stderr}");

    // CORRECT behavior (CLAUDE.md "never a silent no-op"): a quality gate that
    // names a column the export does not produce must NOT silently pass. Rivet
    // must either fail the run (non-zero exit) or emit an explicit
    // "column not found" diagnostic naming the offending column — never just
    // exit 0 with quality treated as a pass.
    let flagged = !result.status.success()
        || (combined.contains("nonexistent_col")
            && (combined.contains("not found")
                || combined.contains("does not exist")
                || combined.contains("unknown")
                || combined.contains("no such column")
                || combined.contains("not produced")));
    assert!(
        flagged,
        "quality unique_columns referencing a nonexistent column must not be a \
         silent no-op — expected non-zero exit or an explicit \
         column-not-found diagnostic naming `nonexistent_col`, got a clean pass. \
         exit={:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        result.status.code()
    );
}

// ─── #32: lossy scale reduction must not be reported 'exact' ──────────────────

#[test]
#[ignore = "live: postgres"]
// AUDIT-RED column-applicability: `columns:{price:decimal(20,0)}` on numeric(10,2) — `check --type-report` says 'exact' but run drops the .99 (lossy scale reduction). Asserts CORRECT behavior; expected to FAIL until fixed.
fn audit_type_report_flags_lossy_scale_reduction() {
    require_alive(LiveService::Postgres);

    let export_name = unique_name("audit_lossy_scale");
    let out = tempfile::tempdir().unwrap();
    // price is numeric(10,2); overriding to decimal(20,0) drops the 2 scale
    // digits — a lossy scale reduction that the run silently truncates.
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, price FROM orders"
    mode: full
    format: parquet
    columns:
      price: "decimal(20,0)"
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            cfgpath.to_str().unwrap(),
            "--export",
            &export_name,
            "--type-report",
            "--json",
        ])
        .output()
        .expect("spawn rivet check --type-report --json");

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);

    // The check should still run (it is allowed to surface the problem as a
    // fidelity downgrade rather than a hard exit). Parse the per-column report
    // and inspect the fidelity of `price`.
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!("check --type-report --json must emit valid JSON ({e}); stdout:\n{stdout}\nstderr:\n{stderr}")
    });
    let cols = json["columns"]
        .as_array()
        .expect("'columns' must be a JSON array");
    let price = cols
        .iter()
        .find(|c| c["column"].as_str() == Some("price"))
        .unwrap_or_else(|| panic!("type-report must include the `price` column; got:\n{stdout}"));
    let fidelity = price["fidelity"].as_str().unwrap_or("<missing>");

    // CORRECT behavior: dropping numeric(10,2) → decimal(20,0) loses the two
    // fractional digits, so the fidelity for `price` must NOT be reported as
    // 'exact' (nor 'compatible'/lossless) — it must be 'lossy'. Today it is
    // reported 'exact', which contradicts what `run` actually does (drops .99).
    assert_ne!(
        fidelity, "exact",
        "type-report must NOT label `price` (numeric(10,2) overridden to \
         decimal(20,0)) as 'exact' — scale-reduction is lossy. \
         reported fidelity={fidelity:?}\nstdout:\n{stdout}"
    );
    assert!(
        fidelity == "lossy",
        "scale-reduction numeric(10,2)->decimal(20,0) must be reported 'lossy'; \
         reported fidelity={fidelity:?}\nstdout:\n{stdout}"
    );
}
