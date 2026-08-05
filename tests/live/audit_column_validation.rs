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

use crate::common::*;

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

// ─── a scale-0 override truncates money, and only scale 0 does ───────────────

/// A `columns:` override that drops non-zero fractional digits must behave the
/// same at scale 0 as at every other scale: refuse, not truncate.
///
/// `decimal_str_to_scaled_i128` refuses a lossy down-scale by returning `None`
/// so the caller fails loudly — its own comment says the guard exists "rather
/// than silently truncating financial digits". But the scale-0 arm
/// short-circuits before ever reaching it (src/types/decimal.rs:95, and the
/// i256 twin at :161 identically):
///
///   ("123.45",   0) -> Some(123)    the cents are gone
///   ("-99.99",   0) -> Some(-99)
///   ("1.234567", 2) -> None         the guard fires
///   ("1.500",    2) -> Some(150)    trailing zeros drop harmlessly, correct
///
/// End to end on this stand, `numeric(10,2)` overridden to `decimal(20,0)`:
///
///   source   1=123.45  2=-99.99  3=1000.01  4=5.50
///   parquet  1=123     2=-99     3=1000     4=5      status: success
///
/// The product's own behaviour one scale over IS the specification, and this
/// test carries it as the control arm: `numeric(18,6)` overridden to
/// `decimal(20,2)` FAILS the run —
/// `cannot parse DECIMAL "1.234567" as decimal(scale=2)`. Same class of loss,
/// same override mechanism, opposite outcome; the only difference is that the
/// target scale is 0.
///
/// Distinct from its sibling `audit_type_report_flags_lossy_scale_reduction`,
/// which is GREEN: that one asserts `check --type-report` labels the column
/// `lossy`, and it does. The diagnostic was fixed; the RUN was not. Nothing
/// asserted the exported VALUES — and that sibling could not have, because its
/// fixture (`orders.price`) holds 5000 rows and not one of them has a non-zero
/// cent, so no truncation is expressible in it.
// AUDIT-RED decimal-scale0: a scale-0 `columns:` override silently truncates cents while every other scale fails loudly. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_scale_zero_override_must_not_silently_truncate() {
    require_alive(LiveService::Postgres);

    let table = unique_name("audit_scale0");
    let mut c = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, amount numeric(10,2));
         INSERT INTO {table} VALUES (1, 123.45), (2, -99.99), (3, 1000.01), (4, 5.50);"
    ))
    .expect("seed");
    // The fixture MUST carry non-zero cents or the test cannot express the bug —
    // the sibling test's table does not, which is why it never saw this.
    let fractional: i64 = c
        .query_one(
            &format!("SELECT count(*) FROM {table} WHERE amount <> trunc(amount)"),
            &[],
        )
        .expect("count")
        .get(0);
    assert!(
        fractional >= 3,
        "fixture is inert: {fractional} row(s) have a non-zero fractional part"
    );

    let out = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {table}
    table: "public.{table}"
    mode: full
    format: parquet
    columns:
      amount: "decimal(20,0)"
    destination: {{type: local, path: {dir}}}
"#,
        dir = out.path().display()
    );
    let (_cfgdir, cfgpath) = cfg(&yaml);
    let run = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfgpath.to_str().unwrap()])
        .output()
        .expect("spawn rivet run");

    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);

    // Refusing is the correct outcome — it is what scale 2 already does.
    if !run.status.success() {
        return;
    }
    // It ran. Then the values must be intact. DuckDB, not the parquet crate
    // rivet wrote with.
    let got = duckdb_dir_parquet_distinct_strings(out.path(), "CAST(amount AS VARCHAR)");
    let truncated: Vec<&String> = got.iter().filter(|s| !s.contains('.')).collect();
    assert!(
        truncated.is_empty(),
        "a scale-0 `columns:` override exported {} of {} values with the fractional part REMOVED, \
         and the run exited 0 with no warning: {:?}\n\n\
         The same override one scale over refuses: `numeric(18,6)` -> `decimal(20,2)` fails the \
         run with `cannot parse DECIMAL \"1.234567\" as decimal(scale=2)`. The loss is identical \
         in kind; only the target scale differs. src/types/decimal.rs short-circuits scale 0 \
         (line 95, and the i256 twin at 161) before the lossy-down-scale guard whose own comment \
         says it exists \"rather than silently truncating financial digits\".",
        truncated.len(),
        got.len(),
        got
    );
}
