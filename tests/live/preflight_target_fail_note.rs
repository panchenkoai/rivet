//! L8 (preflight-target): a column rendered "fail ✗" for `--target` does NOT
//! gate the exit code unless `--strict` is also passed — that is the gate by
//! design (preflight/mod.rs only bails `if strict && any_fatal`). The glyph,
//! however, implies a hard failure, so an operator or CI reading it alone would
//! wrongly assume rc != 0. The fix prints a one-line note in that case; this
//! test drives the real binary to prove the note is produced AND the exit is
//! still 0 (the gating default is unchanged).

use crate::common::*;

/// RAII table guard local to this test (the shared `PgTable` has a private
/// field and no public constructor for arbitrary DDL, so we keep a small
/// panic-safe drop here rather than reach into the helper's internals).
struct BqFailTable {
    name: String,
}

impl BqFailTable {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for BqFailTable {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.name), &[]);
        }
    }
}

/// Postgres table whose `huge` column is `NUMERIC(80,0)` — precision 80 exceeds
/// BigQuery BIGNUMERIC's max of 76, so the BigQuery target resolver returns a
/// hard `Fail` for it (src/types/target.rs `decimal`). That is what renders the
/// "fail ✗" glyph the note explains.
fn seed_bq_failing_table() -> BqFailTable {
    let name = unique_name("rivet_bqfail_tbl");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            huge NUMERIC(80,0) NOT NULL
        );
        INSERT INTO {name} (id, huge) VALUES (1, 12345);"
    ))
    .expect("create bq-failing test table");
    BqFailTable { name }
}

#[test]
#[ignore = "live: postgres"]
fn check_target_fail_without_strict_notes_ungated_exit() {
    require_alive(LiveService::Postgres);

    let table = seed_bq_failing_table();
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, huge FROM {name}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // `--target bigquery` turns on the type report (dispatch wires
    // `tgt.is_some()` into show_type_report); no `--strict`, so the exit is NOT
    // gated even though `huge` resolves to a hard FAIL.
    let result = run_rivet(&[
        "check",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
        "--target",
        "bigquery",
    ]);

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );

    // Gate is unchanged: without --strict a target FAIL must still exit 0.
    assert!(
        result.status.success(),
        "without --strict a target FAIL must not gate the exit; got exit {:?}\n{combined}",
        result.status.code()
    );
    // But the note must appear so the "fail ✗" glyph doesn't mislead.
    assert!(
        combined.contains("FAIL bigquery compatibility")
            && combined.contains("--strict")
            && combined.contains("exit 0"),
        "expected the un-gated FAIL note naming the target and the --strict gate; got:\n{combined}"
    );
}

#[test]
#[ignore = "live: postgres"]
fn check_target_fail_with_strict_gates_exit_nonzero() {
    // Contrast: the same FAILing column WITH --strict gates the exit non-zero,
    // proving the note path is the only thing the no-strict change adds (the
    // gate itself is untouched).
    require_alive(LiveService::Postgres);

    let table = seed_bq_failing_table();
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, huge FROM {name}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = run_rivet(&[
        "check",
        "--config",
        cfg.to_str().unwrap(),
        "--export",
        table.name(),
        "--target",
        "bigquery",
        "--strict",
    ]);

    assert!(
        !result.status.success(),
        "with --strict a target FAIL must gate the exit non-zero; got exit {:?}\nstdout:\n{}\nstderr:\n{}",
        result.status.code(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
