//! Live regression: SQL Server chunked mode with `table:` shortcut auto-resolves
//! `chunk_column` from PK and supports `chunk_size_memory_mb` — parity with
//! the same path on Postgres/MySQL. SQL Server (MSSQL) twin of
//! `tests/live_mysql_chunked.rs`.

mod common;

use common::*;

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_chunked_auto_resolves_chunk_column_from_pk() {
    require_alive(LiveService::Mssql);

    let tbl = seed_mssql_numeric_table(2_000);
    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true

exports:
  - name: chunked_auto_pk
    table: {name}
    mode: chunked
    chunk_size: 500
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
    columns:
      amount: "decimal(12,2)"
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "MSSQL chunked + table: with no explicit chunk_column must auto-resolve from PK:\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // 2,000 rows / chunk_size 500 ⇒ 4 chunk files
    let pq: Vec<_> = std::fs::read_dir(&out_dir)
        .expect("read out_dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(
        pq.len(),
        4,
        "expected 4 chunk files for 2k rows / 500 chunk_size, got {}",
        pq.len()
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_chunk_size_memory_mb_derives_chunk_size() {
    require_alive(LiveService::Mssql);

    // Seed a narrow-ish table; UPDATE STATISTICS so the catalog row-count /
    // average-row-length estimates the memory budget reads are populated.
    let tbl = seed_mssql_numeric_table(20_000);
    mssql_exec(&format!("UPDATE STATISTICS {}", tbl.name()));

    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true

exports:
  - name: mem_budget
    table: {name}
    mode: chunked
    chunk_size_memory_mb: 1
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
    columns:
      amount: "decimal(12,2)"
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "MSSQL memory-budgeted chunked run must succeed:\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // 1 MB ÷ ~64 B/row of narrow table ⇒ ~16k rows/chunk after the 10k-row
    // floor clamp; for 20k rows we expect 2 chunks. Lower bound ≥2 is the
    // invariant — the budget actually shrank chunk_size below the YAML default.
    let pq: Vec<_> = std::fs::read_dir(&out_dir)
        .expect("read out_dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert!(
        pq.len() >= 2,
        "memory budget should have produced ≥2 chunk files; got {}",
        pq.len(),
    );
}
