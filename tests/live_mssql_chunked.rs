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

/// Epic 18 A2: keyset (seek) pagination on a **non-PK** single-column unique
/// NOT NULL index — parity with Postgres/MySQL, which already discover every
/// such index. Before A2 the MSSQL introspector only treated the single-column
/// PK as a keyset key, so `chunk_by_key: <unique non-PK col>` bailed with
/// "not a usable keyset key"; now it runs via keyset.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_keyset_on_non_pk_unique_index() {
    require_alive(LiveService::Mssql);

    mssql_drop_table("a2_keyset_unique");
    mssql_exec(
        "CREATE TABLE a2_keyset_unique (\
           id INT NOT NULL PRIMARY KEY, \
           email NVARCHAR(200) NOT NULL UNIQUE, \
           payload NVARCHAR(100) NULL)",
    );
    mssql_exec(
        "INSERT INTO a2_keyset_unique VALUES \
           (1,N'a@x.com',N'p1'),(2,N'b@x.com',N'p2'),(3,N'c@x.com',N'p3'),\
           (4,N'd@x.com',N'p4'),(5,N'e@x.com',N'p5'),(6,N'f@x.com',N'p6')",
    );

    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true

exports:
  - name: a2_keyset_unique
    table: a2_keyset_unique
    mode: chunked
    chunk_by_key: email
    chunk_size: 2
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    mssql_drop_table("a2_keyset_unique");

    assert!(
        out.status.success(),
        "keyset on a non-PK unique index must be accepted (A2):\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    // 6 rows / page size 2 ⇒ 3 keyset pages = 3 part files; proves it paged by
    // `email` (the unique non-PK key), not a single full scan or a bail.
    let pq: Vec<_> = std::fs::read_dir(&out_dir)
        .expect("read out_dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(pq.len(), 3, "expected 3 keyset pages (6 rows / size 2)");
}
