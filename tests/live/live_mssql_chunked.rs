//! Live regression: SQL Server chunked mode with `table:` shortcut auto-resolves
//! `chunk_column` from PK and supports `chunk_size_memory_mb` — parity with
//! the same path on Postgres/MySQL. SQL Server (MSSQL) twin of
//! `tests/live_mysql_chunked.rs`.

use crate::common::*;

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

#[test]
#[ignore = "live: requires docker compose mssql (batch stack :1433)"]
fn roast_mssql_chunk_by_key_on_a_nullable_unique_key_bails_loud() {
    require_alive(LiveService::Mssql);
    mssql_drop_table("a2_keyset_nullable");
    // The REFUSE side of the keyset-key guard, missing until now. A UNIQUE index
    // on a NULLABLE column passes `is_unique = 1` but MUST be rejected by the
    // `c.is_nullable = 0` filter in the MSSQL keyset-key introspection
    // (src/source/mssql/mod.rs:1068) — otherwise keyset `col > cursor` silently
    // DROPS the NULL rows (NULL comparisons are UNKNOWN). Only the ACCEPT side
    // (mssql_keyset_on_non_pk_unique_index, NOT NULL unique) was tested, so a
    // mutant dropping `AND c.is_nullable = 0` survived the whole suite. Through
    // the canonical Rig (a standby of the batch stack, chunked mode + chunk_by_key
    // via export_line). RED-proof: drop the nullable filter → the key is accepted
    // → run succeeds → run_expect_fail's non-zero-exit assert goes RED.
    mssql_exec(
        "CREATE TABLE a2_keyset_nullable (\
           id INT NOT NULL PRIMARY KEY, \
           email NVARCHAR(200) NULL UNIQUE)",
    );
    // One NULL row (id 2) — the row a mutant-accepted keyset would silently lose.
    mssql_exec("INSERT INTO a2_keyset_nullable VALUES (1,N'a@x.com'),(2,NULL),(3,N'c@x.com')");
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out");
    let rig = Rig::mssql_batch("a2_keyset_nullable")
        .mode("chunked")
        .export_line("chunk_by_key: email")
        .export_line("chunk_size: 2")
        .dest_path(out);
    let err = rig.run_expect_fail();
    mssql_drop_table("a2_keyset_nullable");
    assert!(
        err.contains("not a usable keyset key") && err.contains("NOT NULL"),
        "chunk_by_key on a nullable UNIQUE column must bail loud (keyset would silently drop the \
         NULL rows) — got:\n{err}"
    );
}
