//! Differential correctness at scale (Tier-1 trust lever).
//!
//! rivet's whole pitch is "I won't lose, duplicate, or corrupt your rows." The
//! strongest proof of that is not "we wrote tests" but "an INDEPENDENT reader
//! agrees with the SOURCE on the exported data." This test computes an
//! order-independent content fingerprint two ways:
//!
//! - SOURCE: aggregates run by the source database itself (Postgres).
//! - DEST: the same aggregates run by DuckDB over the exported Parquet — a
//!   third-party reader that never saw rivet's internal counters.
//!
//! If rivet drops a row, duplicates one, or corrupts a value, at least one
//! fingerprint field diverges (count, the integer sums, the text-length sum, or
//! the distinct-id / distinct-text counts). Neither side is rivet's own
//! bookkeeping, so a regression that keeps rivet's counters self-consistent
//! (the systemic blind spot from the data-correctness audit) still fails here.
//!
//! The export runs in CHUNKED mode (multi-file), so the differential also covers
//! the multi-part write/commit path at a non-trivial row count.
//!
//! Run: `docker compose up -d postgres rivet-duckdb && \
//!       cargo test --test live_differential -- --ignored`

mod common;
use common::*;
use mysql::prelude::Queryable;
use proptest::prelude::*;

/// Row count. Large enough that the export produces several chunk files and the
/// aggregates are non-trivial, small enough to stay a fast CI gate. The harness
/// scales: bump `N` (and `chunk_size`) for a heavier soak.
const N: i64 = 20_000;
const CHUNK: i64 = 4_000; // → 5 chunk files

struct DropPg(String);
impl Drop for DropPg {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

/// Order-independent content fingerprint. Every field is an unambiguous integer
/// in both Postgres and DuckDB (no cross-engine representation pitfalls):
/// counts, integer sums, a text-length sum, and two distinct counts. Together
/// they move under loss (counts/sums fall), duplication (counts/sums rise,
/// distinct < total), value corruption (a sum changes), and text
/// truncation/mangling (length sum or distinct-text changes).
#[derive(PartialEq, Eq, Debug)]
struct Fingerprint {
    rows: i64,
    sum_id: i64,
    sum_n: i64,
    sum_big: i64,
    sum_text_len: i64,
    distinct_id: i64,
    distinct_text: i64,
}

const FINGERPRINT_SQL: &str = "SELECT count(*)::BIGINT, \
     sum(id)::BIGINT, sum(n)::BIGINT, sum(big)::BIGINT, \
     sum(length(c_text))::BIGINT, \
     count(DISTINCT id)::BIGINT, count(DISTINCT c_text)::BIGINT";

fn seed_pg(table: &str) {
    seed_pg_n(table, N);
}

/// Seed `rows` differential rows (the parameterised core of [`seed_pg`], used by
/// the generative boundary harnesses). Distinct, varied values per row: id is the
/// dense key; n is bounded so its sum stays in i64; big spreads across the i64
/// range; c_text is unique per row (id + md5) so distinct(c_text) == rows catches
/// any text dup/corruption.
fn seed_pg_n(table: &str, rows: i64) {
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, n INT NOT NULL, big BIGINT NOT NULL, c_text TEXT NOT NULL)"
    ))
    .expect("create differential table");
    c.batch_execute(&format!(
        "INSERT INTO {table} (id, n, big, c_text) \
         SELECT g, ((g * 7) % 100000) - 50000, g::bigint * 1000003, \
                'row-' || g || '-' || md5(g::text) \
         FROM generate_series(0, {rows} - 1) g"
    ))
    .expect("seed differential rows");
}

fn pg_fingerprint(table: &str) -> Fingerprint {
    let mut c = pg_connect();
    let row = c
        .query_one(&format!("{FINGERPRINT_SQL} FROM {table}"), &[])
        .expect("pg fingerprint query");
    Fingerprint {
        rows: row.get(0),
        sum_id: row.get(1),
        sum_n: row.get(2),
        sum_big: row.get(3),
        sum_text_len: row.get(4),
        distinct_id: row.get(5),
        distinct_text: row.get(6),
    }
}

fn duckdb_fp_from(from_sql: &str) -> Fingerprint {
    let json = duckdb_run_sql_json(&format!("{FINGERPRINT_SQL} FROM {from_sql}"));
    let row = json["rows"][0]
        .as_array()
        .unwrap_or_else(|| panic!("duckdb fingerprint returned no row: {json}"));
    let cell = |i: usize| -> i64 {
        row[i]
            .as_str()
            .unwrap_or_else(|| panic!("duckdb cell {i} not a string: {json}"))
            .parse()
            .unwrap_or_else(|e| panic!("duckdb cell {i} parse: {e}"))
    };
    Fingerprint {
        rows: cell(0),
        sum_id: cell(1),
        sum_n: cell(2),
        sum_big: cell(3),
        sum_text_len: cell(4),
        distinct_id: cell(5),
        distinct_text: cell(6),
    }
}

/// Fingerprint the exported Parquet as-is (clean export).
fn duckdb_fingerprint(glob: &str) -> Fingerprint {
    duckdb_fp_from(&format!("read_parquet('{glob}')"))
}

/// Fingerprint exported CSV via DuckDB's `read_csv_auto` — a fully independent CSV
/// parser. If rivet mis-escapes a field (comma/quote/newline), DuckDB's parse of
/// the round-tripped value diverges from the source on the text-length or
/// distinct-text fields.
fn duckdb_csv_fingerprint(glob: &str) -> Fingerprint {
    duckdb_fp_from(&format!("read_csv_auto('{glob}', header=true)"))
}

/// Fingerprint the exported Parquet with at-least-once duplicate parts collapsed.
/// After a crash + resume the destination holds the re-run chunk twice; rivet's
/// duplicate parts are byte-identical re-exports, so `SELECT DISTINCT *` recovers
/// the exactly-once set — which must equal the source.
fn duckdb_fingerprint_dedup(glob: &str) -> Fingerprint {
    duckdb_fp_from(&format!("(SELECT DISTINCT * FROM read_parquet('{glob}'))"))
}

/// Raw physical row count over the whole glob (counts at-least-once duplicates).
fn duckdb_raw_rows(glob: &str) -> i64 {
    let json = duckdb_run_sql_json(&format!(
        "SELECT count(*)::BIGINT FROM read_parquet('{glob}')"
    ));
    json["rows"][0][0]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("duckdb raw count failed: {json}"))
}

#[test]
#[ignore = "live: requires docker compose postgres + rivet-duckdb"]
fn pg_chunked_export_matches_source_fingerprint_via_duckdb() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);

    let table = unique_name("rivet_diff");
    let _guard = DropPg(table.clone());
    seed_pg(&table);

    let export = unique_name("diff_exp");
    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("diff_out"));
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, n, big, c_text FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: {CHUNK}
    format: parquet
    compression: zstd
    destination: {{type: local, path: {dir}}}
"#,
        dir = host_dir.display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let run = run_rivet_export(&cfg, &export);
    assert!(
        run.status.success(),
        "differential export must succeed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    // Several chunk files must exist — this is the multi-part path, not one file.
    let files = files_with_extension(&host_dir, "parquet");
    assert!(
        files.len() >= 2,
        "chunked export must produce multiple parquet parts; got {files:?}"
    );
    make_shared_world_readable();

    let src = pg_fingerprint(&table);
    let dst = duckdb_fingerprint(&format!("{container_dir}/*.parquet"));

    // The whole point: a third-party reader's view of the destination must equal
    // the source's own view, field for field.
    assert_eq!(
        src,
        dst,
        "DIFFERENTIAL MISMATCH: DuckDB's fingerprint of the exported Parquet disagrees with the \
         source database — rivet lost, duplicated, or corrupted at least one row across {N} rows \
         / {} files. source={src:?} dest={dst:?}",
        files.len()
    );
    // Sanity: the fingerprint actually saw all N rows (guards a silently-empty
    // read that would make the equality trivially true).
    assert_eq!(src.rows, N, "source fingerprint must cover all {N} rows");
    assert_eq!(src.distinct_id, N, "id is the PK — distinct must equal N");
    assert_eq!(
        src.distinct_text, N,
        "c_text is unique per row by construction"
    );
}

/// Differential correctness AFTER A CRASH, via the independent reader.
///
/// `live_crash_soak.rs` proves no row is lost after a crash, but it re-reads the
/// destination with the in-process arrow/parquet-rs stack — the same library
/// family rivet writes with (independent of rivet's *bookkeeping*, but not of
/// the *codec*). This test closes that gap: it crashes a chunked export, resumes,
/// then fingerprints the recovered Parquet with **DuckDB** (a fully independent
/// reader) and compares to the source.
///
/// At-least-once means the crashed chunk is written twice, so the raw glob shows
/// a surplus; `SELECT DISTINCT *` collapses the byte-identical duplicate parts to
/// the exactly-once set, which must equal the source field-for-field.
#[test]
#[ignore = "live: requires docker compose postgres + rivet-duckdb"]
fn pg_chunked_export_after_crash_matches_source_via_duckdb() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::DuckDb);

    let table = unique_name("rivet_diff_crash");
    let _guard = DropPg(table.clone());
    seed_pg(&table);

    let export = unique_name("diffc_exp");
    let (host_dir, container_dir) = duckdb_shared_workdir(&unique_name("diffc_out"));
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, n, big, c_text FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: {CHUNK}
    chunk_checkpoint: true
    format: parquet
    compression: zstd
    destination: {{type: local, path: {dir}}}
"#,
        dir = host_dir.display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Crash mid-run, after chunk 1's file is written but before it commits →
    // chunk 1 re-runs on resume (at-least-once duplicate part).
    let crash = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
        ])
        .env("RIVET_TEST_PANIC_AT", "after_chunk_file:1")
        .output()
        .expect("spawn rivet");
    assert!(
        !crash.status.success(),
        "crash run must exit non-zero; stderr:\n{}",
        String::from_utf8_lossy(&crash.stderr)
    );
    std::thread::sleep(std::time::Duration::from_millis(1100));

    let resume = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
            "--resume",
        ])
        .output()
        .expect("spawn rivet resume");
    assert!(
        resume.status.success(),
        "--resume must succeed; stderr:\n{}",
        String::from_utf8_lossy(&resume.stderr)
    );
    make_shared_world_readable();

    let glob = format!("{container_dir}/*.parquet");
    let src = pg_fingerprint(&table);

    // At-least-once: the raw destination physically holds MORE than N rows
    // (chunk 1 written twice) — proves the duplicate part is really there.
    assert!(
        duckdb_raw_rows(&glob) > N,
        "after a crash the destination must carry the at-least-once duplicate part (>{N} raw rows)"
    );

    // Exactly-once view (duplicates collapsed) must equal the source — read by
    // DuckDB, a fully independent reader, AFTER the crash + resume.
    let dst = duckdb_fingerprint_dedup(&glob);
    assert_eq!(
        src, dst,
        "POST-CRASH DIFFERENTIAL MISMATCH: DuckDB's exactly-once view of the recovered Parquet \
         disagrees with the source — recovery lost or corrupted a row. source={src:?} dest={dst:?}"
    );
    assert_eq!(src.rows, N, "source must cover all {N} rows");
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    /// Generative completeness across chunk boundaries: for random (rows, chunk_size)
    /// the chunked export, read back by DuckDB, must match the source field-for-field.
    /// Exercises the off-by-one cases a fixed (N, CHUNK) misses — rows < chunk, rows
    /// == k·chunk (exact boundary), rows == k·chunk + 1, single-row exports, …
    #[test]
    #[ignore = "live+loaders: postgres + rivet-duckdb (nightly); reads via_duckdb"]
    fn batch_chunked_export_complete_across_boundaries_via_duckdb(
        rows in 1i64..400,
        chunk in 5i64..150,
    ) {
        require_alive(LiveService::Postgres);
        require_alive(LiveService::DuckDb);
        let table = unique_name("rivet_bp");
        let _g = DropPg(table.clone());
        seed_pg_n(&table, rows);

        let export = unique_name("bp_exp");
        let (host, container) = duckdb_shared_workdir(&unique_name("bp_out"));
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, n, big, c_text FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: {chunk}
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
            dir = host.display(),
        );
        let cfg = write_config(&cfg_dir, &yaml);
        let run = run_rivet_export(&cfg, &export);
        prop_assert!(
            run.status.success(),
            "rows={} chunk={}: export failed:\n{}",
            rows, chunk, String::from_utf8_lossy(&run.stderr)
        );
        make_shared_world_readable();

        let src = pg_fingerprint(&table);
        let dst = duckdb_fingerprint(&format!("{container}/*.parquet"));
        prop_assert_eq!(
            &src, &dst,
            "rows={} chunk={}: DuckDB's view disagrees with the source — a boundary lost or duped a row",
            rows, chunk
        );
        prop_assert_eq!(src.rows, rows, "rows={}: the fingerprint must cover every row", rows);
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 10, ..ProptestConfig::default() })]

    /// Generative at-least-once: crash a chunked export at a random chunk, resume, and
    /// the deduplicated DuckDB view must still equal the source — across random (rows,
    /// chunk count, crash point). `chunk` is derived from `n_chunks` so the export has
    /// ~`n_chunks` parts and the crash (`crash_at ∈ {1,2}`, with `n_chunks ≥ 3`) always
    /// lands inside the run.
    #[test]
    #[ignore = "live+loaders: postgres + rivet-duckdb (nightly); reads via_duckdb"]
    fn batch_chunked_crash_resume_complete_via_duckdb(
        rows in 80i64..320,
        n_chunks in 3i64..7,
        crash_at in 1usize..3,
    ) {
        require_alive(LiveService::Postgres);
        require_alive(LiveService::DuckDb);
        let chunk = (rows / n_chunks).max(2);
        let table = unique_name("rivet_bpc");
        let _g = DropPg(table.clone());
        seed_pg_n(&table, rows);

        let export = unique_name("bpc_exp");
        let (host, container) = duckdb_shared_workdir(&unique_name("bpc_out"));
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, n, big, c_text FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: {chunk}
    chunk_checkpoint: true
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
            dir = host.display(),
        );
        let cfg = write_config(&cfg_dir, &yaml);

        let crash = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap(), "--export", &export])
            .env("RIVET_TEST_PANIC_AT", format!("after_chunk_file:{crash_at}"))
            .output()
            .expect("spawn rivet");
        prop_assert!(
            !crash.status.success(),
            "rows={} chunk={} crash_at={}: the injected crash must fail the run",
            rows, chunk, crash_at
        );
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let resume = std::process::Command::new(RIVET_BIN)
            .args(["run", "--config", cfg.to_str().unwrap(), "--export", &export, "--resume"])
            .output()
            .expect("spawn rivet resume");
        prop_assert!(
            resume.status.success(),
            "rows={} chunk={} crash_at={}: resume failed:\n{}",
            rows, chunk, crash_at, String::from_utf8_lossy(&resume.stderr)
        );
        make_shared_world_readable();

        let src = pg_fingerprint(&table);
        let dst = duckdb_fingerprint_dedup(&format!("{container}/*.parquet"));
        prop_assert_eq!(
            &src, &dst,
            "rows={} chunk={} crash_at={}: post-crash exactly-once view disagrees with the source",
            rows, chunk, crash_at
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]

    /// CSV write-path completeness + escaping: a full CSV export, read back by
    /// DuckDB's independent CSV parser, must match the source. One row carries a
    /// comma and a double-quote, so a mis-escape (RFC-4180: quote the field, double
    /// the quote) diverges the text-length / distinct-text fingerprint fields.
    #[test]
    #[ignore = "live+loaders: postgres + rivet-duckdb (nightly); reads via_duckdb"]
    fn batch_csv_export_complete_and_escaped_via_duckdb(rows in 1i64..300) {
        require_alive(LiveService::Postgres);
        require_alive(LiveService::DuckDb);
        let table = unique_name("rivet_csv");
        let _g = DropPg(table.clone());
        seed_pg_n(&table, rows);
        // A row that stresses CSV escaping: c_text contains a comma and a quote.
        pg_connect()
            .execute(
                &format!(r#"INSERT INTO {table} VALUES ({rows}, 0, 0, 'a,b"c')"#),
                &[],
            )
            .expect("seed escaping row");

        let export = unique_name("csv_exp");
        let (host, container) = duckdb_shared_workdir(&unique_name("csv_out"));
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, n, big, c_text FROM {table}"
    mode: full
    format: csv
    destination: {{type: local, path: {dir}}}
"#,
            dir = host.display(),
        );
        let cfg = write_config(&cfg_dir, &yaml);
        let run = run_rivet_export(&cfg, &export);
        prop_assert!(
            run.status.success(),
            "rows={}: csv export failed:\n{}",
            rows, String::from_utf8_lossy(&run.stderr)
        );
        make_shared_world_readable();

        let src = pg_fingerprint(&table);
        let dst = duckdb_csv_fingerprint(&format!("{container}/*.csv"));
        prop_assert_eq!(
            &src, &dst,
            "rows={}: DuckDB's CSV view disagrees with the source — a row was lost, duped, or mis-escaped",
            rows
        );
    }
}

/// Multi-export isolation: one export failing at *runtime* must not poison the
/// others. The failing export is listed FIRST, so a fail-fast loop would abort
/// before the good export ever runs — this proves the run is continue-on-error
/// (run.rs collects each export's result and carries on). The contract:
///   - the good export completes fully — status:success, a `_SUCCESS` marker, all rows;
///   - the failed export RECORDS its failure — a status:failed manifest with no parts
///     and crucially NO `_SUCCESS` marker, so it's never mistaken for a clean export;
///   - the run still reports overall failure (non-zero exit).
#[test]
#[ignore = "live: requires docker compose postgres"]
fn multi_export_one_runtime_failure_does_not_poison_the_others() {
    require_alive(LiveService::Postgres);
    let table = unique_name("rivet_me");
    let _g = DropPg(table.clone());
    seed_pg_n(&table, 200);

    let cfg_dir = tempfile::tempdir().unwrap();
    let ok_out = cfg_dir.path().join("ok");
    let bad_out = cfg_dir.path().join("bad");
    std::fs::create_dir_all(&ok_out).unwrap();
    std::fs::create_dir_all(&bad_out).unwrap();
    // bad_exp FIRST: a valid config whose query fails at runtime (missing table).
    let yaml = format!(
        r#"source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: bad_exp
    query: "SELECT * FROM rivet_nonexistent_table_xyz"
    mode: full
    format: parquet
    destination: {{type: local, path: {bad}}}
  - name: ok_exp
    query: "SELECT id, n, big, c_text FROM {table}"
    mode: full
    format: parquet
    destination: {{type: local, path: {ok}}}
"#,
        bad = bad_out.display(),
        ok = ok_out.display(),
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let run = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet");

    // The run reports overall failure (bad_exp failed)…
    assert!(
        !run.status.success(),
        "the run must exit non-zero because bad_exp failed; stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    // …but ok_exp — listed AFTER the failure — still completed: a fail-fast loop
    // would have aborted before it ran.
    let manifest = ok_out.join("manifest.json");
    assert!(
        manifest.exists(),
        "ok_exp must complete despite the earlier bad_exp failing (continue-on-error isolation); stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let m: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
    assert_eq!(
        m["row_count"].as_i64().unwrap(),
        200,
        "ok_exp must export every row — its output must be whole, not truncated by the sibling failure"
    );
    assert_eq!(
        m["status"].as_str(),
        Some("success"),
        "ok_exp manifest must record status:success"
    );
    assert!(
        ok_out.join("_SUCCESS").exists(),
        "ok_exp must write its _SUCCESS marker (the clean-completion signal)"
    );

    // bad_exp RECORDS its failure (not silently absent) but is unmistakably not a
    // clean export: status:failed, zero rows, and NO _SUCCESS marker.
    let bad_manifest = bad_out.join("manifest.json");
    assert!(
        bad_manifest.exists(),
        "bad_exp must record its failure with a manifest, not vanish silently"
    );
    let bm: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&bad_manifest).unwrap()).unwrap();
    assert_eq!(
        bm["status"].as_str(),
        Some("failed"),
        "bad_exp manifest must record status:failed"
    );
    assert_eq!(
        bm["row_count"].as_i64(),
        Some(0),
        "a failed export writes no rows"
    );
    assert!(
        !bad_out.join("_SUCCESS").exists(),
        "bad_exp must NOT write a _SUCCESS marker — that's how downstream tells it apart from a clean export"
    );
}

// ---- Engine variant: the same differential, but a MySQL source -------------
// The batch write path is shared, so this exercises the MySQL source adapter's
// chunked read against the same independent-reader fingerprint.

struct DropMysql(String);
impl Drop for DropMysql {
    fn drop(&mut self) {
        if let Ok(p) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = p.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

/// MySQL twin of [`seed_pg_n`] — same (id, n, big, c_text) shape, distinct values,
/// c_text unique per row. Batched INSERTs (MySQL has no `generate_series`).
fn seed_mysql_n(table: &str, rows: i64) {
    let mut c = mysql::Pool::new(MYSQL_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn");
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, n INT NOT NULL, big BIGINT NOT NULL, c_text TEXT NOT NULL)"
    ))
    .unwrap();
    let mut g = 0i64;
    while g < rows {
        let end = (g + 1000).min(rows);
        let vals: Vec<String> = (g..end)
            .map(|i| {
                format!(
                    "({i}, {}, {}, 'row-{i}')",
                    ((i * 7) % 100000) - 50000,
                    i * 1000003
                )
            })
            .collect();
        c.query_drop(format!("INSERT INTO {table} VALUES {}", vals.join(", ")))
            .unwrap();
        g = end;
    }
}

/// MySQL twin of [`pg_fingerprint`]. `CAST(... AS SIGNED)` keeps every field an
/// unambiguous i64 (MySQL `sum()` over BIGINT otherwise widens to DECIMAL).
fn mysql_fingerprint(table: &str) -> Fingerprint {
    let mut c = mysql::Pool::new(MYSQL_URL)
        .expect("mysql pool")
        .get_conn()
        .expect("mysql conn");
    let row: mysql::Row = c
        .query_first(format!(
            "SELECT CAST(count(*) AS SIGNED), CAST(sum(id) AS SIGNED), CAST(sum(n) AS SIGNED), \
             CAST(sum(big) AS SIGNED), CAST(sum(length(c_text)) AS SIGNED), \
             CAST(count(DISTINCT id) AS SIGNED), CAST(count(DISTINCT c_text) AS SIGNED) FROM {table}"
        ))
        .expect("mysql fingerprint query")
        .expect("one row");
    Fingerprint {
        rows: row.get(0).unwrap(),
        sum_id: row.get(1).unwrap(),
        sum_n: row.get(2).unwrap(),
        sum_big: row.get(3).unwrap(),
        sum_text_len: row.get(4).unwrap(),
        distinct_id: row.get(5).unwrap(),
        distinct_text: row.get(6).unwrap(),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]

    /// Engine variant of the boundary-completeness harness: a MySQL source, chunked
    /// export, fingerprinted by DuckDB against the MySQL source across random
    /// (rows, chunk_size). Proves the MySQL adapter's chunked read loses nothing at
    /// the boundaries the same way the PostgreSQL path is proven.
    #[test]
    #[ignore = "live+loaders: mysql + rivet-duckdb (nightly); reads via_duckdb"]
    fn mysql_chunked_export_complete_across_boundaries_via_duckdb(
        rows in 1i64..400,
        chunk in 5i64..150,
    ) {
        require_alive(LiveService::Mysql);
        require_alive(LiveService::DuckDb);
        let table = unique_name("rivet_mybp");
        let _g = DropMysql(table.clone());
        seed_mysql_n(&table, rows);

        let export = unique_name("mybp_exp");
        let (host, container) = duckdb_shared_workdir(&unique_name("mybp_out"));
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"source: {{type: mysql, url: "{MYSQL_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, n, big, c_text FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: {chunk}
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
            dir = host.display(),
        );
        let cfg = write_config(&cfg_dir, &yaml);
        let run = run_rivet_export(&cfg, &export);
        prop_assert!(
            run.status.success(),
            "rows={} chunk={}: mysql export failed:\n{}",
            rows, chunk, String::from_utf8_lossy(&run.stderr)
        );
        make_shared_world_readable();

        let src = mysql_fingerprint(&table);
        let dst = duckdb_fingerprint(&format!("{container}/*.parquet"));
        prop_assert_eq!(
            &src, &dst,
            "rows={} chunk={}: DuckDB's view disagrees with the MySQL source — a boundary lost or duped a row",
            rows, chunk
        );
    }
}

// ---- Engine variant: a SQL Server source -----------------------------------

/// SQL Server twin of [`seed_pg_n`]. T-SQL caps a multi-row VALUES at 1000, so
/// chunk the INSERTs.
fn seed_mssql_n(table: &str, rows: i64) {
    mssql_drop_table(table);
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, n INT NOT NULL, big BIGINT NOT NULL, c_text NVARCHAR(100) NOT NULL)"
    ));
    let mut start = 0;
    while start < rows {
        let end = (start + 1000).min(rows);
        let mut sql = format!("INSERT INTO {table} (id, n, big, c_text) VALUES ");
        for i in start..end {
            if i > start {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "({i}, {}, {}, 'row-{i}')",
                ((i * 7) % 100000) - 50000,
                i * 1000003
            ));
        }
        mssql_exec(&sql);
        start = end;
    }
}

/// SQL Server twin of [`pg_fingerprint`]. Every field is `CAST(... AS BIGINT)` so it
/// reads back as i64 and `sum` can't overflow its source column type.
fn mssql_fingerprint(table: &str) -> Fingerprint {
    let v = mssql_query_bigints(
        &format!(
            "SELECT CAST(count(*) AS BIGINT), sum(CAST(id AS BIGINT)), sum(CAST(n AS BIGINT)), \
             sum(CAST(big AS BIGINT)), sum(CAST(LEN(c_text) AS BIGINT)), \
             CAST(count(DISTINCT id) AS BIGINT), CAST(count(DISTINCT c_text) AS BIGINT) FROM {table}"
        ),
        7,
    );
    Fingerprint {
        rows: v[0],
        sum_id: v[1],
        sum_n: v[2],
        sum_big: v[3],
        sum_text_len: v[4],
        distinct_id: v[5],
        distinct_text: v[6],
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 8, ..ProptestConfig::default() })]

    /// Engine variant: a SQL Server source. The same boundary-completeness proof as
    /// the PostgreSQL and MySQL paths — chunked export, DuckDB fingerprint vs the SQL
    /// Server source across random (rows, chunk_size). Fewer cases: the per-call
    /// tiberius runtime makes each fingerprint heavier.
    #[test]
    #[ignore = "live+loaders: mssql + rivet-duckdb (nightly); reads via_duckdb"]
    fn mssql_chunked_export_complete_across_boundaries_via_duckdb(
        rows in 1i64..400,
        chunk in 5i64..150,
    ) {
        require_alive(LiveService::Mssql);
        require_alive(LiveService::DuckDb);
        let table = unique_name("rivet_msbp");
        seed_mssql_n(&table, rows);
        let _g = MssqlTable::adopt(table.clone());

        let export = unique_name("msbp_exp");
        let (host, container) = duckdb_shared_workdir(&unique_name("msbp_out"));
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
exports:
  - name: {export}
    query: "SELECT id, n, big, c_text FROM {table}"
    mode: chunked
    chunk_column: id
    chunk_size: {chunk}
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
            dir = host.display(),
        );
        let cfg = write_config(&cfg_dir, &yaml);
        let run = run_rivet_export(&cfg, &export);
        prop_assert!(
            run.status.success(),
            "rows={} chunk={}: mssql export failed:\n{}",
            rows, chunk, String::from_utf8_lossy(&run.stderr)
        );
        make_shared_world_readable();

        let src = mssql_fingerprint(&table);
        let dst = duckdb_fingerprint(&format!("{container}/*.parquet"));
        prop_assert_eq!(
            &src, &dst,
            "rows={} chunk={}: DuckDB's view disagrees with the SQL Server source — a boundary lost or duped a row",
            rows, chunk
        );
    }
}
