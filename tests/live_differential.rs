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
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, n INT NOT NULL, big BIGINT NOT NULL, c_text TEXT NOT NULL)"
    ))
    .expect("create differential table");
    // Distinct, varied values per row: id is the dense key; n is bounded so its
    // sum stays in i64; big spreads across the i64 range; c_text is unique per
    // row (id + md5) so distinct(c_text) == N catches any text dup/corruption.
    c.batch_execute(&format!(
        "INSERT INTO {table} (id, n, big, c_text) \
         SELECT g, ((g * 7) % 100000) - 50000, g::bigint * 1000003, \
                'row-' || g || '-' || md5(g::text) \
         FROM generate_series(0, {N} - 1) g"
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

fn duckdb_fingerprint(glob: &str) -> Fingerprint {
    let json = duckdb_run_sql_json(&format!("{FINGERPRINT_SQL} FROM read_parquet('{glob}')"));
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
