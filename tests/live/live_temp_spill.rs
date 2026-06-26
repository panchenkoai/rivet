//! Live regression: chunked min/max/COUNT do not materialise a wrapped subquery.
//!
//! Lives in its own test binary so other live-test files in the suite don't
//! pollute `pg_stat_database.temp_bytes` during the measurement window — cargo
//! runs each `tests/*.rs` file in its own process, but tests *within* a file
//! run in parallel by default and other wide-table fixtures we seed would
//! double-count temp spill into our delta.
//!
//! Requires `docker compose up -d` (Postgres).

use crate::common::*;

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_min_max_count_do_not_materialise_wrapped_subquery() {
    require_alive(LiveService::Postgres);

    // Wide-row fixture (~600-char payload × 30k rows ≈ 18 MB heap). Before the
    // fast-path patch, three `SELECT <agg> FROM (SELECT * FROM <tbl>) AS _rivet`
    // probes (min, max, COUNT) each materialised the full inner result —
    // three table-size temp writes per export, scaling linearly with the table
    // (3.2 GB on content_items).
    //
    // The patched path emits `SELECT min(id) / max(id) / COUNT(*) FROM <tbl>`
    // directly. PG satisfies them with index-only / heap scans and writes no
    // temp file at all. Remaining spill is per-FETCH cursor buffering — PG's
    // own behaviour with `DECLARE … CURSOR FOR SELECT *` on wide rows, ~3 MB
    // per chunk × N chunks — and is independent of this patch.
    //
    // Budget: 1.5 × heap_bytes. A pre-patch min/max wrap alone would already
    // breach it on this fixture, so the assertion cleanly fails if the wrap
    // returns.
    let tbl = seed_pg_wide_table(30_000, 600);
    let mut c = pg_connect();
    c.batch_execute(&format!("ANALYZE {}", tbl.name()))
        .expect("analyze");

    let heap_bytes: i64 = c
        .query_one(
            &format!("SELECT pg_relation_size('{}')::bigint", tbl.name()),
            &[],
        )
        .expect("relation size")
        .get(0);

    let temp_bytes_before: i64 = c
        .query_one(
            "SELECT temp_bytes::bigint FROM pg_stat_database WHERE datname = current_database()",
            &[],
        )
        .expect("temp_bytes pre")
        .get(0);

    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: no_wrap_spill
    table: public.{name}
    mode: chunked
    chunk_size: 5000
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "chunked run must succeed:\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let temp_bytes_after: i64 = c
        .query_one(
            "SELECT temp_bytes::bigint FROM pg_stat_database WHERE datname = current_database()",
            &[],
        )
        .expect("temp_bytes post")
        .get(0);
    let delta = temp_bytes_after.saturating_sub(temp_bytes_before);
    let budget = heap_bytes + heap_bytes / 2; // 1.5 × heap
    assert!(
        delta < budget,
        "chunked min/max/COUNT wrap regressed: temp_bytes delta = {delta} \
         vs budget {budget} (1.5 × heap_bytes {heap_bytes}). \
         If this fails, check that \
         `crate::pipeline::chunked::strip_select_star_from` still gates the wrap."
    );
}
