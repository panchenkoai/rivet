//! #148 live proof: init's row figure must come from the SAMPLE, not the
//! catalog, when `TABLE_ROWS` lies — the field bite was a versioned table
//! under-counted ~2.5×, flipping every threshold decision on the first run.
//!
//! Fixture: stats FROZEN small (`STATS_AUTO_RECALC=0` + `ANALYZE` at 1 000
//! rows), then bulk-grown to 150 000 rows (3 rows per key value — the
//! versioned shape). `TABLE_ROWS` keeps claiming ~1 000; believing it scaffolds
//! `mode: full` (below the 100 K threshold). The probe must see ~150 K and
//! scaffold `mode: chunked` — the RED direction is exactly "trust the catalog".

use crate::common::*;
use mysql::prelude::Queryable;

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn init_probe_overrules_a_stale_table_rows_on_a_versioned_table() {
    let mut c = mysql_connect();
    c.query_drop("SET SESSION cte_max_recursion_depth = 60000")
        .unwrap();
    let name = unique_name("probe_versioned");
    // No PK: the chunk candidate is the versioned key itself (`ref_id`), whose
    // density (~3 rows per key unit) is what TABLE_ROWS structurally cannot see.
    c.query_drop(format!(
        "CREATE TABLE {name} (ref_id BIGINT NOT NULL, v INT NOT NULL, KEY(ref_id)) \
         STATS_AUTO_RECALC=0, STATS_PERSISTENT=1"
    ))
    .unwrap();
    let _guard = MysqlTable::adopt(name.clone());

    // Freeze the stats while the table is TINY.
    c.query_drop(format!(
        "INSERT INTO {name} (ref_id, v) \
         SELECT seq, 0 FROM (WITH RECURSIVE s(seq) AS \
           (SELECT 1 UNION ALL SELECT seq+1 FROM s WHERE seq < 1000) SELECT seq FROM s) t"
    ))
    .unwrap();
    c.query_drop(format!("ANALYZE TABLE {name}")).unwrap();

    // Grow to 150 000 rows = 50 000 keys × 3 versions, stats stay frozen.
    for round in 0..3 {
        c.query_drop(format!(
            "INSERT INTO {name} (ref_id, v) \
             SELECT seq, {round} FROM (WITH RECURSIVE s(seq) AS \
               (SELECT 1001 UNION ALL SELECT seq+1 FROM s WHERE seq < 50000) SELECT seq FROM s) t"
        ))
        .unwrap();
    }
    let true_rows: i64 = c
        .query_first(format!("SELECT COUNT(*) FROM {name}"))
        .unwrap()
        .unwrap();
    assert!(true_rows > 140_000, "fixture grew: {true_rows}");
    let catalog: Option<i64> = c
        .query_first(format!(
            "SELECT TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{name}'"
        ))
        .unwrap();
    let catalog = catalog.unwrap_or(0);
    // The fixture is only meaningful when the catalog genuinely LIES ≥2×.
    assert!(
        catalog * 2 < true_rows,
        "fixture inert: TABLE_ROWS {catalog} vs true {true_rows} — stats not frozen"
    );

    // rivet init on that table: the probe must overrule the catalog.
    let dir = tempfile::tempdir().unwrap();
    let out_yaml = dir.path().join("probe.yaml");
    let out = run_rivet_env(
        &[
            "init",
            "--source",
            MYSQL_URL,
            "--table",
            &name,
            "-o",
            out_yaml.to_str().unwrap(),
        ],
        &[],
    );
    assert!(
        out.status.success(),
        "init: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let yaml = std::fs::read_to_string(&out_yaml).unwrap();
    // Believing TABLE_ROWS (~1k) scaffolds `mode: full`; the probe (~150k > the
    // 100k threshold) must scaffold chunked. THE assertion of #148.
    assert!(
        yaml.contains("mode: chunked"),
        "probe must overrule the stale catalog (catalog={catalog}, true={true_rows}):\n{yaml}"
    );
}

/// #148 PG leg: a NEVER-ANALYZED table has reltuples -1 (clamped 0) — the gate
/// now probes it (a 0/unknown catalog on a table that may hold real rows is
/// exactly when the probe is needed). Fresh table, no ANALYZE, 60_000 rows:
/// `init` must probe and scaffold `mode: chunked`, not trust the 0 into `full`.
/// Live-covers the postgres.rs density_probe mutants the offline gate can't.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_density_probe_corrects_a_never_analyzed_table() {
    require_alive(LiveService::Postgres);
    let table = unique_name("pg_probe_fresh");
    let mut c = pg_connect();
    // Create + bulk-load but DELIBERATELY never ANALYZE → reltuples stays -1.
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 150000) g;"
    ))
    .unwrap();
    let reltuples: f32 = c
        .query_one(
            "SELECT reltuples FROM pg_class WHERE relname = $1",
            &[&table],
        )
        .unwrap()
        .get(0);
    assert!(
        reltuples <= 0.0,
        "fixture must be un-analyzed (reltuples {reltuples} should be -1/0)"
    );

    let dir = tempfile::tempdir().unwrap();
    let out_yaml = dir.path().join("probe.yaml");
    let out = run_rivet_env(
        &[
            "init",
            "--source",
            POSTGRES_URL,
            "--table",
            &format!("public.{table}"),
            "-o",
            out_yaml.to_str().unwrap(),
        ],
        &[],
    );
    assert!(
        out.status.success(),
        "init: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let yaml = std::fs::read_to_string(&out_yaml).unwrap();
    assert!(
        yaml.contains("mode: chunked"),
        "probe must overrule reltuples=-1 on a never-analyzed 150k table:\n{yaml}"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}

/// #148 (roast 2026-08-10, gate exit 101): the MySQL density probe read
/// MIN/MAX as (Option<i64>, Option<i64>); a BIGINT UNSIGNED key past i64::MAX
/// made the mysql crate's FromRow PANIC (not a catchable Err), crashing
/// `rivet init`. A table whose id reaches u64::MAX must init CLEANLY (the probe
/// skips it as unverified — i64 offsets cannot span it). RED against the i64 read.
#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn init_does_not_panic_on_a_bigint_unsigned_key_past_i64() {
    let mut c = mysql_connect();
    let name = unique_name("ubig_key");
    c.query_drop(format!(
        "CREATE TABLE {name} (id BIGINT UNSIGNED PRIMARY KEY, v INT NOT NULL)"
    ))
    .unwrap();
    let _guard = MysqlTable::adopt(name.clone());
    // Rows straddling i64::MAX so MIN fits i64 but MAX (u64::MAX) does not.
    c.query_drop(format!(
        "INSERT INTO {name} (id, v) VALUES (1, 1), (18446744073709551615, 2)"
    ))
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let out_yaml = dir.path().join("u.yaml");
    let out = run_rivet_env(
        &[
            "init",
            "--source",
            MYSQL_URL,
            "--table",
            &name,
            "-o",
            out_yaml.to_str().unwrap(),
        ],
        &[],
    );
    assert!(
        out.status.success(),
        "init must NOT panic on a BIGINT UNSIGNED key past i64 (exit {:?}):\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out_yaml.exists(), "init must still scaffold a config");
}
