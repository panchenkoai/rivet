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
