//! Two configs whose exports share a NAME on ONE shared state DB — the shape the
//! release gate's parallel engine matrix has (every engine's `users` into one
//! Postgres state). `export_load_spec` is keyed by export name, last writer wins,
//! so B's run retypes A's table between A's run and A's load: a `_id` key on a
//! table that has `id`. The load must type each table from the spec of the run
//! it consumes (`export_load_spec_run`), never from the by-name row.
//!
//! Oracles: the load's exit, and BigQuery's own column catalog for the table
//! (INFORMATION_SCHEMA) — never rivet's summary. RED against `pin_plan_to_its_run`
//! stubbed to `Ok(plan.clone())`, MEASURED: the load exits 0 and the catalog shows
//! `["_id", "w"]` — the postgres export's DDL on the mysql table. The exit code
//! alone would have called that green; the catalog is the oracle.
//!
//! Needs mysql + postgres + BigQuery creds + `RIVET_TEST_STATE_URL` (a Postgres
//! state DB — the race needs a SHARED state); SKIPS without them.

use crate::common::*;

#[test]
#[ignore = "live: requires mysql + postgres + BigQuery creds + RIVET_TEST_STATE_URL (postgres)"]
fn a_load_is_typed_from_its_own_run_not_from_the_last_writer_of_its_name() {
    let Some(bq) = BqLive::from_env("pin") else {
        return;
    };
    let Ok(state_url) = std::env::var("RIVET_TEST_STATE_URL") else {
        return;
    };
    if !state_url.starts_with("postgres") {
        return;
    }
    let my = SqlEngine::Mysql;
    let pg = SqlEngine::Pg;
    my.alive();
    pg.alive();
    let (t_my, _g_my) = my.create("pin_my", "id BIGINT PRIMARY KEY, v INT");
    let (t_pg, _g_pg) = pg.create("pin_pg", "_id BIGINT PRIMARY KEY, w INT");
    my.exec(&format!(
        "INSERT INTO {t_my} (id, v) VALUES (1, 1), (2, 2), (3, 3)"
    ));
    pg.exec(&format!(
        "INSERT INTO {t_pg} (_id, w) VALUES (1, 1), (2, 2), (3, 3)"
    ));
    let _cleanup = bq.cleanup(&[&t_my, &t_pg]);
    let env = [("RIVET_STATE_URL", state_url.as_str())];

    // Both exports are called `users` — the collision is the point.
    let rig_my = my
        .rig(&t_my)
        .export_named("users")
        .dest_gcs_live(&bq.bucket, &format!("{}my/", bq.prefix))
        .top_line(&bq.load_line(""));
    let rig_pg = pg
        .rig(&t_pg)
        .export_named("users")
        .dest_gcs_live(&bq.bucket, &format!("{}pg/", bq.prefix))
        .top_line(&bq.load_line(""));

    // A runs, then B runs: B's spec is now the by-name row for `users`.
    for (label, rig) in [("mysql", &rig_my), ("postgres", &rig_pg)] {
        let out = rig.run_args_env(&[], &env);
        assert!(
            out.status.success(),
            "{label} run must succeed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // A loads: typed from A's OWN run — `id`, not B's `_id`.
    let out = rig_my.cli_env(&["load"], &env);
    assert!(
        out.status.success(),
        "the mysql load must be typed from its own run's spec, not from the postgres \
         export that last wrote the by-name row `users`:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let n: i64 = bq.read_bq_count(&t_my).parse().expect("a count");
    assert_eq!(n, 3, "all three mysql rows landed");
    let cols: Vec<String> = bq
        .read_bq_rows(&format!(
            "SELECT column_name FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{t_my}' \
             ORDER BY ordinal_position",
            bq.project, bq.dataset
        ))
        .iter()
        .map(|r| r["column_name"].as_str().expect("name").to_string())
        .collect();
    assert!(
        cols.iter().any(|c| c == "id")
            && cols.iter().any(|c| c == "v")
            && !cols.iter().any(|c| c == "_id"),
        "BigQuery's catalog must show the mysql table's own columns, got {cols:?}"
    );
}
