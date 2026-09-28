//! A shared Postgres state ranks runs by the state SERVER's clock, never a writer's.

use crate::common::*;

/// A crashed run from a host whose clock ran an hour ahead is superseded by the next run:
/// the fast host writes its own future start through `begin_run`, and on a Postgres state
/// the server's stamp replaces it, so nothing reads the prefix as still being written.
#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn a_crashed_run_from_a_fast_clock_host_is_superseded_by_the_next_run() {
    let db = ScratchStateDb::new("st_skew");
    let url = db.url();
    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, _g) = pg.create("st_skew", "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, 1000) g"
    ));
    let rig = pg
        .rig(&t)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 500");
    let env = [("RIVET_STATE_URL", url.as_str())];
    assert!(
        rig.run_args_env(&[], &env).status.success(),
        "fixture: the first run"
    );
    let (export, prefix): (String, String) = {
        let r = db
            .client()
            .query_one("SELECT export_name, prefix FROM run_status LIMIT 1", &[])
            .unwrap();
        (r.get(0), r.get(1))
    };
    let st = rivet::state::StateStore::open_at_ref(&rivet::state::StateRef::Postgres(url.clone()))
        .unwrap();
    let ahead = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    st.begin_run("skewed-crash", &export, &prefix, &ahead)
        .unwrap();
    assert!(rig.run_args_env(&[], &env).status.success(), "the next run");
    assert!(
        !st.has_active_run_on_prefix(&prefix).unwrap(),
        "a crashed run stamped by a clock an hour ahead still reads as a live writer on \
         {prefix} after the next run succeeded"
    );
}
