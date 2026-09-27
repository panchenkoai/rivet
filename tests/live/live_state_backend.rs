//! The Postgres STATE backend as production meets it: the ledger connection lost in the
//! middle of an export. The gate had covered the ledger dying under `rivet load`, never
//! under `rivet run`, where every committed page writes state.
//!
//! Oracles: DuckDB over the parts the manifests declare, the source's own count.

use crate::common::*;

const ROWS: i64 = 20_000;

/// Whether `dir` holds at least one parquet part yet.
fn has_a_part(dir: &std::path::Path) -> bool {
    walk_files(dir)
        .iter()
        .any(|p| p.extension().is_some_and(|e| e == "parquet"))
}

/// Every file under `dir`, recursively (empty when it does not exist).
fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    rd.flatten()
        .flat_map(|e| {
            let p = e.path();
            if p.is_dir() { walk_files(&p) } else { vec![p] }
        })
        .collect()
}

/// The state ledger lost after the first part of a keyset run: the run fails loudly and
/// ships no success marker; with the ledger back, the next run delivers every row once.
/// RED against state writes made best-effort (a swallowed Postgres error): that run reports
/// success over a dead ledger. Ignoring only the per-page cursor write stays green — the
/// finalize-time writes still fail the run — so no single write is load-bearing here.
#[test]
#[ignore = "live: requires postgres + postgres-state + toxiproxy"]
fn a_state_ledger_lost_mid_run_fails_loudly_and_the_next_run_delivers_every_row_once() {
    let _lock = toxiproxy_guard();
    ensure_toxi_proxy("postgres_state", 15433, "postgres-state:5432");
    toxi_reset_toxics("postgres_state");
    toxi_enable("postgres_state");
    require_alive(LiveService::PostgresStateToxi);
    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, _guard) = pg.create("state_cut", "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, {ROWS}) g"
    ));
    let rig = pg
        .rig(&t)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 500");
    let toxi_url = postgres_state_toxi_url();
    let env = [("RIVET_STATE_URL", toxi_url.as_str())];
    // Slow every ledger round-trip so the run is still paging when the first part lands.
    toxi_add_latency("postgres_state", 40);
    let out_dir = rig.out_dir();
    let killer = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !has_a_part(&out_dir) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        toxi_disable("postgres_state");
    });
    let crashed = rig.run_args_env(&[], &env);
    killer.join().expect("the killer thread");
    toxi_enable("postgres_state");
    toxi_reset_toxics("postgres_state");
    let said = String::from_utf8_lossy(&crashed.stderr).to_string();
    assert!(
        !crashed.status.success(),
        "a run whose ledger died must fail:\n{said}"
    );
    assert!(
        !rig.out_dir().join("_SUCCESS").exists(),
        "no success marker for a run whose ledger died"
    );
    let rerun = rig.run_args_env(&[], &env);
    assert!(
        rerun.status.success(),
        "with the ledger back the next run finishes:\n{}",
        String::from_utf8_lossy(&rerun.stderr)
    );
    let ids = dir_manifest_copy_id_set(&rig.out_dir());
    let declared_rows = duckdb_declared_dir_scalar(&rig.out_dir(), "count(*)");
    assert_eq!(
        (ids.len() as i64, declared_rows),
        (ROWS, ROWS),
        "every row delivered, each once"
    );
}

// ─── Block C: the Postgres state as production deploys it ────────────────────────

const STATE_ADMIN: &str = "host=127.0.0.1 port=5433 user=rivet password=rivet dbname=postgres";

/// A fresh database on the state server, dropped (with anything it holds) when the guard goes.
struct ScratchStateDb {
    name: String,
}

impl ScratchStateDb {
    fn new(tag: &str) -> Self {
        let name = unique_name(tag);
        let mut c = postgres::Client::connect(STATE_ADMIN, postgres::NoTls).expect("state server");
        c.batch_execute(&format!("CREATE DATABASE {name}")).unwrap();
        Self { name }
    }
    fn url(&self) -> String {
        format!("postgresql://rivet:rivet@127.0.0.1:5433/{}", self.name)
    }
    fn client(&self) -> postgres::Client {
        postgres::Client::connect(&self.url(), postgres::NoTls).expect("scratch state DB")
    }
}

impl Drop for ScratchStateDb {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(STATE_ADMIN, postgres::NoTls) {
            let _ = c.batch_execute(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                self.name
            ));
        }
    }
}

/// A keyset-checkpointed PostgreSQL export of `rows` fresh rows (it takes the run lease).
fn checkpointed(prefix: &str, rows: i64) -> (Rig, Box<dyn std::any::Any>) {
    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, guard) = pg.create(prefix, "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, {rows}) g"
    ));
    let rig = pg
        .rig(&t)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 500");
    (rig, guard)
}

/// A least-privilege role whose only rights are its own schema, reached through
/// `options=-c search_path=…`: every state table lands in that schema and none in `public`.
#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn a_least_privilege_role_keeps_the_whole_state_in_its_own_schema() {
    let db = ScratchStateDb::new("st_lp");
    let role = unique_name("st_lp_role");
    let mut c = db.client();
    c.batch_execute(&format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'lp';
         REVOKE ALL ON SCHEMA public FROM PUBLIC;
         REVOKE ALL ON DATABASE {name} FROM PUBLIC;
         GRANT CONNECT ON DATABASE {name} TO {role};
         CREATE SCHEMA rivet_state AUTHORIZATION {role};",
        name = db.name
    ))
    .unwrap();
    let url = format!(
        "postgresql://{role}:lp@127.0.0.1:5433/{}?options=-c%20search_path%3Drivet_state",
        db.name
    );
    let (rig, _t) = checkpointed("st_lp", 2000);
    let out = rig.run_args_env(&[], &[("RIVET_STATE_URL", url.as_str())]);
    let said = String::from_utf8_lossy(&out.stderr).to_string();
    let mut count = |schema: &str| -> i64 {
        c.query_one(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1",
            &[&schema],
        )
        .unwrap()
        .get(0)
    };
    let (own, public) = (count("rivet_state"), count("public"));
    drop(c);
    if let Ok(mut a) = postgres::Client::connect(STATE_ADMIN, postgres::NoTls) {
        drop(db);
        let _ = a.batch_execute(&format!("DROP ROLE IF EXISTS {role}"));
    }
    assert!(
        out.status.success(),
        "the run on a least-privilege state:\n{said}"
    );
    assert!(
        own > 5 && public == 0,
        "state tables: {own} in rivet_state, {public} in public"
    );
}

/// Thirty-two runs of different exports at once on one Postgres state: every one finishes
/// and records its success — none is turned away for connections or starved by a lease.
#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn thirty_two_concurrent_runs_on_one_postgres_state_all_finish() {
    let db = ScratchStateDb::new("st_32");
    let url = db.url();
    let rigs: Vec<_> = (0..32)
        .map(|i| checkpointed(&format!("st32_{i}"), 1000))
        .collect();
    let kids: Vec<_> = rigs
        .iter()
        .map(|(rig, _)| rig.spawn_args_env(&[], &[("RIVET_STATE_URL", url.as_str())]))
        .collect();
    let failed: Vec<String> = kids
        .into_iter()
        .map(|k| k.wait_with_output().unwrap())
        .filter(|o| !o.status.success())
        .map(|o| {
            String::from_utf8_lossy(&o.stderr)
                .lines()
                .last()
                .unwrap_or("")
                .to_string()
        })
        .collect();
    assert!(
        failed.is_empty(),
        "{} of 32 runs failed: {failed:?}",
        failed.len()
    );
    let done: i64 = db
        .client()
        .query_one(
            "SELECT count(*) FROM run_status WHERE status = 'success'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(
        done, 32,
        "every run's success recorded in the shared ledger"
    );
}

/// A state database whose default TimeZone is not UTC: an incremental cursor continues, and
/// a second run finds the first's lease released (its expiry is server time either way).
/// Pins behaviour: the state stores cursors as text and pins UTC on connect, so no mutant
/// of today's code turns this RED; it guards the day something reads the session zone.
#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn a_state_db_whose_timezone_is_not_utc_continues_the_cursor() {
    let db = ScratchStateDb::new("st_tz");
    db.client()
        .batch_execute(&format!(
            "ALTER DATABASE {} SET timezone TO 'Asia/Tokyo'",
            db.name
        ))
        .unwrap();
    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, _g) = pg.create(
        "st_tz",
        "id BIGINT PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL",
    );
    pg.exec(&format!(
        "INSERT INTO {t} SELECT g, TIMESTAMPTZ '2026-01-01 00:00:00+00' + g * INTERVAL '1 minute' \
         FROM generate_series(1, 500) g"
    ));
    let rig = pg
        .rig(&t)
        .mode("incremental")
        .export_line("cursor_column: updated_at");
    let url = db.url();
    let env = [("RIVET_STATE_URL", url.as_str())];
    let first = rig.run_args_env(&[], &env);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    pg.exec(&format!(
        "INSERT INTO {t} SELECT g, TIMESTAMPTZ '2026-02-01 00:00:00+00' + g * INTERVAL '1 minute' \
         FROM generate_series(501, 600) g"
    ));
    let second = rig.run_args_env(&[], &env);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        (
            dir_manifest_copy_id_set(&rig.out_dir()).len(),
            duckdb_declared_dir_scalar(&rig.out_dir(), "count(*)")
        ),
        (600, 600),
        "every id once across both runs: none re-read, none skipped"
    );
}

/// A `running` row left by a crashed host whose clock ran an hour ahead: the next successful
/// run of the same export on the same prefix supersedes it, so nothing reads the prefix as
/// still being written for the hour the skew lasts.
#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn a_crashed_run_from_a_fast_clock_host_is_superseded_by_the_next_run() {
    let db = ScratchStateDb::new("st_skew");
    let url = db.url();
    let (rig, _t) = checkpointed("st_skew", 1000);
    let env = [("RIVET_STATE_URL", url.as_str())];
    assert!(
        rig.run_args_env(&[], &env).status.success(),
        "fixture: the first run"
    );
    let mut c = db.client();
    let (export, prefix): (String, String) = {
        let r = c
            .query_one("SELECT export_name, prefix FROM run_status LIMIT 1", &[])
            .unwrap();
        (r.get(0), r.get(1))
    };
    let ahead = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
    c.execute(
        "INSERT INTO run_status (run_id, export_name, prefix, status, started_at) \
         VALUES ('skewed-crash', $1, $2, 'running', $3)",
        &[&export, &prefix, &ahead],
    )
    .unwrap();
    assert!(rig.run_args_env(&[], &env).status.success(), "the next run");
    let st = rivet::state::StateStore::open_at_ref(&rivet::state::StateRef::Postgres(url.clone()))
        .unwrap();
    assert!(
        !st.has_active_run_on_prefix(&prefix).unwrap(),
        "a crashed run stamped by a clock an hour ahead still reads as a live writer on \
         {prefix} after the next run succeeded"
    );
}

/// Moving an export from a SQLite state to a Postgres one: the first run on the empty
/// Postgres state loses no row of the prefix the SQLite runs filled.
/// Pins behaviour (there is no migration path to mutate): switching backends starts empty.
#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn moving_from_a_sqlite_state_to_a_postgres_state_loses_no_row() {
    let db = ScratchStateDb::new("st_move");
    let (rig, _t) = checkpointed("st_move", 3000);
    assert!(
        rig.run_args_env(&[], &[("RIVET_STATE_URL", "")])
            .status
            .success(),
        "fixture: the SQLite run"
    );
    let url = db.url();
    let moved = rig.run_args_env(&[], &[("RIVET_STATE_URL", url.as_str())]);
    assert!(
        moved.status.success(),
        "{}",
        String::from_utf8_lossy(&moved.stderr)
    );
    assert_eq!(
        dir_manifest_copy_id_set(&rig.out_dir()).len(),
        3000,
        "every row still declared after the move"
    );
}

/// A ledger holding 100,000 prior runs: a run on it takes no more than three times as long
/// as on an empty one (plus two seconds) — no state query scans the history per page.
/// Pins behaviour: measured 2026-09-27 well inside the bound; a per-page history scan is
/// what it would catch, and none exists to mutate.
#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn a_run_on_a_hundred_thousand_run_ledger_is_not_slowed_by_the_history() {
    let db = ScratchStateDb::new("st_big");
    let url = db.url();
    let (rig, _t) = checkpointed("st_big", 5000);
    let env = [("RIVET_STATE_URL", url.as_str())];
    let t0 = std::time::Instant::now();
    assert!(
        rig.run_args_env(&[], &env).status.success(),
        "fixture: the empty-ledger run"
    );
    let empty = t0.elapsed();
    let mut c = db.client();
    let export: String = c
        .query_one("SELECT export_name FROM run_status LIMIT 1", &[])
        .unwrap()
        .get(0);
    c.batch_execute(&format!(
        "INSERT INTO run_status (run_id, export_name, prefix, status, started_at, finished_at)
           SELECT 'hist-' || g, '{export}', 'file:///hist/' || g, 'success',
                  '2020-01-01T00:00:00Z', '2020-01-01T00:00:01Z' FROM generate_series(1, 100000) g;
         INSERT INTO export_metrics (export_name, run_id, run_at, duration_ms, status, total_rows)
           SELECT '{export}', 'hist-' || g, '2020-01-01T00:00:00Z', 1000, 'success', 1
           FROM generate_series(1, 100000) g;"
    ))
    .unwrap();
    let t1 = std::time::Instant::now();
    let out = rig.run_args_env(&[], &env);
    let loaded = t1.elapsed();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        loaded <= empty * 3 + std::time::Duration::from_secs(2),
        "a run on a 100k-run ledger took {loaded:?} against {empty:?} on an empty one"
    );
}
