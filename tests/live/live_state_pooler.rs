//! The run lease behind a transaction-mode pooler (pgBouncer in front of the STATE DB, a common
//! managed-Postgres setup). The lease is a session advisory lock; behind the pooler a second
//! rivet can be handed the very server connection that already holds it, and take it again.
//! Measured 2026-09-27: two concurrent runs of one export both ran (exit 0, 0), where the same
//! pair against the state DB directly refuses the second. Needs `--profile pool pgbouncer-state`.

use crate::common::*;

/// The `RIVET_STATE_URL` that reaches `db` (a database on the state server) through the pooler.
fn through_the_pooler(db: &ScratchStateDb) -> String {
    format!("postgresql://rivet:rivet@127.0.0.1:6433/{}", db.name)
}

/// Two runs of one export started together through the pooler: at most one may proceed.
#[test]
#[ignore = "live: requires postgres + postgres-state + pgbouncer-state (profile pool)"]
fn two_runs_of_one_export_through_a_transaction_pooler_never_both_proceed() {
    if std::net::TcpStream::connect("127.0.0.1:6433").is_err() {
        skip_live(
            "pgbouncer-state (:6433) is down — docker compose --profile pool up -d pgbouncer-state",
        );
        return;
    }
    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, _guard) = pg.create("pooler_lease", "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, 400000) g"
    ));
    let rig = pg
        .rig(&t)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 50000");
    let cfg = rig.config_path();
    let db = ScratchStateDb::new("pooler_lease");
    let url = through_the_pooler(&db);
    let run = |delay_ms: u64| {
        let (cfg, url) = (cfg.clone(), url.clone());
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            run_rivet_env(
                &["run", "-c", cfg.to_str().unwrap()],
                &[("RIVET_STATE_URL", url.as_str())],
            )
        })
    };
    let (a, b) = (run(0), run(300));
    let (a, b) = (a.join().unwrap(), b.join().unwrap());
    let said = |o: &std::process::Output| String::from_utf8_lossy(&o.stderr).to_string();
    let refused = |o: &std::process::Output| {
        !o.status.success() && said(o).contains("still in progress in another live rivet process")
    };
    // Exactly one proceeds and the other is refused BY THE LEASE — a run failing for any
    // other reason (a state DB it cannot open, a missing table) must not count as the lease
    // holding.
    assert!(
        (a.status.success() && refused(&b)) || (b.status.success() && refused(&a)),
        "one run must proceed and the other be refused by the run lease:\nA ({}): {}\nB ({}): {}",
        a.status,
        said(&a).lines().last().unwrap_or(""),
        b.status,
        said(&b).lines().last().unwrap_or("")
    );
}

/// A run SIGKILLed mid-export leaves its lease row (no Drop runs); the next run on this host
/// takes it over at once — its holder's pid is gone — well inside a TTL long enough to rule
/// out expiry.
#[test]
#[ignore = "live: requires postgres + postgres-state + pgbouncer-state (profile pool)"]
fn a_run_killed_on_this_host_does_not_block_the_next_one() {
    if std::net::TcpStream::connect("127.0.0.1:6433").is_err() {
        skip_live(
            "pgbouncer-state (:6433) is down — docker compose --profile pool up -d pgbouncer-state",
        );
        return;
    }
    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, _guard) = pg.create("pooler_kill", "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, 400000) g"
    ));
    let rig = pg
        .rig(&t)
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("chunk_checkpoint: true")
        .export_line("chunk_size: 5000");
    let db = ScratchStateDb::new("pooler_kill");
    let url = through_the_pooler(&db);
    let env = [
        ("RIVET_STATE_URL", url.as_str()),
        ("RIVET_STATE_LEASE_TTL_S", "600"),
    ];
    let mut first = rig.spawn_args_env(&[], &env);
    let t0 = std::time::Instant::now();
    let has_part = |d: &std::path::Path| {
        std::fs::read_dir(d)
            .map(|rd| {
                rd.flatten()
                    .any(|e| e.path().extension().is_some_and(|x| x == "parquet"))
            })
            .unwrap_or(false)
    };
    while !has_part(&rig.out_dir()) {
        assert!(
            first.try_wait().unwrap().is_none(),
            "fixture: the first run finished before it could be killed"
        );
        assert!(
            t0.elapsed().as_secs() < 60,
            "fixture: no part was ever written"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    first.kill().expect("SIGKILL the first run");
    let _ = first.wait();
    let started = std::time::Instant::now();
    let next = rig.run_args_env(&[], &env);
    assert!(
        next.status.success(),
        "the run after a kill on this host must proceed, not wait out a 600 s lease:\n{}",
        String::from_utf8_lossy(&next.stderr)
    );
    assert!(
        started.elapsed().as_secs() < 120,
        "it waited {:?}",
        started.elapsed()
    );
    assert_eq!(
        duckdb_declared_dir_scalar(&rig.out_dir(), "count(DISTINCT id)"),
        400000,
        "the recovered run delivers every row"
    );
}

/// A dropped lease is free at once — even to the SAME live process, which a session advisory
/// lock would have let in twice while held (it is re-entrant) and a TTL would have kept out.
#[test]
#[ignore = "live: requires postgres-state"]
fn a_lease_is_refused_while_held_and_free_the_moment_it_is_dropped() {
    // Its own database on the state server, so the test needs nothing a stand was given by hand.
    let admin = "host=127.0.0.1 port=5433 user=rivet password=rivet dbname=postgres";
    let db = unique_name("lease_drop");
    postgres::Client::connect(admin, postgres::NoTls)
        .expect("the state server")
        .batch_execute(&format!("CREATE DATABASE {db}"))
        .unwrap();
    let url = format!("postgresql://rivet:rivet@127.0.0.1:5433/{db}");
    let st = rivet::state::StateStore::open_at_ref(&rivet::state::StateRef::Postgres(url))
        .expect("the scratch state DB");
    let key = unique_name("lease_release");
    let held = st.try_load_lease(&key).unwrap().expect("the first take");
    assert!(
        st.try_load_lease(&key).unwrap().is_none(),
        "held: a second take, even by this process, is refused"
    );
    drop(held);
    let free = st.try_load_lease(&key).unwrap().is_some();
    drop(st);
    let _ = postgres::Client::connect(admin, postgres::NoTls)
        .map(|mut c| c.batch_execute(&format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)")));
    assert!(free, "dropped: free at once, not after the TTL");
}
