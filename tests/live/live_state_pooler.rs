//! The run lease behind a transaction-mode pooler (pgBouncer in front of the STATE DB, a common
//! managed-Postgres setup). The lease is a session advisory lock; behind the pooler a second
//! rivet can be handed the very server connection that already holds it, and take it again.
//! Measured 2026-09-27: two concurrent runs of one export both ran (exit 0, 0), where the same
//! pair against the state DB directly refuses the second. Needs `--profile pool pgbouncer-state`.

use crate::common::*;

const BOUNCER_STATE_URL: &str = "postgresql://rivet:rivet@127.0.0.1:6433/rivet_state_bouncer";

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
    let run = |delay_ms: u64| {
        let cfg = cfg.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            run_rivet_env(
                &["run", "-c", cfg.to_str().unwrap()],
                &[("RIVET_STATE_URL", BOUNCER_STATE_URL)],
            )
        })
    };
    let (a, b) = (run(0), run(300));
    let (a, b) = (a.join().unwrap(), b.join().unwrap());
    assert!(
        !(a.status.success() && b.status.success()),
        "both concurrent runs proceeded through the pooler — the run lease did not hold:\nA: {}\nB: {}",
        String::from_utf8_lossy(&a.stderr)
            .lines()
            .last()
            .unwrap_or(""),
        String::from_utf8_lossy(&b.stderr)
            .lines()
            .last()
            .unwrap_or("")
    );
}
