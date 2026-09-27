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
    let env = [("RIVET_STATE_URL", POSTGRES_STATE_TOXI_URL)];
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
