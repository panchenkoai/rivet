//! The PostgreSQL-state lease under `rivet load`: one keeper per process renews every lease,
//! and a load whose lease is not kept stops instead of writing the warehouse.
//!
//! Oracles: BigQuery's own table list and counts, the source's count, and the state
//! server's `state_lease`, `load_run` and `pg_stat_activity`, each read by this file.
//!
//! Needs postgres, `RIVET_TEST_STATE_URL` (an admin URL on a PostgreSQL server: every test
//! creates its own database and role there) and BigQuery credentials.

use crate::common::*;
use rivet::state::{StateRef, StateStore};

const ROWS: i64 = 50;
/// The lease TTL of a load a test stalls: its keeper renews every second.
const SHORT_TTL: [(&str, &str); 1] = [("RIVET_STATE_LEASE_TTL_S", "3")];
/// A lease TTL no stalled load of this file outlives.
const LONG_TTL: [(&str, &str); 1] = [("RIVET_STATE_LEASE_TTL_S", "600")];

/// A state database and its own login role on the server `RIVET_TEST_STATE_URL` names, both dropped with this.
struct ScratchState {
    admin_url: String,
    name: String,
}

impl ScratchState {
    /// Create the role and its database, or `None` after recording the skip.
    fn create(label: &str) -> Option<Self> {
        let admin_url = pg_state_url()?;
        let name = unique_name(label);
        let mut admin = postgres::Client::connect(&admin_url, postgres::NoTls)
            .unwrap_or_else(|e| panic!("connecting to RIVET_TEST_STATE_URL as admin: {e:#}"));
        for sql in [
            format!("CREATE ROLE {name} LOGIN PASSWORD '{name}'"),
            format!("CREATE DATABASE {name} OWNER {name}"),
        ] {
            admin
                .batch_execute(&sql)
                .unwrap_or_else(|e| panic!("creating the scratch state: {sql}: {e:#}"));
        }
        Some(Self { admin_url, name })
    }

    /// `host:port` of the state server.
    fn server(&self) -> &str {
        let after_auth = self.admin_url.rsplit_once('@').map_or("", |(_, r)| r);
        after_auth.split('/').next().unwrap_or_default()
    }

    /// The URL rivet reaches this state by, as the scratch role.
    fn url(&self) -> String {
        format!("postgresql://{n}:{n}@{}/{n}", self.server(), n = self.name)
    }

    /// An admin session on the server's own database.
    fn admin(&self) -> postgres::Client {
        postgres::Client::connect(&self.admin_url, postgres::NoTls).expect("admin session")
    }

    /// An admin session inside the scratch database; it does not count against the role's limit.
    fn inside(&self) -> postgres::Client {
        let (base, _) = self
            .admin_url
            .rsplit_once('/')
            .expect("an admin URL with a path");
        postgres::Client::connect(&format!("{base}/{}", self.name), postgres::NoTls)
            .expect("admin session in the scratch state")
    }

    /// Let the scratch role hold at most `most` connections (`-1`: no limit).
    fn connection_limit(&self, most: i32) {
        self.admin()
            .batch_execute(&format!("ALTER ROLE {} CONNECTION LIMIT {most}", self.name))
            .expect("alter the role's connection limit");
    }

    /// How many sessions the scratch role holds right now.
    fn connections(&self, admin: &mut postgres::Client) -> i64 {
        admin
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE usename = $1",
                &[&self.name],
            )
            .expect("pg_stat_activity")
            .get(0)
    }

    /// The holder of `table`'s lease, when a row for it exists.
    fn lease_holder(&self, inside: &mut postgres::Client, table: &str) -> Option<String> {
        inside
            .query_opt(
                "SELECT holder FROM state_lease WHERE lease_key LIKE '%.' || $1",
                &[&table],
            )
            .expect("state_lease")
            .map(|r| r.get(0))
    }

    /// Whether `table`'s ledger row says its load is writing the warehouse.
    fn is_writing(&self, inside: &mut postgres::Client, table: &str) -> bool {
        inside
            .query_opt(
                "SELECT 1 FROM load_run WHERE status = 'writing' AND target_table LIKE '%.' || $1",
                &[&table],
            )
            .expect("load_run")
            .is_some()
    }

    /// The status of every ledger row of `table`, oldest first.
    fn ledger(&self, table: &str) -> Vec<String> {
        self.inside()
            .query(
                "SELECT status FROM load_run WHERE target_table LIKE '%.' || $1 ORDER BY finished_at",
                &[&table],
            )
            .expect("load_run")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }
}

impl Drop for ScratchState {
    fn drop(&mut self) {
        if let Ok(mut admin) = postgres::Client::connect(&self.admin_url, postgres::NoTls) {
            let _ = admin.batch_execute(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                self.name
            ));
            let _ = admin.batch_execute(&format!("DROP ROLE IF EXISTS {}", self.name));
        }
    }
}

/// A source table of `ROWS` rows, its rig landing in this test's bucket prefix and loading into its dataset.
fn loadable(bq: &BqLive, prefix: &str) -> (String, Rig, Box<dyn std::any::Any>) {
    let pg = SqlEngine::Pg;
    pg.alive();
    let (t, guard) = pg.create(prefix, "id BIGINT PRIMARY KEY, v TEXT");
    pg.exec(&format!(
        "INSERT INTO {t} (id, v) SELECT g, md5(g::text) FROM generate_series(1, {ROWS}) g"
    ));
    let rig = pg
        .rig(&t)
        .dest_gcs_live(&bq.bucket, &format!("{}/{t}", bq.prefix))
        .top_line(&bq.load_line(""));
    (t, rig, guard)
}

/// The source's own row count.
fn source_rows(table: &str) -> i64 {
    pg_connect()
        .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
        .expect("the source's own row count")
        .get(0)
}

/// Which of `names` exist as BigQuery tables.
fn existing_tables(bq: &BqLive, names: &[&str]) -> Vec<String> {
    let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    bq.read_bq_rows(&format!(
        "SELECT table_name FROM `{}.{}.INFORMATION_SCHEMA.TABLES` WHERE table_name IN ({})",
        bq.project,
        bq.dataset,
        quoted.join(", ")
    ))
    .iter()
    .map(|r| r["table_name"].as_str().expect("name").to_string())
    .collect()
}

/// What a finished rivet printed on stderr.
fn stderr(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The pid inside a `host:pid:nonce` holder id.
fn holder_pid(holder: &str) -> i32 {
    holder
        .split(':')
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or_else(|| panic!("a holder id without a pid: {holder}"))
}

/// Send `signal` to the rivet process `pid`.
fn signal(pid: i32, signal: i32) {
    // SAFETY: `kill` on the pid of a child this test started.
    assert_eq!(
        unsafe { libc::kill(pid, signal) },
        0,
        "kill({pid}, {signal})"
    );
}

/// Where a load is stopped: the moment it holds its table's lease, or the moment its ledger row says it is writing the warehouse.
#[derive(Clone, Copy, PartialEq)]
enum At {
    LeaseTaken,
    Writing,
}

/// Run `load` beside this thread, stop its process `at` that point of `table`'s load, do `act` with the lease's holder id, let it continue and return what it printed with `act`'s answer.
fn stalled_under_its_lease<T>(
    state: &ScratchState,
    table: &str,
    at: At,
    load: impl FnOnce() -> std::process::Output + Send,
    act: impl FnOnce(&str) -> T,
) -> (std::process::Output, T) {
    let ended = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let running = scope.spawn(|| {
            let out = load();
            ended.store(true, std::sync::atomic::Ordering::SeqCst);
            out
        });
        let mut inside = state.inside();
        let holder = loop {
            let writing = at == At::LeaseTaken || state.is_writing(&mut inside, table);
            if let Some(holder) = state.lease_holder(&mut inside, table).filter(|_| writing) {
                break holder;
            }
            if ended.load(std::sync::atomic::Ordering::SeqCst) {
                let out = running.join().expect("the load thread");
                panic!(
                    "fixture: the load ended before it was seen at its stop:\n{}",
                    stderr(&out)
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        };
        let pid = holder_pid(&holder);
        signal(pid, libc::SIGSTOP);
        let acted = act(&holder);
        signal(pid, libc::SIGCONT);
        (running.join().expect("the load thread"), acted)
    })
}

/// A load that cannot open its lease keeper refuses, twice, leaving nothing behind; once it can, it loads the source.
#[test]
#[ignore = "live: requires postgres + a PostgreSQL state server + BigQuery creds"]
fn a_load_that_cannot_open_its_lease_keeper_refuses_twice_then_loads() {
    let Some(bq) = BqLive::from_env("lease_keeper") else {
        return;
    };
    let Some(state) = ScratchState::create("lk_refuse") else {
        return;
    };
    let (t, rig, _guard) = loadable(&bq, "lk_refuse");
    let _cleanup = bq.cleanup(&[&t]);
    let url = state.url();
    let env = [("RIVET_STATE_URL", url.as_str())];
    let out = rig.run_args_env(&[], &env);
    assert!(out.status.success(), "the extract:\n{}", stderr(&out));

    // One connection: the worker's own. The keeper's is the second.
    state.connection_limit(1);
    for cycle in 1..=2 {
        let out = rig.load_args_env(&["--pool", "1"], &env);
        let said = stderr(&out);
        assert!(
            !out.status.success(),
            "cycle {cycle}: a load whose lease nothing renews exited 0:\n{said}"
        );
        assert!(
            said.contains("[RIVET_STATE_LEASE_KEEPER_UNAVAILABLE]"),
            "cycle {cycle}: refused by its code:\n{said}"
        );
        assert_eq!(
            existing_tables(&bq, &[&t]),
            Vec::<String>::new(),
            "cycle {cycle}: nothing is loaded without a kept lease"
        );
        assert_eq!(
            state.lease_holder(&mut state.inside(), &t),
            None,
            "cycle {cycle}: no lease row is left behind"
        );
        assert_eq!(
            state.ledger(&t),
            Vec::<String>::new(),
            "cycle {cycle}: no ledger row makes the table rivet's own"
        );
    }

    state.connection_limit(-1);
    rig.load_ok(&["--pool", "1"], &env);
    let loaded: i64 = bq.read_bq_count(&t).parse().expect("a count");
    assert_eq!(loaded, source_rows(&t), "with a keeper the table is loaded");
}

/// Another holder takes `table`'s lease and the stalled load's TTL (`SHORT_TTL`) passes.
fn taken_by_another_holder(state: &ScratchState, table: &str) {
    let taken = state
        .inside()
        .execute(
            "UPDATE state_lease SET holder = 'elsewhere:1:1', \
             expires_at = now() + interval '10 minutes' WHERE lease_key LIKE '%.' || $1",
            &[&table],
        )
        .expect("take the lease away");
    assert_eq!(taken, 1, "fixture: the lease row to take");
    std::thread::sleep(std::time::Duration::from_secs(4));
}

/// The other holder of [`taken_by_another_holder`] ends.
fn the_other_holder_ends(state: &ScratchState) {
    let freed = state
        .inside()
        .execute(
            "DELETE FROM state_lease WHERE holder = 'elsewhere:1:1'",
            &[],
        )
        .expect("the other holder ends");
    assert_eq!(freed, 1, "the other holder's row was still its own");
}

/// A load stalled past its TTL whose lease another holder took writes nothing, records `refused`, and loads once the lease is free.
#[test]
#[ignore = "live: requires postgres + a PostgreSQL state server + BigQuery creds"]
fn a_load_whose_lease_is_taken_mid_load_writes_nothing_and_is_recorded_refused() {
    let Some(bq) = BqLive::from_env("lease_taken") else {
        return;
    };
    let Some(state) = ScratchState::create("lk_taken") else {
        return;
    };
    let (t, rig, _guard) = loadable(&bq, "lk_taken");
    let _cleanup = bq.cleanup(&[&t]);
    let url = state.url();
    let env = [("RIVET_STATE_URL", url.as_str()), SHORT_TTL[0]];
    let out = rig.run_args_env(&[], &env);
    assert!(out.status.success(), "the extract:\n{}", stderr(&out));

    let stalled = rig.twin();
    let (out, ()) = stalled_under_its_lease(
        &state,
        &t,
        At::LeaseTaken,
        move || stalled.load_args_env(&["--pool", "1"], &env),
        |_| taken_by_another_holder(&state, &t),
    );
    let said = stderr(&out);
    assert!(
        !out.status.success(),
        "a load whose lease another holder took exited 0:\n{said}"
    );
    assert_eq!(out.status.code(), Some(5), "a refusal:\n{said}");
    assert!(said.contains("[RIVET_STATE_LEASE_LOST]"), "{said}");
    assert_eq!(
        existing_tables(&bq, &[&t]),
        Vec::<String>::new(),
        "no warehouse write after the lease was lost"
    );
    assert_eq!(state.ledger(&t), vec!["refused".to_string()]);
    assert_eq!(
        state.lease_holder(&mut state.inside(), &t).as_deref(),
        Some("elsewhere:1:1"),
        "the other holder's row is not released by the load that lost it"
    );
    let again = rig.load_args_env(&["--pool", "1"], &env);
    assert!(!again.status.success() && stderr(&again).contains("is writing `"));
    assert_eq!(
        existing_tables(&bq, &[&t]),
        Vec::<String>::new(),
        "a second cycle beside the other holder loads nothing either"
    );
    assert_eq!(state.ledger(&t), vec!["refused".to_string()]);

    the_other_holder_ends(&state);
    rig.load_ok(&["--pool", "1"], &env);
    let loaded: i64 = bq.read_bq_count(&t).parse().expect("a count");
    assert_eq!(
        loaded,
        source_rows(&t),
        "a refused load is loaded by the next"
    );
}

/// A load whose lease another holder took while it was writing the warehouse fails, is recorded `failed`, and the next load owns the table.
#[test]
#[ignore = "live: requires postgres + a PostgreSQL state server + BigQuery creds"]
fn a_load_whose_lease_is_taken_during_the_write_fails_and_is_recorded_failed() {
    let Some(bq) = BqLive::from_env("lease_written") else {
        return;
    };
    let Some(state) = ScratchState::create("lk_written") else {
        return;
    };
    let (t, rig, _guard) = loadable(&bq, "lk_written");
    let rig = rig.a_failed_run_may_leave(
        &[Leftover::LoadAttempt],
        "the lease is lost after the warehouse write: the load did reach the write",
    );
    let _cleanup = bq.cleanup(&[&t]);
    let url = state.url();
    let env = [("RIVET_STATE_URL", url.as_str()), SHORT_TTL[0]];
    let out = rig.run_args_env(&[], &env);
    assert!(out.status.success(), "the extract:\n{}", stderr(&out));

    let stalled = rig.twin();
    let (out, ()) = stalled_under_its_lease(
        &state,
        &t,
        At::Writing,
        move || stalled.load_args_env(&["--pool", "1"], &env),
        |_| taken_by_another_holder(&state, &t),
    );
    let said = stderr(&out);
    assert!(
        !out.status.success(),
        "a load whose lease another holder took during the write exited 0:\n{said}"
    );
    assert_eq!(out.status.code(), Some(3), "an integrity failure:\n{said}");
    assert!(
        said.contains("[RIVET_LOAD_LEASE_LOST_DURING_WRITE]"),
        "{said}"
    );
    assert!(
        said.contains("This run's warehouse write is done and is recorded as failed"),
        "{said}"
    );
    assert_eq!(state.ledger(&t), vec!["failed".to_string()]);
    let written: i64 = bq.read_bq_count(&t).parse().expect("a count");
    assert_eq!(written, source_rows(&t), "the write the message names");

    the_other_holder_ends(&state);
    rig.load_ok(&["--pool", "1"], &env);
    let loaded: i64 = bq.read_bq_count(&t).parse().expect("a count");
    assert_eq!(
        loaded,
        source_rows(&t),
        "the table is rivet's own for the next load"
    );
}

/// `loadable` with `n` tables in one config: the rig, and every table name.
fn loadable_pool(
    bq: &BqLive,
    prefix: &str,
    n: usize,
) -> (Vec<String>, Rig, Box<dyn std::any::Any>) {
    let (t, mut rig, guard) = loadable(bq, prefix);
    let mut tables = vec![t.clone()];
    for i in 1..n {
        let name = format!("{t}_s{i}");
        rig = rig.also_export(&name, &format!("SELECT id, v FROM {t}"));
        tables.push(name);
    }
    (tables, rig, guard)
}

/// Run `load` while sampling the state server every 20 ms; what it printed and the peak of each sample of `probe`.
fn sampled<const N: usize>(
    state: &ScratchState,
    probe: impl Fn(&mut postgres::Client) -> [i64; N] + Sync,
    load: impl FnOnce() -> std::process::Output,
) -> (std::process::Output, [i64; N]) {
    let ended = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let mut inside = state.inside();
            let mut peak = [0; N];
            while !ended.load(std::sync::atomic::Ordering::SeqCst) {
                for (p, seen) in peak.iter_mut().zip(probe(&mut inside)) {
                    *p = (*p).max(seen);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            peak
        });
        let out = load();
        ended.store(true, std::sync::atomic::Ordering::SeqCst);
        (out, sampler.join().expect("the sampler"))
    })
}

/// A pooled load short of file descriptors never works under a lease the state server has let lapse.
#[test]
#[ignore = "live: requires postgres + a PostgreSQL state server + BigQuery creds"]
fn a_pooled_load_short_of_file_descriptors_never_runs_under_a_lapsed_lease() {
    const POOL: usize = 16;
    /// Open files for sixteen workers: enough for some tables to load, too few for every connection.
    const FEW_FILES: u64 = 150;
    let Some(bq) = BqLive::from_env("lease_fds") else {
        return;
    };
    let Some(state) = ScratchState::create("lk_fds") else {
        return;
    };
    let (tables, rig, _guard) = loadable_pool(&bq, "lk_fds", POOL);
    let names: Vec<&str> = tables.iter().map(String::as_str).collect();
    let _cleanup = bq.cleanup(&names);
    let url = state.url();
    let env = [("RIVET_STATE_URL", url.as_str()), SHORT_TTL[0]];
    let out = rig.run_args_env(&[], &env);
    assert!(out.status.success(), "the extract:\n{}", stderr(&out));

    let capped = rig.twin().open_files(FEW_FILES);
    let leases = |inside: &mut postgres::Client| {
        let row = inside
            .query_one(
                "SELECT count(*), count(*) FILTER (WHERE expires_at < now()) FROM state_lease",
                &[],
            )
            .expect("state_lease");
        [row.get(0), row.get(1)]
    };
    let (out, [held, lapsed]) = sampled(&state, leases, || {
        capped.load_args_env(&["--pool", &POOL.to_string()], &env)
    });
    let said = stderr(&out);
    assert!(
        said.contains("Too many open files"),
        "fixture: {FEW_FILES} open files were enough for the whole pool:\n{said}"
    );
    assert!(held > 0, "fixture: the sampler saw no lease row:\n{said}");
    assert_eq!(
        lapsed, 0,
        "{lapsed} lease row(s) at once were past their expiry while the load was still running:\n{said}"
    );
    assert_ne!(out.status.code(), Some(101), "the load crashed:\n{said}");

    rig.load_ok(&["--pool", &POOL.to_string()], &env);
    let mut loaded = existing_tables(&bq, &names);
    loaded.sort();
    let mut expected = tables.clone();
    expected.sort();
    assert_eq!(
        loaded, expected,
        "with the cap lifted every table is loaded"
    );
}

/// The most TLS connections a `rivet load` of this config holds at once, sampled with `lsof` every 50 ms while `load` runs.
fn peak_tls_connections(
    rig: &Rig,
    load: impl FnOnce() -> std::process::Output,
) -> (
    std::process::Output,
    std::collections::BTreeMap<String, usize>,
) {
    let wanted = format!("load --config {}", rig.config_path().display());
    let ended = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let run = |cmd: &str, args: &[&str]| {
                let out = std::process::Command::new(cmd)
                    .args(args)
                    .output()
                    .unwrap_or_else(|e| {
                        panic!("fixture: {cmd} is a prerequisite of this cell: {e}")
                    });
                String::from_utf8_lossy(&out.stdout).into_owned()
            };
            let mut peak = std::collections::BTreeMap::<String, usize>::new();
            while !ended.load(std::sync::atomic::Ordering::SeqCst) {
                for pid in run("pgrep", &["-f", &wanted]).split_whitespace() {
                    let open = run("lsof", &["-a", "-n", "-P", "-i", "TCP", "-p", pid]);
                    let mut by_remote = std::collections::BTreeMap::<String, usize>::new();
                    for line in open.lines().filter(|l| l.contains(":443")) {
                        let remote = line.rsplit("->").next().unwrap_or_default();
                        let remote = remote.split_whitespace().next().unwrap_or_default();
                        *by_remote.entry(remote.to_string()).or_default() += 1;
                    }
                    if by_remote.values().sum::<usize>() > peak.values().sum::<usize>() {
                        peak = by_remote;
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            peak
        });
        let out = load();
        ended.store(true, std::sync::atomic::Ordering::SeqCst);
        (out, sampler.join().expect("the sampler"))
    })
}

/// A pooled load of sixteen tables takes its storage requests from one budget, so it holds far fewer connections than three per table.
#[test]
#[ignore = "live: requires postgres + BigQuery creds"]
fn a_pooled_load_of_sixteen_tables_shares_one_budget_of_storage_connections() {
    const POOL: usize = 16;
    /// Between the peaks measured with the budget (50 and 51: 22 to storage, 26 to BigQuery) and without it (74).
    const MOST: usize = 60;
    let Some(bq) = BqLive::from_env("load_budget") else {
        return;
    };
    let (tables, rig, _guard) = loadable_pool(&bq, "lk_budget", POOL);
    let names: Vec<&str> = tables.iter().map(String::as_str).collect();
    let _cleanup = bq.cleanup(&names);
    let out = rig.run_args_env(&[], &[]);
    assert!(out.status.success(), "the extract:\n{}", stderr(&out));

    let (out, peak) = peak_tls_connections(&rig, || {
        rig.load_args_env(&["--pool", &POOL.to_string()], &[])
    });
    assert!(out.status.success(), "the pooled load:\n{}", stderr(&out));
    let all: usize = peak.values().sum();
    assert!(
        all > POOL,
        "fixture: the sampler saw {all} TLS connection(s), fewer than one per worker: {peak:?}"
    );
    assert!(
        all <= MOST,
        "a load of {POOL} tables held {all} TLS connections at once, more than {MOST}: {peak:?}"
    );
}

/// A pooled load of sixteen tables holds at most one state connection per worker plus the keeper's.
#[test]
#[ignore = "live: requires postgres + a PostgreSQL state server + BigQuery creds"]
fn a_pooled_load_holds_one_state_connection_per_worker_plus_one() {
    const POOL: usize = 16;
    let Some(bq) = BqLive::from_env("lease_ceiling") else {
        return;
    };
    let Some(state) = ScratchState::create("lk_ceiling") else {
        return;
    };
    let (tables, rig, _guard) = loadable_pool(&bq, "lk_ceiling", POOL);
    let names: Vec<&str> = tables.iter().map(String::as_str).collect();
    let _cleanup = bq.cleanup(&names);
    let url = state.url();
    let env = [("RIVET_STATE_URL", url.as_str())];
    let out = rig.run_args_env(&[], &env);
    assert!(out.status.success(), "the extract:\n{}", stderr(&out));

    let connections = |inside: &mut postgres::Client| [state.connections(inside)];
    let (out, [peak]) = sampled(&state, connections, || {
        rig.load_args_env(&["--pool", &POOL.to_string()], &env)
    });
    assert!(out.status.success(), "the pooled load:\n{}", stderr(&out));
    assert!(
        peak >= POOL as i64,
        "fixture: the sampler saw {peak} connection(s), fewer than the {POOL} workers"
    );
    assert!(
        peak <= POOL as i64 + 1,
        "a load at --pool {POOL} held {peak} state connections at once: the ceiling is one \
         per worker plus one"
    );
    let mut loaded = existing_tables(&bq, &names);
    loaded.sort();
    let mut expected = tables.clone();
    expected.sort();
    assert_eq!(loaded, expected, "every table of the pool is loaded");
}

/// `holder`'s load of `table` is stalled under its lease while `contender` loads the same table: the contender is stopped by `refusal` and loads nothing, the lease stays the holder's, and the holder then loads the source.
fn contend(
    state: &ScratchState,
    bq: &BqLive,
    table: &str,
    holder: &Rig,
    contender: &Rig,
    refusal: &str,
) {
    let url = state.url();
    let env = [("RIVET_STATE_URL", url.as_str()), LONG_TTL[0]];
    let stalled = holder.twin();
    let (first, second) = stalled_under_its_lease(
        state,
        table,
        At::LeaseTaken,
        move || stalled.load_args_env(&["--pool", "1"], &env),
        |held_by| {
            let second = contender.load_args_env(&["--pool", "1"], &env);
            assert_eq!(
                state.lease_holder(&mut state.inside(), table).as_deref(),
                Some(held_by),
                "the lease is still the first holder's after the contender ran"
            );
            assert_eq!(
                existing_tables(bq, &[table]),
                Vec::<String>::new(),
                "nothing is loaded while the holder is stalled"
            );
            second
        },
    );
    let said = stderr(&second);
    assert!(
        !second.status.success(),
        "the contender loaded a table whose lease another process held:\n{said}"
    );
    assert!(
        said.contains(refusal),
        "the contender is stopped by `{refusal}`:\n{said}"
    );
    assert!(first.status.success(), "the holder:\n{}", stderr(&first));
    let loaded: i64 = bq.read_bq_count(table).parse().expect("a count");
    assert_eq!(
        loaded,
        source_rows(table),
        "the holder loaded the source once"
    );
}

/// The previous release and this tree contend for one table's lease, in both orders: one holds it each time. The previous release cannot open a state this tree migrated, so it is stopped at the state's schema before it reaches the lease.
#[test]
#[ignore = "live: requires postgres + a PostgreSQL state server + BigQuery creds + RIVET_PREV_RELEASE_BIN"]
fn the_previous_release_and_this_tree_never_hold_one_lease_together() {
    let Some(bq) = BqLive::from_env("lease_compat") else {
        return;
    };
    for old_holds in [true, false] {
        let Some(state) = ScratchState::create("lk_compat") else {
            return;
        };
        let (t, new, _guard) = loadable(&bq, "lk_compat");
        let Some(old) = new.as_previous_release() else {
            return;
        };
        let _cleanup = bq.cleanup(&[&t]);
        let url = state.url();
        let env = [("RIVET_STATE_URL", url.as_str())];
        let (holder, contender, refusal) = match old_holds {
            true => (&old, &new, "is writing `"),
            false => (&new, &old, "[RIVET_STATE_SCHEMA_NEWER]"),
        };
        let out = holder.run_args_env(&[], &env);
        assert!(out.status.success(), "the extract:\n{}", stderr(&out));
        contend(&state, &bq, &t, holder, contender, refusal);
        assert_eq!(
            state.ledger(&t),
            vec!["success".to_string()],
            "one load of the table is recorded, the holder's"
        );
    }
}

/// One keeper renews every lease of its process on one connection, and tells the lease whose row is gone from the one still held.
#[test]
#[ignore = "live: requires a PostgreSQL state server"]
fn one_keeper_renews_every_lease_and_reports_the_one_that_is_gone() {
    let Some(state) = ScratchState::create("lk_keeper") else {
        return;
    };
    let store = StateStore::open_at_ref(&StateRef::Postgres(state.url())).expect("open the state");
    let leases: Vec<_> = ["a", "b", "c"]
        .iter()
        .map(|k| {
            store
                .try_load_lease(&format!("p.d.{k}"))
                .expect("take")
                .expect("free")
        })
        .collect();
    let mut admin = state.admin();
    assert_eq!(
        state.connections(&mut admin),
        2,
        "the store's connection and the keeper's, for three leases"
    );
    // The default TTL is 30 s and the keeper renews every 10 s: after 12 s an unrenewed row has 18 s left.
    std::thread::sleep(std::time::Duration::from_secs(12));
    let mut inside = state.inside();
    let renewed: i64 = inside
        .query_one(
            "SELECT count(*) FROM state_lease WHERE expires_at > now() + interval '23 seconds'",
            &[],
        )
        .expect("state_lease")
        .get(0);
    assert_eq!(renewed, 3, "every lease of the process is renewed");
    assert!(leases.iter().all(|l| l.is_held()));

    let gone = inside
        .execute("DELETE FROM state_lease WHERE lease_key = 'p.d.b'", &[])
        .expect("delete one row");
    assert_eq!(gone, 1);
    std::thread::sleep(std::time::Duration::from_secs(11));
    let held: Vec<bool> = leases.iter().map(|l| l.is_held()).collect();
    assert_eq!(
        held,
        [true, false, true],
        "only the lease whose row is gone is lost"
    );

    drop(leases);
    assert_eq!(
        state.connections(&mut admin),
        1,
        "the keeper's connection ends with its last lease"
    );
    let left: i64 = inside
        .query_one("SELECT count(*) FROM state_lease", &[])
        .expect("state_lease")
        .get(0);
    assert_eq!(left, 0, "every lease row is released");
}
