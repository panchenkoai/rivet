//! A command that rewrites an export's stored progress, against a LIVE checkpointed run of
//! that export: it is refused while the run holds the export's run lease, the run delivers
//! every row, and the command works once the holder is gone (finished, or SIGKILLed).
//!
//! Oracles: the source's own ids, the parts the Success manifests declare read back with
//! Arrow, and the state DB re-read by the test.

use crate::common::*;

const ROWS: i64 = 500;
const RANGE: &str = "chunk_column: id";
const KEYSET: &str = "chunk_by_key: id";
const REFUSAL: &str = "[RIVET_STATE_RUN_IN_PROGRESS]";

/// The state backend a scenario runs on, re-read by the test itself.
enum State {
    Sqlite,
    Postgres(ScratchStateDb),
}

impl State {
    /// A fresh database on the Postgres state server (:5433), dropped with the scenario.
    fn postgres(tag: &str) -> Self {
        State::Postgres(ScratchStateDb::new(tag))
    }

    /// The `RIVET_STATE_URL` every rivet of the scenario gets; empty selects SQLite beside the config.
    fn url(&self) -> String {
        match self {
            State::Sqlite => String::new(),
            State::Postgres(db) => db.url(),
        }
    }

    /// One integer read from the state DB; 0 while the table is not there yet.
    fn count(&self, cfg: &std::path::Path, sql: &str) -> i64 {
        match self {
            State::Sqlite => sqlite(cfg)
                .query_row(sql, [], |r| r.get::<_, i64>(0))
                .unwrap_or(0),
            State::Postgres(db) => db
                .client()
                .query_one(sql, &[])
                .map(|r| r.get::<_, i64>(0))
                .unwrap_or(0),
        }
    }

    /// The newest `running` run-status row of `export`.
    fn running_run_id(&self, cfg: &std::path::Path, export: &str) -> String {
        let sql = format!(
            "SELECT run_id FROM run_status WHERE export_name = '{export}' AND status = 'running' \
             ORDER BY started_at DESC LIMIT 1"
        );
        match self {
            State::Sqlite => sqlite(cfg)
                .query_row(&sql, [], |r| r.get::<_, String>(0))
                .expect("a running run-status row"),
            State::Postgres(db) => db
                .client()
                .query_one(&sql, &[])
                .expect("a running run-status row")
                .get(0),
        }
    }

    /// Run statements on the state DB behind rivet's back.
    fn exec(&self, cfg: &std::path::Path, sql: &str) {
        match self {
            State::Sqlite => sqlite(cfg).execute_batch(sql).expect("state DB write"),
            State::Postgres(db) => db.client().batch_execute(sql).expect("state DB write"),
        }
    }
}

/// The SQLite state DB beside `cfg`, waiting out rivet's own writes.
fn sqlite(cfg: &std::path::Path) -> rusqlite::Connection {
    let c = rusqlite::Connection::open(cfg.parent().unwrap().join(".rivet_state.db"))
        .expect("open the state DB");
    c.busy_timeout(std::time::Duration::from_secs(10)).unwrap();
    c
}

/// One live scenario: a slow checkpointed export of `ROWS` rows, its source table and its state.
struct Scenario {
    engine: SqlEngine,
    table: String,
    rig: Rig,
    state: State,
    url: String,
    _guard: Box<dyn std::any::Any>,
}

impl Scenario {
    /// Seed the table and build the export, chunked by `key_line`, one batch every 400 ms.
    fn new(engine: SqlEngine, key_line: &str, state: State) -> Self {
        engine.alive();
        let (id, v) = (engine.col("id"), engine.col("v"));
        let int = engine.int64();
        let (table, guard) =
            engine.create("reset_live", &format!("{id} {int} PRIMARY KEY, {v} {int}"));
        let rows: Vec<String> = (1..=ROWS).map(|i| format!("({i}, {})", i * 7)).collect();
        engine.exec(&format!(
            "INSERT INTO {table} ({id}, {v}) VALUES {}",
            rows.join(", ")
        ));
        let rig = engine
            .rig(&table)
            .mode("chunked")
            .export_line(key_line)
            .export_line("chunk_size: 20")
            .export_line("chunk_checkpoint: true")
            .source_line("tuning:")
            .source_line("  batch_size: 10")
            .source_line("  throttle_ms: 400");
        let url = state.url();
        Self {
            engine,
            table,
            rig,
            state,
            url,
            _guard: guard,
        }
    }

    fn env(&self) -> [(&str, &str); 1] {
        [("RIVET_STATE_URL", self.url.as_str())]
    }

    fn count(&self, sql: &str) -> i64 {
        self.state.count(&self.rig.config_path(), sql)
    }

    /// Chunk-run rows the state holds for this export.
    fn chunk_runs(&self) -> i64 {
        self.count(&format!(
            "SELECT COUNT(*) FROM chunk_run WHERE export_name = '{}'",
            self.rig.export_name()
        ))
    }

    /// Start the run and return once it is mid-export: its ledger row is `running` and a part is on disk.
    fn spawn_mid_run(&self) -> Spawned<'_> {
        let mut owner = self.rig.spawn_args_env(&[], &self.env());
        let t0 = std::time::Instant::now();
        let running = format!(
            "SELECT COUNT(*) FROM run_status WHERE status = 'running' AND export_name = '{}'",
            self.rig.export_name()
        );
        while self.count(&running) == 0 || !has_a_part(&self.rig.out_dir()) {
            assert!(
                owner.try_wait().unwrap().is_none(),
                "fixture: the run exited before it was seen mid-export"
            );
            assert!(
                t0.elapsed().as_secs() < 60,
                "fixture: the run never reached its first part"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        owner
    }

    /// `rivet state <args>` on this scenario's state: exit code, stdout, and stdout + stderr.
    fn state_cmd(&self, args: &[&str]) -> (Option<i32>, String, String) {
        let mut argv = vec!["state"];
        argv.extend_from_slice(args);
        let out = self.rig.cli_env(&argv, &self.env());
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let said = format!("{stdout}{}", String::from_utf8_lossy(&out.stderr));
        (out.status.code(), stdout, said)
    }

    /// A state command must be refused, naming the live `owner` and the way out.
    fn assert_refused(&self, args: &[&str], owner: &mut Spawned<'_>, live_run: &str) {
        let (code, _, said) = self.state_cmd(args);
        assert!(
            owner.try_wait().unwrap().is_none(),
            "fixture: the run must still be alive when `state {args:?}` answers"
        );
        assert_eq!(
            code,
            Some(5),
            "`state {args:?}` against a live run must be a refusal (exit 5):\n{said}"
        );
        assert!(
            said.contains(REFUSAL)
                && said.contains(&format!(
                    "run '{live_run}' is in progress in a live rivet process, which holds the \
                     export's run lease. Wait for it to finish, or stop that process (its pid \
                     ends the run id), then repeat this command."
                ))
                && live_run.ends_with(&format!("_{}", owner.id())),
            "the refusal must carry its code, name the live run (whose id ends in its pid) and \
             say how to proceed:\n{said}"
        );
    }

    /// With no live run both resets answer exactly as they did before the lease check (exit 0, the same line).
    fn assert_uncontested_resets_answer_as_before(&self, chunk_runs: i64) {
        let export = self.rig.export_name();
        assert_eq!(
            self.chunk_runs(),
            chunk_runs,
            "fixture: chunk runs to remove"
        );
        let (code, answer, _) = self.state_cmd(&["reset-chunks", "--export", export]);
        assert_eq!(
            (code, answer.trim()),
            (
                Some(0),
                format!("Removed {chunk_runs} chunk run record(s) for export '{export}'.").as_str()
            ),
            "reset-chunks with no live run"
        );
        assert_eq!(self.chunk_runs(), 0, "reset-chunks removed the chunk run");
        let (code, answer, _) = self.state_cmd(&["reset", "--export", export]);
        assert_eq!(
            (code, answer.trim()),
            (
                Some(0),
                format!("State reset for export '{export}'").as_str()
            ),
            "reset with no live run"
        );
    }

    /// The source ids against the ids in the parts the Success manifests declare.
    fn assert_every_row_delivered_once(&self, what: &str) {
        let source: Vec<i64> = self
            .engine
            .id_v_pairs(&self.table)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let mut delivered: Vec<i64> = declared_parquet_parts(&self.rig.out_dir())
            .iter()
            .flat_map(|p| parquet_ids(p))
            .collect();
        delivered.sort_unstable();
        assert_eq!(source.len() as i64, ROWS, "fixture: the source row count");
        assert_eq!(
            delivered.len(),
            source.len(),
            "{what}: manifest-declared rows against source rows"
        );
        assert_eq!(delivered, source, "{what}: every source id exactly once");
    }
}

/// Whether `dir` holds a parquet part yet.
fn has_a_part(dir: &std::path::Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|rd| {
        rd.flatten()
            .any(|e| e.path().extension().is_some_and(|x| x == "parquet"))
    })
}

/// While the run is alive every reset is refused, twice over; it then delivers every row,
/// and once it has ended the same resets go through.
fn a_reset_is_refused_while_the_run_is_alive_and_works_after_it(
    engine: SqlEngine,
    key_line: &str,
    state: State,
) {
    let s = Scenario::new(engine, key_line, state);
    let export = s.rig.export_name().to_string();
    let mut owner = s.spawn_mid_run();
    let live_run = s.state.running_run_id(&s.rig.config_path(), &export);
    let runs_before = s.chunk_runs();

    for cycle in 0..2 {
        s.assert_refused(
            &["reset-chunks", "--export", &export],
            &mut owner,
            &live_run,
        );
        s.assert_refused(&["reset", "--export", &export], &mut owner, &live_run);
        assert_eq!(
            s.chunk_runs(),
            runs_before,
            "cycle {cycle}: a refused reset must leave the chunk run in place"
        );
    }
    let (code, _, said) = s.state_cmd(&["reset-chunks", "--stuck-checkpoints"]);
    assert!(
        owner.try_wait().unwrap().is_none(),
        "fixture: the run must still be alive when --stuck-checkpoints answers"
    );
    assert_eq!(code, Some(0), "{said}");
    assert!(
        !said.contains(&format!("for export '{export}'")),
        "--stuck-checkpoints must not clear a live run's checkpoint:\n{said}"
    );
    assert_eq!(
        s.chunk_runs(),
        runs_before,
        "--stuck-checkpoints must leave a live run's chunk run in place"
    );
    if key_line == RANGE {
        assert_eq!(runs_before, 1, "fixture: a range run keeps one chunk run");
        assert!(
            said.contains(&format!(
                "Skipping '{export}': run '{live_run}' is in progress in a live rivet process, \
                 so its checkpoint is not stuck."
            )),
            "--stuck-checkpoints must say why it left the export alone:\n{said}"
        );
    }

    assert!(
        owner.wait().unwrap().success(),
        "the run the resets were refused for must finish"
    );
    assert!(s.rig.out_dir().join("_SUCCESS").exists());
    s.assert_every_row_delivered_once("after the refused resets");

    s.assert_uncontested_resets_answer_as_before(runs_before);
}

/// A SIGKILLed holder does not keep the lease: the reset works at once, and the next run delivers every row.
fn a_killed_run_does_not_block_the_reset(engine: SqlEngine, key_line: &str, state: State) {
    let s = Scenario::new(engine, key_line, state);
    let export = s.rig.export_name().to_string();
    let mut owner = s.spawn_mid_run();
    let live_run = s.state.running_run_id(&s.rig.config_path(), &export);
    s.assert_refused(
        &["reset-chunks", "--export", &export],
        &mut owner,
        &live_run,
    );
    owner.kill().expect("SIGKILL the run");
    assert!(
        !owner.wait().unwrap().success(),
        "fixture: the run was killed"
    );

    s.assert_uncontested_resets_answer_as_before(1);

    let rerun = s.rig.run_args_env(&[], &s.env());
    assert!(
        rerun.status.success(),
        "the run after the reset: {}",
        String::from_utf8_lossy(&rerun.stderr)
    );
    s.assert_every_row_delivered_once("the run after a killed run and a reset");
}

/// A run whose chunk-checkpoint rows are deleted under it fails; it does not publish an empty success.
fn a_run_whose_checkpoint_rows_vanish_fails_loudly(engine: SqlEngine, state: State) {
    let mut s = Scenario::new(engine, RANGE, state);
    s.rig = s.rig.a_failed_run_may_leave(
        &[
            Leftover::OrphanPart,
            Leftover::FileLog,
            Leftover::ChunkCheckpoint,
        ],
        "the test deletes the run's checkpoint rows under it: the run fails after writing pages and leaves them beside its released claim",
    );
    let export = s.rig.export_name().to_string();
    let mut owner = s.spawn_mid_run();
    s.state.exec(
        &s.rig.config_path(),
        &format!(
            "DELETE FROM chunk_task WHERE run_id IN \
               (SELECT run_id FROM chunk_run WHERE export_name = '{export}'); \
             DELETE FROM chunk_run WHERE export_name = '{export}';"
        ),
    );
    assert!(
        owner.try_wait().unwrap().is_none(),
        "fixture: the run must still be alive when its checkpoint rows go"
    );
    let status = owner.wait().unwrap();
    assert_eq!(
        status.code(),
        Some(5),
        "a run that lost its checkpoint rows must refuse to finish"
    );
    assert!(
        !s.rig.out_dir().join("_SUCCESS").exists(),
        "no success marker for a run whose checkpoint rows vanished"
    );
    assert!(
        declared_parquet_parts(&s.rig.out_dir()).is_empty(),
        "no Success manifest for a run whose checkpoint rows vanished"
    );
    let rerun = s.rig.run_args_env(&[], &s.env());
    assert!(
        rerun.status.success(),
        "the remedy the refusal names, running the export again: {}",
        String::from_utf8_lossy(&rerun.stderr)
    );
    s.assert_every_row_delivered_once("the run after one that lost its checkpoint rows");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_a_reset_is_refused_while_a_range_run_is_alive_and_works_after_it() {
    a_reset_is_refused_while_the_run_is_alive_and_works_after_it(
        SqlEngine::Pg,
        RANGE,
        State::Sqlite,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_a_reset_is_refused_while_a_keyset_run_is_alive_and_works_after_it() {
    a_reset_is_refused_while_the_run_is_alive_and_works_after_it(
        SqlEngine::Pg,
        KEYSET,
        State::Sqlite,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_a_killed_range_run_does_not_block_the_reset() {
    a_killed_run_does_not_block_the_reset(SqlEngine::Pg, RANGE, State::Sqlite);
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_a_range_run_whose_checkpoint_rows_vanish_fails_loudly() {
    a_run_whose_checkpoint_rows_vanish_fails_loudly(SqlEngine::Pg, State::Sqlite);
}

#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn pg_state_a_reset_is_refused_while_a_range_run_is_alive_and_works_after_it() {
    a_reset_is_refused_while_the_run_is_alive_and_works_after_it(
        SqlEngine::Pg,
        RANGE,
        State::postgres("reset_live"),
    );
}

#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn pg_state_a_killed_range_run_does_not_block_the_reset() {
    a_killed_run_does_not_block_the_reset(SqlEngine::Pg, RANGE, State::postgres("reset_kill"));
}

#[test]
#[ignore = "live: requires postgres + postgres-state"]
fn pg_state_a_range_run_whose_checkpoint_rows_vanish_fails_loudly() {
    a_run_whose_checkpoint_rows_vanish_fails_loudly(SqlEngine::Pg, State::postgres("reset_vanish"));
}
