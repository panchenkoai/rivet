//! Live integration tests for the PostgreSQL state backend.
//!
//! These tests require a running PostgreSQL instance.  They are skipped by
//! default (`#[ignore]`) and opt-in via:
//!
//! ```text
//! RIVET_TEST_STATE_URL=postgresql://user:pass@localhost/rivet_state_test \
//!   cargo test -p rivet-cli --test live_suite -- --ignored
//! ```
//!
//! The test database must exist; tables are created / migrated automatically.

use rivet::state::StateStore;

/// Open a Postgres-backed `StateStore` using `RIVET_TEST_STATE_URL`, or skip
/// the test if the variable is not set.
fn pg_store() -> Option<StateStore> {
    let url = std::env::var("RIVET_TEST_STATE_URL").ok()?;
    if !url.starts_with("postgres") {
        return None;
    }
    // Temporarily set RIVET_STATE_URL so StateStore::open() picks up Postgres.
    // Safety: tests are single-threaded at the point this helper is called.
    unsafe { std::env::set_var("RIVET_STATE_URL", &url) };
    let store = StateStore::open(":memory:").expect("open pg state store");
    unsafe { std::env::remove_var("RIVET_STATE_URL") };
    Some(store)
}

/// PARITY: the per-table lease refuses a second holder on Postgres exactly as it
/// does on SQLite. This is the ONE mechanism in the state layer with two genuinely
/// different implementations — `flock` on a per-key sidecar file vs
/// `pg_try_advisory_lock` on the SESSION — and the only one this file did not cover,
/// while nine other aspects of the backend already were.
///
/// `rivet load --pool N` / `rivet compact --pool N` rest on it: every worker reopens
/// its own store, so every worker holds its own session, and the lease is the only
/// thing keeping two of them off one table.
///
/// MEASURED DIVERGENCE, deliberately NOT asserted below because it is PostgreSQL's
/// behaviour rather than rivet's: `pg_try_advisory_lock` is RE-ENTRANT within one
/// session — the same session takes the same key twice and both calls return true
/// (`pg_locks` still shows a single entry; it counts). SQLite's `flock` refuses the
/// second holder even inside one process, which is exactly what the sibling unit test
/// in `state/load_lease.rs` pins. So a future change that hands several workers ONE
/// shared store would fail loudly on SQLite and pass SILENTLY on Postgres, with the
/// per-table guarantee quietly gone. One store per worker is a requirement, not a
/// style choice.
#[test]
#[ignore]
fn pg_lease_refuses_a_second_session_like_sqlite_refuses_a_second_store() {
    let Some(a) = pg_store() else { return };
    let Some(b) = pg_store() else { return };
    let key = format!("p.d.lease_{}", std::process::id());

    let held = a
        .try_load_lease(&key)
        .unwrap()
        .expect("the first session takes the lease");
    assert!(
        b.try_load_lease(&key).unwrap().is_none(),
        "a second SESSION must be refused while the first holds the lease"
    );
    assert!(
        b.try_load_lease(&format!("{key}_other")).unwrap().is_some(),
        "another table is independent — the lease is per-table, not global"
    );
    drop(held);
    assert!(
        b.try_load_lease(&key).unwrap().is_some(),
        "free once released — no timer, no cleanup step"
    );
}

#[test]
#[ignore]
fn pg_cursor_round_trip() {
    let Some(s) = pg_store() else { return };

    s.update_legacy("pg_orders", "2024-06-01").unwrap();
    let got = s.get("pg_orders").unwrap();
    assert_eq!(got.last_cursor_value.as_deref(), Some("2024-06-01"));

    s.update_legacy("pg_orders", "2024-07-01").unwrap();
    let got2 = s.get("pg_orders").unwrap();
    assert_eq!(got2.last_cursor_value.as_deref(), Some("2024-07-01"));

    s.reset("pg_orders").unwrap();
    let empty = s.get("pg_orders").unwrap();
    assert!(empty.last_cursor_value.is_none());
}

/// Parallel-keyset crash-recovery ranges must round-trip on a Postgres STATE
/// backend. The `keyset_range` table's range_index/done are bound as StateParam::I64
/// and read as i64; the v19 PG migration originally declared them int4, so
/// persist_keyset_ranges errored (WrongType) at OPEN and load/commit panicked on
/// resume — parallel keyset + chunk_checkpoint was dead-on-arrival on PG state, a gap
/// the SQLite-only unit tests could never see. This exercises persist → load → commit
/// against real Postgres: RED on the int4 DDL, green on BIGINT.
#[test]
#[ignore]
fn pg_keyset_range_round_trips_and_commits() {
    use rivet::state::{KeysetRangePart, StateStore};
    let Some(s) = pg_store() else { return };
    let export = "pg_keyset_range_rt";
    s.clear_keyset_ranges(export).ok();

    let ranges = vec![
        (None, Some("k0500".to_string())),
        (Some("k0500".to_string()), None),
    ];
    // Binds range_index (i64) into the range_index column — WrongType on int4.
    s.persist_keyset_ranges(export, "run-1", "id", &ranges)
        .unwrap();
    // Reads range_index/done (i64) back — panics on int4.
    let loaded = s.load_keyset_ranges(export, "run-1", "id").unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(loaded.iter().all(|r| !r.done), "fresh ranges are not done");

    // Commit range 1 (a worker's atomic done-flip + file_log) over the PG StateRef.
    StateStore::commit_keyset_range_at_ref(
        s.state_ref(),
        "run-1",
        export,
        1,
        &[KeysetRangePart {
            file_name: "pk_w1_0.parquet".to_string(),
            rows: 7,
            bytes: 70,
        }],
        "parquet",
        None,
    )
    .unwrap();
    let after = s.load_keyset_ranges(export, "run-1", "id").unwrap();
    assert!(!after[0].done, "range 0 untouched");
    assert!(after[1].done, "range 1 committed → done");

    s.clear_keyset_ranges(export).ok();
}

#[test]
#[ignore]
fn pg_schema_drift_detection() {
    use rivet::state::SchemaColumn;

    let Some(s) = pg_store() else { return };
    // Unique per invocation: the fixed name persisted its stored schema on the
    // shared :5434 state db, so a SECOND --ignored run saw v2 already stored
    // and the "first run: no drift" assertion flipped (r6 bughunt).
    let export = crate::common::unique_name("pg_schema_drift");
    let export = export.as_str();

    let v1 = vec![
        SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        },
        SchemaColumn {
            name: "name".into(),
            data_type: "Utf8".into(),
        },
    ];
    let no_change = s.detect_schema_change(export, &v1).unwrap();
    assert!(no_change.is_none(), "first run: no drift");

    let v2 = vec![
        SchemaColumn {
            name: "id".into(),
            data_type: "Int64".into(),
        },
        SchemaColumn {
            name: "name".into(),
            data_type: "Utf8".into(),
        },
        SchemaColumn {
            name: "email".into(),
            data_type: "Utf8".into(),
        },
    ];
    let change = s.detect_schema_change(export, &v2).unwrap().unwrap();
    assert_eq!(change.added.len(), 1);
    assert!(change.added[0].contains("email"));

    s.store_schema(export, &v2).unwrap();
    let no_change2 = s.detect_schema_change(export, &v2).unwrap();
    assert!(no_change2.is_none(), "after store: no drift");
}

#[test]
#[ignore]
fn pg_metrics_record_and_query() {
    let Some(s) = pg_store() else { return };
    // Unique export name: record_metric_full is a plain INSERT and get_metrics
    // returns every row for the name, so a fixed name made a 2nd run see len==2
    // (r6 bughunt — fixed run_id on a shared state db).
    let metrics_export = crate::common::unique_name("pg_metrics");
    s.record_metric_full(&rivet::state::MetricRow {
        export_name: metrics_export.clone(),
        run_id: crate::common::unique_name("run_pg"),
        duration_ms: 1500,
        total_rows: 100_000,
        peak_rss_mb: Some(256),
        status: "success".to_string(),
        error_message: None,
        tuning_profile: Some("balanced".to_string()),
        format: Some("parquet".to_string()),
        mode: Some("full".to_string()),
        files_produced: 3,
        bytes_written: 1_048_576,
        retries: 0,
        validated: Some(true),
        schema_changed: Some(false),
        ..Default::default()
    })
    .unwrap();

    let metrics = s.get_metrics(Some(&metrics_export), 10).unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].status, "success");
    assert_eq!(metrics[0].total_rows, 100_000);
    assert_eq!(metrics[0].files_produced, 3);
    assert_eq!(metrics[0].validated, Some(true));
}

#[test]
#[ignore]
fn pg_chunk_checkpoint_claim_complete() {
    let url = match std::env::var("RIVET_TEST_STATE_URL") {
        Ok(u) if u.starts_with("postgres") => u,
        _ => return,
    };
    // Safety: test is single-threaded at this point.
    unsafe { std::env::set_var("RIVET_STATE_URL", &url) };
    let s = StateStore::open(":memory:").expect("open pg state store");
    unsafe { std::env::remove_var("RIVET_STATE_URL") };

    let run_id = format!("pg_test_run_{}", chrono::Utc::now().timestamp_micros());
    s.create_chunk_run(&run_id, "pg_orders", "hash_abc", 3)
        .unwrap();
    s.insert_chunk_tasks(&run_id, &[(0, 100), (101, 200), (201, 300)])
        .unwrap();

    let state_ref = s.state_ref().clone();
    let t0 = StateStore::claim_next_chunk_task_at_ref(&state_ref, &run_id)
        .unwrap()
        .expect("claim chunk 0");
    assert_eq!(t0.0, 0);

    s.complete_chunk_task(&run_id, 0, 100, Some("part0.parquet"))
        .unwrap();

    let t1 = StateStore::claim_next_chunk_task_at_ref(&state_ref, &run_id)
        .unwrap()
        .expect("claim chunk 1");
    assert_eq!(t1.0, 1);
    s.complete_chunk_task(&run_id, 1, 100, Some("part1.parquet"))
        .unwrap();

    let t2 = StateStore::claim_next_chunk_task_at_ref(&state_ref, &run_id)
        .unwrap()
        .expect("claim chunk 2");
    assert_eq!(t2.0, 2);
    s.complete_chunk_task(&run_id, 2, 99, Some("part2.parquet"))
        .unwrap();

    assert_eq!(s.count_chunk_tasks_not_completed(&run_id).unwrap(), 0);
    s.finalize_chunk_run_completed(&run_id).unwrap();

    // Cleanup
    s.reset_chunk_checkpoint("pg_orders").unwrap();
}

// ── v13/v14 load layer: the ledger + snapshot-completion on the Postgres arm ──
// These exercise the `StateConn::Postgres` branches of load_journal_store (v13)
// and cdc_snapshot_store (v14) — arms that the SQLite unit tests never run.

#[test]
#[ignore]
fn pg_load_ledger_round_trip() {
    use rivet::state::LoadRecord;
    let Some(s) = pg_store() else { return };
    let target = format!("pg.d.load_ledger_{}", chrono::Utc::now().timestamp_micros());
    let rec = LoadRecord {
        source_ident: "postgres:public.orders".into(),
        load_id: format!("Lpg_{}", chrono::Utc::now().timestamp_micros()),
        export_name: "pg_orders".into(),
        target_table: target.clone(),
        warehouse: "bigquery".into(),
        mode: "cdc".into(),
        source_run_ids: vec!["r1".into(), "r2".into()],
        rows_loaded: 100,
        status: "success".into(),
        finished_at: "2026-01-01T00:00:00Z".into(),
    };
    s.store_load(&rec).unwrap();

    let loaded = s.loaded_source_run_ids(&target).unwrap();
    assert!(
        loaded.contains("r1") && loaded.contains("r2"),
        "a successful load marks its runs loaded on Postgres"
    );
    let loads = s.recent_loads(Some(&target), 10).unwrap();
    assert_eq!(loads.len(), 1);
    assert_eq!(loads[0].rows_loaded, 100);
    assert_eq!(loads[0].source_run_ids, vec!["r1", "r2"]);

    // ON CONFLICT DO UPDATE — a replayed load never double-inserts.
    s.store_load(&rec).unwrap();
    assert_eq!(s.recent_loads(Some(&target), 10).unwrap().len(), 1);
    assert_eq!(s.loaded_source_run_ids(&target).unwrap().len(), 2);
}

#[test]
#[ignore]
fn pg_failed_load_leaves_runs_retryable() {
    use rivet::state::LoadRecord;
    let Some(s) = pg_store() else { return };
    let target = format!("pg.d.retry_{}", chrono::Utc::now().timestamp_micros());
    let rec = |id: &str, status: &str, rows: i64| LoadRecord {
        source_ident: String::new(),
        load_id: id.into(),
        export_name: "pg_orders".into(),
        target_table: target.clone(),
        warehouse: "bigquery".into(),
        mode: "cdc".into(),
        source_run_ids: vec!["rA".into(), "rB".into()],
        rows_loaded: rows,
        status: status.into(),
        finished_at: "2026-01-01T00:00:00Z".into(),
    };
    // A FAILED load records its audit row but marks NO runs loaded (the
    // data-loss guard) — verified here on the Postgres arm specifically.
    s.store_load(&rec("Lfail", "failed", 0)).unwrap();
    assert!(
        s.loaded_source_run_ids(&target).unwrap().is_empty(),
        "a failed load must leave its runs retryable on Postgres"
    );
    // A later SUCCESS over the same runs marks them (the retry landed).
    s.store_load(&rec("Lok", "success", 50)).unwrap();
    let loaded = s.loaded_source_run_ids(&target).unwrap();
    assert!(loaded.contains("rA") && loaded.contains("rB"));
}

#[test]
#[ignore]
fn pg_cdc_snapshot_completion_round_trip() {
    let Some(s) = pg_store() else { return };
    let export = format!("pg_snap_{}", chrono::Utc::now().timestamp_micros());
    assert!(
        !s.snapshot_done(&export, "t1", "gs://b/p/").unwrap(),
        "not done before mark"
    );
    s.mark_snapshot_done(&export, "t1", "gs://b/p/", "run_pg_1")
        .unwrap();
    assert!(
        s.snapshot_done(&export, "t1", "gs://b/p/").unwrap(),
        "done after mark"
    );
    assert!(
        !s.snapshot_done(&export, "t2", "gs://b/p/").unwrap(),
        "a different table is still not done"
    );
    // Idempotent upsert on (export, table).
    s.mark_snapshot_done(&export, "t1", "gs://b/p/", "run_pg_2")
        .unwrap();
    assert!(s.snapshot_done(&export, "t1", "gs://b/p/").unwrap());
}

/// The load spec round-trips on a Postgres state backend (RED on a CASE-bound parameter).
#[test]
#[ignore]
fn pg_load_spec_round_trips() {
    use rivet::state::LoadSpecColumn;
    use rivet::types::{RivetType, TypeFidelity};
    let Some(s) = pg_store() else { return };
    let export = "pg_load_spec_rt";
    let col = LoadSpecColumn {
        name: "id".into(),
        source_type: "int8".into(),
        rivet_type: RivetType::Int64,
        fidelity: TypeFidelity::Exact,
        nullable: false,
        warnings: Vec::new(),
    };
    let pk = vec!["id".to_string()];
    s.record_load_spec(
        export,
        None,
        std::slice::from_ref(&col),
        Some(&pk),
        "run_pg_1",
    )
    .expect("recording a load spec must succeed on Postgres");

    let spec = s
        .load_spec(export, None)
        .unwrap()
        .expect("the spec must read back");
    assert_eq!(spec.columns, vec![col.clone()]);
    assert_eq!(spec.primary_key, Some(pk));
    assert_eq!(spec.run_id.as_deref(), Some("run_pg_1"));
    assert_eq!(spec.origin, "run");

    // The UPDATE arms carry the same shape: a later capture with no key clears a key
    // a RUN recorded (the export stopped reading that relation).
    s.record_load_spec(export, None, std::slice::from_ref(&col), None, "run_pg_2")
        .expect("the keyless capture must also succeed");
    assert_eq!(
        s.load_spec(export, None).unwrap().unwrap().primary_key,
        None
    );
}

/// A database created for one test and dropped when it ends, however it ends.
struct ScratchDb {
    admin_url: String,
    name: String,
}

impl ScratchDb {
    /// `CREATE DATABASE` on the server `admin_url` points at, returning `None`
    /// when the url is not one this can take apart.
    fn create(admin_url: &str, name: &str) -> Option<Self> {
        admin_url.rsplit_once('/')?;
        let mut admin = postgres::Client::connect(admin_url, postgres::NoTls).ok()?;
        let _ = admin.batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE);"));
        admin
            .batch_execute(&format!("CREATE DATABASE {name};"))
            .unwrap_or_else(|e| panic!("creating the scratch database {name}: {e:#}"));
        Some(Self {
            admin_url: admin_url.to_string(),
            name: name.to_string(),
        })
    }

    fn url(&self) -> String {
        let (base, _) = self.admin_url.rsplit_once('/').expect("checked in create");
        format!("{base}/{}", self.name)
    }
}

impl Drop for ScratchDb {
    fn drop(&mut self) {
        if let Ok(mut admin) = postgres::Client::connect(&self.admin_url, postgres::NoTls) {
            let _ = admin.batch_execute(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE);",
                self.name
            ));
        }
    }
}

/// PARITY, and the half the roast left open: several writers migrating ONE
/// Postgres state database at once all succeed.
///
/// The SQLite sibling (`several_writers_migrating_one_database_at_once_all_succeed`,
/// `src/state/migrations.rs`) can live inline because its race fits in a tempdir. This one
/// needs the stand, which is exactly why it was missing while the guard it grades —
/// `pg_advisory_lock(PG_MIGRATION_LOCK)` in `migrate_pg` — carried a MEASUREMENT in
/// its own comment and no test: four concurrent exports against an empty schema,
/// three of the four dead at the very first statement with `state(pg): create
/// version table`. `rivet load --pool 16` leans on it sixteen times harder.
///
/// Two fixture facts decide whether this grades anything at all:
///
/// 1. **The database must be FRESH.** Migrating an already-migrated database is a
///    no-op ladder, so the writers never contend and the test passes against a
///    deleted lock. Hence the scratch database rather than the gate's own, which
///    has been migrated since the stand came up.
/// 2. **Open, THEN line up.** The barrier sits after `Client::connect` so the
///    contention under test is the MIGRATION, not the TCP handshake.
///
/// The oracle is two-sided because the guard fails in two directions. All four
/// writers returning `Ok` is the first side. The second is the version ladder
/// itself: `migrate_pg_locked` reads `MAX(version)` and INSERTS a row per applied
/// migration, so two clients that both read version N and both applied N+1 leave
/// TWO rows for N+1 — a duplicate is the signature of the double-apply the lock
/// exists to prevent, and it survives even when both clients report success.
#[test]
#[ignore]
fn pg_several_writers_migrating_one_database_at_once_all_succeed() {
    use rivet::state::{StateRef, StateStore};
    const WRITERS: usize = 4;

    let Ok(admin_url) = std::env::var("RIVET_TEST_STATE_URL") else {
        return;
    };
    if !admin_url.starts_with("postgres") {
        return;
    }
    let name = format!(
        "rivet_migrace_{}",
        chrono::Utc::now().timestamp_micros().unsigned_abs()
    );
    let Some(scratch) = ScratchDb::create(&admin_url, &name) else {
        return;
    };
    let url = scratch.url();

    let start = std::sync::Barrier::new(WRITERS);
    let results: Vec<anyhow::Result<()>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..WRITERS)
            .map(|_| {
                let url = url.clone();
                let start = &start;
                s.spawn(move || {
                    // Line up AFTER the connect, so the overlap is the migration.
                    let conn = postgres::Client::connect(&url, postgres::NoTls)
                        .map_err(|e| anyhow::anyhow!("connect: {e:#}"))?;
                    drop(conn);
                    start.wait();
                    // `open_at_ref` is the seam the pool's workers use, and it
                    // migrates inside — so this races the real entry point, not a
                    // private helper.
                    StateStore::open_at_ref(&StateRef::Postgres(url.clone())).map(|_| ())
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("a writer thread"))
            .collect()
    });

    for (i, r) in results.iter().enumerate() {
        assert!(
            r.is_ok(),
            "writer {i} of {WRITERS} failed to migrate a shared Postgres state database: {:?}",
            r.as_ref().err()
        );
    }

    let mut check = postgres::Client::connect(&url, postgres::NoTls).expect("read the ladder back");
    let dupes: Vec<(i64, i64)> = check
        .query(
            "SELECT version, COUNT(*) FROM rivet_schema_version \
             GROUP BY version HAVING COUNT(*) > 1 ORDER BY version",
            &[],
        )
        .expect("group the version ladder")
        .iter()
        .map(|r| (r.get(0), r.get::<_, i64>(1)))
        .collect();
    assert!(
        dupes.is_empty(),
        "every migration must be applied exactly ONCE however many writers raced; \
         these versions have duplicate rows (version, times applied): {dupes:?}"
    );
}
