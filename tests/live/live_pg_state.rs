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

#[test]
#[ignore]
fn pg_cursor_round_trip() {
    let Some(s) = pg_store() else { return };

    s.update("pg_orders", "2024-06-01").unwrap();
    let got = s.get("pg_orders").unwrap();
    assert_eq!(got.last_cursor_value.as_deref(), Some("2024-06-01"));

    s.update("pg_orders", "2024-07-01").unwrap();
    let got2 = s.get("pg_orders").unwrap();
    assert_eq!(got2.last_cursor_value.as_deref(), Some("2024-07-01"));

    s.reset("pg_orders").unwrap();
    let empty = s.get("pg_orders").unwrap();
    assert!(empty.last_cursor_value.is_none());
}

#[test]
#[ignore]
fn pg_schema_drift_detection() {
    use rivet::state::SchemaColumn;

    let Some(s) = pg_store() else { return };
    let export = "pg_schema_drift_test";

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

    s.record_metric(
        "pg_metrics_export",
        "run_pg_001",
        1500,
        100_000,
        Some(256),
        "success",
        None,
        Some("balanced"),
        Some("parquet"),
        Some("full"),
        3,
        1_048_576,
        0,
        Some(true),
        Some(false),
    )
    .unwrap();

    let metrics = s.get_metrics(Some("pg_metrics_export"), 10).unwrap();
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
        !s.snapshot_done(&export, "t1").unwrap(),
        "not done before mark"
    );
    s.mark_snapshot_done(&export, "t1", "run_pg_1").unwrap();
    assert!(s.snapshot_done(&export, "t1").unwrap(), "done after mark");
    assert!(
        !s.snapshot_done(&export, "t2").unwrap(),
        "a different table is still not done"
    );
    // Idempotent upsert on (export, table).
    s.mark_snapshot_done(&export, "t1", "run_pg_2").unwrap();
    assert!(s.snapshot_done(&export, "t1").unwrap());
}
