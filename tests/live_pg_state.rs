//! Live integration tests for the PostgreSQL state backend.
//!
//! These tests require a running PostgreSQL instance.  They are skipped by
//! default (`#[ignore]`) and opt-in via:
//!
//! ```text
//! RIVET_TEST_STATE_URL=postgresql://user:pass@localhost/rivet_state_test \
//!   cargo test -p rivet-cli --test live_pg_state -- --ignored
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
