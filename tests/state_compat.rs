//! State database compatibility and fault-tolerance tests.
//!
//! QA backlog:
//!   * **Task 1.3** — Corrupted state handling (fault injection against the
//!     physical SQLite file).
//!   * **Task 1.4** — Cross-version state compatibility (legacy DB fixtures,
//!     reopening, idempotent migrations).
//!
//! All tests operate through the public `StateStore` API so they simultaneously
//! verify that `StateStore::open_at_path` is a safe entry point for arbitrary
//! on-disk files (user may point it at anything).

use std::io::Write;

use rivet::state::StateStore;

// =============================================================================
// Task 1.3 — Corrupted state handling
// =============================================================================
//
// Acceptance criteria:
//   - Tool fails fast with actionable error, or recovers in a defined way.
//   - No panic or undefined behavior.

#[test]
fn random_bytes_as_state_db_produces_error_not_panic() {
    // Write garbage bytes to a file and try to open it as a state DB.
    // The test harness auto-fails on panic; either Err or a healed Ok is fine.
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(&[0u8; 1024]).unwrap();
    tmp.write_all(b"definitely not sqlite").unwrap();
    tmp.flush().unwrap();

    let _ = StateStore::open_at_path(tmp.path());
}

#[test]
fn truncated_sqlite_header_is_rejected_cleanly() {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(b"SQLite f").unwrap(); // half of "SQLite format 3\0"
    tmp.flush().unwrap();

    let res = StateStore::open_at_path(tmp.path());
    assert!(
        res.is_err(),
        "opening a truncated SQLite file must not silently succeed"
    );
}

#[test]
fn existing_db_missing_required_tables_is_healed_by_migration() {
    // Create a SQLite DB with an unrelated table, then open through StateStore.
    // Migration must add the required tables and the store must be usable.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch("CREATE TABLE unrelated (x INTEGER);")
            .unwrap();
    }

    let store = StateStore::open_at_path(tmp.path())
        .expect("migrations must heal a DB that lacks the required tables");
    store.update("heal_test", "v1").unwrap();
    assert_eq!(
        store.get("heal_test").unwrap().last_cursor_value.as_deref(),
        Some("v1")
    );
}

#[test]
fn corrupted_cursor_row_is_handled_without_panic() {
    // Seed the DB through the public API, then poke a NULL into a column that
    // StateStore::get expects non-null in some rusqlite versions.  Acceptable
    // outcomes: Ok(None-cursor) or Err; a panic would be a regression (the
    // test harness converts panics to failures).
    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let store = StateStore::open_at_path(tmp.path()).unwrap();
        store.update("orders", "2024-06-01").unwrap();
    }
    {
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute(
            "UPDATE export_state SET last_cursor_value = NULL WHERE export_name = ?1",
            rusqlite::params!["orders"],
        )
        .unwrap();
    }

    let store = StateStore::open_at_path(tmp.path()).unwrap();
    if let Ok(state) = store.get("orders") {
        assert!(
            state.last_cursor_value.is_none(),
            "NULL cursor in DB must surface as None, not a bogus value"
        );
    }
}

// =============================================================================
// Task 1.4 — Cross-version state compatibility
// =============================================================================
//
// Acceptance criteria:
//   - Upgrade path is supported and tested, OR intentionally blocked with
//     clear messaging.
//   - Compatibility expectations are explicit.

#[test]
fn fresh_db_opens_and_is_functional_end_to_end() {
    // Baseline: a fresh DB must be at the current version and support the
    // full public API with no errors.  If a future migration forgets to
    // update this path, the test fails loudly.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let store = StateStore::open_at_path(tmp.path()).unwrap();

    store.update("orders", "2024-01-01T00:00:00Z").unwrap();
    store
        .record_file(
            "run-1",
            "orders",
            "orders_202401.parquet",
            100,
            1024,
            "parquet",
            Some("zstd"),
        )
        .unwrap();
    assert_eq!(
        store.get("orders").unwrap().last_cursor_value.as_deref(),
        Some("2024-01-01T00:00:00Z")
    );
    assert_eq!(store.get_files(Some("orders"), 10).unwrap().len(), 1);
}

#[test]
fn reopening_an_existing_db_is_idempotent() {
    // Migrations that forget IF NOT EXISTS / IF EXISTS guards would fail on
    // the second open; existing data must survive.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let s = StateStore::open_at_path(tmp.path()).unwrap();
        s.update("orders", "v1").unwrap();
    }
    {
        let s = StateStore::open_at_path(tmp.path()).unwrap();
        assert_eq!(
            s.get("orders").unwrap().last_cursor_value.as_deref(),
            Some("v1"),
            "reopening must preserve data"
        );
        s.update("orders", "v2").unwrap();
    }
    {
        let s = StateStore::open_at_path(tmp.path()).unwrap();
        assert_eq!(
            s.get("orders").unwrap().last_cursor_value.as_deref(),
            Some("v2")
        );
    }
}

#[test]
fn legacy_preversioned_db_is_migrated_and_preserves_existing_rows() {
    // Simulate a DB created by a very old rivet version: only v1 tables,
    // no schema_version table, a couple of existing rows.  After opening
    // through StateStore the data must be readable and newer-version tables
    // (chunk_run, chunk_task, export_progression) must be present.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE export_state (
                export_name TEXT PRIMARY KEY,
                last_cursor_value TEXT,
                last_run_at TEXT
            );
            CREATE TABLE export_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_name TEXT NOT NULL,
                run_at TEXT NOT NULL,
                duration_ms INTEGER NOT NULL,
                total_rows INTEGER NOT NULL,
                status TEXT NOT NULL
            );
            INSERT INTO export_state (export_name, last_cursor_value, last_run_at)
            VALUES ('legacy', 'v-from-old-binary', '2024-01-01T00:00:00Z');",
        )
        .unwrap();
    }

    let store = StateStore::open_at_path(tmp.path()).unwrap();
    assert_eq!(
        store.get("legacy").unwrap().last_cursor_value.as_deref(),
        Some("v-from-old-binary"),
        "legacy cursor rows must survive migration"
    );

    // Newer-version features (chunk checkpoint) are usable after migration.
    store
        .create_chunk_run("run-new", "legacy", "hash", 3)
        .unwrap();
    store.insert_chunk_tasks("run-new", &[(0, 10)]).unwrap();
    assert!(
        store.claim_next_chunk_task("run-new").unwrap().is_some(),
        "v2 chunk_task features must be usable after migration"
    );
}

#[test]
fn db_with_only_schema_version_table_upgrades_cleanly() {
    // Regression guard for a partial migration state: `schema_version` exists
    // but no migrations have been applied.  migrate() must treat this as
    // version 0 and run all migrations.
    let tmp = tempfile::NamedTempFile::new().unwrap();
    {
        let conn = rusqlite::Connection::open(tmp.path()).unwrap();
        conn.execute_batch("CREATE TABLE schema_version (version INTEGER NOT NULL);")
            .unwrap();
    }
    let store = StateStore::open_at_path(tmp.path()).unwrap();
    store.update("x", "y").unwrap();
    assert_eq!(
        store.get("x").unwrap().last_cursor_value.as_deref(),
        Some("y")
    );
}
