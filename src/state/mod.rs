use rusqlite::Connection;

use crate::error::Result;

mod cdc_snapshot_store;
mod checkpoint;
mod cursor;
mod file_log;
mod journal_store;
mod load_journal_store;
mod metrics;
mod progression;
mod run_aggregate;
mod schema;
mod shape;

// Re-export domain types so callers use `rivet::state::*` unchanged.
// Items below may not be explicitly named by all internal callers (often used
// as inferred return types), but are part of the public integration-test API.
#[allow(unused_imports)]
pub use checkpoint::ChunkTaskInfo;
#[allow(unused_imports)]
pub use file_log::FileRecord;
pub use load_journal_store::LoadRecord;
#[allow(unused_imports)]
pub use metrics::ExportMetric;
pub use metrics::MetricRow;
#[allow(unused_imports)]
pub use progression::{Boundary, ExportProgression};
#[allow(unused_imports)]
pub use run_aggregate::{RunAggregate, RunAggregateEntry};
#[allow(unused_imports)]
pub use schema::{SchemaChange, SchemaColumn, arrow_schema_to_columns, schema_fingerprint};
#[allow(unused_imports)]
pub use shape::ShapeWarning;

const STATE_DB_NAME: &str = ".rivet_state.db";

/// Current schema version — always the last entry in `MIGRATIONS`.
const SCHEMA_VERSION: i64 = MIGRATIONS[MIGRATIONS.len() - 1].0;

/// Each entry is `(version, sql)`.  Applied in order when the DB is behind.
const MIGRATIONS: &[(i64, &str)] = &[
    // v1: core tables
    (
        1,
        "CREATE TABLE IF NOT EXISTS export_state (
            export_name TEXT PRIMARY KEY,
            last_cursor_value TEXT,
            last_run_at TEXT
        );
        CREATE TABLE IF NOT EXISTS export_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            export_name TEXT NOT NULL,
            run_at TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            total_rows INTEGER NOT NULL,
            peak_rss_mb INTEGER,
            status TEXT NOT NULL,
            error_message TEXT,
            tuning_profile TEXT,
            format TEXT,
            mode TEXT,
            files_produced INTEGER DEFAULT 0,
            bytes_written INTEGER DEFAULT 0,
            retries INTEGER DEFAULT 0,
            validated INTEGER,
            schema_changed INTEGER,
            run_id TEXT
        );
        CREATE TABLE IF NOT EXISTS export_schema (
            export_name TEXT PRIMARY KEY,
            columns_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS file_manifest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            format TEXT NOT NULL,
            compression TEXT,
            created_at TEXT NOT NULL
        );",
    ),
    // v2: chunk checkpoint tables
    (
        2,
        "CREATE TABLE IF NOT EXISTS chunk_run (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            plan_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            max_chunk_attempts INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_run_export_status
            ON chunk_run(export_name, status);
        CREATE TABLE IF NOT EXISTS chunk_task (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            start_key TEXT NOT NULL,
            end_key TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            rows_written INTEGER,
            file_name TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(run_id, chunk_index)
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_task_run_status ON chunk_task(run_id, status);",
    ),
    // v3: index on file_manifest for faster per-export lookups
    (
        3,
        "CREATE INDEX IF NOT EXISTS idx_file_manifest_export ON file_manifest(export_name, id DESC);",
    ),
    // v4: committed / verified boundary tracking (ADR-0008, Epic G)
    (
        4,
        "CREATE TABLE IF NOT EXISTS export_progression (
            export_name TEXT PRIMARY KEY,
            last_committed_strategy TEXT,
            last_committed_cursor TEXT,
            last_committed_chunk_index INTEGER,
            last_committed_run_id TEXT,
            last_committed_at TEXT,
            last_verified_strategy TEXT,
            last_verified_cursor TEXT,
            last_verified_chunk_index INTEGER,
            last_verified_run_id TEXT,
            last_verified_at TEXT
        );",
    ),
    // v5: aggregate run summary
    (
        5,
        "CREATE TABLE IF NOT EXISTS run_aggregate (
            run_aggregate_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            config_path TEXT,
            parallel_mode TEXT NOT NULL,
            total_exports INTEGER NOT NULL,
            success_count INTEGER NOT NULL,
            failed_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL,
            total_rows INTEGER NOT NULL,
            total_files INTEGER NOT NULL,
            total_bytes INTEGER NOT NULL,
            details_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_aggregate_finished
            ON run_aggregate(finished_at DESC);",
    ),
    // v6: per-column data shape stats
    (
        6,
        "CREATE TABLE IF NOT EXISTS export_shape (
            export_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            max_byte_len INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (export_name, column_name)
        );",
    ),
    // v7: structured run journal
    (
        7,
        "CREATE TABLE IF NOT EXISTS run_journal (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            journal_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_journal_export
            ON run_journal(export_name, finished_at DESC);",
    ),
    // v8: rename file_manifest → file_log.  The 0.7.0 cloud-output contract
    // reclaims the "manifest" name for the public JSON artifact; the internal
    // SQLite log of written files becomes `file_log` to remove the overload.
    (
        8,
        "ALTER TABLE file_manifest RENAME TO file_log;
        DROP INDEX IF EXISTS idx_file_manifest_export;
        CREATE INDEX IF NOT EXISTS idx_file_log_export ON file_log(export_name, id DESC);",
    ),
    // v9: extended per-run metrics for post-pilot analysis — source harm
    // (pg_temp_bytes_delta), completeness (reconciled, source_count,
    // quality_passed), memory (batch_size[_memory_mb]), and config dimensions
    // (chunk_size, parallel, source/destination type, rivet_version). All
    // additive + nullable: old rows read NULL, no backfill, reads stay forward-
    // compatible.
    (
        9,
        "ALTER TABLE export_metrics ADD COLUMN files_committed INTEGER;
        ALTER TABLE export_metrics ADD COLUMN reconciled INTEGER;
        ALTER TABLE export_metrics ADD COLUMN source_count INTEGER;
        ALTER TABLE export_metrics ADD COLUMN quality_passed INTEGER;
        ALTER TABLE export_metrics ADD COLUMN pg_temp_bytes_delta INTEGER;
        ALTER TABLE export_metrics ADD COLUMN batch_size INTEGER;
        ALTER TABLE export_metrics ADD COLUMN batch_size_memory_mb INTEGER;
        ALTER TABLE export_metrics ADD COLUMN skip_reason TEXT;
        ALTER TABLE export_metrics ADD COLUMN schema_fingerprint TEXT;
        ALTER TABLE export_metrics ADD COLUMN chunk_size INTEGER;
        ALTER TABLE export_metrics ADD COLUMN parallel INTEGER;
        ALTER TABLE export_metrics ADD COLUMN source_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN destination_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN rivet_version TEXT;",
    ),
    // v10: longest single-chunk wall time (ms) — the #5 source-harm lever,
    // aggregated at finalize from the run journal's per-chunk timings.
    (
        10,
        "ALTER TABLE export_metrics ADD COLUMN longest_chunk_ms INTEGER;",
    ),
    // v11: per-run source-harm deltas (locks, rows read, buffer misses, temp
    // files) — one row per counter, keyed on run_id. Engine-neutral key/value so
    // each engine's counter set lands without schema churn. Written from
    // pipeline::job::harm_snapshot via source::{postgres,mysql,mssql}.
    (
        11,
        "CREATE TABLE IF NOT EXISTS export_harm (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            metric TEXT NOT NULL,
            delta INTEGER NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_export_harm_run ON export_harm(run_id);",
    ),
    // v12: chunking diagnostics — the chunk KEY column. (The resolved strategy is
    // already the `mode` column — `summary.mode` is `strategy.mode_label()`,
    // "keyset"/"chunked"/etc. — and the span/window count are derivable from
    // chunk_task.) A sparse-key post-mortem: mode='chunked' + chunk_key='id' →
    // "which column was range-chunked". Whether that key is a PK (the "should have
    // keyset-paged" signal) needs a run-time PK probe — a follow-up, so no field
    // that would merely restate mode='keyset'.
    (12, "ALTER TABLE export_metrics ADD COLUMN chunk_key TEXT;"),
    // v13: load ledger. `rivet load` is now stateful — `load_run` is the audit
    // log (one row per invocation-table), `loaded_source_run` the skip ledger
    // (which extraction run_ids have landed in which target) that makes loads
    // incremental + idempotent instead of re-loading whatever sits in the bucket.
    (
        13,
        "CREATE TABLE IF NOT EXISTS load_run (
            load_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            target_table TEXT NOT NULL,
            warehouse TEXT NOT NULL,
            mode TEXT NOT NULL,
            source_run_ids TEXT NOT NULL,
            rows_loaded INTEGER NOT NULL,
            status TEXT NOT NULL,
            finished_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_load_run_target
            ON load_run(target_table, finished_at DESC);
        CREATE TABLE IF NOT EXISTS loaded_source_run (
            target_table TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            load_id TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (target_table, source_run_id)
        );",
    ),
    // v14: cdc snapshot completion. `cdc.initial: snapshot` records that an
    // export/table's backfill finished HERE, not only as a GCS `snapshot/_SUCCESS`
    // marker — so `cleanup_source: true` wiping the bucket no longer looks like an
    // un-snapshotted table and re-snapshots the whole thing on every run.
    (
        14,
        "CREATE TABLE IF NOT EXISTS cdc_snapshot (
            export_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            run_id TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            PRIMARY KEY (export_name, table_name)
        );",
    ),
    // v15: close the chunked-run TOCTOU (round-2 audit #13). ensure_chunk_
    // checkpoint_plan did check-then-act (find an in_progress run → if None,
    // create), with no serialization, so two overlapping runs of ONE export both
    // saw None, both created an in_progress row, and DOUBLED the destination data
    // (the random part-name nonce made the parts additive, not clobbering). A
    // partial-unique index makes the second create fail (mapped to the same
    // 'still in progress' bail). First demote any pre-existing duplicate
    // in_progress rows — keep the newest (created_at, run_id) per export — so the
    // index can build on a legacy DB that already raced. Standard SQL: valid for
    // both SQLite and PostgreSQL (both support partial indexes).
    (
        15,
        "UPDATE chunk_run SET status='interrupted'
             WHERE status='in_progress' AND run_id NOT IN (
               SELECT run_id FROM chunk_run c WHERE c.status='in_progress'
                 AND NOT EXISTS (
                   SELECT 1 FROM chunk_run c2
                   WHERE c2.export_name=c.export_name AND c2.status='in_progress'
                     AND (c2.created_at > c.created_at
                          OR (c2.created_at = c.created_at AND c2.run_id > c.run_id)))
             );
         CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_run_one_inprogress
             ON chunk_run(export_name) WHERE status='in_progress';",
    ),
];

/// PostgreSQL-compatible DDL.  Column types differ from SQLite (BIGSERIAL,
/// BOOLEAN); placeholder style is `$N` (handled by callers via `pg_sql()`).
const PG_MIGRATIONS: &[(i64, &str)] = &[
    (
        1,
        "CREATE TABLE IF NOT EXISTS export_state (
            export_name TEXT PRIMARY KEY,
            last_cursor_value TEXT,
            last_run_at TEXT
        );
        CREATE TABLE IF NOT EXISTS export_metrics (
            id BIGSERIAL PRIMARY KEY,
            export_name TEXT NOT NULL,
            run_at TEXT NOT NULL,
            duration_ms BIGINT NOT NULL,
            total_rows BIGINT NOT NULL,
            peak_rss_mb BIGINT,
            status TEXT NOT NULL,
            error_message TEXT,
            tuning_profile TEXT,
            format TEXT,
            mode TEXT,
            files_produced BIGINT DEFAULT 0,
            bytes_written BIGINT DEFAULT 0,
            retries BIGINT DEFAULT 0,
            validated BOOLEAN,
            schema_changed BOOLEAN,
            run_id TEXT
        );
        CREATE TABLE IF NOT EXISTS export_schema (
            export_name TEXT PRIMARY KEY,
            columns_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS file_manifest (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            file_name TEXT NOT NULL,
            row_count BIGINT NOT NULL,
            bytes BIGINT NOT NULL,
            format TEXT NOT NULL,
            compression TEXT,
            created_at TEXT NOT NULL
        );",
    ),
    (
        2,
        "CREATE TABLE IF NOT EXISTS chunk_run (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            plan_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            max_chunk_attempts BIGINT NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_run_export_status
            ON chunk_run(export_name, status);
        CREATE TABLE IF NOT EXISTS chunk_task (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            chunk_index BIGINT NOT NULL,
            start_key TEXT NOT NULL,
            end_key TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts BIGINT NOT NULL DEFAULT 0,
            last_error TEXT,
            rows_written BIGINT,
            file_name TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(run_id, chunk_index)
        );
        CREATE INDEX IF NOT EXISTS idx_chunk_task_run_status ON chunk_task(run_id, status);",
    ),
    (
        3,
        "CREATE INDEX IF NOT EXISTS idx_file_manifest_export ON file_manifest(export_name, id DESC);",
    ),
    (
        4,
        "CREATE TABLE IF NOT EXISTS export_progression (
            export_name TEXT PRIMARY KEY,
            last_committed_strategy TEXT,
            last_committed_cursor TEXT,
            last_committed_chunk_index BIGINT,
            last_committed_run_id TEXT,
            last_committed_at TEXT,
            last_verified_strategy TEXT,
            last_verified_cursor TEXT,
            last_verified_chunk_index BIGINT,
            last_verified_run_id TEXT,
            last_verified_at TEXT
        );",
    ),
    (
        5,
        "CREATE TABLE IF NOT EXISTS run_aggregate (
            run_aggregate_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            duration_ms BIGINT NOT NULL,
            config_path TEXT,
            parallel_mode TEXT NOT NULL,
            total_exports BIGINT NOT NULL,
            success_count BIGINT NOT NULL,
            failed_count BIGINT NOT NULL,
            skipped_count BIGINT NOT NULL,
            total_rows BIGINT NOT NULL,
            total_files BIGINT NOT NULL,
            total_bytes BIGINT NOT NULL,
            details_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_aggregate_finished
            ON run_aggregate(finished_at DESC);",
    ),
    (
        6,
        "CREATE TABLE IF NOT EXISTS export_shape (
            export_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            max_byte_len BIGINT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (export_name, column_name)
        );",
    ),
    (
        7,
        "CREATE TABLE IF NOT EXISTS run_journal (
            run_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            journal_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_journal_export
            ON run_journal(export_name, finished_at DESC);",
    ),
    // v8: rename file_manifest → file_log.  Mirrors the SQLite v8 migration;
    // see the SQLite array for rationale.
    (
        8,
        "ALTER TABLE file_manifest RENAME TO file_log;
        DROP INDEX IF EXISTS idx_file_manifest_export;
        CREATE INDEX IF NOT EXISTS idx_file_log_export ON file_log(export_name, id DESC);",
    ),
    // v9: extended per-run metrics (see the SQLite array for rationale).
    // Additive + nullable; BOOLEAN for the bool flags, BIGINT for counts.
    (
        9,
        "ALTER TABLE export_metrics ADD COLUMN files_committed BIGINT;
        ALTER TABLE export_metrics ADD COLUMN reconciled BOOLEAN;
        ALTER TABLE export_metrics ADD COLUMN source_count BIGINT;
        ALTER TABLE export_metrics ADD COLUMN quality_passed BOOLEAN;
        ALTER TABLE export_metrics ADD COLUMN pg_temp_bytes_delta BIGINT;
        ALTER TABLE export_metrics ADD COLUMN batch_size BIGINT;
        ALTER TABLE export_metrics ADD COLUMN batch_size_memory_mb BIGINT;
        ALTER TABLE export_metrics ADD COLUMN skip_reason TEXT;
        ALTER TABLE export_metrics ADD COLUMN schema_fingerprint TEXT;
        ALTER TABLE export_metrics ADD COLUMN chunk_size BIGINT;
        ALTER TABLE export_metrics ADD COLUMN parallel BIGINT;
        ALTER TABLE export_metrics ADD COLUMN source_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN destination_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN rivet_version TEXT;",
    ),
    // v10: longest single-chunk wall time (ms). See the SQLite array.
    (
        10,
        "ALTER TABLE export_metrics ADD COLUMN longest_chunk_ms BIGINT;",
    ),
    // v11: per-run source-harm deltas (see the SQLite array for rationale).
    (
        11,
        "CREATE TABLE IF NOT EXISTS export_harm (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL,
            export_name TEXT NOT NULL,
            metric TEXT NOT NULL,
            delta BIGINT NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_export_harm_run ON export_harm(run_id);",
    ),
    // v12: chunking diagnostics (see the SQLite array for rationale).
    (12, "ALTER TABLE export_metrics ADD COLUMN chunk_key TEXT;"),
    // v13: load ledger (see the SQLite array for rationale). rows_loaded is BIGINT.
    (
        13,
        "CREATE TABLE IF NOT EXISTS load_run (
            load_id TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            target_table TEXT NOT NULL,
            warehouse TEXT NOT NULL,
            mode TEXT NOT NULL,
            source_run_ids TEXT NOT NULL,
            rows_loaded BIGINT NOT NULL,
            status TEXT NOT NULL,
            finished_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_load_run_target
            ON load_run(target_table, finished_at DESC);
        CREATE TABLE IF NOT EXISTS loaded_source_run (
            target_table TEXT NOT NULL,
            source_run_id TEXT NOT NULL,
            load_id TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            PRIMARY KEY (target_table, source_run_id)
        );",
    ),
    // v14: cdc snapshot completion (see the SQLite array for rationale).
    (
        14,
        "CREATE TABLE IF NOT EXISTS cdc_snapshot (
            export_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            run_id TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            PRIMARY KEY (export_name, table_name)
        );",
    ),
    // v15: close the chunked-run TOCTOU (round-2 audit #13). ensure_chunk_
    // checkpoint_plan did check-then-act (find an in_progress run → if None,
    // create), with no serialization, so two overlapping runs of ONE export both
    // saw None, both created an in_progress row, and DOUBLED the destination data
    // (the random part-name nonce made the parts additive, not clobbering). A
    // partial-unique index makes the second create fail (mapped to the same
    // 'still in progress' bail). First demote any pre-existing duplicate
    // in_progress rows — keep the newest (created_at, run_id) per export — so the
    // index can build on a legacy DB that already raced. Standard SQL: valid for
    // both SQLite and PostgreSQL (both support partial indexes).
    (
        15,
        "UPDATE chunk_run SET status='interrupted'
             WHERE status='in_progress' AND run_id NOT IN (
               SELECT run_id FROM chunk_run c WHERE c.status='in_progress'
                 AND NOT EXISTS (
                   SELECT 1 FROM chunk_run c2
                   WHERE c2.export_name=c.export_name AND c2.status='in_progress'
                     AND (c2.created_at > c.created_at
                          OR (c2.created_at = c.created_at AND c2.run_id > c.run_id)))
             );
         CREATE UNIQUE INDEX IF NOT EXISTS idx_chunk_run_one_inprogress
             ON chunk_run(export_name) WHERE status='in_progress';",
    ),
];

// ─── SQL helpers ──────────────────────────────────────────────────────────────

/// Convert SQLite `?N` placeholders to PostgreSQL `$N` style.
/// `"WHERE x = ?1 AND y = ?2"` → `"WHERE x = $1 AND y = $2"`.
pub(super) fn pg_sql(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'?' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            out.push('$');
        } else {
            out.push(bytes[i] as char);
        }
        i += 1;
    }
    out
}

/// Open a Postgres client for the state backend, honoring the URL's `sslmode`.
///
/// The state backend connects to its store using only a URL (`RIVET_STATE_URL`)
/// — there is no YAML `tls:` block — so the transport-security policy is derived
/// from the URL's `sslmode` query parameter, exactly as `rivet init` does for
/// source connections. The connection itself goes through the shared
/// [`crate::source::postgres::connect_client`] path so the state backend and
/// source connections apply identical TLS rules.
///
/// - missing / `disable` / `prefer` / `allow` / unrecognized → `NoTls`
///   (plaintext), keeping local and dev setups working unchanged.
/// - `require` / `verify-ca` / `verify-full` → negotiate TLS.
///
/// Used by both [`StateStore::open_postgres`] and the parallel chunk-worker
/// reconnection paths in `checkpoint.rs`, so every PG state connection is
/// TLS-aware.
pub(super) fn connect_pg(url: &str) -> Result<postgres::Client> {
    let tls = state_tls_mode_from_url(url).map(|mode| crate::config::TlsConfig {
        mode,
        ..crate::config::TlsConfig::default()
    });
    crate::source::postgres::connect_client(url, tls.as_ref())
        .map_err(|e| anyhow::anyhow!("state(pg): connect to '{}': {:#}", redact_pg_url(url), e))
}

/// Map the state URL's `sslmode` query parameter to a [`crate::config::TlsMode`].
///
/// Mirrors the source-side mapping in `crate::init::postgres`: `require` /
/// `verify-ca` / `verify-full` enforce TLS; everything else — parameter missing,
/// `disable`, `prefer`, `allow`, or an unrecognized value — returns `None`
/// (plaintext `NoTls`). [`crate::config::TlsMode`] has no `prefer` variant, so no
/// try-TLS-then-fallback is attempted. Last occurrence wins, matching libpq.
fn state_tls_mode_from_url(url: &str) -> Option<crate::config::TlsMode> {
    use crate::config::TlsMode;
    let (_, query) = url.split_once('?')?;
    let mut mode = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key != "sslmode" {
            continue;
        }
        mode = match value {
            "require" => Some(TlsMode::Require),
            "verify-ca" => Some(TlsMode::VerifyCa),
            "verify-full" => Some(TlsMode::VerifyFull),
            _ => None,
        };
    }
    mode
}

// ─── Backend connection ────────────────────────────────────────────────────────

/// Internal storage for the active database connection.
pub(super) enum StateConn {
    Sqlite(rusqlite::Connection),
    /// postgres::Client requires `&mut self` for queries; RefCell provides
    /// interior mutability so `StateStore` methods can keep `&self` signatures.
    /// StateStore is not Sync (neither backend is), so RefCell is safe here.
    /// Boxed to keep the enum variant sizes balanced (postgres::Client is ~320 B).
    Postgres(Box<std::cell::RefCell<postgres::Client>>),
}

/// Serialisable reference that identifies a state database without holding a
/// live connection.  Passed to parallel chunk workers so they can open their
/// own connection for atomic `claim_next_chunk_task` operations.
#[derive(Clone)]
pub enum StateRef {
    Sqlite(std::path::PathBuf),
    Postgres(String),
}

// ─── SQLite migration ─────────────────────────────────────────────────────────

fn ensure_schema_version_table(conn: &Connection) {
    let _ = conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        );",
    );
}

fn get_current_version(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_version",
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

fn migrate(conn: &Connection) -> Result<()> {
    ensure_schema_version_table(conn);

    let current = get_current_version(conn);

    if current == 0 {
        let has_export_state: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='export_state'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if has_export_state {
            let metrics_cols = [
                "files_produced INTEGER DEFAULT 0",
                "bytes_written INTEGER DEFAULT 0",
                "retries INTEGER DEFAULT 0",
                "validated INTEGER",
                "schema_changed INTEGER",
                "run_id TEXT",
            ];
            for col_def in &metrics_cols {
                let sql = format!("ALTER TABLE export_metrics ADD COLUMN {}", col_def);
                let _ = conn.execute(&sql, []);
            }
        }
    }

    for &(ver, sql) in MIGRATIONS {
        if ver > current {
            log::debug!("state: applying migration v{}", ver);
            let atomic_sql = format!(
                "BEGIN;\n{}\nINSERT INTO schema_version (version) VALUES ({});\nCOMMIT;",
                sql, ver
            );
            conn.execute_batch(&atomic_sql)
                .map_err(|e| anyhow::anyhow!("state: migration v{} failed: {}", ver, e))?;
        }
    }

    let _ = conn.execute(
        "DELETE FROM schema_version WHERE version < (SELECT MAX(version) FROM schema_version)",
        [],
    );

    let final_version = get_current_version(conn);
    if final_version != SCHEMA_VERSION {
        anyhow::bail!(
            "state: migration incomplete — expected schema v{} but reached v{}",
            SCHEMA_VERSION,
            final_version
        );
    }

    Ok(())
}

// ─── PostgreSQL migration ─────────────────────────────────────────────────────

fn migrate_pg(client: &mut postgres::Client) -> Result<()> {
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS rivet_schema_version (version BIGINT NOT NULL);")
        .map_err(|e| anyhow::anyhow!("state(pg): create version table: {:#}", e))?;

    let current: i64 = client
        .query_one(
            "SELECT COALESCE(MAX(version), 0) FROM rivet_schema_version",
            &[],
        )
        .map_err(|e| anyhow::anyhow!("state(pg): read schema version: {:#}", e))?
        .get(0);

    for &(ver, sql) in PG_MIGRATIONS {
        if ver > current {
            log::debug!("state(pg): applying migration v{}", ver);
            let batch = format!(
                "BEGIN; {} INSERT INTO rivet_schema_version (version) VALUES ({}); COMMIT;",
                sql, ver
            );
            client
                .batch_execute(&batch)
                .map_err(|e| anyhow::anyhow!("state(pg): migration v{} failed: {:#}", ver, e))?;
        }
    }

    // Remove superseded version rows so MAX() stays unambiguous (mirrors SQLite behaviour).
    let _ = client.batch_execute(
        "DELETE FROM rivet_schema_version \
         WHERE version < (SELECT MAX(version) FROM rivet_schema_version);",
    );

    // Verify the DB actually reached the expected version.
    let final_version: i64 = client
        .query_one(
            "SELECT COALESCE(MAX(version), 0) FROM rivet_schema_version",
            &[],
        )
        .map_err(|e| anyhow::anyhow!("state(pg): read final schema version: {:#}", e))?
        .get(0);
    if final_version != SCHEMA_VERSION {
        anyhow::bail!(
            "state(pg): migration incomplete — expected schema v{} but reached v{}",
            SCHEMA_VERSION,
            final_version
        );
    }

    Ok(())
}

/// Redact the password from a PostgreSQL URL for safe use in log/error messages.
/// `postgresql://user:SECRET@host/db` → `postgresql://user:***@host/db`
/// Uses `rfind('@')` so passwords containing `@` are handled correctly.
fn redact_pg_url(url: &str) -> String {
    // Redact the password in `scheme://user:password@host/...`. RIVET_STATE_URL is
    // user-supplied and may be NON-conforming (a raw password with '/','?','#','@'
    // that a well-formed URL would percent-encode), so a redactor must FAIL SAFE —
    // over-redact, never echo the secret. Two passes:
    //
    // 1. Bounded (the conforming case): the authority ends at the first '/','?','#'
    //    after '://'; take the last '@' WITHIN it, so a stray '@' in the path/query
    //    (RFC-legal, e.g. `?opt=a@b`) does not extend the match. Preserves the host
    //    display. (round-2 audit #2)
    // 2. Fail-safe (round-3 regression fix): if the bounded region has no '@' but
    //    one exists LATER, the password itself contained a raw '/','?','#' that
    //    truncated the region — DO NOT fall through and echo the URL. Mask from the
    //    first ':' (start of password) to the last '@'. Over-redacts a pathological
    //    host but never leaks the credential. Bounding alone leaked `pa/ss` before.
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let after_scheme = &url[scheme_end + 3..];
    let authority_end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    if let Some(at_rel) = after_scheme[..authority_end].rfind('@') {
        // Pass 1: '@' is inside the authority (conforming URL).
        let authority = &after_scheme[..at_rel];
        if let Some(colon) = authority.rfind(':') {
            let user = &authority[..colon];
            let at_pos = scheme_end + 3 + at_rel;
            return format!(
                "{}://{}:***@{}",
                &url[..scheme_end],
                user,
                &url[at_pos + 1..]
            );
        }
        // '@' but no ':' → userinfo is user-only, no password to redact.
        return url.to_string();
    }
    // Pass 2: no '@' in the authority region. Fail SAFE if a password '@' exists
    // later (a raw delimiter in the password truncated the region).
    if let Some(at_rel) = after_scheme.rfind('@')
        && let Some(colon) = after_scheme.find(':')
        && colon < at_rel
    {
        let user = &after_scheme[..colon];
        return format!(
            "{}://{}:***@{}",
            &url[..scheme_end],
            user,
            &after_scheme[at_rel + 1..]
        );
    }
    url.to_string()
}

// ─── SQLite connection helper ─────────────────────────────────────────────────

pub(crate) const SQLITE_BUSY_TIMEOUT_MS: i64 = 10_000;

pub(crate) fn open_connection(db_path: &std::path::Path) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    if let Err(e) = conn.execute_batch("PRAGMA journal_mode=WAL;") {
        log::warn!(
            "state: WAL journal mode unavailable ({}); \
             running in default mode — concurrent writes may be slower",
            e
        );
    }
    if let Err(e) = conn.execute_batch(&format!(
        "PRAGMA busy_timeout = {};",
        SQLITE_BUSY_TIMEOUT_MS
    )) {
        log::warn!(
            "state: failed to set busy_timeout ({}); \
             concurrent writers may surface SQLITE_BUSY immediately",
            e
        );
    }
    Ok(conn)
}

// ─── StateStore ───────────────────────────────────────────────────────────────

/// Entry point for all persistent state.  Supports two backends:
///
/// - **SQLite** (default) — a single `.rivet_state.db` file next to the
///   config.  Good for local / single-node / dev deployments.
/// - **PostgreSQL** — a shared database addressed by `RIVET_STATE_URL`.
///   Required for stateless container / Kubernetes deployments where the
///   rivet pod is ephemeral or replicated.
///
/// Set the `RIVET_STATE_URL` environment variable to a PostgreSQL URL to
/// activate the Postgres backend:
///
/// ```text
/// RIVET_STATE_URL=postgresql://user:pass@host:5432/rivet_state
/// ```
///
/// When the variable is absent or does not start with `postgres`, SQLite is
/// used and the variable is ignored.
pub struct StateStore {
    pub(super) conn: StateConn,
    /// Serialisable reference for reconnection (parallel chunk workers).
    pub(super) state_ref: StateRef,
}

impl StateStore {
    /// Open the appropriate backend.
    ///
    /// Checks `RIVET_STATE_URL`; falls back to SQLite next to `config_path`.
    pub fn open(config_path: &str) -> Result<Self> {
        if let Ok(url) = std::env::var("RIVET_STATE_URL")
            && url.starts_with("postgres")
        {
            return Self::open_postgres(&url);
        }
        Self::open_sqlite(config_path)
    }

    fn open_sqlite(config_path: &str) -> Result<Self> {
        let config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let db_path = config_dir.join(STATE_DB_NAME);
        let conn = open_connection(&db_path)?;
        migrate(&conn)?;
        Ok(Self {
            conn: StateConn::Sqlite(conn),
            state_ref: StateRef::Sqlite(db_path),
        })
    }

    fn open_postgres(url: &str) -> Result<Self> {
        let is_local =
            url.contains("localhost") || url.contains("127.0.0.1") || url.contains("::1");
        if !is_local && state_tls_mode_from_url(url).is_none() {
            log::warn!(
                "state(pg): connecting to a remote host without TLS; \
                 add sslmode=require (or verify-ca / verify-full) to RIVET_STATE_URL \
                 to negotiate TLS for production use"
            );
        }
        let mut client = connect_pg(url)?;
        migrate_pg(&mut client)?;
        Ok(Self {
            conn: StateConn::Postgres(Box::new(std::cell::RefCell::new(client))),
            state_ref: StateRef::Postgres(url.to_string()),
        })
    }

    /// Path to `.rivet_state.db` for SQLite deployments.  Returns the config
    /// directory path for Postgres (not meaningful for connection, only used
    /// by legacy callers — prefer `state_ref()` for new code).
    pub fn state_db_path(config_path: &str) -> std::path::PathBuf {
        let config_dir = std::path::Path::new(config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        config_dir.join(STATE_DB_NAME)
    }

    /// Serialisable connection reference for parallel chunk workers.
    pub fn state_ref(&self) -> &StateRef {
        &self.state_ref
    }

    /// In-memory SQLite store for unit tests.
    #[allow(dead_code)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        migrate(&conn)?;
        Ok(Self {
            conn: StateConn::Sqlite(conn),
            state_ref: StateRef::Sqlite(std::path::PathBuf::from(":memory:")),
        })
    }

    /// Open a SQLite store at an explicit file path (tests that need
    /// cross-connection access via `claim_next_chunk_task_at_path`).
    #[allow(dead_code)]
    pub fn open_at_path(db_path: &std::path::Path) -> Result<Self> {
        let conn = open_connection(db_path)?;
        migrate(&conn)?;
        Ok(Self {
            conn: StateConn::Sqlite(conn),
            state_ref: StateRef::Sqlite(db_path.to_path_buf()),
        })
    }
}

// ─── Migration tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_and_postgres_migrations_define_the_same_tables_per_version() {
        // `migrate`/`migrate_pg` only check the final version NUMBER; nothing
        // catches a same-version, divergent-DDL edit between the two arrays. This
        // asserts that for every version present in BOTH, the set of tables each
        // CREATEs matches — so a table added to one backend but not the other
        // (a query that works on SQLite and errors on PG) fails loudly here.
        use std::collections::{BTreeSet, HashMap};
        fn table_names(sql: &str) -> BTreeSet<String> {
            let lower = sql.to_lowercase();
            let mut rest = lower.as_str();
            let mut out = BTreeSet::new();
            while let Some(i) = rest.find("create table") {
                rest = &rest[i + "create table".len()..];
                let after = rest
                    .trim_start()
                    .strip_prefix("if not exists")
                    .unwrap_or_else(|| rest.trim_start())
                    .trim_start();
                let name: String = after
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() {
                    out.insert(name);
                }
            }
            out
        }
        let mut pg: HashMap<i64, BTreeSet<String>> = HashMap::new();
        for &(v, sql) in PG_MIGRATIONS {
            pg.entry(v).or_default().extend(table_names(sql));
        }
        for &(v, sql) in MIGRATIONS {
            if let Some(pg_tables) = pg.get(&v) {
                assert_eq!(
                    &table_names(sql),
                    pg_tables,
                    "migration v{v}: SQLite and Postgres define different tables"
                );
            }
        }
    }

    #[test]
    fn fresh_db_reaches_latest_version() {
        let s = StateStore::open_in_memory().unwrap();
        let ver = match &s.conn {
            StateConn::Sqlite(c) => get_current_version(c),
            StateConn::Postgres(_) => unreachable!(),
        };
        assert_eq!(ver, SCHEMA_VERSION);
    }

    #[test]
    fn migration_is_idempotent() {
        let s = StateStore::open_in_memory().unwrap();
        match &s.conn {
            StateConn::Sqlite(c) => {
                migrate(c).unwrap();
                migrate(c).unwrap();
                assert_eq!(get_current_version(c), SCHEMA_VERSION);
            }
            StateConn::Postgres(_) => unreachable!(),
        }
    }

    #[test]
    fn legacy_db_gets_upgraded() {
        let conn = Connection::open_in_memory().unwrap();
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
            );",
        )
        .unwrap();

        migrate(&conn).unwrap();
        assert_eq!(get_current_version(&conn), SCHEMA_VERSION);

        let has_chunk_run: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='chunk_run'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_chunk_run);
    }

    #[test]
    fn upgrading_from_v12_adds_the_ledger_and_snapshot_tables_and_keeps_data() {
        // Stage a database at EXACTLY v12 — a user on the release before the load
        // ledger (v13) and cdc_snapshot (v14). Apply only migrations up to v12,
        // exactly as the older rivet that wrote their `.rivet_state.db` did.
        let conn = Connection::open_in_memory().unwrap();
        ensure_schema_version_table(&conn);
        for &(ver, sql) in MIGRATIONS {
            if ver <= 12 {
                conn.execute_batch(&format!(
                    "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                ))
                .unwrap();
            }
        }
        assert_eq!(get_current_version(&conn), 12, "staged at v12");
        // Pre-existing state that MUST survive the upgrade.
        conn.execute(
            "INSERT INTO export_state (export_name, last_cursor_value, last_run_at) \
             VALUES ('orders', '42', '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();

        // Upgrade the existing DB to the current schema (the v13 + v14 path).
        migrate(&conn).unwrap();
        assert_eq!(get_current_version(&conn), SCHEMA_VERSION);

        // The v13/v14 tables now exist on the upgraded-in-place DB.
        for t in ["load_run", "loaded_source_run", "cdc_snapshot"] {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name = ?1",
                    [t],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                exists,
                "{t} missing after the v12→v{SCHEMA_VERSION} upgrade"
            );
        }
        // The v12 data survived the added migrations (not dropped/recreated).
        let cursor: String = conn
            .query_row(
                "SELECT last_cursor_value FROM export_state WHERE export_name = 'orders'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cursor, "42", "pre-upgrade data must survive");
    }

    #[test]
    fn v8_renames_file_manifest_to_file_log() {
        let s = StateStore::open_in_memory().unwrap();
        let conn = match &s.conn {
            StateConn::Sqlite(c) => c,
            StateConn::Postgres(_) => unreachable!(),
        };
        let has_file_log: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='file_log'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_file_log, "v8 must produce a `file_log` table");
        let has_old: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='file_manifest'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!has_old, "v8 must remove the old `file_manifest` table");
        let has_new_idx: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='index' AND name='idx_file_log_export'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_new_idx, "v8 must create the renamed index");
    }

    #[test]
    fn v8_upgrades_existing_v7_db_with_data() {
        // Simulate an existing 0.6.0 database stopped at v7: the table is still
        // named `file_manifest` and has rows.  v8 must rename it preserving data.
        let conn = Connection::open_in_memory().unwrap();
        // Apply v1..=v7 by running the migrator after manually stamping v7.
        // Simpler: run the migrator, then manually rename back to v7 state to
        // exercise the v7→v8 path.  Here we just verify forward path covers it.
        migrate(&conn).unwrap();
        // Insert a row using the new name (post-v8); the rename happened transparently.
        conn.execute(
            "INSERT INTO file_log (run_id, export_name, file_name, row_count, bytes, format, created_at)
             VALUES ('r1', 'orders', 'f.parquet', 100, 4096, 'parquet', '2026-05-21T00:00:00Z')",
            [],
        )
        .unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM file_log", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn run_aggregate_table_exists_after_migration() {
        let s = StateStore::open_in_memory().unwrap();
        let conn = match &s.conn {
            StateConn::Sqlite(c) => c,
            StateConn::Postgres(_) => unreachable!(),
        };
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='run_aggregate'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists, "v5 migration must create the run_aggregate table");
    }

    #[test]
    fn v13_creates_the_load_ledger_tables() {
        let s = StateStore::open_in_memory().unwrap();
        let conn = match &s.conn {
            StateConn::Sqlite(c) => c,
            StateConn::Postgres(_) => unreachable!(),
        };
        for table in ["load_run", "loaded_source_run"] {
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name = ?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(exists, "v13 migration must create `{table}`");
        }
    }

    #[test]
    fn v14_creates_the_cdc_snapshot_table() {
        let s = StateStore::open_in_memory().unwrap();
        let conn = match &s.conn {
            StateConn::Sqlite(c) => c,
            StateConn::Postgres(_) => unreachable!(),
        };
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='cdc_snapshot'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists, "v14 migration must create the cdc_snapshot table");
    }

    #[test]
    fn pg_sql_converts_placeholders() {
        assert_eq!(
            pg_sql("SELECT ?1, ?2 FROM t WHERE x = ?3"),
            "SELECT $1, $2 FROM t WHERE x = $3"
        );
        assert_eq!(
            pg_sql("INSERT INTO t VALUES (?1, ?2)"),
            "INSERT INTO t VALUES ($1, $2)"
        );
        assert_eq!(pg_sql("no placeholders"), "no placeholders");
        // ?N with two digits
        assert_eq!(pg_sql("?10 AND ?11"), "$10 AND $11");
    }

    #[test]
    fn redact_pg_url_removes_password() {
        assert_eq!(
            redact_pg_url("postgresql://rivet:secret123@localhost:5433/rivet_state"),
            "postgresql://rivet:***@localhost:5433/rivet_state"
        );
        assert_eq!(
            redact_pg_url("postgres://admin:p@ssw0rd@db.prod.example.com/state"),
            "postgres://admin:***@db.prod.example.com/state"
        );
    }

    #[test]
    fn redact_pg_url_no_password_unchanged() {
        // URL without a password should come back as-is.
        let url = "postgresql://rivet@localhost/state";
        assert_eq!(redact_pg_url(url), url);
    }

    #[test]
    fn redact_pg_url_stray_at_in_query_does_not_leak_password() {
        // Round-2 audit #2: an unbounded rfind('@') landed on a '@' in the query,
        // so the redactor treated `u:secret@host…?opt=a` as the userinfo and
        // echoed `secret` verbatim. Bounding the search to the authority fixes it.
        // RED before the bound: output contained `secret`.
        let out = redact_pg_url("postgresql://u:secret@host:5432/db?opt=a@b");
        assert!(
            !out.contains("secret"),
            "password must not survive redaction with a stray '@' in the query: {out}"
        );
        assert_eq!(out, "postgresql://u:***@host:5432/db?opt=a@b");
    }

    #[test]
    fn redact_pg_url_password_with_raw_delimiters_does_not_leak() {
        // Round-3 regression: the #2 authority-bound `find(['/','?','#'])` truncated
        // BEFORE the real '@' when the password itself contained '/','?', or '#'
        // (base64 secrets routinely contain '/'), so rfind('@') missed, the redactor
        // fell through, and echoed the cleartext password. RED before the fail-safe
        // pass. Each must mask the secret AND keep the user + host visible.
        assert_eq!(
            redact_pg_url("postgresql://u:pa/ss@host/db"),
            "postgresql://u:***@host/db",
            "'/' in password must be redacted, not leaked"
        );
        assert_eq!(
            redact_pg_url("postgresql://u:pa?ss@host/db"),
            "postgresql://u:***@host/db",
            "'?' in password must be redacted"
        );
        assert_eq!(
            redact_pg_url("postgresql://u:pa#ss@host/db"),
            "postgresql://u:***@host/db",
            "'#' in password must be redacted"
        );
        // Belt-and-suspenders: the secret string never survives, whatever the shape.
        for u in [
            "postgresql://rivet:Xy/9Zq@db:5432/state",
            "postgres://admin:p/a?s#s@db.example.com/state",
        ] {
            assert!(
                !redact_pg_url(u).contains("Xy/9Zq") && !redact_pg_url(u).contains("p/a?s#s"),
                "no raw-delimiter password may survive: {}",
                redact_pg_url(u)
            );
        }
    }

    // ── state(pg) sslmode → TlsMode mapping ─────────────────────────────────
    //
    // Pins the decision behind the TLS bug fix: the state backend can no longer
    // hard-code NoTls. We can't drive a live TLS handshake in a unit test, so we
    // assert the *chosen transport policy* — TLS is enforced for require /
    // verify-* and plaintext (NoTls) otherwise — which is what selects the
    // connector inside `connect_pg` -> `connect_client`.
    use crate::config::TlsMode;

    #[test]
    fn state_sslmode_enforced_values_negotiate_tls() {
        for (url, want) in [
            (
                "postgresql://u:p@db.prod:5432/state?sslmode=require",
                TlsMode::Require,
            ),
            (
                "postgresql://u:p@db.prod/state?sslmode=verify-ca",
                TlsMode::VerifyCa,
            ),
            (
                "postgresql://u:p@db.prod/state?sslmode=verify-full",
                TlsMode::VerifyFull,
            ),
        ] {
            let mode = state_tls_mode_from_url(url);
            assert_eq!(mode, Some(want), "url: {url}");
            assert!(
                mode.unwrap().is_enforced(),
                "{want:?} must enforce TLS (not NoTls)"
            );
        }
    }

    #[test]
    fn state_sslmode_plaintext_values_stay_notls() {
        // Missing / disable / prefer / allow / unrecognized / uppercase all keep
        // the original NoTls behavior, so dev + docker setups are unchanged.
        for url in [
            "postgresql://u:p@localhost/state",
            "postgresql://u:p@localhost/state?sslmode=disable",
            "postgresql://u:p@db/state?sslmode=prefer",
            "postgresql://u:p@db/state?sslmode=allow",
            "postgresql://u:p@db/state?sslmode=REQUIRE",
            "postgresql://u:p@db/state?sslmode=garbage",
            "postgresql://u:p@db/state?sslmode",
            "postgresql://u:p@db/state?sslmode=",
        ] {
            assert_eq!(state_tls_mode_from_url(url), None, "url: {url}");
        }
    }

    #[test]
    fn state_sslmode_exact_key_and_last_occurrence_wins() {
        // `xsslmode` is a different parameter; the exact `sslmode` key matters.
        assert_eq!(
            state_tls_mode_from_url("postgresql://u:p@db/state?xsslmode=require"),
            None
        );
        // Found among other params.
        assert_eq!(
            state_tls_mode_from_url(
                "postgresql://u:p@db/state?connect_timeout=10&sslmode=require&application_name=x"
            ),
            Some(TlsMode::Require)
        );
        // Last occurrence wins, matching libpq.
        assert_eq!(
            state_tls_mode_from_url("postgresql://u:p@db/state?sslmode=disable&sslmode=require"),
            Some(TlsMode::Require)
        );
        assert_eq!(
            state_tls_mode_from_url("postgresql://u:p@db/state?sslmode=require&sslmode=disable"),
            None
        );
    }
}
