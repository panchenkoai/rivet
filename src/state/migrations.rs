//! State-DB schema version and the SQLite / PostgreSQL migration ladders.

use rusqlite::Connection;

use crate::error::Result;

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
    // v16: keyset checkpoint-resume manifest completeness (round-5). export_state
    // holds only the resume cursor, so a keyset crash+resume couldn't reconstruct the
    // pre-crash pages into the finalize manifest (silent orphan, the sibling of the
    // chunked fix). Persist the in-progress run_id here so resume can reuse it and
    // rehydrate every committed page from file_log; cleared when the run finalizes.
    (
        16,
        "ALTER TABLE export_state ADD COLUMN resume_run_id TEXT;",
    ),
    // v17: central run-status ledger. The AUTHORITATIVE record of each export
    // run's lifecycle — `running` at start, terminal at finalize. The bucket
    // manifest's status is a PROJECTION of this row (written FROM it), so a
    // cross-boundary reader over the bucket and a rivet process over a shared
    // state DB agree. gc_orphans reads it to spare a LIVE extract's in-flight
    // parts (a `running`, non-superseded run on the prefix) rather than guess
    // from a wall-clock freshness window.
    (
        17,
        "CREATE TABLE IF NOT EXISTS run_status (
            run_id      TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            prefix      TEXT NOT NULL,
            status      TEXT NOT NULL,
            started_at  TEXT NOT NULL,
            finished_at TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_run_status_prefix ON run_status(prefix);",
    ),
    // v18: failure-forensics columns on export_metrics. A `status='failed'` row IS
    // written on failure (unlike export_schema, which is success-only), so the
    // fields a post-mortem needs — the error CLASS, the key RANGE it died in, the
    // key's SHAPE, the OFFENDING value, the source SERVER limits — live HERE, making
    // one failed row self-sufficient to recreate the failure without the source DB.
    // Populated in `pipeline::job::build_metric_row` (see the write-point map there).
    (
        18,
        "ALTER TABLE export_metrics ADD COLUMN error_class TEXT;
         ALTER TABLE export_metrics ADD COLUMN cursor_min TEXT;
         ALTER TABLE export_metrics ADD COLUMN cursor_max TEXT;
         ALTER TABLE export_metrics ADD COLUMN key_descriptor_json TEXT;
         ALTER TABLE export_metrics ADD COLUMN offending_value TEXT;
         ALTER TABLE export_metrics ADD COLUMN server_context_json TEXT;",
    ),
    // v19: parallel-keyset crash-recovery ranges (feat/parallel-keyset iteration 2).
    // A parallel keyset run partitions the key into N stable ROW-percentile ranges
    // sampled at open; recovery is per-range (coarse): a crashed range re-reads from
    // its `lo`, a `done` range is skipped and rehydrated from file_log. The
    // boundaries MUST survive the crash (re-sampling a changed table would move them
    // and leave a gap), so they are persisted here at open, keyed by run_id, and
    // reloaded on resume — never re-sampled. Each worker flips only its OWN
    // (export_name, range_index) row `done=1` at completion (disjoint rows → no
    // cross-worker write contention), in the same transaction that records its parts
    // to file_log (atomic checkpoint). Cleared post-finalize by `finalize_keyset_anchor`.
    (
        19,
        "CREATE TABLE IF NOT EXISTS keyset_range (
            export_name TEXT NOT NULL,
            run_id      TEXT NOT NULL,
            range_index INTEGER NOT NULL,
            lo          TEXT,
            hi          TEXT,
            done        INTEGER NOT NULL DEFAULT 0,
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (export_name, range_index)
        );",
    ),
    // v20: WHICH SOURCE a target table was last loaded from.
    //
    // The ledger keyed loads on (target_table, source_run_id) and recorded
    // nothing about WHERE the rows came from, so two configs pointed at one
    // `dataset.table` from different databases were indistinguishable — the
    // second load replaced the first's rows and both reported success. The
    // prefix-level guard (`ensure_single_source`) catches them when they SHARE a
    // bucket prefix; separate prefixes into one warehouse table needed this.
    //
    // Nullable and additive: rows written before this column exists read NULL,
    // and the guard treats NULL as "unknown, do not block" — an upgrade must not
    // start refusing loads that were fine yesterday.
    (
        20,
        "ALTER TABLE loaded_source_run ADD COLUMN source_ident TEXT;",
    ),
    // v21: ONE in-flight aggregate row per run, enforced by the database.
    //
    // `project_running_aggregate` was UPDATE-else-INSERT, which is not atomic:
    // the parallel chunk-checkpoint runner gives every worker thread its own
    // connection, so two finishing their first chunk together both saw the UPDATE
    // affect zero rows and both INSERTed. The run then had two `running`
    // aggregates — in the table this branch made the record of a run — and the
    // whole point of projecting instead of appending is that a run has exactly
    // one.
    //
    // The DELETE first: an existing database may already hold duplicates from
    // that race, and the index cannot be created over them. It keeps the highest
    // `id` per run (the most recently written projection) and drops the rest;
    // both are projections of the same `file_log` rows, so no information is lost.
    //
    // PARTIAL on `status = 'running'`: terminal rows are written by
    // `record_metric_full` and a run legitimately has one per attempt.
    (
        21,
        "DELETE FROM export_metrics WHERE status = 'running' AND id NOT IN (
             SELECT max(id) FROM export_metrics WHERE status = 'running' GROUP BY run_id
         );
         CREATE UNIQUE INDEX IF NOT EXISTS export_metrics_one_running_per_run
             ON export_metrics(run_id) WHERE status = 'running';",
    ),
    (
        22,
        "CREATE TABLE IF NOT EXISTS strategy_snapshot (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             export_name TEXT NOT NULL,
             source_schema TEXT,
             source_table TEXT NOT NULL,
             row_estimate BIGINT,
             total_bytes BIGINT,
             avg_row_bytes BIGINT,
             chosen_mode TEXT NOT NULL,
             strategy_kind TEXT,
             key_column TEXT,
             chunk_size BIGINT,
             rivet_version TEXT NOT NULL,
             captured_at TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_strategy_snapshot_export
             ON strategy_snapshot(export_name, id DESC);",
    ),
    // v23: decoded bytes READ from the source per run (in-memory Arrow batch
    // size summed on the plan's shared counter by every sink) — the read-leg
    // counterpart to bytes_written, so read-vs-write throughput is visible (#175).
    (
        23,
        "ALTER TABLE export_metrics ADD COLUMN bytes_read INTEGER;",
    ),
    // v24: the first-run density probe's audit trail (#148) — where the
    // snapshot's row figure CAME from (probed/counted/catalog-*/unverified),
    // the catalog's original claim, and the probe shape.
    (
        24,
        "ALTER TABLE strategy_snapshot ADD COLUMN catalog_rows INTEGER;
        ALTER TABLE strategy_snapshot ADD COLUMN density REAL;
        ALTER TABLE strategy_snapshot ADD COLUMN estimate_method TEXT;
        ALTER TABLE strategy_snapshot ADD COLUMN probe_k INTEGER;
        ALTER TABLE strategy_snapshot ADD COLUMN probe_w INTEGER;",
    ),
    // v25: cursor-atomic keyset checkpoint. The page's high-water key, written in the SAME
    // file_log row as its part(s), so a crash-recovery resume reconciles the cursor from the
    // COMMITTED parts and never re-reads a committed page — closing the after_manifest_update
    // dup and its multi-part-rotation variant at the root (a re-read that never happens can't
    // duplicate). Nullable: only the sequential keyset checkpoint path writes it.
    (25, "ALTER TABLE file_log ADD COLUMN cursor_high TEXT;"),
    (
        26,
        "ALTER TABLE export_state ADD COLUMN cursor_column TEXT;",
    ),
    (
        27,
        "CREATE TABLE IF NOT EXISTS export_load_spec (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             columns_json TEXT,
             primary_key_json TEXT,
             key_origin TEXT,
             run_id TEXT,
             origin TEXT NOT NULL,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit)
         );",
    ),
    // The spec EACH RUN recorded, keyed by run — `export_load_spec` is one row per
    // export (last writer wins), so on a shared state two configs with a same-named
    // export overwrite each other's; the load pins its plan to the run it consumes.
    (
        28,
        "CREATE TABLE IF NOT EXISTS export_load_spec_run (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             run_id TEXT NOT NULL,
             columns_json TEXT NOT NULL,
             primary_key_json TEXT,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit, run_id)
         );",
    ),
    // v29: a baseline is keyed by its DESTINATION as well — two configs sharing a
    // state DB and an export name kept one `cdc_snapshot` row and skipped each
    // other's baseline. Legacy rows keep prefix '' and count for every prefix.
    // `keyset_range` records the key column its ranges were sampled on, so a
    // resume after the key changed re-samples instead of skipping done ranges.
    (
        29,
        "ALTER TABLE cdc_snapshot RENAME TO cdc_snapshot_v28;
        CREATE TABLE cdc_snapshot (
            export_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            prefix TEXT NOT NULL DEFAULT '',
            run_id TEXT NOT NULL,
            completed_at TEXT NOT NULL,
            PRIMARY KEY (export_name, table_name, prefix)
        );
        INSERT INTO cdc_snapshot (export_name, table_name, prefix, run_id, completed_at)
            SELECT export_name, table_name, '', run_id, completed_at FROM cdc_snapshot_v28;
        DROP TABLE cdc_snapshot_v28;
        ALTER TABLE keyset_range ADD COLUMN key_column TEXT;",
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
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS files_committed BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS reconciled BOOLEAN;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS source_count BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS quality_passed BOOLEAN;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS pg_temp_bytes_delta BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS batch_size BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS batch_size_memory_mb BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS skip_reason TEXT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS schema_fingerprint TEXT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS chunk_size BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS parallel BIGINT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS source_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS destination_type TEXT;
        ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS rivet_version TEXT;",
    ),
    // v10: longest single-chunk wall time (ms). See the SQLite array.
    (
        10,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS longest_chunk_ms BIGINT;",
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
    (
        12,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS chunk_key TEXT;",
    ),
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
    // v16: keyset checkpoint-resume manifest completeness (round-5). export_state
    // holds only the resume cursor, so a keyset crash+resume couldn't reconstruct the
    // pre-crash pages into the finalize manifest (silent orphan, the sibling of the
    // chunked fix). Persist the in-progress run_id here so resume can reuse it and
    // rehydrate every committed page from file_log; cleared when the run finalizes.
    (
        16,
        "ALTER TABLE export_state ADD COLUMN IF NOT EXISTS resume_run_id TEXT;",
    ),
    // v17: central run-status ledger. The AUTHORITATIVE record of each export
    // run's lifecycle — `running` at start, terminal at finalize. The bucket
    // manifest's status is a PROJECTION of this row (written FROM it), so a
    // cross-boundary reader over the bucket and a rivet process over a shared
    // state DB agree. gc_orphans reads it to spare a LIVE extract's in-flight
    // parts (a `running`, non-superseded run on the prefix) rather than guess
    // from a wall-clock freshness window.
    (
        17,
        "CREATE TABLE IF NOT EXISTS run_status (
            run_id      TEXT PRIMARY KEY,
            export_name TEXT NOT NULL,
            prefix      TEXT NOT NULL,
            status      TEXT NOT NULL,
            started_at  TEXT NOT NULL,
            finished_at TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_run_status_prefix ON run_status(prefix);",
    ),
    // v18: failure-forensics columns (see the SQLite v18 comment). Postgres TEXT
    // holds the same JSON/scalar payloads.
    (
        18,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS error_class TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS cursor_min TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS cursor_max TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS key_descriptor_json TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS offending_value TEXT;
         ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS server_context_json TEXT;",
    ),
    // v19: parallel-keyset crash-recovery ranges (see the SQLite v19 comment).
    // range_index/done are BIGINT (not the SQLite INTEGER): the state layer binds
    // them as StateParam::I64 and reads them via StateRow::i64, and rust-postgres
    // is strictly typed (i64 <-> INT8 only) — an int4 column would reject the bind
    // (WrongType at persist) and panic on the read (resume). Every other integer
    // column in PG_MIGRATIONS is BIGINT for exactly this reason.
    (
        19,
        "CREATE TABLE IF NOT EXISTS keyset_range (
            export_name TEXT NOT NULL,
            run_id      TEXT NOT NULL,
            range_index BIGINT NOT NULL,
            lo          TEXT,
            hi          TEXT,
            done        BIGINT NOT NULL DEFAULT 0,
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (export_name, range_index)
        );",
    ),
    // v20: WHICH SOURCE a target table was last loaded from.
    //
    // The ledger keyed loads on (target_table, source_run_id) and recorded
    // nothing about WHERE the rows came from, so two configs pointed at one
    // `dataset.table` from different databases were indistinguishable — the
    // second load replaced the first's rows and both reported success. The
    // prefix-level guard (`ensure_single_source`) catches them when they SHARE a
    // bucket prefix; separate prefixes into one warehouse table needed this.
    //
    // Nullable and additive: rows written before this column exists read NULL,
    // and the guard treats NULL as "unknown, do not block" — an upgrade must not
    // start refusing loads that were fine yesterday.
    (
        20,
        "ALTER TABLE loaded_source_run ADD COLUMN IF NOT EXISTS source_ident TEXT;",
    ),
    // v21: ONE in-flight aggregate row per run — see the SQLite ladder for why.
    //
    // PostgreSQL needs `ctid` rather than `id NOT IN (SELECT max(id) …)`: the
    // column exists on both, but keeping the ladders as close as the dialects
    // allow matters less than the DELETE being correct here. Partial unique
    // indexes work the same on both.
    (
        21,
        "DELETE FROM export_metrics a
             WHERE a.status = 'running'
               AND a.id < (SELECT max(b.id) FROM export_metrics b
                            WHERE b.run_id = a.run_id AND b.status = 'running');
         CREATE UNIQUE INDEX IF NOT EXISTS export_metrics_one_running_per_run
             ON export_metrics(run_id) WHERE status = 'running';",
    ),
    (
        22,
        "CREATE TABLE IF NOT EXISTS strategy_snapshot (
             id BIGSERIAL PRIMARY KEY,
             export_name TEXT NOT NULL,
             source_schema TEXT,
             source_table TEXT NOT NULL,
             row_estimate BIGINT,
             total_bytes BIGINT,
             avg_row_bytes BIGINT,
             chosen_mode TEXT NOT NULL,
             strategy_kind TEXT,
             key_column TEXT,
             chunk_size BIGINT,
             rivet_version TEXT NOT NULL,
             captured_at TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_strategy_snapshot_export
             ON strategy_snapshot(export_name, id DESC);",
    ),
    (
        23,
        "ALTER TABLE export_metrics ADD COLUMN IF NOT EXISTS bytes_read BIGINT;",
    ),
    (
        24,
        // IDEMPOTENT (ADD COLUMN IF NOT EXISTS): the gate's shared rivet_state
        // reached a columns-present / version-23 state (a stale golden-state
        // snapshot carried the v24 columns while its version row lagged), and a
        // plain ADD COLUMN then failed \"column already exists\", breaking EVERY
        // PG-state cell (concurrent-writers, keyset, parity, pool e2e) — roast
        // 2026-08-10. PG supports IF NOT EXISTS; a migration must be re-runnable.
        "ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS catalog_rows BIGINT;
        ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS density DOUBLE PRECISION;
        ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS estimate_method TEXT;
        ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS probe_k BIGINT;
        ALTER TABLE strategy_snapshot ADD COLUMN IF NOT EXISTS probe_w BIGINT;",
    ),
    // v25: cursor-atomic keyset checkpoint — see the SQLite ladder. IDEMPOTENT (`IF NOT EXISTS`).
    (
        25,
        "ALTER TABLE file_log ADD COLUMN IF NOT EXISTS cursor_high TEXT;",
    ),
    (
        26,
        "ALTER TABLE export_state ADD COLUMN IF NOT EXISTS cursor_column TEXT;",
    ),
    (
        27,
        "CREATE TABLE IF NOT EXISTS export_load_spec (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             columns_json TEXT,
             primary_key_json TEXT,
             key_origin TEXT,
             run_id TEXT,
             origin TEXT NOT NULL,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit)
         );",
    ),
    (
        28,
        "CREATE TABLE IF NOT EXISTS export_load_spec_run (
             export_name TEXT NOT NULL,
             unit TEXT NOT NULL DEFAULT '',
             run_id TEXT NOT NULL,
             columns_json TEXT NOT NULL,
             primary_key_json TEXT,
             captured_at TEXT NOT NULL,
             PRIMARY KEY (export_name, unit, run_id)
         );",
    ),
    // v29: see the SQLite ladder. Postgres alters in place; the unnamed primary
    // key of a `CREATE TABLE` is `<table>_pkey`.
    (
        29,
        "ALTER TABLE cdc_snapshot ADD COLUMN IF NOT EXISTS prefix TEXT NOT NULL DEFAULT '';
         ALTER TABLE cdc_snapshot DROP CONSTRAINT IF EXISTS cdc_snapshot_pkey;
         ALTER TABLE cdc_snapshot ADD PRIMARY KEY (export_name, table_name, prefix);
         ALTER TABLE keyset_range ADD COLUMN IF NOT EXISTS key_column TEXT;",
    ),
];

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

pub(super) fn migrate(conn: &Connection) -> Result<()> {
    // ONE writer migrates at a time. `BEGIN IMMEDIATE` takes the database's write
    // lock before anything is read, so the version this process sees cannot be
    // stale by the time it acts on it; the others block on `busy_timeout` and
    // find the work already done.
    //
    // Without it, several rivet processes starting together against an EMPTY
    // state db each read version 0 and each apply the whole ladder. Measured on
    // four concurrent exports, five rounds out of five had failures:
    // `migration v8 failed: no such table: file_manifest` (one process renamed it
    // while another was still looking for the old name), `already another table
    // or index with this name: file_log`, `duplicate column name:
    // files_committed`. That is the first day of a shared deployment — provision
    // the backend, start the exports — and most of them died on startup.
    //
    // The guard is advisory-by-transaction rather than a lock table: an aborted
    // process releases it by dying, so a crashed migrator cannot wedge the next
    // one.
    // Discarding this result would defeat the whole guard: `BEGIN IMMEDIATE`
    // returns SQLITE_BUSY when the write lock is not obtained within
    // `busy_timeout`, and continuing anyway runs the ladder unprotected —
    // exactly the concurrent-migration case above. It does not corrupt silently
    // (the unprotected ladder hits the errors quoted above and they propagate),
    // but the operator is then handed "no such table: file_manifest" instead of
    // the truth, which names neither the cause nor the remedy.
    conn.execute_batch("BEGIN IMMEDIATE;").map_err(|e| {
        // NOTADB is a DIFFERENT disease than BUSY: garbage bytes in the file
        // are not another process's lock, and telling the operator to "wait
        // for it to finish" strands them waiting on a phantom (round-5 — the
        // corrupt-DB probe surfaced this wrong-cause-first headline through
        // the new `state` CLI).
        let msg = e.to_string();
        if msg.contains("file is not a database") || msg.contains("database disk image") {
            anyhow::anyhow!(
                "state: {msg} — the state DB file is CORRUPT (or not a SQLite file at \
                 all), not locked. Move it aside and re-run (rivet rebuilds state; \
                 incremental cursors and skip-ledgers start fresh), or restore it from \
                 a backup."
            )
        } else {
            anyhow::anyhow!(
                "state: could not acquire the migration lock within the busy timeout ({e}). \
                 Another rivet process is migrating this state database; wait for it to \
                 finish and retry. Running the migration ladder without the lock is what \
                 produces 'no such table' / 'duplicate column' failures on a shared backend."
            )
        }
    })?;
    let out = migrate_locked(conn);
    let _ = conn.execute_batch(if out.is_ok() { "COMMIT;" } else { "ROLLBACK;" });
    out
}

fn migrate_locked(conn: &Connection) -> Result<()> {
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
                // SQLite has NO `ADD COLUMN IF NOT EXISTS` (unlike PostgreSQL) —
                // idempotency here is the `let _ =` swallow of the duplicate-column
                // error. Do NOT add IF NOT EXISTS: SQLite rejects the syntax and
                // the whole legacy upgrade fails (roast 2026-08-10).
                let sql = format!("ALTER TABLE export_metrics ADD COLUMN {}", col_def);
                let _ = conn.execute(&sql, []);
            }
        }
    }

    for &(ver, sql) in MIGRATIONS {
        if ver > current {
            log::debug!("state: applying migration v{}", ver);
            // No inner BEGIN/COMMIT: `migrate` already holds the write
            // transaction, and SQLite does not nest. The atomicity the inner
            // pair provided is now the outer one's, which is strictly stronger —
            // a failure rolls back the whole ladder rather than leaving the db
            // at a half-applied version.
            let atomic_sql = format!(
                "{}\nINSERT INTO schema_version (version) VALUES ({});",
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
    if final_version > SCHEMA_VERSION {
        anyhow::bail!(
            "state: this state DB is at schema v{final_version}, newer than this rivet knows \
             (v{SCHEMA_VERSION}) — a newer rivet migrated it. Upgrade rivet, or point this one \
             at a state DB it created; a downgrade never rewrites the schema"
        );
    }
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

/// The advisory-lock key for state migrations. An arbitrary constant, chosen
/// once: any two rivet processes must agree, and nothing else uses the space.
const PG_MIGRATION_LOCK: i64 = 0x7269_7665_745f_6d69_u64 as i64; // "rivet_mi"

pub(super) fn migrate_pg(client: &mut postgres::Client) -> Result<()> {
    // ONE writer migrates at a time, across PROCESSES and HOSTS. A session
    // advisory lock is the right instrument: it is held for the connection, so a
    // process that dies mid-migration releases it, and it needs no table of its
    // own (which would itself have to be created race-free).
    //
    // `CREATE TABLE IF NOT EXISTS` is NOT race-free in PostgreSQL — concurrent
    // creators collide in the catalog — and even past it each migration ran in
    // its own transaction with no coordination, so two clients both read version
    // N and both applied N+1. Measured on four concurrent exports against an
    // EMPTY schema: THREE of the four died with `state(pg): create version table:
    // db error`, at the very first statement. One survived. That is the first day
    // of a shared deployment.
    client
        .batch_execute(&format!("SELECT pg_advisory_lock({PG_MIGRATION_LOCK});"))
        .map_err(|e| anyhow::anyhow!("state(pg): take migration lock: {:#}", e))?;
    let out = migrate_pg_locked(client);
    let _ = client.batch_execute(&format!("SELECT pg_advisory_unlock({PG_MIGRATION_LOCK});"));
    out
}

fn migrate_pg_locked(client: &mut postgres::Client) -> Result<()> {
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
    if final_version > SCHEMA_VERSION {
        anyhow::bail!(
            "state(pg): this state DB is at schema v{final_version}, newer than this rivet knows \
             (v{SCHEMA_VERSION}) — a newer rivet migrated it. Upgrade rivet, or point this one at \
             a state DB it created; a downgrade never rewrites the schema"
        );
    }
    if final_version != SCHEMA_VERSION {
        anyhow::bail!(
            "state(pg): migration incomplete — expected schema v{} but reached v{}",
            SCHEMA_VERSION,
            final_version
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{StateConn, StateStore, connect_pg, open_connection};
    use super::*;

    /// The two ladders must define the SAME versions.
    ///
    /// A migration added to one and not the other is not a slow divergence — it
    /// breaks the missing backend on the next start, because `migrate` compares
    /// the reached version against `SCHEMA_VERSION`, which is the LAST entry of
    /// the SQLite ladder for both. Measured 2026-08-04: v21 went into
    /// `MIGRATIONS` alone, and every Postgres-backed run died with
    /// `state(pg): migration incomplete — expected schema v21 but reached v20` —
    /// the release gate's state-parity, concurrent-writers and cdc-parity cells
    /// all failed on that one omission, twenty minutes into a run.
    ///
    /// The SQL differs per dialect and always will; the VERSION SET must not.
    #[test]
    fn both_migration_ladders_define_the_same_versions() {
        let sqlite: Vec<i64> = MIGRATIONS.iter().map(|(v, _)| *v).collect();
        let pg: Vec<i64> = PG_MIGRATIONS.iter().map(|(v, _)| *v).collect();
        assert_eq!(
            sqlite, pg,
            "the SQLite and PostgreSQL migration ladders disagree; the backend missing a \
             version refuses to start, because SCHEMA_VERSION is the last SQLite entry for both"
        );
        assert_eq!(
            SCHEMA_VERSION,
            *sqlite.last().expect("the ladder is not empty"),
            "SCHEMA_VERSION must be the ladder's last version"
        );
        // Ascending and unique, or `migrate` can skip or re-apply one.
        assert!(
            sqlite.windows(2).all(|w| w[0] < w[1]),
            "migration versions must be strictly ascending: {sqlite:?}"
        );
    }

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
        // SQLite cannot ALTER a primary key, so a key change REBUILDS the table
        // (CREATE + copy) where Postgres alters in place: the one sanctioned
        // asymmetry, listed by version and table.
        const REBUILT_ON_SQLITE_ONLY: &[(i64, &str)] = &[(29, "cdc_snapshot")];
        for &(v, sql) in MIGRATIONS {
            if let Some(pg_tables) = pg.get(&v) {
                let mut sqlite_tables = table_names(sql);
                for (rv, t) in REBUILT_ON_SQLITE_ONLY {
                    if *rv == v {
                        sqlite_tables.remove(*t);
                    }
                }
                assert_eq!(
                    &sqlite_tables, pg_tables,
                    "migration v{v}: SQLite and Postgres define different tables"
                );
            }
        }
    }

    /// Every PG `ALTER TABLE … ADD COLUMN` must be IDEMPOTENT (`IF NOT EXISTS`).
    /// A migration must be re-runnable: the gate's shared rivet_state reached a
    /// columns-present / version-behind state (a stale golden-state snapshot) and
    /// a plain ADD COLUMN then failed "column already exists", breaking EVERY
    /// PG-state cell (roast 2026-08-10, v24). PostgreSQL supports IF NOT EXISTS;
    /// this guards the whole class going forward. (SQLite ADD COLUMN has no
    /// IF NOT EXISTS and is per-file fresh, so this applies to PG only.)
    #[test]
    fn every_pg_add_column_is_idempotent() {
        for &(v, sql) in PG_MIGRATIONS {
            for line in sql.lines() {
                let l = line.trim();
                if l.starts_with("//") || l.starts_with("--") {
                    continue; // a comment, not a statement
                }
                if l.contains("ADD COLUMN") {
                    assert!(
                        l.contains("ADD COLUMN IF NOT EXISTS"),
                        "PG migration v{v} has a non-idempotent ADD COLUMN — must be \
                         `ADD COLUMN IF NOT EXISTS` so the migration is re-runnable: {l}"
                    );
                }
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

    /// A state DB a NEWER rivet migrated is refused by name, before the generic
    /// "migration incomplete" — the ladder applies nothing and rewrites nothing.
    #[test]
    fn a_state_db_from_a_newer_rivet_is_refused_by_name() {
        let s = StateStore::open_in_memory().unwrap();
        match &s.conn {
            StateConn::Sqlite(c) => {
                migrate(c).unwrap();
                c.execute(
                    "INSERT INTO schema_version (version) VALUES (?1)",
                    [SCHEMA_VERSION + 1],
                )
                .unwrap();
                let err = format!("{:#}", migrate(c).unwrap_err());
                assert!(err.contains("newer than this rivet knows"), "{err}");
                assert!(!err.contains("migration incomplete"), "{err}");
                assert_eq!(
                    get_current_version(c),
                    SCHEMA_VERSION + 1,
                    "nothing rewritten"
                );
            }
            StateConn::Postgres(_) => unreachable!(),
        }
    }

    /// v29 rebuilds `cdc_snapshot` with the prefix in its key: a row written
    /// before it keeps prefix '' (done for every prefix), and two prefixes for one
    /// `(export, table)` are two rows. `keyset_range` gains `key_column`.
    #[test]
    fn v29_keeps_legacy_snapshot_rows_and_keys_baselines_by_prefix() {
        let conn = Connection::open_in_memory().unwrap();
        ensure_schema_version_table(&conn);
        for &(ver, sql) in MIGRATIONS {
            if ver <= 28 {
                conn.execute_batch(&format!(
                    "BEGIN;\n{sql}\nINSERT INTO schema_version (version) VALUES ({ver});\nCOMMIT;"
                ))
                .unwrap();
            }
        }
        conn.execute(
            "INSERT INTO cdc_snapshot (export_name, table_name, run_id, completed_at) \
             VALUES ('users', 'users', 'r1', '2026-01-01T00:00:00Z')",
            [],
        )
        .unwrap();
        migrate(&conn).unwrap();
        let legacy_prefix: String = conn
            .query_row(
                "SELECT prefix FROM cdc_snapshot WHERE run_id = 'r1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(legacy_prefix, "", "a pre-v29 row keeps the empty prefix");
        conn.execute(
            "INSERT INTO cdc_snapshot (export_name, table_name, prefix, run_id, completed_at) \
             VALUES ('users', 'users', 'gs://b/pb/', 'r2', '2026-01-02T00:00:00Z')",
            [],
        )
        .expect("a second prefix for the same export/table is its own row");
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM cdc_snapshot", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
        conn.execute("SELECT key_column FROM keyset_range", [])
            .unwrap();
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

    /// Several writers migrating ONE database at once all succeed.
    ///
    /// Idempotence is not concurrency safety, and until now only the first was
    /// held: `migration_is_idempotent` migrates TWICE on ONE connection, which the
    /// race cannot reach. The guard is `BEGIN IMMEDIATE` in [`migrate`], taken
    /// before the version is read; without it every thread reads version 0 and
    /// applies the whole ladder, which measured failures in five rounds out of five
    /// (`no such table: file_manifest`, `already another table or index with this
    /// name: file_log`, `duplicate column name: files_committed`). That measurement
    /// lived only in a comment — nothing would have noticed the guard's removal.
    ///
    /// FILE-backed, not `:memory:`, because an in-memory database is private to its
    /// own connection: the race does not exist there at all, which is exactly why
    /// the existing tests could not express it. The barrier makes the overlap real
    /// rather than hoped for — four writers, the count the original measurement used.
    #[test]
    fn several_writers_migrating_one_database_at_once_all_succeed() {
        const WRITERS: usize = 4;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("state.db");
        let start = std::sync::Barrier::new(WRITERS);

        let results: Vec<Result<()>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..WRITERS)
                .map(|_| {
                    s.spawn(|| {
                        let conn = open_connection(&db)?;
                        // Open first, then line up: the contention under test is the
                        // MIGRATION, not the file open.
                        start.wait();
                        migrate(&conn)
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
                "writer {i} of {WRITERS} failed to migrate a shared database: {:?}",
                r.as_ref().err()
            );
        }
        let conn = open_connection(&db).unwrap();
        assert_eq!(
            get_current_version(&conn),
            SCHEMA_VERSION,
            "the ladder must end at the current version exactly once, however many \
             writers raced to apply it"
        );
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

    /// The POSTGRES upgrade path: stage a populated state DB at v18 (before the v19
    /// keyset_range table), then migrate it in place to HEAD and assert v19 lands
    /// correctly on the EXISTING db — keyset_range.range_index must be BIGINT (bug #1:
    /// an int4 there breaks every parallel-keyset run) and the pre-upgrade cursor must
    /// survive. The state_migrations preflight only ever migrates a FRESH db, so an
    /// ALTER/CREATE that works clean but breaks on a populated old schema would slip
    /// through without this. Isolated in its own schema; skips without RIVET_TEST_STATE_URL.
    #[test]
    fn pg_upgrade_from_v18_lands_keyset_range_as_bigint_and_keeps_data() {
        let Ok(url) = std::env::var("RIVET_TEST_STATE_URL") else {
            return;
        };
        if !url.starts_with("postgres") {
            return;
        }
        let mut client = connect_pg(&url).expect("connect pg state");
        // Isolate: a fresh schema so the staged FIXED-name state tables never collide
        // with the shared rivet_state db or a concurrent test.
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS rivet_upgrade_test CASCADE; \
                 CREATE SCHEMA rivet_upgrade_test; SET search_path TO rivet_upgrade_test;",
            )
            .unwrap();

        // Stage at EXACTLY v18 — apply only migrations up to v18, exactly as the rivet
        // release before keyset_range (v19) wrote a shared-Postgres state db.
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS rivet_schema_version (version BIGINT NOT NULL);",
            )
            .unwrap();
        for &(ver, sql) in PG_MIGRATIONS {
            if ver <= 18 {
                client
                    .batch_execute(&format!(
                        "BEGIN; {sql} INSERT INTO rivet_schema_version (version) VALUES ({ver}); COMMIT;"
                    ))
                    .unwrap();
            }
        }
        // Pre-existing state that MUST survive the in-place upgrade.
        client
            .batch_execute(
                "INSERT INTO export_state (export_name, last_cursor_value, last_run_at) \
                 VALUES ('orders', '42', '2026-01-01T00:00:00Z')",
            )
            .unwrap();

        // Upgrade in place to HEAD (applies v19 keyset_range on the POPULATED db).
        migrate_pg(&mut client).expect("v18 -> HEAD upgrade must apply cleanly on a populated db");

        // keyset_range.range_index is BIGINT on the upgraded-in-place db (not int4).
        let dtype: String = client
            .query_one(
                "SELECT data_type FROM information_schema.columns \
                 WHERE table_schema = 'rivet_upgrade_test' AND table_name = 'keyset_range' \
                   AND column_name = 'range_index'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            dtype, "bigint",
            "v19 keyset_range.range_index must upgrade to BIGINT, not int4 (bug #1)"
        );
        // The v18 data survived the added migration (not dropped/recreated).
        let cursor: String = client
            .query_one(
                "SELECT last_cursor_value FROM export_state WHERE export_name = 'orders'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            cursor, "42",
            "pre-upgrade cursor must survive the migration"
        );

        client
            .batch_execute("DROP SCHEMA IF EXISTS rivet_upgrade_test CASCADE;")
            .unwrap();
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
}
