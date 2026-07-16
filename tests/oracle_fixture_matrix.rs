//! Entry point for the production oracle+fixture matrix (ADR-0001).
//!
//! The cells live in `docs/oracle-fixture-matrix.yaml`; each `test:` id maps to
//! one `#[test] fn` here that calls the black-box [`harness`]. The live cells are
//! `#[ignore]` (they need the devbox seeded stack + BigQuery creds); a future
//! ratchet guard asserts every mapped id exists and declares a real oracle.

mod harness;

use harness::{
    Engine, Fixture, Mode, OracleOutcome, SoakConfig, SoakOracle, Verification, Warehouse,
    WarehouseOracle,
};

/// Assert every requested oracle passed — a `Skipped` (no-op) outcome is a
/// strength-floor failure, never a silent pass. Generic over the oracle family
/// (batch or soak) so the type system keeps the two runners' oracles apart.
fn assert_all_pass<O: std::fmt::Debug>(results: Vec<(O, OracleOutcome)>) {
    for (oracle, outcome) in results {
        match outcome {
            OracleOutcome::Pass => {}
            OracleOutcome::Fail { detail } => panic!("{oracle:?} FAILED: {detail}"),
            OracleOutcome::Skipped { why } => {
                panic!("{oracle:?} is a no-op for this lane (strength-floor): {why}")
            }
        }
    }
}

// ── tracer cell: heavy content_items under Postgres batch → BigQuery ─────────
#[test]
#[ignore = "live: needs devbox seeded stack + BigQuery creds"]
fn heavy_content_batch_pg() {
    let v = Verification::new(
        Engine::Postgres,
        Mode::Batch,
        Fixture::heavy_batch("content_items"),
    );
    assert_all_pass(
        v.run(&[
            WarehouseOracle::RowCount,
            WarehouseOracle::DistinctId,
            WarehouseOracle::NullProfile,
            WarehouseOracle::TypeFidelity,
        ])
        .expect("pipeline"),
    );
}

// ── the white space: heavy content_items under Postgres CDC → BigQuery ───────
#[test]
#[ignore = "live: needs devbox cdc stack + BigQuery creds"]
fn heavy_content_cdc_pg() {
    let v = Verification::new(
        Engine::Postgres,
        Mode::Cdc,
        Fixture::heavy_cdc_base("content_items"),
    );
    assert_all_pass(
        v.run_soak(
            &SoakConfig::quick(),
            &[WarehouseOracle::TypeFidelity],
            &[SoakOracle::FlatRss, SoakOracle::ZeroGap],
        )
        .expect("soak"),
    );
}

// ── MySQL parity: heavy content_items under CDC (binlog) → BigQuery ───────────
#[test]
#[ignore = "live: needs devbox mysql-cdc stack + BigQuery creds"]
fn heavy_content_cdc_mysql() {
    let v = Verification::new(
        Engine::Mysql,
        Mode::Cdc,
        Fixture::heavy_cdc_base("content_items"),
    );
    assert_all_pass(
        v.run_soak(
            &SoakConfig::quick(),
            &[WarehouseOracle::TypeFidelity],
            &[SoakOracle::FlatRss, SoakOracle::ZeroGap],
        )
        .expect("soak"),
    );
}

// ── live smokes: fast end-to-end validation on MySQL → BigQuery ───────────────
#[test]
#[ignore = "live smoke: mysql batch → BigQuery"]
fn smoke_batch_mysql() {
    let v = Verification::new(Engine::Mysql, Mode::Batch, Fixture::smoke("content_items"));
    assert_all_pass(
        v.run(&[
            WarehouseOracle::RowCount,
            WarehouseOracle::DistinctId,
            WarehouseOracle::NullProfile,
            WarehouseOracle::TypeFidelity,
            // The cleanup side-effect: this cell loads with cleanup_source: true,
            // so the staging bucket must be empty afterward.
            WarehouseOracle::StagingWiped,
        ])
        .expect("pipeline"),
    );
}

// ── Incremental: cursor-delta append + a cursor-ordered current-state view,
// with cleanup_source (the matrix's incremental leg — no-loss, cursor dedup,
// never-tombstones, staging wiped). MySQL batch source (no binlog needed).
#[test]
#[ignore = "live: needs devbox mysql stack + BigQuery creds"]
fn incremental_dedup_mysql() {
    // Mode::Batch is a placeholder — run_incremental builds its own incremental
    // config (like run_mongo_cdc_delete); it does not route through the Mode enum.
    let v = Verification::new(Engine::Mysql, Mode::Batch, Fixture::smoke("inc_matrix"));
    assert_all_pass(v.run_incremental().expect("incremental"));
}

#[test]
#[ignore = "live smoke: mysql cdc soak → BigQuery"]
fn smoke_cdc_mysql() {
    let v = Verification::new(Engine::Mysql, Mode::Cdc, Fixture::smoke("content_items"));
    assert_all_pass(
        v.run_soak(
            &SoakConfig::quick(),
            &[WarehouseOracle::TypeFidelity],
            &[SoakOracle::FlatRss, SoakOracle::ZeroGap],
        )
        .expect("soak"),
    );
}

#[test]
#[ignore = "live smoke: postgres cdc soak → BigQuery"]
fn smoke_cdc_pg() {
    let v = Verification::new(Engine::Postgres, Mode::Cdc, Fixture::smoke("content_items"));
    assert_all_pass(
        v.run_soak(
            &SoakConfig::quick(),
            &[WarehouseOracle::TypeFidelity],
            &[SoakOracle::FlatRss, SoakOracle::ZeroGap],
        )
        .expect("soak"),
    );
}

#[test]
#[ignore = "live smoke: postgres cdc soak → Snowflake"]
fn smoke_cdc_pg_snowflake() {
    let v = Verification::new(Engine::Postgres, Mode::Cdc, Fixture::smoke("content_items"))
        .warehouse(Warehouse::Snowflake);
    assert_all_pass(
        v.run_soak(
            &SoakConfig::quick(),
            &[WarehouseOracle::TypeFidelity],
            &[SoakOracle::FlatRss, SoakOracle::ZeroGap],
        )
        .expect("soak"),
    );
}

#[test]
#[ignore = "live smoke: mysql cdc soak → Snowflake"]
fn smoke_cdc_mysql_snowflake() {
    let v = Verification::new(Engine::Mysql, Mode::Cdc, Fixture::smoke("content_items"))
        .warehouse(Warehouse::Snowflake);
    assert_all_pass(
        v.run_soak(
            &SoakConfig::quick(),
            &[WarehouseOracle::TypeFidelity],
            &[SoakOracle::FlatRss, SoakOracle::ZeroGap],
        )
        .expect("soak"),
    );
}

// ── Snowflake parity: the same batch pipeline into a second warehouse ─────────
#[test]
#[ignore = "live smoke: postgres batch → Snowflake"]
fn smoke_batch_pg_snowflake() {
    let v = Verification::new(
        Engine::Postgres,
        Mode::Batch,
        Fixture::smoke("content_items"),
    )
    .warehouse(Warehouse::Snowflake);
    assert_all_pass(
        v.run(&[
            WarehouseOracle::RowCount,
            WarehouseOracle::DistinctId,
            WarehouseOracle::NullProfile,
            // Guards the timestamptz UTC fix + jsonb→VARIANT + int-vs-decimal split.
            WarehouseOracle::TypeFidelity,
        ])
        .expect("pipeline"),
    );
}

// ── MySQL parity: the same full/batch pipeline into Snowflake (COPY FILES) ─────
// Mirrors smoke_batch_pg_snowflake with a MySQL source (engine-prefixed table
// `mysql_content_items`, so no collision with the PG cell). Validates the COPY
// FILES materialize path + type fidelity for a MySQL-sourced snapshot.
#[test]
#[ignore = "live smoke: mysql batch → Snowflake"]
fn smoke_batch_mysql_snowflake() {
    let v = Verification::new(Engine::Mysql, Mode::Batch, Fixture::smoke("content_items"))
        .warehouse(Warehouse::Snowflake);
    assert_all_pass(
        v.run(&[
            WarehouseOracle::RowCount,
            WarehouseOracle::DistinctId,
            WarehouseOracle::NullProfile,
            WarehouseOracle::TypeFidelity,
        ])
        .expect("pipeline"),
    );
}

// ── Mongo CDC: soft-delete tombstone in the current-state view ────────────────
#[test]
#[ignore = "live: needs the mongo replica-set devbox (rivet-mongo-rs-1) + Snowflake"]
fn mongo_cdc_delete_flag_snowflake() {
    let v = Verification::new(Engine::Mongo, Mode::Cdc, Fixture::smoke("cdc_del"))
        .warehouse(Warehouse::Snowflake);
    assert_all_pass(v.run_mongo_cdc_delete().expect("mongo cdc delete"));
}

#[test]
#[ignore = "live: needs the mongo replica-set devbox (rivet-mongo-rs-1) + BigQuery"]
fn mongo_cdc_delete_flag_bigquery() {
    let v = Verification::new(Engine::Mongo, Mode::Cdc, Fixture::smoke("cdc_del"))
        .warehouse(Warehouse::BigQuery);
    assert_all_pass(v.run_mongo_cdc_delete().expect("mongo cdc delete"));
}

// ── CDC backfill: initial:snapshot covers a preexisting table; the view keeps ─
// every backfilled row live (the snapshot/stream seam — NULL __op/__pos rows).
#[test]
#[ignore = "live: needs devbox mysql-cdc stack + BigQuery creds"]
fn cdc_backfill_snapshot_mysql() {
    // Engine-distinct table: CDC names the target from `table:`, so every engine's
    // backfill cell must own a table or they collide on `rivet_matrix.<t>__changes`
    // (an `id` vs `_id` schema clash, or silent cross-engine row contamination).
    let v = Verification::new(Engine::Mysql, Mode::Cdc, Fixture::smoke("cdc_bf_mysql"))
        .initial_snapshot();
    assert_all_pass(v.run_cdc_backfill().expect("cdc backfill"));
}

// Postgres parity: the slot-anchor CDC path (the config carries `slot:` from the
// PG lane, not `server_id:`) backfills the same way — anchor = slot creation.
#[test]
#[ignore = "live: needs devbox postgres-cdc stack + BigQuery creds"]
fn cdc_backfill_snapshot_pg() {
    let v = Verification::new(Engine::Postgres, Mode::Cdc, Fixture::smoke("cdc_bf_pg"))
        .initial_snapshot();
    assert_all_pass(v.run_cdc_backfill().expect("cdc backfill"));
}

// Mongo parity: no SQL client — seed/churn go through `mongosh`, `_id` is the
// dedup PK, and `initial: snapshot` covers the pre-existing documents.
#[test]
#[ignore = "live: needs the mongo replica-set devbox (rivet-mongo-rs-1) + BigQuery"]
fn cdc_backfill_snapshot_mongo() {
    let v = Verification::new(Engine::Mongo, Mode::Cdc, Fixture::smoke("cdc_bf_mongo"))
        .initial_snapshot();
    assert_all_pass(v.run_cdc_backfill().expect("cdc backfill"));
}

// Snowflake parity for the backfill: the dedup view uses PARSE_JSON(__pos) and
// the `COALESCE(__op='delete',FALSE)` guard must keep the backfill live there too.
#[test]
#[ignore = "live: needs devbox mysql-cdc stack + Snowflake (RIVET_SF_* + snow CLI)"]
fn cdc_backfill_snapshot_mysql_snowflake() {
    // Shares the MySQL source table with the BigQuery cell (different warehouse ⇒
    // no target collision); run one at a time.
    let v = Verification::new(Engine::Mysql, Mode::Cdc, Fixture::smoke("cdc_bf_mysql"))
        .initial_snapshot()
        .warehouse(Warehouse::Snowflake);
    assert_all_pass(v.run_cdc_backfill().expect("cdc backfill"));
}

// MSSQL backfill: BLOCKED — the SQL Server CDC lane is not wired in the harness
// (`Lane::of` gives it an empty `client: &[]` and no seed/anchor path), so
// `run_cdc_backfill` bails by design. This cell documents the gap so the missing
// engine is VISIBLE in the matrix, and pins that the bail is a clear MSSQL error
// rather than a silent skip; unignore once the MSSQL CDC lane is built out.
#[test]
#[ignore = "blocked: MSSQL CDC lane not wired in the harness (empty Lane client)"]
fn cdc_backfill_snapshot_mssql() {
    let v = Verification::new(Engine::Mssql, Mode::Cdc, Fixture::smoke("cdc_backfill"))
        .initial_snapshot();
    let err = v
        .run_cdc_backfill()
        .expect_err("MSSQL backfill must bail, not run")
        .to_string();
    assert!(
        err.contains("MSSQL"),
        "expected a clear MSSQL-not-wired bail: {err}"
    );
}
