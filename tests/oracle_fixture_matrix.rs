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
        ])
        .expect("pipeline"),
    );
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
