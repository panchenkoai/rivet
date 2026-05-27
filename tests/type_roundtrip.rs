//! Type roundtrip proof suite (roadmap v0.7.8 — Phase 1).
//!
//! * **Offline** (`contract_*`): mapping contracts from `fixtures/expected_contracts.yaml`.
//! * **Live** (`*_parquet_roundtrip`, `*_csv_roundtrip`): require `docker compose up -d`.
//!
//! Run only this crate:
//!   `cargo test --test type_roundtrip`
//!   `make test-types`        — offline contracts (PR-fast)
//!   `make test-types-live`   — full matrix with docker

mod common;

#[path = "type_roundtrip/clickhouse_load.rs"]
mod clickhouse_load;
#[path = "type_roundtrip/compression_matrix.rs"]
mod compression_matrix;
#[path = "type_roundtrip/contract.rs"]
mod contract;
#[path = "type_roundtrip/csv_load.rs"]
mod csv_load;
#[path = "type_roundtrip/duckdb_load.rs"]
mod duckdb_load;
#[path = "type_roundtrip/helpers.rs"]
mod helpers;
#[path = "type_roundtrip/mysql_csv_roundtrip.rs"]
mod mysql_csv_roundtrip;
#[path = "type_roundtrip/mysql_parquet_roundtrip.rs"]
mod mysql_parquet_roundtrip;
#[path = "type_roundtrip/mysql_uint64_decimal_override.rs"]
mod mysql_uint64_decimal_override;
#[path = "type_roundtrip/parquet_metadata.rs"]
mod parquet_metadata;
#[path = "type_roundtrip/parquet_schema.rs"]
mod parquet_schema;
#[path = "type_roundtrip/pg_edge_cases.rs"]
mod pg_edge_cases;
#[path = "type_roundtrip/postgres_csv_roundtrip.rs"]
mod postgres_csv_roundtrip;
#[path = "type_roundtrip/postgres_parquet_roundtrip.rs"]
mod postgres_parquet_roundtrip;
#[path = "type_roundtrip/pyarrow_load.rs"]
mod pyarrow_load;
