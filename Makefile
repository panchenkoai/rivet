# Rivet developer shortcuts.
# Requires Rust 1.94+ (see rust-toolchain.toml if present).

.PHONY: test-types test-types-live test-types-property test-types-validators test-types-bigquery test-types-snowflake

# PR-fast: offline type-mapping contracts (no docker).
test-types:
	cargo test --test type_roundtrip contract_

# Full type matrix: MySQL + PostgreSQL × Parquet + CSV (docker required).
test-types-live:
	cargo test --test type_roundtrip -- --include-ignored

# Property-based value round-trip (OPT-3): random in-range values → MySQL →
# Parquet → read-back, asserting every value survives. Requires `docker compose
# up -d mysql`. Tune case count with PROPTEST_CASES (default 12).
test-types-property:
	cargo test --test type_roundtrip mysql_value_roundtrip -- --ignored

# Independent-reader validators: PG/MySQL matrix → Parquet → {DuckDB, ClickHouse}.
# Requires `docker compose up -d postgres mysql duckdb clickhouse` first.
# See ADR-0014; the duckdb + clickhouse services are oracles for the Parquet
# layer, not productive components.
test-types-validators:
	cargo test --test type_roundtrip duckdb_validates clickhouse_validates -- --ignored

# Cloud validator: PG/MySQL matrix → Parquet → BigQuery (real warehouse oracle).
# Requires:
#   - `bq` CLI on PATH and authenticated (`gcloud auth application-default login`).
#   - BIGQUERY_TEST_PROJECT env var. Optional: BIGQUERY_TEST_DATASET (default
#     `rivet_type_lab`), BIGQUERY_TEST_LOCATION (default `EU`).
#   - docker-compose postgres + mysql for the source databases.
# Mirrors the docs/recipes/snowflake-load.md fidelity table — pins what
# BigQuery's autoload actually does to rivet Parquet today.
# Example: `BIGQUERY_TEST_PROJECT=my-proj make test-types-bigquery`.
test-types-bigquery:
	cargo test --test type_roundtrip bigquery_validates -- --include-ignored --test-threads=1

# Cloud validator: PG matrix → Parquet → Snowflake (real warehouse oracle).
# The CI guardian for the Snowflake resolver claims in src/types/target.rs —
# asserts INFER_SCHEMA autoload degradations + the recovery casts against a
# live account. Requires:
#   - `snow` CLI on PATH (NOT `snowsql`).
#   - SNOWFLAKE_TEST_CONNECTION env (the connection name). Optional:
#     SNOWFLAKE_TEST_PRIVATE_KEY (absolute .p8 path if the connection's
#     private_key_file uses a literal `~`), SNOWFLAKE_TEST_DATABASE / _SCHEMA.
#   - docker-compose postgres for the source database.
# Example: `SNOWFLAKE_TEST_CONNECTION=rivet make test-types-snowflake`.
test-types-snowflake:
	cargo test --test type_roundtrip snowflake_validates -- --include-ignored --test-threads=1
