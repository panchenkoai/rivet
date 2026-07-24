# Rivet developer shortcuts.
# Requires Rust 1.94+ (see rust-toolchain.toml if present).

.PHONY: test-types test-types-live test-types-property test-types-validators test-types-bigquery test-types-snowflake sweep-test-db test-live seed-build seed-db seed-postgres seed-mysql seed-mssql seed-mongo

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
	cargo test --test type_roundtrip duckdb_validates -- --ignored --test-threads=1
	cargo test --test type_roundtrip clickhouse_validates -- --ignored --test-threads=1

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

# Drop test-fixture tables left behind by INTERRUPTED live runs (a killed test
# process skips the RAII Drop guard, so the slow cloud suites can leak
# `<prefix>_<pid>_<counter>` tables into the shared `rivet` fixture DB). Safe
# anytime — only touches those fixtures, never the init.sql / seed.rs tables.
# Best-effort per engine: a down service is skipped. See dev/sweep-test-cruft.sh.
sweep-test-db:
	bash dev/sweep-test-cruft.sh

# Full live suite under nextest (per-test isolation), sweeping stale fixtures
# FIRST so an interrupted prior run never pollutes the shared `rivet` DB.
# Requires `docker compose up -d` (postgres + mysql + the validator containers).
test-live: sweep-test-db
	cargo nextest run --run-ignored all

# ── Fixture seed ────────────────────────────────────────────────────────────
# Regenerate the shared live-test dataset (orders/users/events/page_views/
# content_items). The SQL seed tool is gated behind the off-by-default
# `dev-seed` feature (so `cargo install` never ships the destructive TRUNCATE
# tool); RIVET_SEED_I_KNOW=1 confirms the TRUNCATE…CASCADE. Standard size mirrors
# nightly-live.yml — 60k content_items (~1 min) is enough to trigger WAL pressure;
# the 1M-row `max_pressure` test stays manual (override SEED_ARGS to change).
SEED_ARGS ?= --users 1000 --orders-per-user 5 --events-per-user 5 --page-views 5000 --content-items 60000

seed-build:
	cargo build --bin seed --features dev-seed

# Seed EVERY live database. A per-DB target below runs one engine at a time.
seed-db: seed-postgres seed-mysql seed-mssql seed-mongo

seed-postgres: seed-build
	RIVET_SEED_I_KNOW=1 target/debug/seed --target postgres $(SEED_ARGS)

seed-mysql: seed-build
	RIVET_SEED_I_KNOW=1 target/debug/seed --target mysql $(SEED_ARGS)

seed-mssql: seed-build
	RIVET_SEED_I_KNOW=1 target/debug/seed --target sqlserver $(SEED_ARGS)

# Mongo has no shared bench fixture (the live suite self-seeds each collection
# via the Rig), so this seeds a standard `rivet.orders`/`rivet.users` collection
# for demos + manual runs. Idempotent (drops + rebuilds). See dev/mongo/seed.js.
seed-mongo:
	docker compose exec -T mongo mongosh --quiet "mongodb://127.0.0.1:27017/rivet?directConnection=true" < dev/mongo/seed.js
