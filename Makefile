# Rivet developer shortcuts.
# Requires Rust 1.94+ (see rust-toolchain.toml if present).

.PHONY: test-types test-types-live test-types-property test-types-validators test-types-bigquery test-types-snowflake sweep-test-db test-live seed-build seed-db seed-postgres seed-mysql seed-mssql seed-mongo seed-garbage seed-garbage-postgres seed-garbage-mysql seed-garbage-mssql

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
# Best-effort per engine: a down service is skipped. See dev/pytools/sweep.py.
sweep-test-db:
	python3 -m dev.pytools.sweep test-cruft

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

# ── Release-size fixture ────────────────────────────────────────────────────
# The PRE-RELEASE size. `live_content_load::pg_full_content_export_max_pressure`
# asserts `content_items >= 1_000_000` and FAILS — it does not skip — below that,
# so `cargo test --release -- --ignored` (release-checklist §2) cannot pass on a
# standard-seeded stand. That is a gate that reports red for a fixture reason,
# which is exactly the noise a go/no-go check must not produce.
#
# Fixed in the SEED rather than in the test, because the seed is the canonical
# generator for every engine and is deterministic by construction: every row
# comes from a SQL series (`generate_series` / a chunked range), with no RNG
# anywhere in `src/bin/seed/`. The same command therefore yields byte-identical
# fixtures on PostgreSQL, MySQL and SQL Server, which is what makes a
# cross-engine comparison meaningful in the first place.
#
# NOT the default: 1M content_items is ~17x the standard 60k (~1 min), and the
# everyday `make seed-db` should stay fast. Run this before the release matrix.
RELEASE_SEED_ARGS ?= --users 1000 --orders-per-user 5 --events-per-user 5 --page-views 5000 --content-items 1000000

seed-release: seed-build
	RIVET_SEED_I_KNOW=1 target/debug/seed --target postgres $(RELEASE_SEED_ARGS)
	RIVET_SEED_I_KNOW=1 target/debug/seed --target mysql $(RELEASE_SEED_ARGS)

seed-build:
	cargo build --bin seed --features dev-seed

# Seed EVERY live database. A per-DB target below runs one engine at a time.
seed-db: seed-postgres seed-mysql seed-mssql seed-mongo

seed-postgres: seed-build
	RIVET_SEED_I_KNOW=1 target/debug/seed --target postgres $(SEED_ARGS)

seed-mysql: seed-build
	RIVET_SEED_I_KNOW=1 target/debug/seed --target mysql $(SEED_ARGS)

# NOTE: the seed tool's SQL Server `dbo.orders` (rich benchmark shape: user_id /
# product / price DECIMAL(10,2) / …) DIVERGES from dev/mssql/init.sql's `dbo.orders`
# (simple id / name / amount DECIMAL(12,2) — a decimal-precision test fixture that
# `audit_init_deferred::init_mssql_*` asserts on). CI seeds only Postgres, so its
# MSSQL fixture stays init.sql. Running `seed-mssql` locally therefore overwrites
# that test fixture; re-apply dev/mssql/init.sql (the `dbo.orders` section) if you
# then run the MSSQL init tests. PG/MySQL init.sql agree with the seed tool's rich
# shape, so this only bites MSSQL.
seed-mssql: seed-build
	RIVET_SEED_I_KNOW=1 target/debug/seed --target sqlserver $(SEED_ARGS)

# Mongo has no shared bench fixture (the live suite self-seeds each collection
# via the Rig), so this seeds a standard `rivet.orders`/`rivet.users` collection
# for demos + manual runs. Idempotent (drops + rebuilds). See dev/mongo/seed.js.
seed-mongo:
	docker compose exec -T mongo mongosh --quiet "mongodb://127.0.0.1:27017/rivet?directConnection=true" < dev/mongo/seed.js

# ── Garbage-profile fixture ─────────────────────────────────────────────────
# Materialize the obfuscated GARBAGE profile — the anonymized SHAPE of a real
# 200+-table field DB (heterogeneous PK, dual timestamps, non-default schema,
# sparse spans, scale-0 decimal key, BIGINT UNSIGNED past i64::MAX, a wide table)
# — as persistent tables in a NON-default schema/database `ext`, for manual runs
# + exploration. ZERO source identity, ZERO real data. The automated verification
# of these shapes lives in the offline oracle (src/init/catalog_replay.rs) + the
# live stand (tests/live/chunking_stand.rs), which self-seed; this target is for
# poking at rivet by hand against the profile. Idempotent. The sweep never touches
# `ext.*`, so it persists. See docs/cli-flag-matrix.yaml + dev/garbage/*.sql.
seed-garbage: seed-garbage-postgres seed-garbage-mysql seed-garbage-mssql

seed-garbage-postgres:
	docker compose exec -T postgres psql -U rivet -d rivet -v ON_ERROR_STOP=1 -f - < dev/garbage/postgres.sql

seed-garbage-mysql:
	docker compose exec -T mysql mysql -urivet -privet rivet < dev/garbage/mysql.sql

seed-garbage-mssql:
	docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -C -d rivet -b -i /dev/stdin < dev/garbage/mssql.sql

release-oracle:  ## Release gate: every engine×version × scenario against local MinIO/fake-gcs/Azurite + a BigQuery golden. Green everywhere ⇒ releasable.
	python3 -m dev.release_oracle
