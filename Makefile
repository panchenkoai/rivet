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
# generator and the DEFAULT profile is deterministic by construction: `fast`
# generates every row SQL-side from a series (`generate_series` / a chunked
# range), and `fast.rs` + `mssql.rs` contain ZERO `rand::rng()` calls. The same
# command therefore yields byte-identical fixtures on PostgreSQL, MySQL and SQL
# Server, which is what makes a cross-engine comparison meaningful.
#
# The claim is now TRUE and measured; it was not before, in two layers. The
# comment said "no RNG anywhere in `src/bin/seed/`" — false, `copy_pg.rs` and
# `insert.rs` call `rand::rng()` in ten places (those are the `realistic` /
# `insert` profiles, which remain non-deterministic BY DESIGN and must never be
# used for a cross-engine comparison). Correcting only that would still have
# left the sentence wrong, because `fast.rs` called SQL-side `random()` five
# times: postgres drew `balance` / `is_active` / `bio` and two page_views
# columns at random, so the DEFAULT profile was not reproducible even run to
# run. Worse, the two engines that WERE deterministic disagreed: MySQL wrote
# `bio` when `i %% 3 = 0` and SQL Server when `i %% 3 <> 0` — inverted, and the
# same inversion on `page_views.user_id`.
#
# All five sites now use SQL Server's index-derived formulas, which had no
# randomness to begin with. Verified engine-neutrally: exporting `users` from
# each engine and hashing the parquet through DuckDB yields one digest,
# 4b7ee9027565608f23ca39aae2d10f23, on all three. A hash computed with each
# engine's OWN string concatenation does NOT agree — that is type rendering,
# not data, and it is why the check has to go through a neutral reader.
#
# NOT the default: 1M content_items is ~17x the standard 60k (~1 min), and the
# everyday `make seed-db` should stay fast. Run this before the release matrix.
RELEASE_SEED_ARGS ?= --users 1000 --orders-per-user 5 --events-per-user 5 --page-views 5000 --content-items 1000000

# `--target all` covers postgres + mysql + sqlserver in ONE invocation. It used
# to be two calls naming postgres and mysql, which silently left SQL Server on
# whatever an earlier run had put there — and a stand where the engines hold
# DIFFERENT row counts makes every cross-engine comparison meaningless, which is
# the one thing this fixture exists to enable. Measured 2026-08-04, before the
# fix: postgres 1000/5000/1000521, mysql 1000/5000/1000000, mssql 150000/500/150000.
seed-release: seed-build
	RIVET_SEED_I_KNOW=1 target/debug/seed --target all $(RELEASE_SEED_ARGS)

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

# The BARE target carries no baseline, so it must GIVE UP the prev-release
# comparison BY NAME. Since a missing `RIVET_PREV_RELEASE_BIN` became a FAIL
# rather than a SKIP (a check that grades nothing must fail — 0.24.4 shipped a
# +1h48m regression through a green gate whose only comparison leg SKIPped),
# a bare invocation without this flag goes red on three stages for a reason that
# has nothing to do with what it is checking. `--without-prev-release-comparison`
# is the deliberate, named escape: it says out loud, in every row it records and
# in the summary, that this run cannot support a tag.
release-oracle:  ## Release gate, BARE: only what is already in your shell, and the prev-release comparison GIVEN UP by name (cannot support a tag). Read the SKIP count — with nothing set it is ~95 PASS / 60 SKIP and still prints RELEASE-READY.
	python3 -m dev.release_oracle --without-prev-release-comparison $(ARGS)

# ─── the gate's environment, assembled ────────────────────────────────────────
#
# Every URL below points at the CANONICAL dev stand in docker-compose.yaml, and
# every one is `?=` so a caller can override a single service without editing
# this file.
#
# WHY THIS EXISTS. The gate is env-driven and SKIPs — never silently passes —
# when a URL is absent. That is the honest behaviour and it has a trap: a run
# with none of these set is ~95 PASS / 60 SKIP and prints RELEASE-READY, which
# is literally true ("every non-skipped cell is green") and nearly meaningless.
# Assembling twenty variables by hand before every release means the verdict
# depends on someone remembering them. It did: the first end-to-end pass with
# the full environment surfaced SIX latent defects in the gate's own scaffolding
# that no partial run could reach.
#
# The CDC ports are NOT the batch ports. `mysql-cdc` is 3307 and `mssql-cdc` is
# 1434 — separate services with binlog / CDC + Agent enabled. Pointing the CDC
# legs at 3306/1433 makes them fail in a way that reads like a product defect.
RIVET_ORACLE_POSTGRES_URL ?= postgresql://rivet:rivet@127.0.0.1:5432/rivet
RIVET_ORACLE_MYSQL_URL    ?= mysql://rivet:rivet@127.0.0.1:3306/rivet
RIVET_ORACLE_MSSQL_URL    ?= mssql://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet
RIVET_ORACLE_MONGO_URL    ?= mongodb://127.0.0.1:27017/rivet
RIVET_CDC_POSTGRES_URL    ?= postgresql://rivet:rivet@127.0.0.1:5434/rivet
RIVET_CDC_MYSQL_URL       ?= mysql://rivet:rivet@127.0.0.1:3307/rivet
RIVET_CDC_MSSQL_URL       ?= mssql://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet
# `directConnection=true`, not `replicaSet=rs0`: the set advertises a member
# address the host cannot resolve, so the driver reports ReplicaSetNoPrimary and
# every mongo CDC cell dies at init.
RIVET_CDC_MONGO_URL       ?= mongodb://127.0.0.1:27018/rivet?directConnection=true
# One Postgres state DB serves every "grade the other backend" leg.
RIVET_GATE_STATE_URL      ?= postgresql://rivet:rivet@127.0.0.1:5433/rivet_state
RIVET_SWEEP_STATE_CONTAINER ?= rivet-postgres-state-1
RIVET_CONC_SRC_CONTAINER  ?= rivet-postgres-1
# Not hard-coded to anyone's project: whatever `gcloud` is pointed at. Empty ⇒
# the BigQuery legs SKIP with that reason, which is correct on a machine with no
# warehouse.
BQ_ORACLE_PROJECT         ?= $(shell gcloud config get-value project 2>/dev/null)
BQ_ORACLE_DATASET         ?= rivet_blessed
BQ_ORACLE_BUCKET          ?= rivet_data_test
# OUTSIDE target/: the oracle's first act is `cargo clean` (the gate builds the
# binary it grades), which silently deleted a baseline downloaded into
# target/prev-release — the regression leg then "declared-skipped" on every
# single full run while the download step reported success. A baseline the
# gate needs must live where the gate's own hygiene cannot eat it.
PREV_RELEASE_DIR          ?= .gate-baseline

GATE_ENV = \
  RIVET_ORACLE_POSTGRES_URL='$(RIVET_ORACLE_POSTGRES_URL)' \
  RIVET_ORACLE_MYSQL_URL='$(RIVET_ORACLE_MYSQL_URL)' \
  RIVET_ORACLE_MSSQL_URL='$(RIVET_ORACLE_MSSQL_URL)' \
  RIVET_ORACLE_MONGO_URL='$(RIVET_ORACLE_MONGO_URL)' \
  RIVET_REGRESSION_SOURCE_URL='$(RIVET_ORACLE_POSTGRES_URL)' \
  RIVET_CDC_POSTGRES_URL='$(RIVET_CDC_POSTGRES_URL)' \
  RIVET_CDC_MYSQL_URL='$(RIVET_CDC_MYSQL_URL)' \
  RIVET_CDC_MSSQL_URL='$(RIVET_CDC_MSSQL_URL)' \
  RIVET_CDC_MONGO_URL='$(RIVET_CDC_MONGO_URL)' \
  RIVET_CDC_STATE_URL='$(RIVET_GATE_STATE_URL)' \
  RIVET_CONC_STATE_URL='$(RIVET_GATE_STATE_URL)' \
  RIVET_GATE_STATE_URL='$(RIVET_GATE_STATE_URL)' \
  RIVET_TEST_STATE_URL='$(RIVET_GATE_STATE_URL)' \
  RIVET_SWEEP_STATE_CONTAINER='$(RIVET_SWEEP_STATE_CONTAINER)' \
  RIVET_CONC_SRC_CONTAINER='$(RIVET_CONC_SRC_CONTAINER)' \
  RIVET_CONC_SRC_URL='$(RIVET_ORACLE_POSTGRES_URL)' \
  BQ_ORACLE_PROJECT='$(BQ_ORACLE_PROJECT)' \
  BQ_ORACLE_DATASET='$(BQ_ORACLE_DATASET)' \
  BQ_ORACLE_BUCKET='$(BQ_ORACLE_BUCKET)'

release-oracle-prev-bin:  ## Download the PREVIOUS release binary (the regression + scale baseline). A downloaded asset, never a locally rebuilt parent.
	@rm -rf $(PREV_RELEASE_DIR) && mkdir -p $(PREV_RELEASE_DIR)  # exactly one baseline: `ls | tail` below must not pick a lexicographically-wrong version from an accumulating dir (bug hunt 2026-08-08)
	@tag=$$(gh release list --limit 1 --json tagName -q '.[0].tagName'); \
	 arch=$$(uname -m); os=$$(uname -s | tr 'A-Z' 'a-z'); \
	 [ "$$arch" = "arm64" ] && arch=aarch64; \
	 case "$$os" in darwin) triple="$$arch-apple-darwin";; *) triple="$$arch-unknown-linux-gnu";; esac; \
	 echo "  downloading $$tag ($$triple) — the artifact users actually run"; \
	 gh release download "$$tag" --pattern "rivet-$$tag-$$triple.tar.gz" --dir $(PREV_RELEASE_DIR) --clobber; \
	 tar -xzf "$(PREV_RELEASE_DIR)/rivet-$$tag-$$triple.tar.gz" -C $(PREV_RELEASE_DIR); \
	 chmod +x "$(PREV_RELEASE_DIR)/rivet-$$tag-$$triple/rivet"; \
	 echo "  $$($(PREV_RELEASE_DIR)/rivet-$$tag-$$triple/rivet --version) → $(PREV_RELEASE_DIR)/rivet-$$tag-$$triple/rivet"

release-oracle-full: release-oracle-prev-bin  ## Release gate with the WHOLE environment against the local dev stand. This is the one a release is judged by.
	@# The gate grades `target/release/rivet`. A stale one grades yesterday's
	@# code, and `cargo package` poisons the fingerprints so cargo reports
	@# `Fresh` on a binary that predates your edits — drop the snapshot first.
	@rm -rf target/package
	cargo build --release
	@# newest by mtime, not lexical order (accumulating dir would mis-tail)
	@# An ABSENT baseline is now a FAIL, not a SKIP, on all three prev-release
	@# stages (release regression / previous-release differential / field symptom
	@# replay) — so the echo says FAIL and names the escape. A gate run that
	@# reaches here with an empty $$prev is telling you the download failed.
	@prev=$$(ls -t -d $(PREV_RELEASE_DIR)/rivet-v*/rivet 2>/dev/null | head -1); \
	 echo "  previous release: $${prev:-<none — the scale legs will SKIP and the regression / differential / field-replay legs will FAIL; re-run release-oracle-prev-bin, or give the comparison up by name with ARGS=--without-prev-release-comparison>}"; \
	 env $(GATE_ENV) RIVET_PREV_RELEASE_BIN="$$prev" python3 -m dev.release_oracle $(ARGS)

release-oracle-bless: release-oracle-prev-bin  ## Re-capture the verdict + duckdb-type goldens. Deliberate: a golden must be written by rivet's own code, never edited by hand.
	@rm -rf target/package
	cargo build --release
	@# It DEPENDS on release-oracle-prev-bin, so it downloads a baseline — and
	@# used to then not pass it, failing three stages over the absence of a
	@# binary it had just fetched. Same threading as release-oracle-full.
	@prev=$$(ls -t -d $(PREV_RELEASE_DIR)/rivet-v*/rivet 2>/dev/null | head -1); \
	 echo "  previous release: $${prev:-<none — the scale legs will SKIP and the regression / differential / field-replay legs will FAIL; re-run release-oracle-prev-bin, or give the comparison up by name with ARGS=--without-prev-release-comparison>}"; \
	 env $(GATE_ENV) RIVET_PREV_RELEASE_BIN="$$prev" python3 -m dev.release_oracle --bless-local $(ARGS)
