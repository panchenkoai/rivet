#!/usr/bin/env bash
#
# Regenerate all Rivet instructional GIFs using VHS (charmbracelet/vhs).
#
# Prerequisites:
#   - `vhs`, `ttyd`, `ffmpeg` on PATH (Homebrew: `brew install vhs`)
#   - Docker Desktop running with the repo's `docker-compose.yaml` stack:
#       docker compose up -d postgres mysql
#   - target/release/rivet and target/release/seed built
#       (the script builds them if missing).
#
# Each scenario:
#   1. Preps an ephemeral /tmp/rivet-gif-<name> workdir.
#   2. Seeds any required database fixtures on the primary Postgres (port 5432).
#   3. Runs `vhs <name>.tape` inside that workdir -> produces <name>.gif.
#   4. Drops the fixtures.
#
# Safe to run repeatedly. Ephemeral fixtures live in schema `rivet_gif` and
# tables prefixed `rivet_gif_` so we never touch user data.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
BIN="$REPO_ROOT/target/release/rivet"
SEED="$REPO_ROOT/target/release/seed"

PG_HOST="${PGHOST:-localhost}"
PG_PORT="${PGPORT:-5432}"
PG_USER="${PGUSER:-rivet}"
PG_PASSWORD="${PGPASSWORD:-rivet}"
PG_DB="${PGDATABASE:-rivet}"
DATABASE_URL="postgresql://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DB}"

export PGPASSWORD="$PG_PASSWORD"
psql_() {
    if command -v psql >/dev/null 2>&1; then
        psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 "$@"
    elif [[ -x /opt/homebrew/opt/libpq/bin/psql ]]; then
        /opt/homebrew/opt/libpq/bin/psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 "$@"
    else
        docker compose -f "$REPO_ROOT/docker-compose.yaml" exec -T postgres \
            psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 "$@"
    fi
}

log() { printf '\033[1;34m[gifs]\033[0m %s\n' "$*" >&2; }

ensure_prereqs() {
    command -v vhs >/dev/null || { echo "vhs not found — brew install vhs" >&2; exit 1; }
    command -v ttyd >/dev/null || { echo "ttyd not found — brew install ttyd" >&2; exit 1; }
    command -v ffmpeg >/dev/null || { echo "ffmpeg not found — brew install ffmpeg" >&2; exit 1; }

    if [[ ! -x "$BIN" || ! -x "$SEED" ]]; then
        log "building release binaries..."
        (cd "$REPO_ROOT" && cargo build --release --bin rivet --bin seed --features dev-seed)
    fi

    if ! psql_ -c 'SELECT 1' >/dev/null 2>&1; then
        echo "postgres not reachable at $DATABASE_URL — run 'docker compose up -d postgres'" >&2
        exit 1
    fi
}

# ----- Fixture management ----------------------------------------------------

fixture_basic_setup() {
    # Keep `public.orders` in the chunked band (~250K rows) so `rivet init`
    # picks chunked mode and `rivet run` finishes inside the GIF window.
    psql_ <<'SQL'
TRUNCATE orders RESTART IDENTITY CASCADE;
INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at)
SELECT (g % 500) + 1,
       'sku-' || g,
       (g % 5) + 1,
       (g % 100) + 0.99,
       'pending',
       'gif-fixture',
       NOW() - (g || ' minutes')::interval,
       NOW() - (g || ' minutes')::interval
FROM generate_series(1, 250000) AS g;
ANALYZE orders;
SQL
}
fixture_basic_teardown() { :; }

# Scaffolding (rivet init) only needs the live DB; nothing else to prepare.
fixture_init_setup() { :; }
fixture_init_teardown() { :; }

# `rivet check` expects orders.yaml to already exist in the workdir.
fixture_check_setup() {
    local work="/tmp/rivet-gif-check-verdict"
    mkdir -p "$work"
    (
        cd "$work"
        DATABASE_URL="$DATABASE_URL" \
        "$BIN" init --source-env DATABASE_URL --table orders -o orders.yaml >/dev/null
    )
}
fixture_check_teardown() { :; }

# `inspect` needs a prior incremental run so state show / metrics / files /
# progression all have data. The fixture runs chunked so progression is
# populated (incremental-only runs leave progression empty for Epic G).
fixture_inspect_setup() {
    local work="/tmp/rivet-gif-inspect"
    mkdir -p "$work"

    # Use a fresh rivet_gif.events fixture so we can predict counts.
    fixture_chunked_setup

    cat >"$work/orders_incremental.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: events
    query: "SELECT id, user_id, event_type, payload, created_at FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 2500
    chunk_checkpoint: true
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
YAML

    (
        cd "$work"
        DATABASE_URL="$DATABASE_URL" \
        "$BIN" run --config orders_incremental.yaml --validate >/dev/null 2>&1
        DATABASE_URL="$DATABASE_URL" \
        "$BIN" reconcile --config orders_incremental.yaml --export events >/dev/null 2>&1 || true
    )
}
fixture_inspect_teardown() {
    fixture_chunked_teardown
}

# 50k-row fixture so the progress bar has enough chunks to visibly advance.
fixture_chunked_progress_setup() {
    psql_ <<'SQL'
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;
CREATE TABLE rivet_gif.events (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO rivet_gif.events (id, user_id, event_type, payload)
SELECT g, (g % 500) + 1,
       CASE (g % 3) WHEN 0 THEN 'page_view' WHEN 1 THEN 'click' ELSE 'purchase' END,
       repeat('payload-', 8) || g
FROM generate_series(1, 50000) AS g;
SQL

    local work="/tmp/rivet-gif-chunked-progress"
    mkdir -p "$work"
    cat >"$work/chunked-progress.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    # Small batches + per-batch throttle so the progress bar has visible,
    # readable ticks. Demo only — use `balanced` / `fast` in production.
    batch_size: 1000
    throttle_ms: 80
exports:
  - name: events
    query: "SELECT id, user_id, event_type, payload, created_at FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
YAML
}
fixture_chunked_progress_teardown() { fixture_chunked_teardown; }

# Multi-export `--parallel-export-processes` demo: four narrow tables seeded
# under `rivet_gif.*` so the IPC card UI has four cards advancing in parallel.
fixture_parallel_cards_setup() {
    psql_ <<'SQL'
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;

CREATE TABLE rivet_gif.orders (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.orders (id, payload)
SELECT g, repeat('o', 32) FROM generate_series(1, 60000) AS g;

CREATE TABLE rivet_gif.users (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.users (id, payload)
SELECT g, repeat('u', 32) FROM generate_series(1, 30000) AS g;

CREATE TABLE rivet_gif.events (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.events (id, payload)
SELECT g, repeat('e', 32) FROM generate_series(1, 80000) AS g;

CREATE TABLE rivet_gif.sessions (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.sessions (id, payload)
SELECT g, repeat('s', 32) FROM generate_series(1, 40000) AS g;
SQL

    local work="/tmp/rivet-gif-parallel-cards"
    mkdir -p "$work"
    cat >"$work/parallel-cards.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    # Small batches + an exaggerated per-batch throttle so each card has
    # visibly ticking progress bars during the recording window.  Demo
    # only — production runs use `balanced` / `fast`.
    batch_size: 1000
    throttle_ms: 250
exports:
  - name: orders
    query: "SELECT id, payload FROM rivet_gif.orders"
    mode: chunked
    chunk_column: id
    chunk_size: 10000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
  - name: users
    query: "SELECT id, payload FROM rivet_gif.users"
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
  - name: events
    query: "SELECT id, payload FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 10000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
  - name: sessions
    query: "SELECT id, payload FROM rivet_gif.sessions"
    mode: chunked
    chunk_column: id
    chunk_size: 5000
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
YAML
}
fixture_parallel_cards_teardown() { fixture_chunked_teardown; }

# Reuses the 10k-row rivet_gif.events fixture, adds an incremental YAML.
fixture_incremental_setup() {
    fixture_chunked_setup    # 10k rows in rivet_gif.events

    local work="/tmp/rivet-gif-incremental-cursor"
    mkdir -p "$work"
    cat >"$work/incremental.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: events
    query: "SELECT id, user_id, event_type, payload, created_at FROM rivet_gif.events"
    mode: incremental
    cursor_column: id
    format: parquet
    skip_empty: true
    destination:
      type: local
      path: ./output
YAML
}
fixture_incremental_teardown() { fixture_chunked_teardown; }

# Composite cursor (COALESCE) demo: orders with ~35% NULL updated_at so
# `incremental_cursor_mode: coalesce` is the only way to track progression
# without losing the NULL-only rows. ADR-0007 CC1–CC6.
fixture_coalesce_setup() {
    psql_ <<'SQL'
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;

CREATE TABLE rivet_gif.orders (
    id          BIGINT PRIMARY KEY,
    product     TEXT          NOT NULL,
    quantity    INT           NOT NULL,
    price       NUMERIC(10,2) NOT NULL,
    updated_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- 200 rows; ~35% with updated_at NULL (deterministic via id % 100 < 35).
INSERT INTO rivet_gif.orders (id, product, quantity, price, updated_at, created_at)
SELECT g,
       'sku-' || g,
       (g % 10) + 1,
       (g % 100) + 0.99,
       CASE WHEN g % 100 < 35 THEN NULL ELSE NOW() - (g || ' minutes')::interval END,
       NOW() - ((g + 200) || ' minutes')::interval
FROM generate_series(1, 200) AS g;
SQL

    local work="/tmp/rivet-gif-coalesce-cursor"
    mkdir -p "$work"
    cat >"$work/orders.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: orders
    query: "SELECT id, product, quantity, price, updated_at, created_at FROM rivet_gif.orders"
    mode: incremental
    cursor_column: updated_at
    cursor_fallback_column: created_at
    incremental_cursor_mode: coalesce
    format: parquet
    skip_empty: true
    destination:
      type: local
      path: ./output
YAML
}
fixture_coalesce_teardown() { fixture_chunked_teardown; }

# Discovery artifact demo uses the already-seeded public.* schema. jq must
# be on PATH (brew install jq).
fixture_discover_setup() {
    command -v jq >/dev/null || { echo "jq not found — brew install jq" >&2; exit 1; }
}
fixture_discover_teardown() { :; }

# Multi-export campaign with source_group collision. Two chunked exports
# share replica_main -> `shared_source_heavy_conflict` + campaign warning.
fixture_campaign_setup() {
    # Two large tables (> 10M rows) so `classify_cost` returns
    # CostClass::High / VeryHigh; shared `source_group` in the YAML then
    # triggers `shared_source_heavy_conflict` + a campaign-level warning.
    # Narrow rows + generate_series + UNLOGGED make this fast (~20–40 s).
    psql_ <<'SQL'
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;

CREATE UNLOGGED TABLE rivet_gif.events (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.events (id, payload)
SELECT g, 'p' FROM generate_series(1, 20000000) AS g;

CREATE UNLOGGED TABLE rivet_gif.events_archive (
    id BIGINT PRIMARY KEY,
    payload TEXT
);
INSERT INTO rivet_gif.events_archive (id, payload)
SELECT g, 'a' FROM generate_series(1, 15000000) AS g;

ANALYZE rivet_gif.events;
ANALYZE rivet_gif.events_archive;
SQL

    local work="/tmp/rivet-gif-plan-campaign"
    mkdir -p "$work"
    cat >"$work/campaign.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: events
    query: "SELECT id, payload FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 500000
    source_group: replica_main
    format: parquet
    destination:
      type: local
      path: ./output

  - name: events_archive
    query: "SELECT id, payload FROM rivet_gif.events_archive"
    mode: chunked
    chunk_column: id
    chunk_size: 500000
    source_group: replica_main
    format: parquet
    destination:
      type: local
      path: ./output

  - name: users_dim
    query: "SELECT id, email FROM users"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
YAML
}
fixture_campaign_teardown() { fixture_chunked_teardown; }

# Pooler / proxy detection demo. Opt-in: requires the `pool` docker-compose
# profile to be up so pgBouncer (6432) and ProxySQL (6033) are reachable.
#   docker compose --profile pool up -d pgbouncer proxysql
# The four YAML fixtures point at the local docker stack — direct PG (5432),
# pgBouncer (6432), direct MySQL (3306), ProxySQL (6033).
fixture_pool_detect_setup() {
    local work="/tmp/rivet-gif-pool-detect"
    mkdir -p "$work"

    # Probe-only: each rivet check exits in <1s and emits the connect-time
    # warning (if any) before the strategy/verdict block. SELECT 1 keeps the
    # check itself trivial so the warning is the focus.
    cat >"$work/direct-pg.yaml" <<'YAML'
source:
  type: postgres
  url: "postgresql://rivet:rivet@localhost:5432/rivet"
exports:
  - name: probe
    query: "SELECT 1 AS n"
    mode: full
    format: parquet
    destination: {type: local, path: ./output}
YAML

    cat >"$work/bouncer-pg.yaml" <<'YAML'
source:
  type: postgres
  url: "postgresql://rivet:rivet@localhost:6432/rivet"
exports:
  - name: probe
    query: "SELECT 1 AS n"
    mode: full
    format: parquet
    destination: {type: local, path: ./output}
YAML

    cat >"$work/direct-mysql.yaml" <<'YAML'
source:
  type: mysql
  url: "mysql://rivet:rivet@127.0.0.1:3306/rivet"
exports:
  - name: probe
    query: "SELECT 1 AS n"
    mode: full
    format: parquet
    destination: {type: local, path: ./output}
YAML

    cat >"$work/proxysql.yaml" <<'YAML'
source:
  type: mysql
  url: "mysql://rivet:rivet@127.0.0.1:6033/rivet"
exports:
  - name: probe
    query: "SELECT 1 AS n"
    mode: full
    format: parquet
    destination: {type: local, path: ./output}
YAML
}
fixture_pool_detect_teardown() { :; }

# Real GCS via ADC. Requires `gcloud auth application-default login` to have
# been run and the target bucket ($GCS_DEMO_BUCKET, default rivet_data_test)
# to be writable by that identity.
fixture_gcs_setup() {
    : "${GCS_DEMO_BUCKET:=rivet_data_test}"
    : "${GCS_DEMO_PREFIX:=rivet-gif-demo/}"

    local gcloud_bin
    gcloud_bin="$(command -v gcloud || true)"
    if [[ -z "$gcloud_bin" ]]; then
        echo "gcloud not found — install google-cloud-sdk, run 'gcloud auth application-default login'" >&2
        exit 1
    fi
    if [[ ! -f "$HOME/.config/gcloud/application_default_credentials.json" ]]; then
        echo "ADC missing — run 'gcloud auth application-default login' first" >&2
        exit 1
    fi

    local work="/tmp/rivet-gif-doctor-gcs"
    mkdir -p "$work"
    cat >"$work/gcs.yaml" <<YAML
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: users_sample
    query: "SELECT id, email, created_at FROM users LIMIT 100"
    mode: full
    format: parquet
    destination:
      type: gcs
      bucket: ${GCS_DEMO_BUCKET}
      prefix: ${GCS_DEMO_PREFIX}
YAML

    # Expose gcloud binary location to the tape's PATH prelude.
    export GCLOUD_BIN="$gcloud_bin"

    # Wipe any leftovers from a previous render so the final `gcloud storage
    # ls` shows only files written in this take.
    "$gcloud_bin" storage rm --quiet --recursive \
        "gs://${GCS_DEMO_BUCKET}/${GCS_DEMO_PREFIX}" 2>/dev/null || true
}

fixture_gcs_teardown() {
    : "${GCS_DEMO_BUCKET:=rivet_data_test}"
    : "${GCS_DEMO_PREFIX:=rivet-gif-demo/}"
    local gcloud_bin
    gcloud_bin="$(command -v gcloud || true)"
    if [[ -n "$gcloud_bin" ]]; then
        "$gcloud_bin" storage rm --quiet --recursive \
            "gs://${GCS_DEMO_BUCKET}/${GCS_DEMO_PREFIX}" 2>/dev/null || true
    fi
}

fixture_chunked_setup() {
    # 10k-row deterministic fixture in schema rivet_gif.
    psql_ <<'SQL'
DROP SCHEMA IF EXISTS rivet_gif CASCADE;
CREATE SCHEMA rivet_gif;

CREATE TABLE rivet_gif.events (
    id          BIGINT PRIMARY KEY,
    user_id     BIGINT        NOT NULL,
    event_type  TEXT          NOT NULL,
    payload     TEXT          NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

INSERT INTO rivet_gif.events (id, user_id, event_type, payload)
SELECT
    g,
    (g % 500) + 1,
    CASE (g % 3) WHEN 0 THEN 'page_view' WHEN 1 THEN 'click' ELSE 'purchase' END,
    'payload-' || g
FROM generate_series(1, 10000) AS g;

CREATE INDEX ON rivet_gif.events (created_at);
SQL

    # Drop the YAML config into the workdir so the tape can just reference it.
    # (VHS tapes can't cleanly embed a multi-line heredoc via Type.)
    local work="/tmp/rivet-gif-reconcile-repair"
    mkdir -p "$work"
    cat >"$work/chunked.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: events
    query: "SELECT id, user_id, event_type, payload, created_at FROM rivet_gif.events"
    mode: chunked
    chunk_column: id
    chunk_size: 2500
    chunk_checkpoint: true
    parallel: 1
    format: parquet
    destination:
      type: local
      path: ./output
YAML
}

fixture_chunked_teardown() {
    psql_ -c 'DROP SCHEMA IF EXISTS rivet_gif CASCADE;' >/dev/null
}

# ----- Error / recovery scenarios -------------------------------------------
# Each writes a tiny rivet.yaml that triggers one of rivet's actionable failure
# messages. No data seeding: the missing-table probe only needs Postgres to be
# reachable (DATABASE_URL is exported by render.sh), the parse error needs no DB
# at all, and the connection-refused tape overrides DATABASE_URL to a dead port.

fixture_error_missing_table_setup() {
    local work="/tmp/rivet-gif-error-missing-table"
    mkdir -p "$work"
    cat >"$work/rivet.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: orders
    query: "SELECT id, total FROM ordrs"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
YAML
}
fixture_error_missing_table_teardown() { :; }

fixture_error_config_typo_setup() {
    local work="/tmp/rivet-gif-error-config-typo"
    mkdir -p "$work"
    cat >"$work/rivet.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
export:
  - name: orders
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
YAML
}
fixture_error_config_typo_teardown() { :; }

fixture_error_connection_setup() {
    local work="/tmp/rivet-gif-error-connection"
    mkdir -p "$work"
    cat >"$work/rivet.yaml" <<'YAML'
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: orders
    query: "SELECT id FROM orders"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./output
YAML
}
fixture_error_connection_teardown() { :; }

# ----- Scenario runner -------------------------------------------------------

render_scenario() {
    local name="$1"
    local setup="$2"
    local teardown="$3"

    local work="/tmp/rivet-gif-$name"
    rm -rf "$work"
    mkdir -p "$work"

    log "[$name] setup"
    "$setup"

    # Drop the tape file in the workdir so relative Output resolves there.
    cp "$HERE/$name.tape" "$work/"

    # Resolve psql so the reconcile-repair scenario can script the source DB.
    local psql_bin
    psql_bin="$(command -v psql || true)"
    if [[ -z "$psql_bin" && -x /opt/homebrew/opt/libpq/bin/psql ]]; then
        psql_bin=/opt/homebrew/opt/libpq/bin/psql
    fi

    local gcloud_bin
    gcloud_bin="$(command -v gcloud || true)"

    log "[$name] rendering with vhs..."
    (
        cd "$work"
        RIVET_BIN_DIR="$REPO_ROOT/target/release" \
        DATABASE_URL="$DATABASE_URL" \
        PSQL_BIN="$psql_bin" \
        GCLOUD_BIN="$gcloud_bin" \
        vhs "$name.tape"
    )

    if [[ -f "$work/$name.gif" ]]; then
        mv "$work/$name.gif" "$HERE/$name.gif"
        log "[$name] -> $HERE/$name.gif"
    else
        echo "[$name] vhs did not produce $name.gif — inspect $work" >&2
        exit 1
    fi

    log "[$name] teardown"
    "$teardown"
    rm -rf "$work"
}

# ----- Entry point -----------------------------------------------------------

ensure_prereqs

SCENARIOS=("${@:-basic plan-apply reconcile-repair init-scaffold check-verdict inspect chunked-progress parallel-cards incremental-cursor coalesce-cursor discover-artifact plan-campaign error-missing-table error-config-typo error-connection}")
for raw in "${SCENARIOS[@]}"; do
    for name in $raw; do
        case "$name" in
            basic)                 render_scenario basic                 fixture_basic_setup             fixture_basic_teardown ;;
            plan-apply)            render_scenario plan-apply            fixture_basic_setup             fixture_basic_teardown ;;
            reconcile-repair)      render_scenario reconcile-repair      fixture_chunked_setup           fixture_chunked_teardown ;;
            init-scaffold)         render_scenario init-scaffold         fixture_init_setup              fixture_init_teardown ;;
            check-verdict)         render_scenario check-verdict         fixture_check_setup             fixture_check_teardown ;;
            inspect)               render_scenario inspect               fixture_inspect_setup           fixture_inspect_teardown ;;
            chunked-progress)      render_scenario chunked-progress      fixture_chunked_progress_setup  fixture_chunked_progress_teardown ;;
            parallel-cards)        render_scenario parallel-cards        fixture_parallel_cards_setup    fixture_parallel_cards_teardown ;;
            incremental-cursor)    render_scenario incremental-cursor    fixture_incremental_setup       fixture_incremental_teardown ;;
            coalesce-cursor)       render_scenario coalesce-cursor       fixture_coalesce_setup          fixture_coalesce_teardown ;;
            discover-artifact)     render_scenario discover-artifact     fixture_discover_setup          fixture_discover_teardown ;;
            plan-campaign)         render_scenario plan-campaign         fixture_campaign_setup          fixture_campaign_teardown ;;
            pool-detect)           render_scenario pool-detect           fixture_pool_detect_setup       fixture_pool_detect_teardown ;;
            doctor-gcs)            render_scenario doctor-gcs            fixture_gcs_setup               fixture_gcs_teardown ;;
            error-missing-table)   render_scenario error-missing-table   fixture_error_missing_table_setup   fixture_error_missing_table_teardown ;;
            error-config-typo)     render_scenario error-config-typo     fixture_error_config_typo_setup     fixture_error_config_typo_teardown ;;
            error-connection)      render_scenario error-connection      fixture_error_connection_setup      fixture_error_connection_teardown ;;
            *) echo "unknown scenario: $name" >&2; exit 1 ;;
        esac
    done
done

log "done"
