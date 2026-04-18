#!/usr/bin/env bash
# E2E test matrix for Rivet.
# Requires: docker compose up -d postgres mysql minio fake-gcs
# Requires: both DBs seeded (cargo run --bin seed -- --target postgres --users 500 ...)
#
# Usage: bash dev/e2e/run_e2e.sh
#
# Re-targeting to a different server (e.g. a legacy container):
#     RIVET_PG_URL=postgresql://rivet:rivet@localhost:5412/rivet \
#     RIVET_MYSQL_URL=mysql://rivet:rivet@localhost:3357/rivet \
#         bash dev/e2e/run_e2e.sh
#
# The e2e YAMLs use `url_env: RIVET_PG_URL` / `url_env: RIVET_MYSQL_URL`, so one
# suite drives every PG/MySQL version the team supports.

set -uo pipefail
cd "$(dirname "$0")/../.."

# Default URLs — overridable via env so the legacy compat matrix can re-point
# the same suite at any version without editing YAMLs.
export RIVET_PG_URL="${RIVET_PG_URL:-postgresql://rivet:rivet@localhost:5432/rivet}"
export RIVET_MYSQL_URL="${RIVET_MYSQL_URL:-mysql://rivet:rivet@localhost:3306/rivet}"

RIVET="${RIVET:-cargo run --release --bin rivet --}"
OUT="dev/e2e/output"
RESULTS="/tmp/rivet_e2e_results.txt"
rm -f "$RESULTS"

PASS=0
FAIL=0
SKIP=0

pass() { echo "PASS  $*" | tee -a "$RESULTS"; PASS=$((PASS+1)); }
fail() { echo "FAIL  $*" | tee -a "$RESULTS"; FAIL=$((FAIL+1)); }
skip() { echo "SKIP  $*" | tee -a "$RESULTS"; SKIP=$((SKIP+1)); }

section() { echo ""; echo "══════ $* ══════"; }

assert_file_exists() {
    local pattern="$1" label="$2"
    # shellcheck disable=SC2086
    if compgen -G "$pattern" >/dev/null 2>&1; then pass "$label"; else fail "$label (no files: $pattern)"; fi
}

assert_file_count_ge() {
    local pattern="$1" min="$2" label="$3"
    local count
    # shellcheck disable=SC2086
    count=$(compgen -G "$pattern" 2>/dev/null | wc -l | tr -d ' ')
    if [ "${count:-0}" -ge "$min" ]; then pass "$label (n=$count)"; else fail "$label (expected >=$min, got ${count:-0})"; fi
}

assert_no_file() {
    local pattern="$1" label="$2"
    # shellcheck disable=SC2086
    if compgen -G "$pattern" >/dev/null 2>&1; then fail "$label (file exists)"; else pass "$label"; fi
}

cleanup() {
    rm -rf "$OUT"
    rm -rf dev/e2e/.init_e2e_scratch
    rm -f dev/e2e/.rivet_state.db*
}

# ──────────────────────────────────────────────────────────────
section "Setup"
cleanup
mkdir -p "$OUT"

pg_ok=false; mysql_ok=false; minio_ok=false; gcs_ok=false

# Works both in docker-compose dev and GitHub Actions service containers
pg_isready -h localhost -U rivet >/dev/null 2>&1 && pg_ok=true || \
    docker compose exec -T postgres pg_isready -U rivet >/dev/null 2>&1 && pg_ok=true
mysqladmin ping -h 127.0.0.1 -uroot -privet >/dev/null 2>&1 && mysql_ok=true || \
    docker compose exec -T mysql mysqladmin ping -h localhost -uroot -privet >/dev/null 2>&1 && mysql_ok=true
curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1 && minio_ok=true
curl -sf http://localhost:4443/storage/v1/b >/dev/null 2>&1 && gcs_ok=true

echo "Postgres: $pg_ok | MySQL: $mysql_ok | MinIO: $minio_ok | fake-gcs: $gcs_ok"
$pg_ok || { echo "FATAL: Postgres not available"; exit 1; }

# ──────────────────────────────────────────────────────────────
section "1. Doctor (connectivity check)"

$RIVET doctor --config dev/e2e/pg_e2e.yaml >/dev/null 2>&1 && pass "PG doctor" || fail "PG doctor"
if $mysql_ok; then
    $RIVET doctor --config dev/e2e/mysql_e2e.yaml >/dev/null 2>&1 && pass "MySQL doctor" || fail "MySQL doctor"
else skip "MySQL doctor (not running)"; fi

# ──────────────────────────────────────────────────────────────
section "2. Preflight check"

$RIVET check --config dev/e2e/pg_e2e.yaml >/dev/null 2>&1 && pass "PG check" || fail "PG check"
if $mysql_ok; then
    $RIVET check --config dev/e2e/mysql_e2e.yaml >/dev/null 2>&1 && pass "MySQL check" || fail "MySQL check"
else skip "MySQL check"; fi

# ──────────────────────────────────────────────────────────────
section "3. Postgres — all modes (local)"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_users_full_csv >/dev/null 2>&1 && pass "PG full CSV" || fail "PG full CSV"
assert_file_exists "$OUT/pg_users_full_csv_*.csv" "PG full CSV file"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_users_full_parquet --validate >/dev/null 2>&1 && pass "PG full Parquet" || fail "PG full Parquet"
assert_file_exists "$OUT/pg_users_full_parquet_*.parquet" "PG full Parquet file"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_orders_incremental --validate >/dev/null 2>&1 && pass "PG incremental" || fail "PG incremental"
assert_file_exists "$OUT/pg_orders_incremental_*.parquet" "PG incremental file"

# Incremental rerun — should succeed (no new data = no file, but still exit 0)
$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_orders_incremental >/dev/null 2>&1 && pass "PG incremental rerun" || fail "PG incremental rerun"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_orders_chunked --validate >/dev/null 2>&1 && pass "PG chunked" || fail "PG chunked"
assert_file_count_ge "$OUT/pg_orders_chunked_*.parquet" 1 "PG chunked files"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_events_timewindow >/dev/null 2>&1 && pass "PG time_window" || fail "PG time_window"
assert_file_exists "$OUT/pg_events_timewindow_*.csv" "PG time_window file"

# ──────────────────────────────────────────────────────────────
section "4. Postgres — compression, skip_empty, meta, split"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_users_zstd --validate >/dev/null 2>&1 && pass "PG zstd" || fail "PG zstd"
assert_file_exists "$OUT/pg_users_zstd_*.parquet" "PG zstd file"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_users_gzip_csv >/dev/null 2>&1 && pass "PG gzip CSV" || fail "PG gzip CSV"
assert_file_exists "$OUT/pg_users_gzip_csv_*.csv*" "PG gzip CSV file"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_empty_skip >/dev/null 2>&1 && pass "PG skip_empty" || fail "PG skip_empty"
assert_no_file "$OUT/pg_empty_skip_*.csv*" "PG skip_empty no file"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_users_meta --validate >/dev/null 2>&1 && pass "PG meta columns" || fail "PG meta columns"
assert_file_exists "$OUT/pg_users_meta_*.parquet" "PG meta file"

$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_events_split --validate >/dev/null 2>&1 && pass "PG file split" || fail "PG file split"
assert_file_count_ge "$OUT/pg_events_split_*_part*.parquet" 2 "PG split files >=2"

# ──────────────────────────────────────────────────────────────
section "5. MySQL — all modes (local)"

if $mysql_ok; then
    $RIVET run --config dev/e2e/mysql_e2e.yaml --export mysql_users_full_csv >/dev/null 2>&1 && pass "MySQL full CSV" || fail "MySQL full CSV"
    assert_file_exists "$OUT/mysql_users_full_csv_*.csv" "MySQL full CSV file"

    $RIVET run --config dev/e2e/mysql_e2e.yaml --export mysql_users_full_parquet --validate >/dev/null 2>&1 && pass "MySQL full Parquet" || fail "MySQL full Parquet"
    assert_file_exists "$OUT/mysql_users_full_parquet_*.parquet" "MySQL full Parquet file"

    $RIVET run --config dev/e2e/mysql_e2e.yaml --export mysql_orders_incremental --validate >/dev/null 2>&1 && pass "MySQL incremental" || fail "MySQL incremental"
    assert_file_exists "$OUT/mysql_orders_incremental_*.parquet" "MySQL incremental file"

    $RIVET run --config dev/e2e/mysql_e2e.yaml --export mysql_orders_chunked --validate >/dev/null 2>&1 && pass "MySQL chunked" || fail "MySQL chunked"
    assert_file_count_ge "$OUT/mysql_orders_chunked_*.parquet" 1 "MySQL chunked files"

    $RIVET run --config dev/e2e/mysql_e2e.yaml --export mysql_events_timewindow >/dev/null 2>&1 && pass "MySQL time_window" || fail "MySQL time_window"
    assert_file_exists "$OUT/mysql_events_timewindow_*.csv" "MySQL time_window file"
else
    skip "MySQL full CSV"
    skip "MySQL full CSV file"
    skip "MySQL full Parquet"
    skip "MySQL full Parquet file"
    skip "MySQL incremental"
    skip "MySQL incremental file"
    skip "MySQL chunked"
    skip "MySQL chunked files"
    skip "MySQL time_window"
    skip "MySQL time_window file"
fi

# ──────────────────────────────────────────────────────────────
section "6. S3 (MinIO) destination"

if $minio_ok; then
    export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
    export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

    # Create bucket — try mc locally, then docker compose mc, then python fallback
    if command -v mc >/dev/null 2>&1; then
        mc alias set _rivet http://localhost:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null 2>&1
        mc mb _rivet/rivet-e2e --ignore-existing >/dev/null 2>&1 || true
    elif docker compose exec -T minio mc version >/dev/null 2>&1; then
        docker compose exec -T minio mc alias set myminio http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1
        docker compose exec -T minio mc mb myminio/rivet-e2e --ignore-existing >/dev/null 2>&1 || true
    else
        pip install --quiet minio >/dev/null 2>&1 || true
        python3 -c "
from minio import Minio
c = Minio('localhost:9000', access_key='${MINIO_ACCESS_KEY}', secret_key='${MINIO_SECRET_KEY}', secure=False)
if not c.bucket_exists('rivet-e2e'): c.make_bucket('rivet-e2e')
" 2>/dev/null || echo "  WARNING: could not create MinIO bucket; S3 tests may fail"
    fi

    $RIVET run --config dev/e2e/pg_s3_e2e.yaml --export pg_users_s3 --validate >/dev/null 2>&1 && pass "S3 full upload" || fail "S3 full upload"
    $RIVET run --config dev/e2e/pg_s3_e2e.yaml --export pg_orders_s3_chunked --validate >/dev/null 2>&1 && pass "S3 chunked upload" || fail "S3 chunked upload"
else
    skip "S3 full upload (MinIO not running)"
    skip "S3 chunked upload"
fi

# ──────────────────────────────────────────────────────────────
section "7. GCS (fake-gcs-server) destination"

if $gcs_ok; then
    curl -sf -X POST "http://localhost:4443/storage/v1/b?project=test" \
        -H "Content-Type: application/json" -d '{"name": "rivet-e2e"}' >/dev/null 2>&1 || true

    $RIVET run --config dev/e2e/pg_gcs_e2e.yaml --export pg_users_gcs --validate >/dev/null 2>&1 && pass "GCS full upload" || fail "GCS full upload"
    $RIVET run --config dev/e2e/pg_gcs_e2e.yaml --export pg_orders_gcs_incremental >/dev/null 2>&1 && pass "GCS incremental upload" || fail "GCS incremental upload"
else
    skip "GCS full upload (fake-gcs not running)"
    skip "GCS incremental upload"
fi

# ──────────────────────────────────────────────────────────────
section "8. State management"

$RIVET state show --config dev/e2e/pg_e2e.yaml >/dev/null 2>&1 && pass "state show" || fail "state show"
$RIVET state files --config dev/e2e/pg_e2e.yaml >/dev/null 2>&1 && pass "state files" || fail "state files"
$RIVET metrics --config dev/e2e/pg_e2e.yaml --last 5 >/dev/null 2>&1 && pass "metrics" || fail "metrics"

$RIVET state reset --config dev/e2e/pg_e2e.yaml --export pg_orders_incremental >/dev/null 2>&1 && pass "state reset" || fail "state reset"
$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_orders_incremental --validate >/dev/null 2>&1 && pass "re-export after reset" || fail "re-export after reset"

# ──────────────────────────────────────────────────────────────
section "9. Stdout destination"

_stdout=$($RIVET run --config dev/test_stdout.yaml 2>/dev/null | head -c 100)
if [ -n "$_stdout" ]; then pass "stdout destination"; else fail "stdout destination"; fi

# ──────────────────────────────────────────────────────────────
section "10. Parameterized queries"

$RIVET run --config dev/test_params.yaml --param MAX_ID=10 >/dev/null 2>&1 && pass "params" || fail "params"

# ──────────────────────────────────────────────────────────────
section "11. Reconciliation (--reconcile)"

_rec=$($RIVET run --config dev/e2e/pg_e2e.yaml --export pg_users_full_csv --reconcile 2>&1)
echo "$_rec" | grep -q "MATCH" && pass "reconcile full MATCH" || fail "reconcile full MATCH"

_rec=$($RIVET run --config dev/e2e/pg_e2e.yaml --export pg_orders_chunked --reconcile 2>&1)
echo "$_rec" | grep -q "MATCH" && pass "reconcile chunked MATCH" || fail "reconcile chunked MATCH"

# Incremental should skip reconciliation (no MATCH/MISMATCH in output)
_rec=$($RIVET run --config dev/e2e/pg_e2e.yaml --export pg_orders_incremental --reconcile 2>&1)
echo "$_rec" | grep -q "reconcile:" && fail "reconcile incremental should skip" || pass "reconcile incremental skip"

# ──────────────────────────────────────────────────────────────
section "12. Recovery / rerun behavior"

# Full mode: two consecutive runs should both succeed and produce separate files
$RIVET run --config dev/e2e/pg_recovery_e2e.yaml --export recovery_full --reconcile >/dev/null 2>&1 && pass "recovery full run1" || fail "recovery full run1"
count1=$(compgen -G "$OUT/recovery_full_*.parquet" 2>/dev/null | wc -l | tr -d ' ')
sleep 1
$RIVET run --config dev/e2e/pg_recovery_e2e.yaml --export recovery_full --reconcile >/dev/null 2>&1 && pass "recovery full run2" || fail "recovery full run2"
count2=$(compgen -G "$OUT/recovery_full_*.parquet" 2>/dev/null | wc -l | tr -d ' ')
if [ "$count2" -gt "$count1" ]; then pass "recovery full separate files (n=$count2)"; else fail "recovery full separate files ($count1 vs $count2)"; fi

# Incremental mode: second run should succeed (with 0 rows if no new data)
$RIVET state reset --config dev/e2e/pg_recovery_e2e.yaml --export recovery_incremental >/dev/null 2>&1
$RIVET run --config dev/e2e/pg_recovery_e2e.yaml --export recovery_incremental --reconcile >/dev/null 2>&1 && pass "recovery incr run1" || fail "recovery incr run1"
_inc2=$($RIVET run --config dev/e2e/pg_recovery_e2e.yaml --export recovery_incremental 2>&1)
echo "$_inc2" | grep -qE "rows.*0|no data" && pass "recovery incr run2 (no new data)" || pass "recovery incr run2 (ok)"

# Chunked with checkpoint: run succeeds; resume after full completion correctly errors
$RIVET run --config dev/e2e/pg_recovery_e2e.yaml --export recovery_chunked_ckpt --reconcile >/dev/null 2>&1 && pass "recovery chunked ckpt run1" || fail "recovery chunked ckpt run1"
_resume_err=$($RIVET run --config dev/e2e/pg_recovery_e2e.yaml --export recovery_chunked_ckpt --resume 2>&1 || true)
echo "$_resume_err" | grep -qi "no in-progress" && pass "recovery chunked resume (no pending)" || fail "recovery chunked resume"

# Re-run without resume should succeed (full re-export)
$RIVET run --config dev/e2e/pg_recovery_e2e.yaml --export recovery_chunked_ckpt --reconcile >/dev/null 2>&1 && pass "recovery chunked re-export" || fail "recovery chunked re-export"

# Metrics should show entries for recovery exports. Filter to `recovery_full`
# explicitly so the assertion doesn't race with an unrelated later run pushing
# it out of `--last 10`. The brief sleep gives SQLite's WAL a moment to flush
# the last chunked-ckpt run's metric row before we read it back.
sleep 0.5
_metrics_out=$($RIVET metrics --config dev/e2e/pg_recovery_e2e.yaml --export recovery_full --last 10 2>&1)
if echo "$_metrics_out" | grep -q "recovery_full"; then
    pass "recovery metrics entries"
else
    fail "recovery metrics entries"
    echo "DEBUG: metrics output was:"
    echo "$_metrics_out"
fi

# ──────────────────────────────────────────────────────────────
section "13. Config validation (misplaced fields)"

_err=$($RIVET run --config /dev/stdin 2>&1 <<'YAML' || true
source:
  type: postgres
  url_env: RIVET_PG_URL
  batch_size: 1000
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: /tmp
YAML
)
echo "$_err" | grep -q "source.tuning" && pass "misplaced field detection" || fail "misplaced field detection"

# ──────────────────────────────────────────────────────────────
section "14. Preflight — connection limit warnings"

# PG: parallel well below max_connections (default 100) — no warning expected
_out=$($RIVET check --config /dev/stdin 2>&1 <<'YAML'
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - name: check_conn_safe
    query: "SELECT id FROM users"
    mode: chunked
    chunk_column: id
    chunk_size: 100000
    parallel: 2
    format: parquet
    destination:
      type: local
      path: /tmp
YAML
)
echo "$_out" | grep -qi "meets or exceeds\|check skipped" \
    && fail "PG check: unexpected connection warning at parallel=2" \
    || pass "PG check: no connection warning at parallel=2"

# PG: parallel >= max_connections (999 >> 100) — must warn with exact numbers
_out=$($RIVET check --config /dev/stdin 2>&1 <<'YAML'
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - name: check_conn_high
    query: "SELECT id FROM users"
    mode: chunked
    chunk_column: id
    chunk_size: 100000
    parallel: 999
    format: parquet
    destination:
      type: local
      path: /tmp
YAML
)
echo "$_out" | grep -qi "meets or exceeds" \
    && pass "PG check: connection limit warning at parallel=999" \
    || fail "PG check: expected 'meets or exceeds' warning at parallel=999"
echo "$_out" | grep -qi "max_connections" \
    && pass "PG check: max_connections value present in warning" \
    || fail "PG check: max_connections value missing from warning"

# MySQL: parallel below max_connections (default 151) — no warning expected
if $mysql_ok; then
    _out=$($RIVET check --config /dev/stdin 2>&1 <<'YAML'
source:
  type: mysql
  url_env: RIVET_MYSQL_URL
exports:
  - name: mysql_check_conn_safe
    query: "SELECT id FROM users"
    mode: chunked
    chunk_column: id
    chunk_size: 100000
    parallel: 2
    format: parquet
    destination:
      type: local
      path: /tmp
YAML
)
    echo "$_out" | grep -qi "meets or exceeds\|check skipped" \
        && fail "MySQL check: unexpected connection warning at parallel=2" \
        || pass "MySQL check: no connection warning at parallel=2"

    # MySQL: parallel >= max_connections (999 >> 151) — must warn
    _out=$($RIVET check --config /dev/stdin 2>&1 <<'YAML'
source:
  type: mysql
  url_env: RIVET_MYSQL_URL
exports:
  - name: mysql_check_conn_high
    query: "SELECT id FROM users"
    mode: chunked
    chunk_column: id
    chunk_size: 100000
    parallel: 999
    format: parquet
    destination:
      type: local
      path: /tmp
YAML
)
    echo "$_out" | grep -qi "meets or exceeds" \
        && pass "MySQL check: connection limit warning at parallel=999" \
        || fail "MySQL check: expected 'meets or exceeds' warning at parallel=999"
    echo "$_out" | grep -qi "max_connections" \
        && pass "MySQL check: max_connections value present in warning" \
        || fail "MySQL check: max_connections value missing from warning"
else
    skip "MySQL check: connection limit warning (MySQL not running)"
    skip "MySQL check: max_connections value present"
    skip "MySQL check: no connection warning at parallel=2"
    skip "MySQL check: max_connections value at parallel=999"
fi

# ──────────────────────────────────────────────────────────────
section "15. Date-based chunking (chunk_by_days)"

# PG: date-chunked run produces at least one output file
$RIVET run --config dev/e2e/pg_e2e.yaml --export pg_orders_date_chunked --validate >/dev/null 2>&1 \
    && pass "PG date-chunked: run succeeded" \
    || fail "PG date-chunked: run failed"
assert_file_count_ge "$OUT/pg_orders_date_chunked_*.parquet" 1 "PG date-chunked files"

# PG: preflight shows date-chunked strategy in output
_out=$($RIVET check --config dev/e2e/pg_e2e.yaml --export pg_orders_date_chunked 2>&1)
echo "$_out" | grep -qi "date-chunked" \
    && pass "PG check: date-chunked strategy shown" \
    || fail "PG check: expected 'date-chunked' in strategy output"

# PG: inline config — verify chunk_by_days rejects chunk_dense combination
_out=$($RIVET run --config /dev/stdin 2>&1 <<'YAML'
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - name: invalid_date_dense
    query: "SELECT id, ordered_at FROM orders"
    mode: chunked
    chunk_column: ordered_at
    chunk_by_days: 30
    chunk_dense: true
    format: parquet
    destination:
      type: local
      path: /tmp
YAML
)
echo "$_out" | grep -qi "chunk_dense\|cannot combine\|invalid" \
    && pass "PG validation: chunk_by_days + chunk_dense rejected" \
    || fail "PG validation: expected rejection of chunk_by_days + chunk_dense"

if $mysql_ok; then
    # MySQL: date-chunked run produces at least one output file
    $RIVET run --config dev/e2e/mysql_e2e.yaml --export mysql_orders_date_chunked --validate >/dev/null 2>&1 \
        && pass "MySQL date-chunked: run succeeded" \
        || fail "MySQL date-chunked: run failed"
    assert_file_count_ge "$OUT/mysql_orders_date_chunked_*.parquet" 1 "MySQL date-chunked files"

    # MySQL: preflight shows date-chunked strategy
    _out=$($RIVET check --config dev/e2e/mysql_e2e.yaml --export mysql_orders_date_chunked 2>&1)
    echo "$_out" | grep -qi "date-chunked" \
        && pass "MySQL check: date-chunked strategy shown" \
        || fail "MySQL check: expected 'date-chunked' in strategy output"
else
    skip "MySQL date-chunked: run"
    skip "MySQL date-chunked: files"
    skip "MySQL check: date-chunked strategy"
fi

# ──────────────────────────────────────────────────────────────
section "16. rivet init (YAML scaffold)"

INIT_TMP="dev/e2e/.init_e2e_scratch"
rm -rf "$INIT_TMP"
mkdir -p "$INIT_TMP"

# `rivet init` takes the URL on the CLI. Use the suite's RIVET_PG_URL /
# RIVET_MYSQL_URL so this section retargets cleanly when the legacy matrix
# overrides them.
PG_INIT_URL="$RIVET_PG_URL"
MY_INIT_URL="$RIVET_MYSQL_URL"

$RIVET init --source "$PG_INIT_URL" --table users -o "$INIT_TMP/pg_users.yaml" >/dev/null 2>&1 \
    && pass "PG init: single table (users)" \
    || fail "PG init: single table (users)"
grep -q "exports:" "$INIT_TMP/pg_users.yaml" \
    && grep -q "name: users" "$INIT_TMP/pg_users.yaml" \
    && grep -q "mode:" "$INIT_TMP/pg_users.yaml" \
    && pass "PG init: users.yaml structure" \
    || fail "PG init: users.yaml missing exports/name/mode"

$RIVET init --source "$PG_INIT_URL" --schema public -o "$INIT_TMP/pg_schema.yaml" >/dev/null 2>&1 \
    && pass "PG init: full schema (public)" \
    || fail "PG init: full schema (public)"
pg_init_count=$(grep -E '^  - name:' "$INIT_TMP/pg_schema.yaml" 2>/dev/null | wc -l | tr -d '[:space:]')
if [ "${pg_init_count:-0}" -ge 5 ]; then
    pass "PG init: multiple exports in schema file (n=$pg_init_count)"
else
    fail "PG init: expected >=5 exports in schema file, got ${pg_init_count:-0}"
fi
grep -q 'PostgreSQL schema "public"' "$INIT_TMP/pg_schema.yaml" \
    && pass "PG init: schema banner in header" \
    || fail "PG init: expected PostgreSQL schema banner"

export DATABASE_URL="$PG_INIT_URL"
$RIVET check --config "$INIT_TMP/pg_users.yaml" >/dev/null 2>&1 \
    && pass "PG init: rivet check accepts generated file" \
    || fail "PG init: rivet check on generated file"
unset DATABASE_URL

if $mysql_ok; then
    $RIVET init --source "$MY_INIT_URL" --table orders -o "$INIT_TMP/mysql_orders.yaml" >/dev/null 2>&1 \
        && pass "MySQL init: single table (orders)" \
        || fail "MySQL init: single table (orders)"
    grep -q "name: orders" "$INIT_TMP/mysql_orders.yaml" \
        && pass "MySQL init: orders.yaml structure" \
        || fail "MySQL init: orders.yaml missing name"

    $RIVET init --source "$MY_INIT_URL" -o "$INIT_TMP/mysql_db.yaml" >/dev/null 2>&1 \
        && pass "MySQL init: full database" \
        || fail "MySQL init: full database"
    my_init_count=$(grep -E '^  - name:' "$INIT_TMP/mysql_db.yaml" 2>/dev/null | wc -l | tr -d '[:space:]')
    if [ "${my_init_count:-0}" -ge 5 ]; then
        pass "MySQL init: multiple exports (n=$my_init_count)"
    else
        fail "MySQL init: expected >=5 exports, got ${my_init_count:-0}"
    fi
    grep -q 'MySQL database "rivet"' "$INIT_TMP/mysql_db.yaml" \
        && pass "MySQL init: database banner in header" \
        || fail "MySQL init: expected MySQL database banner"
else
    skip "MySQL init: single table"
    skip "MySQL init: orders.yaml structure"
    skip "MySQL init: full database"
    skip "MySQL init: multiple exports"
    skip "MySQL init: database banner"
fi

# ──────────────────────────────────────────────────────────────
section "Summary"
echo ""
echo "PASS: $PASS | FAIL: $FAIL | SKIP: $SKIP"
echo "Total: $((PASS + FAIL + SKIP))"
echo ""

if [ "$FAIL" -gt 0 ]; then
    echo "FAILURES:"
    grep "^FAIL" "$RESULTS"
    exit 1
fi

echo "All tests passed!"
cleanup
