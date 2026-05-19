#!/usr/bin/env bash
# Set up the cross-tool benchmark workspace.
#
# Creates a Python venv with pyarrow + pymysql + psycopg2-binary + dlt,
# installs ClickHouse (direct binary, ~155 MB) into the workspace, and
# verifies that the other tools (rivet, sling, duckdb, odbc2parquet) are
# already on `$PATH`. Everything lives under $BENCH_ROOT (default /tmp/rivet_bench)
# so it stays out of the repo and is easy to throw away.
#
# Usage: bash docs/bench/harness/setup.sh

set -euo pipefail

ROOT="${BENCH_ROOT:-/tmp/rivet_bench}"
mkdir -p "$ROOT/bin" "$ROOT/output" "$ROOT/results" "$ROOT/db_signals" "$ROOT/logs"

echo "==> workspace = $ROOT"

# ── Python venv ────────────────────────────────────────────────────────────
if [ ! -x "$ROOT/.venv/bin/python" ]; then
  echo "==> creating Python venv"
  python3 -m venv "$ROOT/.venv"
fi
"$ROOT/.venv/bin/pip" install --quiet --disable-pip-version-check \
  "dlt[parquet,sql_database]" psycopg2-binary pyarrow numpy pymysql sqlalchemy cryptography pyyaml

# ── ClickHouse local ───────────────────────────────────────────────────────
if [ ! -x "$ROOT/bin/clickhouse" ]; then
  echo "==> downloading clickhouse-local (~155 MB)"
  curl -fsSL -o "$ROOT/bin/clickhouse" \
    "https://builds.clickhouse.com/master/macos-aarch64/clickhouse"
  chmod +x "$ROOT/bin/clickhouse"
fi

# ── Required-on-PATH tools — verify only, never install ─────────────────────
echo "==> tool versions"
require() {
  if command -v "$1" >/dev/null 2>&1; then
    echo "  $1: $($1 --version 2>&1 | head -1)"
  else
    echo "  $1: MISSING (install separately and re-run)"
  fi
}
require rivet
require sling
require duckdb
require odbc2parquet
require gtime
require psql
require mysql

# ── ODBC driver for Postgres (odbc2parquet on PG) ──────────────────────────
if ! odbcinst -q -d 2>/dev/null | grep -qE 'postgre_(ansi|unicode)'; then
  echo "==> psqlodbc not registered in odbcinst.ini — odbc2parquet on PG will be skipped"
fi

echo "==> setup OK; export BENCH_ROOT=$ROOT before running benches"
