#!/usr/bin/env bash
# Top-level orchestrator: discover tables, run perf + DB-signal benches on
# Postgres and MySQL, write reports.
#
# Usage:
#   docs/bench/harness/run_all.sh pg            # PG only
#   docs/bench/harness/run_all.sh mysql         # MySQL only
#   docs/bench/harness/run_all.sh both          # both (default)
#   docs/bench/harness/run_all.sh both signals  # also re-run DB-signal capture
#
# Output: $BENCH_ROOT (default /tmp/rivet_bench) for the JSON & temp parquet,
# docs/bench/reports/ for the rendered Markdown.

set -euo pipefail

WHICH="${1:-both}"
SIGNALS="${2:-perf}"   # `signals` to also run the 50 ms sampler harness
ROOT="${BENCH_ROOT:-/tmp/rivet_bench}"
HARNESS=$(cd "$(dirname "$0")" && pwd)
export BENCH_ROOT="$ROOT"
export BENCH_YAML_PG="$HARNESS/../configs/rivet_pg.yaml"
export BENCH_YAML_MYSQL="$HARNESS/../configs/rivet_mysql.yaml"
mkdir -p "$ROOT"

# ── Postgres ────────────────────────────────────────────────────────────────
run_pg() {
  echo "==> PG: discovering tables"
  PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -tAc "
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
    ORDER BY table_name;" > "$ROOT/_pg_tables.txt"
  : > "$ROOT/tables_pg.txt"
  while read tbl; do
    [ -z "$tbl" ] && continue
    cnt=$(PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -tAc "SELECT count(*) FROM public.\"$tbl\";")
    echo "$tbl|$cnt" >> "$ROOT/tables_pg.txt"
  done < "$ROOT/_pg_tables.txt"
  rm "$ROOT/_pg_tables.txt"
  wc -l "$ROOT/tables_pg.txt"

  TOOLS="rivet sling dlt duckdb clickhouse"
  if command -v odbc2parquet >/dev/null 2>&1 && \
     odbcinst -q -d 2>/dev/null | grep -qE 'postgre_(ansi|unicode)'; then
    TOOLS="$TOOLS odbc2parquet"
  fi

  echo "==> PG: running perf bench (5–6 tools × $(wc -l < $ROOT/tables_pg.txt) tables)"
  while IFS='|' read tbl _; do
    for t in $TOOLS; do
      "$HARNESS/bench.sh" "$t" "$tbl"
    done
  done < "$ROOT/tables_pg.txt"

  if [ "$SIGNALS" = "signals" ]; then
    echo "==> PG: running DB-signal bench on 3 representative tables"
    for tbl in bench_narrow page_views content_items; do
      for t in $TOOLS; do
        "$HARNESS/db_bench.sh" "$t" "$tbl"
      done
    done
  fi
}

# ── MySQL ──────────────────────────────────────────────────────────────────
run_mysql() {
  echo "==> MySQL: discovering tables"
  : > "$ROOT/tables_mysql.txt"
  for tbl in $(mysql -h 127.0.0.1 -u rivet -privet rivet -BNe "SHOW TABLES;" 2>/dev/null); do
    cnt=$(mysql -h 127.0.0.1 -u rivet -privet rivet -BNe "SELECT COUNT(*) FROM \`$tbl\`;" 2>/dev/null)
    echo "$tbl|$cnt" >> "$ROOT/tables_mysql.txt"
  done
  wc -l "$ROOT/tables_mysql.txt"

  TOOLS="rivet sling dlt duckdb clickhouse"
  echo "==> MySQL: ensuring perf_schema GRANTs on the bench user (skip if you already have them)"
  mysql -h 127.0.0.1 -u root -privet -e "
    GRANT SELECT ON performance_schema.* TO 'rivet'@'%';
    GRANT PROCESS ON *.* TO 'rivet'@'%';
    FLUSH PRIVILEGES;" 2>/dev/null || echo "  (skipped — root creds not available; bench will still work, db_bench_mysql may show fewer signals)"

  echo "==> MySQL: running perf bench (5 tools × $(wc -l < $ROOT/tables_mysql.txt) tables)"
  while IFS='|' read tbl _; do
    for t in $TOOLS; do
      "$HARNESS/bench_mysql.sh" "$t" "$tbl"
    done
  done < "$ROOT/tables_mysql.txt"

  if [ "$SIGNALS" = "signals" ]; then
    echo "==> MySQL: running DB-signal bench on 3 representative tables"
    for tbl in audit_log page_views content_items; do
      for t in $TOOLS; do
        "$HARNESS/db_bench_mysql.sh" "$t" "$tbl"
      done
    done
  fi
}

case "$WHICH" in
  pg)    run_pg ;;
  mysql) run_mysql ;;
  both)  run_pg; run_mysql ;;
  *)     echo "usage: $0 [pg|mysql|both] [perf|signals]" >&2; exit 1 ;;
esac

echo "==> rendering reports"
REPO_ROOT=$(cd "$HARNESS/../../.." && pwd)
"$ROOT/.venv/bin/python" "$HARNESS/aggregate.py" "$REPO_ROOT/docs/bench/reports"

echo "==> done"
echo "  results:    $ROOT/results, $ROOT/results_mysql"
echo "  db_signals: $ROOT/db_signals, $ROOT/db_signals_mysql"
echo "  reports:    $REPO_ROOT/docs/bench/reports/"
