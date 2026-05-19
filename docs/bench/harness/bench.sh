#!/usr/bin/env bash
# Run one (tool, table) Postgres benchmark, capture wall/RSS/output into JSON.
#
# Usage: bench.sh <tool> <table>
# Tools: rivet | sling | dlt | duckdb | clickhouse | odbc2parquet
# Workspace: $BENCH_ROOT (default /tmp/rivet_bench)

set -uo pipefail
TOOL=$1
TBL=$2
ROOT="${BENCH_ROOT:-/tmp/rivet_bench}"
PG_URL='postgresql://rivet:rivet@localhost:5432/rivet'
RESULT="$ROOT/results/${TOOL}_${TBL}.json"
LOG="$ROOT/logs/${TOOL}_${TBL}.log"
mkdir -p "$ROOT/results" "$ROOT/logs"

case "$TOOL" in
  rivet)        OUT_DIR="$ROOT/output/rivet/${TBL}";;
  sling)        OUT_DIR="$ROOT/output/sling/public_${TBL}";;
  dlt)          OUT_DIR="$ROOT/output/dlt/${TBL}";;
  duckdb)       OUT_DIR="$ROOT/output/duckdb/${TBL}";;
  clickhouse)   OUT_DIR="$ROOT/output/clickhouse/${TBL}";;
  odbc2parquet) OUT_DIR="$ROOT/output/odbc2parquet/${TBL}";;
esac
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

PG_CONN_BEFORE=$(PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -tAc \
  "SELECT count(*) FROM pg_stat_activity WHERE datname='rivet' AND state IS NOT NULL")

GTIME_OUT="$ROOT/logs/${TOOL}_${TBL}.gtime"

case "$TOOL" in
  rivet)
    SMOKE="$ROOT/_run_rivet_${TBL}.yaml"
    "$ROOT/.venv/bin/python" - "$TBL" >"$SMOKE" <<'PY'
import sys, os, yaml
tbl = sys.argv[1]
cfg = yaml.safe_load(open(os.environ.get('BENCH_YAML_PG',
    os.path.expanduser('~/rivet/docs/bench/configs/rivet_pg.yaml'))))
cfg['exports'] = [e for e in cfg['exports'] if e['name'] == tbl]
print(yaml.safe_dump(cfg))
PY
    gtime -v -o "$GTIME_OUT" "$(command -v rivet)" run -c "$SMOKE" >"$LOG" 2>&1
    RC=$?
    ;;

  sling)
    export PG_LOCAL='postgres://rivet:rivet@localhost:5432/rivet?sslmode=disable'
    gtime -v -o "$GTIME_OUT" sling run \
      --src-conn PG_LOCAL --src-stream "public.${TBL}" \
      --tgt-object "file://${OUT_DIR}/data.parquet" \
      --tgt-options '{format: parquet, compression: snappy}' \
      --mode full-refresh >"$LOG" 2>&1
    RC=$?
    ;;

  dlt)
    gtime -v -o "$GTIME_OUT" "$ROOT/.venv/bin/python" \
      "$(dirname "$0")/dlt_pg_pipeline.py" "$TBL" "$ROOT/output/dlt" "$PG_URL" >"$LOG" 2>&1
    RC=$?
    ;;

  duckdb)
    gtime -v -o "$GTIME_OUT" duckdb -c "
INSTALL postgres; LOAD postgres;
ATTACH 'host=localhost port=5432 user=rivet password=rivet dbname=rivet' AS pg (TYPE postgres, READ_ONLY);
COPY (SELECT * FROM pg.public.\"${TBL}\") TO '${OUT_DIR}/data.parquet' (FORMAT parquet, COMPRESSION snappy);
" >"$LOG" 2>&1
    RC=$?
    ;;

  clickhouse)
    gtime -v -o "$GTIME_OUT" "$ROOT/bin/clickhouse" local --query "
SELECT * FROM postgresql('localhost:5432', 'rivet', '${TBL}', 'rivet', 'rivet', 'public')
INTO OUTFILE '${OUT_DIR}/data.parquet' FORMAT Parquet
SETTINGS output_format_parquet_compression_method = 'snappy';
" >"$LOG" 2>&1
    RC=$?
    ;;

  odbc2parquet)
    gtime -v -o "$GTIME_OUT" gtimeout 300 odbc2parquet --quiet query \
      --column-length-limit 65536 \
      --connection-string "Driver={postgre_unicode};Server=localhost;Port=5432;Database=rivet;Uid=rivet;Pwd=rivet;" \
      "${OUT_DIR}/data.parquet" \
      "SELECT * FROM public.\"${TBL}\"" >"$LOG" 2>&1
    RC=$?
    ;;
esac

PG_CONN_AFTER=$(PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -tAc \
  "SELECT count(*) FROM pg_stat_activity WHERE datname='rivet' AND state IS NOT NULL")

WALL=$(grep "Elapsed (wall clock)" "$GTIME_OUT" 2>/dev/null | awk -F': ' '{print $NF}')
USER_S=$(grep "User time" "$GTIME_OUT" 2>/dev/null | awk '{print $NF}')
SYS_S=$(grep "System time" "$GTIME_OUT" 2>/dev/null | awk '{print $NF}')
RSS_KB=$(grep "Maximum resident set size" "$GTIME_OUT" 2>/dev/null | awk '{print $NF}')

PQ_FILES=$(find "$OUT_DIR" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
PQ_BYTES=$(find "$OUT_DIR" -name "*.parquet" -exec stat -f '%z' {} \; 2>/dev/null | awk '{s+=$1} END {print s+0}')

ROWS=$("$ROOT/.venv/bin/python" - "$OUT_DIR" 2>/dev/null <<'PY'
import sys, os, pyarrow.parquet as pq
total = 0
for dp, _, files in os.walk(sys.argv[1]):
    for f in files:
        if f.endswith('.parquet'):
            try:
                total += pq.ParquetFile(os.path.join(dp, f)).metadata.num_rows
            except Exception:
                pass
print(total)
PY
)

cat >"$RESULT" <<JSON
{
  "tool": "$TOOL",
  "table": "$TBL",
  "rc": $RC,
  "wall": "$WALL",
  "user_s": "$USER_S",
  "sys_s": "$SYS_S",
  "rss_kb": "$RSS_KB",
  "pq_files": $PQ_FILES,
  "pq_bytes": $PQ_BYTES,
  "rows": ${ROWS:-0},
  "pg_conn_before": $PG_CONN_BEFORE,
  "pg_conn_after": $PG_CONN_AFTER
}
JSON

echo "[$TOOL/$TBL] rc=$RC wall=$WALL rss=${RSS_KB}KB rows=$ROWS files=$PQ_FILES bytes=$PQ_BYTES"
