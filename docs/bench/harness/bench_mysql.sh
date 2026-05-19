#!/usr/bin/env bash
# Per-(tool, table) MySQL benchmark. Mirrors bench.sh for PG.
# Usage: bench_mysql.sh <tool> <table>
# Workspace: $BENCH_ROOT (default /tmp/rivet_bench)

set -uo pipefail
TOOL=$1
TBL=$2
ROOT="${BENCH_ROOT:-/tmp/rivet_bench}"
MY_URL='mysql://rivet:rivet@127.0.0.1:3306/rivet'
RESULT="$ROOT/results_mysql/${TOOL}_${TBL}.json"
LOG="$ROOT/logs_mysql/${TOOL}_${TBL}.log"
mkdir -p "$ROOT/results_mysql" "$ROOT/logs_mysql"

case "$TOOL" in
  rivet)      OUT_DIR="$ROOT/output_mysql/rivet/${TBL}";;
  sling)      OUT_DIR="$ROOT/output_mysql/sling/${TBL}";;
  dlt)        OUT_DIR="$ROOT/output_mysql/dlt/${TBL}";;
  duckdb)     OUT_DIR="$ROOT/output_mysql/duckdb/${TBL}";;
  clickhouse) OUT_DIR="$ROOT/output_mysql/clickhouse/${TBL}";;
esac
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

MY_CONN_BEFORE=$(mysql -h 127.0.0.1 -u rivet -privet -BNe \
  "SELECT count(*) FROM information_schema.processlist WHERE db='rivet';" 2>/dev/null)
GTIME_OUT="$ROOT/logs_mysql/${TOOL}_${TBL}.gtime"

case "$TOOL" in
  rivet)
    SMOKE="$ROOT/_run_rivet_mysql_${TBL}.yaml"
    "$ROOT/.venv/bin/python" - "$TBL" >"$SMOKE" <<'PY'
import sys, os, yaml
tbl = sys.argv[1]
cfg = yaml.safe_load(open(os.environ.get('BENCH_YAML_MYSQL',
    os.path.expanduser('~/rivet/docs/bench/configs/rivet_mysql.yaml'))))
cfg['exports'] = [e for e in cfg['exports'] if e['name'] == tbl]
print(yaml.safe_dump(cfg))
PY
    gtime -v -o "$GTIME_OUT" "$(command -v rivet)" run -c "$SMOKE" >"$LOG" 2>&1
    RC=$?
    ;;

  sling)
    export MYSQL_LOCAL='mysql://rivet:rivet@127.0.0.1:3306/rivet?tls=skip-verify'
    gtime -v -o "$GTIME_OUT" sling run \
      --src-conn MYSQL_LOCAL --src-stream "rivet.${TBL}" \
      --tgt-object "file://${OUT_DIR}/data.parquet" \
      --tgt-options '{format: parquet, compression: snappy}' \
      --mode full-refresh >"$LOG" 2>&1
    RC=$?
    ;;

  dlt)
    gtime -v -o "$GTIME_OUT" "$ROOT/.venv/bin/python" \
      "$(dirname "$0")/dlt_mysql_pipeline.py" "$TBL" "$ROOT/output_mysql/dlt" \
      'mysql+pymysql://rivet:rivet@127.0.0.1:3306/rivet' >"$LOG" 2>&1
    RC=$?
    ;;

  duckdb)
    gtime -v -o "$GTIME_OUT" duckdb -c "
INSTALL mysql; LOAD mysql;
ATTACH 'host=127.0.0.1 port=3306 user=rivet password=rivet database=rivet' AS my (TYPE mysql, READ_ONLY);
COPY (SELECT * FROM my.rivet.\"${TBL}\") TO '${OUT_DIR}/data.parquet' (FORMAT parquet, COMPRESSION snappy);
" >"$LOG" 2>&1
    RC=$?
    ;;

  clickhouse)
    gtime -v -o "$GTIME_OUT" "$ROOT/bin/clickhouse" local --query "
SELECT * FROM mysql('127.0.0.1:3306', 'rivet', '${TBL}', 'rivet', 'rivet')
INTO OUTFILE '${OUT_DIR}/data.parquet' FORMAT Parquet
SETTINGS output_format_parquet_compression_method = 'snappy';
" >"$LOG" 2>&1
    RC=$?
    ;;
esac

MY_CONN_AFTER=$(mysql -h 127.0.0.1 -u rivet -privet -BNe \
  "SELECT count(*) FROM information_schema.processlist WHERE db='rivet';" 2>/dev/null)

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
  "my_conn_before": ${MY_CONN_BEFORE:-0},
  "my_conn_after": ${MY_CONN_AFTER:-0}
}
JSON

echo "[$TOOL/$TBL] rc=$RC wall=$WALL rss=${RSS_KB}KB rows=$ROWS files=$PQ_FILES bytes=$PQ_BYTES"
