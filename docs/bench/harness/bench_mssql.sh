#!/usr/bin/env bash
# Competitive perf bench for SQL Server — one (tool, table) cell.
# SQL Server twin of bench_mysql.sh. Tool set is rivet / sling / dlt: DuckDB has
# no SQL Server scanner and ClickHouse has no mssql() table function, so they
# drop out; odbc2parquet needs the MS ODBC Driver 18 (not installed here).
#
# Usage: RIVET=./target/release/rivet docs/bench/harness/bench_mssql.sh <tool> <table>

set -uo pipefail
TOOL=$1
TBL=$2
ROOT="${BENCH_ROOT:-/tmp/rivet_bench}"
RIVET="${RIVET:-$(command -v rivet)}"
OUT_DIR="$ROOT/output_mssql/$TOOL/$TBL"
mkdir -p "$ROOT/logs_mssql" "$ROOT/results_mssql"
rm -rf "$OUT_DIR"; mkdir -p "$OUT_DIR"

GTIME_OUT="$ROOT/logs_mssql/${TOOL}_${TBL}.gtime"
LOG="$ROOT/logs_mssql/${TOOL}_${TBL}.log"
RESULT="$ROOT/results_mssql/${TOOL}_${TBL}.json"

# go-mssqldb URL: the database goes in ?database=, the path is the instance name.
export MSSQL_SLING='sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433?database=rivet&encrypt=disable&TrustServerCertificate=true'

RC=0
case "$TOOL" in
  rivet)
    CFG="$ROOT/_run_rivet_mssql_${TBL}.yaml"
    cat > "$CFG" <<YAML
source:
  type: mssql
  url: "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet"
  tls:
    accept_invalid_certs: true
exports:
  - name: $TBL
    table: $TBL
    mode: chunked
    chunk_size_memory_mb: 256
    format: parquet
    compression: snappy
    destination: { type: local, path: $OUT_DIR }
YAML
    gtime -v -o "$GTIME_OUT" "$RIVET" run -c "$CFG" >"$LOG" 2>&1; RC=$?
    ;;
  sling)
    gtime -v -o "$GTIME_OUT" sling run \
      --src-conn MSSQL_SLING --src-stream "dbo.${TBL}" \
      --tgt-object "file://${OUT_DIR}/data.parquet" >"$LOG" 2>&1; RC=$?
    ;;
  dlt)
    gtime -v -o "$GTIME_OUT" "$ROOT/.venv/bin/python" \
      "$(dirname "$0")/dlt_mssql_pipeline.py" "$TBL" "$ROOT/output_mssql/dlt" \
      'mssql+pymssql://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet' >"$LOG" 2>&1; RC=$?
    ;;
  *) echo "unknown tool: $TOOL" >&2; exit 2;;
esac

WALL=$(grep "Elapsed (wall clock)" "$GTIME_OUT" 2>/dev/null | awk -F': ' '{print $NF}')
USER_S=$(grep "User time" "$GTIME_OUT" 2>/dev/null | awk '{print $NF}')
RSS_KB=$(grep "Maximum resident set size" "$GTIME_OUT" 2>/dev/null | awk '{print $NF}')
PQ_FILES=$(find "$OUT_DIR" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
PQ_BYTES=$(find "$OUT_DIR" -name "*.parquet" -exec stat -f '%z' {} \; 2>/dev/null | awk '{s+=$1} END {print s+0}')
ROWS=$("$ROOT/.venv/bin/python" - "$OUT_DIR" 2>/dev/null <<'PY'
import sys, os, pyarrow.parquet as pq
total = 0
for dp, _, files in os.walk(sys.argv[1]):
    for f in files:
        if f.endswith('.parquet'):
            try: total += pq.ParquetFile(os.path.join(dp, f)).metadata.num_rows
            except Exception: pass
print(total)
PY
)

cat >"$RESULT" <<JSON
{ "tool": "$TOOL", "table": "$TBL", "rc": $RC, "wall": "$WALL", "user_s": "$USER_S",
  "rss_kb": "${RSS_KB:-0}", "pq_files": $PQ_FILES, "pq_bytes": $PQ_BYTES, "rows": ${ROWS:-0} }
JSON

echo "[$TOOL/$TBL] rc=$RC wall=$WALL rss=${RSS_KB}KB rows=$ROWS files=$PQ_FILES bytes=$PQ_BYTES"
