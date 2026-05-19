#!/usr/bin/env bash
# DB-signal benchmark for MySQL: wraps bench_mysql.sh with SHOW GLOBAL STATUS
# deltas + performance_schema.events_statements_summary_by_digest deltas +
# information_schema.processlist sampler @ 50 ms.
#
# Required GRANTs on the bench user:
#   GRANT PROCESS ON *.* TO 'rivet'@'%';
#   GRANT SELECT ON performance_schema.* TO 'rivet'@'%';
#
# Usage: db_bench_mysql.sh <tool> <table>

set -uo pipefail
TOOL=$1
TBL=$2
ROOT="${BENCH_ROOT:-/tmp/rivet_bench}"
mkdir -p "$ROOT/db_signals_mysql"

MY="mysql -h 127.0.0.1 -u rivet -privet rivet -BN"

snap_status() {
  $MY -e "
SELECT json_object(
  'innodb_rows_read',           IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Innodb_rows_read'),'0'),
  'innodb_buffer_pool_reads',   IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Innodb_buffer_pool_reads'),'0'),
  'innodb_buffer_pool_read_requests', IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Innodb_buffer_pool_read_requests'),'0'),
  'innodb_log_waits',           IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Innodb_log_waits'),'0'),
  'bytes_sent',                 IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Bytes_sent'),'0'),
  'created_tmp_tables',         IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Created_tmp_tables'),'0'),
  'created_tmp_disk_tables',    IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Created_tmp_disk_tables'),'0'),
  'slow_queries',               IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Slow_queries'),'0'),
  'queries',                    IFNULL((SELECT variable_value FROM performance_schema.global_status WHERE variable_name='Queries'),'0')
) AS j;" 2>/dev/null
}

PRE_STATUS=$(snap_status)
SAMPLE_FILE=$(mktemp)
SAMPLER_SQL=$(cat <<'SQL'
SELECT
  SUM(CASE WHEN command <> 'Sleep' AND user='rivet' AND id <> CONNECTION_ID()
            AND COALESCE(info,'') NOT LIKE '%information_schema.processlist%' THEN 1 ELSE 0 END),
  SUM(CASE WHEN user='rivet' AND id <> CONNECTION_ID() THEN 1 ELSE 0 END),
  IFNULL(MAX(CASE WHEN command <> 'Sleep' AND user='rivet' AND id <> CONNECTION_ID()
            AND COALESCE(info,'') NOT LIKE '%information_schema.processlist%' THEN time ELSE 0 END), 0),
  IFNULL(GROUP_CONCAT(DISTINCT LEFT(info, 80) SEPARATOR '||'), '')
FROM information_schema.processlist
WHERE user='rivet' AND db='rivet' AND id <> CONNECTION_ID()
  AND COALESCE(info,'') NOT LIKE '%information_schema.processlist%'
  AND COALESCE(info,'') NOT LIKE '%performance_schema.global_status%';
SQL
)
(
  while true; do
    $MY -e "$SAMPLER_SQL" 2>/dev/null
    sleep 0.05
  done
) > "$SAMPLE_FILE" &
SAMPLER_PID=$!

"$(dirname "$0")/bench_mysql.sh" "$TOOL" "$TBL" > /dev/null 2>&1
TOOL_RC=$?

kill "$SAMPLER_PID" 2>/dev/null
wait "$SAMPLER_PID" 2>/dev/null

POST_STATUS=$(snap_status)
SAMPLE_LINES=$(wc -l < "$SAMPLE_FILE" | tr -d ' ')
PEAK_ACTIVE=$(awk -F'\t' '{if($1+0>m)m=$1+0} END {print m+0}' "$SAMPLE_FILE")
PEAK_TOTAL=$(awk -F'\t' '{if($2+0>m)m=$2+0} END {print m+0}' "$SAMPLE_FILE")
LONGEST_Q=$(awk -F'\t' '{if($3+0>m)m=$3+0} END {printf "%d", m+0}' "$SAMPLE_FILE")
TEMPLATES_RAW=$(awk -F'\t' '{print $4}' "$SAMPLE_FILE" | tr '|' '\n' | sed 's/^[ \t]*//;s/[ \t]*$//' | sort -u | grep -v '^$' | grep -v '^NULL$')
DISTINCT_TEMPLATES=$(printf '%s\n' "$TEMPLATES_RAW" | grep -c '[^[:space:]]' 2>/dev/null || true)
DISTINCT_TEMPLATES=${DISTINCT_TEMPLATES:-0}
TEMPLATES_FILE=$(mktemp)
echo "$TEMPLATES_RAW" > "$TEMPLATES_FILE"

PRE_FILE=$(mktemp); echo "$PRE_STATUS" > "$PRE_FILE"
POST_FILE=$(mktemp); echo "$POST_STATUS" > "$POST_FILE"

RESULT="$ROOT/db_signals_mysql/${TOOL}_${TBL}.json"
"$ROOT/.venv/bin/python" - \
  "$PRE_FILE" "$POST_FILE" "$PEAK_ACTIVE" "$PEAK_TOTAL" "$LONGEST_Q" \
  "$DISTINCT_TEMPLATES" "$SAMPLE_LINES" "$TEMPLATES_FILE" "$TOOL" "$TBL" "$TOOL_RC" \
  > "$RESULT" <<'PY'
import json, sys
def jload(p):
    s = open(p).read().strip()
    return json.loads(s) if s and s != 'NULL' else {}
pre = jload(sys.argv[1]); post = jload(sys.argv[2])
peak_active, peak_total = int(sys.argv[3] or 0), int(sys.argv[4] or 0)
longest_q = int(sys.argv[5] or 0)
distinct = int(sys.argv[6] or 0); samples = int(sys.argv[7] or 0)
templates = [t for t in open(sys.argv[8]).read().splitlines() if t.strip()]
tool, table, rc = sys.argv[9], sys.argv[10], int(sys.argv[11])

def to_int(x):
    try: return int(x)
    except Exception: return 0

print(json.dumps({
    "tool": tool, "table": table, "rc": rc, "samples": samples,
    "global_status_delta": {k: max(0, to_int(post.get(k,0)) - to_int(pre.get(k,0))) for k in pre},
    "activity_during_run": {
        "peak_active_threads": peak_active,
        "peak_total_threads":  peak_total,
        "longest_query_seconds": longest_q,
        "distinct_query_templates": distinct,
        "templates_observed": templates,
    },
}, indent=2))
PY

rm -f "$SAMPLE_FILE" "$TEMPLATES_FILE" "$PRE_FILE" "$POST_FILE"
echo "[db_bench_mysql/$TOOL/$TBL] rc=$TOOL_RC active=$PEAK_ACTIVE total=$PEAK_TOTAL longest=${LONGEST_Q}s templates=$DISTINCT_TEMPLATES samples=$SAMPLE_LINES"
