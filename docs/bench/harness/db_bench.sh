#!/usr/bin/env bash
# DB-signal benchmark for Postgres: wraps bench.sh with pg_stat_* snapshots.
#
# Captures per (tool, table):
#   - pg_stat_database delta — commits, blks_read/hit, tuples, temp_files/bytes
#   - pg_stat_bgwriter delta — checkpoint pressure
#   - pg_stat_io delta (PG 16+) — IO ops
#   - pg_stat_activity sampler @ 50 ms — peak active backends, longest query,
#     distinct query templates observed
#
# Usage: db_bench.sh <tool> <table>

set -uo pipefail
TOOL=$1
TBL=$2
ROOT="${BENCH_ROOT:-/tmp/rivet_bench}"
mkdir -p "$ROOT/db_signals"

PG="PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -tAc"

PRE=$(mktemp); POST=$(mktemp)
PRE_BGW=$(mktemp); POST_BGW=$(mktemp)
PRE_IO=$(mktemp); POST_IO=$(mktemp)

snap_db() {
  eval $PG "\"
SELECT json_build_object(
  'xact_commit',     xact_commit,
  'xact_rollback',   xact_rollback,
  'blks_read',       blks_read,
  'blks_hit',        blks_hit,
  'tup_returned',    tup_returned,
  'tup_fetched',     tup_fetched,
  'temp_files',      temp_files,
  'temp_bytes',      temp_bytes,
  'deadlocks',       deadlocks,
  'session_time_ms', session_time::bigint,
  'active_time_ms',  active_time::bigint
) FROM pg_stat_database WHERE datname='rivet';\"" > "$1"
}
snap_bgw() {
  eval $PG "\"
SELECT json_build_object(
  'checkpoints_timed',   checkpoints_timed,
  'checkpoints_req',     checkpoints_req,
  'buffers_checkpoint',  buffers_checkpoint,
  'buffers_clean',       buffers_clean,
  'buffers_backend',     buffers_backend
) FROM pg_stat_bgwriter;\"" > "$1"
}
snap_io() {
  eval $PG "\"
SELECT json_build_object(
  'reads',  COALESCE(SUM(reads),0),
  'writes', COALESCE(SUM(writes),0),
  'extends', COALESCE(SUM(extends),0),
  'hits',   COALESCE(SUM(hits),0),
  'evictions', COALESCE(SUM(evictions),0)
) FROM pg_stat_io WHERE backend_type IN ('client backend','background writer','checkpointer');\"" > "$1"
}

snap_db "$PRE"; snap_bgw "$PRE_BGW"; snap_io "$PRE_IO"

SAMPLE_FILE=$(mktemp)
SAMPLER_SQL=$(cat <<'SQL'
SELECT
  count(*) FILTER (WHERE state <> 'idle' AND backend_type='client backend') AS active_backends,
  count(*) FILTER (WHERE backend_type='client backend') AS total_backends,
  COALESCE(extract(epoch from max(now() - query_start) FILTER (WHERE state <> 'idle')), 0) AS longest_query_s,
  COALESCE(string_agg(DISTINCT LEFT(query, 80), '||') FILTER (WHERE state <> 'idle' AND backend_type='client backend'), '') AS q_templates
FROM pg_stat_activity
WHERE datname='rivet' AND pid <> pg_backend_pid();
SQL
)
(
  while true; do
    PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -tAc "$SAMPLER_SQL" 2>/dev/null
    sleep 0.05
  done
) > "$SAMPLE_FILE" &
SAMPLER_PID=$!

"$(dirname "$0")/bench.sh" "$TOOL" "$TBL" > /dev/null 2>&1
TOOL_RC=$?

kill "$SAMPLER_PID" 2>/dev/null
wait "$SAMPLER_PID" 2>/dev/null

snap_db "$POST"; snap_bgw "$POST_BGW"; snap_io "$POST_IO"

SAMPLE_LINES=$(wc -l < "$SAMPLE_FILE" | tr -d ' ')
PEAK_ACTIVE=$(awk -F'|' '{if($1+0>m)m=$1+0} END {print m+0}' "$SAMPLE_FILE")
PEAK_TOTAL=$(awk -F'|' '{if($2+0>m)m=$2+0} END {print m+0}' "$SAMPLE_FILE")
LONGEST_Q=$(awk -F'|' '{if($3+0>m)m=$3+0} END {printf "%.3f", m+0}' "$SAMPLE_FILE")
TEMPLATES_RAW=$(awk -F'|' '{print $4}' "$SAMPLE_FILE" | tr '|' '\n' | sed 's/^[ \t]*//;s/[ \t]*$//' | sort -u | grep -v '^$')
DISTINCT_TEMPLATES=$(printf '%s\n' "$TEMPLATES_RAW" | grep -c '[^[:space:]]' 2>/dev/null || true)
DISTINCT_TEMPLATES=${DISTINCT_TEMPLATES:-0}
TEMPLATES_FILE=$(mktemp)
echo "$TEMPLATES_RAW" > "$TEMPLATES_FILE"

RESULT="$ROOT/db_signals/${TOOL}_${TBL}.json"
"$ROOT/.venv/bin/python" - "$PRE" "$POST" "$PRE_BGW" "$POST_BGW" "$PRE_IO" "$POST_IO" \
  "$PEAK_ACTIVE" "$PEAK_TOTAL" "$LONGEST_Q" "$DISTINCT_TEMPLATES" "$SAMPLE_LINES" \
  "$TOOL" "$TBL" "$TOOL_RC" "$TEMPLATES_FILE" >"$RESULT" <<'PY'
import json, sys
pre_db = json.loads(open(sys.argv[1]).read().strip())
post_db = json.loads(open(sys.argv[2]).read().strip())
pre_bgw = json.loads(open(sys.argv[3]).read().strip())
post_bgw = json.loads(open(sys.argv[4]).read().strip())
pre_io = json.loads(open(sys.argv[5]).read().strip())
post_io = json.loads(open(sys.argv[6]).read().strip())
peak_active, peak_total = int(sys.argv[7]), int(sys.argv[8])
longest_q = float(sys.argv[9])
distinct_templates = int(sys.argv[10])
sample_lines = int(sys.argv[11])
tool, table, rc = sys.argv[12], sys.argv[13], int(sys.argv[14])
templates = [t for t in open(sys.argv[15]).read().splitlines() if t.strip()]

def delta(post, pre):
    return {k: max(0, int(post[k]) - int(pre[k])) for k in pre if isinstance(pre[k], (int, float))}

print(json.dumps({
    "tool": tool, "table": table, "rc": rc, "samples": sample_lines,
    "pg_stat_database_delta": delta(post_db, pre_db),
    "pg_stat_bgwriter_delta": delta(post_bgw, pre_bgw),
    "pg_stat_io_delta": delta(post_io, pre_io),
    "activity_during_run": {
        "peak_active_backends": peak_active,
        "peak_total_backends":  peak_total,
        "longest_query_seconds": longest_q,
        "distinct_query_templates": distinct_templates,
        "templates_observed": templates,
    },
}, indent=2))
PY

rm -f "$PRE" "$POST" "$PRE_BGW" "$POST_BGW" "$PRE_IO" "$POST_IO" "$SAMPLE_FILE" "$TEMPLATES_FILE"
echo "[db_bench/$TOOL/$TBL] rc=$TOOL_RC active=$PEAK_ACTIVE total=$PEAK_TOTAL longest=${LONGEST_Q}s templates=$DISTINCT_TEMPLATES samples=$SAMPLE_LINES"
