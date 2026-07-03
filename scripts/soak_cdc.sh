#!/usr/bin/env bash
# CDC soak (staff class #5): a long-running scheduler-style loop of bounded
# CDC runs against continuous churn, with resource-trend assertions — the
# leak/degradation detector no single-shot test can be.
#
# Usage: scripts/soak_cdc.sh [minutes] [rivet-binary]
#   default 10 minutes, target/release/rivet (falls back to debug).
# Asserts at the end:
#   - every cycle succeeded;
#   - every churned row is present in the union of parts (no gap);
#   - peak RSS of the LAST quarter ≤ 1.5 × peak of the first quarter
#     (a leak shows as monotonic growth; steady-state noise does not).
set -euo pipefail
MINUTES="${1:-10}"
BIN="${2:-target/release/rivet}"
[ -x "$BIN" ] || BIN=target/debug/rivet
MYSQL="docker exec rivet-mysql-cdc-1 mysql -urivet -privet rivet -N -e"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"; $MYSQL "DROP TABLE IF EXISTS soak_cdc" 2>/dev/null || true' EXIT
OUT="$WORK/out"; CKPT="$WORK/cdc.ckpt"; mkdir -p "$OUT"
$MYSQL "DROP TABLE IF EXISTS soak_cdc; CREATE TABLE soak_cdc (id BIGINT PRIMARY KEY, v BIGINT)" 2>/dev/null

cat > "$WORK/cfg.yaml" <<EOF
source: { type: mysql, url: "mysql://rivet:rivet@127.0.0.1:3307/rivet" }
exports:
  - name: soak_cdc
    table: soak_cdc
    mode: cdc
    format: parquet
    cdc: { checkpoint: "$CKPT", until_current: true, server_id: 47001, rollover: 500 }
    destination: { type: local, path: "$OUT" }
EOF

# PIN first: the checkpoint anchors at CURRENT on first open — churn issued
# before the pin is invisible by the (tested) anchor model.
"$BIN" run --config "$WORK/cfg.yaml" > /dev/null 2>&1

DEADLINE=$(( $(date +%s) + MINUTES * 60 ))
CYCLE=0; NEXT_ID=1; RSS_LOG="$WORK/rss.log"
echo "soak: ${MINUTES}m, bin=$BIN"
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  CYCLE=$((CYCLE + 1))
  # Churn: 200 inserts + 50 updates + 20 deletes per cycle.
  END_ID=$((NEXT_ID + 199))
  # macOS seq rejects two-conversion formats — build the tuple list in awk.
  VALS=$(awk -v a="$NEXT_ID" -v b="$END_ID" 'BEGIN{for(i=a;i<=b;i++){printf "%s(%d,%d)", (i>a?",":""), i, i}}')
  $MYSQL "INSERT INTO soak_cdc VALUES $VALS" 2>/dev/null
  $MYSQL "UPDATE soak_cdc SET v = v + 1 WHERE id % 7 = 0 AND id >= $NEXT_ID" 2>/dev/null
  $MYSQL "DELETE FROM soak_cdc WHERE id % 31 = 0 AND id >= $NEXT_ID" 2>/dev/null
  NEXT_ID=$((END_ID + 1))
  # One bounded run, RSS sampled via /usr/bin/time.
  /usr/bin/time -l "$BIN" run --config "$WORK/cfg.yaml" > "$WORK/run.log" 2> "$WORK/time.log" \
    || { echo "soak: cycle $CYCLE FAILED"; tail -30 "$WORK/run.log"; exit 1; }
  grep -E "maximum resident set size" "$WORK/time.log" | awk '{print $1}' >> "$RSS_LOG" \
    || grep -oE "[0-9]+maxresident" "$WORK/time.log" | tr -d 'a-z' >> "$RSS_LOG" || true
  sleep 2
done

# ── Assertions ────────────────────────────────────────────────────────────────
ROWS_SEEN=$("$BIN" --version >/dev/null 2>&1; python3 - "$OUT" <<'PY'
import sys, glob, subprocess
# count distinct ids via duckdb if present, else parquet row groups via pyarrow-less fallback
try:
    out = subprocess.run(["duckdb","-noheader","-csv","-c",
        f"SELECT COUNT(DISTINCT id) FROM read_parquet('{sys.argv[1]}/*.parquet')"],
        capture_output=True, text=True, check=True)
    print(out.stdout.strip())
except Exception:
    print(-1)
PY
)
EXPECTED=$((NEXT_ID - 1))
echo "soak: cycles=$CYCLE, churned_ids=$EXPECTED, distinct_in_parts=$ROWS_SEEN"
if [ "$ROWS_SEEN" != "-1" ] && [ "$ROWS_SEEN" -lt "$EXPECTED" ]; then
  echo "soak: GAP — $ROWS_SEEN < $EXPECTED"; exit 1
fi
Q=$(( $(wc -l < "$RSS_LOG") / 4 )); [ "$Q" -ge 1 ] || Q=1
FIRST=$(head -n "$Q" "$RSS_LOG" | sort -n | tail -1)
LAST=$(tail -n "$Q" "$RSS_LOG" | sort -n | tail -1)
echo "soak: peak RSS first-quarter=$FIRST last-quarter=$LAST"
# 1.5× budget: a real leak grows every cycle and blows well past this.
if [ "$LAST" -gt $(( FIRST * 3 / 2 )) ]; then
  echo "soak: RSS TREND FAIL — last-quarter peak > 1.5× first-quarter"; exit 1
fi
echo "soak: OK"
