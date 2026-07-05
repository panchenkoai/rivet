#!/usr/bin/env bash
# CDC drain-interval experiment — PostgreSQL edition.
#
# The PostgreSQL sibling of run.sh (which is MySQL). Same 10-table heat spectrum
# (2 hot / 3 warm / 3 cold / 2 frozen), ONE multi-table CDC export (one logical
# slot, one checkpoint, initial snapshots), a continuous loader, and per-table
# convergence verdicts at growing drain intervals.
#
# WHAT IS DIFFERENT FROM MYSQL — and the reason this experiment exists:
# MySQL's binlog is retained for replication/PITR regardless of rivet, so the
# drain interval is "free" (bounded only by binlog retention). PostgreSQL's
# logical slot PINS WAL: the WAL since the last advance is held ONLY because
# rivet's slot has not consumed it. So a longer drain interval is NOT free here
# — it shows up as retained WAL growing on the SOURCE (disk pressure), released
# only when rivet drains and advances the slot. This harness measures exactly
# that: `retained WAL` = pg_current_wal_lsn() - restart_lsn, sampled at the peak
# (end of interval, before the drain) and after the drain (released).
#
# Ordering oracle: PostgreSQL __pos is {lsn:"X/Y"} (the COMMIT LSN). The lexical
# "X/Y" string is NOT sortable across widths; the winner-per-id order is the
# fixed-width hex key upper(lpad(hi,8,'0'))||upper(lpad(lo,8,'0')), tie-broken by
# (filename, file_row_number) for events that share a COMMIT LSN.
#
# Portable to macOS's bash 3.2 (no assoc arrays — per-table state via files).
#
# Usage:
#   RIVET_BIN=target/release/rivet dev/cdc_interval/run_pg.sh            # full run
#   PHASES="2 2;4 1;6 1" dev/cdc_interval/run_pg.sh                      # custom schedule
#   SKIP_SETUP=1 dev/cdc_interval/run_pg.sh                             # resume (keep tables/slot)
#
# Requires: docker compose --profile cdc up -d postgres-cdc, and `duckdb` on PATH
# (the destination-side merge oracle). Writes everything under $WORK.
set -uo pipefail

RIVET_BIN="${RIVET_BIN:-target/release/rivet}"
PG_CDC_URL="${PG_CDC_URL:-postgresql://rivet:rivet@127.0.0.1:5434/rivet}"
PG_CONTAINER="${PG_CONTAINER:-rivet-postgres-cdc-1}"
SLOT="${SLOT:-cdc_interval_pg}"
WORK="${WORK:-/tmp/rivet-cdc-interval-pg}"
# "<minutes> <reps>" per phase, ';'-separated. Default: three growing intervals.
PHASES="${PHASES:-2 2;4 1;6 1}"

R="$RIVET_BIN"
CFG="$WORK/cdc.yaml"; OUT="$WORK/out"; LOG="$WORK/progress.log"
PAUSE="$WORK/loader.pause"; STOP="$WORK/loader.stop"
HOT="ci_h1 ci_h2"; WARM="ci_w1 ci_w2 ci_w3"; COLD="ci_c1 ci_c2 ci_c3"; FROZEN="ci_f1 ci_f2"
ALL="$HOT $WARM $COLD $FROZEN"
PG() { docker exec "$PG_CONTAINER" psql -U rivet -d rivet -tAc "$1" 2>/dev/null; }
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

# retained WAL (bytes) the slot is pinning = current WAL end - slot restart_lsn.
wal_retained_mb() {
  local b; b=$(PG "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint \
                   FROM pg_replication_slots WHERE slot_name='$SLOT'")
  echo $(( ${b:-0} / 1048576 ))
}
slot_line() {
  PG "SELECT active::text||' '||wal_status||' restart='||restart_lsn||' confirmed='||confirmed_flush_lsn \
      FROM pg_replication_slots WHERE slot_name='$SLOT'"
}

mkdir -p "$OUT"; rm -f "$PAUSE" "$STOP"
cat > "$CFG" <<EOF
source: { type: postgres, url: "$PG_CDC_URL" }
exports:
  - name: cdc_interval
    tables: [$(echo $ALL | tr ' ' ',' | sed 's/,/, /g')]
    mode: cdc
    format: parquet
    cdc: { initial: snapshot, checkpoint: "$WORK/cdc.ckpt", slot: $SLOT, until_current: true, rollover: 50000 }
    destination: { type: local, path: "$OUT" }
EOF

if [ "${SKIP_SETUP:-0}" != "1" ]; then
  # Start clean: drop the slot (frees pinned WAL) and the tables + checkpoint.
  PG "SELECT pg_drop_replication_slot('$SLOT') FROM pg_replication_slots WHERE slot_name='$SLOT'" >/dev/null
  rm -f "$WORK/cdc.ckpt"; rm -rf "$OUT"; mkdir -p "$OUT"
  for t in $ALL; do
    PG "DROP TABLE IF EXISTS $t;
        CREATE TABLE $t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, pad VARCHAR(64) NOT NULL,
          row_hash TEXT GENERATED ALWAYS AS (md5(id::text||'#'||v::text||'#'||pad)) STORED);" >/dev/null
    echo 1 > "$WORK/next.$t"
  done
  for t in $ALL; do   # pre-seed so initial snapshots are non-trivial
    vals=$(awk 'BEGIN{for(i=1;i<=2000;i++){printf "%s(%d,%d,'"'"'seed%d'"'"')",(i>1?",":""),i,i*3,i%97}}')
    PG "INSERT INTO $t (id,v,pad) VALUES $vals" >/dev/null; echo 2001 > "$WORK/next.$t"
  done
  say "pre-seeded 10 tables x 2000 rows"
fi

insert_batch() { # table n
  local t=$1 n=$2 a b vals
  a=$(cat "$WORK/next.$t"); b=$((a + n - 1))
  vals=$(awk -v a="$a" -v b="$b" 'BEGIN{for(i=a;i<=b;i++){printf "%s(%d,%d,'"'"'p%d'"'"')",(i>a?",":""),i,i*7,i%1000}}')
  PG "INSERT INTO $t (id,v,pad) VALUES $vals" >/dev/null || say "loader: $t insert @$a failed"
  echo $((b + 1)) > "$WORK/next.$t"
}

loader() {  # ~230 rows/s across the spectrum; hot tables also purge (delete-load)
  local tick=0
  while [ ! -f "$STOP" ]; do
    [ -f "$PAUSE" ] && { sleep 1; continue; }
    tick=$((tick + 1))
    for t in $HOT;  do insert_batch "$t" 250
      PG "UPDATE $t SET v=v+1 WHERE id % 17 = $((tick % 17)) AND id > $(( $(cat "$WORK/next.$t") - 5000 ))" >/dev/null; done
    for t in $WARM; do insert_batch "$t" 50; done
    [ $((tick % 6))  -eq 0 ] && for t in $COLD;   do insert_batch "$t" 3; done
    [ $((tick % 60)) -eq 0 ] && for t in $FROZEN; do insert_batch "$t" 1; done
    if [ $((tick % 12)) -eq 0 ]; then
      for t in $HOT; do local cut=$(( $(cat "$WORK/next.$t") - 60000 ))
        # PostgreSQL DELETE has no LIMIT — bound it by ctid subselect.
        [ $cut -gt 2000 ] && PG "DELETE FROM $t WHERE ctid IN \
          (SELECT ctid FROM $t WHERE id < $cut AND id > 2000 LIMIT 80000)" >/dev/null; done
    fi
    sleep 5
  done
  say "loader stopped"
}

wal_sampler() {  # continuous WAL time-series — the disk-pressure curve + a disk-safety watch
  local csv="$WORK/wal_trace.csv" b w free
  echo "epoch,retained_mb,waldir_mb,vol_avail_mb" > "$csv"
  while [ ! -f "$STOP" ]; do
    b=$(PG "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint FROM pg_replication_slots WHERE slot_name='$SLOT'")
    w=$(PG "SELECT COALESCE(sum(size),0)::bigint FROM pg_ls_waldir()")
    free=$(docker exec "$PG_CONTAINER" df -Pm /var/lib/postgresql/data 2>/dev/null | awk 'NR==2{print $4}')
    echo "$(date +%s),$(( ${b:-0}/1048576 )),$(( ${w:-0}/1048576 )),${free:-?}" >> "$csv"
    sleep 120
  done
}

drain() { # -> "rows elapsedS rssMB"
  local t0 t1 rows rss; t0=$(date +%s)
  /usr/bin/time -l "$R" run -c "$CFG" > "$WORK/d.out" 2> "$WORK/d.err"
  t1=$(date +%s)
  rows=$(cat "$WORK/d.out" "$WORK/d.err" | grep -E "^[[:space:]]+rows:" | head -1 | tr -dc '0-9')
  rss=$(grep -E "maximum resident set size" "$WORK/d.err" | awk '{print $1}')
  echo "${rows:-?} $((t1 - t0))s $(( ${rss:-0} / 1048576 ))MB"
}

merge_metric() { # table -> "count xor" of the reconstructed current state
  local t=$1 snap_arm=""
  ls "$OUT/$t"/snapshot/*.parquet >/dev/null 2>&1 && snap_arm="
    SELECT id,row_hash,'' AS lsn_key,'insert' AS op,'' AS fn,-1::BIGINT AS frn
    FROM read_parquet('$OUT/$t/snapshot/*.parquet') UNION ALL"
  if ls "$OUT/$t"/cdc-*.parquet >/dev/null 2>&1; then
    duckdb -noheader -csv -c "
      WITH uni AS ($snap_arm
        SELECT id,row_hash,
          upper(lpad(split_part(json_extract_string(__pos,'\$.lsn'),'/',1),8,'0')) ||
          upper(lpad(split_part(json_extract_string(__pos,'\$.lsn'),'/',2),8,'0')) AS lsn_key,
          __op AS op, filename AS fn, file_row_number AS frn
        FROM read_parquet('$OUT/$t/cdc-*.parquet', filename=true, file_row_number=true)),
      r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY lsn_key DESC, fn DESC, frn DESC) rn FROM uni)
      SELECT COUNT(*)||' '||COALESCE(bit_xor(CAST(concat('0x',substring(row_hash,1,15)) AS UBIGINT)),0)
      FROM r WHERE rn=1 AND op<>'delete'" 2>>"$LOG"
  else
    duckdb -noheader -csv -c "
      SELECT COUNT(*)||' '||COALESCE(bit_xor(CAST(concat('0x',substring(row_hash,1,15)) AS UBIGINT)),0)
      FROM read_parquet('$OUT/$t/snapshot/*.parquet')" 2>>"$LOG"
  fi
}

verify() {
  local wal_peak; wal_peak=$(wal_retained_mb)     # end-of-interval peak: slot pinned this much WAL
  say "  slot before drain: $(slot_line) | retained WAL=${wal_peak}MB"
  touch "$PAUSE"; sleep 7          # let the in-flight batch land, then quiesce
  local d1; d1=$(drain)            # the big backlog drain — advances the slot
  local t
  for t in $ALL; do
    PG "SELECT COUNT(*) FROM $t" > "$WORK/sc.$t"
    PG "SELECT COALESCE(bit_xor(('x'||substring(row_hash,1,15))::bit(60)::bigint),0) FROM $t" > "$WORK/sx.$t"
    echo $(( $(cat "$WORK/next.$t") - 1 )) > "$WORK/mx.$t"
  done
  local d2; d2=$(drain)            # must be 0: nothing ran between the two drains
  local wal_after; wal_after=$(wal_retained_mb)   # released after the advance
  rm -f "$PAUSE"
  local verdict="OK" line
  for t in $ALL; do
    local mm dc dx; mm=$(merge_metric "$t"); dc=${mm% *}; dx=${mm#* }
    local seen; seen=$(duckdb -noheader -csv -c "
      SELECT COUNT(DISTINCT id) FROM (
        $(ls "$OUT/$t"/cdc-*.parquet >/dev/null 2>&1 && echo "SELECT id FROM read_parquet('$OUT/$t/cdc-*.parquet') WHERE __op='insert' UNION")
        SELECT id FROM read_parquet('$OUT/$t/snapshot/*.parquet'))" 2>>"$LOG")
    local sc sx mx v="ok"; sc=$(cat "$WORK/sc.$t"); sx=$(cat "$WORK/sx.$t"); mx=$(cat "$WORK/mx.$t")
    [ "$sc" = "$dc" ] || v="COUNT($sc!=$dc)"
    [ "$sx" = "$dx" ] || v="$v XOR"
    [ "$seen" = "$mx" ] || v="$v CONS($seen!=$mx)"
    [ "$v" = "ok" ] || verdict="FAIL"
    line="$line $t=$v"
  done
  [ "${d2%% *}" = "0" ] || verdict="$verdict WARN(d2=${d2%% *})"
  say "VERIFY d1=[$d1] d2=[$d2] WAL:${wal_peak}MB->${wal_after}MB =>$line"
  say "TOTAL: $verdict | parts=$(find "$OUT" -name 'cdc-*.parquet' | wc -l | tr -d ' ')"
}

[ "${SKIP_SETUP:-0}" = "1" ] || { say "=== pin slot + initial snapshots ==="
  "$R" run -c "$CFG" > "$WORK/d.out" 2>&1 && say "pin+snapshots ok | $(slot_line)" \
    || { say "PIN FAILED: $(tail -3 "$WORK/d.out")"; exit 1; }; }
loader & LP=$!; say "loader pid=$LP"
wal_sampler & WS=$!; say "wal_sampler pid=$WS (trace -> $WORK/wal_trace.csv)"

IFS=';'; for phase in $PHASES; do IFS=' '
  set -- $phase; mins=$1; reps=$2
  for rep in $(seq 1 "$reps"); do
    say "--- phase ${mins}min rep $rep/$reps (slot pinning WAL for ${mins}min) ---"; sleep $((mins * 60)); verify
  done
done
touch "$STOP"; wait "$LP" 2>/dev/null; wait "$WS" 2>/dev/null
say "=== final verify after loader stop ==="; verify
for t in $ALL; do PG "DROP TABLE IF EXISTS $t" >/dev/null; done
PG "SELECT pg_drop_replication_slot('$SLOT') FROM pg_replication_slots WHERE slot_name='$SLOT'" >/dev/null
say "=== complete (slot dropped, WAL released) ==="
