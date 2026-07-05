#!/usr/bin/env bash
# CDC drain-interval experiment: 10 tables across a heat spectrum
# (2 hot / 3 warm / 3 cold / 2 frozen), ONE multi-table CDC export (one stream,
# one checkpoint, initial snapshots), a continuous loader, and per-table
# convergence verdicts at growing drain intervals.
#
# The question it answers: as you drain LESS often, does anything degrade?
# It measures, at every interval, per table: convergence (source count + XOR of
# a stored per-row hash == merge of snapshot ∪ events, winner by (file,pos)),
# conservation (every id ever inserted appears as an insert event or snapshot),
# drain wall-time, drain RSS, and a quiesced control drain that must return 0.
#
# Portable to macOS's bash 3.2 (no assoc arrays — per-table state via files).
#
# Usage:
#   RIVET_BIN=target/release/rivet dev/cdc_interval/run.sh              # full run
#   PHASES="10 2;20 2;30 2" dev/cdc_interval/run.sh                     # custom schedule
#   SKIP_SETUP=1 dev/cdc_interval/run.sh                               # resume (keep tables/ckpt)
#
# Requires: docker compose --profile cdc up -d mysql-cdc, and `duckdb` on PATH
# (the destination-side merge oracle). Writes everything under $WORK.
set -uo pipefail

RIVET_BIN="${RIVET_BIN:-target/release/rivet}"
MYSQL_CDC_URL="${MYSQL_CDC_URL:-mysql://rivet:rivet@127.0.0.1:3307/rivet}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-rivet-mysql-cdc-1}"
WORK="${WORK:-/tmp/rivet-cdc-interval}"
# "<minutes> <reps>" per phase, ';'-separated. Default: 10/10, 20/20, 30/30.
PHASES="${PHASES:-10 2;20 2;30 2}"

R="$RIVET_BIN"
CFG="$WORK/cdc.yaml"; OUT="$WORK/out"; LOG="$WORK/progress.log"
PAUSE="$WORK/loader.pause"; STOP="$WORK/loader.stop"
HOT="ci_h1 ci_h2"; WARM="ci_w1 ci_w2 ci_w3"; COLD="ci_c1 ci_c2 ci_c3"; FROZEN="ci_f1 ci_f2"
ALL="$HOT $WARM $COLD $FROZEN"
M() { docker exec "$MYSQL_CONTAINER" mysql -urivet -privet rivet -N -e "$1" 2>/dev/null; }
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

mkdir -p "$OUT"; rm -f "$PAUSE" "$STOP"
cat > "$CFG" <<EOF
source: { type: mysql, url: "$MYSQL_CDC_URL" }
exports:
  - name: cdc_interval
    tables: [$(echo $ALL | tr ' ' ',' | sed 's/,/, /g')]
    mode: cdc
    format: parquet
    cdc: { initial: snapshot, checkpoint: "$WORK/cdc.ckpt", until_current: true, server_id: 48121, rollover: 50000 }
    destination: { type: local, path: "$OUT" }
EOF

if [ "${SKIP_SETUP:-0}" != "1" ]; then
  for t in $ALL; do
    M "DROP TABLE IF EXISTS $t; CREATE TABLE $t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, \
       pad VARCHAR(64) NOT NULL, \
       row_hash CHAR(32) AS (MD5(CONCAT_WS('#',id,v,pad))) STORED);"
    echo 1 > "$WORK/next.$t"
  done
  for t in $ALL; do   # pre-seed so initial snapshots are non-trivial
    vals=$(awk 'BEGIN{for(i=1;i<=2000;i++){printf "%s(%d,%d,\"seed%d\")",(i>1?",":""),i,i*3,i%97}}')
    M "INSERT INTO $t (id,v,pad) VALUES $vals"; echo 2001 > "$WORK/next.$t"
  done
  say "pre-seeded 10 tables x 2000 rows"
fi

insert_batch() { # table n
  local t=$1 n=$2 a b vals
  a=$(cat "$WORK/next.$t"); b=$((a + n - 1))
  vals=$(awk -v a="$a" -v b="$b" 'BEGIN{for(i=a;i<=b;i++){printf "%s(%d,%d,\"p%d\")",(i>a?",":""),i,i*7,i%1000}}')
  M "INSERT INTO $t (id,v,pad) VALUES $vals" || say "loader: $t insert @$a failed"
  echo $((b + 1)) > "$WORK/next.$t"
}

loader() {  # ~230 rows/s across the spectrum; hot tables also purge (delete-load)
  local tick=0
  while [ ! -f "$STOP" ]; do
    [ -f "$PAUSE" ] && { sleep 1; continue; }
    tick=$((tick + 1))
    for t in $HOT;  do insert_batch "$t" 250
      M "UPDATE $t SET v=v+1 WHERE id % 17 = $((tick % 17)) AND id > $(( $(cat "$WORK/next.$t") - 5000 ))"; done
    for t in $WARM; do insert_batch "$t" 50; done
    [ $((tick % 6))  -eq 0 ] && for t in $COLD;   do insert_batch "$t" 3; done
    [ $((tick % 60)) -eq 0 ] && for t in $FROZEN; do insert_batch "$t" 1; done
    if [ $((tick % 12)) -eq 0 ]; then
      for t in $HOT; do local cut=$(( $(cat "$WORK/next.$t") - 60000 ))
        [ $cut -gt 2000 ] && M "DELETE FROM $t WHERE id < $cut AND id > 2000 LIMIT 80000"; done
    fi
    sleep 5
  done
  say "loader stopped"
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
    SELECT id,row_hash,'' AS f,-1::BIGINT AS p,'insert' AS op
    FROM read_parquet('$OUT/$t/snapshot/*.parquet') UNION ALL"
  if ls "$OUT/$t"/cdc-*.parquet >/dev/null 2>&1; then
    duckdb -noheader -csv -c "
      WITH uni AS ($snap_arm
        SELECT id,row_hash, json_extract_string(__pos,'\$.file') AS f,
               CAST(json_extract(__pos,'\$.pos') AS BIGINT) AS p, __op AS op
        FROM read_parquet('$OUT/$t/cdc-*.parquet')),
      r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY f DESC,p DESC) rn FROM uni)
      SELECT COUNT(*)||' '||COALESCE(bit_xor(CAST(concat('0x',substring(row_hash,1,15)) AS UBIGINT)),0)
      FROM r WHERE rn=1 AND op<>'delete'" 2>>"$LOG"
  else
    duckdb -noheader -csv -c "
      SELECT COUNT(*)||' '||COALESCE(bit_xor(CAST(concat('0x',substring(row_hash,1,15)) AS UBIGINT)),0)
      FROM read_parquet('$OUT/$t/snapshot/*.parquet')" 2>>"$LOG"
  fi
}

verify() {
  touch "$PAUSE"; sleep 7          # let the in-flight batch land, then quiesce
  local d1; d1=$(drain)
  local t
  for t in $ALL; do
    M "SELECT COUNT(*) FROM $t" > "$WORK/sc.$t"
    M "SELECT COALESCE(BIT_XOR(CONV(SUBSTRING(row_hash,1,15),16,10)),0) FROM $t" > "$WORK/sx.$t"
    echo $(( $(cat "$WORK/next.$t") - 1 )) > "$WORK/mx.$t"
  done
  local d2; d2=$(drain)            # must be 0: nothing ran between the two drains
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
  say "VERIFY d1=[$d1] d2=[$d2] =>$line"
  say "TOTAL: $verdict | parts=$(find "$OUT" -name 'cdc-*.parquet' | wc -l | tr -d ' ')"
}

[ "${SKIP_SETUP:-0}" = "1" ] || { say "=== pin + initial snapshots ==="
  "$R" run -c "$CFG" > "$WORK/d.out" 2>&1 && say "pin+snapshots ok" \
    || { say "PIN FAILED: $(tail -3 "$WORK/d.out")"; exit 1; }; }
loader & LP=$!; say "loader pid=$LP"

IFS=';'; for phase in $PHASES; do IFS=' '
  set -- $phase; mins=$1; reps=$2
  for rep in $(seq 1 "$reps"); do
    say "--- phase ${mins}min rep $rep/$reps ---"; sleep $((mins * 60)); verify
  done
done
touch "$STOP"; wait "$LP" 2>/dev/null
say "=== final verify after loader stop ==="; verify
for t in $ALL; do M "DROP TABLE IF EXISTS $t"; done
say "=== complete ==="
