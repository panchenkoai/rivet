#!/usr/bin/env bash
# CDC drain-interval MEMORY soak — one engine, growing intervals, bounded drain.
#
# Post-fix validation: under continuous churn, drain at growing intervals
# (default 10/20/30/60/120 min) and assert (a) peak drain RSS stays FLAT as the
# per-interval backlog grows 12× (the bounded-peek fix — a leak or an O(backlog)
# regression shows as RSS tracking the interval), and (b) every inserted row is
# captured (source COUNT(*) == distinct ids in the drained parts).
#
# Run one engine per invocation (parallelise across engines from the caller):
#   RIVET_BIN=/tmp/rivet-cdc-bin/rivet PHASES="10 20 30 60 120" \
#     dev/cdc_interval/soak_all.sh pg     > pg.soak.log 2>&1 &
#
# Requires: docker compose --profile cdc up, and `duckdb` on PATH.
set -uo pipefail

RIVET_BIN="${RIVET_BIN:-target/release/rivet}"
[ -x "$RIVET_BIN" ] || RIVET_BIN=target/debug/rivet
ENGINE="${1:?usage: soak_all.sh <pg|mysql|mssql|mongo>}"
PHASES="${PHASES:-10 20 30 60 120}"   # minutes, space-separated
BATCH="${BATCH:-2000}"                # rows per churn tick
TICK="${TICK:-5}"                     # seconds between churn ticks (~24k rows/min)
WORK="${WORK:-/tmp/rivet-cdc-soak/$ENGINE}"
R="$RIVET_BIN"
PAD="s_$(printf '%*s' 38 '' | tr ' ' x)"
mkdir -p "$WORK"
STOP="$WORK/churn.stop"; PAUSE="$WORK/churn.pause"; rm -f "$STOP" "$PAUSE"
LOG="$WORK/soak.log"
say() { echo "[$(date +%H:%M:%S)] [$ENGINE] $*" | tee -a "$LOG"; }

PGC=rivet-postgres-cdc-1; MYC=rivet-mysql-cdc-1; MSC=rivet-mssql-cdc-1; MOC=rivet-mongo-rs-1
PG()  { docker exec "$PGC" psql -U rivet -d rivet -tAc "$1" 2>/dev/null; }
MY()  { docker exec "$MYC" mysql -urivet -privet rivet -N -e "$1" 2>/dev/null; }
MS()  { docker exec "$MSC" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Rivet_Passw0rd!' -d rivet -h -1 -Q "$1" 2>/dev/null; }
MSN() { MS "SET NOCOUNT ON; $1" | grep -Eo '[0-9]+' | head -1; }
# Mongo: the whole db `soakdb` is watched; `sk` is the collection, `_id` the key.
MO()  { docker exec "$MOC" mongosh --quiet --port 27017 soakdb --eval "$1" 2>/dev/null; }

CFG="$WORK/cdc.yaml"; OUT="$WORK/out"; NEXT="$WORK/next"

setup() {
  rm -rf "$OUT"; mkdir -p "$OUT"; rm -f "$WORK/cdc.ckpt"; echo 1 > "$NEXT"
  case "$ENGINE" in
    pg)
      PG "SELECT pg_drop_replication_slot('soak_pg') FROM pg_replication_slots WHERE slot_name='soak_pg'" >/dev/null
      PG "DROP TABLE IF EXISTS sk; CREATE TABLE sk (id bigint primary key, v bigint not null, pad varchar(64) not null)" >/dev/null
      cat > "$CFG" <<EOF
source: { type: postgres, url: "postgresql://rivet:rivet@127.0.0.1:5434/rivet" }
exports:
  - { name: sk, table: sk, mode: cdc, format: parquet, cdc: { checkpoint: "$WORK/cdc.ckpt", slot: soak_pg, until_current: true, rollover: 50000 }, destination: { type: local, path: "$OUT" } }
EOF
      ;;
    mysql)
      MY "DROP TABLE IF EXISTS sk; CREATE TABLE sk (id bigint primary key, v bigint not null, pad varchar(64) not null)"
      cat > "$CFG" <<EOF
source: { type: mysql, url: "mysql://rivet:rivet@127.0.0.1:3307/rivet" }
exports:
  - { name: sk, table: sk, mode: cdc, format: parquet, cdc: { checkpoint: "$WORK/cdc.ckpt", server_id: 49333, until_current: true, rollover: 50000 }, destination: { type: local, path: "$OUT" } }
EOF
      ;;
    mssql)
      MS "IF OBJECT_ID('dbo.sk') IS NOT NULL DROP TABLE dbo.sk; CREATE TABLE dbo.sk (id bigint primary key, v bigint not null, pad varchar(64) not null);" >/dev/null
      MS "IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_sk') EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='sk',@capture_instance='dbo_sk';" >/dev/null
      MS "EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='sk',@role_name=NULL,@capture_instance='dbo_sk',@supports_net_changes=0;" >/dev/null
      for _ in $(seq 1 20); do [ "$(MSN "SELECT COUNT(*) FROM cdc.change_tables WHERE capture_instance='dbo_sk'")" -ge 1 ] 2>/dev/null && break; sleep 2; done
      cat > "$CFG" <<EOF
source:
  type: mssql
  url: "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet"
  tls: { accept_invalid_certs: true }
exports:
  - { name: sk, table: sk, mode: cdc, format: parquet, cdc: { checkpoint: "$WORK/cdc.ckpt", capture_instance: dbo_sk, until_current: true, rollover: 50000 }, destination: { type: local, path: "$OUT" } }
EOF
      ;;
    mongo)
      MO "db.sk.drop()" >/dev/null
      cat > "$CFG" <<EOF
source: { type: mongo, url: "mongodb://127.0.0.1:27018/soakdb?directConnection=true&serverSelectionTimeoutMS=5000" }
exports:
  - { name: sk, table: sk, mode: cdc, format: parquet, cdc: { checkpoint: "$WORK/cdc.ckpt", until_current: true, rollover: 50000 }, destination: { type: local, path: "$OUT" } }
EOF
      ;;
  esac
  "$R" run -c "$CFG" >/dev/null 2>&1   # pin the anchor, drain 0
}

insert_rows() { # a .. b
  local a=$1 b=$2
  case "$ENGINE" in
    pg)    PG "INSERT INTO sk SELECT g,g,'$PAD' FROM generate_series($a,$b) g" >/dev/null ;;
    mysql) MY "SET SESSION cte_max_recursion_depth=1000000; INSERT INTO sk (id,v,pad) SELECT n,n,'$PAD' FROM (WITH RECURSIVE s(n) AS (SELECT $a UNION ALL SELECT n+1 FROM s WHERE n < $b) SELECT n FROM s) q" ;;
    mssql) MS "SET NOCOUNT ON; INSERT INTO dbo.sk (id,v,pad) SELECT TOP ($((b-a+1))) $((a-1))+ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), $((a-1))+ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '$PAD' FROM sys.all_columns x CROSS JOIN sys.all_columns y;" >/dev/null ;;
    mongo) MO "var d=[]; for(var i=$a;i<=$b;i++){d.push({_id:i,v:i,pad:'$PAD'}); if(d.length===5000){db.sk.insertMany(d);d.length=0;}} if(d.length)db.sk.insertMany(d)" >/dev/null ;;
  esac
}

churner() {
  while [ ! -f "$STOP" ]; do
    [ -f "$PAUSE" ] && { sleep 1; continue; }
    local a b; a=$(cat "$NEXT"); b=$((a + BATCH - 1))
    insert_rows "$a" "$b"; echo $((b + 1)) > "$NEXT"
    sleep "$TICK"
  done
}

src_count() {
  case "$ENGINE" in
    pg)    PG "SELECT COUNT(*) FROM sk" ;;
    mysql) MY "SELECT COUNT(*) FROM sk" ;;
    mssql) MS "SET NOCOUNT ON; SELECT COUNT_BIG(*) FROM dbo.sk" | grep -Eo '[0-9]+' | head -1 ;;
    mongo) MO "print(db.sk.countDocuments())" | grep -Eo '[0-9]+' | head -1 ;;
  esac
}

# MSSQL only: wait until the Agent has extracted every inserted row into the CT.
mssql_agent_catchup() {
  local want; want=$(src_count)
  for _ in $(seq 1 60); do
    [ "$(MSN "SELECT COUNT_BIG(*) FROM cdc.dbo_sk_CT")" -ge "$want" ] 2>/dev/null && return
    sleep 3
  done
}

drain_measure() { # -> "events rssMB"
  /usr/bin/time -l "$R" run -c "$CFG" > "$WORK/d.out" 2> "$WORK/d.err"
  local rows rss
  rows=$(grep -E "^[[:space:]]+rows:" "$WORK/d.out" "$WORK/d.err" 2>/dev/null | head -1 | tr -dc '0-9')
  rss=$(grep -E "maximum resident set size" "$WORK/d.err" | awk '{print $1}')
  echo "${rows:-?} $(( ${rss:-0} / 1048576 ))"
}

dest_distinct() {
  # A single-table export writes parts to the destination path directly. The key
  # column is `id` for the SQL engines and `_id` for Mongo's JSON-blob model.
  ls "$OUT"/cdc-*.parquet >/dev/null 2>&1 || { echo 0; return; }
  local key=id; [ "$ENGINE" = mongo ] && key='"_id"'
  duckdb -noheader -csv -c "SELECT COUNT(DISTINCT $key) FROM read_parquet('$OUT/cdc-*.parquet') WHERE __op='insert'" 2>/dev/null
}

say "=== soak start: phases='$PHASES' min, churn ${BATCH}/${TICK}s, bin=$R ==="
setup
churner & CH=$!
say "churner pid=$CH"
FIRST_RSS=""; LAST_RSS=""; VERDICT=OK
for mins in $PHASES; do
  say "--- interval ${mins}min (accumulating backlog) ---"
  sleep $((mins * 60))
  touch "$PAUSE"; sleep 3                 # quiesce so source count == what will drain
  [ "$ENGINE" = mssql ] && mssql_agent_catchup
  read -r EV RSS <<<"$(drain_measure)"
  SC=$(src_count); DD=$(dest_distinct)
  local_ok=OK; [ "$SC" = "$DD" ] || local_ok="COUNT($SC!=$DD)"
  [ "$local_ok" = OK ] || VERDICT=FAIL
  rm -f "$PAUSE"
  [ -z "$FIRST_RSS" ] && FIRST_RSS=$RSS; LAST_RSS=$RSS
  say "RESULT interval=${mins}min events=${EV} rss=${RSS}MB src=${SC} dest_distinct=${DD} => ${local_ok}"
done
touch "$STOP"; wait "$CH" 2>/dev/null

# RSS-trend leak gate: the last (largest-backlog) drain must not exceed 1.5× the
# first — a leak or an O(backlog) regression blows well past this.
TREND=OK
[ -n "$FIRST_RSS" ] && [ "$LAST_RSS" -gt $(( FIRST_RSS * 3 / 2 )) ] && { TREND="RSS-TREND-FAIL"; VERDICT=FAIL; }
say "=== soak done: verdict=$VERDICT | RSS first=${FIRST_RSS}MB last=${LAST_RSS}MB ($TREND) ==="

# Cleanup
case "$ENGINE" in
  pg)    PG "SELECT pg_drop_replication_slot('soak_pg') FROM pg_replication_slots WHERE slot_name='soak_pg'" >/dev/null; PG "DROP TABLE IF EXISTS sk" >/dev/null ;;
  mysql) MY "DROP TABLE IF EXISTS sk" ;;
  mssql) MS "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='sk',@capture_instance='dbo_sk'; DROP TABLE IF EXISTS dbo.sk;" >/dev/null ;;
  mongo) MO "db.sk.drop()" >/dev/null ;;
esac
