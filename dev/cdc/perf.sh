#!/usr/bin/env bash
# CDC capture-throughput matrix — one bounded backlog drain per engine.
#
# For each engine: pin the resume anchor, seed a backlog of N rows, then run ONE
# bounded (`until_current`) `mode: cdc` drain and measure wall / rows / rows-per-
# second / peak RSS. This is the BULK-BACKLOG profile (one drain of the whole
# backlog at the default-ish rollover) — NOT steady-state; for the steady-state
# memory-flatness contract use dev/cdc_interval/soak_all.sh.
#
#   RIVET_BIN=target/release/rivet dev/cdc/perf.sh              # 100k, all engines
#   N=500000 dev/cdc/perf.sh pg mysql                          # a subset
#
# Requires: docker compose --profile cdc up (postgres-cdc / mysql-cdc / mssql-cdc
# / mongo-rs), the mssql `rivet` DB created (dev/cdc/stand.sh up does this), and
# a release rivet for representative numbers.
set -uo pipefail

RIVET_BIN="${RIVET_BIN:-target/release/rivet}"
[ -x "$RIVET_BIN" ] || RIVET_BIN=target/debug/rivet
R="$RIVET_BIN"
N="${N:-100000}"
ENGINES=("$@"); [ ${#ENGINES[@]} -eq 0 ] && ENGINES=(pg mysql mssql mongo)
WORK="${WORK:-/tmp/rivet-cdc-perf}"; rm -rf "$WORK"; mkdir -p "$WORK"
PAD="$(printf '%*s' 48 '' | tr ' ' x)"

PGC=rivet-postgres-cdc-1; MYC=rivet-mysql-cdc-1; MSC=rivet-mssql-cdc-1; MOC=rivet-mongo-rs-1
PG() { docker exec "$PGC" psql -U rivet -d rivet -tAc "$1" 2>/dev/null; }
MY() { docker exec "$MYC" mysql -urivet -privet rivet -N -e "$1" 2>/dev/null; }
MS() { docker exec "$MSC" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Rivet_Passw0rd!' -d rivet -h -1 -Q "$1" 2>/dev/null; }
MSN(){ MS "SET NOCOUNT ON; $1" | grep -Eo '[0-9]+' | head -1; }
MO() { docker exec "$MOC" mongosh --quiet --port 27017 perfdb --eval "$1" 2>/dev/null; }

cfg_for() { # engine -> writes $WORK/$engine.yaml, returns via echo
  local e=$1 ck="$WORK/$e.ckpt" out="$WORK/$e.out" cfg="$WORK/$e.yaml"
  rm -rf "$out" "$ck"; mkdir -p "$out"
  case "$e" in
    pg) cat > "$cfg" <<EOF
source: { type: postgres, url: "postgresql://rivet:rivet@127.0.0.1:5434/rivet" }
exports:
  - { name: pf, table: pf, mode: cdc, format: parquet, cdc: { checkpoint: "$ck", slot: perf_pg, until_current: true }, destination: { type: local, path: "$out" } }
EOF
    ;;
    mysql) cat > "$cfg" <<EOF
source: { type: mysql, url: "mysql://rivet:rivet@127.0.0.1:3307/rivet" }
exports:
  - { name: pf, table: pf, mode: cdc, format: parquet, cdc: { checkpoint: "$ck", server_id: 9911, until_current: true }, destination: { type: local, path: "$out" } }
EOF
    ;;
    mssql) cat > "$cfg" <<EOF
source:
  type: mssql
  url: "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet"
  tls: { accept_invalid_certs: true }
exports:
  - { name: pf, table: pf, mode: cdc, format: parquet, cdc: { checkpoint: "$ck", capture_instance: dbo_pf, until_current: true }, destination: { type: local, path: "$out" } }
EOF
    ;;
    mongo) cat > "$cfg" <<EOF
source: { type: mongo, url: "mongodb://127.0.0.1:27018/perfdb?directConnection=true&serverSelectionTimeoutMS=5000" }
exports:
  - { name: pf, table: pf, mode: cdc, format: parquet, cdc: { checkpoint: "$ck", until_current: true }, destination: { type: local, path: "$out" } }
EOF
    ;;
  esac
  echo "$cfg"
}

setup() { # engine — create the captured table/collection + pin the anchor
  local e=$1
  case "$e" in
    pg)
      PG "SELECT pg_drop_replication_slot('perf_pg') FROM pg_replication_slots WHERE slot_name='perf_pg'" >/dev/null
      PG "DROP TABLE IF EXISTS pf; CREATE TABLE pf(id bigint primary key, v int, pad varchar(64))" >/dev/null
      ;;
    mysql) MY "DROP TABLE IF EXISTS pf; CREATE TABLE pf(id bigint primary key, v int, pad varchar(64))" ;;
    mssql)
      MS "IF EXISTS(SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_pf') EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='pf',@capture_instance='dbo_pf';" >/dev/null
      MS "IF OBJECT_ID('dbo.pf') IS NOT NULL DROP TABLE dbo.pf; CREATE TABLE dbo.pf(id bigint primary key, v int, pad varchar(64));" >/dev/null
      MS "IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet')=0 EXEC sys.sp_cdc_enable_db;" >/dev/null
      MS "EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='pf',@role_name=NULL,@capture_instance='dbo_pf',@supports_net_changes=0;" >/dev/null
      ;;
    mongo) MO "db.pf.drop()" >/dev/null ;;
  esac
  # Pin the anchor + drain 0 (the first run is the resume anchor for the backlog).
  "$R" run -c "$(cfg_for "$e")" >/dev/null 2>&1 || true
}

seed() { # engine N
  local e=$1 n=$2
  case "$e" in
    pg)    PG "INSERT INTO pf SELECT g, g%1000, '$PAD' FROM generate_series(1,$n) g" >/dev/null ;;
    mysql) MY "SET SESSION cte_max_recursion_depth=100000000; INSERT INTO pf SELECT g, g%1000, '$PAD' FROM (WITH RECURSIVE s(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM s WHERE g<$n) SELECT g FROM s) q" ;;
    mssql) MS "SET NOCOUNT ON; WITH n AS (SELECT 1 g UNION ALL SELECT g+1 FROM n WHERE g<$n) INSERT INTO dbo.pf SELECT g, g%1000, '$PAD' FROM n OPTION (MAXRECURSION 0);" >/dev/null ;;
    mongo) MO "var d=[];for(var i=1;i<=$n;i++){d.push({_id:i,v:i%1000,pad:'$PAD'});if(d.length===5000){db.pf.insertMany(d);d.length=0;}}if(d.length)db.pf.insertMany(d)" >/dev/null ;;
  esac
}

mssql_catchup() { # wait for the Agent to extract N rows into the change table
  local n=$1
  for _ in $(seq 1 120); do [ "$(MSN "SELECT COUNT_BIG(*) FROM cdc.dbo_pf_CT")" -ge "$n" ] 2>/dev/null && return; sleep 2; done
}

drained_rows() { # engine — distinct captured ids from the parquet
  local e=$1 out="$WORK/$e.out" key=id; [ "$e" = mongo ] && key='"_id"'
  ls "$out"/cdc-*.parquet >/dev/null 2>&1 || { echo 0; return; }
  duckdb -noheader -csv -c "SELECT COUNT(DISTINCT $key) FROM read_parquet('$out/cdc-*.parquet') WHERE __op='insert'" 2>/dev/null
}

declare -a ROWS_OUT
for e in "${ENGINES[@]}"; do
  echo "[$e] setup + anchor..."
  setup "$e"
  echo "[$e] seeding $N rows..."
  seed "$e" "$N"
  [ "$e" = mssql ] && { echo "[$e] waiting for Agent capture..."; mssql_catchup "$N"; }
  echo "[$e] timed bounded drain..."
  cfg="$WORK/$e.yaml"
  /usr/bin/time -l "$R" run -c "$cfg" >/dev/null 2>"$WORK/$e.time" || cat "$WORK/$e.time"
  REAL=$(grep -E "real" "$WORK/$e.time" | awk '{print $1}')
  RSS=$(grep "maximum resident" "$WORK/$e.time" | awk '{print $1}')
  DR=$(drained_rows "$e")
  RPS=$(awk "BEGIN{printf \"%.0f\", ${DR:-0}/(${REAL:-1}+0.0001)}")
  RSSMB=$(( ${RSS:-0} / 1048576 ))
  ROWS_OUT+=("$(printf '%-10s %-9s %-8s %-10s %-8s' "$e" "$DR" "${REAL}s" "$RPS" "${RSSMB}MB")")
  # cleanup
  case "$e" in
    pg) PG "SELECT pg_drop_replication_slot('perf_pg') FROM pg_replication_slots WHERE slot_name='perf_pg'" >/dev/null; PG "DROP TABLE IF EXISTS pf" >/dev/null ;;
    mysql) MY "DROP TABLE IF EXISTS pf" ;;
    mssql) MS "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='pf',@capture_instance='dbo_pf'; DROP TABLE IF EXISTS dbo.pf;" >/dev/null ;;
    mongo) MO "db.pf.drop()" >/dev/null ;;
  esac
done

echo
echo "=== CDC capture-throughput (bulk backlog of $N rows, $(basename "$R")) ==="
printf '%-10s %-9s %-8s %-10s %-8s\n' engine rows wall rows/s peakRSS
for r in "${ROWS_OUT[@]}"; do echo "$r"; done
