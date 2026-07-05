#!/usr/bin/env bash
# CDC drain MEMORY baseline — all three engines, one big backlog, one drain.
#
# Purpose: capture the "before-fix" peak drain RSS vs event count for MySQL,
# PostgreSQL and SQL Server, so a later run (after the bounded-peek fix) is a
# like-for-like comparison. Pumps N row changes in MODERATE transactions
# (batch each ≤ BATCH rows) so a single giant transaction never confounds the
# result — the question is memory-vs-BACKLOG, not memory-vs-transaction.
#
# Hypothesis under test: MySQL streams the binlog (O(largest transaction) →
# flat RSS); PostgreSQL peeks the whole slot (O(total backlog)); SQL Server
# polls the change-table window (likely O(backlog) too).
#
# Usage:
#   RIVET_BIN=/tmp/rivet-cdc-bin/rivet N=1000000 dev/cdc_interval/baseline_mem.sh [pg|mysql|mssql|all]
#
# Writes per-engine drain out/err under $WORK; prints one summary line per engine.
set -uo pipefail

RIVET_BIN="${RIVET_BIN:-target/release/rivet}"
[ -x "$RIVET_BIN" ] || RIVET_BIN=target/debug/rivet
N="${N:-1000000}"
BATCH="${BATCH:-5000}"
WORK="${WORK:-/tmp/rivet-cdc-baseline}"
ONLY="${1:-all}"
PAD="p_$(printf '%*s' 38 '' | tr ' ' x)"   # ~40-char payload so events aren't trivially tiny
R="$RIVET_BIN"
mkdir -p "$WORK"
say() { echo "[$(date +%H:%M:%S)] $*"; }

PGC=rivet-postgres-cdc-1; MYC=rivet-mysql-cdc-1; MSC=rivet-mssql-cdc-1
PG()  { docker exec "$PGC" psql -U rivet -d rivet -tAc "$1" 2>/dev/null; }
MY()  { docker exec "$MYC" mysql -urivet -privet rivet -N -e "$1" 2>/dev/null; }
MS()  { docker exec "$MSC" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Rivet_Passw0rd!' -d rivet -h -1 -Q "$1" 2>/dev/null; }
# Numeric MSSQL query: SET NOCOUNT ON kills the "(N rows affected)" trailer, and
# we take the FIRST integer token — so a count never fuses with the rowcount line.
MSN() { docker exec "$MSC" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'Rivet_Passw0rd!' -d rivet -h -1 -Q "SET NOCOUNT ON; $1" 2>/dev/null | grep -Eo '[0-9]+' | head -1; }

# ── measured drain: "rows elapsedS rssMB" via /usr/bin/time -l (macOS, bytes) ──
drain() { local cfg=$1 t0 t1 rows rss; t0=$(date +%s)
  /usr/bin/time -l "$R" run -c "$cfg" > "$WORK/d.out" 2> "$WORK/d.err"
  t1=$(date +%s)
  rows=$(grep -E "^[[:space:]]+rows:" "$WORK/d.out" "$WORK/d.err" 2>/dev/null | head -1 | tr -dc '0-9')
  rss=$(grep -E "maximum resident set size" "$WORK/d.err" | awk '{print $1}')
  echo "${rows:-?} $((t1 - t0)) $(( ${rss:-0} / 1048576 ))"
}

baseline_pg() {
  say "PG: setup"
  PG "SELECT pg_drop_replication_slot('bl_pg') FROM pg_replication_slots WHERE slot_name='bl_pg'" >/dev/null
  PG "DROP TABLE IF EXISTS bl; CREATE TABLE bl (id bigint primary key, v bigint not null, pad varchar(64) not null)" >/dev/null
  rm -f "$WORK/pg.ckpt"; rm -rf "$WORK/pg_out"; mkdir -p "$WORK/pg_out"
  cat > "$WORK/pg.yaml" <<EOF
source: { type: postgres, url: "postgresql://rivet:rivet@127.0.0.1:5434/rivet" }
exports:
  - { name: bl, table: bl, mode: cdc, format: parquet, cdc: { checkpoint: "$WORK/pg.ckpt", slot: bl_pg, until_current: true, rollover: 50000 }, destination: { type: local, path: "$WORK/pg_out" } }
EOF
  "$R" run -c "$WORK/pg.yaml" >/dev/null 2>&1   # pin slot, drain 0
  say "PG: pump $N in ${BATCH}-row txns"
  PG "CREATE OR REPLACE PROCEDURE bl_pump() LANGUAGE plpgsql AS \$\$
      DECLARE i bigint := 0;
      BEGIN WHILE i < $N LOOP
        INSERT INTO bl SELECT g, g, '$PAD' FROM generate_series(i+1, LEAST(i+$BATCH,$N)) g;
        COMMIT; i := i + $BATCH; END LOOP; END \$\$;" >/dev/null
  PG "CALL bl_pump()" >/dev/null
  local wal; wal=$(PG "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint/1048576 FROM pg_replication_slots WHERE slot_name='bl_pg'")
  say "PG: drain (retained WAL=${wal}MB)"
  local d; d=$(drain "$WORK/pg.yaml")
  echo "RESULT postgres events=$(echo $d|awk '{print $1}') wall=$(echo $d|awk '{print $2}')s rss=$(echo $d|awk '{print $3}')MB retained_wal=${wal}MB"
  PG "SELECT pg_drop_replication_slot('bl_pg') FROM pg_replication_slots WHERE slot_name='bl_pg'" >/dev/null
  PG "DROP TABLE IF EXISTS bl" >/dev/null
}

baseline_mysql() {
  say "MySQL: setup"
  MY "DROP TABLE IF EXISTS bl; CREATE TABLE bl (id bigint primary key, v bigint not null, pad varchar(64) not null)"
  MY "DROP TABLE IF EXISTS nums; CREATE TABLE nums (n int primary key)"
  MY "SET SESSION cte_max_recursion_depth=1000000; INSERT INTO nums (n) WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n < $BATCH) SELECT n FROM s"
  rm -f "$WORK/my.ckpt"; rm -rf "$WORK/my_out"; mkdir -p "$WORK/my_out"
  cat > "$WORK/my.yaml" <<EOF
source: { type: mysql, url: "mysql://rivet:rivet@127.0.0.1:3307/rivet" }
exports:
  - { name: bl, table: bl, mode: cdc, format: parquet, cdc: { checkpoint: "$WORK/my.ckpt", server_id: 49222, until_current: true, rollover: 50000 }, destination: { type: local, path: "$WORK/my_out" } }
EOF
  "$R" run -c "$WORK/my.yaml" >/dev/null 2>&1   # pin binlog checkpoint at current, drain 0
  say "MySQL: pump $N in ${BATCH}-row txns"
  local i=0
  while [ "$i" -lt "$N" ]; do   # each INSERT is its own autocommit txn ⇒ largest txn = BATCH
    MY "INSERT INTO bl (id,v,pad) SELECT $i+n, $i+n, '$PAD' FROM nums WHERE $i+n <= $N"
    i=$((i + BATCH))
  done
  say "MySQL: drain"
  local d; d=$(drain "$WORK/my.yaml")
  echo "RESULT mysql events=$(echo $d|awk '{print $1}') wall=$(echo $d|awk '{print $2}')s rss=$(echo $d|awk '{print $3}')MB retained_wal=n/a"
  MY "DROP TABLE IF EXISTS bl, nums"
}

baseline_mssql() {
  say "MSSQL: setup + enable CDC"
  MS "IF OBJECT_ID('dbo.bl') IS NOT NULL DROP TABLE dbo.bl; CREATE TABLE dbo.bl (id bigint primary key, v bigint not null, pad varchar(64) not null);" >/dev/null
  MS "IF EXISTS (SELECT 1 FROM cdc.change_tables ct JOIN sys.tables t ON ct.source_object_id=t.object_id WHERE t.name='bl')
        EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='bl', @capture_instance='dbo_bl';" >/dev/null
  MS "EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='bl', @role_name=NULL, @capture_instance='dbo_bl', @supports_net_changes=0;" >/dev/null
  # Wait for the capture instance to come online (Agent).
  for _ in $(seq 1 20); do
    [ "$(MSN "SELECT COUNT(*) FROM cdc.change_tables ct JOIN sys.tables t ON ct.source_object_id=t.object_id WHERE t.name='bl'")" -ge 1 ] 2>/dev/null && break
    sleep 2
  done
  rm -f "$WORK/ms.ckpt"; rm -rf "$WORK/ms_out"; mkdir -p "$WORK/ms_out"
  cat > "$WORK/ms.yaml" <<EOF
source:
  type: mssql
  url: "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet"
  tls: { accept_invalid_certs: true }
exports:
  - { name: bl, table: bl, mode: cdc, format: parquet, cdc: { checkpoint: "$WORK/ms.ckpt", capture_instance: dbo_bl, until_current: true, rollover: 50000 }, destination: { type: local, path: "$WORK/ms_out" } }
EOF
  "$R" run -c "$WORK/ms.yaml" >/dev/null 2>&1   # pin checkpoint at current max LSN, drain 0
  # Baseline the change-table count BEFORE pumping — stale rows below the pinned
  # checkpoint LSN are never drained, but the Agent-extract wait must key on the
  # DELTA (before→after), not an absolute count, or it can fire early.
  local ct0; ct0=$(MSN "SELECT COUNT_BIG(*) FROM cdc.dbo_bl_CT"); ct0=${ct0:-0}
  say "MSSQL: pump $N (cross-join, ${BATCH}-row batches; CT baseline=$ct0)"
  local i=0
  while [ "$i" -lt "$N" ]; do
    MS "INSERT INTO dbo.bl (id,v,pad)
        SELECT TOP ($BATCH) $i + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
               $i + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '$PAD'
        FROM sys.all_columns a CROSS JOIN sys.all_columns b;" >/dev/null
    i=$((i + BATCH))
  done
  local target=$((ct0 + N))
  say "MSSQL: wait for Agent to extract to CT>=$target"
  for _ in $(seq 1 120); do
    local ct; ct=$(MSN "SELECT COUNT_BIG(*) FROM cdc.dbo_bl_CT")
    [ "${ct:-0}" -ge "$target" ] && { say "MSSQL: CT has ${ct} rows (delta $((ct - ct0)))"; break; }
    sleep 5
  done
  say "MSSQL: drain"
  local d; d=$(drain "$WORK/ms.yaml")
  echo "RESULT mssql events=$(echo $d|awk '{print $1}') wall=$(echo $d|awk '{print $2}')s rss=$(echo $d|awk '{print $3}')MB retained_wal=n/a"
  MS "EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='bl', @capture_instance='dbo_bl'; DROP TABLE IF EXISTS dbo.bl;" >/dev/null
}

say "=== CDC memory baseline: N=$N, batch=$BATCH, bin=$R, only=$ONLY ==="
case "$ONLY" in
  pg)     baseline_pg ;;
  mysql)  baseline_mysql ;;
  mssql)  baseline_mssql ;;
  all)    baseline_mysql; baseline_pg; baseline_mssql ;;
  *) echo "unknown target: $ONLY"; exit 1 ;;
esac
say "=== baseline complete ==="
