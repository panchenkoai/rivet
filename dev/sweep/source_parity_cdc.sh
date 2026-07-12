#!/usr/bin/env bash
# Independent source-parity sweep through the CDC path (pg slot / mysql binlog /
# mssql capture job). Same oracle as source_parity_sweep.sh but the CDC parquet is
# DEDUPED to current-state (latest change per id by __pos,__seq, deletes excluded)
# before comparing against the source. This is a SEPARATE value-decode path from
# batch (per-engine build_column) and is where the uuid->null field bug lived.
#
# Requires the `cdc` compose profile up (postgres-cdc:5434 / mysql-cdc:3307 /
# mssql-cdc:1434, `rivet` DB seeded on mssql-cdc), duckdb CLI, built rivet.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
RIVET=${RIVET:-target/debug/rivet}
[ -x "$RIVET" ] || { echo "rivet not at $RIVET"; exit 2; }
command -v duckdb >/dev/null || { echo "duckdb CLI not on PATH"; exit 2; }

TOTAL=0; MATCH=0; FAILED=0
DC() { docker compose exec -T "$@"; }
norm() { printf '%s' "$1" | tr -d '[:space:]' | sed 's/0*$//;s/\.$//'; }
chk() { TOTAL=$((TOTAL+1))
  if [ "$(norm "$2")" = "$(norm "$3")" ]; then MATCH=$((MATCH+1)); printf "  %-14s %-22s ok\n" "$1" "$2"
  else FAILED=$((FAILED+1)); printf "  %-14s src=%s dest=%s  *** MISMATCH ***\n" "$1" "$2" "$3"; fi; }
ddv() { duckdb -noheader -list -c "$1" 2>/dev/null; }
COLS="id k maybe_null amount label uid ts payload d"; NUMS=" id k maybe_null amount "
# dedup-to-current-state view over a CDC parquet glob
view() { echo "(SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY __pos DESC,__seq DESC) rn FROM read_parquet('$1')) WHERE rn=1 AND __op <> 'delete')"; }
compare() { local statf=$1 sumf=$2 v=$3 col
  for col in $COLS; do
    chk "$col" "$($statf "$col")" "$(ddv "SELECT count(*)||'/'||count($col)||'/'||count(distinct $col) FROM $v")"
    [[ "$NUMS" == *" $col "* ]] && chk "$col/sum" "$($sumf "$col")" "$(ddv "SELECT COALESCE(SUM($col),0)::VARCHAR FROM $v")"
  done; }
cdc_run() { printf 'source: { type: %s, url: "%s" }\nexports:\n  - name: s\n    table: sweep_src\n    mode: cdc\n    format: parquet\n    cdc: { %s }\n    destination: { type: local, path: "%s" }\n' "$1" "$2" "$3" "$4" > "$4.yaml"
  "$RIVET" run --config "$4.yaml" >/dev/null 2>&1 || { echo "  rivet FAILED"; FAILED=$((FAILED+1)); }; }

OUT=$(mktemp -d)
echo "###### rivet source-parity sweep (CDC) ######"

# ---- Postgres (logical slot) -------------------------------------------------
PG() { DC postgres-cdc psql -U rivet -d rivet -tAc "$1"; }
pg_stat() { PG "SELECT count(*)||'/'||count($1)||'/'||count(distinct $1) FROM sweep_src"; }
pg_sum()  { PG "SELECT COALESCE(SUM($1),0) FROM sweep_src"; }
echo "== CDC postgres =="
PG "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, amount DECIMAL(38,10), label TEXT, uid UUID, ts TIMESTAMP, payload JSONB, d DATE);" >/dev/null
PG "SELECT pg_drop_replication_slot('sweep_slot') FROM pg_replication_slots WHERE slot_name='sweep_slot';" >/dev/null 2>&1
PG "SELECT pg_create_logical_replication_slot('sweep_slot','test_decoding');" >/dev/null
PG "INSERT INTO sweep_src SELECT g,g%50,CASE WHEN g%3=0 THEN NULL ELSE g END,(g::numeric*0.0000000001),CASE WHEN g%7=0 THEN NULL ELSE 'rôw_😀_'||(g%100) END,gen_random_uuid(),timestamp '2020-01-01'+(g||' minutes')::interval,json_build_object('k',g%50,'v',g)::jsonb,date '2020-01-01'+g FROM generate_series(1,1000) g;" >/dev/null
cdc_run postgres "postgresql://rivet:rivet@127.0.0.1:5434/rivet" "slot: sweep_slot, until_current: true" "$OUT/pg"
compare pg_stat pg_sum "$(view "$OUT/pg/*.parquet")"
PG "SELECT pg_drop_replication_slot('sweep_slot') FROM pg_replication_slots WHERE slot_name='sweep_slot';" >/dev/null 2>&1
PG "DROP TABLE sweep_src;" >/dev/null

# ---- MySQL (binlog + checkpoint pin) -----------------------------------------
MY() { DC mysql-cdc mysql -urivet -privet rivet -N -e "$1" 2>/dev/null; }
my_stat() { MY "SELECT CONCAT(count(*),'/',count($1),'/',count(distinct $1)) FROM sweep_src"; }
my_sum()  { MY "SELECT COALESCE(SUM($1),0) FROM sweep_src"; }
echo "== CDC mysql =="
MY "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, amount DECIMAL(38,10), label VARCHAR(60), uid CHAR(36), ts DATETIME, payload JSON, d DATE);"
read -r FILE POS _ < <(MY "SHOW MASTER STATUS")
echo "{\"file\":\"$FILE\",\"pos\":$POS}" > "$OUT/myckpt"
MY "SET SESSION cte_max_recursion_depth=4000; INSERT INTO sweep_src WITH RECURSIVE seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq WHERE g<1000) SELECT g,g%50,IF(g%3=0,NULL,g),CAST(g AS DECIMAL(38,10))*0.0000000001,IF(g%7=0,NULL,CONCAT('rôw_😀_',g%100)),UUID(),TIMESTAMP('2020-01-01')+INTERVAL g MINUTE,JSON_OBJECT('k',g%50,'v',g),DATE('2020-01-01')+INTERVAL g DAY FROM seq;"
cdc_run mysql "mysql://rivet:rivet@127.0.0.1:3307/rivet" "until_current: true, checkpoint: \"$OUT/myckpt\", server_id: 47523" "$OUT/my"
compare my_stat my_sum "$(view "$OUT/my/*.parquet")"
MY "DROP TABLE sweep_src;"

# ---- SQL Server (async capture job) ------------------------------------------
MS() { DC mssql-cdc /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -C -d rivet -h-1 -W -Q "SET NOCOUNT ON; $1" 2>/dev/null; }
ms_stat() { MS "SELECT CAST(count(*) AS VARCHAR)+'/'+CAST(count($1) AS VARCHAR)+'/'+CAST(count(distinct $1) AS VARCHAR) FROM dbo.sweep_src"; }
ms_sum()  { MS "SELECT CAST(COALESCE(SUM($1),0) AS VARCHAR(64)) FROM dbo.sweep_src"; }
echo "== CDC mssql =="
MS "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) EXEC sys.sp_cdc_enable_db;"
MS "IF OBJECT_ID('dbo.sweep_src','U') IS NOT NULL DROP TABLE dbo.sweep_src; CREATE TABLE dbo.sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, amount DECIMAL(38,10), label NVARCHAR(60), uid UNIQUEIDENTIFIER, ts DATETIME2, payload NVARCHAR(200), d DATE);"
MS "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'sweep_src', @role_name=NULL, @capture_instance=N'dbo_sweep_src';"
MS ";WITH seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq WHERE g<1000) INSERT INTO dbo.sweep_src SELECT g,g%50,CASE WHEN g%3=0 THEN NULL ELSE g END,CAST(g*0.0000000001 AS DECIMAL(38,10)),CASE WHEN g%7=0 THEN NULL ELSE N'rôw_'+CAST(g%100 AS NVARCHAR(10)) END,NEWID(),DATEADD(MINUTE,g,CAST('2020-01-01' AS DATETIME2)),N'{\"k\":'+CAST(g%50 AS NVARCHAR(10))+N',\"v\":'+CAST(g AS NVARCHAR(10))+N'}',DATEADD(DAY,g,CAST('2020-01-01' AS DATE)) FROM seq OPTION (MAXRECURSION 0);"
for _ in $(seq 1 60); do n=$(MS "SELECT COUNT(*) FROM cdc.dbo_sweep_src_CT" | tr -d '[:space:]'); [ "${n:-0}" -ge 1000 ] 2>/dev/null && break; sleep 0.5; done
cdc_run mssql "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet" "capture_instance: dbo_sweep_src, checkpoint: \"$OUT/msckpt\"" "$OUT/ms"
compare ms_stat ms_sum "$(view "$OUT/ms/*.parquet")"
MS "EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'sweep_src', @capture_instance=N'dbo_sweep_src';" >/dev/null 2>&1
MS "IF OBJECT_ID('dbo.sweep_src','U') IS NOT NULL DROP TABLE dbo.sweep_src;"

rm -rf "$OUT"
echo "#############################################"
echo "CDC: $MATCH/$TOTAL independent checks matched the source ($FAILED failed)"
[ "$FAILED" -eq 0 ] || { echo "SILENT-CORRUPTION DETECTED"; exit 1; }
