#!/usr/bin/env bash
# Independent source-parity sweep — the strongest oracle rivet has: it does NOT
# trust rivet's own counters. For each engine it seeds a hostile type fixture,
# exports it with rivet (batch AND CDC), then compares a per-column profile
# (row count / non-null / distinct / sum) computed independently from the SOURCE
# (direct DB query) against the DESTINATION parquet read by DuckDB. Any mismatch
# is a silent-corruption bug and fails the run (exit 1).
#
# Permanent form of the sweep that confirmed the uuid->null and part-name-clobber
# classes are dead. Catches: row loss, null injection, distinct collapse, decimal
# precision loss. (Uniform value shift / tz is covered by the dedicated non-UTC
# tests; see live_cdc.rs.)
#
# Requires: docker compose stack up (batch postgres/mysql/mssql + cdc profile
# postgres-cdc/mysql-cdc/mssql-cdc, `rivet` DB seeded on mssql-cdc), `duckdb` CLI
# on PATH, and a built rivet at $RIVET (default target/debug/rivet).
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1
RIVET=${RIVET:-target/debug/rivet}
[ -x "$RIVET" ] || { echo "rivet not at $RIVET (cargo build --bin rivet, or set RIVET=)"; exit 2; }
command -v duckdb >/dev/null || { echo "duckdb CLI not on PATH"; exit 2; }

TOTAL=0; MATCH=0; FAILED=0
DC() { docker compose exec -T "$@"; }
norm() { printf '%s' "$1" | tr -d '[:space:]' | sed 's/0*$//;s/\.$//'; }
chk() { TOTAL=$((TOTAL+1))
  if [ "$(norm "$2")" = "$(norm "$3")" ]; then MATCH=$((MATCH+1)); printf "  %-14s %-22s ok\n" "$1" "$2"
  else FAILED=$((FAILED+1)); printf "  %-14s src=%s dest=%s  *** MISMATCH ***\n" "$1" "$2" "$3"; fi; }
ddv() { duckdb -noheader -list -c "$1" 2>/dev/null; }

COLS="id k maybe_null amount label uid ts payload d"
NUMS=" id k maybe_null amount "

# compare <stat_fn> <sum_fn> <view>   — stat_fn/sum_fn take a column, echo the value
compare() { local statf=$1 sumf=$2 view=$3 col
  for col in $COLS; do
    chk "$col" "$($statf "$col")" "$(ddv "SELECT count(*)||'/'||count($col)||'/'||count(distinct $col) FROM $view")"
    [[ "$NUMS" == *" $col "* ]] && chk "$col/sum" "$($sumf "$col")" "$(ddv "SELECT COALESCE(SUM($col),0)::VARCHAR FROM $view")"
  done
}
export_parquet() { # type url query out
  printf 'source: { type: %s, url: "%s" }\nexports:\n  - name: s\n    query: "%s"\n    mode: full\n    format: parquet\n    destination: { type: local, path: "%s" }\n' "$1" "$2" "$3" "$4" > "$4.yaml"
  "$RIVET" run --config "$4.yaml" >/dev/null 2>&1 || { echo "  rivet FAILED"; FAILED=$((FAILED+1)); }
}

# ---- Postgres ----------------------------------------------------------------
PG() { DC postgres psql -U rivet -d rivet -tAc "$1"; }
pg_stat() { PG "SELECT count(*)||'/'||count($1)||'/'||count(distinct $1) FROM sweep_src"; }
pg_sum()  { PG "SELECT COALESCE(SUM($1),0) FROM sweep_src"; }
pg_seed() { PG "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, amount DECIMAL(38,10), label TEXT, uid UUID, ts TIMESTAMP, payload JSONB, d DATE);
INSERT INTO sweep_src SELECT g, g%50, CASE WHEN g%3=0 THEN NULL ELSE g END, (g::numeric*0.0000000001), CASE WHEN g%7=0 THEN NULL ELSE 'rôw_😀_'||(g%100) END, gen_random_uuid(), timestamp '2020-01-01'+(g||' minutes')::interval, json_build_object('k',g%50,'v',g)::jsonb, date '2020-01-01'+g FROM generate_series(1,1000) g;" >/dev/null; }

# ---- MySQL -------------------------------------------------------------------
MY() { DC mysql mysql -urivet -privet rivet -N -e "$1" 2>/dev/null; }
my_stat() { MY "SELECT CONCAT(count(*),'/',count($1),'/',count(distinct $1)) FROM sweep_src"; }
my_sum()  { MY "SELECT COALESCE(SUM($1),0) FROM sweep_src"; }
my_seed() { MY "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, amount DECIMAL(38,10), label VARCHAR(60), uid CHAR(36), ts DATETIME, payload JSON, d DATE);
SET SESSION cte_max_recursion_depth=4000;
INSERT INTO sweep_src WITH RECURSIVE seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq WHERE g<1000) SELECT g,g%50,IF(g%3=0,NULL,g),CAST(g AS DECIMAL(38,10))*0.0000000001,IF(g%7=0,NULL,CONCAT('rôw_😀_',g%100)),UUID(),TIMESTAMP('2020-01-01')+INTERVAL g MINUTE,JSON_OBJECT('k',g%50,'v',g),DATE('2020-01-01')+INTERVAL g DAY FROM seq;"; }

# ---- SQL Server --------------------------------------------------------------
MS() { DC mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -C -d rivet -h-1 -W -Q "SET NOCOUNT ON; $1" 2>/dev/null; }
ms_stat() { MS "SELECT CAST(count(*) AS VARCHAR)+'/'+CAST(count($1) AS VARCHAR)+'/'+CAST(count(distinct $1) AS VARCHAR) FROM dbo.sweep_src"; }
ms_sum()  { MS "SELECT CAST(COALESCE(SUM($1),0) AS VARCHAR(64)) FROM dbo.sweep_src"; }
ms_seed() { MS "IF OBJECT_ID('dbo.sweep_src','U') IS NOT NULL DROP TABLE dbo.sweep_src; CREATE TABLE dbo.sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, amount DECIMAL(38,10), label NVARCHAR(60), uid UNIQUEIDENTIFIER, ts DATETIME2, payload NVARCHAR(200), d DATE);
;WITH seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq WHERE g<1000) INSERT INTO dbo.sweep_src SELECT g,g%50,CASE WHEN g%3=0 THEN NULL ELSE g END,CAST(g*0.0000000001 AS DECIMAL(38,10)),CASE WHEN g%7=0 THEN NULL ELSE N'rôw_'+CAST(g%100 AS NVARCHAR(10)) END,NEWID(),DATEADD(MINUTE,g,CAST('2020-01-01' AS DATETIME2)),N'{\"k\":'+CAST(g%50 AS NVARCHAR(10))+N',\"v\":'+CAST(g AS NVARCHAR(10))+N'}',DATEADD(DAY,g,CAST('2020-01-01' AS DATE)) FROM seq OPTION (MAXRECURSION 0);"; }

echo "###### rivet source-parity sweep (batch) ######"
OUT=$(mktemp -d)
echo "== BATCH postgres =="; pg_seed
export_parquet postgres "postgresql://rivet:rivet@127.0.0.1:5432/rivet" "SELECT * FROM sweep_src" "$OUT/pg"
compare pg_stat pg_sum "read_parquet('$OUT/pg/*.parquet')"; PG "DROP TABLE sweep_src;" >/dev/null

echo "== BATCH mysql =="; my_seed
export_parquet mysql "mysql://rivet:rivet@127.0.0.1:3306/rivet" "SELECT * FROM sweep_src" "$OUT/my"
compare my_stat my_sum "read_parquet('$OUT/my/*.parquet')"; MY "DROP TABLE sweep_src;"

echo "== BATCH mssql =="; ms_seed
export_parquet mssql "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet" "SELECT * FROM dbo.sweep_src" "$OUT/ms"
compare ms_stat ms_sum "read_parquet('$OUT/ms/*.parquet')"; MS "DROP TABLE dbo.sweep_src;"

rm -rf "$OUT"
echo "###############################################"
echo "BATCH: $MATCH/$TOTAL independent checks matched the source ($FAILED failed)"
[ "$FAILED" -eq 0 ] || { echo "SILENT-CORRUPTION DETECTED"; exit 1; }
