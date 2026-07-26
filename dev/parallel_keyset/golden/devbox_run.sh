#!/usr/bin/env bash
# DEVBOX golden runner — seed the pre-release batch stands + run keyset-parallel +
# assert the golden (10M rows / 21 files), per engine. Detached, writes a report.
#
# Expects (on the devbox):
#   RIVET_BIN  — the scp'd RELEASE binary (feat/parallel-keyset). Default ~/rivet-golden/rivet.
#   duckdb on PATH (verify.sh re-reads the parquet).
# Stands (batch): pg18=5518, mysql80=3580, mssql2022=1522.
set -uo pipefail
cd ~/rivet
G=dev/parallel_keyset/golden
export RIVET_BIN="${RIVET_BIN:-$HOME/rivet-golden/rivet}"
REPORT="${REPORT:-$HOME/rivet-golden/report.txt}"
mkdir -p "$(dirname "$REPORT")"
exec > "$REPORT" 2>&1
echo "=== keyset-parallel GOLDEN on devbox — started $(date) ==="
echo "binary: $RIVET_BIN ($($RIVET_BIN --version 2>&1 | head -1))"

run() { echo; echo "########## $1 ##########"; }

# ── Postgres (stand-pg18-batch : 5518) ──────────────────────────────────────
run "PG18 seed"
docker exec -i stand-pg18-batch-1 psql -U rivet -d rivet -q < "$G/fixture_postgres.sql" | tail -2
run "PG18 verify"
"$G/verify.sh" postgres "postgresql://rivet:rivet@127.0.0.1:5518/rivet"

# ── MySQL 8.0 signed (stand-mysql80-batch : 3580) ───────────────────────────
run "MySQL80 signed seed"
docker exec -i stand-mysql80-batch-1 mysql -urivet -privet rivet < "$G/fixture_mysql.sql" | tail -2
run "MySQL80 signed verify"
"$G/verify.sh" mysql "mysql://rivet:rivet@127.0.0.1:3580/rivet"

# ── MySQL 8.0 BIGINT UNSIGNED (key > i64::MAX) ──────────────────────────────
run "MySQL80 unsigned seed"
docker exec -i stand-mysql80-batch-1 mysql -urivet -privet rivet < "$G/fixture_mysql_unsigned.sql" | tail -2
run "MySQL80 unsigned verify"
"$G/verify.sh" mysql "mysql://rivet:rivet@127.0.0.1:3580/rivet" keyset_sparse_unsigned

# ── SQL Server 2022 (stand-mssql2022-batch : 1522) ──────────────────────────
run "MSSQL2022 seed"
SQLCMD="/opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P Rivet_Passw0rd! -C -b"
# create the rivet DB (batch stands do not auto-create it) + SIMPLE recovery so
# the single 10M INSERT does not blow the transaction log.
docker exec -i stand-mssql2022-batch-1 bash -lc "$SQLCMD -Q \"IF DB_ID('rivet') IS NULL CREATE DATABASE rivet; ALTER DATABASE rivet SET RECOVERY SIMPLE;\""
docker exec -i stand-mssql2022-batch-1 bash -lc "$SQLCMD -d rivet -i /dev/stdin" < "$G/fixture_mssql.sql" | tail -3
run "MSSQL2022 verify"
"$G/verify.sh" mssql "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1522/rivet"

echo; echo "=== GOLDEN run done $(date) ==="
grep -E "GOLDEN (PASS|RED)" "$REPORT" || true
