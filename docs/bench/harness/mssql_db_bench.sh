#!/usr/bin/env bash
# DBA-harm probe for rivet on SQL Server — the MSSQL analogue of the PG harm
# signals in REPORT_pg.md / REPORT_mssql.md.
#
# SQL Server has no MVCC / xmin horizon, so the PG harm story (a long snapshot
# pinning the vacuum horizon) does not apply. rivet's MSSQL export is a sequence
# of *autocommit* chunked SELECTs (no explicit BEGIN TRAN), so it holds no long
# open transaction and blocks no log truncation. What it does touch:
#   - shared (S) locks per chunk under READ COMMITTED,
#   - zero log-flush write pressure (rivet only issues SELECTs — read-only).
# This probe measures both directly against the live `mssql` container.
#
# Signals (rivet chunked export of bench_narrow, 500k rows):
#   - Log Flush Waits delta  — sys.dm_os_performance_counters (write pressure)
#   - longest single request — sys.dm_exec_requests (per-chunk SELECT duration)
#   - longest open txn       — sys.dm_tran_active_transactions (autocommit span)
#   - peak user locks held   — sys.dm_tran_locks (the READ COMMITTED footprint)
#   - log_reuse_wait_desc    — is rivet holding back log truncation?
#
# Prereqs: `docker compose up -d mssql`, dev/mssql/init.sql applied,
#          dev/bench/seed_bench_mssql.sql applied (bench_narrow seeded).
# Usage:   RIVET=./target/release/rivet docs/bench/harness/mssql_db_bench.sh

set -uo pipefail
RIVET="${RIVET:-./target/release/rivet}"
SQLCMD=(docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd
        -S localhost -U sa -P 'Rivet_Passw0rd!' -C -d rivet -h -1 -W)

q() { "${SQLCMD[@]}" -Q "SET NOCOUNT ON; $1" 2>/dev/null | tr -d '\r' | grep -vE '^$|rows affected'; }

WORK="$(mktemp -d)"
CFG="$WORK/rivet.yaml"
cat > "$CFG" <<YAML
source:
  type: mssql
  url: "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet"
  tls:
    accept_invalid_certs: true
exports:
  - name: bench_narrow
    table: bench_narrow
    mode: chunked
    chunk_column: id
    chunk_size: 50000
    format: parquet
    destination: {type: local, path: $WORK/out}
YAML

# Exact counter name — `LIKE 'Log Flush Wait%'` also matches `Log Flush Wait Time`.
lfw() { q "SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name = 'Log Flush Waits/sec' AND instance_name = 'rivet'"; }

echo "=== Run A — write pressure + log-truncation hold (rivet is read-only) ==="
PRE_LFW="$(lfw)"
"$RIVET" run --config "$CFG" --export bench_narrow >/dev/null 2>&1
RC=$?
POST_LFW="$(lfw)"
# CHECKPOINT first so log_reuse_wait_desc reflects the *current* blocker, not a
# stale one from before. With rivet's autocommit SELECTs done and no other
# session active, NOTHING here proves rivet pins nothing back from truncation.
LOG_REUSE="$(q "CHECKPOINT; SELECT log_reuse_wait_desc FROM sys.databases WHERE name='rivet'")"
echo "  rivet export rc=$RC"
echo "  Log Flush Waits delta (rivet DB): $((POST_LFW - PRE_LFW))"
echo "  log_reuse_wait_desc after export: $LOG_REUSE"

echo "=== Run B — lock / transaction footprint (DMV sampler @ 50 ms) ==="
q "IF OBJECT_ID('rivet_harm_samples','U') IS NOT NULL DROP TABLE rivet_harm_samples;
   CREATE TABLE rivet_harm_samples (longest_req_ms INT, user_locks INT, longest_txn_ms INT, user_sessions INT);" >/dev/null

# Background server-side sampler: 400 × 50 ms ≈ 20 s window (comfortably longer
# than the export). MAX() over the window captures the peak during the run;
# idle samples read 0 and don't affect it. @self excludes the sampler session.
"${SQLCMD[@]}" -Q "
SET NOCOUNT ON;
DECLARE @i INT = 0, @self INT = @@SPID;
WHILE @i < 400
BEGIN
  INSERT INTO rivet_harm_samples (longest_req_ms, user_locks, longest_txn_ms, user_sessions)
  SELECT
    ISNULL((SELECT MAX(r.total_elapsed_time) FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON s.session_id=r.session_id
            WHERE s.is_user_process=1 AND r.session_id<>@self AND r.database_id=DB_ID('rivet')),0),
    (SELECT COUNT(*) FROM sys.dm_tran_locks l
            JOIN sys.dm_exec_sessions s2 ON s2.session_id=l.request_session_id
            WHERE s2.is_user_process=1 AND s2.session_id<>@self),
    ISNULL((SELECT MAX(DATEDIFF(MILLISECOND,t.transaction_begin_time,SYSUTCDATETIME()))
            FROM sys.dm_tran_active_transactions t
            JOIN sys.dm_tran_session_transactions st ON st.transaction_id=t.transaction_id
            JOIN sys.dm_exec_sessions s3 ON s3.session_id=st.session_id
            WHERE s3.is_user_process=1 AND s3.session_id<>@self),0),
    (SELECT COUNT(DISTINCT r.session_id) FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON s.session_id=r.session_id
            WHERE s.is_user_process=1 AND r.session_id<>@self AND r.database_id=DB_ID('rivet'));
  SET @i=@i+1; WAITFOR DELAY '00:00:00.050';
END" >/dev/null 2>&1 &
SAMPLER=$!
sleep 0.4
"$RIVET" run --config "$CFG" --export bench_narrow >/dev/null 2>&1
wait "$SAMPLER" 2>/dev/null

echo "  peak longest single request : $(q "SELECT MAX(longest_req_ms) FROM rivet_harm_samples") ms"
echo "  peak longest open txn       : $(q "SELECT MAX(longest_txn_ms) FROM rivet_harm_samples") ms"
echo "  peak user locks held        : $(q "SELECT MAX(user_locks) FROM rivet_harm_samples")"
echo "  peak concurrent rivet sess  : $(q "SELECT MAX(user_sessions) FROM rivet_harm_samples")"
q "DROP TABLE rivet_harm_samples" >/dev/null
rm -rf "$WORK"
