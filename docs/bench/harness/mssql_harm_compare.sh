#!/usr/bin/env bash
# Per-tool DBA-harm comparison for SQL Server — the comparative analogue of the
# PG "DB-side signals" table (REPORT_pg.md). Each extractor (rivet / sling / dlt)
# is wrapped in the same DMV sampler so its footprint on the source is measured
# like-for-like: longest single request, longest open transaction, peak shared
# locks, peak concurrent sessions. All three are read-only, so Log Flush Waits
# stays ~0 for every tool (reported separately by mssql_db_bench.sh).
#
# Usage: RIVET=./target/release/rivet docs/bench/harness/mssql_harm_compare.sh [table]
#        (table defaults to content_items; must be seeded — seed_bench_mssql.sql)

set -uo pipefail
RIVET="${RIVET:-./target/release/rivet}"
TBL="${1:-content_items}"
ROOT="$(mktemp -d)"
SQLCMD=(docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd
        -S localhost -U sa -P 'Rivet_Passw0rd!' -C -d rivet -h -1 -W)
q() { "${SQLCMD[@]}" -Q "SET NOCOUNT ON; $1" 2>/dev/null | tr -d '\r' | grep -vE '^$|rows affected'; }
export MSSQL_SLING='sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433?database=rivet&encrypt=disable&TrustServerCertificate=true'

run_tool() {
    local tool=$1 out="$ROOT/$tool"
    rm -rf "$out"; mkdir -p "$out"
    case "$tool" in
        rivet)
            cat > "$ROOT/$tool.yaml" <<YAML
source:
  type: mssql
  url: "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet"
  tls: { accept_invalid_certs: true }
exports:
  - { name: $TBL, table: $TBL, mode: chunked, chunk_column: id, chunk_size: 10000, format: parquet, compression: zstd, destination: { type: local, path: $out } }
YAML
            "$RIVET" run -c "$ROOT/$tool.yaml" >/dev/null 2>&1 ;;
        sling)
            sling run --src-conn MSSQL_SLING --src-stream "dbo.$TBL" \
                --tgt-object "file://$out/data.parquet" >/dev/null 2>&1 ;;
        dlt)
            /tmp/rivet_bench/.venv/bin/python "$(dirname "$0")/dlt_mssql_pipeline.py" \
                "$TBL" "$out" 'mssql+pymssql://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet' >/dev/null 2>&1 ;;
    esac
}

harm_probe() {
    local tool=$1
    q "IF OBJECT_ID('rivet_harm_samples','U') IS NOT NULL DROP TABLE rivet_harm_samples;
       CREATE TABLE rivet_harm_samples (longest_req_ms INT, user_locks INT, longest_txn_ms INT, user_sessions INT);" >/dev/null
    # Background DMV sampler @ 50 ms; @self excludes the sampler. The cap is a
    # generous ceiling (~17 min) — the sampler is killed the moment the tool
    # finishes, so MAX() reflects only the peaks observed during the run.
    "${SQLCMD[@]}" -Q "
SET NOCOUNT ON; DECLARE @i INT=0,@self INT=@@SPID;
WHILE @i<20000 BEGIN
  INSERT INTO rivet_harm_samples
  SELECT ISNULL((SELECT MAX(r.total_elapsed_time) FROM sys.dm_exec_requests r
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
  SET @i=@i+1; WAITFOR DELAY '00:00:00.050'; END" >/dev/null 2>&1 &
    local sampler=$!
    sleep 0.4
    run_tool "$tool"
    # Stop the sampler now that the tool is done (don't wait out the full cap).
    kill "$sampler" 2>/dev/null
    wait "$sampler" 2>/dev/null
    local req lock txn sess
    req=$(q "SELECT MAX(longest_req_ms) FROM rivet_harm_samples")
    lock=$(q "SELECT MAX(user_locks) FROM rivet_harm_samples")
    txn=$(q "SELECT MAX(longest_txn_ms) FROM rivet_harm_samples")
    sess=$(q "SELECT MAX(user_sessions) FROM rivet_harm_samples")
    q "DROP TABLE rivet_harm_samples" >/dev/null
    printf "| %-6s | %6s | %6s | %5s | %5s |\n" "$tool" "${req:-?}" "${txn:-?}" "${lock:-?}" "${sess:-?}"
}

echo "Table: $TBL"
echo "| Tool | Longest single request (ms) | Longest open txn (ms) | Peak user locks | Peak concurrent sessions |"
echo "|---|---:|---:|---:|---:|"
for t in rivet sling dlt; do harm_probe "$t"; done
rm -rf "$ROOT"
