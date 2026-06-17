#!/usr/bin/env bash
# Source-harm A/B — rivet vs itself (or vs $RIVET_OLD), per engine.
#
# The harm complement to batch_throughput_ab.sh: that one measures throughput +
# RSS; this measures rivet's FOOTPRINT ON THE SOURCE while extracting — the axis
# rivet is built around. It confirms the memory-driven batch default (0.12.0)
# never makes the source *worse*, and is gentler exactly where it drains faster.
#
# Sampled while rivet extracts $TABLE (chunked, default tuning), OLD vs NEW:
#   MySQL  (perf_schema, bash-loop @~50ms): longest active rivet query (ms, max +
#          p50) and InnoDB row locks held (0 = pure MVCC read, no row locks).
#   SQL Server (DMVs, server-side @50ms loop): longest single request (ms),
#          longest open transaction (ms), peak user locks (dm_tran_locks).
#   PostgreSQL: the NEUTRAL engine — its work_mem-bounded server-side cursor was
#          already efficient, so 0.12.0 doesn't change its hold/locks/dead-tuple
#          footprint at all. Measuring it needs a server-side sampler (psql is
#          often not on PATH and per-tick `docker exec` is too slow for PG's short
#          per-chunk reads); left out here by design — the verdict is "flat".
#
# Reference findings — bench_narrow 10.24M, 0.11 (static 10k) -> 0.12 (memory-driven):
#   SQL Server: longest request 1839 -> 276 ms (6.7x shorter); peak locks 31 -> 10
#               (3x fewer); longest txn ~0 both (no transaction/version-store pin).
#   MySQL:      0 row locks (MVCC) in both; median query 230 -> 137 ms; the source
#               is *engaged* ~7x less in aggregate (wall 58s -> 9s).
#   PostgreSQL: flat (no speedup -> no harm change).
# The law: faster drain = less source engagement; rivet never harms the source
# more with the bigger batch, and is strictly gentler on the engine it accelerates.
#
# Prereqs: docker DBs up + bench tables seeded; `cargo build --release`; GNU time.
# Usage:
#   RIVET_OLD=/tmp/rivet-old ./dev/bench/harm_ab.sh
#   ENGINES="mysql" RIVET_OLD=/tmp/rivet-old ./dev/bench/harm_ab.sh
set -uo pipefail

RIVET="${RIVET:-./target/release/rivet}"
RIVET_OLD="${RIVET_OLD:-}"                 # if set, runs an OLD vs NEW pair; else NEW only
MYSQL_URL="${MYSQL_URL:-mysql://rivet:rivet@127.0.0.1:3306/rivet}"
MSSQL_URL="${MSSQL_URL:-sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet}"
MYSQL_CTR="${MYSQL_CTR:-rivet-mysql-1}"    # container for root-priv perf_schema sampling
MYSQL_ROOT_PW="${MYSQL_ROOT_PW:-rivet}"
TABLE="${TABLE:-bench_narrow}"
CHUNK_SIZE="${CHUNK_SIZE:-512000}"
ENGINES="${ENGINES:-mysql mssql}"
CFG_DIR="$(mktemp -d)"
command -v gtime &>/dev/null && TIME=gtime || TIME=/usr/bin/time

# --- MySQL: perf_schema bash-loop sampler (root, for cross-session visibility) ---
my_sql_longest="SELECT (SELECT IFNULL(MAX(esc.TIMER_WAIT)/1e9,0) FROM performance_schema.events_statements_current esc JOIN performance_schema.threads t ON t.thread_id=esc.thread_id WHERE t.processlist_db='rivet'),(SELECT COUNT(*) FROM performance_schema.data_locks dl JOIN performance_schema.threads t2 ON dl.thread_id=t2.thread_id WHERE t2.processlist_db='rivet')"
mysql_probe() { # bin label
    local bin="$1" lbl="$2"
    local out="$CFG_DIR/my_$lbl" samp="$CFG_DIR/my_$lbl.samp"; : > "$samp"
    cat >"$CFG_DIR/my.yaml" <<YAML
source: {type: mysql, url: "$MYSQL_URL"}
exports:
  - {name: $TABLE, query: "SELECT * FROM $TABLE", mode: chunked, chunk_column: id, chunk_size: $CHUNK_SIZE, parallel: 1, format: parquet, destination: {type: local, path: $out}}
YAML
    ( while true; do docker exec -i "$MYSQL_CTR" mysql -uroot -p"$MYSQL_ROOT_PW" rivet -N -B -e "$my_sql_longest" 2>/dev/null; done ) >>"$samp" &
    local sampler=$!
    "$TIME" --format="%e" --output="$CFG_DIR/my_$lbl.w" "$bin" run --config "$CFG_DIR/my.yaml" >/dev/null 2>&1 || true
    kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null
    local wall qmax qp50 lk
    wall=$(tail -1 "$CFG_DIR/my_$lbl.w")
    qmax=$(awk '{if($1+0>m)m=$1+0}END{printf "%.0f",m+0}' "$samp")
    qp50=$(awk '{print $1+0}' "$samp" | sort -n | awk '{a[NR]=$1}END{printf "%.0f",a[int(NR*0.5)]+0}')
    lk=$(awk '{if($2+0>m)m=$2+0}END{print m+0}' "$samp")
    printf "| %-10s | %-3s | %8ss | %8s ms | %8s ms | %7s |\n" "mysql" "$lbl" "$wall" "$qmax" "$qp50" "$lk"
}

# --- SQL Server: server-side DMV sampler (matches mssql_harm_compare.sh) ---
SQLCMD=(docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -C -d rivet -h -1 -W)
msq() { "${SQLCMD[@]}" -Q "SET NOCOUNT ON; $1" 2>/dev/null | tr -d '\r' | grep -vE '^$|rows affected'; }
mssql_probe() { # bin label
    local bin="$1" lbl="$2"
    local out="$CFG_DIR/ms_$lbl"
    cat >"$CFG_DIR/ms.yaml" <<YAML
source:
  type: mssql
  url: "$MSSQL_URL"
  tls: { accept_invalid_certs: true }
exports:
  - { name: $TABLE, query: "SELECT * FROM $TABLE", mode: chunked, chunk_column: id, chunk_size: $CHUNK_SIZE, parallel: 1, format: parquet, destination: { type: local, path: $out } }
YAML
    msq "IF OBJECT_ID('rivet_harm_samples','U') IS NOT NULL DROP TABLE rivet_harm_samples;
         CREATE TABLE rivet_harm_samples(req_ms INT, locks INT, txn_ms INT);" >/dev/null
    "${SQLCMD[@]}" -Q "SET NOCOUNT ON; DECLARE @i INT=0,@self INT=@@SPID;
      WHILE @i<20000 BEGIN
        INSERT INTO rivet_harm_samples
        SELECT ISNULL((SELECT MAX(r.total_elapsed_time) FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id=r.session_id WHERE s.is_user_process=1 AND r.session_id<>@self AND r.database_id=DB_ID('rivet')),0),
               (SELECT COUNT(*) FROM sys.dm_tran_locks l JOIN sys.dm_exec_sessions s2 ON s2.session_id=l.request_session_id WHERE s2.is_user_process=1 AND s2.session_id<>@self),
               ISNULL((SELECT MAX(DATEDIFF(MILLISECOND,t.transaction_begin_time,SYSUTCDATETIME())) FROM sys.dm_tran_active_transactions t JOIN sys.dm_tran_session_transactions st ON st.transaction_id=t.transaction_id JOIN sys.dm_exec_sessions s3 ON s3.session_id=st.session_id WHERE s3.is_user_process=1 AND s3.session_id<>@self),0);
        SET @i=@i+1; WAITFOR DELAY '00:00:00.050'; END" >/dev/null 2>&1 &
    local sampler=$!; sleep 0.4
    "$TIME" --format="%e" --output="$CFG_DIR/ms_$lbl.w" "$bin" run --config "$CFG_DIR/ms.yaml" >/dev/null 2>&1 || true
    kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null
    local wall req txn lock
    wall=$(tail -1 "$CFG_DIR/ms_$lbl.w")
    req=$(msq "SELECT MAX(req_ms) FROM rivet_harm_samples"); txn=$(msq "SELECT MAX(txn_ms) FROM rivet_harm_samples"); lock=$(msq "SELECT MAX(locks) FROM rivet_harm_samples")
    msq "DROP TABLE rivet_harm_samples" >/dev/null
    printf "| %-10s | %-3s | %8ss | %8s ms | %8s ms | %7s |\n" "mssql/req" "$lbl" "$wall" "${req:-?}" "${txn:-?}" "${lock:-?}"
}

echo "# Source-harm A/B — rivet${RIVET_OLD:+ OLD vs NEW}, table=$TABLE, chunked, default tuning"
echo
echo "## MySQL — longest active query + InnoDB row locks (0 = MVCC)"
printf "| %-10s | %-3s | %8s | %11s | %11s | %7s |\n" engine bin wall "longest" "median(p50)" "rowlocks"
printf "| %-10s | %-3s | %8s | %11s | %11s | %7s |\n" --- --- ---: ---: ---: ---:
case " $ENGINES " in *" mysql "*) [[ -n "$RIVET_OLD" ]] && mysql_probe "$RIVET_OLD" old; mysql_probe "$RIVET" new;; esac
echo
echo "## SQL Server — longest request / longest txn / peak locks"
printf "| %-10s | %-3s | %8s | %11s | %11s | %7s |\n" engine bin wall "longest_req" "longest_txn" "locks"
printf "| %-10s | %-3s | %8s | %11s | %11s | %7s |\n" --- --- ---: ---: ---: ---:
case " $ENGINES " in *" mssql "*) [[ -n "$RIVET_OLD" ]] && mssql_probe "$RIVET_OLD" old; mssql_probe "$RIVET" new;; esac
echo
echo "_PostgreSQL: neutral engine (work_mem-bounded cursor) — 0.12.0 leaves its source footprint unchanged; not sampled here (see header)._"
rm -rf "$CFG_DIR"
