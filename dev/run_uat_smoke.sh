#!/usr/bin/env bash
# Quick PG-focused smoke for USER_TEST_PLAN.md (no MySQL unless container up).
# From repo root: bash dev/run_uat_smoke.sh
# Review /tmp/rivet_uat_smoke.txt and update Actual/Pass in the plan manually or via agent.

set -euo pipefail
cd "$(dirname "$0")/.."
U=/tmp/rivet_uat_smoke.txt
rm -f "$U"
pass() { echo "PASS $*" >> "$U"; }
fail() { echo "FAIL $*" >> "$U"; }
skip() { echo "SKIP $*" >> "$U"; }

rivet --help 2>&1 | grep -q run && pass A1 || fail A1
bash -c 'rivet completions zsh 2>/dev/null | head -1' | grep -qE 'compdef|rivet' && pass A2 || fail A2
# Capture output: pipeline exit status is unreliable under some shells / pipefail.
_a3=$(rivet check --config /nonexistent/no.yaml 2>&1) || true
printf '%s\n' "$_a3" | grep -qiE 'Error|error|no such|os error|not found' && pass A3 || fail A3

rivet check --config dev/pg_full.yaml >/dev/null 2>&1 && pass B1 || fail B1
rivet check --config dev/pg_full.yaml --export pg_users_csv >/dev/null 2>&1 && pass B2 || fail B2
if docker compose ps mysql 2>/dev/null | grep -q mysql; then
  rivet check --config dev/mysql_full.yaml >/dev/null 2>&1 && pass B3 || fail B3
else
  skip B3_no_mysql
fi
rivet doctor --config dev/pg_full.yaml >/dev/null 2>&1 && pass B4 || fail B4

rivet run --config dev/pg_full.yaml --export pg_users_csv --validate >/dev/null 2>&1 && pass C1_sample || fail C1_sample
rivet run --config dev/pg_full.yaml --export pg_users_parquet --validate 2>&1 | grep -qiE 'validat.*pass|pass.*validat' && pass C2 || fail C2
rivet run --config dev/pg_incremental.yaml --export pg_orders_incremental >/dev/null 2>&1 && pass C3 || fail C3
rivet run --config dev/pg_incremental.yaml --export pg_orders_incremental >/dev/null 2>&1 && pass C4 || fail C4
if docker compose ps mysql 2>/dev/null | grep -q mysql; then
  rivet run --config dev/mysql_full.yaml >/dev/null 2>&1 && pass C5 || fail C5
  rivet run --config dev/mysql_incremental.yaml >/dev/null 2>&1 && pass C6 || fail C6
else
  skip C5_C6_no_mysql
fi

rivet state show --config dev/pg_incremental.yaml >/dev/null 2>&1 && pass D1 || fail D1
rivet metrics --config dev/pg_incremental.yaml --last 5 >/dev/null 2>&1 && pass D2 || fail D2

rivet run --config dev/bench_chunked_seq.yaml >/dev/null 2>&1 && pass E1 || fail E1
rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4_serial --validate >/dev/null 2>&1 && pass E2_R1 || fail E2_R1

rivet run --config dev/test_meta_columns.yaml --validate >/dev/null 2>&1 && pass F1 || fail F1
rivet run --config dev/test_compression.yaml >/dev/null 2>&1 && pass F2 || fail F2
rivet run --config dev/test_compression.yaml --export users_skip_empty >/dev/null 2>&1 && pass F3 || fail F3

PGPASSWORD=rivet rivet check --config dev/pg_structured.yaml >/dev/null 2>&1 && pass G1 || fail G1
if docker compose ps mysql 2>/dev/null | grep -q mysql; then
  rivet check --config dev/mysql_structured.yaml >/dev/null 2>&1 && pass G2 || fail G2
else
  skip G2_no_mysql
fi

rivet check --config dev/pg_degraded.yaml >/dev/null 2>&1 && pass H1 || fail H1
_h2=$(rivet doctor --config dev/test_pg_wrongpass.yaml 2>&1) || true
printf '%s\n' "$_h2" | grep -qiE '\[FAIL\]|FAIL|Source error|db error|auth|password|denied|refused' && pass H2 || fail H2

if test -f dev/_uat_time_window.yaml; then
  rivet run --config dev/_uat_time_window.yaml >/dev/null 2>&1 && pass J1 || fail J1
else
  skip J1_no_file
fi

bash -c 'rivet run --config dev/test_stdout.yaml 2>/dev/null | head -1' | grep -q . && pass L1 || fail L1

rivet run --config dev/test_params.yaml --param MAX_ID=10 >/dev/null 2>&1 && pass M1 || fail M1
rivet check --config dev/test_params.yaml --param MAX_ID=100 >/dev/null 2>&1 && pass M3 || fail M3

rivet run --config dev/test_quality.yaml --export users_quality_pass --validate >/dev/null 2>&1 && pass N1 || fail N1

_o1=$(RUST_LOG=info rivet run --config dev/test_memory_batch.yaml 2>&1) || true
printf '%s\n' "$_o1" | grep -qE 'batch_size_memory|batch_size_memory_mb|computed batch_size' && pass O1 || fail O1

rivet run --config dev/test_file_split.yaml --validate >/dev/null 2>&1 && pass P1 || fail P1
n=$(bash -c 'ls dev/output/users_split_*_part*.parquet 2>/dev/null | wc -l' | tr -d ' ')
if [ "${n:-0}" -ge 2 ] 2>/dev/null; then pass "P2(n=$n)"; else fail P2; fi

rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4_balanced >/dev/null 2>&1 && pass S1 || fail S1
rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4 --parallel-exports >/dev/null 2>&1 && pass S4 || fail S4

curl -sf http://localhost:9090/-/healthy >/dev/null && pass T1 || fail T1

cat "$U"
echo "---"
grep -c '^PASS' "$U" || true
grep -c '^FAIL' "$U" || true
grep -c '^SKIP' "$U" || true
