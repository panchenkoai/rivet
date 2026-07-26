#!/usr/bin/env bash
# Parallel-keyset sparse GOLDEN verifier — DEVBOX runner.
# Runs keyset-parallel against an already-seeded engine and asserts the golden:
# EXACTLY 10,000,000 rows and 20 part files (parallel=4, chunk_size=500000).
#
#   ./verify.sh <engine> <url> [table] [expected_rows] [expected_files]
#   ./verify.sh postgres "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
#   ./verify.sh mysql    "mysql://rivet:rivet@127.0.0.1:3306/rivet" keyset_sparse_unsigned
#
# Requires: RIVET_BIN (defaults to `rivet` on PATH — use the DOWNLOADED release
# binary on the pre-release stand), duckdb on PATH for the parquet re-read.
set -euo pipefail

engine="${1:?usage: verify.sh <engine> <url> [table] [rows] [files]}"
url="${2:?missing source url}"
table="${3:-keyset_sparse}"
want_rows="${4:-10000000}"
want_files="${5:-21}"
RIVET_BIN="${RIVET_BIN:-rivet}"

out="$(mktemp -d)/out"
cfg="$(mktemp).yaml"
mkdir -p "$out"
# SQL Server stands use a self-signed cert — accept it (the URL scheme is sqlserver://).
tls_block=""
[ "$engine" = "mssql" ] && tls_block=$'\n  tls:\n    accept_invalid_certs: true'
cat >"$cfg" <<YAML
source:
  type: ${engine}
  url: "${url}"${tls_block}
exports:
  - name: keyset_sparse_golden
    table: ${table}
    mode: chunked
    chunk_by_key: id
    parallel: 4
    chunk_size: 500000
    format: parquet
    compression: zstd
    destination:
      type: local
      path: ${out}/
YAML

echo "[$engine/$table] running keyset-parallel ($RIVET_BIN)…"
"$RIVET_BIN" run -c "$cfg"

got_files=$(find "$out" -name '*.parquet' | wc -l | tr -d ' ')
got_rows=$(duckdb -noheader -list -c \
  "SELECT count(*) FROM read_parquet('$out/**/*.parquet')" | tr -d ' ')
distinct=$(duckdb -noheader -list -c \
  "SELECT count(DISTINCT id) FROM read_parquet('$out/**/*.parquet')" | tr -d ' ')

echo "[$engine/$table] rows=$got_rows distinct=$distinct files=$got_files (golden: $want_rows rows / $want_files files)"

fail=0
[ "$got_rows"  = "$want_rows" ]  || { echo "FAIL: rows $got_rows != $want_rows"; fail=1; }
[ "$distinct"  = "$want_rows" ]  || { echo "FAIL: distinct $distinct != $want_rows (boundary drop/dup)"; fail=1; }
[ "$got_files" = "$want_files" ] || { echo "FAIL: files $got_files != $want_files"; fail=1; }
if [ "$fail" = 0 ]; then echo "[$engine/$table] GOLDEN PASS ✅"; else echo "[$engine/$table] GOLDEN RED ❌"; exit 1; fi
