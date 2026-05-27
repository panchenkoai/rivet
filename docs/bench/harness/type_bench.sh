#!/usr/bin/env bash
# Cross-tool TYPE-FIDELITY benchmark (sibling of perf bench.sh).
#
# Drives the same six tools used by the perf bench — Rivet, Sling, DuckDB
# (postgres_scanner / mysql_scanner), ClickHouse local, odbc2parquet, dlt —
# but instead of measuring wall/RSS we measure *what types each tool puts
# into the Parquet file*. The output is one Parquet per (tool, source, table)
# triple; `type_diff.py` then reads them and produces the comparison matrix.
#
# We deliberately reuse the canonical `rivet_type_matrix` and
# `rivet_type_matrix_full` seed tables from `dev/{postgres,mysql}/init.sql`
# — the same tables the in-process roundtrip suite already validates Rivet
# against, so this bench measures *how the other tools degrade* those
# columns rather than introducing a new fixture.
#
# Usage:
#   docs/bench/harness/type_bench.sh <source> <tool> <table>
#     source = pg | mysql
#     tool   = rivet | sling | duckdb | clickhouse | odbc2parquet
#     table  = rivet_type_matrix | rivet_type_matrix_full
#
#   docs/bench/harness/type_bench.sh all          # run the full matrix
#
# Output:
#   $BENCH_ROOT/<source>/<tool>/<table>/data.parquet
#   $BENCH_ROOT/<source>/<tool>/<table>/stderr.log
# where BENCH_ROOT defaults to tests/.live-tmp/type_bench/ so the duckdb +
# clickhouse containers (which bind-mount tests/.live-tmp as /work) can
# read the artefacts back without extra docker plumbing.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
BENCH_ROOT="${BENCH_ROOT:-$REPO_ROOT/tests/.live-tmp/type_bench}"
# In-container view of BENCH_ROOT for ClickHouse / DuckDB containers.
BENCH_CTR="${BENCH_CTR:-/work/type_bench}"

PG_URL='postgresql://rivet:rivet@127.0.0.1:5432/rivet'
MY_URL='mysql://rivet:rivet@127.0.0.1:3306/rivet'

RIVET_BIN="${RIVET_BIN:-$REPO_ROOT/target/release/rivet}"

# ── tool runners ──────────────────────────────────────────────────────────

# run_rivet <source> <table> <host_out_dir>
run_rivet() {
    local src=$1 tbl=$2 out=$3
    local cfg="$out/rivet.yaml"
    if [[ $src == pg ]]; then
        cat > "$cfg" <<EOF
source: { type: postgres, url: "$PG_URL" }
exports:
  - name: $tbl
    table: public.$tbl
    mode: full
    format: parquet
    compression: snappy
    destination: { type: local, path: $out }
EOF
    else
        # MySQL: the wire protocol does not expose NUMERIC precision/scale,
        # so the `rivet_type_matrix` decimal columns need explicit overrides.
        # This matches the perf-bench rivet_mysql.yaml shape (roadmap §12).
        local decimals=""
        if [[ $tbl == rivet_type_matrix ]]; then
            decimals='    columns: { amount: "decimal(18,2)", fee: "decimal(18,6)" }'
        fi
        cat > "$cfg" <<EOF
source: { type: mysql, url: "$MY_URL" }
exports:
  - name: $tbl
    table: rivet.$tbl
    mode: full
    format: parquet
    compression: snappy
$decimals
    destination: { type: local, path: $out }
EOF
    fi
    "$RIVET_BIN" run -c "$cfg"
}

# run_sling <source> <table> <host_out_dir>
# Sling expects a host-side path. We pass the host path directly.
run_sling() {
    local src=$1 tbl=$2 out=$3
    if [[ $src == pg ]]; then
        export PG_LOCAL="postgres://rivet:rivet@127.0.0.1:5432/rivet?sslmode=disable"
        sling run --src-conn PG_LOCAL --src-stream "public.$tbl" \
            --tgt-object "file://$out/data.parquet" \
            --tgt-options '{format: parquet, compression: snappy}' \
            --mode full-refresh
    else
        export MY_LOCAL="mysql://rivet:rivet@127.0.0.1:3306/rivet"
        sling run --src-conn MY_LOCAL --src-stream "rivet.$tbl" \
            --tgt-object "file://$out/data.parquet" \
            --tgt-options '{format: parquet, compression: snappy}' \
            --mode full-refresh
    fi
}

# run_duckdb <source> <table> <host_out_dir>
run_duckdb() {
    local src=$1 tbl=$2 out=$3
    if [[ $src == pg ]]; then
        duckdb -c "
INSTALL postgres; LOAD postgres;
ATTACH 'host=127.0.0.1 port=5432 user=rivet password=rivet dbname=rivet' AS pg (TYPE postgres, READ_ONLY);
COPY (SELECT * FROM pg.public.\"$tbl\") TO '$out/data.parquet' (FORMAT parquet, COMPRESSION snappy);
"
    else
        duckdb -c "
INSTALL mysql; LOAD mysql;
ATTACH 'host=127.0.0.1 port=3306 user=rivet password=rivet database=rivet' AS my (TYPE mysql, READ_ONLY);
COPY (SELECT * FROM my.rivet.\"$tbl\") TO '$out/data.parquet' (FORMAT parquet, COMPRESSION snappy);
"
    fi
}

# run_clickhouse <source> <table> <host_out_dir>
# Runs inside the `rivet-clickhouse` container. From within the docker
# compose network it reaches the source DBs via their compose service names
# (`postgres`, `mysql`) — portable across macOS Desktop and native Linux
# Docker, no `host.docker.internal` dependency. Output path is /work/...
run_clickhouse() {
    local src=$1 tbl=$2 out=$3
    local rel="${out#$BENCH_ROOT/}"          # source/tool/table
    local ctr_out="$BENCH_CTR/$rel"
    if [[ $src == pg ]]; then
        docker exec rivet-clickhouse clickhouse local --query "
SELECT * FROM postgresql('postgres:5432', 'rivet', '$tbl', 'rivet', 'rivet', 'public')
INTO OUTFILE '$ctr_out/data.parquet' FORMAT Parquet
SETTINGS output_format_parquet_compression_method = 'snappy';
"
    else
        docker exec rivet-clickhouse clickhouse local --query "
SELECT * FROM mysql('mysql:3306', 'rivet', '$tbl', 'rivet', 'rivet')
INTO OUTFILE '$ctr_out/data.parquet' FORMAT Parquet
SETTINGS output_format_parquet_compression_method = 'snappy';
"
    fi
}

# run_odbc2parquet <source> <table> <host_out_dir>
# Only PG path is exercised — MySQL ODBC driver is not part of the default
# perf-bench setup; mirror that for type-bench (skip with note).
run_odbc2parquet() {
    local src=$1 tbl=$2 out=$3
    if [[ $src == pg ]]; then
        odbc2parquet --quiet query \
            --column-length-limit 65536 \
            --connection-string "Driver={postgre_unicode};Server=127.0.0.1;Port=5432;Database=rivet;Uid=rivet;Pwd=rivet;" \
            "$out/data.parquet" \
            "SELECT * FROM public.\"$tbl\""
    else
        echo "skip: odbc2parquet MySQL path not configured in this bench" >&2
        return 99
    fi
}

# ── dispatcher ────────────────────────────────────────────────────────────

run_one() {
    local src=$1 tool=$2 tbl=$3
    local out="$BENCH_ROOT/$src/$tool/$tbl"
    rm -rf "$out"; mkdir -p "$out"
    local log="$out/stderr.log"
    local rc=0
    case "$tool" in
        rivet)        run_rivet        "$src" "$tbl" "$out" >>"$log" 2>&1 || rc=$?;;
        sling)        run_sling        "$src" "$tbl" "$out" >>"$log" 2>&1 || rc=$?;;
        duckdb)       run_duckdb       "$src" "$tbl" "$out" >>"$log" 2>&1 || rc=$?;;
        clickhouse)   run_clickhouse   "$src" "$tbl" "$out" >>"$log" 2>&1 || rc=$?;;
        odbc2parquet) run_odbc2parquet "$src" "$tbl" "$out" >>"$log" 2>&1 || rc=$?;;
        *)            echo "unknown tool: $tool" >&2; return 2;;
    esac
    local files
    files=$(find "$out" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
    printf "  %-13s %-8s %-25s rc=%-3d files=%s\n" "$tool" "$src" "$tbl" "$rc" "$files"
}

if [[ "${1:-}" == "all" || -z "${1:-}" ]]; then
    mkdir -p "$BENCH_ROOT"
    echo "== TYPE BENCH =="
    echo "BENCH_ROOT=$BENCH_ROOT"
    for src in pg mysql; do
        for tbl in rivet_type_matrix rivet_type_matrix_full; do
            for tool in rivet sling duckdb clickhouse odbc2parquet; do
                run_one "$src" "$tool" "$tbl"
            done
        done
    done
else
    run_one "$1" "$2" "$3"
fi
