#!/usr/bin/env bash
# Narrow-table throughput + source-hold guard — the regression guard for the
# memory-driven default batch sizing (0.12.0).
#
# WHY THIS EXISTS
#   rivet's default batch was a static 10,000 rows. On NARROW tables (few/small
#   columns, many rows) that pinned MySQL and SQL Server to tiny Arrow flushes —
#   one parquet row-group per 10k rows — so the per-flush handoff dominated
#   wall-clock (MySQL/MSSQL ran ~7-9x slower than they needed to). The fix made
#   the default memory-driven (~64 MB per flush). This harness is what caught
#   the gap: rivet vs itself, per engine, on a narrow table. Run it after any
#   change to batch sizing (`src/tuning/profile.rs`), the shared
#   `AdaptiveBatchController`, or a per-engine read loop.
#
# WHAT IT MEASURES, per engine (pg / mysql / mssql)
#   hold(s)    wall time = how long the source query/cursor stays open. This is
#              rivet's source-pressure axis — a bigger client batch drains the
#              SAME SQL faster, so hold time DROPS (server-side work is
#              identical: the page `LIMIT`/`FETCH NEXT` is sized by `chunk_size`,
#              never `batch_size`). Lower = friendlier to the source.
#   rows/s     extraction throughput.
#   speedup×   NEW vs $RIVET_OLD, when an older binary is provided (the A/B).
#
# SEED-AGNOSTIC: `SELECT *` + an independent duckdb row-count, so it works against
# whatever `bench_narrow` your seed produced (500k in-tree; the headline 7-9x
# numbers were measured on a 10.24M-row variant — bigger table, bigger gap).
#
# PREREQS
#   1. DBs up (docker compose up -d) and bench tables seeded:
#        psql  "$PG_URL"    -f dev/bench/seed_bench_pg.sql
#        mysql … < dev/bench/seed_bench_mysql.sql      (and seed_bench_mssql.sql)
#   2. cargo build --release
#   3. GNU time (`brew install gnu-time`) and duckdb (row-count oracle) on PATH
#
# USAGE
#   ./dev/bench/batch_throughput_ab.sh                       # current binary, all engines
#   RIVET_OLD=/tmp/rivet-old ./dev/bench/batch_throughput_ab.sh   # A/B vs an older binary
#   ENGINES="mysql mssql" ./dev/bench/batch_throughput_ab.sh      # subset
set -uo pipefail

RIVET="${RIVET:-./target/release/rivet}"
RIVET_OLD="${RIVET_OLD:-}"                     # optional A/B reference binary
PG_URL="${PG_URL:-postgres://rivet:rivet@localhost:5432/rivet}"
MYSQL_URL="${MYSQL_URL:-mysql://rivet:rivet@127.0.0.1:3306/rivet}"
MSSQL_URL="${MSSQL_URL:-sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet}"
TABLE="${TABLE:-bench_narrow}"
CHUNK_COL="${CHUNK_COL:-id}"
CHUNK_SIZE="${CHUNK_SIZE:-512000}"
ENGINES="${ENGINES:-pg mysql mssql}"
OUT_ROOT="${OUT_ROOT:-/tmp/rivet_batch_bench}"
CFG_DIR="$(mktemp -d)"

command -v gtime &>/dev/null && TIME=gtime || TIME=/usr/bin/time

url_for()  { case "$1" in pg) echo "$PG_URL";; mysql) echo "$MYSQL_URL";; mssql) echo "$MSSQL_URL";; esac; }
type_for() { case "$1" in pg) echo postgres;; mysql) echo mysql;; mssql) echo mssql;; esac; }

gen_cfg() { # engine out_dir -> path
    local engine="$1" out="$2" cfg="$CFG_DIR/$engine.yaml"
    cat >"$cfg" <<YAML
source: {type: $(type_for "$engine"), url: "$(url_for "$engine")"}
exports:
  - {name: $TABLE, query: "SELECT * FROM $TABLE", mode: chunked, chunk_column: $CHUNK_COL, chunk_size: $CHUNK_SIZE, parallel: 1, format: parquet, destination: {type: local, path: $out}}
YAML
    echo "$cfg"
}

# one warmup + one timed run; echoes "<wall_seconds>|<rows_in_parquet>"
timed_run() { # bin cfg out_dir
    local bin="$1" cfg="$2" out="$3" tf; tf=$(mktemp)
    rm -rf "$out"; "$bin" run --config "$cfg" >/dev/null 2>/dev/null   # warmup (page cache / buffer pool)
    rm -rf "$out"
    "$TIME" --format="%e" --output="$tf" "$bin" run --config "$cfg" >/dev/null 2>/dev/null || true
    local w r; w=$(tail -1 "$tf"); rm -f "$tf"
    r=$(duckdb -noheader -list -c "SELECT count(*) FROM read_parquet('$out/*.parquet')" 2>/dev/null | tr -d ' ')
    echo "${w:-0}|${r:-0}"
}

echo "# rivet narrow-table throughput${RIVET_OLD:+ — A/B vs OLD}  (table=$TABLE, chunked, default tuning)"
echo "# $(date -u +%FT%TZ)  |  binary: $RIVET${RIVET_OLD:+  |  old: $RIVET_OLD}"
echo
if [[ -n "$RIVET_OLD" ]]; then
    printf "| %-7s | %11s | %11s | %8s | %12s | %s |\n" engine "OLD hold(s)" "NEW hold(s)" "speedup" "rows/s(NEW)" "rows"
    printf "| %-7s | %11s | %11s | %8s | %12s | %s |\n" --- ---: ---: ---: ---: :--:
else
    printf "| %-7s | %11s | %12s | %s |\n" engine "hold(s)" "rows/s" "rows"
    printf "| %-7s | %11s | %12s | %s |\n" --- ---: ---: :--:
fi

for e in $ENGINES; do
    out="$OUT_ROOT/$e"; cfg=$(gen_cfg "$e" "$out")
    IFS='|' read -r nw nr < <(timed_run "$RIVET" "$cfg" "$out")
    rps=$(awk "BEGIN{if($nw>0)printf \"%.0f\", $nr/$nw; else print \"-\"}")
    rowcell=$([ "${nr:-0}" -gt 0 ] 2>/dev/null && echo "✓ $nr" || echo "FAIL")
    if [[ -n "$RIVET_OLD" ]]; then
        IFS='|' read -r ow orr < <(timed_run "$RIVET_OLD" "$cfg" "$out.old")
        spd=$(awk "BEGIN{if($nw>0)printf \"%.2fx\", $ow/$nw; else print \"-\"}")
        [ "$nr" = "$orr" ] || rowcell="MISMATCH old=$orr new=$nr"
        printf "| %-7s | %11s | %11s | %8s | %12s | %s |\n" "$e" "$ow" "$nw" "$spd" "$rps" "$rowcell"
    else
        printf "| %-7s | %11s | %12s | %s |\n" "$e" "$nw" "$rps" "$rowcell"
    fi
done
echo
echo "_hold(s) = how long the source query/cursor is held open (source-pressure axis); lower is friendlier._"
echo "_The same SQL runs server-side regardless of batch_size (page size = chunk_size); a bigger client batch only drains it faster._"
rm -rf "$CFG_DIR"
