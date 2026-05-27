#!/usr/bin/env bash
# BigQuery TYPE-FIDELITY round-trip (sibling of `type_bench.sh`).
#
# Takes the Parquet files Rivet already produced under
# `$BENCH_ROOT/{pg,mysql}/rivet/{rivet_type_matrix,rivet_type_matrix_full}/`
# (run `type_bench.sh` first to refresh them), loads each via `bq load
# --autodetect --source_format=PARQUET`, then dumps:
#   - the BigQuery-inferred schema (one JSON file per table)
#   - a small results table (count + sum of decimals + sample row)
#
# Cells land under `$BENCH_ROOT/bq/{src}/{table}/` so `type_diff_bq.py`
# can pick them up and join the BQ column into the same matrix shape
# `type_diff.py` produces for DuckDB / ClickHouse / pyarrow.
#
# Auth: relies on the gcloud user already being signed in
#   (`gcloud auth login` once). The bench never edits user gcloud config.
#
# Usage:
#   docs/bench/harness/type_bench_bq.sh all
#   docs/bench/harness/type_bench_bq.sh <source> <table>
#     source = pg | mysql
#     table  = rivet_type_matrix | rivet_type_matrix_full

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
BENCH_ROOT="${BENCH_ROOT:-$REPO_ROOT/tests/.live-tmp/type_bench}"

# Targets — overridable per environment. The dataset is per-developer; the
# bench creates / replaces tables there, so a throwaway dataset is fine.
BQ_PROJECT="${BQ_PROJECT:-rivet-data-tool}"
BQ_DATASET="${BQ_DATASET:-rivet_type_lab}"

# ── one (source, table) cell ──────────────────────────────────────────────

run_bq() {
    local src=$1 tbl=$2
    local pq_dir="$BENCH_ROOT/$src/rivet/$tbl"
    local out_dir="$BENCH_ROOT/bq/$src/$tbl"
    rm -rf "$out_dir"; mkdir -p "$out_dir"
    local log="$out_dir/stderr.log"

    # Find the rivet-produced parquet for this cell. `type_bench.sh`
    # writes exactly one *.parquet per Rivet export.
    local pq
    pq=$(ls "$pq_dir"/*.parquet 2>/dev/null | head -1 || true)
    if [[ -z "${pq:-}" ]]; then
        echo "skip: no parquet at $pq_dir (run type_bench.sh first)" | tee "$log" >&2
        printf "  %-13s %-8s %-25s rc=%-3d schema=miss\n" "bq" "$src" "$tbl" 1
        return 0
    fi

    # BQ table id is per cell so two runs don't collide.
    local bq_tbl="rivet_bench_${src}_${tbl}"
    local fq="${BQ_PROJECT}:${BQ_DATASET}.${bq_tbl}"
    local rc=0

    bq load \
        --project_id="$BQ_PROJECT" \
        --replace \
        --source_format=PARQUET \
        --parquet_enable_list_inference \
        --parquet_enum_as_string \
        "$fq" "$pq" >"$log" 2>&1 || rc=$?

    if [[ $rc -ne 0 ]]; then
        printf "  %-13s %-8s %-25s rc=%-3d load=fail\n" "bq" "$src" "$tbl" "$rc"
        return 0
    fi

    # Capture the inferred schema as canonical JSON for type_diff_bq.py.
    bq show \
        --project_id="$BQ_PROJECT" \
        --schema --format=prettyjson \
        "${BQ_PROJECT}:${BQ_DATASET}.${bq_tbl}" \
        > "$out_dir/schema.json" 2>>"$log" || rc=$?

    # Aggregates that downstream value-validators want. We keep them in a
    # second JSON so the value-comparison row is independent of the schema
    # dump and either can be regenerated alone.
    local agg_query
    case "$tbl" in
        rivet_type_matrix)
            agg_query="SELECT
                count(*) AS n,
                CAST(sum(amount * 100) AS INT64) AS s_amount,
                CAST(sum(fee * 1000000) AS INT64) AS s_fee
                FROM \`${BQ_PROJECT}.${BQ_DATASET}.${bq_tbl}\`"
            ;;
        rivet_type_matrix_full)
            # No decimals here; just confirm row count round-tripped.
            agg_query="SELECT count(*) AS n
                FROM \`${BQ_PROJECT}.${BQ_DATASET}.${bq_tbl}\`"
            ;;
    esac
    bq query \
        --project_id="$BQ_PROJECT" \
        --use_legacy_sql=false \
        --format=prettyjson \
        --quiet \
        "$agg_query" \
        > "$out_dir/agg.json" 2>>"$log" || rc=$?

    # Best-effort cleanup so the dataset does not accumulate. The
    # schema/agg JSON we already captured stays in $out_dir.
    bq rm -f --project_id="$BQ_PROJECT" \
        "${BQ_PROJECT}:${BQ_DATASET}.${bq_tbl}" >>"$log" 2>&1 || true

    printf "  %-13s %-8s %-25s rc=%-3d schema=ok  agg=ok\n" "bq" "$src" "$tbl" "$rc"
}

# ── dispatcher ────────────────────────────────────────────────────────────

if [[ "${1:-}" == "all" || -z "${1:-}" ]]; then
    echo "== BQ TYPE BENCH =="
    echo "BENCH_ROOT=$BENCH_ROOT"
    echo "BQ ${BQ_PROJECT}:${BQ_DATASET}"
    for src in pg mysql; do
        for tbl in rivet_type_matrix rivet_type_matrix_full; do
            run_bq "$src" "$tbl"
        done
    done
else
    run_bq "$1" "$2"
fi
