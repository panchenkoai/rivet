#!/usr/bin/env bash
# Rivet v0.5.x stabilization benchmark runner — Phase 2.
#
# Captures wall time, user CPU, sys CPU, peak RSS, and output size for each
# benchmark scenario, then prints a Markdown table.
#
# Prerequisites:
#   1. postgres running (docker compose up -d postgres)
#   2. bench tables seeded: psql "$POSTGRES_URL" -f dev/bench/seed_bench_pg.sql
#   3. rivet binary built: cargo build --release
#   4. GNU time available: brew install gnu-time (macOS) or use /usr/bin/time (Linux)
#
# Usage:
#   ./dev/bench/run_bench.sh [suite]
#
#   suite (optional, default = all):
#     compression   — compare none/fast/balanced/compact profiles
#     row_group     — compare row group targets 32/64/128/256 MB
#     batch_memory  — compare warn/fail/auto_shrink policies
#     quality       — compare uniqueness tracking with/without cap
#     all           — run every suite in order

set -euo pipefail

RIVET="${RIVET:-./target/release/rivet}"
POSTGRES_URL="${POSTGRES_URL:-postgres://rivet:rivet@localhost:5432/rivet}"
BENCH_OUT="${BENCH_OUT:-/tmp/rivet_bench}"
SUITE="${1:-all}"

# ── portable time ──────────────────────────────────────────────────────────────
# gtime (GNU time via brew) on macOS, /usr/bin/time on Linux.
# Both support --format; `time` shell builtin does not.
if command -v gtime &>/dev/null; then
    TIME_CMD="gtime"
elif /usr/bin/time --version &>/dev/null 2>&1; then
    TIME_CMD="/usr/bin/time"
else
    echo "WARNING: GNU time not found — RSS measurements will be 0. Install with: brew install gnu-time"
    TIME_CMD=""
fi

TIME_FMT="%e %U %S %M"    # elapsed real, user, sys, max RSS KB

# ── helpers ───────────────────────────────────────────────────────────────────

mkdir -p "$BENCH_OUT"

# run_export LABEL CONFIG_PATH [--export EXPORT_NAME] ENV_OVERRIDES...
# Runs rivet export, captures metrics, stores in LAST_* globals.
# Optional --export EXPORT_NAME selects a single export from the config.
run_export() {
    local label="$1"
    local config="$2"
    shift 2

    local export_flag=()
    if [[ "${1:-}" == "--export" ]]; then
        export_flag=(--export "$2")
        shift 2
    fi

    local env_args=("$@")

    local out_dir="$BENCH_OUT/$(echo "$label" | tr ' /' '__')"
    # Clean previous run's output so file counts reflect only this run.
    rm -rf "$out_dir"
    mkdir -p "$out_dir"

    local time_out
    time_out=$(mktemp)
    local rivet_stderr
    rivet_stderr=$(mktemp)

    # Build env args for the child process
    local env_prefix=("BENCH_RUN_OUT=$out_dir")
    for kv in "${env_args[@]:-}"; do
        env_prefix+=("$kv")
    done

    local exit_code=0
    if [[ -n "$TIME_CMD" ]]; then
        env ${env_prefix[@]+"${env_prefix[@]}"} "$TIME_CMD" --format="$TIME_FMT" --output="$time_out" \
            "$RIVET" run --config "$config" ${export_flag[@]+"${export_flag[@]}"} \
            2>"$rivet_stderr" || exit_code=$?
    else
        env ${env_prefix[@]+"${env_prefix[@]}"} \
            "$RIVET" run --config "$config" ${export_flag[@]+"${export_flag[@]}"} \
            2>"$rivet_stderr" || exit_code=$?
        echo "0 0 0 0" > "$time_out"
    fi

    if [[ $exit_code -ne 0 ]]; then
        echo "  [WARN] $label exited $exit_code — $(tail -1 "$rivet_stderr")"
    fi

    # Parse time output (last line contains the format values)
    read -r LAST_ELAPSED LAST_USER LAST_SYS LAST_RSS < <(tail -1 "$time_out" || echo "0 0 0 0")
    LAST_RSS_MB=$(echo "scale=1; ${LAST_RSS:-0} / 1024" | bc 2>/dev/null || echo "?")

    # Count output files and total size
    LAST_FILES=$(find "$out_dir" -name "*.parquet" -o -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    if [[ $LAST_FILES -gt 0 ]]; then
        LAST_SIZE_MB=$(du -sm "$out_dir" 2>/dev/null | cut -f1 || echo "?")
    else
        LAST_SIZE_MB=0
    fi

    rm -f "$time_out" "$rivet_stderr"
}

# print_header prints a Markdown table header for the suite
print_header() {
    local title="$1"
    echo ""
    echo "## $title"
    echo ""
    printf "| %-30s | %8s | %8s | %8s | %8s | %8s | %8s |\n" \
        "Scenario" "Wall(s)" "User(s)" "Sys(s)" "RSS(MB)" "Files" "Size(MB)"
    printf "| %-30s | %8s | %8s | %8s | %8s | %8s | %8s |\n" \
        "---" "---:" "---:" "---:" "---:" "---:" "---:"
}

# print_row prints a single Markdown table row
print_row() {
    local label="$1"
    printf "| %-30s | %8s | %8s | %8s | %8s | %8s | %8s |\n" \
        "$label" "$LAST_ELAPSED" "$LAST_USER" "$LAST_SYS" "$LAST_RSS_MB" \
        "$LAST_FILES" "$LAST_SIZE_MB"
}

# ── suites ────────────────────────────────────────────────────────────────────

run_compression() {
    print_header "§4.3 Compression Profiles — bench_narrow (500 000 rows)"
    for profile in none fast balanced compact; do
        run_export "compression_${profile}" \
            "dev/bench/configs/compression_narrow.yaml" \
            "POSTGRES_URL=$POSTGRES_URL" \
            "BENCH_OUT=$BENCH_OUT" \
            "COMPRESSION=$profile"
        print_row "$profile"
    done
}

run_row_group() {
    print_header "§4.4 Row Group Targets — bench_wide (100 000 rows × 10 cols × 200 B)"
    for target in 32 64 128 256; do
        run_export "row_group_${target}mb" \
            "dev/bench/configs/row_group_wide.yaml" \
            "POSTGRES_URL=$POSTGRES_URL" \
            "BENCH_OUT=$BENCH_OUT" \
            "TARGET_MB=$target"
        print_row "${target} MB target"
    done
}

run_batch_memory() {
    print_header "§4.5 Batch Memory Policies — bench_wide (100 000 rows × 10 cols × 200 B)"
    for policy in warn auto_shrink; do
        run_export "batch_memory_${policy}" \
            "dev/bench/configs/batch_memory_wide.yaml" \
            "POSTGRES_URL=$POSTGRES_URL" \
            "BENCH_OUT=$BENCH_OUT" \
            "POLICY=$policy" \
            "CAP_MB=64"
        print_row "on_batch_memory_exceeded: $policy"
    done
    # Baseline: no cap (large cap so it never triggers)
    run_export "batch_memory_no_cap" \
        "dev/bench/configs/batch_memory_wide.yaml" \
        "POSTGRES_URL=$POSTGRES_URL" \
        "BENCH_OUT=$BENCH_OUT" \
        "POLICY=warn" \
        "CAP_MB=4096"
    print_row "no cap (warn, 4 GB)"
}

run_quality() {
    print_header "§4.6 Quality Uniqueness — bench_hc (200 000 rows, 2 UUID/email cols)"

    # With cap
    run_export "quality_cap" \
        "dev/bench/configs/quality_hc.yaml" \
        --export bench_quality_cap \
        "POSTGRES_URL=$POSTGRES_URL" \
        "BENCH_OUT=$BENCH_OUT"
    print_row "unique_max_entries: 50 000"

    # Without cap (separate export in the same config)
    run_export "quality_no_cap" \
        "dev/bench/configs/quality_hc.yaml" \
        --export bench_quality_no_cap \
        "POSTGRES_URL=$POSTGRES_URL" \
        "BENCH_OUT=$BENCH_OUT"
    print_row "no cap"
}

# ── entrypoint ────────────────────────────────────────────────────────────────

echo "# Rivet v0.5.x Benchmark Report"
echo ""
echo "Generated: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
echo "Binary:    $RIVET"
echo "Database:  $POSTGRES_URL"
echo ""

case "$SUITE" in
    compression)   run_compression ;;
    row_group)     run_row_group ;;
    batch_memory)  run_batch_memory ;;
    quality)       run_quality ;;
    all|*)
        run_compression
        run_row_group
        run_batch_memory
        run_quality
        ;;
esac

echo ""
echo "_All RSS measurements are peak RSS reported by GNU time (max resident set size)._"
echo "_Wall time includes source query + Parquet write + local flush._"
