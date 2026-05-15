#!/usr/bin/env bash
# One-command Criterion benchmark runner with optional baseline save/compare.
#
# Usage:
#   ./dev/scripts/bench.sh                        # run all groups, print results
#   ./dev/scripts/bench.sh save <tag>             # run + save baseline (e.g. "v0_5_0")
#   ./dev/scripts/bench.sh compare <tag>          # run + compare against saved baseline
#   ./dev/scripts/bench.sh group <name>           # run a single benchmark group
#
# Benchmark suites (Criterion, in-process microbenchmarks):
#   hot_paths       — CSV write, hash column, Parquet flush, MySQL parse
#   resource_aware  — auto_shrink splits, compression codecs, row group sizing,
#                     quality uniqueness cap  (Phase 2 stabilization)
#
# End-to-end resource benchmarks (wall time + peak RSS against live Postgres):
#   ./dev/bench/run_bench.sh                      # all e2e suites
#   ./dev/bench/run_bench.sh compression          # §4.3 compression profiles
#   ./dev/bench/run_bench.sh row_group            # §4.4 row group targets
#   ./dev/bench/run_bench.sh batch_memory         # §4.5 batch memory policies
#   ./dev/bench/run_bench.sh quality              # §4.6 quality uniqueness cap
#
# Baselines are stored in target/criterion/ by Criterion automatically.
# HTML reports: target/criterion/<group>/report/index.html

set -euo pipefail

cmd="${1:-run}"
tag="${2:-main}"

case "$cmd" in
  save)
    echo "▶ saving baseline '$tag'"
    cargo bench -- --save-baseline "$tag"
    echo "✓ baseline '$tag' saved (target/criterion/)"
    ;;
  compare)
    echo "▶ comparing against baseline '$tag'"
    cargo bench -- --baseline "$tag"
    ;;
  group)
    group="${2:?usage: bench.sh group <group_name>}"
    echo "▶ running benchmark group '$group'"
    cargo bench -- "$group"
    ;;
  run|*)
    echo "▶ running all benchmark groups"
    cargo bench
    echo ""
    echo "HTML reports: target/criterion/*/report/index.html"
    ;;
esac
