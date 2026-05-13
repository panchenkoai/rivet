#!/usr/bin/env bash
# One-command benchmark runner with optional baseline save/compare.
#
# Usage:
#   ./dev/bench.sh                        # run all groups, print results
#   ./dev/bench.sh save <tag>             # run + save baseline (e.g. "before-refactor")
#   ./dev/bench.sh compare <tag>          # run + compare against saved baseline
#   ./dev/bench.sh group <name>           # run a single benchmark group
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
