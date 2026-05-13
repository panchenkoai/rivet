#!/usr/bin/env bash
# One-command live integration test runner.
#
# Usage:
#   ./dev/test_live.sh                      # run all live tests (postgres + mysql)
#   ./dev/test_live.sh pg                   # run only postgres live tests
#   ./dev/test_live.sh mysql                # run only mysql live tests
#   ./dev/test_live.sh filter <pattern>     # run tests matching pattern
#
# Requires docker compose to be running:
#   docker compose up -d postgres mysql
#
# Equivalent to CI step:
#   cargo test --release -- --ignored

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

filter="${1:-}"
pattern="${2:-}"

# Verify required services are reachable before spending time on a build.
check_service() {
    local name="$1" host="$2" port="$3"
    if ! nc -z -w1 "$host" "$port" 2>/dev/null; then
        echo "✗ $name not reachable on $host:$port"
        echo "  hint: docker compose up -d $name"
        return 1
    fi
    echo "✓ $name reachable on $host:$port"
}

case "$filter" in
  pg|postgres)
    check_service postgres 127.0.0.1 5432
    echo ""
    echo "▶ building release binary..."
    cargo build --release -q
    echo "▶ running postgres live tests..."
    cargo test --release -- --ignored postgres 2>&1 | grep -E "test .* ok|test .* FAILED|test .* ignored|running [0-9]+ test|FAILED|ok"
    ;;
  mysql)
    check_service mysql 127.0.0.1 3306
    echo ""
    echo "▶ building release binary..."
    cargo build --release -q
    echo "▶ running mysql live tests..."
    cargo test --release -- --ignored mysql 2>&1 | grep -E "test .* ok|test .* FAILED|test .* ignored|running [0-9]+ test|FAILED|ok"
    ;;
  filter)
    pattern="${2:?usage: test_live.sh filter <pattern>}"
    check_service postgres 127.0.0.1 5432 || true
    check_service mysql    127.0.0.1 3306 || true
    echo ""
    echo "▶ building release binary..."
    cargo build --release -q
    echo "▶ running live tests matching '$pattern'..."
    cargo test --release -- --ignored "$pattern"
    ;;
  *)
    check_service postgres 127.0.0.1 5432
    check_service mysql    127.0.0.1 3306
    echo ""
    echo "▶ building release binary..."
    cargo build --release -q
    echo "▶ running all live tests..."
    cargo test --release -- --ignored
    ;;
esac
