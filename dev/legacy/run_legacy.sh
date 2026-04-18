#!/usr/bin/env bash
# Compatibility smoke matrix for legacy Postgres and MySQL versions.
#
# Before running:
#   docker compose --profile legacy up -d postgres-12 postgres-13 postgres-14 postgres-15 mysql-57
#   cargo build --release --bin rivet --bin seed
#
# For each legacy server this script runs the same flow the primary e2e covers
# in miniature: doctor → check → full → chunked → incremental (and time_window
# for Postgres). It's meant to be a fast, readable compat gate — not a full
# performance or soak test.
#
# Usage:
#   bash dev/legacy/run_legacy.sh
#   TARGETS="pg-12 pg-15 mysql-57" bash dev/legacy/run_legacy.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

RIVET="${RIVET:-$(pwd)/target/release/rivet}"
SEED="${SEED:-$(pwd)/target/release/seed}"
OUT="dev/legacy/output"
RESULTS="/tmp/rivet_legacy_results.txt"
rm -f "$RESULTS"

PASS=0
FAIL=0
SKIP=0

pass() { echo "  PASS  $*" | tee -a "$RESULTS"; PASS=$((PASS+1)); }
fail() { echo "  FAIL  $*" | tee -a "$RESULTS"; FAIL=$((FAIL+1)); }
skip() { echo "  SKIP  $*" | tee -a "$RESULTS"; SKIP=$((SKIP+1)); }
section() { echo ""; echo "▒▒▒▒▒▒ $* ▒▒▒▒▒▒"; }

# macOS ships bash 3.2 which lacks associative arrays — a plain case statement
# is the portable way to map target → port.
TARGETS_DEFAULT="pg-12 pg-13 pg-14 pg-15 mysql-57"
TARGETS="${TARGETS:-$TARGETS_DEFAULT}"

rm -rf "$OUT" dev/legacy/.rivet_state.db*
mkdir -p "$OUT"

for target in $TARGETS; do
    case "$target" in
        pg-12)   port="5412"; url="postgresql://rivet:rivet@localhost:5412/rivet"; kind="postgres" ;;
        pg-13)   port="5413"; url="postgresql://rivet:rivet@localhost:5413/rivet"; kind="postgres" ;;
        pg-14)   port="5414"; url="postgresql://rivet:rivet@localhost:5414/rivet"; kind="postgres" ;;
        pg-15)   port="5415"; url="postgresql://rivet:rivet@localhost:5415/rivet"; kind="postgres" ;;
        mysql-57) port="3357"; url="mysql://rivet:rivet@localhost:3357/rivet"; kind="mysql" ;;
        *)
            section "$target"
            fail "unknown target '$target' (expected pg-12|pg-13|pg-14|pg-15|mysql-57)"
            continue
            ;;
    esac

    section "$target @ localhost:${port}"

    # Reachability probe — prefer bash TCP test so we don't depend on a
    # specific client (mysqladmin on macOS ≥ 9.0 lacks native_password).
    if ! (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
        skip "$target: port ${port} unreachable (is \`docker compose --profile legacy up -d $target\` running?)"
        continue
    fi
    exec 3<&- 2>/dev/null || true
    exec 3>&- 2>/dev/null || true

    if [ "$kind" = "postgres" ]; then
        cfg="dev/legacy/pg_legacy.yaml"
        seed_args=(--target postgres --pg-url "$url" --users 500 --orders-per-user 4 --events-per-user 10 --page-views 500 --content-items 100)
    else
        cfg="dev/legacy/mysql_legacy.yaml"
        seed_args=(--target mysql --mysql-url "$url" --users 500 --orders-per-user 4 --events-per-user 10 --page-views 500 --content-items 100)
    fi

    # One fresh seed per target so incremental cursors start clean.
    if "$SEED" "${seed_args[@]}" >/dev/null 2>&1; then
        pass "$target: seed"
    else
        fail "$target: seed"
        continue
    fi

    rm -f dev/legacy/.rivet_state.db*
    export DATABASE_URL="$url"

    "$RIVET" doctor --config "$cfg" >/dev/null 2>&1 && pass "$target: doctor" || fail "$target: doctor"
    "$RIVET" check  --config "$cfg" >/dev/null 2>&1 && pass "$target: check"  || fail "$target: check"

    "$RIVET" run --config "$cfg" --export legacy_users_full --validate --reconcile >/dev/null 2>&1 \
        && pass "$target: users full (validate+reconcile)" \
        || fail "$target: users full"

    "$RIVET" run --config "$cfg" --export legacy_orders_chunked --validate --reconcile >/dev/null 2>&1 \
        && pass "$target: orders chunked (validate+reconcile)" \
        || fail "$target: orders chunked"

    "$RIVET" run --config "$cfg" --export legacy_orders_incremental --validate >/dev/null 2>&1 \
        && pass "$target: orders incremental run1" \
        || fail "$target: orders incremental run1"

    # Re-run incremental — should succeed with 0 new rows.
    "$RIVET" run --config "$cfg" --export legacy_orders_incremental >/dev/null 2>&1 \
        && pass "$target: orders incremental rerun (no new rows)" \
        || fail "$target: orders incremental rerun"

    if [ "$kind" = "postgres" ]; then
        "$RIVET" run --config "$cfg" --export legacy_events_timewindow >/dev/null 2>&1 \
            && pass "$target: events time_window" \
            || fail "$target: events time_window"
    fi

    # init should be able to introspect the legacy schema and emit a valid YAML.
    scratch="dev/legacy/.init_${target}.yaml"
    rm -f "$scratch"
    "$RIVET" init --source "$url" --table users -o "$scratch" >/dev/null 2>&1 \
        && grep -q "exports:" "$scratch" \
        && pass "$target: init --table users" \
        || fail "$target: init --table users"
done

echo ""
echo "══════ Summary ══════"
echo "PASS: $PASS | FAIL: $FAIL | SKIP: $SKIP"
echo "Total: $((PASS + FAIL + SKIP))"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "FAILURES:"
    grep "FAIL" "$RESULTS"
    exit 1
fi

echo "All legacy compatibility checks passed."
