#!/usr/bin/env bash
# Full end-to-end test matrix across every supported PG/MySQL version.
#
# For each target, the script:
#   1. Seeds the corresponding DB (small fixture, ~500 users).
#   2. Re-points `dev/e2e/run_e2e.sh` via `RIVET_PG_URL` / `RIVET_MYSQL_URL`.
#   3. Runs the ENTIRE e2e suite (doctor, preflight, all modes, compression,
#      reconcile, recovery, state, init, …).
#
# S3/GCS sections auto-skip when MinIO / fake-gcs aren't reachable — they
# aren't version-sensitive so running them once against PG 16 is enough.
#
# Usage:
#   docker compose --profile legacy up -d postgres-12 postgres-13 postgres-14 postgres-15 mysql-57
#   docker compose up -d postgres mysql minio fake-gcs
#   cargo build --release --bin rivet --bin seed
#   bash dev/legacy/run_full_matrix.sh
#
# Per-target override:
#   TARGETS="pg-12 pg-16" bash dev/legacy/run_full_matrix.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

RIVET="${RIVET:-$(pwd)/target/release/rivet}"
SEED="${SEED:-$(pwd)/target/release/seed}"
export RIVET

TARGETS_DEFAULT="pg-12 pg-13 pg-14 pg-15 pg-16 mysql-57 mysql-80"
TARGETS="${TARGETS:-$TARGETS_DEFAULT}"

RESULTS_DIR="/tmp/rivet_full_matrix"
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

SUMMARY=()
OVERALL_EXIT=0

probe_tcp() {
    local port="$1"
    (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null
    local rc=$?
    exec 3<&- 2>/dev/null || true
    exec 3>&- 2>/dev/null || true
    return $rc
}

seed_pg() {
    local url="$1"
    "$SEED" --target postgres --pg-url "$url" \
        --users 500 --orders-per-user 4 --events-per-user 10 \
        --page-views 500 --content-items 100 >/dev/null 2>&1
}

seed_mysql() {
    local url="$1"
    "$SEED" --target mysql --mysql-url "$url" \
        --users 500 --orders-per-user 4 --events-per-user 10 \
        --page-views 500 --content-items 100 >/dev/null 2>&1
}

run_target() {
    local target="$1" pg_url="$2" my_url="$3" seed_kind="$4" seed_url="$5"
    local log="$RESULTS_DIR/${target}.log"

    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║  TARGET: ${target}"
    echo "║    PG  URL → ${pg_url}"
    echo "║    MY  URL → ${my_url}"
    echo "╚════════════════════════════════════════════════════════════╝"

    # Reachability probes against the specific port (derived from URL).
    local pg_port my_port
    pg_port="$(echo "$pg_url" | sed -E 's|.*@[^:]+:([0-9]+)/.*|\1|')"
    my_port="$(echo "$my_url" | sed -E 's|.*@[^:]+:([0-9]+)/.*|\1|')"

    if ! probe_tcp "$pg_port"; then
        echo "  SKIP: PG server on port ${pg_port} unreachable"
        SUMMARY+=("${target}: SKIP (pg unreachable)")
        return 0
    fi
    # MySQL is not hard-required (run_e2e auto-skips when its probe fails),
    # but we still need seeding if it IS up.

    # Seed the target DB fresh so incremental cursors and file counts are
    # deterministic for each matrix pass.
    case "$seed_kind" in
        pg)
            if ! seed_pg "$seed_url"; then
                echo "  FAIL: seed (pg: $seed_url)"
                SUMMARY+=("${target}: FAIL (seed)")
                OVERALL_EXIT=1
                return 1
            fi
            ;;
        mysql)
            if ! seed_mysql "$seed_url"; then
                echo "  FAIL: seed (mysql: $seed_url)"
                SUMMARY+=("${target}: FAIL (seed)")
                OVERALL_EXIT=1
                return 1
            fi
            ;;
    esac
    echo "  seed: ok"

    # Force the e2e suite to talk to the target URLs.
    export RIVET_PG_URL="$pg_url"
    export RIVET_MYSQL_URL="$my_url"

    if bash dev/e2e/run_e2e.sh >"$log" 2>&1; then
        local pass fail skip
        pass=$(grep -E "^PASS:" "$log" | awk '{print $2}')
        fail=$(grep -E "^PASS:" "$log" | awk '{print $5}')
        skip=$(grep -E "^PASS:" "$log" | awk '{print $8}')
        echo "  RESULT: ${pass} passed, ${fail} failed, ${skip} skipped"
        SUMMARY+=("${target}: PASS (${pass} passed, ${skip} skipped, log: ${log})")
    else
        local pass fail skip
        pass=$(grep -E "^PASS:" "$log" | awk '{print $2}')
        fail=$(grep -E "^PASS:" "$log" | awk '{print $5}')
        skip=$(grep -E "^PASS:" "$log" | awk '{print $8}')
        echo "  RESULT: ${pass:-?} passed, ${fail:-?} FAILED, ${skip:-?} skipped — see $log"
        echo "  FAILURES:"
        grep "^FAIL " "$log" | sed 's|^|    |'
        SUMMARY+=("${target}: FAIL (${pass:-?} passed, ${fail:-?} failed, log: ${log})")
        OVERALL_EXIT=1
    fi
}

# Build the default MySQL URL used for PG-targeted runs. Primary MySQL 8.0 runs
# on 3306; we reuse it so MySQL-specific e2e sections still exercise something.
PRIMARY_MY_URL="mysql://rivet:rivet@localhost:3306/rivet"
LEGACY_MY_URL="mysql://rivet:rivet@localhost:3357/rivet"
PRIMARY_PG_URL="postgresql://rivet:rivet@localhost:5432/rivet"

for target in $TARGETS; do
    case "$target" in
        pg-12) run_target "$target" "postgresql://rivet:rivet@localhost:5412/rivet" "$PRIMARY_MY_URL" pg "postgresql://rivet:rivet@localhost:5412/rivet" ;;
        pg-13) run_target "$target" "postgresql://rivet:rivet@localhost:5413/rivet" "$PRIMARY_MY_URL" pg "postgresql://rivet:rivet@localhost:5413/rivet" ;;
        pg-14) run_target "$target" "postgresql://rivet:rivet@localhost:5414/rivet" "$PRIMARY_MY_URL" pg "postgresql://rivet:rivet@localhost:5414/rivet" ;;
        pg-15) run_target "$target" "postgresql://rivet:rivet@localhost:5415/rivet" "$PRIMARY_MY_URL" pg "postgresql://rivet:rivet@localhost:5415/rivet" ;;
        pg-16) run_target "$target" "$PRIMARY_PG_URL" "$PRIMARY_MY_URL" pg "$PRIMARY_PG_URL" ;;
        mysql-57)
            # MySQL-targeted run: keep PG on the primary so pg-only sections
            # have a working server and we isolate the MySQL variable.
            run_target "$target" "$PRIMARY_PG_URL" "$LEGACY_MY_URL" mysql "$LEGACY_MY_URL"
            ;;
        mysql-80)
            run_target "$target" "$PRIMARY_PG_URL" "$PRIMARY_MY_URL" mysql "$PRIMARY_MY_URL"
            ;;
        *)
            echo "  SKIP: unknown target '$target' (expected pg-12|pg-13|pg-14|pg-15|pg-16|mysql-57|mysql-80)"
            ;;
    esac
done

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "Full compatibility matrix summary"
echo "══════════════════════════════════════════════════════════════"
for line in "${SUMMARY[@]}"; do
    echo "  $line"
done
echo ""

if [ "$OVERALL_EXIT" -ne 0 ]; then
    echo "ONE OR MORE TARGETS FAILED."
    exit 1
fi
echo "All targets passed."
