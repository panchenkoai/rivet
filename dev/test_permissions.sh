#!/usr/bin/env bash
set -euo pipefail

# E2E test: permission denied errors are handled correctly (no retries, instant fail)
# Requires: Docker containers running (docker compose up -d), rivet built
#
# Usage: bash dev/test_permissions.sh

RIVET="cargo run --release --bin rivet --"
PSQL="docker compose exec -T postgres psql -U rivet -q"
MYSQL="docker compose exec -T mysql mysql -uroot -privet rivet -q"
PASS=0
FAIL=0

green()  { echo -e "\033[32m$1\033[0m"; }
red()    { echo -e "\033[31m$1\033[0m"; }
header() { echo ""; echo "=== $1 ==="; }

assert_contains() {
    local output="$1" pattern="$2" label="$3"
    if echo "$output" | grep -qi "$pattern"; then
        green "  PASS: $label"
        PASS=$((PASS + 1))
    else
        red "  FAIL: $label (expected '$pattern')"
        echo "  Output: $output"
        FAIL=$((FAIL + 1))
    fi
}

assert_not_contains() {
    local output="$1" pattern="$2" label="$3"
    if echo "$output" | grep -qi "$pattern"; then
        red "  FAIL: $label (unexpected '$pattern' found)"
        FAIL=$((FAIL + 1))
    else
        green "  PASS: $label"
        PASS=$((PASS + 1))
    fi
}

assert_fast() {
    local seconds="$1" max="$2" label="$3"
    if [ "$seconds" -le "$max" ]; then
        green "  PASS: $label (${seconds}s <= ${max}s)"
        PASS=$((PASS + 1))
    else
        red "  FAIL: $label (${seconds}s > ${max}s -- likely retried)"
        FAIL=$((FAIL + 1))
    fi
}

# ─── Setup: Create restricted users ──────────────────────────

header "Setup: PostgreSQL restricted users"

$PSQL <<'SQL'
-- Clean up existing roles
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM rivet_noaccess CASCADE;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM rivet_partial CASCADE;
REVOKE USAGE ON SCHEMA public FROM rivet_partial;
REVOKE CONNECT ON DATABASE rivet FROM rivet_partial;
DROP ROLE IF EXISTS rivet_noaccess;
DROP ROLE IF EXISTS rivet_partial;

-- User with NO access
CREATE ROLE rivet_noaccess LOGIN PASSWORD 'noaccess';

-- User with SELECT on users only
CREATE ROLE rivet_partial LOGIN PASSWORD 'partial';
GRANT CONNECT ON DATABASE rivet TO rivet_partial;
GRANT USAGE ON SCHEMA public TO rivet_partial;
GRANT SELECT ON users TO rivet_partial;
SQL
echo "  PG users created: rivet_noaccess, rivet_partial"

header "Setup: MySQL restricted users"

$MYSQL <<'SQL'
DROP USER IF EXISTS 'rivet_noaccess'@'%';
CREATE USER 'rivet_noaccess'@'%' IDENTIFIED BY 'noaccess';

DROP USER IF EXISTS 'rivet_partial'@'%';
CREATE USER 'rivet_partial'@'%' IDENTIFIED BY 'partial';
GRANT SELECT ON rivet.users TO 'rivet_partial'@'%';

FLUSH PRIVILEGES;
SQL
echo "  MySQL users created: rivet_noaccess, rivet_partial"

mkdir -p dev/output/perms

# ─── PG: No access at all ────────────────────────────────────

header "PG: No access -- should fail instantly, no retry"

START=$(date +%s)
OUTPUT=$(RUST_LOG=error $RIVET run --config dev/test_pg_noaccess.yaml 2>&1 || true)
END=$(date +%s)
ELAPSED=$((END - START))

assert_contains "$OUTPUT" "permission denied" "PG no-access error message"
assert_not_contains "$OUTPUT" "retry" "PG no-access did not retry"
assert_fast "$ELAPSED" 3 "PG no-access completed quickly"

# ─── PG: Partial access ──────────────────────────────────────

header "PG: Partial access -- users OK, orders denied"

OUTPUT=$(RUST_LOG=error $RIVET run --config dev/test_pg_partial.yaml 2>&1 || true)

assert_not_contains "$OUTPUT" "test_users_ok.*failed" "PG users export succeeded"
assert_contains "$OUTPUT" "permission denied for table orders" "PG orders correctly denied"
assert_not_contains "$OUTPUT" "retry" "PG partial did not retry on permission error"

# ─── MySQL: No access at all ─────────────────────────────────

header "MySQL: No access -- should fail instantly"

START=$(date +%s)
OUTPUT=$(RUST_LOG=error $RIVET run --config dev/test_mysql_noaccess.yaml 2>&1 || true)
END=$(date +%s)
ELAPSED=$((END - START))

assert_contains "$OUTPUT" "access denied\|Access denied" "MySQL no-access error message"
assert_not_contains "$OUTPUT" "retry" "MySQL no-access did not retry"
assert_fast "$ELAPSED" 3 "MySQL no-access completed quickly"

# ─── MySQL: Partial access ───────────────────────────────────

header "MySQL: Partial access -- users OK, orders denied"

OUTPUT=$(RUST_LOG=error $RIVET run --config dev/test_mysql_partial.yaml 2>&1 || true)

assert_not_contains "$OUTPUT" "test_mysql_users_ok.*failed" "MySQL users export succeeded"
assert_contains "$OUTPUT" "denied\|SELECT command denied" "MySQL orders correctly denied"
assert_not_contains "$OUTPUT" "retry" "MySQL partial did not retry"

# ─── PG: Wrong password ──────────────────────────────────────

header "PG: Wrong password -- connection should fail, no retry loop"

START=$(date +%s)
OUTPUT=$(RUST_LOG=error $RIVET run --config dev/test_pg_wrongpass.yaml 2>&1 || true)
END=$(date +%s)
ELAPSED=$((END - START))

assert_contains "$OUTPUT" "password\|authentication" "PG wrong password error message"
assert_fast "$ELAPSED" 5 "PG wrong password completed quickly"

# ─── Cleanup ─────────────────────────────────────────────────

header "Cleanup"

$PSQL <<'SQL'
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM rivet_noaccess CASCADE;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM rivet_partial CASCADE;
REVOKE USAGE ON SCHEMA public FROM rivet_partial;
REVOKE CONNECT ON DATABASE rivet FROM rivet_partial;
DROP ROLE IF EXISTS rivet_noaccess;
DROP ROLE IF EXISTS rivet_partial;
SQL

$MYSQL <<'SQL'
DROP USER IF EXISTS 'rivet_noaccess'@'%';
DROP USER IF EXISTS 'rivet_partial'@'%';
FLUSH PRIVILEGES;
SQL

echo "  Restricted users dropped"

# ─── Summary ─────────────────────────────────────────────────

echo ""
echo "================================"
echo "Results: $PASS passed, $FAIL failed"
echo "================================"

if [ $FAIL -gt 0 ]; then
    exit 1
fi
