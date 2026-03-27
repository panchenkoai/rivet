#!/usr/bin/env bash
set -euo pipefail

# E2E test: schema evolution detection via real database migrations
# Requires: Docker containers running (docker compose up -d), rivet built
#
# Usage: bash dev/test_schema_evolution.sh

RIVET="cargo run --release --bin rivet --"
CONFIG="dev/test_schema_evolution.yaml"
PSQL="docker compose exec -T postgres psql -U rivet -q"
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
        red "  FAIL: $label (expected '$pattern' in output)"
        echo "  Output was: $output"
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

# ─── Setup ────────────────────────────────────────────────────

header "Setup: create test table"
mkdir -p dev/output/schema
rm -f dev/.rivet_state.db

$PSQL <<'SQL'
DROP TABLE IF EXISTS schema_test_table;
CREATE TABLE schema_test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    age INT
);
INSERT INTO schema_test_table (name, email, age) VALUES
    ('Alice', 'alice@test.com', 30),
    ('Bob', 'bob@test.com', 25);
SQL
echo "  Table created with columns: id, name, email, age"

# ─── Step 1: First export (baseline) ─────────────────────────

header "Step 1: First export -- baseline schema stored"
OUTPUT=$(RUST_LOG=warn $RIVET run --config $CONFIG 2>&1)
assert_not_contains "$OUTPUT" "schema changed" "no schema change on first run"

# ─── Step 2: Same schema, no change ──────────────────────────

header "Step 2: Same schema -- no change expected"
OUTPUT=$(RUST_LOG=warn $RIVET run --config $CONFIG 2>&1)
assert_not_contains "$OUTPUT" "schema changed" "no schema change on identical schema"

# ─── Step 3: ADD COLUMN ──────────────────────────────────────

header "Step 3: Migration -- ADD COLUMN phone"
$PSQL -c "ALTER TABLE schema_test_table ADD COLUMN phone VARCHAR(20);"
$PSQL -c "UPDATE schema_test_table SET phone = '+1-555-0100' WHERE id = 1;"

OUTPUT=$(RUST_LOG=warn $RIVET run --config $CONFIG 2>&1)
assert_contains "$OUTPUT" "schema changed" "schema change detected after ADD COLUMN"
assert_contains "$OUTPUT" "added" "reports added columns"
assert_contains "$OUTPUT" "phone" "mentions phone column"

# ─── Step 4: Same schema after add, no change ────────────────

header "Step 4: Same schema after migration -- no change"
OUTPUT=$(RUST_LOG=warn $RIVET run --config $CONFIG 2>&1)
assert_not_contains "$OUTPUT" "schema changed" "no change after schema stabilized"

# ─── Step 5: DROP COLUMN ─────────────────────────────────────

header "Step 5: Migration -- DROP COLUMN age"
$PSQL -c "ALTER TABLE schema_test_table DROP COLUMN age;"

OUTPUT=$(RUST_LOG=warn $RIVET run --config $CONFIG 2>&1)
assert_contains "$OUTPUT" "schema changed" "schema change detected after DROP COLUMN"
assert_contains "$OUTPUT" "removed" "reports removed columns"
assert_contains "$OUTPUT" "age" "mentions age column"

# ─── Step 6: ALTER TYPE ──────────────────────────────────────

header "Step 6: Migration -- ALTER COLUMN id type to BIGINT"
$PSQL -c "ALTER TABLE schema_test_table ALTER COLUMN id TYPE BIGINT;"

OUTPUT=$(RUST_LOG=warn $RIVET run --config $CONFIG 2>&1)
assert_contains "$OUTPUT" "schema changed" "schema change detected after ALTER TYPE"
assert_contains "$OUTPUT" "type changed" "reports type changes"

# ─── Step 7: Multiple changes at once ────────────────────────

header "Step 7: Migration -- ADD status + DROP phone simultaneously"
$PSQL -c "ALTER TABLE schema_test_table ADD COLUMN status VARCHAR(20) DEFAULT 'active';"
$PSQL -c "ALTER TABLE schema_test_table DROP COLUMN phone;"

OUTPUT=$(RUST_LOG=warn $RIVET run --config $CONFIG 2>&1)
assert_contains "$OUTPUT" "schema changed" "schema change detected on combined migration"
assert_contains "$OUTPUT" "added" "reports added column"
assert_contains "$OUTPUT" "status" "mentions status"
assert_contains "$OUTPUT" "removed" "reports removed column"
assert_contains "$OUTPUT" "phone" "mentions phone removed"

# ─── Step 8: Verify metrics recorded ─────────────────────────

header "Step 8: Verify metrics recorded for all runs"
OUTPUT=$($RIVET metrics --config $CONFIG 2>&1)
RUNS=$(echo "$OUTPUT" | grep -c "schema_test" || true)
assert_contains "$RUNS" "7" "7 runs recorded in metrics (or more)"

# ─── Cleanup ─────────────────────────────────────────────────

header "Cleanup"
$PSQL -c "DROP TABLE IF EXISTS schema_test_table;"
rm -f dev/.rivet_state.db
echo "  Test table dropped, state cleaned"

# ─── Summary ─────────────────────────────────────────────────

echo ""
echo "================================"
echo "Results: $PASS passed, $FAIL failed"
echo "================================"

if [ $FAIL -gt 0 ]; then
    exit 1
fi
