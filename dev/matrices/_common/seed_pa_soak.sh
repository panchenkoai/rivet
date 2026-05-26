#!/usr/bin/env bash
# Seed the pa_soak table (default 10_000 rows) on the primary PG container.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
FIXTURES="$ROOT/fixtures"
PG_CONTAINER="${PG_CONTAINER:-rivet-postgres-1}"
ROWS="${ROWS:-10000}"

sql=$(sed "s/__ROWS__/$ROWS/g" "$FIXTURES/pa_soak_pg.sql")
docker exec -i "$PG_CONTAINER" psql -U rivet -d rivet -f - >/dev/null <<< "$sql"
echo "pa_soak seeded with $(docker exec "$PG_CONTAINER" psql -U rivet -d rivet -tAc 'SELECT COUNT(*) FROM pa_soak;') rows"
