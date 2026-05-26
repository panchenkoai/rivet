#!/usr/bin/env bash
# Seed pa_audit on every supported PG (12–16) and MySQL (5.7, 8.0) container.
# Skips unreachable containers (legacy profile not started).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
FIXTURES="$ROOT/fixtures"

seed_pg() {
  local container="$1"
  local label="$2"
  if ! docker exec "$container" psql -U rivet -d rivet -c "SELECT 1" >/dev/null 2>&1; then
    echo "  SKIP: $label ($container) unreachable"
    return 0
  fi
  docker exec -i "$container" psql -U rivet -d rivet \
    -f - >/dev/null < "$FIXTURES/pa_audit_pg.sql"
  local n
  n=$(docker exec "$container" psql -U rivet -d rivet -tAc 'SELECT COUNT(*) FROM pa_audit;')
  echo "  $label: pa_audit = $n rows"
}

seed_mysql() {
  local container="$1"
  local label="$2"
  if ! docker exec "$container" mysql -urivet -privet rivet -e "SELECT 1" >/dev/null 2>&1; then
    echo "  SKIP: $label ($container) unreachable"
    return 0
  fi
  docker exec -i "$container" mysql -urivet -privet rivet \
    < "$FIXTURES/pa_audit_mysql.sql" 2>/dev/null
  for i in $(seq 1 30); do
    docker exec "$container" mysql -urivet -privet rivet \
      -e "INSERT INTO pa_audit (id, name) VALUES ($i, 'row_$i');" 2>/dev/null
  done
  local n
  n=$(docker exec "$container" mysql -urivet -privet -BN -e 'SELECT COUNT(*) FROM rivet.pa_audit;' 2>/dev/null)
  echo "  $label: pa_audit = $n rows"
}

echo "Seeding all PG versions:"
seed_pg rivet-postgres-12-1 pg-12
seed_pg rivet-postgres-13-1 pg-13
seed_pg rivet-postgres-14-1 pg-14
seed_pg rivet-postgres-15-1 pg-15
seed_pg rivet-postgres-1    pg-16

echo "Seeding all MySQL versions:"
seed_mysql rivet-mysql-1    mysql-80
seed_mysql rivet-mysql-57-1 mysql-57
