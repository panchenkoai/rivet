#!/usr/bin/env bash
# Seed the 30-row pa_audit fixture on primary PG and/or MySQL containers.
#
# Usage:
#   seed_pa_audit.sh                 # both engines (default)
#   seed_pa_audit.sh --engines=postgres
#   seed_pa_audit.sh --engines=mysql
#   seed_pa_audit.sh --engines=postgres,mysql
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
FIXTURES="$ROOT/fixtures"

PG_CONTAINER="${PG_CONTAINER:-rivet-postgres-1}"
MY_CONTAINER="${MY_CONTAINER:-rivet-mysql-1}"
ENGINES="postgres,mysql"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engines=*) ENGINES="${1#*=}"; shift ;;
    -h|--help)
      echo "Usage: seed_pa_audit.sh [--engines=postgres|mysql|postgres,mysql]" >&2
      exit 0
      ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

seed_pg() {
  docker exec -i "$PG_CONTAINER" psql -U rivet -d rivet \
    -f - >/dev/null < "$FIXTURES/pa_audit_pg.sql"
  echo "PG pa_audit: $(docker exec "$PG_CONTAINER" psql -U rivet -d rivet -tAc 'SELECT COUNT(*) FROM pa_audit;') rows"
}

seed_mysql() {
  docker exec -i "$MY_CONTAINER" mysql -urivet -privet rivet \
    < "$FIXTURES/pa_audit_mysql.sql" 2>/dev/null
  for i in $(seq 1 30); do
    docker exec "$MY_CONTAINER" mysql -urivet -privet rivet \
      -e "INSERT INTO pa_audit (id, name) VALUES ($i, 'row_$i');" 2>/dev/null
  done
  echo "MySQL pa_audit: $(docker exec "$MY_CONTAINER" mysql -urivet -privet -BN -e 'SELECT COUNT(*) FROM rivet.pa_audit;' 2>/dev/null) rows"
}

IFS=',' read -ra ENGINE_LIST <<< "$ENGINES"
for engine in "${ENGINE_LIST[@]}"; do
  case "$engine" in
    postgres|pg) seed_pg ;;
    mysql|my) seed_mysql ;;
    *) echo "Unknown engine '$engine'" >&2; exit 2 ;;
  esac
done
