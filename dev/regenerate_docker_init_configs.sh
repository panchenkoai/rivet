#!/usr/bin/env bash
# Regenerate YAML scaffolds from docker-compose Postgres + MySQL (dev/*/init.sql).
#
# Prerequisites:
#   docker compose up -d postgres mysql
#   (optional) seed data — row estimates in comments reflect pg_class / TABLE_ROWS
#
# Usage: from repo root —  bash dev/regenerate_docker_init_configs.sh

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PG_URL="${PG_URL:-postgresql://rivet:rivet@localhost:5432/rivet?sslmode=disable}"
MYSQL_URL="${MYSQL_URL:-mysql://rivet:rivet@localhost:3306/rivet}"

TABLES=(
  users orders events page_views content_items orders_sparse orders_sparse_for_export
)

mkdir -p dev/init_generated/pg dev/init_generated/mysql

for t in "${TABLES[@]}"; do
  cargo run --release -q -- init --source "$PG_URL" --table "$t" -o "dev/init_generated/pg/${t}.yaml"
done

for t in "${TABLES[@]}"; do
  cargo run --release -q -- init --source "$MYSQL_URL" --table "$t" -o "dev/init_generated/mysql/${t}.yaml"
done

# One file per database schema: all tables/views in public (PG) or rivet (MySQL from URL).
cargo run --release -q -- init --source "$PG_URL" --schema public -o dev/init_generated/pg/schema_public.yaml
cargo run --release -q -- init --source "$MYSQL_URL" -o dev/init_generated/mysql/schema_rivet.yaml

# Match other dev configs (./dev/output is gitignored)
find dev/init_generated -name '*.yaml' -exec sed -i.bak 's|path: ./output|path: ./dev/output|' {} \;
find dev/init_generated -name '*.bak' -delete

echo "Done. Set DATABASE_URL then, e.g.:  rivet check --config dev/init_generated/pg/schema_public.yaml"
