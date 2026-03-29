#!/usr/bin/env bash
# Configure Toxiproxy proxies after `docker compose up -d toxiproxy`.
# Requires: curl, jq (optional for pretty-printing).
set -euo pipefail

TOXI=http://localhost:8474

echo "→ Creating Postgres proxy (localhost:15432 → postgres:5432)..."
curl -sf -X POST "$TOXI/proxies" -H 'Content-Type: application/json' -d '{
  "name": "pg",
  "listen": "0.0.0.0:15432",
  "upstream": "postgres:5432"
}' && echo " OK" || echo " (already exists)"

echo "→ Creating MySQL proxy (localhost:13306 → mysql:3306)..."
curl -sf -X POST "$TOXI/proxies" -H 'Content-Type: application/json' -d '{
  "name": "mysql",
  "listen": "0.0.0.0:13306",
  "upstream": "mysql:3306"
}' && echo " OK" || echo " (already exists)"

echo ""
echo "Proxies configured. Inject faults with:"
echo "  # Add 2s latency"
echo "  curl -X POST $TOXI/proxies/pg/toxics -d '{\"name\":\"latency\",\"type\":\"latency\",\"attributes\":{\"latency\":2000}}'"
echo "  # Reset connection after 5KB"
echo "  curl -X POST $TOXI/proxies/pg/toxics -d '{\"name\":\"limit\",\"type\":\"limit_data\",\"attributes\":{\"bytes\":5000}}'"
echo "  # Remove all toxics"
echo "  curl -X DELETE $TOXI/proxies/pg/toxics/latency"
echo "  curl -X DELETE $TOXI/proxies/pg/toxics/limit"
