#!/usr/bin/env bash
set -euo pipefail

# Rivet export to local fake-gcs-server (see docker-compose service fake-gcs).
#
# Usage:
#   ./dev/run_gcs_fake_export.sh
#   ./dev/run_gcs_fake_export.sh check
#
# Prerequisites:
#   docker compose up -d fake-gcs postgres
#   cargo run --bin seed -- --target postgres --users 100   # if orders is empty

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export RUST_LOG="${RUST_LOG:-info}"
BUCKET="${FAKE_GCS_BUCKET:-rivet-fake}"
BASE="${FAKE_GCS_URL:-http://127.0.0.1:4443}"

echo "Ensuring bucket '${BUCKET}' exists on fake-gcs-server..."
if curl -sf -X POST "${BASE}/storage/v1/b?project=test" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"${BUCKET}\"}" >/dev/null; then
  echo "  created bucket ${BUCKET}"
else
  # 409 = already exists; other errors still try rivet run
  echo "  (bucket may already exist or server not ready)"
fi

CONFIG="${RIVET_CONFIG:-dev/rivet_gcs_fake_test.yaml}"
SUB="${1:-run}"
shift || true

exec cargo run --bin rivet -- "$SUB" --config "$CONFIG" "$@"
