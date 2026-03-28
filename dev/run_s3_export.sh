#!/usr/bin/env bash
set -euo pipefail

# Run rivet S3 export against local MinIO.
#
# Usage:
#   ./dev/run_s3_export.sh              # rivet run
#   ./dev/run_s3_export.sh check        # preflight
#
# Prerequisites:
#   docker compose up -d minio postgres
#   cargo run --bin seed -- --target postgres --users 1000   # if DB is empty

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export RUST_LOG="${RUST_LOG:-info}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

# Create bucket if it doesn't exist (MinIO mc not needed, use curl)
echo "Ensuring bucket 'rivet-test' exists in MinIO..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X PUT "http://localhost:9000/rivet-test" \
  -H "Authorization: AWS4-HMAC-SHA256" \
  2>/dev/null || true)

# Simpler: use AWS CLI or just let it fail gracefully; create via MinIO API
python3 -c "
import urllib.request, urllib.error, base64, hmac, hashlib, datetime
# MinIO accepts simple PUT to create bucket
try:
    req = urllib.request.Request('http://localhost:9000/rivet-test', method='PUT')
    urllib.request.urlopen(req)
    print('  bucket created')
except urllib.error.HTTPError as e:
    if e.code == 409:
        print('  bucket already exists')
    else:
        print(f'  bucket creation: HTTP {e.code} (may need manual creation)')
except Exception as e:
    print(f'  could not auto-create bucket: {e}')
    print('  create it manually at http://localhost:9001 (minioadmin/minioadmin)')
" 2>/dev/null || echo "  auto-create skipped; create bucket at http://localhost:9001"

CONFIG="${RIVET_CONFIG:-dev/rivet_s3_minio_test.yaml}"
SUB="${1:-run}"
shift || true

exec cargo run --bin rivet -- "$SUB" --config "$CONFIG" "$@"
