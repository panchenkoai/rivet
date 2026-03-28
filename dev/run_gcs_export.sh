#!/usr/bin/env bash
set -euo pipefail

# Run rivet against dev/rivet_gcs_rivet_data_test.yaml from the repo root.
#
# Usage:
#   ./dev/run_gcs_export.sh              # rivet run
#   ./dev/run_gcs_export.sh check        # preflight
#   ./dev/run_gcs_export.sh run --validate
#
# Optional env:
#   RIVET_CONFIG   alternate config path (default: dev/rivet_gcs_rivet_data_test.yaml)
#   RUST_LOG       default: info
#   RIVET_RELEASE=1  use cargo run --release
#   DATABASE_URL   default: postgresql://rivet:rivet@localhost:5432/rivet (docker-compose)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export DATABASE_URL="${DATABASE_URL:-postgresql://rivet:rivet@localhost:5432/rivet}"
export RUST_LOG="${RUST_LOG:-info}"
CONFIG="${RIVET_CONFIG:-dev/rivet_gcs_rivet_data_test.yaml}"
SUB="${1:-run}"
shift || true

CARGO=(cargo run --bin rivet --)
if [[ "${RIVET_RELEASE:-}" == "1" ]]; then
  CARGO=(cargo run --release --bin rivet --)
fi

exec "${CARGO[@]}" "$SUB" --config "$CONFIG" "$@"
