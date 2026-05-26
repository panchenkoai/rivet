#!/usr/bin/env bash
# Soak matrix fixtures: 3 export modes against a 10_000-row table.
# Each YAML is sized so a healthy build completes in under ~5 seconds and
# uses well under 200 MB peak RSS. The matrix has tunable thresholds in
# expected/<id>.thresholds; a 2x regression in duration or memory trips
# them.
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
CFG="$ROOT/cfg"

w() {
  local path="$CFG/$1"
  mkdir -p "$(dirname "$path")"
  cat > "$path"
}

w s01_full_10k.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_soak
    query: "SELECT id, name, payload FROM pa_soak"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/s01 }
YAML

w s02_chunked_10k.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_soak
    table: pa_soak
    mode: chunked
    chunk_column: id
    chunk_size: 1000
    format: parquet
    destination: { type: local, path: ./out/s02 }
YAML

w s03_incremental_10k.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_soak
    query: "SELECT id, name, payload, updated_at FROM pa_soak"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: { type: local, path: ./out/s03 }
YAML

echo "Generated $(find "$CFG" -name '*.yaml' | wc -l | tr -d ' ') soak fixtures under $CFG"
