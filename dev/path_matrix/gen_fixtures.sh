#!/usr/bin/env bash
# Regenerate every YAML fixture under cfg/. Idempotent.
# Each scenario targets a specific path-layout invariant:
#   p01  full + parquet + local → single parquet, manifest.json, _SUCCESS
#   p02  full + csv + local → same shape but .csv
#   p03  chunked → multiple <ts>_chunk<N>.parquet files
#   p04  two exports → two separate destination subdirs
#   p05  stdout destination → no files in out/, summary still in .rivet/runs
#   p06  custom relative path → resolved relative to CWD
#   p07  full run → asserts .rivet_state.db + .rivet/runs/<RUNID>/summary.*
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
CFG="$ROOT/cfg"

w() {
  local path="$CFG/$1"
  mkdir -p "$(dirname "$path")"
  cat > "$path"
}

w p01_full_parquet_local.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p01 }
YAML

w p02_full_csv_local.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: csv
    destination: { type: local, path: ./out/p02 }
YAML

w p03_chunked_multifile.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    table: pa_audit
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: { type: local, path: ./out/p03 }
YAML

w p04_multi_export.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit_a
    query: "SELECT id, name FROM pa_audit WHERE id <= 15"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p04_a }
  - name: pa_audit_b
    query: "SELECT id, name FROM pa_audit WHERE id > 15"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p04_b }
YAML

w p05_stdout.yaml <<'YAML'
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE id <= 5"
    mode: full
    format: csv
    destination: { type: stdout }
YAML

w p06_subdir_path.yaml <<'YAML'
# A nested relative path. We assert that the leaf directory ./out/p06/nested/run
# is created (not just ./out/p06), so any future change that strips intermediate
# path components from `destination.path` is caught.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p06/nested/run }
YAML

w p07_run_summary_landing.yaml <<'YAML'
# Same shape as p01 — fixture exists so an assertion can be pinned to a
# scenario whose only purpose is to verify .rivet/runs/<RUNID>/summary.*
# landed alongside the config (not in CWD or out/).
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE id <= 5"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/p07 }
YAML

echo "Generated $(find "$CFG" -name '*.yaml' | wc -l | tr -d ' ') path-matrix fixtures under $CFG"
