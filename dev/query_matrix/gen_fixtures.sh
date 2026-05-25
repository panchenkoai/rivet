#!/usr/bin/env bash
# Regenerate query-shape fixtures. Each YAML produces a `rivet plan` whose
# `base_query` is then run through `EXPLAIN (COSTS OFF)` against PG. The
# matrix pins the structural plan (Seq Scan vs Index Scan vs Bitmap, sort
# steps, subqueries) without the noisy per-environment cost numbers.
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
CFG="$ROOT/cfg"

w() {
  local path="$CFG/$1"
  mkdir -p "$(dirname "$path")"
  cat > "$path"
}

w q01_full_scan.yaml <<'YAML'
# Full scan of the table — should plan as Seq Scan (no WHERE filter).
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q01 }
YAML

w q02_pk_filter.yaml <<'YAML'
# PK-bounded query — should plan as Index Scan on the id PK (or Seq Scan
# with Filter, but never as Bitmap Heap on a 30-row fixture). The exact
# plan body is the contract.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE id <= 10"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q02 }
YAML

w q03_non_index_filter.yaml <<'YAML'
# Non-indexed predicate — should plan as Seq Scan with Filter. A regression
# that adds a "helpful" subquery / CTE wrapper changes the EXPLAIN shape.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit WHERE name LIKE 'row_1%'"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q03 }
YAML

w q04_order_by_pk.yaml <<'YAML'
# Ordering on the PK — small tables may sort in-memory or use the index.
# Pin whichever PG 16 picks for the fixture; future regression that wraps
# the ORDER BY in a Materialize / Sort step diverges.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT id, name FROM pa_audit ORDER BY id"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q04 }
YAML

w q05_aggregate.yaml <<'YAML'
# Aggregate — should plan as Aggregate over Seq Scan. Catches "did the
# planner push down predicate" / "did we wrap in a subquery" regressions.
source: { type: postgres, url_env: PG_URL }
exports:
  - name: pa_audit
    query: "SELECT COUNT(*)::INTEGER AS n FROM pa_audit"
    mode: full
    format: parquet
    destination: { type: local, path: ./out/q05 }
YAML

echo "Generated $(find "$CFG" -name '*.yaml' | wc -l | tr -d ' ') query-shape fixtures under $CFG"
