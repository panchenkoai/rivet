#!/usr/bin/env bash
# Fourth matrix in the family — pins what the DB planner does with the
# user-written `query:` from each YAML, by:
#
#   1. Running `rivet plan` to get the materialized base_query
#      (including any `${VAR}` / `--param` substitution).
#   2. Asking PostgreSQL to `EXPLAIN (COSTS OFF)` it.
#   3. Snapshotting the normalized plan and diffing against
#      expected/<id>.plan.
#
# Catches regressions that stay at rc=0 / message-OK but change what
# actually hits the DB:
#   - Operator query `SELECT id FROM users WHERE id <= 100` regressed
#     to a subquery wrapper → EXPLAIN shape changes from Index Scan
#     to Subquery Scan.
#   - Planner stops using the PK index because we forgot to declare
#     `id` integer in a future schema change.
#   - Chunked rewrite goes from `WHERE id BETWEEN $1 AND $2` to
#     `OFFSET $1 LIMIT $2`. (Not pinned yet — base_query is the
#     pre-chunked query; chunked-rewrite SQL needs runtime capture,
#     a separate future story.)
#
# COSTS OFF strips per-environment cost numbers from the plan; remaining
# variance is small and accepted into the baseline.

set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
R="${RIVET_BIN:-$ROOT/rivet}"
LOGS="$ROOT/logs"
EXPECTED="$ROOT/expected"

if [[ ! -x $R ]]; then
  echo "rivet binary not found at $R" >&2
  echo "Build: cargo build --bin rivet --release && cp target/release/rivet dev/query_matrix/rivet" >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "query_matrix requires jq" >&2
  exit 2
fi

export PG_URL="${PG_URL:-postgresql://rivet:rivet@127.0.0.1:5432/rivet}"
PG_CONTAINER="${PG_CONTAINER:-rivet-postgres-1}"

if ! docker exec "$PG_CONTAINER" psql -U rivet -d rivet -c "SELECT 1" >/dev/null 2>&1; then
  echo "PG container '$PG_CONTAINER' not reachable" >&2
  exit 2
fi

rm -rf "$LOGS"
mkdir -p "$LOGS"

fail=0
pass=0
new=0
total=0

for yaml in $(find "$ROOT/cfg" -name '*.yaml' | sort); do
  sid=$(basename "$yaml" .yaml)
  total=$((total+1))
  dir="$LOGS/$sid"
  mkdir -p "$dir"

  # 1. plan → JSON
  "$R" plan -c "$yaml" -e pa_audit --format json > "$dir/plan.json" 2> "$dir/plan.stderr"
  plan_rc=$?
  if [[ $plan_rc -ne 0 ]]; then
    printf '%-32s  PLAN-FAIL rc=%s (see logs/%s/plan.stderr)\n' "$sid" "$plan_rc" "$sid"
    fail=$((fail+1))
    continue
  fi

  # 2. extract base_query
  base_query=$(jq -r '.resolved_plan.base_query' "$dir/plan.json")
  if [[ -z $base_query || $base_query == "null" ]]; then
    printf '%-32s  NO-QUERY base_query missing from plan.json\n' "$sid"
    fail=$((fail+1))
    continue
  fi
  printf '%s\n' "$base_query" > "$dir/base_query.sql"

  # 3. EXPLAIN (COSTS OFF). Pipe the query into docker exec so we don't have
  # to worry about shell-escaping arbitrary SQL.
  printf 'EXPLAIN (COSTS OFF)\n%s;\n' "$base_query" | \
    docker exec -i "$PG_CONTAINER" psql -U rivet -d rivet -qAtX 2> "$dir/explain.stderr" \
    > "$dir/explain.raw"

  # 4. Normalize: trim trailing whitespace, drop empty lines. EXPLAIN's
  # tree-line prefixes (`->`) and indentation are stable across runs at
  # the same PG version + same statistics; we keep them as part of the
  # contract.
  sed -e 's/[[:space:]]*$//' -e '/^$/d' "$dir/explain.raw" > "$dir/explain"

  # 5. Compare against baseline.
  if [[ -f "$EXPECTED/$sid.plan" ]]; then
    if diff -u "$EXPECTED/$sid.plan" "$dir/explain" > "$dir/explain.diff" 2>&1; then
      printf '%-32s  PASS    EXPLAIN matches baseline\n' "$sid"
      pass=$((pass+1))
    else
      printf '%-32s  FAIL    EXPLAIN diverged (see logs/%s/explain.diff)\n' "$sid" "$sid"
      fail=$((fail+1))
    fi
  else
    printf '%-32s  NEW     no baseline (review logs/%s/explain, copy to expected/%s.plan)\n' "$sid" "$sid" "$sid"
    new=$((new+1))
  fi
done

echo
echo "DONE.  $total scenarios: $pass PASS, $fail FAIL, $new NEW"
if (( fail > 0 )); then
  exit 1
fi
