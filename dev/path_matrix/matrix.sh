#!/usr/bin/env bash
# Run `rivet run` against every YAML fixture under cfg/. After each run,
# normalize the on-disk layout and (optionally) diff against expected/<id>.layout.
#
# Workflow:
#   1. matrix.sh runs all scenarios, emits actual layouts under logs/<id>/layout.
#   2. If expected/<id>.layout exists, diff it; report PASS/FAIL.
#   3. If it does not, print the actual layout so the operator can review and
#      copy it into expected/ as a new baseline.
#
# Each scenario runs in an isolated workdir to keep the .rivet_state.db and
# .rivet/runs/ directories scenario-local. The destination `path:` in each
# YAML is `./out/<id>...`, which is interpreted relative to that workdir.

set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
R="${RIVET_BIN:-$ROOT/rivet}"
# Fall back to the workspace build — no per-matrix binary copy needed (the
# copies were 14 MB of stale artifacts each before the dev/ cleanup).
[[ -x "$R" ]] || R="$ROOT/../../target/release/rivet"
[[ -x "$R" ]] || R="$ROOT/../../target/debug/rivet"
NORMALIZE="$ROOT/normalize.sh"
EXTRACT_SUMMARY="$ROOT/extract_summary.sh"
LOGS="$ROOT/logs"
EXPECTED="$ROOT/expected"

if [[ ! -x $R ]]; then
  echo "rivet binary not found at $R" >&2
  echo "Build: cargo build --bin rivet --release && cp target/release/rivet dev/path_matrix/rivet" >&2
  exit 2
fi

export PG_URL="${PG_URL:-postgresql://rivet:rivet@127.0.0.1:5432/rivet}"
export MY_URL="${MY_URL:-mysql://rivet:rivet@127.0.0.1:3306/rivet}"

rm -rf "$LOGS"
mkdir -p "$LOGS"

fail=0
pass=0
new=0
total=0

for yaml in $(find "$ROOT/cfg" -name '*.yaml' | sort); do
  sid=$(basename "$yaml" .yaml)
  total=$((total+1))
  work="$LOGS/$sid/work"
  mkdir -p "$work"
  # Copy the YAML into the workdir so all relative paths resolve there.
  cp "$yaml" "$work/rivet.yaml"

  # Capture stdout/stderr/rc.
  (
    cd "$work"
    "$R" run -c rivet.yaml
  ) > "$LOGS/$sid/stdout" 2> "$LOGS/$sid/stderr"
  rc=$?
  echo "$rc" > "$LOGS/$sid/exit_code"

  # Walk the workdir and emit a layout listing relative to it.
  # Strip the workdir prefix and the `./` so the paths are stable.
  (
    cd "$work"
    find . -mindepth 1 -not -name 'rivet.yaml' | sed 's|^\./||'
  ) | "$NORMALIZE" > "$LOGS/$sid/layout"

  # Extract data-accounting fields from each summary.json. This is the matrix-
  # level counterpart to tests/live_reconcile_repair.rs — a code-path that
  # exports 0 rows where 30 were expected stays at rc=0 but the snapshot
  # diverges. Scenarios where rc != 0 (run failed) skip this step.
  if [[ "$rc" == "0" ]]; then
    "$EXTRACT_SUMMARY" "$work" > "$LOGS/$sid/summary" 2>/dev/null || : > "$LOGS/$sid/summary"
  else
    : > "$LOGS/$sid/summary"
  fi

  # Compare against baselines if they exist.
  layout_status="NEW"
  if [[ -f "$EXPECTED/$sid.layout" ]]; then
    if diff -u "$EXPECTED/$sid.layout" "$LOGS/$sid/layout" > "$LOGS/$sid/layout.diff" 2>&1; then
      layout_status="OK"
    else
      layout_status="DIVERGED"
    fi
  fi
  summary_status="NEW"
  if [[ -f "$EXPECTED/$sid.summary" ]]; then
    if diff -u "$EXPECTED/$sid.summary" "$LOGS/$sid/summary" > "$LOGS/$sid/summary.diff" 2>&1; then
      summary_status="OK"
    else
      summary_status="DIVERGED"
    fi
  fi

  if [[ "$layout_status" == "DIVERGED" || "$summary_status" == "DIVERGED" ]]; then
    printf '%-32s  rc=%-3s  FAIL    layout=%s summary=%s (see logs/%s/*.diff)\n' \
      "$sid" "$rc" "$layout_status" "$summary_status" "$sid"
    fail=$((fail+1))
  elif [[ "$layout_status" == "NEW" || "$summary_status" == "NEW" ]]; then
    printf '%-32s  rc=%-3s  NEW     layout=%s summary=%s (review logs/%s/, copy baselines)\n' \
      "$sid" "$rc" "$layout_status" "$summary_status" "$sid"
    new=$((new+1))
  else
    printf '%-32s  rc=%-3s  PASS    layout+summary match expected\n' "$sid" "$rc"
    pass=$((pass+1))
  fi
done

echo
echo "DONE.  $total scenarios: $pass PASS, $fail FAIL, $new NEW (no baseline yet)"
if (( fail > 0 )); then
  exit 1
fi
