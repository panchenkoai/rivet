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
NORMALIZE="$ROOT/normalize.sh"
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

  # Compare against baseline if one exists.
  if [[ -f "$EXPECTED/$sid.layout" ]]; then
    if diff -u "$EXPECTED/$sid.layout" "$LOGS/$sid/layout" > "$LOGS/$sid/layout.diff" 2>&1; then
      printf '%-32s  rc=%-3s  PASS    layout matches expected\n' "$sid" "$rc"
      pass=$((pass+1))
    else
      printf '%-32s  rc=%-3s  FAIL    layout diverged (see logs/%s/layout.diff)\n' "$sid" "$rc" "$sid"
      fail=$((fail+1))
    fi
  else
    printf '%-32s  rc=%-3s  NEW     no baseline (review logs/%s/layout, copy to expected/)\n' "$sid" "$rc" "$sid"
    new=$((new+1))
  fi
done

echo
echo "DONE.  $total scenarios: $pass PASS, $fail FAIL, $new NEW (no baseline yet)"
if (( fail > 0 )); then
  exit 1
fi
