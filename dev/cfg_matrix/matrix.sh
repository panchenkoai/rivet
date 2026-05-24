#!/usr/bin/env bash
# Run doctor + check + plan against every YAML fixture under cfg/.
# Captures stdout/stderr/exit code per (scenario, probe) into
# logs/<scenario>/<probe>/.
#
# Why three probes:
#   doctor → exercises source connect + preflight messages
#   check  → exercises full config validation + DB reachability
#   plan   → exercises export+destination validation, requires a valid export
#
# This is OBSERVATION mode — no expected_* baseline yet. Inspect logs and
# decide which behaviors deserve a guard once we know what's there.

set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
R="${RIVET_BIN:-$ROOT/rivet}"
LM="$ROOT/logs"
mkdir -p "$LM"
if [[ ! -x "$R" ]]; then
  echo "rivet binary not found at $R" >&2
  echo "Build: cargo build --bin rivet --release && cp target/release/rivet dev/cfg_matrix/rivet" >&2
  exit 2
fi

export PG_URL="${PG_URL:-postgresql://rivet:rivet@127.0.0.1:5432/rivet}"
export MY_URL="${MY_URL:-mysql://rivet:rivet@127.0.0.1:3306/rivet}"
export PG_PASSWORD="${PG_PASSWORD:-rivet}"
export MY_PASSWORD="${MY_PASSWORD:-rivet}"

# Extract the first export's `name:` value for the plan probe. Greps the
# YAML; not a real parser but enough for our fixtures, which all keep
# `- name: <id>` on a single line and use single-quoted/no-quoted names.
first_export_name() {
  local yaml="$1"
  awk '
    /^[[:space:]]*-[[:space:]]+name:/ {
      sub(/^[[:space:]]*-[[:space:]]+name:[[:space:]]*/, "")
      gsub(/["'\'']/, "")
      sub(/[[:space:]]+$/, "")
      print
      exit
    }' "$yaml"
}

run_probe() {
  # run_probe <scenario_id> <probe_name> <cmd...>
  local sid="$1"; shift
  local probe="$1"; shift
  local dir="$LM/$sid/$probe"
  mkdir -p "$dir"
  printf '%s ' "$@" > "$dir/cmd"; printf '\n' >> "$dir/cmd"
  "$@" > "$dir/stdout" 2> "$dir/stderr"
  local rc=$?
  printf '%s\n' "$rc" > "$dir/exit_code"
  printf '%s' "$rc"
}

count=0
declare -A summary

for yaml in $(find "$ROOT/cfg" -name '*.yaml' | sort); do
  sid=$(basename "$yaml" .yaml)
  count=$((count+1))

  doctor_rc=$(run_probe "$sid" doctor "$R" doctor -c "$yaml")
  check_rc=$(run_probe "$sid" check  "$R" check  -c "$yaml")

  export_name=$(first_export_name "$yaml")
  if [[ -n "$export_name" ]]; then
    plan_rc=$(run_probe "$sid" plan "$R" plan -c "$yaml" -e "$export_name" --format json)
  else
    plan_rc="--"
    mkdir -p "$LM/$sid/plan"
    echo "no-export-name" > "$LM/$sid/plan/skipped"
  fi

  printf '%-44s  doctor=%-3s  check=%-3s  plan=%-3s\n' \
    "$sid" "$doctor_rc" "$check_rc" "$plan_rc"
done

echo
echo "DONE.  $count scenarios across 3 probes captured in $LM"
