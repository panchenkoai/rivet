#!/usr/bin/env bash
# Cross-version regression guard. macOS-bash-3.2 compatible (no -A arrays).
#
# Walks logs/<version>/<probe>/exit_code and asserts:
#
#   1. Each available version of Postgres returns the SAME exit code for
#      the SAME probe.  Differences across PG versions almost always
#      indicate a behavior regression on the older or newer end.
#
#   2. Same for MySQL: 5.7 vs 8.0 must agree on rc for each probe.
#
#   3. SKIPPED versions are ignored (legacy profile not started in CI is OK).
#
#   4. If at least one PG version probed, AT LEAST ONE must succeed —
#      every-PG-version-fails-doctor is the kind of catastrophic regression
#      this guard exists to catch.
#
# Exit 0 → no cross-version divergence.
# Exit 1 → at least one probe diverged across versions.
set -u
cd "$(dirname "$0")"
LOGS=logs

if [[ ! -d $LOGS ]]; then
  echo "$LOGS missing — run matrix.sh first." >&2
  exit 2
fi

# Echo every <version> for which logs/<v>/_status == "ok".
available() {
  local prefix="$1"; shift
  for v in "$@"; do
    if [[ -f "$LOGS/$v/_status" && "$(cat "$LOGS/$v/_status")" == "ok" ]]; then
      echo "$v"
    fi
  done
}

# Print "rc=N" for each version on a single probe, sorted by version.
# Used by check_agreement to compute the unique rc set.
collect_rcs() {
  local probe="$1"; shift
  for v in "$@"; do
    local f="$LOGS/$v/$probe/exit_code"
    if [[ -f "$f" ]]; then
      printf '%s=%s\n' "$v" "$(cat "$f")"
    fi
  done
}

check_agreement() {
  local family="$1"; shift
  local versions=("$@")
  local fail=0
  if [[ ${#versions[@]} -lt 2 ]]; then
    echo "  $family: only ${#versions[@]} version reachable, nothing to compare"
    return 0
  fi
  for probe in doctor check plan_full doctor_chunked check_chunked plan_full_chunked; do
    local pairs
    pairs="$(collect_rcs "$probe" "${versions[@]}")"
    if [[ -z "$pairs" ]]; then
      continue
    fi
    local uniq
    uniq="$(printf '%s\n' "$pairs" | awk -F= '{print $2}' | sort -u | tr '\n' ' ')"
    local distinct
    distinct="$(printf '%s\n' "$uniq" | tr -s ' ' '\n' | grep -c .)"
    if [[ "$distinct" == "1" ]]; then
      printf '  %-10s %-22s OK (rc=%s)\n' "$family" "$probe" "${uniq% }"
    else
      printf '  %-10s %-22s DIVERGED rc set: { %s} -- per version:\n' "$family" "$probe" "$uniq"
      printf '%s\n' "$pairs" | while IFS='=' read -r v rc; do
        printf '      %-10s rc=%s\n' "$v" "$rc"
      done
      fail=$((fail+1))
    fi
  done
  return $fail
}

echo "================ Cross-version agreement ==========="
pg_versions=()
while IFS= read -r v; do pg_versions+=("$v"); done < <(available _ pg-12 pg-13 pg-14 pg-15 pg-16)
mysql_versions=()
while IFS= read -r v; do mysql_versions+=("$v"); done < <(available _ mysql-57 mysql-80)

pg_fail=0
my_fail=0

if [[ ${#pg_versions[@]} -gt 0 ]]; then
  check_agreement PG "${pg_versions[@]}"
  pg_fail=$?
fi
if [[ ${#mysql_versions[@]} -gt 0 ]]; then
  check_agreement MySQL "${mysql_versions[@]}"
  my_fail=$?
fi

echo
echo "================ Sanity: at least one version succeeded =================="
if [[ ${#pg_versions[@]} -gt 0 ]]; then
  any_ok=0
  for v in "${pg_versions[@]}"; do
    if [[ -f "$LOGS/$v/doctor/exit_code" && "$(cat "$LOGS/$v/doctor/exit_code")" == "0" ]]; then
      any_ok=1; break
    fi
  done
  if (( any_ok == 1 )); then
    echo "  PG: at least one version reached rc=0 on doctor"
  else
    echo "  PG: ALL versions failed doctor — catastrophic regression?" >&2
    pg_fail=$((pg_fail+1))
  fi
fi

total_fail=$((pg_fail + my_fail))
if (( total_fail == 0 )); then
  echo
  echo "OK: no cross-version divergence."
  exit 0
fi
echo
echo "$total_fail divergence(s) found." >&2
echo "If intentional (e.g. PG 12 dropped a feature): document in CHANGELOG." >&2
exit 1
