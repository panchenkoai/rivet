#!/usr/bin/env bash
# Compare actual per-scenario exit codes from the most recent
# `matrix.sh` run against the baseline in `expected_rc.txt`.
#
# Exit 0  → every scenario's rc matches the baseline.
# Exit 1  → at least one rc diverged.  Stderr lists each divergence.
#
# Intentional changes: regenerate `expected_rc.txt` in the same PR
# that lands the behavior change, and document the change in the
# CHANGELOG entry that ships the PR.  CI failure on uncommitted
# divergence is the regression guard.
set -u

cd "$(dirname "$0")"

if [[ ! -f expected_rc.txt ]]; then
  echo "expected_rc.txt missing — run matrix.sh and bootstrap the baseline." >&2
  exit 2
fi
if [[ ! -d logs/matrix ]]; then
  echo "logs/matrix/ missing — run matrix.sh first." >&2
  exit 2
fi

failures=0
checked=0
missing=0

while IFS=' ' read -r id expected_rc; do
  [[ -z "$id" ]] && continue
  rc_file="logs/matrix/$id/exit_code"
  if [[ ! -f "$rc_file" ]]; then
    echo "MISSING  $id (expected rc=$expected_rc)" >&2
    missing=$((missing+1))
    continue
  fi
  actual=$(cat "$rc_file")
  checked=$((checked+1))
  if [[ "$actual" != "$expected_rc" ]]; then
    desc=$(cat "logs/matrix/$id/description" 2>/dev/null)
    echo "DIVERGED $id: rc $expected_rc → $actual  ($desc)" >&2
    failures=$((failures+1))
  fi
done < expected_rc.txt

if (( failures == 0 && missing == 0 )); then
  echo "OK: $checked scenarios match baseline."
  exit 0
fi

echo >&2
echo "$failures scenario(s) diverged, $missing scenario(s) missing." >&2
echo "If the change is intentional: regenerate expected_rc.txt and document in CHANGELOG." >&2
exit 1
