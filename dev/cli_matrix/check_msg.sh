#!/usr/bin/env bash
# Companion to check_rc.sh: enforces per-scenario stderr/stdout substring
# contracts from expected_msg.txt against the most recent `matrix.sh` run.
#
# Why this exists: expected_rc.txt only pins exit codes. A release that
# silently drops a key WARN, double-prints a noisy log, or rewords an
# error hint stays at rc=0 and ships unnoticed. This script catches that
# class of regression.
#
# Exit 0  → every assertion holds.
# Exit 1  → at least one assertion failed.  Stderr lists divergences.
set -u

cd "$(dirname "$0")"

CONTRACT=expected_msg.txt
LOGS=logs/matrix

if [[ ! -f $CONTRACT ]]; then
  echo "$CONTRACT missing." >&2
  exit 2
fi
if [[ ! -d $LOGS ]]; then
  echo "$LOGS missing — run matrix.sh first." >&2
  exit 2
fi

failures=0
checked=0
missing=0

while IFS= read -r raw || [[ -n "$raw" ]]; do
  # Strip comments and skip blanks.
  line=${raw%%#*}
  # Trim trailing whitespace.
  line=$(printf '%s' "$line" | sed 's/[[:space:]]*$//')
  [[ -z "$line" ]] && continue

  # Parse: <id> <stream> <op> <substring...>
  id=$(printf '%s' "$line" | awk '{print $1}')
  stream=$(printf '%s' "$line" | awk '{print $2}')
  op=$(printf '%s' "$line" | awk '{print $3}')
  substr=$(printf '%s' "$line" | awk '{for (i=4; i<=NF; i++) printf "%s%s", $i, (i==NF?"":" ")}')

  if [[ -z "$id" || -z "$stream" || -z "$op" || -z "$substr" ]]; then
    echo "MALFORMED  $raw" >&2
    failures=$((failures+1))
    continue
  fi

  file="$LOGS/$id/$stream"
  if [[ ! -f $file ]]; then
    echo "MISSING    $id ($stream not captured)" >&2
    missing=$((missing+1))
    continue
  fi

  checked=$((checked+1))

  case "$op" in
    +)
      if ! grep -F -q -- "$substr" "$file"; then
        echo "NOT FOUND  $id $stream: expected substring not present: $substr" >&2
        failures=$((failures+1))
      fi
      ;;
    -)
      if grep -F -q -- "$substr" "$file"; then
        offending=$(grep -F -- "$substr" "$file" | head -1)
        echo "PRESENT    $id $stream: forbidden substring found: $substr" >&2
        echo "           line: $offending" >&2
        failures=$((failures+1))
      fi
      ;;
    =*)
      want=${op#=}
      got=$(grep -F -c -- "$substr" "$file" || true)
      if [[ "$got" != "$want" ]]; then
        echo "COUNT      $id $stream: expected $want occurrences, got $got: $substr" >&2
        failures=$((failures+1))
      fi
      ;;
    *)
      echo "BAD OP     $id: unknown op '$op' (expected +, -, or =N)" >&2
      failures=$((failures+1))
      ;;
  esac
done < "$CONTRACT"

if (( failures == 0 && missing == 0 )); then
  echo "OK: $checked assertion(s) hold."
  exit 0
fi

echo >&2
echo "$failures assertion(s) failed, $missing scenario(s) missing." >&2
echo "If the change is intentional: regenerate expected_msg.txt and document in CHANGELOG." >&2
exit 1
