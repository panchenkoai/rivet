#!/usr/bin/env bash
# Shared substring contract checker for matrix harnesses.
#
# Usage:
#   check_msg.sh --contract FILE --logs DIR [--layout flat|probe]
#
# Contract line format:
#   flat:  <id> <stream> <op> <substring...>     (cli_matrix)
#   probe: <id> <probe/stream> <op> <substring...>  (cfg_matrix)
#
# Operators: + (must contain), - (must not), =N (exact count)
set -u

CONTRACT=""
LOGS=""
LAYOUT="flat"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --contract) CONTRACT="$2"; shift 2 ;;
    --logs) LOGS="$2"; shift 2 ;;
    --layout) LAYOUT="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: check_msg.sh --contract FILE --logs DIR [--layout flat|probe]" >&2
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$CONTRACT" || -z "$LOGS" ]]; then
  echo "check_msg.sh requires --contract and --logs" >&2
  exit 2
fi
if [[ ! -f $CONTRACT ]]; then
  echo "$CONTRACT missing." >&2
  exit 2
fi
if [[ ! -d $LOGS ]]; then
  echo "$LOGS missing — run matrix.sh first." >&2
  exit 2
fi
if [[ "$LAYOUT" != flat && "$LAYOUT" != probe ]]; then
  echo "Unknown layout '$LAYOUT' (expected flat or probe)" >&2
  exit 2
fi

failures=0
checked=0
missing=0

while IFS= read -r raw || [[ -n "$raw" ]]; do
  line=${raw%%#*}
  line=$(printf '%s' "$line" | sed 's/[[:space:]]*$//')
  [[ -z "$line" ]] && continue

  id=$(printf '%s' "$line" | awk '{print $1}')
  col2=$(printf '%s' "$line" | awk '{print $2}')
  op=$(printf '%s' "$line" | awk '{print $3}')
  # The substring runs to end-of-line and may legitimately contain multiple
  # consecutive spaces (e.g. column-aligned validator output like
  # `status:    PASSED`). awk's field reassembly collapses runs of whitespace
  # to a single space, so use sed to strip exactly the first three
  # whitespace-delimited tokens and keep the rest of the line byte-verbatim.
  substr=$(printf '%s' "$line" | sed -E 's/^[[:space:]]*[^[:space:]]+[[:space:]]+[^[:space:]]+[[:space:]]+[^[:space:]]+[[:space:]]+//')

  if [[ -z "$id" || -z "$col2" || -z "$op" || -z "$substr" ]]; then
    echo "MALFORMED  $raw" >&2
    failures=$((failures+1))
    continue
  fi

  if [[ "$LAYOUT" == flat ]]; then
    stream="$col2"
    file="$LOGS/$id/$stream"
    label="$id $stream"
  else
    probe=${col2%%/*}
    stream=${col2##*/}
    if [[ "$probe" == "$stream" ]]; then
      echo "BAD FMT    $id: expected <probe>/<stream>, got '$col2'" >&2
      failures=$((failures+1))
      continue
    fi
    file="$LOGS/$id/$probe/$stream"
    label="$id/$probe/$stream"
  fi

  if [[ ! -f $file ]]; then
    echo "MISSING    $label not captured" >&2
    missing=$((missing+1))
    continue
  fi

  checked=$((checked+1))

  case "$op" in
    +)
      if ! grep -F -q -- "$substr" "$file"; then
        echo "NOT FOUND  $label: expected substring not present: $substr" >&2
        failures=$((failures+1))
      fi
      ;;
    -)
      if grep -F -q -- "$substr" "$file"; then
        offending=$(grep -F -- "$substr" "$file" | head -1)
        echo "PRESENT    $label: forbidden substring found: $substr" >&2
        echo "           line: $offending" >&2
        failures=$((failures+1))
      fi
      ;;
    =*)
      want=${op#=}
      got=$(grep -F -c -- "$substr" "$file" || true)
      if [[ "$got" != "$want" ]]; then
        echo "COUNT      $label: expected $want occurrences, got $got: $substr" >&2
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
