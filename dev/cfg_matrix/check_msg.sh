#!/usr/bin/env bash
# Enforce per-(scenario, probe, stream) substring contracts from
# expected_msg.txt against the most recent cfg_matrix run.
#
# Distinct from dev/cli_matrix/check_msg.sh because cfg_matrix logs are laid
# out as logs/<scenario>/<probe>/<stream>; the cli_matrix variant uses
# logs/matrix/<scenario>/<stream>. Same operator semantics: + / - / =N.
#
# Exit 0 → every assertion holds.
# Exit 1 → at least one assertion failed.
set -u

cd "$(dirname "$0")"
CONTRACT=expected_msg.txt
LOGS=logs

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
  line=${raw%%#*}
  line=$(printf '%s' "$line" | sed 's/[[:space:]]*$//')
  [[ -z "$line" ]] && continue

  id=$(printf '%s' "$line" | awk '{print $1}')
  probe_stream=$(printf '%s' "$line" | awk '{print $2}')
  op=$(printf '%s' "$line" | awk '{print $3}')
  substr=$(printf '%s' "$line" | awk '{for (i=4; i<=NF; i++) printf "%s%s", $i, (i==NF?"":" ")}')

  if [[ -z "$id" || -z "$probe_stream" || -z "$op" || -z "$substr" ]]; then
    echo "MALFORMED  $raw" >&2
    failures=$((failures+1))
    continue
  fi

  probe=${probe_stream%%/*}
  stream=${probe_stream##*/}
  if [[ "$probe" == "$stream" ]]; then
    echo "BAD FMT    $id: expected <probe>/<stream>, got '$probe_stream'" >&2
    failures=$((failures+1))
    continue
  fi

  file="$LOGS/$id/$probe/$stream"
  if [[ ! -f $file ]]; then
    echo "MISSING    $id/$probe/$stream not captured" >&2
    missing=$((missing+1))
    continue
  fi

  checked=$((checked+1))

  case "$op" in
    +)
      if ! grep -F -q -- "$substr" "$file"; then
        echo "NOT FOUND  $id/$probe/$stream: expected substring not present: $substr" >&2
        failures=$((failures+1))
      fi
      ;;
    -)
      if grep -F -q -- "$substr" "$file"; then
        offending=$(grep -F -- "$substr" "$file" | head -1)
        echo "PRESENT    $id/$probe/$stream: forbidden substring found: $substr" >&2
        echo "           line: $offending" >&2
        failures=$((failures+1))
      fi
      ;;
    =*)
      want=${op#=}
      got=$(grep -F -c -- "$substr" "$file" || true)
      if [[ "$got" != "$want" ]]; then
        echo "COUNT      $id/$probe/$stream: expected $want, got $got: $substr" >&2
        failures=$((failures+1))
      fi
      ;;
    *)
      echo "BAD OP     $id: unknown op '$op'" >&2
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
echo "If intentional: regenerate expected_msg.txt and document in CHANGELOG." >&2
exit 1
