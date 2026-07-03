#!/usr/bin/env bash
# Fifth matrix — soak / load regression guard.
#
# For each YAML under cfg/, runs `rivet run` against a 10_000-row PG table,
# captures duration_ms + peak_rss_mb + total_rows + files_produced from
# summary.json, and compares against per-scenario thresholds in
# expected/<id>.thresholds.
#
# Threshold file format (one `key=value` per line):
#   total_rows_min=10000      hard lower bound — row count regression
#   duration_ms_max=5000      hard upper bound — perf regression
#   peak_rss_mb_max=200       hard upper bound — memory regression
#
# Thresholds are intentionally generous (2-3x over a healthy local run) so
# CI runners with varying perf characteristics don't flap. The guard exists
# to catch ORDER-OF-MAGNITUDE regressions ("now 50x slower / 10x memory"),
# not micro-tuning regressions — those belong in `cargo bench` benchmarks
# and are graded by a dedicated nightly job.
#
# Bring up PG first:
#   docker compose up -d postgres
#   ./seed.sh
set -u
ROOT="$(cd "$(dirname "$0")" && pwd)"
R="${RIVET_BIN:-$ROOT/rivet}"
# Fall back to the workspace build — no per-matrix binary copy needed (the
# copies were 14 MB of stale artifacts each before the dev/ cleanup).
[[ -x "$R" ]] || R="$ROOT/../../target/release/rivet"
[[ -x "$R" ]] || R="$ROOT/../../target/debug/rivet"
LOGS="$ROOT/logs"
EXPECTED="$ROOT/expected"
PATH_EXTRACT="$ROOT/../matrices/_common/lib/extract_summary.sh"

if [[ ! -x $R ]]; then
  echo "rivet binary not found at $R" >&2
  echo "Build: cargo build --bin rivet --release && cp target/release/rivet dev/soak_matrix/rivet" >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "soak_matrix requires jq" >&2
  exit 2
fi

export PG_URL="${PG_URL:-postgresql://rivet:rivet@127.0.0.1:5432/rivet}"

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
  cp "$yaml" "$work/rivet.yaml"

  start_ts="$(date +%s%3N 2>/dev/null || date +%s000)"
  (cd "$work" && "$R" run -c rivet.yaml) > "$LOGS/$sid/stdout" 2> "$LOGS/$sid/stderr"
  rc=$?
  echo "$rc" > "$LOGS/$sid/exit_code"

  if [[ $rc -ne 0 ]]; then
    printf '%-26s  RUN-FAIL rc=%s\n' "$sid" "$rc"
    fail=$((fail+1))
    continue
  fi

  # Extract the first (and only) summary.json's metrics.
  summary_file=$(find "$work/.rivet/runs" -name 'summary.json' | head -1)
  if [[ -z $summary_file ]]; then
    printf '%-26s  NO-SUMMARY (summary.json missing)\n' "$sid"
    fail=$((fail+1))
    continue
  fi
  total_rows=$(jq -r '.total_rows' "$summary_file")
  duration_ms=$(jq -r '.duration_ms' "$summary_file")
  peak_rss_mb=$(jq -r '.peak_rss_mb' "$summary_file")
  files_produced=$(jq -r '.files_produced' "$summary_file")
  printf 'total_rows=%s\nduration_ms=%s\npeak_rss_mb=%s\nfiles_produced=%s\n' \
    "$total_rows" "$duration_ms" "$peak_rss_mb" "$files_produced" > "$LOGS/$sid/metrics"

  if [[ ! -f "$EXPECTED/$sid.thresholds" ]]; then
    printf '%-26s  NEW    rows=%s dur=%sms rss=%sMB (no thresholds yet; copy from logs/%s/metrics)\n' \
      "$sid" "$total_rows" "$duration_ms" "$peak_rss_mb" "$sid"
    new=$((new+1))
    continue
  fi

  # Read thresholds and assert. We use awk for portable integer comparisons.
  scenario_fail=0
  while IFS='=' read -r key val; do
    [[ -z "$key" || "$key" =~ ^# ]] && continue
    case "$key" in
      total_rows_min)
        if [[ -z "$total_rows" ]] || [[ "$total_rows" -lt "$val" ]]; then
          echo "  FAIL $sid: total_rows=$total_rows < total_rows_min=$val" >&2
          scenario_fail=1
        fi
        ;;
      duration_ms_max)
        if [[ -z "$duration_ms" ]] || [[ "$duration_ms" -gt "$val" ]]; then
          echo "  FAIL $sid: duration_ms=$duration_ms > duration_ms_max=$val (perf regression)" >&2
          scenario_fail=1
        fi
        ;;
      peak_rss_mb_max)
        if [[ -z "$peak_rss_mb" ]] || [[ "$peak_rss_mb" -gt "$val" ]]; then
          echo "  FAIL $sid: peak_rss_mb=$peak_rss_mb > peak_rss_mb_max=$val (memory regression)" >&2
          scenario_fail=1
        fi
        ;;
      *)
        echo "  WARN $sid: unknown threshold key '$key'" >&2
        ;;
    esac
  done < "$EXPECTED/$sid.thresholds"

  if (( scenario_fail == 0 )); then
    printf '%-26s  PASS   rows=%s dur=%sms rss=%sMB\n' "$sid" "$total_rows" "$duration_ms" "$peak_rss_mb"
    pass=$((pass+1))
  else
    printf '%-26s  FAIL   rows=%s dur=%sms rss=%sMB (see above)\n' "$sid" "$total_rows" "$duration_ms" "$peak_rss_mb"
    fail=$((fail+1))
  fi
done

echo
echo "DONE.  $total scenarios: $pass PASS, $fail FAIL, $new NEW"
if (( fail > 0 )); then
  exit 1
fi
