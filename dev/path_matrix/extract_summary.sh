#!/usr/bin/env bash
# Read every .rivet/runs/<run>/summary.json under the given workdir and emit
# a normalized accounting snapshot — one line per (export, key field). The
# format is `key=value` with timestamps and run_ids stripped, sorted so the
# output is deterministic.
#
# Fields pinned (stable, operator-visible):
#   export_name, status, format, compression, total_rows, files_produced
#
# Fields intentionally NOT pinned:
#   duration_ms / peak_rss_mb / bytes_written — env-dependent, noisy in CI
#   run_id / started_at / finished_at — non-deterministic
#
# Usage:
#   extract_summary.sh <workdir>
#   (writes the snapshot to stdout)
set -uo pipefail

work="${1:-}"
if [[ -z $work || ! -d $work ]]; then
  echo "extract_summary.sh <workdir>" >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "extract_summary.sh requires jq" >&2
  exit 2
fi

# Walk every summary.json deterministically.
find "$work/.rivet/runs" -name 'summary.json' 2>/dev/null | sort | while read -r f; do
  jq -r '
    "export=\(.export_name)\tstatus=\(.status)\tformat=\(.format)\tcompression=\(.compression)\ttotal_rows=\(.total_rows)\tfiles_produced=\(.files_produced)"
  ' "$f"
done | sort
