#!/usr/bin/env bash
# Copy or build the release rivet binary into a matrix directory.
#
# Usage: stage_rivet.sh <matrix_dir> [repo_root]
#
# Honors RIVET_BIN when set (copies from that path). Otherwise uses
# <repo_root>/target/release/rivet, building if missing and --build
# was passed via STAGE_RIVET_BUILD=1.
set -uo pipefail

dir="${1:-}"
repo="${2:-$(cd "$(dirname "$0")/../../../.." && pwd)}"

if [[ -z "$dir" ]]; then
  echo "Usage: stage_rivet.sh <matrix_dir> [repo_root]" >&2
  exit 2
fi

mkdir -p "$dir"

if [[ -n "${RIVET_BIN:-}" && -x "$RIVET_BIN" ]]; then
  cp "$RIVET_BIN" "$dir/rivet"
  echo "Staged $RIVET_BIN → $dir/rivet"
  exit 0
fi

bin="$repo/target/release/rivet"
if [[ ! -x "$bin" ]]; then
  if [[ "${STAGE_RIVET_BUILD:-0}" == 1 ]]; then
    (cd "$repo" && cargo build --bin rivet --release)
  else
    echo "Release binary not found at $bin" >&2
    echo "Run: cargo build --bin rivet --release" >&2
    echo "Or: STAGE_RIVET_BUILD=1 $0 $dir" >&2
    exit 2
  fi
fi

cp "$bin" "$dir/rivet"
echo "Staged $bin → $dir/rivet"
