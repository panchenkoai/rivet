#!/usr/bin/env bash
# Create symlinks from dev/matrices/<layer>/<name> to the canonical matrix dirs.
# Safe to re-run — overwrites existing symlinks.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

link() {
  local target="$1"
  local linkpath="$2"
  mkdir -p "$(dirname "$linkpath")"
  ln -sfn "$target" "$linkpath"
  echo "  $linkpath → $target"
}

echo "Linking matrix harnesses under dev/matrices/:"

link "../../cli_matrix"           "$ROOT/surface/cli"
link "../../cfg_matrix"           "$ROOT/surface/cfg"
link "../../path_matrix"          "$ROOT/execution/path"
link "../../query_matrix"         "$ROOT/execution/query"
link "../../cross_version_matrix" "$ROOT/compatibility/cross_version"
link "../../legacy"               "$ROOT/compatibility/legacy"
link "../../soak_matrix"          "$ROOT/resources/soak"

echo "Done."
