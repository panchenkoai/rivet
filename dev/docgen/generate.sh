#!/usr/bin/env bash
# Regenerate all code-derived reference docs. Single source of truth = the code
# (clap + schemars). Run from the repo root; a CI gate diffs the result.
#
#   bash dev/docgen/generate.sh          # regenerate in place
#   bash dev/docgen/generate.sh --check  # fail if committed docs are stale
set -euo pipefail
cd "$(dirname "$0")/../.."

RIVET="${RIVET:-./target/debug/rivet}"
[ -x "$RIVET" ] || { echo "build the binary first: cargo build --bin rivet"; exit 1; }

gen() { # <outfile> <generator-cmd...>
  local out="$1"; shift
  "$@" > "$out.tmp"
  mv "$out.tmp" "$out"
  echo "generated $out ($(wc -l < "$out" | tr -d ' ') lines)"
}

# Config reference — from the JSON Schema the binary emits (schemars <- Config).
gen docs/reference/config-reference.md \
  bash -c "$RIVET schema config | python3 dev/docgen/config_md.py"

# CLI reference — from the clap `Cli` derive (same source as `--help`).
gen docs/reference/cli-reference.md "$RIVET" schema cli

GENERATED="docs/reference/config-reference.md docs/reference/cli-reference.md"

if [ "${1:-}" = "--check" ]; then
  # Shared prose fragments (docs/shared/*) must be expanded into their consumers.
  python3 dev/docgen/expand_includes.py --check
  if ! git diff --quiet -- $GENERATED; then
    echo "::error::generated docs are stale — run 'bash dev/docgen/generate.sh' and commit"
    git --no-pager diff -- $GENERATED | head -60
    exit 1
  fi
  echo "docs-gen: up to date"
else
  python3 dev/docgen/expand_includes.py
fi
