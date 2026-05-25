#!/usr/bin/env bash
# Read paths from stdin (one per line) and emit a normalized listing.
# Rules:
#   YYYYMMDD_HHMMSS                          → <TS>     (parquet/csv filenames)
#   YYYYMMDDTHHMMSS.NNN                      → <RUNID>  (.rivet/runs/<run>)
#   _chunk0/_chunk1/... — preserved (chunk numbering IS the contract)
#   Sort lines lexicographically so order is stable.
#
# Goal: produce a deterministic, diff-able file layout snapshot.
set -u
sed -E '
  s|[0-9]{8}T[0-9]{6}\.[0-9]+|<RUNID>|g
  s|[0-9]{8}_[0-9]{6}|<TS>|g
' | sort
