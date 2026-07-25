#!/usr/bin/env python3
"""Canonicalize `bq query --format=prettyjson` output into a stable golden form.

Reads the JSON array on stdin, sorts rows by `id` (the type-matrix key), and emits
a compact JSON array with sorted keys. Determinism: same warehouse round-trip →
byte-identical output, so a golden diff means a REAL change in rivet's type export
or BigQuery's Parquet mapping — never row-order or key-order noise.
"""
import json
import sys

def key(row):
    v = row.get("id")
    try:
        return (0, int(v))
    except (TypeError, ValueError):
        return (1, str(v))

def main():
    rows = json.load(sys.stdin)
    if not isinstance(rows, list):
        rows = [rows]
    rows.sort(key=key)
    print(json.dumps(rows, sort_keys=True, ensure_ascii=False))

if __name__ == "__main__":
    main()
