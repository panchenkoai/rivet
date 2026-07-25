#!/usr/bin/env python3
"""Parse `rivet check` output into a canonical {table: {strategy, verdict}} map.

The verdicts golden fixes the STRATEGY (keyset | range | full) and the VERDICT
(ACCEPTABLE | DEGRADED | …) init/check assigns to every seed + garbage table on
every SQL engine — so a change in strategy selection (a keyset that regresses to
full, a decimal that stops bailing, a name-trap that starts keyset'ing) fails the
release against a checked-in truth, not a hand-picked assertion.

Reads check output on stdin; merges into an existing JSON map (arg1, or '{}') so
seeds + garbage checks accumulate. Prints the merged canonical JSON.
"""
import json
import re
import sys


def strategy_class(s):
    if s.startswith("keyset("):
        return "keyset"
    if s.startswith("chunked(") or "date-chunked" in s:
        return "range"
    if s.startswith("incremental("):
        return "incremental"
    return "full"


def main():
    acc = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
    name = None
    for line in sys.stdin:
        m = re.match(r"Export:\s*(\S+)", line)
        if m:
            name = m.group(1)
            continue
        if name is None:
            continue
        m = re.match(r"\s*Strategy:\s*(.+?)\s*$", line)
        if m:
            acc.setdefault(name, {})["strategy"] = strategy_class(m.group(1).strip())
            continue
        m = re.match(r"\s*Verdict:\s*(\S+)", line)
        if m:
            acc.setdefault(name, {})["verdict"] = m.group(1)
    print(json.dumps(acc, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
