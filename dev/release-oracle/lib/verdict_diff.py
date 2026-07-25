#!/usr/bin/env python3
"""Show which tables diverged between the golden verdicts (arg1) and live (arg2).
Compact one-line summary for the gate's FAIL detail."""
import json
import sys

want = json.loads(sys.argv[1] or "{}")
got = json.loads(sys.argv[2] or "{}")
diffs = []
for t in sorted(set(want) | set(got)):
    w, g = want.get(t), got.get(t)
    if w != g:
        diffs.append(f"{t}:golden={w or '-'}≠live={g or '-'}")
print("; ".join(diffs) if diffs else "(no per-table diff)")
