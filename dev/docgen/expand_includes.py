#!/usr/bin/env python3
"""Expand shared doc fragments into `.md` files — one source, no copy-paste.

A doc marks a reusable block with:

    <!-- include: shared/<name>.md -->
    ...expanded content (overwritten from docs/shared/<name>.md)...
    <!-- /include: shared/<name>.md -->

The fragment lives ONCE in `docs/shared/`. This tool rewrites the content between
the markers to match the fragment, so the committed `.md` stays plain (GitHub
renders it) while the source of truth is the single fragment. A CI `--check`
fails if any expansion is out of date.

    python3 dev/docgen/expand_includes.py           # expand in place
    python3 dev/docgen/expand_includes.py --check    # fail if stale
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
OPEN = re.compile(r"<!-- include: (\S+) -->")

def fragment(rel):
    p = DOCS / rel
    if not p.is_file():
        sys.exit(f"missing fragment: docs/{rel}")
    return p.read_text().strip("\n")

def expand_text(text):
    out, i, lines = [], 0, text.split("\n")
    changed = False
    while i < len(lines):
        m = OPEN.search(lines[i])
        if not m:
            out.append(lines[i]); i += 1; continue
        rel = m.group(1)
        close = f"<!-- /include: {rel} -->"
        # find the matching close
        j = i + 1
        while j < len(lines) and close not in lines[j]:
            j += 1
        if j >= len(lines):
            sys.exit(f"unclosed include for {rel}")
        # Build as individual lines so the idempotency comparison against
        # lines[i:j+1] (also a list of single lines) is valid.
        block = [lines[i], ""] + fragment(rel).split("\n") + ["", close]
        if block != lines[i:j+1]:
            changed = True
        out += block
        i = j + 1
    return "\n".join(out), changed

def main():
    check = "--check" in sys.argv
    stale = []
    for md in DOCS.rglob("*.md"):
        if "shared/" in str(md.relative_to(DOCS)):
            continue
        txt = md.read_text()
        if "<!-- include:" not in txt:
            continue
        new, changed = expand_text(txt)
        if changed:
            if check:
                stale.append(str(md.relative_to(ROOT)))
            else:
                md.write_text(new)
                print(f"expanded {md.relative_to(ROOT)}")
    if check and stale:
        print("::error::stale doc includes — run 'python3 dev/docgen/expand_includes.py':")
        for s in stale: print(f"  {s}")
        sys.exit(1)
    if check:
        print("includes: up to date")

if __name__ == "__main__":
    main()
