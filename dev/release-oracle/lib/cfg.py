#!/usr/bin/env python3
"""Dependency-free query interface over matrix.yaml for `python3 -m dev.release_oracle` — one source of
truth. Deliberately NO PyYAML: a release gate must run on a bare python3 (broken
pip / missing yaml must never block it). A tiny line scanner over the KNOWN
matrix.yaml shape is enough; matrix.yaml stays the human-readable spec.

Usage: cfg.py <matrix.yaml> <query> [args]
  engines | versions <eng> | url <eng> | seed <eng> | tls <eng>
  stores  | store <name> <field> | bq <field>
"""
import re
import sys


def read(path):
    out = []
    for raw in open(path):
        line = raw.rstrip("\n")
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        out.append((indent, s, line))
    return out


def top_block(rows, name):
    """Lines strictly inside the top-level `name:` mapping (indent > 0)."""
    block, grabbing = [], False
    for indent, s, line in rows:
        if indent == 0:
            grabbing = s.rstrip(":") == name or s.startswith(name + ":")
            continue
        if grabbing:
            block.append((indent, s, line))
    return block


def sub_block(block, key, base_indent=2):
    """Within a block, the lines under `<key>:` at base_indent (deeper indent)."""
    out, grabbing, want = [], False, None
    for indent, s, line in block:
        if indent == base_indent and (s.rstrip(":") == key or s.startswith(key + ":")):
            grabbing, want = True, indent
            continue
        if grabbing:
            if indent <= want:
                break
            out.append((indent, s, line))
    return out


def scalar(block, key, base_indent=None):
    for indent, s, line in block:
        if (base_indent is None or indent == base_indent) and re.match(rf"{re.escape(key)}\s*:", s):
            v = s.split(":", 1)[1].strip()
            return v.strip('"').strip("'")
    return ""


def main():
    rows = read(sys.argv[1])
    q, a = sys.argv[2], sys.argv[3:]
    if q == "engines":
        eng = top_block(rows, "engines")
        print("\n".join(s[:-1] for i, s, _ in eng if i == 2 and s.endswith(":")))
    elif q in ("url", "seed", "tls"):
        eng = sub_block(top_block(rows, "engines"), a[0], base_indent=2)
        print(scalar(eng, q))
    elif q == "versions":
        eng = sub_block(top_block(rows, "engines"), a[0], base_indent=2)
        ver = sub_block(eng, "versions", base_indent=4)
        for _, s, _ in ver:
            m = re.search(r"tag:\s*\"?([^\",}]+)\"?.*image:\s*\"?([^\",}]+)\"?.*port:\s*(\d+)", s)
            if m:
                print(f"{m.group(1).strip()} {m.group(2).strip()} {m.group(3)}")
    elif q == "stores":
        st = top_block(rows, "stores")
        print("\n".join(s.split(":", 1)[0] for i, s, _ in st if i == 2 and ":" in s))
    elif q == "store":
        st = top_block(rows, "stores")
        line = next((s for i, s, _ in st if i == 2 and s.startswith(a[0] + ":")), "")
        m = re.search(rf"{a[1]}\s*:\s*\"?([^\",}}]+)", line)
        print(m.group(1).strip() if m else "")
    elif q == "bq":
        bq = sub_block(top_block(rows, "final"), "bigquery", base_indent=2)
        v = scalar(bq, a[0])
        # a flow list `[x, y]` → comma-joined bare
        print(re.sub(r"[\[\]\s]", "", v) if v.startswith("[") else v)
    else:
        sys.exit(f"unknown query: {q}")


if __name__ == "__main__":
    main()
