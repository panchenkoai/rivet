#!/usr/bin/env python3
"""Differential compare: rivet's parquet vs Debezium's JSONL — in DuckDB.

DuckDB reads BOTH sides natively (`read_json_auto`, `read_parquet`), so the
normalisation and the set difference are one SQL statement over the two captures
rather than Python looping over parsed objects. That matters for the same reason
the rest of this repo's oracles use DuckDB: the comparison engine is independent
of both tools being compared, and a bug in a hand-written Python loop would be a
bug in the oracle itself.

rivet's side is read through the parts the MANIFEST DECLARES, never a directory
glob — a crashed run leaves parts no manifest names, and counting them would
report delivery no consumer will ever see.

Exit 0 = the two agree. Exit 1 = symmetric difference, printed both ways.
"""
import argparse, glob, json, os, subprocess, sys, tempfile


def declared_parts(root: str) -> list[str]:
    """The parts the manifest declares as committed — mirrors declared_parquet_parts
    (tests/common/parquet.rs) and _manifest_declared_parts (dev/release_oracle)."""
    copies = sorted(glob.glob(os.path.join(root, "manifest-*.json")))
    if not copies:
        c = os.path.join(root, "manifest.json")
        copies = [c] if os.path.isfile(c) else []
    out: set[str] = set()
    for d in copies:
        try:
            art = json.load(open(d))
        except (OSError, json.JSONDecodeError):
            continue
        for p in art.get("parts") or []:
            if isinstance(p, dict) and p.get("status") not in (None, "committed"):
                continue
            name = (p.get("path") or p.get("name")) if isinstance(p, dict) else p
            if not name:
                continue
            cand = name if os.path.isabs(name) else os.path.join(os.path.dirname(d), name)
            if os.path.isfile(cand):
                out.add(cand)
    return sorted(out)


SQL = """
-- Debezium Server's http sink with `format.value=json` and schemas disabled emits
-- the row envelope FLAT: {after, before, op, source}. The enveloped
-- {payload:{...}} shape appears when schemas are enabled, so both are accepted —
-- and an event matching NEITHER surfaces as UNKNOWN rather than being dropped. A
-- normaliser that silently skips what it does not understand would reintroduce the
-- exact class this harness exists to catch.
CREATE OR REPLACE VIEW dbz AS
SELECT
  CASE op
    WHEN 'c' THEN 'insert' WHEN 'r' THEN 'insert'
    WHEN 'u' THEN 'update' WHEN 'd' THEN 'delete'
    WHEN 't' THEN 'truncate' ELSE 'UNKNOWN' END AS op,
  CAST(coalesce(after.__KEY__, before.__KEY__) AS VARCHAR) AS k
FROM read_json_auto('__DBZ__', union_by_name=true, ignore_errors=false);

CREATE OR REPLACE VIEW riv AS
SELECT __op AS op, CAST(__KEY__ AS VARCHAR) AS k
FROM read_parquet(__PARTS__);
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rivet-dir", required=True)
    ap.add_argument("--debezium-jsonl", required=True)
    ap.add_argument("--key", required=True, help="the key column, on both sides")
    ap.add_argument("--exclude-op", action="append", default=[],
                    help="an op the two tools legitimately disagree on (see README)")
    a = ap.parse_args()

    parts = declared_parts(a.rivet_dir)
    if not parts:
        print(f"FAIL: no manifest-declared parts under {a.rivet_dir} — that is an "
              f"outcome ('nothing was delivered'), not a reason to glob", file=sys.stderr)
        return 1

    where = ""
    if a.exclude_op:
        ops = ", ".join(f"'{o}'" for o in a.exclude_op)
        where = f" WHERE op NOT IN ({ops})"

    # Explicit replace, not str.format: the SQL contains DuckDB struct braces.
    sql = (SQL.replace("__DBZ__", a.debezium_jsonl)
              .replace("__KEY__", a.key)
              .replace("__PARTS__", "[" + ", ".join(f"'{p}'" for p in parts) + "]"))
    sql += f"""
SELECT 'rivet-only' AS side, op, k FROM (SELECT * FROM riv{where} EXCEPT SELECT * FROM dbz{where})
UNION ALL
SELECT 'debezium-only', op, k FROM (SELECT * FROM dbz{where} EXCEPT SELECT * FROM riv{where})
UNION ALL
SELECT 'UNKNOWN-SHAPE', op, k FROM dbz WHERE op = 'UNKNOWN'
ORDER BY 1, 2, 3;
"""
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(sql)
        path = f.name
    try:
        r = subprocess.run(["duckdb", "-box", "-c", f".read {path}"],
                           capture_output=True, text=True)
    finally:
        os.unlink(path)
    if r.returncode != 0:
        print(r.stderr, file=sys.stderr)
        return 1
    body = r.stdout.strip()
    # DuckDB prints an empty box when a query returns no rows; agreement is the
    # absence of any differing row.
    if not body or "0 rows" in body or body.count("│") == 0:
        print("AGREE: rivet and Debezium produced the same (op, key) set")
        return 0
    print("DISAGREE — one of the two is wrong, and which is the finding:\n")
    print(body)
    return 1


if __name__ == "__main__":
    sys.exit(main())
