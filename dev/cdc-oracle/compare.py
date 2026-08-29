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
-- `json_extract_string` returns the 4-char STRING 'null' for a JSON null, and the
-- same 'null' for a JSON string whose value is the text "null" — only `json_type`
-- tells them apart (measured in DuckDB, not assumed). Two consequences, and both
-- had to be fixed before `--value` could be trusted:
--
--   1. Every NULL cell disagreed: Debezium's side rendered 'null' while rivet's
--      parquet gave SQL NULL. Any nullable column made every row containing one
--      report DISAGREE, which is why `--value` was believed broken on deletes.
--   2. Conflating them the lazy way (mapping the literal 'null' to SQL NULL) would
--      make a cell holding the TEXT "null" indistinguishable from an absent one —
--      the degrade-to-null silent-loss class, reintroduced inside the very oracle
--      meant to catch it.
--
-- So the mapping is driven by json_type: a JSON null becomes SQL NULL, a string
-- "null" stays the string.
CREATE OR REPLACE MACRO jstr(doc, path) AS
  CASE WHEN json_type(doc, path) = 'NULL' THEN NULL
       ELSE json_extract_string(doc, path) END;
-- Debezium Server's http sink with `format.value=json` and schemas disabled emits
-- the row envelope FLAT: {after, before, op, source}. The enveloped
-- {payload:{...}} shape appears when schemas are enabled, so both are accepted —
-- and an event matching NEITHER surfaces as UNKNOWN rather than being dropped. A
-- normaliser that silently skips what it does not understand would reintroduce the
-- exact class this harness exists to catch.
CREATE OR REPLACE VIEW dbz AS
SELECT
  -- `__op` is the ExtractNewDocumentState shape (MongoDB): the transform flattens
  -- the document and prefixes its metadata, which is what makes a DELETE carry a
  -- key at all on that engine.
  CASE coalesce(jstr(j, '$.op'),
                jstr(j, '$.payload.op'),
                jstr(j, '$.__op'))
    WHEN 'c' THEN 'insert' WHEN 'r' THEN 'insert'
    WHEN 'u' THEN 'update' WHEN 'd' THEN 'delete'
    WHEN 't' THEN 'truncate' ELSE 'UNKNOWN' END AS op,
  -- `before` is absent from an insert-only capture, and read_json_auto then does
  -- not create the column at all — referencing it unconditionally is a BINDER
  -- error, not a NULL. json_extract_string over the raw line is shape-agnostic:
  -- it survives whichever fields the capture happens to contain, which is the
  -- point. The harness must not depend on the reference having exercised every op.
  -- MongoDB's connector serialises the document as a JSON STRING in `after`,
  -- where the relational connectors nest it as an object. Both are tried: the
  -- nested path first, then a second parse of the string form. Without the second
  -- the key comes back NULL for every Mongo event, which looks like a total
  -- disagreement rather than a normaliser that cannot read the shape.
  CAST(coalesce(
    jstr(j, '$.after.__KEY__'),
    jstr(j, '$.before.__KEY__'),
    jstr(j, '$.payload.after.__KEY__'),
    jstr(j, '$.payload.before.__KEY__'),
    jstr(jstr(j, '$.after'), '$.__KEY__'),
    jstr(jstr(j, '$.before'), '$.__KEY__'),
    jstr(jstr(j, '$.payload.after'), '$.__KEY__'),
    -- flattened form: the key sits at the top level beside `__op`
    jstr(j, '$.__KEY__')
  ) AS VARCHAR) AS k
  -- Value columns come AFTER the key, matching `riv` below. `EXCEPT` compares by
  -- POSITION, not by name, so a view that put them BEFORE the key compared v
  -- against k on every row — with `--value` on, all four rows of a four-row
  -- scenario reported as "both sides only", which reads as total disagreement and
  -- is why `--value` was believed to be broken on the delete path alone.
  __DBZ_VALS__
FROM (SELECT unnest(str_split(trim(content, chr(10)), chr(10))) AS j
      FROM read_text('__DBZ__'))
-- MySQL and SQL Server also emit SCHEMA-CHANGE events on the same sink. They are
-- DDL, not row changes, and PostgreSQL's connector does not send them — a
-- legitimate asymmetry, so they are excluded EXPLICITLY here rather than swallowed
-- by the UNKNOWN arm. Silencing them there would disarm the shape check for real
-- unrecognised events too.
--
-- Excluded by SHAPE (`tableChanges`), not by `ddl` being non-null: SQL Server sends
-- schema-change events with `ddl` set to JSON **null**, and once `jstr` started
-- mapping a JSON null to SQL NULL (correctly — see the macro), a `ddl IS NULL`
-- filter stopped recognising them. Measured: 24 of 32 events in an mssql crud
-- capture, all surfacing as UNKNOWN-SHAPE. `tableChanges` is present on both
-- engines' schema-change events and on no row event.
WHERE j <> ''
  AND json_type(j, '$.tableChanges') IS NULL
  AND jstr(j, '$.ddl') IS NULL
  -- The mysql liveness probe (run.py 4b) writes into <t>_probe, which is in
  -- Debezium's include-list ONLY so its delivery proves the pipe is live —
  -- its events are harness plumbing, not scenario data, on either side.
  AND NOT ends_with(coalesce(jstr(j, '$.source.table'), ''), '_probe');

CREATE OR REPLACE VIEW riv AS
SELECT __op AS op, CAST(__KEY__ AS VARCHAR) AS k __RIV_VALS__
FROM read_parquet(__PARTS__);
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rivet-dir", required=True)
    ap.add_argument("--debezium-jsonl", required=True)
    ap.add_argument("--key", required=True, help="the key column, on both sides")
    ap.add_argument("--value", action="append", default=[],
                    help="a VALUE column to compare as well as the key; repeatable. "
                         "Without one the comparison is blind to cell corruption — "
                         "a parquet with every value NULLed still reports AGREE.")
    ap.add_argument("--rivet-json-column", default=None,
                    help="on rivet's side the value columns live INSIDE this JSON "
                         "column (MongoDB writes the whole document into `document`), "
                         "so they are extracted from it rather than read as columns")
    ap.add_argument("--allow-empty-reference", action="store_true",
                    help="accept a zero-event Debezium capture (asserting BOTH are empty)")
    ap.add_argument("--exclude-op", action="append", default=[],
                    help="an op the two tools legitimately disagree on (see README)")
    a = ap.parse_args()

    # ── the empty-capture guard, and it runs BEFORE any comparison ──────────
    #
    # An empty capture and "the two agree" produce the SAME output from a set
    # difference: nothing. So a Debezium that never started, a sink that never
    # received, or a rivet run that wrote nothing would all read as agreement —
    # the harness would be green exactly when it is broken. This session hit that
    # shape three times (a CI monitor reading "no checks" as pass, a self-test
    # grading a function it never called, a mutants config whose parse failure
    # printed an empty list), so it is checked first and refuses rather than warns.
    #
    # `--allow-empty-reference` exists for the one legitimate case: asserting that
    # NEITHER tool captured anything. It must be explicit, because the default has
    # to be that silence is a failure.
    parts = declared_parts(a.rivet_dir)
    if not parts:
        print(f"FAIL: no manifest-declared parts under {a.rivet_dir} — that is an "
              f"outcome ('nothing was delivered'), not a reason to glob", file=sys.stderr)
        return 1

    try:
        dbz_lines = sum(1 for _ in open(a.debezium_jsonl))
    except OSError as e:
        print(f"FAIL: the reference capture is unreadable ({e}). An absent file is "
              f"NOT agreement — it means Debezium never delivered, and comparing "
              f"against it would report 'no differences' for a harness that did "
              f"not run.", file=sys.stderr)
        return 1
    if dbz_lines == 0 and not a.allow_empty_reference:
        print(f"FAIL: the reference captured ZERO events ({a.debezium_jsonl}). "
              f"Empty and 'agrees' are the same set difference, so this refuses "
              f"instead of passing. Check that Debezium started (config at "
              f"/debezium/config/, `debezium.format.value` set) and that the sink "
              f"is reachable from its network. Pass --allow-empty-reference only "
              f"to assert that NEITHER side captured anything.", file=sys.stderr)
        return 1

    where = ""
    if a.exclude_op:
        ops = ", ".join(f"'{o}'" for o in a.exclude_op)
        where = f" WHERE op NOT IN ({ops})"

    # Explicit replace, not str.format: the SQL contains DuckDB struct braces.
    # Value columns, if any. Compared as text on both sides: the point is whether
    # the CELL survived, not whether two type systems render it identically — and
    # a comparison that skipped values entirely reported AGREE over a parquet with
    # 100% of a column NULLed (measured, which is why this exists).
    # A DELETE has no `after` — its image is in `before`, and rivet writes that
    # image into the same columns. Reading `$.after.<v>` unconditionally therefore
    # compared rivet's before-image against NULL on every delete. Under REPLICA
    # IDENTITY DEFAULT both sides carry only the key and agree by accident; set it
    # to FULL and they diverge, which is the configuration a value comparison is
    # for. Op-aware, so both are right.
    dbz_vals = "".join(
        f""", CASE WHEN coalesce(jstr(j, '$.op'),
                                 jstr(j, '$.payload.op'),
                                 jstr(j, '$.__op')) = 'd'
           THEN coalesce(jstr(j, '$.before.{v}'),
                         jstr(j, '$.payload.before.{v}'),
                         jstr(jstr(j, '$.before'), '$.{v}'),
                         jstr(j, '$.{v}'))
           ELSE coalesce(jstr(j, '$.after.{v}'),
                         jstr(j, '$.payload.after.{v}'),
                         jstr(jstr(j, '$.after'), '$.{v}'),
                         jstr(j, '$.{v}')) END AS v_{v}\n  """
        for v in a.value
    )
    # MongoDB's value columns are not columns: rivet writes the whole document into
    # one JSON column, and Debezium's ExtractNewDocumentState flattens the same
    # fields to the top level. Extracting `$.<v>` from both compares the same CELL —
    # which is what a value comparison is for — where a text comparison of the two
    # whole documents would have graded JSON key ORDER and whitespace instead. That
    # difference is why this engine was briefly written off as `na`.
    if a.rivet_json_column:
        riv_vals = "".join(
            f", jstr(CAST({a.rivet_json_column} AS VARCHAR), '$.{v}') AS v_{v}"
            for v in a.value
        )
    else:
        riv_vals = "".join(f", CAST({v} AS VARCHAR) AS v_{v}" for v in a.value)
    sql = (SQL.replace("__DBZ_VALS__", dbz_vals)
              .replace("__RIV_VALS__", riv_vals)
              .replace("__DBZ__", a.debezium_jsonl)
              .replace("__KEY__", a.key)
              .replace("__PARTS__", "[" + ", ".join(f"'{p}'" for p in parts) + "]"))
    # SHOW the values in the diff, not just (op, key). A row that appears on BOTH
    # sides means the tuples differ in a column the output hides, and "everything
    # disagrees, no reason given" is what makes a gate get bypassed (precondition 3
    # in the README). With the values printed, a value mismatch reads as a value
    # mismatch instead of a phantom key difference.
    shown = "".join(f", coalesce(v_{v}, '<null>') AS v_{v}" for v in a.value)
    sql += f"""
SELECT 'rivet-only' AS side, op, k{shown} FROM (SELECT * FROM riv{where} EXCEPT SELECT * FROM dbz{where})
UNION ALL
SELECT 'debezium-only', op, k{shown} FROM (SELECT * FROM dbz{where} EXCEPT SELECT * FROM riv{where})
UNION ALL
SELECT 'UNKNOWN-SHAPE', op, k{shown} FROM dbz WHERE op = 'UNKNOWN'
ORDER BY 2, 3, 1;
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
        # Report the counts that were compared. "AGREE" over two captures whose
        # sizes are never printed is the same silence this guard exists to remove —
        # a reader must be able to see the comparison had something to compare.
        # Report the size of the SET that was compared, not only the input sizes.
        # Two captures can be non-empty and still compare a degenerate set — e.g.
        # every event on one key — and "AGREE over 1 pair" deserves to look
        # different from "AGREE over 40". Same reason the counts are printed at all.
        cnt = subprocess.run(
            ["duckdb", "-noheader", "-list", "-c",
             sql.split("SELECT 'rivet-only'")[0] +
             "SELECT (SELECT count(*) FROM (SELECT DISTINCT * FROM riv)) || '/' || "
             "(SELECT count(*) FROM (SELECT DISTINCT * FROM dbz));"],
            capture_output=True, text=True)
        pairs = cnt.stdout.strip() or "?"
        # Name what was actually compared. An "AGREE" that says (op, key) while the
        # caller passed --value understates the check; one that claims values when
        # none were passed OVERSTATES it, which is the direction that matters — a
        # gate reporting a value comparison it never ran is the blind spot this
        # whole harness exists to close.
        scope = (f"(op, key, {', '.join(a.value)})" if a.value
                 else "(op, key) — VALUES NOT COMPARED, pass --value to see cell corruption")
        print(f"AGREE: rivet and Debezium produced the same {scope} set "
              f"[{len(parts)} declared part(s) / {dbz_lines} reference event(s) "
              f"-> {pairs} distinct tuples compared]")
        return 0
    print("DISAGREE — one of the two is wrong, and which is the finding:\n")
    print(body)
    return 1


if __name__ == "__main__":
    sys.exit(main())
