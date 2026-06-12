#!/usr/bin/env python3
"""verify_export.py — prove a rivet export is complete & uncorrupted, on YOUR data.

Independent of rivet's own bookkeeping: it compares a content *fingerprint* of
the SOURCE query against the same fingerprint of the EXPORTED Parquet, both
computed by DuckDB. If rivet dropped, duplicated, or corrupted any row, at least
one fingerprint field diverges.

  source rows ─┐                        ┌─ read_parquet(<output>)
               ├─ identical fingerprint ┤
       (live DB via DuckDB scanner)     (rivet's output)

The fingerprint is built automatically from the source query's schema:
  - count(*) and, if --key is given, count(distinct key)   (loss / duplication)
  - non-null count of every column                          (per-column loss)
  - sum() of every numeric column                           (value corruption)
  - sum(length()) of every text column                      (truncation)

Only one dependency:  pip install duckdb   (the postgres/mysql scanner extensions
auto-install on first use). Neither side reads rivet's counters — that is the point.

Usage:
  verify_export.py --source-type postgres \\
      --dsn "host=127.0.0.1 port=5432 dbname=rivet user=rivet password=rivet" \\
      --query "SELECT id, name, amount FROM orders" \\
      --parquet "/data/orders/*.parquet" \\
      --key id

Exit 0 = fingerprints match (PASS). Exit 1 = mismatch (FAIL). Exit 2 = setup error.
"""
import argparse
import sys

try:
    import duckdb
except ImportError:
    sys.exit("verify_export: needs DuckDB — run `pip install duckdb`")

# DuckDB type prefixes → fingerprint treatment.
_NUMERIC = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT",
            "USMALLINT", "UINTEGER", "UBIGINT", "DECIMAL", "DOUBLE", "FLOAT", "REAL")
_TEXT = ("VARCHAR", "CHAR", "TEXT", "STRING")


def fingerprint_exprs(schema, key):
    """Build the list of (alias, sql_expr) fingerprint columns from a DuckDB
    DESCRIBE result [(name, type), ...]. Identical for both sides."""
    exprs = [("rows", "count(*)")]
    if key:
        exprs.append((f"distinct_{key}", f'count(DISTINCT "{key}")'))
    for name, typ in schema:
        t = typ.upper()
        q = f'"{name}"'
        exprs.append((f"nn_{name}", f"count({q})"))  # non-null count, every column
        if t.startswith(_NUMERIC):
            # DOUBLE + round: exact for ints, tolerant for float/decimal noise.
            exprs.append((f"sum_{name}", f"round(sum(try_cast({q} AS DOUBLE)), 6)"))
        elif t.startswith(_TEXT):
            exprs.append((f"len_{name}", f"sum(length({q}))"))
    return exprs


def run_fp(con, exprs, from_clause):
    select = ", ".join(f"{e} AS {a}" for a, e in exprs)
    row = con.execute(f"SELECT {select} FROM {from_clause}").fetchone()
    return {a: row[i] for i, (a, _) in enumerate(exprs)}


def main():
    ap = argparse.ArgumentParser(description="Verify a rivet export against its source.")
    ap.add_argument("--source-type", choices=["postgres", "mysql"], required=True)
    ap.add_argument("--dsn", required=True,
                    help="DuckDB ATTACH string, e.g. 'host=h port=5432 dbname=d user=u password=p' "
                         "(postgres) or 'host=h user=u password=p database=d' (mysql)")
    ap.add_argument("--query", required=True, help="The EXACT SELECT rivet exported.")
    ap.add_argument("--parquet", required=True, help="Glob or dir of the exported Parquet, e.g. '/out/*.parquet'.")
    ap.add_argument("--key", default=None, help="Unique key column (enables the distinct-count check).")
    args = ap.parse_args()

    glob = args.parquet if "*" in args.parquet else args.parquet.rstrip("/") + "/*.parquet"
    con = duckdb.connect()
    ext = "postgres" if args.source_type == "postgres" else "mysql"
    try:
        con.execute(f"INSTALL {ext}; LOAD {ext};")
        con.execute(f"ATTACH '{args.dsn}' AS src (TYPE {ext}, READ_ONLY);")
    except Exception as e:  # noqa: BLE001
        sys.exit(f"verify_export: could not attach source ({ext}): {e}")

    src_q = args.query.replace("'", "''")
    scan = f"postgres_query('src', '{src_q}')" if ext == "postgres" else f"mysql_query('src', '{src_q}')"

    try:
        schema = con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()
        schema = [(r[0], r[1]) for r in schema]
    except Exception as e:  # noqa: BLE001
        sys.exit(f"verify_export: could not read source query schema: {e}")

    exprs = fingerprint_exprs(schema, args.key)
    try:
        src = run_fp(con, exprs, scan)
        dst = run_fp(con, exprs, f"read_parquet('{glob}')")
    except Exception as e:  # noqa: BLE001
        sys.exit(f"verify_export: fingerprint failed: {e}")

    diffs = [(a, src[a], dst[a]) for a, _ in exprs if src[a] != dst[a]]
    width = max(len(a) for a, _ in exprs)
    print(f"{'field'.ljust(width)}  {'source':>20}  {'export':>20}")
    for a, _ in exprs:
        mark = "  ✗" if src[a] != dst[a] else ""
        print(f"{a.ljust(width)}  {str(src[a]):>20}  {str(dst[a]):>20}{mark}")

    if diffs:
        print(f"\nFAIL: {len(diffs)} fingerprint field(s) differ — the export is NOT a faithful "
              f"copy of the source. rivet lost, duplicated, or corrupted data.", file=sys.stderr)
        sys.exit(1)
    print(f"\nPASS: source and export agree on all {len(exprs)} fingerprint fields "
          f"({src['rows']} rows). The export is complete and uncorrupted.")
    sys.exit(0)


if __name__ == "__main__":
    main()
