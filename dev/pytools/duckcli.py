"""The `duckdb` CLI subset the harness uses, on the uv-pinned duckdb package instead of whatever binary is on PATH."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

#: argv prefix replacing a bare `duckdb` in every harness subprocess call.
ARGV = [sys.executable, str(Path(__file__).resolve())]


def _render(con, rel, mode: str, header: bool, out) -> None:
    """Print one result the way the CLI does: DuckDB's own VARCHAR rendering, NULL as empty."""
    cols = rel.columns
    if mode == "json":
        con.register("__duckcli_r", rel)
        rows = [r[0] for r in con.sql("SELECT to_json(__duckcli_r) FROM __duckcli_r").fetchall()]
        con.unregister("__duckcli_r")
        if rows:
            out.write("[" + ",\n".join(rows) + "]\n")
        return
    text = rel.project(", ".join(f'CAST("{c.replace(chr(34), chr(34) * 2)}" AS VARCHAR)' for c in cols))
    rows = [["" if v is None else v for v in r] for r in text.fetchall()]
    if mode == "csv":
        w = csv.writer(out, lineterminator="\n")
        if header:
            w.writerow(cols)
        w.writerows(rows)
        return
    if header:
        out.write("|".join(cols) + "\n")
    for r in rows:
        out.write("|".join(r) + "\n")


def main(argv: list[str]) -> int:
    """`duckdb [-noheader] [-list|-csv|-json|-box] -c SQL` — every statement's result printed, exit 1 on error."""
    import duckdb

    mode, header, sql = "list", True, None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "-noheader":
            header = False
        elif a in ("-list", "-csv", "-json", "-box"):
            mode = a[1:]
        elif a == "-c":
            i += 1
            sql = argv[i]
        else:
            print(f"duckcli: unsupported argument {a!r}", file=sys.stderr)
            return 2
        i += 1
    if sql is None:
        print("duckcli: -c SQL is required", file=sys.stderr)
        return 2
    if sql.strip().startswith(".read "):
        sql = Path(sql.strip()[len(".read "):].strip()).read_text()
    con = duckdb.connect()
    try:
        for stmt in duckdb.extract_statements(sql):
            rel = con.sql(stmt.query)
            if rel is not None:
                _render(con, rel, "list" if mode == "box" else mode, header, sys.stdout)
    except duckdb.Error as e:
        sys.stdout.flush()
        print(str(e), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
