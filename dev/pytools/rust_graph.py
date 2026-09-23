"""Exact Rust CALLS edges for code-review-graph from rust-analyzer's LSIF index (`index` ~45 s, `apply` <1 s, `check`)."""

from __future__ import annotations

import bisect
import json
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / ".code-review-graph" / "graph.db"
TIER = "RUST_ANALYZER"


def _head() -> str:
    """The commit the working tree is on."""
    return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def _lsif(out: Path) -> None:
    """Write rust-analyzer's LSIF dump of the workspace to `out`."""
    with out.open("w") as f:
        subprocess.run(["rust-analyzer", "lsif", str(ROOT)], stdout=f, stderr=subprocess.DEVNULL, check=True)


def _uses(lsif: Path) -> list[tuple[str, int, str, int, int, int]]:
    """(use file, use line, def file, def line, def start char, def end char) for every non-definition occurrence."""
    doc_uri: dict[int, str] = {}
    range_pos: dict[int, tuple[int, int, int]] = {}
    range_doc: dict[int, int] = {}
    next_of: dict[int, int] = {}
    def_result_of: dict[int, int] = {}
    def_ranges: dict[int, list[int]] = defaultdict(list)
    with lsif.open() as f:
        for line in f:
            o = json.loads(line)
            label = o["label"]
            if label == "document":
                doc_uri[o["id"]] = o["uri"].removeprefix("file://")
            elif label == "range":
                range_pos[o["id"]] = (o["start"]["line"], o["start"]["character"], o["end"]["character"])
            elif label == "contains" and o["outV"] in doc_uri:
                for r in o["inVs"]:
                    range_doc[r] = o["outV"]
            elif label == "next":
                next_of[o["outV"]] = o["inV"]
            elif label == "textDocument/definition":
                def_result_of[o["outV"]] = o["inV"]
            elif label == "item" and "property" not in o:
                def_ranges[o["outV"]].extend(o["inVs"])
    out = []
    for r, rs in next_of.items():
        if r not in range_doc:
            continue
        defs = def_ranges.get(def_result_of.get(rs, -1), [])
        if not defs or r in defs:
            continue
        d = defs[0]
        if d not in range_doc:
            continue
        line, _, _ = range_pos[r]
        dline, dstart, dend = range_pos[d]
        out.append((doc_uri[range_doc[r]], line + 1, doc_uri[range_doc[d]], dline + 1, dstart, dend))
    return out


class _Spans:
    """Function/Test nodes of one file, for innermost-enclosing and by-name lookups."""

    def __init__(self, rows: list[tuple[str, str, int, int]]):
        self.rows = sorted(rows, key=lambda r: r[2])
        self.starts = [r[2] for r in self.rows]

    def enclosing(self, line: int, name: str | None = None) -> str | None:
        """Qualified name of the innermost function spanning `line` (named `name` when given)."""
        best = None
        for q, n, s, e in self.rows[: bisect.bisect_right(self.starts, line)]:
            if s <= line <= e and (name is None or n == name) and (best is None or e - s < best[1]):
                best = (q, e - s)
        return best[0] if best else None


def index() -> int:
    """Build the LSIF dump, derive call edges, store them, apply them."""
    con = sqlite3.connect(DB)
    by_file: dict[str, list] = defaultdict(list)
    for q, n, f, s, e in con.execute(
        "SELECT qualified_name, name, file_path, line_start, line_end FROM nodes "
        "WHERE language = 'rust' AND kind IN ('Function', 'Test') AND line_start IS NOT NULL"
    ):
        by_file[f].append((q, n, s, e))
    spans = {f: _Spans(rows) for f, rows in by_file.items()}
    source_lines: dict[str, list[str]] = {}

    def ident(path: str, line: int, a: int, b: int) -> str:
        if path not in source_lines:
            try:
                source_lines[path] = Path(path).read_text(errors="replace").splitlines()
            except OSError:
                source_lines[path] = []
        text = source_lines[path]
        return text[line - 1][a:b] if 0 < line <= len(text) else ""

    with tempfile.TemporaryDirectory() as tmp:
        dump = Path(tmp) / "index.lsif"
        _lsif(dump)
        uses = _uses(dump)
    edges = set()
    for ufile, uline, dfile, dline, a, b in uses:
        if ufile not in spans or dfile not in spans:
            continue
        callee = spans[dfile].enclosing(dline, ident(dfile, dline, a, b))
        caller = spans[ufile].enclosing(uline)
        if callee and caller and callee != caller:
            edges.add((caller, callee, ufile, uline))
    con.execute(
        "CREATE TABLE IF NOT EXISTS rust_analyzer_calls "
        "(source_qualified TEXT, target_qualified TEXT, file_path TEXT, line INTEGER)"
    )
    con.execute("DELETE FROM rust_analyzer_calls")
    con.executemany("INSERT INTO rust_analyzer_calls VALUES (?, ?, ?, ?)", sorted(edges))
    con.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('rust_analyzer_sha', ?)", (_head(),))
    con.commit()
    print(f"rust_graph: {len(uses)} uses in the LSIF dump -> {len(edges)} call edges between graph nodes")
    return apply(con)


def apply(con: sqlite3.Connection | None = None) -> int:
    """Replace the heuristic Rust CALLS edges with the stored rust-analyzer ones."""
    con = con or sqlite3.connect(DB)
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name = 'rust_analyzer_calls'").fetchone():
        print("rust_graph: no rust_analyzer_calls yet — run `index` first")
        return 0
    now = time.time()
    with con:
        con.execute("DELETE FROM edges WHERE kind = 'CALLS' AND file_path LIKE '%.rs'")
        con.execute(
            "INSERT INTO edges (kind, source_qualified, target_qualified, file_path, line, confidence_tier, updated_at) "
            "SELECT 'CALLS', source_qualified, target_qualified, file_path, line, ?, ? FROM rust_analyzer_calls",
            (TIER, now),
        )
    n = con.execute("SELECT count(*) FROM edges WHERE confidence_tier = ?", (TIER,)).fetchone()[0]
    row = con.execute("SELECT value FROM metadata WHERE key = 'rust_analyzer_sha'").fetchone()
    stale = "" if row and row[0] == _head() else f" (index built at {row[0][:8] if row else '?'}, HEAD is {_head()[:8]} — run `index`)"
    print(f"rust_graph: {n} Rust CALLS edges now come from rust-analyzer{stale}")
    return 0


def check() -> int:
    """The call the name-based resolver lost (`super::super::duckdb::duckdb_row_census`) must resolve."""
    con = sqlite3.connect(DB)
    n = con.execute(
        "SELECT count(*) FROM edges WHERE kind = 'CALLS' AND target_qualified LIKE ? AND source_qualified LIKE ?",
        ("%tests/common/duckdb.rs::duckdb_row_census", "%tests/common/rig/oracle.rs::Rig.row_census"),
    ).fetchone()[0]
    print("rust_graph check:", "ok" if n else "FAILED — Rig.row_census -> duckdb_row_census is missing")
    return 0 if n else 1


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    sys.exit({"index": index, "apply": apply, "check": check}.get(cmd, lambda: print(__doc__) or 2)())
