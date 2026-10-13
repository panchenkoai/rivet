"""Every connection the Python harness opens is held by a `with`.

A `connect(` / `MongoClient(` / `urlopen(` call under `dev/` must be the context
expression of a `with` item (directly, inside `closing(...)`, or handed to
`ExitStack.enter_context`), so its descriptor is released where the block ends
and not when the garbage collector gets to it. `sqlite3.connect` needs `closing`:
its own context manager commits and leaves the connection open.

A class may keep a connection on `self` only when it is listed in OWNERS and is
itself a context manager; a listed class that opens in `__init__` is then an
opener too, so constructing it outside a `with` fails the same way.

    python3 dev/pytools/connection_lint.py            # grade the tree
    python3 dev/pytools/connection_lint.py --self-test
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

OPENERS = frozenset({"connect", "MongoClient", "urlopen"})

#: `file::Class` → the connection it keeps for its lifetime. Shrink-only.
OWNERS: dict[str, str] = {  # ratchet-pin: connection-owners strings
    "dev/release_oracle/duck.py::Oracle": "the DuckDB session a cell reads its stores through",
    "dev/release_oracle/rig_oracle.py::_Mem": "the in-memory DuckDB session of the self-tests",
    "dev/release_oracle/upgrade.py::_MongoKeys": "the Mongo client of one resume-load cell",
    "dev/release_oracle/field_state.py::_Watch": "the DuckDB session polling one Postgres state",
    "dev/pytools/soak.py::Engine": "the Mongo client shared by a soak's writer and samplers, opened on first use",
}  # ratchet-pin: end


def _name(call: ast.Call, alias: dict[str, str]) -> str:
    """The called name, through this file's `import x as y`."""
    f = call.func
    n = f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else ""
    return alias.get(n, n)


def _held(expr: ast.expr, wrapped: bool = False):
    """(call, inside `closing`) for every call a `with` item or `enter_context` closes."""
    if isinstance(expr, ast.IfExp):
        yield from _held(expr.body, wrapped)
        yield from _held(expr.orelse, wrapped)
    elif isinstance(expr, ast.Call):
        if _name(expr, {}) == "closing" and expr.args:
            yield from _held(expr.args[0], True)
        else:
            yield expr, wrapped


def _self_assigned(cls: ast.ClassDef, within: str | None = None):
    """Every call assigned to `self.<attr>` in the class (in one method when `within` names it)."""
    for fn in cls.body:
        if not isinstance(fn, ast.FunctionDef) or (within and fn.name != within):
            continue
        for node in ast.walk(fn):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call) and all(
                isinstance(t, ast.Attribute) and isinstance(t.value, ast.Name) and t.value.id == "self" for t in node.targets
            ):
                yield node.value


def check(sources: dict[str, str], owners: dict[str, str]) -> tuple[list[str], int]:
    """(findings, opens held correctly) over `path → source`; an unreadable file is a finding."""
    out: list[str] = []
    trees: dict[str, ast.Module] = {}
    for path, text in sorted(sources.items()):
        try:
            trees[path] = ast.parse(text)
        except SyntaxError as e:
            out.append(f"CONN: {path}:{e.lineno}: cannot be parsed, so its connections cannot be graded")
    alias = {p: {a.asname: a.name for n in ast.walk(t) if isinstance(n, ast.ImportFrom) for a in n.names if a.asname}
             for p, t in trees.items()}
    classes = {f"{p}::{n.name}": (p, n) for p, t in trees.items() for n in ast.walk(t) if isinstance(n, ast.ClassDef)}
    for key in sorted(owners):
        if key not in classes:
            out.append(f"STALE: {key} is listed in OWNERS and does not exist")
        elif not {"__enter__", "__exit__"} <= {f.name for f in classes[key][1].body if isinstance(f, ast.FunctionDef)}:
            out.append(f"CONN: {key} keeps a connection and is not a context manager (no `__enter__`/`__exit__`)")
    listed = {k: classes[k] for k in owners if k in classes}
    openers = set(OPENERS)
    while True:  # a listed class that opens in `__init__` is an opener itself
        more = {c.name for p, c in listed.values() if any(_name(v, alias[p]) in openers for v in _self_assigned(c, "__init__"))}
        if more <= openers:
            break
        openers |= more
    held_ok, used = 0, set()
    for path, tree in trees.items():
        held: dict[int, bool] = {}
        for node in ast.walk(tree):
            if isinstance(node, (ast.With, ast.AsyncWith)):
                for item in node.items:
                    held.update((id(c), w) for c, w in _held(item.context_expr))
            elif isinstance(node, ast.Call) and _name(node, {}) == "enter_context" and node.args:
                held.update((id(c), w) for c, w in _held(node.args[0]))
        owned = {id(v): k for k, (p, c) in listed.items() if p == path for v in _self_assigned(c)}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or _name(node, alias[path]) not in openers:
                continue
            what, at = _name(node, {}), f"{path}:{node.lineno}"
            f = node.func
            sqlite = isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and (f.value.id, f.attr) == ("sqlite3", "connect")
            if id(node) in held:
                if sqlite and not held[id(node)]:
                    out.append(f"CONN: {at}: `with sqlite3.connect(` commits and leaves the connection open — wrap it in `closing(...)`")
                else:
                    held_ok += 1
            elif id(node) in owned:
                used.add(owned[id(node)])
                held_ok += 1
            else:
                out.append(f"CONN: {at}: `{what}(` is not held by a `with` (use `with ... as`, `closing(...)` or `ExitStack.enter_context`)")
    for key in sorted(set(listed) - used):
        out.append(f"STALE: {key} keeps no connection on `self` — remove it from OWNERS (the list only shrinks)")
    return out, held_ok


def tree_sources(root: Path) -> dict[str, str]:
    """`dev/...` path → source for every Python file under `dev/`."""
    files = (f.relative_to(root) for f in sorted((root / "dev").rglob("*.py")))
    return {f.as_posix(): (root / f).read_text() for f in files if not any(p.startswith(".") for p in f.parts)}


def self_test() -> int:
    """Each unmanaged shape is a finding naming its line; each managed shape, and a listed owner, is not."""
    def found(src: str, owners: dict[str, str] | None = None) -> list[str]:
        return check({"dev/t.py": src}, owners or {})[0]

    for bad, line in (
        ("import duckdb\ncon = duckdb.connect()\n", 2),
        ("import pymongo\n\n\ndef f(u):\n    return pymongo.MongoClient(u)\n", 5),
        ("import urllib.request as r\nr.urlopen('http://x').read()\n", 2),
        ("import duckdb\ncon = duckdb.connect()\nwith con:\n    pass\n", 2),
        ("from contextlib import closing\nimport sqlite3\nc = closing(sqlite3.connect('x'))\n", 3),
        ("import duckdb\n\n\nclass K:\n    def __init__(self):\n        self.db = duckdb.connect()\n", 6),
    ):
        got = found(bad)
        assert len(got) == 1 and f"dev/t.py:{line}: " in got[0] and "is not held by a `with`" in got[0], (bad, got)
    got = found("import sqlite3\nwith sqlite3.connect('x') as c:\n    pass\n")
    assert len(got) == 1 and "dev/t.py:2" in got[0] and "wrap it in `closing(...)`" in got[0], got
    for good in (
        "import duckdb\nwith duckdb.connect() as con:\n    pass\n",
        "from contextlib import closing\nimport sqlite3\nwith closing(sqlite3.connect('x')) as c:\n    pass\n",
        "import pymongo\nwith pymongo.MongoClient(u) as a, pymongo.MongoClient(v) as b:\n    pass\n",
        "from contextlib import ExitStack\nimport duckdb\nwith ExitStack() as s:\n    c = s.enter_context(duckdb.connect())\n",
        "import duckdb\nwith (duckdb.connect(a) if a else duckdb.connect()) as c:\n    pass\n",
        "def connect():\n    pass\n",
    ):
        assert found(good) == [], (good, found(good))
    owner = ("import duckdb\n\n\nclass K:\n    def __init__(self):\n        self.db = duckdb.connect()\n\n"
             "    def __enter__(self):\n        return self\n\n    def __exit__(self, *e):\n        self.db.close()\n\n\n")
    listed = {"dev/t.py::K": "test"}
    assert found(owner + "with K() as k:\n    pass\n", listed) == []
    got = found(owner + "k = K()\n", listed)
    assert len(got) == 1 and "dev/t.py:15: `K(` is not held" in got[0], got
    got = check({"dev/t.py": owner, "dev/u.py": "from .t import K as Duck\nd = Duck()\n"}, listed)[0]
    assert len(got) == 1 and "dev/u.py:2: `Duck(` is not held" in got[0], got
    got = found(owner.replace("    def __exit__(self, *e):\n        self.db.close()\n", ""), listed)
    assert len(got) == 1 and "is not a context manager" in got[0], got
    got = found(owner.replace("self.db = duckdb.connect()", "self.db = None"), listed)
    assert len(got) == 1 and got[0].startswith("STALE: dev/t.py::K keeps no connection"), got
    assert found("", {"dev/t.py::Gone": "test"})[0].startswith("STALE: dev/t.py::Gone is listed")
    lazy = owner.replace("def __init__(self):", "def get(self):")
    assert found(lazy + "k = K()\n", listed) == [], "a class that opens after construction is not an opener"
    assert "cannot be parsed" in found("def f(:\n")[0]
    print("connection_lint self-test: ok")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    findings, held = check(tree_sources(ROOT), OWNERS)
    for line in findings:
        print(line)
    if not findings and not held:
        print("connection_lint: FAILED — no connection open found under dev/, so nothing was graded")
        return 1
    if not findings:
        print(f"connection_lint: ok ({held} connection open(s) held by a `with` or a listed owner, {len(OWNERS)} owner(s))")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
