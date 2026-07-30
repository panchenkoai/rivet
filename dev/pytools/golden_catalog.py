"""Rebuild `dev/golden-catalog/golden_catalog.db` from its versioned `.sql` source.

Port of `dev/golden-catalog/build.sh`. The `.sql` is the diffable source of
truth; the `.db` is the ready-to-query artifact. Both are checked in so a
consumer can query without a build step — but the `.db` must be REBUILT here,
never hand-edited, whenever `golden_catalog.sql` changes.

Two bash properties are gone rather than translated:

1. The `sqlite3` CLI is no longer a dependency — the stdlib `sqlite3` module
   executes the script in-process. `bash build.sh` on a host without the CLI
   died with `command not found` AFTER `rm -f golden_catalog.db` had already
   run, i.e. it destroyed the artifact and could not rebuild it. Here the
   source is read and parsed BEFORE the old `.db` is touched.
2. The summary line ran three separate `sqlite3` invocations nested inside an
   `echo` — command substitutions whose failure `set -o pipefail` cannot see
   (a failed `$(...)` inside `echo` yields an empty field, and `echo` still
   exits 0). The counts here come from one connection and a failure raises.

Usage:
    python3 -m dev.pytools.golden_catalog          # rebuild
    python3 -m dev.pytools.golden_catalog --check  # rebuild into a temp file and
                                                   # report drift without writing
"""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
from pathlib import Path
from typing import Sequence

from .shell import Fail, log, main, ok

HERE = Path(__file__).resolve().parents[2] / "dev" / "golden-catalog"
SQL = HERE / "golden_catalog.sql"
DB = HERE / "golden_catalog.db"


def _counts(conn: sqlite3.Connection) -> tuple[int, int, int]:
    total = conn.execute("SELECT count(*) FROM golden_seed").fetchone()[0]
    normal = conn.execute(
        "SELECT count(*) FROM golden_seed WHERE category='normal'"
    ).fetchone()[0]
    garbage = conn.execute(
        "SELECT count(*) FROM golden_seed WHERE category='garbage'"
    ).fetchone()[0]
    return total, normal, garbage


def build(dest: Path, *, source: Path = SQL) -> tuple[int, int, int]:
    """Execute `source` into a FRESH database at `dest`, returning the seed counts.

    Reads the source first: an unreadable / empty `.sql` must not cost us the
    existing artifact (the bash version deleted the `.db` before finding out).
    """
    script = source.read_text()
    if "golden_seed" not in script:
        raise Fail(f"{source} does not define golden_seed — refusing to rebuild {dest}")

    if dest.exists():
        dest.unlink()
    conn = sqlite3.connect(dest)
    try:
        conn.executescript(script)
        conn.commit()
        return _counts(conn)
    finally:
        conn.close()


def run(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="golden_catalog")
    ap.add_argument(
        "--check",
        action="store_true",
        help="rebuild into a temp file and report whether the committed .db is stale",
    )
    ns = ap.parse_args(list(argv) if argv is not None else None)

    if not SQL.is_file():
        raise Fail(f"missing source of truth: {SQL}")

    if ns.check:
        with tempfile.TemporaryDirectory() as td:
            fresh = Path(td) / "golden_catalog.db"
            total, normal, garbage = build(fresh)
            if not DB.is_file():
                raise Fail(f"{DB} is missing — run 'python3 -m dev.pytools.golden_catalog'")
            # Compare the SEED CONTENT, not the file bytes: SQLite page layout is
            # not reproducible (free-page reuse, encoding of the same rows), so a
            # byte diff would report drift on an identical catalog.
            live = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
            try:
                have = _counts(live)
            finally:
                live.close()
            if have != (total, normal, garbage):
                raise Fail(
                    f"{DB.name} is stale: has {have} (total, normal, garbage), "
                    f"{SQL.name} yields {(total, normal, garbage)} — "
                    "run 'python3 -m dev.pytools.golden_catalog' and commit"
                )
            ok(f"{DB.name} matches {SQL.name} — {total} seeds")
            return 0

    total, normal, garbage = build(DB)
    log(f"built {DB.name} — {total} seeds ({normal} normal, {garbage} garbage)")
    return 0


if __name__ == "__main__":
    main(run)
