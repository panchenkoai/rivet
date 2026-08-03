"""The SAME run, recorded into SQLite and into Postgres — do the two agree?

The gate already checks that a CDC run POPULATES the expected state tables on
each backend, against `golden/cdc_state_snapshot.json`. That is presence, not
parity: a table can be non-empty on both and still hold different numbers, and a
column can be written on one backend and silently dropped on the other.

This has happened. Shape tracking never worked on Postgres AT ALL — the code ran,
the run succeeded, and `export_shape` stayed empty on one backend while filling
on the other. Nothing failed, because nothing compared them. The two stores are
separate implementations of one contract (`StateConn::Sqlite | Postgres`), with
hand-written SQL on both sides and a placeholder translation between them, so
"they agree" is an assumption unless something asks.

WHAT IS COMPARED, and what deliberately is not:

  the POPULATED SET   which tables the run wrote to. This is the check that would
                      have caught the shape-tracking gap on the day it landed: a
                      table written on one backend and empty on the other.
  the ROW COUNT       per table. Same work, same number of records about it.
  the WORK it claims  `total_rows`, `files_committed`, `status` from
                      `export_metrics`; the `file_log` row count and its summed
                      `row_count`. These describe the export, so they must be
                      identical — they are not identity.

  NOT the identity    run ids and timestamps differ between two runs by
                      construction. Comparing them would only prove that two runs
                      are two runs.

One export, run twice with nothing changed but `RIVET_STATE_URL`. Any difference
is the backends disagreeing about what happened, because nothing else did.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path

try:
    from .core import Ledger, rivet, run
    from .scenarios import NO_TIMEOUT, _failed, _passed, _skipped, work_dir
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import Ledger, rivet, run  # type: ignore
    from scenarios import NO_TIMEOUT, _failed, _passed, _skipped, work_dir  # type: ignore

__all__ = ["verify_state_backend_parity"]

ROWS = 2000

#: Tables whose CONTENT is compared beyond a row count, and the columns that
#: describe the WORK rather than the run's identity.
WORK_COLUMNS = {
    "export_metrics": ["total_rows", "files_committed", "status"],
}

#: The migration bookkeeping, which the two backends name DIFFERENTLY
#: (`schema_version` on SQLite, `rivet_schema_version` on Postgres). Excluded
#: from the comparison because it records the STORE, not the run — but the
#: divergence is real and reported as an observation rather than swallowed: two
#: names for one concept is a trap for anyone who writes a query against "the"
#: version table.
BOOKKEEPING = {"schema_version", "rivet_schema_version"}

#: Tables with no `export_name` column. They cannot be scoped to this cell's
#: export, and the Postgres store is SHARED with the rest of the gate while the
#: SQLite one is fresh — so comparing their raw counts compares "everything this
#: gate ever did" against "this run", which is a harness artefact and not a
#: finding. Their PARITY is still worth having; it needs a scoped query per
#: table, which is a follow-up rather than a silent omission.
UNSCOPED = {"chunk_run", "chunk_task", "keyset_range", "loaded_source_run", "load_run"}


def _psql(container: str, sql: str) -> str:
    return run(
        ["docker", "exec", container, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc", sql]
    ).stdout.strip()


def _seed(src_container: str) -> bool:
    return run(
        [
            "docker", "exec", "-i", src_container, "psql", "-U", "rivet", "-d", "rivet",
            "-v", "ON_ERROR_STOP=1", "-c",
            "DROP TABLE IF EXISTS parity_probe; "
            "CREATE TABLE parity_probe(id INT PRIMARY KEY, v TEXT); "
            f"INSERT INTO parity_probe SELECT g, 'v'||g FROM generate_series(1,{ROWS}) g;",
        ],
        timeout=180,
    ).ok


def _sqlite_tables(db: Path) -> list[str]:
    with sqlite3.connect(db) as c:
        return [
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        ]


def _sqlite_profile(db: Path, export: str) -> dict[str, object]:
    """Row counts per table (scoped to this export where the table knows about
    exports) plus the work columns."""
    out: dict[str, object] = {}
    with sqlite3.connect(db) as c:
        for t in _sqlite_tables(db):
            cols = {r[1] for r in c.execute(f"PRAGMA table_info({t})")}
            where = f" WHERE export_name = '{export}'" if "export_name" in cols else ""
            try:
                out[f"count:{t}"] = c.execute(f"SELECT count(*) FROM {t}{where}").fetchone()[0]
            except sqlite3.Error:
                out[f"count:{t}"] = "unreadable"
        for t, columns in WORK_COLUMNS.items():
            if t not in _sqlite_tables(db):
                continue
            rows = c.execute(
                f"SELECT {', '.join(columns)} FROM {t} WHERE export_name = '{export}' "
                f"ORDER BY {columns[0]}"
            ).fetchall()
            out[f"work:{t}"] = [tuple(str(v) for v in r) for r in rows]
        if "file_log" in _sqlite_tables(db):
            n, s = c.execute(
                f"SELECT count(*), coalesce(sum(row_count),0) FROM file_log "
                f"WHERE export_name = '{export}'"
            ).fetchone()
            out["file_log:rows"] = (str(n), str(s))
    return out


def _pg_profile(container: str, export: str) -> dict[str, object]:
    out: dict[str, object] = {}
    tables = [
        t
        for t in _psql(
            container, "SELECT tablename FROM pg_tables WHERE schemaname='public'"
        ).splitlines()
        if t.strip()
    ]
    for t in tables:
        cols = _psql(
            container,
            f"SELECT string_agg(column_name, ',') FROM information_schema.columns "
            f"WHERE table_name = '{t}'",
        )
        where = f" WHERE export_name = '{export}'" if "export_name" in cols.split(",") else ""
        out[f"count:{t}"] = int(_psql(container, f"SELECT count(*) FROM {t}{where}") or 0)
    for t, columns in WORK_COLUMNS.items():
        if t not in tables:
            continue
        expr = "||'|'||".join(f"coalesce({c}::text,'-')" for c in columns)
        rows = _psql(
            container,
            f"SELECT {expr} FROM {t} WHERE export_name = '{export}' ORDER BY {columns[0]}",
        ).splitlines()
        out[f"work:{t}"] = [tuple(r.split("|")) for r in rows if r.strip()]
    if "file_log" in tables:
        raw = _psql(
            container,
            f"SELECT count(*)||'|'||coalesce(sum(row_count),0) FROM file_log "
            f"WHERE export_name = '{export}'",
        )
        out["file_log:rows"] = tuple(raw.split("|")) if "|" in raw else ("?", "?")
    return out


def verify_state_backend_parity(
    led: Ledger,
    *,
    state_url: str | None,
    state_container: str,
    src_container: str,
    src_url: str,
) -> None:
    if not state_url:
        _skipped(
            led, "-", "-", "state_backend_parity", "-",
            "state-parity: no Postgres state URL — there is only one backend to compare, so the "
            "cell would be comparing SQLite to itself",
            "no state url",
        )
        return
    if not _seed(src_container):
        _skipped(led, "-", "-", "state_backend_parity", "-",
                 "state-parity: could not seed the probe table", "no seed")
        return

    work = work_dir() / "state_parity"
    shutil.rmtree(work, ignore_errors=True)
    (work / "sqlite").mkdir(parents=True, exist_ok=True)
    (work / "pg").mkdir(parents=True, exist_ok=True)
    export = f"parity_probe_{os.getpid()}"

    def cfg_at(base: Path) -> Path:
        c = base / "parity.yaml"
        c.write_text(
            f"source:\n  type: postgres\n  url: \"{src_url}\"\n"
            f"exports:\n"
            f"  - name: {export}\n"
            f"    table: public.parity_probe\n"
            f"    mode: chunked\n"
            f"    format: parquet\n"
            f"    chunk_column: id\n"
            f"    chunk_size: 500\n"
            f"    destination: {{ type: local, path: {base}/out }}\n"
        )
        return c

    # SQLite: the state db lands beside the config, so a fresh directory IS a
    # fresh backend. Postgres is shared with the rest of the gate, hence the
    # export-name scoping on every query.
    sq_cfg = cfg_at(work / "sqlite")
    sq = rivet("run", "-c", str(sq_cfg), env={"RIVET_STATE_URL": ""}, timeout=NO_TIMEOUT)
    pg_cfg = cfg_at(work / "pg")
    pg = rivet("run", "-c", str(pg_cfg), env={"RIVET_STATE_URL": state_url}, timeout=NO_TIMEOUT)
    if not sq.ok or not pg.ok:
        _failed(
            led, "-", "-", "state_backend_parity", "-",
            f"state-parity: a run failed before anything could be compared "
            f"(sqlite exit {sq.returncode}, postgres exit {pg.returncode}): "
            f"{(sq.out or pg.out or '')[-200:]}",
            "run failed",
        )
        return

    db = work / "sqlite" / ".rivet_state.db"
    if not db.exists():
        _failed(led, "-", "-", "state_backend_parity", "-",
                f"state-parity: no SQLite state db at {db} — the run wrote its state somewhere "
                f"this cell cannot see, so there is nothing to compare",
                "no sqlite db")
        return

    a = _sqlite_profile(db, export)
    b = _pg_profile(state_container, export)
    notes: list[str] = []

    # A table with no `export_name` column cannot be scoped, and the Postgres
    # store is SHARED with every other cell in this gate while the SQLite one is
    # fresh — so its counts are incomparable by construction, not by defect. Said
    # out loud rather than dropped: a silently narrowed comparison is how a cell
    # comes to grade less than its name claims.
    def populated(profile: dict[str, object]) -> set[str]:
        return {
            k[len("count:"):]
            for k, v in profile.items()
            if k.startswith("count:")
            and v not in (0, "unreadable")
            and k[len("count:"):] not in BOOKKEEPING
            and k[len("count:"):] not in UNSCOPED
        }

    pop_a, pop_b = populated(a), populated(b)
    if BOOKKEEPING & ({k[6:] for k in a} | {k[6:] for k in b}):
        notes.append(
            "the migration bookkeeping table is named `schema_version` on SQLite and "
            "`rivet_schema_version` on Postgres — excluded from the comparison (it records the "
            "STORE, not the run), but two names for one concept is a trap for any query written "
            "against `the` version table"
        )
    problems: list[str] = []
    only_sq, only_pg = sorted(pop_a - pop_b), sorted(pop_b - pop_a)
    if only_sq or only_pg:
        problems.append(
            f"the same run populated different tables: SQLite-only {only_sq}, Postgres-only "
            f"{only_pg} — this is the shape of the defect where a feature's writes were simply "
            f"absent on one backend while every run reported success"
        )
    for t in sorted(pop_a & pop_b):
        if a[f"count:{t}"] != b[f"count:{t}"]:
            problems.append(
                f"{t}: {a[f'count:{t}']} row(s) on SQLite vs {b[f'count:{t}']} on Postgres"
            )
    for key in ("work:export_metrics", "file_log:rows"):
        if key in a and key in b and a[key] != b[key]:
            problems.append(f"{key}: SQLite {a[key]} vs Postgres {b[key]}")

    if problems:
        _failed(
            led, "-", "-", "state_backend_parity", "-",
            "state-parity: " + "; ".join(problems),
            "backends disagree",
        )
        return
    _passed(
        led, "-", "-", "state_backend_parity", "-",
        f"state-parity: one export recorded into BOTH backends agrees on the populated table set "
        f"({len(pop_a)} tables), every per-table row count, the metrics row's work columns and "
        f"the file_log's count + summed rows"
        + (f". NOTE: {'; '.join(notes)}" if notes else "")
        + f". NOT compared: {sorted(UNSCOPED)} (no export_name column, and the Postgres store is "
        f"shared with the rest of the gate)",
    )
