"""Upgrade continuity: what the PREVIOUS release left behind, carried on by THIS binary.

`release_regression` asks whether the new binary can READ what the old one wrote, and
`previous_release_differential` whether both do the same thing from scratch. Neither asks
the upgrade question an operator lives through: the config `rivet init` generated last
release, the state and checkpoints the last release wrote, the prefix it filled — does the
new binary pick all of that up without losing or re-reading a row?

Per SQL engine, on the downloaded previous binary and this one:

  config     the previous `init` writes the config; this binary `check`s it with the
             same strategy (the config file is the artifact).
  cursor     previous incremental run → source changes → this binary's run: every id
             once, every latest value right (the cursor in state is the artifact).
  crash      the previous binary crashes mid keyset run → this binary resumes it: every
             id exactly once (the crash checkpoint is the artifact).
  future     a state DB one schema version ahead of this binary is refused before any
             part is written (this binary is next release's "previous").
  fresh      the old config over the used prefix with an EMPTY state: no row is lost;
             the rows written again are reported (a re-baseline, measured not judged).

`cursor` and `crash` run on the SQLite state and, when the gate grades Postgres state, on
it too. Oracles: DuckDB over the parts the manifests declare, the source's own counts.
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from .core import Ledger, Proc, rivet_bin, run
from .regression import _require_prev_binary

__all__ = ["verify_upgrade_continuity"]

SCEN = "upgrade_continuity"
ROWS = 5000
CRASH_ROWS = 250_000
ENGINES = ("postgres", "mysql", "mssql")


def _sql(engine: str, url: str, sql: str) -> Proc:
    """Run `sql` on the stand engine behind `url`."""
    from .cdc import _mysql, _psql, _sqlcmd

    if engine == "postgres":
        return _psql(url, sql=sql)
    if engine == "mysql":
        return _mysql(url, sql)
    return _sqlcmd(url, q=sql)


def _seed(engine: str, url: str, table: str, rows: int, with_cursor: bool) -> bool:
    """(Re)create `table` holding ids 1..rows (and a cursor column when asked)."""
    ts = {"postgres": "TIMESTAMPTZ", "mysql": "DATETIME(6)", "mssql": "DATETIME2"}[engine]
    cols = f"id BIGINT PRIMARY KEY, v BIGINT NOT NULL{f', updated_at {ts} NOT NULL' if with_cursor else ''}"
    if engine == "postgres":
        val = ", TIMESTAMPTZ '2026-01-01' + g * INTERVAL '1 second'" if with_cursor else ""
        body = f"INSERT INTO {table} SELECT g, g{val} FROM generate_series(1, {rows}) g;"
    elif engine == "mysql":
        val = ", TIMESTAMP('2026-01-01') + INTERVAL n SECOND" if with_cursor else ""
        body = ("SET SESSION cte_max_recursion_depth = 1000000; "
                f"INSERT INTO {table} WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 "
                f"FROM g WHERE n < {rows}) SELECT n, n{val} FROM g;")
    else:
        val = ", DATEADD(SECOND, n, CAST('2026-01-01' AS DATETIME2))" if with_cursor else ""
        body = (f"INSERT INTO {table} SELECT TOP ({rows}) n, n{val} FROM (SELECT "
                "ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a "
                "CROSS JOIN sys.all_objects b) q;")
    return _sql(engine, url, f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({cols}); {body}").ok


def _mutate(engine: str, url: str, table: str) -> bool:
    """Add ids ROWS+1..ROWS+300 and flip v for ids 1..50, each with a newer cursor."""
    lo, hi = ROWS + 1, ROWS + 300
    if engine == "postgres":
        sql = (f"INSERT INTO {table} SELECT g, g, TIMESTAMPTZ '2026-06-01' + g * INTERVAL '1 second' "
               f"FROM generate_series({lo}, {hi}) g; "
               f"UPDATE {table} SET v = -v, updated_at = TIMESTAMPTZ '2026-07-01' WHERE id <= 50;")
    elif engine == "mysql":
        sql = (f"INSERT INTO {table} WITH RECURSIVE g(n) AS (SELECT {lo} UNION ALL SELECT n + 1 FROM g "
               f"WHERE n < {hi}) SELECT n, n, TIMESTAMP('2026-06-01') + INTERVAL n SECOND FROM g; "
               f"UPDATE {table} SET v = -v, updated_at = '2026-07-01' WHERE id <= 50;")
    else:
        sql = (f"INSERT INTO {table} SELECT n, n, DATEADD(SECOND, n, CAST('2026-06-01' AS DATETIME2)) "
               f"FROM (SELECT TOP ({hi - lo + 1}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {lo - 1} "
               "AS n FROM sys.all_objects) q; "
               f"UPDATE {table} SET v = -v, updated_at = '2026-07-01' WHERE id <= 50;")
    return _sql(engine, url, sql).ok


def _declared(out: Path, select: str) -> list[tuple]:
    """`select` over the parts every run-unique manifest under `out` declares (`{parts}` is the relation)."""
    import duckdb

    parts: list[str] = []
    for m in glob.glob(str(out / "**" / "manifest-*.json"), recursive=True):
        doc = json.loads(Path(m).read_text())
        parts += [str(Path(m).parent / Path(p["path"]).name) for p in doc.get("parts", [])]
    if not parts:
        return []
    return duckdb.connect().execute(select.format(parts=f"read_parquet({parts})")).fetchall()


def _declared_names(out: Path) -> set[str]:
    """The part file names every run-unique manifest under `out` declares."""
    names: set[str] = set()
    for m in glob.glob(str(out / "**" / "manifest-*.json"), recursive=True):
        names |= {Path(p["path"]).name for p in json.loads(Path(m).read_text()).get("parts", [])}
    return names


def _strategy(p: Proc) -> list[str]:
    """The `Strategy:` lines a `rivet check` printed."""
    return [ln.strip() for ln in p.out.splitlines() if ln.strip().startswith("Strategy:")]


class _Env:
    """One config dir: the previous `init` wrote its config; runs go through either binary."""

    def __init__(self, prev: Path, root: Path, engine: str, url: str, table: str, mode: str,
                 state_url: str):
        self.dir = root / f"{engine}_{table}_{'pg' if state_url else 'sq'}"
        self.dir.mkdir(parents=True)
        self.prev, self.env = prev, {"RIVET_UPG_URL": url, "RIVET_STATE_URL": state_url}
        self.init = run([str(prev), "init", "--source-env", "RIVET_UPG_URL", "--table", table,
                         "--mode", mode, "-o", "c.yaml"], env=self.env, cwd=self.dir)

    def rivet(self, binary: Path, *args: str, extra: dict[str, str] | None = None) -> Proc:
        """`binary args…` in this dir, with the source URL and state backend pinned."""
        return run([str(binary), *args], env={**self.env, **(extra or {})}, cwd=self.dir, timeout=None)

    def parquet_count(self) -> int:
        return len(glob.glob(str(self.dir / "output" / "**" / "*.parquet"), recursive=True))


def _cursor_leg(led: Ledger, prev: Path, root: Path, engine: str, url: str, state_url: str) -> None:
    store = "pg-state" if state_url else "sqlite"
    table = f"upg_cur_{engine[:2]}_{os.getpid()}_{store.replace('-', '')}"
    if not _seed(engine, url, table, ROWS, with_cursor=True):
        led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/cursor]: seed failed", "seed")
        return
    e = _Env(prev, root, engine, url, table, "incremental", state_url)
    try:
        if not e.init.ok:
            led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/config]: previous init failed: "
                       f"{e.init.stderr.strip()[-200:]}", "init")
            return
        if state_url == "":
            ps, cs = _strategy(e.rivet(prev, "check", "-c", "c.yaml")), _strategy(e.rivet(rivet_bin(), "check", "-c", "c.yaml"))
            if cs and cs == ps:
                led.passed(engine, "-", SCEN, "config", f"upgrade[{engine}/config]: this binary checks the previous init's config the same way ({cs[0]})")
            else:
                led.failed(engine, "-", SCEN, "config", f"upgrade[{engine}/config]: strategy prev {ps} vs this {cs}", "strategy")
        r1 = e.rivet(prev, "run", "-c", "c.yaml")
        ok_mut = r1.ok and _mutate(engine, url, table)
        r2 = e.rivet(rivet_bin(), "run", "-c", "c.yaml")
        got = _declared(e.dir / "output",
                        "SELECT count(DISTINCT id), count(*) FILTER (WHERE (id <= 50 AND lv >= 0) OR "
                        "(id > 50 AND lv < 0)), sum(n) FROM (SELECT id, arg_max(v, updated_at) lv, "
                        "count(*) n FROM {parts} GROUP BY id)")
        want = ROWS + 300
        # Exactly the first pass plus the delta (300 new, 50 changed): a re-read would add more.
        if ok_mut and r2.ok and got and got[0] == (want, 0, ROWS + 350):
            led.passed(engine, "-", SCEN, store, f"upgrade[{engine}/cursor/{store}]: this binary continued the "
                       f"previous release's cursor — {want} ids, every latest value right")
        else:
            led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/cursor/{store}]: prev ok={r1.ok} "
                       f"this ok={r2.ok} (distinct ids, wrong latest, rows)={got} want ({want}, 0, {ROWS + 350}): "
                       f"{r2.stderr.strip()[-200:]}", "cursor")
            return
        if state_url == "":
            _future_leg(led, e, engine)
            _fresh_leg(led, e, engine, want)
    finally:
        _sql(engine, url, f"DROP TABLE IF EXISTS {table};")


def _future_leg(led: Ledger, e: _Env, engine: str) -> None:
    """A state one schema version ahead of this binary is refused before any part lands."""
    db = e.dir / ".rivet_state.db"
    shutil.copy(db, e.dir / "state.bak")
    con = sqlite3.connect(db)
    (ver,) = con.execute("SELECT max(version) FROM schema_version").fetchone()
    con.execute("INSERT INTO schema_version(version) VALUES (?)", (ver + 1,))
    con.commit()
    con.close()
    before = e.parquet_count()
    p = e.rivet(rivet_bin(), "run", "-c", "c.yaml")
    shutil.copy(e.dir / "state.bak", db)
    said = p.out
    if not p.ok and "newer than this rivet knows" in said and e.parquet_count() == before:
        led.passed(engine, "-", SCEN, "future", f"upgrade[{engine}/future]: a v{ver + 1} state is refused "
                   f"by this v{ver} binary before any part is written")
    else:
        led.failed(engine, "-", SCEN, "future", f"upgrade[{engine}/future]: exit ok={p.ok}, parts "
                   f"{before}->{e.parquet_count()}: {said.strip()[-200:]}", "future")


def _fresh_leg(led: Ledger, e: _Env, engine: str, want: int) -> None:
    """The old config over the used prefix with an EMPTY state: nothing lost; re-written rows measured."""
    shutil.move(str(e.dir / ".rivet_state.db"), str(e.dir / "state.old"))
    before = _declared(e.dir / "output", "SELECT count(*) FROM {parts}")
    p = e.rivet(rivet_bin(), "run", "-c", "c.yaml")
    got = _declared(e.dir / "output", "SELECT count(DISTINCT id), count(*) FROM {parts}")
    if p.ok and got and got[0][0] == want:
        again = got[0][1] - (before[0][0] if before else 0)
        led.passed(engine, "-", SCEN, "fresh", f"upgrade[{engine}/fresh]: an empty state over the used "
                   f"prefix loses nothing — {want} ids; it re-baselined {again} rows (written again)")
    else:
        led.failed(engine, "-", SCEN, "fresh", f"upgrade[{engine}/fresh]: ok={p.ok} {got}: "
                   f"{p.stderr.strip()[-200:]}", "fresh")


def _crash_leg(led: Ledger, prev: Path, root: Path, engine: str, url: str, state_url: str) -> None:
    store = "pg-state" if state_url else "sqlite"
    table = f"upg_crash_{engine[:2]}_{os.getpid()}_{store.replace('-', '')}"
    if not _seed(engine, url, table, CRASH_ROWS, with_cursor=False):
        led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/crash]: seed failed", "seed")
        return
    e = _Env(prev, root, engine, url, table, "chunked", state_url)
    try:
        crashed = e.rivet(prev, "run", "-c", "c.yaml", extra={"RIVET_TEST_PANIC_AT": "after_keyset_page:1"})
        by_prev = {Path(f).name for f in glob.glob(str(e.dir / "output" / "**" / "*.parquet"), recursive=True)}
        r = e.rivet(rivet_bin(), "run", "-c", "c.yaml")
        got = _declared(e.dir / "output", "SELECT count(*), count(DISTINCT id) FROM {parts}")
        # Resumed, not restarted: the parts the previous binary wrote before the crash are declared.
        adopted = by_prev & _declared_names(e.dir / "output")
        if e.init.ok and not crashed.ok and r.ok and adopted and got and got[0] == (CRASH_ROWS, CRASH_ROWS):
            led.passed(engine, "-", SCEN, store, f"upgrade[{engine}/crash/{store}]: this binary resumed the "
                       f"previous release's crashed keyset run — {CRASH_ROWS} rows, each once")
        else:
            led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/crash/{store}]: init ok={e.init.ok} "
                       f"prev crashed={not crashed.ok} this ok={r.ok} pre-crash parts adopted={len(adopted)}/{len(by_prev)} (rows, ids)={got}: "
                       f"{r.stderr.strip()[-200:]}", "crash")
    finally:
        _sql(engine, url, f"DROP TABLE IF EXISTS {table};")


def verify_upgrade_continuity(led: Ledger) -> None:
    """The previous release's config, state and crash checkpoint, carried on by this binary."""
    prev = _require_prev_binary(led, "all", "-", SCEN, "local", "upgrade continuity")
    if prev is None:
        return
    led.phase("Upgrade continuity — the previous release's config, cursor and crash, carried on")
    root = Path(tempfile.mkdtemp(prefix="rivet-oracle-upgrade-"))
    states = [""] + ([os.environ["RIVET_GATE_STATE_URL"]] if os.environ.get("RIVET_GATE_STATE_URL") else [])
    for engine in ENGINES:
        uvar = f"RIVET_ORACLE_{engine.upper()}_URL"
        url = os.environ.get(uvar, "")
        if not url:
            led.skipped(engine, "-", SCEN, "local", f"upgrade[{engine}]: no {uvar}", "no url")
            continue
        for state_url in states:
            _cursor_leg(led, prev, root, engine, url, state_url)
            _crash_leg(led, prev, root, engine, url, state_url)
