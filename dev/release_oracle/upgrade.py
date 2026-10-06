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
             id exactly once, and no part the crashed run left is written again (the crash
             checkpoint is the artifact; a restart from scratch rewrites the same part names).
  future     a state DB one schema version ahead of this binary is refused before any
             part is written (this binary is next release's "previous").
  fresh      the old config over the used prefix with an EMPTY state: no row is lost;
             the rows written again are reported (a re-baseline, measured not judged).
  load       the previous release runs and `rivet load`s an incremental export into
             BigQuery; after a delta this binary runs and loads the same config: the base
             keeps the first load, the buffer holds exactly the delta, the union every
             source id. The previous state carries the cursor and the primary key the load
             needs (RED: dropping it fails the load). The loaded-run skip set is NOT graded:
             init's `cleanup_source: true` deletes the first parts, so erasing it changes
             nothing here (measured).
  resume-load  per engine, a continued-key export into BigQuery (MongoDB `page_size` + `resume`;
             the SQL engines' previous `init --mode chunked` config with its scaffolded
             `keyset_incremental` switched on), run + loaded twice by the previous release, then by
             this binary. A previous release that OVERWROTE (the table holds fewer keys than the
             source had) must be warned about on the first load, and `state reset` + run + load
             must restore every key; one that appended must draw no warning and stay complete.
  cdc-load   per CDC engine into BigQuery, in UTC and a non-UTC source zone: the previous
             `init --mode cdc` config continued by this binary (upgrade_cdc_load.py).
  matrix     every family of `load_mode_of` x engine x ClickHouse and BigQuery: the previous
             release's init, run and load, a change, this binary's run and load, graded by
             rig_oracle.grade_load (upgrade_matrix.py).
  cdc        per CDC engine: the previous release anchors a stream and captures a batch;
             this binary continues its checkpoint and captures exactly the next batch —
             nothing skipped, nothing of the first batch re-read.

`cursor` and `crash` run on the SQLite state and, when the gate grades Postgres state, on
it too. Oracles: DuckDB over the parts the manifests declare, the source's own counts.
"""

from __future__ import annotations

import glob
import json
import os
import re
import shutil
import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path

from .core import Ledger, Proc, first_error, isolate_state_db, rivet_bin, run
from .engines import sql as _sql
from .regression import _require_prev_binary
from .upgrade_cdc_load import cdc_load_cells
from .upgrade_matrix import matrix_cells

__all__ = ["verify_upgrade_continuity"]

SCEN = "upgrade_continuity"
ROWS = 5000
CRASH_ROWS = 250_000
ENGINES = ("postgres", "mysql", "mssql", "oracle")


def _seed(engine: str, url: str, table: str, rows: int, with_cursor: bool) -> bool:
    """(Re)create `table` holding ids 1..rows (and a cursor column when asked)."""
    ts = {"postgres": "TIMESTAMPTZ", "mysql": "DATETIME(6)", "mssql": "DATETIME2", "oracle": "TIMESTAMP"}[engine]
    cols = f"id BIGINT PRIMARY KEY, v BIGINT NOT NULL{f', updated_at {ts} NOT NULL' if with_cursor else ''}"
    if engine == "oracle":
        cols = cols.replace("BIGINT", "NUMBER(19)")
        val = ", TIMESTAMP '2026-01-01 00:00:00' + NUMTODSINTERVAL(level, 'SECOND')" if with_cursor else ""
        body = f"INSERT INTO {table} SELECT level, level{val} FROM dual CONNECT BY level <= {rows};"
    elif engine == "postgres":
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
    if engine == "oracle":
        sql = (f"INSERT INTO {table} SELECT level + {lo - 1}, level + {lo - 1}, TIMESTAMP '2026-06-01 00:00:00' "
               f"+ NUMTODSINTERVAL(level + {lo - 1}, 'SECOND') FROM dual CONNECT BY level <= {hi - lo + 1}; "
               f"UPDATE {table} SET v = -v, updated_at = TIMESTAMP '2026-07-01 00:00:00' WHERE id <= 50;")
    elif engine == "postgres":
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


def _case(engine: str, table: str) -> str:
    """The name as the engine's catalog holds it (Oracle stores unquoted names upper-case)."""
    return table.upper() if engine == "oracle" else table


def _declared(out: Path, select: str) -> list[tuple]:
    """`select` over the parts the manifests under `out` declare (`{parts}` is the relation):
    success manifests, committed parts only — the loader's rule, one definition."""
    import duckdb

    from .scenarios import _manifest_declared_parts

    parts = _manifest_declared_parts(out)
    if not parts:
        return []
    return duckdb.connect().execute(select.format(parts=f"read_parquet({parts})")).fetchall()


def _declared_names(out: Path) -> set[str]:
    """The part file names the manifests under `out` declare, by the same rule as `_declared`."""
    from .scenarios import _manifest_declared_parts

    return {Path(p).name for p in _manifest_declared_parts(out)}


def _part_stats(out: Path) -> dict[str, tuple[int, int]]:
    """Every parquet under `out` by name -> (inode, mtime_ns): a rewrite changes one of them."""
    return {Path(f).name: (os.stat(f).st_ino, os.stat(f).st_mtime_ns)
            for f in glob.glob(str(out / "**" / "*.parquet"), recursive=True)}


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
    table = _case(engine, f"upg_cur_{engine[:2]}_{os.getpid()}_{store.replace('-', '')}")
    if not _seed(engine, url, table, ROWS, with_cursor=True):
        led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/cursor]: seed failed", "seed")
        return
    e = _Env(prev, root, engine, url, table, "incremental", state_url)
    try:
        if not e.init.ok:
            led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/config]: previous init failed: "
                       f"{first_error(e.init.stderr)[:300]}", "init")
            return
        # The upgrade order: the previous release ran first, THEN this binary arrives (its
        # `check` may migrate the state, which the previous one could no longer open).
        ps = _strategy(e.rivet(prev, "check", "-c", "c.yaml"))
        r1 = e.rivet(prev, "run", "-c", "c.yaml")
        if state_url == "":
            cs = _strategy(e.rivet(rivet_bin(), "check", "-c", "c.yaml"))
            if cs and cs == ps:
                led.passed(engine, "-", SCEN, "config", f"upgrade[{engine}/config]: this binary checks the previous init's config the same way ({cs[0]})")
            else:
                led.failed(engine, "-", SCEN, "config", f"upgrade[{engine}/config]: strategy prev {ps} vs this {cs}", "strategy")
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
                       f"{first_error(r2.stderr)[:300]}", "cursor")
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
                   f"{before}->{e.parquet_count()}: {first_error(said)[:300]}", "future")


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
                   f"{first_error(p.stderr)[:300]}", "fresh")


def _crash_leg(led: Ledger, prev: Path, root: Path, engine: str, url: str, state_url: str) -> None:
    store = "pg-state" if state_url else "sqlite"
    table = _case(engine, f"upg_crash_{engine[:2]}_{os.getpid()}_{store.replace('-', '')}")
    if not _seed(engine, url, table, CRASH_ROWS, with_cursor=False):
        led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/crash]: seed failed", "seed")
        return
    e = _Env(prev, root, engine, url, table, "chunked", state_url)
    try:
        crashed = e.rivet(prev, "run", "-c", "c.yaml", extra={"RIVET_TEST_PANIC_AT": "after_keyset_page:1"})
        by_prev = _part_stats(e.dir / "output")
        r = e.rivet(rivet_bin(), "run", "-c", "c.yaml")
        got = _declared(e.dir / "output", "SELECT count(*), count(DISTINCT id) FROM {parts}")
        # Resumed, not restarted: the parts the previous binary wrote before the crash are declared,
        # and none was written again — a restart rewrites the same part names, which the
        # declared set and the counts cannot tell from a resume (the file's inode/mtime can).
        adopted = set(by_prev) & _declared_names(e.dir / "output")
        after = _part_stats(e.dir / "output")
        rewritten = sorted(n for n, st in by_prev.items() if after.get(n) != st)
        if (e.init.ok and not crashed.ok and r.ok and adopted and not rewritten and got
                and got[0] == (CRASH_ROWS, CRASH_ROWS)):
            led.passed(engine, "-", SCEN, store, f"upgrade[{engine}/crash/{store}]: this binary resumed the "
                       f"previous release's crashed keyset run — {CRASH_ROWS} rows, each once")
        else:
            led.failed(engine, "-", SCEN, store, f"upgrade[{engine}/crash/{store}]: init ok={e.init.ok} "
                       f"prev crashed={not crashed.ok} this ok={r.ok} pre-crash parts adopted={len(adopted)}/{len(by_prev)} "
                       f"rewritten={len(rewritten)} (rows, ids)={got}: "
                       f"{first_error(r.stderr)[:300]}", "crash")
    finally:
        _sql(engine, url, f"DROP TABLE IF EXISTS {table};")


CDC_ENGINES = ("postgres", "mysql", "mssql", "mongo", "oracle")
#: Oracle's CDC URL (the capture user) is not RIVET_CDC_ORACLE_URL, which would switch on other stages' Oracle CDC cells.
CDC_URL_VARS = {"oracle": "RIVET_UPG_ORACLE_CDC_URL"}


@contextmanager
def _oracle_cdc_probe(url: str):
    """An `ORC_UPG_PROBE` table the capture user streams, as `cdc_probe` yields it; dropped on exit."""
    from types import SimpleNamespace

    owner = os.environ.get("RIVET_ORACLE_ORACLE_URL", "")
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-upgrade-ora-cdc-"))
    t = "ORC_UPG_PROBE"
    ok = bool(owner) and _sql("oracle", owner, (
        f"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id NUMBER(19) PRIMARY KEY, amount NUMBER(19), "
        f"meta VARCHAR2(200)); ALTER TABLE {t} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS; "
        f'GRANT SELECT ON {t} TO "C##RIVETCDC";')).ok
    try:
        if not ok:
            yield None
            return
        schema = re.search(r"//([^:]+):", owner).group(1).upper()
        (work / "c.yaml").write_text(
            f"source:\n  type: oracle\n  url: \"{url}\"\nexports:\n  - name: orc_cdc_probe\n"
            f"    table: {schema}.{t}\n    mode: cdc\n    format: parquet\n"
            f"    cdc: {{ until_current: true, checkpoint: \"{work}/cdc.ckpt\" }}\n"
            "    destination:\n      type: local\n      path: ./output/\n")
        yield SimpleNamespace(id_col="ID"), work, ""
    finally:
        if owner:
            _sql("oracle", owner, f"DROP TABLE IF EXISTS {t};")


def _oracle_cdc_changes(url: str, lo: int) -> None:
    """`CDC_CHANGES` changes from id `lo` in one transaction: inserts, then updates and deletes."""
    from .perf import CDC_CHANGES

    hi = lo + CDC_CHANGES - 1
    _sql("oracle", os.environ["RIVET_ORACLE_ORACLE_URL"], (
        f"INSERT INTO ORC_UPG_PROBE SELECT level + {lo - 1}, level + {lo - 1}, '{{\"k\":' || (level + {lo - 1}) || '}}' "
        f"FROM dual CONNECT BY level <= {CDC_CHANGES}; "
        f"UPDATE ORC_UPG_PROBE SET amount = amount + 1 WHERE id BETWEEN {lo} AND {hi} AND MOD(id, 10) = 0; "
        f"DELETE FROM ORC_UPG_PROBE WHERE id BETWEEN {lo} AND {hi} AND MOD(id, 17) = 0;"))


def _cdc_leg(led: Ledger, prev: Path, engine: str, url: str) -> None:
    """The previous release anchors a CDC stream and captures a batch; this binary continues its checkpoint."""
    import duckdb

    from .cdc import cdc_probe
    from .perf import CDC_CHANGES, _cdc_changes

    probe_cm, changes = ((_oracle_cdc_probe(url), lambda _e, _u, lo: _oracle_cdc_changes(_u, lo))
                         if engine == "oracle" else (cdc_probe(engine, url), _cdc_changes))
    with probe_cm as probe:
        if probe is None:
            led.failed(engine, "-", SCEN, "local", f"upgrade[{engine}/cdc]: the CDC source setup failed", "setup")
            return
        eng, work, _ = probe
        env = {"RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": ""}
        out = work / "output"
        anchored = run([str(prev), "run", "-c", "c.yaml"], env=env, cwd=work, timeout=None)
        changes(engine, url, 1)
        first = run([str(prev), "run", "-c", "c.yaml"], env=env, cwd=work, timeout=None)
        before = _declared_names(out)
        changes(engine, url, 1 + CDC_CHANGES)
        cont = run([str(rivet_bin()), "run", "-c", "c.yaml"], env=env, cwd=work, timeout=None)
        mine = sorted(str(p) for p in out.rglob("*.parquet") if p.name in _declared_names(out) - before)
        idc = f"CAST({eng.id_col} AS BIGINT)"  # MongoDB's `_id` lands as text
        every = _declared(out, f"SELECT count(DISTINCT {idc}) FROM {{parts}}")
        span = (duckdb.connect().execute(
            f"SELECT min({idc}), count(DISTINCT {idc}) FROM read_parquet({mine})").fetchone()
            if mine else (None, 0))
        ok = (anchored.ok and first.ok and cont.ok
              and every and every[0][0] == 2 * CDC_CHANGES
              and span[0] is not None and span[0] > CDC_CHANGES and span[1] == CDC_CHANGES)
        shown = (f"distinct ids {every[0][0] if every else 0}/{2 * CDC_CHANGES}; this binary's parts "
                 f"hold ids from {span[0]} ({span[1]} distinct, want {CDC_CHANGES} from {CDC_CHANGES + 1})")
        if ok:
            led.passed(engine, "-", SCEN, "local", f"upgrade[{engine}/cdc]: this binary continued the "
                       f"previous release's checkpoint — {shown}", "cdc")
        else:
            led.failed(engine, "-", SCEN, "local", f"upgrade[{engine}/cdc]: prev anchor ok={anchored.ok} "
                       f"prev capture ok={first.ok} this ok={cont.ok}; {shown}: "
                       f"{first_error(cont.stderr or first.stderr or anchored.stderr)[:300]}", "cdc")


def _load_leg(led: Ledger, prev: Path, root: Path, url: str) -> None:
    """The previous release's BigQuery base+buffer and state, continued by this binary's run and load."""
    from . import gcp
    from .bigquery import _bq_json
    from ..pytools.registry import bq_tmp

    proj, bucket = os.environ.get("BQ_ORACLE_PROJECT", ""), os.environ.get("BQ_ORACLE_BUCKET", "")
    if not proj or not bucket:
        led.skipped("postgres", "-", SCEN, "load", "upgrade[postgres/load]: no BQ_ORACLE_PROJECT / "
                    "BQ_ORACLE_BUCKET", "no bigquery")
        return
    table = f"upg_load_{os.getpid()}"
    dset = bq_tmp(f"upg_{os.getpid()}")
    if not _seed("postgres", url, table, ROWS, with_cursor=True):
        led.failed("postgres", "-", SCEN, "load", "upgrade[postgres/load]: seed failed", "seed")
        return
    d = root / "load"
    d.mkdir()
    env = {"RIVET_UPG_URL": url, "RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": ""}
    try:
        gcp.bq_ensure_dataset(proj, dset)
        init = run([str(prev), "init", "--source-env", "RIVET_UPG_URL", "--table", table, "--mode",
                    "incremental", "--gcs-bucket", bucket, "--bigquery-project", proj,
                    "--bigquery-dataset", dset, "-o", "c.yaml"], env=env, cwd=d)
        steps = [(prev, "run"), (prev, "load"), (None, "delta"), (rivet_bin(), "run"), (rivet_bin(), "load")]
        for binary, step in steps:
            if binary is None:
                ok = _mutate("postgres", url, table)
            else:
                p = run([str(binary), step, "-c", "c.yaml"], env=env, cwd=d, timeout=None)
                ok = p.ok
            if not init.ok or not ok:
                why = init.stderr if not init.ok else ("" if binary is None else p.stderr)
                led.failed("postgres", "-", SCEN, "load", f"upgrade[postgres/load]: {step} by "
                           f"{'prev' if binary == prev else 'this'} failed: {first_error(why)[:300]}", step)
                return
        src = _sql("postgres", url, f"SELECT 'rows=' || count(*) FROM {table};")
        m = re.search(r"rows=(\d+)", src.stdout or "") if src.ok else None
        want = int(m.group(1)) if m else None
        # Base + buffer: the first load fills the base, every later one appends to
        # `__changes`. A continued ledger leaves exactly the delta (300 new + 50 changed)
        # in the buffer; a lost one reloads the first run's parts there too.
        t = f"`{proj}.{dset}.{table}`"
        c = f"`{proj}.{dset}.{table}__changes`"
        got = _bq_json(proj, f"SELECT (SELECT count(*) FROM {t}) b, (SELECT count(*) FROM {c}) ch, "
                             f"(SELECT count(DISTINCT id) FROM (SELECT id FROM {t} UNION ALL SELECT id FROM {c})) u")
        b, ch, u = (int(got[0]["b"]), int(got[0]["ch"]), int(got[0]["u"])) if got else (None, None, None)
        if want is not None and (b, ch, u) == (ROWS, 350, want):
            led.passed("postgres", "-", SCEN, "load", f"upgrade[postgres/load]: this binary's load continued "
                       f"the previous release's ledger — base {b}, buffer {ch} (the delta only), {u} ids")
        else:
            led.failed("postgres", "-", SCEN, "load", f"upgrade[postgres/load]: (base, buffer, ids) = "
                       f"{(b, ch, u)}, want ({ROWS}, 350, {want})", "ledger")
    finally:
        _sql("postgres", url, f"DROP TABLE IF EXISTS {table};")
        if not os.environ.get("RIVET_UPG_KEEP"):
            gcp.bq_delete_dataset(proj, dset)
        gcp.gcs_delete_prefix(bucket, f"exports/{table}/")


class _MongoKeys:
    """A Mongo collection of ObjectId documents: the source side of a resume-load cell."""

    def __init__(self, url: str, name: str):
        import pymongo

        self.coll = pymongo.MongoClient(url, serverSelectionTimeoutMS=5000).get_default_database("rivet")[name]

    def add(self, n: int) -> bool:
        self.coll.insert_many([{"v": i} for i in range(n)])
        return True

    def keys(self) -> list:
        return sorted(str(d["_id"]) for d in self.coll.find({}, {"_id": 1}))

    @staticmethod
    def norm(v: str):
        return v

    def drop(self) -> None:
        self.coll.drop()


class _SqlKeys:
    """A SQL table of ids 1..n: the source side of a keyset_incremental resume-load cell."""

    def __init__(self, engine: str, url: str, name: str):
        self.engine, self.url, self.name, self.n = engine, url, name, 0

    def add(self, n: int) -> bool:
        lo, hi = self.n + 1, self.n + n
        if self.n == 0:
            ok = _seed(self.engine, self.url, self.name, n, with_cursor=False)
        elif self.engine == "oracle":
            ok = _sql(self.engine, self.url, f"INSERT INTO {self.name} SELECT level + {lo - 1}, level FROM dual "
                      f"CONNECT BY level <= {n}").ok
        elif self.engine == "postgres":
            ok = _sql(self.engine, self.url, f"INSERT INTO {self.name} SELECT g, g FROM generate_series({lo}, {hi}) g;").ok
        elif self.engine == "mysql":
            ok = _sql(self.engine, self.url, f"INSERT INTO {self.name} WITH RECURSIVE g(n) AS (SELECT {lo} UNION ALL "
                      f"SELECT n + 1 FROM g WHERE n < {hi}) SELECT n, n FROM g;").ok
        else:
            ok = _sql(self.engine, self.url, f"INSERT INTO {self.name} SELECT n, n FROM (SELECT TOP ({n}) "
                      f"ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {lo - 1} AS n FROM sys.all_objects) q;").ok
        self.n = hi if ok else self.n
        return ok

    def keys(self) -> list:
        return list(range(1, self.n + 1))

    @staticmethod
    def norm(v: str):
        return int(v)

    def drop(self) -> None:
        _sql(self.engine, self.url, f"DROP TABLE IF EXISTS {self.name};")


RESUME_LOAD_ENGINES = ("mongo", *ENGINES)


def _continued_key_load_cells(led: Ledger, prev: Path, root: Path) -> None:
    """upgrade[<engine>/resume-load] for every engine with a URL, each recorded PASS, FAIL or a named SKIP."""
    proj, bucket = os.environ.get("BQ_ORACLE_PROJECT", ""), os.environ.get("BQ_ORACLE_BUCKET", "")
    for engine in RESUME_LOAD_ENGINES:
        var = f"RIVET_ORACLE_{engine.upper()}_URL"
        url = os.environ.get(var, "")
        why = "no BQ_ORACLE_PROJECT / BQ_ORACLE_BUCKET" if not (proj and bucket) else f"no {var}" if not url else ""
        if why:
            led.skipped(engine, "-", SCEN, "resume-load", f"upgrade[{engine}/resume-load]: {why}", why)
            continue
        _continued_key_load_leg(led, prev, root, engine, url, proj, bucket)


def _continued_key_load_leg(led: Ledger, prev: Path, root: Path, engine: str, url: str, proj: str,
                            bucket: str) -> None:
    """A continued-key export the previous release loaded twice, then this binary: complete, or warned and restored."""
    from . import gcp
    from ..pytools.registry import bq_tmp

    row, tag = (engine, "-", SCEN, "resume-load"), f"upgrade[{engine}/resume-load]"
    name = _case(engine, f"upg_ck_{engine[:2]}_{os.getpid()}")
    dset = bq_tmp(f"upgck_{engine[:2]}_{os.getpid()}")
    d = root / f"resume-load-{engine}"
    d.mkdir()
    env = {"RIVET_UPG_URL": url, "RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": ""}
    if engine == "mongo":
        try:
            src = _MongoKeys(url, name)
        except ImportError:
            led.skipped(*row, f"{tag}: pymongo absent", "no pymongo")
            return
    else:
        src = _SqlKeys(engine, url, name)

    def step(binary: Path, *args: str) -> Proc | None:
        p = run([str(binary), *args, "-c", "c.yaml"], env=env, cwd=d, timeout=None)
        if not p.ok:
            led.failed(*row, f"{tag}: {' '.join(args)} by {'prev' if binary == prev else 'this'} failed: "
                       f"{first_error(p.stderr)[:300]}", args[0])
        return p if p.ok else None

    def loaded() -> list:
        # The table the load names, matched in BigQuery's own listing (`bq query` caps at 100 rows unless told).
        ls = run(["bq", f"--project_id={proj}", "ls", "--format=json", "--max_results=100", dset], timeout=300)
        names = [t["tableReference"]["tableId"] for t in json.loads(ls.stdout or "[]")] if ls.ok else []
        table = next((t for t in names if t.lower().endswith(name.lower())), None)
        if table is None:
            return []
        key = "_id" if engine == "mongo" else "id"
        p = run(["bq", f"--project_id={proj}", "query", "--nouse_legacy_sql", "--format=json", "--max_rows=1000000",
                 f"SELECT CAST({key} AS STRING) k FROM `{proj}.{dset}.{table}`"], timeout=600)
        return sorted(src.norm(r["k"]) for r in json.loads(p.stdout or "[]")) if p.ok else []

    try:
        gcp.bq_ensure_dataset(proj, dset)
        if not src.add(2000):
            led.failed(*row, f"{tag}: seed failed", "seed")
            return
        if engine == "mongo":
            (d / "c.yaml").write_text(
                "source:\n  type: mongo\n  url_env: RIVET_UPG_URL\n  mongo:\n    page_size: 500\n    resume: true\n"
                f"exports:\n  - name: {name}\n    table: {name}\n    mode: full\n    format: parquet\n"
                f"    destination: {{ type: gcs, bucket: {bucket}, prefix: \"exports/{name}/\" }}\n"
                f"load:\n  target: bigquery\n  project: {proj}\n  dataset: {dset}\n  pk: auto\n")
        else:
            # The previous release's own config, with the opt-in it scaffolds commented out switched on.
            init = run([str(prev), "init", "--source-env", "RIVET_UPG_URL", "--table", name, "--mode", "chunked",
                        "--gcs-bucket", bucket, "--bigquery-project", proj, "--bigquery-dataset", dset,
                        "-o", "c.yaml"], env=env, cwd=d)
            text = (d / "c.yaml").read_text() if init.ok else ""
            if "# keyset_incremental: true" not in text:
                led.failed(*row, f"{tag}: previous init wrote no keyset_incremental opt-in: "
                           f"{first_error(init.stderr)[:300]}", "init")
                return
            (d / "c.yaml").write_text(text.replace("# keyset_incremental: true", "keyset_incremental: true", 1))
        export = re.search(r"^\s*- name: (\S+)", (d / "c.yaml").read_text(), re.M).group(1)
        for _ in range(2):
            if not (step(prev, "run") and step(prev, "load")):
                return
            if not src.add(500):
                led.failed(*row, f"{tag}: insert failed", "seed")
                return
        before, damaged = len(src.keys()) - 500, len(loaded())
        overwrote = damaged < before
        if not step(rivet_bin(), "run"):
            return
        first = step(rivet_bin(), "load")
        if not first:
            return
        warned = (f"was last loaded as a whole-table overwrite, and export `{export}` now loads by append"
                  in first.stderr and f"`rivet state reset -c c.yaml --export {export}`" in first.stderr)
        if warned != overwrote:
            led.failed(*row, f"{tag}: the previous release left {damaged} of {before} keys "
                       f"(overwrote={overwrote}) but this binary's first load warned={warned}", "warning")
            return
        if overwrote and not (step(rivet_bin(), "state", "reset", "--export", export)
                              and step(rivet_bin(), "run") and step(rivet_bin(), "load")):
            return
        want, got = src.keys(), loaded()
        said = (f"the previous release overwrote ({damaged} of {before} keys left); this binary's first load "
                f"warned with the remedy, and the remedy restored" if overwrote else
                f"the previous release appended ({damaged} of {before} keys); this binary's load stayed silent and kept")
        if got == want:
            led.passed(*row, f"{tag}: {said} all {len(want)}")
        else:
            led.failed(*row, f"{tag}: warehouse {len(got)} keys (distinct {len(set(got))}) vs source {len(want)} "
                       f"after {'the remedy' if overwrote else 'the first load'}", "remedy" if overwrote else "append")
    finally:
        src.drop()
        if not os.environ.get("RIVET_UPG_KEEP"):
            gcp.bq_delete_dataset(proj, dset)
        gcp.gcs_delete_prefix(bucket, f"exports/{name}/")


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
            for leg in (_cursor_leg, _crash_leg):
                # Each Postgres-state leg gets a DB the PREVIOUS release creates: the gate's own
                # DB is already migrated by this binary, which the previous one rightly refuses.
                fresh = isolate_state_db(state_url, f"{os.getpid()}_{engine}_{leg.__name__}") \
                    if state_url else ""
                if state_url and not fresh:
                    led.failed(engine, "-", SCEN, "pg-state", f"upgrade[{engine}]: could not create "
                               "a fresh Postgres state DB for the previous release", "no state db")
                    continue
                leg(led, prev, root, engine, url, fresh)
    if os.environ.get("RIVET_ORACLE_POSTGRES_URL"):
        _load_leg(led, prev, root, os.environ["RIVET_ORACLE_POSTGRES_URL"])
    else:
        led.skipped("postgres", "-", SCEN, "load", "upgrade[postgres/load]: no RIVET_ORACLE_POSTGRES_URL", "no url")
    _continued_key_load_cells(led, prev, root)
    cdc_load_cells(led, prev, root)
    matrix_cells(led, prev, root)
    for engine in CDC_ENGINES:
        cvar = CDC_URL_VARS.get(engine, f"RIVET_CDC_{engine.upper()}_URL")
        curl = os.environ.get(cvar, "")
        if not curl:
            led.skipped(engine, "-", SCEN, "local", f"upgrade[{engine}/cdc]: no {cvar}", "no url")
            continue
        _cdc_leg(led, prev, engine, curl)


if __name__ == "__main__":
    # The warehouse legs alone: `RIVET_PREV_RELEASE_BIN=<old rivet> python -m dev.release_oracle.upgrade`.
    _led = Ledger()
    _prev = _require_prev_binary(_led, "all", "-", SCEN, "warehouse", "upgrade continuity")
    if _prev is not None:
        _root = Path(tempfile.mkdtemp(prefix="rivet-oracle-upgrade-"))
        if os.environ.get("RIVET_ORACLE_POSTGRES_URL"):
            _load_leg(_led, _prev, _root, os.environ["RIVET_ORACLE_POSTGRES_URL"])
        else:
            _led.skipped("postgres", "-", SCEN, "load", "upgrade[postgres/load]: no RIVET_ORACLE_POSTGRES_URL",
                         "no url")
        _continued_key_load_cells(_led, _prev, _root)
        cdc_load_cells(_led, _prev, _root)
    raise SystemExit(_led.report())
