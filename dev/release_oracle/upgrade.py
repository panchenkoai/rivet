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
  load       the previous release runs and `rivet load`s an incremental export into
             BigQuery; after a delta this binary runs and loads the same config: the base
             keeps the first load, the buffer holds exactly the delta, the union every
             source id. The previous state carries the cursor and the primary key the load
             needs (RED: dropping it fails the load). The loaded-run skip set is NOT graded:
             init's `cleanup_source: true` deletes the first parts, so erasing it changes
             nothing here (measured).
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
from pathlib import Path

from .core import Ledger, Proc, isolate_state_db, rivet_bin, run
from .engines import sql as _sql
from .regression import _require_prev_binary

__all__ = ["verify_upgrade_continuity"]

SCEN = "upgrade_continuity"
ROWS = 5000
CRASH_ROWS = 250_000
ENGINES = ("postgres", "mysql", "mssql")


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


CDC_ENGINES = ("postgres", "mysql", "mssql", "mongo")


def _cdc_leg(led: Ledger, prev: Path, engine: str, url: str) -> None:
    """The previous release anchors a CDC stream and captures a batch; this binary continues its checkpoint."""
    import duckdb

    from .cdc import cdc_probe
    from .perf import CDC_CHANGES, _cdc_changes

    with cdc_probe(engine, url) as probe:
        if probe is None:
            led.failed(engine, "-", SCEN, "local", f"upgrade[{engine}/cdc]: the CDC source setup failed", "setup")
            return
        eng, work, _ = probe
        env = {"RIVET_STATE_URL": ""}
        out = work / "output"
        anchored = run([str(prev), "run", "-c", "c.yaml"], env=env, cwd=work, timeout=None)
        _cdc_changes(engine, url, 1)
        first = run([str(prev), "run", "-c", "c.yaml"], env=env, cwd=work, timeout=None)
        before = _declared_names(out)
        _cdc_changes(engine, url, 1 + CDC_CHANGES)
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
                       f"{(cont.stderr or first.stderr or anchored.stderr).strip()[-200:]}", "cdc")


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
    env = {"RIVET_UPG_URL": url, "RIVET_STATE_URL": ""}
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
                           f"{'prev' if binary == prev else 'this'} failed: {why.strip()[-200:]}", step)
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


CDC_TABLES = 3
CDC_SEED = 5


def _cdc_fp_mysql(url: str, table: str) -> tuple[str, set[int]]:
    """The source's rows as one `id:v:epoch` string, and its id set."""
    from .cdc import _mysql

    p = _mysql(url, "SET SESSION group_concat_max_len = 1000000; SELECT IFNULL(GROUP_CONCAT("
                    f"CONCAT(id, ':', v, ':', UNIX_TIMESTAMP(created_at)) ORDER BY id SEPARATOR ','), '') "
                    f"AS fp FROM {table};")
    if not p.ok:
        raise RuntimeError(f"source read of {table} failed: {(p.stderr or '').strip()[-200:]}")
    lines = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
    fp = lines[1] if len(lines) >= 2 else ""  # line 0 is the `fp` header
    return fp, {int(x.split(":")[0]) for x in fp.split(",") if ":" in x}


def _cdc_fp_bq(proj: str, fq: str) -> tuple[str, str, int, bool]:
    """(live fingerprint, flagged ids, rows minus distinct ids, buffer left) for one base table."""
    from . import gcp

    live = gcp.bq_scalar(proj, "SELECT IFNULL(STRING_AGG(FORMAT('%d:%d:%d', id, v, UNIX_SECONDS("
                               f"created_at)), ',' ORDER BY id), '') FROM `{fq}` WHERE NOT __is_deleted") or ""
    gone = gcp.bq_scalar(proj, "SELECT IFNULL(STRING_AGG(CAST(id AS STRING), ',' ORDER BY id), '') "
                               f"FROM `{fq}` WHERE __is_deleted") or ""
    dup = int(gcp.bq_scalar(proj, f"SELECT COUNT(*) - COUNT(DISTINCT id) FROM `{fq}`") or 0)
    ds, t = fq.rsplit(".", 1)
    buf = int(gcp.bq_scalar(proj, f"SELECT COUNT(*) FROM `{ds}.INFORMATION_SCHEMA.TABLES` "
                                  f"WHERE table_name = '{t}__changes'") or 0) > 0
    return live, gone, dup, buf


def _cdc_delta(url: str, tables: list[str], cycle: int) -> None:
    """Cycle-specific inserts, updates, a partition-moving update and a delete in every table."""
    from .cdc import _mysql

    for k, t in enumerate(tables):
        b = k * 100
        ins = ", ".join(f"({b + 10 * cycle + i}, {i}, '2026-02-0{cycle} 12:00:00')" for i in (1, 2, 3))
        sql = [f"INSERT INTO {t} (id, v, created_at) VALUES {ins};",
               f"UPDATE {t} SET v = {1000 * cycle} WHERE id = {b + 1 + cycle % 5};",
               f"DELETE FROM {t} WHERE id = {b + cycle};"]
        if cycle >= 2:
            # A row whose partition day moves, and a row the previous cycle inserted.
            sql += [f"UPDATE {t} SET created_at = '2025-12-2{cycle} 08:00:00' WHERE id = {b + 5};",
                    f"UPDATE {t} SET v = -v WHERE id = {b + 10 * (cycle - 1) + 1};"]
        _mysql(url, " ".join(sql))


def _cdc_load_leg(led: Ledger, prev: Path, root: Path, url: str) -> None:
    """The partner cycle: the previous release's `init --mode cdc` config, run → load → compact
    three times by it, then twice by this binary; the compacted base equals the source by value."""
    from . import gcp
    from .cdc import _mysql
    from ..pytools.registry import bq_tmp

    proj, bucket = os.environ.get("BQ_ORACLE_PROJECT", ""), os.environ.get("BQ_ORACLE_BUCKET", "")
    if not proj or not bucket:
        led.skipped("mysql", "-", SCEN, "warehouse", "upgrade[mysql/cdc-warehouse]: no "
                    "BQ_ORACLE_PROJECT / BQ_ORACLE_BUCKET", "no bigquery")
        return
    pid = os.getpid()
    tables = [f"upg_cdcw_{pid}_{k}" for k in range(CDC_TABLES)]
    dset = bq_tmp(f"upgcdc_{pid}")
    pfx = f"upgrade-cdc/{pid}"
    d = root / "cdc_warehouse"
    d.mkdir()
    env = {"RIVET_UPG_URL": url, "RIVET_STATE_URL": ""}
    seen: dict[str, set[int]] = {t: set() for t in tables}

    def fail(stage: str, why: str) -> None:
        led.failed("mysql", "-", SCEN, "warehouse", f"upgrade[mysql/cdc-warehouse]: {stage}: {why}", stage)

    try:
        for k, t in enumerate(tables):
            rows = ", ".join(f"({k * 100 + i}, {i}, '2026-01-0{i} 10:00:00')" for i in range(1, CDC_SEED + 1))
            if not _mysql(url, f"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id BIGINT PRIMARY KEY, "
                               f"v INT NOT NULL, created_at TIMESTAMP NOT NULL); "
                               f"INSERT INTO {t} VALUES {rows};").ok:
                return fail("seed", t)
        gcp.bq_ensure_dataset(proj, dset)
        init = run([str(prev), "init", "--source-env", "RIVET_UPG_URL", "--mode", "cdc", "--include",
                    *tables, "--tls", "disable", "--gcs-bucket", bucket, "--bigquery-project", proj,
                    "--bigquery-dataset", dset, "-o", "c.yaml"], env=env, cwd=d)
        if not init.ok:
            return fail("init", f"previous init: {(init.stderr or '').strip()[-240:]}")
        # Harness isolation only: init writes fixed prefixes, shared by every run on the stand bucket.
        cfg = d / "c.yaml"
        body = cfg.read_text()
        cfg.write_text(body.replace("prefix: exports/", f"prefix: {pfx}/exports/")
                           .replace("prefix: cdc/", f"prefix: {pfx}/cdc/"))
        cycles = [(prev, 0), (prev, 1), (prev, 2), (rivet_bin(), 3), (rivet_bin(), 4)]
        for n, (binary, delta) in enumerate(cycles, 1):
            who = "prev" if binary == prev else "this"
            if delta:
                _cdc_delta(url, tables, delta)
            if binary != prev and cycles[n - 2][0] == prev:
                chk = run([str(binary), "check", "-c", "c.yaml"], env=env, cwd=d, timeout=None)
                if not chk.ok:
                    return fail("check", f"this binary refuses the previous init's config: "
                                         f"{(chk.stderr or chk.out).strip()[-240:]}")
            for step in ("run", "load", "compact"):
                extra = [] if step == "run" else ["--run-id", f"upg-{pid}-{n}"]
                p = run([str(binary), step, "-c", "c.yaml", *extra], env=env, cwd=d, timeout=None)
                if not p.ok:
                    return fail(f"cycle{n}/{step}", f"{who} exit {p.returncode}: {(p.stderr or '').strip()[-240:]}")
            bad = []
            for t in tables:
                src, ids = _cdc_fp_mysql(url, t)
                seen[t] |= ids
                live, gone, dup, buf = _cdc_fp_bq(proj, f"{proj}.{dset}.{t}")
                want_gone = ",".join(str(i) for i in sorted(seen[t] - ids))
                if (live, gone, dup, buf) != (src, want_gone, 0, False):
                    bad.append(f"{t}: live={live!r} src={src!r} flagged={gone!r} want={want_gone!r} "
                               f"dup={dup} buffer_left={buf}")
            if bad:
                return fail(f"cycle{n}", f"after {who}'s compact the base differs from the source — "
                                         + "; ".join(bad)[:600])
        part = gcp.bq_scalar(proj, f"SELECT IFNULL(STRING_AGG(column_name), '') FROM `{proj}.{dset}"
                                   f".INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{tables[0]}' "
                                   "AND is_partitioning_column = 'YES'")
        led.passed("mysql", "-", SCEN, "warehouse", f"upgrade[mysql/cdc-warehouse]: three cycles by the "
                   f"previous release, two by this binary on its init config; after every compact each "
                   f"of {CDC_TABLES} bases equals the source by value, deletes flagged, no duplicate key, "
                   f"no buffer left (partition column: {part or 'none'})", "cdc-warehouse")
    finally:
        for t in tables:
            _mysql(url, f"DROP TABLE IF EXISTS {t};")
        if not os.environ.get("RIVET_UPG_KEEP"):
            gcp.bq_delete_dataset(proj, dset)
        gcp.gcs_delete_prefix(bucket, f"{pfx}/")


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
    if os.environ.get("RIVET_CDC_MYSQL_URL"):
        _cdc_load_leg(led, prev, root, os.environ["RIVET_CDC_MYSQL_URL"])
    for engine in CDC_ENGINES:
        cvar = f"RIVET_CDC_{engine.upper()}_URL"
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
        if os.environ.get("RIVET_CDC_MYSQL_URL"):
            _cdc_load_leg(_led, _prev, _root, os.environ["RIVET_CDC_MYSQL_URL"])
    raise SystemExit(_led.report())
