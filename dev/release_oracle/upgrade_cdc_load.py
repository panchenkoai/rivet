"""upgrade[<engine>/cdc-load]: the previous release's `init --mode cdc` → BigQuery cycle, continued by this binary.

Per CDC engine (MySQL, PostgreSQL, SQL Server, Oracle, MongoDB), on three tables: run → load → compact
three cycles by the previous release and two by this binary, with inserts, updates, a partition-moving
update and a delete between cycles. After every compact each base's live rows equal the source
`id:v:epoch`, every deleted id is flagged `__is_deleted`, no key is there twice and no `__changes`
buffer is left. The source epoch is computed by the source engine itself, never read from a
session-rendered text. Every SQL engine runs a second time with a non-UTC zone (see `Src.tz_note`).
Every engine runs once more as `/init=this`: THIS binary's init config, five cycles by this binary, so
a fix in init is graded before the previous release carries it.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from .core import Ledger, Proc, rivet_bin, run, run_lanes, server_of

SCEN = "upgrade_continuity"
CDC_TABLES = 3
CDC_SEED = 5


def _ops(k: int, cycle: int) -> list[tuple]:
    """Table `k`'s changes for `cycle` (0 = the seed): inserts, updates, a partition-moving update, a delete."""
    b = k * 100
    if cycle == 0:
        return [("ins", b + i, i, f"2026-01-0{i} 10:00:00") for i in range(1, CDC_SEED + 1)]
    ops = [("ins", b + 10 * cycle + i, i, f"2026-02-0{cycle} 12:00:00") for i in (1, 2, 3)]
    ops += [("set_v", b + 1 + cycle % 5, 1000 * cycle), ("del", b + cycle)]
    if cycle >= 2:
        # A row whose partition day moves, and a row the previous cycle inserted.
        ops += [("set_ts", b + 5, f"2025-12-2{cycle} 08:00:00"), ("neg", b + 10 * (cycle - 1) + 1)]
    return ops


class Src:
    """One cdc-load cell's source: tables, changes, the engine-computed fingerprint, set-up and tear-down."""

    ts_type = "TIMESTAMP"
    anchor_first = False  # init writes no baseline: anchor the stream before the seed
    init_args: tuple[str, ...] = ("--tls", "disable")
    bq_id = "id"
    bq_v = "CAST(v AS BIGINT)"
    bq_epoch = "epoch(created_at)"
    tz_note = ""

    def __init__(self, url: str, tz: str | None, tag: str):
        self.url, self.tz, self.tag = url, tz, tag
        self.rivet_url, self.slot, self.attach, self.env = url, None, {}, {}

    def name(self, k: int) -> str:
        return f"upg_cdcw_{self.tag}_{k}"

    def bq_table(self, t: str) -> str:
        """The warehouse table the load names for source table `t`."""
        return t

    def lit(self, ts: str) -> str:
        return f"'{ts}'"

    def sql(self, stmts: list[str]) -> bool:
        raise NotImplementedError

    def create(self, t: str) -> bool:
        return self.sql([f"DROP TABLE IF EXISTS {t}",
                         f"CREATE TABLE {t} (id BIGINT PRIMARY KEY, v INT NOT NULL, created_at {self.ts_type} NOT NULL)"])

    def drop(self, t: str) -> None:
        self.sql([f"DROP TABLE IF EXISTS {t}"])

    def render(self, t: str, op: tuple) -> str:
        kind, i = op[0], op[1]
        return {
            "ins": lambda: f"INSERT INTO {t} (id, v, created_at) VALUES ({i}, {op[2]}, {self.lit(op[3])})",
            "set_v": lambda: f"UPDATE {t} SET v = {op[2]} WHERE id = {i}",
            "set_ts": lambda: f"UPDATE {t} SET created_at = {self.lit(op[2])} WHERE id = {i}",
            "neg": lambda: f"UPDATE {t} SET v = -v WHERE id = {i}",
            "del": lambda: f"DELETE FROM {t} WHERE id = {i}",
        }[kind]()

    def apply(self, t: str, ops: list[tuple]) -> bool:
        return self.sql([self.render(t, op) for op in ops])

    def fp(self, o, t: str) -> str:
        """`id:v:epoch` of every source row, ordered by id, the epoch computed by the engine."""
        raise NotImplementedError

    def __enter__(self) -> "Src":
        return self

    def __exit__(self, *_exc) -> None:
        return None


def _join(rows) -> str:
    # Numeric order: a fingerprint parsed from text (sqlcmd, mongosh) must sort like one read as integers.
    return ",".join(f"{i}:{v}:{e}" for i, v, e in sorted((int(i), int(v), int(e)) for i, v, e in rows))


class MySQL(Src):
    tz_note = "the server's GLOBAL time_zone, restored on exit"

    def sql(self, stmts):
        from .cdc import _mysql
        return _mysql(self.url, "; ".join(stmts) + ";").ok

    def fp(self, o, t):
        return _join(o.rows(f"SELECT * FROM mysql_query('my', 'SELECT id, v, UNIX_TIMESTAMP(created_at) FROM {t}')"))

    def _root(self, q: str) -> Proc:
        from .cdc import _container_for, _no_container
        from .core import docker_exec
        c = _container_for(self.url)
        return docker_exec(c, "mysql", "-uroot", "-privet", "-N", stdin=q) if c else _no_container(self.url)

    def __enter__(self):
        from .cdc import _mysql
        self.attach = {"mysql": self.url}
        if self.tz:
            self.was = self._root("SELECT @@GLOBAL.time_zone;").stdout.strip()
            if (not self.was or not self._root(f"SET GLOBAL time_zone = '{self.tz}';").ok
                    or _mysql(self.url, "SELECT @@session.time_zone;").stdout.split()[-1:] != [self.tz]):
                raise RuntimeError(f"could not set the MySQL GLOBAL time_zone to {self.tz} (was {self.was!r})")
        return self

    def __exit__(self, *_exc):
        if self.tz:
            self._root(f"SET GLOBAL time_zone = '{self.was}';")


class Postgres(Src):
    ts_type = "TIMESTAMPTZ"
    tz_note = "the default TimeZone of a database of the cell's own"

    def __init__(self, url, tz, tag):
        from urllib.parse import urlsplit, urlunsplit
        super().__init__(url, tz, tag)
        self.db = self.slot = f"upg_{tag}"
        self.rivet_url = urlunsplit(urlsplit(url)._replace(path=f"/{self.db}"))
        self.attach = {"postgres": self.rivet_url}

    def _psql(self, q: str, on: str = "rivet") -> Proc:
        from .cdc import _container_for, _no_container
        from .core import docker_exec
        c = _container_for(self.url)
        return docker_exec(c, "psql", "-U", "rivet", "-d", on, "-v", "ON_ERROR_STOP=1", "-tAq",
                           stdin=q) if c else _no_container(self.url)

    def sql(self, stmts):
        return self._psql("; ".join(stmts) + ";", self.db).ok

    def fp(self, o, t):
        return _join(o.rows("SELECT * FROM postgres_query('pg', 'SELECT id, v, extract(epoch FROM created_at)::bigint "
                            f"FROM {t}')"))

    def _drop_db(self) -> None:
        # The slot first: a database a logical slot points into cannot be dropped.
        self._psql("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                   f"WHERE slot_name = '{self.slot}' AND active_pid IS NOT NULL;")
        self._psql(f"SELECT pg_drop_replication_slot('{self.slot}') FROM pg_replication_slots "
                   f"WHERE slot_name = '{self.slot}';")
        self._psql(f"DROP DATABASE IF EXISTS {self.db} WITH (FORCE);")

    def __enter__(self):
        self._drop_db()
        made = self._psql(f"CREATE DATABASE {self.db};")
        if not made.ok or (self.tz and (not self._psql(f"ALTER DATABASE {self.db} SET TimeZone = '{self.tz}';").ok
                                        or self._psql("SHOW TimeZone;", self.db).stdout.strip() != self.tz)):
            self._drop_db()
            raise RuntimeError(f"could not create the cell's database {self.db}: {made.stderr.strip()[-200:]}")
        return self

    def __exit__(self, *_exc):
        self._drop_db()


class MSSQL(Src):
    ts_type = "DATETIMEOFFSET"
    anchor_first = True
    tz_note = ("SQL Server has no session zone: every value carries a +09:00 offset and rivet runs "
               "under TZ=Asia/Tokyo")

    def __init__(self, url, tz, tag):
        super().__init__(url, tz, tag)
        if tz:
            self.env = {"TZ": "Asia/Tokyo"}

    def bq_table(self, t):
        return f"dbo_{t}"  # the load names a SQL Server table `<schema>_<table>`

    def lit(self, ts):
        return f"'{ts} {self.tz or '+00:00'}'"

    def sql(self, stmts):
        from .cdc import _sqlcmd
        return _sqlcmd(self.url, sql=";\n".join(stmts) + ";\n").ok

    def create(self, t):
        from .cdc import _lc_drop, _sqlcmd
        from .core import wait_until
        _lc_drop("mssql", self.url, t)
        if not self.sql([f"CREATE TABLE dbo.{t} (id BIGINT PRIMARY KEY, v INT NOT NULL, created_at DATETIMEOFFSET NOT NULL)",
                         f"EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='{t}', "
                         f"@role_name=NULL, @capture_instance='dbo_{t}'"]):
            return False
        # The instance is usable once the capture job has run (fn_cdc_get_min_lsn non-NULL).
        return wait_until(lambda: "NULL" not in _sqlcmd(
            self.url, q=f"SET NOCOUNT ON; SELECT ISNULL(CONVERT(varchar(64), sys.fn_cdc_get_min_lsn('dbo_{t}'), 1), 'NULL')"
        ).stdout, tries=40, delay=1.5)

    def drop(self, t):
        from .cdc import _lc_drop
        _lc_drop("mssql", self.url, t)

    def apply(self, t, ops):
        from .cdc import _mssql_max_lsn, _wait_mssql_captured
        before = _mssql_max_lsn(self.url)
        ok = super().apply(t, ops)
        _wait_mssql_captured(self.url, before)  # the capture job settles before the next run
        return ok

    def fp(self, o, t):
        from .cdc import _sqlcmd
        out = _sqlcmd(self.url, q=f"SET NOCOUNT ON; SELECT CONCAT(id, ':', v, ':', DATEDIFF_BIG(second, "
                                  f"'1970-01-01', SWITCHOFFSET(created_at, 0))) FROM dbo.{t}").stdout
        # sqlcmd pads a column to its width: the trailing blanks are not part of the value.
        return _join(tuple(ln.split(":")) for ln in re.findall(r"^(-?\d+:-?\d+:-?\d+) *$", out, re.M))


class Oracle(Src):
    """Oracle has no DuckDB scanner: the source fingerprint is read through python-oracledb."""

    ts_type = "TIMESTAMP WITH TIME ZONE"
    anchor_first = True
    tz_note = ("the writing session's TIME_ZONE (values stored with the +09:00 region) and rivet's "
               "client zone (TZ / ORA_SDTZ)")

    def __init__(self, url, tz, tag):
        from urllib.parse import unquote, urlparse
        super().__init__(url, tz, tag)
        self.owner_url = os.environ["RIVET_ORACLE_ORACLE_URL"]
        self.owner = unquote(urlparse(self.owner_url).username or "").upper()
        self.capture = unquote(urlparse(url).username or "")
        self.init_args = ("--schema", self.owner)
        if tz:
            self.env = {"TZ": tz, "ORA_SDTZ": tz}

    def name(self, k):
        return super().name(k).upper()

    def lit(self, ts):
        return f"TIMESTAMP '{ts}'"

    def sql(self, stmts):
        from urllib.parse import unquote, urlparse

        import oracledb
        u = urlparse(self.owner_url)
        try:
            with oracledb.connect(user=unquote(u.username or ""), password=unquote(u.password or ""),
                                  dsn=f"{u.hostname}:{u.port or 1521}/{u.path.lstrip('/')}") as con:
                cur = con.cursor()
                cur.execute(f"ALTER SESSION SET TIME_ZONE = '{self.tz or 'UTC'}'")
                for s in stmts:
                    cur.execute(s)
                con.commit()
            return True
        except oracledb.DatabaseError:
            return False

    def create(self, t):
        self.drop(t)
        return self.sql([f"CREATE TABLE {t} (id NUMBER(18) PRIMARY KEY, v NUMBER(10) NOT NULL, "
                         "created_at TIMESTAMP WITH TIME ZONE NOT NULL)",
                         f"ALTER TABLE {t} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
                         f'GRANT SELECT ON {t} TO "{self.capture.upper()}"'])

    def drop(self, t):
        self.sql([f"DROP TABLE {t} PURGE"])

    def fp(self, o, t):
        from .value_diff import oracle_rows
        rows = oracle_rows(self.owner_url, "SELECT id, v, (CAST(SYS_EXTRACT_UTC(created_at) AS DATE) - "
                                           f"DATE '1970-01-01') * 86400 e FROM {t}")
        return _join((r["ID"], r["V"], r["E"]) for r in rows)


class Mongo(Src):
    anchor_first = True
    init_args = ()
    bq_id = "CAST(_id AS BIGINT)"  # MongoDB's `_id` lands as text, the rest as relaxed extended JSON
    bq_v = "CAST(json_extract(document, '$.v') AS BIGINT)"
    bq_epoch = "epoch(CAST(json_extract_string(document, '$.created_at.\"$date\"') AS TIMESTAMPTZ))"

    def sql(self, stmts):
        from .cdc import _mongosh
        return _mongosh(self.url, "; ".join(stmts)).ok

    def create(self, t):
        return self.sql([f"db.{t}.drop()", f'db.createCollection("{t}")'])

    def drop(self, t):
        self.sql([f"db.{t}.drop()"])

    def render(self, t, op):
        kind, i = op[0], f"NumberLong({op[1]})"
        date = (lambda s: f'ISODate("{s.replace(" ", "T")}Z")')
        return {
            "ins": lambda: f"db.{t}.insertOne({{_id: {i}, v: NumberLong({op[2]}), created_at: {date(op[3])}}})",
            "set_v": lambda: f"db.{t}.updateOne({{_id: {i}}}, {{$set: {{v: NumberLong({op[2]})}}}})",
            "set_ts": lambda: f"db.{t}.updateOne({{_id: {i}}}, {{$set: {{created_at: {date(op[2])}}}}})",
            "neg": lambda: f"db.{t}.updateOne({{_id: {i}}}, {{$mul: {{v: NumberLong(-1)}}}})",
            "del": lambda: f"db.{t}.deleteOne({{_id: {i}}})",
        }[kind]()

    def fp(self, o, t):
        from .cdc import _mongosh
        out = _mongosh(self.url, f"db.{t}.find().toArray().forEach(d => print('ROW ' + d._id + ':' + d.v + ':' + "
                                 "Math.floor(d.created_at.getTime() / 1000)))").stdout
        return _join(tuple(m.split(":")) for m in re.findall(r"^ROW (-?\d+:-?\d+:-?\d+)$", out, re.M))


#: Per cdc-load engine: the source class, the URL env vars it needs, its non-UTC zone (None: no variant).
#: Oracle's capture URL is RIVET_UPG_ORACLE_CDC_URL, not RIVET_CDC_ORACLE_URL, which would switch on
#: other stages' Oracle CDC cells.
CDC_LOAD_ENGINES: dict[str, tuple[type[Src], tuple[str, ...], str | None]] = {
    "mysql": (MySQL, ("RIVET_CDC_MYSQL_URL",), "+09:00"),
    "postgres": (Postgres, ("RIVET_CDC_POSTGRES_URL",), "Asia/Tokyo"),
    "mssql": (MSSQL, ("RIVET_CDC_MSSQL_URL",), "+09:00"),
    "oracle": (Oracle, ("RIVET_UPG_ORACLE_CDC_URL", "RIVET_ORACLE_ORACLE_URL"), "Asia/Tokyo"),
    "mongo": (Mongo, ("RIVET_CDC_MONGO_URL",), None),
}


def _state(o, src: Src, dset: str, t: str) -> tuple[str, str, str, int, bool]:
    """(source `id:v:epoch`, base live `id:v:epoch`, flagged ids, rows minus distinct ids, buffer exists)."""
    w = src.bq_table(t)
    rel = f'bq.{dset}."{w}"'
    i = src.bq_id
    live = _join(o.rows(f"SELECT {i}, {src.bq_v}, {src.bq_epoch} FROM {rel} WHERE NOT __is_deleted"))
    gone = ",".join(str(int(r[0])) for r in sorted(o.rows(f"SELECT {i} FROM {rel} WHERE __is_deleted")))
    dup = o.scalar(f"SELECT count(*) - count(DISTINCT {i}) FROM {rel}")
    buf = o.scalar("SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'bq' "
                   f"AND table_schema = '{dset}' AND table_name = '{w}__changes'")
    return src.fp(o, t), live, gone, dup, buf > 0


def _oracle_no_load(led: Ledger, name: str, fail, step, body: str, bucket: str, pfx: str,
                    tables: list[str], src: Src, d: Path) -> None:
    """Oracle CDC does not load yet (ADR-0037; the GA work): init writes no `load:` block, says why,
    and `run` writes every table's baseline to Parquet whose `id:v:epoch` equals the source's."""
    import duckdb

    from . import gcp
    from .scenarios import _declared_read

    if "\nload:" in body or "ADR-0037" not in body:
        return fail("init", "an Oracle CDC scaffold must carry no `load:` block and name ADR-0037: " + body[-400:])
    r = step(rivet_bin(), "run", "-c", "c.yaml")
    if not r.ok:
        return fail("run", f"this {r.why}")
    names = gcp.gcs_list(bucket, f"{pfx}/")
    bad = []
    for t in tables:
        # The snapshot leg's objects, then only the parts its manifest DECLARES (what a consumer reads).
        objs = [n for n in names if f"/{t}/cdc/snapshot/" in n and "/" not in n.split("/snapshot/", 1)[1]]
        local = d / "baseline" / t
        local.mkdir(parents=True)
        for n in objs:
            gcp.gcs_download(bucket, n, local / n.rsplit("/", 1)[1])
        declared = _declared_read(local, ".parquet")
        if declared is None:
            bad.append(f"{t}: the manifest declares no baseline Parquet ({len(objs)} objects)")
            continue
        got = _join(duckdb.sql(f"SELECT ID, V, epoch(CREATED_AT) FROM read_parquet({declared})").fetchall())
        want = src.fp(None, t)
        if not want or got != want:
            bad.append(f"{t}: parquet={got!r} src={want!r}")
    if bad:
        return fail("run", "the baseline Parquet differs from the source — " + "; ".join(bad)[:600])
    led.passed("oracle", "-", SCEN, "cdc-load", f"{name}: init writes no `load:` block (Oracle CDC load is "
               f"the GA work, ADR-0037), `check` passes, `run` writes each of {len(tables)} tables' baseline "
               "to Parquet equal to the source `id:v:epoch` (DuckDB over the Parquet, python-oracledb over "
               "the source)", "cdc-load")


def cdc_load_leg(led: Ledger, prev: Path, root: Path, engine: str, url: str, tz: str | None = None,
                 init_this: bool = False) -> None:
    """One cdc-load cell: the previous release's init config, three cycles by it and two by this binary;
    with `init_this`, this binary's init config and five cycles by this binary."""
    import duckdb

    from . import gcp
    from .duck import BQ_DATASET_ENV, BQ_PROJECT_ENV, Oracle as Duck, OracleUnavailable, bq_target, retry
    from ..pytools.registry import bq_tmp

    name = f"upgrade[{engine}/cdc-load{'/init=this' if init_this else ''}{f'/tz={tz}' if tz else ''}]"
    target, bucket = bq_target(), os.environ.get("BQ_ORACLE_BUCKET", "")
    if target is None or not bucket:
        led.skipped(engine, "-", SCEN, "cdc-load", f"{name}: no {BQ_PROJECT_ENV} / {BQ_DATASET_ENV} "
                    "/ BQ_ORACLE_BUCKET", "no bigquery")
        return
    proj = target[0]
    tag = f"{engine[:2]}{'tz' if tz else ''}{'n' if init_this else ''}_{os.getpid()}"
    initer = rivet_bin() if init_this else prev
    # 0.30.0's init writes no baseline for a per-table stream; this tree's always does.
    anchor_first = CDC_LOAD_ENGINES[engine][0].anchor_first and not init_this
    src = CDC_LOAD_ENGINES[engine][0](url, tz, tag)
    tables = [src.name(k) for k in range(CDC_TABLES)]
    dset = bq_tmp(f"upgcdc_{tag}")
    pfx = f"upgrade-cdc/{tag}"
    d = root / f"cdc_load_{tag}"
    d.mkdir()
    seen: dict[str, set[int]] = {t: set() for t in tables}
    part = ""

    def fail(stage: str, why: str) -> None:
        led.failed(engine, "-", SCEN, "cdc-load", f"{name}: {stage}: {why}", stage)

    def buffers() -> list[bool]:
        with Duck(bigquery=True, bq_dataset=dset, **src.attach) as o:
            return [o.scalar("SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'bq' "
                             f"AND table_schema = '{dset}' AND table_name = '{src.bq_table(t)}__changes'") > 0 for t in tables]

    try:
        with src:
            env = {"RIVET_UPG_URL": src.rivet_url, "RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": "", **src.env}

            def step(binary: Path, *args: str) -> Proc:
                return run([str(binary), *args], env=env, cwd=d, timeout=None)

            for t in tables:
                if not src.create(t):
                    return fail("seed", f"could not create {t}")
            if not anchor_first and not all(src.apply(t, _ops(k, 0)) for k, t in enumerate(tables)):
                return fail("seed", "the seed rows failed")
            gcp.bq_ensure_dataset(proj, dset)
            init = step(initer, "init", "--source-env", "RIVET_UPG_URL", "--mode", "cdc", "--include", *tables,
                        *src.init_args, "--gcs-bucket", bucket, "--bigquery-project", proj,
                        "--bigquery-dataset", dset, "-o", "c.yaml")
            if not init.ok:
                return fail("init", f"{'this' if init_this else 'previous'} init: "
                                    f"{init.why}")
            # Harness isolation only: init writes fixed prefixes and a fixed PG slot name,
            # shared by every run on the stand.
            cfg = d / "c.yaml"
            body = re.sub(r"prefix: (exports|cdc)/", rf"prefix: {pfx}/\1/", cfg.read_text())
            if src.slot:
                body = re.sub(r"slot: \S+", f"slot: {src.slot}", body)
            cfg.write_text(body)
            if [ln for ln in body.splitlines() if "prefix:" in ln and pfx not in ln] or (
                    src.slot and f"slot: {src.slot}" not in body):
                return fail("init", "a prefix or slot the harness could not isolate: " + body[:400])
            if init_this:
                chk = step(initer, "check", "-c", "c.yaml")
                if not chk.ok:
                    return fail("check", f"this binary refuses its own init's config: {chk.why}")
            if init_this and engine == "oracle":
                return _oracle_no_load(led, name, fail, step, body, bucket, pfx, tables, src, d)
            if anchor_first:
                # init's own advice for a changes-only stream: anchor first, then the rows arrive.
                a = step(prev, "run", "-c", "c.yaml")
                if not a.ok:
                    return fail("anchor", f"prev {a.why}")
                if not all(src.apply(t, _ops(k, 0)) for k, t in enumerate(tables)):
                    return fail("seed", "the seed rows failed")
            cycles = [(initer, 0), (initer, 1), (initer, 2), (rivet_bin(), 3), (rivet_bin(), 4)]
            for n, (binary, delta) in enumerate(cycles, 1):
                who = "prev" if binary == prev else "this"
                if delta and not all(src.apply(t, _ops(k, delta)) for k, t in enumerate(tables)):
                    return fail(f"cycle{n}/delta", "source change failed")
                if binary != prev and cycles[n - 2][0] == prev:
                    chk = step(binary, "check", "-c", "c.yaml")
                    if not chk.ok:
                        return fail("check", f"this binary refuses the previous init's config: "
                                             f"{chk.why}")
                for s in ("run", "load", "compact"):
                    if s == "compact" and n > 1 and not all(retry(buffers)):
                        # The positive control: a buffer check that never sees a buffer proves nothing.
                        return fail(f"cycle{n}/load", f"{who}'s load left no `__changes` buffer: {buffers()}")
                    extra = [] if s == "run" else ["--run-id", f"upg-{tag}-{n}"]
                    p = step(binary, s, "-c", "c.yaml", *extra)
                    if not p.ok:
                        return fail(f"cycle{n}/{s}", f"{who} {p.why}")
                def grade() -> tuple[list[str], str]:
                    bad = []
                    with Duck(bigquery=True, bq_dataset=dset, **src.attach) as o:
                        for t in tables:
                            want, live, gone, dup, buf = _state(o, src, dset, t)
                            ids = {int(x.split(":")[0]) for x in want.split(",") if x}
                            seen[t] |= ids
                            want_gone = ",".join(str(i) for i in sorted(seen[t] - ids))
                            if not want or (live, gone, dup, buf) != (want, want_gone, 0, False):
                                bad.append(f"{t}: live={live!r} src={want!r} flagged={gone!r} "
                                           f"want={want_gone!r} dup={dup} buffer_left={buf}")
                        return bad, o.scalar(
                            f"SELECT IFNULL(string_agg(column_name, ','), '') FROM bigquery_query('{proj}', "
                            f"'SELECT column_name FROM `{proj}.{dset}.INFORMATION_SCHEMA.COLUMNS` WHERE "
                            f"table_name = \"{src.bq_table(tables[0])}\" AND is_partitioning_column = \"YES\"')")

                bad, part = retry(grade)
                if bad:
                    return fail(f"cycle{n}", f"after {who}'s compact the base differs from the source — "
                                             + "; ".join(bad)[:600])
            how = ("five cycles by this binary on its own init config" if init_this else
                   "three cycles by the previous release, two by this binary on its init config")
            led.passed(engine, "-", SCEN, "cdc-load", f"{name}: {how}; after every compact each of {CDC_TABLES} bases "
                       f"equals the source instant by value, deletes flagged, no duplicate key, no buffer left "
                       f"(partition column: {part or 'none'}{f'; zone: {src.tz_note}' if tz else ''})", "cdc-load")
    except OracleUnavailable as e:
        led.ungraded(engine, "-", SCEN, "cdc-load", f"{name}: oracle: the BigQuery read did not complete: {e}", "oracle")
    except duckdb.Error as e:
        # A base missing a column the source has is a finding, not a harness crash.
        fail("oracle", f"the DuckDB read failed: {str(e)[:400]}")
    except RuntimeError as e:
        fail("setup", str(e))
    finally:
        for t in tables:
            src.drop(t)
        if not os.environ.get("RIVET_UPG_KEEP"):
            gcp.bq_delete_dataset(proj, dset)
        gcp.gcs_delete_prefix(bucket, f"{pfx}/")


def cdc_load_lane_cells(prev: Path, root: Path) -> list[tuple[object, object]]:
    """Every cdc-load cell as `(lane, fn)`, per engine in UTC, in its non-UTC zone, then on this binary's init;
    the lane is the source server (MySQL's zone is server-GLOBAL, Oracle's capture shares the batch server)."""
    skips: list[tuple[object, object]] = []
    cells: list[tuple[object, object]] = []
    for e, (_, envs, tz) in CDC_LOAD_ENGINES.items():
        if not all(os.environ.get(v) for v in envs):
            skips.append((None, lambda led, e=e, envs=envs: led.skipped(
                e, "-", SCEN, "cdc-load", f"upgrade[{e}/cdc-load]: no {' / '.join(envs)}", "no url")))
            continue
        url = os.environ[envs[0]]
        for z, init_this in ((None, False), *(((tz, False),) if tz else ()), (None, True)):
            cells.append((server_of(url), lambda led, e=e, url=url, z=z, i=init_this: cdc_load_leg(
                led, prev, root, e, url, z, init_this=i)))
    return skips + cells


def cdc_load_cells(led: Ledger, prev: Path, root: Path) -> None:
    """The cdc-load cells alone: one lane per source server."""
    run_lanes(led, cdc_load_lane_cells(prev, root))
