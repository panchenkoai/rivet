"""Multi-hour soak harness: sustained write load + scheduled batch and CDC runs, graded by an independent journal.

    uv run python -m dev.pytools.soak [--engines pg,mysql,mssql,mongo] [--modes batch,cdc]
        [--duration 4h] [--rate 200] [--chaos none|kill|net] [--bin PATH] [--red MUTATION]

Not part of the release gate. See dev/soak/README.md for what each check proves and what it cannot.
`dev/pytools/cdc_soak.py soak` (MySQL-only, insert-only) is the older, narrower predecessor and still works.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import shutil
import signal
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from urllib.parse import urlparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

if __package__:
    from . import cdc_stand, registry, shell
else:  # executed as a plain script
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import cdc_stand  # type: ignore[no-redef]
    import registry  # type: ignore[no-redef]
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
TABLE = "soak_load"
SA_PW = cdc_stand.SA_PASSWORD
TOXI = "http://127.0.0.1:8474"
RED_MUTATIONS = ("drop-cdc-part", "corrupt-source", "dup-snapshot-part", "drop-inc-part")
MIB = 1024 * 1024


def log(msg: str, tag: str = "soak") -> None:
    """A timestamped progress line on stderr."""
    shell.log(f"{datetime.now():%H:%M:%S} {msg}", tag=tag)


def parse_duration(text: str) -> float:
    """`4h` / `10m` / `90s` / `1h30m` → seconds."""
    parts = re.findall(r"(\d+(?:\.\d+)?)([hms])", text.strip())
    if not parts or "".join(a + b for a, b in parts) != text.strip():
        raise argparse.ArgumentTypeError(f"bad duration {text!r} (use e.g. 4h, 10m, 1h30m)")
    return sum(float(n) * {"h": 3600, "m": 60, "s": 1}[u] for n, u in parts)


# ══ engines ════════════════════════════════════════════════════════════════════
@dataclass
class Engine:
    """One source engine: how to write to it, read it back, and probe its log retention."""

    name: str
    container: str
    url: str  # what rivet connects to (the CDC stand; batch and CDC share it)
    init_table: str
    sql: bool = True
    incremental: bool = True  # rivet's mongo source is full-only (src/source/mongo/mod.rs)
    proxy: tuple[str, int, str] | None = None  # (toxiproxy name, listen port, upstream) for --chaos net
    proxy_url: str | None = None
    proxy_owned: bool = False  # the harness creates (and deletes) the proxy

    # ── plumbing ──
    def cli(self, script: str, *, timeout: float = 300) -> shell.Proc:
        """Run a SQL script through the container's CLI on stdin, stopping at the first error."""
        c = self.container
        if self.name == "pg":
            argv = ["psql", "-U", "rivet", "-d", "rivet", "-v", "ON_ERROR_STOP=1", "-qtA", "-f", "-"]
        elif self.name == "mysql":
            argv = ["mysql", "-urivet", "-privet", "--default-character-set=utf8mb4", "rivet", "-N", "-B"]
        elif self.name == "mssql":
            argv = [cdc_stand.SQLCMD, "-C", "-S", "localhost", "-U", "sa", "-P", SA_PW, "-d", "rivet",
                    "-b", "-f", "65001", "-h", "-1", "-W"]
        else:
            raise ValueError(self.name)
        return shell.docker_exec(c, *argv, stdin=script, timeout=timeout)

    def query(self, q: str) -> str:
        """One query's output, stripped; raises on failure."""
        if self.name == "mssql":
            q = "SET NOCOUNT ON; " + q
        return self.cli(q.rstrip(";") + ";\n", timeout=120).check(f"{self.name}: {q[:60]}").stdout.strip()

    def lit(self, s: str) -> str:
        """A string literal, quote-escaped, national on SQL Server."""
        return ("N" if self.name == "mssql" else "") + "'" + s.replace("'", "''") + "'"

    def qualified(self) -> str:
        """The table name as the engine's SQL spells it."""
        return f"dbo.{TABLE}" if self.name == "mssql" else TABLE

    def ddl(self) -> str:
        """CREATE TABLE for the soak table in the engine's dialect."""
        ts, txt = {"pg": ("TIMESTAMP(6)", "VARCHAR(64)"), "mysql": ("DATETIME(6)", "VARCHAR(64)"),
                   "mssql": ("DATETIME2(6)", "NVARCHAR(64)")}[self.name]
        return (f"CREATE TABLE {self.qualified()} (id BIGINT PRIMARY KEY, version BIGINT NOT NULL, "
                f"updated_at {ts} NOT NULL, amount DECIMAL(18,2), payload {txt})")

    def tx(self, stmts: list[str]) -> str:
        """Wrap statements in ONE transaction, in the engine's dialect."""
        body = "\n".join(stmts)
        if self.name == "pg":
            return f"BEGIN;\n{body}\nCOMMIT;\n"
        if self.name == "mysql":
            return f"START TRANSACTION;\n{body}\nCOMMIT;\n"
        return f"SET NOCOUNT ON; SET XACT_ABORT ON; BEGIN TRAN;\n{body}\nCOMMIT;\n"

    # ── lifecycle ──
    def up(self) -> str | None:
        """None when the engine is usable, else the SKIP reason."""
        p = shell.docker("inspect", "-f", "{{.State.Status}} {{.State.Health.Status}}", self.container)
        if not p.ok:
            return f"container {self.container} not found"
        state = p.stdout.strip()
        if not state.startswith("running") or state.endswith("unhealthy") or state.endswith("starting"):
            return f"container {self.container} is '{state}'"
        try:
            if self.sql:
                self.query("SELECT 1")
            else:
                self.mongo().admin.command("ping")
        except Exception as e:  # noqa: BLE001 — any failure to talk to it is the SKIP reason
            return f"{self.container} does not answer: {str(e)[:200]}"
        return None

    def cdc_ready(self) -> str | None:
        """None when the engine can do CDC, else the SKIP reason."""
        try:
            if self.name == "pg":
                v = self.query("SHOW wal_level")
                return None if v == "logical" else f"wal_level={v!r} (need logical)"
            if self.name == "mysql":
                v = self.query("SELECT @@binlog_format")
                return None if v == "ROW" else f"binlog_format={v!r} (need ROW)"
            if self.name == "mssql":
                v = self.query("SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'")
                return None if "Running" in v else f"SQL Server Agent is {v!r} (the capture job IS the Agent)"
            primary = self.mongo().admin.command("hello").get("isWritablePrimary")
            return None if primary else "not a replica-set primary (change streams need one)"
        except Exception as e:  # noqa: BLE001
            return f"CDC readiness probe failed: {str(e)[:200]}"

    def mongo(self):
        """A cached pymongo client (Mongo only)."""
        if not hasattr(self, "_client"):
            import pymongo

            self._client = pymongo.MongoClient(_MONGO_URL,
                                               serverSelectionTimeoutMS=5000, tz_aware=False)
        return self._client

    def coll(self):
        """The Mongo collection."""
        return self.mongo()["rivet"][TABLE]

    def drop_all(self, slot: str | None, ci: str | None) -> None:
        """Drop the table plus its slot / capture instance; best effort, never raises."""
        try:
            if self.name == "pg":
                if slot:
                    self.cli(f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                             f"WHERE slot_name='{slot}' AND NOT active;")
                self.cli(f"DROP TABLE IF EXISTS {TABLE};")
            elif self.name == "mysql":
                self.cli(f"DROP TABLE IF EXISTS {TABLE};")
            elif self.name == "mssql":
                # Disable BEFORE drop: a dropped table orphans its change table and the next enable fails (22926).
                self.cli("IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_soak_load') "
                         "EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'soak_load', "
                         "@capture_instance=N'dbo_soak_load';")
                self.cli(f"IF OBJECT_ID('dbo.{TABLE}','U') IS NOT NULL DROP TABLE dbo.{TABLE};")
            else:
                self.coll().drop()
        except Exception as e:  # noqa: BLE001
            shell.warn(f"{self.name}: teardown step failed: {e}")

    def create(self, cdc: bool) -> None:
        """(Re)create the soak table, and enable SQL Server CDC on it when asked."""
        self.drop_all("rivet_soak_load", "dbo_soak_load")
        if not self.sql:
            self.mongo()["rivet"].create_collection(TABLE)
            return
        self.cli(self.ddl() + ";\n").check(f"{self.name}: CREATE TABLE")
        if self.name == "mssql" and cdc:
            self.cli("IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet')=0 EXEC sys.sp_cdc_enable_db;")
            self.cli("EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'soak_load', "
                     "@role_name=NULL, @capture_instance=N'dbo_soak_load';").check("mssql: sp_cdc_enable_table")
            # rivet's preflight reads fn_cdc_get_min_lsn, so "enabled" is not "ready" until it is non-zero.
            ok = shell.wait_until(lambda: self.query(
                "SELECT CASE WHEN sys.fn_cdc_get_min_lsn('dbo_soak_load') > 0x00 THEN 1 ELSE 0 END") == "1",
                tries=60, delay=1.0)
            if not ok:
                raise shell.Fail("mssql: capture instance dbo_soak_load never became ready")

    def count(self) -> int:
        """Current row count of the soak table."""
        if not self.sql:
            return self.coll().count_documents({})
        return int(self.query(f"SELECT COUNT(*) FROM {self.qualified()}").split()[-1])

    def live_versions(self) -> dict[int, int]:
        """{id: version} as the source holds it now (the writer's recovery path)."""
        if not self.sql:
            return {int(d["_id"]): int(d["version"]) for d in self.coll().find({}, {"version": 1})}
        out = self.query(f"SELECT id, version FROM {self.qualified()}")
        rows = (re.split(r"[\s|\t]+", line.strip()) for line in out.splitlines() if line.strip())
        return {int(r[0]): int(r[1]) for r in rows if len(r) >= 2 and r[0].lstrip("-").isdigit()}

    def id_exists(self, i: int) -> bool:
        """Did a row with this id commit? The commit-ambiguity probe."""
        if not self.sql:
            return self.coll().count_documents({"_id": i}) > 0
        return self.query(f"SELECT COUNT(*) FROM {self.qualified()} WHERE id={i}").split()[-1] == "1"

    def retention(self, slot: str | None) -> dict:
        """One sample of source log retention (what the reader may be pinning)."""
        if self.name == "pg":
            v = self.query(f"SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, -1) "
                           f"FROM pg_replication_slots WHERE slot_name='{slot}'")
            return {"slot_lag_bytes": int(v) if v.lstrip("-").isdigit() else None}
        if self.name == "mysql":
            out = self.query("SHOW BINARY LOGS")
            sizes = [int(r.split()[1]) for r in out.splitlines() if len(r.split()) >= 2 and r.split()[1].isdigit()]
            return {"binlog_files": len(sizes), "binlog_bytes": sum(sizes)}
        if self.name == "mssql":
            v = self.query("SELECT COUNT_BIG(*) FROM cdc.dbo_soak_load_CT")
            return {"change_table_rows": int(v.split()[-1])}
        oplog = self.mongo()["local"]["oplog.rs"]
        first = oplog.find({}, {"ts": 1}).sort("$natural", 1).limit(1).next()["ts"]
        last = oplog.find({}, {"ts": 1}).sort("$natural", -1).limit(1).next()["ts"]
        return {"oplog_window_s": last.time - first.time}

    # ── writes ──
    def commit(self, ops: list[dict]) -> None:
        """Apply ops in ONE transaction; raises on failure."""
        if not self.sql:
            from bson.decimal128 import Decimal128

            coll = self.coll()

            def body(s):
                ins = [o for o in ops if o["op"] == "i"]
                if len(ins) == len(ops):
                    coll.insert_many([_mongo_doc(o, Decimal128) for o in ins], session=s, ordered=True)
                    return
                for o in ops:
                    if o["op"] == "i":
                        coll.insert_one(_mongo_doc(o, Decimal128), session=s)
                    elif o["op"] == "u":
                        coll.replace_one({"_id": o["id"]}, _mongo_doc(o, Decimal128), session=s)
                    else:
                        coll.delete_one({"_id": o["id"]}, session=s)

            with self.mongo().start_session() as s:
                s.with_transaction(body)
            return
        stmts: list[str] = []
        inserts = [o for o in ops if o["op"] == "i"]
        if len(inserts) == len(ops) and len(ops) > 50:  # the big transaction: multi-row INSERTs
            for k in range(0, len(ops), 500):
                rows = ",\n".join(self._row(o) for o in ops[k:k + 500])
                stmts.append(f"INSERT INTO {self.qualified()} (id,version,updated_at,amount,payload) VALUES {rows};")
        else:
            for o in ops:
                if o["op"] == "i":
                    stmts.append(f"INSERT INTO {self.qualified()} (id,version,updated_at,amount,payload) "
                                 f"VALUES {self._row(o)};")
                elif o["op"] == "u":
                    stmts.append(f"UPDATE {self.qualified()} SET version={o['v']}, updated_at='{o['ts']}', "
                                 f"amount={o['amount']}, payload={self.lit(o['payload'])} WHERE id={o['id']};")
                else:
                    stmts.append(f"DELETE FROM {self.qualified()} WHERE id={o['id']};")
        self.cli(self.tx(stmts), timeout=180).check(f"{self.name}: write batch")

    def _row(self, o: dict) -> str:
        """One VALUES tuple."""
        return f"({o['id']},{o['v']},'{o['ts']}',{o['amount']},{self.lit(o['payload'])})"


def _mongo_doc(o: dict, dec) -> dict:
    """The document a journal op writes."""
    ts = datetime.strptime(o["ts"], "%Y-%m-%d %H:%M:%S.%f")
    return {"_id": o["id"], "version": o["v"], "updated_at": ts, "amount": dec(o["amount"]), "payload": o["payload"]}


def _src(name: str) -> tuple[str, str]:
    """(container, url) of a stand source, from dev/stand/registry.yaml."""
    x = registry.source(name)
    return x["container"], x["url"]


def _port(url: str, port: int) -> str:
    """The same URL on another host port (the toxiproxy listener)."""
    u = urlparse(url)
    return u._replace(netloc=f"{u.username}:{u.password}@127.0.0.1:{port}").geturl()


_MONGO_C, _MONGO_URL = _src("mongo_rs")

#: The registry. A new engine (e.g. `oracle`, batch first) is one entry here plus its dialect arms above.
ENGINES: dict[str, Engine] = {
    "pg": Engine("pg", *_src("postgres_cdc"), "public.soak_load", proxy=("soak_pg_cdc", 15433, "postgres-cdc:5432"),
                 proxy_url=_port(_src("postgres_cdc")[1], 15433), proxy_owned=True),
    "mysql": Engine("mysql", *_src("mysql_cdc"), "soak_load", proxy=("mysql_cdc_gremlin", 13307, "mysql-cdc:3306"),
                    proxy_url=_port(_src("mysql_cdc")[1], 13307)),
    "mssql": Engine("mssql", *_src("mssql_cdc"), "dbo.soak_load"),
    "mongo": Engine("mongo", _MONGO_C, _MONGO_URL.replace("/?", "/rivet?"), "soak_load", sql=False, incremental=False),
}


# ══ the load writer + ground-truth journal ═════════════════════════════════════
class Writer(threading.Thread):
    """Background INSERT 60 / UPDATE 30 / DELETE 10 load; journals every COMMITTED op to JSONL."""

    HOT = 50

    def __init__(self, eng: Engine, journal: Path, rate: float, seed_rows: int, big_rows: int,
                 big_every: float, stop: threading.Event) -> None:
        super().__init__(name=f"writer-{eng.name}", daemon=True)
        self.eng, self.journal, self.rate, self.stop = eng, journal, rate, stop
        self.seed_rows, self.big_rows, self.big_every = seed_rows, big_rows, big_every
        self.live: list[int] = []
        self.pos: dict[int, int] = {}
        self.ver: dict[int, int] = {}
        self.next_id = 1
        self.last_ts = datetime(2000, 1, 1)
        self.rng = random.Random(hash(eng.name) & 0xFFFF)
        self.stats = {"ops": 0, "i": 0, "u": 0, "d": 0, "batches": 0, "failed_batches": 0,
                      "ambiguous_committed": 0, "big_tx": 0, "per_minute": []}
        self.error: str | None = None
        self.acked = 0.0

    def _ts(self) -> str:
        """A strictly increasing UTC timestamp at the engine's precision."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if self.eng.name == "mongo":  # BSON dates are milliseconds
            now = now.replace(microsecond=now.microsecond // 1000 * 1000)
        step = timedelta(milliseconds=1) if self.eng.name == "mongo" else timedelta(microseconds=1)
        self.last_ts = max(now, self.last_ts + step)
        return self.last_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    def _add(self, i: int, v: int) -> None:
        """Mark an id live at a version."""
        self.pos[i] = len(self.live)
        self.live.append(i)
        self.ver[i] = v

    def _remove(self, i: int) -> None:
        """Drop an id from the live set in O(1)."""
        k = self.pos.pop(i)
        last = self.live.pop()
        if last != i:
            self.live[k] = last
            self.pos[last] = k
        self.ver.pop(i)

    def _amount(self) -> str:
        """A random DECIMAL(18,2) as text."""
        return f"{self.rng.randrange(0, 100_000_000) / 100:.2f}"

    def _insert(self, ts: str) -> dict:
        """A fresh-id insert op."""
        i = self.next_id
        self.next_id += 1
        self._add(i, 1)
        return {"id": i, "op": "i", "v": 1, "ts": ts, "amount": self._amount(), "payload": f"i{i}v1-ü'q"}

    def build(self, n: int) -> list[dict]:
        """n ops against the tentative state; the first is always an insert (the commit probe)."""
        ts = self._ts()
        ops = [self._insert(ts)]
        hot = [i for i in range(1, self.HOT + 1) if i in self.pos]
        for _ in range(n - 1):
            r = self.rng.random()
            if r < 0.6 or len(self.live) < self.HOT * 2:
                ops.append(self._insert(ts))
            elif r < 0.9:
                i = self.rng.choice(hot) if hot and self.rng.random() < 0.3 else self.rng.choice(self.live)
                v = self.ver[i] + 1
                self.ver[i] = v
                ops.append({"id": i, "op": "u", "v": v, "ts": ts, "amount": self._amount(), "payload": f"u{i}v{v}-ü'q"})
            else:
                i = self.rng.choice(self.live)
                if i <= self.HOT:
                    continue
                ops.append({"id": i, "op": "d", "v": self.ver[i], "ts": ts, "amount": None, "payload": None})
                self._remove(i)
        return ops

    def build_big(self, n: int) -> list[dict]:
        """One large insert-only transaction (crosses CDC rollover and the spill cap)."""
        ts = self._ts()
        return [self._insert(ts) for _ in range(n)]

    def run(self) -> None:
        """Thread body: a crash becomes `error`, never a silently dead writer."""
        try:
            self._loop()
        except Exception as e:  # noqa: BLE001 — a dead writer must be a named failure, not a silent thread
            self.error = f"{type(e).__name__}: {e}"
            traceback.print_exc()

    def _loop(self) -> None:
        """Seed, then commit a rate-sized batch every second, with a big transaction every big_every seconds."""
        with self.journal.open("a", buffering=1) as jf:
            self._commit(self.build_big(self.seed_rows), jf, big=True)
            last, next_big, minute_start, minute_ops = time.time(), time.time() + self.big_every, time.time(), 0
            while not self.stop.is_set():
                now = time.time()
                if now >= next_big:
                    self._commit(self.build_big(self.big_rows), jf, big=True)
                    next_big = now + self.big_every
                n = max(1, int(self.rate * (now - last)))
                last = now
                before = self.stats["ops"]
                self._commit(self.build(min(n, int(self.rate * 5) + 1)), jf)
                minute_ops += self.stats["ops"] - before
                if now - minute_start >= 60:
                    self.stats["per_minute"].append(round(minute_ops / (now - minute_start), 1))
                    minute_start, minute_ops = now, 0
                self.stop.wait(max(0.0, 1.0 - (time.time() - now)))

    def _commit(self, ops: list[dict], jf, big: bool = False) -> None:
        """Commit one batch, then journal it; `acked` moves only once every earlier commit is journaled."""
        try:
            self._commit_once(ops, jf, big)
        finally:
            self.acked = time.time()

    def _probe(self, i: int, err: Exception) -> bool:
        """Did the failed batch commit? Retried for 2 min, since the usual cause is the source restarting."""
        last: Exception | None = None
        for _ in range(24):
            try:
                return self.eng.id_exists(i)
            except Exception as probe:  # noqa: BLE001
                last = probe
                time.sleep(5)
        raise RuntimeError(f"batch failed ({err}) and the commit probe failed for 2 min ({last})")

    def _commit_once(self, ops: list[dict], jf, big: bool) -> None:
        """Commit, probe on failure, journal what committed; resync the live set from the source on a rollback."""
        st = time.time()
        try:
            self.eng.commit(ops)
            committed = True
        except Exception as e:  # noqa: BLE001
            committed = self._probe(ops[0]["id"], e)  # all-or-nothing: the first insert decides
            if committed:
                self.stats["ambiguous_committed"] += 1
            else:
                self.stats["failed_batches"] += 1
                shell.warn(f"{self.eng.name}: write batch rolled back ({str(e)[:120]}); resyncing from source")
                self.live, self.pos, self.ver = [], {}, {}
                for i, v in self.eng.live_versions().items():
                    self._add(i, v)
                return
        ct = time.time()
        self.stats["batches"] += 1
        self.stats["big_tx"] += int(big)
        b = self.stats["batches"]
        for o in ops:
            self.stats[o["op"]] += 1
            jf.write(json.dumps({**o, "st": st, "ct": ct, "b": b}) + "\n")
        self.stats["ops"] += len(ops)


# ══ rivet runs ═════════════════════════════════════════════════════════════════
@dataclass
class Cycle:
    engine: str
    stream: str  # cdc | incremental | snapshot-keyset | snapshot-range | pin | final
    n: int
    t0: float
    dur: float
    rc: int
    rss: int | None
    killed: bool = False
    hung: bool = False
    log: str = ""
    note: str = ""

    @property
    def ok(self) -> bool:
        """Exited 0 and was not chaos-killed."""
        return self.rc == 0 and not self.killed


class Rivet:
    """Runs `rivet run` under /usr/bin/time, optionally SIGKILLing it mid-flight."""

    def __init__(self, binary: Path, logs: Path, timeout: float) -> None:
        self.bin, self.logs, self.timeout = binary, logs, timeout
        self.lock = threading.Lock()
        self.seq = 0

    def run(self, eng: str, stream: str, n: int, cfg_dir: Path, url: str,
            kill_after: float | None = None, extra_env: dict | None = None) -> Cycle:
        """One `rivet run -c c.yaml` in cfg_dir; SIGKILL the process group after kill_after seconds."""
        with self.lock:
            self.seq += 1
            logf = self.logs / f"{self.seq:05d}-{eng}-{stream}-{n}.log"
        env = {**os.environ, "DATABASE_URL": url, **(extra_env or {})}
        argv = ["/usr/bin/time", cdc_stand.time_flag(), str(self.bin), "run", "-c", "c.yaml"]
        t0 = time.time()
        p = subprocess.Popen(argv, cwd=cfg_dir, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             text=True, start_new_session=True)
        killed = hung = False
        try:
            out, err = p.communicate(timeout=kill_after if kill_after else self.timeout)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            out, err = p.communicate()
            killed, hung = bool(kill_after), not kill_after
        dur = time.time() - t0
        logf.write_text(f"$ {' '.join(argv)}  (cwd={cfg_dir})\n--- stdout\n{out}\n--- stderr\n{err}\n")
        _, rss = cdc_stand.parse_time_report(err)
        rc = p.returncode if not hung else 124
        return Cycle(eng, stream, n, t0, dur, rc, rss, killed=killed, hung=hung, log=str(logf.relative_to(self.logs.parent)))


# ══ config generation (rivet init) ═════════════════════════════════════════════
def gen_config(rivet: Path, eng: Engine, d: Path, mode: str) -> dict:
    """`rivet init` in place (it records primary keys beside the config), keep init.yaml pristine, return the parsed config."""
    import yaml

    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    p = shell.run([str(rivet), "init", "--source", eng.url, "--table", eng.init_table, "--mode", mode,
                   "-o", "init.yaml"], cwd=d, timeout=180)
    if not p.ok:
        raise shell.Fail(f"{eng.name}: rivet init --mode {mode} failed: {(p.stderr or p.stdout).strip()[-400:]}")
    return yaml.safe_load((d / "init.yaml").read_text())


def write_patched(d: Path, cfg: dict, why: str) -> None:
    """Write c.yaml (the run config) with a header naming every edit made to init's output."""
    import yaml

    (d / "c.yaml").write_text(f"# rivet init output (init.yaml) with soak-only edits: {why}\n"
                              + yaml.safe_dump(cfg, sort_keys=False))


def setup_configs(rivet: Path, eng: Engine, work: Path, modes: set[str]) -> dict[str, Path]:
    """Generate every config this engine needs; refuse loudly when init did not emit what the soak depends on."""
    dirs: dict[str, Path] = {}
    if "cdc" in modes:
        d = work / eng.name / "cdc"
        cfg = gen_config(rivet, eng, d, "cdc")
        ex = cfg["exports"][0]
        if ex.get("mode") != "cdc" or not ex.get("cdc", {}).get("until_current"):
            raise shell.Fail(f"{eng.name}: init --mode cdc did not emit a bounded (until_current) cdc export: {ex}")
        ex["cdc"]["rollover"] = 1000  # small parts, so the big transaction crosses the rollover
        write_patched(d, cfg, "cdc.rollover=1000")
        dirs["cdc"] = d
    if "batch" in modes:
        if eng.incremental:
            d = work / eng.name / "incremental"
            cfg = gen_config(rivet, eng, d, "incremental")
            ex = cfg["exports"][0]
            if ex.get("mode") != "incremental" or ex.get("cursor_column") != "updated_at":
                raise shell.Fail(f"{eng.name}: init --mode incremental did not pick cursor_column updated_at: "
                                 f"mode={ex.get('mode')} cursor={ex.get('cursor_column')}")
            write_patched(d, cfg, "none")
            dirs["incremental"] = d
        for kind in ("keyset", "range"):
            d = work / eng.name / f"snapshot-{kind}"
            cfg = gen_config(rivet, eng, d, "chunked")
            ex = cfg["exports"][0]
            if eng.sql:
                if ex.get("chunk_by_key") != "id":
                    raise shell.Fail(f"{eng.name}: init --mode chunked did not emit chunk_by_key: id ({ex})")
                ex["chunk_size"] = 5000  # several chunks at soak table sizes
                why = "chunk_size=5000"
                if kind == "range":  # init has no flag for range chunking; swap the keyset key for a range column
                    ex["chunk_column"] = ex.pop("chunk_by_key")
                    why += "; chunk_by_key→chunk_column (range)"
            else:  # mongo: keyset = source.mongo.page_size, range = parallel _id-range fan-out
                if kind == "keyset":
                    cfg["source"].setdefault("mongo", {})["page_size"] = 1000
                    why = "source.mongo.page_size=1000 (keyset)"
                else:
                    ex["parallel"] = 4
                    why = "parallel=4 (_id-range fan-out)"
            write_patched(d, cfg, why)
            dirs[f"snapshot-{kind}"] = d
    return dirs


# ══ DuckDB projections ═════════════════════════════════════════════════════════


def duck():
    """A fresh DuckDB session in UTC."""
    import duckdb

    con = duckdb.connect()
    con.sql("SET TimeZone='UTC'")
    return con


def parts_rel(eng: Engine, files: list[Path], cdc: bool) -> str:
    """SQL over parquet files projected to the canonical columns (+ __op/poskey/__seq for CDC)."""
    lst = "[" + ",".join(f"'{f}'" for f in files) + "]"
    src = f"read_parquet({lst}, union_by_name=true)"
    if eng.sql:
        cols = "id::BIGINT id, \"version\"::BIGINT ver, updated_at::TIMESTAMP updated_at, " \
               "amount::DECIMAL(18,2) amount, payload::VARCHAR payload"
    else:
        cols = ("_id::BIGINT id, json_extract(document,'$.version')::BIGINT ver, "
                "json_extract_string(document,'$.updated_at.\"$date\"')::TIMESTAMPTZ::TIMESTAMP updated_at, "
                "json_extract_string(document,'$.amount.\"$numberDecimal\"')::DECIMAL(18,2) amount, "
                "json_extract_string(document,'$.payload') payload")
    if not cdc:
        return f"(SELECT {cols} FROM {src})"
    pos = {
        "pg": "lpad(upper(split_part(json_extract_string(__pos,'$.lsn'),'/',1)),8,'0') || "
              "lpad(upper(split_part(json_extract_string(__pos,'$.lsn'),'/',2)),8,'0')",
        "mysql": "lpad(regexp_extract(json_extract_string(__pos,'$.file'),'(\\d+)$',1),12,'0') || "
                 "lpad(json_extract_string(__pos,'$.pos'),20,'0')",
        "mssql": "json_extract_string(__pos,'$.lsn')",
        "mongo": "__pos::VARCHAR",
    }[eng.name]
    return f"(SELECT {cols}, __op, ({pos}) poskey, __seq FROM {src})"


def journal_rel(path: Path, cutoff_ct: float | None = None) -> str:
    """The ground-truth journal as a relation; a torn last line (writer mid-append) is skipped."""
    where = f" WHERE ct < {cutoff_ct}" if cutoff_ct is not None else ""
    return (f"(SELECT * FROM read_json('{path}', format='newline_delimited', ignore_errors=true, "
            "columns={id:'BIGINT',op:'VARCHAR',v:'BIGINT',ts:'TIMESTAMP',amount:'DECIMAL(18,2)',"
            f"payload:'VARCHAR',st:'DOUBLE',ct:'DOUBLE',b:'BIGINT'}}){where})")


def cdc_parts(d: Path) -> list[Path]:
    """Every parquet part under a CDC config's output (kill orphans included: their events are real)."""
    return sorted((d / "output").rglob("*.parquet"))


def declared_parts(out: Path) -> list[Path]:
    """Parts the canonical manifest declares (the consumer's view of a snapshot)."""
    m = json.loads((out / "manifest.json").read_text())
    if m.get("status") != "success":
        raise RuntimeError(f"manifest status {m.get('status')!r}")
    return [out / p["path"] for p in m["parts"]]


# ══ oracles ════════════════════════════════════════════════════════════════════
@dataclass
class Check:
    name: str
    status: str  # PASS | FAIL | WARN | INFO | SKIP
    detail: str


def check_cdc_gap(eng: Engine, d: Path, journal: Path, cutoff: float | None, red_drop: bool = False) -> Check:
    """Every committed journal op (before cutoff) has its event in the parts: (id, version) for ins/upd, a delete for d."""
    files = cdc_parts(d)
    if red_drop and files:
        files = files[:-1] if len(files) > 1 else []
    label = "cdc.no_gap" + ("" if cutoff is None else ".periodic")
    if not files:
        n = duck().sql(f"SELECT count(*) FROM {journal_rel(journal, cutoff)}").fetchone()[0]
        return Check(label, "FAIL" if n else "PASS", f"no parts; {n} journal ops expected")
    con = duck()
    con.sql(f"CREATE TEMP TABLE ev AS SELECT id, ver, __op FROM {parts_rel(eng, files, True)}")
    con.sql(f"CREATE TEMP TABLE j AS SELECT * FROM {journal_rel(journal, cutoff)}")
    miss_up = con.sql("SELECT count(*), min(j.id) FROM j ANTI JOIN (SELECT * FROM ev WHERE __op<>'delete') e "
                      "ON e.id=j.id AND e.ver=j.v WHERE j.op<>'d'").fetchone()
    miss_del = con.sql("SELECT count(*), min(j.id) FROM j ANTI JOIN (SELECT * FROM ev WHERE __op='delete') e "
                       "ON e.id=j.id WHERE j.op='d'").fetchone()
    total = con.sql("SELECT count(*) FROM j").fetchone()[0]
    dups = con.sql("SELECT count(*) - count(DISTINCT (id, ver, __op)) FROM ev WHERE __op<>'delete'").fetchone()[0]
    bad = miss_up[0] + miss_del[0]
    detail = (f"{total} journal ops, {len(files)} parts; missing ins/upd={miss_up[0]} (e.g. id {miss_up[1]}), "
              f"missing deletes={miss_del[0]} (e.g. id {miss_del[1]}); re-delivered duplicates={dups} (allowed)")
    return Check(label, "FAIL" if bad else "PASS", detail)


def source_rel(con, eng: Engine) -> str:
    """ATTACH the source in the same DuckDB session (pymongo export for Mongo) and return the canonical relation."""
    if eng.name == "pg":
        con.sql("INSTALL postgres; LOAD postgres;")
        con.sql(f"ATTACH '{eng.url}' AS src (TYPE postgres, READ_ONLY)")
        t = "src.public.soak_load"
    elif eng.name == "mysql":
        con.sql("INSTALL mysql; LOAD mysql;")
        u = urlparse(eng.url)
        con.sql(f"ATTACH 'host={u.hostname} port={u.port} user={u.username} password={u.password} "
                f"database={u.path.lstrip('/')}' AS src (TYPE mysql, READ_ONLY)")
        t = "src.soak_load"
    elif eng.name == "mssql":
        con.sql("INSTALL mssql FROM community; LOAD mssql;")
        u = urlparse(eng.url)
        con.sql(f"ATTACH 'Server={u.hostname},{u.port};Database={u.path.lstrip('/')};UID={u.username};"
                f"PWD={u.password};TrustServerCertificate=true' "
                "AS src (TYPE mssql, READ_ONLY)")
        t = "src.dbo.soak_load"
    else:
        import pyarrow as pa

        docs = list(eng.coll().find({}))
        tbl = pa.table({
            "id": pa.array([int(x["_id"]) for x in docs], pa.int64()),
            "version": pa.array([int(x["version"]) for x in docs], pa.int64()),
            "updated_at": pa.array([x["updated_at"] for x in docs], pa.timestamp("us")),
            "amount": pa.array([str(x["amount"]) for x in docs], pa.string()),
            "payload": pa.array([x["payload"] for x in docs], pa.string()),
        })
        con.register("mongo_src", tbl)
        t = "mongo_src"
    con.sql(f"CREATE TEMP TABLE s AS SELECT id::BIGINT id, \"version\"::BIGINT ver, updated_at::TIMESTAMP updated_at, "
            f"amount::DECIMAL(18,2) amount, payload::VARCHAR payload FROM {t}")
    return "s"


COLS = ("ver", "updated_at", "amount", "payload")


def compare_sql(a: str, b: str) -> str:
    """Per-id, per-column disagreement counts between two canonical relations."""
    diffs = ", ".join(f"count(*) FILTER (WHERE x.id IS NOT NULL AND y.id IS NOT NULL AND x.{c} IS DISTINCT FROM y.{c}) {c}"
                      for c in COLS)
    return (f"SELECT count(*) FILTER (WHERE y.id IS NULL) only_a, count(*) FILTER (WHERE x.id IS NULL) only_b, {diffs}, "
            f"min(coalesce(x.id, y.id)) FILTER (WHERE x.id IS NULL OR y.id IS NULL OR "
            + " OR ".join(f"x.{c} IS DISTINCT FROM y.{c}" for c in COLS)
            + f") first_bad FROM {a} x FULL OUTER JOIN {b} y ON x.id = y.id")


def fmt_cmp(row: tuple, a: str, b: str) -> tuple[bool, str]:
    """(any disagreement?, a one-line summary) of a compare_sql row."""
    only_a, only_b, *cols, first = row
    bad = only_a + only_b + sum(cols)
    per = ", ".join(f"{c}={n}" for c, n in zip(COLS, cols))
    return bool(bad), f"only in {a}={only_a}, only in {b}={only_b}, per-column mismatches: {per}" + \
        (f"; first bad id {first}" if bad else "")


def journal_state_sql(j: str) -> str:
    """The expected current state from the journal: ids never deleted, at their max version."""
    return (f"(SELECT id, v ver, ts updated_at, amount, payload FROM (SELECT *, row_number() OVER "
            f"(PARTITION BY id ORDER BY v DESC, (op='d') DESC) rn FROM {j}) WHERE rn=1 AND op<>'d')")


def final_cdc(eng: Engine, d: Path, journal: Path, red: str | None) -> list[Check]:
    """Replay parts to current state and compare per column with the source; journal-vs-source is the harness self-check."""
    checks = [check_cdc_gap(eng, d, journal, None, red_drop=red == "drop-cdc-part")]
    files = cdc_parts(d)
    if red == "drop-cdc-part" and files:
        files = files[:-1]
    con = duck()
    s = source_rel(con, eng)
    con.sql(f"CREATE TEMP TABLE j AS SELECT * FROM {journal_rel(journal)}")
    bad, det = fmt_cmp(con.sql(compare_sql(journal_state_sql("j"), s)).fetchone(), "journal", "source")
    checks.append(Check("harness.journal_matches_source", "FAIL" if bad else "PASS", det))
    if not files:
        checks.append(Check("cdc.replay_matches_source", "FAIL", "no parts to replay"))
        return checks
    con.sql(f"CREATE TEMP TABLE ev AS SELECT * FROM {parts_rel(eng, files, True)}")
    con.sql("CREATE TEMP TABLE latest AS SELECT * FROM (SELECT *, row_number() OVER "
            "(PARTITION BY id ORDER BY poskey DESC, __seq DESC) rn FROM ev) WHERE rn=1")
    bad, det = fmt_cmp(con.sql(compare_sql("(SELECT * FROM latest WHERE __op<>'delete')", s)).fetchone(),
                       "replay", "source")
    checks.append(Check("cdc.replay_matches_source", "FAIL" if bad else "PASS", det))
    n = con.sql("SELECT count(*) FROM latest l JOIN (SELECT id, max(ver) mv FROM ev WHERE __op<>'delete' "
                "GROUP BY id) m USING (id) WHERE l.__op<>'delete' AND l.ver<>m.mv").fetchone()[0]
    checks.append(Check("cdc.order_consistent", "FAIL" if n else "PASS",
                        f"{n} ids whose (__pos,__seq)-latest event is not their highest version"))
    return checks


def check_incremental(eng: Engine, d: Path, journal: Path, settle: float, final: bool, red: str | None) -> list[Check]:
    """Incremental completeness vs the journal, cut at the output's own max cursor minus the settle window."""
    files = sorted((d / "output").rglob("*.parquet"))
    if red == "drop-inc-part" and final and files:
        files = files[:-1]
    tag = "" if final else ".periodic"
    if not files:
        if not final:
            return []  # nothing exported yet (the writer may not have committed before the first run)
        live = duck().sql(f"SELECT count(*) FROM {journal_state_sql(journal_rel(journal))}").fetchone()[0]
        return [Check("incremental.complete", "FAIL" if live else "PASS", f"no parts; {live} live ids expected")]
    con = duck()
    con.sql(f"CREATE TEMP TABLE p AS SELECT * FROM {parts_rel(eng, files, False)}")
    hi = con.sql("SELECT max(updated_at) FROM p").fetchone()[0]
    cutoff = "TIMESTAMP '9999-01-01'" if final else f"TIMESTAMP '{hi}' - INTERVAL {settle} SECOND"
    con.sql(f"CREATE TEMP TABLE j AS SELECT * FROM {journal_rel(journal)}")
    # A row deleted before a read is invisible to a cursor forever, so a periodic check exempts every deleted id.
    exempt = "" if final else " AND id NOT IN (SELECT id FROM j WHERE op='d')"
    con.sql(f"CREATE TEMP TABLE jl AS SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY id ORDER BY v DESC, "
            f"(op='d') DESC) rn FROM j WHERE ts <= {cutoff}{exempt}) WHERE rn=1")
    con.sql("CREATE TEMP TABLE pm AS SELECT id, max(ver) mv FROM p GROUP BY id")
    cmp_ = "<>" if final else "<"
    miss, first = con.sql(f"SELECT count(*), min(jl.id) FROM jl LEFT JOIN pm USING (id) WHERE jl.op<>'d' "
                          f"AND (pm.mv IS NULL OR pm.mv {cmp_} jl.v)").fetchone()
    checked = con.sql("SELECT count(*) FROM jl WHERE op<>'d'").fetchone()[0]
    out = [Check("incremental.complete" + tag, "FAIL" if miss else "PASS",
                 f"{checked} live ids with last change ≤ cutoff ({'end of run' if final else f'{hi} - {settle}s'}); "
                 f"{miss} absent or behind their journal version" + (f" (e.g. id {first})" if miss else ""))]
    if final:
        vals = con.sql("SELECT count(*) FROM jl JOIN p ON p.id=jl.id AND p.ver=jl.v WHERE jl.op<>'d' AND "
                       "(p.amount IS DISTINCT FROM jl.amount OR p.payload IS DISTINCT FROM jl.payload "
                       "OR p.updated_at IS DISTINCT FROM jl.ts)").fetchone()[0]
        out.append(Check("incremental.values", "FAIL" if vals else "PASS",
                         f"{vals} rows whose exported (id, version) carries values other than the journal's"))
        ghosts = con.sql("SELECT count(*) FROM jl JOIN pm USING (id) WHERE jl.op='d'").fetchone()[0]
        out.append(Check("incremental.deletes_invisible", "INFO",
                         f"{ghosts} ids deleted in the source still present in incremental output — expected: a "
                         "cursor read never sees a DELETE"))
    return out


def check_snapshot(eng: Engine, out_dir: Path, journal: Path, s: float, e: float, c0: int, c1: int,
                   red: bool) -> list[Check]:
    """No duplicate PK; ids alive for the whole run present; ids never alive during it absent; count within journal bounds."""
    try:
        files = declared_parts(out_dir)
    except Exception as ex:  # noqa: BLE001
        return [Check("snapshot.declared", "FAIL", f"cannot read the manifest: {ex}")]
    if red and files:
        dup = out_dir / ("red_dup_" + files[0].name)
        shutil.copy(files[0], dup)
        files.append(dup)
    raw = len(list(out_dir.rglob("*.parquet")))
    con = duck()
    if files:
        con.sql(f"CREATE TEMP TABLE p AS SELECT id FROM {parts_rel(eng, files, False)}")
    else:  # an empty table exports a manifest with no parts
        con.sql("CREATE TEMP TABLE p (id BIGINT)")
    con.sql(f"CREATE TEMP TABLE j AS SELECT * FROM {journal_rel(journal)}")
    n, distinct = con.sql("SELECT count(*), count(DISTINCT id) FROM p").fetchone()
    ins_before = f"(SELECT id FROM j WHERE op='i' AND ct < {s})"
    del_maybe = f"(SELECT id FROM j WHERE op='d' AND st < {e})"
    must, must_first = con.sql(f"SELECT count(*), min(id) FROM {ins_before} WHERE id NOT IN {del_maybe} "
                               "AND id NOT IN (SELECT id FROM p)").fetchone()
    never = con.sql(f"SELECT count(*) FROM p WHERE id IN (SELECT id FROM j WHERE op='d' AND ct < {s}) "
                    f"OR id NOT IN (SELECT id FROM j WHERE op='i' AND st <= {e})").fetchone()[0]
    ins_during = con.sql(f"SELECT count(*) FROM j WHERE op='i' AND ct >= {s} AND st <= {e}").fetchone()[0]
    del_during = con.sql(f"SELECT count(*) FROM j WHERE op='d' AND ct >= {s} AND st <= {e}").fetchone()[0]
    lo, hi = c0 - del_during, c0 + ins_during
    res = [
        Check("snapshot.no_duplicate_pk", "FAIL" if n != distinct else "PASS",
              f"{n} rows, {distinct} distinct ids in {len(files)} declared parts ({raw} parquet files on disk)"),
        Check("snapshot.contains_stable_rows", "FAIL" if must else "PASS",
              f"{must} ids alive for the whole run are missing" + (f" (e.g. id {must_first})" if must else "")),
        Check("snapshot.no_phantom_rows", "FAIL" if never else "PASS", f"{never} ids that were never alive during the run"),
        Check("snapshot.count_in_bounds", "PASS" if lo <= distinct <= hi else "FAIL",
              f"{distinct} ∈ [{lo}, {hi}]? (source count {c0} at start, {c1} at end; "
              f"{ins_during} inserts / {del_during} deletes during the run)"),
    ]
    return res


def trend(values: list[float]) -> tuple[float, float] | None:
    """(first-quarter peak, last-quarter peak), or None under 8 samples."""
    if len(values) < 8:
        return None
    q = len(values) // 4
    return max(values[:q]), max(values[-q:])


def spark(values: list[float], width: int = 48) -> str:
    """An ASCII-ish sparkline, downsampled to `width` buckets."""
    vals = [v for v in values if v is not None]
    if not vals:
        return "(no data)"
    if len(vals) > width:
        step = len(vals) / width
        vals = [max(vals[int(i * step):int((i + 1) * step)] or [vals[int(i * step)]]) for i in range(width)]
    lo, hi = min(vals), max(vals)
    bars = "▁▂▃▄▅▆▇█"
    return "".join(bars[0 if hi == lo else int((v - lo) / (hi - lo) * 7)] for v in vals)


def cycle_checks(cycles: list[Cycle], chaos: str) -> list[Check]:
    """Every cycle exited 0, or failed retryably and the NEXT cycle of the same stream recovered."""
    out: list[Check] = []
    base = lambda c: c.stream.removeprefix("final-")  # noqa: E731 — the final drain continues its stream
    for st in sorted({base(c) for c in cycles}):
        cs = sorted((c for c in cycles if base(c) == st), key=lambda c: c.t0)
        allowed_streak = 3 if chaos == "net" else 1
        streak, worst, unrecovered, fails, kills = 0, 0, [], 0, 0
        for c in cs:
            if c.ok:
                streak = 0
                continue
            streak += 1
            worst = max(worst, streak)
            kills += int(c.killed)
            fails += int(not c.killed)
        if cs and not cs[-1].ok:
            unrecovered.append(cs[-1].log)
        bad = worst > allowed_streak or unrecovered
        fl = [c.log for c in cs if not c.ok and not c.killed][:3]
        out.append(Check(f"cycles.{st}", "FAIL" if bad else ("WARN" if fails else "PASS"),
                         f"{len(cs)} runs, {kills} chaos-killed, {fails} failed, longest failure streak {worst} "
                         f"(allowed {allowed_streak}); last run {'ok' if cs and cs[-1].ok else 'FAILED'}"
                         + (f"; failure logs: {fl}" if fl else "")))
    return out


def trend_checks(cycles: list[Cycle], streams: tuple[str, ...]) -> list[Check]:
    """RSS (1.5x, 16 MiB floor) and duration (3x, warn) trends per gated stream; snapshots INFO."""
    out: list[Check] = []
    for st in sorted({c.stream for c in cycles}):
        cs = [c for c in cycles if c.stream == st and c.ok]
        gated = st in streams
        rss = [c.rss for c in cs if c.rss]
        t = trend(rss)
        if t is None:
            out.append(Check(f"rss.{st}", "INFO", f"{len(rss)} samples — under 8, no trend verdict"))
        else:
            first, last = t
            grew = last > first * 1.5 and last - first > 16 * MIB
            out.append(Check(f"rss.{st}", ("FAIL" if gated else "WARN") if grew else ("PASS" if gated else "INFO"),
                             f"peak RSS first quarter {first / MIB:.1f} MiB, last quarter {last / MIB:.1f} MiB "
                             f"(fail if > 1.5x and > +16 MiB{'' if gated else '; not gated: grows with table size'})"))
        durs = [c.dur for c in cs]
        if len(durs) >= 8:
            q = len(durs) // 4
            a, b = statistics.median(durs[:q]), statistics.median(durs[-q:])
            out.append(Check(f"duration.{st}", "WARN" if b > 3 * a and b - a > 5 else "INFO",
                             f"median cycle {a:.1f}s first quarter → {b:.1f}s last quarter"))
    return out


def retention_check(eng: Engine, samples: list[dict]) -> Check:
    """PostgreSQL's slot is consume-retention: a lag whose last-quarter MINIMUM exceeds 2x the first-quarter peak (and 64 MiB) is a pinned log."""
    key = {"pg": "slot_lag_bytes", "mysql": "binlog_bytes", "mssql": "change_table_rows", "mongo": "oplog_window_s"}[eng.name]
    vals = [s[key] for s in samples if s.get(key) is not None and s[key] >= 0]
    if eng.name != "pg":
        return Check(f"retention.{key}", "INFO", f"{len(vals)} samples, first {vals[:1]}, last {vals[-1:]}, "
                     "reader-independent retention: rivet cannot pin it")
    if len(vals) < 8:
        return Check("retention.slot_lag_bytes", "INFO", f"{len(vals)} samples — under 8, no trend verdict")
    q = len(vals) // 4
    first_max, last_min = max(vals[:q]), min(vals[-q:])
    pinned = last_min > max(64 * MIB, 2 * first_max)
    return Check("retention.slot_lag_bytes", "FAIL" if pinned else "PASS",
                 f"slot lag first-quarter max {first_max / MIB:.1f} MiB, last-quarter MIN {last_min / MIB:.1f} MiB "
                 "(a pinned slot's minimum only grows)")


def harm_summary(d: Path) -> Check:
    """rivet's own source-harm counters (export_harm in the state DB beside the config) — reported, not graded."""
    db = d / ".rivet_state.db"
    if not db.exists():
        return Check(f"harm.{d.name}", "INFO", "no state DB")
    try:
        with sqlite3.connect(db) as c:  # WAL-mode DB: a mode=ro open fails without its -shm
            rows = c.execute("SELECT metric, count(*), sum(delta), max(delta) FROM export_harm GROUP BY metric").fetchall()
    except sqlite3.Error as e:
        return Check(f"harm.{d.name}", "INFO", f"unreadable: {e}")
    if not rows:
        return Check(f"harm.{d.name}", "INFO", "no export_harm rows recorded for this engine/mode")
    return Check(f"harm.{d.name}", "INFO", "; ".join(f"{m}: n={n} sum={s} max={mx}" for m, n, s, mx in rows))


# ══ chaos: toxiproxy ═══════════════════════════════════════════════════════════
def toxi(method: str, path: str, body: dict | None = None) -> tuple[int, str]:
    """One Toxiproxy admin call."""
    req = urllib.request.Request(TOXI + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except OSError as e:
        return 0, str(e)


def net_chaos(eng: Engine, stop: threading.Event, events: list) -> None:
    """Alternate a 20 s latency toxic and a 5 s connection-reset toxic on the engine's proxy until stopped."""
    name = eng.proxy[0]
    k = 0
    while not stop.wait(40):
        k += 1
        tox = ({"name": "soak_latency", "type": "latency", "stream": "downstream",
                "attributes": {"latency": 400, "jitter": 300}}, 20) if k % 2 else \
              ({"name": "soak_reset", "type": "reset_peer", "stream": "downstream", "attributes": {"timeout": 0}}, 5)
        code, _ = toxi("POST", f"/proxies/{name}/toxics", tox[0])
        events.append({"t": time.time(), "toxic": tox[0]["name"], "status": code})
        stop.wait(tox[1])
        toxi("DELETE", f"/proxies/{name}/toxics/{tox[0]['name']}")


# ══ orchestration ══════════════════════════════════════════════════════════════
@dataclass
class EngineRun:
    eng: Engine
    dirs: dict[str, Path]
    journal: Path
    writer: Writer | None = None
    slot: str | None = None
    ci: str | None = None
    retention: list[dict] = field(default_factory=list)
    checks: dict[str, list[Check]] = field(default_factory=lambda: {"cdc": [], "batch": []})
    chaos_events: list = field(default_factory=list)
    periodic: dict = field(default_factory=lambda: {"cdc": [0, 0], "batch": [0, 0]})
    url: str = ""


class Soak:
    def __init__(self, a: argparse.Namespace) -> None:
        self.a = a
        self.rivet = cdc_stand.resolve_bin(a.bin)
        stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        self.dir = Path(a.out or ROOT / "dev" / "soak_runs" / stamp).resolve()
        self.logs = self.dir / "logs"
        self.logs.mkdir(parents=True, exist_ok=True)
        self.runner = Rivet(self.rivet, self.logs, timeout=a.cycle_timeout)
        self.cycles: list[Cycle] = []
        self.clock = threading.Lock()
        self.stop_writers = threading.Event()
        self.stop_sched = threading.Event()
        self.skips: list[dict] = []
        self.runs: dict[str, EngineRun] = {}
        self.errors: list[str] = []
        self.modes = set(a.modes)

    def record(self, c: Cycle, er: EngineRun) -> Cycle:
        """Keep a cycle and print its one-line summary."""
        with self.clock:
            self.cycles.append(c)
        state = "ok" if c.ok else ("KILLED" if c.killed else f"rc={c.rc}")
        log(f"{c.stream:<16} #{c.n:<4} {state:<7} {c.dur:6.1f}s rss={(c.rss or 0) / MIB:6.1f}MiB", tag=er.eng.name)
        return c

    def kill_after(self, n: int) -> float | None:
        """Seconds after which this run is SIGKILLed under --chaos kill, else None."""
        if self.a.chaos == "kill" and n % self.a.kill_every == self.a.kill_every - 1:
            return random.uniform(0.3, 3.0)
        return None

    def cdc_env(self) -> dict:
        """Spill opt-in with a small row cap, so the big transaction exercises the spill path."""
        return {"RIVET_CDC_MAX_TX_ROWS": str(self.a.spill_cap), "RIVET_CDC_SPILL_DIR": "1"}

    # ── setup ──
    def setup(self, name: str) -> None:
        """Preflight, create, generate configs and pin the CDC anchor for one engine; SKIP it loudly on any failure."""
        eng = ENGINES[name]
        reason = eng.up()
        if reason:
            self.skip(name, "all", reason)
            return
        modes = set(self.modes)
        if "cdc" in modes:
            r = eng.cdc_ready()
            if r:
                self.skip(name, "cdc", r)
                modes.discard("cdc")
        if not modes:
            return
        work = self.dir / "work"
        try:
            eng.create(cdc="cdc" in modes)
            dirs = setup_configs(self.rivet, eng, work, modes)
        except Exception as e:  # noqa: BLE001 — any setup failure is a loud per-engine FAIL, not an aborted soak
            eng.drop_all("rivet_soak_load", "dbo_soak_load")
            self.skip(name, "all", f"setup failed: {getattr(e, 'message', None) or e}", fail=True)
            return
        er = EngineRun(eng, dirs, self.dir / f"journal-{name}.jsonl")
        er.url = eng.url
        if self.a.chaos == "net":
            if eng.proxy is None:
                self.skip(name, "chaos=net", "no free published toxiproxy port routes to this engine — runs WITHOUT "
                          "network chaos")
            else:
                pname, port, upstream = eng.proxy
                if eng.proxy_owned:
                    toxi("POST", "/proxies", {"name": pname, "listen": f"0.0.0.0:{port}", "upstream": upstream,
                                              "enabled": True})
                code, _ = toxi("GET", f"/proxies/{pname}")
                if code != 200:
                    self.skip(name, "chaos=net", f"toxiproxy proxy {pname} unavailable (HTTP {code}) — runs WITHOUT "
                              "network chaos")
                else:
                    er.url = eng.proxy_url
        if "cdc" in dirs:
            import yaml

            cdc = yaml.safe_load((dirs["cdc"] / "c.yaml").read_text())
            er.slot = cdc["exports"][0]["cdc"].get("slot")
            er.ci = cdc["exports"][0]["cdc"].get("capture_instance")
            # PIN before any churn: MySQL/Mongo have client-side anchors, PG creates the slot here.
            c = self.record(self.runner.run(name, "pin", 0, dirs["cdc"], er.url, extra_env=self.cdc_env()), er)
            if not c.ok:
                eng.drop_all(er.slot, er.ci)
                self.skip(name, "all", f"the CDC anchor (pin) run failed — see {c.log}", fail=True)
                return
        self.runs[name] = er

    def skip(self, engine: str, what: str, reason: str, fail: bool = False) -> None:
        """Record a SKIP (or a setup FAIL) loudly."""
        self.skips.append({"engine": engine, "what": what, "reason": reason, "fail": fail})
        shell.bad(f"SKIP {engine}/{what}: {reason}") if fail else shell.skip(f"SKIP {engine}/{what}: {reason}")

    # ── scheduler loops ──
    def loop(self, er: EngineRun, mode: str, interval: float, body: Callable[[int], None]) -> None:
        """Call body(n) every `interval` seconds until stopped; a crashed iteration is recorded, not fatal."""
        n = 0
        while not self.stop_sched.is_set():
            t0 = time.time()
            try:
                body(n)
            except Exception as e:  # noqa: BLE001 — e.g. the source restarting under us
                er.checks[mode].append(Check("harness.iteration_error", "WARN",
                                             f"cycle {n}: {type(e).__name__}: {str(e)[:240]}"))
                shell.warn(f"{er.eng.name}/{mode}: iteration {n} crashed: {str(e)[:200]}")
            n += 1
            self.stop_sched.wait(max(0.0, interval - (time.time() - t0)))

    def cdc_loop(self, er: EngineRun) -> None:
        """Bounded CDC cycles with periodic gap checks and retention samples."""
        def body(n: int) -> None:
            t_open = time.time()
            c = self.record(self.runner.run(er.eng.name, "cdc", n, er.dirs["cdc"], er.url,
                                            kill_after=self.kill_after(n), extra_env=self.cdc_env()), er)
            if c.ok:
                self.sample(er)
                if n % self.a.check_every == 0:
                    self.periodic(er, "cdc", n, lambda: [check_cdc_gap(er.eng, er.dirs["cdc"], er.journal,
                                                                       t_open - self.a.settle)])
        self.loop(er, "cdc", self.a.cdc_interval, body)

    def batch_loop(self, er: EngineRun) -> None:
        """Incremental cycles with periodic completeness checks and a snapshot every N cycles."""
        def body(n: int) -> None:
            if "incremental" in er.dirs:
                c = self.record(self.runner.run(er.eng.name, "incremental", n, er.dirs["incremental"], er.url,
                                                kill_after=self.kill_after(n)), er)
                if c.ok and n % self.a.check_every == 0:
                    self.periodic(er, "batch", n, lambda: check_incremental(
                        er.eng, er.dirs["incremental"], er.journal, self.a.settle, False, None))
            if n % self.a.snapshot_every == 0:
                k = n // self.a.snapshot_every
                self.snapshot(er, "keyset" if k % 2 == 0 else "range", k)
        self.loop(er, "batch", self.a.inc_interval, body)

    def periodic(self, er: EngineRun, mode: str, n: int, fn: Callable[[], list[Check]]) -> None:
        """Run a lightweight mid-soak oracle; keep failures (and crashes) as checks, count every run."""
        try:
            checks = fn()
        except Exception as e:  # noqa: BLE001 — a crashed oracle is a failed oracle, never a silent pass
            checks = [Check(f"harness.periodic_{mode}", "FAIL", f"{type(e).__name__}: {str(e)[:300]}")]
        er.periodic[mode][0] += 1
        for chk in checks:
            if chk.status == "FAIL":
                er.periodic[mode][1] += 1
                er.checks[mode].append(Check(chk.name, chk.status, f"cycle {n}: {chk.detail}"))
                shell.bad(f"{er.eng.name}: periodic {chk.name} at cycle {n}: {chk.detail}")

    def snapshot(self, er: EngineRun, kind: str, k: int) -> None:
        """One full snapshot (retried after a chaos kill, which the next attempt must recover), then its checks."""
        d = er.dirs[f"snapshot-{kind}"]
        out = next(iter(yaml_dest(d)), d / "output")
        s = time.time()  # before the count: a window that starts early only widens the bounds
        c0 = er.eng.count()
        attempt, c = 0, None
        while attempt < 3:
            c = self.record(self.runner.run(er.eng.name, f"snapshot-{kind}", k, d, er.url,
                                            kill_after=self.kill_after(k) if attempt == 0 else None), er)
            if c.ok:
                break
            attempt += 1
        e, c1 = time.time(), er.eng.count()
        # A batch can be visible to the snapshot before the writer has journaled it: wait for the journal to pass e.
        shell.wait_until(lambda: not er.writer.is_alive() or er.writer.acked >= e, tries=600, delay=0.2)
        if not c or not c.ok:
            return  # recorded as an unrecovered cycle failure
        try:
            checks = check_snapshot(er.eng, out, er.journal, s, e, c0, c1,
                                    red=self.a.red == "dup-snapshot-part")
        except Exception as ex:  # noqa: BLE001 — a crashed oracle is a failed oracle
            checks = [Check("harness.snapshot_check", "FAIL", f"{type(ex).__name__}: {str(ex)[:300]}")]
        failed = [x for x in checks if x.status == "FAIL"]
        for x in checks:
            er.checks["batch"].append(Check(f"{x.name}.{kind}", x.status, f"snapshot {k}: {x.detail}"))
        keep = self.dir / "snapshots" / f"{er.eng.name}-{k:03d}-{kind}"
        if failed or self.a.keep_parts:
            keep.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(out), keep)
        else:
            shutil.rmtree(out, ignore_errors=True)

    def sample(self, er: EngineRun) -> None:
        """One retention sample."""
        try:
            er.retention.append({"t": time.time(), **er.eng.retention(er.slot)})
        except Exception as e:  # noqa: BLE001
            er.retention.append({"t": time.time(), "error": str(e)[:200]})

    # ── the run ──
    def run(self) -> int:
        """The whole soak: setup, writers + schedulers for `duration`, final oracles, report; teardown always."""
        a = self.a
        log(f"soak → {self.dir}  bin={self.rivet}  engines={a.engines} modes={a.modes} duration={a.duration:.0f}s "
            f"rate={a.rate}/s chaos={a.chaos}" + (f" RED={a.red}" if a.red else ""))
        try:
            for name in a.engines:
                self.setup(name)
            if not self.runs:
                raise shell.Fail("no engine could be set up — nothing was soaked", code=2)
            big_every = a.big_tx_every or max(60.0, min(600.0, a.duration / 3))
            for er in self.runs.values():
                er.writer = Writer(er.eng, er.journal, a.rate, a.seed_rows, a.big_tx_rows, big_every, self.stop_writers)
                er.writer.start()
            threads: list[threading.Thread] = []
            for er in self.runs.values():
                if "cdc" in er.dirs:
                    threads.append(threading.Thread(target=self.guard, args=(self.cdc_loop, er), daemon=True))
                if any(k in er.dirs for k in ("incremental", "snapshot-keyset")):
                    threads.append(threading.Thread(target=self.guard, args=(self.batch_loop, er), daemon=True))
                if a.chaos == "net" and er.url != er.eng.url:
                    threads.append(threading.Thread(target=net_chaos, args=(er.eng, self.stop_sched, er.chaos_events),
                                                    daemon=True))
            for t in threads:
                t.start()
            deadline = time.time() + a.duration
            while time.time() < deadline:
                time.sleep(min(60, max(0.1, deadline - time.time())))
                dead = [er.eng.name for er in self.runs.values() if er.writer and not er.writer.is_alive()]
                if dead:
                    log(f"writer died on {dead} — ending the soak early", tag="soak")
                    break
                log(f"{max(0, deadline - time.time()) / 60:5.1f} min left; ops so far "
                    + ", ".join(f"{n}={er.writer.stats['ops']}" for n, er in self.runs.items()))
            log("stopping writers, then the schedulers")
            self.stop_writers.set()
            for er in self.runs.values():
                er.writer.join(timeout=300)
            self.stop_sched.set()
            for t in threads:
                t.join(timeout=a.cycle_timeout + 60)
            self.final()  # final runs go straight at the source, so a leftover toxic cannot touch them
            return self.report()
        finally:
            self.teardown()

    def guard(self, fn: Callable, er: EngineRun) -> None:
        """Run a scheduler, turning a crash into a recorded harness error."""
        try:
            fn(er)
        except Exception as e:  # noqa: BLE001 — a crashed scheduler is a harness failure, recorded
            self.errors.append(f"{er.eng.name} {fn.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()

    def final(self) -> None:
        """Drain to the end, then run the end-of-run oracles per engine and mode."""
        for name, er in self.runs.items():
            if er.writer.error:
                self.errors.append(f"{name} writer: {er.writer.error}")
            if not shell.wait_until(lambda: er.eng.up() is None, tries=36, delay=5.0):
                self.errors.append(f"{name}: source unreachable for 180 s at the end of the run — no final oracle")
                continue
            for mode, fn in (("cdc", self.final_cdc_mode), ("batch", self.final_batch_mode)):
                try:
                    fn(er)
                except Exception as e:  # noqa: BLE001 — a crashed end-of-run oracle is a FAILED oracle
                    traceback.print_exc()
                    er.checks[mode].append(Check("harness.final_oracle", "FAIL", f"{type(e).__name__}: {str(e)[:300]}"))

    def final_run(self, er: EngineRun, stream: str, d: Path, env: dict | None = None) -> Cycle:
        """A final run straight at the source (no proxy), retried with backoff."""
        c = None
        for k in range(3):
            c = self.record(self.runner.run(er.eng.name, stream, k, d, er.eng.url, extra_env=env), er)
            if c.ok:
                break
            time.sleep(10 * (k + 1))
        return c

    def final_cdc_mode(self, er: EngineRun) -> None:
        """Final drain (after the Agent catches up on SQL Server), then the end-of-run CDC oracles."""
        if "cdc" not in er.dirs:
            return
        if er.eng.name == "mssql":
            self.wait_mssql_agent(er)
        c = self.final_run(er, "final-cdc", er.dirs["cdc"], self.cdc_env())
        self.sample(er)
        if self.a.red == "corrupt-source":
            corrupt_source(er)  # after the drain, so the log cannot carry the change into the parts
        er.checks["cdc"] += final_cdc(er.eng, er.dirs["cdc"], er.journal, self.a.red)
        er.checks["cdc"].append(Check("cdc.final_drain", "PASS" if c.ok else "FAIL",
                                      "final drain " + ("succeeded" if c.ok else f"failed 3x, see {c.log}")))

    def final_batch_mode(self, er: EngineRun) -> None:
        """Final incremental run, then the end-of-run incremental oracles."""
        if "incremental" in er.dirs:
            c = self.final_run(er, "final-incremental", er.dirs["incremental"])
            if c.ok:
                er.checks["batch"] += check_incremental(er.eng, er.dirs["incremental"], er.journal, 0, True, self.a.red)
            else:
                er.checks["batch"].append(Check("incremental.final_run", "FAIL", f"failed 3x, see {c.log}"))
        elif "batch" in self.modes and not er.eng.incremental:
            er.checks["batch"].append(Check("incremental", "SKIP",
                                            "not applicable: rivet's MongoDB source is full-only; snapshots only"))

    def wait_mssql_agent(self, er: EngineRun) -> None:
        """SQL Server capture is asynchronous: wait until the change table holds every journal op (ins+del+2*upd)."""
        import duckdb

        i, u, dl = duckdb.connect().sql(
            f"SELECT count(*) FILTER (WHERE op='i'), count(*) FILTER (WHERE op='u'), count(*) FILTER (WHERE op='d') "
            f"FROM {journal_rel(er.journal)}").fetchone()
        want = i + dl + 2 * u
        ok = shell.wait_until(lambda: er.eng.retention(None)["change_table_rows"] >= want, tries=150, delay=2.0)
        if not ok:
            got = er.eng.retention(None)["change_table_rows"]
            er.checks["cdc"].append(Check("mssql.agent_caught_up", "WARN",
                                          f"change table holds {got} of {want} rows after 300 s — final drain is partial"))

    def teardown(self) -> None:
        """Drop every table, slot, capture instance and proxy the soak created."""
        if self.a.keep_source:
            log("--keep-source: leaving tables, slots and capture instances in place")
            return
        for er in self.runs.values():
            er.eng.drop_all(er.slot, er.ci)
        for name in self.a.engines:
            eng = ENGINES[name]
            if eng.proxy and self.a.chaos == "net":
                for tn in ("soak_latency", "soak_reset"):
                    toxi("DELETE", f"/proxies/{eng.proxy[0]}/toxics/{tn}")
                if eng.proxy_owned:
                    toxi("DELETE", f"/proxies/{eng.proxy[0]}")

    # ── report ──
    def report(self) -> int:
        """Grade, write soak-report.json + soak-report.md, return the exit code."""
        a = self.a
        results: dict = {}
        any_fail = bool(self.errors) or any(s["fail"] for s in self.skips)
        for name, er in self.runs.items():
            ec = [c for c in self.cycles if c.engine == name]
            for mode, streams in (("cdc", ("cdc",)), ("batch", ("incremental",))):
                mine = [c for c in ec if (c.stream in ("cdc", "final-cdc") if mode == "cdc" else
                                          c.stream in ("incremental", "final-incremental")
                                          or c.stream.startswith("snapshot"))]
                sched = [c for c in mine if not c.stream.startswith("final-")]
                if mode == "cdc" and "cdc" not in er.dirs:
                    continue
                if mode == "batch" and not any(k in er.dirs for k in ("incremental", "snapshot-keyset")):
                    continue
                ran, failed = er.periodic[mode]
                checks = cycle_checks(mine, a.chaos) + list(er.checks[mode]) + trend_checks(sched, streams)
                if mode == "cdc" or "incremental" in er.dirs:
                    checks.append(Check(f"{'cdc.no_gap' if mode == 'cdc' else 'incremental.complete'}.periodic_runs",
                                        "FAIL" if failed else ("PASS" if ran else "WARN"),
                                        f"{ran} periodic checks ran, {failed} failed"
                                        + ("" if ran else " — NONE ran, so nothing mid-soak was verified")))
                if mode == "cdc":
                    checks.append(retention_check(er.eng, er.retention))
                    checks.append(harm_summary(er.dirs["cdc"]))
                else:
                    checks += [harm_summary(er.dirs[k]) for k in er.dirs if k != "cdc"]
                verdict = "FAIL" if any(c.status == "FAIL" for c in checks) else "PASS"
                any_fail |= verdict == "FAIL"
                results.setdefault(name, {})[mode] = {
                    "verdict": verdict,
                    "checks": [c.__dict__ for c in checks],
                    "series": {
                        st: {"t": [round(c.t0 - self.cycles[0].t0, 1) for c in mine if c.stream == st],
                             "dur_s": [round(c.dur, 2) for c in mine if c.stream == st],
                             "rss_mib": [round((c.rss or 0) / MIB, 1) for c in mine if c.stream == st],
                             "rc": [c.rc for c in mine if c.stream == st],
                             "killed": [c.killed for c in mine if c.stream == st]}
                        for st in sorted({c.stream for c in mine})},
                }
            results.setdefault(name, {})["writer"] = er.writer.stats
            results[name]["retention"] = er.retention
            results[name]["chaos_events"] = er.chaos_events
        doc = {"args": {k: v for k, v in vars(a).items()}, "binary": str(self.rivet),
               "rivet_version": shell.run([str(self.rivet), "--version"]).stdout.strip(),
               "started": self.dir.name, "skips": self.skips, "harness_errors": self.errors, "results": results,
               "verdict": "FAIL" if any_fail else "PASS"}
        (self.dir / "soak-report.json").write_text(json.dumps(doc, indent=1, default=str))
        (self.dir / "soak-report.md").write_text(render_md(doc))
        print(render_md(doc))
        log(f"report: {self.dir / 'soak-report.md'}")
        ran = sum(1 for r in results.values() for m in ("cdc", "batch") if m in r)
        if not ran:
            return 2
        return 1 if any_fail else 0


def yaml_dest(d: Path) -> list[Path]:
    """The config's local destination directory, resolved against the config dir (rivet runs with cwd there)."""
    import yaml

    cfg = yaml.safe_load((d / "c.yaml").read_text())
    return [(d / cfg["exports"][0]["destination"]["path"]).resolve()]


def render_md(doc: dict) -> str:
    """The markdown summary: verdict table, per-check tables, sparklines."""
    a = doc["args"]
    L = [f"# rivet soak — {doc['verdict']}", "",
         f"`{doc['rivet_version']}` · engines {','.join(a['engines'])} · modes {','.join(a['modes'])} · "
         f"{a['duration']:.0f}s · {a['rate']} ops/s/engine · chaos {a['chaos']}"
         + (f" · **RED mutation `{a['red']}`**" if a.get("red") else ""), ""]
    if doc["skips"]:
        L += ["## SKIPPED — what is listed here was NOT exercised", "", "| engine | what | reason |", "|---|---|---|"]
        L += [f"| {s['engine']} | {s['what']} | {'**FAIL** ' if s['fail'] else ''}{s['reason']} |" for s in doc["skips"]]
        L.append("")
    if doc["harness_errors"]:
        L += ["## Harness errors", ""] + [f"- {e}" for e in doc["harness_errors"]] + [""]
    L += ["## Verdicts", "", "| engine | cdc | batch | writer ops (i/u/d) | achieved ops/s per minute |", "|---|---|---|---|---|"]
    for name, r in doc["results"].items():
        w = r["writer"]
        L.append(f"| {name} | {r.get('cdc', {}).get('verdict', '—')} | {r.get('batch', {}).get('verdict', '—')} | "
                 f"{w['ops']} ({w['i']}/{w['u']}/{w['d']}), {w['batches']} tx, {w['big_tx']} big, "
                 f"{w['failed_batches']} rolled back | {spark(w['per_minute'])} |")
    for name, r in doc["results"].items():
        for mode in ("cdc", "batch"):
            if mode not in r:
                continue
            L += ["", f"## {name} / {mode} — {r[mode]['verdict']}", "", "| check | status | detail |", "|---|---|---|"]
            L += [f"| {c['name']} | {c['status']} | {c['detail'].replace('|', '/')} |" for c in r[mode]["checks"]]
            L += ["", "| stream | runs | RSS MiB (min…max) | RSS trend | duration s trend |", "|---|---|---|---|---|"]
            for st, s in r[mode]["series"].items():
                rss = [x for x in s["rss_mib"] if x]
                L.append(f"| {st} | {len(s['rc'])} | {min(rss, default=0)}…{max(rss, default=0)} | {spark(rss)} | "
                         f"{spark(s['dur_s'])} |")
        ret = [x for x in r.get("retention", []) if "error" not in x]
        if ret:
            key = [k for k in ret[0] if k != "t"][0]
            L += ["", f"retention `{key}`: {spark([x.get(key) for x in ret])} (last {ret[-1].get(key)})"]
    return "\n".join(L) + "\n"


def corrupt_source(er: EngineRun) -> None:
    """RED: change one source value WITHOUT journaling it — both the replay and journal checks must fail."""
    eng = er.eng
    if eng.sql:
        eng.cli(f"UPDATE {eng.qualified()} SET amount = amount + 1 WHERE id = (SELECT MIN(id) FROM "
                f"(SELECT id FROM {eng.qualified()}) t);\n").check("red: corrupt source")
    else:
        doc = eng.coll().find_one(sort=[("_id", 1)])
        from bson.decimal128 import Decimal128

        eng.coll().update_one({"_id": doc["_id"]}, {"$set": {"amount": Decimal128(str(Decimal(str(doc["amount"])) + 1))}})


def _self_test() -> int:
    """The pure grading rules, checked without a stand: `python -m dev.pytools.soak --self-test`."""
    assert parse_duration("1h30m") == 5400 and parse_duration("10m") == 600
    mk = lambda st, rc, killed=False, t=0.0: Cycle("pg", st, 0, t, 1.0, rc, 20 * MIB, killed=killed)  # noqa: E731
    ok = cycle_checks([mk("cdc", 0, t=1), mk("cdc", 137, True, t=2), mk("cdc", 0, t=3)], "kill")[0]
    assert ok.status == "PASS", ok
    rec = cycle_checks([mk("cdc", 0, t=1), mk("cdc", 1, t=2), mk("final-cdc", 0, t=3)], "none")[0]
    assert rec.status == "WARN", rec  # a failure the next run recovered from
    stuck = cycle_checks([mk("cdc", 0, t=1), mk("cdc", 1, t=2), mk("cdc", 1, t=3), mk("cdc", 0, t=4)], "none")[0]
    assert stuck.status == "FAIL", stuck
    assert cycle_checks([mk("cdc", 0, t=1), mk("cdc", 1, t=2)], "none")[0].status == "FAIL"
    flat = [{"slot_lag_bytes": v} for v in [MIB, 2 * MIB, 0, MIB, 0, MIB, 2 * MIB, 0]]
    assert retention_check(ENGINES["pg"], flat).status == "PASS"
    pinned = [{"slot_lag_bytes": 100 * MIB * (i + 1)} for i in range(8)]
    assert retention_check(ENGINES["pg"], pinned).status == "FAIL"
    leak = [Cycle("pg", "cdc", i, i, 1.0, 0, (20 + 10 * i) * MIB) for i in range(8)]
    assert trend_checks(leak, ("cdc",))[0].status == "FAIL"
    noise = [Cycle("pg", "cdc", i, i, 1.0, 0, (20 + i % 2 * 12) * MIB) for i in range(8)]
    assert trend_checks(noise, ("cdc",))[0].status == "PASS"
    print("soak self-test ok")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse the CLI and run one soak."""
    if (argv if argv is not None else sys.argv[1:]) == ["--self-test"]:
        return _self_test()
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--engines", default=",".join(ENGINES), help="comma list (default: all)")
    p.add_argument("--modes", default="batch,cdc")
    p.add_argument("--duration", type=parse_duration, default=parse_duration("4h"))
    p.add_argument("--rate", type=float, default=200, help="target ops/s per engine")
    p.add_argument("--chaos", choices=("none", "kill", "net"), default="none")
    p.add_argument("--kill-every", type=int, default=5, help="--chaos kill: SIGKILL every Nth run of each stream")
    p.add_argument("--cdc-interval", type=float, default=30)
    p.add_argument("--inc-interval", type=float, default=30)
    p.add_argument("--snapshot-every", type=int, default=4, help="a full snapshot every Nth batch cycle")
    p.add_argument("--check-every", type=int, default=5, help="periodic lightweight oracle every Nth cycle")
    p.add_argument("--settle", type=float, default=15, help="seconds a change must be old before a periodic check demands it")
    p.add_argument("--seed-rows", type=int, default=2000)
    p.add_argument("--big-tx-rows", type=int, default=5000)
    p.add_argument("--big-tx-every", type=float, default=0, help="seconds (default: duration/3, clamped 60..600)")
    p.add_argument("--spill-cap", type=int, default=1000, help="RIVET_CDC_MAX_TX_ROWS for CDC runs (spill opt-in)")
    p.add_argument("--cycle-timeout", type=float, default=900)
    p.add_argument("--bin", default=None, help="rivet binary (default RIVET_BIN / target/release/rivet)")
    p.add_argument("--out", default=None, help="run directory (default dev/soak_runs/<timestamp>)")
    p.add_argument("--keep-parts", action="store_true", help="keep passing snapshots too")
    p.add_argument("--keep-source", action="store_true", help="skip teardown (debugging)")
    p.add_argument("--red", choices=RED_MUTATIONS, default=None, help="mutate the harness INPUT to prove an oracle bites")
    a = p.parse_args(argv)
    a.engines = [e.strip() for e in a.engines.split(",") if e.strip()]
    a.modes = [m.strip() for m in a.modes.split(",") if m.strip()]
    bad = [e for e in a.engines if e not in ENGINES] + [m for m in a.modes if m not in ("batch", "cdc")]
    if bad:
        p.error(f"unknown engine/mode: {bad}")
    shell.require("/usr/bin/time", hint="install GNU time")
    return Soak(a).run()


if __name__ == "__main__":
    shell.main(lambda: main())
