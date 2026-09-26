"""`tls: {mode: verify-full}` against the loopback stand must REFUSE, never export in plaintext.

PG and Mongo serve plaintext; MySQL 8 and SQL Server present self-signed certs verify-full must reject.

The loopback host exempts a config from the TLS-REQUIRED policy gate
(`require_tls_or_loopback`), so on the stand the only thing between an enforced
mode and a plaintext export is each driver honouring `is_enforced()` on its own
connect path. Those paths are separate per engine and per leg (batch source, the
MySQL preflight pool behind `check`, each CDC reader), so each is its own case.
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

try:
    from .core import Ledger, port_of, rivet
    from .scenarios import NO_TIMEOUT, _failed, _passed, _skipped, _tcp_open, work_dir
    from .cdc import _mysql, _psql, _sqlcmd
    from ..pytools import registry
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import Ledger, port_of, rivet  # type: ignore
    from scenarios import NO_TIMEOUT, _failed, _passed, _skipped, _tcp_open, work_dir  # type: ignore
    from cdc import _mysql, _psql, _sqlcmd  # type: ignore
    import registry  # type: ignore

__all__ = ["verify_tls_downgrade_refused"]

#: What a TLS refusal or failed handshake says, across the four drivers.
TLS_SHAPED = re.compile(r"tls|ssl|certificat|handshake|encrypt", re.IGNORECASE)

# (label, stand source, rivet subcommand, cdc?)
CASES = [
    ("postgres cdc run", "postgres_cdc", "run", True),
    ("mysql batch run", "mysql", "run", False),
    ("mysql check (pool)", "mysql", "check", False),
    ("mysql cdc run", "mysql_cdc", "run", True),
    ("mssql batch run", "mssql", "run", False),
    ("mongo batch run", "mongo", "run", False),
]


def _seed(src: str, url: str, table: str) -> bool:
    """Create a three-row table (collection) the enforced config would export if it connected."""
    if src.startswith("postgres"):
        return _psql(url, sql=f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table}(id int PRIMARY KEY, v text); "
                              f"INSERT INTO {table} VALUES (1,'a'),(2,'b'),(3,'c');").ok
    if src.startswith("mysql"):
        return _mysql(url, f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table}(id int PRIMARY KEY, v text); "
                           f"INSERT INTO {table} VALUES (1,'a'),(2,'b'),(3,'c');").ok
    if src == "mssql":
        return _sqlcmd(url, q=f"DROP TABLE IF EXISTS dbo.{table}; CREATE TABLE dbo.{table}(id int PRIMARY KEY, v nvarchar(10)); "
                              f"INSERT INTO dbo.{table} VALUES (1,'a'),(2,'b'),(3,'c');").ok
    try:
        import pymongo

        with pymongo.MongoClient(url, serverSelectionTimeoutMS=5000) as c:
            c.rivet[table].drop()
            c.rivet[table].insert_many([{"_id": i, "v": v} for i, v in ((1, "a"), (2, "b"), (3, "c"))])
        return True
    except Exception:  # noqa: BLE001 — an unseedable source is a SKIP, never a crash of the gate
        return False


def _drop(src: str, url: str, table: str) -> None:
    """Remove the seeded table and, for PG CDC, the slot the refused run must never have created."""
    if src.startswith("postgres"):
        _psql(url, sql=f"SELECT pg_drop_replication_slot('{table}') FROM pg_replication_slots "
                       f"WHERE slot_name='{table}'; DROP TABLE IF EXISTS {table};")
    elif src.startswith("mysql"):
        _mysql(url, f"DROP TABLE IF EXISTS {table};")
    elif src == "mssql":
        _sqlcmd(url, q=f"DROP TABLE IF EXISTS dbo.{table};")
    else:
        try:
            import pymongo

            with pymongo.MongoClient(url, serverSelectionTimeoutMS=5000) as c:
                c.rivet[table].drop()
        except Exception:  # noqa: BLE001 — cleanup is best-effort
            pass


def _config(src: str, url: str, table: str, out: Path, cdc: bool) -> str:
    """One export under `tls: {mode: verify-full}` — the only non-default line is the enforced mode."""
    engine = {"postgres_cdc": "postgres", "mysql_cdc": "mysql"}.get(src, src)
    if src == "mongo":
        # The refused handshake surfaces as a server-selection timeout, retried 3x: 3 s, not the driver's 30 s.
        url = url.rstrip("/") + "/rivet?serverSelectionTimeoutMS=3000"
    mode = "    mode: full\n"
    if cdc:
        block = (f"slot: {table}" if engine == "postgres"
                 else f'checkpoint: "{out.parent}/{table}.ckpt", server_id: {7000 + os.getpid() % 1000}')
        mode = f"    mode: cdc\n    cdc: {{ {block}, until_current: true }}\n"
    return (
        f"source:\n  type: {engine}\n  url: \"{url}\"\n  tls: {{ mode: verify-full }}\n"
        f"exports:\n  - name: {table}\n    table: {table}\n{mode}"
        f"    format: parquet\n    destination: {{ type: local, path: {out} }}\n"
    )


def verify_tls_downgrade_refused(led: Ledger) -> None:
    """Each engine leg under enforced `verify-full` over plaintext loopback: exit != 0, zero parts, a TLS-shaped error."""
    led.phase("TLS downgrade refused (verify-full over the loopback stand)")
    work = work_dir() / f"tls_downgrade_{os.getpid()}"
    work.mkdir(parents=True, exist_ok=True)
    for n, (label, src, cmd, cdc) in enumerate(CASES):
        scen = "tls_downgrade_refused"
        url = registry.source(src)["url"]
        port = port_of(url) or 0
        if not _tcp_open("127.0.0.1", port):
            _skipped(led, "infra", src, scen, "-", f"tls-downgrade[{label}]: {src} :{port} down", "down")
            continue
        table = f"tls_{os.getpid()}_{n}"
        out = work / table
        try:
            if not _seed(src, url, table):
                _skipped(led, "infra", src, scen, "-", f"tls-downgrade[{label}]: could not seed {table}", "seed")
                continue
            cfg = work / f"{table}.yaml"
            cfg.write_text(_config(src, url, table, out, cdc))
            t0 = time.perf_counter()
            p = rivet(cmd, "-c", str(cfg), timeout=NO_TIMEOUT)
            dt = time.perf_counter() - t0
            parts = list(out.rglob("*.parquet")) if out.exists() else []
            tail = p.stderr.strip().splitlines()[-1:] or [""]
            if p.ok or parts:
                _failed(led, "infra", src, scen, "-",
                        f"tls-downgrade[{label}]: DEFECT — verify-full over plaintext "
                        f"exited {p.returncode} with {len(parts)} part(s): {tail[0][:200]}", "plaintext")
            elif not TLS_SHAPED.search(p.stderr):
                _failed(led, "infra", src, scen, "-",
                        f"tls-downgrade[{label}]: refused, but not for a TLS reason "
                        f"(exit {p.returncode}): {tail[0][:200]}", "wrong reason")
            else:
                _passed(led, "infra", src, scen, "-",
                        f"tls-downgrade[{label}]: refused in {dt:.0f}s (exit {p.returncode}, 0 parts): {tail[0][:160]}")
        finally:
            _drop(src, url, table)
