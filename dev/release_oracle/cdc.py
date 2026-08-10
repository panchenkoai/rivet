"""Release-oracle CDC end-to-end stage — the Python port of `lib/cdc.sh`.

CDC is the most engine-divergent, correctness-critical surface, and the batch
scenarios (verdicts/integrity_types/keyset_parallel/load) never exercise it. This
preflight codifies the manual CDC dogfood into the go/no-go gate: per engine, a
typed table is anchored, mutated (INSERT/UPDATE/DELETE), captured (`mode: cdc`,
`until_current`) to an object store, and INDEPENDENTLY read back by DuckDB (never
rivet) — plus rivet's own `validate`, the state-DB metabase population, a
SQLite-vs-Postgres state PARITY snapshot, and an at-least-once CRASH recovery.

Source-specific (a CDC-enabled engine per mechanism: PG logical slot, MySQL
binlog, SQL Server change tables + Agent, Mongo change stream on a replica set),
so — like the state-migration stage — it runs ONCE from env URLs and SKIPs (never
a silent pass) when they are absent. Set, per engine you want gated:

    RIVET_CDC_POSTGRES_URL  postgresql://rivet:rivet@host:port/db  (wal_level=logical)
    RIVET_CDC_MYSQL_URL     mysql://rivet:rivet@host:port/db       (log_bin, ROW)
    RIVET_CDC_MSSQL_URL     sqlserver://rivet:rivet@host:port/db   (CDC enabled + Agent)
    RIVET_CDC_MONGO_URL     mongodb://rivet:rivet@host:port/db?authSource=admin&directConnection=true

and RIVET_CDC_STATE_URL (a Postgres db for the state-parity leg; SKIP that leg if
unset).

The reference state snapshot (эталонный слепок) is golden/cdc_state_snapshot.json:
the canonical set of state-DB tables a CDC run populates. Both backends must match
it — a release that stops populating run_status (or drifts the schema) fails here.

WHY THIS MODULE EXISTS IN PYTHON: of all the layers, this is the one the SHELL
broke rather than the checks it makes — see `_store_readback` below. Every printed
line and every recorded cell keeps the bash wording, so a CI log diff cannot tell
the two implementations apart.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:  # imported as part of the package
    from . import scenarios
    from .core import (
        HERE,
        ROOT,
        Ledger,
        Proc,
        container_for_port,
        docker_exec,
        have,
        port_of,
        rivet,
        run,
        wait_until,
    )
except ImportError:  # run directly out of dev/release_oracle/
    import scenarios  # type: ignore[no-redef]
    from core import (  # type: ignore[no-redef]
        HERE,
        ROOT,
        Ledger,
        Proc,
        container_for_port,
        docker_exec,
        have,
        port_of,
        rivet,
        run,
        wait_until,
    )

CDC_HOOK_FLUSH = "cdc_after_flush_before_ack"  # flushed to store, anchor NOT yet advanced
CDC_HOOK_ACK = "cdc_after_ack"  # past the ack — where a split transaction loses its tail

# The typed change set every engine applies: 3 INSERT, 1 UPDATE (id=2 → 55.55),
# 1 DELETE (id=3) → 5 change events; the id set {1,2,3} and the UPDATE after-image
# (55.55) are the completeness oracle. The crash leg then adds ONE row (id=4) and
# crashes mid-flush — a distinct id so the re-read proves at-least-once with no
# primary-key collision on the re-applied set.
CDC_EXPECT_EVENTS = 5

_LEGACY = ROOT / "dev" / "release-oracle"  # the bash gate, while both coexist
_STATE_TABLES = (
    "run_status",
    "export_metrics",
    "run_journal",
    "run_aggregate",
    "export_state",
    "export_schema",
    # `file_log` — one row per durable part, written by the CDC sink as each part
    # lands. Added when the sink started recording them (2026-08-03): before that
    # a CDC run wrote `begin_run` and one aggregate at finalize and nothing per
    # part, so the manifest at the destination was both finer-grained and earlier
    # than the database. This snapshot calls itself the canonical set of state
    # tables a CDC run populates, so a table the run now populates and the golden
    # does not name is the ledger being wrong about itself.
    "file_log",
)


# ── shared plumbing ───────────────────────────────────────────────────────────
# cdc.sh was SOURCED into scenarios.sh and used its `cfg`, `_store_dest` and
# `_store_readback`; the port keeps that layering — `scenarios` owns the one copy
# of each, so the CDC stage cannot drift from the batch stage's store handling.
_cfg = scenarios.cfg
_store_dest = scenarios.store_dest
_work_root = scenarios.work_dir  # `${WORK##*/}` — the run-unique prefix token


def _golden_snapshot() -> Path:
    """The reference snapshot, preferring a copy beside this file.

    The goldens are DATA shared with the bash gate: a duplicated golden would let
    one implementation bless a snapshot the other never sees.
    """
    local = HERE / "golden" / "cdc_state_snapshot.json"
    return local if local.exists() else _LEGACY / "golden" / "cdc_state_snapshot.json"


def _workdir() -> Path:
    """One engine's scratch dir (`mktemp -d`) — holds cdc.yaml, the CDC checkpoint
    and the SQLite state db, so it is also the state-snapshot's fresh-per-run leg."""
    return Path(tempfile.mkdtemp())


def _export_store_creds() -> None:
    """The bash `export MINIO_… / AZURITE_KEY`.

    The generated configs reference these by `access_key_env`, so they must be in
    the CHILD rivet's environment; the literals come from `scenarios` so there is
    one copy per gate, not one per stage.
    """
    os.environ["MINIO_ACCESS_KEY"] = scenarios.MINIO_ACCESS_KEY
    os.environ["MINIO_SECRET_KEY"] = scenarios.MINIO_SECRET_KEY
    os.environ["AZURITE_KEY"] = scenarios.AZURITE_KEY


# ── engine client shims (docker exec into whatever container serves the URL) ──
# The gate connects from the host; these run SQL against the CDC source. They
# locate the container by matching the URL's host:port to a published docker port,
# so the stage works against any CDC stand (repo-compose or versioned) without
# hard-coding a container name.
def _container_for(url: str) -> str | None:
    port = port_of(url)
    return container_for_port(port) if port is not None else None


def _no_container(url: str) -> Proc:
    """A failed Proc for "no container publishes that URL's port".

    The bash shim passed the empty lookup result straight to `docker exec ""`,
    which fails with a container-name error the caller then mis-blamed on the
    engine. Here the miss is its own non-zero result with a readable reason.
    """
    return Proc(("docker", "exec", "<none>"), 125, "", f"no running container publishes {url}")


def _psql(url: str, *args: str, sql: str | None = None) -> Proc:
    """psql against the CDC source. Either extra ARGS (e.g. `-c "…"`) or SQL on
    stdin — BOTH forms must work: the bash shim originally ignored `"$@"`, which
    silently dropped the `-c` INSERT of the crash delta, so there was no delta to
    re-read and at-least-once looked like a data LOSS.
    """
    c = _container_for(url)
    if c is None:
        return _no_container(url)
    return docker_exec(
        c, "psql", "-U", "rivet", "-d", "rivet", "-v", "ON_ERROR_STOP=1", "-q", *args, stdin=sql
    )


def _mysql(url: str, sql: str) -> Proc:
    """SQL via STDIN, never as an argument: the change set carries JSON
    (`'{"k":1}'`) whose double quotes collide with a `-e "…"` argument and
    silently break the INSERT."""
    c = _container_for(url)
    if c is None:
        return _no_container(url)
    return docker_exec(c, "mysql", "-urivet", "-privet", "rivet", stdin=sql)


def _sqlcmd(url: str, *, q: str | None = None, sql: str | None = None) -> Proc:
    c = _container_for(url)
    if c is None:
        return _no_container(url)
    tool = "/opt/mssql-tools18/bin/sqlcmd"
    base = [tool, "-S", "localhost", "-U", "rivet", "-P", "rivet", "-C", "-d", "rivet", "-b"]
    if q is not None:
        return docker_exec(c, *base, "-Q", q)
    return docker_exec(c, *base, stdin=sql)


def _mongosh(url: str, script: str) -> Proc:
    c = _container_for(url)
    if c is None:
        return _no_container(url)
    # Rebuild the caller's URL for the IN-CONTAINER port, carrying over exactly
    # the auth and db it declares. The previous hardcoded
    # `rivet:rivet@…?authSource=admin` could never pass against the repo's own
    # compose: `mongo-rs` runs WITHOUT auth (see MONGO_RS_URL in
    # tests/common/env.rs), so every setup died on "Authentication failed" —
    # invisible for as long as the cell was never armed and always SKIPped.
    u = urllib.parse.urlsplit(url)
    auth = f"{u.username}:{u.password}@" if u.username else ""
    db = (u.path or "").lstrip("/") or "rivet"
    query = f"?{u.query}" if u.query else ""
    inner = f"mongodb://{auth}127.0.0.1:27017/{db}{query}"
    p = docker_exec(c, "mongosh", inner, "--quiet", "--eval", script)
    if p.returncode in (126, 127):  # image ships only the legacy shell (mongo:4.4)
        p = docker_exec(c, "mongo", inner, "--quiet", "--eval", script)
    return p


# ── per-engine: create+anchor the typed table / apply changes / crash-delta / clean
# `setup` returns the export's `cdc:` config block on success and None on failure.
# The bash equivalent returned the status of its LAST command (a `printf` into
# $work/cdc.block, which always succeeds), so a failed DDL sailed through and
# resurfaced later as a bogus `independent-readback[…]`. Here a setup failure is
# reported as a setup failure.
def _slot_name(prefix: str, work: Path) -> str:
    """A run-unique, valid replication-slot identifier from the work dir name.

    Derived (not persisted to `$work/slot` as the bash did) so create and drop
    read one pure function. The bash pipeline also folded the basename's trailing
    NEWLINE into a `_` via `tr -c`; the name only has to be lowercase, unique and
    identifier-safe, so that artifact is dropped.
    """
    return prefix + re.sub(r"[^a-z0-9_]", "_", work.name.lower().replace(".", "_"))


def _cdc_postgres(url: str, work: Path) -> str | None:
    slot = _slot_name("orc_cdc_", work)
    p = _psql(
        url,
        sql=(
            "DROP TABLE IF EXISTS orc_cdc_probe;\n"
            f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots WHERE slot_name='{slot}';\n"
            "CREATE TABLE orc_cdc_probe (id int PRIMARY KEY, amount numeric(18,4), meta jsonb);\n"
            "ALTER TABLE orc_cdc_probe REPLICA IDENTITY FULL;\n"
        ),
    )
    if not p.ok:
        return None
    return f"cdc: {{ slot: {slot}, until_current: true }}"


def _cdc_postgres_changes(url: str) -> None:
    _psql(
        url,
        sql=(
            "INSERT INTO orc_cdc_probe VALUES (1,12.3400,'{\"k\":1}'),(2,999999999999.9999,'{\"k\":2}'),(3,0.0001,'{\"k\":3}');\n"
            "UPDATE orc_cdc_probe SET amount=55.5500 WHERE id=2;\n"
            "DELETE FROM orc_cdc_probe WHERE id=3;\n"
        ),
    )


def _cdc_postgres_crashdelta(url: str) -> None:
    _psql(url, "-c", "INSERT INTO orc_cdc_probe VALUES (4,4.4400,'{\"k\":4}');")


def _cdc_postgres_cleanup(url: str, work: Path) -> None:
    slot = _slot_name("orc_cdc_", work)
    _psql(
        url,
        sql=(
            f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots WHERE slot_name='{slot}';\n"
            "DROP TABLE IF EXISTS orc_cdc_probe;\n"
        ),
    )


def _cdc_mysql(url: str, work: Path) -> str | None:
    p = _mysql(
        url,
        "DROP TABLE IF EXISTS orc_cdc_probe; "
        "CREATE TABLE orc_cdc_probe (id int PRIMARY KEY, amount decimal(18,4), meta json);",
    )
    if not p.ok:
        return None
    return f'cdc: {{ until_current: true, checkpoint: "{work}/cdc.ckpt", server_id: 7777 }}'


def _cdc_mysql_changes(url: str) -> None:
    _mysql(
        url,
        'INSERT INTO orc_cdc_probe VALUES (1,12.3400,\'{"k":1}\'),(2,999999999999.9999,\'{"k":2}\'),(3,0.0001,\'{"k":3}\'); '
        "UPDATE orc_cdc_probe SET amount=55.5500 WHERE id=2; "
        "DELETE FROM orc_cdc_probe WHERE id=3;",
    )


def _cdc_mysql_crashdelta(url: str) -> None:
    _mysql(url, 'INSERT INTO orc_cdc_probe VALUES (4,4.4400,\'{"k":4}\');')


def _cdc_mysql_cleanup(url: str, work: Path) -> None:
    _mysql(url, "DROP TABLE IF EXISTS orc_cdc_probe;")


def _cdc_mssql(url: str, work: Path) -> str | None:
    p = _sqlcmd(
        url,
        sql=(
            # DISABLE BEFORE DROP, and guard on the capture instance rather than
            # on a join to sys.tables.
            #
            # The original order was DROP TABLE first, then
            # `IF EXISTS (cdc.change_tables ct JOIN sys.tables t ON
            # ct.source_object_id = t.object_id WHERE t.name='orc_cdc_probe')`.
            # Dropping the table ORPHANS its change table — the row survives in
            # cdc.change_tables but `source_object_id` no longer resolves, so the
            # join finds nothing, the disable is skipped, and the next
            # `sp_cdc_enable_table` fails with 22926: "capture instance name
            # 'dbo_orc_cdc_probe' already exists". It alternated pass/fail run to
            # run, which read as a race and is not one: it is order-dependent
            # state the guard could no longer see.
            "IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_orc_cdc_probe')\n"
            "  EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='orc_cdc_probe', @capture_instance='dbo_orc_cdc_probe';\n"
            "IF OBJECT_ID('dbo.orc_cdc_probe') IS NOT NULL DROP TABLE dbo.orc_cdc_probe;\n"
            "CREATE TABLE dbo.orc_cdc_probe (id int PRIMARY KEY, amount decimal(18,4), meta nvarchar(200));\n"
            "EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orc_cdc_probe', @role_name=NULL, @capture_instance='dbo_orc_cdc_probe';\n"
        ),
    )
    if not p.ok:
        return None
    # `sp_cdc_enable_table` returning success does NOT mean the capture instance
    # is queryable: `fn_cdc_get_min_lsn` stays NULL until the capture job has run
    # once, and rivet's own preflight reads exactly that function. So the fixture
    # is not ready when the enable call returns — it is ready when the LSN is.
    #
    # Every mssql CDC cell of blessed_flow failed at `doctor` on this
    # ("fn_cdc_get_min_lsn returned NULL — the capture instance does not exist"),
    # which is the preflight being right about a fixture that had lied about
    # being up. Waiting on the enable call's exit code is waiting on the wrong
    # signal; a `time.sleep` here would be the same guess with a nicer face.
    ready = wait_until(
        lambda: "NULL"
        not in _sqlcmd(
            url,
            q="SET NOCOUNT ON; SELECT ISNULL(CONVERT(varchar(64), "
            "sys.fn_cdc_get_min_lsn('dbo_orc_cdc_probe'), 1), 'NULL')",
        ).stdout,
        tries=20,
        delay=1.5,
    )
    if not ready:
        return None
    return (
        "cdc: { capture_instance: dbo_orc_cdc_probe, until_current: true, "
        f'checkpoint: "{work}/cdc.ckpt" }}'
    )


def _mssql_max_lsn(url: str) -> str:
    """The highest LSN the CDC capture job has PROCESSED, or 'NULL' if none yet.
    Database-wide (`sys.fn_cdc_get_max_lsn`), so once the job ingests our
    just-committed transaction it strictly advances past the value read before the
    write — the precise "the capture job has caught up to my changes" signal that
    a fixed `time.sleep` only guessed at."""
    return _sqlcmd(
        url,
        q="SELECT ISNULL(CONVERT(varchar(64), sys.fn_cdc_get_max_lsn(), 1), 'NULL')",
    ).stdout.strip()


def _wait_mssql_captured(url: str, before: str) -> None:
    """Poll until the capture job advances past `before` (the max-LSN read BEFORE
    the write). Replaces a blind `time.sleep(8)`: it returns the instant the job
    has captured our changes (often ~1-3s) and only waits out the full budget when
    the Agent is genuinely slow — the same readiness-poll the fixture SETUP already
    uses (`fn_cdc_get_min_lsn`), applied to the CHANGES too."""
    wait_until(
        lambda: _mssql_max_lsn(url) not in ("NULL", before),
        tries=30,
        delay=1.0,
    )


def _cdc_mssql_changes(url: str) -> None:
    before = _mssql_max_lsn(url)
    _sqlcmd(
        url,
        sql=(
            'INSERT INTO dbo.orc_cdc_probe VALUES (1,12.3400,\'{"k":1}\'),(2,999999999999.9999,\'{"k":2}\'),(3,0.0001,\'{"k":3}\');\n'
            "UPDATE dbo.orc_cdc_probe SET amount=55.5500 WHERE id=2;\n"
            "DELETE FROM dbo.orc_cdc_probe WHERE id=3;\n"
        ),
    )
    _wait_mssql_captured(url, before)


def _cdc_mssql_crashdelta(url: str) -> None:
    before = _mssql_max_lsn(url)
    _sqlcmd(url, q='INSERT INTO dbo.orc_cdc_probe VALUES (4,4.4400,\'{"k":4}\');')
    _wait_mssql_captured(url, before)


def _cdc_mssql_cleanup(url: str, work: Path) -> None:
    _sqlcmd(url, q="IF OBJECT_ID('dbo.orc_cdc_probe') IS NOT NULL DROP TABLE dbo.orc_cdc_probe;")


def _cdc_mongo(url: str, work: Path) -> str | None:
    p = _mongosh(url, 'db.orc_cdc_probe.drop(); db.createCollection("orc_cdc_probe");')
    if not p.ok:
        return None
    return f'cdc: {{ until_current: true, checkpoint: "{work}/cdc.ckpt" }}'


def _cdc_mongo_changes(url: str) -> None:
    _mongosh(
        url,
        "db.orc_cdc_probe.insertMany([{_id:1,amount:12.34,meta:{k:1}},"
        "{_id:2,amount:999999999999.9999,meta:{k:2}},{_id:3,amount:0.0001,meta:{k:3}}]); "
        "db.orc_cdc_probe.updateOne({_id:2},{$set:{amount:55.55}}); "
        "db.orc_cdc_probe.deleteOne({_id:3});",
    )


def _cdc_mongo_crashdelta(url: str) -> None:
    _mongosh(url, "db.orc_cdc_probe.insertOne({_id:4,amount:4.44,meta:{k:4}});")


def _cdc_mongo_cleanup(url: str, work: Path) -> None:
    _mongosh(url, "db.orc_cdc_probe.drop();")


@dataclass(frozen=True)
class _Engine:
    name: str
    id_col: str  # `id` for SQL, `_id` for mongo — parquet can't COALESCE an absent column
    setup: Callable[[str, Path], str | None]
    changes: Callable[[str], None]
    crash_delta: Callable[[str], None]
    cleanup: Callable[[str, Path], None]


# Insertion order IS the gate's engine order (the bash `for eng in …` loop).
_ENGINES: dict[str, _Engine] = {
    "postgres": _Engine(
        "postgres", "id", _cdc_postgres, _cdc_postgres_changes, _cdc_postgres_crashdelta, _cdc_postgres_cleanup
    ),
    "mysql": _Engine(
        "mysql", "id", _cdc_mysql, _cdc_mysql_changes, _cdc_mysql_crashdelta, _cdc_mysql_cleanup
    ),
    "mssql": _Engine(
        "mssql", "id", _cdc_mssql, _cdc_mssql_changes, _cdc_mssql_crashdelta, _cdc_mssql_cleanup
    ),
    "mongo": _Engine(
        "mongo", "_id", _cdc_mongo, _cdc_mongo_changes, _cdc_mongo_crashdelta, _cdc_mongo_cleanup
    ),
}


# ── the object store: the INDEPENDENT readback + the CDC id-set oracle ────────
# The CDC stage only ever writes to s3 (MinIO), so the httpfs preamble is the only
# store plumbing it owns; the destination blocks and the row-count readback come
# from `scenarios`, exactly as cdc.sh took them from scenarios.sh.
_S3_PREAMBLE = (
    "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='127.0.0.1:9000'; SET s3_use_ssl=false; "
    "SET s3_url_style='path'; SET s3_access_key_id='minioadmin'; "
    "SET s3_secret_access_key='minioadmin'; "
)


def _s3_dest(bkt: str, pfx: str) -> str:
    """The MinIO `destination:` block. Total (never None), so the legs that only
    ever target s3 need no Optional dance and can never write a headless config."""
    dest = _store_dest("s3", bkt, pfx)
    if dest is None:  # a raise, not an assert: `python -O` must not turn this into "None" in a config
        raise RuntimeError("release-oracle: scenarios.store_dest has no s3 branch")
    return dest


def _duckdb(sql: str) -> str:
    """DuckDB as the INDEPENDENT reader. Empty output ⇒ no reader / no parts."""
    if not have("duckdb"):
        return ""
    return run(["duckdb", "-noheader", "-list", "-c", sql]).stdout.strip()


def _store_readback(store: str, bkt: str, pfx: str, work: Path) -> str:
    """The row count the STORE actually holds, via its own protocol + DuckDB —
    never rivet's `validate`, so a rivet READ bug cannot rubber-stamp its own
    write. Empty ⇒ the reader is absent or the prefix holds nothing.

    THIS CALL IS WHY THE PORT EXISTS. The bash `_store_readback` declared
    `local store=$1 bkt=$2 pfx=$3 dl="$WORK/dl_${store}_$RANDOM"` on ONE line, and
    macOS bash 3.2 expands a same-line `${store}` against the ENCLOSING scope
    rather than the local just assigned. The batch-load caller HAD a `store`
    local, so there it silently took the caller's value; THIS caller — the CDC
    stage — has none, so under `set -u` the whole function ABORTED and returned an
    empty count, which every engine then reported as `independent-readback[!=5]`.
    The CDC layer of the go/no-go gate could therefore never pass. In Python a
    parameter cannot resolve to a caller's variable, so the class of bug is gone
    rather than fixed-per-site (the same gotcha needed fixing three times in bash).
    """
    return scenarios.store_readback(store, bkt, pfx, work)


def _cdc_store_ids(store: str, bkt: str, pfx: str, idc: str) -> str:
    """The INDEPENDENT distinct id-set the store holds (never rivet) — the
    completeness oracle, as a bare comma-joined string so it can be quoted
    verbatim into a failure reason."""
    if store != "s3":
        return ""
    return _duckdb(
        _S3_PREAMBLE
        + f"SELECT string_agg(DISTINCT CAST({idc} AS VARCHAR),',') "
        f"FROM read_parquet('s3://{bkt}/{pfx}/**/*.parquet')"
    )


@dataclass(frozen=True)
class _Capture:
    """Where one engine's capture writes, and the config that drives it."""

    yaml: Path
    bucket: str
    prefix: str


def _cdc_write_cfg(eng: str, url: str, work: Path, store: str, cdc_block: str) -> _Capture | None:
    """Write $work/cdc.yaml pointing at $store, using the engine's `cdc:` block.

    The bash passed the bucket and prefix back through `$work/bkt` / `$work/pfx`
    files because a shell function has one return channel; here they ride back on
    the object.
    """
    bkt = _cfg("store", store, "bucket")
    pfx = f"oracle-cdc/{_work_root().name}/{eng}/{store}/{work.name}"
    dest = _store_dest(store, bkt, pfx)
    if dest is None:
        return None
    tls = "\n  tls: { accept_invalid_certs: true }" if eng == "mssql" else ""
    yaml = work / "cdc.yaml"
    yaml.write_text(
        "source:\n"
        f"  type: {eng}\n"
        f'  url: "{url}"{tls}\n'
        "exports:\n"
        "  - name: orc_cdc_probe\n"
        "    table: orc_cdc_probe\n"
        "    mode: cdc\n"
        "    format: parquet\n"
        f"    {cdc_block}\n"
        "    destination:\n"
        f"{dest}\n"
    )
    return _Capture(yaml, bkt, pfx)


def _runs_seen(export: str = "orc_cdc_probe") -> frozenset[str] | None:
    """Run ids already recorded for `export` on the Postgres store, or `None` when
    the run is going to SQLite (fresh per engine, nothing to subtract)."""
    url = os.environ.get("RIVET_STATE_URL", "").strip()
    if not url:
        return None
    c = container_for_port(port_of(url) or 5432)
    if c is None:
        return frozenset()
    return frozenset(
        ln.strip()
        for ln in docker_exec(
            c, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc",
            f"SELECT run_id FROM run_status WHERE export_name = '{export}'",
        ).stdout.splitlines()
        if ln.strip()
    )


def _state_populated(
    sdb: Path, export: str = "orc_cdc_probe", before: frozenset[str] | None = None
) -> bool:
    """Did the run populate the state metabase — every recorded run a success?

    READS THE BACKEND THE RUN ACTUALLY USED. It used to read a SQLite file at a
    fixed path unconditionally, so the moment the gate was pointed at Postgres
    (`--state-url`) every engine reported `state-not-populated`: the file it
    looked for had never been created, because the run had written to Postgres.
    Four red cells, none of them about rivet. A diagnostic that assumes the
    environment it was written in grades that environment, not the product.

    On Postgres the store is SHARED and PERMANENT, so the export name is not
    enough scope: `orc_cdc_probe` is a constant, and every past invocation's
    CRASH leg left a `running` row under it (54 success, 8 running when this was
    first measured). The check must see only THIS invocation's runs.

    The scope is the RUN ID SET taken before the run, not a timestamp. A
    timestamp looked simpler and was wrong twice over: `now()` renders
    `2026-08-03 13:45` with a space while `started_at` is RFC3339 with a `T`, and
    a space sorts BELOW `T`, so the window came out wider than everything instead
    of narrower. Isolation from the KEY, never from a clock — the same rule the
    sweeps paid for.

    On SQLite the work dir is fresh per engine, so no scope is needed.
    """
    url = os.environ.get("RIVET_STATE_URL", "").strip()
    if url:
        c = container_for_port(port_of(url) or 5432)
        if c is None:
            return False
        rows = [
            ln.split("|", 1)
            for ln in docker_exec(
                c, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc",
                f"SELECT run_id||'|'||status FROM run_status WHERE export_name = '{export}'",
            ).stdout.splitlines()
            if "|" in ln
        ]
        mine = [st for rid, st in rows if before is None or rid not in before]
        return bool(mine) and all(st == "success" for st in mine)
    # Goes through the same opener, so a WAL-mode db right after a run is READ
    # rather than mistaken for an empty one. An unreadable db returns False here
    # (the caller's `state-not-populated` wording covers it), but the read itself
    # is no longer the thing that silently fails — see `open_state_db`.
    try:
        con = open_state_db(sdb)
    except (sqlite3.Error, OSError):
        return False
    try:
        n, s = con.execute("SELECT count(*), sum(status='success') FROM run_status").fetchone()
        return bool(n) and n == s
    except sqlite3.Error:
        return False
    finally:
        con.close()


# ── the CDC end-to-end preflight (once, env-driven, SKIP-if-absent) ──────────
def setup_with_retry(spec, url: str, work: Path, *, tries: int = 3, delay: float = 3.0):
    """Run a CDC fixture's `setup`, retrying a transient failure.

    SQL Server's `sp_cdc_enable_table` can fail while the PREVIOUS capture job is
    still shutting down — six of eight mssql cells once failed exactly this way,
    and it read as a source failure when it is a fixture race. `blessed_flow` had
    absorbed it with an inline retry loop for MONTHS while `verify_cdc_e2e` called
    `setup` ONCE — so the preflight CDC[mssql] cell went RED on the same race the
    engine-matrix cell shrugged off (measured: min_lsn populated in ~5s, so the
    None came from `not p.ok` on the enable, not the readiness poll). This is the
    ONE definition both paths now share, so they cannot drift again. The other
    engines succeed on the first try, so the retry is a no-op for them."""
    blk = spec.setup(url, work)
    for _ in range(max(0, tries - 1)):
        if blk is not None:
            return blk
        time.sleep(delay)
        blk = spec.setup(url, work)
    return blk


def verify_cdc_e2e(led: Ledger) -> None:
    any_engine = False
    _export_store_creds()

    for eng, spec in _ENGINES.items():
        uvar = f"RIVET_CDC_{eng.upper()}_URL"
        url = os.environ.get(uvar, "")
        if not url:
            led.skipped(eng, "cdc", "e2e", "-", f"cdc[{eng}]: no {uvar}", "no url")
            continue
        any_engine = True
        led.phase(
            f"CDC end-to-end [{eng}] (anchor → typed changes → capture → store → "
            "independent readback + validate + state + crash)"
        )
        work = _workdir()
        idc = spec.id_col
        cdc_block = setup_with_retry(spec, url, work)
        if cdc_block is None:
            led.failed(eng, "cdc", "e2e", "-", f"cdc[{eng}]: source setup failed", "setup")
            continue
        cap = _cdc_write_cfg(eng, url, work, "s3", cdc_block)
        if cap is None:
            led.skipped(eng, "cdc", "e2e", "s3", f"cdc[{eng}]: no s3 dest", "no dest")
            continue

        # 1. clean capture: anchor (A) → changes → capture (B)
        rivet("run", "-c", str(cap.yaml))
        spec.changes(url)
        before = _runs_seen()  # the key set this cell will subtract, see _state_populated
        rivet("run", "-c", str(cap.yaml))
        n = _store_readback("s3", cap.bucket, cap.prefix, work)  # INDEPENDENT (DuckDB)
        vout = rivet("validate", "-c", str(cap.yaml)).out
        vp = "passed" in vout.lower()
        spop = _state_populated(work / ".rivet_state.db", before=before)

        # 2. crash-recovery: an extra row (id=4) + crash at cdc_after_flush_before_ack →
        #    anchor held → recovery re-reads it (at-least-once, no loss).
        spec.crash_delta(url)
        rivet("run", "-c", str(cap.yaml), env={"RIVET_TEST_PANIC_AT": CDC_HOOK_FLUSH})  # crash
        rivet("run", "-c", str(cap.yaml))  # recover
        ids = _cdc_store_ids("s3", cap.bucket, cap.prefix, idc)
        # An id-SET membership test. The bash `grep -q 4` was a substring match —
        # equivalent on this probe's {1,2,3,4}, a false pass on any wider set.
        crashok = "4" in [t.strip() for t in ids.split(",")]
        spec.cleanup(url, work)

        reasons: list[str] = []
        if (n or "0") != str(CDC_EXPECT_EVENTS):
            reasons.append(f"independent-readback[{n}!={CDC_EXPECT_EVENTS}]")
        if not vp:
            reasons.append("validate-not-PASSED")
        if not spop:
            reasons.append("state-not-populated")
        if not crashok:
            reasons.append(f"crash-lost-the-delta[ids={ids}]")
        if not reasons:
            led.passed(
                eng,
                "cdc",
                "e2e",
                "s3",
                f"cdc[{eng}]: readback={n}, validate PASSED, state populated, "
                "crash at-least-once (id=4 re-read)",
                f"{n} events",
            )
        else:
            fails = "".join(f"{r} " for r in reasons)  # trailing space, as the bash accumulated
            led.failed(eng, "cdc", "e2e", "s3", f"cdc[{eng}]: {fails}", fails)

    if not any_engine:
        return
    _cdc_large_tx_atomic(led)
    _cdc_state_parity(led)


# ── large-transaction atomicity (the committed-boundary invariant) ────────────
# A source transaction LARGER than `rollover` must roll as ONE unit — the adapter
# marks ONLY its last event `committed`, so the sink flushes+checkpoints+acks at
# the true commit boundary, never mid-transaction. A crash mid-flush therefore
# holds the anchor BEFORE the whole transaction; recovery re-reads it entire,
# losing no tail. RED-proven against the `committed:true`-on-every-event mutant
# (which rolls + acks mid-transaction, so a crash advances the anchor past the
# committed prefix and the tail is skipped on resume). PG only — the slot-anchor
# engine where the mid-transaction advance is observable (the sink code is shared,
# so one engine proves the invariant).
def _cdc_large_tx_atomic(led: Ledger) -> None:
    url = os.environ.get("RIVET_CDC_POSTGRES_URL", "")
    if not url:
        led.skipped(
            "postgres",
            "cdc",
            "large-tx-atomic",
            "-",
            "cdc large-tx-atomic: no RIVET_CDC_POSTGRES_URL",
            "no pg cdc url",
        )
        return
    _export_store_creds()
    led.phase("CDC large-transaction atomicity (a >rollover transaction survives a mid-flush crash whole)")
    work = _workdir()
    slot = _slot_name("orc_ltx_", work)
    bkt = _cfg("store", "s3", "bucket")
    pfx = f"oracle-cdc-ltx/{_work_root().name}/{work.name}"
    _psql(
        url,
        sql=(
            "DROP TABLE IF EXISTS orc_ltx;\n"
            f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots WHERE slot_name='{slot}';\n"
            "CREATE TABLE orc_ltx (id int PRIMARY KEY);\n"
            "ALTER TABLE orc_ltx REPLICA IDENTITY FULL;\n"
        ),
    )
    dest = _s3_dest(bkt, pfx)
    yaml = work / "ltx.yaml"
    yaml.write_text(
        f'source: {{ type: postgres, url: "{url}" }}\n'
        "exports:\n"
        "  - name: orc_ltx\n"
        "    table: orc_ltx\n"
        "    mode: cdc\n"
        "    format: parquet\n"
        f"    cdc: {{ slot: {slot}, until_current: true, rollover: 5 }}\n"
        "    destination:\n"
        f"{dest}\n"
    )
    rivet("run", "-c", str(yaml))  # anchor
    # ONE transaction of 12 rows — larger than rollover 5. Must NOT split.
    _psql(url, sql="BEGIN;\nINSERT INTO orc_ltx SELECT g FROM generate_series(1,12) g;\nCOMMIT;\n")
    rivet("run", "-c", str(yaml), env={"RIVET_TEST_PANIC_AT": CDC_HOOK_ACK})  # crash mid-flush
    rivet("run", "-c", str(yaml))  # recover
    ids = _cdc_store_ids("s3", bkt, pfx, "id")
    cnt = sum(1 for t in ids.split(",") if re.fullmatch(r"[0-9]+", t.strip()))
    _psql(
        url,
        "-c",
        f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots WHERE slot_name='{slot}'; "
        "DROP TABLE IF EXISTS orc_ltx;",
    )
    if cnt == 12:
        led.passed(
            "postgres",
            "cdc",
            "large-tx-atomic",
            "s3",
            "cdc large-tx-atomic: all 12 rows of the >rollover transaction survived the mid-flush crash",
            "12/12",
        )
    else:
        led.failed(
            "postgres",
            "cdc",
            "large-tx-atomic",
            "s3",
            f"cdc large-tx-atomic: only {cnt}/12 rows survived — the transaction split + lost its tail across the crash",
            f"{cnt}/12 (ids={ids})",
        )


# ── SQLite-vs-Postgres state PARITY + reference snapshot (эталонный слепок) ───
# A canonical PG CDC run against BOTH state backends; the populated table set must
# match golden/cdc_state_snapshot.json on each. RIVET_CDC_STATE_URL provides the PG
# state db; SKIP the PG leg (not the SQLite one) when unset.
def _cdc_state_parity(led: Ledger) -> None:
    url = os.environ.get("RIVET_CDC_POSTGRES_URL", "")
    if not url:
        led.skipped(
            "postgres",
            "cdc",
            "state-parity",
            "-",
            "cdc state-parity: no RIVET_CDC_POSTGRES_URL",
            "no pg cdc url",
        )
        return
    # Self-contained store creds — this leg also runs standalone under --bless-cdc,
    # not only after verify_cdc_e2e set them; without them the s3 write fails, every
    # run's status is non-success, and the blessed reference snapshot is poisoned
    # (run_status_all_success = no).
    _export_store_creds()
    led.phase("CDC state metabase parity (SQLite vs Postgres) + reference snapshot")

    w1 = _workdir()
    block1 = _cdc_postgres(url, w1)
    cap1 = _cdc_write_cfg("postgres", url, w1, "s3", block1) if block1 is not None else None
    if cap1 is None:
        # The bash had no guard here: it ran rivet against a config that was never
        # written and blamed the missing snapshot on the state backend.
        led.skipped("postgres", "cdc", "state-parity", "-", "cdc state-parity: source setup failed", "setup")
        return
    # The SQLite half must actually USE SQLite. This leg exists to compare the two
    # backends, so it cannot inherit whichever one the gate is pointed at: with
    # `--state-url` in force the run wrote to Postgres, no `.rivet_state.db` was
    # ever created, and the comparison SKIPped itself with "state db unreadable"
    # — the parity check quietly absent in exactly the configuration that makes
    # parity worth checking. An empty `RIVET_STATE_URL` pins it.
    sqlite_only = {"RIVET_STATE_URL": ""}
    rivet("run", "-c", str(cap1.yaml), env=sqlite_only)
    _cdc_postgres_changes(url)
    rivet("run", "-c", str(cap1.yaml), env=sqlite_only)
    try:
        snap_sqlite = _cdc_state_snapshot_sqlite(w1 / ".rivet_state.db")
    except (sqlite3.Error, OSError) as e:
        # An unreadable state db is a SKIP, never a divergence: reporting
        # "sqlite!=golden" here would state something about the run's state that
        # was never observed. The distinction matters because both outcomes are
        # non-green — but only one of them means rivet did something wrong.
        _cdc_postgres_cleanup(url, w1)
        led.skipped("postgres", "cdc", "state-parity", "-",
                    f"cdc state-parity: state db unreadable ({e})", "unreadable")
        return
    _cdc_postgres_cleanup(url, w1)

    golden = _golden_snapshot()
    want: dict[str, str] | None = None
    if golden.exists():
        try:
            want = json.loads(golden.read_text())
        except (OSError, ValueError):
            want = None

    surl = os.environ.get("RIVET_CDC_STATE_URL", "")
    snap_pg: dict[str, str] | None = None
    if surl:
        # The SQLite leg is fresh per $work; the Postgres state db is PERSISTENT and
        # accumulates across runs (run_aggregate/run_journal grow), so it would
        # diverge from the fresh-run golden. Reset it so the snapshot reflects a
        # SINGLE run — parity is "same table set populated", not "same accumulated
        # history".
        _cdc_pg_state_reset(surl)
        w2 = _workdir()
        block2 = _cdc_postgres(url, w2)
        cap2 = _cdc_write_cfg("postgres", url, w2, "s3", block2) if block2 is not None else None
        if cap2 is not None:
            rivet("run", "-c", str(cap2.yaml), env={"RIVET_STATE_URL": surl})
            _cdc_postgres_changes(url)
            rivet("run", "-c", str(cap2.yaml), env={"RIVET_STATE_URL": surl})
            try:
                snap_pg = _cdc_state_snapshot_pg(surl)
            except OSError as e:
                # Symmetry with the SQLite leg above: a state db the reader cannot
                # REACH says nothing about parity. Reporting `postgres!=golden`
                # here would blame rivet for the gate's own connectivity.
                _cdc_postgres_cleanup(url, w2)
                led.skipped("postgres", "cdc", "state-parity", "-",
                            f"cdc state-parity: pg state db unreadable ({e})", "unreadable")
                return
        _cdc_postgres_cleanup(url, w2)

    if os.environ.get("BLESS_CDC", "0") == "1":
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_text(json.dumps(snap_sqlite, indent=2, sort_keys=True) + "\n")
        led.passed("postgres", "cdc", "state-parity", "-", "blessed cdc state snapshot", "blessed")
        return

    reasons: list[str] = []
    if want is None:
        reasons.append("no-golden(bless first)")
    if snap_sqlite != want:
        reasons.append("sqlite!=golden")
    if surl and snap_pg != want:
        reasons.append("postgres!=golden")
    if not reasons:
        led.passed(
            "postgres",
            "cdc",
            "state-parity",
            "-",
            f"cdc state parity: SQLite{' == Postgres' if surl else ''} == golden snapshot",
            "sqlite+pg" if surl else "sqlite-only",
        )
    else:
        fails = "".join(f"{r} " for r in reasons)
        led.failed("postgres", "cdc", "state-parity", "-", f"cdc state parity: {fails}", fails)


def open_state_db(sdb: Path) -> sqlite3.Connection:
    """Open a rivet state db for READING, or raise.

    Why this is not a one-liner: rivet's SQLite state db is in WAL mode, and a
    `mode=ro` connection cannot create the `-shm` shared-memory index a WAL
    database needs. So while a `-wal` file is present — i.e. exactly in the window
    right after a run — every query through a read-only URI raises "unable to open
    database file". The read-only open is still tried FIRST, because a diagnostic
    should not be able to modify what it inspects; the writable open is the
    fallback that makes the read possible at all.

    Raising, rather than returning None, is the point. The first version of this
    swallowed the failure per table and produced an all-"empty" snapshot, which
    the caller then reported as `sqlite!=golden` — a confident statement about the
    STATE when the truth was "I could not read it". A diagnostic that renders
    "unreadable" as a data verdict is the same silent-loss shape as a decode path
    that degrades to null: the answer looks like evidence and is not.
    """
    if not sdb.is_file():
        raise FileNotFoundError(sdb)
    last: Exception | None = None
    for uri, kw in ((f"file:{sdb}?mode=ro", {"uri": True}), (str(sdb), {})):
        try:
            con = sqlite3.connect(uri, **kw)  # type: ignore[arg-type]
            con.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
            return con
        except sqlite3.Error as e:  # noqa: PERF203 — two attempts, not a loop over data
            last = e
    raise sqlite3.OperationalError(f"cannot read state db {sdb}: {last}")


def _cdc_state_snapshot_sqlite(sdb: Path) -> dict[str, str]:
    """The reference snapshot: the table→populated? map a CDC run leaves, plus the
    all-success flag.

    Raises when the db cannot be read at all — see [`open_state_db`]. A per-table
    `except` still maps a MISSING table to "empty", which is what the bash meant:
    a table absent from the schema genuinely holds nothing.
    """
    snap: dict[str, str] = {}
    con: sqlite3.Connection | None = open_state_db(sdb)
    for t in _STATE_TABLES:
        n = 0
        if con is not None:
            try:
                n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            except sqlite3.Error:
                n = 0
        snap[t] = "populated" if n > 0 else "empty"
    snap["run_status_all_success"] = "no"
    if con is not None:
        try:
            n, s = con.execute("SELECT count(*), sum(status='success') FROM run_status").fetchone()
            snap["run_status_all_success"] = "yes" if n and n == s else "no"
        except sqlite3.Error:
            pass
        con.close()
    return snap


def _db_name(url: str) -> str:
    """The database name in a connection URL (the bash `sed -E
    's#.*/([^/?]+).*#\\1#'`) — the last path segment, query string excluded."""
    return re.sub(r".*/([^/?]+).*", r"\1", url)


def _cdc_pg_state_reset(surl: str) -> None:
    """Reset the PG state db to empty so a parity snapshot reflects ONE run, not
    the accumulated history (the SQLite leg is a fresh file per run)."""
    c = _container_for(surl)
    if c is None:
        return
    tables = (
        "run_status,export_metrics,run_journal,run_aggregate,export_state,export_schema,"
        "schema_version,rivet_schema_version,cdc_snapshot,file_log,chunk_run,chunk_task,"
        "keyset_range,export_progression,export_shape,export_harm,load_run,loaded_source_run"
    )
    docker_exec(
        c, "psql", "-U", "rivet", "-d", _db_name(surl), "-q", "-c",
        f"DROP TABLE IF EXISTS {tables} CASCADE;",
    )


def _cdc_state_snapshot_pg(surl: str) -> dict[str, str]:
    """The same snapshot from the Postgres state backend, read through the psql of
    the container that PUBLISHES THE STATE URL's port (so the gate needs no client
    on the host).

    Two bugs fixed here, both of which reported rivet as broken when it was not:

    1. The container was resolved from `RIVET_CDC_POSTGRES_URL` — the CDC SOURCE.
       That only works when the state db happens to live in the source container.
       Point the state url at its own server (which is the realistic deployment,
       and what the dev stand provides) and every query ran inside the source
       container against a database that does not exist there.
    2. A failed query was swallowed: `q()` returned "" on ANY failure, `""` became
       `"0"`, and `"0"` reads as `empty` — indistinguishable from a table that is
       genuinely empty. So an unreachable state db rendered as "every table empty"
       and the cell reported `postgres!=golden`, i.e. a state-backend PARITY
       DIVERGENCE, for what was really "the reader could not connect".

    Fix (2) is the same rule the SQLite leg already follows in `_cdc_state_parity`:
    an unreadable state db is a SKIP, never a divergence — only one of those two
    outcomes means rivet did something wrong. So a query failure RAISES here and
    the caller turns it into a skip.
    """
    c = _container_for(surl)
    db = _db_name(surl)
    if c is None:
        raise OSError(f"no running container publishes the state url {surl}")

    def q(sql: str) -> str:
        p = docker_exec(c, "psql", "-U", "rivet", "-d", db, "-tAc", sql)
        if not p.ok:
            raise OSError(
                f"state db query failed in {c} (db={db}): "
                f"{(p.stderr or p.stdout).strip().splitlines()[-1] if (p.stderr or p.stdout).strip() else f'exit {p.returncode}'}"
            )
        return p.stdout.strip()

    snap: dict[str, str] = {}
    for t in _STATE_TABLES:
        n = q(f"SELECT count(*) FROM {t}") or "0"
        snap[t] = "populated" if n.isdigit() and int(n) > 0 else "empty"
    r = q("SELECT count(*)||' '||COALESCE(sum((status='success')::int),0) FROM run_status").split()
    snap["run_status_all_success"] = "yes" if len(r) == 2 and r[0] != "0" and r[0] == r[1] else "no"
    return snap
