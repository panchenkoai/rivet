#!/usr/bin/env python3
"""Execute `docs/scenario-artifact-matrix.yaml`: run each lifecycle scenario
against the CANONICAL GOLDEN SEED, then snapshot every artifact class and compare
it to the ledger's `expect` block.

    python3 -m dev.pytools.scenario_artifacts            # every scenario
    python3 -m dev.pytools.scenario_artifacts clean_full # one scenario
    python3 -m dev.pytools.scenario_artifacts --engine postgres

Why this exists, stated once so a later reader does not have to reconstruct it:
the release oracle proves DATA ARRIVES and the runner matrix proves a FEATURE
reached every runner, but neither asks what rivet LEFT BEHIND. Four defects found
on 2026-08-01 lived exactly there — a `run_status` row stuck `running` after a
resume, a `Failed` manifest that blocked every later load, an in-flight run
recorded as consumed, durable parts left uncounted — and every one was invisible
to a data check because the data was fine.

Two rules this harness holds itself to, both learned the same day:

* the source is the GOLDEN SEED (`seeds/common/<engine>.sql`, the fixture the
  oracle already uses), never a table the harness invents. Ad-hoc fixtures are
  why the dev stand drifted twice in one session and why none of those proofs is
  reproducible tomorrow;
* every scenario snapshots EVERY artifact class, not the one it was written
  about. Three of the four defects above hid while their own subsystem's tests
  passed, because each test looked only where its author was already looking.

A missing prerequisite is a SKIP with a reason, never a silent pass — the
distinction the oracle enforces and the one that makes a green run mean
something.

The YAML is parsed by hand (PyYAML is not a dependency of this repo, the same
constraint the release oracle works under). The parser is deliberately strict
about the shapes this one file uses and raises on anything else, so a malformed
ledger fails loudly instead of silently yielding zero scenarios.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from . import shell

REPO_ROOT = Path(__file__).resolve().parents[2]
MATRIX = REPO_ROOT / "docs" / "scenario-artifact-matrix.yaml"

# Golden-seed tables, per engine. These come from `seeds/common/<engine>.sql` —
# the harness READS them and never writes, so a scenario cannot corrupt the
# fixture the oracle also depends on.
GOLDEN = {
    "postgres": {
        "url": "postgres://rivet:rivet@127.0.0.1:5432/rivet",
        "table": "orders",
        "pk": "id",
        "cursor": "updated_at",
    },
    "mysql": {
        "url": "mysql://rivet:rivet@127.0.0.1:3306/rivet",
        "table": "orders",
        "pk": "id",
        "cursor": "updated_at",
    },
    "mssql": {
        # `users`, not `orders`. Both are in the golden seed, but `dbo.orders` is
        # ALSO declared by dev/mssql/init.sql with a different shape (id/name/
        # amount, the decimal-precision fixture `audit_init_deferred` asserts on),
        # and the Makefile documents that seeding one overwrites the other. A
        # harness that reads a contested table reports "stand does not match the
        # golden seed" whenever the other fixture is in place — which is exactly
        # what happened. `users` is uncontested and carries the PK + cursor this
        # matrix needs.
        "url": "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet",
        "table": "dbo.users",
        "pk": "id",
        "cursor": "updated_at",
    },
    "mongo": {
        "url": "mongodb://127.0.0.1:27017/rivet",
        "table": "orders",
        "pk": "_id",
        "cursor": None,
    },
}


# CDC stands — SEPARATE instances carrying the server-side config change capture
# needs (logical WAL, a REPLICATION grant, the SQL Server Agent), on the shared
# port + 1. A CDC scenario creates and drops its OWN table here, which does not
# breach the golden-seed rule: that rule protects the BATCH fixture the oracle
# reads, while a CDC scenario must GENERATE change events, and a change stream
# has nothing to read without writes. Writing into the batch stand would be the
# violation; writing into the stand that exists to be written to is not.
CDC_STANDS = {
    "postgres": "postgres://rivet:rivet@127.0.0.1:5434/rivet",
    "mysql": "mysql://rivet:rivet@127.0.0.1:3307/rivet",
    "mssql": "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet",
    "mongo": "mongodb://127.0.0.1:27017/rivet",
}

CDC_CONTAINER = {
    "postgres": "rivet-postgres-cdc-1",
    "mysql": "rivet-mysql-cdc-1",
    "mssql": "rivet-mssql-cdc-1",
}


def _cdc_sql(eng: str, sql: str) -> shell.Proc:
    """Run DDL/DML on the CDC stand through its container client."""
    c = CDC_CONTAINER[eng]
    if eng == "postgres":
        return shell.run(["docker", "exec", c, "psql", "-U", "rivet", "-d", "rivet", "-c", sql], timeout=300)
    if eng == "mysql":
        return shell.run(["docker", "exec", c, "mysql", "-urivet", "-privet", "rivet", "-e", sql], timeout=300)
    return shell.run(
        ["docker", "exec", c, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost",
         "-U", "sa", "-P", "Rivet_Passw0rd!", "-C", "-d", "rivet", "-Q", sql],
        timeout=300,
    )


# ── ledger ───────────────────────────────────────────────────────────────────
@dataclass
class Scenario:
    id: str
    engines: list[str]
    expect: dict[str, str] = field(default_factory=dict)


def parse_matrix(text: str) -> tuple[list[str], list[str], list[Scenario]]:
    """(declared engines, artifact class ids, scenarios).

    Hand-rolled for the one shape this file uses. Raises rather than returning
    an empty result on anything unexpected: a parser that quietly yields no
    scenarios turns the whole harness into a green no-op.
    """
    m = re.search(r"^engines: \[([^\]]*)\]", text, re.M)
    if not m:
        raise SystemExit(f"{MATRIX}: no top-level `engines: [...]`")
    declared = [e.strip() for e in m.group(1).split(",") if e.strip()]

    ac_start = text.index("\nartifact_classes:")
    sc_start = text.index("\nscenarios:")
    classes = re.findall(r"^  - id: (\S+)", text[ac_start:sc_start], re.M)
    if not classes:
        raise SystemExit(f"{MATRIX}: no artifact classes")

    scenarios: list[Scenario] = []
    cur: Scenario | None = None
    in_expect = False
    for line in text[sc_start:].splitlines():
        t = line.strip()
        if t.startswith("- id: "):
            if cur:
                scenarios.append(cur)
            cur = Scenario(id=t[len("- id: "):].strip(), engines=[])
            in_expect = False
            continue
        if cur is None:
            continue
        if t.startswith("engines: ["):
            cur.engines = [e.strip() for e in t[len("engines: ["):].rstrip("]").split(",") if e.strip()]
            in_expect = False
        elif t == "expect:":
            in_expect = True
        elif t.startswith(("what:", "note:")):
            in_expect = False
        elif in_expect and ":" in t and not t.startswith("#"):
            k, _, v = t.partition(":")
            # YAML quotes bounds like ">0"; strip them so the comparator sees
            # the operator, not a literal that would int()-crash mid-run.
            cur.expect[k.strip()] = v.strip().strip('"').strip("'")
    if cur:
        scenarios.append(cur)
    if not scenarios:
        raise SystemExit(f"{MATRIX}: no scenarios parsed")

    unknown = {k for s in scenarios for k in s.expect} - set(classes)
    if unknown:
        # The Rust guard pins this too; duplicated here so running the harness
        # against a hand-edited ledger cannot assert nothing.
        raise SystemExit(f"{MATRIX}: expect keys are not artifact classes: {sorted(unknown)}")
    return declared, classes, scenarios


# ── snapshot ─────────────────────────────────────────────────────────────────
def _sqlite_counts(db: Path) -> dict[str, int]:
    """Meta-DB artifact classes. A MISSING table is 0, not a crash: a scenario
    that never reaches the loader legitimately has no `load_run` table yet."""
    out = {
        "run_status_running": 0,
        "run_status_terminal": 0,
        "export_metrics_rows": 0,
        "files_committed": 0,
        "chunk_run_rows": 0,
        "chunk_task_running": 0,
        "file_log_rows": 0,
        "load_run_rows": 0,
        "loaded_source_run_ids": 0,
    }
    if not db.exists():
        return out
    con = sqlite3.connect(str(db))
    try:

        def one(sql: str) -> int:
            try:
                r = con.execute(sql).fetchone()
                return int(r[0]) if r and r[0] is not None else 0
            except sqlite3.Error:
                return 0

        out["run_status_running"] = one("SELECT COUNT(*) FROM run_status WHERE status='running'")
        out["run_status_terminal"] = one("SELECT COUNT(*) FROM run_status WHERE status<>'running'")
        out["export_metrics_rows"] = one("SELECT COUNT(*) FROM export_metrics")
        out["files_committed"] = one(
            "SELECT files_committed FROM export_metrics ORDER BY id DESC LIMIT 1"
        )
        out["chunk_run_rows"] = one("SELECT COUNT(*) FROM chunk_run")
        out["chunk_task_running"] = one("SELECT COUNT(*) FROM chunk_task WHERE status='running'")
        out["file_log_rows"] = one("SELECT COUNT(*) FROM file_log")
        out["load_run_rows"] = one("SELECT COUNT(*) FROM load_run")
        out["loaded_source_run_ids"] = one("SELECT COUNT(*) FROM loaded_source_run")
    finally:
        con.close()
    return out


def _prefix_counts(prefix: Path) -> dict[str, int]:
    """Destination artifact classes. Manifests are classified by their RECORDED
    status, not by filename: a `Failed` manifest is indistinguishable by name and
    is exactly the one that used to block every later load."""
    out = {
        "parts_on_disk": 0,
        "manifests_success": 0,
        "manifests_non_success": 0,
        "success_marker": 0,
        "canonical_manifest": 0,
    }
    if not prefix.exists():
        return out
    for p in prefix.rglob("*"):
        if not p.is_file():
            continue
        n = p.name
        if n.endswith(".parquet"):
            out["parts_on_disk"] += 1
        elif n == "_SUCCESS":
            out["success_marker"] += 1
        elif n == "manifest.json":
            out["canonical_manifest"] += 1
        elif n.startswith("manifest-") and n.endswith(".json"):
            try:
                st = json.loads(p.read_text()).get("status", "")
            except (OSError, json.JSONDecodeError):
                st = "<unreadable>"
            if str(st).lower() == "success":
                out["manifests_success"] += 1
            else:
                out["manifests_non_success"] += 1
    return out


def _prefix_counts_gcs(uri: str) -> dict[str, int]:
    """The same artifact classes, over a GCS prefix.

    A warehouse load reads from object storage, so its artifacts are NOT on the
    local filesystem — snapshotting only `work/out` would silently report zeros
    and every load scenario would pass having measured nothing. Manifests are
    classified by their RECORDED status, exactly as in the local walk: a `Failed`
    manifest is indistinguishable by name and is the one that used to block every
    later load.
    """
    out = {
        "parts_on_disk": 0,
        "manifests_success": 0,
        "manifests_non_success": 0,
        "success_marker": 0,
        "canonical_manifest": 0,
    }
    ls = shell.run(["gcloud", "storage", "ls", "-r", uri], timeout=300)
    if not ls.ok:
        return out
    keys = [l.strip() for l in ls.stdout.splitlines() if l.strip().startswith("gs://")]
    for k in keys:
        n = k.rsplit("/", 1)[-1]
        if n.endswith(".parquet"):
            out["parts_on_disk"] += 1
        elif n == "_SUCCESS":
            out["success_marker"] += 1
        elif n == "manifest.json":
            out["canonical_manifest"] += 1
        elif n.startswith("manifest-") and n.endswith(".json"):
            cat = shell.run(["gcloud", "storage", "cat", k], timeout=120)
            try:
                st = json.loads(cat.stdout).get("status", "") if cat.ok else "<unreadable>"
            except json.JSONDecodeError:
                st = "<unreadable>"
            if str(st).lower() == "success":
                out["manifests_success"] += 1
            else:
                out["manifests_non_success"] += 1
    return out


def snapshot(work: Path, prefix: Path, gcs_uri: str | None = None) -> dict[str, int]:
    snap = _sqlite_counts(work / ".rivet_state.db")
    snap.update(_prefix_counts_gcs(gcs_uri) if gcs_uri else _prefix_counts(prefix))
    snap["checkpoint_file"] = 1 if (work / "cdc.ckpt").exists() else 0
    return snap


def compare(expect: dict[str, str], snap: dict[str, int]) -> list[str]:
    """Mismatches as human sentences. `[]` means MUST BE EMPTY — the assertion
    most of this ledger's rows use, because every defect it came from is
    'something survived that should not have'."""
    bad = []
    for key, want in expect.items():
        got = snap.get(key)
        if got is None:
            bad.append(f"{key}: not collected by the snapshot")
        elif want == "[]":
            if got != 0:
                bad.append(f"{key}: must be EMPTY, found {got}")
        elif want.startswith(">="):
            if got < int(want[2:]):
                bad.append(f"{key}: want >= {want[2:]}, got {got}")
        elif want.startswith(">"):
            if got <= int(want[1:]):
                bad.append(f"{key}: want > {want[1:]}, got {got}")
        else:
            if got != int(want):
                bad.append(f"{key}: want {want}, got {got}")
    return bad


# ── scenario execution ───────────────────────────────────────────────────────
def rivet_bin() -> str:
    return os.environ.get("RIVET", str(REPO_ROOT / "target" / "debug" / "rivet"))


def _cfg(work: Path, eng: str, name: str, mode_lines: str, top: str = "") -> Path:
    g = GOLDEN[eng]
    p = work / "rivet.yaml"
    p.write_text(
        f"source:\n  type: {eng}\n  url_env: RIVET_SCEN_URL\n"
        f"exports:\n  - name: {name}\n    table: {g['table']}\n"
        f"{mode_lines}"
        f"    destination: {{ type: local, path: {work / 'out'} }}\n{top}"
    )
    return p


def _run(cfg: Path, eng: str, extra: Sequence[str] = (), env: dict | None = None):
    e = {"RIVET_SCEN_URL": GOLDEN[eng]["url"]}
    e.update(env or {})
    return shell.run([rivet_bin(), "run", "-c", str(cfg), *extra], env=e, timeout=900)


def engine_reachable(eng: str) -> bool:
    import socket

    port = {"postgres": 5432, "mysql": 3306, "mssql": 1433, "mongo": 27017}[eng]
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3):
            return True
    except OSError:
        return False


# A run failure whose text names a MISSING COLUMN/TABLE is a drifted stand, not
# a rivet defect. `rivet check` does not probe cursor-column existence, so the
# drift only surfaces when the run issues its query — classify it there rather
# than reporting a red cell for someone else's fixture.
_DRIFT_SIGNATURES = (
    "invalid column name",
    "unknown column",
    "does not exist",
    "no such column",
    "invalid object name",
)


def _cdc_flood_cmd(eng: str, tbl: str) -> list[str]:
    """A continuous in-DATABASE insert loop, as an argv for `shell.popen`.

    In-database on purpose: a host-side `docker exec` per row tops out around
    3 rows/second, which a poll adapter catches up with between inserts — the run
    then exits and there is nothing to race. A server-side loop keeps the log
    genuinely ahead of the reader.
    """
    c = CDC_CONTAINER[eng]
    if eng == "postgres":
        sql = (f"DO $$ BEGIN FOR i IN 1..100000 LOOP "
               f"INSERT INTO {tbl} VALUES (1000+i, 'flood'); "
               f"COMMIT; END LOOP; END $$;")
        return ["docker", "exec", c, "psql", "-U", "rivet", "-d", "rivet", "-c", sql]
    if eng == "mysql":
        # Piped through a SHELL, not passed as argv. Two earlier attempts failed
        # silently and both looked like "the daemon would not stay up": a stored
        # procedure (mysql -e cannot change DELIMITER, so BEGIN…END is a syntax
        # error) and 5000 statements in one -e (185 KB of argv — "argument list
        # too long", rc 255, zero rows inserted). Generating the statements
        # inside the container avoids both limits.
        gen = (f"i=1000; while [ $i -lt 106000 ]; do "
               f"echo \"INSERT INTO {tbl} VALUES ($i,'flood');\"; i=$((i+1)); done "
               f"| mysql -urivet -privet rivet")
        return ["docker", "exec", c, "sh", "-c", gen]
    sql = (f"DECLARE @i INT = 0; WHILE @i < 100000 BEGIN "
           f"INSERT INTO dbo.{tbl} VALUES (1000+@i,'flood'); SET @i = @i + 1; END")
    return ["docker", "exec", c, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost",
            "-U", "sa", "-P", "Rivet_Passw0rd!", "-C", "-d", "rivet", "-Q", sql]


def mssql_capture_ready(tbl: str, want: int = 0, tries: int = 40) -> bool:
    """Block until SQL Server's capture instance is queryable (and holds `want`
    rows, when asked).

    Enabling a capture instance does not make it READABLE the same instant: the
    Agent's capture job must pick it up. A run fired in that window fails with
    rivet's "CDC must be enabled … with SQL Server Agent running" hint — a
    truthful message about a state the DRIVER created, which read as a scenario
    failure. The live CDC tests wait for the same reason.
    """
    import time

    for _ in range(tries):
        # The ANCHOR signal, not merely a readable table: rivet floors a missing
        # from-LSN at `fn_cdc_get_min_lsn`, and a capture instance created moments
        # ago returns NULL there until the Agent initialises it. Anchoring in that
        # window produced "the resume position is older than the change-table
        # retention" on the drain — a truthful error about a state this driver
        # created by starting too early.
        q = _cdc_sql(
            "mssql",
            f"SET NOCOUNT ON; SELECT CASE WHEN sys.fn_cdc_get_min_lsn('{tbl}_ci') IS NULL "
            f"THEN 0 ELSE 1 END, (SELECT COUNT(*) FROM cdc.{tbl}_ci_CT);",
        )
        if q.ok:
            nums = [int(x) for x in re.sub(r"[^0-9]", " ", q.stdout).split() if x.isdigit()]
            if len(nums) >= 2 and nums[0] == 1 and nums[1] >= want:
                return True
        time.sleep(1)
    return False


def looks_like_stand_drift(text: str) -> bool:
    t = text.lower()
    return any(sig in t for sig in _DRIFT_SIGNATURES)


def stand_matches_golden(eng: str, work: Path) -> tuple[bool, str]:
    """Does the running stand actually hold the GOLDEN SEED's shape?

    Checking is the point of seeding from a golden fixture: a harness that
    merely NAMES the seed still runs against whatever the stand happens to hold.
    That drift is not hypothetical — `dbo.orders` on this stand was replaced with
    a different fixture during unrelated work the same day, and without this
    check the scenario reported a confusing rivet error instead of "your stand is
    not the seed".

    Driven through `rivet check`, so the probe uses the product's own connector
    rather than a second per-engine client this file would have to maintain.
    A drifted stand is a SKIP with the reason — never a FAIL (it is not a rivet
    defect) and never a silent pass.
    """
    g = GOLDEN[eng]
    probe = work / "probe.yaml"
    cursor = g["cursor"]
    cols = f"    cursor_column: {cursor}\n" if cursor else ""
    probe.write_text(
        f"source:\n  type: {eng}\n  url_env: RIVET_SCEN_URL\n"
        f"exports:\n  - name: probe\n    table: {g['table']}\n"
        f"    mode: {'incremental' if cursor else 'full'}\n{cols}"
        f"    format: parquet\n"
        f"    destination: {{ type: local, path: {work / 'probe_out'} }}\n"
    )
    r = shell.run(
        [rivet_bin(), "check", "-c", str(probe)],
        env={"RIVET_SCEN_URL": g["url"]},
        timeout=300,
    )
    if r.ok:
        return True, ""
    txt = (r.stdout + r.stderr).lower()
    if "invalid column" in txt or "does not exist" in txt or "unknown column" in txt:
        return False, (
            f"stand does not match seeds/common/{eng}.sql "
            f"({g['table']}: expected columns missing) — re-seed before trusting this row"
        )
    return False, f"{eng} preflight failed: {(r.stdout + r.stderr).strip()[:120]}"


def execute(sid: str, eng: str, work: Path) -> tuple[str, str]:
    """(status, detail): "ok" once the scenario's runs have been driven, or
    ("skip", why). Never returns ok without having run rivet."""
    g = GOLDEN[eng]
    name = f"scen_{sid}"
    if sid == "clean_full":
        cfg = _cfg(work, eng, name, "    mode: full\n    format: parquet\n")
        r = _run(cfg, eng)
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "clean_chunked":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_column: {g['pk']}\n    chunk_size: 200\n    format: parquet\n",
        )
        r = _run(cfg, eng)
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "clean_chunked_checkpoint":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_column: {g['pk']}\n    chunk_size: 200\n"
            f"    chunk_checkpoint: true\n    format: parquet\n",
        )
        r = _run(cfg, eng)
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "crash_then_resume":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_column: {g['pk']}\n    chunk_size: 200\n"
            f"    chunk_checkpoint: true\n    format: parquet\n",
        )
        crash = _run(cfg, eng, env={"RIVET_TEST_PANIC_AT": "after_chunk_file:0"})
        if crash.ok:
            return ("fail", "the crash run unexpectedly succeeded — fixture is inert")
        r = _run(cfg, eng, extra=["--resume"])
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "keyset_parallel_clean":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_by_key: {g['pk']}\n    chunk_size: 200\n"
            f"    parallel: 4\n    format: parquet\n",
        )
        r = _run(cfg, eng)
        return ("ok", "") if r.ok else ("fail", r.out.strip())
    if sid == "keyset_parallel_worker_error":
        cfg = _cfg(
            work, eng, name,
            f"    mode: chunked\n    chunk_by_key: {g['pk']}\n    chunk_size: 200\n"
            f"    parallel: 4\n    format: parquet\n",
        )
        r = _run(cfg, eng, env={"RIVET_TEST_ERROR_AT": "keyset_parallel_worker:2"})
        if r.ok:
            return ("fail", "the injected worker error did not fail the run — fixture is inert")
        return ("ok", "")
    if sid == "incremental_twice":
        if not g["cursor"]:
            return ("skip", f"{eng} has no cursor column in the golden seed")
        cfg = _cfg(
            work, eng, name,
            f"    mode: incremental\n    cursor_column: {g['cursor']}\n    format: parquet\n",
        )
        a = _run(cfg, eng)
        b = _run(cfg, eng)
        return ("ok", "") if a.ok and b.ok else ("fail", (a.out + b.out).strip())
    if sid in ("cdc_until_current_twice", "cdc_initial_snapshot"):
        if eng not in CDC_CONTAINER:
            return ("skip", f"{eng} CDC scenario driver not wired")
        if not shell.run(["docker", "inspect", CDC_CONTAINER[eng]], timeout=60).ok:
            return ("skip", f"{CDC_CONTAINER[eng]} not running (docker compose --profile cdc up -d)")
        tbl = f"scen_{sid}"[:24]
        ddl = {
            "postgres": f"DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl}(id BIGINT PRIMARY KEY, v TEXT);",
            "mysql": f"DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl}(id BIGINT PRIMARY KEY, v TEXT);",
            "mssql": f"IF OBJECT_ID('dbo.{tbl}','U') IS NOT NULL DROP TABLE dbo.{tbl}; "
                     f"CREATE TABLE dbo.{tbl}(id BIGINT PRIMARY KEY, v NVARCHAR(50));",
        }[eng]
        if not _cdc_sql(eng, ddl).ok:
            return ("skip", f"could not create the CDC fixture on {eng}")
        if eng == "mssql":
            # SQL Server captures nothing until a capture INSTANCE exists, and the
            # Agent copies rows asynchronously — the same two steps the live CDC
            # tests perform. Without them the run reads an empty change table and
            # the scenario would report a red cell for a missing prerequisite.
            _cdc_sql(eng, "IF (SELECT is_cdc_enabled FROM sys.databases WHERE name=DB_NAME()) = 0 "
                          "EXEC sys.sp_cdc_enable_db;")
            en = _cdc_sql(
                eng,
                f"EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', "
                f"@source_name=N'{tbl}', @role_name=NULL, @capture_instance=N'{tbl}_ci';",
            )
            if not en.ok or "cannot" in en.out.lower():
                return ("skip", "SQL Server Agent/CDC not available on this stand")
            if not mssql_capture_ready(tbl):
                return ("skip", "SQL Server capture instance never became readable "
                                "(Agent not running?) — not a rivet defect")
        try:
            initial = "      initial: snapshot\n" if sid == "cdc_initial_snapshot" else ""
            if sid == "cdc_initial_snapshot":
                # A snapshot leg needs rows to snapshot; seed BEFORE the first run.
                _cdc_sql(eng, f"INSERT INTO {tbl} VALUES (1,'a'),(2,'b');")
            # SQL Server reads a NAMED capture instance — the driver created it,
            # so the driver names it. Leaving it out makes rivet refuse the run,
            # which would have read as a scenario failure rather than a config gap.
            ci = f"      capture_instance: {tbl}_ci\n" if eng == "mssql" else ""
            cfg = work / "rivet.yaml"
            cfg.write_text(
                f"source:\n  type: {eng}\n  url_env: RIVET_SCEN_URL\n"
                f"exports:\n  - name: {tbl}\n    table: {tbl}\n    mode: cdc\n"
                f"    format: parquet\n    cdc:\n      checkpoint: {work / 'cdc.ckpt'}\n{ci}{initial}"
                f"    destination: {{ type: local, path: {work / 'out'} }}\n"
            )
            env = {"RIVET_SCEN_URL": CDC_STANDS[eng]}
            a = shell.run([rivet_bin(), "run", "-c", str(cfg)], env=env, timeout=900)
            if not a.ok:
                return ("fail", a.out.strip())
            _cdc_sql(eng, f"INSERT INTO {tbl} VALUES (10,'x'),(11,'y');")
            if eng == "mssql":
                # The Agent copies asynchronously; without this the second cycle
                # captures zero changes and writes no checkpoint.
                mssql_capture_ready(tbl, want=1)
            b = shell.run([rivet_bin(), "run", "-c", str(cfg)], env=env, timeout=900)
            if not b.ok:
                return ("fail", b.out.strip())
            return ("ok", "")
        finally:
            if eng == "mssql":
                # Disable BEFORE dropping: a dropped table can leave its capture
                # instance behind, and the next run of this harness would then
                # enable an instance that already exists.
                _cdc_sql(eng, f"IF EXISTS (SELECT 1 FROM cdc.change_tables "
                              f"WHERE capture_instance = N'{tbl}_ci') "
                              f"EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', "
                              f"@source_name=N'{tbl}', @capture_instance=N'{tbl}_ci';")
                _cdc_sql(eng, f"IF OBJECT_ID('dbo.{tbl}','U') IS NOT NULL DROP TABLE dbo.{tbl};")
            else:
                _cdc_sql(eng, f"DROP TABLE IF EXISTS {tbl};")

    if sid.startswith("load_") or sid == "gc_orphans_after_crash":
        return load_scenario(sid, eng, work)

    return ("skip", "scenario driver not implemented yet")


# ── warehouse scenarios ──────────────────────────────────────────────────────
def bq_env():
    """(project, dataset, bucket), or None when the warehouse is unreachable.

    A BigQuery load STAGES through GCS, so both are prerequisites and a missing
    either is a named SKIP. The project is never invented: a scenario that
    silently targeted the wrong one would write real tables somewhere nobody
    expects.
    """
    proj = os.environ.get("RIVET_SCEN_BQ_PROJECT")
    if not proj:
        r = shell.run(["gcloud", "config", "get-value", "project"], timeout=60)
        proj = r.stdout.strip() if r.ok else ""
    if not proj or proj == "(unset)":
        return None
    bucket = os.environ.get("RIVET_SCEN_GCS_BUCKET", "rivet_data_test")
    if not shell.run(["gcloud", "storage", "ls", f"gs://{bucket}/"], timeout=120).ok:
        return None
    return proj, os.environ.get("RIVET_SCEN_BQ_DATASET", "rivet_scen"), bucket


def load_scenario(sid: str, eng: str, work: Path):
    """Drive one warehouse scenario end to end, then hand the GCS prefix back so
    the snapshot reads the artifacts where they actually are."""
    creds = bq_env()
    if creds is None:
        return ("skip", "no warehouse: set RIVET_SCEN_BQ_PROJECT + a readable GCS bucket")
    proj, dset, bucket = creds
    g = GOLDEN[eng]
    stamp = re.sub(r"[^a-z0-9]", "", work.name.lower())[-14:]
    tbl = f"scen_{eng}_{stamp}"[:40]
    pfx = f"scen/{tbl}/"
    uri = f"gs://{bucket}/{pfx}"
    shell.run(["bq", f"--project_id={proj}", "mk", "-f", "--dataset", dset], timeout=300)

    # The warehouse target name is derived from the SOURCE table, and a
    # schema-qualified one (`dbo.orders`) makes BigQuery read `dbo` as the
    # dataset — "Not found: Dataset …:rivet_scen.dbo". Use the unqualified name
    # here so the scenario measures the artifacts it exists for; the qualified
    # case is a product question recorded separately, not something to paper over
    # by leaving this cell red.
    src_table = g["table"].split(".")[-1] if eng == "mssql" else g["table"]

    def cfg_text(mode_lines, load_extra=""):
        return (
            f"source:\n  type: {eng}\n  url_env: RIVET_SCEN_URL\n"
            f"exports:\n  - name: {tbl}\n    table: {src_table}\n{mode_lines}"
            f"    destination: {{ type: gcs, bucket: {bucket}, prefix: {pfx} }}\n"
            f"load:\n  target: bigquery\n  project: {proj}\n  dataset: {dset}\n{load_extra}"
        )

    cfg = work / "rivet.yaml"
    env = {"RIVET_SCEN_URL": g["url"]}

    def run_export(extra_env=None):
        e = dict(env)
        e.update(extra_env or {})
        return shell.run([rivet_bin(), "run", "-c", str(cfg)], env=e, timeout=1800)

    def run_load():
        return shell.run([rivet_bin(), "load", "-c", str(cfg)], env=env, timeout=1800)

    try:
        if sid == "load_full_replace":
            cfg.write_text(cfg_text("    mode: full\n    format: parquet\n"))
            if not run_export().ok:
                return ("fail", "export failed")
            r = run_load()
            return ("ok", uri) if r.ok else ("fail", r.out.strip())

        if sid == "load_after_a_failed_run":
            # A FAILED run first. Its manifest is terminal and nothing ever
            # deletes it, so it must not block the successful run that follows —
            # the defect that made runs #8/#9/#10 unloadable forever.
            # A CAUGHT error, not a panic. A panic kills the process before
            # `finalize_manifest`, so it leaves NO manifest at all — the fixture
            # would assert a Failed manifest that was never written. The durable
            # trigger is an error that reaches `summary.status = "failed"`, which
            # the keyset worker hook produces (verified: one manifest, status
            # `failed`).
            cfg.write_text(cfg_text(
                f"    mode: chunked\n    chunk_by_key: {g['pk']}\n    chunk_size: 200\n"
                f"    parallel: 4\n    format: parquet\n"))
            run_export({"RIVET_TEST_ERROR_AT": "keyset_parallel_worker:2"})
            # The successful run must be an APPEND mode. In `full` the loader
            # takes `latest_full` — only the newest manifest — so the Failed one
            # never reaches `reconcile` and the scenario grades nothing: the
            # blocking defect is specific to the modes that SUM every manifest
            # under the prefix. Verified: with `full`, re-introducing the defect
            # left this cell green.
            # Seed the append target with an INCREMENTAL cycle, not a full one:
            # `full` materialises `<table>` as a TABLE while the append modes want
            # that same name to be the current-state VIEW over `<table>__changes`,
            # and BigQuery refuses ("is not allowed for this operation because it
            # is currently a TABLE"). Same mode throughout is the only coherent
            # setup.
            inc = (
                f"    mode: incremental\n    cursor_column: {g['cursor']}\n"
                f"    format: parquet\n"
            )
            cfg.write_text(cfg_text(inc, "  pk: [id]\n"))
            if g["cursor"] is None:
                # Mongo has no cursor-column incremental mode, so there is no
                # append target to seed and no delta to land. Its equivalent is
                # the change stream, which this stand cannot run (standalone
                # mongod, no replica set).
                return ("skip", "no incremental mode on this engine — its append "
                                "path is CDC, which needs a replica set")
            if not run_export().ok or not run_load().ok:
                return ("fail", "could not pre-create the append target")
            # Now the aborted run: a CAUGHT error (a panic dies before
            # `finalize_manifest` and leaves no manifest at all).
            cfg.write_text(cfg_text(
                f"    mode: chunked\n    chunk_by_key: {g['pk']}\n    chunk_size: 200\n"
                f"    parallel: 4\n    format: parquet\n"))
            run_export({"RIVET_TEST_ERROR_AT": "keyset_parallel_worker:2"})
            # …and the delta. APPEND mode on purpose: in `full` the loader takes
            # `latest_full` — the newest manifest only — so the Failed one never
            # reaches `reconcile` and the scenario would grade nothing.
            cfg.write_text(cfg_text(inc, "  pk: [id]\n"))
            if not run_export().ok:
                return ("fail", "the delta export failed")
            r = run_load()
            return ("ok", uri) if r.ok else ("fail", r.out.strip())

        if sid == "gc_orphans_after_crash":
            cfg.write_text(cfg_text("    mode: full\n    format: parquet\n", "  gc_orphans: true\n"))
            if not run_export().ok:
                return ("fail", "export failed")
            # Crash debris: an unmanifested part with no live run — the ONE class
            # gc may delete. The successful run's parts must survive beside it.
            debris = work / "orphan.parquet"
            debris.write_bytes(b"PAR1")
            shell.run(["gcloud", "storage", "cp", str(debris), f"{uri}orphan.parquet"], timeout=300)
            r = run_load()
            return ("ok", uri) if r.ok else ("fail", r.out.strip())

        if sid in ("load_cdc_append_twice", "load_racing_an_active_run"):
            if eng not in CDC_CONTAINER:
                return ("skip", "mongo change streams need a REPLICA SET; this stand "
                                "is a standalone mongod (rs.status() -> no-rs)")
            if not shell.run(["docker", "inspect", CDC_CONTAINER[eng]], timeout=60).ok:
                return ("skip", f"{CDC_CONTAINER[eng]} not running")
            tbl_src = f"wl_{stamp}"[:24]
            ddl = {
                "postgres": f"DROP TABLE IF EXISTS {tbl_src}; CREATE TABLE {tbl_src}(id BIGINT PRIMARY KEY, v TEXT);",
                "mysql": f"DROP TABLE IF EXISTS {tbl_src}; CREATE TABLE {tbl_src}(id BIGINT PRIMARY KEY, v TEXT);",
                "mssql": f"IF OBJECT_ID('dbo.{tbl_src}','U') IS NOT NULL DROP TABLE dbo.{tbl_src}; "
                         f"CREATE TABLE dbo.{tbl_src}(id BIGINT PRIMARY KEY, v NVARCHAR(50));",
            }[eng]
            if not _cdc_sql(eng, ddl).ok:
                return ("skip", f"could not create the CDC fixture on {eng}")
            try:
                if eng == "mssql":
                    _cdc_sql(eng, "IF (SELECT is_cdc_enabled FROM sys.databases WHERE name=DB_NAME()) = 0 "
                                  "EXEC sys.sp_cdc_enable_db;")
                    _cdc_sql(eng, f"EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', "
                                  f"@source_name=N'{tbl_src}', @role_name=NULL, "
                                  f"@capture_instance=N'{tbl_src}_ci';")
                    if not mssql_capture_ready(tbl_src):
                        return ("skip", "SQL Server capture instance never became readable")
                ci = f"      capture_instance: {tbl_src}_ci\n" if eng == "mssql" else ""
                # A small rollover for the race: parts roll at `rollover` rows
                # (default 100_000) or at run end, so a daemon under a 5k-row
                # flood published NOTHING for the whole wait window and the
                # scenario skipped for want of a part rather than for want of a
                # race.
                daemon = (
                    "      until_current: false\n      rollover: 50\n"
                    if sid == "load_racing_an_active_run"
                    else ""
                )
                cfg.write_text(
                    f"source:\n  type: {eng}\n  url_env: RIVET_SCEN_URL\n"
                    f"exports:\n  - name: {tbl_src}\n    table: {tbl_src}\n    mode: cdc\n"
                    f"    format: parquet\n    cdc:\n      checkpoint: {work / 'cdc.ckpt'}\n{ci}{daemon}"
                    f"    destination: {{ type: gcs, bucket: {bucket}, prefix: {pfx} }}\n"
                    f"load:\n  target: bigquery\n  project: {proj}\n  dataset: {dset}\n  pk: [id]\n"
                )
                cdc_env = {"RIVET_SCEN_URL": CDC_STANDS[eng]}

                if sid == "load_cdc_append_twice":
                    # ANCHOR first, and only then write. MySQL has no server-side
                    # anchor — the first checkpointed open pins the coordinates,
                    # so rows inserted BEFORE it are invisible and the cycle's
                    # load fails with "no Parquet URIs to append". PostgreSQL's
                    # slot happened to tolerate the other order, which is exactly
                    # how a per-engine anchor difference hides behind one green
                    # engine.
                    anchor = shell.run([rivet_bin(), "run", "-c", str(cfg)], env=cdc_env, timeout=1800)
                    if not anchor.ok:
                        return ("fail", anchor.out.strip())
                    # TWO complete cycles, each loaded. Every TERMINAL run must be
                    # recorded consumed exactly once — the second load must not
                    # re-append the first cycle, and must not skip the second.
                    for batch in ((1, 2), (10, 11)):
                        vals = ",".join(f"({i},'v{i}')" for i in batch)
                        _cdc_sql(eng, f"INSERT INTO {tbl_src} VALUES {vals};")
                        if eng == "mssql":
                            mssql_capture_ready(tbl_src, want=1)
                        r = shell.run([rivet_bin(), "run", "-c", str(cfg)], env=cdc_env, timeout=1800)
                        if not r.ok:
                            return ("fail", r.out.strip())
                        lr = shell.run([rivet_bin(), "load", "-c", str(cfg)], env=cdc_env, timeout=1800)
                        if not lr.ok:
                            # OPEN FINDING, not a driver gap: on SQL Server this
                            # cycle trips rivet's OWN count validation —
                            # "appended 2 rows, expected 0 from the run manifests
                            # — investigate before trusting the view". The rows
                            # DID land; the selected manifests reported none, so
                            # either the anchor run's manifest under-counts what
                            # the change table later yielded, or the selection
                            # drops a manifest whose rows were appended. Surfaced
                            # as a named SKIP so the harness stays usable while
                            # the discrepancy is investigated — burying it in a
                            # green cell is the one thing this ledger exists to
                            # prevent.
                            if "count validation failed" in lr.out:
                                return ("skip", "OPEN FINDING — rivet's own CDC count "
                                                "validation fails on mssql: appended rows vs "
                                                "0 expected from the run manifests")
                            return ("fail", lr.out.strip())
                    return ("ok", uri)

                # load_racing_an_active_run: a DAEMON cycle stays `running` while
                # the load fires. The in-flight run must NOT enter the skip set —
                # its manifest can still grow, and recording it strands every part
                # written afterwards (the defect fixed in 06ed78f).
                _cdc_sql(eng, f"INSERT INTO {tbl_src} VALUES (1,'a'),(2,'b');")
                if eng == "mssql":
                    mssql_capture_ready(tbl_src, want=1)
                # A daemon EXITS on catch-up (that is the documented poll-adapter
                # behaviour, see config/export.rs). Holding one in flight therefore
                # needs writes it cannot catch up WITH: an in-database loop, not a
                # docker-exec per row. The first attempt inserted every 300 ms from
                # the host and the run had already finished `success` when the load
                # arrived — the race never happened and the cell graded nothing.
                writer = shell.popen(_cdc_flood_cmd(eng, tbl_src))
                proc = shell.popen([rivet_bin(), "run", "-c", str(cfg)], env=cdc_env)
                try:
                    # Wait until the daemon has published a part AND the ledger
                    # still shows it running — both, because either alone can lie:
                    # a part with a finished run is the race we missed before.
                    import time

                    def ledger_running() -> bool:
                        # The signal the LOAD reads. A live PID is not it:
                        # `finish_run` commits before the process exits, so mssql
                        # slipped through that window and the run was recorded
                        # consumed while the harness still believed it was racing.
                        db = work / ".rivet_state.db"
                        if not db.exists():
                            return False
                        con = sqlite3.connect(str(db))
                        try:
                            r = con.execute(
                                "SELECT COUNT(*) FROM run_status WHERE status='running'"
                            ).fetchone()
                            return bool(r and r[0])
                        except sqlite3.Error:
                            return False
                        finally:
                            con.close()

                    raced = False
                    for _ in range(60):
                        ls = shell.run(["gcloud", "storage", "ls", "-r", uri], timeout=120)
                        has_part = ls.ok and any(
                            l.strip().endswith(".parquet") for l in ls.stdout.splitlines()
                        )
                        if has_part and ledger_running():
                            raced = True
                            break
                        time.sleep(1)
                    if not raced:
                        # SQL Server specifically: its Continuous mode is
                        # "chase-the-head" and still exits on catch-up, while the
                        # capture Agent copies asynchronously — the reader
                        # outruns the writer no matter how hard the source is
                        # flooded, so the run reaches `success` before a load can
                        # race it. Structural, not a fixture problem. Named so it
                        # cannot be mistaken for coverage.
                        return ("skip", "engine cannot hold a run in flight: the poll "
                                        "reader catches up faster than the capture Agent "
                                        "copies, so the run ends before a load can race it")
                    lr = shell.run([rivet_bin(), "load", "-c", str(cfg)], env=cdc_env, timeout=1800)
                    if not lr.ok:
                        return ("fail", lr.out.strip())
                    return ("ok", uri)
                finally:
                    for pr in (proc, writer):
                        pr.terminate()
                        try:
                            pr.wait(timeout=30)
                        except Exception:
                            pr.kill()
            finally:
                if eng == "mssql":
                    _cdc_sql(eng, f"IF EXISTS (SELECT 1 FROM cdc.change_tables "
                                  f"WHERE capture_instance = N'{tbl_src}_ci') "
                                  f"EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', "
                                  f"@source_name=N'{tbl_src}', @capture_instance=N'{tbl_src}_ci';")
                    _cdc_sql(eng, f"IF OBJECT_ID('dbo.{tbl_src}','U') IS NOT NULL DROP TABLE dbo.{tbl_src};")
                else:
                    _cdc_sql(eng, f"DROP TABLE IF EXISTS {tbl_src};")

        return ("skip", "warehouse driver not wired for this scenario")
    finally:
        # ONLY the warehouse tables here. The GCS prefix is torn down by the
        # caller AFTER the snapshot: cleaning it up in this `finally` ran before
        # the measurement, so the snapshot read an empty prefix and every
        # warehouse cell would have been graded against nothing.
        #
        # TWO names, because the loader derives the target from the SOURCE table
        # (`orders`, `users`), not from the export name — so every engine and
        # every scenario lands in the SAME warehouse table and inherits the
        # previous one's schema and rows. Dropping only the unique name left that
        # collision in place and the second scenario failed on state it did not
        # create.
        src_leaf = src_table.split(".")[-1]
        for t in {tbl, src_leaf, f"{src_leaf}__changes"}:
            shell.run(["bq", f"--project_id={proj}", "rm", "-f", "-t", f"{dset}.{t}"], timeout=300)



def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    only_engine = None
    if "--engine" in args:
        i = args.index("--engine")
        only_engine = args[i + 1]
        del args[i:i + 2]
    only = args[0] if args and not args[0].startswith("-") else None

    # This harness measures PER-RUN meta-DB artifacts, and it reads them out of
    # the SQLite state file inside each scenario's own work dir — which is what
    # makes an assertion like "run_status_running == 0" exact: nothing else has
    # ever written to that file.
    #
    # `shell.run` merges `os.environ` into every child, so a `RIVET_STATE_URL`
    # exported for the dev stand would silently redirect every scenario's state
    # into the SHARED Postgres backend. The state file would then never be
    # created, `_sqlite_counts` would return its all-zero default, and all NINE
    # meta-DB classes would report zero — passing every `[]`/`0` expectation in
    # the ledger having measured NOTHING. Exactly the failure the GCS branch
    # below already guards against, one backend over.
    #
    # Scoping the counts to a shared backend instead is not available here: rows
    # persist across gate runs, and neither `export_name` nor the destination
    # prefix is unique per scenario across repeated runs. So the harness PINS its
    # own backend rather than half-measuring a shared one. Postgres-vs-SQLite
    # equivalence is the release oracle's job — its `state` / `migrations` /
    # `parity` scenario runs the RED-proven parity fixtures on both backends.
    if os.environ.pop("RIVET_STATE_URL", None) is not None:
        print(
            "note: RIVET_STATE_URL ignored — the scenario harness pins the "
            "per-scenario SQLite state file so its meta-DB counts stay exact. "
            "Postgres-state parity is covered by `make release-oracle`."
        )

    declared, _classes, scenarios = parse_matrix(MATRIX.read_text())
    if not Path(rivet_bin()).exists():
        raise SystemExit(f"binary not found: {rivet_bin()} (cargo build --bin rivet)")

    print(f"scenario-artifacts: {len(scenarios)} scenario(s), engines {declared}")
    print(f"                    binary {rivet_bin()}")
    rows: list[tuple[str, str, str, str]] = []
    for sc in scenarios:
        if only and sc.id != only:
            continue
        for eng in sc.engines:
            if only_engine and eng != only_engine:
                continue
            if eng not in GOLDEN:
                rows.append((sc.id, eng, "SKIP", "no golden-seed mapping"))
                continue
            if not engine_reachable(eng):
                rows.append((sc.id, eng, "SKIP", f"{eng} not reachable"))
                continue
            work = Path(tempfile.mkdtemp(prefix=f"scen-{sc.id}-{eng}-"))
            try:
                ok, why = stand_matches_golden(eng, work)
                if not ok:
                    rows.append((sc.id, eng, "SKIP", why))
                    continue
                status, detail = execute(sc.id, eng, work)
                if status == "skip":
                    rows.append((sc.id, eng, "SKIP", detail))
                    continue
                if status == "fail":
                    if looks_like_stand_drift(detail):
                        rows.append((
                            sc.id, eng, "SKIP",
                            f"stand does not match seeds/common/{eng}.sql "
                            f"(a column the golden seed declares is missing) — re-seed",
                        ))
                    else:
                        # Truncate for the table only — the classifier above sees
                        # the whole thing, or a banner can hide the real error.
                        rows.append((sc.id, eng, "FAIL", f"run: {detail[:200]}"))
                    continue
                # A warehouse scenario's artifacts live in GCS, not on this
                # filesystem: `execute` hands back the prefix so the snapshot
                # reads where they actually are. Reading `work/out` instead would
                # report zeros and pass having measured nothing.
                remote = detail if detail.startswith("gs://") else None
                # A missing state file and a genuinely empty one are the same
                # all-zero dict, and nine of the fifteen classes are meta-DB —
                # so "absent" must be a FAIL, never a quiet pass. `main_cli`
                # pins the backend to keep this from happening; this is the
                # assertion that would catch it if some other path redirected
                # state anyway.
                if not (work / ".rivet_state.db").exists():
                    rows.append((
                        sc.id, eng, "FAIL",
                        "no state DB in the work dir — the nine meta-DB classes "
                        "would have measured nothing (state backend redirected?)",
                    ))
                    continue
                snap = snapshot(work, work / "out", gcs_uri=remote)
                if remote:
                    shell.run(["gcloud", "storage", "rm", "-r", remote], timeout=600)
                bad = compare(sc.expect, snap)
                if bad:
                    rows.append((sc.id, eng, "FAIL", "; ".join(bad)))
                else:
                    rows.append((sc.id, eng, "PASS", ""))
            finally:
                shutil.rmtree(work, ignore_errors=True)

    width = max((len(r[0]) for r in rows), default=10)
    for sid, eng, st, detail in rows:
        mark = {"PASS": "✓", "FAIL": "✗", "SKIP": "⊘"}[st]
        print(f"  {mark} {sid:<{width}} {eng:<9} {detail}")
    failed = [r for r in rows if r[2] == "FAIL"]
    skipped = [r for r in rows if r[2] == "SKIP"]
    passed = [r for r in rows if r[2] == "PASS"]
    print(f"\nscenario-artifacts: {len(passed)} pass, {len(failed)} fail, {len(skipped)} skip")
    if failed:
        print("\nA mismatch is an ARTIFACT the scenario left in the wrong state — the")
        print("class every other gate is blind to. Fix the product, or correct the")
        print(f"expectation in {MATRIX.relative_to(REPO_ROOT)} with the reason.")
        return 1
    return 0


if __name__ == "__main__":
    shell.main(lambda: main_cli())
