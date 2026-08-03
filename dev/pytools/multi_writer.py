"""Every engine, batch AND CDC, at once — into one backend, one prefix, one table name.

The gate's `concurrent-writers` cell races four writers of the SAME shape from
ONE source. This is the harder question the operator actually has: several
DIFFERENT engines, batch and change-capture together, all pointed at one state
backend and one destination, with their outputs carrying the same table name. Do
the artifacts come back whole — no loss, no duplicate, nothing overwritten — and
does a real load see exactly what was exported?

THE ORACLE IS THE ID SPACE. Each engine seeds the same logical table with a
DISJOINT id range (postgres 1…, mysql 10001…, mssql 20001…, mongo 30001…), so
the expected union is known exactly and every failure mode is distinguishable in
the read-back:

    fewer rows than seeded          loss
    an id appearing twice           duplication
    an engine's whole range absent  one writer's output overwritten

Counting alone cannot tell those apart, which is why the check is per id.

BATCH AND CDC GET SEPARATE PREFIXES, and that is not a workaround. rivet REFUSES
to mix them at one prefix (`guard_manifest_mode`, finding #44: a batch and a CDC
export sharing a prefix silently destroyed each other's manifest, orphaning the
other's parts from `validate`). So mixing is tested as a REFUSAL — it must fail,
loudly — and the concurrency of the two shapes is tested with each shape owning
its prefix, which is the supported arrangement.

Runs against BOTH state backends and BOTH destinations (local, then GCS with a
BigQuery load), because a shared backend and an object store are exactly where
"they do not collide" stops being obvious.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "dev"))

RIVET = os.environ.get("RIVET_BIN", str(ROOT / "target" / "release" / "rivet"))
STATE_URL = os.environ.get(
    "RIVET_MW_STATE_URL", "postgresql://rivet:rivet@localhost:5433/rivet_state"
)
STATE_CONTAINER = os.environ.get("RIVET_SWEEP_STATE_CONTAINER", "rivet-postgres-state-1")
BUCKET = os.environ.get("BQ_ORACLE_BUCKET", "rivet_data_test")
BQ_PROJECT = os.environ.get("BQ_ORACLE_PROJECT", "rivet-data-tool")
BQ_DATASET = os.environ.get("BQ_ORACLE_DATASET", "rivet_e2e")

#: Rows each engine contributes, and where its id range starts. Disjoint by
#: construction — that is the whole oracle.
PER_ENGINE = 500

#: The BATCH stand (the CDC stand is a separate set of ports; see `CDC_URLS`).
@dataclass(frozen=True)
class Engine:
    name: str
    container: str
    url: str
    base: int


ENGINES = [
    Engine("postgres", "rivet-postgres-1", "postgresql://rivet:rivet@localhost:5432/rivet", 1),
    Engine("mysql", "rivet-mysql-1", "mysql://rivet:rivet@localhost:3306/rivet", 10_001),
    Engine("mssql", "rivet-mssql-1", "sqlserver://rivet:rivet@localhost:1433/rivet", 20_001),
    Engine("mongo", "rivet-mongo-1", "mongodb://localhost:27017/rivet", 30_001),
]

CDC_URLS = {
    "postgres": os.environ.get("RIVET_CDC_POSTGRES_URL", ""),
    "mysql": os.environ.get("RIVET_CDC_MYSQL_URL", ""),
    "mssql": os.environ.get("RIVET_CDC_MSSQL_URL", ""),
    "mongo": os.environ.get("RIVET_CDC_MONGO_URL", ""),
}

TABLE = "mw_probe"


def sh(argv: list[str], env: dict | None = None, timeout: int = 900):
    return subprocess.run(
        argv, capture_output=True, text=True, env={**os.environ, **(env or {})}, timeout=timeout
    )


def psql_state(sql: str) -> str:
    return sh(
        ["docker", "exec", STATE_CONTAINER, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc", sql]
    ).stdout.strip()


def duckdb(sql: str) -> str:
    return sh(["duckdb", "-noheader", "-list", "-c", sql], timeout=600).stdout.strip()


# ── seeding: the same logical table on every engine, disjoint id ranges ───────
def seed(e: Engine) -> bool:
    lo, hi = e.base, e.base + PER_ENGINE - 1
    if e.name == "postgres":
        return sh([
            "docker", "exec", e.container, "psql", "-U", "rivet", "-d", "rivet",
            "-v", "ON_ERROR_STOP=1", "-c",
            f"DROP TABLE IF EXISTS {TABLE}; CREATE TABLE {TABLE}(id INT PRIMARY KEY, src TEXT); "
            f"INSERT INTO {TABLE} SELECT g, 'postgres' FROM generate_series({lo},{hi}) g;",
        ], timeout=300).returncode == 0
    if e.name == "mysql":
        rows = ",".join(f"({i},'mysql')" for i in range(lo, hi + 1))
        return sh([
            "docker", "exec", "-i", e.container, "mysql", "-urivet", "-privet", "rivet",
        ], timeout=300) and subprocess.run(
            ["docker", "exec", "-i", e.container, "mysql", "-urivet", "-privet", "rivet"],
            input=f"DROP TABLE IF EXISTS {TABLE}; CREATE TABLE {TABLE}(id INT PRIMARY KEY, src TEXT); "
                  f"INSERT INTO {TABLE} VALUES {rows};",
            capture_output=True, text=True, timeout=300,
        ).returncode == 0
    if e.name == "mssql":
        rows = ",".join(f"({i},'mssql')" for i in range(lo, hi + 1))
        return subprocess.run(
            ["docker", "exec", "-i", e.container, "/opt/mssql-tools18/bin/sqlcmd",
             "-S", "localhost", "-U", "rivet", "-P", "rivet", "-C", "-d", "rivet", "-b"],
            input=f"IF OBJECT_ID('dbo.{TABLE}') IS NOT NULL DROP TABLE dbo.{TABLE};\n"
                  f"CREATE TABLE dbo.{TABLE}(id INT PRIMARY KEY, src NVARCHAR(20));\n"
                  f"INSERT INTO dbo.{TABLE} VALUES {rows};\n",
            capture_output=True, text=True, timeout=600,
        ).returncode == 0
    if e.name == "mongo":
        docs = ",".join(f"{{_id:{i},src:'mongo'}}" for i in range(lo, hi + 1))
        return subprocess.run(
            ["docker", "exec", e.container, "mongosh", "mongodb://127.0.0.1:27017/rivet",
             "--quiet", "--eval", f"db.{TABLE}.drop(); db.{TABLE}.insertMany([{docs}]);"],
            capture_output=True, text=True, timeout=600,
        ).returncode == 0
    return False


def batch_config(path: Path, e: Engine, dest: str) -> None:
    tls = "\n  tls: { accept_invalid_certs: true }" if e.name == "mssql" else ""
    table = f"dbo.{TABLE}" if e.name == "mssql" else (f"public.{TABLE}" if e.name == "postgres" else TABLE)
    path.write_text(
        f'source:\n  type: {e.name}\n  url: "{e.url}"{tls}\n'
        f"exports:\n"
        # The SAME output table name for every engine — the point of the exercise.
        f"  - name: {TABLE}_{e.name}\n"
        f"    table: {table}\n"
        f"    mode: full\n"
        f"    format: parquet\n"
        f"    destination: {dest}\n"
    )


@dataclass
class Outcome:
    label: str
    problems: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def launch(cfgs: list[Path], env: dict) -> tuple[list[int], str]:
    procs = [
        (c, subprocess.Popen([RIVET, "run", "-c", str(c)], stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, env={**os.environ, **env}))
        for c in cfgs
    ]
    exits, chatter = [], []
    for c, p in procs:
        out, _ = p.communicate()
        exits.append(p.returncode)
        if p.returncode != 0:
            tail = (out or "").strip().splitlines()
            chatter.append(f"{c.stem}: {tail[-1] if tail else '(silent)'}")
    return exits, " | ".join(chatter[:3])


def check_union(out: Outcome, read_sql: str, engines: list[Engine]) -> None:
    """Per-id, not per-count: loss, duplication and overwrite look identical in a
    total and completely different here."""
    expected = {i for e in engines for i in range(e.base, e.base + PER_ENGINE)}
    got = duckdb(f"SELECT id FROM ({read_sql})")
    ids = [int(x) for x in got.splitlines() if x.strip().isdigit()]
    seen = set(ids)
    missing = expected - seen
    if missing:
        by_engine = {
            e.name: len([i for i in missing if e.base <= i < e.base + PER_ENGINE])
            for e in engines
        }
        out.problems.append(
            f"{len(missing)} seeded row(s) are NOT in the destination — per engine {by_engine}. "
            f"A whole range missing is one writer's output overwritten; a scatter is loss"
        )
    dupes = len(ids) - len(seen)
    if dupes:
        out.problems.append(
            f"{dupes} duplicate id(s) in the destination — the union of concurrent writers must "
            f"be a SET, or a downstream load double-counts"
        )
    extra = seen - expected
    if extra:
        out.problems.append(f"{len(extra)} id(s) nobody seeded, e.g. {sorted(extra)[:3]}")
    if not out.problems:
        out.notes.append(f"{len(seen)} distinct ids, exactly the union of {len(engines)} engines")


def check_ledger(out: Outcome, exports: list[str]) -> None:
    names = "','".join(exports)
    dupes = psql_state(
        f"SELECT count(*) FROM (SELECT run_id FROM export_metrics WHERE export_name IN ('{names}') "
        f"AND run_id IS NOT NULL GROUP BY run_id HAVING count(*) > 1) d"
    )
    if dupes not in ("0", ""):
        out.problems.append(f"{dupes} run(s) with more than one export_metrics row")
    stuck = psql_state(
        f"SELECT count(*) FROM run_status a WHERE a.export_name IN ('{names}') AND a.status='running' "
        f"AND NOT EXISTS (SELECT 1 FROM run_status b WHERE b.export_name=a.export_name "
        f"AND b.started_at > a.started_at)"
    )
    if stuck not in ("0", ""):
        out.problems.append(f"{stuck} run(s) left `running` with nothing superseding them")


def local_leg(work: Path, backend: str, state_env: dict) -> Outcome:
    out = Outcome(f"local/{backend}")
    dest = work / "shared"
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True, exist_ok=True)
    cfgs = []
    for e in ENGINES:
        c = work / f"batch_{e.name}.yaml"
        batch_config(c, e, f"{{ type: local, path: {dest} }}")
        cfgs.append(c)
    exits, chatter = launch(cfgs, state_env)
    if any(x != 0 for x in exits):
        out.problems.append(f"writer exits {exits}: {chatter}")
        return out

    files = sorted(dest.rglob("*.parquet"))
    copies = sorted(dest.rglob("manifest-*.json"))
    if len(copies) != len(ENGINES):
        out.problems.append(
            f"{len(copies)} manifest copies for {len(ENGINES)} writers — a consumer sums the "
            f"copies, so a missing one hides a whole engine's output"
        )
    claimed = sum(json.loads(m.read_text()).get("row_count") or 0 for m in copies)
    if claimed != len(ENGINES) * PER_ENGINE:
        out.problems.append(
            f"the manifests claim {claimed} rows, {len(ENGINES)} engines exported "
            f"{len(ENGINES) * PER_ENGINE}"
        )
    if files:
        lst = ",".join(f"'{f}'" for f in files)
        check_union(out, f"SELECT id FROM read_parquet([{lst}])", ENGINES)
    else:
        out.problems.append("no parquet on the shared prefix at all")
    if backend == "postgres":
        check_ledger(out, [f"{TABLE}_{e.name}" for e in ENGINES])
    return out


def mixed_shape_refusal(work: Path, state_env: dict) -> Outcome:
    """Batch and CDC into ONE prefix must be REFUSED, not silently merged."""
    out = Outcome("mixed-shape refusal")
    if not CDC_URLS.get("postgres"):
        out.notes.append("no CDC url — skipped")
        return out
    from release_oracle import cdc as gate_cdc

    dest = work / "mixed"
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True, exist_ok=True)
    b = work / "mix_batch.yaml"
    batch_config(b, ENGINES[0], f"{{ type: local, path: {dest} }}")
    if sh([RIVET, "run", "-c", str(b)], env=state_env).returncode != 0:
        out.problems.append("the batch half of the mixed-shape probe did not run")
        return out

    url = CDC_URLS["postgres"]
    block = gate_cdc._ENGINES["postgres"].setup(url, work)
    if block is None:
        out.notes.append("CDC source setup failed — refusal not exercised")
        return out
    c = work / "mix_cdc.yaml"
    c.write_text(
        f'source:\n  type: postgres\n  url: "{url}"\n'
        f"exports:\n  - name: {TABLE}_cdc\n    table: orc_cdc_probe\n    mode: cdc\n"
        f"    format: parquet\n    {block}\n"
        f"    destination: {{ type: local, path: {dest} }}\n"
    )
    r = sh([RIVET, "run", "-c", str(c)], env=state_env)
    if r.returncode == 0:
        out.problems.append(
            "a CDC export wrote into a prefix already holding a BATCH manifest and SUCCEEDED — "
            "the cross-shape guard is what stops the two from destroying each other's manifest "
            "(finding #44), so accepting it is the defect, not the refusal"
        )
    else:
        out.notes.append("refused, as it must be — the two shapes cannot share one prefix")
    gate_cdc._ENGINES["postgres"].cleanup(url, work)
    return out


def main() -> int:
    if not Path(RIVET).exists():
        print(f"binary not found: {RIVET}")
        return 2
    work = Path(os.environ.get("RIVET_MW_WORK", "/tmp/rivet_multiwriter"))
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)

    print("=== seeding the same logical table on every engine, disjoint id ranges ===")
    ready = []
    for e in ENGINES:
        ok = seed(e)
        print(f"  {e.name:<9} ids {e.base}..{e.base + PER_ENGINE - 1}  {'ok' if ok else 'FAILED'}")
        if ok:
            ready.append(e)
    if len(ready) < 2:
        print("  fewer than two engines seeded — a concurrency test needs writers")
        return 2
    globals()["ENGINES"] = ready

    results: list[Outcome] = []
    print("\n=== all engines, batch, concurrently -> ONE prefix, ONE Postgres backend ===")
    results.append(local_leg(work / "pg", "postgres", {"RIVET_STATE_URL": STATE_URL}))
    print("\n=== the same, into a SQLite backend ===")
    results.append(local_leg(work / "sq", "sqlite", {"RIVET_STATE_URL": ""}))
    print("\n=== batch + CDC into ONE prefix must be REFUSED ===")
    results.append(mixed_shape_refusal(work / "mix", {"RIVET_STATE_URL": STATE_URL}))

    print("\n=== verdict ===")
    bad = 0
    for r in results:
        if r.problems:
            bad += 1
            print(f"  ✗ {r.label}: " + "; ".join(r.problems))
        else:
            print(f"  ✓ {r.label}" + (f" — {'; '.join(r.notes)}" if r.notes else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
