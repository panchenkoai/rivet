"""Every engine, batch AND CDC, into ONE state backend at the same time.

The sweeps run one engine at a time, so every number they produce comes from a
process that had the state database to itself. That is not what a shared
deployment looks like: there, several exports run at once against one Postgres
host, and the questions that only appear under concurrency are exactly the ones
a single-writer test cannot ask.

    Does anything get LOST?      one `export_metrics` row per run, one `file_log`
                                 row per durable part — under interleaved writers,
                                 not just sequential ones.
    Does anything BREAK?         no run left wedged `running`, no duplicate
                                 aggregate, no run whose ledger disagrees with its
                                 own artifacts.
    Does the SCHEMA survive?     the database starts EMPTY, so N processes reach
                                 the migrations simultaneously. A shared backend
                                 is provisioned exactly once and then several
                                 exports hit it at once — if that races, it races
                                 on day one of a deployment.

The last one is why this wipes first rather than reusing a migrated database. It
is the cheapest possible way to run the migration path concurrently, and it costs
nothing to include.

Reuses the per-engine sweeps as the WORKLOAD (each already drives 17 CDC axes and
checks its own artifacts) and adds the checks that only mean anything across all
of them at once. The sweeps' lock is per engine, so they may run together; they
would still refuse to run twice on the same engine, which is a different hazard
(shared probe tables) that concurrency does not make safe.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE_CONTAINER = os.environ.get("RIVET_SWEEP_STATE_CONTAINER", "rivet-postgres-state-1")
CDC_SWEEP = ROOT / "dev" / "pytools" / "cdc_sweep.py"
BATCH_SWEEP = ROOT / "dev" / "pytools" / "extraction_sweep.py"

ENGINES = {
    "postgres": "RIVET_CDC_POSTGRES_URL",
    "mysql": "RIVET_CDC_MYSQL_URL",
    "mssql": "RIVET_CDC_MSSQL_URL",
    "mongo": "RIVET_CDC_MONGO_URL",
}


def psql_state(sql: str) -> str:
    return subprocess.run(
        ["docker", "exec", STATE_CONTAINER, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc", sql],
        capture_output=True,
        text=True,
    ).stdout.strip()


def wipe() -> None:
    psql_state(
        "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO rivet;"
    )
    for base in (ROOT, Path("/tmp")):
        for p in base.rglob(".rivet_state.db*"):
            try:
                p.unlink()
            except OSError:
                pass
    for p in Path("/tmp").glob(".rivet_cdc_sweep.*.lock"):
        p.unlink(missing_ok=True)


def main() -> int:
    token = os.environ.get("RIVET_CONC_TOKEN", "conc")
    work = os.environ.get("RIVET_CONC_WORK", "/tmp/rivet_conc")
    missing = [e for e, var in ENGINES.items() if not os.environ.get(var)]
    if missing:
        print(f"no URL for: {', '.join(missing)} — set them or expect fewer writers")

    print("=== wiping the state backend (the migrations run concurrently below) ===")
    wipe()
    left = psql_state("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
    print(f"  {left} tables — every writer below meets an empty database at once")

    procs: list[tuple[str, subprocess.Popen]] = []
    started = time.strftime("%H:%M:%S")
    for engine, var in ENGINES.items():
        if not os.environ.get(var):
            continue
        log = open(f"/tmp/conc_{engine}.log", "w")
        procs.append((
            engine,
            subprocess.Popen(
                [sys.executable, "-u", str(CDC_SWEEP)],
                stdout=log,
                stderr=subprocess.STDOUT,
                env={
                    **os.environ,
                    "RIVET_CDC_SWEEP_ENGINE": engine,
                    "RIVET_CDC_SWEEP_TOKEN": f"{token}{engine[:2]}",
                    "RIVET_CDC_SWEEP_WORK": f"{work}/{engine}",
                },
            ),
        ))
    log = open("/tmp/conc_batch.log", "w")
    procs.append((
        "batch",
        subprocess.Popen(
            [sys.executable, "-u", str(BATCH_SWEEP)],
            stdout=log,
            stderr=subprocess.STDOUT,
            env={**os.environ, "RIVET_SWEEP_WORK": f"{work}/batch"},
        ),
    ))

    print(f"=== {len(procs)} writers started at {started}, all into one state backend ===")
    for name, p in procs:
        print(f"  {name}: pid {p.pid}  log /tmp/conc_{name}.log")
    outcomes = {}
    for name, p in procs:
        outcomes[name] = p.wait()
        print(f"  {name} finished, exit {outcomes[name]}")

    print("\n=== what the sweeps themselves said ===")
    for name in outcomes:
        line = ""
        try:
            for ln in Path(f"/tmp/conc_{name}.log").read_text().splitlines():
                if "agree," in ln:
                    line = ln.strip()
        except OSError:
            pass
        print(f"  {name:<9} {line or '(no summary — read the log)'}")

    # ── the checks that only exist across all writers at once ────────────────
    print("\n=== the ledger, across every writer ===")
    problems: list[str] = []

    dupes = psql_state(
        "SELECT count(*) FROM (SELECT run_id FROM export_metrics WHERE run_id IS NOT NULL "
        "GROUP BY run_id HAVING count(*) > 1) d"
    )
    print(f"  runs with MORE THAN ONE export_metrics row: {dupes}")
    if dupes not in ("0", ""):
        problems.append(
            f"{dupes} run(s) have several aggregate rows — the in-flight row must be UPDATED and "
            f"then replaced, never appended, or every sum over export_metrics double-counts"
        )

    orphan_metrics = psql_state(
        "SELECT count(*) FROM export_metrics m WHERE m.run_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM run_status s WHERE s.run_id = m.run_id)"
    )
    print(f"  metrics rows with no run_status row: {orphan_metrics}")
    if orphan_metrics not in ("0", ""):
        problems.append(
            f"{orphan_metrics} metrics row(s) name a run the run-status ledger never recorded — "
            f"two tables written by the same run disagree that it happened"
        )

    stuck = psql_state(
        "SELECT count(*) FROM run_status a WHERE a.status = 'running' "
        "AND NOT EXISTS (SELECT 1 FROM run_status b WHERE b.export_name = a.export_name "
        "AND b.started_at > a.started_at)"
    )
    print(f"  `running` rows NOT outranked by a later run: {stuck}")
    if stuck not in ("0", ""):
        problems.append(
            f"{stuck} run(s) still read as ACTIVE with nothing superseding them — `gc_orphans` "
            f"would defer forever on those prefixes"
        )

    files = psql_state("SELECT count(*) FROM file_log")
    runs = psql_state("SELECT count(*) FROM run_status")
    metrics = psql_state("SELECT count(*) FROM export_metrics")
    print(f"  totals: {runs} runs, {metrics} aggregates, {files} durable parts recorded")
    if files in ("0", "") or runs in ("0", ""):
        problems.append(
            "the ledger is EMPTY after a concurrent run — the writers did not reach the database "
            "at all, so nothing above means anything"
        )

    # Migration under concurrency: N processes met an empty database at once.
    ver = psql_state("SELECT count(*) FROM rivet_schema_version")
    print(f"  rivet_schema_version rows after a concurrent cold start: {ver}")
    if ver in ("0", ""):
        problems.append("the schema-version table is empty — the concurrent migration left no record")

    print()
    if problems:
        print("  DISAGREEMENTS:")
        for p in problems:
            print(f"    - {p}")
        return 1
    bad_sweeps = [n for n, rc in outcomes.items() if rc not in (0,)]
    if bad_sweeps:
        print(
            f"  the ledger is consistent, but these writers reported disagreements of their own: "
            f"{', '.join(bad_sweeps)} — read their logs"
        )
        return 1
    print("  no loss, no duplicate, no wedged run — every writer's own sweep agreed too")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
