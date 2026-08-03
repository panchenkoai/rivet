"""Every CDC mechanic, run for real, reconciled three ways.

The batch sibling (`extraction_sweep.py`) reconciles SOURCE / DESTINATION /
META-DB for the extraction surface. CDC needs the same treatment and a different
notion of truth: the source side is not a row count but the CHANGE SET applied,
which the harness knows exactly because it applied it.

    SOURCE      the changes issued — 3 inserts, 1 update, 1 delete = 5 events
    DESTINATION what DuckDB reads back from the captured parts
    META-DB     what the run recorded about itself

Reuses the release gate's per-engine CDC setup (`dev/release_oracle/cdc.py`)
rather than reimplementing slot / binlog / capture-instance provisioning — one
definition of "a CDC-ready probe table", so a sweep failure is about the mechanic
and not about my own DDL.

Two properties this exists to check, neither visible in a single run:

  COMPLETENESS  every issued change reaches the destination.
  AT-LEAST-ONCE a crash between flush and ack must re-read, never skip. The
                union across the crash and the recovery run must still contain
                every change; duplicates are permitted by the contract, loss is
                not.

Runs against the POSTGRES state backend, like the batch sweep, because that is
what a shared deployment uses and it has already diverged from SQLite once.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "dev"))

STATE_URL = os.environ.get(
    "RIVET_SWEEP_STATE_URL", "postgresql://rivet:rivet@localhost:5433/rivet_state"
)
STATE_CONTAINER = os.environ.get("RIVET_SWEEP_STATE_CONTAINER", "rivet-postgres-state-1")
RIVET = os.environ.get("RIVET_BIN", str(ROOT / "target" / "release" / "rivet"))

#: The gate's own probe workload: 3 inserts, 1 update, 1 delete.
EXPECTED_EVENTS = 5

ENGINE_URLS = {
    "postgres": os.environ.get("RIVET_CDC_POSTGRES_URL", ""),
    "mysql": os.environ.get("RIVET_CDC_MYSQL_URL", ""),
    "mssql": os.environ.get("RIVET_CDC_MSSQL_URL", ""),
    # Mongo CDC is a change stream, which requires a REPLICA SET — a standalone
    # mongod cannot serve one, so the URL must point at the rs member.
    "mongo": os.environ.get("RIVET_CDC_MONGO_URL", ""),
}


@dataclass
class Case:
    name: str
    #: Extra keys folded into the `cdc:` block.
    cdc_extra: str = ""
    crash: str | None = None
    #: Run again after the crash — the recovery half of at-least-once.
    recover: bool = False
    #: Run twice with no crash — resume must add nothing.
    twice: bool = False
    notes: str = ""


@dataclass
class Result:
    engine: str
    case: str
    dest_events: int | None = None
    dest_ids: int | None = None
    parts: int | None = None
    metrics_rows: int | None = None
    running_left: int | None = None
    validate_exit: int | None = None
    disagreements: list[str] = field(default_factory=list)
    skipped: str = ""


CASES: list[Case] = [
    Case("bounded", notes="until_current: one bounded drain"),
    Case("bounded+twice", twice=True, notes="the second drain must add nothing"),
    Case("rollover_1", cdc_extra=", rollover: 1", notes="a part per change — many parts, one run"),
    Case(
        "crash_flush_before_ack",
        crash="cdc_after_flush_before_ack",
        recover=True,
        notes="the classic at-least-once point: parts durable, source not acked",
    ),
    Case(
        "crash_checkpoint_before_ack",
        crash="cdc_after_checkpoint_before_ack",
        recover=True,
        notes="checkpoint advanced, ack not sent",
    ),
    Case(
        "crash_after_ack",
        crash="cdc_after_ack",
        recover=True,
        notes="source acked — the recovery must not lose what was already captured",
    ),
    Case(
        "crash_before_manifest",
        crash="cdc_before_manifest",
        recover=True,
        notes="parts written, manifest not — the orphan case",
    ),
]


def sh(argv: list[str], env: dict | None = None, timeout: int = 900):
    return subprocess.run(
        argv, capture_output=True, text=True, env={**os.environ, **(env or {})}, timeout=timeout
    )


def psql_state(sql: str) -> str:
    return sh(
        ["docker", "exec", STATE_CONTAINER, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc", sql]
    ).stdout.strip()


def duckdb(sql: str) -> str:
    return sh(["duckdb", "-noheader", "-list", "-c", sql]).stdout.strip()


def as_int(s: str) -> int | None:
    try:
        return int(s.strip())
    except (ValueError, AttributeError):
        return None


def measure(engine: str, case: Case, work_root: Path) -> Result:
    from release_oracle import cdc as gate_cdc

    r = Result(engine=engine, case=case.name)
    url = ENGINE_URLS.get(engine, "")
    spec = gate_cdc._ENGINES.get(engine)
    if not url or spec is None:
        r.skipped = f"no RIVET_CDC_{engine.upper()}_URL"
        return r

    export = f"cdcsweep_{engine}_{case.name}"
    work = work_root / export
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    out = work / "out"
    out.mkdir(parents=True, exist_ok=True)

    # Retry the provisioning. SQL Server's capture instance is disabled and
    # re-enabled per case, and that is ASYNCHRONOUS — running seven cases
    # back-to-back on one table outran the capture Agent and five of them
    # reported "source setup failed", which reads as an engine problem when it is
    # the harness not waiting. Verified by running the same DDL by hand between
    # sweeps: it succeeds. Bounded, so a genuinely broken source still surfaces.
    cdc_block = None
    for attempt in range(4):
        cdc_block = spec.setup(url, work)
        if cdc_block is not None:
            break
        time.sleep(2.0 * (attempt + 1))
    if cdc_block is None:
        r.skipped = "source setup failed after 4 attempts"
        return r
    if case.cdc_extra:
        cdc_block = cdc_block.rstrip().rstrip("}") + case.cdc_extra + " }"

    tls = "\n  tls: { accept_invalid_certs: true }" if engine == "mssql" else ""
    cfg = work / "cdc.yaml"
    cfg.write_text(
        f"source:\n  type: {engine}\n  url: \"{url}\"{tls}\n"
        f"exports:\n"
        f"  - name: {export}\n"
        f"    table: orc_cdc_probe\n"
        f"    mode: cdc\n"
        f"    format: parquet\n"
        f"    {cdc_block}\n"
        f"    destination: {{ type: local, path: {out} }}\n"
    )
    env = {"RIVET_STATE_URL": STATE_URL}
    before_runs = {
        rid
        for rid in psql_state(
            f"SELECT run_id FROM run_status WHERE export_name='{export}'"
        ).splitlines()
        if rid.strip()
    }

    try:
        # Anchor first (an empty drain), THEN issue the changes: a CDC run that
        # has never opened has nothing to resume from, and anchoring after the
        # changes would race them.
        sh([RIVET, "run", "-c", str(cfg)], env=env)
        spec.changes(url)

        if case.crash:
            crashed = sh([RIVET, "run", "-c", str(cfg)], env={**env, "RIVET_TEST_PANIC_AT": case.crash})
            if crashed.returncode == 0:
                r.disagreements.append(
                    f"the injected crash `{case.crash}` never fired — this case proves nothing"
                )
            if case.recover:
                sh([RIVET, "run", "-c", str(cfg)], env=env)
        else:
            sh([RIVET, "run", "-c", str(cfg)], env=env)
            if case.twice:
                sh([RIVET, "run", "-c", str(cfg)], env=env)

        parts = sorted(out.rglob("*.parquet"))
        r.parts = len(parts)
        if parts:
            files = ",".join(f"'{p}'" for p in parts)
            # The key column is the ENGINE's, not a constant: Mongo documents
            # carry `_id`. Querying `id` there made DuckDB error, the read
            # returned nothing, and every completeness assertion below was
            # skipped — seven cases reported OK having checked nothing.
            id_col = spec.id_col
            got = duckdb(
                f'SELECT count(*)||\'|\'||count(DISTINCT "{id_col}") FROM read_parquet([{files}])'
            )
            if "|" in got:
                a, b = got.split("|", 1)
                r.dest_events, r.dest_ids = as_int(a), as_int(b)

        r.metrics_rows = as_int(
            psql_state(
                f"SELECT total_rows FROM export_metrics WHERE export_name='{export}' "
                f"ORDER BY id DESC LIMIT 1"
            )
        )
        now_running = {
            rid
            for rid in psql_state(
                f"SELECT run_id FROM run_status WHERE export_name='{export}' AND status='running'"
            ).splitlines()
            if rid.strip()
        }
        r.running_left = len(now_running - before_runs)
        r.validate_exit = sh(
            [RIVET, "validate", "-c", str(cfg), "--depth", "full"], env=env
        ).returncode

        # COMPLETENESS. Duplicates are allowed by at-least-once; loss is not, so
        # the check is `>=` on events and exact on the distinct ids touched.
        # A destination that could not be READ is a failed measurement, never a
        # pass. Leaving it as "no opinion" is how the mongo rows above reported OK
        # while the check never ran.
        if r.parts and r.dest_events is None:
            r.disagreements.append(
                "the destination could not be read back — the completeness check "
                "never executed, so this case proves nothing"
            )
        if r.dest_events is not None and r.dest_events < EXPECTED_EVENTS:
            r.disagreements.append(
                f"captured {r.dest_events} of {EXPECTED_EVENTS} changes — at-least-once "
                f"permits duplicates, never loss"
            )
        if r.validate_exit not in (0, None):
            r.disagreements.append(f"validate exit {r.validate_exit}")
        # A crashed CDC run leaves its `running` row EVEN AFTER a successful
        # recovery, and that is correct — unlike a batch `--resume`, which adopts
        # the crashed run's id and closes that very row, CDC recovery is a FRESH
        # run with a new id. The crashed row is resolved by SUPERSESSION: a later
        # run of the same export outranks it by `started_at`, so it stops counting
        # as active without anything having to reach back and close it (verified
        # on the ledger — never an age timer). Expecting 0 here was my error, not
        # the product's.
        expected_running = 1 if case.crash else 0
        if r.running_left != expected_running:
            r.disagreements.append(
                f"{r.running_left} run_status row(s) left `running`, expected {expected_running}"
            )
        elif case.crash and case.recover:
            superseded = psql_state(
                f"SELECT count(*) FROM run_status a WHERE a.export_name='{export}' "
                f"AND a.status='running' AND NOT EXISTS (SELECT 1 FROM run_status b "
                f"WHERE b.export_name=a.export_name AND b.started_at > a.started_at)"
            )
            if as_int(superseded):
                r.disagreements.append(
                    "the crashed run's `running` row is NOT outranked by the recovery — "
                    "it would keep reading as an active run and gc would defer forever"
                )
    finally:
        try:
            spec.cleanup(url, work)
        except Exception:  # noqa: BLE001 - cleanup must never mask a finding
            pass
    return r


def main() -> int:
    only_engine = os.environ.get("RIVET_CDC_SWEEP_ENGINE")
    if not Path(RIVET).exists():
        print(f"binary not found: {RIVET}")
        return 2
    work = Path(os.environ.get("RIVET_CDC_SWEEP_WORK", "/tmp/rivet_cdc_sweep"))
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)

    engines = [e for e in ENGINE_URLS if not only_engine or e == only_engine]
    results: list[Result] = []
    print(f"cdc sweep: {len(engines)} engine(s) x {len(CASES)} case(s), state = postgres\n")
    for engine in engines:
        for case in CASES:
            results.append(measure(engine, case, work))

    hdr = f"  {'engine':<9} {'case':<28} {'events':>6} {'parts':>5} {'meta':>5} {'val':>3}  verdict"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    bad = 0
    for r in results:
        if r.skipped:
            v = f"SKIP {r.skipped}"
        elif r.disagreements:
            v = "; ".join(r.disagreements)
            bad += 1
        else:
            v = "OK"
        print(
            f"  {r.engine:<9} {r.case:<28} {str(r.dest_events or '-'):>6} "
            f"{str(r.parts or '-'):>5} {str(r.metrics_rows or '-'):>5} "
            f"{str(r.validate_exit if r.validate_exit is not None else '-'):>3}  {v}"
        )
    ran = [r for r in results if not r.skipped]
    print(f"\n  {len(ran) - bad} agree, {bad} disagree, {len(results) - len(ran)} skipped")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
