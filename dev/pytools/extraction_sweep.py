"""Every extraction mechanic, run for real, and cross-checked against the meta-DB.

The scenario-artifact matrix grades a curated list of lifecycle events. This is
the other axis: the EXTRACTION surface itself — mode × runner-selecting flag ×
crash point — with every run's artifacts reconciled three ways.

    SOURCE          what the database actually holds
    DESTINATION     what DuckDB reads back from the parts the manifest lists
    META-DB         what the run recorded about itself

The point is the disagreements. Each of those three is produced by a different
party, so a run where all three agree is evidence, while a run where only rivet's
own two numbers agree is the shape that has hidden every bookkeeping defect this
codebase has had. Runs against the POSTGRES state backend on purpose: it is the
one an operator uses for a shared deployment, and it has already been shown to
diverge from SQLite (shape tracking never worked there).

Scoped by export NAME rather than by wiping the state: a shared backend keeps
every run ever made, so isolation has to come from the key, not from the table
being empty.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE_URL = os.environ.get(
    "RIVET_SWEEP_STATE_URL", "postgresql://rivet:rivet@localhost:5433/rivet_state"
)
SOURCE_URL = os.environ.get(
    "RIVET_SWEEP_SOURCE_URL", "postgresql://rivet:rivet@localhost:5432/rivet"
)
STATE_CONTAINER = os.environ.get("RIVET_SWEEP_STATE_CONTAINER", "rivet-postgres-state-1")
SRC_CONTAINER = os.environ.get("RIVET_SWEEP_SRC_CONTAINER", "rivet-postgres-1")
RIVET = os.environ.get("RIVET_BIN", str(ROOT / "target" / "release" / "rivet"))

#: Rows in the probe table. Deliberately larger than PROBE_BATCH_SIZE (500) so
#: every chunk spans MORE THAN ONE read batch — a fixture that fits in one batch
#: cannot tell a per-batch fold from a per-part one, which is exactly how a
#: broken Form B stayed green in the live suite.
ROWS = 4000


@dataclass
class Case:
    name: str
    export_block: str
    crash: str | None = None
    resume: bool = False
    #: Runs this case a second time (resume / repeat semantics).
    second_run: bool = False
    notes: str = ""


@dataclass
class Result:
    case: str
    source_rows: int | None = None
    dest_rows: int | None = None
    dest_distinct: int | None = None
    manifest_rows: int | None = None
    manifest_parts: int | None = None
    parts_on_disk: int | None = None
    metrics_rows: int | None = None
    files_committed: int | None = None
    file_log_rows: int | None = None
    running_left: int | None = None
    validate_exit: int | None = None
    disagreements: list[str] = field(default_factory=list)
    note: str = ""


def sh(argv: list[str], env: dict | None = None, timeout: int = 900) -> subprocess.CompletedProcess:
    e = {**os.environ, **(env or {})}
    return subprocess.run(argv, capture_output=True, text=True, env=e, timeout=timeout)


def psql_src(sql: str) -> str:
    p = sh(["docker", "exec", SRC_CONTAINER, "psql", "-U", "rivet", "-d", "rivet", "-tAc", sql])
    return p.stdout.strip()


def psql_state(sql: str) -> str:
    p = sh(
        ["docker", "exec", STATE_CONTAINER, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc", sql]
    )
    return p.stdout.strip()


def duckdb(sql: str) -> str:
    p = sh(["duckdb", "-noheader", "-list", "-c", sql])
    return p.stdout.strip()


def as_int(s: str) -> int | None:
    try:
        return int(s.strip())
    except (ValueError, AttributeError):
        return None


CASES: list[Case] = [
    Case("full", "    mode: full\n"),
    Case(
        "full+row_hash",
        "    mode: full\n    meta_columns:\n      row_hash: true\n",
        notes="the enrichment column travels with the data",
    ),
    Case("chunked", "    mode: chunked\n    chunk_column: id\n    chunk_size: 1000\n"),
    Case(
        "chunked+checkpoint",
        "    mode: chunked\n    chunk_column: id\n    chunk_size: 1000\n    chunk_checkpoint: true\n",
    ),
    Case(
        "chunked+checkpoint+crash",
        "    mode: chunked\n    chunk_column: id\n    chunk_size: 1000\n    chunk_checkpoint: true\n",
        crash="after_chunk_file:0",
        resume=True,
        notes="crash mid-chunk, then --resume",
    ),
    Case(
        "chunked+dense",
        "    mode: chunked\n    chunk_column: id\n    chunk_dense: true\n    chunk_size: 1000\n",
    ),
    Case(
        "chunked+parallel",
        "    mode: chunked\n    chunk_column: id\n    chunk_size: 1000\n    parallel: 3\n",
    ),
    Case("keyset", "    mode: chunked\n    chunk_by_key: id\n    chunk_size: 1000\n"),
    Case(
        "keyset+parallel",
        "    mode: chunked\n    chunk_by_key: id\n    chunk_size: 1000\n    parallel: 3\n",
    ),
    Case(
        "keyset+checkpoint",
        "    mode: chunked\n    chunk_by_key: id\n    chunk_size: 1000\n    chunk_checkpoint: true\n",
    ),
    Case(
        "keyset+crash",
        "    mode: chunked\n    chunk_by_key: id\n    chunk_size: 1000\n    chunk_checkpoint: true\n",
        crash="keyset_after_data_complete",
        notes="crash after the data is written, before the run is finished",
    ),
    Case(
        "incremental",
        "    mode: incremental\n    cursor_column: id\n",
        second_run=True,
        notes="second run must add nothing",
    ),
    Case(
        "full+crash_after_file",
        "    mode: full\n",
        crash="after_file_write",
        notes="crash after the part is written, before the manifest",
    ),
    # `before_commit_rename` is deliberately NOT here: it is a BLOCK hook
    # (`maybe_block_at`), not a panic point, so `RIVET_TEST_PANIC_AT` never fires
    # it and the case ran to a clean success while calling itself a crash test —
    # a fixture that tests nothing and reports green. The harness now asserts the
    # crash actually happened, which is what surfaced it.
    Case(
        "full+crash_after_manifest",
        "    mode: full\n",
        crash="after_manifest_update",
        notes="crash after the manifest is written",
    ),
]


def write_cfg(work: Path, name: str, block: str, out: Path) -> Path:
    cfg = work / f"{name}.yaml"
    cfg.write_text(
        f"source:\n"
        f"  type: postgres\n"
        f'  url: "{SOURCE_URL}"\n'
        f"exports:\n"
        f"  - name: {name}\n"
        f"    table: public.sweep_probe\n"
        f"{block}"
        f"    format: parquet\n"
        f"    destination: {{ type: local, path: {out} }}\n"
    )
    return cfg


def measure(case: Case, work: Path) -> Result:
    r = Result(case=case.name, note=case.notes)
    out = work / case.name
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True, exist_ok=True)
    cfg = write_cfg(work, case.name, case.export_block, out)
    env = {"RIVET_STATE_URL": STATE_URL}

    # Every run_id this export already has. A shared Postgres backend KEEPS every
    # run ever made, so counting `running` rows by export name alone sums this
    # sweep's own history — it reported a leak growing 1 -> 2 -> 3 across three
    # sweep executions when a single crash leaves exactly one row, correctly, as
    # the marker that stops gc from collecting a possibly-live run. Isolation on a
    # shared store has to come from the KEY, not from the table being empty.
    before_runs = {
        rid
        for rid in psql_state(
            f"SELECT run_id FROM run_status WHERE export_name='{case.name}'"
        ).splitlines()
        if rid.strip()
    }

    if case.crash:
        sh([RIVET, "run", "-c", str(cfg)], env={**env, "RIVET_TEST_PANIC_AT": case.crash})
        if case.resume:
            sh([RIVET, "run", "-c", str(cfg), "--export", case.name, "--resume"], env=env)
    else:
        sh([RIVET, "run", "-c", str(cfg)], env=env)
        if case.second_run:
            sh([RIVET, "run", "-c", str(cfg)], env=env)

    r.source_rows = as_int(psql_src("SELECT count(*) FROM sweep_probe"))
    r.parts_on_disk = len(list(out.rglob("*.parquet")))

    manifest = out / "manifest.json"
    listed: list[str] = []
    if manifest.exists():
        try:
            m = json.loads(manifest.read_text())
            r.manifest_rows = m.get("row_count")
            r.manifest_parts = len(m.get("parts", []))
            listed = [p["path"] for p in m.get("parts", [])]
        except (OSError, json.JSONDecodeError):
            pass

    # DuckDB reads ONLY the parts the manifest lists — an orphan from a crash is
    # deliberately excluded, because the manifest is what every consumer follows.
    if listed:
        files = ",".join(f"'{out}/{p}'" for p in listed)
        got = duckdb(f"SELECT count(*)||'|'||count(DISTINCT id) FROM read_parquet([{files}])")
        if "|" in got:
            a, b = got.split("|", 1)
            r.dest_rows, r.dest_distinct = as_int(a), as_int(b)

    r.metrics_rows = as_int(
        psql_state(
            f"SELECT total_rows FROM export_metrics WHERE export_name='{case.name}' "
            f"ORDER BY id DESC LIMIT 1"
        )
    )
    r.files_committed = as_int(
        psql_state(
            f"SELECT files_committed FROM export_metrics WHERE export_name='{case.name}' "
            f"ORDER BY id DESC LIMIT 1"
        )
    )
    r.file_log_rows = as_int(
        psql_state(f"SELECT count(*) FROM file_log WHERE export_name='{case.name}'")
    )
    now_running = {
        rid
        for rid in psql_state(
            f"SELECT run_id FROM run_status WHERE export_name='{case.name}' AND status='running'"
        ).splitlines()
        if rid.strip()
    }
    r.running_left = len(now_running - before_runs)
    r.validate_exit = sh([RIVET, "validate", "-c", str(cfg), "--depth", "full"], env=env).returncode

    crashed_no_resume = bool(case.crash) and not case.resume
    if not crashed_no_resume:
        if r.dest_rows is not None and r.source_rows is not None and r.dest_rows != r.source_rows:
            r.disagreements.append(f"dest {r.dest_rows} != source {r.source_rows}")
        if r.dest_distinct is not None and r.dest_rows != r.dest_distinct:
            r.disagreements.append(f"dest has duplicates ({r.dest_rows} rows, {r.dest_distinct} ids)")
        if r.manifest_rows is not None and r.dest_rows is not None and r.manifest_rows != r.dest_rows:
            r.disagreements.append(f"manifest {r.manifest_rows} != dest {r.dest_rows}")
        if (
            r.metrics_rows is not None
            and r.manifest_rows is not None
            and r.metrics_rows != r.manifest_rows
        ):
            r.disagreements.append(f"meta-DB rows {r.metrics_rows} != manifest {r.manifest_rows}")
        if (
            r.files_committed is not None
            and r.manifest_parts is not None
            and r.files_committed != r.manifest_parts
        ):
            r.disagreements.append(
                f"files_committed {r.files_committed} != manifest parts {r.manifest_parts}"
            )
        if r.validate_exit not in (0, None):
            r.disagreements.append(f"validate exit {r.validate_exit} on data that reconciles")
    # A crash with no resume SHOULD leave one: it marks a run that may still be
    # live, so `gc_orphans` defers rather than deleting an in-flight run's parts,
    # and a later run of the same export supersedes it by `started_at` (verified —
    # never an age timer). Only an UNEXPLAINED one is a disagreement.
    expected_running = 1 if (case.crash and not case.resume) else 0
    if r.running_left != expected_running:
        r.disagreements.append(
            f"{r.running_left} run_status row(s) left `running`, expected {expected_running}"
            + (
                " — the injected crash never fired, so this case proves nothing"
                if case.crash and r.running_left == 0
                else ""
            )
        )
    return r


def main() -> int:
    only = sys.argv[1] if len(sys.argv) > 1 else None
    if not Path(RIVET).exists():
        print(f"binary not found: {RIVET}")
        return 2
    psql_src(
        "DROP TABLE IF EXISTS sweep_probe; "
        "CREATE TABLE sweep_probe(id INT PRIMARY KEY, v TEXT, n INT); "
        f"INSERT INTO sweep_probe SELECT g,'v'||g,g*2 FROM generate_series(1,{ROWS}) g;"
    )
    work = Path(os.environ.get("RIVET_SWEEP_WORK", "/tmp/rivet_sweep"))
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)

    cases = [c for c in CASES if not only or c.name == only]
    print(f"extraction sweep: {len(cases)} case(s), {ROWS} source rows, state = postgres\n")
    results = [measure(c, work) for c in cases]

    hdr = f"  {'case':<26} {'src':>5} {'dest':>5} {'mfst':>5} {'meta':>5} {'parts':>5} {'val':>3}  verdict"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    bad = 0
    for r in results:
        v = "OK" if not r.disagreements else "; ".join(r.disagreements)
        if r.disagreements:
            bad += 1
        print(
            f"  {r.case:<26} {str(r.source_rows or '-'):>5} {str(r.dest_rows or '-'):>5} "
            f"{str(r.manifest_rows or '-'):>5} {str(r.metrics_rows or '-'):>5} "
            f"{str(r.parts_on_disk or '-'):>5} {str(r.validate_exit):>3}  {v}"
        )
    print(f"\n  {len(results) - bad} agree, {bad} disagree")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
