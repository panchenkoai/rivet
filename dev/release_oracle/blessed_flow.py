"""The WHOLE user journey, end to end, as one executable matrix.

`blessed_path` walks the batch command chain once per read-strategy. This module
is the complete thing: the full chain including `load`, both pipeline kinds, the
lifecycles a real deployment goes through (clean → repeat → crash+resume), every
store, both state backends, and the flags each command actually has.

WHY ONE MODULE AND NOT MORE CELLS ELSEWHERE

Because the alternative was tried and it is how the gaps happened. Coverage grew
by adding a cell wherever a defect appeared, so twenty-four matrices exist and
the axis that mattered — type × value-consumer — was in none of them. Adding
`cdc` here, `s3` there and `resume` somewhere else repeats that: each addition
is locally sensible and the whole stays un-designed.

So the axes are declared up front, the cross-product is generated, and a cell is
either RUN or explicitly `na` with a reason. What is not covered is visible as a
hole in a grid rather than as an absence nobody enumerated.

THE AXES

  pipeline    batch | cdc — different chains, not a flag on one chain. `plan`
              and `apply` REFUSE a cdc export ("cdc mode has no batch plan"),
              which is correct: a plan fixes the bounds of finite work and a
              change stream has none. So `cdc` runs `run` where `batch` runs
              `plan → apply`.
  lifecycle   clean | repeat | resume. `repeat` is the scheduler's real shape
              (two runs into one prefix, union must survive); `resume` is the
              crash path. Both have cost real data: parts clobbered by a second
              run's identical names, and a resumed run declaring a zero-part
              manifest over parquet already on disk.
  store       local | s3 | gcs. The object stores are not a formality — they
              have no rename, so a concurrent writer's staging collides where
              a filesystem's would not (found live, 3 of 6 rounds losing a
              manifest copy).
  state       sqlite | postgres. Two hand-written SQL implementations of one
              contract. Shape tracking never once worked on Postgres while
              every run reported success.
  engine      postgres | mysql | mssql | mongo.

WHAT EVERY CELL ASSERTS

Each stage is its own ledger row, so a chain that dies at `plan` does not leave
`apply` silently absent — it leaves it SKIP "not reached". Artifacts are checked
per stage: config, plan.json, parquet parts, manifest.json, `_SUCCESS`, the CDC
checkpoint, and the meta-DB rows on WHICHEVER backend the cell runs
(export_metrics, file_log, run_status, and for load: load_run,
loaded_source_run).

Values are read by DuckDB — a decoder rivet does not share. `validate` re-reads
with rivet's own code and `reconcile` asks the source; three different questions,
and a chain that answers one of them is not a verified chain.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass, field
from pathlib import Path

from . import blessed_path, cdc, scenarios
from .core import (
    run,
    Ledger,
    cell_gate,
    cell_parallel,
    container_for_port,
    docker_exec,
    have,
    port_of,
    rivet,
    rivet_bin,
)

#: The gate's own bucket, read from its config rather than named here. A private
#: name (`rivet-blessed`) is a bucket the bring-up never creates: every s3 cell
#: failed at `doctor` with an auth/reachability error that looked like a
#: credentials problem and was a missing bucket.
def _bucket(store: str) -> str:
    return scenarios.cfg("store", store, "bucket") or "rivet-oracle"

#: The golden seed's tables, smallest first so a failure surfaces cheaply.
TABLES = ("users", "orders")

#: Per-engine URL templates come from the gate's own matrix; only the stand's
#: ports live here.
STAND_PORT = {"postgres": 5432, "mysql": 3306, "mssql": 1433, "mongo": 27017}


@dataclass
class Cell:
    """One point of the cross-product, and the flags its chain will carry."""

    engine: str
    pipeline: str  # batch | cdc
    lifecycle: str  # clean | repeat | resume
    store: str  # local | s3 | gcs
    state: str  # sqlite | postgres
    table: str = "users"
    #: Flags exercised on this cell, per command. Declared rather than implied so
    #: the matrix can be read for flag coverage without running it.
    flags: dict[str, list[str]] = field(default_factory=dict)

    @property
    def label(self) -> str:
        return f"{self.pipeline}/{self.lifecycle}/{self.store}/{self.state}/{self.table}"

    def na_reason(self) -> str | None:
        """Why this cell cannot run, or None if it should.

        Stated per combination rather than filtered silently: a cross-product
        with invisible holes is the thing this module exists to replace.
        """
        if self.pipeline == "cdc" and self.engine == "mongo" and self.store != "local":
            return "mongo CDC to an object store is covered by the cdc_e2e cells; not duplicated here"
        if self.lifecycle == "resume" and self.pipeline == "cdc":
            return "CDC resume is checkpoint-driven and has its own two-run tests per engine (roast_*_resume)"
        if self.store != "local" and self.state == "sqlite":
            # One backend per store keeps the product honest without squaring it:
            # the store axis tests the WRITER, the state axis tests the LEDGER.
            return "store × state is not squared — object stores run on the postgres ledger only"
        return None


def cross_product(engines: list[str]) -> list[Cell]:
    """Every combination, `na` ones included — they are recorded, not dropped."""
    out: list[Cell] = []
    for eng in engines:
        for pipeline in ("batch", "cdc"):
            for lifecycle in ("clean", "repeat", "resume"):
                for store in ("local", "s3", "gcs"):
                    for state in ("sqlite", "postgres"):
                        out.append(
                            Cell(
                                engine=eng,
                                pipeline=pipeline,
                                lifecycle=lifecycle,
                                store=store,
                                state=state,
                                flags=_flags_for(len(out), pipeline, lifecycle, store),
                            )
                        )
    return out


def _rot(seed: str, n: int) -> int:
    """A stable rotation index from a cell's coordinates.

    NOT the enumeration index: `cell_ix % 3` over a nested cross-product
    aliases with the inner loops — the first probe run drew `--discover` for
    every postgres-backend cell and `--table` for every sqlite one, so two
    axes silently became one. A digest of the coordinates has no such period.
    """
    return int(hashlib.sha1(seed.encode()).hexdigest(), 16) % n


def _flags_for(cell_ix: int, pipeline: str, lifecycle: str, store: str) -> dict[str, list[str]]:
    """The flags a cell's chain carries.

    Distributed across cells rather than piled onto each, so the UNION covers
    every flag of every command in the chain while no single cell tests one
    combination over and over. `flag_coverage()` reports the union, and a flag
    that reaches no cell shows up there as absent — the same "hole in a grid
    rather than an unenumerated absence" the module is built on.

    Excluded deliberately, with reasons rather than by omission:
      --help / --json-errors   surface, not behaviour; `--json-errors` shapes
                               stderr on a path this chain does not take (it
                               asserts success, so there is no error to shape).
      --export                 the configs here declare ONE export, so a
                               selector cannot select anything different.
      --param                  belongs to the curated-query matrix, which owns
                               the substitution semantics.
    """
    f: dict[str, list[str]] = {"check": ["--strict"], "validate": ["--depth", "full"]}

    # init is the first stage of every chain and was being run with ONE of its
    # thirteen flags — the `verify_flag_surface` guard's first real finding.
    # `--source-env` is fixed (a URL on argv is visible in `ps`); the rest rotate.
    seed = f"{pipeline}/{lifecycle}/{store}/{cell_ix}"
    f["init"] = ["--source-env", "ORACLE_URL", "--mode", "cdc" if pipeline == "cdc" else "full"]
    if store == "s3":
        f["init"] += ["--s3-bucket", "BUCKET", "--s3-region", "us-east-1"]
    elif store == "gcs":
        f["init"] += ["--gcs-bucket", "BUCKET"]
    # `--discover` is incompatible with the bucket scaffolds, and --include /
    # --exclude are whole-schema only (they conflict with --table). So the
    # shapes rotate rather than stack — three ways to ask init for a config,
    # each of which a real user types.
    if store == "local" and _rot(seed + "init", 3) == 1:
        f["init"] += ["--discover"]
    elif _rot(seed + "init", 3) == 2:
        # --schema rides with the whole-schema shape: it is what makes
        # --include/--exclude address a schema at all, and on MySQL it names
        # the database.
        f["init"] += ["--schema", "SCHEMA", "--include", "GLOB",
                      "--exclude", "nothing_matches_this"]
    else:
        f["init"] += ["--table", "TABLE"]  # resolved per engine by the executor
    # `--tls disable` on half the cells (0.24.4, #146): loopback stands need no
    # TLS, but the flag must (a) be accepted, (b) write `tls: { mode: disable }`
    # into the scaffold, and (c) carry that block through check/plan/run — the
    # whole chain then exercises a config with an explicit posture, which is
    # what a trusted-VPC operator generates. verify-ca/verify-full cannot run
    # here: the compose stand terminates no TLS (see FLAG_EXCUSED --tls-ca).
    if _rot(seed + "tls", 2) == 1:
        f["init"] += ["--tls", "disable"]

    # check: --type-report / --json / --target rotate, so all three are carried
    # somewhere without three runs of `check` per cell.
    rot = _rot(seed + "check", 3)
    if rot == 0:
        f["check"] += ["--type-report"]
    elif rot == 1:
        f["check"] += ["--json"]
    else:
        f["check"] += ["--target", "bigquery"]

    # validate: --format json on half, and the addressing flags (--prefix,
    # --run-id, --date) are filled in by the caller once the run exists — a
    # run id typed before the run is a run id that matches nothing.
    if _rot(seed + "vfmt", 2) == 0:
        f["validate"] += ["--format", "json"]

    # validate's ADDRESSING flags (--prefix / --run-id / --date) name a run that
    # does not exist until the run happens, so they are declared here and filled
    # in by the executor from the artifacts. Declaring them as literals would be
    # a test of this module's guess at a run id.
    f["validate:addr"] = [["--prefix"], ["--run-id"], ["--date"]][_rot(seed + "addr", 3)]

    if pipeline == "batch":
        f["plan"] = ["--format", "json"]
        # `--annotate-waves` (0.24.4, #150) on a third of the batch cells: the
        # blessed config is init-generated IN this cell, so overwriting its
        # waves is safe by construction — and the cell then proves plan's
        # write-back path end to end. The OTHER two thirds prove the new
        # default: a plan run leaves the config's schedule untouched.
        if _rot(seed + "waves", 3) == 0:
            f["plan"] += ["--annotate-waves"]
        f["apply"] = []
        if lifecycle == "resume":
            # `--resume` on a tree that never crashed resumes nothing — the flag
            # is carried, the path is not. The executor crashes the first apply
            # with a fault hook and this second one is the real resume.
            f["apply"] = ["--resume"]
            f["apply:crash"] = ["after_file_write"]
        elif lifecycle == "repeat":
            # The second apply of a repeat cell carries --force; the first must
            # not, or the cell would stop being a repeat and become two clean
            # runs that happen to share a prefix.
            f["apply:second"] = ["--force"]
        if store == "local" and _rot(seed + "pep", 4) == 0:
            f["apply"] = f["apply"] + ["--parallel-export-processes"]
        # The reconcile leg is a `rivet run`, so it is where the batch chain
        # carries the rest of run's surface. `--force` and `--resume` are
        # mutually exclusive in meaning (redo everything vs continue), so they
        # rotate rather than stack.
        # The reconcile leg is a second `rivet run` over a prefix that already
        # has `_SUCCESS`, so `--force` is REQUIRED — rivet refuses otherwise,
        # by design and with a clear message ("re-running would overwrite a
        # verified dataset"). The first draft rotated `--resume` in here and
        # the refusal is what taught it: `--resume` belongs to the lifecycle
        # that actually crashed, not to a rotation over completed runs.
        f["reconcile"] = ["--reconcile", "--force"]
        f["reconcile"] += [
            [],
            ["--parallel-export-processes"],
            ["--parallel-exports"],
        ][_rot(seed + "rec", 3)]
    else:
        f["run"] = ["--validate", "--summary-output", "SUMMARY"]
        if lifecycle == "repeat":
            f["run"] += ["--reconcile"]
        if _rot(seed + "rjson", 3) == 0:
            f["run"] += ["--json"]
        if _rot(seed + "pexp", 5) == 0:
            f["run"] += ["--parallel-exports"]
    return f


# ── the chain ────────────────────────────────────────────────────────────────
#
# Declared as data so `batch` and `cdc` share ONE executor. The two differ in
# exactly one place — where batch does `plan → apply`, cdc does `run` — and a
# second executor would let them drift the way `dbo.orders` drifted when two
# files owned one table.
CHAIN = {
    "batch": ["init", "doctor", "check", "plan", "apply", "artifacts", "state", "oracle",
              "validate", "reconcile", "load", "load:cleanup"],
    "cdc":   ["init", "doctor", "check", "run", "artifacts", "state", "oracle",
              "validate", "load"],
}


def _why(p) -> str:
    """The line that explains a non-zero exit.

    `exit=1` in a 200-row report tells a reader to go re-run the cell by hand.
    rivet already prints the reason; carrying it into the ledger is the
    difference between a report and a to-do list.
    """
    for src in (p.stderr or "", p.stdout or ""):
        lines = [ln.strip() for ln in src.splitlines() if ln.strip()]
        for ln in reversed(lines):
            if ln.lower().startswith(("error", "caused by", "rivet: error")):
                return ln[:220]
        if lines:
            return lines[-1][:220]
    return ""


def _stage(led: Ledger, cell: Cell, tag: str, stage: str, ok: bool, detail: str) -> bool:
    """Record one stage. Every stage is its own row — a chain that dies at `plan`
    must leave `apply` as SKIP "not reached", never absent: in a 200-row report an
    absent cell reads as not-applicable."""
    name = f"flow:{stage}"
    msg = f"{cell.engine} {cell.label} · {stage}"
    if ok:
        led.passed(cell.engine, tag, name, cell.store, msg, detail)
    else:
        led.failed(cell.engine, tag, name, cell.store, f"{msg} — {detail}", detail)
    return ok


# Console-surface contract — the STABLE lines an operator reads at each stage,
# verified per CELL (engine × store × pipeline) inside the full blessed flow.
#
# This is the terminal-flow-in-the-logs guard. The GIF surface digest
# (release_oracle/gifs.py) fingerprints only `--help`, by design — runtime output
# like the `check` verdict block, the #149 Row-estimate SOURCE LABEL, and the
# bytes read/written run summary is invisible to it and can go stale silently.
# Here the gate already runs the real binary per engine and captures the
# transcript; asserting the surface lines turns that capture into a contract.
#
# Each entry: (stream, must-appear substring). Add a line to pin a new console
# surface — it is then checked for every engine × store the cell matrix covers.
_SURFACE: dict[str, list[tuple[str, str]]] = {
    # #149: the verdict block + the Row-estimate line (with its source label).
    "check": [("stdout", "Strategy:"), ("stdout", "Row estimate:"), ("stdout", "Verdict:")],
    # bytes_read surfacing: the read leg beside the write leg in the summary.
    "apply": [("stderr", "bytes read"), ("stderr", "bytes written")],
}
# A stage's surface applies only to these pipelines (absent ⇒ all). `apply` is
# batch-only; CDC's run summary does not carry a read-leg figure yet.
_SURFACE_PIPELINES: dict[str, set[str]] = {"apply": {"batch"}}


def _verify_surface(led: Ledger, cell: Cell, tag: str, stage: str, p) -> None:
    """Assert the stage's console surface, as its own `flow:<stage>:surface` row.

    Runs only on a SUCCEEDING stage — a failed stage's surface is the error, not
    the summary. A missing required line is a FAIL: the operator-visible output
    regressed even though the exit code did not (the class the GIF digest is
    blind to)."""
    reqs = _SURFACE.get(stage)
    if not reqs:
        return
    pipes = _SURFACE_PIPELINES.get(stage)
    if pipes is not None and cell.pipeline not in pipes:
        return
    # The `check` cell rotates `--json` on some cells (see _flags_for), which
    # SUPPRESSES the entire human verdict block (preflight/mod.rs gates it behind
    # `if !json_output`) and emits lowercase JSON keys instead. The human-surface
    # substrings (Strategy:/Row estimate:/Verdict:) legitimately do not appear
    # there, so this contract only applies to the HUMAN rendering — skip it when
    # the stage ran under --json (roast 2026-08-10: the contract failed every
    # --json check cell, an own-goal in #195). The JSON surface has its own
    # golden shape and is not what this operator-readability check is about.
    if "--json" in cell.flags.get(stage, []):
        return
    missing = [
        f"{stream}:{sub!r}"
        for stream, sub in reqs
        if sub not in ((p.stdout if stream == "stdout" else p.stderr) or "")
    ]
    name = f"flow:{stage}:surface"
    msg = f"{cell.engine} {cell.label} · {stage} console-surface"
    if missing:
        led.failed(cell.engine, tag, name, cell.store,
                   f"{msg} — MISSING {', '.join(missing)}", f"missing {len(missing)}")
    else:
        led.passed(cell.engine, tag, name, cell.store, msg,
                   f"{len(reqs)} surface line(s) present")


def _unreached(led: Ledger, cell: Cell, tag: str, after: str) -> None:
    steps = CHAIN[cell.pipeline]
    if after not in steps:
        return
    for s in steps[steps.index(after) + 1:]:
        led.skipped(cell.engine, tag, f"flow:{s}", cell.store,
                    f"{cell.engine} {cell.label} · {s} — not reached ({after} failed)",
                    f"unreached after {after}")


def _config(cell: Cell, work: Path, src_block: str, dest_block: str,
            cdc_block: str = "") -> Path:
    cfg = work / "rivet.yaml"
    if cell.pipeline == "cdc":
        body = (
            f"  - name: flow\n"
            f"    table: {cell.table}\n"
            f"    mode: cdc\n"
            f"    format: parquet\n"
            f"    {cdc_block}\n"
            f"{dest_block}\n"
        )
    else:
        body = (
            f"  - name: flow\n"
            f"    table: {cell.table}\n"
            f"    mode: full\n"
            f"    format: parquet\n"
            f"{dest_block}\n"
        )
    cfg.write_text(f"{src_block}\nexports:\n{body}")
    return cfg


def _state_sql(cell: Cell, work: Path, state_url: str, sql: str) -> int:
    """One scalar from whichever state backend this cell writes to, or -1."""
    if cell.state == "sqlite":
        import sqlite3
        db = work / ".rivet_state.db"
        if not db.is_file():
            return -1
        tmp = db.with_name(".read_state.db")
        # Read a COPY: `mode=ro` on a live WAL database fails to open, which
        # reads as "no state DB" when the file is right there.
        shutil.copy2(db, tmp)
        for side in ("-wal", "-shm"):
            sc = db.with_name(db.name + side)
            if sc.is_file():
                shutil.copy2(sc, tmp.with_name(tmp.name + side))
        try:
            row = sqlite3.connect(str(tmp)).execute(sql).fetchone()
            return int(row[0]) if row and row[0] is not None else 0
        except sqlite3.Error:
            return -1
    port = port_of(state_url)
    c = container_for_port(port) if port else None
    if not c:
        return -1
    db = state_url.rsplit("/", 1)[-1]
    out = docker_exec(c, "psql", "-U", "rivet", "-d", db, "-tA", "-c", sql).stdout.strip()
    head = out.splitlines()[0].strip() if out else ""
    return int(head) if head.lstrip("-").isdigit() else -1


def _state_watermark(cell: Cell, work: Path, state_url: str) -> int:
    """`max(id)` in `export_metrics` BEFORE the chain runs.

    The scoping problem, and why a name is not enough: every cell names its
    export `flow`, and the Postgres ledger is SHARED across cells and across
    days. `WHERE export_name='flow'` therefore counted 37 rows on a cell that
    wrote 3 — and would have counted 37 on a cell that wrote NONE. That is the
    unscoped-count defect this repo has paid for more than once, and it was
    sitting inside a function whose own docstring warned about it.

    A watermark is the fix rather than a timestamp because `export_metrics` has
    no `run_id` and comparing the host's clock to the database's is two clocks.
    `id > watermark` is neither.
    """
    if cell.state == "sqlite":
        return 0  # a fresh per-cell file; there is no prior history to exclude
    return max(0, _state_sql(cell, work, state_url,
                             "SELECT coalesce(max(id), 0) FROM export_metrics"))


def _meta_rows(cell: Cell, work: Path, state_url: str, mark: int,
               run_ids: list[str]) -> tuple[bool, str]:
    """Did THIS run record itself, on whichever backend this cell uses?

    Scoped three ways, one per table's own key: `export_metrics` by the
    watermark (it has no run_id), `file_log` and `run_status` by the run ids the
    manifests actually name. A count that a previous cell could satisfy is not
    evidence about this one.
    """
    ids = ", ".join(f"'{r}'" for r in run_ids) if run_ids else "''"
    # export_metrics has no run_id, so it is scoped by a watermark (id > mark). Under
    # CONCURRENT cells this can also count a sibling's row, so its ≥1 check is
    # best-effort: it can only ever produce a false PASS (never a false fail — the
    # count only inflates), and that is masked by the cell-scoped, sibling-proof
    # checks below (artifacts/oracle read THIS cell's own work-dir/prefix; file_log
    # and run_status are scoped by THIS cell's run_ids). The authoritative per-cell
    # signals are those; export_metrics ≥1 is corroboration.
    got = {
        "export_metrics": _state_sql(cell, work, state_url,
                                     f"SELECT count(*) FROM export_metrics "
                                     f"WHERE id > {mark} AND export_name = 'flow'"),
        "file_log": _state_sql(cell, work, state_url,
                               f"SELECT count(*) FROM file_log WHERE run_id IN ({ids})"),
        "run_status": _state_sql(cell, work, state_url,
                                 f"SELECT count(*) FROM run_status WHERE run_id IN ({ids})"),
    }
    missing = [t for t, n in got.items() if n < 1]
    return (not missing), (", ".join(f"{t}={n}" for t, n in got.items())
                           + f" (scoped: id>{mark}, {len(run_ids)} run id(s))")


def _run_ids(work: Path) -> list[str]:
    """The run ids of every run this cell performed.

    Read from `.rivet/runs/*/summary.json`, which rivet writes beside the CONFIG
    for every run regardless of where the data went. The first version read the
    destination manifest instead, which is correct for a local prefix and empty
    for every cloud one — so each s3/gcs cell scoped its ledger check to zero ids
    and reported `file_log=0` about a run that had just written files. A
    per-run record that lives with the config has no such asymmetry.
    """
    out = set()
    for f in sorted((work / ".rivet" / "runs").glob("*/summary.json")):
        try:
            rid = json.loads(f.read_text()).get("run_id")
        except (OSError, json.JSONDecodeError):
            continue
        if rid:
            out.add(str(rid))
    return sorted(out)


def _dest_block(cell: Cell, dest_dir: Path, prefix: str) -> str | None:
    if cell.store == "local":
        return f"    destination: {{type: local, path: {dest_dir}/}}"
    raw = scenarios.store_dest(cell.store, _bucket(cell.store), prefix)
    if not raw:
        return None
    # `store_dest` returns the CONTENT under `destination:`, already indented —
    # the key itself is the caller's to write.
    return "    destination:\n" + raw.rstrip("\n")


def _readback(cell: Cell, dest_dir: Path, prefix: str, work: Path) -> int:
    """Rows the destination DELIVERED, read by DuckDB and nothing else.

    Cloud prefixes are PULLED to a local directory first so that all three
    stores go through the SAME oracle — `_flow_rows`, which counts only parts a
    manifest declares committed. The alternative (a cloud-specific readback that
    counts everything under the prefix) is a second definition of "what was
    delivered", and it drifted immediately: the local path was fixed to ignore a
    crashed run's orphans and the cloud path went on counting them, so every
    resume cell on s3/gcs read 2000 rows from a 1000-row table. One oracle, or
    two that disagree about the crash you are trying to measure.
    """
    if cell.store == "local":
        return _flow_rows(dest_dir)
    pulled = _pull(cell.store, _bucket(cell.store), prefix, work)
    return _flow_rows(pulled) if pulled else -1


def _pull(store: str, bucket: str, prefix: str, work: Path) -> Path | None:
    """Bring a cloud prefix down whole — manifests included, not just parquet."""
    dl = work / f"pull_{store}"
    shutil.rmtree(dl, ignore_errors=True)
    dl.mkdir(parents=True, exist_ok=True)
    if store == "s3":
        if not have("mc"):
            return None
        alias = "rivetgate"
        run(["mc", "alias", "set", alias, "http://127.0.0.1:9000",
             scenarios.MINIO_ACCESS_KEY, scenarios.MINIO_SECRET_KEY])
        p = run(["mc", "cp", "--recursive", f"{alias}/{bucket}/{prefix}/", str(dl)])
        return dl if p.ok else None
    if store == "gcs":
        # The emulator's JSON API is enough, and it keeps the pull independent
        # of rivet — the same helper `store_readback` uses.
        # `--all`: the manifests are the point. The default mode pulls only
        # parquet and renames it `part_N.parquet`, which cannot answer "what did
        # this run declare".
        got = run([scenarios.PY, str(scenarios._asset("lib/gcs_pull.py")),
                   "http://127.0.0.1:4443", bucket, prefix, str(dl), "--all"]).stdout.strip()
        try:
            return dl if int(got) > 0 else None
        except ValueError:
            return None
    return None


def _flow_rows(path: Path) -> int:
    """Rows in the MANIFESTED dataset — not every parquet under the prefix.

    The difference is the whole point on a `resume` cell. A crash leaves parts
    on disk that no manifest declares; the resumed run then re-exports the
    range, so a blind `**/*.parquet` reads the table twice and calls it a
    doubling. It is not: those orphans are the `gc_orphans` case, and a run's
    delivery is what its manifest declares. Reading them as delivered was this
    module's own bug, and the repo has the rule in writing — raw artifacts under
    a failed prefix are not evidence about what the run delivered.
    """
    files = _manifest_files(path)
    if files is None:
        # No manifest: nothing was declared, so nothing was delivered. Counting
        # the parquet anyway would report a failed run's leftovers as output.
        return -1
    if not files:
        return 0
    lst = ", ".join(f"'{f}'" for f in files)
    got = scenarios._duckdb_list(f"SELECT count(*) FROM read_parquet([{lst}])")
    head = got.splitlines()[0].strip() if got else ""
    head = head.split("|")[0].split(",")[0].strip()
    return int(head) if head.lstrip("-").isdigit() else -1


def _manifest_files(prefix: Path) -> list[str] | None:
    """Absolute paths of the parts the manifest DECLARES, or None if unmanifested.

    On a `repeat` cell there are several manifest copies (one per run, immutable)
    plus the canonical last-writer-wins pointer. The union across the COPIES is
    the readable dataset — reading only `manifest.json` would report the last
    run's parts as the whole, which is exactly how a sidecar clobber stays
    invisible.
    """
    copies = sorted(prefix.rglob("manifest-*.json"))
    docs = copies or ([prefix / "manifest.json"] if (prefix / "manifest.json").is_file() else [])
    if not docs:
        return None
    out: list[str] = []
    for d in docs:
        try:
            art = json.loads(d.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        for f in art.get("parts", []) or []:
            # A part the manifest lists but marks non-committed is not delivered
            # data; counting it would report an in-flight row as an outcome.
            if isinstance(f, dict) and f.get("status") not in (None, "committed"):
                continue
            name = f.get("path") or f.get("name") if isinstance(f, dict) else f
            if not name:
                continue
            cand = Path(name)
            if not cand.is_absolute():
                cand = d.parent / cand
            if cand.is_file():
                out.append(str(cand))
    return sorted(set(out))


def run_cell(led: Ledger, cell: Cell, url: str, state_url: str, tag: str = "live") -> None:
    """Walk one cell's chain, with the CDC fixture raised and torn down around it.

    The teardown is a `finally` because on PostgreSQL it drops a replication
    slot, and a leaked inactive slot pins WAL and consumes one of the stand's
    32. A morning's tests once leaked 25 of them and the resulting failures
    looked like three different bugs before the slot count explained all of
    them at once. A teardown that only runs on the happy path is a leak with
    extra steps.
    """
    # `tag` is the engine VERSION and arrives from the caller. It used to be
    # hard-coded to the literal "flow" here, which put "flow" in the ledger's
    # VERSION column for all four gridded postgres versions — and, once the
    # prefix started using it, silently undid that fix by shadowing the
    # parameter. The gate caught the inert fix on the very next run.
    slug = f"{cell.pipeline}_{cell.lifecycle}_{cell.store}_{cell.state}"
    work = scenarios.work_dir() / f"flow_{cell.engine}{tag}_{slug}_{cell.table}"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)

    cdc_block = ""
    if cell.pipeline == "cdc":
        # The CDC stands carry no golden seed — they are per-test fixtures. So
        # the cell seeds its own, through the SAME per-engine setup the cdc_e2e
        # matrix uses. A second seeder here is exactly the parallel harness the
        # rig rule exists to prevent: two definitions of "the CDC probe table"
        # drift, and the one that drifts is never the one you are reading.
        #
        # It happens BEFORE init, not after: init discovers the source, and a
        # table that does not exist yet is a table init cannot scaffold.
        spec = cdc._ENGINES.get(cell.engine)
        if spec is None:
            led.skipped(cell.engine, tag, "flow:chain", cell.store,
                        f"{cell.engine} {cell.label} — no cdc engine spec")
            return
        # Retried: SQL Server's fixture disables and re-enables CDC on the table,
        # and `sp_cdc_enable_table` can fail while the previous capture job is
        # still shutting down. Six of eight mssql cells failed that way — a race
        # in the fixture, reported as a source failure. The retry lives in
        # `cdc.setup_with_retry` — ONE definition, shared with verify_cdc_e2e, so
        # the preflight and engine-matrix setup paths cannot drift (they did:
        # verify_cdc_e2e retried nothing and went RED on this very race).
        blk = cdc.setup_with_retry(spec, url, work)
        if blk is None:
            _stage(led, cell, tag, "init", False, "cdc fixture setup failed on the source")
            _unreached(led, cell, tag, "init")
            return
        # A checkpoint is what makes the SECOND run resume rather than
        # re-anchor; the shared block omits it because cdc_e2e drives resume by
        # other means. Spliced into the inline table rather than appended, so
        # the engine's own slot/stream params stay exactly as that module set
        # them.
        ck = work / "cdc.ckpt"
        # Splice a checkpoint ONLY if the engine's own block has none. MySQL's
        # already carries one (it needs the anchor for its no-server-side-anchor
        # resume model); appending a second produced a duplicate key, the config
        # stopped parsing, and all eight mysql CDC cells died at `doctor` with
        # "config check failed" — a harness bug that reads exactly like a
        # product one.
        cdc_block = (blk.rstrip() if "checkpoint:" in blk
                     else blk.rstrip().rstrip("}").rstrip().rstrip(",") + f", checkpoint: {ck} }}")
        cell.table = _CDC_TABLE.get(cell.engine, cell.table)

    try:
        _run_chain(led, cell, url, state_url, work, cdc_block, tag)
    finally:
        if cell.pipeline == "cdc":
            cdc._ENGINES[cell.engine].cleanup(url, work)


def _run_chain(led: Ledger, cell: Cell, url: str, state_url: str, work: Path,
               cdc_block: str, tag: str = "live") -> None:
    """Walk one cell's whole chain, recording a row per stage."""
    # NOT re-bound here. `tag` is the engine version, passed in — a literal
    # "flow" at this line shadowed the parameter and made the version-scoped
    # prefix inert while looking correct at its definition. Twice, in two
    # functions, before the gate's readback said so out loud.
    slug = f"{cell.pipeline}_{cell.lifecycle}_{cell.store}_{cell.state}"
    dest_dir = work / "out"
    # The work root's name makes the prefix unique PER GATE INVOCATION. Without
    # it, an object-store prefix accumulates every previous run's parts and the
    # readback — which counts what is under the prefix — reads 3000 rows from a
    # 1000-row table across three invocations. The local path is immune because
    # it counts only manifested parts in a fresh tempdir; the cloud path had no
    # such filter, and the same defect came back one store over. Same shape as
    # the CDC leg's `oracle-cdc/<work-root>/…`.
    # The VERSION TAG belongs in the prefix, not just the engine. The gate runs
    # this matrix once per gridded version — postgres 13, 14, 16, 18 — and
    # `cell.engine` is "postgres" for all four, so they shared one object-store
    # prefix and the readback counted every earlier version's parts as this
    # one's: 450000 rows read from a 150000-row table (3x), then 750000 (5x) as
    # the run progressed. Caught by the gate itself on its first full pass.
    prefix = (f"flow/{scenarios.work_dir().name}/{cell.engine}{tag}/{slug}/"
              f"{cell.table.replace('.', '_')}")
    mark = 0

    cdc._export_store_creds()  # MINIO_/AZURITE_ keys the dest blocks name by *_env
    env = dict(scenarios._store_env(url))
    if cell.state == "postgres":
        if not state_url:
            led.skipped(cell.engine, tag, "flow:chain", cell.store,
                        f"{cell.engine} {cell.label} — postgres backend needs --state-url")
            return
        env["RIVET_STATE_URL"] = state_url
        mark = _state_watermark(cell, work, state_url)
    else:
        # An inherited RIVET_STATE_URL would silently make every "sqlite" cell a
        # second Postgres cell — the axis would read as covered and test one
        # backend twice.
        env["RIVET_STATE_URL"] = ""

    # ── init ─────────────────────────────────────────────────────────────────
    gen = work / "init.yaml"
    args = _resolve_init(cell)
    disco = ""
    if "--discover" in args:
        # `--discover` writes a SURVEY (JSON: per-table row estimates, cursor
        # candidates, a suggested_mode) — not a config, and it says so, noting
        # that `--mode` is ignored. So this cell does what an operator does:
        # survey first, then scaffold. Grading the survey as if it were a config
        # was this module's own bug, found by running it.
        rep = work / "discover.json"
        d = rivet("init", *args, "-o", str(rep), env=env, timeout=scenarios.NO_TIMEOUT)
        try:
            art = json.loads(rep.read_text()) if rep.is_file() else {}
        except json.JSONDecodeError:
            art = {}
        tables = art.get("tables") or []
        okd = d.ok and bool(tables) and all("suggested_mode" in t for t in tables)
        disco = f"discover: {len(tables)} tables surveyed · "
        if not _stage(led, cell, tag, "init:discover", okd,
                      f"exit={d.returncode} tables={len(tables)} "
                      f"keys={sorted(art)[:5]}"):
            _unreached(led, cell, tag, "init")
            return
        args = [a for a in args if a != "--discover"]
        args += ["--table", _qualified(cell)]
    p = rivet("init", *args, "-o", str(gen), env=env, timeout=scenarios.NO_TIMEOUT)
    body = gen.read_text() if gen.is_file() else ""
    # What init's flags PRODUCED, not just that it exited 0. `--mode cdc` must
    # yield a cdc block, `--s3-bucket` an s3 destination, `--exclude` must not
    # have swallowed the table. A chain that only checks the exit code is
    # running init with thirteen flags and grading one.
    want_mode = "mode: cdc" if cell.pipeline == "cdc" else "mode:"
    shape = [
        ("exports", "exports:" in body),
        ("mode", want_mode in body),
        ("table", cell.table in body),
    ]
    if cell.store in ("s3", "gcs"):
        shape.append((f"{cell.store} dest", f"type: {cell.store}" in body))
    bad = [n for n, okk in shape if not okk]
    ok = p.ok and not bad
    detail = (f"{disco}{' '.join(args)} → {len(body.splitlines())} lines"
              if ok else f"exit={p.returncode} missing={bad or 'file'} {_why(p)}")
    if not _stage(led, cell, tag, "init", ok, detail):
        _unreached(led, cell, tag, "init")
        return

    db = _dest_block(cell, dest_dir, prefix)
    if db is None:
        led.skipped(cell.engine, tag, "flow:chain", cell.store,
                    f"{cell.engine} {cell.label} — no destination block for {cell.store}")
        return
    tls = "\n  tls: {accept_invalid_certs: true}" if cell.engine == "mssql" else ""
    src = f"source:\n  type: {cell.engine}\n  url_env: ORACLE_URL{tls}"
    cfg = _config(cell, work, src, db, cdc_block)

    # ── doctor / check ───────────────────────────────────────────────────────
    for st in ("doctor", "check"):
        args = [st, "-c", str(cfg), *cell.flags.get(st, [])]
        p = rivet(*args, env=env, timeout=scenarios.NO_TIMEOUT)
        # Read-only source probes; a transient connection blip under the parallel
        # matrix is a false alarm — retry like seed_engine (a real failure fails
        # every attempt; _why below still surfaces an exhausted one).
        for _ in range(2):
            if p.ok:
                break
            time.sleep(1.5)
            p = rivet(*args, env=env, timeout=scenarios.NO_TIMEOUT)
        if not _stage(led, cell, tag, st, p.ok, f"exit={p.returncode} " + (p.out.strip().splitlines()[-1][:120] if p.ok and p.out.strip() else _why(p))):
            _unreached(led, cell, tag, st)
            return
        # Console-surface: the check verdict block (per engine × store × pipeline).
        _verify_surface(led, cell, tag, st, p)

    # ── the work: plan → apply (batch) or run (cdc) ──────────────────────────
    if cell.pipeline == "batch":
        plan = work / "plan.json"
        p = rivet("plan", "-c", str(cfg), "-o", str(plan), *cell.flags.get("plan", []),
                  env=env, timeout=scenarios.NO_TIMEOUT)
        # A plan artifact is a CONTRACT `apply` must still accept — the 0.7.5
        # fixtures asserted its JSON had certain keys for two months while
        # `apply` rejected every one of them. So the assertion here is that the
        # file parses AND names the export; `apply`, next stage, is the real
        # reader.
        art = json.loads(plan.read_text()) if plan.is_file() else {}
        ok = p.ok and bool(art) and "flow" in json.dumps(art)
        if not _stage(led, cell, tag, "plan", ok,
                      f"exit={p.returncode} plan.json={plan.is_file()} keys={sorted(art)[:6]}"):
            _unreached(led, cell, tag, "plan")
            return

        crash_note = ""
        if cell.lifecycle == "resume":
            # A resume test whose first run SUCCEEDED is testing a clean re-run
            # wearing a `--resume` flag. The fault hook is what makes the second
            # apply resume something: it kills the process after a part is
            # durable but before the manifest records it.
            hook = cell.flags.get("apply:crash", ["after_file_write"])[0]
            c = rivet("apply", str(plan), env={**env, "RIVET_TEST_PANIC_AT": hook},
                      timeout=scenarios.NO_TIMEOUT)
            left = sorted(dest_dir.rglob("*.parquet")) if cell.store == "local" else []
            # A crash that changed nothing leaves the resume with nothing to
            # resume — the fixture would be inert, which is the defect class
            # this whole gate exists to catch. So it is asserted, not assumed.
            if c.ok:
                _stage(led, cell, tag, "apply:crash", False,
                       f"fault hook {hook} did not stop the run (exit=0) — "
                       "the resume below would be a clean re-run, not a resume")
                _unreached(led, cell, tag, "apply")
                return
            crash_note = f"crashed at {hook} (exit={c.returncode}, {len(left)} part(s) left) → "

        p = rivet("apply", str(plan), *cell.flags.get("apply", []),
                  env=env, timeout=scenarios.NO_TIMEOUT)
        applied = p.ok
        if cell.lifecycle == "repeat" and applied:
            # The scheduler's real shape: a second run into the SAME prefix. The
            # union of both runs must be readable — a part name that carries only
            # a per-run sequence silently overwrites the first run's output, and
            # the data is gone from the destination while both runs report
            # success.
            p2 = rivet("apply", str(plan), *cell.flags.get("apply:second", []),
                       env=env, timeout=scenarios.NO_TIMEOUT)
            applied = p2.ok
        if not _stage(led, cell, tag, "apply", applied, f"{crash_note}exit={p.returncode} {_why(p) if not applied else ''}"):
            _unreached(led, cell, tag, "apply")
            return
        # Console-surface: the run summary's read/write byte lines (batch apply).
        _verify_surface(led, cell, tag, "apply", p)
    else:
        # Anchor first (an empty run that pins the position), THEN write the
        # changes, THEN capture. A capture run with no prior anchor tests the
        # first-open path only; the idle-first-run variant is where MySQL once
        # lost a change outright.
        rivet("run", "-c", str(cfg), env=env, timeout=scenarios.NO_TIMEOUT)
        cdc._ENGINES[cell.engine].changes(url)
        # `--summary-output` is resolved against the CWD, so a bare filename
        # writes into whatever directory the gate happens to run from — it left
        # a `summary.json` in the repo root. The placeholder is filled here,
        # where the cell's work dir is known.
        args = ["run", "-c", str(cfg),
                *[str(work / "run-summary.json") if a == "SUMMARY" else a
                  for a in cell.flags.get("run", [])]]
        p = rivet(*args, env=env, timeout=scenarios.NO_TIMEOUT)
        ran = p.ok
        if cell.lifecycle == "repeat" and ran:
            p2 = rivet(*args, env=env, timeout=scenarios.NO_TIMEOUT)
            ran = p2.ok
        if not _stage(led, cell, tag, "run", ran, f"exit={p.returncode} {_why(p)}"):
            _unreached(led, cell, tag, "run")
            return

    # ── artifacts ────────────────────────────────────────────────────────────
    #
    # What a consumer finds on disk, not what the run said it did. The manifest
    # is read for its declared totals; the parquet is counted by DuckDB two
    # stages down. A run reporting success over a zero-part manifest is exactly
    # the shape this separation catches.
    if cell.store == "local":
        man = dest_dir / "manifest.json"
        parts = sorted(dest_dir.rglob("*.parquet"))
        copies = sorted(dest_dir.rglob("manifest-*.json"))
        succ = list(dest_dir.rglob("_SUCCESS"))
        art = json.loads(man.read_text()) if man.is_file() else {}
        ok = bool(parts) and bool(art)
        detail = (f"parts={len(parts)} manifest={man.is_file()} "
                  f"copies={len(copies)} _SUCCESS={len(succ)}")
        if cell.lifecycle == "repeat":
            # Two runs into one prefix must leave TWO immutable manifest copies.
            # Asserting on the parquet instead is what hid the sidecar clobber:
            # the data survives a clobbered manifest, which is precisely what
            # makes the clobber invisible.
            ok = ok and len(copies) >= 2
            detail += " (repeat wants >=2 manifest copies)"
    else:
        # No local mirror to walk; the store's own client answers at `oracle`.
        ok, detail = True, f"{cell.store} — artifacts asserted at the store by the oracle stage"
    if not _stage(led, cell, tag, "artifacts", ok, detail):
        _unreached(led, cell, tag, "artifacts")
        return

    if cell.pipeline == "cdc":
        ck = work / "cdc.ckpt"
        # A CDC run that captured but wrote no checkpoint resumes from a NEWER
        # position next cycle and silently skips everything between — the
        # anchor, not the capture, is what makes the second run correct.
        if not _stage(led, cell, tag, "artifacts:checkpoint", ck.is_file(),
                      f"checkpoint={ck.is_file()} bytes={ck.stat().st_size if ck.is_file() else 0}"):
            _unreached(led, cell, tag, "artifacts")
            return

    # ── state (meta DB) ──────────────────────────────────────────────────────
    ids = _run_ids(work)
    ok, detail = _meta_rows(cell, work, state_url, mark, ids)
    if not _stage(led, cell, tag, "state", ok, f"[{cell.state}] {detail}"):
        _unreached(led, cell, tag, "state")
        return

    # ── oracle: DuckDB, and only DuckDB ──────────────────────────────────────
    if not have("duckdb"):
        led.skipped(cell.engine, tag, "flow:oracle", cell.store,
                    f"{cell.engine} {cell.label} · oracle — no duckdb")
    else:
        duck = _readback(cell, dest_dir, prefix, work)
        if cell.pipeline == "batch":
            want = blessed_path._source_rows(cell.engine, url, cell.table)
            if want < 0:
                led.skipped(cell.engine, tag, "flow:oracle", cell.store,
                            f"{cell.engine} {cell.label} · oracle — source unreachable for a count")
            else:
                # `repeat` writes the same table twice into one prefix, so the
                # readable dataset is the UNION: 2x. That is the assertion — a
                # clobbered second run reads back 1x and looks like a clean run.
                mult = 2 if cell.lifecycle == "repeat" else 1
                # Per-column null profile: a decode/write regression can null a WHOLE
                # column while the row count matches (the documented uuid→FixedSizeBinary
                # silent loss — which happened on a CLOUD run). Count-parity is blind to
                # it. Local reads the parts on disk; cloud reads the copy _readback ALREADY
                # pulled to work/pull_<store> — so every store gets the same oracle, not
                # just local (the GCS store is exactly where the real incident occurred).
                prof = dest_dir if cell.store == "local" else (work / f"pull_{cell.store}")
                nn, _nc = scenarios.duckdb_allnull_columns(f"{prof}/**/*.parquet")
                n_null = max(0, nn)
                _stage(led, cell, tag, "oracle", duck == want * mult and n_null == 0,
                       f"duckdb={duck} source={want}x{mult}"
                       + (f" · ALLNULL {n_null} cols" if n_null else ""))
        else:
            # A change stream is not the table: the count is of CHANGES. But the
            # change set here is the SHARED, DETERMINISTIC cdc.changes() — cdc.py's
            # e2e proves it is exactly CDC_EXPECT_EVENTS across every engine. So the
            # floor is at-least-once: fewer than that means capture DROPPED a change
            # (the old `duck > 0` passed with 4 of 5 lost). >= tolerates an
            # at-least-once duplicate; < is a real loss.
            want = cdc.CDC_EXPECT_EVENTS
            _stage(led, cell, tag, "oracle", duck >= want,
                   f"duckdb={duck} >= {want} deterministic changes (at-least-once floor)")

    # ── validate ─────────────────────────────────────────────────────────────
    p = rivet("validate", "-c", str(cfg), *cell.flags.get("validate", []),
              env=env, timeout=scenarios.NO_TIMEOUT)
    ok, detail = p.ok, f"exit={p.returncode}" + ("" if p.ok else f" {_why(p)}")
    # Second pass, ADDRESSED. `validate` with no selector validates the latest
    # run; the addressing flags are how an operator checks yesterday's output,
    # and a flag that resolves nothing exits 0 on some paths — so the value
    # comes from the artifact, never from a guess.
    addr = cell.flags.get("validate:addr", [])
    extra = _addressing(cell, addr, dest_dir, prefix, work)
    if ok and extra:
        p2 = rivet("validate", "-c", str(cfg), *extra, env=env, timeout=scenarios.NO_TIMEOUT)
        ok = p2.ok
        detail += f" · addressed {' '.join(extra)} exit={p2.returncode}"
    elif ok:
        detail += f" · addressed {addr[0] if addr else '-'} SKIPPED (no value in artifacts)"
    if not _stage(led, cell, tag, "validate", ok, detail):
        _unreached(led, cell, tag, "validate")
        return

    # ── reconcile (batch only — a change stream has no row-count identity) ───
    if cell.pipeline == "batch":
        p = rivet("run", "-c", str(cfg), *cell.flags.get("reconcile", ["--reconcile"]),
                  env=env, timeout=scenarios.NO_TIMEOUT)
        if not _stage(led, cell, tag, "reconcile", p.ok, f"exit={p.returncode} {_why(p)}"):
            _unreached(led, cell, tag, "reconcile")
            return

    # ── load ─────────────────────────────────────────────────────────────────
    _load_leg(led, cell, tag, work, env, url, state_url, mark)


def _load_leg(led: Ledger, cell: Cell, tag: str, work: Path, env: dict, url: str,
              state_url: str, mark: int) -> None:
    """Export to REAL GCS, load into BigQuery, verify, clean up — then assert the
    LEDGER recorded it.

    Not run against fake-gcs, and the reason is not fastidiousness: a warehouse
    cannot read an emulator's bucket, so a `load` cell pointed there would be
    asserting on an error. The store axis proves rivet SPEAKS the protocol; this
    proves a reader that shares no code with rivet can decode the result.

    What this adds over the sibling bq cycle is the ledger half: `load_run` and
    `loaded_source_run`. The second is what makes a re-load idempotent — it is
    the link back to the extract run whose parts were consumed — and nothing
    asserted it existed.
    """
    proj = os.environ.get("BQ_ORACLE_PROJECT") or run(
        ["gcloud", "config", "get-value", "project"]).stdout.strip()
    bucket = os.environ.get("BQ_ORACLE_BUCKET", "rivet_data_test")
    # Per ENGINE, because `rivet load` derives the warehouse table from `table:`
    # — so all four engines' `users` land on ONE table. rivet caught this itself
    # and refused ("last loaded from `postgres:users`"), which is the guard doing
    # its job; nine cells failed on a collision the matrix created.
    # PER CELL, not just per engine. rivet load derives the warehouse table from
    # `table:`, so every cell's `users` lands on ONE table; under the PARALLEL matrix
    # concurrent cells then drop the table each other is querying (RED-proven: "Not
    # found: rivet_blessed_mssql.users"). A per-cell dataset gives each its own
    # `{dset}.users`. Only ~6 slugs × N engines distinct names, reused every run (bq
    # mk -f is idempotent), so this does not accumulate datasets.
    _cell_slug = f"{cell.lifecycle}_{cell.store}_{cell.state}"
    dset = (os.environ.get("BQ_ORACLE_DATASET", "rivet_blessed")
            + f"_{cell.engine}_{_cell_slug}")
    if cell.store != "gcs" or cell.pipeline != "batch":
        led.skipped(cell.engine, tag, "flow:load", cell.store,
                    f"{cell.engine} {cell.label} · load — the warehouse leg runs on the "
                    "batch/gcs cells; a change stream has no batch load and the other "
                    "stores are covered by their own readback")
        return
    if not have("bq") or not proj:
        led.skipped(cell.engine, tag, "flow:load", cell.store,
                    f"{cell.engine} {cell.label} · load — no `bq` CLI or no project "
                    "(set BQ_ORACLE_PROJECT); the emulator cannot stand in, a warehouse "
                    "must read the real bucket")
        return

    tbl = cell.table.split(".")[-1]
    # Version-scoped for the same reason the readback prefix is: the gate runs
    # this once per gridded version and `cell.engine` does not distinguish them.
    pfx = f"flow/{cell.engine}{tag}/{cell.pipeline}/{_cell_slug}/{tbl}"
    lcfg = work / "load.yaml"
    tls = "\n  tls: {accept_invalid_certs: true}" if cell.engine == "mssql" else ""
    lcfg.write_text(
        f"source:\n  type: {cell.engine}\n  url_env: ORACLE_URL{tls}\n"
        # The warehouse table is derived from `table:`, not from the export name.
        f"exports:\n  - name: {tbl}\n    table: {cell.table}\n"
        f"    mode: full\n    format: parquet\n"
        f"    destination: {{type: gcs, bucket: {bucket}, prefix: {pfx}/}}\n"
        f"load:\n  target: bigquery\n  project: {proj}\n  dataset: {dset}\n"
    )
    # Clear both sides first. A leftover table makes the next count a union of
    # two loads, and a gate that accumulates state stops measuring the run in
    # front of it.
    run(["bq", "--project_id", proj, "mk", "-f", "--dataset", f"{proj}:{dset}"])
    run(["bq", "--project_id", proj, "rm", "-f", "-t", f"{proj}:{dset}.{tbl}"])
    run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])

    e = rivet("run", "-c", str(lcfg), env=env, timeout=scenarios.NO_TIMEOUT)
    if not e.ok:
        _stage(led, cell, tag, "load", False, f"export to real gcs failed: {_why(e)}")
        return
    # Unique per cell: the id is what the ledger check scopes on, and a shared
    # one would let one cell's rows satisfy another's assertion.
    load_id = (
        f"flow-{cell.engine}-{cell.lifecycle}-{cell.state}-{scenarios.work_dir().name}"
    )
    p = rivet("load", "-c", str(lcfg), "--rivet-bin", str(rivet_bin()),
              "--run-id", load_id, env=env, timeout=scenarios.NO_TIMEOUT)
    if not p.ok:
        _stage(led, cell, tag, "load", False, f"exit={p.returncode} {_why(p)}")
        run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])
        return

    # BigQuery's own count, decoded by Google's parquet reader.
    q = run(["bq", "--project_id", proj, "query", "--nouse_legacy_sql", "--format", "csv",
             f"SELECT count(*) FROM `{proj}.{dset}.{tbl}`"]).stdout.strip().splitlines()
    got = int(q[-1]) if q and q[-1].strip().isdigit() else -1
    want = blessed_path._source_rows(cell.engine, url, cell.table)
    lok, ldetail = _load_rows(cell, work, state_url, load_id, tbl)
    _stage(led, cell, tag, "load", got == want and got > 0 and lok,
           f"bigquery={got} source={want} · ledger {ldetail}")

    # Cleanup is part of the cycle. `bq rm -f` exits 0 whether or not the table
    # existed, so its exit code is not evidence — the drop is verified by asking
    # for the table afterwards.
    run(["bq", "--project_id", proj, "rm", "-f", "-t", f"{proj}:{dset}.{tbl}"])
    gone = run(["bq", "--project_id", proj, "show", "-t", f"{proj}:{dset}.{tbl}"])
    run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])
    _stage(led, cell, tag, "load:cleanup", not gone.ok,
           "warehouse table dropped (verified by a show that fails)"
           if not gone.ok else "the table is STILL THERE after rm — the next run's count "
           "would be a union of two loads")


def _load_rows(cell: Cell, work: Path, state_url: str, load_id: str,
               tbl: str) -> tuple[bool, str]:
    """Did THIS load record itself, and does the link back to the extract exist?

    Scoped by the `--run-id` the load was given, which becomes its `load_id`.
    The first version counted the tables whole and reported `load_run=21` in
    every one of twelve cells — the same unscoped-history defect already fixed
    for `export_metrics`/`file_log`/`run_status`, left standing one function
    over. A count that twelve different cells agree on is not measuring any of
    them.

    `loaded_source_run` is the half that matters most: it is the link back to
    the extract run whose parts were consumed, and without it a re-load
    double-counts.
    """
    # PREFIX match, not equality: the stored id is `<--run-id>:<export_name>`.
    # One `rivet load` over N exports records N rows, and the suffix is what
    # tells them apart — so the correlation id on the CLI is a prefix of every
    # row it produced, never equal to one. An exact match found zero rows on a
    # load that had recorded four correct ones.
    like = f"{load_id}:%"
    got = {
        "load_run": _state_sql(cell, work, state_url,
                               f"SELECT count(*) FROM load_run WHERE load_id LIKE '{like}'"),
        "loaded_source_run": _state_sql(
            cell, work, state_url,
            f"SELECT count(*) FROM loaded_source_run WHERE load_id LIKE '{like}'"),
    }
    missing = [t for t, n in got.items() if n < 1]
    return (not missing), ", ".join(f"{t}={n}" for t, n in got.items()) + f" (load_id={load_id})"


#: Where a run leaves its per-CELL verdicts. The stage rows go to the gate's
#: ledger; this is the coarser record a re-run needs — which CELLS to redo.
VERDICTS = "blessed-flow-verdicts.json"


def _verdict_path(engine: str | None = None) -> Path:
    base = Path(os.environ.get("RIVET_FLOW_VERDICTS", scenarios.work_dir() / VERDICTS))
    # PER ENGINE. engine_loop runs the four engines' sc_blessed_flow concurrently, and
    # each read-modify-overwrites this file under its OWN per-engine lock — so a shared
    # path means the last engine to write CLOBBERS the others' resume anchors (a crashed
    # matrix could then re-run only one engine's cells). A file per engine keeps each
    # engine's incremental verdicts independent; failed_cells() unions them back.
    if engine:
        return base.with_name(f"{base.stem}.{engine}{base.suffix}")
    return base


def failed_cells(path: Path | None = None) -> set[str]:
    """The cell keys a previous run failed, for `only=`.

    The matrix is ~15 minutes; re-running all 76 cells to re-check four is how a
    feedback loop gets long enough that people stop closing it. Unions the per-engine
    verdict files (see _verdict_path) plus any legacy shared file.
    """
    if path is not None:
        paths = [path]
    else:
        base = _verdict_path()
        paths = [base, *sorted(base.parent.glob(f"{base.stem}.*{base.suffix}"))]
    out: set[str] = set()
    for p in paths:
        try:
            out |= {k for k, v in json.loads(p.read_text()).items() if v == "fail"}
        except (OSError, json.JSONDecodeError):
            continue
    return out


def sc_blessed_flow(led: Ledger, engine: str, tag: str, url: str,
                    state_url: str = "", cdc_url: str = "",
                    only: set[str] | None = None) -> None:
    """Run this engine's column of the matrix, recording every cell.

    Per engine×version rather than once for all engines, because that is the
    shape the gate already has: `engine_loop` brings ONE engine up at a time and
    tears it down, so a signature taking a list of engines could only ever be
    called from a place where three of the four are down. An `na` cell is a
    ledger SKIP carrying its reason, so the report shows the grid's holes rather
    than leaving them to be inferred from what is absent.

    `only` restricts the run to the named cell keys (see `failed_cells`). A cell
    outside the set is recorded SKIP with that reason, not omitted — a partial
    re-run that silently prints fewer rows is a report that looks like a smaller
    matrix.
    """
    led.phase(f"blessed flow · {engine} {tag} — whole journey × pipeline × lifecycle × store × state"
              + (f" (rerunning {len(only)} cell(s))" if only else ""))
    cells = cross_product([engine])
    runnable = [c for c in cells if c.na_reason() is None]
    led.ok(f"blessed flow[{engine}]: {len(cells)} cells declared, {len(runnable)} runnable, "
           f"{len(cells) - len(runnable)} na")

    verdicts: dict[str, str] = {}
    vp = _verdict_path(engine)  # per-engine: concurrent engines must not clobber
    try:
        verdicts = json.loads(vp.read_text())
    except (OSError, json.JSONDecodeError):
        verdicts = {}

    cdc_url = cdc_url or os.environ.get(f"RIVET_CDC_{engine.upper()}_URL", "")
    # Skips are decided SEQUENTIALLY (no rivet, just a ledger row, kept in
    # declaration order); only the cells that actually run a chain go through the
    # pool, so no-op work never occupies a slot.
    tasks: list[tuple[Cell, str, str]] = []
    for cell in cells:
        key = f"{engine}/{cell.pipeline}/{cell.lifecycle}/{cell.store}/{cell.state}"
        why = cell.na_reason()
        if why:
            led.skipped(cell.engine, tag, f"flow:{cell.pipeline}/{cell.lifecycle}",
                        cell.store, f"{cell.engine} {cell.label} — na: {why}", "na")
            continue
        if only is not None and key not in only:
            led.skipped(cell.engine, tag, "flow:chain", cell.store,
                        f"{cell.engine} {cell.label} — not in this re-run's cell set")
            continue
        u = cdc_url if cell.pipeline == "cdc" else url
        if not u:
            led.skipped(cell.engine, tag, "flow:chain", cell.store,
                        f"{cell.engine} {cell.label} — no RIVET_CDC_{engine.upper()}_URL "
                        "(the batch stand has no logical-decoding/CDC setup)")
            continue
        if cell.state == "postgres" and not state_url:
            led.skipped(cell.engine, tag, "flow:chain", cell.store,
                        f"{cell.engine} {cell.label} — postgres ledger needs --state-url")
            continue
        if cell.store != "local" and not scenarios.store_up(cell.store):
            led.skipped(cell.engine, tag, "flow:chain", cell.store,
                        f"{cell.engine} {cell.label} — {cell.store} store not up")
            continue
        tasks.append((cell, key, u))

    if not tasks:
        return

    # Cells are independent (own work-dir + own destination prefix), so run them
    # concurrently. Each records into its OWN buffered child, so (a) its rows/spans
    # stay contiguous instead of interleaving with a sibling's, and (b) its pass/fail
    # is that child's alone — thread-safe, unlike counting FAILs on the shared ledger.
    # cell_gate() caps concurrency ACROSS engines (which also run in parallel). Flush +
    # verdict write happen under a lock as each cell completes, so a matrix killed
    # mid-run still leaves a resumable verdicts file — the property the per-cell write
    # has always guarded, preserved under parallelism.
    lock = threading.Lock()
    # cdc cells DESTRUCTIVELY set up the ONE shared source probe per engine
    # (drop/create/enable orc_cdc_probe), so they cannot run concurrently WITH EACH
    # OTHER on this engine — RED-proven: 12 "orc_cdc_probe does not exist" collisions.
    # Each engine has its own lock (sc_blessed_flow is per-engine), so cdc still
    # overlaps ACROSS the 4 engines; batch cells never take it.
    cdc_lock = threading.Lock()

    def run_one(task: tuple[Cell, str, str]) -> None:
        cell, key, u = task
        child = led.buffered_child()
        # Take the per-engine cdc lock BEFORE the global slot, so a queued cdc cell
        # does not hold a cell_gate() permit while it waits (that would starve batch
        # cells). Non-cdc cells use a no-op context and never serialise.
        serialize = cdc_lock if cell.pipeline == "cdc" else nullcontext()
        try:
            with serialize, cell_gate():
                with child.span(
                    f"cell {engine} flow {cell.pipeline}/{cell.lifecycle}/{cell.store}/{cell.state}"
                ):
                    run_cell(child, cell, u, state_url, tag)
        except Exception as e:  # a raising cell must not abort the whole matrix
            child.failed(cell.engine, tag, f"flow:{cell.pipeline}/{cell.lifecycle}",
                         cell.store, f"{cell.engine} {cell.label} — cell raised: {e}", "raised")
        failed = any(c.status.value == "FAIL" for c in child.cells)
        with lock:
            child.flush_into(led)
            verdicts[key] = "fail" if failed else "pass"
            try:
                vp.write_text(json.dumps(verdicts, indent=1, sort_keys=True))
            except OSError:
                pass

    workers = min(len(tasks), cell_parallel())
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(run_one, tasks))


def flag_coverage(cells: list[Cell]) -> dict[str, set[str]]:
    """Which flags the matrix actually exercises, per command.

    Reported alongside the run so a flag nobody carries is visible as an empty
    set rather than as an assumption.
    """
    out: dict[str, set[str]] = {}
    for c in cells:
        if c.na_reason():
            continue
        for cmd, fl in c.flags.items():
            out.setdefault(cmd, set()).update(f for f in fl if f.startswith("--"))
    return out


def _addressing(cell: Cell, addr: list[str], dest_dir: Path, prefix: str,
                work: Path) -> list[str]:
    """Turn a declared addressing flag into one carrying a REAL value.

    Returns [] when the artifacts do not supply one — the caller then records
    the skip in the stage detail rather than validating the latest run twice
    and calling it addressed coverage.
    """
    if not addr:
        return []
    flag = addr[0]
    man = (dest_dir / "manifest.json") if cell.store == "local" else None
    art = {}
    if man and man.is_file():
        try:
            art = json.loads(man.read_text())
        except json.JSONDecodeError:
            art = {}
    if flag == "--prefix":
        return ["--prefix", str(dest_dir)] if cell.store == "local" else []
    if flag == "--run-id":
        rid = art.get("run_id") or art.get("runId")
        return ["--run-id", str(rid)] if rid else []
    if flag == "--date":
        stamp = art.get("started_at") or art.get("completed_at") or ""
        day = str(stamp)[:10]
        return ["--date", day] if len(day) == 10 and day[4] == "-" else []
    return []


#: Flags the matrix deliberately does not carry, each with the reason. A flag
#: absent from BOTH this map and `flag_coverage()` fails `verify_flag_surface` —
#: which is the point: a new flag lands with no cell carrying it and the gate
#: says so, rather than the matrix quietly grading what its author remembered.
FLAG_EXCUSED = {
    "--help": "surface, not behaviour",
    "--json-errors": "shapes stderr on a failing path; this chain asserts success",
    "--config": "carried by every stage as -c",
    "--export": "the configs here declare ONE export — a selector cannot select differently",
    "--param": "owned by the curated-query matrix, which defines substitution",
    "--output": "carried positionally as -o by plan; validate's is the same writer",
    "--source": "a URL on argv is visible in `ps` — the gate uses --source-env on purpose",
    "--source-file": "the file form of --source; same reader, and --source-env is the one ops use",
    "--s3-region": "carried with --s3-bucket on the s3 cells",
    "--gcs-credentials-file": "fake-gcs takes no credentials; the real path is the bigquery cycle's",
    "--pool": "pool mode schedules a whole CONFIG's exports (multi-export, duration-ordered); "
              "the blessed cell chain applies a sealed SINGLE-export plan artifact, where --pool "
              "is a refusal by design. Carried INSIDE the gate by its own stage instead: "
              "scenarios.verify_pool_e2e drives tests/live/live_pool_toxiproxy.rs (bandwidth-"
              "capped multi-export pool, exact rows + the predicted-vs-actual makespan "
              "contract + resume defer-not-drop).",
    "--tls-ca": "verify-ca/full need a TLS-terminating server; the compose stand has none — the "
                "CA path is unit-pinned (scaffold_records_the_tls_posture…) and refused-with-"
                "disable is offline-tested",
    "--split": "splits a dominating export into range sub-exports UNDER --pool (needs a "
               "multi-export config, a measured dominating duration, and a source to probe "
               "the key span) — the sealed single-export plan cell cannot express it. Carried "
               "INSIDE the gate by scenarios.verify_pool_e2e, which runs the whole "
               "live_pool_toxiproxy module including "
               "pool_split_realizes_the_range_split_and_the_union_is_exact (prime → clear → "
               "--pool --split → DuckDB union exact, plus a vacuous-oracle guard that the "
               "split actually fired).",
}


def _cli_flags(cmd: str) -> set[str]:
    """The flags `rivet <cmd>` ACTUALLY has, parsed from its own help.

    Derived, never typed: a hand-written list grades only what its author knew,
    and the whole reason this module exists is that hand-curated coverage missed
    the axis that mattered.

    Only OPTION lines count — a line whose first token is a dash. clap's prose
    quotes flags too (`--validate`, `--type-report`), and scanning every token
    turned those into two phantom findings on the first run of this guard.
    """
    out = rivet(cmd, "--help").out
    got: set[str] = set()
    for line in out.splitlines():
        m = re.match(r"\s+(?:-\w,\s+)?(--[a-z0-9-]+)", line)
        if m:
            got.add(m.group(1))
    return got


def verify_flag_surface(led: Ledger) -> None:
    """Every flag of every command in the chain is carried, or excused by name."""
    led.phase("blessed flow · flag surface (derived from the CLI, not from a list)")
    cells = cross_product(["postgres"])
    cov = flag_coverage(cells)
    carried = {f for fl in cov.values() for f in fl}
    # `-c`, `-o` and load's two are carried by the executor rather than declared.
    carried |= {"--config", "--output", "--rivet-bin", "--run-id", "--prefix", "--date"}
    for cmd in ("plan", "apply", "run", "validate", "check", "load", "doctor", "init"):
        real = _cli_flags(cmd)
        if not real:
            led.failed("-", "flow", "flow:flags", cmd,
                       f"flag surface · `rivet {cmd} --help` produced no flags — cannot grade")
            continue
        gap = sorted(f for f in real if f not in carried and f not in FLAG_EXCUSED)
        if gap:
            led.failed("-", "flow", "flow:flags", cmd,
                       f"flag surface · `{cmd}` has flags no cell carries and no excuse names: "
                       f"{', '.join(gap)}", ",".join(gap))
        else:
            led.passed("-", "flow", "flow:flags", cmd,
                       f"flag surface · `{cmd}`: {len(real)} flags, all carried or excused")


#: What `--schema` means per engine. MySQL has no schema layer, so the flag
#: names the DATABASE; Mongo has neither and takes no `--schema` at all.
INIT_SCHEMA = {"postgres": "public", "mssql": "dbo", "mysql": "rivet"}


def _qualified(cell: Cell) -> str:
    """The cell's table as `--table` wants it: schema-qualified where the engine
    has schemas, bare where it does not (MySQL puts the database in the URL)."""
    schema = INIT_SCHEMA.get(cell.engine)
    if not schema or cell.engine == "mysql" or "." in cell.table:
        # Already qualified (the mssql CDC fixture is `dbo.orc_cdc_probe`) or the
        # engine has no schema layer. Prepending anyway produced
        # `dbo.dbo.orc_cdc_probe`, and init reported a table that does not exist.
        return cell.table
    return f"{schema}.{cell.table}"


def _resolve_init(cell: Cell) -> list[str]:
    """Fill the per-engine placeholders in the cell's declared init flags.

    The flags are declared engine-agnostically so `flag_coverage` can read them
    without an engine; `public.users` vs `dbo.users` vs `users` is resolved here,
    at the one place that knows which engine is running.
    """
    schema = INIT_SCHEMA.get(cell.engine)
    qualified = _qualified(cell)
    out: list[str] = []
    skip_next = False
    for i, a in enumerate(cell.flags.get("init", [])):
        if skip_next:
            skip_next = False
            continue
        if a == "TABLE":
            out.append(qualified)
        elif a == "BUCKET":
            out.append(_bucket(cell.store))
        elif a == "GLOB":
            # Addresses THIS cell's table. A fixed `user*` matched the golden
            # seed and nothing on the CDC stands, whose only table is the
            # per-engine probe — init refused with a clear message and the
            # whole cdc column died at stage one.
            out.append(cell.table.split(".")[-1][:4] + "*")
        elif a == "SCHEMA":
            out.append(schema or "")
        elif a == "--schema" and not schema:
            # Mongo: no schema layer, so the flag and its value both go.
            skip_next = True
        else:
            out.append(a)
    return out


#: What each engine's shared CDC fixture calls its probe table. Read from the
#: setup helpers rather than re-declared: `cdc._cdc_postgres` creates
#: `orc_cdc_probe`, and a second name here would route the export at a table
#: the fixture never made.
_CDC_TABLE = {
    "postgres": "orc_cdc_probe",
    "mysql": "orc_cdc_probe",
    "mssql": "dbo.orc_cdc_probe",
    "mongo": "orc_cdc_probe",
}


# ── proving the chain can go red ─────────────────────────────────────────────
#
# A green stage that was never red is unverified. The repo has a 63-test audit
# behind that sentence, so this module carries its own inertness probe rather
# than asserting its sensitivity in prose.
#
# These perturb the ARTIFACTS, not the product — a product mutant needs a
# fat-LTO rebuild per mutant, and what is being graded here is whether the
# ORACLE bites, which an artifact-level break answers exactly. The product-level
# equivalents live in the mutant corpus and the fault hooks.
_PERTURB = [
    ("oracle", "a lost part", "delete one manifested parquet"),
    ("oracle", "a truncated dataset", "delete every parquet"),
    ("artifacts", "a lost manifest", "delete manifest.json and its copies"),
    ("state", "a ledger that recorded nothing", "scope to a run id that does not exist"),
]


def _bit(led: Ledger, engine: str, ok: bool, msg: str, why: str) -> None:
    """Record one sensitivity result. A probe that cannot fail is itself the
    defect being probed for, so the failing message names what stayed green."""
    if ok:
        led.passed(engine, "flow", "flow:inert", "local", msg)
    else:
        led.failed(engine, "flow", "flow:inert", "local", f"inertness · {why}", why)


def sc_not_inert(led: Ledger, engine: str, url: str, state_url: str) -> None:
    """Break each artifact class and require the matching stage to go RED."""
    led.phase("blessed flow · inertness probe (each oracle must fail when its subject is broken)")
    cell = Cell(engine=engine, pipeline="batch", lifecycle="clean", store="local",
                state="sqlite", flags=_flags_for(0, "batch", "clean", "local"))
    work = scenarios.work_dir() / f"inert_{engine}"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    base = Ledger()
    _run_chain(base, cell, url, state_url, work, "")
    if base.red:
        led.failed(engine, "flow", "flow:inert", "local",
                   "inertness probe · the baseline chain is not green — cannot grade sensitivity")
        return
    dest = work / "out"
    parts = sorted(dest.rglob("*.parquet"))
    if not parts:
        led.failed(engine, "flow", "flow:inert", "local",
                   "inertness probe · baseline wrote no parquet — the fixture is inert")
        return

    # 1. NEGATIVE control first, because the naive version of this probe got it
    #    backwards. The chain runs the export more than once (apply, then the
    #    reconcile leg), so several manifests declare several complete copies.
    #    Deleting ONE copy's part must NOT move the oracle — the union is still
    #    the whole table, and a probe that demands a failure there would be
    #    demanding a false alarm. This is what the first draft asserted, and it
    #    went red against a correct oracle.
    victim = parts[0]
    keep = victim.read_bytes()
    victim.unlink()
    got = _flow_rows(dest)
    want = blessed_path._source_rows(engine, url, cell.table)
    if want < 0:
        # An UNREADABLE source count cannot grade anything, and comparing against
        # the sentinel is worse than not comparing: on mongo 4.4 — whose image
        # ships no `mongosh`, so the count came back -1 — the negative control
        # read "the oracle moved to 150000" and reported a false alarm about a
        # false alarm. The `oracle` stage one layer up already SKIPs on this;
        # the probe must too.
        led.skipped(engine, "flow", "flow:inert", "local",
                    f"inertness · {engine} — source count unreadable, cannot grade sensitivity")
        victim.write_bytes(keep)
        return
    _bit(led, engine, got == want,
         f"inertness (negative control) · one redundant copy removed → oracle still reads "
         f"{got} == source {want}",
         f"the oracle moved to {got} when a REDUNDANT copy was removed — it would "
         f"raise a false alarm on any repeated run")
    victim.write_bytes(keep)

    # 1b. POSITIVE control: remove every declared part. Now nothing is delivered
    #     and the oracle must refuse to agree with the source.
    declared = _manifest_files(dest) or []
    saved_parts = {Path(f): Path(f).read_bytes() for f in declared}
    for f in saved_parts:
        f.unlink()
    got = _flow_rows(dest)
    _bit(led, engine, got != want,
         f"inertness · every declared part removed → oracle reads {got}, source {want}",
         f"the oracle read {got} with no parts on disk — it agrees with the source "
         f"about a dataset that is gone")
    for f, b in saved_parts.items():
        f.write_bytes(b)

    # 2. a lost manifest: an unmanifested prefix has delivered nothing, and the
    #    oracle must say so rather than counting the parquet lying next to it.
    mans = sorted(dest.rglob("manifest*.json"))
    saved = {m: m.read_bytes() for m in mans}
    for m in mans:
        m.unlink()
    got = _flow_rows(dest)
    _bit(led, engine, got < 0, f"inertness · no manifest → oracle reports {got}",
         "it counted undeclared parquet as delivered")
    for m, b in saved.items():
        m.write_bytes(b)

    # 3. a ledger that recorded nothing: the state stage must be scoped tightly
    #    enough that a run id which never ran cannot satisfy it. This is the
    #    exact check that was passing on 37 rows of unrelated history.
    ok, detail = _meta_rows(cell, work, state_url, 0, ["run-id-that-never-existed"])
    _bit(led, engine, not ok,
         f"inertness · a run id that never ran → state refuses ({detail})",
         f"the state stage accepted a run id that never ran ({detail})")


# ── the DAG, printed from the same data the executor walks ───────────────────
#: What each stage asserts and WHO answers it. The oracle column is the point of
#: the whole module: three different questions (rivet re-reads, DuckDB decodes,
#: the source counts) and a chain that answers one of them is not verified.
STAGE_ORACLE = {
    "init":            ("rivet init",     "the config's SHAPE (exports, mode, dest kind, table)"),
    "init:discover":   ("rivet init",     "the survey artifact: tables[] each with suggested_mode"),
    "doctor":          ("rivet doctor",   "source + destination auth, CDC slot hygiene"),
    "check":           ("rivet check",    "column types resolve; --strict admits no warning"),
    "plan":            ("rivet plan",     "plan.json parses and names the export"),
    "apply":           ("rivet apply",    "wave execution; resume cells crash FIRST (fault hook)"),
    "run":             ("rivet run",      "cdc capture after an anchor run + real changes"),
    "artifacts":       ("filesystem",     "parts, manifest.json, run-unique copies, _SUCCESS"),
    "artifacts:checkpoint": ("filesystem", "the CDC resume anchor exists and is non-empty"),
    "state":           ("state backend",  "export_metrics/file_log/run_status SCOPED to this run"),
    "oracle":          ("DuckDB",         "rows in MANIFEST-DECLARED parts vs the source's own count"),
    "validate":        ("rivet validate", "rivet re-reads its own output; then again ADDRESSED"),
    "reconcile":       ("rivet run",      "destination count vs a fresh source count"),
    "load":            ("BigQuery",       "a foreign reader decodes it; ledger has load_run"),
    "load:cleanup":    ("bq show",        "the table is really gone (rm -f exits 0 regardless)"),
}


def print_dag() -> str:
    """Render the matrix as a DAG. Built from CHAIN/axes, never hand-drawn."""
    L: list[str] = []
    cells = cross_product(["postgres", "mysql", "mssql", "mongo"])
    run_n = sum(1 for c in cells if c.na_reason() is None)
    L.append("blessed flow — the whole user journey as one cross-product")
    L.append("")
    L.append("  AXES                                                cells")
    L.append("  ────────────────────────────────────────────────────────")
    for name, vals in (("engine", ["postgres", "mysql", "mssql", "mongo"]),
                       ("pipeline", ["batch", "cdc"]),
                       ("lifecycle", ["clean", "repeat", "resume"]),
                       ("store", ["local", "s3", "gcs"]),
                       ("state", ["sqlite", "postgres"])):
        L.append(f"  {name:<10} {' × '.join(vals):<40} {len(vals):>3}")
    L.append(f"  {'':<10} {'=':<40} {len(cells):>3} declared, "
             f"{run_n} runnable, {len(cells) - run_n} na")
    L.append("")
    for pipe in ("batch", "cdc"):
        L.append(f"  {pipe.upper()} chain")
        L.append("  " + "─" * 74)
        steps = [s for s in CHAIN[pipe]]
        if pipe == "cdc":
            steps.insert(steps.index("artifacts") + 1, "artifacts:checkpoint")
        for i, st in enumerate(steps):
            who, what = STAGE_ORACLE.get(st, ("?", "?"))
            arm = "└─" if i == len(steps) - 1 else "├─"
            L.append(f"   {arm} {st:<22} [{who:<14}] {what}")
        L.append("")
    L.append("  A stage that fails leaves every later stage as SKIP \"not reached\" —")
    L.append("  an absent row in a 900-row report reads as not-applicable.")
    return "\n".join(L)


def main(argv: list[str] | None = None) -> int:
    """Standalone runner: the whole matrix, or only what last failed.

        python3 -m release_oracle.blessed_flow                # everything
        python3 -m release_oracle.blessed_flow --rerun-failed # only last run's failures
        python3 -m release_oracle.blessed_flow --engines mysql,mssql

    The gate itself calls `sc_blessed_flow` per engine from `run_scenarios`; this
    entry point exists for the iteration loop, where re-running 76 cells to
    re-check four is the difference between fixing a harness bug in a minute and
    in a quarter of an hour.
    """
    import argparse

    ap = argparse.ArgumentParser(prog="blessed_flow")
    ap.add_argument("--engines", default="postgres,mysql,mssql,mongo")
    ap.add_argument("--rerun-failed", action="store_true",
                    help=f"only the cells marked fail in {VERDICTS}")
    ap.add_argument("--state-url", default=os.environ.get("RIVET_GATE_STATE_URL", ""))
    ap.add_argument("--verdicts", default="", help="path to the verdict ledger")
    ns = ap.parse_args(argv)
    if ns.verdicts:
        os.environ["RIVET_FLOW_VERDICTS"] = ns.verdicts

    only: set[str] | None = None
    if ns.rerun_failed:
        only = failed_cells()
        if not only:
            print(f"no failed cells recorded in {_verdict_path()} — nothing to re-run")
            return 0
        print(f"re-running {len(only)} failed cell(s): {', '.join(sorted(only))}")

    led = Ledger()
    verify_flag_surface(led)
    for eng in [e for e in ns.engines.split(",") if e]:
        url = os.environ.get(f"RIVET_ORACLE_{eng.upper()}_URL", "")
        if not url:
            led.skipped(eng, "flow", "flow:chain", "-",
                        f"{eng} — no RIVET_ORACLE_{eng.upper()}_URL")
            continue
        sc_blessed_flow(led, eng, "live", url, state_url=ns.state_url, only=only)
        if only is None:
            sc_not_inert(led, eng, url, ns.state_url)
    return led.report()


if __name__ == "__main__":
    raise SystemExit(main())
