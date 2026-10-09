"""Rivet Release Oracle — the go/no-go gate.

Brings up each pinned engine version, seeds it from the canonical seed, runs every
scenario against the local object-store fakes (MinIO / fake-gcs / Azurite), then
the BigQuery golden stage. GREEN everywhere ⇒ releasable.

    python3 -m dev.release_oracle                        # full gate
    python3 -m dev.release_oracle --engines postgres,mysql
    python3 -m dev.release_oracle --no-cloud             # local stage only
    python3 -m dev.release_oracle --keep                 # leave containers up

Every check is PASS only when it RAN and MATCHED; a down service or absent
credential is SKIP (never a silent pass). Exit 0 iff no non-skipped cell failed —
and that verdict is DERIVED from the recorded rows, so the printed table and the
exit code cannot disagree (the bash version tracked them as two separate facts).

PHASE ORDER IS LOAD-BEARING, not cosmetic:

* `verify_release_build_path` runs FIRST, before any other cargo command. It
  checks the committed lock with `cargo metadata --locked`, and `cargo build` /
  `cargo test` silently RECONCILE a stale lock in the working tree — so a later
  position would make the check read a lock that cargo had already fixed, i.e. a
  guaranteed pass. (0.16.1 shipped a desynced lock exactly this way, aborting
  `cargo publish --locked` after the tag was cut.)
* the state-migration parity check is source-agnostic, so it runs once, before
  the engine loop, rather than N times inside it.
* the coverage-ledger drift guards are pure file checks — cheap, and a drifted
  ledger means the rest of the run is being measured against a stale map, so it
  is worth knowing before spending twenty minutes.
* each engine version is torn down before the next, so peak container count stays
  at one engine + the three store fakes. Accumulating every version is what
  starves fresh connects and flakes the gate.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import argparse
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

from .core import Ledger, Status, engine_container, verify_no_invariant_violations, docker, have, record_stage_runs, remove_engine_containers, rivet, rivet_bin, run, run_lanes, sqlcmd, target_dir, verify_nextest_grading, HERE, ROOT
from . import (
    bigquery,
    blessed_flow,
    cdc,
    cdc_schema_drift,
    clickhouse_load,
    concurrency,
    failure,
    field_state,
    fix_cells,
    gifs,
    guarantees,
    init_delta,
    live_modules,
    partner_shape,
    perf,
    regression,
    release_path,
    scenarios,
    shared_state,
    skip_census,
    state_parity,
    tls_downgrade,
    upgrade,
    warehouse_layout,
)


def matrix_cfg(*args: str) -> str:
    """Query matrix.yaml through the existing `lib/cfg.py` reader.

    Kept as a subprocess rather than re-implemented: it is the one parser both
    implementations share, so the matrix cannot mean two different things while
    the port is in flight (and PyYAML is not a dependency here)."""
    p = run(["python3", str(HERE.parent / "release-oracle" / "lib" / "cfg.py"),
             str(HERE.parent / "release-oracle" / "matrix.yaml"), *args])
    return p.stdout.strip()


BUCKET = "rivet-oracle"


def start_stores(led: Ledger) -> None:
    """Bring up the local object-store fakes and make sure the bucket exists.

    Lives with the driver rather than in `scenarios`, mirroring the bash layout
    (`run.sh` owned this) — it is bring-up, not a check, and it records no ledger
    row. Every step is best-effort on purpose: a store that does not come up is
    reported in the summary line and every cell that needs it then SKIPs, which
    is strictly better than aborting the whole gate over one emulator.

    The bucket creation is idempotent, so a re-run against already-up stores is a
    no-op rather than an error.
    """
    led.phase("Local object stores (MinIO / fake-gcs / Azurite)")
    run(["docker", "compose", "up", "-d", "minio", "fake-gcs", "azurite"], cwd=ROOT, timeout=300)

    from .core import wait_until

    # Each store on its own: `s3 or gcs` returned as soon as fake-gcs answered, and a
    # MinIO the `up` had just RECREATED (no volume — its buckets gone) was not up yet,
    # so the bucket PUT below failed unread and every s3 cell then failed on
    # `bucket not found` (measured 2026-09-25: all four CDC cells).
    wait_until(lambda: scenarios.store_up("s3"), tries=30, delay=1.0)
    wait_until(lambda: scenarios.store_up("gcs"), tries=15, delay=1.0)

    # MinIO: a signed REST PUT (the `mc` images are no longer public); 409 = already there.
    from dev.pytools.e2e import s3_bucket_exists, s3_make_bucket

    wait_until(lambda: s3_make_bucket("http://127.0.0.1:9000", BUCKET,
                                      scenarios.MINIO_ACCESS_KEY, scenarios.MINIO_SECRET_KEY),
               tries=15, delay=1.0)

    # fake-gcs: the JSON API, because an upload 404s until the bucket exists.
    import json as _json
    import urllib.error
    import urllib.request

    req = urllib.request.Request(
        f"http://127.0.0.1:4443/storage/v1/b?project=oracle",
        data=_json.dumps({"name": BUCKET}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        pass  # already exists, or the emulator is down — the summary below says which

    if have("az"):
        run(["az", "storage", "container", "create", "--name", BUCKET,
             "--connection-string", scenarios.AZURITE_CONN], timeout=120)

    def mark(store: str) -> str:
        up = scenarios.store_up(store)
        if store == "s3":
            # Up is not enough: the cells write INTO the bucket, so say ✓ only when it exists.
            up = up and s3_bucket_exists("http://127.0.0.1:9000", BUCKET,
                                         scenarios.MINIO_ACCESS_KEY, scenarios.MINIO_SECRET_KEY)
        return "✓" if up else "✗"

    led.ok(f"stores up (bucket/container {BUCKET}: minio {mark('s3')} gcs {mark('gcs')} azure {mark('azure')})")


def env_flag(name: str) -> bool:
    """An environment variable read as a BOOLEAN, in the project's one grammar.

    `bool(os.environ.get(NAME))` is not that grammar: it is TRUE for the string
    `"0"`. Both gate-wide knobs below defaulted that way, so
    `RIVET_ORACLE_WITHOUT_PREV_RELEASE=0` — the natural spelling of "no, keep
    comparing against the last release" — TURNED THE ESCAPE ON, downgrading all
    three previous-release stages from FAIL to SKIP. `regression.py` already
    read the same variable with the opposite (correct) grammar, so one variable
    had two parsers written in the same change and the coarser one won.

    Empty/unset/`0`/`false`/`no`/`off` ⇒ False; anything else ⇒ True. The
    direction for an UNRECOGNISED value (`ture`) is stated rather than implied:
    strictly, the fail-safe answer for a knob that gives up grading is False,
    and this returns True — because `regression.without_prev_release_comparison`
    is the authoritative reader the stages themselves consult, and ONE grammar
    that is imperfect at the edge beats two that disagree in the middle (`0`
    parsed both ways is exactly the defect). The spellings an operator actually
    reaches for to say "no" are all False now, and an opt-in is loud: the run
    banner prints "previous-release comparison: GIVEN UP" and every affected
    stage records SKIP.

    Kept identical to `regression.without_prev_release_comparison`'s coercion;
    `--self-test` asserts the two agree value-by-value so they cannot drift.
    """
    return os.environ.get(name, "").strip().lower() not in ("", "0", "false", "no", "off")


_ENV_FLAG_TABLE = {
    "": False, "0": False, "false": False, "FALSE": False, "no": False, "off": False,
    " 0 ": False, "1": True, "true": True, "TRUE": True, "yes": True, "on": True,
    # An unrecognised value is TRUE — see env_flag's docstring on direction.
    "maybe": True,
}


def verify_seeded_recall(led: Ledger, enabled: bool) -> None:
    """Seeded-defect recall (dev/seeded): every known bug class re-introduced must turn its catching cells red."""
    if not enabled:
        led.skipped("-", "harness", "seeded_recall", "-",
                    "seeded-defect recall: opt-in (--with-seeded-recall or RIVET_ORACLE_SEEDED_RECALL=1)")
        return
    import yaml

    from dev.seeded import recall as seeded

    seeded.recall(led, seeded.parse_manifest(yaml.safe_load(seeded.MANIFEST.read_text())))


def _gate_modules() -> list[object]:
    """Every module of this package (imported) plus this entry point: where a stage function can be bound."""
    import importlib
    import pkgutil
    pkg = sys.modules[__package__]
    mods: list[object] = [importlib.import_module(f"{__package__}.{m.name}")
                          for m in pkgutil.iter_modules(pkg.__path__) if m.name != "__main__"]
    return mods + [sys.modules[__name__]]


def _raising_stage_self_test() -> None:
    """A stage whose setup raises records one FAIL row naming it and its first error line; the run still reports NOT RELEASABLE and exits 1."""
    import contextlib
    import io
    import types

    from . import core

    def verify_boom(led: Ledger, engine: str) -> None:
        raise SystemExit("uv sync in /x/tree failed:\n  × Failed to build `pyarrow==18.1.0`")

    m = types.ModuleType("probe_raising_stage")
    m.verify_boom = verify_boom
    m.verify_after = lambda led: led.passed("-", "-", "after", "-", "the next stage still ran")
    record_stage_runs([m])
    led = Ledger(colour=False)
    out = io.StringIO()
    saved = core.TIMINGS_HISTORY
    with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(out), \
            contextlib.redirect_stderr(io.StringIO()):
        core.TIMINGS_HISTORY = Path(tmp) / "timings.jsonl"
        try:
            m.verify_boom(led, "postgres")
            m.verify_after(led)
            rc = led.report(full=True)
            wrote = core.TIMINGS_HISTORY.read_text()
        finally:
            core.TIMINGS_HISTORY = saved
    fails = [c for c in led.cells if c.status is Status.FAIL]
    assert len(fails) == 1 and fails[0].scenario == "verify_boom" and fails[0].engine == "postgres", led.cells
    assert fails[0].detail == "verify_boom raised SystemExit: uv sync in /x/tree failed:", fails[0].detail
    assert [c.scenario for c in led.cells] == ["verify_boom", "after"], led.cells
    assert rc == 1 and "NOT RELEASABLE" in out.getvalue() and '"verdict": "NOT RELEASABLE"' in wrote, (rc, wrote)
    print("self-test ok: a stage that raises is one FAIL row naming it; the gate still reports and exits 1")


def _verdict_scope_self_test() -> None:
    """A partial run writes no timings line and never prints RELEASE-READY; an ungraded cell blocks without reading as a product failure."""
    import contextlib
    import io

    from . import core

    def report(led: Ledger, full: bool) -> tuple[int, str, str]:
        out, saved = io.StringIO(), core.TIMINGS_HISTORY
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(out):
            core.TIMINGS_HISTORY = Path(tmp) / "timings.jsonl"
            try:
                rc = led.report(full=full)
                wrote = core.TIMINGS_HISTORY.read_text() if core.TIMINGS_HISTORY.exists() else ""
            finally:
                core.TIMINGS_HISTORY = saved
        return rc, out.getvalue(), wrote

    green = Ledger(colour=False)
    green.passed("-", "-", "s", "-", "four cells of a re-run")
    rc, said, wrote = report(green, full=False)
    assert rc == 0 and wrote == "" and "RELEASE-READY" not in said and "PARTIAL RUN" in said, (rc, said, wrote)
    rc, said, wrote = report(green, full=True)
    assert rc == 0 and '"verdict": "RELEASE-READY"' in wrote and "RELEASE-READY" in said, (rc, said, wrote)

    infra = Ledger(colour=False)
    infra.passed("-", "-", "s", "-", "a graded cell")
    infra.ungraded("-", "-", "s", "-", "the BigQuery read did not complete")
    rc, said, wrote = report(infra, full=True)
    assert rc == 1 and not infra.red and '"verdict": "NOT GRADED"' in wrote, (rc, wrote)
    assert "NOT GRADED" in said and "NOT RELEASABLE" not in said and "RELEASE-READY" not in said, said
    rc, said, _ = report(infra, full=False)
    assert rc == 1 and "PARTIAL RUN" not in said, "an ungraded cell must block a partial run too"
    infra.failed("-", "-", "s", "-", "the base differs from the source")
    assert infra.verdict() == "NOT RELEASABLE", "a product failure outranks an ungraded cell"

    from .core import Proc, first_error
    err = ("warning: 2 tables have no primary key\nError: [RIVET_LOAD_REFUSED] load refused: the base is ahead\n"
           "  hint: re-run with --run-id\nman" "ifests=1 parquet_files=1 expected_rows=5300")
    assert first_error(err) == "Error: [RIVET_LOAD_REFUSED] load refused: the base is ahead", first_error(err)
    assert first_error("no marker here\nlast line") == "last line" and first_error("") == ""
    with contextlib.redirect_stderr(io.StringIO()) as full_text:
        why = Proc(["rivet", "load"], 1, "", err).why
    assert why == "exit 1 [RIVET_LOAD_REFUSED]: Error: [RIVET_LOAD_REFUSED] load refused: the base is ahead", why
    assert "expected_rows=5300" in full_text.getvalue(), "the whole stderr belongs in the log"

    from .blessed_path import GOLDEN_TABLES, SCENARIOS, cell_export, same_name_outcome
    names = [cell_export(scenarios.Scope("mongo", v).dir("blessed", t, sc, store, st))
             for v in ("4.4", "8") for t in GOLDEN_TABLES for sc in SCENARIOS for store in ("local", "gcs") for st in ("pg", "sq")]
    assert len(set(names)) == len(names), "two blessed-path cells share an export name (and so a lease and a cursor)"
    assert [same_name_outcome(False, -1, 9), same_name_outcome(True, 9, 9), same_name_outcome(True, 4, 9)] == \
        ["refused", "independent", "collides"]
    print("self-test ok: partial runs write no verdict line; an oracle outage is NOT GRADED, not a product failure; "
          "rows keep the first error line; blessed-path cells have distinct export names")


def _lanes_self_test() -> None:
    """Lanes overlap, one lane's cells never do, rows flush in list order, a raise keeps every row; the upgrade
    cells sharing a source server share a lane; grading a load leaves the process environment alone."""
    import threading

    both, busy, overlap = threading.Barrier(2, timeout=10), {"a": 0, "b": 0}, []

    def cell(lane: str, n: int, meet: bool = False):
        def run_cell(sub: Ledger) -> None:
            busy[lane] += 1
            overlap.append(busy[lane])
            if meet:
                both.wait()  # raises unless the other lane is inside its cell at the same time
            time.sleep(0.02)
            busy[lane] -= 1
            sub.passed("-", "-", "lanes", lane, f"{lane}{n}")
        return run_cell

    led = Ledger(colour=False)
    led._buf = []
    run_lanes(led, [("a", cell("a", 1, True)), ("b", cell("b", 1, True)), ("a", cell("a", 2)), ("b", cell("b", 2)),
                    ("a", cell("a", 3))])
    assert [c.detail for c in led.cells] == ["a1", "b1", "a2", "b2", "a3"], led.cells
    assert set(overlap) == {1}, f"two cells of one lane ran at once: {overlap}"

    def boom(sub: Ledger) -> None:
        raise RuntimeError("cell blew up")

    led = Ledger(colour=False)
    led._buf = []
    try:
        run_lanes(led, [("a", cell("a", 1)), ("b", boom), ("a", cell("a", 2))])
    except RuntimeError:
        pass
    else:
        raise AssertionError("a raising cell must reach the stage, which records it")
    assert [c.detail for c in led.cells] == ["a1", "a2"], "rows recorded beside a raising cell were dropped"

    # The gate's own URLs (Makefile GATE_ENV): nine servers; Oracle's batch, resume-load and CDC cells share one.
    from . import upgrade, upgrade_matrix
    urls = {"RIVET_ORACLE_POSTGRES_URL": "postgresql://rivet:rivet@127.0.0.1:5432/rivet",
            "RIVET_ORACLE_MYSQL_URL": "mysql://rivet:rivet@127.0.0.1:3306/rivet",
            "RIVET_ORACLE_MSSQL_URL": "mssql://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet",
            "RIVET_ORACLE_MONGO_URL": "mongodb://127.0.0.1:27017/rivet",
            "RIVET_ORACLE_ORACLE_URL": "oracle://rivet:rivet@localhost:1521/FREEPDB1",
            "RIVET_UPG_ORACLE_CDC_URL": "oracle://c%23%23rivetcdc:rivet@127.0.0.1:1521/FREEPDB1",
            "RIVET_CDC_POSTGRES_URL": "postgresql://rivet:rivet@127.0.0.1:5434/rivet",
            "RIVET_CDC_MYSQL_URL": "mysql://rivet:rivet@127.0.0.1:3307/rivet",
            "RIVET_CDC_MSSQL_URL": "mssql://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet",
            "RIVET_CDC_MONGO_URL": "mongodb://127.0.0.1:27018/rivet?directConnection=true",
            "BQ_ORACLE_PROJECT": "p", "BQ_ORACLE_BUCKET": "b"}
    saved = {k: os.environ.get(k) for k in urls}
    os.environ.update(urls)
    try:
        cells = upgrade.warehouse_lane_cells(Path("prev"), Path("root"))
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
    lanes: dict[object, int] = {}
    for lane, _ in cells:
        lanes[lane] = lanes.get(lane, 0) + 1
    ports = {str(k).rsplit(":", 1)[-1] for k in lanes if k is not None}
    assert ports == {"5432", "3306", "1433", "27017", "1521", "5434", "3307", "1434", "27018"} and None not in lanes, lanes
    fam = len(upgrade_matrix.rows()) * len(upgrade_matrix.TARGETS)
    assert lanes["127.0.0.1:1521"] == fam + 1 + 3, f"Oracle's matrix, resume-load and cdc-load cells are not one lane: {lanes}"
    # MySQL's CDC server: UTC, the non-UTC zone, init=this and the empty-baseline cell.
    assert lanes["127.0.0.1:3307"] == 4 and lanes["127.0.0.1:5432"] == fam + 1, lanes

    # grade_load hands its BigQuery target to the session; the environment every other lane reads stays as it was.
    import types

    from . import rig_oracle
    seen: dict = {}

    class _Stop(Exception):
        pass

    def _oracle(**kw):
        seen.update(kw)
        raise _Stop

    fake = types.ModuleType(f"{__package__}.duck")
    fake.Oracle = _oracle
    real, before = sys.modules.get(fake.__name__), dict(os.environ)
    sys.modules[fake.__name__] = fake
    try:
        rig_oracle.grade_load({"engine": "postgres", "mode": "batch", "state": "", "url": "postgresql://x/y",
                               "load": {"target": "bigquery", "project": "proj-of-the-cell", "dataset": "ds_of_the_cell"}})
    except _Stop:
        pass
    finally:
        sys.modules.pop(fake.__name__, None)
        if real is not None:
            sys.modules[fake.__name__] = real
    assert dict(os.environ) == before, "grade_load wrote the process environment: cells in other lanes read it"
    assert (seen.get("bq_project"), seen.get("bq_dataset")) == ("proj-of-the-cell", "ds_of_the_cell"), seen
    print(f"self-test ok: {len(cells)} upgrade cells in {len(lanes)} lanes, one per source server; lanes overlap, "
          "a lane is serial, rows flush in ledger order; grade_load leaves the environment alone")


def _stage_table_self_test() -> None:
    """A measuring stage shares its wave with nothing; holders and users of one resource never overlap; the table keeps its order and every stage."""
    def st(name: str, needs: set[str], uses: set[str] = frozenset()) -> Stage:
        return Stage(name, lambda led: None, frozenset(needs), frozenset(uses))

    cut = [[s.name for s in w] for w in waves([
        st("a", {"x"}), st("b", {"y"}), st("c", {"x"}), st("m", {ALONE}), st("d", {"z"}), st("u1", {"p"}, {"x"}),
        st("u2", {"q"}, {"x"}), st("h", {"x"}), st("u3", {"r"}, {"x"})])]
    assert cut == [["a", "b"], ["c"], ["m"], ["d", "u1", "u2"], ["h"], ["u3"]], cut
    order: list[str] = []
    probe = Ledger(colour=False)
    probe._buf = []
    run_stages(probe, [Stage(n, lambda led, n=n: (order.append(n), led.passed("-", "-", n, "-", n))[1], frozenset({ALONE}))
                       for n in ("one", "two")])
    assert order == ["one", "two"] and [c.scenario for c in probe.cells] == ["one", "two"], (order, probe.cells)
    for argv in ([], ["--no-cloud"], ["--no-clean"]):
        ns = parse_args(argv)
        table = build_stages(ns, {}) + gate_stages(ns)
        cut = waves(table)
        assert [s for w in cut for s in w] == table, "the waves must hold every stage once, in table order"
        measuring = {"release regression", "perf regression", "source harm regression", "previous-release differential",
                     "field symptom replay", "scale memory"}
        assert measuring <= {s.name for s in table}, measuring - {s.name for s in table}
        shared = [[s.name for s in w] for w in cut if len(w) > 1 and measuring & {s.name for s in w}]
        assert not shared, f"a stage that measures against the previous release shares its wave: {shared}"
        assert table[-1].name == "live modules" and [s.name for s in cut[-1]] == ["live modules"], \
            "live modules runs last and alone: it runs what no stage before it ran"
        first = next(s for s in table if s.name == "release build path")
        assert not [s for s in table[:table.index(first)] if CARGO in s.needs | s.uses and s.name != "clean tree"], \
            "the release build path reads the committed lock before any other cargo stage"
    overlapping = [" + ".join(s.name for s in w) for w in waves(build_stages(parse_args([]), {}) + gate_stages(parse_args([])))
                   if len(w) > 1]
    print(f"self-test ok: the stage table keeps every stage in order, measurement stages run alone; overlapping: {overlapping}")


def _stages_self_test() -> None:
    """Stage recording counts the rows a stage adds; the end-of-run matrix check fails on a stage that never ran or graded nothing; the real matrix resolves to recorded stages."""
    import types

    import yaml

    from .core import STAGES_DEFINED, STAGES_RUN, matrix_test_stages

    m = types.ModuleType("probe_stage_module")
    m.verify_a = lambda led: led.passed("-", "-", "a", "-", "a ran")
    m.sc_b = lambda led, engine: None
    m.helper = lambda led: led.passed("-", "-", "h", "-", "not a stage")
    record_stage_runs([m])
    assert {"verify_a", "sc_b"} <= STAGES_DEFINED and "helper" not in STAGES_DEFINED
    probe = Ledger(colour=False)
    probe._buf = []
    m.verify_a(probe)
    m.sc_b(probe, "postgres")
    m.sc_b(probe, "mysql")
    assert STAGES_RUN["verify_a"] == 1 and STAGES_RUN["sc_b"] == 0, STAGES_RUN
    record_stage_runs([m])
    m.verify_a(probe)
    assert STAGES_RUN["verify_a"] == 2, "re-recording must not wrap the wrapper"
    _raising_stage_self_test()
    _verdict_scope_self_test()
    doc = {
        "preflights": [{"id": "a", "status": "test"}, {"id": "off", "status": "gap"}],
        "infra": [{"id": "undriven", "status": "test"}],
        "scenarios": [{"id": "b", "what": "w", "postgres": "test", "mysql": {"na": "r"}},
                      {"id": "c", "what": "w", "postgres": {"gap": "r"}},
                      {"id": "never", "what": "w", "postgres": "test"}],
    }
    assert matrix_test_stages(doc, {"verify_a", "sc_b"}) == {
        "verify_a": "preflight `a`", "sc_b": "scenario `b`", "sc_never": "scenario `never`"}, \
        matrix_test_stages(doc, {"verify_a", "sc_b"})
    probe.close_matrix_rows(doc)
    fails = [c.detail for c in probe.cells if c.status is Status.FAIL]
    assert len(fails) == 2 and "sc_b() and it recorded no ledger row" in fails[0] and "sc_never() never ran" in fails[1], fails
    import ast

    real = yaml.safe_load((ROOT / "docs/release-gate-matrix.yaml").read_text())
    # Read by `ast`, not imported: the self-test runs on a bare python3 without duckdb.
    defined = {n.name for f in HERE.glob("*.py") for n in ast.parse(f.read_text()).body
               if isinstance(n, ast.FunctionDef) and n.name.startswith(("sc_", "verify_"))}
    want = matrix_test_stages(real, defined)
    assert len(want) > 40, f"the real matrix resolves to only {len(want)} stages"
    missing = sorted(set(want) - defined)
    assert not missing, f"`test` rows whose stage the gate cannot record: {missing}"
    print(f"self-test ok: {len(want)} matrix `test` rows resolve to recorded stages; a stage that never ran or graded nothing fails the run")


def _self_test() -> int:
    """Grade this module's env-flag grammar against `regression`'s.

    RED-provable: restore `default=bool(os.environ.get(...))` in `parse_args`
    and the `--without-prev-release-comparison` case below fails on `"0"`.
    """
    for raw, expect in _ENV_FLAG_TABLE.items():
        os.environ["RIVET_ORACLE_SELFTEST_FLAG"] = raw
        got = env_flag("RIVET_ORACLE_SELFTEST_FLAG")
        assert got is expect, f"env_flag({raw!r}) = {got}, expected {expect}"
    os.environ.pop("RIVET_ORACLE_SELFTEST_FLAG", None)
    # A stage that moves the Rig off Postgres must blank BOTH knobs: the tests read RIVET_GATE_STATE_URL.
    old, gate = "RIVET_" + "STATE_URL", "RIVET_GATE_" + "STATE_URL"
    shapes = ('"{}": ""', '"-u", "{}"', '.pop("{}", None)')
    for path in sorted(Path(__file__).parent.glob("*.py")):
        src = path.read_text()
        for shape in shapes:
            assert src.count(shape.format(old)) == src.count(shape.format(gate)), (path.name, shape)
    from dev.seeded import recall as seeded

    seeded.self_test()
    guarantees.self_test()
    off = Ledger(colour=False)
    off._buf = []
    verify_seeded_recall(off, False)
    assert [c.status for c in off.cells] == [Status.SKIP], "a disabled recall stage must say so, not vanish"

    # The escape is the one that costs a release: argparse's default, the
    # authoritative reader in regression.py, and this table must agree on EVERY
    # spelling — that disagreement is what let `=0` give up the baseline.
    saved = os.environ.pop(regression._ESCAPE_ENV, None)
    try:
        for raw, expect in _ENV_FLAG_TABLE.items():
            os.environ[regression._ESCAPE_ENV] = raw
            assert regression.without_prev_release_comparison() is expect, raw
            ns = parse_args([])
            assert ns.without_prev_release_comparison is expect, (
                f"{regression._ESCAPE_ENV}={raw!r} parses as "
                f"{ns.without_prev_release_comparison}, but regression.py reads it as {expect}"
            )
            assert parse_args(["--without-prev-release-comparison"]).without_prev_release_comparison
            os.environ["RIVET_ORACLE_LATEST_ONLY"] = raw
            assert parse_args([]).latest_only is expect, raw
            # The replica escape shares the grammar: a `=0` that gave the stand up would be the
            # same defect, one stage over.
            os.environ[scenarios._REPLICA_ESCAPE_ENV] = raw
            assert scenarios.without_replica_topologies() is expect, raw
            assert parse_args([]).without_replica_topologies is expect, raw
            assert parse_args(["--without-replica-topologies"]).without_replica_topologies
    finally:
        os.environ.pop(regression._ESCAPE_ENV, None)
        os.environ.pop("RIVET_ORACLE_LATEST_ONLY", None)
        os.environ.pop(scenarios._REPLICA_ESCAPE_ENV, None)
        if saved is not None:
            os.environ[regression._ESCAPE_ENV] = saved
    # A down replica stand is a FAIL row unless the escape names the give-up.
    saved_replica = os.environ.pop(scenarios._REPLICA_ESCAPE_ENV, None)
    try:
        strict = Ledger(colour=False)
        scenarios._replica_down(strict, "probe", ":1 closed", "start it")
        assert strict.red and strict.cells[-1].status is Status.FAIL, "a down replica stand must FAIL the row"
        scenarios.set_without_replica_topologies(True)
        lenient = Ledger(colour=False)
        scenarios._replica_down(lenient, "probe", ":1 closed", "start it")
        assert not lenient.red and lenient.cells[-1].status is Status.SKIP, "the escape must turn it into a SKIP"
        assert scenarios._REPLICA_ESCAPE_FLAG in lenient.cells[-1].detail, "the SKIP row must name the escape"
        # The replica stages register their tests whatever the ports say, so the derived
        # live-modules cell never runs them again (a stand that is down, probed as closed).
        if have("cargo"):
            from .core import RAN_LIVE_TESTS
            real_probe, real_ran = scenarios._tcp_open, set(RAN_LIVE_TESTS)
            scenarios._tcp_open = lambda *a, **k: False
            try:
                probe_led = Ledger(colour=False)
                scenarios.verify_replica_read(probe_led)
                scenarios.verify_cdc_standby(probe_led)
                expected = {scenarios.REPLICA_READ_TEST, scenarios.CDC_STANDBY_TEST,
                            *(t for _, _, t, _ in scenarios.REPLICA_CELLS)}
                assert expected <= RAN_LIVE_TESTS, f"replica tests not registered as run: {expected - RAN_LIVE_TESTS}"
                assert all(c.status is Status.SKIP for c in probe_led.cells), probe_led.cells
            finally:
                scenarios._tcp_open = real_probe
                RAN_LIVE_TESTS.clear()
                RAN_LIVE_TESTS.update(real_ran)
    finally:
        scenarios.set_without_replica_topologies(False)
        if saved_replica is not None:
            os.environ[scenarios._REPLICA_ESCAPE_ENV] = saved_replica

    print(f"self-test ok: {len(_ENV_FLAG_TABLE)} spellings — argparse, env_flag and "
          "regression.without_prev_release_comparison() agree on every one")

    # Two versions of the gate's matrix must never mint one name: every cell's dirs,
    # prefixes and names come from `scenarios.Scope`, keyed on engine AND version.
    grid = [(e, line.split()[0]) for e in matrix_cfg("engines").split()
            for line in matrix_cfg("versions", e).splitlines() if line.split()]
    scopes = [scenarios.Scope(e, t) for e, t in grid]
    for mint in (lambda s: s.name("x", "t"), lambda s: str(s.dir("x", "t")),
                 lambda s: s.prefix("x", "t")):
        minted = [mint(s) for s in scopes]
        assert len(set(minted)) == len(minted), f"two versions share a name: {minted}"
    print(f"self-test ok: {len(scopes)} engine versions mint {len(scopes)} distinct names each way")

    # …and the regression module's own decisions about a child harness it cannot
    # run here: the SIGINT-first timeout, the grace period's grammar and where
    # its default comes from, the stand row when the container will not answer,
    # and the banner/footer that describe whether anything was compared at all.
    # One entry point, so CI and the offline suite get both halves.
    # A failed test that also leaked prints `FAIL + LEAK [`: it must never read as green.
    from .core import nextest_grading_error
    why = nextest_grading_error()
    assert why is None, why
    print("self-test ok: a FAIL + LEAK line is a failure, a LEAK line a pass, SLOW is not final")
    # A Rig cell whose local service is down is a SKIP naming it, and needs no cloud.
    from .shared_state import run_rig_tests
    probe = Ledger(colour=False)
    run_rig_tests(probe, "probe", ("t",), cell=str, msg=str, cloud=False, services=(("nothing", 1),))
    skipped = [c for c in probe.cells if c.status == Status.SKIP]
    assert len(probe.cells) == 1 and skipped and "nothing (:1)" in skipped[0].detail, probe.cells
    print("self-test ok: a Rig cell with a service down SKIPs naming it, without cloud prerequisites")
    from .core import nextest_filter, test_passed
    assert test_passed("t", {"m::t"}) and not test_passed("t", {"m::at", "m::t_x"}), "suffix match"
    assert nextest_filter(["t"]) == "test(/(^|::)t$/)"
    print("self-test ok: a test is matched by its whole name, never by a suffix of another")
    # A run-integrity warning in ANY command's output fails the gate; a clean run passes.
    from . import core as _core
    saved = list(_core.INVARIANT_HITS)
    _core.INVARIANT_HITS.clear()
    _core.Proc(["rivet", "run"], 0, "", "[WARN] export 't': run-integrity invariant violated — x")
    probe = Ledger(colour=False)
    _core.verify_no_invariant_violations(probe)
    assert [c.status for c in probe.cells] == [Status.FAIL], probe.cells
    _core.INVARIANT_HITS.clear()
    probe = Ledger(colour=False)
    _core.verify_no_invariant_violations(probe)
    assert [c.status for c in probe.cells] == [Status.PASS], probe.cells
    _core.INVARIANT_HITS.extend(saved)
    print("self-test ok: a run-integrity warning in any command's output fails the gate")
    # Without cargo-llvm-cov the offline battery still runs and is graded on its own row.
    from . import scenarios as _sc
    real_run, real_have = _sc.run, _sc.have
    try:
        _sc.have = lambda _tool: True
        for battery_rc, want in ((1, Status.FAIL), (0, Status.PASS)):
            _sc.run = lambda argv, **_k: _core.Proc(
                list(argv), 1 if "llvm-cov" in argv else battery_rc, "", "")
            probe = Ledger(colour=False)
            _sc.verify_live_only_coverage(probe)
            rows = {c.scenario: c.status for c in probe.cells}
            assert rows.get("battery") == want, probe.cells
        # A self-skip in the battery fails its row unless another stage grades that test.
        for skipped, want in (("other::t — X unset", Status.FAIL),
                              ("state::row::tests::pg_accessor_reads_every_integer_width_and_refuses_other_types"
                               " — RIVET_TEST_STATE_URL unset", Status.PASS)):
            def _skipping_run(argv, env=None, _line=skipped, **_k):
                if "llvm-cov" in argv:
                    return _core.Proc(list(argv), 1, "", "")
                Path(env["RIVET_SKIP_LOG"]).write_text(f"RIVET-SKIP {_line}\n")
                return _core.Proc(list(argv), 0, "", "")
            _sc.run = _skipping_run
            probe = Ledger(colour=False)
            _sc.verify_live_only_coverage(probe)
            rows = {c.scenario: c.status for c in probe.cells}
            assert rows.get("battery") == want, (skipped, probe.cells)
        # A self-skip naming the env var the stage can set is run again under it, and only that leg is its verdict.
        real_live, _legs = _sc.nextest_live, []

        def _two_legs(_log, expr, env=None, _threads=None):
            _legs.append(expr)
            if len(_legs) % 2:
                return ({f"m::{t}": "PASS" for t in "abcde"}, 5, {**{f"m::{t}": "set RIVET_GATE_STATE_URL" for t in "abe"}, "m::d": "X unset"}, {})
            here = (env or {}).get("RIVET_GATE_STATE_URL", "").startswith("postgres")
            return {"m::a": "PASS", "m::b": "PASS"}, 2, {"m::b": "still"} if here else {"m::a": "still", "m::b": "still"}, {}
        _sc.nextest_live = _two_legs
        try:
            for again, want in ((("RIVET_GATE_STATE_URL", {"RIVET_GATE_STATE_URL": "postgresql://s"}), "PFPFF"), (None, "FFPFF")):
                probe = Ledger(colour=False)
                _sc._run_live_modules(probe, "probe", "probe", "probe", ["m"], again=again)
                got = "".join("P" if c.status == Status.PASS else "F" for c in probe.cells)
                assert got == want, (again, probe.cells)
            assert _legs[1] == "test(=m::a) | test(=m::b) | test(=m::e)" and len(_legs) == 3, _legs
        finally:
            _sc.nextest_live = real_live
    finally:
        _sc.run, _sc.have = real_run, real_have
    print("self-test ok: a live test that self-skips for an env var the stage can set is graded by its second leg")
    # Leftovers of killed runs (2087 Mongo databases, 2026-10-09) held the open files the gate's cells needed.
    import re as _re
    assert _re.search(r"^release-oracle-full:[^#\n]*\bsweep-test-db\b", (ROOT / "Makefile").read_text(), _re.M), \
        "`make release-oracle-full` must depend on sweep-test-db: the gate starts on a swept stand"
    print("self-test ok: the full gate sweeps the stand's leftovers before it starts")
    from . import state_lib as _sl
    assert _sl.vacuous("running 0 tests\ntest result: ok. 0 passed; 0 failed", {}), "a zero-match filter graded nothing"
    assert _sl.vacuous("test result: ok. 9 passed; 0 failed", {"state::x::t": "RIVET_TEST_STATE_URL unset"})
    assert _sl.vacuous("test result: ok. 9 passed; 0 failed", {}) == []
    import subprocess
    _a = _sl.argv()
    _seen = subprocess.run(_a[:_a.index("cargo")] + ["printenv"], capture_output=True, text=True,
                           env={**os.environ, "RIVET_STATE_URL": "postgresql://x", "RIVET_GATE_STATE_URL": "postgresql://x",
                                "RIVET_TEST_STATE_URL": "postgresql://t"}).stdout
    assert "RIVET_TEST_STATE_URL=postgresql://t" in _seen and "RIVET_STATE_URL=" not in _seen.replace(
        "RIVET_TEST_STATE_URL=", "") and "RIVET_GATE_STATE_URL" not in _seen, "the state lib tests see only RIVET_TEST_STATE_URL"
    print("self-test ok: without cargo-llvm-cov the offline battery is still graded, its self-skips too")
    # A graded harm counter past prev × tol + slack fails; noise within it passes; a counter
    # only one binary records is not compared.
    hv = regression.harm_verdict
    assert hv("postgres", {"pg_tup_returned": 1000}, {"pg_tup_returned": 1400}, 1.25, 200) == (
        [], ["pg_tup_returned"])
    worse, _ = hv("postgres", {"pg_tup_returned": 1000}, {"pg_tup_returned": 1500}, 1.25, 200)
    assert worse == ["pg_tup_returned 1500 > 1000×1.25+200"], worse
    assert hv("mssql", {}, {"mssql_page_lookups": 9}, 1.25, 200) == ([], [])
    assert hv("postgres", {"pg_blks_hit": 1}, {"pg_blks_hit": 999999}, 1.25, 200) == ([], []), \
        "cache counters are recorded, not graded"
    print("self-test ok: a source-harm counter past the previous release fails the gate")
    # The derived live-module run: an exclusion names a module that exists, and the filter
    # drops what a dedicated cell already ran.
    mods = live_modules.live_suite_modules()
    stale = [m for m in live_modules.EXCLUDED if m not in mods]
    assert not stale, f"live_modules.EXCLUDED names modules live_suite no longer has: {stale}"
    left, expr = live_modules.derived_filter(["a", "b", "common"], {"b"}, {"t_1"})
    assert left == ["a"] and expr == "(test(/^a::/)) - test(/::(t_1)$/)", (left, expr)
    # perf: a regression past the tolerance fails, noise under the absolute slack does not.
    from .perf import Sample, perf_verdict
    base = Sample(True, 1.0, 0.02, 50 * 1024 * 1024, {})
    assert perf_verdict("postgres", base, Sample(True, 1.05, 0.05, 50 * 1024 * 1024, {})) == []
    assert perf_verdict("postgres", base, Sample(True, 2.0, 0.02, 50 * 1024 * 1024, {}))
    assert perf_verdict("postgres", base, Sample(True, 1.0, 0.02, 200 * 1024 * 1024, {}))
    # A plan evicted between the two SQL Server readings takes nothing away from the run's
    # own reads; an unanswered reading is not a measurement; rivet's report yields to it.
    from .perf import measured_harm
    assert measured_harm("mssql://h", {"old": 9000, "kept": 100}, {"kept": 130, "new": 20},
                         {"mssql_logical_reads": 0, "mssql_worktables_created": 1}) == {
        "mssql_logical_reads": 50, "mssql_worktables_created": 1}
    assert measured_harm("mssql://h", {}, {"new": 7}, {}) == {"mssql_logical_reads": 7}
    assert measured_harm("mssql://h", None, {"new": 7}, {"mssql_logical_reads": 7}) is None
    assert measured_harm("postgresql://h", {"pg_tup_returned": 5}, {"pg_tup_returned": 9}, {}) == {
        "pg_tup_returned": 4}
    assert measured_harm("", None, None, {"mongo_docs_scanned": 3}) == {"mongo_docs_scanned": 3}
    # cdc-conns: the ceiling holds even when the previous release was worse, and one
    # connection more than the previous release is a regression under the ceiling too.
    from .perf import conns_verdict
    assert conns_verdict("postgres", 5, 2) == [] and conns_verdict("postgres", 2, 3)
    assert conns_verdict("mongo", 3, 4) and conns_verdict("mongo", 6, 6) == []
    # A path graded without its wall still fails on CPU, and passes a slower wall alone.
    from .perf import _grade
    slow_wall = Ledger(colour=False)
    _grade(slow_wall, "mongo", "p", base, Sample(True, 9.0, 0.02, 50 * 1024 * 1024, {}), wall=False)
    assert not slow_wall.red, "wall=False must not grade the wall"
    hot_cpu = Ledger(colour=False)
    _grade(hot_cpu, "mongo", "p", base, Sample(True, 1.0, 5.0, 50 * 1024 * 1024, {}), wall=False)
    assert hot_cpu.red, "wall=False must still grade CPU"
    # "Declared" is one rule: a failed run's manifest delivers nothing, an uncommitted part neither.
    import json as _json
    import tempfile as _tf
    from .upgrade import _declared_names
    with _tf.TemporaryDirectory() as d:
        root = Path(d)
        for name in ("ok.parquet", "failed.parquet", "pending.parquet"):
            (root / name).write_bytes(b"x")
        (root / "manifest-ok.json").write_text(_json.dumps({"status": "success", "parts": [
            {"path": "ok.parquet", "status": "committed"}, {"path": "pending.parquet", "status": "pending"}]}))
        (root / "manifest-bad.json").write_text(_json.dumps({"status": "failed", "parts": [
            {"path": "failed.parquet", "status": "committed"}]}))
        assert _declared_names(root) == {"ok.parquet"}, _declared_names(root)
    # An INTERNAL error anywhere in a gated command's output is a gate failure.
    from . import core as _core
    _core.note_invariant_violations(["rivet", "run"], "Error: [RIVET_INTERNAL_SPILL] cdc spill: sealed twice")
    _core.note_invariant_violations(["rivet", "run"], 'a row mentions RIVET_INTERNAL_ in its data')
    assert len(_core.INTERNAL_HITS) == 1, _core.INTERNAL_HITS
    _core.INTERNAL_HITS.clear()
    # Known reds: an in-date entry downgrades its failure, an expired one does not, and an
    # entry nothing matched in a full run is reported as fixed.
    import datetime as _dt
    from . import known_red as kr
    # A synthetic registry, so the check does not depend on how many real reds are open.
    real_reds = kr.KNOWN_RED
    kr.KNOWN_RED = (kr.KnownRed("probe-red-a", "self-test", "2099-01-01"),
                    kr.KnownRed("probe-red-b", "self-test", "2099-01-01"))
    k = kr.KNOWN_RED[0]
    assert kr.match(f"x {k.match} y", _dt.date(2026, 1, 1)) == (k, True)
    assert kr.match(f"x {k.match} y", _dt.date(2099, 1, 2)) == (k, False)
    assert kr.match("an unrelated failure", _dt.date(2026, 1, 1)) == (None, False)
    probe = Ledger(colour=False)
    probe.failed("-", "-", "s", "-", f"boom {k.match}")
    assert probe.cells[-1].status is Status.KNOWN and not probe.red
    from .scenarios import _failed
    from .scenarios import _passed
    seen = Ledger(colour=False)
    _passed(seen, "-", "-", "s", "-", f"ok {k.match}")
    assert k.match in seen.known_passed, "a scenario PASS must reach the known-red registry"
    via = Ledger(colour=False)
    _failed(via, "-", "-", "s", "-", f"boom {k.match}", "detail")
    assert via.cells[-1].status is Status.KNOWN, "a scenario failure must meet the known-red registry"
    probe.close_known_red()
    assert probe.red, "an entry that matched nothing in a full run must fail"
    other = kr.KNOWN_RED[-1]
    shown = Ledger(colour=False)
    shown.passed("-", "-", "s", "-", f"ok {other.match}")
    shown.close_known_red()
    said = [c.detail for c in shown.cells if c.scenario == "known_red"]
    assert any(other.match in d and "PASSED" in d for d in said), said
    assert any(k.match in d and "no cell" in d for d in said), said
    kr.KNOWN_RED = real_reds
    from .core import SKIP_ALLOWED
    from .live_modules import exclusive_tests
    live_src = "\n".join(f.read_text() for f in (ROOT / "tests" / "live").glob("*.rs"))
    for key in SKIP_ALLOWED:
        assert f"fn {key.split('::')[-1]}(" in live_src, f"SKIP_ALLOWED names no live test: {key}"
    # A replica row whose test was renamed would grade an empty nextest run as PASS.
    for name in (scenarios.REPLICA_READ_TEST, scenarios.CDC_STANDBY_TEST, *(t for _, _, t, _ in scenarios.REPLICA_CELLS)):
        assert f"fn {name}(" in live_src, f"a replica cell names no live test: {name}"
    assert exclusive_tests(), "no live+exclusive test found — the exclusive pass would grade nothing"
    print("self-test ok: live modules are derived; perf tolerances grade regressions, not noise")
    import json as _json
    import tempfile as _tf

    from .core import record_timings

    with _tf.TemporaryDirectory() as d:
        hist = Path(d) / "t.jsonl"
        first = record_timings(hist, [("A", 60.0), ("B", 120.0)], [("s", 30.0)], "RELEASE-READY", 3, 0,
                               {"A — detail": (1.26, 30.04)})
        assert first["phases_load1"] == {"A": [1.3, 30.0]} and first["cores"], first
        timed = Ledger(colour=False)
        timed._buf = []
        timed.phase("one")
        timed.phase("two")
        lo, hi = timed._phase_load["one"]
        assert lo > 0 and hi > 0, "a phase must record the load average it opened and closed under"
        second = record_timings(hist, [("A", 180.0), ("B", 120.0)], [], "NOT RELEASABLE", 2, 1)
        lines = hist.read_text().splitlines()
        assert len(lines) == 2, lines
        assert _json.loads(lines[0])["total_min"] == 3.0 and second["total_min"] == 5.0, lines
        assert second["phases_min"] == {"A": 3.0, "B": 2.0} and second["failed"] == 1, second
    print("self-test ok: gate timings append one history line per run, with each phase's load average")
    _stage_table_self_test()
    from . import sentinels

    sentinels._self_test()
    print("self-test ok: sentinel verdicts (exact, or a loud non-panic refusal on a risky value)")
    from . import state_parity_duckdb

    state_parity_duckdb._self_test()
    print("self-test ok: state parity excludes surrogate keys by rule and compares a reference by content")
    _stages_self_test()
    from . import upgrade_matrix

    upgrade_matrix._self_test()
    _lanes_self_test()
    skip_census._self_test()
    fix_cells._self_test()
    field_state._self_test()
    print("\nregression stage (child harness, stand, banner):")
    return regression._self_test()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="release-oracle", add_help=True)
    ap.add_argument("--engines", default="", help="comma-separated subset, e.g. postgres,mysql")
    ap.add_argument(
        "--versions",
        default=os.environ.get("RIVET_ORACLE_VERSIONS", ""),
        help="narrow THIS RUN to the named versions, as `engine=tag,tag` joined by "
        "`;` (e.g. postgres=14,16,18). Engines not named run their whole grid. "
        "This is deliberately a RUN filter, not a matrix edit: deleting versions "
        "from matrix.yaml forces them into `gaps` in docs/release-gate-matrix.yaml, "
        "whose guard asserts EQUALITY against a shrink-only ratchet — so the edit "
        "would trade wall-clock for a false coverage claim. A tag the matrix does "
        "not list FAILS that engine loudly; running zero versions green is the one "
        "outcome this must never produce.",
    )
    ap.add_argument(
        "--version-parallel", type=int, default=8,
        help="how many VERSIONS of one engine family to run concurrently "
        "(default 8 = every version the matrix lists; 1 = serial). Serial, mongo's "
        "five versions were the whole matrix wall (13.6 min). Each version owns its "
        "own container name, port, work dirs and export names (`scenarios.Scope`), so the "
        "ceiling is MEMORY: measured 2026-09-24 in the full gate, every family at once "
        "peaked near 31 of 40 GiB in the Docker VM and the matrix took 5.9 min. Lower "
        "it if Docker memory is tight.",
    )
    ap.add_argument(
        "--latest-only",
        action="store_true",
        default=env_flag("RIVET_ORACLE_LATEST_ONLY"),
        help="run only the LAST version of each engine family (fast dev loop). "
        "The full version matrix is for release tags; a single-version pass "
        "still covers every scenario × store × state-backend cell.",
    )
    ap.add_argument(
        "--without-prev-release-comparison",
        action="store_true",
        # The AUTHORITATIVE reader, not a second parse of the same variable:
        # `bool(os.environ.get(...))` made `RIVET_ORACLE_WITHOUT_PREV_RELEASE=0`
        # — and `false`, `no`, `off` — turn the escape ON, silently downgrading
        # every previous-release stage to SKIP. regression.py owns the grammar
        # (it is what the stages themselves consult); argparse now asks it.
        default=regression.without_prev_release_comparison(),
        help="GIVE UP every comparison against the previously released binary — the "
        "regression (format+perf), the observable-surface differential, and the field "
        "symptom replay all become SKIP instead of FAIL when RIVET_PREV_RELEASE_BIN is "
        "absent. Named after what it costs, and never a default: a release run without "
        "a baseline is exactly how 0.24.4 shipped a +1h48m governor regression through a "
        "green gate — the one leg that would have caught it reported a non-failure "
        "because it never ran. Use it for local partial runs; a run carrying this flag "
        "cannot support a tag.",
    )
    ap.add_argument(
        "--without-replica-topologies",
        action="store_true",
        default=scenarios.without_replica_topologies(),
        help="GIVE UP every replica/standby topology row (mysql replicas, the PostgreSQL "
        "standby pair, the SQL Server availability group, the second mongo replica set): a "
        "stand that is down then records SKIP instead of FAIL. The release gate is the ONLY "
        "runner of those tests (CI skips them by name), so without this flag a down stand is "
        "an ungraded release and fails. Use it for local partial runs; a run carrying this "
        "flag cannot support a tag.",
    )
    ap.add_argument(
        "--with-seeded-recall", action="store_true", default=env_flag("RIVET_ORACLE_SEEDED_RECALL"),
        help="also run the seeded-defect recall stage (dev/seeded): re-introduce each known bug class "
             "as a source patch in a scratch tree, build it, and require its catching cells to go red. "
             "Slow (one build per seed), so opt-in; `make seeded-recall` runs it alone.")
    ap.add_argument("--no-cloud", action="store_true", help="local stage only (skip BigQuery)")
    ap.add_argument("--keep", action="store_true", help="leave engine containers up (debug)")
    ap.add_argument(
        "--cell-parallel", type=int, default=16,
        help="global cap on concurrent MATRIX CELLS (blessed_flow/blessed_path) across "
             "ALL engines. The matrix is I/O-bound (~62%% CPU idle at 4-way), so running "
             "its independent cells concurrently fills the idle cores; this bounds the "
             "total so the shared state DB / source containers are not stampeded.")
    ap.add_argument(
        "--engine-parallel", type=int, default=5,
        help="how many engines to run CONCURRENTLY in the engine loop (default 5 — every engine, so the longest never queues). Each engine "
             "owns its own containers/ports, and the scenarios race on the SHARED state backend "
             "— which doubles as a real concurrent-writer test. The dominant serial cost is CDC "
             "capture-job waits (sleep-for-the-agent), which overlap under parallelism, so the "
             "engine-loop wall drops from sum(engines) toward max(engine). Set 1 for the old "
             "serial behaviour, or lower if Docker memory is tight (MSSQL and Oracle are capped at 3 GiB each).")
    ap.add_argument("--no-clean", action="store_true",
                    help="skip the clean rebuild (iteration only — a release must be gated on a "
                         "tree built from nothing)")
    ap.add_argument("--fast-clean", action="store_true",
                    help="rebuild removing ONLY target/package (the documented `cargo package` "
                         "fingerprint poison), not the whole target/ — keeps the binary=HEAD "
                         "guarantee via cargo's own honest fingerprints while skipping the full "
                         "dependency recompile. NOT a cache: no external keying heuristic can "
                         "return a stale artifact. The tag verdict should still use the full "
                         "clean (default); this is for fast pre-tag hunt passes.")
    ap.add_argument("--state-url", default="",
                    help="state backend for EVERY cell (default: SQLite beside each config). "
                         "A gate pass grades ONE backend; run it twice to grade both.")
    ap.add_argument("--bless-local", action="store_true", help="re-capture verdict + duckdb-type goldens (implies --no-cloud)")
    ap.add_argument("--bless-cdc", action="store_true", help="re-capture the cdc state-snapshot golden")
    ap.add_argument("--bless-gifs", action="store_true",
                    help="re-capture docs/gifs/surface.lock.json AFTER re-rendering the GIFs")
    ns = ap.parse_args(argv)
    if ns.bless_local:
        ns.no_cloud = True
    return ns


def clean_tree_and_build(led: Ledger, *, fast: bool = False) -> bool:
    """Step ZERO: gate a binary built from nothing.

    A release must be graded on a tree with no history in it. `cargo package`
    alone can make every later build LIE about being fresh — it copies the crate
    to `target/package/<name>-<version>/` and builds THAT, leaving fingerprints
    that record the sources at frozen paths, so the next `cargo build` compares
    against a snapshot, finds nothing newer, prints `Fresh`, and produces a binary
    without your edits. Every signal you would use to check it is lied to too:
    `cargo test` passes, a deliberate type error compiles clean. Stale artifacts
    from an interrupted run are the same class with a smaller blast radius.

    So the gate starts by deleting `target/package`, the release profile and its own
    scratch, then builds `--release` itself: one release compile buys the guarantee
    that the binary every cell below exercises is the code in the tree.

    `fast=True` removes ONLY `target/package` — the exact directory the poison
    lives in — and keeps the release profile. Cargo's own fingerprints are honest
    once that snapshot is gone (every freshness-lie this repo has hit was
    package-related), so the binary=HEAD guarantee holds while the dependency
    recompile is skipped. This is NOT a compilation cache: there is no external
    hash-keying heuristic (sccache's build-script / env / include! edge cases) that
    can hand back a stale object — it is cargo's normal, sound incremental build
    with the one known liar deleted. The full clean stays the default for the tag
    verdict; `fast` is for the fast pre-tag hunt passes.
    """
    led.phase("Clean tree — the gate builds the binary it grades")
    for stale in ("/tmp/rivet_conc", "/tmp/rivet_iso", "/tmp/rivet_sweep", "/tmp/rivet_cdc_sweep"):
        shutil.rmtree(stale, ignore_errors=True)
    for lock in Path("/tmp").glob(".rivet_cdc_sweep*.lock"):
        lock.unlink(missing_ok=True)
    # `target/package` is the one known liar either way. Full mode also rebuilds the
    # RELEASE profile — the binary this gate grades — from nothing; the test and
    # coverage profiles keep their caches (cargo's fingerprints grade them honestly,
    # and wiping them cost every later cargo stage a cold build).
    shutil.rmtree(target_dir() / "package", ignore_errors=True)
    if not fast and not run(["cargo", "clean", "--release"], cwd=ROOT, timeout=600).ok:
        led.failed("-", "-", "clean_tree", "-",
                   "clean tree: `cargo clean --release` failed — the gate cannot vouch for "
                   "the binary it is about to grade")
        return False
    build = run(["cargo", "build", "--release"], cwd=ROOT, timeout=3600)
    if not build.ok or not rivet_bin().is_file():
        led.failed(
            "-", "-", "clean_tree", "-",
            f"clean tree: the release build FAILED on a clean tree — nothing below can mean "
            f"anything: {(build.stderr or build.stdout)[-300:]}",
        )
        return False
    how = ("target/package removed (fast: cargo fingerprints trusted, binary=HEAD)"
           if fast else "release profile + target/package removed and the release binary "
           "rebuilt from nothing")
    led.passed(
        "-", "-", "clean_tree", "-",
        f"clean tree: {how} ({rivet_bin()})",
    )
    return True


#: Held by a stage that MEASURES (timings, RSS, server counters against the previous release): nothing else runs beside it.
ALONE = "the whole machine"
#: The long-lived stand's source servers, driven by ONE runner at a time: a nextest run's test groups and the
#: upgrade lanes each serialise their own flock users and server-global settings, and neither sees the other's.
STAND = "the stand's source servers"
#: The test-profile build directory and the cores a compile takes.
CARGO = "cargo"
SERIAL = frozenset({STAND, CARGO})


@dataclass(frozen=True)
class Stage:
    """One gate stage: what it runs, what it holds exclusively (`needs`) and what it shares with other users (`uses`)."""

    name: str
    run: Callable[[Ledger], None]
    needs: frozenset[str]
    uses: frozenset[str] = frozenset()

    def conflicts(self, other: "Stage") -> bool:
        """Whether the two may not overlap: one measures, or one holds what the other holds or uses."""
        return bool(ALONE in self.needs | other.needs or self.needs & (other.needs | other.uses) or other.needs & self.uses)


def waves(stages: Sequence[Stage]) -> list[list[Stage]]:
    """The table cut into consecutive runs of stages that conflict with none of their wave: order is kept, a wave overlaps."""
    out: list[list[Stage]] = []
    for st in stages:
        if out and not any(st.conflicts(o) for o in out[-1]):
            out[-1].append(st)
        else:
            out.append([st])
    return out


def run_stages(led: Ledger, stages: Sequence[Stage]) -> None:
    """Run the table: a wave of one prints live, a wave of several runs side by side and prints in table order."""
    for wave in waves(stages):
        if len(wave) == 1:
            wave[0].run(led)
        else:
            run_concurrently(led, " + ".join(st.name for st in wave), [(st.name, st.run) for st in wave])


def build_stages(ns: argparse.Namespace, built: dict) -> list[Stage]:
    """The clean build and what only waits beside it."""
    build = [Stage("clean tree", lambda led: built.update(ok=clean_tree_and_build(led, fast=ns.fast_clean)),
                   frozenset({CARGO}))] if not ns.no_clean else []
    return [*build, Stage("object stores", start_stores, frozenset({"the object-store fakes"}))]


def gate_stages(ns: argparse.Namespace) -> list[Stage]:
    """Every stage after the build, in ledger order, with what it holds. Stages are resolved at call time (they are wrapped after import)."""
    state = dict(
        state_url=os.environ.get("RIVET_CDC_STATE_URL") or os.environ.get("RIVET_CONC_STATE_URL"),
        state_container=os.environ.get("RIVET_SWEEP_STATE_CONTAINER", "rivet-postgres-state-1"),
        src_container=os.environ.get("RIVET_CONC_SRC_CONTAINER", "rivet-postgres-1"),
        src_url=os.environ.get("RIVET_CONC_SRC_URL", "postgresql://rivet:rivet@localhost:5432/rivet"),
    )

    def serial(name: str, run: Callable[[Ledger], None]) -> Stage:
        return Stage(name, run, SERIAL)

    def measured(name: str, run: Callable[[Ledger], None]) -> Stage:
        return Stage(name, run, frozenset({ALONE}))

    def warehouse(name: str, run: Callable[[Ledger], None]) -> Stage:
        # Four nextest runs side by side since 2026-09: each waits on its own BigQuery dataset.
        return Stage(name, run, frozenset({f"the BigQuery dataset of {name}"}), SERIAL)

    table = [
        # FIRST: it reads the committed lock, which any later cargo command reconciles.
        Stage("release build path", lambda led: release_path.verify_release_build_path(led), frozenset({CARGO})),
        serial("state migrations", lambda led: scenarios.verify_state_migrations(led)),
        serial("network faults", lambda led: scenarios.verify_network_faults(led)),
        serial("tls required", lambda led: scenarios.verify_tls_required(led)),
        serial("tls downgrade", lambda led: tls_downgrade.verify_tls_downgrade_refused(led)),
        serial("auth", lambda led: scenarios.verify_auth(led)),
        serial("cdc standby", lambda led: scenarios.verify_cdc_standby(led)),
        serial("coverage matrices", lambda led: scenarios.verify_coverage_matrices(led)),
        serial("flag surface", lambda led: blessed_flow.verify_flag_surface(led)),
        serial("run strict", lambda led: blessed_flow.verify_run_strict(led)),
        serial("gif currency", lambda led: gifs.verify_gif_currency(led, bless=ns.bless_gifs)),
        serial("replica read", lambda led: scenarios.verify_replica_read(led)),
        serial("pool e2e", lambda led: scenarios.verify_pool_e2e(led)),
        serial("pool split", lambda led: scenarios.verify_pool_split(led)),
        serial("failed-run tail", lambda led: failure.verify_failed_run_tail(led)),
        serial("transient retry", lambda led: failure.verify_transient_retry_exact(led)),
        serial("batch resume", lambda led: scenarios.verify_batch_resume(led)),
        serial("partition footer", lambda led: scenarios.verify_partition_footer(led)),
        serial("audit suspects", lambda led: scenarios.verify_audit_suspects(led)),
        serial("cdc harm", lambda led: scenarios.verify_cdc_harm(led)),
        serial("session state", lambda led: scenarios.verify_session_state(led)),
        serial("cdc e2e", lambda led: cdc.verify_cdc_e2e(led)),
        serial("cdc differential", lambda led: cdc.verify_cdc_differential(led)),
        measured("release regression", lambda led: regression.verify_release_regression(led)),
        # The offline battery under llvm-cov compiles and reads no server; the upgrade cells wait on servers and BigQuery.
        Stage("live-only coverage", lambda led: scenarios.verify_live_only_coverage(led), frozenset({CARGO})),
        Stage("upgrade continuity", lambda led: upgrade.verify_upgrade_continuity(led), frozenset({STAND})),
        Stage("upgrade from a field state", lambda led: field_state.verify_upgrade_from_field_state(led), frozenset({STAND})),
        measured("perf regression", lambda led: perf.verify_perf_regression(led)),
        measured("source harm regression", lambda led: regression.verify_harm_regression(led)),
        measured("previous-release differential", lambda led: regression.verify_previous_release_differential(led)),
        measured("field symptom replay", lambda led: regression.verify_field_symptom_replay(led)),
        measured("scale memory", lambda led: regression.verify_scale_memory(led)),
        measured("flat rss", lambda led: guarantees.verify_flat_rss(led)),
        serial("byte-identical parts", lambda led: guarantees.verify_byte_identical_parts(led)),
        serial("state backend parity", lambda led: state_parity.verify_state_backend_parity(led, **state)),
        warehouse("shared state", lambda led: shared_state.verify_shared_state_same_name(led)),
        warehouse("warehouse layout", lambda led: warehouse_layout.verify_warehouse_layout(led)),
        warehouse("init delta", lambda led: init_delta.verify_init_delta(led)),
        warehouse("partner shape", lambda led: partner_shape.verify_partner_shape(led)),
        # Alone on the stand: its CDC cells would queue on the engine locks init delta holds.
        serial("clickhouse load", lambda led: clickhouse_load.verify_clickhouse_load(led)),
        serial("cdc schema drift", lambda led: cdc_schema_drift.verify_cdc_schema_drift(led)),
        serial("concurrent writers", lambda led: concurrency.verify_concurrent_writers_share_a_prefix(
            led, **state, bucket=os.environ.get("BQ_ORACLE_BUCKET", ""))),
        # Its own containers and ports; its CDC cells read the stand's CDC servers (blessed_flow).
        Stage("engine matrix", lambda led: engine_loop(led, ns), frozenset({"the gate's engine containers"}), SERIAL),
    ]
    if not ns.no_cloud:
        # Its own `bq`-tagged containers; handed this module's bring_up/seed_engine so readiness has one definition.
        table.append(Stage("bigquery golden", lambda led: bigquery.run_bigquery_golden(
            led, keep=ns.keep, parallel=ns.engine_parallel, bring_up=bring_up, seed_engine=seed_engine),
            frozenset({"the BigQuery golden's containers"})))
    # The cells a fix added or edited since the baseline, through the baseline: each must fail there.
    table.append(serial("fix cells", lambda led: fix_cells.verify_fix_cells(led)))
    # Last: it runs every live_suite test no stage above already ran.
    table.append(serial("live modules", lambda led: live_modules.verify_live_modules(led)))
    return table


def _usable_port(port: int) -> int:
    """The pinned host port when it is bindable, else a free ephemeral one (a leaked OrbStack forward holds it)."""
    import socket

    for want in (port, 0):
        with socket.socket() as s:
            try:
                s.bind(("0.0.0.0", want))
                return s.getsockname()[1]
            except OSError:
                continue
    return port


def _host_port_open(port: int) -> bool:
    """True when the published port accepts a TCP connect from the host, the path the seed and rivet use."""
    import socket

    try:
        socket.create_connection(("127.0.0.1", port), timeout=2).close()
        return True
    except OSError:
        return False


def bring_up(led: Ledger, engine: str, tag: str, image: str, port: int) -> str | None:
    """Start one engine×version and wait for it to answer. Returns its URL, or
    None when it could not start (the caller records a SKIP).

    The container name is a plain function of (engine, tag) — see
    `core.engine_container`. The bash equivalent had to keep that expansion on
    its own line, because a same-line `${eng}` read the ENCLOSING scope and so
    named the BigQuery stage's container after whichever engine the main loop had
    visited last."""
    name = engine_container(engine, tag)
    port = _usable_port(port)
    # `-v`, not a bare `-f`. Every engine image here declares a VOLUME for its
    # data directory, and `docker run` with no `-v` of our own answers that by
    # creating an ANONYMOUS volume. `docker rm -f` deletes the container and
    # ORPHANS that volume — so each gate run leaked one per engine×version
    # (~15), invisibly, because nothing ever lists them. Measured on this
    # machine before the fix: 814 anonymous volumes holding 370 GB, against 30
    # named ones, on a disk at 89%. The engines are seeded from scratch every
    # run, so there is nothing in them worth keeping past teardown.
    docker("rm", "-fv", name)
    # Engines get their own network, not `bridge`: a killed run can leave a phantom endpoint
    # for this name in `bridge` that no disconnect clears, and every later run then fails to
    # start the container. They are reached through published ports, so nothing else changes.
    docker("network", "create", "rivet-gate-engines")

    # Every engine ran with NO memory limit, and each then sized itself off the
    # WHOLE Docker VM rather than off what a 150k-row fixture needs. Measured on
    # this stand: SQL Server held 4,226 MiB with `max server memory` unbounded
    # (SQLOS target 9,954 MiB and climbing), and MongoDB's WiredTiger ceiling came
    # out at 19,544 MiB — exactly its documented default of 50% of (RAM − 1 GiB),
    # so the engine was obeying its own rule applied to a 39 GiB VM. PostgreSQL is
    # the counter-case and the reason this is about POLICY, not weight: 7 MiB of
    # its own plus reclaimable page cache, because `shared_buffers` is a fixed
    # 128 MB and does not scale with RAM.
    #
    # So the ceiling goes on the CONTAINER (kernel-enforced) and, where the engine
    # has its own knob, on the engine too — below the container limit, which is
    # Microsoft's own guidance, so the OS keeps headroom. 2 GB is the documented
    # minimum to START SQL Server on Linux.
    #
    # Tightened 2026-09-20 against what the engines ACTUALLY take under this
    # gate's 150k-row fixture, measured live during the matrix and cross-checked
    # against the long-lived stand containers (two independent samples each):
    #
    #   postgres  145-176 MiB (gate) / 146 MiB (stand)  -> 512m, ~3x headroom
    #   mongo     174-189 MiB (gate) / 225 MiB (stand)  -> 1g,   ~4x, and above
    #                                                      the 0.5 GiB cache pin
    #   mssql     1.67 GiB   (gate) / 2.91 GiB uncapped -> 3g/2048, still over
    #                                                      the documented floor
    #   mysql     509 MiB (8.0) / 644 MiB (8.4), measured in the 2026-09-21
    #             matrix — the "real number" the previous note said 2g was
    #             waiting for. 1g keeps ~1.5x over the larger sample, the same
    #             multiple the other three carry.
    #
    # Multiples, not tight fits: those samples are moments during the matrix, not
    # proven peaks, and a cap that OOM-kills a container mid-run surfaces as a
    # product failure rather than as a resource decision.
    args: list[str] = ["run", "-d", "--name", name, "--network", "rivet-gate-engines"]
    cmd: list[str] = []
    if engine == "postgres":
        args += ["--memory", "512m",
                 "-e", "POSTGRES_USER=rivet", "-e", "POSTGRES_PASSWORD=rivet",
                 "-e", "POSTGRES_DB=rivet", "-p", f"{port}:5432"]
    elif engine == "mysql":
        args += ["--memory", "2g",
                 "-e", "MYSQL_ROOT_PASSWORD=rivet", "-e", "MYSQL_DATABASE=rivet",
                 "-e", "MYSQL_USER=rivet", "-e", "MYSQL_PASSWORD=rivet", "-p", f"{port}:3306"]
    elif engine == "mssql":
        args += ["--memory", "3g", "-e", "MSSQL_MEMORY_LIMIT_MB=2048",
                 "-e", "ACCEPT_EULA=Y", "-e", "MSSQL_SA_PASSWORD=Rivet_Passw0rd!", "-p", f"{port}:1433"]
    elif engine == "mongo":
        args += ["--memory", "1g", "-p", f"{port}:27017"]
        cmd = ["--wiredTigerCacheSizeGB", "0.5"]
    elif engine == "oracle":
        # The compose `oracle` service's settings, grants script included (harm counters).
        args += ["--memory", "3g", "-e", "ORACLE_PASSWORD=rivet", "-e", "APP_USER=rivet",
                 "-e", "APP_USER_PASSWORD=rivet",
                 "-v", f"{ROOT / 'dev' / 'oracle' / 'init'}:/container-entrypoint-initdb.d:ro",
                 "-p", f"{port}:1521"]
    else:
        led.skipped(engine, tag, "all", "-", f"{engine}:{tag} unknown engine kind")
        return None

    started = docker(*args, image, *cmd)
    if not started.ok:
        led.skip(f"{engine}:{tag} could not start ({image}): {started.stderr.strip()[-200:]}")
        return None

    # One engine may need SEVERAL probe spellings across the versions this gate
    # pins. Mongo is the case: `mongosh` exists from 5.0 on, while 4.4 ships only
    # the legacy `mongo` shell — so a mongosh-only probe reports 4.4 as never
    # ready, which (now that the result is honoured) turns a perfectly healthy
    # server into a SKIP. The compose healthcheck makes the same fallback.
    # The probe must query the TARGET DATABASE, not merely the server.
    #
    # `pg_isready` reports whether the server ANSWERS, and a server that answers
    # `FATAL: database "rivet" does not exist` is answering — so it exits 0 while
    # the database the seed needs is still being created by the entrypoint. The
    # two-successes-a-second-apart rule below does not help: both land inside the
    # same window. Measured on postgres:14 — `pg_isready -U rivet` went true 3
    # polls before `rivet` was usable, and `pg_isready -U rivet -d rivet` (the
    # obvious fix, and wrong) still went true 14 polls early. That is what failed
    # this gate run: `postgres 14 seed FAIL … database "rivet" does not exist`,
    # a false NOT RELEASABLE with nothing wrong with the product.
    #
    # A real `select 1` against `rivet` is true exactly when the seed can connect,
    # and it subsumes the vanishing-temporary-server case the comment below
    # describes. Same reasoning for MySQL, whose `mysqladmin ping` answers before
    # the entrypoint has created `MYSQL_DATABASE`.
    probes: list[list[str]] = {
        "postgres": [["psql", "-U", "rivet", "-d", "rivet", "-tAc", "select 1"]],
        "mysql": [["mysql", "-urivet", "-privet", "rivet", "-e", "select 1"]],
        # tools18 is the 2022 image's path; the 2019 image ships plain
        # `mssql-tools`. A hardcoded path made the older version unrunnable and
        # the matrix recorded that as a coverage GAP — so the fix is one
        # fallback, not a permanent hole.
        #
        # The path is resolved LAZILY, below: this dict literal is built in
        # full before `[engine]` selects an arm, so calling `sqlcmd(name)`
        # here ran it against the POSTGRES container and killed the run on the
        # first postgres version with "no sqlcmd at tools18 or tools". A helper
        # that is correct for its own engine and fatal for the others is the
        # same shape as a feature wired into one runner of four.
        "mssql": [["__SQLCMD__", "-S", "localhost", "-U", "sa",
                   "-P", "Rivet_Passw0rd!", "-Q", "SELECT 1"]],
        "mongo": [["mongosh", "--quiet", "--eval", "db.runCommand({ping:1})"],
                  ["mongo", "--quiet", "--eval", "db.runCommand({ping:1})"]],
        # The APP_USER exists only once the entrypoint has created it, after the DB opens.
        "oracle": [["bash", "-c", "echo \"select 'rivet-ready' from dual;\" | sqlplus -s -L "
                    "rivet/rivet@localhost/FREEPDB1 | grep -q rivet-ready"]],
    }[engine]
    if engine == "mssql":
        probes = [[x for a in p for x in (sqlcmd(name) if a == "__SQLCMD__" else (a,))]
                  for p in probes]
    from .core import docker_exec, wait_until

    # TWO consecutive successes, a second apart — not one.
    #
    # The official Postgres/MySQL entrypoints start a TEMPORARY server to run
    # initdb and the init scripts, shut it down, and only then start the real
    # one. A single `pg_isready` can land on that temporary server, so the probe
    # says "ready" and the socket vanishes moments later; the seed then dies with
    # `connection to server on socket "…/.s.PGSQL.5432" failed: No such file or
    # directory`. Observed exactly that on postgres:14 in a full gate run.
    def ready() -> bool:
        if not _host_port_open(port):
            return False
        for probe in probes:
            if not docker_exec(name, *probe, timeout=20).ok:
                continue  # this spelling is absent on this version — try the next
            time.sleep(1.0)
            if docker_exec(name, *probe, timeout=20).ok:
                return True
        return False

    # …and the RESULT is honoured. Ignoring it meant proceeding to seed a server
    # that never came up, which surfaced as a seed error blaming the SQL rather
    # than the bring-up — the same "ignored boolean" shape this gate exists to
    # catch elsewhere.
    tries = 150 if engine == "oracle" else 45
    if not wait_until(ready, tries=tries, delay=2.0):
        # FAIL, not SKIP: an engine the gate was asked to grade and did not is a hole.
        led.failed(engine, tag, "all", "-",
                    f"{engine}:{tag} never became ready (no probe of "
                    f"{[p[0] for p in probes]} "
                    f"never passed twice in ~{tries * 2}s)", "not ready")
        return None

    if engine == "mssql":
        docker_exec(name, *sqlcmd(name), "-S", "localhost", "-U", "sa",
                    "-P", "Rivet_Passw0rd!", "-Q",
                    "IF DB_ID('rivet') IS NULL CREATE DATABASE rivet")

    if engine == "oracle":
        # processes=200 lets the listener's lagging handler count refuse a 16-cell connect burst (ORA-12516); measured 189/300 refused at 200, 0/300 at 1000.
        docker_exec(name, "sqlplus", "-s", "/", "as", "sysdba", timeout=180,
                    stdin="alter system set processes=1000 scope=spfile;\nshutdown immediate\nstartup\nexit\n")
        if not wait_until(ready, tries=60, delay=2.0):
            led.skipped(engine, tag, "all", "-", f"{engine}:{tag} not ready after the processes restart", "not ready")
            return None

    return matrix_cfg("url", engine).replace("%PORT%", str(port))


def seed_engine(engine: str, tag: str, url: str) -> str:
    """Seed one engine. Returns an error summary ("" when clean).

    Mongo is the ONLY engine seeded FROM THE HOST (pymongo); the SQL engines seed
    inside the container. That asymmetry is why the pymongo preflight exists —
    without it a host python lacking pymongo made every mongo version read
    NOT-RELEASABLE with no hint that the cause was the environment."""
    from .core import docker_exec

    name = engine_container(engine, tag)
    seed = ROOT / matrix_cfg("seed", engine)
    body = seed.read_text() if engine != "mongo" else ""
    if engine == "oracle":
        # The gate's own TIMESTAMP WITH TIME ZONE fixture, in the gate's own container only.
        body += "\n" + (ROOT / "dev" / "release-oracle" / "oracle_tz_probe.sql").read_text()

    if engine == "postgres":
        p = docker_exec(name, "psql", "-U", "rivet", "-d", "rivet", "-q",
                        "-v", "ON_ERROR_STOP=1", stdin=body, timeout=900)
    elif engine == "mysql":
        p = docker_exec(name, "mysql", "-urivet", "-privet", "rivet", stdin=body, timeout=900)
    elif engine == "oracle":
        p = docker_exec(name, "sqlplus", "-s", "-L", "rivet/rivet@localhost/FREEPDB1",
                        stdin=body, timeout=900)
    elif engine == "mssql":
        docker("cp", str(seed), f"{name}:/tmp/s.sql")
        p = docker_exec(name, *sqlcmd(name), "-S", "localhost", "-U", "sa",
                        "-P", "Rivet_Passw0rd!", "-d", "rivet", "-i", "/tmp/s.sql", timeout=900)
    else:  # mongo — from the host
        p = run(["python3", str(seed)], timeout=1800,
                env={"RIVET_MONGO_URI": url, "RIVET_SEED_USERS": "150000", "RIVET_SEED_ORDERS": "150000"})

    hay = p.out.lower()
    hits = [ln for ln in p.out.splitlines() if "error" in ln.lower() or "msg " in ln.lower()][:3]
    # The exit status is the primary signal; the grep only supplies detail. The
    # bash decided purely on the grep, so a seed that failed silently (non-zero,
    # no "error" in the text) read as seeded.
    if p.ok and not hits:
        return ""
    return "; ".join(hits) or f"exit {p.returncode}"


def _wanted_versions(spec: str, engine: str) -> set[str] | None:
    """Tags this run wants for `engine`, or None when the whole grid runs."""
    for part in (p.strip() for p in spec.split(";") if p.strip()):
        name, _, tags = part.partition("=")
        if name.strip() == engine:
            return {t.strip() for t in tags.split(",") if t.strip()} or None
    return None


def _run_one_engine(led: Ledger, ns: argparse.Namespace, engine: str) -> None:
    """One engine's whole leg: every gridded version brought up, seeded, and run
    through the scenarios, then torn down. Takes `led` so a parallel caller can
    hand it a BUFFERED sub-ledger (its output is flushed as one block afterwards)."""
    version_lines = [
        l for l in matrix_cfg("versions", engine).splitlines() if len(l.split()) >= 3
    ]
    # --versions: narrow this RUN, never the declared grid (see the flag's help).
    want = _wanted_versions(getattr(ns, "versions", ""), engine)
    if want is not None:
        have = {l.split()[0] for l in version_lines}
        missing = sorted(want - have)
        if missing:
            # Loud, not a SKIP: a typo'd tag that silently ran nothing would be a
            # green leg over zero versions — the exact vacuous pass this gate exists
            # to refuse.
            led.failed(engine, "-", "versions", "-",
                       f"--versions named {', '.join(missing)} for {engine}, which the "
                       f"matrix does not list (it has: {', '.join(sorted(have))})")
            return
        dropped = len(version_lines) - len(want)
        version_lines = [l for l in version_lines if l.split()[0] in want]
        if dropped:
            led.add(engine, "-", "other-versions", "-", Status.SKIP,
                    f"--versions: {dropped} other {engine} version(s) not run this pass")
    # --latest-only: keep just the last version of the family (the newest,
    # matrix.yaml lists them ascending). Cuts the dev-loop wall roughly in
    # proportion to the version count (postgres 4→1, mongo 5→1) while every
    # scenario × store × state cell still runs once.
    if ns.latest_only and version_lines:
        skipped = len(version_lines) - 1
        version_lines = version_lines[-1:]
        if skipped:
            led.add(engine, "-", "older-versions", "-", Status.SKIP,
                    f"--latest-only: {skipped} older {engine} version(s) not run")
    cap = max(1, getattr(ns, "version_parallel", 1))
    if cap == 1 or len(version_lines) <= 1:
        for line in version_lines:
            _run_one_version(led, ns, engine, line)
        return
    # Versions of one family in parallel: each owns its own container name
    # (engine×tag) and its own port from matrix.yaml, so nothing is shared but the
    # state backend — which the gate races on DELIBERATELY. Output would interleave,
    # so each version runs into a BUFFERED sub-ledger, flushed in MATRIX order (not
    # completion order) after the join, exactly as `engine_loop` does for engines.
    workers = min(cap, len(version_lines))
    # Say the setting OUT LOUD, the way the engine matrix phase does. Without it
    # the only way to answer "is --version-parallel actually in force?" mid-run is
    # to read the driver's argv out of the process table — which cost three failed
    # attempts through three different broken readers on 2026-09-20. A run must be
    # able to explain its own concurrency from its own log.
    led.ok(
        f"{engine}: {len(version_lines)} versions, up to {workers} concurrent "
        f"(--version-parallel {cap})"
    )
    run_lanes(led, [(line, _timed(f"{engine} {line.split()[0]}: version-total",
                                  lambda sub, line=line: _run_one_version(sub, ns, engine, line)))
                    for line in version_lines], workers=workers)


def _run_one_version(led: Ledger, ns: argparse.Namespace, engine: str, line: str) -> None:
    """One engine×version: brought up, seeded, run through the scenarios, torn down."""
    parts = line.split()
    tag, image, port = parts[0], parts[1], int(parts[2])
    led.phase(f"{engine} {tag} ({image})")
    with led.span(f"{engine}: bring-up"):
        url = bring_up(led, engine, tag, image, port)
    if not url:
        led.add(engine, tag, "all", "-", Status.FAIL, "bring-up failed")
        return
    # The seed is idempotent (DROP TABLE IF EXISTS …), so a transient
    # failure is retried: a fresh container under load can drop the seed
    # connection mid-stream, and crying wolf on that is worse than a retry.
    err = ""
    with led.span(f"{engine}: seed"):
      for attempt in range(3):
        err = seed_engine(engine, tag, url)
        if not err:
            break
        # Back off between attempts. Without this the three retries fire
        # back-to-back and all land inside the SAME startup window, so a
        # race the retry exists to absorb is retried three times in the
        # few hundred ms during which it cannot possibly succeed — which
        # is how a transient became a FAIL.
        time.sleep(2.0 * (attempt + 1))
    if err:
        led.failed(engine, tag, "seed", "-", f"{engine}:{tag} seed had errors", err)
        return
    led.ok("seeded")
    scenarios.run_scenarios(led, engine, tag, url)
    if not ns.keep:
        docker("rm", "-fv", engine_container(engine, tag))


def _timed(span: str, fn: Callable[[Ledger], None]) -> Callable[[Ledger], None]:
    """`fn` with its wall-clock recorded as `span` on the ledger it runs into."""
    def run_timed(sub: Ledger) -> None:
        with sub.span(span):
            fn(sub)
    return run_timed


def run_concurrently(led: Ledger, title: str, stages: list[tuple[str, Callable[[Ledger], None]]]) -> None:
    """Run independent gate stages at once, each into a buffered sub-ledger flushed in list order."""
    led.phase(title)
    run_lanes(led, [(name, _timed(f"{name}: stage-total", fn)) for name, fn in stages])


def engine_loop(led: Ledger, ns: argparse.Namespace) -> None:
    wanted = [e for e in ns.engines.split(",") if e] if ns.engines else None
    engines = [e for e in matrix_cfg("engines").split() if not wanted or e in wanted]
    cap = max(1, ns.engine_parallel)

    # Serial (unchanged behaviour) when asked or with nothing to overlap.
    if cap == 1 or len(engines) <= 1:
        for engine in engines:
            _run_one_engine(led, ns, engine)
        return

    # Parallel: each engine owns its own containers/ports; the scenarios race on
    # the shared state backend (a real concurrent-writer test). The big serial
    # cost — CDC capture-job waits — overlaps, so the wall drops toward the
    # slowest single engine. Output would interleave, so each engine runs into a
    # BUFFERED sub-ledger and its block is flushed IN ENGINE ORDER after the join.
    workers = min(cap, len(engines))
    led.phase(
        f"Engine matrix — {len(engines)} engines, up to {workers} concurrent "
        f"(wall ≈ slowest engine; scenarios race on the shared state backend)"
    )
    run_lanes(led, [(e, _timed(f"{e}: engine-total", lambda sub, e=e: _run_one_engine(sub, ns, e)))
                    for e in engines], workers=workers)


def _hold_gate_lock():
    """An exclusive lock on this tree's target dir for the whole run, or None if held."""
    import fcntl

    target_dir().mkdir(parents=True, exist_ok=True)
    fh = open(target_dir() / ".gate.lock", "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        return None
    return fh


def _warehouse_env() -> str:
    """Fill the BigQuery/GCS env every leg and live test reads from dev/stand/registry.yaml where the shell left it empty, when a Google credential is at hand; the line saying which."""
    from ..pytools.registry import bq_tmp, warehouse

    if not run(["gcloud", "auth", "print-access-token"]).ok:
        return ("BigQuery/GCS legs SKIP: no Google credential (`gcloud auth print-access-token` failed: "
                "`gcloud auth login` and `gcloud auth application-default login`)")
    project, bucket, location = warehouse()
    for var, value in (("BQ_ORACLE_PROJECT", project), ("BQ_ORACLE_BUCKET", bucket), ("BQ_ORACLE_DATASET", bq_tmp("gate"))):
        os.environ[var] = os.environ.get(var) or value
    for var, value in (("BIGQUERY_TEST_PROJECT", os.environ["BQ_ORACLE_PROJECT"]),
                       ("RIVET_TEST_GCS_BUCKET", os.environ["BQ_ORACLE_BUCKET"]), ("BIGQUERY_TEST_LOCATION", location)):
        os.environ[var] = os.environ.get(var) or value
    return f"warehouse: BigQuery {os.environ['BQ_ORACLE_PROJECT']} ({location}), GCS bucket {os.environ['BQ_ORACLE_BUCKET']}"


def main(argv: list[str] | None = None) -> int:
    if (argv if argv is not None else sys.argv[1:]) == ["--self-test"]:
        return _self_test()
    ns = parse_args(argv)
    from .core import scrub_inherited_rivet_env
    dropped = scrub_inherited_rivet_env()
    if dropped:
        print(f"  dropped from the inherited environment (the product's, not the gate's): {' '.join(dropped)}")
    lock = _hold_gate_lock()
    if lock is None:
        print(f"another gate run holds {target_dir() / '.gate.lock'} — two runs in one tree clean "
              "and rebuild each other's binary; wait for it or stop it", file=sys.stderr)
        return 2
    from .core import set_cell_parallel
    set_cell_parallel(ns.cell_parallel)

    # A clean-tree run builds the binary itself; only --no-clean needs one up front.
    if ns.no_clean and (not rivet_bin().is_file() or not os.access(rivet_bin(), os.X_OK)):
        print(f"rivet binary not found at {rivet_bin()} (build --release or set RIVET_BIN)", file=sys.stderr)
        return 2
    if not have("duckdb"):
        print("the pinned duckdb package is not importable — run the gate through `uv run` (make does)", file=sys.stderr)
        return 2
    # Fail LOUD on the host-side mongo seed dependency, and only when mongo is in
    # scope: a python3 that cannot import pymongo otherwise turns every mongo
    # version into a cryptic "seed had errors" and the gate into NOT-RELEASABLE,
    # with nothing pointing at the environment as the cause.
    engines_wanted = [e for e in ns.engines.split(",") if e]
    if (not engines_wanted or "mongo" in engines_wanted) and not run(["python3", "-c", "import pymongo"]).ok:
        exe = shutil.which("python3") or "python3"
        print(f"mongo seed needs pymongo but `{exe}` can't import it — `pip install pymongo`, "
              "or fix PATH (a login shell may shadow a pyenv shim with /usr/local/bin/python3).",
              file=sys.stderr)
        return 2

    print(f"  {_warehouse_env()}")
    os.environ.setdefault("BLESS_VERDICTS", "1" if ns.bless_local else "0")
    os.environ.setdefault("BLESS_DUCKDB", "1" if ns.bless_local else "0")
    os.environ.setdefault("BLESS_CDC", "1" if ns.bless_cdc else "0")

    record_stage_runs(_gate_modules())
    led = Ledger()
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-"))
    os.environ["WORK"] = str(work)
    # Every live test this gate starts appends its rig oracle verdict here; verify_oracle_verdict_census grades it.
    os.environ["RIVET_ORACLE_LOG"] = str(work / "rivet-oracle.log")
    try:
        from datetime import datetime, timezone

        led.phase(f"Rivet Release Oracle — {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")

        # WHICH STATE BACKEND IS BEING GRADED, said out loud.
        #
        # `run()` merges os.environ into every cell's env, so one variable decides
        # the backend for the WHOLE gate — and until it was printed, nothing in a
        # green report told you which one. That matters: a SQLite pass does not
        # cover Postgres, and the two have diverged before (shape tracking never
        # worked on Postgres at all). Grading both is two passes, deliberately,
        # rather than a subset of cells quietly touching the other backend.
        state_url = ns.state_url or os.environ.get("RIVET_GATE_STATE_URL", "")
        # One state DB per gate run (RIVET_GATE_SHARED_STATE=1 keeps the shared one): a build
        # that bumps the state schema must not migrate the stand DB other branches still open.
        if state_url and os.environ.get("RIVET_GATE_SHARED_STATE") != "1":
            from .core import isolate_state_db
            own = isolate_state_db(state_url, f"{os.getpid()}")
            if own is None:
                led.failed("-", "-", "state-isolation", "-",
                           f"could not create a per-run state DB beside {state_url.split('@')[-1]}")
            else:
                state_url = own
                for var in ("RIVET_GATE_STATE_URL", "RIVET_CDC_STATE_URL",
                            "RIVET_CONC_STATE_URL", "RIVET_TEST_STATE_URL"):
                    os.environ[var] = own
                import urllib.parse as _up
                t = _up.urlsplit(own)
                os.environ["RIVET_TEST_STATE_TOXI_URL"] = _up.urlunsplit(
                    (t.scheme, f"{t.username}:{t.password}@127.0.0.1:15433", t.path, "", ""))
                print(f"  per-run state DB: {t.path.lstrip('/')} (dropped at exit)")
        # RIVET_STATE_URL reaches the cells the gate spawns itself; RIVET_GATE_STATE_URL is
        # how the Rust harness learns the backend under test and re-adds it to every rivet
        # it spawns (it strips the shell's RIVET_* first). Both say the same thing.
        if state_url:
            os.environ["RIVET_STATE_URL"] = state_url
            os.environ["RIVET_GATE_STATE_URL"] = state_url
            backend = f"POSTGRES ({state_url.split('@')[-1]})"
        else:
            os.environ.pop("RIVET_STATE_URL", None)
            os.environ.pop("RIVET_GATE_STATE_URL", None)
            backend = "SQLITE (a .rivet_state.db beside each config — the default)"
        print(f"  state backend under test: {backend}")
        print("  a pass grades ONE backend; --state-url runs the same cells against the other")
        # The Rig cells are graded by parsing nextest; a parser that reads `FAIL + LEAK`
        # as a pass turns red tests green, so it is checked before anything is graded.
        verify_nextest_grading(led)

        # WHETHER THE RELEASE IS BEING GRADED AGAINST THE PREVIOUS ONE, said out
        # loud — for the same reason the state backend is. The stages read this
        # from the environment (that is how every gate-wide knob reaches them and
        # how `run()` passes it to children), so argv and env are one switch.
        #
        # Both this banner and the closing line come from ONE pure function keyed
        # on the BASELINE, never on the flag alone: the flag does not decide (see
        # `regression.prev_release_banner`).
        regression.set_without_prev_release_comparison(ns.without_prev_release_comparison)
        scenarios.set_without_replica_topologies(ns.without_replica_topologies)
        if ns.without_replica_topologies:
            print("  replica topologies: GIVEN UP by --without-replica-topologies — a down stand "
                  "records SKIP; this run cannot support a tag")
        banner, footer = regression.prev_release_banner(
            regression.prev_binary(), ns.without_prev_release_comparison)
        for line in banner:
            print(line)

        built: dict = {}
        run_stages(led, build_stages(ns, built))
        if not ns.no_clean and not built.get("ok"):
            return 1
        version = rivet("--version").stdout.splitlines()
        print(f"  rivet: {rivet_bin()} ({version[0] if version else 'unknown'})")
        run_stages(led, gate_stages(ns))
        skip_census.verify_oracle_verdict_census(led)
        verify_seeded_recall(led, ns.with_seeded_recall)
        verify_no_invariant_violations(led)
        # Only a FULL run can say a known red no longer fires, and only one writes a verdict line.
        full = not (ns.engines or ns.versions or ns.no_cloud or ns.latest_only)
        if full:
            led.close_known_red()
            # A bless run returns before most stages on purpose (run_scenarios), so it cannot grade the matrix.
            if not (ns.bless_local or ns.bless_cdc):
                led.close_matrix_rows()
        rc = led.report(full=full and not (ns.bless_local or ns.bless_cdc))
        # A run that graded nothing against the previous release has to say so
        # AFTER the verdict, where the reader's eye lands: `RELEASE-READY` is
        # derived from the rows and is literally true ("every non-skipped cell is
        # green"), which is exactly the sentence 0.24.4 shipped under. The
        # condition is the BASELINE, not the flag — a run that carried a baseline
        # compared against it and must not be told it did not.
        if footer:
            print(footer)
        return rc
    finally:
        if not ns.keep:
            remove_engine_containers()
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
