"""Upgrade continuity per load-mode family x engine x warehouse, graded by the independent oracle.

One row per arm of `load_mode_of` (src/load/plan.rs), read from the code: the previous release's
`init` writes the config, the previous release runs and loads it, the source changes, this build
runs and loads the same config, and `rig_oracle.grade_load` compares the source with the warehouse
table in the one DuckDB session. A delta family loaded as an overwrite after the upgrade leaves the
warehouse holding the last delta only, which the oracle reports by row.

    RIVET_PREV_RELEASE_BIN=<old rivet> python -m dev.release_oracle.upgrade_matrix [engine,…] [target,…]
    python -m dev.release_oracle.upgrade_matrix --self-test
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path

from .core import ROOT, Ledger, rivet_bin, run, run_lanes, server_of
from .engines import sql as _sql

SCEN = "upgrade_continuity"
TARGETS = ("clickhouse", "bigquery")
ENGINES = ("postgres", "mysql", "mssql", "oracle", "mongo")
#: The primary key every seeded table declares, as the engine's catalog spells it.
KEY = {"postgres": "id", "mysql": "id", "mssql": "id", "oracle": "ID", "mongo": "_id"}
CH_URL, CH_USER, CH_PASSWORD = "http://127.0.0.1:8123", "rivet", "rivet"
#: An arm's guard, by the identifier it calls, as the commented line `init` scaffolds for the
#: operator to switch it on (`None`: init emits no such line, so the row is a named SKIP).
OPT_IN = {
    ("Chunked", "keyset_incremental"): "keyset_incremental: true",
    ("Chunked", "continued_key"): "keyset_incremental: true",
    ("Full", "continued_key"): None,
}
#: Families this matrix does not build, with the reason the ledger prints.
NOT_HERE = {
    "Cdc": "CDC streams cross the upgrade in upgrade_cdc_load.cdc_load_cells (BigQuery only; "
           "no ClickHouse CDC upgrade cell exists)",
    "TimeWindow": "a time_window load holds the run's clock window and the oracle grades whole tables",
}


def load_mode_arms(src: str) -> list[tuple[str, str, str]]:
    """(export mode, guard identifier or "", load mode) for every arm of `load_mode_of` in `src`."""
    m = re.search(r"pub fn load_mode_of\b.*?\bmatch export\.mode \{(.*?)\n    \}\n\}", src, re.S)
    if not m:
        raise ValueError("load_mode_of's match was not found in src/load/plan.rs")
    body = re.sub(r"//[^\n]*", "", m.group(1))
    arms = []
    for pats, guard, load in re.findall(
            r"((?:(?:crate::config::)?ExportMode::\w+\s*\|?\s*)+)(?:if\s+(.+?))?=>\s*\{?\s*LoadMode::(\w+)", body, re.S):
        call = re.search(r"(\w+)\s*\(", guard)
        ident = call.group(1) if call else (re.findall(r"\w+", guard) or [""])[-1]
        for mode in re.findall(r"ExportMode::(\w+)", pats):
            arms.append((mode, ident, load))
    if not arms:
        raise ValueError("load_mode_of has no parsable arm")
    return arms


def rows() -> list[tuple[str, str, str]]:
    """The families of the tree under test, from its own `load_mode_of`."""
    return load_mode_arms((ROOT / "src/load/plan.rs").read_text())


def _warehouse() -> tuple[str, str, str]:
    """(BigQuery project, GCS bucket, location): the gate's override, else dev/stand/registry.yaml."""
    from ..pytools import registry

    if hasattr(registry, "warehouse"):
        return registry.warehouse()
    reg = registry.load()
    return (os.environ.get("BQ_ORACLE_PROJECT") or reg["bigquery"]["project"],
            os.environ.get("BQ_ORACLE_BUCKET") or reg["gcs"]["bucket"], reg["bigquery"]["location"])


def _ch(sql: str) -> str | None:
    """One statement against the stand's ClickHouse; None on error."""
    from .perf import _ch as ch

    return ch(sql)


def _seed(engine: str, url: str, table: str) -> bool:
    """The family's source table: ids 1..ROWS with a cursor column."""
    from .cdc import _mongosh
    from .upgrade import ROWS, _seed as seed_sql

    if engine != "mongo":
        return seed_sql(engine, url, table, ROWS, with_cursor=True)
    js = (f"db.{table}.drop(); db.{table}.insertMany(Array.from({{length: {ROWS}}}, (_, i) => "
          f"({{_id: i + 1, v: i + 1, updated_at: new Date(Date.UTC(2026, 0, 1) + (i + 1) * 1000)}})))")
    return _mongosh(url, js).ok


def _change(engine: str, url: str, table: str, append_only: bool) -> bool:
    """300 new ids and (unless the family reads past its last key only) 50 changed values, each with a newer cursor."""
    from .cdc import _mongosh
    from .upgrade import ROWS, _mutate

    if engine != "mongo" and not append_only:
        return _mutate(engine, url, table)
    if engine != "mongo":
        from .upgrade import _seed as seed_sql

        tmp = f"{table}_add"
        cols = "id, v, updated_at"
        ok = seed_sql(engine, url, tmp, ROWS + 300, with_cursor=True)
        ok = ok and _sql(engine, url, f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {tmp} WHERE id > {ROWS}").ok
        _drop(engine, url, tmp)
        return ok
    js = (f"db.{table}.insertMany(Array.from({{length: 300}}, (_, i) => ({{_id: {ROWS} + i + 1, v: {ROWS} + i + 1, "
          f"updated_at: new Date(Date.UTC(2026, 5, 1) + i * 1000)}})));")
    if not append_only:
        js += (f" db.{table}.updateMany({{_id: {{$lte: 50}}}}, [{{$set: {{v: {{$multiply: ['$v', -1]}}, "
               "updated_at: new Date(Date.UTC(2026, 6, 1))}}]);")
    return _mongosh(url, js).ok


def _drop(engine: str, url: str, table: str) -> None:
    """Drop the source table."""
    from .cdc import _mongosh

    if engine == "mongo":
        _mongosh(url, f"db.{table}.drop()")
    elif engine == "oracle":
        _sql(engine, url, f"DROP TABLE {table.upper()} PURGE")
    else:
        _sql(engine, url, f"DROP TABLE IF EXISTS {table};")


def opt_in(cfg_text: str, line: str) -> str | None:
    """`cfg_text` with init's commented `# <line>` switched on, or None when init did not scaffold it."""
    pat = re.compile(rf"^(\s*)#\s*{re.escape(line)}", re.M)
    return pat.sub(rf"\g<1>{line}", cfg_text, count=1) if pat.search(cfg_text) else None


def verdict_status(verdict: object) -> tuple[str, str]:
    """('skip'|'fail'|'pass', detail) from a grade_load verdict; any other shape is an oracle error."""
    if not isinstance(verdict, dict):
        raise TypeError(f"oracle verdict is not an object: {verdict!r}")
    if "skip" in verdict:
        if not isinstance(verdict["skip"], str):
            raise TypeError(f"oracle verdict `skip` is not a string: {verdict!r}")
        return "skip", verdict["skip"]
    fails = verdict.get("failures")
    if not isinstance(fails, list) or not all(isinstance(f, str) for f in fails):
        raise TypeError(f"oracle verdict has no `failures` list of strings: {verdict!r}")
    if fails:
        return "fail", " | ".join(fails)
    partial = verdict.get("partial")
    return "pass", f"{verdict.get('facts')}" + (f"; PARTIAL: {partial}" if partial else "")


def cell(led: Ledger, prev: Path, root: Path, engine: str, url: str, family: tuple[str, str, str],
         target: str) -> None:
    """One row: the previous release's init, run and load; a change; this build's run and load; graded."""
    import yaml

    from . import gcp
    from .rig_oracle import grade_load
    from ..pytools.registry import bq_tmp

    mode, guard, load_mode = family
    name = f"upgrade[{engine}/{mode}{'+' + guard if guard else ''}->{load_mode}/{target}]"
    store = f"load-{target}"
    if mode in NOT_HERE:
        return led.skipped(engine, "-", SCEN, store, f"{name}: {NOT_HERE[mode]}", "not here")
    if guard and (mode, guard) not in OPT_IN:
        return led.failed(engine, "-", SCEN, store, f"{name}: load_mode_of has an arm this matrix cannot "
                          "build — add its shape to upgrade_matrix.OPT_IN", "unmapped arm")
    line = OPT_IN.get((mode, guard)) if guard else ""
    if line is None:
        return led.skipped(engine, "-", SCEN, store, f"{name}: `rivet init` scaffolds no line for this "
                           "arm's guard, so no generated config reaches it", "no init line")
    proj, bucket, location = _warehouse()
    tag = f"{engine[:2]}{mode[:2].lower()}{'k' if guard else ''}{target[:2]}_{os.getpid()}"
    from .upgrade import _case

    table = _case(engine, f"upgm_{tag}")
    d = root / f"matrix_{tag}"
    d.mkdir(parents=True)
    env = {"RIVET_UPG_URL": url, "RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": "",
           "CLICKHOUSE_PASSWORD": CH_PASSWORD}
    db = bq_tmp(f"upgm_{tag}")
    try:
        if not _seed(engine, url, table):
            return led.failed(engine, "-", SCEN, store, f"{name}: seed failed", "seed")
        if target == "clickhouse":
            if _ch(f"CREATE DATABASE IF NOT EXISTS {db}") is None:
                return led.skipped(engine, "-", SCEN, store, f"{name}: ClickHouse on :8123 is down", "no clickhouse")
            wh = ["--clickhouse-url", CH_URL, "--clickhouse-database", db, "--clickhouse-user", CH_USER]
        else:
            gcp.bq_ensure_dataset(proj, db, location)
            wh = ["--bigquery-project", proj, "--bigquery-dataset", db]
        init = run([str(prev), "init", "--source-env", "RIVET_UPG_URL", "--table", table, "--mode",
                    mode.lower(), "--gcs-bucket", bucket, *wh,
                    "-o", "c.yaml"], env=env, cwd=d)
        if not init.ok:
            return led.skipped(engine, "-", SCEN, store, f"{name}: the previous release's init refuses it: "
                               f"{init.why}", "init refused")
        cfg_path = d / "c.yaml"
        if line:
            text = opt_in(cfg_path.read_text(), line)
            if text is None:
                return led.skipped(engine, "-", SCEN, store, f"{name}: the previous release's init scaffolds "
                                   f"no `# {line}` here", "no init line")
            cfg_path.write_text(text)
        for binary, step in ((prev, "run"), (prev, "load"), (None, "change"), (rivet_bin(), "run"),
                             (rivet_bin(), "load")):
            if binary is None:
                if not _change(engine, url, table, append_only=bool(guard)):
                    return led.failed(engine, "-", SCEN, store, f"{name}: the source change failed", "change")
                continue
            p = run([str(binary), step, "-c", "c.yaml"], env=env, cwd=d, timeout=None)
            if not p.ok:
                who = "previous" if binary == prev else "this"
                return led.failed(engine, "-", SCEN, store, f"{name}: {step} by {who} failed: "
                                  f"{p.why}", step)
        cfg = yaml.safe_load(cfg_path.read_text())
        export = cfg["exports"][0]
        spec = {"engine": engine, "url": url, "database": url.rsplit("/", 1)[-1].split("?")[0],
                "table": export.get("table"), "query": export.get("query"), "mode": "batch", "key": [KEY[engine]], "overrides": {},
                "cursor_expr": None, "state": str(d / ".rivet_state.db"), "load": cfg["load"],
                "password": CH_PASSWORD if target == "clickhouse" else "", "export": export["name"],
                "verb": "load", "delta": load_mode == "Incremental", "snapshot": False, "capture_instance": None}
        try:
            status, detail = verdict_status(grade_load(spec))
        except Exception as e:  # noqa: BLE001 — an oracle error is a FAIL, never a pass
            return led.failed(engine, "-", SCEN, store, f"{name}: oracle error: {type(e).__name__}: {str(e)[:300]}",
                              "oracle error")
        {"skip": led.skipped, "fail": led.failed, "pass": led.passed}[status](
            engine, "-", SCEN, store, f"{name}: {detail}", status)
    finally:
        _drop(engine, url, table)
        if target == "clickhouse":
            _ch(f"DROP DATABASE IF EXISTS {db}")
        elif not os.environ.get("RIVET_UPG_KEEP"):
            gcp.bq_delete_dataset(proj, db)
        gcp.gcs_delete_prefix(bucket, f"exports/{table}/")
        shutil.rmtree(d, ignore_errors=True)


def matrix_lane_cells(prev: Path, root: Path, engines: tuple[str, ...] = ENGINES,
                      targets: tuple[str, ...] = TARGETS) -> list[tuple[object, object]]:
    """Every family of `load_mode_of` x every engine with a gate URL x every warehouse, as `(lane, fn)`: the lane is the source server."""
    cells: list[tuple[object, object]] = []
    for family in rows():
        for engine in engines:
            uvar = f"RIVET_ORACLE_{engine.upper()}_URL"
            url = os.environ.get(uvar, "")
            for target in targets:
                if not url:
                    cells.append((None, lambda led, e=engine, f=family, t=target, v=uvar: led.skipped(
                        e, "-", SCEN, f"load-{t}", f"upgrade[{e}/{f[0]}]: no {v}", "no url")))
                    continue
                cells.append((server_of(url), lambda led, e=engine, u=url, f=family, t=target: cell(
                    led, prev, root, e, u, f, t)))
    return cells


def matrix_cells(led: Ledger, prev: Path, root: Path, engines: tuple[str, ...] = ENGINES,
                 targets: tuple[str, ...] = TARGETS) -> None:
    """The matrix alone: one lane per source server."""
    run_lanes(led, matrix_lane_cells(prev, root, engines, targets))


def _self_test() -> None:
    """The arm parser reads main's and the Mongo-resume branch's shapes; the opt-in and verdict readers are strict."""
    main = """pub fn load_mode_of(export: &crate::config::ExportConfig) -> LoadMode {
    match export.mode {
        crate::config::ExportMode::Cdc => LoadMode::Cdc,
        crate::config::ExportMode::Full => LoadMode::Full, // whole result set
        // only the keys past the last run's: a delta, never the whole table
        crate::config::ExportMode::Chunked if export.keyset_incremental => LoadMode::Incremental,
        crate::config::ExportMode::Chunked => LoadMode::Full, // parallel full snapshot
    }
}"""
    assert load_mode_arms(main) == [("Cdc", "", "Cdc"), ("Full", "", "Full"),
                                    ("Chunked", "keyset_incremental", "Incremental"), ("Chunked", "", "Full")]
    branch = """pub fn load_mode_of(
    config: &crate::config::Config,
    export: &crate::config::ExportConfig,
) -> LoadMode {
    use crate::config::ExportMode;
    match export.mode {
        ExportMode::Incremental => LoadMode::Incremental,
        ExportMode::Full | ExportMode::Chunked
            if crate::plan::build::continued_key(config, export).is_some() =>
        {
            LoadMode::Incremental
        }
        ExportMode::TimeWindow => LoadMode::Full, // the current window, whole
    }
}"""
    assert load_mode_arms(branch) == [("Incremental", "", "Incremental"), ("Full", "continued_key", "Incremental"),
                                      ("Chunked", "continued_key", "Incremental"), ("TimeWindow", "", "Full")]
    tree = rows()
    assert {m for m, _, _ in tree} >= {"Full", "Incremental", "Chunked", "TimeWindow", "Cdc"}, tree
    assert all((m, g) in OPT_IN for m, g, _ in tree if g), f"an arm of this tree has no shape: {tree}"
    assert opt_in("x:\n    # keyset_incremental: true  # why\n", "keyset_incremental: true") == \
        "x:\n    keyset_incremental: true  # why\n"
    assert opt_in("x: 1\n", "keyset_incremental: true") is None
    assert verdict_status({"failures": [], "facts": {"n": 1}})[0] == "pass"
    assert verdict_status({"failures": ["COUNT: 1 vs 2"]}) == ("fail", "COUNT: 1 vs 2")
    assert verdict_status({"skip": "no table"}) == ("skip", "no table")
    for bad in ({"failure": []}, {"failures": "x"}, {"failures": [{"x": 1}]}, {"skip": 1}, []):
        try:
            verdict_status(bad)
        except TypeError:
            continue
        raise AssertionError(f"an unknown verdict shape read as a verdict: {bad!r}")
    print("self-test ok: upgrade matrix families are read from load_mode_of; verdict shapes are strict")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        _self_test()
        raise SystemExit(0)
    import tempfile

    from .regression import _require_prev_binary

    _led = Ledger()
    _prev = _require_prev_binary(_led, "all", "-", SCEN, "warehouse", "upgrade matrix")
    if _prev is not None:
        _eng = tuple((sys.argv[1:2] or [",".join(ENGINES)])[0].split(","))
        _tgt = tuple((sys.argv[2:3] or [",".join(TARGETS)])[0].split(","))
        matrix_cells(_led, _prev, Path(tempfile.mkdtemp(prefix="rivet-oracle-upgm-")), _eng, _tgt)
    raise SystemExit(_led.report())
