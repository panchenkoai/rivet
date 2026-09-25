"""The partner shape, as the OPERATOR gets it: `rivet init --mode cdc` over several
tables → the scaffold IS the config → anchor + backfill → load → delta → load.

Every other CDC cell hand-writes its config. This one starts from what `rivet init`
emits — one batch RECIPE per table plus one `tables:` stream with `backfill: auto`
— because that is the config a partner runs: six hot tables, one binlog stream,
BigQuery. A scaffold that parses but cannot complete the cycle (a recipe the
stream cannot pair, a `pk: auto` the load cannot resolve per table) is the
defect this cell exists to catch, and no unit test of the scaffold sees it.

Per engine whose `RIVET_CDC_<ENGINE>_URL` is set (MySQL and PostgreSQL — the two
init consolidates into one stream):

1. seed THREE tables (`id` PK + `v`) with five rows each;
2. `rivet init --source-env … --mode cdc --include t0 t1 t2 --gcs-bucket …` —
   the SHAPE is asserted (`backfill: auto`, no `initial: snapshot`, a recipe per
   table, one `mode: cdc`), then `load:` is appended (init never writes one);
3. `doctor`, `check`;
4. run 1 = anchor + every table's baseline → load 1 → per table: BigQuery
   COUNT == source COUNT == 5;
5. a delta in every table (3 inserts, 1 update, 1 delete) → run 2 → load 2 →
   per table: the `__changes` buffer holds exactly the 5 changes, the base still
   the 5-row baseline → `compact` → per table: live rows (`WHERE NOT
   __is_deleted`) == source == 7, one flagged tombstone, the buffer dropped.

Oracles: BigQuery over REST (`gcp.bq_scalar`, never rivet) and a re-query of the source. SKIP — never a
silent pass — without the engine URL, `gcloud` (the REST token) or a project. Cleans up the
warehouse tables, the GCS prefix, the source tables and (PostgreSQL) the slot.
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

from .cdc import _mysql, _psql
from ..pytools import registry
from . import gcp
from .core import Ledger, have, rivet, run
from .scenarios import NO_TIMEOUT, work_dir

# Per-invocation TABLE names, so a concurrent invocation's `DROP TABLE` cannot hit
# ours. Honest scope: the BigQuery dataset (`rivet_partner_<engine>`), the
# PostgreSQL slot init derives (`rivet_public_cdc`) and MySQL's `server_id` (4271)
# are per-STAND singletons — two invocations of this cell on one stand still
# interfere on those, and a killed invocation leaves its pid-named tables behind.
# One gate per stand at a time is the contract, as for every other CDC cell here.
TABLES = [f"orc_ps_{s}_{os.getpid()}" for s in ("a", "b", "c")]
SEED = 5
DELTA_LIVE = 7  # 5 seeded + 3 inserted - 1 deleted


def _ids(k: int, lo: int, hi: int) -> list[int]:
    """Table k holds ids k*100+lo..hi — no two tables share a row, so a fan-out that
    routes one table's events under another's prefix shows in SUM(id) (counts agree)."""
    return [k * 100 + i for i in range(lo, hi + 1)]


def _sql(engine: str, url: str, sql: str):
    return _mysql(url, sql) if engine == "mysql" else _psql(url, "-tA", sql=sql)


def _scalar(stdout: str) -> int:
    lines = [ln.strip() for ln in (stdout or "").splitlines() if ln.strip().lstrip("-").isdigit()]
    return int(lines[-1]) if lines else -1


def _count(engine: str, url: str, table: str) -> int:
    return _scalar(_sql(engine, url, f"SELECT COUNT(*) FROM {table};").stdout)


def _sum_id(engine: str, url: str, table: str) -> int:
    return _scalar(_sql(engine, url, f"SELECT COALESCE(SUM(id), 0) FROM {table};").stdout)


def _bq_scalar(proj: str, dset: str, expr: str, table: str, where: str = "") -> int:
    return _scalar(gcp.bq_scalar(proj, f"SELECT {expr} FROM `{proj}.{dset}.{table}` {where}") or "")


def _bq_count(proj: str, dset: str, table: str, where: str = "") -> int:
    return _bq_scalar(proj, dset, "COUNT(*)", table, where)


def _bq_table_exists(proj: str, dset: str, table: str) -> bool:
    return _bq_scalar(proj, dset, "COUNT(*)", "INFORMATION_SCHEMA.TABLES",
                      f"WHERE table_name = '{table}'") > 0


def _seed(engine: str, url: str) -> bool:
    for k, t in enumerate(TABLES):
        rows = ", ".join(f"({i},{i})" for i in _ids(k, 1, SEED))
        ddl = f"DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id INT PRIMARY KEY, v INT); INSERT INTO {t} (id, v) VALUES {rows};"
        if not _sql(engine, url, ddl).ok:
            return False
    return True


def _delta(engine: str, url: str) -> None:
    for k, t in enumerate(TABLES):
        rows = ", ".join(f"({i},{i})" for i in _ids(k, 6, 8))
        one, two = _ids(k, 1, 1)[0], _ids(k, 2, 2)[0]
        _sql(engine, url,
             f"INSERT INTO {t} (id, v) VALUES {rows}; "
             f"UPDATE {t} SET v = 100 WHERE id = {one}; DELETE FROM {t} WHERE id = {two};")


def _cleanup(engine: str, url: str, slot: str | None) -> None:
    for t in TABLES:
        _sql(engine, url, f"DROP TABLE IF EXISTS {t};")
    if engine == "postgres" and slot:
        _psql(url, sql=f"SELECT pg_drop_replication_slot('{slot}') FROM pg_replication_slots "
                       f"WHERE slot_name='{slot}';")


def _shape_problems(body: str) -> list[str]:
    """What the scaffold must say to be the partner shape — each miss is named."""
    bad: list[str] = []
    if "backfill: auto" not in body:
        bad.append("no `backfill: auto`")
    if "initial: snapshot" in body:
        bad.append("still `initial: snapshot` (the single-stream snapshot leg)")
    if body.count("mode: cdc") != 1:
        bad.append(f"{body.count('mode: cdc')} cdc exports, want 1")
    for t in TABLES:
        if not re.search(rf"^\s*- name: {t}\s*$", body, re.M):
            bad.append(f"no recipe export for {t}")
        if not re.search(rf"^\s*table: {t}\s*$", body, re.M):
            bad.append(f"recipe for {t} does not read by `table:`")
    if "tables: [" not in body:
        bad.append("no `tables:` stream")
    return bad


def verify_partner_shape(led: Ledger) -> None:
    led.phase("Partner shape — `rivet init --mode cdc` over 3 tables → anchor + backfill → load → delta → load (BigQuery)")
    proj = os.environ.get("BQ_ORACLE_PROJECT") or run(["gcloud", "config", "get-value", "project"]).stdout.strip()
    bucket = os.environ.get("BQ_ORACLE_BUCKET", "rivet_data_test")
    if not have("gcloud") or not proj:
        led.skipped("-", "partner", "shape", "gcs",
                    "partner shape: no `gcloud` (the REST token) or no project (set BQ_ORACLE_PROJECT)", "no gcloud")
        return
    for engine in ("mysql", "postgres"):
        uvar = f"RIVET_CDC_{engine.upper()}_URL"
        url = os.environ.get(uvar, "")
        if not url:
            led.skipped(engine, "partner", "shape", "gcs", f"partner[{engine}]: no {uvar}", "no url")
            continue
        try:
            _one_engine(led, engine, url, proj, bucket)
        except Exception as e:  # noqa: BLE001 — graded, never swallowed
            led.failed(engine, "partner", "shape", "gcs",
                       f"partner shape[{engine}]: stage raised: {e!r}"[:400], "raised")


def _row(led: Ledger, engine: str, stage: str, ok: bool, detail: str) -> bool:
    msg = f"partner[{engine}] · {stage}"
    if ok:
        led.passed(engine, "partner", f"shape:{stage}", "gcs", msg, detail)
    else:
        led.failed(engine, "partner", f"shape:{stage}", "gcs", f"{msg} — {detail}", detail)
    return ok


def _one_engine(led: Ledger, engine: str, url: str, proj: str, bucket: str) -> None:
    work = work_dir() / f"partner_{engine}"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    slug = f"{work_dir().name}-{os.getpid()}"
    dset = registry.bq_tmp(f"partner_{engine}")
    pfx = f"partner/{engine}/{slug}"
    # init names the stream `cdc` (MySQL) / `public_cdc` (PostgreSQL) → slot `rivet_<name>`.
    slot = "rivet_public_cdc" if engine == "postgres" else None
    _cleanup(engine, url, slot)
    env = {"ORACLE_URL": url}
    try:
        if not _row(led, engine, "seed", _seed(engine, url), f"{len(TABLES)} tables × {SEED} rows"):
            return

        cfg = work / "rivet.yaml"
        p = rivet("init", "--source-env", "ORACLE_URL", "--mode", "cdc",
                  "--include", *TABLES, "--tls", "disable", "--gcs-bucket", bucket,
                  "-o", str(cfg), env=env, timeout=NO_TIMEOUT)
        body = cfg.read_text() if cfg.is_file() else ""
        bad = _shape_problems(body) if p.ok else [f"init exit={p.returncode}: {(p.stderr or '')[-200:]}"]
        if not _row(led, engine, "init", not bad,
                    f"init --mode cdc --include {' '.join(TABLES)} → {len(body.splitlines())} lines"
                    if not bad else "; ".join(bad)):
            return
        # A per-run prefix (init writes fixed ones) and the `load:` init never writes.
        body = body.replace("prefix: exports/", f"prefix: {pfx}/exports/").replace("prefix: cdc/", f"prefix: {pfx}/cdc/")
        body += f"\nload:\n  target: bigquery\n  project: {proj}\n  dataset: {dset}\n  pk: auto\n"
        cfg.write_text(body)
        gcp.bq_ensure_dataset(proj, dset)
        for t in TABLES:
            gcp.bq_delete_table(proj, dset, t)
            gcp.bq_delete_table(proj, dset, f"{t}__changes")
        gcp.gcs_delete_prefix(bucket, f"{pfx}/")

        for st in ("doctor", "check"):
            q = rivet(st, "-c", str(cfg), env=env, timeout=NO_TIMEOUT)
            if not _row(led, engine, st, q.ok, f"exit={q.returncode}" + ("" if q.ok else f" {(q.stderr or q.stdout or '')[-240:]}")):
                return

        # run 1: anchor + every table's baseline through its recipe; load 1.
        r1 = rivet("run", "-c", str(cfg), env=env, timeout=NO_TIMEOUT)
        if not _row(led, engine, "run1", r1.ok, f"exit={r1.returncode}" + ("" if r1.ok else f" {(r1.stderr or '')[-240:]}")):
            return
        l1 = rivet("load", "-c", str(cfg), "--run-id", f"partner-{engine}-{slug}-1", env=env, timeout=NO_TIMEOUT)
        if not _row(led, engine, "load1", l1.ok, f"exit={l1.returncode}" + ("" if l1.ok else f" {(l1.stderr or '')[-240:]}")):
            return
        # Count AND id-sum: identical counts across three tables cannot see a fan-out
        # that routed one table's rows under another's prefix; the sums can.
        got = {t: (_bq_count(proj, dset, t), _count(engine, url, t),
                   _bq_scalar(proj, dset, "IFNULL(SUM(id), 0)", t), _sum_id(engine, url, t)) for t in TABLES}
        ok = all(b == s == SEED and bs == ss for b, s, bs, ss in got.values())
        if not _row(led, engine, "baseline", ok,
                    "; ".join(f"{t}: bigquery={b} source={s} sum(id) bq={bs} src={ss}" for t, (b, s, bs, ss) in got.items())):
            return

        # delta in every table → run 2 → load 2: the buffer holds exactly the 5 changes,
        # the base still the baseline; → compact: live == source, buffer dropped.
        _delta(engine, url)
        r2 = rivet("run", "-c", str(cfg), env=env, timeout=NO_TIMEOUT)
        if not _row(led, engine, "run2", r2.ok, f"exit={r2.returncode}" + ("" if r2.ok else f" {(r2.stderr or '')[-240:]}")):
            return
        l2 = rivet("load", "-c", str(cfg), "--run-id", f"partner-{engine}-{slug}-2", env=env, timeout=NO_TIMEOUT)
        if not _row(led, engine, "load2", l2.ok, f"exit={l2.returncode}" + ("" if l2.ok else f" {(l2.stderr or '')[-240:]}")):
            return
        buffered = {t: (_bq_count(proj, dset, f"{t}__changes"), _bq_count(proj, dset, t)) for t in TABLES}
        if not _row(led, engine, "buffer", all(b == 5 and base == SEED for b, base in buffered.values()),
                    "; ".join(f"{t}: changes={b} base={base}" for t, (b, base) in buffered.items())):
            return
        c2 = rivet("compact", "-c", str(cfg), "--run-id", f"partner-{engine}-{slug}-c2", env=env, timeout=NO_TIMEOUT)
        if not _row(led, engine, "compact", c2.ok, f"exit={c2.returncode}" + ("" if c2.ok else f" {(c2.stderr or '')[-240:]}")):
            return
        got2 = {t: (_bq_count(proj, dset, t, "WHERE NOT __is_deleted"),
                    _bq_count(proj, dset, t, "WHERE __is_deleted"),
                    _count(engine, url, t),
                    _bq_scalar(proj, dset, "IFNULL(SUM(id), 0)", t, "WHERE NOT __is_deleted"),
                    _sum_id(engine, url, t),
                    _bq_table_exists(proj, dset, f"{t}__changes")) for t in TABLES}
        ok2 = all(live == src == DELTA_LIVE and gone == 1 and bs == ss and not buf
                  for live, gone, src, bs, ss, buf in got2.values())
        _row(led, engine, "delta", ok2,
             "; ".join(f"{t}: live={live} flagged={gone} source={src} sum(id) bq={bs} src={ss} buffer_left={buf}"
                       for t, (live, gone, src, bs, ss, buf) in got2.items()))
    finally:
        # The source teardown first: it drops a replication slot, and a cloud call
        # below may raise — a leaked slot pins WAL on the stand.
        _cleanup(engine, url, slot)
        try:
            gcp.bq_delete_dataset(proj, dset)
        finally:
            gcp.gcs_delete_prefix(bucket, f"{pfx}/")
