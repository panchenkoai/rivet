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
   per table: the changelog holds baseline + 5, the live view (`WHERE NOT
   __is_deleted`) == source == 7.

Oracles: the `bq` CLI (never rivet) and a re-query of the source. SKIP — never a
silent pass — without the engine URL, the `bq` CLI or a project. Cleans up the
warehouse tables, the GCS prefix, the source tables and (PostgreSQL) the slot.
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

from .cdc import _mysql, _psql
from .core import Ledger, have, rivet, run
from .scenarios import NO_TIMEOUT, work_dir

# Per-invocation names: two gates on one stand must not drop each other's tables.
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
    q = run(["bq", f"--project_id={proj}", "query", "--nouse_legacy_sql", "--format=csv",
             f"SELECT {expr} FROM `{proj}.{dset}.{table}` {where}"], timeout=None)
    return _scalar(q.stdout)


def _bq_count(proj: str, dset: str, table: str, where: str = "") -> int:
    return _bq_scalar(proj, dset, "COUNT(*)", table, where)


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
    if not have("bq") or not proj:
        led.skipped("-", "partner", "shape", "gcs",
                    "partner shape: no `bq` CLI or no project (set BQ_ORACLE_PROJECT)", "no bq")
        return
    for engine in ("mysql", "postgres"):
        uvar = f"RIVET_CDC_{engine.upper()}_URL"
        url = os.environ.get(uvar, "")
        if not url:
            led.skipped(engine, "partner", "shape", "gcs", f"partner[{engine}]: no {uvar}", "no url")
            continue
        _one_engine(led, engine, url, proj, bucket)


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
    dset = f"rivet_partner_{engine}"
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
        run(["bq", f"--project_id={proj}", "mk", "-f", "--dataset", f"{proj}:{dset}"])
        for t in TABLES:
            run(["bq", f"--project_id={proj}", "rm", "-f", "-t", f"{proj}:{dset}.{t}"])
            run(["bq", f"--project_id={proj}", "rm", "-f", "-t", f"{proj}:{dset}.{t}__changes"])
        run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])

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

        # delta in every table → run 2 → load 2: live == source, changelog == baseline + 5.
        _delta(engine, url)
        r2 = rivet("run", "-c", str(cfg), env=env, timeout=NO_TIMEOUT)
        if not _row(led, engine, "run2", r2.ok, f"exit={r2.returncode}" + ("" if r2.ok else f" {(r2.stderr or '')[-240:]}")):
            return
        l2 = rivet("load", "-c", str(cfg), "--run-id", f"partner-{engine}-{slug}-2", env=env, timeout=NO_TIMEOUT)
        if not _row(led, engine, "load2", l2.ok, f"exit={l2.returncode}" + ("" if l2.ok else f" {(l2.stderr or '')[-240:]}")):
            return
        got2 = {t: (_bq_count(proj, dset, t, "WHERE NOT __is_deleted"),
                    _bq_count(proj, dset, f"{t}__changes"),
                    _count(engine, url, t),
                    _bq_scalar(proj, dset, "IFNULL(SUM(id), 0)", t, "WHERE NOT __is_deleted"),
                    _sum_id(engine, url, t)) for t in TABLES}
        ok2 = all(live == src == DELTA_LIVE and log == SEED + 5 and bs == ss
                  for live, log, src, bs, ss in got2.values())
        _row(led, engine, "delta", ok2,
             "; ".join(f"{t}: live={live} changelog={log} source={src} sum(id) bq={bs} src={ss}"
                       for t, (live, log, src, bs, ss) in got2.items()))
    finally:
        for t in TABLES:
            run(["bq", f"--project_id={proj}", "rm", "-f", "-t", f"{proj}:{dset}.{t}"])
            run(["bq", f"--project_id={proj}", "rm", "-f", "-t", f"{proj}:{dset}.{t}__changes"])
        run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])
        _cleanup(engine, url, slot)
