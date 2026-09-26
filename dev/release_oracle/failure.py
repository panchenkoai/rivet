"""Runs that fail by RETURNING an error, and retries that really succeed.

Every other fault hook the gate uses is a panic, and a panicked run never reaches
the code that decides what a failed run leaves behind: whether the cursor
advances, what the manifest says, whether `validate` believes it, and whether a
retry may run over parts already durable. These two cells drive that code.

* `verify_failed_run_tail` — PostgreSQL single / chunked / checkpoint / parallel
  keyset / schema-drift, plus parallel Mongo, each failed by an error that
  RETURNS; then a clean re-run.
* `verify_transient_retry_exact` — a one-shot transient (two hook points) and a
  toxiproxy `reset_peer` before and after the first durable part.

Oracles never ask rivet: the exit code, files on disk, the manifest JSON, the
state DB through DuckDB ATTACH, and DuckDB over the parts SUCCESS manifests
declare, compared to the source.
"""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from .core import Ledger, have, rivet, rivet_bin, run
from .scenarios import _manifest_declared_parts, _tcp_open, work_dir

__all__ = ["verify_failed_run_tail", "verify_transient_retry_exact"]

PG_URL = os.environ.get("RIVET_CONC_SRC_URL", "postgresql://rivet:rivet@localhost:5432/rivet")
PG_TOXI_URL = "postgresql://rivet:rivet@127.0.0.1:15432/rivet"
MONGO_URL = os.environ.get("RIVET_FAILURE_MONGO_URL", "mongodb://127.0.0.1:27017")
TOXI_API = "http://127.0.0.1:8474"
TOXI_PROXY = "postgres"
RUN_TIMEOUT = 600.0
VER = "stand"


# ── small helpers ────────────────────────────────────────────────────────────
def Oracle(**kw):  # noqa: N802
    """The harness DuckDB session, imported lazily so `--self-test` runs on bare python3."""
    from .duck import Oracle as _Oracle

    return _Oracle(**kw)


def _token() -> str:
    """A name fragment unique to this cell invocation (the stand is shared)."""
    return f"{os.getpid()}_{uuid.uuid4().hex[:6]}"


def _psql(sql: str, url: str = PG_URL) -> bool:
    """Run SQL on the stand PostgreSQL; ON_ERROR_STOP so a failed seed is loud."""
    return run(["psql", url, "-q", "-v", "ON_ERROR_STOP=1", "-c", sql], timeout=300).ok


def _state_ref(cfg: Path) -> str:
    """The state DB rivet used for `cfg`: the gate-wide Postgres URL, else the SQLite file beside it."""
    return os.environ.get("RIVET_STATE_URL") or str(cfg.parent / ".rivet_state.db")


def _state(cfg: Path, export: str) -> dict:
    """The newest terminal metrics row and the persisted cursor, read through DuckDB ATTACH."""
    with Oracle(state=_state_ref(cfg)) as o:
        m = o.rows(
            "SELECT files_committed, retries, status, run_id FROM st.export_metrics "
            f"WHERE export_name = '{export}' AND status <> 'running' ORDER BY id DESC LIMIT 1")
        cur = o.rows(f"SELECT last_cursor_value FROM st.export_state WHERE export_name = '{export}'")
    row = m[0] if m else (None, None, None, None)
    return {"files_committed": row[0], "retries": row[1], "status": row[2], "run_id": row[3],
            "cursor": cur[0][0] if cur else None}


def _parquet(out: Path) -> set[str]:
    """Every parquet file under the destination (durable or not, declared or not)."""
    return {str(p) for p in out.rglob("*.parquet")}


def _manifests(out: Path) -> list[dict]:
    """Every run-unique manifest copy under `out`, oldest first."""
    docs = []
    for p in sorted(out.rglob("manifest-*.json"), key=lambda q: q.stat().st_mtime):
        try:
            docs.append(json.loads(p.read_text()))
        except (OSError, json.JSONDecodeError):
            continue
    return docs


def _success_cursor(out: Path) -> str | None:
    """The cursor_high the newest SUCCESS manifest declares — the furthest a cursor may be."""
    best = None
    for d in _manifests(out):
        if str(d.get("status")).lower() == "success":
            ex = (d.get("source") or {}).get("extraction") or {}
            best = ex.get("cursor_high", best)
    return None if best is None else str(best)


def _declared_counts(out: Path, idc: str) -> tuple[int, int]:
    """(rows, distinct ids) over only the parts a SUCCESS manifest declares, read by DuckDB."""
    parts = _manifest_declared_parts(out)
    if not parts:
        return (0, 0)
    lst = "[" + ",".join(f"'{p}'" for p in parts) + "]"
    with Oracle() as o:
        r = o.rows(f"SELECT count(*), count(DISTINCT {idc}) FROM read_parquet({lst})")[0]
    return (int(r[0]), int(r[1]))


def _pg_source_counts(table: str) -> tuple[int, int]:
    """(rows, distinct id) of the source table, read by DuckDB's postgres scanner."""
    with Oracle(postgres=PG_URL) as o:
        r = o.rows(f"SELECT count(*), count(DISTINCT id) FROM pg.public.{table}")[0]
    return (int(r[0]), int(r[1]))


def _write_cfg(work: Path, name: str, source: str, subject: str, lines: list[str]) -> tuple[Path, Path]:
    """Write one-export config; returns (config path, local destination dir)."""
    out = work / "out"
    out.mkdir(parents=True, exist_ok=True)
    body = "".join(f"    {ln}\n" for ln in lines)
    cfg = work / "rivet.yaml"
    cfg.write_text(
        f"{source}\nexports:\n  - name: {name}\n    {subject}\n    format: parquet\n{body}"
        f"    destination: {{ type: local, path: {out} }}\n")
    return cfg, out


def _rivet_run(cfg: Path, env: dict | None = None, *extra: str):
    """`rivet run -c cfg` with the given fault env."""
    return rivet("run", "-c", str(cfg), *extra, env=env or {}, timeout=RUN_TIMEOUT)


def _tail(text: str, n: int = 400) -> str:
    """The last `n` characters of a transcript, on one line."""
    return " ".join(text[-n:].split())


# ── the failed-run grader (shared by every C1 case) ───────────────────────────
@dataclass
class Failed:
    """What a failed run must leave behind, and the problems found."""

    problems: list[str] = field(default_factory=list)

    def need(self, ok: bool, what: str) -> None:
        """Record `what` as a problem unless `ok`."""
        if not ok:
            self.problems.append(what)


def _grade_failed(p, cfg: Path, out: Path, export: str, before: set[str], cause: str,
                  env: dict | None = None) -> Failed:
    """Grade one failed run: exit, _SUCCESS, manifest, validate, cursor, files_committed."""
    g = Failed()
    g.need(p.returncode != 0, f"exit {p.returncode}, a failed run must exit non-zero")
    g.need(cause in p.out, f"the failure is not our injected cause `{cause}`: {_tail(p.out)}")
    st = _state(cfg, export)
    run_id = st["run_id"]
    g.need(st["status"] == "failed", f"export_metrics status={st['status']!r}, expected 'failed'")
    mpath = out / f"manifest-{run_id}.json"
    if not mpath.is_file():
        mpath = out / "manifest.json"
    try:
        mstatus = json.loads(mpath.read_text()).get("status")
    except (OSError, json.JSONDecodeError):
        mstatus = None
    g.need(mstatus == "failed", f"{mpath.name} status={mstatus!r}, expected 'failed'")
    v = rivet("validate", "-c", str(cfg), env=env or {}, timeout=RUN_TIMEOUT)
    g.need(v.returncode != 0, f"`rivet validate` exit 0 on a failed manifest: {_tail(v.out)}")
    g.need("RIVET_VERIFY_RUN_NOT_SUCCESSFUL" in v.out,
           f"`rivet validate` does not name RIVET_VERIFY_RUN_NOT_SUCCESSFUL: {_tail(v.out)}")
    if (out / "_SUCCESS").exists():
        stale = "RIVET_VERIFY_SUCCESS_STALE" in v.out
        g.problems.append("_SUCCESS present after a failed run (a prior success's marker left beside a "
                          f"failed canonical manifest; validate {'does' if stale else 'does NOT'} "
                          "flag it RIVET_VERIFY_SUCCESS_STALE)")
    allowed = _success_cursor(out)
    got = None if st["cursor"] is None else str(st["cursor"])
    g.need(got == allowed,
           f"state cursor={got!r} but the newest success manifest declares {allowed!r} — a failed "
           "run moved the anchor")
    new_files = len(_parquet(out) - before)
    g.need(st["files_committed"] == new_files,
           f"files_committed={st['files_committed']} but the failed run left {new_files} parquet on disk")
    return g


# ── C1 cases ──────────────────────────────────────────────────────────────────
@dataclass
class PgCase:
    """One PostgreSQL failed-run case."""

    key: str
    lines: list[str]
    err_at: str
    rows: int = 2000
    rerun: tuple[str, ...] = ()


PG_CASES = [
    PgCase("a_single_incr", ["mode: incremental", "cursor_column: id", "compression: none",
                             "max_file_size: 64KB",
                             "parquet: { row_group_strategy: fixed_rows, row_group_rows: 100 }",
                             "tuning: { batch_size: 100 }"], "single_part_commit:1"),
    PgCase("b_chunked_par", ["mode: chunked", "chunk_column: id", "chunk_size: 250", "parallel: 4"],
           "chunk_export:1"),
    PgCase("c_ckpt_seq", ["mode: chunked", "chunk_column: id", "chunk_size: 250",
                          "chunk_checkpoint: true"], "chunk_export:1", rerun=("--resume",)),
    PgCase("c_ckpt_par", ["mode: chunked", "chunk_column: id", "chunk_size: 250",
                          "chunk_checkpoint: true", "parallel: 4"], "chunk_export:1",
           rerun=("--resume",)),
    PgCase("d_keyset_incr", ["mode: chunked", "chunk_by_key: id", "chunk_size: 100", "parallel: 4",
                             "keyset_incremental: true"], "keyset_parallel_worker_midrange:1"),
]


def _seed_pg(table: str, lo: int, hi: int) -> bool:
    """Insert ids lo..hi with a 1 KiB payload (so size-rolled parts really roll)."""
    return _psql(f"INSERT INTO {table} SELECT g, repeat(md5(g::text), 32), g FROM "
                 f"generate_series({lo}, {hi}) g")


def _create_pg(table: str) -> bool:
    """A fresh (id, payload, extra) table; `extra` is last so the drift case can drop and restore it."""
    return _psql(f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} "
                 "(id BIGINT PRIMARY KEY, payload TEXT NOT NULL, extra BIGINT)")


def _finish(led: Ledger, engine: str, scenario: str, key: str, problems: list[str], ok_msg: str,
            t0: float) -> None:
    """Record one sub-case row with its wall time."""
    dt = time.perf_counter() - t0
    led.record_span(f"{scenario} {engine} {key}", dt)
    if problems:
        led.failed(engine, VER, scenario, key, f"{scenario}[{key}] ({dt:.1f}s): " + "; ".join(problems))
    else:
        led.passed(engine, VER, scenario, key, f"{scenario}[{key}] ({dt:.1f}s): {ok_msg}")


def _clean_rerun(cfg: Path, out: Path, source: tuple[int, int], idc: str, env: dict | None = None,
                 *extra: str) -> list[str]:
    """Re-run without faults (`--resume` where the product prescribes it); declared parts must equal the source."""
    p = _rivet_run(cfg, env, *extra)
    if not p.ok:
        return [f"the clean re-run failed (exit {p.returncode}): {_tail(p.out)}"]
    got = _declared_counts(out, idc)
    if got == source:
        return []
    return [f"after the clean re-run the declared parts hold {got[0]} rows / {got[1]} distinct, "
            f"the source holds {source[0]} / {source[1]}"]


def _pg_case(led: Ledger, case: PgCase, tok: str) -> None:
    """Fail one PostgreSQL export by a RETURNED error, grade the tail, then re-run clean."""
    t0 = time.perf_counter()
    table = f"frt_{case.key}_{tok}"
    work = work_dir() / f"failed_run_tail_{case.key}_{tok}"
    work.mkdir(parents=True, exist_ok=True)
    try:
        if not (_create_pg(table) and _seed_pg(table, 1, case.rows)):
            led.skipped("postgres", VER, "failed_run_tail", case.key,
                        f"failed_run_tail[{case.key}]: could not seed {table}", "no seed")
            return
        cfg, out = _write_cfg(work, table, "source: { type: postgres, url_env: FRT_URL }",
                              f"table: public.{table}", case.lines)
        env = {"FRT_URL": PG_URL}
        before = _parquet(out)
        p = rivet("run", "-c", str(cfg), env={**env, "RIVET_TEST_ERROR_AT": case.err_at},
                  timeout=RUN_TIMEOUT)
        g = _grade_failed(p, cfg, out, table, before, "RIVET_TEST_ERROR_AT", env)
        g.need(len(_parquet(out) - before) > 0 or case.key.startswith("b_"),
               "fixture inert: the failed run left no durable part, so files_committed proves nothing")
        g.problems += _clean_rerun(cfg, out, _pg_source_counts(table), "id", env, *case.rerun)
        _finish(led, "postgres", "failed_run_tail", case.key, g.problems,
                f"{case.err_at}: exit≠0, no _SUCCESS, manifest failed, validate names "
                "RUN_NOT_SUCCESSFUL, cursor held, files_committed == disk; clean re-run == source",
                t0)
    finally:
        _psql(f"DROP TABLE IF EXISTS {table}")


def _pg_drift_case(led: Ledger, tok: str) -> None:
    """(e) an incremental run failed by `on_schema_drift: fail` after a column is dropped."""
    key = "e_drift_incr"
    t0 = time.perf_counter()
    table = f"frt_{key}_{tok}"
    work = work_dir() / f"failed_run_tail_{key}_{tok}"
    work.mkdir(parents=True, exist_ok=True)
    try:
        if not (_create_pg(table) and _seed_pg(table, 1, 1000)):
            led.skipped("postgres", VER, "failed_run_tail", key,
                        f"failed_run_tail[{key}]: could not seed {table}", "no seed")
            return
        cfg, out = _write_cfg(work, table, "source: { type: postgres, url_env: FRT_URL }",
                              f"table: public.{table}",
                              ["mode: incremental", "cursor_column: id", "on_schema_drift: fail"])
        env = {"FRT_URL": PG_URL}
        first = _rivet_run(cfg, env)
        if not first.ok:
            _finish(led, "postgres", "failed_run_tail", key,
                    [f"the baseline run failed: {_tail(first.out)}"], "", t0)
            return
        ok = _seed_pg(table, 1001, 1500) and _psql(f"ALTER TABLE {table} DROP COLUMN extra")
        before = _parquet(out)
        p = _rivet_run(cfg, env)
        g = _grade_failed(p, cfg, out, table, before, "drift", env)
        g.need(ok, "the drift DDL itself failed, so nothing was tested")
        g.need(_success_cursor(out) == "1000", f"the baseline success manifest declares cursor "
               f"{_success_cursor(out)!r}, expected '1000' — the fixture is not what it claims")
        restored = _psql(f"ALTER TABLE {table} ADD COLUMN extra BIGINT")
        g.need(restored, "could not restore the dropped column for the clean re-run")
        g.problems += _clean_rerun(cfg, out, _pg_source_counts(table), "id", env)
        _finish(led, "postgres", "failed_run_tail", key, g.problems,
                "on_schema_drift: fail — exit≠0, manifest failed, validate names RUN_NOT_SUCCESSFUL, "
                "cursor held at the success manifest's 1000; clean re-run == source", t0)
    finally:
        _psql(f"DROP TABLE IF EXISTS {table}")


def _mongo_case(led: Ledger, tok: str) -> None:
    """(f) parallel Mongo keyset failed by `mongo_parallel_worker:1`."""
    key = "f_mongo_par"
    t0 = time.perf_counter()
    try:
        import pymongo
    except ImportError:
        led.skipped("mongo", VER, "failed_run_tail", key,
                    f"failed_run_tail[{key}]: pymongo not importable (run through uv)", "no pymongo")
        return
    db, coll = f"frt_{tok}", "bench"
    client = pymongo.MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
    work = work_dir() / f"failed_run_tail_{key}_{tok}"
    work.mkdir(parents=True, exist_ok=True)
    try:
        client[db][coll].insert_many([{"_id": i, "v": f"row{i}"} for i in range(1, 4001)])
        cfg, out = _write_cfg(
            work, f"frt_mongo_{tok}",
            f"source:\n  type: mongo\n  url: \"{MONGO_URL}/{db}\"\n  mongo: {{ page_size: 500 }}",
            f"table: {coll}", ["mode: full", "parallel: 4"])
        before = _parquet(out)
        p = _rivet_run(cfg, {"RIVET_TEST_ERROR_AT": "mongo_parallel_worker:1"})
        g = _grade_failed(p, cfg, out, f"frt_mongo_{tok}", before, "RIVET_TEST_ERROR_AT")
        g.need(len(_parquet(out) - before) > 0,
               "fixture inert: the surviving workers wrote no part")
        src = (client[db][coll].count_documents({}), len(client[db][coll].distinct("_id")))
        g.problems += _clean_rerun(cfg, out, src, "_id")
        _finish(led, "mongo", "failed_run_tail", key, g.problems,
                "mongo_parallel_worker:1 — exit≠0, manifest failed, validate names "
                "RUN_NOT_SUCCESSFUL, files_committed == disk; clean re-run == source", t0)
    finally:
        client.drop_database(db)
        client.close()


def verify_failed_run_tail(led: Ledger) -> None:
    """C1: what a RETURNED failure leaves behind, on every runner shape, then a clean re-run."""
    led.phase("Failed-run tail — errors that RETURN (single, chunked, checkpoint, keyset, drift, mongo)")
    if not have("psql") or not _tcp_open("127.0.0.1", 5432):
        led.skipped("postgres", VER, "failed_run_tail", "-",
                    "failed_run_tail: the stand PostgreSQL (:5432) or psql is unavailable", "no pg")
    else:
        tok = _token()
        for case in PG_CASES:
            _pg_case(led, case, tok)
        _pg_drift_case(led, tok)
    if not _tcp_open("127.0.0.1", 27017):
        led.skipped("mongo", VER, "failed_run_tail", "f_mongo_par",
                    "failed_run_tail[f_mongo_par]: the stand MongoDB (:27017) is down", "no mongo")
    else:
        _mongo_case(led, _token())


# ── C7: transient retries ─────────────────────────────────────────────────────
def _toxi(method: str, path: str, body: dict | None = None) -> int:
    """One toxiproxy admin call; returns the HTTP status (0 when unreachable)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(TOXI_API + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except (urllib.error.URLError, OSError):
        return 0


@contextmanager
def _toxi_lock():
    """The same flock the Rust live tests take (tests/common/toxi.rs) around toxic changes."""
    path = Path(os.environ.get("TMPDIR") or tempfile.gettempdir()) / "rivet_qa_toxiproxy.lock"
    with open(path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _chunked_pg_cfg(work: Path, table: str, url_env: str, extra: list[str]) -> tuple[Path, Path]:
    """A plain chunked-sequential export of `table` (4 chunks of 250)."""
    return _write_cfg(work, table, f"source: {{ type: postgres, url_env: {url_env} }}",
                      f"table: public.{table}",
                      ["mode: chunked", "chunk_column: id", "chunk_size: 250", *extra])


def _exact_once(out: Path, table: str) -> list[str]:
    """Declared parts == source in count and distinct, and no undeclared part left beside them."""
    src = _pg_source_counts(table)
    got = _declared_counts(out, "id")
    if got == src:
        return []
    return [f"declared parts hold {got[0]} rows / {got[1]} distinct, source {src[0]} / {src[1]}"]


def _transient_hook_case(led: Ledger, point: str, tok: str) -> None:
    """(a) RIVET_TEST_TRANSIENT_ONCE=<point>: the run retries, succeeds, and counts every row once."""
    key = f"a_{point}"
    t0 = time.perf_counter()
    table = f"trx_{point}_{tok}"
    work = work_dir() / f"transient_retry_{point}_{tok}"
    work.mkdir(parents=True, exist_ok=True)
    probs: list[str] = []
    try:
        if not (_create_pg(table) and _seed_pg(table, 1, 1000)):
            led.skipped("postgres", VER, "transient_retry_exact", key,
                        f"transient_retry_exact[{key}]: could not seed", "no seed")
            return
        ckpt = ["chunk_checkpoint: true"] if point == "after_resume_adopt" else []
        cfg, out = _chunked_pg_cfg(work, table, "TRX_URL", ckpt)
        env = {"TRX_URL": PG_URL}
        extra: tuple[str, ...] = ()
        if point == "after_resume_adopt":
            crash = _rivet_run(cfg, {**env, "RIVET_TEST_PANIC_AT": "after_chunk_complete:0"})
            probs += [] if not crash.ok else ["the setup crash did not stop the first run"]
            extra = ("--resume",)
        p = _rivet_run(cfg, {**env, "RIVET_TEST_TRANSIENT_ONCE": point}, *extra)
        st = _state(cfg, table)
        probs += [] if p.ok else [f"exit {p.returncode}: the retry must finish the run: {_tail(p.out)}"]
        probs += [] if "RIVET_TEST_TRANSIENT_ONCE" in p.out else ["the transient never fired (inert)"]
        probs += [] if (st["retries"] or 0) >= 1 else [f"export_metrics.retries={st['retries']}, expected >= 1"]
        probs += _exact_once(out, table)
        _finish(led, "postgres", "transient_retry_exact", key, probs,
                f"retried {st['retries']}x, succeeded, declared parts == source (no double count)", t0)
    finally:
        _psql(f"DROP TABLE IF EXISTS {table}")


def _reset_peer_case(led: Ledger, after_part: bool, tok: str) -> None:
    """(b) toxiproxy reset_peer mid-stream: before a durable part it retries; after one the duplicate guard stops it."""
    key = "b_reset_after" if after_part else "b_reset_before"
    t0 = time.perf_counter()
    table = f"trx_{'after' if after_part else 'before'}_{tok}"
    work = work_dir() / f"transient_retry_{key}_{tok}"
    work.mkdir(parents=True, exist_ok=True)
    marker = work / "paused"
    probs: list[str] = []
    toxic = f"gate_reset_{tok}_{int(after_part)}"
    try:
        if not (_create_pg(table) and _seed_pg(table, 1, 1000)):
            led.skipped("postgres", VER, "transient_retry_exact", key,
                        f"transient_retry_exact[{key}]: could not seed", "no seed")
            return
        cfg, out = _chunked_pg_cfg(work, table, "TRX_URL",
                                   ["tuning: { max_retries: 2, retry_backoff_ms: 3000 }"])
        env = {**os.environ, "TRX_URL": PG_TOXI_URL,
               "RIVET_TEST_PAUSE_AT": "pg_after_snapshot_open:1500",
               "RIVET_TEST_PAUSE_MARKER": str(marker)}
        with _toxi_lock():
            _toxi("POST", "/proxies", {"name": TOXI_PROXY, "listen": "0.0.0.0:15432",
                                       "upstream": "postgres:5432", "enabled": True})
            proc = subprocess.Popen([str(rivet_bin()), "run", "-c", str(cfg)], env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            injected, part_seen_at_inject = False, 0
            armed = not after_part
            deadline = time.monotonic() + 120
            while proc.poll() is None and time.monotonic() < deadline:
                if not armed and _parquet(out):
                    marker.unlink(missing_ok=True)
                    armed = True
                elif armed and marker.exists():
                    part_seen_at_inject = len(_parquet(out))
                    code = _toxi("POST", f"/proxies/{TOXI_PROXY}/toxics",
                                 {"name": toxic, "type": "reset_peer", "stream": "downstream",
                                  "toxicity": 1.0, "attributes": {"timeout": 0}})
                    injected = code == 200
                    if not injected:
                        probs.append(f"toxiproxy refused the reset_peer toxic (HTTP {code})")
                    else:
                        time.sleep(2.4)
                        _toxi("DELETE", f"/proxies/{TOXI_PROXY}/toxics/{toxic}")
                    break
                time.sleep(0.05)
            try:
                transcript, _ = proc.communicate(timeout=RUN_TIMEOUT)
            finally:
                _toxi("DELETE", f"/proxies/{TOXI_PROXY}/toxics/{toxic}")
        rc = proc.returncode
        if not injected:
            probs.append("the reset was never injected (no pause window matched) — nothing was tested")
        st = _state(cfg, table)
        if after_part:
            probs += [] if part_seen_at_inject >= 1 else ["no part was durable when the reset hit"]
            probs += [] if rc != 0 else ["exit 0 — a retry ran over a durable part (duplicate guard off?)"]
            probs += [] if "cannot safely retry" in transcript else [
                f"the stop was not the duplicate guard: {_tail(transcript)}"]
            probs += [] if not (out / "_SUCCESS").exists() else ["_SUCCESS on a guarded stop"]
            disk = len(_parquet(out))
            probs += [] if st["files_committed"] == disk else [
                f"files_committed={st['files_committed']} but {disk} parquet on disk"]
            ok = f"reset after {part_seen_at_inject} durable part(s): duplicate guard stop, exit {rc}, files_committed == disk"
        else:
            probs += [] if rc == 0 else [f"exit {rc}: a reset before any durable part must retry and succeed: {_tail(transcript)}"]
            probs += [] if (st["retries"] or 0) >= 1 else [f"export_metrics.retries={st['retries']}, expected >= 1"]
            probs += _exact_once(out, table)
            disk = len(_parquet(out))
            declared = len(_manifest_declared_parts(out))
            probs += [] if disk == declared else [f"{disk} parquet on disk, {declared} declared — an attempt left a part behind"]
            ok = f"reset before any part: retried {st['retries']}x, succeeded, exactly once per row"
        _finish(led, "postgres", "transient_retry_exact", key, probs, ok, t0)
    finally:
        _psql(f"DROP TABLE IF EXISTS {table}")


def verify_transient_retry_exact(led: Ledger) -> None:
    """C7: a transient fault retries to an exact result; after a durable part it stops instead."""
    led.phase("Transient retry — one-shot transients and a toxiproxy reset_peer, exactly once")
    if not have("psql") or not _tcp_open("127.0.0.1", 5432):
        led.skipped("postgres", VER, "transient_retry_exact", "-",
                    "transient_retry_exact: the stand PostgreSQL (:5432) or psql is unavailable", "no pg")
        return
    tok = _token()
    for point in ("chunk_write", "after_resume_adopt"):
        _transient_hook_case(led, point, tok)
    if not (_tcp_open("127.0.0.1", 8474) and _tcp_open("127.0.0.1", 15432)):
        for key in ("b_reset_before", "b_reset_after"):
            led.skipped("postgres", VER, "transient_retry_exact", key,
                        f"transient_retry_exact[{key}]: toxiproxy (:8474/:15432) is down", "no toxiproxy")
        return
    _reset_peer_case(led, False, tok)
    _reset_peer_case(led, True, tok)
