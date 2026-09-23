"""Release-oracle BigQuery stage — the real-cloud final oracle.

For every SQL engine (postgres / mysql / mssql — Mongo has no type matrix) the round
trip goes through rivet on BOTH legs: `rivet run` stages that engine's
`rivet_type_matrix` to a REAL GCS bucket and `rivet load` loads it into BigQuery.
The expected value is the SOURCE table, never a golden blessed from rivet's output:
one DuckDB session attaches the source and BigQuery and compares every column value
(`value_diff.compare_to_bigquery`). The golden this replaced froze rivet's own
UUID-as-unreadable-STRING as "expected" from 0.22 to 0.27.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Callable

from . import gcp
from .core import (
    ROOT,
    Ledger,
    docker,
    docker_exec,
    engine_container,
    have,
    rivet,
    run,
)

# The golden and the two dependency-free helper scripts stay in the BASH tree and
# are shared, not copied: one blessed artifact and one matrix parser, so the two
# implementations cannot come to mean different things while the port is in
# flight. (A second copy of a golden is a golden that will drift.)
_BASH_HERE = ROOT / "dev" / "release-oracle"
_LIB = _BASH_HERE / "lib"
_MATRIX_YAML = _BASH_HERE / "matrix.yaml"

# Ports this stage owns, distinct from the main engine loop's so a BQ run can
# share a machine with one.
_PORTS = {"postgres": 55099, "mysql": 55098, "mssql": 55097}

_TAG = "bq"


def _matrix_cfg(*args: str) -> str:
    """Query matrix.yaml through the existing `lib/cfg.py` reader.

    A subprocess rather than a re-implementation, on purpose: it is the one
    parser both implementations share, and it deliberately avoids PyYAML so a
    broken pip can never block a release gate.
    """
    p = run(["python3", str(_LIB / "cfg.py"), str(_MATRIX_YAML), *args])
    return p.stdout.strip()


def _grade_chain(led: Ledger, engine: str, url: str, table: str, bucket: str, pfx: str,
                 config: Path, dset: str) -> None:
    """Source, manifest, GCS parquet footers, rivet's ledger and BigQuery must agree for the run just loaded — one DuckDB session."""
    from .value_diff import chain_census, chain_disagreements

    state = os.environ.get("RIVET_STATE_URL", "")
    if not state.startswith("postgres"):
        state = str(config.with_name(".rivet_state.db"))  # rivet keeps its state beside the config
    try:
        c = chain_census(engine, url, table, bucket, pfx, state, dset, table)
    except Exception as e:  # noqa: BLE001 — an oracle that cannot read is a FAIL, never a pass
        led.failed("bigquery", engine, "chain", "-", f"chain[{engine}]: oracle failed: {e}", "oracle-error")
        return
    bad = chain_disagreements(c)
    if bad:
        led.failed("bigquery", engine, "chain", "-", f"chain[{engine}]: " + "; ".join(bad), "disagree")
    else:
        led.passed("bigquery", engine, "chain", "-",
                   f"chain[{engine}]: source = manifest = GCS footers = metrics = file_log = "
                   f"load_run = BigQuery = {c['source']} rows, run {c['run_ids'][0]}, no undeclared parts")


def _grade_against_source(led: Ledger, engine: str, url: str, dset: str, table: str) -> None:
    """Every column of the loaded table against the SOURCE row, both read by DuckDB; a golden rivet wrote would grade change, not correctness."""
    from .duck import bq_target
    from .value_diff import compare_to_bigquery

    if bq_target() is None:
        led.skipped("bigquery", engine, "golden", "-",
                    "BigQuery: the DuckDB oracle needs BQ_ORACLE_PROJECT and BQ_ORACLE_DATASET",
                    "no creds")
        return
    try:
        rows, diffs = compare_to_bigquery(engine, url, dset, table)
    except Exception as e:  # noqa: BLE001 — an oracle that cannot read is a FAIL, never a pass
        led.failed("bigquery", engine, "golden", "-",
                   f"BigQuery[{engine}]: the DuckDB oracle could not compare — {str(e).splitlines()[0][:160]}",
                   "oracle")
        return
    if diffs:
        led.failed("bigquery", engine, "golden", "-",
                   f"BigQuery[{engine}] differs from the SOURCE: " + " | ".join(diffs)[:600], "diverged")
    elif rows == 0:
        led.failed("bigquery", engine, "golden", "-",
                   f"BigQuery[{engine}]: the warehouse table is empty — the oracle compared nothing",
                   "empty-readback")
    else:
        led.passed("bigquery", engine, "golden", "-",
                   f"BigQuery[{engine}]: {rows} row(s), every column equal to the source (DuckDB on both sides)")

def _work_dir() -> Path:
    """The driver exports `WORK` for the whole run; make our own if it did not."""
    env = os.environ.get("WORK")
    return Path(env) if env else Path(tempfile.mkdtemp(prefix="rivet-oracle-bq-"))


# ── the stage ──────────────────────────────────────────────────────────────────
# ── load_pool ────────────────────────────────────────────────────────────────
# Sixteen, because that is BOTH `MAX_POOL` and today's `DEFAULT_POOL`: the cell
# is meant to exercise the pool at its full declared width, not at a width that
# happens to be comfortable. `effective_pool` clamps the pool to the work
# available, so sixteen WORKERS need sixteen TABLES or the flag is decorative.
_POOL_TABLES = 16
# Small on purpose. This cell grades CONCURRENCY and COMPLETENESS, not
# throughput: the row count only has to be large enough that each load job is a
# real job, and every row of it is billed.
_POOL_ROWS = 200
_POOL_PREFIX = "pool_t"


def _bq_json(proj: str, sql: str) -> list[dict]:
    """One BigQuery query, as parsed JSON rows ([] when the CLI is unhappy)."""
    p = run(["bq", f"--project_id={proj}", "query", "--nouse_legacy_sql",
             "--format=json", sql], timeout=600)
    if not p.ok:
        return []
    try:
        return json.loads(p.stdout or "[]")
    except json.JSONDecodeError:
        return []


def _max_overlap(spans: list[tuple[int, int]]) -> int:
    """The most jobs that were in flight at once, by a sweep over the endpoints.

    A START adds one, an END removes one; ties resolve END-first so two jobs that
    merely touch (one ends exactly as the next begins) are NOT counted as
    overlapping. That tie-break is the whole point — a sequential loader produces
    exactly that shape, and counting it would make the guard vacuous.
    """
    events: list[tuple[int, int]] = []
    for s, e in spans:
        events.append((s, +1))
        events.append((e, -1))
    events.sort(key=lambda ev: (ev[0], ev[1]))
    cur = best = 0
    for _, delta in events:
        cur += delta
        best = max(best, cur)
    return best


def verify_load_pool(led: Ledger, *, proj: str, dset: str, bucket: str, work: Path,
                     child: dict[str, str], engine: str, url: str) -> None:
    """`rivet load --pool N` loads N tables AT ONCE, loses none, and really overlaps.

    The branch's headline feature had NO gate cell. `pool_e2e` and `pool_split`
    both grade `apply --pool` — the EXPORT scheduler, a different subsystem — and
    of the gate's seven `rivet load` sites six load a single table, where
    `effective_pool` clamps the pool to one worker. `partner_shape` loads three
    and is therefore already concurrent, but it would pass identically if the
    pool silently degraded to sequential: nothing there can tell the difference.

    So this cell carries TWO oracles, and the second is the one that matters:

    * **completeness** — per table, BigQuery's own `COUNT(*)` AND `SUM(id)`
      against a re-query of the SOURCE. Counts alone cannot see a fan-out that
      routed one table's rows under another's name, because sixteen tables seeded
      alike have identical counts; the id-sums differ, so they can. (The same
      argument `partner_shape` already makes for three tables.)
    * **non-vacuity** — BigQuery's own job history. At least two `LOAD_DATA` jobs
      for these tables must have OVERLAPPED in time. A pool that degraded to
      sequential still loads every row and still passes the first oracle; it
      cannot produce overlapping jobs. This is the sibling of `keyset_parallel`'s
      ">=2 distinct worker parts" guard, and it is BigQuery's data rather than
      rivet's summary.

    The config is GENERATED — `rivet init --include 'pool_t*' --gcs-bucket
    --bigquery-project --bigquery-dataset` emits the exports AND the `load:`
    block, so not one line of it is written here. That is the rule, and it also
    buys the cell something: it grades what init DECIDES over sixteen tables
    (per-table prefixes, the partition guess, the load target), which a
    hand-written config would hide. If init stops emitting a loadable config, the
    fixture check below fails before the pool is ever measured.

    Postgres only. The load reads Parquet out of GCS and talks to BigQuery — the
    source engine cannot change how the pool behaves, so the other three engines
    are `{na}` in the ledger rather than three more billed copies of one answer.
    """
    if not (have("bq") and have("gcloud")):
        led.skipped(engine, "-", "load_pool", "-",
                    "load_pool: needs the `bq` and `gcloud` CLIs", "no cli")
        return

    pool_dset = f"{dset}_{engine}_pool"
    tables = [f"{_POOL_PREFIX}{i:02d}" for i in range(_POOL_TABLES)]
    name = engine_container(engine, _TAG)

    # Sixteen tables, seeded alike so the COUNTS are identical and only the
    # id-sums can tell them apart — see the completeness oracle above.
    ddl = "\n".join(
        f"DROP TABLE IF EXISTS {t}; "
        f"CREATE TABLE {t} (id BIGINT PRIMARY KEY, v TEXT NOT NULL); "
        f"INSERT INTO {t} (id, v) "
        f"SELECT g + {i * 1000}, md5(g::text) FROM generate_series(1, {_POOL_ROWS}) g;"
        for i, t in enumerate(tables)
    )
    seeded = docker_exec(name, "psql", "-U", "rivet", "-d", "rivet", "-q",
                         "-v", "ON_ERROR_STOP=1", stdin=ddl, timeout=900)
    if not seeded.ok:
        led.skipped(engine, "-", "load_pool", "-",
                    f"load_pool: seeding {_POOL_TABLES} tables failed — "
                    f"{(seeded.out or '').strip()[-200:]}", "seed")
        return

    # A leftover prefix from an earlier gate run would be loaded alongside this
    # one's and the read-back would compare a union. init fixes the prefix at
    # `exports/<table>/`, so the wipe is by table name rather than by a token.
    gcp.gcs_delete_prefixes(bucket, [f"exports/{t}/" for t in tables])
    gcp.bq_delete_dataset(proj, pool_dset)
    gcp.bq_ensure_dataset(proj, pool_dset)

    cfg = work / f"load_pool_{engine}.yaml"
    gen = rivet("init", "--source-env", "ORACLE_URL",
                "--include", f"{_POOL_PREFIX}*",
                "--gcs-bucket", bucket,
                "--bigquery-project", proj,
                "--bigquery-dataset", pool_dset,
                "-o", str(cfg), env=child, timeout=600)
    if not gen.ok or not cfg.exists():
        led.failed(engine, "-", "load_pool", "-",
                   f"load_pool: `rivet init` did not produce a config — "
                   f"{(gen.stderr or gen.stdout or '').strip()[-200:]}", "init")
        return
    # ACTIVATION CHECK, before anything is measured: sixteen workers need sixteen
    # exports. If init emitted fewer, `effective_pool` clamps the pool to what it
    # emitted and the cell would grade a narrower pool while reporting the wide
    # one — the fixture answering for the product.
    body = cfg.read_text()
    emitted = sum(1 for t in tables if f"table: {t}" in body)
    if emitted != _POOL_TABLES or "target: bigquery" not in body:
        led.failed(engine, "-", "load_pool", "-",
                   f"load_pool: the generated config carries {emitted} of {_POOL_TABLES} "
                   f"pool exports and load target "
                   f"{'present' if 'target: bigquery' in body else 'ABSENT'} — the pool "
                   f"cannot be exercised at width {_POOL_TABLES}", "fixture")
        return

    r = rivet("run", "-c", str(cfg), env=child, timeout=None)
    if not r.ok:
        led.failed(engine, "-", "load_pool", "-",
                   f"load_pool: the extract failed — "
                   f"{(r.stderr or r.stdout or '').strip()[-200:]}", "run")
        return

    # The overlap window's lower bound comes from BIGQUERY'S clock, not this
    # host's. The job timestamps it is compared against are BigQuery's, and
    # comparing two clocks is how a skewed runner turns a real overlap into a
    # phantom (or hides one) — the same two-clock trap the GC cell's liveness
    # signal was written to avoid.
    since_rows = _bq_json(proj, "SELECT UNIX_MILLIS(CURRENT_TIMESTAMP()) AS t")
    if not since_rows:
        led.skipped(engine, "-", "load_pool", "-",
                    "load_pool: BigQuery would not answer for its own clock", "no clock")
        return
    since = int(since_rows[0]["t"])

    with led.span(f"load_pool[{engine}]: load --pool {_POOL_TABLES}"):
        lp = rivet("load", "-c", str(cfg), "--pool", str(_POOL_TABLES),
                   env=child, timeout=None)
    if not lp.ok:
        led.failed(engine, "-", "load_pool", "-",
                   f"load_pool: `rivet load --pool {_POOL_TABLES}` failed — "
                   f"{(lp.stderr or lp.stdout or '').strip()[-200:]}", "load")
        return

    # ── oracle 1: completeness, per table, against the SOURCE ──
    src = docker_exec(
        name, "psql", "-U", "rivet", "-d", "rivet", "-t", "-A", "-F", ",",
        "-c", " UNION ALL ".join(
            f"SELECT '{t}', COUNT(*), COALESCE(SUM(id), 0) FROM {t}" for t in tables),
        timeout=600)
    source: dict[str, tuple[int, int]] = {}
    for line in (src.stdout or "").splitlines():
        bits = line.strip().split(",")
        if len(bits) == 3 and bits[0] in tables:
            source[bits[0]] = (int(bits[1]), int(bits[2]))

    warehouse: dict[str, tuple[int, int]] = {}
    for row in _bq_json(proj, " UNION ALL ".join(
            f"SELECT '{t}' AS t, COUNT(*) AS n, IFNULL(SUM(id), 0) AS s "
            f"FROM `{proj}.{pool_dset}.{t}`" for t in tables)):
        warehouse[row["t"]] = (int(row["n"]), int(row["s"]))

    bad = [f"{t}: source={source.get(t)} bigquery={warehouse.get(t)}"
           for t in tables if source.get(t) != warehouse.get(t)]
    if len(source) != _POOL_TABLES:
        led.failed(engine, "-", "load_pool", "-",
                   f"load_pool: the SOURCE re-query answered for {len(source)} of "
                   f"{_POOL_TABLES} tables — the oracle itself is incomplete, so a "
                   f"match would prove nothing", "oracle")
        return
    if bad:
        led.failed(engine, "-", "load_pool", "-",
                   f"load_pool: {len(bad)} of {_POOL_TABLES} tables disagree with the "
                   f"source after a --pool {_POOL_TABLES} load — " + "; ".join(bad[:4]),
                   "rows")
        return

    # ── oracle 2: the pool really OVERLAPPED (never vacuous) ──
    jobs = _bq_json(proj, f"""
        SELECT job_id, UNIX_MILLIS(start_time) AS s, UNIX_MILLIS(end_time) AS e
        FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
        WHERE creation_time >= TIMESTAMP_MILLIS({since})
          AND statement_type = 'LOAD_DATA'
          AND EXISTS (SELECT 1 FROM UNNEST(labels) l
                      WHERE l.key = 'rivet_table' AND STARTS_WITH(l.value, '{_POOL_PREFIX}'))
    """)
    spans = [(int(j["s"]), int(j["e"])) for j in jobs
             if j.get("s") is not None and j.get("e") is not None]
    if len(spans) < 2:
        led.failed(engine, "-", "load_pool", "-",
                   f"load_pool: BigQuery's job history shows {len(spans)} timed LOAD_DATA "
                   f"job(s) for these tables — the concurrency oracle has nothing to "
                   f"measure, so a PASS here would be vacuous", "no jobs")
        return
    peak = _max_overlap(spans)
    if peak < 2:
        led.failed(engine, "-", "load_pool", "-",
                   f"load_pool: {len(spans)} LOAD_DATA jobs and NONE overlapped — every "
                   f"row arrived, but `--pool {_POOL_TABLES}` ran them one after another; "
                   f"the pool degraded to sequential", "no overlap")
        return

    led.passed(engine, "-", "load_pool", "-",
               f"load_pool: {_POOL_TABLES} tables loaded by `--pool {_POOL_TABLES}` — "
               f"every table's count AND sum(id) match the source, and BigQuery's job "
               f"history shows {peak} of {len(spans)} LOAD_DATA jobs in flight at once",
               f"peak={peak}/{len(spans)}")

    gcp.bq_delete_dataset(proj, pool_dset)
    gcp.gcs_delete_prefixes(bucket, [f"exports/{t}/" for t in tables])
    docker_exec(name, "psql", "-U", "rivet", "-d", "rivet", "-q",
                stdin="".join(f"DROP TABLE IF EXISTS {t};" for t in tables), timeout=600)


def _gcs_ls(uri: str) -> list[str]:
    p = run(["gcloud", "storage", "ls", "-r", uri], timeout=300)
    return [ln.strip() for ln in p.stdout.splitlines() if ln.strip().startswith("gs://")]


def verify_gc_survival(led: Ledger, *, bucket: str, pfx: str, cfg_text: str,
                       work: Path, child: dict[str, str], engine: str) -> None:
    """`load.gc_orphans` must delete crash debris and SPARE a live run's parts.

    Until now this cell was a SKIP whose reason read "covered in BigQuery stage"
    — and the BigQuery stage contained no such check, in either implementation.
    So the one gate cell guarding a DELETE path had never run. That matters more
    than most: `gc_orphans` removes `.parquet` from a shared prefix, and the
    dangerous case is a concurrent extract whose committed parts have no manifest
    yet — indistinguishable from crash debris except by a liveness signal.

    The contract (src/load/reconcile.rs) is three-way, and all three arms are
    asserted here:

    * a part in a `success` manifest → KEEP, always;
    * a part with NO manifest and NO live run → crash debris → DELETE;
    * a part with NO manifest while a run IS live → SPARE.

    Liveness is `ledger_active || has_active_running_manifest`. This check drives
    the SECOND signal — a `running` marker manifest in the bucket — because that
    is the cross-boundary projection a foreign-host load actually relies on, and
    because it needs no surgery on the state DB to arrange. (Worth knowing: the
    caller passes `active = false` when there is NO state store at all, while
    reconcile.rs's docstring claims a stateless load is conservative. The bucket
    marker is what protects that case, not a conservative default.)

    Runs against the prefix the golden stage just loaded, BEFORE it is wiped, so
    a real `success` manifest and real parts are already in place.
    """
    base = f"gs://{bucket}/{pfx}"
    listing = _gcs_ls(base + "/")
    parts = [k for k in listing if k.endswith(".parquet")]
    manifests = [k for k in listing if k.rsplit("/", 1)[-1].startswith("manifest-")]
    if not parts or not manifests:
        led.skipped(engine, "-", "gc_survival", "-",
                    f"gc_survival[{engine}]: prefix has no part/manifest to work from",
                    "no fixture")
        return

    real_part, real_manifest = parts[0], manifests[0]
    gc_cfg = work / f"gc_{engine}.yaml"
    gc_cfg.write_text(cfg_text + "  gc_orphans: true\n")

    def plant_orphan(name: str) -> str:
        """A byte-identical copy of a real part: valid parquet, referenced by no
        manifest — exactly the shape of a part committed before a crash."""
        dst = f"{base}/{name}"
        run(["gcloud", "storage", "cp", real_part, dst], timeout=300)
        return dst

    def exists(uri: str) -> bool:
        return any(k == uri for k in _gcs_ls(base + "/"))

    fails: list[str] = []

    def _gc_load_why(tries: int = 3) -> str:
        """The empty string when the load succeeded, else WHY it did not.

        A boolean here cost a whole investigation. When arm 2 failed on ONE engine
        (2026-08-22, mssql), the cell reported `gc-load-failed-with-a-running-marker`
        and nothing else — the load's own error, which named the cause in one
        sentence, went to a `Proc` that was reduced to `p.ok` and dropped. The
        message IS the finding; carry its tail into the ledger detail.

        `rivet load` here hits REAL BigQuery/GCS, so a transient Google-side hiccup
        is an infra blip, not a gc-logic failure — retry like seed_engine/doctor.
        A real gc failure fails every attempt (and the text below says which).
        """
        p = rivet("load", "-c", str(gc_cfg), env=child, timeout=None)
        for _ in range(tries - 1):
            if p.ok:
                break
            time.sleep(2.0)
            p = rivet("load", "-c", str(gc_cfg), env=child, timeout=None)
        if p.ok:
            return ""
        why = (p.stderr or p.stdout or "").strip()
        return " / ".join(why.splitlines()[-2:]) if why else f"exit {p.returncode}"

    # ── arm 1: no live run → the unmanifested part is debris → collected ──
    dead = plant_orphan("orphan-crash-debris.parquet")
    why = _gc_load_why()
    if why:
        led.failed(engine, "-", "gc_survival", "-",
                   f"gc_survival[{engine}]: the gc load itself failed — {why}", "load")
        return
    if exists(dead):
        fails.append("debris-survived(no live run, yet the unmanifested part was kept) ")
    if not exists(real_part):
        fails.append("success-part-deleted(a manifested part must ALWAYS survive) ")

    # ── arm 2: a live run → the unmanifested part is spared ──
    live = plant_orphan("orphan-inflight.parquet")
    marker = json.loads(run(["gcloud", "storage", "cat", real_manifest], timeout=300).stdout)
    marker["status"] = "running"          # snake_case, per ManifestStatus's serde
    marker["run_id"] = "gc-survival-probe"
    marker["parts"] = []                  # a running marker carries no parts
    # started_at must be NEWER than every other manifest of this export, or the
    # marker reads as SUPERSEDED — a crashed run's leftover, not a live signal.
    marker["started_at"] = "2099-01-01T00:00:00Z"
    # ENGINE-KEYED, like every other file this stage stages into `work`
    # (`bqload_{engine}.yaml`, `gc_{engine}.yaml`): `work` is ONE directory shared
    # by the three engine legs, which `run_bigquery_golden` runs CONCURRENTLY.
    # A single `manifest-gc-survival-probe.json` is written by all three, and the
    # window between `write_text` and the `gcloud cp` that READS it is a whole
    # subprocess spawn (~1-2 s) — so a sibling leg passing through the same lines
    # overwrites the file mid-copy and this engine uploads the SIBLING'S manifest
    # into its own prefix. rivet then (correctly) refuses the load: "the load
    # prefix holds manifests from 2 DIFFERENT SOURCES (mssql:…, mysql:…)" — a
    # two-source prefix is exactly the silent warehouse-clobber `ensure_single_
    # source` exists to stop. The cell recorded that refusal as a gc failure.
    # Measured on the 2026-08-22 gate: mssql and mysql ran 0.12 s apart (their
    # arm-1 loads land at 08:55:16.207 / .329 in `load_run`), mssql's arm-2 load
    # failed all three attempts and wrote no ledger row at all — it never reached
    # the loader — while postgres, 13 s out of step, passed.
    mk_local = work / f"manifest-gc-survival-probe-{engine}.json"
    mk_local.write_text(json.dumps(marker))
    mk_remote = f"{base}/manifest-gc-survival-probe.json"
    run(["gcloud", "storage", "cp", str(mk_local), mk_remote], timeout=300)

    # The fixture is not inert, and it is not a SIBLING'S: read the marker back
    # from the bucket — the copy `rivet load` will actually read — and demand it
    # is this engine's, `running`. Without this the two ways the plant can go
    # wrong (no marker at all → arm 2 degenerates into arm 1 and passes for the
    # wrong reason; a foreign marker → a load refusal misread as a gc failure)
    # are both invisible.
    back = json.loads(run(["gcloud", "storage", "cat", mk_remote], timeout=300).stdout or "{}")
    planted = (back.get("source") or {}).get("engine")
    if planted != engine or back.get("status") != "running":
        run(["gcloud", "storage", "rm", mk_remote, live], timeout=300)
        led.failed(engine, "-", "gc_survival", "-",
                   f"gc_survival[{engine}]: the planted running marker is "
                   f"engine={planted!r} status={back.get('status')!r}, not this engine's live "
                   "marker — the FIXTURE is wrong (a sibling engine leg overwrote the staged "
                   "file), so arm 2 graded nothing about gc", "fixture")
        return

    why = _gc_load_why()
    if why:
        fails.append(f"gc-load-failed-with-a-running-marker[{why}] ")
    elif not exists(live):
        fails.append("inflight-part-DELETED(a live run's unmanifested part was collected) ")

    run(["gcloud", "storage", "rm", mk_remote, live], timeout=300)

    if fails:
        led.failed(engine, "-", "gc_survival", "-",
                   f"gc_survival[{engine}]: {''.join(fails)}", "".join(fails))
    else:
        led.passed(engine, "-", "gc_survival", "-",
                   f"gc_survival[{engine}]: debris collected, live run's unmanifested part "
                   "spared, manifested parts untouched", "3 arms")


def _bq_one_engine(
    led: Ledger,
    engine: str,
    *,
    proj: str,
    dset: str,
    bucket: str,
    matrix: str,
    work: Path,
    keep: bool,
    up: Callable[..., str | None],
    seed: Callable[..., str],
) -> None:
    """One engine's whole BQ leg: export → load → compare to the source → clean up.

    Fully self-contained PER ENGINE — its own dataset (`{dset}_{engine}`), GCS prefix
    (`bq/{engine}`), config, and container — which is exactly why the outer loop can
    run these concurrently (the per-dataset fix below removed the shared-table hazard
    the old sequential-only comment warned about). Records its rows/spans into `led`
    (a buffered child under parallelism). The `bq {engine}: run/load/readback` spans answer whether the load and the
    read-back SELECT actually speed up under parallelism or are BQ-rate-limited."""
    versions = _matrix_cfg("versions", engine).splitlines()
    fields = versions[0].split() if versions else []
    image = fields[1] if len(fields) > 1 else ""
    port = _PORTS[engine]

    url = up(led, engine, _TAG, image, port)
    if not url:
        led.skipped("bigquery", engine, "golden", "-",
                    f"BigQuery[{engine}]: bring-up failed", "bring-up")
        return None
    if seed(engine, _TAG, url):
        led.skipped("bigquery", engine, "golden", "-",
                    f"BigQuery[{engine}]: seed errors", "seed")
        if not keep:
            docker("rm", "-fv", engine_container(engine, _TAG))
        return None

    tlsblk = "\n  tls: {accept_invalid_certs: true}" if engine == "mssql" else ""
    exp = matrix
    # ONE DATASET PER SOURCE. Every engine exports a table called `rivet_type_matrix`
    # and `rivet load` derives the warehouse table from that name, so a shared dataset
    # would mean three databases writing one table last-write-wins — which `rivet load`
    # refuses, and rightly (2026-08-03: postgres owned it, mysql/mssql failed). Three
    # configs → three DESTINATIONS (own dataset + own GCS prefix), so the legs are
    # independent and the ownership guard stays ARMED. This independence is what makes
    # the outer loop safe to parallelise.
    eng_dset = f"{dset}_{engine}"
    gcp.bq_ensure_dataset(proj, eng_dset)
    pfx = f"release-oracle/bq/{engine}"
    cfgf = work / f"bqload_{engine}.yaml"
    cfgf.write_text(
        "source:\n"
        f"  type: {engine}\n"
        f"  url_env: ORACLE_URL{tlsblk}\n"
        "exports:\n"
        f"  - name: {exp}\n"
        f"    table: {matrix}\n"
        "    mode: full\n"
        "    format: parquet\n"
        f"    destination: {{type: gcs, bucket: {bucket}, prefix: {pfx}/}}\n"
        "load:\n"
        "  target: bigquery\n"
        f"  project: {proj}\n"
        f"  dataset: {eng_dset}\n"
    )

    # Clear the prefix first: a leftover part from an earlier run would be loaded
    # alongside this one's and the read-back would compare a union.
    gcp.gcs_delete_prefix(bucket, f"{pfx}/")
    # And the table: a run killed before its cleanup strands it, and a fresh state DB refuses to overwrite it.
    gcp.bq_delete_table(proj, eng_dset, exp)

    got = ""
    child = {"ORACLE_URL": url}
    with led.span(f"bq {engine}: run"):
        rp = rivet("run", "-c", str(cfgf), env=child, timeout=None)
    lp = None
    if rp.ok:
        with led.span(f"bq {engine}: load"):
            lp = rivet("load", "-c", str(cfgf), env=child, timeout=None)
    if rp.ok and lp is not None and lp.ok:
        with led.span(f"bq {engine}: source-vs-warehouse"):
            _grade_against_source(led, engine, url, eng_dset, exp)
            got = "loaded"
        with led.span(f"bq {engine}: chain"):
            _grade_chain(led, engine, url, exp, bucket, pfx, cfgf, eng_dset)
    else:
        failed_proc = rp if not rp.ok else lp
        leg = "run" if not rp.ok else "load"
        why = ((failed_proc.stderr or failed_proc.stdout or "").strip()
               if failed_proc is not None else "")
        tail = " / ".join(why.splitlines()[-3:]) if why else "no output captured"
        led.failed("bigquery", engine, "golden", "-",
                   f"BigQuery[{engine}]: rivet {leg} failed — {tail}", "load")

    # gc_survival needs the prefix AS LOADED (a success manifest + real parts), so it
    # runs here — after the read-back, before the wipe.
    if got:
        verify_gc_survival(led, bucket=bucket, pfx=pfx, cfg_text=cfgf.read_text(),
                           work=work, child=child, engine=engine)

    # The LOAD pool, at its full declared width. Postgres only — the load reads
    # Parquet from GCS and talks to BigQuery, so the source engine cannot change
    # how the pool behaves; see verify_load_pool's own doc.
    if engine == "postgres":
        with led.span(f"{engine}: load_pool"):
            verify_load_pool(led, proj=proj, dset=dset, bucket=bucket, work=work,
                             child=child, engine=engine, url=url)

    try:
        gcp.gcs_delete_prefix(bucket, f"{pfx}/")
        if not keep:
            gcp.bq_delete_dataset(proj, eng_dset)
    finally:
        # The engine container goes even when a cloud cleanup raises.
        if not keep:
            docker("rm", "-fv", engine_container(engine, _TAG))
    return None


def _bq_one_engine_graded(led: Ledger, engine: str, **kw) -> None:
    """`_bq_one_engine`, with an escaping error graded as this engine's FAIL — a cloud
    call that raises must not take every other engine's graded rows down with it."""
    try:
        _bq_one_engine(led, engine, **kw)
    except Exception as e:  # noqa: BLE001 — graded, never swallowed
        led.failed(engine, "bq", "bigquery", "-", f"bigquery[{engine}]: stage raised: {e!r}"[:400],
                   "raised")


def run_bigquery_golden(
    led: Ledger,
    *,
    keep: bool = False,
    parallel: int = 1,
    bring_up: Callable[..., str | None],
    seed_engine: Callable[..., str],
) -> None:
    _up, _seed = bring_up, seed_engine

    proj_env = _matrix_cfg("bq", "project_env")
    dset_env = _matrix_cfg("bq", "dataset_env")
    proj = os.environ.get(proj_env, "")
    dset = os.environ.get(dset_env, "")

    # An absent tool and an absent credential are both SKIP, and are reported
    # separately so the reader knows which one to go fix.
    if not have("bq"):
        led.skipped("bigquery", "-", "golden", "-", "BigQuery: bq CLI absent", "no bq")
        return
    if not proj or not dset:
        led.skipped("bigquery", "-", "golden", "-",
                    f"BigQuery: set {proj_env} and {dset_env} to run the cloud oracle",
                    "no creds")
        return

    led.phase(f"BigQuery golden stage ({proj}.{dset}_<engine>) — one dataset PER SOURCE")
    bucket = os.environ.get("BQ_ORACLE_BUCKET") or "rivet_data_test"
    matrix = _matrix_cfg("bq", "tables")  # the single comprehensive matrix name
    work = _work_dir()

    engines = [e for e in _matrix_cfg("engines").split()
               if e != "mongo" and _PORTS.get(e) is not None]  # mongo: no type matrix

    kw = dict(proj=proj, dset=dset, bucket=bucket, matrix=matrix, work=work,
              keep=keep, up=_up, seed=_seed)
    cap = max(1, parallel)
    if cap == 1 or len(engines) <= 1:
        for engine in engines:
            with led.span(f"bq {engine}: engine-total"):
                _bq_one_engine_graded(led, engine, **kw)
    else:
        # Each engine's leg is independent (own dataset/prefix/config/container), so
        # race them — same buffered-child pattern as the engine matrix, so an engine's
        # output stays contiguous. BQ load jobs are async per-table and queries are
        # slot-scheduled, so concurrency is COST-neutral (identical bytes) and only
        # compresses wall-clock; the `bq {engine}: load/readback` spans measure how
        # much the load and the read-back SELECT actually parallelise vs BQ throttling.
        from concurrent.futures import ThreadPoolExecutor
        subs = {e: led.buffered_child() for e in engines}

        def run_one(engine: str) -> None:
            with subs[engine].span(f"bq {engine}: engine-total"):
                _bq_one_engine_graded(subs[engine], engine, **kw)

        workers = min(cap, len(engines))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(run_one, engines))
        for engine in engines:  # deterministic order, not completion order
            subs[engine].flush_into(led)
