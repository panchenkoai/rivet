"""Release-oracle BigQuery golden stage — the real-cloud final oracle.

Port of `dev/release-oracle/lib/bigquery.sh`.

Everything before this stage runs against local fakes (MinIO / fake-gcs /
Azurite) or a local warehouse. This one runs against the real thing, and is made
DETERMINISTIC by a per-engine checked-in golden rather than by fuzzy count
checks.

For every SQL engine (postgres / mysql / mssql — Mongo has no type matrix, it is
full-scan only) the round trip goes through rivet on BOTH legs: `rivet run`
stages that engine's `rivet_type_matrix` to a REAL GCS bucket, `rivet load`
loads it GCS → BigQuery, and then `bq query` — gcloud, an INDEPENDENT reader,
not rivet re-reading its own output — pulls it back as JSON. Every column value
is compared to `golden/bigquery_type_matrix.json`, keyed
`{engine: {table: [rows]}}`. Any divergence fails the release, because what
moved is either rivet's type export or BigQuery's Parquet mapping, and both are
silent: decimal precision, TIMESTAMP instants, BYTES, NUMERIC-widened unsigned
ints and JSON all survive a row count unharmed.

RE-BLESSING IS AN EXPLICIT OPT-IN (`--bless-bigquery-golden` → `bless=True`).
A normal run only ever READS the golden; nothing here overwrites it as a side
effect. A gate that silently re-captures its own baseline cannot fail.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Callable

from .core import (
    ROOT,
    Ledger,
    Proc,
    Status,
    docker,
    docker_exec,
    engine_container,
    have,
    rivet,
    run,
    wait_until,
)

# The golden and the two dependency-free helper scripts stay in the BASH tree and
# are shared, not copied: one blessed artifact and one matrix parser, so the two
# implementations cannot come to mean different things while the port is in
# flight. (A second copy of a golden is a golden that will drift.)
_BASH_HERE = ROOT / "dev" / "release-oracle"
_LIB = _BASH_HERE / "lib"
_MATRIX_YAML = _BASH_HERE / "matrix.yaml"
GOLDEN = _BASH_HERE / "golden" / "bigquery_type_matrix.json"

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


def _work_dir() -> Path:
    """The driver exports `WORK` for the whole run; make our own if it did not."""
    env = os.environ.get("WORK")
    return Path(env) if env else Path(tempfile.mkdtemp(prefix="rivet-oracle-bq-"))


# ── engine bring-up / seed ─────────────────────────────────────────────────────
# These mirror the driver's own `bring_up` / `seed_engine`. They are duplicated
# here rather than imported from `__main__`, because importing the module that is
# currently EXECUTING as `__main__` re-runs it under a second name. Both are
# injectable below, so the driver can pass its own copies and keep one
# definition; the right long-term home is `core`.
def _bring_up(led: Ledger, engine: str, tag: str, image: str, port: int) -> str | None:
    """Start one engine×version and wait for it to answer. Returns its URL, or
    None when it could not start (the caller records the SKIP row).

    The container name is a plain function of (engine, tag) — see
    `core.engine_container`. In bash that expansion had to sit on its own line,
    because a same-line `${eng}` read the ENCLOSING scope and so named THIS
    stage's container after whichever engine the main loop had visited last.
    """
    name = engine_container(engine, tag)
    docker("rm", "-fv", name)

    args: list[str] = ["run", "-d", "--name", name]
    if engine == "postgres":
        args += ["-e", "POSTGRES_USER=rivet", "-e", "POSTGRES_PASSWORD=rivet",
                 "-e", "POSTGRES_DB=rivet", "-p", f"{port}:5432"]
    elif engine == "mysql":
        args += ["-e", "MYSQL_ROOT_PASSWORD=rivet", "-e", "MYSQL_DATABASE=rivet",
                 "-e", "MYSQL_USER=rivet", "-e", "MYSQL_PASSWORD=rivet", "-p", f"{port}:3306"]
    elif engine == "mssql":
        args += ["-e", "ACCEPT_EULA=Y", "-e", "MSSQL_SA_PASSWORD=Rivet_Passw0rd!",
                 "-p", f"{port}:1433"]
    elif engine == "mongo":
        args += ["-p", f"{port}:27017"]
    else:
        led.skip(f"{engine}:{tag} could not start ({image})")
        return None

    if not docker(*args, image).ok:
        led.skip(f"{engine}:{tag} could not start ({image})")
        return None

    probes = {
        "postgres": ["pg_isready", "-U", "rivet"],
        "mysql": ["mysqladmin", "ping", "-urivet", "-privet"],
        "mssql": ["/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
                  "-P", "Rivet_Passw0rd!", "-C", "-Q", "SELECT 1"],
        "mongo": ["mongosh", "--quiet", "--eval", "db.runCommand({ping:1})"],
    }[engine]
    # Best-effort (~90 s). Deliberately not fatal: the URL is returned either
    # way and the first real query is what decides, so a slow-but-alive engine
    # is not turned into a phantom bring-up failure.
    wait_until(lambda: docker_exec(name, *probes, timeout=20).ok, tries=45, delay=2.0)

    if engine == "mssql":
        docker_exec(name, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
                    "-P", "Rivet_Passw0rd!", "-C", "-Q",
                    "IF DB_ID('rivet') IS NULL CREATE DATABASE rivet")

    return _matrix_cfg("url", engine).replace("%PORT%", str(port))


def _seed_engine(engine: str, tag: str, url: str) -> str:
    """Seed one engine from the canonical seed. Returns an error summary, "" when
    clean.

    Mongo is the ONLY engine seeded FROM THE HOST (pymongo); the SQL engines seed
    inside the container over `docker exec`. That asymmetry is why the driver
    preflights pymongo — without it, a host python lacking the module turned
    every mongo cell into a cryptic "seed had errors" with nothing pointing at
    the environment as the cause.
    """
    name = engine_container(engine, tag)
    seed = ROOT / _matrix_cfg("seed", engine)

    if engine == "postgres":
        p: Proc = docker_exec(name, "psql", "-U", "rivet", "-d", "rivet", "-q",
                              "-v", "ON_ERROR_STOP=1", stdin=seed.read_text(), timeout=900)
    elif engine == "mysql":
        p = docker_exec(name, "mysql", "-urivet", "-privet", "rivet",
                        stdin=seed.read_text(), timeout=900)
    elif engine == "mssql":
        docker("cp", str(seed), f"{name}:/tmp/s.sql")
        p = docker_exec(name, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
                        "-P", "Rivet_Passw0rd!", "-d", "rivet", "-C", "-i", "/tmp/s.sql",
                        timeout=900)
    else:  # mongo — from the host
        p = run(["python3", str(seed)], timeout=1800,
                env={"RIVET_MONGO_URI": url, "RIVET_SEED_USERS": "150000",
                     "RIVET_SEED_ORDERS": "150000"})

    needles = ("error", "msg ") if engine == "mssql" else ("error",)
    hits = [ln for ln in p.out.splitlines()
            if any(n in ln.lower() for n in needles)][:3]
    # The exit status is the primary signal and the grep only supplies detail:
    # deciding purely on the text (as the bash did) lets a seed that failed
    # silently — non-zero, nothing matching "error" — read as seeded.
    if p.ok and not hits:
        return ""
    return "; ".join(hits) or f"exit {p.returncode}"


# ── the stage ──────────────────────────────────────────────────────────────────
def _load_golden() -> dict:
    """The checked-in golden, or {} when it has never been blessed."""
    if not GOLDEN.exists():
        return {}
    try:
        return json.loads(GOLDEN.read_text())
    except json.JSONDecodeError:
        return {}


def _canon(doc: object) -> str:
    """Both sides of the comparison go through the SAME serializer, so a diff can
    only mean a real value change — never key order."""
    return json.dumps(doc, sort_keys=True)


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

    # ── arm 1: no live run → the unmanifested part is debris → collected ──
    dead = plant_orphan("orphan-crash-debris.parquet")
    if not rivet("load", "-c", str(gc_cfg), env=child, timeout=None).ok:
        led.failed(engine, "-", "gc_survival", "-",
                   f"gc_survival[{engine}]: the gc load itself failed", "load")
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
    mk_local = work / f"manifest-gc-survival-probe.json"
    mk_local.write_text(json.dumps(marker))
    mk_remote = f"{base}/manifest-gc-survival-probe.json"
    run(["gcloud", "storage", "cp", str(mk_local), mk_remote], timeout=300)

    if not rivet("load", "-c", str(gc_cfg), env=child, timeout=None).ok:
        fails.append("gc-load-failed-with-a-running-marker ")
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
    bless: bool,
    keep: bool,
    up: Callable[..., str | None],
    seed: Callable[..., str],
) -> dict | None:
    """One engine's whole BQ leg: export → load → read back → compare → clean up.

    Fully self-contained PER ENGINE — its own dataset (`{dset}_{engine}`), GCS prefix
    (`bq/{engine}`), config, and container — which is exactly why the outer loop can
    run these concurrently (the per-dataset fix below removed the shared-table hazard
    the old sequential-only comment warned about). Records its rows/spans into `led`
    (a buffered child under parallelism) and returns the blessed rows (bless mode) or
    None. The `bq {engine}: run/load/readback` spans answer whether the load and the
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
    run(["bq", f"--project_id={proj}", "mk", "-f", "--dataset", f"{proj}:{eng_dset}"])
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
    run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])

    got = ""
    child = {"ORACLE_URL": url}
    with led.span(f"bq {engine}: run"):
        rp = rivet("run", "-c", str(cfgf), env=child, timeout=None)
    lp = None
    if rp.ok:
        with led.span(f"bq {engine}: load"):
            lp = rivet("load", "-c", str(cfgf), env=child, timeout=None)
    if rp.ok and lp is not None and lp.ok:
        with led.span(f"bq {engine}: readback"):
            q = run(["bq", f"--project_id={proj}", "query", "--nouse_legacy_sql",
                     "--format=prettyjson",
                     f"SELECT * FROM `{proj}.{eng_dset}.{exp}` ORDER BY id"], timeout=None)
            got = run(["python3", str(_LIB / "normalize_bq.py")], stdin=q.stdout).stdout.strip()
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

    run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])
    run(["bq", f"--project_id={proj}", "rm", "-f", "-t", f"{eng_dset}.{exp}"])
    if not keep:
        docker("rm", "-fv", engine_container(engine, _TAG))
    if not got:
        # A read-back that returns NOTHING after a SUCCESSFUL run+load is a failure of
        # the INDEPENDENT oracle (Google's parquet reader via bq query), not a no-op —
        # it used to record no row at all, so absence read as green. The run/load-failed
        # path already recorded its FAIL above; only this (rp.ok and lp.ok) path did not.
        if rp.ok and lp is not None and lp.ok:
            led.failed("bigquery", engine, "golden", "-",
                       f"BigQuery[{engine}]: run+load OK but the bq read-back returned nothing "
                       f"({eng_dset}.{exp} empty) — the independent oracle saw zero rows",
                       "empty-readback")
        return None

    if bless:
        rows = json.loads(got)
        led.passed("bigquery", engine, "golden", "-",
                   f"BigQuery[{engine}] blessed ({len(rows)} rows)", "blessed")
        return rows
    want = _load_golden().get(engine, {}).get(matrix)
    if want is not None and _canon(json.loads(got)) == _canon(want):
        led.ok(f"BigQuery[{engine}] matches golden")
        led.add("bigquery", engine, "golden", "-", Status.PASS)
    else:
        led.failed("bigquery", engine, "golden", "-",
                   f"BigQuery[{engine}] DIVERGED from golden — "
                   "rivet type export or BQ mapping changed", "diverged")
    return None


def run_bigquery_golden(
    led: Ledger,
    *,
    bless: bool = False,
    keep: bool = False,
    parallel: int = 1,
    bring_up: Callable[..., str | None] | None = None,
    seed_engine: Callable[..., str] | None = None,
) -> None:
    _up = bring_up or _bring_up
    _seed = seed_engine or _seed_engine

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
    blessed: dict[str, dict[str, object]] = {}

    engines = [e for e in _matrix_cfg("engines").split()
               if e != "mongo" and _PORTS.get(e) is not None]  # mongo: no type matrix

    def collect(engine: str, rows: dict | None) -> None:
        if rows is not None:
            blessed.setdefault(engine, {})[matrix] = rows

    kw = dict(proj=proj, dset=dset, bucket=bucket, matrix=matrix, work=work,
              bless=bless, keep=keep, up=_up, seed=_seed)
    cap = max(1, parallel)
    if cap == 1 or len(engines) <= 1:
        for engine in engines:
            with led.span(f"bq {engine}: engine-total"):
                collect(engine, _bq_one_engine(led, engine, **kw))
    else:
        # Each engine's leg is independent (own dataset/prefix/config/container), so
        # race them — same buffered-child pattern as the engine matrix, so an engine's
        # output stays contiguous. BQ load jobs are async per-table and queries are
        # slot-scheduled, so concurrency is COST-neutral (identical bytes) and only
        # compresses wall-clock; the `bq {engine}: load/readback` spans measure how
        # much the load and the read-back SELECT actually parallelise vs BQ throttling.
        from concurrent.futures import ThreadPoolExecutor
        subs = {e: led.buffered_child() for e in engines}

        def run_one(engine: str) -> tuple[str, dict | None]:
            with subs[engine].span(f"bq {engine}: engine-total"):
                return engine, _bq_one_engine(subs[engine], engine, **kw)

        workers = min(cap, len(engines))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(run_one, engines))
        for engine in engines:  # deterministic order, not completion order
            subs[engine].flush_into(led)
        for engine, rows in results:
            collect(engine, rows)

    if bless:
        # The opt-in write, and the only one. NOTE (bash behaviour, kept): this
        # REPLACES the file with just the engines that produced a read-back this
        # invocation, so blessing while one engine is down drops that engine's
        # entry — and the next normal run then reports it DIVERGED. Bless with
        # every engine healthy.
        GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        GOLDEN.write_text(json.dumps(blessed, indent=2, sort_keys=True) + "\n")
        led.passed("bigquery", "-", "golden", "-",
                   f"BLESSED BQ golden (per engine) → {GOLDEN}", "blessed")
