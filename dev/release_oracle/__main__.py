"""Rivet Release Oracle — the go/no-go gate.

Brings up each pinned engine version, seeds it from the canonical seed, runs every
scenario against the local object-store fakes (MinIO / fake-gcs / Azurite), then
the BigQuery golden stage. GREEN everywhere ⇒ releasable.

    python3 -m dev.release_oracle                        # full gate
    python3 -m dev.release_oracle --engines postgres,mysql
    python3 -m dev.release_oracle --no-cloud             # local stage only
    python3 -m dev.release_oracle --keep                 # leave containers up
    python3 -m dev.release_oracle --bless-bigquery-golden

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

import argparse
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

from .core import Ledger, Status, engine_container, docker, have, remove_engine_containers, rivet, rivet_bin, run, HERE, ROOT
from . import bigquery, cdc, gifs, regression, release_path, scenarios


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

    wait_until(lambda: scenarios.store_up("s3") or scenarios.store_up("gcs"), tries=15, delay=1.0)

    # MinIO: its own client, run on the host network so 127.0.0.1 means the host.
    run(["docker", "run", "--rm", "--network", "host", "--entrypoint", "sh",
         "minio/mc:latest", "-c",
         f"mc alias set o http://127.0.0.1:9000 {scenarios.MINIO_ACCESS_KEY} "
         f"{scenarios.MINIO_SECRET_KEY} >/dev/null 2>&1 && mc mb -p o/{BUCKET} >/dev/null 2>&1; true"],
        timeout=180)

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
        urllib.request.urlopen(req, timeout=5).read()
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        pass  # already exists, or the emulator is down — the summary below says which

    if have("az"):
        run(["az", "storage", "container", "create", "--name", BUCKET,
             "--connection-string", scenarios.AZURITE_CONN], timeout=120)

    def mark(store: str) -> str:
        return "✓" if scenarios.store_up(store) else "✗"

    led.ok(f"stores up (bucket/container {BUCKET}: minio {mark('s3')} gcs {mark('gcs')} azure {mark('azure')})")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="release-oracle", add_help=True)
    ap.add_argument("--engines", default="", help="comma-separated subset, e.g. postgres,mysql")
    ap.add_argument("--no-cloud", action="store_true", help="local stage only (skip BigQuery)")
    ap.add_argument("--keep", action="store_true", help="leave engine containers up (debug)")
    ap.add_argument("--bless-bigquery-golden", action="store_true")
    ap.add_argument("--bless-local", action="store_true", help="re-capture verdict + duckdb-type goldens (implies --no-cloud)")
    ap.add_argument("--bless-cdc", action="store_true", help="re-capture the cdc state-snapshot golden")
    ap.add_argument("--bless-gifs", action="store_true",
                    help="re-capture docs/gifs/surface.lock.json AFTER re-rendering the GIFs")
    ns = ap.parse_args(argv)
    if ns.bless_local:
        ns.no_cloud = True
    return ns


def preflight(led: Ledger, *, bless_gifs: bool = False) -> None:
    """Everything that is source-agnostic, in the order the comments above give."""
    release_path.verify_release_build_path(led)
    scenarios.verify_state_migrations(led)
    scenarios.verify_inflight_run_stays_loadable(led)
    scenarios.verify_coverage_matrices(led)
    # The instructional GIFs are documentation that can go stale silently — they
    # are binary assets, so no test reads them and no diff flags them. Placed
    # among the cheap file-level guards, before anything spends twenty minutes on
    # containers: a GIF showing a command this binary rejects is worth knowing
    # early, and it is not fixable by re-rendering (the tape has to change).
    gifs.verify_gif_currency(led, bless=bless_gifs)
    scenarios.verify_replica_read(led)
    cdc.verify_cdc_e2e(led)
    regression.verify_release_regression(led)
    regression.verify_scale_memory(led)


def bring_up(led: Ledger, engine: str, tag: str, image: str, port: int) -> str | None:
    """Start one engine×version and wait for it to answer. Returns its URL, or
    None when it could not start (the caller records a SKIP).

    The container name is a plain function of (engine, tag) — see
    `core.engine_container`. The bash equivalent had to keep that expansion on
    its own line, because a same-line `${eng}` read the ENCLOSING scope and so
    named the BigQuery stage's container after whichever engine the main loop had
    visited last."""
    name = engine_container(engine, tag)
    # `-v`, not a bare `-f`. Every engine image here declares a VOLUME for its
    # data directory, and `docker run` with no `-v` of our own answers that by
    # creating an ANONYMOUS volume. `docker rm -f` deletes the container and
    # ORPHANS that volume — so each gate run leaked one per engine×version
    # (~15), invisibly, because nothing ever lists them. Measured on this
    # machine before the fix: 814 anonymous volumes holding 370 GB, against 30
    # named ones, on a disk at 89%. The engines are seeded from scratch every
    # run, so there is nothing in them worth keeping past teardown.
    docker("rm", "-fv", name)

    args: list[str] = ["run", "-d", "--name", name]
    if engine == "postgres":
        args += ["-e", "POSTGRES_USER=rivet", "-e", "POSTGRES_PASSWORD=rivet",
                 "-e", "POSTGRES_DB=rivet", "-p", f"{port}:5432"]
    elif engine == "mysql":
        args += ["-e", "MYSQL_ROOT_PASSWORD=rivet", "-e", "MYSQL_DATABASE=rivet",
                 "-e", "MYSQL_USER=rivet", "-e", "MYSQL_PASSWORD=rivet", "-p", f"{port}:3306"]
    elif engine == "mssql":
        args += ["-e", "ACCEPT_EULA=Y", "-e", "MSSQL_SA_PASSWORD=Rivet_Passw0rd!", "-p", f"{port}:1433"]
    elif engine == "mongo":
        args += ["-p", f"{port}:27017"]
    else:
        led.skipped(engine, tag, "all", "-", f"{engine}:{tag} unknown engine kind")
        return None

    if not docker(*args, image).ok:
        led.skip(f"{engine}:{tag} could not start ({image})")
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
        "mssql": [["/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
                   "-P", "Rivet_Passw0rd!", "-C", "-Q", "SELECT 1"]],
        "mongo": [["mongosh", "--quiet", "--eval", "db.runCommand({ping:1})"],
                  ["mongo", "--quiet", "--eval", "db.runCommand({ping:1})"]],
    }[engine]
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
    if not wait_until(ready, tries=45, delay=2.0):
        led.skipped(engine, tag, "all", "-",
                    f"{engine}:{tag} never became ready (no probe of "
                    f"{[p[0] for p in probes]} "
                    f"never passed twice in ~90s)", "not ready")
        return None

    if engine == "mssql":
        docker_exec(name, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
                    "-P", "Rivet_Passw0rd!", "-C", "-Q",
                    "IF DB_ID('rivet') IS NULL CREATE DATABASE rivet")

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

    if engine == "postgres":
        p = docker_exec(name, "psql", "-U", "rivet", "-d", "rivet", "-q",
                        "-v", "ON_ERROR_STOP=1", stdin=body, timeout=900)
    elif engine == "mysql":
        p = docker_exec(name, "mysql", "-urivet", "-privet", "rivet", stdin=body, timeout=900)
    elif engine == "mssql":
        docker("cp", str(seed), f"{name}:/tmp/s.sql")
        p = docker_exec(name, "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa",
                        "-P", "Rivet_Passw0rd!", "-d", "rivet", "-C", "-i", "/tmp/s.sql", timeout=900)
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


def engine_loop(led: Ledger, ns: argparse.Namespace) -> None:
    wanted = [e for e in ns.engines.split(",") if e] if ns.engines else None
    for engine in matrix_cfg("engines").split():
        if wanted and engine not in wanted:
            continue
        for line in matrix_cfg("versions", engine).splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            tag, image, port = parts[0], parts[1], int(parts[2])
            led.phase(f"{engine} {tag} ({image})")
            url = bring_up(led, engine, tag, image, port)
            if not url:
                led.add(engine, tag, "all", "-", Status.SKIP, "bring-up failed")
                continue
            # The seed is idempotent (DROP TABLE IF EXISTS …), so a transient
            # failure is retried: a fresh container under load can drop the seed
            # connection mid-stream, and crying wolf on that is worse than a retry.
            err = ""
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
                continue
            led.ok("seeded")
            scenarios.run_scenarios(led, engine, tag, url)
            if not ns.keep:
                docker("rm", "-fv", engine_container(engine, tag))


def main(argv: list[str] | None = None) -> int:
    ns = parse_args(argv)

    if not rivet_bin().is_file() or not os.access(rivet_bin(), os.X_OK):
        print(f"rivet binary not found at {rivet_bin()} (build --release or set RIVET_BIN)", file=sys.stderr)
        return 2
    if not have("duckdb"):
        print("duckdb not on PATH (needed for the integrity oracle)", file=sys.stderr)
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

    os.environ.setdefault("BLESS_VERDICTS", "1" if ns.bless_local else "0")
    os.environ.setdefault("BLESS_DUCKDB", "1" if ns.bless_local else "0")
    os.environ.setdefault("BLESS_CDC", "1" if ns.bless_cdc else "0")

    led = Ledger()
    work = Path(tempfile.mkdtemp(prefix="rivet-oracle-"))
    os.environ["WORK"] = str(work)
    try:
        from datetime import datetime, timezone

        led.phase(f"Rivet Release Oracle — {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
        version = rivet("--version").stdout.splitlines()
        print(f"  rivet: {rivet_bin()} ({version[0] if version else 'unknown'})")

        start_stores(led)
        preflight(led, bless_gifs=ns.bless_gifs)
        engine_loop(led, ns)
        if not ns.no_cloud:
            # Inject THIS module's bring_up/seed_engine rather than letting the
            # stage use its own copies. The copies exist only because importing
            # the module executing as __main__ would re-run it — but a duplicate
            # is a duplicate: the readiness hardening (two consecutive probe
            # passes, and honouring the result) landed here and NOT there, so the
            # BQ stage's mysql leg still hit the initdb race and recorded
            # `SKIP seed` where the previous run had a PASS. One definition now.
            bigquery.run_bigquery_golden(led, bless=ns.bless_bigquery_golden,
                                         keep=ns.keep,
                                         bring_up=bring_up, seed_engine=seed_engine)
        return led.report()
    finally:
        if not ns.keep:
            remove_engine_containers()
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
