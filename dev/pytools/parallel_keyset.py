"""Parallel-keyset sparse GOLDEN (verifier + devbox runner + bench fixture),
plus the two Airflow-recipe init helpers.

Ports of:

* `dev/parallel_keyset/golden/verify.sh`    → `verify()`
  The golden itself: run keyset-parallel (`chunk_by_key: id`, `parallel: 4`,
  `chunk_size: 500000`) against an already-seeded engine and assert EXACTLY
  10,000,000 rows, 10,000,000 DISTINCT ids and 21 part files. The key is
  `id = n × 997`, so `span_per_row ≈ 997`: range chunking would carve ~20K
  near-empty windows out of the key SPAN, while keyset pages by ROW on the unique
  index and is immune. That immunity is what the fixed file count pins (see
  `dev/parallel_keyset/golden/README.md` for why it is 21 and not 20 — the
  verify.sh header comment still says 20, the default has said 21 for a while).

* `dev/parallel_keyset/golden/devbox_run.sh` → `devbox_run()`
  Seed the four pre-release batch stands (pg18:5518, mysql80:3580 signed AND
  BIGINT-UNSIGNED, mssql2022:1522), run the golden per engine, write a report.

* `dev/parallel_keyset/fixture.sh`           → `fixture()`
  The 1M-row skewed-key MySQL bench fixture on :3310 (95% dense in [1..950k],
  5% sparse out to ~501M) used by `examples/parallel_keyset_probe`.

* `docs/recipes/airflow/init/init-state-schema.sh` → `airflow_init_state_schema()`
* `docs/recipes/airflow/init/create-buckets.sh`    → `airflow_create_buckets()`
  **Recipe assets, not dev tooling.** They are the compose entrypoints of the
  Airflow recipe's two init containers (`rivet-state-schema-init`, `minio-init`),
  and the shipped `docker-compose.2.10.yaml` still points at the `.sh` files —
  the `minio/mc` image has no `python3`, so this port cannot replace that
  entrypoint. It exists so the same steps can be run (and debugged) by hand
  against a running recipe stack, and so the two vacuous-success bugs below are
  fixed somewhere. Their default hostnames (`minio`, `fake-gcs`,
  `rivet-state-db`, `host.docker.internal`) only resolve INSIDE the recipe's
  compose network; override them per call when running from the host.

Usage:

    python3 dev/pytools/parallel_keyset.py verify <engine> <url> [table] [rows] [files]
    python3 dev/pytools/parallel_keyset.py devbox-run [--report P] [--bin P] [--repo D]
    python3 dev/pytools/parallel_keyset.py fixture
    python3 dev/pytools/parallel_keyset.py airflow-init-state-schema
    python3 dev/pytools/parallel_keyset.py airflow-create-buckets

WHAT IS DELIBERATELY DIFFERENT FROM THE BASH (each one a bug it shipped):

1. **`devbox_run` always exited 0.** Its last line was `grep -E "GOLDEN
   (PASS|RED)" "$REPORT" || true`, so a RED golden — the entire point of the run
   — returned success to whatever launched it. It now returns 1 if any engine is
   RED, any verify could not run, or no verdict was produced at all.
2. **…and the summary was invisible.** `exec > "$REPORT" 2>&1` was still in
   effect for that final `grep`, so the summary it printed went INTO the report
   instead of to the operator watching the detached run. The report is still
   written byte-for-byte (the redirect is done with `dup2`, so children inherit
   it and streaming still works), and the verdict lines are now ALSO printed on
   the restored stdout.
3. **`verify.sh` left its output behind.** No trap, `out="$(mktemp -d)/out"`, and
   a golden run writes ~10M rows of parquet per engine — four engines of it, on
   every devbox run, never cleaned. The tempdir (only ever the one this script
   created, never a destination the caller named) is removed in a `finally`;
   `--keep` opts out. It also leaked the `$(mktemp)` file created just to derive
   `$(mktemp).yaml`; the config now lives inside the one tempdir.
4. **A duckdb failure produced no verdict.** Under `set -euo pipefail` the
   `got_rows=$(duckdb …)` assignment aborted the script mid-way, so the operator
   got an exit code and no `GOLDEN` line at all. An unreadable destination is now
   a named FAIL (`rows unreadable != 10000000`), and `duckdb` is a checked
   precondition instead of a mid-run surprise.
5. **A missing binary / wrong repo was discovered four failures later.**
   `devbox_run` did `cd ~/rivet` and `$RIVET_BIN --version` with neither checked
   (no `set -e`), then ran four engines against nothing. Both are loud
   preconditions now, as is `duckdb` (which `verify.sh` needs and `devbox_run`
   never checked).
6. **No `bash -lc "$SQLCMD …"`.** The mssql seed built a command STRING with the
   password inside it and re-parsed it with a shell inside the container; the
   argv form cannot be re-split. Same commands, same order.
7. **A masked seed failure.** `docker exec … < fixture.sql | tail -2` reports
   `tail`'s status (bug class 5), so a failed 10M-row seed was silent and the
   golden that followed was RED for the wrong reason. The seed's status is now
   checked and named (`SEED FAILED`), and — as in the bash — the verify still
   runs, so the report's shape is unchanged.
8. **`fixture.sh`'s readiness loop fell through.** After 45 failed probes the
   `for` loop just ended and the seed ran against a MySQL that was not up,
   failing with the client's error and no mention of the timeout. Now a `Fail`
   that says which container and how long it waited. The container is
   deliberately NOT torn down — it IS the fixture.
9. **`create-buckets.sh` could hang forever** (`until mc alias set …; do sleep 2;
   done` with no bound, in a `restart: "no"` init container), and both of its
   creates were `|| true` + `>/dev/null 2>&1`, so it printed "bucket ready" for a
   permission error or an unreachable emulator just as happily as for success
   (bug class 8). Bounded waits with a loud `Fail`, and "already exists" is now
   distinguished from a real error.
10. **`init-state-schema.sh` printed "state schema ready: X" seven times
    unconditionally** — `rivet state show … >/dev/null 2>&1 || true` — which is
    exactly backwards for a warm-up whose whole job is to prove the schema
    exists before the parallel wave tasks race for it. Each warm now reports its
    real outcome; a missing `rivet` binary is a hard `Fail` instead of seven
    lies. The exit code stays 0 so the recipe stack still comes up.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Sequence

if __package__:
    from . import cdc_stand, shell
else:  # executed as a plain script: `python3 dev/pytools/parallel_keyset.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import cdc_stand  # type: ignore[no-redef]
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT


def _say(msg: str = "") -> None:
    """stdout, FLUSHED. These lines interleave with CHILD output on the same fd,
    and a block-buffered `print` lands after it whenever stdout is a pipe or a
    file — the bash `echo` was unbuffered, so its ordering was always right.
    (Seen for real: "Ensuring bucket …" printed *after* the whole cargo run.)"""
    print(msg, flush=True)


USAGE = (
    "usage: dev/pytools/parallel_keyset.py verify <engine> <url> [table] [rows] [files] [--keep]\n"
    "       dev/pytools/parallel_keyset.py devbox-run [--report P] [--bin P] [--repo D]\n"
    "       dev/pytools/parallel_keyset.py fixture\n"
    "       dev/pytools/parallel_keyset.py airflow-init-state-schema\n"
    "       dev/pytools/parallel_keyset.py airflow-create-buckets"
)

# The golden. Empirically measured on the devbox, not theorised: the first of the
# four OFFSET-sampled ranges is the INCLUSIVE (-∞, b1], so it holds total/N + 1
# rows and tips into a 6th part at chunk_size=500_000 → 6/5/5/5 = 21 files.
GOLDEN_ROWS = 10_000_000
GOLDEN_FILES = 21
GOLDEN_TABLE = "keyset_sparse"

GOLDEN_CFG = """source:
  type: {engine}
  url: "{url}"{tls}
exports:
  - name: keyset_sparse_golden
    table: {table}
    mode: chunked
    chunk_by_key: id
    parallel: 4
    chunk_size: 500000
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out}/
"""
# SQL Server stands use a self-signed cert — accept it (the URL scheme is sqlserver://).
TLS_BLOCK = "\n  tls:\n    accept_invalid_certs: true"


# ── dev/parallel_keyset/golden/verify.sh ───────────────────────────────────────
def _resolve_rivet(spelled: str | Path) -> str:
    """A rivet binary that EXISTS: either on PATH or at an executable path.

    `verify.sh` defaulted to the bare name `rivet` and let the exec fail; on the
    pre-release stand the intended binary is the DOWNLOADED release artifact, and
    silently running a stale `rivet` from PATH instead would bless the wrong
    build's numbers.
    """
    text = str(spelled)
    if os.sep in text or text.startswith("."):
        p = Path(text).expanduser()
        if not (p.is_file() and os.access(p, os.X_OK)):
            raise shell.Fail(
                f"rivet binary not executable at {p}",
                code=2,
                hint="scp the downloaded RELEASE binary there, or set RIVET_BIN=",
            )
        return str(p)
    found = shell.require(
        text, hint="set RIVET_BIN to the downloaded release binary (never a rebuilt parent)"
    )
    return found


def _duck_count(query: str) -> int | None:
    """One integer via the duckdb CLI, or None when it could not be read.

    None is a NAMED failure at the call site, never a 0 or an empty string: "the
    destination is unreadable" and "the destination has no rows" are different
    findings, and the bash could express only the second (as an aborted script).
    """
    p = shell.run(["duckdb", "-noheader", "-list", "-c", query], cwd=ROOT, timeout=None)
    if not p.ok:
        return None
    text = "".join(p.stdout.split())
    return int(text) if text.isdigit() else None


def verify(
    engine: str,
    url: str,
    table: str = GOLDEN_TABLE,
    want_rows: int = GOLDEN_ROWS,
    want_files: int = GOLDEN_FILES,
    *,
    rivet_bin: str | Path | None = None,
    keep: bool = False,
) -> int:
    """Run keyset-parallel against an already-seeded engine and assert the golden.

    Returns 0 on PASS, 1 on RED, and rivet's own code if the export failed.
    """
    if not engine:
        # The bash's `${1:?usage: verify.sh …}` message, renamed to the port.
        raise shell.Fail("usage: verify <engine> <url> [table] [rows] [files]", code=2)
    if not url:
        raise shell.Fail("missing source url", code=2)
    binary = _resolve_rivet(rivet_bin or os.environ.get("RIVET_BIN") or "rivet")
    shell.require(
        "duckdb",
        hint="the golden is asserted by an INDEPENDENT re-read of the parquet; "
             "without duckdb there is no oracle",
    )

    work = Path(tempfile.mkdtemp(prefix="rivet-keyset-golden-"))
    out = work / "out"
    out.mkdir(parents=True, exist_ok=True)
    cfg = work / "golden.yaml"
    try:
        shell.atomic_write(
            cfg,
            GOLDEN_CFG.format(
                engine=engine,
                url=url,
                tls=TLS_BLOCK if engine == "mssql" else "",
                table=table,
                out=out,
            ),
        )

        _say(f"[{engine}/{table}] running keyset-parallel ({binary})…")
        run = shell.stream([binary, "run", "-c", str(cfg)], cwd=ROOT, timeout=None)
        if not run.ok:
            # The bash aborted here (`set -e`) with no verdict line; say which
            # step failed so the report is not silent about it.
            _say(f"[{engine}/{table}] FAIL: rivet run exited {run.returncode}")
            _say(f"[{engine}/{table}] GOLDEN RED ❌")
            return run.returncode or 1

        got_files = len(list(out.rglob("*.parquet")))
        got_rows = _duck_count(f"SELECT count(*) FROM read_parquet('{out}/**/*.parquet')")
        distinct = _duck_count(f"SELECT count(DISTINCT id) FROM read_parquet('{out}/**/*.parquet')")
        shown_rows = "unreadable" if got_rows is None else str(got_rows)
        shown_distinct = "unreadable" if distinct is None else str(distinct)

        _say(
            f"[{engine}/{table}] rows={shown_rows} distinct={shown_distinct} "
            f"files={got_files} (golden: {want_rows} rows / {want_files} files)"
        )

        fail = False
        if got_rows != want_rows:
            _say(f"FAIL: rows {shown_rows} != {want_rows}")
            fail = True
        if distinct != want_rows:
            _say(f"FAIL: distinct {shown_distinct} != {want_rows} (boundary drop/dup)")
            fail = True
        if got_files != want_files:
            _say(f"FAIL: files {got_files} != {want_files}")
            fail = True

        if not fail:
            _say(f"[{engine}/{table}] GOLDEN PASS ✅")
            return 0
        _say(f"[{engine}/{table}] GOLDEN RED ❌")
        return 1
    finally:
        # Only ever the tempdir this function created — a caller-named
        # destination is never touched (and none is accepted).
        if keep:
            shell.warn(f"kept {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


# ── dev/parallel_keyset/golden/devbox_run.sh ───────────────────────────────────
PG18_C = "stand-pg18-batch-1"
MY80_C = "stand-mysql80-batch-1"
MS22_C = "stand-mssql2022-batch-1"

PG18_URL = "postgresql://rivet:rivet@127.0.0.1:5518/rivet"
MY80_URL = "mysql://rivet:rivet@127.0.0.1:3580/rivet"
MS22_URL = f"sqlserver://sa:{cdc_stand.SA_PASSWORD}@127.0.0.1:1522/rivet"

# `-b` makes sqlcmd exit non-zero on a T-SQL error, which is what lets the seed
# failure below be detected at all.
MS_SQLCMD = [
    cdc_stand.SQLCMD, "-S", "localhost", "-U", "SA", "-P", cdc_stand.SA_PASSWORD, "-C", "-b",
]

_VERDICT = re.compile(r"GOLDEN (PASS|RED)")


def _banner(label: str) -> None:
    """The bash `run()` — a blank line then a `########## label ##########` rule."""
    _say()
    _say(f"########## {label} ##########")


def _tail_lines(text: str, n: int) -> None:
    for line in text.splitlines()[-n:]:
        _say(line)


def _seed(label: str, container: str, argv: Sequence[str], sql: Path, tail: int) -> bool:
    """`docker exec -i <c> <argv> < fixture.sql | tail -N`, with the status read.

    The pipeline's status came from `tail`, so a failed 10M-row seed printed
    nothing and left the golden to fail for an unexplained reason.
    """
    if not sql.is_file():
        _say(f"!! {label} SEED FAILED: fixture not found: {sql}")
        return False
    p = shell.docker_exec(container, *argv, stdin=sql.read_text(), timeout=None)
    _tail_lines(p.stdout, tail)
    if not p.ok:
        detail = (p.stderr or p.stdout).strip().splitlines()
        _say(f"!! {label} SEED FAILED (exit {p.returncode}): {detail[-1] if detail else ''}")
        return False
    return True


def _devbox_body(golden: Path, binary: str) -> int:
    """The four engine passes, in the bash's order, writing to the report."""
    _say(f"=== keyset-parallel GOLDEN on devbox — started {time.strftime('%a %b %d %H:%M:%S %Z %Y')} ===")
    version = shell.run([binary, "--version"]).out.strip().splitlines()
    _say(f"binary: {binary} ({version[0] if version else 'unknown'})")

    failures = 0

    def golden_run(engine: str, url: str, table: str) -> None:
        nonlocal failures
        try:
            if verify(engine, url, table, rivet_bin=binary) != 0:
                failures += 1
        except shell.Fail as e:
            # A per-engine environment gap must not abandon the other three.
            shell.bad(e.message)
            failures += 1

    _banner("PG18 seed")
    _seed("PG18", PG18_C, ("psql", "-U", "rivet", "-d", "rivet", "-q"),
          golden / "fixture_postgres.sql", 2)
    _banner("PG18 verify")
    golden_run("postgres", PG18_URL, GOLDEN_TABLE)

    _banner("MySQL80 signed seed")
    _seed("MySQL80 signed", MY80_C, ("mysql", "-urivet", "-privet", "rivet"),
          golden / "fixture_mysql.sql", 2)
    _banner("MySQL80 signed verify")
    golden_run("mysql", MY80_URL, GOLDEN_TABLE)

    _banner("MySQL80 unsigned seed")
    # The key is ENTIRELY above i64::MAX (id = 10^19 + n×10^8) — the regime that
    # broke unsigned keyset in the field. Same golden: keyset pages by row, so the
    # magnitude cannot change the counts, but the boundary literals and the paging
    # cursor must round-trip as u64.
    _seed("MySQL80 unsigned", MY80_C, ("mysql", "-urivet", "-privet", "rivet"),
          golden / "fixture_mysql_unsigned.sql", 2)
    _banner("MySQL80 unsigned verify")
    golden_run("mysql", MY80_URL, "keyset_sparse_unsigned")

    _banner("MSSQL2022 seed")
    # The batch stands do not auto-create `rivet`; SIMPLE recovery keeps the single
    # 10M-row INSERT from blowing the transaction log.
    prep = shell.docker_exec(
        MS22_C, *MS_SQLCMD, "-Q",
        "IF DB_ID('rivet') IS NULL CREATE DATABASE rivet; "
        "ALTER DATABASE rivet SET RECOVERY SIMPLE;",
        timeout=None,
    )
    if not prep.ok:
        detail = (prep.stderr or prep.stdout).strip().splitlines()
        _say(f"!! MSSQL2022 DB PREP FAILED (exit {prep.returncode}): "
              f"{detail[-1] if detail else ''}")
    _seed("MSSQL2022", MS22_C, (*MS_SQLCMD, "-d", "rivet", "-i", "/dev/stdin"),
          golden / "fixture_mssql.sql", 3)
    _banner("MSSQL2022 verify")
    golden_run("mssql", MS22_URL, GOLDEN_TABLE)

    _say()
    _say(f"=== GOLDEN run done {time.strftime('%a %b %d %H:%M:%S %Z %Y')} ===")
    return failures


def devbox_run(
    *,
    report: str | Path | None = None,
    rivet_bin: str | Path | None = None,
    repo: str | Path | None = None,
) -> int:
    """Seed the pre-release batch stands, run the golden per engine, write a report.

    Detached-friendly: everything the run prints (including the children's
    output) goes to `$REPORT` exactly as `exec > "$REPORT" 2>&1` did, so a `tail
    -f` on it still works. The PASS/RED summary is additionally printed on the
    real stdout — the bash's copy went into the report, where the operator
    watching the detached run never saw it.
    """
    repo_dir = Path(repo or os.environ.get("RIVET_REPO") or Path.home() / "rivet").expanduser()
    if not repo_dir.is_dir():
        raise shell.Fail(
            f"repo not found at {repo_dir} — refusing to run from the wrong directory",
            code=2,
            hint="clone the repo there or pass --repo <dir>",
        )
    golden = repo_dir / "dev/parallel_keyset/golden"
    if not golden.is_dir():
        raise shell.Fail(f"golden fixtures not found at {golden}", code=2)

    binary = _resolve_rivet(
        rivet_bin or os.environ.get("RIVET_BIN") or Path.home() / "rivet-golden/rivet"
    )
    shell.require("duckdb", hint="verify() re-reads the parquet with duckdb")

    report_path = Path(
        report or os.environ.get("REPORT") or Path.home() / "rivet-golden/report.txt"
    ).expanduser()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    saved_out, saved_err = os.dup(1), os.dup(2)
    handle = open(report_path, "w")
    try:
        sys.stdout.flush()
        sys.stderr.flush()
        os.dup2(handle.fileno(), 1)
        os.dup2(handle.fileno(), 2)
        # fd 1 is a FILE now, so Python would block-buffer and interleave badly
        # with the children writing to the same fd directly — the report would
        # read as if every phase line came after the output it introduces.
        line_buffered = getattr(sys.stdout, "line_buffering", False)
        for stream in (sys.stdout, sys.stderr):
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(line_buffering=True)
        try:
            failures = _devbox_body(golden, binary)
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            for stream in (sys.stdout, sys.stderr):
                if hasattr(stream, "reconfigure"):
                    stream.reconfigure(line_buffering=line_buffered)
    finally:
        os.dup2(saved_out, 1)
        os.dup2(saved_err, 2)
        os.close(saved_out)
        os.close(saved_err)
        handle.close()

    verdicts = [
        line
        for line in report_path.read_text(errors="replace").splitlines()
        if _VERDICT.search(line)
    ]
    with report_path.open("a") as fh:  # the bash appended the grep to the report
        for line in verdicts:
            fh.write(line + "\n")
    for line in verdicts:
        _say(line)
    _say(f"(report: {report_path})")

    if not verdicts:
        raise shell.Fail("no GOLDEN verdict was produced — see the report")
    if failures:
        raise shell.Fail(f"{failures} of 4 golden runs did not PASS")
    return 0


# ── dev/parallel_keyset/fixture.sh ─────────────────────────────────────────────
PK_CONTAINER = "pk-mysql"
PK_PORT = 3310
PK_IMAGE = "mysql:8.4"
PK_SQL = """DROP TABLE IF EXISTS big;
CREATE TABLE big (id BIGINT PRIMARY KEY, payload VARCHAR(120) NOT NULL, n INT NOT NULL) ENGINE=InnoDB;
INSERT INTO big (id, payload, n)
WITH RECURSIVE nums(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM nums WHERE n < 999)
SELECT
  CASE WHEN rn <= 950000 THEN rn ELSE 1000000 + (rn-950000)*10000 END AS id,
  CONCAT('row-', rn, '-', REPEAT('x',80)), rn
FROM (SELECT a.n*1000 + b.n + 1 AS rn FROM nums a CROSS JOIN nums b) t
WHERE rn BETWEEN 1 AND 1000000;
SELECT COUNT(*) AS row_cnt, MIN(id) AS min_id, MAX(id) AS max_id,
       ROUND((MAX(id)-MIN(id))/COUNT(*),1) AS span_per_row FROM big;
"""


def fixture(*, container: str = PK_CONTAINER, port: int = PK_PORT) -> int:
    """Skewed-key MySQL bench fixture for the parallel-keyset probe.

    1M rows, `id BIGINT` PK, 95% dense in [1..950k], 5% sparse out to ~501M
    (`span_per_row ≈ 501`) — the sparse footgun a value-split blows up on. The
    container is left RUNNING on purpose: it is the fixture the probe connects to.

        cargo run --release --example parallel_keyset_probe -- \\
          "mysql://rivet:rivet@127.0.0.1:3310/bench" big id 4 10000
    """
    shell.require("docker")
    shell.docker("rm", "-f", container)  # `|| true` in the bash: absence is fine
    shell.docker(
        "run", "-d", "--name", container,
        "-e", "MYSQL_ROOT_PASSWORD=rivet", "-e", "MYSQL_DATABASE=bench",
        "-e", "MYSQL_USER=rivet", "-e", "MYSQL_PASSWORD=rivet",
        "-p", f"{port}:3306", PK_IMAGE,
        timeout=None,
    ).check(f"docker run {PK_IMAGE}")

    def ready() -> bool:
        return shell.docker_exec(
            container, "mysql", "-urivet", "-privet", "bench", "-e", "SELECT 1", timeout=60
        ).ok

    if not shell.wait_until(ready, tries=45, delay=3.0, what="mysql ready"):
        # The bash's loop simply ended and the seed then failed with the client's
        # error, never mentioning that the wait had run out.
        raise shell.Fail(
            f"{container} did not accept connections within 135s",
            hint=f"docker logs {container}",
        )

    seeded = shell.docker_exec(
        container, "mysql", "-urivet", "-privet", "bench", stdin=PK_SQL, timeout=None
    )
    sys.stdout.write(seeded.stdout)
    sys.stdout.flush()
    seeded.check("seed table `big`")
    _say(f"fixture ready: mysql://rivet:rivet@127.0.0.1:{port}/bench table=big")
    return 0


# ══ docs/recipes/airflow/init/ (RECIPE ASSETS) ═════════════════════════════════
# `init-state-schema.sh`: create the rivet state schema (version + tables) in
# every state database up front, so the parallel wave tasks never race to create
# it on a fresh DB. `rivet state show` connects to the state DB and runs the
# migration; the source URL is only needed so the config parses (state show does
# not query the source).
AIRFLOW_STATE_DBS: tuple[tuple[str, str], ...] = (
    ("rivet_state_postgres", "postgres.yaml"),
    ("rivet_state_mysql", "mysql.yaml"),
    ("rivet_state_mssql", "mssql.yaml"),
    ("rivet_state_postgres_s3", "postgres.s3.yaml"),
    ("rivet_state_mysql_s3", "mysql.s3.yaml"),
    ("rivet_state_mssql_s3", "mssql.s3.yaml"),
    ("rivet_state_postgres_gcs", "postgres.gcs.yaml"),
)


def airflow_init_state_schema(
    *,
    dags: str | Path = "/opt/airflow/dags",
    state_dsn: str = "postgresql://rivet:rivet@rivet-state-db:5432/{db}?sslmode=require",
    source_host: str = "host.docker.internal",
) -> int:
    """Warm the state schema in every state DB (recipe asset — see the module doc).

    Best-effort per database, and exit 0 either way so the recipe's init
    container still lets the stack come up — but each warm now REPORTS what
    happened. The bash's `|| true` with stderr to `/dev/null` printed
    "state schema ready" seven times whether or not a single migration had run,
    which defeats the purpose: the race it exists to prevent was still there.
    """
    shell.require("rivet", hint="this runs inside the rivet-airflow image, which ships the binary")
    base_env = {
        "RIVET_PG_URL": f"postgresql://rivet:rivet@{source_host}:5432/rivet",
        "RIVET_MY_URL": f"mysql://rivet:rivet@{source_host}:3306/rivet",
        "RIVET_MS_URL": f"sqlserver://sa:{cdc_stand.SA_PASSWORD}@{source_host}:1433/rivet",
    }

    failed: list[str] = []
    for db, cfg in AIRFLOW_STATE_DBS:
        p = shell.run(
            ["rivet", "state", "show", "--config", f"{Path(dags)}/{cfg}"],
            env={**base_env, "RIVET_STATE_URL": state_dsn.format(db=db)},
            timeout=None,
        )
        if p.ok:
            _say(f"  state schema ready: {db}")
        else:
            detail = (p.stderr or p.stdout).strip().splitlines()
            failed.append(db)
            shell.warn(
                f"  state schema NOT ready: {db} (exit {p.returncode}) "
                f"{detail[-1] if detail else ''}"
            )

    if failed:
        # Loud, but not fatal: the wave tasks can still create the schema
        # themselves — they just have to race for it, which is the cost the
        # warm-up was meant to remove.
        shell.bad(
            f"state schema init INCOMPLETE: {len(failed)} of {len(AIRFLOW_STATE_DBS)} "
            f"failed ({', '.join(failed)})"
        )
        return 0
    _say("all state schemas initialised")
    return 0


def _http_ok(url: str, *, timeout: float = 5.0) -> bool:
    """A 2xx, mirroring `wget -q -O- <url>` — NOT `shell.http_up`, which counts a
    4xx as "up". The probe here is the emulator's bucket LISTING; a 403 would mean
    it is answering but not usable, and looping on that is the honest behaviour."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return int(getattr(r, "status", 200) or 200) < 300
    except (urllib.error.URLError, OSError):
        return False


def airflow_create_buckets(
    *,
    minio_alias_url: str = "http://minio:9000",
    minio_bucket: str = "rivet-lake",
    gcs_url: str = "http://fake-gcs:4443",
    gcs_bucket: str = "rivet-lake-gcs",
    tries: int = 60,
    delay: float = 2.0,
) -> int:
    """Create the recipe's MinIO + fake-GCS buckets (recipe asset — module doc).

    Both waits are BOUNDED. The bash's `until … ; do sleep 2; done` pairs never
    gave up, so a misconfigured store left the `restart: "no"` init container
    running forever and the DAGs failing later for an unrelated-looking reason.
    """
    shell.require("mc", hint="this runs in the minio/mc image; on a host: brew install minio-mc")

    def alias_ok() -> bool:
        if shell.run(
            ["mc", "alias", "set", "local", minio_alias_url, "minioadmin", "minioadmin"]
        ).ok:
            return True
        _say("waiting for minio…")
        return False

    if not shell.wait_until(alias_ok, tries=tries, delay=delay, what="minio"):
        raise shell.Fail(
            f"minio never answered at {minio_alias_url} (waited {int(tries * delay)}s)",
            hint="docker compose -f docs/recipes/airflow/docker-compose.2.10.yaml up -d minio",
        )

    # `mc mb -p` is already idempotent (-p = --ignore-existing), so the bash's
    # `|| true` masked nothing but REAL errors — a permission failure printed
    # "bucket ready" and the DAGs failed on first write.
    made = shell.run(["mc", "mb", "-p", f"local/{minio_bucket}"])
    if not made.ok:
        detail = (made.stderr or made.stdout).strip().splitlines()
        raise shell.Fail(
            f"could not create MinIO bucket {minio_bucket}: "
            f"{detail[-1] if detail else made.returncode}"
        )
    _say(f"MinIO bucket {minio_bucket} ready")

    def gcs_ok() -> bool:
        if _http_ok(f"{gcs_url.rstrip('/')}/storage/v1/b"):
            return True
        _say("waiting for fake-gcs…")
        return False

    if not shell.wait_until(gcs_ok, tries=tries, delay=delay, what="fake-gcs"):
        raise shell.Fail(
            f"fake-gcs never answered at {gcs_url} (waited {int(tries * delay)}s)"
        )

    req = urllib.request.Request(
        f"{gcs_url.rstrip('/')}/storage/v1/b",
        data=json.dumps({"name": gcs_bucket}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=10).read(0)
    except urllib.error.HTTPError as e:
        if e.code != 409:  # 409 = already exists, the only benign failure
            raise shell.Fail(
                f"could not create fake-gcs bucket {gcs_bucket}: HTTP {e.code}"
            )
    except (urllib.error.URLError, OSError) as e:
        raise shell.Fail(f"could not create fake-gcs bucket {gcs_bucket}: {e}")
    _say(f"fake-gcs bucket {gcs_bucket} ready")
    return 0


# ══ CLI ════════════════════════════════════════════════════════════════════════
def _parse_verify_args(args: Sequence[str]) -> tuple[list[str], dict[str, object]]:
    positional: list[str] = []
    kw: dict[str, object] = {}
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--keep":
            kw["keep"] = True
            i += 1
        elif a == "--bin" and i + 1 < len(args):
            kw["rivet_bin"] = args[i + 1]
            i += 2
        elif a.startswith("-"):
            raise shell.Fail(f"verify: unknown option {a}", code=2, hint=USAGE)
        else:
            positional.append(a)
            i += 1
    return positional, kw


def _parse_devbox_args(args: Sequence[str]) -> dict[str, object]:
    kw: dict[str, object] = {}
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--report" and i + 1 < len(args):
            kw["report"] = args[i + 1]; i += 2
        elif a == "--bin" and i + 1 < len(args):
            kw["rivet_bin"] = args[i + 1]; i += 2
        elif a == "--repo" and i + 1 < len(args):
            kw["repo"] = args[i + 1]; i += 2
        else:
            raise shell.Fail(f"devbox-run: unknown option {a}", code=2, hint=USAGE)
    return kw


def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    cmd = args[0] if args else ""
    rest = args[1:]

    if cmd == "verify":
        pos, kw = _parse_verify_args(rest)
        if len(pos) < 2:
            raise shell.Fail("usage: verify <engine> <url> [table] [rows] [files]", code=2)
        engine, url = pos[0], pos[1]
        table = pos[2] if len(pos) > 2 else GOLDEN_TABLE
        rows = int(pos[3]) if len(pos) > 3 else GOLDEN_ROWS
        files = int(pos[4]) if len(pos) > 4 else GOLDEN_FILES
        return verify(engine, url, table, rows, files, **kw)  # type: ignore[arg-type]
    if cmd in ("devbox-run", "devbox_run", "devbox"):
        return devbox_run(**_parse_devbox_args(rest))  # type: ignore[arg-type]
    if cmd == "fixture":
        return fixture()
    if cmd in ("airflow-init-state-schema", "init-state-schema"):
        return airflow_init_state_schema()
    if cmd in ("airflow-create-buckets", "create-buckets"):
        return airflow_create_buckets()

    _say(USAGE)
    return 1


if __name__ == "__main__":
    shell.main(lambda: main_cli())
