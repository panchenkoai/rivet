"""Batch scenarios of the release oracle — the Python port of
`dev/release-oracle/lib/scenarios.sh`.

One function per scenario, each taking the `Ledger` plus (engine, tag, url) and
recording PASS/FAIL/SKIP. The discipline the bash encoded is preserved verbatim:

* every check is PASS only when it RAN and MATCHED — a down emulator, an absent
  client or a missing credential is SKIP, never a silent pass;
* the completeness oracle is INDEPENDENT of rivet. `store_readback` counts what
  the object store actually holds using the STORE's own client plus DuckDB, so a
  rivet read bug cannot rubber-stamp a rivet write bug. `rivet validate` is
  deliberately not used for that job;
* the type/fidelity matrices are compared to a checked-in GOLDEN, so a
  regression fails against a fixed truth rather than a hand-picked assertion.

Not ported here, because they live in a sibling shell file and belong to that
file's port: `start_stores` (run.sh — it owns the compose bring-up and `WORK`)
and `verify_scale_memory` (lib/regression.sh). `run_bigquery_golden`,
`verify_cdc_e2e`, `verify_release_build_path` and `verify_release_regression`
are likewise separate modules; the bash `source`d them from the bottom of
scenarios.sh purely as a loader, which a Python driver does with imports.

A note on the bug class this rewrite retires. The bash needed comments like
"# own line — bash 3.2 same-line ${eng} gotcha" on five separate declarations:
`local store=$1 ... dl="…${store}…"` on ONE line makes macOS bash 3.2 expand
`${store}` against the ENCLOSING scope, so it silently took the CALLER's value
where one existed (the batch load path has a `store` local) and, under `set -u`,
ABORTED the function where none did (the CDC path) — which is how the CDC layer
of the gate reported `independent-readback[!=5]` for every engine and could
never pass. In Python a parameter is bound before the body runs and cannot be
read from an enclosing frame, so the class is structurally impossible and the
per-site reminders are gone.
"""

from __future__ import annotations

import json
import os
import random
import re
import shutil
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from tempfile import mkdtemp

try:  # importable both as a package module and as a plain sibling file
    from .core import HERE, ROOT, Ledger, Status, docker, docker_exec, have, rivet, rivet_bin, run
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import (  # type: ignore
        HERE,
        ROOT,
        Ledger,
        Status,
        docker,
        docker_exec,
        have,
        rivet,
        rivet_bin,
        run,
    )

__all__ = [
    "run_scenarios",
    "store_dest",
    "store_readback",
    "store_up",
    "verify_coverage_matrices",
    "verify_pool_e2e",
    "verify_replica_read",
    "verify_state_migrations",
]

# The helper scripts and blessed goldens are DATA, shared with the bash gate: the
# goldens are the checked-in truth both implementations must assert against, and
# lib/*.py are dependency-free scripts this port keeps shelling out to rather than
# reimplementing (a second copy of a parser is a second thing to drift). Prefer a
# copy next to this file if the assets ever move here.
_BASH_ORACLE = ROOT / "dev" / "release-oracle"


def _asset(rel: str) -> Path:
    local = HERE / rel
    return local if local.exists() else _BASH_ORACLE / rel


MATRIX = _asset("matrix.yaml")
GOLDEN_VERDICTS = _asset("golden/verdicts.json")
GOLDEN_DUCKDB = _asset("golden/duckdb_type_matrix.json")

# Store credentials the generated configs reference by *_env name, so the rivet
# child process must see them in its environment. AZURITE_CONN is the az CLI's
# own form; run.sh keeps it as a shell global (never exported), hence a constant
# here rather than a lookup with no default.
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
AZURITE_CONN = os.environ.get(
    "AZURITE_CONN",
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    f"AccountKey={AZURITE_KEY};"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
)

# The interpreter that runs the lib/*.py helpers. The bash said `python3`; using
# the one already running the gate avoids a PATH surprise (run.sh has a preflight
# for exactly that class), and the helpers are stdlib-only so either works.
PY = sys.executable or "python3"

# cargo builds + full-table exports are unbounded in the bash (no `timeout`), and
# a gate that kills its own 5-minute cargo build reports a phantom FAIL. Keep
# them unbounded; the short probes keep `run`'s default.
NO_TIMEOUT: float | None = None


# ── this run's scratch dir ────────────────────────────────────────────────────
# `$WORK` in the bash — a mktemp dir owned by the driver. Kept as module state so
# the scenario signatures stay (led, engine, tag, url); the driver may hand its
# own in, and the basename is load-bearing (it is the run-unique token in every
# destination prefix).
_WORK: Path | None = None


def set_work_dir(path: Path) -> None:
    global _WORK
    _WORK = Path(path)
    _WORK.mkdir(parents=True, exist_ok=True)


def work_dir() -> Path:
    global _WORK
    if _WORK is None:
        _WORK = Path(os.environ.get("RIVET_ORACLE_WORK") or mkdtemp())
        _WORK.mkdir(parents=True, exist_ok=True)
    return _WORK


# ── ledger rows ──────────────────────────────────────────────────────────────
# Print AND record in one call (the property `Ledger.passed/failed/skipped`
# exists for — a ✓ without a row, or a row without a ✓, is how the bash's printed
# table and exit code could disagree). These wrappers differ from the Ledger's
# combined helpers in ONE respect: `detail` is used exactly as given, including
# empty, because several bash rows carry no detail and the final table must stay
# byte-identical between the two implementations.
def _passed(led: Ledger, eng: str, ver: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
    led.ok(msg)
    led.add(eng, ver, scenario, store, Status.PASS, detail)


def _failed(led: Ledger, eng: str, ver: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
    led.bad(msg)
    led.add(eng, ver, scenario, store, Status.FAIL, detail)


def _skipped(led: Ledger, eng: str, ver: str, scenario: str, store: str, msg: str, detail: str = "") -> None:
    led.skip(msg)
    led.add(eng, ver, scenario, store, Status.SKIP, detail)


# ── small shared helpers ─────────────────────────────────────────────────────
def cfg(*query: str) -> str:
    """Query matrix.yaml through lib/cfg.py — one source of truth, no PyYAML (a
    release gate must run on a bare python3)."""
    return run([PY, str(_asset("lib/cfg.py")), str(MATRIX), *query]).stdout.strip()


def _bless(flag: str) -> bool:
    return os.environ.get(flag, "0") == "1"


def _tag(tag: str) -> str:
    return tag.replace(".", "_")


def _tcp_open(host: str, port: int, timeout: float = 3.0) -> bool:
    """The `exec 3<>/dev/tcp/host/port` probe: can we connect at all."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _http_reachable(url: str, timeout: float = 3.0) -> bool:
    """`curl -s -o /dev/null --max-time 3 <url>`.

    curl without `-f` exits 0 on ANY completed transfer, so an HTTP 400/403/404
    counts as "the service answered" — which is the point for the Azurite probe
    (`?comp=list` unauthenticated is a 4xx) and for fake-gcs. Only a refused
    connection / timeout means down, so HTTPError is UP and URLError is DOWN.
    """
    try:
        urllib.request.urlopen(url, timeout=timeout).close()
        return True
    except urllib.error.HTTPError:
        return True
    except (urllib.error.URLError, OSError):
        return False


def _first_match(text: str, pattern: str) -> str:
    """`grep -aiE <pattern> | head -1` — the first matching LINE, or ""."""
    rx = re.compile(pattern, re.IGNORECASE)
    for line in text.splitlines():
        if rx.search(line):
            return line
    return ""


def _duckdb_list(sql: str) -> str:
    """`duckdb -noheader -list -c SQL`, stdout only.

    An absent duckdb (exit 127) or a query error leaves stdout empty, which every
    caller already treats as "no answer" — the same way the bash did with
    `2>/dev/null`. The exit status is deliberately not consulted so the two
    implementations classify identically.
    """
    return run(["duckdb", "-noheader", "-list", "-c", sql]).stdout.strip()


def _duckdb_json_normalized(sql: str) -> str:
    """`duckdb -json -c SQL | lib/normalize_bq.py` — the canonical golden form.

    normalize_bq.py sorts rows by `id` and sorts keys, so a golden diff means a
    real change in rivet's type export, never row/key-order noise.
    """
    raw = run(["duckdb", "-json", "-c", sql]).stdout
    if not raw.strip():
        return ""
    return run([PY, str(_asset("lib/normalize_bq.py"))], stdin=raw).stdout.strip()


def _json_canon(text: str) -> str:
    try:
        return json.dumps(json.loads(text), sort_keys=True)
    except (ValueError, TypeError):
        return ""


def _golden_load(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _golden_get(path: Path, engine: str, key: str) -> str:
    """The blessed value as canonical JSON, or "" when it was never blessed."""
    val = _golden_load(path).get(engine, {}).get(key)
    return "" if val is None else json.dumps(val, sort_keys=True)


def _golden_put(path: Path, engine: str, key: str, got: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    golden = _golden_load(path)
    golden.setdefault(engine, {})[key] = json.loads(got)
    path.write_text(json.dumps(golden, indent=2, sort_keys=True) + "\n")


def _engine_container(engine: str) -> str | None:
    """The running container for `engine`, ANY pinned version.

    Found by name prefix rather than built from (engine, tag) because the callers
    that need it do not carry the tag. Returns None instead of "" so no caller
    can hand an empty name to `docker exec` ("invalid container name or ID").
    """
    for name in docker_names():
        if name.startswith(f"rivet-oracle-eng-{engine}-"):
            return name
    return None


def docker_names() -> list[str]:
    return docker("ps", "--format", "{{.Names}}").stdout.split()


# ── object stores ────────────────────────────────────────────────────────────
def store_up(store: str) -> bool:
    """Health probe per store. A down emulator makes the cell SKIP, never PASS."""
    if store == "s3":
        return _http_reachable("http://127.0.0.1:9000/minio/health/live")
    if store == "gcs":
        return _http_reachable("http://127.0.0.1:4443/storage/v1/b")
    if store == "azure":
        return _http_reachable("http://127.0.0.1:10000/devstoreaccount1?comp=list")
    # Unreachable with matrix.yaml's three stores. The bash `case` fell through
    # to exit 0 here — i.e. an unknown store read as UP without probing anything,
    # the exact silent-pass shape this gate forbids. False is the honest answer.
    return False


def store_readback(store: str, bucket: str, prefix: str, work: Path) -> str:
    """Count the rows the store ACTUALLY holds, via the store's OWN client plus
    DuckDB — never through rivet.

    That separation is the whole point of the load gate: `rivet validate` re-reads
    the parts with the same code that wrote them, so a symmetric read/write bug
    passes its own inspection. MinIO is read straight over S3 by DuckDB's httpfs,
    fake-gcs is pulled through its JSON API by lib/gcs_pull.py, Azurite through
    the `az` CLI. An empty return means the client is absent — the caller records
    SKIP ("no readback tool"), not a pass and not a failure.

    (The bash form of this function is where the `local store=$1 … dl="${store}"`
    same-line gotcha lived: bash 3.2 resolved `${store}` in the enclosing scope,
    so it silently used the caller's value in the batch path and aborted under
    `set -u` in the CDC path. A Python parameter cannot be shadowed that way.)
    """
    dl = work / f"dl_{store}_{random.randint(0, 32767)}"
    if store == "s3":
        return _duckdb_list(
            "INSTALL httpfs; LOAD httpfs; SET s3_endpoint='127.0.0.1:9000'; "
            "SET s3_use_ssl=false; SET s3_url_style='path'; "
            "SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin'; "
            f"SELECT count(*) FROM read_parquet('s3://{bucket}/{prefix}/**/*.parquet')"
        )
    if store == "gcs":
        # No gsutil needed — the fake-gcs JSON API is enough, and it keeps the
        # readback independent of rivet.
        dl.mkdir(parents=True, exist_ok=True)
        got = run(
            [PY, str(_asset("lib/gcs_pull.py")), "http://127.0.0.1:4443", bucket, prefix, str(dl)]
        ).stdout.strip()
        try:
            pulled = int(got)
        except ValueError:
            pulled = 0
        if pulled <= 0:
            return ""
        return _duckdb_list(f"SELECT count(*) FROM read_parquet('{dl}/**/*.parquet')")
    if store == "azure":
        if not have("az"):
            return ""
        dl.mkdir(parents=True, exist_ok=True)
        run(
            [
                "az", "storage", "blob", "download-batch",
                "--connection-string", AZURITE_CONN,
                "-s", bucket,
                "--pattern", f"{prefix}/*",
                "-d", str(dl),
            ]
        )
        return _duckdb_list(f"SELECT count(*) FROM read_parquet('{dl}/**/*.parquet')")
    return ""


def store_dest(store: str, bucket: str, prefix: str) -> str | None:
    """The `destination:` YAML block for one store (already indented under it).

    None for an unknown store — the caller records SKIP "no dest config". No
    trailing newline, matching the `$(…)` the bash interpolated into a heredoc.
    """
    if store == "s3":
        return (
            "      type: s3\n"
            f"      bucket: {bucket}\n"
            f"      prefix: {prefix}/\n"
            "      region: us-east-1\n"
            "      endpoint: http://127.0.0.1:9000\n"
            "      access_key_env: MINIO_ACCESS_KEY\n"
            "      secret_key_env: MINIO_SECRET_KEY"
        )
    if store == "gcs":
        return (
            "      type: gcs\n"
            f"      bucket: {bucket}\n"
            f"      prefix: {prefix}/\n"
            "      endpoint: http://127.0.0.1:4443"
        )
    if store == "azure":
        return (
            "      type: azure\n"
            f"      bucket: {bucket}\n"
            f"      prefix: {prefix}/\n"
            "      account_name: devstoreaccount1\n"
            "      account_key_env: AZURITE_KEY\n"
            "      endpoint: http://127.0.0.1:10000/devstoreaccount1"
        )
    return None


def _store_env(url: str) -> dict[str, str]:
    """What a store-bound `rivet run` needs in its environment: the source URL
    plus the credentials the config references by *_env name."""
    return {
        "ORACLE_URL": url,
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
        "AZURITE_KEY": AZURITE_KEY,
    }


# ── source-side oracles ──────────────────────────────────────────────────────
def _source_count_distinct(engine: str, url: str, table: str, id_col: str) -> str:
    """`"<count> <distinct>"` straight from the source engine's own client.

    `url` is unused (as in the bash): the container is found by name, and each
    engine's in-container client already knows its credentials. Empty when the
    container is gone — no `docker exec ""`.
    """
    del url
    container = _engine_container(engine)
    if container is None:
        return ""
    if engine == "postgres":
        return docker_exec(
            container, "psql", "-U", "rivet", "-d", "rivet", "-tA",
            "-c", f"SELECT count(*)||' '||count(DISTINCT {id_col}) FROM {table}",
        ).stdout.strip()
    if engine == "mysql":
        return docker_exec(
            container, "mysql", "-urivet", "-privet", "rivet", "-N",
            "-e", f"SELECT CONCAT(count(*),' ',count(DISTINCT {id_col})) FROM {table}",
        ).stdout.strip()
    if engine == "mssql":
        out = docker_exec(
            container, "/opt/mssql-tools18/bin/sqlcmd",
            "-S", "localhost", "-U", "sa", "-P", "Rivet_Passw0rd!", "-d", "rivet",
            "-C", "-h", "-1", "-W",
            "-Q", f"SET NOCOUNT ON; SELECT CONCAT(count(*),' ',count(DISTINCT {id_col})) FROM {table}",
        ).stdout
        return out.replace("\r", "").strip()
    if engine == "mongo":
        # mongo:4.4 ships the legacy `mongo` shell, 5.0+ ships `mongosh`. Pick
        # whichever the container actually has, or the count comes back as an OCI
        # "executable not found" string and the gate false-fails on good data.
        shell = mongo_shell(container)
        # countDocuments({}) NOT countDocuments() — the legacy 4.4 shell rejects
        # the no-arg form ("match filter must be an expression in an object").
        return docker_exec(
            container, shell, "mongodb://127.0.0.1:27017/rivet", "--quiet",
            "--eval",
            f"print(db.{table}.countDocuments({{}})+' '+db.{table}.distinct('_id').length)",
        ).stdout.strip()
    return ""


def mongo_shell(container: str) -> str:
    """`mongosh`, or the legacy `mongo` on an image that predates it.

    Mongo 4.4 ships only the old shell — `mongosh` became standard in 5.0. A
    caller that assumes the new name gets "executable not found" and, if it
    parses the empty output as a count, a silent -1 that reads as "the source is
    unreachable". That is what happened to `blessed_path._source_rows`: every
    mongo 4.4 oracle cell SKIPped with `source=-1 duckdb=150000` while 5, 6, 7
    and 8 passed, so the one version most likely to expose an old-shell
    incompatibility was the one version not being compared.

    Extracted here because the knowledge already existed in this file and a
    second caller had gone without it; a third would too.
    """
    if docker_exec(container, "sh", "-c", "command -v mongosh >/dev/null 2>&1").ok:
        return "mongosh"
    return "mongo"


def _fidelity_check(engine: str, key: str, got: str) -> bool:
    """Bless-or-compare ONE golden argument against the DuckDB golden.

    Bless mode records and succeeds. Compare mode is true only when it EQUALS the
    blessed value — a missing golden is a failure (bless first), never a pass on
    an absent expectation.
    """
    if _bless("BLESS_DUCKDB"):
        _golden_put(GOLDEN_DUCKDB, engine, key, got)
        return True
    want = _golden_get(GOLDEN_DUCKDB, engine, key)
    return bool(want) and _json_canon(got) == want


# ── config generation ────────────────────────────────────────────────────────
def _init_cfg(engine: str, url: str, schema: str) -> Path | None:
    """`rivet init` a config for the whole DB (or one --schema), returning its path.

    The URL goes in as ORACLE_URL on the child's environment — the generated
    configs carry `url_env: ORACLE_URL`. Passing it per call is what the bash
    could not do: its `export` ran inside a `$()` subshell, so the value never
    reached the shell running `rivet check`, which then connected to whatever
    stale URL a previous engine had left behind (a 0-table verdict map for the
    first version of every engine).

    `rivet init` has NO tls flag — it connects to SQL Server with cert validation
    disabled by default (emitting a warn), so a flag here is neither needed nor
    accepted; passing one aborts init with a usage error and yields an empty
    (0-table) verdict map.
    """
    out = work_dir() / f"{engine}_{schema or 'all'}.yaml"
    argv = ["init", "--source-env", "ORACLE_URL"]
    if schema:
        argv += ["--schema", schema]
    argv += ["-o", str(out)]
    return out if rivet(*argv, env={"ORACLE_URL": url}, timeout=NO_TIMEOUT).ok else None


def _export_local(
    engine: str,
    url: str,
    table: str,
    dest_dir: Path,
    mode: str,
    fmt: str = "parquet",
    parallel: int | None = None,
) -> bool:
    """Export one table to a LOCAL dir. mode: chunked|full, fmt: parquet|csv."""
    yaml_path = work_dir() / f"ex_{os.getpid()}_{table.replace('.', '_')}_{fmt}.yaml"
    shutil.rmtree(dest_dir, ignore_errors=True)
    if engine == "mongo":
        mode = "full"  # Mongo has no keyset/chunked — full scan only
    key_block = "    mode: full"
    if mode == "chunked":
        key_block = "    mode: chunked\n    chunk_by_key: id\n    chunk_size: 50000"
        # parallel: N fans keyset out into N row-percentile ranges.
        if parallel:
            key_block += f"\n    parallel: {parallel}"
    tls_block = "\n  tls: {accept_invalid_certs: true}" if engine == "mssql" else ""
    yaml_path.write_text(
        f"source:\n"
        f"  type: {engine}\n"
        f"  url_env: ORACLE_URL{tls_block}\n"
        f"exports:\n"
        f"  - name: {table}\n"
        f"    table: {table}\n"
        f"{key_block}\n"
        f"    format: {fmt}\n"
        f"    destination: {{type: local, path: {dest_dir}/}}\n"
    )
    return rivet("run", "-c", str(yaml_path), env={"ORACLE_URL": url}, timeout=NO_TIMEOUT).ok


# ── verdicts: init+check → {table: {strategy, verdict}} == GOLDEN ────────────
def sc_verdicts(led: Ledger, engine: str, tag: str, url: str) -> None:
    """The golden fixes the strategy AND verdict of EVERY seed and garbage table
    per engine, so a divergence (keyset→full regression, a decimal that stops
    bailing, a name-trap that starts keyset'ing) fails the release against a
    checked-in truth. Re-bless on purpose with --bless-verdicts-golden."""
    verdict_map = "{}"
    phantom = 0
    checks: list[Path] = []
    if engine == "mongo":
        c = _init_cfg("mongo", url, "")
        if c is None:
            _skipped(led, "mongo", tag, "verdicts", "-", "mongo init failed", "init")
            return
        checks = [c]
    else:
        # The seeds' schema is per engine: PG `public`, MSSQL `dbo`, MySQL none
        # (the DB comes from the URL). init WITHOUT --schema on PG scaffolds
        # nothing usable → an empty check.
        schema = {"postgres": "public", "mssql": "dbo"}.get(engine, "")
        sc = _init_cfg(engine, url, schema)
        if sc is None:
            _skipped(led, engine, tag, "verdicts", "-", f"{engine} init failed", "init")
            return
        checks = [sc]
        # pg/mssql keep the garbage tables in schema `ext` (a separate init);
        # mysql has them in the same DB.
        if engine != "mysql":
            gc = _init_cfg(engine, url, "ext")
            if gc is not None:
                checks.append(gc)

    for cfg_path in checks:
        # `rivet check` writes to stderr — capture both streams.
        out = rivet("check", "-c", str(cfg_path), env={"ORACLE_URL": url}, timeout=NO_TIMEOUT).out
        phantom += sum(1 for line in out.splitlines() if "Heavy chunk" in line)
        verdict_map = run(
            [PY, str(_asset("lib/parse_verdicts.py")), verdict_map], stdin=out
        ).stdout.strip()

    if _bless("BLESS_VERDICTS"):
        # Never bless an EMPTY map — a failed connect must fail LOUD, not quietly
        # write a 0-table golden that asserts nothing on the next release.
        if verdict_map in ("", "{}"):
            _failed(
                led, engine, tag, "verdicts", "-",
                f"verdicts[{engine}]: empty map — refusing to bless (check could not connect?)",
                "empty map",
            )
            return
        GOLDEN_VERDICTS.parent.mkdir(parents=True, exist_ok=True)
        golden = _golden_load(GOLDEN_VERDICTS)
        golden[engine] = json.loads(verdict_map)
        GOLDEN_VERDICTS.write_text(json.dumps(golden, indent=2, sort_keys=True) + "\n")
        _passed(led, engine, tag, "verdicts", "-", f"blessed verdicts[{engine}]", "blessed")
        return

    # Zero phantom heavy-chunk warnings — a cross-check the strategy/verdict
    # golden cannot hold.
    if phantom != 0:
        _failed(
            led, engine, tag, "verdicts", "-",
            f"verdicts: {phantom} phantom heavy-chunk warning(s)",
            f"phantom-heavy-chunk={phantom}",
        )
        return
    want = json.dumps(_golden_load(GOLDEN_VERDICTS).get(engine, {}), sort_keys=True)
    got = _json_canon(verdict_map)
    if want in ("", "{}"):
        _skipped(led, engine, tag, "verdicts", "-", f"no golden for {engine} (bless first)", "no golden")
        return
    if want == got:
        _passed(led, engine, tag, "verdicts", "-", "verdicts match golden (0 phantom)")
    else:
        diff = run([PY, str(_asset("lib/verdict_diff.py")), want, got]).stdout.strip()
        _failed(led, engine, tag, "verdicts", "-", f"verdicts DIVERGED: {diff}", diff)


# ── integrity + types + fidelity ─────────────────────────────────────────────
def sc_integrity_types(led: Ledger, engine: str, tag: str, url: str) -> None:
    """Two independent oracles.

    (1) 150K `users` loss/dup — the source's own count+distinct against DuckDB's
        read of the exported parts.
    (2) The type + fidelity matrix, compared to a DuckDB golden per engine with
        TWO arguments that DuckDB (never rivet) reads back:
          <tmt>       PARQUET readback — the binary/typed path (decimal
                      precision, uuid nulling, timestamp shift, enum).
          <tmt>__csv  CSV readback, read ALL-VARCHAR so the compare is on the
                      exact TEXT the writer emitted (escape/quote/unicode/null
                      fidelity) — the text-writer class the binary parquet path
                      never exercises.
    """
    fails = ""
    out = work_dir() / f"it_{engine}_{_tag(tag)}"
    out.mkdir(parents=True, exist_ok=True)

    # (1) users loss/dup — all engines.
    if _export_local(engine, url, "users", out / "users", "chunked"):
        scnt = _source_count_distinct(engine, url, "users", "id")
        id_col = "_id" if engine == "mongo" else "id"
        dcnt = _duckdb_list(
            f"SELECT count(*)||' '||count(DISTINCT {id_col}) "
            f"FROM read_parquet('{out}/users/**/*.parquet')"
        )
        if scnt != dcnt:
            fails += f"users src[{scnt}]!=parquet[{dcnt}] "
    else:
        fails += "users-export-failed "

    # (2) the type + fidelity matrix (SQL engines only — Mongo has no
    #     rivet_type_matrix). ONE comprehensive matrix per engine now: the
    #     canonical full type set, a port of
    #     tests/type_roundtrip/fixtures/<eng>_schema.sql.
    if engine != "mongo":
        for tmt in ("rivet_type_matrix",):
            if _export_local(engine, url, tmt, out / tmt, "full"):
                got = _duckdb_json_normalized(
                    f"SELECT * FROM read_parquet('{out}/{tmt}/**/*.parquet') ORDER BY id"
                )
                if not got:
                    fails += f"{tmt}-readback "
                elif not _fidelity_check(engine, tmt, got):
                    fails += f"{tmt}-TYPE-DIVERGED "
            else:
                fails += f"{tmt}-export "

            # CSV fidelity. rivet REFUSES a nested/array column in CSV (PG text[]
            # → "CSV cannot serialize"), and that refusal is itself a fidelity
            # guarantee: no silent lossy array→CSV. So record the serialized text
            # when it serializes and a refusal SENTINEL when it does not — either
            # way a fixed truth the compare asserts, and a regression that starts
            # emitting a corrupted array flips it.
            csv_dir = out / f"{tmt}_csv"
            exported = _export_local(engine, url, tmt, csv_dir, "full", "csv")
            # bash 3.2 has no globstar, so its `**/*.csv` was really one level
            # deep — mirrored here rather than an unbounded rglob.
            has_csv = bool(list(csv_dir.glob("*/*.csv")) or list(csv_dir.glob("*.csv")))
            if exported and has_csv:
                gotc = _duckdb_json_normalized(
                    f"SELECT * FROM read_csv('{csv_dir}/**/*.csv', all_varchar=true, header=true) "
                    f"ORDER BY id"
                )
                if not gotc:
                    fails += f"{tmt}-csv-readback "
                elif not _fidelity_check(engine, f"{tmt}__csv", gotc):
                    fails += f"{tmt}-CSV-DIVERGED "
            elif not _fidelity_check(
                engine, f"{tmt}__csv", '{"csv_writer":"refused-unrepresentable-column"}'
            ):
                fails += f"{tmt}-CSV-REFUSAL-CHANGED "

    if _bless("BLESS_DUCKDB"):
        _passed(led, engine, tag, "integrity_types", "-", f"blessed duckdb-types[{engine}]", "blessed")
        return
    if not fails:
        _passed(
            led, engine, tag, "integrity_types", "-",
            "integrity+types (loss/dup 0, type matrices match DuckDB golden)",
        )
    else:
        _failed(led, engine, tag, "integrity_types", "-", f"integrity+types: {fails}", fails)


# ── keyset_parallel: the parallel-keyset fan-out, end-to-end per engine ──────
def sc_keyset_parallel(led: Ledger, engine: str, tag: str, url: str) -> None:
    """The sequential keyset path is already exercised by integrity_types/load;
    this proves the PARALLEL variant on the same 150K users table.

    Two assertions, because either alone is passable by a broken run:
      (1) the same independent DuckDB loss/dup oracle — no row lost or duplicated
          across the N ranges;
      (2) the run actually FANNED OUT — `parallel: 4` must produce >=2 distinct
          `_pk_w{ridx}_` worker parts, else it silently degraded to sequential
          and the headline feature never engaged while every count still matched.
    """
    if engine == "mongo":
        _skipped(
            led, engine, tag, "keyset_parallel", "-",
            "keyset_parallel[mongo]: separate _id-range path (mongo_parallel), not SQL keyset",
            "mongo na",
        )
        return
    out = work_dir() / f"kp_{engine}_{_tag(tag)}"
    out.mkdir(parents=True, exist_ok=True)
    if not _export_local(engine, url, "users", out / "users", "chunked", "parquet", 4):
        _failed(led, engine, tag, "keyset_parallel", "-", f"keyset_parallel[{engine}]: export failed", "export")
        return

    fails = ""
    # (1) loss/dup — the SAME independent oracle integrity_types uses.
    scnt = _source_count_distinct(engine, url, "users", "id")
    dcnt = _duckdb_list(
        f"SELECT count(*)||' '||count(DISTINCT id) FROM read_parquet('{out}/users/**/*.parquet')"
    )
    if scnt != dcnt:
        fails += f"loss/dup src[{scnt}]!=parquet[{dcnt}] "
    # (2) fan-out — distinct `_pk_w{ridx}_` stamps across the part names.
    workers = len(
        {m for p in (out / "users").rglob("*.parquet") for m in re.findall(r"_pk_w[0-9]+_", str(p))}
    )
    if workers < 2:
        fails += f"fan-out workers={workers} (parallel:4 degraded to sequential) "

    if not fails:
        _passed(
            led, engine, tag, "keyset_parallel", "-",
            f"keyset_parallel (loss/dup 0, fan-out {workers} workers)",
            f"{workers} workers",
        )
    else:
        _failed(led, engine, tag, "keyset_parallel", "-", f"keyset_parallel: {fails}", fails)


# ── load: export TO the object store, then read it back INDEPENDENTLY ────────
def sc_load(led: Ledger, engine: str, tag: str, url: str, store: str) -> None:
    if not store_up(store):
        _skipped(led, engine, tag, "load", store, f"{store} emulator down", "emulator down")
        return
    # A run-UNIQUE prefix (this run's mktemp token). Part names are run-unique by
    # design, so they never clobber a prior run's — which means a STABLE prefix
    # would make the readback count EVERY past run's parts (N×150k). A fresh
    # prefix per run isolates this run so the count gate compares 150k to 150k,
    # not an accumulation.
    bucket = cfg("store", store, "bucket")
    prefix = f"oracle/{work_dir().name}/{engine}_{_tag(tag)}/{store}"
    dest = store_dest(store, bucket, prefix)
    if dest is None:
        _skipped(led, engine, tag, "load", store, f"{store}: no dest config", "no dest")
        return
    yaml_path = work_dir() / f"load_{engine}_{_tag(tag)}_{store}.yaml"
    tls_block = "\n  tls: {accept_invalid_certs: true}" if engine == "mssql" else ""
    mode_block = "    mode: chunked\n    chunk_by_key: id\n    chunk_size: 50000"
    if engine == "mongo":
        mode_block = "    mode: full"  # Mongo: full scan only
    yaml_path.write_text(
        f"source:\n"
        f"  type: {engine}\n"
        f"  url_env: ORACLE_URL{tls_block}\n"
        f"exports:\n"
        f"  - name: users\n"
        f"    table: users\n"
        f"{mode_block}\n"
        f"    format: parquet\n"
        f"    destination:\n"
        f"{dest}\n"
    )
    out = rivet("run", "-c", str(yaml_path), env=_store_env(url), timeout=NO_TIMEOUT).out
    # Kept as the bash had it — a transcript grep, not the exit status — so the two
    # implementations classify the same run identically. It is the weaker signal
    # (a data row containing the word "error" trips it); the readback count below
    # is the real oracle.
    if re.search(r"error|failed", out, re.IGNORECASE):
        _failed(
            led, engine, tag, "load", store, f"load→{store} export failed",
            _first_match(out, r"error|fail"),
        )
        return
    n = store_readback(store, bucket, prefix, work_dir())
    scnt = _source_count_distinct(engine, url, "users", "id").split(" ")[0]
    if n and n == scnt:
        _passed(led, engine, tag, "load", store, f"load→{store} gcloud-verified {n} rows", n)
    elif not n:
        _skipped(led, engine, tag, "load", store, f"{store} readback tool unavailable", "no readback tool")
    else:
        _failed(
            led, engine, tag, "load", store,
            f"load->{store} readback {n} != source {scnt}", f"{n}!={scnt}",
        )


# ── gc_survival (store-level) ────────────────────────────────────────────────
def sc_gc_survival(led: Ledger, engine: str, tag: str, url: str) -> None:
    """Spare an in-flight part while a run is active, delete a true orphan.

    Invoking `gc_orphans` needs a warehouse load target, which the BigQuery stage
    has (real load + gc). Recorded here so the ledger is EXPLICIT about where the
    coverage lives rather than silently missing a row.
    """
    del url
    _skipped(
        led, engine, tag, "gc_survival", "-",
        "gc_survival runs in the BigQuery stage (needs a warehouse load target)",
        "covered in BQ stage",
    )


def run_scenarios(led: Ledger, engine: str, tag: str, url: str) -> None:
    """Every batch scenario for one engine×version, in the gate's order."""
    sc_verdicts(led, engine, tag, url)
    sc_integrity_types(led, engine, tag, url)
    # When blessing the local goldens (verdicts + duckdb-type) the store loads add
    # nothing — skip them.
    if _bless("BLESS_VERDICTS") or _bless("BLESS_DUCKDB"):
        return
    sc_keyset_parallel(led, engine, tag, url)
    # The one integrity column a reader is asked to trust, checked by something
    # that is not rivet — see rowhash.py for why the in-tree auditor cannot.
    from . import corruption, rowhash

    rowhash.verify_row_hash(led, engine, tag, url)
    # The NEGATIVE half: does verification fail when the data is wrong?
    corruption.verify_corruption_is_detected(led, engine, tag, url)
    # The CDC sink has its OWN accumulator and manifests — proving the batch leg
    # detects corruption says nothing about this one.
    corruption.verify_cdc_corruption_is_detected(led, engine, tag, url)
    # reconcile and validate check DIFFERENT sides; the pair is the claim.
    corruption.verify_reconcile_and_validate_cover_both_sides(led, engine, tag, url)
    # Is the schema fingerprint sensitive to a real schema change at all?
    from . import schema_drift

    schema_drift.verify_schema_fingerprint_moves_with_the_schema(led, engine, tag, url)
    for store in cfg("stores").split():
        sc_load(led, engine, tag, url, store)
    if engine == "postgres":
        sc_gc_survival(led, engine, tag, url)
    # The COMMAND CHAIN, end to end. Both of these were registered in
    # docs/release-gate-matrix.yaml as `test` while `verify_blessed_path` had no
    # caller anywhere in the tree — the matrix guard checks that a gate function
    # has a ROW, not that anything calls it, so a dead check reads as coverage.
    # `every_test_cell_has_a_call_site` now closes that.
    from . import blessed_flow, blessed_path

    state_url = os.environ.get("RIVET_GATE_STATE_URL", "") or os.environ.get(
        "RIVET_CDC_STATE_URL", ""
    )
    blessed_path.verify_blessed_path(led, engine, tag, url, state_url=state_url)
    blessed_flow.sc_blessed_flow(led, engine, tag, url, state_url=state_url)
    # Does the chain above have working oracles at all? Breaks each artifact
    # class and requires the matching stage to go RED — a green stage that was
    # never red is unverified, and this module's own first draft had one.
    blessed_flow.sc_not_inert(led, engine, url, state_url)


# ── state-migration parity PREFLIGHT (source-agnostic, runs once) ────────────
def verify_state_migrations(led: Ledger) -> None:
    """The state layer has TWO migration arrays — MIGRATIONS (SQLite) and
    PG_MIGRATIONS (Postgres) — behind a dialect seam that ASSUMES they stay
    type-compatible, and nothing verified it: a Postgres-only schema drift (the
    int4 keyset_range bug) was invisible to every SQLite unit test.

    This INITs (migrates) and RUNs the state-exercising golden fixtures on BOTH
    backends and compares, driving the RED-proven live parity tests through the
    RELEASE binary so the gate blesses what actually ships. A schema drift aborts
    the Postgres run → the parity assert fails → NOT RELEASABLE.

    Needs a Postgres STATE db (RIVET_TEST_STATE_URL), the source Postgres on
    :5432, and cargo; SKIP — never a silent pass — when any is absent.
    """
    state_url = os.environ.get("RIVET_TEST_STATE_URL", "")
    if not state_url:
        _skipped(
            led, "state", "migrations", "parity", "-",
            "state-migration parity: no Postgres STATE url (set RIVET_TEST_STATE_URL)",
            "no state url",
        )
        return
    # The url MUST be postgres. The underlying tests self-skip (plain `return`) on
    # any other scheme, which libtest counts as PASSED — so RIVET_TEST_STATE_URL=
    # sqlite://… would print a green "SQLite == Postgres" having verified NOTHING.
    if not state_url.startswith(("postgres", "postgresql")):
        _skipped(
            led, "state", "migrations", "parity", "-",
            "state-migration parity: RIVET_TEST_STATE_URL is not a postgres url",
            "non-postgres url",
        )
        return
    # The docstring promises a SKIP when the state db is absent, but a URL string
    # is not a reachable server: without this probe a stopped stand turns the
    # documented SKIP into a hard gate FAIL, and "the stand is down" reads as
    # "the release is broken". Probe what the URL actually points at.
    _su = urllib.parse.urlsplit(state_url)
    if not _tcp_open(_su.hostname or "127.0.0.1", _su.port or 5432):
        _skipped(
            led, "state", "migrations", "parity", "-",
            f"state-migration parity: state Postgres {_su.hostname or '127.0.0.1'}:"
            f"{_su.port or 5432} unreachable (is the dev stand up?)",
            "state pg down",
        )
        return
    if not have("cargo"):
        _skipped(led, "state", "migrations", "parity", "-", "state-migration parity: cargo absent", "no cargo")
        return
    # The parity fixtures seed a source table on :5432.
    if not _tcp_open("127.0.0.1", 5432):
        _skipped(
            led, "state", "migrations", "parity", "-",
            "state-migration parity: source Postgres :5432 down", "no source pg",
        )
        return
    led.phase("State-migration parity (SQLite vs Postgres state, RED-proven, release binary)")
    # The test binary itself is debug (fast to build) and only ORCHESTRATES — the
    # rivet process it spawns is the RELEASE binary via RIVET_BIN, so the gate
    # blesses what ships. Two legs: the parity fixtures on a FRESH db (live_suite)
    # and the in-place UPGRADE path (a lib test that stages a populated v18 db and
    # migrates it to HEAD — a migration that works clean but breaks on populated
    # old data slips past the fresh-db parity otherwise).
    log_path = work_dir() / "state_parity.log"
    fresh = run(
        ["cargo", "test", "--manifest-path", str(ROOT / "Cargo.toml"), "--test", "live_suite",
         "--", "--ignored", "--test-threads=1",
         "state_parity_", "pg_keyset_range_round_trips_and_commits"],
        env={"RIVET_BIN": str(rivet_bin()), "RIVET_TEST_STATE_URL": state_url},
        timeout=NO_TIMEOUT,
    )
    transcript = fresh.out
    upgrade = None
    if fresh.ok:
        upgrade = run(
            ["cargo", "test", "--manifest-path", str(ROOT / "Cargo.toml"), "--lib",
             "--", "pg_upgrade_from_v18", "pg_shared_state_cross_connection"],
            env={"RIVET_TEST_STATE_URL": state_url},
            timeout=NO_TIMEOUT,
        )
        transcript += upgrade.out
    log_path.write_text(transcript)
    if fresh.ok and upgrade is not None and upgrade.ok:
        _passed(
            led, "state", "migrations", "parity", "-",
            "state-migration parity: SQLite == Postgres (fresh + upgrade + shared-state concurrency)",
        )
    else:
        _failed(
            led, "state", "migrations", "parity", "-",
            "state-migration parity FAILED — a schema drift between MIGRATIONS and "
            f"PG_MIGRATIONS (see {log_path})",
            _first_match(transcript, r"must succeed|cannot convert|FAILED"),
        )


# ── coverage-ledger drift-guards PREFLIGHT (offline, runs ONCE) ──────────────
def verify_inflight_run_stays_loadable(led: Ledger) -> None:
    """A load must NOT record an in-flight extraction run as fully consumed.

    The load's skip set is keyed on `run_id` alone (`select_runs` ->
    `loaded_source_run`), but a CDC run's manifest GROWS under one id: the sink
    rewrites a `Success` superset at every commit-boundary roll, and
    `list_manifest_keys` deliberately prefers that run-unique copy. A load firing
    mid-cycle therefore sampled a partial superset, recorded the id, and every
    part the same run wrote afterwards was skipped FOREVER while the next load
    printed "CDC LOAD SKIP: up to date". With `until_current: false` the id never
    rotates, so the loss was unbounded. Released since 0.20.0.

    The fix excludes the runs the ledger reports still writing into the prefix
    (`StateStore::active_run_ids_on_prefix`) from the consumed set, leaving them
    retryable. This cell drives THAT signal through the release binary, because
    it is the half a release can regress silently and it decides both failure
    directions: a run wrongly reported active makes every load re-append
    forever; a run wrongly reported finished restores the original silent loss.

    Deliberately a ledger check, not a timed extract/load race: the race needs
    sub-second timing against a live stream, and a flaky gate is worse than a
    narrow one. The end-to-end half lives in the live suite; this pins the signal
    it depends on, in the binary that ships.
    """
    if not have("sqlite3"):
        _skipped(led, "load", "inflight", "skipset", "-",
                 "in-flight load skip-set: sqlite3 absent", "no sqlite3")
        return
    work = work_dir() / "inflight_skipset"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    pfx = "gs://oracle/exports/orders/"

    # The schema comes from the RELEASE binary — `state show` opens and migrates
    # the store beside the config — so a migration that drops or renames
    # run_status fails HERE rather than only in a unit test built from source.
    cfgf = work / "probe.yaml"
    cfgf.write_text(
        "source: { type: postgres, url_env: RIVET_ORACLE_PROBE_URL }\n"
        "exports:\n  - name: probe\n    table: probe\n    mode: full\n"
        "    format: parquet\n"
        f"    destination: {{ type: local, path: {work}/out }}\n"
    )
    show = run([str(rivet_bin()), "state", "show", "-c", str(cfgf)],
               env={"RIVET_ORACLE_PROBE_URL": "postgres://rivet:rivet@127.0.0.1:5432/rivet"},
               timeout=120)
    # This cell inspects the state through `sqlite3` on the file beside the
    # config, so it grades the SQLITE backend only. On a `--state-url` pass the
    # state lives in PostgreSQL and no such file exists — a legitimate skip, but
    # it used to be reported as `rc=0`, which names the one thing that was fine.
    # A skip reason that does not say why is how a backend silently loses a cell.
    db = work / ".rivet_state.db"
    if show.returncode != 0:
        _skipped(led, "load", "inflight", "skipset", "-",
                 f"in-flight load skip-set: `rivet state show` failed (rc={show.returncode})",
                 "state show failed")
        return
    if not db.exists():
        _skipped(led, "load", "inflight", "skipset", "-",
                 "in-flight load skip-set: this cell reads the SQLite state file directly, and "
                 "this pass stores state in PostgreSQL — graded on the SQLite pass",
                 "sqlite-only cell")
        return

    def sql(stmt: str) -> str:
        return run(["sqlite3", str(db), stmt], timeout=60).stdout.strip()

    now = "2026-08-01T10:00:00Z"
    sql(
        "INSERT INTO run_status (run_id, export_name, prefix, status, started_at) "
        f"VALUES ('run_live','orders','{pfx}','running','{now}'), "
        f"       ('run_done','archive','{pfx}','success','{now}');"
    )
    # The loader's predicate, verbatim from run_status_store.rs, run against the
    # schema the release binary just created.
    named = sql(
        "SELECT group_concat(run_id) FROM run_status r "
        f"WHERE (rtrim(r.prefix,'/') = rtrim('{pfx}','/') "
        f"       OR r.prefix LIKE rtrim('{pfx}','/') || '/%' "
        f"       OR rtrim('{pfx}','/') LIKE rtrim(r.prefix,'/') || '/%') "
        "  AND r.status = 'running' "
        "  AND NOT EXISTS (SELECT 1 FROM run_status r2 "
        "                  WHERE r2.export_name = r.export_name "
        "                    AND r2.started_at > r.started_at);"
    )
    ids = {x for x in named.split(",") if x}
    if ids != {"run_live"}:
        _failed(led, "load", "inflight", "skipset", "-",
                "in-flight load skip-set: the ledger must name EXACTLY the running run "
                f"on the prefix — expected {{'run_live'}}, got {ids or 'nothing'}",
                "wrong active set")
        return
    _passed(led, "load", "inflight", "skipset", "-",
            "in-flight load skip-set: the ledger names the running run and forgets the "
            "finished one, so a growing manifest is never recorded consumed",
            "run_live named, run_done not")


def verify_coverage_matrices(led: Ledger) -> None:
    """Run every docs/*-matrix.yaml drift-guard so the go/no-go gate itself blocks
    on a rotted ledger, not just CI. A drifted matrix means the coverage claims a
    release rests on are stale → NOT RELEASABLE. SKIP when cargo is absent."""
    if not have("cargo"):
        _skipped(led, "coverage", "matrices", "ledger", "-", "coverage matrices: cargo absent", "no cargo")
        return
    led.phase("Coverage-ledger drift-guards (all docs/*-matrix.yaml)")
    log_path = work_dir() / "matrices.log"
    p = run(
        ["cargo", "test", "--manifest-path", str(ROOT / "Cargo.toml"),
         "--test", "offline_suite", "matrix_guard"],
        timeout=NO_TIMEOUT,
    )
    log_path.write_text(p.out)
    if p.ok:
        _passed(led, "coverage", "matrices", "ledger", "-", "coverage matrices: all ledgers drift-free")
    else:
        _failed(
            led, "coverage", "matrices", "ledger", "-",
            f"coverage matrices: a ledger drifted (see {log_path})",
            _first_match(p.out, r"FAILED|panicked|assertion"),
        )


# ── replica-read PREFLIGHT (prod topology: reading from a read-replica) ──────
def verify_replica_read(led: Ledger) -> None:
    """Prod often hands you a read REPLICA, not the master ("we have no master
    access"), while the rest of the gate reads every engine's primary. This drives
    the RED-proven replica test: rivet reads the mysql-replica's OWN re-logged
    binlog (log_replica_updates) after the primary's changes stream to it, proving
    the no-master-access topology end to end.

    Needs the `replica` compose profile (mysql-primary :3308 → mysql-replica
    :3309); SKIP when it is down.
    """
    if not have("cargo"):
        _skipped(led, "replica", "read", "-", "-", "replica read: cargo absent", "no cargo")
        return
    if not _tcp_open("127.0.0.1", 3309):
        _skipped(
            led, "replica", "read", "-", "-",
            "replica read: no mysql-replica :3309 "
            "(docker compose --profile replica up -d mysql-primary mysql-replica)",
            "no replica",
        )
        return
    led.phase(
        "Replica read (rivet captures a read-replica's re-logged binlog — "
        "the no-master-access topology)"
    )
    log_path = work_dir() / "replica.log"
    p = run(
        ["cargo", "test", "--manifest-path", str(ROOT / "Cargo.toml"), "--test", "live_suite",
         "--", "--ignored", "--test-threads=1", "cdc_reads_changes_from_a_replica"],
        env={"RIVET_BIN": str(rivet_bin())},
        timeout=NO_TIMEOUT,
    )
    log_path.write_text(p.out)
    if p.ok:
        _passed(
            led, "replica", "read", "-", "-",
            "replica read: rivet captures changes from the replica's re-logged binlog",
        )
    else:
        _failed(
            led, "replica", "read", "-", "-",
            f"replica read FAILED — rivet could not read from the replica (see {log_path})",
            _first_match(p.out, r"FAILED|panic|assert|error"),
        )


def verify_pool_e2e(led: Ledger) -> None:
    """The pool scheduler's e2e flow AS A GATE STAGE (#166 GA): drives the
    dedicated toxiproxy scenario — `apply --pool 2` on a multi-export config
    (one heavy + three parallel_safe) through a BANDWIDTH-CAPPED source link,
    exact per-export rows via independent DuckDB readback, plus the
    predicted-vs-actual makespan contract on stdout, and the --resume
    defer-not-drop twin. The shared-link cap is the field pathology (60 workers
    splitting one ~0.25 MB/s tunnel), so this is the honest shape to gate the
    LPT model in — not an uncontended localhost.

    Needs toxiproxy (:8474) and the main postgres (:5432); SKIP when down.
    """
    if not have("cargo"):
        _skipped(led, "pool", "e2e", "-", "-", "pool e2e: cargo absent", "no cargo")
        return
    if not _tcp_open("127.0.0.1", 8474):
        _skipped(
            led, "pool", "e2e", "-", "-",
            "pool e2e: no toxiproxy :8474 (docker compose up -d toxiproxy)",
            "no toxiproxy",
        )
        return
    led.phase(
        "Pool scheduler e2e (apply --pool through a bandwidth-capped link — "
        "exact rows + the predicted-vs-actual makespan contract)"
    )
    log_path = work_dir() / "pool_e2e.log"
    p = run(
        # `--ignored` is REQUIRED: the pool e2e tests carry #[ignore] (they need
        # the live stand), added in the CI-tier split. Without it they are SKIPPED
        # and the gate read "0 passed" as a FAIL (roast 2026-08-10). The pass check
        # below is count-agnostic (0 failed AND at least one passed) so adding a
        # pool test never silently breaks the gate the way pinning "2 passed" did.
        ["cargo", "test", "--manifest-path", str(ROOT / "Cargo.toml"), "--test", "live_suite",
         "--", "--ignored", "--test-threads=1", "live_pool_toxiproxy"],
        env={"RIVET_BIN": str(rivet_bin())},
        timeout=NO_TIMEOUT,
    )
    log_path.write_text(p.out)
    if p.ok and "0 failed" in p.out and "0 passed" not in p.out:
        _passed(
            led, "pool", "e2e", "-", "-",
            "pool e2e: bandwidth-capped --pool run exact + self-grading; resume defers, drops nothing",
        )
    else:
        _failed(
            led, "pool", "e2e", "-", "-",
            f"pool e2e FAILED (see {log_path})",
            _first_match(p.out, r"FAILED|panic|assert|error"),
        )


# NOTE: pooler safety (session-pin survival through pgbouncer/proxysql) is
# deliberately NOT a gate stage. It is a pure re-run of tests/live/
# live_pool_safety.rs, which CI already runs on every push (ci.yml brings up
# `--profile pool` for it). Re-running a CI-owned correctness suite here adds no
# coverage AND is a noise source: it reddens on a missing pool backend
# (`require_alive(Mysql)` panics when the compose `mysql` :3306 is down), drowning
# the real go/no-go signal. This gate's job is the SHIPPING-ARTIFACT checks CI
# cannot do — the release build path, regression against the previous release,
# flat RSS at scale — plus the independent-oracle scenarios above.
