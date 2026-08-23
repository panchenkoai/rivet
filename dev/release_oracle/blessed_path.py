"""The blessed path, end to end: init → doctor → check → plan → apply → validate.

Every stage of this sequence is covered somewhere in the test suite. The
SEQUENCE is covered nowhere, and that is a different thing. Measured on
2026-08-04: not one test in the repository drives even THREE of the five
subcommands in a single body — the only one that mentions all five greps
`rivet --help` for the NAMES. Per-stage coverage is real (init 4 files, doctor
7, check 10, plan 5, apply 4) and the matrices are dense, but every axis in
them is `feature × runner` or `type × engine`. Nothing measures a HANDOFF.

Both defects found the day this module was written lived in exactly that gap,
and both had the same shape — **the writer and the reader addressing different
places**:

  `apply` passed `""` as the config path into the parallel-checkpoint worker,
  which resolved its state DB to `./.rivet_state.db` in the CWD while the rest
  of the run used the real one. Clean runs were fine; resume declared a
  zero-part manifest over parquet already on disk.

  `mode: cdc` never expanded `{date}` in a destination, because expansion is a
  side effect of building a PLAN and the CDC path returns before `build_plan`.
  The drain wrote to a directory named, literally, `{date}`, while validate
  resolved the template to today's date and reported an empty destination.

Neither is visible to a per-stage test: each stage does its own job correctly.

WHAT THIS IS NOT, because an earlier version of this docstring over-claimed and
a reviewer caught it. `docs/scenario-artifact-matrix.yaml` (driver
`dev/pytools/scenario_artifacts.py`) already covers the ARTIFACT half, on all
engines, against the canonical golden seed, across fourteen lifecycle scenarios
— and it snapshots MORE than this module looks at (`chunk_run`, `chunk_task`,
`load_run`, `loaded_source_run`). If the question is "what did this scenario
leave behind", that ledger is the answer and this one is not.

Three things it genuinely does not do, each verified rather than assumed:

  no independent oracle   `grep duckdb` over its driver returns nothing. It
                          compares rivet's records to declared expectations and
                          rivet's files to rivet's manifest — both sides are
                          rivet's. No foreign decoder reads the parquet.
  no command chain        it drives `check` as a probe and `load`; nothing runs
                          init → doctor → plan → apply in sequence.
  no date window          `chunk_by_days` appears in neither the matrix nor the
                          driver.

Those three are this module's contribution, plus the BigQuery cycle. The
artifact snapshot here is deliberately thinner than that ledger's and must not
be read as a replacement for it.

WHAT THIS ASSERTS, per engine × state backend × destination:

  init      the config file EXISTS and parses as YAML with an `exports:` list.
  doctor    exit status.
  check     exit status.
  plan      `plan.json` exists, parses, and carries the fields `apply` REQUIRES
            — `verify` among them. A frozen-fixture test asserted this file's
            SHAPE for two months while `rivet apply` rejected every plan users
            had on disk; the artifact is therefore fed BACK to apply here, not
            inspected.
  apply     parquet parts, a `manifest.json`, a `_SUCCESS` marker, and rows in
            the state DB (`export_metrics`, `file_log`, `run_status`).
  validate  exit status.
  oracle    DuckDB reads the parts INDEPENDENTLY — row count and file count —
            and all three parties must agree: the source table, the manifest
            rivet wrote, and DuckDB's own read. Two-party agreement is not
            enough; rivet writing and rivet re-reading share a codec.

WHAT A MISSING TOOL DOES. It SKIPs, loudly, with the reason. It never passes.
Five of the gate's own known defects are false-greens of exactly this shape —
an unreadable query scored PASS, a `cargo test` filter matching zero tests
scored PASS. A stage that could not run is not a stage that succeeded.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .core import (Ledger, cell_gate, cell_parallel, container_for_port, docker_exec,
                   have, port_of, run, rivet)
from . import scenarios

BUCKET = "rivet-blessed"

# The dev stand, by engine. `_matrix_cfg("url", engine)` owns the TEMPLATE; the
# ports here are the stand's, so the same walk runs against a local stand and
# against the gate's own containers without two copies of the URL grammar.
STAND_PORT = {"postgres": 5432, "mysql": 3306, "mssql": 1433, "mongo": 27017}

# The GOLDEN batches — what `make seed` / `make seed-release` writes, identical
# on every engine by construction, so a row count is an absolute expectation
# rather than whatever the stand happens to hold. Three shapes on purpose: a
# small dimension, a fact table with a foreign key, and the wide million-row
# table the release seed exists for. A chain proven on 1000 rows has not met a
# rollover, a part boundary, or a multi-file manifest — and the file-count half
# of the oracle is vacuous until the export produces more than one part.
GOLDEN_TABLES = ("users", "orders", "content_items")

# The timestamp column each golden table actually carries, for the date-window
# scenario. Read from the seed, not assumed.
DATE_COLUMN = {"users": "created_at", "orders": "ordered_at", "content_items": "created_at"}

# The READ STRATEGIES, which are a separate axis from the tables. rivet has
# four export runners and three of them own their own execution loop that
# returns before the shared one — so a feature proven on `full` is proven on
# ONE of four paths. That is the runner-bypass class the coverage matrix exists
# for, and it has bitten repeatedly (the drift gate absent on keyset and
# mongo_parallel; value-checksum Form B absent on all three large-table
# runners). Walking the chain on `full` alone would repeat the mistake at the
# level of the chain.
#
# `applies` keeps a scenario off a table it cannot run rather than letting it
# fail as if the product were broken: date windows need a timestamp column,
# and Mongo has neither range nor keyset chunking.
SCENARIOS: dict[str, dict] = {
    "full":     {"block": "    mode: full", "applies": lambda t, e: True},
    "keyset":   {"block": "    mode: chunked\n    chunk_by_key: id\n    chunk_size: 20000",
                 "applies": lambda t, e: e != "mongo"},
    "chunked":  {"block": "    mode: chunked\n    chunk_column: id\n    chunk_size: 20000",
                 "applies": lambda t, e: e != "mongo"},
    # `{date_col}` is substituted per table: the golden seed does NOT use one
    # name everywhere (orders has `ordered_at`, not `created_at`), and hard-
    # coding one produced a plan failure that looked like a product defect for
    # exactly as long as it took to read rivet's error — which named the real
    # column and suggested the right one.
    "datewin":  {"block": "    mode: chunked\n    chunk_column: {date_col}\n    chunk_by_days: 30",
                 "applies": lambda t, e: e != "mongo" and t in DATE_COLUMN},
}


def cloud_prefix(engine: str, tag: str, table: str, scenario: str) -> str:
    """The object-store prefix for one cell, unique per gate invocation.

    ONE function so the write side and the readback cannot drift apart — they
    are two call sites of the same string, and a prefix that differs between
    them reads as "the export wrote nothing".
    """
    return f"blessed/{scenarios.work_dir().name}/{engine}/{tag}/{table}/{scenario}"


def _duckdb_ok() -> bool:
    """Is there a usable DuckDB? The oracle is worthless if its absence is
    indistinguishable from agreement — `_duckdb_list` returns "" on exit 127
    (no binary) and "" on a query error, and "" compares unequal to any count,
    which would read as a FAIL rather than the SKIP it is."""
    return have("duckdb")


def _parquet_rows_and_files(path: Path) -> tuple[int, int]:
    """(rows, distinct files) the MANIFEST declares, read by DuckDB alone.

    `filename=true` gives the file count from the dataset DuckDB actually read
    rather than from a directory listing — but that only excludes undeclared
    parts if DuckDB was pointed at the declared ones. This docstring used to
    claim independence from "parts of a FAILED run that no manifest declares"
    while the query globbed `**/*.parquet`, which reads precisely those: the
    promise was in the comment and not in the code. Both numbers are now scoped
    to the manifest, so `duck_files` answers the same question `part_count` does
    and the two can be compared at all.

    The two numbers deliberately come from DIFFERENT reads, because they answer
    different questions and the caller compares each to a different truth:

      rows  — over the parts the MANIFEST declares, compared to the SOURCE. This
              is the delivery claim, and a glob answers it wrongly: a run that
              under-declares agrees with the source while `rivet load` reads short.
      files — over the parts ON DISK, compared to the manifest's `part_count`.
              Scoping this one to the manifest too would make both sides of that
              comparison the manifest and retire the check: disk > declared is an
              undeclared orphan, disk < declared a part that was never written,
              and only a disk-side count can see either.

    (-1, -1) when nothing readable is declared — an outcome, not a reason to fall
    back to the glob for the ROW count.)"""
    src = scenarios._declared_read(path, ".parquet")
    if not src:
        return (-1, -1)
    got = scenarios._duckdb_list(
        f"SELECT (SELECT count(*) FROM read_parquet({src})), "
        "(SELECT count(DISTINCT filename) FROM "
        f"read_parquet('{path}/**/*.parquet', filename=true))"
    )
    if not got:
        return (-1, -1)
    head = got.splitlines()[0]
    parts = head.split("|") if "|" in head else head.split(",")
    try:
        return (int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return (-1, -1)


def _source_rows(engine: str, url: str, table: str) -> int:
    """Row count from the source engine's OWN client, resolved by the URL's port.

    `scenarios._source_count_distinct` finds the gate's own `rivet-oracle-eng-*`
    containers by name; pointed at a dev stand it returns "" and the oracle then
    SKIPs — correct, but it never compares. `container_for_port` resolves
    whichever container actually serves the URL under test, so the same walk
    works on the gate's containers and on a local stand. -1 means unresolvable,
    which the caller turns into a SKIP, never into agreement."""
    port = port_of(url)
    c = container_for_port(port) if port else None
    if not c:
        return -1
    if engine == "postgres":
        out = docker_exec(c, "psql", "-U", "rivet", "-d", "rivet", "-tA",
                          "-c", f"SELECT count(*) FROM {table}").stdout.strip()
    elif engine == "mysql":
        out = docker_exec(c, "mysql", "-urivet", "-privet", "rivet", "-N",
                          "-e", f"SELECT count(*) FROM {table}").stdout.strip()
    elif engine == "mssql":
        out = docker_exec(c, "/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost",
                          "-U", "sa", "-P", "Rivet_Passw0rd!", "-d", "rivet",
                          "-h-1", "-W", "-Q",
                          f"SET NOCOUNT ON; SELECT count(*) FROM {table}").stdout.strip()
    elif engine == "mongo":
        # `mongosh` does not exist on 4.4 — see `scenarios.mongo_shell`. Asking
        # for it there returned nothing, which this function turned into -1 and
        # the oracle read as "source unreachable", SKIPping the comparison on the
        # oldest gridded version. `countDocuments({})` and not the no-arg form:
        # the legacy shell rejects it.
        out = docker_exec(c, scenarios.mongo_shell(c), "--quiet", "rivet", "--eval",
                          f"print(db.{table}.countDocuments({{}}))").stdout.strip()
    else:
        return -1
    head = out.splitlines()[0].strip() if out else ""
    return int(head) if head.isdigit() else -1


def _manifest_of(prefix: Path) -> dict | None:
    m = prefix / "manifest.json"
    if not m.is_file():
        return None
    try:
        return json.loads(m.read_text())
    except json.JSONDecodeError:
        return None


def _cloud_manifest(bucket: str, prefix: str) -> dict | None:
    """Pull `manifest.json` out of the object store, so the cloud oracle can
    compare the SAME three parties the local one does.

    Without this the cloud cell compared source vs DuckDB — two parties — and
    the file count was unchecked. A manifest is exactly the artifact a
    cross-boundary reader (`rivet load`, a warehouse job) trusts, so leaving it
    unread meant the leg most like production was the least verified."""
    url = f"http://127.0.0.1:4443/storage/v1/b/{bucket}/o/{prefix.strip('/')}%2Fmanifest.json?alt=media"
    out = run(["curl", "-sf", url]).stdout
    try:
        return json.loads(out) if out.strip() else None
    except json.JSONDecodeError:
        return None


def _state_counts(db: Path) -> dict[str, int]:
    """Per-table row counts from a SQLite state DB. Empty dict if unreadable —
    the caller distinguishes 'no DB' from 'DB with no rows', which are very
    different answers to 'did apply record its work'."""
    if not db.is_file():
        return {}
    try:
        # Read a COPY, not the artifact. `mode=ro` on a live WAL database fails
        # with "unable to open database file" (SQLite needs write access to the
        # -wal/-shm sidecars to recover), which reads as "no state DB" — the
        # exact absent/unreadable conflation this module refuses to make.
        tmp = db.parent / f".read_{db.name}"
        shutil.copy2(db, tmp)
        for side in ("-wal", "-shm"):
            s_path = db.with_name(db.name + side)
            if s_path.is_file():
                shutil.copy2(s_path, tmp.with_name(tmp.name + side))
        con = sqlite3.connect(str(tmp))
        tables = [
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        ]
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}
    except sqlite3.Error as e:
        # NOT the same as absent, and the difference decides whether the cell is
        # a product finding or a harness one. Signalled distinctly so the report
        # can say which — conflating them is the shape this whole module exists
        # to stop.
        return {"__unreadable__": -1, "__error__": str(e)[:120]}  # type: ignore[dict-item]


def _stage(led: Ledger, engine: str, tag: str, store: str, stage: str, ok: bool, detail: str,
           scenario: str = "") -> bool:
    """Record one stage of the chain and return whether to continue.

    Each stage is its own cell. A chain that dies at `plan` must not leave the
    later stages unrecorded — an absent cell reads as 'not applicable' in the
    report, when it means 'never reached'."""
    name = f"blessed:{stage}"
    if ok:
        led.passed(engine, tag, name, store, f"{engine} {tag} {store} {scenario} · {stage}", detail)
    else:
        led.failed(engine, tag, name, store, f"{engine} {tag} {store} {scenario} · {stage} — {detail}", detail)
    return ok


def _downstream_unreached(led: Ledger, engine: str, tag: str, store: str, after: str) -> None:
    """Every stage past the one that failed, recorded as SKIP with the reason.

    Silence here is the false-green shape: a run that dies at `check` and
    records nothing for `apply` looks, in a 200-row report, exactly like a run
    where apply was not applicable."""
    order = ["init", "doctor", "check", "plan", "apply", "artifacts", "state", "oracle", "validate"]
    if after not in order:
        return
    for s in order[order.index(after) + 1 :]:
        led.skipped(
            engine, tag, f"blessed:{s}", store,
            f"{engine} {tag} {store} · {s} — not reached ({after} failed)",
            f"unreached after {after}",
        )


def _pg_query(state_url: str, sql: str) -> str | None:
    """One scalar query against the Postgres state DB behind state_url. None if the
    backing container cannot be resolved (caller turns that into a FAIL, not a pass)."""
    port = port_of(state_url)
    c = container_for_port(port) if port else None
    if not c:
        return None
    db = state_url.rsplit("/", 1)[-1]
    return docker_exec(c, "psql", "-U", "rivet", "-d", db, "-tA", "-c", sql).stdout.strip()


def _run_ids(work: Path) -> list[str]:
    """This cell's run ids, from `.rivet/runs/*/summary.json` (rivet writes it beside
    the CONFIG for every run, cloud or local — no destination-manifest asymmetry).
    Used to scope the shared Postgres ledger to THIS run, mirroring blessed_flow."""
    out: list[str] = []
    for f in sorted((work / ".rivet" / "runs").glob("*/summary.json")):
        try:
            rid = json.loads(f.read_text()).get("run_id")
        except (OSError, json.JSONDecodeError):
            continue
        if rid:
            out.append(rid)
    return out


def sc_blessed_path(
    led: Ledger,
    engine: str,
    tag: str,
    url: str,
    table: str,
    store: str = "local",
    state_url: str = "",
    scenario: str = "full",
) -> None:
    """One full traversal of the blessed path for one engine × store × backend.

    `state_url` empty ⇒ the SQLite default beside the config; set ⇒ Postgres.
    The two are the same contract with hand-written SQL on each side, so the
    chain is walked on both rather than assumed transferable.
    """
    work = scenarios.work_dir() / f"blessed_{engine}_{tag}_{table.replace('.','_')}_{scenario}_{store}_{'pg' if state_url else 'sq'}"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    dest_dir = work / "out"
    env = dict(scenarios._store_env(url))
    if state_url:
        env["RIVET_STATE_URL"] = state_url
    else:
        # NEUTRALISE an inherited one. `__main__` sets `RIVET_STATE_URL` on the
        # PROCESS when the gate is pointed at a Postgres backend ("one variable
        # decides the backend for the WHOLE gate"), and `run()` merges os.environ
        # into every cell — so this module's SQLite cells wrote their ledger to
        # Postgres and then looked for a `.rivet_state.db` that was never
        # created. 84 cells failed that way on the gate's first full pass, each
        # with its own message correctly naming the cause: "apply recorded its
        # ledger elsewhere". An empty value is not a Postgres URL, so
        # `StateStore::open` falls back to SQLite (`url.starts_with("postgres")`).
        env["RIVET_STATE_URL"] = ""

    # ── init ──────────────────────────────────────────────────────────────────
    cfg = work / "rivet.yaml"
    # init WITH the TLS flag (#146), exercised end to end — not just "init exits
    # 0". mssql's stand encrypts the login packet with a self-signed cert, so
    # `require` (= TLS + accept-any after the 2026-08-08 fix) is its honest
    # posture; the loopback SQL/Mongo stands serve plaintext, so `disable` is
    # theirs. Either way the flag must REACH the scaffold's source.tls block.
    init_tls = "require" if engine == "mssql" else "disable"
    p = rivet(
        "init", "--source-env", "ORACLE_URL", "--tls", init_tls, "-o", str(cfg),
        env=env, timeout=scenarios.NO_TIMEOUT,
    )
    body = cfg.read_text() if cfg.is_file() else ""
    # The posture init was ASKED for must be IN the generated config — the
    # finding this closes: --tls cells that proved only "init exits 0" while the
    # posture never entered the chain config.
    tls_reflected = f"mode: {init_tls}" in body
    # Deliberately NOT parsed with a YAML library here. "Does the file parse"
    # is answered by feeding it to rivet's own parser — `check`, two stages
    # down, whose failure is a real failure of the chain. A private parse would
    # grade this module's YAML knowledge against init's, holding both sides of
    # the comparison; the repo has paid for that shape twice.
    ok = bool(p.ok and body.strip() and "exports:" in body and tls_reflected)
    detail = (f"config {cfg.name}, {len(body.splitlines())} lines, tls={init_tls}" if ok
              else f"exit={p.returncode} file={cfg.is_file()} tls_reflected={tls_reflected}")
    if not _stage(led, engine, tag, store, "init", ok, detail):
        _downstream_unreached(led, engine, tag, store, "init")
        return

    # Narrow the generated config to ONE table with an explicit destination, so
    # the rest of the chain has a single prefix to assert against. Everything
    # else init decided (source block, url_env, tls) is kept — rewriting it here
    # would test this module's YAML rather than init's.
    if store == "local":
        dest_block = f"    destination: {{type: local, path: {dest_dir}/}}"
    else:
        # The work root's name makes this unique PER GATE INVOCATION. Without
        # it two runs into the same bucket accumulate and the readback counts the
        # earlier one's parts as this one's — measured on the gate's first
        # back-to-back pass: `source=150000 duckdb=300000 manifest=150000`, with
        # the manifest correctly reporting 150000 the whole time. Same token the
        # CDC leg and blessed_flow already carry.
        raw = scenarios.store_dest(store, BUCKET, cloud_prefix(engine, tag, table, scenario))
        if not raw:
            led.skipped(engine, tag, "blessed:init", store, f"{store} — no destination block")
            _downstream_unreached(led, engine, tag, store, "init")
            return
        # `store_dest` returns the CONTENT under `destination:`, already
        # indented — the key itself is the caller's to write. Omitting it made
        # init's own config unparseable, which doctor caught immediately and
        # correctly; the chain surfaced it as a doctor FAIL, which is the
        # handoff working exactly as intended.
        dest_block = "    destination:\n" + raw.rstrip("\n")
    # Carry init's OWN tls posture into the narrowed config (do not hand-write
    # one) — the whole point is the chain runs over what init generated.
    tls = f"\n  tls: {{ mode: {init_tls} }}"
    cfg.write_text(
        f"source:\n"
        f"  type: {engine}\n"
        f"  url_env: ORACLE_URL{tls}\n"
        f"exports:\n"
        f"  - name: blessed\n"
        f"    table: {table}\n"
        f"{SCENARIOS[scenario]['block'].format(date_col=DATE_COLUMN.get(table, 'created_at'))}\n"
        f"    format: parquet\n"
        f"{dest_block}\n"
    )

    # ── doctor / check ────────────────────────────────────────────────────────
    for stage in ("doctor", "check"):
        r = rivet(stage, "-c", str(cfg), env=env, timeout=scenarios.NO_TIMEOUT)
        # doctor/check are READ-ONLY source probes. Under the parallel matrix a
        # transient connection blip (many cells hitting one source at once) is a
        # false alarm, not a config error — retry like seed_engine does ("a source
        # under load can drop the connection; crying wolf is worse than a retry"). A
        # real failure fails every attempt. Capture the last error line so an
        # exhausted retry is diagnosable (it was `exit=1` and nothing else before).
        for _ in range(2):
            if r.ok:
                break
            time.sleep(1.5)
            r = rivet(stage, "-c", str(cfg), env=env, timeout=scenarios.NO_TIMEOUT)
        why = "" if r.ok else " " + ((r.stderr or r.stdout or "").strip().splitlines() or [""])[-1][:160]
        if not _stage(led, engine, tag, store, stage, r.ok, f"exit={r.returncode}{why}"):
            _downstream_unreached(led, engine, tag, store, stage)
            return

    # ── plan ──────────────────────────────────────────────────────────────────
    plan_path = work / "plan.json"
    r = rivet("plan", "-c", str(cfg), "--format", "json", "-o", str(plan_path),
              env=env, timeout=scenarios.NO_TIMEOUT)
    art = None
    if r.ok and plan_path.is_file():
        try:
            art = json.loads(plan_path.read_text())
        except json.JSONDecodeError:
            art = None
    # Existence + valid JSON only. WHICH fields the artifact must carry is
    # apply's question, and it is asked by handing this file to apply below —
    # a shape assertion here is the frozen-fixture defect that stayed green for
    # two months while apply rejected every plan.json on disk.
    ok = isinstance(art, dict) and bool(art)
    if not _stage(
        led, engine, tag, store, "plan", ok,
        f"exit={r.returncode} keys={sorted(art)[:6] if isinstance(art, dict) else None}",
    ):
        _downstream_unreached(led, engine, tag, store, "plan")
        return

    # ── apply ─────────────────────────────────────────────────────────────────
    # The plan artifact is fed BACK to apply rather than inspected for shape:
    # a shape assertion is what stayed green for two months while apply
    # rejected every plan.json on disk (the `verify` field became required and
    # the fixture test never deserialized with the real type).
    # Watermark BEFORE apply: export_metrics has no run_id, and every cell names its
    # export 'blessed' on a shared long-lived Postgres ledger, so `id > pg_mark` is the
    # only way to isolate THIS run's row (a constant-name count passes on history — the
    # very defect this cell's docstring warns about; mirrors blessed_flow._state_watermark).
    pg_mark = 0
    if state_url:
        w = _pg_query(state_url, "SELECT coalesce(max(id),0) FROM export_metrics")
        pg_mark = int(w) if w and w.lstrip("-").isdigit() else 0
    r = rivet("apply", str(plan_path), env=env, timeout=scenarios.NO_TIMEOUT)
    if not _stage(led, engine, tag, store, "apply", r.ok, f"exit={r.returncode} {r.stderr[-200:]}"):
        _downstream_unreached(led, engine, tag, store, "apply")
        return

    # ── artifacts on disk ─────────────────────────────────────────────────────
    if store == "local":
        parts = sorted(dest_dir.rglob("*.parquet"))
        man = _manifest_of(dest_dir)
        success = (dest_dir / "_SUCCESS").is_file()
        ok = bool(parts) and man is not None and success
        detail = f"{len(parts)} parquet, manifest={man is not None}, _SUCCESS={success}"
    else:
        # Cloud: presence is asserted through the readback below, since a
        # bucket listing is not a local path. Counting objects with `mc ls`
        # was the documented false-green — file PRESENCE is not row content.
        # KNOWN WEAKER THAN LOCAL, stated rather than hidden. The manifest is
        # not pulled out of the bucket, so the cloud oracle below compares TWO
        # parties (source vs DuckDB) where local compares three, and the file
        # count is not checked at all (`-1` in the cell). Closing it means
        # pulling manifest.json through the store's own API — the readback
        # helper only fetches parquet today. A cell that reads PASS must not
        # imply the local cell's strength.
        parts, man, ok = [], None, True
        detail = "cloud prefix — rows via readback; manifest+file-count NOT compared (weaker than local)"
    if not _stage(led, engine, tag, store, "artifacts", ok, detail):
        _downstream_unreached(led, engine, tag, store, "artifacts")
        return

    # ── state DB ──────────────────────────────────────────────────────────────
    if state_url:
        # Query the Postgres state DB for THIS run. Deferring to state_parity was
        # a PASS that asserted nothing — the vacuous-cell shape this module is
        # written against, and I wrote one. The backend is a separate SQL
        # implementation of the same contract; "the other pass covers it" is the
        # assumption, not the evidence.
        # Scoped to THIS RUN, not just this export name. An unscoped count on the
        # shared, long-lived Postgres ledger passes on history — the cell stayed green
        # even if apply recorded NOTHING for this run (the exact defect the docstring
        # above claims to prevent, caught by the harness bughunt). export_metrics has
        # no run_id → the pre-apply watermark (id > pg_mark) isolates it; file_log and
        # run_status ARE run-scoped → the exact rows this run wrote. Mirrors blessed_flow.
        ids = _run_ids(work)
        idlist = ", ".join(f"'{r}'" for r in ids) if ids else "''"
        scoped = {
            # export_metrics DOES have a run_id (src/state/metrics.rs) — scope all three
            # by run_id, not a watermark: under the parallel gate a concurrent sibling
            # 'blessed' cell inserts id>mark rows on the shared ledger, so the watermark
            # counted a sibling and an export_metrics-specific regression passed. run_id
            # isolates THIS run regardless of concurrency. (pg_mark now vestigial.)
            "export_metrics": f"SELECT count(*) FROM export_metrics WHERE run_id IN ({idlist})",
            "file_log":       f"SELECT count(*) FROM file_log WHERE run_id IN ({idlist})",
            "run_status":     f"SELECT count(*) FROM run_status WHERE run_id IN ({idlist})",
        }
        got = {}
        for t, q in scoped.items():
            out = _pg_query(state_url, q)
            got[t] = int(out) if out and out.lstrip("-").isdigit() else -1
        if got.get("export_metrics", -1) < 0:
            ok, detail = False, f"state backend not queryable from {state_url}"
        else:
            missing = [t for t, n in got.items() if n < 1]
            ok = not missing
            detail = (f"(export=blessed, {len(ids)} run id(s)): "
                      + ", ".join(f"{t}={n}" for t, n in got.items()))
    else:
        db = cfg.parent / ".rivet_state.db"
        counts = _state_counts(db)
        wanted = ("export_metrics", "file_log", "run_status")
        # Per-RUN, not per-database. A fresh workdir makes ">= 1 row" equal to
        # "this run recorded", but the Postgres backend is shared and long-lived
        # — there the same assertion is satisfied by rows from months ago. The
        # two backends must be asked the same question, so both are asked about
        # THIS export by name.
        missing = [t for t in wanted if counts.get(t, 0) < 1]
        if "__unreadable__" in counts:
            ok, detail = False, f"state DB EXISTS at {db} but is unreadable: {counts.get('__error__')}"
        elif not counts:
            ok, detail = False, f"no state DB at {db} (file absent — apply recorded its ledger elsewhere)"
        else:
            ok = not missing
            detail = f"{db.name}: " + ", ".join(f"{t}={counts.get(t, 0)}" for t in wanted)
    if not _stage(led, engine, tag, store, "state", ok, detail):
        _downstream_unreached(led, engine, tag, store, "state")
        return

    # ── independent oracle ────────────────────────────────────────────────────
    if not _duckdb_ok():
        led.skipped(engine, tag, "blessed:oracle", store, f"{engine} {tag} {store} · oracle — no duckdb")
    else:
        want = _source_rows(engine, url, table)
        if store == "local":
            duck_rows, duck_files = _parquet_rows_and_files(dest_dir)
        else:
            pfx = cloud_prefix(engine, tag, table, scenario)
            got = scenarios.store_readback(store, BUCKET, pfx, work)
            duck_rows = int(got.splitlines()[0]) if got.strip().isdigit() else -1
            duck_files = -1  # the readback helper reports rows only
            man = _cloud_manifest(BUCKET, pfx) or man
        man_rows = int(man.get("row_count", -1)) if man else -1
        man_files = int(man.get("part_count", -1)) if man else -1

        if want < 0:
            # Source truth unreachable → cannot compare, legitimately SKIP.
            led.skipped(
                engine, tag, "blessed:oracle", store,
                f"{engine} {tag} {store} · oracle — source unreachable (source={want})",
            )
        else:
            # want>=0 and duckdb works (checked above): a duck_rows<0 here is an
            # EMPTY/undelivered destination, NOT "unreadable" — it must FAIL, not SKIP
            # (a run that reports success but delivered zero parts to the bucket used to
            # read as release-ready). The compare below does exactly that: -1 != want.
            agree = duck_rows == want and (man_rows < 0 or man_rows == want)
            files_agree = duck_files < 0 or man_files < 0 or duck_files == man_files
            # Per-column null profile, independent of rivet: a decode regression can
            # null a WHOLE column while the row count stays correct (the documented
            # uuid→FixedSizeBinary GCS incident — a CLOUD run). Local reads parts on
            # disk; cloud reads them store-native via duckdb_allnull_cloud (the same
            # read path as store_readback). -1 means unreadable (count oracle SKIPs that).
            if store == "local":
                n_null, n_cols = scenarios.duckdb_allnull_columns(f"{dest_dir}/**/*.parquet")
            else:
                # Cloud (gcs here): read the parts store-native (same path as
                # store_readback) — the GCS store is exactly where the documented
                # uuid→null incident happened, so the profile must run here too.
                pfx = cloud_prefix(engine, tag, table, scenario)
                n_null, n_cols = scenarios.duckdb_allnull_cloud(store, BUCKET, pfx, work)
            cols_ok = n_null <= 0
            _stage(
                led, engine, tag, store, "oracle", agree and files_agree and cols_ok,
                f"source={want} duckdb={duck_rows} manifest={man_rows} · "
                f"files duckdb={duck_files} manifest={man_files}"
                + (f" · ALLNULL {n_null}/{n_cols} cols" if n_null > 0 else
                   (f" · cols ok {n_cols}" if n_cols > 0 else "")),
            )

    # ── validate ──────────────────────────────────────────────────────────────
    r = rivet("validate", "-c", str(cfg), env=env, timeout=scenarios.NO_TIMEOUT)
    _stage(led, engine, tag, store, "validate", r.ok, f"exit={r.returncode}", scenario)

    # ── reconcile ─────────────────────────────────────────────────────────────
    # The count-vs-source leg. Distinct from validate (which re-reads the parts
    # rivet wrote) and from the DuckDB oracle (which reads them with a foreign
    # decoder): reconcile asks the SOURCE. Three different questions, and a
    # chain that answers only one of them is not a verified chain.
    r = rivet("run", "-c", str(cfg), "--reconcile", env=env, timeout=scenarios.NO_TIMEOUT)
    _stage(led, engine, tag, store, "reconcile", r.ok, f"exit={r.returncode}", scenario)


def sc_bq_cycle(led: Ledger, engine: str, tag: str, url: str, table: str) -> None:
    """The full cloud cycle: export to REAL GCS -> rivet load -> verify -> clean.

    This is the leg the other stores stand in for. fake-gcs proves the protocol;
    it does not prove that a warehouse can READ what rivet wrote — schema types,
    logical annotations, compression and file layout all have to survive a
    reader that shares no code with rivet at all.

    BigQuery is the independent oracle here, and a strong one: the row count
    comes from `bq query`, decoded by Google's parquet reader. When it agrees
    with the source, the artifact has been read end to end by something that
    has never seen rivet's writer.

    CLEANUP IS PART OF THE CYCLE, not politeness. A left-behind table makes the
    NEXT run's count a union of two loads, and a gate that quietly accumulates
    state stops measuring the run in front of it — the same reason the prefix is
    cleared before the export rather than after.
    """
    proj = os.environ.get("BQ_ORACLE_PROJECT") or run(
        ["gcloud", "config", "get-value", "project"]).stdout.strip()
    # PER SOURCE, which is what the gate's README already promises this variable
    # does ("one dataset PER SOURCE is derived from this") and what this line did
    # not do. `rivet load` derives the warehouse table from `table:`, so every
    # engine's `users` landed on ONE table and rivet's cross-source guard refused
    # the second one — correctly, and on six cells: mysql 8.0/8.4, mssql 2022 and
    # mongo 4.4/5/6/7/8, each after postgres had already claimed
    # `rivet_blessed.users`. The guard doing its job is not the same as the gate
    # being configured right.
    dset = os.environ.get("BQ_ORACLE_DATASET", "rivet_blessed") + "_" + engine
    bucket = os.environ.get("BQ_ORACLE_BUCKET", "rivet_data_test")
    if not have("bq") or not proj:
        led.skipped(engine, tag, "blessed:bq", "bigquery",
                    f"{engine} {tag} · bigquery — no bq CLI or project", "no creds")
        return

    work = scenarios.work_dir() / f"bq_{engine}_{tag}_{table}"
    work.mkdir(parents=True, exist_ok=True)
    pfx = f"blessed/{engine}/{tag}/{table}"
    # `rivet load` derives the warehouse table from the `table:` field, NOT from
    # the export name — so this must be the source table's name or the verify
    # queries a table that was never created. It read `-1` (absent) while the
    # load had in fact landed 1000 rows correctly.
    tbl = table.split(".")[-1]
    cfg = work / "rivet.yaml"
    tls = "\n  tls: {accept_invalid_certs: true}" if engine == "mssql" else ""
    cfg.write_text(
        f"source:\n  type: {engine}\n  url_env: ORACLE_URL{tls}\n"
        f"exports:\n  - name: {tbl}\n    table: {table}\n"
        f"    mode: full\n    format: parquet\n"
        f"    destination: {{type: gcs, bucket: {bucket}, prefix: {pfx}/}}\n"
        f"load:\n  target: bigquery\n  project: {proj}\n  dataset: {dset}\n"
    )
    env = {"ORACLE_URL": url}

    run(["bq", "--project_id", proj, "mk", "-f", "--dataset", f"{proj}:{dset}"])
    run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])

    rp = rivet("run", "-c", str(cfg), env=env, timeout=scenarios.NO_TIMEOUT)
    if not _stage(led, engine, tag, "gcs-real", "bq-export", rp.ok, f"exit={rp.returncode} {rp.stderr[-160:]}"):
        return
    lp = rivet("load", "-c", str(cfg), env=env, timeout=scenarios.NO_TIMEOUT)
    if not _stage(led, engine, tag, "bigquery", "bq-load", lp.ok, f"exit={lp.returncode} {lp.stderr[-160:]}"):
        return

    q = run(["bq", "--project_id", proj, "--format", "csv", "query", "--nouse_legacy_sql",
             f"SELECT count(*) FROM `{proj}.{dset}.{tbl}`"]).stdout.strip().splitlines()
    got = int(q[-1]) if q and q[-1].strip().isdigit() else -1
    want = _source_rows(engine, url, table)
    _stage(led, engine, tag, "bigquery", "bq-verify", got >= 0 and got == want,
           f"source={want} bigquery={got}")

    # Clean both ends. Reported, because a cleanup that silently fails leaves
    # the next run measuring a union.
    run(["bq", "--project_id", proj, "rm", "-f", "-t", f"{proj}:{dset}.{tbl}"])
    d2 = run(["gcloud", "storage", "rm", "-r", f"gs://{bucket}/{pfx}"])
    # `bq rm -f` exits 0 whether it deleted a table or found none, so its exit
    # status cannot answer "is it gone" — this cell reported a clean cleanup
    # while dropping a table that never existed. Ask afterwards instead.
    still = run(["bq", "--project_id", proj, "show", "-t", f"{proj}:{dset}.{tbl}"])
    _stage(led, engine, tag, "bigquery", "bq-cleanup", not still.ok and d2.ok,
           f"table absent after drop={not still.ok}, prefix removed={d2.ok}")


def verify_blessed_path(
    led: Ledger,
    engine: str,
    tag: str,
    url: str,
    table: str = "",
    state_url: str = "",
) -> None:
    """The matrix for one engine × version: {local, gcs} × {sqlite, postgres}.

    The store axis is here because a local filesystem has a rename and an
    object store does not; the backend axis because the two state stores are
    separate SQL implementations of one contract. Neither is a property of the
    engine, but the CHAIN is walked per engine — a handoff that works on
    Postgres proves nothing about SQL Server, which is the whole finding this
    module exists for.
    """
    led.phase(f"blessed path · {engine} {tag}")
    tables = (table,) if table else GOLDEN_TABLES
    # Collect the runnable cells across the three store×backend planes; skips are
    # recorded inline (no rivet), the chains run through the pool. Each cell has its
    # own work-dir + cloud prefix, so they are independent.
    tasks: list[tuple[str, str, str, str, str]] = []  # (table, scenario, store, state_url, slug)
    for t in tables:
        for name, sc in SCENARIOS.items():
            if not sc["applies"](t, engine):
                led.skipped(engine, tag, f"blessed:{name}", "local",
                            f"{engine} {tag} {t} · {name} — not applicable to this table/engine")
                continue
            tasks.append((t, name, "local", "", "local/sq"))
    if state_url:
        for t in tables:
            for name, sc in SCENARIOS.items():
                if sc["applies"](t, engine):
                    tasks.append((t, name, "local", state_url, "local/pg"))
    else:
        led.skipped(
            engine, tag, "blessed:backend", "postgres",
            f"{engine} {tag} · postgres state backend — no --state-url given",
        )
    if scenarios.store_up("gcs"):
        for t in tables:
            for name, sc in SCENARIOS.items():
                if sc["applies"](t, engine):
                    tasks.append((t, name, "gcs", "", "gcs/sq"))
    else:
        led.skipped(engine, tag, "blessed:store", "gcs", f"{engine} {tag} · gcs — store not up")

    # The warehouse cycle runs on ONE table per engine, deliberately: it costs a
    # real GCS round-trip and a BigQuery job, and what it proves — that a foreign
    # reader can decode what rivet wrote — does not multiply with the row count. Kept
    # sequential (one real BQ job; the BQ golden stage already parallelises engines).
    sc_bq_cycle(led, engine, tag, url, tables[0])

    if not tasks:
        return
    lock = threading.Lock()

    def run_one(task: tuple[str, str, str, str, str]) -> None:
        t, name, store, st, slug = task
        child = led.buffered_child()
        try:
            with cell_gate():
                with child.span(f"cell {engine} path {name}/{slug}"):
                    sc_blessed_path(child, engine, tag, url, t, store=store,
                                    state_url=st, scenario=name)
        except Exception as e:  # a raising cell must not abort the matrix
            child.failed(engine, tag, f"blessed:{name}", store,
                         f"{engine} {tag} {t} · {name}/{slug} — cell raised: {e}", "raised")
        with lock:
            child.flush_into(led)

    workers = min(len(tasks), cell_parallel())
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(run_one, tasks))
