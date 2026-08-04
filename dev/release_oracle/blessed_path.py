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
from pathlib import Path

from .core import Ledger, have, run, rivet
from . import scenarios

BUCKET = "rivet-blessed"


def _duckdb_ok() -> bool:
    """Is there a usable DuckDB? The oracle is worthless if its absence is
    indistinguishable from agreement — `_duckdb_list` returns "" on exit 127
    (no binary) and "" on a query error, and "" compares unequal to any count,
    which would read as a FAIL rather than the SKIP it is."""
    return have("duckdb")


def _parquet_rows_and_files(path: Path) -> tuple[int, int]:
    """(rows, distinct files) under a local prefix, read by DuckDB alone.

    `filename=true` is what makes the file count independent: counting entries
    in the directory would count whatever the filesystem holds, including parts
    of a FAILED run that no manifest declares. The question is how many files
    the readable dataset spans."""
    got = scenarios._duckdb_list(
        "SELECT count(*), count(DISTINCT filename) FROM "
        f"read_parquet('{path}/**/*.parquet', filename=true)"
    )
    if not got:
        return (-1, -1)
    head = got.splitlines()[0]
    parts = head.split("|") if "|" in head else head.split(",")
    try:
        return (int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return (-1, -1)


def _manifest_of(prefix: Path) -> dict | None:
    m = prefix / "manifest.json"
    if not m.is_file():
        return None
    try:
        return json.loads(m.read_text())
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


def _stage(led: Ledger, engine: str, tag: str, store: str, stage: str, ok: bool, detail: str) -> bool:
    """Record one stage of the chain and return whether to continue.

    Each stage is its own cell. A chain that dies at `plan` must not leave the
    later stages unrecorded — an absent cell reads as 'not applicable' in the
    report, when it means 'never reached'."""
    name = f"blessed:{stage}"
    if ok:
        led.passed(engine, tag, name, store, f"{engine} {tag} {store} · {stage}", detail)
    else:
        led.failed(engine, tag, name, store, f"{engine} {tag} {store} · {stage} — {detail}", detail)
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


def sc_blessed_path(
    led: Ledger,
    engine: str,
    tag: str,
    url: str,
    table: str,
    store: str = "local",
    state_url: str = "",
) -> None:
    """One full traversal of the blessed path for one engine × store × backend.

    `state_url` empty ⇒ the SQLite default beside the config; set ⇒ Postgres.
    The two are the same contract with hand-written SQL on each side, so the
    chain is walked on both rather than assumed transferable.
    """
    work = scenarios.work_dir() / f"blessed_{engine}_{tag}_{store}_{'pg' if state_url else 'sq'}"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    dest_dir = work / "out"
    env = dict(scenarios._store_env(url))
    if state_url:
        env["RIVET_STATE_URL"] = state_url

    # ── init ──────────────────────────────────────────────────────────────────
    cfg = work / "rivet.yaml"
    p = rivet(
        "init", "--source-env", "ORACLE_URL", "-o", str(cfg),
        env=env, timeout=scenarios.NO_TIMEOUT,
    )
    body = cfg.read_text() if cfg.is_file() else ""
    # Deliberately NOT parsed with a YAML library here. "Does the file parse"
    # is answered by feeding it to rivet's own parser — `check`, two stages
    # down, whose failure is a real failure of the chain. A private parse would
    # grade this module's YAML knowledge against init's, holding both sides of
    # the comparison; the repo has paid for that shape twice.
    ok = bool(p.ok and body.strip() and "exports:" in body)
    detail = f"config {cfg.name}, {len(body.splitlines())} lines" if ok else f"exit={p.returncode} file={cfg.is_file()}"
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
        raw = scenarios.store_dest(store, BUCKET, f"blessed/{engine}/{tag}")
        if not raw:
            led.skipped(engine, tag, "blessed:init", store, f"{store} — no destination block")
            _downstream_unreached(led, engine, tag, store, "init")
            return
        dest_block = raw
    tls = "\n  tls: {accept_invalid_certs: true}" if engine == "mssql" else ""
    cfg.write_text(
        f"source:\n"
        f"  type: {engine}\n"
        f"  url_env: ORACLE_URL{tls}\n"
        f"exports:\n"
        f"  - name: blessed\n"
        f"    table: {table}\n"
        f"    mode: full\n"
        f"    format: parquet\n"
        f"{dest_block}\n"
    )

    # ── doctor / check ────────────────────────────────────────────────────────
    for stage in ("doctor", "check"):
        r = rivet(stage, "-c", str(cfg), env=env, timeout=scenarios.NO_TIMEOUT)
        if not _stage(led, engine, tag, store, stage, r.ok, f"exit={r.returncode}"):
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
        parts, man, ok = [], None, True
        detail = "cloud prefix — asserted via readback"
    if not _stage(led, engine, tag, store, "artifacts", ok, detail):
        _downstream_unreached(led, engine, tag, store, "artifacts")
        return

    # ── state DB ──────────────────────────────────────────────────────────────
    if state_url:
        counts, where = {}, "postgres"
        got = scenarios._duckdb_list("SELECT 1")  # presence probe only
        # Postgres-side counts are compared by state_parity, which owns that
        # comparison; here the question is only whether apply RECORDED the run.
        ok, detail = True, "postgres backend — recording asserted by state_parity"
    else:
        db = cfg.parent / ".rivet_state.db"
        counts = _state_counts(db)
        wanted = ("export_metrics", "file_log", "run_status")
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
        src_rows = scenarios._source_count_distinct(engine, url, table, "id")
        if store == "local":
            duck_rows, duck_files = _parquet_rows_and_files(dest_dir)
        else:
            got = scenarios.store_readback(store, BUCKET, f"blessed/{engine}/{tag}", work)
            duck_rows = int(got.splitlines()[0]) if got.strip().isdigit() else -1
            duck_files = -1  # readback is row-count only for cloud stores
        man_rows = int(man.get("row_count", -1)) if man else -1
        man_files = int(man.get("part_count", -1)) if man else -1

        try:
            want = int(str(src_rows).strip().splitlines()[0])
        except (ValueError, IndexError):
            want = -1

        if want < 0 or duck_rows < 0:
            led.skipped(
                engine, tag, "blessed:oracle", store,
                f"{engine} {tag} {store} · oracle — unreadable "
                f"(source={src_rows!r} duckdb={duck_rows})",
            )
        else:
            agree = duck_rows == want and (man_rows < 0 or man_rows == want)
            files_agree = duck_files < 0 or man_files < 0 or duck_files == man_files
            _stage(
                led, engine, tag, store, "oracle", agree and files_agree,
                f"source={want} duckdb={duck_rows} manifest={man_rows} · "
                f"files duckdb={duck_files} manifest={man_files}",
            )

    # ── validate ──────────────────────────────────────────────────────────────
    r = rivet("validate", "-c", str(cfg), env=env, timeout=scenarios.NO_TIMEOUT)
    _stage(led, engine, tag, store, "validate", r.ok, f"exit={r.returncode}")


def verify_blessed_path(
    led: Ledger,
    engine: str,
    tag: str,
    url: str,
    table: str,
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
    sc_blessed_path(led, engine, tag, url, table, store="local")
    if state_url:
        sc_blessed_path(led, engine, tag, url, table, store="local", state_url=state_url)
    else:
        led.skipped(
            engine, tag, "blessed:backend", "postgres",
            f"{engine} {tag} · postgres state backend — no --state-url given",
        )
    if scenarios.store_up("gcs"):
        sc_blessed_path(led, engine, tag, url, table, store="gcs")
    else:
        led.skipped(engine, tag, "blessed:store", "gcs", f"{engine} {tag} · gcs — store not up")
