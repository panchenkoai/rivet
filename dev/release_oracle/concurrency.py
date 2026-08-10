"""Several writers, ONE destination prefix, ONE state backend, all at once.

Every other cell in this gate — and both sweeps — runs a single writer with the
prefix and the database to itself. A shared deployment is not that: several
exports run concurrently, and operators point them at one bucket path and one
Postgres state host because that is the obvious thing to do.

The failure this exists to catch has a precedent. `roast_second_run_into_same_
prefix_must_not_clobber_prior_parts` proved that CONSECUTIVE runs into one prefix
do not overwrite each other, after a version that did. Consecutive is the easy
half: run N+1 starts after run N's names are already on disk. Concurrent writers
choose their names at the SAME MOMENT, from the same clock, and nothing in the
consecutive test says what happens then.

WHAT IS ASSERTED, and each of these can fail on its own:

  no lost part      the number of data files on the prefix equals the number of
                    parts the manifest COPIES list, summed. A collision shows up
                    here as a file count SHORT of the sum — one writer's part
                    silently at the other's path.
  no lost rows      DuckDB's count over those files equals the sum of the copies'
                    `row_count`, which equals the sum of each run's own
                    `export_metrics`. Three parties, one number.
  no lost manifest  the canonical `manifest.json` is last-writer-wins BY DESIGN;
                    what must survive is one immutable `manifest-<run_id>.json`
                    per writer. A consumer sums the copies, so a missing copy is
                    a whole run invisible to the loader while its data sits there.
  no wedged run     no `running` row left unsuperseded — the signal `gc_orphans`
                    reads to decide whether unmanifested parts are live or debris.
  one row per run   no duplicate aggregate. Concurrency is exactly where an
                    UPDATE-or-INSERT can become two INSERTs.

Runs LOCAL and CLOUD. Local proves the logic; the cloud leg is not a formality —
an object store has no directory semantics, no rename, and a different commit
seam, so "two writers do not collide" has to be shown where the writers are
actually racing over a flat namespace.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from typing import Callable
from pathlib import Path

try:
    from .core import Ledger, have, run
    from .scenarios import NO_TIMEOUT, _failed, _passed, _skipped, work_dir
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import Ledger, have, run  # type: ignore
    from scenarios import NO_TIMEOUT, _failed, _passed, _skipped, work_dir  # type: ignore

__all__ = ["verify_concurrent_writers_share_a_prefix"]

#: How many exports write into the one prefix at once. Four, not two: with two,
#: a name collision has one chance to happen and a passing run is weak evidence.
WRITERS = 4
#: Rows each writer exports. Enough to roll more than one part at the chunk size
#: below, so a collision has several files to land on rather than exactly one.
ROWS = 4000
CHUNK = 1000


def _rivet_bin() -> Path:
    return Path(
        os.environ.get("RIVET_BIN")
        or Path(__file__).resolve().parents[2] / "target" / "release" / "rivet"
    )


def _psql_state(container: str, sql: str) -> str | None:
    """The scalar, or None when the QUERY DID NOT RUN.

    This returned `.stdout.strip()` and threw the exit status away, so a wrong
    container name, a stopped state DB or a SQL error all produced `""` — and
    every caller compared against `"0"`-or-empty, making an unreachable meta-DB
    score exactly like a clean one. That is not hypothetical: pointing the gate
    at the wrong Postgres is a one-flag mistake, and it was made in this repo.
    `None` forces the caller to say "could not verify", which is a different
    verdict from "verified clean"."""
    p = run(
        ["docker", "exec", container, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc", sql]
    )
    return p.stdout.strip() if p.ok else None


def _sqlite_state(db: Path, sql: str) -> str | None:
    """The scalar from a SQLite state file, or None when the query DID NOT RUN
    (same "unreadable != clean" discipline as `_psql_state`). Used by the
    shared-SQLite concurrent-writers leg: all writers open ONE `.rivet_state.db`,
    so its rows are the SQLite backend's answer to the same three questions the
    Postgres leg asks. The queries are ANSI SQL — identical on both backends."""
    if not db.is_file():
        return None
    p = run(["sqlite3", str(db), sql])
    return p.stdout.strip() if p.ok else None


def _seed(src_container: str) -> bool:
    return run(
        [
            "docker", "exec", "-i", src_container, "psql", "-U", "rivet", "-d", "rivet",
            "-v", "ON_ERROR_STOP=1", "-c",
            "DROP TABLE IF EXISTS conc_probe; "
            "CREATE TABLE conc_probe(id INT PRIMARY KEY, v TEXT); "
            f"INSERT INTO conc_probe SELECT g, 'v'||g FROM generate_series(1,{ROWS}) g;",
        ],
        timeout=180,
    ).ok


def _config(path: Path, name: str, dest_block: str, src_url: str) -> None:
    path.write_text(
        f"source:\n  type: postgres\n  url: \"{src_url}\"\n"
        f"exports:\n"
        f"  - name: {name}\n"
        f"    table: public.conc_probe\n"
        f"    mode: chunked\n"
        f"    format: parquet\n"
        f"    chunk_column: id\n"
        f"    chunk_size: {CHUNK}\n"
        f"    destination: {dest_block}\n"
    )


def _run_writers(cfgs: list[Path], state_url: str | None) -> tuple[list[int], str]:
    """Start every writer, THEN wait. Sequential `run()` calls would make this a
    consecutive-runs test wearing a concurrent name.

    `state_url` selects the shared state backend every writer contends on:
      * a Postgres URL → all writers set RIVET_STATE_URL to it (one Postgres DB);
      * `None` → RIVET_STATE_URL is UNSET, so each writer falls back to the
        beside-config `.rivet_state.db`. When the configs share a directory
        (the shared-SQLite leg), that is ONE SQLite file every writer opens at
        once — the SQLite concurrent-writer contract (lock/BUSY/WAL), untested
        until now. Must remove the var, not just skip setting it: the gate exports
        RIVET_GATE_STATE_URL → RIVET_STATE_URL into os.environ.

    Output is KEPT, not discarded: "writer exit codes [1,1,1,1]" with nothing to
    read is a cell that can only tell you it is unhappy.
    """
    env = {**os.environ}
    if state_url:
        env["RIVET_STATE_URL"] = state_url
    else:
        env.pop("RIVET_STATE_URL", None)
    procs = [
        (
            c,
            subprocess.Popen(
                [str(_rivet_bin()), "run", "-c", str(c)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            ),
        )
        for c in cfgs
    ]
    exits, chatter = [], []
    for c, p in procs:
        out, _ = p.communicate()
        exits.append(p.returncode)
        if p.returncode != 0:
            chatter.append(f"{c.name}: {(out or '').strip().splitlines()[-1] if out else '(silent)'}")
    return exits, " | ".join(chatter[:2])


def _duckdb_count(glob: str, preamble: str = "") -> int | None:
    if not have("duckdb"):
        return None
    p = run(["duckdb", "-noheader", "-list", "-c", f"{preamble}SELECT count(*) FROM read_parquet('{glob}')"])
    try:
        return int(p.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None


def _judge(
    led: Ledger,
    label: str,
    *,
    exits: list[int],
    chatter: str,
    files: int,
    copies: list[dict],
    rows_read: int | None,
    state_query: Callable[[str], str | None],
    export_prefix: str,
) -> bool:
    """The five assertions, each named so a red cell says which one broke."""
    problems: list[str] = []
    if any(e != 0 for e in exits):
        problems.append(
            f"writer exit codes {exits} — not every writer completed: {chatter or '(no output)'}"
        )

    if len(copies) != WRITERS:
        problems.append(
            f"{len(copies)} run-unique manifest copies for {WRITERS} writers — a consumer sums "
            f"the copies, so a missing one is an entire run invisible to the loader while its "
            f"data sits on the prefix"
        )
    claimed_parts = sum(int(c.get("part_count") or 0) for c in copies)
    claimed_rows = sum(int(c.get("row_count") or 0) for c in copies)
    if files != claimed_parts:
        problems.append(
            f"{files} data file(s) on the prefix but the manifests claim {claimed_parts} parts — "
            f"a shortfall is one writer's part written at another's path"
        )
    if rows_read is None:
        problems.append(
            "the DuckDB physical readback DID NOT RUN (no duckdb) — the only foreign-reader "
            "oracle is absent, which is NOT the same as agreement; the manifest self-report "
            "cannot vouch for itself"
        )
    elif rows_read != claimed_rows:
        problems.append(
            f"DuckDB reads {rows_read} rows from the prefix, the manifests claim {claimed_rows}"
        )
    if claimed_rows != WRITERS * ROWS:
        problems.append(
            f"the manifests claim {claimed_rows} rows, {WRITERS} writers exported "
            f"{WRITERS * ROWS}"
        )

    dupes = state_query(
        f"SELECT count(*) FROM (SELECT run_id FROM export_metrics WHERE export_name LIKE "
        f"'{export_prefix}%' AND run_id IS NOT NULL GROUP BY run_id HAVING count(*) > 1) d",
    )
    # `None` means the query DID NOT RUN. It is called out separately from a
    # non-zero count because the two are different verdicts and only one of them
    # is about rivet: an unreachable meta-DB says nothing about duplicate rows,
    # and must never be reported as having found none.
    if dupes is None:
        problems.append(
            "the duplicate-export_metrics check DID NOT RUN — the state DB was "
            "unreadable, which is not the same as clean"
    )
    elif dupes != "0":
        problems.append(
            f"{dupes} run(s) left MORE THAN ONE export_metrics row — under concurrency an "
            f"UPDATE-or-INSERT becomes two INSERTs, and every sum then double-counts"
        )
    stuck = state_query(
        f"SELECT count(*) FROM run_status a WHERE a.export_name LIKE '{export_prefix}%' "
        f"AND a.status='running' AND NOT EXISTS (SELECT 1 FROM run_status b "
        f"WHERE b.export_name=a.export_name AND b.started_at > a.started_at)",
    )
    if stuck is None:
        problems.append(
            "the stuck-active check DID NOT RUN — the state DB was unreadable"
    )
    elif stuck != "0":
        problems.append(
            f"{stuck} run(s) still read as ACTIVE with nothing superseding them — gc_orphans "
            f"would defer on that prefix forever"
        )
    meta_rows = state_query(
        f"SELECT coalesce(sum(total_rows),0) FROM export_metrics WHERE export_name LIKE "
        f"'{export_prefix}%' AND status='success'",
    )
    if meta_rows is None:
        problems.append(
            "the meta-row/manifest agreement check DID NOT RUN — the state DB was unreadable"
    )
    elif meta_rows != str(claimed_rows):
        problems.append(
            f"the state DB records {meta_rows} rows for these runs, the manifests claim "
            f"{claimed_rows}"
        )

    if problems:
        _failed(
            led, "-", "-", "concurrent_writers_share_a_prefix", label,
            f"concurrent-writers[{label}]: " + "; ".join(problems),
            "collision",
        )
        return False
    _passed(
        led, "-", "-", "concurrent_writers_share_a_prefix", label,
        f"concurrent-writers[{label}]: {WRITERS} writers into ONE prefix and ONE state backend — "
        f"{files} parts, {claimed_rows} rows, {len(copies)} manifest copies, all three parties "
        f"agree; no duplicate aggregate, no unsuperseded `running`",
    )
    return True


def verify_concurrent_writers_share_a_prefix(
    led: Ledger,
    *,
    state_url: str | None,
    state_container: str,
    src_container: str,
    src_url: str,
    bucket: str | None = None,
) -> None:
    """SQLite-shared leg (always), then the Postgres LOCAL + CLOUD legs when a
    state_url is given — the concurrent-writer contract on BOTH backends."""
    if not _rivet_bin().exists():
        _skipped(led, "-", "-", "concurrent_writers_share_a_prefix", "-",
                 "concurrent-writers: no rivet binary", "no binary")
        return
    if not _seed(src_container):
        _skipped(led, "-", "-", "concurrent_writers_share_a_prefix", "-",
                 "concurrent-writers: could not seed the probe table", "no seed")
        return

    stamp = time.strftime("%H%M%S")
    work = work_dir() / f"conc_{stamp}"
    work.mkdir(parents=True, exist_ok=True)

    # ── SQLITE (one shared file) ───────────────────────────────────────────────
    # N writers, ONE directory → ONE beside-config `.rivet_state.db` they all open
    # at once. This is the SQLite backend's concurrent-writer contract — file
    # lock / SQLITE_BUSY / WAL — which the Postgres-only leg below never touched:
    # the prior code SKIPPED without a Postgres URL, so SQLite under concurrency
    # was unverified. A writer that dies on a BUSY it does not retry fails here.
    if have("sqlite3"):
        sq = work / "sqlite"
        sq_dest = sq / "shared"
        sq_dest.mkdir(parents=True, exist_ok=True)
        sq_cfgs = []
        for i in range(WRITERS):
            c = sq / f"sqlite_{i}.yaml"  # same dir ⇒ shared sq/.rivet_state.db
            _config(c, f"conc_{stamp}_s{i}", f"{{ type: local, path: {sq_dest} }}", src_url)
            sq_cfgs.append(c)
        exits, chatter = _run_writers(sq_cfgs, None)  # None ⇒ beside-config SQLite
        sq_db = sq / ".rivet_state.db"
        _judge(
            led, "sqlite-shared",
            exits=exits,
            chatter=chatter,
            files=len(list(sq_dest.rglob("*.parquet"))),
            copies=[json.loads(p.read_text()) for p in sorted(sq_dest.rglob("manifest-*.json"))],
            rows_read=_duckdb_count(f"{sq_dest}/*.parquet"),
            state_query=lambda s: _sqlite_state(sq_db, s),
            export_prefix=f"conc_{stamp}_s",
        )
    else:
        _skipped(led, "-", "-", "concurrent_writers_share_a_prefix", "sqlite-shared",
                 "concurrent-writers[sqlite-shared]: sqlite3 CLI absent", "no sqlite3")

    if not state_url:
        _skipped(
            led, "-", "-", "concurrent_writers_share_a_prefix", "postgres",
            "concurrent-writers[postgres]: no shared Postgres state URL — the SQLite-shared leg "
            "ran; pass --state-url to also race the writers on one Postgres backend",
            "no state url",
        )
        return

    # ── LOCAL (Postgres state) ─────────────────────────────────────────────────
    dest = work / "shared"
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True, exist_ok=True)
    cfgs = []
    for i in range(WRITERS):
        c = work / f"local_{i}.yaml"
        _config(c, f"conc_{stamp}_w{i}", f"{{ type: local, path: {dest} }}", src_url)
        cfgs.append(c)
    exits, chatter = _run_writers(cfgs, state_url)
    files = len(list(dest.rglob("*.parquet")))
    copies = [
        json.loads(p.read_text())
        for p in sorted(dest.rglob("manifest-*.json"))
    ]
    _judge(
        led, "local",
        exits=exits,
        chatter=chatter,
        files=files,
        copies=copies,
        rows_read=_duckdb_count(f"{dest}/*.parquet"),
        state_query=lambda s: _psql_state(state_container, s),
        export_prefix=f"conc_{stamp}_w",
    )

    # ── CLOUD ────────────────────────────────────────────────────────────────
    if not bucket:
        _skipped(
            led, "-", "-", "concurrent_writers_share_a_prefix", "gcs",
            "concurrent-writers[gcs]: no bucket — the local leg proves the naming, the cloud leg "
            "proves it over a FLAT namespace with no rename, which is where writers actually race",
            "no bucket",
        )
        return
    if not have("gsutil"):
        _skipped(led, "-", "-", "concurrent_writers_share_a_prefix", "gcs",
                 "concurrent-writers[gcs]: gsutil absent", "no gsutil")
        return

    pfx = f"conc-{stamp}"
    cfgs = []
    for i in range(WRITERS):
        c = work / f"gcs_{i}.yaml"
        _config(c, f"conc_{stamp}_g{i}", f"{{ type: gcs, bucket: {bucket}, prefix: {pfx}/ }}", src_url)
        cfgs.append(c)
    exits, chatter = _run_writers(cfgs, state_url)

    listing = run(["gsutil", "ls", f"gs://{bucket}/{pfx}/**"], timeout=300).stdout.splitlines()
    objects = [o.strip() for o in listing if o.strip().startswith("gs://")]
    files = len([o for o in objects if o.endswith(".parquet")])
    copies = []
    for o in objects:
        if "/manifest-" not in o:
            continue
        text = run(["gsutil", "cat", o], timeout=180).stdout
        try:
            copies.append(json.loads(text))
        except json.JSONDecodeError:
            pass
    # Count the rows BEFORE the prefix is deleted. This passed `rows_read=None`
    # on the strength of a comment saying they were "counted by the loader
    # below" — there is no loader below; the function ends on the next line,
    # deleting the evidence. So the GCS leg rendered a verdict with the
    # row-count arm of `_judge` switched off (it is guarded by
    # `rows_read is not None`), and nothing in the report said so.
    #
    # httpfs cannot read `gs://` here, which is what the old comment was
    # reaching for — but pulling the parts down and reading them with DuckDB is
    # the same independent decode, plus a copy step.
    dl = work / f"dl_gcs_{stamp}"
    dl.mkdir(parents=True, exist_ok=True)
    pulled = run(["gsutil", "-m", "cp", "-r", f"gs://{bucket}/{pfx}/*", str(dl)], timeout=900)
    rows_read = _duckdb_count(f"{dl}/**/*.parquet") if pulled.ok else None
    if rows_read is None:
        _skipped(led, "-", "-", "concurrent_writers_share_a_prefix", "gcs-rows",
                 "concurrent-writers[gcs]: row count NOT verified — "
                 + ("duckdb absent or unreadable" if pulled.ok else "gsutil cp failed"),
                 "no row oracle")
    _judge(
        led, "gcs",
        exits=exits,
        chatter=chatter,
        files=files,
        copies=copies,
        rows_read=rows_read,
        state_query=lambda s: _psql_state(state_container, s),
        export_prefix=f"conc_{stamp}_g",
    )
    run(["gsutil", "-m", "rm", "-r", f"gs://{bucket}/{pfx}"], timeout=600)
