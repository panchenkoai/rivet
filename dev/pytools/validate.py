#!/usr/bin/env python3
"""Port of `dev/validate_exports.sh` and `dev/validate_deep.sh`.

Two post-fix validation passes over the seeded Postgres garbage fixtures
(`ext.*`), one module because they share every helper — the source-of-truth
`psql` reader, the independent DuckDB reader, the config writer and the tally:

    python3 -m dev.pytools.validate exports   # every EXPORT TYPE: artifacts + state DB
    python3 -m dev.pytools.validate deep      # VALUES, determinism, state-DB positions

`exports` walks the seven export shapes (full / keyset-seq / keyset-parallel /
keyset-parallel-checkpoint / keyset-incremental / chunked-range / CSV) and checks
the local artifacts (rows == source, manifest `row_count`, part files, `_SUCCESS`)
plus the state-DB records. `deep` goes past counts: per-column aggregates against
an INDEPENDENT reader, byte-identical values across two runs, and the persisted
keyset cursor == `max(id)`.

    RIVET_BIN   rivet binary (default ~/rivet/target/release/rivet)

Messages, ordering, colours and exit codes match the originals (`  ✓ …` /
`  ✗ …` / `▸ …`, "RESULT: N passed, N failed", exit 1 on any failure).

Deviations, each marked `DEVIATION:` at its site:

1. **The verdict is the EXIT STATUS, not a grep over the transcript.**
   `validate_exports.sh` decided a run had failed with
   `grep -qaiE "error|panic|failed" <<<"$out"`. Verified: that predicate matches
   the string `reconcile: 0 errors, 0 failed rows`, so a CLEAN run whose output
   happens to mention errors (a `0 errors` summary, a column named `error_count`,
   a table named `failed_jobs`, a status value `failed` in a preview) is reported
   as `run errored` and every one of its five checks is skipped — a false alarm
   that also silently shrinks the pass count. In the other direction it cannot
   see a run that exits non-zero without saying so (a SIGKILL/OOM is exit 137
   with an empty transcript). This is the exact anti-pattern `shell.py` was
   written to remove. `validate_deep.sh` went further and ignored the exit status
   of all four of its `rivet run` invocations, so a failed export surfaced as
   "source [150000] != parquet []" — a data-corruption verdict for a run that
   never happened.
2. **Missing tools are a precondition failure, not a wall of data failures.**
   With `duckdb` absent every `2>/dev/null` reader returned "" and each check
   reported `artifact rows [] != source [150000]`; with the PG container down,
   BOTH sides were empty. The port requires `duckdb`, the rivet binary and a
   reachable `rivet-postgres-1` up front and names what is missing.
3. **A failed `rivet state …` call no longer reads as an empty state DB.**
   `fl="$(… | grep -c …)"` with stderr on `/dev/null` gave `0` both when the
   file_log was genuinely empty and when the command failed outright, and
   `${fl:-0}` then rendered the difference invisible. Verified: `state files -c
   <missing config>` exits 1 with "config file … not found", and the shell's
   expression scores it `state files: file_log EMPTY` — a data verdict for a
   tooling failure. The port distinguishes the two and prints the client's error.
4. **The temp workdir is removed in a `finally`.** `rm -rf "$WORK"` was the last
   line of each script, so any early exit (or Ctrl-C, or the `set -u` abort) left
   a `mktemp -d` tree behind holding the exported parquet.
5. **The second `keyset_incr` run is checked.** `"$RIVET" run … >/dev/null 2>&1`
   with no `&&`/`||` meant a failing second run flowed straight into the
   "2nd run added 0" comparison, which then passed — the artifact from the FIRST
   run is still on disk and still matches the source. A no-op re-run and a
   crashed re-run were indistinguishable, which is precisely the property under
   test.

Faithful on purpose, even where it looks odd: the aggregate expressions are
passed VERBATIM to both engines (that is what makes DuckDB an independent
oracle — same SQL text, two implementations), the soft `state chunks` probe stays
a note rather than a verdict, and the manifest is read from the canonical
`manifest.json` only — the sink also writes an immutable `manifest-<run_id>.json`
copy, so a `manifest*.json` glob would double-count.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

try:  # `python3 -m dev.pytools.validate`
    from . import shell
except ImportError:  # `python3 dev/pytools/validate.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT
Fail = shell.Fail
Proc = shell.Proc
docker_exec, run = shell.docker_exec, shell.run

RIVET = Path(os.environ.get("RIVET_BIN") or (Path.home() / "rivet/target/release/rivet"))
PGC = "rivet-postgres-1"
PGURL = "postgresql://rivet:rivet@127.0.0.1:5432/rivet"

USAGE = """Usage: python3 -m dev.pytools.validate <exports|deep>

  exports   dev/validate_exports.sh — every export type: artifacts + state DB
  deep      dev/validate_deep.sh    — values, determinism, state-DB positions

Env: RIVET_BIN (default ~/rivet/target/release/rivet)"""

# `hdr()` reproduces the scripts' `printf '\033[1;34m▸ %s\033[0m\n'`; ok/bad are
# shell.py's, which already render exactly `  ✓ …` / `  ✗ …` in the same colours.
_COLOUR = sys.stderr.isatty() or os.environ.get("FORCE_COLOR") == "1"


def hdr(msg: str) -> None:
    text = f"\033[1;34m▸ {msg}\033[0m" if _COLOUR else f"▸ {msg}"
    print(text, file=sys.stderr, flush=True)


def note(msg: str) -> None:
    """A line that is neither a pass nor a fail — the original printed one bare
    `echo` for the post-finalize `state chunks` case."""
    print(msg, file=sys.stderr, flush=True)


@dataclass
class Tally:
    passed: int = 0
    failed: int = 0

    def ok(self, msg: str) -> None:
        shell.ok(msg)
        self.passed += 1

    def bad(self, msg: str) -> None:
        shell.bad(msg)
        self.failed += 1

    def result(self, label: str) -> int:
        print("", file=sys.stderr)
        hdr(f"{label}: {self.passed} passed, {self.failed} failed")
        return 0 if self.failed == 0 else 1


# ── readers ────────────────────────────────────────────────────────────────────
# NOTE: no `-i` on docker exec — `psql -c` needs no stdin, and `-i` would CONSUME
# the stdin of an enclosing loop (the shell's aggregate list is read from a
# here-doc on stdin, and one stray `-i` ate every line after the first). `run()`
# passes `-i` only when there is stdin to send, so the class is gone by
# construction; the comment stays because the reason is not obvious.
def pg(sql: str) -> str:
    """A scalar from the source of truth, whitespace squeezed out (`tr -d
    '[:space:]'`) so a client's column padding cannot fail a comparison."""
    p = docker_exec(PGC, "psql", "-U", "rivet", "-d", "rivet", "-tA", "-c", sql, timeout=900)
    return "".join(p.stdout.split())


def duck(sql: str) -> str:
    """The same scalar via DuckDB — an INDEPENDENT reader, which is the whole
    point: a value check whose `expected` comes from the code under test cannot
    catch the bug."""
    p = run(["duckdb", "-noheader", "-list", "-c", sql], timeout=900)
    return "".join(p.stdout.split())


def srccount(table: str) -> str:
    return pg(f"SELECT count(*) FROM {table}")


def pqcount(directory: Path) -> str:
    return duck(f"SELECT count(*) FROM read_parquet('{directory}/**/*.parquet')")


def csvcount(directory: Path) -> str:
    return duck(
        f"SELECT count(*) FROM read_csv('{directory}/**/*.csv', header=true, all_varchar=true)"
    )


def mfrows(directory: Path) -> int:
    """Read ONLY the canonical `manifest.json`. The sink ALSO writes an immutable
    `manifest-<run_id>.json` copy (the run-unique sidecar rule), so a
    `manifest*.json` glob double-counts; the canonical pointer is the single
    source of truth per run. `-1` when absent, as the shell's helper returned."""
    f = directory / "manifest.json"
    if not f.is_file():
        return -1
    try:
        return int(json.loads(f.read_text()).get("row_count", 0))
    except (ValueError, TypeError):
        return -1


def rivet(*args: str, timeout: float | None = None) -> Proc:
    """`$PGURL` is passed to the child explicitly rather than `export`ed into the
    ambient environment — the configs read it via `url_env: PGURL`, and a value
    that only exists for the processes that need it cannot be inherited by
    something else later in a shared shell."""
    return run([str(RIVET), *args], env={"PGURL": PGURL}, timeout=timeout)


def _tail(p: Proc, *, limit: int = 200) -> str:
    lines = [ln.strip() for ln in (p.stderr or p.stdout).splitlines() if ln.strip()]
    return (lines[-1][:limit] if lines else f"exit {p.returncode}")


def count_lines_matching(p: Proc, needle: str) -> int:
    """`grep -c <needle>` — matching LINES, not occurrences."""
    return sum(1 for line in p.stdout.splitlines() if needle in line)


def state_part_rows(tally: Tally, cfg: Path, export: str, fmt: str, *, msg: str, empty: str) -> None:
    """`rivet state files -c CFG -e EXPORT | grep -c '\\.<fmt>'`.

    file_log is written by EVERY runner (universal), so it must list this run's
    parts. (Cursor state via `state show` exists only for keyset/incremental and
    is checked per-type.)

    DEVIATION 3: a failed `state files` is reported as such instead of being
    laundered into "file_log EMPTY" by `grep -c`'s `0` and `${fl:-0}`.
    """
    p = rivet("state", "files", "-c", str(cfg), "-e", export, timeout=600)
    if not p.ok:
        tally.bad(f"state files: command failed ({_tail(p)})")
        return
    n = count_lines_matching(p, f".{fmt}")
    if n >= 1:
        tally.ok(msg.format(n=n))
    else:
        tally.bad(empty)


# ── preconditions (DEVIATION 2) ────────────────────────────────────────────────
def preflight() -> str:
    """Everything both scripts silently assumed. Returns the rivet version line,
    which each script printed in its banner."""
    if not RIVET.is_file() or not os.access(RIVET, os.X_OK):
        raise Fail(
            f"rivet binary not executable: {RIVET}",
            hint="cargo build --release --bin rivet, or set $RIVET_BIN",
        )
    shell.require("duckdb", hint="brew install duckdb — the independent read-back oracle")
    shell.require("docker", hint="the ext.* fixtures are read through the rivet-postgres-1 container")
    probe = docker_exec(PGC, "psql", "-U", "rivet", "-d", "rivet", "-tA", "-c", "SELECT 1",
                        timeout=60)
    if not probe.ok:
        raise Fail(
            f"cannot reach postgres in container {PGC}: {_tail(probe)}",
            hint="docker compose up -d postgres  (then seed dev/garbage/postgres.sql)",
        )
    version = rivet("--version", timeout=60)
    return version.stdout.strip() or version.stderr.strip()


# ══════════════════════════════════════════════════════════════════════════════
# dev/validate_exports.sh
# ══════════════════════════════════════════════════════════════════════════════
def write_cfg(cfg: Path, name: str, table: str, fmt: str, extra: Sequence[str],
              directory: Path) -> None:
    """The shell's `{ echo …; } > "$cfg"` block, line for line."""
    lines = [
        "source:",
        "  type: postgres",
        "  url_env: PGURL",
        "exports:",
        f"  - name: {name}",
        f"    table: {table}",
        *[f"    {line}" for line in extra],
        f"    format: {fmt}",
        f"    destination: {{type: local, path: {directory}/}}",
    ]
    shell.atomic_write(cfg, "".join(f"{ln}\n" for ln in lines))


def run_check(tally: Tally, work: Path, name: str, table: str, fmt: str,
              counter: Callable[[Path], str], *extra: str) -> Path:
    """One export type end to end: run it, then check the artifacts and the state
    DB. Returns the config path so a caller can add per-type checks."""
    directory = work / name
    cfg = work / f"{name}.yaml"
    shell.rm_rf(directory)
    directory.mkdir(parents=True, exist_ok=True)
    write_cfg(cfg, name, table, fmt, extra, directory)

    hdr(f"{name}  ({table}, {fmt})")

    # DEVIATION 1: the verdict is the exit status. The shell ran
    # `grep -qaiE "error|panic|failed"` over the transcript, which both
    # false-positives on data and misses a quiet non-zero exit.
    proc = rivet("run", "-c", str(cfg))
    if not proc.ok:
        tally.bad(f"{name}: run errored: {_tail(proc)}")
        return cfg

    # ── artifacts ──
    s = srccount(table)
    a = counter(directory)
    if a and s == a:
        tally.ok(f"artifact rows {a} == source {s}")
    else:
        tally.bad(f"artifact rows [{a}] != source [{s}]")

    mr = mfrows(directory)
    if str(mr) == s:
        tally.ok(f"manifest row_count {mr} == source {s}")
    else:
        tally.bad(f"manifest row_count [{mr}] != source [{s}]")

    parts = len(list(directory.rglob(f"*.{fmt}")))
    if parts >= 1:
        tally.ok(f"{parts} part file(s) written")
    else:
        tally.bad("no part files")

    if any(directory.rglob("_SUCCESS")):
        tally.ok("_SUCCESS marker present")
    else:
        tally.bad("no _SUCCESS marker")

    state_part_rows(
        tally, cfg, name, fmt,
        msg="state files: file_log has {n} part row(s)",
        empty="state files: file_log EMPTY",
    )
    return cfg


_PK_WORKER = re.compile(r"_pk_w[0-9]+_")


def validate_exports() -> int:
    """`dev/validate_exports.sh`."""
    version = preflight()
    tally = Tally()
    work = Path(tempfile.mkdtemp())
    # DEVIATION 4: teardown in a finally — the shell's trailing `rm -rf "$WORK"`
    # never ran on an early exit.
    try:
        print(f"=== rivet: {version} ===", file=sys.stderr)
        print("=== validating export types against ext.* garbage seeds ===", file=sys.stderr)

        # 1. FULL (keyless table)
        run_check(tally, work, "full_mode", "ext.no_pk_no_ts", "parquet", pqcount, "mode: full")

        # 2. KEYSET sequential
        run_check(tally, work, "keyset_seq", "ext.bigint_pk_dual_ts", "parquet", pqcount,
                  "mode: chunked", "chunk_by_key: id", "chunk_size: 50000")

        # 3. KEYSET parallel (NON-checkpoint) — bug #4: file_log MUST be written now
        cfg_par = run_check(tally, work, "keyset_par", "ext.bigint_pk_dual_ts", "parquet", pqcount,
                            "mode: chunked", "chunk_by_key: id", "parallel: 4",
                            "chunk_size: 50000")
        hdr("keyset_par: extra checks (fan-out + bug #4 file_log)")
        workers = {m.group(0) for p in (work / "keyset_par").rglob("*.parquet")
                   for m in [_PK_WORKER.search(p.name)] if m}
        if len(workers) >= 2:
            tally.ok(f"fan-out: {len(workers)} distinct pk_w workers")
        else:
            tally.bad(f"fan-out: only {len(workers)} workers")
        state_part_rows(
            tally, cfg_par, "keyset_par", "parquet",
            msg="state files: file_log has {n} rows "
                "(bug #4 fixed — non-checkpoint parallel writes file_log)",
            empty="state files: EMPTY (bug #4 regressed)",
        )

        # 4. KEYSET parallel + CHECKPOINT — keyset_range via state chunks
        cfg_ckpt = run_check(tally, work, "keyset_ckpt", "ext.bigint_pk_dual_ts", "parquet",
                             pqcount, "mode: chunked", "chunk_by_key: id", "parallel: 4",
                             "chunk_checkpoint: true", "chunk_size: 50000")
        hdr("keyset_ckpt: extra checks (keyset_range)")
        chunks = rivet("state", "chunks", "-c", str(cfg_ckpt), timeout=600)
        if re.search(r"keyset_ckpt|range|done", chunks.stdout, re.IGNORECASE):
            tally.ok("state chunks shows checkpoint rows")
        else:
            # Not a failure: finalize_keyset_anchor clears them. Kept as a note,
            # exactly as the shell had it.
            note("  (state chunks: no rows post-finalize — cleared by "
                 "finalize_keyset_anchor, expected)")

        # 5. KEYSET incremental — cursor via state show; 2nd run adds 0
        cfg_incr = run_check(tally, work, "keyset_incr", "ext.bigint_pk_dual_ts", "parquet",
                             pqcount, "mode: chunked", "chunk_by_key: id", "parallel: 4",
                             "chunk_checkpoint: true", "keyset_incremental: true",
                             "chunk_size: 50000")
        hdr("keyset_incr: extra checks (cursor persisted, 2nd run adds 0)")
        show = rivet("state", "show", "-c", str(cfg_incr), timeout=600)
        if re.search(r"keyset_incr", show.stdout, re.IGNORECASE):
            tally.ok("state show has a persisted cursor")
        else:
            tally.bad("state show: no cursor")
        # DEVIATION 5: the re-run's exit status is checked. Unchecked, a crashed
        # re-run passed the comparison below on the FIRST run's artifact.
        rerun = rivet("run", "-c", str(cfg_incr))
        if not rerun.ok:
            tally.bad(f"keyset_incr: 2nd run errored: {_tail(rerun)}")
        a2 = pqcount(work / "keyset_incr")
        s2 = srccount("ext.bigint_pk_dual_ts")
        if a2 == s2:
            tally.ok(f"2nd incremental run added 0 (rows still {a2} == {s2}, no re-export)")
        else:
            tally.bad(f"2nd run diverged: {a2} vs {s2}")

        # 6. CHUNKED range (dense int key)
        run_check(tally, work, "chunked_range", "ext.int_pk_dual_ts", "parquet", pqcount,
                  "mode: chunked", "chunk_column: id", "chunk_size: 30000")

        # 7. CSV writer path
        run_check(tally, work, "keyset_csv", "ext.bigint_pk_dual_ts", "csv", csvcount,
                  "mode: chunked", "chunk_by_key: id", "chunk_size: 50000")
    finally:
        shell.rm_rf(work)

    return tally.result("RESULT")


# ══════════════════════════════════════════════════════════════════════════════
# dev/validate_deep.sh
# ══════════════════════════════════════════════════════════════════════════════
def write_deep_cfg(cfg: Path, name: str, table: str, directory: Path,
                   extra: Sequence[str]) -> None:
    """`validate_deep.sh`'s `mkcfg` — the flow-mapping `source:` one-liner it
    used, kept as-is so the two scripts' configs stay textually distinct."""
    lines = [
        "source: {type: postgres, url_env: PGURL}",
        "exports:",
        f"  - name: {name}",
        f"    table: {table}",
        *[f"    {line}" for line in extra],
        "    format: parquet",
        f"    destination: {{type: local, path: {directory}/}}",
    ]
    shell.atomic_write(cfg, "".join(f"{ln}\n" for ln in lines))


# label | expression, applied VERBATIM to both engines. `ref_id_history` is the
# rich fixture: bigint, int, smallint, 4× numeric, text, char, 2× timestamp.
DEEP_AGGREGATES: tuple[tuple[str, str], ...] = (
    ("rows", "count(*)"),
    ("distinct ref_id", "count(distinct ref_id)"),
    ("sum(version)", "sum(version)"),
    ("sum(sign)", "sum(sign)"),
    ("sum(cart)", "round(sum(cart),4)"),
    ("sum(earning)", "round(sum(earning),4)"),
    ("sum(subtotal)", "round(sum(subtotal),4)"),
    ("sum(total)", "round(sum(total),4)"),
    ("min/max ref_id", "min(ref_id)||'-'||max(ref_id)"),
    ("distinct status", "count(distinct status)"),
    ("distinct field", "count(distinct field)"),
    ("min/max created_at", "min(created_at)||'|'||max(created_at)"),
)


def row_hash_query(directory: Path) -> str:
    """Full-row value hash, deterministically ordered (`ref_id` is non-unique →
    `+version`). No subquery alias (`row` is a reserved word in DuckDB);
    `string_agg` carries the ORDER BY."""
    return (
        "SELECT md5(string_agg(ref_id||'|'||version||'|'||coalesce(total::VARCHAR,'')||'|'"
        "||coalesce(status,'')||'|'||coalesce(created_at::VARCHAR,''), chr(10) "
        f"ORDER BY ref_id, version)) FROM read_parquet('{directory}/**/*.parquet')"
    )


def validate_deep() -> int:
    """`dev/validate_deep.sh`: values, determinism, state-DB positions."""
    version = preflight()
    tally = Tally()
    work = Path(tempfile.mkdtemp())
    try:
        print(f"=== rivet {version} — DEEP correctness validation ===", file=sys.stderr)

        # ── 1. VALUE CORRECTNESS via range-chunk — independent DuckDB aggregates ──
        hdr("ref_id_history (range-chunk): per-column VALUE correctness vs source")
        d1 = work / "rih1"
        d1.mkdir(parents=True, exist_ok=True)
        cfg1 = work / "rih1.yaml"
        write_deep_cfg(cfg1, "rih1", "ext.ref_id_history", d1,
                       ("mode: chunked", "chunk_column: ref_id", "chunk_size: 30000"))
        run1 = rivet("run", "-c", str(cfg1))
        # DEVIATION 1: the shell ignored this exit status, so a failed export was
        # reported as a per-column VALUE mismatch — a corruption verdict for data
        # that was never written.
        run1_ok = run1.ok
        if not run1_ok:
            tally.bad(f"rih1: run errored: {_tail(run1)}")
        else:
            parq = f"read_parquet('{d1}/**/*.parquet')"
            for label, expr in DEEP_AGGREGATES:
                s = pg(f"SELECT {expr} FROM ext.ref_id_history")
                p = duck(f"SELECT {expr} FROM {parq}")
                if s and s == p:
                    tally.ok(f"{label}: {s} (source==parquet)")
                else:
                    tally.bad(f"{label}: source [{s}] != parquet [{p}]")

        # ── 2. DETERMINISM: a second independent run produces identical VALUES ──
        hdr("ref_id_history: DETERMINISM (two independent runs, full-row value hash)")
        d2 = work / "rih2"
        d2.mkdir(parents=True, exist_ok=True)
        cfg2 = work / "rih2.yaml"
        write_deep_cfg(cfg2, "rih2", "ext.ref_id_history", d2,
                       ("mode: chunked", "chunk_column: ref_id", "chunk_size: 30000"))
        run2 = rivet("run", "-c", str(cfg2))
        if not run2.ok:
            tally.bad(f"rih2: run errored: {_tail(run2)}")
        elif not run1_ok:
            tally.bad("determinism: skipped — the first run never produced an artifact")
        else:
            h1 = duck(row_hash_query(d1))
            h2 = duck(row_hash_query(d2))
            if h1 and h1 == h2:
                tally.ok(f"two independent runs -> byte-identical VALUES (md5 {h1[:12]})")
            else:
                tally.bad(f"non-deterministic: run1 [{h1[:12]}] != run2 [{h2[:12]}]")

        # ── 3. keyset VALUE correctness + STATE-DB determinism (cursor == max key) ──
        hdr("bigint_pk_dual_ts (keyset incremental): values + STATE cursor determinism")
        d3 = work / "ks"
        d3.mkdir(parents=True, exist_ok=True)
        cfgk = work / "ksv.yaml"
        write_deep_cfg(cfgk, "ksv", "ext.bigint_pk_dual_ts", d3,
                       ("mode: chunked", "chunk_by_key: id", "parallel: 4",
                        "chunk_checkpoint: true", "keyset_incremental: true",
                        "chunk_size: 40000"))
        runk = rivet("run", "-c", str(cfgk))
        if not runk.ok:
            tally.bad(f"ksv: run errored: {_tail(runk)}")
            return tally.result("DEEP RESULT")

        kp = f"read_parquet('{d3}/**/*.parquet')"
        smax = pg("SELECT max(id) FROM ext.bigint_pk_dual_ts")
        pmax = duck(f"SELECT max(id) FROM {kp}")
        if smax == pmax:
            tally.ok(f"max(id): {smax} (source==parquet)")
        else:
            tally.bad(f"max(id): source [{smax}] != parquet [{pmax}]")

        ssum = pg("SELECT count(distinct id) FROM ext.bigint_pk_dual_ts")
        psum = duck(f"SELECT count(distinct id) FROM {kp}")
        if ssum == psum:
            tally.ok(f"distinct id: {ssum} (source==parquet)")
        else:
            tally.bad(f"distinct id: [{ssum}] != [{psum}]")

        # STATE-DB: the persisted incremental cursor must be EXACTLY max(id).
        # `state show` columns: EXPORT | LAST CURSOR | LAST RUN — field 2 of the
        # `ksv` row, as the original's `awk '$1=="ksv"{print $2}'` took it.
        show = rivet("state", "show", "-c", str(cfgk), timeout=600)
        cur = "\n".join(
            (f[1] if len(f) > 1 else "")
            for f in (line.split() for line in show.stdout.splitlines())
            if f and f[0] == "ksv"
        )
        if cur == smax:
            tally.ok(f"STATE cursor == max(id) {smax} (deterministic persisted position)")
        elif not show.ok:
            tally.bad(f"STATE cursor: `state show` failed ({_tail(show)})")
        else:
            tally.bad(f"STATE cursor [{cur}] != max(id) [{smax}]")

        # STATE-DB: a second run adds 0 — the cursor blocks the re-export.
        rerun = rivet("run", "-c", str(cfgk))
        if not rerun.ok:
            tally.bad(f"ksv: 2nd run errored: {_tail(rerun)}")
        pmax2 = duck(f"SELECT count(*) FROM {kp}")
        srows = pg("SELECT count(*) FROM ext.bigint_pk_dual_ts")
        if pmax2 == srows:
            tally.ok(f"2nd run added 0 rows (still {srows}) — cursor deterministically "
                     "blocks re-read")
        else:
            tally.bad(f"2nd run diverged: {pmax2} != {srows}")
    finally:
        shell.rm_rf(work)

    return tally.result("DEEP RESULT")


# ══════════════════════════════════════════════════════════════════════════════
def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help", "help"):
        print(USAGE, file=sys.stdout if args else sys.stderr)
        return 0 if args else 2
    if len(args) > 1:
        print(f"Unexpected argument: {args[1]}", file=sys.stderr)
        return 2
    if args[0] == "exports":
        return validate_exports()
    if args[0] == "deep":
        return validate_deep()
    print(f"Unknown command: {args[0]}", file=sys.stderr)
    print(USAGE, file=sys.stderr)
    return 2


if __name__ == "__main__":
    shell.main(lambda: main_cli())
