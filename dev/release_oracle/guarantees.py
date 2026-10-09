"""Two stated guarantees graded by measurement, per engine.

`flat_rss` (docs/why/flat-memory.md): this binary's own `init` config in each batch mode over a
table of 10 thousand, 1 million and 5 million rows, and a CDC drain of 10 and of 30 change sets
(both past the default `rollover`). Every peak RSS stays under the engine's ceiling, and where the
smaller size already fills the buffer (keyset pages, a CDC part) the larger one costs no more.
The sentence "a 10-thousand-row table and a 500-million-row table run at the same resident set
size" is graded as written by its own cell, red today: see `SAME_RSS`.

`byte_identical_parts` (docs/semantics.md): the previous release and this binary export the same
rows through the previous release's `init` config. With `exported_at` off the parts are equal byte
for byte; as `init` writes it the parts hold one schema and the same rows in every column but
`STAMP`, which is later on every row of the later run.

One stage alone: `python -m dev.release_oracle.guarantees flat_rss|byte_identical_parts`.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import NamedTuple

from .core import Ledger, rivet_bin, run

__all__ = ["verify_flat_rss", "verify_byte_identical_parts"]

RSS_SCEN, BYTES_SCEN = "flat_rss", "byte_identical_parts"
ENGINES = ("postgres", "mysql", "mssql", "oracle", "mongo")
#: Table sizes: below one batch, past a keyset page, and five times that.
SIZES = (10_000, 1_000_000, 5_000_000)
#: Change sets (of perf.CDC_CHANGES changes each) waiting when a drain starts; both exceed the default `rollover`.
BACKLOGS = (10, 30)
#: A larger input may cost this much more than a smaller one that already fills the buffer.
FLAT_TOL = 1.25
#: The cell that grades "the same resident set size" from the smallest table to the largest; red while a table below one batch runs smaller.
SAME_RSS = "open_defect_a_small_and_a_large_table_run_at_the_same_rss"
BYTES_ROWS = 250_000
MIB = 1024 * 1024
_ENV = {"RIVET_STATE_URL": "", "RIVET_GATE_STATE_URL": ""}

#: MiB. The largest peak measured per engine on 2026-10-08 (release build, the stand), plus half:
#: batch 96 / 128 / 141 / 141 / 135 at 5 million rows in `mode: full`, CDC 187 / 123 / 301 / 274 / 116 at 600,000 changes.
RSS_CEILING_MIB = {  # ratchet-pin: flat-rss-ceiling-mib sum
    "batch": {"postgres": 144, "mysql": 192, "mssql": 212, "oracle": 212, "mongo": 203},
    "cdc": {"postgres": 281, "mysql": 185, "mssql": 452, "mongo": 411, "oracle": 174},
}  # ratchet-pin: end


def over_ceiling(peaks: dict[str, int | None], ceiling_mib: int) -> list[str]:
    """What breaks the ceiling among labelled peaks in bytes: a run that gave no measurement, or a peak above it."""
    return [f"{k}: no measurement" if not v else f"{k}: {v // MIB} MiB > {ceiling_mib} MiB"
            for k, v in peaks.items() if not v or v > ceiling_mib * MIB]


def grew(peaks: dict[str, int | None], shape: str, small: int, large: int, slack_mib: int = 0) -> str | None:
    """How `shape`'s peak at `large` exceeds its peak at `small` by more than FLAT_TOL (plus `slack_mib`); None when it does not, or either is unmeasured."""
    a, b = peaks.get(f"{shape}/{small}"), peaks.get(f"{shape}/{large}")
    if not a or not b or b <= a * FLAT_TOL + slack_mib * MIB:
        return None
    return f"{shape}: {b // MIB} MiB at {large} against {a // MIB} MiB at {small}"


def parts_differ(prev: list[str], cur: list[str]) -> str | None:
    """Why two sorted lists of part digests are not the same parts; None when they are, and there is one."""
    if not prev or not cur:
        return f"no declared part (previous {len(prev)}, this {len(cur)})"
    if len(prev) != len(cur):
        return f"{len(cur)} parts against the previous release's {len(prev)}"
    n = sum(a != b for a, b in zip(prev, cur))
    return f"{n} of {len(prev)} parts differ in bytes" if n else None


def _modes(engine: str) -> tuple[str, ...]:
    """The batch modes `init` writes for this engine."""
    return ("full",) if engine == "mongo" else ("full", "chunked", "incremental")


def _seed_any(engine: str, url: str, table: str, rows: int) -> bool:
    """(Re)create `table` holding ids 1..rows on any engine."""
    from .upgrade import _seed

    if engine != "mongo":
        return _seed(engine, url, table, rows, with_cursor=True)
    from .cdc import _mongosh

    return _mongosh(url, (
        f"db.{table}.drop(); for (let b = 0; b < {rows}; b += 50000) db.{table}.insertMany("
        f"Array.from({{length: Math.min(50000, {rows} - b)}}, (_, i) => ({{_id: b + i + 1, v: b + i + 1}})));")).ok


def _drop(engine: str, url: str, table: str) -> None:
    """Drop what `_seed_any` made."""
    if engine == "mongo":
        from .cdc import _mongosh

        _mongosh(url, f"db.{table}.drop();")
    else:
        from .engines import sql

        sql(engine, url, f"DROP TABLE IF EXISTS {table};")


def _resized(engine: str, url: str, table: str, rows: int, had: int) -> bool:
    """`table` holding ids 1..rows: seeded afresh, or a 1-million-row SQL table copied up in 1-million steps."""
    if engine == "mongo" or had != 1_000_000:
        return _seed_any(engine, url, table, rows)
    from .engines import sql

    return all(sql(engine, url, f"INSERT INTO {table} SELECT id + {lo}, v, updated_at FROM {table} "
                                f"WHERE id <= 1000000;").ok for lo in range(had, rows, had))


def _batch_peaks(root: Path, engine: str, url: str) -> dict[str, int | None]:
    """Peak RSS of this binary's `init` config per batch mode and table size; None where a run failed or came back short."""
    from .perf import _init_dir, _timed
    from .upgrade import _declared

    peaks: dict[str, int | None] = {}
    table, had = f"rss_{engine[:2]}_{os.getpid()}", 0
    try:
        for rows in SIZES:
            sized = _resized(engine, url, table, rows, had)
            had = rows
            for mode in _modes(engine):
                d = _init_dir(rivet_bin(), root, f"{table}_{mode}_{rows}", url, table, mode) if sized else None
                if d is None:
                    peaks[f"{mode}/{rows}"] = None
                    continue
                s = _timed(rivet_bin(), d, {"RIVET_PERF_URL": url, **_ENV}, "run", "-c", "c.yaml")
                got = _declared(d / "output", "SELECT count(*) FROM {parts}")
                peaks[f"{mode}/{rows}"] = s.rss if s.ok and got and got[0][0] == rows else None
                shutil.rmtree(d / "output", ignore_errors=True)  # 5 million rows per run: counted, not kept
    finally:
        _drop(engine, url, table)
    return peaks


def _cdc_peaks(engine: str, url: str) -> dict[str, int | None]:
    """Peak RSS of a CDC drain per backlog; None where the stream failed or a drain missed an inserted id."""
    from .perf import CDC_CHANGES, _timed
    from .upgrade import _declared, cdc_stream

    peaks: dict[str, int | None] = {f"backlog/{n * CDC_CHANGES}": None for n in BACKLOGS}
    probe_cm, changes = cdc_stream(engine, url)
    with probe_cm as probe:
        if probe is None:
            return peaks
        eng, work, _ = probe
        if not run([str(rivet_bin()), "run", "-c", "c.yaml"], env=_ENV, cwd=work, timeout=None).ok:
            return peaks
        lo = 1
        for n in BACKLOGS:
            for _ in range(n):
                changes(engine, url, lo)
                lo += CDC_CHANGES
            s = _timed(rivet_bin(), work, _ENV, "run", "-c", "c.yaml")
            got = _declared(work / "output", f"SELECT count(DISTINCT CAST({eng.id_col} AS BIGINT)) FROM {{parts}}")
            if not s.ok or not got or got[0][0] != lo - 1:
                return peaks
            peaks[f"backlog/{n * CDC_CHANGES}"] = s.rss
    return peaks


def _grade_rss(led: Ledger, engine: str, path: str, peaks: dict[str, int | None], flat: list[str | None]) -> None:
    """Record one engine's peaks against its ceiling and the `flat` findings (None = held)."""
    ceiling = RSS_CEILING_MIB[path][engine]
    shown = " ".join(f"{k}={v // MIB if v else '-'}MiB" for k, v in peaks.items()) + f" ceiling {ceiling}MiB"
    bad = over_ceiling(peaks, ceiling) + [f for f in flat if f]
    if bad:
        led.failed(engine, "-", RSS_SCEN, path, f"flat-rss[{engine}/{path}]: {'; '.join(bad)} ({shown})", shown)
    else:
        led.passed(engine, "-", RSS_SCEN, path, f"flat-rss[{engine}/{path}]: {shown}", shown)


def verify_flat_rss(led: Ledger) -> None:
    """Peak RSS per engine under its ceiling and flat past the buffer: batch at three table sizes, CDC at two backlogs."""
    from .perf import CDC_CHANGES
    from .upgrade import CDC_ENGINES, CDC_URL_VARS

    led.phase("Flat RSS (peak resident memory per engine against its ceiling)")
    root = Path(tempfile.mkdtemp(prefix="rivet-oracle-rss-"))
    for engine in ENGINES:
        var = f"RIVET_ORACLE_{engine.upper()}_URL"
        if not os.environ.get(var):
            led.skipped(engine, "-", RSS_SCEN, "batch", f"flat-rss[{engine}/batch]: no {var}", "no url")
            continue
        peaks = _batch_peaks(root, engine, os.environ[var])
        # A keyset page is the one batch buffer a million rows already fill; MongoDB's init writes no paged mode.
        _grade_rss(led, engine, "batch", peaks, [grew(peaks, "chunked", SIZES[1], SIZES[2])])
        same = [g for g in (grew(peaks, m, SIZES[0], SIZES[2], 8) for m in _modes(engine)) if g]
        if same or over_ceiling(peaks, 1 << 30):
            led.failed(engine, "-", RSS_SCEN, SAME_RSS, f"flat-rss[{engine}/{SAME_RSS}]: the largest table took "
                       f"more memory than the smallest: {'; '.join(same) or 'a run gave no measurement'}", "not the same")
        else:
            led.passed(engine, "-", RSS_SCEN, SAME_RSS, f"flat-rss[{engine}/{SAME_RSS}]: every mode within "
                       f"{FLAT_TOL}x + 8 MiB from {SIZES[0]} to {SIZES[2]} rows", "same")
    for engine in CDC_ENGINES:
        var = CDC_URL_VARS.get(engine, f"RIVET_CDC_{engine.upper()}_URL")
        if not os.environ.get(var):
            led.skipped(engine, "-", RSS_SCEN, "cdc", f"flat-rss[{engine}/cdc]: no {var}", "no url")
            continue
        peaks = _cdc_peaks(engine, os.environ[var])
        _grade_rss(led, engine, "cdc", peaks, [grew(peaks, "backlog", *(n * CDC_CHANGES for n in BACKLOGS))])


def _part_digests(out: Path) -> list[str]:
    """sha256 of every part the manifests under `out` declare, sorted."""
    from .upgrade import _declared_names

    names = _declared_names(out)
    return sorted(hashlib.sha256(p.read_bytes()).hexdigest() for p in out.rglob("*.parquet") if p.name in names)


#: The technical column `meta_columns.exported_at` adds (src/enrich.rs `COL_EXPORTED_AT`): the time of the write.
STAMP = "_rivet_exported_at"
#: Suffix of the cells that run init's config as written, `STAMP` on.
STAMPED = "as_init_writes_it"


class Stamped(NamedTuple):
    """What a reader found in two runs' declared parts, the earlier run first.

    `only` and the two stamps are None when the schemas gave nothing to compare.
    """

    parts: tuple[int, int]
    schemas: tuple[object, object]
    rows: tuple[int, int]
    only: tuple[int, int] | None
    last_prev: object
    first_cur: object


def stamped_differ(f: Stamped, rows: int) -> str | None:
    """Why two runs' parts are not one schema and the same `rows` rows in every column but STAMP, STAMP later on every row of the later run; None when they are."""
    if not all(f.parts):
        return f"no declared part (previous {f.parts[0]}, this {f.parts[1]})"
    was, now = f.schemas
    if was != now:
        return f"the schemas differ: previous {was[0]}, this {now[0]}"
    if STAMP not in [name for name, _ in now[0]]:
        return f"no {STAMP} column: the config as init wrote it no longer stamps its rows"
    if not f.rows[0] == f.rows[1] == rows:
        return f"{f.rows[1]} rows against the previous run's {f.rows[0]}, of {rows} at the source"
    if f.only is None:
        return "the reader compared no rows"
    if any(f.only):
        return f"outside {STAMP}, {f.only[0]} rows are only in the previous run's parts and {f.only[1]} only in this one's"
    if f.last_prev is None or f.first_cur is None or not f.last_prev < f.first_cur:
        return f"{STAMP} is not later on every row of the later run: the previous run's reach {f.last_prev}, this one's start at {f.first_cur}"
    return None


def _shape(con, parts: list[str]) -> tuple[list[tuple], list[tuple]]:
    """The parts' schema as DuckDB reads it: the columns in order, and every Parquet field's physical and logical type."""
    cols = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [parts]).fetchall()
    fields = con.execute("SELECT DISTINCT name, type, type_length, repetition_type, converted_type, logical_type "
                         "FROM parquet_schema(?) ORDER BY ALL", [parts]).fetchall()
    return [c[:2] for c in cols], fields


def _stamped(prev: list[str], cur: list[str]) -> Stamped:
    """Read the facts `stamped_differ` grades from two runs' parts with DuckDB."""
    import duckdb

    if not prev or not cur:
        return Stamped((len(prev), len(cur)), (None, None), (0, 0), None, None, None)
    con = duckdb.connect()
    one = lambda q, *a: con.execute(q, list(a)).fetchone()[0]  # noqa: E731
    schemas = (_shape(con, prev), _shape(con, cur))
    rows = (one("SELECT count(*) FROM read_parquet(?)", prev), one("SELECT count(*) FROM read_parquet(?)", cur))
    if schemas[0] != schemas[1] or STAMP not in [name for name, _ in schemas[1][0]]:
        return Stamped((len(prev), len(cur)), schemas, rows, None, None, None)
    rest = f"SELECT * EXCLUDE ({STAMP}) FROM read_parquet(?)"
    only = (one(f"SELECT count(*) FROM ({rest} EXCEPT ALL {rest})", prev, cur),
            one(f"SELECT count(*) FROM ({rest} EXCEPT ALL {rest})", cur, prev))
    return Stamped((len(prev), len(cur)), schemas, rows, only,
                   one(f"SELECT max({STAMP}) FROM read_parquet(?)", prev), one(f"SELECT min({STAMP}) FROM read_parquet(?)", cur))


def _same_parts(prev: Path, root: Path, engine: str, url: str, table: str, mode: str, stamped: bool) -> tuple[str | None, int]:
    """Why the two releases' parts of `table` in `mode` differ (None = they do not) and how many this binary wrote.

    Both run the previous release's `init` config: as written when `stamped` (graded by `stamped_differ`),
    else with its `exported_at` column off (equal bytes).
    """
    from .perf import _init_dir
    from .scenarios import _manifest_declared_parts
    from .upgrade import _declared

    tag = f"{table}_{mode}_{'stamped' if stamped else 'plain'}"
    d_prev = _init_dir(prev, root, f"{tag}_prev", url, table, mode)
    if d_prev is None:
        return "the previous release's init failed", 0
    if not stamped:
        cfg = d_prev / "c.yaml"
        cfg.write_text(cfg.read_text().replace("exported_at: true", "exported_at: false"))
    d_cur = root / f"{tag}_cur"
    d_cur.mkdir()
    shutil.copy(d_prev / "c.yaml", d_cur / "c.yaml")
    env = {"RIVET_PERF_URL": url, **_ENV}
    ran = [run([str(b), "run", "-c", "c.yaml"], env=env, cwd=d, timeout=None).ok
           for b, d in ((prev, d_prev), (rivet_bin(), d_cur))]
    if not all(ran):
        return f"a run failed (previous ok={ran[0]}, this ok={ran[1]})", 0
    if stamped:
        was, now = (_manifest_declared_parts(d / "output") for d in (d_prev, d_cur))
        return stamped_differ(_stamped(was, now), BYTES_ROWS), len(now)
    got = _declared(d_cur / "output", "SELECT count(*) FROM {parts}")
    if not got or got[0][0] != BYTES_ROWS:
        return f"this binary delivered {got[0][0] if got else 0} of {BYTES_ROWS} rows", 0
    cur = _part_digests(d_cur / "output")
    return parts_differ(_part_digests(d_prev / "output"), cur), len(cur)


def verify_byte_identical_parts(led: Ledger) -> None:
    """The previous release and this binary write the same parts for the same rows, per engine and batch mode: equal bytes, or equal outside STAMP."""
    from .regression import _require_prev_binary

    led.phase("Byte-identical parts (the previous release and this binary, same rows)")
    prev = _require_prev_binary(led, "all", "-", BYTES_SCEN, "local", "byte-identical parts")
    if prev is None:
        return
    root = Path(tempfile.mkdtemp(prefix="rivet-oracle-bytes-"))
    for engine in ENGINES:
        var = f"RIVET_ORACLE_{engine.upper()}_URL"
        url = os.environ.get(var, "")
        if not url:
            led.skipped(engine, "-", BYTES_SCEN, "local", f"byte-identical[{engine}]: no {var}", "no url")
            continue
        table = f"bytes_{engine[:2]}_{os.getpid()}"
        try:
            if not _seed_any(engine, url, table, BYTES_ROWS):
                led.failed(engine, "-", BYTES_SCEN, "seed", f"byte-identical[{engine}]: the seed failed", "seed")
                continue
            for mode, stamped in ((m, s) for s in (False, True) for m in _modes(engine)[:2]):
                cell = f"{mode}_{STAMPED}" if stamped else mode
                why, n = _same_parts(prev, root, engine, url, table, mode, stamped)
                if why:
                    led.failed(engine, "-", BYTES_SCEN, cell, f"byte-identical[{engine}/{cell}]: {why}", why)
                else:
                    led.passed(engine, "-", BYTES_SCEN, cell, f"byte-identical[{engine}/{cell}]: {n} parts, {BYTES_ROWS} rows, "
                               f"equal {f'in every column but {STAMP}' if stamped else 'bytes'}", f"{n} parts")
        finally:
            _drop(engine, url, table)


def _stamped_self_test() -> None:
    """`stamped_differ` on hand-written facts: only a later STAMP over equal rows and one schema passes."""
    def shape(*cols: str) -> tuple[list[tuple], list[tuple]]:
        return [(c, "BIGINT") for c in cols], [(c, "INT64") for c in sorted(cols)]

    plain, both = shape("id", "v"), shape("id", "v", STAMP)

    def facts(**kw) -> Stamped:
        return Stamped(**{"parts": (1, 1), "schemas": (both, both), "rows": (4, 4), "only": (0, 0), "last_prev": 1, "first_cur": 2, **kw})

    assert stamped_differ(facts(), 4) is None
    assert "1 rows are only in the previous run's parts and 1 only in this one's" in stamped_differ(facts(only=(1, 1)), 4)
    assert "0 rows are only in the previous run's parts and 2 only in this one's" in stamped_differ(facts(only=(0, 2)), 4)
    assert "is not later on every row" in stamped_differ(facts(first_cur=1), 4)
    assert "is not later on every row" in stamped_differ(facts(last_prev=2, first_cur=1), 4)
    assert "is not later on every row" in stamped_differ(facts(first_cur=None), 4)
    assert f"no {STAMP} column" in stamped_differ(facts(schemas=(plain, plain), only=None), 4)
    assert "the schemas differ" in stamped_differ(facts(schemas=(both, shape("id", "v", "extra", STAMP)), only=None), 4)
    assert "the schemas differ" in stamped_differ(facts(schemas=(both, plain), only=None), 4)
    assert "the schemas differ" in stamped_differ(facts(schemas=(both, ([("id", "BIGINT"), ("v", "INTEGER"), (STAMP, "BIGINT")], both[1]))), 4)
    assert "3 rows against the previous run's 4" in stamped_differ(facts(rows=(4, 3)), 4)
    assert "of 5 at the source" in stamped_differ(facts(), 5)
    assert stamped_differ(facts(only=None), 4) == "the reader compared no rows"
    assert stamped_differ(facts(parts=(0, 1), schemas=(None, None)), 4) == "no declared part (previous 0, this 1)"


def self_test() -> None:
    """The verdicts on hand-made inputs."""
    assert over_ceiling({"full/10000": 40 * MIB, "full/1000000": 41 * MIB}, 41) == []
    assert over_ceiling({"full/1000000": 42 * MIB}, 41) == ["full/1000000: 42 MiB > 41 MiB"]
    assert over_ceiling({"full/10000": None, "chunked/10000": 0}, 41) == [
        "full/10000: no measurement", "chunked/10000: no measurement"]
    flat = {"chunked/1": 100 * MIB, "chunked/5": 125 * MIB, "full/1": 40 * MIB, "full/5": 59 * MIB, "x/1": None, "x/5": 9}
    assert grew(flat, "chunked", 1, 5) is None and grew(flat, "x", 1, 5) is None
    assert grew(flat, "full", 1, 5) == "full: 59 MiB at 5 against 40 MiB at 1"
    assert grew(flat, "full", 1, 5, slack_mib=9) is None
    assert parts_differ(["a", "b"], ["a", "b"]) is None
    assert parts_differ(["a", "b"], ["a", "c"]) == "1 of 2 parts differ in bytes"
    assert parts_differ(["a"], ["a", "b"]) == "2 parts against the previous release's 1"
    assert parts_differ([], []) == "no declared part (previous 0, this 0)"
    _stamped_self_test()
    for path, per_engine in RSS_CEILING_MIB.items():
        assert all(v > 0 for v in per_engine.values()), f"a {path} ceiling of 0 fails every run"
    print("self-test ok: a peak over its ceiling, a missing measurement or growth past the buffer fails flat-rss; "
          "parts that differ, differ in number or are absent fail byte-identical, and so do stamped parts that "
          f"differ outside {STAMP}, in schema or in count, or whose {STAMP} did not move")


if __name__ == "__main__":
    _stages = {RSS_SCEN: verify_flat_rss, BYTES_SCEN: verify_byte_identical_parts}
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        raise SystemExit(0)
    _led = Ledger()
    for _name in sys.argv[1:] or list(_stages):
        _stages[_name](_led)
    raise SystemExit(_led.report())
