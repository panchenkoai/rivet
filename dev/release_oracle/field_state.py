"""Upgrade from a field state: what an OLD release wrote by real runs, carried on by this build.

`upgrade_continuity` crosses one release boundary. A client two or more releases behind crosses
several at once, so per old release (every tag from `COMPATIBILITY_FLOOR` to the previous one) the downloaded binary
writes the config (its own `init`) and the state (its own runs on the stand), the source changes, and
this build runs the same config twice. The destination is read by DuckDB over the parts the manifests
declare; the source by its own scanner.

  stream    `init --mode cdc` over several tables into GCS, the shape a client runs: one `tables:`
            stream with `backfill: auto` and a keyset recipe per table (MySQL, PostgreSQL), or
            `initial: snapshot` on every stream (each CDC engine). The old release takes the baseline
            and change runs, one table leaves `tables:`, and a change run is killed at its `running`
            row; or the baseline itself is killed after a part of its last table. This build must add
            or rewrite no object under a finished table's `snapshot/`, deliver exactly the changes made
            since, and leave each captured table's latest image equal to the source.
  batch     keyset_incremental, incremental, and chunk_checkpoint (keyset and range): finished, and
            killed by signal after its first part.
  ladder    every old release in turn, then this build, on ONE state.
  load      the delta families of upgrade_matrix, from each release older than the previous one.
  config    this build's `check` and `plan` over the unchanged config, against the old release's own.
  contract  what a cron wrapper reads from `run`, `load` and `compact` (exit code, `status:` and verdict
            lines) in a clean cycle, an idle one and one with a warehouse table dropped: the old release
            against this build on the same history. Every difference is a `was -> is` row; one that
            `ACCEPTED_DIFFERENCES` does not list fails.
  empty     a table EMPTY at baseline, loaded and compacted by the old release; rows arrive; this build's
            cycle must exit 0 and leave the warehouse table equal to the source.
  way back  KNOWN GAP: the previous release refuses the state this build migrated.

A red row names its kind: re-baseline, restart, duplicate, loss, refuses its own old state, config no
longer loads, contract, empty table.

    python -m dev.release_oracle.field_state --fetch <cache dir>
    RIVET_PREV_RELEASE_BIN=<previous rivet> python -m dev.release_oracle.field_state [name part ...]
    python -m dev.release_oracle.field_state --self-test
"""

from __future__ import annotations

from contextlib import closing
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .core import Ledger, Proc, isolate_state_db, rivet_bin, run, run_lanes, server_of

__all__ = ["verify_upgrade_from_field_state", "COMPATIBILITY_FLOOR", "ACCEPTED_DIFFERENCES"]

SCEN = "upgrade_from_field_state"
#: The oldest release an upgrade is guaranteed from. It moves only on the owner's word.
COMPATIBILITY_FLOOR = "0.27.0"
REPO = "panchenkoai/rivet"
GCS = "http://127.0.0.1:4443"
BUCKET = "rivet-field-state"
ROWS, STEP = 2000, 150
KILL_ROWS, PAGE = 60_000, 2000
SEED, STREAM_TABLES, BIG = 40, 4, 40_000
#: The verdict of a line a cron wrapper reads from `run`, `load` and `compact`.
VERDICT = re.compile(r"^\s*(?:(status:)\s*(\w+)|((?:CDC )?LOAD|COMPACT) (OK|SKIP|FAILED|REFUSED)\b|(\d+ of \d+) .*(failed)|(Error):(?: \[(RIVET_\w+)\])?)")
#: A wrapper-visible difference between an old release and this build that is intended, with its reason:
#: `<situation> · <step>: <was> -> <is>`.
ACCEPTED_DIFFERENCES: dict[str, str] = {  # ratchet-pin: field-state-accepted-differences strings
}  # ratchet-pin: end
#: A step that died on the road to the warehouse or its token service said nothing about rivet's contract.
TRANSPORT = re.compile(r"ADC token request failed|HTTP 5\d\d|Timeout was reached|timed out|connection reset|Connection refused", re.I)
#: The engines whose `init --mode cdc` over several tables writes one `tables:` stream with recipes.
RECIPE_ENGINES = ("mysql", "postgres")
BULK = {
    "mysql": "INSERT INTO {t} (id, v, created_at) WITH RECURSIVE g(n) AS (SELECT {lo} UNION ALL SELECT n + 1 FROM g "
             "WHERE n < {hi}) SELECT n, n, TIMESTAMP('2026-01-01 10:00:00') FROM g",
    "postgres": "INSERT INTO {t} (id, v, created_at) SELECT g, g, TIMESTAMPTZ '2026-01-01 10:00:00+00' "
                "FROM generate_series({lo}, {hi}) g",
}


@dataclass(frozen=True)
class Shape:
    """One batch shape: the old init's mode, the operator's edits of its config, and what a run owes."""

    name: str
    mode: str
    edits: tuple[tuple[str, str], ...]
    delta: bool
    killed: bool = False


_RANGE = (r"chunk_by_key: (\w+)", r"chunk_column: \1")
SHAPES = (
    Shape("keyset-incremental", "chunked", ((r"# keyset_incremental: true", "keyset_incremental: true"),), True),
    Shape("incremental", "incremental", (), True),
    Shape("keyset-checkpoint", "chunked", ((r"chunk_size: \d+", "chunk_size: 500"),), False),
    Shape("chunked-checkpoint", "chunked", (_RANGE, (r"chunk_size: \d+", "chunk_size: 500")), False),
    Shape("keyset-checkpoint-killed", "chunked", ((r"chunk_size: \d+", f"chunk_size: {PAGE}"),), False, True),
    Shape("chunked-checkpoint-killed", "chunked", (_RANGE, (r"chunk_size: \d+", f"chunk_size: {PAGE}")), False, True),
)


def triple() -> str:
    """The release-asset triple of this machine."""
    import platform

    arch = {"arm64": "aarch64", "amd64": "x86_64"}.get(platform.machine().lower(), platform.machine().lower())
    return f"{arch}-apple-darwin" if sys.platform == "darwin" else f"{arch}-unknown-linux-gnu"


def binary_in(cache: Path, version: str) -> Path:
    """Where `fetch` leaves the binary of `version`."""
    return cache / f"rivet-v{version}-{triple()}" / "rivet"


def field_cache(prev: Path) -> Path:
    """The field releases' directory: `field/` in the cache the previous release's binary sits in."""
    return prev.parent.parent / "field"


def _key(version: str) -> tuple[int, ...]:
    """A release version as a sortable tuple."""
    return tuple(int(x) for x in version.split("."))


def field_versions(tags: list[str], before: str | None = None) -> list[str]:
    """Every release among `tags` from the floor on (and older than `before`), oldest first."""
    found = {m.group(1) for t in tags if (m := re.fullmatch(r"v(\d+\.\d+\.\d+)", t))}
    return [v for v in sorted(found, key=_key)
            if _key(v) >= _key(COMPATIBILITY_FLOOR) and (before is None or _key(v) < _key(before))]


def release_tags() -> list[str]:
    """The repo's release tags."""
    from .fix_cells import git

    return git("tag", "--list", "v*").split()


def absent_note(cache: Path, version: str) -> Path:
    """Where `fetch` records that a release has no asset for this machine."""
    return cache / f"rivet-v{version}-{triple()}.absent"


def fetch(cache: Path) -> int:
    """Download every release's asset from the floor on for this machine into `cache`, checked against the release's SHA256SUMS."""
    import hashlib
    import tarfile

    cache.mkdir(parents=True, exist_ok=True)
    rc = 0
    for v in field_versions(release_tags()):
        name = f"rivet-v{v}-{triple()}.tar.gz"
        base = f"https://github.com/{REPO}/releases/download/v{v}"
        try:
            with urllib.request.urlopen(f"{base}/SHA256SUMS.txt", timeout=120) as r:
                sums = r.read().decode()
            want = next((ln.split()[0] for ln in sums.splitlines() if ln.split()[1:] and ln.split()[-1].lstrip("*") == name), None)
            if want is None:
                absent_note(cache, v).write_text(f"release v{v} publishes no {name}\n")
                print(f"  v{v}: NO ASSET for this machine ({name} is not in the release's SHA256SUMS.txt)")
                continue
            with urllib.request.urlopen(f"{base}/{name}", timeout=600) as r:
                data = r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                absent_note(cache, v).write_text(f"tag v{v} has no published release (HTTP 404)\n")
                print(f"  v{v}: NO RELEASE published for this tag")
                continue
            print(f"  v{v}: {name} could not be downloaded: {e}")
            rc = 1
            continue
        except (urllib.error.URLError, OSError) as e:
            print(f"  v{v}: {name} could not be downloaded: {e}")
            rc = 1
            continue
        if hashlib.sha256(data).hexdigest() != want:
            print(f"  v{v}: {name} does not match the release's SHA256SUMS.txt")
            rc = 1
            continue
        (cache / name).write_bytes(data)
        with tarfile.open(cache / name) as t:
            t.extractall(cache, filter="data")
        said = run([str(binary_in(cache, v)), "--version"]).stdout.strip()
        print(f"  {said} -> {binary_in(cache, v)}")
        rc = rc or (0 if f" {v} " in f"{said} " else 1)
    return rc


def old_releases(led: Ledger) -> list[tuple[str, Path]]:
    """(version, binary) of every release the stage upgrades from, oldest first; a missing one is recorded."""
    from .fix_cells import prev_tag
    from .regression import _require_prev_binary, absent_release_binary

    what = "upgrade from a field state"
    prev = _require_prev_binary(led, "all", "-", SCEN, "-", what)
    if prev is None:
        return []
    out: list[tuple[str, Path]] = []
    last = prev_tag(prev).lstrip("v")
    want = field_versions(release_tags(), before=last)
    if want[:1] != [COMPATIBILITY_FLOOR]:
        led.failed("all", "-", SCEN, "-", f"{what}: the releases older than v{last} start at {want[:1] or 'none'}, not at the "
                   f"compatibility floor v{COMPATIBILITY_FLOOR} — the repo lacks its release tags (`git fetch --tags`) or "
                   "the previous release is not newer than the floor", "floor")
        return []
    for v in want:
        b, note = binary_in(field_cache(prev), v), absent_note(field_cache(prev), v)
        if note.is_file():
            led.skipped("all", "-", SCEN, "-", f"{what}: v{v} is not upgraded from on {triple()}: {note.read_text().strip()}", "no asset")
        elif not (b.is_file() and os.access(b, os.X_OK)):
            absent_release_binary(led, "all", "-", SCEN, "-", what, f"the v{v} release binary is not at {b}")
        elif prev_tag(b) != f"v{v}":
            led.failed("all", "-", SCEN, "-", f"{what}: {b} is {prev_tag(b)}, not v{v}", "wrong binary")
        else:
            out.append((v, b))
    return [*out, (last, prev)]


def edited(text: str, edits: tuple[tuple[str, str], ...]) -> str | None:
    """`text` with every (pattern, line) applied; None when a pattern names nothing the old init wrote."""
    for pat, repl in edits:
        text, n = re.subn(pat, repl, text)
        if not n:
            return None
    return text


def with_endpoint(text: str) -> str | None:
    """init's GCS destinations pointed at the stand's emulator; None when it wrote none."""
    return edited(text, ((r"(?m)^(\s*)bucket: \S+.*$", rf"\g<0>\n\g<1>endpoint: {GCS}"),))


def as_snapshot_streams(text: str) -> str:
    """init's CDC config with every stream taking its baseline by `initial: snapshot` and no recipe left."""
    import yaml

    cfg = yaml.safe_load(text)
    cfg["exports"] = [x for x in cfg["exports"] if x.get("mode") == "cdc"]
    for x in cfg["exports"]:
        x["cdc"].pop("backfill", None)
        x["cdc"]["initial"] = "snapshot"
    return yaml.safe_dump(cfg, sort_keys=False)


def without_table(text: str, table: str) -> str:
    """The config after an operator takes `table` out: its stream entry, its recipe and its `load.tables` entry."""
    import yaml

    cfg = yaml.safe_load(text)
    cfg["exports"] = [x for x in cfg["exports"] if table not in (x.get("name"), x.get("table"))]
    for x in cfg["exports"]:
        if isinstance(x.get("tables"), list):
            x["tables"] = [t for t in x["tables"] if t != table]
        ((x.get("load") or {}).get("tables") or {}).pop(table, None)
    return yaml.safe_dump(cfg, sort_keys=False)


def finding(src: list, got: list, fresh: list | None = None, owed: list | None = None) -> tuple[str, str] | None:
    """(kind, line) of the first way `got` (what a consumer reads) differs from `src`, or `fresh` (one run's rows) from `owed`."""
    if fresh is not None:
        extra = sorted((Counter(fresh) - Counter(owed or [])).elements())
        if extra:
            kind = "restart" if len(fresh) >= len(src) > len(owed or []) else "duplicate"
            return kind, f"the run declared {len(fresh)} rows where the source gained {len(owed or [])}; first not owed: {extra[0]}"
        missed = sorted((Counter(owed or []) - Counter(fresh)).elements())
        if missed:
            return "loss", f"the run declared {len(fresh)} of the {len(owed or [])} rows the source gained; first missing: {missed[0]}"
    have, want = Counter(got), Counter(src)
    twice = sorted(r for r, n in have.items() if n > want[r] > 0)
    if twice:
        return "duplicate", f"{len(twice)} source rows are declared more than once; first: {twice[0]}"
    lost = sorted((want - have).elements())
    if lost:
        return "loss", f"{len(lost)} of {len(src)} source rows are not declared; first: {lost[0]}"
    stray = sorted((have - want).elements())
    if stray:
        return "loss", f"{len(stray)} declared rows are not source rows; first: {stray[0]}"
    return None


def rebaselined(before: dict[str, str], after: dict[str, str], tables: list[str]) -> list[str]:
    """The objects under a `snapshot/` of `tables` that are new or rewritten in `after`."""
    return sorted(n for t in tables for n in _of(list(after), t, True) if before.get(n) != after[n])


def config_finding(was: dict[str, tuple[bool, list[str]]], now: dict[str, tuple[bool, list[str]]], why: str) -> str | None:
    """How this build's `check` / `plan` answer the old config differently from the old release, or None."""
    for verb in ("check", "plan"):
        if was[verb][0] and not now[verb][0]:
            return f"`rivet {verb}` passed on the old release and fails on this build: {why}"
    if was["check"][0] and was["check"][1] != now["check"][1]:
        return f"`rivet check` reads it differently: {was['check'][1]} on the old release, {now['check'][1]} on this build"
    return None


def _objects(prefix: str) -> dict[str, str]:
    """name -> generation of every object under `prefix`, by the emulator's own listing."""
    out: dict[str, str] = {}
    token = ""
    while True:
        q = urllib.parse.urlencode({"prefix": prefix, **({"pageToken": token} if token else {})})
        with urllib.request.urlopen(f"{GCS}/storage/v1/b/{BUCKET}/o?{q}", timeout=30) as r:
            doc = json.load(r)
        out.update({it["name"]: str(it.get("generation")) for it in doc.get("items", [])})
        token = doc.get("nextPageToken", "")
        if not token:
            return out


def _settled(prefix: str) -> dict[str, str]:
    """The listing once two in a row agree: the emulator finishes a killed writer's upload on its own time."""
    last = _objects(prefix)
    for _ in range(40):
        time.sleep(0.25)
        now = _objects(prefix)
        if now == last:
            break
        last = now
    return last


def _bucket() -> bool:
    """Create the stage's bucket on the emulator (kept if there); False when the emulator is down."""
    req = urllib.request.Request(f"{GCS}/storage/v1/b?project=field", data=json.dumps({"name": BUCKET}).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
    except urllib.error.HTTPError:
        pass
    except (urllib.error.URLError, OSError):
        return False
    return True


def _forget(prefix: str) -> None:
    """Delete the cell's objects from the emulator."""
    for name in _objects(prefix):
        req = urllib.request.Request(f"{GCS}/storage/v1/b/{BUCKET}/o/{urllib.parse.quote(name, safe='')}", method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                r.read()
        except (urllib.error.URLError, OSError):
            pass


class _Table:
    """A source table of ids 1..n with `v` and a cursor column that grows with the id."""

    def __init__(self, engine: str, url: str, name: str):
        self.engine, self.url, self.name, self.n = engine, url, name, 0

    def add(self, n: int) -> bool:
        from .engines import sql
        from .upgrade import _seed
        from .upgrade_matrix import _drop

        hi = self.n + n
        if self.n == 0:
            ok = _seed(self.engine, self.url, self.name, hi, with_cursor=True)
        else:
            tmp = f"{self.name}_add"
            ok = _seed(self.engine, self.url, tmp, hi, with_cursor=True) and sql(
                self.engine, self.url, f"INSERT INTO {self.name} (id, v, updated_at) SELECT id, v, updated_at "
                                       f"FROM {tmp} WHERE id > {self.n}").ok
            _drop(self.engine, self.url, tmp)
        self.n = hi if ok else self.n
        return ok

    def rows(self) -> list[tuple[int, int]]:
        """(id, v) of every source row, read by the engine's own scanner."""
        from .duck import Oracle as Duck
        from .value_diff import oracle_rows, source_attach

        if self.engine == "oracle":
            return sorted((int(r["ID"]), int(r["V"])) for r in oracle_rows(self.url, f"SELECT id, v FROM {self.name}"))
        kw, schema = source_attach(self.engine, self.url)
        with Duck(**kw) as o:
            return sorted((int(i), int(v)) for i, v in o.rows(f"SELECT id, v FROM {schema}.{self.name}"))

    def drop(self) -> None:
        from .upgrade_matrix import _drop

        _drop(self.engine, self.url, self.name)


def _manifests(root: Path) -> set[str]:
    """Every per-run manifest under `root`, relative to it."""
    return {str(p.relative_to(root)) for p in root.rglob("manifest-*.json")}


def _parts(root: Path, manifests: set[str], newest: bool = False) -> list[str]:
    """The committed parts the named success manifests declare; with `newest`, those of the last finished one."""
    from .rig_oracle import declared_parts, in_run_order

    ok = [m for m in in_run_order(str(root), sorted(manifests)) if declared_parts(str(root), [m])]
    return declared_parts(str(root), ok[-1:] if newest else ok)


def _id_v(parts: list[str]) -> list[tuple[int, int]]:
    """(id, v) of every row in `parts`."""
    import duckdb

    if not parts:
        return []
    with duckdb.connect() as con:
        return sorted((int(i), int(v)) for i, v in con.execute(f"SELECT id, v FROM read_parquet({parts})").fetchall())


def state_facts(state: str) -> tuple[int, str]:
    """(schema version, what the state DB holds): the version, the form of `export_state`'s source prefix, runs left `running`."""
    from .duck import Oracle as Duck
    from .rig_oracle import _state_table

    with Duck(state=state) as o:
        def t(name: str) -> str:
            return _state_table(o, {"state": state}, name)

        ver = int(o.scalar(f"SELECT max(version) FROM {t('rivet_schema_version' if state.startswith('postgres') else 'schema_version')}"))
        cols = [r[0] for r in o.rows(f"DESCRIBE SELECT * FROM {t('export_state')}")]
        if "prefix" in cols:
            n, empty = o.rows(f"SELECT count(*), count(*) FILTER (WHERE coalesce(prefix, '') = '') FROM {t('export_state')}")[0]
            form = f"source prefix empty on {empty} of {n} export_state rows"
        else:
            n = o.scalar(f"SELECT count(*) FROM {t('export_state')}")
            form = f"export_state keyed by name alone ({n} rows, no prefix column)"
        running = o.scalar(f"SELECT count(*) FROM {t('run_status')} WHERE status = 'running'")
    return ver, f"schema v{ver}, {form}, {running} run(s) left `running`"


class _Watch:
    """A reader of one state DB, polled for the runs it holds as `running`."""

    def __init__(self, state: str):
        self.state, self.duck = state, None
        if state.startswith("postgres"):
            from .duck import Oracle as Duck

            self.duck = Duck(state=state)

    def __enter__(self) -> "_Watch":
        return self

    def __exit__(self, *_exc) -> None:
        if self.duck is not None:
            self.duck.close()

    def running(self) -> int:
        q = "SELECT count(*) FROM run_status WHERE status = 'running'"
        try:
            if self.duck is not None:
                return int(self.duck.db.sql(f"SELECT * FROM postgres_query('st', '{q.replace(chr(39), chr(39) * 2)}')").fetchone()[0])
            import sqlite3

            with closing(sqlite3.connect(f"file:{self.state}?mode=ro", uri=True, timeout=1)) as con:
                return int(con.execute(q).fetchone()[0])
        except Exception:  # noqa: BLE001 — a state not readable yet holds no run
            return 0


def _who(olds: list[tuple[str, Path]]) -> str:
    """The cell's old side: one release, or the ladder."""
    return f"v{olds[0][0]}" if len(olds) == 1 else "ladder " + ">".join(v for v, _ in olds)


def _tag(olds: list[tuple[str, Path]]) -> str:
    """A short identifier of the cell's old side."""
    return olds[0][0].replace(".", "") if len(olds) == 1 else "lad"


def _strategy(p: Proc) -> list[str]:
    """The `Strategy:` lines a `rivet check` printed."""
    return [ln.strip() for ln in p.out.splitlines() if ln.strip().startswith("Strategy:")]


def config_row(led: Ledger, row: tuple, name: str, e, ver: str, old: Path) -> None:
    """This build's `check` and `plan` over the config the old release ran, against the old release's own answers."""
    was = {v: e.rivet(old, v, "-c", "c.yaml") for v in ("check", "plan")}
    now = {v: e.rivet(rivet_bin(), v, "-c", "c.yaml") for v in ("check", "plan")}
    why = next((now[v].why for v in ("check", "plan") if not now[v].ok and was[v].ok), "")
    bad = config_finding({v: (p.ok, _strategy(p)) for v, p in was.items()}, {v: (p.ok, _strategy(p)) for v, p in now.items()}, why)
    if bad:
        return led.failed(*row, f"{name} config: config no longer loads — {bad}", "config no longer loads")
    led.passed(*row, f"{name} config: this build checks and plans the v{ver} config as v{ver} does "
                     f"(check exit {now['check'].returncode}, {len(_strategy(now['check']))} strategies; plan exit "
                     f"{was['plan'].returncode} then {now['plan'].returncode})", "config")


def way_back(led: Ledger, row: tuple, name: str, e, prev: Path, before: int, after: int, out: Path) -> None:
    """KNOWN GAP, pinned: the previous release refuses the state this build migrated. Rewrite when a downgrade path lands."""
    from .upgrade import _part_stats

    gap = f"{name} way back (KNOWN GAP: no downgrade path)"
    if before == after:
        return led.skipped(*row, f"{gap}: this build left the state at schema v{after}, the previous release's own", "same schema")
    held = _part_stats(out)
    p = e.rivet(prev, "run", "-c", "c.yaml")
    if p.returncode == 5 and "[RIVET_STATE_SCHEMA_NEWER]" in p.stderr and _part_stats(out) == held:
        return led.passed(*row, f"{gap}: the previous release refuses the v{after} state with RIVET_STATE_SCHEMA_NEWER "
                                "(exit 5) and writes no part", "way back")
    led.failed(*row, f"{gap}: the previous release on a v{after} state answered exit {p.returncode}, not exit 5 "
                     f"RIVET_STATE_SCHEMA_NEWER — if a downgrade path landed, rewrite this cell to grade it: "
                     f"{p.why}", "way back")


def batch_cell(led: Ledger, olds: list[tuple[str, Path]], prev: Path | None, root: Path, engine: str, url: str,
               shape: Shape, state_url: str) -> None:
    """One batch shape: each old release runs it, then this build twice, rows added before every run."""
    from .upgrade import _case, _declared_names, _Env, _part_stats

    store = "pg-state" if state_url else "sqlite"
    row, name = (engine, "-", SCEN, store), f"field[{_who(olds)}][{engine}/{shape.name}/{store}]"
    tag = f"{engine[:2]}{_tag(olds)}s{SHAPES.index(shape)}{store[:2]}_{os.getpid()}"
    if state_url:
        state_url = isolate_state_db(state_url, tag) or ""
        if not state_url:
            return led.failed(*row, f"{name}: could not create a fresh Postgres state DB for the old release", "no state db")
    src = _Table(engine, url, _case(engine, f"fs_{tag}"))

    def fail(kind: str, why: str) -> None:
        led.failed(*row, f"{name}: {kind} — {why}", kind)

    try:
        if not src.add(KILL_ROWS if shape.killed else ROWS):
            return fail("setup", "the seed failed")
        e = _Env(olds[0][1], root, engine, url, src.name, shape.mode, state_url)
        cfg, out = e.dir / "c.yaml", e.dir / "output"
        text = edited(cfg.read_text(), shape.edits) if e.init.ok else None
        if text is None:
            return fail("setup", f"v{olds[0][0]} init wrote no config this shape can be made from: {e.init.why if not e.init.ok else shape.edits}")
        cfg.write_text(text)
        state = state_url or str(e.dir / ".rivet_state.db")
        by_old: dict = {}
        if shape.killed:
            p = e.rivet_killed(olds[0][1], "run", "-c", "c.yaml", when=lambda: len(_part_stats(out)) > 1)
            by_old = _part_stats(out)
            if p.returncode != -9 or len(by_old) < 2:
                return fail("setup", f"the kill missed: v{olds[0][0]} ended with exit {p.returncode} and {len(by_old)} parts")
        else:
            for i, (ver, b) in enumerate(olds * 2 if len(olds) == 1 else olds):
                if i and not src.add(STEP):
                    return fail("setup", "the source insert failed")
                p = e.rivet(b, "run", "-c", "c.yaml")
                if not p.ok:
                    return fail("setup", f"v{ver} failed its own run {i + 1}: {p.why}")
        ver0, wrote = state_facts(state)
        config_row(led, row, name, e, *olds[-1])
        said: list[str] = []
        for n in (1, 2):
            seen, had = _manifests(out), src.n
            if not (shape.killed and n == 1) and not src.add(STEP):
                return fail("setup", "the source insert failed")
            p = e.rivet(rivet_bin(), "run", "-c", "c.yaml")
            if not p.ok and shape.killed and n == 1 and "--resume" in p.stderr:
                said.append("the plain run refuses the unfinished run and names `--resume`")
                p = e.rivet(rivet_bin(), "run", "-c", "c.yaml", "--resume")
            if not p.ok:
                return fail("refuses its own old state", f"run {n} of this build: {p.why}")
            want = src.rows()
            if shape.delta:
                bad = finding(want, _id_v(_parts(out, _manifests(out))), _id_v(_parts(out, _manifests(out) - seen)),
                              [r for r in want if r[0] > had])
            elif shape.killed and n == 1:
                first = min(by_old, key=lambda f: by_old[f][1])
                bad = finding(want, _id_v(_parts(out, _manifests(out))))
                if first not in _declared_names(out) or _part_stats(out).get(first) != by_old[first]:
                    bad = ("restart", f"the first part the killed run committed ({first}) is not declared as it was written")
            else:
                bad = finding(want, _id_v(_parts(out, _manifests(out), newest=True)))
            if bad:
                return fail(bad[0], f"run {n} of this build: {bad[1]}")
            said.append(f"run {n} declared exactly the {len(want) - had} new rows" if shape.delta else
                        f"run {n} left {len(want)} rows declared, each once")
        ver1, _ = state_facts(state)
        led.passed(*row, f"{name}: the old release wrote {wrote}; this build (now schema v{ver1}): {'; '.join(said)}", shape.name)
        if prev is not None:
            way_back(led, row, name, e, prev, ver0, ver1, out)
    finally:
        src.drop()


def _ops(c: int) -> list[tuple]:
    """Cycle `c`'s changes of one stream table: three inserts, an update and a delete, on ids no other cycle touches."""
    return [*(("ins", SEED + 10 * c + i, c, "2026-03-01 10:00:00") for i in (1, 2, 3)), ("set_v", c, -c), ("del", SEED - c)]


def _touched(c: int) -> list[int]:
    """The ids cycle `c` changes."""
    return sorted(op[1] for op in _ops(c))


def _of(parts: list[str], table: str, snapshot: bool | None = None) -> list[str]:
    """The parts of `table` (its snapshot leg only, its stream only, or both)."""
    mine = re.compile(rf"/(?:[^/]*[._])?{re.escape(table.lower())}/")
    return [p for p in parts if mine.search(p.lower()) and (snapshot is None or ("/snapshot/" in p) == snapshot)]


def _stream_state(o, src, engine: str, parts: list[str]) -> tuple[str, int]:
    """(`id:v:epoch` of the latest image per key over a table's parts, baseline rows and change events declared twice)."""
    from .rig_oracle import pos_order
    from .upgrade_cdc_load import _join

    cols = f"{src.bq_id} AS i, {src.bq_v} AS v, {src.bq_epoch} AS e"
    legs, dup = [], 0
    snap = [p for p in parts if "/snapshot/" in p]
    stream = [p for p in parts if "/snapshot/" not in p]
    if snap:
        legs.append(f"SELECT {cols}, NULL AS pos, 0 AS seq, 'snapshot' AS op FROM read_parquet({snap})")
        dup = o.scalar(f"SELECT count(*) - count(DISTINCT {src.bq_id}) FROM read_parquet({snap})")
    if stream:
        rel = f"read_parquet({stream}, union_by_name = true)"
        legs.append(f"SELECT {cols}, {pos_order(engine)} AS pos, __seq AS seq, __op AS op FROM {rel}")
        dup += o.scalar(f"SELECT count(*) - count(DISTINCT (__op, __pos, __seq, {src.bq_id})) FROM {rel}")
    if not legs:
        return "", 0
    live = o.rows(f"SELECT i, v, e FROM (SELECT *, row_number() OVER (PARTITION BY i ORDER BY pos IS NOT NULL DESC, "
                  f"pos DESC, seq DESC) AS rn FROM ({' UNION ALL '.join(legs)})) WHERE rn = 1 AND op <> 'delete'")
    return _join(live), int(dup)


def stream_cell(led: Ledger, olds: list[tuple[str, Path]], root: Path, engine: str, url: str, baseline: str,
                how: str, state_url: str) -> None:
    """The client's CDC shape with one run killed, then this build twice; `how`: "" (the bucket as run), "cleaned" (emptied as a `cleanup_source` load leaves it) or "killed-baseline"."""
    from .duck import Oracle as Duck
    from .scenarios import _manifest_declared_parts, pull_prefix, store_up
    from .upgrade import _Env
    from .upgrade_cdc_load import CDC_LOAD_ENGINES, isolated

    store = "pg-state" if state_url else "sqlite"
    variant, kill = f"stream-{baseline}{'/' + how if how else ''}", "baseline" if how == "killed-baseline" else "change"
    row, name = (engine, "-", SCEN, store), f"field[{_who(olds)}][{engine}/{variant}/{store}]"
    if not store_up("gcs") or not _bucket():
        return led.skipped(*row, f"{name}: the GCS emulator on :4443 is down", "no fake-gcs")
    tag = f"{engine[:2]}{_tag(olds)}{baseline[0]}{(how or 'as-run')[0]}{store[:2]}{os.getpid()}"
    if state_url:
        state_url = isolate_state_db(state_url, tag) or ""
        if not state_url:
            return led.failed(*row, f"{name}: could not create a fresh Postgres state DB for the old release", "no state db")
    src = CDC_LOAD_ENGINES[engine][0](url, None, tag)
    tables = [src.name(k) for k in range(STREAM_TABLES)]
    big, pfx = tables[-1], f"field/{tag}"

    def fail(kind: str, why: str) -> None:
        led.failed(*row, f"{name}: {kind} — {why}", kind)

    try:
        with src:
            for t in tables:
                if not (src.create(t) and src.apply(t, [("ins", i, i, "2026-01-01 10:00:00") for i in range(1, SEED + 1)])):
                    return fail("setup", f"could not create and seed {t}")
            if kill == "baseline" and not src.sql(["SET SESSION cte_max_recursion_depth = 1000000"] * (engine == "mysql")
                                                  + [BULK[engine].format(t=big, lo=1001, hi=1000 + BIG)]):
                return fail("setup", f"could not fill {big}")
            e = _Env(olds[0][1], root, engine, src.rivet_url, tag, "cdc", state_url, select=("--include", *tables),
                     extra=(*src.init_args, "--gcs-bucket", BUCKET, "--bigquery-project", "field-state",
                            "--bigquery-dataset", "field_state"), env=src.env)
            cfg = e.dir / "c.yaml"
            text = isolated(cfg.read_text(), pfx, src.slot) if e.init.ok else None
            text = with_endpoint(text) if text else None
            if text and baseline == "snapshot":
                text = as_snapshot_streams(text)
            if text and kill == "baseline":
                text = edited(text, ((r"chunk_size: \d+", f"chunk_size: {PAGE}"),))
            if text is None:
                return fail("setup", f"v{olds[0][0]} init wrote no config this shape can be made from: {e.init.why if not e.init.ok else 'an edit named nothing'}")
            cfg.write_text(text)
            state = state_url or str(e.dir / ".rivet_state.db")
            live, cycle, pulls = list(tables), 0, 0

            def change() -> bool:
                nonlocal cycle
                cycle += 1
                return all(src.apply(t, _ops(cycle)) for t in tables)

            def pulled() -> Path:
                nonlocal pulls
                pulls += 1
                dl = e.dir / f"dl{pulls}"
                pull_prefix("gcs", BUCKET, pfx, dl)
                return dl

            if kill == "baseline":
                def legs(listing: dict[str, str]) -> list[str]:
                    return [n for n in _of(list(listing), big, True) if n.endswith(".parquet")]

                p = e.rivet_killed(olds[0][1], "run", "-c", "c.yaml", when=lambda: len(legs(_objects(pfx))) > 1)
                left = _settled(pfx)
                done = [t for t in tables if any(n.lower().endswith("/snapshot/_success") for n in _of(list(left), t, True))]
                if p.returncode != -9 or big in done or len(done) != len(tables) - 1 or len(legs(left)) < 2:
                    return fail("setup", f"the kill missed: v{olds[0][0]} ended with exit {p.returncode}, baselines finished: {done}")
                first = min(legs(left), key=lambda n: int(left[n]))
                seen: set[str] = set()
                owed: list[int] = []
                read = None
            else:
                p = e.rivet(olds[0][1], "run", "-c", "c.yaml")
                if not p.ok:
                    return fail("setup", f"v{olds[0][0]} failed its own baseline run: {p.why}")
                for ver, b in olds:
                    if not change():
                        return fail("setup", "the source change failed")
                    p = e.rivet(b, "run", "-c", "c.yaml")
                    if not p.ok:
                        return fail("setup", f"v{ver} failed its own change run: {p.why}")
                live.remove(tables[1])
                cfg.write_text(without_table(cfg.read_text(), tables[1]))
                done, left = list(tables), _objects(pfx)
                read = pulled()
                seen = _manifests(read)
                if how == "cleaned":
                    _forget(pfx)
                    left = {}
                with _Watch(state) as watch:
                    held = watch.running()
                    if not change():
                        return fail("setup", "the source change failed")
                    p = e.rivet_killed(olds[-1][1], "run", "-c", "c.yaml", when=lambda: watch.running() > held)
                    if p.returncode != -9 or watch.running() <= held:
                        return fail("setup", f"the kill missed: v{olds[-1][0]} ended with exit {p.returncode}: {p.why}")
                owed = _touched(cycle)
            ver0, wrote = state_facts(state)
            ckpt = sorted(str(f.relative_to(e.dir)) for f in e.dir.rglob("*.ckpt"))
            config_row(led, row, name, e, *olds[-1])
            said: list[str] = []
            for n in (1, 2):
                if n == 2:
                    if not change():
                        return fail("setup", "the source change failed")
                    owed = _touched(cycle)
                p = e.rivet(rivet_bin(), "run", "-c", "c.yaml")
                if not p.ok:
                    return fail("refuses its own old state", f"run {n} of this build: {p.why}")
                now = _objects(pfx)
                again = rebaselined(left, now, done)
                if again:
                    return fail("re-baseline", f"run {n} of this build wrote {len(again)} object(s) under the snapshot of a "
                                               f"table whose baseline was finished; first: {again[0]}")
                dl = pulled()
                fresh = _parts(dl, _manifests(dl) - seen)
                if kill == "baseline" and n == 1 and (now.get(first) != left[first] or not [
                        f for f in _manifest_declared_parts(dl) if f.endswith(first[len(pfx):])]):
                    return fail("restart", f"the first part the killed baseline committed ({first}) is not declared as it was written")
                with Duck(**src.attach) as o:
                    for t in tables:
                        got = sorted(int(r[0]) for r in o.rows(f"SELECT DISTINCT {src.bq_id} FROM read_parquet({_of(fresh, t, False)}, union_by_name = true)")) \
                            if _of(fresh, t, False) else []
                        want = owed if t in live else []
                        if got != want:
                            kind = "loss" if set(want) - set(got) else "duplicate"
                            return fail(kind, f"run {n} of this build delivered changes of ids {got[:12]} for {t}, the source changed {want}")
                    declared = _manifest_declared_parts(dl) + (_manifest_declared_parts(read) if how == "cleaned" else [])
                    for t in live:
                        image, twice = _stream_state(o, src, engine, _of(declared, t))
                        truth = src.fp(o, t)
                        if twice:
                            return fail("duplicate", f"after run {n} of this build {twice} change event(s) of {t} are declared twice")
                        if not truth or image != truth:
                            return fail("loss", f"after run {n} of this build the latest image of {t} differs from the source: "
                                                f"{len(image.split(','))} live keys against {len(truth.split(','))}")
                left, done, seen = now, list(tables), _manifests(dl)
                said.append(f"run {n} delivered the changes of ids {owed or 'none'} on {len(live)} table(s)")
            ver1, _ = state_facts(state)
            took = (f"the baseline was killed in {big} after {len(tables) - 1} tables; " if kill == "baseline" else
                    f"{tables[1]} left `tables:` after its baseline; "
                    + ("the bucket was emptied as a `cleanup_source` load leaves it; " if how == "cleaned" else ""))
            led.passed(*row, f"{name}: the old release wrote {wrote}, checkpoint {ckpt or 'in the source'}; {took}this build (now schema "
                             f"v{ver1}) wrote nothing under a finished `snapshot/`; {'; '.join(said)}; each table's latest image "
                             "equals the source", variant)
    except RuntimeError as err:
        fail("setup", str(err))
    finally:
        for t in tables:
            src.drop(t)
        _forget(pfx)


def signature(p: Proc) -> str:
    """What a cron wrapper reads from one step: its exit code and the verdict of each line that carries one."""
    said = Counter(" ".join(g for g in m.groups() if g) for ln in p.out.splitlines() if (m := VERDICT.match(ln)))
    return f"exit {p.returncode}" + "".join(f"; {n}x {v}" for v, n in sorted(said.items()))


def differences(was: dict[tuple[str, str], str], now: dict[tuple[str, str], str]) -> list[tuple[str, bool]]:
    """(`<situation> · <step>: <was> -> <is>`, accepted) for every step this build answers differently."""
    rows = [f"{sit} · {step}: {was[sit, step]} -> {now[sit, step]}" for sit, step in was if was[sit, step] != now[sit, step]]
    return [(r, r in ACCEPTED_DIFFERENCES) for r in rows]


class _World:
    """One warehouse deployment of the client's shape: init's CDC config with a `load:` block, under its own prefix, dataset, state and replica id."""

    def __init__(self, root: Path, tag: str, n: int, target: str, old: Path, src, tables: list[str]):
        from ..pytools.registry import bq_tmp
        from . import gcp
        from .upgrade import _Env
        from .upgrade_cdc_load import isolated
        from .upgrade_matrix import CH_PASSWORD, CH_URL, CH_USER, _ch

        self.target, self.tables, self.src, self.old = target, tables, src, old
        self.db, self.pfx = bq_tmp(f"fs_{tag}"), f"field/{tag}"
        if target == "bigquery":
            self.proj, self.bucket = os.environ["BQ_ORACLE_PROJECT"], os.environ["BQ_ORACLE_BUCKET"]
            gcp.bq_ensure_dataset(self.proj, self.db)
            wh = ("--gcs-bucket", self.bucket, "--bigquery-project", self.proj, "--bigquery-dataset", self.db)
        else:
            _ch(f"CREATE DATABASE IF NOT EXISTS {self.db}")
            wh = ("--gcs-bucket", BUCKET, "--clickhouse-url", CH_URL, "--clickhouse-database", self.db, "--clickhouse-user", CH_USER)
        self.e = _Env(old, root, "mysql", src.rivet_url, tag, "cdc", "", select=("--include", *tables),
                      extra=(*src.init_args, *wh), env={"CLICKHOUSE_PASSWORD": CH_PASSWORD})
        text = isolated((self.e.dir / "c.yaml").read_text(), self.pfx, None) if self.e.init.ok else None
        if text and target == "clickhouse":
            text = with_endpoint(text)
        text = edited(text, ((r"server_id: \d+", f"server_id: {4300 + n}"),)) if text else None
        self.why = "" if text else (self.e.init.why if not self.e.init.ok else "its config could not be isolated")
        if text:
            (self.e.dir / "c.yaml").write_text(text)

    def cycle(self, binary: Path) -> dict[str, Proc]:
        """`run`, `load` and `compact` in turn, each whatever the one before answered."""
        return {verb: self.e.rivet(binary, verb, "-c", "c.yaml") for verb in ("run", "load", "compact")}

    def plain(self, text: str) -> str:
        """`text` without this world's own names."""
        for i, t in enumerate(self.tables):
            text = text.replace(t, f"<table {i}>")
        return text.replace(self.db, "<dataset>").replace(getattr(self, "proj", self.db), "<project>")

    def drop_table(self, table: str) -> None:
        """Drop `table` from the warehouse, as an operator's mistake would."""
        from . import gcp
        from .upgrade_matrix import _ch

        if self.target == "bigquery":
            gcp.bq_delete_table(self.proj, self.db, table)
        else:
            _ch(f"DROP TABLE IF EXISTS {self.db}.{table}")

    def grade(self, table: str) -> tuple[str, str]:
        """('pass'|'fail'|'skip'|'error', detail): the warehouse table against the source, by the rig oracle (read twice before 'error')."""
        import yaml

        from .rig_oracle import grade_load
        from .upgrade_matrix import CH_PASSWORD, verdict_status

        cfg = yaml.safe_load((self.e.dir / "c.yaml").read_text())
        url = self.src.rivet_url
        spec = {"engine": "mysql", "url": url, "database": url.rsplit("/", 1)[-1].split("?")[0], "table": table, "query": None,
                "mode": "cdc", "key": ["id"], "overrides": {}, "cursor_expr": None, "state": str(self.e.dir / ".rivet_state.db"),
                "load": cfg["load"], "password": CH_PASSWORD if self.target == "clickhouse" else "",
                "export": next(x["name"] for x in cfg["exports"] if x.get("mode") == "cdc"),
                "verb": "compact" if self.target == "bigquery" else "load", "delta": False, "snapshot": True, "capture_instance": None}
        for attempt in (1, 2):
            try:
                return verdict_status(grade_load(spec))
            except Exception as err:  # noqa: BLE001 — an oracle that could not read is NOT GRADED, never a pass
                if attempt == 2:
                    return "error", f"{type(err).__name__}: {str(err)[:300]}"
        raise AssertionError("unreachable")

    def close(self) -> None:
        from . import gcp
        from .upgrade_matrix import _ch

        if self.target == "bigquery":
            gcp.bq_delete_dataset(self.proj, self.db)
            gcp.gcs_delete_prefix(self.bucket, f"{self.pfx}/")
        else:
            _ch(f"DROP DATABASE IF EXISTS {self.db}")
            _forget(self.pfx)


def _warehouse_down(target: str) -> str:
    """Why `target` cannot be loaded into here, or ''."""
    from .scenarios import store_up
    from .upgrade_matrix import _ch

    if target == "bigquery":
        return "" if os.environ.get("BQ_ORACLE_PROJECT") and os.environ.get("BQ_ORACLE_BUCKET") else "no BQ_ORACLE_PROJECT / BQ_ORACLE_BUCKET"
    if _ch("SELECT 1") is None:
        return "ClickHouse on :8123 is down"
    return "" if store_up("gcs") and _bucket() else "the GCS emulator on :4443 is down"


def _together(jobs: list[Callable[[], object]]) -> list:
    """Run `jobs` side by side; their results in order."""
    from concurrent.futures import ThreadPoolExecutor

    if not jobs:
        return []
    with ThreadPoolExecutor(max_workers=min(len(jobs), 5)) as ex:
        return list(ex.map(lambda j: j(), jobs))


def contract_cell(led: Ledger, releases: list[tuple[str, Path]], root: Path, url: str, target: str) -> None:
    """What a cron wrapper reads from each step of the cycle: every old release against this build, on the same history."""
    from .upgrade_cdc_load import MySQL

    row = ("mysql", "-", SCEN, f"contract-{target}")
    down = _warehouse_down(target)
    if down:
        return led.skipped(*row, f"field[contract/mysql/{target}]: {down}", down)
    tag = f"ct{target[:2]}{os.getpid()}"
    src = MySQL(url, None, tag)
    tables = [src.name(0), src.name(1)]
    worlds: dict[str, tuple[_World, _World]] = {}
    try:
        with src:
            for t in tables:
                if not (src.create(t) and src.apply(t, [("ins", i, i, "2026-01-01 10:00:00") for i in range(1, 6)])):
                    return led.failed(*row, f"field[contract/mysql/{target}]: setup — could not create and seed {t}", "setup")
            for i, (ver, old) in enumerate(releases):
                pair = tuple(_World(root, f"{tag}v{i}{side}", 2 * i + j, target, old, src, tables) for j, side in enumerate("wi"))
                if pair[0].why:
                    led.skipped(*row, f"field[v{ver}][contract/mysql/{target}]: v{ver} init writes no config for this warehouse: {pair[0].why}", "init refused")
                    for w in pair:
                        w.close()
                    continue
                worlds[ver] = pair
            olds = dict(releases)
            first = _together([lambda w=w, v=v: (v, w.cycle(olds[v])) for v, pair in worlds.items() for w in pair])
            for ver, steps in first:
                bad = next((f"`{verb}` {p.why}" for verb, p in steps.items() if not p.ok), None)
                if bad and ver in worlds:
                    led.failed(*row, f"field[v{ver}][contract/mysql/{target}]: setup — v{ver} failed its own baseline cycle: "
                               f"{worlds[ver][0].plain(bad)}", "setup")
                    for w in worlds.pop(ver):
                        w.close()
            seen: dict[str, tuple[dict, dict]] = {v: ({}, {}) for v in worlds}
            errors: dict[tuple[str, str, str], str] = {}
            situations = (
                ("a clean cycle", [("ins", 11, 1, "2026-02-01 10:00:00"), ("ins", 12, 1, "2026-02-01 10:00:00"), ("set_v", 1, -1), ("del", 5)], False),
                ("nothing new", [], False),
                ("a warehouse table dropped", [("ins", 21, 2, "2026-02-02 10:00:00"), ("set_v", 2, -2)], True),
            )
            for sit, ops, dropped in situations:
                if dropped:
                    _together([lambda w=w: w.drop_table(tables[1]) for pair in worlds.values() for w in pair])
                if ops and not all(src.apply(t, ops) for t in tables):
                    return led.failed(*row, f"field[contract/mysql/{target}]: setup — the source change failed", "setup")
                done = _together([lambda w=w, v=v, b=b, k=k: (v, k, w.cycle(b)) for v, pair in worlds.items()
                                  for k, (w, b) in enumerate(zip(pair, (olds[v], rivet_bin())))])
                for ver, k, steps in done:
                    seen[ver][k].update({(sit, verb): signature(p) for verb, p in steps.items()})
                    errors.update({(ver, sit, verb): f"{'is' if k else 'was'}: {worlds[ver][k].plain(p.why)[:260]}"
                                   for verb, p in steps.items() if not p.ok and (k or (ver, sit, verb) not in errors)})
                if sit == "a clean cycle":
                    for ver, graded in _together([lambda v=v, w=pair[1]: (v, [(w.plain(t), *w.grade(t)) for t in tables])
                                                  for v, pair in worlds.items()]):
                        for t, status, detail in graded:
                            said = f"`{t}` after this build's clean cycle on v{ver}'s baseline, load and compact"
                            if status == "error":
                                led.ungraded(*row, f"field[v{ver}][contract/mysql/{target}]: oracle: {said} could not be read: {detail}", "oracle")
                            elif status != "pass":
                                led.failed(*row, f"field[v{ver}][contract/mysql/{target}]: loss — {said} is not the source's: "
                                                 f"{worlds[ver][1].plain(detail)[:300]}", "loss")
            for ver, (was, now) in seen.items():
                name = f"field[v{ver}][contract/mysql/{target}]"
                diffs = differences(was, now)
                for line, accepted in diffs:
                    sit, verb = line.split(": ", 1)[0].split(" · ")
                    why = errors.get((ver, sit, verb), "")
                    if TRANSPORT.search(why):
                        led.ungraded(*row, f"{name}: infra: a step did not reach the warehouse, so its answer is not rivet's — {line} ({why})", "infra")
                    elif accepted:
                        led.passed(*row, f"{name} was -> is (accepted: {ACCEPTED_DIFFERENCES[line]}) — {line}", "accepted difference")
                    else:
                        led.failed(*row, f"{name}: contract — was -> is, not in field_state.ACCEPTED_DIFFERENCES — {line}"
                                         f"{' (' + why + ')' if why else ''}", "contract")
                same = " | ".join(f"{sit} · {step}: {sig}" for (sit, step), sig in was.items() if now[sit, step] == sig)
                led.passed(*row, f"{name}: {len(was) - len(diffs)} of {len(was)} steps answer a cron wrapper as v{ver} did"
                                 f"{'; ' + str(len(diffs)) + ' differ, listed above' if diffs else ''} — {same}", "contract")
    except RuntimeError as err:
        led.failed(*row, f"field[contract/mysql/{target}]: setup — {err}", "setup")
    finally:
        for t in tables:
            src.drop(t)
        for pair in worlds.values():
            for w in pair:
                w.close()


def empty_table_cell(led: Ledger, releases: list[tuple[str, Path]], root: Path, url: str, target: str) -> None:
    """A table EMPTY at baseline, loaded and compacted by the old release; rows arrive; this build's cycle must load them."""
    from .upgrade_cdc_load import MySQL

    row = ("mysql", "-", SCEN, f"empty-{target}")
    down = _warehouse_down(target)
    if down:
        return led.skipped(*row, f"field[mysql/empty-at-baseline/{target}]: {down}", down)
    tag = f"em{target[:2]}{os.getpid()}"
    src = MySQL(url, None, tag)
    kept, empty = src.name(0), src.name(1)
    worlds: dict[str, _World] = {}
    try:
        with src:
            if not (src.create(kept) and src.create(empty) and src.apply(kept, [("ins", i, i, "2026-01-01 10:00:00") for i in range(1, 6)])):
                return led.failed(*row, f"field[mysql/empty-at-baseline/{target}]: setup — could not create the tables", "setup")
            for i, (ver, old) in enumerate(releases):
                w = _World(root, f"{tag}v{i}", 100 + i, target, old, src, [kept, empty])
                if w.why:
                    led.skipped(*row, f"field[v{ver}][mysql/empty-at-baseline/{target}]: v{ver} init writes no config for this warehouse: {w.why}", "init refused")
                    w.close()
                    continue
                worlds[ver] = w
            olds = dict(releases)
            said: dict[str, list[str]] = {v: [] for v in worlds}
            for rows in ([], [("ins", i, i, "2026-02-01 10:00:00") for i in (1, 2, 3)]):
                if rows and not src.apply(empty, rows):
                    return led.failed(*row, f"field[mysql/empty-at-baseline/{target}]: setup — the source insert failed", "setup")
                for ver, steps in _together([lambda v=v, w=w: (v, w.cycle(olds[v])) for v, w in worlds.items()]):
                    said[ver].append(", ".join(f"{verb} exit {p.returncode}" for verb, p in steps.items()))
            bad: dict[str, str] = {}
            unread: dict[str, str] = {}
            for rows in ([("ins", i, i, "2026-02-02 10:00:00") for i in (4, 5)], []):
                if rows and not (src.apply(empty, rows) and src.apply(kept, [("ins", 11, 1, "2026-02-02 10:00:00")])):
                    return led.failed(*row, f"field[mysql/empty-at-baseline/{target}]: setup — the source insert failed", "setup")
                for ver, steps in _together([lambda v=v, w=w: (v, w.cycle(rivet_bin())) for v, w in worlds.items()]):
                    w = worlds[ver]
                    failed = next((f"this build's `{verb}` {'after the rows arrived' if rows else 'with nothing new'}: {w.plain(p.why)}"
                                   for verb, p in steps.items() if not p.ok), None)
                    for t in (empty, kept):
                        status, detail = w.grade(t) if not failed else ("pass", "")
                        if status == "error":
                            unread.setdefault(ver, f"`{w.plain(t)}` could not be read: {detail}")
                        elif status != "pass":
                            failed = f"after this build's cycle {'with the new rows' if rows else 'with nothing new'} `{w.plain(t)}` is not the source's: {w.plain(detail)[:300]}"
                    if failed:
                        bad.setdefault(ver, failed)
            for ver, w in worlds.items():
                name = f"field[v{ver}][mysql/empty-at-baseline/{target}]"
                if bad.get(ver):
                    kind = "infra" if TRANSPORT.search(bad[ver]) else "empty table"
                    (led.ungraded if kind == "infra" else led.failed)(*row, f"{name}: {kind} — {bad[ver]}", kind)
                elif unread.get(ver):
                    led.ungraded(*row, f"{name}: oracle: {unread[ver]}", "oracle")
                else:
                    led.passed(*row, f"{name}: v{ver} answered [{said[ver][0]}] on the empty baseline and [{said[ver][1]}] once rows arrived; "
                                     "this build's two cycles exit 0 and the warehouse table holds the source's rows", "empty table")
    except RuntimeError as err:
        led.failed(*row, f"field[mysql/empty-at-baseline/{target}]: setup — {err}", "setup")
    finally:
        for t in (kept, empty):
            src.drop(t)
        for w in worlds.values():
            w.close()


def _delta_families() -> list[tuple[str, str, str]]:
    """The load families that depend on what an earlier run and load left: a full load replaces its table."""
    from .upgrade_matrix import OPT_IN, rows

    return [f for f in rows() if f[2] == "Incremental" and OPT_IN.get((f[0], f[1]), "") is not None]


def cells(releases: list[tuple[str, Path]], root: Path, states: list[str]) -> list[tuple[object, str, Callable[[Ledger], None]]]:
    """Every cell as (lane, name, fn): the lane is the source server the cell changes."""
    from .upgrade import ENGINES
    from .upgrade_cdc_load import CDC_LOAD_ENGINES
    from .upgrade_matrix import matrix_lane_cells

    out: list[tuple[object, str, Callable[[Ledger], None]]] = []
    prev = releases[-1][1] if releases else None
    sides = [[r] for r in releases] + ([releases] if len(releases) > 1 else [])
    for engine in ENGINES:
        var = f"RIVET_ORACLE_{engine.upper()}_URL"
        url = os.environ.get(var, "")
        if not url:
            out.append((None, f"field[{engine}/batch]", lambda led, e=engine, v=var: led.skipped(
                e, "-", SCEN, "-", f"field[{e}/batch]: no {v}", "no url")))
            continue
        for olds in sides:
            for shape in SHAPES:
                if len(olds) > 1 and not shape.delta:
                    continue
                for st in states:
                    out.append((server_of(url), f"field[{_who(olds)}][{engine}/{shape.name}/{'pg-state' if st else 'sqlite'}]",
                                lambda led, o=olds, e=engine, u=url, s=shape, st=st: batch_cell(
                                    led, o, prev if len(o) > 1 else None, root, e, u, s, st)))
    for engine, (_, envs, _) in CDC_LOAD_ENGINES.items():
        if not all(os.environ.get(v) for v in envs):
            out.append((None, f"field[{engine}/stream]", lambda led, e=engine, envs=envs: led.skipped(
                e, "-", SCEN, "-", f"field[{e}/stream]: no {' / '.join(envs)}", "no url")))
            continue
        url = os.environ[envs[0]]
        recipes = [("recipes", "cleaned"), ("recipes", ""), ("recipes", "killed-baseline")] if engine in RECIPE_ENGINES else []
        kinds = [*recipes, ("snapshot", "")]
        for olds in sides:
            for baseline, how in kinds:
                if len(olds) > 1 and (baseline, how) != kinds[0]:
                    continue
                for st in states:
                    label = f"stream-{baseline}{'/' + how if how else ''}"
                    out.append((server_of(url), f"field[{_who(olds)}][{engine}/{label}/{'pg-state' if st else 'sqlite'}]",
                                lambda led, o=olds, e=engine, u=url, b=baseline, h=how, st=st: stream_cell(
                                    led, o, root, e, u, b, h, st)))
    mysql_cdc = os.environ.get("RIVET_CDC_MYSQL_URL", "")
    for target in ("clickhouse", "bigquery"):
        for what, cell in (("contract", contract_cell), ("empty-at-baseline", empty_table_cell)):
            name = f"field[{what}/mysql/{target}]"
            if not mysql_cdc:
                out.append((None, name, lambda led, n=name, t=target, w=what: led.skipped(
                    "mysql", "-", SCEN, f"{w.split('-')[0]}-{t}", f"{n}: no RIVET_CDC_MYSQL_URL", "no url")))
                continue
            out.append((server_of(mysql_cdc), name, lambda led, c=cell, t=target: c(led, releases, root, mysql_cdc, t)))
    for ver, old in releases[:-1]:
        label = f"field[v{ver}]"
        out += [(lane, f"{label}[load]", fn) for lane, fn in matrix_lane_cells(
            old, root / f"load-{ver}", families=_delta_families(), scen=SCEN, label=label, own_refusal_skips=True)]
    return out


def verify_upgrade_from_field_state(led: Ledger, only: list[str] | None = None) -> None:
    """Each release a client still runs writes its state by real runs; this build carries it on."""
    led.phase("Upgrade from a field state — an old release's own config, state and checkpoint, carried on")
    releases = old_releases(led)
    if not releases:
        return
    t0 = time.monotonic()
    root = Path(tempfile.mkdtemp(prefix="rivet-oracle-field-"))
    states = [""] + ([os.environ["RIVET_GATE_STATE_URL"]] if os.environ.get("RIVET_GATE_STATE_URL") else [])
    led.passed("all", "-", SCEN, "-", "field releases: " + ", ".join(f"v{v} ({b})" for v, b in releases)
               + f"; state backends: sqlite{' and Postgres' if len(states) > 1 else ' only (no RIVET_GATE_STATE_URL)'}", "binaries")
    def guarded(name: str, fn: Callable[[Ledger], None]) -> Callable[[Ledger], None]:
        def cell(sub: Ledger) -> None:
            try:
                fn(sub)
            except Exception as err:  # noqa: BLE001 — a cell that could not grade is a red row, and its lane goes on
                import traceback

                traceback.print_exc()
                sub.failed("all", "-", SCEN, "-", f"{name}: harness error — {type(err).__name__}: {str(err).splitlines()[0][:300] if str(err) else ''}", "harness error")
        return cell

    todo = [(lane, guarded(name, fn)) for lane, name, fn in cells(releases, root, states) if not only or any(o in name for o in only)]
    run_lanes(led, todo)
    led.ok(f"upgrade from a field state: {len(todo)} cells in {time.monotonic() - t0:.0f}s")


def _self_test() -> None:
    """The graders on fixtures: each finding kind is the one row that fails, and the config edits are strict."""
    src = [(i, i) for i in range(1, 11)]
    old, new = src[:8], src[8:]
    assert finding(src, src, new, new) is None and finding(src, src) is None
    assert finding(src, src + src, src, new)[0] == "restart", finding(src, src + src, src, new)
    assert finding(src, src + new[:1], new + new[:1], new)[0] == "duplicate"
    assert finding(src, old + new[:1], new[:1], new)[0] == "loss"
    assert finding(src, src + src[:1])[0] == "duplicate" and finding(src, old)[0] == "loss"
    assert finding(src, [*old, (9, 9), (10, -10)])[0] == "loss"
    before = {"p/cdc/a/snapshot/x.parquet": "1", "p/cdc/a/snapshot/_SUCCESS": "2", "p/cdc/a/cdc-1.parquet": "3"}
    assert rebaselined(before, {**before, "p/cdc/a/cdc-2.parquet": "9"}, ["a"]) == []
    assert rebaselined(before, {**before, "p/cdc/a/snapshot/y.parquet": "9"}, ["a"]) == ["p/cdc/a/snapshot/y.parquet"]
    assert rebaselined(before, {**before, "p/cdc/a/snapshot/x.parquet": "7"}, ["A"]) == ["p/cdc/a/snapshot/x.parquet"]
    assert rebaselined(before, {**before, "p/cdc/b/snapshot/y.parquet": "9"}, ["a"]) == []
    ok, same = (True, ["Strategy: keyset"]), {"check": (True, ["Strategy: keyset"]), "plan": (False, [])}
    assert config_finding(same, same, "") is None
    assert "fails on this build" in config_finding({**same, "plan": ok}, same, "exit 1")
    assert "fails on this build" in config_finding(same, {**same, "check": (False, [])}, "exit 1")
    assert "differently" in config_finding(same, {**same, "check": (True, ["Strategy: full"])}, "")
    assert config_finding({**same, "check": (False, [])}, same, "") is None
    init = ("exports:\n  - name: a\n    table: a\n    mode: chunked\n    chunk_by_key: id  # keyset\n    chunk_size: 100000\n"
            "    # keyset_incremental: true # append-only\n    destination:\n      type: gcs\n      bucket: b\n      prefix: exports/a/\n"
            "  - name: b\n    table: b\n    mode: chunked\n    chunk_by_key: id\n    destination: {type: gcs, bucket: b, prefix: exports/b/}\n"
            "  - name: cdc\n    tables: [a, b]\n    mode: cdc\n    cdc: {backfill: auto, until_current: true}\n"
            "    load:\n      tables:\n        a: {partition: none}\n        b: {partition: none}\n")
    for shape in SHAPES:
        got = edited(init, shape.edits)
        assert got is not None and (got != init or not shape.edits), shape
    assert "chunk_column: id" in edited(init, SHAPES[3].edits) and "    keyset_incremental: true" in edited(init, SHAPES[0].edits)
    assert edited("mode: full\n", SHAPES[0].edits) is None and with_endpoint("mode: full\n") is None
    assert f"      bucket: b\n      endpoint: {GCS}\n" in with_endpoint(init)
    import yaml

    cut = yaml.safe_load(without_table(init, "b"))
    assert [x["name"] for x in cut["exports"]] == ["a", "cdc"] and cut["exports"][1]["tables"] == ["a"], cut
    assert cut["exports"][1]["load"]["tables"] == {"a": {"partition": "none"}}, cut
    snap = yaml.safe_load(as_snapshot_streams(init))
    assert [x["name"] for x in snap["exports"]] == ["cdc"] and snap["exports"][0]["cdc"] == {"until_current": True, "initial": "snapshot"}, snap
    ids = [i for c in range(1, 13) for i in _touched(c)]
    assert len(ids) == len(set(ids)) == 60 and min(ids) >= 1, ids
    assert [s.name for s in SHAPES if s.delta] == ["keyset-incremental", "incremental"]
    tags = ["v0.3.1", "v0.26.0", "v0.27.0", "v0.28.0", "v0.31.0", "v0.30.0", "nightly", "v1.0.0-rc1"]
    assert field_versions(tags, before="0.31.0") == ["0.27.0", "0.28.0", "0.30.0"], field_versions(tags, before="0.31.0")
    assert field_versions(tags)[0] == COMPATIBILITY_FLOOR and field_versions(["v0.28.0"])[:1] != [COMPATIBILITY_FLOOR]
    ok_run = Proc(["rivet"], 0, "── cdc ──\n  run_id: cdc_1\n  status:       success\n  rows: 3\n", "")
    assert signature(ok_run) == "exit 0; 1x status: success", signature(ok_run)
    bad = Proc(["rivet"], 1, "COMPACT OK [a]: 3 change row(s) merged\nCOMPACT SKIP [b]: no buffer\nCDC LOAD OK [a]: x\n  LOAD FAILED [b]: no Parquet\n",
               "1 of 2 compacted table(s) failed\nError: [RIVET_LOAD_X] load 'b': no Parquet URIs\n")
    assert signature(bad) == ("exit 1; 1x 1 of 2 failed; 1x CDC LOAD OK; 1x COMPACT OK; 1x COMPACT SKIP; 1x Error RIVET_LOAD_X; "
                              "1x LOAD FAILED"), signature(bad)
    was = {("a clean cycle", "load"): "exit 1; 1x LOAD FAILED", ("a clean cycle", "run"): "exit 0; 1x status: success"}
    now = {**was, ("a clean cycle", "load"): "exit 0; 1x CDC LOAD OK"}
    line = "a clean cycle · load: exit 1; 1x LOAD FAILED -> exit 0; 1x CDC LOAD OK"
    assert differences(was, was) == [] and differences(was, now) == [(line, line in ACCEPTED_DIFFERENCES)], differences(was, now)
    assert all(why.strip() for why in ACCEPTED_DIFFERENCES.values()), "an accepted difference has no reason"
    assert binary_in(Path("c"), "1.2.3").parts[-2].startswith("rivet-v1.2.3-") and field_cache(Path("c/rivet-v9/rivet")) == Path("c/field")
    names = [n for _, n, _ in cells([("0.1.0", Path("a")), ("0.2.0", Path("b"))], Path(tempfile.gettempdir()), [""])]
    assert len(names) == len([n for n in names if n.startswith("field[")]) > 0
    print("self-test ok: a re-baseline, a restart, a duplicate, a loss and a config that no longer loads each fail their row")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        _self_test()
        raise SystemExit(0)
    if sys.argv[1:2] == ["--fetch"]:
        raise SystemExit(fetch(Path(sys.argv[2])))
    _led = Ledger()
    verify_upgrade_from_field_state(_led, sys.argv[1:] or None)
    raise SystemExit(_led.report())
