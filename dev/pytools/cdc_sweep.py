"""Every CDC axis and mechanic, run for real, judged by three oracles.

The batch sibling (`extraction_sweep.py`) reconciles SOURCE / DESTINATION /
META-DB for the extraction surface. CDC needs the same treatment, a different
notion of truth, and — because a CDC run's output is a SET OF SIDECARS as much as
a set of rows — a third oracle that reads the artifacts as artifacts.

    SOURCE       the changes issued — 3 inserts, 1 update, 1 delete = 5 events,
                 known exactly because the harness issued them
    ARTIFACTS    the manifests, their per-part arithmetic, the files on disk, the
                 checkpoint. Recomputed here: `content_md5` against Python's own
                 hashlib, `size_bytes` against stat(), `rows` against the parquet
                 FOOTER. rivet does not get to be the only witness to its own
                 sidecars.
    META-DB      Postgres state backend, joined to the artifacts BY RUN ID —
                 `manifest-<run_id>.json` names the run, so every comparison is
                 per-run, not a total that two errors can cancel inside.
    DUCKDB       an independent counter over exactly the parts the manifests
                 list. Never rivet's `validate`, so a read bug cannot rubber-stamp
                 the write that produced it.

THE CONSUMER VIEW IS THE UNION OF THE MANIFEST COPIES, not the parquet on disk.
A crash leaves durable parts no manifest lists; a consumer summing manifests
(the Pro loader's reconcile does) never sees them. So completeness is asserted on
the union of `manifest-<run_id>.json`, and the orphan set is checked separately —
it must be EMPTY unless a crash created it.

AXES. Derived from `CdcExportConfig` in src/config/export.rs, not from memory;
`docs/cdc-axis-matrix.yaml` is the ledger and `tests/offline/cdc_axis_matrix_guard.rs`
fails the build when a new CDC knob or a new `cdc_*` crash hook appears without a
row saying how it is swept.

Runs against the POSTGRES state backend, like the batch sweep, because that is
what a shared deployment uses and it has already diverged from SQLite once.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "dev"))

STATE_URL = os.environ.get(
    "RIVET_SWEEP_STATE_URL", "postgresql://rivet:rivet@localhost:5433/rivet_state"
)
STATE_CONTAINER = os.environ.get("RIVET_SWEEP_STATE_CONTAINER", "rivet-postgres-state-1")
RIVET = os.environ.get("RIVET_BIN", str(ROOT / "target" / "release" / "rivet"))

#: Per-INVOCATION token folded into every export name (and, through the work dir,
#: into the PostgreSQL slot name).
#:
#: The state backend is shared and persistent, so a case that reuses its export
#: name inherits its own last resume position. `initial: snapshot` then met a
#: `export_state` row from the previous sweep with no slot to match it and — quite
#: correctly — refused to re-anchor, which read as the snapshot leg being broken.
#: rivet was right and the harness was dirty. Isolation comes from the KEY, never
#: from wiping the table.
TOKEN = os.environ.get("RIVET_CDC_SWEEP_TOKEN") or time.strftime("%H%M%S")

#: The gate's own probe workload: 3 inserts, 1 update, 1 delete.
EXPECTED_EVENTS = 5
#: The ids those changes touch. A count alone cannot tell "all five changes" from
#: "one change captured five times".
EXPECTED_IDS = {1, 2, 3}
#: op -> how many of that op the change set contains.
EXPECTED_OPS = {"insert": 3, "update": 1, "delete": 1}
#: Rows the `initial: snapshot` seed puts in the table BEFORE the anchor.
SEED_IDS = {10, 11}

ENGINE_URLS = {
    "postgres": os.environ.get("RIVET_CDC_POSTGRES_URL", ""),
    "mysql": os.environ.get("RIVET_CDC_MYSQL_URL", ""),
    "mssql": os.environ.get("RIVET_CDC_MSSQL_URL", ""),
    # Mongo CDC is a change stream, which requires a REPLICA SET — a standalone
    # mongod cannot serve one, so the URL must point at the rs member.
    "mongo": os.environ.get("RIVET_CDC_MONGO_URL", ""),
}

#: Where each engine's resume position LIVES, which decides what "no checkpoint
#: file" means for it. Not a preference — the three anchor models the CDC docs
#: name, written down so a case can assert against the right one instead of
#: assuming every engine behaves like PostgreSQL.
ANCHOR_MODEL = {
    "postgres": "server-side: the slot pins WAL at creation, so a checkpoint file is optional",
    "mysql": "client-side ONLY: the checkpoint file IS the anchor — without it a run re-anchors at 'current'",
    "mssql": "server-side floor: reads from fn_cdc_get_min_lsn when it has no position (over-reads, never skips)",
    "mongo": "client-side ONLY: the resume token lives in the checkpoint file",
}

#: `tables:` (several tables through ONE change stream). SQL Server is documented
#: as unsupported — capture instances are per-table. Mongo is `None` = the docs
#: do not say, so the sweep MEASURES it and reports rather than asserting a
#: guess.
MULTI_TABLE = {"postgres": True, "mysql": True, "mssql": False, "mongo": None}


@dataclass
class Case:
    name: str
    axis: str
    #: Extra keys folded into the `cdc:` block.
    cdc_extra: str = ""
    #: Force the `checkpoint:` key present / absent regardless of the engine's default.
    checkpoint: bool | None = None
    crash: str | None = None
    #: Run again after the crash — the recovery half of at-least-once.
    recover: bool = False
    #: Run twice with no crash — resume must add nothing.
    twice: bool = False
    #: `until_current: false` — the daemon. Bounded by `max_events` instead, with
    #: a wall-clock net so a stream that does NOT stop fails rather than hangs.
    continuous: bool = False
    #: Seed rows BEFORE the anchor, and capture them via `initial: snapshot`.
    snapshot: bool = False
    #: `tables:` — several tables, one stream.
    multi: bool = False
    fmt: str = "parquet"
    #: The remainder `max_events` deferred must arrive in a follow-up run.
    drain_after: bool = False
    notes: str = ""


@dataclass
class Result:
    engine: str
    case: str
    #: Rows DuckDB reads from exactly the parts the manifest copies list.
    dest_events: int | None = None
    #: Parts listed by any manifest copy.
    parts: int | None = None
    #: Parquet on disk that NO manifest copy lists.
    orphans: int | None = None
    #: Σ total_rows over the meta-DB rows for this case's runs.
    meta_rows: int | None = None
    validate_exit: int | None = None
    disagreements: list[str] = field(default_factory=list)
    observations: list[str] = field(default_factory=list)
    skipped: str = ""


CASES: list[Case] = [
    Case("bounded", "until_current=true", notes="the default: one bounded drain"),
    Case("bounded_twice", "resume", twice=True, notes="the second drain must add nothing"),
    Case("rollover_1", "rollover", cdc_extra=", rollover: 1", notes="a part per change"),
    Case(
        "rollover_memory_mb",
        "rollover_memory_mb",
        cdc_extra=", rollover_memory_mb: 1",
        notes="the byte-capped sibling of rollover",
    ),
    Case(
        "max_events_2",
        "max_events",
        cdc_extra=", max_events: 2",
        drain_after=True,
        notes="a soft cap: stops at the first COMMIT boundary past 2, remainder deferred not dropped",
    ),
    Case(
        "continuous",
        "until_current=false",
        continuous=True,
        cdc_extra=", until_current: false, max_events: 2",
        drain_after=True,
        notes="the daemon — no open-time bound, stopped by max_events",
    ),
    Case(
        "checkpoint_present",
        "checkpoint",
        checkpoint=True,
        twice=True,
        notes="a checkpoint file on every engine, including the one that anchors server-side",
    ),
    Case(
        "checkpoint_absent",
        "checkpoint",
        checkpoint=False,
        notes="no checkpoint file — what the engine's own anchor model then guarantees",
    ),
    Case("initial_snapshot", "initial", snapshot=True, notes="anchor -> snapshot the seed -> drain"),
    Case("multi_table", "tables", multi=True, notes="several tables through ONE stream"),
    Case("format_csv", "format", fmt="csv", notes="the text writer, not parquet's binary path"),
    Case(
        "crash_after_open",
        "crash hook",
        crash="cdc_after_open",
        recover=True,
        notes="died holding the stream, before reading anything",
    ),
    Case(
        "crash_before_resolve",
        "crash hook",
        crash="cdc_before_resolve",
        recover=True,
        notes="died before resolving the resume position",
    ),
    Case(
        "crash_flush_before_ack",
        "crash hook",
        crash="cdc_after_flush_before_ack",
        recover=True,
        notes="the classic at-least-once point: parts durable, source not acked",
    ),
    Case(
        "crash_checkpoint_before_ack",
        "crash hook",
        crash="cdc_after_checkpoint_before_ack",
        recover=True,
        notes="checkpoint advanced, ack not sent",
    ),
    Case(
        "crash_after_ack",
        "crash hook",
        crash="cdc_after_ack",
        recover=True,
        notes="source acked — the recovery must not lose what was already captured",
    ),
    Case(
        "crash_before_manifest",
        "crash hook",
        crash="cdc_before_manifest",
        recover=True,
        notes="parts written, manifest not — the orphan case",
    ),
]


# ── plumbing ──────────────────────────────────────────────────────────────────
def sh(argv: list[str], env: dict | None = None, timeout: int = 900):
    return subprocess.run(
        argv, capture_output=True, text=True, env={**os.environ, **(env or {})}, timeout=timeout
    )


def run_rivet(cfg: Path, env: dict, timeout: int = 300, extra: dict | None = None):
    """Run rivet, and return `None` if it had to be killed instead of raising.

    Every invocation is bounded and no bound is fatal. A `TimeoutExpired` from
    ONE case used to propagate out of `measure` and kill the entire sweep — three
    multi-engine passes died that way, silently, mid-run, taking every result
    already computed with them (they were only printed at the end; they are
    printed per case now, which is the other half of the fix).

    A sweep that hangs a case must report that case as hung and go on to the
    next: the whole point is to be told which mechanic misbehaves, and a harness
    that dies at the first one tells you nothing about the sixteen after it.
    """
    try:
        return sh([RIVET, "run", "-c", str(cfg)], env={**env, **(extra or {})}, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def psql_state(sql: str) -> str:
    return sh(
        ["docker", "exec", STATE_CONTAINER, "psql", "-U", "rivet", "-d", "rivet_state", "-tAc", sql]
    ).stdout.strip()


def duckdb(sql: str) -> str:
    return sh(["duckdb", "-noheader", "-list", "-c", sql]).stdout.strip()


def owned_by(export: str) -> str:
    """SQL predicate for the rows this case owns: the export, plus its
    `initial: snapshot` backfill legs.

    The snapshot leg runs as its OWN export (`<name>__snapshot_<table>`) with its
    own run id, so an equality match misses it — and the ledger-coverage oracle
    then reported the leg's perfectly-recorded parquet as a file the database had
    never heard of.

    `starts_with`, not `LIKE '<export>%'`: the case names are not prefix-free
    (`bounded` is a prefix of `bounded_twice`), so a trailing wildcard would pull
    a sibling case's runs into this one's set.
    """
    return f"(export_name = '{export}' OR starts_with(export_name, '{export}__snapshot_'))"


def as_int(s: str) -> int | None:
    try:
        return int(str(s).strip())
    except (ValueError, AttributeError):
        return None


def quoted(paths: list[Path]) -> str:
    return "[" + ",".join(f"'{p}'" for p in paths) + "]"


def reader(fmt: str, paths: list[Path]) -> str:
    return (
        f"read_parquet({quoted(paths)})" if fmt == "parquet" else f"read_csv_auto({quoted(paths)})"
    )


# ── the second table, for `tables:` ───────────────────────────────────────────
# The gate's engine spec owns ONE probe table; multi-table needs a sibling on the
# same stream. Kept here rather than pushed into the gate spec because only this
# sweep has a use for it.
SECOND_TABLE = "orc_cdc_probe2"


def _second_table_ddl(engine: str, url: str) -> bool:
    """Create + CDC-enable the sibling table. False ⇒ this engine cannot."""
    from release_oracle import cdc as gate_cdc

    if engine == "postgres":
        return gate_cdc._psql(
            url,
            sql=(
                f"DROP TABLE IF EXISTS {SECOND_TABLE};"
                f"CREATE TABLE {SECOND_TABLE} (id int PRIMARY KEY, amount numeric(18,4));"
                f"ALTER TABLE {SECOND_TABLE} REPLICA IDENTITY FULL;"
            ),
        ).ok
    if engine == "mysql":
        return gate_cdc._mysql(
            url,
            f"DROP TABLE IF EXISTS {SECOND_TABLE}; "
            f"CREATE TABLE {SECOND_TABLE} (id int PRIMARY KEY, amount decimal(18,4));",
        ).ok
    if engine == "mssql":
        return gate_cdc._sqlcmd(
            url,
            sql=(
                f"IF OBJECT_ID('dbo.{SECOND_TABLE}') IS NOT NULL DROP TABLE dbo.{SECOND_TABLE};\n"
                f"CREATE TABLE dbo.{SECOND_TABLE} (id int PRIMARY KEY, amount decimal(18,4));\n"
                f"EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='{SECOND_TABLE}',"
                f" @role_name=NULL, @capture_instance='dbo_{SECOND_TABLE}';\n"
            ),
        ).ok
    if engine == "mongo":
        return gate_cdc._mongosh(
            url, f'db.{SECOND_TABLE}.drop(); db.createCollection("{SECOND_TABLE}");'
        ).ok
    return False


def _second_table_changes(engine: str, url: str) -> None:
    from release_oracle import cdc as gate_cdc

    if engine == "postgres":
        gate_cdc._psql(url, "-c", f"INSERT INTO {SECOND_TABLE} VALUES (1,1.1),(2,2.2);")
    elif engine == "mysql":
        gate_cdc._mysql(url, f"INSERT INTO {SECOND_TABLE} VALUES (1,1.1),(2,2.2);")
    elif engine == "mssql":
        gate_cdc._sqlcmd(url, q=f"INSERT INTO dbo.{SECOND_TABLE} VALUES (1,1.1),(2,2.2);")
        time.sleep(8)
    elif engine == "mongo":
        gate_cdc._mongosh(
            url, f"db.{SECOND_TABLE}.insertMany([{{_id:1,amount:1.1}},{{_id:2,amount:2.2}}]);"
        )


def _second_table_drop(engine: str, url: str) -> None:
    from release_oracle import cdc as gate_cdc

    if engine == "postgres":
        gate_cdc._psql(url, "-c", f"DROP TABLE IF EXISTS {SECOND_TABLE};")
    elif engine == "mysql":
        gate_cdc._mysql(url, f"DROP TABLE IF EXISTS {SECOND_TABLE};")
    elif engine == "mssql":
        gate_cdc._sqlcmd(
            url,
            sql=(
                f"IF EXISTS (SELECT 1 FROM cdc.change_tables ct JOIN sys.tables t ON "
                f"ct.source_object_id=t.object_id WHERE t.name='{SECOND_TABLE}')\n"
                f"  EXEC sys.sp_cdc_disable_table @source_schema='dbo', @source_name='{SECOND_TABLE}',"
                f" @capture_instance='all';\n"
                f"IF OBJECT_ID('dbo.{SECOND_TABLE}') IS NOT NULL DROP TABLE dbo.{SECOND_TABLE};\n"
            ),
        )
    elif engine == "mongo":
        gate_cdc._mongosh(url, f"db.{SECOND_TABLE}.drop();")


# ── the `initial: snapshot` seed ──────────────────────────────────────────────
def _seed(engine: str, url: str) -> None:
    """Rows that exist BEFORE the anchor, so `initial: snapshot` has something to
    snapshot that the change stream can never produce."""
    from release_oracle import cdc as gate_cdc

    if engine == "postgres":
        gate_cdc._psql(
            url, "-c", "INSERT INTO orc_cdc_probe VALUES (10,10.1,'{\"k\":10}'),(11,11.1,'{\"k\":11}');"
        )
    elif engine == "mysql":
        gate_cdc._mysql(
            url,
            'INSERT INTO orc_cdc_probe VALUES (10,10.1,\'{"k":10}\'),(11,11.1,\'{"k":11}\');',
        )
    elif engine == "mssql":
        gate_cdc._sqlcmd(
            url, q='INSERT INTO dbo.orc_cdc_probe VALUES (10,10.1,\'{"k":10}\'),(11,11.1,\'{"k":11}\');'
        )
    elif engine == "mongo":
        gate_cdc._mongosh(
            url,
            "db.orc_cdc_probe.insertMany([{_id:10,amount:10.1,meta:{k:10}},"
            "{_id:11,amount:11.1,meta:{k:11}}]);",
        )


# ── ORACLE 1: the artifacts, recomputed ───────────────────────────────────────
@dataclass
class Copy:
    """One run-unique manifest COPY, and where it sits."""

    run_id: str
    doc: dict
    dir: Path
    #: `initial: snapshot` writes its backfill under `<dest>/snapshot/`, as a
    #: separate export with its OWN name and run id. It is a different leg with a
    #: different shape — its rows carry no `__op`, because they never were changes
    #: — so mixing it into the change-stream oracle asks the wrong question of the
    #: right data (it read a `__op` profile of all zeros off perfectly good rows).
    snapshot_leg: bool


def read_manifest_copies(out: Path) -> list[Copy]:
    """Every run-unique manifest COPY.

    The copies, never `manifest.json`: that name is last-writer-wins, so N runs
    into one prefix leave one canonical pointer and N immutable copies. A check
    that reads the canonical name sees the last run and calls it the whole story.

    A single run can leave SEVERAL copies — `tables:` writes one manifest per
    captured table, all under the same run id — so nothing here may assume the
    run id identifies a document.
    """
    found = []
    for m in sorted(out.rglob("manifest-*.json")):
        try:
            doc = json.loads(m.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        found.append(
            Copy(
                run_id=doc.get("run_id") or m.stem[len("manifest-") :],
                doc=doc,
                dir=m.parent,
                snapshot_leg="snapshot" in m.parent.parts[len(out.parts) :],
            )
        )
    return found


def oracle_artifacts(out: Path, fmt: str) -> tuple[list[str], list[Copy], int]:
    """Do the sidecars describe the bytes that are actually there?

    Every number here has an independent recomputation available, so none of them
    is taken on rivet's word: `size_bytes` against stat(), `content_md5` against
    hashlib, `rows` against the parquet footer, `row_count`/`part_count` against
    their own parts list.
    """
    problems: list[str] = []
    copies = read_manifest_copies(out)
    if not copies:
        return ["no manifest copy at all — nothing recorded what this run captured"], [], 0

    listed: list[Path] = []
    want_rows: dict[Path, int] = {}
    for c in copies:
        parts = c.doc.get("parts") or []
        if c.doc.get("part_count") != len(parts):
            problems.append(
                f"{c.run_id}: part_count={c.doc.get('part_count')} but the parts list has {len(parts)}"
            )
        rows_sum = sum(int(p.get("rows") or 0) for p in parts)
        if c.doc.get("row_count") != rows_sum:
            problems.append(
                f"{c.run_id}: row_count={c.doc.get('row_count')} but its parts sum to {rows_sum}"
            )
        for p in parts:
            f = (c.dir / p["path"]).resolve()
            if not f.exists():
                problems.append(f"{c.run_id}: the manifest lists {p['path']}, which is not on disk")
                continue
            listed.append(f)
            # Keyed by the RESOLVED PATH, never the basename: `tables:` gives every
            # table's first part the same name (`cdc-<run_id>-000000.parquet`) in
            # its own directory, so a basename key made one table's part answer for
            # the other's — and reported the manifest lying about a row count when
            # both manifests were right.
            want_rows[f] = int(p.get("rows") or 0)
            blob = f.read_bytes()
            if p.get("size_bytes") is not None and p["size_bytes"] != len(blob):
                problems.append(
                    f"{c.run_id}/{p['path']}: size_bytes={p['size_bytes']}, the file is {len(blob)}"
                )
            want = p.get("content_md5")
            if want:
                got = base64.b64encode(hashlib.md5(blob).digest()).decode()
                if got != want:
                    problems.append(
                        f"{c.run_id}/{p['path']}: content_md5 describes different bytes "
                        f"({want} recorded, {got} recomputed)"
                    )

    # The parquet FOOTER is the file's own count, written by the parquet writer
    # rather than by rivet's bookkeeping — so `rows` and the footer disagreeing
    # means the manifest is describing a file that does not hold what it says.
    if fmt == "parquet" and listed:
        for line in duckdb(
            f"SELECT file_name||'|'||num_rows FROM parquet_file_metadata({quoted(listed)})"
        ).splitlines():
            if "|" not in line:
                continue
            name, n = line.rsplit("|", 1)
            f = Path(name).resolve()
            if f in want_rows and as_int(n) != want_rows[f]:
                problems.append(
                    f"{f.name}: the manifest says {want_rows[f]} rows, the parquet footer "
                    f"says {as_int(n)}"
                )

    ext = "*.parquet" if fmt == "parquet" else "*.csv"
    on_disk = {p.resolve() for p in out.rglob(ext)}
    orphans = len(on_disk - set(listed))
    return problems, copies, orphans


def parts_of(copies: list[Copy], *, snapshot_leg: bool) -> list[Path]:
    return [
        (c.dir / p["path"]).resolve()
        for c in copies
        if c.snapshot_leg is snapshot_leg
        for p in (c.doc.get("parts") or [])
    ]


def rows_of(copies: list[Copy], *, snapshot_leg: bool) -> int:
    return sum(
        int(c.doc.get("row_count") or 0) for c in copies if c.snapshot_leg is snapshot_leg
    )


def oracle_ledger_covers_the_files(
    export: str, out: Path, fmt: str, new_runs: set[str]
) -> list[str]:
    """The DATABASE is the record; the files are what it is a record OF.

    So the direction of this check is deliberate: it starts from every data file
    ON DISK — not from the manifests — and requires the ledger to account for
    each one. A part that exists and is in no `file_log` row is a durable write
    the system has no record of, whatever the manifest beside it happens to say.

    This is the check that would have gone red before the CDC sink recorded its
    parts: the sink wrote per-part manifests and touched the state store not at
    all, so a crashed run's durable parts were known to the destination and to
    nothing else. It is also strictly stronger than comparing the ledger to the
    manifests, because the orphan a crash leaves is exactly the file no manifest
    mentions — the one case where the manifest cannot be the oracle.
    """
    ext = "*.parquet" if fmt == "parquet" else "*.csv"
    on_disk = {p.name for p in out.rglob(ext)}
    if not on_disk:
        return []
    logged = {
        line.split("|")[1]
        for line in psql_state(
            f"SELECT run_id||'|'||file_name FROM file_log WHERE {owned_by(export)}"
        ).splitlines()
        if "|" in line and line.split("|")[0] in new_runs
    }
    unrecorded = sorted(on_disk - logged)
    if unrecorded:
        return [
            f"{len(unrecorded)} durable file(s) the state DB has NO file_log row for "
            f"({unrecorded[0]}…) — the database is supposed to be the record and the manifest "
            f"its projection; a file only the destination knows about inverts that"
        ]
    return []


# ── ORACLE 2: the meta-DB, joined to the artifacts by run id ──────────────────
def oracle_metadb(
    export: str, copies: list[Copy], new_runs: set[str], crashed: set[str]
) -> tuple[list[str], list[str], int]:
    """Per RUN ID, does the ledger say what the artifacts say?

    The join key is the run id the manifest COPY carries in its own name, so this
    is never a total-vs-total comparison two errors could cancel inside. The
    `LIKE` on the export name is what reaches the `initial: snapshot` backfill,
    which runs as its own export (`<name>__snapshot_<table>`) with its own id.
    """
    problems: list[str] = []
    notes: list[str] = []
    rows = {}
    for line in psql_state(
        f"SELECT run_id||'|'||total_rows||'|'||coalesce(files_committed,-1) "
        f"FROM export_metrics WHERE {owned_by(export)}"
    ).splitlines():
        f = line.split("|")
        if len(f) == 3 and f[0] in new_runs:
            rows[f[0]] = (as_int(f[1]), as_int(f[2]))

    # `tables:` puts several manifests under ONE run id — the ledger records the
    # run, not the table, so the comparison is against the run's TOTAL.
    per_run: dict[str, tuple[int, int]] = {}
    for c in copies:
        if c.run_id not in new_runs:
            continue
        r, p = per_run.get(c.run_id, (0, 0))
        per_run[c.run_id] = (
            r + int(c.doc.get("row_count") or 0),
            p + int(c.doc.get("part_count") or 0),
        )

    total = 0
    for run_id, (m_rows, m_parts) in sorted(per_run.items()):
        rec = rows.get(run_id)
        if rec is None:
            if run_id in crashed:
                # BY DESIGN, and worth stating rather than tolerating silently:
                # `roll_all` writes a Success run-unique manifest BEFORE the ack,
                # so a crash past that point leaves durable, manifest-covered rows
                # — while the metrics row, written at finalize, never happens. The
                # ledger therefore UNDER-COUNTS a crashed run by exactly the rows
                # it made durable. Anything reconciling from the state DB alone
                # would miss them; the manifest copies are the authority.
                notes.append(
                    f"the crashed run left {m_rows} durable manifest-covered row(s) that the "
                    f"meta-DB has no metrics row for — the manifest is written before the ack, "
                    f"the metrics row at finalize"
                )
                continue
            problems.append(
                f"{run_id}: the run wrote a manifest but the meta-DB has no metrics row for it — "
                f"the artifact and the ledger disagree about whether this run happened"
            )
            continue
        meta_rows, files_committed = rec
        if meta_rows != m_rows:
            problems.append(
                f"{run_id}: meta-DB total_rows={meta_rows}, its own manifest(s) say {m_rows}"
            )
        if files_committed not in (None, -1) and files_committed != m_parts:
            problems.append(
                f"{run_id}: meta-DB files_committed={files_committed}, the manifest(s) list "
                f"{m_parts} parts"
            )
        total += meta_rows or 0
    return problems, notes, total


# ── ORACLE 3: DuckDB over exactly what the manifests list ─────────────────────
#: Where the UPDATE's after-image lives, per engine.
#:
#: The SQL engines project each source column, so `amount` is a column. Mongo's
#: CDC shape is `{__op, __pos, __seq, _id, document}` — the document is a
#: VERBATIM JSON blob, by design, so there is no `amount` column to compare and
#: the value has to be read out of the blob. Asking every engine the SQL
#: question reported the after-image MISSING on all four mongo cases while it was
#: sitting in the document exactly as it should be.
AFTER_IMAGE_EXPR = {
    "mongo": "CAST(document->>'$.amount' AS DOUBLE)",
}


def oracle_duckdb(
    listed: list[Path], fmt: str, id_col: str, engine: str
) -> tuple[list[str], int | None]:
    """Counts, the id set, and the OP PROFILE.

    A count is the weakest thing that can be checked here: five rows could be one
    change captured five times. The change set is 3 inserts / 1 update / 1 delete
    on ids {1,2,3}, and that is what gets asserted.
    """
    if not listed:
        return ["the manifests list no part at all"], None
    src = reader(fmt, listed)
    total = as_int(duckdb(f"SELECT count(*) FROM {src}"))
    if total is None:
        return [
            "the parts the manifests list could not be READ back — the completeness check never "
            "executed, so this case proves nothing"
        ], None

    problems: list[str] = []
    ops = {}
    for line in duckdb(f'SELECT __op||\'|\'||count(*) FROM {src} GROUP BY __op').splitlines():
        if "|" in line:
            k, v = line.rsplit("|", 1)
            ops[k] = as_int(v) or 0
    for op, want in EXPECTED_OPS.items():
        if ops.get(op, 0) < want:
            problems.append(
                f"{ops.get(op, 0)} `{op}` events, the change set applied {want} — at-least-once "
                f"permits duplicates, never loss"
            )
    got_ids = {
        i
        for i in (
            as_int(x) for x in duckdb(f'SELECT DISTINCT "{id_col}" FROM {src}').splitlines()
        )
        if i is not None
    }
    missing = EXPECTED_IDS - got_ids
    if missing:
        problems.append(f"no change captured for id(s) {sorted(missing)}")
    # The UPDATE's after-image. A stream that carried the update as a bare
    # "row 2 changed" would satisfy every count above and lose the new value.
    amount = AFTER_IMAGE_EXPR.get(engine, '"amount"')
    after = duckdb(
        f"SELECT count(*) FROM {src} WHERE __op='update' AND "
        f"CAST(\"{id_col}\" AS VARCHAR)='2' AND {amount}=55.55"
    )
    if not as_int(after):
        problems.append("the UPDATE's after-image (id=2 -> 55.55) is not in the captured rows")
    return problems, total


# ── one case ──────────────────────────────────────────────────────────────────
def build_config(engine: str, export: str, url: str, cdc_block: str, out: Path, case: Case) -> str:
    tls = "\n  tls: { accept_invalid_certs: true }" if engine == "mssql" else ""
    target = (
        f"    tables: [orc_cdc_probe, {SECOND_TABLE}]\n"
        if case.multi
        else "    table: orc_cdc_probe\n"
    )
    return (
        f'source:\n  type: {engine}\n  url: "{url}"{tls}\n'
        f"exports:\n"
        f"  - name: {export}\n"
        f"{target}"
        f"    mode: cdc\n"
        f"    format: {case.fmt}\n"
        f"    {cdc_block}\n"
        f"    destination: {{ type: local, path: {out} }}\n"
    )


def apply_axes(cdc_block: str, case: Case, work: Path) -> str:
    """Fold the case's axes into the engine's `cdc:` block.

    The block arrives from the gate's engine spec, so the engine-specific keys
    (`slot`, `server_id`, `capture_instance`) and that engine's own checkpoint
    default are already right; only the axis under test is changed.
    """
    body = cdc_block.strip()
    assert body.startswith("cdc: {") and body.endswith("}"), body
    inner = body[len("cdc: {") : -1].strip().rstrip(",")
    keys = [k.strip() for k in inner.split(",") if k.strip()]

    if case.checkpoint is False:
        keys = [k for k in keys if not k.startswith("checkpoint:")]
    elif case.checkpoint is True and not any(k.startswith("checkpoint:") for k in keys):
        keys.append(f'checkpoint: "{work}/cdc.ckpt"')
    if case.continuous:
        keys = [k for k in keys if not k.startswith("until_current:")]
    if case.snapshot:
        keys.append("initial: snapshot")
        if not any(k.startswith("checkpoint:") for k in keys):
            # `initial: snapshot` anchors, and on an engine with no server-side
            # anchor the checkpoint file IS that anchor — the product says so and
            # refuses the config otherwise.
            keys.append(f'checkpoint: "{work}/cdc.ckpt"')
    return "cdc: { " + ", ".join(keys) + case.cdc_extra + " }"


def settle(engine: str, url: str, table: str, want: int) -> bool:
    """Wait until the SOURCE itself can see the changes.

    SQL Server's capture is an ASYNCHRONOUS Agent job reading the log into change
    tables, and the sweep disables and re-enables the capture instance per case,
    which restarts that pipeline. The gate's `changes()` covers it with a fixed
    8-second sleep — enough for one run, not for seventeen back-to-back, where
    several cases read an empty change table and reported "the manifests list no
    part at all" as though rivet had lost them.

    A fixed sleep is the wrong instrument twice over: too short it reports a
    phantom loss, too long it wastes minutes per case. Poll the change table and
    stop when the source has the changes — bounded, so an Agent that never runs
    still surfaces as a failure rather than a hang.
    """
    if engine != "mssql":
        return True  # the other three engines' logs are written synchronously
    from release_oracle import cdc as gate_cdc

    ct = f"cdc.dbo_{table}_CT"
    for _ in range(30):
        p = gate_cdc._sqlcmd(url, q=f"SET NOCOUNT ON; SELECT count(*) FROM {ct}")
        n = as_int("".join(ch for ch in (p.stdout or "") if ch.isdigit() or ch == "\n").strip())
        if n is not None and n >= want:
            return True
        time.sleep(2.0)
    return False


def bounded(cdc_block: str) -> str:
    """The same `cdc:` block with every stop-condition axis removed.

    Used for the anchor and for the deferred-remainder drain — both are steps
    AROUND the axis under test, and must terminate whatever the case is doing.
    """
    keys = [
        k.strip()
        for k in cdc_block[len("cdc: {") : -1].split(",")
        if k.strip() and not k.strip().startswith(("max_events:", "until_current:"))
    ]
    return "cdc: { " + ", ".join(keys) + ", until_current: true }"


def measure(engine: str, case: Case, work_root: Path) -> Result:
    from release_oracle import cdc as gate_cdc

    r = Result(engine=engine, case=case.name)
    url = ENGINE_URLS.get(engine, "")
    spec = gate_cdc._ENGINES.get(engine)
    if not url or spec is None:
        r.skipped = f"no RIVET_CDC_{engine.upper()}_URL"
        return r
    if case.multi and MULTI_TABLE.get(engine) is False:
        r.skipped = "`tables:` is documented unsupported here (capture instances are per-table)"
        return r

    export = f"cdcsw_{TOKEN}_{engine}_{case.name}"
    work = work_root / export
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    out = work / "out"
    out.mkdir(parents=True, exist_ok=True)

    # Retry the provisioning. SQL Server's capture instance is disabled and
    # re-enabled per case, and that is ASYNCHRONOUS — running the cases
    # back-to-back on one table outran the capture Agent and most of them
    # reported "source setup failed", which reads as an engine problem when it is
    # the harness not waiting. Verified by running the same DDL by hand between
    # sweeps: it succeeds. Bounded, so a genuinely broken source still surfaces.
    cdc_block = None
    for attempt in range(4):
        cdc_block = spec.setup(url, work)
        if cdc_block is not None:
            break
        time.sleep(2.0 * (attempt + 1))
    if cdc_block is None:
        r.skipped = "source setup failed after 4 attempts"
        return r
    if case.multi and not _second_table_ddl(engine, url):
        r.skipped = "could not create the second table"
        return r
    cdc_block = apply_axes(cdc_block, case, work)

    cfg = work / "cdc.yaml"
    cfg.write_text(build_config(engine, export, url, cdc_block, out, case))

    # The ANCHOR runs a BOUNDED config, always — never the case's own.
    #
    # Anchoring is setup, not the axis under test, and for `continuous` the
    # case's config is a daemon: pointed at a source with nothing to consume it
    # waits forever, and `max_events` cannot cap what never arrives. Three
    # multi-engine sweeps died exactly there. It passed intermittently on MySQL,
    # which is worse than failing — the binlog carries DDL, so the table the
    # setup had just created sometimes supplied enough events to hit the cap and
    # let the anchor exit by accident.
    anchor_cfg = work / "anchor.yaml"
    anchor_cfg.write_text(build_config(engine, export, url, bounded(cdc_block), out, case))
    env = {"RIVET_STATE_URL": STATE_URL}
    before_runs = {
        rid
        for rid in psql_state(
            f"SELECT run_id FROM run_status WHERE {owned_by(export)}"
        ).splitlines()
        if rid.strip()
    }

    def runs_now() -> set[str]:
        return {
            rid
            for rid in psql_state(
                f"SELECT run_id FROM run_status WHERE {owned_by(export)}"
            ).splitlines()
            if rid.strip()
        } - before_runs

    crashed_runs: set[str] = set()
    try:
        if case.snapshot:
            _seed(engine, url)
        # Anchor first (an empty drain), THEN issue the changes: a CDC run that
        # has never opened has nothing to resume from, and anchoring after the
        # changes would race them.
        if run_rivet(anchor_cfg, env) is None:
            r.skipped = "the anchor run never terminated"
            return r
        spec.changes(url)
        if not settle(engine, url, "orc_cdc_probe", EXPECTED_EVENTS):
            r.skipped = "the source never registered the changes (capture agent not running?)"
            return r
        if case.multi:
            _second_table_changes(engine, url)
            if not settle(engine, url, SECOND_TABLE, 2):
                r.skipped = "the second table's changes never reached the source"
                return r

        if case.crash:
            seen = runs_now()
            crashed = run_rivet(cfg, env, extra={"RIVET_TEST_PANIC_AT": case.crash})
            if crashed is None:
                r.disagreements.append(f"the run with `{case.crash}` injected never terminated")
            elif crashed.returncode == 0:
                r.disagreements.append(
                    f"the injected crash `{case.crash}` never fired — this case proves nothing"
                )
            # Which run id died, taken from the ledger around the invocation. The
            # crashed run is the one whose manifest may exist with no metrics row,
            # so the meta-DB oracle needs to know it by name rather than excusing
            # every missing row.
            crashed_runs = runs_now() - seen
            if case.recover:
                run_rivet(cfg, env)
        elif case.continuous:
            # No open-time bound, so nothing but `max_events` can stop it. The
            # wall clock is the NET, not the mechanism: a timeout is reported as
            # a failure to stop, never quietly absorbed.
            if run_rivet(cfg, env, timeout=120) is None:
                r.disagreements.append(
                    "`until_current: false` with `max_events` did not terminate within 120s — "
                    "the cap is the only stop condition a daemon has"
                )
        else:
            run_rivet(cfg, env)
            if case.twice:
                run_rivet(cfg, env)

        if case.drain_after:
            # The remainder `max_events` left behind must arrive, not vanish:
            # defer-not-drop. Bounded, so this run cannot inherit the cap.
            drain_cfg = work / "drain.yaml"
            drain_cfg.write_text(build_config(engine, export, url, bounded(cdc_block), out, case))
            run_rivet(drain_cfg, env)

        new_runs = runs_now()

        art_problems, copies, orphans = oracle_artifacts(out, case.fmt)
        cdc_parts = parts_of(copies, snapshot_leg=False)
        r.parts, r.orphans = len(cdc_parts), orphans
        r.disagreements += art_problems

        meta_problems, meta_notes, meta_total = oracle_metadb(
            export, copies, new_runs, crashed_runs
        )
        r.meta_rows = meta_total
        r.disagreements += meta_problems
        r.observations += meta_notes
        r.disagreements += oracle_ledger_covers_the_files(export, out, case.fmt, new_runs)

        duck_problems, dest = oracle_duckdb(cdc_parts, case.fmt, spec.id_col, engine)
        r.dest_events = dest
        # The third edge: what DuckDB counts in the parts must be what the
        # manifests SAY those parts hold. The meta-DB leg above compares the
        # ledger to the manifest; without this one, a manifest could agree with
        # the ledger and both be wrong about the file.
        claimed = rows_of(copies, snapshot_leg=False)
        if dest is not None and dest != claimed:
            r.disagreements.append(
                f"DuckDB reads {dest} rows from the change-stream parts, their manifests claim "
                f"{claimed}"
            )
        r.disagreements += duck_problems

        if orphans and case.crash is None:
            r.disagreements.append(
                f"{orphans} data file(s) on disk that no manifest lists, with no crash to explain "
                f"them — a consumer summing manifests would never see this data"
            )
        if case.crash and not orphans and case.crash in (
            "cdc_after_flush_before_ack",
            "cdc_before_manifest",
        ):
            r.observations.append(
                f"`{case.crash}` left no unmanifested part — the crash fired before anything was "
                f"flushed, so the recovery carried the whole change set"
            )

        r.validate_exit = sh(
            [RIVET, "validate", "-c", str(cfg), "--depth", "full"], env=env
        ).returncode
        if r.validate_exit not in (0, None):
            r.disagreements.append(f"validate exit {r.validate_exit}")

        # A crashed CDC run leaves its `running` row EVEN AFTER a successful
        # recovery, and that is correct — unlike a batch `--resume`, which adopts
        # the crashed run's id and closes that very row, CDC recovery is a FRESH
        # run with a new id. The crashed row is resolved by SUPERSESSION: a later
        # run of the same export outranks it by `started_at`, so it stops counting
        # as active without anything having to reach back and close it.
        # Scoped to THIS invocation's run ids. The state backend is shared and
        # persistent, so counting every `running` row for the export name counted
        # six earlier sweeps of the same case and called it a leak — the isolation
        # has to come from the key, never from the table being empty.
        ids = "','".join(sorted(new_runs))
        running = as_int(
            psql_state(
                f"SELECT count(*) FROM run_status WHERE status='running' AND run_id IN ('{ids}')"
            )
        )
        expected_running = 1 if case.crash else 0
        if running != expected_running:
            r.disagreements.append(
                f"{running} run_status row(s) left `running`, expected {expected_running}"
            )
        elif case.crash and case.recover:
            unsuperseded = as_int(
                psql_state(
                    f"SELECT count(*) FROM run_status a WHERE a.run_id IN ('{ids}') "
                    f"AND a.status='running' AND NOT EXISTS (SELECT 1 FROM run_status b "
                    f"WHERE b.export_name=a.export_name AND b.started_at > a.started_at)"
                )
            )
            if unsuperseded:
                r.disagreements.append(
                    "the crashed run's `running` row is NOT outranked by the recovery — "
                    "it would keep reading as an active run and gc would defer forever"
                )

        if case.snapshot:
            snap = parts_of(copies, snapshot_leg=True)
            if not snap:
                r.disagreements.append(
                    "`initial: snapshot` produced no snapshot leg — the rows that existed before "
                    "the anchor exist in no change stream and would be lost outright"
                )
            else:
                seeded = {
                    i
                    for i in (
                        as_int(x)
                        for x in duckdb(
                            f'SELECT DISTINCT "{spec.id_col}" FROM {reader(case.fmt, snap)}'
                        ).splitlines()
                    )
                    if i is not None
                }
                if not SEED_IDS <= seeded:
                    r.disagreements.append(
                        f"the snapshot leg is missing pre-anchor id(s) {sorted(SEED_IDS - seeded)}"
                    )
        if case.multi:
            here = {p.name for p in out.iterdir() if p.is_dir()}
            want = {"orc_cdc_probe", SECOND_TABLE}
            if not want <= here:
                r.disagreements.append(
                    f"`tables:` routed to {sorted(here)}, expected a prefix per table {sorted(want)}"
                )
            elif MULTI_TABLE.get(engine) is None:
                r.observations.append(
                    "`tables:` is undocumented for this engine and DOES route per table here"
                )

        # `checkpoint_absent` on a CLIENT-SIDE-anchor engine, judged last because
        # it reinterprets everything above.
        #
        # With no file there is no resume position to carry between two
        # processes, so the run legitimately opens at 'current', past the
        # changes, and captures nothing — no parts, no manifest, and a `validate`
        # with nothing to check. Every oracle above fires, and every one of them
        # is asking PostgreSQL's question of an engine that answers differently.
        # Holding MySQL to a server-side anchor model it does not have is how a
        # harness comes to report the documented meaning of a config as a defect.
        #
        # This is not licence to accept anything. The case still asserts, in both
        # directions: ALL or NOTHING. A partial capture is a TORN window, which no
        # anchor model explains and which is exactly the silent subset the sweep
        # exists to catch — so the case can still go red, and does.
        if case.checkpoint is False and "client-side ONLY" in ANCHOR_MODEL[engine]:
            captured = r.dest_events or 0
            if captured == 0:
                r.disagreements = []
                r.observations.append(
                    f"nothing captured, and correctly so — {ANCHOR_MODEL[engine]}. With no file "
                    f"the second process opened at 'current', past the changes; the empty prefix "
                    f"is why there is no manifest and why `validate` had nothing to check"
                )
            elif captured >= EXPECTED_EVENTS and not duck_problems:
                r.disagreements = []
                r.observations.append(
                    "complete with no checkpoint file — the runs happened to share a window; "
                    "this engine still has no anchor to rely on"
                )
            else:
                r.disagreements = [
                    f"captured {captured} of {EXPECTED_EVENTS} changes with no checkpoint — a "
                    f"TORN window. No anchor model explains a partial capture: with no anchor the "
                    f"run starts past the changes and captures NONE of them"
                ]
    finally:
        try:
            spec.cleanup(url, work)
            if case.multi:
                _second_table_drop(engine, url)
        except Exception:  # noqa: BLE001 - cleanup must never mask a finding
            pass
    return r


def selftest(out: Path) -> list[str]:
    """Break the artifacts on purpose and require each oracle to notice.

    A green oracle that was never red is unverified — and this one grades files,
    so it is trivially possible to write a check that only ever agrees with
    itself. Each mutation below is applied to a COPY of a real passing case's
    output, and the check that must catch it is named beside it. Anything that
    stays green here is a check that would have stayed green through the defect
    it exists to catch.
    """
    failures = []

    def case(label: str, mutate) -> None:
        scratch = out.parent / f"selftest_{label}"
        shutil.rmtree(scratch, ignore_errors=True)
        shutil.copytree(out, scratch)
        mutate(scratch)
        problems, copies, orphans = oracle_artifacts(scratch, "parquet")
        duck, _ = oracle_duckdb(
            parts_of(copies, snapshot_leg=False), "parquet", "id", "postgres"
        )
        claimed = rows_of(copies, snapshot_leg=False)
        if not (problems or orphans or duck):
            failures.append(f"{label}: mutated the artifacts and every oracle still passed")
        _ = claimed

    def flip_a_byte(d: Path) -> None:
        f = next(iter(sorted(d.rglob("cdc-*.parquet"))))
        b = bytearray(f.read_bytes())
        b[len(b) // 2] ^= 0xFF  # same LENGTH, so only a digest can see it
        f.write_bytes(bytes(b))

    def overstate_rows(d: Path) -> None:
        # The manifest with PARTS, not the first one on disk: the anchor run
        # leaves a legitimate empty manifest that sorts first, and inflating a
        # zero-length parts list mutates nothing — the mutation passed and the
        # selftest reported the ORACLE as blind. A mutation that does not change
        # the artifact proves nothing about the check.
        m = next(
            m
            for m in sorted(d.rglob("manifest-*.json"))
            if json.loads(m.read_text()).get("parts")
        )
        doc = json.loads(m.read_text())
        for p in doc["parts"]:
            p["rows"] += 1
        doc["row_count"] = sum(p["rows"] for p in doc["parts"])
        m.write_text(json.dumps(doc))

    def drop_a_listed_part(d: Path) -> None:
        next(iter(sorted(d.rglob("cdc-*.parquet")))).unlink()

    def plant_an_unlisted_part(d: Path) -> None:
        f = next(iter(sorted(d.rglob("cdc-*.parquet"))))
        shutil.copy(f, f.with_name("cdc-unlisted-999999.parquet"))

    case("byte_flip", flip_a_byte)  # content_md5, recomputed
    case("overstated_rows", overstate_rows)  # the parquet footer
    case("missing_part", drop_a_listed_part)  # listed-but-absent
    case("orphan_part", plant_an_unlisted_part)  # on disk, in no manifest
    return failures


def main() -> int:
    only_engine = os.environ.get("RIVET_CDC_SWEEP_ENGINE")
    only_case = os.environ.get("RIVET_CDC_SWEEP_CASE")
    if not Path(RIVET).exists():
        print(f"binary not found: {RIVET}")
        return 2
    work = Path(os.environ.get("RIVET_CDC_SWEEP_WORK", "/tmp/rivet_cdc_sweep"))
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)

    engines = [e for e in ENGINE_URLS if not only_engine or e == only_engine]
    cases = [c for c in CASES if not only_case or c.name == only_case]
    axes = sorted({c.axis for c in cases})
    results: list[Result] = []
    print(
        f"cdc sweep: {len(engines)} engine(s) x {len(cases)} case(s) over {len(axes)} axes, "
        f"state = postgres"
    )
    print(f"  axes: {', '.join(axes)}\n")

    hdr = (
        f"  {'engine':<9} {'case':<28} {'rows':>5} {'parts':>5} {'orph':>4} {'meta':>5} "
        f"{'val':>3}  verdict"
    )
    print(hdr)
    print("  " + "-" * (len(hdr) - 2), flush=True)

    def render(r: Result) -> str:
        if r.skipped:
            v = f"SKIP {r.skipped}"
        elif r.disagreements:
            v = "; ".join(r.disagreements)
        else:
            v = "OK" + ("  // " + "; ".join(r.observations) if r.observations else "")
        return (
            f"  {r.engine:<9} {r.case:<28} "
            f"{str(r.dest_events if r.dest_events is not None else '-'):>5} "
            f"{str(r.parts if r.parts is not None else '-'):>5} "
            f"{str(r.orphans if r.orphans is not None else '-'):>4} "
            f"{str(r.meta_rows if r.meta_rows is not None else '-'):>5} "
            f"{str(r.validate_exit if r.validate_exit is not None else '-'):>3}  {v}"
        )

    # Printed as each case finishes, not collected and printed at the end. A run
    # this long is going to be interrupted sometimes, and a report that exists
    # only in memory until the last case turns every interruption into a total
    # loss of the evidence — which is exactly what happened to the first full
    # four-engine pass.
    for engine in engines:
        for case in cases:
            r = measure(engine, case, work)
            results.append(r)
            print(render(r), flush=True)

    bad = sum(1 for r in results if not r.skipped and r.disagreements)
    ran = [r for r in results if not r.skipped]
    print(f"\n  {len(ran) - bad} agree, {bad} disagree, {len(results) - len(ran)} skipped")

    # Prove the oracles bite, on a copy of output they just called clean.
    clean = next(
        (
            work / f"cdcsw_{TOKEN}_{r.engine}_{r.case}" / "out"
            for r in results
            if not r.skipped and not r.disagreements and r.case == "bounded"
        ),
        None,
    )
    if clean and clean.exists():
        inert = selftest(clean)
        if inert:
            print("\n  ORACLE SELFTEST FAILED:")
            for line in inert:
                print(f"    {line}")
            return 1
        print("  oracle selftest: byte flip, overstated rows, missing part, orphan part — all caught")
    else:
        print("  oracle selftest: SKIPPED (no clean `bounded` case to mutate)")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
