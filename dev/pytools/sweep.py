"""Source-parity sweep (BATCH) + the stale-fixture pre-run sweep.

Two ports that share a name and nothing else:

* `dev/sweep/source_parity_sweep.sh` → `source_parity()`
  The BATCH half of rivet's strongest oracle. Per engine it seeds a hostile type
  fixture (1000 rows: low-cardinality, null-heavy, `DECIMAL(38,10)` differing in
  the 10th decimal, unicode/emoji, uuid, json, date), exports it with rivet, and
  compares a per-column profile computed from the SOURCE (direct DB query)
  against the DESTINATION parquet read by DuckDB. It does NOT trust rivet's own
  counters, which is the point: row loss, null injection, distinct collapse and
  decimal truncation all survive a self-oracle. Exit 1 on any mismatch.

  The CDC half is `dev/pytools/cdc_soak.py source-parity`, and this module
  deliberately IMPORTS its comparison core (`Tally`, `norm`, `chk`, `compare`,
  `ddv`, `dc_exec`) rather than re-deriving it — the two sweeps drifted apart in
  bash precisely because each carried its own copy of `norm`/`chk`.

* `dev/sweep-test-cruft.sh` → `test_cruft()`
  Drops test objects left behind by INTERRUPTED live runs, on every source in
  `dev/stand/registry.yaml` (tables, PG slots, Mongo databases and collections;
  BigQuery with `--bigquery`). Live tests name objects `<prefix>_<pid>_<counter>`
  (`tests/common::unique_name`); an object is dropped only when that pid is no
  longer running, so a concurrent live run is never swept.

Usage:

    python3 dev/pytools/sweep.py source-parity [rivet-binary]
    python3 dev/pytools/sweep.py test-cruft

WHAT IS DELIBERATELY DIFFERENT FROM THE BASH (each one a bug it shipped):

1. **No `trap`, so every early exit leaked.** Same gap as the CDC sibling: a
   Ctrl-C mid-sweep, or the `exit 2` preconditions, left `sweep_src` behind on
   whichever engines had been seeded plus the `mktemp -d` output tree (three
   engines × 1000 rows of parquet). Each engine's body is now `try/finally`, and
   the tempdir is removed in a `finally` around all three. (The batch sweep has
   no replication slot or capture instance to leak — that half of the CDC
   sibling's leak is CDC-only.)
2. **An empty source read no longer compares equal to an empty destination.**
   `MY()`/`MS()` sent the client's stderr to `/dev/null` (bug class 8), so a
   down engine produced `src=""`; the export then wrote no parquet, so `ddv`
   returned `""` too — and `chk` printed **`ok`** for a comparison of nothing
   against nothing. CONFIRMED by running the bash against this stack with the
   batch mssql container down: it printed **13 `ok` cells for an engine that was
   not running**, then exited 1 on the single "rivet FAILED" tally — i.e. it
   reported `SILENT-CORRUPTION DETECTED` (which
   `tests/live/live_source_parity_sweep.rs` reads as a real regression) for a
   service that was merely absent. Now: a seed that cannot run marks that engine
   NOT AVAILABLE, prints no cells for it, and — unless a genuine mismatch was
   found elsewhere, which outranks it — ends as `sweep INCOMPLETE` with **exit
   2**, the code that wrapper already documents as "a service down" and turns
   into a SKIP. A destination with no `.parquet` is likewise a named failure
   rather than an agreement.
KNOWN-WEAK ORACLE, kept as-is for fidelity: `norm()` (imported from `cdc_soak`)
strips trailing zeros — `sed 's/0*$//'` — so the two sides can agree across
engines that render decimals differently. It also means `1000` normalises to `1`,
so the comparison is blind to some magnitude differences in the LAST field of a
value. Reproduced bug-for-bug rather than tightened, because tightening it would
change which cells fail.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Sequence

if __package__:
    from . import cdc_soak, cdc_stand, registry, shell
else:  # executed as a plain script: `python3 dev/pytools/sweep.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import cdc_soak  # type: ignore[no-redef]
    import cdc_stand  # type: ignore[no-redef]
    import registry  # type: ignore[no-redef]
    import shell  # type: ignore[no-redef]

ROOT = shell.ROOT

USAGE = (
    "usage: dev/pytools/sweep.py source-parity [rivet-binary]\n"
    "       dev/pytools/sweep.py test-cruft [--bigquery]"
)

# The comparison core is the CDC sibling's, on purpose — one `norm`, one `chk`,
# one column list for both sweeps.
Tally = cdc_soak.Tally
compare = cdc_soak.compare
norm = cdc_soak.norm
ddv = cdc_soak.ddv
SWEEP_ROWS = cdc_soak.SWEEP_ROWS

# ── batch stack (the OTHER ports than the cdc profile: 5432 / 3306 / 1433) ─────
PG_URL = registry.source("postgres")["url"]
MY_URL = registry.source("mysql")["url"]
MS_URL = registry.source("mssql")["url"]


def export_parquet(t: Tally, rivet: Path, engine: str, url: str, query: str, out: Path) -> None:
    """Write `<out>.yaml` and run one `mode: full` export into `<out>`.

    `atomic_write` rather than `printf > "$4.yaml"`: the redirect truncates the
    config before the generator writes it, so an interrupted write left a
    half-config that rivet then reported as a parse error.
    """
    cfg = Path(f"{out}.yaml")
    shell.atomic_write(
        cfg,
        f'source: {{ type: {engine}, url: "{url}" }}\n'
        "exports:\n"
        "  - name: s\n"
        f'    query: "{query}"\n'
        "    mode: full\n"
        "    format: parquet\n"
        f'    destination: {{ type: local, path: "{out}" }}\n',
    )
    if not shell.run([str(rivet), "run", "--config", str(cfg)], cwd=ROOT, timeout=None).ok:
        print("  rivet FAILED")
        t.failed += 1


def _unavailable(engine: str, proc: shell.Proc, hint: str) -> str:
    """A down engine is an ENVIRONMENT gap (exit 2), not a corruption finding.

    The bash reported it as corruption: with the client's stderr in `/dev/null`
    the source profile came back empty, the export failed ("rivet FAILED"), and
    the run ended on `SILENT-CORRUPTION DETECTED` + exit 1 — which
    `tests/live/live_source_parity_sweep.rs` reads as a real regression. Exit 2 is
    the code that wrapper turns into a SKIP, and it is what "a service down"
    already meant in its own comment.
    """
    detail = (proc.stderr or proc.stdout).strip().splitlines()
    reason = detail[-1] if detail else f"exit {proc.returncode}"
    # stdout, because the Rust wrapper echoes stdout when it decides to skip.
    print(f"  {engine} NOT AVAILABLE: {reason}")
    shell.skip(f"{engine}: not available — {hint}")
    return f"{engine} not available ({reason})"


def _destination_readable(t: Tally, out: Path) -> bool:
    """Is there anything to compare against?

    Without this, a run that wrote NO parts compares an empty source profile
    against an empty destination profile and `chk` reports `ok` for all thirteen
    cells — the vacuous pass this sweep exists to prevent.
    """
    if sorted(out.glob("*.parquet")):
        return True
    shell.bad(f"{out.name}: no parquet parts written — nothing to compare against")
    t.failed += 1
    return False


# ── Postgres ───────────────────────────────────────────────────────────────────
PG_SEED = (
    "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, "
    "k INT, maybe_null INT, amount DECIMAL(38,10), label TEXT, uid UUID, ts TIMESTAMP, "
    "payload JSONB, d DATE);\n"
    "INSERT INTO sweep_src SELECT g, g%50, CASE WHEN g%3=0 THEN NULL ELSE g END, "
    "(g::numeric*0.0000000001), CASE WHEN g%7=0 THEN NULL ELSE 'rôw_😀_'||(g%100) END, "
    "gen_random_uuid(), timestamp '2020-01-01'+(g||' minutes')::interval, "
    f"json_build_object('k',g%50,'v',g)::jsonb, date '2020-01-01'+g FROM generate_series(1,{SWEEP_ROWS}) g;"
)


def _sweep_postgres(t: Tally, rivet: Path, out: Path) -> str | None:
    """Returns None when the engine ran, or a reason string when it was down."""

    def pg(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return cdc_soak.dc_exec("postgres", *cdc_stand.psql_argv(q), timeout=timeout)

    print("== BATCH postgres ==")
    # Checked where the bash had `>/dev/null`: a seed that cannot run is the
    # difference between "this engine is corrupting values" and "this engine is
    # not up", and the bash reported the second as the first.
    seeded = pg(PG_SEED, timeout=None)
    if not seeded.ok:
        return _unavailable("postgres", seeded, "docker compose up -d postgres")
    try:
        export_parquet(t, rivet, "postgres", PG_URL, "SELECT * FROM sweep_src", out / "pg")
        if _destination_readable(t, out / "pg"):
            compare(
                t,
                lambda c: pg(
                    f"SELECT count(*)||'/'||count({c})||'/'||count(distinct {c}) FROM sweep_src"
                ).stdout.strip(),
                lambda c: pg(f"SELECT COALESCE(SUM({c}),0) FROM sweep_src").stdout.strip(),
                f"read_parquet('{out / 'pg'}/*.parquet')",
            )
    finally:
        pg("DROP TABLE sweep_src;")
    return None


# ── MySQL ──────────────────────────────────────────────────────────────────────
MY_SEED = (
    "DROP TABLE IF EXISTS sweep_src; CREATE TABLE sweep_src (id BIGINT PRIMARY KEY, "
    "k INT, maybe_null INT, amount DECIMAL(38,10), label VARCHAR(60), uid CHAR(36), "
    "ts DATETIME, payload JSON, d DATE);\n"
    "SET SESSION cte_max_recursion_depth=4000;\n"
    "INSERT INTO sweep_src WITH RECURSIVE seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq "
    f"WHERE g<{SWEEP_ROWS}) SELECT g,g%50,IF(g%3=0,NULL,g),CAST(g AS DECIMAL(38,10))"
    "*0.0000000001,IF(g%7=0,NULL,CONCAT('rôw_😀_',g%100)),UUID(),"
    "TIMESTAMP('2020-01-01')+INTERVAL g MINUTE,JSON_OBJECT('k',g%50,'v',g),"
    "DATE('2020-01-01')+INTERVAL g DAY FROM seq;"
)


def _sweep_mysql(t: Tally, rivet: Path, out: Path) -> str | None:
    def my(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return cdc_soak.dc_exec("mysql", *cdc_stand.mysql_argv(q), timeout=timeout)

    print("== BATCH mysql ==")
    seeded = my(MY_SEED, timeout=None)
    if not seeded.ok:
        return _unavailable("mysql", seeded, "docker compose up -d mysql")
    try:
        export_parquet(t, rivet, "mysql", MY_URL, "SELECT * FROM sweep_src", out / "my")
        if _destination_readable(t, out / "my"):
            compare(
                t,
                lambda c: my(
                    f"SELECT CONCAT(count(*),'/',count({c}),'/',count(distinct {c})) FROM sweep_src"
                ).stdout.strip(),
                lambda c: my(f"SELECT COALESCE(SUM({c}),0) FROM sweep_src").stdout.strip(),
                f"read_parquet('{out / 'my'}/*.parquet')",
            )
    finally:
        my("DROP TABLE sweep_src;")
    return None


# ── SQL Server ─────────────────────────────────────────────────────────────────
MS_SEED = (
    "IF OBJECT_ID('dbo.sweep_src','U') IS NOT NULL DROP TABLE dbo.sweep_src; "
    "CREATE TABLE dbo.sweep_src (id BIGINT PRIMARY KEY, k INT, maybe_null INT, "
    "amount DECIMAL(38,10), label NVARCHAR(60), uid UNIQUEIDENTIFIER, ts DATETIME2, "
    "payload NVARCHAR(200), d DATE);\n"
    ";WITH seq(g) AS (SELECT 1 UNION ALL SELECT g+1 FROM seq WHERE "
    f"g<{SWEEP_ROWS}) INSERT INTO dbo.sweep_src SELECT g,g%50,"
    "CASE WHEN g%3=0 THEN NULL ELSE g END,CAST(g*0.0000000001 AS DECIMAL(38,10)),"
    "CASE WHEN g%7=0 THEN NULL ELSE N'rôw_'+CAST(g%100 AS NVARCHAR(10)) END,NEWID(),"
    "DATEADD(MINUTE,g,CAST('2020-01-01' AS DATETIME2)),"
    "N'{\"k\":'+CAST(g%50 AS NVARCHAR(10))+N',\"v\":'+CAST(g AS NVARCHAR(10))+N'}',"
    "DATEADD(DAY,g,CAST('2020-01-01' AS DATE)) FROM seq OPTION (MAXRECURSION 0);"
)


def _sweep_mssql(t: Tally, rivet: Path, out: Path) -> str | None:
    def ms(q: str, *, timeout: float | None = 600) -> shell.Proc:
        return cdc_soak.dc_exec(
            "mssql", *cdc_stand.sqlcmd_argv(q, wide=True, nocount=True), timeout=timeout
        )

    print("== BATCH mssql ==")
    seeded = ms(MS_SEED, timeout=None)
    if not seeded.ok:
        return _unavailable("mssql", seeded, "docker compose up -d mssql")
    try:
        export_parquet(t, rivet, "mssql", MS_URL, "SELECT * FROM dbo.sweep_src", out / "ms")
        if _destination_readable(t, out / "ms"):
            compare(
                t,
                lambda c: ms(
                    f"SELECT CAST(count(*) AS VARCHAR)+'/'+CAST(count({c}) AS VARCHAR)+'/'"
                    f"+CAST(count(distinct {c}) AS VARCHAR) FROM dbo.sweep_src"
                ).stdout.strip(),
                lambda c: ms(
                    f"SELECT CAST(COALESCE(SUM({c}),0) AS VARCHAR(64)) FROM dbo.sweep_src"
                ).stdout.strip(),
                f"read_parquet('{out / 'ms'}/*.parquet')",
            )
    finally:
        ms("DROP TABLE dbo.sweep_src;")
    return None


def source_parity(binary: str | Path | None = None) -> int:
    """The BATCH value-parity sweep across PostgreSQL, MySQL and SQL Server.

    All three engines always run, as in the CDC sibling: a sweep that quietly
    drops an engine is worse than one that fails, because the missing engine's
    decode path is exactly where a silent-corruption bug survives.

    Exit codes are the ones `tests/live/live_source_parity_sweep.rs` reads: 0
    clean, 1 corruption, **2 environment/setup missing** (which that wrapper
    turns into a SKIP rather than a failure).
    """
    spelled = str(binary or os.environ.get("RIVET") or "target/debug/rivet")
    rivet = Path(spelled)
    if not rivet.is_absolute():
        rivet = ROOT / rivet
    if not (rivet.is_file() and os.access(rivet, os.X_OK)):
        raise shell.Fail(
            f"rivet not at {spelled} (cargo build --bin rivet, or set RIVET=)", code=2
        )
    if not shell.have("duckdb"):
        raise shell.Fail("the pinned duckdb package is not importable — run through `uv run`", code=2)

    t = Tally()
    out = Path(tempfile.mkdtemp(prefix="rivet-parity-batch-"))
    print("###### rivet source-parity sweep (batch) ######")
    missing: list[str] = []
    try:
        for engine_sweep in (_sweep_postgres, _sweep_mysql, _sweep_mssql):
            reason = engine_sweep(t, rivet, out)
            if reason:
                missing.append(reason)
    finally:
        # The bash's `rm -rf "$OUT"` ran only on the happy path.
        shutil.rmtree(out, ignore_errors=True)

    print("###############################################")
    print(f"BATCH: {t.match}/{t.total} independent checks matched the source ({t.failed} failed)")
    # Corruption outranks an incomplete sweep: a mismatch that DID happen is a
    # finding regardless of which other engine was down.
    if t.failed:
        raise shell.Fail("SILENT-CORRUPTION DETECTED")
    if missing:
        raise shell.Fail("sweep INCOMPLETE: " + "; ".join(missing), code=2)
    return 0


# ══ dev/sweep-test-cruft.sh ════════════════════════════════════════════════════
def _lines(p: shell.Proc) -> list[str]:
    """Non-empty stripped stdout lines."""
    return [ln.strip() for ln in p.stdout.splitlines() if ln.strip()]


def _report(source: str, what: str, dropped: list[str], failed: list[str]) -> None:
    """One line per source and object kind."""
    print(f"  {source}: {len(dropped)} orphaned {what} dropped" + (f", {len(failed)} FAILED: {failed[:3]}" if failed else ""))


def _pg_cruft(source: str, container: str, db: str) -> None:
    """Orphaned tables and inactive replication slots on one PostgreSQL database."""
    q = lambda sql: shell.docker_exec(container, *cdc_stand.psql_argv(sql, db=db), timeout=300)  # noqa: E731
    listed = q("SELECT schemaname||'.'||tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')")
    if not listed.ok:
        return
    dropped, failed = [], []
    for fq in _lines(listed):
        schema, name = fq.split(".", 1)
        if registry.orphaned(name):
            ident = '"' + schema.replace('"', '""') + '"."' + name.replace('"', '""') + '"'
            (dropped if q(f"DROP TABLE IF EXISTS {ident} CASCADE").ok else failed).append(fq)
    _report(f"{source}/{db}", "tables", dropped, failed)
    slots = q("SELECT slot_name FROM pg_replication_slots WHERE NOT active")
    dropped, failed = [], []
    for slot in _lines(slots) if slots.ok else []:
        if registry.orphaned(slot):
            lit = slot.replace("'", "''")
            (dropped if q(f"SELECT pg_drop_replication_slot('{lit}')").ok else failed).append(slot)
    if dropped or failed:
        _report(source, "replication slots", dropped, failed)


def _my_cruft(source: str, container: str, db: str) -> None:
    """Orphaned tables on one MySQL database."""
    listed = shell.docker_exec(container, *cdc_stand.mysql_argv("SHOW TABLES", db=db), timeout=300)
    if not listed.ok:
        return
    dropped, failed = [], []
    for name in _lines(listed):
        if registry.orphaned(name):
            ok = shell.docker_exec(container, *cdc_stand.mysql_argv(f"DROP TABLE IF EXISTS {_my_quote(name)}", db=db)).ok
            (dropped if ok else failed).append(name)
    _report(f"{source}/{db}", "tables", dropped, failed)


def _ms_cruft(source: str, container: str, db: str) -> None:
    """Orphaned tables on one SQL Server database — CDC disabled BEFORE the drop, or the change table is orphaned."""
    q = lambda sql: shell.docker_exec(container, *cdc_stand.sqlcmd_argv(sql, db=db, wide=True, nocount=True), timeout=300)  # noqa: E731
    listed = q("SELECT s.name+'|'+t.name+'|'+CAST(t.is_tracked_by_cdc AS varchar) FROM sys.tables t "
               "JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name <> 'cdc' AND t.is_ms_shipped=0")
    if not listed.ok:
        return
    dropped, failed = [], []
    for row in _lines(listed):
        parts = row.split("|")
        if len(parts) != 3 or not registry.orphaned(parts[1]):
            continue
        schema, name, tracked = parts
        sq, nq = schema.replace("'", "''"), name.replace("'", "''")
        disable = (f"EXEC sys.sp_cdc_disable_table @source_schema=N'{sq}', @source_name=N'{nq}', "
                   "@capture_instance=N'all'; ") if tracked == "1" else ""
        ident = "[" + schema.replace("]", "]]") + "].[" + name.replace("]", "]]") + "]"
        (dropped if q(f"{disable}DROP TABLE IF EXISTS {ident}").ok else failed).append(f"{schema}.{name}")
    _report(f"{source}/{db}", "tables", dropped, failed)


_MONGO_SWEEP_JS = """
const keep = %s, orphan = new Set(%s);
let dbs = 0, colls = 0;
db.adminCommand({listDatabases: 1}).databases.forEach(d => {
  if (orphan.has(d.name)) { db.getSiblingDB(d.name).dropDatabase(); dbs++; return; }
  if (!keep.includes(d.name)) return;
  db.getSiblingDB(d.name).getCollectionNames().forEach(c => {
    if (orphan.has(d.name + "." + c)) { db.getSiblingDB(d.name).getCollection(c).drop(); colls++; }
  });
});
print(dbs + " " + colls);
"""


def _mongo_cruft(source: str, container: str) -> None:
    """Orphaned databases, and orphaned collections inside the persistent ones."""
    import json

    keep = registry.load()["databases"]
    listing = shell.docker_exec(container, "mongosh", "--quiet", "--eval",
        f"const k={json.dumps(keep)}; db.adminCommand({{listDatabases:1}}).databases.forEach(d => {{ print(d.name); "
        "if (k.includes(d.name)) db.getSiblingDB(d.name).getCollectionNames().forEach(c => print(d.name + '.' + c)); });",
        timeout=300)
    if not listing.ok:
        return
    orphan = [n for n in _lines(listing) if registry.orphaned(n.rsplit(".", 1)[-1])]
    swept = shell.docker_exec(container, "mongosh", "--quiet", "--eval",
                              _MONGO_SWEEP_JS % (json.dumps(keep), json.dumps(orphan)), timeout=900)
    if not swept.ok:
        shell.warn(f"  {source}: sweep FAILED: {(swept.stderr or swept.stdout).strip()[-200:]}")
        return
    dbs, colls = (_lines(swept)[-1].split() + ["?", "?"])[:2]
    print(f"  {source}: {dbs} orphaned databases, {colls} orphaned collections dropped")


def _bigquery_cruft() -> None:
    """Every disposable (`tmp_prefix`) dataset, and orphaned tables in the permanent one — opt-in, never during a run."""
    bq = registry.load()["bigquery"]
    proj, tmp, e2e = bq["project"], bq["tmp_prefix"], bq["e2e"]
    ls = shell.run(["bq", "ls", "--format=json", "--max_results=10000", f"{proj}:"], timeout=300)
    if not ls.ok:
        shell.warn(f"  bigquery: listing FAILED: {ls.stderr.strip()[-200:]}")
        return
    import json

    names = [d["datasetReference"]["datasetId"] for d in json.loads(ls.stdout or "[]")]
    dropped, failed = [], []
    for name in names:
        if name.startswith(tmp):
            ok = shell.run(["bq", "rm", "-r", "-f", "-d", f"{proj}:{name}"], timeout=300).ok
            (dropped if ok else failed).append(name)
    _report("bigquery", f"datasets ({tmp}*)", dropped, failed)
    tl = shell.run(["bq", "ls", "--format=json", "--max_results=100000", f"{proj}:{e2e}"], timeout=300)
    dropped, failed = [], []
    for t in json.loads(tl.stdout or "[]") if tl.ok else []:
        name = t["tableReference"]["tableId"]
        if registry.orphaned(name):
            ok = shell.run(["bq", "rm", "-f", "-t", f"{proj}:{e2e}.{name}"], timeout=300).ok
            (dropped if ok else failed).append(name)
    _report(f"bigquery/{e2e}", "tables", dropped, failed)


def _my_quote(name: str) -> str:
    """MySQL identifier quoting: backticks, internal backticks doubled."""
    return "`" + name.replace("`", "``") + "`"


def _container_up(name: str) -> bool:
    """`docker exec <c> true` — the liveness gate."""
    return shell.docker_exec(name, "true", timeout=30).ok


def test_cruft(bigquery: bool = False) -> int:
    """Drop test objects whose creating process is gone, on every source in the stand registry.

    Best-effort: a down container is skipped, a failed drop is reported, the exit code stays 0.
    Objects of a live process are never touched, so a concurrent run is safe.
    """
    print("sweep-test-cruft: dropping objects `<prefix>_<pid>_<n>` whose pid is gone")
    arms = {"postgres": _pg_cruft, "mysql": _my_cruft, "mssql": _ms_cruft}
    for source, spec in registry.load()["sources"].items():
        container = spec["container"]
        if not _container_up(container):
            print(f"  {source}: not up — skipped")
            continue
        if source.startswith("mongo"):
            _mongo_cruft(source, container)
            continue
        for db in registry.load()["databases"]:
            arms[source.split("_")[0]](source, container, db)
    if bigquery:
        _bigquery_cruft()
    _sweep_live_tmp()
    print("sweep-test-cruft: done")
    return 0


def _sweep_live_tmp(max_age_days: float = 2.0) -> None:
    """Prune old entries under `tests/.live-tmp`, the shared bind-mount workdir.

    Every `duckdb_oracle()` / `census_oracle()` / `resume_into_fresh_dest()`
    leaks its labeled dir forever: `live_shared_workdir` clears contents only on
    a label collision (pid+counter — near-never), the Rig's TempDir cleanup does
    not cover the shared mount, and nothing else sweeps it. Measured at the
    2026-08-29 harness audit: 3,715 dirs / 592 MB on one dev machine, including
    fossils from a pre-fix census. Age-gated so anything a LIVE run could still
    be using (hours old at most) is never touched; entries are removed whole.
    Two days, not seven: a workdir entry is only ever read by the test that
    created it, so anything past its own run is dead weight.
    """
    import time

    root = Path("tests/.live-tmp")
    if not root.is_dir():
        return
    cutoff = time.time() - max_age_days * 86400.0
    removed = 0
    for entry in root.iterdir():
        try:
            if entry.stat().st_mtime >= cutoff:
                continue
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
            removed += 1
        except OSError as e:
            print(f"  live-tmp: could not remove {entry.name}: {e}")
    if removed:
        print(f"  live-tmp: pruned {removed} entries older than {max_age_days:g} days")


# ══ CLI ════════════════════════════════════════════════════════════════════════
def main_cli(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    cmd = args[0] if args else ""
    rest = args[1:]

    if cmd in ("source-parity", "source_parity", "parity"):
        return source_parity(rest[0] if rest else None)
    if cmd in ("test-cruft", "test_cruft", "cruft"):
        return test_cruft(bigquery="--bigquery" in rest)

    print(USAGE)
    return 1


if __name__ == "__main__":
    shell.main(lambda: main_cli())
