"""A/B: range chunking vs keyset on a UNIQUE NOT NULL (non-PK) key — PostgreSQL, SQL Server, Oracle.

Per engine, a dense (1..N) and a sparse (spread over ~500·N) table; variants R1/R4 (init's range
config, parallel 1/4) and K1/K4 (the same config with `chunk_by_key`). Each cell is repeated,
interleaved, and reports wall time, rivet peak RSS, server CPU (container cgroup), the engine's
own logical-read figure, and a DuckDB readback (rows, distinct keys) of the parts it wrote.

    uv run python -m dev.pytools.keyset_ab --rows 2000000 --reps 5
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PG = ("rivet-postgres-1", "postgresql://rivet:rivet@127.0.0.1:5432/rivet_ab")
MS = ("rivet-mssql-1", "mssql://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet_ab")
ORA = ("rivet-oracle-1", "oracle://rivet:rivet@127.0.0.1:1521/FREEPDB1")
SQLCMD = "/opt/mssql-tools18/bin/sqlcmd"


def sh(*a: str, stdin: str | None = None, check: bool = True) -> str:
    p = subprocess.run(a, input=stdin, capture_output=True, text=True)
    if check and p.returncode:
        raise RuntimeError(f"{a[:4]}… exit {p.returncode}: {p.stderr[-800:] or p.stdout[-800:]}")
    return p.stdout


def psql(sql: str, db: str = "rivet_ab") -> str:
    return sh("docker", "exec", "-i", PG[0], "psql", "-U", "rivet", "-d", db, "-Atq", "-v", "ON_ERROR_STOP=1", stdin=sql)


def mssql(sql: str, db: str = "rivet_ab") -> str:
    return sh("docker", "exec", MS[0], SQLCMD, "-C", "-S", "localhost", "-U", "sa", "-P", "Rivet_Passw0rd!",
              "-d", db, "-h", "-1", "-W", "-b", "-Q", "SET NOCOUNT ON; " + sql)


def ora(sql: str, sysdba: bool = False) -> str:
    who = ["/", "as", "sysdba"] if sysdba else ["rivet/rivet@localhost/FREEPDB1"]
    body = "set heading off feedback off pagesize 0 linesize 4000 trimspool on\nwhenever sqlerror exit failure\n" + sql + "\nexit\n"
    return sh("docker", "exec", "-i", ORA[0], "sqlplus", "-s", "-L", *who, stdin=body)


def sparse_expr(n: str) -> str:
    """Unique, ~500 apart: key = n*500 + (n*7919 mod 500)."""
    return f"({n}*500 + MOD({n}*7919, 500))"


def seed(engine: str, rows: int) -> None:
    """(Re)create ab_dense / ab_sparse with `rows` rows and fresh statistics."""
    for kind in ("dense", "sparse"):
        t = f"ab_{kind}"
        if engine == "postgres":
            key = "n" if kind == "dense" else sparse_expr("n").replace("MOD(", "mod(")
            psql(f"""DROP TABLE IF EXISTS {t};
CREATE TABLE {t} (order_id bigint NOT NULL, md5 char(32) NOT NULL, amount numeric(12,2), status varchar(16),
  created_at timestamp, payload varchar(120), CONSTRAINT ux_{t} UNIQUE (order_id));
INSERT INTO {t} SELECT {key}, md5(n::text), (n % 100000) / 100.0, (ARRAY['new','paid','void'])[1 + n % 3],
  timestamp '2020-01-01' + n * interval '1 second', repeat('x', 60 + (n % 60)::int) FROM generate_series(1, {rows}) n;
ANALYZE {t};""")
        elif engine == "mssql":
            key = "n" if kind == "dense" else "(n*500 + (n*7919) % 500)"
            mssql(f"""DROP TABLE IF EXISTS {t};
CREATE TABLE {t} (order_id bigint NOT NULL, md5 char(32) NOT NULL, amount decimal(12,2), status varchar(16),
  created_at datetime2(6), payload varchar(120), CONSTRAINT ux_{t} UNIQUE (order_id));
WITH s AS (SELECT TOP ({rows}) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS bigint) n
           FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c)
INSERT INTO {t} SELECT {key}, CONVERT(char(32), HASHBYTES('MD5', CAST(n AS varchar(20))), 2), (n % 100000) / 100.0,
  CHOOSE(1 + n % 3, 'new','paid','void'), DATEADD(SECOND, n % 2000000000, '2020-01-01'), REPLICATE('x', 60 + n % 60) FROM s;
UPDATE STATISTICS {t} WITH FULLSCAN;""")
        else:
            key = "n" if kind == "dense" else sparse_expr("n")
            ora(f"""BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t} PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE {t} (order_id NUMBER(18) NOT NULL, md5 CHAR(32) NOT NULL, amount NUMBER(12,2), status VARCHAR2(16),
  created_at TIMESTAMP(6), payload VARCHAR2(120), CONSTRAINT ux_{t} UNIQUE (order_id));
INSERT /*+ APPEND */ INTO {t}
SELECT {key}, LOWER(RAWTOHEX(STANDARD_HASH(TO_CHAR(n), 'MD5'))), MOD(n, 100000) / 100,
  DECODE(MOD(n, 3), 0, 'new', 1, 'paid', 'void'), TIMESTAMP '2020-01-01 00:00:00' + NUMTODSINTERVAL(n, 'SECOND'),
  RPAD('x', 60 + MOD(n, 60), 'x')
FROM (SELECT (a.l - 1) * 1000 + b.l n FROM (SELECT LEVEL l FROM dual CONNECT BY LEVEL <= {(rows + 999) // 1000}) a,
      (SELECT LEVEL l FROM dual CONNECT BY LEVEL <= 1000) b) WHERE n <= {rows};
COMMIT;
EXEC DBMS_STATS.GATHER_TABLE_STATS(USER, '{t.upper()}');""")


def ensure_databases() -> None:
    if psql("SELECT 1 FROM pg_database WHERE datname='rivet_ab'", db="rivet").strip() != "1":
        psql("CREATE DATABASE rivet_ab", db="rivet")
    mssql("IF DB_ID('rivet_ab') IS NULL CREATE DATABASE rivet_ab", db="master")


def container_cpu_us(container: str) -> int:
    out = sh("docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat")
    return int(re.search(r"usage_usec (\d+)", out).group(1))


def logical_reads(engine: str, table: str) -> int:
    """The engine's own read-work counter scoped to `table` (blocks / pages / buffer gets)."""
    if engine == "postgres":
        return int(psql(f"SELECT coalesce(heap_blks_hit,0)+coalesce(heap_blks_read,0)+coalesce(idx_blks_hit,0)"
                        f"+coalesce(idx_blks_read,0) FROM pg_statio_user_tables WHERE relname='{table}'").strip() or 0)
    if engine == "mssql":
        return int(mssql(f"SELECT ISNULL(SUM(qs.total_logical_reads),0) FROM sys.dm_exec_query_stats qs "
                         f"CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st WHERE st.text LIKE '%{table}%' "
                         f"AND st.text NOT LIKE '%dm_exec_query_stats%'").strip() or 0)
    return int(ora(f"ALTER SESSION SET CONTAINER = FREEPDB1;\nSELECT NVL(SUM(value),0) FROM v$segment_statistics WHERE owner='RIVET' AND statistic_name="
                   f"'logical reads' AND object_name IN ('{table.upper()}', 'UX_{table.upper()}');", sysdba=True).split()[-1])


def url_env(engine: str) -> tuple[str, str]:
    return {"postgres": ("PG_URL", PG[1]), "mssql": ("MS_URL", MS[1]), "oracle": ("ORACLE_URL", ORA[1])}[engine]


def init_config(rivet: str, engine: str, table: str, work: Path) -> str:
    """`rivet init --mode chunked` for `table` — the R (range) variant as the product scaffolds it."""
    var, url = url_env(engine)
    tbl = {"postgres": f"public.{table}", "mssql": f"dbo.{table}", "oracle": table}[engine]
    cfg = work / "init.yaml"
    subprocess.run([rivet, "init", "--source-env", var, "--table", tbl, "--mode", "chunked", "-o", str(cfg)],
                   env={**os.environ, var: url}, cwd=work, check=True, capture_output=True)
    return cfg.read_text()


def variant(base: str, engine: str, table: str, keyset: bool, parallel: int) -> str:
    """R = init's config; K = the same with the range key swapped for `chunk_by_key` via the `table:` form keyset needs."""
    m = re.search(r"^    chunk_column: (\S+)", base, re.M)
    if not m:
        raise RuntimeError(f"{engine}/{table}: init emitted no chunk_column:\n{base}")
    key = m.group(1)
    if key.lower() != "order_id":
        raise RuntimeError(f"{engine}/{table}: init chose chunk_column {key}, not the unique key order_id")
    cfg = re.sub(r"^    chunk_checkpoint: .*\n", "", base, flags=re.M)
    if keyset:
        tbl = {"postgres": table, "mssql": f"dbo.{table}", "oracle": table.upper()}[engine]
        cfg = re.sub(r"^    query: >\n(?:      .*\n)+", f"    table: {tbl}\n", cfg, flags=re.M)
        cfg = cfg.replace(f"    chunk_column: {key}", f"    chunk_by_key: {key}")
    cfg = cfg.replace(f"    chunk_{'by_key' if keyset else 'column'}: {key}",
                      f"    chunk_{'by_key' if keyset else 'column'}: {key}\n    parallel: {parallel}")
    return cfg


def duck_readback(out: Path) -> tuple[int, int]:
    """(rows, distinct order_id) over every part the run wrote, read by DuckDB."""
    import duckdb

    files = sorted(str(p) for p in out.rglob("*.parquet"))
    if not files:
        return 0, 0
    return duckdb.sql(f"SELECT count(*), count(DISTINCT COLUMNS('(?i)^order_id$')) FROM read_parquet({files!r})").fetchone()


def run_cell(rivet: str, engine: str, table: str, cfg_text: str, work: Path) -> dict:
    container = {"postgres": PG[0], "mssql": MS[0], "oracle": ORA[0]}[engine]
    var, url = url_env(engine)
    d = Path(tempfile.mkdtemp(dir=work))
    (d / "rivet.yaml").write_text(cfg_text)
    cpu0, lr0 = container_cpu_us(container), logical_reads(engine, table)
    t0 = time.monotonic()
    p = subprocess.run(["/usr/bin/time", "-l", rivet, "run", "-c", "rivet.yaml"], cwd=d, env={**os.environ, var: url},
                       capture_output=True, text=True)
    wall = time.monotonic() - t0
    cpu1, lr1 = container_cpu_us(container), logical_reads(engine, table)
    rss = int(re.search(r"(\d+)\s+maximum resident set size", p.stderr).group(1)) / 2**20
    rows, distinct = duck_readback(d / "output")
    shutil.rmtree(d)
    return {"ok": p.returncode == 0, "wall": wall, "rss_mib": rss, "db_cpu_s": (cpu1 - cpu0) / 1e6,
            "reads": lr1 - lr0, "rows": rows, "distinct": distinct,
            "err": "" if p.returncode == 0 else next((l for l in reversed(p.stderr.splitlines()) if "rror" in l), "")[:300]}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--engines", default="postgres,mssql,oracle")
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--no-seed", action="store_true")
    ap.add_argument("--bin", default=str(ROOT / "target/release/rivet"))
    ap.add_argument("--out", default=None, help="results JSON (default dev/keyset_ab/<ts>.json)")
    ns = ap.parse_args()
    engines = ns.engines.split(",")
    out = Path(ns.out or ROOT / "dev/keyset_ab" / f"{time.strftime('%Y%m%dT%H%M%S')}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="keyset-ab-"))
    if not ns.no_seed:
        ensure_databases()
        for e in engines:
            t0 = time.monotonic()
            seed(e, ns.rows)
            print(f"[seed] {e}: {ns.rows} rows ×2 tables in {time.monotonic() - t0:.0f}s", flush=True)
    variants = [("R1", False, 1), ("K1", True, 1), ("R4", False, 4), ("K4", True, 4)]
    results = []
    for e in engines:
        for table in ("ab_dense", "ab_sparse"):
            iw = Path(tempfile.mkdtemp(dir=work))
            base = init_config(ns.bin, e, table, iw)
            cfgs = {name: variant(base, e, table, ks, par) for name, ks, par in variants}
            for name in cfgs:  # warm-up, discarded
                run_cell(ns.bin, e, table, cfgs[name], work)
            for rep in range(ns.reps):
                order = list(cfgs) if rep % 2 == 0 else list(reversed(cfgs))
                for name in order:
                    r = run_cell(ns.bin, e, table, cfgs[name], work)
                    r.update(engine=e, table=table, variant=name, rep=rep)
                    results.append(r)
                    print(f"[{e} {table} {name} #{rep}] {'ok ' if r['ok'] else 'ERR'} wall={r['wall']:.2f}s "
                          f"db_cpu={r['db_cpu_s']:.2f}s reads={r['reads']} rss={r['rss_mib']:.0f}MiB "
                          f"rows={r['rows']}/{r['distinct']} {r['err']}", flush=True)
                    out.write_text(json.dumps({"rows": ns.rows, "results": results}, indent=1))
    print(f"\n{'engine':9} {'table':10} {'var':4} {'wall':>7} {'db_cpu':>7} {'reads':>10} {'rss':>5} rows-ok")
    for e in engines:
        for table in ("ab_dense", "ab_sparse"):
            for name, _, _ in variants:
                rs = [r for r in results if (r["engine"], r["table"], r["variant"]) == (e, table, name)]
                med = lambda k: statistics.median(r[k] for r in rs)
                good = all(r["ok"] and r["rows"] == r["distinct"] == ns.rows for r in rs)
                print(f"{e:9} {table:10} {name:4} {med('wall'):7.2f} {med('db_cpu_s'):7.2f} {med('reads'):10.0f} "
                      f"{med('rss_mib'):5.0f} {'yes' if good else 'NO'}")
    print(f"\nresults: {out}")
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
