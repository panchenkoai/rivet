#!/usr/bin/env python3
"""SQL Server CDC co-tenancy harm.

Like MySQL/Mongo, SQL Server has NO reader-pinned retention: the change tables are
trimmed by the CDC cleanup job on its own schedule, independent of any reader — a
lagging/abandoned rivet MSSQL CDC run pins nothing, so there is no disk-fill harm
(the PostgreSQL logical-slot hazard is absent). The source-side cost is CO-TENANCY:
does the capture Agent + rivet's change-table reads degrade the writer? Compare the
writer's throughput WITHOUT any CDC vs WITH CDC enabled and a looping until_current
MSSQL CDC drain.

Usage: RIVET_BIN=target/release/rivet python3 dev/cdc/harm_mssql.py [secs]
"""
import os
import subprocess
import sys
import tempfile
import threading
import time

MSC = os.environ.get("MSC_CONTAINER", "rivet-mssql-cdc-1")
MS_URL = os.environ.get("MSSQL_CDC_URL", "sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1434/rivet")
RIVET_BIN = os.environ.get("RIVET_BIN", "target/release/rivet")
SECS = int(sys.argv[1]) if len(sys.argv) > 1 else 15
TABLE = "harm_ms"
CI = f"dbo_{TABLE}"


def sqlcmd(q: str) -> str:
    return subprocess.run(
        ["docker", "exec", "-i", MSC, "/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost",
         "-U", "sa", "-P", "Rivet_Passw0rd!", "-d", "rivet", "-h", "-1", "-Q", q],
        capture_output=True, text=True,
    ).stdout.strip()


def change_rows() -> int:
    r = sqlcmd(f"SET NOCOUNT ON; SELECT COUNT_BIG(*) FROM cdc.{CI}_CT")
    try:
        return int(r.split()[-1])
    except (ValueError, IndexError):
        return 0


def writer_run(secs: int) -> int:
    """A T-SQL time-bounded loop of 200-row inserts; returns the row count."""
    q = (
        "SET NOCOUNT ON; "
        f"DECLARE @e datetime2 = DATEADD(second, {secs}, SYSDATETIME()); DECLARE @n int = 0; "
        f"WHILE SYSDATETIME() < @e BEGIN "
        f"INSERT INTO dbo.{TABLE}(v, pad) SELECT TOP 200 1, REPLICATE('x',120) FROM sys.all_columns; "
        "SET @n += 200; END; SELECT @n;"
    )
    out = sqlcmd(q)
    try:
        return int(out.split()[-1])
    except (ValueError, IndexError):
        return 0


def cfg_path() -> str:
    d = tempfile.mkdtemp(prefix="harm_ms_")
    out = os.path.join(d, "out")
    os.makedirs(out, exist_ok=True)
    ckpt = os.path.join(d, "ms.ckpt")
    cfg = os.path.join(d, "cdc.yaml")
    with open(cfg, "w") as f:
        f.write(
            "source:\n  type: mssql\n"
            f'  url: "{MS_URL}"\n  tls:\n    accept_invalid_certs: true\n'
            f"exports:\n  - name: {TABLE}\n    table: {TABLE}\n    mode: cdc\n"
            f"    format: parquet\n"
            f'    cdc: {{ capture_instance: {CI}, checkpoint: "{ckpt}" }}\n'
            f'    destination: {{ type: local, path: "{out}" }}\n'
        )
    return cfg


def phase(with_cdc: bool) -> float:
    stop = threading.Event()
    dt = None
    if with_cdc:
        cfg = cfg_path()
        subprocess.run([RIVET_BIN, "run", "--config", cfg], capture_output=True, text=True)  # anchor

        def drain():
            while not stop.is_set():
                subprocess.run([RIVET_BIN, "run", "--config", cfg], capture_output=True, text=True)

        dt = threading.Thread(target=drain, daemon=True)
        dt.start()
    rows = writer_run(SECS)
    stop.set()
    if dt:
        dt.join(timeout=30)
    return rows / SECS


def main():
    print(f"SQL Server CDC co-tenancy harm — {SECS}s/phase, dbo.{TABLE}\n")
    # ensure a clean, CDC-disabled table for the baseline
    sqlcmd(
        f"IF EXISTS(SELECT 1 FROM cdc.change_tables ct JOIN sys.tables t ON ct.source_object_id=t.object_id "
        f"WHERE t.name='{TABLE}') EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'{TABLE}', "
        f"@capture_instance=N'{CI}'; DROP TABLE IF EXISTS dbo.{TABLE}"
    )
    sqlcmd(f"CREATE TABLE dbo.{TABLE} (id INT IDENTITY PRIMARY KEY, v INT, pad NVARCHAR(200))")

    print("Phase 1/2: BASELINE (CDC disabled)")
    b = phase(with_cdc=False)

    print("enabling CDC + letting the capture Agent start ...")
    sqlcmd("IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) EXEC sys.sp_cdc_enable_db")
    sqlcmd(
        f"EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{TABLE}', "
        f"@role_name=NULL, @capture_instance=N'{CI}'"
    )
    time.sleep(6)  # the Agent capture job needs a moment to begin

    print("Phase 2/2: UNDER CDC (capture Agent + a looping MSSQL CDC drain)")
    ch0 = change_rows()
    u = phase(with_cdc=True)
    ch_grew = change_rows() - ch0

    print("\n" + "=" * 60)
    print("SQL Server HARM REPORT")
    print("=" * 60)
    print(f"writer throughput (rows/s):  baseline {b:.0f}   under-cdc {u:.0f}   x{u/b:.2f}" if b else "n/a")
    print(f"change-table rows captured under CDC:  {ch_grew}")
    print("  → change tables are trimmed by the CDC cleanup job (reader-independent);")
    print("    a lagging/abandoned MSSQL CDC reader pins NO retention — no disk-fill harm.")
    print("=" * 60)
    sqlcmd(
        f"EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N'{TABLE}', @capture_instance=N'{CI}'; "
        f"DROP TABLE IF EXISTS dbo.{TABLE}"
    )


if __name__ == "__main__":
    main()
