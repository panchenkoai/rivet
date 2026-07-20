#!/usr/bin/env python3
"""Heavy-load CDC harm (PostgreSQL) — the high-write-rate case the paced OLTP
probe couldn't reach.

pgbench drives a high INSERT rate (N clients, no -R cap) against the CDC database;
we compare its throughput (tps) + average latency WITHOUT a CDC drain vs WITH a
back-to-back until_current drain looping, and track the peak WAL the slot pins.
The question: at a WAL rate that stresses the slot, does the drain KEEP UP
(retention bounded) or FALL BEHIND (retention grows), and does the writer degrade?

SAFETY: a watchdog samples the slot's retained WAL every 0.5 s and KILLS pgbench
if it exceeds HARM_CAP_MB (default 1024). An earlier UNPACED flood pinned WAL past
the colima VM disk and crashed postgres — this run must never repeat that.

Usage: RIVET_BIN=target/release/rivet python3 dev/cdc/harm_heavy.py [secs] [clients]
"""
import os
import subprocess
import tempfile
import threading
import time

PGC = os.environ.get("PGC_CONTAINER", "rivet-postgres-cdc-1")
PG_URL = os.environ.get("POSTGRES_CDC_URL", "postgresql://rivet:rivet@127.0.0.1:5434/rivet")
RIVET_BIN = os.environ.get("RIVET_BIN", "target/release/rivet")
CAP_B = int(os.environ.get("HARM_CAP_MB", "1024")) * 1_048_576
import sys
SECS = int(sys.argv[1]) if len(sys.argv) > 1 else 20
CLIENTS = int(sys.argv[2]) if len(sys.argv) > 2 else 8
TABLE = "harm_heavy"
SLOT = "harm_heavy_slot"


def pg(sql: str) -> str:
    return subprocess.run(
        ["docker", "exec", "-i", PGC, "psql", "-U", "rivet", "-d", "rivet", "-tAc", sql],
        capture_output=True, text=True,
    ).stdout.strip()


def retained_b() -> int:
    r = pg(
        "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint "
        f"FROM pg_replication_slots WHERE slot_name='{SLOT}'"
    )
    try:
        return int(r or 0)
    except ValueError:
        return 0


def lag_b() -> int:
    r = pg(
        "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),0)::bigint "
        f"FROM pg_replication_slots WHERE slot_name='{SLOT}'"
    )
    try:
        return int(r or 0)
    except ValueError:
        return 0


def pgbench(secs: int, clients: int) -> tuple:
    """Run pgbench with a custom INSERT script; return (tps, latency_avg_ms)."""
    subprocess.run(
        ["docker", "exec", "-i", PGC, "bash", "-c", "cat > /tmp/harm_ins.sql"],
        input=f"INSERT INTO {TABLE}(v, pad) VALUES (1, repeat('x',120));\n",
        text=True,
    )
    out = subprocess.run(
        ["docker", "exec", PGC, "pgbench", "-U", "rivet", "-d", "rivet", "-n",
         "-f", "/tmp/harm_ins.sql", "-c", str(clients), "-j", "4", "-T", str(secs)],
        capture_output=True, text=True,
    ).stdout
    tps, lat = 0.0, 0.0
    for line in out.splitlines():
        if line.startswith("tps"):
            tps = float(line.split("=")[1].split()[0])
        elif "latency average" in line:
            lat = float(line.split("=")[1].split()[0])
    return tps, lat


def cfg_path() -> str:
    d = tempfile.mkdtemp(prefix="harm_heavy_")
    out = os.path.join(d, "out")
    os.makedirs(out, exist_ok=True)
    cfg = os.path.join(d, "cdc.yaml")
    with open(cfg, "w") as f:
        f.write(
            f'source: {{type: postgres, url: "{PG_URL}"}}\n'
            f"exports:\n  - name: {TABLE}\n    table: {TABLE}\n    mode: cdc\n"
            f"    format: parquet\n    cdc: {{ slot: {SLOT}, until_current: true, rollover: 50000 }}\n"
            f'    destination: {{ type: local, path: "{out}" }}\n'
        )
    return cfg


def main():
    print(f"Heavy CDC harm — pgbench {CLIENTS} clients, {SECS}s, cap {CAP_B // 1_048_576} MB\n")
    pg(f"DROP TABLE IF EXISTS {TABLE}")
    pg(f"CREATE TABLE {TABLE} (id bigserial primary key, v int, pad text)")
    pg(f"SELECT pg_drop_replication_slot('{SLOT}') WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{SLOT}')")

    print("warmup ...")
    pgbench(5, CLIENTS)

    print(f"Phase 1/2: BASELINE — pgbench, no CDC drain")
    b_tps, b_lat = pgbench(SECS, CLIENTS)

    print(f"Phase 2/2: UNDER a looping CDC drain (with a {CAP_B // 1_048_576} MB retention watchdog)")
    cfg = cfg_path()
    subprocess.run([RIVET_BIN, "run", "--config", cfg], capture_output=True, text=True)  # prime slot
    stop = threading.Event()
    peak = {"ret": 0, "lag": 0, "tripped": False}

    def drain():
        while not stop.is_set():
            subprocess.run([RIVET_BIN, "run", "--config", cfg], capture_output=True, text=True)

    def watchdog():
        while not stop.is_set():
            r, l = retained_b(), lag_b()
            peak["ret"] = max(peak["ret"], r)
            peak["lag"] = max(peak["lag"], l)
            if r > CAP_B:
                peak["tripped"] = True
                subprocess.run(["docker", "exec", PGC, "pkill", "-f", "pgbench"], capture_output=True)
                break
            time.sleep(0.5)

    dt = threading.Thread(target=drain, daemon=True)
    wt = threading.Thread(target=watchdog, daemon=True)
    dt.start()
    wt.start()
    u_tps, u_lat = pgbench(SECS, CLIENTS)
    stop.set()
    dt.join(timeout=30)
    wt.join(timeout=5)

    # let the drain catch up, then clean up
    subprocess.run([RIVET_BIN, "run", "--config", cfg], capture_output=True, text=True)
    pg(f"SELECT pg_drop_replication_slot('{SLOT}') WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{SLOT}')")
    pg(f"DROP TABLE IF EXISTS {TABLE}; CHECKPOINT")

    print("\n" + "=" * 62)
    print("HEAVY HARM REPORT (PostgreSQL)")
    print("=" * 62)
    print(f"throughput (tps):   baseline {b_tps:.0f}   under-cdc {u_tps:.0f}   x{u_tps/b_tps:.2f}" if b_tps else "n/a")
    print(f"avg latency (ms):   baseline {b_lat:.3f}   under-cdc {u_lat:.3f}   x{u_lat/b_lat:.2f}" if b_lat else "n/a")
    print(f"peak WAL retained:  {peak['ret']/1_048_576:.1f} MB   peak lag {peak['lag']/1_048_576:.1f} MB")
    print(f"watchdog tripped:   {'YES — drain FELL BEHIND (retention hit the cap)' if peak['tripped'] else 'no — drain kept up'}")
    print("=" * 62)


if __name__ == "__main__":
    main()
