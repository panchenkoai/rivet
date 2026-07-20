#!/usr/bin/env python3
"""MySQL CDC co-tenancy harm.

The retention harm that bites PostgreSQL (a lagging/abandoned logical slot pins WAL
until the disk fills) is STRUCTURALLY ABSENT on MySQL: the binlog is purged on the
server's own schedule (binlog_expire_logs_seconds), independent of any CDC reader —
a rivet MySQL CDC run is a forward `COM_BINLOG_DUMP` reader, it pins nothing.

So the only source-side cost to measure is CO-TENANCY: does running the binlog-dump
drain degrade the writer? We compare the writer's throughput (rows/s it achieves)
WITHOUT vs WITH a looping until_current MySQL CDC drain, and confirm the binlog
grows the SAME either way (reader-independent — no disk-fill lever).

Usage: RIVET_BIN=target/release/rivet python3 dev/cdc/harm_mysql.py [secs]
"""
import os
import subprocess
import sys
import tempfile
import threading
import time

MYC = os.environ.get("MYC_CONTAINER", "rivet-mysql-cdc-1")
MY_URL = os.environ.get("MYSQL_CDC_URL", "mysql://rivet:rivet@127.0.0.1:3307/rivet")
RIVET_BIN = os.environ.get("RIVET_BIN", "target/release/rivet")
SECS = int(sys.argv[1]) if len(sys.argv) > 1 else 15
TABLE = "harm_my"


def my(sql: str) -> str:
    return subprocess.run(
        ["docker", "exec", "-i", MYC, "mysql", "-urivet", "-privet", "-N", "-e", sql],
        capture_output=True, text=True,
    ).stdout.strip()


def binlog_total_bytes() -> int:
    rows = my("SHOW BINARY LOGS")
    total = 0
    for line in rows.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2:
            try:
                total += int(parts[1])
            except ValueError:
                pass
    return total


class Writer:
    """Persistent mysql client firing batch inserts as fast as it can; counts rows."""

    def __init__(self):
        self.rows = 0
        self.stop = threading.Event()
        self.t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        p = subprocess.Popen(
            ["docker", "exec", "-i", MYC, "mysql", "-urivet", "-privet", "rivet"],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True,
        )
        while not self.stop.is_set():
            try:
                p.stdin.write(
                    f"INSERT INTO {TABLE}(v, pad) VALUES "
                    + ",".join(f"({i},REPEAT('x',120))" for i in range(200))
                    + ";\n"
                )
                p.stdin.flush()
                self.rows += 200
            except BrokenPipeError:
                break
        try:
            p.stdin.close()
            p.wait(timeout=5)
        except Exception:
            p.kill()

    def start(self):
        self.t.start()

    def finish(self) -> int:
        self.stop.set()
        self.t.join(timeout=10)
        return self.rows


def cfg_path() -> str:
    d = tempfile.mkdtemp(prefix="harm_my_")
    out = os.path.join(d, "out")
    os.makedirs(out, exist_ok=True)
    ckpt = os.path.join(d, "my.ckpt")
    cfg = os.path.join(d, "cdc.yaml")
    with open(cfg, "w") as f:
        f.write(
            f'source: {{type: mysql, url: "{MY_URL}"}}\n'
            f"exports:\n  - name: {TABLE}\n    table: {TABLE}\n    mode: cdc\n"
            f"    format: parquet\n"
            f'    cdc: {{ until_current: true, checkpoint: "{ckpt}", server_id: 5000 }}\n'
            f'    destination: {{ type: local, path: "{out}" }}\n'
        )
    return cfg


def phase(with_cdc: bool) -> tuple:
    cfg = None
    stop_drain = threading.Event()
    drain_thread = None
    if with_cdc:
        cfg = cfg_path()
        subprocess.run([RIVET_BIN, "run", "--config", cfg], capture_output=True, text=True)  # anchor

        def drain():
            while not stop_drain.is_set():
                subprocess.run([RIVET_BIN, "run", "--config", cfg], capture_output=True, text=True)

        drain_thread = threading.Thread(target=drain, daemon=True)
        drain_thread.start()

    binlog0 = binlog_total_bytes()
    w = Writer()
    w.start()
    time.sleep(SECS)
    rows = w.finish()
    stop_drain.set()
    if drain_thread:
        drain_thread.join(timeout=30)
    binlog_grew = binlog_total_bytes() - binlog0
    return rows / SECS, binlog_grew


def main():
    print(f"MySQL CDC co-tenancy harm — {SECS}s/phase, table {TABLE}\n")
    my(f"DROP TABLE IF EXISTS {TABLE}")
    my(f"CREATE TABLE {TABLE} (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT, pad TEXT)")

    print("Phase 1/2: BASELINE (no CDC drain)")
    b_tps, b_binlog = phase(with_cdc=False)
    print("Phase 2/2: UNDER a looping MySQL CDC (binlog-dump) drain")
    u_tps, u_binlog = phase(with_cdc=True)

    print("\n" + "=" * 60)
    print("MySQL HARM REPORT")
    print("=" * 60)
    print(f"writer throughput (rows/s):  baseline {b_tps:.0f}   under-cdc {u_tps:.0f}   x{u_tps/b_tps:.2f}" if b_tps else "n/a")
    print(f"binlog grew (MB):            baseline {b_binlog/1_048_576:.0f}   under-cdc {u_binlog/1_048_576:.0f}")
    print("  → binlog growth is reader-INDEPENDENT (purged on the server's schedule);")
    print("    a lagging/abandoned MySQL CDC reader pins NO retention — no disk-fill harm.")
    print("=" * 60)
    my(f"DROP TABLE IF EXISTS {TABLE}")


if __name__ == "__main__":
    main()
