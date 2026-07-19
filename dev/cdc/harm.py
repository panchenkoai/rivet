#!/usr/bin/env python3
"""CDC harm-under-load measurement (PostgreSQL).

Measures the SOURCE-SIDE cost of running a rivet CDC drain against a database
under a sustained, paced OLTP writer — the "harm" the CDC slot charges the source:

  1. co-tenancy: the writer's per-statement latency (p50/p99/max) WITH the CDC
     drain looping vs a WITHOUT-CDC baseline;
  2. replication lag: pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn);
  3. slot WAL retention: pg_current_wal_lsn() - restart_lsn (the disk-fill harm);
  4. catalog_xmin age: age(catalog_xmin) of the slot (blocks VACUUM).

Dependency-free: talks to postgres through a persistent `psql` inside the
postgres-cdc container (server-side \\timing, no host psql / driver needed), and
drives rivet via subprocess. Devbox is macOS (RIVET_BIN builds to target/release).

Usage:
  RIVET_BIN=target/release/rivet python3 dev/cdc/harm.py [seconds] [rate_per_s]
Defaults: 30 s per phase, 200 statements/s.
"""

import os
import subprocess
import sys
import tempfile
import threading
import time

PGC = os.environ.get("PGC_CONTAINER", "rivet-postgres-cdc-1")
PG_URL = os.environ.get("POSTGRES_CDC_URL", "postgresql://rivet:rivet@127.0.0.1:5434/rivet")
RIVET_BIN = os.environ.get("RIVET_BIN", "target/release/rivet")
SECONDS = int(sys.argv[1]) if len(sys.argv) > 1 else 30
RATE = int(sys.argv[2]) if len(sys.argv) > 2 else 200
TABLE = "harm_writes"
SLOT = "harm_slot"


def psql(sql: str) -> str:
    """One-shot query inside the postgres-cdc container; returns trimmed scalar/text."""
    out = subprocess.run(
        ["docker", "exec", "-i", PGC, "psql", "-U", "rivet", "-d", "rivet", "-tAc", sql],
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


def sample_harm() -> dict:
    """The slot's source-side harm gauges, from PostgreSQL itself (never rivet counters)."""
    row = psql(
        "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),0)::bigint, "
        "       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint, "
        "       COALESCE(age(catalog_xmin),0) "
        f"FROM pg_replication_slots WHERE slot_name='{SLOT}'"
    )
    parts = (row or "0|0|0").split("|")
    return {
        "lag_b": int(parts[0] or 0),
        "retained_b": int(parts[1] or 0),
        "xmin_age": int(parts[2] or 0),
    }


class PacedWriter:
    """A persistent psql that fires one INSERT per tick at ~RATE/s, recording each
    statement's SERVER-SIDE latency (psql \\timing 'Time: N ms')."""

    def __init__(self):
        self.latencies_ms: list[float] = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        p = subprocess.Popen(
            ["docker", "exec", "-i", PGC, "psql", "-U", "rivet", "-d", "rivet", "-q"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        p.stdin.write("\\timing on\n")
        p.stdin.flush()
        interval = 1.0 / RATE
        i = 0
        while not self.stop.is_set():
            t0 = time.perf_counter()
            p.stdin.write(
                f"INSERT INTO {TABLE}(v, pad) VALUES ({i}, repeat('x',120));\n"
            )
            p.stdin.flush()
            # read until the 'Time:' line psql prints per statement
            while True:
                line = p.stdout.readline()
                if not line:
                    break
                if line.startswith("Time:"):
                    try:
                        self.latencies_ms.append(float(line.split()[1]))
                    except (IndexError, ValueError):
                        pass
                    break
            i += 1
            sleep = interval - (time.perf_counter() - t0)
            if sleep > 0:
                time.sleep(sleep)
        try:
            p.stdin.write("\\q\n")
            p.stdin.flush()
            p.wait(timeout=5)
        except Exception:
            p.kill()

    def start(self):
        self.thread.start()

    def finish(self) -> dict:
        self.stop.set()
        self.thread.join(timeout=10)
        return pctl(self.latencies_ms)


def pctl(xs: list[float]) -> dict:
    if not xs:
        return {"n": 0, "p50": 0.0, "p99": 0.0, "max": 0.0}
    s = sorted(xs)
    return {
        "n": len(s),
        "p50": s[len(s) // 2],
        "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
        "max": s[-1],
    }


def rivet_cdc_config(d: str) -> str:
    out = os.path.join(d, "out")
    os.makedirs(out, exist_ok=True)
    ckpt = os.path.join(d, "harm.ckpt")
    cfg = os.path.join(d, "harm_cdc.yaml")
    with open(cfg, "w") as f:
        f.write(
            f'source: {{type: postgres, url: "{PG_URL}"}}\n'
            "exports:\n"
            f"  - name: {TABLE}\n"
            f"    table: {TABLE}\n"
            "    mode: cdc\n"
            "    format: parquet\n"
            f"    cdc: {{ slot: {SLOT}, until_current: true, rollover: 50000 }}\n"
            f'    destination: {{ type: local, path: "{out}" }}\n'
        )
    return cfg


def drain_loop(cfg: str, stop: threading.Event, stats: dict):
    """Back-to-back bounded until_current drains — the scheduler model hammering the source."""
    runs = 0
    while not stop.is_set():
        subprocess.run(
            [RIVET_BIN, "run", "--config", cfg],
            capture_output=True,
            text=True,
        )
        runs += 1
    stats["runs"] = runs


def phase(label: str, with_cdc: bool) -> dict:
    writer = PacedWriter()
    peak = {"lag_b": 0, "retained_b": 0, "xmin_age": 0}
    stop_sampler = threading.Event()

    def sampler():
        while not stop_sampler.is_set():
            h = sample_harm()
            for k in peak:
                peak[k] = max(peak[k], h[k])
            time.sleep(1.0)

    samp = threading.Thread(target=sampler, daemon=True)
    samp.start()

    drain_stats: dict = {"runs": 0}
    stop_drain = threading.Event()
    drain_thread = None
    cfg = None
    if with_cdc:
        d = tempfile.mkdtemp(prefix="harm_")
        cfg = rivet_cdc_config(d)
        # prime the slot with one run so it exists before the writer floods it
        subprocess.run([RIVET_BIN, "run", "--config", cfg], capture_output=True, text=True)
        drain_thread = threading.Thread(target=drain_loop, args=(cfg, stop_drain, drain_stats), daemon=True)
        drain_thread.start()

    print(f"  [{label}] running {SECONDS}s at {RATE} stmt/s ...", flush=True)
    writer.start()
    time.sleep(SECONDS)
    lat = writer.finish()
    stop_drain.set()
    if drain_thread:
        drain_thread.join(timeout=30)
    stop_sampler.set()
    samp.join(timeout=5)

    return {"lat": lat, "peak": peak, "drain_runs": drain_stats.get("runs", 0)}


def main():
    print(f"CDC harm-under-load — {SECONDS}s/phase, {RATE} stmt/s, table {TABLE}, slot {SLOT}\n")
    # fresh table + slot
    psql(f"DROP TABLE IF EXISTS {TABLE}")
    psql(f"CREATE TABLE {TABLE} (id bigserial primary key, v int, pad text)")
    psql(f"SELECT pg_drop_replication_slot('{SLOT}') "
         f"WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{SLOT}')")

    print("Phase 1/2: BASELINE (no CDC drain)")
    base = phase("baseline", with_cdc=False)

    print("Phase 2/2: UNDER a looping CDC drain")
    psql(f"TRUNCATE {TABLE}")
    under = phase("under-cdc", with_cdc=True)

    b, u = base["lat"], under["lat"]
    p = under["peak"]
    print("\n" + "=" * 62)
    print("HARM REPORT")
    print("=" * 62)
    print(f"writer statements:   baseline n={b['n']}   under-cdc n={u['n']}")
    print(f"                     (cdc drain ran {under['drain_runs']} bounded passes)")
    print("writer latency (ms, server-side):")
    print(f"  p50   baseline {b['p50']:.2f}   under-cdc {u['p50']:.2f}   x{ratio(u['p50'], b['p50'])}")
    print(f"  p99   baseline {b['p99']:.2f}   under-cdc {u['p99']:.2f}   x{ratio(u['p99'], b['p99'])}")
    print(f"  max   baseline {b['max']:.2f}   under-cdc {u['max']:.2f}   x{ratio(u['max'], b['max'])}")
    print("slot harm under the drain (peak, from pg_replication_slots):")
    print(f"  replication lag        {mb(p['lag_b'])}  (current - confirmed_flush)")
    print(f"  WAL retained by slot   {mb(p['retained_b'])}  (current - restart_lsn — disk-fill)")
    print(f"  catalog_xmin age       {p['xmin_age']}  (vacuum-blocking)")
    print("=" * 62)


def ratio(a: float, b: float) -> str:
    return f"{a / b:.2f}" if b else "n/a"


def mb(n: int) -> str:
    return f"{n / 1_048_576:.1f} MB"


if __name__ == "__main__":
    main()
