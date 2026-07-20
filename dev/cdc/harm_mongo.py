#!/usr/bin/env python3
"""MongoDB CDC co-tenancy harm.

Like MySQL, Mongo has NO reader-pinned retention: the oplog is a fixed-size CAPPED
collection, overwritten on its own schedule regardless of any change-stream reader
— a lagging/abandoned rivet Mongo CDC run pins nothing, so there is no disk-fill
harm (the PostgreSQL logical-slot hazard is absent). The only source-side cost is
CO-TENANCY: does the change-stream drain degrade the writer? Compare the writer's
throughput (docs/s) WITHOUT vs WITH a looping until_current Mongo CDC drain.

Usage: RIVET_BIN=target/release/rivet python3 dev/cdc/harm_mongo.py [secs]
"""
import os
import subprocess
import sys
import tempfile
import threading
import time

MOC = os.environ.get("MOC_CONTAINER", "rivet-mongo-rs-1")
DB = "harmdb"
COLL = "harm_mo"
MO_URL = os.environ.get("MONGO_CDC_URL", f"mongodb://127.0.0.1:27018/{DB}?directConnection=true")
RIVET_BIN = os.environ.get("RIVET_BIN", "target/release/rivet")
SECS = int(sys.argv[1]) if len(sys.argv) > 1 else 15


def mongosh(js: str) -> str:
    return subprocess.run(
        ["docker", "exec", "-i", MOC, "mongosh", "--quiet", "--port", "27017", DB, "--eval", js],
        capture_output=True, text=True,
    ).stdout.strip()


def oplog_is_capped() -> str:
    return mongosh("const s=db.getSiblingDB('local').oplog.rs.stats(); print(s.capped, Math.round(s.maxSize/1048576)+'MB cap')")


def writer_run(secs: int) -> int:
    """A JS insert loop for `secs` seconds; returns the doc count (throughput proxy)."""
    js = (
        "let n=0; const end=Date.now()+%d*1000; "
        "const b=[]; for(let i=0;i<200;i++) b.push({v:i, pad:'x'.repeat(120)}); "
        "while(Date.now()<end){ db.%s.insertMany(b); n+=200; } print(n)" % (secs, COLL)
    )
    out = mongosh(js)
    try:
        return int(out.splitlines()[-1])
    except (ValueError, IndexError):
        return 0


def cfg_path() -> str:
    d = tempfile.mkdtemp(prefix="harm_mo_")
    out = os.path.join(d, "out")
    os.makedirs(out, exist_ok=True)
    ckpt = os.path.join(d, "mo.ckpt")
    cfg = os.path.join(d, "cdc.yaml")
    with open(cfg, "w") as f:
        f.write(
            f'source: {{type: mongo, url: "{MO_URL}"}}\n'
            f"exports:\n  - name: {COLL}\n    table: {COLL}\n    mode: cdc\n"
            f"    format: parquet\n"
            f'    cdc: {{ until_current: true, checkpoint: "{ckpt}" }}\n'
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
    print(f"MongoDB CDC co-tenancy harm — {SECS}s/phase, {DB}.{COLL}\n")
    mongosh(f"db.{COLL}.drop()")
    print("oplog:", oplog_is_capped())

    print("Phase 1/2: BASELINE (no CDC drain)")
    b = phase(with_cdc=False)
    print("Phase 2/2: UNDER a looping Mongo CDC (change-stream) drain")
    u = phase(with_cdc=True)

    print("\n" + "=" * 60)
    print("MongoDB HARM REPORT")
    print("=" * 60)
    print(f"writer throughput (docs/s):  baseline {b:.0f}   under-cdc {u:.0f}   x{u/b:.2f}" if b else "n/a")
    print("  → oplog is a CAPPED collection (fixed size, reader-independent);")
    print("    a lagging/abandoned Mongo CDC reader pins NO retention — no disk-fill harm.")
    print("=" * 60)
    mongosh(f"db.{COLL}.drop()")


if __name__ == "__main__":
    main()
