"""Mongo keyset: `source.mongo.page_size` pages `_id` sequentially, and `parallel: N` fans out
`_id` ranges (`run_mongo_parallel`). Both must deliver every document once, and a
heterogeneous `_id` must be REFUSED (`ensure_uniform_id_type`) — `$gt` is BSON-type-bracketed,
so a keyset over int+string `_id`s would otherwise export one bracket and report success.
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

try:
    from .core import Ledger, rivet
    from .duck import Oracle
    from .scenarios import NO_TIMEOUT, Scope, _declared_read, _failed, _passed, _skipped
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import Ledger, rivet  # type: ignore
    from duck import Oracle  # type: ignore
    from scenarios import NO_TIMEOUT, Scope, _declared_read, _failed, _passed, _skipped  # type: ignore

__all__ = ["sc_mongo_keyset"]

#: One version is enough: the runner is shared code, and the per-version loss/dup arm already runs under integrity_types.
MONGO_KEYSET_TAG = "8"
DOCS = 6000
PAGE = 1000
SCEN = "mongo_keyset"


def _export(url: str, coll: str, dest: Path, parallel: int | None) -> tuple[int, str]:
    """Run one keyset export of `coll` to `dest`; returns (exit code, stderr)."""
    shutil.rmtree(dest, ignore_errors=True)
    cfg = dest.with_suffix(".yaml")
    par = f"    parallel: {parallel}\n" if parallel else ""
    cfg.write_text(
        f"source:\n  type: mongo\n  url_env: ORACLE_URL\n  mongo:\n    page_size: {PAGE}\n"
        f"exports:\n  - name: {coll}\n    table: {coll}\n    mode: full\n{par}"
        f"    format: parquet\n    destination: {{ type: local, path: {dest} }}\n"
    )
    p = rivet("run", "-c", str(cfg), env={"ORACLE_URL": url}, timeout=NO_TIMEOUT)
    return p.returncode, p.stderr


def sc_mongo_keyset(led: Ledger, engine: str, tag: str, url: str) -> None:
    """Sequential + parallel:4 keyset deliver the source exactly (DuckDB over declared parts, >=2 range parts); mixed `_id` is refused with zero parts."""
    if engine != "mongo" or tag != MONGO_KEYSET_TAG:
        return
    try:
        import pymongo
    except ImportError:
        _skipped(led, engine, tag, SCEN, "-", "mongo-keyset: pymongo absent", "no pymongo")
        return
    sc = Scope(engine, tag)
    uniform, mixed = sc.name("mk", str(os.getpid())), sc.name("mkh", str(os.getpid()))
    client = pymongo.MongoClient(url, serverSelectionTimeoutMS=5000)
    db = client.get_default_database("rivet")
    try:
        try:
            db[uniform].drop()
            db[uniform].insert_many([{"_id": i, "v": f"v{i}"} for i in range(1, DOCS + 1)])
            db[mixed].drop()
            db[mixed].insert_many([{"_id": i, "v": "n"} for i in range(1, 51)]
                                  + [{"_id": f"s{i:03d}", "v": "s"} for i in range(50)])
        except pymongo.errors.PyMongoError as e:
            _skipped(led, engine, tag, SCEN, "-", f"mongo-keyset: cannot seed ({e})", "seed")
            return
        src = f"{db[uniform].count_documents({})} {len(db[uniform].distinct('_id'))}"
        fails, counts = [], {}
        for label, par, stamp in (("sequential", None, r"_keyset_([^/']+)\.parquet"), ("parallel:4", 4, r"_w(\d+)_keyset")):
            dest = sc.dir("mk", label.replace(":", ""))
            rc, err = _export(url, uniform, dest, par)
            if rc != 0:
                fails.append(f"{label} exit {rc}: {err.strip()[-160:]}")
                continue
            lst = _declared_read(dest, ".parquet")
            if lst is None:
                fails.append(f"{label}: nothing declared")
                continue
            with Oracle() as o:
                got = o.scalar(f"SELECT count(*)||' '||count(DISTINCT _id) FROM read_parquet({lst})")
            ranges = {m for f in lst.split(",") for m in re.findall(stamp, f)}
            counts[label] = len(ranges)
            if got != src:
                fails.append(f"{label}: source {src} != declared {got}")
            if len(ranges) < 2:
                fails.append(f"{label}: {len(ranges)} range part(s), need >=2")
        for label, par in (("mixed sequential", None), ("mixed parallel:4", 4)):
            dest = sc.dir("mkh", label.split()[-1].replace(":", ""))
            rc, err = _export(url, mixed, dest, par)
            parts = list(dest.rglob("*.parquet")) if dest.exists() else []
            if rc == 0 or parts:
                fails.append(f"{label}: heterogeneous _id NOT refused (exit {rc}, {len(parts)} part(s))")
            elif "heterogeneous" not in err:
                fails.append(f"{label}: refused for another reason: {err.strip()[-160:]}")
        if fails:
            _failed(led, engine, tag, SCEN, "-", "mongo-keyset: " + "; ".join(fails), "; ".join(fails)[:200])
        else:
            _passed(led, engine, tag, SCEN, "-",
                    f"mongo-keyset: sequential + parallel:4 == source ({src}), range parts {counts}; "
                    f"int+string _id refused with 0 parts on both")
    finally:
        db[uniform].drop()
        db[mixed].drop()
        client.close()
