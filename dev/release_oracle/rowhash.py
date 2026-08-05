"""`_rivet_row_hash` verification — the one integrity column rivet asks a reader
to trust, checked by something that is not rivet.

Why this exists as its own gate cell. The auditor recomputes the hash with
rivet's OWN `enrich` function, and `src/lib.rs` says so deliberately: "it must do
so with THIS function. A reimplementation on the consumer side would be a second
thing that can drift from the extractor." That is sound for its purpose — telling
whether the DATA changed between extract and audit — and structurally incapable
of catching a defect in the SPECIFICATION, because both sides share it. Render id
v1 shipped non-injective in two independent ways and lived four months: fields
were joined by a bare \\x1f with no length, so a value carrying that byte forged a
field boundary, and containers were hashed from Arrow's lossy display text.

So this cell holds the other side of the comparison. It rebuilds the canonical
image from the SPEC in `src/enrich.rs` — presence tag, u32 length, rendered bytes
— in Python, hashes it with the `xxhash` package, and compares. DuckDB supplies
the values and never computes the hash; the digest is produced by a different
language, a different library and a different reading of the spec, which is what
makes a divergence mean something.

Two checks, because two different things can be wrong and neither implies the
other:

  A. DIGEST PARITY — the independent implementation must reproduce the column
     exactly. Catches a drift in the rendering or the framing.
  B. INJECTIVITY — two rows that differ only in WHERE a field boundary falls must
     hash differently. Catches the v1 class directly.

B is the subtle one, and it needs the coverage to EXCLUDE the primary key: with a
unique column in the hash no two rows can collide however the framing works, so
the same probe under `row_hash: true` passes against a length-less join. Measured
— under v1's rendering the probe scores 4/4 distinct with the key covered and 3/4
without it. Hence two exports of one table.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

try:
    from .core import Ledger, Status, have, rivet, run
    from .scenarios import NO_TIMEOUT, _duckdb_list, _failed, _passed, _skipped, work_dir
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import Ledger, Status, have, rivet, run  # type: ignore
    from scenarios import NO_TIMEOUT, _duckdb_list, _failed, _passed, _skipped, work_dir  # type: ignore

__all__ = ["verify_row_hash", "canonical_image", "row_hash_of"]

TAG_NULL = 0x00
TAG_PRESENT = 0x01

#: The rendering this implementation reproduces. If the product's
#: `ROW_HASH_RENDER_ID` moves past this, the check must be re-derived from the new
#: spec rather than left comparing different bytes.
KNOWN_RENDER = "xxh3-128-i64-arrow-display-us-v2"

#: The probe table the golden seed carries, per engine's schema convention.
PROBE = {"postgres": "public.row_hash_probe", "mysql": "row_hash_probe", "mssql": "dbo.row_hash_probe"}


def canonical_image(cells: list[str | None]) -> bytes:
    """The canonical bytes of one row, per `src/enrich.rs`.

    A leaf is: absence tag, or presence tag + u32 little-endian length + the
    rendered bytes. The LENGTH is the whole point — a bare separator lets a value
    imitate a field boundary, which is precisely what v1 allowed.
    """
    out = bytearray()
    for c in cells:
        if c is None:
            out.append(TAG_NULL)
            continue
        b = c.encode("utf-8")
        out.append(TAG_PRESENT)
        out += len(b).to_bytes(4, "little")
        out += b
    return bytes(out)


def row_hash_of(cells: list[str | None]) -> int:
    """xxh3-128 of the canonical image, truncated the way `as i64` truncates."""
    import xxhash

    low = xxhash.xxh3_128_intdigest(canonical_image(cells)) & 0xFFFF_FFFF_FFFF_FFFF
    return low - (1 << 64) if low >= (1 << 63) else low


def _export(engine: str, url: str, table: str, dest: Path, row_hash: str) -> bool:
    """Export the probe with a given `meta_columns.row_hash` setting."""
    yaml_path = work_dir() / f"rh_{os.getpid()}_{row_hash.strip('[] ').replace(', ', '_')}.yaml"
    shutil.rmtree(dest, ignore_errors=True)
    tls = "\n  tls: {accept_invalid_certs: true}" if engine == "mssql" else ""
    yaml_path.write_text(
        f"source:\n"
        f"  type: {engine}\n"
        f"  url_env: ORACLE_URL{tls}\n"
        f"exports:\n"
        f"  - name: rh_probe\n"
        f"    table: {table}\n"
        f"    mode: full\n"
        f"    format: parquet\n"
        f"    meta_columns:\n"
        f"      row_hash: {row_hash}\n"
        f"    destination: {{type: local, path: {dest}/}}\n"
    )
    return rivet("run", "-c", str(yaml_path), env={"ORACLE_URL": url}, timeout=NO_TIMEOUT).ok


def _manifest_coverage(dest: Path) -> object | None:
    """What the run says its hash covers — read from the artifact, never assumed.

    A hash over `(a, b)` compared against a recomputation over `(id, a, b)` is a
    valid-looking column attesting a different text, so the contract travels with
    the data for exactly this reader.
    """
    for m in sorted(dest.rglob("manifest*.json")):
        try:
            doc = json.loads(m.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        found: list[object] = []

        def walk(o: object) -> None:
            if isinstance(o, dict):
                for k, v in o.items():
                    if k == "row_hash" and isinstance(v, dict) and "covered" in v:
                        found.append(v)
                    walk(v)
            elif isinstance(o, list):
                for i in o:
                    walk(i)

        walk(doc)
        if found:
            return found[0]
    return None


def _rows(dest: Path, cols: list[str]) -> list[dict]:
    sel = ", ".join([*cols, "_rivet_row_hash"])
    q = f"SELECT {sel} FROM read_parquet('{dest}/**/*.parquet') ORDER BY 1"
    p = subprocess.run(["duckdb", "-json", "-c", q], capture_output=True, text=True)
    try:
        return json.loads(p.stdout)
    except json.JSONDecodeError:
        return []


def verify_row_hash(led: Ledger, engine: str, tag: str, url: str) -> None:
    """Check A (digest parity) and Check B (injectivity) on the seeded probe."""
    table = PROBE.get(engine)
    if table is None:
        _skipped(
            led, engine, tag, "row_hash", "-",
            f"row_hash: no probe table for {engine} (the golden seed is SQL-engine only)",
            "no probe",
        )
        return
    try:
        import xxhash  # noqa: F401
    except ImportError:
        _skipped(
            led, engine, tag, "row_hash", "-",
            "row_hash: the independent implementation needs the `xxhash` package "
            "(pip install xxhash) — SKIP rather than compare rivet against itself",
            "no xxhash",
        )
        return
    if not have("duckdb"):
        _skipped(led, engine, tag, "row_hash", "-", "row_hash: duckdb absent", "no duckdb")
        return

    base = work_dir() / f"rh_{engine}_{tag.replace('.', '_')}"

    # ── Check A: an independent implementation must reproduce the column ──
    full = base / "full"
    if not _export(engine, url, table, full, "true"):
        _failed(led, engine, tag, "row_hash", "-", f"row_hash[{engine}]: probe export failed", "export")
        return
    contract = _manifest_coverage(full)
    if not isinstance(contract, dict):
        _failed(
            led, engine, tag, "row_hash", "-",
            f"row_hash[{engine}]: the run manifest carries no row-hash contract, so a "
            f"reader cannot know what the column covers",
            "no contract",
        )
        return
    if contract.get("render") != KNOWN_RENDER:
        _skipped(
            led, engine, tag, "row_hash", "-",
            f"row_hash[{engine}]: render id is {contract.get('render')!r}, this checker "
            f"implements {KNOWN_RENDER!r} — re-derive it from the new spec rather than "
            f"compare different bytes",
            "render moved",
        )
        return

    cols = ["id", "a", "b"]  # `covered: true` == every projected source column
    rows = _rows(full, cols)
    if len(rows) != 4:
        _failed(
            led, engine, tag, "row_hash", "-",
            f"row_hash[{engine}]: probe re-read gave {len(rows)} rows, expected 4 — the "
            f"golden seed's row_hash_probe is missing or drifted",
            "bad fixture",
        )
        return
    mismatched = [
        f"id={r['id']}: rivet={r['_rivet_row_hash']} independent={row_hash_of([str(r['id']), r['a'], r['b']])}"
        for r in rows
        if row_hash_of([str(r["id"]), r["a"], r["b"]]) != r["_rivet_row_hash"]
    ]

    # ── Check B: injectivity, with the primary key OUT of the coverage ──
    pair = base / "pair"
    if not _export(engine, url, table, pair, "[a, b]"):
        _failed(led, engine, tag, "row_hash", "-", f"row_hash[{engine}]: [a,b] export failed", "export")
        return
    prows = _rows(pair, ["id"])
    hashes = [r["_rivet_row_hash"] for r in prows]
    collisions = {h: [r["id"] for r in prows if r["_rivet_row_hash"] == h] for h in set(hashes) if hashes.count(h) > 1}

    if mismatched or collisions or len(prows) != 4:
        detail = []
        if mismatched:
            detail.append("digest parity: " + "; ".join(mismatched))
        if collisions:
            detail.append(
                "INJECTIVE FAILURE — rows differing only in where a field boundary "
                f"falls share one hash: {collisions}"
            )
        if len(prows) != 4:
            detail.append(f"[a,b] export gave {len(prows)} rows, expected 4")
        _failed(led, engine, tag, "row_hash", "-", f"row_hash[{engine}]: " + " | ".join(detail), "mismatch")
        return

    _passed(
        led, engine, tag, "row_hash", "-",
        f"row_hash[{engine}]: 4/4 reproduced by an independent implementation, "
        f"4/4 distinct with the key out of coverage",
    )
