"""The NEGATIVE half of verification: `rivet validate` must FAIL on real corruption.

Every other cell in this gate asks "does a good run come out green?". This one
asks the question that actually decides whether verification means anything:
**when the data on disk is wrong, does rivet say so?**

It exists because the answer was no. On 2026-08-02 a part was corrupted in place
— one byte inside a dictionary page, `x` -> `y`, file length unchanged, manifest
and `_SUCCESS` untouched — and `rivet validate --depth full` returned PASSED,
exit 0, while DuckDB and rivet's own Arrow reader both read the changed values.
The cause was the value-checksum fold: per-cell hashes were combined with XOR,
which annihilates (`h ^ h == 0`), so a column of duplicated values checksummed to
zero regardless of content and corruption of it was invisible. The fold is now a
wrapping sum, versioned in the manifest as `checksum_render`.

A fix with no negative test is a fix that regresses quietly, so this runs per
engine, and it is deliberately shaped to fail if verification weakens in any of
three ways: the corruption goes undetected, the failure is reported as an
operational error rather than data-integrity (exit 3), or the message stops
naming which column diverged.

The corruption is byte-level ON PURPOSE. Rewriting the part with DuckDB also
works, but it changes the file LENGTH, so the size check fires first and the
value checksum is never reached — the test would then pass while proving nothing
about Form B. Same-length in-place mutation is the only shape that isolates it.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

try:
    from .core import Ledger, have, rivet
    from .scenarios import NO_TIMEOUT, _failed, _passed, _skipped, work_dir
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import Ledger, have, rivet  # type: ignore
    from scenarios import NO_TIMEOUT, _failed, _passed, _skipped, work_dir  # type: ignore

__all__ = ["verify_corruption_is_detected"]

#: Same seeded probe the row-hash cell uses — it carries duplicated values
#: (`x` twice), which is exactly the shape the old XOR fold cancelled to zero.
PROBE = {
    "postgres": "public.row_hash_probe",
    "mysql": "row_hash_probe",
    "mssql": "dbo.row_hash_probe",
}

#: rivet's data-integrity exit code. A corrupted part must be classified as
#: verified-WRONG, not as could-not-verify (exit 1) — an operator triaging exit 1
#: looks for a broken credential, not for corrupt data.
EXIT_DATA_INTEGRITY = 3


def _export(engine: str, url: str, table: str, dest: Path) -> Path | None:
    yaml_path = work_dir() / f"corrupt_{os.getpid()}_{engine}.yaml"
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True, exist_ok=True)
    tls = "\n  tls: {accept_invalid_certs: true}" if engine == "mssql" else ""
    yaml_path.write_text(
        f"source:\n"
        f"  type: {engine}\n"
        f"  url_env: ORACLE_URL{tls}\n"
        f"exports:\n"
        f"  - name: corrupt_probe\n"
        f"    table: {table}\n"
        f"    mode: full\n"
        f"    format: parquet\n"
        f"    destination: {{type: local, path: {dest}/}}\n"
    )
    if not rivet("run", "-c", str(yaml_path), env={"ORACLE_URL": url}, timeout=NO_TIMEOUT).ok:
        return None
    return yaml_path


def _flip_one_byte(part: Path) -> tuple[int, int] | None:
    """Change one value byte in place. Returns (offset, size) or None.

    The file LENGTH must not change — that is the whole point. A rewrite trips
    the size check and never reaches the value checksum.
    """
    body = bytearray(part.read_bytes())
    before = len(body)
    i = body.find(b"x")
    if i < 0:
        return None
    body[i] = ord("y")
    part.write_bytes(bytes(body))
    return i, before


def verify_corruption_is_detected(led: Ledger, engine: str, tag: str, url: str) -> None:
    table = PROBE.get(engine)
    if table is None:
        _skipped(
            led, engine, tag, "corruption_is_detected", "-",
            f"corruption-detected: no probe table for {engine} (the golden seed is SQL-engine only)",
            "no probe",
        )
        return
    if not have("duckdb"):
        _skipped(led, engine, tag, "corruption_is_detected", "-", "corruption-detected: duckdb absent", "no duckdb")
        return

    dest = work_dir() / f"corrupt_{engine}_{tag.replace('.', '_')}"
    cfg = _export(engine, url, table, dest)
    if cfg is None:
        _failed(led, engine, tag, "corruption_is_detected", "-", f"corruption-detected[{engine}]: export failed", "export")
        return

    # A clean run must be GREEN first: a cell that only ever sees red proves the
    # command fails, not that it discriminates.
    clean = rivet("validate", "-c", str(cfg), "--depth", "full", timeout=NO_TIMEOUT)
    if not clean.ok:
        _failed(
            led, engine, tag, "corruption_is_detected", "-",
            f"corruption-detected[{engine}]: validate FAILED on an untouched export — "
            f"the negative test cannot mean anything until the positive one holds: {clean.out[-200:]}",
            "clean red",
        )
        return

    # The manifest must say which fold produced its checksums; without the marker
    # a reader silently falls back to the v1 (annihilating) fold.
    manifest = dest / "manifest.json"
    render = None
    if manifest.exists():
        try:
            render = json.loads(manifest.read_text()).get("checksum_render")
        except (OSError, json.JSONDecodeError):
            render = None
    if not render:
        _failed(
            led, engine, tag, "corruption_is_detected", "-",
            f"corruption-detected[{engine}]: the manifest records no `checksum_render` — a reader "
            f"cannot tell which fold produced these checksums and falls back to the annihilating one",
            "no render id",
        )
        return

    parts = sorted(dest.rglob("*.parquet"))
    if not parts:
        _failed(led, engine, tag, "corruption_is_detected", "-", f"corruption-detected[{engine}]: no part written", "no part")
        return
    flipped = _flip_one_byte(parts[0])
    if flipped is None:
        _failed(
            led, engine, tag, "corruption_is_detected", "-",
            f"corruption-detected[{engine}]: the probe part contains no 'x' byte to flip — the "
            f"golden seed's row_hash_probe drifted, so this cell would silently test nothing",
            "no target byte",
        )
        return
    offset, size_before = flipped
    if parts[0].stat().st_size != size_before:
        _failed(
            led, engine, tag, "corruption_is_detected", "-",
            f"corruption-detected[{engine}]: the flip changed the file length — the size check "
            f"would fire first and the VALUE checksum would never be reached",
            "size changed",
        )
        return

    bad = rivet("validate", "-c", str(cfg), "--depth", "full", timeout=NO_TIMEOUT)
    text = (bad.out or "") + (getattr(bad, "err", "") or "")
    if bad.ok:
        _failed(
            led, engine, tag, "corruption_is_detected", "-",
            f"corruption-detected[{engine}]: validate PASSED on a part whose bytes were changed "
            f"in place at offset {offset} — verification is not verifying",
            "undetected",
        )
        return
    if bad.returncode != EXIT_DATA_INTEGRITY:
        _failed(
            led, engine, tag, "corruption_is_detected", "-",
            f"corruption-detected[{engine}]: corruption reported with exit {bad.returncode}, expected "
            f"{EXIT_DATA_INTEGRITY} (data-integrity). An operator triaging exit 1 hunts a broken "
            f"credential, not corrupt data",
            "wrong exit",
        )
        return
    if "value checksum mismatch" not in text or "column 'b'" not in text:
        _failed(
            led, engine, tag, "corruption_is_detected", "-",
            f"corruption-detected[{engine}]: failed, but not as a value-checksum mismatch naming the "
            f"diverging column — a verdict that cannot say WHAT diverged sends the operator hunting: "
            f"{text[-200:]}",
            "unnamed",
        )
        return

    _passed(
        led, engine, tag, "corruption_is_detected", "-",
        f"corruption-detected[{engine}]: clean export verifies; a same-length in-place byte flip is "
        f"caught as a value-checksum mismatch on column 'b' (exit {EXIT_DATA_INTEGRITY})",
    )
