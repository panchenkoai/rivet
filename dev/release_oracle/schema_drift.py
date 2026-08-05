"""Is `schema_fingerprint` actually sensitive to a schema change?

The attestation ledger's remaining `none`. Drift detection compares this run's
fingerprint to the previous run's — both produced by the same function — so a
fingerprint blind to some change is blind on BOTH sides and reports `unchanged`.
Nothing derived the schema independently and disagreed.

An exact independent digest is not available here, and saying so is more useful
than faking one: the fingerprint hashes `<name>\\t<data_type>` where `data_type`
is rivet's OWN rendering of the Arrow type, so a second implementation would have
to reproduce that naming and would then be a copy rather than a witness. What CAN
be checked without rivet's cooperation is the property that matters — the
fingerprint must MOVE when the schema really moves, and must NOT move when
nothing did.

So the DDL is the independent party. Each case applies a real schema change to a
real table and requires the fingerprint to change; a control case re-exports an
untouched table and requires it not to. A fingerprint that ignores a rename, a
retype or a dropped column would still satisfy every drift check rivet performs
on itself, and would be caught here.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

try:
    from .core import Ledger, have, rivet, run
    from .scenarios import NO_TIMEOUT, _failed, _passed, _skipped, work_dir
except ImportError:  # pragma: no cover - depends on how the driver is invoked
    from core import Ledger, have, rivet, run  # type: ignore
    from scenarios import NO_TIMEOUT, _failed, _passed, _skipped, work_dir  # type: ignore

__all__ = ["verify_schema_fingerprint_moves_with_the_schema"]

#: (label, DDL, must the fingerprint change?)
#:
#: The `False` row is not filler — it is the control. Without it a fingerprint
#: that changed on EVERY export (a timestamp mixed in, say) would pass every
#: other case while being useless for drift detection.
CASES: list[tuple[str, str, bool]] = [
    ("add a column", "ALTER TABLE sf_probe ADD COLUMN extra INT", True),
    ("rename a column", "ALTER TABLE sf_probe RENAME COLUMN v TO v_renamed", True),
    ("retype a column", "ALTER TABLE sf_probe ALTER COLUMN n TYPE BIGINT", True),
    ("drop a column", "ALTER TABLE sf_probe DROP COLUMN extra", True),
    ("touch nothing", "SELECT 1", False),
]


def _fingerprint(dest: Path) -> str | None:
    m = dest / "manifest.json"
    if not m.exists():
        return None
    try:
        return json.loads(m.read_text()).get("schema_fingerprint")
    except (OSError, json.JSONDecodeError):
        return None


def verify_schema_fingerprint_moves_with_the_schema(
    led: Ledger, engine: str, tag: str, url: str
) -> None:
    if engine != "postgres":
        _skipped(
            led, engine, tag, "schema_fingerprint_moves_with_the_schema", "-",
            "schema-fingerprint: postgres only — the property is the fingerprint's, not an "
            "engine's, and one engine's DDL exercises it",
            "one engine",
        )
        return
    if not have("psql"):
        _skipped(
            led, engine, tag, "schema_fingerprint_moves_with_the_schema", "-",
            "schema-fingerprint: psql absent", "no psql",
        )
        return

    work = work_dir() / f"sf_{tag.replace('.', '_')}"
    out = work / "out"
    out.mkdir(parents=True, exist_ok=True)
    cfg = work / "sf.yaml"
    cfg.write_text(
        "source:\n  type: postgres\n  url_env: ORACLE_URL\n"
        "exports:\n"
        "  - name: sf_probe\n"
        "    table: public.sf_probe\n"
        "    mode: full\n"
        "    format: parquet\n"
        f"    destination: {{ type: local, path: {out} }}\n"
    )
    env = {"ORACLE_URL": url}

    def psql(sql: str) -> bool:
        return run(["psql", url, "-v", "ON_ERROR_STOP=1", "-c", sql], timeout=120).ok

    def export_fingerprint() -> str | None:
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True, exist_ok=True)
        if not rivet("run", "-c", str(cfg), env=env, timeout=NO_TIMEOUT).ok:
            return None
        return _fingerprint(out)

    if not psql(
        "DROP TABLE IF EXISTS sf_probe; "
        "CREATE TABLE sf_probe(id INT PRIMARY KEY, v TEXT, n INT); "
        "INSERT INTO sf_probe VALUES (1,'a',1),(2,'b',2);"
    ):
        _skipped(
            led, engine, tag, "schema_fingerprint_moves_with_the_schema", "-",
            "schema-fingerprint: could not seed the probe", "no seed",
        )
        return

    try:
        previous = export_fingerprint()
        if not previous:
            _failed(
                led, engine, tag, "schema_fingerprint_moves_with_the_schema", "-",
                "schema-fingerprint: the baseline export recorded no fingerprint",
                "no baseline",
            )
            return

        wrong: list[str] = []
        for label, ddl, must_change in CASES:
            if not psql(ddl):
                wrong.append(f"{label}: the DDL itself failed, so nothing was tested")
                continue
            current = export_fingerprint()
            if current is None:
                wrong.append(f"{label}: the export after the change recorded no fingerprint")
                continue
            changed = current != previous
            if changed != must_change:
                wrong.append(
                    f"{label}: fingerprint {'changed' if changed else 'did NOT change'} "
                    f"({previous} -> {current}), expected it to "
                    f"{'change' if must_change else 'stay'}"
                )
            previous = current

        if wrong:
            _failed(
                led, engine, tag, "schema_fingerprint_moves_with_the_schema", "-",
                "schema-fingerprint: " + "; ".join(wrong),
                "insensitive",
            )
            return
        _passed(
            led, engine, tag, "schema_fingerprint_moves_with_the_schema", "-",
            f"schema-fingerprint: moves for every real schema change "
            f"({len([c for c in CASES if c[2]])} kinds) and stays put when nothing changed",
        )
    finally:
        psql("DROP TABLE IF EXISTS sf_probe;")
