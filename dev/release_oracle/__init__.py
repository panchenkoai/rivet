"""Rivet Release Oracle — the go/no-go pre-release gate.

A Python port of `dev/release-oracle/*.sh`, kept output-identical so the two can
be compared transcript-to-transcript rather than trusted. See `core` for why the
port exists (three separate bugs that were shell semantics, not logic).

RULE FOR ANY PER-VERSION CELL: key every name by the dimension the runner
parallelises over, and take the SAME lock as every other toucher of a shared
fixture.

`--version-parallel` runs N versions of one engine as THREADS IN ONE PROCESS,
against stands that are per-version (the matrix containers) OR shared
(`RIVET_CDC_<ENGINE>_URL` is a fixed env var — one database for every version of
that engine). A cell that forgets this looks correct and fails as the PRODUCT:

  * a fixed probe table (`orc_cdc_probe`) on the shared CDC stand: version A's
    cleanup drops it mid-capture for B. Surfaced as "the capture wrote no part"
    on 3 of 7 postgres versions and "Table not found" in another cell — read as
    a rivet bug for a whole run. The lock lives in `cdc._cdc_lock_for`; EVERY
    per-version toucher must take it (blessed_flow did, corruption.py did not).
  * a config path keyed by pid+engine, or by a destination SUBDIRECTORY whose
    name is a constant ("users", "full"): N versions write N destinations into
    ONE file, so a cell corrupts its own part and then validates another
    version's clean one. Surfaced as "validate PASSED on a part whose bytes were
    changed" and as `src[150000]!=declared[300000]`. Key by the PARENT (which
    carries engine+version), not the leaf.

Preflight callers are exempt BY POSITION, not by luck: `preflight()` runs once,
serially, before any version container exists. Check which side a new cell is on
before copying either pattern.
"""
