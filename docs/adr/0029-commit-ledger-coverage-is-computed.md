# ADR-0029: The commit ledger separates observations from integrity, and computes coverage rather than trusting it

**Status**: Proposed (design accepted; implementation not started)
**Date**: 2026-08
**Relates to**: ADR-0028 (the finalize seam this ledger feeds), ADR-0012 (manifest durability ordering)

---

## Context

ADR-0028 introduced `CommitLedger`: runners FEED it as they commit, and one seam
(`finalize::finalize_export`) APPLIES the tail — schema-fingerprint pin, the
`on_schema_drift` gate, the Form-B checksum harvest, the shape-drift warn. A
later fix added `finalize_export_records`, so a run whose RUNNER FAILED still
records what it observed: a Failed manifest must describe its durable debris
with the OBSERVED fingerprint, never the stale open-time baseline.

An interaction bughunt found that half of this does not work. Three parallel
runners — `keyset.rs`, `chunked/exec.rs`, `chunked/parallel_checkpoint.rs` —
record durable parts ABOVE their error bail but feed the ledger BELOW it, so on
the failure path `finalize_export_records` receives an EMPTY ledger. Keyset is
the worst case: it has no direct `summary.schema_fingerprint` assignment at all,
so the ledger is its only path and a failed parallel-keyset run records an
actively wrong fingerprint.

The obvious fix — move the feed above the bail — is WRONG, and the reason is the
whole point of this ADR. `keyset.rs` says it out loud at its part-publishing
site:

> Cursor (`rmax`) and checksums stay commit-gated below — those feed the SUMMARY
> and must reflect only committed data — but the durability count must reflect
> what is physically on disk.

So parts and checksums have DIFFERENT truth conditions BY DESIGN. Parts are
published per PAGE (a durability count must show what is on disk, or the retry
guard reads zero over durable parquet — the #200-1 fix). Checksums are published
per COMMITTED RANGE. On a failure, `parts_mx` therefore holds pages of a range
that never committed while `checksums_mx` does not. Harvesting that pair yields
a Form-B record covering a strict SUBSET of the manifest's parts — precisely the
partial-and-lying integrity record `harvest_column_checksums` already suppresses
via `column_checksums_incomplete`, and `validate --depth full` would then report
a FALSE mismatch on correctly-written data. Worse than the bug it fixes.

The design flaw is upstream of both: **the ledger holds facts with different
truth conditions in one structure with one lifecycle, fed at one moment.**

| fact | true when | fed today |
|---|---|---|
| durable parts | the bytes are on disk | above the bail |
| Form-B checksums | the unit COMMITTED | below the bail |
| schema fingerprint | the first batch was SEEN | below the bail |
| shape bytes | a batch was seen | below the bail |

The schema fingerprint is the accident. It has no coverage requirement at all —
it describes what the runner READ — yet it rides in the same feed as the
checksums and is lost with them. That is the reported defect.

## Decision

**1. Split the ledger by truth condition.**

*Observations* (`drift_schema`, `column_max_bytes`) describe what the runner
SAW. They carry no coverage obligation, they are monotonic, and recording one
early can never be wrong. They are fed EAGERLY — at first sight, above any bail
— and `finalize_export_records` applies them on the failure path unconditionally.

*Integrity* (Form-B checksums + key column) is only meaningful as a set that
covers EXACTLY the parts the manifest lists. It stays commit-gated.

**2. Compute coverage; do not trust the feed order.**

Today the parts/checksums correspondence is maintained by CONVENTION (each
runner remembers to feed only committed units) plus a manually-set
`column_checksums_incomplete` flag for the checkpoint-resume case. Both are
vigilance, and vigilance is what this codebase keeps paying for.

Instead: key each checksum contribution by the SAME unit identifier the parts
already carry — `PartKind` is already `File { part_index }` / `Chunk {
chunk_index }` / `Page { page_index, .. }`. At harvest, the seam compares the
unit set of the contributions against the unit set of the recorded parts:

- every recorded part's unit has a contribution → COMPLETE, record Form B;
- any part without one → INCOMPLETE, set `column_checksums_incomplete` and
  suppress, exactly as the resume path does today.

A runner then CANNOT publish a lying Form-B, because the seam sees the shortfall
itself rather than being told about it.

**3. This subsumes the resume case.** A checkpoint resume rehydrates pre-crash
parts whose per-column checksums are unrecoverable; today it must remember to set
`column_checksums_incomplete`. Under the computed rule those parts simply have no
contribution, so incompleteness follows from the data. One mechanism, not two.

## Consequences

* The reported defect is fixed at its root: a failed run records the fingerprint
  it observed, because observations never depended on commit in the first place.
* Form-B cannot become partial-and-lying by a feed-order mistake — the failure
  mode that made the naive fix unshippable.
* The manual `column_checksums_incomplete` flag becomes a computed result. Its
  call sites (`resume_m8.rs`, the rehydrate paths) lose a thing to remember.
* `record_part` must make the unit id available to the ledger, and each runner's
  checksum feed must carry the unit it belongs to — the one real cost. It is
  mechanical: every feed site already sits next to the code that knows the unit.
* The telltale invariants in `check_post_run_invariants` get STRONGER rather than
  replaced: "Form B is absent" stops being ambiguous between "nothing computed
  it" and "coverage was short", because the second is now a recorded verdict.
* Risk: a unit-id mismatch between the part and its checksum contribution would
  read as incompleteness and silently suppress Form B on a HEALTHY run. That is
  the fail-safe direction (absent, not lying), but it must be caught — the
  implementation needs a test asserting a complete run really does record Form B,
  not only that a short one suppresses it. An absence-only assertion here would
  repeat the redaction-test defect this repo has already paid for twice.

## Alternatives rejected

**Move the checksum feed above the bail.** Publishes a checksum over a subset of
the manifest's parts; `validate --depth full` then reports a false mismatch on
correct data. Rejected on the evidence above.

**Have each runner set `column_checksums_incomplete` on its failure path.** This
is the existing mechanism extended, i.e. more vigilance in exactly the places
that already forgot once. It also cannot express partial coverage — only "all or
nothing" — so a run that committed 9 of 10 ranges would either lie or discard
nine ranges' worth of verification.

**Feed the ledger from inside `record_part`.** ADR-0028 considered this and it
remains attractive for the OBSERVATION half. It does not work for checksums:
they arrive per range/page at a different moment than the parts, so hanging them
on `record_part` would force the same premature publication the first alternative
was rejected for.
