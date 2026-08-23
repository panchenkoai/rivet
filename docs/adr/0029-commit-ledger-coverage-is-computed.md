# ADR-0029: The commit ledger separates observations from integrity, and computes coverage rather than trusting it

**Status**: Accepted — implemented 2026-08-23. `Observations` / `Integrity` split,
`UnitId`-keyed contributions, coverage computed over `summary.manifest_parts`, both
manual `column_checksums_incomplete` sites retired, `check_post_run_invariants`
strengthened. See *Implementation notes* below for the four places the design
above needed correcting.
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

---

## Implementation notes (2026-08-23) — where the design above was wrong

Recorded because each of these is a thing the next reader would otherwise
re-derive, and two of them were only found by running the code.

**1. `PartKind` is the JOURNAL id, not the commit unit — a separate `UnitId` was
needed.** The decision says "key each checksum contribution by the SAME unit
identifier the parts already carry — `PartKind` is already `File { part_index }` /
`Chunk { chunk_index }` / `Page { page_index, .. }`". That is false for exactly the
runner this ADR exists for. Parallel keyset passes `PartKind::Page { page_index:
<flat drain index> }`, which is neither the page nor the range; its commit unit is
the RANGE (parts publish per page, checksums per committed range). And `single`
has no per-part unit at all — ONE sink accumulates the checksums of every part it
writes, so its unit is the whole invocation. So `commit::UnitId { Run, Chunk, Page,
Range }` is a separate argument to `record_part` beside `kind`, stated at every
call site. Re-using `PartKind` would have reported every healthy parallel-keyset
range short, i.e. shipped the ADR's own named risk on day one.

**2. Coverage is compared over `summary.manifest_parts`, keyed on `part_id`.** The
decision says "the unit set of the recorded parts", which reads as "the parts
`record_part` saw". Keying it that way makes the resume case — the one point 3 of
the decision claims to SUBSUME — invisible, because `rehydrate_manifest_parts_from_
file_log` and the M8 `Skip` clone push straight into `manifest_parts` and never call
`record_part`. The manifest is the right subject: it is the list the Form-B record
claims to cover, and it holds the hydrated parts too. `part_id` is the key (4 bytes,
unique by M4, stable across a dedup) rather than the path, so this costs no second
copy of every part name; `record_committed_part_with_fingerprint` now returns
`RecordedPart { deduped, part_id }` instead of a bare bool.

**3. The empty-accumulator path had to be reached too — this shipped a live
regression.** The pre-existing `harvest_column_checksums` early-returns when the
accumulator is empty. Leaving that return above the new coverage computation broke
four live tests (`parallel_keyset_crash_recovery_{postgres,mysql}`, and two
siblings): a crash resume whose ranges were ALL already `done` re-runs nothing, so
there is no checksum to weigh, the computation was skipped, and the run reached
`finalize_manifest` with hydrated parts and no suppression flag — where the
pre-existing Form-B telltale panicked it. Pre-ADR-0029 that flag came from the
manual `resume_m8.rs` assignment this ADR retires. So: compute coverage FIRST, and
early-return on an empty accumulator only once coverage is COMPLETE (that case is a
format fact — a CSV/JSONL sink computes no value checksums — and must leave the flag
clear so the telltale still catches a runner that harvested nothing).

**4. The strengthened invariant needed a second recorded field, not just the
existing flag.** "Form B is absent stops being ambiguous … because the second is now
a recorded verdict" is right, but `column_checksums_incomplete` alone cannot carry
it: a legitimate resume and a runner's unit-id mismatch both set it.
`RunSummary::column_checksums_short_cover` records the half that is never legitimate
on a successful run — a part this run RECORDED whose unit contributed nothing — and
`check_post_run_invariants` fails on it. That is resume-safe by construction with no
exemption to remember, because a hydrated part is *foreign*, not *uncovered*.

**Scope correction on the observation half.** The context says three parallel
runners lose their observations on the failure path. Only ONE of them actually did:
parallel keyset, which has no direct `summary.schema_fingerprint` assignment. Both
chunked parallel runners already set `summary.schema_fingerprint` from their
`shared_fingerprint` ABOVE the bail, and the chunked family deliberately feeds NO
drift schema at all (its gate is pre-chunk, ADR-0021) — do not "fix" that. The
second real loser was not in the list: `single`, whose `drain_tail_into` sat after
a write loop whose per-part `write_part_file(…)?` can escape, so a run that failed
mid-write pinned the stale baseline onto a Failed manifest listing parts written
under the observed schema. It now drains its observations the moment the read
completes.
