# ADR-0028: One finalize seam for the export tail — retiring the runner-bypass class by construction

**Status**: Proposed (design accepted, migration not started)
**Date**: 2026-08
**Relates to**: ADR-0012 (manifest durability ordering), ADR-0021 (chunked drift pre-chunk), `docs/runner-coverage-matrix.yaml` (the ledger this ADR aims to shrink)

---

## Context

`run_export_job` (`src/pipeline/job.rs:896`) dispatches on `plan.strategy` to
one of several execution loops — four runner families (`single`, `chunked` +
its checkpoint twin, `keyset`, `mongo_parallel`), ~eight real commit loops
counting sequential/parallel variants. Each loop owns its own **tail**: after
the last batch it must harvest per-column checksums, run the schema-drift
gate, capture the incremental cursor range, clear the resume anchor, and hand
back to the dispatcher for `finalize_manifest`.

That tail is **re-assembled by hand in every runner**. The primitives are
already shared (`commit::record_part`, `commit::harvest_column_checksums`,
`schema_drift::check_from_sink_schema`, `finalize::finalize_manifest`); the
**orchestration** is not. The result is the *runner-bypass class*: a
per-export feature wired into one runner is silently absent on the others,
and every count/sum/oracle stays green because the missing piece (a gate, an
integrity record, a run-unique stamp) is orthogonal to row correctness.

The class is recurrent, not incidental — it has surfaced across eight bughunt
rounds because it is a property of the shape: a dispatcher fanning out to N
re-implementations of one sequence gives **N places to forget**. Documented
bites, each with its comment still in the tree:

* `on_schema_drift: fail` silently returned exit 0 on keyset and
  mongo_parallel (`keyset.rs:1266` says so verbatim) — round 8 found two
  misses in one round.
* Form-B value-checksum harvest was absent on all three large-table runners:
  the sink *computed* the checksums for every runner, but only `single`
  harvested them into the summary.
* The keyset part-name stamp doubled the export name (#253) because keyset
  alone derived its stamp from `run_id` while its three siblings used a fresh
  Utc stamp — a divergence in a tail step no test compared across runners.

Current defenses, and their limits:

1. **`docs/runner-coverage-matrix.yaml`** (59 `na` / 39 `test` / 4 `gap` as
   of this writing) — a map, not a guard. It catches gaps after the fact and
   collapses ~8 loops into 4 columns, so `test` on `keyset` may be proven
   only on keyset_sequential.
2. **`RunSummary::check_post_run_invariants`** (`summary.rs:795`, called
   inside `finalize_manifest`) — a genuine structural backstop for the two
   telltale-bearing features (drift verdict, Form-B), but it asserts the
   summary is *coherent*, not that every tail step *ran*.
3. **Per-variant audit** (2026-08-04, recorded in the matrix header) — all 8
   loops × 13 scenarios read by hand, adversarially refuted. A snapshot, not
   a mechanism.

The load-bearing observation: `finalize_manifest` and the invariant check are
**already centralized in the dispatcher** (`job.rs:1221`, `job.rs:1493`). The
whole surface of the class is the stretch **between the last batch and
`finalize_manifest`** — the pre-finalize tail the dispatcher does not yet
own.

## Decision

Funnel the pre-finalize tail through **one seam**, `finalize_export`, called
by the dispatcher; and fill that seam's input **as a side effect of the one
call every runner already makes** (`commit::record_part`), so no runner can
bypass what it never touches.

Two contract shapes were designed (design-it-twice):

* **Option A — collect struct.** Each runner returns an
  `ExportTail { drift_schema, checksums+key, cursor, shape }`; the dispatcher
  passes it to `finalize_export`. This moves the *ordering* into one place
  but converts "forget to call" into "forget to fill" — a runner can still
  leave `drift_schema = None` and the gate quietly no-ops.
* **Option B — the sink/commit path owns the tail (CHOSEN).**
  `commit::record_part` is already the single drain point every runner —
  including parallel workers (`mongo_parallel.rs:19`) — must call to write.
  Extend it to accumulate, per part, the drift schema, the checksum XOR, the
  cursor high-water, and shape bytes into a `CommitLedger`. The tail input is
  then captured **by construction of committing**, exactly the mechanism that
  already makes `record_part` itself an `na` cell. `finalize_export` reads a
  ledger it did nothing special to fill.

The seam encodes, once, the ordering constraints each runner currently
re-derives by hand:

* drift gate + checksum harvest **before** `finalize_manifest` — the manifest
  must record the checksums, and a drift `fail` must abort before a manifest
  exists;
* cursor commit + *incremental* checkpoint clear **after** the manifest is
  durable (ADR-0012: a crash in the advance→manifest window strands committed
  parts the manifest-authoritative loader silently drops);
* *non-incremental* checkpoint clear at data-complete, **before** the gates —
  a gate failure must not leave a resume anchor that turns an intended full
  re-run into a crash-recovery skip (`keyset.rs:1228` hand-codes this today).

**What stays apart, deliberately**: the reads. A keyset seek, a chunked
`BETWEEN`, a single cursor scan and a Mongo `$sample` split are genuinely
different access patterns — folding them behind one interface would be a
shallow abstraction over real difference, the opposite mistake. Only the tail
funnels. ADR-0025 made the same call for CDC adapters' refill loops: shared
seams for the invariants, per-adapter loops where the engines truly differ.

## Migration (one runner at a time, each cut RED-proven)

1. Land `finalize_export` against `single` — the reference tail, lowest-risk
   cut; prove parity on its live suite.
2. Extend `record_part` to fill the `CommitLedger` (concurrency-safe — it is
   already the parallel drain point).
3. Cut over `keyset` → `mongo_parallel` → `chunked`(+checkpoint): delete each
   runner's tail block; the deleted block is the RED mutant.
4. Collapse the matrix rows (drift / checksum / cursor flip from N× `test` to
   one `na`) and strengthen `check_post_run_invariants` from "summary is
   coherent" to "the ledger was populated" — the backstop graduates to
   asserting the feature *ran*.

## Consequences

* A new per-export feature has exactly **one** home and is born `na`. The
  places-to-forget count drops from N to 1; the recurring bughunt find ends
  because there is no longer an *other runner* to forget.
* The deletion test passes in the right direction: removing the per-runner
  tail blocks **concentrates** complexity into one ordering-correct,
  tested-once seam rather than moving it around.
* The runner-coverage matrix shrinks toward what it should be: a ledger of
  genuinely per-runner concerns (part naming, read-side semantics), not a
  guard against re-assembly drift.
* Cost: `record_part`'s signature/state widens, and the migration touches
  every runner — hence the one-at-a-time, RED-proven schedule rather than a
  big-bang cut.
* Until the migration completes, the matrix + invariant backstop remain the
  defense; this ADR does not retire them, it names their replacement.

## Sibling class, recorded for the next reader

The same fan-out shape one layer up is the *diagnostic-bypass* class: a
preflight that resolves its subject from a **subset** of strategy fields (an
`.or()` chain shorter than the strategy enum, a `?` placeholder surfacing in
output). The cure rhymes: resolve through the one function that enumerates
every strategy. Wherever a fan-out re-implements a decision its dispatcher
could own, this class is waiting.
