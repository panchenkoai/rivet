# ADR-0025: The CDC paged refill loop stays inlined per adapter

**Status**: Accepted
**Date**: 2026-07-06

---

## Context

After the 0.16.7 bounded-peek fix, the two poll-model CDC adapters —
`source::postgres::cdc::PgChangeStream` and `source::mssql::cdc::MssqlChangeStream`
— carry a **byte-for-byte identical** `next_change()`:

```rust
while self.pending.is_empty() && !self.exhausted {
    if let Err(e) = self.fill() { return Some(Err(e)); }
}
self.pending.pop_front().map(Ok)
```

plus the same supporting state: `pending: VecDeque<ChangeEvent>`, `exhausted: bool`,
and a `batch_limit` clamped from the peek bound. Only `fill()` is genuinely
per-engine (PostgreSQL frames transactions + frontier-dedups a non-consuming
`peek`; SQL Server windows the change table by LSN and advances an internal
cursor). MySQL is the odd one out — it *blocks* on the binlog rather than paging,
so it shares none of this skeleton.

An architecture pass flags this as an un-extracted "polled paged stream" seam and
proposes a shared driver — e.g. a `PolledPagedStream { fill(&mut self) }` the two
adapters delegate to, or a `ChangeStream` default method.

## Decision

**Keep the loop inlined in each adapter. Do not extract a shared paged-stream
driver.**

## Consequences

- A `ChangeStream` default method is wrong: MySQL (blocking binlog) and MongoDB
  (tailable change stream) implement `ChangeStream` but do not page, so a shared
  default `next_change` would be incorrect for two of the four adapters
  (2-of-4, not 4-of-4). PG and MSSQL remain the only poll-paged pair.
- A free-function / wrapper extraction fights the borrow checker. The loop must
  hold `&mut self.pending` **and** call `self.fill()` (also `&mut self`) — a
  borrow conflict. The only way through is an accessor trait
  (`fn pending(&mut self) -> &mut VecDeque; fn exhausted(&self) -> bool; fn fill(&mut self)`)
  with a blanket `next_change` — which is **more boilerplate than the five lines
  and three fields it removes**, and pushes three trivial accessors into the
  interface of both adapters.
- Deletion test: extracting the loop concentrates one identical five-liner. The
  win is small (locality, not leverage) and the abstraction's cost exceeds it.
- The one thing worth encoding — that the PostgreSQL peek must be **≥ the part
  rollover** or it starves — is captured instead by `PeekBound` (the sink builds
  `PeekBound::Sized(rollover)`, NDJSON is `PeekBound::Unbounded`), so a peek that
  undershoots the rollover is unrepresentable. That is the real correctness seam;
  the refill loop's duplication is not.

If a fourth poll adapter appears, or the two `fill()` bodies converge, reopen this.

---

## Amendment (2026-07-17)

The consequence bullet above — "a peek that undershoots the rollover is
unrepresentable" — was falsified by the open-bound work:
`pg_logical_slot_peek_changes`' `upto_nchanges` counts the BEGIN/COMMIT marker
rows too, so `PeekBound::Sized(rollover)` yielded fewer DATA rows than the
sink's ack boundary per peek, the refill re-read the same window, and a bounded
run exhausted with the backlog only partially drained (RED:
`roast_pg_until_current_open_bound_two_runs_lose_nothing` — two runs captured
4 of ~600 ids at rollover 5).

`PeekBound` stays the correctness seam, carrying the sink's ACK CADENCE (the
rollover) — one ack's worth of WAL per peek.

## Amendment (2026-07-19)

An ultracode review found the 2026-07-17 ×3 peek escalation only *partly*
closed the gap: it covered the captured-marker ratio (a single-row transaction
is 3 wire rows for 1 change) but NOT an uncaptured-table transaction or an
empty/DDL span, whose wire:capture ratio is unbounded — a span larger than the
escalated window still starved the slot and the run still exhausted before the
open bound (RED: `roast_pg_cdc_reaches_open_bound_past_a_large_uncaptured_
transaction` — a 200-row uncaptured transaction ahead of the captured backlog
made a run capture zero in-bound rows at rollover 5).

The real seam is the **sink re-drain loop** ([`sink::run_to_files`]), not the
peek budget: after each drain pass it flushes + acks the consumed span
(advancing a consume-on-read slot past uncaptured/empty WAL, whose commit
boundary is recorded before the routing filter), then re-peeks the fresh WAL
beyond it, until a pass yields nothing. So the ×3 escalation is REMOVED — the
peek is a flat 1× rollover (drain RSS back to O(rollover)) and the adapter's
`ack`/`release_empty_frontier` clear `exhausted` so the next pass slides
forward. The decision this ADR records — no shared refill driver, the loop
inlined per adapter — still stands; the re-drain loop lives in the shared sink,
above the adapters, and non-PG engines (whose read cursor advances on its own)
fall straight through it.

## Amendment 2026-08-27: the bounded drain's boundary is approximate, and PostgreSQL can make it exact

`PeekBound` encodes the one thing this ADR found worth encoding — a peek that
undershoots the rollover starves. The bound that ends a `until_current` run is a
separate quantity and it is weaker than it reads.

On PostgreSQL the open-time snapshot is `pg_current_wal_lsn()`: the WAL head of
the whole database, which is not a position in this slot's decoded stream. So
the boundary is approximate on both sides. It can sit past the last commit this
slot will ever decode — the run then waits for traffic that never routes to it,
which is the starvation class the sink's re-drain loop had to be built for — and
it can be reached by WAL this slot never sees. Every fix so far made the drain
more persistent; none made the boundary exact.

**Decision (proposed).** Where the engine can write an ordered marker INTO the
log the reader is already decoding, end the stream on that marker rather than on
a head snapshot: write a run-unique nonce at open, stop when the nonce is
decoded. The boundary then comes from the same ordering as the data instead of
from a different counter. Where an engine has no such primitive, the open-time
snapshot stays.

The per-engine honesty rule this ADR's neighbours already carry applies without
softening: each engine's bound is probed by DISABLING it and observing whether
termination actually depends on it, and one engine's result is never generalized
to another. That mistake has been made twice on this exact question.

**Primary prior art.** `pg_logical_emit_message(transactional, prefix, content)`
is a documented PostgreSQL function (9.6+) whose stated purpose is to place an
application-defined record into the WAL for logical-decoding consumers; a
non-transactional message is decoded in WAL order, which is the property the
bound needs. The general shape — write a marker, then use its position in the
log as the boundary — is the watermark technique from Netflix's DBLog paper.

**Sequencing.** This lands AFTER the `pgoutput` migration (ADR-0031), not
before: the marker is decoded by the same reader that migration replaces, and
building it twice is the avoidable cost.

**RED-proof before Accepted.** A paced writer whose traffic does not route to
the captured table, running throughout the bounded run: the run terminates at
its marker rather than chasing the head. The mutant is the marker check replaced
by the head snapshot — the termination test goes RED while the two-run union
test (`..._until_current_open_bound_two_runs_lose_nothing`) stays green, since
the old behaviour deferred rather than dropped. A test that cannot tell those
two apart is measuring persistence, not the bound.
