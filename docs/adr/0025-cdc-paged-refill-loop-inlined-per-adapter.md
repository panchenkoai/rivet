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

- A `ChangeStream` default method is wrong: MySQL implements `ChangeStream` but
  blocks instead of paging, so a shared default `next_change` would be incorrect
  for one of the three adapters (2-of-3, not 3-of-3).
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
