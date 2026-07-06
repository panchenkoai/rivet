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
