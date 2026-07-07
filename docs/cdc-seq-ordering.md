# CDC change ordering: `__pos` is not a total order — add `__seq`

## Problem (verified live on all three engines)

The CDC output columns are `__op`, `__pos`, and the after-image. `__pos` is the
**commit position** of the change's transaction:

| engine  | `__pos`                                   | source                     |
| ------- | ----------------------------------------- | -------------------------- |
| MySQL   | `{"file":"binlog.000047","pos":11721549}` | transaction commit pos     |
| Postgres| `{"lsn":"3D/485795A0"}`                    | `peek_changes` commit LSN  |
| SQLServer | `{"lsn":"0000002d000000d80194"}`        | `__$start_lsn` (txn LSN)   |

Commit position is exactly right for **resume/checkpoint** (all engines resume
at a commit boundary). But it is **not a total order over changes**: every
change in one transaction shares it. Proven — 8000 `UPDATE`s of one PK in a
single transaction produced `COUNT(DISTINCT __pos) = 1` on **all three engines**.

Downstream, the current-state dedup view

```sql
ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <parsed __pos> DESC) = 1
```

has 8000 tied rows, so `ROW_NUMBER` picks an arbitrary one. Live result: the
view returned `counter = 1` for a row whose committed value was `8000` —
**silently wrong current state**. This bites any transaction that touches the
same PK more than once (triggers, ORMs, read-modify-write loops, batch
upserts). The row order inside the Parquet part *is* the change order, but that
order is lost the moment the log is loaded into an (unordered) warehouse table.

## Design: emit a per-change `__seq` (intra-transaction ordinal)

Add a **`__seq`** column to the CDC output: the change's ordinal **within its
commit group**. Keep `__pos` unchanged (still the commit position, still what
resume uses). The pair `(__pos, __seq)` is then a **total order** that:

- matches log order (commit order across transactions, emission order within),
- is **deterministic and log-derived**, so a re-emitted change (at-least-once,
  crash-before-ack) carries the **same** `(__pos, __seq)` — the dedup tiebreak
  is a true tie between identical rows, so either wins and the value is right,
- survives the load (it is a column, not row order).

Dedup view becomes:

```sql
ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <parsed __pos> DESC, __seq DESC) = 1
```

### Per-engine population

`__seq` is a 0-based counter over the changes of one commit, in log order:

- **SQL Server** — use the native **`__$seqval`** from the change table (it
  already orders operations within `__$start_lsn`). `__seq = dense_rank of
  __$seqval within __$start_lsn` (or `__$seqval` rendered as a comparable
  fixed-width value). No invention — the engine hands us the order.
- **PostgreSQL** — logical decoding yields the transaction's changes in order;
  assign `0,1,2,…`, resetting when `__pos` (commit LSN) advances.
- **MySQL** — binlog row events arrive in order within the transaction; assign
  `0,1,2,…`, resetting at each commit `__pos`.

Because the reset key is `__pos` (the commit position), the ordinal is
reproducible from the log alone on every run — the at-least-once property above
holds without any persisted counter.

### Why not alternatives

- **A single global run counter** (0,1,2,… over the whole run) breaks across
  runs: run 2 resets to 0, so a newer change gets a smaller counter than an
  older one from run 1. `(__pos, __seq)` avoids this by resetting per commit,
  keeping the ordinal log-derived.
- **Folding `__seq` into `__pos`** (making `__pos` distinct per change) would
  break resume, which must stop on a commit boundary, not mid-transaction.
- **Relying on Parquet row order** — lost on load into a warehouse table.

## Blast radius

- CDC sink schema gains one column (`__seq INT64`), all engines.
- Three capture paths populate it (SQL Server from `__$seqval`; MySQL/Postgres
  from a per-commit ordinal).
- `validate.rs` can additionally assert `(__pos, __seq)` is strictly increasing
  in part→row order (today it only checks `__pos` non-decreasing).
- The `rivet-pro` dedup view template orders by `(__pos, __seq)`.
- Regression test per engine: N changes to one PK in **one** transaction →
  the dedup view returns the **last** change's value, not an arbitrary one.
