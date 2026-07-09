# Export modes — which one do I pick?

Every export declares a `mode:`. Start from the decision shortcut, then open the
guide for the mode you land on.

## Decision shortcut

- **Small-to-medium table, want a fresh snapshot every run** → [`full`](full.md)
- **Append-only or has an `updated_at`, want only new/changed rows** → [`incremental`](incremental.md)
- **Millions+ of rows, want speed and crash-resume** → [`chunked`](chunked.md)
- **Only the last N days matter (event/log table)** → [`time_window`](time-window.md)
- **Continuous low-latency replication from the WAL / binlog / oplog** → [`cdc`](../reference/cdc.md)

When in doubt, `rivet init` inspects the table and picks a sensible default for
you (small → `full`, large with an integer key → `chunked`).

## At a glance

| Mode | Use when | Keeps state? | Parallel? | First run |
|------|----------|--------------|-----------|-----------|
| [**full**](full.md) | complete snapshot each run | no (stateless) | no | exports everything |
| [**incremental**](incremental.md) | only rows past the saved cursor | yes (cursor) | no | exports everything, then deltas |
| [**chunked**](chunked.md) | tables too large for one `full` scan | yes (checkpoint, `--resume`) | yes | full, split into ranges |
| [**time_window**](time-window.md) | rolling N-day window | no (recomputed each run) | no | the window only |
| [**cdc**](../reference/cdc.md) | continuous log-based change capture | yes (resume checkpoint) | yes (multiplexed streams) | optional initial snapshot, then stream |

Notes worth knowing before you run:

- **`incremental` first run = full export.** With no cursor yet, every matching
  row is exported, so the first run behaves like `full` — size `batch_size`
  accordingly ([incremental.md](incremental.md#first-run)).
- **`chunked` clean re-runs are NOT idempotent.** A crash + `--resume` is
  at-least-once: a re-run chunk can be written twice (byte-identical), so
  de-duplicate downstream if you re-run ([chunked.md](chunked.md#clean-re-runs-are-not-idempotent)).
- **Composite cursors** are an `incremental` variant, not a separate mode — see
  [incremental-coalesce.md](incremental-coalesce.md) when one timestamp column
  isn't enough.
- **Source engine matters.** The SQL sources (PostgreSQL, MySQL, SQL Server)
  support every mode. **MongoDB**, a document store, supports only `full` (with
  keyset / parallel / resume paging on `_id`) and `cdc` — see
  [../reference/mongodb.md](../reference/mongodb.md).

Full configuration reference: [../reference/](../reference/).
