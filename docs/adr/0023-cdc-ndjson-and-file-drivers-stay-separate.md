# ADR-0023: The CDC NDJSON and file drivers stay separate (no `ChangeSink` trait)

**Status**: Accepted
**Date**: 2026-06-23

---

## Context

CDC has two drivers over the `ChangeStream` seam:

- `source::cdc::run()` — the NDJSON driver for `rivet cdc` without `--output`:
  pulls changes, filters by `--table`, prints one JSON object per change to
  stdout, saves the resume checkpoint on a commit boundary.
- `source::cdc::sink::run_to_files()` — the typed-file driver for `rivet cdc
  --output` and every `mode: cdc` run: buffers, rolls a part at a
  commit-boundary + threshold, and runs the durable sequence
  flush → checkpoint → **ack** (`roll_all`, with per-table `TableSink` state,
  in `src/source/cdc/sink.rs`), then writes a `RunManifest`.

Each architecture pass over CDC flags these two as a duplication and proposes a
single `drive(stream, sink)` loop with a `ChangeSink` trait (an `NdjsonSink` and a
`FileSink` adapter). One pass even reported it as a bug — "the NDJSON driver
forgot to `ack`."

## Decision

Keep the two drivers separate. Do **not** introduce a `ChangeSink` trait to merge
their loops.

The shared *assembly* — open the stream (permission/TLS gate), resolve the typed
schema, build the `SinkConfig` — is already deduped behind one seam,
`cdc::run_capture` (the `CdcCapture` assembler), which both the CLI `--output`
path and the `mode: cdc` run call. Only the NDJSON driver remains its own loop.

## Consequences / reasoning

- **The "missing ack" is not a bug.** `ack` advances a consume-on-read source
  (a PostgreSQL logical slot). The durability rule (ADR-0017 family) is: advance
  only *after* a durable write. NDJSON goes to **stdout**, which is not a durable
  sink — the downstream consumer owns durability — so advancing the slot would be
  premature (at-most-once). The NDJSON driver correctly does **not** `ack`; it
  saves the checkpoint file (MySQL resume) and lets PostgreSQL re-read from the
  slot. So the durability logic is *correctly file-only*, not duplicated.

- **The two loop bodies share almost nothing.** NDJSON: `to_json` + `println` +
  checkpoint-on-commit. File: buffer + byte/row rollover policy + `roll_all`
  (flush → checkpoint → ack) + manifest. The only common code is the ~5-line outer
  skeleton (`while next_change { table-filter; <body>; max_events }`).

- **That skeleton can't be cleanly extracted as an iterator** — the file driver
  calls `stream.ack()` *inside* the loop, so an iterator that owned the stream
  would conflict (borrow) with the ack. The only way to share the loop is the
  heavy `ChangeSink` trait, to dedupe ~5 lines.

- A `ChangeSink` seam whose two adapters share only a 5-line loop is **shallow**
  (the interface is as complex as the shared implementation; near-zero leverage).
  The deletion test agrees: delete the trait and ~5 trivial lines reappear in two
  places — complexity does not concentrate.

**Re-open if** a *third* output sink appears (e.g. a streaming/Kafka or a
distinct CSV-stream sink) that genuinely shares the commit-boundary + checkpoint
machinery — two adapters made `run_capture` a real seam; three sinks sharing
durability would make the loop one too. Until then, the duplication is 5 lines and
the seam would be shallow.

---

## Amendment 2026-08-27: the file driver's transaction buffer has no relief valve

`run_to_files()` rolls a part at a commit boundary plus a threshold. The
commit-boundary half is load-bearing and stays: `should_roll` requires
`committed` (`src/source/cdc/sink.rs:106-109`), which is what keeps a part from
splitting a source transaction and what makes the flush → checkpoint → ack
sequence atomic per transaction.

The consequence is that the thresholds — `rollover`, `rollover_memory_bytes` —
can only take effect AT a commit boundary. A single transaction larger than
memory has nothing to relieve it: it is buffered whole, in RAM, and there is no
spill path in the sink. A bulk `UPDATE` on a large table is one transaction, so
the failure mode is an OOM kill mid-capture. It is recoverable (the slot was
never acked) and it is also unrecoverable in practice: every retry reproduces
it, so the export cannot progress, and to the operator it is indistinguishable
from a crash.

**Decision (proposed).** Buffer beyond a threshold to disk, keyed by
transaction. The atomicity invariant is untouched — the part still closes only
at the commit boundary; only the BYTES in between are allowed to leave RAM. The
threshold is a tuning knob with a protective default, merged with `is_some()`
like its siblings, so a bare profile cannot clobber it to "unbounded" (the
config-clobber rule in CLAUDE.md).

**Primary prior art.** PostgreSQL does exactly this on the server side:
`logical_decoding_work_mem` bounds the reorder buffer and spills a transaction
past it to disk, precisely because a transaction's size is not something the
consumer gets to choose. PostgreSQL 14 offers a second, larger answer — the
decoder can stream an in-progress transaction to the client before its commit —
which is worth evaluating against a spill rather than assuming; the spill keeps
the commit-boundary invariant unchanged, streaming does not.

**RED-proof before Accepted.** A transaction an order of magnitude past the
memory bound, captured under a hard RSS ceiling: complete and atomic (all rows
in one part, none split across a checkpoint), peak RSS flat. The mutant is the
spill threshold set to infinity — the test must OOM or go RED. Per the fixture
rule in CLAUDE.md the transaction must exceed the bound by enough to force more
than one spill, or the spill's own accumulation arithmetic is untested by
construction.

---

## Amendment 2026-08-27: a resume must prove the checkpoint belongs to THIS server

Both drivers save a resume checkpoint on a commit boundary, and the MySQL
checkpoint is `{"file": …, "pos": …}` and nothing else — the comparable key is
the binlog ordinal plus offset (`src/source/cdc/validate.rs:39-52`). No server
identity, no GTID set.

A binlog coordinate is meaningless on a different server and plausible on all of
them. Restore a replica, fail over, point a config at a clone, or copy a
checkpoint between environments, and `binlog.000042 / 1096` names a real
position on the new server that has nothing to do with the captured one. rivet
resumes from it and reports success.

This is the only member of the 2026-08-27 CDC amendment set with no partial
mitigation anywhere. The other engines are covered by construction: a
PostgreSQL slot is server-side and cannot be carried to another server; SQL
Server floors at `fn_cdc_get_min_lsn` (over-reads, never skips); MongoDB's
resume token is rejected by a server that does not recognise it. MySQL alone
accepts a foreign coordinate silently — the same engine that CLAUDE.md already
singles out as having no server-side anchor at all.

**Decision (proposed).** The checkpoint carries the source's lineage identity,
and resume refuses when it does not match. On MySQL that is the server UUID plus
the executed GTID set at checkpoint time, with resume permitted only when the
checkpoint's GTID set is contained in the server's current history. The refusal
is loud and names the escape (a fresh full capture); it is never a silent
re-anchor. Every engine's checkpoint states which lineage identity it carries,
including the ones whose answer is "the server enforces it" — so an omission is
a recorded decision rather than a gap nobody asked about.

**Primary prior art.** MySQL supplies both primitives directly:
`@@GLOBAL.server_uuid` identifies the server, and the built-in
`GTID_SUBSET(subset, set)` answers exactly the containment question against
`@@GLOBAL.gtid_executed` / `@@GLOBAL.gtid_purged`. No third-party technique is
involved; this is the vendor's own answer to "is this position from my history".

**RED-proof before Accepted.** Capture against one server, then resume that
checkpoint against a second server seeded differently: the run must FAIL with
the lineage error, not capture. The mutant is the containment check removed. It
must be a real two-server fixture — the stand already runs
`rivet-mysql-primary-1` and `rivet-mysql-replica-1` — not a hand-edited
checkpoint field, or the test grades its own forgery instead of the product's
check (the fabricated-input class in CLAUDE.md). And per the exit-status rule,
assert the specific error, not merely a non-zero exit.
