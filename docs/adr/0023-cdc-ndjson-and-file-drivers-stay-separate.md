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
  flush → checkpoint → **ack** (`PartCommitter`), then writes a `RunManifest`.

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
  checkpoint-on-commit. File: buffer + byte/row rollover policy + `PartCommitter`
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
