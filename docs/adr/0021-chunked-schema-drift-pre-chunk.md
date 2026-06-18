# ADR-0021: Chunked Schema-Drift Detection Runs Pre-Chunk

**Status**: Accepted
**Date**: 2026-06-18

---

## Context

rivet detects column-level schema drift (added / removed / retyped columns)
against a per-export baseline in `export_schema`
(`state::detect_schema_change` + `store_schema`), driven by
`on_schema_drift: warn|continue|fail`. Until now this ran **only in single
(non-chunked) mode** (`pipeline::single`); the call site's own comment noted the
state store "is only populated by the drift-detect path below, and not at all in
chunked mode."

A pilot re-ran a 655k-row table 4× over two days in **chunked** mode and got no
column-level snapshot at all — `export_schema` stayed empty, `rivet state` showed
nothing, and drift could never be detected on later runs. Chunked is the default
for large tables, so the most-exposed exports were exactly the ones without drift
coverage.

Naively replicating the single-mode flow in chunked is wrong for two reasons:

1. **Timing / `fail` semantics.** Single detects drift from the sink's resolved
   schema and `fail` aborts *before* writing. In chunked, by the time a chunk
   sink resolves a schema the chunk is already written (and parallel modes run
   many chunks at once) — a post-write `fail` cannot prevent the corrupt-shaped
   output it exists to stop.
2. **Statelessness.** The non-checkpoint executors (`chunked::exec`) are
   deliberately stateless (no `StateStore`), so they cannot `store_schema` from
   inside the worker loop at all.

## Decision

Detect drift **once, pre-chunk** — in each chunked run function right after chunk
boundaries are computed and **before any chunk executes** — from a schema
resolved via `Source::type_mappings` (a metadata-only query; no data scan).
`type_mappings` → `build_arrow_field` → `arrow_schema_to_columns` yields the
*same* canonical `SchemaColumn` format single mode derives from the sink, so the
baselines are comparable.

`fail` then aborts before the first chunk writes — matching single's intent;
`warn` / `continue` store-or-update the baseline. The logic is a single shared
helper, `pipeline::schema_drift::check_and_persist`, used by both single
(post-write, sink schema) and chunked (pre-chunk, `type_mappings` schema).

## Consequences

- All chunked modes (sequential / parallel, checkpoint / non-checkpoint) gain the
  column-level drift parity single has; `export_schema` and `rivet state` are
  populated for chunked exports.
- One extra metadata round-trip per chunked Detect run (zero-row `type_mappings`)
  — negligible against the chunk scans, and itself source-friendly.
- **Cross-mode caveat.** A baseline stored by single (data-derived sink schema)
  and one stored by chunked (`type_mappings` schema) can differ for types rivet
  infers from data rather than the catalog (e.g. a decimal scale that is a
  placeholder in `type_mappings` until a value is observed). Within one mode this
  is consistent; switching an export between single and chunked may log a
  one-time drift that self-heals on the next run. Acceptable — documented here so
  a future contributor does not chase a phantom.
- Resume / `Precomputed` chunk sources skip the pre-check (drift was already
  evaluated on the original Detect run that planned the chunks).
