# Rivet vs a cursor-based ELT pipeline — like-for-like

Positioning for teams that already run a **cursor/watermark ELT pipeline**
(typically `odbc2parquet` + orchestration glue: extract by cursor → parquet →
`MERGE` into the warehouse → dedup → a metadata/reconciliation table).

## Honest scope

- **Compared here:** rivet's cursor-based extraction (`mode: full` /
  `incremental` / `chunked`) against the same paradigm — a cursor pipeline.
  Same job, same shape, head to head.
- **Deliberately NOT the baseline:** CDC. Log-based capture is the *evolution*
  step (§4), not the comparison — comparing rivet-CDC against a cursor pipeline
  is apples-to-oranges.
- **What rivet is:** an extraction *engine*. A full ELT pipeline also merges,
  dedups, and keeps metadata. Rivet lands typed parquet + a manifest and leaves
  the MERGE to the warehouse. So rivet slots **under** an existing merge/metadata
  layer, replacing the extract-and-glue tier — not the whole pipeline.
- **When NOT to adopt:** if the data is append-mostly, memory fits the box, and
  log-only observability is tolerable, a working pipeline should not be
  replaced. The honest boundary is in §5.

## 1. Like-for-like: extraction in the same (cursor) paradigm

| Dimension | Rivet (cursor: full / incremental / chunked) | odbc2parquet + orchestration glue | Felt or latent |
|---|---|---|---|
| **Peak memory** | Bounded by construction — a per-flush memory target (default ~32 MB) with streaming rollover; memory is O(batch), not O(table). Measured ~70–90 MB per table. | The `--batch-size-memory` flag is a hint, not a hard cap; ODBC driver + Arrow buffering + wide columns drive the real footprint, which is effectively unbounded per subprocess. | **Felt** |
| **Failure recovery** | Resumable chunk checkpoints — a crashed run resumes from the last committed chunk. | Typically a full re-load on failure. | **Felt** |
| **Observability** | Structured run journal + file manifest + metrics + schema-drift tracker in a state DB; queryable via `rivet state` / `rivet metrics`. | Unstructured log lines (`logger.info`); observability is grepping logs. | **Felt** |
| **Cursor state** | Persisted (`.rivet_state.db`) with drift detection; resume reads the last committed value. | Often recomputed from source `MIN/MAX` each run; the watermark can be an injected run timestamp. | Minor |
| **Type fidelity** | A per-engine type resolver hardened against the known lossy cases (unsigned 64-bit, decimals, timestamps, JSON, UUIDs). | ODBC type mapping; e.g. `bigint unsigned → INT64` silently overflows above ~9.2e18. | Latent¹ |
| **Value verification** | Always-on two-ended value checksum (independent source-side fold vs a fold of the built Arrow column) + `rivet validate` re-reads and re-verifies at the destination. | Reconciliation compares two destination datasets (staging vs raw) by row count — an internal count, not a source-vs-target value check. | Latent¹ |
| **Completeness** | Stops at typed parquet + manifest — the MERGE/dedup is the warehouse's job. | Full cycle: extract → MERGE → dedup → metadata. **More complete.** | — |

¹ *Latent* = real as code, but unexercised by an append-mostly, cursor-always-moves
workload with in-range values. A long clean run is genuine evidence the shape
does not trigger it — the value is insurance against shape changes, not a claim
that the current pipeline is broken.

**Verdict.** The three *felt* rows — bounded memory, resumable recovery,
observability — are solved in the **same cursor paradigm, with no CDC**. The
adoption case stands on the extraction engine alone. The honest cost: rivet is
not a full pipeline, so it goes *under* the existing merge/metadata layer.

## 2. The extraction contract — what rivet ships and guarantees

What a downstream consumer (or a DBA auditing a run) can rely on, per export,
without trusting rivet's internal state:

- **`_SUCCESS` marker** — the prefix is complete and safe to load. Absent ⇒ do
  not consume.
- **`manifest.json`** — `row_count`, and per part: relative path, row count,
  size, and a content fingerprint (xxh3) + content MD5. The manifest is
  self-consistent or `rivet validate` fails loudly.
- **Two-ended value checksums (Form A/B)** — recorded per column; `rivet
  validate` re-reads the parts and re-verifies, catching an Arrow→Parquet
  encode or post-write corruption a row count cannot see.
- **`source.extraction`** (incremental) — the strategy, cursor column, and the
  **cursor range this run covered** (`cursor_low..cursor_high`). Continuity is
  verifiable from manifests alone: run N+1's `cursor_low` must equal run N's
  `cursor_high`; a non-contiguous low is a silently-skipped range. No access to
  rivet's private state required.
- **Run journal + metrics** — a typed, queryable record of what was planned,
  what happened, what committed, and the outcome.
- **Schema-drift tracker** — column adds/removes/retypes surface on the next
  run under `on_schema_drift: warn | continue | fail`.

This is the "contract in the extraction part": every run leaves a portable,
verifiable, warehouse-consumable record — not just files.

## 3. DBA / SRE like-for-like

| Concern | Rivet (cursor) | odbc2parquet + glue |
|---|---|---|
| **Memory envelope** | Bounded per worker (~70–90 MB); N parallel workers = N × bounded ⇒ **plannable**. On a fixed box you know how many fit. | Per-subprocess footprint is effectively unbounded (flag is a hint); capacity must be discovered empirically. |
| **Concurrent starts** | Parallel workers are threads in one process (shared runtime, one destination instance, one connection pool); a synchronous start fits a known envelope. | A subprocess per chunk (fork/exec + driver + interpreter each); concurrent heavy loads can OOM the box, forcing staggered starts. |
| **Throttling** | Not needed — memory is bounded by construction, not throttled after the fact. | Reactive: watch memory and downshift parallel→sequential at a threshold (e.g. 70 %). |
| **Scheduling pressure** | Bounded profile ⇒ heavy loads need no weekend-only window. | Uncapped worker memory can force spreading heavy refreshes to off-peak/weekends. |
| **Retries** | Transient-error classifier + backoff, **plus** resumable chunk checkpoints — a failure continues, it does not restart. | Retries scoped to connection + storage transients; a failed `LOAD`/`MERGE` fails fast, and a re-load is typically full. |
| **Source hold model** | Chunked reads are short queries (PG: server cursor + `FETCH N`, longest single query sub-second on millions of rows; MySQL: PK-range chunks). No minutes-long open transaction. `throttle_ms`, `statement_timeout_s`, `lock_timeout_s`, `profile: safe` are first-class. Server-side cost is reproducibly measurable. | odbc2parquet issues the extraction query per chunk; hold time depends on chunk size and driver. |
| **Telemetry** | `rivet state` / `rivet metrics` — queryable per-run record. | Log lines only. |
| **Schema drift** | Static explicit column lists ⇒ adds ignored, a dropped column fails the query loudly (both tools). Rivet additionally tracks drift in the state DB and can gate on it. | Static SELECTs already make adds safe and drops loud; no persistent drift record. |
| **Hard deletes** | Not captured in cursor mode (a DELETE moves no cursor) — the CDC evolution (§4) captures them. | Not captured (same structural limit of watermark sync). |

**Reading it:** the operational wins a DBA/SRE feels weekly — bounded memory,
no reactive throttle, synchronous starts, resumable recovery, queryable
telemetry — are all in the cursor paradigm. Deletes are the one thing neither
cursor path captures; that is the evolution, not a like-for-like gap.

## 4. CDC as the evolution (not the comparison)

Once on rivet's cursor path — a better extraction engine in the same paradigm,
same orchestration, same downstream merge layer — CDC is a **mode flip**, not a
re-architecture:

```yaml
mode: cdc          # was: incremental
cdc: { initial: snapshot, ... }
```

It removes the cursor's two *structural* blind spots that no tuning or retry can
fix:

- **Hard deletes** — a DELETE moves no cursor; a watermark sync never sees it.
- **Out-of-cursor updates** — an update that does not move the cursor column
  (e.g. a status change with no `updated_at` bump) is never re-extracted.

Same tool, same destination contract (`__op` / `__pos` typed change events +
manifest + `_SUCCESS`), same downstream merge. Adopt cursor-first for the
operational wins; grow into CDC when deletes or out-of-cursor updates start to
matter.

## 5. When NOT to adopt (the honest boundary)

- Data is append-mostly (no hard deletes), the cursor always moves (no
  out-of-cursor updates), and 64-bit values stay in range → the latent rows
  in §1 never fire; a long clean run is real evidence of this.
- The worker's memory already fits the box without weekend staggering → the
  strongest felt win does not apply.
- Log-only observability is tolerable for the team's incident load.

If all three hold, a working pipeline should not be replaced. The credible
pitch is naming this boundary, not claiming the incumbent is broken.
