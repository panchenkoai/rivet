# ADR-0004: Destination Write Contracts

**Status**: Accepted  
**Date**: 2026-04  
**Context**: Rivet writes exported data to four backends — local filesystem, S3, GCS, and stdout. Their failure modes, commit boundaries, and write guarantees differ. The planning and recovery layers must be able to reason about these differences without inspecting backend internals.

---

## Problem

State and manifest writes (ADR-0001 invariants I2–I4) must happen only after the destination write is durably committed. But "committed" means different things for different backends:

- Local `fs::copy` commits atomically from the caller's perspective, but may leave a partial file on failure.
- S3 and GCS object writes are not committed until the writer handle is closed (`dst.close()`); a mid-upload failure leaves nothing at the destination.
- stdout streams data immediately with no atomic commit point; a retry produces duplicate or corrupt output.

Without an explicit contract, the pipeline has no safe way to determine: when is it safe to advance the cursor? when is it safe to record a manifest entry? is a failed write safe to retry automatically?

---

## Decision

Introduce two types in `src/destination/mod.rs`:

- **`WriteCommitProtocol`** — when a write becomes durably committed and visible to readers.
- **`DestinationCapabilities`** — the full set of operational guarantees for a backend.

Add a `capabilities()` method to the `Destination` trait so each backend declares its own contract. The pipeline can inspect capabilities without downcasting.

---

## Per-Backend Capability Table

| Backend | `commit_protocol` | `idempotent_overwrite` | `retry_safe` | `partial_write_risk` |
|---|---|---|---|---|
| `LocalDestination` | `Atomic` | `true` | `false` | `true` |
| `S3Destination` | `FinalizeOnClose` | `true` | `true` | `false` |
| `GcsDestination` | `FinalizeOnClose` | `true` | `true` | `false` |
| `StdoutDestination` | `Streaming` | `false` | `false` | `true` |

### `WriteCommitProtocol` semantics

- **`Atomic`**: `write()` returning `Ok(())` means the full file is present at the destination. A failure may leave a partial artifact (`partial_write_risk = true`); the caller must clean it up before retrying.
- **`FinalizeOnClose`**: The object is committed only when the internal writer handle is closed. A mid-upload failure leaves nothing at the destination — the object is never partially visible to readers. `retry_safe = true` because a failed upload can be retried from scratch with no cleanup needed.
- **`Streaming`**: Data is written to an unbuffered output with no atomic commit boundary. Partial output may be observable before `write()` returns. Retrying after failure produces duplicate or corrupt output. There is no safe commit moment.

---

## Alignment with ADR-0001 Invariants I2–I4

ADR-0001 requires that state writes (manifest, cursor, schema) happen only after the destination write succeeds. This ADR makes the commit boundary explicit:

- **I2 (Write Before Manifest)**: `record_file` is called after `dest.write()` returns `Ok(())`. For `Atomic` and `FinalizeOnClose` backends, this is the commit boundary.
- **I3 (Write Before Cursor)**: `st.update()` is called after the file-writing loop. For `Atomic` and `FinalizeOnClose` backends, all files are committed before the cursor advances.
- **I4 (Metric After Verdict)**: Unchanged — metrics are recorded at the terminal state of the run.

The comment added to `pipeline/single.rs:run_single_export` immediately before the `record_file` call makes this ordering explicit in the source:

```rust
// ADR-0001 I2–I4 / ADR-0004: state writes happen only after destination.write()
// returns Ok(()), which for all current backends is the commit boundary.
```

---

## Runtime Capability Inspection

`pipeline/single.rs:run_single_export` inspects `dest.capabilities()` at runtime and logs the commit protocol for every run:

```
export 'orders': destination commit_protocol=Atomic idempotent=true retry_safe=false partial_risk=true
```

When a destination that is not retry-safe (`retry_safe = false`) is used on a run that performed one or more retries, a `WARN` is emitted:

```
export 'orders': destination is not retry-safe (2 retries used); partial artifacts may exist at destination — manual cleanup may be needed
```

This surfaces retry-safety mismatches (e.g. local filesystem + automatic retries) without blocking the run.

---

## Known Gap: stdout state writes

`StdoutDestination` has `commit_protocol: Streaming`. There is no safe moment to advance state after a streaming write — any output may have been partially consumed by the reader before `write()` returns.

**Current behavior**: The pipeline does not special-case stdout for state writes. If stdout is used as a destination, cursor and manifest writes proceed as normal after `write()` returns. This is safe only because stdout is used exclusively in development/piping scenarios where state persistence is not meaningful. The plan validation layer rejects `stdout + chunked` and `stdout + max_file_size` combinations via `Rejected` diagnostics before execution starts.

**If stdout is ever used in a production pipeline with cursor or manifest state, this gap must be addressed.** The fix is for the pipeline to inspect `capabilities().commit_protocol` and skip or warn on state writes when `Streaming`.

---

## Consequences

- Each backend's operational contract is now machine-readable and located with the implementation.
- The planning and recovery layers can inspect `capabilities()` without coupling to backend types.
- The stdout gap is documented rather than hidden; future callers are warned.
- No breaking changes — `capabilities()` is a new trait method with a defined contract.
