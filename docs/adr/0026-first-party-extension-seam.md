# ADR-0026: First-party extension seam (amends ADR-0002)

**Status**: Accepted
**Date**: 2026-07
**Amends**: [ADR-0002 — CLI Product vs Library](0002-cli-product-vs-library.md)
**Context**: A private, source-available companion crate — **`rivet-pro`** (BSL 1.1, separate repo) — now builds the paid tier (warehouse load, whole-database discovery, continuous CDC) on top of the OSS engine. It links the `rivet` library and depends on a small set of already-`pub` items. ADR-0002 declares the library "not a stable public API" and (Consequence #4 / *Future library path*) prescribes extracting a separate `rivet-engine` crate *if* a stable embedding surface is ever needed. This ADR decides what to do now that there is exactly **one, first-party** embedding consumer.

---

## Decision

**Name a minimal, stability-tracked *first-party extension seam*. Defer the full `rivet-engine` extraction until a non-first-party (external) consumer appears.**

The seam is the exact set of library items `rivet-pro` depends on. It stays `pub`, and a change to the **shape** of any item below is a deliberate act: update `rivet-pro` in lockstep and record it in `CHANGELOG.md` under a `Breaking (extension seam)` line.

### The seam (v0.16.x)

| Item | Path |
|---|---|
| `TypeMapping`, `RivetType`, `TimeUnit`, `TypeFidelity`, `SourceColumn` | `rivet::types` |
| `ExportTarget` (+ variants `DuckDb`/`BigQuery`/`Snowflake`/`ClickHouse`) | `rivet::types::target` |
| `ExportTarget::resolve_table(&[TypeMapping]) -> Vec<TargetColumnSpec>` | `rivet::types::target` |
| `ExportTarget::resolve_column(TargetInput) -> TargetColumnSpec` | `rivet::types::target` |
| `TargetColumnSpec`, `TargetInput`, `TargetStatus` | `rivet::types::target` |

Everything else in the library keeps the ADR-0002 posture: `pub` only for the test harness, no stability guarantee.

### The CLI superset uses the process boundary, not a Rust API

`rivet-pro` ships a superset `rivet` binary (OSS subcommands + `load`/`discover`/`daemon`). It composes them at the **argv/process boundary**: unknown subcommands are delegated to the OSS `rivet` binary (already a stable product contract — config YAML + exit-code taxonomy + manifest). The `cli` module is deliberately **binary-only** (declared in `main.rs`, absent from `lib.rs`; its dispatch reaches `crate::init`, also binary-only). Pulling `cli` into the library to expose `Commands`/`dispatch` would violate ADR-0002's minimal-library principle and freeze the entire command surface. We do not do that.

---

## Rationale

- **One consumer ≠ a public API.** A full `rivet-engine` extraction (separate crate, two manifests, re-export churn) is the right move for *external* consumers with independent release cadence. With a single first-party consumer we own both sides, so a *named + tested* subset delivers the stability guarantee at a fraction of the cost.
- **Smallest seam.** Every `pub` item on the seam is a compatibility commitment. The list is exactly what `rivet-pro` uses today — resolution only. `Destination` promotion (a likely next seam) is **deferred** until a Pro feature needs it, then added here with the same discipline.
- **One-way dependency.** `rivet-pro` depends on `rivet`; the OSS tree must never depend on `rivet-pro` — otherwise the MIT binary can't build without the private crate and paid code leaks into an MIT distribution.

---

## Enforcement

1. **Seam-stability canary** — `tests/offline/extension_seam.rs` compile-locks every signature above. If it fails to build, an item on the seam changed: update `rivet-pro` and add the `Breaking (extension seam)` CHANGELOG line — do not just edit the test to compile.
2. **Dependency-direction guard** — the CI `boundary` job fails if `src/` or `Cargo.toml` references `rivet-pro` / `rivet_pro`.

---

## Consequences

- The seam table is the contract. Widen it only by adding a row here + a canary line, never silently.
- `rivet-pro`'s own CI (dual-checkout, builds against OSS `main`) is a second, cross-repo canary.
- **Trigger to revisit**: the day a *second, external* consumer wants to embed the engine, extract `rivet-engine` per ADR-0002's *Future library path* and move this seam into its semver'd surface.
