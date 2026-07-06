# ADR-0002: CLI Product vs Library

**Status**: Accepted  
**Date**: 2026-04  
**Context**: Rivet ships as a single crate (`rivet-cli` on crates.io) that produces both a library target (`rivet`) and a binary target (`rivet`). The default Rust project layout creates accidental public API surface — any module marked `pub` in `lib.rs` is reachable by external consumers. This ADR decides intentional product boundaries.

---

## Decision

**Rivet is a CLI-first product. The library crate (`rivet`) is not a stable public API.**

Rivet's primary deliverable is the `rivet` binary: end users invoke it from the command line to export data from PostgreSQL/MySQL databases to Parquet/CSV files. No embedding contract, no programmatic API stability guarantee, no semver guarantee on internal types.

The library target exists solely to enable Rust's integration test harness (`tests/*.rs` must link against a library crate). It is an implementation artifact, not a product surface.

---

## Rationale

### Why CLI-first, not library

1. **Use case fit**: The tool solves a concrete operational task (export data). Embedding it in other Rust programs is not a stated use case and adds maintenance overhead (API stability, semver discipline, docs).
2. **Crate name signals intent**: The crate is published as `rivet-cli`, not `rivet`. The `-cli` suffix is the standard Rust convention for CLI tools that are not intended as embeddable libraries.
3. **Binary is the integration point**: All known consumers use the binary — via shell scripts, Docker images, CI pipelines. No known Rust consumer imports the library crate.
4. **Internal types are not API-stable**: `ResolvedRunPlan`, `ExtractionStrategy`, `StateStore`, `SourceTuning` and similar types evolve to serve the pipeline's execution model. Treating them as public API would force design compromises on internal evolution.

### Why the library crate still exists

Rust's integration tests (`tests/` directory) must link against a library target. There is no way to run integration tests against a binary-only crate. The library crate is the Rust mechanism that grants `tests/*.rs` access to internal implementations.

---

## Module Visibility Rules

Reflects `src/lib.rs` as of v0.8.0. `pub` modules are reachable cross-crate
**only** so `tests/*.rs` (and the in-crate MCP surface) can link them — none
carry a stability guarantee (see Consequences #1). `pub` here means "the test
harness needs it", not "public API".

| Module | `lib.rs` visibility | Reason |
|--------|--------------------|----|
| `config` | `pub` | Integration tests import config types (`Config`, `ExportMode`, …) |
| `error` | `pub` | `Result` alias surfaced for the test harness |
| `format` | `pub` | Integration tests validate format output (`CsvFormat`, `ParquetFormat`, …) |
| `journal` | `pub` | `RunJournal` event log — trust-contract type asserted in tests |
| `manifest` | `pub` | `RunManifest` wire schema (ADR-0012) — asserted in trust-artifact tests |
| `pipeline` | `pub` | Integration tests call pipeline functions (`generate_chunks`, `classify_error`, …) |
| `preflight` | `pub` | Integration tests exercise diagnostics / type-report |
| `resource` | `pub` | Integration tests verify memory utilities (`get_rss_mb`, `check_memory`, …) |
| `source` | `pub` | Live integration tests construct `ExportRequest` / introspection directly |
| `state` | `pub` | Integration tests verify state invariants (`StateStore`, `SchemaColumn`) |
| `tuning` | `pub` | Governor / adaptive tuning tests link it (ADR-0019) |
| `types` | `pub` | Type-roundtrip tests assert `RivetType` / fidelity mappings (ADR-0014) |
| `mcp` | `pub` | In-crate MCP server surface (code-review-graph integration) |
| `redact` | `pub` | Cross-cutting credential-redaction helper, asserted in tests |
| `destination_for_tests` | `pub` | Thin test-only shim over the `pub(crate)` `destination` module |
| `destination` | `pub(crate)` | Internal write backends — exercised via `destination_for_tests` |
| `enrich` | `pub(crate)` | Internal pipeline module |
| `notify` | `pub(crate)` | Internal notification module |
| `plan` | `pub(crate)` | Internal execution contract — consumed by pipeline, not by tests |
| `quality` | `pub(crate)` | Internal quality gate |
| `sql` | `pub(crate)` | SQL identifier quoting (`quote_ident`) — internal utility, not a product surface |
| `test_hook` | `pub(crate)` | Internal fault-injection points for tests |

---

## Consequences

- **No stability guarantee**: Consumers who depend on internal modules (any non-`pub` module above, or sub-items of `pub` modules not explicitly documented) accept breakage at any patch release.
- **Docs reflect intent**: `cargo doc` will not generate docs for `pub(crate)` modules, reducing confusion about the intended API surface.
- **Binary compilation path**: `src/main.rs` declares all modules privately via `mod` — it never uses the library crate. The two targets are independent compilation units that happen to share source files.
- **Future library path**: If Rivet ever offers a stable embedding API, a separate `rivet-engine` crate should be extracted with its own semver-tracked surface, rather than promoting internal types to `pub`.
  - *Amended by [ADR-0026](0026-first-party-extension-seam.md)*: a minimal **first-party** extension seam (the `types`/`types::target` resolution items) is now stability-tracked in-crate for the private `rivet-pro` companion. The full `rivet-engine` extraction is deferred until a *non-first-party* external consumer appears.

---

## Alternatives Considered

### Make everything `pub(crate)`, move tests inline

Moving `tests/*.rs` into the library as `#[cfg(test)] mod tests` would allow all modules to be `pub(crate)`. This was rejected because:
- Integration tests (especially chunk/state invariants) benefit from the clean external-crate perspective
- `tests/` layout is idiomatic and easier to locate

### Extract a `rivet-engine` crate now

Premature. No known consumers exist. The extraction cost (separate crate, two Cargo.toml files, re-exports) is not justified until there is a concrete embedding use case.
