# archfacts — deterministic architecture facts

**Role:** contributor tooling. **Read it when** you run an architecture review
(`rivet-architect`, `/improve-codebase-architecture`, a zoom-out or a diagnosis) or change this tool.
**Question it answers:** which facts about the code can a review cite without re-discovering them, and how far can each be trusted.

A review used to spend its first hundred tool calls finding out what changes often, who calls what,
how many types implement a trait and which files move together. Those are facts, and facts are
computed. Judgement stays with the reviewer: which candidates, how strong, whether a seam is real.

## Commands

```bash
make archfacts                                   # collect both feature sets, print the hot-spot digest
uv run python -m dev.pytools.archfacts collect --features default|no-default-jemalloc|both [--backend scip|treesitter] [--force]
uv run python -m dev.pytools.archfacts view architect --lens hotspots [--top N]
uv run python -m dev.pytools.archfacts view zoom --path src/load
uv run python -m dev.pytools.archfacts view diagnose --fn execute_load
uv run python -m dev.pytools.archfacts view diff          # what exists in only one feature set
make archfacts-selftest                          # fixture crate on both backends + expected.yaml against rivet (indexes the crate)
```

`collect` writes `target/archfacts/<sha>-<key>.json` (git-ignored). The views read that file and
recompute nothing; `--features`, `--backend` or `--facts FILE` choose which one. Run `collect` before
the reviewing agent starts: the architect is read-only and never runs cargo. A reviewer cites a row as
`facts:<field>` and still opens every `file:line` itself.

Lenses of `view architect`: `hotspots`, `seams`, `cdc`, `load`, `runners`, `state-config-cli`,
`harness`. A lens only orders the sections and narrows them to its paths; the digest stays under
300 lines.

## Cost

`rust-analyzer scip` over the whole crate takes about 45 seconds per feature set on a 10-core laptop
(about 105 seconds the first time in a fresh `target/`, while it builds the build scripts and proc
macros) and peaks near 4 GB of RAM. Reading the 50 MB index and deriving every fact takes about five
seconds. The index is cached, so only the first `collect` on a commit pays. The cache key is HEAD +
feature set + rust-analyzer version + this tool's schema version (`SCHEMA` in `config.py`: bump it
when a fact changes meaning), plus a hash of the uncommitted changes under `src/`, `tests/`,
`benches/`, `examples/`, `Cargo.toml`, `Cargo.lock` and `build.rs` (then `meta.dirty` is true). The
git, ADR and glossary facts are cheap and recomputed on every `collect`. `--force` re-indexes.

rust-analyzer must be installed for the pinned toolchain: `rustup component add rust-analyzer rust-src`.

## Backends

- `scip` (default) resolves every reference by type through rust-analyzer's SCIP index. The index is a
  protobuf file; `scip.py` reads it with a ~60-line wire-format decoder, so the tool has no Python
  dependency beyond the standard library (PyYAML, already pinned, only for `selftest --rivet`).
- `treesitter` is a deliberately approximate comparison backend with the same schema. It is a
  token-level scan that resolves names by spelling — no tree-sitter binding is installed. It exists to
  measure what type resolution buys a review; do not cite it.

## Reading the output

- **`approx`** — a list on a record naming the fields that were resolved by spelling rather than by
  type. The `treesitter` backend sets it on every caller, implementor, module-graph, match-site and
  leak field; the `scip` backend sets it only on a trait whose macro-generated impl could belong to
  several same-named traits. A field listed in `approx` is a hint, not a fact. `ambiguous: true` adds
  that the name is shared, so the number is known to be merged across items.
- **`degraded`** — `meta.degraded` is true when more than 2% of the identifiers in indexed production
  code carry no symbol (`unresolved`). Then caller and implementor counts are lower bounds. On a
  healthy index the share is about 0.2%: tokens inside macro calls that rust-analyzer does not map back.
- **`depth_proxy`** — implementation lines divided by public items plus their parameters. It is a
  PROXY for where to look. The design vocabulary explicitly rejects "depth = implementation lines /
  interface lines": depth is leverage at the interface, which no line count measures. Never report
  this number as depth.
- **`call_sites` / `callers_modules` / `test_refs`** — reference sites outside `use` statements,
  split into production and test code (`tests/`, `#[cfg(test)]`, `#[test]`). A call through a trait
  object or a generic is counted on the trait's method, not on the implementing method.
- **`cycles`** — per module, the modules it mutually depends on; top-level `cycles` lists the strongly
  connected components. A module is a source file (`src/a/mod.rs` is `a`). References are attributed
  to the defining module, so a `pub use` re-export does not hide a cycle.
- **`traits`** — `impls_prod` / `impls_test` count `impl Trait for T` blocks whose trait resolves to
  this trait, plus one per call of a `macro_rules!` that expands to such an impl. Exactly one
  production impl sets `hypothetical_seam`.
- **`passthrough_to`** — the body is a single call whose arguments are only the function's own
  parameters.
- **`enum_matches`** — `match` expressions whose arm patterns name a variant of the enum. `if let` and
  `matches!` are not counted.
- **`engine_leaks`** — references from outside an engine's own modules to a type defined in them or to
  anything from its driver crate. The engine table is `ENGINES` in `config.py`.
- **`feature_diff`** — items and modules that exist in only one feature set. CI builds
  `--no-default-features --features jemalloc`, where the `oracle` code is absent.
- **`git`** — churn over the last 200 commits and co-change pairs with `confidence` = commits together
  / commits of the less-changed file; a commit touching more than 30 files adds churn but no pairs.

## Limits

- The index describes the host platform. Code under `#[cfg(target_os = "linux")]` or
  `#[cfg(not(unix))]` is compiled out on a macOS host and listed under `compiled_out`.

- rust-analyzer gives the same symbol to same-named items in different targets of the package (the
  lib, the bins, each integration-test crate). A reference is resolved to the definition in its own
  file, then its own directory, then `src/`.
- Code inside a `macro_rules!` body is not scanned; an item produced by a macro call is found through
  the index (`macro_generated`) and has no parameter count.
- Functions and types nested inside a function body are not items.

## Self-test

`python3 -m dev.pytools.archfacts selftest` grades the tool against `fixture/`, a small crate where
every construct has a known answer, on both backends: exact values for `scip`, and for `treesitter`
the `approx` marks plus the wrong answers pinned as wrong. CI runs it with `--no-index` (the image has
no rust-analyzer), which skips the `scip` half with a counted `SKIP` line. `--rivet` adds
`expected.yaml`: facts about rivet itself, each verified by hand with the command recorded next to it.
When one drifts the self-test fails; re-verify it and edit the file deliberately.
