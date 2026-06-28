<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
| ------ | ---------- |
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.

## Verify before publishing agent-walk claims

When a subagent (`Agent(Explore, …)` walk, or any out-of-band
investigation) returns claims with **specific file paths or line
numbers**, do **not** copy those claims into reports, ADRs, or PR
descriptions without first opening the named files yourself.

Specific claims look authoritative; that does not make them correct.
A walk in this repo has produced false claims like *"struct X is
separate per engine"* (when the struct was already shared), *"function
Y exists twice"* (defined once as a method on a shared struct),
*"callers check Z() before each emission"* (when the dispatcher
already centralised the check). Each of these was a `grep` away from
falsification but landed in an HTML architecture report unverified.

Process rule: **for any specific structural claim** (file path, line
number, "exists twice", "duplicated across N callers") **the next
tool call after the walk's report is a `Read` / graph query on the
named site**. Only then write the claim into a deliverable.

The grep is cheap. The wrong claim in a report wastes a future
contributor's day chasing a smell that does not exist.

## Remediation hints must recover from the degraded state

When emitting a fix-it suggestion — a `cast_sql`, a repair query, a
"run this to recover" hint — verify it actually works **from the
already-degraded state**, not from the original value. If the
degradation is lossy (integer overflow, truncation, a naive-vs-instant
timestamp ambiguity, a dropped logical type) no post-hoc cast can
recover it: the only valid remediation is upstream (a declared load
schema / target-native column), and the hint must say exactly that —
never a SELECT-time cast that silently runs on corrupted data.

This bit the target resolver (`src/types/target.rs`): a `UINT64`
column was given `cast_sql = CAST(col AS NUMERIC)`, but once it
autoloads as `INT64` it has already overflowed, so the cast recovers
nothing. The fix: `cast_sql` is `Some(..)` **only** when the autoloaded
value still holds the information losslessly (JSON bytes → `PARSE_JSON`,
UUID bytes → `TO_HEX`); for overflow (`UINT64`) and naive-timestamp the
value is gone at autoload, so `cast_sql` is `None` and the note points
to the load schema.

Process rule: **every lossy degradation that surfaces a remediation
gets a regression edge-case test** asserting the recovery is real — a
post-load cast only where it is lossless, otherwise `None` + an
upstream (load-schema) note, and never a silent no-op. When self-review
catches a bug of this shape, add the edge-case test alongside the fix,
not just the fix.

## CDC resume is per-engine — verify it empirically, twice

A CDC adapter that **captures** correctly can still fail to **resume**:
capture-works ≠ resume-works, and a single-run test sees only capture.
The SQL Server adapter (`src/source/mssql/cdc.rs`) shipped reading
`fn_cdc_get_all_changes(get_min_lsn(), get_max_lsn())` on **every** poll
— it re-read the entire retained change table each run (at-least-
*everything*, not at-least-once) because `open()` never consumed the
checkpoint LSN. The gap was invisible until run twice: capture 2 →
re-run with no new changes → it returned the same 2 instead of 0.

Process rule: **for every CDC engine, prove resume with a two-run live
test** — run, change, resume — asserting the second run captures *only*
the new changes (and, with the `cdc_after_flush_before_ack` fault hook,
that a crash before the checkpoint re-reads rather than loses). Each
engine resumes by a different mechanism (MySQL binlog checkpoint file,
PostgreSQL slot advance, SQL Server from-LSN), so one engine passing
proves nothing about another. Never infer resume from capture.

## Performance diagnosis: measure cold, don't theorize

A "why is this slow?" answer reasoned from the code is a hypothesis, not a
finding — and on the *same* symptom this repo's plausible hypothesis was wrong
three times running. A reported "~12s of idle before the first chunk" on a
chunked `content_items` export was blamed, in order, on a window function
(wrong — `ROW_NUMBER` only runs for `chunk_dense`), on the `SELECT … FROM
(<wide query>)` boundary-probe wrap (wrong — PostgreSQL pushes the `min`/`max`
into the index: 0.1 ms), and on the `COUNT(*) - COUNT(col)` null-key check
(wrong — index-only, 90 ms warm). The real cause, found only by running the
binary **cold** with elapsed-time-prefixed `RUST_LOG` output, was the first
chunk reading 92 MB of wide rows with **no progress feedback** — not a query at
all. Each wrong guess shipped a real fix (they were genuine improvements) that
did nothing for the actual symptom.

Process rule: **for any latency claim, reproduce it and read the timeline before
proposing a fix.** Restart the source (`docker restart rivet-postgres-1`) for a
cold cache; run the real binary with per-line elapsed timestamps and let the
trace name the slow step; `EXPLAIN (ANALYZE, BUFFERS)` the exact SQL rivet emits
rather than guessing the plan. A fix shipped against an unmeasured hypothesis is
a guess wearing a diff — each of the three wrong guesses above cost a build +
install + dogfood round-trip that a 20-second cold trace would have skipped.

## Verify the real release build path, not just `cargo build`

A green `cargo build` does not mean the release will build. The release path runs
**different, stricter tooling** than local dev, and the gap only surfaces at the
tag — *after* crates.io, the binaries, and the GitHub release have already
published, when the failure is no longer re-runnable from the immutable tag.

0.16.0 shipped this way: `cargo build` plus the whole offline + live suite were
green, all four binaries and the crate published — but **both Docker images
failed**. The release Dockerfile's `cargo chef prepare` (dependency-cache layer)
parses `Cargo.toml` with `cargo-manifest`, a **spec-strict** TOML parser that
rejects multi-line inline tables (`postgres = {` … newline … `}`). `cargo`'s own
parser tolerates them, so the two offending tables sailed through every local
check for months and broke only the one build path no local command exercises.
The fix was one line each; the cost was a whole 0.16.1 patch release.

Process rule: **before tagging a release, run the release build path itself, not
a proxy for it** — build the Docker image locally (or at least run `cargo chef
prepare`), plus whatever `cargo publish --dry-run` / cross-compile / packaging
step the workflow runs. When a release-only tool (cargo-chef, cargo-manifest, a
stricter MSRV image, a `--locked` resolve) can reject what `cargo build` accepts,
add a **local regression guard** that mimics its check (here: the
`cargo_manifest_chef` offline test asserts no multi-line inline tables) so the
mismatch fails loud and local instead of half-way through publishing.
