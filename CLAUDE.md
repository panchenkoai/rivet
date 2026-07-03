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

The two-run test has a blind spot the **idle-first-run** variant covers:
run 1 captures *zero* changes → change → run 2 must capture it. MySQL
shipped losing that change — the checkpoint was written only at a part
commit, so an idle bounded run left no anchor and the next run re-anchored
to a newer "current" position (`SHOW MASTER STATUS`), silently skipping
everything in between. "Enable CDC during a quiet period" is exactly the
ops sequence that hits it. The anchor rule per engine: PostgreSQL pins
server-side at slot creation; SQL Server floors at `fn_cdc_get_min_lsn`
(over-reads, never skips); MySQL has NO server-side anchor — the first
checkpointed open must persist its coordinates immediately
(`first_run_with_zero_changes_pins_the_checkpoint_at_open`). A new engine
must state which of the three anchor models it has and test the idle
variant accordingly.

## Sink part names must be run-unique — prove it with a two-run test

A sink that names its output files with only a per-run sequence
(`cdc-000000`, `part-0`, …) silently overwrites the previous run's files
when consecutive runs share a destination prefix — which is exactly the
documented scheduler model (`until_current: true` on an interval). The
CDC sink shipped this way: run N+1's first part clobbered run N's
`cdc-000000.parquet` *after* the slot had been acked past those changes,
so the data was gone from both the source log and the destination — the
stream-level at-least-once guarantee (peek → flush → ack, fault-hook
tested) was fully correct and the data was still lost, one layer up. The
batch path was already immune (run-timestamp part names + a finalize WARN
on prior `_SUCCESS`); the CDC sink reimplemented naming and silently
dropped both defenses.

Process rule: **every new sink/writer gets a two-consecutive-runs-into-
the-same-prefix test** asserting the union of both runs' rows is readable
afterwards (`roast_second_run_into_same_prefix_must_not_clobber_prior_parts`
in `src/source/cdc/sink.rs` is the template). Run-uniqueness needs
sub-second precision: in live verification two scheduler cycles landed
125 ms apart — a `%Y%m%d_%H%M%S` stamp would have collided; derive the
name from the millisecond `run_id`, filename-sanitized.

## Silent-loss classes from the live GCS run: cells, and names

Two same-day bugs, one lesson each — both survived every count/sum check
and 26 green live CDC tests, and were caught only by a human eye on a
real bucket:

1. **A "degrade to null" cell path is a silent-loss path.** The
   `FixedSizeBinary(16)` builder nulled anything not exactly 16 bytes;
   PostgreSQL's `test_decoding` renders uuids as 36-char text → **100%
   of the column became NULL** while `transactions`-style sum checks
   passed. Rule: every lenient per-cell fallback needs a decode attempt
   per engine's actual wire/text rendering *and* a test per engine; and
   completeness verification must include a **per-column null-profile +
   distinct-count vs the source**, not counts/sums of hand-picked
   columns. (Extends the test-self-oracle rule: re-reading the
   destination is not enough — read *every column* of it.)

2. **Names are labels; catalogs are truth.** SQL Server's schema/table
   came from splitting the capture-instance NAME on `_`
   (`product_catalog` → table `catalog`), so routing dropped 100% of
   events for 6 of 8 tables while runs reported success. Rule: when a
   catalog view exists (`cdc.change_tables`), resolve identity from it;
   a naming heuristic is at most a fallback. Corollary that saved us:
   because flush→checkpoint→ack never ran for the dropped tables, the
   events were still in the change tables and one fixed run recovered
   everything — at-least-once turns a routing bug into a delay, not a
   loss. Preserve that property in every sink change.

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

The very next release (0.16.1) proved how easy it is to get the *guard itself*
wrong. The release runs `cargo publish --locked`; a version bump that did not
commit the regenerated `Cargo.lock` (lock still pinned 0.16.0 against a 0.16.1
manifest) made `--locked` abort with exit 101 — again *after* the tag was cut.
The obvious guard, an offline test that reads `Cargo.lock` at runtime and
compares its `rivet-cli` version to `Cargo.toml`, **does not work**: `cargo test`
silently reconciles the working-tree lock *before* the test body runs, so the
test always reads the fixed version and passes even on a stale committed lock.
(Proven by running the RED case — the desynced lock still passed.) The staleness
lives only in git, and `cargo` erases it on the way to running your check.

Process rule: **a guard for a `--locked` mismatch must itself run `--locked` on a
clean tree, not read files at runtime.** The working guard is a CI step —
`cargo metadata --locked` on the freshly-checked-out commit, *before* any
`cargo build`/`test` reconciles the lock — which fails fast on a stale committed
lock without compiling. More generally: when a check's subject is mutated by the
act of checking it (cargo reconciling the lock, a formatter rewriting the file),
the runtime read is a no-op; assert against the committed state or the
strict-mode tool, never the post-reconcile working tree.

## Session-state-dependent renderings need a non-default-state test

Any text rendering that embeds SESSION state is only exercised on the
non-default state. The concrete bite (finding #24): `test_decoding`
renders `timestamptz` in the polling session's zone; every test stack
runs UTC, so the rendered offset was always `+00` — the parser that
*stripped* the offset (treating Tokyo wall-clock as UTC, +9h corruption)
and could not even parse negative offsets (silent NULL for every
western-zone value) passed the full type matrix for weeks. The `+05` in
the INSERT literal proved nothing: rendering happens at read time, in
the reader's zone.

Process rule: when a value crosses a TEXT rendering, enumerate the
session/server state that shapes the text (timezone, locale/lc_*,
datestyle, bytea_output, sql_mode) and add one live test that flips the
state to a non-default (a `+09:00` global, an `Asia/Tokyo` database
default) with a guard that resets it. Parity at default state is not
evidence — the default is exactly where the bug hides.
