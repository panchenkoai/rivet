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

Two sibling facets the coverage matrix later caught in the **same** resolver
— the `target_type` / `autoload_type` fields must tell the truth, not just
the `cast_sql`. (1) **Never claim `Ok` for a native type the warehouse
cannot hold.** Snowflake's decimal arm returned `Ok("NUMBER(50,10)")` with
no precision ceiling, but Snowflake `NUMBER` maxes at precision 38 — the
load would reject it; guard the ceiling and `Fail` past it, the way BigQuery
already does past BIGNUMERIC. (2) **`autoload_type` must be the EMPIRICAL
autoload, flagged as a divergence when it differs from the target.** DuckDB's
wide-decimal arm labelled the autoload `"DECIMAL(38,*)"` (== target, so *not*
counted a divergence), but real DuckDB autoloads `decimal(50,10)` as `DOUBLE`
(lossy past 2^53). A `warn(t)` that sets `autoload == target` hides a real
divergence — use `diverge(target, autoload, note, None)` with the true
autoload when it's lossy. Both were silent: a preflight report confidently
describing a type the warehouse would reject or silently coerce.

## The `committed` flag marks a TRANSACTION boundary — never every event

The sink rolls (flush → checkpoint → ack) only on a `committed` event, to keep
the "never split a transaction across parts" invariant. So `committed` must mark
the LAST event of a source transaction, NOT every event. MySQL got this right
(only the XID event is `committed`), but PostgreSQL (`test_decoding`) and SQL
Server (change-table rows) shipped `committed: true` on EVERY event — so a
transaction larger than `rollover` rolled + checkpointed MID-transaction, and a
crash between that checkpoint and the tail's flush advanced the resume position
(PG slot / MSSQL from-LSN) PAST the transaction's commit; resume reads strictly
after it and skips the tail — an at-least-once break (RED-proven both engines:
a 12-row transaction at rollover 5, crashed at `cdc_after_ack` (PG) /
`cdc_after_checkpoint_before_ack` (MSSQL), lost 7 rows). Fix: the adapter frames
the transaction (PG BEGIN…COMMIT; MSSQL rows sharing `__$start_lsn`) and marks
only its last event `committed`. Process rule: **any new poll/log CDC adapter
must mark `committed` at the true commit boundary, and a large-transaction
mid-flush-crash test (`roast_*_large_transaction_is_atomic_across_a_mid_flush_
crash`) must RED against a `committed: true`-on-every-event mutant.** The tell
that this is wrong: `committed` set unconditionally in the per-event constructor
instead of computed from the transaction framing.

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

## A bounded drain stops at an OPEN-TIME snapshot, never "when it catches up"

`until_current` must mean "current as of the moment the run opened" — a
snapshot taken at open (PG `pg_current_wal_lsn()`, MSSQL `fn_cdc_get_max_lsn()`
pinned once, MySQL the open-time binlog coordinates, Mongo `operationTime`) with
the first commit PAST it ending the stream. But be honest about WHICH engine
actually needs it, and **run the disable-bound probe PER ENGINE — never
generalize one engine's result to another** (this bit twice: the first pass
over-claimed "three of four shipped chasing"; the honesty-correction then
over-corrected the OTHER way — "only PG is load-bearing" — by generalizing the
MySQL/MSSQL probe to Mongo WITHOUT probing Mongo, and a round-2 review caught
that Mongo actually hangs without its bound). The truth, each verified by
disabling that engine's bound: **load-bearing** on the two engines with a
re-reading / tailable reader — PostgreSQL (non-consuming slot peek re-reads from
the un-acked position; a paced 10-row/5 ms writer held it past a 30 s ceiling)
and MongoDB (a tailable change stream whose `next_if_any` keeps returning events
under sustained writes, so the empty-poll check never fires — disabling
`until_current_ts` HANGS the sustained-writes test). **Belt-and-suspenders** on
the two with native catch-up — MySQL (`BINLOG_DUMP_NON_BLOCK` EOF) and MSSQL (the
Agent's scan-gap empty poll) terminate with the bound disabled, so their
open-time pin is only a precise-stop refinement. The excluded tail is deferred,
never lost: checkpoint stops at the last in-bound commit, the next cycle resumes
there — the two-run union test proves DEFER-NOT-DROP on every engine
(`roast_*_until_current_open_bound_two_runs_lose_nothing`), but only the
load-bearing engines' TERMINATION goes RED without the bound; the others are
belt-and-suspenders confirmations (say so in the test, don't claim they prove
the clip). Keep the catch-up exit as the backstop, gate the snapshot on
`until_current` so the daemon mode is untouched, and fail OPEN on an unparseable
boundary (delayed termination is recoverable; an early exit is a dropped commit).

Sibling trap this class caught, and a warning about band-aid fixes: **a
non-consuming peek only slides forward when the consumer ACKS — so slot progress
is the sink's job, not the peek budget's.** PG's `pg_logical_slot_peek_changes`
re-reads from the slot's un-acked position every call, and the slot advances
only when the sink acks a captured part. So ANY WAL the run consumes but does
not capture — an uncaptured-table transaction, the `BEGIN`/`COMMIT` marker rows,
an empty/DDL span — never moved the slot: the peek re-read the same window, the
run exhausted, and it wrote `_SUCCESS` with in-bound captured data unread. The
FIRST fix (a ×3 peek escalation) was a band-aid — it covered the captured-marker
ratio (3 wire rows : 1 change) but an uncaptured/empty span has an UNBOUNDED
wire:capture ratio, so a span larger than the escalated window still starved
(an ultracode review found this: `roast_pg_cdc_reaches_open_bound_past_a_large_
uncaptured_transaction` — a 200-row uncaptured tx ahead of the backlog captured
ZERO in-bound rows at rollover 5). The real fix is a **sink re-drain loop**
(`src/source/cdc/sink.rs::run_to_files`): each pass flushes + acks the consumed
span (advancing the slot past uncaptured/empty WAL, whose commit boundary is
recorded before the routing filter), then re-peeks fresh WAL, until a pass
yields nothing — reaching the bound at any density, RSS back to O(rollover).
Process rule: **when a re-read/starvation shows up on a non-consuming reader,
fix it by ACKING the consumed span (advance the cursor), not by peeking more —
a bigger peek only defers the same starvation to a larger span.** A starvation
fixture must put UNCAPTURED or empty traffic ahead of the captured data (the
unbounded-ratio shape), not just single-row captured txs (ratio 3, which a fixed
budget can cover).

Second sibling, from the adversarial pass over the same branch: **row-less
transactions starve the ACK, not just the budget**. DDL churn decodes as empty
BEGIN/COMMIT pairs — no events reach the sink, the sink never acks, and on a
consume-retention engine (PG slot) the anchor pins WAL behind the noise forever
on an idle database (RED: `confirmed_flush_lsn` frozen across a DDL-churn run).
A ZERO-YIELD run must release the data-free span itself (safe by construction
— `release_empty_frontier`); engines whose retention is reader-independent
(MySQL binlog, MSSQL change tables, Mongo oplog) need nothing. When adding a
consume-retention engine, test the DDL-churn case against the SERVER's anchor
position, never rivet's counters (`roast_pg_cdc_empty_transaction_churn_must_
not_pin_the_slot`).

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
keyset / mongo_parallel paths were immune (millisecond part stamp) — but the
plain batch full/incremental path (`src/pipeline/single.rs`) was **NOT**: an
earlier version of this rule wrongly claimed "the batch path was already
immune". It stamped parts `%Y%m%d_%H%M%S` (second-granularity, no run id), so
two sub-second runs into one prefix collided and the later silently
overwrote the earlier via `idempotent_overwrite`. The resilience coverage
matrix caught it; a live test lost **3 of 6 incremental deltas** (RED) before
the one-line fix (`single.rs` → `%3f`, matching `keyset.rs`).

Process rule: **every new sink/writer gets a two-consecutive-runs-into-
the-same-prefix test** asserting the union of both runs' rows is readable
afterwards. Templates: `roast_second_run_into_same_prefix_must_not_clobber_prior_parts`
(`src/source/cdc/sink.rs`) and `roast_rapid_incremental_runs_into_same_prefix_must_not_clobber_prior_parts`
(`tests/live/live_resume.rs`, the batch path). Run-uniqueness needs sub-second
precision: two scheduler cycles landed 125 ms apart live — a `%Y%m%d_%H%M%S`
stamp collides; derive the name from a millisecond stamp or the `run_id`,
filename-sanitized. **A test that passes only by sleeping ≥1s between runs is
documenting the gap, not closing it** — the batch clobber hid behind exactly
such a `sleep(1100ms)` for months.

The rule extends past the parts to **every per-run SIDECAR**. The CDC sink
named its parts run-uniquely but wrote the manifest to the fixed
`manifest.json` — N `until_current` runs into one prefix clobbered it (parts
accumulated, manifest didn't), and the first consumer that summed manifests
across runs (the Pro loader's reconcile) under-counted 30 parts as 55 rows.
No OSS test saw it because every "repeated run" test re-read the PARQUET —
the artifact that survives — never the manifest. Fix: `write_manifest` leaves
an immutable `manifest-<run_id>.json` copy beside the canonical last-writer-
wins pointer (`run_unique_manifest_name` / `is_run_unique_manifest_name` in
`src/manifest.rs`); guard/validate/resume keep reading the canonical name.
Process rule: **a "repeated runs accumulate" claim must be asserted on the
manifest copies (`dir_manifest_copy_total_rows`), not a data re-read** — the
data surviving is exactly what makes a sidecar clobber invisible.

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

## Benchmark against the DOWNLOADED latest release binary, never a rebuilt parent

A perf regression check compares the working-tree build against the PREVIOUS
version. Do NOT `git checkout <parent> && cargo build --release` to get it — the
release profile is `lto = "fat"` + `codegen-units = 1`, so each build is MINUTES,
and a before/after wastes two of them (measured: the Form B before/after cost two
full fat-LTO rebuilds when one download would have done). Instead **download the
latest already-BUILT release binary** — the artifact the release pipeline already
published (a GitHub release asset, or the Homebrew bottle `brew install rivet`) —
and compare the current `cargo build --release` against it. It is faster AND more
honest: you compare against what users actually run (the release-pipeline binary),
not a locally-rebuilt approximation. `cargo install rivet-cli@<ver>` does NOT count
— it rebuilds from source, the exact cost this avoids. Keep the CURRENT side a
`--release` build (dev/debug is unrepresentative for perf). Baseline reference for
the macro-export bench (200k-row keyset, 5 cols, zstd, release): ~0.66 s warm
(~300k rows/s), ~26 MB flat peak RSS — a regression is a wall-clock or RSS move
past measurement noise on that fixture.

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

The tz find was only the first bracket. The source-parity sweep under a
flipped session later caught two MORE on the same `test_decoding` reader:
`datestyle='German, DMY'` nulled **every** timestamp (the ISO parser choked
on DMY text) and `bytea_output='escape'` corrupted **every** bytea (the
reader assumes hex). The fix is not to teach the parser every format — it's
to **pin the formats on the reader's own connection**
(`SET datestyle='ISO, MDY'; SET bytea_output='hex'; SET intervalstyle='postgres'`
in `src/source/postgres/cdc.rs::open`), immune to the DB default. Binary
readers (MySQL binlog, MSSQL CT, the PG *batch* binary protocol) are exempt
by construction on the READ — this class is text-decode-only. Regression:
`pg_cdc_non_iso_datestyle_and_escape_bytea_match_batch`.

Round-8 corrected the "batch binary protocol is exempt" over-claim: the READ
is exempt, but the batch **incremental/keyset cursor** is a session-dependent
text *ENCODE* on the write-back leg. The cursor boundary is re-injected as an
OFFSET-LESS naive literal into `WHERE col > '<lit>'`, and PostgreSQL parses a
naive literal in the SESSION TimeZone — so on any non-UTC session (a common
prod default) the boundary shifts by the zone offset and every incremental run
silently SKIPS (west) / duplicates (east) the offset window. The READ was
UTC-absolute, so counts/sums passed under CI's UTC session — the exact blind
spot. Same fix shape (pin on the connection): `SET LOCAL TimeZone='UTC'` in
`src/source/postgres/mod.rs::pg_run_export`, mirroring MySQL's `time_zone='+00:00'`.
Process rule extension: the enumeration covers BOTH legs — a value CROSSING a
text rendering (decode, exempt for binary) AND a cursor/boundary value
RE-INJECTED as a text literal (encode, NOT exempt even on a binary reader).
Regression: `pg_incremental_cursor_survives_a_non_utc_session_timezone`.

## Keyset seek is type-bracketed — a heterogeneous key silently loses all but one type

A keyset/seek paginator that advances with `col > cursor` assumes the key
space is ONE totally-ordered domain. On a store where the comparison operator
is **type-bracketed**, that assumption fails silently. MongoDB's `$gt`/`$lt`
compare only WITHIN a BSON type bracket (numbers, then strings, then …): once a
keyset cursor reaches the last number, `{_id: {$gt: 2000}}` matches **zero**
string `_id`s even though strings sort *after* numbers — so a collection mixing
`_id` types pages only the first bracket and drops the rest. Verified: an
int+string collection read 2000/4000 single-worker (parallel varied ~3000/4000,
because `$sample` boundaries then straddle the type gap). Every count/sum check
that trusts the paginator passes — the loss is 100% invisible without a
per-type re-read of the destination.

The full-scan path (a single unbounded cursor sorted by `_id`, no `$gt` seek)
is immune — it crosses brackets. So the fix is not to make keyset cross types
(the index seek fundamentally can't) but to **detect the heterogeneous key up
front and refuse keyset/parallel loudly**, pointing at the full scan. The
detector compares the BSON *bracket* (not the raw type) of the min and max
`_id` — the four numeric types share a bracket, so a mixed Int32/Int64/Double
`_id` still keysets (`src/source/mongo/mod.rs::ensure_uniform_id_type`,
`id_bracket`).

Process rule: **any seek/keyset paginator on a store whose range operator is
type-bracketed (Mongo `$gt`, and check before assuming any document/columnar
store isn't) needs a heterogeneous-key guard + a live test that seeds two key
types and asserts the paginator ERRORS (never silently exports one bracket),
while the unbounded scan stays complete.** Counts/sums are not evidence here —
the dropped bracket is a clean, self-consistent subset.

## Every engine's live tests go through the canonical Rig — no bespoke harness

There is ONE way to build a config, run rivet, and read the output back in a
live test: the `Rig` (`tests/common/rig.rs`). It owns its tempdir (no leaks),
renders the config for any `source.type`, drives batch (`run_ok` /
`run_expect_fail` / `run_and_read`) and CDC (`mode: cdc` via `mongo_cdc`/
`mysql_cdc`/… + `checkpoint_path`), injects faults (`run_with_env`), and the
CDC conformance gate recognises `Rig::run*` as its capture markers. It exists
because ~250 hand-rolled YAML templates + ~240 inline `Command::new(RIVET_BIN)`
sites drifted apart; the rig re-converged them.

When a NEW source engine lands (the Mongo pass was the reminder), wire its live
tests through the rig, do NOT stand up a parallel path:

- Add `Rig::<engine>_batch(table)` (+ `Rig::<engine>_cdc(table)` if it has CDC)
  and any engine-specific option method (`.mongo("page_size: 500")`), rather
  than a per-file `write_cfg` / `run_export` / `cdc_run` helper. A private
  config-writer that `format!`s the YAML — and worse, a `std::mem::forget`
  tempdir to keep the path alive — is the exact smell the rig removes (the rig
  already owns the tempdir).
- Drive CDC through **config mode** (`mode: cdc`) like the SQL engines, not the
  `rivet cdc` CLI subcommand, so all engines share one CDC path and the gate's
  `Rig::run*` capture markers apply without a bespoke addition.
- If the engine's read-back oracle is new (Mongo's `read_mongo_cdc_changes`),
  add that ONE marker to `every_live_cdc_test_asserts_an_outcome`'s dictionary —
  don't route captures around the gate.

Process rule: **a new engine's test PR that introduces a `write_cfg`-style YAML
builder or a `*_run`/`cdc_run` command wrapper instead of a `Rig::<engine>_*`
constructor is incomplete** — the review asks for the rig constructor. The rig
is the seam; per-file config builders are the ~250 templates it replaced coming
back one engine at a time.

## A sparse chunk key is a silent 100× slowdown — warn at run start, not info

`chunk_size` in range chunking divides the **key span** `(min..max)/chunk_size`,
NOT the row count. On a sparse / huge / gappy key the window count explodes far
past what the rows justify — a real 520 k-row MySQL table over an SSM tunnel took
**31 min** because `id` spanned 950 M→1.29 B: `342.7 M / 100 000 = 3428` near-
empty BETWEEN windows, each a separate source query paying the tunnel round-trip
(~152 real rows/window). Nothing was wrong with capture; the plan was pathological
and **rivet said nothing the user could see**. The density diagnostic existed
(`detect.rs::log_chunk_sparsity_at_run`) but logged at **`info`** (invisible at
default level), and for **MySQL it was skipped entirely** — `TABLE_ROWS` is too
unreliable, so `row_estimate` is `None` and the whole sparsity check fell through
to an info-level "skipped". The one engine most likely to hit it got the least
warning.

The fix (`detect.rs::sparse_chunk_warning`, pure + unit-tested): a **`warn`**-level,
actionable headline emitted right after chunk generation, **before** the windows
execute, naming the escape (`chunk_dense: true` / `chunk_count: N` / `mode: full`).
Two regimes because only some engines have a scan-free row count: **with** an
estimate (PG/MSSQL `reltuples` / `dm_db_partition_stats`) flag a ≥4× blow-up over
the dense window count; **without** one (MySQL, curated query) flag only an
egregious absolute count (≥1000), hedged on "if the key is sparse". This is cheap
to detect — min/max on the PK is a single index-boundary seek per side (MySQL
"optimized away", PG index InitPlan), and the range path issues **no** `COUNT(*)`
— so the warning genuinely fires within the first round-trips, not after the run.

Process rule: **any run-start diagnostic that tells the user their config will be
slow must be `warn`, not `info`** (default log level hides `info`, so an
info-level "this will be slow" is functionally silent), **and must degrade to a
count-only heuristic on engines with no trustworthy scan-free row estimate**
rather than skipping — the engine with the weakest catalog stats (MySQL) is
exactly where the pathology is least visible. A sparsity/cost check gated behind
`row_estimate.is_some()` is a check that abandons the user who needs it most.

## A green test that was never RED is unverified — mutate the product to prove it

The 2026-07 audit found **63 green tests that could not fail against the exact
bug they guarded**: sleeps masking a fixed granularity contract, CDC tests
trusting `manifest_rows()` (rivet's own summary), cloud-dest "all rows" cells
asserting file PRESENCE (`mc ls | wc -l`), reconcile tests trusting rivet's own
verdict. A 3766-mutant corpus then measured the same defect from the other
side: whole functions stubbed to `Ok(Default::default())` —
`finalize_manifest`, `apply_m8_resume_decisions`, `source_checksums` — survived
the full lib suite. 23 real gaps were closed the same day, every closure
RED-proven. Three rules fell out, each bitten at least twice:

1. **RED-prove every data-loss/integrity test before committing it.** Apply
   the mutant (stub the function, flip the operator), watch the NEW test fail,
   revert. One of the audit's own fixes was vacuous against its intended
   mutant (`recompute_passed()` masked it — the mutant proved EQUIVALENT, a
   different verdict entirely); another assert was fix-invariant (`file_log`
   accumulates whether or not the manifest sidecar clobbers) and passed both
   pre- and post-fix. Reading the test cannot tell you this; only the mutant
   can. Enforcement: `cargo mutants --in-diff` gates PRs (ci.yml), the nightly
   tier rotation ratchets against `docs/mutants-baseline.txt` (shrink-only),
   equivalents live in `.cargo/mutants.toml` with reasons.

2. **Fixtures must cross the mechanism's activation threshold.** A fold over
   ONE element makes every fold operator identical (`0^s == 0|s` — the
   single-part form-B fixture hid `^=`→`|=` in validate's cross-part fold);
   a single run makes any part-name granularity collision-free (the
   `sleep(1100ms)`s hid the second-granularity clobber); a single row hides
   accumulation arithmetic. If the mechanism under test folds/accumulates/
   collides, the fixture needs ≥2 of the thing — same family as the
   "engineer fixtures past activation thresholds" self-oracle rule.

   Sharpened by the row-hash find (0.24.0 pre-release hunt): **an INJECTIVITY
   or FRAMING guard needs ≥2 FIELDS, because with one field there is no
   boundary to forge.** `_rivet_row_hash` concatenated cells with a bare `\x1f`
   and no length from the day it was born (`9aef5cb`, 2026-03-28), so
   `("a\x1f","b")` and `("a","\x1fb")` were one hash — and its only guard,
   `hash_distinguishes_null_from_empty`, shipped in the SAME commit with a
   ONE-column fixture, which cannot express a field boundary at all. It also
   picked the easy pair: NULL vs `""` is the distinction rivet's own `is_null`
   branch already made, not NULL vs a value that RENDERS AS the null marker
   (a lone `\x00`, which collided). Four months green, reading like an
   injectivity test while testing half of one. When a test's subject is "can
   two different inputs produce one output", enumerate the ways a value can
   imitate a DELIMITER, a LENGTH, or ABSENCE — one field tests none of them.

   The same find's second half is a rule about FEATURES, not fixtures: **a new
   TYPE must be run through every value-consuming mechanism, not just the
   read/write path.** Arrays (`RivetType::List`, `c2742ef`, 2026-05-12) landed
   six weeks after the hash and nobody asked whether the hash could
   canonicalize one — Arrow's container display joins elements with `", "` and
   renders a NULL element as the EMPTY string, so `["a, b"]` == `["a","b"]` and
   `[NULL]` == `[""]` == `[]` in the hash. A container's canonical form must be
   built from its CHILDREN (element count, then each child framed the same way),
   never from its rendered text; a lossy rendering cannot be rescued by framing
   the field around it. When adding a type, grep for every hash/checksum/
   comparison/fingerprint it can now reach and add a per-type injectivity case
   to each.

3. **A sleep (or any workaround) in a test that compensates for PRODUCT
   behaviour is a product bug report, not a test fix.** The `sleep(1100ms)`
   authors knew rivet stamped filenames at second granularity — the comment
   said so — and routed the test around a real data-loss bug instead of
   filing it. 29 such sleeps also INVERTED over time: after the `%3f` fix
   they made the tests permanently unable to catch a regression back.
   When a test needs artificial separation (time, ordering, uniqueness) that
   the product claims to provide, stop and check the product first.

Scope honesty, first and most expensive: **`cargo mutants -- --lib` on a
live-only path reports 100% survival and means NOTHING.** Both chunk-checkpoint
runners measured 84 of 84 MISSED — not 84 gaps, but proof that no unit test
executes those files at all, since they need a real source and destination. The
same scoping inflates every `src/pipeline/` survivor count. Read a survivor as
"no UNIT test", then decide whether the honest oracle is a unit test or a live
one, and PROVE which by mutating and running the live suite: `total_rows += `
→ `*=` (single) looked like a glaring hole and four live tests caught it;
`apply_m8_resume_decisions` stubbed to `Ok(Default::default())` — named in this
file as a lib-suite survivor — is caught by three. What the live suite did NOT
catch was worth the search: see the failed-chunk guard below.

The corollary that keeps that exclusion HONEST: **a body the offline gate cannot
reach may not DECIDE.** Excluding a live-only function wholesale
(`replace run_pool -> Result`) is a truthful claim about the BODY and a silent
one about the branches inside it — every `&&`, `!` or `x > y` written there is a
mutant excluded with nothing asked in return. Measured: five decisions were
pulled out of `execute_resolved_plan` (`src/pipeline/job.rs`) BY HAND this
session — `should_reconcile`, `plan_rejection_error`,
`resume_success_gate_applies`, `rerun_warning_applies`,
`dispatches_to_cdc_runner` — each only because the in-diff gate pointed at that
one operator and someone argued it out; `fold_failures` (`src/pipeline/run.rs`)
came out of the same pass and was a REAL gap, with no unit oracle at all. Six
extractions, six ad-hoc arguments, and nothing stopping the seventh from being
written inline tomorrow. So the rule, not the litigation: **a live-only body is
GLUE — sequencing, I/O, error context — and calls a NAMED PURE PREDICATE for
anything it decides**; the predicate is offline-testable and its mutants are
graded, the glue stays excluded for the reason its exclusion gives. The tell that
this went wrong is an operator-shaped entry in `.cargo/mutants.toml` (`delete !
in run_pool`, `replace == with != in check$`): that is a decision that should
have been a function. Enforced by `tests/offline/live_only_purity_gate.rs`, which
DERIVES the live-only set from the config's whole-function exclusions (never a
typed-in list) and holds each body at a shrink-only ceiling — lower a row the
moment you extract, and it fails downward too so the win stays banked.

Scope honesty: mutation testing measures assertion SENSITIVITY on code that
exists. It cannot see a missing behaviour (the manifest-clobber fix itself was
invisible to it — an independent-oracle harness caught that), nor a test and
code that agree on a wrong spec. Layers: matrices (coverage exists) → mutants
(assertions bite) → live suites (integration) → independent-oracle harness
(absent behaviour). One layer's green is not another layer's proof.

## A guard that only runs at the END of a loop is invisible to every PANIC test

A fault hook that kills the process cannot reach code that runs after the loop
finishes — a crashed run never gets there. So a guard placed at the end of a
runner ("not every claimed chunk completed", "no worker reported an error") is
untestable by the entire existing crash-injection vocabulary, and looks covered
because the crash tests around it are green.

Measured 2026-08-03: disabling the sequential chunk-checkpoint guard
(`pending > 0` → `pending < 0`) left 14 live tests green — 9 chunked-recovery,
1 crash-soak, 4 chaos — while the run shipped **100 of 150 rows with status
`success`**. Disabling BOTH parallel-checkpoint guards (the worker-error bail
and the completion count) left 56 green. Every one of those tests injects
`RIVET_TEST_PANIC_AT`; none of them can reach the guard.

The fixture the class needs is an error that RETURNS, not one that kills:
`maybe_error_at_index` existed for exactly this but was wired only into keyset,
so the chunked runners got the hook (`RIVET_TEST_ERROR_AT=chunk_export:N`) as
part of the fix. Process rule: **for any check that runs after a loop/join,
there must be a test whose failure is RETURNED rather than crashed** — and the
oracle is the LEDGER, not parquet on disk. The first draft of the parallel test
counted files and read "300 of 150 rows": a worker retries its chunk internally
and each attempt writes a part before the error discards its record (measured:
303 files for 299 recorded). Those are unmanifested parts of a FAILED run — the
`gc_orphans` case, not a loss. Raw artifacts under a failed prefix are not
evidence about what the run delivered.

Corollary on redundant guards: the parallel test goes RED only when BOTH guards
are off. Say that in the test body rather than presenting it as two proofs —
the same load-bearing / belt-and-suspenders distinction the `until_current`
rule already insists on. Not every runner is redundant, and the difference is
worth stating: the PLAIN parallel runner keeps no `chunk_task` ledger and no
completion count, so its single post-join bail is all that stands between a
partial export and a green `_SUCCESS` — a single-guard RED, said so in the test.

Sibling class the same fill exposed, and the reason the hook had to be wired at
all: **an exit-status oracle over a fixture that fails for a SECOND reason
grades nothing.** `governor_does_not_deadlock_when_chunks_fail` drives the exact
runner, really does fail chunks, and asserts `!status.success()` — and it stays
GREEN with the guard deleted, because its destination points under a regular
file, so the run exits non-zero on the unwritable path whether or not the guard
fires. It reads as a failure-path test and cannot distinguish rivet's guard from
`Permission denied`. Process rule: **when a test's oracle is the exit status,
name the ONE thing in the fixture that can produce it, and assert the delivered
outcome (the ledger's `files_committed`, the absence of `_SUCCESS`) rather than
the status alone** — otherwise the fixture's own breakage answers for the
product. The tell is a fault-injection test whose fixture is ALSO misconfigured
on purpose (an unwritable path, a missing table, a bad credential) to "make it
fail": that second cause is now the thing being measured. RED-proven by the
disagreement — `a_failed_chunk_must_fail_the_plain_parallel_run_not_ship_a_short_export`
goes RED against the removed guard (100 of 150 rows shipped `status: success`,
exit 0) while all 11 pre-existing tests in the same module stay green.

## A per-export feature must be wired into EVERY runner — the runner-bypass class

rivet has FOUR export runners and three of them — `chunked`, `keyset`,
`mongo_parallel` — own their own execution loop that RETURNS from `run_export`
BEFORE reaching `run_single_export`. So any gate/feature wired only into single
is SILENTLY ABSENT on the headline large-table paths, and every count/sum/oracle
still passes. Round-8 proved the class recurring: `on_schema_drift: fail`
returned exit 0 on keyset AND parallel-Mongo because the drift gate lived only
in single/chunked — two misses in one round. Building the ledger to guard it
then surfaced a BIGGER one: **value-checksum Form B is absent on all three
large-table runners** — the sink COMPUTES per-column checksums for every runner
(`track_checksum` in `sink/mod.rs::on_batch`), but only `single` harvests
`sink.column_checksums` into `summary.column_checksums`, so `finalize_manifest`
records none and `rivet validate`'s Form-B re-read is a no-op precisely on the
paths that move the most data (`read_keyset_page` drops the checksums; chunked
never reads them).

The systematization (rounds 4-8) is `docs/runner-coverage-matrix.yaml`
(`feature × {single, chunked, keyset, mongo_parallel}`, drift-guarded like the
others). Its gap/na split is the load-bearing distinction: a feature applied by
SHARED-SEAM construction (meta_columns via `ExportSink` — no runner can bypass
it) is `na` (runner-agnostic, proven once); a feature each runner must
EXPLICITLY re-apply (the drift gate, the checksum harvest, the part-name stamp)
is `gap` when unproven — that is exactly the class that bites. Process rule:
**any new per-export feature (a gate, an integrity record, a stamp, a warning)
gets a cell PER RUNNER in the runner-coverage matrix — `test` where a per-runner
test proves it, `na` only when it is shared-seam or structurally inapplicable,
never a silent omission.** Sibling ledgers strengthened the same rounds:
`durability-ordering-matrix` grew a `keyset` column + the repair-sidecar /
M8-quarantine rows; `csv-fidelity-matrix` (new) captures the text-writer class
(silent value loss vs escape-corruption) Parquet's binary path never exercises.

Two anti-patterns from the same rounds are LINT-shaped, not matrix-shaped — they
are a LOCAL code smell, so they live here as a review rule, not a cross-product
cell: (1) **config-clobber** — an unconditional `tuning.X = cfg.X` assignment of
an `Option` field that has a protective profile DEFAULT treats the field's
ABSENCE as "disable" (round-8: a bare `tuning: {profile: fast}` clobbered
Balanced's `Some(32)` memory cap to `None`, ~3× PG RSS). Guard the merge with
`is_some()` like its siblings. (2) **self-oracle** — a test (or a checksum) that
derives its `expected` from the SAME code it guards cannot catch the bug (round-7:
the CSV timestamp test recomputed `expected` with the same flawed split, and side
A of the value-checksum shares the writer's own rendering). A value-rendering
test needs an INDEPENDENT oracle: a hard-coded expected string, or a re-read
through a different reader (DuckDB).

## A diagnostic must understand EVERY read strategy — the diagnostic-bypass class

`rivet check` (preflight) resolves the column it probes from a SUBSET of the
strategy fields, so a strategy keyed off a field it doesn't read is silently
mis-analysed — the diagnostic sibling of the runner-bypass class (a feature
wired into only some of the four runners). `range_col = chunk_column.or(cursor_
column)` in all three engine `diagnose_*` paths OMITTED `chunk_by_key`, so EVERY
keyset export rendered `chunked(?, size=…)` (the `?` = the absent chunk_column),
probed the wrong/absent column for an index, and reported a false `UNSAFE` / "no
index" / "create an index on chunk_column" — even though the planner GUARANTEES
a keyset key is a unique index (`plan::build` bails otherwise). A production
config of ~66 keyset tables emitted 66 false alarms; the real problem (if any)
was lost in the noise. The fix teaches the shared `analysis.rs` seam AND each
engine's `range_col` about `chunk_by_key` (correct `keyset(key, size)` label,
index probe on the real key, keyset=sequential, sparse-warning suppressed since
keyset is immune). Process rule: **a diagnostic/preflight that resolves its
subject (the probed column, the row estimate, the strategy label) must enumerate
EVERY strategy the runner can pick — `chunk_column` (range), `chunk_by_key`
(keyset), `cursor_column` (incremental), date/dense/count — not a subset.** The
tell is a `.or()` chain over strategy fields that is shorter than the strategy
enum; a `?`/`unwrap_or("?")` placeholder surfacing in the output is the smell
that a strategy fell through. Cross-check the label against `derive_strategy`'s
arms and the planner's strategy constructors.

Sibling trap in the same file — a cost diagnostic must not EXTRAPOLATE past
physical reality. `check_oversized_chunk` (`preflight/analysis.rs`) estimated a
range chunk's scan as `density × chunk_size` where `density = rows / key_span`;
on a LOW-cardinality key (few distinct values, many rows each — a real field DB
had a 150K-row table over 100 distinct `amount` values) the density is huge but
the span is tiny, so `chunk_size ≥ span` extrapolated **151M rows/chunk on a 150K-
row table** and phantom-warned "~2167 MB, shrink chunk_size 8×". A single chunk
can NEVER scan more rows than the table HOLDS: `rows_per_chunk.clamp(1, rows)`
(the dense/keyset branch has the same bug when `chunk_size > rows`). RED-proven
both branches (`check_oversized_chunk_low_cardinality_range_key_does_not_phantom_
warn`, `..._dense_chunk_larger_than_table_...`). Process rule: **any per-chunk /
per-page cost estimate that multiplies a density or rate by a window size must
clamp the result to the total it is a fraction OF** — a fraction of the table that
comes out bigger than the table is the tell. A false "this is slow" alarm is the
same diagnostic-bypass harm as a false UNSAFE: it drowns the real problem in noise
(the field config's 66 keyset tables emitted 66 false alarms; this emitted one per
low-cardinality chunk key).

## A flag you cannot safely auto-default is often OVERLOADED — split it

When `rivet init` (or a `check --fix`) cannot safely turn a knob on by default,
the reason is frequently that the knob means two things at once, only one of them
safe. `chunk_checkpoint: true` on a keyset export meant crash-recovery AND
incremental-by-key (a clean re-run continues from the last exported key). The
second silently skips UPDATEd rows on any non–append-only table, so init left it
OFF — which is why a crashed keyset run stranded ~738K durable rows behind a
checkpoint most configs never enabled (the live tunnel-drop). The fix is NOT a
band-aid that auto-enables the overloaded flag (a `--fix` would hit the SAME
safety wall init did — neither can know append-only-ness) but to SPLIT it:
`chunk_checkpoint` is now crash-recovery only (clean re-run does a full pass,
never skipping; crash detected via the in-progress run_id), and the append-only
"continue-from-key on a clean re-run" is the new off-by-default
`keyset_incremental`. Now init defaults the SAFE half on (crash-recovery) and the
738K-row gap closes at the SOURCE — the config is born correct, no `--fix`
needed. Process rule: **before building a config-patching feature (`--fix`), ask
why `init` doesn't already birth the config right; if the answer is "that knob
isn't safe to default", the knob is overloaded — split the safe concern (which
init CAN default) from the unsafe opt-in, rather than automating the unsafe
default one layer up.** Every split of a semantic flag needs a three-way live
proof: crash-recovery still resumes, a CLEAN re-run does NOT skip (RED against
the old conflation), and the opt-in restores the old behaviour
(`keyset_checkpoint_without_incremental_rereads_on_a_clean_rerun` +
`..._second_run_captures_only_new_keys` with the opt-in + `..._crash_resume_...`).

## An orphan-GC delete path tells a crash orphan from a LIVE in-flight write via the LEDGER, never a clock

`gc_orphans` (opt-in load cleanup, `src/load/reconcile.rs`) deletes every
`.parquet` under a prefix that no `Success` manifest references. But a manifest
is written at the END of a run, so a CONCURRENT extract's committed-but-not-yet-
manifested parts look IDENTICAL to a crash orphan — both are unmanifested
`.parquet`. With no discriminator, a load fired while a `rivet run` streams into
the same prefix silently deletes its in-flight parts. The fix is a THREE-way
classification driven by a run-status SIGNAL: a part in a `Success` manifest →
KEEP; a part in a `Failed`/`Interrupted` manifest → DELETE (a run that REACHED a
manifest is terminal — no live writer); a part with NO manifest at all → the ONLY
ambiguous case, gated on whether a run is ACTIVE on the prefix. "Active" comes
from the CENTRAL run-status ledger (`StateStore::run_status`, written `running`
at every run's START via the orchestrator choke point `ledger_begin_run`,
terminal at finalize via `ledger_finish_run`) — `has_active_run_on_prefix`,
authoritative and CLOCK-FREE. A stale (hard-crash) `running` row is reconciled by
SUPERSESSION — a newer run of the same export outranks it by `started_at` — never
by an age timer.

A first cut REACHED for a wall-clock freshness window (spare parts younger than N
minutes); it was REJECTED in review as a band-aid — it can't tell a LONG extract's
OLD in-flight parts from a stale crash orphan, and it compares two clocks (the
load host's `now` vs the object store's mtime). The ledger is the right signal
because the state store already records every run's lifecycle; the bucket manifest
is a PROJECTION of it (its status written FROM the ledger), so a rivet process
over a shared state DB and a cross-boundary reader over the bucket agree. The
ledger read is the SEAM: co-located / shared-Postgres loads get a precise
`active`; a stateless or foreign-host load passes `active = true` (conservative —
spare rather than risk a live cross-host extract's parts). The state store is
already backend-pluggable (`StateConn::Sqlite | Postgres`), so a shared-Postgres
deployment makes the ledger authoritative even cross-host.

Process rule: **any GC/prune that deletes "unreferenced" artifacts under a shared
prefix must gate the truly-unknown remainder on an AUTHORITATIVE run-status signal
(the state-store lease, or a projected running-manifest) — never on a wall-clock
age, and never by blanket-deleting everything the latest completion record doesn't
yet mention (that races a concurrent writer whose record isn't written yet).**
When you catch yourself reaching for a freshness timer to tell "live" from "dead",
the real fix is a lifecycle record that SAYS which it is. RED-proven against a
mutant that ignores `active` (`gc_orphans_spares_an_unmanifested_part_while_a_run_
is_active` goes RED) + the defer-not-drop half (`..._collects_an_unmanifested_
part_when_no_run_is_active`) + the clock-free staleness
(`a_superseded_running_row_no_longer_counts_as_active`).

## `cargo package` poisons the shared target dir — a build that lies about being fresh

`cargo package` / `cargo publish` copy the crate to `target/package/<name>-<version>/`
and **build that copy** to verify it. With no `--target-dir` the verify build shares
the workspace `target/`, so the fingerprints it leaves record the crate's sources as
`target/package/<name>-<version>/src/**` — a frozen snapshot. Every later `cargo
build` in the working tree compares those SNAPSHOT mtimes, finds nothing newer,
prints `Fresh` and **silently produces a binary without your edits**. Cargo names the
mechanism itself once the snapshot is deleted: `Dirty rivet-cli: the file
`target/package/rivet-cli-0.24.0/src/types/target.rs` is missing`.

This is worse than a stale build, because every signal you use to check the build is
also lied to: `cargo build` says Finished, `cargo test` passes, and a deliberate type
error appended to `src/lib.rs` compiles clean. It cost half a session — an `apply`
fix verified green by hand kept failing in the matrix, because the matrix staged a
binary built before the fix while `cargo build` insisted there was nothing to do.

The tell: **the source file is newer than `target/debug/<bin>` and cargo still says
`Fresh`.** Confirm with `cargo build -v | grep -E "Fresh|Dirty"`; cure with `rm -rf
target/package` (not a full `cargo clean` — that discards tens of GB of dependency
build for a fault that lives in one directory).

Process rule: **any command that packages or publishes gets its own `--target-dir`,
and any hook or script that relies on cargo's answer must drop `target/package`
BEFORE it asks.** Order matters: a guard placed after the build steps cannot help,
because those steps are exactly what the staleness corrupts — a check cannot detect
what it is blind to. Wired as step 0 of `.githooks/pre-commit`, and pinned on the
release workflow's `cargo publish` so the line stays safe when copied locally.

## A test compares a golden to rivet's OUTPUT — never a fixture to itself

The oracle must be INDEPENDENT of the code under test. A test that reads a
checked-in fixture and then asks questions **of that same fixture** — does this
key exist, does this text contain that substring, does this YAML cell say `test`
— holds both sides of the comparison. It cannot fail when rivet changes; only
when someone edits the fixture. It reads like coverage and is none.

The bite: `tests/artifact_legacy_compat.rs` froze plan artifacts from 0.7.5 and
asserted their JSON contained certain fields, deliberately never deserializing
them with the current type (its doc explained why — `PlanArtifact` is
`pub(crate)`). When `verify` was added as a REQUIRED field on 2026-06-02, every
assertion kept passing while `rivet apply` rejected every `plan.json` users
already had on disk. Two months, and the fixture that existed to catch exactly
this was the thing that hid it. The same shape sat in `tests/compat_gate.rs`,
where v0.16 CDC checkpoints were read with a bare `serde_json::Value` and one key
per engine was asserted (`pos` never was) — a renamed key leaves it green while a
user's checkpoint silently re-anchors and skips every change since it was written.

Process rule: **every fixture/golden test must pass the fixture THROUGH rivet's
own code and compare the result to an expected value produced independently of
that code** — a hand-written literal, a value derived by a different library, the
seed the test itself inserted, or a re-read through a different reader. Three
corollaries, each of which cost something here:

1. **`pub(crate)` is not a reason to test the shape instead.** It is a reason to
   put the test beside the type. Both fixes this session are inline
   `#[cfg(test)]` modules next to the code that owns the type
   (`src/plan/artifact.rs::legacy_wire_compat`,
   `src/source/cdc/validate.rs::v016_checkpoint_compat`), each calling the real
   loader. Both go RED on the mutant the integration-level shape check sails
   through — verified side by side, not argued.
2. **Derive the enumerated dimension, never type it in.** A gate whose engine /
   target / variant columns are a hand-written list grades only what its author
   already knew. `tests/offline/chunking_matrix_guard.rs` parses `SourceType`
   itself; `tests/cdc_conformance_gate.rs` did not, and its four engine columns
   were tied to nothing. Be honest about the strength: adding an enum variant
   does NOT compile silently (every non-exhaustive `match` fails), so the risk is
   not "it ships unnoticed" — it is that once the compiler's list is finished,
   nothing asks for the TEST rows. That is the gap the derivation closes.
3. **A name must not promise what the body cannot check.** `..._guards_...`,
   `..._must_...`, `no_silent_float_...` on a body that only greps a fixture is
   how a reviewer's eye is spent. If a test genuinely documents rather than
   verifies (a migration example, a matrix ledger), name it `..._documents_...`
   and say so in one line.

Scope honesty: this class is invisible to mutation testing of the PRODUCT, since
the fixture-only assertion never executes the mutated code. It is found by asking
of each assertion: *what change in rivet would turn this red?* If the answer is
"none — only editing the fixture", it is this defect.

## A correct test of correct logic on a FABRICATED input proves nothing about the caller

Three distinct defects hide under the phrase "the test is fake", and conflating
them wastes the fix. Separate them by asking *where the wrong value comes from*:

1. **Fixture against itself** — the test holds both sides of the comparison, so
   only editing the fixture can turn it red (the class above).
2. **A test that routes AROUND the defect** — usually honest, and often says so
   in its own comment. It closes nothing, but it lies to nobody.
3. **A correct test of correct logic, fed an input the product never produces.**
   The assertion bites. The logic is right. The defect lives one layer up, in
   whatever HANDS that logic its input — and nothing in the suite looks there.

The third is the one to learn, because every existing safety net misses it.
`decide_export_retry` (`src/pipeline/single.rs`) refuses to retry once parts are
durable, and its unit matrix feeds `files_committed` = 0 / 2 / 5 and checks every
arm — a genuinely sensitive test. Meanwhile `run_keyset_parallel` bailed on a
worker error ABOVE its `record_part` drain, so the decider was handed `0` with
four parquet files already on disk and correctly decided "retry". Both halves
correct; the seam wrong, since 0.23.0. Measured: a transient worker error left 4
parts on disk and produced `retry 1/2` then `retry 2/2`; because keyset part
names key off the stable `run_id`, attempt N+1 normally overwrites attempt N — so
the damage only surfaces when a range's output SHRINKS between attempts (a
mid-backoff `DELETE`), where attempt 1's extra part survived unoverwritten: 751
rows / 750 distinct, one id in two files.

**Mutation testing is structurally blind here.** Mutate inside the decider and
the matrix goes red; mutate the SUPPLIER of its input and nothing does, because
no test observes that value. The same blindness applies to any check whose test
constructs its own subject: a hash test that builds the value it hashes, a guard
test that builds the manifest it guards, a resolver test that re-implements the
resolution rule in a closure.

Process rule: **for any value that crosses a layer boundary and decides
something, one test must observe it AT the boundary, produced by the real
producer — not supplied by the test.** Concretely: read it back from the
artifact the product wrote (a metrics row, a manifest, a summary), or call the
one function that computes it and assert on THAT. Then RED-prove against a
mutant in the PRODUCER, not in the consumer.

Three misses in a single session say how easy this is to get wrong, twice while
actively trying not to. A guard test hand-set `export_family` to a value no
production writer emits (green against the very fold it was written to catch); the
rewrite replaced the hand-set value with a CLOSURE re-implementing the rule
(still green against the same mutant); only extracting `ExportConfig::family()`
and calling it from the test made the mutant bite. A third — the running-marker
family — could not be closed at all, because its writer is cloud-only and no unit
test can observe what it records; that test now states in its own doc that it
pins the READ side only and names the structural fix. **A test you cannot make
RED should say so where a reader will see it, not pass quietly.**

Template: `parallel_keyset_worker_error_still_counts_the_durable_parts_postgres`
(`tests/live/live_keyset_parallel.rs`) — it injects the worker error, asserts the
fixture is not inert (parts really were written), then reads `files_committed`
from the run's own metrics row. RED against the pre-fix order (`left: Some(0)`,
`right: Some(4)`) while the pre-existing worker-error test stays GREEN against
the same mutant. When a new test and an old one disagree about a mutant, the
disagreement is the finding: the old test's blind spot has a name and a boundary.

## A coverage ledger must grade the CALL SITE, not the definition

A matrix row saying `test` means "the gate runs this". The guard that protects
it asked a weaker question — does every `def verify_*` have a ROW — and the gap
between the two was live: `blessed_path.verify_blessed_path` was registered
`test` for all four engines and had **no caller anywhere in the tree**. Dead
code behind four green cells, and every signal a reader has (the matrix, the
definition, the guard, the docstring) agreed it was covered. Nothing in the
suite asks "is this reachable from an entry point".

Process rule: **a drift guard over a coverage ledger must assert the check is
INVOKED, not merely defined** — an occurrence of `name(` that is not the `def`,
in any module the runner can reach. `every_gate_function_has_a_call_site`
(`tests/offline/release_gate_matrix_guard.rs`), RED-proven by deleting one call.
The same shape applies to any registry-of-checks: a lint rule list, a scenario
table, a hook map. Registration is a claim about behaviour; only the call site
is evidence.

Two siblings from the same build, both recurrences of rules already in this file:

1. **An unscoped count is satisfied by history.** Three separate assertions in
   ONE module counted `WHERE export_name='flow'` (37 rows on a cell that wrote
   one) and `SELECT count(*) FROM load_run` (the same 21 in all twelve load
   cells). Scope by whatever key the table actually has — a watermark on
   `max(id)` where there is no `run_id` (clock-free; comparing the host's clock
   to the database's is two clocks), the run ids from `.rivet/runs/*/summary.json`
   (which rivet writes beside the CONFIG whatever the destination is, so it has
   no local-vs-cloud asymmetry the way the destination manifest does), a PREFIX
   match on `load_id` (the stored id is `<--run-id>:<export>` — one load over N
   exports records N rows). A count twelve cells agree on is measuring none of
   them.

2. **One oracle across backends, or two that disagree about the bug.** The local
   readback was fixed to count only manifest-DECLARED parts (a crash leaves
   orphans no manifest names); the cloud readback kept counting everything under
   the prefix, so every resume cell on s3/gcs read 2000 rows from a 1000-row
   table. The fix is not to patch the second one — it is to PULL the cloud prefix
   whole and run the identical local oracle over it. A store-specific "what was
   delivered" is a second definition, and it drifts on the first fix.

And a fixture rule the same run paid for: **`DROP TABLE` before a CDC disable
orphans the change table**, so a guard joining `cdc.change_tables` to
`sys.tables` stops seeing it, the disable is skipped, and the next
`sp_cdc_enable_table` fails with 22926 "capture instance already exists". It
alternates pass/fail and reads as a race; it is order-dependent state the guard
went blind to. Disable first, guard on `capture_instance` (which survives the
drop), and wait on `fn_cdc_get_min_lsn` rather than the enable call's exit code —
rivet's preflight reads that function, so "enabled" is not "ready".
