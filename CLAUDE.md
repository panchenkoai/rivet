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
by construction — this class is text-decode-only. Regression:
`pg_cdc_non_iso_datestyle_and_escape_bytea_match_batch`.

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

3. **A sleep (or any workaround) in a test that compensates for PRODUCT
   behaviour is a product bug report, not a test fix.** The `sleep(1100ms)`
   authors knew rivet stamped filenames at second granularity — the comment
   said so — and routed the test around a real data-loss bug instead of
   filing it. 29 such sleeps also INVERTED over time: after the `%3f` fix
   they made the tests permanently unable to catch a regression back.
   When a test needs artificial separation (time, ordering, uniqueness) that
   the product claims to provide, stop and check the product first.

Scope honesty: mutation testing measures assertion SENSITIVITY on code that
exists. It cannot see a missing behaviour (the manifest-clobber fix itself was
invisible to it — an independent-oracle harness caught that), nor a test and
code that agree on a wrong spec. Layers: matrices (coverage exists) → mutants
(assertions bite) → live suites (integration) → independent-oracle harness
(absent behaviour). One layer's green is not another layer's proof.
