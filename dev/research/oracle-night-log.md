# Oracle source — night log (2026-09-25 → 26)

Running log for the morning report. Branch `feat/oracle-source`.

## Phase 1 — batch (full / incremental / keyset) — commit 3f2d4ebf

Done:
- `SourceType::Oracle` behind the default-on `oracle` feature; driver `oracledb =26.0.0-beta.4`.
- Own Arrow builder; session pin (UTC + ISO NLS masks); server-side re-projection
  (TSTZ/TSLTZ → `SYS_EXTRACT_UTC`, JSON → `JSON_SERIALIZE … CLOB`, ROWID → text);
  intervals as ISO 8601 from the driver's signed fields; bare NUMBER → exact text + warning.
- Preflight (`check`/`plan`), `doctor`, type-report, chunk/keyset introspection.
- Source-harm counters (lock waits + ms, disk sorts, physical reads, consistent gets) and
  governor pressure (`redo log space requests`).
- Stand: `oracle` compose service (gvenzl/oracle-free:23-slim-faststart), classic + garbage
  seeds ported from MySQL table-for-table, `make seed-oracle`.
- Live tests (tests/live/live_oracle.rs): 7, all RED-proven against their mutant
  (7 mutants: TSTZ re-projection, NLS mask, decimal scale, harm name, keyset `>`,
  empty-LOB flag, prefetch).

Driver defects found (oracledb 26.0.0-beta.4) and how rivet routes around them:
| defect | symptom | rivet workaround | test |
|---|---|---|---|
| TSTZ with a region name | panic `todo!()` | re-project `SYS_EXTRACT_UTC` | a_region_named_time_zone… |
| `prefetch_rows ≥ 3` + a LOB in a wide row | `unknown TTC message type` / panic in NUMBER decode | `prefetch_rows(1)`, large fetch array | every_seeded_oracle_table… |
| zero-length LOB | arrives as NULL | server-side empty flag per LOB column | full_export… (EMPTY_CLOB/BLOB) |
| `Error` | only `Debug` | `Ora` adapter | — |
| `TO_CHAR(INTERVAL DAY(0) …)` (server, not driver) | ORA-01877 | decode intervals from driver fields | every_seeded… (RIVET_TYPE_MATRIX) |

Difficulties:
- The official Oracle image downloads at ~0.6 MB/s (network, not the registry) — using
  gvenzl slim for batch; the official image (for CDC ARCHIVELOG) is still pulling.
- The driver's protocol defects only showed on the realistic seeded schema, not on the
  hand-written type matrix — the seed port paid for itself on its first run.

## Phase 1 bughunt (5 axes + roast) — 31 confirmed findings

Commit 9514f57f (phase 2a) and c01038d4 (phase 2b):
| # | finding | fix | test (RED-proven) |
|---|---|---|---|
| 24 | keyset on wide ORDERS: TTC desync at page 3 | statement cache off (re-execute of cached stmt was the trigger) | keyset_over_a_wide_table_survives_many_page_reexecutions |
| 29 | NUMBER(19) PK refused by keyset AND range | keyset accepts any scale-0 NUMBER; cursor reads Decimal128 exactly | an_init_generated_config_keysets_a_number_19_key_past_i64 (ids straddle i64::MAX) |
| — | LISTAGG caps at 4000 bytes (wide tables) | catalog reads fetch rows (`query_rows`/`query_list`) | — |
| 0 | INTERVAL YEAR(9) overflowed i32 (panic/wrap) | render from fields | edge_oracle_types_export_losslessly + unit |
| 1 | BC dates one year early | Oracle has no year 0 | edge… (DuckDB `(BC)` rendering as oracle) + unit pinned to DuckDB epoch |
| 2,3 | NUMBER(3,5), NUMBER(5,-2) crash Parquet | widen losslessly | edge… |
| 4,5 | XMLTYPE dead arm; VECTOR false remediation | project both to text; messages no longer suggest failing fixes | edge… |
| 6 | TS(9)/INTERVAL DS(9) "exact" | Lossy + warning | unit |
| 7,8 | NLS_SORT/NLS_COMP not pinned → keyset/cursor skip rows | pinned BINARY | a_linguistic_session_default_does_not_lose_keyset_rows (logon trigger) |
| 9 | TS(9) keyset loops forever | TS(7..9) not a keyset key + generic no-progress guard | keyset_on_a_timestamp_9_key_fails_loudly_instead_of_looping (both guards off → RED) |
| 10 | incremental TS(9) re-exports the max-µs rows each run | NOT fixed: duplicates, never loss (at-least-once); the column reports Lossy | — |

Difficulty: the old type-matrix test rendered BC years through the same chrono
convention as the product, so it agreed with the bug — a self-oracle; fixed the
renderer and added a DuckDB-rendered check.

Commits de29317b (2c), 5e9abab0 + e4a8e074 (two parallel agents, cherry-picked), ae33d94b (2d):
| # | finding | fix | test (RED-proven) |
|---|---|---|---|
| 13, roast 2 | four `owner.table` parsers disagreed; lower-case `table:` recorded no PK | one `sql::oracle_catalog_preds` | a_lowercase_table_shortcut_records_its_primary_key |
| roast 3, 17 | lower-case key column: check "Looks good", run ORA-00904 | preflight probes every strategy column, names Oracle's case rule | check_refuses_a_key_column_in_the_wrong_case |
| roast 1 | query_scalar read raw → driver panic on region TSTZ (chunk_by_days) | every read goes through the projection seam | chunk_by_days_over_a_region_named_tstz_reads_every_row |
| 14, 15 | unaliased ROWID / unreferenceable names | refused with the alias fix | an_unaliased_rowid_is_refused_with_the_alias_fix |
| 11,12,21,25-28 | `) AS _rivet*` in shared builders: parallel keyset, reconcile, chunk_dense, range over `query:` all failed | every wrap via `sql::derived` (agent) | 4 live tests (agent, each RED) |
| 16 | trailing `;` / `-- comment` | `sql::wrappable_query` for every engine (agent) | live + unit |
| 30 | check "Looks good" after plan build failed | plan-build failure is a Rejected finding (agent) | offline unit |
| 22 | killed session not retried | `is_oracle_lost_session` → reconnect (agent) | live kill test + 2 unit |
| 23 | no doctor note without catalog privileges | exact doctor note (agent) | least-privilege live + control |
| 18 | statement_timeout only between rows (274 s for a 2 s budget) | driver call timeout; server stops the query (0 ACTIVE sessions after) | a_statement_timeout_stops_a_long_query_on_the_server |
| — (found fixing 18) | describe `WHERE 1 = 0` wrap EXECUTED aggregate queries in full before every export | parse-only describe | same test (RED against the old describe) |
| 20 | LOB rows: 1.09 GB RSS with a 16 MB budget | 16-row probe/fetch for LOB projections, cap below 500 | wide_clob_rows_stay_within_the_memory_budget (164 MB) |
| 19 | MIN+MAX in one statement = full index scan | two boundary seeks | unit |

Difficulties:
- A cherry-pick conflict on the shared test file (both agents appended); resolved by taking HEAD + each agent's appended hunk.
- `tests/.live-tmp` in the worktree was a real directory, so the DuckDB container read the main checkout's copy → 0 rows; symlinked.
- The first RED of the timeout test MISSED: warm cache made the fixture query shorter than the ceiling. Fixture made 10x heavier.

## Round 2 — new axes (lifecycle, CLI surfaces, hostile data, concurrency, message truth) — 26 confirmed

Axis yield: lifecycle 1/1, cli 8/8, hostile-data 5/5, consistency 2/3, truth 10/10.
Commits 0b22b8db, db7cc1d8 (mine), 4210e45f (pipeline agent), 92ac0ee1 + d7da0a7e (init agent), 25621dc3 (OraKind), 104107b5.

| # | finding | fix | proof |
|---|---|---|---|
| R2-0 | **data loss, engine-agnostic**: a run failed by `on_schema_drift: fail` still advanced the incremental cursor | `cursor_may_advance(status, manifest_gap)` | PG live (RED) + unit |
| R2-14 | **data loss, engine-agnostic**: `chunk_dense` under concurrent deletes skips rows even on a unique key | NOT fixed at the root: docs corrected, sparse hints recommend keyset first, run-start WARN | unit (exact text) |
| R2-13 | a source column named `_rivet_row_hash` + meta columns → duplicate Parquet field | refused before writing | unit |
| R2-1 | init on a mixed-case table read its UPPER-case twin (validate + reconcile passed) | Oracle names must equal their upper fold for `table:` | live twin-table (RED) |
| R2-2/16 | time_window always failed (ORA-01861) | ANSI `TIMESTAMP '…'` | live DATE + TIMESTAMP (RED) |
| R2-3 | INTEGER mapped to decimal(38,18) override | `number` | live |
| R2-4/20 | never-analyzed table read as 0 rows → mode: full | capped count | live |
| R2-5/19 | init --mode cdc wrote a refused scaffold | refused in init, loader's words | offline |
| R2-6/7 | synonyms invisible; init CLI names exact-case | follow synonym; fold like Oracle | live |
| R2-9/10 | LOB flag alias collision; 1000-col + LOB | unique alias; flags dropped past the cap with WARN | live (RED) |
| R2-11/17 | key-column hint wrong for INVISIBLE / quoted lower-case | catalog lookup names the real spelling / INVISIBLE | live (RED) |
| R2-12/21 | VARRAY/ANYDATA raw ORA-00932, check "Looks good" | refused by name; failed type report blocks, `--strict` fails | live (RED) |
| R2-15 | ORA-02391 session caps permanent | capacity retry | unit |
| R2-18 | ORA-01017 no hint; ORA-12514 "check the tunnel" | auth / new "unknown service" | unit |
| R2-22 | docs listed four sources | docs/reference/oracle.md + compatibility/config/CLI | — |
| R2-23 | ca_file refusal said "system trust store" | the driver trusts only its compiled-in public CA bundle (webpki-roots) — measured in the driver source | unit (exact text) |
| R2-24/25 | bare-NUMBER example; rerun warning over a failed manifest | per-column; success-with-parts only | unit |

Roast round 2 — acted on: N2 (LOB probe by fetched type: JSON/XML/VECTOR bypassed it, 829 MB vs 163 MB),
N4 (preflight `WHERE 1=0` still executed aggregates), N5 (check probed the wrong relation:
range 1..150000 for a query spanning 1000100001..), N10 (call timeout reset on every exit).
Deferred (Worth): N1 shared memory-cap helper across engines (MySQL/MSSQL LOB widths inferred,
not measured), N3 native-label strings, N6 init engine as &str, N7 NUMBER≤18 rule ×5,
N8 typed Oracle error instead of substring matching, N9 Projection layout, N11 doctor note tri-state.

Also this round:
- The build WITHOUT the oracle feature did not compile (agent code); fixed + CI clippy step added.
- Test-harness finds: the kill test killed an idle probe session once describe stopped
  emitting `1 = 0`; fixed by waiting for rows_processed > 0. Oracle Free (2 CPUs) needs
  a nextest test-group of 4 or the whole-schema export exceeds its ceiling.
- Full Oracle live suite: 41/41 under full parallel load.
- `rivet init` schema-wide: 35 s for 21 objects idle (ALL_* catalog views are slow) — not fixed.

## Phase 3 (CDC) — research done, not implemented
dev/research/oracle-cdc-probes.md (agent, on a separate ARCHIVELOG spike container):
mining works from the PDB service with 4 grants; resume across log switches proven incl.
a straddling transaction (restart_scn); a log missing mid-range is SILENT in LogMiner —
a pre-mining contiguity check + V$LOGMNR_LOGS status is mandatory; ROLLBACK TO SAVEPOINT
leaves a compensating ROLLBACK=1 row a naive framer would mis-handle.

## Round 3 — security/redaction, crash-resume, multi-export, formats, hostile config — 20 confirmed

Axis yield: security 3/3, resume 1/1, multi 2/2, formats 4/4, config 10/10. Commit e9842ae5 (+ generic agent, pending).

| # | finding | fix | proof |
|---|---|---|---|
| R3-0/1/10/11/12 | **security, one root cause**: `parse_oracle_url` split userinfo on the LAST '@' of the whole URL; the redactor and TLS gate cut the authority at the first '/'. Raw '/' in a password → plaintext password in plan.json, part of it in connect errors + summary.json, and `u:k@localhost/q@remote` passed the TLS gate as loopback while the driver dialled the remote host in plaintext | one reading: authority ends at first '/', '?', '#'; raw delimiters refused; parse before the gate | unit (both repro URLs refused; encoded form parses) |
| R3-3 | parallel keyset on a DATE key failed every bounded range (ORA-01830: N'…' vs the pinned NLS mask) | VARCHAR2 literal | live (RED) |
| R3-6 | NUMBER(p, s>38) → Decimal256 crash | exact text | unit |
| R3-13/14/15 | TLS hints: "system trust store"; handshake hint on the policy refusal; `ca_file` advice Oracle refuses; bare EOF on a plain listener | reworded / skipped / marked / hint | unit (exact text) |
| R3-16/17/19 | IPv6 without port; structured `::1`; scheme case; templates redacted into `REDACTED@host` | bracket handling; templates not credential-shaped | unit |
| R3-4/5/7/8/9/18 | generic (plan waves, validate over failed run, CSV value check, CSV empty binary vs NULL, CSV manifest compression, PG/MySQL `prefer` hint) | delegated to an agent | pending |

Roast 3 caught three regressions my round-2 fixes introduced, all reproduced and fixed with tests:
A1 meta-column collision guard was exact-case (a `_RIVET_ROW_HASH` source column took over the hash in DuckDB);
A2 silencing the rerun warning over a FAILED manifest hid durable parts (200 rows / 100 distinct);
A3 N5's "whole table only" also dropped the index probe and row estimate for init's own column-list scaffolds.
Lesson: a fix narrowed to the reported case (wording → trigger; values → every probe) regresses the neighbour.
