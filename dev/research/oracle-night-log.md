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
