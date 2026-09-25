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
