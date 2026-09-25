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
