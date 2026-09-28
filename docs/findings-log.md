# Findings log

Every defect a quality layer found, with the layer that caught it and the layer that
SHOULD have caught it earlier. A row whose two layers differ is an escape: it names the
gap the next hunt looks at first. Newest first. "Guard" is what now fails if the defect
comes back.

Layers, cheapest to dearest: unit (lib test) · offline (integration suite) · mutants (in-diff)
· live (stand test) · gate (release oracle) · field (a user).

| Date | Finding | Class | Caught by | Should have been | Guard now |
|---|---|---|---|---|---|
| 2026-09-27 | A run crashed on a fast-clock host outranked its successor; gc kept deferring cleanup until that clock caught up | run ordering by a writer's clock | gate (block C) | live (shared-state cells) | `live_state_clock::a_crashed_run_from_a_fast_clock_host_is_superseded_by_the_next_run` (#323) |
| 2026-09-27 | The state lease (run + load) did not hold behind a transaction-mode pooler: two runs of one export both proceeded | session primitive behind a pooler | gate v2 design (block C) | live (the pooler has been on the stand since the source-side pooler tests) | `live_state_pooler::*` (#321) |
| 2026-09-27 | The pooler test passed with no lease at all: the old binary refused a newer state DB and "not both succeeded" held | exit-status oracle over a second failure cause | gate (a test that should have been red was green) | review of the oracle | the test requires the lease-refusal message (#321) |
| 2026-09-27 | Perf read a 22x source-harm regression on multiplex CDC that did not exist: rivet's self-reported harm changed meaning between releases | self-oracle across versions | gate (perf) | lint | PostgreSQL harm read from `pg_stat_database` by the harness; `self_oracle_lint` for live tests |
| 2026-09-27 | The mutation gate took 2h45m because the binary re-declared every library module: each mutant compiled and tested the crate twice | build topology | measurement | architecture review | `src/main.rs` links the library (#322) |
| 2026-09-27 | A one-shard mutation run read "0 of 1 shards reported" | CI glue | CI (first one-shard diff) | the shard PR's own review | aggregator finds `rc` wherever it lands (#322) |
| 2026-09-27 | The purity gate read a qualified exclusion (`<impl Drop for T>::drop`) by bare name and graded every `fn drop` | lint precision | lint (a false demand) | the lint's own tests | `self_type` + `impl_ranges`, RED-proven (#321) |
| 2026-09-27 | 26 upgrade/perf cells could not start: the previous binary's path was relative and cells run in temp dirs | harness | gate | gate self-test | absolute path in `regression.prev_binary` |
| 2026-09-27 | Two real ClickHouse failures read as seven: nextest ran without `--no-fail-fast` | harness | gate | gate self-test | `--no-fail-fast` on every gate nextest call |
| 2026-09-27 | Known-red entries never matched: scenario cells wrote FAIL/PASS around `Ledger.failed`/`passed` | harness bypass | gate | gate self-test | self-test asserts both helpers reach the registry |
| 2026-09-27 | A full VM disk crashed state PG, the SQL Server AG replica and Oracle mid-gate; 204 failures measured the stand | stand hygiene | gate (by failing everywhere) | a disk check before the gate | VM-disk watch during the gate; 1017 orphan volumes pruned |
| 2026-09-27 | 8 in-diff mutants survived in the cursor-scope change; 6 in the error-code table | missing assertions | mutants | unit | tests added, each RED-proven (#316, #317) |
| 2026-09-27 | An exclusive live test (`a_bounded_run_emits_a_distinct_barrier_per_cycle`) had never run in the gate: it self-skipped every time | vacuous skip | gate (self-skip is a failure) | gate design | the exclusive pass; OPEN: it failed once inside the gate and passes alone |
