# Mutation-testing plan — proving the tests can go RED

The coverage matrices certify that a test EXISTS for every claimed behaviour;
the drift-guard certifies coverage never silently regresses. Neither can
certify that a test's assertions are ADEQUATE — the 2026-07 audit found 60+
green tests that could never fail against the exact bug they guard (stale
sleeps, self-oracles, wrong artifacts). The missing third factor of trust is
measured empirically: mutate the product, and the suite must go RED.

    trust = coverage-exists (guard) × assertions-adequate (mutants) × runs-in-CI (audit)

Tool: `cargo-mutants` (>= 27). A "missed" mutant = a code change no test
notices — either a test gap, an accepted non-oracle (operator UX), or an
equivalent mutant. **Every missed mutant gets exactly one of those three
verdicts**; an untriaged baseline is a landfill, not a ledger.

## Tiers (risk × oracle × cycle cost)

| Tier | Surface | Files | Test cycle | Cadence |
|------|---------|-------|-----------|---------|
| 0 | Manifest/ledger chain | `manifest.rs`, `pipeline/manifest_writer.rs`, `pipeline/manifest_reconcile.rs`, `pipeline/finalize.rs`, `pipeline/single.rs`, `pipeline/keyset.rs`, `pipeline/resume_decisions.rs`, `source/cdc/sink.rs` | `--lib` (~20-30s/mutant) | pilot done; nightly |
| 1 | Value conversion (silent cell corruption) | `source/{postgres,mysql,mssql}/arrow_convert.rs`, `source/cdc/value.rs`, `types/target.rs`, `types/decimal.rs` | `--lib` | nightly rotation |
| 2 | State / checkpoint / integrity | `state.rs`, `pipeline/chunked/resume_m8.rs`, `pipeline/validate_manifest.rs`, `source/value_checksum.rs`, `source/{postgres,mysql,mssql}/cdc.rs` | `--lib` | nightly rotation |
| 3 | Orchestration (offline-blind — pilot proved lib tests cannot see it) | `pipeline/single.rs`, `pipeline/keyset.rs`, `pipeline/chunked/exec.rs`, `pipeline/cdc_job.rs`, `pipeline/mongo_parallel.rs` | live (`--test live_suite -- --ignored <narrow filter>`, minutes/mutant) | weekly, one module per run, devbox |
| 4 | Destination commit protocol | `destination/local.rs` (+ cloud via minio/fake-gcs) | `--lib` + live | weekly rotation |

Narrow live filters for Tier 3 (mutate X → run only its guards):
`single.rs` → `live_resume live_crash_recovery`; `keyset.rs` → `live_keyset`;
`cdc_job.rs`/`sink.rs` → the CDC suites; `chunked/exec.rs` → `live_chunked_recovery`.

## Three enforcement loops

1. **PR gate — `cargo mutants --in-diff` (minutes).** Mutates only the lines
   the PR changed, `--lib` cycle. A NEW missed mutant in your own diff fails
   the check. Cheapest and fairest: everyone pays only for their own code.
2. **Nightly (devbox self-hosted runner).** Full `--lib` runs over Tier 0-2 in
   rotation (~500-1000 mutants/night). Result diffed against the committed
   baseline (`docs/mutants-baseline.txt`): any missed mutant NOT in the
   baseline fails the job. The baseline only shrinks (gap-ratchet discipline).
3. **Weekly (devbox).** One Tier-3 module against its narrow live filter.

## Triage verdicts

- **add-test** — write the unit test that kills it (e.g. the pilot's
  `set_column_checksums`/`set_cursor_range`/part-id-max+1 finds, closed in
  `manifest_writer.rs` tests). The killing test must itself be RED-proven:
  apply the mutant, watch the new test fail, revert.
- **accept** — real behaviour but not a data oracle (operator-UX stderr hints,
  log lines). Excluded in `.cargo/mutants.toml` with a reason comment.
- **equivalent** — semantically identical mutation (e.g. the 64*1024 stream
  buffer size in `compute_part_checksums`: any chunking yields the same
  digest). Excluded with a reason.

## Pilot facts (2026-07, devbox M2 Max)

- `--lib` cycle: build 13-27s + test 3-5s per mutant; -j2 ≈ 12-18s wall each.
- Orchestration files are offline-blind by construction: `replace run_keyset
  -> Ok(())` survives the whole lib suite — only live tests guard those paths.
  This is WHY Tier 3 exists and why its cycle must be live.
- The manifest ledger itself had 5 real gaps (checksums/cursor-range silently
  droppable, part-id arithmetic) — closed same-day with RED-proven unit tests.
