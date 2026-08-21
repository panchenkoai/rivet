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
   the PR changed, `--lib --bins` cycle. A NEW missed mutant in your own diff
   fails the check. Cheapest and fairest: everyone pays only for their own code.

   Since 2026-08-21 the in-diff mutants are **prioritised before they are
   budgeted** (`.github/scripts/mutants_classify.py`, wired into the
   `mutants-in-diff` job). `cargo llvm-cov --lib --bins` measures which
   functions the offline suite actually EXECUTES, and each mutant lands in one
   of two classes:

   - **graded** — its line is inside an executed function. A test ran that code
     and did not notice the change: an assertion gap, and the gate's red.
   - **reported** — its line is inside a function measured at ZERO executions.
     No offline assertion can kill it; it is a triage question
     (`.cargo/mutants.toml` with a live-oracle proof, or a unit oracle that
     moves the function into the graded class).

   This is what makes the gate useful on a big diff: the budget guard now tests
   the GRADED subset, so a foundational PR that used to be graded by nothing
   gets its offline-reachable mutants graded. Two properties keep the split
   from becoming an excuse — everything the measurement does not KNOW (no
   report, an unmentioned file, an unparseable name) stays in the graded class,
   and whenever the budget stretches to it the reported class is RUN as an
   audit: if the offline suite catches one of them, the classification was
   wrong and `Mutants (coverage verdict)` fails.
2. **Nightly (devbox self-hosted runner).** Full `--lib` runs over Tier 0-2 in
   rotation (~500-1000 mutants/night). Result diffed against the committed
   baseline (`docs/mutants-baseline.txt`): any missed mutant NOT in the
   baseline fails the job. The baseline only shrinks (gap-ratchet discipline).
3. **Weekly (devbox).** One Tier-3 module against its narrow live filter.

## Why mutants survive — the degenerate-fixture rule

Survivors come from two causes, and the second is the one worth naming because
the code LOOKS covered. Of 64 closed on 2026-08-02:

- **62 had no test at all** — `parse_time_str_to_micros`, the `Value::Time` arm
  of `RivetValue::from_mysql`, `pg_interval_to_iso8601`, `pg_type_to_rivet`.
  Pure functions reachable only through a live export, so the `--lib` cycle
  never touched them. `src/source/postgres/arrow_convert.rs` — 1059 lines — had
  no `#[cfg(test)]` module whatsoever.
- **2 had EIGHT tests that could not SEE the mutation**, because the fixture
  sat exactly where the mutated operators AGREE:

`rescale_i128` (mssql decimals) had EIGHT unit tests and still lost both of its
scale-arithmetic mutants — every test used `from_scale = 0` or equal scales, and
at zero `to_scale - from_scale` and `to_scale + from_scale` compute the same
factor. The suite could not distinguish the operators it existed to protect.

So: **choose values such that no two operators produce the same result.**

| component            | degenerate fixture      | working fixture | why                                            |
| -------------------- | ----------------------- | --------------- | ---------------------------------------------- |
| `h * 3600`           | `h = 0`                 | `h = 2`         | at 0 every operator yields 0                   |
| `m * 60`             | `m = 0` or `m = 1`      | `m = 4`         | 4*60 = 240 vs 4+60 = 64 vs 4/60 = 0            |
| `6 - us_digits`      | a 6-digit fraction      | 1 digit         | at 6 the exponent is 0 and `-` == `+`          |
| `months / 12`        | `months = 12`           | `months = 25`   | 25/12 = 2, 25%12 = 1, 25*12 = 300 — all differ |
| `to_scale - from`    | `from_scale = 0`        | 1 -> 3 and 3 -> 1 | at 0 the difference equals the sum            |

The same shape appears without arithmetic. A `match` arm deleted from a type map
drops that type to the `_` fallback — a SILENT schema change — so each arm needs
its own row in a table test: remove one, exactly one row fails and names the
type. Two traps inside that:

- **arms that produce the same variant.** `TIMESTAMP` and `TIMESTAMPTZ` both map
  to `Timestamp` and differ only in the timezone field; asserting the variant
  would let the arms be swapped, losing the UTC semantics. Assert the FIELD.
- **arms whose value is a diagnostic.** Bare `NUMERIC` is `Unsupported` on
  purpose (the wire protocol carries no atttypmod), so the oracle is the REASON
  text — it must still name the column-override escape, or the operator is told
  "unsupported" with nowhere to go.

### One pure function, one test

Do not write a test per mutant. A single well-chosen fixture kills a whole
function's arithmetic, measured four times in a row on 2026-08-02:
`parse_time_str_to_micros` 13/13, `RivetValue::from_mysql` 10/10,
`pg_interval_to_iso8601` 11/11, `pg_type_to_rivet` 12/12 — zero survivors each.

### Count kills by MEASURING, never by reasoning

Apply the mutant, run the test, watch it fail; only then delete the baseline
line. Reading a test cannot tell you whether it bites — twice on 2026-08-02 a
test that looked exhaustive did not, and the second one had been WRITTEN to close
that exact mutant.

### Verify "live-guarded" instead of believing it

The baseline explains its adapter entries as caught by the live suites rather
than the `--lib` cycle, and nothing tested that claim, because mutation runs use
`--lib` only. Test it per group with one representative: inverting the MySQL
boolean coercion in `build_array` (`*v != 0` -> `*v == 0`) DOES fail the live
subset (`live_init::init_mysql_schema_wide_discovers_seeded_table`). For that
class the claim holds — as a measurement now, not an assurance.

## The harness's own thermometer (2026-08-21)

Every loop above measures the PRODUCT. Nothing measured the harness, so a guard
could rot for months with every signal a reader has still reading green — three
did (see `tests/offline/nonvacuity.rs`). The `harness-metrics` job in ci.yml now
emits one JSON per run (`.github/scripts/harness_metrics.py`, uploaded as the
`harness-metrics` artifact, one-line summary in the job log):

- **mutants** — in scope, excluded by `.cargo/mutants.toml`, graded, and the
  classifier's offline-reachable / live-only split, plus caught / missed.
- **guards** — convention-cop guards (a `#[test]` file grading a checked-in
  subject by NAME), how many prove that subject is non-empty, how many tests
  are named `..._documents_...` (documentation, not verification), how many
  files declare a blind spot in prose.
- **tests** — DECLARED `#[test]` counts for the offline suite and the lib (the
  job runs no cargo; it counts attributes, so the number differs from a
  runner's tally by whatever is `cfg`-gated out).

It is a THERMOMETER: no threshold, no `needs:` from any job, `continue-on-error`
on top of `if: always()`. A metric with teeth becomes a number people manage
(pad the cop count; rename a test `_documents_` to duck a red). An unknown count
is published as `null`, never `0` — "the mutation job never ran" and "nothing
was missed" must not draw the same line. `tests/offline/harness_metrics_guard.rs`
grades the shaping from a fixture of counts and keeps the job non-blocking.

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

## Disk hygiene (learned the expensive way)

Each `-j N` run keeps N private tree copies with their own `target/` — ~10 GB
each with default debuginfo, and `target/incremental` GROWS over hundreds of
mutants. A killed run leaks its copies (a killed tier run left a 26 GB orphan
in `$TMPDIR/cargo-mutants-rivet-*.tmp`). Rules for every runner:

- `export CARGO_PROFILE_DEV_DEBUG=0` — mutants never need debuginfo; halves
  the build dirs and speeds the link.
- Clean `$TMPDIR/cargo-mutants-rivet-*.tmp` before AND after (nightly job
  step, not trust in graceful exit).
- `mutants.out/` is gitignored; the committed artifact is only the triaged
  baseline list.
- Budget check before launch: N jobs × ~5 GB (debug=0) + headroom; refuse on
  low disk rather than fill it.

## Pilot facts (2026-07, devbox M2 Max)

- `--lib` cycle: build 13-27s + test 3-5s per mutant; -j2 ≈ 12-18s wall each.
- Orchestration files are offline-blind by construction: `replace run_keyset
  -> Ok(())` survives the whole lib suite — only live tests guard those paths.
  This is WHY Tier 3 exists and why its cycle must be live.
- The manifest ledger itself had 5 real gaps (checksums/cursor-range silently
  droppable, part-id arithmetic) — closed same-day with RED-proven unit tests.
