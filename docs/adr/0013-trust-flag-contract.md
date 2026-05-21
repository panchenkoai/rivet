# ADR-0013: Trust Flag Contract

**Status**: Proposed
**Date**: 2026-05-21
**Context**: ADR-0012 introduced cloud manifest invariants (M1–M9). Implementing
M5 (manifest-aware verification), M6 (legacy fallback), M8 (resume decision
matrix), M9 (quarantine), and the future encryption-aware verify path could
each plausibly grow its own CLI flag (`--validate-manifest`, `--validate-deep`,
`--verify`, `--decrypt`, `--check-success`, …).  Without an explicit contract
the surface area drifts; six months from now operators face a flag soup that
contradicts the project's existing "predictable, minimal CLI" stance.

This ADR locks the trust-flag surface for `rivet run` at exactly three flags
(`--validate`, `--reconcile`, `--resume`) and the safety-override flag
`--force`, and pins the rule that ADR-0012 work and beyond extends *semantics*,
never adds new flags.

---

## Goals

1. Operators have **one** mental model for "how do I ask Rivet to prove the run
   was correct?" — and that model fits in a handful of words.
2. New trust invariants (M5–M9 today, encryption later) are absorbed under the
   existing flags transparently, with the operator's existing CI / Airflow
   wiring continuing to work unchanged.
3. The cheapest useful check is reachable without a source query
   (`--validate`); the full audit is reachable with one flag (`--reconcile`).
4. Deprecation pressure on the CLI shape is explicit, not accidental.

## Non-goals

1. A unified `rivet verify` subcommand.  Rejected — the verdict is surfaced
   via the existing run report (`.rivet/runs/<run_id>/summary.{md,json}`)
   and the existing flags.  See ADR-0012 §"Non-goals" item 3.
2. Per-invariant flags (`--check-m5`, `--validate-manifest`, etc.).  Same
   rejection: the operator should not have to know which ADR-0012 letter
   their check maps to.
3. Renaming `--validate` to `--check`.  See "Naming" below.

---

## The contract

`rivet run` exposes three mutually composable trust flags and one
safety-override flag:

| Flag | What it asks | Source query? | Implies |
|---|---|---|---|
| `--validate` | "Is the output internally consistent?" | No | — |
| `--reconcile` | "Does the output match the source right now?" | Yes | `--validate` |
| `--resume` | "Pick up where the prior run left off." | Sometimes | — |
| `--force` | "Override a refusal that would otherwise abort the run." | — | — |

Trust flags are composable: `--validate --reconcile` is the same as just
`--reconcile`; `--resume --reconcile` runs resume and then reconciles.

`--force` is a category apart — it overrides safety gates, not check
semantics.  Calling it a trust flag would be a misnomer; it's the inverse.

---

## Semantics that grow under each flag

ADR-0012 invariants land under existing flags as follows.  The flag itself
does not change between versions — only what it does internally.

### `--validate`

| Version | Behaviour |
|---|---|
| 0.6.x (current) | Per-file row count check (parquet rows / CSV lines minus header) |
| 0.7.0 | Above, **plus** ADR-0012 M5: read `manifest.json` from the destination, verify every listed part exists at the recorded `size_bytes`, verify `_SUCCESS` body matches the manifest fingerprint. |
| 0.7.0 (M6) | When the destination prefix has no manifest (legacy run), the new checks degrade to the 0.6.x file-row check and the report carries `legacy_run: true` so the reduction is explicit, not silent. |
| 0.7.x (future) | Optional `--validate --deep` re-fingerprints every part (heavy; gated). |
| 0.7.2+ (encryption track) | When parts are encrypted, metadata-only verify (no key needed) is the default; `--validate --identity ./key.txt` adds the decrypting verify. |

The flag stays `--validate`.  No `--validate-manifest`, no `--check-success`,
no `--verify-output`.

### `--reconcile`

| Version | Behaviour |
|---|---|
| 0.6.x (current) | `SELECT COUNT(*) FROM (<base_query>)` and compare to exported rows. |
| 0.7.0 | Above, **plus** the full `--validate` chain (M5/M6 included).  In effect, `--reconcile` becomes "everything `--validate` does + source comparison".  No new flag is needed for "full audit" — `--reconcile` already is that. |
| 0.7.x (future) | Source schema fingerprint compared to manifest's `schema_fingerprint` so silent type drift surfaces in the verdict. |

The implication direction is fixed: `--reconcile` implies `--validate`,
never the other way around.  Operators who only want the cheap check use
`--validate`; operators who want the full audit use `--reconcile` and
get everything for free.

### `--resume`

| Version | Behaviour |
|---|---|
| 0.6.x (current) | Reuse `chunk_checkpoint` rows in the local state DB to skip already-completed chunks; no destination-side awareness. |
| 0.7.0 | Above, **plus** ADR-0012 M8: read the manifest at the destination, apply the decision matrix per part (skip / rewrite / lost / quarantine), refuse to start when `_SUCCESS` is present unless `--force` is given. |
| 0.7.0 (M9) | Untracked / fingerprint-mismatch parts are moved to `_quarantine/<run_id>/<original-name>` best-effort during resume.  This is automatic, not a flag. |

Quarantine (M9) is intentionally not a flag.  An operator who said "resume"
already accepted that the destination would be touched; refusing to move a
corrupt artifact in that mode would be worse than a best-effort relocation
with an audit warning.

### `--force`

A safety-override, not a check.

| Version | Use |
|---|---|
| 0.7.0 | `--resume --force`: proceed even when `_SUCCESS` is present (M8 gate).  Without it, resume against a complete run refuses and exits non-zero so an operator can't accidentally re-export over a verified dataset. |

`--force` is scoped to the specific safety gate it overrides.  Future gates
(if they appear) reuse the same flag rather than adding `--force-resume`,
`--force-overwrite`, etc.

---

## Naming

Why keep `--validate` rather than the conceptually cleaner `--check`:

1. The word is already in the codebase (`pipeline::validate`, `validate_output`,
   `RunReport.validation`).  Renaming it now ripples into Airflow operator
   code and CI scripts that grep for `--validate`.  The cost outweighs the
   gain.
2. The roadmap (`rivet_roadmap_0_6_1_to_0_8_0_encryption.md`) reserved `check`
   for the **pre-run** preflight subcommand.  Mixing pre-run and post-run
   checks under one word would re-introduce exactly the ambiguity this ADR
   is trying to prevent.
3. `--validate` is the same word every other widely-used data tool spells
   (dbt, Airflow, Great Expectations).  Operators don't have to learn a
   Rivet-specific dialect.

---

## What this rules out

These are explicitly **not** going to ship, in 0.7.0 or later, unless this
ADR is superseded:

- `rivet verify` subcommand as a **higher-level umbrella** that subsumes
  validate + reconcile + manifest + schema under one new noun.  See the
  carveout below for the narrower allowance.
- `--verify`, `--audit`, `--check-output`, `--full` flags on `rivet run`.
- Per-invariant flags (`--check-m5`, `--validate-manifest`, `--require-success`).
- Behaviour where `--validate` triggers a source query.
- Behaviour where `--reconcile` does *not* imply `--validate`.
- Silent fallback when manifest is missing — see M6: the report must say
  `legacy_run: true` so the operator knows the surface they're looking at.

If a use case appears that these rule out, the right move is to reopen this
ADR and amend it, not to slip a new flag in under the radar.

## Subcommand carveouts (amendment 2026-05-21)

The contract above pins the **flag surface** of `rivet run`.  It does not
forbid subcommands whose only job is to **re-drive existing flag semantics
standalone**, without introducing new trust nouns.  Two examples:

- `rivet reconcile <export>` — already exists; standalone driver for the
  `--reconcile` semantics that `rivet run --reconcile` performs at end-of-run.
- `rivet validate [--export <name>]` — added 2026-05-21; standalone driver
  for the M5/M6 semantics that `rivet run --validate` performs at end-of-run.
  Runs the same `pipeline::validate_manifest::verify_at_destination` code
  path against an existing destination prefix, no source query, no
  extraction, no state writes.

Allowed subcommand patterns:

- The subcommand's verdict **must** be expressible by an existing flag.
  `rivet validate` produces the same `ManifestVerification` shape that
  `validation.manifest` carries in `summary.json`; an Airflow consumer
  reads it identically from either source.
- The subcommand **must not** introduce a new trust noun in the
  operator-facing language.  "Validate" maps to `--validate`; "reconcile"
  maps to `--reconcile`.  A `rivet verify` subcommand was rejected above
  precisely because "verify" is *not* an existing flag.
- The subcommand **must not** depend on having run an extraction.  These
  are between-run inspection tools, not retroactive run mutators.

Rationale: between-run polling (Airflow sensors, CI gating, operator
triage) is a real workflow that the existing `--validate` flag cannot
serve — it only fires at end-of-run.  Refusing to ship a standalone
driver would force operators to either re-run the entire export to
re-verify, or to reimplement M5 in shell against the manifest schema.
Both are worse than a thin subcommand.

---

## Acceptance criteria

- `rivet run --help` lists exactly the flags above for trust/resume/safety.
- `--reconcile` produces a verdict that subsumes everything `--validate`
  produces (i.e. an operator running `--reconcile` never needs to also
  pass `--validate` to get the full picture).
- Run report renders a single "Verdicts" section that names the strongest
  check the operator asked for, not a column per ADR letter.
- Adding M5, M6, M8, M9 to the codebase causes **zero** changes to
  `rivet run`'s `clap` derive struct beyond `--force` (the safety override).
- Subcommand carveouts (see amendment below) are limited to standalone
  drivers that re-run an existing flag's semantics.  `rivet validate` is
  the first such carveout; future carveouts must clear the same bar.

### Status (2026-05-21)

- `--validate` extended with M5/M6 semantics: ✅ `feat(0.7.0): manifest-aware --validate (1ef2fbb)`
- Standalone `rivet validate` subcommand: ✅ `feat(0.7.0): rivet validate subcommand (20b849a)`
- `--force` safety override + `_SUCCESS` gate: ✅ `feat(0.7.0): _SUCCESS gate + pure resume decision matrix (9b510c7)`
- M8 chunked-resume executor wiring (no new flag): ⚠️ Phase C-γ
- M9 quarantine on resume (no new flag): ⚠️ Phase C-δ
- Integration anchor test (`§24` in `trust_artifacts_integration`)
  pins the `rivet run` flag set; refuses any flag outside the contract.

---

## Open items

1. How `--reconcile` reports its three sub-checks (file rows, manifest M5,
   source COUNT) is a render decision, not a CLI surface decision.  The
   suggestion is one block in `summary.md`:

   ```text
   ## Verdicts

   - Validation:    PASSED (manifest M5 + 13 parts verified)
   - Reconciliation: MATCHED (2,500 rows source ↔ 2,500 rows in manifest)
   - Schema:        unchanged (xxh3:cad2…)
   ```

   Bikeshed-friendly; not part of this ADR's contract.

2. The `--validate --deep` and encryption-aware `--validate --identity ...`
   behaviour-modifiers above are a single-flag-with-modifiers pattern
   (`--validate <mode>`).  Whether to spell them as positional sub-modes
   (`--validate=deep`) or boolean co-flags (`--validate --deep`) is a
   future ADR decision, not this one.  Either form keeps the contract:
   no new top-level trust flag.

---

## References

- ADR-0012: Cloud Manifest Contract — defines M1–M9.
- `rivet_roadmap_0_6_1_to_0_8_0_encryption.md` — release scoping.
- `pipeline::report::RunReport.validation` / `.reconciliation` — the
  on-wire shape these flags drive.
