_Last updated: 2026-05-19._

# Pilot guide — operator runbook

This folder is for engineers who are evaluating Rivet seriously: they have already run the [5-minute install + first export](../getting-started.md) and now want the **full flow on their own database**, with production-ready guardrails.

If you have not run a first export yet, do that first — [docs/getting-started.md](../getting-started.md). Come back here when you want to take it further.

---

## "Done" looks like

- **Minimum:** config validates, `rivet run` finishes, files land where you expect, you can read them back.
- **Full pilot (chunked + checkpoints):** you can also read [progression](../concepts.md), [reconcile against the source](../reference/cli.md#rivet-reconcile), and run [targeted repair](../reference/cli.md#rivet-repair) without surprising the database or storage.

---

## Pick one path

| Goal | Time | Where to start |
|------|------|------------|
| **Scripted evaluation** on a 14-table seeded fixture — prove every feature works end-to-end, no real data risk | ~10 min | [Demo quickstart](demo-quickstart.md) — needs Docker + repo checkout + `cargo build` + a seed step |
| **Full pilot on your own database** — discovery → chunked → reconcile → repair → verified | 1–2 sessions | [Pilot walkthrough](pilot-walkthrough.md) |
| **Sign-off on a pilot you've already run** | ~20 min | [UAT checklist](uat-checklist.md) |

Don't mix paths until the first one is green.

---

## Standard pilot order of operations

The [walkthrough](pilot-walkthrough.md) expands every step with examples, YAML, and commands. Use this list as the order; use the walkthrough for the detail.

### Minimum pilot — steps 1–5

Enough for a serious first pass: validated config, successful run, optional plan/apply.

1. **Read once:** [Production checklist](production-checklist.md) — access model, TLS, pooler/proxy detection, tuning, destinations. Skim before pointing at production.
2. **Scaffold:** `rivet init` → YAML + optional `discovery.json` (walkthrough Step 1).
3. **Author config:** match mode to the table — `full` / `incremental` / `chunked` / `time_window`. For reconcile + repair later, use `chunked` with `chunk_checkpoint: true` (walkthrough Step 2).
4. **Preflight:** `rivet doctor` + `rivet check` on the final YAML.
5. **Execute:** `rivet plan` → `rivet run` and/or `rivet apply` (walkthrough Steps 3–4).

### Chunked + trusting the data — steps 6–8

Only when you have **chunked** exports with **`chunk_checkpoint: true`**. Skip this block for pure `full` / simple `incremental` pilots.

6. **Verified extraction:** `rivet state progression` → `rivet reconcile` → `rivet repair` if dirty → reconcile again (walkthrough Steps 5–8). Background contract: [ADR-0009](../adr/0009-reconcile-and-repair-contracts.md).
7. **Automate:** cron / CI pattern in walkthrough Step 9.
8. **Sign-off:** [UAT checklist](uat-checklist.md) when you're ready to call the pilot complete.

---

## Documents in this folder

| Document | Use when |
|----------|----------|
| [demo-quickstart.md](demo-quickstart.md) | Scripted demo on the 14-table fixture |
| [pilot-walkthrough.md](pilot-walkthrough.md) | Full flow on your own data (discovery → verified) |
| [production-checklist.md](production-checklist.md) | Before production or high-stakes databases |
| [uat-checklist.md](uat-checklist.md) | Structured sign-off after the pilot |
| [reconcile-runbook.md](reconcile-runbook.md) | Verify an export against the live source with SQL only |
| [rivet-vs-cursor-pipeline.md](rivet-vs-cursor-pipeline.md) | Like-for-like vs an existing cursor/watermark ELT pipeline |

Full doc index: [docs/README.md](../README.md). Concept glossary (`run_id`, `cursor`, `chunk`, `manifest`, `journal`, `progression`): [docs/concepts.md](../concepts.md).
