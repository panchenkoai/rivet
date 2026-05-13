# Pilot guide — instructions

**New to Rivet?** Do this in order:

1. **Install and orientation** — [Getting Started](../getting-started.md) (binary, connect, what an export is).
2. **First real run** — [Quickstart: PostgreSQL](quickstart-postgres.md) or [Quickstart: MySQL](quickstart-mysql.md) (one table, ~5 minutes).

Then continue with **§1 Choose one path** below if you are moving past a single trial export.

---

This folder is the **operator runbook** for trying Rivet seriously: first export, scripted evaluation, full flow on your database, and go-live checks.

**Who this is for:** engineers or data operators who will own exports in front of a real database.

**What “done” looks like:**  
- **Minimum:** config validates, `rivet run` finishes, files land where you expect.  
- **Full pilot (chunked + checkpoints):** you can also read progression, reconcile against the source, and run targeted repair without surprising the database or storage.

### Terms (quick)

| Term | Meaning |
|------|---------|
| **Scaffold** | Generate a YAML (and optionally `discovery.json`) from the live database with `rivet init`. |
| **Plan / apply** | `rivet plan` writes a sealed run intent; `rivet apply` runs exactly that artifact (good for review and CI). You can use `rivet run` instead of apply when you do not need that split. |
| **Chunked + `chunk_checkpoint`** | Large tables are exported in ranges; checkpoints record per-chunk progress so Rivet can reconcile and repair by partition. |
| **Reconcile / repair** | Reconcile compares source counts to exported chunk metadata; repair re-runs only chunks that were flagged (see [walkthrough](pilot-walkthrough.md) Steps 6–8). |

---

## 1. Choose one path

Pick **one** row and follow it top to bottom. Do not mix paths until the first path is green.

| Goal | Time | Start here |
|------|------|------------|
| **First successful export** (one table, prove connectivity) | ~5 min | [Quickstart: PostgreSQL](quickstart-postgres.md) or [Quickstart: MySQL](quickstart-mysql.md) |
| **Product evaluation** (features end-to-end on a seeded DB, no real data yet) | ~10 min | [Demo quickstart](demo-quickstart.md) — needs Docker, repo checkout, `cargo build`, and seed steps (see that doc) |
| **Real pilot** (your schema, your export targets, production-minded) | 1–2 sessions | [Pilot walkthrough](pilot-walkthrough.md) |

If you have never run Rivet at all, use **Getting Started → Quickstart** above, then either **Demo** or **Walkthrough** depending on whether you want the scripted fixture.

---

## 2. Standard pilot on your own database (checklist)

Use this as the **order of operations**. The [walkthrough](pilot-walkthrough.md) expands every step with examples, YAML, and commands.

### Minimal pilot (steps 1–5)

Enough for a serious first pass: validated config, successful run, optional plan/apply.

1. **Read once:** [Production checklist](production-checklist.md) — access model, TLS, tuning, destinations (skim before pointing at production).
2. **Scaffold:** `rivet init` → YAML + optional `discovery.json` (walkthrough Step 1).
3. **Author config:** match mode to the table (full / incremental / chunked / time window). For reconcile/repair later, use chunked with `chunk_checkpoint: true` (walkthrough Step 2).
4. **Preflight:** `rivet check` and `rivet doctor` on the final YAML.
5. **Execute:** `rivet plan` → `rivet run` and/or `rivet apply` (walkthrough Steps 3–4).

### Chunked + trusting the data (steps 6–8)

Only when you have **chunked** exports with **`chunk_checkpoint: true`**. Skip this block for pure `full` / simple `incremental` pilots unless you add chunked exports.

6. **Progression, reconcile, repair:** `rivet state progression` → `rivet reconcile` → `rivet repair` if needed → reconcile again (walkthrough Steps 5–8).
7. **Automate:** cron/CI pattern in walkthrough Step 9.
8. **Sign-off:** [UAT checklist](uat-checklist.md) when you are ready to call the pilot complete.

---

## 3. Documents in this folder

| Document | Use when |
|----------|----------|
| [quickstart-postgres.md](quickstart-postgres.md) | First export on PostgreSQL |
| [quickstart-mysql.md](quickstart-mysql.md) | First export on MySQL |
| [demo-quickstart.md](demo-quickstart.md) | Repeatable demo on the 14-table fixture |
| [pilot-walkthrough.md](pilot-walkthrough.md) | Full flow on your data (discovery → verified) |
| [production-checklist.md](production-checklist.md) | Before production or high-stakes databases |
| [uat-checklist.md](uat-checklist.md) | Structured sign-off / regression after a release |

Full doc index: [docs/README.md](../README.md).
