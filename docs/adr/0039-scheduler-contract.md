# ADR-0039: The Scheduler Contract — Machine Errors, State Location, and the Run Lease

- **Status:** Accepted (contract only). Nothing below is implemented at this ADR's date except where a row says "today"; the JSON shapes are pinned by fixtures so each implementing change must match them or change the fixture, the test and this document together.
- **Date:** 2026-10-06
- **Amends:** [ADR-0026 — First-party extension seam](0026-first-party-extension-seam.md) (where orchestration, alerting and the warehouse load sit)
- **Context:** A scheduler (Airflow first; cron and any other orchestrator by the same means) has to decide three things about a rivet process without reading its prose: *retry or stop*, *which unit failed and what to do about it*, and *whether another run of the same unit is in flight*. At the tree this ADR was last checked against (`origin/main` on 2026-10-06, after #449) the product cannot answer them:
  - `--json-errors` prints `{"error", "exit_class", "code"?}` for ONE representative failure; `exit_class` is the integer exit code (`src/cli/mod.rs:34-41`). N failed exports fold to one error (`fold_failures`, `src/pipeline/run.rs:1116`; `aggregate_load_failures`, `src/load/orchestrate.rs:365`).
  - A failed export keeps only redacted text; the typed error is in scope and dropped (`src/pipeline/job.rs:1406-1410`), so the run aggregate entry carries `error_message` and nothing a machine can branch on (`RunAggregateEntry`, `src/state/run_aggregate.rs:41-55`).
  - The CDC drain knows it stopped on `max_events` (`hit_max`, `src/source/cdc/sink.rs:598`) and its return type, `(Vec<RunManifest>, Result<()>)` (`src/source/cdc/sink.rs:438-441`), cannot say so. A multi-table stream's summary sums the per-table manifests away (`src/pipeline/cdc_job.rs:262-263`).
  - SQLite state is always `<config dir>/.rivet_state.db` (`src/state/mod.rs:52`, `:293-296`); `RIVET_STATE_URL` is honoured only with a `postgres` prefix (`src/state/mod.rs:235-236`). No record says which source a state DB as a whole belongs to, and an absent one is created empty without a word.
  - A lease exists (`src/state/load_lease.rs:5-9`) and never waits (`try_load_lease`, `src/state/load_lease.rs:129`). Its three holders are a checkpointed chunk run (`src/pipeline/chunked/mod.rs:310`), a table load (`src/load/orchestrate.rs:453`) and staging (`src/load/staging.rs:264`). No runtime guard stops two overlapping cycles of one CDC stream on any engine.
  - A CDC checkpoint is a JSON file and nothing else: `Position::save` writes it by temp file and rename (`src/source/cdc/mod.rs:146`), `Position::load` reads it (`:118`), and a relative `cdc.checkpoint` resolves beside the config (`resolve_checkpoint`, `src/source/cdc/mod.rs:1894`). No state table holds a stream position on any backend. An absent file reads as a first run (`Ok(None)`, `:132`), so a stream whose file was lost re-anchors.
  - A child process killed by a signal has no exit code; the parent records the text `exited with status signal` (`src/pipeline/parallel_children.rs:335-343`) and, when no other child reported a code, fails as an unclassified error, exit 1 (`aggregate_child_result`, `:502-511`).
  - Slack fires per export from three sites (`src/pipeline/job.rs:1560`, `:1669`; `src/pipeline/cdc_job.rs:320`), so a scheduler with its own alerting gets every failure twice.

  The existing recipe works around all of it with a blanket retry count, the state URL on the command line and a chunk reset before each retry (`docs/recipes/airflow/`). This ADR fixes the contract that removes those workarounds. It decides shapes, names and sources of truth; it adds no code.

---

## Decision

### D0 — One name for the unit of work

The unit a scheduler runs, retries and reports is the **export**. Four spellings were in circulation (`export_name`, `export`, `table`, `unit`); the contract has one rule:

- Keys the product already emits keep their spelling: `export_name` on the run aggregate entry and on child events. Renaming them is not additive.
- Every NEW key that names the unit is **`export`**.
- `table` is never a synonym for the unit. It appears only beside `export`, where a table is a second coordinate: the warehouse table of a `load` / `compact` result or failure (one table can be fed by two exports, so neither name alone identifies the row), and the captured source table inside a CDC entry's `tables[]`.
- "Unit" is prose only; no key is called `unit`.

*Rejected: `unit` as the new key.* It would be a fifth spelling for something every state row already keys by export name.

### D1 — One error object, one producer

```json
{ "code": "RIVET_SOURCE_CDC_LOG_GAP", "kind": "refusal", "class": "refusal", "exit_code": 5,
  "retryable": false, "action": "<registry action>", "message": "<redacted text>" }
```

Fixtures: [`error_object.json`](../../tests/fixtures/scheduler/error_object.json) (coded), [`error_object_crashed.json`](../../tests/fixtures/scheduler/error_object_crashed.json) (no exit code). Every key is always present; a value that does not apply is `null`.

| Field | Type | Single source of truth | `null` when |
|---|---|---|---|
| `code`, `kind`, `action` | string | the `Code` registry (`codes::ALL`, `src/error.rs:529`; `Code { id, kind, action }`, `src/error.rs:189`) reached by the downcast `error_code` performs (`src/error.rs:244`); `kind` is `ErrorKind::name` (`src/error.rs:176`) | the failure is uncoded |
| `exit_code` | integer | `classify_exit` (`src/error.rs:283-297`) | the unit's process was killed by a signal |
| `class` | string, closed set | the name of `exit_code`'s `ExitClass` (`src/error.rs:34-53`): `generic`, `retryable`, `data_integrity`, `schema_drift`, `refusal`, `internal`; `crashed` when there is no exit code | never |
| `retryable` | boolean | `true` for `class` `retryable` (`exit_code == ExitClass::Retryable`, `src/error.rs:293-295`) and for `crashed`; `false` otherwise | never |
| `message` | string | `redact_error` then `sanitize_terminal`, as the text line is built today (`src/cli/mod.rs:33`) | never |

`retryable` means: **a re-run without human action may succeed.** It does not mean "retry without limit"; D8 says how many times.

`ExitClass` has no name function today; the implementing change adds ONE beside `ExitClass::code` (`src/error.rs:57`) and it is the only place the six names are written in the product.

**`crashed`.** A unit whose process was killed by a signal produced no exit code and no error from inside. Its object is `class: "crashed"`, `exit_code: null`, `retryable: true`, and `code`, `kind`, `action` `null`. It is retryable because rivet resumes from its state: a chunked run continues from its checkpoint, a CDC drain re-reads the un-acknowledged span, load and compact are driven by the ledger. *Rejected: `retryable: false` with the retry rule living in the scheduler's table of classes* — it makes `retryable` mean two things and sends every consumer back to a table this object exists to remove. *Rejected: reporting a killed unit as `generic`*, which is what happens today: it reads as "fix the input" for a failure no input caused.

**What must not get a second classifier.** One function in `src/error.rs` turns an `anyhow::Error` into this object, and every emitter (text line, `--json-errors`, run aggregate, process-child event, load and compact results, notifications) calls it. The text-derived labels of `classify_error_message` (`src/pipeline/job.rs:20`, stored as `export_metrics.error_class`) are a grouping aid for humans and are not part of this contract.

**What is true about text today, and what the contract requires.** The `retryable` class is NOT free of text matching in the product: `classify_exit` asks `classify_error` (`src/error.rs:293`), which tries typed driver errors first and then falls back to matching the lowercased message (`src/pipeline/retry.rs:184-185`). So today's `exit_code: 2` can be the result of a substring match. The contract requires three things going forward: (1) a CONSUMER never matches text, it reads `retryable`; (2) the object is derived only from `classify_exit` and the registry, never from a second look at the message; (3) the text fallback only shrinks: a failure introduced by an implementing change is classified by a typed error or a registered code, and no new pattern is added to the fallback.

Honest limit: 419 `bail!` sites are uncoded (`tests/offline/error_code_ratchet.rs:7`), so `code`, `kind` and `action` are often `null`. `class` and `retryable` always exist.

### D2 — `--json-errors`: every failed unit, the integer `exit_class` kept

One line on stderr, as today. Fixtures: [`json_errors.json`](../../tests/fixtures/scheduler/json_errors.json) (two failed exports, the representative one coded), [`json_errors_uncoded.json`](../../tests/fixtures/scheduler/json_errors_uncoded.json), [`json_errors_crashed.json`](../../tests/fixtures/scheduler/json_errors_crashed.json), [`json_errors_load.json`](../../tests/fixtures/scheduler/json_errors_load.json) (`rivet load`, two failed tables).

| Key | Type | Rule |
|---|---|---|
| `error` | string | **unchanged**: the text the fold emits today |
| `exit_class` | integer | **unchanged**: the exit code of THIS process (`src/cli/mod.rs:34-41`) |
| `code` | string | **unchanged, including its absence**: the key is OMITTED when the representative failure is uncoded (`src/cli/mod.rs:38-40`); it is never `null` |
| `exit_code` | integer or `null` | new; D1, for the representative failure |
| `class` | string | new; D1 |
| `kind`, `action` | string or `null` | new; always present, `null` when uncoded |
| `retryable` | boolean | new; D1 |
| `failures` | array | new; one entry per failed unit |

So an uncoded failure prints every key except `code`, with `kind` and `action` `null`. `code` is the only key of this contract that may be absent, and only because it already is.

- `exit_code` equals `exit_class` for every class except `crashed` (below).
- `failures[]`: each entry is the D1 object plus `export` (string) and `table` (string, or `null` for `run` and `apply`). `rivet load` and `rivet compact` fill both, per D0. `[]` when the failure belongs to no unit (a config that does not parse). The list is collected at the two folds named in Context BEFORE they flatten; which failure is representative, and therefore the process exit code, does not change.
- `error` for N ≥ 2 failed exports is what `fold_failures` builds (`src/pipeline/run.rs:1138-1142`): `N export(s) failed<context>; representative error follows (also: <the others, joined by "; ">): exit class <n>: <the representative>`. The `exit class <n>` segment is the display of the `PreclassifiedExit` context the fold attaches (`src/error.rs:150-154`). `rivet load` builds `N load(s) failed; representative error follows (also: …): <the representative>` (`src/load/orchestrate.rs:379-382`). With one failed unit `error` is that unit's `message`.

**A killed child.** When a child process is killed by a signal and no failed child reported an exit code, the top-level object reports `class: "crashed"`, `exit_code: null`, `retryable: true`. Today it would say generic. The parent's own exit status, and therefore `exit_class`, stays what it is today in that case (1): this ADR changes no exit code. When another child did report a code, the representative failure is chosen among the coded ones as today (`worst_exit_code`, `src/pipeline/parallel_children.rs:514-521`) and the killed child appears in `failures[]` with `class: "crashed"`.

**The one object the integration builds.** When the WHOLE rivet process is killed, nobody inside can print a line. This is the only case in which an integration constructs an error object instead of reading one: the process ended on a signal (no exit code) → exactly [`error_object_crashed.json`](../../tests/fixtures/scheduler/error_object_crashed.json), with the signal number in `message`:

```json
{ "code": null, "kind": null, "class": "crashed", "exit_code": null,
  "retryable": true, "action": null, "message": "rivet was killed by signal 9" }
```

In every other case the integration reads rivet's line and adds nothing. An exit code with no line is not this case: D8 lists it.

*Rejected: giving `exit_class` the class name.* Every consumer that compares it with a number would break; the name gets its own key. *Rejected: `"code": null` for an uncoded failure.* A consumer that tests for the key's presence today would start seeing a coded failure with no code.

### D3 — Per-unit results carry the error object

Fixtures, each one `per_export[]` entry of the run aggregate (`--json`, `--summary-output`, `details_json`): [`run_entry.json`](../../tests/fixtures/scheduler/run_entry.json) (a successful CDC entry), [`run_entry_failed.json`](../../tests/fixtures/scheduler/run_entry_failed.json) (failed, with the full `error` object), [`run_entry_crashed.json`](../../tests/fixtures/scheduler/run_entry_crashed.json) (a child killed by a signal).

`RunAggregateEntry` (`src/state/run_aggregate.rs:41-55`) gains three members and renames none; `export_name` and `error_message` stay. Every key is always written, `null` when it does not apply; an entry written by an older binary reads back with the new members absent.

| Key | Type | Values |
|---|---|---|
| `export_name`, `run_id`, `mode`, `error_message` | string (`error_message`: or `null`) | as today |
| `status` | string, closed set | `success`, `failed`, `skipped`, `interrupted` |
| `rows`, `files`, `bytes`, `bytes_read`, `duration_ms` | non-negative integer | as today |
| `error` | object or `null` | the D1 object for every `failed` entry on every path (sequential, threads, waves, pool, and process children through a defaulted member on `ChildEvent::Finished`, `src/pipeline/ipc.rs:104-121`); `null` otherwise |
| `stop_reason` | string or `null`, closed set | D4 |
| `tables` | array or `null` | D4 |

A child that was killed never sends `Finished`; the parent writes its entry with `status: "failed"`, the D1 `crashed` object, and the text it has today as `error_message` and `message`.

`rivet load` and `rivet compact` each print one object: `run_id` and `per_table[]` with `export`, `table`, `status`, `skip_reason`, `rows`, `error`. The two commands have different closed sets of `status`:

| Command | Fixture | `status` |
|---|---|---|
| `rivet load` | [`load_result.json`](../../tests/fixtures/scheduler/load_result.json) | `loaded`, `nothing_to_do`, `failed` |
| `rivet compact` | [`compact_result.json`](../../tests/fixtures/scheduler/compact_result.json) | `compacted`, `nothing_to_do`, `skipped`, `failed` |

`error` is the D1 object iff `status` is `failed`. `skip_reason` is non-null iff `status` is `skipped`, so it exists only in a compact result: it is the text the product's own predicate returns (`compact_skip_reason`, `src/load/compact.rs:96`), which a scheduler shows and never re-derives. A load has no skip.

### D4 — CDC: `stop_reason` and per-table `tables[]`

- `stop_reason`: `caught_up` or `max_events` on a successful `mode: cdc` entry, `null` on every other entry. Its single source of truth is `hit_max` (`src/source/cdc/sink.rs:598`) threaded out of `run_to_files`; it is never inferred from row counts or from comparing a count with the cap. The exit code is 0 for both.
- `tables[]`: `{ table, rows, files }` per captured table on every `mode: cdc` entry (empty when the drain delivered nothing), `null` on every other entry; from the per-table manifests the drain already returns. Display data. **It must not gate the load**: the load consumes what the LEDGER says is unloaded, which includes parts from an earlier cycle whose load failed; a scheduler that skipped a table's load because "this run delivered 0" would strand them until the table next changes.
- A run that stops on `max_events` is reported and nothing more. *Rejected for now: the scheduler integration re-triggering itself until `caught_up`* — a loop driven by a field this ADR has only just defined; it can be added on top of `stop_reason` without changing it.

### D5 — Where the state lives, by deployment

`RIVET_STATE_URL` gains one value form:

| Value | Where the state is |
|---|---|
| unset | `<config dir>/.rivet_state.db`, as today |
| `postgres…` | that database, as today |
| `sqlite:<absolute directory>` | `<directory>/.rivet_state.db` — the file keeps its name |

Source of truth: the ONE existing environment read in `StateStore::open` (`src/state/mod.rs:235-236`) gains a value form. No new environment read, and child processes inherit it. The second resolver, `state_db_path` (`src/state/mod.rs:326`), which feeds messages (`src/pipeline/cli.rs:448`), must answer through the same function, or a message names a file rivet did not open. `.rivet/runs` and `.rivet/spill` stay beside the config.

What is allowed depends on what the worker is:

| Worker | State | Rule |
|---|---|---|
| **Ephemeral pod** (Kubernetes) | PostgreSQL, REQUIRED | The integration refuses to start a task with SQLite state. No state, checkpoint or log file may live on the pod's disk: it does not outlive the task. |
| **Local or long-lived worker** | SQLite in a declared directory on a host-mounted volume, or PostgreSQL | SQLite is allowed; the integration prints a warning that the setup is single-host. |

- rivet cannot tell a pod from a host, so the pod rule is enforced by the integration, which knows how it launches the task. *Rejected: letting SQLite state run on a pod with a warning* — the first task succeeds and every later one starts from an empty state, which for incremental and CDC exports is silent re-reading or silent loss.
- **The local worker gets a setup guide, and the guide is part of the contract's delivery.** The integration's documentation carries a page for the single-host setup that states: where the declared directory must be mounted (a host path or named volume, never the container's own filesystem); that every task of every DAG that touches the pipeline must see the SAME absolute path; what happens when it does not (a task that sees an empty directory creates a new state and treats every export as a first run, and the D6 refusal does not fire because an empty state has no identity); and how to move to PostgreSQL state. The warning the integration prints names that page.
- **CDC checkpoints.** A checkpoint is a file on every state backend today (Context). Under `sqlite:<directory>` a relative `cdc.checkpoint` resolves under the declared directory through `resolve_checkpoint` (`src/source/cdc/mod.rs:1894`), the function `doctor` also uses; a checkpoint found beside the config is still used, with a warning, as the working-directory fallback is today.
- **Under PostgreSQL state a RELATIVE `cdc.checkpoint` is refused** (`RIVET_STATE_CHECKPOINT_LOCAL`, kind `refusal`, exit 5; the message names the absolute path the file resolves to today). It resolves beside the config, which is worker-local disk exactly where PostgreSQL state says the worker is replaceable; a lost checkpoint reads as a first run, the stream re-anchors, and the changes between the lost position and the new anchor are gone without an error. An absolute path is the operator's statement that the file is on storage that outlives the worker, and is accepted. *Rejected: a warning.* The failure it would warn about is silent and arrives weeks later.
- **Target, recorded as a planned change: under PostgreSQL state the CDC checkpoint is stored IN the state database**, so a pod needs no volume at all. Until that change lands, **CDC on pods is not supported**: the integration refuses a CDC export on an ephemeral worker and says that the checkpoint would live on the pod's disk.

*Rejected: a `--state-dir` flag.* It would have to be forwarded to every child process and carried by a process-global to the 44 `StateStore::open(` occurrences under `src/`. *Rejected: a `state locate` subcommand.* The caller SET the directory; it has nothing to ask.

### D6 — State identity and the `RIVET_STATE_FOREIGN` refusal

Two derivations in the tree answer "which source", and they are not the same thing:

| Name | Shape | Computed from | Answers |
|---|---|---|---|
| `SourceConfig::state_key` (`src/config/source.rs:411`; `source_state_key`, `:602`) | `engine://host:port/database` | the source URL, credentials and parameters dropped | which DATABASE a state belongs to |
| `source_ident` (`identity_source`, `src/manifest.rs:144-150`; column `loaded_source_run.source_ident`, `src/state/migrations.rs:360`, read by `loaded_source_idents`, `src/state/load_journal_store.rs:124`) | `engine:schema.table` | a run manifest | which TABLE the rows of one warehouse table came from |

- **The state identity is `state_key`.** One state migration adds the record. `source_ident` keeps its job, the load's ownership guard (`src/load/orchestrate.rs:694-709`), and is not the identity: it carries no host and no database, so two databases with the same schema would collide in it, and it exists per warehouse table, not per state. Nothing is derived from one into the other. *Rejected: building the identity from `source_ident` or from `run_aggregate.config_path` (`src/state/migrations.rs:116`)* — the first cannot tell two servers apart, the second changes when a file is moved.
- The claim is made once, in `dispatch` (`src/cli/dispatch.rs:67`), before any command body; the `StateStore::open(` call sites keep their signature.
- **Refusal: only when BOTH keys are non-empty and differ.** A state found in a declared `sqlite:` directory whose recorded key is non-empty, read by a command whose own key is non-empty and different, is refused with `RIVET_STATE_FOREIGN` (kind `refusal`, exit 5): the message names the directory and the other source, the action is "choose another state directory". It is checked BEFORE migrating, so a refused file is byte-identical afterwards.
- **A command that cannot compute its key skips the check.** `state_key` is the empty string when the source URL cannot be resolved (`unwrap_or_default`, `src/config/source.rs:412-414`). `rivet load` and `rivet compact` never connect to the source and may run where its URL is not in the environment at all; they compare nothing and stamp nothing. *Rejected: refusing when the key cannot be computed* — it would demand source credentials from the one task that must not need them.
- A state with progress and no recorded key (written by an older binary, or touched so far only by commands that skip the check) is adopted, with a warning, by the first command that can compute its key; it is never refused.
- **Scope of the refusal.** Only a declared `sqlite:` directory refuses. A state beside the config is stamped and WARNS on a mismatch, because configs for different sources share a directory today. A PostgreSQL state is shared by design and is not enforced. *Rejected: refusing everywhere* — it would break the supported several-configs-one-directory layout on upgrade.
- **Identity content.** The source key only. Two pipelines on the same database share one state, which is today's supported multi-config case. *Rejected: an explicit pipeline name in the identity* — a new required setting whose only job is to forbid a layout that works.

*Rejected: a sidecar identity file.* It can be lost, or copied apart from the database it describes.

### D7 — The run lease: per export, waited on

- One lease per export, held for the whole run by the process that runs the export, in every mode including the CDC drain. Built on `src/state/load_lease.rs`: an `flock` on a sidecar for SQLite, released by the OS when the holder dies; a heartbeat row with a 30 s TTL and immediate takeover of a dead pid on the same host for PostgreSQL (`src/state/load_lease.rs:36`, `:57-70`).
- **Its key stays `chunk-run:<export>`** (`src/pipeline/chunked/mod.rs:310`). The lease is the old one widened to every mode, not a new one beside it, so an old and a new binary on one state take the SAME key and exclude each other during an upgrade. *Rejected: renaming the key to `run:<export>`* — an old binary would hold `chunk-run:` while a new one held `run:`, and two checkpointed runs of one export would overlap for as long as both versions run. *Rejected: the new binary taking both names for one release* — two leases per run, an ordering rule between them, and a second compatibility event when the old name is dropped, all to correct a string no user reads. The name is a misnomer for a CDC or incremental run and stays one.
- A busy lease is WAITED on: a poll of the existing non-blocking take, with a log line naming the holder, up to `--lock-wait <seconds>` (global flag, default **600**). On timeout: `RIVET_STATE_LOCK_TIMEOUT`, exit 2, `retryable: true`. `--lock-wait 0` keeps the immediate refusal a second chunk run gets today (`live_chunk_run_refusal`, `src/pipeline/chunked/mod.rs:291`). The table lease of the load (`src/load/orchestrate.rs:453`) waits by the same rule.
- **Staging cleanup does not wait.** The third holder, the prefix lease around staging cleanup, skips the cleanup when the lease is busy and says so (`src/load/staging.rs:264-271`). It keeps that behaviour under every `--lock-wait` value: the cleanup is best-effort collection that the next load repeats, and waiting would hold a finished load for up to the timeout behind someone else's cleanup. `--lock-wait` governs the run lease and the table lease only.
- `--lock-wait` must reach process children, which take the leases; their argv is a hand-kept list (`src/pipeline/parallel_children.rs:151-175`). The implementing change derives "every global flag is forwarded or declared parent-only" from the clap definition instead of extending the list by hand.
- A crashed checkpointed run is still resumed by the next process that takes the lease; a scheduler never resets chunk state before a retry.

*Rejected: a lease per config.* Several exports of one config run at once as separate tasks; the unit every state row is already keyed by is the export. *Rejected: refusal as the default with waiting opt-in.* Two triggers of one schedule are an ordinary event, and a refusal turns it into a failed task that someone has to read. *Rejected: scheduler-only means (pools, one active run).* They see nothing started from cron or by hand. *Rejected: scanning for a process.* Blind across hosts.

**Documented limits.** The lease lives in the state, so two hosts with two separate SQLite files do not see each other; it does not fence a holder that stalls past the PostgreSQL TTL (logged only, `src/state/load_lease.rs:81`); and `flock` on a network filesystem is only as good as that filesystem. An OLD binary takes the lease only for a checkpointed chunk run, so during an upgrade an old binary's CDC or incremental run is still unguarded against a new one. Fencing across hosts is [ADR-0032](0032-fenced-checkpoint-journal.md), which stays proposed. No engine supplies a runtime guard of its own for two overlapping cycles of one stream, so the lease is the guard and each engine's proof must go red without it.

### D8 — What a scheduler does with the object

A consumer reads `retryable`. `class` then selects the BUDGET:

| `class` | `retryable` | Retry |
|---|---|---|
| `retryable` (includes the lease timeout) | `true` | yes, under the task's transient budget |
| `crashed` | `true` | yes, under its OWN, shorter budget: default **one** retry |
| `generic`, `data_integrity`, `schema_drift`, `refusal`, `internal` | `false` | no |
| exit 2 with no rivet error line (a command-line usage error from the argument parser) | — | no; the integration builds no object |

The crashed budget is separate because the commonest cause of a kill is the kernel's out-of-memory killer, and a run that is killed for memory is killed again: under the transient budget it would burn every retry and its backoff on a failure that needs a bigger worker. One retry covers the eviction and the node loss; the second kill fails the task. *Rejected: one shared budget.* It either starves transient errors of retries or lets an out-of-memory loop run to the end of them.

A batch export is one task that plans and applies its sealed plan, so a retry replays the same plan. That holds only if re-applying a sealed plan resumes a failed run; if the implementing change cannot prove it live, the task is `rivet run -e <export>` instead. *Rejected: one task for the whole config with results fanned out to display-only tasks* — they could be neither retried nor cleared.

### D9 — `--no-notify`, and the secrecy rule

- `--no-notify` (global flag, forwarded to children) suppresses every notification rivet would send (`maybe_send`, the three sites in Context). A scheduler integration always passes it and alerts through its own channel, from the D1 object. *Rejected: withholding the webhook variable* — it works for `webhook_url_env` (`src/notify.rs:64-72`) and an inline `webhook_url:` would still fire. No new environment read is added for this.
- **Secrets travel in the process environment and nowhere else.** argv carries the config path, export names and flags.
- **Nothing secret and no error text in a scheduler's metadata store.** Redaction covers credentials only; error text may embed source cell values (`SECURITY.md:38-45`). What an integration may push (Airflow XCom) is the D1 object WITHOUT `message`, plus `export`, `table`, `run_id`, counts and `stop_reason`: fixture [`xcom_unit.json`](../../tests/fixtures/scheduler/xcom_unit.json). The exception it raises reads `[code] class: action (export)`.
- The task log gets one structured line per failed unit.
- **Raw stderr is written by the integration, not by rivet.** rivet writes its stderr to the stream it was given and creates no log file. On a pod the integration leaves it on the pod's log stream; on a local worker it writes it to a file in the declared state directory and the task log names the path. *Rejected: rivet writing `<state dir>/logs/<run id>.log` itself* — a pod has no such directory, and a second log sink inside the product is a second place for redaction to be wrong. *Rejected: streaming the credential-redacted stderr into the scheduler's task log* — it puts possibly data-bearing text into a store with wider access than the worker.

### D10 — Where the integration lives

Orchestration and alerting integrations, including the operators for `load` and `compact`, are open source and live in this repository as a Python package with its own manifest, lock, version line and release tag prefix, published to PyPI as **`airflow-provider-rivet`**. See the amendment in ADR-0026. Two formatters of a notification, one in Rust and one in Python, are pinned by a shared fixture; *rejected: a `rivet notify` subcommand*, because a scheduler's callback may run where the binary is not installed.

---

## Compatibility

| Surface | Change | Migration / bump |
|---|---|---|
| `--json-errors`, run aggregate JSON, `details_json`, child events | additive members only; `error`, `exit_class` and the presence rule of `code` unchanged | none |
| A child killed by a signal | its entry and, when it is the representative failure, the top-level object say `class: "crashed"` instead of generic; the process exit code is unchanged | none |
| State schema | the identity record | **one** state migration, the version after the current head (`SCHEMA_VERSION`, `src/state/migrations.rs:8`); older binaries refuse the newer state as they do today |
| `RIVET_STATE_URL` | accepts `sqlite:<dir>` | none |
| Lease key | `chunk-run:<export>` kept, now taken in every mode | none: old and new binaries share the key. An old binary still takes it only for checkpointed chunk runs |
| A second run of one export | waits (bounded) instead of refusing (chunked) or overlapping (every other mode); `--lock-wait 0` restores the refusal | behaviour change, CHANGELOG |
| Staging cleanup under a busy prefix lease | skips, as today, whatever `--lock-wait` says | none |
| PostgreSQL state with a relative `cdc.checkpoint` | refused (`RIVET_STATE_CHECKPOINT_LOCAL`); an absolute path is accepted | **behaviour change**, CHANGELOG: a deployment that runs this today must make the path absolute before upgrading |
| CLI | `--no-notify`, `--lock-wait`; `--export`, `--table`, `--json`, `--summary-output` on `load` / `compact`; `--json`, `--summary-output` on `apply` | cli matrices |
| Manifests, part names, checkpoint file format, exit codes, config schema keys | untouched | none |

Additive only: no manifest version and no config-schema bump follows from this ADR. Storing the checkpoint in the state database (D5, planned) will need its own state migration and is not part of the one above.

---

## Notes for implementers

- **A new member on `RunAggregateEntry` needs `#[serde(default)]`.** The read path parses `details_json` with `unwrap_or_default()` (`src/state/run_aggregate.rs:103`): one entry that fails to deserialize does not error, it turns the WHOLE `per_export` list of that row into `[]`. A member added without the attribute silently empties every row an older binary wrote. `bytes_read` (`:50`) is the model. The same holds for a new member on `ChildEvent::Finished` (`dest_retries`, `src/pipeline/ipc.rs:119-120`).
- **`code` is omitted, the other keys are `null`.** Do not "tidy" the top-level object into one rule; the asymmetry is the compatibility guarantee.
- **A killed child reports no `Finished` event.** The `crashed` object is built by the parent at the wait site (`src/pipeline/parallel_children.rs:335-343`), through the same one function as every other object.
- **The text fallback only shrinks** (D1): no new pattern in `classify_error`'s string match.
- **The fixtures carry registry text.** `action` in a fixture must equal the registered code's own; a change to a code's action changes the fixture in the same commit, and the test says which.

---

## Enforcement

`tests/offline/scheduler_contract.rs`:

- parses every fixture and checks its exact keys, the type of every value and the closed vocabularies (`class`, the three `status` sets, `stop_reason`), plus the rules between fields (`error` iff failed, `skip_reason` iff skipped, `crashed` iff no exit code, `exit_class` against `exit_code`, the folded `error` text against `failures[]`); a table of deliberately wrong values (`stop_reason: "whatever"`, `rows: "many"`, `exit_class: "refusal"`, a key called `unit`) must each be refused;
- compares with the product where the product already produces part of a shape: `code`, `kind` and `action` must be a registered code's own values; `exit_code` must be a real `ExitClass`; the run entry must be what `RunAggregateEntry` serializes, key for key and JSON type for JSON type, plus the keys in `NOT_YET_EMITTED`; the `--json-errors` line of the real binary must carry an integer `exit_class` equal to its exit status and only contract keys, the rest being `JSON_ERRORS_NOT_YET_EMITTED`; the fold texts, the killed-child text and the compact skip reason in the fixtures must be literals of the source files that emit them;
- requires this document to link every fixture by a relative path that resolves to a file of the fixture directory, and nothing else there.

Both pending lists are shrink-only: each must equal the contract keys the product does not emit (so a key that starts being emitted must be removed), each has a ceiling in the test that is its size at this ADR's date, and each is a ratchet pin the PR rules compare with the base branch. A change that emits a different shape changes the fixture, the test and this ADR in the same commit.

## Consequences

- A scheduler needs no table of exit codes and no parser of rivet's prose.
- The uncoded-`bail!` ceiling is now visible to integrations as `code: null`; every new failure introduced by the implementing changes is coded.
- Until the lease ships, overlapping cycles of one CDC stream remain unguarded on every engine.
- CDC on ephemeral workers waits for the checkpoint to move into the state database; until then it is refused there, not degraded.
- A deployment with PostgreSQL state and a relative `cdc.checkpoint` has to act before upgrading to the release that carries the refusal.
- An exit-code-only consumer (cron) still sees 1 for a run whose only failure was a killed child: the code did not change, the `retryable: true` is in the object.
