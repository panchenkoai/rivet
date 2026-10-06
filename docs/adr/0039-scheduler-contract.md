# ADR-0039: The Scheduler Contract — Machine Errors, State Location, and the Run Lease

- **Status:** Accepted (contract only). Nothing below is implemented at this ADR's date except where a row says "today"; the JSON shapes are pinned by fixtures so each implementing change must match them or change the fixture, the test and this document together.
- **Date:** 2026-10-06
- **Amends:** [ADR-0026 — First-party extension seam](0026-first-party-extension-seam.md) (where orchestration, alerting and the warehouse load sit)
- **Context:** A scheduler (Airflow first; cron and any other orchestrator by the same means) has to decide three things about a rivet process without reading its prose: *retry or stop*, *which unit failed and what to do about it*, and *whether another run of the same unit is in flight*. At the tree this ADR was last checked against (`origin/main` on 2026-10-06, after #441) the product cannot answer them:
  - `--json-errors` prints `{"error", "exit_class", "code"?}` for ONE representative failure; `exit_class` is the integer exit code (`src/cli/mod.rs:34-41`). N failed units fold to one error at three sites (`fold_failures`, `src/pipeline/run.rs:1116`; `aggregate_load_failures`, `src/load/orchestrate.rs:366`; `aggregate_child_result`, `src/pipeline/parallel_children.rs:502-511`).
  - A failed export keeps only redacted text; the typed error is in scope and dropped (`src/pipeline/job.rs:1406-1410`), so the run aggregate entry carries `error_message` and nothing a machine can branch on (`RunAggregateEntry`, `src/state/run_aggregate.rs:41-55`).
  - The CDC drain knows it stopped on `max_events` (`hit_max`, `src/source/cdc/sink.rs:598`) and its return type, `(Vec<RunManifest>, Result<()>)` (`src/source/cdc/sink.rs:438-441`), cannot say so. A multi-table stream's summary sums the per-table manifests away (`src/pipeline/cdc_job.rs:262-263`).
  - SQLite state is always `<config dir>/.rivet_state.db` (`src/state/mod.rs:52`, `:293-296`); `RIVET_STATE_URL` is honoured only with a `postgres` prefix (`src/state/mod.rs:235-236`). No record says which source a state DB as a whole belongs to, and an absent one is created empty without a word.
  - A lease exists (`src/state/load_lease.rs:5-9`) and never waits (`try_load_lease`, `src/state/load_lease.rs:129`). Its three holders are a checkpointed chunk run (`src/pipeline/chunked/mod.rs:310`), a table load (`src/load/orchestrate.rs:454`) and staging (`src/load/staging.rs:264`). No runtime guard stops two overlapping cycles of one CDC stream on any engine.
  - A CDC checkpoint is a JSON file and nothing else: `Position::save` writes it by temp file and rename (`src/source/cdc/mod.rs:146`), `Position::load` reads it (`:118`), and a relative `cdc.checkpoint` resolves beside the config (`resolve_checkpoint`, `src/source/cdc/mod.rs:1894`). No state table holds a stream position on any backend. An absent file reads as a first run (`Ok(None)`, `:132`), so a stream whose file was lost re-anchors.
  - A child process killed by a signal has no exit code; the parent records the text `exited with status signal` (`src/pipeline/parallel_children.rs:334-344`). When no other child reported a code, the fold returns a bare untyped error (`aggregate_child_result`, `:502-511`), so the parent's exit code is re-derived from the fold's TEXT, and that text contains the export names: it is 1, unless a name matches a pattern of the transient fallback (`dns`, `timeout`; `src/pipeline/retry.rs:225-231`, `:269-270`), in which case it is 2. A killed child of an export called `dns_events` or `orders_timeout` makes the parent exit 2 today.
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
| `exit_code` | integer | `classify_exit` (`src/error.rs:283-297`) | the unit's process ended on a signal, or the integration saw only a kill (D8) |
| `class` | string, closed set | the name of `exit_code`'s `ExitClass` (`src/error.rs:34-53`): `generic`, `retryable`, `data_integrity`, `schema_drift`, `refusal`, `internal`; `crashed` when there is no exit code | never |
| `retryable` | boolean | `true` for `class` `retryable` (`exit_code == ExitClass::Retryable`, `src/error.rs:293-295`) and for `crashed`; `false` otherwise | never |
| `message` | string | `redact_error` then `sanitize_terminal`, as the text line is built today (`src/cli/mod.rs:33`) | only in an object an integration built from a bare exit code 1-6 (D8); rivet itself never writes `null` here |

`retryable` means: **a re-run without human action may succeed.** It does not mean "retry without limit"; D8 says how many times.

`ExitClass` has no name function today; the implementing change adds ONE beside `ExitClass::code` (`src/error.rs:57`) and it is the only place the six names are written in the product.

**`crashed`.** A unit whose process was killed by a signal produced no exit code and no error from inside. Its object is `class: "crashed"`, `exit_code: null`, `retryable: true`, and `code`, `kind`, `action` `null`. It is retryable because rivet resumes from its state: a chunked run continues from its checkpoint, a CDC drain re-reads the un-acknowledged span, load and compact are driven by the ledger. *Rejected: `retryable: false` with the retry rule living in the scheduler's table of classes* — it makes `retryable` mean two things and sends every consumer back to a table this object exists to remove. *Rejected: reporting a killed unit as `generic`*, which is what happens today: it reads as "fix the input" for a failure no input caused.

**What must not get a second classifier.** One function in `src/error.rs` turns an `anyhow::Error` into this object, and every emitter (text line, `--json-errors`, run aggregate, process-child event, load and compact results, notifications) calls it. The text-derived labels of `classify_error_message` (`src/pipeline/job.rs:20`, stored as `export_metrics.error_class`) are a grouping aid for humans and are not part of this contract.

**What is true about text today, and what the contract requires.** The `retryable` class is NOT free of text matching in the product: `classify_exit` asks `classify_error` (`src/error.rs:293`), which tries typed driver errors first and then falls back to matching the lowercased message (`src/pipeline/retry.rs:184-185`). So today's `exit_code: 2` can be the result of a substring match. The contract requires three things going forward: (1) a CONSUMER never matches text, it reads `retryable`; (2) the object is derived only from `classify_exit` and the registry, never from a second look at the message; (3) the text fallback only shrinks: a failure introduced by an implementing change is classified by a typed error or a registered code, and no new pattern is added to the fallback.

Honest limit: 419 `bail!` sites are uncoded (`tests/offline/error_code_ratchet.rs:7`), so `code`, `kind` and `action` are often `null`. `class` and `retryable` always exist.

### D2 — `--json-errors`: every failed unit, the integer `exit_class` kept

One line on stderr, as today. Fixtures: [`json_errors.json`](../../tests/fixtures/scheduler/json_errors.json) (two failed exports in one process, the representative one coded), [`json_errors_uncoded.json`](../../tests/fixtures/scheduler/json_errors_uncoded.json), [`json_errors_crashed.json`](../../tests/fixtures/scheduler/json_errors_crashed.json) (one process child, killed), [`json_errors_mixed.json`](../../tests/fixtures/scheduler/json_errors_mixed.json) (two process children: one exited 5, one was killed), [`json_errors_load.json`](../../tests/fixtures/scheduler/json_errors_load.json) (`rivet load`, two failed tables).

| Key | Type | Rule |
|---|---|---|
| `error` | string | **unchanged**: the text the fold emits today (the three folds are listed below) |
| `exit_class` | integer | **unchanged**: the exit code of THIS process (`src/cli/mod.rs:34-41`) |
| `code` | string | **unchanged, including its absence**: the key is OMITTED when the representative failure is uncoded (`src/cli/mod.rs:38-40`); it is never `null` |
| `exit_code` | integer or `null` | new; D1, for the representative failure |
| `class` | string | new; D1 |
| `kind`, `action` | string or `null` | new; always present, `null` when uncoded |
| `retryable` | boolean | new; D1 |
| `failures` | array | new; one entry per failed unit |

So an uncoded failure prints every key except `code`, with `kind` and `action` `null`. `code` is the only key of this contract that may be absent, and only because it already is.

- **The representative failure.** The top-level `code`, `kind`, `class`, `exit_code`, `retryable` and `action` describe ONE entry of `failures[]`: the one with the highest `stop_rank` (`ExitClass::stop_rank`, `src/error.rs:62-71`: data_integrity > internal > refusal > schema_drift > retryable > generic) among the units that reported an exit code; among equals, the last, which is what `max_by_key` returns in `representative_failure_idx` (`src/pipeline/run.rs:4489-4494`). So a `retryable` failure represents a batch whose other failures are `generic`. This is today's choice and does not change.
- **`crashed` is the top-level class only when no failed unit reported an exit code.** When at least one did, the representative is chosen among those as above, and a killed child appears only in `failures[]`, with `class: "crashed"` ([`json_errors_mixed.json`](../../tests/fixtures/scheduler/json_errors_mixed.json)). *Rejected: a killed child outranking every class* — a refusal or an integrity failure in the same batch is the more specific instruction, and `worst_exit_code` (`src/pipeline/parallel_children.rs:514-521`) already ignores a child with no code.
- `exit_code` equals `exit_class` for every class except `crashed`, where `exit_code` is `null` and `exit_class` is 1. This is a REQUIREMENT on two of the three folds, not today's behaviour everywhere:
  - `fold_failures` satisfies it today: it attaches the representative's class as a typed `PreclassifiedExit` (`src/pipeline/run.rs:1135-1137`).
  - `aggregate_load_failures` does NOT: it attaches no class, so the exit code is re-derived from the folded text, which includes the `(also: …)` part. A representative `connection reset` alone exits 2; folded with another table's `permission denied` (a permanent pattern checked first, `src/pipeline/retry.rs:196`) the same batch exits 1. **Requirement on the implementing change: the load fold carries the representative's class**, as `fold_failures` does. If that adds an `exit class <n>:` segment to the load fold's text, the fixture, the test and this section change in that same commit, as a declared text change.
  - `aggregate_child_result` does not when no child reported a code (Context): the exit follows the export names. **Requirement on the implementing change: the exit class of a child-failure fold comes from the children's own classes, never from text that contains export names.** A fold in which no child reported a code exits 1 whatever the exports are called.
- `failures[]`: each entry is the D1 object plus `export` (string) and `table` (string, or `null` for `run` and `apply`). `rivet load` and `rivet compact` fill both, per D0. `[]` when the failure belongs to no unit (a config that does not parse). The list is collected at the three folds BEFORE they flatten. A process child's entry is the object the child itself reported (D3); a killed child's entry is the one the parent builds.
- `error` is the text of whichever fold ran. There are three, and each keeps its text:

  | Fold | When | Text for N ≥ 2 | Text for N = 1 |
  |---|---|---|---|
  | `fold_failures` (`src/pipeline/run.rs:1116-1143`) | exports run inside one process | `N export(s) failed<context>; representative error follows (also: <the others, joined by "; ">): exit class <n>: <the representative>` | that unit's `message` |
  | `aggregate_load_failures` (`src/load/orchestrate.rs:366-384`) | `rivet load` | `N load(s) failed; representative error follows (also: …): <the representative>` (`:380-383`) | that unit's `message` |
  | `aggregate_child_result` (`src/pipeline/parallel_children.rs:502-511`) | exports run as process children | `export 'a' exited with status 5; export 'b' exited with status signal: exit class 5` | `export 'a' exited with status 5: exit class 5`, or `export 'b' exited with status signal` |

  The `exit class <n>` segment is the display of `PreclassifiedExit` (`src/error.rs:150-154`): `fold_failures` puts it before the representative, the child fold makes it the root of the chain, so it comes last, and only when a child reported a code. The child fold names no representative text at all: it lists every child by name and status, where the status is the child's raw exit status (a panicked child shows `101` there and is classed `internal`, 6, by `worst_exit_code`).

**A killed child.** O1 of the review, decided: **this contract does not change the parent's exit code.** The top-level object carries `class: "crashed"`, `retryable: true`, `exit_code: null`; an integration acts on the JSON. A consumer that reads only the exit code sees a non-retry code and **will not retry** a run whose only failure was a kill. *Rejected: the parent exiting 2 for a killed child* — it changes an exit code under every existing consumer, and puts an out-of-memory kill under the transient budget D8 keeps it out of. What the parent's exit code IS today depends on the export's name (Context); the requirement above makes it 1.

A killed child's `message` is the text the parent stores for it, `exited with status signal`, with no export prefix (`wait_failures`, `src/pipeline/parallel_children.rs:344`); the top-level `error` is the fold's text, which has the prefix.

**Objects the integration builds.** When rivet ended without printing an error object, nobody inside can say why, and the integration builds the object from the table in D8 ([`exit_without_object.json`](../../tests/fixtures/scheduler/exit_without_object.json)); a process that ended on a signal seen directly is exactly [`error_object_crashed.json`](../../tests/fixtures/scheduler/error_object_crashed.json), with the signal number in `message`:

```json
{ "code": null, "kind": null, "class": "crashed", "exit_code": null,
  "retryable": true, "action": null, "message": "rivet was killed by signal 9" }
```

When rivet did print its line, the integration reads it and adds nothing.

*Rejected: giving `exit_class` the class name.* Every consumer that compares it with a number would break; the name gets its own key. *Rejected: `"code": null` for an uncoded failure.* A consumer that tests for the key's presence today would start seeing a coded failure with no code.

### D3 — Per-unit results carry the error object

Fixtures, each one `per_export[]` entry of the run aggregate (`--json`, `--summary-output`, `details_json`): [`run_entry.json`](../../tests/fixtures/scheduler/run_entry.json) (a successful CDC entry that stopped on `max_events`), [`run_entry_caught_up.json`](../../tests/fixtures/scheduler/run_entry_caught_up.json) (a CDC drain that delivered nothing), [`run_entry_skipped.json`](../../tests/fixtures/scheduler/run_entry_skipped.json) (a skipped incremental run), [`run_entry_failed.json`](../../tests/fixtures/scheduler/run_entry_failed.json) (failed, with the full `error` object), [`run_entry_crashed.json`](../../tests/fixtures/scheduler/run_entry_crashed.json) (a child killed by a signal).

`RunAggregateEntry` (`src/state/run_aggregate.rs:41-55`) gains three members and renames none; `export_name` and `error_message` stay. Every key is always written, `null` when it does not apply; an entry written by an older binary reads back with the new members absent.

| Key | Type | Values |
|---|---|---|
| `export_name`, `run_id`, `mode`, `error_message` | string (`error_message`: or `null`) | as today |
| `status` | string, closed set | `success`, `failed`, `skipped` |
| `rows`, `files`, `bytes`, `bytes_read`, `duration_ms` | non-negative integer | as today |
| `error` | object or `null` | the D1 object for every `failed` entry on every path (sequential, threads, waves, pool, and process children through a defaulted member on `ChildEvent::Finished`, `src/pipeline/ipc.rs:104-121`); `null` otherwise |
| `stop_reason` | string or `null`, closed set | D4 |
| `tables` | array or `null` | D4 |

`interrupted` is NOT a status of a run entry: no writer of an aggregate entry can produce it (`entry_from_summary`, `src/pipeline/aggregate.rs:101-114`, copies the summary's status; the two parent-side writers, `:580-591` and `:604-620`, copy a finished metric row or write `failed`). The word exists in `run_status` and in a manifest's status, which this contract does not carry. *Rejected: listing it "for completeness"* — a consumer would write a branch nothing reaches.

**A killed child's entry is a TARGET shape.** A child that was killed never sends `Finished`; the parent writes its entry (`src/pipeline/aggregate.rs:604-620`) with `status: "failed"`, the D1 `crashed` object, and as `error_message` and `error.message` the text it stores today, `exited with status signal`. Today that entry has `run_id: ""`, `mode: ""` and `duration_ms: 0`. The implementing change must fill **`run_id` and `mode`** from the child's `ChildEvent::Started` (`src/pipeline/ipc.rs:83-89`), which carries both; they stay `""` only for a child killed before it sent `Started`. `duration_ms` and the counts stay 0: the child reported none. The test lists the two members as not yet filled, and the list must shrink when the parent fills them.

`rivet load` and `rivet compact` each print one object: `run_id` and `per_table[]` with `export`, `table`, `status`, `skip_reason`, `rows`, `error`. The two commands have different closed sets of `status`:

| Command | Fixture | `status` |
|---|---|---|
| `rivet load` | [`load_result.json`](../../tests/fixtures/scheduler/load_result.json) | `loaded`, `skipped`, `failed` |
| `rivet compact` | [`compact_result.json`](../../tests/fixtures/scheduler/compact_result.json) | `compacted`, `nothing_to_do`, `skipped`, `failed` |

`error` is the D1 object iff `status` is `failed`. `skip_reason` is non-null iff `status` is `skipped`. A load that finds nothing to load is `skipped` with the `skip_reason` `up_to_date`, the word a user already reads (`LOAD SKIP [<table>]: up to date`, `src/load/orchestrate.rs:164`, `:187`, `:208`); it is the load's only skip reason, a closed set of one. *Rejected: a new status `nothing_to_do` for the load* — a second word for what the product already calls a skip. A compact skip's `skip_reason` is the text the product's own predicate returns (`compact_skip_reason`, `src/load/compact.rs:96`), which a scheduler shows and never re-derives.

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
| **Ephemeral pod** (Kubernetes) | PostgreSQL, REQUIRED | The integration refuses to start a task with SQLite state. Nothing REQUIRED FOR CORRECTNESS OR RESUME may live on the pod's disk: no state and no checkpoint. The diagnostic files rivet writes beside the config (D9) do live there and are lost with the pod. |
| **Local or long-lived worker** | SQLite in a declared directory on a host-mounted volume, or PostgreSQL | SQLite is allowed; the integration prints a warning that the setup is single-host. |

- rivet cannot tell a pod from a host, so the pod rule is enforced by the integration, which knows how it launches the task. *Rejected: letting SQLite state run on a pod with a warning* — the first task succeeds and every later one starts from an empty state, which for incremental and CDC exports is silent re-reading or silent loss.
- **The local worker gets a setup guide, and the guide is part of the contract's delivery.** The integration's documentation carries a page for the single-host setup that states: where the declared directory must be mounted (a host path or named volume, never the container's own filesystem); that every task of every DAG that touches the pipeline must see the SAME absolute path; what happens when it does not (a task that sees an empty directory creates a new state and treats every export as a first run, and the D6 refusal does not fire because an empty state has no identity); and how to move to PostgreSQL state. The warning the integration prints names that page.
- **CDC checkpoints.** A checkpoint is a file on every state backend today (Context). Under `sqlite:<directory>` a relative `cdc.checkpoint` resolves under the declared directory through `resolve_checkpoint` (`src/source/cdc/mod.rs:1894`), the function `doctor` also uses; a checkpoint found beside the config is still used, with a warning, as the working-directory fallback is today.
- **Under PostgreSQL state a RELATIVE `cdc.checkpoint` gets a WARNING; the run proceeds.** O3 of the review, decided. The warning is printed once per CDC export at run start and states: the export; the absolute path the checkpoint resolves to; that this file is on the worker's own disk while the state is in PostgreSQL; that if the file is lost the next run reads as a first run, the stream re-anchors, and the changes between the lost position and the new anchor are gone without an error; and the remedy, an absolute `cdc.checkpoint` on storage that outlives the worker. It adds no error code and changes no exit code. *Rejected: a refusal with its own code.* rivet cannot tell an ephemeral worker from a long-lived host: no signal of ephemerality exists under `src/`. PostgreSQL state with a relative checkpoint on a long-lived host is a setup that works today, and a refusal would break it on upgrade to prevent a failure only the pod case has. The party that knows what the worker is refuses instead (next point).
- **On a pod the integration REFUSES every CDC export**, and says that the checkpoint would live on the pod's disk. It can know that it launches a pod; rivet cannot. The rule covers all engines until the planned change below lands.
- **Target, recorded as a planned change: under PostgreSQL state the CDC checkpoint is stored IN the state database**, so a pod needs no volume at all.
- **Open follow-up, to verify, not a promise: PostgreSQL CDC on pods may be allowed earlier.** A checkpoint file is mandatory only for MongoDB, MySQL and Oracle (`src/config/cdc.rs:1009-1013`); a PostgreSQL stream's position is held by its server-side slot. Whether a PostgreSQL CDC run whose checkpoint file did not survive loses and duplicates nothing has to be proven live before the pod rule is relaxed for it; until then the refusal stands for PostgreSQL too.

*Rejected: a `--state-dir` flag.* It would have to be forwarded to every child process and carried by a process-global to the 44 `StateStore::open(` occurrences under `src/`. *Rejected: a `state locate` subcommand.* The caller SET the directory; it has nothing to ask.

### D6 — State identity and the `RIVET_STATE_FOREIGN` refusal

Two derivations in the tree answer "which source", and they are not the same thing:

| Name | Shape | Computed from | Answers |
|---|---|---|---|
| `SourceConfig::state_key` (`src/config/source.rs:415`; `source_state_key`, `:606`) | `engine://host:port/database` | the source URL, credentials and parameters dropped | which DATABASE a state belongs to |
| `source_ident` (`identity_source`, `src/manifest.rs:144-150`; column `loaded_source_run.source_ident`, `src/state/migrations.rs:360`, read by `loaded_source_idents`, `src/state/load_journal_store.rs:124`) | `engine:schema.table` | a run manifest | which TABLE the rows of one warehouse table came from |

- **The state identity is `state_key`.** One state migration adds the record. `source_ident` keeps its job, the load's ownership guard (`src/load/orchestrate.rs:708-723`), and is not the identity: it carries no host and no database, so two databases with the same schema would collide in it, and it exists per warehouse table, not per state. Nothing is derived from one into the other. *Rejected: building the identity from `source_ident` or from `run_aggregate.config_path` (`src/state/migrations.rs:116`)* — the first cannot tell two servers apart, the second changes when a file is moved.
- The claim is made once, in `dispatch` (`src/cli/dispatch.rs:67`), before any command body; the `StateStore::open(` call sites keep their signature.
- **Refusal: only when BOTH keys are non-empty and differ.** A state found in a declared `sqlite:` directory whose recorded key is non-empty, read by a command whose own key is non-empty and different, is refused with `RIVET_STATE_FOREIGN` (kind `refusal`, exit 5): the message names the directory and the other source, the action is "choose another state directory". It is checked BEFORE migrating, so a refused file is byte-identical afterwards.
- **A command that cannot compute its key skips the check.** `state_key` is the empty string when the source URL cannot be resolved (`unwrap_or_default`, `src/config/source.rs:416-418`). `rivet load` and `rivet compact` never connect to the source and may run where its URL is not in the environment at all; they compare nothing and stamp nothing. *Rejected: refusing when the key cannot be computed* — it would demand source credentials from the one task that must not need them.
- A state with progress and no recorded key (written by an older binary, or touched so far only by commands that skip the check) is adopted, with a warning, by the first command that can compute its key; it is never refused.
- **Scope of the refusal.** Only a declared `sqlite:` directory refuses. A state beside the config is stamped and WARNS on a mismatch, because configs for different sources share a directory today. A PostgreSQL state is shared by design and is not enforced. *Rejected: refusing everywhere* — it would break the supported several-configs-one-directory layout on upgrade.
- **Identity content.** The source key only. Two pipelines on the same database share one state, which is today's supported multi-config case. *Rejected: an explicit pipeline name in the identity* — a new required setting whose only job is to forbid a layout that works.

*Rejected: a sidecar identity file.* It can be lost, or copied apart from the database it describes.

### D7 — The run lease: per export, waited on

- One lease per export, held for the whole run by the process that runs the export, in every mode including the CDC drain. Built on `src/state/load_lease.rs`: an `flock` on a sidecar for SQLite, released by the OS when the holder dies; a heartbeat row with a 30 s TTL and immediate takeover of a dead pid on the same host for PostgreSQL (`src/state/load_lease.rs:36`, `:57-70`).
- **Its key stays `chunk-run:<export>`** (`src/pipeline/chunked/mod.rs:310`). The lease is the old one widened to every mode, not a new one beside it, so an old and a new binary on one state take the SAME key and exclude each other during an upgrade. *Rejected: renaming the key to `run:<export>`* — an old binary would hold `chunk-run:` while a new one held `run:`, and two checkpointed runs of one export would overlap for as long as both versions run. *Rejected: the new binary taking both names for one release* — two leases per run, an ordering rule between them, and a second compatibility event when the old name is dropped, all to correct a string no user reads. The name is a misnomer for a CDC or incremental run and stays one.
- A busy lease is WAITED on: a poll of the existing non-blocking take, with a log line naming the holder, up to `--lock-wait <seconds>` (global flag, default **600**). On timeout: `RIVET_STATE_LOCK_TIMEOUT`, exit 2, `retryable: true`. `--lock-wait 0` keeps the immediate refusal a second chunk run gets today (`live_chunk_run_refusal`, `src/pipeline/chunked/mod.rs:291`). The table lease of the load (`src/load/orchestrate.rs:454`) waits by the same rule.
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
| no object was printed | see the next table | see the next table |

The crashed budget is separate because the commonest cause of a kill is the kernel's out-of-memory killer, and a run that is killed for memory is killed again: under the transient budget it would burn every retry and its backoff on a failure that needs a bigger worker. One retry covers the eviction and the node loss; the second kill fails the task. *Rejected: one shared budget.* It either starves transient errors of retries or lets an out-of-memory loop run to the end of them.

**A non-zero exit with no error object.** O4 of the review, decided. rivet prints no object in four known cases: the argument parser rejected the command line before `--json-errors` could apply (exit 2; `src/cli/args.rs:58`, `:61`); the main thread panicked (exit 101: the `release` profile unwinds, `Cargo.toml:225-235`; only `release-min` sets `panic = "abort"`, `:245`, and a binary built with it dies on a signal instead); the process was killed; or the line was printed and lost on the way. The integration then builds the object from what it saw, by this table and nothing else ([`exit_without_object.json`](../../tests/fixtures/scheduler/exit_without_object.json)):

| What the integration saw | `class` | `exit_code` | `retryable` | `message` | Retry |
|---|---|---|---|---|---|
| exit 1-6 | the class of that code (D1) | that code | `true` for 2 | `null` | 2: transient budget; others: no |
| exit 101 | `internal` | 6 | `false` | names the raw status | no |
| exit 128+n (129-255): a kill reported as a number by a shell or a container runtime, e.g. 137 | `crashed` | `null` | `true` | names the raw status | crashed budget |
| a signal, seen by a direct parent | `crashed` | `null` | `true` | names the signal | crashed budget |
| any other exit status | `generic` | 1 | `false` | names the raw status | no |

`code`, `kind` and `action` are `null` in every built object. `exit_code` stays `null` or 1-6 in a built object as in a printed one, so `class` is always the name of `exit_code`; the raw status that fits neither goes into `message`. Exit 101 maps to 6 as the parent already maps a panicked child (`worst_exit_code`, `src/pipeline/parallel_children.rs:514-521`).

Honest limit: exit 2 with no object is ambiguous between a usage error and a retryable failure whose line was lost, and the table calls it `retryable`. A usage error is therefore retried under the transient budget and then fails. *Rejected: never retrying exit 2 without an object* — a lost line would turn a transient failure into a hard stop, while an integration builds its argv from code and does not produce usage errors after its first run. *Rejected: telling the two apart by reading stderr* — that is the prose parsing this contract removes.


A batch export is one task that plans and applies its sealed plan, so a retry replays the same plan. That holds only if re-applying a sealed plan resumes a failed run; if the implementing change cannot prove it live, the task is `rivet run -e <export>` instead. *Rejected: one task for the whole config with results fanned out to display-only tasks* — they could be neither retried nor cleared.

### D9 — `--no-notify`, and the secrecy rule

- `--no-notify` (global flag, forwarded to children) suppresses every notification rivet would send (`maybe_send`, the three sites in Context). A scheduler integration always passes it and alerts through its own channel, from the D1 object. *Rejected: withholding the webhook variable* — it works for `webhook_url_env` (`src/notify.rs:64-72`) and an inline `webhook_url:` would still fire. No new environment read is added for this.
- **Secrets travel in the process environment and nowhere else.** argv carries the config path, export names and flags.
- **Nothing secret and no error text in a scheduler's metadata store.** Redaction covers credentials only; error text may embed source cell values (`SECURITY.md:38-45`). What an integration may push (Airflow XCom) is the D1 object WITHOUT `message`, plus `export`, `table`, `run_id`, counts and `stop_reason`: fixtures [`xcom_unit.json`](../../tests/fixtures/scheduler/xcom_unit.json) (a run unit) and [`xcom_unit_load.json`](../../tests/fixtures/scheduler/xcom_unit_load.json) (a load unit). One shape for both kinds of unit: `table` is `null` for a run or apply unit and a string for a load or compact unit; `files` is a number for a run unit and `null` for a load or compact unit, whose rows carry no file count; `stop_reason` is `null` outside a run unit; `status` comes from that unit's own set (D3). The exception it raises reads `[code] class: action (export)`.
- The task log gets one structured line per failed unit.
- **Raw stderr is captured by the integration.** rivet writes its stderr to the stream it was given. It also writes two kinds of diagnostic file beside the config, today and unchanged: the captured stderr of process children, `rivet-child-stderr-<timestamp>.log` (`emit_child_stderr`, `src/pipeline/run.rs:220-235`, called at `:678`), and the per-run report under `<config dir>/.rivet/runs/<run id>/` (`report_dir`, `src/pipeline/report.rs:29-31`). Neither is needed for correctness or resume: nothing under `src/` reads them back. On a pod both are lost with the pod. What an operator loses: the full stderr of each child of a parallel run, of which the pod's log stream holds only the parent's one-line pointer to a path that no longer exists, and the run report files. **The integration must not depend on either file and must not send a user to them on a pod**; what it needs is in the D1 objects. On a pod the integration leaves the parent's stderr on the pod's log stream; on a local worker it writes it to a file in the declared state directory and the task log names the path. *Rejected: rivet writing `<state dir>/logs/<run id>.log` itself* — a pod has no such directory, and one more log sink inside the product is one more place for redaction to be wrong. *Rejected: streaming the credential-redacted stderr into the scheduler's task log* — it puts possibly data-bearing text into a store with wider access than the worker.

### D10 — Where the integration lives

Orchestration and alerting integrations, including the operators for `load` and `compact`, are open source and live in this repository as a Python package with its own manifest, lock, version line and release tag prefix, published to PyPI as **`airflow-provider-rivet`**. See the amendment in ADR-0026. Two formatters of a notification, one in Rust and one in Python, are pinned by a shared fixture; *rejected: a `rivet notify` subcommand*, because a scheduler's callback may run where the binary is not installed.

---

## Compatibility

| Surface | Change | Migration / bump |
|---|---|---|
| `--json-errors`, run aggregate JSON, `details_json`, child events | additive members only; `error`, `exit_class` and the presence rule of `code` unchanged | none |
| A child killed by a signal | its entry and, when no failed child reported an exit code, the top-level object say `class: "crashed"`; its entry gains `run_id` and `mode` | the parent's exit code becomes 1 whatever the exports are called. Today it is 1 or, by a text match on an export name, 2: a correction, CHANGELOG |
| `rivet load` with several failed tables | the exit code is the representative failure's class, no longer re-derived from the folded text | a correction, CHANGELOG |
| State schema | the identity record | **one** state migration, the version after the current head (`SCHEMA_VERSION`, `src/state/migrations.rs:8`); older binaries refuse the newer state as they do today |
| `RIVET_STATE_URL` | accepts `sqlite:<dir>` | none |
| Lease key | `chunk-run:<export>` kept, now taken in every mode | none: old and new binaries share the key. An old binary still takes it only for checkpointed chunk runs |
| A second run of one export | waits (bounded) instead of refusing (chunked) or overlapping (every other mode); `--lock-wait 0` restores the refusal | behaviour change, CHANGELOG |
| Staging cleanup under a busy prefix lease | skips, as today, whatever `--lock-wait` says | none |
| PostgreSQL state with a relative `cdc.checkpoint` | a warning line at run start; the run proceeds as today | none |
| CLI | `--no-notify`, `--lock-wait`; `--export`, `--table`, `--json`, `--summary-output` on `load` / `compact`; `--json`, `--summary-output` on `apply` | cli matrices |
| Manifests, part names, checkpoint file format, config schema keys, and every exit code except the two corrections above | untouched | none |

Additive only: no manifest version and no config-schema bump follows from this ADR. Storing the checkpoint in the state database (D5, planned) will need its own state migration and is not part of the one above.

---

## Notes for implementers

- **A new member on `RunAggregateEntry` needs `#[serde(default)]`.** The read path parses `details_json` with `unwrap_or_default()` (`src/state/run_aggregate.rs:103`): one entry that fails to deserialize does not error, it turns the WHOLE `per_export` list of that row into `[]`. A member added without the attribute silently empties every row an older binary wrote. `bytes_read` (`:50`) is the model. The same holds for a new member on `ChildEvent::Finished` (`dest_retries`, `src/pipeline/ipc.rs:119-120`).
- **`code` is omitted, the other keys are `null`.** Do not "tidy" the top-level object into one rule; the asymmetry is the compatibility guarantee.
- **A killed child reports no `Finished` event.** The `crashed` object is built by the parent at the wait site (`src/pipeline/parallel_children.rs:334-344`), through the same one function as every other object.
- **The text fallback only shrinks** (D1): no new pattern in `classify_error`'s string match.
- **The fixtures carry registry text.** `action` in a fixture must equal the registered code's own; a change to a code's action changes the fixture in the same commit, and the test says which.
- **Requirements found in verification.** (1) The exit class of a child-failure fold comes from the children's own classes, never from text that contains export names (D2). (2) The load fold carries the representative's class (D2). (3) A killed child's entry gets `run_id` and `mode` from `ChildEvent::Started` (D3). (4) The representative is the highest `stop_rank` among the units with an exit code; do not "fix" a retryable failure representing a batch of generic ones.

---

## Enforcement

`tests/offline/scheduler_contract.rs`:

- parses every fixture and checks its exact keys, the type of every value and the closed vocabularies (`class`, the three `status` sets, `stop_reason`, the load's `skip_reason`), plus the rules between fields (`error` iff failed, `skip_reason` iff skipped, `crashed` iff no exit code, `exit_class` against `exit_code`, the top level against the highest `stop_rank` in `failures[]`, the `error` text against the fold that built it, `files` against the kind of unit, every row of the no-object table against D8); a table of deliberately wrong values (`stop_reason: "whatever"`, `rows: "many"`, `status: "interrupted"`, a generic representative over a retryable failure, a key called `unit`) must each be refused;
- compares with the product where the product already produces part of a shape: `code`, `kind` and `action` must be a registered code's own values; `exit_code` must be a real `ExitClass`; the run entry must be what `RunAggregateEntry` serializes, key for key and JSON type for JSON type, plus the keys in `NOT_YET_EMITTED`; the `--json-errors` line of the real binary must carry an integer `exit_class` equal to its exit status and only contract keys, the rest being `JSON_ERRORS_NOT_YET_EMITTED`; the fold texts, the killed-child text and the compact skip reason in the fixtures must be literals of the source files that emit them; a killed child's `run_id` and `mode` are listed as not yet filled for as long as the parent writes them blank; and one test runs the product's own `classify_exit` over an untyped child fold and over the load fold's text to show the two behaviours D2 records as today's (it documents them; the requirement is on the folds);
- requires this document to link every fixture by a relative path that resolves to a file of the fixture directory, and nothing else there; requires every word of the `status`, `stop_reason` and load `skip_reason` sets to be shown by a fixture of its family and every such word and every class name to be written here in backticks, so a word renamed in this document alone goes red.

The pending lists (two of keys, one of blank members of a killed child's entry) are shrink-only: each must equal the contract keys the product does not emit (so a key that starts being emitted must be removed), each has a ceiling in the test that is its size at this ADR's date, and each is a ratchet pin the PR rules compare with the base branch. A change that emits a different shape changes the fixture, the test and this ADR in the same commit.

## Consequences

- A scheduler needs no table of exit codes and no parser of rivet's prose.
- The uncoded-`bail!` ceiling is now visible to integrations as `code: null`; every new failure introduced by the implementing changes is coded.
- Until the lease ships, overlapping cycles of one CDC stream remain unguarded on every engine.
- CDC on ephemeral workers waits for the checkpoint to move into the state database; until then the integration refuses it there. A hand-run rivet on a pod is warned about a relative checkpoint under PostgreSQL state, not stopped.
- A deployment with PostgreSQL state and a relative `cdc.checkpoint` on a long-lived host keeps working and sees one warning per CDC export.
- An exit-code-only consumer (cron) does not retry a run whose only failure was a killed child: the exit code is not a retry signal there, the `retryable: true` is in the object.
