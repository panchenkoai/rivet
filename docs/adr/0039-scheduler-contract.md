# ADR-0039: The Scheduler Contract — Machine Errors, State Location, and the Run Lease

- **Status:** Accepted (contract only). Nothing below is implemented at this ADR's date except where a row says "today"; the JSON shapes are pinned by fixtures so each implementing change must match them or change the fixture, the test and this document together.
- **Date:** 2026-10-06
- **Amends:** [ADR-0026 — First-party extension seam](0026-first-party-extension-seam.md) (where orchestration, alerting and the warehouse load sit)
- **Context:** A scheduler (Airflow first; cron and any other orchestrator by the same means) has to decide three things about a rivet process without reading its prose: *retry or stop*, *which unit failed and what to do about it*, and *whether another run of the same unit is in flight*. At `origin/main` 4ac2c899 the product cannot answer them:
  - `--json-errors` prints `{"error", "exit_class", "code"?}` for ONE representative failure; `exit_class` is the integer exit code (`src/cli/mod.rs:34-41`). N failed exports fold to one error (`fold_failures`, `src/pipeline/run.rs:1116`; `aggregate_load_failures`, `src/load/orchestrate.rs:365`).
  - A failed export keeps only redacted text; the typed error is in scope and dropped (`src/pipeline/job.rs:1406-1410`), so the run aggregate entry carries `error_message` and nothing a machine can branch on (`RunAggregateEntry`, `src/state/run_aggregate.rs:41-55`).
  - The CDC drain knows it stopped on `max_events` (`hit_max`, `src/source/cdc/sink.rs:594`) and its return type, `(Vec<RunManifest>, Result<()>)` (`src/source/cdc/sink.rs:433-436`), cannot say so. A multi-table stream's summary sums the per-table manifests away (`src/pipeline/cdc_job.rs:262-263`).
  - SQLite state is always `<config dir>/.rivet_state.db` (`src/state/mod.rs:52`, `:296-299`); `RIVET_STATE_URL` is honoured only with a `postgres` prefix (`src/state/mod.rs:234-241`). No record says which source a state DB as a whole belongs to, and an absent one is created empty without a word.
  - A lease exists (`src/state/load_lease.rs:5-9`) and never waits (`try_load_lease`, `src/state/load_lease.rs:129`). Its three holders are a checkpointed chunk run (`src/pipeline/chunked/mod.rs:310`), a table load (`src/load/orchestrate.rs:453`) and staging (`src/load/staging.rs:264`). No runtime guard stops two overlapping cycles of one CDC stream on any engine.
  - Slack fires per export from three sites (`src/pipeline/job.rs:1560`, `:1669`; `src/pipeline/cdc_job.rs:320`), so a scheduler with its own alerting gets every failure twice.

  The existing recipe works around all of it with a blanket retry count, the state URL on the command line and a chunk reset before each retry (`docs/recipes/airflow/`). This ADR fixes the contract that removes those workarounds. It decides shapes, names and sources of truth; it adds no code.

---

## Decision

### D1 — One error object, one producer

```json
{ "code": "RIVET_SOURCE_CDC_LOG_GAP", "kind": "refusal", "class": "refusal", "exit_code": 5,
  "retryable": false, "action": "<registry action>", "message": "<redacted text>" }
```

Fixture: [`tests/fixtures/scheduler/error_object.json`](../../tests/fixtures/scheduler/error_object.json). Every key is always present; a value that does not apply is `null`.

| Field | Single source of truth | `null` when |
|---|---|---|
| `code`, `kind`, `action` | the `Code` registry (`codes::ALL`, `src/error.rs:529`; `Code { id, kind, action }`, `src/error.rs:189`) reached by the downcast `error_code` performs (`src/error.rs:244`); `kind` is `ErrorKind::name` (`src/error.rs:176`) | the failure is uncoded |
| `exit_code` | `classify_exit` (`src/error.rs:283-297`) | the unit's process was killed by a signal |
| `class` | the name of `exit_code`'s `ExitClass` (`src/error.rs:34-53`): `generic`, `retryable`, `data_integrity`, `schema_drift`, `refusal`, `internal`; `crashed` when there is no exit code | never |
| `retryable` | `exit_code == ExitClass::Retryable` (`src/error.rs:293-295`) | never (`false` for `crashed`: see D8 for what a scheduler does with it) |
| `message` | `redact_error` then `sanitize_terminal`, as the text line is built today (`src/cli/mod.rs:33`) | never |

`ExitClass` has no name function today; the implementing change adds ONE beside `ExitClass::code` (`src/error.rs:57`) and it is the only place the six names are written in the product.

**What must not get a second classifier.** One function in `src/error.rs` turns an `anyhow::Error` into this object, and every emitter (text line, `--json-errors`, run aggregate, process-child event, load and compact results, notifications) calls it. The text-derived labels of `classify_error_message` (`src/pipeline/job.rs:20`, stored as `export_metrics.error_class`) are a grouping aid for humans: they are not part of this contract and no retry decision is derived from text. A consumer reads `retryable`; it holds no table of exit codes.

Honest limit: 419 `bail!` sites are uncoded (`tests/offline/error_code_ratchet.rs:7`), so `code`, `kind` and `action` are often `null`. `class` and `retryable` always exist.

### D2 — `--json-errors`: every failed unit, the integer `exit_class` kept

Fixture: [`tests/fixtures/scheduler/json_errors.json`](../../tests/fixtures/scheduler/json_errors.json). One line on stderr, as today.

- `error`, `exit_class`, `code` are **unchanged**: `exit_class` stays the INTEGER exit code, and `code` is still omitted when the representative failure is uncoded (`src/cli/mod.rs:37-39`).
- New, describing the representative failure: `exit_code` (equal to `exit_class`), `class` (the name), `kind`, `retryable`, `action`.
- New: `failures[]`, one entry per failed unit, each the D1 object plus the unit's name under `export` (`run`, `apply`) or `table` (`load`, `compact`). `[]` when the failure belongs to no unit (a config that does not parse). The list is collected at the two folds named in Context BEFORE they flatten; which failure is representative, and therefore the process exit code, does not change.

*Rejected: giving `exit_class` the class name.* Every consumer that compares it with a number would break; the name gets its own key.

### D3 — Per-unit results carry the error object

Fixture: [`tests/fixtures/scheduler/run_entry.json`](../../tests/fixtures/scheduler/run_entry.json) — one `per_export[]` entry of the run aggregate (`--json`, `--summary-output`, `details_json`).

`RunAggregateEntry` (`src/state/run_aggregate.rs:41-55`) gains three members and renames none; `error_message` stays. Every key is always written, `null` when it does not apply; an entry written by an older binary reads back with the new members absent.

- `error`: the D1 object for every `failed` entry on every path (sequential, threads, waves, pool, and process children through a defaulted member on `ChildEvent::Finished`, `src/pipeline/ipc.rs:104-121`); `null` otherwise.
- `stop_reason`, `tables`: D4.

`rivet load` and `rivet compact` print one object (fixture: [`tests/fixtures/scheduler/load_result.json`](../../tests/fixtures/scheduler/load_result.json)): `run_id` and `per_table[]` with `export`, `table`, `status` (`loaded`, `compacted`, `nothing_to_do`, `skipped`, `failed`), `skip_reason`, `rows`, `error`. `skip_reason` is the text the product's own predicate returns (`compact_skip_reason`, `src/load/compact.rs:96`); a scheduler shows it and never re-derives the rule that produced it.

### D4 — CDC: `stop_reason` and per-table `tables[]`

- `stop_reason`, on a successful `mode: cdc` entry only: `caught_up` or `max_events`. Its single source of truth is `hit_max` (`src/source/cdc/sink.rs:594`) threaded out of `run_to_files`; it is never inferred from row counts or from comparing a count with the cap. The exit code is 0 for both.
- `tables[]`, CDC only: `{ table, rows, files }` per captured table, from the per-table manifests the drain already returns. Display data. **It must not gate the load**: the load consumes what the LEDGER says is unloaded, which includes parts from an earlier cycle whose load failed; a scheduler that skipped a table's load because "this run delivered 0" would strand them until the table next changes.
- A run that stops on `max_events` is reported and nothing more. *Rejected for now: the scheduler integration re-triggering itself until `caught_up`* — a loop driven by a field this ADR has only just defined; it can be added on top of `stop_reason` without changing it.

### D5 — State location: `RIVET_STATE_URL=sqlite:<directory>`

| Value | Where the state is |
|---|---|
| unset | `<config dir>/.rivet_state.db`, as today |
| `postgres…` | that database, as today |
| `sqlite:<absolute directory>` | `<directory>/.rivet_state.db` — the file keeps its name |

- Source of truth: the ONE existing environment read in `StateStore::open` (`src/state/mod.rs:234-241`) gains a value form. No new environment read, and child processes inherit it. The second resolver, `state_db_path` (`src/state/mod.rs:326`), which feeds messages, must answer through the same function, or a message names a file rivet did not open.
- A relative `cdc.checkpoint` resolves under the declared directory through `resolve_checkpoint` (`src/source/cdc/mod.rs:1829`), the function `doctor` also uses. An existing checkpoint beside the config is still found, with a warning, as the working-directory fallback is today. CDC checkpoints are files on every state backend, so a CDC stream needs the directory even with Postgres state.
- `.rivet/runs` and `.rivet/spill` stay beside the config.
- The deployment declares one directory per pipeline on a volume every worker mounts. rivet cannot know that a directory is such a mount; the integration checks what it can (exists, writable) and the deployment owns the rest.

*Rejected: a `--state-dir` flag.* It would have to be forwarded to every child process and carried by a process-global to the 44 `StateStore::open(` call sites. *Rejected: a `state locate` subcommand.* The caller SET the directory; it has nothing to ask.

### D6 — State identity and the `RIVET_STATE_FOREIGN` refusal

- The state records the source it belongs to: the credential-free key `SourceConfig::state_key` already computes (`src/config/source.rs:411`; `source_state_key`, `:602`: engine, host, port, database). One state migration adds the record. It must reuse that derivation and reconcile with the two identity-bearing columns the state already has (`run_aggregate.config_path`, `src/state/migrations.rs:116`; `loaded_source_run.source_ident`, `:360`, read by `loaded_source_idents`, `src/state/load_journal_store.rs:124`): a second, differently-derived notion of "the same source" is the defect this decision exists to prevent.
- The claim is made once, in `dispatch` (`src/cli/dispatch.rs:67`), before any command body; the `StateStore::open(` call sites keep their signature.
- **Refusal:** a state found in a declared `sqlite:` directory whose identity names ANOTHER source is refused with `RIVET_STATE_FOREIGN` (kind `refusal`, exit 5): the message names the directory and the other source, the action is "choose another state directory". It is checked BEFORE migrating, so a refused file is byte-identical afterwards, and it holds for every subcommand. The same pipeline finds its own identity and proceeds.
- A state with progress and no identity (written by an older binary) is adopted with a warning; it is never refused.
- **Scope of the refusal.** Only a declared `sqlite:` directory refuses. A state beside the config is stamped and WARNS on a mismatch, because configs for different sources share a directory today. A Postgres state is shared by design and is not enforced. *Rejected: refusing everywhere* — it would break the supported several-configs-one-directory layout on upgrade.
- **Identity content.** The source key only. Two pipelines on the same database share one state, which is today's supported multi-config case. *Rejected: an explicit pipeline name in the identity* — a new required setting whose only job is to forbid a layout that works.

*Rejected: a sidecar identity file.* It can be lost, or copied apart from the database it describes.

### D7 — The run lease: per export, waited on

- Key `run:<export>`, held for the whole run by the process that runs the export, in every mode including the CDC drain. It replaces the `chunk-run:<export>` key (`src/pipeline/chunked/mod.rs:310`). Built on `src/state/load_lease.rs`: an `flock` on a sidecar for SQLite, released by the OS when the holder dies; a heartbeat row with a 30 s TTL and immediate takeover of a dead pid on the same host for Postgres (`src/state/load_lease.rs:36`, `:57-70`).
- A busy lease is WAITED on: a poll of the existing non-blocking take, with a log line naming the holder, up to `--lock-wait <seconds>` (global flag, default **600**). On timeout: `RIVET_STATE_LOCK_TIMEOUT`, exit 2, `retryable: true`. `--lock-wait 0` keeps the immediate refusal a second chunk run gets today (`live_chunk_run_refusal`, `src/pipeline/chunked/mod.rs:291`). The table lease of the load waits by the same rule.
- `--lock-wait` must reach process children, which take the leases; their argv is a hand-kept list (`src/pipeline/parallel_children.rs:150-173`). The implementing change derives "every global flag is forwarded or declared parent-only" from the clap definition instead of extending the list by hand.
- A crashed checkpointed run is still resumed by the next process that takes the lease; a scheduler never resets chunk state before a retry.

*Rejected: a lease per config.* Several exports of one config run at once as separate tasks; the unit every state row is already keyed by is the export. *Rejected: refusal as the default with waiting opt-in.* Two triggers of one schedule are an ordinary event, and a refusal turns it into a failed task that someone has to read. *Rejected: scheduler-only means (pools, one active run).* They see nothing started from cron or by hand. *Rejected: scanning for a process.* Blind across hosts.

**Documented limits.** The lease lives in the state, so two hosts with two separate SQLite files do not see each other; it does not fence a holder that stalls past the Postgres TTL (logged only, `src/state/load_lease.rs:81`); and `flock` on a network filesystem is only as good as that filesystem. Fencing across hosts is [ADR-0032](0032-fenced-checkpoint-journal.md), which stays proposed. No engine supplies a runtime guard of its own for two overlapping cycles of one stream, so the lease is the guard and each engine's proof must go red without it.

### D8 — What a scheduler does with the object

| `class` | Retry |
|---|---|
| `retryable` (includes the lease timeout) | yes |
| `crashed` | yes — at-least-once: a chunked run resumes, CDC re-reads the un-acked span, load and compact are ledger-driven |
| `generic`, `data_integrity`, `schema_drift`, `refusal`, `internal` | no |
| exit 2 with no rivet error line (a command-line usage error from the argument parser) | no |

A batch export is one task that plans and applies its sealed plan, so a retry replays the same plan. That holds only if re-applying a sealed plan resumes a failed run; if the implementing change cannot prove it live, the task is `rivet run -e <export>` instead. *Rejected: one task for the whole config with results fanned out to display-only tasks* — they could be neither retried nor cleared.

### D9 — `--no-notify`, and the secrecy rule

- `--no-notify` (global flag, forwarded to children) suppresses every notification rivet would send (`maybe_send`, the three sites in Context). A scheduler integration always passes it and alerts through its own channel, from the D1 object. *Rejected: withholding the webhook variable* — it works for `webhook_url_env` (`src/notify.rs:64-72`) and an inline `webhook_url:` would still fire. No new environment read is added for this.
- **Secrets travel in the process environment and nowhere else.** argv carries the config path, unit names and flags.
- **Nothing secret and no error text in a scheduler's metadata store.** Redaction covers credentials only; error text may embed source cell values (`SECURITY.md:38-45`). What an integration may push (Airflow XCom) is the D1 object WITHOUT `message`, plus unit, `run_id`, counts and `stop_reason`: fixture [`tests/fixtures/scheduler/xcom_unit.json`](../../tests/fixtures/scheduler/xcom_unit.json). The exception it raises reads `[code] class: action (unit)`.
- The task log gets one structured line per failed unit. rivet's raw stderr goes to a file on the state volume (`<state dir>/logs/<run id>.log`) and the task log names the path. *Rejected: streaming the credential-redacted stderr into the task log* — it puts possibly data-bearing text into a store with wider access than the state volume.

### D10 — Where the integration lives

Orchestration and alerting integrations, including the operators for `load` and `compact`, are open source and live in this repository as a Python package with its own manifest, lock, version line and release tag prefix, published to PyPI (the distribution name is not decided here). See the amendment in ADR-0026. Two formatters of a notification, one in Rust and one in Python, are pinned by a shared fixture; *rejected: a `rivet notify` subcommand*, because a scheduler's callback may run where the binary is not installed.

---

## Compatibility

| Surface | Change | Migration / bump |
|---|---|---|
| `--json-errors`, run aggregate JSON, `details_json`, child events | additive members only | none |
| State schema | the identity record | **one** state migration, the version after the current head (`SCHEMA_VERSION`, `src/state/migrations.rs:8`); older binaries refuse the newer state as they do today |
| `RIVET_STATE_URL` | accepts `sqlite:<dir>` | none |
| A second run of a checkpointed chunk export | waits (bounded) instead of refusing; `--lock-wait 0` restores the refusal | behaviour change, CHANGELOG |
| CLI | `--no-notify`, `--lock-wait`; `--export`, `--table`, `--json`, `--summary-output` on `load` / `compact`; `--json`, `--summary-output` on `apply` | cli matrices |
| Manifests, part names, checkpoint file format, exit codes, config schema keys | untouched | none |

Additive only: no manifest version and no config-schema bump follows from this ADR.

---

## Enforcement

`tests/offline/scheduler_contract.rs` parses every fixture and pins each shape's keys. Where the product already produces part of a shape it compares against the product: `code`, `kind` and `action` in every fixture must be a registered code's own values, `exit_code` must be a real `ExitClass`, and the run entry must equal what `RunAggregateEntry` serializes plus the keys listed in `NOT_YET_EMITTED`, a shrink-only list. A change that starts emitting a key must remove it from that list; a change that emits a different shape must change the fixture, the test and this ADR in the same commit.

## Consequences

- A scheduler needs no table of exit codes and no parser of rivet's prose.
- The uncoded-`bail!` ceiling is now visible to integrations as `code: null`; every new failure introduced by the implementing changes is coded.
- Until the lease ships, overlapping cycles of one CDC stream remain unguarded on every engine.
