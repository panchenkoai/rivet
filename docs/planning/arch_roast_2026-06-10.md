# Rivet — архитектурная прожарка и перформанс-ботлнеки

**Дата:** 2026-06-10 · **Коммит:** 7eb2b4a (`feat/ux-clarity`) · ~64k LOC Rust

> ## СТАТУС ИСПРАВЛЕНИЙ (2026-06-10, после прожарки)
>
> Все находки исправлялись по правилу **«докажи через тест → почини → тест зелёный»**.
> 17 RED-тестов (offline-юниты + live против docker-стека) написаны до фиксов, каждый
> падал на предсказанном ассерте; финальное состояние — **1480 offline + все live зелёные**.
>
> **Wave 1 (CRITICAL + HIGH correctness) — закрыто:**
> - 🟥 **Потеря данных `max_file_size`+chunked/keyset** — новый seam `commit::write_sink_parts`
>   дренирует все ротированные части во всех 5 раннерах. RED: 2000/2000 и 1000/1000 строк доезжают.
> - `rivet validate` exit 0 при нечитаемом манифесте → гейт на `has_failures()`.
> - NULL-ы как дубликаты в uniqueness-гейте → per-column non-null счётчик.
> - CSV-валидация: стриминговый RFC-4180 счётчик вместо `read_to_string`+физ.строки.
> - MSSQL rescale lossy → громкий `Err`. keyset виден правилам плана. GCS token refresh-loader.
>   init PG через `connect_client` (sslmode + один клиент).
>
> **Wave 2 (MEDIUM correctness) — закрыто:** PG JSON raw-bytes (без serde round-trip),
> metric ordering (`record_metric` после `finalize_validate_manifest`), MSSQL declared-scale
> из `sys.columns` (+TLS-warn), MCP bind-параметры, init/mysql N+1, state PG TLS (+3 сайта
> в `checkpoint.rs`), local temp+rename атомарность, zero-row double-schema, MySQL BIT decode,
> progression numeric-cursor, small-table escape, doctor dedup, quarantine-якорь, CSV `\r`.
>
> **Перф-wave — закрыто (бенчи до/после, байт-идентичный вывод):**
> CSV hex `{:02x}`/байт → таблица+чанк = **11.2×**; CSV timestamp strftime re-parse → ручной
> int = **2.33×**; parquet-валидация на footer `num_rows` вместо полного ре-декода;
> `pow10_i256` через `checked_mul` вместо `format!`+parse.
>
> **Арх-wave — частично:** дренаж частей унифицирован (см. wave 1); MSSQL Int16 mismatch-policy
> приведён к bail (был единственный молча-nullящий числовой арм). **Осознанно отложено как
> решения уровня продукта (не рефакторинг):** (1) кросс-движковая унификация `build_array` на
> fail-loud — изменение поведения (MySQL/PG `row.get()` через `Option` не отличает mismatch от
> NULL без переписывания; рискует превратить рабочие экспорты в жёсткие падения); (2) полный
> `run_one_chunk` seam (дедуп 4 раннеров) + retry на non-checkpoint параллельном пути — большой
> рефакторинг, требует fault-инъекции для доказательства; (3) эпилог `job.rs` — в файле
> незакоммиченный WIP пользователя. Эти три ждут отдельной сессии с TDD и/или согласования.

---

## Исходный отчёт (до исправлений)

> **Методика.** 9 параллельных ревью-агентов (8 по подсистемам + глобальный свип по
> антипаттернам горячих циклов) прочитали код и дали 75 находок (69 после дедупа).
> Каждая находка прошла адверсариальную верификацию отдельным агентом-скептиком с
> чтением реального кода: **66 подтверждено, 3 опровергнуто**. Критичные сайты
> (`sink/mod.rs maybe_split`/`track_quality`, `chunked/exec.rs`, `validate_cmd.rs`,
> `gcs.rs`) дополнительно перепроверены вручную. Замечания верификаторов
> (поправки к формулировкам) включены в карточки находок.

---

## TL;DR

**Ядро добротное.** Commit-seam (`commit.rs`), trust-контракт манифеста (журнал в памяти,
манифест пишется один раз, стриминговые чексуммы), claim-based чекпойнты
(`UPDATE...RETURNING` + `SKIP LOCKED`), CloudBackend-seam с единой retry-политикой,
настоящий стриминг во всех трёх коннекторах (RSS ограничен одним батчем по построению) —
это сильные, проверенные тестами куски.

**Системный грех один — копипаста оркестрационных тел, и она уже дрейфанула.**
Не «может разойтись», а разошлась, в четырёх независимых местах:
per-chunk execute→commit тело живёт в 4 раннерах (retry/журналирование/governor — каждый
только на части путей); эпилог `run_export_job` скопирован в apply-вариант (apply-раны
невидимы для `rivet journal`); три `build_array`-диспетчера разошлись на ключевом
инварианте «что делать при несовпадении значения со схемой» (bail vs null vs мусорный
fallback); валидационная матрица режимов утроена (config/plan/resolve) и противоречит
сама себе. `commit.rs` сам каталогизирует три уже отгруженных бага ровно этой природы.

**Один CRITICAL — тихая потеря данных:** `max_file_size` + chunked/keyset роняет все
ротированные части (см. ниже).

**Перф-картина:** движок не compute-bound, а **churn-bound** — свежий tokio-runtime,
свежие соединения с источником и до 3 свежих state-DB соединений на каждый чанк, encode
никогда не перекрывается с upload, каждый байт читается с диска дважды на пути загрузки.
На per-value уровне горят CSV-writer (интерпретатор на каждую ячейку, `format!` на каждый
байт бинарных колонок) и PG-декодеры (NUMERIC ~6 аллокаций/значение, JSON через
round-trip `serde_json::Value` — это ещё и correctness: переписывает payload клиента).

---

## Топ-приоритеты (рекомендованный порядок атаки)

1. **[CRITICAL] Потеря данных `max_file_size`+chunked/keyset** — минимальный фикс:
   Rejected-диагностика в `plan/validate.rs`; правильный фикс: дренаж
   `sink.completed_parts` через `write_part_file` во всех раннерах. Плюс регрессионный
   тест (правило CLAUDE.md: чанк больше `max_file_size` должен довезти все строки).
2. **`rivet validate` выходит с кодом 0 при нечитаемом манифесте** — гейтить на уже
   существующий `has_failures()`; это дыра ровно в том контракте, ради которого validate
   существует (CI-гейт пройдёт мимо умершего destination).
3. **NULL-ы считаются дубликатами в uniqueness-гейте** — nullable unique колонка с ≥2
   NULL валит экспорт. Фикс + регрессионный тест.
4. **Извлечь `run_one_chunk` seam** — один ход убивает копипасту 4 раннеров, даёт retry
   небэкпойнтному параллельному пути (сейчас один TCP reset = весь ран насмарку) и
   унифицирует журналирование чанков.
5. **Эпилог `job.rs`** — один helper вместо двух дрейфующих копий по ~110 строк.
6. **CSV writer** — колоночные сериализаторы вместо per-cell интерпретатора; hex-таблица
   вместо `format!("{:02x}")` на байт; кэш chrono-формата.
7. **PG decode** — limb fold для NUMERIC, zero-copy text/bytea, JSON без round-trip.
8. **Per-run ресурсы вместо per-chunk** — переиспользовать runtime/соединения, перекрыть
   encode и upload.
9. **Auth/TLS-гигиена** — GCS token refresh, MSSQL `trust_cert` и scale из метаданных,
   `init` без NoTls, фикс SQL-интерполяции в MCP `schema`.

---

## Вердикты по подсистемам

### orchestration

The pipeline orchestration layer has genuinely good bones at the bottom: commit.rs (write_part_file/record_part) and run_store.rs are exemplary seam extractions with ADR-pinned ordering, debug-build coherence gates, and tests that explain themselves — the next contributor cannot easily break the commit invariants. The rot is all at the top: job.rs carries two ~110-line near-identical orchestration epilogues that have already drifted (apply runs persist no journal and send no notifications), and run() is a 355-line mode-selector holding three divergent copies of the aggregate epilogue and two copies of the UI-thread lifecycle. The child-process parallelism model itself is sound — same-binary re-exec, one JSON line per chunk event with negligible serialization cost, synthetic-failure cards for crashed children — but its result path is schizophrenic: the parent receives a complete Finished event over IPC, discards it, and reconstructs each child's outcome from the latest SQLite metric row gated by wall-clock timestamp, so a warn-on-fail metric write or a concurrent invocation makes exit code, cards, and summary.json disagree; meanwhile ipc.rs's module doc still claims child stderr is 'left inherited' when parallel_children actually pipes and buffers it until end of run. The single biggest structural risk is the ambient-global rendering dispatch (env var + global mutex sender + two atomics, consulted from inside a constructor documented as a pure data accumulator, with no RAII guard on the installed sender): it is the one place every mode, every renderer, and every panic path intersect, and it will absorb the next contributor's day. On performance, this layer is per-run/per-chunk so it is largely innocent of the hot path — except single.rs, which stages the entire export as local temp files and uploads serially after extraction, a real wall-clock and disk cliff on the default snapshot path.

### chunked

The bones are good: commit.rs is a real seam that fixed three prior drift bugs, the M8 resume decision matrix (resume_decisions.rs) is pure and exhaustively tested, the claim-based checkpoint state machine is sound (atomic UPDATE...RETURNING claim, SKIP LOCKED on PG), and the condvar semaphore + governor design is competent. The rot is one layer up: the per-chunk execute→finish→fingerprint→validate→name→write body is hand-copied into four runners (plus keyset) and has already drifted — retry, journaling, and the adaptive governor each exist on only a subset of paths, and the commit.rs doc itself catalogs three shipped bugs from exactly this duplication. The single biggest structural risk is that ExportSink is an open struct whose finalize/rotation contract lives in the callers' heads: that contract was already missed once, producing a silent data-loss hole (max_file_size rotation drops parts in every chunked/keyset runner). Performance-wise the engine is per-chunk-churn-bound, not compute-bound: fresh multi-thread tokio runtimes, fresh source connections, and up to three fresh state-DB connections per chunk, while encode is never overlapped with upload anywhere in the chunked paths.

### integrity

The manifest/trust-contract core is the best-built part of this layer and most of the prompt's suspected sins are absent: the journal is purely in-memory (no per-event open/fsync), the manifest is built incrementally and written exactly once at finalize (finalize.rs:200 is write_manifest's only call site), part checksums are a single streaming 64KiB-buffered pass, and verify_at_destination rides one prefix listing through a pure, shared reconcile walk instead of per-part HEADs or re-downloads — with ADR-pinned tests throughout. The rot is concentrated in two places. First, quality.rs is a dead museum copy of the live gate: the sink reimplemented null-ratio/uniqueness inline and the copies have drifted into a real user-facing bug (NULLs counted as duplicates → spurious quality-gate aborts) while the typed-hash optimization and its benchmark live only in the code that never runs. Second, the verdict-to-exit-code projection is hand-rolled differently at each consumer (`manifest_found && !passed` in validate_cmd and finalize) instead of using the one helper built for it, so a manifest read error exits 0, prints "NO MANIFest"-style legacy output, and silently passes CI gates — the exact failure mode the trust contract exists to prevent. The biggest structural risk is the live/dead quality split: the next contributor will modify the tested dead copy and ship nothing. validate.rs is small but wrong twice over (line-count CSV semantics, full Parquet decode); report.rs's render_markdown is long but cleanly decoupled from core types and is not a problem.

### connectors

The skeleton of this subsystem is genuinely good: a single RivetType decision site per engine, build_array dispatching on the resolved target type (not the wire type), a shared unit-tested AdaptiveBatchController, injection-tested query construction, server-side cursors / true streaming on all three engines — nothing buffers a result set, peak RSS is bounded by one batch by design, and the probe→memory-cap→adaptive loop is sound (its one weakness is that the pressure proxies are server-global counters, so opt-in adaptive mode on a busy shared host will pin the batch at the floor for reasons unrelated to the export). The rot is concentrated one level down, in the per-value decode bodies: the PG NUMERIC path burns ~6 allocations and three Strings per cell where a limb fold would do, JSON is round-tripped through serde_json::Value (rewriting the customer's payload in a product sold on type fidelity), and text/bytea copy twice. MSSQL is the visibly least-mature engine — decimal scale inferred from the first 500 rows with a silent-truncation fallback, quadratic cell access, and trust-any-cert as the TLS default — and the zero-row schema handling disagrees across all three engines. The single biggest structural risk is that the three build_array dispatchers have already diverged on the most important invariant they share — what happens when a value doesn't match the schema (bail vs silent null vs garbage fallback) — and there is no shared policy layer or conformance test to stop the fourth engine, or the next type, from drifting further.

### io

The destination layer's CloudBackend/CloudDestination seam is genuinely well-built: one copy of the retry policy, prefix join, and read surface across S3/GCS/Azure, with an honest capabilities contract and a clever process-wide one-shot RAM budget. The parquet writer is a thin, sane wrapper with real care for byte-determinism. The rot is concentrated in two places. First, the CSV value writer is a per-cell interpreter — type dispatch, downcast, and core::fmt through a dyn vtable for every single cell, degrading to a format! call per *byte* for binary columns — which makes CSV export several times slower than it needs to be for zero architectural benefit. Second, and the single biggest structural risk: auth-lifetime handling is inconsistent across backends — Azure got a meticulous SAS-expiry preflight while the GCS ADC path silently pins a ~1-hour static token for the entire export with no refresh and no warning, so long GCS runs are guaranteed to die mid-flight with 401s that the retry machinery cannot save. The path-based Destination::write trait (encode to disk, read back for upload, read again for checksums, with the streamed branch crossing a blocking→async boundary every 8 KiB) is a deliberate tradeoff for retry-safety, but it quietly costs a full extra read of every exported byte plus a throughput cap on exactly the largest parts.

### plan-config

The planning/config layer is well-tested and unusually well-documented in the small (contract docs, M-matrix tests, ADR references), but its validation story is structurally incoherent: the mode/knob rule matrix lives in three places (Config::validate_export, resolve_chunked_strategy, plan/validate.rs) with literal copies that have already drifted — chunk_by_key is completely unvalidated at the config layer while appearing in its own remedy text, and the Keyset strategy was added without teaching any of the validate_plan compatibility rules it exists. The single biggest structural risk is that build_plan stopped being a pure config→plan resolver: it now does blocking, per-export DB introspection whose result (a stats estimate) silently rewrites explicit operator intent (chunk_count/by_days/checkpoint collapsed to Snapshot) and is then thrown away so preflight and chunk detection re-open their own connections — three probes, three connections, one table. Layering is aliased rather than enforced: ResolvedRunPlan/PlanArtifact serialize the raw serde config structs, pinning YAML schema, execution contract, and artifact wire format to the same definitions, and the advisory scoring path consumes diagnostics by substring-grepping Display-formatted strings. None of this is hot-path (everything here is per-run), so the cost is in correctness and maintainability, not throughput: the next contributor adding a chunking knob or a strategy variant must edit four uncoordinated sites and will miss at least one, exactly as the last two additions did.

### state-types

The type system (src/types/) is the best-built part of this scope: a single canonical enum, one funnel to Arrow (mapping.rs), one fidelity classifier, and a target resolver (target.rs) that — despite 1237 lines — is roughly half tests and cleanly split into per-warehouse submodules with live-verified autoload behavior pinned by regression tests; it is not a dumping ground. The tuning module is small, pure, and testable, but its pressure signal is a naive "counter went up" boolean with no deadband, so the governor can oscillate connection counts indefinitely, and the memory estimator is a flat 256-byte-per-string guess that ignores the per-column shape statistics the state store already collects. The state store has solid invariants (WAL + busy_timeout + BEGIN IMMEDIATE claims on SQLite, SKIP LOCKED on Postgres) but is structurally rotten in one specific way: every one of ~25 methods hand-duplicates its SQL and row-mapping across two backend match arms in nine files, with column lists spelled up to three times per function and inconsistent parameter binding — this duplication is the single biggest maintenance risk, and it has already produced semantic asymmetries (lexicographic cursor guard, format!-interpolated LIMITs). Secondary structural risk: the *_at_ref worker API opens a brand-new database connection per chunk operation instead of per worker, which turns the parallel checkpoint loop into a connection-churn machine on the Postgres state backend.

### shell

The shell is disciplined in the small — the CLI split (args/validate/params/dispatch) is textbook, the redaction chokepoint at the env_logger sink is genuinely well-designed, and resource.rs (Condvar semaphore, RSS sampler) is solid. But two systemic rots run through it. First, src/main.rs re-includes all ~64k LOC via `mod` declarations instead of linking the lib target, so the entire crate compiles twice and lib.rs is already accreting visibility hacks ('pub so dead-code analysis...') to paper over the split. Second, error.rs is a one-line anyhow alias, which means every behavioral decision downstream — retry/no-retry (retry.rs), doctor categories and hints (doctor.rs), SAS-expiry detection — is substring matching on formatted driver strings, with an in-comment 'CONTRACT' explicitly coupling two modules by error-message text. Preflight and init each re-implement per-engine DB introspection with no trait seam, and the copies have measurably drifted: preflight's range probes interpolate identifiers raw while the shared sql.rs helper exists precisely to prevent that, init hardcodes NoTls while the source layer negotiates TLS, and MSSQL bails out of `check` entirely despite the type-report path supporting it. The single biggest structural risk is the stringly-typed error taxonomy: one upstream driver wording change silently flips retry classification and doctor hints with zero compiler feedback.

### perf-sweep

The hot row->Arrow->Parquet path is, overall, well-tended: builders are pre-sized, simdutf8 and atoi are wired into the MySQL decoder, the pipelined sink is a bounded sync_channel with FIFO ordering, and the benches encode real before/after experiments. The rot is that optimizations land in benches and side modules but do not consistently reach the production call sites: the typed-hash uniqueness path that hot_paths.rs advertises as the shipped "after" lives in quality.rs with zero production callers (hidden by a file-wide #![allow(dead_code)]) while ExportSink::track_quality still string-formats every value, and the CSV writer shipped the bench's inline-escape fix but left per-byte fmt hex and per-value chrono format-string parsing untouched. The PostgreSQL decoder — the most-used engine — is the weakest hot loop: owned String/Vec allocations per text/bytea cell where the driver offers borrows, a full BigDecimal+two-String round-trip per NUMERIC cell, and a serde_json parse+re-serialize per JSON cell that also silently rewrites the user's JSON (key reorder, duplicate-key collapse) in a product whose pitch is type fidelity. The single biggest structural risk is the duplicated quality subsystem (streaming tracker in the sink vs dead batch checker in quality.rs) — two definitions of "unique", duplicated failure messages, and a bench that validates the copy production never runs; the next contributor will optimize or fix the wrong one.


---

## Подтверждённые находки


### 🟥 CRITICAL

#### 1. max_file_size rotation silently drops completed parts in all chunked and keyset runners (data loss)

`src/pipeline/chunked/exec.rs:139` · частота: **per-chunk** · категория: correctness-risk · ревьюер: chunked


```rust
let rec = super::super::commit::write_part_file(
    dest.as_ref(),
    sink.tmp.path(),
    sink.total_rows as i64,
    file_name,
)?;
```


**Почему важно.** ExportSink::maybe_split rotates the temp file into sink.completed_parts whenever bytes_written >= plan.max_file_size_bytes (sink/mod.rs:158-196), and completed_parts is consumed ONLY by single.rs (lines 314-335). All four chunked runners and keyset.rs:118 upload only sink.tmp.path() — the final partial part — while claiming rows = sink.total_rows (which includes the dropped parts' rows). The rotated NamedTempFiles are deleted when the sink drops. plan/validate.rs rejects max_file_size only with stdout; chunked+max_file_size and keyset+max_file_size pass validation cleanly, so a user setting max_file_size with a chunk whose output exceeds it loses rows silently (manifest size check passes because recorded bytes come from the final tmp; only --validate catches it, and plan.validate defaults to false).


**Фикс.** Either add Rejected diagnostics for chunked/keyset + max_file_size in plan/validate.rs (cheapest), or make the runners drain sink.completed_parts through write_part_file like single.rs does. Per the project's own CLAUDE.md rule, add a regression test asserting a chunk larger than max_file_size lands all rows at the destination.


**Поправка верификатора.** Only trivial inaccuracies: single.rs's completed_parts consumption spans lines 313-368 (push at 314, upload loop 335-368), not 314-335 as stated. Everything substantive holds: I attempted to refute via (a) strategy-gated clearing of max_file_size_bytes in plan/build.rs — none, line 108 copies unconditionally; (b) a chunked/keyset rejection rule in plan/validate.rs — none, lines 31-38 enumerate all checks and only check_stdout_split (line 50) touches max_file_size; (c) row/size verification in commit::write_part_file — it records caller-claimed rows verbatim and computes bytes+checksums from the same final tmp it uploads (commit.rs:97-127), so the manifest verifies clean. The proposed fixes both work from the current state.



### 🟧 HIGH

#### 2. GCS ADC access token fetched once and never refreshed — exports longer than the token TTL 401 mid-run

`src/destination/gcs.rs:42` · частота: **per-run** · категория: correctness-risk · ревьюер: io


```rust
} else if let Some(token) = gcs_auth::try_authorized_user_token()? {
    builder = builder.disable_vm_metadata().token(token);
```


**Почему важно.** `try_authorized_user_token()` mints one OAuth access token (~1 h TTL) at destination construction, and opendal's static-token path wraps it as `GoogleToken::new(token, usize::MAX, ...)` (verified in opendal-0.55.0/src/services/gcs/core.rs:93-94) — no expiry tracking, no refresh, and `disable_vm_metadata()` removes the fallback. The chunked runner deliberately shares one destination Arc for the whole export (exec.rs: "One destination (GCS/S3) instance for the whole export"), so any GCS export running past the TTL starts failing with 401s the RetryLayer cannot fix (permission errors are permanent). Azure got a careful SAS-expiry preflight for exactly this failure mode; the GCS path got nothing — not even a warning.


**Фикс.** Drop the static `.token()` shortcut in favor of a refreshing credential loader (opendal's `customized_token_loader` / reqsign with the ADC refresh_token), or at minimum mirror the Azure SAS preflight: log a WARN that the ADC token expires in ~60 min and long exports will fail mid-run.


**Поправка верификатора.** One minor imprecision: the finding says "permission errors are permanent" — in opendal 0.55.0's GCS error mapping (src/services/gcs/error.rs:55-67) a 401 actually falls through to `(ErrorKind::Unexpected, false)` (403 is the one mapped to PermissionDenied), but both are non-retryable, so the substance (RetryLayer cannot fix it) is correct. Also worth noting the blast radius is scoped: the static-token path only triggers when no `credentials_file` is configured AND the ADC file is `authorized_user` type (gcloud user creds); service-account flows fall to opendal's default chain, whose `token_loader` does refresh. That scoping does not lower severity much, because the ad-hoc long export on operator credentials is exactly the scenario this shortcut was added for, and the failure is deterministic for any export exceeding the ~1h TTL. The proposed fix is viable: `customized_token_loader` exists in opendal 0.55.0 (gcs/backend.rs:158) and is wired into the token loader before the static token would be needed.


#### 3. rivet init hardcodes NoTls and opens a fresh connection per table — onboarding fails outright against TLS-required servers

`src/init/postgres.rs:18` · частота: **per-run** · категория: correctness-risk · ревьюер: shell


```rust
let mut client = postgres::Client::connect(url, postgres::NoTls)?;
```


**Почему важно.** doctor/check/run all connect through source::postgres::connect_client(url, tls) which negotiates TLS, but init — the first command a new user runs — uses bare `postgres::Client::connect(url, NoTls)`. Against RDS (rds.force_ssl), Azure PG, or any hostssl-only pg_hba, `rivet init` fails before the user ever has a config, killing the marketed onboarding flow (init → doctor → check → run). Compounding it, introspect() opens a new Client per table, so `introspect_all` on a 300-table schema performs 301 sequential TCP+auth handshakes; init/mysql.rs:41 has the same Pool-per-table shape plus string-interpolated identifiers (`WHERE TABLE_NAME = '{}'` with a homemade escape) instead of the parameterized form used 100 lines away in preflight.


**Фикс.** Accept the URL's sslmode / attempt TLS-preferred by default in init (reuse connect_client with a default `prefer` TlsConfig), and thread one Client/Pool through list_tables + the introspect loop instead of reconnecting per table.


**Поправка верификатора.** Two refinements, neither refuting. (1) The fix's "reuse connect_client with a default `prefer` TlsConfig" cannot work as written: TlsMode (src/config/source.rs:115) has no `prefer` variant — only Disable/Require/VerifyCa/VerifyFull — and connect_client falls back to NoTls for any non-enforced mode, so a prefer-style behavior requires new try-TLS-then-fallback logic or defaulting init to Require with an opt-out. (2) The N+1 connection cost is real (introspect_all loops postgres::introspect / mysql::introspect, each opening a fresh Client/Pool — init/mod.rs:385-403, init/mysql.rs:41) but init is a one-time onboarding command, so the perf half is cold-path; the severity driver is the TLS hard-failure. Bonus the finder missed: the mysql escape() (init/mysql.rs:110, replace ' with \\') is itself broken under NO_BACKSLASH_ESCAPES sql_mode, and the connect_client doc comment falsely claims init already shares it — a stale invariant that strengthens the finding.


#### 4. Per-chunk execute/commit body copy-pasted across four runners; already drifted

`src/pipeline/chunked/exec.rs:402` · частота: **n/a** · категория: duplication · ревьюер: chunked


```rust
if let Some(s) = sink.dest_schema.as_deref() {
    let columns = crate::state::arrow_schema_to_columns(s);
    let _ = shared_fingerprint.set(crate::state::schema_fingerprint(&columns));
}
```


**Почему важно.** The same build_query → ExportSink::new → export → writer.take().finish() → fingerprint → validate → create_format → chunk_part_filename → write_part_file block appears in exec.rs:97-153 (sequential), exec.rs:374-446 (parallel worker), sequential_checkpoint.rs:41-103, and parallel_checkpoint.rs:214-324 (the quoted fingerprint block is verbatim-duplicated at parallel_checkpoint.rs:270-274). Drift is no longer hypothetical: ChunkStarted is journaled only on the two sequential paths (exec.rs:91, sequential_checkpoint.rs:243); empty-chunk ChunkCompleted only in exec.rs:158 — the parallel paths violate the 'run record covers every chunk index' invariant documented right there; ChunkFailed is journaled only by sequential_checkpoint.rs:298; the OPT-2 adaptive governor exists only in run_chunked_parallel, so chunk_checkpoint:true + adaptive:true silently loses it. commit.rs's own doc lists three shipped bugs from this exact duplication pattern.


**Фикс.** Extract a single run_one_chunk(src, plan, cp, range, chunk_index) -> Result<(rows, Option<PartRecord>)> helper (sequential_checkpoint's export_one_chunk_range is 90% there) and have all four runners call it; move journaling of ChunkStarted/ChunkCompleted/ChunkFailed into the seam so coverage cannot diverge per path.


**Поправка верификатора.** Two factual errors and one fix caveat. (1) "empty-chunk ChunkCompleted only in exec.rs:158" is wrong — sequential_checkpoint.rs:278-284 also journals rows:0/file_name:None; the correct split is both sequential paths journal empty chunks, both parallel paths drop them (empty chunks push no PartRecord so the post-scope record_part drain never sees them). (2) The title overstates scope: the commit half is already extracted into the shared commit::write_part_file/record_part seam (built precisely because this pattern shipped bugs), and record_run_schema_fingerprint is shared by the two sequential paths — the remaining duplication is the export half. (3) The proposed run_one_chunk(src, plan, cp, range, chunk_index) signature will not drop in: parallel workers cannot hold &mut RunSummary (the fingerprint must be returned, not recorded via record_run_schema_fingerprint), the helper needs a &dyn Destination parameter because the parallel paths deliberately share one Arc'd destination (per-chunk create_destination caused Tokio runtime shutdown races — exec.rs:282-284 comment) while the sequential paths still create per chunk, and moving ChunkStarted/Completed/Failed "into the seam" requires off-thread event buffering since the journal is not thread-shared (both parallel runners drain into summary.journal only post-scope). Also note the missing journal events are an audit/observability contract gap, not a resume-correctness bug — resume reads the chunk_task table, not the journal.


#### 5. Non-checkpoint parallel runner has zero retry: one transient blip fails the whole run, unresumable

`src/pipeline/chunked/exec.rs:450` · частота: **per-chunk** · категория: correctness-risk · ревьюер: chunked


```rust
if let Err(e) = result {
    log::error!("export '{}': chunk {} failed: {:#}", export_name, i, e);
    poison::lock_recover(errors).push(format!("chunk {}: {:#}", i, e));
}
```


**Почему важно.** The exec.rs parallel worker classifies nothing and retries nothing — classify_error/backoff exist only in sequential_checkpoint.rs:106-172 and parallel_checkpoint.rs:216-324. The export-level retry (single.rs decide_export_retry) explicitly bails once files_committed > 0, and non-checkpoint runs have no chunk_task state to resume from. So a 6-hour 1000-chunk parallel run dies on one TCP reset at chunk 950, must restart from zero, and leaves 949 nonce-named orphan parts at the destination (duplicates for any glob reader; M8 quarantine only runs on checkpoint resumes). tuning.max_retries is silently a no-op on this path.


**Фикс.** Wrap the worker's export attempt in the same transient-classified retry loop the checkpoint paths use (extract run_chunk_with_source_retries from sequential_checkpoint.rs and share it), or deprecate the non-checkpoint parallel path entirely by running parallel_checkpoint with an ephemeral run_id.


**Поправка верификатора.** Two overstatements, neither fatal. (1) A transient failure at chunk 950 does not kill the run mid-flight: the worker pushes to `errors` and the scope completes all remaining chunks; successful parts are committed AND manifest-recorded via the record_part drain (exec.rs:484-492) before the bail at 494. So the run fails at the end with 999/1000 parts committed, not "dies at chunk 950". (2) "must restart from zero" has a manual escape hatch: `rivet repair --execute` (repair_cmd.rs) re-exports only the missing chunk ranges from a reconcile report — but it is operator-driven, not automatic, and also never deletes the old nonce files. Also, decide_export_retry isn't merely bailing on this path — it is not in the parallel call chain at all (job.rs:301-315 calls run_chunked_parallel directly; run_with_reconnect wraps only the non-parallel branch), which makes the gap slightly worse than stated. Finding's claimed line ranges for the checkpoint retry loops are approximately right (sequential_checkpoint.rs:116-163, parallel_checkpoint.rs:216-313). 'tuning.max_retries is a no-op' is accurate in substance: its only use on this path is exec.rs:291 feeding destination::log_capabilities (a log line), and chunk_max_attempts is consumed only by ensure_chunk_checkpoint_plan (checkpoint paths).


#### 6. run_export_job vs run_export_job_with_chunk_source: ~110-line duplicated orchestration epilogue, already drifted

`src/pipeline/job.rs:492` · частота: **n/a** · категория: duplication · ревьюер: orchestration


```rust
let tuning_class = plan.tuning.profile_name().to_string();
    let result = run_chunked_quality_gate(result, plan, &mut summary);
    let failed = result.is_err();
```


**Почему важно.** The entire post-run epilogue (RSS sampling, quality gate, status match, 15-positional-arg record_metric, print, finalize hooks) is hand-copied between run_export_job (lines 278-421) and the apply variant (lines 470-541), and the copies have ALREADY drifted: the apply path records no RunCompleted journal event, never calls state.store_journal (so `rivet journal` is permanently blind to apply runs), skips reconcile, the pg_temp_bytes probe, plan-warning journal records, and notify::maybe_send. Every future epilogue change must be made twice or it silently diverges; the duplicated record_metric call with two adjacent Option<bool> args is a transposition trap in both places.


**Фикс.** Extract the shared epilogue into one function (e.g. `execute_and_finalize(plan, state, summary, dispatch: impl FnOnce(...) -> Result<()>, hooks: FinalizeHooks)`) so run and apply differ only in declared hooks; make journal persistence the default and force the apply path to explicitly opt out if that is intended. Replace the 15-positional-arg record_metric with a call taking &RunSummary.


**Поправка верификатора.** Two refinements. (1) Not every omission is pure accident: the apply variant takes no &Config, so notify::maybe_send structurally cannot be called without a signature change, and a comment at line 478 shows checkpoint-resume divergence is deliberate — but the journal and reconcile omissions are genuine drift, because the apply variant's own doc comment (job.rs:435-436, 'Everything else — quality gate, metrics, state persistence — is identical to run_export_job') is false for journal persistence. (2) The finder understated one item: plan.reconcile from the artifact is silently ignored in apply (only plan.validate is honored at line 536), which is a contract gap, not just missing nicety. The proposed extraction is workable; the hooks need a small context struct to carry config/labels ('export' vs 'apply') since apply lacks Config.


#### 7. Live uniqueness gate hashes NULLs as values — nullable unique columns spuriously FAIL, per-value Display formatting on the hot path

`src/pipeline/sink/mod.rs:233` · частота: **per-value** · категория: correctness-risk · ревьюер: integrity


```rust
scratch.clear();
                    let _ = write!(scratch, "{}", formatter.value(row));
                    set.insert(xxh3_64(&scratch));
```


**Почему важно.** track_quality runs per batch on the export hot path (on_batch_inner, line 386). The loop has no is_null check: every NULL formats to "" (FormatOptions default) and hashes to one set entry, and the verdict at line 366 (`let dupes = self.total_rows.saturating_sub(set.len());`) then counts every NULL beyond the first as a duplicate — so a nullable unique column with 2+ NULLs FAILS the quality gate and aborts the export, contradicting SQL UNIQUE semantics and the dead-but-tested quality.rs implementation which explicitly skips nulls ('Returns `None` for null values; caller skips those'). NULLs also collide with genuine empty strings. Separately, Display-formatting every value through ArrayFormatter before hashing is the exact per-value cost hash_value was written to remove.


**Фикс.** Skip null rows in track_quality (track a per-column non-null counter and compute dupes from it), and hash via quality::hash_value's typed dispatch instead of write!+ArrayFormatter. Add the regression test for the nullable-unique-column case per the project's caught-bug rule.


**Поправка верификатора.** The diagnosis holds in full, but two refinements: (1) the per-value Display-formatting perf angle is real yet mitigated — track_quality early-returns unless quality.unique_columns is configured, and the unique_max_entries cap (which plan/validate.rs warns about if unset) halts hashing once hit, so the cost is opt-in and bounded; the correctness bug, not perf, is the load-bearing issue. (2) The finder could have added that quality.rs's header comment ('called from pipeline::sink') is stale for check_uniqueness/run_checks — they are invoked only by quality.rs's own unit tests, confirming the 'dead-but-tested' claim. The proposed fix is sound: the verdict at mod.rs:366 reuses self.total_rows, so a per-column non-null counter is indeed required, and quality::hash_value's typed dispatch (which returns None on nulls) is directly reusable.


#### 8. CSV validation counts physical lines on a whole-file String — false failures on quoted newlines, unbounded memory

`src/pipeline/validate.rs:25` · частота: **per-chunk** · категория: correctness-risk · ревьюер: integrity


```rust
let content = std::fs::read_to_string(path)?;
            let lines = content.lines().count();
            lines.saturating_sub(1)
```


**Почему важно.** validate_output runs per part on the hot export path (single.rs:337, keyset.rs:102, chunked/exec.rs:130/411, sequential_checkpoint.rs:86, parallel_checkpoint.rs:279) whenever validate=true. Two problems: (1) RFC-4180 CSV quotes embedded newlines, so any text column containing '\n' makes physical-line count exceed record count and the export ABORTS with 'validation failed' on perfectly correct output — a data-dependent landmine; (2) read_to_string materialises the entire part (multi-GB with no max_file_size) as one String per part, right next to the memory-cap machinery the sink carefully maintains.


**Фикс.** Stream the file through a csv::Reader (already-available csv crate semantics) counting records, or at minimum count records with a streaming RFC-4180-aware scanner over a BufReader — never load the part into a String.


**Поправка верификатора.** Minor scoping, not refutation: the false failure requires the opt-in plan.validate=true AND format=csv, so it is not on every export. Also the validator's own unit tests (validate.rs:62-94) deliberately encode physical-line counting — the code is 'as tested' but contradicts the writer's RFC-4180 quoting in src/format/csv.rs:176-188 (which has its own test string_with_newline_is_quoted asserting raw \n inside quotes). The proposed fix is valid: csv = "1" is already a direct dependency (Cargo.toml:46).


#### 9. rivet validate exits 0 and hides failures when manifest read/head fails

`src/pipeline/validate_cmd.rs:195` · частота: **per-run** · категория: correctness-risk · ревьюер: integrity


```rust
let any_failed = all_results
        .iter()
        .any(|r| r.verification.manifest_found && !r.verification.passed);
```


**Почему важно.** In verify_at_destination (src/pipeline/validate_manifest.rs:318-345) a manifest head/read I/O error (permission denied, transient outage) returns a verdict with manifest_found=false plus a ManifestReadError failure. The exit-code gate above requires manifest_found=true, so the command exits 0 — an Airflow/CI gate `rivet validate && next` proceeds against an unreadable destination. Worse, render_pretty (line 249-251) prints 'status: NO MANIFEST' and `continue`s, so the ManifestReadError line is never shown; the operator sees what looks like a benign legacy prefix. This directly contradicts the module's own doc ('non-zero on any explicit failure'). The same `!v.passed && v.manifest_found` guard in finalize.rs:276 means a run's --validate verdict also stays PASSED when the verifier could not re-read the manifest it just wrote.


**Фикс.** Gate on the existing `has_failures()` helper (documented as exactly 'a reason an orchestrator should refuse the run'): `any_failed = !r.verification.passed && r.verification.has_failures()`. In render_pretty, print `v.failures` before the `continue` in the !manifest_found branch.


**Поправка верификатора.** Two minor mitigations understate nothing material but are worth noting: (1) render_json serializes the verdict verbatim (passed=false plus the ManifestReadError kind), so a JSON-consuming orchestrator branching on failure kinds — the wire contract validate_manifest.rs's module doc advertises — would still catch it; only exit-code and pretty-output consumers are blinded. (2) In finalize.rs the pass is documented as non-fatal by design and summary.manifest_verification carries the structured failure into summary.json, so that half of the finding is a softer variant (misleading validated flag, not hidden data). Also a wording quibble: the operator sees "NO MANIFEST", not the legacy_run string — similarly benign-looking, and in fact that branch is only ever reached by read-error verdicts.


#### 10. Keyset strategy is invisible to every plan compatibility rule — stdout+keyset emits concatenated multi-part binary output with no diagnostic

`src/plan/validate.rs:66` · частота: **per-run** · категория: correctness-risk · ревьюер: plan-config


```rust
if is_stdout(plan) && matches!(&plan.strategy, ExtractionStrategy::Chunked(_)) {
```


**Почему важно.** keyset.rs documents 'Each page ... becomes one output part file', i.e. keyset has exactly the multi-part shape that makes `stdout-no-chunked` a Rejected rule — but every matcher in validate_plan tests only `Chunked(_)`. Worse, the planner auto-selects Keyset on MySQL tables without an int PK (build.rs:392-407), so an operator with `table:` + `mode: chunked` + stdout never opted into multi-part output yet gets unusable concatenated Parquet on stdout with zero diagnostics. `quality-chunked-partial` has the same blind spot (quality checks run per page file). The test matrix M1-M20 in this file has no Keyset rows at all, confirming the strategy was added without extending the compatibility contract.


**Фикс.** Extend check_stdout_chunked and check_quality_chunked to match `ExtractionStrategy::Keyset(_)` (rename rules or add keyset-specific ones), and add Keyset rows to the M-matrix tests. Audit other `matches!(.., Chunked(_))` sites for the same gap.


**Поправка верификатора.** Two refinements, neither refuting. (1) "zero diagnostics" is a slight overstatement: any stdout plan fires the Degraded `stdout-manifest-phantom` diagnostic (validate.rs:174-186), logged at info level in job.rs — but it says nothing about multi-part concatenation and does not block, so the operator still gets silently unusable output. (2) The quality blind spot is WORSE than claimed: quality checks do not run "per page file" under Keyset — `run_quality_checks()` is called only in single.rs:257 (snapshot/incremental/timewindow path); the keyset runner (and the chunked runners) never call it, so a configured `quality:` block is silently ignored entirely under Keyset, with no warning because check_quality_chunked matches only Chunked(_). The proposed fix (extend the two matchers to Keyset) is correct for stdout; for quality the rule message should reflect actual behavior (silently skipped, not partial).


#### 11. MSSQL decimal scale inferred from first-batch data; silent integer-division truncation when probe batch is all-NULL

`src/source/mssql/arrow_convert.rs:358` · частота: **per-value** · категория: correctness-risk · ревьюер: connectors


```rust
Ordering::Less => {
    let factor = 10i128.pow((from_scale - to_scale) as u32);
    Ok(value / factor)
}
```


**Почему важно.** Decimaln/Numericn defaults to Decimal{precision:38, scale:0} and the real scale is recovered from the data via decimal_scale_from_rows over the FIRST emitted batch only (probe = 500 rows). A decimal column whose first 500 rows are NULL (sparse column, backfilled column) freezes the schema at scale 0; every later non-null value then hits rescale_i128's Ordering::Less arm, which silently divides — 123.45 becomes 123, cents gone, no error. Worse, in chunked mode each chunk re-infers its own scale from its own first batch, so chunk files of the same table can carry different Parquet decimal schemas.


**Фикс.** Make rescale_i128 error when the division has a non-zero remainder (lossy down-scale must be loud, per the project's own remediation rule), and extract the declared precision/scale from the TDS COLMETADATA instead of from data — tiberius exposes the raw TypeInfo; if it truly cannot, probe sys.columns once per export like the PG numeric catalog-hint path.


**Поправка верификатора.** Two refinements. (1) The gap is partially acknowledged in code: mod.rs:397-405 doc comment admits "a decimal column NULL for the whole first batch falls back to scale 0", and arrow_convert.rs:55-56 marks wire-metadata precision/scale as a planned follow-up — but no runtime guard exists, so the silent /10^s truncation and per-chunk schema divergence are real, and exec.rs:113-114's manifest fingerprint comment ("schema is identical run-wide") is violated by this mechanism. (2) The fix's first branch is wrong: the in-code comment at arrow_convert.rs:104-105 states tiberius' Column DROPS the declared precision/scale (only the type tag survives), so "extract from TDS COLMETADATA" is not available; the workable fix is the finding's own fallback — a sys.columns probe via the existing catalog_hint_query infrastructure (PG-style), plus the remainder check in rescale_i128, which is valid since the value is still lossless at rescale time. Minor: probe batch is min(configured_batch_size, 500), i.e. <=500 rows, not exactly 500.



### 🟨 MEDIUM

#### 12. Cargo.toml ships two TLS stacks (vendored OpenSSL + rustls) and the comment claiming otherwise is stale

`Cargo.toml:98` · частота: **n/a** · категория: duplication · ревьюер: shell


```rust
# MSSQL / SQL Server source driver (async). Bridged to the sync `Source` trait
# via a per-source `tokio` current-thread runtime + `block_on` (ADR-0011 keeps
# `Source` sync). `native-tls` matches the OpenSSL stack the PG/MySQL drivers
# already vendor; `rustls` default is disabled to avoid a second TLS tree.
tiberius = { version = "0.12", default-features = false, features = ["tds73", "chrono", "rustls"] }
```


**Почему важно.** The comment asserts rustls is disabled 'to avoid a second TLS tree', but the feature list on the same line enables `rustls`. Combined with `native-tls = { features = ["vendored"] }` (statically-linked OpenSSL for postgres/mysql) and `reqwest`/`opendal` on rustls-tls, the release binary carries both full TLS implementations: double the binary size contribution, double the CVE-audit surface, and two divergent cert/CA behaviors across sources (a `tls.ca_file` that works for PG may behave differently for MSSQL). The lying comment guarantees the next contributor reasons from a false premise.


**Фикс.** Pick one stack: either tiberius with `vendored-openssl` (it supports native-tls) to consolidate on the OpenSSL tree, or move PG/MySQL to rustls-backed connectors. At minimum, correct the comment to state that two TLS stacks are intentionally shipped.


**Поправка верификатора.** Two corrections, neither refuting. (1) The comment is not 'stale' — `git log -L 95,105:Cargo.toml` shows the comment and the `rustls` feature landed together in commit 49150e3; it was wrong from inception. (2) The fix's first option (tiberius → vendored-openssl) would NOT consolidate to one TLS stack: reqwest/opendal keep rustls 0.23.40 by deliberate, documented choice (Cargo.toml lines 112–116). What that swap would actually fix is the undercounted part of the finding: `cargo tree -i rustls@0.21` proves tiberius pulls a SECOND rustls version (0.21.12 via tokio-rustls 0.24.1), distinct from the 0.23.40 tree reqwest uses — so the binary currently ships THREE TLS trees (vendored OpenSSL + rustls 0.21 + rustls 0.23), and the MSSQL path is live security surface (src/source/mssql/mod.rs:151 sets EncryptionLevel::Required, riding EOL rustls 0.21).


#### 13. dev seeding tool installed as a user-facing binary named `seed` by `cargo install rivet-cli`

`Cargo.toml:25` · частота: **n/a** · категория: architecture · ревьюер: shell


```rust
[[bin]]
name = "seed"
path = "src/bin/seed/main.rs"
```


**Почему важно.** `cargo install rivet-cli` installs all bin targets, so every user gets a 2.8k-LOC test-data generator dropped into ~/.cargo/bin under the maximally generic name `seed` — a tool that creates/drops tables in whatever DB URL it is pointed at. It also adds its compile time to every release build. The `exclude = ["dev/", ...]` line shows the intent to keep dev tooling out of the package, but src/bin/seed/ slipped past it.


**Фикс.** Move seed to a separate workspace member excluded from publish (or gate it with `required-features = ["dev-tools"]` off the default feature set), and rename it `rivet-seed` if it must ship.


**Поправка верификатора.** Two refinements, neither refuting. (1) "creates/drops tables" is slightly off: seed issues CREATE TABLE IF NOT EXISTS and TRUNCATE ... RESTART IDENTITY CASCADE (src/bin/seed/fast.rs:21, insert.rs:146) — no DROP, but TRUNCATE on tables named users/orders/events is equally data-destructive. (2) Accidental blast radius is mitigated by defaults: bare `seed` targets localhost with hardcoded dev creds (postgresql://rivet:rivet@localhost:5432/rivet, args.rs:59), so wiping a real DB requires an explicit --pg-url/--mysql-url. The proposed fix (required-features off default, or separate non-published workspace member) is valid: cargo skips bin targets whose required-features are unmet at both install and build time.


#### 14. mode/knob validation matrix triplicated across validate_export, resolve_chunked_strategy, and validate_plan — and already drifted (chunk_by_key unchecked at config layer, remedy text contradicts its own rule)

`src/config/mod.rs:495` · частота: **per-run** · категория: duplication · ревьюер: plan-config


```rust
if export.chunk_count.is_some() && export.chunk_dense {
                    anyhow::bail!(
                        "export '{}': chunk_count and chunk_dense are mutually exclusive. \
```


**Почему важно.** The chunk_count==0 / chunk_count×chunk_dense / chunk_count×chunk_by_days bails exist verbatim in both Config::validate_export (config/mod.rs:492-510) and resolve_chunked_strategy (build.rs:161-177), with different message text already. Drift is not hypothetical: `chunk_by_key` appears in config/mod.rs only inside an error-message string (line 467) and is never validated, so `rivet check` passes a config with `chunk_by_key + chunk_column` that fails at plan time; meanwhile validate_export's 'needs a chunking strategy. Pick one: ... chunk_by_key: <unique col> ... chunk_count: <N>' fires on the condition `chunk_column.is_none() && table.is_none()` — a user who follows the remedy and sets chunk_by_key alone hits the exact same error again. Same pattern: compression_level is validated against `export.compression` (mod.rs:402-428) while effective_compression() lets compression_profile silently override both. Every new chunk knob now needs edits in 2-3 places that nobody enforces staying in sync.


**Фикс.** Extract the per-export mode/knob rule set into one pure function (taking &ExportConfig) called from both Config::validate_export and resolve_chunked_strategy; keep only plan-shape rules (needs introspection / resolved strategy) in build.rs and validate_plan. Validate compression_level against effective_compression(), not the raw field.


**Поправка верификатора.** "Triplicated" is wrong: validate_plan (src/plan/validate.rs) contains NO chunk-knob rules — it checks stdout/reconcile/resume/quality plan-shape compatibility only. The chunk matrix exists in exactly two places (config/mod.rs validate_export and plan/build.rs resolve_chunked_strategy), not three. The rest holds: chunk_count rules are verbatim-duplicated with already-divergent message text; chunk_by_key appears in config/mod.rs only inside the line-467 error string and is validated solely at plan time (build.rs:182-197, 222-228), and `rivet check` (preflight/mod.rs:80) runs only Config::load_with_params + source/destination probes, so chunk_by_key+chunk_column passes check and fails at run; the remedy at mod.rs:463-473 lists chunk_by_key as a pick yet its trigger condition `chunk_column.is_none() && table.is_none()` re-fires on a config that sets chunk_by_key alone; compression_level is validated against raw export.compression (mod.rs:402-428) while build.rs:100 uses effective_compression() where compression_profile silently overrides. Severity lowered from high to medium: impact is a worse/later error message and a misleading hint, not data corruption or a silently-accepted invalid plan — build.rs still rejects every bad combination before execution. The proposed fix (one pure per-export rule function called from both layers; validate level against effective_compression) is sound, with one nuance: validating against effective_compression means a bad raw compression_level goes unreported when a profile is set, which should be a deliberate choice (ignore vs warn).


#### 15. CSV writer is a per-cell interpreter: DataType match + downcast + dyn-Write fmt per value

`src/format/csv.rs:63` · частота: **per-value** · категория: performance · ревьюер: io


```rust
for row_idx in 0..batch.num_rows() {
    for col_idx in 0..batch.num_columns() {
        if col_idx > 0 {
            buf.push(b',');
        }
        write_csv_value(&mut buf, batch.column(col_idx), row_idx)?;
```


**Почему важно.** Every single cell pays: a match on `array.data_type()` (enum with payload comparisons for Decimal128/Timestamp arms), an `as_any().downcast_ref::<...>()`, and `write!(writer, "{}", ...)` through `&mut dyn Write` — core::fmt machinery plus vtable dispatch per value. For a 10M-row × 20-col export that's 200M dispatch+fmt round trips. This is the textbook CSV-writer anti-pattern; columnarizing typically yields 3–10x on the CSV encode path, which is the entire hot loop for `format: csv`.


**Фикс.** Restructure to columnar: per batch, downcast each column once into an enum of typed serializer closures (or a Vec of `&dyn Fn(usize, &mut Vec<u8>)`), then iterate rows calling the pre-resolved serializers; write integers/floats with itoa/ryu directly into the monomorphic `Vec<u8>` instead of `write!` through `dyn Write`.


**Поправка верификатора.** The finder overlooked that write_batch already pre-allocates a per-batch Vec<u8> (Vec::with_capacity(rows*cols*8)) and flushes it with a single write_all, so the dyn Write dispatch lands in an in-memory Vec, not in per-cell I/O. "Columnarizing typically yields 3-10x ... which is the entire hot loop for format: csv" overstates impact: the CSV encode is one of several per-value passes in the sink (quality/shape tracking, enrichment) and exports are usually source-fetch-bound, so the realistic end-to-end gain is much smaller than the encode-path microbenchmark gain. The fix itself is sound.


#### 16. Per-byte `write!("{:02x}")` hex encoding for Binary columns

`src/format/csv.rs:196` · частота: **per-value** · категория: performance · ревьюер: io · также найдено: perf-sweep


```rust
for byte in val {
    write!(writer, "{:02x}", byte)?;
}
```


**Почему важно.** Each byte of every binary value constructs a `fmt::Arguments`, runs the formatting machinery, and makes a virtual `write` call — three layers of overhead to emit two ASCII chars. A 1 MB blob = one million formatter invocations for what should be a single table-lookup pass. For tables with blob columns exported to CSV this dwarfs every other cost in the writer.


**Фикс.** Hex-encode with a 16-byte nibble lookup table into a reusable scratch `Vec<u8>` (2 bytes per input byte) and emit with one `write_all`, or use the `hex` crate's `encode_to_slice`.


**Поправка верификатора.** The finder overlooked that write_batch (src/format/csv.rs:61-75) buffers each batch into a pre-allocated in-memory Vec<u8> before a single write_all to the real sink, so the per-byte "virtual write call" is a Vec push — no per-byte syscall or I/O flush amplification. The remaining cost is fmt::Arguments construction + format-spec processing per byte (~10-30x slower than a nibble LUT), which is real but only dominates when the export format is CSV and the table carries large Binary blobs; Parquet is the primary load format in this pipeline. Diagnosis and proposed fix (LUT/hex::encode_to_slice into scratch + one write_all) are correct, but severity high is inflated for this conditional path.


#### 17. Per-value heap String for Decimal128 and per-value strftime re-parse for Timestamp

`src/format/csv.rs:153` · частота: **per-value** · категория: performance · ревьюер: io


```rust
let text = scaled_i128_to_decimal_str(arr.value(idx), *scale);
writer.write_all(text.as_bytes())?;
```


**Почему важно.** Every decimal cell allocates and frees a fresh String. Similarly at line 261, `write!(writer, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.6f"))` makes chrono re-parse the strftime format string for every timestamp cell. Both are pure per-value waste on the CSV hot path; the Time64 arm just below (lines 243-250) already shows the right pattern — integer math straight into the writer with no allocation.


**Фикс.** Have `scaled_i128_to_decimal_str` (or a sibling) write into a caller-provided scratch buffer; format timestamps with the same integer-arithmetic approach the Time64 arm uses (the format is fixed ISO-8601), eliminating chrono's per-call format parsing.


**Поправка верификатора.** Finding is accurate; two refinements. (1) There is one mitigation nearby it does not mention: write_batch pre-allocates a per-batch Vec (line 62, `Vec::with_capacity(rows * cols * 8)`), so I/O is already buffered — the waste is strictly the per-cell malloc/free and chrono strftime re-parse, not syscalls. (2) For the timestamp fix, hand-rolled integer math is not the only option: hoisting `StrftimeItems::new("%Y-%m-%dT%H:%M:%S%.6f")` once (per writer or per batch) and using `format_with_items` eliminates the per-cell parse with far less code than civil-date arithmetic. Also note every arm in write_csv_value pays core::fmt dispatch through &mut dyn Write per cell; these two are the worst offenders, not the only per-cell costs.


#### 18. CSV quote predicate misses carriage return — lone \r emitted unquoted

`src/format/csv.rs:176` · частота: **per-value** · категория: correctness-risk · ревьюер: io


```rust
if val.contains(',') || val.contains('"') || val.contains('\n') {
```


**Почему важно.** RFC 4180 requires quoting fields containing CR. A string value with a lone '\r' (common in DB text columns written from old-Mac/mixed-newline sources; '\r\n' is caught only via the '\n' check) is written bare, and readers that treat CR as a record terminator (Excel, Python csv in universal-newline mode) split the row — silent row-count and column-alignment corruption in the validation-facing output. Minor side cost: three separate scans of every string value.


**Фикс.** Add `|| val.contains('\r')` to the predicate (and quote the '\r' branch in the escaping loop); collapse the triple scan into one `memchr`-style pass while there. Add a regression test mirroring `string_with_newline_is_quoted`.


**Поправка верификатора.** Two minor errors: (1) the fix's "(and quote the '\r' branch in the escaping loop)" is wrong — the escaping loop only doubles '"' chars; a CR inside a quoted field is preserved verbatim and legal per RFC 4180, so only the predicate needs `|| val.contains('\r')`. (2) "validation-facing output" misframes the surface: this is the pipeline's data-export writer for `format: csv` destinations (non-default; Parquet is default), not a validation report — same corruption impact, different consumer.


#### 19. CSV timestamp cells re-parse the chrono format string for every value

`src/format/csv.rs:261` · частота: **per-value** · категория: performance · ревьюер: perf-sweep


```rust
write!(writer, "{}", dt.format("%Y-%m-%dT%H:%M:%S%.6f"))?;
```


**Почему важно.** chrono's DateTime::format() parses the strftime pattern into StrftimeItems on every call — per timestamp cell, on the per-value CSV encode path. On a timestamps-heavy export this is a measurable fraction of the write cost, and it sits in exactly the loop the csv_write_batch bench was built to optimize. The Decimal128 arm at line 153 has a sibling cost (scaled_i128_to_decimal_str allocates a String per cell).


**Фикс.** Parse the format once into a cached `StrftimeItems`/`Vec<Item>` (lazy static or struct field) and use `dt.format_with_items(items.iter())` per value; alternatively hand-format Y/M/D/H/M/S.micros from the i64 with integer arithmetic like the Time64 arm at line 243 already does.


**Поправка верификатора.** Mostly accurate, one overstatement: the csv_write_batch bench in benches/hot_paths.rs only exercises Int64/Float64/Utf8 columns — no Timestamp — so the bench as written would not measure this cost (the loop shape is the same, but "exactly the loop the bench was built to optimize" overstates coverage). The fix is valid: chrono 0.4.45's StrftimeItems supports one-time parsing + format_with_items, and the Time64 arm at lines 243-250 already demonstrates the better hand-formatted integer-arithmetic approach (which also skips chrono's per-item Display dispatch, a larger win than just caching the tokenization).


#### 20. binary re-includes the entire crate via `mod` instead of linking the lib — whole 64k-LOC crate compiles twice

`src/main.rs:5` · частота: **n/a** · категория: architecture · ревьюер: shell


```rust
mod cli;
mod config;
mod destination;
mod enrich;
mod error;
```


**Почему важно.** Cargo.toml defines both a lib (src/lib.rs) and the `rivet` bin, but main.rs declares all 22 modules as its own `mod` tree, so every module is compiled twice per build (lib unit for tests/rivet-mcp + a second full copy for the binary). lib.rs is already accumulating compensating hacks — `pub mod preflight` exists solely so dead-code analysis works ('Without that, the lib compilation unit ... would mark the entire transitive surface as dead'), and `destination_for_tests` re-export windows. Two compilation units with different visibility graphs is exactly how lib-vs-bin drift starts, and it doubles incremental compile time on the heaviest crate in the workspace.


**Фикс.** Make main.rs a thin shim (`use rivet::cli; fn main() { ... }`), move `cli/` and `init/` into the lib (pub(crate) or #[doc(hidden)]), and delete the duplicate `mod` tree. The dead-code workarounds in lib.rs then become unnecessary.


**Поправка верификатора.** The finder missed that this is a documented, deliberate decision: ADR-0002 (Accepted, 2026-04, docs/adr/0002-cli-product-vs-library.md) explicitly records "main.rs ... never uses the library crate" as a consequence — so this is not unrecorded drift-in-waiting, and the fix requires revising an accepted ADR. However, the ADR's Alternatives Considered never weighed the thin-shim option (only inline-tests and crate extraction), so the double-compilation was acknowledged but never justified. Two fix-detail errors: (1) `pub(crate)` cannot work for cli/init — the bin shim is an external crate to the lib, so anything main.rs touches must be `pub` (#[doc(hidden)] is the correct half of the suggestion); (2) "the dead-code workarounds then become unnecessary" is only half-true — the `pub mod preflight` hack would go away, but `destination_for_tests` (lib.rs:46-48) exists for the integration-test harness (tests/ is an external crate needing the pub(crate) destination module) and survives the refactor.


#### 21. MCP tool `schema` argument interpolated raw into SQL — breaks the module's own read-only promise

`src/mcp.rs:309` · частота: **per-run** · категория: correctness-risk · ревьюер: shell


```rust
let schema_filter = match schema {
        Some(s) => format!("AND schemaname = '{s}'"),
```


**Почему важно.** The module header promises 'All tools are read-only — no writes, no DDL', but the `schema` argument (supplied by the MCP client — an LLM) is spliced into the query string here and in mysql_table_stats (line 477: `AND table_schema = '{s}'`). An injected `' UNION SELECT pg_terminate_backend(pid)::text,... --` executes volatile functions under the configured credentials — session-killing or data-reading well beyond the advertised stat views. Both drivers support parameterized queries, used elsewhere in this very crate (preflight/mysql.rs exec_first with `?`).


**Фикс.** Use bind parameters: `WHERE schemaname = $1` via client.query(sql, &[&s]) on the PG side and `exec_iter(sql, (s,))` on the MySQL side; keep the static NOT IN fallback as a separate constant query.


**Поправка верификатора.** Impact is narrower than the 'why' implies: single-statement defaults in both drivers block `; DROP`/write chaining, so the breach is data exfiltration (UNION over readable tables) and volatile-function invocation (e.g. pg_terminate_backend) inside a single SELECT — still a genuine break of the read-only promise, but not arbitrary DDL/writes. The real exposure is the prompt-injection / shared-host threat model, since in single-user use the MCP client already possesses the configured credentials. The proposed fix works as stated (two query variants: `WHERE schemaname = $1` / `= ?` for the Some case, static NOT IN constant for None).


#### 22. Parent aggregates child outcomes via timestamp-correlated SQLite rows while discarding the IPC Finished event that carries the same data

`src/pipeline/aggregate.rs:268` · частота: **per-run** · категория: correctness-risk · ревьюер: orchestration


```rust
if let Some(m) = rows.into_iter().next()
                    && let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(&m.run_at)
                    && parsed.with_timezone(&Utc) >= started_at
```


**Почему важно.** Process mode has two truth paths for one child result: the ChildEvent::Finished IPC event (status, rows, files, bytes, duration, error) is consumed by the UI thread and thrown away, while the aggregate re-derives the outcome from the latest export_metrics row gated only by `run_at >= started_at`. Failure modes: a child whose record_metric write fails (explicitly warn-on-fail in job.rs:383) exits 0 but the aggregate/summary.json reports it `failed: no metric recorded for this run` — exit code and persisted summary disagree; a concurrent rivet invocation against the same state DB can satisfy the timestamp gate and be mis-attributed. There is no run_id handshake between parent and child at all.


**Фикс.** Tee Finished events out of the UI channel (or have run_ui return collected terminal events) and build RunAggregateEntry from them, falling back to the DB row only when a child died without a Finished event; alternatively pass a parent-generated run correlation id to children and match on it instead of wall-clock.


**Поправка верификатора.** Two qualifiers, neither refuting: (1) the timestamp strategy is deliberate and documented ("Best-effort aggregate" in run.rs:192, strategy doc on collect_child_entries), and the process exit code is never affected — it is built from real child exit statuses in parallel_children.rs:252-308, so the divergence is confined to observational artifacts (printed aggregate, persisted run_aggregate row, summary.json). (2) The common failure direction is fail-noisy (false "failed: no metric recorded" for a successful run); the false-success direction requires a concurrent same-export invocation against the same state DB. The proposed fix works as stated, including the necessary DB fallback for children that die without emitting Finished. Severity stays medium rather than high because exit codes remain authoritative, but stays medium rather than low because the trigger (SQLite lock contention among N concurrent child writers) is documented as real in this very module (run.rs:167-173 migration-race comment), and summary.json exists precisely for machine consumption.


#### 23. Chunk detection issues two separate full-table COUNT scans, one purely for a log line

`src/pipeline/chunked/detect.rs:284` · частота: **per-run** · категория: performance · ревьюер: chunked


```rust
match query_wrapped_row_count(src, base_query) {
```


**Почему важно.** The integer-range path runs four queries before the first row exports: bail_if_null_keyed's `COUNT(*) - COUNT(col)` (full scan), min, max (index scans), and the quoted COUNT(*) (second full scan) whose result feeds only log_chunk_sparsity_at_run — a failure there is merely warned. On a billion-row table each scan is minutes of wall time and source load, doubled for a diagnostic message. The file's own comment records 3.2 GB of temp_files measured from one of these counts.


**Фикс.** Fold the null-guard, row count, min, and max into a single `SELECT COUNT(*), COUNT(col), MIN(col), MAX(col) FROM ...` pass (requires a small multi-column query API beside query_scalar), or at least gate the sparsity COUNT behind log-level/an opt-in since it is diagnostics-only.


**Поправка верификатора.** Two nuances. (1) The 3.2 GB temp_files spill cited as evidence is already mitigated for the common `table:` shortcut: strip_select_star_from gives both COUNT queries a no-wrap fast path (detect.rs lines 16-25, 49-52), so the spill is fixed for that shape — but the full scans themselves remain, and for custom queries the materializing subquery wrap runs TWICE (null-guard + sparsity count), slightly worse than the finder stated. (2) The first COUNT (bail_if_null_keyed) is a correctness guard against silently dropping NULL-keyed rows, not diagnostics — it cannot simply be gated; only the fold-into-one-query part of the fix addresses it. The gating suggestion applies only to the line-284 sparsity COUNT, which indeed has no log_enabled! gate and runs even when info logging is suppressed. The proposed single-pass SELECT COUNT(*), COUNT(col), MIN(col), MAX(col) is dialect-portable and would work, but the Source trait only exposes query_scalar today, so it does require the new multi-column API the finder mentioned.


#### 24. Sequential chunked + keyset paths build a fresh multi-thread tokio runtime per chunk/page — the exact pattern the parallel path already fixed for races

`src/pipeline/chunked/sequential_checkpoint.rs:93` · частота: **per-chunk** · категория: performance · ревьюер: chunked


```rust
let dest = destination::create_destination(&plan.destination)?;
```


**Почему важно.** create_destination for S3/GCS/Azure constructs a tokio::runtime::Builder::new_multi_thread() runtime + opendal operator + HTTP client per call (destination/cloud.rs:117-129). This sits inside export_one_chunk_range — so it runs per chunk AND per retry attempt — and identically at exec.rs:136 (sequential, per chunk) and keyset.rs:115 (per page). exec.rs:283-284 documents why the parallel path was changed to one shared destination: 'creating one per chunk caused runtime shutdown races under load (`dispatch task is gone: runtime dropped`)'. The sequential/keyset siblings still carry both the runtime spawn/teardown cost (num_cpu worker threads each time, cold HTTP connection pool, credential re-resolution) and the latent race.


**Фикс.** Hoist create_destination out of the loop in run_chunked_sequential, export_one_chunk_range (pass &dyn Destination in, as parallel_checkpoint already does via shared_destination), and run_keyset — one destination per export, matching the parallel runners.


**Поправка верификатора.** Two overstatements. (1) The "latent race" barely applies to the sequential/keyset paths: only one destination exists at a time, `write_part_file` is a blocking call that completes before the destination (and its runtime) drops, and cloud.rs:139-145 already wraps every backend in an OpenDAL RetryLayer that retries the transient `dispatch task is gone` class — the documented race was specific to many concurrent per-chunk runtimes "under load". (2) "per retry attempt" is only true for failures at/after the destination write; line 93 sits after the source export and the `total_rows == 0` early return, so source-side retries and empty chunks never construct a destination. The remaining confirmed cost — multi-thread runtime spawn/teardown, cold HTTP/TLS pool (no connection reuse across chunks/pages), credential re-resolution per cloud upload — is per-chunk overhead amortized against a full SQL fetch + encode + upload, so medium, not high. The proposed fix (hoist to one destination per export, pass &dyn Destination down, matching parallel_checkpoint.rs:130-131 and exec.rs:285-286) is correct and has an in-repo precedent.


#### 25. Checkpoint runners open a fresh source DB connection per chunk while holding an idle one

`src/pipeline/chunked/sequential_checkpoint.rs:140` · частота: **per-chunk** · категория: performance · ревьюер: chunked


```rust
let mut src = match source::create_source(&plan.source) {
```


**Почему важно.** run_chunked_sequential_checkpoint receives `src: &mut dyn Source` but uses it only for chunk detection; every chunk then dials a brand-new connection (TLS + auth handshake, 20-100ms on managed MySQL/PG) via the quoted line — even on attempt 0 with no prior failure. The non-checkpoint sequential path (exec.rs) correctly reuses one connection across all chunks, so this is also behavioral drift between siblings. parallel_checkpoint.rs:236 likewise reconnects per claimed chunk per attempt instead of per worker lifetime. At 5,000 chunks that is minutes of pure handshake and connection-storm pressure on the source.


**Фикс.** Reuse the caller's `src` for attempt 0 in the sequential checkpoint path and reconnect only after a transient failure; in parallel_checkpoint, hold one source connection per worker across the claim loop and rebuild it only inside the retry-on-transient branch.


**Поправка верификатора.** Two overstatements: (1) "connection-storm pressure" does not apply to the sequential path — connects are strictly serial with at most two connections alive; (2) per-chunk connect is not unique to the checkpoint paths — the non-checkpoint parallel path (exec.rs:385) also dials per chunk because it spawns one thread per chunk, so the sibling drift holds only for the sequential pair. The proposed fix (reuse caller's src on attempt 0, reconnect only in the transient-retry branch; per-worker connection in parallel_checkpoint) is structurally feasible as suggested.


#### 26. record_metric persists `validated` before finalize_validate_manifest can downgrade it — metrics table and run report disagree

`src/pipeline/job.rs:383` · частота: **per-run** · категория: correctness-risk · ревьюер: orchestration


```rust
if let Err(e) = state.record_metric(
        &summary.export_name,
        &summary.run_id,
```


**Почему важно.** Ordering in run_export_job: record_metric (line 383) and summary.print() (line 407) fire before finalize_validate_manifest (line 415), which can set summary.validated = Some(false) when the manifest-aware destination pass finds a fatal verdict (finalize.rs:276-278). The export_metrics row — what `rivet metrics` and collect_child_entries read — permanently stores `validated=pass` for a run whose report, summary.json, and notification say validation failed. Two persisted observability surfaces now disagree about the same run, which is exactly the triage confusion the manifest-verification work was meant to eliminate.


**Фикс.** Move the record_metric call after finalize_manifest/finalize_validate_manifest (nothing between them depends on the metric row), or re-record the corrected validated verdict; add a test asserting the metrics row matches summary.validated after a failing manifest verification.


**Поправка верификатора.** Two refinements: (1) collect_child_entries (aggregate.rs:257-282) does NOT read the `validated` column — it only reads status/rows/files/bytes/duration/mode/error — so that cited reader is unaffected; the affected persisted reader is `rivet metrics` (cli.rs:201-202 prints validated=pass/FAIL from the row). (2) The finding understates scope: the identical bug exists at the second call site job.rs:510 + 536-537 (run_export_job_with_chunk_source, the `rivet apply` path), and summary.print() at job.rs:407 also shows the stale pass verdict on the console card. The suggested fix is valid: nothing between record_metric and finalize_validate_manifest reads the metrics table, and the metric row is an INSERT-only write with no UPDATE path, so moving the call (at both sites) is the correct remediation; "re-record" would need a new UPDATE method since record_metric inserts duplicate rows.


#### 27. Child-process flag forwarding is hand-maintained: a new run flag must be wired in three places or process mode silently diverges

`src/pipeline/parallel_children.rs:110` · частота: **n/a** · категория: duplication · ревьюер: orchestration


```rust
if validate {
            cmd.arg("--validate");
        }
        if reconcile {
            cmd.arg("--reconcile");
        }
```


**Почему важно.** run() takes the flags positionally, repackages them into RunOptions for the in-process paths, then parallel_children re-receives them positionally (with its own allow(too_many_arguments)) and re-encodes each into argv by hand. A contributor adding a run-level flag who forgets this argv block gets a flag that works in sequential and --parallel-exports modes but is silently dropped in --parallel-export-processes mode — no compiler error, no test failure, behavior just differs by parallelism mode. The RunOptions struct doc (run.rs:24-27) was written precisely to kill this class of bug, yet the child boundary reintroduces it.


**Фикс.** Give RunOptions a `to_child_args(&self) -> Vec<OsString>` next to the struct definition, pass RunOptions (not five positional params) into run_exports_as_child_processes, and add a unit test asserting every RunOptions field round-trips through the generated argv.


**Поправка верификатора.** Two minor inaccuracies, neither refuting: (1) the RunOptions doc is at run.rs:22-27, not 24-27; (2) it is arguably four hand-maintained places, not three, since run() itself (run.rs:82-94) also still threads the flags positionally under its own too_many_arguments allow, and the process-mode call at run.rs:181-190 ignores the already-built `opts`. Also worth noting: no flag currently diverges — all five RunOptions fields (validate, reconcile, resume, force, params) are forwarded correctly today, so this is a latent drift hazard, not an active bug. The proposed fix is sound: RunOptions contains exactly the forwarded flags and none of the deliberately-withheld ones (parallel modes, summary/json output), so to_child_args maps 1:1.


#### 28. Repair report fabricates per-chunk outcomes: row counts attributed to the first chunk, partial failures marked all-failed

`src/pipeline/repair_cmd.rs:187` · частота: **per-run** · категория: correctness-risk · ревьюер: integrity


```rust
// Even split is a lie — mark each as executed, attribute total to the first.
                    let mut first = true;
                    for a in executed_actions {
                        let rows = if first { delta } else { 0 };
```


**Почему важно.** execute_repair runs all repair ranges through one opaque run_chunked_sequential call, then invents per-chunk outcomes: on success, the entire row delta is credited to the first chunk and the rest report 0 rows (the comment admits the lie); on Err, EVERY parseable action is marked Failed (lines 195-199) even when earlier chunks were successfully re-exported and their files committed. The RepairReport is the audit deliverable an operator uses to decide whether mismatches are resolved — knowingly-wrong rows_written and falsely-Failed chunks send them re-repairing chunks that already succeeded, producing further duplicate files.


**Фикс.** Loop over the actions and call run_chunked_sequential once per range (ChunkSource::Precomputed(vec![range])), capturing the per-call row delta and Ok/Err as that chunk's true outcome. The ranges are independent, so per-range invocation changes no semantics.


**Поправка верификатора.** Two refinements. (1) Harm path is narrower than stated on success: RepairReport::new sums per-chunk rows, so the aggregate summary.rows_written is correct on success; only the per-chunk breakdown lies, and exec.rs's per-chunk log lines actually print the true counts. The serious half is the Err branch — run_chunked_sequential (src/pipeline/chunked/exec.rs:139-153) commits each chunk's file via write_part_file + record_part inside the loop, so an Err on chunk k leaves chunks 0..k-1 committed yet the report marks them Failed. The duplicate-file re-repair harm requires re-running from the stale report file (RepairReportSource::File); with Auto source a fresh reconcile re-derives ground truth from state. (2) The fix's claim "per-range invocation changes no semantics" is overstated: inside run_chunked_sequential the enumeration index would be 0 for every call, so the journal's ChunkStarted/PartKind::Chunk chunk_index and the filename's embedded "chunk0" label would repeat (names still unique via timestamp+nonce per chunk_part_filename), and the caller must choose abort-vs-continue on a per-range Err — current single-call semantics abort the whole set.


#### 29. Retry permanence for statement-duration timeouts hinges on substring-matching rivet's own error wording; MSSQL has no structured classification path

`src/pipeline/retry.rs:167` · частота: **per-run** · категория: correctness-risk · ревьюер: orchestration


```rust
if msg.contains("statement timeout after")      // rivet MSSQL message
        || msg.contains("due to statement timeout")  // PG statement_timeout
```


**Почему важно.** PG and MySQL errors classify on structured codes, but MSSQL errors reach classify_error only as strings — and the branch that makes deterministic duration-timeouts Permanent matches rivet's own MSSQL message text. If anyone rewords that message in the mssql source module (a plausible UX edit given the repo's active error-message polishing), the error falls through to the generic `msg.contains("timeout")` branch ten lines down and flips back to Transient — re-creating the measured 3×300s = 20-minute zero-row failure the comment itself cites. The coupling between an error string in one module and retry semantics in another is invisible at the edit site; the unit tests pin the current strings but won't fire when the producer changes its text.


**Фикс.** Have the MSSQL source return a typed, downcastable error (e.g. MssqlError::StatementTimeout) and classify on the type like the postgres::Error / mysql::Error paths; keep string matching only for third-party errors that genuinely carry no structure. At minimum, define the timeout message as a shared constant used by both the producer and classify_error.


**Поправка верификатора.** Minor points only: this is a latent fragility, not an active bug (current strings match, behavior is correct today). The finding's "per-run" frequency is accurate (classify_error runs once per failed attempt). The fix is workable, with one refinement: the MSSQL timeout is rivet's own client-side Instant check, not a tiberius server error, so a typed rivet error or shared constant is trivially available — there is no third-party-string excuse for this particular branch. Aggravating context the finder undersold: the active branch (feat/ux-clarity) is doing error-message rewording right now, and retry.rs is modified in the working tree.


#### 30. run() is a 355-line mode-selector with three diverging copies of the aggregate build and two copies of the UI-thread lifecycle

`src/pipeline/run.rs:378` · частота: **n/a** · категория: complexity · ревьюер: orchestration


```rust
let entries: Vec<_> = summaries
            .iter()
            .map(aggregate::entry_from_summary)
            .collect();
        let agg = aggregate::build(
```


**Почему важно.** run() tangles export selection, partition expansion, process/thread/sequential dispatch, global render-flag management, UI-thread spawn/clear/join (written twice in this file, with a third copy in parallel_children.rs), and aggregate construction written three times (lines 195-216 process mode, 372-401 multi-export, 402-428 single-export-with-output). The third copy already behaves differently — it writes the JSON inline with fs::write instead of aggregate::persist, so it never logs the 'written:' line and silently produces nothing on serialization failure paths handled elsewhere. Each new output flag (json_output is already threaded into all three) must be wired three times; drift is a matter of time.


**Фикс.** Extract `finish_run(entries, started_at, finished_at, mode, persist: bool, summary_output, json_output)` as the single aggregate epilogue, and an RAII `UiSession` (install tx, spawn run_ui, clear+join on Drop) used by both in-process branches and parallel_children.


**Поправка верификатора.** Two overstatements and one missed item. (1) Site 3 skipping aggregate::persist and the 'written:' line is deliberate and documented — the comment at run.rs:402-404 says 'honour both without polluting the DB or stderr', and run.rs:362-370 explains a child-level persist would duplicate the parent's run_aggregate row; only the unwrap_or_default serde swallow (writes an EMPTY file on serialization failure, worse than 'nothing') and the missing create_dir_all (write_json in aggregate.rs:234-240 creates parent dirs; site 3 does not, so --summary-output into a new directory fails only in single-export mode) are unintended drift. (2) The 'third copy' of the UI lifecycle in parallel_children.rs (lines 77-82, 272-277) does NOT use install_in_process_tx/clear_in_process_tx — events come from child-stdout reader threads — so the proposed UiSession RAII as specified (install tx, clear on Drop) fits only the two run.rs copies; parallel_children needs a variant. (3) The 'global render-flag management' tangling is already partially mitigated by an existing RAII guard (ResetMultiExport, run.rs:241-248). The finish_run(entries, ...) extraction is sound since it takes pre-built entries (site 1 builds them from the state DB via collect_child_entries, sites 2/3 from summaries).


#### 31. Single-mode runner stages the whole export on local disk, then uploads all parts serially after extraction

`src/pipeline/single.rs:335` · частота: **per-chunk** · категория: performance · ревьюер: orchestration


```rust
for (part_idx, part) in sink.completed_parts.iter().enumerate() {
        if plan.validate {
            validate_output(part.tmp.path(), plan.format, part.rows)?;
```


**Почему важно.** sink/mod.rs `maybe_split` (line 173) rolls every finished part into `completed_parts` as a live NamedTempFile, but run_single_export only writes them to the destination after the full source stream and `w.finish()` complete. Cost model: (1) local disk must hold the entire compressed dataset before the first byte is uploaded — a hard scaling cliff for snapshot/incremental/timewindow exports of large tables; (2) wall-clock = extract+encode time PLUS sum of all part uploads with zero overlap, even though the PipelinedSink already proved the team wants fetch/encode overlap; (3) a transient upload error on part k of N hits the duplicate-retry guard after the source work is fully paid for. The chunked path already commits per chunk, so the seam (commit::write_part_file is documented thread-safe) exists.


**Фикс.** Upload each completed part as it rolls over (e.g. hand rolled parts to a dedicated uploader thread that calls commit::write_part_file, joining + record_part-ing before the cursor advance), keeping the existing ADR-0001 ordering: cursor still advances only after all parts commit. This drops temp-disk to ~1 part and overlaps upload with extraction.


**Поправка верификатора.** Three overlooked constraints weaken severity and partially invalidate the fix as proposed. (1) The staging is partly deliberate: single mode is the only mode supporting full-stream quality checks (null_ratio_max/unique_columns — job.rs:37 says chunked mode cannot support them because chunks commit independently), and the current ordering guarantees nothing reaches the destination on quality failure; eager per-part upload breaks that atomicity. (2) File naming depends on the final part count (has_parts = completed_parts.len() > 1 picks name_ts.ext vs name_ts_part{i}.ext, line 332), which is unknown until the stream ends — eager upload forces a consumer-visible naming change or destination renames. (3) The fix widens the non-retryable window: today files_committed stays 0 throughout extraction so any transient source error retries from scratch; with eager uploads a mid-extraction transient error after part 0 commits trips BailDuplicateGuard. Also, parts only exist when max_file_size is set — otherwise there is exactly one part and 'sum of all part uploads' reduces to one upload; and the Chunked strategy (per-chunk commit, checkpoint resume) is the designed escape hatch for large tables. 'per-chunk' hot-path class is generous — the loop is per-part per-run.


#### 32. ExportSink's finalize/rotation contract is caller-enforced via 30 pub-in-pipeline fields

`src/pipeline/sink/mod.rs:35` · частота: **n/a** · категория: architecture · ревьюер: chunked


```rust
pub(in crate::pipeline) writer: Option<Box<dyn FormatWriter + Send>>,
```


**Почему важно.** Every field of ExportSink is pub(in crate::pipeline), so the lifecycle contract — call `if let Some(w) = sink.writer.take() { w.finish()?; }` then upload sink.tmp AND drain sink.completed_parts — lives in six call sites' heads (exec.rs:109/396, sequential_checkpoint.rs:71, parallel_checkpoint.rs:264, keyset.rs:86, single.rs). A forgotten finish() yields a footerless Parquet file; a forgotten completed_parts drain is exactly the critical data-loss finding above — the open-struct API made that bug easy to write and impossible for the compiler to catch. The struct also tangles five concerns (writer/rotation, quality tracking, shape tracking, cursor extraction, memory policy), so it keeps growing fields.


**Фикс.** Add a consuming `fn finalize(self) -> Result<FinishedExport>` that finishes the writer and returns Vec<CompletedPart> (final part included) + total_rows + dest_schema, making missed-finish and dropped-parts unrepresentable; demote the fields to private. Extract quality tracking into its own struct as a follow-up.


**Поправка верификатора.** Two refinements, neither refuting: (1) the single.rs site is line 244 (finding left it unnumbered). (2) The proposed FinishedExport payload is incomplete — besides Vec<CompletedPart>/total_rows/dest_schema, callers also consume last_cursor_value, run_quality_checks() results, column_max_bytes, and oversized_batch_count, and single.rs runs quality checks between finish and upload, so finalize must either return those or quality checking must move before consumption. The fix otherwise composes cleanly with PipelinedSink::finish(), which already returns the sink by value.


#### 33. ExportSink::track_quality string-formats every value while the benchmarked typed-hash twin (quality.rs) is production-dead

`src/pipeline/sink/mod.rs:233` · частота: **per-value** · категория: duplication · ревьюер: perf-sweep


```rust
scratch.clear();
let _ = write!(scratch, "{}", formatter.value(row));
set.insert(xxh3_64(&scratch));
```


**Почему важно.** Two parallel uniqueness implementations exist: this streaming tracker (ArrayFormatter + fmt::Display through scratch per value) and quality.rs::check_uniqueness/hash_value (typed dispatch hashing raw le_bytes — the exact 'after' that benches/hot_paths.rs bench_uniqueness advertises as shipped: 'After: HashSet<u64> via xxh3_64 on raw typed bytes — zero allocs'). check_uniqueness/run_checks/check_null_ratios have zero production callers, hidden by the file-wide `#![allow(dead_code)]` at quality.rs:4. So production pays fmt machinery per value (Int64 -> decimal text -> hash instead of 8-byte hash), the bench validates code that never runs, and the duplicated failure messages (sink/mod.rs:371 vs quality.rs:301) plus two different definitions of 'unique' (formatted text vs raw bytes) can drift silently.


**Фикс.** Make track_quality call quality::hash_value (or a per-column typed hasher built once per batch) and delete the dead batch-based run_checks/check_uniqueness/check_null_ratios surface — or wire it as the single implementation both paths share. Remove the file-wide allow(dead_code) so future dead twins surface.


**Поправка верификатора.** Two overstatements: (1) "string-formats every value" — the formatter path runs only for columns opted into quality.unique_columns (early returns at sink/mod.rs:199 and 206), not all values of all exports; (2) production is NOT the bench's "before" case — track_quality reuses a scratch Vec (with_capacity(64), cleared per value) and stores u64 hashes, not per-row String allocations, so the real gap vs typed hashing is fmt::Display machinery only, smaller than the bench's before/after delta implies. Fix caveats: quality::hash_value is private (needs pub(crate)), and unifying changes behavior — hash_value skips nulls while the sink path hashes formatted nulls (nulls currently count as duplicates via dupes = total_rows - set.len()), so unification needs an explicit null-semantics test. Otherwise the finding understates one thing: drift is not hypothetical — the "capped" message has already diverged (sink/mod.rs:360-363 appends "(set unique_max_entries higher to cover all rows)"; quality.rs:290 lacks it), and the dead_code comment at quality.rs:1-3 claiming the functions are "called from pipeline::sink, pipeline::mod" is false for check_uniqueness/check_null_ratios/run_checks/hash_value.


#### 34. Encode is never overlapped with upload; chunked sequential paths fully serialize fetch→encode→upload

`src/pipeline/sink/pipelined.rs:32` · частота: **per-chunk** · категория: performance · ревьюер: chunked


```rust
//! (`pipeline::single`); the chunked runners are not pipelined yet (each chunk
//! already runs on its own worker — intra-chunk overlap there is a follow-up).
```


**Почему важно.** PipelinedSink overlaps fetch with encode but is wired only into pipeline::single; in every chunked runner and keyset the worker does fetch+encode, then write_part_file (upload + a full second read of the part for xxh3/MD5 in commit.rs:103) strictly serially. For sequential/checkpoint-sequential users (parallel:1 — the recommended source-safe mode) the upload+hash time of each part is pure dead time on the source connection; for parallel runs every worker stalls its DB read for the duration of its upload. With cloud destinations and 100-500 MB parts that is seconds of idle source time per chunk.


**Фикс.** Lowest-cost win: hand the finished tmp + PartRecord work to a small bounded upload queue (1 in-flight) so chunk N+1's fetch/encode overlaps chunk N's upload — ordering is already safe because record_part runs post-hoc on the parent. Wiring PipelinedSink into the chunked runners covers the fetch/encode half.


**Поправка верификатора.** Three nuances: (1) this is a documented known follow-up — the evidence quote is the maintainers' own admission in the module doc, not a hidden defect; (2) the "full second read" for checksums is a deliberate documented tradeoff (manifest_writer.rs:164-167: one read for both digests, "disk-cache cheap relative to the upload") so the meaningful dead time is the upload, not the hash; (3) the pure-dead-time claim fully holds only at parallel:1 — with parallelism > 1, other workers' fetches already overlap a given worker's upload at run level. Also, "record_part runs post-hoc on the parent" is true only for the parallel engine; sequential runners call it inline, so the fix additionally needs a small drain seam plus background-error propagation and tmp-file ownership transfer.


#### 35. Rendering mode is ambient global state; RunSummary::new() performs stdout I/O despite being documented as a pure data accumulator

`src/pipeline/summary.rs:156` · частота: **n/a** · категория: architecture · ревьюер: orchestration


```rust
ipc::emit_event(&ChildEvent::Started {
            export_name: plan.export_name.clone(),
            run_id: run_id.clone(),
```


**Почему важно.** Where output goes is decided by three independent globals — the RIVET_IPC_EVENTS env var, the IN_PROCESS_TX Mutex<Option<Sender>> in ipc.rs, and the MULTI_EXPORT_MODE/MULTI_EXPORT_CONCURRENT atomics in run.rs — consulted from summary.print, ChunkProgress::new, finalize_run_report, and print_stderr_block. summary.rs:10 claims RunSummary 'makes no execution decisions itself — it is a pure data accumulator', yet its constructor writes and flushes stdout. This is exactly where a new contributor gets lost: no signature reveals that constructing a summary emits UI traffic. Worse, IN_PROCESS_TX has no RAII guard: a panic mid-run (e.g. the debug coherence panic in finalize_manifest, re-raised via resume_unwind in run.rs:304) skips clear_in_process_tx, leaking the installed sender and the stderr-owning UI thread for the rest of the process — real for tests and library callers, which run.rs explicitly supports.


**Фикс.** Thread an explicit EventSink handle (enum: Stdout / Channel(Sender) / Quiet) through RunOptions/plan into the renderers instead of ambient lookups, and wrap install_in_process_tx in a Drop guard like the existing ResetMultiExport. RunSummary::new should stop emitting; the caller that constructs it can emit Started.


**Поправка верификатора.** Two precision issues, neither fatal. (1) "its constructor writes and flushes stdout" is conditional: the stdout write+flush (ipc.rs emit(), lines 142-148) happens only when RIVET_IPC_EVENTS is set (child of --parallel-export-processes). In the default CLI path run.rs:332-333 now always installs IN_PROCESS_TX, so the constructor emits a UiMessage over a channel instead; with neither sink active it is a no-op. The substance — the constructor performs ambient-state-dependent UI emission despite the "pure data accumulator" doc — stands. (2) "leaking the installed sender and the stderr-owning UI thread for the rest of the process" is slightly overstated: the next install_in_process_tx replaces (drops) the stale sender, disconnecting the old channel and releasing the wedged UI thread, so the leak self-heals at the next run. Between the panic and that next run, however, capturing_events() stays true process-wide, suppressing/misrouting output for any in-process caller — and run.rs:236-237 explicitly promises tests/library callers a "clean slate". Also note the panic-leak scenario requires a debug build (the finalize_manifest coherence panic at finalize.rs:96-104 is cfg!(debug_assertions)-gated) or any other worker panic in --parallel-exports mode. The proposed fix is sound: a Drop guard mirroring the existing ResetMultiExport (run.rs:241-248) is the established pattern in this exact function, making the asymmetry real and the fix low-risk.


#### 36. Parquet validation decodes the entire just-written file to count rows; the footer already has num_rows

`src/pipeline/validate.rs:15` · частота: **per-chunk** · категория: performance · ревьюер: integrity · также найдено: perf-sweep


```rust
let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            let mut count = 0usize;
            for batch in reader {
                count += batch?.num_rows();
            }
```


**Почему важно.** Per part, this re-decompresses and re-decodes every page of the Parquet file the pipeline just encoded — a full second CPU pass over the data path's output (on top of the separate compute_part_checksums streaming read). Parquet's footer metadata carries the authoritative row count; the ArrowWriter wrote it from the same batches being verified, so full decode adds cost without adding signal for the stated purpose (row-count check). With validate=true on a chunked run, this can approach doubling encode-side CPU.


**Фикс.** Use `builder.metadata().file_metadata().num_rows()` after try_new (which already parses and checksum-validates the footer). If a full-decode integrity tier is wanted, make it the documented `--validate --deep` mode rather than the default per-part cost.


**Поправка верификатора.** Two minor inaccuracies, neither refuting: (1) try_new parses the footer (magic bytes + thrift) but does not "checksum-validate" it — Parquet footers carry no checksum. (2) The full decode is not entirely signal-free: it verifies data pages actually decompress/decode, catching encoder-side corruption that the footer count, compute_part_checksums (hashes local bytes as-written), and the store-MD5 transit check cannot. The finder's proposed --deep tier already covers this, so the fix direction is sound. Also note compute_part_checksums (manifest_writer.rs:168) self-documents its re-read as "disk-cache cheap" — the real duplicated cost is the decode CPU, not the extra I/O.


#### 37. small-table escape silently overrides explicit chunk_count/chunk_by_days/chunk_checkpoint and trusts a stats estimate to drop the bounded-extraction safety shape

`src/plan/build.rs:316` · частота: **per-run** · категория: correctness-risk · ревьюер: plan-config


```rust
if introspection.row_estimate > 0 && (introspection.row_estimate as usize) <= chunk_size {
        log::info!(
            "export '{}': {} has ~{} rows ≤ chunk_size {}; downgrading chunked → snapshot",
```


**Почему важно.** The escape fires whenever the introspection probe ran (chunk_column unset, or chunk_size_memory_mb set) and returns Snapshot before any other knob is honored. (a) An explicit `chunk_count: 5` or `chunk_by_days: 7` is silently collapsed to one file — an output-layout change downstream consumers can depend on; with chunk_count the comparison even uses `chunk_size` (default 100k), a value the chunk_count semantics document as 'recomputed at detect time', so the decision is made on a number that would never be used. (b) `chunk_checkpoint: true` is dropped (Snapshot is not resumable), so --resume later warns 'no effect' on a config that explicitly enabled checkpointing. (c) `reltuples`/stats are estimates — plan_cmd.rs itself documents reltuples=1130 on a 30-row table; a stale-stats large table gets downgraded to one unbounded snapshot query, exactly the RSS/long-query shape chunked mode exists to avoid (worst on MySQL, which has no server-side cursor).


**Фикс.** Gate the downgrade: skip it when chunk_count, chunk_by_days, chunk_checkpoint, or chunk_by_key is explicitly set, and when chunk_count is Some never compare against the static chunk_size. Consider requiring the estimate to be meaningfully below chunk_size (e.g. <= chunk_size/2) to absorb stats staleness, and emit the downgrade as a validate_plan Warning diagnostic rather than only a log::info.


**Поправка верификатора.** Two overstatements. (1) The RSS claim is wrong for the named worst case: MySQL extraction streams via exec_iter "rows from the wire one-at-a-time" with bounded batching (src/source/mysql/mod.rs:478), and PG snapshot uses DECLARE CURSOR which build.rs:383 itself calls "already bounded" — a stale-stats false downgrade yields one long query (timeout/connection exposure, lost resumability), not a memory blowup. (2) The cited plan_cmd.rs reltuples=1130-on-30-rows example (line 277-278) is an OVER-estimate, the safe direction that prevents the escape; the dangerous under-estimate direction is plausible (post-bulk-load staleness, MySQL TABLE_ROWS drift) but not what that comment documents. The proposed fix is otherwise sound and feasible: chunk_checkpoint is a plain bool defaulting to false, so explicit-true is detectable; gating on chunk_count/chunk_by_days/chunk_checkpoint/chunk_by_key plus a validate_plan Warning would work. Note the escape only reaches configs without explicit chunk_column or with chunk_size_memory_mb set (fast path at build.rs:206 returns early otherwise), which the finding correctly stated.


#### 38. build_plan does hidden blocking DB I/O; `rivet plan` opens three separate connections and introspects the same table up to three times per export

`src/plan/build.rs:250` · частота: **per-run** · категория: architecture · ревьюер: plan-config


```rust
let introspection = match config.source.source_type {
        crate::config::SourceType::Postgres => {
            crate::source::postgres::introspect_pg_table_for_chunking(
```


**Почему важно.** resolve_chunked_strategy opens its own connection (connect_client + 3 catalog queries) whenever chunk_column is unset or chunk_size_memory_mb/chunk_by_key is set — so build_plan, whose doc claims it merely resolves config, silently does network I/O in every caller (job.rs:217, plan_cmd.rs:124, reconcile_cmd.rs:51, repair_cmd.rs:71). In plan_cmd.rs the same export then gets connection #2 from preflight::get_export_diagnostic (preflight/mod.rs:68) re-querying the same row stats the probe just fetched and discarded, and connection #3 from compute_plan_data's source::create_source for min/max. That is 3 TLS handshakes and duplicated catalog round-trips per export, ~3N connections for a `rivet plan` over N exports, plus a comment in build.rs:203-205 still advertising a 'no-network plan-build invariant' that only holds on one path.


**Фикс.** Make introspection an explicit input: have callers run the probe once and pass `Option<TableIntrospection>` (or a source handle) into build_plan/preflight/compute_plan_data, or cache the probe result per (source,table) for the duration of one CLI invocation. Update the stale no-network comment either way.


**Поправка верификатора.** Two sub-claims overstate. (1) The build.rs:203-205 comment is not a stale unconditional invariant — it explicitly self-scopes: "Preserves the no-network plan-build invariant for users who hand-tune chunked-mode the old way", i.e. the authors documented that the invariant only holds on the fast path; the conditional probe is a deliberate, documented trade-off (resolve_chunked_strategy's doc describes the shared introspection round-trip). (2) "silently does network I/O in every caller" only applies to mode:chunked exports on the auto-resolve path (chunk_column unset, or chunk_size_memory_mb/chunk_by_key set); explicit chunk_column skips the probe entirely (build.rs:206-217), and Full/Incremental/TimeWindow never probe. Also, preflight's row estimate is EXPLAIN-based on the query, not a literal re-read of the same pg_class row — overlapping information, different mechanism. The genuinely unintended waste is the triplicated connections within one plan_cmd invocation, where the TableIntrospection is discarded and nothing is cached. The proposed fix (pass Option<TableIntrospection>/source handle, or cache per invocation) is sound and workable.


#### 39. anyhow-everywhere forces substring-matching error taxonomy; modules coupled by error-message text via in-comment 'CONTRACT'

`src/preflight/doctor.rs:179` · частота: **n/a** · категория: architecture · ревьюер: shell


```rust
// CONTRACT: the pattern below must match the error text emitted by
    // `enforce_sas_expiry` in destination/azure.rs:
    //   "Azure SAS token already expired (se=…)"
    // If that message ever changes, update both places together.
    if msg.contains("already expired") && msg.contains("sas") {
```


**Почему важно.** src/error.rs is literally `pub type Result<T> = anyhow::Result<T>;` — there is no typed error anywhere in the crate. The consequence is that behavior-bearing decisions are keyed on formatted strings: doctor's categorize_source_error/categorize_dest_error match `msg.contains("password")`, `contains("token")` etc., and src/pipeline/retry.rs:101-141 decides retryable-vs-fatal the same way (`contains("connection reset")`, `contains("invalid_grant")`...). A driver bumping its message wording silently flips retry classification or routes the wrong remediation hint, with zero compiler feedback. The quoted comment is the codebase admitting the coupling instead of fixing it.


**Фикс.** Introduce a small typed error enum at the source/destination boundary (AuthFailed, Connectivity, TlsHandshake, SasExpired, Transient, Fatal{msg}) carried via `anyhow::Error::context`/custom type and matched with `downcast_ref` in doctor/retry. Driver-substring sniffing then lives in exactly one constructor per backend instead of three consumers.


**Поправка верификатора.** Three overstatements. (1) retry.rs is NOT purely string-matched: classify_error first downcasts postgres::Error to SQLSTATE (classify_pg_sqlstate, retry.rs:79-87,204) and mysql::Error to numeric codes (classify_mysql_error, retry.rs:89-95,282); string matching is the documented fallback for IO/cloud/OpenDAL errors lacking structured codes — so the highest-stakes retry decisions (SQL driver errors mid-extraction) already have typed classification, narrowing the blast radius. (2) "No typed error anywhere in the crate" is slightly overstated: ManifestInconsistency (src/manifest.rs:284) implements std::error::Error, though none exists at the source/dest boundary. (3) The fix's "sniffing lives in exactly one constructor per backend" understates that many fallback strings originate in third-party crates (opendal, hyper, gcloud auth) deep in anyhow chains; sniffing must survive at whatever boundary wraps them — though opendal::Error does expose kind()/is_temporary() that could replace the "(temporary)" substring match. Also note doctor miscategorization is UX-only (wrong hint), not behavioral.


#### 40. doctor destination dedup key omits the path — second local destination is never write-probed

`src/preflight/doctor.rs:35` · частота: **per-run** · категория: correctness-risk · ревьюер: shell


```rust
let dest_key = format!(
            "{:?}:{}:{}",
            export.destination.destination_type,
            export.destination.bucket.as_deref().unwrap_or("-"),
            export.destination.endpoint.as_deref().unwrap_or("-"),
        );
```


**Почему важно.** For two local-type destinations with different `path:` values the key is identical ("Local:-:-"), so doctor probes only the first path and prints nothing for the second — an unwritable second directory passes doctor and then fails at run time, which is precisely the failure mode doctor exists to catch (its exit-code fix F-NEW-A says so). The sibling dedup in preflight/mod.rs:126-131 includes `path` in the key, so the two copies of this logic have already drifted apart.


**Фикс.** Include `export.destination.path` (and prefix) in doctor's dest_key — or better, extract one `destination_identity(dest) -> String` helper used by both doctor and check so the keys cannot drift again.


**Поправка верификатора.** Fix suggestion says to include "path (and prefix)" — there is no prefix field on DestinationConfig at these sites; path alone (matching mod.rs:131) is the correct addition. Also note the same key omission silently skips per-prefix probes for two S3/GCS exports to the same bucket with different paths, though auth there is per-bucket so the Local case is the sharp edge.


#### 41. rivet check hard-fails for MSSQL configs even though the type-report path it gates is MSSQL-capable

`src/preflight/mod.rs:113` · частота: **per-run** · категория: correctness-risk · ревьюер: shell


```rust
SourceType::Mssql => anyhow::bail!("mssql check not yet implemented (scaffold)"),
```


**Почему важно.** MSSQL is a fully supported source for `run`, `doctor` (doctor.rs handles Mssql via connect_with_tls), and the type report (type_report.rs:94 constructs MssqlSource). But check() bails on this match arm before reaching the destination-credential preflight or the type report, so `rivet check --type-report` — step 2 of the documented onboarding flow ('Step 2 — column-type & schema report') — returns rc=1 for every MSSQL user, blocking working functionality behind an unimplemented diagnostics step.


**Фикс.** Skip only the EXPLAIN-based diagnostics for Mssql (print a one-line 'diagnostics not yet available for mssql' note) and fall through to the destination preflight and type-report sections, which already work.


**Поправка верификатора.** Minor nits only: the finding cites 'doctor.rs' — the actual path is src/preflight/doctor.rs — and 'hot_path: per-run' overstates it slightly (check is a per-CLI-invocation diagnostics command, not in the export hot path). The proposed fix is sound: check_postgres/check_mysql are EXPLAIN-based source diagnostics (src/preflight/postgres.rs uses EXPLAIN throughout), while the destination preflight and type-report sections below the match are engine-agnostic or already MSSQL-capable, so a skip-with-note for the Mssql arm would let the rest of check() work as designed.


#### 42. quality.rs batch-check API (check_null_ratios / check_uniqueness / run_checks / hash_value) is production-dead; the live sink copy has drifted

`src/quality.rs:1` · частота: **n/a** · категория: duplication · ревьюер: integrity


```rust
// Functions in this module are called from pipeline::sink, pipeline::mod, and integration tests
// via the library crate. The binary re-declares this module but does not call all functions
// directly, producing dead_code warnings only for the bin target.
#![allow(dead_code)]
```


**Почему важно.** The header comment is false: a repo-wide grep shows zero production callers of check_null_ratios, check_uniqueness, run_checks, or the typed hash_value — ExportSink::track_quality (src/pipeline/sink/mod.rs:198) reimplements null-ratio and uniqueness inline. The copies have already drifted three ways: NULL semantics (see separate finding), cap-warning wording ('result may be incomplete' vs '...(set unique_max_entries higher...)'), and — most damning — the typed-hash optimization (hash_value, 'typed dispatch to avoid string formatting') landed only in the dead copy while the live path still string-formats every value. benches/hot_paths.rs:569 ('Mirrors check_uniqueness() in src/quality.rs') benchmarks the dead implementation, so the recorded perf win never reached production. ~290 lines of tested logic that the next contributor will reasonably but wrongly assume is the gate.


**Фикс.** Delete the batch-based check_null_ratios/check_uniqueness/run_checks or make ExportSink::track_quality delegate to quality::hash_value (downcast once per column, not per row). Re-point the bench at the live sink path and fix the module header.


**Поправка верификатора.** Three refinements. (1) The header is only partially false: check_row_count, failure_message, QualityIssue, and Severity ARE live (sink/mod.rs:337, job.rs:32/51, single.rs:280) — but the "integration tests via the library crate" claim is impossible since lib.rs:72 declares `pub(crate) mod quality`, unreachable from tests/ crates; the four batch functions (check_null_ratios, check_uniqueness, run_checks, hash_value) are indeed production-dead. (2) The live sink path is not as bad as the bench's "before" arm: track_quality reuses a scratch Vec via write! (no per-row String alloc) and builds the ArrayFormatter once per column per batch, so the unrealized perf win is smaller than the bench records. (3) The fix's "delegate to quality::hash_value (downcast once per column)" oversells delegation — hash_value downcasts inside every per-row call, so once-per-column dispatch requires restructuring it. Severity lowered from high to medium: no production misbehavior follows from this finding alone (the NULL-semantics divergence is filed separately); the cost is dead-but-tested drifted code, a misleading header, and a bench validating an optimization that never shipped.


#### 43. Three hand-rolled build_array dispatchers (~45 type-arm loops) have already drifted on mismatch policy: bail vs null vs garbage-fallback

`src/source/mssql/arrow_convert.rs:217` · частота: **n/a** · категория: duplication · ревьюер: connectors


```rust
match cell(row, idx) {
    Some(ColumnData::I16(Some(v))) => b.append_value(*v),
    Some(ColumnData::U8(Some(v))) => b.append_value(*v as i16),
    _ => b.append_null(),
}
```


**Почему важно.** The per-engine cell read is genuinely engine-specific (the mysql file's comment at arrow_convert.rs:361-367 defends this), but what has actually drifted is the POLICY layer that should be shared: on an unexpected value, MSSQL's simple! macro bails loudly, yet this hand-written Int16 arm in the same file silently nulls (`_ => b.append_null()`); MySQL Int16/Int32 null on unparseable bytes while MySQL Int64 falls back to garbage bit-decode; PG bails. Capacity hints also drifted (PG/MySQL builders use with_capacity, MSSQL Utf8/Binary use `StringBuilder::new()`). Every new type added to the fidelity system is now implemented three times under three different error philosophies — the next contributor cannot know which one is the contract.


**Фикс.** Keep the engine-specific reads but extract the policy: a shared per-type append skeleton (macro or generic fn taking a `fn(&Row, usize) -> Result<Option<T>>` reader) that owns capacity, null handling, and the mismatch-is-error rule; or minimally, write down the mismatch contract once and add a cross-engine conformance test that feeds each builder an unexpected variant and asserts identical behavior.


**Поправка верификатора.** Two details: (1) "PG bails" is wrong — PG's scalar arms use tokio-postgres `row.get()` / `append_option(row.get(..))` (postgres/arrow_convert.rs:394-438), which PANICS on a wire/type mismatch; the bails there are only for unsupported target types. So the drift is four policies (bail / silent-null / garbage-fallback / panic), not three — worse than claimed. (2) The suggested cross-engine conformance test "feeding each builder an unexpected variant" is impractical as a unit test: tiberius and tokio-postgres Rows are not constructible in-process; it would need integration tests using mis-declared column overrides. Also note MySQL's Int64 bit-decode fallback is commented and deliberate (BIT(n>1) wire format, mysql:446-452), so that single divergence is engine-specific by design, though it still garbles non-numeric bytes routed there via an override. The MSSQL Int16 arm could not use `simple!` directly (i16/u8 or-pattern binding type mismatch), which explains — but does not excuse — the policy drift.


#### 44. MSSQL with no TLS config silently disables certificate validation (trust_cert)

`src/source/mssql/mod.rs:159` · частота: **per-run** · категория: correctness-risk · ревьюер: connectors


```rust
None => config.trust_cert(),
```


**Почему важно.** When the user provides no source.tls block, the MSSQL connector calls tiberius' trust_cert() — accept any certificate — so the mandatory TDS TLS handshake is encrypted but unauthenticated (trivially MITM-able). PG/MySQL in the same situation fall back to plaintext and warn_if_tls_disabled fires, but its message ("result rows cross the network in plaintext") is wrong for MSSQL and nothing tells the operator that cert validation was turned off. Also note `Some(cfg)` with mode: require/verify-ca is ignored entirely — only accept_invalid_certs and ca_file are honored, so the shared TlsMode contract silently doesn't apply to this engine.


**Фикс.** Default `None` to full verification (no trust_cert), map TlsMode::Require/VerifyCa explicitly onto tiberius knobs like build_mysql_ssl_opts does, and emit an MSSQL-specific warning when cert validation is relaxed.


**Поправка верификатора.** Two refinements. (1) "Nothing tells the operator" is overstated: warn_if_tls_disabled (src/source/mod.rs:246) DOES fire for MSSQL with tls=None and its advice (add verify-full) is correct — only its "plaintext" wording is wrong for MSSQL, where the TDS handshake is encrypted-but-unauthenticated. (2) The ignored-TlsMode divergence on Some(cfg) errs strict, not lax: mode: require (PG semantics: encrypt, skip verification) gets FULL system-truststore verification on MSSQL — a connect-failure risk against self-signed certs, not a security hole; accept_invalid_hostnames is also silently ignored. Fix caveat: defaulting None to full verification will break zero-config connects to out-of-the-box SQL Server (self-signed cert is the install default) — likely why trust_cert() was reached for — so the fix needs the accept_invalid_certs escape hatch documented plus a release note.


#### 45. MySQL Int64 builder guesses per value between decimal-text and raw BIT bytes — ASCII-digit BIT values silently misdecoded

`src/source/mysql/arrow_convert.rs:450` · частота: **per-value** · категория: correctness-risk · ревьюер: connectors


```rust
let v =
    atoi::atoi::<i64>(bv).unwrap_or_else(|| bit_bytes_to_u64(bv) as i64);
```


**Почему важно.** BIT(n>1) arrives as raw big-endian bytes; the code tries a decimal-text parse FIRST and only falls back to bit interpretation. A BIT(16) value 0x3132 is the bytes "12": atoi succeeds and writes 12 instead of the true 12594 — silent wrong data whenever every byte of a BIT value happens to be 0x30..0x39 (BIT(8) flag values 48-57 are always wrong). Conversely, a TEXT column overridden to int64 whose value is non-numeric falls into bit_bytes_to_u64 and emits big-endian garbage instead of null/error. Root cause: unlike the PG build_array (which receives pg_type), the MySQL build_array signature dropped the wire column type, so it cannot dispatch deterministically.


**Фикс.** Thread the mysql::Column (or just `is_bit: bool`) into build_array — the schema builder already iterates the columns — and dispatch: BIT columns always bit_bytes_to_u64, everything else atoi with null/error on failure (matching the Int16/Int32 arms). Never guess per value.


**Поправка верификатора.** The finding slightly understates the blast radius: atoi 3.0 (verified in the crate source) ignores trailing non-digit bytes, so corruption is not limited to values where EVERY byte is 0x30..0x39 — any BIT value whose FIRST byte is an ASCII digit is misdecoded (BIT(16) 0x31FF -> 1 instead of 12799; ~3.9% of uniform BIT(16) values, not 0.15%). Everything else checks out: line 237 maps MYSQL_TYPE_BIT (n>1) to RivetType::Int64; exec_iter (binary protocol) delivers BIT as Value::Bytes while native BIGINT arrives as Value::Int; PG's build_array (postgres/arrow_convert.rs:378) does receive pg_type while MySQL's does not; the proposed fix (thread is_bit into build_array, dispatch deterministically, null on atoi failure like the Int16/Int32/UInt64 arms) is sound. Existing unit tests (mod.rs:698-711) only exercise bit_bytes_to_u64 with 0x00/0x01/0xFF/empty — no coverage of ASCII-digit bytes, so a regression test is warranted per the project's own CLAUDE.md rule.


#### 46. PG NUMERIC decode allocates ~6 times per value: wire -> BigDecimal -> three Strings -> re-parse to i128

`src/source/pg_numeric_wire.rs:53` · частота: **per-value** · категория: performance · ревьюер: connectors · также найдено: perf-sweep


```rust
Some(bd.normalized().to_plain_string().trim().to_string())
```


**Почему важно.** Per NUMERIC cell the chain is: Vec<i16> limbs + Vec<u8> cents + BigInt::from_radix_be + BigDecimal::new, then normalized() (another BigInt), then to_plain_string() (String #1), then trim().to_string() (String #2 — trim is provably a no-op on to_plain_string output), then pg_numeric_optional_plain in postgres/arrow_convert.rs:713-714 trims again and does `t.to_string()` (String #3), then decimal_str_to_scaled_i128 re-parses the digits character by character. The wire format is base-10000 limbs; for precision <= 38 (the overwhelmingly common Decimal128 case) the scaled i128 can be computed directly from the limbs with zero heap allocation. Financial tables — the core workload — pay this on every decimal cell.


**Фикс.** Add a direct limbs->scaled-i128 fast path in pg_numeric_wire (fold base-10000 limbs with checked_mul/checked_add, adjust by weight vs target scale), falling back to the BigDecimal path only for Decimal256/overflow. At minimum delete the redundant .trim().to_string() layers — Strings #2 and #3 are pure waste.


**Поправка верификатора.** The diagnosis is fully accurate — if anything "~6 allocations" is an undercount (limbs Vec, cents Vec, BigInt, normalized() BigInt, plain String, trim copy, caller copy, plus a pad-buffer alloc in decimal_str_to_scaled_i128:99 that normalized() makes more frequent by stripping trailing zeros the parser then re-pads). Two caveats: (1) the trim in the Err fallback branch at arrow_convert.rs:724-725 serves real cast-to-text callers and must be kept — only the wire-path trims are deletable; (2) severity "high" is unsubstantiated by any measurement — extraction streams batches over the network and this is pure CPU overhead per non-null decimal cell, so medium until a benchmark shows decode dominates wall time.


#### 47. PG JSON/JSONB decoded via serde_json::Value round-trip: per-value parse+reserialize, key reordering, f64 number mangling

`src/source/postgres/arrow_convert.rs:550` · частота: **per-value** · категория: correctness-risk · ревьюер: connectors · также найдено: perf-sweep


```rust
match row.try_get::<_, Option<Json<JsonValue>>>(col_idx)? {
    None => b.append_null(),
    Some(Json(v)) => b.append_value(&serde_json::to_string(&v)?),
```


**Почему важно.** Every JSON cell is fully parsed into a serde_json::Value tree and re-serialized — massive per-value allocation churn on document-heavy tables. Fidelity is also violated: serde_json is built without preserve_order (Cargo.toml: serde_json = "1"), so Map is a BTreeMap and PG `json` columns (which preserve original text in the DB) come out with alphabetically reordered keys and stripped whitespace; and without arbitrary_precision, non-integer JSON numbers round-trip through f64, so >17-significant-digit values (high-precision monetary amounts inside JSON) are silently altered. For a type-fidelity product this is silent payload rewriting.


**Фикс.** Replace the round-trip with a raw-bytes FromSql adapter: for JSONB strip the leading 0x01 version byte, for JSON take the bytes as-is, validate UTF-8 with simdutf8 (as AnyAsString already does), and append the original text directly. Zero parse, zero fidelity loss.


**Поправка верификатора.** The key-reordering/BTreeMap claim is false: schemars is declared with features=["preserve_order"] (Cargo.toml line 94), and Cargo feature unification enables serde_json/preserve_order crate-wide — verified via `cargo tree -e features -i serde_json` and Cargo.lock (serde_json depends on indexmap, which only happens with preserve_order). Map is an IndexMap; key insertion order survives the round-trip. The finder read only `serde_json = "1"` and missed the transitive feature. What survives: per-value parse+reserialize churn, f64 mangling of non-integer >17-sig-digit numbers and >u64 integers (arbitrary_precision confirmed off), number-format normalization, and whitespace stripping (json columns only; jsonb is already PG-normalized). The proposed raw-bytes fix is valid: JSONB wire format is 0x01 + JSON text, and the simdutf8 pattern already exists in this file (AnyAsString, lines 332-339).


#### 48. PG text and bytea decode allocates an owned String/Vec per cell where the driver offers zero-copy borrows

`src/source/postgres/arrow_convert.rs:543` · частота: **per-value** · категория: performance · ревьюер: perf-sweep · также найдено: connectors


```rust
let val: Option<String> = row.get(col_idx);
b.append_option(val.as_deref());
```


**Почему важно.** Text columns are the highest byte-volume path in the whole pipeline. `row.get::<_, Option<String>>` heap-allocates and memcpys every cell, then immediately copies again into the StringBuilder and drops the String. The postgres crate implements FromSql for `&str` (TEXT/VARCHAR/BPCHAR/NAME) and `&[u8]` (BYTEA) that borrow directly from the row buffer — the BYTEA arm at line 446 (`row.get::<_, Option<Vec<u8>>>`) has the identical problem. The MySQL decoder already does the zero-copy equivalent (`bytes_to_str(bv)` straight into the builder); PG — the flagship engine — is strictly worse for no reason.


**Фикс.** Change to `row.get::<_, Option<&str>>(col_idx)` in the TEXT arm and `row.get::<_, Option<&[u8]>>(col_idx)` in the Binary arm of build_array; both borrows live as long as `row`, which outlives the append. One alloc+copy per cell eliminated.


**Поправка верификатора.** Diagnosis and fix are accurate, but severity is inflated. The avoidable cost is one malloc/free plus one extra memcpy per text/bytea cell — the copy into the StringBuilder/BinaryBuilder is unavoidable either way — and the surrounding pipeline (FETCH round-trips over the cursor, NUMERIC wire decode, Parquet encode/compression) dominates wall time, so this is a single-digit-percent win, not a "high" regression. Also, the zero-copy claim only applies to the TEXT/VARCHAR/BPCHAR/NAME and BYTEA arms; the JSON/NUMERIC/UUID/interval arms of build_pg_text_array allocate inherently and cannot be borrowed. The proposed fix is safe: FromSql for &str accepts exactly the four matched text types and &[u8] accepts BYTEA (the only type mapped to RivetType::Binary at line 148), so accepts() behavior is unchanged.


#### 49. every state-store method hand-duplicates SQL + row mapping across two backend match arms

`src/state/metrics.rs:125` · частота: **per-run** · категория: duplication · ревьюер: state-types


```rust
"SELECT export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb, \
                             status, error_message, tuning_profile, format, mode, \
                             files_produced, bytes_written, retries, validated, schema_changed \
                             FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT ?2",
```


**Почему важно.** get_metrics defines a `cols` string (line 113) for the Postgres branch, then re-spells the identical 16-column list twice more as inline literals for the SQLite branch — three copies of one column list in one function, plus two copies of the 16-field row-mapping closure. The same match-on-StateConn duplication repeats in all nine state files (~25 methods): checkpoint.rs duplicates complete_chunk_task SQL verbatim between the &self and _at_ref variants (lines 272 and 340), journal_store.rs/run_aggregate.rs interpolate LIMIT via format! in the PG branch while SQLite binds it as a parameter, and journal_store.rs uses INSERT OR REPLACE vs ON CONFLICT DO UPDATE (different delete-then-insert semantics if FKs are ever added). Every new column or table touches 4-6 sites that can silently drift; the lexicographic-cursor asymmetry (separate finding) shows drift of this shape already shipping.


**Фикс.** Introduce one internal query-execution seam (e.g. fn exec(&self, sql: &str, params: ...) and fn query_map<T>(...) that dispatches on StateConn internally, using pg_sql() for placeholder translation) so each statement and each row-mapper is written exactly once; keep backend-specific SQL only where semantics genuinely differ (the SKIP LOCKED claim).


**Поправка верификатора.** Two refinements. (1) The checkpoint.rs `complete_chunk_task` vs `complete_chunk_task_at_ref` pair is not pure copy-paste negligence: the `_at_ref` variant dispatches on `StateRef` and opens a fresh connection (for contexts without `&self`), so the function pair is deliberate — but the SQL literal is verbatim identical at lines 272-274 and 340-342 and trivially hoistable to a shared const, so the drift risk stands. (2) The proposed `query_map<T>` seam is harder than the fix implies: rusqlite and postgres have incompatible Row types, error types, and ToSql traits, so a unified seam needs a param-value enum plus a row-accessor wrapper, not just a function. The SKIP LOCKED carve-out in the fix is correct (checkpoint.rs:253 is genuinely PG-only). Severity lowered from high to medium: this is real, extensive duplication with verified semantic asymmetries, but no current bug is demonstrated in this finding — the divergences found (INSERT OR REPLACE vs ON CONFLICT, format!-interpolated LIMIT vs bound param) are presently benign (limit is an internal usize; run_journal has no FKs today).


#### 50. state(pg) backend hard-codes NoTls while its own warning recommends an sslmode=require URL that cannot work

`src/state/mod.rs:611` · частота: **per-run** · категория: correctness-risk · ревьюер: state-types


```rust
if !is_local {
            log::warn!(
                "state(pg): connecting to a remote host without TLS; \
                 set RIVET_STATE_URL to a sslmode=require URL for production use"
            );
        }
        let mut client = postgres::Client::connect(url, postgres::NoTls).map_err(|e| {
```


**Почему важно.** Every Postgres connection in the state layer (here, and the per-chunk worker connects in checkpoint.rs lines 234 and 324 and 352) passes postgres::NoTls, so the binary cannot negotiate TLS at all — with the postgres crate, a URL carrying sslmode=require fails to connect when the connector is NoTls. The remediation the warning prints is therefore impossible to follow: the operator sets sslmode=require for their K8s deployment and the state store stops opening entirely, or they strip it and ship checkpoint/cursor state (and the password in RIVET_STATE_URL traffic) in cleartext. This violates the project's own rule that remediation hints must actually work from the state the user is in.


**Фикс.** Compile the postgres crate with a TLS connector (postgres-native-tls or rustls) and select TLS when the URL requests it; until then, change the warning to state truthfully that the state backend does not support TLS so operators can make an informed choice.


**Поправка верификатора.** Two refinements. (1) The fix is cheaper than stated: no need to "compile the postgres crate with a TLS connector" — the workspace already depends on postgres-native-tls 0.5 and has a shared helper (src/source/postgres/mod.rs:352 connect_client + src/source/tls.rs build_native_tls) that the source driver uses; the state backend just bypasses it. The right fix is to route state connections through that existing connector (honoring sslmode or a TlsConfig), updating all four sites (mod.rs:617, checkpoint.rs:234/324/352) since StateRef::Postgres reconnects per chunk worker. (2) Minor overstatement in "why": the password is typically NOT sent in cleartext even without TLS (Postgres defaults to SCRAM-SHA-256); checkpoint/cursor data and queries do travel cleartext, though. Frequency note: the checkpoint.rs sites are per-chunk-claim, not strictly per-run, but this is a correctness issue so frequency is immaterial.


#### 51. committed-boundary cursor guard compares numerically-valued cursors lexicographically

`src/state/progression.rs:43` · частота: **per-run** · категория: correctness-risk · ревьюер: state-types


```rust
last_committed_cursor = CASE
                    WHEN export_progression.last_committed_cursor IS NULL
                         OR export_progression.last_committed_cursor < excluded.last_committed_cursor
                    THEN excluded.last_committed_cursor
                    ELSE export_progression.last_committed_cursor END,
```


**Почему важно.** The non-regression guard is a TEXT column compared with SQL string `<`. Cursor values are arbitrary strings: RFC3339 timestamps order correctly, but integer-PK cursors do not — after a run commits cursor "999", a later run committing "1000" fails the `"999" < "1000"` lexicographic test and the committed boundary silently freezes (it later jumps erratically at "9990"). record_committed_incremental receives exactly these values from RunStore::commit (src/pipeline/run_store.rs:125) for any export using a numeric cursor column. The actual resume cursor (export_state, updated unconditionally in cursor.rs) is unaffected, but the ADR-0008 operator-facing trust boundary shown by `rivet state progression` reports stale committed positions — the artifact whose entire purpose is to be trusted. The test suite pins only date-string behavior (committed_cursor_does_not_regress_lexicographically), masking the gap.


**Фикс.** Do the regression check in Rust before the upsert: read the stored cursor, compare numerically when both values parse as integers (else as strings), and only write when advancing. Add a regression test with integer cursors crossing a digit-count boundary (999 → 1000).


**Поправка верификатора.** The finding is accurate in every checked detail (quote verbatim at progression.rs:43-47; caller at run_store.rs:125 exact; column is TEXT per state/mod.rs:125; test at progression.rs:312 pins only date strings). Two refinements: (1) Float64 cursors (supported in sink/cursor.rs:60-66) are equally affected, not just integer PKs, so the fix's parse-as-integer fallback should also handle floats or fall back carefully; (2) the suggested Rust-side read-then-write introduces a read-modify-write race on the Postgres backend that the current single-statement upsert avoids — acceptable since the write is advisory (warn-on-fail, PG7) and exports are single-writer per run, but the fix should note it.



### ⬜ LOW

#### 52. Streamed cloud upload crosses the blocking→async runtime boundary once per 8 KiB

`src/destination/cloud.rs:184` · частота: **per-chunk** · категория: performance · ревьюер: io


```rust
let mut src = std::fs::File::open(local_path)?;
let mut dst = self.op.writer(&key)?.into_std_write();
std::io::copy(&mut src, &mut dst)?;
```


**Почему важно.** `std::io::copy` to a generic writer uses an 8 KiB buffer, and opendal 0.55's `StdWriter::write` does `self.handle.block_on(w.write(buf))` per call (verified in opendal-0.55.0/src/blocking/write/std_writer.rs) plus a Bytes copy of each slice. A 1 GiB part = ~131,000 thread-park/unpark round-trips against the destination's tokio runtime. This path is taken by exactly the parts that matter most — anything that doesn't fit the 64 MiB one-shot budget, i.e. the largest transfers — and can cap upload throughput well below disk/NIC speed.


**Фикс.** Replace `std::io::copy` with a manual loop reading multi-MiB chunks (e.g. 8 MiB, aligned with the store's multipart part size) and calling `dst.write` once per chunk — or wrap `dst` in `std::io::BufWriter::with_capacity(8 << 20, dst)` and keep `io::copy`.


**Поправка верификатора.** The mechanism (block_on once per 8 KiB) is real, but the cost model is wrong by ~32x and the throughput claim is refuted. opendal 0.55's StdWriter wraps FuturesAsyncWriter, which buffers internally via oio::FlexBuf::new(256 * 1024): for 31 of 32 calls, poll_write memcpys the 8 KiB slice and returns Ready on the FIRST poll, and tokio's Handle::block_on polls before parking — so those calls incur zero park/unpark, just a ~100-300ns block_on entry plus a memcpy that any buffering does anyway. Actual sink work (and possible parking) happens once per 256 KiB (~4,096 times/GiB, not ~131,000), and real network calls once per multipart chunk. Residual overhead is tens of ms of CPU per GiB versus seconds-to-minutes of network time for a >64 MiB streamed part — nowhere near capping throughput below disk/NIC speed. The suggested BufWriter fix would work and is a harmless micro-cleanup, but recovers negligible real-world time.


#### 53. Local destination copies straight onto the final key instead of temp+rename, then a log special case excuses the partial-file risk

`src/destination/local.rs:27` · частота: **per-chunk** · категория: correctness-risk · ревьюер: io


```rust
std::fs::copy(local_path, &target)?;
```


**Почему важно.** A crash mid-`fs::copy` leaves a truncated part at the *final* destination key — a downstream reader polling the prefix can consume a corrupt parquet file before any retry rewrites it. The codebase knows: capabilities report `retry_safe: false, partial_write_risk: true`, and `log_capabilities` in mod.rs grew a whole `DestinationType::Local` branch (lines 223-231) purely to demote the resulting WARN to DEBUG. The module already has the atomic primitive — `r#move` uses `fs::rename` with an EXDEV fallback — it just isn't used for writes.


**Фикс.** Copy to a dot-prefixed sibling temp path in the target directory, then `fs::rename` onto the final key (atomic on POSIX same-fs). That makes local genuinely Atomic and retry_safe, and deletes the dest_kind special case in log_capabilities.


**Поправка верификатора.** Every structural claim is accurate (line 27 quote verbatim; capability flags at local.rs:36-37; the Local WARN-demotion branch at mod.rs:223-231; the fs::rename+EXDEV primitive in r#move; per-chunk caller via commit.rs write_part_file). But the finder overlooked that the system's consumption contract is manifest-based, not prefix-polling: manifest_writer.rs writes manifest.json then _SUCCESS only after all parts commit, and validate_manifest.rs checks part sizes/fingerprints (SizeMismatch, UntrackedObject failure classes) with quarantine machinery for stale parts. A crashed run never writes _SUCCESS, so protocol-following readers cannot consume the truncated part; in-process copy failures return Err and the idempotent retry rewrites the key. The DEBUG demotion is a documented, reasoned decision (mod.rs:195-202), not a cover-up. The proposed temp+rename fix is sound (same-dir rename, EXDEV impossible) but would leave orphaned dot-temp files on crash that list_prefix reports as UntrackedObject, so 'genuinely retry_safe' slightly overstates it without orphan cleanup.


#### 54. Path-based Destination::write bakes in encode→disk→read-twice for every part

`src/destination/mod.rs:115` · частота: **per-chunk** · категория: architecture · ревьюер: io


```rust
fn write(&self, local_path: &Path, remote_key: &str) -> Result<WriteOutcome>;
```


**Почему важно.** Because the trait only accepts a file path, every exported byte makes three trips: written to the temp file by the format writer, read back in full for the upload (cloud.rs `std::fs::read` / `io::copy`), then read in full *again* by `compute_part_checksums` in commit.rs ("Both body hashes in one read" — but it's a third pass over the same bytes). The temp-file staging itself is defensible (retry-safe uploads, resume), but the interface forecloses both single-pass checksumming and any future streaming encode→upload, and every new caller inherits the double read.


**Фикс.** Compute xxh3+MD5 in a hashing `Write` wrapper at encode time (or a tee reader during the upload read) so part bytes are read from disk exactly once; longer term, add a `write_reader(&self, body: impl Read, len: u64, key: &str)` so the one-shot path can stream from the encode buffer.


**Поправка верификатора.** The "three trips" framing overstates the redundancy: the encode write and the upload read are intrinsic to the deliberate temp-file staging (one-shot budget, Azure single Put Blob Content-MD5, OpenDAL retry layer), so only the checksum pass is genuinely extra — and the codebase documents it as a knowing tradeoff (manifest_writer.rs: "the re-read is disk-cache cheap relative to the upload it follows"), which the finder did not mention. The proposed hashing-Write-wrapper fix is workable (format writers emit sequentially and the transit check at commit.rs:113 only needs the local MD5 available at commit time), with the caveat that a retried re-encode must recompute hashes — handled naturally by the wrapper approach.


#### 55. per-batch Vec<Option<i64>> allocation for the constant exported_at column

`src/enrich.rs:48` · частота: **per-batch** · категория: performance · ревьюер: shell


```rust
let ts_array =
            TimestampMicrosecondArray::from(vec![Some(exported_at_us); n]).with_timezone("UTC");
```


**Почему важно.** Runs once per RecordBatch on the hot export path when meta_columns.exported_at is enabled: materializes n `Option<i64>` (16 bytes each) only for Arrow to unwrap every element and build a needless validity bitmap for a column declared non-nullable, doubling the transient allocation versus the values-only constructor.


**Фикс.** Use `TimestampMicrosecondArray::from(vec![exported_at_us; n])` (the non-Option `From<Vec<i64>>` impl) — same array, no per-value Option, no null buffer.


**Поправка верификатора.** The "needless validity bitmap" half of the rationale is wrong for the pinned arrow 58.3.0: From<Vec<Option<i64>>> goes through from_iter with NullBufferBuilder, which is lazy (bitmap_builder: None until the first null; append_non_null only increments len; finish() returns None when no null was ever appended — arrow-buffer-58.3.0/src/builder/null.rs:116-122, 210-213). With all-Some input no bitmap is ever allocated and the array carries nulls=None. The real waste is the 16-bytes-per-element Vec<Option<i64>> transient (Option<i64> has no niche) plus a per-element unwrap-and-copy into the values Buffer; the suggested From<Vec<i64>> impl exists for TimestampMicrosecondType (def_numeric_from_vec!, arrow-array primitive_array.rs:1557) and is zero-copy via Buffer::from_vec, so the fix compiles and is strictly better — arguably more than 'doubling' since it removes the copy too. Cleaner still: TimestampMicrosecondArray::from_value(exported_at_us, n) (primitive_array.rs:808), which is purpose-built for a constant column. Magnitude is tiny: ~16n transient bytes per batch, dwarfed by the batch itself and by the adjacent per-row hash_column formatting.


#### 56. parallel_checkpoint opens up to 3 fresh state-DB connections per chunk; module doc claims a shared connection

`src/pipeline/chunked/parallel_checkpoint.rs:338` · частота: **per-chunk** · категория: performance · ревьюер: chunked


```rust
let fname_for_state: Option<String> = if let Some(rec) = part {
    if let Some(name) = Some(rec.file_name.as_str()) {
        match StateStore::open(&config_path_w) {
```


**Почему важно.** Per completed chunk a worker pays: (1) claim_next_chunk_task_at_ref — fresh open_connection or postgres::Client::connect per call (state/checkpoint.rs:226/234, doc: 'Claim next chunk using a fresh connection'); (2) complete/fail_chunk_task_at_ref — another fresh connection (checkpoint.rs:320/345); (3) the quoted StateStore::open for the file_log write, which also re-runs migrate() every time (state/mod.rs open_sqlite). With RIVET_STATE_URL=postgres that is 3 TCP+auth connects per chunk. The module header (line 8: 'so all workers share a single SQLite connection') is simply false, which will mislead the next contributor. Bonus: `if let Some(name) = Some(...)` is an always-true pattern hiding dead structure.


**Фикс.** Give each worker one long-lived state connection (open once per worker thread, reuse for claim/complete/fail/record_file — the SQL is already connection-agnostic), or at minimum route record_file through the same *_at_ref connection used for complete_chunk_task. Fix the false doc-comment and delete the `Some(...)` tautology.


**Поправка верификатора.** The finding is factually correct on every point, but two qualifiers lower the severity. (1) Frequency: per-chunk, and each chunk already pays a full source query + Parquet write + destination upload, so 3 connection opens (even PG TCP+auth) are a small fraction of per-chunk wall time — this is a tidiness/doc issue more than a measurable perf one. (2) The per-chunk-open design is partly deliberate: rusqlite::Connection is not Sync, and the inline comment at parallel_checkpoint.rs:121-125 correctly documents per-chunk opens — only the module header (line 8) is false/stale. The proposed fix works: Connection is Send, so a per-worker StateStore opened once at thread start could use the existing &self methods (claim_next_chunk_task, complete_chunk_task, fail_chunk_task, record_file). One precision nit: only the StateStore::open path re-runs migrate(); the _at_ref claim/complete/fail paths use bare open_connection/Client::connect without migration, as the finding correctly attributes.


#### 57. Quarantine filter is a substring match over the whole key — present parts can be reported Missing and re-exported on resume

`src/pipeline/manifest_reconcile.rs:113` · частота: **per-run** · категория: correctness-risk · ревьюер: integrity


```rust
.filter(|m| !m.key.contains(QUARANTINE_PREFIX))
```


**Почему важно.** Any listed key containing the substring `_quarantine` anywhere — a Hive partition segment value, a table-derived prefix like `exports/foo_quarantine_review/`, a user file name — is dropped from the listing index. Both consumers of this walk then misbehave: verify_at_destination reports the committed part as PartMissing (fatal, flips passed=false), and build_resume_plan (resume_decisions.rs:135) classifies it Missing → Rewrite, re-exporting a part that exists and leaving duplicate data. The resume module's own doc contract says 'entries whose key starts with QUARANTINE_PREFIX are silently filtered' — the implementation does not match the documented anchor.


**Фикс.** Anchor the filter to the manifest-rooted quarantine directory: drop keys where the path relative to manifest_dir starts with `_quarantine/` (e.g. `m.key.strip_prefix(&join_key(manifest_dir, QUARANTINE_PREFIX)).is_some_and(|r| r.starts_with('/') || r.is_empty())`), and add a regression test with a part path containing the substring.


**Поправка верификатора.** The mechanism is real but two of the finding's three trigger examples are impossible: destination-path prefixes (e.g. exports/foo_quarantine_review/) and {partition} placeholder values are part of the destination base and are stripped from listing keys (cloud.rs:240-247 returns configured-prefix-relative keys; local.rs:62-64 strip_prefix(base)), so they never reach the filter. The only production vector for a false PartMissing is the user-controlled export name embedded in part filenames (single.rs:345 `{export_name}_{ts}_part{n}`, keyset.rs:108, chunked/mod.rs:63-71 `{export_name}_{ts}_chunk{n}_{nonce}`); config validation (config/mod.rs:163-183) imposes no character restrictions, so an export named e.g. `events_quarantine` legally triggers it. The fix direction is correct and works from the current state, with one nuance: quarantine keys are written at the destination root (resume_m8.rs:344 `_quarantine/{run_id}/{src_key}`) and both production callers pass manifest_dir="" (finalize.rs:267, validate_cmd.rs:155), so root-anchored starts_with("_quarantine/") suffices. Severity lowered from medium to low because the trigger requires a `_quarantine` substring in the export name — narrow, though the consequence (silent duplicate data on resume, false verify failure) is real.


#### 58. O(parts²) untracked-surplus scan in reconcile walk

`src/pipeline/manifest_reconcile.rs:172` · частота: **per-run** · категория: performance · ревьюер: integrity


```rust
if claimed.iter().any(|c| c == key) {
```


**Почему важно.** For each listed object, a linear scan over the Vec of all claimed part keys — O(N²) string comparisons in the part count. Runs once per validate/resume/finalize pass, so it is not a data-path bottleneck, but chunked exports are exactly the case that produces many-thousand-part manifests (10k parts ≈ 100M comparisons), and this sits on the resume startup path. The fix is one line.


**Фикс.** Collect `claimed` into a `HashSet<String>` (or reuse the BTreeMap keys) and test membership with `claimed.contains(key)`.


**Поправка верификатора.** Diagnosis and severity are accurate. Two small refinements: (1) the 10k-parts ≈ 100M figure is the all-untracked worst case; on a clean resume the `any` short-circuits at the match, averaging ~50M comparisons — same order, still sub-seconds of per-run CPU. (2) The fix's alternative phrasing "reuse the BTreeMap keys" is imprecise — the claimed part keys are not the `listed` BTreeMap's keys; the map-based variant would be deleting claimed entries from a mutable copy of `listed`. The primary fix (HashSet<String> + contains) compiles and works as stated.


#### 59. PlanArtifact::print_summary is a 145-line mixed-concern printer inside the serialization type, with two byte-identical thousands-separator formatters beside it

`src/plan/artifact.rs:296` · частота: **per-run** · категория: complexity · ревьюер: plan-config


```rust
let res = self.resolved_plan.tuning.resource_summary();
        println!("  Resources:");
```


**Почему важно.** The artifact struct's job is the plan/apply wire contract (PA1-PA9), but print_summary (lines 226-370) hand-renders tuning resource math, parquet row-group default resolution (re-deriving DEFAULT_TARGET_ROW_GROUP_MB display logic), destination labels, and prioritization reasons via ~40 println! calls — every new plan field grows this untestable block, and it already duplicates knowledge owned by tuning/format modules. Directly below, format_number (lines 384-395) and format_rows (398-409) are the same 12-line algorithm copied for usize vs i64.


**Фикс.** Move the rendering into a presenter (e.g. plan_cmd or a display module) that consumes &PlanArtifact and writes to an impl fmt::Write so it is testable; collapse the two formatters into one generic fn over impl Into<i128> or itoa-style helper.


**Поправка верификатора.** Three overstatements, none fatal. (1) "Hand-renders tuning resource math" is wrong: the math lives in SourceTuning::resource_summary() (src/tuning/profile.rs:288, which has its own tests); print_summary only formats the returned ResourceSummary fields. (2) The formatters are not literally byte-identical — signatures differ (usize vs i64) — but the 12-line bodies are character-for-character copies, and only format_rows is tested (artifact.rs:601-604); format_number is untested. (3) "~40 println!" is actually 34. The fix detail is also slightly wrong: `impl Into<i128>` will not compile because std has no `From<usize> for i128`; since both bodies only need `n.to_string()`, a single `fn format_thousands(n: impl ToString)` (or generic over itoa::Integer) is the working collapse. The row-group claim IS real duplicated knowledge: artifact.rs:347-357 repeats the strategy.unwrap_or_default() + target_row_group_mb.unwrap_or(DEFAULT_TARGET_ROW_GROUP_MB) resolution owned by ParquetConfig::effective_row_group_rows (src/config/format.rs:101-108), so the displayed value can drift from writer behavior.


#### 60. ResolvedRunPlan's execution/persistence contract is the raw config structs (SourceConfig, DestinationConfig, ParquetConfig) — re-exports alias the coupling instead of removing it

`src/plan/contract.rs:67` · частота: **per-run** · категория: architecture · ревьюер: plan-config


```rust
/// Source connection parameters — resolved from config at plan time so pipeline
    /// functions receive the complete execution contract in a single struct.
    pub source: SourceConfig,
```


**Почему важно.** contract.rs states 'Pipeline modules must not read raw config structures' yet ResolvedRunPlan embeds SourceConfig (including the raw `tuning: Option<TuningConfig>` next to the already-resolved `tuning: SourceTuning` — two tuning representations one field apart), DestinationConfig, QualityConfig and ParquetConfig wholesale; plan/mod.rs:32-38 then re-exports the config types 'so pipeline modules import from crate::plan, not crate::config', which renames the dependency rather than severing it. Because PlanArtifact serializes ResolvedRunPlan verbatim, any YAML-facing field rename or `deny_unknown_fields` tweak on these serde structs changes the persisted artifact format and pipeline contract in the same commit — three concerns (YAML schema, execution contract, artifact format) pinned to one struct definition.


**Фикс.** Either declare the config structs the official plan wire format (document it, add artifact round-trip fixtures from older versions) or introduce thin resolved types for the plan (drop the raw tuning from the embedded source, freeze serde names). At minimum strip `source.tuning` when building the plan so only one tuning representation exists post-resolve.


**Поправка верификатора.** Three corrections. (1) The fix's "add artifact round-trip fixtures from older versions" already exists: tests/artifact_legacy_compat.rs freezes v0.7.5 plan artifacts and its header explicitly declares the JSON the wire format — though it parses via serde_json::Value (field-presence only), so a deny_unknown_fields break from a current-code rename would still slip through; the actionable gap is a type-level deserialization test, not fixtures. (2) Major mitigation missed: artifacts expire 24h after creation (artifact.rs:143, StaleError requires --force), so cross-version artifact reads are an edge case by design, sharply limiting the blast radius of format drift. (3) "two tuning representations one field apart" is imprecise — the raw TuningConfig is nested at plan.source.tuning (via build.rs:125 cloning the whole SourceConfig), not a sibling of plan.tuning; the substance (dead raw tuning serialized into every artifact, unused post-build per grep) holds. ADR-0005 PA1 plus SourceConfig::redact_for_artifact show the config-embedding is deliberate documented design, not accidental drift.


#### 61. stringly-typed diagnostics contract: prioritization greps free-text warnings and a Display-formatted verdict, persisted in the artifact JSON

`src/plan/inputs.rs:32` · частота: **per-run** · категория: correctness-risk · ревьюер: plan-config


```rust
let sparse_range_risk = diagnostics.warnings.iter().any(|w| {
        let lower = w.to_lowercase();
        lower.contains("sparse") && lower.contains("range")
    });
```


**Почему важно.** PlanDiagnostics carries `verdict: String` (formatted from preflight::HealthVerdict's Display, "EFFICIENT"/"UNSAFE"...) and `warnings: Vec<String>`; recommend.rs then does `verdict.contains("unsafe")` / `contains("degraded")` (recommend.rs:120-126, 208-211) and inputs.rs greps warning prose for "sparse"+"range". Rewording a preflight warning or verdict label — a change nobody would think is behavioral — silently disables sparse-range risk classification and all verdict-based score adjustments, with no compiler or test signal (plan_cmd.rs already injects a synthetic verdict "unknown (preflight failed)" that matches none of the branches). Because PlanDiagnostics is serialized into PlanArtifact, the strings are also a cross-version compat surface.


**Фикс.** Carry the typed HealthVerdict (serde enum) in PlanDiagnostics and add a structured warning kind (enum + message) so sparse-range is a flag set by preflight, not inferred by substring search; keep the strings only for display.


**Поправка верификатора.** Every structural claim checks out, but the finder missed the blast-radius cap: the prioritization output is explicitly advisory-only — src/plan/artifact.rs:54 ("Advisory source-aware prioritization (ADR-0006). Does not affect execution.") and src/plan/prioritization.rs:3 ("These types are **advisory only**"); grep confirms apply_cmd.rs/run.rs never read recommended_wave/priority_score. A silent regression degrades human-facing ranking advice, never export execution or data. Also, the cross-version compat surface is bounded: PlanArtifact has a 24h TTL (expires_at), so persisted verdict strings rarely cross versions. And the "unknown (preflight failed)" verdict falling through to neutral (no score adjustment, RiskClass::Low) is arguably intentional behavior for an unknown state, not only an unmatched branch. Severity lowered medium -> low: real maintenance fragility, advisory-only impact.


#### 62. preflight per-engine diagnose duplication has already drifted: range probes interpolate identifiers raw, bypassing the shared quoted aggregate_sql

`src/preflight/postgres.rs:219` · частота: **per-run** · категория: duplication · ревьюер: shell


```rust
let range_query = match crate::pipeline::chunked::strip_select_star_from(base_query) {
        Some(tbl) => format!("SELECT min({cursor_col})::text, max({cursor_col})::text FROM {tbl}"),
```


**Почему важно.** sql.rs exists as the single-source-of-truth leaf ('Column and table names that come from user configuration must never be interpolated raw') and `aggregate_sql` quotes the column for the dialect — every pipeline/plan caller uses it. But preflight's get_cursor_range_pg and the twin block in src/preflight/mysql.rs:115-116 (`SELECT CAST(min({col}) AS CHAR)...`) hand-roll the same aggregate with the raw column name. A mixed-case or reserved-word chunk/cursor column makes `rivet check` mis-probe (or error) while the actual run works, producing false verdicts; and diagnose_pg/diagnose_mysql are ~150-line parallel copies ('// Same logic as the PG side' at mysql.rs:149) that adding the MSSQL preflight will turn into a third copy.


**Фикс.** Route both range probes through `crate::sql::aggregate_sql` (it already handles the strip_select_star_from fast path and quoting), and extract the shared diagnose skeleton (mode_str, effective_query, uses_index override, ExportDiagnostic assembly) so per-engine code is only the EXPLAIN/catalog probes.


**Поправка верификатора.** Three overstatements. (1) Scope: the cursor column in incremental mode is NOT exposed — both engines route incremental range probes through incremental_key_expr (src/preflight/cursor_expr.rs:16-21), which quotes via quote_ident; only the chunked-mode branch (range_col) interpolates raw, so realistically only chunk_column is affected. (2) "produces false verdicts" is wrong: probe errors are caught and degraded to (None, None) (postgres.rs:232-235 logs at debug; mysql.rs `_ => (None, None)`), and compute_verdict (analysis.rs:272) consumes only row_estimate/uses_index/has_cursor — the range feeds only cursor_min/max display and the check_sparse_range warning. Failure mode is a silently missing range and lost sparsity warning, not a wrong verdict, and rivet check does not error. (3) The fix as written would not work as a drop-in: aggregate_sql emits no text cast (preflight needs ::text / CAST AS CHAR to read values as strings), builds one aggregate per call (preflight does min+max in a single round trip — doubling probes matters; the code explicitly documents a 3.2 GB temp_files probe-cost incident), and cannot take the quoted COALESCE expression used by the PG incremental branch. The minimal correct fix is quote_ident on the column at the two raw sites, or a new shared two-aggregate text-cast helper in sql.rs.


#### 63. MySQL zero-row export emits a second, empty schema after the typed one — ExportSink rebuilds the writer over the same tmp file

`src/source/mysql/mod.rs:622` · частота: **per-run** · категория: correctness-risk · ревьюер: connectors


```rust
if total_rows == 0 {
    sink.on_schema(Arc::new(Schema::empty()))?;
}
```


**Почему важно.** mysql_run_export has ALREADY called sink.on_schema with the real typed schema (exec_iter always yields column metadata; the comment at 617-620 admits this), so a zero-row run — the steady state of every up-to-date incremental export — fires on_schema twice. ExportSink::on_schema (pipeline/sink/mod.rs:466) unconditionally recreates the format writer over a try_clone of the same tmp file, so the first writer's buffered header flushes on drop and interleaves with the second writer's output, and the surviving schema is the untyped empty one. PG guards on `!had_schema` (schema genuinely unknown); MySQL guards on `total_rows == 0` (schema known and discarded) — a false parity. MSSQL emits the typed schema for empty results, so the three engines disagree on what an empty file looks like.


**Фикс.** Delete the fallback for MySQL (metadata is always available, on_schema always fired) or track had_schema like PG. Separately, make ExportSink::on_schema reject or no-op a second call so no engine can double-initialize the writer.


**Поправка верификатора.** The diagnosis is structurally correct but the blast radius is overstated. The "interleaved writer output" never reaches a destination: all three runners discard the tmp file on zero rows before any upload (src/pipeline/single.rs:291-311 returns before write_part_file at line 353; chunked/sequential_checkpoint.rs:81-83 and chunked/parallel_checkpoint.rs:275-277 return (0, None) before validate/write). So no engine ever publishes an empty or corrupted file, and "the three engines disagree on what an empty file looks like" is moot at the artifact level. The real consequence the finder missed is narrower and different: in chunked mode the run schema fingerprint is captured BEFORE the zero-row check (sequential_checkpoint.rs:77-79; parallel_checkpoint.rs:270-274 via OnceLock first-write-wins), and record_run_schema_fingerprint (manifest_writer.rs:210-214) has no empty-schema guard — so if the first chunk to record has zero rows, the manifest's ADR-0012 M3 schema_fingerprint is computed from the clobbered empty schema, despite MySQL having had the typed schema (PG genuinely lacks it there). That is a manifest-trust/false-drift wrinkle, not data corruption. The suggested fix (delete the fallback or track had_schema; make ExportSink::on_schema reject a second call) is sound and would also fix the fingerprint wrinkle.


#### 64. new DB connection opened per chunk claim/complete/fail in parallel workers

`src/state/checkpoint.rs:233` · частота: **per-chunk** · категория: performance · ревьюер: state-types


```rust
StateRef::Postgres(url) => {
                let mut client = postgres::Client::connect(url, postgres::NoTls)
                    .map_err(|e| anyhow::anyhow!("state(pg): connect for claim: {:#}", e))?;
```


**Почему важно.** claim_next_chunk_task_at_ref, complete_chunk_task_at_ref, and fail_chunk_task_at_ref each open a fresh connection (SQLite: file open + WAL/busy_timeout pragmas; Postgres: full TCP + auth handshake) on every call. The worker loop in src/pipeline/chunked/parallel_checkpoint.rs:159 calls claim once per chunk and complete/fail once per chunk, so each chunk costs 2-3 connection setups per worker. On the RIVET_STATE_URL Postgres backend (the K8s deployment story) with thousands of chunks this is thousands of TCP+auth round-trips and server-side backend forks for work the existing SKIP LOCKED claim SQL already supports on persistent connections. The 'connection is not Sync' constraint only forbids sharing across threads, not reuse within one worker's loop.


**Фикс.** Open one connection (or one StateStore) per worker thread at spawn, before the loop, and pass &mut to claim/complete/fail across all chunks. StateRef stays as the serializable bootstrap; only the per-call connect moves out of the loop.


**Поправка верификатора.** Diagnosis is accurate (and slightly understated: non-empty chunks open a third connection via StateStore::open at parallel_checkpoint.rs:340 for the file_log write, and the module doc at line 8 falsely claims workers 'share a single SQLite connection'). But severity is inflated: the loop is per-chunk, not per-row, and each chunk already opens a fresh SOURCE connection per attempt by design (ADR-0011) plus runs a full query + file write + destination upload, so 2-3 state-DB connects are a small constant fraction of per-chunk cost — not a bottleneck except for pathological all-empty-chunk runs. The proposed fix is type-valid (rusqlite::Connection and postgres::Client are Send, only !Sync), but a persistent per-worker connection needs reconnect-on-error handling that per-call connect currently provides for free, since a chunk export can take minutes during which an idle PG connection may be dropped.


#### 65. governor pressure signal has no deadband — oscillates parallelism and churns real DB connections

`src/tuning/adaptive.rs:110` · частота: **per-run** · категория: correctness-risk · ревьюер: state-types


```rust
let cur_p = sample?;
        let under_pressure = self.prev.is_some_and(|p| cur_p > p);
```


**Почему важно.** The sampled metrics are cumulative monotonic counters (pg_stat_bgwriter.checkpoints_req, Innodb_log_waits per the module docs). 'Strictly greater than the previous sample' means a delta of +1 over a 1.5 s window — statistical background noise on any live database — reads as pressure (shed one worker), and a flat sample reads as calm (recover one). Under a steady low event rate the governor flip-flops ±1 worker every GOVERNOR_SAMPLE_INTERVAL_MS, and this file's own doc comment (line 53) says each step 'opens or retires a real source connection'. The same delta>0 boolean drives next_adaptive_batch_size, so the fetch size oscillates between base and 0.75·base in the same conditions. There is no rate threshold, no hysteresis, no requirement of consecutive samples.


**Фикс.** Make the signal rate-aware: treat pressure as (cur_p - prev) exceeding a per-engine threshold rather than > 0, and add hysteresis (e.g. require N consecutive calm samples before recovering a worker). GovernorState::observe is pure and fully unit-tested, so the change is cheap to pin.


**Поправка верификатора.** Two overstatements. (1) "Churns real DB connections": the governor callback only calls semaphore.resize (exec.rs:325); resize lowers lazily honoring in-flight permits (resource.rs:185-195), and chunk workers open a fresh connection per chunk regardless (exec.rs:385) — oscillation shifts when chunks start, it does not open/retire connections at decision time. The adaptive.rs doc comment describes steady-state concurrent-connection count, not per-step churn. (2) PG noise claim is wrong: checkpoints_req increments only on requested checkpoints, which are rare heavyweight events — +1 per 1.5s IS genuine write pressure, not background noise. The noise scenario is realistic mainly for MySQL, whose actual counters are Created_tmp_disk_tables + Innodb_buffer_pool_wait_free (mysql/mod.rs:683-685), not Innodb_log_waits (stale module doc at adaptive.rs:5). Also mitigations the finder omitted: governor is opt-in (tuning.adaptive) and armed only when parallel > 1 (exec.rs:252), amplitude is bounded to ±1 per 1.5s within [floor, ceiling], and the flat-vs-rising design is explicitly documented as mirroring the batch loop. The suggested per-engine raw-delta threshold is fragile across counters with different semantics; the consecutive-calm-samples hysteresis half of the fix is the sound part.


#### 66. pow10_i256 builds and parses a decimal string per value on the Decimal256 hot path

`src/types/decimal.rs:184` · частота: **per-value** · категория: performance · ревьюер: state-types


```rust
fn pow10_i256(n: u32) -> Option<i256> {
    i256::from_string(&format!("1{}", "0".repeat(n as usize)))
}
```


**Почему важно.** decimal_str_to_scaled_i256 calls pow10_i256 once per parsed value (line 165, plus line 136 on the negative-scale path) and scale_int_to_i256 calls it once per value (line 179). Both are invoked per row inside the column builder loops in src/source/mysql/arrow_convert.rs (lines 746/758/769) and src/source/postgres/arrow_convert.rs (line 772). Each call performs two heap allocations ("0".repeat + format!) and a full base-10 string parse to recompute a constant 10^scale that never changes within a column. The shorter-frac padding branch (lines 154-160, and the i128 twin at lines 99-104) allocates a third String per value when the DB sends trimmed fractions. For a high-precision decimal column over millions of rows this is millions of avoidable allocations in the decode stage.


**Фикс.** Replace pow10_i256 with a precomputed static table of the 77 valid powers (build once via checked_mul in a LazyLock, or const), and replace the pad-with-zeros String in both parsers with arithmetic: parse the fractional digits as an integer, then multiply by 10^(scale - frac_len).


**Поправка верификатора.** Every specific claim checks out (all line numbers verified: decimal.rs 136/165/179/184, mysql arrow_convert.rs 746/758/769, postgres arrow_convert.rs 772; no caching or hoisting exists). However the severity is overstated: the Decimal256 path only fires for precision 39-76 columns, which are uncommon — the common Decimal128 path uses alloc-free 10i128.pow. And on the string-parse branches the loop already allocates per value upstream (Postgres builds a String per value; the value itself is parsed via i256::from_string), so pow10_i256 is a ~2x constant-factor overhead there rather than the dominant cost. Only the integer-override path (scale_int_to_i256 for Value::Int/UInt in a Decimal256 column — an edge case) has pow10 as its dominant per-value cost. The proposed fix (precomputed power table + arithmetic instead of string padding) is valid.



---

## Опровергнутые находки (отброшены верификацией)

- **MSSQL cell access uses cells().nth(idx) — O(column_index) per cell, O(cols^2 x rows) per batch** (`src/source/mssql/arrow_convert.rs:168`) — Opened src/source/mssql/arrow_convert.rs: the quote is verbatim at lines 167-169, and the column-major structure (build_array calls cell per row, mssql_rows_to_record_batch per field, on the per-batch export path via emit_mssql_batch) is as described. However, the core complexity claim fails: I read tiberius 0.12.3's Row::cells() (zip of two slice iterators) and benchmarked Zip::nth in release mode — timing is constant across index 0..199 due to std's TrustedRandomAccess specialization, so there is no O(rows x cols^2) behavior in optimized builds. The try_get-with-usize claim is also wrong; it is O(1) Vec indexing in tiberius.

- **Invalid compression_level silently swallowed by unwrap_or_default** (`src/format/parquet.rs:36`) — Verified the quoted code at src/format/parquet.rs:36 — the unwrap_or_default exists as quoted. Then traced compression_level upstream: it is range-validated at config load in src/config/mod.rs:402-429 with fail-loud errors, and both out-of-range scenarios (zstd 30, gzip 15) have dedicated tests asserting config load fails. The only theoretical bypass is a hand-edited saved plan artifact, which is not the user-config path the finding describes.

- **row-size estimator uses a flat 256 B guess for all string/binary columns, ignoring observed shape stats** (`src/tuning/memory.rs:14`) — Verified the quote: line 14 of src/tuning/memory.rs is exactly `const STRING_ESTIMATE: usize = 256;` and compute_batch_size_from_memory clamps to 1k-500k. Then traced every consumer: effective_batch_size is called only from the Postgres and MySQL export loops, both of which (plus MSSQL) wrap an AdaptiveBatchController that starts at a 500-row probe and applies a memory cap derived from the actually measured batch_memory_bytes after the first batch — exactly the feedback loop the finding claims is missing and proposes as the fix. The module's own doc comment states the estimate is a "fall-back when a fetch loop hasn't observed real row sizes yet", confirming the design is intentional and the 40x-underestimate-to-5GB scenario is unreachable in the real call graph.


---

*Сгенерировано мульти-агентным workflow (78 агентов, ~4.2M токенов суммарно за два
прогона): фаза Roast → дедуп → фаза Verify (адверсариальная, с чтением кода).
3 находки опровергнуты скептиками — что лишний раз оправдывает верификационную фазу.*
