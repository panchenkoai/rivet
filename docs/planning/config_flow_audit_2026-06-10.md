# Rivet — аудит конфигов и CLI-флоу

**Дата:** 2026-06-10 · ветка `feat/ux-clarity` · метод: 13 агентов прогнали **реальный бинарь** (`target/debug/rivet`) против живого docker-стека (pg/mysql/mssql/minio/fake-gcs), 251 живой тест, затем синтез.

> ## СТАТУС ИСПРАВЛЕНИЙ (2026-06-11) — все HIGH+MEDIUM закрыты по RED→GREEN
>
> Каждая находка прошла тот же конвейер, что и архитектурная прожарка: **26 `audit_`-тестов
> написаны ДО фиксов** (offline-юниты + live против docker), упали на предсказанных
> ассертах (доказательная матрица), затем фиксы — каждый позеленил свой тест. Финал:
> **1542 offline + 24 live audit_ + полный integration-пасс зелёные, 0 провалов.**
>
> **Wave A (HIGH):**
> - **doctor-категоризатор** (#0,1,2,13,27): `categorize_*` теперь читают вложенную причину
>   через `{:#}` и ловят PG `db error`, MSSQL `login failed`, S3 `permissiondenied/403/InvalidAccessKeyId`,
>   reqwest `error sending request` → auth/connectivity + actionable hint. + probe-cleanup (#26) для local.
> - **table-form preflight** (#3,15): preflight строит `SELECT * FROM <table>` через `resolve_query`,
>   а не `SELECT 1` — row estimate, scan type, cursor range теперь реальные.
> - **csv+compression** (#10): `format: csv` + compression теперь loud-reject на config-валидации
>   (никакого тихого no-op + лживого манифеста).
> - **plan --output multi-export** (#4): больше не молчаливая перезапись — все экспорты сохраняются.
> - **metrics/journal/state config-validation** (#9,#23): `--config` валидируется (существует+парсится)
>   до открытия state DB → нет «exit 0 с чужими данными».
> - **state guardrails** (#21,#22): `reset-chunks` валидирует имя экспорта; `reset` чистит progression.
> - **journal file-names** (#24): journal печатает имена файлов.
> - **column-applicability** (#11,#31,#32,#33): quality/override на несуществующей колонке → loud fail
>   в `on_schema`; lossy scale-narrowing override → помечается lossy, не «exact».
>
> **Wave B (MEDIUM):**
> - **target-typo** (#14): `check --target <typo>` → loud non-zero, как config-level.
> - **repair trust-loop** (#7,#8): repair обновляет `chunk_task.rows_written` (reconcile сходится) +
>   дописывает repair-часть в манифест (validate чист). `reconcile && repair && reconcile` теперь converge.
> - **re-run guard** (#5,#19,#30): ре-ран в префикс с готовым `_SUCCESS` без `--resume` → loud guard.
> - **validate `--require-manifest`** (#20): новый флаг делает отсутствующий манифест FAILURE (CI-гейт).
> - **max_file_size inert** (#6,#29): parquet без `row_group_rows` + max_file_size → однократный WARN
>   (cap неэнфорсим), а не тихий no-op.
> - **apply tamper-evidence** (#16,#17): fingerprint resolved_plan на plan-time, проверка на apply →
>   изменённый артефакт отклоняется; inline-url ошибка теперь actionable.
> - **observability JSON** (#25): добавлены serde-рендереры metrics/journal (флаг `--json` — следующий шаг).
> - **partition_by docs** (#28): описания исправлены — требует DATE/TIMESTAMP, не произвольное value-партиционирование.
>
> **Осознанно НЕ закрыто** (продуктовые решения / вне scope): полная реализация value-партиционирования
> (#28 — только docs), CSV-compression как фича (#10 — выбран reject), `--json` флаг metrics/journal
> (#25 — рендереры готовы, остаётся проводка в cli.rs).
>
> **LOW (2026-06-11): 18 из 30 закрыто** тем же fix+assert конвейером (doctor single-surface/perm-label/trim/stdout-line,
> check target-msg+note+plan-validation+export-list, state column+hints+unknown-export, plan json-array+output,
> validate untracked→warning, repair chunk-index, config warns+docs). **Дубли уже закрытых HIGH:** L3/L5/L11.
> **Отложено:** L0 (init MSSQL-scaffold) и L1 (init include/exclude фильтр) — фичи; L20 (doctor fail-fast на
> недоступном cloud) — нужен no-retry seam в `cloud.rs`; L26 (hint на не-закавыченный `{partition}`) — ошибка
> serde_yaml upstream; L27 (cursor/window-строка в summary) — частично.

---

# Аудит rivet: конфиг-грамматика и CLI-поверхность

## 1. Вердикт

rivet — это зрелый, заботливо сделанный инструмент: счастливые пути работают, многодвижковый паритет (postgres/mysql/mssql дают идентичный fingerprint и row count) реальный, а UX-полировка ошибок местами лучшая в классе («Did you mean `destination`?», staleness-guard в plan, resume-on-completed gate). Из 251 живого теста прошло 217 — то есть ядро экспорта (типы, чанки, курсоры, форматы parquet) можно доверять. Но грамматика конфига несёт несколько **тихих неправильных поведений**, которые опаснее падений: CSV+compression — полный no-op с лживым манифестом, `max_file_size` инертен для типового parquet, `repair --execute` рапортует успех, не закрывая trust-loop, а multi-export `plan --output` молча теряет все экспорты кроме последнего. Второй системный класс проблем — **команды не валидируют переданный `--config`** (state/metrics/journal используют его только чтобы найти соседний `.rivet_state.db`), из-за чего опечатка в пути выглядит как «нет данных, exit 0». И третий сквозной баг: preflight для `table:`-формы EXPLAIN'ит `SELECT 1`, выдавая «Row estimate: ~1» и ложный DEGRADED именно на том, что генерирует `rivet init`. Кости крепкие; рыхлые места — это валидация на границах и честность зелёного статуса.

## 2. Scorecard

| Area | Score /5 | Status | Однострочный вывод |
|---|---|---|---|
| doctor (auth/connectivity preflight) | 3 | works-with-caveats | Кости и exit-коды верны, но категоризатор ошибок MySQL-образный — самая частая ошибка (неверный пароль/ключ) без подсказки на pg, mssql, s3, azure |
| plan + apply (lifecycle артефакта) | 3 | works-with-caveats | Round-trip url_env идеален, но multi-export `--output` молча теряет экспорты, а артефакт не tamper-evident |
| validate + reconcile + repair (integrity trio) | 3 | works-with-caveats | Первые две ноги 5-уровня, но `repair --execute` рапортует успех и навсегда оставляет reconcile/validate красными |
| formats + compression + max_file_size + partition_by | 3 | works-with-caveats | Parquet-кодеки и CSV RFC-4180 отличны, но CSV+compression — тихий no-op с лживым манифестом |
| check + type-report + strict + target | 4 | works-with-caveats | Type-fidelity машинерия превосходна; `table:`-форма даёт мусорные диагностики, опечатка в `--target` молча проглатывается |
| run — full/snapshot core (pg/mysql/mssql) | 4 | works-with-caveats | Реальный многодвижковый паритет; повторный full-run накапливает осиротевшие part-файлы |
| run — chunked family | 4 | works-with-caveats | Критический row-drop регресс НЕ воспроизводится; `max_file_size` no-op для parquet, повторный run удваивает данные |
| state subcommands | 4 | works-with-caveats | Хорошо читаемо; нет паритета guardrail/config-load по семье команд |
| observability (metrics/journal/schema config/completions) | 4 | works-with-caveats | schema config editor-ready; metrics/journal не валидируют `--config`, нет `--json` |
| destinations (local/s3/gcs/stdout/azure) | 4 | works-with-caveats | Паритет байт-в-байт + stdout-guards 5-уровня; connectivity-ошибки без подсказки, probe-файл не чистится |
| incremental + time-window (cursor lifecycle) | 4 | works-with-caveats | Курсорная математика корректна; манифест/диск дрейфят на повторных run |
| Quality gates + tuning + meta_columns + overrides | 4 | works-with-caveats | NULL-fix реален; int4→string и опечатки колонок проскакивают мимо check и падают/no-op'ят позже |
| init + discover | 5 | works | Безопасный, runnable-конфиг из живой БД с сильной UX-полировкой; лучшая поверхность |

## 3. Что отлично

- **init + discover (5/5)**: единственная пятёрка. Scaffold реально runnable end-to-end (init → doctor → check → run --validate → 2500 строк в parquet подтверждены duckdb). Авто-решения (выбор режима по политике, scaling chunk_size, peak-RSS оценка) объяснены инлайн; `--discover` JSON богат и согласован с YAML; гигиена секретов образцовая (`url_env`, `--source-env/--source-file`, redaction паролей в ошибках).
- **Многодвижковый паритет (run core, destinations)**: pg/mysql/mssql дают **идентичный schema fingerprint** `xxh3:875d4cb9b2dcd229` и 2500 строк; тот же orders→parquet байт-идентичен (4637 B) на local/S3/GCS.
- **Resume/checkpoint (chunked family)**: реальный `kill -9` mid-run + `--resume` восстановился exactly-once (120k distinct, ноль дублей); resume-on-completed gate — образец actionable-ошибки.
- **Wave-1 фиксы подтверждены живьём**: doctor dedup-by-path, exit-код на failure; numeric-cursor advance 999→1000; NULL-unique quality fix (нулы проходят, реальные дубли всё ещё валят); stdout+multipart guards (`[stdout-no-chunked]` и др.). Критический регресс **max_file_size+chunked row-drop НЕ воспроизводится**.
- **Type-fidelity (check)**: per-column отчёт точен по жёсткому набору (numeric→Decimal128, jsonb→Utf8, uuid→FixedSizeBinary(16), timestamptz→Timestamp(us,UTC)); `--target` вскрывает реальные warehouse-несовместимости и эмитит recovery SQL из *деградированного* состояния (CTAS), withholding cast там где это lossy — ровно дисциплина из CLAUDE.md.
- **schema config (observability)**: валидный Draft-2020-12 JSON Schema, байт-идентичный коммитнутому артефакту, ноль stderr, все 27 `$ref` резолвятся — editor-ready.
- **Качество ошибок и validation конфига**: неизвестное поле с «Did you mean»+line:col; staleness-guard в plan; mutex batch_size/batch_size_memory_mb; tuning-профили fast/balanced/safe; CSV RFC-4180 (запятые/кавычки/переводы строк round-trip).

## 4. Находки по серьёзности

### HIGH

1. **`table:`-форма даёт мусорные strategy-диагностики (Row estimate ~1, ложный DEGRADED)** — `kind: bug`, ×3 area (check, plan, run-core отметили). Preflight EXPLAIN'ит `SELECT 1` вместо таблицы (`src/preflight/postgres.rs:71` `base_query = export.query.unwrap_or("SELECT 1")`), хотя `export.table` доступен. Для канонической формы, которую генерирует `rivet init`, row_estimate/scan_type/verdict считаются по `EXPLAIN SELECT 1` → orders (2500 строк, PK btree) показан как «~1 / DEGRADED / add an index». Type-report не затронут. **Фикс**: строить `SELECT * FROM <table>` из `export.table`, когда `query` отсутствует.

2. **CSV + compression — тихий no-op по всем кодекам; манифест лжёт** — `kind: bug` (formats). `format: csv` + `compression: gzip|zstd|...` даёт байт-идентичный несжатый файл (md5 совпадает с none), оставляет `.csv`, но манифест пишет `"compression":"gzip"`. `create_format()` отбрасывает compression для CSV; flate2 — только parquet-feature. Ни config-validation, ни check, ни run не предупреждают. **Фикс**: либо реализовать CSV-compression, либо отклонять/предупреждать на config-validation и **никогда не писать манифест с не-применённым кодеком**.

3. **`repair --execute` рапортует успех, но не закрывает trust-loop** — `kind: footgun` (integrity trio). На чистом chunk-mismatch repair печатает `executed 1 · failed 0`, exit 0 — но reconcile всё ещё `4 match, 1 mismatch` (rows_written не тронут), а validate теперь видит `untracked object` для repair-файла. Манифест не перезаписывается, нет команды finalize/manifest-rebuild. Канонический `reconcile && repair --execute && reconcile` **никогда не сходится**. **Фикс**: repair должен дописывать манифест и пере-записывать chunk counts (или дать follow-up команду), а exit-код/summary не должны читаться как success, пока gate красный.

4. **repair-файл флагается `untracked object`, манифест никогда не обновляется** — `kind: bug` (integrity trio). Прямое следствие №3: re-export садится как `..._chunk0_<hash>.parquet`, manifest всё ещё перечисляет только оригиналы → passing validate невозможен пока repair-артефакты на префиксе. **Фикс**: обновлять манифест после repair.

5. **Multi-export `plan --output` молча перезаписывает — выживает только ПОСЛЕДНИЙ экспорт** — `kind: bug` (plan/apply). Каждый экспорт пишется в один и тот же путь по очереди; файл содержит один plan-объект. `rivet apply` потом тихо гонит только один экспорт. **Фикс**: при >1 экспорта писать JSON-массив (или NDJSON), либо отклонять `--output` с multi-export.

6. **Postgres неверный пароль — opaque (`db error`) И без подсказки** — `kind: bug` (doctor). `categorize_source_error` ищет `password`/`authentication`, но postgres-крейт рендерит auth-fail как просто `db error` (реальная причина в `.source()`-цепочке, не вытащена, т.к. используется `{}` не `{:#}`). Самая частая ошибка doctor на pg — нечитаема и без hint. **Фикс**: использовать `{:#}`/обходить `.source()`-цепочку, добавить per-engine needles.

7. **MSSQL неверный пароль mis-categorized как generic `error`, без подсказки** — `kind: bug` (doctor). `login failed` не матчит auth-needles; MSSQL-специфичный hint на `doctor.rs:253` недостижим. **Фикс**: добавить needle `login failed`.

8. **S3 неверные креды mis-categorized, без auth-hint** — `kind: bug` (doctor). `'PermissionDenied'` лоуэркейсится в `permissiondenied` (без пробела) и не матчит `"permission denied"`; `403`/`forbidden`/`unauthorized` не проверяются. Корректный s3:PutObject/IAM hint недостижим. **Фикс**: матчить `permissiondenied`/`invalidaccesskeyid`/`403`/`forbidden`.

9. **metrics/journal/state-inspect не валидируют `--config`; неверный путь → тихо чужая/пустая история, exit 0** — `kind: footgun`, ×3 area (observability, state, упоминается в общем контексте). `--config` используется только чтобы найти `.rivet_state.db` в родительской директории; файл не парсится. Опечатка пути → exit 0 с чужими `ci`-строками (в общей директории) или «No metrics recorded yet», плюс read-only inspect **создаёт** stray `.rivet_state.db` (114 KB). Контраст: пропущенный флаг даёт exit 2, а `state reset` корректно валидирует. **Фикс**: грузить/валидировать конфиг первым во всех read-only командах семьи (как уже делает `reset`).

10. **`max_file_size` тихо no-op для parquet ниже одной row-group** — `kind: bug` (chunked family; пересекается с formats). `maybe_split` сравнивает `ArrowWriter::bytes_written()` (только flushed-байты) с лимитом; до закрытия row-group → ~0. Без `parquet:`-профиля row group ~128MB/~1M строк → cap для типового экспорта не срабатывает. `max_file_size: "64KB"` дал 112KB-файлы без warning. Строки не теряются. **Фикс**: предупреждать когда cap недостижим без `row_group_rows`, либо учитывать буферизованные байты.

11. **Full re-run накапливает осиротевшие part-файлы; наивный glob over-count** — `kind: footgun`, ×3 area (run-core, chunked, incremental). Part-файлы именуются `chrono::Utc::now()` → уникальны per-run; `idempotent_overwrite` срабатывает только для того же имени, чего не бывает. Манифест+`_SUCCESS` указывают на последний run, но prior parquet остаётся; Spark/DuckDB `read_parquet('dir/*')` читает 2×/5× строк. Нет clean/overwrite-knob. **Фикс**: warn/refuse без `--force` при наличии `_SUCCESS` в non-resume run (как уже делает `--resume`), либо purge старых частей.

12. **int4→string override проскакивает parse/plan/check, падает только mid-run** — `kind: bug` (quality/overrides). Валидный type-string, но неприменимый к source-колонке (int4→string) принят на parse, plan (exit 0), и `check --type-report` (рапортует 'exact'). Падает только `run`, после коннекта. Schema-doc обещает валидацию overrides «at plan time». **Фикс**: проверять source-type применимость override на plan-time, не только синтаксис type-string.

### MEDIUM

- **CLI `--target <typo>` тихо проглатывается (exit 0)** — `kind: footgun` (check). `dispatch.rs:194` `ExportTarget::parse` → None → `tgt = None`, исходная строка отброшена. Config-level `target:` при этом валится громко (с комментарием «never silently ignored» — гарантия держится только для config-пути). `check --target bigqery --strict` в CI **молча НЕ делает warehouse-проверку**. **Фикс**: валидировать `--target` так же громко, как config-level.

- **partition_by — только date/timestamp, документирован как general value-partitioning** — `kind: bug` (formats). `partition_by: status` падает «could not parse partition min 'cancelled' as a date». Date-партиционирование корректно. **Фикс**: исправить docs/schema на «date/timestamp only», либо реализовать value-partitioning.

- **`max_file_size` — batch-granular soft lower-bound, не hard cap** — `kind: footgun` (formats; смежно с chunked). Вызывается после полного батча: таблица в один батч не сплитится; части превышают лимит; `verify: content` doc обещает «part fits single PUT», что soft-limit не гарантирует. **Фикс**: документировать batch-granularity и/или warn когда `max_file_size` < ожидаемого вывода батча.

- **Apply не верифицирует integrity артефакта; tampered resolved_plan гонит без возражений** — `kind: footgun` (plan/apply). `plan_fingerprint` пустой для non-chunked; редактирование `base_query` orders→users экспортировало 500 строк ЧУЖОЙ таблицы под исходным именем, exit 0. Для plan-now/apply-later (CI) артефакт не tamper-evident. **Фикс**: расширить fingerprint на base_query/resolved_plan, верифицировать на apply.

- **Inline `url:` configs дают un-appliable артефакты; warning не actionable** — `kind: ux` (plan/apply). Plan вычищает пароль (хорошо), но apply падает `password missing` без флага для подстановки и без указания env-var. **Фикс**: либо подсказывать «переключись на url_env и пере-планируй», либо дать auth-override флаг на apply.

- **Повторный run завершённого chunked-экспорта в тот же local-путь молча удваивает данные** — `kind: footgun` (chunked; та же семья что №11). `_SUCCESS`-guard только под `--resume`. **Фикс**: warn/refuse без `--force` в non-resume.

- **Scale-reducing decimal override молча усекает, check рапортует 'exact'** — `kind: bug` (quality/overrides). `numeric(10,2)=10.99` + `decimal(20,0)` → parquet `Decimal('10')`, check говорит 'exact'. Нарушает правило CLAUDE.md «lossy degradations must be surfaced». **Фикс**: type-report должен флагать scale-narrowing как lossy.

- **Quality-checks и overrides на несуществующей колонке — тихие no-op** — `kind: footgun` (quality). `unique_columns:[nonexistent]`, `null_ratio_max:{ghost}`, `columns:{not_a_column}` — все exit 0, quality pass, без warning. Опечатка/schema-drift тихо отключает gate. **Фикс**: plan-time валидация что каждая quality/override-колонка есть в resolved schema.

- **`check` не ловит несуществующий cursor_column pre-flight** — `kind: gap` (incremental). `cursor_column: does_not_exist` → DEGRADED, exit 0; падает только на run с raw DB error. **Фикс**: surface'ить отсутствующую cursor/time-колонку как pre-flight failure.

- **Per-run манифест overwrite + нет cleanup → дрейф манифест/диск** — `kind: footgun` (incremental; смежно с №11). После 0-new run манифест `part_count:0`, а реальный parquet прошлого run лежит рядом; на re-run к фиксированному префиксу тихо накапливаются дубли. **Фикс**: то же что №11.

- **journal не показывает file-list, plan-snapshot, event-timeline** — `kind: gap` (observability). `run_journal.journal_json` хранит PlanResolved/FileWritten/RunCompleted, CLI печатает только агрегат. Epic-10 DoD «What was planned?» недостижим через CLI. **Фикс**: рендерить plan+file-list под `--run-id`.

- **Нет `--json` для metrics/journal** — `kind: gap` (observability). Только human-таблица, несмотря на сильный machine-contract posture (schema config, `--json-errors`, run `--json`). **Фикс**: добавить `--json`.

- **doctor оставляет `.rivet_doctor_probe` на destination** — `kind: bug`, ×2 area (doctor, destinations). `check_destination_auth` удаляет только local-temp, не probe-объект в destination → stray 'ok'-файл рядом с данными в export-префиксе после каждого doctor. **Фикс**: удалять probe-объект из destination после проверки.

- **Connectivity/unreachable-endpoint ошибки без hint + raw OpenDAL dump** — `kind: ux`, ×2 area (doctor, destinations). `categorize_dest_error` ключи `connect/refused/timed out/dns/endpoint` не матчат OpenDAL'ское «error sending request for url»; готовый connectivity-hint недостижим. Самый частый misconfig (typo endpoint/host) даёт худшее сообщение. **Фикс**: добавить needles `error sending request`/`send http request`.

- **Azure connectivity/DNS mis-categorized, без hint** — `kind: bug` (doctor; та же причина что выше). Плюс retry-layer ретраит обречённый probe (~10s) — preflight должен fail-fast. **Фикс**: needles + fail-fast retry-policy для preflight.

- **Missing `--param` ошибка говорит про env-var/secret, не называет `--param`** — `kind: ux` (run-core). `${id_max}` без `-p` → «environment variable 'id_max' ... is not set», хотя resolver мержит `--param` и env. Контраст: unused-param WARN говорит на языке `--param`. **Фикс**: добавить «… set it via `--param id_max=<value>` or an env var».

- **reset-chunks `-e <typo>` тихо успешен (exit 0), нет guardrail как у `reset`** — `kind: footgun` (state). `reset` валидирует имя экспорта с «Known exports»-hint, sibling `reset-chunks -e` — нет. **Фикс**: применить guardrail к обоим siblings.

- **reset/reset-chunks не чистят export_progression → расхождение** — `kind: bug` (state). После `reset` `show` пуст, но `progression` показывает COMMITTED 2500 со stale-таймштампом. Данные не теряются. **Фикс**: reset должен чистить/обновлять progression-строку или печатать note.

### LOW

- **check 'Row estimate: ~1'** — `kind: ux`, ×3 area (init, check, run-core отметили как симптом HIGH-бага №1). То же EXPLAIN `SELECT 1`.
- **`--target` один (без `--strict`) не гейтит exit на hard FAIL** — `kind: footgun` (check). Глиф 'fail ✗' визуально implies hard-fail, но exit 0. Defensible, но glyph/exit mismatch — ловушка.
- **Unknown-target error не перечисляет `snowflake`** — `kind: docs` (check). `(expected: bigquery, duckdb)`, хотя `--target snowflake` работает. **Фикс**: добавить snowflake в список.
- **plan `--output` тихо игнорируется без `--format json`** — `kind: footgun` (plan). Help обещает файл, в pretty-формате ничего не пишется. **Фикс**: honor `--output` или warn.
- **Multi-export `plan --format json` на stdout — невалидный конкат JSON** — `kind: ux` (plan). Ни массив, ни NDJSON; jq падает. **Фикс**: массив или NDJSON.
- **chunked→snapshot downgrade виден только как слово 'full'** — `kind: ux` (chunked). Объяснение на info-level (невидимо). **Фикс**: WARN/note в report.
- **validate печатает 'untracked object' под 'failure:' но exit 0** — `kind: ux` (integrity). Лейбл противоречит exit. **Фикс**: отдельный 'warning:'/'note:' лейбл и `warnings`-массив в JSON.
- **validate exit 0 (legacy_run) при неверном `--date`/`--prefix`** — `kind: footgun` (integrity). Опечатанный день/пустой префикс проходят как clean. **Фикс**: флаг `--require-manifest`.
- **repair re-export ре-индексирует chunk как chunk0** — `kind: ux` (integrity). Имя файла не отражает логический chunk. **Фикс**: вшивать оригинальный chunk-индекс/repair-маркер в имя.
- **state files RUN ID колонка (35) переполняется 40-символьным run_id** — `kind: ux` (state). Ломает выравнивание. **Фикс**: расширить колонку.
- **show-empty hint говорит `rivet state` (нет subcommand → ошибка)** — `kind: docs` (state). **Фикс**: `rivet state show`.
- **metrics `--export <typo>` неотличим от 'no runs yet'** — `kind: ux` (observability). **Фикс**: различать unknown-export от no-runs.
- **single-export run report схлопывает multi-line ошибку в `; `** — `kind: ux` (observability). Косметика одного render-пути.
- **chunked-mode warning говорит row_count per-chunk, а он агрегируется** — `kind: docs` (quality). Только unique_columns реально per-chunk; row_count суммируется. **Фикс**: убрать row_count из per-chunk caveat.
- **compression_profile тихо переопределяет explicit compression** — `kind: ux` (formats). `gzip` + profile `fast` → SNAPPY без warning. **Фикс**: one-line warn.
- **`max_file_size` invalid-value error без подсказки единиц; KB=1024 не 1000** — `kind: docs` (formats). **Фикс**: «valid: B/KB/MB/GB», задокументировать KB=1024.
- **Unquoted `{partition}` в YAML-пути → cryptic parser error** — `kind: ux` (formats). **Фикс**: targeted hint про кавычки.
- **init не скаффолдит MSSQL, хотя run его поддерживает** — `kind: gap` (init). Честно отрепорчено, scoped help. **Фикс**: добавить sqlserver:// scaffold.
- **whole-schema scaffold дампит все объекты без include/exclude** — `kind: footgun` (init). 82 stanza, включая bench_narrow ~10.2M строк, без warning. **Фикс**: `--include/--exclude/--limit` glob + warning на multi-million таблицу.
- **init `--output` тихо overwrite без `--force`** — `kind: footgun` (init). Теряет hand-edits. **Фикс**: clobber-guard или note.
- **doctor config-error печатается дважды** — `kind: ux` (doctor). `[FAIL]` + `Error:`. **Фикс**: не дублировать.
- **doctor: FS permission error лейблится 'auth error'** — `kind: ux` (doctor). Hint корректен, лейбл — misnomer. Косметика.
- **doctor cloud-ошибки дампят весь OpenDAL-struct (HTTP-заголовки, request-id)** — `kind: ux`, ×2 area (doctor, destinations). Реальная причина (`InvalidAccessKeyId`/`NoSuchBucket`) погребена. **Фикс**: тримить до root-cause.
- **Azure/S3 connectivity retry-storm ~10s в doctor** — `kind: ux` (destinations; смежно с Azure-категоризацией). **Фикс**: fail-fast preflight retry-policy.
- **Raw OpenDAL error с HTTP-заголовками течёт в `[FAIL]`** — `kind: ux` (destinations). **Фикс**: суммаризовать до one-liner.
- **`check` greenlight'ит stdout+chunked, который `run` отклоняет** — `kind: footgun` (destinations). Footgun заблокирован до утечки байт, но check/run расходятся. **Фикс**: применить plan-validation в check.
- **doctor опускает Destination-строку для stdout** — `kind: docs` (destinations). **Фикс**: `[OK] Destination Stdout (streaming; no preflight needed)`.
- **Нет user-facing cursor/window-позиции в run-output** — `kind: ux` (incremental). '0 rows' opaque; позиция только под RUST_LOG=debug. **Фикс**: one-line `(cursor 2500, unchanged)`.
- **time_window semantics (calendar-midnight, inclusive >=, без upper bound) не задокументированы** — `kind: docs` (incremental). **Фикс**: указать half-open midnight-семантику в help/schema.

## 5. Покрытие тестами

**Прогнано живьём против docker-стека (pg/mysql/mssql + MinIO + fake-gcs):** 251 тест, 217 pass / 34 fail.

| Area | pass/fail | Прогнано |
|---|---|---|
| init + discover | 27/0 | init→doctor→check→run, --discover JSON, cloud-scaffolds, credential-safe inputs (verified duckdb 2500 rows) |
| check + type-report + target | 14/4 | type-fidelity по жёсткому набору, --strict gating, --target bigquery/duckdb/snowflake recovery SQL |
| doctor | 16/5 | SELECT 1 round-trip pg/mysql/mssql, write-probe всех destination, cloud-probe verified в MinIO+fake-gcs |
| plan + apply | 8/6 | full+chunked round-trip, staleness-guard, --force, tampered-plan, inline-url |
| run full/snapshot core | 17/2 | 3 движка→parquet (идентичный fingerprint), --json, --param, --export, --parallel-exports, --reconcile |
| run chunked family | 14/3 | 6 стратегий, kill -9 + --resume exactly-once, max_file_size+chunked row-drop (НЕ воспроизвёлся) |
| integrity trio | 10/4 | validate/reconcile exit-коды, chmod 000 manifest, repair --execute loop, azure manifest_read_error |
| state subcommands | 11/3 | show/files/chunks/progression/reset/reset-chunks, numeric-cursor advance, --stuck-checkpoints |
| observability | 10/2 | schema config (байт-идентичен), metrics-таблица, journal quality-FAIL render, completions bash/zsh/fish |
| destinations | 15/4 | local/s3/gcs/stdout паритет байт-в-байт, stdout-multipart guards |
| incremental + time-window | 13/2 | int+timestamp cursor, boundary (no off-by-one), coalesce, time_window stateless, unix epoch |
| quality + tuning + overrides | 18/5 | NULL-unique fix, все gates, fast/balanced/safe, meta_columns, decimal(20,0) override |
| formats + compression + partition_by | 14/5 | 5 parquet-кодеков, CSV RFC-4180, max_file_size, partition_by (date) |

**НЕ протестировано полноценно (gated/нет эмулятора):**
- **Azure (blob)** — нет эмулятора. Static config-валидация (account_key_env/sas_token_env mutex, expired-SAS hint) проверена; connectivity-категоризация выведена из живой error-строки + чтения категоризатора, без реального write/list.
- **MSSQL scaffolding** — `init` его не поддерживает (gap), так что init-путь для SQL Server не покрыт.
- **CSV compression фактический результат** — проверен как no-op (md5-идентичность), но «правильно сжатый CSV» не существует для сравнения.
- **bench_narrow (~10.2M строк)** — явно исключён из аудита по причине времени экспорта.

## 6. Рекомендации (приоритизированный punch-list)

1. **Починить preflight `SELECT 1` для `table:`-формы** (`src/preflight/postgres.rs:71`) — строить `SELECT * FROM <table>` из `export.table`. Одна правка убирает HIGH-баг и три LOW-симптома «Row estimate ~1 / ложный DEGRADED» по check/plan/run; бьёт именно по тому, что генерит `init`. Наибольший impact на наименьшую правку.
2. **Закрыть CSV+compression тихий no-op + лживый манифест** — отклонять/предупреждать на config-validation и **никогда** не писать манифест с не-применённым кодеком (или реализовать сжатие). Silent-wrong с ложным audit-trail — худший класс.
3. **Сделать `repair --execute` сходящимся** — дописывать манифест и пере-записывать chunk counts (или дать `repair finalize`/manifest-rebuild), чтобы `reconcile && repair && reconcile` доходил до зелёного. Без этого вся integrity-trio некогерентна.
4. **Валидировать `--config` во всех read-only командах** (metrics/journal/state-inspect) — грузить конфиг первым как уже делает `reset`; убрать создание stray `.rivet_state.db`. Снимает системный «пустая/чужая история, exit 0».
5. **Решить накопление осиротевших part-файлов на re-run** — warn/refuse без `--force` при наличии `_SUCCESS` в non-resume run, либо purge старых частей. Один фикс закрывает находки в run-core, chunked и incremental (manifest/disk drift).
6. **Дать категоризатору ошибок per-engine needles** — pg (`{:#}`/`.source()`-цепочка), mssql (`login failed`), s3 (`permissiondenied`/`invalidaccesskeyid`/`403`/`forbidden`), azure/connectivity (`error sending request`); тримить OpenDAL-dump до root-cause; fail-fast retry-policy для preflight. Бьёт по самой частой реальной ошибке (неверный пароль/ключ/endpoint) на 4 из 5 поверхностей.
7. **Plan-time валидация применимости overrides и существования quality/override-колонок** — ловить int4→string и опечатки колонок до run; флагать scale-narrowing decimal как lossy в type-report (правило CLAUDE.md). Убирает мid-run abort и тихие no-op gates.
8. **Защитить артефакты plan/apply** — отклонять/массивить multi-export `plan --output` (тихая потеря экспортов), расширить fingerprint на `resolved_plan`/`base_query` для tamper-evidence, дать inline-url пути actionable выход (подсказать url_env или auth-override на apply).

---

## Приложение: полный список находок (64)

### 🟥 HIGH

**[bug] Postgres wrong-password failure is opaque ('db error') AND gets no hint**  · _doctor (auth + connectivity preflight): rivet doctor -c &lt;cfg&gt;_

On a wrong Postgres password, doctor prints only `[FAIL] Source error: db error`. The categorizer (categorize_source_error in src/preflight/doctor.rs:153) looks for 'password'/'authentication'/'access denied', but the postgres crate's top-level Display for an auth failure renders as just 'db error' (the real 'password authentication failed for user' lives in the nested .source() chain, never surfaced because check_source_auth uses `{}` not `{:#}`). Result: the single most common doctor failure on Postgres is both unreadable and gets the wrong category, so NO actionable hint is printed. Contrast: MySQL ('Access denied') is correctly categorized 'auth error' with a GRANT hint. Even RUST_LOG=debug does not surface the underlying cause.

```
rivet doctor -c pg_badpass.yaml (url with wrong password) -> '[FAIL] Source error: db error', exit 1, no Hint line
```

**[bug] MSSQL wrong-password failure mis-categorized as generic 'error', no hint**  · _doctor (auth + connectivity preflight): rivet doctor -c &lt;cfg&gt;_

MSSQL returns 'mssql: login failed: ... Login failed for user 'sa'. ... (code: 18456 ...)'. The categorizer's auth needles ('password','authentication','access denied') do not match 'login failed', so it falls to generic 'error' and prints no hint. The MSSQL-specific auth hint (contained-DB users, GRANT SELECT) that the code DOES contain at src/preflight/doctor.rs:253 is never reached. Categorizer is MySQL-shaped and misses two of the three supported engines for the most common auth failure.

```
rivet doctor -c mssql_badpass.yaml -> '[FAIL] Source error: mssql: login failed: ... Login failed for user 'sa'', exit 1, no Hint
```

**[bug] S3 wrong-credentials mis-categorized as generic 'error', no auth hint**  · _doctor (auth + connectivity preflight): rivet doctor -c &lt;cfg&gt;_

On bad S3 keys, opendal returns 'PermissionDenied ... InvalidAccessKeyId ... status: 403'. categorize_dest_error (src/preflight/doctor.rs:169) checks `msg.contains("permission denied")` against the lowercased string, but 'PermissionDenied' lowercases to 'permissiondenied' (no space) so it never matches; '403' is only checked in the not-found branch; 'forbidden'/'unauthorized' are absent. So S3 auth failure -> generic 'error', NO hint, even though a correct s3:PutObject/IAM hint exists at line 279. The most common cloud failure gives no remediation.

```
rivet doctor -c s3_badcreds.yaml (wrong access/secret) -> '[FAIL] Destination S3(...) -- error: PermissionDenied ... InvalidAccessKeyId', exit 1, no Hint
```

**[bug] table:-form exports get wrong strategy diagnostics (row estimate ~1, false DEGRADED) — preflight EXPLAINs `SELECT 1` instead of the table**  · _check + type-report + strict + target (rivet check command: type fidelity report, strict gating, warehouse-compat target resolution)_

In src/preflight/postgres.rs:71, `let base_query = export.query.as_deref().unwrap_or("SELECT 1")`. For an export that uses the `table:` shortcut (no `query:`) — the canonical form that `rivet init` emits — the diagnostic never builds `SELECT * FROM <table>`, even though `export.table` is available (it is used at line 140 for the index check). So row_estimate, scan_type, and verdict are all computed against `EXPLAIN SELECT 1`. Result: 'Row estimate: ~1', 'Scan type: Result (cost=0.00..0.01 rows=1 width=4)', 'Verdict: DEGRADED' with 'No index detected -- full table scan. Add an indexed cursor column...'. All wrong: orders has 2500 rows AND a PK btree index. The type-report itself is unaffected (uses real column metadata); only the strategy/estimate/verdict header block is bogus. This misleads sizing/tuning decisions and emits a false DEGRADED + a misleading 'add an index' suggestion for already-indexed tables.

```
Same table, two config forms. TABLE form: `rivet check -c check_basic.yaml` -> 'Row estimate: ~1 / Scan type: Result rows=1 / DEGRADED'. QUERY form `query: SELECT * FROM orders` -> 'Row estimate: ~2K / Scan type: Seq Scan on orders rows=2500'. Actual `rivet run` exported 2,500 rows, confirming the ~1 is wrong.
```

**[bug] Multi-export plan --output silently overwrites: only the LAST export survives in the file**  · _plan + apply (plan artifact lifecycle)_

With >1 export and `--format json --output FILE`, each export is written to the same path in sequence, so the last one overwrites all previous. The file ends up containing a single plan object, not all of them. The only signal is 'Plan written to: FILE' printed once per export. A user planning a 2-export config and then `rivet apply FILE` will silently run only one export and never know the other was dropped. src/pipeline/plan_cmd.rs writes per-export to the same --output target.

```
rivet plan --config rivet.yaml --format json --output plan.json (config has 2 exports) ; then: python3 -c "import json;print(json.load(open('plan.json'))['export_name'])" -> only 'plan-apply_orders_chunked'; first export gone. Exit 0, no warning.
```

**[footgun] Full re-run accumulates orphaned part files; naive glob readers silently over-count**  · _run — full/snapshot core (postgres + mysql + mssql → local parquet/csv)_

Each `run` writes a part file named with chrono::Utc::now() (src/pipeline/single.rs:333 — `let ts = ...format("%Y%m%d_%H%M%S")`), so the filename is unique per run. The destination advertises idempotent_overwrite:true (src/destination/local.rs:53) and a test asserts 'second write must replace the first; no stale content' (local.rs:256) — but that only fires for the SAME filename, which never happens across runs. Result: manifest.json + _SUCCESS are overwritten and correctly reference only the latest part (part_count:1, row_count:2500), but the prior run's parquet is left in the destination. After 5 full re-runs of the same export I had 5 parquet files (228K) in out/pg_orders while the manifest claimed 1 part / 2500 rows. A manifest-aware reader is correct; a Spark/DuckDB/`read_parquet('dir/*.parquet')` consumer reads all 5 → 12,500 rows. There is no overwrite/clean/purge option in `rivet schema config`. The manifest-as-source-of-truth model is defensible, but the silent accumulation is a real data-correctness footgun and is undocumented in the run path. Reproduced deterministically on a fresh directory.

```
rivet run --config fresh.yaml >/dev/null 2>&1; sleep 1; rivet run --config fresh.yaml >/dev/null 2>&1; find out/fresh -name '*.parquet' | wc -l  → 2  (manifest part_count=1)
```

**[bug] max_file_size is silently a no-op for parquet exports below one row group (the common case)**  · _run — chunked family (parallel / checkpoint / resume / keyset / by-days / by-key / chunk_count, plus the max_file_size+chunked row-drop regression)_

ExportSink::maybe_split (src/pipeline/sink/mod.rs:164-172) compares ParquetFormatWriter::bytes_written() (src/format/parquet.rs:94 → ArrowWriter::bytes_written()) against max_file_size. parquet-rs only flushes bytes to the underlying writer when a row group closes; until then bytes_written() returns near-zero. With no `parquet:` profile, parquet_row_group_rows is None (sink/mod.rs:139) so parquet-rs uses its default ~128MB / up to ~1,048,576-row groups. Any chunk smaller than one row group buffers entirely in memory, bytes_written() never reaches the cap, and maybe_split never fires. Result: a declared max_file_size: "64KB" produced 112KB files (one per chunk) with NO warning. Proven format-specific: identical CSV export rolled to 21 files; identical parquet export with row_group_rows:200 rolled to 9 files. An operator capping file size to fit an S3 single-PUT / verify=content requirement (the docs at config/export.rs:161 explicitly suggest 'raise max_file_size down so it fits') gets oversized parquet files silently. GOOD NEWS: rows are never dropped in any configuration (the critical regression holds).

```
rivet run -c maxfile3.yaml  (content_items id<=6000, chunk_size 2000, max_file_size "64KB", format parquet, compression none) → 3 files of ~112KB each, all over the 64KB cap, exit 0, no warning. Same config as CSV → 21 files. Same parquet config + parquet.row_group_rows:200 → 9 files.
```

**[footgun] repair --execute reports success but does not close the trust loop — reconcile/validate stay RED forever**  · _validate + reconcile + repair (integrity trio) — chunked-export trust story_

In a fully isolated test (only a chunk row-count mismatch, no files touched), `repair --execute` printed 'executed 1 · skipped 0 · failed 0 · rows 500' and exited 0. Afterwards: (1) `rivet reconcile` STILL reports '4 match, 1 mismatch' with the SAME chunk 2 as a Repair candidate and exits 1 — the recorded chunk_task.rows_written is untouched (still 499). (2) `rivet validate` now reports an 'untracked object' for the repair-written file. The repair re-export is additive (documented at src/pipeline/repair_cmd.rs:11-18: 'new files alongside the originals; Rivet does not delete or overwrite the old files' and 'repair does not advance last_committed_*'), and there is NO command (no apply/finalize/manifest-rebuild) to re-record counts or rebuild the manifest. Net effect: the CI-friendly loop `reconcile && repair --execute && reconcile` never converges — a green gate is unreachable after a real repair, yet repair's own exit code (0) and summary ('failed 0') read as success.

```
Fresh chunked export; sqlite3 .rivet_state.db "UPDATE chunk_task SET rows_written=499 WHERE chunk_index=2"; rivet reconcile -> '4 match, 1 mismatch' exit 1; rivet repair --execute -> 'executed 1 · failed 0' exit 0; rivet reconcile -> STILL '4 match, 1 mismatch' exit 1 (rows_written still 499).
```

**[bug] repair-written file is flagged 'untracked object' by validate but the manifest is never updated**  · _validate + reconcile + repair (integrity trio) — chunked-export trust story_

After repair --execute, the re-exported chunk lands as e.g. ..._201424_chunk0_5b716....parquet, but manifest.json still lists the original parts only. `rivet validate` then prints 'failure: untracked object: ..._chunk0_... (12347 bytes)'. There is no provided step to reconcile the manifest with the repair output, so a passing validate is impossible while repair artifacts exist on the prefix. (Note: the untracked-object case is non-fatal — json passed:true, exit 0 — so it is a warning, not a gate failure; see the labeling finding below.)

```
After the repair --execute above: rivet validate -c chunked.yaml --format json -> failures: [{"kind":"untracked_object","key":"..._chunk0_...","size_bytes":12347}], passed:true, exit 0.
```

**[footgun] metrics/journal never validate the --config path; a wrong/missing config silently shows unrelated or empty history with exit 0**  · _observability — rivet metrics / journal / schema config / shell completions_

metrics and journal use --config ONLY to locate the parent dir's .rivet_state.db (src/state/mod.rs:646-648 open_sqlite takes Path(config).parent()); the config file is never opened or parsed. Pointing at a nonexistent or wrong config does not error. In a shared dir (e.g. /tmp) it confidently prints whatever exports are in that state DB — including exports that are NOT in any config (saw 20 'ci' rows). In an empty dir it prints 'No metrics recorded yet.' Both exit 0. An operator who typos --config or runs from the wrong cwd gets a confident wrong/empty answer with no hint the config was never found. Compare: a missing --config flag correctly exits 2, so the inconsistency is jarring.

```
rivet metrics --config /tmp/does_not_exist.yaml  ->  exit 0, prints 'ci' export rows from a foreign .rivet_state.db (or 'No metrics recorded yet.' in an empty dir)
```

**[bug] CSV + compression is a silent no-op across ALL codecs; manifest falsely records the requested codec**  · _formats + compression + max_file_size + partition_by_

format: csv with compression: gzip|zstd|snappy|lz4 produces a byte-for-byte UNCOMPRESSED file (orders = 227398 bytes in every case; csv_gzip md5 == csv_plain md5), keeps the .csv extension (never .csv.gz), yet manifest.json records "compression":"gzip". Root cause: src/format/mod.rs create_format() discards compression/compression_level for FormatType::Csv (csv::CsvFormat takes no compression and never wraps the writer in an encoder; flate2 is only a parquet feature in Cargo.toml). No layer warns: config validation accepts it, `rivet check` says nothing, `rivet run` reports success. This is silent-wrong-behavior: the user believes data is compressed and the manifest agrees, but it is not. Either implement CSV compression, or reject/warn at config-validation time, and never write a manifest that claims a compression that was not applied.

```
rivet run -c csvcomp.yaml  (format: csv, compression: gzip) → file out/*.csv == 'CSV text', 227398 bytes, manifest "compression":"gzip"; md5 identical to compression:none
```

**[bug] int4->string override slips past parse/plan/check and only fails mid-run**  · _Quality gates (null_ratio / uniqueness / row_count) + tuning profiles + meta_columns + per-column type overrides_

A `columns:` override that is a VALID type string but INAPPLICABLE to the source column (int4 -> string) is accepted by config parse, by `rivet plan` (exit 0, artifact written), AND by `rivet check --type-report` (which reports the column as 'exact' fidelity, quantity int4 -> text -> Utf8). Only `rivet run` rejects it, after connecting and starting extraction. The schema doc for `columns:` explicitly claims overrides are 'validated at plan time — an invalid type string fails before the export runs'; the validation only covers the type STRING, not the source-type applicability. The runtime message itself is good. The problem is the late detection: an operator gets a green `check` and a successful `plan`, then a mid-run abort. error.rs/source path msg: 'no text rendering for PostgreSQL wire type Int4 (column index 1); a `string` override is supported only for text/json/enum/interval/numeric/uuid columns'.

```
columns: {quantity: string} on SELECT id, quantity(int4) FROM orders -> `rivet check --type-report` prints 'exact', `rivet plan` exits 0, `rivet run` exits 1 with the wire-type error.
```

### 🟨 MEDIUM

**[ux] Cloud probe errors dump the entire opendal error (HTTP headers, request IDs)**  · _doctor (auth + connectivity preflight): rivet doctor -c &lt;cfg&gt;_

Every S3/GCS/Azure failure prints the full opendal error struct inline on the [FAIL] line: uri, the complete response Parts { status, version, ~15 HTTP headers, x-amz-request-id, dates }, service, path, written, then finally the actual cause. For a preflight tool meant to give a crisp verdict this is a wall of noise; the operator must scan to the very end to find 'InvalidAccessKeyId' / 'NoSuchBucket'. The hint (when categorized) helps, but the error line itself should be trimmed to the root cause.

```
rivet doctor -c s3_badcreds.yaml -> single [FAIL] line ~600 chars of opendal Parts{...} before the real S3Error
```

**[bug] Azure connectivity/DNS failure mis-categorized as generic 'error', no hint**  · _doctor (auth + connectivity preflight): rivet doctor -c &lt;cfg&gt;_

Against a non-resolvable Azure account, the error is 'Unexpected ... send http request, source: error sending request for url (https://...blob.core.windows.net/...)'. categorize_dest_error connectivity needles are 'connect'/'refused'/'timed out'/'dns'/'endpoint' — none appear literally ('error sending request' / 'send http request' don't contain them), so it falls to generic 'error' with no connectivity hint. Also: opendal's retry layer retries the doomed probe (WARN 'will retry after 0.2s') before failing, adding latency to a preflight that should fail fast. Assessed without an emulator; categorization gap confirmed from the live error string + reading the categorizer.

```
rivet doctor -c azure_cfg.yaml (fake account) -> '[FAIL] Destination Azure(...) -- error: Unexpected ... error sending request for url', exit 1, no Hint
```

**[footgun] CLI `--target <typo>` is silently ignored (exit 0, no warning); config-level `target:` typo errors loudly — inconsistent, and a code comment claims it never happens**  · _check + type-report + strict + target (rivet check command: type fidelity report, strict gating, warehouse-compat target resolution)_

src/cli/dispatch.rs:194 does `target.as_deref().and_then(ExportTarget::parse)`; an unrecognized target (e.g. `bigqery`) parses to None and is silently dropped to `tgt = None`, discarding the original string so no warning is even possible. Meanwhile src/preflight/mod.rs:173-180 validates the *config-level* `target:` and bails loudly — with a comment 'A declared-but-unknown target is a loud error — never silently ignored.' That guarantee only holds for the config path, not the `--target` flag. Impact: `rivet check --target bigqery --strict` in CI silently performs NO warehouse check and exits 0, giving false assurance of BigQuery compatibility.

```
rivet check -c check_basic.yaml --target nosuchwarehouse -> plain report, no warning, exit 0. vs config `target: bigqery` -> Error: unknown target 'bigqery', exit 1.
```

**[bug] Bogus row_estimate=1 for table: (full mode) — drives wrong priority/verdict**  · _plan + apply (plan artifact lifecycle)_

In full/snapshot mode with the `table:` shortcut (no explicit query), preflight estimates rows by EXPLAINing base_query, which defaults to 'SELECT 1' (src/preflight/postgres.rs:71 base_query = export.query.unwrap_or("SELECT 1"); estimate_rows_pg EXPLAINs it -> rows=1). So every table-mode full export reports Row est ~1 regardless of real size (orders=2500, pg reltuples=2500). The same export written as query:'SELECT * FROM orders' correctly estimates 2500. The bogus 1 then mis-classifies the export as [small_table] in prioritization and skews cost/wave advice.

```
table: orders, mode: full -> computed.row_estimate=1; query:'SELECT * FROM orders', mode: full -> computed.row_estimate=2500. Actual rows=2500.
```

**[footgun] Apply does not verify artifact integrity — tampered resolved_plan runs without objection**  · _plan + apply (plan artifact lifecycle)_

plan_fingerprint is a descriptive chunk-plan hash and is EMPTY for non-chunked plans (artifact.rs:46 'empty for non-chunked'). apply_cmd.rs guards only staleness + cursor_drift; it clones artifact.resolved_plan verbatim and runs it. Editing resolved_plan.base_query from 'SELECT * FROM orders' to 'SELECT * FROM users' (keeping dates fresh) applied cleanly and exported 500 rows of the WRONG table under the original export name. For a 'plan now, apply later (e.g. in CI)' model this means the artifact is fully trusted and not tamper-evident.

```
edit plan_env.json resolved_plan.base_query -> 'SELECT * FROM users'; rivet apply plan_tampered.json -> status success, rows 500 (users), exit 0.
```

**[ux] Inline url: configs produce un-appliable artifacts; warning is not actionable**  · _plan + apply (plan artifact lifecycle)_

plan strips the plaintext password (good) leaving source.url='postgresql://REDACTED@...' with no url_env/url_file. The plan-time WARN says 'apply time must have equivalent env/file-based auth available' but names no env var, and apply has no --url/--password-env/--url-env flag to supply one. Result: apply fails 'invalid configuration: password missing' (exit 1) with no path forward short of rewriting the config to use url_env and re-planning. The documented url_env path round-trips perfectly, so the fix is to either (a) tell the user exactly to switch to url_env and re-plan, or (b) accept an auth override flag on apply.

```
plan from inline-url config -> apply plan_full.json (even with DATABASE_URL set) -> 'invalid configuration: password missing', exit 1.
```

**[ux] Missing --param error speaks 'environment variable / secret', never names --param as the fix**  · _run — full/snapshot core (postgres + mysql + mssql → local parquet/csv)_

When a query contains ${id_max} and the operator forgets `-p id_max=...`, the fatal error is: 'environment variable 'id_max' referenced in config is not set (a missing secret silently becomes an empty string — refusing)'. Source: src/config/resolve.rs:36-40. The resolver merges --param and env-var namespaces (params take precedence, then env fallback — resolve.rs:31-41), but the error message only mentions the env-var branch and 'secret'. A user who thinks of ${id_max} as a query parameter passed via -p gets no hint that `--param id_max=<value>` would fix it. The contrast is jarring because the sibling unused-param WARN (resolve.rs warn_unused_params) DOES speak --param language ('--param X was not referenced by any ${X} placeholder'). Both can fire in the same invocation. Fix: when params were supplied (or always), append a hint like '… set it via --param id_max=<value> or an env var'.

```
rivet run --config param.yaml -p id_min=100   →  Error: environment variable 'id_max' referenced in config is not set ... (exit 1)
```

**[footgun] Re-running a completed chunked export to the same local destination (no --resume) silently doubles the data**  · _run — chunked family (parallel / checkpoint / resume / keyset / by-days / by-key / chunk_count, plus the max_file_size+chunked row-drop regression)_

The _SUCCESS overwrite guard only triggers under --resume. A plain re-run of the same config to the same local path writes a fresh full set of timestamp-named part files alongside the old ones — no overwrite, no warning, exit 0. A downstream consumer globbing *.parquet then reads 2x the rows. Verified: out_rerun ended with 10 parts (2 timestamp groups), duckdb total=10000 / distinct=5000. Operators frequently re-run the same config to a stable path; a non-resume run into a dir that already carries a _SUCCESS marker should at least warn (or refuse without --force), the same way --resume already does.

```
rivet run -c rerun.yaml ; rivet run -c rerun.yaml  (same local path, no --resume) → both exit 0, second run appends; duckdb total=10000 distinct=5000
```

**[footgun] validate exits 0 (legacy_run) when --date / --prefix point at a non-existent prefix — misconfigured CI silently passes**  · _validate + reconcile + repair (integrity trio) — chunked-export trust story_

A genuinely-absent manifest is treated as 'legacy_run (no manifest at destination — pre-0.7.0 prefix)' and exits 0. So `rivet validate --date 2025-01-01` (a typo'd or never-landed day) and `--prefix ./empty_dir/` both PASS. The wave-1 fix correctly distinguishes a manifest READ error (exit 1) from an ABSENT manifest (exit 0), and the legacy-run-passes choice is intentional (src/pipeline/validate_manifest.rs:275-283, head Ok(None) -> legacy()). But in CI a wrong --date/--prefix or a run that never produced output reads as a clean validate. Worth a flag like --require-manifest (fail if no manifest found) or a louder distinction in the exit gate so an empty/wrong target cannot be mistaken for a verified one.

```
rivet validate -c chunked.yaml --date 2025-01-01 -> 'status: legacy_run...' exit 0 (no data ever landed on that day); same for --prefix ./empty_prefix/.
```

**[footgun] reset-chunks -e <typo> silently succeeds (exit 0) — no export-name guardrail, unlike state reset**  · _state subcommands (show / files / chunks / progression / reset / reset-chunks)_

`state reset` validates the export name against the config and errors with a 'Known exports' hint on a typo (the code comment at cli.rs:109-130 explicitly calls this out as an ops footgun it fixed). Its sibling `reset-chunks -e` does NOT: pipeline::reset_chunk_checkpoint (cli.rs:214-222) opens the state store and runs the delete without loading the config or checking the name. A typo prints 'Removed 0 chunk run record(s) for export <typo>.' and exits 0 — looks like success, did nothing. The guardrail should be applied to both siblings for consistency.

```
rivet state reset-chunks -c chunk_cfg.yaml -e state_orders_chunkedd  ->  'Removed 0 chunk run record(s) for export 'state_orders_chunkedd'.'  (exit 0)
```

**[bug] reset / reset-chunks do not clear export_progression — progression diverges from cursor and shows a stale committed boundary + timestamp**  · _state subcommands (show / files / chunks / progression / reset / reset-chunks)_

StateStore::reset() is just 'DELETE FROM export_state' (cursor.rs:75) and reset_chunk_checkpoint() only clears chunk_run/chunk_task; neither touches export_progression. After `state reset`, `state show` correctly reports 'No incremental cursor recorded' but `state progression` still reports COMMITTED 2500 — and after re-running, the monotonic guard (cursor didn't advance past 2500) leaves last_committed_at at the OLD pre-reset timestamp, so progression shows a committed boundary whose timestamp predates the most recent run. An operator who resets to force a re-export sees contradictory state across two commands in the same family. The actual re-export works (the driver reads the cursor, not progression), so this is a legibility/correctness-of-reporting issue, not data loss — but reset arguably should clear or refresh the progression row, or at least print a note that progression is left intact.

```
rivet state reset -c cfg.yaml -e state_orders_incr  (show: empty)  but  rivet state progression -c cfg.yaml  ->  state_orders_incr  incremental  2500  2026-06-10 20:10:55 UTC  (stale)
```

**[footgun] Read-only state inspection commands never load the config; a wrong/garbage config path gives false 'no state' (exit 0) and litters a new .rivet_state.db**  · _state subcommands (show / files / chunks / progression / reset / reset-chunks)_

show/files/chunks/progression/reset-chunks(-e) call StateStore::open(config_path) directly, which only derives the state-DB path from the config's PARENT directory (mod.rs:646-657) and never parses the config. Consequences: (1) a missing config path leaks a raw SQLite error 'unable to open database file: <dir>/.rivet_state.db: Error code 14' that names a file the user never typed; (2) a config file that EXISTS but is invalid YAML returns 'No exports have been run yet' with exit 0 — false reassurance for a typo'd path pointing at the wrong dir; (3) running a read-only inspect command against any path CREATES a 114KB .rivet_state.db there (migrate() runs on open), littering disk. Contrast: `reset` and `reset-chunks --stuck` DO call Config::load first and give a clean 'config file not found. Hint: check the path, or run `rivet init`'. The inspect commands should load/validate the config first for the same clean error and to avoid creating a stray DB.

```
T=$(mktemp -d); echo 'garbage: [' > $T/cfg.yaml; rivet state show -c $T/cfg.yaml  ->  'No exports have been run yet.' (exit 0); ls $T  ->  .rivet_state.db now exists (114688 bytes)
```

**[gap] journal output omits the file list, the plan snapshot, and the full event timeline that the journal DB actually stores**  · _observability — rivet metrics / journal / schema config / shell completions_

The run_journal table stores rich journal_json: PlanResolved (base_query, strategy, batch_size, validate/reconcile), each FileWritten (file_name, rows, bytes, part_index), RunCompleted, and would store retries/chunk events/schema-changes. The CLI render (src/pipeline/cli.rs:323-454) only prints an aggregate 'files: N  rows: X  size: Y' line — never the individual file names, never the plan. So the Epic-10 DoD question 'What was planned?' (plan_snapshot) is recorded but unreachable via the CLI, and 'which files did this run produce?' (file_log/FileWritten file_name) is unanswerable from journal even with --run-id. Retries/quality/schema-change lines DO render when present (verified quality [FAIL] renders), so the help text is accurate for those; the gap is files+plan.

```
rivet journal -e observability_orders --run-id <id>  ->  shows only 'files: 1  rows: 100  size: 4.5 KB', not the parquet filename or base_query, both of which are in run_journal.journal_json
```

**[gap] No machine-readable (--json) output for metrics or journal**  · _observability — rivet metrics / journal / schema config / shell completions_

Both commands emit only a human table/text block. There is no --json to feed run history into a dashboard/orchestrator, despite rivet otherwise embracing machine contracts (schema config, --json-errors, run --json/--summary-output). To consume history programmatically an operator must open the SQLite state DB directly. For an observability surface this is the most notable missing affordance.

```
rivet metrics -c rivet.yaml --json  ->  error: unexpected argument '--json' found
```

**[bug] doctor leaves a .rivet_doctor_probe file at the destination (never cleaned up)**  · _destinations: local / s3(minio) / gcs(fake) / stdout / azure(static)_

src/preflight/doctor.rs:134-151 check_destination_auth() writes a local temp probe, calls d.write(&tmp, probe_key) to copy it INTO the destination at key '.rivet_doctor_probe', then only std::fs::remove_file(&tmp) deletes the *local* temp. The probe object written to the destination (local dir / S3 bucket / GCS bucket) is never deleted, despite the log::debug!('...cleaning up') and the surrounding intent. Every doctor run pollutes the destination with a stray 2-byte 'ok' file that lands right next to the real data in the export prefix — a downstream consumer listing the prefix sees a non-data file. Lossless data, but it mutates the destination on a preflight 'read-only' check and the residue persists.

```
mkdir d; printf 'local cfg path: ./d' ; rivet doctor -c local.yaml; ls -A d -> .rivet_doctor_probe remains. Also visible in `aws s3 ls s3://<bucket>/<prefix>` -> '.rivet_doctor_probe 2' alongside the parquet/manifest.
```

**[ux] Connectivity/unreachable-endpoint errors get NO actionable hint + raw OpenDAL dump**  · _destinations: local / s3(minio) / gcs(fake) / stdout / azure(static)_

categorize_dest_error (src/preflight/doctor.rs:197-203) keys 'connectivity error' on the substrings connect/refused/timed out/dns/endpoint. But OpenDAL+reqwest phrase a dead port / unresolvable host as 'Unexpected (persistent) at write ... => send http request, source: error sending request for url (...)' — none of those keywords match, so it falls through to the generic 'error' bucket and dest_error_hint returns None. Result: a typo'd S3 endpoint or Azure account_name produces a [FAIL] with the full raw OpenDAL context (incl. internal http_util::Client::send) and zero guidance, even though a perfectly good S3/GCS/Azure connectivity hint exists at doctor.rs:304-312 and never fires. The bad-bucket and missing-env cases get great hints; the most common 'wrong endpoint/host' case gets the worst message.

```
rivet doctor -c s3_badendpoint.yaml (endpoint http://127.0.0.1:9999) -> [FAIL] ... Unexpected (persistent) ... 'error sending request for url' with no Hint line. exit 1.
```

**[bug] partition_by is date-only but documented as general value partitioning**  · _formats + compression + max_file_size + partition_by_

The schema/docs (and this audit's brief) advertise partition_by as 'one sub-prefix per distinct bucket of this column's value (Hive-style col=value/)' and explicitly show `partition_by: status`. In practice partition_by only accepts date/timestamp columns bucketed by partition_granularity (day/month/year). Running partition_by: status fails with 'could not parse partition min 'cancelled' from column 'status' as a date'. Date partitioning itself is correct (Hive dirs, NULL→__HIVE_DEFAULT_PARTITION__, per-partition _SUCCESS, total rows preserved), but the feature is narrower than its own documentation. Fix the docs/schema to say 'date/timestamp column only', or implement true value partitioning.

```
rivet run -c part.yaml (partition_by: status on orders) → exit 1: could not parse partition min 'cancelled' from column 'status' as a date
```

**[footgun] max_file_size is a batch-granular soft lower-bound, not a hard cap, and is a no-op for typical parquet exports**  · _formats + compression + max_file_size + partition_by_

maybe_split() is only called after each full batch (src/pipeline/sink/mod.rs:419). Consequences: (1) a whole table that fits in one batch (default 10k rows) NEVER splits regardless of max_file_size — a 222KB CSV with max_file_size: 64KB silently produced 1 file. (2) When splits do occur, parts can exceed the limit (a 64KB cap produced 90KB/91KB parts because the check fires after a batch already pushed past it). (3) For parquet the bytes_written() only counts FLUSHED bytes, so with the default ~1M-row row group an 82KB parquet with max_file_size: 32KB stays a single file; you must set small parquet.row_group_rows for splitting to engage. None of these emit a warning. verify: content docs even reference 'raise max_file_size down so a part fits a single PUT', which the soft-limit cannot guarantee. Document the batch-granularity clearly and/or warn when max_file_size < a batch's expected output.

```
rivet run -c split.yaml (max_file_size 64KB on 222KB CSV, default batch) → '1 files'; rivet run -c split2.yaml (batch_size 500) → parts of 90020/91667 bytes, exceeding 64KB; parquet never splits without small row_group_rows
```

**[footgun] Per-run manifest overwrite + no cleanup of prior part files: manifest/disk drift, silent duplicate accumulation on re-run**  · _incremental + time-window modes (cursor lifecycle)_

manifest.json and _SUCCESS are rewritten every run describing ONLY that run's files; prior part files are not removed. (1) After a 0-new incremental run, manifest reports row_count:0, part_count:0, parts:[] while a real 2500-row parquet from the prior run sits in the same directory (rivet::pipeline::finalize: 'manifest.json written (0 parts, 0 rows) + _SUCCESS'). A downstream consumer trusting manifest.json as the part inventory sees zero parts and skips the real data. (2) On a re-run/reset+re-run to the same local prefix, tw3 had 2 parquet files (466 rows each = 932 if the prefix is globbed) but manifest claimed part_count:1. time_window is meant to be re-run on a schedule, so a fixed local destination silently accumulates duplicate windows. Mitigation today is a fresh/partitioned prefix per run, but nothing warns the operator. Verify expectations against the manifest contract.

```
rivet run --config tw3.yaml (twice) ; ls out/tw3/*.parquet -> 2 files, 466 rows each ; grep part_count out/tw3/manifest.json -> 1
```

**[gap] rivet check does not catch a non-existent cursor_column pre-flight**  · _incremental + time-window modes (cursor lifecycle)_

`check` is documented as 'Step 2 — column-type & schema report for each export' and the help recommends running it before `run`. With cursor_column: does_not_exist it reports Verdict: DEGRADED and 'No index detected -- full table scan' (exit 0) but never flags that the cursor column isn't a real column. The failure only appears at `run` time as a raw DB error. Since check already introspects the schema, a missing cursor/time column should be surfaced as a pre-flight failure.

```
rivet check --config err4.yaml  -> Verdict: DEGRADED, exit 0 ; rivet run --config err4.yaml -> db error: column "does_not_exist" does not exist, exit 1
```

**[bug] Scale-reducing decimal override silently truncates data but check reports 'exact' fidelity**  · _Quality gates (null_ratio / uniqueness / row_count) + tuning profiles + meta_columns + per-column type overrides_

Source orders.price is numeric(10,2) with values 10.99/11.99/12.99. With `columns: {price: decimal(20,0)}`, `rivet check --type-report` labels the column Fidelity = 'exact', and the run succeeds (exit 0), but the written parquet contains Decimal('10'), Decimal('11'), Decimal('12') — the .99 fraction is silently dropped. The operator did request scale 0, so the truncation is arguably their explicit choice, but labelling a lossy scale reduction as 'exact' is misleading and violates the project rule that lossy degradations must be surfaced, not hidden behind a green report. At minimum the type-report should flag scale-narrowing overrides as lossy rather than 'exact'.

```
price numeric(10,2)=10.99; columns:{price: decimal(20,0)}; check --type-report -> 'exact'; run -> output value Decimal('10').
```

**[footgun] Quality checks and column overrides on a non-existent column are silent no-ops (typo/drift footgun)**  · _Quality gates (null_ratio / uniqueness / row_count) + tuning profiles + meta_columns + per-column type overrides_

unique_columns: [nonexistent_col], null_ratio_max: {ghost_column: 0.0}, and columns: {not_a_column: string} ALL run to success (exit 0, quality: pass) with no warning when the named column is absent from the export. A fat-fingered column name, or a schema that drifts and drops a column, disables the intended gate/override silently — the run is green but the guarantee is gone. This is the same 'never a silent no-op' class called out in CLAUDE.md. In src/quality.rs the uniqueness loop uses `if let Ok(idx) = batch.schema().index_of(col_name)` (missing -> skipped, 0 dups) and check_null_ratios uses `null_counts...unwrap_or(0)` (missing -> ratio 0.0). A plan-time validation that every quality/override column exists in the resolved schema would close all three at once.

```
unique_columns: [nonexistent_col] -> rivet run exits 0, quality: pass, no warning.
```

### ⬜ LOW

**[gap] init cannot scaffold MSSQL, though rivet run supports SQL Server**  · _init + discover (scaffold a runnable config from a live DB: single-table / whole-schema YAML, --discover JSON artifact, cloud scaffolds, credential-safe source inputs)_

init/discover only accept postgresql:// and mysql:// (src/init/mod.rs:178-188 source_type()). Pointing it at the seeded MSSQL instance fails with 'Unsupported source URL scheme. Expected postgresql:// or mysql://, got: sqlserver://REDACTED@...'. The top-level CLI and `rivet run` clearly support SQL Server (Epic 18 source-engine parity per recent commits), so a SQL Server user gets no scaffolding help and must hand-write the config. The error is honest and the help text scopes it to pg/mysql, so this is a coverage gap not a bug. The password IS redacted in the error, which is good.

```
rivet init --source 'sqlserver://sa:Rivet_Passw0rd!@127.0.0.1:1433/rivet' --table dbo.orders -> exit 1, 'Unsupported source URL scheme...'
```

**[footgun] Whole-schema scaffold dumps every object with no include/exclude filter**  · _init + discover (scaffold a runnable config from a live DB: single-table / whole-schema YAML, --discover JSON artifact, cloud scaffolds, credential-safe source inputs)_

On the seeded Postgres, `rivet init` with no --table produced a config with 82 exports — most are leftover test-pollution tables (bq_pg_*, type_rt_pg_*, etc.) and it includes bench_narrow (~10.2M rows) which the audit brief explicitly flags as too slow to export. The scaffold is correct and the per-export inline comments make pruning easy, but on a real DB with many tables the output is large and the operator must manually delete dozens of stanzas. There is no --include/--exclude/--limit glob to narrow the scope, and no warning that a multi-million-row table was scaffolded for full/chunked export. Mode selection itself is sound (chunked for big tables with a chunk column, full otherwise).

```
rivet init --source <pg> --output schema.yaml -> '# Generated by rivet init — PostgreSQL schema "public" (82 objects)' with 82 export stanzas
```

**[footgun] --output silently overwrites an existing config with no --force guard**  · _init + discover (scaffold a runnable config from a live DB: single-table / whole-schema YAML, --discover JSON artifact, cloud scaffolds, credential-safe source inputs)_

Writing init --output to an existing path overwrites it without prompting or any --force/no-clobber flag. A user who hand-edited a generated config and re-runs init to refresh one table would lose their edits. Common CLI behavior, but given init explicitly tells users to 'Review and adjust before running', a clobber guard (or a note) would protect those edits.

```
rivet init ... --table users -o ow.yaml ; rivet init ... --table orders -o ow.yaml -> second write overwrites, exit 0, no warning
```

**[ux] check reports 'Row estimate: ~1' for the init-scaffolded full-scan export (init's own estimates are accurate)**  · _init + discover (scaffold a runnable config from a live DB: single-table / whole-schema YAML, --discover JSON artifact, cloud scaffolds, credential-safe source inputs)_

This is NOT an init defect — init's row estimates were accurate everywhere (users ~500, orders ~2K, content_items ~1.2M, all correct vs source). But when the generated scaffold is fed to `rivet check`, the full-scan path prints 'Row estimate: ~1' for orders (2500 rows) and users (500 rows), because check's EXPLAIN probe returns rows=1. Since init's Next-steps guide funnels users straight into `rivet check`, the immediate next screen contradicts the scaffold's own '~2K rows' comment, which is confusing. Worth flagging to the check/plan owner; out of init's strict scope.

```
rivet init ...--table public.orders -o rt.yaml (comment says ~2K) ; DATABASE_URL=<pg> rivet check -c rt.yaml -> 'Row estimate: ~1'
```

**[ux] Config error printed twice (once as [FAIL], once as Error:)**  · _doctor (auth + connectivity preflight): rivet doctor -c &lt;cfg&gt;_

On a config-load failure, doctor() prints '[FAIL] Config error: <msg>' (doctor.rs:14) and then returns Err(e), which the top-level handler prints again as 'Error: <msg>'. The full message (including multi-line hints like 'Hint: check the path, or run rivet init') appears verbatim twice. Harmless but looks like a bug to a careful reader.

```
rivet doctor -c missing.yaml -> '[FAIL] Config error: config file ... not found.\n  Hint: ...' followed by 'Error: config file ... not found.\n  Hint: ...'
```

**[footgun] Local destination probe file (.rivet_doctor_probe) is left behind in the output dir**  · _doctor (auth + connectivity preflight): rivet doctor -c &lt;cfg&gt;_

check_destination_auth (doctor.rs:134) removes the temp /tmp copy but intentionally does NOT remove the probe object written into the destination; for local destinations this leaves a stray '.rivet_doctor_probe' dotfile in the user's output directory after every doctor run. Verified it is filtered by manifest_reconcile.rs:182 and validate_manifest, so it does NOT corrupt later verification (the design comment is accurate), but an unexpected dotfile in a user-facing output dir is mildly surprising and never cleaned up.

```
rivet doctor -c pg_ok.yaml; ls out_pg/ -> .rivet_doctor_probe persists
```

**[ux] Filesystem permission error is labeled 'auth error'**  · _doctor (auth + connectivity preflight): rivet doctor -c &lt;cfg&gt;_

An unwritable local destination (read-only parent) is reported as '[FAIL] Destination Local(...) -- auth error: Permission denied (os error 13)'. Calling a filesystem permission a 'auth error' is a slight category misnomer (there is no auth on a local FS), though the accompanying hint ('Verify filesystem permissions on the destination directory') is correct and useful. Cosmetic.

```
rivet doctor -c local_ro.yaml (path under a chmod 555 dir) -> '[FAIL] ... -- auth error: Permission denied (os error 13)'
```

**[docs] Unknown-target error message omits `snowflake` from the expected list**  · _check + type-report + strict + target (rivet check command: type fidelity report, strict gating, warehouse-compat target resolution)_

src/preflight/mod.rs:177 emits 'unknown target '...' (expected: bigquery, duckdb)'. But ExportTarget::parse (src/types/target.rs:54-57) accepts bigquery/bq, duckdb/duck, AND snowflake/sf — and `--target snowflake` works (verified: emits TO_TIMESTAMP_NTZ recovery SQL). The error message under-advertises a supported target.

```
rivet check -c check_badtarget.yaml --type-report -> Error: ... (expected: bigquery, duckdb) — snowflake missing though valid.
```

**[footgun] `--target` alone does not gate exit code on a hard target FAIL; only `--strict` does**  · _check + type-report + strict + target (rivet check command: type fidelity report, strict gating, warehouse-compat target resolution)_

A column rendered as 'fail ✗' for the target (e.g. numeric(38,9) on bigquery) does NOT cause a non-zero exit unless `--strict` is also passed (src/preflight/mod.rs:203-205 sets any_fatal, but bail only fires `if strict && any_fatal` at 218). The 'fail ✗' glyph visually implies a hard failure, so a CI job running `rivet check --target bigquery` to gate on warehouse compat would pass (exit 0) despite a hard FAIL. Defensible (report is informational, --strict is the gate) but the glyph/exit mismatch is a mild trap.

```
rivet check -c check_rich.yaml --target bigquery -> exit 0 (despite c_huge_numeric 'fail ✗'); add --strict -> exit 1.
```

**[footgun] --output is silently ignored unless --format json is also passed**  · _plan + apply (plan artifact lifecycle)_

plan --help: '-o, --output  Write plan JSON to this file (default: print summary to stdout)'. But in the default pretty format, --output is ignored: the summary prints to stdout and no file is written, with no warning. A user expecting a file from `rivet plan --config x --output plan.json` gets nothing on disk. Should either honor --output by emitting JSON, or warn that --format json is required.

```
rivet plan --config rivet.yaml --output ovr.json ; test -f ovr.json -> NO. Exit 0, no warning.
```

**[ux] Multi-export --format json to stdout emits invalid concatenated JSON (no array/NDJSON)**  · _plan + apply (plan artifact lifecycle)_

With >1 export, `plan --format json` (no --output) prints N JSON objects back-to-back. It is neither a JSON array nor newline-delimited JSON, so standard parsers / jq fail ('Extra data: line 173'). Orchestration consumers must hand-split the stream. Either wrap in an array or emit NDJSON.

```
rivet plan --config rivet.yaml --format json | python3 -c 'import json,sys;json.load(sys.stdin)' -> JSONDecodeError: Extra data.
```

**[ux] `rivet check` row estimate for a 2500-row table reads '~1'**  · _run — full/snapshot core (postgres + mysql + mssql → local parquet/csv)_

Tangential to the run path (check is a separate command) but observed while verifying schema: `rivet check` on the orders table (2500 rows) reports 'Row estimate: ~1' and 'Scan type: Result (cost=0.00..0.01 rows=1 width=4)', then 'Verdict: DEGRADED'. The planner probe appears to read a trivial EXPLAIN node's rows=1 rather than the table cardinality, which would mislead an operator sizing a job. Worth a glance by the check-command owner; not a run-path bug.

```
rivet check --config checkcfg.yaml  → 'Row estimate: ~1' for a 2500-row table
```

**[ux] Nonexistent --export error does not list available export names**  · _run — full/snapshot core (postgres + mysql + mssql → local parquet/csv)_

`--export run-core_does_not_exist` fails cleanly (exit 1) with 'Error: export 'run-core_does_not_exist' not found in config'. Good — it names the bad value. But unlike the excellent unknown-config-field error (which lists all valid fields + 'Did you mean'), this one does not enumerate the available export names, which would make a typo a one-glance fix. Minor polish gap given how good the rest of the error UX is.

```
rivet run --config multi.yaml --export run-core_does_not_exist  → 'not found in config' (no list of valid names)
```

**[ux] chunked → snapshot downgrade (small-table escape) is only visible as the word 'full' in the one-line status**  · _run — chunked family (parallel / checkpoint / resume / keyset / by-days / by-key / chunk_count, plus the max_file_size+chunked row-drop regression)_

When a table-shortcut chunked export has no explicit chunk-shape knob and the table's reltuples estimate is <= chunk_size, resolve_chunked_strategy (src/plan/build.rs:315-342) intentionally downgrades to Snapshot — a sound optimization that correctly does NOT fire when chunk_checkpoint/chunk_count/chunk_by_days/chunk_by_key are set. But the explanation ('~N rows <= chunk_size; downgrading chunked → snapshot') is logged at info level (invisible by default). At default verbosity the only signal is 'full' in the summary; the detailed report block omits it entirely. An operator who wrote 'mode: chunked' expecting parallel/checkpoint behavior gets a single snapshot query with no clear notice. A one-line WARN or a 'mode: chunked → snapshot (table fits one chunk)' note in the report would close the gap.

```
rivet run -c edge_nocol.yaml  (table events ~5000 rows, mode chunked, default chunk_size 100000) → status success, mode 'full', 1 file, no warning at default verbosity
```

**[ux] validate prints 'untracked object' under 'failure:' yet returns passed=true / exit 0**  · _validate + reconcile + repair (integrity trio) — chunked-export trust story_

An untracked extra object is rendered in the pretty output under the same 'failure:' label as a hard failure (missing part), and appears in the json 'failures' array, but it does NOT flip passed to false and does NOT change the exit code (verified exit 0 with passed:true, parts_failed:0). Treating an orphan file as non-fatal is defensible, but labeling it 'failure' while exiting 0 is contradictory — an operator scanning output sees 'failure:' lines and assumes the gate is red when it is green. A distinct 'warning:' / 'note:' label (or a 'warnings' json array separate from 'failures') would remove the ambiguity.

```
rivet validate -c chunked.yaml  -> prints 'failure: untracked object: ...' yet  echo $? = 0; json passed:true.
```

**[ux] repair re-export re-indexes the chunk as chunk0 instead of preserving the original chunk index**  · _validate + reconcile + repair (integrity trio) — chunked-export trust story_

Repairing chunk 2 (id range [1001..1500]) wrote a file named ..._<ts>_chunk0_<hash>.parquet, not chunk2. The Precomputed single-chunk repair source restarts indexing at 0, so the on-disk filename no longer reflects which logical chunk it repairs. Combined with the additive-no-manifest-update behavior, this makes manual reconciliation of which file covers which range harder than it should be. Embedding the original chunk index (or a 'repair' marker) in the filename would aid forensics.

```
rivet repair --execute on chunk 2 -> ls out/.../  shows ..._chunk0_... (timestamped) rather than ..._chunk2_...
```

**[ux] state files RUN ID column (35 wide) overflows the 40-char run_id, breaking column alignment**  · _state subcommands (show / files / chunks / progression / reset / reset-chunks)_

In pipeline/cli.rs:144-157 the header uses {:<35} for RUN ID, but run_ids like 'state_orders_chunked_20260610T201342.493' are 40 chars, so data rows are pushed right and the FILE/ROWS/BYTES columns no longer line up under their headers (header line 104 chars, data line 162 chars). Cosmetic only, but makes the table hard to scan and undercuts the otherwise-clean tabular output.

```
rivet state files -c chunk_cfg.yaml | head -3 | awk '{print length}'  ->  header 104, data 162 (columns misaligned)
```

**[docs] show-empty hint says `rivet state` (no subcommand), which errors**  · _state subcommands (show / files / chunks / progression / reset / reset-chunks)_

The empty-cursor branch of show_state prints '...then try `rivet state` again.' but `rivet state` with no subcommand is an error (it requires <COMMAND>). The hint should say `rivet state show`. Minor wording nit in an otherwise excellent message.

```
rivet state show -c cfg.yaml (after a chunked-only run) -> '...then try `rivet state` again.'  but bare `rivet state` errors with 'Usage: rivet state ... <COMMAND>'
```

**[ux] metrics --export of an unknown/typo'd name prints 'No metrics recorded yet.' instead of distinguishing 'unknown export' from 'no runs yet'**  · _observability — rivet metrics / journal / schema config / shell completions_

Because metrics is purely state-DB-driven and never checks the export name against the config, filtering on a name that has runs in the config but no recorded runs, OR a name that is simply mistyped, both yield the identical 'No metrics recorded yet.' message. journal is slightly better ('No journal entries for export X yet. Journals are recorded after each rivet run.') but still cannot say the export name is unknown. A user debugging empty metrics gets no hint that they fat-fingered the export name.

```
rivet metrics --config rivet.yaml --export does_not_exist  ->  'No metrics recorded yet.' (exit 0), identical to a never-run-but-valid export
```

**[ux] Single-export run report collapses the multi-line error into ';'-joined text, hurting readability of the quality failure**  · _observability — rivet metrics / journal / schema config / shell completions_

The end-of-run single-export report prints 'error: export ...: 1 quality check(s) failed:;   - row_count 10 below minimum 999999;   Fix the source data, or adjust the thresholds under `quality:` ...' — newlines replaced by '; '. The same error renders cleanly (multi-line) both in the top-of-run ERROR log and in the final 'Error:' line, and journal shows just the first line, so this is a cosmetic inconsistency in one render path, not a correctness issue.

```
rivet run --config qual.yaml  (row_count_min:999999)  ->  '  error:     export ...: 1 quality check(s) failed:;   - row_count 10 below minimum 999999;   Fix ...'
```

**[ux] Azure (and S3) connectivity failure retry-storms ~10s before reporting in doctor**  · _destinations: local / s3(minio) / gcs(fake) / stdout / azure(static)_

On an unreachable destination, doctor inherits the export retry policy: the live Azure attempt retried 5 times with escalating backoff (0.37s, 0.75s, 1.5s, 1.67s, 6.2s) emitting scary 'opendal::layers::retry ... Unexpected (temporary)' WARN lines on stderr before the final [FAIL]. A preflight connectivity probe should fail fast (1 attempt, short timeout) — retrying a fat-fingered account name for ~10s and printing 'must be a bug'-adjacent warnings is poor doctor UX.

```
rivet doctor -c azure.yaml (rivetaudit.blob.core.windows.net, no emulator) -> 5 retry WARNs over ~10s, then [FAIL].
```

**[ux] Raw OpenDAL error including full HTTP response headers leaks into user-facing [FAIL]**  · _destinations: local / s3(minio) / gcs(fake) / stdout / azure(static)_

Errors that bubble up from OpenDAL (bad bucket, bad creds) print the entire raw error: 'ConfigInvalid (persistent) at write, context: { uri: ..., response: Parts { status: 404, headers: {accept-ranges, content-length, server, strict-transport-security, x-amz-id-2, x-ratelimit-limit, ...} }, ... }'. The useful bit (NoSuchBucket / InvalidAccessKeyId) is buried in a multi-line headers dump. rivet's own preflight messages (missing env, SAS expiry) are crisp one-liners; the OpenDAL-sourced ones are not summarized to match.

```
rivet doctor -c s3_badbucket.yaml -> [FAIL] line is ~15 fields of OpenDAL context incl. full response headers before '=> S3Error { code: "NoSuchBucket" }'.
```

**[footgun] `check` greenlights stdout+chunked that `run` rejects (false all-clear)**  · _destinations: local / s3(minio) / gcs(fake) / stdout / azure(static)_

`rivet check -c stdout_chunked.yaml` exits 0 and prints Strategy: chunked with no warning; only `run`/`plan` apply plan validation and emit [stdout-no-chunked]. A user who runs `check` first (as the help text 'Step 2' onboarding suggests) gets a false all-clear, then hits the rejection at run time. The footgun is correctly blocked before any bytes leak, so this is informational, but check and run disagreeing on a config's validity is a minor inconsistency.

```
rivet check -c stdout_chunked.yaml -> exit 0, no rejection. rivet run -c stdout_chunked.yaml -> exit 1, [stdout-no-chunked].
```

**[docs] doctor omits the Destination check line entirely for stdout**  · _destinations: local / s3(minio) / gcs(fake) / stdout / azure(static)_

For local/s3/gcs/azure, doctor prints '[OK] Destination <kind>(...)'. For stdout it prints no Destination line at all (just Config + Source). Defensible (no auth/connectivity to verify), but a user can't tell from doctor output whether the stdout destination was even considered — a one-liner like '[OK] Destination Stdout (streaming; no preflight needed)' would close the gap.

```
rivet doctor -c stdout.yaml -> only [OK] Config and [OK] Source; no Destination line.
```

**[ux] compression_profile silently overrides explicit compression with no warning**  · _formats + compression + max_file_size + partition_by_

Setting both compression: gzip and compression_profile: fast yields SNAPPY (profile wins, as documented) but emits no warning that the explicit compression: gzip was discarded. A user who tuned compression and later added a profile gets different output than the explicit field states. A one-line warn ('compression_profile: fast overrides compression: gzip') would prevent confusion.

```
rivet run -c prec.yaml → output codec=SNAPPY, no mention that compression: gzip was ignored
```

**[docs] max_file_size invalid-value error gives no hint of valid units; KB means 1024 not 1000**  · _formats + compression + max_file_size + partition_by_

'invalid max_file_size 'banana'' (and '1KiB' rejected) names the bad value but never states the accepted format (B/KB/MB/GB, fractional ok, IEC units not accepted). Separately, parse_file_size (src/config/resolve.rs:107) treats KB as 1024 bytes — conventionally KB=1000, KiB=1024 — undocumented in the error or schema, a quiet ~2.4% size surprise at GB scale.

```
rivet check -c u.yaml with max_file_size '1KiB' → invalid max_file_size '1KiB' (no 'valid: B/KB/MB/GB' hint); '128KB' silently = 131072 bytes
```

**[ux] Unquoted {partition} in YAML path yields a cryptic parser error instead of a token hint**  · _formats + compression + max_file_size + partition_by_

destination.path: out/{partition} (unquoted, common copy-paste) trips the YAML flow-mapping parser: 'did not find expected ',' or '}' at line 9 column 48'. The error correctly names file/line/column but gives no clue it's the {partition} brace needing quotes. Since {partition} is the required token for partition_by, a targeted hint ('did you mean "{partition}" — wrap path values containing { } in quotes') would save users. Quoted paths work fine.

```
rivet run -c part_month.yaml (path: out_month/{partition} unquoted) → Error: did not find expected ',' or '}' at line 9 column 48
```

**[ux] No user-facing cursor/window position in run output — '0 rows' is correct but opaque**  · _incremental + time-window modes (cursor lifecycle)_

On a 0-new incremental run the summary shows only '0 rows  0 files'; there is no 'cursor unchanged at 2500' or 'no rows newer than 2026-06-07...'. For time_window a 0-row run gives no hint of the computed bound (updated_at >= 2026-06-09 00:00:00), so the operator can't distinguish 'empty window' from 'wrong column/window'. The bound and 'cursor updated to X' lines exist only under RUST_LOG=debug; the --json summary also omits the cursor value and window bounds. A one-line '(cursor 2500, unchanged)' / '(window >= 2026-06-09)' in the human summary would close the loop the task explicitly looks for.

```
rivet run --config inc_ts.yaml (2nd run) -> '0 rows 0 files'; cursor value only via separate `rivet state show`
```

**[docs] time_window semantics (calendar-midnight, inclusive >=, no upper bound) are undocumented in user-facing output/help**  · _incremental + time-window modes (cursor lifecycle)_

days_window: N compiles to `time_column >= midnight(today - N days)` — calendar-day truncated, NOT a rolling N*24h window, inclusive on the low end, and with NO upper bound (no <= now() cap). days_window:3 on 2026-06-10 yielded 466 rows from 2026-06-07 00:00:33 onward, whereas a naive `now() - interval '3 days'` (=2026-06-07 20:19) would yield 0. Future-dated rows would also be included since there is no high cap. Reasonable design, but the half-open midnight semantics aren't stated in --help or schema docs, so operators may misjudge the boundary.

```
rivet run --config tw3.yaml (days_window:3) -> WHERE updated_at >= '2026-06-07 00:00:00', 466 rows (debug log)
```

**[docs] Chunked-mode warning text says row_count is per-chunk, but it is actually aggregated**  · _Quality gates (null_ratio / uniqueness / row_count) + tuning profiles + meta_columns + per-column type overrides_

In chunked mode the plan-validation warning states: 'row_count and unique_columns checks apply to each chunk independently, not the full dataset — results may be misleading'. But the observed behaviour aggregates row_count: with 5 chunks of 500 the failure reported 'row_count 2500 below minimum 99999' (the full total) under the '(chunked aggregate)' context. So row_count IS summed across chunks (correct), only unique_columns is genuinely per-chunk. The warning over-claims by lumping row_count in with the per-chunk caveat, which could confuse an operator into distrusting a correct aggregate row_count gate.

```
mode: chunked + row_count_min 99999 -> warning lumps row_count with per-chunk caveat; failure message shows aggregated 2500.
```