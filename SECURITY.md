# Security Policy

Rivet is a CLI that reads from production databases and writes data to local disk or object storage. This document describes what Rivet has access to, what it writes locally, how credentials are handled, and how to report vulnerabilities.

For execution guarantees (crash, retry, resume), see [docs/semantics.md](docs/semantics.md). For destination commit boundaries, see [ADR-0004](docs/adr/0004-destination-write-contracts.md).

---

## What Rivet can access

When you run Rivet, the process has access to:

- **Source database rows** returned by user-defined `query` strings.
- **Source schema metadata** — column names, types, table list (via `rivet init` / `rivet check --type-report`).
- **SQL queries** authored in `rivet.yaml` (executed verbatim against the source).
- **Cursor and checkpoint values** — the last extracted value for incremental and chunked exports.
- **Destination paths and credentials** — local paths, S3 / GCS bucket names, IAM keys provided via env or YAML.
- **State backend file** — `.rivet_state.db` (SQLite) in the working directory unless overridden.
- **Local files** written by the export: temp files during extraction, final output files, journal/metrics records.

Rivet does **not** execute DDL, `INSERT`, `UPDATE`, or `DELETE` against the source. It issues `SELECT` and (for cursors) `DECLARE CURSOR` / `FETCH` only.

---

## Sensitive local artifacts

The following files may contain sensitive information even when credentials are kept outside them:

| Artifact | Sensitive content |
|---|---|
| `rivet.yaml` | Source URL, query bodies, destination credentials (if inlined) |
| `plan.json` | Table names, query SQL, chunk bounds, row estimates |
| `.rivet_state.db` | Cursor values, manifest of exported files, run metrics |
| `*.jsonl` journal | Per-run event timeline; includes export names, run IDs, chunk boundaries |
| Parquet / CSV outputs | The actual exported data |
| Log output (stdout / `--log-format json`) | Query SQL (truncated), table names, row counts; redacted URLs |

### `.gitignore` recommendations

If you use a Rivet config inside a git repository, exclude generated and state artifacts:

```gitignore
# Rivet local state and run artifacts
.rivet_state.db*
*.rivet.local.yaml
*.rivet.secrets.yaml
plan.json
*.journal.jsonl
output/
```

The Rivet repository itself already excludes its own `output/` and `*_state.db*` artifacts — see [`.gitignore`](.gitignore).

---

## Credential handling

### Preferred: environment variables

All connection strings and destination credentials support an `_env` indirection. The plaintext secret never enters a YAML file that you would commit:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL      # value read from env at runtime
  tls:
    mode: verify-full
    ca_file: /etc/ssl/certs/rds-ca-2019-root.pem

destinations:
  - name: warehouse
    type: s3
    bucket: my-exports
    access_key_env: AWS_ACCESS_KEY_ID
    secret_key_env: AWS_SECRET_ACCESS_KEY
```

Inline `password:` / `url:` / `access_key:` fields are accepted but **not recommended** for any file that ships outside the operator's machine.

### Redaction in errors and artifacts

Credential redaction is split across two layers (v0.7.2 P0.3):

- **Structural redaction** — `SourceConfig::redact_for_artifact` strips
  plaintext `password` and `user:password@` userinfo from a
  `SourceConfig` *before* it lands in a `PlanArtifact` (`plan.json`).
  Env-var and file references (`url_env`, `password_env`, `url_file`,
  `credentials_file`) are preserved by name — names are not secrets,
  and `apply` needs them to re-resolve credentials.
- **String redaction** — `crate::redact::redact_secrets` /
  `redact_error` walks a string and rewrites every
  `scheme://user:password@host` URL it finds to
  `scheme://REDACTED@host`.  Applied as an *invariant* at every
  boundary where an `anyhow::Error` becomes an operator-visible
  artifact:
  - `RunSummary::error_message` (→ `summary.json` / `summary.md` /
    Slack / webhook payloads),
  - run-journal `RetryAttempted::reason` and `ChunkFailed::error`
    (→ `.rivet_state.db`, `rivet journal`),
  - hard-failure log lines (`log::error!` / `log::warn!` in
    pipeline/job, pipeline/single, chunked/sequential and
    parallel checkpoints, repair_cmd),
  - top-level CLI error output (`main.rs` `eprintln!`),
  - `rivet validate` hard-failure messages (`could not open destination`
    / `verify_at_destination failed`).

Pinned in tests:

- [`tests/config_secrets.rs`](tests/config_secrets.rs) — config-parse
  errors never echo plaintext.
- [`tests/redaction_invariant.rs`](tests/redaction_invariant.rs) —
  string-redactor unit + integration suite; pins the URL-rewrite
  rule and proves the redactor is wired into every error → artifact
  path enumerated above.

If you observe a plaintext credential in any Rivet-produced output,
treat it as a security bug (see Reporting below).

#### Known limitations

- **User-authored SQL** — `query:` strings are stored verbatim in
  `plan.json` and the run journal.  Do not embed credentials inside SQL;
  use database-side roles or parameterised drivers instead.
- **Third-party driver output** — Rivet redacts the strings *it*
  produces.  If a Postgres / MySQL driver emits a URL-shaped error
  that bypasses our error wrappers (e.g. via direct `stderr`), the
  redactor will not see it.  Run with `RUST_LOG=info` to see all
  Rivet-emitted log lines; anything that lands outside that channel
  is upstream.
- **Process memory** — passwords are wrapped in `Zeroizing<String>` at
  the source-config boundary, but once a URL has been assembled the
  combined string is a plain `String` until the connection is opened.
  Memory introspection of a running Rivet process is out of scope.
- **Shapes outside `scheme://user:password@host`** — AWS access keys
  and Azure account keys are pulled from env vars at runtime and never
  flow through our string formatters.  If a leak vector is discovered,
  add a pattern to `src/redact.rs`, pin it in
  `tests/redaction_invariant.rs`, and ship a patch release.

---

## Network security

- **PostgreSQL TLS** — Rivet supports `disable | require | verify-ca | verify-full` via the `tls` block on the source. `verify-full` is recommended for any non-local target. See [docs/reference/config.md](docs/reference/config.md#tls).
- **MySQL TLS** — supported through the underlying `mysql` crate; configure via the connection URL (`?ssl-mode=REQUIRED`).
- **Object storage** — S3 and GCS endpoints use HTTPS by default. The CI/dev fixtures use plain HTTP against `minio` and `fake-gcs` containers; do not point a production config at those URLs.

For production exports against shared / managed databases:

1. Use a **dedicated read-only user** scoped to the tables you export.
2. Route through a **read replica** when one exists.
3. Set a **statement timeout** on the source-side role — and verify it survives any connection pooler in front of the database (transaction-mode pgBouncer / ProxySQL drop session-scoped settings between statements; Rivet warns at startup when it detects this — see [docs/pilot/production-checklist.md § Connection poolers and proxies](docs/pilot/production-checklist.md#connection-poolers-and-proxies)).
4. Set TLS to `verify-full` with a pinned CA file.

The pilot guide covers this end-to-end: [docs/pilot/production-checklist.md](docs/pilot/production-checklist.md).

---

## Supply chain

| Control | Status | Notes |
|---|---|---|
| RustSec advisory audit | **Active** | `audit` job in [.github/workflows/ci.yml](.github/workflows/ci.yml) runs `rustsec/audit-check` on every PR |
| Dependency review | **Active** | Cargo.lock is committed; bumps land in dedicated PRs |
| Release checksums (`SHA256SUMS`) | **Active** | Every release publishes `SHA256SUMS.txt` as an asset ([`.github/workflows/release.yml`](.github/workflows/release.yml)); verification below |
| Signed releases (cosign keyless) | **Active** | Every release signs `SHA256SUMS.txt` via Sigstore/cosign keyless (GitHub OIDC — no key to manage), published as `SHA256SUMS.txt.cosign.bundle`; verification below |
| SBOM | **Active** | Every release publishes an SPDX SBOM (`rivet-<tag>.spdx.json`, generated by syft) of the source + Cargo dependency graph; its hash rides in the signed `SHA256SUMS.txt`, so the cosign signature covers it too |

**Verify a downloaded release.** Confirm provenance first (the checksums were
produced by *this repo's* release workflow), then integrity:

```bash
# 1. Provenance — verify the keyless signature on SHA256SUMS.txt.
#    Needs cosign: https://docs.sigstore.dev/cosign/installation
cosign verify-blob \
  --bundle SHA256SUMS.txt.cosign.bundle \
  --certificate-identity-regexp '^https://github\.com/.+/rivet/\.github/workflows/release\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS.txt

# 2. Integrity — verify the tarball(s) match the now-trusted checksums.
sha256sum -c SHA256SUMS.txt        # Linux
shasum -a 256 -c SHA256SUMS.txt    # macOS
```

Step 1 proves the checksums were signed by GitHub's OIDC identity for this repo's
`release.yml` (recorded in Sigstore's public transparency log — no key to fetch or
trust on first use); step 2 ties your downloaded bytes to those signed checksums.
You can still rebuild from source at the tagged commit (`cargo build --release`
after `git checkout <tag>`) as an independent cross-check.

---

## Reporting a vulnerability

Please report suspected vulnerabilities through GitHub's private advisory channel:

- **Preferred:** open a draft Security Advisory at https://github.com/panchenkoai/rivet/security/advisories/new
- Alternative: email the maintainer listed in `Cargo.toml` with subject `rivet-security:`.

We aim to acknowledge reports within **7 days** and to ship a fix or mitigation within **30 days** for confirmed issues affecting a supported release.

### Supported versions

Only the latest minor release line (currently `0.5.x`) receives security fixes. Older lines may be patched on a best-effort basis when the fix is trivial to backport.

---

## Scope

In scope:

- Credential leakage in CLI output, logs, error messages, plan/journal/metrics artifacts.
- Crash-recovery bugs that lose data or silently advance the cursor past unwritten rows.
- Path traversal or symlink issues in destination writes.
- TLS / certificate validation bypasses.
- Vulnerabilities in declared direct dependencies that Rivet's code path actually reaches.

Out of scope (please do not file as security issues):

- User-authored SQL injecting into the user's own database.
- Misconfiguration that grants Rivet excessive privileges on the source.
- Performance issues without a security dimension.
- Vulnerabilities in upstream databases (PostgreSQL, MySQL) themselves.
