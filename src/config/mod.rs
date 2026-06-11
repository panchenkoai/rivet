pub mod cursor;
mod destination;
mod export;
mod format;
mod lints;
mod notifications;
pub mod resolve;
pub mod schema;
mod source;

pub use cursor::IncrementalCursorMode;
pub use destination::*;
pub use export::*;
pub use format::*;
pub use notifications::*;
#[allow(unused_imports)]
pub(crate) use resolve::resolve_env_vars;
pub use resolve::{parse_file_size, resolve_vars};
pub use schema::generate_config_schema_pretty;
pub use source::*;

use schemars::JsonSchema;
use serde::Deserialize;

/// Top-level Rivet configuration root.
///
/// Operators write this struct as YAML (typically `rivet.yaml`).  The
/// `JsonSchema` derive is the source of truth for the `schemas/rivet.schema.json`
/// artifact and the `rivet schema config` command's output (v0.7.3 P0).
#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub source: SourceConfig,
    pub exports: Vec<ExportConfig>,
    #[serde(default)]
    pub notifications: Option<NotificationsConfig>,
    #[serde(default)]
    pub parallel_exports: bool,
    #[serde(default)]
    pub parallel_export_processes: bool,
}

impl Config {
    pub fn load(path: &str) -> crate::error::Result<Self> {
        Self::load_with_params(path, None)
    }

    pub fn load_with_params(
        path: &str,
        params: Option<&std::collections::HashMap<String, String>>,
    ) -> crate::error::Result<Self> {
        // F11 (0.7.5 audit): raw `std::io::Error` lost the path on
        // not-found.  Wrap with the file path + a hint so the operator
        // can see *which* config the tool could not open.
        let contents = std::fs::read_to_string(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                anyhow::anyhow!(
                    "config file '{}' not found.\n  Hint: check the path, or run `rivet init` to generate one.",
                    path
                )
            } else {
                anyhow::anyhow!("cannot read config file '{}': {}", path, e)
            }
        })?;
        // Warn about typo'd `--param` keys once per CLI invocation, using the
        // un-resolved YAML as the haystack so the placeholders are still there.
        // We pass the raw `contents` (not `resolved`) on purpose: after
        // resolution the placeholders are gone, and every key would look unused.
        resolve::warn_unused_params(&contents, params);
        let resolved = resolve_vars(&contents, params)?;
        // F12 (0.7.5 audit): YAML parse errors did not name the config
        // file.  When loading from disk we know the path — thread it
        // into the parse error.
        Self::from_yaml(&resolved).map_err(|e| anyhow::anyhow!("config file '{}': {:#}", path, e))
    }

    pub fn from_yaml(yaml: &str) -> crate::error::Result<Self> {
        // Intercept the unquoted-`{partition}` footgun before the raw-YAML
        // pre-scans below (they `from_str::<Value>` too and would re-emit the
        // same cryptic libyaml message). A document that does not even parse as
        // a `Value` is malformed; if the failure looks like a flow-mapping
        // scanner error *and* the source carries an unquoted brace value, point
        // straight at the quoting fix. On a valid config this `Value` parse
        // succeeds and the block is skipped, so the success path is unchanged.
        if let Err(e) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(yaml)
            && let Some(hint) = Self::unquoted_template_brace_hint(yaml, &e.to_string())
        {
            return Err(anyhow::anyhow!("{e}\n  {hint}"));
        }
        Self::check_misplaced_tuning_fields(yaml)?;
        Self::check_csv_compression(yaml)?;
        Self::check_tls_mode_downgrade(yaml)?;
        let config: Config = serde_yaml_ng::from_str(yaml).map_err(|e| {
            // A well-formed flow map (`prefix: {partition}`) parses as a YAML
            // value but serde then rejects it with `invalid type: map, expected
            // a string`. That is the same unquoted-brace footgun, surfacing one
            // layer later than the scanner errors caught above — so try the same
            // hint here before falling back to the generic field-typo enhancer.
            if let Some(hint) = Self::unquoted_template_brace_hint(yaml, &e.to_string()) {
                anyhow::anyhow!("{e}\n  {hint}")
            } else {
                lints::enhance_parse_error(e)
            }
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Detect the unquoted-`{partition}` (or `{date}`, …) template footgun and
    /// return an actionable quoting hint, or `None` when the error is unrelated.
    ///
    /// A YAML value that *starts* a flow mapping — `prefix: {partition}` — is
    /// the common copy-paste mistake: `{partition}` is the required token for
    /// `partition_by`, but unquoted it parses as a YAML map (or, with trailing
    /// text, trips the libyaml scanner). serde then emits a cryptic
    /// `did not find expected ',' or '}'` / `while parsing a flow mapping` /
    /// `invalid type: map, expected a string` with no hint that a pair of
    /// quotes is the fix.
    ///
    /// Two guards keep this from firing on unrelated parse errors: the error
    /// message must carry one of the flow-mapping symptoms, AND the raw source
    /// must actually contain an unquoted `{…}` value. Both must hold, so a
    /// valid config (every brace value quoted) never sees the hint, and a
    /// genuine map-typed field error elsewhere is left alone.
    fn unquoted_template_brace_hint(yaml: &str, err_msg: &str) -> Option<String> {
        const FLOW_SYMPTOMS: &[&str] = &[
            "did not find expected ',' or '}'",
            "while parsing a flow mapping",
            // A bare `key: {token}` parses as a map, then serde rejects the
            // map where it wanted a scalar — same root cause, later layer.
            "invalid type: map, expected a string",
            // `key: {token}/more` runs the flow map into block context.
            "did not find expected key",
        ];
        if !FLOW_SYMPTOMS.iter().any(|s| err_msg.contains(s)) {
            return None;
        }
        if !yaml.lines().any(line_has_unquoted_brace_value) {
            return None;
        }
        Some(
            "a YAML value containing { } (such as {partition} or {date}) must be quoted, \
             e.g. prefix: \"exports/{partition}/\""
                .to_string(),
        )
    }

    /// Reject `format: csv` paired with an explicitly-requested compression
    /// codec (Finding #10). The CSV writer has no compression encoder, so the
    /// codec is silently dropped on write while the run manifest still records
    /// it — a degraded, dishonest no-op. We reject loudly at config-validate
    /// time so `rivet check` / `rivet doctor` catch it before any run.
    ///
    /// This is a raw-YAML scan (like [`Self::check_misplaced_tuning_fields`])
    /// rather than a `validate_export` check on purpose: `ExportConfig.
    /// compression` is `#[serde(default)]` and `CompressionType::default()` is
    /// `Zstd`, so a parsed export cannot distinguish "user asked for zstd" from
    /// "user omitted the field". Only a user who *wrote* `compression:`/
    /// `compression_profile:` is asking for something the CSV writer cannot
    /// honour; the bare-`format: csv` default writes uncompressed and is fine.
    fn check_csv_compression(yaml: &str) -> crate::error::Result<()> {
        let root: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)?;
        let Some(exports) = root.get("exports").and_then(|e| e.as_sequence()) else {
            return Ok(());
        };
        for export in exports {
            if export.get("format").and_then(|f| f.as_str()) != Some("csv") {
                continue;
            }
            let name = export
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("<unnamed>");

            // Explicit `compression:` codec that the CSV writer cannot apply.
            // An unrecognised label is left for serde to reject during the real
            // parse; we only act on a codec we understand and that CSV cannot
            // honour (everything but `none`).
            if let Some(codec) = export.get("compression").and_then(|c| c.as_str())
                && let Some(ct) = CompressionType::from_label(codec)
                && !format::compression_supported(FormatType::Csv, ct)
            {
                anyhow::bail!(
                    "export '{}': CSV output does not support compression: {}. \
                     CSV has no compression encoder, so the codec would be silently dropped \
                     while the manifest records it.\n  \
                     Hint: use `format: parquet` for compression, or set `compression: none`.",
                    name,
                    codec,
                );
            }

            // A `compression_profile:` other than `none` resolves to a real
            // codec too (fast→snappy, balanced/compact→zstd) — same no-op.
            if let Some(profile) = export.get("compression_profile").and_then(|c| c.as_str())
                && profile != CompressionProfile::None.label()
            {
                anyhow::bail!(
                    "export '{}': CSV output does not support compression_profile: {} \
                     (it resolves to a compression codec the CSV writer cannot apply).\n  \
                     Hint: use `format: parquet` for compression, or set `compression_profile: none`.",
                    name,
                    profile,
                );
            }
        }
        Ok(())
    }

    /// V13: reject a `source.tls` block that pairs an *explicitly chosen*
    /// enforced `mode:` with a verification-disabling danger knob
    /// (`accept_invalid_certs` / `accept_invalid_hostnames`). `mode: verify-full`
    /// promises chain + hostname verification, but the knob silently downgrades
    /// it to "trust anything" — a MITM exposure that contradicts the stated
    /// intent (see `src/source/tls.rs::build_native_tls`, whose comment claims
    /// this is warned about at config-time but is not).
    ///
    /// Like [`Self::check_csv_compression`], this is a raw-YAML scan rather than
    /// a `validate` check on purpose: `TlsMode` is `#[serde(default)]` and the
    /// default is `VerifyFull`, so a parsed config cannot distinguish "user
    /// wrote `mode: verify-full`" (a contradiction to flag) from "user omitted
    /// `mode:`" (the common dev-container case `tls: { accept_invalid_certs:
    /// true }` against a loopback self-signed cert — which must keep working).
    /// Only an *explicit* enforced `mode:` next to a danger knob is the footgun.
    fn check_tls_mode_downgrade(yaml: &str) -> crate::error::Result<()> {
        let root: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)?;
        let Some(tls) = root.get("source").and_then(|s| s.get("tls")) else {
            return Ok(());
        };

        // Only an explicitly written `mode:` is a deliberate, contradicted
        // choice; an omitted mode is the dev-container default path.
        let Some(mode) = tls.get("mode").and_then(|m| m.as_str()) else {
            return Ok(());
        };
        // `disable` carries no verification promise to contradict; the danger
        // knobs are a no-op there. Flag only the enforced modes.
        if mode == "disable" {
            return Ok(());
        }

        let knob = if tls
            .get("accept_invalid_certs")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            Some("accept_invalid_certs")
        } else if tls
            .get("accept_invalid_hostnames")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            Some("accept_invalid_hostnames")
        } else {
            None
        };

        if let Some(knob) = knob {
            anyhow::bail!(
                "source.tls: {} disables certificate verification, silently downgrading the \
                 chosen `mode: {}` to trust-anything (MITM exposure — credentials and rows \
                 readable/forgeable on the wire).\n  \
                 Hint: drop the danger knob and trust a private CA with `tls.ca_file: <pem>`; \
                 only use a danger knob for a loopback self-signed dev container, and then omit \
                 the explicit `mode:` so the contradiction is gone.",
                knob,
                mode,
            );
        }
        Ok(())
    }

    /// Detect tuning-related fields placed directly under `source:` or an
    /// `exports[]` entry instead of inside the `tuning:` sub-key. Without this
    /// check serde silently ignores unknown keys and the user gets unexpected
    /// defaults (e.g. batch_size=10 000 instead of the intended 1 000).
    fn check_misplaced_tuning_fields(yaml: &str) -> crate::error::Result<()> {
        const TUNING_FIELDS: &[&str] = &[
            "batch_size",
            "batch_size_memory_mb",
            "throttle_ms",
            "statement_timeout_s",
            "max_retries",
            "retry_backoff_ms",
            "lock_timeout_s",
            "memory_threshold_mb",
            "profile",
        ];

        let root: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)?;

        if let Some(source) = root.get("source") {
            let misplaced: Vec<&str> = TUNING_FIELDS
                .iter()
                .copied()
                .filter(|&f| source.get(f).is_some())
                .collect();
            if !misplaced.is_empty() {
                anyhow::bail!(
                    "source: field(s) [{}] belong under 'source.tuning:', not directly under 'source:'. \
                     Example:\n  source:\n    tuning:\n      {}: <value>",
                    misplaced.join(", "),
                    misplaced[0],
                );
            }
        }

        if let Some(exports) = root.get("exports").and_then(|e| e.as_sequence()) {
            for (i, export) in exports.iter().enumerate() {
                let name = export
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("<unnamed>");
                let misplaced: Vec<&str> = TUNING_FIELDS
                    .iter()
                    .copied()
                    .filter(|&f| export.get(f).is_some())
                    .collect();
                if !misplaced.is_empty() {
                    anyhow::bail!(
                        "export '{}' (index {}): field(s) [{}] belong under 'exports[].tuning:', \
                         not directly in the export. Example:\n  exports:\n    - name: {}\n      tuning:\n        {}: <value>",
                        name,
                        i,
                        misplaced.join(", "),
                        name,
                        misplaced[0],
                    );
                }
            }
        }

        Ok(())
    }

    /// Reject a config before any plan/connect step. The body is split into
    /// three cohesive validators so each can be read — and unit-tested — on its
    /// own: the export-list shape, the source connection block, and the
    /// per-export rules. The end-to-end surface (`Config::from_yaml`) is
    /// covered by `config/tests/{validation,secops}.rs`; the split additionally
    /// lets a rule be exercised directly via `validate_export`.
    fn validate(&self) -> crate::error::Result<()> {
        self.validate_exports_list()?;
        self.validate_source_connection()?;
        for export in &self.exports {
            self.validate_export(export)?;
        }
        Ok(())
    }

    /// Whole-config shape: at least one export, names unique.
    fn validate_exports_list(&self) -> crate::error::Result<()> {
        // An empty `exports:` list is almost always a typo (wrong config file,
        // dropped anchor, merged doc with the anchor section missing). Running
        // with zero exports is a silent no-op that looks like success in CI;
        // reject fast instead. See QA backlog Task 5.1.
        if self.exports.is_empty() {
            anyhow::bail!("exports: at least one export must be defined (got empty list)");
        }

        // Duplicate export names break state tracking: `export_state`,
        // `file_log`, and `chunk_run` are all keyed by `export_name`, so
        // two configs with the same name silently share cursor/file-log rows.
        // QA backlog Task 5.1.
        let mut seen: std::collections::HashSet<&str> =
            std::collections::HashSet::with_capacity(self.exports.len());
        for e in &self.exports {
            if !seen.insert(e.name.as_str()) {
                anyhow::bail!(
                    "exports: duplicate export name '{}' (each export must have a unique name; state is keyed by name)",
                    e.name
                );
            }
        }
        Ok(())
    }

    /// Source connection block: exactly one connection method, well-formed,
    /// and the source-level tuning that is shared by every export.
    fn validate_source_connection(&self) -> crate::error::Result<()> {
        if let Some(t) = &self.source.tuning
            && t.batch_size.is_some()
            && t.batch_size_memory_mb.is_some()
        {
            anyhow::bail!(
                "tuning: batch_size and batch_size_memory_mb are mutually exclusive. \
                 Prefer batch_size_memory_mb (rivet sizes the batch to a memory budget, \
                 adapting to row width); set batch_size only to pin an exact row count."
            );
        }

        if !self.source.has_url_fields() && !self.source.has_structured_fields() {
            // First-run footgun: a config that forgot the source block
            // entirely.  Show the recommended path (`url_env`) up-front;
            // operators who actually want structured fields know to look
            // for them.
            anyhow::bail!(
                "source: no connection method configured. Add one of:\n  url_env: DATABASE_URL                          (URL from env var — recommended)\n  url: 'postgresql://user:pass@host:5432/db'      (inline — not recommended for committed configs)\n  url_file: /etc/rivet/source.url                 (URL from file — rotation-friendly)\n  host/user/database/...                          (structured fields under `source:`)"
            );
        }

        if self.source.has_url_fields() {
            let url_count = [
                &self.source.url,
                &self.source.url_env,
                &self.source.url_file,
            ]
            .iter()
            .filter(|u| u.is_some())
            .count();
            if url_count > 1 {
                anyhow::bail!(
                    "source: specify exactly one of 'url', 'url_env', or 'url_file' (got {} set).\n  Hint: pick one — `url_env` is recommended so credentials never enter the YAML.",
                    url_count
                );
            }
        }

        if self.source.has_url_fields() && self.source.has_structured_fields() {
            anyhow::bail!(
                "source: pick either URL-based config (url/url_env/url_file) OR structured fields (host/user/database/port/password_env), not both.\n  Hint: remove whichever block you don't want; mixing the two is ambiguous."
            );
        }

        if self.source.has_structured_fields() {
            if self.source.host.is_none() {
                anyhow::bail!(
                    "source: structured config is missing 'host'.\n  Hint: add `host: localhost` (or your DB host) under `source:` in rivet.yaml.\n  Or switch to URL-based config: `url_env: DATABASE_URL`."
                );
            }
            if self.source.user.is_none() {
                anyhow::bail!(
                    "source: structured config is missing 'user'.\n  Hint: add `user: <username>` under `source:` in rivet.yaml."
                );
            }
            if self.source.database.is_none() {
                anyhow::bail!(
                    "source: structured config is missing 'database'.\n  Hint: add `database: <dbname>` under `source:` in rivet.yaml."
                );
            }
            if self.source.password.is_some() && self.source.password_env.is_some() {
                anyhow::bail!(
                    "source: specify 'password' OR 'password_env', not both.\n  Hint: prefer `password_env: DB_PASSWORD` so credentials never enter the YAML."
                );
            }
        }
        Ok(())
    }

    /// Per-export rules: effective tuning, query source, `query_file` SecOps,
    /// destination auth, compression, and the mode/chunk matrix. Takes `&self`
    /// because effective tuning merges the source-level block.
    fn validate_export(&self, export: &ExportConfig) -> crate::error::Result<()> {
        // V5: `name` is keyed into output paths, file logs, and on-disk state,
        // yet is otherwise free-form. A traversal (`../../etc/x`), absolute or
        // slash-bearing (`/abs/x`, `sub/dir`), leading-dot, or NUL-bearing name
        // escapes the intended output tree and corrupts name-keyed state.
        // Mirror the `query_file` `..`/absolute guard: reject at config-load,
        // accepting only a filename-safe charset.
        if !is_filename_safe_name(&export.name) {
            anyhow::bail!(
                "export name '{}' is not filename-safe: it must not be absolute, contain \
                 '/', '\\', '..', a NUL, or start with '.' (the name is used in output paths \
                 and state keys). Use a plain identifier like `orders` or `daily_events`.",
                export.name.escape_default(),
            );
        }

        let merged =
            crate::tuning::merge_tuning_config(self.source.tuning.as_ref(), export.tuning.as_ref());
        if let Some(t) = merged
            && t.batch_size.is_some()
            && t.batch_size_memory_mb.is_some()
        {
            anyhow::bail!(
                "export '{}': effective tuning has both batch_size and batch_size_memory_mb (mutually exclusive)",
                export.name
            );
        }
        if let Some(et) = &export.tuning
            && et.batch_size.is_some()
            && et.batch_size_memory_mb.is_some()
        {
            anyhow::bail!(
                "export '{}': tuning.batch_size and tuning.batch_size_memory_mb are mutually exclusive",
                export.name
            );
        }

        let set_count = [
            export.query.is_some(),
            export.query_file.is_some(),
            export.table.is_some(),
        ]
        .iter()
        .filter(|b| **b)
        .count();
        if set_count == 0 {
            anyhow::bail!(
                "export '{}': specify exactly one of 'query', 'query_file', or 'table'. \
                 Use table: <name> for a whole table (enables PK auto-chunking); \
                 query: \"SELECT …\" for an inline one-liner; \
                 query_file: <path> for SQL you keep in version control.",
                export.name
            );
        }
        if set_count > 1 {
            anyhow::bail!(
                "export '{}': specify exactly one of 'query', 'query_file', or 'table' (got {} set)",
                export.name,
                set_count
            );
        }
        // SecOps: syntactic `query_file` checks must run at config-validate
        // time so `rivet check` / `rivet doctor` catch them before any
        // plan step. The same checks repeat (with a canonicalize-based
        // symlink probe) in `ExportConfig::resolve_query` because the
        // file may have been swapped between validation and read.
        if let Some(file) = &export.query_file {
            let p = std::path::Path::new(file);
            if p.is_absolute() {
                anyhow::bail!(
                    "export '{}': query_file must be a relative path: '{}'",
                    export.name,
                    file
                );
            }
            if p.components().any(|c| c == std::path::Component::ParentDir) {
                anyhow::bail!(
                    "export '{}': query_file path must not contain '..': '{}'",
                    export.name,
                    file
                );
            }
        }
        // V2/V12: a custom cloud `endpoint` is handed straight to the opendal
        // S3/GCS/Azure builder with no validation, so a committed config can
        // silently redirect every upload to an attacker host (exfiltration) or
        // send credentials + rows over cleartext `http://`. The legitimate use
        // is a local emulator (Minio / Azurite / fake-gcs on `127.0.0.1`), so
        // accept a loopback host (any scheme), and otherwise accept a remote
        // endpoint only when the operator has explicitly opted into anonymous
        // (emulator) mode. Reject every other custom endpoint at config-load.
        if matches!(
            export.destination.destination_type,
            DestinationType::S3 | DestinationType::Gcs | DestinationType::Azure
        ) && let Some(endpoint) = &export.destination.endpoint
        {
            // Loopback emulator (Minio/Azurite/fake-gcs) is the legitimate
            // local-dev path — accept any scheme. A non-loopback (or
            // unparseable) custom endpoint is only accepted when the operator
            // has explicitly opted into anonymous (emulator) mode, where no
            // credentials are sent. Everything else is rejected.
            let loopback = endpoint_host(endpoint).is_some_and(|host| is_loopback_host(&host));
            if !loopback && !export.destination.allow_anonymous {
                anyhow::bail!(
                    "export '{}': destination.endpoint '{}' points at a non-loopback host. \
                     A custom endpoint redirects every upload there — committing one is a \
                     data-exfiltration / cleartext-credential risk.\n  \
                     Hint: drop `endpoint:` to use the provider default, point it at a \
                     loopback emulator (e.g. http://127.0.0.1:9000 with allow_anonymous: true \
                     for Minio/Azurite), or set `allow_anonymous: true` for an anonymous \
                     emulator.",
                    export.name,
                    endpoint,
                );
            }
        }

        // V15: a `type: local` destination `path` (or `prefix`) is written
        // verbatim to the filesystem. A `..` component lets a committed config
        // climb out of the intended output tree (`../../../../tmp/x`) — mirror
        // the `query_file` traversal guard and reject it at config-load.
        //
        // Absolute paths are deliberately *not* rejected: `path: /output` is a
        // legitimate Docker volume-mount pattern (see `examples/rivet.yaml`) and
        // an explicit operator choice, not a hidden escape. The `..` climb is
        // the unambiguous traversal footgun.
        if export.destination.destination_type == DestinationType::Local {
            for (field, value) in [
                ("path", export.destination.path.as_deref()),
                ("prefix", export.destination.prefix.as_deref()),
            ] {
                let Some(value) = value else { continue };
                if std::path::Path::new(value)
                    .components()
                    .any(|c| c == std::path::Component::ParentDir)
                {
                    anyhow::bail!(
                        "export '{}': local destination {} must not contain a '..' component: \
                         '{}' (a parent-dir climb writes outside the output tree).",
                        export.name,
                        field,
                        value
                    );
                }
            }
        }

        if export.destination.destination_type == DestinationType::S3 {
            let ak = export.destination.access_key_env.is_some();
            let sk = export.destination.secret_key_env.is_some();
            if ak != sk {
                anyhow::bail!(
                    "export '{}': S3 requires both access_key_env and secret_key_env, or neither (use default AWS credential chain)",
                    export.name
                );
            }
        }

        if export.destination.destination_type == DestinationType::Gcs
            && export.destination.allow_anonymous
            && export.destination.credentials_file.is_some()
        {
            anyhow::bail!(
                "export '{}': GCS allow_anonymous cannot be used together with credentials_file",
                export.name
            );
        }

        if export.destination.destination_type == DestinationType::Azure {
            let has_name = export.destination.account_name.is_some();
            let has_key = export.destination.account_key_env.is_some();
            let has_sas = export.destination.sas_token_env.is_some();
            if export.destination.allow_anonymous {
                if has_name || has_key || has_sas {
                    anyhow::bail!(
                        "export '{}': Azure allow_anonymous cannot be combined with account_name/account_key_env/sas_token_env",
                        export.name
                    );
                }
            } else if has_key && has_sas {
                anyhow::bail!(
                    "export '{}': Azure account_key_env and sas_token_env are mutually exclusive — pick one auth mode",
                    export.name
                );
            } else if !has_name {
                anyhow::bail!(
                    "export '{}': Azure requires account_name (plus account_key_env or sas_token_env), or allow_anonymous: true for Azurite",
                    export.name
                );
            } else if !has_key && !has_sas {
                anyhow::bail!(
                    "export '{}': Azure requires account_key_env or sas_token_env (or allow_anonymous: true for Azurite)",
                    export.name
                );
            }
        }

        if let Some(cred_path) = &export.destination.credentials_file
            && !std::path::Path::new(cred_path).exists()
        {
            anyhow::bail!(
                "export '{}': credentials_file '{}' does not exist",
                export.name,
                cred_path
            );
        }

        if let Some(ref size_str) = export.max_file_size {
            parse_file_size(size_str).map_err(|_| {
                anyhow::anyhow!(
                    "export '{}': invalid max_file_size '{}'",
                    export.name,
                    size_str
                )
            })?;
        }

        if let Some(level) = export.compression_level {
            match export.compression {
                CompressionType::Zstd => {
                    if !(1..=22).contains(&level) {
                        anyhow::bail!(
                            "export '{}': zstd compression_level must be 1..22, got {}",
                            export.name,
                            level
                        );
                    }
                }
                CompressionType::Gzip => {
                    if level > 10 {
                        anyhow::bail!(
                            "export '{}': gzip compression_level must be 0..10, got {}",
                            export.name,
                            level
                        );
                    }
                }
                _ => {
                    anyhow::bail!(
                        "export '{}': compression_level is only supported for zstd and gzip",
                        export.name
                    );
                }
            }
        }

        match export.mode {
            ExportMode::Incremental => {
                if export.cursor_column.is_none() {
                    anyhow::bail!(
                        "export '{}': incremental mode requires cursor_column",
                        export.name
                    );
                }
                match export.incremental_cursor_mode {
                    IncrementalCursorMode::Coalesce => {
                        if export.cursor_fallback_column.is_none() {
                            anyhow::bail!(
                                "export '{}': incremental_cursor_mode: coalesce requires cursor_fallback_column",
                                export.name
                            );
                        }
                    }
                    IncrementalCursorMode::SingleColumn => {
                        if export.cursor_fallback_column.is_some() {
                            anyhow::bail!(
                                "export '{}': cursor_fallback_column is only valid with incremental_cursor_mode: coalesce",
                                export.name
                            );
                        }
                    }
                }
            }
            ExportMode::Chunked => {
                // `chunk_column` is mandatory unless the user used the `table:`
                // shortcut on a Postgres source — in that case it is auto-resolved
                // from the table's single-integer PK at plan-build time (see
                // `crate::plan::build::resolve_chunk_column`).
                if export.chunk_column.is_none() && export.table.is_none() {
                    anyhow::bail!(
                        "export '{}': chunked mode needs a chunking strategy. Pick one:\n  \
                         chunk_column: <int col>    range chunks on an integer column (most common)\n  \
                         chunk_by_key: <unique col>  keyset pagination when there's no integer PK\n  \
                         chunk_count: <N>            split the range into N equal chunks\n  \
                         chunk_by_days: <D>          time-bucketed chunks (needs a date/timestamp column)\n  \
                         Or use the `table:` shortcut on a single table — rivet auto-resolves the column from the primary key.",
                        export.name
                    );
                }
                // chunk_size == 0 would divide the range into zero-width
                // slices and (before the saturating fix in generate_chunks)
                // either infinite-loop or produce no progress. QA backlog
                // Task 5.1.
                if export.chunk_size == 0 {
                    anyhow::bail!(
                        "export '{}': chunked mode requires chunk_size >= 1 (got 0)",
                        export.name
                    );
                }
                // parallel == 0 means "spawn zero workers". Claiming tasks
                // with no workers stalls the pipeline. QA backlog Task 5.1.
                if export.parallel == 0 {
                    anyhow::bail!(
                        "export '{}': chunked mode requires parallel >= 1 (got 0)",
                        export.name
                    );
                }
                if let Some(0) = export.chunk_count {
                    anyhow::bail!("export '{}': chunk_count must be >= 1", export.name);
                }
                if export.chunk_count.is_some() && export.chunk_dense {
                    anyhow::bail!(
                        "export '{}': chunk_count and chunk_dense are mutually exclusive. \
                         Use chunk_count for equal-sized chunks over a sparse key; \
                         use chunk_dense only when the key has no gaps.",
                        export.name
                    );
                }
                if export.chunk_count.is_some() && export.chunk_by_days.is_some() {
                    anyhow::bail!(
                        "export '{}': chunk_count and chunk_by_days are mutually exclusive. \
                         Use chunk_count: N to split an integer range into N chunks; \
                         use chunk_by_days: D to bucket a date/timestamp column by D-day windows.",
                        export.name
                    );
                }
            }
            ExportMode::TimeWindow => {
                if export.time_column.is_none() {
                    anyhow::bail!(
                        "export '{}': time_window mode requires time_column",
                        export.name
                    );
                }
                if export.days_window.is_none() {
                    anyhow::bail!(
                        "export '{}': time_window mode requires days_window",
                        export.name
                    );
                }
            }
            ExportMode::Full => {}
        }

        if export.chunk_dense && export.mode != ExportMode::Chunked {
            anyhow::bail!(
                "export '{}': chunk_dense is only valid with mode: chunked",
                export.name
            );
        }

        if let Some(days) = export.chunk_by_days {
            if export.mode != ExportMode::Chunked {
                anyhow::bail!(
                    "export '{}': chunk_by_days requires mode: chunked",
                    export.name
                );
            }
            if export.chunk_dense {
                anyhow::bail!(
                    "export '{}': chunk_by_days cannot be combined with chunk_dense",
                    export.name
                );
            }
            if days == 0 {
                anyhow::bail!("export '{}': chunk_by_days must be at least 1", export.name);
            }
        }
        Ok(())
    }
}

/// True when a single YAML line carries a mapping value (text after `key:`)
/// that contains a `{` outside of any quotes — the unquoted-template-brace
/// shape (`prefix: {partition}`, `path: {date}/out`).
///
/// Quote-aware so a properly quoted value (`prefix: "exports/{partition}/"`)
/// does *not* match, and `$`-prefixed braces (`${VAR}` env placeholders) are
/// ignored — they are resolved before the parse and are not the footgun.
fn line_has_unquoted_brace_value(line: &str) -> bool {
    // Whole-line comments never carry a value — skip before splitting.
    if line.trim_start().starts_with('#') {
        return false;
    }
    // Split key from value at the first `": "` / `":\t"` / trailing `:`.
    // A YAML plain-key separator is a colon followed by whitespace or EOL.
    let bytes = line.as_bytes();
    let mut sep = None;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b':' && (i + 1 == bytes.len() || bytes[i + 1].is_ascii_whitespace()) {
            sep = Some(i + 1);
            break;
        }
        i += 1;
    }
    let Some(value_start) = sep else {
        return false;
    };
    let value = line[value_start..].trim_start();
    // A trailing `#` after the value starts an inline comment; an empty or
    // comment-only value carries no brace to flag.
    if value.is_empty() || value.starts_with('#') {
        return false;
    }

    let mut in_single = false;
    let mut in_double = false;
    let vbytes = value.as_bytes();
    for (j, &c) in vbytes.iter().enumerate() {
        match c {
            b'\'' if !in_double => in_single = !in_single,
            b'"' if !in_single => in_double = !in_double,
            b'{' if !in_single && !in_double => {
                // Ignore `${...}` env placeholders (resolved pre-parse).
                if j > 0 && vbytes[j - 1] == b'$' {
                    continue;
                }
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Extract the lower-cased host from a `scheme://host[:port][/path]` endpoint,
/// or `None` when it does not look like a URL.
///
/// SecOps helper for the cloud-`endpoint` exfiltration guard (V2/V12): the host
/// decides whether a custom endpoint is a local emulator (loopback) or a remote
/// redirect target. We reject every non-loopback custom endpoint regardless of
/// scheme (covering both the exfil and the cleartext-`http` gaps), so only the
/// host is needed. We hand-parse rather than pull in a URL crate — the inputs
/// are operator-typed endpoints, not arbitrary URIs. A bracketed IPv6 literal
/// authority (`http://[::1]:9000`) keeps its address so it compares against the
/// loopback list.
fn endpoint_host(endpoint: &str) -> Option<String> {
    let (scheme, rest) = endpoint.split_once("://")?;
    if scheme.is_empty() {
        return None;
    }
    // Authority ends at the first `/` (path), `?` (query), or `#` (fragment);
    // any `user[:pass]@` userinfo head is dropped (host is after the last `@`).
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .rsplit('@')
        .next()
        .unwrap_or("");
    let host = if let Some(stripped) = authority.strip_prefix('[') {
        // Bracketed IPv6 literal: take up to the closing `]`.
        stripped.split(']').next().unwrap_or("")
    } else {
        // host[:port] — strip the port suffix.
        authority.split(':').next().unwrap_or("")
    };
    if host.is_empty() {
        return None;
    }
    Some(host.to_ascii_lowercase())
}

/// True when `host` names the local machine — the legitimate cloud-emulator
/// target (Minio / Azurite / fake-gcs on `127.0.0.1`). Anything else is a
/// remote host and a potential exfiltration redirect.
fn is_loopback_host(host: &str) -> bool {
    // `localhost` is the only non-IP host that counts as loopback. Everything
    // else must PARSE as an IP literal in the loopback range — a lexical
    // `starts_with("127.")` would accept attacker-controlled DNS like
    // `127.attacker.com` or `127.0.0.1.evil.com` (both resolve off-box), turning
    // the credential-exfil gate into a bypass (V2/V12). Parse strictly: only a
    // real `127.0.0.0/8` / `::1` address is loopback; a hostname is not.
    if host == "localhost" {
        return true;
    }
    // Tolerate a bracketed IPv6 literal (`[::1]`) in case a caller forwards one.
    let h = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    h.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

/// True when `name` is filename-safe: rejects path-traversal (`..`), absolute
/// or slash-bearing names (`/`, `\`), a leading `.` (hidden / current-dir), and
/// embedded NULs. `ExportConfig.name` is keyed into output paths and on-disk
/// state, so a `../../etc/x` or absolute name escapes the output tree (V5).
fn is_filename_safe_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
        && !name.contains('\0')
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod audit_csv_compression {
    //! Finding #10: `format: csv` + a compression codec is a silent no-op
    //! (the file stays uncompressed but the manifest records the codec). The
    //! combo must be rejected at config-validate time. These tests encode the
    //! new rule, so reverting the fix turns them red.
    use super::*;

    fn yaml(format: &str, compression_line: &str) -> String {
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: {format}\n\
             {compression_line}    destination:\n      type: local\n      path: ./out\n"
        )
    }

    #[test]
    fn audit_csv_compression_is_rejected() {
        // csv + gzip → rejected, with an actionable message.
        let err = Config::from_yaml(&yaml("csv", "    compression: gzip\n")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("CSV output does not support compression") && msg.contains("gzip"),
            "csv+gzip must be rejected with an actionable message; got: {msg}"
        );
        assert!(
            msg.contains("parquet") && msg.contains("none"),
            "message must point to the real options (parquet / none); got: {msg}"
        );

        // Guard the boundaries: parquet+gzip and csv+none still validate.
        Config::from_yaml(&yaml("parquet", "    compression: gzip\n"))
            .expect("parquet+gzip must validate");
        Config::from_yaml(&yaml("csv", "    compression: none\n")).expect("csv+none must validate");
    }

    #[test]
    fn audit_csv_every_real_codec_is_rejected() {
        // Each non-None codec is a silent no-op for CSV — none may slip through.
        for codec in ["zstd", "snappy", "gzip", "lz4"] {
            let err = Config::from_yaml(&yaml("csv", &format!("    compression: {codec}\n")))
                .unwrap_err();
            let msg = format!("{err:#}");
            assert!(
                msg.contains("CSV output does not support compression") && msg.contains(codec),
                "csv+{codec} must be rejected; got: {msg}"
            );
        }
    }

    #[test]
    fn audit_csv_compression_profile_is_rejected() {
        // A `compression_profile:` other than `none` resolves to a real codec,
        // so it is the same silent no-op for CSV.
        for profile in ["fast", "balanced", "compact"] {
            let err = Config::from_yaml(&yaml(
                "csv",
                &format!("    compression_profile: {profile}\n"),
            ))
            .unwrap_err();
            let msg = format!("{err:#}");
            assert!(
                msg.contains("CSV output does not support compression_profile")
                    && msg.contains(profile),
                "csv+profile {profile} must be rejected; got: {msg}"
            );
        }
        // profile: none is a no-op request and is fine.
        Config::from_yaml(&yaml("csv", "    compression_profile: none\n"))
            .expect("csv + compression_profile: none must validate");
    }

    #[test]
    fn audit_csv_default_compression_still_validates() {
        // Regression guard: a bare `format: csv` (no explicit codec) must keep
        // validating. `CompressionType::default()` is `Zstd`, but the user did
        // not *ask* for it — only an explicit codec is a no-op request. This
        // pins that the fix scans for explicit intent, not the struct default
        // (which would break ~60 existing csv configs).
        Config::from_yaml(&yaml("csv", "")).expect("bare format: csv must validate");
    }

    #[test]
    fn audit_compression_supported_predicate() {
        // `compression_supported` is re-exported via `pub use format::*`.
        // Parquet supports every codec; CSV supports only None.
        for ct in [
            CompressionType::Zstd,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
            CompressionType::None,
        ] {
            assert!(compression_supported(FormatType::Parquet, ct));
        }
        assert!(compression_supported(
            FormatType::Csv,
            CompressionType::None
        ));
        for ct in [
            CompressionType::Zstd,
            CompressionType::Snappy,
            CompressionType::Gzip,
            CompressionType::Lz4,
        ] {
            assert!(
                !compression_supported(FormatType::Csv, ct),
                "CSV must not claim to support {}",
                ct.label()
            );
        }
    }
}

#[cfg(test)]
mod audit_unquoted_template_brace {
    //! yaml-hint: an unquoted `{partition}` (or `{date}`) in a path/prefix
    //! value trips serde_yaml_ng's flow-mapping parser with a cryptic message
    //! that gives no clue the brace needs quoting. Since `{partition}` is the
    //! required token for `partition_by`, this is a common copy-paste footgun.
    //! `Config::from_yaml` augments the parser error with a quoting hint; these
    //! tests pin that behavior (and guard that valid configs are untouched).
    use super::*;

    /// A full, otherwise-valid config whose `prefix:` value is whatever the
    /// caller passes verbatim (quoted or not). Only the `prefix:` line varies,
    /// so any parse error is attributable to the brace under test.
    fn yaml_with_prefix(prefix_value: &str) -> String {
        format!(
            "source:\n\
             \x20 type: postgres\n\
             \x20 url: \"postgresql://localhost/test\"\n\
             exports:\n\
             \x20 - name: t\n\
             \x20   query: \"SELECT 1\"\n\
             \x20   format: parquet\n\
             \x20   partition_by: created_date\n\
             \x20   destination:\n\
             \x20     type: local\n\
             \x20     path: ./out\n\
             \x20     prefix: {prefix_value}\n"
        )
    }

    const HINT_FRAGMENT: &str =
        "a YAML value containing { } (such as {partition} or {date}) must be quoted";

    #[test]
    fn bare_partition_token_gets_quoting_hint() {
        // `prefix: {partition}` parses as a YAML map, so serde rejects it with
        // `invalid type: map, expected a string` — no clue it's a quoting bug.
        let err = Config::from_yaml(&yaml_with_prefix("{partition}")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(HINT_FRAGMENT),
            "bare {{partition}} must carry the quoting hint; got: {msg}"
        );
        // The original parser detail (type + location) is preserved.
        assert!(
            msg.contains("invalid type: map") || msg.contains("line"),
            "the original parser error must be kept; got: {msg}"
        );
    }

    #[test]
    fn trailing_text_after_brace_gets_quoting_hint() {
        // `prefix: {date}/{partition}/` runs the flow map into block context:
        // serde emits `did not find expected key ... while parsing a block
        // mapping`. Same footgun, different libyaml symptom.
        let err = Config::from_yaml(&yaml_with_prefix("{date}/{partition}/")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(HINT_FRAGMENT),
            "{{date}}/{{partition}}/ must carry the quoting hint; got: {msg}"
        );
    }

    #[test]
    fn unclosed_brace_gets_quoting_hint() {
        // `prefix: {partition` (unclosed) is the canonical flow-mapping scanner
        // error: `did not find expected ',' or '}' ... while parsing a flow
        // mapping`. The hint must still fire.
        let err = Config::from_yaml(&yaml_with_prefix("{partition")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(HINT_FRAGMENT),
            "unclosed brace must carry the quoting hint; got: {msg}"
        );
    }

    #[test]
    fn quoted_brace_value_loads_ok() {
        // The fix itself, applied: a properly quoted brace value parses and
        // validates. This is the guard that the hint never reaches a valid
        // config and the success path is unchanged.
        let cfg = Config::from_yaml(&yaml_with_prefix("\"exports/{partition}/\""))
            .expect("quoted {partition} prefix must load");
        assert_eq!(
            cfg.exports[0].destination.prefix.as_deref(),
            Some("exports/{partition}/")
        );
    }

    #[test]
    fn config_without_braces_is_untouched() {
        // No brace anywhere: a plain valid config still loads, and an unrelated
        // YAML error elsewhere must not pick up a spurious quoting hint.
        Config::from_yaml(&yaml_with_prefix("exports/data/"))
            .expect("a brace-free prefix must load");
    }

    // ── line_has_unquoted_brace_value() unit coverage ──────────────────────

    #[test]
    fn unquoted_brace_value_is_detected() {
        assert!(line_has_unquoted_brace_value("    prefix: {partition}"));
        assert!(line_has_unquoted_brace_value("      path: {date}/out"));
        assert!(line_has_unquoted_brace_value("prefix: {partition")); // unclosed
    }

    #[test]
    fn quoted_brace_value_is_not_flagged() {
        // Quotes around the value hide the brace from the scanner — not a bug.
        assert!(!line_has_unquoted_brace_value(
            "    prefix: \"exports/{partition}/\""
        ));
        assert!(!line_has_unquoted_brace_value("    prefix: 'data/{date}/'"));
    }

    #[test]
    fn env_placeholder_and_plain_values_are_not_flagged() {
        // `${VAR}` placeholders are resolved before the parse and are not the
        // footgun; plain brace-free values are obviously fine.
        assert!(!line_has_unquoted_brace_value("    url: ${DATABASE_URL}"));
        assert!(!line_has_unquoted_brace_value("    path: ./out"));
        assert!(!line_has_unquoted_brace_value("  # prefix: {partition}")); // comment
        assert!(!line_has_unquoted_brace_value("    prefix:")); // no value
    }
}

#[cfg(test)]
mod sec_config_validation_regression {
    //! Regression edge-cases that pin the *compat boundaries* of the
    //! config-validation security fixes — the cases that distinguish a real
    //! attack from a legitimate loopback / dev-container / Docker pattern.
    //! These complement the RED tests in `sec_config_validation`: the RED
    //! tests assert the attack is rejected; these assert the fix stays narrow
    //! enough not to break local-dev usage (see CRITICAL COMPAT).
    use super::*;

    /// A full, otherwise-valid config whose single export's `destination:`
    /// block is whatever the caller passes verbatim.
    fn yaml_with_destination(dest_block: &str) -> String {
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n\
             {dest_block}"
        )
    }

    // ── endpoint_host / is_loopback_host helpers ─────────────────────────────

    #[test]
    fn endpoint_host_parses_forms() {
        assert_eq!(
            endpoint_host("https://attacker.example.com").as_deref(),
            Some("attacker.example.com")
        );
        // Port and path are stripped from the host.
        assert_eq!(
            endpoint_host("http://127.0.0.1:10000/devstoreaccount1").as_deref(),
            Some("127.0.0.1")
        );
        // userinfo head is dropped (host is after the last `@`).
        assert_eq!(
            endpoint_host("http://user:pass@127.0.0.1:9000").as_deref(),
            Some("127.0.0.1")
        );
        // Bracketed IPv6 literal keeps its address.
        assert_eq!(endpoint_host("http://[::1]:9000").as_deref(), Some("::1"));
        // Not a URL → None (treated as a non-loopback custom endpoint upstream).
        assert_eq!(endpoint_host("not-a-url"), None);
        assert_eq!(endpoint_host("://nohost"), None);
    }

    #[test]
    fn loopback_host_classification() {
        for h in ["127.0.0.1", "127.0.0.53", "localhost", "::1"] {
            assert!(is_loopback_host(h), "{h} must be loopback");
        }
        for h in ["attacker.example.com", "evil.com", "10.0.0.1", "::2"] {
            assert!(!is_loopback_host(h), "{h} must be remote");
        }
    }

    // ── V2/V12 endpoint: loopback accepted regardless of allow_anonymous ─────

    #[test]
    fn loopback_endpoint_without_allow_anonymous_still_accepted() {
        // A loopback emulator endpoint with credentials (no allow_anonymous) is
        // the Minio-with-keys local-dev pattern and must stay accepted — the
        // exfil guard targets *remote* hosts, not localhost.
        let cfg = yaml_with_destination(
            "    destination:\n      type: s3\n      bucket: b\n      region: us-east-1\n\
             \x20     endpoint: http://127.0.0.1:9000\n      access_key_env: AK\n      secret_key_env: SK\n",
        );
        Config::from_yaml(&cfg).expect("loopback endpoint with creds must stay accepted");
    }

    #[test]
    fn remote_https_endpoint_with_allow_anonymous_is_the_only_remote_escape() {
        // The documented escape hatch: an explicit anonymous (emulator) opt-in
        // permits a non-loopback endpoint (no credentials are sent). Without
        // allow_anonymous the same endpoint is rejected (covered by the RED
        // test); with it, accepted.
        let cfg = yaml_with_destination(
            "    destination:\n      type: gcs\n      bucket: b\n\
             \x20     endpoint: https://emulator.example.com\n      allow_anonymous: true\n",
        );
        Config::from_yaml(&cfg).expect("remote endpoint + allow_anonymous opt-in must be accepted");
    }

    // ── V15 local path: absolute allowed (Docker mount), `..` rejected ───────

    #[test]
    fn absolute_local_path_is_allowed() {
        // `path: /output` is a legitimate Docker volume-mount pattern
        // (examples/rivet.yaml) and must keep validating — only `..` climbs are
        // the traversal footgun.
        let cfg =
            yaml_with_destination("    destination:\n      type: local\n      path: /output\n");
        Config::from_yaml(&cfg).expect("absolute local path (Docker mount) must validate");
    }

    #[test]
    fn dotdot_in_local_prefix_is_rejected() {
        // `prefix` is guarded the same as `path`.
        let cfg = yaml_with_destination(
            "    destination:\n      type: local\n      path: ./out\n      prefix: a/../b\n",
        );
        let err = Config::from_yaml(&cfg).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("prefix") && msg.contains(".."),
            "a '..' in the local prefix must be rejected naming prefix/..; got: {msg}"
        );
    }

    // ── V13 TLS: explicit enforced mode + knob rejected; default-mode kept ───

    #[test]
    fn tls_danger_knob_without_explicit_mode_still_accepted() {
        // The dev-container pattern `tls: { accept_invalid_certs: true }` against
        // a loopback self-signed cert (e.g. the MSSQL docker container) omits
        // `mode:` — there is no *explicit* mode to contradict, so it must keep
        // validating. The RED test rejects only the explicit-mode contradiction.
        let yaml = "source:\n  type: mssql\n  url: \"sqlserver://sa:pw@127.0.0.1:1433/db\"\n  \
                    tls:\n    accept_invalid_certs: true\n\
                    exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    \
                    destination:\n      type: local\n      path: ./out\n";
        Config::from_yaml(yaml)
            .expect("dev-container default-mode + accept_invalid_certs must stay accepted");
    }

    #[test]
    fn tls_explicit_verify_ca_plus_invalid_hostnames_rejected() {
        // The hostname knob is flagged too, against any explicit enforced mode.
        let yaml = "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n  \
                    tls:\n    mode: verify-ca\n    accept_invalid_hostnames: true\n\
                    exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    \
                    destination:\n      type: local\n      path: ./out\n";
        let err = Config::from_yaml(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("accept_invalid_hostnames") && msg.contains("verify-ca"),
            "explicit verify-ca + accept_invalid_hostnames must be rejected; got: {msg}"
        );
    }

    #[test]
    fn tls_explicit_disable_with_knob_is_not_flagged() {
        // `mode: disable` carries no verification promise to contradict, so the
        // danger knob is a no-op there and must not be rejected.
        let yaml = "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n  \
                    tls:\n    mode: disable\n    accept_invalid_certs: true\n\
                    exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n    \
                    destination:\n      type: local\n      path: ./out\n";
        Config::from_yaml(yaml).expect("mode: disable + knob is a no-op and must validate");
    }

    // ── V5 name: filename-safe predicate boundaries ──────────────────────────

    #[test]
    fn filename_safe_name_boundaries() {
        for ok in ["t", "orders", "daily_events", "v2-2024", "name.with.dots"] {
            assert!(is_filename_safe_name(ok), "{ok:?} must be accepted");
        }
        for bad in [
            "",
            "..",
            "../x",
            "/abs",
            "sub/dir",
            "back\\slash",
            ".hidden",
            "with\u{0000}nul",
        ] {
            assert!(!is_filename_safe_name(bad), "{bad:?} must be rejected");
        }
    }
}

#[cfg(test)]
mod sec_config_validation {
    //! RED security tests for config-load validation gaps (cluster:
    //! config-validation). Each asserts the SECURE behavior through the
    //! stable `Config::from_yaml` seam: a malicious config that is accepted
    //! today must be REJECTED (or, for warn-only knobs, surfaced as an
    //! error/loud warning) at config-load. These are expected to FAIL until
    //! the corresponding production fix lands.
    //!
    //! The pattern mirrors the existing `query_file` `..`/absolute-path guard
    //! in `validate_export` (see `config/tests/validation.rs`): a syntactic
    //! check that runs at config-validate time so `rivet check` / `rivet
    //! doctor` catch the problem before any connect/plan/upload step.
    use super::*;

    /// A full, otherwise-valid config whose single export's `destination:`
    /// block is whatever the caller passes verbatim. Only the destination
    /// varies, so any rejection is attributable to the destination under test.
    fn yaml_with_destination(dest_block: &str) -> String {
        format!(
            "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
             exports:\n  - name: t\n    query: \"SELECT 1\"\n    format: parquet\n\
             {dest_block}"
        )
    }

    // ── V2/V12: cloud-endpoint exfiltration + http cleartext ────────────────
    //
    // `destination.endpoint` is passed straight to the opendal S3/GCS/Azure
    // builder with no validation (see `src/destination/{s3,gcs,azure}.rs`),
    // so a committed config can silently redirect every export to an
    // attacker-controlled host. Two distinct gaps:
    //   V2  — a custom *non-loopback* endpoint (data exfiltration target).
    //   V12 — an `http://` (plaintext) endpoint (credentials + data on the
    //         wire in cleartext).
    // The secure behavior is to reject (or require explicit opt-in) at
    // config-load. Loopback/emulator endpoints (Minio/Azurite/fake-gcs on
    // 127.0.0.1) MUST stay accepted — that path is exercised by the existing
    // `gcs_allow_anonymous_parses` test and the guard test below.

    #[test]
    fn sec_s3_custom_endpoint_rejected() {
        // SEC-RED V2: a non-loopback custom S3 endpoint is an exfiltration
        // target — every part upload goes to attacker.example.com. Must be
        // rejected (or require explicit opt-in) at config-load. Accepted today.
        let cfg = yaml_with_destination(
            "    destination:\n      type: s3\n      bucket: my-bucket\n      region: us-east-1\n\
             \x20     endpoint: https://attacker.example.com\n",
        );
        let res = Config::from_yaml(&cfg);
        assert!(
            res.is_err(),
            "a non-loopback custom S3 endpoint (https://attacker.example.com) must be \
             rejected at config-load (data-exfiltration target); got Ok"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("endpoint"),
            "rejection must name the offending 'endpoint' field; got: {msg}"
        );
    }

    #[test]
    fn sec_http_endpoint_rejected() {
        // SEC-RED V12: a plaintext http:// endpoint to a *remote* host sends
        // credentials and exported rows over the wire in cleartext. Must be
        // rejected (or require explicit opt-in) at config-load. Accepted today.
        // Use a non-loopback host so this is distinct from the Minio/Azurite
        // loopback emulator case (guarded below).
        let cfg = yaml_with_destination(
            "    destination:\n      type: s3\n      bucket: my-bucket\n      region: us-east-1\n\
             \x20     endpoint: http://evil.com\n",
        );
        let res = Config::from_yaml(&cfg);
        assert!(
            res.is_err(),
            "a plaintext http:// endpoint to a remote host (http://evil.com) must be \
             rejected at config-load (cleartext credentials + data); got Ok"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("endpoint") || msg.to_lowercase().contains("http"),
            "rejection must name the endpoint / cleartext problem; got: {msg}"
        );
    }

    #[test]
    fn sec_loopback_endpoint_still_accepted_guard() {
        // SEC-RED V2/V12 (guard): a loopback emulator endpoint
        // (`http://127.0.0.1:9000` Minio, with allow_anonymous) is the
        // legitimate local-dev path and MUST stay accepted after the fix.
        // This pins that the endpoint rejection targets *remote* hosts, not
        // localhost — otherwise the fix breaks every Minio/Azurite/fake-gcs
        // integration test (see `gcs_allow_anonymous_parses`).
        let cfg = yaml_with_destination(
            "    destination:\n      type: s3\n      bucket: my-bucket\n      region: us-east-1\n\
             \x20     endpoint: http://127.0.0.1:9000\n      allow_anonymous: true\n",
        );
        Config::from_yaml(&cfg)
            .expect("a loopback emulator endpoint with allow_anonymous must stay accepted");
    }

    // ── V5: export `name` path traversal ────────────────────────────────────
    //
    // `ExportConfig.name` is a free-form `String` keyed into state tracking,
    // file logs, and (via the destination layout) output paths — yet it is
    // never validated. A name like `../../../etc/x`, an absolute `/abs/x`, a
    // bare slash, or an embedded NUL can escape the intended output tree.
    // Mirror the `query_file` `..`/absolute guard: reject at config-load.

    #[test]
    fn sec_export_name_traversal_rejected() {
        // SEC-RED V5: a traversal / absolute / slash / NUL export name escapes
        // the output tree (and corrupts name-keyed state). Must be rejected at
        // config-load. Accepted today.
        for bad in ["../../../etc/x", "/abs/x", "sub/dir", "with\u{0000}nul"] {
            // `name:` is JSON-encoded so embedded slashes / NULs survive the
            // YAML parse verbatim and reach validation.
            let name_yaml = serde_json::to_string(bad).expect("encode name");
            let cfg = format!(
                "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n\
                 exports:\n  - name: {name_yaml}\n    query: \"SELECT 1\"\n    format: parquet\n\
                 \x20   destination:\n      type: local\n      path: ./out\n"
            );
            let res = Config::from_yaml(&cfg);
            assert!(
                res.is_err(),
                "export name {bad:?} (traversal/absolute/slash/NUL) must be rejected at \
                 config-load; got Ok"
            );
            let msg = format!("{:#}", res.unwrap_err());
            assert!(
                msg.contains("name"),
                "rejection of name {bad:?} must name the offending 'name' field; got: {msg}"
            );
        }
    }

    #[test]
    fn sec_export_name_normal_still_accepted_guard() {
        // SEC-RED V5 (guard): a plain, well-formed export name must keep
        // loading after the fix. Pins that the traversal check is narrow.
        let cfg = yaml_with_destination("    destination:\n      type: local\n      path: ./out\n");
        Config::from_yaml(&cfg).expect("a normal export name ('t') must stay accepted");
    }

    // ── V15: local destination `path` traversal ─────────────────────────────
    //
    // `destination.path` for a `type: local` export is written verbatim to the
    // filesystem. A relative `../../../../tmp/x` or absolute path lets a
    // committed config write outside the intended output directory. Must be
    // rejected (or at minimum loudly surfaced) at config-load. Accepted today.

    #[test]
    fn sec_local_dest_path_traversal_rejected() {
        // SEC-RED V15: a traversal local-destination path writes outside the
        // intended output tree. Must be rejected at config-load. Accepted today.
        let cfg = yaml_with_destination(
            "    destination:\n      type: local\n      path: ../../../../tmp/x\n",
        );
        let res = Config::from_yaml(&cfg);
        assert!(
            res.is_err(),
            "a local destination path containing '..' (../../../../tmp/x) must be rejected \
             at config-load (writes outside the output tree); got Ok"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("path") || msg.contains(".."),
            "rejection must name the offending 'path' / traversal; got: {msg}"
        );
    }

    // ── V13: dangerous TLS cert-knob combination ─────────────────────────────
    //
    // `tls: { mode: verify-full, accept_invalid_certs: true }` silently
    // *downgrades* the strongest mode to "accept any cert" — `verify-full`
    // promises chain + hostname verification, but the danger knob disables
    // chain verification (see `src/source/tls.rs::build_native_tls`). The
    // comment at `src/source/tls.rs:55-56` claims "Each one emits a warning at
    // config-time (see `Config::validate`)" — but `Config::validate` emits no
    // such warning today. The secure behavior is a LOUD error (or surfaced
    // warning) at config-load. No `Err`/warning is produced today, so this is
    // RED.

    #[test]
    fn sec_accept_invalid_certs_warns() {
        // SEC-RED V13: verify-full + accept_invalid_certs: true is a silent
        // security downgrade that contradicts the chosen mode. It must be
        // loudly surfaced at config-load. The only stable secure seam is an
        // `Err` from `Config::from_yaml` (validate returns Ok today, and there
        // is no captured-warning seam exposed from here — see notes). Asserting
        // `Err` is the strongest secure assertion and is RED against current
        // code.
        let cfg = yaml_with_destination("    destination:\n      type: local\n      path: ./out\n");
        // Splice the TLS block into the source rather than the destination so
        // the rest of the config stays valid.
        let cfg = cfg.replace(
            "  url: \"postgresql://localhost/test\"\n",
            "  url: \"postgresql://localhost/test\"\n  tls:\n    mode: verify-full\n    accept_invalid_certs: true\n",
        );
        let res = Config::from_yaml(&cfg);
        assert!(
            res.is_err(),
            "tls mode: verify-full with accept_invalid_certs: true is a silent security \
             downgrade and must be loudly surfaced (error) at config-load; got Ok"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("accept_invalid_certs") || msg.to_lowercase().contains("verify"),
            "the surfaced error must name the dangerous knob / mode contradiction; got: {msg}"
        );
    }
}
