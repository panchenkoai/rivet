//! Plan artifact — the serializable result of `rivet plan`.
//!
//! A `PlanArtifact` captures a complete, resolved execution plan at a point in
//! time: the `ResolvedRunPlan`, pre-computed chunk ranges (for Chunked exports),
//! a cursor snapshot (for Incremental exports), preflight diagnostics, and
//! metadata needed to detect staleness before `rivet apply`.
//!
//! # Contracts (ADR-0005)
//!
//! | ID  | Name                           | Enforced by                              |
//! |-----|--------------------------------|------------------------------------------|
//! | PA1 | Artifact Is the Communication Channel | `apply_cmd` reads artifact only, never config |
//! | PA2 | Artifact Immutability          | artifact file never written by `apply`   |
//! | PA3 | Staleness Boundary             | [`PlanArtifact::staleness`]              |
//! | PA4 | Cursor Snapshot Integrity      | [`PlanArtifact::cursor_matches`]         |
//! | PA5 | Chunk Range Monotonicity       | by construction via `generate_chunks`    |
//! | PA6 | Fingerprint Stability          | logged at apply time (advisory)          |
//! | PA7 | State Writes Unchanged         | ADR-0001 invariants apply unchanged      |
//! | PA8 | Diagnostics Are Advisory       | verdict not enforced at apply time       |
//! | PA10| Resolved-Plan Integrity        | [`PlanArtifact::verify_integrity`] (#16) |

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::plan::ResolvedRunPlan;

use super::prioritization::{CampaignRecommendation, ExportRecommendation};

/// Fully self-contained plan artifact written by `rivet plan` and consumed by
/// `rivet apply`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanArtifact {
    /// Rivet version that produced this artifact.
    pub rivet_version: String,
    /// Random hex identifier for this plan instance.
    pub plan_id: String,
    /// Wall-clock time when the plan was generated.
    pub created_at: DateTime<Utc>,
    /// Wall-clock time after which `rivet apply` will refuse to execute without
    /// `--force`.  Default: `created_at + 24 h`.
    pub expires_at: DateTime<Utc>,
    /// Export this plan targets.
    pub export_name: String,
    /// Human-readable mode label: "full", "incremental", "chunked", "timewindow".
    pub strategy: String,
    /// Deterministic fingerprint of the chunked plan inputs (empty for non-chunked).
    pub plan_fingerprint: String,
    /// Accidental-corruption checksum over [`Self::resolved_plan`] (ADR-0005
    /// PA10, finding #16).  Computed at `rivet plan` time over the canonical
    /// JSON of the *redacted* resolved plan and re-checked by `rivet apply`
    /// before any query runs — a `base_query` (or any other execution-affecting
    /// field) that changed since planning makes apply reject the artifact rather
    /// than export the wrong thing under the planned export name.
    ///
    /// **This is NOT tamper protection.** The checksum is an *unkeyed* `xxh3`
    /// digest stored in the same JSON it covers, so anyone who edits the plan
    /// can recompute a matching value — there is no secret/MAC. It catches
    /// *accidental* edits (a hand-tweaked field, a botched merge, on-disk bit
    /// rot), not a *malicious* writer. A real tamper seal would need a keyed MAC
    /// plus a key store, which is out of scope; `apply` trusts the artifact the
    /// same way it trusts an untrusted config handed to it. See
    /// [`Self::verify_integrity`].
    ///
    /// Format mirrors the crate's other content checksums: `"xxh3:<16 hex>"`.
    /// `#[serde(default)]` keeps pre-PA10 artifacts (empty string) readable;
    /// [`Self::verify_integrity`] treats an unchecksummed artifact as legacy and
    /// warns rather than failing closed (back-compat, never a silent pass).
    #[serde(default)]
    pub integrity: String,
    /// The fully-resolved execution plan embedded verbatim.
    pub resolved_plan: ResolvedRunPlan,
    /// Data computed at plan time that can be replayed at apply time.
    pub computed: ComputedPlanData,
    /// Preflight diagnostics captured at plan time.
    pub diagnostics: PlanDiagnostics,
    /// Advisory source-aware prioritization (ADR-0006). Does not affect execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prioritization: Option<PlanPrioritizationSnapshot>,
    /// Absolute path to the config file that produced this plan, captured
    /// at `rivet plan` time.  `rivet apply` uses this to locate the
    /// matching `.rivet_state.db` (cursors, manifest history) even when
    /// the plan file is stored in a different directory.  `None` for
    /// pre-0.7.5 artifacts; in that case apply falls back to the plan
    /// file's own directory.  See finding **F13** of the 0.7.5 audit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
}

/// Per-export recommendation and optional multi-export campaign view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanPrioritizationSnapshot {
    pub export_recommendation: ExportRecommendation,
    /// Present when `rivet plan` runs on the full config with multiple exports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign: Option<CampaignRecommendation>,
}

/// Data computed during `rivet plan` that is reused at `rivet apply` time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedPlanData {
    /// Pre-computed chunk ranges `(start, end)` for Chunked exports.
    /// Empty for Snapshot / Incremental / TimeWindow.
    pub chunk_ranges: Vec<(i64, i64)>,
    /// Number of chunk windows (convenience; equals `chunk_ranges.len()`).
    pub chunk_count: usize,
    /// Last cursor value from `StateStore` at plan time, for Incremental exports.
    /// `None` for all other strategies or when no prior run exists.
    pub cursor_snapshot: Option<String>,
    /// Row estimate returned by preflight (may be `None` if unavailable).
    pub row_estimate: Option<i64>,
}

/// Preflight diagnostics captured at plan time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanDiagnostics {
    /// Health verdict: "Efficient", "Acceptable", "Degraded", or "Unsafe".
    pub verdict: String,
    /// Human-readable warnings (sparsity, memory risk, missing index, …).
    pub warnings: Vec<String>,
    /// Recommended tuning profile: "fast", "balanced", or "safe".
    pub recommended_profile: String,
    /// Narrative explaining *why* this export's strategy (mode + chunk geometry +
    /// parallelism) was chosen, plus its risk profile (resumable? memory bound?).
    /// Built from the preflight `ExportDiagnostic` + config by
    /// [`crate::plan::explain::explain_strategy`].  `#[serde(default)]` keeps
    /// pre-existing artifacts (which lack the field) readable: they deserialize
    /// to an empty string and `print_summary` simply omits the block.  This field
    /// is **not** covered by the resolved-plan integrity seal (that hashes only
    /// `resolved_plan`), so adding it never disturbs an existing artifact's
    /// checksum.
    #[serde(default)]
    pub strategy_rationale: String,
}

/// Result of a staleness check against the current wall clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StalenessCheck {
    /// Plan is recent enough to apply without warnings.
    Fresh,
    /// Plan is older than the warn threshold but younger than the error threshold.
    StaleWarn(Duration),
    /// Plan is older than the error threshold; `--force` is required to apply.
    StaleError(Duration),
}

impl PlanArtifact {
    /// Build a new artifact with default TTL of 24 hours.
    ///
    /// **Credential redaction (ADR-0005 PA9):** before the resolved plan is embedded,
    /// any plaintext `password` or credentials-in-URL are stripped via
    /// [`crate::config::SourceConfig::redact_for_artifact`]. A warning is logged
    /// when redaction runs so operators know to provide env/file creds at apply time.
    pub fn new(
        export_name: String,
        strategy: String,
        plan_fingerprint: String,
        mut resolved_plan: ResolvedRunPlan,
        computed: ComputedPlanData,
        diagnostics: PlanDiagnostics,
    ) -> Self {
        let (safe_source, was_redacted) = resolved_plan.source.redact_for_artifact();
        resolved_plan.source = safe_source;
        if was_redacted {
            log::warn!(
                "plan '{}': plaintext credentials stripped from artifact — \
                 apply time must have equivalent env/file-based auth available",
                export_name
            );
        }

        // PA10 (#16): checksum the *redacted* resolved plan so apply can detect
        // an accidental post-plan edit (not a malicious one — the digest is
        // unkeyed; see the `integrity` field doc).  Computed here, after
        // redaction, so the checksum matches exactly what gets serialized to
        // disk.  `config_path` (set by the caller after construction) is
        // intentionally outside the checksum — it is an apply-time
        // state-location hint, not part of the execution contract, and the
        // operator may legitimately relocate the plan file.
        let integrity = resolved_plan_integrity(&resolved_plan);

        let created_at = Utc::now();
        Self {
            rivet_version: env!("CARGO_PKG_VERSION").to_string(),
            plan_id: new_plan_id(),
            created_at,
            expires_at: created_at + Duration::hours(24),
            export_name,
            strategy,
            plan_fingerprint,
            integrity,
            resolved_plan,
            computed,
            diagnostics,
            prioritization: None,
            config_path: None,
        }
    }

    /// Re-check the resolved-plan corruption checksum (ADR-0005 PA10, finding
    /// #16).
    ///
    /// Recomputes the canonical hash of [`Self::resolved_plan`] and compares it
    /// to the [`Self::integrity`] checksum recorded at plan time.  On mismatch,
    /// returns an error so `rivet apply` exits non-zero rather than running a
    /// changed query under the planned export name.
    ///
    /// **Not a tamper guard.** Because the checksum is unkeyed and stored beside
    /// the data it covers (see the [`Self::integrity`] field doc), this detects
    /// an *accidental* change — a hand-edited field, a bad merge, on-disk
    /// corruption — but not a *deliberate* attacker, who can recompute a
    /// matching digest. There is no secret/MAC; a real tamper guard needs a key
    /// store, which is out of scope. `apply` trusts the artifact the way it
    /// trusts the config it was generated from.
    ///
    /// Back-compat: a pre-PA10 artifact has an empty checksum (the
    /// `#[serde(default)]` fallback).  Such an artifact cannot be checked, so we
    /// log a WARN and accept it — failing closed would break apply on every plan
    /// file generated before this field existed.  This is explicit, not a silent
    /// pass: the operator is told the artifact predates the corruption checksum.
    pub fn verify_integrity(&self) -> Result<()> {
        if self.integrity.is_empty() {
            log::warn!(
                "plan '{}': artifact predates the corruption checksum (no `integrity` field); \
                 cannot detect an accidental edit after planning. Re-run `rivet plan` to \
                 produce a checksummed artifact.",
                self.export_name,
            );
            return Ok(());
        }
        let actual = resolved_plan_integrity(&self.resolved_plan);
        if actual != self.integrity {
            anyhow::bail!(
                "plan artifact integrity check failed: resolved_plan was modified after planning \
                 (export '{}', sealed {}, recomputed {}). \
                 Do not hand-edit plan files — re-run `rivet plan` to regenerate.",
                self.export_name,
                self.integrity,
                actual,
            );
        }
        Ok(())
    }

    /// Serialize to pretty-printed JSON.
    pub fn to_json_pretty(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Serialize to compact JSON (used in tests and by callers that need minimal output).
    #[allow(dead_code)]
    pub fn to_json_compact(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserialize from a JSON string.
    pub fn from_json(s: &str) -> Result<Self> {
        Ok(serde_json::from_str(s)?)
    }

    /// Load from a JSON file on disk.
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read plan file '{}': {}", path, e))?;
        // F8 (0.7.5 audit): wrap raw serde errors so the operator gets
        // a Rivet-shaped message instead of `expected ident at line 1
        // column 2`.  The original parser error is preserved as inner
        // context for diagnostics.
        Self::from_json(&content).map_err(|e| {
            anyhow::anyhow!(
                "plan file '{}' is not a valid Rivet plan artifact.\n  \
                 Hint: only files generated by `rivet plan` are accepted. \
                 Parser detail: {}",
                path,
                e
            )
        })
    }

    /// Check whether the plan is still fresh enough to apply.
    ///
    /// Implements **PA3 — Staleness Boundary** (ADR-0005):
    ///
    /// - age < `warn_after`  → `Fresh`
    /// - `warn_after` ≤ age < `error_after` → `StaleWarn(age)`
    /// - age ≥ `error_after` → `StaleError(age)`
    ///
    /// `apply_cmd` uses `warn_after = 1 h`, `error_after = 24 h`.
    pub fn staleness(&self, warn_after: Duration, error_after: Duration) -> StalenessCheck {
        let age = Utc::now().signed_duration_since(self.created_at);
        if age >= error_after {
            StalenessCheck::StaleError(age)
        } else if age >= warn_after {
            StalenessCheck::StaleWarn(age)
        } else {
            StalenessCheck::Fresh
        }
    }

    /// Returns `true` when the current cursor value in the state store equals
    /// the cursor snapshot recorded at plan time.
    ///
    /// Implements **PA4 — Cursor Snapshot Integrity** (ADR-0005).
    ///
    /// Always returns `true` for non-Incremental exports (cursor is irrelevant).
    /// When `false`, `apply_cmd` rejects the artifact to prevent re-exporting
    /// rows that were already exported after the plan was generated.
    pub fn cursor_matches(&self, current_cursor: Option<&str>) -> bool {
        match &self.computed.cursor_snapshot {
            None => true, // not an incremental export, or first run
            Some(snap) => current_cursor == Some(snap.as_str()),
        }
    }

    /// Pretty-print a human-readable plan summary to stdout.
    pub fn print_summary(&self) {
        println!();
        println!("  Plan ID  : {}", self.plan_id);
        println!(
            "  Created  : {}",
            self.created_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        println!(
            "  Expires  : {}",
            self.expires_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        println!("  Export   : {}", self.export_name);
        println!("  Strategy : {}", self.strategy);
        if !self.plan_fingerprint.is_empty() {
            println!("  Fingerprint : {}", self.plan_fingerprint);
        }

        // computed data
        if self.computed.chunk_count > 0 {
            println!("  Chunks   : {}", self.computed.chunk_count);
        }
        if let Some(est) = self.computed.row_estimate {
            println!("  Row est. : ~{}", format_rows(est));
        }
        if let Some(ref cur) = self.computed.cursor_snapshot {
            println!("  Cursor   : {}", cur);
        }

        // diagnostics
        println!("  Verdict  : {}", self.diagnostics.verdict);
        println!("  Profile  : {}", self.diagnostics.recommended_profile);
        if !self.diagnostics.strategy_rationale.is_empty() {
            println!("  Strategy :");
            // The rationale is one paragraph (sentences joined by spaces). Wrap it
            // to a readable width and indent under the "Why:" label so the block
            // reads as the *reason* for the numbers printed above.
            println!("    Why: {}", self.diagnostics.strategy_rationale);
        }
        if !self.diagnostics.warnings.is_empty() {
            println!("  Warnings :");
            for w in &self.diagnostics.warnings {
                println!("    • {}", w);
            }
        }

        if let Some(ref p) = self.prioritization {
            println!(
                "  Priority   : score {} — {:?} (wave {})",
                p.export_recommendation.priority_score,
                p.export_recommendation.priority_class,
                p.export_recommendation.recommended_wave
            );
            if p.export_recommendation.isolate_on_source {
                println!("  Isolate    : yes (shared source advisory)");
            }
            if !p.export_recommendation.reasons.is_empty() {
                println!("  Prioritize :");
                for r in &p.export_recommendation.reasons {
                    println!("    • [{}] {}", r.kind.as_str(), r.message);
                }
            }
            if let Some(ref c) = p.campaign {
                if c.source_group_warnings.is_empty() {
                    println!(
                        "  Campaign   : {} export(s) ordered by advisory score",
                        c.ordered_exports.len()
                    );
                } else {
                    println!("  Campaign   :");
                    for w in &c.source_group_warnings {
                        println!("    • {}", w);
                    }
                }
            }
        }

        // resource estimates
        let res = self.resolved_plan.tuning.resource_summary();
        println!("  Resources:");
        if let Some(mem_mb) = res.batch_size_memory_mb {
            println!("    Batch size   : adaptive (target {} MB/batch)", mem_mb);
        } else {
            println!(
                "    Batch size   : {:>7} rows",
                format_number(res.batch_size)
            );
        }
        println!(
            "    Batch memory : ~{:.0} MB (narrow) – ~{:.0} MB (wide)",
            res.batch_narrow_mb, res.batch_wide_mb
        );
        if res.memory_threshold_mb > 0 {
            println!(
                "    RSS guard    : {} MB",
                format_number(res.memory_threshold_mb)
            );
        }
        if res.throttle_ms > 0 {
            println!("    Throttle     : {} ms between batches", res.throttle_ms);
        }
        if res.wide_table_risk {
            println!(
                "    ⚠ Wide tables may use up to {:.0} MB/batch — consider batch_size_memory_mb or a lower batch_size",
                res.batch_wide_mb
            );
        }

        // destination
        let dest = &self.resolved_plan.destination;
        let dest_label = dest.destination_type.label();
        let dest_location = dest
            .path
            .as_deref()
            .or(dest.bucket.as_deref())
            .unwrap_or("(default)");
        println!("  Output   : {} → {}", dest_label, dest_location);

        // format / compression / parquet row group
        let fmt = self.resolved_plan.format.label();
        let comp = self.resolved_plan.compression.label();
        if let Some(level) = self.resolved_plan.compression_level {
            println!("  Format   : {} + {} (level {})", fmt, comp, level);
        } else {
            println!("  Format   : {} + {}", fmt, comp);
        }
        if let (crate::config::FormatType::Parquet, Some(pc)) =
            (self.resolved_plan.format, &self.resolved_plan.parquet)
        {
            let strategy = pc.row_group_strategy.unwrap_or_default();
            match strategy {
                crate::config::RowGroupStrategy::FixedRows => {
                    if let Some(rows) = pc.row_group_rows {
                        println!("  Row group: fixed {} rows", format_number(rows));
                    }
                }
                _ => {
                    let target_mb = pc
                        .target_row_group_mb
                        .unwrap_or(crate::config::ParquetConfig::DEFAULT_TARGET_ROW_GROUP_MB);
                    println!(
                        "  Row group: {:?} (target {} MB{})",
                        strategy,
                        target_mb,
                        pc.max_row_group_mb
                            .map(|m| format!(", max {} MB", m))
                            .unwrap_or_default(),
                    );
                }
            }
        }
        println!();
    }
}

/// Compute the accidental-corruption checksum over a resolved plan (ADR-0005
/// PA10, #16).
///
/// This is an *unkeyed* `xxh3` digest (matching the crate's other content
/// checksums in `manifest.rs` / `commit.rs`, format `"xxh3:<16 hex>"`) — it is
/// deliberately NOT a security MAC. Stored beside the data it covers, it detects
/// an accidental change to the plan, not a malicious one (an attacker recomputes
/// it for free); see the [`PlanArtifact::integrity`] field doc for the threat
/// model. The digest runs over a **canonical** JSON encoding of the plan.
/// Canonicalisation matters because the plan embeds process-randomized maps —
/// `column_overrides: HashMap<String, RivetType>` and
/// `quality.null_ratio_max: HashMap<String, f64>` — whose iteration order is
/// **not** stable across processes (std `HashMap` uses a per-process random
/// seed).  The checksum is written by one process (`rivet plan`) and re-checked
/// by another (`rivet apply`), so the bytes hashed must not depend on map
/// ordering.
///
/// We **cannot** rely on `serde_json::Value` to sort keys for us: this crate
/// pulls `serde_json/preserve_order` transitively (via `schemars`'s
/// `preserve_order` feature → `serde_json/preserve_order`), so `Value::Object`
/// is an *insertion-ordered* `IndexMap`, not a key-sorted `BTreeMap`.  Without
/// an explicit sort, a plan with ≥2 column overrides would hash differently on
/// the apply host and a legitimate, untouched artifact would be wrongly rejected.
/// Instead we serialize to a `Value` and recursively sort every object's keys
/// (`canonicalize_value`) before encoding — order-independent by construction,
/// mirroring the crate's own `schema_fingerprint` (which sorts columns by name).
///
/// On the (practically impossible) event that the plan fails to serialize, the
/// checksum degrades to a sentinel that will never match a real one — apply then
/// rejects the artifact rather than silently skipping the check.
fn resolved_plan_integrity(plan: &ResolvedRunPlan) -> String {
    use xxhash_rust::xxh3::xxh3_64;
    let encoded = serde_json::to_value(plan).and_then(|mut v| {
        canonicalize_value(&mut v);
        serde_json::to_vec(&v)
    });
    match encoded {
        Ok(bytes) => format!("xxh3:{:016x}", xxh3_64(&bytes)),
        Err(e) => {
            log::error!(
                "plan integrity checksum: failed to canonicalize resolved_plan ({e}); \
                 emitting a non-matching value so apply rejects this artifact"
            );
            "xxh3:unserializable".to_string()
        }
    }
}

/// Recursively rewrite every JSON object in `value` into key-sorted order so the
/// serialized byte stream is independent of map iteration order.  Required
/// because `serde_json/preserve_order` is active crate-wide (see
/// [`resolved_plan_integrity`]); without it a `serde_json::Value::Object` keeps
/// the (process-randomized) `HashMap` insertion order and the checksum would not
/// be reproducible across the plan/apply process boundary.
fn canonicalize_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            // Reinsert entries in sorted-key order. `serde_json::Map` is an
            // `IndexMap` under `preserve_order`, so iteration order == insertion
            // order; draining into a sorted `Vec` and reinserting fixes it.
            let mut entries: Vec<(String, serde_json::Value)> =
                map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            map.clear();
            for (k, mut v) in entries {
                canonicalize_value(&mut v);
                map.insert(k, v);
            }
        }
        serde_json::Value::Array(items) => {
            // Arrays are positional (e.g. chunk_ranges, warnings) — order is
            // semantically meaningful, so only recurse, never reorder.
            for item in items {
                canonicalize_value(item);
            }
        }
        _ => {}
    }
}

/// Generate a random 32-char lowercase hex plan identifier.
///
/// Uses `rand::random::<u128>()` which is available via the existing `rand`
/// dependency; avoids a UUID crate dependency.
fn new_plan_id() -> String {
    let hi = rand::random::<u64>();
    let lo = rand::random::<u64>();
    format!("{:016x}{:016x}", hi, lo)
}

/// Format an integer with thousands separators.
fn format_number(n: usize) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

/// Format a row count with thousands separators (US locale style).
fn format_rows(n: i64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, MetaColumns, SourceConfig,
        SourceType,
    };
    use crate::plan::{ChunkedPlan, ExtractionStrategy};
    use crate::tuning::SourceTuning;

    fn minimal_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "orders".into(),
            base_query: "SELECT * FROM orders".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::Zstd,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".into()),
                ..Default::default()
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced (default)".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://localhost/test".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                environment: None,
                tuning: None,
                tls: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 2.0,
            parquet: None,
        }
    }

    fn minimal_artifact() -> PlanArtifact {
        PlanArtifact::new(
            "orders".into(),
            "full".into(),
            String::new(),
            minimal_plan(),
            ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot: None,
                row_estimate: Some(42),
            },
            PlanDiagnostics {
                verdict: "Efficient".into(),
                warnings: vec![],
                recommended_profile: "balanced".into(),
                strategy_rationale: String::new(),
            },
        )
    }

    #[test]
    fn round_trip_json() {
        let artifact = minimal_artifact();
        let json = artifact.to_json_pretty().unwrap();
        let restored: PlanArtifact = PlanArtifact::from_json(&json).unwrap();
        assert_eq!(restored.export_name, artifact.export_name);
        assert_eq!(restored.plan_id, artifact.plan_id);
        assert_eq!(restored.computed.row_estimate, Some(42));
        assert_eq!(restored.diagnostics.verdict, "Efficient");
    }

    #[test]
    fn strategy_rationale_serializes_and_round_trips() {
        // The strategy narrative must reach the JSON artifact verbatim so a
        // machine consumer can read *why* the plan chose its strategy, and it
        // must survive the plan→apply round trip unchanged.
        let mut artifact = minimal_artifact();
        let why = "Mode full: ~42 rows, below the threshold. Risk — resumable: no.";
        artifact.diagnostics.strategy_rationale = why.to_string();

        let json = artifact.to_json_pretty().unwrap();
        assert!(
            json.contains("strategy_rationale"),
            "the rationale field must appear in the JSON: {json}"
        );
        assert!(
            json.contains("Mode full"),
            "the rationale text must appear in the JSON: {json}"
        );

        let restored = PlanArtifact::from_json(&json).unwrap();
        assert_eq!(
            restored.diagnostics.strategy_rationale, why,
            "rationale must survive the round trip unchanged"
        );
    }

    #[test]
    fn legacy_artifact_without_rationale_deserializes_to_empty() {
        // A pre-existing artifact JSON (no `strategy_rationale` key) must still
        // load — `#[serde(default)]` fills an empty string rather than failing.
        let mut artifact = minimal_artifact();
        artifact.diagnostics.strategy_rationale = "should be dropped".into();
        let mut value: serde_json::Value =
            serde_json::from_str(&artifact.to_json_pretty().unwrap()).unwrap();
        // Simulate an older artifact by removing the field entirely.
        value["diagnostics"]
            .as_object_mut()
            .unwrap()
            .remove("strategy_rationale");
        let legacy_json = serde_json::to_string(&value).unwrap();
        assert!(!legacy_json.contains("strategy_rationale"));

        let restored = PlanArtifact::from_json(&legacy_json)
            .expect("legacy artifact without the rationale field must deserialize");
        assert_eq!(
            restored.diagnostics.strategy_rationale, "",
            "missing rationale must default to empty, not fail"
        );
    }

    #[test]
    fn round_trip_chunked() {
        let mut plan = minimal_plan();
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 50_000,
            chunk_count: None,
            parallel: 4,
            dense: false,
            by_days: None,
            checkpoint: true,
            max_attempts: 3,
        });
        let artifact = PlanArtifact::new(
            "orders".into(),
            "chunked".into(),
            "abc123".into(),
            plan,
            ComputedPlanData {
                chunk_ranges: vec![(1, 50000), (50001, 100000)],
                chunk_count: 2,
                cursor_snapshot: None,
                row_estimate: Some(100000),
            },
            PlanDiagnostics {
                verdict: "Acceptable".into(),
                warnings: vec!["sparse range".into()],
                recommended_profile: "balanced".into(),
                strategy_rationale: String::new(),
            },
        );
        let json = artifact.to_json_compact().unwrap();
        let restored = PlanArtifact::from_json(&json).unwrap();
        assert_eq!(
            restored.computed.chunk_ranges,
            vec![(1, 50000), (50001, 100000)]
        );
        assert_eq!(restored.computed.chunk_count, 2);
        assert_eq!(restored.plan_fingerprint, "abc123");
    }

    #[test]
    fn staleness_fresh() {
        let artifact = minimal_artifact();
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert_eq!(result, StalenessCheck::Fresh);
    }

    #[test]
    fn staleness_warn_between_thresholds() {
        let mut artifact = minimal_artifact();
        // Backdate to 2 hours ago: past warn (1h) but before error (24h).
        artifact.created_at = Utc::now() - Duration::hours(2);
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert!(
            matches!(result, StalenessCheck::StaleWarn(_)),
            "expected StaleWarn, got {result:?}"
        );
    }

    #[test]
    fn staleness_expired_artifact() {
        let mut artifact = minimal_artifact();
        // Backdate creation to 25 hours ago
        artifact.created_at = Utc::now() - Duration::hours(25);
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert!(matches!(result, StalenessCheck::StaleError(_)));
    }

    #[test]
    fn staleness_exactly_at_warn_threshold() {
        let mut artifact = minimal_artifact();
        // Exactly 1 hour ago → StaleWarn (age >= warn_after).
        artifact.created_at = Utc::now() - Duration::hours(1) - Duration::seconds(1);
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert!(matches!(result, StalenessCheck::StaleWarn(_)));
    }

    #[test]
    fn staleness_exactly_at_error_threshold() {
        let mut artifact = minimal_artifact();
        // Exactly 24h + 1s → StaleError.
        artifact.created_at = Utc::now() - Duration::hours(24) - Duration::seconds(1);
        let result = artifact.staleness(Duration::hours(1), Duration::hours(24));
        assert!(matches!(result, StalenessCheck::StaleError(_)));
    }

    #[test]
    fn cursor_matches_none_snapshot() {
        let artifact = minimal_artifact();
        // No cursor snapshot → always matches
        assert!(artifact.cursor_matches(None));
        assert!(artifact.cursor_matches(Some("anything")));
    }

    #[test]
    fn cursor_matches_incremental() {
        let mut artifact = minimal_artifact();
        artifact.computed.cursor_snapshot = Some("2026-01-01T00:00:00Z".into());
        assert!(artifact.cursor_matches(Some("2026-01-01T00:00:00Z")));
        assert!(!artifact.cursor_matches(Some("2026-01-02T00:00:00Z")));
        assert!(!artifact.cursor_matches(None));
    }

    #[test]
    fn format_rows_basic() {
        assert_eq!(super::format_rows(1000), "1,000");
        assert_eq!(super::format_rows(1_000_000), "1,000,000");
        assert_eq!(super::format_rows(42), "42");
    }

    // ─── ADR-0005 PA9 — artifact credential redaction (end-to-end) ──────

    #[test]
    fn artifact_strips_plaintext_password_from_source() {
        let mut plan = minimal_plan();
        plan.source.password = Some("s3cret!".into());
        let artifact = PlanArtifact::new(
            "orders".into(),
            "full".into(),
            String::new(),
            plan,
            ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot: None,
                row_estimate: None,
            },
            PlanDiagnostics {
                verdict: "Efficient".into(),
                warnings: vec![],
                recommended_profile: "balanced".into(),
                strategy_rationale: String::new(),
            },
        );
        assert_eq!(
            artifact.resolved_plan.source.password, None,
            "plaintext password must never leave the process"
        );
        let json = artifact.to_json_pretty().unwrap();
        assert!(
            !json.contains("s3cret"),
            "secret must not appear anywhere in the JSON"
        );
    }

    #[test]
    fn artifact_strips_credentials_from_url() {
        let mut plan = minimal_plan();
        plan.source.url = Some("postgresql://rivet:s3cret@db.example.com/prod".into());
        let artifact = PlanArtifact::new(
            "orders".into(),
            "full".into(),
            String::new(),
            plan,
            ComputedPlanData {
                chunk_ranges: vec![],
                chunk_count: 0,
                cursor_snapshot: None,
                row_estimate: None,
            },
            PlanDiagnostics {
                verdict: "Efficient".into(),
                warnings: vec![],
                recommended_profile: "balanced".into(),
                strategy_rationale: String::new(),
            },
        );
        let url = artifact.resolved_plan.source.url.as_deref().unwrap();
        assert!(
            !url.contains("s3cret"),
            "password must not remain in URL: {url}"
        );
        assert!(url.contains("REDACTED@db.example.com/prod"));
    }

    // ─── ADR-0005 PA10 — resolved-plan integrity seal (finding #16) ──────

    #[test]
    fn integrity_seal_accepts_untouched_artifact() {
        // A plan straight out of `PlanArtifact::new` must verify cleanly — the
        // seal is computed over exactly the bytes that get serialized.
        let artifact = minimal_artifact();
        assert!(
            !artifact.integrity.is_empty(),
            "new artifact must be sealed"
        );
        artifact
            .verify_integrity()
            .expect("untouched artifact must pass integrity check");

        // Round-tripping through JSON (the real plan/apply boundary) must not
        // disturb the seal.
        let json = artifact.to_json_pretty().unwrap();
        let restored = PlanArtifact::from_json(&json).unwrap();
        restored
            .verify_integrity()
            .expect("round-tripped artifact must still verify");
    }

    #[test]
    fn integrity_seal_rejects_tampered_base_query() {
        // Mirror the live RED test (audit_apply_rejects_tampered_plan) at the
        // unit level: edit base_query after planning → verify_integrity must
        // fail with the documented message so apply exits non-zero.
        let mut artifact = minimal_artifact();
        artifact.resolved_plan.base_query = "SELECT * FROM users".into();
        let err = artifact.verify_integrity().unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("integrity check failed") && msg.contains("modified after planning"),
            "tamper must be reported with the PA10 message, got: {msg}"
        );
    }

    #[test]
    fn integrity_seal_rejects_tamper_in_any_execution_field() {
        // Not just base_query — any execution-affecting edit must be caught.
        let mut artifact = minimal_artifact();
        artifact.resolved_plan.destination.path = Some("/etc/evil".into());
        assert!(
            artifact.verify_integrity().is_err(),
            "editing the destination path must be detected as tampering"
        );
    }

    #[test]
    fn integrity_seal_legacy_empty_is_accepted_with_warning() {
        // Back-compat: a pre-PA10 artifact has an empty seal. It cannot be
        // checked, but failing closed would break apply on every older plan
        // file — so it is accepted (the WARN is emitted, see verify_integrity).
        let mut artifact = minimal_artifact();
        artifact.integrity = String::new();
        artifact
            .verify_integrity()
            .expect("unsealed (legacy) artifact must be accepted, not failed closed");
    }

    #[test]
    fn integrity_checksum_is_not_tamper_protection() {
        // V17 (CWE-345 honesty): the checksum is an UNKEYED hash stored beside
        // the data it covers, so a writer who edits the plan can recompute a
        // matching value for free — this catches *accidental* edits only, never
        // a *malicious* one. Document that contract here so nobody mistakes the
        // field for a security MAC: edit the plan, recompute via the same
        // function apply uses, and verify_integrity then passes.
        let mut artifact = minimal_artifact();
        artifact.resolved_plan.base_query = "SELECT * FROM users".into();
        // A real tamper seal would still reject this; an unkeyed checksum cannot,
        // because the "attacker" simply recomputes it from the edited plan.
        artifact.integrity = resolved_plan_integrity(&artifact.resolved_plan);
        artifact.verify_integrity().expect(
            "an unkeyed checksum cannot detect a deliberate edit + recompute — \
             this is accidental-corruption detection, not tamper protection",
        );
    }

    #[test]
    fn integrity_seal_distinguishes_a_real_override_change() {
        // The seal must change when column_overrides genuinely change — a single
        // entry is order-free, so this is robust regardless of HashMap seeding.
        let mut plan_a = minimal_plan();
        plan_a.column_overrides = [("alpha".to_string(), crate::types::RivetType::String)]
            .into_iter()
            .collect();
        let mut plan_b = minimal_plan();
        plan_b.column_overrides = [("alpha".to_string(), crate::types::RivetType::Int64)]
            .into_iter()
            .collect();
        assert_ne!(
            resolved_plan_integrity(&plan_a),
            resolved_plan_integrity(&plan_b),
            "a real override type change must change the seal"
        );
    }

    #[test]
    fn canonicalize_makes_seal_independent_of_object_key_order() {
        // Regression for the canonicalization bug. The seal is written by `rivet
        // plan` and re-verified by `rivet apply` in a *different* process; the
        // plan embeds std `HashMap`s (`column_overrides`, `quality.null_ratio_max`)
        // whose iteration order is randomized per process. With
        // `serde_json/preserve_order` active crate-wide, `serde_json::to_value`
        // keeps that randomized order, so without canonicalization the recomputed
        // seal would not match across the process boundary.
        //
        // We exercise the mechanism deterministically (not at the mercy of the
        // per-process HashMap seed): build two JSON objects with the SAME entries
        // in DIFFERENT insertion order and assert the canonicalized bytes — and
        // therefore the hash — are identical. This is precisely what differs
        // between the plan and apply processes for a multi-key override map.
        let mut a = serde_json::json!({});
        let mut b = serde_json::json!({});
        if let (serde_json::Value::Object(ma), serde_json::Value::Object(mb)) = (&mut a, &mut b) {
            for k in ["zeta", "alpha", "mu"] {
                ma.insert(k.to_string(), serde_json::json!(k));
            }
            for k in ["mu", "zeta", "alpha"] {
                mb.insert(k.to_string(), serde_json::json!(k));
            }
        }
        // After canonicalization the byte encodings must be identical
        // regardless of insertion order — that equality is the contract.
        canonicalize_value(&mut a);
        canonicalize_value(&mut b);
        assert_eq!(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap(),
            "canonicalized bytes must not depend on object key insertion order"
        );
    }

    #[test]
    fn canonicalize_value_sorts_nested_keys_preserves_arrays() {
        // Direct check of the canonicalizer: object keys sorted recursively,
        // array element order untouched.
        let mut v = serde_json::json!({
            "b": 1,
            "a": { "y": [3, 2, 1], "x": 0 },
        });
        canonicalize_value(&mut v);
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, r#"{"a":{"x":0,"y":[3,2,1]},"b":1}"#);
    }
}
