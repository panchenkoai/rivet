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
    /// The fully-resolved execution plan embedded verbatim.
    pub resolved_plan: ResolvedRunPlan,
    /// Data computed at plan time that can be replayed at apply time.
    pub computed: ComputedPlanData,
    /// Preflight diagnostics captured at plan time.
    pub diagnostics: PlanDiagnostics,
    /// Advisory source-aware prioritization (ADR-0006). Does not affect execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prioritization: Option<PlanPrioritizationSnapshot>,
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

        let created_at = Utc::now();
        Self {
            rivet_version: env!("CARGO_PKG_VERSION").to_string(),
            plan_id: new_plan_id(),
            created_at,
            expires_at: created_at + Duration::hours(24),
            export_name,
            strategy,
            plan_fingerprint,
            resolved_plan,
            computed,
            diagnostics,
            prioritization: None,
        }
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
        Self::from_json(&content)
            .map_err(|e| anyhow::anyhow!("invalid plan file '{}': {}", path, e))
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

        // destination
        let dest = &self.resolved_plan.destination;
        let dest_label = dest.destination_type.label();
        let dest_location = dest
            .path
            .as_deref()
            .or(dest.bucket.as_deref())
            .unwrap_or("(default)");
        println!("  Output   : {} → {}", dest_label, dest_location);

        // format / compression
        let fmt = self.resolved_plan.format.label();
        let comp = self.resolved_plan.compression.label();
        println!("  Format   : {} + {}", fmt, comp);
        println!();
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
                bucket: None,
                prefix: None,
                path: Some("./out".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
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
                tuning: None,
                tls: None,
            },
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
    fn round_trip_chunked() {
        let mut plan = minimal_plan();
        plan.strategy = ExtractionStrategy::Chunked(ChunkedPlan {
            column: "id".into(),
            chunk_size: 50_000,
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
    fn staleness_expired_artifact() {
        let mut artifact = minimal_artifact();
        // Backdate creation to 25 hours ago
        artifact.created_at = Utc::now() - Duration::hours(25);
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
            },
        );
        let url = artifact.resolved_plan.source.url.as_deref().unwrap();
        assert!(
            !url.contains("s3cret"),
            "password must not remain in URL: {url}"
        );
        assert!(url.contains("REDACTED@db.example.com/prod"));
    }
}
