//! **Layer: Coordinator helper** (destination prefix templating)
//!
//! Single source of truth for `{date}`/`{export}`/`{table}`/`{run_id}`
//! substitution in `destination.path` and `destination.prefix`.
//!
//! Every command that touches a destination prefix — `run`, `doctor`,
//! `validate`, `reconcile`, `repair`, `plan` — MUST resolve placeholders
//! through this module so they all look at the same physical bytes.
//!
//! Why this exists as its own module (ADR carveout for v0.7.2):
//! the v0.7.1 lineage carried two divergent substitution paths
//! (`plan::build::apply_destination_placeholders` and inline `replace()`
//! calls scattered through validate/doctor wiring).  The v0.7.2 cloud-landing
//! pass makes the resolver explicit and overrideable so `validate --date`
//! and `validate --run-id` can re-target an *earlier* run's prefix without
//! re-running the export.
//!
//! Supported placeholders:
//! - `{date}`   → context date, formatted `YYYY-MM-DD` (UTC by default).
//! - `{export}` → export name.
//! - `{table}`  → alias for `{export}`.
//! - `{run_id}` → run identifier; substituted only when the context carries
//!   one.  An unsubstituted `{run_id}` is left verbatim so a downstream
//!   open / head-object call fails loudly instead of silently pointing at
//!   a wrong prefix.
//! - `{partition}` → Hive `col=value` segment for value-based partitioning.
//!   Resolved out-of-band by [`expand_destination_partition`] during `run`'s
//!   partition expansion, not by the [`PlaceholderContext`] pass below.

use chrono::{NaiveDate, Utc};

use crate::config::DestinationConfig;

/// Substitution context shared by every command that resolves destination
/// prefixes.
#[derive(Debug, Clone)]
pub struct PlaceholderContext {
    /// Date used for `{date}` substitution.
    pub date: NaiveDate,
    /// Export name used for `{export}` and `{table}`.
    pub export_name: String,
    /// Optional run identifier used for `{run_id}`.  `None` leaves the
    /// literal `{run_id}` token in place.
    pub run_id: Option<String>,
}

impl PlaceholderContext {
    /// Context anchored at today's UTC date.  Used by `run` and the
    /// default branch of `doctor`/`validate` when the operator does not
    /// override the resolution target.
    pub fn for_today(export_name: impl Into<String>) -> Self {
        Self {
            date: Utc::now().date_naive(),
            export_name: export_name.into(),
            run_id: None,
        }
    }

    /// Context anchored at an explicit date.  Used by
    /// `validate --date YYYY-MM-DD` to re-check a run that landed on
    /// a prior day's prefix.
    pub fn for_date(date: NaiveDate, export_name: impl Into<String>) -> Self {
        Self {
            date,
            export_name: export_name.into(),
            run_id: None,
        }
    }

    /// Attach a run identifier for `{run_id}` substitution.
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }
}

/// Apply placeholder substitution to a single string.
///
/// Unrecognised `{token}` patterns are preserved verbatim — a missing
/// `{run_id}` is left intact so the eventual destination open fails fast
/// rather than silently aliasing to an unintended prefix.
pub fn apply(s: &str, ctx: &PlaceholderContext) -> String {
    let date = ctx.date.format("%Y-%m-%d").to_string();
    let mut out = s
        .replace("{date}", &date)
        .replace("{export}", &ctx.export_name)
        .replace("{table}", &ctx.export_name);
    if let Some(rid) = &ctx.run_id {
        out = out.replace("{run_id}", rid);
    }
    out
}

/// Substitute **only** the `{partition}` token in `destination.path` /
/// `destination.prefix` with a Hive-style `col=value` segment.
///
/// This is a targeted pre-pass owned by `pipeline::partition_expand`: when
/// `partition_by` is set, `run` expands one export into one concrete export
/// per bucket and resolves `{partition}` here, *before* the normal plan-build
/// pass resolves `{date}` / `{run_id}` / `{export}`. Keeping it separate avoids
/// resolving the date twice and leaves every other token untouched.
pub fn expand_destination_partition(
    mut dest: DestinationConfig,
    segment: &str,
) -> DestinationConfig {
    dest.path = dest.path.map(|s| s.replace("{partition}", segment));
    dest.prefix = dest.prefix.map(|s| s.replace("{partition}", segment));
    dest
}

/// Expand placeholders in `destination.path` and `destination.prefix`.
///
/// All other destination fields are passed through unchanged.
pub fn expand_destination(
    mut dest: DestinationConfig,
    ctx: &PlaceholderContext,
) -> DestinationConfig {
    dest.path = dest.path.map(|s| apply(&s, ctx));
    dest.prefix = dest.prefix.map(|s| apply(&s, ctx));
    dest
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DestinationType;
    use chrono::NaiveDate;

    fn ctx_on(date_str: &str, export: &str) -> PlaceholderContext {
        PlaceholderContext::for_date(
            NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap(),
            export,
        )
    }

    // ── apply() ────────────────────────────────────────────────────────────

    #[test]
    fn apply_substitutes_date_export_table() {
        let ctx = ctx_on("2026-05-21", "orders");
        assert_eq!(
            apply("runs/{date}/{export}/{table}", &ctx),
            "runs/2026-05-21/orders/orders",
        );
    }

    #[test]
    fn apply_leaves_unknown_tokens_verbatim() {
        let ctx = ctx_on("2026-05-21", "orders");
        // Unknown tokens must round-trip — a typo is better surfaced as
        // "object not found" than silently dropped.
        assert_eq!(
            apply("runs/{date}/{unknown}/", &ctx),
            "runs/2026-05-21/{unknown}/",
        );
    }

    #[test]
    fn apply_run_id_only_when_set() {
        let ctx = ctx_on("2026-05-21", "orders");
        // No run_id on the context → literal `{run_id}` preserved.
        assert_eq!(apply("runs/{run_id}/", &ctx), "runs/{run_id}/");

        let ctx = ctx.with_run_id("r-abc123");
        assert_eq!(apply("runs/{run_id}/", &ctx), "runs/r-abc123/");
    }

    #[test]
    fn apply_is_idempotent_on_already_expanded_string() {
        // The v0.7.0/v0.7.1 wiring already calls the resolver from
        // `validate` and `doctor`; both run against config that `run` may
        // have just expanded.  Second pass must be a no-op.
        let ctx = ctx_on("2026-05-21", "orders");
        let once = apply("runs/{date}/{export}/", &ctx);
        let twice = apply(&once, &ctx);
        assert_eq!(once, twice);
        assert_eq!(once, "runs/2026-05-21/orders/");
    }

    // ── expand_destination() ───────────────────────────────────────────────

    #[test]
    fn expand_destination_rewrites_path_and_prefix() {
        let dest = DestinationConfig {
            destination_type: DestinationType::S3,
            prefix: Some("exports/{date}/{export}/".into()),
            path: Some("/scratch/{table}/{date}".into()),
            ..Default::default()
        };
        let ctx = ctx_on("2026-05-21", "orders");
        let expanded = expand_destination(dest, &ctx);
        assert_eq!(
            expanded.prefix.as_deref(),
            Some("exports/2026-05-21/orders/")
        );
        assert_eq!(expanded.path.as_deref(), Some("/scratch/orders/2026-05-21"));
    }

    #[test]
    fn expand_destination_no_placeholders_unchanged() {
        let dest = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("./out".into()),
            ..Default::default()
        };
        let ctx = ctx_on("2026-05-21", "orders");
        let expanded = expand_destination(dest, &ctx);
        assert_eq!(expanded.path.as_deref(), Some("./out"));
        assert!(expanded.prefix.is_none());
    }

    #[test]
    fn expand_destination_with_run_id() {
        let dest = DestinationConfig {
            destination_type: DestinationType::S3,
            prefix: Some("runs/{date}/{run_id}/{export}/".into()),
            ..Default::default()
        };
        let ctx = ctx_on("2026-05-21", "orders").with_run_id("r-abc123");
        let expanded = expand_destination(dest, &ctx);
        assert_eq!(
            expanded.prefix.as_deref(),
            Some("runs/2026-05-21/r-abc123/orders/"),
        );
    }
}
