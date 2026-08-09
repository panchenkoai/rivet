//! First-run density probe (#148): sample the chunk key instead of believing
//! the catalog's row estimate.
//!
//! The lie, measured in the field (2026-08-07): a ~300M-row versioned table's
//! `TABLE_ROWS` said ~332M; the run had read 835.7M rows by chunk 2853/3320 —
//! the catalog under-counted ~2.5×, and every decision init makes (the 100K
//! full→chunked threshold, parallel scaling, sparsity, duration prediction)
//! stood on that number. This module is the pure math half; the per-engine
//! probes live beside each engine's introspection (the connection is theirs).
//!
//! Method: `MIN(k)`/`MAX(k)` (two index-boundary seeks), then K windows of
//! width W **evenly spaced across the span** — stratified, not random, because
//! versioned tables densify toward fresh ids and a uniform grid averages that
//! honestly. `rows ≈ span × Σcount/(K×W)`; `density = Σcount/(K×W)` falls out
//! free and answers keyset-vs-range + rows-per-key questions the catalog
//! structurally cannot.

/// How a snapshot's row figure was obtained — recorded so the decision is
/// auditable and the next `plan` can see how far the catalog was off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EstimateMethod {
    /// Below the triage line — the catalog figure is accepted as-is (no
    /// strategy boundary is near; a catalog error changes nothing).
    CatalogTriaged,
    /// The engine's catalog figure is near-exact by construction
    /// (MSSQL `dm_db_partition_stats`).
    CatalogExact,
    /// The stratified index probe ran; the snapshot's rows are sampled.
    Probed,
    /// Small table with no usable integer key — an honest `COUNT(*)`.
    Counted,
    /// Large table with no usable key/index: the catalog figure is kept but
    /// the snapshot says so — never silently trusted.
    Unverified,
}

impl EstimateMethod {
    pub(crate) fn label(self) -> &'static str {
        match self {
            EstimateMethod::CatalogTriaged => "catalog-triaged",
            EstimateMethod::CatalogExact => "catalog-exact",
            EstimateMethod::Probed => "probed",
            EstimateMethod::Counted => "counted",
            EstimateMethod::Unverified => "unverified",
        }
    }
}

/// One probe's outcome, carried on `TableInfo` and recorded in the snapshot.
#[derive(Debug, Clone)]
pub(crate) struct DensityProbe {
    /// The estimate init's decisions now stand on.
    pub rows: i64,
    /// Rows per key-unit across the sampled windows (1.0 = perfectly dense
    /// autoincrement; >1 = versioned key (N rows per key value); <1 = gappy).
    pub density: f64,
    pub method: EstimateMethod,
    /// The catalog's original figure, kept for the audit trail.
    pub catalog_rows: i64,
    pub k: usize,
    pub w: i64,
}

/// Tables claimed below this are candidates for tier-0 trust — but the claim
/// must also survive the SPAN check (see [`span_agrees_with_claim`]).
pub(crate) const TRIAGE_ROWS: i64 = 50_000;

/// Tier-0's honesty gate, pure for the mutation gate. `TABLE_ROWS` and even
/// `DATA_LENGTH` come from the SAME frozen statistics (this feature's own live
/// fixture proved both lie together: stats frozen at 1 000 rows on a 148 000-
/// row table). The one cheap signal that is NOT statistics is the key's
/// MIN/MAX span — two index-boundary seeks. A genuinely small dense-ish table
/// has span ≈ claim; a span far past the claim (×4, floored at 1 000 so tiny
/// tables with a few gaps don't false-probe) means the claim cannot be
/// trusted and the full probe runs. Probing a truly-small table is harmless
/// (the window grid clamps), so false positives cost milliseconds.
pub(crate) fn span_agrees_with_claim(catalog_rows: i64, span: i64) -> bool {
    span <= catalog_rows.saturating_mul(4).max(1_000)
}
/// Default probe shape: ~1–2 s per table on a real index at any size.
pub(crate) const PROBE_K: usize = 50;
pub(crate) const PROBE_W: i64 = 10_000;

/// The K stratified window starts over `[min, max]`, each window `w` wide —
/// evenly spaced so the grid covers the WHOLE span (first at `min`, last
/// ending at/near `max`). Returns fewer (possibly one) windows when the span
/// is smaller than `k × w`; `w` is never widened (the caller's COUNT cost
/// bound is `k × w` rows, kept honest).
pub(crate) fn stratified_offsets(min: i64, max: i64, k: usize, w: i64) -> Vec<i64> {
    let span = max.saturating_sub(min).saturating_add(1);
    if span <= 0 || k == 0 || w <= 0 {
        return Vec::new();
    }
    if span <= w.saturating_mul(k as i64) {
        // Tiny span: contiguous windows cover it exactly — no gaps, no overlap.
        let n = ((span + w - 1) / w).max(1);
        return (0..n).map(|i| min + i * w).collect();
    }
    // Even spacing: window i starts at min + round(i × (span − w) / (k − 1)),
    // so the first window starts at min and the last ENDS at max.
    let k_i = k as i64;
    (0..k_i)
        .map(|i| {
            if k_i == 1 {
                min
            } else {
                min + ((span - w) as f64 * i as f64 / (k_i - 1) as f64).round() as i64
            }
        })
        .collect()
}

/// `rows ≈ span × Σcount/(sampled key-units)`; density = mean rows per
/// key-unit. `counts[i]` is `COUNT(*) WHERE k BETWEEN off[i] AND off[i]+w-1`.
/// Empty windows are DATA (a gappy key), not absence — they stay in the mean.
pub(crate) fn estimate_from_windows(min: i64, max: i64, counts: &[i64], w: i64) -> (i64, f64) {
    let span = max.saturating_sub(min).saturating_add(1);
    if counts.is_empty() || w <= 0 || span <= 0 {
        return (0, 0.0);
    }
    let sampled_units = (counts.len() as i64).saturating_mul(w).min(span);
    let total: i64 = counts.iter().sum();
    let density = total as f64 / sampled_units as f64;
    ((span as f64 * density).round() as i64, density)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The grid covers the WHOLE span: first window at min, last window's end
    /// reaches max — a grid that stops short would systematically miss the
    /// fresh-id end where versioned tables densify (the field bias). RED
    /// against `span` instead of `span − w` in the spacing.
    #[test]
    fn stratified_offsets_cover_the_whole_span() {
        let (min, max, k, w) = (1_000i64, 1_000_000i64, 50usize, 10_000i64);
        let offs = stratified_offsets(min, max, k, w);
        assert_eq!(offs.len(), k);
        assert_eq!(offs[0], min, "first window starts at MIN");
        assert_eq!(
            offs[k - 1] + w - 1,
            max,
            "last window must END at MAX (fresh-id end sampled)"
        );
        // Monotonic, no window past the span.
        for pair in offs.windows(2) {
            assert!(pair[0] < pair[1], "offsets strictly increase");
        }
    }

    /// A span smaller than K×W degrades to contiguous FULL coverage, never a
    /// widened W (the COUNT cost bound stays k×w rows).
    #[test]
    fn stratified_offsets_clamp_on_a_tiny_span() {
        let offs = stratified_offsets(10, 109, 50, 25); // span 100, k*w = 1250
        assert_eq!(offs, vec![10, 35, 60, 85], "contiguous cover, no widening");
        assert!(stratified_offsets(5, 5, 3, 10).len() == 1);
        assert!(stratified_offsets(9, 5, 3, 10).is_empty(), "inverted span");
    }

    /// The estimator: dense key → density ~1.0, rows ≈ span; versioned key
    /// (3 rows per key) → density ~3; gappy (10% occupancy) → ~0.1. RED
    /// against dropping empty windows from the mean (which would inflate a
    /// gappy key's estimate ~10×) and against Σ/K instead of Σ/(K×W).
    #[test]
    fn estimate_from_windows_dense_versioned_and_gappy() {
        let w = 1_000i64;
        // dense: every window full
        let (rows, d) = estimate_from_windows(1, 1_000_000, &[w; 50], w);
        assert!((d - 1.0).abs() < 1e-9);
        assert_eq!(rows, 1_000_000);
        // versioned: 3 rows per key value
        let (rows, d) = estimate_from_windows(1, 1_000_000, &[3 * w; 50], w);
        assert!((d - 3.0).abs() < 1e-9);
        assert_eq!(rows, 3_000_000);
        // gappy: 10% of keys exist — EMPTY windows count in the mean
        let mut counts = vec![0i64; 45];
        counts.extend(vec![w; 5]);
        let (rows, d) = estimate_from_windows(1, 1_000_000, &counts, w);
        assert!((d - 0.1).abs() < 1e-9, "empty windows are data: {d}");
        assert_eq!(rows, 100_000);
    }

    /// The span gate (RED against rows-only triage — the live fixture's exact
    /// escape: 1 000 claimed rows over a 51 000-wide key span → must probe;
    /// and RED against dropping the ×4 slack or the 1 000 floor).
    #[test]
    fn span_gate_catches_a_small_claim_over_a_wide_span() {
        assert!(span_agrees_with_claim(1_000, 1_000), "dense small table");
        assert!(
            span_agrees_with_claim(1_000, 3_999),
            "modest gaps tolerated"
        );
        assert!(
            !span_agrees_with_claim(1_000, 51_000),
            "the frozen-stats fixture shape MUST probe"
        );
        assert!(
            span_agrees_with_claim(100, 900),
            "the 1 000 floor for tiny tables"
        );
        assert!(!span_agrees_with_claim(100, 1_500));
    }

    #[test]
    fn estimate_degenerate_inputs_are_zero_not_panic() {
        assert_eq!(estimate_from_windows(1, 100, &[], 10), (0, 0.0));
        assert_eq!(estimate_from_windows(1, 100, &[5], 0), (0, 0.0));
        assert_eq!(estimate_from_windows(100, 1, &[5], 10), (0, 0.0));
    }
}
