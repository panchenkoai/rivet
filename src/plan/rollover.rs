//! Keeping one output file inside a load job's partition budget.
//!
//! BigQuery writes at most 4,000 partitions per load job, and nothing splits a single
//! Parquet file at load time — so a file whose rows land in more partitions than that
//! cannot be loaded at all, at any granularity the operator actually wants. The extract
//! is the only place that can prevent it: the sink already streams every row, so it can
//! count the partitions the current part would touch and start a new part before the
//! budget is spent.
//!
//! The measure is the number of DISTINCT partitions the rows occupy, which is what the
//! load job writes — not the span between the lowest and highest value. A file holding
//! 4,000 scattered days across twenty years fits one job; its span does not.
//!
//! This module is the pure half: bucket identity and the fitting decision. The sink owns
//! the Arrow decode and the rotation itself.

use std::collections::HashSet;

use chrono::{DateTime, Datelike};

use crate::config::load::Granularity;

/// The partition bucket rows with a NULL key occupy. The warehouse gives them a
/// partition of their own, so they cost budget like any other — but they share ONE
/// bucket however many there are, which this sentinel reproduces.
pub const NULL_BUCKET: i64 = i64::MIN;

/// What the extract must know to keep a part loadable: the column the warehouse
/// partitions by, at what granularity, and how many partitions one load job may write.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionRollover {
    pub column: String,
    pub granularity: Granularity,
    pub cap: usize,
}

/// How a partition column's values are stored, for the conversion to epoch seconds.
/// Named here rather than in the sink so the arithmetic below can be graded offline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionUnit {
    Days,
    Seconds,
    Millis,
    Micros,
    Nanos,
}

/// A stored value as epoch seconds.
///
/// Sub-second units divide toward MINUS infinity, not toward zero: a timestamp before
/// 1970 truncated by a plain division lands in the following second, which at day
/// granularity is the following DAY — one row filed under the wrong partition.
pub fn to_epoch_seconds(value: i64, unit: PartitionUnit) -> i64 {
    match unit {
        PartitionUnit::Days => value.saturating_mul(86_400),
        PartitionUnit::Seconds => value,
        PartitionUnit::Millis => value.div_euclid(1_000),
        PartitionUnit::Micros => value.div_euclid(1_000_000),
        PartitionUnit::Nanos => value.div_euclid(1_000_000_000),
    }
}

/// The partition a value falls in, as a bucket id comparable within one granularity.
///
/// Hour and day divide the epoch; month and year are calendar buckets, so they go
/// through a civil date rather than fixed-width arithmetic.
pub fn bucket_of(secs: i64, granularity: Granularity) -> i64 {
    match granularity {
        Granularity::Hour => secs.div_euclid(3_600),
        Granularity::Day => secs.div_euclid(86_400),
        Granularity::Month => {
            civil(secs).map_or(0, |t| i64::from(t.year()) * 12 + i64::from(t.month0()))
        }
        Granularity::Year => civil(secs).map_or(0, |t| i64::from(t.year())),
    }
}

fn civil(secs: i64) -> Option<DateTime<chrono::Utc>> {
    DateTime::from_timestamp(secs, 0)
}

/// How many leading rows of `buckets` the current part can still take before it would
/// hold more than `cap` distinct partitions.
///
/// Returns `buckets.len()` when the whole batch fits, so the caller writes it whole and
/// does nothing. A row whose bucket the part already holds is free: it consumes no
/// budget, which is why a wide batch over a narrow date range never rotates.
pub fn rows_that_fit(held: &HashSet<i64>, buckets: &[i64], cap: usize) -> usize {
    // A zero budget is not a budget. Reporting that nothing fits would close the part,
    // leave the same rows still not fitting, and close the next one on them too — for
    // ever. An absent budget is expressed by not counting at all, so treat it as one.
    if cap == 0 {
        return buckets.len();
    }
    let mut added: HashSet<i64> = HashSet::new();
    for (row, bucket) in buckets.iter().enumerate() {
        if held.contains(bucket) || added.contains(bucket) {
            continue;
        }
        if held.len() + added.len() >= cap {
            return row;
        }
        added.insert(*bucket);
    }
    buckets.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(y: i32, m: u32, d: u32, h: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp()
    }

    /// Two values in the same calendar bucket are ONE partition, and the next one over
    /// is a different partition — per granularity. RED against a fixed-width month
    /// (30 days) or year (365 days), which drift off the calendar within one span.
    #[test]
    fn bucket_identity_follows_the_calendar_per_granularity() {
        let noon = at(2024, 3, 15, 12);
        let later = at(2024, 3, 15, 13);
        assert_eq!(
            bucket_of(noon, Granularity::Day),
            bucket_of(later, Granularity::Day)
        );
        assert_ne!(
            bucket_of(noon, Granularity::Hour),
            bucket_of(later, Granularity::Hour)
        );

        // A leap year: Feb 29 and Mar 1 are adjacent days but different months.
        let leap = at(2024, 2, 29, 0);
        let march = at(2024, 3, 1, 0);
        assert_eq!(
            bucket_of(march, Granularity::Day) - bucket_of(leap, Granularity::Day),
            1
        );
        assert_eq!(
            bucket_of(march, Granularity::Month) - bucket_of(leap, Granularity::Month),
            1
        );
        assert_eq!(bucket_of(leap, Granularity::Year), 2024);

        // December to January crosses both the month and the year bucket.
        let dec = at(2023, 12, 31, 23);
        let jan = at(2024, 1, 1, 0);
        assert_eq!(
            bucket_of(jan, Granularity::Month) - bucket_of(dec, Granularity::Month),
            1
        );
        assert_eq!(
            bucket_of(jan, Granularity::Year) - bucket_of(dec, Granularity::Year),
            1
        );
    }

    /// Values before the epoch keep going down rather than folding onto zero — a
    /// truncating division would put 1969-12-31 in the same day bucket as 1970-01-01.
    #[test]
    fn buckets_stay_ordered_before_the_epoch() {
        let before = at(1969, 12, 31, 12);
        let after = at(1970, 1, 1, 12);
        assert_eq!(
            bucket_of(after, Granularity::Day) - bucket_of(before, Granularity::Day),
            1
        );
        assert!(bucket_of(before, Granularity::Day) < 0);
    }

    /// A zero budget must not rotate. Without the guard every row "does not fit", so the
    /// caller closes a part, re-asks about the same rows, and closes another — the
    /// non-terminating shape that a missing `part_buckets.clear()` produced for real.
    /// RED against removing the guard: this returns 0 and the caller never progresses.
    #[test]
    fn a_zero_budget_never_rotates() {
        assert_eq!(rows_that_fit(&HashSet::new(), &[1, 2, 3], 0), 3);
    }

    /// The whole batch fits when it stays inside the budget, and nothing is rotated.
    #[test]
    fn a_batch_within_the_budget_fits_whole() {
        let held = HashSet::from([1, 2]);
        assert_eq!(rows_that_fit(&held, &[1, 2, 3, 3, 2], 10), 5);
    }

    /// The split lands on the row that would exceed the cap, not after it — the part
    /// is closed holding exactly `cap` partitions. RED against `>` instead of `>=`.
    #[test]
    fn the_split_row_is_the_one_that_would_exceed_the_cap() {
        let held = HashSet::from([10, 11]);
        // cap 4: buckets 12 and 13 fit (4 held), 14 would be the fifth.
        assert_eq!(rows_that_fit(&held, &[12, 13, 14, 15], 4), 2);
    }

    /// Repeats of a bucket the part already holds cost nothing, so a wide batch over a
    /// narrow range never rotates. RED against counting rows instead of distinct buckets.
    #[test]
    fn rows_repeating_a_held_bucket_consume_no_budget() {
        let held = HashSet::from([7]);
        let buckets = vec![7; 10_000];
        assert_eq!(rows_that_fit(&held, &buckets, 1), buckets.len());
    }

    /// A part already at the cap takes nothing further — the caller must rotate first,
    /// and a zero here is what tells it to. RED against returning 1 (writing one more row).
    #[test]
    fn a_full_part_accepts_no_new_bucket() {
        let held = HashSet::from([1, 2, 3]);
        assert_eq!(rows_that_fit(&held, &[4, 5], 3), 0);
        // …but a row landing in a bucket it already holds still fits.
        assert_eq!(rows_that_fit(&held, &[2, 2], 3), 2);
    }

    /// NULL keys share one partition between them, so they cost one bucket however many
    /// rows carry them.
    #[test]
    fn null_keys_share_a_single_bucket() {
        let held = HashSet::new();
        let buckets = vec![NULL_BUCKET, NULL_BUCKET, NULL_BUCKET];
        assert_eq!(rows_that_fit(&held, &buckets, 1), 3);
        assert_eq!(rows_that_fit(&held, &[NULL_BUCKET, 5], 1), 1);
    }
}
