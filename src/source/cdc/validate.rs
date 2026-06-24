//! Post-hoc `__pos` continuity check for CDC outputs.
//!
//! Reads a CDC run's parts back from the destination (in manifest order) and
//! verifies the `__pos` column is **monotonically non-decreasing** — every change
//! stayed in source-log order, no part was reordered, and no part boundary
//! overlaps the previous one. That proves *ordering + no-duplication + no-reorder*
//! integrity of what landed in the bucket.
//!
//! It deliberately does **not** claim "no transaction was missed": log positions
//! are byte offsets (MySQL) / LSNs (PostgreSQL, SQL Server), not a dense counter,
//! so a skipped transaction is indistinguishable from a large one. Completeness at
//! the *seam between runs* is instead guaranteed by the commit-boundary checkpoint
//! (and, for PostgreSQL, the slot only advancing after a durable write).

use crate::destination::Destination;
use crate::error::Result;
use crate::manifest::{MANIFEST_FILENAME, RunManifest, join_key};

/// Outcome of a `__pos` continuity check over one CDC run's output.
pub(crate) struct PositionCheck {
    pub parts: usize,
    pub rows: usize,
    pub first: Option<String>,
    pub last: Option<String>,
    /// Out-of-order / unparseable `__pos` findings — empty ⇒ clean.
    pub violations: Vec<String>,
}

impl PositionCheck {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// A `__pos` value normalised to a comparable key. Within a run every position is
/// the same engine's shape, so cross-variant comparison never happens (the derived
/// `Ord`'s discriminant tie-break is irrelevant).
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum PosKey {
    /// MySQL binlog `{file, pos}` — zero-padded filename sorts lexically, then pos.
    Binlog(String, u64),
    /// PostgreSQL LSN `hi/lo` (hex) → a single u64.
    PgLsn(u64),
    /// SQL Server LSN — fixed-width hex, lexically comparable.
    Lsn(String),
}

/// Parse one `__pos` JSON string (`{"file":…,"pos":…}` or `{"lsn":…}`) into a
/// comparable key.
fn parse_pos(s: &str) -> Option<PosKey> {
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    if let (Some(file), Some(pos)) = (v.get("file").and_then(|x| x.as_str()), v.get("pos")) {
        let pos = pos.as_u64()?;
        return Some(PosKey::Binlog(file.to_string(), pos));
    }
    let lsn = v.get("lsn").and_then(|x| x.as_str())?;
    if let Some((hi, lo)) = lsn.split_once('/') {
        let hi = u64::from_str_radix(hi, 16).ok()?;
        let lo = u64::from_str_radix(lo, 16).ok()?;
        return Some(PosKey::PgLsn((hi << 32) | lo));
    }
    Some(PosKey::Lsn(lsn.to_string()))
}

/// Read the `__pos` column (Utf8) out of a Parquet part body. The body is staged
/// to a temp file so the parquet reader's `File` `ChunkReader` can seek the footer.
fn read_pos_column(body: Vec<u8>) -> Result<Vec<String>> {
    use std::io::Write;

    use arrow::array::{Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut tmp = tempfile::NamedTempFile::new()?;
    tmp.write_all(&body)?;
    tmp.flush()?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(tmp.reopen()?)?.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let idx = batch
            .schema()
            .index_of("__pos")
            .map_err(|_| anyhow::anyhow!("part has no __pos column — not a CDC output?"))?;
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("__pos column is not Utf8"))?;
        for i in 0..col.len() {
            if col.is_valid(i) {
                out.push(col.value(i).to_string());
            }
        }
    }
    Ok(out)
}

/// Verify `__pos` is monotonically non-decreasing across a CDC run's Parquet parts
/// at `dest`/`prefix`. Returns the range covered and any ordering violations.
pub(crate) fn check_positions(dest: &dyn Destination, prefix: &str) -> Result<PositionCheck> {
    let manifest_key = join_key(prefix, MANIFEST_FILENAME);
    let manifest: RunManifest = serde_json::from_slice(&dest.read(&manifest_key)?)?;

    // IO half: read every part's __pos in part→row order.
    let mut items: Vec<(u32, String)> = Vec::new();
    for part in &manifest.parts {
        let body = dest.read(&join_key(prefix, &part.path))?;
        items.extend(
            read_pos_column(body)?
                .into_iter()
                .map(|p| (part.part_id, p)),
        );
    }

    let (first, last, violations) = check_order(&items);
    Ok(PositionCheck {
        parts: manifest.parts.len(),
        rows: items.len(),
        first,
        last,
        violations,
    })
}

/// Pure half: the monotonicity invariant over `(part_id, __pos)` in part→row
/// order, lifted out of the IO so it is testable without a destination.
fn check_order(items: &[(u32, String)]) -> (Option<String>, Option<String>, Vec<String>) {
    let mut prev: Option<PosKey> = None;
    let (mut first, mut last, mut violations) = (None, None, Vec::new());
    for (part_id, raw) in items {
        let Some(key) = parse_pos(raw) else {
            violations.push(format!("part {part_id}: unparseable __pos {raw:?}"));
            continue;
        };
        first.get_or_insert_with(|| raw.clone());
        if prev.as_ref().is_some_and(|p| key < *p) {
            violations.push(format!(
                "part {part_id}: __pos went backwards at {raw:?} (out of log order)"
            ));
        }
        prev = Some(key);
        last = Some(raw.clone());
    }
    (first, last, violations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pos_ordering_per_engine() {
        // MySQL: same file, increasing pos.
        assert!(
            parse_pos(r#"{"file":"binlog.000046","pos":100}"#).unwrap()
                < parse_pos(r#"{"file":"binlog.000046","pos":200}"#).unwrap()
        );
        // MySQL: file rotation dominates.
        assert!(
            parse_pos(r#"{"file":"binlog.000046","pos":999}"#).unwrap()
                < parse_pos(r#"{"file":"binlog.000047","pos":4}"#).unwrap()
        );
        // PostgreSQL LSN: hi/lo hex parsed numerically (not string — "9" > "1A" as
        // strings would be wrong).
        assert!(parse_pos(r#"{"lsn":"3C/9"}"#).unwrap() < parse_pos(r#"{"lsn":"3C/1A"}"#).unwrap());
        assert!(
            parse_pos(r#"{"lsn":"3C/FFFFFFFF"}"#).unwrap()
                < parse_pos(r#"{"lsn":"3D/0"}"#).unwrap()
        );
    }

    #[test]
    fn malformed_pos_is_none() {
        assert!(parse_pos("not json").is_none());
        assert!(parse_pos(r#"{"nope":1}"#).is_none());
    }

    #[test]
    fn check_order_flags_a_backwards_jump() {
        let items = vec![
            (1, r#"{"lsn":"3C/10"}"#.into()),
            (2, r#"{"lsn":"3C/05"}"#.into()), // earlier LSN in a later part
        ];
        let (.., violations) = check_order(&items);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("backwards"));
    }

    #[test]
    fn check_order_clean_when_monotonic() {
        let items = vec![
            (1, r#"{"file":"b.000001","pos":100}"#.into()),
            (2, r#"{"file":"b.000002","pos":4}"#.into()), // file rotation still forward
        ];
        let (first, last, violations) = check_order(&items);
        assert!(violations.is_empty());
        assert!(first.unwrap().contains("100") && last.unwrap().contains("000002"));
    }
}
