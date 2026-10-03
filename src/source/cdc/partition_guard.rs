//! An UPDATE that moves its row: to another primary key, or to another partition of a
//! base-and-buffer table.
//!
//! Either move is written as a delete of the old row and an insert of the new one (ADR-0030).
//! A key move must retract the old key, or every latest-image-per-key merge keeps it live
//! beside the new one; a partition move must carry the old partition into the buffer, because
//! `rivet compact` merges only within the partitions the buffer's own rows name. The delete
//! keeps the event's ordinal and the insert takes the next one, so the insert is the key's
//! latest change.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use super::value::{RivetValue, mysql_cell_fix};
use super::{CdcEngine, ChangeEvent, ChangeOp};
use crate::config::load::{Granularity, PartitionForm};
use crate::types::TypeMapping;

/// The partition key of a base-and-buffer table, as the stream checks it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionGuard {
    pub column: String,
    pub unit: GuardUnit,
}

/// How a partition value maps to its partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GuardUnit {
    Time(Granularity),
    Range { start: i64, end: i64, interval: i64 },
}

impl PartitionGuard {
    /// The guard for a partition form; `None` for ingestion time, which no change can move.
    pub(crate) fn of(form: &PartitionForm) -> Option<Self> {
        match form {
            PartitionForm::Column {
                column,
                granularity,
            } => Some(Self {
                column: column.clone(),
                unit: GuardUnit::Time(*granularity),
            }),
            PartitionForm::Range {
                column,
                start,
                end,
                interval,
            } => Some(Self {
                column: column.clone(),
                unit: GuardUnit::Range {
                    start: *start,
                    end: *end,
                    interval: *interval,
                },
            }),
            PartitionForm::Ingestion(_) => None,
        }
    }
}

/// The partition a value lands in; `None` for NULL, whose partition every merge reads.
fn partition_of(v: &RivetValue, unit: GuardUnit) -> Option<i64> {
    match (v, unit) {
        (RivetValue::DateTime(dt), GuardUnit::Time(g)) => Some(crate::plan::rollover::bucket_of(
            dt.and_utc().timestamp(),
            g,
        )),
        (RivetValue::Int(i), GuardUnit::Range { .. }) => Some(range_bucket(*i, unit)),
        (RivetValue::UInt(u), GuardUnit::Range { .. }) => {
            Some(range_bucket(i64::try_from(*u).unwrap_or(i64::MAX), unit))
        }
        _ => None,
    }
}

/// An integer's range partition: below `start` and at or past `end` are the two outside partitions.
fn range_bucket(v: i64, unit: GuardUnit) -> i64 {
    let GuardUnit::Range {
        start,
        end,
        interval,
    } = unit
    else {
        return 0;
    };
    if v < start {
        i64::MIN
    } else if v >= end {
        i64::MAX
    } else {
        (v - start).div_euclid(interval)
    }
}

/// Whether a change from `before` to `after` moves the row to another partition.
pub(crate) fn moves(before: &RivetValue, after: &RivetValue, unit: GuardUnit) -> bool {
    match (partition_of(before, unit), partition_of(after, unit)) {
        (Some(b), Some(a)) => b != a,
        _ => false,
    }
}

/// An image with the names of its cells, only when every cell is named (a positional partial image is not).
fn named<'a>(
    names: Option<&'a std::sync::Arc<[String]>>,
    img: Option<&'a Vec<RivetValue>>,
) -> Option<(&'a [String], &'a [RivetValue])> {
    let (n, v) = (names?, img?);
    (n.len() == v.len()).then_some((n, v))
}

/// An event's pre-image with its names: `before_names` when the engine gave its own, else `image_names`.
fn before_image(ev: &ChangeEvent) -> Option<(&[String], &[RivetValue])> {
    named(
        ev.before_names.as_ref().or(ev.image_names.as_ref()),
        ev.before.as_ref(),
    )
}

/// Whether an UPDATE changes its key: every key column carried in both images, and one differs.
pub(crate) fn key_moves(ev: &ChangeEvent, key: &[String]) -> bool {
    let (Some((bn, b)), Some((an, a))) = (
        before_image(ev),
        named(ev.image_names.as_ref(), ev.after.as_ref()),
    ) else {
        return false;
    };
    let pairs: Option<Vec<_>> = key
        .iter()
        .map(|k| cell(bn, b, k).zip(cell(an, a, k)))
        .collect();
    pairs.is_some_and(|p| !p.is_empty() && p.iter().any(|(old, new)| old != new))
}

/// An UPDATE that moves its key or its partition as `(delete of the old row, insert of the new one)`, `None` for any other change.
pub(crate) fn split_move(
    ev: &ChangeEvent,
    key: &[String],
    guard: Option<&PartitionGuard>,
    columns: &[TypeMapping],
    engine: CdcEngine,
) -> Result<Option<(ChangeEvent, ChangeEvent)>> {
    if ev.op != ChangeOp::Update {
        return Ok(None);
    }
    let moved = key_moves(ev, key)
        || match guard {
            Some(g) => partition_moves(ev, g, columns, engine)?,
            None => false,
        };
    if !moved {
        return Ok(None);
    }
    let delete = ChangeEvent {
        op: ChangeOp::Delete,
        after: None,
        committed: false,
        image_names: ev.before_names.clone().or(ev.image_names.clone()),
        ..ev.clone()
    };
    let insert = ChangeEvent {
        op: ChangeOp::Insert,
        before: None,
        ..ev.clone()
    };
    Ok(Some((delete, insert)))
}

/// The value of `col` in an image, by name (exact first, then ASCII case-insensitive).
fn cell<'a>(names: &[String], img: &'a [RivetValue], col: &str) -> Option<&'a RivetValue> {
    names
        .iter()
        .position(|n| n == col)
        .or_else(|| names.iter().position(|n| n.eq_ignore_ascii_case(col)))
        .and_then(|i| img.get(i))
}

/// An image's `key` values rendered as one map key; `None` when a key column is absent.
fn key_text(names: &[String], img: &[RivetValue], key: &[String]) -> Option<String> {
    if key.is_empty() {
        return None;
    }
    key.iter()
        .map(|k| cell(names, img, k).map(|v| format!("{v:?}")))
        .collect::<Option<Vec<_>>>()
        .map(|v| v.join("\u{1f}"))
}

/// Whether a before-image is the row a held image shows: every cell it carries agrees by name.
fn same_row(
    held_names: &[String],
    held: &[RivetValue],
    names: &[String],
    before: &[RivetValue],
) -> bool {
    names
        .iter()
        .zip(before)
        .all(|(n, v)| cell(held_names, held, n).is_none_or(|h| h == v))
}

/// The keys key moves gave rows in the open transaction's part, so a per-statement renumber's delete precedes the moved-in insert (ADR-0030).
#[derive(Default)]
pub(crate) struct MovedIn {
    at: Option<super::Position>,
    held: HashMap<String, Held>,
}

/// A moved-in row: its image's names, the image, its row identity, and its index in the open part.
struct Held {
    names: Arc<[String]>,
    image: Vec<RivetValue>,
    row_id: Option<String>,
    at: usize,
}

/// Whether a delete retracts another row than `held`: by row identity when both carry one, else by image.
fn another_row(held: &Held, names: &[String], delete: &ChangeEvent) -> bool {
    match (&held.row_id, &delete.row_id) {
        (Some(a), Some(b)) => a != b,
        _ => !same_row(
            &held.names,
            &held.image,
            names,
            delete.before.as_deref().unwrap_or(&[]),
        ),
    }
}

impl MovedIn {
    /// Forget the open part's rows; called whenever the part is written out.
    pub(crate) fn reset(&mut self) {
        self.held.clear();
    }

    /// Push `ev` onto the open part `buf`; `moved_in` marks the insert half of a key move.
    pub(crate) fn push(
        &mut self,
        buf: &mut Vec<ChangeEvent>,
        mut ev: ChangeEvent,
        key: &[String],
        moved_in: bool,
    ) {
        if self.at.as_ref() != Some(&ev.position) {
            self.held.clear();
            self.at = Some(ev.position.clone());
        }
        let Some(names) = ev.image_names.clone() else {
            buf.push(ev);
            return;
        };
        let image = if ev.op == ChangeOp::Delete {
            &ev.before
        } else {
            &ev.after
        };
        let Some(k) = image.as_ref().and_then(|img| key_text(&names, img, key)) else {
            buf.push(ev);
            return;
        };
        match (ev.op, self.held.remove(&k)) {
            (ChangeOp::Delete, Some(mut h)) if another_row(&h, &names, &ev) => {
                std::mem::swap(&mut buf[h.at].seq, &mut ev.seq);
                let mover = std::mem::replace(&mut buf[h.at], ev);
                h.at = buf.len();
                self.held.insert(k, h);
                buf.push(mover);
            }
            (ChangeOp::Delete, _) => buf.push(ev),
            (_, held) => {
                if moved_in || held.is_some() {
                    let h = Held {
                        names,
                        image: ev.after.clone().unwrap_or_default(),
                        row_id: ev.row_id.clone(),
                        at: buf.len(),
                    };
                    self.held.insert(k, h);
                }
                buf.push(ev);
            }
        }
    }
}

/// Whether an UPDATE moves its row to another partition of `guard`; refused on MySQL without the previous value.
fn partition_moves(
    ev: &ChangeEvent,
    guard: &PartitionGuard,
    columns: &[TypeMapping],
    engine: CdcEngine,
) -> Result<bool> {
    let table = &ev.table;
    let col = &guard.column;
    let old = before_image(ev).and_then(|(n, v)| cell(n, v, col).cloned());
    let new = named(ev.image_names.as_ref(), ev.after.as_ref())
        .and_then(|(n, v)| cell(n, v, col).cloned());
    let pair = old.zip(new);
    let Some((before, after)) = pair else {
        if engine == CdcEngine::Mysql {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_PREREQUISITE,
                "mysql cdc: `{table}` is loaded as base + buffer partitioned by `{col}`, and an \
                 UPDATE arrived without the row's previous `{col}`, so rivet cannot tell which \
                 partition holds the old row. The server needs `binlog_row_image = FULL` \
                 (`SET PERSIST binlog_row_image = FULL;`). No part was written and the checkpoint \
                 did not move."
            );
        }
        return Ok(false);
    };
    let native = columns
        .iter()
        .find(|m| m.column_name == *col)
        .map_or("", |m| m.source_native_type.as_str());
    let fixed = |v: RivetValue| match mysql_cell_fix(engine, native) {
        Some(fix) => fix.apply(&v).ok(),
        None => Some(v),
    };
    // A cell the decoder cannot read is refused by the part writer with its own code.
    let (Some(before), Some(after)) = (fixed(before), fixed(after)) else {
        return Ok(false);
    };
    if !moves(&before, &after, guard.unit) {
        return Ok(false);
    }
    let zoned = native.to_ascii_lowercase().starts_with("timestamp");
    log::warn!(
        "cdc: `{table}` moved a row from `{col}` = {b} to {a}, another partition (at {pos}); \
         written as a delete of the old row and an insert of the new one",
        pos = ev.position.0,
        b = shown(&before, zoned),
        a = shown(&after, zoned),
    );
    Ok(true)
}

/// A partition value as the warning names it.
fn shown(v: &RivetValue, zoned: bool) -> String {
    match v {
        RivetValue::DateTime(dt) => crate::types::iso_timestamp_nanos(*dt, zoned),
        RivetValue::Int(i) => i.to_string(),
        RivetValue::UInt(u) => u.to_string(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> RivetValue {
        RivetValue::DateTime(chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap())
    }

    #[test]
    fn a_change_moves_its_row_only_across_a_partition_boundary() {
        let day = GuardUnit::Time(Granularity::Day);
        let month = GuardUnit::Time(Granularity::Month);
        for (b, a, unit, want) in [
            ("2024-01-01 00:00:00", "2024-01-01 23:59:59", day, false),
            ("2024-01-01 23:59:59", "2024-01-02 00:00:00", day, true),
            ("2024-01-01 00:00:00", "2024-01-31 12:00:00", month, false),
            ("2024-01-31 12:00:00", "2024-02-01 00:00:00", month, true),
        ] {
            assert_eq!(moves(&at(b), &at(a), unit), want, "{b} -> {a} at {unit:?}");
        }
    }

    #[test]
    fn a_null_partition_value_never_counts_as_a_move() {
        let day = GuardUnit::Time(Granularity::Day);
        assert!(!moves(&RivetValue::Null, &at("2024-01-01 00:00:00"), day));
        assert!(!moves(&at("2024-01-01 00:00:00"), &RivetValue::Null, day));
    }

    #[test]
    fn an_integer_range_partition_moves_between_intervals_and_the_outside_partitions() {
        let r = GuardUnit::Range {
            start: 0,
            end: 100,
            interval: 10,
        };
        for (b, a, want) in [
            (1, 9, false),
            (9, 10, true),
            (99, 100, true),
            (0, 5, false),
            (-1, -5, false),
            (-1, -15, false),
            (100, 5000, false),
        ] {
            assert_eq!(
                moves(&RivetValue::Int(b), &RivetValue::Int(a), r),
                want,
                "{b} -> {a}"
            );
        }
        assert!(
            moves(&RivetValue::UInt(9), &RivetValue::UInt(10), r),
            "an unsigned value"
        );
        let offset = GuardUnit::Range {
            start: 3,
            end: 103,
            interval: 10,
        };
        for (b, a, want) in [(12, 14, true), (8, 12, false)] {
            assert_eq!(
                moves(&RivetValue::Int(b), &RivetValue::Int(a), offset),
                want,
                "{b} -> {a} from 3"
            );
        }
    }

    fn ts_col() -> Vec<TypeMapping> {
        vec![TypeMapping {
            column_name: "created_at".into(),
            source_native_type: "timestamp".into(),
            rivet_type: crate::types::RivetType::Timestamp {
                unit: crate::types::TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            },
            arrow_type: None,
            fidelity: crate::types::TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
            delivery: crate::types::Delivery::Native,
        }]
    }

    fn update(before: Option<&str>, after: &str) -> ChangeEvent {
        let ts = |s: &str| RivetValue::Bytes(s.as_bytes().to_vec());
        ChangeEvent {
            op: ChangeOp::Update,
            schema: "s".into(),
            table: "t".into(),
            before: before.map(|b| vec![RivetValue::Int(1), ts(b)]),
            after: Some(vec![RivetValue::Int(1), ts(after)]),
            position: super::super::Position(
                serde_json::json!({"file": "binlog.000001", "pos": 4}),
            ),
            committed: true,
            image_names: Some(std::sync::Arc::from(vec![
                "id".to_string(),
                "created_at".to_string(),
            ])),
            seq: 0,
            poison: None,
            row_id: None,
            before_names: None,
        }
    }

    fn day_guard() -> PartitionGuard {
        PartitionGuard {
            column: "created_at".into(),
            unit: GuardUnit::Time(Granularity::Day),
        }
    }

    /// A MySQL TIMESTAMP arrives as epoch text; the split compares its UTC days, not the text.
    #[test]
    fn a_mysql_timestamp_update_splits_only_when_its_utc_day_changes() {
        let (cols, g) = (ts_col(), day_guard());
        // 2023-12-31 23:59:59 UTC and 2024-01-01 00:00:00 UTC, one second apart.
        let ev = update(Some("1704067199"), "1704067200");
        let (delete, insert) = split_move(&ev, &[], Some(&g), &cols, CdcEngine::Mysql)
            .unwrap()
            .expect("a day boundary crossed");
        assert_eq!(
            (
                delete.op,
                delete.before.clone(),
                delete.after.clone(),
                delete.committed
            ),
            (ChangeOp::Delete, ev.before.clone(), None, false),
            "the delete carries the OLD row and never closes the transaction"
        );
        assert_eq!(
            (
                insert.op,
                insert.before.clone(),
                insert.after.clone(),
                insert.committed
            ),
            (ChangeOp::Insert, None, ev.after.clone(), true),
            "the insert carries the NEW row and keeps the commit boundary"
        );
        let same_day = update(Some("1704067200"), "1704153599");
        assert!(
            split_move(&same_day, &[], Some(&g), &cols, CdcEngine::Mysql)
                .unwrap()
                .is_none(),
            "the same UTC day is an ordinary update"
        );
    }

    /// Without the row's previous value MySQL cannot know the old partition, so it is refused; other engines are not.
    #[test]
    fn an_update_without_its_previous_value_is_refused_on_mysql_only() {
        let (cols, g) = (ts_col(), day_guard());
        let e = split_move(
            &update(None, "1704067200"),
            &[],
            Some(&g),
            &cols,
            CdcEngine::Mysql,
        )
        .expect_err("no before-image");
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_SOURCE_CDC_PREREQUISITE")
        );
        assert!(e.to_string().contains("binlog_row_image = FULL"), "{e}");
        assert!(
            split_move(
                &update(None, "1704067200"),
                &[],
                Some(&g),
                &cols,
                CdcEngine::Postgres
            )
            .unwrap()
            .is_none(),
            "an engine whose compaction still finds moved rows itself"
        );
    }

    fn row(names: &[&str], before: Option<Vec<i64>>, after: Vec<i64>) -> ChangeEvent {
        let img = |v: Vec<i64>| v.into_iter().map(RivetValue::Int).collect();
        ChangeEvent {
            before: before.map(img),
            after: Some(img(after)),
            image_names: Some(names.iter().map(|n| n.to_string()).collect()),
            ..update(None, "0")
        }
    }

    /// A two-column key moves only when both old cells were carried and one differs; NULL is a value, absent is not.
    #[test]
    fn a_key_moves_only_when_every_old_key_cell_was_carried_and_one_differs() {
        use RivetValue::{Bytes, Int, Null};
        let after = vec![Int(1), Bytes(b"\x1f".to_vec()), Bytes(b"z".to_vec())];
        let states = |now: RivetValue| {
            [
                ("absent", None),
                ("null", Some(Null)),
                ("equal", Some(now.clone())),
                ("different", Some(Bytes(b"NULL".to_vec()))),
            ]
        };
        let key = vec!["a".to_string(), "b".to_string()];
        for (sa, a_old) in states(Int(1)) {
            for (sb, b_old) in states(Bytes(b"\x1f".to_vec())) {
                let (mut names, mut before) = (Vec::new(), Vec::new());
                for (n, v) in [("b", &b_old), ("a", &a_old)] {
                    if let Some(v) = v {
                        names.push(n.to_string());
                        before.push(v.clone());
                    }
                }
                let ev = ChangeEvent {
                    before: Some(before),
                    before_names: Some(names.into()),
                    after: Some(after.clone()),
                    image_names: Some(vec!["a".into(), "b".into(), "v".into()].into()),
                    ..update(None, "0")
                };
                let want = a_old.is_some() && b_old.is_some() && (sa != "equal" || sb != "equal");
                assert_eq!(key_moves(&ev, &key), want, "a {sa}, b {sb}");
                if let Some((delete, _)) =
                    split_move(&ev, &key, None, &[], CdcEngine::Postgres).unwrap()
                {
                    let names = delete.image_names.clone().unwrap();
                    let old = delete.before.clone().unwrap();
                    assert_eq!(names.len(), old.len(), "the delete names exactly its cells");
                    assert!(
                        key.iter().all(|k| cell(&names, &old, k).is_some()),
                        "the delete carries every key cell: a {sa}, b {sb}"
                    );
                }
            }
        }
        let k = |a: &[u8], b: &[u8]| {
            key_text(
                &["a".to_string(), "b".to_string()],
                &[Bytes(a.to_vec()), Bytes(b.to_vec())],
                &key,
            )
        };
        assert_ne!(
            k(b"x\x1f", b"y"),
            k(b"x", b"\x1fy"),
            "a delimiter inside a value"
        );
        assert_ne!(
            key_text(&["a".to_string()], &[Null], &key[..1]),
            key_text(&["a".to_string()], &[Bytes(b"Null".to_vec())], &key[..1]),
            "NULL and the text `Null`"
        );
    }

    /// The key is compared by NAME across full images, never by position in a partial one.
    #[test]
    fn a_key_moves_only_when_a_key_column_differs_between_full_images() {
        let k = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        for (names, key, before, after, want) in [
            (
                vec!["v", "id"],
                k(&["id"]),
                Some(vec![10, 1]),
                vec![10, 2],
                true,
            ),
            (
                vec!["v", "id"],
                k(&["id"]),
                Some(vec![10, 1]),
                vec![11, 1],
                false,
            ),
            (
                vec!["a", "b"],
                k(&["a", "b"]),
                Some(vec![1, 2]),
                vec![1, 3],
                true,
            ),
            (
                vec!["a", "b"],
                k(&["a", "b"]),
                Some(vec![1, 2]),
                vec![1, 2],
                false,
            ),
            (
                vec!["ID", "V"],
                k(&["id"]),
                Some(vec![1, 5]),
                vec![2, 5],
                true,
            ),
            (
                vec!["v", "id"],
                k(&[]),
                Some(vec![10, 1]),
                vec![10, 2],
                false,
            ),
            (vec!["v", "id"], k(&["id"]), None, vec![10, 2], false),
            (
                vec!["blob", "id", "v"],
                k(&["id"]),
                Some(vec![1, 7]),
                vec![0, 1, 8],
                false,
            ),
            (
                vec!["v", "id"],
                k(&["id"]),
                Some(vec![1]),
                vec![10, 2],
                false,
            ),
            (
                vec!["v", "id"],
                k(&["nope"]),
                Some(vec![10, 1]),
                vec![10, 2],
                false,
            ),
        ] {
            let ev = row(&names, before.clone(), after.clone());
            assert_eq!(
                key_moves(&ev, &key),
                want,
                "{names:?} key {key:?}: {before:?} -> {after:?}"
            );
        }
    }

    /// A key move splits with no partition guard; one that also moves the partition is still ONE pair.
    #[test]
    fn a_key_move_splits_once_whether_or_not_it_also_moves_the_partition() {
        let key = vec!["id".to_string()];
        let range = PartitionGuard {
            column: "p".into(),
            unit: GuardUnit::Range {
                start: 0,
                end: 100,
                interval: 10,
            },
        };
        for (guard, after_p) in [(None, 5), (Some(&range), 5), (Some(&range), 55)] {
            let ev = row(&["id", "p"], Some(vec![1, 5]), vec![2, after_p]);
            let (delete, insert) = split_move(&ev, &key, guard, &[], CdcEngine::Postgres)
                .unwrap()
                .expect("a key move");
            assert_eq!(
                (delete.op, delete.before, delete.after, delete.committed),
                (ChangeOp::Delete, ev.before.clone(), None, false)
            );
            assert_eq!(
                (insert.op, insert.before, insert.after, insert.committed),
                (ChangeOp::Insert, None, ev.after.clone(), true)
            );
        }
        let same_key = row(&["id", "p"], Some(vec![1, 5]), vec![1, 6]);
        assert!(
            split_move(&same_key, &key, Some(&range), &[], CdcEngine::Postgres)
                .unwrap()
                .is_none(),
            "neither the key nor the partition moved"
        );
        let insert = ChangeEvent {
            op: ChangeOp::Insert,
            ..row(&["id", "p"], Some(vec![1, 5]), vec![2, 5])
        };
        assert!(
            split_move(&insert, &key, None, &[], CdcEngine::Postgres)
                .unwrap()
                .is_none(),
            "only an UPDATE moves"
        );
    }

    /// One `(op, before, after)` change of an `(id, v)` row.
    type Change = (ChangeOp, Option<[i64; 2]>, Option<[i64; 2]>);

    /// The destination's live `(id, v)` rows after one transaction's changes, routed as the sink routes them.
    fn applied(changes: &[Change]) -> Vec<(i64, i64)> {
        applied_by_row(changes, &[])
    }

    /// [`applied`] with the engine's row identity: `row_ids[i]` rides change `i`.
    fn applied_by_row(changes: &[Change], row_ids: &[&str]) -> Vec<(i64, i64)> {
        let key = vec!["id".to_string()];
        let mut seq = super::super::TxnSeq::default();
        let (mut moved, mut buf) = (MovedIn::default(), Vec::new());
        for (i, (op, b, a)) in changes.iter().enumerate() {
            let mut ev = ChangeEvent {
                op: *op,
                row_id: row_ids.get(i).map(|r| r.to_string()),
                ..row(
                    &["id", "v"],
                    b.map(|b| b.to_vec()),
                    a.unwrap_or([0, 0]).to_vec(),
                )
            };
            if a.is_none() {
                ev.after = None;
            }
            seq.stamp(&mut ev);
            match split_move(&ev, &key, None, &[], CdcEngine::Oracle).unwrap() {
                Some((delete, mut insert)) => {
                    insert.seq = seq.next(&insert.position);
                    moved.push(&mut buf, delete, &key, false);
                    moved.push(&mut buf, insert, &key, true);
                }
                None => moved.push(&mut buf, ev, &key, false),
            }
        }
        let seqs: Vec<u64> = buf.iter().map(|e| e.seq).collect();
        assert!(seqs.is_sorted(), "the part stays in __seq order: {seqs:?}");
        let mut last: HashMap<i64, (u64, Option<i64>)> = HashMap::new();
        for e in &buf {
            let img = e.after.as_ref().or(e.before.as_ref()).unwrap();
            let RivetValue::Int(id) = img[0] else {
                panic!()
            };
            let v = e.after.as_ref().map(|a| match a[1] {
                RivetValue::Int(v) => v,
                _ => panic!(),
            });
            if last.get(&id).is_none_or(|(s, _)| *s < e.seq) {
                last.insert(id, (e.seq, v));
            }
        }
        let mut live: Vec<(i64, i64)> = last
            .into_iter()
            .filter_map(|(id, (_, v))| v.map(|v| (id, v)))
            .collect();
        live.sort();
        live
    }

    /// A per-statement renumber's delete is the row that held the key before, not the row moved into it.
    #[test]
    fn a_statement_that_renumbers_keys_keeps_every_moved_row() {
        use ChangeOp::*;
        let renumber = [
            (Update, Some([1, 10]), Some([2, 10])),
            (Update, Some([2, 20]), Some([3, 20])),
            (Update, Some([3, 30]), Some([4, 30])),
        ];
        assert_eq!(applied(&renumber), vec![(2, 10), (3, 20), (4, 30)]);
        let swap = [
            (Update, Some([1, 10]), Some([2, 10])),
            (Update, Some([2, 20]), Some([1, 20])),
        ];
        assert_eq!(applied(&swap), vec![(1, 20), (2, 10)]);
        let by_two = [
            (Update, Some([1, 10]), Some([3, 10])),
            (Update, Some([2, 20]), Some([4, 20])),
            (Update, Some([3, 30]), Some([5, 30])),
            (Update, Some([4, 40]), Some([6, 40])),
        ];
        assert_eq!(
            applied(&by_two),
            vec![(3, 10), (4, 20), (5, 30), (6, 40)],
            "two keys moved in at once are each paired with their own delete"
        );
    }

    /// Statement by statement, a delete is the row the key's last change left, so it stays put.
    #[test]
    fn sequential_changes_of_a_moved_key_apply_in_their_order() {
        use ChangeOp::*;
        let insert_then_move = [
            (Insert, None, Some([5, 1])),
            (Update, Some([5, 1]), Some([6, 1])),
            (Update, Some([6, 1]), Some([7, 1])),
        ];
        assert_eq!(applied(&insert_then_move), vec![(7, 1)]);
        let move_update_move = [
            (Update, Some([1, 10]), Some([2, 10])),
            (Update, Some([2, 10]), Some([2, 11])),
            (Update, Some([2, 11]), Some([3, 11])),
            (Delete, Some([3, 11]), None),
        ];
        assert_eq!(applied(&move_update_move), vec![]);
    }

    /// Rows with EQUAL images are told apart by row identity; one row moved twice stays one row.
    #[test]
    fn a_renumber_over_rows_with_equal_images_keeps_every_row_by_its_row_id() {
        use ChangeOp::*;
        let renumber = [
            (Update, Some([1, 10]), Some([2, 10])),
            (Update, Some([2, 10]), Some([3, 10])),
            (Update, Some([3, 10]), Some([4, 10])),
        ];
        assert_eq!(
            applied_by_row(&renumber, &["A", "B", "C"]),
            vec![(2, 10), (3, 10), (4, 10)]
        );
        assert_eq!(applied_by_row(&renumber, &["A", "A", "A"]), vec![(4, 10)]);
    }

    /// Known limitation (ADR-0030): without a row identity a renumber over EQUAL images loses a row.
    #[test]
    fn a_renumber_over_equal_images_without_a_row_id_documents_the_lost_row() {
        use ChangeOp::*;
        let renumber = [
            (Update, Some([1, 10]), Some([2, 10])),
            (Update, Some([2, 10]), Some([3, 10])),
        ];
        assert_eq!(applied(&renumber), vec![(3, 10)]);
    }

    /// The moved-in keys belong to one open part of one transaction.
    #[test]
    fn moved_in_keys_are_forgotten_at_a_new_transaction_and_a_written_part() {
        let key = vec!["id".to_string()];
        let (mut moved, mut buf) = (MovedIn::default(), Vec::new());
        let ev = |pos: &str| ChangeEvent {
            op: ChangeOp::Insert,
            position: super::super::Position(serde_json::json!(pos)),
            ..row(&["id", "v"], None, vec![2, 10])
        };
        moved.push(&mut buf, ev("a"), &key, true);
        assert_eq!(moved.held.len(), 1);
        moved.reset();
        assert!(moved.held.is_empty(), "a written part");
        moved.push(&mut buf, ev("a"), &key, true);
        moved.push(
            &mut buf,
            ChangeEvent {
                op: ChangeOp::Update,
                ..ev("b")
            },
            &key,
            false,
        );
        assert!(moved.held.is_empty(), "a new transaction");
    }

    /// A key-only before-image (PostgreSQL's REPLICA IDENTITY DEFAULT) is compared on the cells it carries.
    #[test]
    fn a_key_only_before_image_is_the_row_its_key_names() {
        let names: Vec<String> = vec!["id".into(), "v".into()];
        let held = [RivetValue::Int(6), RivetValue::Int(1)];
        assert!(
            !same_row(
                &names,
                &held,
                &names,
                &[RivetValue::Int(6), RivetValue::Null]
            ),
            "a NULL it carries is a value, and it differs"
        );
        assert!(!same_row(
            &names,
            &held,
            &names,
            &[RivetValue::Int(6), RivetValue::Int(2)]
        ));
        assert!(same_row(&names, &held, &names[..1], &[RivetValue::Int(6)]));
    }

    #[test]
    fn ingestion_time_has_no_guard() {
        assert_eq!(
            PartitionGuard::of(&PartitionForm::Ingestion(Granularity::Day)),
            None
        );
        assert_eq!(
            PartitionGuard::of(&PartitionForm::Column {
                column: "created_at".into(),
                granularity: Granularity::Day
            }),
            Some(PartitionGuard {
                column: "created_at".into(),
                unit: GuardUnit::Time(Granularity::Day)
            })
        );
    }
}
