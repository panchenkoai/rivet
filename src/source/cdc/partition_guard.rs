//! A base-and-buffer table's partition key must not move under a change.
//!
//! `rivet compact` merges the buffer into the base only within the partitions the
//! buffer's own rows name. An UPDATE that moves a row from one partition to another
//! leaves its old copy in a partition the merge never reads, so the base would hold the
//! key twice. The stream sees the row before and after the change, so it refuses such a
//! change before any part is written, and the operator re-partitions the table.

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

/// Refuse an UPDATE that moves its row to another partition, before any part is written.
pub(crate) fn refuse_partition_move(
    ev: &ChangeEvent,
    guard: &PartitionGuard,
    columns: &[TypeMapping],
    engine: CdcEngine,
) -> Result<()> {
    if ev.op != ChangeOp::Update {
        return Ok(());
    }
    let table = &ev.table;
    let col = &guard.column;
    let at = ev
        .image_names
        .as_ref()
        .and_then(|n| n.iter().position(|c| c == col));
    let pair = at.and_then(|i| {
        let full = |img: &Option<Vec<RivetValue>>| {
            img.as_ref()
                .filter(|v| Some(v.len()) == ev.image_names.as_ref().map(|n| n.len()))
                .map(|v| v[i].clone())
        };
        full(&ev.before).zip(full(&ev.after))
    });
    let Some((before, after)) = pair else {
        if engine == CdcEngine::Mysql {
            crate::rivet_bail!(
                crate::error::codes::SOURCE_CDC_PREREQUISITE,
                "mysql cdc: `{table}` is loaded as base + buffer partitioned by `{col}`, and an \
                 UPDATE arrived without the row's previous `{col}`, so rivet cannot tell whether \
                 it moved the row to another partition. The server needs `binlog_row_image = FULL` \
                 (`SET PERSIST binlog_row_image = FULL;`). No part was written and the checkpoint \
                 did not move."
            );
        }
        return Ok(());
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
        return Ok(());
    };
    if moves(&before, &after, guard.unit) {
        let zoned = native.to_ascii_lowercase().starts_with("timestamp");
        crate::rivet_bail!(
            crate::error::codes::CDC_PARTITION_MOVED,
            "cdc: `{table}` is loaded as base + buffer partitioned by `{col}`, and an UPDATE at \
             {pos} moved a row from `{col}` = {b} to {a}, another partition. `rivet compact` \
             merges only the partitions the changes name, so the row's old copy would stay \
             behind and the base would hold its key twice. Partition this table by a column a \
             change never moves (or `partition: none`), then recreate its base. No part was \
             written and the checkpoint did not move.",
            pos = ev.position.0,
            b = shown(&before, zoned),
            a = shown(&after, zoned),
        );
    }
    Ok(())
}

/// A partition value as the refusal names it.
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
        }
    }

    fn day_guard() -> PartitionGuard {
        PartitionGuard {
            column: "created_at".into(),
            unit: GuardUnit::Time(Granularity::Day),
        }
    }

    /// A MySQL TIMESTAMP arrives as epoch text; the guard compares its UTC days, not the text.
    #[test]
    fn a_mysql_timestamp_update_is_refused_only_when_its_utc_day_changes() {
        let (cols, g) = (ts_col(), day_guard());
        // 2023-12-31 23:59:59 UTC and 2024-01-01 00:00:00 UTC, one second apart.
        let moved = refuse_partition_move(
            &update(Some("1704067199"), "1704067200"),
            &g,
            &cols,
            CdcEngine::Mysql,
        )
        .expect_err("a day boundary crossed");
        assert_eq!(
            crate::error::error_code(&moved),
            Some("RIVET_CDC_PARTITION_MOVED")
        );
        let said = moved.to_string();
        assert!(
            said.contains("2023-12-31T23:59:59.000000000Z")
                && said.contains("2024-01-01T00:00:00.000000000Z"),
            "{said}"
        );
        refuse_partition_move(
            &update(Some("1704067200"), "1704153599"),
            &g,
            &cols,
            CdcEngine::Mysql,
        )
        .expect("the same UTC day");
    }

    /// Without the row's previous value MySQL cannot be checked, so it is refused; other engines are not.
    #[test]
    fn an_update_without_its_previous_value_is_refused_on_mysql_only() {
        let (cols, g) = (ts_col(), day_guard());
        let e = refuse_partition_move(&update(None, "1704067200"), &g, &cols, CdcEngine::Mysql)
            .expect_err("no before-image");
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_SOURCE_CDC_PREREQUISITE")
        );
        assert!(e.to_string().contains("binlog_row_image = FULL"), "{e}");
        refuse_partition_move(&update(None, "1704067200"), &g, &cols, CdcEngine::Postgres)
            .expect("an engine whose compaction still finds moved rows itself");
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
