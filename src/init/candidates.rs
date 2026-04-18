//! Cursor and chunk candidate ranking (Epic B).
//!
//! Deterministic rule-based scoring so the discovery artifact is reproducible
//! and reviewable without a live database connection.

use super::{
    ChunkCandidate, CursorCandidate, CursorCandidateReason, TableInfo, is_integer_type,
    is_timestamp_type,
};

/// Rank every plausible cursor candidate.
///
/// The list is sorted by descending score and tie-broken by column order in the
/// table definition (so output is stable across platforms).
pub(super) fn cursor_candidates(info: &TableInfo) -> Vec<CursorCandidate> {
    let mut out: Vec<CursorCandidate> = Vec::new();

    for (idx, col) in info.columns.iter().enumerate() {
        let mut reasons: Vec<CursorCandidateReason> = Vec::new();
        let mut score: i32 = 0;

        if is_timestamp_type(&col.data_type) {
            reasons.push(CursorCandidateReason::TimestampType);
            score += 40;
        } else if is_integer_type(&col.data_type) && col.is_primary_key {
            // Integer PK is a usable surrogate cursor (monotonic ids).
            reasons.push(CursorCandidateReason::IntegerMonotonic);
            reasons.push(CursorCandidateReason::PrimaryKey);
            score += 25;
        } else {
            continue;
        }

        match col.name.as_str() {
            "updated_at" | "modified_at" => {
                reasons.push(CursorCandidateReason::NameSuggestsUpdated);
                score += 40;
            }
            "created_at" => {
                reasons.push(CursorCandidateReason::NameSuggestsCreated);
                score += 20;
            }
            _ => {}
        }

        if col.is_nullable {
            reasons.push(CursorCandidateReason::Nullable);
            score -= 10;
        }

        // Small boost to earlier columns so suggestions stay stable across wide tables.
        let position_bonus = (info.columns.len() as i32 - idx as i32).clamp(0, 5);
        score += position_bonus;

        out.push(CursorCandidate {
            column: col.name.clone(),
            data_type: col.data_type.clone(),
            is_nullable: col.is_nullable,
            is_primary_key: col.is_primary_key,
            score,
            reasons,
        });
    }

    out.sort_by(|a, b| b.score.cmp(&a.score).then(a.column.cmp(&b.column)));
    out
}

/// Pick a NULL-safe fallback timestamp to suggest for coalesce mode.
///
/// Returns `Some(column_name)` only when:
/// - the best cursor candidate is nullable (so coalesce actually helps), AND
/// - a *different* NOT-NULL timestamp column exists.
pub(super) fn suggest_cursor_fallback(info: &TableInfo) -> Option<String> {
    let primary = info.cursor_candidates().into_iter().next()?;
    if !primary.is_nullable {
        return None;
    }
    info.columns
        .iter()
        .find(|c| !c.is_nullable && is_timestamp_type(&c.data_type) && c.name != primary.column)
        .map(|c| c.name.clone())
}

/// Rank chunk-column candidates (integer types, PK preferred).
pub(super) fn chunk_candidates(info: &TableInfo) -> Vec<ChunkCandidate> {
    let mut out: Vec<ChunkCandidate> = info
        .columns
        .iter()
        .filter(|c| is_integer_type(&c.data_type))
        .map(|c| {
            let mut score = 20;
            if c.is_primary_key {
                score += 40;
            }
            ChunkCandidate {
                column: c.name.clone(),
                data_type: c.data_type.clone(),
                is_primary_key: c.is_primary_key,
                score,
            }
        })
        .collect();
    out.sort_by(|a, b| b.score.cmp(&a.score).then(a.column.cmp(&b.column)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init::{ColumnInfo, TableInfo};

    fn col(name: &str, ty: &str, pk: bool, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: ty.into(),
            is_primary_key: pk,
            is_nullable: nullable,
        }
    }

    fn table(cols: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            schema: "public".into(),
            table: "t".into(),
            row_estimate: 1_000,
            total_bytes: None,
            columns: cols,
        }
    }

    #[test]
    fn updated_at_scores_highest() {
        let t = table(vec![
            col("id", "bigint", true, false),
            col("created_at", "timestamp", false, false),
            col("updated_at", "timestamp", false, false),
        ]);
        let cands = cursor_candidates(&t);
        assert_eq!(cands[0].column, "updated_at");
        assert!(cands.iter().any(|c| {
            c.reasons
                .contains(&CursorCandidateReason::NameSuggestsUpdated)
        }));
    }

    #[test]
    fn nullable_updated_at_still_leads_but_with_nullable_reason() {
        let t = table(vec![
            col("id", "bigint", true, false),
            col("created_at", "timestamp", false, false),
            col("updated_at", "timestamp", false, true),
        ]);
        let cands = cursor_candidates(&t);
        assert_eq!(cands[0].column, "updated_at");
        assert!(cands[0].reasons.contains(&CursorCandidateReason::Nullable));
    }

    #[test]
    fn coalesce_fallback_suggested_when_primary_nullable_and_sibling_notnull_exists() {
        let t = table(vec![
            col("id", "bigint", true, false),
            col("created_at", "timestamp", false, false),
            col("updated_at", "timestamp", false, true),
        ]);
        assert_eq!(suggest_cursor_fallback(&t), Some("created_at".to_string()));
    }

    #[test]
    fn no_coalesce_fallback_when_primary_notnull() {
        let t = table(vec![
            col("id", "bigint", true, false),
            col("created_at", "timestamp", false, false),
            col("updated_at", "timestamp", false, false),
        ]);
        assert_eq!(suggest_cursor_fallback(&t), None);
    }

    #[test]
    fn chunk_candidates_prefer_pk() {
        let t = table(vec![
            col("other_int", "bigint", false, false),
            col("id", "bigint", true, false),
            col("name", "text", false, false),
        ]);
        let cands = chunk_candidates(&t);
        assert_eq!(cands[0].column, "id");
        assert!(cands[0].is_primary_key);
    }
}
