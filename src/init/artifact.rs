//! Discovery artifact — serializable output of `rivet init --discover` (Epic B).
//!
//! Machine-readable counterpart to the YAML scaffold. External orchestrators and
//! reviewers can consume this JSON to decide modes, cursors, and priorities without
//! re-running introspection queries.

use serde::{Deserialize, Serialize};

/// Why a column is considered a cursor candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CursorCandidateReason {
    /// Conventional name (`updated_at`, `modified_at`).
    NameSuggestsUpdated,
    /// Conventional name (`created_at`).
    NameSuggestsCreated,
    /// Timestamp / date / datetime data type.
    TimestampType,
    /// Integer column — usable as a monotonic surrogate cursor.
    IntegerMonotonic,
    /// Column is the primary key.
    PrimaryKey,
    /// Column is declared NULL-able — cursor progression may need `COALESCE` fallback.
    Nullable,
}

/// One ranked candidate for an incremental cursor column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CursorCandidate {
    pub column: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    /// Rough score: higher is stronger. Deterministic; not a probability.
    pub score: i32,
    pub reasons: Vec<CursorCandidateReason>,
}

/// One candidate for a chunked extraction key (integer-typed column).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChunkCandidate {
    pub column: String,
    pub data_type: String,
    pub is_primary_key: bool,
    pub score: i32,
}

/// Per-table discovery record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableDiscovery {
    pub schema: String,
    pub table: String,
    pub row_estimate: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<i64>,
    pub suggested_mode: String,
    /// Ordered; the first entry is the best candidate.
    pub cursor_candidates: Vec<CursorCandidate>,
    /// Suggested `cursor_fallback_column` when the best cursor is NULL-able and a
    /// better-than-nothing timestamp exists (triggers `incremental_cursor_mode: coalesce`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_cursor_fallback_column: Option<String>,
    pub chunk_candidates: Vec<ChunkCandidate>,
    /// Human-readable advisory notes surfaced to operators reviewing the artifact.
    pub notes: Vec<String>,
}

/// Root discovery artifact emitted by `rivet init --discover`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DiscoveryArtifact {
    /// Rivet version that produced this artifact.
    pub rivet_version: String,
    /// `"postgres"` or `"mysql"` — the source dialect.
    pub source_type: String,
    /// Human-readable scope (e.g. `"PostgreSQL schema \"public\" (5 objects)"`).
    pub scope: String,
    pub tables: Vec<TableDiscovery>,
}

impl DiscoveryArtifact {
    pub fn to_json_pretty(&self) -> crate::error::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_round_trip_artifact() {
        let a = DiscoveryArtifact {
            rivet_version: "0.0.0-test".into(),
            source_type: "postgres".into(),
            scope: r#"schema "public" (1 object)"#.into(),
            tables: vec![TableDiscovery {
                schema: "public".into(),
                table: "orders".into(),
                row_estimate: 1000,
                total_bytes: Some(4096),
                suggested_mode: "incremental".into(),
                cursor_candidates: vec![CursorCandidate {
                    column: "updated_at".into(),
                    data_type: "timestamp".into(),
                    is_nullable: false,
                    is_primary_key: false,
                    score: 120,
                    reasons: vec![
                        CursorCandidateReason::NameSuggestsUpdated,
                        CursorCandidateReason::TimestampType,
                    ],
                }],
                suggested_cursor_fallback_column: None,
                chunk_candidates: vec![],
                notes: vec![],
            }],
        };
        let j = a.to_json_pretty().unwrap();
        let back: DiscoveryArtifact = serde_json::from_str(&j).unwrap();
        assert_eq!(back, a);
    }
}
