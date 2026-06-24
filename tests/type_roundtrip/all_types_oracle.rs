//! Capability-keyed resolver oracle (offline; PR-fast, no docker).
//!
//! One curated all-types row resolved across **every** [`ExportTarget`], checked
//! by a single assertion whose tolerances come from the resolver's *own* output
//! — `autoload_type` vs `target_type`, `status`, `cast_sql`, `note` — not a
//! hand-maintained per-target expected map (which silently drifts from the
//! resolver). Borrowed from dlt's `assert_all_data_types_row`: one oracle, keyed
//! on the destination's declared capabilities, over many destinations.
//!
//! What it enforces is the remediation contract recorded in CLAUDE.md: a
//! post-load `cast_sql` exists **only** to bridge an autoload divergence, and
//! **only** when that bridge is lossless. A lossy divergence (a `UINT64` that has
//! already overflowed to `INT64` on autoload, a naive timestamp) yields
//! `cast_sql = None` plus an upstream (load-schema) `note` — never a SELECT-time
//! cast that silently runs on already-degraded data.

use rivet::types::target::{ExportTarget, TargetInput, TargetStatus};
use rivet::types::{RivetType, SourceColumn, TimeUnit, TypeMapping};

const TARGETS: [ExportTarget; 3] = [
    ExportTarget::DuckDb,
    ExportTarget::BigQuery,
    ExportTarget::Snowflake,
];

/// The curated all-types row — every [`RivetType`] variant, including the
/// adversarial boundaries: the lossy `UInt64` (overflows on warehouse autoload),
/// the naive vs tz-aware timestamps, and the recoverable logical types (`Json`,
/// `Uuid`). Fresh each call so it can be consumed per target without cloning.
fn all_types() -> Vec<(&'static str, &'static str, RivetType)> {
    vec![
        ("c_bool", "boolean", RivetType::Bool),
        ("c_int16", "smallint", RivetType::Int16),
        ("c_int32", "integer", RivetType::Int32),
        ("c_int64", "bigint", RivetType::Int64),
        ("c_uint64", "bigint unsigned", RivetType::UInt64),
        ("c_f32", "real", RivetType::Float32),
        ("c_f64", "double precision", RivetType::Float64),
        (
            "c_decimal",
            "numeric(18,2)",
            RivetType::Decimal {
                precision: 18,
                scale: 2,
            },
        ),
        ("c_date", "date", RivetType::Date),
        (
            "c_time",
            "time",
            RivetType::Time {
                unit: TimeUnit::Microsecond,
            },
        ),
        (
            "c_ts_naive",
            "timestamp",
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
        ),
        (
            "c_ts_tz",
            "timestamptz",
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".to_string()),
            },
        ),
        ("c_string", "varchar", RivetType::String),
        ("c_text", "text", RivetType::Text),
        ("c_bytes", "bytea", RivetType::Binary),
        ("c_json", "jsonb", RivetType::Json),
        ("c_uuid", "uuid", RivetType::Uuid),
        ("c_enum", "status_enum", RivetType::Enum),
        ("c_interval", "interval", RivetType::Interval),
        (
            "c_list",
            "integer[]",
            RivetType::List {
                inner: Box::new(RivetType::Int32),
            },
        ),
        (
            "c_unsupported",
            "point",
            RivetType::Unsupported {
                native_type: "point".to_string(),
                reason: "geometry has no interchange type".to_string(),
            },
        ),
    ]
}

fn resolve(
    tgt: ExportTarget,
    name: &str,
    native: &str,
    rivet: RivetType,
) -> rivet::types::target::TargetColumnSpec {
    let m = TypeMapping::from_source(&SourceColumn::simple(name, native, true), rivet);
    tgt.resolve_column(TargetInput::from(&m))
}

/// The capability-keyed oracle: for every (target × type) the resolver's own
/// output must obey the remediation contract. No hardcoded expected types — the
/// assertion is driven by what the resolver itself declares.
#[test]
fn resolver_remediation_contract_holds_across_all_targets() {
    for tgt in TARGETS {
        for (name, native, rivet) in all_types() {
            let spec = resolve(tgt, name, native, rivet);
            let ctx = format!("{tgt:?}/{name}");

            // (1) An unmappable column is a `Fail` row — and must never dangle a
            //     recovery snippet that can't run.
            if spec.status == TargetStatus::Fail {
                assert!(
                    spec.cast_sql.is_none(),
                    "{ctx}: a Fail column must not advertise a cast_sql ({:?})",
                    spec.cast_sql
                );
                continue;
            }

            // (2) The remediation rule.
            if spec.autoload_type == spec.target_type {
                // Autoload already lands the native type — a cast_sql would be a
                // no-op, so there must not be one.
                assert!(
                    spec.cast_sql.is_none(),
                    "{ctx}: autoload already equals the native type ({}) — cast_sql must be None, got {:?}",
                    spec.target_type,
                    spec.cast_sql
                );
            } else if spec.cast_sql.is_none() {
                // Autoload diverges from the native type and the file offers no
                // recovery → the gap is lossy and MUST be flagged upstream, never
                // left as a silent size-only-equivalent pass.
                assert!(
                    spec.note.is_some(),
                    "{ctx}: autoload diverges ({} -> {}) with no cast_sql — a lossy gap MUST carry an upstream note",
                    spec.autoload_type,
                    spec.target_type
                );
            }
        }
    }
}

/// The lossy boundary, pinned (dlt's "the capability flag is itself under test"):
/// once `UINT64` autoloads as `INT64` it has already overflowed, so no post-load
/// cast recovers it — the warehouses must point at the load schema, not a cast.
#[test]
fn uint64_overflow_is_upstream_only_never_a_select_cast() {
    let mut diverged = 0;
    for tgt in [ExportTarget::BigQuery, ExportTarget::Snowflake] {
        let spec = resolve(tgt, "big_u", "bigint unsigned", RivetType::UInt64);
        if spec.autoload_type != spec.target_type {
            diverged += 1;
            assert!(
                spec.cast_sql.is_none(),
                "{tgt:?}/UInt64: an overflowed autoload is unrecoverable — cast_sql must be None, got {:?}",
                spec.cast_sql
            );
            assert!(
                spec.note.is_some(),
                "{tgt:?}/UInt64: must point to the load schema (upstream), got note=None"
            );
        }
    }
    // Non-vacuity: the boundary must actually fire on at least one warehouse —
    // otherwise the resolver stopped flagging the overflow and this test proves
    // nothing.
    assert!(
        diverged > 0,
        "UInt64 must autoload-diverge from its native type on at least one warehouse"
    );
}

/// The recoverable side of the rule: `Json`/`Uuid` autoload to a byte-ish type on
/// the warehouses, but the bytes are intact, so a cast (`PARSE_JSON` / `TO_HEX`)
/// recovers the native type losslessly — a cast_sql is expected, not a note-only.
#[test]
fn json_uuid_divergence_is_losslessly_recoverable() {
    let mut diverged = 0;
    for (native, rivet) in [("jsonb", RivetType::Json), ("uuid", RivetType::Uuid)] {
        for tgt in [ExportTarget::BigQuery, ExportTarget::Snowflake] {
            let spec = resolve(tgt, "c", native, rivet.clone());
            if spec.autoload_type != spec.target_type {
                diverged += 1;
                assert!(
                    spec.cast_sql.is_some(),
                    "{tgt:?}/{native}: byte autoload is losslessly recoverable — expected a cast_sql, got None"
                );
            }
        }
    }
    // Non-vacuity: at least one (type × warehouse) must actually diverge, else
    // the recoverable branch was never exercised.
    assert!(
        diverged > 0,
        "Json/Uuid must autoload-diverge from native on at least one warehouse"
    );
}

/// DuckDb is the reference consumer — it honors every native logical type on
/// autoload, so no column ever needs a recovery cast.
#[test]
fn duckdb_honors_every_native_type_so_never_needs_a_cast() {
    for (name, native, rivet) in all_types() {
        if matches!(rivet, RivetType::Unsupported { .. }) {
            continue;
        }
        let spec = resolve(ExportTarget::DuckDb, name, native, rivet);
        assert!(
            spec.cast_sql.is_none(),
            "DuckDb/{name}: the reference consumer needs no cast_sql, got {:?}",
            spec.cast_sql
        );
    }
}
