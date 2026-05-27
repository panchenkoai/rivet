//! Offline type-mapping contracts (PR-fast; no docker).

use std::collections::HashMap;

use arrow::datatypes::DataType;
use rivet::types::{RivetType, SourceColumn};
use rivet::types::{
    TypeFidelity, TypeMapping, build_arrow_field, derive_fidelity, rivet_type_to_arrow,
};
use serde::Deserialize;

use super::helpers::load_fixture;

#[derive(Debug, Deserialize)]
struct ContractsFile {
    contracts: Vec<ContractRow>,
}

#[derive(Debug, Deserialize)]
struct ContractRow {
    source: String,
    source_type: String,
    rivet_type: RivetType,
    arrow: String,
    fidelity: String,
    #[serde(default)]
    tested: String,
}

#[test]
fn contract_yaml_loads_and_covers_core_types() {
    let raw = load_fixture("expected_contracts.yaml");
    let file: ContractsFile = serde_yaml_ng::from_str(&raw).expect("parse contracts yaml");
    let pg = file
        .contracts
        .iter()
        .filter(|c| c.source == "postgres")
        .count();
    let my = file
        .contracts
        .iter()
        .filter(|c| c.source == "mysql")
        .count();
    assert!(pg >= 20, "postgres doc types: expected >=20, got {pg}");
    assert!(my >= 32, "mysql doc types: expected >=32, got {my}");
}

#[test]
fn contract_decimal_never_maps_to_float() {
    let raw = load_fixture("expected_contracts.yaml");
    let file: ContractsFile = serde_yaml_ng::from_str(&raw).expect("parse contracts yaml");
    for row in &file.contracts {
        if !matches!(row.rivet_type, RivetType::Decimal { .. }) {
            continue;
        }
        if row.tested == "planned" {
            continue;
        }
        let dt = rivet_type_to_arrow(&row.rivet_type).expect("decimal must map");
        assert!(
            matches!(dt, DataType::Decimal128(..) | DataType::Decimal256(..)),
            "{} {} must not become float, got {dt:?}",
            row.source,
            row.source_type
        );
        assert_eq!(derive_fidelity(&row.rivet_type), TypeFidelity::Exact);
    }
}

#[test]
fn contract_arrow_types_match_documented_shape() {
    let raw = load_fixture("expected_contracts.yaml");
    let file: ContractsFile = serde_yaml_ng::from_str(&raw).expect("parse contracts yaml");
    for row in file.contracts {
        if row.tested == "planned" {
            continue;
        }
        let dt = rivet_type_to_arrow(&row.rivet_type)
            .unwrap_or_else(|| panic!("{} {} must map to Arrow", row.source, row.source_type));
        assert_eq!(
            format!("{dt:?}"),
            row.arrow,
            "{} {} arrow mismatch",
            row.source,
            row.source_type
        );
        assert_eq!(
            row.fidelity,
            derive_fidelity(&row.rivet_type).label(),
            "{} {} fidelity",
            row.source,
            row.source_type
        );
    }
}

#[test]
fn contract_json_and_uuid_carry_logical_metadata() {
    let cases = [
        ("jsonb", RivetType::Json, Some("json")),
        ("uuid", RivetType::Uuid, Some("uuid")),
    ];
    let interval = TypeMapping::from_source(
        &SourceColumn::simple("dur", "interval", false),
        RivetType::Interval,
    );
    let field = build_arrow_field(&interval).expect("interval field");
    assert_eq!(field.data_type(), &DataType::Utf8);

    for (native, rivet, logical) in cases {
        let mapping = TypeMapping::from_source(&SourceColumn::simple("col", native, false), rivet);
        let field = build_arrow_field(&mapping).expect("field");
        assert_eq!(
            field
                .metadata()
                .get("rivet.logical_type")
                .map(String::as_str),
            logical,
            "logical_type for {native}"
        );
    }
}

#[test]
fn contract_binary_stays_binary_not_utf8() {
    let mapping = TypeMapping::from_source(
        &SourceColumn::simple("raw", "bytea", false),
        RivetType::Binary,
    );
    let field = build_arrow_field(&mapping).expect("field");
    assert_eq!(field.data_type(), &DataType::Binary);
}

#[test]
fn contract_nullable_flag_propagates() {
    let mapping =
        TypeMapping::from_source(&SourceColumn::simple("x", "text", true), RivetType::String);
    assert!(build_arrow_field(&mapping).expect("f").is_nullable());
}

#[test]
fn contract_timestamptz_carries_utc_not_none() {
    let tz = RivetType::Timestamp {
        unit: rivet::types::TimeUnit::Microsecond,
        timezone: Some("UTC".into()),
    };
    let dt = rivet_type_to_arrow(&tz).unwrap();
    match dt {
        DataType::Timestamp(_, Some(z)) => assert_eq!(z.as_ref(), "UTC"),
        other => panic!("expected UTC timestamp, got {other:?}"),
    }
}

#[test]
fn contract_no_silent_float_for_decimal_precision() {
    // Regression guard: money columns must never route through Float64.
    let mut seen = HashMap::new();
    let raw = load_fixture("expected_contracts.yaml");
    let file: ContractsFile = serde_yaml_ng::from_str(&raw).expect("parse");
    for row in file.contracts {
        if let RivetType::Decimal { precision, scale } = row.rivet_type {
            seen.insert((precision, scale), row.arrow.clone());
        }
    }
    assert!(seen.contains_key(&(18, 2)));
    assert!(seen.contains_key(&(20, 6)));
    for (ps, arrow) in seen {
        assert!(
            arrow.starts_with("Decimal"),
            "decimal({ps:?}) documented as {arrow}"
        );
    }
}
