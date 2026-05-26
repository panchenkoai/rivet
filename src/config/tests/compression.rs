//! Compression-profile (`fast`/`balanced`/`compact`/`none`) preset parsing.

use super::*;

#[test]
fn compression_profile_none_maps_to_uncompressed() {
    let (ct, level) = CompressionProfile::None.to_codec();
    assert_eq!(ct, CompressionType::None);
    assert!(level.is_none());
}

#[test]
fn compression_profile_fast_maps_to_snappy() {
    let (ct, level) = CompressionProfile::Fast.to_codec();
    assert_eq!(ct, CompressionType::Snappy);
    assert!(level.is_none(), "snappy has no level, got: {level:?}");
}

#[test]
fn compression_profile_balanced_maps_to_zstd_level_3() {
    let (ct, level) = CompressionProfile::Balanced.to_codec();
    assert_eq!(ct, CompressionType::Zstd);
    assert_eq!(level, Some(3));
}

#[test]
fn compression_profile_compact_maps_to_zstd_level_9() {
    let (ct, level) = CompressionProfile::Compact.to_codec();
    assert_eq!(ct, CompressionType::Zstd);
    assert_eq!(level, Some(9));
}

#[test]
fn compression_profile_takes_precedence_over_legacy_compression_field() {
    // When compression_profile is set, effective_compression must return the profile codec,
    // ignoring the legacy `compression` + `compression_level` fields.
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: events
    query: "SELECT 1"
    format: parquet
    compression: snappy
    compression_profile: compact
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let (ct, level) = cfg.exports[0].effective_compression();
    assert_eq!(
        ct,
        CompressionType::Zstd,
        "profile must win over legacy field"
    );
    assert_eq!(level, Some(9), "compact profile must set level=9");
}

#[test]
fn legacy_compression_field_used_when_no_profile_set() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: events
    query: "SELECT 1"
    format: parquet
    compression: snappy
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let (ct, level) = cfg.exports[0].effective_compression();
    assert_eq!(ct, CompressionType::Snappy);
    assert!(level.is_none());
}

#[test]
fn compression_profile_parses_from_yaml_balanced() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: events
    query: "SELECT 1"
    format: parquet
    compression_profile: balanced
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert_eq!(
        cfg.exports[0].compression_profile,
        Some(CompressionProfile::Balanced)
    );
    let (ct, level) = cfg.exports[0].effective_compression();
    assert_eq!(ct, CompressionType::Zstd);
    assert_eq!(level, Some(3));
}

#[test]
fn no_compression_profile_falls_through_to_default_compression_type() {
    // Default CompressionType is Zstd (derive(Default) on the enum).
    // When no profile is set, effective_compression returns the raw compression field.
    let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
    let (ct, level) = cfg.exports[0].effective_compression();
    assert!(
        cfg.exports[0].compression_profile.is_none(),
        "minimal YAML must not set a compression_profile"
    );
    // compression field default is Zstd; level is None (no compression_level set).
    assert_eq!(ct, CompressionType::Zstd);
    assert!(level.is_none());
}

#[test]
fn compression_profile_label_strings() {
    assert_eq!(CompressionProfile::None.label(), "none");
    assert_eq!(CompressionProfile::Fast.label(), "fast");
    assert_eq!(CompressionProfile::Balanced.label(), "balanced");
    assert_eq!(CompressionProfile::Compact.label(), "compact");
}
