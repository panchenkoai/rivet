//! One classification of an Oracle column's driver type; every type decision in the
//! adapter matches on it instead of on the driver's `DB_TYPE_*` names.

use oracledb::Metadata;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OraKind {
    Number,
    BinaryFloat,
    BinaryDouble,
    Boolean,
    Date,
    Timestamp,
    TimestampTz,
    TimestampLtz,
    IntervalDs,
    IntervalYm,
    /// VARCHAR2 / NVARCHAR2 / CHAR / NCHAR / LONG.
    Text,
    /// CLOB / NCLOB.
    Clob,
    Blob,
    /// RAW / LONG RAW.
    Raw,
    /// ROWID / UROWID.
    Rowid,
    Json,
    /// XMLTYPE, which the driver describes as an object type, or any other object type.
    Object,
    Vector,
    Other,
}

impl OraKind {
    pub(super) fn of(meta: &Metadata) -> Self {
        Self::from_driver_name(meta.db_type().name())
    }

    fn from_driver_name(name: &str) -> Self {
        match name {
            "DB_TYPE_NUMBER" => Self::Number,
            "DB_TYPE_BINARY_FLOAT" => Self::BinaryFloat,
            "DB_TYPE_BINARY_DOUBLE" => Self::BinaryDouble,
            "DB_TYPE_BOOLEAN" => Self::Boolean,
            "DB_TYPE_DATE" => Self::Date,
            "DB_TYPE_TIMESTAMP" => Self::Timestamp,
            "DB_TYPE_TIMESTAMP_TZ" => Self::TimestampTz,
            "DB_TYPE_TIMESTAMP_LTZ" => Self::TimestampLtz,
            "DB_TYPE_INTERVAL_DS" => Self::IntervalDs,
            "DB_TYPE_INTERVAL_YM" => Self::IntervalYm,
            "DB_TYPE_VARCHAR"
            | "DB_TYPE_NVARCHAR"
            | "DB_TYPE_CHAR"
            | "DB_TYPE_NCHAR"
            | "DB_TYPE_LONG"
            | "DB_TYPE_LONG_NVARCHAR" => Self::Text,
            "DB_TYPE_CLOB" | "DB_TYPE_NCLOB" => Self::Clob,
            "DB_TYPE_BLOB" => Self::Blob,
            "DB_TYPE_RAW" | "DB_TYPE_LONG_RAW" => Self::Raw,
            "DB_TYPE_ROWID" | "DB_TYPE_UROWID" => Self::Rowid,
            "DB_TYPE_JSON" => Self::Json,
            "DB_TYPE_XMLTYPE" | "DB_TYPE_OBJECT" => Self::Object,
            "DB_TYPE_VECTOR" => Self::Vector,
            _ => Self::Other,
        }
    }

    /// The server-side expression a column of this kind is fetched through, or `None`
    /// to fetch it as is. The driver panics decoding a region-named zone, and cannot
    /// decode ROWID, JSON, object types or VECTOR at all.
    pub(super) fn projection(self, quoted: &str) -> Option<String> {
        Some(match self {
            Self::TimestampTz | Self::TimestampLtz => format!("SYS_EXTRACT_UTC({quoted})"),
            Self::Rowid => format!("ROWIDTOCHAR({quoted})"),
            Self::Json => format!("JSON_SERIALIZE({quoted} RETURNING CLOB)"),
            Self::Object => format!("XMLSERIALIZE(CONTENT {quoted} AS CLOB)"),
            Self::Vector => format!("FROM_VECTOR({quoted} RETURNING CLOB)"),
            _ => return None,
        })
    }

    /// The driver returns a zero-length LOB as NULL; these kinds carry a server-side flag.
    pub(super) fn needs_empty_flag(self) -> bool {
        matches!(self, Self::Clob | Self::Blob)
    }
}

/// The lowercase native label (`number`, `timestamp_tz`, …) of a driver type name.
pub(super) fn native_label(meta: &Metadata) -> String {
    meta.db_type()
        .name()
        .trim_start_matches("DB_TYPE_")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_driver_name_rivet_decides_on_has_its_kind() {
        let k = OraKind::from_driver_name;
        assert_eq!(k("DB_TYPE_NUMBER"), OraKind::Number);
        assert_eq!(k("DB_TYPE_BINARY_FLOAT"), OraKind::BinaryFloat);
        assert_eq!(k("DB_TYPE_BINARY_DOUBLE"), OraKind::BinaryDouble);
        assert_eq!(k("DB_TYPE_BOOLEAN"), OraKind::Boolean);
        assert_eq!(k("DB_TYPE_DATE"), OraKind::Date);
        assert_eq!(k("DB_TYPE_TIMESTAMP"), OraKind::Timestamp);
        assert_eq!(k("DB_TYPE_TIMESTAMP_TZ"), OraKind::TimestampTz);
        assert_eq!(k("DB_TYPE_TIMESTAMP_LTZ"), OraKind::TimestampLtz);
        assert_eq!(k("DB_TYPE_INTERVAL_DS"), OraKind::IntervalDs);
        assert_eq!(k("DB_TYPE_INTERVAL_YM"), OraKind::IntervalYm);
        for t in [
            "VARCHAR",
            "NVARCHAR",
            "CHAR",
            "NCHAR",
            "LONG",
            "LONG_NVARCHAR",
        ] {
            assert_eq!(k(&format!("DB_TYPE_{t}")), OraKind::Text, "{t}");
        }
        assert_eq!(k("DB_TYPE_CLOB"), OraKind::Clob);
        assert_eq!(k("DB_TYPE_NCLOB"), OraKind::Clob);
        assert_eq!(k("DB_TYPE_BLOB"), OraKind::Blob);
        assert_eq!(k("DB_TYPE_RAW"), OraKind::Raw);
        assert_eq!(k("DB_TYPE_LONG_RAW"), OraKind::Raw);
        assert_eq!(k("DB_TYPE_ROWID"), OraKind::Rowid);
        assert_eq!(k("DB_TYPE_UROWID"), OraKind::Rowid);
        assert_eq!(k("DB_TYPE_JSON"), OraKind::Json);
        assert_eq!(k("DB_TYPE_XMLTYPE"), OraKind::Object);
        assert_eq!(k("DB_TYPE_OBJECT"), OraKind::Object);
        assert_eq!(k("DB_TYPE_VECTOR"), OraKind::Vector);
        assert_eq!(k("DB_TYPE_BFILE"), OraKind::Other);
    }

    #[test]
    fn only_undecodable_kinds_are_reprojected() {
        let q = "\"C\"";
        assert_eq!(
            OraKind::TimestampTz.projection(q).as_deref(),
            Some("SYS_EXTRACT_UTC(\"C\")")
        );
        assert_eq!(
            OraKind::TimestampLtz.projection(q).as_deref(),
            Some("SYS_EXTRACT_UTC(\"C\")")
        );
        assert_eq!(
            OraKind::Rowid.projection(q).as_deref(),
            Some("ROWIDTOCHAR(\"C\")")
        );
        assert_eq!(
            OraKind::Json.projection(q).as_deref(),
            Some("JSON_SERIALIZE(\"C\" RETURNING CLOB)")
        );
        assert_eq!(
            OraKind::Object.projection(q).as_deref(),
            Some("XMLSERIALIZE(CONTENT \"C\" AS CLOB)")
        );
        assert_eq!(
            OraKind::Vector.projection(q).as_deref(),
            Some("FROM_VECTOR(\"C\" RETURNING CLOB)")
        );
        for k in [
            OraKind::Number,
            OraKind::Timestamp,
            OraKind::Clob,
            OraKind::IntervalDs,
        ] {
            assert_eq!(k.projection(q), None, "{k:?}");
        }
        assert!(OraKind::Clob.needs_empty_flag() && OraKind::Blob.needs_empty_flag());
        assert!(!OraKind::Raw.needs_empty_flag() && !OraKind::Text.needs_empty_flag());
    }
}
