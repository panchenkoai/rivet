//! How a column's values travel (ADR-0038 CP2/CP5): natively, or as one canonical text form.
//!
//! The renderers are pure functions of typed values, so a text column is byte-identical
//! whichever engine or mode (batch, CDC) wrote it (CP12).

use chrono::{Datelike, NaiveDateTime, Timelike};
use serde::Serialize;

/// Whether a column is written in its native Arrow type or as canonical text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Delivery {
    Native,
    Text(TextForm),
}

impl Delivery {
    /// True for [`Delivery::Native`].
    pub fn is_native(&self) -> bool {
        *self == Delivery::Native
    }
}

/// The closed set of canonical text renderings (ADR-0038 CP5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TextForm {
    /// Rendered by [`decimal_plain`].
    DecimalPlain,
    /// Rendered by [`iso8601_duration`].
    Iso8601Duration,
    /// Rendered by [`iso_timestamp_nanos`].
    IsoTimestampNanos,
    /// Rendered by [`time_of_day_offset`].
    TimeOfDayOffset,
    /// Rendered by [`time_beyond_day`].
    TimeBeyondDay,
    /// Rendered by [`uuid36`].
    Uuid36,
    /// Engine-produced: the source's RFC 8259 text, passed through.
    Json,
    /// Engine-produced WKB in lowercase hex; the SRID travels separately.
    HexWkb,
    /// Rendered by [`hex_bytes`].
    HexBytes,
    /// Rendered by [`bit_string`].
    BitString,
    /// Engine-produced: the server's canonical address text (`/n` for a network).
    InetText,
    /// Engine-produced: the server's range literal, e.g. `[lo,hi)`.
    RangeText,
    /// Engine-produced: the server's XML serialization.
    XmlText,
    /// Engine-produced: the server's own exact output for a type with no portable form.
    ServerText,
}

impl TextForm {
    /// The value written to the `rivet.text_form` field metadata.
    pub fn label(self) -> &'static str {
        match self {
            TextForm::DecimalPlain => "decimal_plain",
            TextForm::Iso8601Duration => "iso8601_duration",
            TextForm::IsoTimestampNanos => "iso_timestamp_nanos",
            TextForm::TimeOfDayOffset => "time_of_day_offset",
            TextForm::TimeBeyondDay => "time_beyond_day",
            TextForm::Uuid36 => "uuid36",
            TextForm::Json => "json",
            TextForm::HexWkb => "hex_wkb",
            TextForm::HexBytes => "hex_bytes",
            TextForm::BitString => "bit_string",
            TextForm::InetText => "inet_text",
            TextForm::RangeText => "range_text",
            TextForm::XmlText => "xml_text",
            TextForm::ServerText => "server_text",
        }
    }
}

/// A decimal as plain digits: no exponent, no grouping, exactly `scale` fraction digits.
pub fn decimal_plain(unscaled: i128, scale: i8) -> String {
    let sign = if unscaled < 0 { "-" } else { "" };
    let digits = unscaled.unsigned_abs().to_string();
    if scale <= 0 {
        let zeros = if unscaled == 0 { 0 } else { -scale as usize };
        return format!("{sign}{digits}{}", "0".repeat(zeros));
    }
    let scale = scale as usize;
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{sign}{int}.{frac}")
}

/// An ISO 8601 duration where every non-zero component carries its own sign (`P-1Y-2M3DT-4H`).
pub fn iso8601_duration(months: i32, days: i32, micros: i64) -> String {
    let mut out = String::from("P");
    let (years, months) = (months / 12, months % 12);
    for (v, unit) in [
        (years as i64, 'Y'),
        (months as i64, 'M'),
        (days as i64, 'D'),
    ] {
        if v != 0 {
            out.push_str(&format!("{v}{unit}"));
        }
    }
    if micros != 0 {
        let sign = if micros < 0 { "-" } else { "" };
        let abs = micros.unsigned_abs();
        let (h, m) = (abs / 3_600_000_000, abs / 60_000_000 % 60);
        let (s, frac) = (abs / 1_000_000 % 60, abs % 1_000_000);
        out.push('T');
        if h != 0 {
            out.push_str(&format!("{sign}{h}H"));
        }
        if m != 0 {
            out.push_str(&format!("{sign}{m}M"));
        }
        if s != 0 || frac != 0 {
            let f = format!("{frac:06}");
            let f = f.trim_end_matches('0');
            let dot = if f.is_empty() { "" } else { "." };
            out.push_str(&format!("{sign}{s}{dot}{f}S"));
        }
    }
    if out == "P" { "PT0S".into() } else { out }
}

/// `[±]YYYY-MM-DDTHH:MM:SS.fffffffff[Z]`: astronomical year, always nine fraction digits.
pub fn iso_timestamp_nanos(dt: NaiveDateTime, zoned: bool) -> String {
    let y = dt.year();
    let year = match y {
        0..=9999 => format!("{y:04}"),
        _ if y < 0 => format!("-{:04}", -y),
        _ => format!("+{y}"),
    };
    let (sec, nanos) = match dt.nanosecond() {
        n if n >= 1_000_000_000 => (dt.second() + 1, n - 1_000_000_000),
        n => (dt.second(), n),
    };
    format!(
        "{year}-{:02}-{:02}T{:02}:{:02}:{sec:02}.{nanos:09}{}",
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        if zoned { "Z" } else { "" }
    )
}

/// `[-]hh:mm:ss.ffffff` with as many hour digits as needed (at least two).
pub fn time_beyond_day(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let abs = micros.unsigned_abs();
    format!(
        "{sign}{:02}:{:02}:{:02}.{:06}",
        abs / 3_600_000_000,
        abs / 60_000_000 % 60,
        abs / 1_000_000 % 60,
        abs % 1_000_000
    )
}

/// `HH:MM:SS.ffffff±HH:MM`, offset east of UTC, `:SS` appended only for a seconds offset.
pub fn time_of_day_offset(micros_of_day: u64, offset_secs: i32) -> String {
    let sign = if offset_secs < 0 { '-' } else { '+' };
    let off = offset_secs.unsigned_abs();
    let secs = if off.is_multiple_of(60) {
        String::new()
    } else {
        format!(":{:02}", off % 60)
    };
    format!(
        "{:02}:{:02}:{:02}.{:06}{sign}{:02}:{:02}{secs}",
        micros_of_day / 3_600_000_000,
        micros_of_day / 60_000_000 % 60,
        micros_of_day / 1_000_000 % 60,
        micros_of_day % 1_000_000,
        off / 3600,
        off / 60 % 60
    )
}

/// Lowercase 8-4-4-4-12 UUID text.
pub fn uuid36(bytes: &[u8; 16]) -> String {
    let h = hex_bytes(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

/// Lowercase hex, no prefix.
pub fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// One `0`/`1` character per bit, most significant first.
pub fn bit_string(bits: &[bool]) -> String {
    bits.iter().map(|&b| if b { '1' } else { '0' }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, n: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_nano_opt(h, mi, s, n)
            .unwrap()
    }

    #[test]
    fn labels_and_serde_names_match_the_documented_strings() {
        let cases = [
            (TextForm::DecimalPlain, "decimal_plain"),
            (TextForm::Iso8601Duration, "iso8601_duration"),
            (TextForm::IsoTimestampNanos, "iso_timestamp_nanos"),
            (TextForm::TimeOfDayOffset, "time_of_day_offset"),
            (TextForm::TimeBeyondDay, "time_beyond_day"),
            (TextForm::Uuid36, "uuid36"),
            (TextForm::Json, "json"),
            (TextForm::HexWkb, "hex_wkb"),
            (TextForm::HexBytes, "hex_bytes"),
            (TextForm::BitString, "bit_string"),
            (TextForm::InetText, "inet_text"),
            (TextForm::RangeText, "range_text"),
            (TextForm::XmlText, "xml_text"),
            (TextForm::ServerText, "server_text"),
        ];
        for (form, want) in cases {
            assert_eq!(form.label(), want);
            assert_eq!(serde_json::to_string(&form).unwrap(), format!("\"{want}\""));
        }
        assert_eq!(
            serde_json::to_string(&Delivery::Native).unwrap(),
            "\"native\""
        );
        assert_eq!(
            serde_json::to_string(&Delivery::Text(TextForm::Uuid36)).unwrap(),
            r#"{"text":"uuid36"}"#
        );
    }

    #[test]
    fn decimal_plain_renders_sign_scale_and_leading_zeros() {
        assert_eq!(decimal_plain(12345, 2), "123.45");
        assert_eq!(decimal_plain(-12345, 2), "-123.45");
        assert_eq!(decimal_plain(5, 3), "0.005");
        assert_eq!(decimal_plain(-5, 3), "-0.005");
        assert_eq!(decimal_plain(150, 2), "1.50");
        assert_eq!(decimal_plain(0, 0), "0");
        assert_eq!(decimal_plain(0, 3), "0.000");
        assert_eq!(decimal_plain(42, 0), "42");
        assert_eq!(decimal_plain(-42, 0), "-42");
        assert_eq!(decimal_plain(12, -3), "12000");
        assert_eq!(decimal_plain(-12, -3), "-12000");
        assert_eq!(decimal_plain(0, -3), "0");
        assert_eq!(
            decimal_plain(i128::MIN, 0),
            "-170141183460469231731687303715884105728"
        );
        assert_eq!(
            decimal_plain(99999999999999999999999999999999999999, 38),
            "0.99999999999999999999999999999999999999"
        );
    }

    #[test]
    fn iso8601_duration_signs_each_component_and_trims_the_fraction() {
        assert_eq!(iso8601_duration(0, 0, 0), "PT0S");
        assert_eq!(
            iso8601_duration(14, 3, 14_706_789_000),
            "P1Y2M3DT4H5M6.789S"
        );
        assert_eq!(iso8601_duration(-14, 0, 0), "P-1Y-2M");
        assert_eq!(iso8601_duration(12, 0, 0), "P1Y");
        assert_eq!(iso8601_duration(-1, 5, 0), "P-1M5D");
        assert_eq!(iso8601_duration(0, -1, 3_600_000_000), "P-1DT1H");
        assert_eq!(iso8601_duration(0, 0, -14_706_000_000), "PT-4H-5M-6S");
        assert_eq!(iso8601_duration(0, 0, -500_000), "PT-0.5S");
        assert_eq!(iso8601_duration(0, 0, 1), "PT0.000001S");
        assert_eq!(iso8601_duration(0, 0, 60_000_000), "PT1M");
        assert_eq!(iso8601_duration(0, 0, 90_000_000_000), "PT25H");
    }

    #[test]
    fn iso_timestamp_nanos_has_nine_digits_astronomical_year_and_z() {
        assert_eq!(
            iso_timestamp_nanos(ts(2024, 2, 29, 13, 4, 5, 123_456_789), true),
            "2024-02-29T13:04:05.123456789Z"
        );
        assert_eq!(
            iso_timestamp_nanos(ts(1970, 1, 1, 0, 0, 0, 0), false),
            "1970-01-01T00:00:00.000000000"
        );
        assert_eq!(
            iso_timestamp_nanos(ts(12, 3, 4, 5, 6, 7, 8), false),
            "0012-03-04T05:06:07.000000008"
        );
        assert_eq!(
            iso_timestamp_nanos(ts(0, 1, 1, 0, 0, 0, 0), true),
            "0000-01-01T00:00:00.000000000Z"
        );
        assert_eq!(
            iso_timestamp_nanos(ts(-44, 3, 15, 12, 0, 0, 0), false),
            "-0044-03-15T12:00:00.000000000"
        );
        assert_eq!(
            iso_timestamp_nanos(ts(10000, 1, 1, 0, 0, 0, 1), true),
            "+10000-01-01T00:00:00.000000001Z"
        );
        assert_eq!(
            iso_timestamp_nanos(ts(2016, 12, 31, 23, 59, 59, 1_500_000_000), true),
            "2016-12-31T23:59:60.500000000Z"
        );
    }

    #[test]
    fn time_beyond_day_renders_long_and_negative_hours() {
        assert_eq!(time_beyond_day(3_020_399_000_000), "838:59:59.000000");
        assert_eq!(time_beyond_day(-3_020_399_000_000), "-838:59:59.000000");
        assert_eq!(time_beyond_day(-1_000_000), "-00:00:01.000000");
        assert_eq!(time_beyond_day(0), "00:00:00.000000");
        assert_eq!(time_beyond_day(90_000_000_001), "25:00:00.000001");
    }

    #[test]
    fn time_of_day_offset_renders_offset_sign_and_seconds() {
        assert_eq!(
            time_of_day_offset(45_296_789_012, 19_800),
            "12:34:56.789012+05:30"
        );
        assert_eq!(time_of_day_offset(0, 0), "00:00:00.000000+00:00");
        assert_eq!(
            time_of_day_offset(86_400_000_000, -28_800),
            "24:00:00.000000-08:00"
        );
        assert_eq!(time_of_day_offset(1, 9_017), "00:00:00.000001+02:30:17");
        assert_eq!(time_of_day_offset(1, -9_017), "00:00:00.000001-02:30:17");
    }

    #[test]
    fn uuid36_hex_bytes_and_bit_string_are_lowercase_and_unprefixed() {
        let u = [
            0x12, 0x3E, 0x45, 0x67, 0xE8, 0x9B, 0x12, 0xD3, 0xA4, 0x56, 0x42, 0x66, 0x14, 0x17,
            0x40, 0x00,
        ];
        assert_eq!(uuid36(&u), "123e4567-e89b-12d3-a456-426614174000");
        assert_eq!(uuid36(&[0; 16]), "00000000-0000-0000-0000-000000000000");
        assert_eq!(hex_bytes(&[0x00, 0x0A, 0xFF]), "000aff");
        assert_eq!(hex_bytes(&[]), "");
        assert_eq!(bit_string(&[true, false, true, true]), "1011");
        assert_eq!(bit_string(&[]), "");
    }
}
