//! Exact decimal string → scaled integer conversion (roadmap §12).
//!
//! The rule from the roadmap: **never convert decimal through f64**.
//!
//! Arrow Decimal128 stores a value as `i128 * 10^(-scale)`. So to
//! represent "123.45" in `Decimal128(18, 2)` we need `i128 = 12345`.
//!
//! This module converts a DB text-protocol decimal string to the scaled
//! integer Arrow needs without ever touching floating-point arithmetic —
//! `i128` for `Decimal128` (precision ≤ 38) and `i256` for `Decimal256`
//! (precision 39–76).

use arrow::datatypes::i256;

/// Convert a decimal string (as returned by the database text protocol) to
/// a scaled `i128` ready to be stored in an Arrow `Decimal128` array.
///
/// `scale` is the Rivet/Arrow scale parameter: the result satisfies
/// `actual_value = result * 10^(-scale)`, i.e. `result = actual_value * 10^scale`.
///
/// Returns `None` for strings that are empty, contain non-numeric characters,
/// or whose values would overflow `i128`. The caller is responsible for
/// distinguishing SQL `NULL` (which never reaches this function) from parse
/// errors.
///
/// # Examples
///
/// ```ignore
/// use rivet::types::decimal::decimal_str_to_scaled_i128;
/// assert_eq!(decimal_str_to_scaled_i128("123.45",  2), Some(12345));
/// assert_eq!(decimal_str_to_scaled_i128("0.10",    2), Some(10));
/// assert_eq!(decimal_str_to_scaled_i128("-1.23",   2), Some(-123));
/// assert_eq!(decimal_str_to_scaled_i128("1200",   -2), Some(12));
/// assert_eq!(decimal_str_to_scaled_i128("",        2), None);
/// ```
/// Format a scaled `Decimal128` value as exact decimal text (no float).
pub fn scaled_i128_to_decimal_str(value: i128, scale: i8) -> String {
    if scale < 0 {
        let factor = 10i128.pow(scale.unsigned_abs() as u32);
        return (value * factor).to_string();
    }
    let scale_u = scale as u32;
    if scale_u == 0 {
        return value.to_string();
    }
    let factor = 10i128.pow(scale_u);
    let negative = value < 0;
    let abs = value.abs();
    let int_part = abs / factor;
    let frac = abs % factor;
    format!(
        "{sign}{int_part}.{frac:0width$}",
        sign = if negative { "-" } else { "" },
        width = scale_u as usize
    )
}

pub fn decimal_str_to_scaled_i128(s: &str, scale: i8) -> Option<i128> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let negative = s.starts_with('-');
    let s = if negative {
        &s[1..]
    } else {
        s.trim_start_matches('+')
    };

    if scale < 0 {
        // Negative scale: the stored integer represents multiples of 10^|scale|.
        // E.g. scale=-2 means the column stores whole hundreds: "1200" → 12.
        let divisor = 10i128.pow(scale.unsigned_abs() as u32);
        let int_val: i128 = s.split('.').next()?.trim().parse().ok()?;
        let result = int_val.checked_div(divisor)?;
        return Some(if negative { -result } else { result });
    }

    let scale_u = scale as u32;

    // Split on the decimal point (may be absent for integer-valued decimals).
    let (int_part, frac_part) = if let Some(dot) = s.find('.') {
        (&s[..dot], &s[dot + 1..])
    } else {
        (s, "")
    };

    let int_val: i128 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().ok()?
    };

    let frac_aligned: i128 = if scale_u == 0 {
        0
    } else if frac_part.len() < scale_u as usize {
        // Pad right with zeros: "0.1" with scale=2 → frac "10" → 10
        let mut buf = String::with_capacity(scale_u as usize);
        buf.push_str(frac_part);
        for _ in 0..(scale_u as usize - frac_part.len()) {
            buf.push('0');
        }
        buf.parse().ok()?
    } else {
        // Truncate to exactly `scale` digits. If the source column truly has
        // scale=2 and a DB value arrives with more digits, that is either a
        // type mismatch or a DB rounding artefact — we preserve the declared
        // scale rather than silently extending it.
        frac_part.get(..scale_u as usize)?.parse().ok()?
    };

    let scale_factor = 10i128.pow(scale_u);
    let result = int_val
        .checked_mul(scale_factor)?
        .checked_add(frac_aligned)?;
    Some(if negative { -result } else { result })
}

/// `Decimal256` analogue of [`decimal_str_to_scaled_i128`] — parses straight
/// into `i256` so values beyond `i128` (precision 39–76) are not truncated.
/// Returns `None` for empty / non-numeric strings or `i256` overflow.
pub fn decimal_str_to_scaled_i256(s: &str, scale: i8) -> Option<i256> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let negative = s.starts_with('-');
    let s = if negative {
        &s[1..]
    } else {
        s.trim_start_matches('+')
    };

    if scale < 0 {
        let divisor = pow10_i256(scale.unsigned_abs() as u32)?;
        let int_val = i256::from_string(s.split('.').next()?.trim())?;
        let result = int_val.checked_div(divisor)?;
        return Some(if negative { -result } else { result });
    }

    let scale_u = scale as u32;
    let (int_part, frac_part) = match s.find('.') {
        Some(dot) => (&s[..dot], &s[dot + 1..]),
        None => (s, ""),
    };
    let int_val = if int_part.is_empty() {
        i256::ZERO
    } else {
        i256::from_string(int_part)?
    };
    let frac_aligned = if scale_u == 0 {
        i256::ZERO
    } else if frac_part.len() < scale_u as usize {
        let mut buf = String::with_capacity(scale_u as usize);
        buf.push_str(frac_part);
        for _ in 0..(scale_u as usize - frac_part.len()) {
            buf.push('0');
        }
        i256::from_string(&buf)?
    } else {
        i256::from_string(frac_part.get(..scale_u as usize)?)?
    };

    let scale_factor = pow10_i256(scale_u)?;
    let result = int_val
        .checked_mul(scale_factor)?
        .checked_add(frac_aligned)?;
    Some(if negative { -result } else { result })
}

/// Scale an integer (already widened to `i128`) by `10^scale` into `i256` — the
/// `Decimal256` analogue of the source drivers' `scale_int_to_i128`. `None` on
/// negative scale or `i256` overflow.
pub fn scale_int_to_i256(v: i128, scale: i8) -> Option<i256> {
    if scale < 0 {
        return None;
    }
    i256::from_i128(v).checked_mul(pow10_i256(scale as u32)?)
}

/// `10^n` as `i256` (up to 10^76, the `Decimal256` scale ceiling); `None` if it
/// would exceed `i256`. Repeated `checked_mul` rather than the old
/// `format!("1{0:0}")` + `i256::from_string` — no per-value string allocation
/// or parse on the Decimal256 scale path; same overflow contract.
fn pow10_i256(n: u32) -> Option<i256> {
    let ten = i256::from_i128(10);
    let mut acc = i256::from_i128(1);
    for _ in 0..n {
        acc = acc.checked_mul(ten)?;
    }
    Some(acc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaled_to_str_roundtrip_financial() {
        assert_eq!(scaled_i128_to_decimal_str(10, 2), "0.10");
        assert_eq!(scaled_i128_to_decimal_str(12345, 2), "123.45");
        assert_eq!(scaled_i128_to_decimal_str(-123, 2), "-1.23");
        assert_eq!(scaled_i128_to_decimal_str(10_123_456, 6), "10.123456");
    }

    #[test]
    fn standard_financial_values() {
        assert_eq!(decimal_str_to_scaled_i128("0.10", 2), Some(10));
        assert_eq!(decimal_str_to_scaled_i128("0.20", 2), Some(20));
        assert_eq!(decimal_str_to_scaled_i128("0.30", 2), Some(30));
        assert_eq!(decimal_str_to_scaled_i128("123.45", 2), Some(12345));
        assert_eq!(decimal_str_to_scaled_i128("-1.23", 2), Some(-123));
        assert_eq!(decimal_str_to_scaled_i128("-100.05", 2), Some(-10005));
    }

    /// Roadmap §12 golden-test values: SUM(amount) must equal 999999999900.24
    #[test]
    fn golden_test_payment_values() {
        let rows = [
            ("0.10", 10i128),
            ("0.20", 20),
            ("999999999999.99", 99999999999999),
            ("-100.05", -10005),
        ];
        let sum: i128 = rows.iter().map(|(_, v)| v).sum();
        // 10 + 20 + 99999999999999 + (-10005) = 99999999990024
        // = 999999999900.24 × 100
        assert_eq!(sum, 99999999990024);

        for (s, expected) in &rows {
            assert_eq!(
                decimal_str_to_scaled_i128(s, 2),
                Some(*expected),
                "mismatch for '{s}'"
            );
        }
    }

    #[test]
    fn integer_valued_decimal_with_nonzero_scale() {
        assert_eq!(decimal_str_to_scaled_i128("100", 2), Some(10000));
        assert_eq!(decimal_str_to_scaled_i128("0", 2), Some(0));
    }

    #[test]
    fn frac_shorter_than_scale_is_right_padded() {
        // "0.1" with scale=3 means 0.100 → 100
        assert_eq!(decimal_str_to_scaled_i128("0.1", 3), Some(100));
        // "5.4" with scale=6 means 5.400000 → 5400000
        assert_eq!(decimal_str_to_scaled_i128("5.4", 6), Some(5_400_000));
    }

    #[test]
    fn negative_scale_represents_large_round_numbers() {
        // scale=-2: values are multiples of 100
        assert_eq!(decimal_str_to_scaled_i128("1200", -2), Some(12));
        assert_eq!(decimal_str_to_scaled_i128("50000", -2), Some(500));
    }

    #[test]
    fn zero_scale_ignores_fractional_digits() {
        assert_eq!(decimal_str_to_scaled_i128("42", 0), Some(42));
        assert_eq!(decimal_str_to_scaled_i128("42.0", 0), Some(42));
    }

    #[test]
    fn null_like_empty_string_returns_none() {
        assert_eq!(decimal_str_to_scaled_i128("", 2), None);
        assert_eq!(decimal_str_to_scaled_i128("  ", 2), None);
    }

    #[test]
    fn non_numeric_string_returns_none() {
        assert_eq!(decimal_str_to_scaled_i128("NaN", 2), None);
        assert_eq!(decimal_str_to_scaled_i128("Infinity", 2), None);
    }

    #[test]
    fn large_precision_near_i128_boundary() {
        // Decimal128 max precision=38, so the largest i128 value is ~1.7e38
        // This just verifies we don't overflow for values within p=18,s=0
        let big = "999999999999999999"; // 18 nines
        assert_eq!(
            decimal_str_to_scaled_i128(big, 0),
            Some(999_999_999_999_999_999i128)
        );
    }

    /// Overflow edge cases: a value beyond `i128` range must return `None`,
    /// never panic or wrap. Guards the `checked_mul`/`checked_add` + parse
    /// paths. See CLAUDE.md "Remediation hints must recover from the degraded
    /// state" — a wrapped decimal is exactly the silent corruption we forbid.
    #[test]
    fn value_beyond_i128_returns_none_not_panic() {
        // 40-digit integer cannot fit i128 → parse fails → None.
        let too_big = format!("1{}", "0".repeat(40));
        assert_eq!(decimal_str_to_scaled_i128(&too_big, 0), None);

        // 38 nines fits i128 (~1e38 < i128::MAX ~1.7e38) at scale 0 …
        let max_digits = "9".repeat(38);
        assert!(decimal_str_to_scaled_i128(&max_digits, 0).is_some());
        // … but scaling it by 10^2 overflows i128 → None, not a wrap.
        assert_eq!(decimal_str_to_scaled_i128(&max_digits, 2), None);

        // Fractional overflow: huge integer part + any scale.
        assert_eq!(
            decimal_str_to_scaled_i128(&format!("{max_digits}.5"), 5),
            None
        );
    }

    // ── i256 (Decimal256) path: the i128 bottleneck is gone ──────────────────

    #[test]
    fn i256_handles_values_beyond_i128() {
        // A 45-digit integer overflows i128 (~38 digits) but fits i256.
        let big = "123456789012345678901234567890123456789012345";
        assert_eq!(decimal_str_to_scaled_i128(big, 0), None, "i128 overflows");
        assert_eq!(
            decimal_str_to_scaled_i256(big, 0).unwrap(),
            i256::from_string(big).unwrap()
        );
        // With a fractional part scaled in.
        let v = decimal_str_to_scaled_i256("123456789012345678901234567890123456789012.345", 3)
            .unwrap();
        assert_eq!(
            v,
            i256::from_string("123456789012345678901234567890123456789012345").unwrap()
        );
    }

    #[test]
    fn i256_matches_i128_for_in_range_values() {
        for (s, scale) in [("123.45", 2i8), ("-1.23", 2), ("0.10", 2), ("1200", -2)] {
            let small = decimal_str_to_scaled_i128(s, scale).unwrap();
            assert_eq!(
                decimal_str_to_scaled_i256(s, scale).unwrap(),
                i256::from_i128(small),
                "i256 and i128 must agree for in-range value {s}"
            );
        }
    }

    #[test]
    fn scale_int_to_i256_scales_beyond_i128() {
        // u64::MAX (~1.8e19) scaled by 10^30 ≈ 1.8e49 — fits i256.
        assert!(scale_int_to_i256(u64::MAX as i128, 30).is_some());
        assert_eq!(scale_int_to_i256(5, 2), Some(i256::from_i128(500)));
        assert_eq!(scale_int_to_i256(123, -1), None, "negative scale rejected");
    }

    #[test]
    fn pow10_i256_matches_string_form_and_respects_ceiling() {
        // The checked_mul loop must equal the old format!+from_string for every
        // power, span the i128 boundary, reach the Decimal256 ceiling (10^76),
        // and overflow to None beyond it.
        for n in [0u32, 1, 5, 18, 38, 39, 76] {
            let expected = i256::from_string(&format!("1{}", "0".repeat(n as usize)));
            assert_eq!(pow10_i256(n), expected, "10^{n} mismatch");
        }
        assert!(pow10_i256(76).is_some(), "10^76 fits i256");
        assert!(pow10_i256(77).is_none(), "10^77 overflows i256 → None");
    }
}

#[cfg(test)]
mod fuzz {
    use super::*;

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, ..Default::default()
        })]

        // Total on arbitrary text; EXACT on well-formed digit strings — the
        // one decimal canon must scale int_part·10^s + frac verbatim.
        #[test]
        fn decimal_parsers_total_and_exact(
            junk in ".{0,60}",
            int_part in 0u64..1_000_000_000_000,
            frac in 0u32..10_000,
            neg in proptest::prelude::any::<bool>(),
        ) {
            let _ = decimal_str_to_scaled_i128(&junk, 4);
            let _ = decimal_str_to_scaled_i256(&junk, 4);
            let s = format!("{}{}.{:04}", if neg { "-" } else { "" }, int_part, frac);
            let expect = {
                let mag = int_part as i128 * 10_000 + frac as i128;
                if neg { -mag } else { mag }
            };
            proptest::prop_assert_eq!(decimal_str_to_scaled_i128(&s, 4), Some(expect));
            proptest::prop_assert_eq!(
                decimal_str_to_scaled_i256(&s, 4),
                Some(arrow::datatypes::i256::from_i128(expect))
            );
        }
    }
}
