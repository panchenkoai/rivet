//! Exact decimal string → scaled integer conversion (roadmap §12).
//!
//! The rule from the roadmap: **never convert decimal through f64**.
//!
//! Arrow Decimal128 stores a value as `i128 * 10^(-scale)`. So to
//! represent "123.45" in `Decimal128(18, 2)` we need `i128 = 12345`.
//!
//! This module provides one pure function — [`decimal_str_to_scaled_i128`] —
//! that converts a DB text-protocol decimal string to the scaled i128 Arrow
//! needs without ever touching floating-point arithmetic.

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
        frac_part[..scale_u as usize].parse().ok()?
    };

    let scale_factor = 10i128.pow(scale_u);
    let result = int_val
        .checked_mul(scale_factor)?
        .checked_add(frac_aligned)?;
    Some(if negative { -result } else { result })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
