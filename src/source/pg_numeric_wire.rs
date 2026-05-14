//! PostgreSQL binary `numeric` wire format decoder (`numeric.c::numeric_recv`).
//!
//! Payload layout (`big-endian`):
//!
//! ```text
//! uint16 ndigits
//! i16 weight
//! uint16 sign
//! uint16 dscale           // informational; excluded from reconstruction (see pg× driver notes)
//! int16 digits[ndigits]   // base-10000 limbs, values in [0, 9999]
//! ```

use std::io::Cursor;

use bigdecimal::num_bigint::{BigInt, Sign};
use bigdecimal::{BigDecimal, Zero};
use byteorder::{BigEndian, ReadBytesExt};
use postgres_types::{FromSql, Type};

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;
const PG_NBASE: i64 = 10_000;

/// Borrowed Postgres `numeric` OID payload (`Type::NUMERIC`).
#[derive(Debug, Clone, Copy)]
pub struct PgNumericWire<'a>(pub &'a [u8]);

impl<'a> FromSql<'a> for PgNumericWire<'a> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if ty != &Type::NUMERIC {
            return Err(format!("expected NUMERIC oid, got {ty}",).into());
        }
        Ok(PgNumericWire(raw))
    }

    fn accepts(ty: &Type) -> bool {
        ty == &Type::NUMERIC
    }
}

/// Plain decimal text suitable for [`crate::types::decimal::decimal_str_to_scaled_i128`].
///
/// NaN / ±Infinity payloads yield `None` (no deterministic decimal literal).
pub fn numeric_wire_normalized_plain(wire: &[u8]) -> Option<String> {
    let bd = wire_to_big_decimal(wire)?;

    Some(bd.normalized().to_plain_string().trim().to_string())
}

fn wire_to_big_decimal(wire: &[u8]) -> Option<BigDecimal> {
    let mut cur = Cursor::new(wire);
    let ndigits = cur.read_u16::<BigEndian>().ok()? as usize;
    let weight = cur.read_i16::<BigEndian>().ok()?;
    let sign_field = cur.read_u16::<BigEndian>().ok()?;
    let _display_scale = cur.read_u16::<BigEndian>().ok()?;

    if wire.len().checked_sub(8)? < ndigits.saturating_mul(2) {
        return None;
    }

    match sign_field {
        NUMERIC_POS | NUMERIC_NEG => {}
        NUMERIC_NAN | NUMERIC_PINF | NUMERIC_NINF => return None,
        _ => return None,
    };

    let sign = if sign_field == NUMERIC_NEG {
        Sign::Minus
    } else {
        Sign::Plus
    };

    if ndigits == 0 {
        return Some(BigDecimal::zero());
    }

    let mut limbs = Vec::<i16>::with_capacity(ndigits);
    for _ in 0..ndigits {
        let d = cur.read_i16::<BigEndian>().ok()?;
        if !(0..PG_NBASE).contains(&(d as i64)) {
            return None;
        }
        limbs.push(d);
    }

    // Prisma/quaint scheme: expand each base-10000 limb into two base-100 octets.
    let scale = ((limbs.len() as i64) - i64::from(weight) - 1) * 4;
    let mut cents = Vec::<u8>::with_capacity(limbs.len().saturating_mul(2));
    for limb in limbs {
        cents.push((limb / 100) as u8);
        cents.push((limb % 100) as u8);
    }

    let coef = BigInt::from_radix_be(sign, &cents, 100)?;

    Some(BigDecimal::new(coef, scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a raw PG numeric wire payload from its constituent fields.
    ///
    /// Layout (all big-endian):
    ///   uint16 ndigits | i16 weight | uint16 sign | uint16 dscale | i16 digits[ndigits]
    fn encode(ndigits: u16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + digits.len() * 2);
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for &d in digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    // ── zero / empty ─────────────────────────────────────────────────────────

    #[test]
    fn zero_ndigits_gives_zero() {
        // ndigits=0 → early-return BigDecimal::zero()
        let wire = encode(0, 0, NUMERIC_POS, 0, &[]);
        assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some("0"));
    }

    // ── positive integers ────────────────────────────────────────────────────

    #[test]
    fn value_one() {
        // 1 * 10000^0 = 1; limb=[1], weight=0
        let wire = encode(1, 0, NUMERIC_POS, 0, &[1]);
        assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some("1"));
    }

    #[test]
    fn value_one_hundred() {
        let wire = encode(1, 0, NUMERIC_POS, 0, &[100]);
        assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some("100"));
    }

    #[test]
    fn value_9999_single_limb() {
        let wire = encode(1, 0, NUMERIC_POS, 0, &[9999]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("9999")
        );
    }

    #[test]
    fn value_10000_two_limbs() {
        // 10000 = 1 * 10000^1 + 0 * 10000^0; weight=1, limbs=[1, 0]
        let wire = encode(2, 1, NUMERIC_POS, 0, &[1, 0]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("10000")
        );
    }

    // ── negative values ───────────────────────────────────────────────────────

    #[test]
    fn negative_five() {
        let wire = encode(1, 0, NUMERIC_NEG, 0, &[5]);
        assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some("-5"));
    }

    #[test]
    fn negative_large() {
        // -12345: 1 * 10000^1 + 2345 * 10000^0, weight=1
        let wire = encode(2, 1, NUMERIC_NEG, 0, &[1, 2345]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("-12345")
        );
    }

    // ── fractional values ────────────────────────────────────────────────────

    #[test]
    fn value_point_01() {
        // 0.01 = 100 * 10000^(-1); weight=-1, limb=[100]
        // scale = (1 - (-1) - 1) * 4 = 4 → BigDecimal::new(100, 4) = 0.0100 → "0.01"
        let wire = encode(1, -1, NUMERIC_POS, 2, &[100]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("0.01")
        );
    }

    #[test]
    fn value_12_dot_34() {
        // 12.34: limbs=[12, 3400], weight=0
        // scale = (2 - 0 - 1)*4 = 4; cents=[0,12,34,0]; coef=123400
        // BigDecimal::new(123400, 4) = 12.3400 → normalized "12.34"
        let wire = encode(2, 0, NUMERIC_POS, 2, &[12, 3400]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("12.34")
        );
    }

    #[test]
    fn negative_fractional() {
        let wire = encode(2, 0, NUMERIC_NEG, 2, &[12, 3400]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("-12.34")
        );
    }

    // ── special values → None ────────────────────────────────────────────────

    #[test]
    fn nan_returns_none() {
        let wire = encode(0, 0, NUMERIC_NAN, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn positive_infinity_returns_none() {
        let wire = encode(0, 0, NUMERIC_PINF, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn negative_infinity_returns_none() {
        let wire = encode(0, 0, NUMERIC_NINF, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn unknown_sign_field_returns_none() {
        let wire = encode(0, 0, 0xBEEF, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    // ── malformed payloads ────────────────────────────────────────────────────

    #[test]
    fn empty_slice_returns_none() {
        assert!(numeric_wire_normalized_plain(&[]).is_none());
    }

    #[test]
    fn truncated_header_returns_none() {
        // Only 6 bytes — not enough for the 8-byte header.
        assert!(numeric_wire_normalized_plain(&[0, 1, 0, 0, 0, 0]).is_none());
    }

    #[test]
    fn ndigits_exceeds_actual_data_returns_none() {
        // Claims ndigits=3 but provides 0 digit limbs.
        let wire = encode(3, 0, NUMERIC_POS, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn limb_out_of_range_returns_none() {
        // Limb value >= 10000 is invalid.
        let wire = encode(1, 0, NUMERIC_POS, 0, &[10000]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }
}
