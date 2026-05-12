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
