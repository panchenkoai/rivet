//! Ground-truth crate for `archfacts selftest`: every construct here has an expected fact in selftest.py.

pub mod alpha;
pub mod beta;
#[cfg(feature = "oracle")]
pub mod extra;
pub mod facade;
pub mod shapes;

pub use alpha::Widget;

pub fn version() -> u32 {
    1
}
