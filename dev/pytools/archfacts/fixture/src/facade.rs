use crate::alpha::{self, Widget};

pub fn forward(x: u32) -> u32 {
    alpha::alpha_leaf(x)
}

pub fn forward_method(w: &Widget) -> u32 {
    w.render()
}

pub enum Mode {
    Fast,
    Slow,
    Off,
}

pub fn speed(m: &Mode) -> u32 {
    match m {
        Mode::Fast => 3,
        Mode::Slow => 1,
        Mode::Off => 0,
    }
}

pub fn is_on(m: &Mode) -> bool {
    use Mode::*;
    match m {
        Fast | Slow => true,
        Off => false,
    }
}

impl crate::beta::Solo for Mode {
    fn solo(&self) -> u32 {
        speed(self)
    }
}

pub fn label(x: u32) -> (&'static str, u32) {
    ("caf\u{e9} — naïve", alpha::alpha_leaf(x))
}

#[cfg(feature = "oracle")]
pub fn leak(t: &crate::extra::OraThing) -> u32 {
    t.0
}
