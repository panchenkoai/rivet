use crate::beta;

pub struct Widget {
    pub n: u32,
}

pub struct Gadget {
    pub n: u32,
}

impl Widget {
    pub fn render(&self) -> u32 {
        self.n + 1
    }
}

impl Gadget {
    pub fn render(&self) -> u32 {
        self.n + 2
    }
}

pub fn alpha_calls_beta(x: u32) -> u32 {
    beta::beta_leaf(x)
}

pub fn alpha_leaf(x: u32) -> u32 {
    x * 2
}

pub fn square_side(s: &crate::shapes::Square) -> u32 {
    s.0
}

pub fn only_tested(x: u32) -> u32 {
    x + 7
}

macro_rules! make_fn {
    ($name:ident) => {
        pub fn $name() -> u32 {
            5
        }
    };
}

make_fn!(generated);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_tested_adds_seven() {
        assert_eq!(only_tested(1), 8);
    }
}
