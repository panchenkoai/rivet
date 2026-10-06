pub trait Shape {
    fn area(&self) -> u32;
}

pub trait Solo {
    fn solo(&self) -> u32;
}

pub struct Square(pub u32);
pub struct Circle(pub u32);
pub struct Triangle;

impl Shape for Square {
    fn area(&self) -> u32 {
        self.0 * self.0
    }
}

impl Shape for Circle {
    fn area(&self) -> u32 {
        3 * self.0 * self.0
    }
}

impl Solo for Square {
    fn solo(&self) -> u32 {
        self.0
    }
}

macro_rules! impl_shape {
    ($t:ty, $v:expr) => {
        impl Shape for $t {
            fn area(&self) -> u32 {
                $v
            }
        }
    };
}

impl_shape!(Triangle, 3);

pub fn widget_size(w: &crate::Widget) -> u32 {
    w.n
}

pub fn dup_one(values: &[u32]) -> u32 {
    let mut total = 0;
    for v in values {
        if *v > 10 {
            total += v * 2;
        } else if *v > 5 {
            total += v + 1;
        } else {
            total += 1;
        }
    }
    total
}

pub fn dup_two(values: &[u32]) -> u32 {
    let mut total = 0;
    for v in values {
        if *v > 10 {
            total += v * 2;
        } else if *v > 5 {
            total += v + 1;
        } else {
            total += 1;
        }
    }
    total
}

pub fn dup_renamed(items: &[u32]) -> u32 {
    let mut acc = 0;
    for it in items {
        if *it > 10 {
            acc += it * 2;
        } else if *it > 5 {
            acc += it + 1;
        } else {
            acc += 1;
        }
    }
    acc
}

pub fn dup_near(values: &[u32]) -> u32 {
    let mut total = 0;
    for v in values {
        if *v > 10 {
            total += v * 2;
        } else if *v > 5 {
            total += v + 1;
        } else {
            total += 1;
        }
    }
    total + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake;

    impl Solo for Fake {
        fn solo(&self) -> u32 {
            0
        }
    }

    #[test]
    fn fake_is_solo() {
        assert_eq!(Fake.solo(), 0);
    }
}
