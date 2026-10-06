use crate::alpha;

pub trait Solo {
    fn solo(&self) -> u32;
}

impl Solo for u8 {
    fn solo(&self) -> u32 {
        9
    }
}

pub fn beta_leaf(x: u32) -> u32 {
    x + 1
}

pub fn beta_calls_alpha(x: u32) -> u32 {
    alpha::alpha_leaf(x)
}

pub fn use_widget(w: &alpha::Widget) -> u32 {
    w.render()
}

pub fn use_widget_twice(w: &crate::Widget) -> u32 {
    w.render() + w.render()
}

pub fn use_gadget(g: &alpha::Gadget) -> u32 {
    g.render() + alpha::generated()
}

pub fn beta_version() -> u32 {
    crate::version()
}

pub fn clamp_low(input: &[u32], floor: u32) -> Vec<u32> {
    let mut out = Vec::new();
    for value in input {
        if *value < floor {
            out.push(floor);
        } else {
            out.push(*value);
        }
    }
    out.sort();
    out.dedup();
    out
}

pub fn clamp_high(items: &[u32], ceiling: u32) -> Vec<u32> {
    let mut kept = Vec::new();
    for entry in items {
        if *entry < ceiling {
            kept.push(ceiling);
        } else {
            kept.push(*entry);
        }
    }
    kept.sort();
    kept.dedup();
    kept
}
