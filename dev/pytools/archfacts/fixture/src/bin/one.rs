fn helper() -> u32 {
    1
}

fn main() {
    println!("{}", helper() + archfixture::version());
}
