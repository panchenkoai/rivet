#[test]
fn only_tested_from_an_integration_test() {
    assert_eq!(archfixture::alpha::only_tested(2), 9);
}

fn version() -> u32 {
    2
}

#[test]
fn a_test_crate_function_shares_its_symbol_with_the_library() {
    assert_eq!(version(), 2);
}
