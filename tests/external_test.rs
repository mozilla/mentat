extern crate datomish;

#[test]
fn external_test() {
    assert_eq!(4, datomish::add_two(2));
}