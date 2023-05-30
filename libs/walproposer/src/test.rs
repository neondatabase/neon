use crate::TestFunc;

#[test]
fn run_test() {
    let res = unsafe { TestFunc(1, 2) };
    println!("res: {}", res);
}
