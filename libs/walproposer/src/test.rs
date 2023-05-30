use crate::{TestFunc, TMP_TEST};

#[test]
fn run_test() {
    let res = unsafe { TestFunc(1, 2) };
    println!("res: {}", res);
}
