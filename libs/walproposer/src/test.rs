use crate::bindings::{TestFunc, WalProposerRust};

#[test]
fn test_rust_c_calls() {
    let res = unsafe { TestFunc(1, 2) };
    println!("res: {}", res);
}

#[test]
fn test_sim_bindings() {
    // unsafe { RunClientC(0); }
    unsafe {
        WalProposerRust();
    }
}
