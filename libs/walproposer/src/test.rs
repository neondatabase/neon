use crate::bindings::{TestFunc, MyContextInit};

#[test]
fn test_rust_c_calls() {
    let res = std::thread::spawn(|| {
        let res = unsafe {
            MyContextInit();
            TestFunc(1, 2)
        };
        res
    }).join().unwrap();
    println!("res: {}", res);
}

#[test]
fn test_sim_bindings() {
    std::thread::spawn(|| {
        unsafe {
            MyContextInit();
            TestFunc(1, 2)
        }
    }).join().unwrap();
    std::thread::spawn(|| {
        unsafe {
            MyContextInit();
            TestFunc(1, 2)
        }
    }).join().unwrap();
}
