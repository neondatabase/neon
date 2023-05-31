#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::{TestFunc};

use std::cell::RefCell;

thread_local! {
    pub static TMP_TEST: RefCell<Vec<u32>> = RefCell::new(vec![]);
}

#[no_mangle]
pub extern "C" fn rust_function(a: u32) {
    println!("Hello from Rust!");
    println!("a: {}", a);
    TMP_TEST.with(|f| {
        f.borrow_mut().push(a);
        println!("TMP_TEST: {:?}", f.borrow());
    });
}

#[cfg(test)]
mod test;
