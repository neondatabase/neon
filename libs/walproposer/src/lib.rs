#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use bindings::TestFunc;

#[no_mangle]
pub extern "C" fn rust_function() {
    println!("Hello from Rust!");
}

#[cfg(test)]
mod test;
