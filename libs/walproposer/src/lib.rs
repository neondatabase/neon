#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[no_mangle]
pub extern "C" fn rust_function(a: u32) {
    println!("Hello from Rust!");
    println!("a: {}", a);
}

pub mod sim;
pub mod sim_proto;

#[cfg(test)]
mod test;

#[cfg(test)]
pub mod simtest;
